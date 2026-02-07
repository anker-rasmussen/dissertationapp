use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use veilid_core::{PublicKey, RecordKey};

use super::dht::DHTOperations;
use crate::traits::{SystemTimeProvider, TimeProvider};

/// Registry entry for a bidder on a specific listing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BidderEntry {
    /// Bidder's public key
    pub bidder: PublicKey,
    /// DHT key where their BidRecord is stored
    pub bid_record_key: RecordKey,
    /// Timestamp when they registered
    pub timestamp: u64,
}

/// Registry of all bidders for a specific listing
/// Stored in the listing's DHT record at subkey 3
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BidderRegistry {
    /// Listing this registry is for
    pub listing_key: RecordKey,
    /// All registered bidders
    pub bidders: Vec<BidderEntry>,
}

impl BidderRegistry {
    pub fn new(listing_key: RecordKey) -> Self {
        Self {
            listing_key,
            bidders: Vec::new(),
        }
    }

    /// Add a bidder using system time.
    pub fn add_bidder(&mut self, bidder: PublicKey, bid_record_key: RecordKey) {
        self.add_bidder_with_time(bidder, bid_record_key, &SystemTimeProvider::new());
    }

    /// Add a bidder with a custom time provider.
    pub fn add_bidder_with_time<T: TimeProvider>(
        &mut self,
        bidder: PublicKey,
        bid_record_key: RecordKey,
        time: &T,
    ) {
        // Avoid duplicates
        if !self.bidders.iter().any(|b| b.bidder == bidder) {
            self.bidders.push(BidderEntry {
                bidder,
                bid_record_key,
                timestamp: time.now_unix(),
            });
        }
    }

    pub fn to_cbor(&self) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        ciborium::ser::into_writer(self, &mut data)
            .map_err(|e| anyhow::anyhow!("Failed to serialize bidder registry: {}", e))?;
        Ok(data)
    }

    pub fn from_cbor(data: &[u8]) -> Result<Self> {
        ciborium::de::from_reader(data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize bidder registry: {}", e))
    }
}

/// Operations for managing the bidder registry
pub struct BidderRegistryOps {
    dht: DHTOperations,
}

impl BidderRegistryOps {
    pub fn new(dht: DHTOperations) -> Self {
        Self { dht }
    }

    const BIDDER_REGISTRY_SUBKEY: u32 = 3;

    /// Register as a bidder for a listing
    pub async fn register_bidder(
        &self,
        listing_key: &RecordKey,
        bidder: PublicKey,
        bid_record_key: RecordKey,
    ) -> Result<()> {
        let max_retries = 10;
        let mut retry_delay = std::time::Duration::from_millis(50);

        let routing_context = self.dht.get_routing_context_pub()?;

        // Open the listing record
        let _ = routing_context
            .open_dht_record(listing_key.clone(), None)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open listing record: {}", e))?;

        for attempt in 0..max_retries {
            // Read current registry
            let old_value = routing_context
                .get_dht_value(listing_key.clone(), Self::BIDDER_REGISTRY_SUBKEY, true)
                .await?;
            let old_seq = old_value.as_ref().map(|v| v.seq());

            let mut registry = match &old_value {
                Some(value) => BidderRegistry::from_cbor(value.data())?,
                None => {
                    debug!("No bidder registry found, creating new one");
                    BidderRegistry::new(listing_key.clone())
                }
            };

            // Add ourselves
            registry.add_bidder(bidder.clone(), bid_record_key.clone());
            let data = registry.to_cbor()?;

            // Try to write
            match routing_context
                .set_dht_value(
                    listing_key.clone(),
                    Self::BIDDER_REGISTRY_SUBKEY,
                    data,
                    None,
                )
                .await
            {
                Ok(returned_value) => {
                    // Check for concurrent modifications
                    if let Some(rv) = returned_value {
                        if Some(rv.seq()) != old_seq {
                            warn!(
                                "Bidder registry conflict (attempt {}/{}), retrying...",
                                attempt + 1,
                                max_retries
                            );
                            tokio::time::sleep(retry_delay).await;
                            retry_delay *= 2;
                            continue;
                        }
                    }
                    info!(
                        "Successfully registered as bidder (attempt {}/{})",
                        attempt + 1,
                        max_retries
                    );
                    let _ = routing_context.close_dht_record(listing_key.clone()).await;
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "Failed to write bidder registry (attempt {}/{}): {}",
                        attempt + 1,
                        max_retries,
                        e
                    );
                    if attempt < max_retries - 1 {
                        tokio::time::sleep(retry_delay).await;
                        retry_delay *= 2;
                        continue;
                    }
                    let _ = routing_context.close_dht_record(listing_key.clone()).await;
                    return Err(anyhow::anyhow!(
                        "Failed to register bidder after {} attempts: {}",
                        max_retries,
                        e
                    ));
                }
            }
        }

        let _ = routing_context.close_dht_record(listing_key.clone()).await;
        Err(anyhow::anyhow!(
            "Failed to register bidder after {} retries",
            max_retries
        ))
    }

    /// Fetch all registered bidders for a listing
    pub async fn fetch_bidders(&self, listing_key: &RecordKey) -> Result<BidderRegistry> {
        let routing_context = self.dht.get_routing_context_pub()?;

        match routing_context
            .open_dht_record(listing_key.clone(), None)
            .await
        {
            Ok(_) => {
                match routing_context
                    .get_dht_value(listing_key.clone(), Self::BIDDER_REGISTRY_SUBKEY, true)
                    .await?
                {
                    Some(value) => {
                        let registry = BidderRegistry::from_cbor(value.data())?;
                        info!(
                            "Fetched bidder registry with {} bidders",
                            registry.bidders.len()
                        );
                        let _ = routing_context.close_dht_record(listing_key.clone()).await;
                        Ok(registry)
                    }
                    None => {
                        debug!("No bidder registry found");
                        let _ = routing_context.close_dht_record(listing_key.clone()).await;
                        Ok(BidderRegistry::new(listing_key.clone()))
                    }
                }
            }
            Err(e) => Err(anyhow::anyhow!("Failed to open listing record: {}", e)),
        }
    }
}
