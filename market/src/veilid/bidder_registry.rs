use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use veilid_core::{PublicKey, RecordKey};

use super::dht::DHTOperations;
use crate::config::now_unix;
use crate::traits::TimeProvider;

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
    pub const fn new(listing_key: RecordKey) -> Self {
        Self {
            listing_key,
            bidders: Vec::new(),
        }
    }

    /// Add a bidder using system time.
    pub fn add_bidder(&mut self, bidder: PublicKey, bid_record_key: RecordKey) {
        // Avoid duplicates
        if !self.bidders.iter().any(|b| b.bidder == bidder) {
            self.bidders.push(BidderEntry {
                bidder,
                bid_record_key,
                timestamp: now_unix(),
            });
        }
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
            .map_err(|e| anyhow::anyhow!("Failed to serialize bidder registry: {e}"))?;
        Ok(data)
    }

    pub fn from_cbor(data: &[u8]) -> Result<Self> {
        ciborium::de::from_reader(data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize bidder registry: {e}"))
    }
}

/// Operations for managing the bidder registry
pub struct BidderRegistryOps {
    dht: DHTOperations,
}

impl BidderRegistryOps {
    pub const fn new(dht: DHTOperations) -> Self {
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
            .map_err(|e| anyhow::anyhow!("Failed to open listing record: {e}"))?;

        for attempt in 0..max_retries {
            // Read current registry
            let old_value = routing_context
                .get_dht_value(listing_key.clone(), Self::BIDDER_REGISTRY_SUBKEY, true)
                .await?;
            let old_seq = old_value.as_ref().map(veilid_core::ValueData::seq);

            let mut registry = if let Some(value) = &old_value {
                BidderRegistry::from_cbor(value.data())?
            } else {
                debug!("No bidder registry found, creating new one");
                BidderRegistry::new(listing_key.clone())
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
                        "Failed to register bidder after {max_retries} attempts: {e}"
                    ));
                }
            }
        }

        let _ = routing_context.close_dht_record(listing_key.clone()).await;
        Err(anyhow::anyhow!(
            "Failed to register bidder after {max_retries} retries"
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
                if let Some(value) = routing_context
                    .get_dht_value(listing_key.clone(), Self::BIDDER_REGISTRY_SUBKEY, true)
                    .await?
                {
                    let registry = BidderRegistry::from_cbor(value.data())?;
                    info!(
                        "Fetched bidder registry with {} bidders",
                        registry.bidders.len()
                    );
                    let _ = routing_context.close_dht_record(listing_key.clone()).await;
                    Ok(registry)
                } else {
                    debug!("No bidder registry found");
                    let _ = routing_context.close_dht_record(listing_key.clone()).await;
                    Ok(BidderRegistry::new(listing_key.clone()))
                }
            }
            Err(e) => Err(anyhow::anyhow!("Failed to open listing record: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocks::{make_test_public_key, make_test_record_key, MockTime};

    #[test]
    fn test_bidder_registry_cbor_roundtrip() {
        let listing_key = make_test_record_key(1);
        let bidder1 = make_test_public_key(1);
        let bidder2 = make_test_public_key(2);
        let bid_record_key1 = make_test_record_key(10);
        let bid_record_key2 = make_test_record_key(20);

        let registry = BidderRegistry {
            listing_key: listing_key.clone(),
            bidders: vec![
                BidderEntry {
                    bidder: bidder1.clone(),
                    bid_record_key: bid_record_key1.clone(),
                    timestamp: 1000,
                },
                BidderEntry {
                    bidder: bidder2.clone(),
                    bid_record_key: bid_record_key2.clone(),
                    timestamp: 2000,
                },
            ],
        };

        let bytes = registry.to_cbor().expect("Should serialize");
        let decoded = BidderRegistry::from_cbor(&bytes).expect("Should deserialize");

        assert_eq!(decoded.listing_key, listing_key);
        assert_eq!(decoded.bidders.len(), 2);
        assert_eq!(decoded.bidders[0].bidder, bidder1);
        assert_eq!(decoded.bidders[0].bid_record_key, bid_record_key1);
        assert_eq!(decoded.bidders[0].timestamp, 1000);
        assert_eq!(decoded.bidders[1].bidder, bidder2);
        assert_eq!(decoded.bidders[1].bid_record_key, bid_record_key2);
        assert_eq!(decoded.bidders[1].timestamp, 2000);
    }

    #[test]
    fn test_bidder_registry_new() {
        let listing_key = make_test_record_key(1);
        let registry = BidderRegistry::new(listing_key.clone());

        assert_eq!(registry.listing_key, listing_key);
        assert_eq!(registry.bidders.len(), 0);
    }

    #[test]
    fn test_bidder_registry_add_bidder() {
        let listing_key = make_test_record_key(1);
        let mut registry = BidderRegistry::new(listing_key);

        let bidder = make_test_public_key(1);
        let bid_record_key = make_test_record_key(10);

        registry.add_bidder(bidder.clone(), bid_record_key.clone());

        assert_eq!(registry.bidders.len(), 1);
        assert_eq!(registry.bidders[0].bidder, bidder);
        assert_eq!(registry.bidders[0].bid_record_key, bid_record_key);
        assert!(registry.bidders[0].timestamp > 0);
    }

    #[test]
    fn test_bidder_registry_add_bidder_with_time() {
        let listing_key = make_test_record_key(1);
        let mut registry = BidderRegistry::new(listing_key);

        let bidder = make_test_public_key(1);
        let bid_record_key = make_test_record_key(10);
        let time = MockTime::new(5000);

        registry.add_bidder_with_time(bidder.clone(), bid_record_key.clone(), &time);

        assert_eq!(registry.bidders.len(), 1);
        assert_eq!(registry.bidders[0].bidder, bidder);
        assert_eq!(registry.bidders[0].bid_record_key, bid_record_key);
        assert_eq!(registry.bidders[0].timestamp, 5000);
    }

    #[test]
    fn test_bidder_registry_no_duplicates() {
        let listing_key = make_test_record_key(1);
        let mut registry = BidderRegistry::new(listing_key);

        let bidder = make_test_public_key(1);
        let bid_record_key1 = make_test_record_key(10);
        let bid_record_key2 = make_test_record_key(20);
        let time = MockTime::new(5000);

        // Add bidder first time
        registry.add_bidder_with_time(bidder.clone(), bid_record_key1.clone(), &time);
        assert_eq!(registry.bidders.len(), 1);
        assert_eq!(registry.bidders[0].bid_record_key, bid_record_key1);

        // Try to add same bidder again (different bid_record_key) — should be ignored
        time.advance(1000);
        registry.add_bidder_with_time(bidder.clone(), bid_record_key2, &time);
        assert_eq!(registry.bidders.len(), 1);
        assert_eq!(registry.bidders[0].bid_record_key, bid_record_key1);
        assert_eq!(registry.bidders[0].timestamp, 5000); // Unchanged
    }

    #[test]
    fn test_bidder_registry_multiple_bidders() {
        let listing_key = make_test_record_key(1);
        let mut registry = BidderRegistry::new(listing_key);

        let time = MockTime::new(1000);

        // Add 5 different bidders
        for i in 1..=5 {
            let bidder = make_test_public_key(i);
            let bid_record_key = make_test_record_key(i as u64 * 10);
            registry.add_bidder_with_time(bidder.clone(), bid_record_key.clone(), &time);
            time.advance(100);
        }

        assert_eq!(registry.bidders.len(), 5);

        // Verify each bidder is unique
        for (idx, i) in (1..=5).enumerate() {
            assert_eq!(registry.bidders[idx].bidder, make_test_public_key(i));
            assert_eq!(
                registry.bidders[idx].bid_record_key,
                make_test_record_key(i as u64 * 10)
            );
            assert_eq!(registry.bidders[idx].timestamp, 1000 + idx as u64 * 100);
        }
    }

    #[test]
    fn test_bidder_entry_roundtrip() {
        let entry = BidderEntry {
            bidder: make_test_public_key(5),
            bid_record_key: make_test_record_key(42),
            timestamp: 123456,
        };

        // Verify entry fields
        assert_eq!(entry.bidder, make_test_public_key(5));
        assert_eq!(entry.bid_record_key, make_test_record_key(42));
        assert_eq!(entry.timestamp, 123456);
    }

    #[test]
    fn test_bidder_registry_empty_serialization() {
        let listing_key = make_test_record_key(99);
        let registry = BidderRegistry::new(listing_key.clone());

        let bytes = registry.to_cbor().expect("Should serialize empty registry");
        let decoded = BidderRegistry::from_cbor(&bytes).expect("Should deserialize empty registry");

        assert_eq!(decoded.listing_key, listing_key);
        assert_eq!(decoded.bidders.len(), 0);
    }
}
