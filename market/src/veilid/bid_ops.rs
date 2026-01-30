use anyhow::Result;
use tracing::{info, debug, warn};
use veilid_core::RecordKey;

use crate::marketplace::{BidRecord, BidIndex};
use super::dht::{DHTOperations, OwnedDHTRecord};

/// Operations for managing bids in the DHT
pub struct BidOperations {
    dht: DHTOperations,
}

impl BidOperations {
    pub fn new(dht: DHTOperations) -> Self {
        Self { dht }
    }

    /// Publish a bid to the DHT
    pub async fn publish_bid(&self, bid: BidRecord) -> Result<OwnedDHTRecord> {
        // Create a new DHT record for this bid
        let record = self.dht.create_record().await?;

        // Serialize and store the bid
        let data = bid.to_cbor()?;
        self.dht.set_value(&record, data).await?;

        info!("Published bid to DHT at {}", record.key);
        Ok(record)
    }

    /// Fetch a bid from the DHT
    pub async fn fetch_bid(&self, bid_key: &RecordKey) -> Result<Option<BidRecord>> {
        match self.dht.get_value(bid_key).await? {
            Some(data) => {
                let bid = BidRecord::from_cbor(&data)?;
                debug!("Fetched bid from DHT at {}", bid_key);
                Ok(Some(bid))
            }
            None => Ok(None),
        }
    }

    /// Get the bid index subkey (stored alongside listing in same DHT record)
    /// Listing is at subkey 0, bid index is at subkey 1
    const BID_INDEX_SUBKEY: u32 = 1;

    /// Register a bid in the shared bid index for a listing
    pub async fn register_bid(&mut self, bid: BidRecord) -> Result<()> {
        let listing_key = bid.listing_key.clone();

        // Use optimistic concurrency control with retry
        let max_retries = 10;
        let mut retry_delay = std::time::Duration::from_millis(50);

        let routing_context = self.dht.get_routing_context_pub()?;

        // Open the listing record (read/write)
        let _ = routing_context.open_dht_record(listing_key.clone(), None).await
            .map_err(|e| anyhow::anyhow!("Failed to open listing record: {}", e))?;

        for attempt in 0..max_retries {
            // Fetch current bid index from subkey 1 (read directly, don't use fetch_bid_index to avoid double-open)
            let old_value = routing_context.get_dht_value(listing_key.clone(), Self::BID_INDEX_SUBKEY, true).await?;
            let old_seq = old_value.as_ref().map(|v| v.seq());

            let mut index = match &old_value {
                Some(value) => {
                    BidIndex::from_cbor(value.data())?
                }
                None => {
                    // No bid index yet, create empty one
                    debug!("No bid index found, creating new one");
                    BidIndex::new(listing_key.clone())
                }
            };

            // Add our bid
            index.add_bid(bid.clone());

            // Try to write back to subkey 1
            let data = index.to_cbor()?;

            // Write new value
            match routing_context.set_dht_value(listing_key.clone(), Self::BID_INDEX_SUBKEY, data.clone(), None).await {
                Ok(returned_value) => {
                    // Check for concurrent modifications
                    if let Some(rv) = returned_value {
                        if Some(rv.seq()) != old_seq {
                            // Concurrent write detected, retry
                            warn!("Bid index conflict detected (attempt {}/{}), retrying...", attempt + 1, max_retries);
                            tokio::time::sleep(retry_delay).await;
                            retry_delay *= 2;
                            continue;
                        }
                    }
                    info!("Successfully registered bid in index (attempt {}/{})", attempt + 1, max_retries);
                    let _ = routing_context.close_dht_record(listing_key).await;
                    return Ok(());
                }
                Err(e) => {
                    warn!("Failed to write bid index (attempt {}/{}): {}", attempt + 1, max_retries, e);
                    if attempt < max_retries - 1 {
                        tokio::time::sleep(retry_delay).await;
                        retry_delay *= 2;
                        continue;
                    }
                    let _ = routing_context.close_dht_record(listing_key).await;
                    return Err(anyhow::anyhow!("Failed to register bid after {} attempts: {}", max_retries, e));
                }
            }
        }

        let _ = routing_context.close_dht_record(listing_key).await;
        Err(anyhow::anyhow!("Failed to register bid after {} retries", max_retries))
    }

    /// Discover bids for a listing in devnet mode
    /// Uses a simple approach: write bid announcements to listing's subkey 1
    /// Each bidder's BidStorage maintains their bid_key, which we'll aggregate
    pub async fn discover_bids_devnet(
        &self,
        listing_key: &RecordKey,
        _my_node_id: &veilid_core::PublicKey,
        my_bid_key: &RecordKey,
    ) -> Result<BidIndex> {

        let mut bid_index = BidIndex::new(listing_key.clone());

        // Add our own bid first
        if let Some(our_bid) = self.fetch_bid(my_bid_key).await? {
            info!("Added our own bid to index");
            bid_index.add_bid(our_bid);
        }

        // For devnet MVP: Just return our own bid for now
        // In a real system, we'd use AppMessages to announce bids to each other
        // or maintain a shared coordination record

        info!("Devnet bid discovery: found {} bids (currently only our own)", bid_index.bids.len());
        Ok(bid_index)
    }

    /// Fetch all bids for a listing by discovering bidders' individual BidRecords
    /// This is the production version that would use proper P2P discovery
    pub async fn fetch_bid_index(&self, listing_key: &RecordKey) -> Result<BidIndex> {
        // For now, just return empty - this would be implemented with AppMessage-based discovery
        let bid_index = BidIndex::new(listing_key.clone());
        debug!("P2P bid discovery not yet implemented, returning empty index");
        Ok(bid_index)
    }
}
