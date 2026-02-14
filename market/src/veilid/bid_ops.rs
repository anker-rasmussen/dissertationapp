use tracing::{debug, info, warn};
use veilid_core::RecordKey;

use crate::config::{subkeys, BID_REGISTER_INITIAL_DELAY_MS, BID_REGISTER_MAX_RETRIES};
use crate::error::{MarketError, MarketResult};
use crate::marketplace::{BidIndex, BidRecord};
use crate::traits::DhtStore;

/// Operations for managing bids in the DHT.
/// Generic over the DHT store implementation for testability.
pub struct BidOperations<D: DhtStore> {
    dht: D,
}

impl<D: DhtStore> BidOperations<D> {
    pub const fn new(dht: D) -> Self {
        Self { dht }
    }

    /// Publish a bid to the DHT
    pub async fn publish_bid(&self, bid: BidRecord) -> MarketResult<D::OwnedRecord> {
        // Create a new DHT record for this bid
        let record = self.dht.create_record().await?;

        // Serialize and store the bid
        let data = bid.to_cbor()?;
        self.dht.set_value(&record, data).await?;

        let key = D::record_key(&record);
        info!("Published bid to DHT at {}", key);
        Ok(record)
    }

    /// Fetch a bid from the DHT
    pub async fn fetch_bid(&self, bid_key: &RecordKey) -> MarketResult<Option<BidRecord>> {
        match self.dht.get_value(bid_key).await? {
            Some(data) => {
                let bid = BidRecord::from_cbor(&data)?;
                debug!("Fetched bid from DHT at {}", bid_key);
                Ok(Some(bid))
            }
            None => Ok(None),
        }
    }

    /// Register a bid in the shared bid index for a listing.
    /// Uses optimistic concurrency control with retry.
    pub async fn register_bid(
        &self,
        listing_record: &D::OwnedRecord,
        bid: BidRecord,
    ) -> MarketResult<()> {
        let listing_key = D::record_key(listing_record);
        let max_retries = BID_REGISTER_MAX_RETRIES;
        let mut retry_delay = std::time::Duration::from_millis(BID_REGISTER_INITIAL_DELAY_MS);

        for attempt in 0..max_retries {
            // Fetch current bid index from subkey
            let old_value = self
                .dht
                .get_subkey(&listing_key, subkeys::BID_INDEX)
                .await?;

            let mut index = if let Some(data) = old_value {
                BidIndex::from_cbor(&data)?
            } else {
                debug!("No bid index found, creating new one");
                BidIndex::new(listing_key.clone())
            };

            // Add our bid
            index.add_bid(bid.clone());

            // Try to write back
            let data = index.to_cbor()?;

            match self
                .dht
                .set_subkey(listing_record, subkeys::BID_INDEX, data)
                .await
            {
                Ok(()) => {
                    info!(
                        "Successfully registered bid in index (attempt {}/{})",
                        attempt + 1,
                        max_retries
                    );
                    return Ok(());
                }
                Err(e) => {
                    warn!(
                        "Failed to write bid index (attempt {}/{}): {}",
                        attempt + 1,
                        max_retries,
                        e
                    );
                    if attempt < max_retries - 1 {
                        tokio::time::sleep(retry_delay).await;
                        retry_delay *= 2;
                        continue;
                    }
                    return Err(MarketError::Dht(format!(
                        "Failed to register bid after {max_retries} attempts: {e}"
                    )));
                }
            }
        }

        Err(MarketError::Dht(format!(
            "Failed to register bid after {max_retries} retries"
        )))
    }

    /// Fetch the bid index for a listing
    pub async fn fetch_bid_index(&self, listing_key: &RecordKey) -> MarketResult<BidIndex> {
        if let Some(data) = self.dht.get_subkey(listing_key, subkeys::BID_INDEX).await? {
            let index = BidIndex::from_cbor(&data)?;
            debug!("Fetched bid index with {} bids", index.bids.len());
            Ok(index)
        } else {
            debug!("No bid index found, returning empty");
            Ok(BidIndex::new(listing_key.clone()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocks::{make_test_public_key, make_test_record_key, MockDht};

    fn make_test_bid(listing_key: RecordKey, bidder_id: u8) -> BidRecord {
        BidRecord {
            listing_key,
            bidder: make_test_public_key(bidder_id),
            commitment: [bidder_id; 32],
            timestamp: 1000,
            bid_key: make_test_record_key(bidder_id as u64),
            signing_pubkey: [bidder_id; 32],
        }
    }

    #[tokio::test]
    async fn test_publish_bid() {
        let dht = MockDht::new();
        let ops = BidOperations::new(dht.clone());
        let listing_key = make_test_record_key(1);

        let bid = make_test_bid(listing_key, 1);
        let record = ops.publish_bid(bid).await.unwrap();
        let key = MockDht::record_key(&record);

        assert!(dht.has_record(&key).await);
    }

    #[tokio::test]
    async fn test_fetch_bid() {
        let dht = MockDht::new();
        let ops = BidOperations::new(dht.clone());
        let listing_key = make_test_record_key(1);

        let bid = make_test_bid(listing_key, 1);
        let record = ops.publish_bid(bid.clone()).await.unwrap();
        let key = MockDht::record_key(&record);

        let fetched = ops.fetch_bid(&key).await.unwrap().unwrap();
        assert_eq!(fetched.bidder, bid.bidder);
        assert_eq!(fetched.commitment, bid.commitment);
    }

    #[tokio::test]
    async fn test_fetch_nonexistent_bid() {
        let dht = MockDht::new();
        let ops = BidOperations::new(dht);

        let key = make_test_record_key(99);
        let result = ops.fetch_bid(&key).await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_register_bid() {
        let dht = MockDht::new();
        let ops = BidOperations::new(dht.clone());

        // Create a listing record first
        let listing_record = dht.create_record().await.unwrap();
        let listing_key = MockDht::record_key(&listing_record);

        let bid = make_test_bid(listing_key.clone(), 1);
        ops.register_bid(&listing_record, bid).await.unwrap();

        // Fetch the index and verify
        let index = ops.fetch_bid_index(&listing_key).await.unwrap();
        assert_eq!(index.bids.len(), 1);
    }

    #[tokio::test]
    async fn test_register_multiple_bids() {
        let dht = MockDht::new();
        let ops = BidOperations::new(dht.clone());

        let listing_record = dht.create_record().await.unwrap();
        let listing_key = MockDht::record_key(&listing_record);

        // Register multiple bids
        for i in 1..=3 {
            let bid = make_test_bid(listing_key.clone(), i);
            ops.register_bid(&listing_record, bid).await.unwrap();
        }

        let index = ops.fetch_bid_index(&listing_key).await.unwrap();
        assert_eq!(index.bids.len(), 3);
    }

    #[tokio::test]
    async fn test_register_duplicate_bid() {
        let dht = MockDht::new();
        let ops = BidOperations::new(dht.clone());

        let listing_record = dht.create_record().await.unwrap();
        let listing_key = MockDht::record_key(&listing_record);

        let bid = make_test_bid(listing_key.clone(), 1);
        ops.register_bid(&listing_record, bid.clone())
            .await
            .unwrap();
        ops.register_bid(&listing_record, bid).await.unwrap(); // Duplicate

        let index = ops.fetch_bid_index(&listing_key).await.unwrap();
        assert_eq!(index.bids.len(), 1); // Should not have duplicate
    }

    #[tokio::test]
    async fn test_fetch_empty_bid_index() {
        let dht = MockDht::new();
        let ops = BidOperations::new(dht);

        let listing_key = make_test_record_key(1);
        let index = ops.fetch_bid_index(&listing_key).await.unwrap();

        assert!(index.bids.is_empty());
    }
}
