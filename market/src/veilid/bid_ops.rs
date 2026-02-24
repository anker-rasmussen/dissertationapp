use tracing::{debug, info, warn};
use veilid_core::RecordKey;

use crate::config::subkeys;
use crate::error::MarketResult;
use crate::marketplace::{BidIndex, BidRecord};
use crate::traits::DhtStore;
use crate::veilid::bid_announcement::BidAnnouncementRegistry;

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

    /// Build a bid index for a listing by reading the BID_ANNOUNCEMENTS
    /// registry and fetching each bidder's individual BidRecord.
    ///
    /// This is the CRDT read path: the seller writes announcements to
    /// its own record; readers merge at read time by fetching individual
    /// bid records from their respective DHT keys.
    pub async fn fetch_bid_index(&self, listing_key: &RecordKey) -> MarketResult<BidIndex> {
        let registry = if let Some(data) = self
            .dht
            .get_subkey(listing_key, subkeys::BID_ANNOUNCEMENTS)
            .await?
        {
            BidAnnouncementRegistry::from_bytes(&data)?
        } else {
            debug!("No bid announcements found, returning empty index");
            return Ok(BidIndex::new(listing_key.clone()));
        };

        let mut index = BidIndex::new(listing_key.clone());

        for (bidder, bid_record_key, _timestamp) in &registry.announcements {
            match self.dht.get_value(bid_record_key).await {
                Ok(Some(data)) => match BidRecord::from_cbor(&data) {
                    Ok(bid) => {
                        index.add_bid(bid);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to deserialize BidRecord at {}: {}",
                            bid_record_key, e
                        );
                    }
                },
                Ok(None) => {
                    warn!(
                        "BidRecord not found at {} for bidder {}",
                        bid_record_key, bidder
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to fetch BidRecord at {} for bidder {}: {}",
                        bid_record_key, bidder, e
                    );
                }
            }
        }

        debug!(
            "Built bid index with {} bids from {} announcements",
            index.bids.len(),
            registry.announcements.len()
        );
        Ok(index)
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
    async fn test_fetch_empty_bid_index() {
        let dht = MockDht::new();
        let ops = BidOperations::new(dht);

        let listing_key = make_test_record_key(1);
        let index = ops.fetch_bid_index(&listing_key).await.unwrap();

        assert!(index.bids.is_empty());
    }

    #[tokio::test]
    async fn test_fetch_bid_index_from_announcements() {
        let dht = MockDht::new();
        let ops = BidOperations::new(dht.clone());
        let listing_key = make_test_record_key(100);

        // Create a listing record with BID_ANNOUNCEMENTS
        let listing_record = dht.create_record().await.unwrap();
        // Override listing_key to use listing_record's key
        let listing_key = MockDht::record_key(&listing_record);

        // Publish two bids to their own DHT records
        let bid1 = BidRecord {
            listing_key: listing_key.clone(),
            bidder: make_test_public_key(1),
            commitment: [1; 32],
            timestamp: 1000,
            bid_key: make_test_record_key(10), // placeholder, overwritten below
            signing_pubkey: [1; 32],
        };
        let bid1_record = ops.publish_bid(bid1.clone()).await.unwrap();
        let bid1_key = MockDht::record_key(&bid1_record);

        let bid2 = BidRecord {
            listing_key: listing_key.clone(),
            bidder: make_test_public_key(2),
            commitment: [2; 32],
            timestamp: 2000,
            bid_key: make_test_record_key(20),
            signing_pubkey: [2; 32],
        };
        let bid2_record = ops.publish_bid(bid2.clone()).await.unwrap();
        let bid2_key = MockDht::record_key(&bid2_record);

        // Write BID_ANNOUNCEMENTS registry to the listing record
        let mut registry = BidAnnouncementRegistry::new();
        registry.add(bid1.bidder.clone(), bid1_key, 1000);
        registry.add(bid2.bidder.clone(), bid2_key, 2000);
        let data = registry.to_bytes().unwrap();
        dht.set_subkey(&listing_record, subkeys::BID_ANNOUNCEMENTS, data)
            .await
            .unwrap();

        // Fetch bid index — should build from announcements + individual records
        let index = ops.fetch_bid_index(&listing_key).await.unwrap();
        assert_eq!(index.bids.len(), 2);
        assert!(index.bids.iter().any(|b| b.bidder == bid1.bidder));
        assert!(index.bids.iter().any(|b| b.bidder == bid2.bidder));
    }
}
