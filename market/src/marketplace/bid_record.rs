use serde::{Deserialize, Serialize};
use veilid_core::{PublicKey, RecordKey};

use crate::config::now_unix;
use crate::traits::TimeProvider;

/// A bid record published to the DHT for auction participation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BidRecord {
    /// The listing this bid is for
    pub listing_key: RecordKey,

    /// Bidder's node ID (public key)
    pub bidder: PublicKey,

    /// Commitment to the bid value (hash of bid + nonce)
    pub commitment: [u8; 32],

    /// Timestamp when bid was submitted
    pub timestamp: u64,

    /// DHT key where this bid record is stored
    pub bid_key: RecordKey,
}

impl BidRecord {
    /// Serialize to CBOR for DHT storage
    pub fn to_cbor(&self) -> anyhow::Result<Vec<u8>> {
        let mut data = Vec::new();
        ciborium::ser::into_writer(self, &mut data)
            .map_err(|e| anyhow::anyhow!("Failed to serialize bid record: {e}"))?;
        Ok(data)
    }

    /// Deserialize from CBOR
    pub fn from_cbor(data: &[u8]) -> anyhow::Result<Self> {
        ciborium::de::from_reader(data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize bid record: {e}"))
    }
}

/// Index of all bids for a specific listing
/// Published to a well-known DHT location derived from listing key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BidIndex {
    /// Listing this index is for
    pub listing_key: RecordKey,

    /// All bid records for this listing
    pub bids: Vec<BidRecord>,

    /// Last update timestamp
    pub last_updated: u64,
}

impl BidIndex {
    pub const fn new(listing_key: RecordKey) -> Self {
        Self {
            listing_key,
            bids: Vec::new(),
            last_updated: 0,
        }
    }

    /// Add a bid using the system clock for timestamp
    pub fn add_bid(&mut self, bid: BidRecord) {
        // Inline timestamp to avoid generic TimeProvider in the production path
        if !self.bids.iter().any(|b| b.bidder == bid.bidder) {
            self.bids.push(bid);
            self.last_updated = now_unix();
        }
    }

    /// Add a bid with a custom time provider for testing
    pub fn add_bid_with_time<T: TimeProvider>(&mut self, bid: BidRecord, time: &T) {
        // Check for duplicates (same bidder)
        if !self.bids.iter().any(|b| b.bidder == bid.bidder) {
            self.bids.push(bid);
            self.last_updated = time.now_unix();
        }
    }

    /// Get bidders sorted by bid timestamp (ascending), with pubkey as tiebreaker.
    /// The seller's auto-bid (created at listing time) always has the earliest
    /// timestamp, making them party 0. Remaining bidders are ordered by time.
    pub fn sorted_bidders(&self) -> Vec<PublicKey> {
        let mut bids_sorted: Vec<_> = self.bids.iter().collect();
        bids_sorted.sort_by(|a, b| {
            a.timestamp
                .cmp(&b.timestamp)
                .then_with(|| a.bidder.to_string().cmp(&b.bidder.to_string()))
        });
        bids_sorted.into_iter().map(|b| b.bidder.clone()).collect()
    }

    /// Get party ID for a specific bidder
    pub fn get_party_id(&self, bidder: &PublicKey) -> Option<usize> {
        let sorted = self.sorted_bidders();
        sorted.iter().position(|b| b == bidder)
    }

    pub fn to_cbor(&self) -> anyhow::Result<Vec<u8>> {
        let mut data = Vec::new();
        ciborium::ser::into_writer(self, &mut data)
            .map_err(|e| anyhow::anyhow!("Failed to serialize bid index: {e}"))?;
        Ok(data)
    }

    pub fn from_cbor(data: &[u8]) -> anyhow::Result<Self> {
        ciborium::de::from_reader(data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize bid index: {e}"))
    }

    /// Merge another index into this one
    pub fn merge(&mut self, other: &Self) {
        for bid in &other.bids {
            // Use the production add_bid which stamps with now_unix()
            self.add_bid(bid.clone());
        }
    }

    /// Merge with custom time provider
    pub fn merge_with_time<T: TimeProvider>(&mut self, other: &Self, time: &T) {
        for bid in &other.bids {
            self.add_bid_with_time(bid.clone(), time);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocks::{make_test_public_key, make_test_record_key, MockTime};

    fn make_test_key() -> RecordKey {
        make_test_record_key(1)
    }

    fn make_bidder(id: u8) -> PublicKey {
        make_test_public_key(id)
    }

    fn make_bid_record(listing_key: RecordKey, bidder_id: u8) -> BidRecord {
        BidRecord {
            listing_key,
            bidder: make_bidder(bidder_id),
            commitment: [bidder_id; 32],
            timestamp: 1000,
            bid_key: make_test_record_key(bidder_id as u64),
        }
    }

    #[test]
    fn test_bid_record_serialization() {
        let original = make_bid_record(make_test_key(), 1);

        let cbor = original.to_cbor().unwrap();
        let restored = BidRecord::from_cbor(&cbor).unwrap();

        assert_eq!(original.listing_key, restored.listing_key);
        assert_eq!(original.bidder, restored.bidder);
        assert_eq!(original.commitment, restored.commitment);
        assert_eq!(original.timestamp, restored.timestamp);
        assert_eq!(original.bid_key, restored.bid_key);
    }

    #[test]
    fn test_bid_index_new() {
        let listing_key = make_test_key();
        let index = BidIndex::new(listing_key.clone());

        assert_eq!(index.listing_key, listing_key);
        assert!(index.bids.is_empty());
        assert_eq!(index.last_updated, 0);
    }

    #[test]
    fn test_bid_index_add_bid() {
        let time = MockTime::new(2000);
        let listing_key = make_test_key();
        let mut index = BidIndex::new(listing_key.clone());

        let bid = make_bid_record(listing_key.clone(), 1);
        index.add_bid_with_time(bid.clone(), &time);

        assert_eq!(index.bids.len(), 1);
        assert_eq!(index.last_updated, 2000);
    }

    #[test]
    fn test_bid_index_no_duplicates() {
        let time = MockTime::new(2000);
        let listing_key = make_test_key();
        let mut index = BidIndex::new(listing_key.clone());

        let bid = make_bid_record(listing_key.clone(), 1);
        index.add_bid_with_time(bid.clone(), &time);
        index.add_bid_with_time(bid.clone(), &time);

        assert_eq!(index.bids.len(), 1);
    }

    #[test]
    fn test_bid_index_sorted_bidders() {
        let time = MockTime::new(2000);
        let listing_key = make_test_key();
        let mut index = BidIndex::new(listing_key.clone());

        // Add bids with different timestamps (all use default timestamp=1000)
        // Since all have same timestamp, falls back to pubkey sort
        index.add_bid_with_time(make_bid_record(listing_key.clone(), 3), &time);
        index.add_bid_with_time(make_bid_record(listing_key.clone(), 1), &time);
        index.add_bid_with_time(make_bid_record(listing_key.clone(), 2), &time);

        let sorted = index.sorted_bidders();
        assert_eq!(sorted.len(), 3);

        // All bids have timestamp=1000, so sorted by pubkey string as tiebreaker
        for i in 0..sorted.len() - 1 {
            assert!(sorted[i].to_string() <= sorted[i + 1].to_string());
        }
    }

    #[test]
    fn test_bid_index_sorted_bidders_by_timestamp() {
        let time = MockTime::new(2000);
        let listing_key = make_test_key();
        let mut index = BidIndex::new(listing_key.clone());

        // Create bids with distinct timestamps
        let mut bid3 = make_bid_record(listing_key.clone(), 3);
        bid3.timestamp = 3000; // Latest
        let mut bid1 = make_bid_record(listing_key.clone(), 1);
        bid1.timestamp = 1000; // Earliest
        let mut bid2 = make_bid_record(listing_key.clone(), 2);
        bid2.timestamp = 2000; // Middle

        index.add_bid_with_time(bid3, &time);
        index.add_bid_with_time(bid1, &time);
        index.add_bid_with_time(bid2, &time);

        let sorted = index.sorted_bidders();
        assert_eq!(sorted.len(), 3);

        // Should be sorted by timestamp: bidder1 (ts=1000), bidder2 (ts=2000), bidder3 (ts=3000)
        assert_eq!(sorted[0], make_bidder(1));
        assert_eq!(sorted[1], make_bidder(2));
        assert_eq!(sorted[2], make_bidder(3));
    }

    #[test]
    fn test_bid_index_get_party_id() {
        let time = MockTime::new(2000);
        let listing_key = make_test_key();
        let mut index = BidIndex::new(listing_key.clone());

        index.add_bid_with_time(make_bid_record(listing_key.clone(), 1), &time);
        index.add_bid_with_time(make_bid_record(listing_key.clone(), 2), &time);
        index.add_bid_with_time(make_bid_record(listing_key.clone(), 3), &time);

        // Each bidder should have a unique party ID
        let id1 = index.get_party_id(&make_bidder(1));
        let id2 = index.get_party_id(&make_bidder(2));
        let id3 = index.get_party_id(&make_bidder(3));

        assert!(id1.is_some());
        assert!(id2.is_some());
        assert!(id3.is_some());

        // All IDs should be distinct
        let ids: std::collections::HashSet<_> = [id1, id2, id3].into_iter().flatten().collect();
        assert_eq!(ids.len(), 3);

        // All IDs should be in range [0, 3)
        for id in ids {
            assert!(id < 3);
        }
    }

    #[test]
    fn test_bid_index_get_party_id_unknown() {
        let listing_key = make_test_key();
        let index = BidIndex::new(listing_key);

        let unknown_bidder = make_bidder(99);
        assert!(index.get_party_id(&unknown_bidder).is_none());
    }

    #[test]
    fn test_bid_index_deterministic_party_ids() {
        let time = MockTime::new(2000);
        let listing_key = make_test_key();

        // Create two indices with same bids added in different order
        // Both bids have same default timestamp (1000), so pubkey tiebreaker applies
        let mut index1 = BidIndex::new(listing_key.clone());
        index1.add_bid_with_time(make_bid_record(listing_key.clone(), 1), &time);
        index1.add_bid_with_time(make_bid_record(listing_key.clone(), 2), &time);

        let mut index2 = BidIndex::new(listing_key.clone());
        index2.add_bid_with_time(make_bid_record(listing_key.clone(), 2), &time);
        index2.add_bid_with_time(make_bid_record(listing_key.clone(), 1), &time);

        // Party IDs should be the same regardless of insertion order
        let bidder1 = make_bidder(1);
        let bidder2 = make_bidder(2);

        assert_eq!(index1.get_party_id(&bidder1), index2.get_party_id(&bidder1));
        assert_eq!(index1.get_party_id(&bidder2), index2.get_party_id(&bidder2));
    }

    #[test]
    fn test_bid_index_deterministic_party_ids_by_timestamp() {
        let time = MockTime::new(5000);
        let listing_key = make_test_key();

        // Create bids with distinct timestamps
        let mut bid1 = make_bid_record(listing_key.clone(), 1);
        bid1.timestamp = 2000;
        let mut bid2 = make_bid_record(listing_key.clone(), 2);
        bid2.timestamp = 1000; // Earlier = lower party ID

        // Add in different orders
        let mut index1 = BidIndex::new(listing_key.clone());
        index1.add_bid_with_time(bid1.clone(), &time);
        index1.add_bid_with_time(bid2.clone(), &time);

        let mut index2 = BidIndex::new(listing_key.clone());
        index2.add_bid_with_time(bid2, &time);
        index2.add_bid_with_time(bid1, &time);

        let bidder1 = make_bidder(1);
        let bidder2 = make_bidder(2);

        assert_eq!(index1.get_party_id(&bidder1), index2.get_party_id(&bidder1));
        assert_eq!(index1.get_party_id(&bidder2), index2.get_party_id(&bidder2));

        // Bidder 2 has earlier timestamp (1000) so should be party 0
        assert_eq!(index1.get_party_id(&bidder2), Some(0));
        assert_eq!(index1.get_party_id(&bidder1), Some(1));
    }

    #[test]
    fn test_bid_index_merge() {
        let time = MockTime::new(2000);
        let listing_key = make_test_key();

        let mut index1 = BidIndex::new(listing_key.clone());
        index1.add_bid_with_time(make_bid_record(listing_key.clone(), 1), &time);

        let mut index2 = BidIndex::new(listing_key.clone());
        index2.add_bid_with_time(make_bid_record(listing_key.clone(), 2), &time);
        index2.add_bid_with_time(make_bid_record(listing_key.clone(), 3), &time);

        index1.merge_with_time(&index2, &time);

        assert_eq!(index1.bids.len(), 3);
    }

    #[test]
    fn test_bid_index_merge_no_duplicates() {
        let time = MockTime::new(2000);
        let listing_key = make_test_key();

        let mut index1 = BidIndex::new(listing_key.clone());
        index1.add_bid_with_time(make_bid_record(listing_key.clone(), 1), &time);
        index1.add_bid_with_time(make_bid_record(listing_key.clone(), 2), &time);

        let mut index2 = BidIndex::new(listing_key.clone());
        index2.add_bid_with_time(make_bid_record(listing_key.clone(), 2), &time); // Duplicate
        index2.add_bid_with_time(make_bid_record(listing_key.clone(), 3), &time);

        index1.merge_with_time(&index2, &time);

        assert_eq!(index1.bids.len(), 3);
    }

    #[test]
    fn test_bid_index_serialization() {
        let time = MockTime::new(2000);
        let listing_key = make_test_key();
        let mut original = BidIndex::new(listing_key.clone());
        original.add_bid_with_time(make_bid_record(listing_key.clone(), 1), &time);
        original.add_bid_with_time(make_bid_record(listing_key.clone(), 2), &time);

        let cbor = original.to_cbor().unwrap();
        let restored = BidIndex::from_cbor(&cbor).unwrap();

        assert_eq!(original.listing_key, restored.listing_key);
        assert_eq!(original.bids.len(), restored.bids.len());
        assert_eq!(original.last_updated, restored.last_updated);
    }
}
