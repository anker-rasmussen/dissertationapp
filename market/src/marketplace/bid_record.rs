use serde::{Deserialize, Serialize};
use veilid_core::{PublicKey, RecordKey};

use crate::config::now_unix;
use crate::error::MarketError;
use crate::traits::TimeProvider;

/// A bid record published to the DHT for auction participation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    /// Ed25519 signing public key for message authentication.
    /// Only the BidRecord owner can write this; verified against
    /// the `SignedEnvelope.signer` field on incoming messages.
    #[serde(default)]
    pub signing_pubkey: [u8; 32],
}

impl BidRecord {
    /// Serialize to CBOR for DHT storage
    pub fn to_cbor(&self) -> Result<Vec<u8>, MarketError> {
        let mut data = Vec::new();
        ciborium::ser::into_writer(self, &mut data).map_err(|e| {
            MarketError::Serialization(format!("Failed to serialize bid record: {e}"))
        })?;
        Ok(data)
    }

    /// Deserialize from CBOR
    pub fn from_cbor(data: &[u8]) -> Result<Self, MarketError> {
        crate::util::cbor_from_limited_reader(data, crate::util::MAX_DHT_VALUE_SIZE)
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

    /// Get bidders sorted deterministically by pubkey.
    /// Seller (matching seller_pubkey) is always party 0.
    /// Remaining bidders are sorted by pubkey bytes (lexicographic).
    pub fn sorted_bidders(&self, seller: &PublicKey) -> Vec<PublicKey> {
        // Partition: seller vs non-seller bids
        let (seller_bids, mut non_seller_bids): (Vec<_>, Vec<_>) =
            self.bids.iter().partition(|b| &b.bidder == seller);

        // Sort non-seller bids by pubkey only
        non_seller_bids.sort_by(|a, b| a.bidder.cmp(&b.bidder));

        // Return seller first, then sorted non-sellers
        if let Some(seller_bid) = seller_bids.first() {
            let mut result = vec![seller_bid.bidder.clone()];
            result.extend(non_seller_bids.into_iter().map(|b| b.bidder.clone()));
            result
        } else {
            // No seller bid found, just sort all by pubkey
            let mut all_bids: Vec<_> = self.bids.iter().collect();
            all_bids.sort_by(|a, b| a.bidder.cmp(&b.bidder));
            all_bids.into_iter().map(|b| b.bidder.clone()).collect()
        }
    }

    /// Get party ID for a specific bidder
    pub fn get_party_id(&self, bidder: &PublicKey, seller: &PublicKey) -> Option<usize> {
        let sorted = self.sorted_bidders(seller);
        sorted.iter().position(|b| b == bidder)
    }

    pub fn to_cbor(&self) -> Result<Vec<u8>, MarketError> {
        let mut data = Vec::new();
        ciborium::ser::into_writer(self, &mut data).map_err(|e| {
            MarketError::Serialization(format!("Failed to serialize bid index: {e}"))
        })?;
        Ok(data)
    }

    pub fn from_cbor(data: &[u8]) -> Result<Self, MarketError> {
        crate::util::cbor_from_limited_reader(data, crate::util::MAX_DHT_VALUE_SIZE)
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
            signing_pubkey: [bidder_id; 32],
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

        // Add bids (seller is bidder 1)
        index.add_bid_with_time(make_bid_record(listing_key.clone(), 3), &time);
        index.add_bid_with_time(make_bid_record(listing_key.clone(), 1), &time);
        index.add_bid_with_time(make_bid_record(listing_key.clone(), 2), &time);

        let seller = make_bidder(1);
        let sorted = index.sorted_bidders(&seller);
        assert_eq!(sorted.len(), 3);

        // Seller should be first (party 0)
        assert_eq!(sorted[0], make_bidder(1));

        // Remaining bidders sorted by pubkey
        for i in 1..sorted.len() - 1 {
            assert!(sorted[i] <= sorted[i + 1]);
        }
    }

    #[test]
    fn test_bid_index_sorted_bidders_by_pubkey() {
        let time = MockTime::new(2000);
        let listing_key = make_test_key();
        let mut index = BidIndex::new(listing_key.clone());

        // Create bids with distinct timestamps (timestamps are now ignored)
        let mut bid3 = make_bid_record(listing_key.clone(), 3);
        bid3.timestamp = 3000; // Latest timestamp (ignored)
        let mut bid1 = make_bid_record(listing_key.clone(), 1);
        bid1.timestamp = 1000; // Earliest timestamp (ignored)
        let mut bid2 = make_bid_record(listing_key.clone(), 2);
        bid2.timestamp = 2000; // Middle timestamp (ignored)

        index.add_bid_with_time(bid3, &time);
        index.add_bid_with_time(bid1, &time);
        index.add_bid_with_time(bid2, &time);

        // Seller is bidder 1
        let seller = make_bidder(1);
        let sorted = index.sorted_bidders(&seller);
        assert_eq!(sorted.len(), 3);

        // Seller should be party 0 regardless of timestamp
        assert_eq!(sorted[0], make_bidder(1));

        // Remaining bidders sorted by pubkey only
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

        let seller = make_bidder(1);

        // Each bidder should have a unique party ID
        let id1 = index.get_party_id(&make_bidder(1), &seller);
        let id2 = index.get_party_id(&make_bidder(2), &seller);
        let id3 = index.get_party_id(&make_bidder(3), &seller);

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

        let seller = make_bidder(1);
        let unknown_bidder = make_bidder(99);
        assert!(index.get_party_id(&unknown_bidder, &seller).is_none());
    }

    #[test]
    fn test_bid_index_deterministic_party_ids() {
        let time = MockTime::new(2000);
        let listing_key = make_test_key();

        // Create two indices with same bids added in different order
        let mut index1 = BidIndex::new(listing_key.clone());
        index1.add_bid_with_time(make_bid_record(listing_key.clone(), 1), &time);
        index1.add_bid_with_time(make_bid_record(listing_key.clone(), 2), &time);

        let mut index2 = BidIndex::new(listing_key.clone());
        index2.add_bid_with_time(make_bid_record(listing_key.clone(), 2), &time);
        index2.add_bid_with_time(make_bid_record(listing_key.clone(), 1), &time);

        // Party IDs should be the same regardless of insertion order
        let seller = make_bidder(1);
        let bidder1 = make_bidder(1);
        let bidder2 = make_bidder(2);

        assert_eq!(
            index1.get_party_id(&bidder1, &seller),
            index2.get_party_id(&bidder1, &seller)
        );
        assert_eq!(
            index1.get_party_id(&bidder2, &seller),
            index2.get_party_id(&bidder2, &seller)
        );
    }

    #[test]
    fn test_bid_index_deterministic_party_ids_seller_first() {
        let time = MockTime::new(5000);
        let listing_key = make_test_key();

        // Create bids with distinct timestamps (timestamps now ignored)
        let mut bid1 = make_bid_record(listing_key.clone(), 1);
        bid1.timestamp = 2000; // Later timestamp (ignored)
        let mut bid2 = make_bid_record(listing_key.clone(), 2);
        bid2.timestamp = 1000; // Earlier timestamp (ignored)

        // Add in different orders
        let mut index1 = BidIndex::new(listing_key.clone());
        index1.add_bid_with_time(bid1.clone(), &time);
        index1.add_bid_with_time(bid2.clone(), &time);

        let mut index2 = BidIndex::new(listing_key.clone());
        index2.add_bid_with_time(bid2, &time);
        index2.add_bid_with_time(bid1, &time);

        let seller = make_bidder(1);
        let bidder1 = make_bidder(1);
        let bidder2 = make_bidder(2);

        assert_eq!(
            index1.get_party_id(&bidder1, &seller),
            index2.get_party_id(&bidder1, &seller)
        );
        assert_eq!(
            index1.get_party_id(&bidder2, &seller),
            index2.get_party_id(&bidder2, &seller)
        );

        // Seller (bidder 1) should always be party 0, regardless of timestamp
        assert_eq!(index1.get_party_id(&bidder1, &seller), Some(0));
        assert_eq!(index1.get_party_id(&bidder2, &seller), Some(1));
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
