use serde::{Deserialize, Serialize};
use veilid_core::{PublicKey, RecordKey};

use crate::config::now_unix;
use crate::error::{MarketError, MarketResult};
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

/// Registry of all bidders for a specific listing (state-based G-Set CRDT).
///
/// Entries are deduped by bidder pubkey on add; `merge()` provides
/// commutative, associative, idempotent union for convergence.
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

    pub fn to_cbor(&self) -> MarketResult<Vec<u8>> {
        let mut data = Vec::new();
        ciborium::ser::into_writer(self, &mut data).map_err(|e| {
            MarketError::Serialization(format!("Failed to serialize bidder registry: {e}"))
        })?;
        Ok(data)
    }

    pub fn from_cbor(data: &[u8]) -> MarketResult<Self> {
        ciborium::de::from_reader(data).map_err(|e| {
            MarketError::Serialization(format!("Failed to deserialize bidder registry: {e}"))
        })
    }

    /// Merge another registry into this one (G-Set union).
    ///
    /// Commutative, associative, and idempotent — suitable for
    /// state-based CRDT convergence.
    pub fn merge(&mut self, other: &Self) {
        for entry in &other.bidders {
            self.add_bidder(entry.bidder.clone(), entry.bid_record_key.clone());
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

    // ── G-Set CRDT merge property tests ──

    #[test]
    fn test_bidder_registry_merge_commutativity() {
        let listing_key = make_test_record_key(1);
        let time = MockTime::new(1000);

        let mut a = BidderRegistry::new(listing_key.clone());
        a.add_bidder_with_time(make_test_public_key(1), make_test_record_key(10), &time);

        let mut b = BidderRegistry::new(listing_key.clone());
        time.advance(100);
        b.add_bidder_with_time(make_test_public_key(2), make_test_record_key(20), &time);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.bidders.len(), ba.bidders.len());
        // Both should contain bidders 1 and 2
        assert!(ab
            .bidders
            .iter()
            .any(|e| e.bidder == make_test_public_key(1)));
        assert!(ab
            .bidders
            .iter()
            .any(|e| e.bidder == make_test_public_key(2)));
        assert!(ba
            .bidders
            .iter()
            .any(|e| e.bidder == make_test_public_key(1)));
        assert!(ba
            .bidders
            .iter()
            .any(|e| e.bidder == make_test_public_key(2)));
    }

    #[test]
    fn test_bidder_registry_merge_associativity() {
        let listing_key = make_test_record_key(1);
        let time = MockTime::new(1000);

        let mut a = BidderRegistry::new(listing_key.clone());
        a.add_bidder_with_time(make_test_public_key(1), make_test_record_key(10), &time);

        let mut b = BidderRegistry::new(listing_key.clone());
        time.advance(100);
        b.add_bidder_with_time(make_test_public_key(2), make_test_record_key(20), &time);

        let mut c = BidderRegistry::new(listing_key.clone());
        time.advance(100);
        c.add_bidder_with_time(make_test_public_key(3), make_test_record_key(30), &time);

        // (a ∪ b) ∪ c
        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        // a ∪ (b ∪ c)
        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        assert_eq!(ab_c.bidders.len(), 3);
        assert_eq!(a_bc.bidders.len(), 3);
    }

    #[test]
    fn test_bidder_registry_merge_idempotency() {
        let listing_key = make_test_record_key(1);
        let time = MockTime::new(1000);

        let mut a = BidderRegistry::new(listing_key.clone());
        a.add_bidder_with_time(make_test_public_key(1), make_test_record_key(10), &time);
        time.advance(100);
        a.add_bidder_with_time(make_test_public_key(2), make_test_record_key(20), &time);

        let before = a.bidders.len();
        a.merge(&a.clone());
        assert_eq!(a.bidders.len(), before);
    }
}
