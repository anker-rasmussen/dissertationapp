use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use veilid_core::RecordKey;

/// Per-listing bid data: optional (value, nonce) and optional DHT record key.
/// The value/nonce are `None` until the actual bid is placed via `store_bid()`.
#[derive(Clone)]
struct BidEntry {
    value: Option<(u64, [u8; 32])>,
    bid_key: Option<RecordKey>,
}

type BidMap = Arc<RwLock<HashMap<String, BidEntry>>>;

/// Stores bid values locally so we can reveal them during MPC
/// This is kept in memory - in production should be encrypted on disk
#[derive(Clone)]
pub struct BidStorage {
    bids: BidMap,
}

impl BidStorage {
    pub fn new() -> Self {
        Self {
            bids: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Store a bid value for later reveal
    pub async fn store_bid(&self, listing_key: &RecordKey, value: u64, nonce: [u8; 32]) {
        let key = listing_key.to_string();
        self.bids
            .write()
            .await
            .entry(key)
            .or_insert_with(|| BidEntry {
                value: None,
                bid_key: None,
            })
            .value = Some((value, nonce));
    }

    /// Store the bid record key for later coordination
    pub async fn store_bid_key(&self, listing_key: &RecordKey, bid_key: &RecordKey) {
        let key = listing_key.to_string();
        self.bids
            .write()
            .await
            .entry(key)
            .or_insert_with(|| BidEntry {
                value: None,
                bid_key: None,
            })
            .bid_key = Some(bid_key.clone());
    }

    /// Retrieve a bid value for MPC execution.
    /// Returns `None` if only the bid key has been stored (no actual value yet).
    pub async fn get_bid(&self, listing_key: &RecordKey) -> Option<(u64, [u8; 32])> {
        let key = listing_key.to_string();
        let bids = self.bids.read().await;
        bids.get(&key).and_then(|e| e.value)
    }

    /// Retrieve the bid record key
    pub async fn get_bid_key(&self, listing_key: &RecordKey) -> Option<RecordKey> {
        let key = listing_key.to_string();
        let bids = self.bids.read().await;
        bids.get(&key).and_then(|e| e.bid_key.clone())
    }

    /// Check if we have a bid value for this listing.
    /// Returns `false` if only the bid key has been stored.
    pub async fn has_bid(&self, listing_key: &RecordKey) -> bool {
        let key = listing_key.to_string();
        let bids = self.bids.read().await;
        bids.get(&key).is_some_and(|e| e.value.is_some())
    }
}

impl Default for BidStorage {
    fn default() -> Self {
        Self::new()
    }
}
