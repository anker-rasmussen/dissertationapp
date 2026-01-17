use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use veilid_core::RecordKey;

/// Stores bid values locally so we can reveal them during MPC
/// This is kept in memory - in production should be encrypted on disk
#[derive(Clone)]
pub struct BidStorage {
    // Map: listing_key -> (bid_value, nonce, bid_record_key)
    bids: Arc<RwLock<HashMap<String, (u64, [u8; 32], Option<RecordKey>)>>>,
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
        let mut bids = self.bids.write().await;
        // Preserve existing bid_key if present
        let existing_bid_key = bids.get(&key).and_then(|(_, _, k)| k.clone());
        bids.insert(key, (value, nonce, existing_bid_key));
    }

    /// Store the bid record key for later coordination
    pub async fn store_bid_key(&self, listing_key: &RecordKey, bid_key: &RecordKey) {
        let key = listing_key.to_string();
        let mut bids = self.bids.write().await;
        // Clone the existing entry if present
        let existing = bids.get(&key).map(|(v, n, _)| (*v, *n));
        if let Some((value, nonce)) = existing {
            bids.insert(key, (value, nonce, Some(bid_key.clone())));
        } else {
            // Bid value not stored yet, just store the key
            bids.insert(key, (0, [0u8; 32], Some(bid_key.clone())));
        }
    }

    /// Retrieve a bid value for MPC execution
    pub async fn get_bid(&self, listing_key: &RecordKey) -> Option<(u64, [u8; 32])> {
        let key = listing_key.to_string();
        let bids = self.bids.read().await;
        bids.get(&key).map(|(v, n, _)| (*v, *n))
    }

    /// Retrieve the bid record key
    pub async fn get_bid_key(&self, listing_key: &RecordKey) -> Option<RecordKey> {
        let key = listing_key.to_string();
        let bids = self.bids.read().await;
        bids.get(&key).and_then(|(_, _, k)| k.clone())
    }

    /// Check if we have a bid for this listing
    pub async fn has_bid(&self, listing_key: &RecordKey) -> bool {
        let key = listing_key.to_string();
        let bids = self.bids.read().await;
        bids.contains_key(&key)
    }
}

impl Default for BidStorage {
    fn default() -> Self {
        Self::new()
    }
}
