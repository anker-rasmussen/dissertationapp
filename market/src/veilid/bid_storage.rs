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

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a test RecordKey from an id.
    fn make_test_record_key(id: u64) -> RecordKey {
        let mut key_bytes = [0u8; 32];
        key_bytes[..8].copy_from_slice(&id.to_le_bytes());
        for i in 8..32 {
            key_bytes[i] = ((id >> ((i % 8) * 8)) & 0xFF) as u8;
        }
        let encoded = data_encoding::BASE64URL_NOPAD.encode(&key_bytes);
        let key_str = format!("VLD0:{}", encoded);
        RecordKey::try_from(key_str.as_str()).expect("Should create valid RecordKey")
    }

    #[tokio::test]
    async fn test_store_and_get_bid() {
        let storage = BidStorage::new();
        let listing_key = make_test_record_key(1);
        let bid_value = 1000u64;
        let nonce = [42u8; 32];

        storage.store_bid(&listing_key, bid_value, nonce).await;

        let result = storage.get_bid(&listing_key).await;
        assert_eq!(result, Some((bid_value, nonce)));
    }

    #[tokio::test]
    async fn test_has_bid_true() {
        let storage = BidStorage::new();
        let listing_key = make_test_record_key(2);
        let bid_value = 2000u64;
        let nonce = [99u8; 32];

        storage.store_bid(&listing_key, bid_value, nonce).await;

        assert!(storage.has_bid(&listing_key).await);
    }

    #[tokio::test]
    async fn test_has_bid_false() {
        let storage = BidStorage::new();
        let listing_key = make_test_record_key(3);

        assert!(!storage.has_bid(&listing_key).await);
    }

    #[tokio::test]
    async fn test_overwrite_bid() {
        let storage = BidStorage::new();
        let listing_key = make_test_record_key(4);
        let first_value = 500u64;
        let first_nonce = [1u8; 32];
        let second_value = 1500u64;
        let second_nonce = [2u8; 32];

        storage
            .store_bid(&listing_key, first_value, first_nonce)
            .await;
        storage
            .store_bid(&listing_key, second_value, second_nonce)
            .await;

        let result = storage.get_bid(&listing_key).await;
        assert_eq!(result, Some((second_value, second_nonce)));
    }

    #[tokio::test]
    async fn test_store_key_and_get() {
        let storage = BidStorage::new();
        let listing_key = make_test_record_key(5);
        let bid_key = make_test_record_key(100);

        storage.store_bid_key(&listing_key, &bid_key).await;

        let result = storage.get_bid_key(&listing_key).await;
        assert_eq!(result, Some(bid_key));
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let storage = BidStorage::new();
        let listing_key = make_test_record_key(6);

        let result = storage.get_bid(&listing_key).await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_store_key_only_has_bid_false() {
        let storage = BidStorage::new();
        let listing_key = make_test_record_key(7);
        let bid_key = make_test_record_key(101);

        // Store only the bid key, not the bid value
        storage.store_bid_key(&listing_key, &bid_key).await;

        // has_bid should return false since we only have the key
        assert!(!storage.has_bid(&listing_key).await);
        // But get_bid_key should work
        assert_eq!(storage.get_bid_key(&listing_key).await, Some(bid_key));
    }

    #[tokio::test]
    async fn test_store_both_bid_and_key() {
        let storage = BidStorage::new();
        let listing_key = make_test_record_key(8);
        let bid_value = 3000u64;
        let nonce = [77u8; 32];
        let bid_key = make_test_record_key(102);

        // Store both bid and key
        storage.store_bid(&listing_key, bid_value, nonce).await;
        storage.store_bid_key(&listing_key, &bid_key).await;

        // Both should be retrievable
        assert_eq!(
            storage.get_bid(&listing_key).await,
            Some((bid_value, nonce))
        );
        assert_eq!(storage.get_bid_key(&listing_key).await, Some(bid_key));
        assert!(storage.has_bid(&listing_key).await);
    }

    #[tokio::test]
    async fn test_get_bid_key_nonexistent() {
        let storage = BidStorage::new();
        let listing_key = make_test_record_key(9);

        let result = storage.get_bid_key(&listing_key).await;
        assert_eq!(result, None);
    }
}
