//! Mock DHT store for testing.

use crate::traits::DhtStore;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use veilid_core::RecordKey;

/// A mock DHT record with a unique key.
#[derive(Debug, Clone)]
pub struct MockOwnedRecord {
    /// The record key.
    pub key: RecordKey,
}

impl MockOwnedRecord {
    fn new(id: u64) -> Self {
        // Create a deterministic RecordKey from the id
        Self {
            key: make_test_record_key(id),
        }
    }
}

/// Create a test record key for mock purposes.
/// Uses the VLD0 crypto kind with deterministic bytes.
pub fn make_test_record_key(id: u64) -> RecordKey {
    // Create a key string in the format expected by veilid
    // VLD0:base64url_encoded_32_bytes
    let mut key_bytes = [0u8; 32];
    key_bytes[..8].copy_from_slice(&id.to_le_bytes());
    // Fill rest with a pattern
    for i in 8..32 {
        key_bytes[i] = ((id >> ((i % 8) * 8)) & 0xFF) as u8;
    }

    let encoded = data_encoding::BASE64URL_NOPAD.encode(&key_bytes);
    let key_str = format!("VLD0:{}", encoded);
    RecordKey::try_from(key_str.as_str()).expect("Should create valid RecordKey")
}

/// Create a test public key for mock purposes.
pub fn make_test_public_key(id: u8) -> veilid_core::PublicKey {
    let mut key_bytes = [id; 32];
    // Make it slightly different to avoid collisions
    key_bytes[0] = id;
    key_bytes[1] = id.wrapping_add(1);

    let encoded = data_encoding::BASE64URL_NOPAD.encode(&key_bytes);
    let key_str = format!("VLD0:{}", encoded);
    veilid_core::PublicKey::try_from(key_str.as_str()).expect("Should create valid PublicKey")
}

/// Storage for a single DHT record with multiple subkeys.
#[derive(Debug, Clone, Default)]
struct RecordStorage {
    subkeys: HashMap<u32, Vec<u8>>,
    is_watched: bool,
}

/// Mock DHT store for testing DHT operations.
#[derive(Debug, Clone)]
pub struct MockDht {
    /// Storage for all records: Map<record_key_string, RecordStorage>
    storage: Arc<RwLock<HashMap<String, RecordStorage>>>,
    /// Counter for generating unique record IDs.
    next_id: Arc<AtomicU64>,
    /// Whether to simulate failures.
    fail_mode: Arc<RwLock<Option<MockDhtFailure>>>,
}

/// Types of failures that can be simulated.
#[derive(Debug, Clone)]
pub enum MockDhtFailure {
    /// Fail all operations.
    All,
    /// Fail only read operations.
    Reads,
    /// Fail only write operations.
    Writes,
    /// Fail on specific record key.
    OnKey(String),
}

impl MockDht {
    /// Create a new mock DHT store.
    pub fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
            fail_mode: Arc::new(RwLock::new(None)),
        }
    }

    /// Set failure mode for testing error handling.
    pub async fn set_fail_mode(&self, mode: Option<MockDhtFailure>) {
        *self.fail_mode.write().await = mode;
    }

    /// Check if current operation should fail.
    async fn should_fail(&self, is_write: bool, key: Option<&str>) -> bool {
        let mode = self.fail_mode.read().await;
        match &*mode {
            None => false,
            Some(MockDhtFailure::All) => true,
            Some(MockDhtFailure::Reads) => !is_write,
            Some(MockDhtFailure::Writes) => is_write,
            Some(MockDhtFailure::OnKey(k)) => key.map(|key| key == k).unwrap_or(false),
        }
    }

    /// Get a snapshot of all stored data (for test assertions).
    pub async fn snapshot(&self) -> HashMap<String, HashMap<u32, Vec<u8>>> {
        let storage = self.storage.read().await;
        storage
            .iter()
            .map(|(k, v)| (k.clone(), v.subkeys.clone()))
            .collect()
    }

    /// Check if a specific record exists.
    pub async fn has_record(&self, key: &RecordKey) -> bool {
        let storage = self.storage.read().await;
        storage.contains_key(&key.to_string())
    }

    /// Get the number of records stored.
    pub async fn record_count(&self) -> usize {
        self.storage.read().await.len()
    }
}

impl Default for MockDht {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DhtStore for MockDht {
    type OwnedRecord = MockOwnedRecord;

    async fn create_record(&self) -> Result<Self::OwnedRecord> {
        if self.should_fail(true, None).await {
            return Err(anyhow!("MockDht: simulated create failure"));
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let record = MockOwnedRecord::new(id);

        let mut storage = self.storage.write().await;
        storage.insert(record.key.to_string(), RecordStorage::default());

        Ok(record)
    }

    fn record_key(record: &Self::OwnedRecord) -> RecordKey {
        record.key.clone()
    }

    async fn get_value(&self, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        self.get_subkey(key, 0).await
    }

    async fn set_value(&self, record: &Self::OwnedRecord, value: Vec<u8>) -> Result<()> {
        self.set_subkey(record, 0, value).await
    }

    async fn get_subkey(&self, key: &RecordKey, subkey: u32) -> Result<Option<Vec<u8>>> {
        let key_str = key.to_string();
        if self.should_fail(false, Some(&key_str)).await {
            return Err(anyhow!("MockDht: simulated read failure"));
        }

        let storage = self.storage.read().await;
        Ok(storage
            .get(&key_str)
            .and_then(|r| r.subkeys.get(&subkey).cloned()))
    }

    async fn set_subkey(
        &self,
        record: &Self::OwnedRecord,
        subkey: u32,
        value: Vec<u8>,
    ) -> Result<()> {
        let key_str = record.key.to_string();
        if self.should_fail(true, Some(&key_str)).await {
            return Err(anyhow!("MockDht: simulated write failure"));
        }

        let mut storage = self.storage.write().await;
        let record_storage = storage
            .entry(key_str)
            .or_insert_with(RecordStorage::default);
        record_storage.subkeys.insert(subkey, value);

        Ok(())
    }

    async fn delete_record(&self, key: &RecordKey) -> Result<()> {
        let key_str = key.to_string();
        if self.should_fail(true, Some(&key_str)).await {
            return Err(anyhow!("MockDht: simulated delete failure"));
        }

        let mut storage = self.storage.write().await;
        storage.remove(&key_str);
        Ok(())
    }

    async fn watch_record(&self, key: &RecordKey) -> Result<bool> {
        let key_str = key.to_string();
        if self.should_fail(true, Some(&key_str)).await {
            return Err(anyhow!("MockDht: simulated watch failure"));
        }

        let mut storage = self.storage.write().await;
        if let Some(record) = storage.get_mut(&key_str) {
            let was_watched = record.is_watched;
            record.is_watched = true;
            Ok(!was_watched) // Return true if this is a new watch
        } else {
            Ok(false)
        }
    }

    async fn cancel_watch(&self, key: &RecordKey) -> Result<bool> {
        let key_str = key.to_string();
        if self.should_fail(true, Some(&key_str)).await {
            return Err(anyhow!("MockDht: simulated cancel_watch failure"));
        }

        let mut storage = self.storage.write().await;
        if let Some(record) = storage.get_mut(&key_str) {
            let was_watched = record.is_watched;
            record.is_watched = false;
            Ok(was_watched) // Return true if watch was active
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_dht_create_and_get() {
        let dht = MockDht::new();

        let record = dht.create_record().await.unwrap();
        let key = MockDht::record_key(&record);

        // Initially empty
        let value = dht.get_value(&key).await.unwrap();
        assert!(value.is_none());

        // Set a value
        dht.set_value(&record, b"hello".to_vec()).await.unwrap();

        // Should now have the value
        let value = dht.get_value(&key).await.unwrap();
        assert_eq!(value, Some(b"hello".to_vec()));
    }

    #[tokio::test]
    async fn test_mock_dht_subkeys() {
        let dht = MockDht::new();
        let record = dht.create_record().await.unwrap();
        let key = MockDht::record_key(&record);

        // Set values at different subkeys
        dht.set_subkey(&record, 0, b"zero".to_vec()).await.unwrap();
        dht.set_subkey(&record, 1, b"one".to_vec()).await.unwrap();
        dht.set_subkey(&record, 2, b"two".to_vec()).await.unwrap();

        // Read them back
        assert_eq!(dht.get_subkey(&key, 0).await.unwrap(), Some(b"zero".to_vec()));
        assert_eq!(dht.get_subkey(&key, 1).await.unwrap(), Some(b"one".to_vec()));
        assert_eq!(dht.get_subkey(&key, 2).await.unwrap(), Some(b"two".to_vec()));
        assert_eq!(dht.get_subkey(&key, 3).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_mock_dht_delete() {
        let dht = MockDht::new();
        let record = dht.create_record().await.unwrap();
        let key = MockDht::record_key(&record);

        dht.set_value(&record, b"hello".to_vec()).await.unwrap();
        assert!(dht.has_record(&key).await);

        dht.delete_record(&key).await.unwrap();
        assert!(!dht.has_record(&key).await);
    }

    #[tokio::test]
    async fn test_mock_dht_watch() {
        let dht = MockDht::new();
        let record = dht.create_record().await.unwrap();
        let key = MockDht::record_key(&record);

        // First watch should return true (new watch)
        assert!(dht.watch_record(&key).await.unwrap());

        // Second watch should return false (already watching)
        assert!(!dht.watch_record(&key).await.unwrap());

        // Cancel should return true (was watching)
        assert!(dht.cancel_watch(&key).await.unwrap());

        // Cancel again should return false (not watching)
        assert!(!dht.cancel_watch(&key).await.unwrap());
    }

    #[tokio::test]
    async fn test_mock_dht_fail_mode() {
        let dht = MockDht::new();

        // Normal operation works
        let record = dht.create_record().await.unwrap();

        // Enable failure mode
        dht.set_fail_mode(Some(MockDhtFailure::All)).await;

        // Operations should fail
        assert!(dht.create_record().await.is_err());
        assert!(dht.get_value(&MockDht::record_key(&record)).await.is_err());

        // Disable failure mode
        dht.set_fail_mode(None).await;

        // Operations work again
        assert!(dht.get_value(&MockDht::record_key(&record)).await.is_ok());
    }

    #[tokio::test]
    async fn test_mock_dht_fail_reads_only() {
        let dht = MockDht::new();
        let record = dht.create_record().await.unwrap();
        let key = MockDht::record_key(&record);

        dht.set_fail_mode(Some(MockDhtFailure::Reads)).await;

        // Writes should work
        assert!(dht.set_value(&record, b"test".to_vec()).await.is_ok());

        // Reads should fail
        assert!(dht.get_value(&key).await.is_err());
    }
}
