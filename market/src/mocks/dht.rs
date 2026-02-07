//! Mock DHT store for testing.

use crate::traits::DhtStore;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use veilid_core::{PublicKey, RecordKey};

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

#[derive(Debug)]
struct MockDhtInner {
    /// Storage for all records: Map<record_key_string, RecordStorage>
    storage: RwLock<HashMap<String, RecordStorage>>,
    /// Counter for generating unique record IDs.
    next_id: AtomicU64,
    /// Track which node created each record (for ownership simulation).
    record_owners: RwLock<HashMap<String, PublicKey>>,
    /// Whether to simulate failures.
    fail_mode: RwLock<Option<MockDhtFailure>>,
}

/// Mock DHT store for testing.
///
/// Simulates a DHT network where all parties sharing the same underlying
/// `SharedDhtHandle` see the same state. Each `MockDht` carries a `node_id`
/// representing the party using this view.
///
/// For single-party unit tests, use `MockDht::new()` (defaults to party 0).
/// For multi-party integration tests, use `SharedDhtHandle` to create views.
#[derive(Debug, Clone)]
pub struct MockDht {
    /// The underlying shared storage.
    inner: Arc<MockDhtInner>,
    /// The node ID of the party using this DHT view.
    node_id: PublicKey,
}

/// Backward-compatibility alias.
pub type SharedMockDht = MockDht;

/// Handle to shared DHT storage for creating party views.
#[derive(Debug, Clone)]
pub struct SharedDhtHandle {
    inner: Arc<MockDhtInner>,
}

impl SharedDhtHandle {
    /// Create a new shared DHT handle.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MockDhtInner {
                storage: RwLock::new(HashMap::new()),
                next_id: AtomicU64::new(1),
                record_owners: RwLock::new(HashMap::new()),
                fail_mode: RwLock::new(None),
            }),
        }
    }

    /// Create a view of the shared DHT for a specific party.
    pub fn create_party_view(&self, node_id: PublicKey) -> MockDht {
        MockDht {
            inner: self.inner.clone(),
            node_id,
        }
    }
}

impl Default for SharedDhtHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl MockDht {
    /// Create a new single-party mock DHT (uses `make_test_public_key(0)`).
    ///
    /// For multi-party tests, use `SharedDhtHandle` instead.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MockDhtInner {
                storage: RwLock::new(HashMap::new()),
                next_id: AtomicU64::new(1),
                record_owners: RwLock::new(HashMap::new()),
                fail_mode: RwLock::new(None),
            }),
            node_id: make_test_public_key(0),
        }
    }

    /// Create a new mock DHT with a specific node ID.
    ///
    /// Use `SharedDhtHandle` when you need multiple parties to share the same DHT.
    pub fn with_node_id(node_id: PublicKey) -> Self {
        Self {
            inner: Arc::new(MockDhtInner {
                storage: RwLock::new(HashMap::new()),
                next_id: AtomicU64::new(1),
                record_owners: RwLock::new(HashMap::new()),
                fail_mode: RwLock::new(None),
            }),
            node_id,
        }
    }

    /// Get the node ID for this DHT view.
    pub fn node_id(&self) -> PublicKey {
        self.node_id.clone()
    }

    /// Set failure mode for testing error handling.
    pub async fn set_fail_mode(&self, mode: Option<MockDhtFailure>) {
        *self.inner.fail_mode.write().await = mode;
    }

    /// Check if current operation should fail.
    async fn should_fail(&self, is_write: bool, key: Option<&str>) -> bool {
        let mode = self.inner.fail_mode.read().await;
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
        let storage = self.inner.storage.read().await;
        storage
            .iter()
            .map(|(k, v)| (k.clone(), v.subkeys.clone()))
            .collect()
    }

    /// Check if a specific record exists.
    pub async fn has_record(&self, key: &RecordKey) -> bool {
        let storage = self.inner.storage.read().await;
        storage.contains_key(&key.to_string())
    }

    /// Get the number of records stored.
    pub async fn record_count(&self) -> usize {
        self.inner.storage.read().await.len()
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

        let id = self.inner.next_id.fetch_add(1, Ordering::SeqCst);
        let record = MockOwnedRecord::new(id);

        let mut storage = self.inner.storage.write().await;
        storage.insert(record.key.to_string(), RecordStorage::default());

        // Track ownership
        let mut owners = self.inner.record_owners.write().await;
        owners.insert(record.key.to_string(), self.node_id.clone());

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

        let storage = self.inner.storage.read().await;
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

        let mut storage = self.inner.storage.write().await;
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

        let mut storage = self.inner.storage.write().await;
        storage.remove(&key_str);
        Ok(())
    }

    async fn watch_record(&self, key: &RecordKey) -> Result<bool> {
        let key_str = key.to_string();
        if self.should_fail(true, Some(&key_str)).await {
            return Err(anyhow!("MockDht: simulated watch failure"));
        }

        let mut storage = self.inner.storage.write().await;
        if let Some(record) = storage.get_mut(&key_str) {
            let was_watched = record.is_watched;
            record.is_watched = true;
            Ok(!was_watched)
        } else {
            Ok(false)
        }
    }

    async fn cancel_watch(&self, key: &RecordKey) -> Result<bool> {
        let key_str = key.to_string();
        if self.should_fail(true, Some(&key_str)).await {
            return Err(anyhow!("MockDht: simulated cancel_watch failure"));
        }

        let mut storage = self.inner.storage.write().await;
        if let Some(record) = storage.get_mut(&key_str) {
            let was_watched = record.is_watched;
            record.is_watched = false;
            Ok(was_watched)
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
        assert_eq!(
            dht.get_subkey(&key, 0).await.unwrap(),
            Some(b"zero".to_vec())
        );
        assert_eq!(
            dht.get_subkey(&key, 1).await.unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(
            dht.get_subkey(&key, 2).await.unwrap(),
            Some(b"two".to_vec())
        );
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

    #[tokio::test]
    async fn test_shared_mock_dht_multi_party() {
        // Create shared DHT handle
        let handle = SharedDhtHandle::new();

        // Create two party views
        let party1 = handle.create_party_view(make_test_public_key(1));
        let party2 = handle.create_party_view(make_test_public_key(2));

        // Party 1 creates a record
        let record = party1.create_record().await.unwrap();
        let key = MockDht::record_key(&record);

        // Party 1 writes a value
        party1
            .set_value(&record, b"shared data".to_vec())
            .await
            .unwrap();

        // Party 2 can read the value (same underlying storage)
        let value = party2.get_value(&key).await.unwrap();
        assert_eq!(value, Some(b"shared data".to_vec()));

        // Both parties see the same record count
        assert_eq!(party1.record_count().await, 1);
        assert_eq!(party2.record_count().await, 1);
    }

    #[tokio::test]
    async fn test_shared_mock_dht_independent_records() {
        let handle = SharedDhtHandle::new();

        let party1 = handle.create_party_view(make_test_public_key(1));
        let party2 = handle.create_party_view(make_test_public_key(2));

        // Both parties create records
        let record1 = party1.create_record().await.unwrap();
        let record2 = party2.create_record().await.unwrap();

        // Each writes to their own record
        party1
            .set_value(&record1, b"party1 data".to_vec())
            .await
            .unwrap();
        party2
            .set_value(&record2, b"party2 data".to_vec())
            .await
            .unwrap();

        // Both records exist in shared storage
        assert_eq!(party1.record_count().await, 2);

        // Each can read the other's data
        let key1 = MockDht::record_key(&record1);
        let key2 = MockDht::record_key(&record2);

        assert_eq!(
            party2.get_value(&key1).await.unwrap(),
            Some(b"party1 data".to_vec())
        );
        assert_eq!(
            party1.get_value(&key2).await.unwrap(),
            Some(b"party2 data".to_vec())
        );
    }
}
