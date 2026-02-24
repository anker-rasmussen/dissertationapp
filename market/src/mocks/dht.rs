//! Mock DHT store for testing.

use crate::error::{MarketError, MarketResult};
use crate::traits::DhtStore;
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
    for (j, byte) in key_bytes[8..32].iter_mut().enumerate() {
        *byte = ((id >> ((j % 8) * 8)) & 0xFF) as u8;
    }

    let encoded = data_encoding::BASE64URL_NOPAD.encode(&key_bytes);
    let key_str = format!("VLD0:{encoded}");
    #[allow(clippy::expect_used)]
    RecordKey::try_from(key_str.as_str()).expect("Should create valid RecordKey")
}

/// Create a test public key for mock purposes.
pub fn make_test_public_key(id: u8) -> veilid_core::PublicKey {
    let mut key_bytes = [id; 32];
    // Make it slightly different to avoid collisions
    key_bytes[0] = id;
    key_bytes[1] = id.wrapping_add(1);

    let encoded = data_encoding::BASE64URL_NOPAD.encode(&key_bytes);
    let key_str = format!("VLD0:{encoded}");
    #[allow(clippy::expect_used)]
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
            Some(MockDhtFailure::OnKey(k)) => key.is_some_and(|key| key == k),
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

    async fn create_record(&self) -> MarketResult<Self::OwnedRecord> {
        if self.should_fail(true, None).await {
            return Err(MarketError::Dht("MockDht: simulated create failure".into()));
        }

        let id = self.inner.next_id.fetch_add(1, Ordering::SeqCst);
        let record = MockOwnedRecord::new(id);

        self.inner
            .storage
            .write()
            .await
            .insert(record.key.to_string(), RecordStorage::default());

        // Track ownership
        self.inner
            .record_owners
            .write()
            .await
            .insert(record.key.to_string(), self.node_id.clone());

        Ok(record)
    }

    fn record_key(record: &Self::OwnedRecord) -> RecordKey {
        record.key.clone()
    }

    async fn get_value(&self, key: &RecordKey) -> MarketResult<Option<Vec<u8>>> {
        self.get_subkey(key, 0).await
    }

    async fn set_value(&self, record: &Self::OwnedRecord, value: Vec<u8>) -> MarketResult<()> {
        self.set_subkey(record, 0, value).await
    }

    async fn get_subkey(&self, key: &RecordKey, subkey: u32) -> MarketResult<Option<Vec<u8>>> {
        let key_str = key.to_string();
        if self.should_fail(false, Some(&key_str)).await {
            return Err(MarketError::Dht("MockDht: simulated read failure".into()));
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
    ) -> MarketResult<()> {
        let key_str = record.key.to_string();
        if self.should_fail(true, Some(&key_str)).await {
            return Err(MarketError::Dht("MockDht: simulated write failure".into()));
        }

        // Verify ownership: only the creator can write to a record
        {
            let owners = self.inner.record_owners.read().await;
            if let Some(owner) = owners.get(&key_str) {
                if owner != &self.node_id {
                    return Err(MarketError::Dht(format!(
                        "MockDht: node {} does not own record {} (owned by {})",
                        self.node_id, key_str, owner
                    )));
                }
            }
        }

        // Validate bounds before acquiring write lock
        if subkey >= u32::from(crate::config::DHT_SUBKEY_COUNT) {
            return Err(MarketError::Dht(format!(
                "MockDht: subkey {} exceeds max {} for record {}",
                subkey,
                crate::config::DHT_SUBKEY_COUNT - 1,
                key_str
            )));
        }
        if value.len() > crate::traits::dht::MAX_DHT_VALUE_SIZE {
            return Err(MarketError::Dht(format!(
                "MockDht: value size {} exceeds max {} bytes",
                value.len(),
                crate::traits::dht::MAX_DHT_VALUE_SIZE
            )));
        }

        // Reject writes to nonexistent records
        let mut storage = self.inner.storage.write().await;
        let record_storage = storage
            .get_mut(&key_str)
            .ok_or_else(|| MarketError::Dht(format!("MockDht: record {key_str} does not exist")))?;
        record_storage.subkeys.insert(subkey, value);
        drop(storage);

        Ok(())
    }

    async fn delete_record(&self, key: &RecordKey) -> MarketResult<()> {
        let key_str = key.to_string();
        if self.should_fail(true, Some(&key_str)).await {
            return Err(MarketError::Dht("MockDht: simulated delete failure".into()));
        }

        self.inner.storage.write().await.remove(&key_str);
        Ok(())
    }

    async fn watch_record(&self, key: &RecordKey) -> MarketResult<bool> {
        let key_str = key.to_string();
        if self.should_fail(true, Some(&key_str)).await {
            return Err(MarketError::Dht("MockDht: simulated watch failure".into()));
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

    async fn cancel_watch(&self, key: &RecordKey) -> MarketResult<bool> {
        let key_str = key.to_string();
        if self.should_fail(true, Some(&key_str)).await {
            return Err(MarketError::Dht(
                "MockDht: simulated cancel_watch failure".into(),
            ));
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
