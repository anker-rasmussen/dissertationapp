//! DHT storage abstraction for testable DHT operations.

use anyhow::Result;
use async_trait::async_trait;
use veilid_core::RecordKey;

/// Abstraction over DHT storage operations.
///
/// This trait enables testing of DHT-dependent code without requiring
/// a running Veilid node or network connection.
#[async_trait]
pub trait DhtStore: Send + Sync + Clone {
    /// The type representing an owned DHT record with write access.
    type OwnedRecord: Send + Sync + Clone;

    /// Create a new DHT record.
    ///
    /// Returns an owned record that can be used for write operations.
    async fn create_record(&self) -> Result<Self::OwnedRecord>;

    /// Get the record key from an owned record.
    fn record_key(record: &Self::OwnedRecord) -> RecordKey;

    /// Get a value from a DHT record at subkey 0.
    ///
    /// Returns `None` if the value hasn't been set yet.
    async fn get_value(&self, key: &RecordKey) -> Result<Option<Vec<u8>>>;

    /// Set a value in a DHT record at subkey 0.
    ///
    /// Requires write access via the owned record.
    async fn set_value(&self, record: &Self::OwnedRecord, value: Vec<u8>) -> Result<()>;

    /// Get a value from a specific subkey of a DHT record.
    ///
    /// Returns `None` if the value hasn't been set yet.
    async fn get_subkey(&self, key: &RecordKey, subkey: u32) -> Result<Option<Vec<u8>>>;

    /// Set a value at a specific subkey of a DHT record.
    ///
    /// Requires write access via the owned record.
    async fn set_subkey(
        &self,
        record: &Self::OwnedRecord,
        subkey: u32,
        value: Vec<u8>,
    ) -> Result<()>;

    /// Delete a DHT record.
    async fn delete_record(&self, key: &RecordKey) -> Result<()>;

    /// Watch a DHT record for changes.
    ///
    /// Returns `true` if watch was successfully established.
    async fn watch_record(&self, key: &RecordKey) -> Result<bool>;

    /// Cancel watching a DHT record.
    ///
    /// Returns `true` if watch was successfully cancelled.
    async fn cancel_watch(&self, key: &RecordKey) -> Result<bool>;
}
