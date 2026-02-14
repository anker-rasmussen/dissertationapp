use async_trait::async_trait;
use tracing::{debug, info};
use veilid_core::{
    DHTRecordDescriptor, DHTSchema, KeyPair, RecordKey, ValueSubkeyRangeSet, VeilidAPI,
    CRYPTO_KIND_VLD0,
};

use crate::config::DHT_SUBKEY_COUNT;
use crate::error::{MarketError, MarketResult};
use crate::traits::DhtStore;

/// DHT operations wrapper for Veilid.
///
/// Always uses Veilid's default safe routing — both in devnet and production.
#[derive(Clone)]
pub struct DHTOperations {
    api: VeilidAPI,
}

/// A DHT record with its owner keypair for write access
#[derive(Clone)]
pub struct OwnedDHTRecord {
    pub key: RecordKey,
    pub owner: KeyPair,
}

impl DHTOperations {
    pub const fn new(api: VeilidAPI) -> Self {
        Self { api }
    }

    /// Get a routing context using Veilid's default safe routing.
    pub fn routing_context(&self) -> MarketResult<veilid_core::RoutingContext> {
        self.api
            .routing_context()
            .map_err(|e| MarketError::Dht(format!("Failed to get routing context: {e}")))
    }

    /// Create a new DHT record with the default crypto system (VLD0)
    /// Returns the OwnedDHTRecord that includes the owner keypair for write access
    pub async fn create_dht_record(&self) -> MarketResult<OwnedDHTRecord> {
        let routing_context = self.routing_context()?;

        // Create DHT schema with configured subkeys:
        // - Subkey 0: Primary data (e.g., listing)
        // - Subkey 1: Bid index (for auctions)
        // - Subkey 2: Coordination record (for bid announcements)
        // - Subkey 3: Bidder registry (for n-party MPC)
        let schema = DHTSchema::dflt(DHT_SUBKEY_COUNT)
            .map_err(|e| MarketError::Dht(format!("Failed to create DHT schema: {e}")))?;

        // Create the record - this generates a new key and allocates storage
        // kind: CRYPTO_KIND_VLD0, schema: dflt(4), owner: None (random keypair)
        let record_descriptor = routing_context
            .create_dht_record(CRYPTO_KIND_VLD0, schema, None)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to create DHT record: {e}")))?;

        let key = record_descriptor.key();
        let owner = record_descriptor.owner_keypair().ok_or_else(|| {
            MarketError::Dht("Record was created but owner keypair is missing".into())
        })?;

        info!("Created DHT record with key: {}", key);

        Ok(OwnedDHTRecord { key, owner })
    }

    /// Open an existing DHT record for read/write access
    /// Returns the record descriptor if successful
    pub async fn open_record(&self, key: &RecordKey) -> MarketResult<DHTRecordDescriptor> {
        let routing_context = self.routing_context()?;

        let descriptor = routing_context
            .open_dht_record(key.clone(), None)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to open DHT record: {e}")))?;

        debug!("Opened DHT record: {}", key);
        Ok(descriptor)
    }

    /// Set a value in a DHT record (requires write access via owner keypair)
    /// For simple records, use subkey 0
    pub async fn set_dht_value(&self, record: &OwnedDHTRecord, value: Vec<u8>) -> MarketResult<()> {
        const MAX_DHT_VALUE_SIZE: usize = 32 * 1024; // 32KB Veilid limit
        if value.len() > MAX_DHT_VALUE_SIZE {
            return Err(MarketError::Dht(format!(
                "DHT value size {} exceeds maximum {} bytes",
                value.len(),
                MAX_DHT_VALUE_SIZE
            )));
        }

        let routing_context = self.routing_context()?;

        // Open the record with owner keypair for write access
        let _ = routing_context
            .open_dht_record(record.key.clone(), Some(record.owner.clone()))
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to open DHT record for writing: {e}")))?;

        // Set the value at subkey 0
        routing_context
            .set_dht_value(record.key.clone(), 0, value.clone(), None)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to set DHT value: {e}")))?;

        info!(
            "Set DHT value for key {}, {} bytes at subkey 0",
            record.key,
            value.len()
        );

        // Close the record after writing
        routing_context
            .close_dht_record(record.key.clone())
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to close DHT record: {e}")))?;

        Ok(())
    }

    /// Get a value from a DHT record
    /// For simple records, use subkey 0
    /// Returns None if the value hasn't been set yet
    pub async fn get_dht_value(&self, key: &RecordKey) -> MarketResult<Option<Vec<u8>>> {
        self.get_value_at_subkey(key, 0, true).await
    }

    /// Set a value at a specific subkey (requires write access)
    pub async fn set_value_at_subkey(
        &self,
        record: &OwnedDHTRecord,
        subkey: u32,
        value: Vec<u8>,
    ) -> MarketResult<()> {
        const MAX_DHT_VALUE_SIZE: usize = 32 * 1024; // 32KB Veilid limit
        if value.len() > MAX_DHT_VALUE_SIZE {
            return Err(MarketError::Dht(format!(
                "DHT value size {} exceeds maximum {} bytes",
                value.len(),
                MAX_DHT_VALUE_SIZE
            )));
        }

        let routing_context = self.routing_context()?;

        // Open the record with owner keypair for write access
        let _ = routing_context
            .open_dht_record(record.key.clone(), Some(record.owner.clone()))
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to open DHT record for writing: {e}")))?;

        // Set the value at specified subkey
        routing_context
            .set_dht_value(record.key.clone(), subkey, value.clone(), None)
            .await
            .map_err(|e| {
                MarketError::Dht(format!("Failed to set DHT value at subkey {subkey}: {e}"))
            })?;

        info!(
            "Set DHT value for key {}, {} bytes at subkey {}",
            record.key,
            value.len(),
            subkey
        );

        // Close the record after writing
        routing_context
            .close_dht_record(record.key.clone())
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to close DHT record: {e}")))?;

        Ok(())
    }

    /// Get a value from a specific subkey
    /// Returns None if the value hasn't been set yet
    /// When force_refresh is true, fetches the latest value from the network
    pub async fn get_value_at_subkey(
        &self,
        key: &RecordKey,
        subkey: u32,
        force_refresh: bool,
    ) -> MarketResult<Option<Vec<u8>>> {
        let routing_context = self.routing_context()?;

        // Open the record first
        let _ = routing_context
            .open_dht_record(key.clone(), None)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to open DHT record for reading: {e}")))?;

        // Get the value at specified subkey
        let value_data = routing_context
            .get_dht_value(key.clone(), subkey, force_refresh)
            .await
            .map_err(|e| {
                MarketError::Dht(format!("Failed to get DHT value from subkey {subkey}: {e}"))
            })?;

        // Close the record after reading
        routing_context
            .close_dht_record(key.clone())
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to close DHT record: {e}")))?;

        if let Some(data) = value_data {
            info!(
                "Retrieved DHT value for key {}: {} bytes from subkey {}",
                key,
                data.data().len(),
                subkey
            );
            Ok(Some(data.data().to_vec()))
        } else {
            debug!("No value found for key {} at subkey {}", key, subkey);
            Ok(None)
        }
    }

    /// Delete a DHT record
    /// This removes the record from the DHT entirely
    pub async fn delete_dht_record(&self, key: &RecordKey) -> MarketResult<()> {
        let routing_context = self.routing_context()?;

        // Open the record first
        let _ = routing_context
            .open_dht_record(key.clone(), None)
            .await
            .map_err(|e| {
                MarketError::Dht(format!("Failed to open DHT record for deletion: {e}"))
            })?;

        // Delete the record
        routing_context
            .delete_dht_record(key.clone())
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to delete DHT record: {e}")))?;

        info!("Deleted DHT record: {}", key);
        Ok(())
    }

    /// Watch a DHT record for changes
    /// Returns true if watch was successfully established
    pub async fn watch_dht_record(&self, key: &RecordKey) -> MarketResult<bool> {
        let routing_context = self.routing_context()?;

        // Open the record first
        let _ = routing_context
            .open_dht_record(key.clone(), None)
            .await
            .map_err(|e| {
                MarketError::Dht(format!("Failed to open DHT record for watching: {e}"))
            })?;

        // Watch all subkeys (primary data, bid index, coordination, bidder registry)
        let subkeys = Some(ValueSubkeyRangeSet::full());
        let expiration = None; // No expiration
        let count = None; // No watch limit

        let watch_result = routing_context
            .watch_dht_values(key.clone(), subkeys, expiration, count)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to watch DHT record: {e}")))?;

        if watch_result {
            info!("Now watching DHT record: {}", key);
        } else {
            debug!("Watch already active for DHT record: {}", key);
        }

        Ok(watch_result)
    }

    /// Cancel watching a DHT record
    pub async fn cancel_dht_watch(&self, key: &RecordKey) -> MarketResult<bool> {
        let routing_context = self.routing_context()?;

        // Cancel watch on all subkeys
        let subkeys = Some(ValueSubkeyRangeSet::full());

        let cancel_result = routing_context
            .cancel_dht_watch(key.clone(), subkeys)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to cancel DHT watch: {e}")))?;

        if cancel_result {
            info!("Cancelled watch on DHT record: {}", key);
        } else {
            debug!("No active watch found for DHT record: {}", key);
        }

        Ok(cancel_result)
    }
}

// Implement the DhtStore trait for DHTOperations
#[async_trait]
impl DhtStore for DHTOperations {
    type OwnedRecord = OwnedDHTRecord;

    async fn create_record(&self) -> MarketResult<Self::OwnedRecord> {
        self.create_dht_record().await
    }

    fn record_key(record: &Self::OwnedRecord) -> RecordKey {
        record.key.clone()
    }

    async fn get_value(&self, key: &RecordKey) -> MarketResult<Option<Vec<u8>>> {
        self.get_dht_value(key).await
    }

    async fn set_value(&self, record: &Self::OwnedRecord, value: Vec<u8>) -> MarketResult<()> {
        self.set_dht_value(record, value).await
    }

    async fn get_subkey(&self, key: &RecordKey, subkey: u32) -> MarketResult<Option<Vec<u8>>> {
        self.get_value_at_subkey(key, subkey, true).await
    }

    async fn set_subkey(
        &self,
        record: &Self::OwnedRecord,
        subkey: u32,
        value: Vec<u8>,
    ) -> MarketResult<()> {
        self.set_value_at_subkey(record, subkey, value).await
    }

    async fn delete_record(&self, key: &RecordKey) -> MarketResult<()> {
        self.delete_dht_record(key).await
    }

    async fn watch_record(&self, key: &RecordKey) -> MarketResult<bool> {
        self.watch_dht_record(key).await
    }

    async fn cancel_watch(&self, key: &RecordKey) -> MarketResult<bool> {
        self.cancel_dht_watch(key).await
    }
}

// Note: DHTOperations tests require a running Veilid node.
// See tests/integration/ for mock-based auction flow tests.
