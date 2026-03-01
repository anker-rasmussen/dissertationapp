use async_trait::async_trait;
use tracing::{debug, info};
use veilid_core::{DHTSchema, KeyPair, RecordKey, VeilidAPI, CRYPTO_KIND_VLD0};

use crate::config::DHT_SUBKEY_COUNT;
use crate::error::{MarketError, MarketResult};
use crate::traits::DhtStore;

/// Veilid DHT value size limit (32 KB).
const MAX_DHT_VALUE_SIZE: usize = 32 * 1024;

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
    #[tracing::instrument(skip_all)]
    pub async fn create_dht_record(&self) -> MarketResult<OwnedDHTRecord> {
        let routing_context = self.routing_context()?;

        // Create DHT schema with configured subkeys:
        // - Subkey 0: Primary data (e.g., listing)
        // - Subkey 1: Bid announcement registry (G-Set CRDT)
        let schema = DHTSchema::dflt(DHT_SUBKEY_COUNT)
            .map_err(|e| MarketError::Dht(format!("Failed to create DHT schema: {e}")))?;

        // Create the record - this generates a new key and allocates storage
        // kind: CRYPTO_KIND_VLD0, schema: dflt(2), owner: None (random keypair)
        let record_descriptor = routing_context
            .create_dht_record(CRYPTO_KIND_VLD0, schema, None)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to create DHT record: {e}")))?;

        let key = record_descriptor.key();
        let owner = record_descriptor.owner_keypair().ok_or_else(|| {
            MarketError::Dht("Record was created but owner keypair is missing".into())
        })?;

        info!(key = %key, "Created DHT record");

        Ok(OwnedDHTRecord { key, owner })
    }

    /// Set a value at a specific subkey (requires write access)
    pub async fn set_value_at_subkey(
        &self,
        record: &OwnedDHTRecord,
        subkey: u32,
        value: Vec<u8>,
    ) -> MarketResult<()> {
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

        // Set the value at specified subkey — capture result so we always close
        let write_result = routing_context
            .set_dht_value(record.key.clone(), subkey, value.clone(), None)
            .await
            .map_err(|e| {
                MarketError::Dht(format!("Failed to set DHT value at subkey {subkey}: {e}"))
            });

        // Always close, even on write failure
        routing_context
            .close_dht_record(record.key.clone())
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to close DHT record: {e}")))?;

        write_result?;
        info!(key = %record.key, bytes = value.len(), subkey, "Set DHT value");

        Ok(())
    }

    /// Atomic read-modify-write: opens the record once with the owner keypair,
    /// reads the current value (local cache, no network refresh — we are the
    /// writer so our local copy is authoritative), applies a user-supplied
    /// transform, and writes the result back before closing.
    #[tracing::instrument(skip_all, fields(key = %record.key, subkey))]
    pub async fn read_modify_write_subkey(
        &self,
        record: &OwnedDHTRecord,
        subkey: u32,
        transform: impl FnOnce(Option<Vec<u8>>) -> MarketResult<Vec<u8>>,
    ) -> MarketResult<()> {
        let routing_context = self.routing_context()?;

        // Open with owner keypair so both read and write see the same local state.
        let _ = routing_context
            .open_dht_record(record.key.clone(), Some(record.owner.clone()))
            .await
            .map_err(|e| {
                MarketError::Dht(format!(
                    "Failed to open DHT record for read-modify-write: {e}"
                ))
            })?;

        // Read current value (force_refresh = false: our local copy is
        // authoritative since we are the only writer for this record).
        let current = routing_context
            .get_dht_value(record.key.clone(), subkey, false)
            .await
            .map_err(|e| {
                MarketError::Dht(format!("Failed to read subkey {subkey} during RMW: {e}"))
            })?;

        let new_value = match transform(current.map(|d| d.data().to_vec())) {
            Ok(v) => v,
            Err(e) => {
                routing_context
                    .close_dht_record(record.key.clone())
                    .await
                    .ok();
                return Err(e);
            }
        };

        if new_value.len() > MAX_DHT_VALUE_SIZE {
            routing_context
                .close_dht_record(record.key.clone())
                .await
                .ok();
            return Err(MarketError::Dht(format!(
                "DHT value size {} exceeds maximum {} bytes",
                new_value.len(),
                MAX_DHT_VALUE_SIZE
            )));
        }

        let write_result = routing_context
            .set_dht_value(record.key.clone(), subkey, new_value, None)
            .await
            .map_err(|e| {
                MarketError::Dht(format!("Failed to write subkey {subkey} during RMW: {e}"))
            });

        routing_context
            .close_dht_record(record.key.clone())
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to close DHT record after RMW: {e}")))?;

        write_result?;
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

        // Get the value at specified subkey — capture result so we always close
        let read_result = routing_context
            .get_dht_value(key.clone(), subkey, force_refresh)
            .await
            .map_err(|e| {
                MarketError::Dht(format!("Failed to get DHT value from subkey {subkey}: {e}"))
            });

        // Always close, even on read failure
        routing_context
            .close_dht_record(key.clone())
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to close DHT record: {e}")))?;

        let value_data = read_result?;

        if let Some(data) = value_data {
            debug!(
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
        self.get_value_at_subkey(key, 0, true).await
    }

    async fn set_value(&self, record: &Self::OwnedRecord, value: Vec<u8>) -> MarketResult<()> {
        self.set_value_at_subkey(record, 0, value).await
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
}

// Note: DHTOperations tests require a running Veilid node.
// See tests/integration/ for mock-based auction flow tests.
