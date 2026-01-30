use anyhow::{Context, Result};
use tracing::{debug, info};
use veilid_core::{
    DHTRecordDescriptor, DHTSchema, KeyPair, RecordKey, SafetySelection, Sequencing, VeilidAPI,
    ValueSubkeyRangeSet, CRYPTO_KIND_VLD0,
};

/// DHT operations wrapper for Veilid
#[derive(Clone)]
pub struct DHTOperations {
    api: VeilidAPI,
    /// Whether to use unsafe (direct) routing for devnet
    use_unsafe_routing: bool,
}

/// A DHT record with its owner keypair for write access
#[derive(Clone)]
pub struct OwnedDHTRecord {
    pub key: RecordKey,
    pub owner: KeyPair,
}

impl DHTOperations {
    /// Create a new DHT operations instance
    /// For devnet, use unsafe_routing=true to disable private routes
    pub fn new(api: VeilidAPI, use_unsafe_routing: bool) -> Self {
        Self {
            api,
            use_unsafe_routing,
        }
    }

    /// Get a routing context with the appropriate safety selection (internal)
    fn get_routing_context(&self) -> Result<veilid_core::RoutingContext> {
        self.get_routing_context_pub()
    }

    /// Get a routing context with the appropriate safety selection (public)
    pub fn get_routing_context_pub(&self) -> Result<veilid_core::RoutingContext> {
        let mut routing_context = self
            .api
            .routing_context()
            .context("Failed to get routing context")?;

        if self.use_unsafe_routing {
            // Use unsafe (direct) routing for devnet - no private routes
            // PreferOrdered ensures messages arrive in order when possible
            routing_context = routing_context
                .with_safety(SafetySelection::Unsafe(Sequencing::PreferOrdered))
                .context("Failed to set unsafe routing")?;
            debug!("Using unsafe (direct) routing for DHT operations");
        }

        Ok(routing_context)
    }

    /// Create a new DHT record with the default crypto system (VLD0)
    /// Returns the OwnedDHTRecord that includes the owner keypair for write access
    pub async fn create_record(&self) -> Result<OwnedDHTRecord> {
        let routing_context = self.get_routing_context()?;

        // Create DHT schema with 4 subkeys:
        // - Subkey 0: Primary data (e.g., listing)
        // - Subkey 1: Bid index (for auctions, currently unused)
        // - Subkey 2: Coordination record (for bid announcements)
        // - Subkey 3: Bidder registry (for n-party MPC)
        let schema = DHTSchema::dflt(4)?;

        // Create the record - this generates a new key and allocates storage
        // kind: CRYPTO_KIND_VLD0, schema: dflt(1), owner: None (random keypair)
        let record_descriptor = routing_context
            .create_dht_record(CRYPTO_KIND_VLD0, schema, None)
            .await
            .context("Failed to create DHT record")?;

        let key = record_descriptor.key();
        let owner = record_descriptor
            .owner_keypair()
            .context("Record was created but owner keypair is missing")?;

        info!("Created DHT record with key: {}", key);

        Ok(OwnedDHTRecord { key, owner })
    }

    /// Open an existing DHT record for read/write access
    /// Returns the record descriptor if successful
    pub async fn open_record(&self, key: &RecordKey) -> Result<DHTRecordDescriptor> {
        let routing_context = self.get_routing_context()?;

        let descriptor = routing_context
            .open_dht_record(key.clone(), None)
            .await
            .context("Failed to open DHT record")?;

        debug!("Opened DHT record: {}", key);
        Ok(descriptor)
    }

    /// Set a value in a DHT record (requires write access via owner keypair)
    /// For simple records, use subkey 0
    pub async fn set_value(&self, record: &OwnedDHTRecord, value: Vec<u8>) -> Result<()> {
        let routing_context = self.get_routing_context()?;

        // Open the record with owner keypair for write access
        let _ = routing_context
            .open_dht_record(record.key.clone(), Some(record.owner.clone()))
            .await
            .context("Failed to open DHT record for writing")?;

        // Set the value at subkey 0
        routing_context
            .set_dht_value(record.key.clone(), 0, value.clone(), None)
            .await
            .context("Failed to set DHT value")?;

        info!(
            "Set DHT value for key {}, {} bytes at subkey 0",
            record.key,
            value.len()
        );

        // Close the record after writing
        routing_context
            .close_dht_record(record.key.clone())
            .await
            .context("Failed to close DHT record")?;

        Ok(())
    }

    /// Get a value from a DHT record
    /// For simple records, use subkey 0
    /// Returns None if the value hasn't been set yet
    pub async fn get_value(&self, key: &RecordKey) -> Result<Option<Vec<u8>>> {
        self.get_value_at_subkey(key, 0).await
    }

    /// Set a value at a specific subkey (requires write access)
    pub async fn set_value_at_subkey(&self, record: &OwnedDHTRecord, subkey: u32, value: Vec<u8>) -> Result<()> {
        let routing_context = self.get_routing_context()?;

        // Open the record with owner keypair for write access
        let _ = routing_context
            .open_dht_record(record.key.clone(), Some(record.owner.clone()))
            .await
            .context("Failed to open DHT record for writing")?;

        // Set the value at specified subkey
        routing_context
            .set_dht_value(record.key.clone(), subkey, value.clone(), None)
            .await
            .context(format!("Failed to set DHT value at subkey {}", subkey))?;

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
            .context("Failed to close DHT record")?;

        Ok(())
    }

    /// Get a value from a specific subkey
    /// Returns None if the value hasn't been set yet
    pub async fn get_value_at_subkey(&self, key: &RecordKey, subkey: u32) -> Result<Option<Vec<u8>>> {
        let routing_context = self.get_routing_context()?;

        // Open the record first
        let _ = routing_context
            .open_dht_record(key.clone(), None)
            .await
            .context("Failed to open DHT record for reading")?;

        // Get the value at specified subkey
        // force_refresh=true ensures we get the latest value from the network
        let value_data = routing_context
            .get_dht_value(key.clone(), subkey, true)
            .await
            .context(format!("Failed to get DHT value from subkey {}", subkey))?;

        // Close the record after reading
        routing_context
            .close_dht_record(key.clone())
            .await
            .context("Failed to close DHT record")?;

        match value_data {
            Some(data) => {
                info!(
                    "Retrieved DHT value for key {}: {} bytes from subkey {}",
                    key,
                    data.data().len(),
                    subkey
                );
                Ok(Some(data.data().to_vec()))
            }
            None => {
                debug!("No value found for key {} at subkey {}", key, subkey);
                Ok(None)
            }
        }
    }

    /// Delete a DHT record
    /// This removes the record from the DHT entirely
    pub async fn delete_record(&self, key: &RecordKey) -> Result<()> {
        let routing_context = self.get_routing_context()?;

        // Open the record first
        let _ = routing_context
            .open_dht_record(key.clone(), None)
            .await
            .context("Failed to open DHT record for deletion")?;

        // Delete the record
        routing_context
            .delete_dht_record(key.clone())
            .await
            .context("Failed to delete DHT record")?;

        info!("Deleted DHT record: {}", key);
        Ok(())
    }

    /// Watch a DHT record for changes
    /// Returns true if watch was successfully established
    pub async fn watch_record(&self, key: &RecordKey) -> Result<bool> {
        let routing_context = self.get_routing_context()?;

        // Open the record first
        let _ = routing_context
            .open_dht_record(key.clone(), None)
            .await
            .context("Failed to open DHT record for watching")?;

        // Watch subkey 0 only (single value record)
        let subkeys = Some(ValueSubkeyRangeSet::single(0));
        let expiration = None; // No expiration
        let count = None; // No watch limit

        let watch_result = routing_context
            .watch_dht_values(key.clone(), subkeys, expiration, count)
            .await
            .context("Failed to watch DHT record")?;

        if watch_result {
            info!("Now watching DHT record: {}", key);
        } else {
            debug!("Watch already active for DHT record: {}", key);
        }

        Ok(watch_result)
    }

    /// Cancel watching a DHT record
    pub async fn cancel_watch(&self, key: &RecordKey) -> Result<bool> {
        let routing_context = self.get_routing_context()?;

        // Cancel watch on subkey 0
        let subkeys = Some(ValueSubkeyRangeSet::single(0));

        let cancel_result = routing_context
            .cancel_dht_watch(key.clone(), subkeys)
            .await
            .context("Failed to cancel DHT watch")?;

        if cancel_result {
            info!("Cancelled watch on DHT record: {}", key);
        } else {
            debug!("No active watch found for DHT record: {}", key);
        }

        Ok(cancel_result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require a running Veilid node, so they're integration tests
    // They should be run with: cargo test --test integration_tests

    #[tokio::test]
    #[ignore] // Requires running Veilid node
    async fn test_create_and_retrieve() {
        // This test would need a VeilidAPI instance
        // See integration tests for actual implementation
    }
}
