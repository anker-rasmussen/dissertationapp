use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use veilid_core::{DHTSchema, KeyPair, RecordKey, CRYPTO_KIND_VLD0};

use super::dht::DHTOperations;
use crate::config::now_unix;
use crate::traits::TimeProvider;

/// Default registry keypair for devnet/demo.
/// Override via `VEILID_REGISTRY_KEYPAIR` environment variable.
/// In production, this should be loaded from a secure shared location.
const DEFAULT_REGISTRY_KEYPAIR: &str =
    "VLD0:Qz4_xPTDDkwDtIgwuk1AarJTudWkg1hlrMIuQstprzM:o70UeMuq22ZrE-ysnmtN8wthjO0YSRUe6nxn2DnnWeQ";

/// Default registry record key for devnet/demo.
/// Override via `VEILID_REGISTRY_RECORD_KEY` environment variable.
const DEFAULT_REGISTRY_RECORD_KEY: &str = "VLD0:WvPYrb8EnnKOsCQ6MB_inMSnlXyQ6mkXuMa2fh55Dz4";

/// Read the registry keypair from the environment, falling back to the default.
fn registry_keypair_str() -> String {
    std::env::var("VEILID_REGISTRY_KEYPAIR")
        .unwrap_or_else(|_| DEFAULT_REGISTRY_KEYPAIR.to_string())
}

/// Read the registry record key from the environment, falling back to the default.
fn registry_record_key_str() -> String {
    std::env::var("VEILID_REGISTRY_RECORD_KEY")
        .unwrap_or_else(|_| DEFAULT_REGISTRY_RECORD_KEY.to_string())
}

/// Listing entry in the registry (minimal info for discovery)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryEntry {
    /// DHT key of the listing
    pub key: String,
    /// Title for quick display
    pub title: String,
    /// Seller's public key
    pub seller: String,
    /// Reserve price
    pub reserve_price: u64,
    /// When the auction ends (unix timestamp)
    pub auction_end: u64,
}

/// The shared registry stored in DHT
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ListingRegistry {
    /// All registered listings
    pub listings: Vec<RegistryEntry>,
    /// Version for conflict resolution (higher wins)
    pub version: u64,
}

impl ListingRegistry {
    /// Serialize to CBOR
    pub fn to_cbor(&self) -> Result<Vec<u8>, ciborium::ser::Error<std::io::Error>> {
        let mut buffer = Vec::new();
        ciborium::into_writer(self, &mut buffer)?;
        Ok(buffer)
    }

    /// Deserialize from CBOR
    pub fn from_cbor(data: &[u8]) -> Result<Self, ciborium::de::Error<std::io::Error>> {
        ciborium::from_reader(data)
    }

    /// Add a listing to the registry
    pub fn add_listing(&mut self, entry: RegistryEntry) {
        // Don't add duplicates
        if !self.listings.iter().any(|e| e.key == entry.key) {
            self.listings.push(entry);
            self.version += 1;
        }
    }

    /// Remove expired listings (auction ended more than 1 hour ago)
    pub fn cleanup_expired(&mut self) {
        let now = now_unix();
        let one_hour_ago = now.saturating_sub(3600);

        let before = self.listings.len();
        self.listings.retain(|e| e.auction_end > one_hour_ago);
        if self.listings.len() != before {
            self.version += 1;
        }
    }

    /// Remove expired listings with a custom time provider
    pub fn cleanup_expired_with_time<T: TimeProvider>(&mut self, time: &T) {
        let now = time.now_unix();
        let one_hour_ago = now.saturating_sub(3600);

        let before = self.listings.len();
        self.listings.retain(|e| e.auction_end > one_hour_ago);
        if self.listings.len() != before {
            self.version += 1;
        }
    }
}

/// Operations for the shared listing registry
pub struct RegistryOperations {
    dht: DHTOperations,
    /// Cached registry record key (once created/found)
    registry_key: Option<RecordKey>,
    /// Track if we have an open record handle
    record_open: bool,
}

impl RegistryOperations {
    pub const fn new(dht: DHTOperations) -> Self {
        Self {
            dht,
            registry_key: None,
            record_open: false,
        }
    }

    /// Get the shared registry keypair
    /// Uses a hardcoded keypair for devnet to ensure all nodes share the same registry
    /// In production, this should load from a secure shared key management system
    fn get_or_create_registry_keypair() -> Result<KeyPair> {
        let keypair_str = registry_keypair_str();
        let keypair = KeyPair::try_from(keypair_str.as_str())
            .map_err(|e| anyhow::anyhow!("Failed to parse registry keypair: {e}"))?;

        debug!("Using registry keypair (from env or default)");
        Ok(keypair)
    }

    /// Create or get the registry DHT record
    /// All nodes use the hardcoded record key to share the same registry
    pub async fn get_or_create_registry(&mut self) -> Result<RecordKey> {
        if let Some(key) = &self.registry_key {
            return Ok(key.clone());
        }

        let keypair = Self::get_or_create_registry_keypair()?;
        let routing_context = self.dht.get_routing_context_pub()?;

        // Parse the registry record key (from env or default)
        let record_key_str = registry_record_key_str();
        let key = RecordKey::try_from(record_key_str.as_str())
            .map_err(|e| anyhow::anyhow!("Failed to parse registry record key: {e}"))?;

        // Try to open the existing record first (most common case)
        let is_new = match routing_context
            .open_dht_record(key.clone(), Some(keypair.clone()))
            .await
        {
            Ok(_) => {
                info!("Opened existing registry at: {}", key);
                false
            }
            Err(open_err) => {
                // Record doesn't exist yet - try to create it
                debug!(
                    "Open failed ({}), attempting to create new registry record",
                    open_err
                );

                let schema = DHTSchema::dflt(1)?;
                match routing_context
                    .create_dht_record(CRYPTO_KIND_VLD0, schema, Some(keypair.clone()))
                    .await
                {
                    Ok(descriptor) => {
                        let created_key = descriptor.key();
                        if created_key != key {
                            warn!(
                                "Created registry key {} doesn't match expected key {}. Using created key.",
                                created_key, key
                            );
                            // This shouldn't happen with a hardcoded keypair, but handle it gracefully
                        }
                        info!("Created new registry at: {}", created_key);
                        true
                    }
                    Err(create_err) => {
                        // Race condition: another node created it between our open and create
                        // Try opening again
                        debug!(
                            "Create failed ({}), trying to open again (race condition)",
                            create_err
                        );
                        let _ = routing_context
                            .open_dht_record(key.clone(), Some(keypair.clone()))
                            .await
                            .map_err(|e| {
                                anyhow::anyhow!(
                                    "Failed to open or create registry. Open error: {open_err}, Create error: {create_err}, Retry open error: {e}"
                                )
                            })?;
                        info!("Opened registry at: {} (after race)", key);
                        false
                    }
                }
            }
        };

        self.registry_key = Some(key.clone());

        // Only initialize if this is a NEW record we just created
        if is_new {
            info!("Initializing new empty registry");
            let empty_registry = ListingRegistry::default();
            let data = empty_registry
                .to_cbor()
                .map_err(|e| anyhow::anyhow!("Failed to serialize registry: {e}"))?;
            routing_context
                .set_dht_value(key.clone(), 0, data, None)
                .await?;
        }

        // Keep the record open for future operations
        // Don't close it - we'll reuse the open handle
        self.record_open = true;
        info!("Registry record is now open and ready");
        Ok(key)
    }

    /// Fetch the current registry from DHT
    pub async fn fetch_registry(&mut self) -> Result<ListingRegistry> {
        let key = self.get_or_create_registry().await?;
        let routing_context = self.dht.get_routing_context_pub()?;

        // Record should already be open from get_or_create_registry
        // If not, open it now
        if !self.record_open {
            let keypair = Self::get_or_create_registry_keypair()?;
            let _ = routing_context
                .open_dht_record(key.clone(), Some(keypair))
                .await?;
            self.record_open = true;
        }

        // Force refresh to get latest data from the network
        let data = routing_context
            .get_dht_value(key.clone(), 0, true)
            .await?
            .map(|v| v.data().to_vec());

        // Don't close the record - keep it open for future operations

        if let Some(bytes) = data {
            // Handle corrupted data gracefully
            match ListingRegistry::from_cbor(&bytes) {
                Ok(registry) => {
                    debug!("Fetched registry with {} listings", registry.listings.len());
                    Ok(registry)
                }
                Err(e) => {
                    warn!("Registry data corrupted, returning empty: {}", e);
                    Ok(ListingRegistry::default())
                }
            }
        } else {
            debug!("Registry is empty");
            Ok(ListingRegistry::default())
        }
    }

    /// Add a listing to the registry with retry logic for conflict resolution
    pub async fn register_listing(&mut self, entry: RegistryEntry) -> Result<()> {
        let key = self.get_or_create_registry().await?;
        let routing_context = self.dht.get_routing_context_pub()?;

        // Record should already be open from get_or_create_registry
        if !self.record_open {
            let keypair = Self::get_or_create_registry_keypair()?;
            let _ = routing_context
                .open_dht_record(key.clone(), Some(keypair))
                .await?;
            self.record_open = true;
        }

        // Retry up to 10 times with exponential backoff to handle concurrent writes
        // This implements optimistic concurrency control
        let max_retries = 10;
        let mut retry_delay = std::time::Duration::from_millis(50);

        for attempt in 0..max_retries {
            // Fetch current registry with force_refresh to get latest state from network
            let value_data = routing_context.get_dht_value(key.clone(), 0, true).await?;

            let (mut registry, old_seq) = match value_data {
                Some(v) => {
                    let seq = v.seq();
                    let registry = ListingRegistry::from_cbor(v.data())
                        .map_err(|e| anyhow::anyhow!("Failed to deserialize registry: {e}"))?;
                    (registry, Some(seq))
                }
                None => (ListingRegistry::default(), None),
            };

            // Check if listing already exists (no-op if duplicate)
            if registry.listings.iter().any(|e| e.key == entry.key) {
                info!("Listing '{}' already in registry, skipping", entry.title);
                return Ok(());
            }

            // Add the new listing
            registry.add_listing(entry.clone());

            // Serialize the updated registry
            let data = registry
                .to_cbor()
                .map_err(|e| anyhow::anyhow!("Failed to serialize registry: {e}"))?;

            // Write the updated registry (Veilid automatically increments sequence number)
            match routing_context
                .set_dht_value(key.clone(), 0, data.clone(), None)
                .await
            {
                Ok(old_value) => {
                    // Verify the write succeeded by checking if the sequence changed
                    // If another node wrote between our read and write, we need to retry
                    if let Some(returned_value) = old_value {
                        let returned_seq = returned_value.seq();
                        if let Some(expected_seq) = old_seq {
                            if returned_seq != expected_seq {
                                // Sequence changed - another write happened
                                if attempt < max_retries - 1 {
                                    warn!(
                                        "Concurrent write detected (seq {} -> {}), retrying in {:?}",
                                        expected_seq, returned_seq, retry_delay
                                    );
                                    tokio::time::sleep(retry_delay).await;
                                    retry_delay *= 2; // Exponential backoff
                                    continue;
                                }
                            }
                        }
                    }

                    info!(
                        "Successfully registered listing '{}' in registry (attempt {}/{})",
                        entry.title,
                        attempt + 1,
                        max_retries
                    );
                    return Ok(());
                }
                Err(e) => {
                    // Write failed - could be network issue or other error
                    if attempt < max_retries - 1 {
                        warn!(
                            "Write failed ({}), retrying in {:?} (attempt {}/{})",
                            e,
                            retry_delay,
                            attempt + 1,
                            max_retries
                        );
                        tokio::time::sleep(retry_delay).await;
                        retry_delay *= 2;
                        continue;
                    }
                    return Err(anyhow::anyhow!(
                        "Failed to register listing after {max_retries} attempts: {e}"
                    ));
                }
            }
        }

        Err(anyhow::anyhow!(
            "Failed to register listing after {max_retries} attempts"
        ))
    }
}
