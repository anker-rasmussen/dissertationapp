use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::{debug, info, warn};
use veilid_core::{DHTSchema, KeyPair, RecordKey, CRYPTO_KIND_VLD0};

use super::dht::DHTOperations;

/// Path to the shared registry keypair file
/// All demo instances on the same machine share this file
fn get_registry_keypair_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("smpc-auction-registry-keypair.txt")
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
    /// Minimum bid
    pub min_bid: u64,
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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
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
}

impl RegistryOperations {
    pub fn new(dht: DHTOperations) -> Self {
        Self {
            dht,
            registry_key: None,
        }
    }

    /// Get or create the shared registry keypair
    /// First instance generates it and saves to a shared file
    /// Subsequent instances load from the file
    fn get_or_create_registry_keypair(&self) -> Result<KeyPair> {
        let keypair_path = get_registry_keypair_path();

        // Try to load existing keypair
        if keypair_path.exists() {
            let keypair_str = std::fs::read_to_string(&keypair_path)
                .map_err(|e| anyhow::anyhow!("Failed to read registry keypair file: {}", e))?;
            let keypair_str = keypair_str.trim();
            let keypair = KeyPair::try_from(keypair_str)
                .map_err(|e| anyhow::anyhow!("Failed to parse registry keypair: {}", e))?;
            debug!("Loaded registry keypair from {:?}", keypair_path);
            return Ok(keypair);
        }

        // Generate new keypair using Veilid's crypto system
        let routing_context = self.dht.get_routing_context_pub()?;
        let api = routing_context.api();
        let crypto = api
            .crypto()
            .map_err(|e| anyhow::anyhow!("Failed to get crypto: {}", e))?;
        let vcrypto = crypto
            .get(CRYPTO_KIND_VLD0)
            .ok_or_else(|| anyhow::anyhow!("VLD0 crypto not available"))?;

        let keypair = vcrypto.generate_keypair();
        let keypair_str = keypair.to_string();

        // Save to file for other instances
        std::fs::write(&keypair_path, &keypair_str)
            .map_err(|e| anyhow::anyhow!("Failed to save registry keypair: {}", e))?;

        info!("Generated new registry keypair, saved to {:?}", keypair_path);
        Ok(keypair)
    }

    /// Create or get the registry DHT record
    /// First node creates it, others open it
    pub async fn get_or_create_registry(&mut self) -> Result<RecordKey> {
        if let Some(key) = &self.registry_key {
            return Ok(key.clone());
        }

        let keypair = self.get_or_create_registry_keypair()?;
        let routing_context = self.dht.get_routing_context_pub()?;

        // Try to create the registry record with our known keypair
        let schema = DHTSchema::dflt(1)?;

        let (key, is_new) = match routing_context
            .create_dht_record(CRYPTO_KIND_VLD0, schema, Some(keypair.clone()))
            .await
        {
            Ok(descriptor) => {
                let key = descriptor.key();
                info!("Created new registry at: {}", key);
                (key, true)
            }
            Err(e) => {
                // Record might already exist - try to open it
                debug!("Create failed ({}), trying to open existing record", e);

                // Load the registry key from file
                let key_path = get_registry_keypair_path().with_extension("key");
                if key_path.exists() {
                    let key_str = std::fs::read_to_string(&key_path)?;
                    let key = RecordKey::try_from(key_str.trim())
                        .map_err(|e| anyhow::anyhow!("Failed to parse registry key: {}", e))?;

                    // Open the existing record
                    let _ = routing_context
                        .open_dht_record(key.clone(), Some(keypair.clone()))
                        .await?;
                    info!("Opened existing registry at: {}", key);
                    (key, false) // NOT new - don't initialize!
                } else {
                    return Err(anyhow::anyhow!("Registry record not found and cannot create: {}", e));
                }
            }
        };

        // Save the registry key for future use
        let key_path = get_registry_keypair_path().with_extension("key");
        if !key_path.exists() {
            std::fs::write(&key_path, key.to_string())?;
            debug!("Saved registry key to {:?}", key_path);
        }

        self.registry_key = Some(key.clone());

        // Only initialize if this is a NEW record we just created
        if is_new {
            info!("Initializing new empty registry");
            let empty_registry = ListingRegistry::default();
            let data = empty_registry
                .to_cbor()
                .map_err(|e| anyhow::anyhow!("Failed to serialize registry: {}", e))?;
            routing_context
                .set_dht_value(key.clone(), 0, data, None)
                .await?;
        }

        routing_context.close_dht_record(key.clone()).await?;
        Ok(key)
    }

    /// Fetch the current registry from DHT
    pub async fn fetch_registry(&mut self) -> Result<ListingRegistry> {
        let key = self.get_or_create_registry().await?;
        let keypair = self.get_or_create_registry_keypair()?;
        let routing_context = self.dht.get_routing_context_pub()?;

        // Open with keypair for potential writes
        let _ = routing_context
            .open_dht_record(key.clone(), Some(keypair))
            .await?;

        let data = routing_context
            .get_dht_value(key.clone(), 0, true)
            .await?
            .map(|v| v.data().to_vec());

        routing_context.close_dht_record(key.clone()).await?;

        match data {
            Some(bytes) => {
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
            }
            None => {
                debug!("Registry is empty");
                Ok(ListingRegistry::default())
            }
        }
    }

    /// Add a listing to the registry
    pub async fn register_listing(&mut self, entry: RegistryEntry) -> Result<()> {
        let key = self.get_or_create_registry().await?;
        let keypair = self.get_or_create_registry_keypair()?;
        let routing_context = self.dht.get_routing_context_pub()?;

        // Open with keypair for writing
        let _ = routing_context
            .open_dht_record(key.clone(), Some(keypair))
            .await?;

        // Fetch current registry
        let mut registry = match routing_context.get_dht_value(key.clone(), 0, true).await? {
            Some(v) => ListingRegistry::from_cbor(v.data())
                .map_err(|e| anyhow::anyhow!("Failed to deserialize registry: {}", e))?,
            None => ListingRegistry::default(),
        };

        // Add the new listing
        registry.add_listing(entry.clone());

        // Write back
        let data = registry
            .to_cbor()
            .map_err(|e| anyhow::anyhow!("Failed to serialize registry: {}", e))?;
        routing_context
            .set_dht_value(key.clone(), 0, data, None)
            .await?;

        routing_context.close_dht_record(key.clone()).await?;

        info!("Registered listing '{}' in registry", entry.title);
        Ok(())
    }
}
