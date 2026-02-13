use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};
use veilid_core::{
    BareKeyPair, BarePublicKey, BareSecretKey, DHTSchema, KeyPair, RecordKey, CRYPTO_KIND_VLD0,
};

use super::dht::{DHTOperations, OwnedDHTRecord};
use crate::config::now_unix;
use crate::error::{MarketError, MarketResult};
use crate::traits::TimeProvider;

/// Derive a deterministic VLD0 keypair from the network key.
///
/// All nodes on the same network derive the same keypair, enabling any node
/// to create or write to the shared master registry DHT record.
fn derive_registry_keypair(network_key: &str) -> KeyPair {
    let mut hasher = Sha256::new();
    hasher.update(b"smpc-auction-market-registry:");
    hasher.update(network_key.as_bytes());
    let seed: [u8; 32] = hasher.finalize().into();

    let signing_key = ed25519_dalek::SigningKey::from_bytes(&seed);
    let verifying_key = signing_key.verifying_key();

    let bare_pub = BarePublicKey::new(&verifying_key.to_bytes());
    let bare_sec = BareSecretKey::new(&signing_key.to_bytes());
    KeyPair::new(CRYPTO_KIND_VLD0, BareKeyPair::new(bare_pub, bare_sec))
}

// ── Data structures ──────────────────────────────────────────────────

/// Entry in the master registry — one per seller.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SellerEntry {
    /// Seller's public key (string form).
    pub seller_pubkey: String,
    /// DHT key of the seller's catalog record.
    pub catalog_key: String,
    /// When this seller was registered (unix timestamp).
    pub registered_at: u64,
}

/// The master registry stored in the bootstrap node's DHT record (subkey 0, CBOR).
/// Contains the directory of all sellers.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MarketRegistry {
    /// Registered sellers.
    pub sellers: Vec<SellerEntry>,
    /// Version for conflict resolution (higher wins).
    pub version: u64,
}

impl MarketRegistry {
    pub fn to_cbor(&self) -> Result<Vec<u8>, ciborium::ser::Error<std::io::Error>> {
        let mut buffer = Vec::new();
        ciborium::into_writer(self, &mut buffer)?;
        Ok(buffer)
    }

    pub fn from_cbor(data: &[u8]) -> Result<Self, ciborium::de::Error<std::io::Error>> {
        ciborium::from_reader(data)
    }

    /// Add a seller, deduplicating by pubkey.
    /// Caps the registry at 500 sellers to prevent unbounded growth.
    pub fn add_seller(&mut self, entry: SellerEntry) {
        if self.sellers.len() >= 500 {
            return; // Cap at 500 sellers
        }
        if !self
            .sellers
            .iter()
            .any(|e| e.seller_pubkey == entry.seller_pubkey)
        {
            self.sellers.push(entry);
            self.version += 1;
        }
    }
}

/// Entry in a seller's catalog — one per listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    /// DHT key of the listing.
    pub listing_key: String,
    /// Title for quick display.
    pub title: String,
    /// Reserve price.
    pub reserve_price: u64,
    /// When the auction ends (unix timestamp).
    pub auction_end: u64,
}

/// A seller's catalog of listings, stored in the seller's own DHT record (subkey 0, CBOR).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SellerCatalog {
    /// Seller's public key (string form).
    pub seller_pubkey: String,
    /// Listings in this catalog.
    pub listings: Vec<CatalogEntry>,
    /// Version for conflict resolution.
    pub version: u64,
}

impl SellerCatalog {
    pub fn to_cbor(&self) -> Result<Vec<u8>, ciborium::ser::Error<std::io::Error>> {
        let mut buffer = Vec::new();
        ciborium::into_writer(self, &mut buffer)?;
        Ok(buffer)
    }

    pub fn from_cbor(data: &[u8]) -> Result<Self, ciborium::de::Error<std::io::Error>> {
        ciborium::from_reader(data)
    }

    /// Add a listing, deduplicating by key.
    /// Caps the catalog at 200 listings per seller to prevent unbounded growth.
    pub fn add_listing(&mut self, entry: CatalogEntry) {
        if self.listings.len() >= 200 {
            return; // Cap at 200 listings per seller
        }
        if !self
            .listings
            .iter()
            .any(|e| e.listing_key == entry.listing_key)
        {
            self.listings.push(entry);
            self.version += 1;
        }
    }

    /// Remove expired listings (auction ended more than 1 hour ago).
    pub fn cleanup_expired(&mut self) {
        let now = now_unix();
        let one_hour_ago = now.saturating_sub(3600);

        let before = self.listings.len();
        self.listings.retain(|e| e.auction_end > one_hour_ago);
        if self.listings.len() != before {
            self.version += 1;
        }
    }

    /// Remove expired listings with a custom time provider.
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

/// Flattened listing view produced by two-hop discovery.
/// Preserves the downstream API (same fields as the old `RegistryEntry`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryEntry {
    /// DHT key of the listing.
    pub key: String,
    /// Title for quick display.
    pub title: String,
    /// Seller's public key.
    pub seller: String,
    /// Reserve price.
    pub reserve_price: u64,
    /// When the auction ends (unix timestamp).
    pub auction_end: u64,
}

// ── Operations ───────────────────────────────────────────────────────

/// Shared-keypair registry operations.
///
/// All nodes derive the same keypair from the network key, so any node can
/// create or write to the master registry DHT record.
///
/// - **Any node**: can create-or-open the master registry, register sellers.
/// - **Seller node**: owns its own catalog DHT record, adds listings.
/// - **All nodes**: can read the master registry + seller catalogs for discovery.
pub struct RegistryOperations {
    dht: DHTOperations,
    /// Shared keypair derived from the network key (all nodes have the same one).
    shared_keypair: KeyPair,
    /// The master registry DHT key (set after create-or-open).
    master_registry_key: Option<RecordKey>,
    /// This seller's catalog DHT record (sellers only).
    seller_catalog_record: Option<OwnedDHTRecord>,
}

impl RegistryOperations {
    pub fn new(dht: DHTOperations, network_key: &str) -> Self {
        Self {
            dht,
            shared_keypair: derive_registry_keypair(network_key),
            master_registry_key: None,
            seller_catalog_record: None,
        }
    }

    // ── Master registry (any node) ─────────────────────────────────

    /// Create the master registry DHT record using the shared keypair.
    ///
    /// The first node to call this creates the record. The shared keypair
    /// means any node that knows the record key can write to it.
    /// On the same node, a second call will fail (record already in local store).
    pub async fn create_master_registry(&mut self) -> MarketResult<RecordKey> {
        let routing_context = self.dht.routing_context()?;
        let schema = DHTSchema::dflt(1)
            .map_err(|e| MarketError::Dht(format!("Failed to create DHT schema: {e}")))?;

        let descriptor = routing_context
            .create_dht_record(CRYPTO_KIND_VLD0, schema, Some(self.shared_keypair.clone()))
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to create master registry: {e}")))?;

        let key = descriptor.key();

        // Initialize with empty registry
        let empty = MarketRegistry::default();
        let data = empty.to_cbor().map_err(|e| {
            MarketError::Serialization(format!("Failed to serialize empty registry: {e}"))
        })?;
        routing_context
            .set_dht_value(key.clone(), 0, data, None)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to set initial registry value: {e}")))?;

        info!("Created master registry at: {}", key);
        self.master_registry_key = Some(key.clone());
        Ok(key)
    }

    /// Ensure the master registry is available (create or use already-known key).
    ///
    /// - If the key is already known (from a peer broadcast), returns it.
    /// - Otherwise tries to create the record (first node on this machine wins).
    pub async fn ensure_master_registry(&mut self) -> MarketResult<RecordKey> {
        if let Some(key) = &self.master_registry_key {
            return Ok(key.clone());
        }
        self.create_master_registry().await
    }

    /// Set the master registry key (received from a peer broadcast).
    pub fn set_master_registry_key(&mut self, key: RecordKey) {
        info!("Set master registry key: {}", key);
        self.master_registry_key = Some(key);
    }

    /// Register a seller in the master registry.
    /// Retries on concurrent-write conflicts.
    #[allow(clippy::too_many_lines)]
    pub async fn register_seller(
        &mut self,
        seller_pubkey: &str,
        catalog_key: &str,
    ) -> MarketResult<()> {
        let registry_key = self
            .master_registry_key
            .as_ref()
            .ok_or_else(|| MarketError::InvalidState("Master registry key not known yet".into()))?
            .clone();

        let routing_context = self.dht.routing_context()?;

        // Open with shared keypair for write access
        let _ = routing_context
            .open_dht_record(registry_key.clone(), Some(self.shared_keypair.clone()))
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to open registry record: {e}")))?;

        let max_retries: u32 = 10;
        let mut retry_delay = std::time::Duration::from_millis(50);

        for attempt in 0..max_retries {
            let value_data = routing_context
                .get_dht_value(registry_key.clone(), 0, true)
                .await
                .map_err(|e| MarketError::Dht(format!("Failed to get registry value: {e}")))?;

            let (mut registry, old_seq) = match value_data {
                Some(v) => {
                    let seq = v.seq();
                    match MarketRegistry::from_cbor(v.data()) {
                        Ok(reg) => (reg, Some(seq)),
                        Err(e) => {
                            warn!("Registry data corrupted (seq {}), overwriting: {}", seq, e);
                            (MarketRegistry::default(), Some(seq))
                        }
                    }
                }
                None => (MarketRegistry::default(), None),
            };

            // Dedup check
            if registry
                .sellers
                .iter()
                .any(|e| e.seller_pubkey == seller_pubkey)
            {
                info!("Seller '{}' already registered, skipping", seller_pubkey);
                return Ok(());
            }

            registry.add_seller(SellerEntry {
                seller_pubkey: seller_pubkey.to_string(),
                catalog_key: catalog_key.to_string(),
                registered_at: now_unix(),
            });

            let data = registry.to_cbor().map_err(|e| {
                MarketError::Serialization(format!("Failed to serialize registry: {e}"))
            })?;

            match routing_context
                .set_dht_value(registry_key.clone(), 0, data, None)
                .await
            {
                Ok(old_value) => {
                    if let Some(returned) = old_value {
                        if let Some(expected) = old_seq {
                            if returned.seq() != expected && attempt < max_retries - 1 {
                                warn!(
                                    "Concurrent write (seq {} -> {}), retrying",
                                    expected,
                                    returned.seq()
                                );
                                tokio::time::sleep(retry_delay).await;
                                retry_delay *= 2;
                                continue;
                            }
                        }
                    }
                    info!(
                        "Registered seller '{}' (attempt {}/{})",
                        seller_pubkey,
                        attempt + 1,
                        max_retries
                    );
                    return Ok(());
                }
                Err(e) => {
                    if attempt < max_retries - 1 {
                        warn!("Write failed ({}), retrying", e);
                        tokio::time::sleep(retry_delay).await;
                        retry_delay *= 2;
                        continue;
                    }
                    return Err(MarketError::Dht(format!(
                        "Failed to register seller after {max_retries} attempts: {e}"
                    )));
                }
            }
        }

        Err(MarketError::Dht(format!(
            "Failed to register seller after {max_retries} attempts"
        )))
    }

    /// Get the master registry key (if known).
    pub fn master_registry_key(&self) -> Option<RecordKey> {
        self.master_registry_key.clone()
    }

    // ── Seller operations ────────────────────────────────────────────

    /// Create (or return existing) seller catalog DHT record.
    pub async fn get_or_create_seller_catalog(
        &mut self,
        seller_pubkey: &str,
    ) -> MarketResult<RecordKey> {
        if let Some(record) = &self.seller_catalog_record {
            return Ok(record.key.clone());
        }

        let routing_context = self.dht.routing_context()?;
        let schema = DHTSchema::dflt(1)
            .map_err(|e| MarketError::Dht(format!("Failed to create DHT schema: {e}")))?;
        let descriptor = routing_context
            .create_dht_record(CRYPTO_KIND_VLD0, schema, None)
            .await
            .map_err(|e| {
                MarketError::Dht(format!("Failed to create seller catalog record: {e}"))
            })?;

        let key = descriptor.key();
        let owner = descriptor.owner_keypair().ok_or_else(|| {
            MarketError::Dht("Seller catalog record missing owner keypair".into())
        })?;

        // Initialize with empty catalog
        let empty = SellerCatalog {
            seller_pubkey: seller_pubkey.to_string(),
            listings: Vec::new(),
            version: 0,
        };
        let data = empty.to_cbor().map_err(|e| {
            MarketError::Serialization(format!("Failed to serialize empty catalog: {e}"))
        })?;
        routing_context
            .set_dht_value(key.clone(), 0, data, None)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to set initial catalog value: {e}")))?;

        let record = OwnedDHTRecord { key, owner };
        info!("Created seller catalog at: {}", record.key);
        let catalog_key = record.key.clone();
        self.seller_catalog_record = Some(record);

        Ok(catalog_key)
    }

    /// Get the seller catalog key (if created).
    pub fn seller_catalog_key(&self) -> Option<RecordKey> {
        self.seller_catalog_record.as_ref().map(|r| r.key.clone())
    }

    /// Add a listing to the seller's own catalog.
    pub async fn add_listing_to_catalog(&self, entry: CatalogEntry) -> MarketResult<()> {
        let record = self
            .seller_catalog_record
            .as_ref()
            .ok_or_else(|| MarketError::InvalidState("Seller catalog not created yet".into()))?;

        let routing_context = self.dht.routing_context()?;
        let _ = routing_context
            .open_dht_record(record.key.clone(), Some(record.owner.clone()))
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to open catalog record: {e}")))?;

        let value_data = routing_context
            .get_dht_value(record.key.clone(), 0, true)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to get catalog value: {e}")))?;

        let mut catalog = match value_data {
            Some(v) => SellerCatalog::from_cbor(v.data()).map_err(|e| {
                MarketError::Serialization(format!("Failed to deserialize catalog: {e}"))
            })?,
            None => SellerCatalog::default(),
        };

        catalog.add_listing(entry);

        let data = catalog
            .to_cbor()
            .map_err(|e| MarketError::Serialization(format!("Failed to serialize catalog: {e}")))?;
        routing_context
            .set_dht_value(record.key.clone(), 0, data, None)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to set catalog value: {e}")))?;

        info!(
            "Added listing to catalog, now has {} listings",
            catalog.listings.len()
        );
        Ok(())
    }

    // ── Discovery (all nodes) ────────────────────────────────────────

    /// Fetch the master registry from DHT.
    ///
    /// Returns an empty registry if the master key is not yet known
    /// (e.g. no `RegistryAnnouncement` received yet).
    pub async fn fetch_registry(&self) -> MarketResult<MarketRegistry> {
        let Some(key) = &self.master_registry_key else {
            info!("Master registry key not known yet, returning empty registry");
            return Ok(MarketRegistry::default());
        };
        let key = key.clone();

        let routing_context = self.dht.routing_context()?;
        let _ = routing_context
            .open_dht_record(key.clone(), None)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to open registry record: {e}")))?;

        let data = routing_context
            .get_dht_value(key.clone(), 0, true)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to get registry value: {e}")))?
            .map(|v| v.data().to_vec());

        let _ = routing_context.close_dht_record(key).await;

        if let Some(bytes) = data {
            match MarketRegistry::from_cbor(&bytes) {
                Ok(registry) => {
                    debug!("Fetched registry with {} sellers", registry.sellers.len());
                    Ok(registry)
                }
                Err(e) => {
                    warn!("Registry data corrupted, returning empty: {}", e);
                    Ok(MarketRegistry::default())
                }
            }
        } else {
            debug!("Registry is empty");
            Ok(MarketRegistry::default())
        }
    }

    /// Fetch a single seller's catalog from DHT.
    pub async fn fetch_seller_catalog(
        &self,
        catalog_key: &RecordKey,
    ) -> MarketResult<SellerCatalog> {
        let routing_context = self.dht.routing_context()?;
        let _ = routing_context
            .open_dht_record(catalog_key.clone(), None)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to open catalog record: {e}")))?;

        let data = routing_context
            .get_dht_value(catalog_key.clone(), 0, true)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to get catalog value: {e}")))?
            .map(|v| v.data().to_vec());

        let _ = routing_context.close_dht_record(catalog_key.clone()).await;

        if let Some(bytes) = data {
            match SellerCatalog::from_cbor(&bytes) {
                Ok(catalog) => {
                    debug!(
                        "Fetched catalog for seller '{}' with {} listings",
                        catalog.seller_pubkey,
                        catalog.listings.len()
                    );
                    Ok(catalog)
                }
                Err(e) => {
                    warn!("Catalog data corrupted, returning empty: {}", e);
                    Ok(SellerCatalog::default())
                }
            }
        } else {
            debug!("Catalog is empty");
            Ok(SellerCatalog::default())
        }
    }

    /// Two-hop discovery: master registry → seller catalogs → flat listing list.
    pub async fn fetch_all_listings(&self) -> MarketResult<Vec<RegistryEntry>> {
        let registry = self.fetch_registry().await?;
        let mut all_listings = Vec::new();

        for seller in &registry.sellers {
            let catalog_key = match RecordKey::try_from(seller.catalog_key.as_str()) {
                Ok(key) => key,
                Err(e) => {
                    warn!(
                        "Skipping seller '{}': invalid catalog key '{}': {}",
                        seller.seller_pubkey, seller.catalog_key, e
                    );
                    continue;
                }
            };

            match self.fetch_seller_catalog(&catalog_key).await {
                Ok(catalog) => {
                    for entry in &catalog.listings {
                        all_listings.push(RegistryEntry {
                            key: entry.listing_key.clone(),
                            title: entry.title.clone(),
                            seller: seller.seller_pubkey.clone(),
                            reserve_price: entry.reserve_price,
                            auction_end: entry.auction_end,
                        });
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to fetch catalog for seller '{}': {}",
                        seller.seller_pubkey, e
                    );
                }
            }
        }

        info!(
            "Two-hop discovery: {} sellers, {} total listings",
            registry.sellers.len(),
            all_listings.len()
        );
        Ok(all_listings)
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_registry_cbor_roundtrip() {
        let reg = MarketRegistry {
            sellers: vec![
                SellerEntry {
                    seller_pubkey: "pk1".to_string(),
                    catalog_key: "ck1".to_string(),
                    registered_at: 1000,
                },
                SellerEntry {
                    seller_pubkey: "pk2".to_string(),
                    catalog_key: "ck2".to_string(),
                    registered_at: 2000,
                },
            ],
            version: 5,
        };

        let bytes = reg.to_cbor().unwrap();
        let decoded = MarketRegistry::from_cbor(&bytes).unwrap();
        assert_eq!(decoded.sellers.len(), 2);
        assert_eq!(decoded.sellers[0].seller_pubkey, "pk1");
        assert_eq!(decoded.sellers[1].catalog_key, "ck2");
        assert_eq!(decoded.version, 5);
    }

    #[test]
    fn seller_catalog_cbor_roundtrip() {
        let catalog = SellerCatalog {
            seller_pubkey: "seller1".to_string(),
            listings: vec![CatalogEntry {
                listing_key: "lk1".to_string(),
                title: "Test".to_string(),
                reserve_price: 100,
                auction_end: 9999,
            }],
            version: 3,
        };

        let bytes = catalog.to_cbor().unwrap();
        let decoded = SellerCatalog::from_cbor(&bytes).unwrap();
        assert_eq!(decoded.seller_pubkey, "seller1");
        assert_eq!(decoded.listings.len(), 1);
        assert_eq!(decoded.listings[0].listing_key, "lk1");
        assert_eq!(decoded.version, 3);
    }

    #[test]
    fn market_registry_dedup() {
        let mut reg = MarketRegistry::default();
        reg.add_seller(SellerEntry {
            seller_pubkey: "pk1".to_string(),
            catalog_key: "ck1".to_string(),
            registered_at: 1000,
        });
        assert_eq!(reg.sellers.len(), 1);
        assert_eq!(reg.version, 1);

        // Duplicate — should be ignored
        reg.add_seller(SellerEntry {
            seller_pubkey: "pk1".to_string(),
            catalog_key: "ck1".to_string(),
            registered_at: 2000,
        });
        assert_eq!(reg.sellers.len(), 1);
        assert_eq!(reg.version, 1);

        // Different seller — should be added
        reg.add_seller(SellerEntry {
            seller_pubkey: "pk2".to_string(),
            catalog_key: "ck2".to_string(),
            registered_at: 3000,
        });
        assert_eq!(reg.sellers.len(), 2);
        assert_eq!(reg.version, 2);
    }

    #[test]
    fn seller_catalog_dedup() {
        let mut catalog = SellerCatalog::default();
        catalog.add_listing(CatalogEntry {
            listing_key: "lk1".to_string(),
            title: "Item 1".to_string(),
            reserve_price: 50,
            auction_end: 9999,
        });
        assert_eq!(catalog.listings.len(), 1);
        assert_eq!(catalog.version, 1);

        // Duplicate — should be ignored
        catalog.add_listing(CatalogEntry {
            listing_key: "lk1".to_string(),
            title: "Item 1".to_string(),
            reserve_price: 50,
            auction_end: 9999,
        });
        assert_eq!(catalog.listings.len(), 1);
        assert_eq!(catalog.version, 1);
    }

    #[test]
    fn seller_catalog_cleanup_expired() {
        use crate::mocks::MockTime;

        let mut catalog = SellerCatalog {
            seller_pubkey: "s".to_string(),
            listings: vec![
                CatalogEntry {
                    listing_key: "expired".to_string(),
                    title: "Old".to_string(),
                    reserve_price: 10,
                    auction_end: 100, // way in the past
                },
                CatalogEntry {
                    listing_key: "active".to_string(),
                    title: "New".to_string(),
                    reserve_price: 20,
                    auction_end: 99_999_999, // far future
                },
            ],
            version: 0,
        };

        catalog.cleanup_expired_with_time(&MockTime::new(5000));
        assert_eq!(catalog.listings.len(), 1);
        assert_eq!(catalog.listings[0].listing_key, "active");
        assert_eq!(catalog.version, 1);
    }

    #[test]
    fn test_market_registry_max_sellers() {
        let mut reg = MarketRegistry::default();

        // Add 500 sellers (the cap)
        for i in 0..500 {
            reg.add_seller(SellerEntry {
                seller_pubkey: format!("pk{}", i),
                catalog_key: format!("ck{}", i),
                registered_at: i as u64 * 1000,
            });
        }
        assert_eq!(reg.sellers.len(), 500);
        assert_eq!(reg.version, 500);

        // Try to add 501st seller — should be ignored
        reg.add_seller(SellerEntry {
            seller_pubkey: "pk500".to_string(),
            catalog_key: "ck500".to_string(),
            registered_at: 500_000,
        });
        assert_eq!(reg.sellers.len(), 500);
        assert_eq!(reg.version, 500); // Version unchanged

        // Verify last seller is still the 499th one (0-indexed)
        assert_eq!(reg.sellers[499].seller_pubkey, "pk499");
    }

    #[test]
    fn test_seller_catalog_max_listings() {
        let mut catalog = SellerCatalog {
            seller_pubkey: "seller1".to_string(),
            listings: Vec::new(),
            version: 0,
        };

        // Add 200 listings (the cap)
        for i in 0..200 {
            catalog.add_listing(CatalogEntry {
                listing_key: format!("lk{}", i),
                title: format!("Item {}", i),
                reserve_price: i as u64 * 10,
                auction_end: 99_999 + i as u64,
            });
        }
        assert_eq!(catalog.listings.len(), 200);
        assert_eq!(catalog.version, 200);

        // Try to add 201st listing — should be ignored
        catalog.add_listing(CatalogEntry {
            listing_key: "lk200".to_string(),
            title: "Item 200".to_string(),
            reserve_price: 2000,
            auction_end: 100_199,
        });
        assert_eq!(catalog.listings.len(), 200);
        assert_eq!(catalog.version, 200); // Version unchanged

        // Verify last listing is still the 199th one (0-indexed)
        assert_eq!(catalog.listings[199].listing_key, "lk199");
    }

    #[test]
    fn test_seller_catalog_listing_entries() {
        let mut catalog = SellerCatalog {
            seller_pubkey: "seller1".to_string(),
            listings: Vec::new(),
            version: 0,
        };

        let entry1 = CatalogEntry {
            listing_key: "lk1".to_string(),
            title: "Vintage Watch".to_string(),
            reserve_price: 500,
            auction_end: 10_000,
        };

        let entry2 = CatalogEntry {
            listing_key: "lk2".to_string(),
            title: "Rare Coin".to_string(),
            reserve_price: 1000,
            auction_end: 20_000,
        };

        catalog.add_listing(entry1.clone());
        catalog.add_listing(entry2.clone());

        assert_eq!(catalog.listings.len(), 2);
        assert_eq!(catalog.version, 2);

        // Verify first entry
        assert_eq!(catalog.listings[0].listing_key, "lk1");
        assert_eq!(catalog.listings[0].title, "Vintage Watch");
        assert_eq!(catalog.listings[0].reserve_price, 500);
        assert_eq!(catalog.listings[0].auction_end, 10_000);

        // Verify second entry
        assert_eq!(catalog.listings[1].listing_key, "lk2");
        assert_eq!(catalog.listings[1].title, "Rare Coin");
        assert_eq!(catalog.listings[1].reserve_price, 1000);
        assert_eq!(catalog.listings[1].auction_end, 20_000);

        // Test full roundtrip
        let bytes = catalog.to_cbor().unwrap();
        let decoded = SellerCatalog::from_cbor(&bytes).unwrap();

        assert_eq!(decoded.listings[0].listing_key, entry1.listing_key);
        assert_eq!(decoded.listings[0].title, entry1.title);
        assert_eq!(decoded.listings[1].listing_key, entry2.listing_key);
        assert_eq!(decoded.listings[1].title, entry2.title);
    }

    #[test]
    fn test_registry_entry_creation() {
        let entry = RegistryEntry {
            key: "listing_key_123".to_string(),
            title: "Test Item".to_string(),
            seller: "seller_pk_456".to_string(),
            reserve_price: 250,
            auction_end: 15_000,
        };

        assert_eq!(entry.key, "listing_key_123");
        assert_eq!(entry.title, "Test Item");
        assert_eq!(entry.seller, "seller_pk_456");
        assert_eq!(entry.reserve_price, 250);
        assert_eq!(entry.auction_end, 15_000);
    }

    #[test]
    fn test_market_registry_add_multiple_unique_sellers() {
        let mut reg = MarketRegistry::default();

        for i in 1..=10 {
            reg.add_seller(SellerEntry {
                seller_pubkey: format!("seller_{}", i),
                catalog_key: format!("catalog_{}", i),
                registered_at: i as u64 * 100,
            });
        }

        assert_eq!(reg.sellers.len(), 10);
        assert_eq!(reg.version, 10);

        // Verify each seller is distinct
        for (idx, i) in (1..=10).enumerate() {
            assert_eq!(reg.sellers[idx].seller_pubkey, format!("seller_{}", i));
            assert_eq!(reg.sellers[idx].catalog_key, format!("catalog_{}", i));
            assert_eq!(reg.sellers[idx].registered_at, i as u64 * 100);
        }
    }

    #[test]
    fn test_seller_catalog_empty_cleanup() {
        use crate::mocks::MockTime;

        let mut catalog = SellerCatalog {
            seller_pubkey: "s".to_string(),
            listings: Vec::new(),
            version: 0,
        };

        // Cleanup on empty catalog should not change version
        catalog.cleanup_expired_with_time(&MockTime::new(5000));
        assert_eq!(catalog.listings.len(), 0);
        assert_eq!(catalog.version, 0);
    }

    #[test]
    fn test_seller_catalog_cleanup_no_expired() {
        use crate::mocks::MockTime;

        let mut catalog = SellerCatalog {
            seller_pubkey: "s".to_string(),
            listings: vec![
                CatalogEntry {
                    listing_key: "active1".to_string(),
                    title: "Item1".to_string(),
                    reserve_price: 10,
                    auction_end: 10_000, // Far in the future
                },
                CatalogEntry {
                    listing_key: "active2".to_string(),
                    title: "Item2".to_string(),
                    reserve_price: 20,
                    auction_end: 20_000, // Far in the future
                },
            ],
            version: 0,
        };

        // Cleanup at time 5000 (all auctions end after this + 1 hour)
        catalog.cleanup_expired_with_time(&MockTime::new(5000));
        assert_eq!(catalog.listings.len(), 2);
        assert_eq!(catalog.version, 0); // Version unchanged
    }

    #[test]
    fn test_seller_catalog_cleanup_all_expired() {
        use crate::mocks::MockTime;

        let mut catalog = SellerCatalog {
            seller_pubkey: "s".to_string(),
            listings: vec![
                CatalogEntry {
                    listing_key: "expired1".to_string(),
                    title: "Old1".to_string(),
                    reserve_price: 10,
                    auction_end: 100,
                },
                CatalogEntry {
                    listing_key: "expired2".to_string(),
                    title: "Old2".to_string(),
                    reserve_price: 20,
                    auction_end: 200,
                },
            ],
            version: 0,
        };

        // Cleanup at time 10000 (all auctions ended > 1 hour ago)
        catalog.cleanup_expired_with_time(&MockTime::new(10_000));
        assert_eq!(catalog.listings.len(), 0);
        assert_eq!(catalog.version, 1); // Version bumped
    }

    #[test]
    fn test_market_registry_version_increments() {
        let mut reg = MarketRegistry::default();
        assert_eq!(reg.version, 0);

        reg.add_seller(SellerEntry {
            seller_pubkey: "pk1".to_string(),
            catalog_key: "ck1".to_string(),
            registered_at: 1000,
        });
        assert_eq!(reg.version, 1);

        reg.add_seller(SellerEntry {
            seller_pubkey: "pk2".to_string(),
            catalog_key: "ck2".to_string(),
            registered_at: 2000,
        });
        assert_eq!(reg.version, 2);

        // Duplicate — version should not increment
        reg.add_seller(SellerEntry {
            seller_pubkey: "pk1".to_string(),
            catalog_key: "ck1".to_string(),
            registered_at: 3000,
        });
        assert_eq!(reg.version, 2);
    }

    #[test]
    fn test_seller_catalog_version_increments() {
        let mut catalog = SellerCatalog::default();
        assert_eq!(catalog.version, 0);

        catalog.add_listing(CatalogEntry {
            listing_key: "lk1".to_string(),
            title: "Item1".to_string(),
            reserve_price: 100,
            auction_end: 5000,
        });
        assert_eq!(catalog.version, 1);

        catalog.add_listing(CatalogEntry {
            listing_key: "lk2".to_string(),
            title: "Item2".to_string(),
            reserve_price: 200,
            auction_end: 6000,
        });
        assert_eq!(catalog.version, 2);

        // Duplicate — version should not increment
        catalog.add_listing(CatalogEntry {
            listing_key: "lk1".to_string(),
            title: "Item1".to_string(),
            reserve_price: 100,
            auction_end: 5000,
        });
        assert_eq!(catalog.version, 2);
    }

    #[test]
    fn test_derive_registry_keypair_deterministic() {
        let network_key = "test-network-key";
        let keypair1 = derive_registry_keypair(network_key);
        let keypair2 = derive_registry_keypair(network_key);

        // Same network key should produce same keypair
        assert_eq!(keypair1.kind(), keypair2.kind());
        assert_eq!(keypair1.value().key(), keypair2.value().key());
        assert_eq!(keypair1.value().secret(), keypair2.value().secret());
    }

    #[test]
    fn test_derive_registry_keypair_different_networks() {
        let keypair1 = derive_registry_keypair("network-1");
        let keypair2 = derive_registry_keypair("network-2");

        // Different network keys should produce different keypairs
        assert_ne!(keypair1.value().key(), keypair2.value().key());
        assert_ne!(keypair1.value().secret(), keypair2.value().secret());
    }
}
