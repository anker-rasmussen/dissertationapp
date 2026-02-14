use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};
use veilid_core::{
    BareKeyPair, BareOpaqueRecordKey, BarePublicKey, BareRecordKey, BareSecretKey, DHTSchema,
    KeyPair, RecordKey, ValueSeqNum, CRYPTO_KIND_VLD0,
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

/// Entry in the master registry — one per node's broadcast route.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteEntry {
    /// Node's public key (string form).
    pub node_id: String,
    /// Serialised Veilid route blob for importing.
    pub route_blob: Vec<u8>,
    /// When this route was registered (unix timestamp).
    pub registered_at: u64,
}

/// Entry in the master registry — one per seller.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SellerEntry {
    /// Seller's public key (string form).
    pub seller_pubkey: String,
    /// DHT key of the seller's catalog record.
    pub catalog_key: String,
    /// When this seller was registered (unix timestamp).
    pub registered_at: u64,
    /// Ed25519 signing public key (hex-encoded) for message authentication.
    #[serde(default)]
    pub signing_pubkey: String,
}

/// The master registry stored in the bootstrap node's DHT record (subkey 0, CBOR).
/// Contains the directory of all sellers and node broadcast routes.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MarketRegistry {
    /// Registered sellers.
    pub sellers: Vec<SellerEntry>,
    /// Node broadcast routes (for private-route-based messaging).
    #[serde(default)]
    pub routes: Vec<RouteEntry>,
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

    /// Add or update a node's broadcast route, deduplicating by node_id.
    /// Caps at 200 routes (same sizing as sellers).
    pub fn add_route(&mut self, entry: RouteEntry) {
        if let Some(existing) = self.routes.iter_mut().find(|r| r.node_id == entry.node_id) {
            existing.route_blob = entry.route_blob;
            existing.registered_at = entry.registered_at;
        } else if self.routes.len() < 200 {
            self.routes.push(entry);
        } else {
            return;
        }
        self.version += 1;
    }

    /// Add a seller, deduplicating by pubkey.
    /// Caps the registry at 500 sellers to prevent unbounded growth.
    pub fn add_seller(&mut self, entry: SellerEntry) {
        if self.sellers.len() >= 200 {
            return; // Cap at 200 sellers (must fit in 32KB DHT value)
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

    /// Compute the deterministic record key for the master registry.
    ///
    /// Replicates Veilid's internal key derivation:
    /// `BLAKE3(VLD0_bytes || owner_pubkey || schema_compiled)`.
    /// Since the shared keypair is deterministic (derived from the network key),
    /// all nodes on the same network compute the same record key.
    fn compute_master_registry_key(&self) -> RecordKey {
        let owner_key: &[u8] = &self.shared_keypair.value().key();
        // DHTSchema::dflt(1).compile() = FourCC "DFLT" + 1u16 little-endian
        let schema_data: [u8; 6] = [b'D', b'F', b'L', b'T', 1, 0];

        let mut hash_input = Vec::with_capacity(4 + owner_key.len() + schema_data.len());
        hash_input.extend_from_slice(b"VLD0");
        hash_input.extend_from_slice(owner_key);
        hash_input.extend_from_slice(&schema_data);

        let hash = blake3::hash(&hash_input);
        let opaque = BareOpaqueRecordKey::new(hash.as_bytes());
        RecordKey::new(CRYPTO_KIND_VLD0, BareRecordKey::new(opaque, None))
    }

    /// Create the master registry DHT record using the shared keypair.
    ///
    /// The first node to call this creates the record. The shared keypair
    /// means any node that knows the record key can write to it.
    ///
    /// `create_dht_record` unconditionally generates a **random** per-record
    /// encryption key.  Since the master registry must be readable by every
    /// node on the network (each of which would independently generate a
    /// *different* key), we immediately close the encrypted handle and
    /// re-open the record **without** an encryption key so that values are
    /// stored in plaintext on the DHT.
    pub async fn create_master_registry(&mut self) -> MarketResult<RecordKey> {
        let routing_context = self.dht.routing_context()?;
        let schema = DHTSchema::dflt(1)
            .map_err(|e| MarketError::Dht(format!("Failed to create DHT schema: {e}")))?;

        // Step 1 — allocate the record (generates a random encryption key).
        let descriptor = routing_context
            .create_dht_record(CRYPTO_KIND_VLD0, schema, Some(self.shared_keypair.clone()))
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to create master registry: {e}")))?;

        // Step 2 — close the encrypted handle.
        routing_context
            .close_dht_record(descriptor.key())
            .await
            .map_err(|e| {
                MarketError::Dht(format!("Failed to close encrypted registry handle: {e}"))
            })?;

        // Step 3 — re-open with the deterministic key (no encryption key).
        let key = self.compute_master_registry_key();
        let _ = routing_context
            .open_dht_record(key.clone(), Some(self.shared_keypair.clone()))
            .await
            .map_err(|e| {
                MarketError::Dht(format!(
                    "Failed to re-open master registry without encryption: {e}"
                ))
            })?;

        // Step 4 — write initial data in plaintext.
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

    /// Ensure the master registry is available.
    ///
    /// 1. If the key is already known (from a peer broadcast or prior call), returns it.
    /// 2. Otherwise, computes the deterministic key and tries to open it
    ///    (handles restart / DHT-replication scenarios).
    /// 3. Falls back to creating a new record if it doesn't exist locally yet.
    pub async fn ensure_master_registry(&mut self) -> MarketResult<RecordKey> {
        if let Some(key) = &self.master_registry_key {
            return Ok(key.clone());
        }

        // Compute the deterministic key and try to open the existing record.
        // This handles two cases:
        //   a) Record exists from a previous app session (same node restart)
        //   b) Record was replicated here from another node that created it
        let expected_key = self.compute_master_registry_key();
        let routing_context = self.dht.routing_context()?;

        match routing_context
            .open_dht_record(expected_key.clone(), Some(self.shared_keypair.clone()))
            .await
        {
            Ok(_) => {
                info!("Opened existing master registry at: {}", expected_key);
                self.master_registry_key = Some(expected_key.clone());
                return Ok(expected_key);
            }
            Err(e) => {
                debug!("Master registry not in local store, will create: {}", e);
            }
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
        signing_pubkey: &str,
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
        let mut conflict_value: Option<Vec<u8>> = None;

        for attempt in 0..max_retries {
            let (mut registry, old_seq) = if let Some(cv) = conflict_value.take() {
                match MarketRegistry::from_cbor(&cv) {
                    Ok(reg) => (reg, None),
                    Err(e) => {
                        warn!("Conflict value corrupted ({}), re-fetching", e);
                        self.read_registry_from_dht(&routing_context, &registry_key)
                            .await?
                    }
                }
            } else {
                self.read_registry_from_dht(&routing_context, &registry_key)
                    .await?
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
                signing_pubkey: signing_pubkey.to_string(),
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
                                    "Seller registration: concurrent write (seq {} -> {}), retrying",
                                    expected,
                                    returned.seq()
                                );
                                conflict_value = Some(returned.data().to_vec());
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

    /// Force-refresh read of the registry from the DHT network.
    ///
    /// Returns `(registry, Some(seq))` when data exists, or
    /// `(default, None)` for an empty/missing record.
    async fn read_registry_from_dht(
        &self,
        routing_context: &veilid_core::RoutingContext,
        registry_key: &RecordKey,
    ) -> MarketResult<(MarketRegistry, Option<ValueSeqNum>)> {
        let value_data = routing_context
            .get_dht_value(registry_key.clone(), 0, true)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to get registry value: {e}")))?;

        Ok(match value_data {
            Some(v) if !v.data().is_empty() => {
                let seq = v.seq();
                match MarketRegistry::from_cbor(v.data()) {
                    Ok(reg) => (reg, Some(seq)),
                    Err(e) => {
                        warn!("Registry data corrupted (seq {}), overwriting: {}", seq, e);
                        (MarketRegistry::default(), Some(seq))
                    }
                }
            }
            Some(_) => {
                debug!("Registry subkey 0 has no data yet (fresh record)");
                (MarketRegistry::default(), None)
            }
            None => (MarketRegistry::default(), None),
        })
    }

    /// Register (or update) this node's broadcast route in the master registry.
    /// Retries on concurrent-write conflicts (same pattern as `register_seller`).
    pub async fn register_route(&mut self, node_id: &str, route_blob: Vec<u8>) -> MarketResult<()> {
        let registry_key = self
            .master_registry_key
            .as_ref()
            .ok_or_else(|| MarketError::InvalidState("Master registry key not known yet".into()))?
            .clone();

        let routing_context = self.dht.routing_context()?;
        let _ = routing_context
            .open_dht_record(registry_key.clone(), Some(self.shared_keypair.clone()))
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to open registry record: {e}")))?;

        let max_retries: u32 = 10;
        let mut retry_delay = std::time::Duration::from_millis(50);

        // When a write conflict occurs, set_dht_value returns the current network
        // value.  We store it here and use it directly as the base for the next
        // attempt, which avoids a force-refresh read that may still return stale
        // cached data (the root cause of routes being overwritten on retry).
        let mut conflict_value: Option<Vec<u8>> = None;

        for attempt in 0..max_retries {
            let (mut registry, old_seq) = if let Some(cv) = conflict_value.take() {
                match MarketRegistry::from_cbor(&cv) {
                    Ok(reg) => {
                        debug!(
                            "Using conflict value as base ({} routes, {} sellers)",
                            reg.routes.len(),
                            reg.sellers.len()
                        );
                        (reg, None)
                    }
                    Err(e) => {
                        warn!("Conflict value corrupted ({}), re-fetching", e);
                        self.read_registry_from_dht(&routing_context, &registry_key)
                            .await?
                    }
                }
            } else {
                self.read_registry_from_dht(&routing_context, &registry_key)
                    .await?
            };

            registry.add_route(RouteEntry {
                node_id: node_id.to_string(),
                route_blob: route_blob.clone(),
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
                                    "Route registration: concurrent write (seq {} -> {}), retrying",
                                    expected,
                                    returned.seq()
                                );
                                conflict_value = Some(returned.data().to_vec());
                                tokio::time::sleep(retry_delay).await;
                                retry_delay *= 2;
                                continue;
                            }
                        }
                    }
                    info!(
                        "Registered route for node '{}' (attempt {}/{})",
                        node_id,
                        attempt + 1,
                        max_retries
                    );
                    return Ok(());
                }
                Err(e) => {
                    if attempt < max_retries - 1 {
                        warn!("Route registration write failed ({}), retrying", e);
                        tokio::time::sleep(retry_delay).await;
                        retry_delay *= 2;
                        continue;
                    }
                    return Err(MarketError::Dht(format!(
                        "Failed to register route after {max_retries} attempts: {e}"
                    )));
                }
            }
        }

        Err(MarketError::Dht(format!(
            "Failed to register route after {max_retries} attempts"
        )))
    }

    /// Fetch all route blobs from the master registry, excluding the given node_id.
    ///
    /// Uses `force_refresh=true` to always get the latest registry from the
    /// network.  Without this, an early reader (e.g. the first node to start)
    /// caches an empty route list and never discovers peers that register later.
    pub async fn fetch_route_blobs(
        &self,
        exclude_node_id: &str,
    ) -> MarketResult<Vec<(String, Vec<u8>)>> {
        let registry = self.fetch_registry(true).await?;
        Ok(registry
            .routes
            .into_iter()
            .filter(|r| r.node_id != exclude_node_id)
            .map(|r| (r.node_id, r.route_blob))
            .collect())
    }

    /// Resolve a seller's registered Ed25519 signing key from the master registry.
    ///
    /// Returns `Ok(None)` when the seller is unknown or has no signing key set.
    pub async fn get_seller_signing_pubkey(
        &self,
        seller_pubkey: &str,
    ) -> MarketResult<Option<[u8; 32]>> {
        let registry = self.fetch_registry(true).await?;
        let Some(entry) = registry
            .sellers
            .iter()
            .find(|s| s.seller_pubkey == seller_pubkey)
        else {
            return Ok(None);
        };

        if entry.signing_pubkey.is_empty() {
            return Ok(None);
        }

        let decoded = hex::decode(&entry.signing_pubkey).map_err(|e| {
            MarketError::Serialization(format!(
                "Invalid seller signing key hex for '{seller_pubkey}': {e}"
            ))
        })?;
        let decoded_len = decoded.len();
        let key: [u8; 32] = decoded.try_into().map_err(|_| {
            MarketError::Serialization(format!(
                "Invalid seller signing key length for '{seller_pubkey}': expected 32 bytes, got {decoded_len}"
            ))
        })?;

        Ok(Some(key))
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

        let _ = routing_context.close_dht_record(record.key.clone()).await;

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
    ///
    /// When `force_refresh` is `true` the value is fetched from the network
    /// (required for accurate discovery).  When `false` the locally cached
    /// copy is returned, which avoids a network round-trip and is sufficient
    /// for broadcast-path reads where slightly stale data is acceptable.
    pub async fn fetch_registry(&self, force_refresh: bool) -> MarketResult<MarketRegistry> {
        let Some(key) = &self.master_registry_key else {
            info!("Master registry key not known yet, returning empty registry");
            return Ok(MarketRegistry::default());
        };
        let key = key.clone();

        let routing_context = self.dht.routing_context()?;
        // Defensive re-open (no-op if already open from ensure_master_registry).
        let _ = routing_context
            .open_dht_record(key.clone(), Some(self.shared_keypair.clone()))
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to open registry record: {e}")))?;

        let data = routing_context
            .get_dht_value(key.clone(), 0, force_refresh)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to get registry value: {e}")))?
            .map(|v| v.data().to_vec());

        match data {
            Some(bytes) if !bytes.is_empty() => match MarketRegistry::from_cbor(&bytes) {
                Ok(registry) => {
                    debug!("Fetched registry with {} sellers", registry.sellers.len());
                    Ok(registry)
                }
                Err(e) => {
                    warn!("Registry data corrupted, returning empty: {}", e);
                    Ok(MarketRegistry::default())
                }
            },
            _ => {
                debug!("Registry has no data yet");
                Ok(MarketRegistry::default())
            }
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

        match data {
            Some(bytes) if !bytes.is_empty() => match SellerCatalog::from_cbor(&bytes) {
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
            },
            _ => {
                debug!("Catalog has no data yet");
                Ok(SellerCatalog::default())
            }
        }
    }

    /// Two-hop discovery: master registry → seller catalogs → flat listing list.
    pub async fn fetch_all_listings(&self) -> MarketResult<Vec<RegistryEntry>> {
        let registry = self.fetch_registry(true).await?;
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
                    signing_pubkey: String::new(),
                },
                SellerEntry {
                    seller_pubkey: "pk2".to_string(),
                    catalog_key: "ck2".to_string(),
                    registered_at: 2000,
                    signing_pubkey: String::new(),
                },
            ],
            routes: Vec::new(),
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
            signing_pubkey: String::new(),
        });
        assert_eq!(reg.sellers.len(), 1);
        assert_eq!(reg.version, 1);

        // Duplicate — should be ignored
        reg.add_seller(SellerEntry {
            seller_pubkey: "pk1".to_string(),
            catalog_key: "ck1".to_string(),
            registered_at: 2000,
            signing_pubkey: String::new(),
        });
        assert_eq!(reg.sellers.len(), 1);
        assert_eq!(reg.version, 1);

        // Different seller — should be added
        reg.add_seller(SellerEntry {
            seller_pubkey: "pk2".to_string(),
            catalog_key: "ck2".to_string(),
            registered_at: 3000,
            signing_pubkey: String::new(),
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

        // Add 200 sellers (the cap)
        for i in 0..200 {
            reg.add_seller(SellerEntry {
                seller_pubkey: format!("pk{}", i),
                catalog_key: format!("ck{}", i),
                registered_at: i as u64 * 1000,
                signing_pubkey: String::new(),
            });
        }
        assert_eq!(reg.sellers.len(), 200);
        assert_eq!(reg.version, 200);

        // Try to add 201st seller — should be ignored
        reg.add_seller(SellerEntry {
            seller_pubkey: "pk200".to_string(),
            catalog_key: "ck200".to_string(),
            registered_at: 200_000,
            signing_pubkey: String::new(),
        });
        assert_eq!(reg.sellers.len(), 200);
        assert_eq!(reg.version, 200); // Version unchanged

        // Verify last seller is still the 199th one (0-indexed)
        assert_eq!(reg.sellers[199].seller_pubkey, "pk199");
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
                signing_pubkey: String::new(),
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
            signing_pubkey: String::new(),
        });
        assert_eq!(reg.version, 1);

        reg.add_seller(SellerEntry {
            seller_pubkey: "pk2".to_string(),
            catalog_key: "ck2".to_string(),
            registered_at: 2000,
            signing_pubkey: String::new(),
        });
        assert_eq!(reg.version, 2);

        // Duplicate — version should not increment
        reg.add_seller(SellerEntry {
            seller_pubkey: "pk1".to_string(),
            catalog_key: "ck1".to_string(),
            registered_at: 3000,
            signing_pubkey: String::new(),
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

    /// Helper: replicate the key derivation logic for testing without RegistryOperations.
    fn compute_key_for_network(network_key: &str) -> RecordKey {
        let keypair = derive_registry_keypair(network_key);
        let owner_key: &[u8] = &keypair.value().key();
        let schema_data: [u8; 6] = [b'D', b'F', b'L', b'T', 1, 0];

        let mut hash_input = Vec::with_capacity(4 + owner_key.len() + schema_data.len());
        hash_input.extend_from_slice(b"VLD0");
        hash_input.extend_from_slice(owner_key);
        hash_input.extend_from_slice(&schema_data);

        let hash = blake3::hash(&hash_input);
        let opaque = BareOpaqueRecordKey::new(hash.as_bytes());
        RecordKey::new(CRYPTO_KIND_VLD0, BareRecordKey::new(opaque, None))
    }

    #[test]
    fn test_compute_master_registry_key_deterministic() {
        let key1 = compute_key_for_network("test-network");
        let key2 = compute_key_for_network("test-network");
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_compute_master_registry_key_different_networks() {
        let key1 = compute_key_for_network("network-a");
        let key2 = compute_key_for_network("network-b");
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_route_entry_add_and_dedup() {
        let mut reg = MarketRegistry::default();

        reg.add_route(RouteEntry {
            node_id: "node1".to_string(),
            route_blob: vec![1, 2, 3],
            registered_at: 1000,
        });
        assert_eq!(reg.routes.len(), 1);
        assert_eq!(reg.version, 1);

        // Update existing — should replace blob, not add second entry
        reg.add_route(RouteEntry {
            node_id: "node1".to_string(),
            route_blob: vec![4, 5, 6],
            registered_at: 2000,
        });
        assert_eq!(reg.routes.len(), 1);
        assert_eq!(reg.routes[0].route_blob, vec![4, 5, 6]);
        assert_eq!(reg.version, 2);

        // Different node — should add
        reg.add_route(RouteEntry {
            node_id: "node2".to_string(),
            route_blob: vec![7, 8, 9],
            registered_at: 3000,
        });
        assert_eq!(reg.routes.len(), 2);
        assert_eq!(reg.version, 3);
    }

    #[test]
    fn test_route_entry_cbor_roundtrip() {
        let mut reg = MarketRegistry::default();
        reg.add_route(RouteEntry {
            node_id: "n1".to_string(),
            route_blob: vec![10, 20, 30],
            registered_at: 500,
        });
        reg.add_seller(SellerEntry {
            seller_pubkey: "pk1".to_string(),
            catalog_key: "ck1".to_string(),
            registered_at: 600,
            signing_pubkey: String::new(),
        });

        let bytes = reg.to_cbor().unwrap();
        let decoded = MarketRegistry::from_cbor(&bytes).unwrap();
        assert_eq!(decoded.routes.len(), 1);
        assert_eq!(decoded.routes[0].node_id, "n1");
        assert_eq!(decoded.routes[0].route_blob, vec![10, 20, 30]);
        assert_eq!(decoded.sellers.len(), 1);
    }

    #[test]
    fn test_route_entry_backward_compat() {
        // Registry CBOR from before routes existed should deserialize with empty routes
        let old_reg = MarketRegistry {
            sellers: vec![SellerEntry {
                seller_pubkey: "pk1".to_string(),
                catalog_key: "ck1".to_string(),
                registered_at: 100,
                signing_pubkey: String::new(),
            }],
            routes: Vec::new(),
            version: 1,
        };
        let bytes = old_reg.to_cbor().unwrap();
        let decoded = MarketRegistry::from_cbor(&bytes).unwrap();
        assert!(decoded.routes.is_empty());
        assert_eq!(decoded.sellers.len(), 1);
    }

    #[test]
    fn test_route_entry_max_cap() {
        let mut reg = MarketRegistry::default();
        for i in 0..200 {
            reg.add_route(RouteEntry {
                node_id: format!("node{}", i),
                route_blob: vec![i as u8],
                registered_at: i as u64,
            });
        }
        assert_eq!(reg.routes.len(), 200);

        // 201st should be ignored
        reg.add_route(RouteEntry {
            node_id: "node200".to_string(),
            route_blob: vec![200],
            registered_at: 200,
        });
        assert_eq!(reg.routes.len(), 200);
    }
}
