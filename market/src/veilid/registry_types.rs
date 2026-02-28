//! Data types for the marketplace registry and seller catalogs.
//!
//! Extracted from `registry.rs` to separate type definitions from DHT operations.

use serde::{Deserialize, Serialize};

use crate::traits::TimeProvider;

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

    pub fn from_cbor(data: &[u8]) -> crate::error::MarketResult<Self> {
        crate::util::cbor_from_limited_reader(data, crate::util::MAX_DHT_VALUE_SIZE)
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

    /// G-Set merge: union sellers and routes from `other` into `self`.
    ///
    /// Pure add-only, dedup by key field, commutative + associative + idempotent.
    /// Does not use `version` — version is only bumped by individual `add_*` calls
    /// for DHT conflict detection.
    pub fn merge(&mut self, other: &Self) {
        for seller in &other.sellers {
            if self.sellers.len() >= 200 {
                break;
            }
            if !self
                .sellers
                .iter()
                .any(|s| s.seller_pubkey == seller.seller_pubkey)
            {
                self.sellers.push(seller.clone());
            }
        }
        for route in &other.routes {
            if self.routes.len() >= 200 {
                break;
            }
            if let Some(existing) = self.routes.iter_mut().find(|r| r.node_id == route.node_id) {
                // Keep the fresher route
                if route.registered_at > existing.registered_at {
                    existing.route_blob.clone_from(&route.route_blob);
                    existing.registered_at = route.registered_at;
                }
            } else {
                self.routes.push(route.clone());
            }
        }
    }

    /// Add a seller, deduplicating by pubkey.
    /// Caps the registry at 200 sellers to prevent unbounded growth.
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

    pub fn from_cbor(data: &[u8]) -> crate::error::MarketResult<Self> {
        crate::util::cbor_from_limited_reader(data, crate::util::MAX_DHT_VALUE_SIZE)
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
