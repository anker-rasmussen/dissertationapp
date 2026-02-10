#![recursion_limit = "256"]
// ── Clippy lint configuration (strict, like Airbnb for Rust) ──
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![warn(clippy::cargo)]
// Pedantic lints that are too noisy for this codebase
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::doc_markdown)]
// Cargo lints not relevant (not publishing to crates.io)
#![allow(clippy::cargo_common_metadata)]
#![allow(clippy::multiple_crate_versions)]
// Nursery lints that fire on valid patterns
#![allow(clippy::redundant_pub_crate)]
#![allow(clippy::option_if_let_else)]

pub mod config;
pub mod crypto;
pub mod error;
pub mod marketplace;
pub mod traits;
pub mod veilid;

pub use error::{MarketError, MarketResult};

#[cfg(any(test, feature = "test"))]
pub mod mocks;

pub use config::*;
pub use crypto::{decrypt_content, encrypt_content, generate_key, ContentKey, ContentNonce};
pub use marketplace::{Bid, BidCollection, BidIndex, BidRecord, Listing, ListingStatus, SealedBid};
pub use traits::{DhtStore, TimeProvider};
// MessageTransport, MpcRunner, and RandomSource are only needed by
// AuctionLogic (test simulator) and mock infrastructure.
#[cfg(any(test, feature = "test"))]
pub use traits::{MessageTransport, MpcRunner, RandomSource};
pub use veilid::auction_coordinator::AuctionCoordinator;
pub use veilid::bid_ops::BidOperations;
pub use veilid::bid_storage::BidStorage;
pub use veilid::dht::{DHTOperations, OwnedDHTRecord};
pub use veilid::listing_ops::ListingOperations;
pub use veilid::mpc::MpcTunnelProxy;
pub use veilid::mpc_routes::MpcRouteManager;
pub use veilid::node::{DevNetConfig, VeilidNode};
pub use veilid::registry::{
    CatalogEntry, MarketRegistry, RegistryEntry, RegistryOperations, SellerCatalog, SellerEntry,
};
