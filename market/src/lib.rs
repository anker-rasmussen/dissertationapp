//! P2P sealed-bid auction marketplace over Veilid with MASCOT MPC.
//!
//! Provides a two-tier coordination architecture: [`veilid::AuctionLogic`] for
//! testable auction state machines, and [`veilid::AuctionCoordinator`] for the
//! real Veilid network integration. Bids are committed via SHA256, resolved by
//! multi-party computation (MP-SPDZ MASCOT protocol), and listing content is
//! encrypted with AES-256-GCM until the winner is verified.

#![recursion_limit = "256"]
// ── Clippy lint configuration (strict, like Airbnb for Rust) ──
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![warn(clippy::cargo)]
#![warn(clippy::perf)]
#![warn(clippy::unwrap_used)]
#![warn(clippy::expect_used)]
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

pub mod actions;
pub mod config;
pub mod crypto;
pub mod error;
pub mod marketplace;
pub mod shared_state;
pub mod traits;
pub mod util;
pub mod veilid;

pub use error::{MarketError, MarketResult};

#[cfg(any(test, feature = "test"))]
pub mod mocks;

pub use crypto::{decrypt_content, encrypt_content, generate_key, ContentKey, ContentNonce};
pub use marketplace::{
    Bid, BidCollection, BidIndex, BidRecord, Listing, ListingStatus, PublicListing, SealedBid,
};
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
pub use veilid::node::{DevNetConfig, VeilidNode};
pub use veilid::registry::RegistryOperations;
pub use veilid::registry_types::{
    CatalogEntry, MarketRegistry, RegistryEntry, SellerCatalog, SellerEntry,
};
