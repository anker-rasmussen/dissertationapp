//! Veilid integration layer: P2P networking, DHT storage, MPC tunnel, and route management.
//!
//! This module bridges the generic auction logic with the Veilid network. Key types:
//! [`AuctionCoordinator`] (real network coordinator), [`MpcTunnelProxy`] (TCP-over-Veilid
//! tunnel for MP-SPDZ), [`MpcRouteManager`] (MPC party route lifecycle), and
//! [`DHTOperations`] (atomic DHT read-modify-write).

pub mod auction_coordinator;
pub mod auction_logic;
pub mod bid_announcement;
pub mod bid_ops;
pub mod bid_storage;
pub mod dht;
pub mod listing_ops;
pub mod mpc;
mod mpc_execution;
pub mod mpc_orchestrator;
pub mod mpc_routes;
mod mpc_verification;
pub use mpc_verification::verify_commitment;
pub mod node;
pub mod registry;
pub mod registry_types;

pub use auction_coordinator::AuctionCoordinator;
pub use auction_logic::AuctionLogic;
pub use bid_ops::BidOperations;
pub use bid_storage::BidStorage;
pub use dht::DHTOperations;
pub use listing_ops::ListingOperations;
pub use mpc::MpcTunnelProxy;
pub use mpc_routes::MpcRouteManager;
pub use node::VeilidNode;
pub use registry::RegistryOperations;
pub use registry_types::{CatalogEntry, MarketRegistry, RegistryEntry, SellerCatalog, SellerEntry};
