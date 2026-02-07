//! Configuration constants for the marketplace application.
//!
//! This module centralizes magic numbers and configuration values
//! to improve maintainability and enable easier tuning.

/// Buffer size for MPC network messages (must stay under Veilid's 32KB limit).
pub const MPC_BUFFER_SIZE: usize = 32_000;

/// Default hop count for Veilid private routes.
pub const DEFAULT_HOP_COUNT: usize = 2;

/// Interval in seconds between registry cleanup operations.
pub const REGISTRY_CLEANUP_INTERVAL_SECS: u64 = 3600;

/// Interval in seconds between auction deadline polls.
pub const AUCTION_POLL_INTERVAL_SECS: u64 = 10;

/// Time to wait for MPC parties to be ready before starting computation.
pub const MPC_PARTY_WAIT_SECS: u64 = 5;

/// Maximum retries for network attachment operations.
pub const NETWORK_ATTACH_MAX_RETRIES: u32 = 30;

/// Maximum retries for DHT bid registration with optimistic concurrency.
pub const BID_REGISTER_MAX_RETRIES: u32 = 10;

/// Initial delay for bid registration retry (doubles on each retry).
pub const BID_REGISTER_INITIAL_DELAY_MS: u64 = 50;

/// Number of DHT subkeys per record.
/// - Subkey 0: Primary data (listing)
/// - Subkey 1: Bid index
/// - Subkey 2: Bid announcement registry
/// - Subkey 3: Bidder registry
pub const DHT_SUBKEY_COUNT: u16 = 4;

/// Subkey indices for DHT record data.
pub mod subkeys {
    /// Primary listing data.
    pub const LISTING: u32 = 0;
    /// Bid index for the listing.
    pub const BID_INDEX: u32 = 1;
    /// Bid announcement registry.
    pub const BID_ANNOUNCEMENTS: u32 = 2;
    /// Bidder registry for MPC coordination.
    pub const BIDDER_REGISTRY: u32 = 3;
}

/// Base port for MPC network communication.
pub const MPC_BASE_PORT: u16 = 5000;

/// Port offset multiplier per node (for running multiple nodes on same machine).
pub const MPC_PORT_OFFSET_MULTIPLIER: u16 = 10;

/// Time to wait for route collection before starting MPC.
pub const ROUTE_COLLECTION_INITIAL_WAIT_SECS: u64 = 2;

/// Maximum time to wait for route collection.
pub const ROUTE_COLLECTION_MAX_WAIT_SECS: u64 = 5;

/// Default MP-SPDZ directory path.
pub const DEFAULT_MP_SPDZ_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../MP-SPDZ");

/// Environment variable for MP-SPDZ directory override.
pub const MP_SPDZ_DIR_ENV: &str = "MP_SPDZ_DIR";

/// Environment variable for market node offset.
pub const MARKET_NODE_OFFSET_ENV: &str = "MARKET_NODE_OFFSET";

/// Return the current Unix timestamp in seconds.
///
/// This is a convenience wrapper that avoids the boilerplate of
/// `SystemTimeProvider::new().now_unix()` in production code paths.
/// For testable code, prefer accepting a `TimeProvider` parameter instead.
pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
