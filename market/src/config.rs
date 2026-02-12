//! Configuration constants for the marketplace application.
//!
//! This module centralizes magic numbers and configuration values
//! to improve maintainability and enable easier tuning.

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

/// Default network key for shared-keypair registry derivation.
pub const DEFAULT_NETWORK_KEY: &str = "development-network-2025";

/// Environment variable to override the network key.
pub const MARKET_NETWORK_KEY_ENV: &str = "MARKET_NETWORK_KEY";

/// Return the network key used for shared-keypair derivation.
pub fn network_key() -> String {
    std::env::var(MARKET_NETWORK_KEY_ENV).unwrap_or_else(|_| DEFAULT_NETWORK_KEY.to_string())
}

/// Default MP-SPDZ directory path.
pub const DEFAULT_MP_SPDZ_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../MP-SPDZ");

/// Environment variable for MP-SPDZ directory override.
pub const MP_SPDZ_DIR_ENV: &str = "MP_SPDZ_DIR";

/// Return the current Unix timestamp in seconds.
///
/// This is a convenience wrapper that avoids the boilerplate of
/// `SystemTimeProvider::new().now_unix()` in production code paths.
/// For testable code, prefer accepting a `TimeProvider` parameter instead.
pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("System clock is before Unix epoch")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now_unix_reasonable() {
        let now = now_unix();
        // Verify timestamp is reasonable (after 2023-11-01)
        // 1700000000 = 2023-11-14 22:13:20 UTC
        assert!(now > 1700000000, "Timestamp should be after 2023");
        // Verify it's not too far in the future (before 2030)
        // 1900000000 = 2030-03-15 01:06:40 UTC
        assert!(now < 1900000000, "Timestamp should be before 2030");
    }

    #[test]
    fn test_network_key_default() {
        // Ensure env var is not set for this test
        std::env::remove_var(MARKET_NETWORK_KEY_ENV);

        let key = network_key();
        assert_eq!(key, DEFAULT_NETWORK_KEY);
    }

    #[test]
    fn test_network_key_from_env() {
        // Set env var
        std::env::set_var(MARKET_NETWORK_KEY_ENV, "custom-network");

        let key = network_key();
        assert_eq!(key, "custom-network");

        // Clean up
        std::env::remove_var(MARKET_NETWORK_KEY_ENV);
    }

    #[test]
    fn test_subkey_constants() {
        // Verify the expected subkey values
        assert_eq!(subkeys::LISTING, 0);
        assert_eq!(subkeys::BID_INDEX, 1);
        assert_eq!(subkeys::BID_ANNOUNCEMENTS, 2);
        assert_eq!(subkeys::BIDDER_REGISTRY, 3);
    }

    #[test]
    fn test_dht_subkey_count() {
        // Verify subkey count matches the number of defined subkeys
        assert_eq!(DHT_SUBKEY_COUNT, 4);
    }
}
