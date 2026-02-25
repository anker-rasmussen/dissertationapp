//! Configuration constants for the marketplace application.
//!
//! This module centralizes magic numbers and configuration values
//! to improve maintainability and enable easier tuning.

/// Number of DHT subkeys per record.
/// - Subkey 0: Primary data (listing)
/// - Subkey 1: Bid announcement registry (G-Set CRDT, seller-owned)
pub const DHT_SUBKEY_COUNT: u16 = 2;

/// Subkey indices for DHT record data.
pub mod subkeys {
    /// Primary listing data.
    pub const LISTING: u32 = 0;
    /// Bid announcement registry (state-based G-Set, merged at read time).
    pub const BID_ANNOUNCEMENTS: u32 = 1;
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
    #[allow(clippy::expect_used)]
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("System clock is before Unix epoch")
        .as_secs()
}

/// Timeout for individual app_message sends (control messages).
pub const APP_MESSAGE_TIMEOUT_SECS: u64 = 10;

/// Timeout for auctions stuck in pending-MPC state.
pub const AUCTION_STALE_TIMEOUT_SECS: u64 = 600;

/// Default node offset for devnet deployments.
/// Determines port (5160 + offset) and IP (1.2.3.1 + offset).
pub const DEFAULT_NODE_OFFSET: u16 = 9;

/// Default bootstrap nodes for devnet.
pub const DEFAULT_BOOTSTRAP_NODES: &[&str] = &["udp://1.2.3.1:5160"];

/// Environment variable to override bootstrap node addresses (comma-separated).
pub const MARKET_BOOTSTRAP_NODES_ENV: &str = "MARKET_BOOTSTRAP_NODES";

/// Environment variable to override the listen address (e.g., "0.0.0.0").
/// When set, the market node listens on this address instead of 127.0.0.1.
pub const MARKET_LISTEN_ADDR_ENV: &str = "MARKET_LISTEN_ADDR";

/// Environment variable to override the public address advertised to peers.
/// Format: "ip:port" (e.g., "100.64.0.1:5180"). When unset, computed from offset.
pub const MARKET_PUBLIC_ADDR_ENV: &str = "MARKET_PUBLIC_ADDR";

/// Default update channel capacity for Veilid updates.
pub const DEFAULT_UPDATE_CHANNEL_CAPACITY: usize = 4096;

/// Default timeout for attachment operations (waiting for AttachedWeak state).
pub const DEFAULT_ATTACHMENT_TIMEOUT_SECS: u64 = 20;

/// Default wait time for MPC route establishment.
pub const DEFAULT_MPC_ROUTE_WAIT_SECS: u64 = 20;

/// Default timeout for MPC execution.
pub const DEFAULT_MPC_EXECUTION_TIMEOUT_SECS: u64 = 900;

/// Default Veilid RPC timeout in milliseconds.
///
/// Controls how long `app_call` waits internally for a response.
/// Lower values detect dead routes faster during MPC data transfer
/// (reducing per-retry overhead from 10s to 5s).  DHT operations
/// typically complete in 1-3s so 5s is sufficient for bootstrapping.
pub const DEFAULT_RPC_TIMEOUT_MS: u32 = 5_000;

/// Default MPC protocol binary name.
///
/// MASCOT: dishonest majority, malicious security, N >= 2 parties.
/// Override via `MPC_PROTOCOL` env var for alternative protocols.
pub const DEFAULT_MPC_PROTOCOL: &str = "mascot-party.x";

// --- MPC Orchestrator ---

/// Delay before first route collection check (allows late parties to publish).
pub const MPC_ROUTE_ANNOUNCE_DELAY_SECS: u64 = 2;

/// Maximum wait for all parties to publish MPC routes.
pub const MPC_ROUTE_COLLECTION_TIMEOUT_SECS: u64 = 45;

/// Maximum wait for all parties to signal `MpcReady`.
pub const MPC_READINESS_BARRIER_TIMEOUT_SECS: u64 = 120;

/// Pause after barrier passes to let slower parties also pass.
pub const MPC_POST_BARRIER_SETTLE_SECS: u64 = 5;

/// Poll interval inside the readiness barrier loop.
pub const MPC_BARRIER_POLL_SECS: u64 = 1;

/// Wait for peer route refresh announcements after barrier.
pub const MPC_ROUTE_PROPAGATION_WAIT_SECS: u64 = 8;

/// Timeout for MPC tunnel warmup (Ping round-trip to all peers).
pub const MPC_TUNNEL_WARMUP_TIMEOUT_SECS: u64 = 300;

// --- MPC Tunnel ---

/// Interval between Ping messages during MPC tunnel warmup.
pub const MPC_PING_INTERVAL_SECS: u64 = 3;

/// Interval for Open re-send retries to handle route-death.
pub const MPC_OPEN_RESEND_INTERVAL_SECS: u64 = 5;

/// Post-accept delay while holding the local connect mutex (ms).
pub const MPC_POST_ACCEPT_DELAY_MS: u64 = 20;

/// Delay between releasing an imported route (ms).
pub const MPC_ROUTE_RELEASE_DELAY_MS: u64 = 100;

/// Interval for MPC data flow progress logging.
pub const MPC_PROGRESS_LOG_INTERVAL_SECS: u64 = 30;

/// Retry interval for local MP-SPDZ TCP connect on Open.
pub const MPC_ACCEPT_RETRY_SECS: u64 = 1;

// --- MPC Execution ---

/// Timeout for reaping a killed MP-SPDZ zombie process.
pub const MPC_ZOMBIE_REAP_TIMEOUT_SECS: u64 = 5;

// --- Auction Coordinator ---

/// Timeout for sending WinnerDecryptionRequest / WinnerBidReveal.
pub const WINNER_REVEAL_TIMEOUT_SECS: u64 = 20;

/// Retry interval when sending challenges / reveals.
pub const WINNER_REVEAL_RETRY_SECS: u64 = 1;

/// Monitoring loop poll interval (seconds).
pub const MONITOR_POLL_INTERVAL_SECS: u64 = 5;

/// Maximum wait for peer route blobs before MPC.
pub const PEER_ROUTE_WAIT_SECS: u64 = 15;

// --- DHT Retry ---

/// Initial delay for DHT retry with exponential backoff (ms).
pub const DHT_RETRY_INITIAL_DELAY_MS: u64 = 50;

/// Environment variable to override the MPC protocol binary.
pub const MPC_PROTOCOL_ENV: &str = "MPC_PROTOCOL";

use std::path::PathBuf;

/// Centralized configuration for the marketplace application.
///
/// This struct consolidates all runtime configuration parameters,
/// enabling easier testing, deployment flexibility, and reduced
/// coupling to environment variables throughout the codebase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketConfig {
    /// Network key for shared-keypair registry derivation.
    pub network_key: String,
    /// Bootstrap node addresses (e.g., "udp://1.2.3.1:5160").
    pub bootstrap_nodes: Vec<String>,
    /// Path to MP-SPDZ installation directory.
    pub mp_spdz_dir: PathBuf,
    /// Whether to use insecure (unencrypted) protected storage.
    /// Should be `false` in production.
    pub insecure_storage: bool,
    /// Port offset for devnet deployments (base port = 5160).
    pub node_offset: u16,
    /// Timeout (seconds) for Veilid attachment operations.
    pub attachment_timeout_secs: u64,
    /// Wait time (seconds) for MPC route establishment.
    pub mpc_route_wait_secs: u64,
    /// Timeout (seconds) for MPC execution.
    pub mpc_execution_timeout_secs: u64,
    /// Timeout (seconds) for individual app_message sends.
    pub app_message_timeout_secs: u64,
    /// Capacity for Veilid update channel.
    pub update_channel_capacity: usize,
    /// Timeout (seconds) for auctions stuck in pending-MPC state.
    pub auction_stale_timeout_secs: u64,
    /// Routing table limit for over-attached peers.
    pub limit_over_attached: u32,
    /// Maximum seconds to wait for network attachment.
    pub max_attachment_wait_secs: u64,
    /// Veilid RPC timeout in milliseconds (default: 10_000).
    /// Controls how long app_call waits for a response before timing out.
    pub rpc_timeout_ms: u32,
    /// Override listen address for Veilid protocol sockets (e.g., "0.0.0.0" for tailnet).
    /// When `None`, devnet defaults to `127.0.0.1`.
    pub listen_addr: Option<String>,
    /// Override public address advertised to peers (e.g., "100.64.0.1:5180").
    /// When `None`, devnet computes from offset (`1.2.3.X:port`).
    pub public_addr: Option<String>,
}

impl MarketConfig {
    /// Construct configuration from environment variables with sensible defaults.
    ///
    /// Environment variables:
    /// - `MARKET_NETWORK_KEY`: Network key (default: "development-network-2025")
    /// - `MP_SPDZ_DIR`: MP-SPDZ directory path
    /// - `MARKET_NODE_OFFSET`: Port offset for devnet (default: 9)
    /// - `MARKET_INSECURE_STORAGE`: Set to "true" to enable (default: false)
    ///
    /// All timeout values use hardcoded defaults unless explicitly overridden
    /// via `with_*` builder methods.
    #[must_use]
    pub fn from_env() -> Self {
        let network_key = network_key();

        let mp_spdz_dir = std::env::var(MP_SPDZ_DIR_ENV)
            .unwrap_or_else(|_| DEFAULT_MP_SPDZ_DIR.to_string())
            .into();

        let node_offset = std::env::var("MARKET_NODE_OFFSET")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(DEFAULT_NODE_OFFSET);

        let insecure_storage = std::env::var("MARKET_INSECURE_STORAGE")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let bootstrap_nodes = std::env::var(MARKET_BOOTSTRAP_NODES_ENV)
            .ok()
            .filter(|s| !s.is_empty())
            .map_or_else(
                || {
                    DEFAULT_BOOTSTRAP_NODES
                        .iter()
                        .map(|s| (*s).to_string())
                        .collect()
                },
                |s| s.split(',').map(|t| t.trim().to_string()).collect(),
            );

        let listen_addr = std::env::var(MARKET_LISTEN_ADDR_ENV)
            .ok()
            .filter(|s| !s.is_empty());

        let public_addr = std::env::var(MARKET_PUBLIC_ADDR_ENV)
            .ok()
            .filter(|s| !s.is_empty());

        Self {
            network_key,
            bootstrap_nodes,
            mp_spdz_dir,
            insecure_storage,
            node_offset,
            attachment_timeout_secs: DEFAULT_ATTACHMENT_TIMEOUT_SECS,
            mpc_route_wait_secs: DEFAULT_MPC_ROUTE_WAIT_SECS,
            mpc_execution_timeout_secs: DEFAULT_MPC_EXECUTION_TIMEOUT_SECS,
            app_message_timeout_secs: APP_MESSAGE_TIMEOUT_SECS,
            update_channel_capacity: DEFAULT_UPDATE_CHANNEL_CAPACITY,
            auction_stale_timeout_secs: AUCTION_STALE_TIMEOUT_SECS,
            limit_over_attached: 16,
            max_attachment_wait_secs: 180,
            rpc_timeout_ms: DEFAULT_RPC_TIMEOUT_MS,
            listen_addr,
            public_addr,
        }
    }
}

impl Default for MarketConfig {
    /// Default configuration matches current hardcoded values for devnet.
    fn default() -> Self {
        Self {
            network_key: DEFAULT_NETWORK_KEY.to_string(),
            bootstrap_nodes: DEFAULT_BOOTSTRAP_NODES
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
            mp_spdz_dir: DEFAULT_MP_SPDZ_DIR.into(),
            insecure_storage: false,
            node_offset: DEFAULT_NODE_OFFSET,
            attachment_timeout_secs: DEFAULT_ATTACHMENT_TIMEOUT_SECS,
            mpc_route_wait_secs: DEFAULT_MPC_ROUTE_WAIT_SECS,
            mpc_execution_timeout_secs: DEFAULT_MPC_EXECUTION_TIMEOUT_SECS,
            app_message_timeout_secs: APP_MESSAGE_TIMEOUT_SECS,
            update_channel_capacity: DEFAULT_UPDATE_CHANNEL_CAPACITY,
            auction_stale_timeout_secs: AUCTION_STALE_TIMEOUT_SECS,
            limit_over_attached: 16,
            max_attachment_wait_secs: 180,
            rpc_timeout_ms: DEFAULT_RPC_TIMEOUT_MS,
            listen_addr: None,
            public_addr: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Serializes tests that mutate the `MARKET_NETWORK_KEY` env var
    /// to avoid race conditions from parallel test execution.
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn test_network_key_default() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::remove_var(MARKET_NETWORK_KEY_ENV);

        let key = network_key();
        assert_eq!(key, DEFAULT_NETWORK_KEY);
    }

    #[test]
    fn test_network_key_from_env() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::set_var(MARKET_NETWORK_KEY_ENV, "custom-network");

        let key = network_key();
        assert_eq!(key, "custom-network");

        // Clean up
        std::env::remove_var(MARKET_NETWORK_KEY_ENV);
    }

    #[test]
    fn test_market_config_from_env_defaults() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::remove_var(MARKET_NETWORK_KEY_ENV);
        std::env::remove_var(MP_SPDZ_DIR_ENV);
        std::env::remove_var("MARKET_NODE_OFFSET");
        std::env::remove_var("MARKET_INSECURE_STORAGE");
        std::env::remove_var(MARKET_BOOTSTRAP_NODES_ENV);
        std::env::remove_var(MARKET_LISTEN_ADDR_ENV);
        std::env::remove_var(MARKET_PUBLIC_ADDR_ENV);

        let config = MarketConfig::from_env();
        assert_eq!(config.network_key, DEFAULT_NETWORK_KEY);
        assert_eq!(config.node_offset, DEFAULT_NODE_OFFSET);
        assert!(!config.insecure_storage);
        assert!(config.listen_addr.is_none());
        assert!(config.public_addr.is_none());
    }

    #[test]
    fn test_market_config_from_env_override() {
        let _guard = ENV_MUTEX.lock().unwrap();
        std::env::set_var(MARKET_NETWORK_KEY_ENV, "test-network");
        std::env::set_var(MP_SPDZ_DIR_ENV, "/custom/path");
        std::env::set_var("MARKET_NODE_OFFSET", "15");
        std::env::set_var("MARKET_INSECURE_STORAGE", "true");
        std::env::set_var(
            MARKET_BOOTSTRAP_NODES_ENV,
            "udp://10.0.0.1:5160,udp://10.0.0.2:5160",
        );
        std::env::set_var(MARKET_LISTEN_ADDR_ENV, "0.0.0.0");
        std::env::set_var(MARKET_PUBLIC_ADDR_ENV, "100.64.0.1:5180");

        let config = MarketConfig::from_env();
        assert_eq!(config.network_key, "test-network");
        assert_eq!(config.mp_spdz_dir.to_str().unwrap(), "/custom/path");
        assert_eq!(config.node_offset, 15);
        assert!(config.insecure_storage);
        assert_eq!(
            config.bootstrap_nodes,
            vec!["udp://10.0.0.1:5160", "udp://10.0.0.2:5160"]
        );
        assert_eq!(config.listen_addr.as_deref(), Some("0.0.0.0"));
        assert_eq!(config.public_addr.as_deref(), Some("100.64.0.1:5180"));

        // Clean up
        std::env::remove_var(MARKET_NETWORK_KEY_ENV);
        std::env::remove_var(MP_SPDZ_DIR_ENV);
        std::env::remove_var("MARKET_NODE_OFFSET");
        std::env::remove_var("MARKET_INSECURE_STORAGE");
        std::env::remove_var(MARKET_BOOTSTRAP_NODES_ENV);
        std::env::remove_var(MARKET_LISTEN_ADDR_ENV);
        std::env::remove_var(MARKET_PUBLIC_ADDR_ENV);
    }

    #[test]
    fn test_market_config_insecure_storage_case_insensitive() {
        let _guard = ENV_MUTEX.lock().unwrap();

        for value in ["true", "TRUE", "True", "TrUe"] {
            std::env::set_var("MARKET_INSECURE_STORAGE", value);
            let config = MarketConfig::from_env();
            assert!(config.insecure_storage, "Failed for value: {}", value);
        }

        for value in ["false", "FALSE", "0", "no", ""] {
            std::env::set_var("MARKET_INSECURE_STORAGE", value);
            let config = MarketConfig::from_env();
            assert!(!config.insecure_storage, "Failed for value: {}", value);
        }

        std::env::remove_var("MARKET_INSECURE_STORAGE");
    }
}
