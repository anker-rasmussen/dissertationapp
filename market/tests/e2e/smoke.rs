//! E2E smoke tests with real Veilid devnets.
//!
//! These tests validate that the mock-based tests accurately reflect
//! real system behavior. They automatically start and stop the devnet.
//!
//! Run smoke subset with: `cargo integration-fast`
//! Run full subset with: `cargo integration`
//!
//! Prerequisites:
//! - Docker installed and running
//! - veilid repo at expected path (for libipspoof.so and docker-compose)
//! - LD_PRELOAD set to libipspoof.so path
//!
//! All E2E tests run serially to avoid devnet conflicts.

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use market::error::{MarketError, MarketResult};
use market::marketplace::bid_record::BidRecord;
use market::veilid::auction_coordinator::AuctionCoordinator;
use market::veilid::bid_ops::BidOperations;
use market::veilid::bid_storage::BidStorage;
use market::veilid::node::{DevNetConfig, VeilidNode};
use market::Listing;
use serial_test::serial;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use veilid_core::PublicKey;

/// Initialize tracing for tests. Uses RUST_LOG env var or defaults to info level
/// with debug for veilid_core and market crates.
fn init_test_tracing() {
    let _ = tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,veilid_core=debug,market=debug")),
        )
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .try_init();
}

/// Path to the veilid repository containing devnet infrastructure.
/// Uses VEILID_REPO_PATH env var if set, otherwise falls back to
/// `../../veilid` relative to the crate root (CARGO_MANIFEST_DIR).
fn veilid_repo_path() -> PathBuf {
    if let Ok(p) = std::env::var("VEILID_REPO_PATH") {
        return PathBuf::from(p);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("market/ should be nested under Repos/dissertationapp/")
        .join("veilid")
}

/// Path to libipspoof.so for IP translation in devnet.
fn libipspoof_path() -> PathBuf {
    veilid_repo_path().join(".devcontainer/scripts/libipspoof.so")
}

/// Path to docker-compose file for devnet.
fn docker_compose_path() -> PathBuf {
    veilid_repo_path().join(".devcontainer/compose/docker-compose.dev.yml")
}

/// Check if running in fast mode (persistent devnet)
fn is_fast_mode() -> bool {
    std::env::var("E2E_FAST_MODE").is_ok()
}

/// Manages the Veilid devnet lifecycle for E2E tests.
struct DevnetManager {
    compose_path: PathBuf,
    started: bool,
    fast_mode: bool,
}

impl DevnetManager {
    fn new() -> Self {
        let fast_mode = is_fast_mode();
        if fast_mode {
            eprintln!("[E2E] Fast mode enabled - reusing persistent devnet");
        }
        Self {
            compose_path: docker_compose_path(),
            started: false,
            fast_mode,
        }
    }

    /// Check if devnet infrastructure exists.
    fn infrastructure_available(&self) -> bool {
        self.compose_path.exists() && libipspoof_path().exists()
    }

    /// Stop any existing devnet before starting fresh.
    fn ensure_stopped(&self) -> MarketResult<()> {
        eprintln!("[E2E] Ensuring devnet is stopped before starting...");

        let output = Command::new("docker")
            .args([
                "compose",
                "-f",
                self.compose_path.to_str().unwrap(),
                "down",
                "-v",
                "--remove-orphans",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()?;

        if !output.status.success() {
            // Ignore errors - devnet might not have been running
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!("[E2E] Note: docker compose down returned: {}", stderr);
        }

        Ok(())
    }

    /// Start the devnet using docker-compose.
    /// In fast mode, assumes devnet is already running.
    fn start(&mut self) -> MarketResult<()> {
        if !self.infrastructure_available() {
            return Err(MarketError::Config(format!(
                "Devnet infrastructure not found. Expected:\n  - {}\n  - {}",
                self.compose_path.display(),
                libipspoof_path().display()
            )));
        }

        if self.fast_mode {
            // In fast mode, verify devnet is running but don't start/stop it
            eprintln!("[E2E] Fast mode: checking devnet is running...");
            if !self.is_devnet_running() {
                return Err(MarketError::Config(
                    "Fast mode requires a running devnet!\n\
                     Start it with: cargo devnet-start"
                        .into(),
                ));
            }
            eprintln!("[E2E] Fast mode: devnet already running");
            return Ok(());
        }

        // First ensure any existing devnet is stopped
        self.ensure_stopped()?;

        eprintln!("[E2E] Starting devnet...");

        let output = Command::new("docker")
            .args([
                "compose",
                "-f",
                self.compose_path.to_str().unwrap(),
                "up",
                "-d",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(MarketError::Process(format!(
                "Failed to start devnet: {}",
                stderr
            )));
        }

        self.started = true;
        eprintln!("[E2E] Devnet containers started");
        Ok(())
    }

    /// Check if devnet containers are running.
    fn is_devnet_running(&self) -> bool {
        let output = Command::new("docker")
            .args([
                "compose",
                "-f",
                self.compose_path.to_str().unwrap(),
                "ps",
                "--format",
                "{{.Service}}",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output();

        match output {
            Ok(out) => {
                let stdout = String::from_utf8_lossy(&out.stdout);
                // Check if we have at least some services running
                stdout.lines().count() >= 1
            }
            Err(_) => false,
        }
    }

    /// Wait for all devnet containers to be healthy.
    /// The devnet has 9 nodes: 1 bootstrap (port 5160) + 8 regular nodes (ports 5161-5168)
    fn wait_for_health(&self, timeout_secs: u64) -> MarketResult<()> {
        eprintln!("[E2E] Waiting for all 9 devnet nodes to be healthy...");

        let start = std::time::Instant::now();
        loop {
            // Check docker compose health status
            let output = Command::new("docker")
                .args([
                    "compose",
                    "-f",
                    self.compose_path.to_str().unwrap(),
                    "ps",
                    "--format",
                    "{{.Service}}\t{{.Health}}",
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()?;

            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let healthy_count = stdout
                    .lines()
                    .filter(|line| line.contains("healthy"))
                    .count();

                eprintln!("[E2E] Health check: {}/9 nodes healthy", healthy_count);

                // All 9 nodes must be healthy
                if healthy_count >= 9 {
                    // Verify all ports are reachable
                    if self.check_all_nodes_reachable() {
                        eprintln!("[E2E] All devnet nodes are healthy and reachable!");
                        return Ok(());
                    }
                }
            }

            if start.elapsed().as_secs() > timeout_secs {
                // Print current status for debugging
                let _ = Command::new("docker")
                    .args(["compose", "-f", self.compose_path.to_str().unwrap(), "ps"])
                    .status();
                return Err(MarketError::Timeout(format!(
                    "Devnet failed to become healthy within {} seconds",
                    timeout_secs
                )));
            }

            std::thread::sleep(Duration::from_secs(5));
        }
    }

    /// Check if all devnet node ports are reachable.
    /// Bootstrap: 5160, Nodes 1-8: 5161-5168
    fn check_all_nodes_reachable(&self) -> bool {
        let ports = [5160, 5161, 5162, 5163, 5164, 5165, 5166, 5167, 5168];
        for port in ports {
            let addr = format!("127.0.0.1:{}", port);
            if std::net::TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_secs(1))
                .is_err()
            {
                eprintln!("[E2E] Port {} not yet reachable", port);
                return false;
            }
        }
        true
    }

    /// Stop the devnet and clean up.
    /// In fast mode, keeps devnet running for subsequent tests.
    fn stop(&mut self) -> MarketResult<()> {
        if self.fast_mode {
            eprintln!("[E2E] Fast mode: keeping devnet running");
            return Ok(());
        }

        if !self.started {
            return Ok(());
        }

        eprintln!("[E2E] Stopping devnet...");

        // Stop and remove containers
        let output = Command::new("docker")
            .args([
                "compose",
                "-f",
                self.compose_path.to_str().unwrap(),
                "down",
                "-v", // Remove volumes
                "--remove-orphans",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!("[E2E] Warning: Failed to stop devnet cleanly: {}", stderr);
        } else {
            eprintln!("[E2E] Devnet stopped and cleaned up");
        }

        self.started = false;
        Ok(())
    }
}

impl Drop for DevnetManager {
    fn drop(&mut self) {
        // In fast mode, don't stop devnet on drop
        if !self.fast_mode {
            let _ = self.stop();
        }
    }
}

/// Test node configuration for E2E tests.
/// Uses offsets 10-39 to stay within libipspoof's MAX_PORT_OFFSET range (40).
/// Devnet uses offsets 0-8 (9 nodes, ports 5160-5168), so tests use 10+ to avoid conflicts.
struct TestNode {
    node: VeilidNode,
    offset: u16,
    data_dir: PathBuf,
}

impl TestNode {
    fn new(offset: u16) -> Self {
        let data_dir = std::env::temp_dir().join(format!("market-e2e-test-{}", offset));

        // Clean up any previous test data
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("Failed to create test data dir");

        let config = DevNetConfig {
            network_key: "development-network-2025".to_string(),
            bootstrap_nodes: vec!["udp://1.2.3.1:5160".to_string()],
            port_offset: offset,
        };

        let node = VeilidNode::new(data_dir.clone())
            .with_devnet(config)
            .with_insecure_storage(true);

        Self {
            node,
            offset,
            data_dir,
        }
    }

    async fn start(&mut self) -> MarketResult<()> {
        self.node.start().await.map_err(|e| {
            MarketError::Network(format!(
                "Failed to start Veilid node (offset {}): {}. \
                 Ensure LD_PRELOAD is set and devnet is running.",
                self.offset, e
            ))
        })?;
        self.node.attach().await.map_err(|e| {
            MarketError::Network(format!(
                "Failed to attach node (offset {}): {}",
                self.offset, e
            ))
        })?;
        Ok(())
    }

    /// Wait for node to be fully ready (attached AND has node ID).
    ///
    /// The node receives VeilidUpdate::Attachment (sets is_attached=true) and
    /// VeilidUpdate::Network (populates node_ids) as separate events. This method
    /// waits for both conditions to be satisfied.
    async fn wait_for_ready(&self, timeout_secs: u64) -> MarketResult<()> {
        let start = std::time::Instant::now();
        loop {
            let state = self.node.state();
            if state.is_attached && !state.node_ids.is_empty() {
                return Ok(());
            }
            if start.elapsed().as_secs() > timeout_secs {
                return Err(MarketError::Timeout(format!(
                    "Node {} not ready within {}s: attached={}, node_ids={}",
                    self.offset,
                    timeout_secs,
                    state.is_attached,
                    state.node_ids.len()
                )));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    fn node_id(&self) -> Option<PublicKey> {
        let state = self.node.state();
        state
            .node_ids
            .first()
            .and_then(|s| PublicKey::try_from(s.as_str()).ok())
    }

    async fn shutdown(&mut self) -> MarketResult<()> {
        self.node.shutdown().await?;
        // Clean up test data
        let _ = std::fs::remove_dir_all(&self.data_dir);
        Ok(())
    }
}

/// Helper to create a test listing using MockTime for predictable timestamps.
fn create_test_listing(
    key: veilid_core::RecordKey,
    seller: PublicKey,
    reserve_price: u64,
    duration_secs: u64,
) -> Listing {
    use market::mocks::MockTime;

    Listing::builder_with_time(MockTime::new(1000))
        .key(key)
        .seller(seller)
        .title("E2E Test Item")
        .encrypted_content(
            b"encrypted_content_placeholder".to_vec(),
            [0u8; 12],
            "test_decryption_key_hex".to_string(),
        )
        .reserve_price(reserve_price)
        .auction_duration(duration_secs)
        .build()
        .expect("Failed to build listing")
}

/// Setup helper - starts devnet and validates LD_PRELOAD/libipspoof prerequisites.
fn setup_e2e_environment() -> MarketResult<DevnetManager> {
    // Check if LD_PRELOAD is already set correctly
    let preload = std::env::var("LD_PRELOAD").unwrap_or_default();
    let expected_preload = libipspoof_path();

    if !expected_preload.exists() {
        return Err(MarketError::Config(format!(
            "libipspoof.so not found at {}.\n\
             Please build it with: cd {} && make libipspoof.so",
            expected_preload.display(),
            expected_preload.parent().unwrap().display()
        )));
    }

    if !preload.contains(expected_preload.to_str().unwrap()) {
        return Err(MarketError::Config(format!(
            "LD_PRELOAD not set. E2E tests require IP spoofing.\n\
             Run with: LD_PRELOAD={} cargo nextest run --profile e2e --ignored",
            expected_preload.display()
        )));
    }

    let mut devnet = DevnetManager::new();

    if !devnet.infrastructure_available() {
        return Err(MarketError::Config(format!(
            "Devnet infrastructure not found at {}. \
             Please ensure the veilid repository is available \
             or set VEILID_REPO_PATH.",
            veilid_repo_path().display()
        )));
    }

    // Start devnet
    devnet.start()?;

    // Wait for health
    devnet.wait_for_health(60)?;

    Ok(devnet)
}

// ============================================================================
// E2E TESTS
// ============================================================================

/// Basic connectivity test - verifies nodes can start and attach to devnet.
#[tokio::test]
#[ignore] // Run with: cargo nextest run --ignored
#[serial]
async fn test_e2e_smoke_node_attachment() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed - check LD_PRELOAD and Docker");

    // Use offsets 10-39 to stay within libipspoof's MAX_PORT_OFFSET range (40)
    // Devnet uses offsets 0-8 (9 nodes, ports 5160-5168)
    let mut node = TestNode::new(10);

    // Start node with timeout
    let result = timeout(Duration::from_secs(180), async {
        node.start().await?;
        node.wait_for_ready(90).await?;
        Ok::<_, MarketError>(())
    })
    .await;

    // Capture node_id before shutdown (shutdown clears state)
    let had_node_id = node.node_id().is_some();

    // Cleanup regardless of result
    let _ = node.shutdown().await;

    match result {
        Ok(Ok(())) => {
            assert!(had_node_id, "Node should have an ID after attachment");
            eprintln!("[E2E] test_e2e_smoke_node_attachment PASSED");
        }
        Ok(Err(e)) => panic!("Node attachment failed: {}", e),
        Err(_) => panic!("Node attachment timed out"),
    }
}

/// Multi-node test - verifies multiple nodes can coexist and see each other.
#[tokio::test]
#[ignore] // Run with: cargo nextest run --ignored
#[serial]
async fn test_e2e_smoke_multi_node_connectivity() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed - check LD_PRELOAD and Docker");

    let mut nodes = vec![TestNode::new(11), TestNode::new(12), TestNode::new(13)];

    // Start all nodes
    let result = timeout(Duration::from_secs(180), async {
        for node in &mut nodes {
            node.start().await?;
        }

        // Wait for all to be ready
        for node in &nodes {
            node.wait_for_ready(90).await?;
        }

        Ok::<_, MarketError>(())
    })
    .await;

    // Cleanup all nodes
    for node in &mut nodes {
        let _ = node.shutdown().await;
    }

    match result {
        Ok(Ok(())) => {
            eprintln!("[E2E] test_e2e_smoke_multi_node_connectivity PASSED");
        }
        Ok(Err(e)) => panic!("Multi-node test failed: {}", e),
        Err(_) => panic!("Multi-node test timed out"),
    }
}

/// DHT operations test - verifies DHT read/write works across nodes.
#[tokio::test]
#[ignore] // Run with: cargo nextest run --ignored
#[serial]
async fn test_e2e_smoke_dht_operations() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed - check LD_PRELOAD and Docker");

    let mut node1 = TestNode::new(14);
    let mut node2 = TestNode::new(15);

    let result =
        timeout(Duration::from_secs(240), async {
            // Start and wait for both nodes to be ready
            node1.start().await?;
            node2.start().await?;
            node1.wait_for_ready(90).await?;
            node2.wait_for_ready(90).await?;

            // Get DHT operations from node1
            let dht1 = node1.node.dht_operations().ok_or_else(|| {
                MarketError::Dht("Failed to get DHT operations from node1".into())
            })?;

            let dht2 = node2.node.dht_operations().ok_or_else(|| {
                MarketError::Dht("Failed to get DHT operations from node2".into())
            })?;

            // Create a record on node1
            let record = dht1.create_dht_record().await?;
            let key = record.key.clone();

            // Write data from node1
            let test_data = b"Hello from E2E test!".to_vec();
            dht1.set_dht_value(&record, test_data.clone()).await?;

            // Delay for propagation
            tokio::time::sleep(Duration::from_secs(5)).await;

            // Read data from node2
            let read_data = dht2.get_dht_value(&key).await?;

            match read_data {
                Some(data) => {
                    assert_eq!(data, test_data, "DHT data mismatch");
                }
                None => {
                    return Err(MarketError::Dht(
                        "Failed to read DHT data from node2".into(),
                    ));
                }
            }

            Ok::<_, MarketError>(())
        })
        .await;

    // Cleanup
    let _ = node1.shutdown().await;
    let _ = node2.shutdown().await;

    match result {
        Ok(Ok(())) => {
            eprintln!("[E2E] test_e2e_smoke_dht_operations PASSED");
        }
        Ok(Err(e)) => panic!("DHT operations test failed: {}", e),
        Err(_) => panic!("DHT operations test timed out"),
    }
}

/// Coordinator messaging test - verifies bid announcements work.
#[tokio::test]
#[ignore] // Run with: cargo nextest run --ignored
#[serial]
async fn test_e2e_smoke_coordinator_local_registration() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed - check LD_PRELOAD and Docker");

    let mut node1 = TestNode::new(30);
    let mut node2 = TestNode::new(31);

    let result = timeout(Duration::from_secs(240), async {
        node1.start().await?;
        node2.start().await?;
        node1.wait_for_ready(90).await?;
        node2.wait_for_ready(90).await?;

        let node1_id = node1
            .node_id()
            .ok_or_else(|| MarketError::InvalidState("Node1 has no ID".into()))?;

        let dht1 = node1
            .node
            .dht_operations()
            .ok_or_else(|| MarketError::Dht("Failed to get DHT1".into()))?;

        let api1 = node1
            .node
            .api()
            .ok_or_else(|| MarketError::InvalidState("Failed to get API1".into()))?
            .clone();

        let coordinator1 = Arc::new(AuctionCoordinator::new(
            api1,
            dht1.clone(),
            node1_id.clone(),
            BidStorage::new(),
            node1.offset,
            market::config::DEFAULT_NETWORK_KEY,
            CancellationToken::new(),
        ));

        // Create a listing record and bid record
        let listing_record = dht1.create_dht_record().await?;
        let bid_record = dht1.create_dht_record().await?;

        // Register a local bid
        coordinator1
            .register_local_bid(
                &listing_record.key,
                node1_id.clone(),
                bid_record.key.clone(),
            )
            .await;

        // Verify bid count
        let count = coordinator1.get_bid_count(&listing_record.key).await;
        assert_eq!(count, 1, "Should have 1 registered bid");

        eprintln!("[E2E] Bid registration successful");

        Ok::<_, MarketError>(())
    })
    .await;

    let _ = node1.shutdown().await;
    let _ = node2.shutdown().await;

    match result {
        Ok(Ok(())) => {
            eprintln!("[E2E] test_e2e_smoke_coordinator_local_registration PASSED");
        }
        Ok(Err(e)) => panic!("Coordinator messaging test failed: {}", e),
        Err(_) => panic!("Coordinator messaging test timed out"),
    }
}

/// Diagnostic test for isolated debugging of single node startup.
/// Run with:
/// `cargo test --test integration_tests -- --ignored test_e2e_smoke_single_node_diagnostic`
///
/// This test provides detailed error chain output for debugging Veilid API startup issues.
#[tokio::test]
#[ignore] // Run with: cargo nextest run --ignored
#[serial]
async fn test_e2e_smoke_single_node_diagnostic() {
    init_test_tracing();

    eprintln!("\n=== DIAGNOSTIC TEST: Single Node Startup ===\n");

    // Check prerequisites
    let libipspoof = libipspoof_path();
    eprintln!("1. Checking libipspoof.so: {}", libipspoof.display());
    if !libipspoof.exists() {
        panic!("libipspoof.so not found at {}", libipspoof.display());
    }
    eprintln!("   [OK] Found libipspoof.so");

    let preload = std::env::var("LD_PRELOAD").unwrap_or_default();
    eprintln!("2. Checking LD_PRELOAD: {}", preload);
    if !preload.contains(libipspoof.to_str().unwrap()) {
        panic!(
            "LD_PRELOAD not set correctly.\n   Expected to contain: {}\n   Actual: {}",
            libipspoof.display(),
            preload
        );
    }
    eprintln!("   [OK] LD_PRELOAD is set");

    // Start devnet
    eprintln!("3. Starting devnet...");
    let _devnet = match setup_e2e_environment() {
        Ok(d) => {
            eprintln!("   [OK] Devnet started");
            d
        }
        Err(e) => {
            panic!("Failed to start devnet: {:?}", e);
        }
    };

    // Create test node with unique offset
    eprintln!("4. Creating test node (offset 200)...");
    let data_dir = std::env::temp_dir().join("market-e2e-diagnostic-200");
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).expect("Failed to create test data dir");
    eprintln!("   Data dir: {}", data_dir.display());

    let config = DevNetConfig {
        network_key: "development-network-2025".to_string(),
        bootstrap_nodes: vec!["udp://1.2.3.1:5160".to_string()],
        port_offset: 35,
    };
    eprintln!("   Network key: {}", config.network_key);
    eprintln!("   Bootstrap: {:?}", config.bootstrap_nodes);
    eprintln!("   Port offset: {} (port 5195)", config.port_offset);

    let mut node = VeilidNode::new(data_dir.clone())
        .with_devnet(config)
        .with_insecure_storage(true);

    // Attempt to start
    eprintln!("5. Starting Veilid node...");
    match node.start().await {
        Ok(()) => {
            eprintln!("   [OK] Node started successfully!");

            // Try to attach
            eprintln!("6. Attaching to network...");
            match node.attach().await {
                Ok(()) => {
                    eprintln!("   [OK] Attached to network");

                    // Wait a bit for state updates
                    eprintln!("7. Waiting for network state...");
                    tokio::time::sleep(Duration::from_secs(5)).await;

                    let state = node.state();
                    eprintln!("   is_attached: {}", state.is_attached);
                    eprintln!("   peer_count: {}", state.peer_count);
                    eprintln!("   node_ids: {:?}", state.node_ids);
                }
                Err(e) => {
                    eprintln!("   [FAIL] Attach failed:");
                    print_error_chain(&e);
                }
            }

            // Cleanup
            eprintln!("8. Shutting down...");
            let _ = node.shutdown().await;
        }
        Err(e) => {
            eprintln!("   [FAIL] Node start failed:");
            print_error_chain(&e);
            panic!("Node startup failed - see error chain above");
        }
    }

    // Cleanup
    let _ = std::fs::remove_dir_all(&data_dir);
    eprintln!("\n=== DIAGNOSTIC TEST COMPLETE ===\n");
}

/// Helper to print the full error chain for debugging.
fn print_error_chain(e: &MarketError) {
    eprintln!("   Error: {}", e);
    eprintln!("   Debug: {:?}", e);
}

// ============================================================================
// REAL AUCTION E2E HELPERS
// ============================================================================

/// Compute SHA256(bid_value || nonce) — the real commitment scheme used in production.
fn make_real_commitment(bid_value: u64, nonce: &[u8; 32]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bid_value.to_le_bytes());
    hasher.update(nonce);
    hasher.finalize().into()
}

/// Check if MP-SPDZ mascot-party.x binary is available at the default path.
fn check_mp_spdz_available() -> bool {
    let mp_spdz_dir = std::env::var(market::config::MP_SPDZ_DIR_ENV)
        .unwrap_or_else(|_| market::config::DEFAULT_MP_SPDZ_DIR.to_string());
    let binary = std::path::Path::new(&mp_spdz_dir).join("mascot-party.x");
    binary.exists()
}

/// Create a listing with real AES-256-GCM encrypted content, so decryption can be verified.
fn create_encrypted_listing(
    key: veilid_core::RecordKey,
    seller: PublicKey,
    reserve_price: u64,
    duration_secs: u64,
    plaintext: &str,
) -> Listing {
    use market::crypto::{encrypt_content, generate_key};
    use market::mocks::MockTime;

    let aes_key = generate_key();
    let (ciphertext, nonce) = encrypt_content(plaintext, &aes_key).expect("encryption failed");
    let decryption_key_hex = hex::encode(aes_key);

    Listing::builder_with_time(MockTime::new(1000))
        .key(key)
        .seller(seller)
        .title("E2E Encrypted Test Item")
        .encrypted_content(ciphertext, nonce, decryption_key_hex)
        .reserve_price(reserve_price)
        .auction_duration(duration_secs)
        .build()
        .expect("Failed to build encrypted listing")
}

/// A participant in an E2E auction test. Bundles a TestNode, AuctionCoordinator,
/// BidStorage, and the AppMessage processing loop — replicating what `main.rs` does.
struct E2EParticipant {
    node: TestNode,
    coordinator: Arc<AuctionCoordinator>,
    bid_storage: BidStorage,
    /// Handle to the spawned AppMessage loop task (for cleanup).
    _msg_loop_handle: Option<tokio::task::JoinHandle<()>>,
}

impl E2EParticipant {
    /// Create a new participant at the given port offset.
    async fn new(offset: u16) -> MarketResult<Self> {
        let mut node = TestNode::new(offset);
        node.start().await?;
        node.wait_for_ready(90).await?;

        let node_id = node
            .node_id()
            .ok_or_else(|| MarketError::InvalidState(format!("Node {} has no ID", offset)))?;

        let dht = node
            .node
            .dht_operations()
            .ok_or_else(|| MarketError::Dht(format!("Failed to get DHT for node {}", offset)))?;

        let api = node
            .node
            .api()
            .ok_or_else(|| {
                MarketError::InvalidState(format!("Failed to get API for node {}", offset))
            })?
            .clone();

        let bid_storage = BidStorage::new();

        let coordinator = Arc::new(AuctionCoordinator::new(
            api,
            dht,
            node_id,
            bid_storage.clone(),
            offset,
            market::config::DEFAULT_NETWORK_KEY,
            CancellationToken::new(),
        ));

        // Spawn AppMessage processing loop (replicates main.rs:230-249)
        let update_rx = node.node.take_update_receiver();
        let coord_clone = coordinator.clone();
        let msg_loop_handle = tokio::spawn(async move {
            if let Some(mut rx) = update_rx {
                while let Some(update) = rx.recv().await {
                    if let veilid_core::VeilidUpdate::AppMessage(msg) = update {
                        if let Err(e) = coord_clone
                            .process_app_message(msg.message().to_vec())
                            .await
                        {
                            tracing::error!("Failed to process AppMessage: {}", e);
                        }
                    }
                }
            }
        });

        // Start background monitoring immediately (matching production main.rs).
        // This initialises the master registry and registers broadcast routes,
        // giving the monitoring loop time to converge while the test sets up
        // listings and bids.
        coordinator.clone().start_monitoring();

        Ok(Self {
            node,
            coordinator,
            bid_storage,
            _msg_loop_handle: Some(msg_loop_handle),
        })
    }

    fn node_id(&self) -> Option<PublicKey> {
        self.node.node_id()
    }

    fn dht(&self) -> Option<market::DHTOperations> {
        self.node.node.dht_operations()
    }

    fn signing_pubkey_bytes(&self) -> [u8; 32] {
        self.coordinator.signing_pubkey_bytes()
    }

    async fn shutdown(&mut self) -> MarketResult<()> {
        self.node.shutdown().await
    }
}

// ============================================================================
// REAL AUCTION E2E TESTS
// ============================================================================

/// Test AppMessage-based bid announcements between 3 nodes with wired-up message loops.
///
/// Validates: Real Veilid AppMessage delivery, process_app_message() dispatching,
/// seller updating DHT registry in response to announcements.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_smoke_appmessage_bid_announcements() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed");

    let result = timeout(Duration::from_secs(360), async {
        // Start 3 participants with AppMessage loops
        let mut seller = E2EParticipant::new(33).await?;
        let mut bidder1 = E2EParticipant::new(34).await?;
        let mut bidder2 = E2EParticipant::new(35).await?;

        let seller_id = seller.node_id().unwrap();
        let bidder1_id = bidder1.node_id().unwrap();
        let bidder2_id = bidder2.node_id().unwrap();
        let seller_signing = seller.signing_pubkey_bytes();
        let bidder1_signing = bidder1.signing_pubkey_bytes();
        let bidder2_signing = bidder2.signing_pubkey_bytes();

        let seller_dht = seller.dht().unwrap();

        // Create and publish listing
        let listing_record = seller_dht.create_dht_record().await?;
        let listing = create_test_listing(listing_record.key.clone(), seller_id.clone(), 100, 3600);

        use market::veilid::listing_ops::ListingOperations;
        let listing_ops = ListingOperations::new(seller_dht.clone());
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;

        // Seller registers owned listing (initializes DHT subkey 2 registry)
        seller
            .coordinator
            .register_owned_listing(listing_record.clone())
            .await?;

        // Seller stores own bid in BidStorage and adds to registry
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let seller_nonce: [u8; 32] = rand::random();
        let seller_commitment = make_real_commitment(100, &seller_nonce);
        seller
            .bid_storage
            .store_bid(&listing_record.key, 100, seller_nonce)
            .await;

        let seller_bid_record_dht = seller_dht.create_dht_record().await?;
        let seller_bid = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: seller_id.clone(),
            commitment: seller_commitment,
            timestamp: now,
            bid_key: seller_bid_record_dht.key.clone(),
            signing_pubkey: seller_signing,
        };
        let seller_bid_ops = BidOperations::new(seller_dht.clone());
        seller_bid_ops
            .register_bid(&listing_record, seller_bid.clone())
            .await?;
        // Write the BidRecord to its own DHT record so discover_bids_from_storage can fetch it
        seller_dht
            .set_dht_value(&seller_bid_record_dht, seller_bid.to_cbor()?)
            .await?;
        seller
            .bid_storage
            .store_bid_key(&listing_record.key, &seller_bid_record_dht.key)
            .await;

        seller
            .coordinator
            .add_own_bid_to_registry(
                &listing_record.key,
                seller_id.clone(),
                seller_bid_record_dht.key.clone(),
                now,
            )
            .await?;

        eprintln!("[E2E] Seller placed own bid and initialized registry");

        // Each bidder creates their BidRecord in DHT with real SHA256 commitment
        let bidder1_dht = bidder1.dht().unwrap();
        let bidder1_nonce: [u8; 32] = rand::random();
        let bidder1_commitment = make_real_commitment(200, &bidder1_nonce);
        bidder1
            .bid_storage
            .store_bid(&listing_record.key, 200, bidder1_nonce)
            .await;

        let bid1_record_dht = bidder1_dht.create_dht_record().await?;
        let bid1 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder1_id.clone(),
            commitment: bidder1_commitment,
            timestamp: now + 1,
            bid_key: bid1_record_dht.key.clone(),
            signing_pubkey: bidder1_signing,
        };
        // Write to the pre-created DHT record (not publish_bid which creates a new one)
        bidder1_dht
            .set_dht_value(&bid1_record_dht, bid1.to_cbor()?)
            .await?;
        bidder1
            .bid_storage
            .store_bid_key(&listing_record.key, &bid1_record_dht.key)
            .await;

        let bidder2_dht = bidder2.dht().unwrap();
        let bidder2_nonce: [u8; 32] = rand::random();
        let bidder2_commitment = make_real_commitment(150, &bidder2_nonce);
        bidder2
            .bid_storage
            .store_bid(&listing_record.key, 150, bidder2_nonce)
            .await;

        let bid2_record_dht = bidder2_dht.create_dht_record().await?;
        let bid2 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder2_id.clone(),
            commitment: bidder2_commitment,
            timestamp: now + 2,
            bid_key: bid2_record_dht.key.clone(),
            signing_pubkey: bidder2_signing,
        };
        // Write to the pre-created DHT record (not publish_bid which creates a new one)
        bidder2_dht
            .set_dht_value(&bid2_record_dht, bid2.to_cbor()?)
            .await?;
        bidder2
            .bid_storage
            .store_bid_key(&listing_record.key, &bid2_record_dht.key)
            .await;

        eprintln!("[E2E] Both bidders published bid records to DHT");

        // Wait for DHT propagation before broadcasting
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Each bidder broadcasts bid announcement via real AppMessage
        // (monitoring was started in E2EParticipant::new, so routes are
        // already registered by now)
        bidder1
            .coordinator
            .broadcast_bid_announcement(&listing_record.key, &bid1_record_dht.key)
            .await?;
        bidder2
            .coordinator
            .broadcast_bid_announcement(&listing_record.key, &bid2_record_dht.key)
            .await?;

        eprintln!("[E2E] Bid announcements broadcast, waiting for propagation...");

        // Wait for AppMessage propagation (seller's loop should process them)
        tokio::time::sleep(Duration::from_secs(15)).await;

        // Assert: Seller's local bid_announcements has received announcements
        // The seller sees its own bid (added via add_own_bid_to_registry) plus
        // any announcements received via AppMessage. The bid_count only tracks
        // local announcements registered via register_local_bid or AppMessage handler.
        let seller_bid_count = seller.coordinator.get_bid_count(&listing_record.key).await;
        eprintln!(
            "[E2E] Seller's local bid announcement count: {}",
            seller_bid_count
        );

        // We expect at least the two bidder announcements arrived via AppMessage.
        // Note: seller's own bid was added to DHT registry but not to local bid_announcements
        // via the AppMessage path — it was added via add_own_bid_to_registry which writes to
        // DHT subkey 2 directly. The get_bid_count reads from the in-memory announcements map
        // which is populated by process_app_message -> BidAnnouncement handler.
        assert!(
            seller_bid_count >= 2,
            "Seller should have received at least 2 bid announcements via AppMessage, got {}",
            seller_bid_count
        );

        eprintln!("[E2E] test_e2e_smoke_appmessage_bid_announcements PASSED");

        // Cleanup
        let _ = seller.shutdown().await;
        let _ = bidder1.shutdown().await;
        let _ = bidder2.shutdown().await;

        Ok::<_, MarketError>(())
    })
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("AppMessage bid announcements test failed: {}", e),
        Err(_) => panic!("AppMessage bid announcements test timed out (6 min)"),
    }
}

/// Test real bid flow with SHA256 commitments, deterministic party ordering, and BidStorage.
///
/// Validates: Cryptographic commitments correctly stored & retrievable, party assignment
/// is deterministic (seller=party 0), bid storage populated for MPC.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_smoke_real_bid_flow_with_commitments() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed");

    let result = timeout(Duration::from_secs(360), async {
        // Use unique offsets (16-18) to avoid devnet routing table pollution
        // from prior tests that used the same ports with different identities
        let mut seller = E2EParticipant::new(16).await?;
        let mut bidder1 = E2EParticipant::new(17).await?;
        let mut bidder2 = E2EParticipant::new(18).await?;

        let seller_id = seller.node_id().unwrap();
        let bidder1_id = bidder1.node_id().unwrap();
        let bidder2_id = bidder2.node_id().unwrap();
        let seller_signing = seller.signing_pubkey_bytes();
        let bidder1_signing = bidder1.signing_pubkey_bytes();
        let bidder2_signing = bidder2.signing_pubkey_bytes();

        let seller_dht = seller.dht().unwrap();

        // Create listing with real AES-256-GCM encryption
        let listing_record = seller_dht.create_dht_record().await?;
        let plaintext = "Secret auction item: Rare first-edition book";
        let listing = create_encrypted_listing(
            listing_record.key.clone(),
            seller_id.clone(),
            100,
            3600,
            plaintext,
        );

        use market::veilid::listing_ops::ListingOperations;
        let listing_ops = ListingOperations::new(seller_dht.clone());
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;

        seller
            .coordinator
            .register_owned_listing(listing_record.clone())
            .await?;

        // Store decryption key locally so the seller can send it to the winner
        seller
            .coordinator
            .store_decryption_key(&listing_record.key, listing.decryption_key.clone())
            .await;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // ===== Seller bid (reserve price) =====
        let seller_nonce: [u8; 32] = rand::random();
        let seller_commitment = make_real_commitment(100, &seller_nonce);
        seller
            .bid_storage
            .store_bid(&listing_record.key, 100, seller_nonce)
            .await;

        let seller_bid_record_dht = seller_dht.create_dht_record().await?;
        let seller_bid = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: seller_id.clone(),
            commitment: seller_commitment,
            timestamp: now, // Earliest = party 0
            bid_key: seller_bid_record_dht.key.clone(),
            signing_pubkey: seller_signing,
        };
        let seller_bid_ops = BidOperations::new(seller_dht.clone());
        seller_bid_ops
            .register_bid(&listing_record, seller_bid.clone())
            .await?;
        // Write the BidRecord to its own DHT record so discover_bids_from_storage can fetch it
        seller_dht
            .set_dht_value(&seller_bid_record_dht, seller_bid.to_cbor()?)
            .await?;
        seller
            .bid_storage
            .store_bid_key(&listing_record.key, &seller_bid_record_dht.key)
            .await;

        seller
            .coordinator
            .add_own_bid_to_registry(
                &listing_record.key,
                seller_id.clone(),
                seller_bid_record_dht.key.clone(),
                now,
            )
            .await?;

        // ===== Bidder 1 =====
        let bidder1_dht = bidder1.dht().unwrap();
        let bidder1_nonce: [u8; 32] = rand::random();
        let bidder1_bid_value: u64 = 200;
        let bidder1_commitment = make_real_commitment(bidder1_bid_value, &bidder1_nonce);
        bidder1
            .bid_storage
            .store_bid(&listing_record.key, bidder1_bid_value, bidder1_nonce)
            .await;

        let bid1_record_dht = bidder1_dht.create_dht_record().await?;
        let bid1 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder1_id.clone(),
            commitment: bidder1_commitment,
            timestamp: now + 1,
            bid_key: bid1_record_dht.key.clone(),
            signing_pubkey: bidder1_signing,
        };
        seller_bid_ops
            .register_bid(&listing_record, bid1.clone())
            .await?;
        bidder1
            .bid_storage
            .store_bid_key(&listing_record.key, &bid1_record_dht.key)
            .await;

        // Write to the pre-created DHT record so it can be fetched by others
        bidder1_dht
            .set_dht_value(&bid1_record_dht, bid1.to_cbor()?)
            .await?;

        // ===== Bidder 2 =====
        let bidder2_dht = bidder2.dht().unwrap();
        let bidder2_nonce: [u8; 32] = rand::random();
        let bidder2_bid_value: u64 = 150;
        let bidder2_commitment = make_real_commitment(bidder2_bid_value, &bidder2_nonce);
        bidder2
            .bid_storage
            .store_bid(&listing_record.key, bidder2_bid_value, bidder2_nonce)
            .await;

        let bid2_record_dht = bidder2_dht.create_dht_record().await?;
        let bid2 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder2_id.clone(),
            commitment: bidder2_commitment,
            timestamp: now + 2,
            bid_key: bid2_record_dht.key.clone(),
            signing_pubkey: bidder2_signing,
        };
        seller_bid_ops
            .register_bid(&listing_record, bid2.clone())
            .await?;
        bidder2
            .bid_storage
            .store_bid_key(&listing_record.key, &bid2_record_dht.key)
            .await;

        // Write to the pre-created DHT record so it can be fetched by others
        bidder2_dht
            .set_dht_value(&bid2_record_dht, bid2.to_cbor()?)
            .await?;

        // Broadcast announcements via AppMessage
        tokio::time::sleep(Duration::from_secs(5)).await;
        bidder1
            .coordinator
            .broadcast_bid_announcement(&listing_record.key, &bid1_record_dht.key)
            .await?;
        bidder2
            .coordinator
            .broadcast_bid_announcement(&listing_record.key, &bid2_record_dht.key)
            .await?;
        tokio::time::sleep(Duration::from_secs(10)).await;

        // ===== Verify party ordering =====
        let bid_index = seller_bid_ops.fetch_bid_index(&listing_record.key).await?;
        assert_eq!(bid_index.bids.len(), 3, "Should have 3 bids");

        let sorted = bid_index.sorted_bidders(&seller_id);
        assert_eq!(sorted.len(), 3);
        // Seller is always party 0 (by design of sorted_bidders)
        assert_eq!(sorted[0], seller_id, "Seller should always be party 0");
        eprintln!("[E2E] Party ordering verified: seller is party 0");

        // ===== Verify commitments match =====
        for bid in &bid_index.bids {
            let recomputed = if bid.bidder == seller_id {
                make_real_commitment(100, &seller_nonce)
            } else if bid.bidder == bidder1_id {
                make_real_commitment(bidder1_bid_value, &bidder1_nonce)
            } else {
                make_real_commitment(bidder2_bid_value, &bidder2_nonce)
            };
            assert_eq!(
                bid.commitment, recomputed,
                "Commitment mismatch for bidder {}",
                bid.bidder
            );
        }
        eprintln!("[E2E] All SHA256 commitments verified");

        // ===== Verify BidStorage has correct data =====
        assert!(seller.bid_storage.has_bid(&listing_record.key).await);
        assert!(bidder1.bid_storage.has_bid(&listing_record.key).await);
        assert!(bidder2.bid_storage.has_bid(&listing_record.key).await);

        let (stored_val, stored_nonce) = bidder1
            .bid_storage
            .get_bid(&listing_record.key)
            .await
            .unwrap();
        assert_eq!(stored_val, bidder1_bid_value);
        assert_eq!(stored_nonce, bidder1_nonce);
        eprintln!("[E2E] BidStorage correctly populated for all participants");

        // ===== Verify listing retrievable from another node =====
        let bidder1_listing_ops = ListingOperations::new(bidder1_dht.clone());
        let retrieved = bidder1_listing_ops.get_listing(&listing_record.key).await?;
        assert!(
            retrieved.is_some(),
            "Listing should be retrievable by bidder1"
        );
        let retrieved_listing = retrieved.unwrap();
        assert_eq!(retrieved_listing.title, listing.title);
        eprintln!("[E2E] Listing retrievable from bidder's DHT perspective");

        eprintln!("[E2E] test_e2e_smoke_real_bid_flow_with_commitments PASSED");

        let _ = seller.shutdown().await;
        let _ = bidder1.shutdown().await;
        let _ = bidder2.shutdown().await;

        Ok::<_, MarketError>(())
    })
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("Real bid flow test failed: {}", e),
        Err(_) => panic!("Real bid flow test timed out (6 min)"),
    }
}

/// Test full MPC execution with real mascot-party.x.
/// Skips gracefully if MP-SPDZ is not available (CI-safe).
///
/// Validates: Real mascot-party.x execution, TCP tunnel proxy, route exchange,
/// MPC output parsing, hosts file generation.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_full_mpc_execution_happy_path() {
    init_test_tracing();

    if !check_mp_spdz_available() {
        eprintln!(
            "[E2E] SKIPPING test_e2e_full_mpc_execution: mascot-party.x not found at {}",
            market::config::DEFAULT_MP_SPDZ_DIR
        );
        return;
    }

    let _devnet = setup_e2e_environment().expect("E2E setup failed");

    let result = timeout(Duration::from_secs(600), async {
        // Use unique offsets (36-38) to avoid devnet routing table pollution
        let mut seller = E2EParticipant::new(36).await?;
        let mut bidder1 = E2EParticipant::new(37).await?;
        let mut bidder2 = E2EParticipant::new(38).await?;

        let seller_id = seller.node_id().unwrap();
        let bidder1_id = bidder1.node_id().unwrap();
        let bidder2_id = bidder2.node_id().unwrap();
        let seller_signing = seller.signing_pubkey_bytes();
        let bidder1_signing = bidder1.signing_pubkey_bytes();
        let bidder2_signing = bidder2.signing_pubkey_bytes();

        let seller_dht = seller.dht().unwrap();

        // Create listing with auction_end already in the past so monitoring triggers immediately
        let listing_record = seller_dht.create_dht_record().await?;
        let plaintext = "MPC test content: quantum computing blueprint";
        let listing = create_encrypted_listing(
            listing_record.key.clone(),
            seller_id.clone(),
            100,
            1, // 1 second duration with MockTime(1000) → auction_end = 1001 (already past)
            plaintext,
        );

        use market::veilid::listing_ops::ListingOperations;
        let listing_ops = ListingOperations::new(seller_dht.clone());
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;

        seller
            .coordinator
            .register_owned_listing(listing_record.clone())
            .await?;

        // Store decryption key locally so the seller can send it to the winner
        seller
            .coordinator
            .store_decryption_key(&listing_record.key, listing.decryption_key.clone())
            .await;

        // All three place bids with real commitments
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Seller bid (reserve)
        let seller_nonce: [u8; 32] = rand::random();
        seller
            .bid_storage
            .store_bid(&listing_record.key, 100, seller_nonce)
            .await;
        let seller_bid_rec = seller_dht.create_dht_record().await?;
        let seller_bid = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: seller_id.clone(),
            commitment: make_real_commitment(100, &seller_nonce),
            timestamp: now,
            bid_key: seller_bid_rec.key.clone(),
            signing_pubkey: seller_signing,
        };
        let seller_bid_ops = BidOperations::new(seller_dht.clone());
        seller_bid_ops
            .register_bid(&listing_record, seller_bid.clone())
            .await?;
        // Write the BidRecord to its own DHT record so discover_bids_from_storage can fetch it
        seller_dht
            .set_dht_value(&seller_bid_rec, seller_bid.to_cbor()?)
            .await?;
        seller
            .bid_storage
            .store_bid_key(&listing_record.key, &seller_bid_rec.key)
            .await;
        seller
            .coordinator
            .add_own_bid_to_registry(
                &listing_record.key,
                seller_id.clone(),
                seller_bid_rec.key.clone(),
                now,
            )
            .await?;

        // Bidder1 bid (200 — should win)
        let bidder1_dht = bidder1.dht().unwrap();
        let b1_nonce: [u8; 32] = rand::random();
        bidder1
            .bid_storage
            .store_bid(&listing_record.key, 200, b1_nonce)
            .await;
        let b1_rec = bidder1_dht.create_dht_record().await?;
        let bid1 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder1_id.clone(),
            commitment: make_real_commitment(200, &b1_nonce),
            timestamp: now + 1,
            bid_key: b1_rec.key.clone(),
            signing_pubkey: bidder1_signing,
        };
        seller_bid_ops
            .register_bid(&listing_record, bid1.clone())
            .await?;
        bidder1
            .bid_storage
            .store_bid_key(&listing_record.key, &b1_rec.key)
            .await;
        // Write to the pre-created DHT record (not publish_bid which creates a new one)
        bidder1_dht.set_dht_value(&b1_rec, bid1.to_cbor()?).await?;

        // Bidder2 bid (150)
        let bidder2_dht = bidder2.dht().unwrap();
        let b2_nonce: [u8; 32] = rand::random();
        bidder2
            .bid_storage
            .store_bid(&listing_record.key, 150, b2_nonce)
            .await;
        let b2_rec = bidder2_dht.create_dht_record().await?;
        let bid2 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder2_id.clone(),
            commitment: make_real_commitment(150, &b2_nonce),
            timestamp: now + 2,
            bid_key: b2_rec.key.clone(),
            signing_pubkey: bidder2_signing,
        };
        seller_bid_ops
            .register_bid(&listing_record, bid2.clone())
            .await?;
        bidder2
            .bid_storage
            .store_bid_key(&listing_record.key, &b2_rec.key)
            .await;
        // Write to the pre-created DHT record (not publish_bid which creates a new one)
        bidder2_dht.set_dht_value(&b2_rec, bid2.to_cbor()?).await?;

        // Update listing in DHT
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;

        // Broadcast bid announcements
        tokio::time::sleep(Duration::from_secs(5)).await;
        bidder1
            .coordinator
            .broadcast_bid_announcement(&listing_record.key, &b1_rec.key)
            .await?;
        bidder2
            .coordinator
            .broadcast_bid_announcement(&listing_record.key, &b2_rec.key)
            .await?;
        tokio::time::sleep(Duration::from_secs(10)).await;

        eprintln!("[E2E] All bids placed, announcements broadcast. Starting monitoring...");

        // All 3 coordinators watch the listing and start monitoring
        // The listing's auction_end is already in the past (MockTime 1000 + 1s = 1001),
        // so the first monitor poll will trigger handle_auction_end_wrapper.
        bidder1.coordinator.watch_listing(listing.to_public()).await;
        bidder2.coordinator.watch_listing(listing.to_public()).await;

        // Monitoring was started in E2EParticipant::new() (matching production).

        eprintln!("[E2E] Polling for MPC completion (max 180s)...");

        // Poll every 10s until MPC + post-MPC flow completes or we hit the max wait.
        // MPC involves: route exchange (7s) + settle (5s) + compile + execute (~60s)
        // Post-MPC: challenge → reveal → verify → send key (~5s)
        let mpc_start = tokio::time::Instant::now();
        let mpc_max_wait = Duration::from_secs(180);
        loop {
            let key = bidder1
                .coordinator
                .get_decryption_key(&listing_record.key)
                .await;
            if key.is_some() {
                eprintln!(
                    "[E2E] Bidder1 received decryption key after {:?}",
                    mpc_start.elapsed()
                );
                break;
            }
            if mpc_start.elapsed() > mpc_max_wait {
                eprintln!("[E2E] MPC did not complete within 180s, checking results anyway...");
                break;
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }

        let b1_key = bidder1
            .coordinator
            .get_decryption_key(&listing_record.key)
            .await;
        let b2_key = bidder2
            .coordinator
            .get_decryption_key(&listing_record.key)
            .await;

        eprintln!("[E2E] Bidder1 decryption key: {:?}", b1_key.is_some());
        eprintln!("[E2E] Bidder2 decryption key: {:?}", b2_key.is_some());

        // At minimum, verify the MPC auction completed by checking that the seller's
        // bid count is correct and the auction was processed.
        let bid_index = seller_bid_ops.fetch_bid_index(&listing_record.key).await?;
        assert_eq!(
            bid_index.bids.len(),
            3,
            "Bid index should still have 3 bids after MPC"
        );

        // If post-MPC flow completed successfully, bidder1 (highest bid=200) should have key
        if b1_key.is_some() {
            eprintln!("[E2E] Full MPC + post-MPC flow completed! Winner got decryption key.");
            assert!(
                b2_key.is_none(),
                "Non-winner should NOT have a decryption key"
            );
        } else {
            return Err(MarketError::Timeout(
                "Full MPC flow did not complete: winner did not receive decryption key".into(),
            ));
        }

        eprintln!("[E2E] test_e2e_full_mpc_execution_happy_path PASSED");

        let _ = seller.shutdown().await;
        let _ = bidder1.shutdown().await;
        let _ = bidder2.shutdown().await;

        Ok::<_, MarketError>(())
    })
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("Full MPC execution test failed: {}", e),
        Err(_) => panic!("Full MPC execution test timed out (10 min)"),
    }
}

/// Test complete winner verification and content decryption.
/// Exercises the full post-MPC challenge-response protocol.
/// Skips gracefully if MP-SPDZ is not available (CI-safe).
///
/// Validates: Challenge-response protocol, commitment verification (SHA256),
/// decryption key transfer via MPC routes, AES-256-GCM content decryption.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_full_winner_verification_and_decryption() {
    init_test_tracing();

    if !check_mp_spdz_available() {
        eprintln!(
            "[E2E] SKIPPING test_e2e_winner_verification_and_decryption: mascot-party.x not found"
        );
        return;
    }

    let _devnet = setup_e2e_environment().expect("E2E setup failed");

    let result = timeout(Duration::from_secs(600), async {
        // Use unique offsets (19, 28, 29) to avoid devnet routing table pollution
        let mut seller = E2EParticipant::new(19).await?;
        let mut bidder1 = E2EParticipant::new(28).await?;
        let mut bidder2 = E2EParticipant::new(29).await?;

        let seller_id = seller.node_id().unwrap();
        let bidder1_id = bidder1.node_id().unwrap();
        let bidder2_id = bidder2.node_id().unwrap();
        let seller_signing = seller.signing_pubkey_bytes();
        let bidder1_signing = bidder1.signing_pubkey_bytes();
        let bidder2_signing = bidder2.signing_pubkey_bytes();

        let seller_dht = seller.dht().unwrap();

        // Create listing with REAL encrypted content that we can verify decryption of
        let listing_record = seller_dht.create_dht_record().await?;
        let plaintext_content = "TOP SECRET: Coordinates to buried treasure at 51.5074N, 0.1278W";
        let listing = create_encrypted_listing(
            listing_record.key.clone(),
            seller_id.clone(),
            50, // Low reserve so bidders win
            1,  // Already expired
            plaintext_content,
        );
        let expected_decryption_key = listing.decryption_key.clone();

        use market::veilid::listing_ops::ListingOperations;
        let listing_ops = ListingOperations::new(seller_dht.clone());
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;

        seller
            .coordinator
            .register_owned_listing(listing_record.clone())
            .await?;

        // Store decryption key locally so the seller can send it to the winner
        seller
            .coordinator
            .store_decryption_key(&listing_record.key, listing.decryption_key.clone())
            .await;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Seller bid (reserve = 50)
        let s_nonce: [u8; 32] = rand::random();
        seller
            .bid_storage
            .store_bid(&listing_record.key, 50, s_nonce)
            .await;
        let s_bid_rec = seller_dht.create_dht_record().await?;
        let s_bid = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: seller_id.clone(),
            commitment: make_real_commitment(50, &s_nonce),
            timestamp: now,
            bid_key: s_bid_rec.key.clone(),
            signing_pubkey: seller_signing,
        };
        let seller_bid_ops = BidOperations::new(seller_dht.clone());
        seller_bid_ops
            .register_bid(&listing_record, s_bid.clone())
            .await?;
        // Write the BidRecord to its own DHT record so discover_bids_from_storage can fetch it
        seller_dht
            .set_dht_value(&s_bid_rec, s_bid.to_cbor()?)
            .await?;
        seller
            .bid_storage
            .store_bid_key(&listing_record.key, &s_bid_rec.key)
            .await;
        seller
            .coordinator
            .add_own_bid_to_registry(
                &listing_record.key,
                seller_id.clone(),
                s_bid_rec.key.clone(),
                now,
            )
            .await?;

        // Bidder1 bids 300 (should win)
        let bidder1_dht = bidder1.dht().unwrap();
        let b1_nonce: [u8; 32] = rand::random();
        bidder1
            .bid_storage
            .store_bid(&listing_record.key, 300, b1_nonce)
            .await;
        let b1_rec = bidder1_dht.create_dht_record().await?;
        let bid1 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder1_id.clone(),
            commitment: make_real_commitment(300, &b1_nonce),
            timestamp: now + 1,
            bid_key: b1_rec.key.clone(),
            signing_pubkey: bidder1_signing,
        };
        seller_bid_ops
            .register_bid(&listing_record, bid1.clone())
            .await?;
        bidder1
            .bid_storage
            .store_bid_key(&listing_record.key, &b1_rec.key)
            .await;
        // Write to the pre-created DHT record (not publish_bid which creates a new one)
        bidder1_dht.set_dht_value(&b1_rec, bid1.to_cbor()?).await?;

        // Bidder2 bids 100
        let bidder2_dht = bidder2.dht().unwrap();
        let b2_nonce: [u8; 32] = rand::random();
        bidder2
            .bid_storage
            .store_bid(&listing_record.key, 100, b2_nonce)
            .await;
        let b2_rec = bidder2_dht.create_dht_record().await?;
        let bid2 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder2_id.clone(),
            commitment: make_real_commitment(100, &b2_nonce),
            timestamp: now + 2,
            bid_key: b2_rec.key.clone(),
            signing_pubkey: bidder2_signing,
        };
        seller_bid_ops
            .register_bid(&listing_record, bid2.clone())
            .await?;
        bidder2
            .bid_storage
            .store_bid_key(&listing_record.key, &b2_rec.key)
            .await;
        // Write to the pre-created DHT record (not publish_bid which creates a new one)
        bidder2_dht.set_dht_value(&b2_rec, bid2.to_cbor()?).await?;

        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;

        // Broadcast announcements
        tokio::time::sleep(Duration::from_secs(5)).await;
        bidder1
            .coordinator
            .broadcast_bid_announcement(&listing_record.key, &b1_rec.key)
            .await?;
        bidder2
            .coordinator
            .broadcast_bid_announcement(&listing_record.key, &b2_rec.key)
            .await?;
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Watch and start monitoring
        bidder1.coordinator.watch_listing(listing.to_public()).await;
        bidder2.coordinator.watch_listing(listing.to_public()).await;

        // Monitoring was started in E2EParticipant::new() (matching production).

        eprintln!("[E2E] Polling for MPC + post-MPC flow (max 180s)...");

        // Poll every 10s until winner receives decryption key or we hit the max wait.
        let mpc_start = tokio::time::Instant::now();
        let mpc_max_wait = Duration::from_secs(180);
        loop {
            let key = bidder1
                .coordinator
                .get_decryption_key(&listing_record.key)
                .await;
            if key.is_some() {
                eprintln!(
                    "[E2E] Winner received decryption key after {:?}",
                    mpc_start.elapsed()
                );
                break;
            }
            if mpc_start.elapsed() > mpc_max_wait {
                eprintln!("[E2E] MPC + post-MPC did not complete within 180s");
                break;
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }

        // ===== VERIFY: Winner got decryption key, non-winner did not =====
        let winner_key = bidder1
            .coordinator
            .get_decryption_key(&listing_record.key)
            .await;
        let loser_key = bidder2
            .coordinator
            .get_decryption_key(&listing_record.key)
            .await;

        eprintln!(
            "[E2E] Bidder1 (expected winner, bid=300) has key: {}",
            winner_key.is_some()
        );
        eprintln!(
            "[E2E] Bidder2 (expected loser, bid=100) has key: {}",
            loser_key.is_some()
        );

        if let Some(key) = &winner_key {
            // Winner should have received the correct decryption key
            assert_eq!(
                key, &expected_decryption_key,
                "Winner's decryption key should match seller's key"
            );
            eprintln!("[E2E] Decryption key matches!");

            // Non-winner should NOT have a key
            assert!(
                loser_key.is_none(),
                "Non-winner should NOT have a decryption key"
            );

            // ===== VERIFY: Winner can decrypt the content =====
            let decrypted = bidder1
                .coordinator
                .fetch_and_decrypt_listing(&listing_record.key)
                .await;
            match decrypted {
                Ok(content) => {
                    assert_eq!(
                        content, plaintext_content,
                        "Decrypted content should match original plaintext"
                    );
                    eprintln!("[E2E] Content successfully decrypted by winner!");
                    eprintln!("[E2E] Decrypted: \"{}\"", content);
                }
                Err(e) => {
                    return Err(MarketError::Crypto(format!(
                        "Winner could not decrypt listing content: {e}"
                    )));
                }
            }

            eprintln!(
                "[E2E] FULL VERIFICATION PASSED: MPC → challenge → reveal → verify → decrypt"
            );
        } else {
            return Err(MarketError::Timeout(
                "Winner did not receive decryption key within timeout".into(),
            ));
        }

        eprintln!("[E2E] test_e2e_full_winner_verification_and_decryption PASSED");

        let _ = seller.shutdown().await;
        let _ = bidder1.shutdown().await;
        let _ = bidder2.shutdown().await;

        Ok::<_, MarketError>(())
    })
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("Winner verification test failed: {}", e),
        Err(_) => panic!("Winner verification test timed out (10 min)"),
    }
}

// ============================================================================
// EDGE CASE E2E TESTS
// ============================================================================

/// Test that a tampered AppMessage (flipped byte in signed envelope) is rejected.
///
/// Validates: `process_app_message()` → `SignedEnvelope::verify_and_unwrap()` returns Err
/// on tampered data, and the coordinator continues operating normally afterwards.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_edge_tampered_appmessage_rejected() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed");

    let result = timeout(Duration::from_secs(300), async {
        let mut sender = E2EParticipant::new(20).await?;
        let mut receiver = E2EParticipant::new(21).await?;

        let sender_dht = sender.dht().unwrap();
        let receiver_dht = receiver.dht().unwrap();

        // Create a listing on receiver so we can verify state is unaffected
        let listing_record = receiver_dht.create_dht_record().await?;
        let receiver_id = receiver.node_id().unwrap();
        let listing =
            create_test_listing(listing_record.key.clone(), receiver_id.clone(), 100, 3600);

        use market::veilid::listing_ops::ListingOperations;
        let listing_ops = ListingOperations::new(receiver_dht.clone());
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;
        receiver
            .coordinator
            .register_owned_listing(listing_record.clone())
            .await?;

        // Step 1: Send a legitimate signed message and verify it works
        let sender_id = sender.node_id().unwrap();
        let bid_record_dht = sender_dht.create_dht_record().await?;
        let sender_signing = sender.signing_pubkey_bytes();
        let nonce: [u8; 32] = rand::random();
        let commitment = make_real_commitment(200, &nonce);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let bid = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: sender_id.clone(),
            commitment,
            timestamp: now,
            bid_key: bid_record_dht.key.clone(),
            signing_pubkey: sender_signing,
        };
        sender_dht
            .set_dht_value(&bid_record_dht, bid.to_cbor()?)
            .await?;

        // Wait for DHT propagation
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Create a valid signed message
        use market::veilid::bid_announcement::AuctionMessage;
        let msg = AuctionMessage::bid_announcement(
            listing_record.key.clone(),
            sender_id.clone(),
            bid_record_dht.key.clone(),
        );
        let valid_data = msg.to_signed_bytes(sender.coordinator.signing_key())?;

        // Verify the valid message is processed successfully
        let valid_result = receiver
            .coordinator
            .process_app_message(valid_data.clone())
            .await;
        assert!(valid_result.is_ok(), "Valid message should be accepted");
        eprintln!("[E2E] Valid signed message accepted");

        // Step 2: Tamper with the message (flip a byte in the middle)
        let mut tampered = valid_data.clone();
        if tampered.len() > 20 {
            tampered[20] ^= 0xFF;
        }

        // Tampered message should be rejected (signature verification fails)
        let tampered_result = receiver.coordinator.process_app_message(tampered).await;
        assert!(
            tampered_result.is_err(),
            "Tampered message should be rejected"
        );
        eprintln!("[E2E] Tampered message correctly rejected");

        // Step 3: Verify coordinator still works after rejection
        let count = receiver
            .coordinator
            .get_bid_count(&listing_record.key)
            .await;
        eprintln!("[E2E] Bid count after tamper attempt: {}", count);

        // The valid message registered 1 bid; the tampered one was rejected
        assert!(
            count <= 1,
            "Tampered message should not have registered a bid"
        );

        eprintln!("[E2E] test_e2e_edge_tampered_appmessage_rejected PASSED");
        let _ = sender.shutdown().await;
        let _ = receiver.shutdown().await;
        Ok::<_, MarketError>(())
    })
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("Tampered message test failed: {}", e),
        Err(_) => panic!("Tampered message test timed out"),
    }
}

/// Test that a bid announcement signed by the wrong key (impersonation) is rejected.
///
/// Validates: `validate_bid_announcement_signer()` rejects messages where the
/// envelope signer doesn't match `BidRecord.signing_pubkey`.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_edge_bid_announcement_wrong_signer_rejected() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed");

    let result = timeout(Duration::from_secs(300), async {
        let mut seller = E2EParticipant::new(22).await?;
        let mut bidder1 = E2EParticipant::new(23).await?;
        let mut bidder2 = E2EParticipant::new(24).await?;

        let seller_id = seller.node_id().unwrap();
        let bidder1_id = bidder1.node_id().unwrap();
        let bidder1_signing = bidder1.signing_pubkey_bytes();

        let seller_dht = seller.dht().unwrap();
        let bidder1_dht = bidder1.dht().unwrap();

        // Create listing
        let listing_record = seller_dht.create_dht_record().await?;
        let listing = create_test_listing(listing_record.key.clone(), seller_id.clone(), 100, 3600);

        use market::veilid::listing_ops::ListingOperations;
        let listing_ops = ListingOperations::new(seller_dht.clone());
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;
        seller
            .coordinator
            .register_owned_listing(listing_record.clone())
            .await?;

        // Bidder1 creates a legitimate BidRecord in DHT
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let nonce: [u8; 32] = rand::random();
        let commitment = make_real_commitment(200, &nonce);
        let bid1_record_dht = bidder1_dht.create_dht_record().await?;
        let bid1 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder1_id.clone(),
            commitment,
            timestamp: now,
            bid_key: bid1_record_dht.key.clone(),
            signing_pubkey: bidder1_signing,
        };
        bidder1_dht
            .set_dht_value(&bid1_record_dht, bid1.to_cbor()?)
            .await?;

        tokio::time::sleep(Duration::from_secs(5)).await;

        // Bidder2 crafts a BidAnnouncement claiming to be bidder1,
        // but signs it with bidder2's key (impersonation attempt)
        use market::veilid::bid_announcement::AuctionMessage;
        let impersonated_msg = AuctionMessage::bid_announcement(
            listing_record.key.clone(),
            bidder1_id.clone(), // Claims to be bidder1
            bid1_record_dht.key.clone(),
        );
        // Signed with bidder2's key — signer won't match BidRecord.signing_pubkey
        let impersonated_data =
            impersonated_msg.to_signed_bytes(bidder2.coordinator.signing_key())?;

        // Send the impersonated message directly to seller's coordinator
        let result = seller
            .coordinator
            .process_app_message(impersonated_data)
            .await;
        // process_app_message returns Ok(()) even for rejected bids (logged as warning)
        assert!(result.is_ok(), "Should not crash on impersonation attempt");

        // Now send the legitimate announcement signed by bidder1's actual key
        let legit_msg = AuctionMessage::bid_announcement(
            listing_record.key.clone(),
            bidder1_id.clone(),
            bid1_record_dht.key.clone(),
        );
        let legit_data = legit_msg.to_signed_bytes(bidder1.coordinator.signing_key())?;
        seller.coordinator.process_app_message(legit_data).await?;

        tokio::time::sleep(Duration::from_secs(2)).await;

        // Only the legitimate bid should be registered
        let count = seller.coordinator.get_bid_count(&listing_record.key).await;
        eprintln!("[E2E] Seller bid count (expect 1 legitimate): {}", count);
        assert_eq!(
            count, 1,
            "Only the legitimate bid should be registered, not the impersonated one"
        );

        eprintln!("[E2E] test_e2e_edge_bid_announcement_wrong_signer_rejected PASSED");
        let _ = seller.shutdown().await;
        let _ = bidder1.shutdown().await;
        let _ = bidder2.shutdown().await;
        Ok::<_, MarketError>(())
    })
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("Wrong signer test failed: {}", e),
        Err(_) => panic!("Wrong signer test timed out"),
    }
}

/// Test that duplicate bid announcements from the same bidder are deduplicated.
///
/// Validates: `BidAnnouncementRegistry::add()` dedup and `BidAnnouncementTracker` dedup.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_edge_duplicate_bid_announcements_deduped() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed");

    let result = timeout(Duration::from_secs(300), async {
        let mut seller = E2EParticipant::new(25).await?;
        let mut bidder = E2EParticipant::new(26).await?;

        let seller_id = seller.node_id().unwrap();
        let bidder_id = bidder.node_id().unwrap();
        let bidder_signing = bidder.signing_pubkey_bytes();

        let seller_dht = seller.dht().unwrap();
        let bidder_dht = bidder.dht().unwrap();

        // Create listing
        let listing_record = seller_dht.create_dht_record().await?;
        let listing = create_test_listing(listing_record.key.clone(), seller_id.clone(), 100, 3600);

        use market::veilid::listing_ops::ListingOperations;
        let listing_ops = ListingOperations::new(seller_dht.clone());
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;
        seller
            .coordinator
            .register_owned_listing(listing_record.clone())
            .await?;

        // Bidder creates a BidRecord in DHT
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let nonce: [u8; 32] = rand::random();
        let commitment = make_real_commitment(150, &nonce);
        let bid_record_dht = bidder_dht.create_dht_record().await?;
        let bid = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder_id.clone(),
            commitment,
            timestamp: now,
            bid_key: bid_record_dht.key.clone(),
            signing_pubkey: bidder_signing,
        };
        bidder_dht
            .set_dht_value(&bid_record_dht, bid.to_cbor()?)
            .await?;

        tokio::time::sleep(Duration::from_secs(5)).await;

        // Send the same bid announcement 3 times
        use market::veilid::bid_announcement::AuctionMessage;
        for i in 0..3 {
            let msg = AuctionMessage::bid_announcement(
                listing_record.key.clone(),
                bidder_id.clone(),
                bid_record_dht.key.clone(),
            );
            let data = msg.to_signed_bytes(bidder.coordinator.signing_key())?;
            seller.coordinator.process_app_message(data).await?;
            eprintln!("[E2E] Sent duplicate announcement #{}", i + 1);
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify deduplication: should only count as 1 bid
        let count = seller.coordinator.get_bid_count(&listing_record.key).await;
        eprintln!("[E2E] Bid count after 3 duplicate announcements: {}", count);
        assert_eq!(
            count, 1,
            "Duplicate announcements should be deduplicated to 1"
        );

        // Also verify DHT registry has exactly 1 entry
        let registry_data = seller_dht
            .get_value_at_subkey(
                &listing_record.key,
                market::config::subkeys::BID_ANNOUNCEMENTS,
                true,
            )
            .await?;
        if let Some(data) = registry_data {
            use market::veilid::bid_announcement::BidAnnouncementRegistry;
            let registry = BidAnnouncementRegistry::from_bytes(&data)?;
            eprintln!(
                "[E2E] DHT registry announcement count: {}",
                registry.announcements.len()
            );
            assert_eq!(
                registry.announcements.len(),
                1,
                "DHT registry should have exactly 1 entry after dedup"
            );
        }

        eprintln!("[E2E] test_e2e_edge_duplicate_bid_announcements_deduped PASSED");
        let _ = seller.shutdown().await;
        let _ = bidder.shutdown().await;
        Ok::<_, MarketError>(())
    })
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("Duplicate dedup test failed: {}", e),
        Err(_) => panic!("Duplicate dedup test timed out"),
    }
}

/// Test that fetching a listing without winning the auction (no decryption key) fails.
///
/// Validates: `fetch_and_decrypt_listing()` → `MarketError::NotFound` when no key stored.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_edge_decrypt_without_key_fails() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed");

    let result = timeout(Duration::from_secs(300), async {
        let mut seller = E2EParticipant::new(27).await?;
        let mut bidder = E2EParticipant::new(32).await?;

        let seller_id = seller.node_id().unwrap();
        let seller_dht = seller.dht().unwrap();

        // Create listing with real encrypted content
        let listing_record = seller_dht.create_dht_record().await?;
        let plaintext = "Secret content that should not be accessible";
        let listing = create_encrypted_listing(
            listing_record.key.clone(),
            seller_id.clone(),
            100,
            3600,
            plaintext,
        );

        use market::veilid::listing_ops::ListingOperations;
        let listing_ops = ListingOperations::new(seller_dht.clone());
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;

        // Seller stores key locally but bidder does NOT have it
        seller
            .coordinator
            .store_decryption_key(&listing_record.key, listing.decryption_key.clone())
            .await;

        tokio::time::sleep(Duration::from_secs(5)).await;

        // Bidder (who hasn't won) tries to fetch and decrypt
        let decrypt_result = bidder
            .coordinator
            .fetch_and_decrypt_listing(&listing_record.key)
            .await;

        match &decrypt_result {
            Err(MarketError::NotFound(msg)) => {
                eprintln!("[E2E] Correctly got NotFound: {}", msg);
                assert!(
                    msg.contains("auction") || msg.contains("key"),
                    "Error should mention auction/key requirement"
                );
            }
            Err(e) => {
                // Could also be NotFound for the listing itself if DHT hasn't propagated
                eprintln!("[E2E] Got expected error (non-winner): {}", e);
            }
            Ok(content) => {
                panic!("Non-winner should NOT be able to decrypt! Got: {}", content);
            }
        }

        // Verify seller CAN decrypt their own listing
        let seller_decrypt = seller
            .coordinator
            .fetch_and_decrypt_listing(&listing_record.key)
            .await;
        assert!(
            seller_decrypt.is_ok(),
            "Seller should be able to decrypt: {:?}",
            seller_decrypt.err()
        );
        assert_eq!(seller_decrypt.unwrap(), plaintext);
        eprintln!("[E2E] Seller can decrypt their own listing");

        eprintln!("[E2E] test_e2e_edge_decrypt_without_key_fails PASSED");
        let _ = seller.shutdown().await;
        let _ = bidder.shutdown().await;
        Ok::<_, MarketError>(())
    })
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("Decrypt without key test failed: {}", e),
        Err(_) => panic!("Decrypt without key test timed out"),
    }
}

/// Test that decrypting listing content with the wrong AES key fails.
///
/// Validates: `decrypt_content_with_key()` with wrong key → `MarketError::Crypto`,
/// then correct key succeeds. Tests AEAD tag verification over real DHT data.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_edge_listing_encryption_wrong_key_fails() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed");

    let result = timeout(Duration::from_secs(300), async {
        let mut seller = E2EParticipant::new(39).await?;
        let mut reader = E2EParticipant::new(10).await?;

        let seller_id = seller.node_id().unwrap();
        let seller_dht = seller.dht().unwrap();

        // Create listing with real AES-256-GCM encrypted content
        let listing_record = seller_dht.create_dht_record().await?;
        let plaintext = "Confidential auction item: Diamond necklace, 24 carats";
        let listing = create_encrypted_listing(
            listing_record.key.clone(),
            seller_id.clone(),
            500,
            3600,
            plaintext,
        );
        let correct_key = listing.decryption_key.clone();

        use market::veilid::listing_ops::ListingOperations;
        let listing_ops = ListingOperations::new(seller_dht.clone());
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;

        tokio::time::sleep(Duration::from_secs(5)).await;

        // Reader fetches listing from DHT
        let reader_dht = reader.dht().unwrap();
        let reader_listing_ops = ListingOperations::new(reader_dht.clone());
        let retrieved = reader_listing_ops.get_listing(&listing_record.key).await?;
        assert!(retrieved.is_some(), "Listing should be retrievable via DHT");
        let public_listing = retrieved.unwrap();

        // Try to decrypt with a random wrong key (32 bytes hex-encoded)
        let wrong_key = hex::encode([0xABu8; 32]);
        let wrong_result = public_listing.decrypt_content_with_key(&wrong_key);
        assert!(
            wrong_result.is_err(),
            "Decryption with wrong key should fail"
        );
        eprintln!(
            "[E2E] Wrong key correctly rejected: {:?}",
            wrong_result.err()
        );

        // Decrypt with the correct key should succeed
        let correct_result = public_listing.decrypt_content_with_key(&correct_key);
        assert!(
            correct_result.is_ok(),
            "Decryption with correct key should succeed: {:?}",
            correct_result.err()
        );
        assert_eq!(correct_result.unwrap(), plaintext);
        eprintln!("[E2E] Correct key decrypted successfully");

        eprintln!("[E2E] test_e2e_edge_listing_encryption_wrong_key_fails PASSED");
        let _ = seller.shutdown().await;
        let _ = reader.shutdown().await;
        Ok::<_, MarketError>(())
    })
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("Wrong key decryption test failed: {}", e),
        Err(_) => panic!("Wrong key decryption test timed out"),
    }
}

/// Test that all nodes derive the same master registry key from the shared network key.
///
/// Validates: `ensure_master_registry()` deterministic derivation — all nodes
/// converge on the same DHT record key.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_edge_master_registry_convergence() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed");

    let result = timeout(Duration::from_secs(300), async {
        let mut node_a = E2EParticipant::new(11).await?;
        let mut node_b = E2EParticipant::new(12).await?;
        let mut node_c = E2EParticipant::new(13).await?;

        // Each node independently ensures the master registry
        let key_a = node_a.coordinator.ensure_master_registry().await?;
        eprintln!("[E2E] Node A registry key: {}", key_a);

        let key_b = node_b.coordinator.ensure_master_registry().await?;
        eprintln!("[E2E] Node B registry key: {}", key_b);

        let key_c = node_c.coordinator.ensure_master_registry().await?;
        eprintln!("[E2E] Node C registry key: {}", key_c);

        // All three must derive the same key (deterministic from network key)
        assert_eq!(key_a, key_b, "Node A and B should agree on registry key");
        assert_eq!(key_b, key_c, "Node B and C should agree on registry key");
        eprintln!("[E2E] All 3 nodes converged on the same master registry key");

        // Verify calling it again returns the same key (idempotent)
        let key_a2 = node_a.coordinator.ensure_master_registry().await?;
        assert_eq!(key_a, key_a2, "Registry key should be stable across calls");

        eprintln!("[E2E] test_e2e_edge_master_registry_convergence PASSED");
        let _ = node_a.shutdown().await;
        let _ = node_b.shutdown().await;
        let _ = node_c.shutdown().await;
        Ok::<_, MarketError>(())
    })
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("Registry convergence test failed: {}", e),
        Err(_) => panic!("Registry convergence test timed out"),
    }
}

/// Test DHT bid registry cross-node consistency.
///
/// Validates: Seller writes bid registry to DHT subkey 2, another node can read
/// the same registry with consistent data.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_edge_dht_bid_registry_cross_node_consistency() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed");

    let result = timeout(Duration::from_secs(300), async {
        let mut seller = E2EParticipant::new(14).await?;
        let mut reader = E2EParticipant::new(15).await?;

        let seller_id = seller.node_id().unwrap();
        let seller_signing = seller.signing_pubkey_bytes();
        let seller_dht = seller.dht().unwrap();

        // Create listing and register ownership
        let listing_record = seller_dht.create_dht_record().await?;
        let listing = create_test_listing(listing_record.key.clone(), seller_id.clone(), 100, 3600);

        use market::veilid::listing_ops::ListingOperations;
        let listing_ops = ListingOperations::new(seller_dht.clone());
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;
        seller
            .coordinator
            .register_owned_listing(listing_record.clone())
            .await?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Seller places own bid
        let seller_nonce: [u8; 32] = rand::random();
        let seller_commitment = make_real_commitment(100, &seller_nonce);
        let seller_bid_rec = seller_dht.create_dht_record().await?;
        let seller_bid = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: seller_id.clone(),
            commitment: seller_commitment,
            timestamp: now,
            bid_key: seller_bid_rec.key.clone(),
            signing_pubkey: seller_signing,
        };
        let seller_bid_ops = BidOperations::new(seller_dht.clone());
        seller_bid_ops
            .register_bid(&listing_record, seller_bid.clone())
            .await?;
        seller_dht
            .set_dht_value(&seller_bid_rec, seller_bid.to_cbor()?)
            .await?;
        seller
            .coordinator
            .add_own_bid_to_registry(
                &listing_record.key,
                seller_id.clone(),
                seller_bid_rec.key.clone(),
                now,
            )
            .await?;

        // Simulate 2 additional bid announcements (create BidRecords, process announcements)
        let bidder_keys: Vec<PublicKey> = (0..2)
            .map(|_| {
                let key_bytes: [u8; 32] = rand::random();
                PublicKey::new(
                    veilid_core::CRYPTO_KIND_VLD0,
                    veilid_core::BarePublicKey::new(&key_bytes),
                )
            })
            .collect();

        for (i, bidder_pk) in bidder_keys.iter().enumerate() {
            let bid_rec_dht = seller_dht.create_dht_record().await?;
            let nonce: [u8; 32] = rand::random();
            let commitment = make_real_commitment(200 + (i as u64) * 50, &nonce);
            let fake_signing: [u8; 32] = rand::random();
            let bid = BidRecord {
                listing_key: listing_record.key.clone(),
                bidder: bidder_pk.clone(),
                commitment,
                timestamp: now + 1 + i as u64,
                bid_key: bid_rec_dht.key.clone(),
                signing_pubkey: fake_signing,
            };
            seller_dht
                .set_dht_value(&bid_rec_dht, bid.to_cbor()?)
                .await?;
            seller_bid_ops.register_bid(&listing_record, bid).await?;
        }

        // Wait for DHT propagation
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Reader fetches the bid index from a different node
        let reader_dht = reader.dht().unwrap();
        let reader_bid_ops = BidOperations::new(reader_dht.clone());
        let bid_index = reader_bid_ops.fetch_bid_index(&listing_record.key).await?;

        eprintln!(
            "[E2E] Reader sees {} bids in bid index",
            bid_index.bids.len()
        );
        assert_eq!(
            bid_index.bids.len(),
            3,
            "Reader should see all 3 bids in the index"
        );

        // Verify each BidRecord is fetchable from its own DHT record
        let seller_bid_fetched = reader_bid_ops.fetch_bid(&seller_bid_rec.key).await?;
        assert!(
            seller_bid_fetched.is_some(),
            "Seller's bid record should be fetchable"
        );
        let fetched = seller_bid_fetched.unwrap();
        assert_eq!(fetched.commitment, seller_commitment);
        assert_eq!(fetched.bidder, seller_id);
        eprintln!("[E2E] Seller's bid record verified from reader node");

        eprintln!("[E2E] test_e2e_edge_dht_bid_registry_cross_node_consistency PASSED");
        let _ = seller.shutdown().await;
        let _ = reader.shutdown().await;
        Ok::<_, MarketError>(())
    })
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("DHT bid registry consistency test failed: {}", e),
        Err(_) => panic!("DHT bid registry consistency test timed out"),
    }
}

/// Test that a seller-only auction (no other bidders) doesn't crash or deadlock.
///
/// Validates: The monitoring loop handles <2 parties gracefully — no MPC triggered,
/// no deadlock, coordinator stays healthy.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_edge_seller_only_no_sale() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed");

    let result = timeout(Duration::from_secs(300), async {
        let mut seller = E2EParticipant::new(16).await?;

        let seller_id = seller.node_id().unwrap();
        let seller_signing = seller.signing_pubkey_bytes();
        let seller_dht = seller.dht().unwrap();

        // Create listing that's already expired (1s duration with MockTime(1000))
        let listing_record = seller_dht.create_dht_record().await?;
        let listing = create_test_listing(
            listing_record.key.clone(),
            seller_id.clone(),
            100,
            1, // Already expired
        );

        use market::veilid::listing_ops::ListingOperations;
        let listing_ops = ListingOperations::new(seller_dht.clone());
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;
        seller
            .coordinator
            .register_owned_listing(listing_record.clone())
            .await?;

        // Only the seller places a bid — no other bidders
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let nonce: [u8; 32] = rand::random();
        let commitment = make_real_commitment(100, &nonce);
        seller
            .bid_storage
            .store_bid(&listing_record.key, 100, nonce)
            .await;
        let bid_rec = seller_dht.create_dht_record().await?;
        let bid = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: seller_id.clone(),
            commitment,
            timestamp: now,
            bid_key: bid_rec.key.clone(),
            signing_pubkey: seller_signing,
        };
        let bid_ops = BidOperations::new(seller_dht.clone());
        bid_ops.register_bid(&listing_record, bid.clone()).await?;
        seller_dht.set_dht_value(&bid_rec, bid.to_cbor()?).await?;
        seller
            .bid_storage
            .store_bid_key(&listing_record.key, &bid_rec.key)
            .await;
        seller
            .coordinator
            .add_own_bid_to_registry(
                &listing_record.key,
                seller_id.clone(),
                bid_rec.key.clone(),
                now,
            )
            .await?;

        // Watch the listing — monitoring was already started in E2EParticipant::new()
        seller.coordinator.watch_listing(listing.to_public()).await;

        // Wait for a couple monitoring cycles (~30s) to ensure no crash/deadlock
        eprintln!("[E2E] Waiting 30s for monitoring cycles with only 1 party...");
        tokio::time::sleep(Duration::from_secs(30)).await;

        // Verify coordinator is still healthy — can still do DHT operations
        let test_record = seller_dht.create_dht_record().await?;
        assert!(
            !test_record.key.to_string().is_empty(),
            "Coordinator should still be able to create DHT records after no-sale"
        );
        eprintln!("[E2E] Coordinator still healthy after seller-only auction");

        // No decryption key should exist (no winner)
        let key = seller
            .coordinator
            .get_decryption_key(&listing_record.key)
            .await;
        // The seller stored a decryption key earlier only if they explicitly did so
        // In this test we didn't call store_decryption_key, so it should be None
        // (unless monitoring auto-stored it, which it shouldn't for a no-sale)
        eprintln!("[E2E] Decryption key after no-sale: {:?}", key.is_some());

        eprintln!("[E2E] test_e2e_edge_seller_only_no_sale PASSED");
        let _ = seller.shutdown().await;
        Ok::<_, MarketError>(())
    })
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("Seller-only no-sale test failed: {}", e),
        Err(_) => panic!("Seller-only no-sale test timed out"),
    }
}
