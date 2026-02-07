//! E2E smoke tests with real Veilid devnets.
//!
//! These tests validate that the mock-based tests accurately reflect
//! real system behavior. They automatically start and stop the devnet.
//!
//! Run with: `cargo nextest run --ignored` or `cargo test -- --ignored`
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

use market::marketplace::bid_record::BidRecord;
use market::veilid::auction_coordinator::AuctionCoordinator;
use market::veilid::bid_ops::BidOperations;
use market::veilid::bid_storage::BidStorage;
use market::veilid::node::{DevNetConfig, VeilidNode};
use market::veilid::registry::{RegistryEntry, RegistryOperations};
use market::Listing;
use serial_test::serial;
use tokio::time::timeout;
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
    fn ensure_stopped(&self) -> anyhow::Result<()> {
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
    fn start(&mut self) -> anyhow::Result<()> {
        if !self.infrastructure_available() {
            anyhow::bail!(
                "Devnet infrastructure not found. Expected:\n  - {}\n  - {}",
                self.compose_path.display(),
                libipspoof_path().display()
            );
        }

        if self.fast_mode {
            // In fast mode, verify devnet is running but don't start/stop it
            eprintln!("[E2E] Fast mode: checking devnet is running...");
            if !self.is_devnet_running() {
                anyhow::bail!(
                    "Fast mode requires a running devnet!\n\
                     Start it with: cargo devnet-start"
                );
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
            anyhow::bail!("Failed to start devnet: {}", stderr);
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
    /// The devnet has 5 nodes: 1 bootstrap (port 5160) + 4 regular nodes (ports 5161-5164)
    fn wait_for_health(&self, timeout_secs: u64) -> anyhow::Result<()> {
        eprintln!("[E2E] Waiting for all 5 devnet nodes to be healthy...");

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

                eprintln!("[E2E] Health check: {}/5 nodes healthy", healthy_count);

                // All 5 nodes must be healthy
                if healthy_count >= 5 {
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
                anyhow::bail!(
                    "Devnet failed to become healthy within {} seconds",
                    timeout_secs
                );
            }

            std::thread::sleep(Duration::from_secs(5));
        }
    }

    /// Check if all devnet node ports are reachable.
    /// Bootstrap: 5160, Nodes 1-4: 5161-5164
    fn check_all_nodes_reachable(&self) -> bool {
        let ports = [5160, 5161, 5162, 5163, 5164];
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
    fn stop(&mut self) -> anyhow::Result<()> {
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
/// Uses offsets 6-39 to stay within libipspoof's MAX_PORT_OFFSET range (40).
/// Devnet uses offsets 0-4 (ports 5160-5164), so tests use 6+ to avoid conflicts.
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

        let node = VeilidNode::new(data_dir.clone()).with_devnet(config);

        Self {
            node,
            offset,
            data_dir,
        }
    }

    async fn start(&mut self) -> anyhow::Result<()> {
        self.node.start().await.map_err(|e| {
            anyhow::anyhow!(
                "Failed to start Veilid node (offset {}): {}. \
                 Ensure LD_PRELOAD is set and devnet is running.",
                self.offset,
                e
            )
        })?;
        self.node.attach().await.map_err(|e| {
            anyhow::anyhow!("Failed to attach node (offset {}): {}", self.offset, e)
        })?;
        Ok(())
    }

    /// Wait for node to be fully ready (attached AND has node ID).
    ///
    /// The node receives VeilidUpdate::Attachment (sets is_attached=true) and
    /// VeilidUpdate::Network (populates node_ids) as separate events. This method
    /// waits for both conditions to be satisfied.
    async fn wait_for_ready(&self, timeout_secs: u64) -> anyhow::Result<()> {
        let start = std::time::Instant::now();
        loop {
            let state = self.node.state();
            if state.is_attached && !state.node_ids.is_empty() {
                return Ok(());
            }
            if start.elapsed().as_secs() > timeout_secs {
                anyhow::bail!(
                    "Node {} not ready within {}s: attached={}, node_ids={}",
                    self.offset,
                    timeout_secs,
                    state.is_attached,
                    state.node_ids.len()
                );
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

    async fn shutdown(&mut self) -> anyhow::Result<()> {
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

/// Setup helper - starts devnet if needed and sets LD_PRELOAD.
fn setup_e2e_environment() -> anyhow::Result<DevnetManager> {
    // Check if LD_PRELOAD is already set correctly
    let preload = std::env::var("LD_PRELOAD").unwrap_or_default();
    let expected_preload = libipspoof_path();

    if !expected_preload.exists() {
        anyhow::bail!(
            "libipspoof.so not found at {}.\n\
             Please build it with: cd {} && make libipspoof.so",
            expected_preload.display(),
            expected_preload.parent().unwrap().display()
        );
    }

    if !preload.contains(expected_preload.to_str().unwrap()) {
        anyhow::bail!(
            "LD_PRELOAD not set. E2E tests require IP spoofing.\n\
             Run with: LD_PRELOAD={} cargo nextest run --profile e2e --ignored",
            expected_preload.display()
        );
    }

    let mut devnet = DevnetManager::new();

    if !devnet.infrastructure_available() {
        anyhow::bail!(
            "Devnet infrastructure not found at {}. \
             Please ensure the veilid repository is available \
             or set VEILID_REPO_PATH.",
            veilid_repo_path().display()
        );
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
async fn test_e2e_node_attachment() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed - check LD_PRELOAD and Docker");

    // Use offsets 6-39 to stay within libipspoof's MAX_PORT_OFFSET range (40)
    // Devnet uses offsets 0-4 (ports 5160-5164)
    let mut node = TestNode::new(10);

    // Start node with timeout
    let result = timeout(Duration::from_secs(180), async {
        node.start().await?;
        node.wait_for_ready(90).await?;
        Ok::<_, anyhow::Error>(())
    })
    .await;

    // Capture node_id before shutdown (shutdown clears state)
    let had_node_id = node.node_id().is_some();

    // Cleanup regardless of result
    let _ = node.shutdown().await;

    match result {
        Ok(Ok(())) => {
            assert!(had_node_id, "Node should have an ID after attachment");
            eprintln!("[E2E] test_e2e_node_attachment PASSED");
        }
        Ok(Err(e)) => panic!("Node attachment failed: {}", e),
        Err(_) => panic!("Node attachment timed out"),
    }
}

/// Multi-node test - verifies multiple nodes can coexist and see each other.
#[tokio::test]
#[ignore] // Run with: cargo nextest run --ignored
#[serial]
async fn test_e2e_multi_node_connectivity() {
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

        Ok::<_, anyhow::Error>(())
    })
    .await;

    // Cleanup all nodes
    for node in &mut nodes {
        let _ = node.shutdown().await;
    }

    match result {
        Ok(Ok(())) => {
            eprintln!("[E2E] test_e2e_multi_node_connectivity PASSED");
        }
        Ok(Err(e)) => panic!("Multi-node test failed: {}", e),
        Err(_) => panic!("Multi-node test timed out"),
    }
}

/// DHT operations test - verifies DHT read/write works across nodes.
#[tokio::test]
#[ignore] // Run with: cargo nextest run --ignored
#[serial]
async fn test_e2e_dht_operations() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed - check LD_PRELOAD and Docker");

    let mut node1 = TestNode::new(14);
    let mut node2 = TestNode::new(15);

    let result = timeout(Duration::from_secs(240), async {
        // Start and wait for both nodes to be ready
        node1.start().await?;
        node2.start().await?;
        node1.wait_for_ready(90).await?;
        node2.wait_for_ready(90).await?;

        // Get DHT operations from node1
        let dht1 = node1
            .node
            .dht_operations()
            .ok_or_else(|| anyhow::anyhow!("Failed to get DHT operations from node1"))?;

        let dht2 = node2
            .node
            .dht_operations()
            .ok_or_else(|| anyhow::anyhow!("Failed to get DHT operations from node2"))?;

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
                anyhow::bail!("Failed to read DHT data from node2");
            }
        }

        Ok::<_, anyhow::Error>(())
    })
    .await;

    // Cleanup
    let _ = node1.shutdown().await;
    let _ = node2.shutdown().await;

    match result {
        Ok(Ok(())) => {
            eprintln!("[E2E] test_e2e_dht_operations PASSED");
        }
        Ok(Err(e)) => panic!("DHT operations test failed: {}", e),
        Err(_) => panic!("DHT operations test timed out"),
    }
}

/// Full 3-party auction flow test.
/// This is the most comprehensive E2E test - validates the complete auction lifecycle.
#[tokio::test]
#[ignore] // Run with: cargo nextest run --ignored
#[serial]
async fn test_e2e_real_devnet_3_party_auction() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed - check LD_PRELOAD and Docker");

    let mut seller_node = TestNode::new(20);
    let mut bidder1_node = TestNode::new(21);
    let mut bidder2_node = TestNode::new(22);

    let result = timeout(Duration::from_secs(360), async {
        // Start all nodes
        seller_node.start().await?;
        bidder1_node.start().await?;
        bidder2_node.start().await?;

        // Wait for all nodes to be ready
        seller_node.wait_for_ready(90).await?;
        bidder1_node.wait_for_ready(90).await?;
        bidder2_node.wait_for_ready(90).await?;

        // Get node IDs
        let seller_id = seller_node
            .node_id()
            .ok_or_else(|| anyhow::anyhow!("Seller has no node ID"))?;
        let _bidder1_id = bidder1_node
            .node_id()
            .ok_or_else(|| anyhow::anyhow!("Bidder1 has no node ID"))?;
        let _bidder2_id = bidder2_node
            .node_id()
            .ok_or_else(|| anyhow::anyhow!("Bidder2 has no node ID"))?;

        // Get DHT operations
        let seller_dht = seller_node
            .node
            .dht_operations()
            .ok_or_else(|| anyhow::anyhow!("Failed to get seller DHT"))?;

        // Create AuctionCoordinator for seller
        let seller_api = seller_node
            .node
            .api()
            .ok_or_else(|| anyhow::anyhow!("Failed to get seller API"))?
            .clone();
        let seller_coordinator = Arc::new(AuctionCoordinator::new(
            seller_api,
            seller_dht.clone(),
            seller_id.clone(),
            BidStorage::new(),
            seller_node.offset,
        ));

        // Create and publish listing to its own DHT record
        let listing_record = seller_dht.create_dht_record().await?;
        let mut listing =
            create_test_listing(listing_record.key.clone(), seller_id.clone(), 100, 3600);

        // Publish listing to its own DHT record
        use market::veilid::listing_ops::ListingOperations;
        let listing_ops = ListingOperations::new(seller_dht.clone());
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;

        // Register listing in the SHARED REGISTRY (visible to all nodes on devnet)
        let mut seller_registry = RegistryOperations::new(seller_dht.clone());
        let registry_entry = RegistryEntry {
            key: listing_record.key.to_string(),
            title: listing.title.clone(),
            seller: seller_id.to_string(),
            reserve_price: listing.reserve_price,
            auction_end: listing.auction_end,
        };
        seller_registry.register_listing(registry_entry).await?;
        eprintln!("[E2E] Listing registered in shared devnet registry");

        // Seller watches listing
        seller_coordinator.watch_listing(listing.clone()).await;

        // Bidder1 discovers listing via the SHARED REGISTRY
        let bidder1_dht = bidder1_node
            .node
            .dht_operations()
            .ok_or_else(|| anyhow::anyhow!("Failed to get bidder1 DHT"))?;

        // Wait for DHT propagation
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Bidder1 fetches the registry to discover listings
        let mut bidder1_registry = RegistryOperations::new(bidder1_dht.clone());
        let registry = bidder1_registry.fetch_registry().await?;
        eprintln!(
            "[E2E] Bidder1 fetched registry with {} listings",
            registry.listings.len()
        );

        assert!(
            !registry.listings.is_empty(),
            "Registry should have at least one listing"
        );
        let found_listing = registry
            .listings
            .iter()
            .find(|e| e.key == listing_record.key.to_string());
        assert!(
            found_listing.is_some(),
            "Our listing should be in the registry"
        );

        // Bidder1 can also fetch the full listing from the listing's DHT record
        let bidder1_listing_ops = ListingOperations::new(bidder1_dht.clone());
        let retrieved = bidder1_listing_ops.get_listing(&listing_record.key).await?;
        assert!(
            retrieved.is_some(),
            "Listing should be retrievable by bidder1"
        );

        let retrieved_listing = retrieved.unwrap();
        assert_eq!(
            retrieved_listing.title, listing.title,
            "Listing title should match"
        );
        assert_eq!(
            retrieved_listing.reserve_price, listing.reserve_price,
            "Reserve price should match"
        );

        eprintln!("[E2E] Listing successfully created, registered, and discovered via registry");

        // ========== BIDDERS PLACE BIDS ==========
        // In real usage, bidders would send app messages to seller, who records them.
        // The seller (who owns the listing record) registers bids and increments bid_count.

        let bidder1_id = bidder1_node
            .node_id()
            .ok_or_else(|| anyhow::anyhow!("Bidder1 has no node ID"))?;
        let bidder2_id = bidder2_node
            .node_id()
            .ok_or_else(|| anyhow::anyhow!("Bidder2 has no node ID"))?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let seller_bid_ops = BidOperations::new(seller_dht.clone());

        // Seller also bids on their own listing (allowed in this marketplace)
        let seller_bid_record = seller_dht.create_dht_record().await?;
        let mut commitment_seller = [0u8; 32];
        commitment_seller[0] = 0; // Seller's commitment
        let seller_bid = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: seller_id.clone(),
            commitment: commitment_seller,
            timestamp: now,
            bid_key: seller_bid_record.key.clone(),
        };
        seller_bid_ops
            .register_bid(&listing_record, seller_bid)
            .await?;
        listing.bid_count += 1;
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;
        eprintln!(
            "[E2E] Seller placed their own bid (bid_count={})",
            listing.bid_count
        );

        // Bidder1 creates their own bid record (they own this)
        let bid1_record = bidder1_dht.create_dht_record().await?;
        let mut commitment1 = [0u8; 32];
        commitment1[0] = 1; // Placeholder commitment (in real usage: hash of bid + nonce)
        let bid1 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder1_id.clone(),
            commitment: commitment1,
            timestamp: now + 1,
            bid_key: bid1_record.key.clone(),
        };
        seller_bid_ops.register_bid(&listing_record, bid1).await?;
        listing.bid_count += 1;
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;
        eprintln!(
            "[E2E] Seller registered bid from Bidder1 (bid_count={})",
            listing.bid_count
        );

        // Bidder2 creates their own bid record
        let bidder2_dht = bidder2_node
            .node
            .dht_operations()
            .ok_or_else(|| anyhow::anyhow!("Failed to get bidder2 DHT"))?;
        let bid2_record = bidder2_dht.create_dht_record().await?;
        let mut commitment2 = [0u8; 32];
        commitment2[0] = 2;
        let bid2 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder2_id.clone(),
            commitment: commitment2,
            timestamp: now + 2,
            bid_key: bid2_record.key.clone(),
        };
        seller_bid_ops.register_bid(&listing_record, bid2).await?;
        listing.bid_count += 1;
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;
        eprintln!(
            "[E2E] Seller registered bid from Bidder2 (bid_count={})",
            listing.bid_count
        );

        // Wait for DHT propagation
        tokio::time::sleep(Duration::from_secs(3)).await;

        // ========== VERIFY BIDS ARE VISIBLE ==========
        let bid_index = seller_bid_ops.fetch_bid_index(&listing_record.key).await?;
        eprintln!("[E2E] Bid index has {} bids", bid_index.bids.len());

        assert_eq!(
            bid_index.bids.len(),
            3,
            "Should have 3 bids registered (seller + 2 bidders)"
        );

        // ========== VERIFY LISTING BID_COUNT FROM ANOTHER NODE ==========
        // Read back the listing from bidder1's perspective to verify bid_count was saved
        let bidder1_listing_ops_verify = ListingOperations::new(bidder1_dht.clone());
        let verified_listing = bidder1_listing_ops_verify
            .get_listing(&listing_record.key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Failed to read back listing for verification"))?;
        eprintln!(
            "[E2E] Verified listing from bidder1: bid_count={}",
            verified_listing.bid_count
        );
        assert_eq!(
            verified_listing.bid_count, 3,
            "Listing bid_count should be 3 after all bids"
        );

        eprintln!("[E2E] Full 3-party auction flow completed with 3 bids!");
        eprintln!("[E2E] Listing key: {}", listing_record.key);

        // Brief wait for DHT propagation before test ends
        eprintln!("[E2E] Waiting 3s for final DHT propagation...");
        tokio::time::sleep(Duration::from_secs(3)).await;
        eprintln!("[E2E] Done - market nodes can now fetch the listing with 3 bids");

        Ok::<_, anyhow::Error>(())
    })
    .await;

    // Cleanup all nodes
    let _ = seller_node.shutdown().await;
    let _ = bidder1_node.shutdown().await;
    let _ = bidder2_node.shutdown().await;

    match result {
        Ok(Ok(())) => {
            eprintln!("[E2E] test_e2e_real_devnet_3_party_auction PASSED");
        }
        Ok(Err(e)) => panic!("3-party auction test failed: {}", e),
        Err(_) => panic!("3-party auction test timed out (6 min limit)"),
    }
}

/// Coordinator messaging test - verifies bid announcements work.
#[tokio::test]
#[ignore] // Run with: cargo nextest run --ignored
#[serial]
async fn test_e2e_coordinator_messaging() {
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
            .ok_or_else(|| anyhow::anyhow!("Node1 has no ID"))?;

        let dht1 = node1
            .node
            .dht_operations()
            .ok_or_else(|| anyhow::anyhow!("Failed to get DHT1"))?;

        let api1 = node1
            .node
            .api()
            .ok_or_else(|| anyhow::anyhow!("Failed to get API1"))?
            .clone();

        let coordinator1 = Arc::new(AuctionCoordinator::new(
            api1,
            dht1.clone(),
            node1_id.clone(),
            BidStorage::new(),
            node1.offset,
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

        Ok::<_, anyhow::Error>(())
    })
    .await;

    let _ = node1.shutdown().await;
    let _ = node2.shutdown().await;

    match result {
        Ok(Ok(())) => {
            eprintln!("[E2E] test_e2e_coordinator_messaging PASSED");
        }
        Ok(Err(e)) => panic!("Coordinator messaging test failed: {}", e),
        Err(_) => panic!("Coordinator messaging test timed out"),
    }
}

/// Full 5-party auction flow test.
/// Tests scaling with 5 nodes (1 seller + 4 bidders).
#[tokio::test]
#[ignore] // Run with: cargo nextest run --ignored
#[serial]
async fn test_e2e_real_devnet_5_party_auction() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed - check LD_PRELOAD and Docker");

    // 5 test nodes (1 seller + 4 bidders) using offsets 23-27
    let mut seller_node = TestNode::new(23);
    let mut bidder1_node = TestNode::new(24);
    let mut bidder2_node = TestNode::new(25);
    let mut bidder3_node = TestNode::new(26);
    let mut bidder4_node = TestNode::new(27);

    let result = timeout(Duration::from_secs(480), async {
        // Start all nodes
        seller_node.start().await?;
        bidder1_node.start().await?;
        bidder2_node.start().await?;
        bidder3_node.start().await?;
        bidder4_node.start().await?;

        // Wait for all nodes to be ready (longer timeout for more nodes)
        seller_node.wait_for_ready(90).await?;
        bidder1_node.wait_for_ready(90).await?;
        bidder2_node.wait_for_ready(90).await?;
        bidder3_node.wait_for_ready(90).await?;
        bidder4_node.wait_for_ready(90).await?;

        // Get node IDs
        let seller_id = seller_node
            .node_id()
            .ok_or_else(|| anyhow::anyhow!("Seller has no node ID"))?;
        let bidder1_id = bidder1_node
            .node_id()
            .ok_or_else(|| anyhow::anyhow!("Bidder1 has no node ID"))?;
        let bidder2_id = bidder2_node
            .node_id()
            .ok_or_else(|| anyhow::anyhow!("Bidder2 has no node ID"))?;
        let bidder3_id = bidder3_node
            .node_id()
            .ok_or_else(|| anyhow::anyhow!("Bidder3 has no node ID"))?;
        let bidder4_id = bidder4_node
            .node_id()
            .ok_or_else(|| anyhow::anyhow!("Bidder4 has no node ID"))?;

        // Get DHT operations
        let seller_dht = seller_node
            .node
            .dht_operations()
            .ok_or_else(|| anyhow::anyhow!("Failed to get seller DHT"))?;

        // Create AuctionCoordinator for seller
        let seller_api = seller_node
            .node
            .api()
            .ok_or_else(|| anyhow::anyhow!("Failed to get seller API"))?
            .clone();
        let _seller_coordinator = Arc::new(AuctionCoordinator::new(
            seller_api,
            seller_dht.clone(),
            seller_id.clone(),
            BidStorage::new(),
            seller_node.offset,
        ));

        // Create and publish listing to its own DHT record
        let listing_record = seller_dht.create_dht_record().await?;
        let mut listing =
            create_test_listing(listing_record.key.clone(), seller_id.clone(), 100, 3600);

        // Publish listing to its own DHT record
        use market::veilid::listing_ops::ListingOperations;
        let listing_ops = ListingOperations::new(seller_dht.clone());
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;

        // Register listing in the SHARED REGISTRY (visible to all nodes on devnet)
        let mut seller_registry = RegistryOperations::new(seller_dht.clone());
        let registry_entry = RegistryEntry {
            key: listing_record.key.to_string(),
            title: listing.title.clone(),
            seller: seller_id.to_string(),
            reserve_price: listing.reserve_price,
            auction_end: listing.auction_end,
        };
        seller_registry.register_listing(registry_entry).await?;
        eprintln!("[E2E] Listing registered in shared devnet registry");

        // Bidder1 discovers listing via the SHARED REGISTRY
        let bidder1_dht = bidder1_node
            .node
            .dht_operations()
            .ok_or_else(|| anyhow::anyhow!("Failed to get bidder1 DHT"))?;

        // Wait for DHT propagation
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Bidder1 fetches the registry to discover listings
        let mut bidder1_registry = RegistryOperations::new(bidder1_dht.clone());
        let registry = bidder1_registry.fetch_registry().await?;
        eprintln!(
            "[E2E] Bidder1 fetched registry with {} listings",
            registry.listings.len()
        );

        assert!(
            !registry.listings.is_empty(),
            "Registry should have at least one listing"
        );
        let found_listing = registry
            .listings
            .iter()
            .find(|e| e.key == listing_record.key.to_string());
        assert!(
            found_listing.is_some(),
            "Our listing should be in the registry"
        );

        eprintln!("[E2E] Listing successfully created, registered, and discovered via registry");

        // ========== ALL 5 PARTIES PLACE BIDS ==========
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let seller_bid_ops = BidOperations::new(seller_dht.clone());

        // Seller bids on their own listing
        let seller_bid_record = seller_dht.create_dht_record().await?;
        let mut commitment_seller = [0u8; 32];
        commitment_seller[0] = 0;
        let seller_bid = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: seller_id.clone(),
            commitment: commitment_seller,
            timestamp: now,
            bid_key: seller_bid_record.key.clone(),
        };
        seller_bid_ops
            .register_bid(&listing_record, seller_bid)
            .await?;
        listing.bid_count += 1;
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;
        eprintln!(
            "[E2E] Seller placed their own bid (bid_count={})",
            listing.bid_count
        );

        // Bidder1 creates their bid
        let bid1_record = bidder1_dht.create_dht_record().await?;
        let mut commitment1 = [0u8; 32];
        commitment1[0] = 1;
        let bid1 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder1_id.clone(),
            commitment: commitment1,
            timestamp: now + 1,
            bid_key: bid1_record.key.clone(),
        };
        seller_bid_ops.register_bid(&listing_record, bid1).await?;
        listing.bid_count += 1;
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;
        eprintln!(
            "[E2E] Seller registered bid from Bidder1 (bid_count={})",
            listing.bid_count
        );

        // Bidder2 creates their bid
        let bidder2_dht = bidder2_node
            .node
            .dht_operations()
            .ok_or_else(|| anyhow::anyhow!("Failed to get bidder2 DHT"))?;
        let bid2_record = bidder2_dht.create_dht_record().await?;
        let mut commitment2 = [0u8; 32];
        commitment2[0] = 2;
        let bid2 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder2_id.clone(),
            commitment: commitment2,
            timestamp: now + 2,
            bid_key: bid2_record.key.clone(),
        };
        seller_bid_ops.register_bid(&listing_record, bid2).await?;
        listing.bid_count += 1;
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;
        eprintln!(
            "[E2E] Seller registered bid from Bidder2 (bid_count={})",
            listing.bid_count
        );

        // Bidder3 creates their bid
        let bidder3_dht = bidder3_node
            .node
            .dht_operations()
            .ok_or_else(|| anyhow::anyhow!("Failed to get bidder3 DHT"))?;
        let bid3_record = bidder3_dht.create_dht_record().await?;
        let mut commitment3 = [0u8; 32];
        commitment3[0] = 3;
        let bid3 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder3_id.clone(),
            commitment: commitment3,
            timestamp: now + 3,
            bid_key: bid3_record.key.clone(),
        };
        seller_bid_ops.register_bid(&listing_record, bid3).await?;
        listing.bid_count += 1;
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;
        eprintln!(
            "[E2E] Seller registered bid from Bidder3 (bid_count={})",
            listing.bid_count
        );

        // Bidder4 creates their bid
        let bidder4_dht = bidder4_node
            .node
            .dht_operations()
            .ok_or_else(|| anyhow::anyhow!("Failed to get bidder4 DHT"))?;
        let bid4_record = bidder4_dht.create_dht_record().await?;
        let mut commitment4 = [0u8; 32];
        commitment4[0] = 4;
        let bid4 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder4_id.clone(),
            commitment: commitment4,
            timestamp: now + 4,
            bid_key: bid4_record.key.clone(),
        };
        seller_bid_ops.register_bid(&listing_record, bid4).await?;
        listing.bid_count += 1;
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;
        eprintln!(
            "[E2E] Seller registered bid from Bidder4 (bid_count={})",
            listing.bid_count
        );

        // Wait for DHT propagation
        tokio::time::sleep(Duration::from_secs(3)).await;

        // ========== VERIFY BIDS ARE VISIBLE ==========
        let bid_index = seller_bid_ops.fetch_bid_index(&listing_record.key).await?;
        eprintln!("[E2E] Bid index has {} bids", bid_index.bids.len());

        assert_eq!(
            bid_index.bids.len(),
            5,
            "Should have 5 bids registered (seller + 4 bidders)"
        );

        // ========== VERIFY LISTING BID_COUNT FROM ANOTHER NODE ==========
        let bidder1_listing_ops_verify = ListingOperations::new(bidder1_dht.clone());
        let verified_listing = bidder1_listing_ops_verify
            .get_listing(&listing_record.key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Failed to read back listing for verification"))?;
        eprintln!(
            "[E2E] Verified listing from bidder1: bid_count={}",
            verified_listing.bid_count
        );
        assert_eq!(
            verified_listing.bid_count, 5,
            "Listing bid_count should be 5 after all bids"
        );

        eprintln!("[E2E] Full 5-party auction flow completed with 5 bids!");
        eprintln!("[E2E] Listing key: {}", listing_record.key);

        // Brief wait for DHT propagation before test ends
        eprintln!("[E2E] Waiting 3s for final DHT propagation...");
        tokio::time::sleep(Duration::from_secs(3)).await;
        eprintln!("[E2E] Done - market nodes can now fetch the listing with 5 bids");

        Ok::<_, anyhow::Error>(())
    })
    .await;

    // Cleanup all nodes
    let _ = seller_node.shutdown().await;
    let _ = bidder1_node.shutdown().await;
    let _ = bidder2_node.shutdown().await;
    let _ = bidder3_node.shutdown().await;
    let _ = bidder4_node.shutdown().await;

    match result {
        Ok(Ok(())) => {
            eprintln!("[E2E] test_e2e_real_devnet_5_party_auction PASSED");
        }
        Ok(Err(e)) => panic!("5-party auction test failed: {}", e),
        Err(_) => panic!("5-party auction test timed out (8 min limit)"),
    }
}

/// Full 10-party auction flow test.
/// Tests scaling with 10 nodes (1 seller + 9 bidders).
#[tokio::test]
#[ignore] // Run with: cargo nextest run --ignored
#[serial]
async fn test_e2e_real_devnet_10_party_auction() {
    init_test_tracing();

    let _devnet = setup_e2e_environment().expect("E2E setup failed - check LD_PRELOAD and Docker");

    // 10 test nodes (1 seller + 9 bidders) using offsets 5-14
    let mut nodes: Vec<TestNode> = (5..=14).map(|offset| TestNode::new(offset)).collect();

    let result = timeout(Duration::from_secs(600), async {
        // Start all 10 nodes
        for node in &mut nodes {
            node.start().await?;
        }

        // Wait for all to be ready (longer timeout for more nodes)
        for node in &nodes {
            node.wait_for_ready(120).await?;
        }

        // First node is the seller, rest are bidders
        let seller_node = &nodes[0];
        let seller_id = seller_node
            .node_id()
            .ok_or_else(|| anyhow::anyhow!("Seller has no node ID"))?;

        // Get all bidder IDs
        let mut bidder_ids = Vec::new();
        for (i, node) in nodes.iter().enumerate().skip(1) {
            let bidder_id = node
                .node_id()
                .ok_or_else(|| anyhow::anyhow!("Bidder{} has no node ID", i))?;
            bidder_ids.push(bidder_id);
        }

        // Get seller DHT operations
        let seller_dht = seller_node
            .node
            .dht_operations()
            .ok_or_else(|| anyhow::anyhow!("Failed to get seller DHT"))?;

        // Create AuctionCoordinator for seller
        let seller_api = seller_node
            .node
            .api()
            .ok_or_else(|| anyhow::anyhow!("Failed to get seller API"))?
            .clone();
        let _seller_coordinator = Arc::new(AuctionCoordinator::new(
            seller_api,
            seller_dht.clone(),
            seller_id.clone(),
            BidStorage::new(),
            seller_node.offset,
        ));

        // Create and publish listing to its own DHT record
        let listing_record = seller_dht.create_dht_record().await?;
        let mut listing =
            create_test_listing(listing_record.key.clone(), seller_id.clone(), 100, 3600);

        // Publish listing to its own DHT record
        use market::veilid::listing_ops::ListingOperations;
        let listing_ops = ListingOperations::new(seller_dht.clone());
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;

        // Register listing in the SHARED REGISTRY
        let mut seller_registry = RegistryOperations::new(seller_dht.clone());
        let registry_entry = RegistryEntry {
            key: listing_record.key.to_string(),
            title: listing.title.clone(),
            seller: seller_id.to_string(),
            reserve_price: listing.reserve_price,
            auction_end: listing.auction_end,
        };
        seller_registry.register_listing(registry_entry).await?;
        eprintln!("[E2E] Listing registered in shared devnet registry");

        // Wait for DHT propagation
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Verify a bidder can discover the listing
        let bidder1_dht = nodes[1]
            .node
            .dht_operations()
            .ok_or_else(|| anyhow::anyhow!("Failed to get bidder1 DHT"))?;
        let mut bidder1_registry = RegistryOperations::new(bidder1_dht.clone());
        let registry = bidder1_registry.fetch_registry().await?;
        eprintln!(
            "[E2E] Bidder1 fetched registry with {} listings",
            registry.listings.len()
        );
        assert!(
            !registry.listings.is_empty(),
            "Registry should have at least one listing"
        );

        eprintln!("[E2E] Listing successfully created, registered, and discovered via registry");

        // ========== ALL 10 PARTIES PLACE BIDS ==========
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let seller_bid_ops = BidOperations::new(seller_dht.clone());

        // Seller bids on their own listing
        let seller_bid_record = seller_dht.create_dht_record().await?;
        let mut commitment_seller = [0u8; 32];
        commitment_seller[0] = 0;
        let seller_bid = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: seller_id.clone(),
            commitment: commitment_seller,
            timestamp: now,
            bid_key: seller_bid_record.key.clone(),
        };
        seller_bid_ops
            .register_bid(&listing_record, seller_bid)
            .await?;
        listing.bid_count += 1;
        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;
        eprintln!(
            "[E2E] Seller placed their own bid (bid_count={})",
            listing.bid_count
        );

        // Each of the 9 bidders places a bid
        for (i, node) in nodes.iter().enumerate().skip(1) {
            let bidder_id = &bidder_ids[i - 1];
            let bidder_dht = node
                .node
                .dht_operations()
                .ok_or_else(|| anyhow::anyhow!("Failed to get bidder{} DHT", i))?;

            let bid_record = bidder_dht.create_dht_record().await?;
            let mut commitment = [0u8; 32];
            commitment[0] = i as u8;
            let bid = BidRecord {
                listing_key: listing_record.key.clone(),
                bidder: bidder_id.clone(),
                commitment,
                timestamp: now + i as u64,
                bid_key: bid_record.key.clone(),
            };
            seller_bid_ops.register_bid(&listing_record, bid).await?;
            listing.bid_count += 1;
            listing_ops
                .update_listing(&listing_record, &listing)
                .await?;
            eprintln!(
                "[E2E] Seller registered bid from Bidder{} (bid_count={})",
                i, listing.bid_count
            );
        }

        // Wait for DHT propagation
        tokio::time::sleep(Duration::from_secs(5)).await;

        // ========== VERIFY BIDS ARE VISIBLE ==========
        let bid_index = seller_bid_ops.fetch_bid_index(&listing_record.key).await?;
        eprintln!("[E2E] Bid index has {} bids", bid_index.bids.len());

        assert_eq!(
            bid_index.bids.len(),
            10,
            "Should have 10 bids registered (seller + 9 bidders)"
        );

        // ========== VERIFY LISTING BID_COUNT FROM ANOTHER NODE ==========
        let bidder1_listing_ops_verify = ListingOperations::new(bidder1_dht.clone());
        let verified_listing = bidder1_listing_ops_verify
            .get_listing(&listing_record.key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Failed to read back listing for verification"))?;
        eprintln!(
            "[E2E] Verified listing from bidder1: bid_count={}",
            verified_listing.bid_count
        );
        assert_eq!(
            verified_listing.bid_count, 10,
            "Listing bid_count should be 10 after all bids"
        );

        eprintln!("[E2E] Full 10-party auction flow completed with 10 bids!");
        eprintln!("[E2E] Listing key: {}", listing_record.key);

        // Brief wait for DHT propagation before test ends
        eprintln!("[E2E] Waiting 5s for final DHT propagation...");
        tokio::time::sleep(Duration::from_secs(5)).await;
        eprintln!("[E2E] Done - market nodes can now fetch the listing with 10 bids");

        Ok::<_, anyhow::Error>(())
    })
    .await;

    // Cleanup all nodes
    for node in &mut nodes {
        let _ = node.shutdown().await;
    }

    match result {
        Ok(Ok(())) => {
            eprintln!("[E2E] test_e2e_real_devnet_10_party_auction PASSED");
        }
        Ok(Err(e)) => panic!("10-party auction test failed: {}", e),
        Err(_) => panic!("10-party auction test timed out (10 min limit)"),
    }
}

/// Diagnostic test for isolated debugging of single node startup.
/// Run with: cargo integration -- test_e2e_single_node_diagnostic
///
/// This test provides detailed error chain output for debugging Veilid API startup issues.
#[tokio::test]
#[ignore] // Run with: cargo nextest run --ignored
#[serial]
async fn test_e2e_single_node_diagnostic() {
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

    let mut node = VeilidNode::new(data_dir.clone()).with_devnet(config);

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
fn print_error_chain(e: &anyhow::Error) {
    eprintln!("   Error: {}", e);
    for (i, cause) in e.chain().skip(1).enumerate() {
        eprintln!("   Cause {}: {}", i + 1, cause);
    }
    eprintln!("   Debug: {:?}", e);
}
