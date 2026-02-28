//! Shared E2E test infrastructure: devnet management, node lifecycle, and helpers.
//!
//! This module provides the building blocks for E2E tests that run against
//! a real Veilid devnet. It is imported by both `smoke` and `edge_cases`.

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use market::error::{MarketError, MarketResult};
use market::veilid::auction_coordinator::AuctionCoordinator;
use market::veilid::bid_storage::BidStorage;
use market::veilid::node::{DevNetConfig, VeilidNode};
use market::Listing;

use tokio_util::sync::CancellationToken;
use veilid_core::PublicKey;

// ── Path helpers ─────────────────────────────────────────────────────

/// Path to the veilid repository containing devnet infrastructure.
pub fn veilid_repo_path() -> PathBuf {
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
pub fn libipspoof_path() -> PathBuf {
    veilid_repo_path().join(".devcontainer/scripts/libipspoof.so")
}

/// Path to docker-compose file for devnet.
fn docker_compose_path() -> PathBuf {
    veilid_repo_path().join(".devcontainer/compose/docker-compose.dev.yml")
}

/// Check if running in fast mode (persistent devnet).
fn is_fast_mode() -> bool {
    std::env::var("E2E_FAST_MODE").is_ok()
}

// ── DevnetManager ────────────────────────────────────────────────────

/// Manages the Veilid devnet lifecycle for E2E tests.
pub struct DevnetManager {
    compose_path: PathBuf,
    started: bool,
    fast_mode: bool,
}

impl DevnetManager {
    pub fn new() -> Self {
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

    pub fn infrastructure_available(&self) -> bool {
        self.compose_path.exists() && libipspoof_path().exists()
    }

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
            let stderr = String::from_utf8_lossy(&output.stderr);
            eprintln!("[E2E] Note: docker compose down returned: {}", stderr);
        }
        Ok(())
    }

    pub fn start(&mut self) -> MarketResult<()> {
        if !self.infrastructure_available() {
            return Err(MarketError::Config(format!(
                "Devnet infrastructure not found. Expected:\n  - {}\n  - {}",
                self.compose_path.display(),
                libipspoof_path().display()
            )));
        }

        if self.fast_mode {
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
                stdout.lines().count() >= 1
            }
            Err(_) => false,
        }
    }

    pub fn wait_for_health(&self, timeout_secs: u64) -> MarketResult<()> {
        eprintln!("[E2E] Waiting for all 20 devnet nodes to be healthy...");
        let start = std::time::Instant::now();
        loop {
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

                eprintln!("[E2E] Health check: {}/20 nodes healthy", healthy_count);

                if healthy_count >= 20 && self.check_all_nodes_reachable() {
                    eprintln!("[E2E] All devnet nodes are healthy and reachable!");
                    return Ok(());
                }
            }

            if start.elapsed().as_secs() > timeout_secs {
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

    fn check_all_nodes_reachable(&self) -> bool {
        let ports: Vec<u16> = (5160..=5179).collect();
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

    pub fn stop(&mut self) -> MarketResult<()> {
        if self.fast_mode {
            eprintln!("[E2E] Fast mode: keeping devnet running");
            return Ok(());
        }
        if !self.started {
            return Ok(());
        }
        eprintln!("[E2E] Stopping devnet...");
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
        if !self.fast_mode {
            let _ = self.stop();
        }
    }
}

// ── TestNode ─────────────────────────────────────────────────────────

/// Test node configuration for E2E tests.
/// Uses offsets 20-39 to stay within libipspoof's MAX_PORT_OFFSET range (40).
/// Devnet uses offsets 0-19 (20 nodes).
pub struct TestNode {
    pub node: VeilidNode,
    pub offset: u16,
    data_dir: PathBuf,
}

impl TestNode {
    pub fn new(offset: u16) -> Self {
        let data_dir = std::env::temp_dir().join(format!("market-e2e-test-{}", offset));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("Failed to create test data dir");

        let config = DevNetConfig {
            network_key: "development-network-2025".to_string(),
            bootstrap_nodes: vec!["udp://1.2.3.1:5160".to_string()],
            port_offset: offset,
            limit_over_attached: 24,
            listen_addr: None,
            public_addr: None,
        };

        let mut market_config = market::config::MarketConfig::default();
        market_config.insecure_storage = true;
        let node = VeilidNode::new(data_dir.clone(), &market_config).with_devnet(config);

        Self {
            node,
            offset,
            data_dir,
        }
    }

    pub async fn start(&mut self) -> MarketResult<()> {
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

    pub async fn wait_for_ready(&self, timeout_secs: u64) -> MarketResult<()> {
        let start = std::time::Instant::now();
        // Phase 1: wait for attachment + node IDs
        loop {
            let state = self.node.state();
            if state.is_attached && !state.node_ids.is_empty() {
                break;
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
        // Phase 2: wait for enough peers so routing table can support
        // private route relay operations.  AttachedWeak (2 peers) is
        // insufficient — routes die immediately because relay nodes
        // aren't in the table yet.
        let min_peers = 4;
        loop {
            let state = self.node.state();
            if state.peer_count >= min_peers {
                eprintln!(
                    "[E2E] Node {} ready: {} peers (needed {})",
                    self.offset, state.peer_count, min_peers
                );
                return Ok(());
            }
            if start.elapsed().as_secs() > timeout_secs {
                let state = self.node.state();
                eprintln!(
                    "[E2E] Node {} peer wait timeout: {} peers (needed {}), proceeding anyway",
                    self.offset, state.peer_count, min_peers
                );
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    pub fn node_id(&self) -> Option<PublicKey> {
        let state = self.node.state();
        state
            .node_ids
            .first()
            .and_then(|s| PublicKey::try_from(s.as_str()).ok())
    }

    pub async fn shutdown(&mut self) -> MarketResult<()> {
        self.node.shutdown().await?;
        let _ = std::fs::remove_dir_all(&self.data_dir);
        Ok(())
    }
}

// ── E2EParticipant ───────────────────────────────────────────────────

/// A participant in an E2E auction test. Bundles a TestNode, AuctionCoordinator,
/// BidStorage, and the AppMessage processing loop — replicating what `main.rs` does.
pub struct E2EParticipant {
    pub node: TestNode,
    pub coordinator: Arc<AuctionCoordinator>,
    pub bid_storage: BidStorage,
    _msg_loop_handle: Option<tokio::task::JoinHandle<()>>,
}

impl E2EParticipant {
    pub async fn new(offset: u16) -> MarketResult<Self> {
        let mut node = TestNode::new(offset);
        node.start().await?;
        node.wait_for_ready(180).await?;

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

        let update_rx = node.node.take_update_receiver();
        let coord_clone = coordinator.clone();
        let msg_loop_handle = tokio::spawn(async move {
            if let Some(mut rx) = update_rx {
                while let Some(update) = rx.recv().await {
                    match update {
                        veilid_core::VeilidUpdate::AppMessage(msg) => {
                            if let Err(e) = coord_clone
                                .process_app_message(msg.message().to_vec())
                                .await
                            {
                                tracing::error!("Failed to process AppMessage: {}", e);
                            }
                        }
                        veilid_core::VeilidUpdate::AppCall(call) => {
                            tracing::info!(
                                ">>> AppCall received: {} bytes, id={}",
                                call.message().len(),
                                call.id()
                            );
                            let api = coord_clone.api().clone();
                            let call_id = call.id();
                            match coord_clone.process_app_call(call.message().to_vec()).await {
                                Ok(response) => {
                                    if let Err(e) = api.app_call_reply(call_id, response).await {
                                        tracing::error!("Failed to send app_call reply: {}", e);
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to process AppCall: {}", e);
                                    let _ = api.app_call_reply(call_id, vec![0x00]).await;
                                }
                            }
                        }
                        veilid_core::VeilidUpdate::RouteChange(change) => {
                            coord_clone
                                .handle_route_change(
                                    change.dead_routes.clone(),
                                    change.dead_remote_routes.clone(),
                                )
                                .await;
                        }
                        _ => {}
                    }
                }
            }
        });

        coordinator.clone().start_monitoring();

        Ok(Self {
            node,
            coordinator,
            bid_storage,
            _msg_loop_handle: Some(msg_loop_handle),
        })
    }

    pub fn node_id(&self) -> Option<PublicKey> {
        self.node.node_id()
    }

    pub fn dht(&self) -> Option<market::DHTOperations> {
        self.node.node.dht_operations()
    }

    pub fn signing_pubkey_bytes(&self) -> [u8; 32] {
        self.coordinator.signing_pubkey_bytes()
    }

    pub async fn shutdown(&mut self) -> MarketResult<()> {
        self.node.shutdown().await
    }
}

// ── Listing helpers ──────────────────────────────────────────────────

/// Real unix time for listing `created_at` so that `auction_end` is a
/// real-world deadline (not immediately expired as with `MockTime(1000)`).
fn listing_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("System clock before epoch")
        .as_secs()
}

/// Create a test listing with a real `created_at` timestamp.
pub fn create_test_listing(
    key: veilid_core::RecordKey,
    seller: PublicKey,
    reserve_price: u64,
    duration_secs: u64,
) -> Listing {
    use market::mocks::MockTime;

    Listing::builder_with_time(MockTime::new(listing_now()))
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

/// Create a listing with real AES-256-GCM encrypted content.
pub fn create_encrypted_listing(
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

    Listing::builder_with_time(MockTime::new(listing_now()))
        .key(key)
        .seller(seller)
        .title("E2E Encrypted Test Item")
        .encrypted_content(ciphertext, nonce, decryption_key_hex)
        .reserve_price(reserve_price)
        .auction_duration(duration_secs)
        .build()
        .expect("Failed to build encrypted listing")
}

/// Compute SHA256(bid_value || nonce) — the real commitment scheme.
pub fn make_real_commitment(bid_value: u64, nonce: &[u8; 32]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bid_value.to_le_bytes());
    hasher.update(nonce);
    hasher.finalize().into()
}

/// Check if the configured MP-SPDZ protocol binary is available.
pub fn check_mp_spdz_available() -> bool {
    let mp_spdz_dir = std::env::var(market::config::MP_SPDZ_DIR_ENV)
        .unwrap_or_else(|_| market::config::DEFAULT_MP_SPDZ_DIR.to_string());
    let protocol = std::env::var(market::config::MPC_PROTOCOL_ENV)
        .unwrap_or_else(|_| market::config::DEFAULT_MPC_PROTOCOL.to_string());
    let binary = std::path::Path::new(&mp_spdz_dir).join(&protocol);
    binary.exists()
}

/// Setup helper — starts devnet and validates LD_PRELOAD/libipspoof prerequisites.
pub fn setup_e2e_environment() -> MarketResult<DevnetManager> {
    // Kill any orphaned processes from previous runs.
    // When a test process is killed (e.g. timeout, Ctrl-C), child
    // processes become orphans and hold TCP/UDP ports indefinitely,
    // causing "address already in use" on the next run.
    let _ = std::process::Command::new("pkill")
        .arg("-f")
        .arg("market-headless")
        .status();
    let _ = std::process::Command::new("pkill")
        .arg("-f")
        .arg("mascot-party.x")
        .status();
    let _ = std::process::Command::new("pkill")
        .arg("-f")
        .arg("replicated-ring-party.x")
        .status();
    let _ = std::process::Command::new("pkill")
        .arg("-f")
        .arg("shamir-party.x")
        .status();

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

    devnet.start()?;
    devnet.wait_for_health(60)?;
    Ok(devnet)
}

/// Helper to print the full error chain for debugging.
pub fn print_error_chain(e: &MarketError) {
    eprintln!("   Error: {}", e);
    eprintln!("   Debug: {:?}", e);
}

/// Wait until at least `expected_peers` broadcast routes are visible from a
/// node's perspective.  Returns `true` if the routes were found, `false` on
/// timeout.
#[allow(dead_code)]
pub async fn wait_for_broadcast_routes(
    coordinator: &AuctionCoordinator,
    my_node_id: &str,
    expected_peers: usize,
    timeout_secs: u64,
) -> bool {
    let start = tokio::time::Instant::now();
    let max_wait = Duration::from_secs(timeout_secs);
    loop {
        let routes = {
            let mut ops = coordinator.registry_ops().lock().await;
            ops.fetch_route_blobs(my_node_id).await
        };
        match routes {
            Ok(r) if r.len() >= expected_peers => {
                eprintln!(
                    "[E2E] Broadcast routes ready: {} peers visible (needed {})",
                    r.len(),
                    expected_peers
                );
                return true;
            }
            Ok(r) => {
                eprintln!(
                    "[E2E] Only {} peer routes visible (need {}), waiting...",
                    r.len(),
                    expected_peers
                );
            }
            Err(e) => {
                eprintln!("[E2E] Route fetch error: {}, retrying...", e);
            }
        }
        if start.elapsed() > max_wait {
            eprintln!(
                "[E2E] WARNING: Route registration timed out after {}s",
                timeout_secs
            );
            return false;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// Initialize tracing for tests.
///
/// Writes to `/tmp/e2e-trace.log` for real-time observation with `tail -f`.
pub fn init_test_tracing() {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,veilid_core=warn,market=debug"));
    if let Ok(file) = std::fs::File::create("/tmp/e2e-trace.log") {
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::sync::Mutex::new(file))
                    .with_ansi(false),
            )
            .try_init();
    } else {
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().with_test_writer())
            .try_init();
    }
}

/// Run an E2E test with standard boilerplate: tracing init, devnet setup,
/// timeout wrapping, and pass/fail reporting.
pub async fn run_e2e_test<F, Fut>(name: &str, timeout_secs: u64, f: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<(), MarketError>>,
{
    init_test_tracing();
    let _devnet = setup_e2e_environment().expect("E2E setup failed");
    match tokio::time::timeout(Duration::from_secs(timeout_secs), f()).await {
        Ok(Ok(())) => eprintln!("[E2E] {} PASSED", name),
        Ok(Err(e)) => {
            print_error_chain(&e);
            panic!("{} failed: {}", name, e);
        }
        Err(_) => panic!("{} timed out after {}s", name, timeout_secs),
    }
}

// ── HeadlessParticipant ─────────────────────────────────────────────

/// IPC-based test participant that runs `market-headless` in a child process.
///
/// Each participant gets its own OS process with its own tokio runtime,
/// eliminating the in-process runtime contention that causes 40x MPC slowdown.
#[allow(dead_code)]
pub struct HeadlessParticipant {
    child: tokio::process::Child,
    stdin: tokio::io::BufWriter<tokio::process::ChildStdin>,
    stdout: tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStdout>>,
    pub node_id: String,
    pub signing_pubkey: String,
    pub offset: u16,
}

impl HeadlessParticipant {
    /// Spawn a `market-headless` child process and wait for the `Ready` event.
    pub async fn new(offset: u16) -> MarketResult<Self> {
        use tokio::io::{AsyncBufReadExt, BufReader, BufWriter};
        use tokio::process::Command;

        // Locate the binary relative to CARGO_MANIFEST_DIR
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let binary = manifest_dir
            .join("target")
            .join("debug")
            .join("market-headless");

        if !binary.exists() {
            // Try standard cargo target dir (workspace root)
            let workspace_binary = manifest_dir
                .parent()
                .and_then(|p| p.parent())
                .map(|p| p.join("target").join("debug").join("market-headless"));
            if let Some(ref wb) = workspace_binary {
                if !wb.exists() {
                    return Err(MarketError::Config(format!(
                        "market-headless binary not found at {} or {}. \
                         Build it first: cargo build --bin market-headless",
                        binary.display(),
                        wb.display()
                    )));
                }
            }
        }

        // Determine which binary path exists
        let binary_path = if binary.exists() {
            binary
        } else {
            manifest_dir
                .parent()
                .and_then(|p| p.parent())
                .map(|p| p.join("target").join("debug").join("market-headless"))
                .unwrap_or(binary)
        };

        eprintln!(
            "[E2E] Spawning headless node (offset={}) from {}",
            offset,
            binary_path.display()
        );

        let mut child = Command::new(&binary_path)
            .arg("--offset")
            .arg(offset.to_string())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit()) // pass through logs
            .spawn()
            .map_err(|e| {
                MarketError::Process(format!(
                    "Failed to spawn market-headless (offset {}): {}",
                    offset, e
                ))
            })?;

        let child_stdin = child
            .stdin
            .take()
            .ok_or_else(|| MarketError::Process("No stdin on child".into()))?;
        let child_stdout = child
            .stdout
            .take()
            .ok_or_else(|| MarketError::Process("No stdout on child".into()))?;

        let stdin = BufWriter::new(child_stdin);
        let mut stdout = BufReader::new(child_stdout).lines();

        // Wait for Ready event (with timeout)
        let ready_line = tokio::time::timeout(Duration::from_secs(300), stdout.next_line())
            .await
            .map_err(|_| {
                MarketError::Timeout(format!(
                    "Headless node {} did not emit Ready within 300s",
                    offset
                ))
            })?
            .map_err(|e| MarketError::Process(format!("Failed to read Ready line: {}", e)))?
            .ok_or_else(|| {
                MarketError::Process(format!(
                    "Headless node {} stdout closed before Ready",
                    offset
                ))
            })?;

        let ready: serde_json::Value = serde_json::from_str(&ready_line).map_err(|e| {
            MarketError::Process(format!("Invalid Ready JSON '{}': {}", ready_line, e))
        })?;

        let node_id = ready["node_id"]
            .as_str()
            .ok_or_else(|| MarketError::Process("Ready event missing node_id".into()))?
            .to_string();
        let signing_pubkey = ready["signing_pubkey"]
            .as_str()
            .ok_or_else(|| MarketError::Process("Ready event missing signing_pubkey".into()))?
            .to_string();

        eprintln!(
            "[E2E] Headless node {} ready: node_id={}...",
            offset,
            &node_id[..16.min(node_id.len())]
        );

        Ok(Self {
            child,
            stdin,
            stdout,
            node_id,
            signing_pubkey,
            offset,
        })
    }

    /// Send a command and read the response.
    async fn send(&mut self, cmd: &serde_json::Value) -> MarketResult<serde_json::Value> {
        use tokio::io::AsyncWriteExt;

        let line = serde_json::to_string(cmd).expect("JSON serialization failed");
        self.stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|e| MarketError::Process(format!("stdin write failed: {}", e)))?;
        self.stdin
            .write_all(b"\n")
            .await
            .map_err(|e| MarketError::Process(format!("stdin newline failed: {}", e)))?;
        self.stdin
            .flush()
            .await
            .map_err(|e| MarketError::Process(format!("stdin flush failed: {}", e)))?;

        let resp_line = tokio::time::timeout(
            Duration::from_secs(600), // actions can take a while (DHT ops)
            self.stdout.next_line(),
        )
        .await
        .map_err(|_| MarketError::Timeout("Command response timed out (600s)".into()))?
        .map_err(|e| MarketError::Process(format!("stdout read error: {}", e)))?
        .ok_or_else(|| MarketError::Process("stdout closed unexpectedly".into()))?;

        let resp: serde_json::Value = serde_json::from_str(&resp_line)
            .map_err(|e| MarketError::Process(format!("Invalid response JSON: {}", e)))?;

        if resp["status"] == "Err" {
            return Err(MarketError::Process(
                resp["message"].as_str().unwrap_or("unknown error").into(),
            ));
        }

        Ok(resp)
    }

    /// Create a listing. Returns (listing_key, decryption_key).
    pub async fn create_listing(
        &mut self,
        title: &str,
        content: &str,
        reserve_price: u64,
        duration_secs: u64,
    ) -> MarketResult<(String, String)> {
        let resp = self
            .send(&serde_json::json!({
                "cmd": "CreateListing",
                "title": title,
                "content": content,
                "reserve_price": reserve_price,
                "duration_secs": duration_secs,
            }))
            .await?;

        let data = &resp["data"];
        let listing_key = data["listing_key"]
            .as_str()
            .ok_or_else(|| MarketError::Process("Missing listing_key in response".into()))?
            .to_string();
        let decryption_key = data["decryption_key"]
            .as_str()
            .ok_or_else(|| MarketError::Process("Missing decryption_key in response".into()))?
            .to_string();

        Ok((listing_key, decryption_key))
    }

    /// Place a bid. Returns the bid_key.
    pub async fn place_bid(
        &mut self,
        listing_key: &str,
        amount: u64,
    ) -> MarketResult<Option<String>> {
        let resp = self
            .send(&serde_json::json!({
                "cmd": "PlaceBid",
                "listing_key": listing_key,
                "amount": amount,
            }))
            .await?;

        let bid_key = resp["data"]["bid_key"].as_str().map(String::from);
        Ok(bid_key)
    }

    /// Poll for the decryption key (returns None if not yet available).
    pub async fn get_decryption_key(&mut self, listing_key: &str) -> MarketResult<Option<String>> {
        let resp = self
            .send(&serde_json::json!({
                "cmd": "GetDecryptionKey",
                "listing_key": listing_key,
            }))
            .await?;

        let key = resp["data"]["key"].as_str().map(String::from);
        Ok(key)
    }

    /// Request graceful shutdown.
    pub async fn shutdown(&mut self) -> MarketResult<()> {
        // Send shutdown command (ignore errors if process already died)
        let _ = self.send(&serde_json::json!({ "cmd": "Shutdown" })).await;

        // Wait for process to exit
        let _ = tokio::time::timeout(Duration::from_secs(30), self.child.wait()).await;

        // Force kill if still alive
        let _ = self.child.kill().await;

        Ok(())
    }
}

impl Drop for HeadlessParticipant {
    fn drop(&mut self) {
        // start_kill() is synchronous — sends SIGKILL without awaiting exit.
        // Prevents orphaned market-headless processes when a test panics.
        let _ = self.child.start_kill();
    }
}
