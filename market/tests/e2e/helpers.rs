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

/// Platform-specific ipspoof library name.
fn ipspoof_lib_name() -> &'static str {
    if cfg!(target_os = "macos") {
        "libveilid_ipspoof.dylib"
    } else {
        "libveilid_ipspoof.so"
    }
}

/// Platform-specific preload env var.
fn preload_env_var() -> &'static str {
    if cfg!(target_os = "macos") {
        "DYLD_INSERT_LIBRARIES"
    } else {
        "LD_PRELOAD"
    }
}

/// Path to libipspoof for IP translation in devnet.
pub fn libipspoof_path() -> PathBuf {
    veilid_repo_path().join(format!("target/release/{}", ipspoof_lib_name()))
}

/// Find the `veilid-playground` binary.
fn find_playground_binary() -> Option<PathBuf> {
    let release = veilid_repo_path().join("target/release/veilid-playground");
    if release.exists() {
        return Some(release);
    }
    let debug = veilid_repo_path().join("target/debug/veilid-playground");
    if debug.exists() {
        return Some(debug);
    }
    // Check PATH
    let output = Command::new("which")
        .arg("veilid-playground")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .ok()?;
    if output.status.success() {
        let p = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !p.is_empty() {
            return Some(PathBuf::from(p));
        }
    }
    None
}

/// Check if running in fast mode (persistent devnet).
fn is_fast_mode() -> bool {
    std::env::var("E2E_FAST_MODE").is_ok()
}

const PLAYGROUND_DATA_DIR: &str = "/tmp/veilid-playground";
const DEVNET_NODE_COUNT: u16 = 20;

// ── DevnetManager ────────────────────────────────────────────────────

/// Manages the Veilid devnet lifecycle for E2E tests.
/// Uses `veilid-playground` to launch devnet nodes as local processes.
pub struct DevnetManager {
    child: Option<std::process::Child>,
    started: bool,
    fast_mode: bool,
    /// True if we started the devnet ourselves (vs found it pre-existing).
    /// Only tear down on Drop if we own it.
    owns_devnet: bool,
}

impl DevnetManager {
    pub fn new() -> Self {
        let fast_mode = is_fast_mode();
        if fast_mode {
            eprintln!("[E2E] Fast mode enabled - reusing persistent devnet");
        }
        Self {
            child: None,
            started: false,
            fast_mode,
            owns_devnet: false,
        }
    }

    pub fn infrastructure_available(&self) -> bool {
        find_playground_binary().is_some() && libipspoof_path().exists()
    }

    fn kill_existing_processes() {
        let _ = Command::new("pkill")
            .args(["-f", "veilid-playground.*start"])
            .status();
        let _ = Command::new("pkill")
            .args(["-f", "veilid-server.*subnode-index"])
            .status();
    }

    fn ensure_stopped(&self) -> MarketResult<()> {
        eprintln!("[E2E] Ensuring devnet is stopped before starting...");
        Self::kill_existing_processes();
        // Give processes time to exit cleanly
        std::thread::sleep(Duration::from_secs(2));
        // Clean data dir for fresh routing tables
        let data_dir = PathBuf::from(PLAYGROUND_DATA_DIR);
        if data_dir.exists() {
            let _ = std::fs::remove_dir_all(&data_dir);
        }
        Ok(())
    }

    pub fn start(&mut self) -> MarketResult<()> {
        if !self.infrastructure_available() {
            let binary_status = if find_playground_binary().is_some() {
                "found"
            } else {
                "NOT found"
            };
            let ipspoof_status = if libipspoof_path().exists() {
                "found"
            } else {
                "NOT found"
            };
            return Err(MarketError::Config(format!(
                "Devnet infrastructure not found.\n  \
                 - veilid-playground: {binary_status}\n  \
                 - libipspoof: {ipspoof_status} (at {})",
                libipspoof_path().display()
            )));
        }

        if self.fast_mode {
            eprintln!("[E2E] Fast mode: assuming external devnet is running");
            return Ok(());
        }

        // Always restart the devnet for a fresh routing table.  Reusing a
        // devnet from a previous test leaves stale market-node entries in
        // the routing tables, causing route delivery failures and timeouts.
        self.ensure_stopped()?;
        eprintln!("[E2E] Starting devnet via veilid-playground...");

        let binary = find_playground_binary().expect("already checked");
        let ipspoof = libipspoof_path();

        let child = Command::new(&binary)
            .args([
                "start",
                &DEVNET_NODE_COUNT.to_string(),
                "--ipspoof",
                ipspoof.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| {
                MarketError::Process(format!("Failed to start veilid-playground: {}", e))
            })?;

        eprintln!("[E2E] Playground started (pid={})", child.id());
        self.child = Some(child);
        self.started = true;
        self.owns_devnet = true;
        Ok(())
    }

    pub fn wait_for_health(&self, timeout_secs: u64) -> MarketResult<()> {
        eprintln!(
            "[E2E] Waiting for {} devnet nodes to become reachable...",
            DEVNET_NODE_COUNT
        );
        let start = std::time::Instant::now();
        loop {
            if self.check_all_nodes_reachable() {
                eprintln!("[E2E] All devnet nodes reachable!");
                return Ok(());
            }
            if start.elapsed().as_secs() > timeout_secs {
                return Err(MarketError::Timeout(format!(
                    "Devnet not reachable within {} seconds",
                    timeout_secs
                )));
            }
            std::thread::sleep(Duration::from_secs(2));
        }
    }

    fn check_all_nodes_reachable(&self) -> bool {
        for port in 5150..5150 + DEVNET_NODE_COUNT {
            let addr = format!("127.0.0.1:{}", port);
            if std::net::TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_secs(1))
                .is_err()
            {
                return false;
            }
        }
        true
    }

    pub fn stop(&mut self) -> MarketResult<()> {
        if self.fast_mode || !self.owns_devnet {
            eprintln!(
                "[E2E] Keeping devnet running (fast_mode={}, owns={})",
                self.fast_mode, self.owns_devnet
            );
            return Ok(());
        }
        if !self.started {
            return Ok(());
        }
        eprintln!("[E2E] Stopping devnet...");

        // Send SIGTERM to the playground process (it cleans up children)
        if let Some(ref child) = self.child {
            let pid = child.id().to_string();
            let _ = Command::new("kill").args(["-TERM", &pid]).status();
        }

        // Wait for graceful shutdown, then force-kill stragglers
        std::thread::sleep(Duration::from_secs(3));
        Self::kill_existing_processes();

        // Reap child process
        if let Some(ref mut child) = self.child {
            let _ = child.wait();
        }

        // Clean up data dir
        let _ = std::fs::remove_dir_all(PLAYGROUND_DATA_DIR);

        eprintln!("[E2E] Devnet stopped and cleaned up");
        self.started = false;
        Ok(())
    }
}

impl Drop for DevnetManager {
    fn drop(&mut self) {
        if !self.fast_mode && self.owns_devnet {
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

        let mut market_config = market::config::MarketConfig::default();
        market_config.insecure_storage = true;
        let mut devnet_config = DevNetConfig::from_market_config(&market_config);
        devnet_config.port_offset = offset;
        let node = VeilidNode::new(data_dir.clone(), &market_config).with_devnet(devnet_config);

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
                 Ensure {} is set and devnet is running.",
                self.offset,
                e,
                preload_env_var()
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

/// Setup helper — starts devnet and validates ipspoof preload prerequisites.
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

    let preload_var = preload_env_var();
    let preload = std::env::var(preload_var).unwrap_or_default();
    let expected_preload = libipspoof_path();

    if !expected_preload.exists() {
        return Err(MarketError::Config(format!(
            "{} not found at {}.\n\
             Please build it with: cargo build -p ipspoof --release",
            ipspoof_lib_name(),
            expected_preload.display(),
        )));
    }

    if !preload.contains(expected_preload.to_str().unwrap()) {
        return Err(MarketError::Config(format!(
            "{preload_var} not set. E2E tests require IP spoofing.\n\
             Run with: {preload_var}={} cargo nextest run --profile e2e --ignored",
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
    devnet.wait_for_health(90)?;
    Ok(devnet)
}

/// Helper to print the full error chain for debugging.
pub fn print_error_chain(e: &MarketError) {
    eprintln!("   Error: {}", e);
    eprintln!("   Debug: {:?}", e);
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

        // Locate the binary relative to CARGO_MANIFEST_DIR.
        // Prefer release builds (much faster MPC execution).
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let binary_path = {
            let candidates = [
                manifest_dir.join("target/release/market-headless"),
                manifest_dir
                    .parent()
                    .and_then(|p| p.parent())
                    .map(|p| p.join("target/release/market-headless"))
                    .unwrap_or_default(),
                manifest_dir.join("target/debug/market-headless"),
                manifest_dir
                    .parent()
                    .and_then(|p| p.parent())
                    .map(|p| p.join("target/debug/market-headless"))
                    .unwrap_or_default(),
            ];
            candidates
                .iter()
                .find(|p| p.exists())
                .cloned()
                .ok_or_else(|| {
                    MarketError::Config(format!(
                        "market-headless binary not found. Build with: \
                         cargo build --release --bin market-headless\n\
                         Searched: {:?}",
                        candidates
                            .iter()
                            .map(|p| p.display().to_string())
                            .collect::<Vec<_>>()
                    ))
                })?
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

    /// Wait until this node's broadcast route is ready.
    ///
    /// Must be called before creating listings or placing bids, otherwise
    /// bid announcements silently fail (no peer routes to deliver to).
    pub async fn wait_for_routes(&mut self, timeout_secs: u64) -> MarketResult<()> {
        let resp = self
            .send(&serde_json::json!({
                "cmd": "WaitForRoutes",
                "timeout_secs": timeout_secs,
            }))
            .await?;
        if resp.get("status").and_then(|s| s.as_str()) == Some("Err") {
            let msg = resp["message"].as_str().unwrap_or("route wait failed");
            return Err(MarketError::Network(msg.to_string()));
        }
        Ok(())
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
