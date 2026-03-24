//! Benchmark binary for Veilid MPC auctions.
//!
//! Replaces the fragile bash `run_veilid_bench.sh` by reusing the same
//! `market-headless` process-per-node pattern that the E2E tests use.
//!
//! Usage:
//!   BENCH_PARTIES=3 BENCH_ITERS=3 MP_SPDZ_DIR=../../MP-SPDZ \
//!     cargo run --release --bin bench-auction

use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};

// ── Configuration ────────────────────────────────────────────────────

struct BenchConfig {
    party_counts: Vec<usize>,
    devnet_sizes: Vec<u32>,
    iters: usize,
    auction_duration_secs: u64,
    out_path: PathBuf,
    skip_devnet_restart: bool,
    devnet_mode: DevnetMode,
    warmup_secs: u64,
}

#[derive(Clone, Copy, PartialEq)]
enum DevnetMode {
    Docker,
    Playground,
}

impl DevnetMode {
    fn from_env() -> Self {
        match std::env::var("BENCH_DEVNET_MODE").as_deref() {
            Ok("playground") => Self::Playground,
            _ => Self::Docker,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Docker => "docker",
            Self::Playground => "playground",
        }
    }
}

impl BenchConfig {
    fn from_env() -> Self {
        Self {
            party_counts: env_list("BENCH_PARTIES", vec![3, 4, 5, 6, 8, 10]),
            devnet_sizes: env_list("BENCH_DEVNET_SIZES", vec![20]),
            iters: env_or("BENCH_ITERS", 3),
            auction_duration_secs: env_or("BENCH_AUCTION_DURATION", 30),
            out_path: PathBuf::from(
                std::env::var("BENCH_OUT")
                    .unwrap_or_else(|_| "bench-results/veilid_auction.csv".into()),
            ),
            skip_devnet_restart: std::env::var("BENCH_SKIP_DEVNET_RESTART").is_ok(),
            devnet_mode: DevnetMode::from_env(),
            warmup_secs: env_or("BENCH_WARMUP_SECS", 30),
        }
    }
}

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_list<T: std::str::FromStr>(key: &str, default: Vec<T>) -> Vec<T> {
    std::env::var(key)
        .ok()
        .map(|v| {
            v.split_whitespace()
                .filter_map(|s| s.parse().ok())
                .collect::<Vec<T>>()
        })
        .filter(|v| !v.is_empty())
        .unwrap_or(default)
}

// ── HeadlessParticipant ──────────────────────────────────────────────
// Copied from tests/e2e/helpers.rs with stderr piped (not inherited)
// so we can capture BENCH: lines.

struct HeadlessParticipant {
    child: tokio::process::Child,
    stdin: BufWriter<tokio::process::ChildStdin>,
    stdout: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    stderr_lines: Arc<Mutex<Vec<String>>>,
    offset: u16,
}

impl HeadlessParticipant {
    async fn spawn(offset: u16) -> anyhow::Result<Self> {
        let binary_path = find_headless_binary()?;

        eprintln!(
            "[bench] Spawning headless node (offset={offset}) from {}",
            binary_path.display()
        );

        let mut child = tokio::process::Command::new(&binary_path)
            .arg("--offset")
            .arg(offset.to_string())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

        let child_stdin = child.stdin.take().expect("stdin should be piped");
        let child_stdout = child.stdout.take().expect("stdout should be piped");
        let child_stderr = child.stderr.take().expect("stderr should be piped");

        let stdin = BufWriter::new(child_stdin);
        let mut stdout = BufReader::new(child_stdout).lines();

        // Drain stderr in a background task — capture lines AND forward to our stderr.
        // CRITICAL: must be spawned BEFORE waiting for Ready, otherwise the child's
        // stderr buffer fills and blocks stdout writes.
        let stderr_lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let lines_clone = stderr_lines.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(child_stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                eprintln!("[node-{offset}] {line}");
                lines_clone.lock().unwrap().push(line);
            }
        });

        // Wait for Ready event
        let ready_line = tokio::time::timeout(Duration::from_secs(300), stdout.next_line())
            .await
            .map_err(|_| anyhow::anyhow!("Headless node {offset} did not emit Ready within 300s"))?
            .map_err(|e| anyhow::anyhow!("Failed to read Ready line: {e}"))?
            .ok_or_else(|| anyhow::anyhow!("Headless node {offset} stdout closed before Ready"))?;

        let ready: serde_json::Value = serde_json::from_str(&ready_line)?;
        let node_id = ready["node_id"].as_str().unwrap_or("unknown");
        eprintln!(
            "[bench] Node {offset} ready: node_id={}...",
            &node_id[..16.min(node_id.len())]
        );

        Ok(Self {
            child,
            stdin,
            stdout,
            stderr_lines,
            offset,
        })
    }

    async fn send(&mut self, cmd: &serde_json::Value) -> anyhow::Result<serde_json::Value> {
        let line = serde_json::to_string(cmd)?;
        self.stdin.write_all(line.as_bytes()).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await?;

        let resp_line = tokio::time::timeout(Duration::from_secs(600), self.stdout.next_line())
            .await
            .map_err(|_| anyhow::anyhow!("Command response timed out (600s)"))?
            .map_err(|e| anyhow::anyhow!("stdout read error: {e}"))?
            .ok_or_else(|| anyhow::anyhow!("stdout closed unexpectedly"))?;

        let resp: serde_json::Value = serde_json::from_str(&resp_line)?;
        if resp["status"] == "Err" {
            anyhow::bail!("{}", resp["message"].as_str().unwrap_or("unknown error"));
        }
        Ok(resp)
    }

    async fn wait_for_routes(&mut self, timeout_secs: u64) -> anyhow::Result<()> {
        let resp = self
            .send(&serde_json::json!({
                "cmd": "WaitForRoutes",
                "timeout_secs": timeout_secs,
            }))
            .await?;
        if resp.get("status").and_then(|s| s.as_str()) == Some("Err") {
            anyhow::bail!(
                "{}",
                resp["message"].as_str().unwrap_or("route wait failed")
            );
        }
        Ok(())
    }

    async fn create_listing(
        &mut self,
        title: &str,
        content: &str,
        reserve_price: u64,
        duration_secs: u64,
    ) -> anyhow::Result<(String, String)> {
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
            .ok_or_else(|| anyhow::anyhow!("Missing listing_key"))?
            .to_string();
        let decryption_key = data["decryption_key"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing decryption_key"))?
            .to_string();
        Ok((listing_key, decryption_key))
    }

    async fn place_bid(
        &mut self,
        listing_key: &str,
        amount: u64,
    ) -> anyhow::Result<Option<String>> {
        let resp = self
            .send(&serde_json::json!({
                "cmd": "PlaceBid",
                "listing_key": listing_key,
                "amount": amount,
            }))
            .await?;
        Ok(resp["data"]["bid_key"].as_str().map(String::from))
    }

    async fn get_decryption_key(&mut self, listing_key: &str) -> anyhow::Result<Option<String>> {
        let resp = self
            .send(&serde_json::json!({
                "cmd": "GetDecryptionKey",
                "listing_key": listing_key,
            }))
            .await?;
        Ok(resp["data"]["key"].as_str().map(String::from))
    }

    async fn shutdown(&mut self) {
        let _ = self.send(&serde_json::json!({ "cmd": "Shutdown" })).await;
        let _ = tokio::time::timeout(Duration::from_secs(30), self.child.wait()).await;
        let _ = self.child.kill().await;
    }

    fn take_stderr_lines(&self) -> Vec<String> {
        std::mem::take(&mut *self.stderr_lines.lock().unwrap())
    }
}

impl Drop for HeadlessParticipant {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

// ── Binary resolution ────────────────────────────────────────────────

fn find_headless_binary() -> anyhow::Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
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
            anyhow::anyhow!(
                "market-headless binary not found. Build with: cargo build --release --bin market-headless\nSearched: {:?}",
                candidates.iter().map(|p| p.display().to_string()).collect::<Vec<_>>()
            )
        })
}

// ── Helpers: bid retry + decryption key polling ──────────────────────

async fn place_bid_with_retry(
    node: &mut HeadlessParticipant,
    listing_key: &str,
    amount: u64,
    timeout_secs: u64,
) -> anyhow::Result<Option<String>> {
    let start = Instant::now();
    let max_wait = Duration::from_secs(timeout_secs);
    loop {
        match node.place_bid(listing_key, amount).await {
            Ok(key) => return Ok(key),
            Err(e) => {
                if start.elapsed() > max_wait {
                    return Err(e);
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn poll_decryption_key(
    node: &mut HeadlessParticipant,
    listing_key: &str,
    timeout_secs: u64,
) -> Option<String> {
    let start = Instant::now();
    let max_wait = Duration::from_secs(timeout_secs);
    loop {
        match node.get_decryption_key(listing_key).await {
            Ok(Some(key)) => return Some(key),
            Ok(None) => {}
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("Broken pipe") || msg.contains("stdout closed") {
                    eprintln!("[bench] GetDecryptionKey fatal: {e}");
                    return None;
                }
                eprintln!("[bench] GetDecryptionKey error (retrying): {e}");
            }
        }
        if start.elapsed() > max_wait {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ── BENCH: line parsing ──────────────────────────────────────────────

#[derive(Default)]
struct BenchMetrics {
    route_exchange_secs: f64,
    mpc_wall_secs: f64,
    mpc_self_secs: f64,
    data_sent_mb: f64,
    rounds: u64,
    global_data_mb: f64,
    tunnel_bytes_sent: u64,
    tunnel_bytes_recv: u64,
}

fn parse_bench_lines(lines: &[String]) -> BenchMetrics {
    let mut m = BenchMetrics::default();
    for line in lines {
        let Some(json_str) = line.split("BENCH: ").nth(1) else {
            continue;
        };
        // The BENCH: line may have trailing whitespace or tracing metadata; try to parse
        let Ok(v) = serde_json::from_str::<serde_json::Value>(json_str.trim()) else {
            continue;
        };
        match v["event"].as_str() {
            Some("phase") if v["name"] == "route_exchange" => {
                m.route_exchange_secs = v["secs"].as_f64().unwrap_or(0.0);
            }
            Some("mpc_complete") => {
                m.mpc_wall_secs = v["mpc_wall_secs"].as_f64().unwrap_or(0.0);
                m.mpc_self_secs = v["mpc_self_secs"].as_f64().unwrap_or(0.0);
                m.data_sent_mb = v["data_sent_mb"].as_f64().unwrap_or(0.0);
                m.rounds = v["rounds"].as_u64().unwrap_or(0);
                m.global_data_mb = v["global_data_mb"].as_f64().unwrap_or(0.0);
                m.tunnel_bytes_sent = v["tunnel_bytes_sent"].as_u64().unwrap_or(0);
                m.tunnel_bytes_recv = v["tunnel_bytes_recv"].as_u64().unwrap_or(0);
            }
            Some("auction_complete") => {
                // We use our own wall clock for total_secs, but could cross-check here
            }
            _ => {}
        }
    }
    m
}

// ── CSV output ───────────────────────────────────────────────────────

const CSV_HEADER: &str = "timestamp,protocol,devnet_nodes,num_parties,iteration,total_secs,route_exchange_secs,mpc_wall_secs,mpc_self_secs,data_sent_mb,rounds,global_data_mb,tunnel_bytes_sent,tunnel_bytes_recv";

fn append_csv_row(
    path: &PathBuf,
    protocol: &str,
    devnet_nodes: u32,
    parties: usize,
    iteration: usize,
    total_secs: f64,
    m: &BenchMetrics,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let needs_header = !path.exists()
        || std::fs::metadata(path)
            .map(|m| m.len() == 0)
            .unwrap_or(true);

    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;

    if needs_header {
        writeln!(f, "{CSV_HEADER}")?;
    }

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    writeln!(
        f,
        "{ts},{protocol},{},{},{iteration},{total_secs:.2},{:.2},{:.2},{:.2},{:.3},{},{:.3},{},{}",
        devnet_nodes,
        parties,
        m.route_exchange_secs,
        m.mpc_wall_secs,
        m.mpc_self_secs,
        m.data_sent_mb,
        m.rounds,
        m.global_data_mb,
        m.tunnel_bytes_sent,
        m.tunnel_bytes_recv,
    )?;

    Ok(())
}

// ── Devnet management ────────────────────────────────────────────────

fn compose_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("veilid/.devcontainer/compose/docker-compose.dev.yml")
}

fn devnet_restart(desired_nodes: u32, warmup_secs: u64) -> anyhow::Result<()> {
    let compose = compose_path();
    let compose_str = compose.display().to_string();

    eprintln!("[bench] Restarting devnet (desired size: {desired_nodes})...");

    // Clean data dirs
    for offset in 0..100 {
        let dir = format!("/tmp/market-headless-{offset}");
        let _ = std::fs::remove_dir_all(&dir);
    }
    // Also clean persistent data
    if let Some(home) = dirs::home_dir() {
        for entry in std::fs::read_dir(home.join(".local/share"))
            .into_iter()
            .flatten()
            .flatten()
        {
            if entry
                .file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("smpc-auction-node-"))
            {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }

    let status = Command::new("docker")
        .args(["compose", "-f", &compose_str, "down", "-v"])
        .status()?;
    if !status.success() {
        anyhow::bail!("docker compose down failed");
    }

    let status = Command::new("docker")
        .args(["compose", "-f", &compose_str, "up", "-d"])
        .status()?;
    if !status.success() {
        anyhow::bail!("docker compose up failed");
    }

    // Wait for healthy
    eprintln!("[bench] Waiting for devnet to be healthy...");
    let start = Instant::now();
    loop {
        let output = Command::new("docker")
            .args(["compose", "-f", &compose_str, "ps"])
            .output()?;
        let ps_output = String::from_utf8_lossy(&output.stdout);
        if ps_output.contains("healthy") {
            break;
        }
        if start.elapsed() > Duration::from_secs(60) {
            anyhow::bail!("Devnet did not become healthy within 60s");
        }
        std::thread::sleep(Duration::from_secs(2));
    }

    // Stop nodes beyond the desired count (devnet always starts 20)
    if desired_nodes < 20 {
        eprintln!("[bench] Stopping nodes {desired_nodes}..19 to simulate smaller devnet...");
        for idx in desired_nodes..20 {
            let _ = Command::new("docker")
                .args([
                    "compose",
                    "-f",
                    &compose_str,
                    "stop",
                    &format!("veilid-dev-node-{idx}"),
                ])
                .status();
        }
    }

    eprintln!("[bench] Devnet healthy ({desired_nodes} nodes). Warming up ({warmup_secs}s)...");
    std::thread::sleep(Duration::from_secs(warmup_secs));

    Ok(())
}

// ── Playground devnet management ─────────────────────────────────────

/// Search paths for veilid workspace binaries: submodule (Repos/veilid inside
/// dissertation) and standalone (~/Repos/veilid).
fn find_veilid_binary(name: &str, hint: &str) -> PathBuf {
    let submodule_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("veilid");
    let home_dir = dirs::home_dir()
        .map(|h| h.join("Repos/veilid"))
        .unwrap_or_default();
    // Check ~/Repos/veilid first — it may have patches not in the submodule
    let candidates: Vec<PathBuf> = [&home_dir, &submodule_dir]
        .iter()
        .flat_map(|dir| {
            [
                dir.join(format!("target/release/{name}")),
                dir.join(format!("target/debug/{name}")),
            ]
        })
        .collect();
    candidates
        .iter()
        .find(|p| p.exists())
        .cloned()
        .unwrap_or_else(|| {
            eprintln!(
                "[bench] {name} not found. {hint}\nSearched: {:?}",
                candidates.iter().map(|p| p.display().to_string()).collect::<Vec<_>>()
            );
            std::process::exit(1);
        })
}

fn playground_binary_path() -> PathBuf {
    find_veilid_binary(
        "veilid-playground",
        "Build with: cd Repos/veilid && cargo build --release -p veilid-playground",
    )
}

fn playground_ipspoof_path() -> PathBuf {
    find_veilid_binary(
        "libveilid_ipspoof.so",
        "Build with: cd Repos/veilid && cargo build --release -p veilid-ipspoof",
    )
}

fn playground_veilid_server_path() -> PathBuf {
    find_veilid_binary(
        "veilid-server",
        "Build with: cd Repos/veilid && cargo build --release -p veilid-server",
    )
}

const PLAYGROUND_DATA_DIR: &str = "/tmp/veilid-playground-bench";
const PLAYGROUND_BASE_PORT: u16 = 5150;

/// Start playground devnet with the given number of nodes.
/// Returns the child process handle — caller must kill it when done.
fn playground_restart(desired_nodes: u32, warmup_secs: u64) -> anyhow::Result<std::process::Child> {
    let playground = playground_binary_path();
    let veilid_server = playground_veilid_server_path();
    let ipspoof = playground_ipspoof_path();

    eprintln!("[bench] Restarting playground devnet (desired size: {desired_nodes})...");

    // Kill any existing playground / veilid-server processes
    let _ = Command::new("pkill").args(["-9", "-f", "veilid-server"]).status();
    std::thread::sleep(Duration::from_millis(500));

    // Clean data dirs
    let _ = std::fs::remove_dir_all(PLAYGROUND_DATA_DIR);
    for offset in 0..60 {
        let _ = std::fs::remove_dir_all(format!("/tmp/market-headless-{offset}"));
    }
    if let Some(home) = dirs::home_dir() {
        for entry in std::fs::read_dir(home.join(".local/share"))
            .into_iter()
            .flatten()
            .flatten()
        {
            if entry
                .file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("smpc-auction-node-"))
            {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }

    // Start playground
    let child = Command::new(&playground)
        .args([
            "start",
            &desired_nodes.to_string(),
            "--base-port",
            &PLAYGROUND_BASE_PORT.to_string(),
            "--data-dir",
            PLAYGROUND_DATA_DIR,
            "--veilid-server",
            &veilid_server.display().to_string(),
            "--ipspoof",
            &ipspoof.display().to_string(),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .spawn()?;

    // Wait for all node ports to be connectable
    eprintln!("[bench] Waiting for {desired_nodes} playground nodes to be healthy...");
    let start = Instant::now();
    for i in 0..desired_nodes {
        let port = PLAYGROUND_BASE_PORT + i as u16;
        loop {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break;
            }
            if start.elapsed() > Duration::from_secs(60) {
                anyhow::bail!("Playground node {i} (port {port}) did not become healthy within 60s");
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    }

    eprintln!("[bench] Playground healthy ({desired_nodes} nodes). Warming up ({warmup_secs}s)...");
    std::thread::sleep(Duration::from_secs(warmup_secs));

    Ok(child)
}

// ── Main ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = BenchConfig::from_env();
    let mpc_protocol = std::env::var("BENCH_MPC_PROTOCOL").unwrap_or_else(|_| "mascot".into());

    eprintln!("[bench] Veilid MPC Auction Benchmark");
    eprintln!(
        "[bench] party_counts={:?}, devnet_sizes={:?}, iters={}, protocol={}, mode={}, warmup={}s, auction_duration={}s, out={}",
        cfg.party_counts,
        cfg.devnet_sizes,
        cfg.iters,
        mpc_protocol,
        cfg.devnet_mode.label(),
        cfg.warmup_secs,
        cfg.auction_duration_secs,
        cfg.out_path.display()
    );

    for &party_count in &cfg.party_counts {
        if party_count < 3 {
            eprintln!("[bench] Skipping N={party_count} (need >= 3: 1 seller + 2+ bidders)");
            continue;
        }
    }

    // Track playground child process so we can kill/restart it
    let mut playground_child: Option<std::process::Child> = None;

    for &devnet_size in &cfg.devnet_sizes {
        for &parties in &cfg.party_counts {
            if parties < 3 {
                continue;
            }

            for iter in 0..cfg.iters {
                // Restart devnet before every iteration for clean routing state
                if !cfg.skip_devnet_restart {
                    match cfg.devnet_mode {
                        DevnetMode::Docker => {
                            devnet_restart(devnet_size, cfg.warmup_secs)?;
                        }
                        DevnetMode::Playground => {
                            // Kill previous playground if running
                            if let Some(ref mut child) = playground_child {
                                let _ = child.kill();
                                let _ = child.wait();
                            }
                            playground_child =
                                Some(playground_restart(devnet_size, cfg.warmup_secs)?);
                        }
                    }
                }

                eprintln!(
                    "\n[bench] ===== devnet={devnet_size} N={parties} protocol={mpc_protocol} iter {}/{} =====",
                    iter + 1,
                    cfg.iters
                );

                let result = run_auction_iteration(&cfg, devnet_size, parties, iter, &mpc_protocol).await;

                match result {
                    Ok(()) => {}
                    Err(e) => {
                        eprintln!("[bench] Iteration FAILED: {e}");
                        eprintln!("[bench] Continuing to next iteration...");
                    }
                }
            }
        }
    }

    // Clean up playground if running
    if let Some(ref mut child) = playground_child {
        eprintln!("[bench] Stopping playground...");
        let _ = child.kill();
        let _ = child.wait();
    }

    eprintln!(
        "\n[bench] Done. Results written to {}",
        cfg.out_path.display()
    );
    Ok(())
}

async fn run_auction_iteration(
    cfg: &BenchConfig,
    devnet_nodes: u32,
    parties: usize,
    iter: usize,
    mpc_protocol: &str,
) -> anyhow::Result<()> {
    // Kill orphaned MPC processes from previous iterations
    let _ = Command::new("pkill").args(["-9", "-f", "mascot-party.x"]).status();
    let _ = Command::new("pkill").args(["-9", "-f", "shamir-party.x"]).status();
    let _ = Command::new("pkill").args(["-9", "-f", "replicated-ring-party.x"]).status();
    let _ = Command::new("pkill").args(["-9", "-f", "market-headless"]).status();
    // Wait for processes to fully exit and release ports
    std::thread::sleep(Duration::from_secs(2));

    // Market nodes start after devnet nodes to avoid port/IP collisions
    let base_offset = devnet_nodes as u16;

    // Clean data dirs for this iteration's node offsets
    for offset in base_offset..(base_offset + parties as u16) {
        let _ = std::fs::remove_dir_all(format!("/tmp/market-headless-{offset}"));
    }

    // Spawn all participants concurrently
    let offsets: Vec<u16> = (base_offset..base_offset + parties as u16).collect();
    let mut participants = Vec::with_capacity(parties);
    let mut spawn_futs = Vec::with_capacity(parties);
    for &offset in &offsets {
        spawn_futs.push(HeadlessParticipant::spawn(offset));
    }
    let results = futures::future::join_all(spawn_futs).await;
    for result in results {
        participants.push(result?);
    }

    // Wait for routes
    eprintln!("[bench] Waiting for routes...");
    for p in &mut participants {
        p.wait_for_routes(30).await?;
    }
    eprintln!("[bench] All routes ready");

    let wall_start = Instant::now();

    // Seller creates listing
    eprintln!("[bench] Seller creating listing...");
    let (listing_key, expected_key) = participants[0]
        .create_listing(
            "Bench Item",
            "benchmark content payload",
            100,
            cfg.auction_duration_secs,
        )
        .await?;
    eprintln!(
        "[bench] Listing created: {}...",
        &listing_key[..20.min(listing_key.len())]
    );

    // Bidders place bids with retry
    for (i, bidder) in participants[1..].iter_mut().enumerate() {
        let amount = 200 + i as u64;
        eprintln!("[bench] Bidder {} placing bid ({amount})...", bidder.offset);
        place_bid_with_retry(bidder, &listing_key, amount, 30).await?;
    }

    // The highest bidder is the last one (highest amount)
    let winner_idx = parties - 1; // index into participants[]

    // Poll winner for decryption key
    eprintln!("[bench] Polling for MPC + post-MPC verification (max 600s)...");
    let winner_key =
        poll_decryption_key(&mut participants[winner_idx], &listing_key, 600).await;

    let total_secs = wall_start.elapsed().as_secs_f64();

    if let Some(ref key) = winner_key {
        if key == &expected_key {
            eprintln!("[bench] SUCCESS: Winner got correct decryption key in {total_secs:.1}s");
        } else {
            eprintln!(
                "[bench] WARNING: Winner key mismatch! expected={expected_key}, got={key}"
            );
        }
    } else {
        eprintln!("[bench] FAILURE: Winner did not receive decryption key within 600s");
    }

    // Parse BENCH: lines from seller's stderr
    let seller_lines = participants[0].take_stderr_lines();
    let metrics = parse_bench_lines(&seller_lines);

    // Shutdown all participants
    for p in &mut participants {
        p.shutdown().await;
    }

    // Write CSV row
    append_csv_row(&cfg.out_path, mpc_protocol, devnet_nodes, parties, iter, total_secs, &metrics)?;
    eprintln!(
        "[bench] CSV row written: total={total_secs:.1}s route_exchange={:.1}s mpc_wall={:.1}s mpc_self={:.1}s rounds={}",
        metrics.route_exchange_secs,
        metrics.mpc_wall_secs,
        metrics.mpc_self_secs,
        metrics.rounds,
    );

    Ok(())
}
