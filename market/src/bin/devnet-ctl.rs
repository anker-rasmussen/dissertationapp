//! Devnet control utility for E2E testing
//!
//! Spawns 5 veilid-server processes on localhost with LD_PRELOAD=libipspoof.so
//! for fake-global-IP translation (1.2.3.x ↔ 127.0.0.1).
//!
//! Commands:
//!   start  - Spawn 5 nodes, wait for IPC sockets
//!   stop   - Kill all nodes, clean up
//!   status - Check which nodes are alive

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Instant;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const NODE_COUNT: usize = 5;
const BASE_PORT: u16 = 5160;
const BASE_API_PORT: u16 = 5959;
const DEVNET_DIR: &str = "/tmp/veilid-devnet";
const NETWORK_KEY: &str = "development-network-2025";
const HEALTH_TIMEOUT_SECS: u64 = 30;

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

fn veilid_server_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../veilid/target/debug/veilid-server")
}

fn veilid_cli_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../veilid/target/debug/veilid-cli")
}

fn libipspoof_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../ip-spoof/target/debug/libipspoof.so")
}

fn devnet_dir() -> PathBuf {
    PathBuf::from(DEVNET_DIR)
}

fn peer_info_file_path() -> PathBuf {
    devnet_dir().join("peer_info.json")
}

fn node_dir(i: usize) -> PathBuf {
    devnet_dir().join(format!("node-{i}"))
}

fn state_file() -> PathBuf {
    devnet_dir().join("devnet.json")
}

fn ipc_path(i: usize) -> PathBuf {
    node_dir(i).join("ipc").join("0")
}

// ---------------------------------------------------------------------------
// Config generation
// ---------------------------------------------------------------------------

fn generate_config(i: usize) -> String {
    let port = BASE_PORT + i as u16;
    let api_port = BASE_API_PORT + i as u16;
    let ip_suffix = i + 1;
    let fake_ip = format!("1.2.3.{ip_suffix}");
    let public_addr = format!("'{fake_ip}:{port}'");
    let dir = node_dir(i);
    let ipc_dir = dir.join("ipc");

    let is_bootstrap = i == 0;

    let bootstrap_section = if is_bootstrap {
        "      bootstrap: []\n".to_string()
    } else {
        format!("      bootstrap:\n      - 'udp://1.2.3.1:{BASE_PORT}'\n")
    };

    let capabilities_disable = if is_bootstrap {
        "\n    - TUNL\n    - SGNL\n    - RLAY\n    - DIAL\n    - DHTV\n    - DHTW\n    - APPM\n    - ROUT"
    } else {
        "\n    - APPM"
    };

    let min_peer_count: u32 = 2;

    format!(
        r#"daemon:
  enabled: false
client_api:
  ipc_enabled: true
  ipc_directory: '{ipc}'
  network_enabled: false
  listen_address: 'localhost:{api_port}'
auto_attach: true
logging:
  system:
    enabled: false
    level: debug
    ignore_log_targets: []
  terminal:
    enabled: true
    level: debug
    ignore_log_targets: []
  file:
    enabled: true
    path: '{log}'
    append: true
    level: debug
    ignore_log_targets: []
  api:
    enabled: true
    level: debug
    ignore_log_targets: []
testing:
  subnode_index: 0
core:
  capabilities:
    disable: {capabilities_disable}
  protected_store:
    allow_insecure_fallback: true
    always_use_insecure_storage: true
    directory: '{protected_store}'
    delete: false
  table_store:
    directory: '{table_store}'
    delete: false
  block_store:
    directory: '{block_store}'
    delete: false
  network:
    network_key_password: '{NETWORK_KEY}'
    routing_table:
{bootstrap_section}      bootstrap_keys: []
      limit_over_attached: 64
      limit_fully_attached: 32
      limit_attached_strong: 16
      limit_attached_good: 8
      limit_attached_weak: 4
    dht:
      min_peer_count: {min_peer_count}
    upnp: false
    detect_address_changes: false
    protocol:
      udp:
        enabled: true
        socket_pool_size: 0
        listen_address: ':{port}'
        public_address: {public_addr}
      tcp:
        connect: true
        listen: true
        max_connections: 256
        listen_address: ':{port}'
        public_address: {public_addr}
      ws:
        connect: false
        listen: false
        max_connections: 16
        listen_address: ':{port}'
        path: ws
      wss:
        connect: false
        listen: false
        max_connections: 16
        listen_address: ':{port}'
        path: ws
"#,
        ipc = ipc_dir.display(),
        log = dir.join("veilid-server.log").display(),
        protected_store = dir.join("protected_store").display(),
        table_store = dir.join("table_store").display(),
        block_store = dir.join("block_store").display(),
    )
}

// ---------------------------------------------------------------------------
// State file (JSON with PIDs)
// ---------------------------------------------------------------------------

fn write_state(pids: &[u32]) {
    let entries: Vec<String> = pids
        .iter()
        .enumerate()
        .map(|(i, pid)| format!("    {{\"node\": {i}, \"pid\": {pid}}}"))
        .collect();
    let json = format!("{{\n  \"nodes\": [\n{}\n  ]\n}}\n", entries.join(",\n"));
    std::fs::write(state_file(), json).expect("failed to write state file");
}

fn read_pids() -> Vec<(usize, u32)> {
    let data = match std::fs::read_to_string(state_file()) {
        Ok(d) => d,
        Err(_) => return vec![],
    };
    // Minimal JSON parsing — extract "node": N, "pid": N pairs
    let mut result = Vec::new();
    for line in data.lines() {
        let line = line.trim();
        if line.contains("\"node\"") && line.contains("\"pid\"") {
            let node = extract_json_int(line, "node");
            let pid = extract_json_int(line, "pid");
            if let (Some(n), Some(p)) = (node, pid) {
                result.push((n as usize, p as u32));
            }
        }
    }
    result
}

fn extract_json_int(line: &str, key: &str) -> Option<u64> {
    let pattern = format!("\"{key}\":");
    let start = line.find(&pattern)? + pattern.len();
    let rest = line[start..].trim_start();
    let end = rest.find(|c: char| !c.is_ascii_digit())?;
    rest[..end].parse().ok()
}

// ---------------------------------------------------------------------------
// Process helpers
// ---------------------------------------------------------------------------

fn pid_alive(pid: u32) -> bool {
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

fn kill_pid(pid: u32, sig: i32) {
    unsafe {
        libc::kill(pid as i32, sig);
    }
}

// ---------------------------------------------------------------------------
// Peer info extraction (two-phase bootstrap)
// ---------------------------------------------------------------------------

/// Run veilid-cli with a per-invocation timeout.
/// Returns Ok(stdout) on success, Err on timeout/failure.
fn run_veilid_cli(
    cli: &std::path::Path,
    args: &[&str],
    timeout_secs: u64,
) -> Result<String, String> {
    let mut child = Command::new(cli)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("Failed to spawn veilid-cli: {e}"))?;

    let deadline = Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) => {
                if Instant::now() > deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err("veilid-cli timed out".to_string());
                }
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
            Err(e) => return Err(format!("Failed to wait for veilid-cli: {e}")),
        }
    }

    let output = child
        .wait_with_output()
        .map_err(|e| format!("Failed to read veilid-cli output: {e}"))?;
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Poll `veilid-cli` until the bootstrap node publishes its peer info.
/// Returns the JSON array string (BOOT v0 format: `[{...PeerInfo...}]`).
fn generate_peer_info(ipc: &std::path::Path) -> Result<String, String> {
    let cli = veilid_cli_path();
    let cli = cli
        .canonicalize()
        .map_err(|e| format!("veilid-cli not found at {}: {e}", cli.display()))?;

    let ipc_str = ipc
        .to_str()
        .ok_or_else(|| "IPC path is not valid UTF-8".to_string())?;

    let start = Instant::now();
    let timeout = std::time::Duration::from_secs(HEALTH_TIMEOUT_SECS);

    loop {
        // `jsonpeerinfo` is a debug command that outputs the published PeerInfo
        // as a JSON array in BOOT v0 format: `[{...}]` or `[]` if not yet ready.
        match run_veilid_cli(&cli, &["-p", ipc_str, "-e", "jsonpeerinfo"], 10) {
            Ok(stdout) => {
                let trimmed = stdout.trim();
                // A non-empty JSON array like `[{"routing_domain":...}]` means success.
                // `[]` means not published yet; anything else is an error/not-ready.
                if trimmed.starts_with('[') && trimmed != "[]" && trimmed.contains('{') {
                    return Ok(trimmed.to_string());
                }
            }
            Err(e) => {
                eprintln!("[devnet-ctl] veilid-cli attempt failed: {e}");
            }
        }

        if start.elapsed() > timeout {
            return Err(format!(
                "Timed out after {HEALTH_TIMEOUT_SECS}s waiting for bootstrap peer info"
            ));
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

/// Write the peer info JSON array to the shared peer info file.
fn write_peer_info(peer_info_json: &str) -> Result<(), String> {
    std::fs::write(peer_info_file_path(), peer_info_json)
        .map_err(|e| format!("Failed to write peer info file: {e}"))?;
    eprintln!(
        "[devnet-ctl] Peer info written to {}",
        peer_info_file_path().display()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

fn start_devnet() -> Result<(), String> {
    let veilid = veilid_server_path();
    let veilid = veilid
        .canonicalize()
        .map_err(|e| format!("veilid-server not found at {}: {e}", veilid.display()))?;

    let ipspoof = libipspoof_path();
    let ipspoof = ipspoof
        .canonicalize()
        .map_err(|e| format!("libipspoof.so not found at {}: {e}", ipspoof.display()))?;

    // Stop any existing devnet first
    if state_file().exists() {
        eprintln!("[devnet-ctl] Stopping existing devnet...");
        let _ = stop_devnet();
    }

    // Create base directory
    std::fs::create_dir_all(devnet_dir())
        .map_err(|e| format!("Failed to create {}: {e}", devnet_dir().display()))?;

    // --- Phase 0: Create dirs + configs for all nodes ---
    for i in 0..NODE_COUNT {
        let ndir = node_dir(i);
        for subdir in &["ipc", "protected_store", "table_store", "block_store"] {
            std::fs::create_dir_all(ndir.join(subdir))
                .map_err(|e| format!("mkdir {}: {e}", ndir.join(subdir).display()))?;
        }

        let config_path = ndir.join("veilid-server.conf");
        let config = generate_config(i);
        std::fs::write(&config_path, &config).map_err(|e| format!("write config: {e}"))?;
    }

    let mut pids = Vec::new();
    let peer_info_file = peer_info_file_path();

    // --- Phase 1: Start bootstrap node (node 0) ---
    {
        let i = 0;
        let ndir = node_dir(i);
        let config_path = ndir.join("veilid-server.conf");
        let port = BASE_PORT + i as u16;
        let ip_suffix = i + 1;
        eprintln!("[devnet-ctl] Phase 1: Starting bootstrap node {i} (port {port}, IP 1.2.3.{ip_suffix})...");

        let child = Command::new(&veilid)
            .args(["-c", config_path.to_str().unwrap()])
            .env("LD_PRELOAD", &ipspoof)
            .env("VEILID_BASE_PORT", BASE_PORT.to_string())
            .env("VEILID_PEER_INFO_FILE", &peer_info_file)
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(ndir.join("stderr.log"))
                    .map_err(|e| format!("create stderr log: {e}"))?,
            )
            .spawn()
            .map_err(|e| format!("Failed to spawn veilid-server: {e}"))?;

        pids.push(child.id());
    }

    // Wait for bootstrap IPC socket
    eprintln!("[devnet-ctl] Waiting for bootstrap IPC socket...");
    let start = Instant::now();
    loop {
        if ipc_path(0).exists() {
            break;
        }
        if start.elapsed().as_secs() > HEALTH_TIMEOUT_SECS {
            return Err("Timed out waiting for bootstrap IPC socket".to_string());
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    // Extract peer info from bootstrap node via veilid-cli
    eprintln!("[devnet-ctl] Waiting for bootstrap peer info...");
    let peer_info_json = generate_peer_info(&ipc_path(0))?;
    write_peer_info(&peer_info_json)?;

    // --- Phase 2: Start remaining nodes (1..N) ---
    for i in 1..NODE_COUNT {
        let ndir = node_dir(i);
        let config_path = ndir.join("veilid-server.conf");
        let port = BASE_PORT + i as u16;
        let ip_suffix = i + 1;
        eprintln!("[devnet-ctl] Phase 2: Starting node {i} (port {port}, IP 1.2.3.{ip_suffix})...");

        let child = Command::new(&veilid)
            .args(["-c", config_path.to_str().unwrap()])
            .env("LD_PRELOAD", &ipspoof)
            .env("VEILID_BASE_PORT", BASE_PORT.to_string())
            .env("VEILID_PEER_INFO_FILE", &peer_info_file)
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(ndir.join("stderr.log"))
                    .map_err(|e| format!("create stderr log: {e}"))?,
            )
            .spawn()
            .map_err(|e| format!("Failed to spawn veilid-server: {e}"))?;

        pids.push(child.id());

        // Small delay between spawns to avoid port races
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    write_state(&pids);

    // Wait for all remaining IPC sockets
    eprintln!("[devnet-ctl] Waiting for remaining nodes to start...");
    wait_for_ipc(HEALTH_TIMEOUT_SECS)?;

    eprintln!("[devnet-ctl] Devnet is ready! ({NODE_COUNT} nodes)");
    eprintln!();
    print_status_table(
        &pids
            .iter()
            .enumerate()
            .map(|(i, &p)| (i, p))
            .collect::<Vec<_>>(),
    );
    eprintln!();
    eprintln!("Run tests with: cargo test -- --ignored");
    eprintln!("Stop devnet with: cargo run --bin devnet-ctl -- stop");

    Ok(())
}

fn wait_for_ipc(timeout_secs: u64) -> Result<(), String> {
    let start = Instant::now();

    loop {
        let mut ready = 0;
        for i in 0..NODE_COUNT {
            if ipc_path(i).exists() {
                ready += 1;
            }
        }

        eprintln!("[devnet-ctl] IPC sockets: {ready}/{NODE_COUNT}");

        if ready == NODE_COUNT {
            return Ok(());
        }

        if start.elapsed().as_secs() > timeout_secs {
            return Err(format!(
                "Timed out after {timeout_secs}s — only {ready}/{NODE_COUNT} nodes ready"
            ));
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn stop_devnet() -> Result<(), String> {
    let pids = read_pids();

    if pids.is_empty() {
        eprintln!("[devnet-ctl] No devnet state found");
    } else {
        // SIGTERM
        for &(i, pid) in &pids {
            if pid_alive(pid) {
                eprintln!("[devnet-ctl] Sending SIGTERM to node {i} (PID {pid})");
                kill_pid(pid, libc::SIGTERM);
            }
        }

        // Wait up to 5s for graceful exit
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let alive: Vec<_> = pids.iter().filter(|&&(_, p)| pid_alive(p)).collect();
            if alive.is_empty() {
                break;
            }
            if Instant::now() > deadline {
                // SIGKILL stragglers
                for &&(i, pid) in &alive {
                    eprintln!("[devnet-ctl] Sending SIGKILL to node {i} (PID {pid})");
                    kill_pid(pid, libc::SIGKILL);
                }
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    }

    // Clean up devnet directory
    if devnet_dir().exists() {
        eprintln!("[devnet-ctl] Removing {}", devnet_dir().display());
        let _ = std::fs::remove_dir_all(devnet_dir());
    }

    // Clean up local node data directories
    clean_local_data();

    eprintln!("[devnet-ctl] Devnet stopped");
    Ok(())
}

fn clean_local_data() {
    if let Some(home) = dirs::home_dir() {
        let data_dir = home.join(".local/share");
        if let Ok(entries) = std::fs::read_dir(&data_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("smpc-auction-node-") {
                        let path = entry.path();
                        eprintln!("[devnet-ctl] Removing {}", path.display());
                        let _ = std::fs::remove_dir_all(&path);
                    }
                }
            }
        }
    }
}

fn check_status() -> Result<(), String> {
    let pids = read_pids();

    if pids.is_empty() {
        eprintln!("[devnet-ctl] Devnet is NOT running (no state file)");
        eprintln!();
        eprintln!("Start with: cargo run --bin devnet-ctl -- start");
        return Ok(());
    }

    eprintln!("[devnet-ctl] Devnet status:");
    eprintln!();
    print_status_table(&pids);

    let alive_count = pids.iter().filter(|&&(_, p)| pid_alive(p)).count();
    eprintln!();
    if alive_count == NODE_COUNT {
        eprintln!("[devnet-ctl] All {NODE_COUNT} nodes alive — ready for tests!");
    } else {
        eprintln!("[devnet-ctl] {alive_count}/{NODE_COUNT} nodes alive");
    }

    // Check libipspoof
    let ipspoof = libipspoof_path();
    if ipspoof.exists() {
        if let Ok(canonical) = ipspoof.canonicalize() {
            eprintln!();
            eprintln!("[devnet-ctl] libipspoof.so: {}", canonical.display());
        }
    } else {
        eprintln!();
        eprintln!(
            "[devnet-ctl] WARNING: libipspoof.so not found at {}",
            ipspoof.display()
        );
        eprintln!("Build with: cargo build --manifest-path ../ip-spoof/Cargo.toml");
    }

    Ok(())
}

fn print_status_table(pids: &[(usize, u32)]) {
    eprintln!("NODE  PID     PORT   IP        IPC    STATUS");
    eprintln!("----  ------  -----  --------  -----  ------");
    for &(i, pid) in pids {
        let port = BASE_PORT + i as u16;
        let ip = format!("1.2.3.{}", i + 1);
        let alive = pid_alive(pid);
        let ipc_ok = ipc_path(i).exists();
        eprintln!(
            "{i:<4}  {pid:<6}  {port:<5}  {ip:<8}  {ipc:<5}  {status}",
            ipc = if ipc_ok { "yes" } else { "no" },
            status = if alive { "alive" } else { "DEAD" },
        );
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: devnet-ctl <command>");
        eprintln!();
        eprintln!("Commands:");
        eprintln!("  start   Spawn {NODE_COUNT} veilid-server processes");
        eprintln!("  stop    Kill all nodes and clean up");
        eprintln!("  status  Check node status");
        std::process::exit(1);
    }

    let result = match args[1].as_str() {
        "start" => start_devnet(),
        "stop" => stop_devnet(),
        "status" => check_status(),
        cmd => Err(format!("Unknown command: {cmd}")),
    };

    if let Err(e) = result {
        eprintln!("[devnet-ctl] Error: {e}");
        std::process::exit(1);
    }
}
