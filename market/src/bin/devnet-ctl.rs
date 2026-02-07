//! Devnet control utility for E2E testing
//!
//! Commands:
//!   start  - Start the devnet and wait for health
//!   stop   - Stop the devnet
//!   status - Check devnet status

use std::path::PathBuf;
use std::process::{Command, Stdio};

const VEILID_REPO_PATH: &str = "../../veilid";

fn docker_compose_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(VEILID_REPO_PATH)
        .join(".devcontainer")
        .join("compose")
        .join("docker-compose.dev.yml")
}

fn libipspoof_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(VEILID_REPO_PATH)
        .join(".devcontainer")
        .join("scripts")
        .join("libipspoof.so")
}

fn start_devnet() -> Result<(), String> {
    let compose_path = docker_compose_path();

    if !compose_path.exists() {
        return Err(format!(
            "docker-compose.yml not found at {}",
            compose_path.display()
        ));
    }

    // First stop any existing devnet
    eprintln!("[devnet-ctl] Stopping any existing devnet...");
    let _ = Command::new("docker")
        .args([
            "compose",
            "-f",
            compose_path.to_str().unwrap(),
            "down",
            "-v",
            "--remove-orphans",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    eprintln!("[devnet-ctl] Starting devnet...");
    let output = Command::new("docker")
        .args(["compose", "-f", compose_path.to_str().unwrap(), "up", "-d"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| format!("Failed to run docker compose: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Failed to start devnet: {}", stderr));
    }

    eprintln!("[devnet-ctl] Waiting for devnet to be healthy...");
    wait_for_health(120)?;

    eprintln!("[devnet-ctl] Devnet is ready!");
    eprintln!();
    eprintln!("Run tests with: cargo integration-fast");
    eprintln!("Stop devnet with: cargo devnet-stop");

    Ok(())
}

fn stop_devnet() -> Result<(), String> {
    let compose_path = docker_compose_path();

    eprintln!("[devnet-ctl] Stopping devnet...");
    let output = Command::new("docker")
        .args([
            "compose",
            "-f",
            compose_path.to_str().unwrap(),
            "down",
            "-v",
            "--remove-orphans",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| format!("Failed to run docker compose: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!("[devnet-ctl] Warning: {}", stderr);
    }

    eprintln!("[devnet-ctl] Devnet stopped");

    // Clean up local node data directories
    eprintln!("[devnet-ctl] Cleaning up local node data...");
    if let Some(home) = dirs::home_dir() {
        let data_dir = home.join(".local/share");
        if data_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&data_dir) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    if let Some(name_str) = name.to_str() {
                        if name_str.starts_with("smpc-auction-node-") {
                            let path = entry.path();
                            eprintln!("[devnet-ctl] Removing {}", path.display());
                            let _ = std::fs::remove_dir_all(&path);
                        }
                    }
                }
            }
        }
    }

    // Clean up Docker volumes matching "veilid"
    eprintln!("[devnet-ctl] Cleaning up veilid Docker volumes...");
    let volume_list = Command::new("docker")
        .args(["volume", "ls", "--format", "{{.Name}}"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();

    if let Ok(output) = volume_list {
        let stdout = String::from_utf8_lossy(&output.stdout);
        for volume in stdout.lines() {
            if volume.contains("veilid") {
                eprintln!("[devnet-ctl] Removing volume: {}", volume);
                let _ = Command::new("docker")
                    .args(["volume", "rm", volume])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
        }
    }

    eprintln!("[devnet-ctl] Cleanup complete");
    Ok(())
}

fn check_status() -> Result<(), String> {
    let compose_path = docker_compose_path();

    let output = Command::new("docker")
        .args([
            "compose",
            "-f",
            compose_path.to_str().unwrap(),
            "ps",
            "--format",
            "{{.Service}}\t{{.State}}\t{{.Health}}",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| format!("Failed to run docker compose: {}", e))?;

    let stdout = String::from_utf8_lossy(&output.stdout);

    if stdout.trim().is_empty() {
        eprintln!("[devnet-ctl] Devnet is NOT running");
        eprintln!();
        eprintln!("Start with: cargo devnet-start");
        return Ok(());
    }

    eprintln!("[devnet-ctl] Devnet status:");
    eprintln!();
    eprintln!("SERVICE\t\tSTATE\t\tHEALTH");
    eprintln!("-------\t\t-----\t\t------");
    for line in stdout.lines() {
        eprintln!("{}", line);
    }

    // Count healthy nodes
    let healthy_count = stdout.lines().filter(|l| l.contains("healthy")).count();

    eprintln!();
    if healthy_count == 5 {
        eprintln!("[devnet-ctl] All 5 nodes healthy - ready for tests!");
        eprintln!();
        eprintln!("Run tests with: cargo integration-fast");
    } else {
        eprintln!("[devnet-ctl] {}/5 nodes healthy", healthy_count);
    }

    // Check libipspoof
    let libipspoof = libipspoof_path();
    if libipspoof.exists() {
        eprintln!();
        eprintln!("[devnet-ctl] libipspoof.so: {}", libipspoof.display());
    } else {
        eprintln!();
        eprintln!("[devnet-ctl] WARNING: libipspoof.so not found!");
        eprintln!(
            "Build with: cd {} && make libipspoof.so",
            libipspoof.parent().unwrap().display()
        );
    }

    Ok(())
}

fn wait_for_health(timeout_secs: u64) -> Result<(), String> {
    let compose_path = docker_compose_path();
    let start = std::time::Instant::now();

    loop {
        let output = Command::new("docker")
            .args([
                "compose",
                "-f",
                compose_path.to_str().unwrap(),
                "ps",
                "--format",
                "{{.Service}}\t{{.Health}}",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|e| format!("Failed to check health: {}", e))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let healthy_count = stdout.lines().filter(|l| l.contains("healthy")).count();

        eprintln!(
            "[devnet-ctl] Health check: {}/5 nodes healthy",
            healthy_count
        );

        if healthy_count >= 5 {
            return Ok(());
        }

        if start.elapsed().as_secs() > timeout_secs {
            return Err(format!(
                "Devnet not healthy within {}s ({}/5 nodes)",
                timeout_secs, healthy_count
            ));
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: devnet-ctl <command>");
        eprintln!();
        eprintln!("Commands:");
        eprintln!("  start   Start the devnet and wait for health");
        eprintln!("  stop    Stop the devnet");
        eprintln!("  status  Check devnet status");
        std::process::exit(1);
    }

    let result = match args[1].as_str() {
        "start" => start_devnet(),
        "stop" => stop_devnet(),
        "status" => check_status(),
        cmd => Err(format!("Unknown command: {}", cmd)),
    };

    if let Err(e) = result {
        eprintln!("[devnet-ctl] Error: {}", e);
        std::process::exit(1);
    }
}
