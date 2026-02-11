//! SMPC Auction Marketplace - Main entry point.
#![recursion_limit = "512"]

mod app;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use market::{config, DevNetConfig, VeilidNode};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::app::{SharedAppState, SHARED_STATE};

fn init_logging() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();
}

fn get_data_dir() -> PathBuf {
    let base_dir = dirs::data_local_dir().unwrap_or_else(|| PathBuf::from("."));

    // Check if running in public network mode
    if std::env::var("MARKET_MODE").as_deref() == Ok("public") {
        return base_dir.join("smpc-auction-public");
    }

    // Use node-specific data directory for devnet
    let node_offset = std::env::var("MARKET_NODE_OFFSET")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(5);

    base_dir.join(format!("smpc-auction-node-{}", node_offset))
}

/// Preflight check: ensure MP-SPDZ is ready before starting the node.
///
/// Verifies that the MP-SPDZ directory, binary, SSL certs, and compiler exist.
/// If anything is missing, attempts to run `setup-mpspdz.sh` automatically.
fn ensure_mpspdz_ready() -> anyhow::Result<()> {
    let mp_spdz_dir = std::env::var(config::MP_SPDZ_DIR_ENV)
        .unwrap_or_else(|_| config::DEFAULT_MP_SPDZ_DIR.to_string());
    let dir = Path::new(&mp_spdz_dir);

    info!("MP-SPDZ preflight: checking {}", mp_spdz_dir);

    let checks = [
        (dir.to_path_buf(), "MP-SPDZ directory"),
        (dir.join("shamir-party.x"), "shamir-party.x binary"),
        (dir.join("Player-Data/P0.pem"), "SSL certificate P0.pem"),
        (dir.join("compile.py"), "compile.py"),
    ];

    let all_ok = checks.iter().all(|(path, label)| {
        let exists = path.exists();
        if exists {
            info!("  [OK] {}", label);
        } else {
            warn!("  [MISSING] {} ({})", label, path.display());
        }
        exists
    });

    if all_ok {
        info!("MP-SPDZ preflight: all checks passed");
        return Ok(());
    }

    // Try to auto-run setup-mpspdz.sh
    // Look for the script relative to MP-SPDZ dir (one level up) or next to it
    let candidates = [
        dir.join("../setup-mpspdz.sh"),
        dir.join("../../setup-mpspdz.sh"),
    ];

    let script = candidates.iter().find(|p| p.exists());

    match script {
        Some(script_path) => {
            info!(
                "Running setup script: {} --mp-spdz-dir {}",
                script_path.display(),
                mp_spdz_dir
            );
            let status = std::process::Command::new("bash")
                .arg(script_path)
                .arg("--mp-spdz-dir")
                .arg(&mp_spdz_dir)
                .status();

            match status {
                Ok(s) if s.success() => {
                    info!("MP-SPDZ setup completed successfully");
                    Ok(())
                }
                Ok(s) => {
                    anyhow::bail!(
                        "MP-SPDZ setup script failed (exit code: {:?}).\n\
                         Run manually: {} --mp-spdz-dir {}",
                        s.code(),
                        script_path.display(),
                        mp_spdz_dir
                    );
                }
                Err(e) => {
                    anyhow::bail!(
                        "Failed to execute MP-SPDZ setup script: {}\n\
                         Run manually: {} --mp-spdz-dir {}",
                        e,
                        script_path.display(),
                        mp_spdz_dir
                    );
                }
            }
        }
        None => {
            anyhow::bail!(
                "MP-SPDZ is not ready and setup-mpspdz.sh was not found.\n\
                 Please run the setup script manually:\n\
                   ./setup-mpspdz.sh --mp-spdz-dir {}\n\
                 Or ensure shamir-party.x, SSL certs, and compile.py exist in {}",
                mp_spdz_dir,
                mp_spdz_dir
            );
        }
    }
}

fn main() -> anyhow::Result<()> {
    init_logging();
    info!("Starting SMPC Auction Marketplace");

    // Preflight: ensure MP-SPDZ artifacts are ready
    ensure_mpspdz_ready()?;

    // Initialize shared state
    let app_state = SharedAppState::new(market::BidStorage::new());
    SHARED_STATE.set(app_state.clone()).ok();

    // Start Veilid node in background thread
    let node_holder = app_state.node_holder.clone();
    let coordinator_holder = app_state.coordinator.clone();
    std::thread::spawn(move || {
        let rt = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
                error!("Failed to create Tokio runtime: {e}");
                return;
            }
        };
        rt.block_on(async move {
            let data_dir = get_data_dir();
            info!("Using data directory: {:?}", data_dir);

            // Create node based on network mode
            let mut node = if std::env::var("MARKET_MODE").as_deref() == Ok("public") {
                info!("Connecting to PUBLIC Veilid network");
                VeilidNode::new(data_dir).with_public_network()
            } else {
                info!("Connecting to LOCAL devnet");
                VeilidNode::new(data_dir)
                    .with_devnet(DevNetConfig::default())
                    .with_insecure_storage(true)
            };

            if let Err(e) = node.start().await {
                error!("Failed to start Veilid node: {}", e);
                return;
            }

            if let Err(e) = node.attach().await {
                error!("Failed to attach to network: {}", e);
                let _ = node.shutdown().await;
                return;
            }

            // Wait for network to stabilize
            info!("Waiting for network to stabilize...");
            let mut retries = 0;
            loop {
                let state = node.state();
                if state.is_attached {
                    info!("Node attached and ready");
                    break;
                }
                retries += 1;
                if retries > 30 {
                    error!("Timeout waiting for network attachment");
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }

            // Initialize auction coordinator
            if let Some(dht) = node.dht_operations() {
                let state = node.state();
                if let Some(node_id_str) = state.node_ids.first() {
                    if let Ok(my_node_id) = veilid_core::PublicKey::try_from(node_id_str.as_str()) {
                        let bid_storage = app_state.bid_storage.clone();

                        let node_offset = std::env::var("MARKET_NODE_OFFSET")
                            .ok()
                            .and_then(|s| s.parse::<u16>().ok())
                            .unwrap_or(5);

                        let api = match node.api() {
                            Some(api) => api.clone(),
                            None => {
                                error!("Veilid API not available after successful start");
                                return;
                            }
                        };
                        let network_key = market::config::network_key();
                        let shutdown = CancellationToken::new();
                        let coordinator = Arc::new(market::AuctionCoordinator::new(
                            api,
                            dht,
                            my_node_id,
                            bid_storage,
                            node_offset,
                            &network_key,
                            shutdown,
                        ));

                        coordinator.clone().start_monitoring();
                        *coordinator_holder.write() = Some(coordinator.clone());
                        info!("Auction coordinator started");
                    }
                }
            }

            // Take update receiver for AppMessages
            let update_rx = node.take_update_receiver();

            // Store node for UI access
            *node_holder.write() = Some(node);

            // Process updates
            if let Some(mut rx) = update_rx {
                loop {
                    match rx.recv().await {
                        Some(veilid_core::VeilidUpdate::AppMessage(msg)) => {
                            let coordinator = coordinator_holder.read().clone();
                            if let Some(coordinator) = coordinator {
                                if let Err(e) = coordinator
                                    .process_app_message(msg.message().to_vec())
                                    .await
                                {
                                    error!("Failed to process MPC message: {}", e);
                                }
                            }
                        }
                        Some(_) => {}
                        None => {
                            warn!("Update channel closed");
                            break;
                        }
                    }
                }
            } else {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                }
            }
        });
    });

    // Launch Dioxus UI
    dioxus::launch(app::app);

    Ok(())
}
