//! SMPC Auction Marketplace - Main entry point.
// Dioxus rsx! macro generates deeply nested types requiring higher recursion limit
#![recursion_limit = "512"]

mod app;
mod demo;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use market::error::{MarketError, MarketResult};
use market::{config, DevNetConfig, VeilidNode};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Auto-demo role parsed from `--demo-role`.
enum DemoRole {
    Seller { duration: u64 },
    Bidder,
}

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
    let node_offset = std::env::var("VEILID_NODE_OFFSET")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(9);

    base_dir.join(format!("smpc-auction-node-{}", node_offset))
}

/// Runtime security checks for network-mode specific requirements.
fn validate_runtime_security() -> MarketResult<()> {
    if std::env::var("MARKET_MODE").as_deref() == Ok("public")
        && std::env::var(config::VEILID_NETWORK_KEY_ENV).is_err()
    {
        return Err(MarketError::Config(format!(
            "Public mode requires an explicit {} env var. \
             Refusing to use default development network key.",
            config::VEILID_NETWORK_KEY_ENV
        )));
    }
    Ok(())
}

/// Preflight check: ensure MP-SPDZ directory, binary, and compiler exist.
fn ensure_mpspdz_ready() -> MarketResult<()> {
    let mp_spdz_dir = std::env::var(config::MP_SPDZ_DIR_ENV)
        .unwrap_or_else(|_| config::DEFAULT_MP_SPDZ_DIR.to_string());
    let dir = Path::new(&mp_spdz_dir);

    let checks = [
        (dir.to_path_buf(), "MP-SPDZ directory"),
        (
            dir.join(market::config::DEFAULT_MPC_PROTOCOL),
            "MPC protocol binary",
        ),
        (dir.join("compile.py"), "compile.py"),
    ];

    let mut missing = Vec::new();
    for (path, label) in &checks {
        if path.exists() {
            info!("MP-SPDZ preflight: [OK] {label}");
        } else {
            missing.push(format!("{label} ({})", path.display()));
        }
    }

    if missing.is_empty() {
        return Ok(());
    }

    Err(MarketError::Config(format!(
        "MP-SPDZ is not ready. Missing:\n  {}\n\
         Run: ./setup-mpspdz.sh --mp-spdz-dir {}",
        missing.join("\n  "),
        mp_spdz_dir
    )))
}

/// Parse `--demo-role seller|bidder` and `--demo-duration <secs>` from argv.
fn parse_demo_args() -> Option<DemoRole> {
    let args: Vec<String> = std::env::args().collect();
    let mut role = None;
    let mut duration = 90u64;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--demo-role" => {
                i += 1;
                if i < args.len() {
                    role = Some(args[i].clone());
                }
            }
            "--demo-duration" => {
                i += 1;
                if i < args.len() {
                    duration = args[i].parse().unwrap_or(90);
                }
            }
            _ => {}
        }
        i += 1;
    }

    role.map(|r| match r.as_str() {
        "seller" => DemoRole::Seller { duration },
        _ => DemoRole::Bidder,
    })
}

fn main() -> MarketResult<()> {
    init_logging();
    info!("Starting SMPC Auction Marketplace");

    validate_runtime_security()?;

    // Preflight: ensure MP-SPDZ artifacts are ready
    ensure_mpspdz_ready()?;

    let demo_role = parse_demo_args();

    // Initialize shared state
    let app_state = SharedAppState::new(market::BidStorage::new());
    SHARED_STATE.set(app_state.clone()).ok();

    // Shutdown token: cancelled when the UI window closes
    let shutdown = CancellationToken::new();
    let shutdown_bg = shutdown.clone();

    // Start Veilid node in background thread
    let node_holder = app_state.node_holder.clone();
    let coordinator_holder = app_state.coordinator.clone();
    let demo_state = app_state.clone();
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

            let config = market::config::MarketConfig::from_env();

            // Create node based on network mode
            let mut node = if std::env::var("MARKET_MODE").as_deref() == Ok("public") {
                info!("Connecting to PUBLIC Veilid network");
                VeilidNode::new(data_dir, &config)
            } else {
                info!("Connecting to LOCAL devnet");
                VeilidNode::new(data_dir, &config)
                    .with_devnet(DevNetConfig::from_market_config(&config))
            };

            if let Err(e) = node.start().await {
                error!("Failed to start Veilid node: {}", e);
                return;
            }

            if let Err(e) = node.attach().await {
                error!("Failed to attach to network: {}", e);
                if let Err(e) = node.shutdown().await {
                    tracing::error!("Failed to shutdown Veilid node: {}", e);
                }
                return;
            }

            // Wait for network to stabilize
            // Devnet regular nodes can take ~2 minutes to bootstrap, so allow
            // up to 180s for attachment.
            info!("Waiting for network to stabilize...");
            let mut retries = 0;
            let max_wait_secs = config.max_attachment_wait_secs;
            loop {
                let state = node.state();
                if state.is_attached {
                    info!("Node attached and ready (after {}s)", retries);
                    break;
                }
                retries += 1;
                if retries > max_wait_secs {
                    error!(
                        "Timeout waiting for network attachment after {max_wait_secs}s, giving up"
                    );
                    if let Err(e) = node.shutdown().await {
                        tracing::error!("Failed to shutdown Veilid node: {}", e);
                    }
                    return;
                }
                if retries % 30 == 0 {
                    warn!("Still waiting for attachment ({retries}s / {max_wait_secs}s)...");
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }

            // Initialize auction coordinator
            let Some(dht) = node.dht_operations() else {
                error!("DHT operations not available after node start");
                return;
            };
            let state = node.state();
            let Some(node_id_str) = state.node_ids.first() else {
                error!("No node IDs available after network attachment");
                return;
            };
            let my_node_id = match veilid_core::PublicKey::try_from(node_id_str.as_str()) {
                Ok(id) => id,
                Err(e) => {
                    error!("Failed to parse node ID '{}': {}", node_id_str, e);
                    return;
                }
            };
            let bid_storage = app_state.bid_storage.clone();

            let api = match node.api() {
                Some(api) => api.clone(),
                None => {
                    error!("Veilid API not available after successful start");
                    return;
                }
            };
            let network_key = market::config::network_key();
            let coordinator_shutdown = shutdown_bg.clone();
            let coordinator = Arc::new(market::AuctionCoordinator::new(
                api,
                dht,
                my_node_id,
                bid_storage,
                config.node_offset,
                &network_key,
                coordinator_shutdown,
            ));

            coordinator.clone().start_monitoring();
            *coordinator_holder.write() = Some(coordinator.clone());
            info!("Auction coordinator started");

            // Spawn auto-demo task if --demo-role was specified
            if let Some(role) = demo_role {
                let demo_s = demo_state;
                tokio::spawn(async move {
                    // Wait for broadcast route to be established (not a blind sleep)
                    let deadline =
                        tokio::time::Instant::now() + tokio::time::Duration::from_secs(60);
                    loop {
                        if let Some(coord) = demo_s.coordinator() {
                            if coord.has_broadcast_route().await {
                                info!("Demo: broadcast route established, starting demo");
                                break;
                            }
                        }
                        if tokio::time::Instant::now() >= deadline {
                            tracing::warn!(
                                "Demo: broadcast route not established after 60s, proceeding anyway"
                            );
                            break;
                        }
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                    match role {
                        DemoRole::Seller { duration } => {
                            demo::demo_seller_task(demo_s, duration).await;
                        }
                        DemoRole::Bidder => {
                            demo::demo_bidder_task(demo_s).await;
                        }
                    }
                });
            }

            // Take update receiver for AppMessages
            let update_rx = node.take_update_receiver();

            // Store node for UI access
            *node_holder.write() = Some(node);

            // Process updates
            if let Some(mut rx) = update_rx {
                loop {
                    tokio::select! {
                        () = shutdown_bg.cancelled() => {
                            info!("Shutdown signal received in node thread");
                            break;
                        }
                        maybe_update = rx.recv() => {
                            match maybe_update {
                                Some(update) => {
                                    let coordinator = coordinator_holder.read().clone();
                                    if let Some(coordinator) = coordinator {
                                        coordinator.dispatch_veilid_update(update).await;
                                    }
                                }
                                None => {
                                    warn!("Update channel closed");
                                    break;
                                }
                            }
                        }
                    }
                }
            } else {
                shutdown_bg.cancelled().await;
            }

            // Graceful coordinator shutdown — release routes and DHT records.
            {
                let coord = coordinator_holder.read().clone();
                if let Some(c) = coord {
                    c.shutdown().await;
                }
            }

            // Ensure node is detached and shut down before thread exits.
            let mut node = node_holder.write().take();
            if let Some(node_ref) = node.as_ref() {
                if let Err(e) = node_ref.detach().await {
                    warn!("Detach during shutdown failed (continuing): {}", e);
                }
            }
            if let Some(node_ref) = node.as_mut() {
                if let Err(e) = node_ref.shutdown().await {
                    error!("Node shutdown failed: {}", e);
                }
            }
        });
    });

    // Spawn a signal handler so Ctrl-C / SIGTERM trigger shutdown
    // even while the UI is running.
    let signal_shutdown = shutdown.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("signal handler runtime");
        rt.block_on(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Ctrl-C received, initiating shutdown");
                }
                _ = async {
                    #[cfg(unix)]
                    {
                        let mut sig = tokio::signal::unix::signal(
                            tokio::signal::unix::SignalKind::terminate(),
                        ).expect("SIGTERM handler");
                        sig.recv().await;
                    }
                    #[cfg(not(unix))]
                    std::future::pending::<()>().await;
                } => {
                    info!("SIGTERM received, initiating shutdown");
                }
            }
            signal_shutdown.cancel();
        });
    });

    // Launch Dioxus UI (blocks until window is closed)
    dioxus::launch(app::app);

    // Signal background tasks to shut down
    info!("UI closed, shutting down...");
    shutdown.cancel();

    Ok(())
}
