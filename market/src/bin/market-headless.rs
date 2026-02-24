#![recursion_limit = "512"]
//! Headless market node for E2E testing.
//!
//! Runs a single Veilid node per process with JSON-over-stdio IPC.
//! Logging goes to stderr; stdout is reserved for the IPC protocol.
//!
//! Usage:
//!   market-headless --offset 36
//!
//! On startup, emits a `Ready` JSON line to stdout.
//! Then reads `TestCommand` JSON lines from stdin and writes `TestResponse` lines.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use market::actions::{create_and_publish_listing, submit_bid};
use market::shared_state::SharedAppState;
use market::{config, AuctionCoordinator, BidStorage, DevNetConfig, VeilidNode};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

// ── IPC types ────────────────────────────────────────────────────────

#[derive(Serialize)]
struct ReadyEvent {
    event: &'static str,
    node_id: String,
    signing_pubkey: String,
}

#[derive(Deserialize)]
#[serde(tag = "cmd")]
enum TestCommand {
    CreateListing {
        title: String,
        content: String,
        reserve_price: u64,
        duration_secs: u64,
    },
    PlaceBid {
        listing_key: String,
        amount: u64,
    },
    GetDecryptionKey {
        listing_key: String,
    },
    GetNodeId,
    Shutdown,
}

#[derive(Serialize)]
#[serde(tag = "status")]
enum TestResponse {
    Ok { data: Option<serde_json::Value> },
    Err { message: String },
}

// ── Helpers ──────────────────────────────────────────────────────────

fn parse_offset() -> u16 {
    let args: Vec<String> = std::env::args().collect();
    for i in 0..args.len() {
        if args[i] == "--offset" {
            if let Some(val) = args.get(i + 1) {
                return val.parse().expect("--offset must be a u16");
            }
        }
    }
    panic!("Usage: market-headless --offset <u16>");
}

fn init_logging_stderr() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,veilid_core=warn"));
    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_ansi(false),
        )
        .init();
}

fn emit_json(value: &impl Serialize) {
    let line = serde_json::to_string(value).expect("JSON serialization failed");
    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "{}", line).expect("stdout write failed");
    stdout.flush().expect("stdout flush failed");
}

fn data_dir(offset: u16) -> PathBuf {
    std::env::temp_dir().join(format!("market-headless-{}", offset))
}

// ── Main ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let offset = parse_offset();
    init_logging_stderr();
    info!("market-headless starting (offset={})", offset);

    // Clean + create data dir
    let dir = data_dir(offset);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("Failed to create data dir");

    // Create + start Veilid node
    let devnet_config = DevNetConfig {
        network_key: "development-network-2025".to_string(),
        bootstrap_nodes: vec!["udp://1.2.3.1:5160".to_string()],
        port_offset: offset,
        limit_over_attached: 24,
    };
    let market_config = config::MarketConfig {
        insecure_storage: true,
        ..config::MarketConfig::default()
    };
    let mut node = VeilidNode::new(dir.clone(), &market_config).with_devnet(devnet_config);

    if let Err(e) = node.start().await {
        error!("Failed to start node: {}", e);
        std::process::exit(1);
    }
    if let Err(e) = node.attach().await {
        error!("Failed to attach: {}", e);
        std::process::exit(1);
    }

    // Wait for attachment + peers
    let start = std::time::Instant::now();
    loop {
        let state = node.state();
        if state.is_attached && !state.node_ids.is_empty() && state.peer_count >= 4 {
            info!(
                "Node ready: {} peers, {} IDs ({}s)",
                state.peer_count,
                state.node_ids.len(),
                start.elapsed().as_secs()
            );
            break;
        }
        if start.elapsed().as_secs() > 180 {
            // Proceed anyway if attached with IDs
            let state = node.state();
            if state.is_attached && !state.node_ids.is_empty() {
                warn!(
                    "Peer wait timeout, proceeding with {} peers",
                    state.peer_count
                );
                break;
            }
            error!("Node not ready after 180s");
            std::process::exit(1);
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    // Build coordinator
    let node_state = node.state();
    let node_id_str = node_state.node_ids.first().expect("Node should have an ID");
    let my_node_id =
        veilid_core::PublicKey::try_from(node_id_str.as_str()).expect("Invalid node ID");

    let dht = node.dht_operations().expect("DHT not available");
    let api = node.api().expect("API not available").clone();
    let bid_storage = BidStorage::new();
    let shutdown = CancellationToken::new();
    let network_key = config::network_key();

    let coordinator = Arc::new(AuctionCoordinator::new(
        api,
        dht.clone(),
        my_node_id,
        bid_storage.clone(),
        offset,
        &network_key,
        shutdown.clone(),
    ));

    coordinator.clone().start_monitoring();

    // Build shared state
    let app_state = SharedAppState::new(bid_storage);
    *app_state.node_holder.write() = Some(node);
    *app_state.coordinator.write() = Some(coordinator.clone());

    // Spawn Veilid update loop
    let update_rx = {
        let holder = app_state.node_holder.read();
        // take_update_receiver needs &mut, but we've already stored it.
        // We need to take before storing. Let me restructure.
        drop(holder);
        let mut holder = app_state.node_holder.write();
        holder.as_mut().and_then(|n| n.take_update_receiver())
    };

    let coord_for_loop = coordinator.clone();
    tokio::spawn(async move {
        if let Some(mut rx) = update_rx {
            while let Some(update) = rx.recv().await {
                match update {
                    veilid_core::VeilidUpdate::AppMessage(msg) => {
                        if let Err(e) = coord_for_loop
                            .process_app_message(msg.message().to_vec())
                            .await
                        {
                            error!("AppMessage error: {}", e);
                        }
                    }
                    veilid_core::VeilidUpdate::AppCall(call) => {
                        let api = coord_for_loop.api().clone();
                        let call_id = call.id();
                        match coord_for_loop
                            .process_app_call(call.message().to_vec())
                            .await
                        {
                            Ok(response) => {
                                if let Err(e) = api.app_call_reply(call_id, response).await {
                                    error!("app_call_reply error: {}", e);
                                }
                            }
                            Err(e) => {
                                error!("AppCall error: {}", e);
                                let _ = api.app_call_reply(call_id, vec![0x00]).await;
                            }
                        }
                    }
                    veilid_core::VeilidUpdate::RouteChange(change) => {
                        coord_for_loop
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

    // Emit Ready
    let signing_pubkey = coordinator.signing_pubkey_hex();
    emit_json(&ReadyEvent {
        event: "Ready",
        node_id: node_id_str.clone(),
        signing_pubkey,
    });
    info!("Ready event emitted");

    // Command loop
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();

    loop {
        let line = match lines.next_line().await {
            Ok(Some(l)) => l,
            Ok(None) => {
                info!("stdin closed, shutting down");
                break;
            }
            Err(e) => {
                error!("stdin read error: {}", e);
                break;
            }
        };

        let cmd: TestCommand = match serde_json::from_str(&line) {
            Ok(c) => c,
            Err(e) => {
                emit_json(&TestResponse::Err {
                    message: format!("Invalid command JSON: {}", e),
                });
                continue;
            }
        };

        match cmd {
            TestCommand::CreateListing {
                title,
                content,
                reserve_price,
                duration_secs,
            } => {
                let result = create_and_publish_listing(
                    &app_state,
                    &dht,
                    &title,
                    &content,
                    &reserve_price.to_string(),
                    &duration_secs.to_string(),
                )
                .await;
                match result {
                    Ok(r) => {
                        // Retrieve the decryption key from the coordinator
                        let dec_key = coordinator
                            .get_decryption_key(
                                &veilid_core::RecordKey::try_from(r.key.as_str())
                                    .expect("valid key"),
                            )
                            .await;
                        emit_json(&TestResponse::Ok {
                            data: Some(serde_json::json!({
                                "listing_key": r.key,
                                "decryption_key": dec_key,
                            })),
                        });
                    }
                    Err(e) => emit_json(&TestResponse::Err {
                        message: e.to_string(),
                    }),
                }
            }

            TestCommand::PlaceBid {
                listing_key,
                amount,
            } => {
                let result = submit_bid(&app_state, &dht, &listing_key, amount).await;
                match result {
                    Ok(_msg) => {
                        let bid_key = app_state
                            .bid_storage
                            .get_bid_key(
                                &veilid_core::RecordKey::try_from(listing_key.as_str())
                                    .expect("valid key"),
                            )
                            .await;
                        emit_json(&TestResponse::Ok {
                            data: Some(serde_json::json!({
                                "bid_key": bid_key.map(|k| k.to_string()),
                            })),
                        });
                    }
                    Err(e) => emit_json(&TestResponse::Err {
                        message: e.to_string(),
                    }),
                }
            }

            TestCommand::GetDecryptionKey { listing_key } => {
                let key = match veilid_core::RecordKey::try_from(listing_key.as_str()) {
                    Ok(k) => coordinator.get_decryption_key(&k).await,
                    Err(e) => {
                        emit_json(&TestResponse::Err {
                            message: format!("Invalid listing key: {}", e),
                        });
                        continue;
                    }
                };
                emit_json(&TestResponse::Ok {
                    data: Some(serde_json::json!({ "key": key })),
                });
            }

            TestCommand::GetNodeId => {
                emit_json(&TestResponse::Ok {
                    data: Some(serde_json::json!({ "node_id": node_id_str })),
                });
            }

            TestCommand::Shutdown => {
                info!("Shutdown command received");
                emit_json(&TestResponse::Ok { data: None });
                break;
            }
        }
    }

    // Graceful shutdown
    shutdown.cancel();
    let node = app_state.node_holder.write().take();
    if let Some(mut n) = node {
        let _ = n.detach().await;
        let _ = n.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&dir);
    info!("Shutdown complete");
}
