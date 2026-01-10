use std::path::PathBuf;
use std::sync::Arc;

use dioxus::prelude::*;
use market::veilid::node::NodeState;
use market::{DevNetConfig, VeilidNode};
use parking_lot::RwLock;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

// Global node state for UI access
static NODE: once_cell::sync::OnceCell<Arc<RwLock<Option<VeilidNode>>>> =
    once_cell::sync::OnceCell::new();

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

    // Use node-specific data directory for devnet based on MARKET_NODE_OFFSET
    // This allows running multiple devnet instances without conflicts
    let node_offset = std::env::var("MARKET_NODE_OFFSET")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(5);

    base_dir.join(format!("smpc-auction-node-{}", node_offset))
}

fn main() {
    init_logging();
    info!("Starting SMPC Auction Marketplace");

    // Initialize global node holder
    NODE.set(Arc::new(RwLock::new(None))).ok();

    // Start Veilid node in background thread
    let node_holder = NODE.get().unwrap().clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let data_dir = get_data_dir();
            info!("Using data directory: {:?}", data_dir);

            // Check if running in public network mode
            let mut node = if std::env::var("MARKET_MODE").as_deref() == Ok("public") {
                info!("Connecting to PUBLIC Veilid network");
                VeilidNode::new(data_dir).with_public_network()
            } else {
                info!("Connecting to LOCAL devnet");
                VeilidNode::new(data_dir).with_devnet(DevNetConfig::default())
            };

            if let Err(e) = node.start().await {
                tracing::error!("Failed to start Veilid node: {}", e);
                return;
            }

            if let Err(e) = node.attach().await {
                tracing::error!("Failed to attach to network: {}", e);
                let _ = node.shutdown().await;
                return;
            }

            // Store node for UI access
            *node_holder.write() = Some(node);

            // Keep the runtime alive
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            }
        });
    });

    // Launch Dioxus UI (synchronous)
    dioxus::launch(app);
}

fn get_node_state() -> NodeState {
    if let Some(node_holder) = NODE.get() {
        if let Some(node) = node_holder.read().as_ref() {
            return node.state();
        }
    }
    NodeState::default()
}

fn app() -> Element {
    let mut node_state = use_signal(NodeState::default);

    // Use Dioxus's use_future for polling
    let _state_poller = use_resource(move || async move {
        loop {
            // Poll every second
            async_std::task::sleep(std::time::Duration::from_secs(1)).await;
            let state = get_node_state();
            node_state.set(state);
        }
    });

    let state = node_state.read();

    let connection_status = if state.is_attached {
        "Connected"
    } else {
        "Connecting..."
    };

    let node_id_display = if state.node_ids.is_empty() {
        "Waiting for network...".to_string()
    } else {
        state.node_ids.first().cloned().unwrap_or_default()
    };

    rsx! {
        div {
            class: "container",
            style: "font-family: system-ui; padding: 20px; max-width: 800px; margin: 0 auto;",

            h1 {
                style: "color: #333;",
                "SMPC Sealed-Bid Auction"
            }

            div {
                class: "status-card",
                style: "background: #f5f5f5; padding: 15px; border-radius: 8px; margin-bottom: 20px;",

                h2 {
                    style: "margin-top: 0; font-size: 1.2em;",
                    "Network Status"
                }

                div {
                    style: "display: grid; grid-template-columns: auto 1fr; gap: 10px;",

                    span { style: "font-weight: bold;", "Status:" }
                    span {
                        style: if state.is_attached { "color: green;" } else { "color: orange;" },
                        "{connection_status}"
                    }

                    span { style: "font-weight: bold;", "Peers:" }
                    span { "{state.peer_count}" }

                    span { style: "font-weight: bold;", "Node ID:" }
                    span {
                        style: "font-family: monospace; font-size: 0.85em; word-break: break-all;",
                        "{node_id_display}"
                    }
                }
            }

            div {
                class: "placeholder",
                style: "background: #e8f4fd; padding: 20px; border-radius: 8px; text-align: center; color: #666;",

                p { "Marketplace features coming soon..." }
                p {
                    style: "font-size: 0.9em;",
                    "Phase 1: Veilid Integration ✓"
                }
                p {
                    style: "font-size: 0.9em;",
                    "Next: DHT-based listing publish/discovery"
                }
            }
        }
    }
}
