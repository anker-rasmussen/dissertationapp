//! Veilid node lifecycle: configuration, startup, network attachment, and shutdown.
//!
//! [`VeilidNode`] wraps the Veilid API with market-specific configuration (devnet
//! bootstrap, port offsets, capability flags). [`DevNetConfig`] holds devnet parameters
//! (network key, bootstrap IPs, port offset).

use std::path::PathBuf;
use std::sync::Arc;

use crate::error::{MarketError, MarketResult};
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use veilid_core::{
    api_startup, VeilidAPI, VeilidConfig, VeilidConfigCapabilities, VeilidConfigDHT,
    VeilidConfigNetwork, VeilidConfigProtectedStore, VeilidConfigProtocol, VeilidConfigRPC,
    VeilidConfigRoutingTable, VeilidConfigTCP, VeilidConfigTableStore, VeilidConfigUDP,
    VeilidConfigWS, VeilidUpdate, VEILID_CAPABILITY_SIGNAL, VEILID_CAPABILITY_VALIDATE_DIAL_INFO,
};

/// Snapshot of Veilid node connectivity: attachment status, peer count, and node IDs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeState {
    pub is_attached: bool,
    pub peer_count: usize,
    pub node_ids: Vec<String>,
}

/// Configuration for connecting to the local Docker devnet.
///
/// Default: 20-node devnet (1 bootstrap + 19 regular), market nodes start at offset 20.
/// Port formula: `5160 + offset`, IP formula: `1.2.3.(offset + 1)`.
#[derive(Debug, Clone)]
pub struct DevNetConfig {
    pub network_key: String,
    pub bootstrap_nodes: Vec<String>,
    /// Port offset from base port 5160 (0=bootstrap, 1-4=nodes, 5+=market clients)
    pub port_offset: u16,
    /// Routing table limit for over-attached peers.
    pub limit_over_attached: u32,
}

impl Default for DevNetConfig {
    fn default() -> Self {
        // Port offset can be overridden via MARKET_NODE_OFFSET env var
        // The devnet uses offsets 0-19 (20 nodes), so market instances start at 20+:
        // - MARKET_NODE_OFFSET=20 (default)  -> port 5180, IP 1.2.3.21, data: ~/.local/share/smpc-auction-node-20
        // - MARKET_NODE_OFFSET=21 -> port 5181, IP 1.2.3.22, data: ~/.local/share/smpc-auction-node-21
        // - MARKET_NODE_OFFSET=22 -> port 5182, IP 1.2.3.23, data: ~/.local/share/smpc-auction-node-22
        let port_offset = std::env::var("MARKET_NODE_OFFSET")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(20);

        Self {
            network_key: "development-network-2025".to_string(),
            // Use fake global IP for bootstrap (1.2.3.1:5160)
            // The LD_PRELOAD library translates this to 127.0.0.1:5160
            // Use UDP for BOOT protocol - TCP requires VL framing which BOOT doesn't have
            bootstrap_nodes: vec!["udp://1.2.3.1:5160".to_string()],
            port_offset,
            limit_over_attached: 24,
        }
    }
}

/// Manages the Veilid node lifecycle: startup, network attachment, update polling, and shutdown.
///
/// Wraps `VeilidAPI` and exposes a channel of `VeilidUpdate` events for the
/// coordinator to consume. Supports both public network and devnet configurations.
pub struct VeilidNode {
    api: Option<VeilidAPI>,
    state: Arc<RwLock<NodeState>>,
    data_dir: PathBuf,
    devnet_config: Option<DevNetConfig>,
    /// Whether to use insecure (unencrypted) protected storage.
    /// Defaults to `false` for production safety; set to `true` for devnet/test.
    insecure_storage: bool,
    /// Veilid RPC timeout in milliseconds (default: 10_000).
    rpc_timeout_ms: u32,
    update_tx: mpsc::Sender<VeilidUpdate>,
    update_rx: Option<mpsc::Receiver<VeilidUpdate>>,
}

impl VeilidNode {
    /// Create a new node with the given data directory and market configuration.
    pub fn new(data_dir: PathBuf, config: &crate::config::MarketConfig) -> Self {
        let (update_tx, update_rx) = mpsc::channel(config.update_channel_capacity);
        Self {
            api: None,
            state: Arc::new(RwLock::new(NodeState::default())),
            data_dir,
            devnet_config: None,
            insecure_storage: config.insecure_storage,
            rpc_timeout_ms: config.rpc_timeout_ms,
            update_tx,
            update_rx: Some(update_rx),
        }
    }

    /// Configure this node to connect to the local devnet (requires LD_PRELOAD).
    #[must_use]
    pub fn with_devnet(mut self, config: DevNetConfig) -> Self {
        self.devnet_config = Some(config);
        self
    }

    /// Explicitly set whether to use insecure (unencrypted) protected storage.
    ///
    /// In production this should remain `false` (the default).
    /// For devnet or testing, chain `.with_insecure_storage(true)` explicitly.
    #[must_use]
    pub const fn with_insecure_storage(mut self, insecure: bool) -> Self {
        self.insecure_storage = insecure;
        self
    }

    /// Start the Veilid node: initialise the API, configure networking, and attach.
    #[allow(clippy::too_many_lines)]
    pub async fn start(&mut self) -> MarketResult<()> {
        info!("Starting Veilid node...");

        if self.insecure_storage {
            warn!(
                "Insecure (unencrypted) protected storage is ENABLED. \
                 Do not use this setting in production."
            );
        }

        let protected_store_dir = self.data_dir.join("protected_store");
        let table_store_dir = self.data_dir.join("table_store");

        // Create directories if they don't exist
        std::fs::create_dir_all(&protected_store_dir).map_err(|e| {
            MarketError::Config(format!("Failed to create protected_store directory: {e}"))
        })?;
        std::fs::create_dir_all(&table_store_dir).map_err(|e| {
            MarketError::Config(format!("Failed to create table_store directory: {e}"))
        })?;

        // Build network config based on whether we're connecting to devnet
        let network_config = if let Some(devnet) = &self.devnet_config {
            // Calculate our port and public address for the ip_spoof layer.
            // Both market nodes and Docker infrastructure nodes use libipspoof,
            // so advertising 1.2.3.X as our public address lets all nodes
            // reach us.  This gives market nodes valid dial info, enabling
            // safe route allocation for broadcasts.
            let port = 5160 + devnet.port_offset;
            let listen_addr = format!("127.0.0.1:{port}");
            let public_addr = format!("1.2.3.{}:{port}", devnet.port_offset + 1);

            info!(
                "Configuring for devnet: key={}, bootstrap={:?}, listen={}, public={}",
                devnet.network_key, devnet.bootstrap_nodes, listen_addr, public_addr
            );

            VeilidConfigNetwork {
                network_key_password: Some(devnet.network_key.clone()),
                detect_address_changes: Some(false), // Static addresses for devnet
                rpc: VeilidConfigRPC {
                    timeout_ms: self.rpc_timeout_ms,
                    ..Default::default()
                },
                dht: VeilidConfigDHT {
                    get_value_timeout_ms: self.rpc_timeout_ms * 2,
                    set_value_timeout_ms: self.rpc_timeout_ms * 2,
                    resolve_node_timeout_ms: self.rpc_timeout_ms * 2,
                    ..Default::default()
                },
                routing_table: VeilidConfigRoutingTable {
                    bootstrap: devnet.bootstrap_nodes.clone(),
                    bootstrap_keys: vec![], // No signature verification for devnet
                    // Limits for 9-node devnet (9 docker + N market instances).
                    // Over-attached at 16 peers to ensure enough routing table
                    // entries for unique safety route construction.
                    limit_over_attached: devnet.limit_over_attached,
                    limit_fully_attached: 20,
                    limit_attached_strong: 16,
                    limit_attached_good: 8,
                    limit_attached_weak: 2, // Reach "AttachedWeak" with just 2 good peers
                    ..Default::default()
                },
                protocol: VeilidConfigProtocol {
                    udp: VeilidConfigUDP {
                        enabled: true,
                        socket_pool_size: 0,
                        listen_address: listen_addr.clone(),
                        public_address: Some(public_addr.clone()),
                    },
                    tcp: VeilidConfigTCP {
                        connect: true,
                        listen: true,
                        max_connections: 32,
                        listen_address: listen_addr.clone(),
                        public_address: Some(public_addr),
                    },
                    ws: VeilidConfigWS {
                        connect: true,
                        listen: false, // Disabled in devnet — conflicts with TCP on same port
                        max_connections: 16,
                        listen_address: listen_addr,
                        path: "ws".to_string(),
                        url: None,
                    },
                },
                ..Default::default()
            }
        } else {
            info!("Configuring for public Veilid network");
            VeilidConfigNetwork {
                rpc: VeilidConfigRPC {
                    timeout_ms: self.rpc_timeout_ms,
                    ..Default::default()
                },
                dht: VeilidConfigDHT {
                    get_value_timeout_ms: self.rpc_timeout_ms * 2,
                    set_value_timeout_ms: self.rpc_timeout_ms * 2,
                    resolve_node_timeout_ms: self.rpc_timeout_ms * 2,
                    ..Default::default()
                },
                ..Default::default()
            }
        };

        // For devnet, disable capabilities not needed by market nodes.
        // RELAY is left enabled so safe routing works (devnet nodes 1-4 are
        // relay-capable, and market nodes need to construct safe routes).
        let capabilities = if self.devnet_config.is_some() {
            VeilidConfigCapabilities {
                disable: vec![
                    VEILID_CAPABILITY_SIGNAL,
                    VEILID_CAPABILITY_VALIDATE_DIAL_INFO,
                ],
            }
        } else {
            VeilidConfigCapabilities::default()
        };

        let namespace = self
            .data_dir
            .file_name()
            .and_then(|s| s.to_str())
            .map_or_else(
                || "smpc-auction".to_string(),
                |s| format!("smpc-auction-{s}"),
            );

        let config = VeilidConfig {
            program_name: "market".into(),
            namespace,
            capabilities,
            protected_store: VeilidConfigProtectedStore {
                always_use_insecure_storage: self.insecure_storage,
                directory: protected_store_dir.to_string_lossy().to_string(),
                ..Default::default()
            },
            table_store: VeilidConfigTableStore {
                directory: table_store_dir.to_string_lossy().to_string(),
                ..Default::default()
            },
            network: network_config,
            ..Default::default()
        };

        let state = self.state.clone();
        let update_tx = self.update_tx.clone();

        let update_callback = Arc::new(move |update: VeilidUpdate| {
            // Forward update to channel for async processing
            if let Err(e) = update_tx.try_send(update.clone()) {
                tracing::warn!("Veilid update channel full, dropping update: {}", e);
            }

            // Update internal state synchronously
            match &update {
                VeilidUpdate::Network(network) => {
                    let mut st = state.write();
                    st.peer_count = network.peers.len();
                    st.node_ids = network
                        .node_ids
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect();
                    let peer_count = st.peer_count;
                    let node_ids = st.node_ids.clone();
                    drop(st);
                    debug!(
                        "Network update: {} peers, node_ids: {:?}",
                        peer_count, node_ids
                    );
                }
                VeilidUpdate::Attachment(attachment) => {
                    state.write().is_attached = attachment.state.is_attached();
                    info!("Attachment state: {:?}", attachment.state);
                }
                VeilidUpdate::AppMessage(msg) => {
                    debug!("Received app message: {} bytes", msg.message().len());
                }
                VeilidUpdate::RouteChange(change) => {
                    warn!("Route change: {:?}", change);
                }
                VeilidUpdate::Shutdown => {
                    info!("Veilid shutdown signal received");
                }
                _ => {
                    debug!("Veilid update: {:?}", update);
                }
            }
        });

        let api = api_startup(update_callback, config).await.map_err(|e| {
            error!("Veilid API startup failed: {:?}", e);
            MarketError::Network(format!("Failed to start Veilid API: {e}"))
        })?;

        self.api = Some(api);
        info!("Veilid node started successfully");

        Ok(())
    }

    pub async fn attach(&self) -> MarketResult<()> {
        let api = self
            .api
            .as_ref()
            .ok_or_else(|| MarketError::InvalidState("Veilid node not started".into()))?;
        info!("Attaching to Veilid network...");
        api.attach().await?;
        info!("Attached to network");
        Ok(())
    }

    pub async fn detach(&self) -> MarketResult<()> {
        let api = self
            .api
            .as_ref()
            .ok_or_else(|| MarketError::InvalidState("Veilid node not started".into()))?;
        info!("Detaching from Veilid network...");
        api.detach().await?;
        info!("Detached from network");
        Ok(())
    }

    /// Detach from the network and shut down the Veilid API.
    pub async fn shutdown(&mut self) -> MarketResult<()> {
        if let Some(api) = self.api.take() {
            info!("Shutting down Veilid node...");
            api.shutdown().await;
            info!("Veilid node shut down");
        }
        Ok(())
    }

    pub const fn api(&self) -> Option<&VeilidAPI> {
        self.api.as_ref()
    }

    pub fn state(&self) -> NodeState {
        self.state.read().clone()
    }

    pub const fn take_update_receiver(&mut self) -> Option<mpsc::Receiver<VeilidUpdate>> {
        self.update_rx.take()
    }

    /// Create a DHT operations handle for this node.
    /// Returns None if the node hasn't been started yet.
    pub fn dht_operations(&self) -> Option<crate::veilid::dht::DHTOperations> {
        self.api
            .as_ref()
            .map(|api| crate::veilid::dht::DHTOperations::new(api.clone()))
    }
}

impl Drop for VeilidNode {
    fn drop(&mut self) {
        if self.api.is_some() {
            error!("VeilidNode dropped without calling shutdown()! This may cause resource leaks.");
        }
    }
}
