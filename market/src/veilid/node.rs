use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use veilid_core::{
    api_startup, VeilidAPI, VeilidConfig, VeilidConfigCapabilities, VeilidConfigNetwork,
    VeilidConfigProtectedStore, VeilidConfigProtocol, VeilidConfigRoutingTable, VeilidConfigTCP,
    VeilidConfigTableStore, VeilidConfigUDP, VeilidConfigWS, VeilidUpdate, VEILID_CAPABILITY_RELAY,
    VEILID_CAPABILITY_SIGNAL, VEILID_CAPABILITY_VALIDATE_DIAL_INFO,
};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct NodeState {
    pub is_attached: bool,
    pub peer_count: usize,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DevNetConfig {
    pub network_key: String,
    pub bootstrap_nodes: Vec<String>,
    /// Port offset from base port 5160 (0=bootstrap, 1-4=nodes, 5+=market clients)
    pub port_offset: u16,
}

impl Default for DevNetConfig {
    fn default() -> Self {
        // Port offset can be overridden via MARKET_NODE_OFFSET env var
        // This allows running multiple market instances for your demo:
        // - MARKET_NODE_OFFSET=5 (default) -> port 5165, IP 1.2.3.6, data: ~/.local/share/smpc-auction-node-5
        // - MARKET_NODE_OFFSET=6 -> port 5166, IP 1.2.3.7, data: ~/.local/share/smpc-auction-node-6
        // - MARKET_NODE_OFFSET=7 -> port 5167, IP 1.2.3.8, data: ~/.local/share/smpc-auction-node-7
        let port_offset = std::env::var("MARKET_NODE_OFFSET")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(5);

        Self {
            network_key: "development-network-2025".to_string(),
            // Use fake global IP for bootstrap (1.2.3.1:5160)
            // The LD_PRELOAD library translates this to 127.0.0.1:5160
            // Use UDP for BOOT protocol - TCP requires VL framing which BOOT doesn't have
            bootstrap_nodes: vec!["udp://1.2.3.1:5160".to_string()],
            port_offset,
        }
    }
}

pub struct VeilidNode {
    api: Option<VeilidAPI>,
    state: Arc<RwLock<NodeState>>,
    data_dir: PathBuf,
    devnet_config: Option<DevNetConfig>,
    /// Whether to use insecure (unencrypted) protected storage.
    /// Defaults to `false` for production safety; set to `true` for devnet/test.
    insecure_storage: bool,
    update_tx: mpsc::UnboundedSender<VeilidUpdate>,
    update_rx: Option<mpsc::UnboundedReceiver<VeilidUpdate>>,
}

impl VeilidNode {
    pub fn new(data_dir: PathBuf) -> Self {
        let (update_tx, update_rx) = mpsc::unbounded_channel();
        Self {
            api: None,
            state: Arc::new(RwLock::new(NodeState::default())),
            data_dir,
            devnet_config: None,
            insecure_storage: false,
            update_tx,
            update_rx: Some(update_rx),
        }
    }

    /// Configure this node to connect to the local devnet (requires LD_PRELOAD)
    ///
    /// Automatically enables insecure storage for devnet use.
    pub fn with_devnet(mut self, config: DevNetConfig) -> Self {
        self.devnet_config = Some(config);
        self.insecure_storage = true;
        self
    }

    /// Explicitly set whether to use insecure (unencrypted) protected storage.
    ///
    /// In production this should remain `false` (the default).
    /// Devnet mode sets this to `true` automatically via [`with_devnet`].
    pub fn with_insecure_storage(mut self, insecure: bool) -> Self {
        self.insecure_storage = insecure;
        self
    }

    /// Configure this node to connect to the public Veilid network
    /// For production use - connects to real bootstrap nodes on the internet
    pub fn with_public_network(self) -> Self {
        // Leave devnet_config as None - will use default public network config
        self
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("Starting Veilid node...");

        let protected_store_dir = self.data_dir.join("protected_store");
        let table_store_dir = self.data_dir.join("table_store");

        // Create directories if they don't exist
        std::fs::create_dir_all(&protected_store_dir)
            .context("Failed to create protected_store directory")?;
        std::fs::create_dir_all(&table_store_dir)
            .context("Failed to create table_store directory")?;

        // Build network config based on whether we're connecting to devnet
        let network_config = if let Some(devnet) = &self.devnet_config {
            // Calculate our port - bind to 127.0.0.1 directly for LocalNetwork routing domain
            // No public_address means we use LocalNetwork for peer discovery
            let port = 5160 + devnet.port_offset;
            let listen_addr = format!("127.0.0.1:{}", port);

            info!(
                "Configuring for devnet: key={}, bootstrap={:?}, listen_addr={}",
                devnet.network_key, devnet.bootstrap_nodes, listen_addr
            );

            VeilidConfigNetwork {
                network_key_password: Some(devnet.network_key.clone()),
                detect_address_changes: Some(false), // Static addresses for devnet
                routing_table: VeilidConfigRoutingTable {
                    bootstrap: devnet.bootstrap_nodes.clone(),
                    bootstrap_keys: vec![], // No signature verification for devnet
                    // Lower limits for small devnet (5 nodes + 3 market instances = 8 total)
                    limit_over_attached: 8,
                    limit_fully_attached: 6,
                    limit_attached_strong: 4,
                    limit_attached_good: 3,
                    limit_attached_weak: 2, // Reach "AttachedWeak" with just 2 good peers
                    ..Default::default()
                },
                protocol: VeilidConfigProtocol {
                    udp: VeilidConfigUDP {
                        enabled: true,
                        socket_pool_size: 0,
                        listen_address: listen_addr.clone(),
                        public_address: None,
                    },
                    tcp: VeilidConfigTCP {
                        connect: true,
                        listen: true,
                        max_connections: 32,
                        listen_address: listen_addr.clone(),
                        public_address: None,
                    },
                    ws: VeilidConfigWS {
                        connect: true,
                        listen: true,
                        max_connections: 16,
                        listen_address: listen_addr.clone(),
                        path: "ws".to_string(),
                        url: None,
                    },
                },
                ..Default::default()
            }
        } else {
            info!("Configuring for public Veilid network");
            VeilidConfigNetwork::default()
        };

        // For devnet, disable capabilities that are disabled on devnet nodes
        // Match the bootstrap config: TUNL, SGNL, RLAY, DIAL disabled
        let capabilities = if self.devnet_config.is_some() {
            VeilidConfigCapabilities {
                disable: vec![
                    VEILID_CAPABILITY_SIGNAL,
                    VEILID_CAPABILITY_RELAY,
                    VEILID_CAPABILITY_VALIDATE_DIAL_INFO,
                    // Note: VEILID_CAPABILITY_TUNNEL is behind feature flag "unstable-tunnels"
                    // and not available by default, so we don't include it
                ],
            }
        } else {
            VeilidConfigCapabilities::default()
        };

        let namespace = self
            .data_dir
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| format!("smpc-auction-{}", s))
            .unwrap_or_else(|| "smpc-auction".to_string());

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
            let _ = update_tx.send(update.clone());

            // Update internal state synchronously
            match &update {
                VeilidUpdate::Network(network) => {
                    let mut state = state.write();
                    state.peer_count = network.peers.len();
                    state.node_ids = network.node_ids.iter().map(|id| id.to_string()).collect();
                    debug!(
                        "Network update: {} peers, node_ids: {:?}",
                        state.peer_count, state.node_ids
                    );
                }
                VeilidUpdate::Attachment(attachment) => {
                    let mut state = state.write();
                    state.is_attached = attachment.state.is_attached();
                    info!("Attachment state: {:?}", attachment.state);
                }
                VeilidUpdate::AppMessage(msg) => {
                    info!(
                        "Received app message: {}",
                        String::from_utf8_lossy(msg.message())
                    );
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

        let api = api_startup(update_callback, config)
            .await
            .map_err(|e| {
                error!("Veilid API startup failed: {:?}", e);
                e
            })
            .context("Failed to start Veilid API")?;

        self.api = Some(api);
        info!("Veilid node started successfully");

        Ok(())
    }

    pub async fn attach(&self) -> Result<()> {
        let api = self.api.as_ref().context("Veilid node not started")?;
        info!("Attaching to Veilid network...");
        api.attach().await.context("Failed to attach to network")?;
        info!("Attached to network");
        Ok(())
    }

    pub async fn detach(&self) -> Result<()> {
        let api = self.api.as_ref().context("Veilid node not started")?;
        info!("Detaching from Veilid network...");
        api.detach()
            .await
            .context("Failed to detach from network")?;
        info!("Detached from network");
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(api) = self.api.take() {
            info!("Shutting down Veilid node...");
            api.shutdown().await;
            info!("Veilid node shut down");
        }
        Ok(())
    }

    pub fn api(&self) -> Option<&VeilidAPI> {
        self.api.as_ref()
    }

    pub fn state(&self) -> NodeState {
        self.state.read().clone()
    }

    pub fn take_update_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<VeilidUpdate>> {
        self.update_rx.take()
    }

    /// Create a DHT operations handle for this node
    /// Returns None if the node hasn't been started yet
    pub fn dht_operations(&self) -> Option<crate::veilid::dht::DHTOperations> {
        self.api.as_ref().map(|api| {
            // Use unsafe routing for devnet (no private routes)
            // Use safe routing for public network
            let use_unsafe_routing = self.devnet_config.is_some();
            crate::veilid::dht::DHTOperations::new(api.clone(), use_unsafe_routing)
        })
    }
}

impl Drop for VeilidNode {
    fn drop(&mut self) {
        if self.api.is_some() {
            error!("VeilidNode dropped without calling shutdown()! This may cause resource leaks.");
        }
    }
}
