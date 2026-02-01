//! Common utilities for integration tests.
//!
//! Provides helpers for connecting to the devnet and spawning test nodes.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use market::veilid::{BidStorage, DHTOperations, VeilidNode};
use market::veilid::node::DevNetConfig;
use veilid_core::PublicKey;

/// Base port offset for integration tests.
/// Devnet uses ports 5160-5164 (offsets 0-4), so tests start at offset 5.
/// Maximum offset is 39 (port 5199) due to ip_spoof library limit.
pub const TEST_PORT_OFFSET_BASE: u16 = 5;

/// Configuration for a test node in the auction.
#[derive(Debug, Clone)]
pub struct TestNodeConfig {
    /// Unique identifier for this test node
    pub id: usize,
    /// Port offset from base (5160 + TEST_PORT_OFFSET_BASE + id)
    pub port_offset: u16,
    /// Bid value this node will submit
    pub bid_value: u64,
    /// Data directory for this node
    pub data_dir: PathBuf,
}

/// Maximum port offset supported by ip_spoof library (40 ports: 5160-5199)
const MAX_PORT_OFFSET: u16 = 39;

impl TestNodeConfig {
    pub fn new(id: usize, bid_value: u64) -> Self {
        // Use modulo to keep port offset within valid range (5-39)
        // This allows test IDs to be any value while staying in valid port range
        let port_offset = TEST_PORT_OFFSET_BASE + (id as u16 % (MAX_PORT_OFFSET - TEST_PORT_OFFSET_BASE + 1));
        let data_dir = std::env::temp_dir()
            .join("market-integration-tests")
            .join(format!("node-{}", id));

        Self {
            id,
            port_offset,
            bid_value,
            data_dir,
        }
    }
}

/// A running test node with its Veilid connection.
pub struct TestNode {
    pub config: TestNodeConfig,
    pub node: VeilidNode,
    pub dht: DHTOperations,
    pub bid_storage: BidStorage,
    pub node_id: PublicKey,
}

impl TestNode {
    /// Create and start a new test node.
    pub async fn spawn(config: TestNodeConfig) -> Result<Self> {
        // Clean up any existing data
        if config.data_dir.exists() {
            std::fs::remove_dir_all(&config.data_dir)
                .context("Failed to clean test data directory")?;
        }
        std::fs::create_dir_all(&config.data_dir)
            .context("Failed to create test data directory")?;

        let devnet_config = DevNetConfig {
            network_key: "development-network-2025".to_string(),
            bootstrap_nodes: vec!["udp://1.2.3.1:5160".to_string()],
            port_offset: config.port_offset,
        };

        let mut node = VeilidNode::new(config.data_dir.clone())
            .with_devnet(devnet_config);

        node.start().await.context("Failed to start Veilid node")?;
        node.attach().await.context("Failed to attach to network")?;

        // Wait for network attachment
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(60);

        loop {
            let state = node.state();
            if state.is_attached && state.peer_count >= 2 {
                break;
            }
            if start.elapsed() > timeout {
                anyhow::bail!(
                    "Timeout waiting for network attachment (attached={}, peers={})",
                    state.is_attached,
                    state.peer_count
                );
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let dht = node.dht_operations().context("DHT not available")?;
        let bid_storage = BidStorage::new();

        // Get our node ID from the node state
        let state = node.state();
        let node_id_str = state.node_ids
            .first()
            .context("No node IDs available")?;
        let node_id = PublicKey::try_from(node_id_str.as_str())
            .map_err(|e| anyhow::anyhow!("Invalid node ID: {}", e))?;

        println!(
            "[Node {}] Started with ID {} on port {}",
            config.id,
            node_id,
            5160 + config.port_offset
        );

        Ok(Self {
            config,
            node,
            dht,
            bid_storage,
            node_id,
        })
    }

    /// Shutdown this test node.
    pub async fn shutdown(mut self) -> Result<()> {
        self.node.detach().await.ok();
        self.node.shutdown().await?;

        // Clean up data directory
        if self.config.data_dir.exists() {
            std::fs::remove_dir_all(&self.config.data_dir).ok();
        }

        Ok(())
    }
}

/// A collection of test nodes for multi-party tests.
pub struct TestCluster {
    pub nodes: Vec<TestNode>,
}

impl TestCluster {
    /// Spawn a cluster of test nodes with the given bid values.
    ///
    /// # Arguments
    /// * `bid_values` - Bid value for each node (length determines node count)
    /// * `offset_base` - Starting offset to avoid port conflicts between tests
    pub async fn spawn(bid_values: &[u64], offset_base: usize) -> Result<Self> {
        let mut nodes = Vec::with_capacity(bid_values.len());

        for (i, &bid_value) in bid_values.iter().enumerate() {
            let config = TestNodeConfig {
                id: offset_base + i,
                port_offset: TEST_PORT_OFFSET_BASE + (offset_base + i) as u16,
                bid_value,
                data_dir: std::env::temp_dir()
                    .join("market-integration-tests")
                    .join(format!("node-{}", offset_base + i)),
            };

            let node = TestNode::spawn(config).await
                .with_context(|| format!("Failed to spawn node {}", i))?;
            nodes.push(node);

            // Small delay between node spawns to avoid overwhelming the network
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Wait for all nodes to see each other
        println!("Waiting for cluster to stabilize ({} nodes)...", nodes.len());
        tokio::time::sleep(Duration::from_secs(3)).await;

        Ok(Self { nodes })
    }

    /// Get the number of nodes in the cluster.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if the cluster is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Get a reference to a node by index.
    pub fn get(&self, index: usize) -> Option<&TestNode> {
        self.nodes.get(index)
    }

    /// Get a mutable reference to a node by index.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut TestNode> {
        self.nodes.get_mut(index)
    }

    /// Shutdown all nodes in the cluster.
    pub async fn shutdown(self) -> Result<()> {
        for node in self.nodes {
            node.shutdown().await.ok();
        }
        Ok(())
    }

    /// Find the node with the highest bid.
    pub fn expected_winner(&self) -> Option<&TestNode> {
        self.nodes.iter().max_by_key(|n| n.config.bid_value)
    }

    /// Get all node IDs sorted (for deterministic party assignment).
    pub fn sorted_node_ids(&self) -> Vec<PublicKey> {
        let mut ids: Vec<_> = self.nodes.iter().map(|n| n.node_id.clone()).collect();
        ids.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
        ids
    }
}

/// Check if the devnet is running by attempting to parse docker output.
pub fn check_devnet_running() -> bool {
    std::process::Command::new("docker")
        .args(["ps", "--filter", "name=veilid-dev", "--format", "{{.Names}}"])
        .output()
        .map(|output| {
            let stdout = String::from_utf8_lossy(&output.stdout);
            stdout.contains("veilid-dev-bootstrap")
        })
        .unwrap_or(false)
}

/// Skip test if devnet is not running.
#[macro_export]
macro_rules! require_devnet {
    () => {
        if !$crate::common::check_devnet_running() {
            eprintln!("Skipping test: devnet not running");
            eprintln!("Start devnet with: cd /path/to/veilid && docker-compose -f .devcontainer/compose/docker-compose.dev.yml up -d");
            return Ok(());
        }
    };
}

/// Generate random bid values for testing.
pub fn generate_bid_values(count: usize, min: u64, max: u64) -> Vec<u64> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..count).map(|_| rng.gen_range(min..=max)).collect()
}

/// Generate bid values where one specific index wins.
pub fn generate_bid_values_with_winner(count: usize, winner_index: usize, base: u64) -> Vec<u64> {
    (0..count)
        .map(|i| {
            if i == winner_index {
                base + 1000 // Winner has highest bid
            } else {
                base + (i as u64 * 10) // Others have lower bids
            }
        })
        .collect()
}
