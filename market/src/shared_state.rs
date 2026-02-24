//! Shared application state for both the GUI and headless binaries.
//!
//! `SharedAppState` bundles all state needed to drive auction operations.
//! The GUI binary hands it to Dioxus via a `OnceLock`; the headless binary
//! constructs one directly.

use std::sync::Arc;

use crate::veilid::node::NodeState;
use crate::{AuctionCoordinator, BidStorage, DHTOperations, VeilidNode};
use parking_lot::RwLock;

/// Bundled application state shared between the background Veilid thread
/// and the command interface (GUI or headless IPC).
#[derive(Clone)]
pub struct SharedAppState {
    pub node_holder: Arc<RwLock<Option<VeilidNode>>>,
    pub bid_storage: BidStorage,
    pub coordinator: Arc<RwLock<Option<Arc<AuctionCoordinator>>>>,
}

impl SharedAppState {
    pub fn new(bid_storage: BidStorage) -> Self {
        Self {
            node_holder: Arc::new(RwLock::new(None)),
            bid_storage,
            coordinator: Arc::new(RwLock::new(None)),
        }
    }

    /// Get the current node state, or default if not yet initialized.
    pub fn get_node_state(&self) -> NodeState {
        if let Some(node) = self.node_holder.read().as_ref() {
            return node.state();
        }
        NodeState::default()
    }

    /// Get a snapshot of the coordinator (if initialized).
    pub fn coordinator(&self) -> Option<Arc<AuctionCoordinator>> {
        self.coordinator.read().clone()
    }

    /// Get DHT operations from the current node (if started).
    pub fn dht_operations(&self) -> Option<DHTOperations> {
        self.node_holder
            .read()
            .as_ref()
            .and_then(super::veilid::node::VeilidNode::dht_operations)
    }
}
