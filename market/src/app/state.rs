//! Application state management.
//!
//! Provides a single `SharedAppState` struct that bundles all shared state
//! and is handed from the main thread to the Dioxus UI via a `OnceCell`.
//! Inside the Dioxus tree, components access it through `use_context`.

use std::sync::Arc;

use market::veilid::node::NodeState;
use market::{AuctionCoordinator, BidStorage, VeilidNode};
use parking_lot::RwLock;

/// Bundled application state shared between the background Veilid thread
/// and the Dioxus UI.
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
    pub fn dht_operations(&self) -> Option<market::DHTOperations> {
        self.node_holder
            .read()
            .as_ref()
            .and_then(|node| node.dht_operations())
    }
}

/// Single static for the main -> Dioxus handoff.
pub static SHARED_STATE: std::sync::OnceLock<SharedAppState> = std::sync::OnceLock::new();
