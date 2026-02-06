//! Global application state management.

use std::sync::Arc;

use market::veilid::node::NodeState;
use market::{AuctionCoordinator, BidStorage, VeilidNode};
use parking_lot::RwLock;

/// Global node holder for UI access.
pub static NODE: once_cell::sync::OnceCell<Arc<RwLock<Option<VeilidNode>>>> =
    once_cell::sync::OnceCell::new();

/// Global bid storage for keeping bid values.
pub static BID_STORAGE: once_cell::sync::OnceCell<BidStorage> = once_cell::sync::OnceCell::new();

/// Global auction coordinator (handles MPC sidecars dynamically per auction).
pub static AUCTION_COORDINATOR: once_cell::sync::OnceCell<Arc<AuctionCoordinator>> =
    once_cell::sync::OnceCell::new();

/// Get the current node state, or default if not yet initialized.
pub fn get_node_state() -> NodeState {
    if let Some(node_holder) = NODE.get() {
        if let Some(node) = node_holder.read().as_ref() {
            return node.state();
        }
    }
    NodeState::default()
}
