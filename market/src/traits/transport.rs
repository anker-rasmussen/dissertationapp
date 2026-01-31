//! Message transport abstraction for testable network operations.

use anyhow::Result;
use async_trait::async_trait;
use veilid_core::{PublicKey, RouteBlob, RouteId};

/// Target for sending messages.
#[derive(Debug, Clone)]
pub enum TransportTarget {
    /// Send to a specific node by its public key.
    Node(PublicKey),
    /// Send to a private route.
    Route(RouteId),
}

/// Abstraction over message transport operations.
///
/// This trait enables testing of network-dependent code without requiring
/// actual network connections.
#[async_trait]
pub trait MessageTransport: Send + Sync + Clone {
    /// Send a message to the specified target.
    async fn send(&self, target: TransportTarget, message: Vec<u8>) -> Result<()>;

    /// Create a new private route for receiving messages.
    ///
    /// Returns the route ID and the route blob that can be shared with others.
    async fn create_private_route(&self) -> Result<(RouteId, RouteBlob)>;

    /// Import a remote private route so we can send to it.
    ///
    /// Returns the route ID that can be used as a target.
    fn import_remote_route(&self, blob: RouteBlob) -> Result<RouteId>;

    /// Get all connected peer node IDs.
    async fn get_peers(&self) -> Result<Vec<PublicKey>>;

    /// Broadcast a message to all connected peers.
    async fn broadcast(&self, message: Vec<u8>) -> Result<usize> {
        let peers = self.get_peers().await?;
        let mut sent_count = 0;
        for peer in peers {
            if self
                .send(TransportTarget::Node(peer), message.clone())
                .await
                .is_ok()
            {
                sent_count += 1;
            }
        }
        Ok(sent_count)
    }
}
