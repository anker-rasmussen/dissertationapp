//! Mock message transport for testing.

use crate::error::{MarketError, MarketResult};
use crate::traits::{MessageTransport, TransportTarget};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use veilid_core::{PublicKey, RouteBlob, RouteId};

/// A recorded message for test assertions.
#[derive(Debug, Clone)]
pub struct RecordedMessage {
    pub target: TransportTarget,
    pub data: Vec<u8>,
    pub sequence_number: u64,
}

/// Mock transport for testing message sending.
#[derive(Debug, Clone)]
pub struct MockTransport {
    /// All messages that have been sent.
    sent_messages: Arc<RwLock<Vec<RecordedMessage>>>,
    /// Counter for generating unique route IDs.
    route_counter: Arc<AtomicU64>,
    /// Simulated peers.
    peers: Arc<RwLock<Vec<PublicKey>>>,
    /// Created routes: Map<route_id_string, RouteBlob>
    routes: Arc<RwLock<HashMap<String, RouteBlob>>>,
    /// Whether to simulate failures.
    fail_sends: Arc<RwLock<bool>>,
    /// Message counter for timestamps.
    message_counter: Arc<AtomicU64>,
}

impl MockTransport {
    /// Create a new mock transport.
    pub fn new() -> Self {
        Self {
            sent_messages: Arc::new(RwLock::new(Vec::new())),
            route_counter: Arc::new(AtomicU64::new(1)),
            peers: Arc::new(RwLock::new(Vec::new())),
            routes: Arc::new(RwLock::new(HashMap::new())),
            fail_sends: Arc::new(RwLock::new(false)),
            message_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Add a simulated peer.
    pub async fn add_peer(&self, peer: PublicKey) {
        self.peers.write().await.push(peer);
    }

    /// Set whether sends should fail.
    pub async fn set_fail_sends(&self, fail: bool) {
        *self.fail_sends.write().await = fail;
    }

    /// Get all sent messages.
    pub async fn get_sent_messages(&self) -> Vec<RecordedMessage> {
        self.sent_messages.read().await.clone()
    }

    /// Get messages sent to a specific target type.
    pub async fn get_messages_to_nodes(&self) -> Vec<RecordedMessage> {
        self.sent_messages
            .read()
            .await
            .iter()
            .filter(|m| matches!(m.target, TransportTarget::Node(_)))
            .cloned()
            .collect()
    }

    /// Clear all recorded messages.
    pub async fn clear_messages(&self) {
        self.sent_messages.write().await.clear();
    }

    /// Get the number of messages sent.
    pub async fn message_count(&self) -> usize {
        self.sent_messages.read().await.len()
    }

    /// Create a mock RouteId from a counter value.
    fn make_route_id(counter: u64) -> RouteId {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&counter.to_le_bytes());
        // Fill with pattern
        for (j, byte) in bytes[8..32].iter_mut().enumerate() {
            *byte = ((counter >> ((j % 8) * 8)) & 0xFF) as u8;
        }

        let encoded = data_encoding::BASE64URL_NOPAD.encode(&bytes);
        let key_str = format!("VLD0:{encoded}");
        #[allow(clippy::expect_used)]
        RouteId::try_from(key_str.as_str()).expect("Should create valid RouteId")
    }
}

impl Default for MockTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageTransport for MockTransport {
    async fn send(&self, target: TransportTarget, message: Vec<u8>) -> MarketResult<()> {
        if *self.fail_sends.read().await {
            return Err(MarketError::Transport("simulated send failure".into()));
        }

        let sequence_number = self.message_counter.fetch_add(1, Ordering::SeqCst);
        self.sent_messages.write().await.push(RecordedMessage {
            target,
            data: message,
            sequence_number,
        });

        Ok(())
    }

    async fn create_private_route(&self) -> MarketResult<(RouteId, RouteBlob)> {
        let counter = self.route_counter.fetch_add(1, Ordering::SeqCst);
        let route_id = Self::make_route_id(counter);

        // Create a mock RouteBlob
        let blob = RouteBlob {
            route_id: route_id.clone(),
            blob: format!("mock_route_blob_{counter}").into_bytes(),
        };

        self.routes
            .write()
            .await
            .insert(route_id.to_string(), blob.clone());

        Ok((route_id, blob))
    }

    fn import_remote_route(&self, blob: RouteBlob) -> MarketResult<RouteId> {
        Ok(blob.route_id)
    }

    async fn get_peers(&self) -> MarketResult<Vec<PublicKey>> {
        Ok(self.peers.read().await.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_transport_send() {
        let transport = MockTransport::new();

        // Create a mock target
        let route_id = MockTransport::make_route_id(1);

        transport
            .send(TransportTarget::Route(route_id), b"hello".to_vec())
            .await
            .unwrap();

        let messages = transport.get_sent_messages().await;
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].data, b"hello".to_vec());
    }

    #[tokio::test]
    async fn test_mock_transport_fail_mode() {
        let transport = MockTransport::new();
        let route_id = MockTransport::make_route_id(1);

        transport.set_fail_sends(true).await;

        let result = transport
            .send(TransportTarget::Route(route_id), b"hello".to_vec())
            .await;

        assert!(result.is_err());
        assert_eq!(transport.message_count().await, 0);
    }

    #[tokio::test]
    async fn test_mock_transport_create_route() {
        let transport = MockTransport::new();

        let (route_id1, blob1) = transport.create_private_route().await.unwrap();
        let (route_id2, blob2) = transport.create_private_route().await.unwrap();

        assert_ne!(route_id1, route_id2);
        assert_ne!(blob1.blob, blob2.blob);
    }

    #[tokio::test]
    async fn test_mock_transport_broadcast() {
        use crate::mocks::make_test_public_key;

        let transport = MockTransport::new();

        // Add some peers
        let peer1 = make_test_public_key(1);
        let peer2 = make_test_public_key(2);

        transport.add_peer(peer1).await;
        transport.add_peer(peer2).await;

        let sent_count = transport.broadcast(b"broadcast".to_vec()).await.unwrap();
        assert_eq!(sent_count, 2);

        let messages = transport.get_messages_to_nodes().await;
        assert_eq!(messages.len(), 2);
    }

    #[tokio::test]
    async fn test_mock_transport_import_route() {
        let transport = MockTransport::new();

        // Create a route blob
        let (_route_id, blob) = transport.create_private_route().await.unwrap();

        // Import the route
        let imported_route_id = transport.import_remote_route(blob.clone()).unwrap();

        // The imported route ID should match the blob's route ID
        assert_eq!(imported_route_id, blob.route_id);
    }

    #[tokio::test]
    async fn test_mock_transport_get_peers() {
        use crate::mocks::make_test_public_key;

        let transport = MockTransport::new();

        // Initially no peers
        let peers = transport.get_peers().await.unwrap();
        assert!(peers.is_empty());

        // Add some peers
        let peer1 = make_test_public_key(1);
        let peer2 = make_test_public_key(2);
        let peer3 = make_test_public_key(3);

        transport.add_peer(peer1.clone()).await;
        transport.add_peer(peer2.clone()).await;
        transport.add_peer(peer3.clone()).await;

        // Get peers
        let peers = transport.get_peers().await.unwrap();
        assert_eq!(peers.len(), 3);
        assert!(peers.contains(&peer1));
        assert!(peers.contains(&peer2));
        assert!(peers.contains(&peer3));
    }

    #[tokio::test]
    async fn test_mock_transport_clear_messages() {
        let transport = MockTransport::new();
        let route_id = MockTransport::make_route_id(1);

        transport
            .send(TransportTarget::Route(route_id.clone()), b"msg1".to_vec())
            .await
            .unwrap();
        transport
            .send(TransportTarget::Route(route_id), b"msg2".to_vec())
            .await
            .unwrap();

        assert_eq!(transport.message_count().await, 2);

        transport.clear_messages().await;

        assert_eq!(transport.message_count().await, 0);
        assert!(transport.get_sent_messages().await.is_empty());
    }

    #[tokio::test]
    async fn test_mock_transport_default() {
        let transport = MockTransport::default();

        let route_id = MockTransport::make_route_id(1);
        transport
            .send(TransportTarget::Route(route_id), b"test".to_vec())
            .await
            .unwrap();

        assert_eq!(transport.message_count().await, 1);
    }

    #[tokio::test]
    async fn test_mock_transport_sequence_numbers() {
        let transport = MockTransport::new();
        let route_id = MockTransport::make_route_id(1);

        transport
            .send(TransportTarget::Route(route_id.clone()), b"msg1".to_vec())
            .await
            .unwrap();
        transport
            .send(TransportTarget::Route(route_id.clone()), b"msg2".to_vec())
            .await
            .unwrap();
        transport
            .send(TransportTarget::Route(route_id), b"msg3".to_vec())
            .await
            .unwrap();

        let messages = transport.get_sent_messages().await;

        // Verify sequence numbers are incrementing
        assert_eq!(messages[0].sequence_number, 0);
        assert_eq!(messages[1].sequence_number, 1);
        assert_eq!(messages[2].sequence_number, 2);
    }
}
