use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use veilid_core::{RouteId, SafetySelection, SafetySpec, Sequencing, Stability, Target, VeilidAPI};

use super::bid_announcement::{bincode_deserialize_limited, SignedEnvelope};
use crate::error::{MarketError, MarketResult};

/// Calculate the base port for a given node offset.
/// Formula: `5000 + (node_offset * 10)`
///
/// # Panics
/// Panics if `node_offset > 6000` (would overflow valid port range).
const fn base_port_for_offset(node_offset: u16) -> u16 {
    assert!(node_offset <= 6000, "node_offset too large for port range");
    5000 + (node_offset * 10)
}

/// Calculate the port for a specific party relative to a base port.
/// Formula: `base_port + party_id`
#[allow(clippy::cast_possible_truncation)] // party count fits in u16
const fn party_port(base_port: u16, party_id: usize) -> u16 {
    base_port + party_id as u16
}

#[derive(Serialize, Deserialize, Debug)]
enum MpcMessage {
    Open {
        source_party_id: usize,
    },
    Data {
        source_party_id: usize,
        payload: Vec<u8>,
    },
    Close {
        source_party_id: usize,
    },
}

/// Manages TCP tunnel proxying between MP-SPDZ parties over Veilid routes
#[derive(Clone)]
pub struct MpcTunnelProxy {
    inner: Arc<MpcTunnelProxyInner>,
}

struct MpcTunnelProxyInner {
    api: VeilidAPI,
    party_id: usize,
    party_routes: HashMap<usize, RouteId>,
    /// Reverse mapping: signer pubkey bytes → party id.
    /// Built from `party_signers` at construction time.
    signer_to_party: HashMap<[u8; 32], usize>,
    /// Signing key for outgoing messages.
    signing_key: SigningKey,
    base_port: u16,
    /// Active sessions: Map<RemotePartyID, TcpStream>
    /// Note: This simple model assumes one connection per party pair (which MP-SPDZ usually does).
    /// If multiple connections are needed, we need SessionIDs.
    sessions: Mutex<HashMap<usize, tokio::net::tcp::OwnedWriteHalf>>,
    /// Buffer for data received before Open message (race condition)
    pending_data: Mutex<HashMap<usize, Vec<Vec<u8>>>>,
}

impl MpcTunnelProxy {
    /// Create a new MPC tunnel proxy.
    ///
    /// `party_signers` maps party ID → Ed25519 verifying key bytes, used
    /// to authenticate incoming MPC messages (the envelope signer must match
    /// the expected party).
    pub fn new(
        api: VeilidAPI,
        party_id: usize,
        party_routes: HashMap<usize, RouteId>,
        node_offset: u16,
        signing_key: SigningKey,
        party_signers: &HashMap<usize, [u8; 32]>,
    ) -> Self {
        // Offset ports based on node to avoid conflicts when running multiple nodes on same machine
        // Node 5: base 5050
        // Node 6: base 5060
        // Node 7: base 5070
        let base_port = base_port_for_offset(node_offset);

        // Build reverse mapping: signer → party_id
        let signer_to_party: HashMap<[u8; 32], usize> = party_signers
            .iter()
            .map(|(&pid, &key)| (key, pid))
            .collect();

        info!(
            "MPC TunnelProxy for Party {}: using base port {} (node offset {}), {} authenticated peers",
            party_id, base_port, node_offset, signer_to_party.len()
        );

        Self {
            inner: Arc::new(MpcTunnelProxyInner {
                api,
                party_id,
                party_routes,
                signer_to_party,
                signing_key,
                base_port,
                sessions: Mutex::new(HashMap::new()),
                pending_data: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn run(&self) -> MarketResult<()> {
        info!("Starting MPC TunnelProxy for Party {}", self.inner.party_id);

        // Setup outgoing proxies - listen directly on MP-SPDZ ports
        for (&pid, route_id) in &self.inner.party_routes {
            if pid == self.inner.party_id {
                continue;
            }

            let listen_port = party_port(self.inner.base_port, pid);
            let proxy = self.clone();
            let route_id = route_id.clone();

            tokio::spawn(async move {
                if let Err(e) = proxy.run_outgoing_proxy(listen_port, pid, route_id).await {
                    error!("Proxy for Party {} failed: {}", pid, e);
                }
            });
        }

        Ok(())
    }

    /// Serialize an MPC message and wrap it in a signed envelope.
    fn sign_mpc_message(&self, msg: &MpcMessage) -> MarketResult<Vec<u8>> {
        let payload = bincode::serialize(msg).map_err(|e| {
            MarketError::Serialization(format!("Failed to serialize MPC message: {e}"))
        })?;
        let envelope = SignedEnvelope::sign(payload, &self.inner.signing_key)?;
        envelope.to_bytes()
    }

    /// Cleanup resources: clear active sessions to drop TCP connections.
    pub fn cleanup(&self) {
        info!(
            "Cleaning up MPC tunnel proxy for Party {}",
            self.inner.party_id
        );
        // Clear all active sessions to drop TCP connections
        // Note: We can't async here (Drop context), so we use try_lock
        if let Ok(mut sessions) = self.inner.sessions.try_lock() {
            sessions.clear();
        }
        if let Ok(mut pending) = self.inner.pending_data.try_lock() {
            pending.clear();
        }
    }

    async fn run_outgoing_proxy(
        &self,
        listen_port: u16,
        target_pid: usize,
        target_route: RouteId,
    ) -> MarketResult<()> {
        let addr = format!("127.0.0.1:{listen_port}");
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| MarketError::Network(format!("Failed to bind proxy at {addr}: {e}")))?;

        info!(
            "Listening for MP-SPDZ connection to Party {} on port {}",
            target_pid, listen_port
        );

        loop {
            let (socket, _) = listener.accept().await?;
            debug!("MP-SPDZ connected to proxy for Party {}", target_pid);

            let (mut rd, wr) = socket.into_split();

            // Register write half so we can send data received from Veilid BACK to MP-SPDZ
            {
                let mut sessions = self.inner.sessions.lock().await;
                sessions.insert(target_pid, wr);
            }

            // Create RoutingContext
            let ctx = self
                .inner
                .api
                .routing_context()?
                .with_safety(SafetySelection::Safe(SafetySpec {
                    preferred_route: None,
                    hop_count: 2, // Default
                    stability: Stability::Reliable,
                    sequencing: Sequencing::PreferOrdered,
                }))?;

            // Send Open message
            let open_msg = MpcMessage::Open {
                source_party_id: self.inner.party_id,
            };
            let data = self.sign_mpc_message(&open_msg)?;
            ctx.app_message(Target::RouteId(target_route.clone()), data)
                .await?;

            // Read loop: TCP -> Veilid
            let ctx_clone = ctx.clone();
            let target_route_clone = target_route.clone();
            let my_pid = self.inner.party_id;
            let proxy_for_send = self.clone();

            let mut buf = vec![0u8; 31000]; // Keep under 32KB with bincode envelope overhead
            loop {
                let n = match rd.read(&mut buf).await {
                    Ok(0) => break, // EOF
                    Ok(n) => n,
                    Err(e) => {
                        error!("TCP Read Error: {}", e);
                        break;
                    }
                };

                let msg = MpcMessage::Data {
                    source_party_id: my_pid,
                    payload: buf[..n].to_vec(),
                };
                let data = match proxy_for_send.sign_mpc_message(&msg) {
                    Ok(d) => d,
                    Err(e) => {
                        error!("Failed to sign MPC message: {}", e);
                        break;
                    }
                };

                if let Err(e) = ctx_clone
                    .app_message(Target::RouteId(target_route_clone.clone()), data)
                    .await
                {
                    error!("Failed to send data to Party {}: {}", target_pid, e);
                    break;
                }
            }

            // Send Close
            let close_msg = MpcMessage::Close {
                source_party_id: my_pid,
            };
            let data = match self.sign_mpc_message(&close_msg) {
                Ok(d) => d,
                Err(e) => {
                    error!("Failed to sign Close message: {}", e);
                    return Ok(());
                }
            };
            let _ = ctx_clone
                .app_message(Target::RouteId(target_route_clone), data)
                .await;

            // Cleanup
            {
                let mut sessions = self.inner.sessions.lock().await;
                sessions.remove(&target_pid);
            }
        }
    }

    /// Process incoming Veilid message (already unwrapped from [`SignedEnvelope`]).
    ///
    /// `signer` is the Ed25519 verifying key from the envelope. The method
    /// verifies that the claimed `source_party_id` matches the signer via the
    /// `signer_to_party` reverse mapping built at construction.
    #[allow(clippy::too_many_lines)]
    pub async fn process_message(&self, message: Vec<u8>, signer: [u8; 32]) -> MarketResult<()> {
        let mpc_msg: MpcMessage = bincode_deserialize_limited(&message)?;

        // Authenticate: verify the signer corresponds to the claimed party ID.
        let claimed_party = match &mpc_msg {
            MpcMessage::Open { source_party_id }
            | MpcMessage::Data {
                source_party_id, ..
            }
            | MpcMessage::Close { source_party_id } => *source_party_id,
        };
        if let Some(&expected_party) = self.inner.signer_to_party.get(&signer) {
            if expected_party != claimed_party {
                warn!(
                    "MPC tunnel auth failed: signer maps to party {} but message claims party {}",
                    expected_party, claimed_party
                );
                return Err(MarketError::Crypto("MPC tunnel party ID mismatch".into()));
            }
        } else {
            warn!(
                "MPC tunnel: unknown signer {}, rejecting message",
                hex::encode(signer)
            );
            return Err(MarketError::Crypto(
                "MPC tunnel message from unknown signer".into(),
            ));
        }

        match mpc_msg {
            MpcMessage::Open { source_party_id } => {
                info!("Received OPEN from Party {}", source_party_id);
                // Connect to local MP-SPDZ listening port
                // MP-SPDZ listens on base_port + MyPartyID ?
                // Wait, in full mesh:
                // Party i listens on port_base + i
                // Party j connects to Party i at port_base + i
                // So if I am Party 0, I listen on 5000.
                // Party 1 connects to me on 5000.

                // My local MP-SPDZ is listening on `base_port + my_party_id`?
                // Actually, standard MP-SPDZ setup:
                // -p 0 means I listen on 5000.
                // -p 1 means I listen on 5001.
                // But since we are tunneling, maybe we change this.
                // If I am Party 0, my MP-SPDZ listens on 5000.
                // Party 1's Sidecar connects to me.
                // So Party 1's Sidecar sends OPEN.
                // I (Sidecar 0) receive OPEN.
                // I should connect to `localhost:5000` (my MP-SPDZ).

                let local_target_port = party_port(self.inner.base_port, self.inner.party_id);
                let addr = format!("127.0.0.1:{local_target_port}");

                match TcpStream::connect(&addr).await {
                    Ok(stream) => {
                        // We only need the WriteHalf to write data coming FROM Veilid
                        // The ReadHalf? We don't read from this socket to send to Veilid?
                        // YES WE DO.
                        // Wait, if I am accepting a connection, it's bidirectional.
                        //
                        // Scenario: P1 connects to P0.
                        // P1 Sidecar (Proxy) -> P0 Sidecar (Receiver).
                        // P1 Sidecar reads P1-MP-SPDZ (Client) -> Sends to P0 Sidecar.
                        // P0 Sidecar writes to P0-MP-SPDZ (Server).
                        //
                        // BUT P0-MP-SPDZ writes back to P1-MP-SPDZ.
                        // So P0 Sidecar must read from P0-MP-SPDZ -> Send to P1 Sidecar.
                        //
                        // So when we connect to local MP-SPDZ, we must also spawn a read loop.

                        let (mut rd, wr) = stream.into_split();

                        {
                            let mut sessions = self.inner.sessions.lock().await;
                            sessions.insert(source_party_id, wr);
                        }

                        // Flush any buffered data that arrived before this Open message
                        let buffered = {
                            let mut pending = self.inner.pending_data.lock().await;
                            pending.remove(&source_party_id)
                        };
                        if let Some(buffered) = buffered {
                            let mut sessions = self.inner.sessions.lock().await;
                            if let Some(wr) = sessions.get_mut(&source_party_id) {
                                for payload in buffered {
                                    if let Err(e) = wr.write_all(&payload).await {
                                        error!(
                                            "Failed to flush buffered data for Party {}: {}",
                                            source_party_id, e
                                        );
                                        break;
                                    }
                                }
                                info!("Flushed buffered data for Party {}", source_party_id);
                            }
                        }

                        // Spawn read loop for the "Response" path (Server -> Client)
                        let proxy = self.clone();
                        let target_pid = source_party_id;
                        let target_route = self.inner.party_routes.get(&target_pid).cloned();
                        let my_pid = self.inner.party_id;

                        if let Some(route) = target_route {
                            tokio::spawn(async move {
                                let Ok(ctx) = proxy.inner.api.routing_context() else {
                                    return;
                                };
                                let Ok(ctx) = ctx.with_safety(SafetySelection::Safe(SafetySpec {
                                    preferred_route: None,
                                    hop_count: 2,
                                    stability: Stability::Reliable,
                                    sequencing: Sequencing::PreferOrdered,
                                })) else {
                                    return;
                                };

                                let mut buf = vec![0u8; 31000];
                                loop {
                                    let n = match rd.read(&mut buf).await {
                                        Ok(0) | Err(_) => break,
                                        Ok(n) => n,
                                    };

                                    let msg = MpcMessage::Data {
                                        source_party_id: my_pid,
                                        payload: buf[..n].to_vec(),
                                    };
                                    if let Ok(data) = proxy.sign_mpc_message(&msg) {
                                        let _ = ctx
                                            .app_message(Target::RouteId(route.clone()), data)
                                            .await;
                                    }
                                }
                            });
                        } else {
                            warn!("No route found for Party {}", target_pid);
                        }
                    }
                    Err(e) => {
                        error!("Failed to connect to local MP-SPDZ at {}: {}", addr, e);
                    }
                }
            }
            MpcMessage::Data {
                source_party_id,
                payload,
            } => {
                let mut sessions = self.inner.sessions.lock().await;
                if let Some(wr) = sessions.get_mut(&source_party_id) {
                    if let Err(e) = wr.write_all(&payload).await {
                        error!(
                            "Failed to write to local MP-SPDZ for Party {}: {}",
                            source_party_id, e
                        );
                    }
                } else {
                    // Buffer data pending Open message (race condition)
                    debug!(
                        "Buffering data from Party {} (no session yet)",
                        source_party_id
                    );
                    drop(sessions); // Release sessions lock before acquiring pending_data lock
                    let mut pending = self.inner.pending_data.lock().await;
                    let total: usize = pending.values().flat_map(|v| v.iter()).map(Vec::len).sum();
                    if total + payload.len() > 10 * 1024 * 1024 {
                        return Err(MarketError::Network(
                            "MPC pending data buffer exceeded 10MB limit".into(),
                        ));
                    }
                    pending.entry(source_party_id).or_default().push(payload);
                }
            }
            MpcMessage::Close { source_party_id } => {
                info!("Received CLOSE from Party {}", source_party_id);
                let mut sessions = self.inner.sessions.lock().await;
                sessions.remove(&source_party_id);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mpc_message_open_roundtrip() {
        let msg = MpcMessage::Open { source_party_id: 5 };
        let bytes = bincode::serialize(&msg).unwrap();
        let decoded: MpcMessage = bincode::deserialize(&bytes).unwrap();
        match decoded {
            MpcMessage::Open { source_party_id } => assert_eq!(source_party_id, 5),
            other => panic!("Expected Open, got {other:?}"),
        }
    }

    #[test]
    fn test_mpc_message_data_roundtrip() {
        let payload = vec![1, 2, 3, 4, 5];
        let msg = MpcMessage::Data {
            source_party_id: 2,
            payload: payload.clone(),
        };
        let bytes = bincode::serialize(&msg).unwrap();
        let decoded: MpcMessage = bincode::deserialize(&bytes).unwrap();
        match decoded {
            MpcMessage::Data {
                source_party_id,
                payload: decoded_payload,
            } => {
                assert_eq!(source_party_id, 2);
                assert_eq!(decoded_payload, payload);
            }
            other => panic!("Expected Data, got {other:?}"),
        }
    }

    #[test]
    fn test_mpc_message_close_roundtrip() {
        let msg = MpcMessage::Close { source_party_id: 0 };
        let bytes = bincode::serialize(&msg).unwrap();
        let decoded: MpcMessage = bincode::deserialize(&bytes).unwrap();
        match decoded {
            MpcMessage::Close { source_party_id } => assert_eq!(source_party_id, 0),
            other => panic!("Expected Close, got {other:?}"),
        }
    }

    #[test]
    fn test_mpc_message_data_large_payload() {
        let payload = vec![0xAB; 32 * 1024]; // 32KB (Veilid max)
        let msg = MpcMessage::Data {
            source_party_id: 1,
            payload: payload.clone(),
        };
        let bytes = bincode::serialize(&msg).unwrap();
        let decoded: MpcMessage = bincode::deserialize(&bytes).unwrap();
        match decoded {
            MpcMessage::Data {
                payload: decoded_payload,
                ..
            } => {
                assert_eq!(decoded_payload.len(), 32 * 1024);
                assert_eq!(decoded_payload, payload);
            }
            other => panic!("Expected Data, got {other:?}"),
        }
    }

    #[test]
    fn test_mpc_message_data_empty_payload() {
        let msg = MpcMessage::Data {
            source_party_id: 3,
            payload: vec![],
        };
        let bytes = bincode::serialize(&msg).unwrap();
        let decoded: MpcMessage = bincode::deserialize(&bytes).unwrap();
        match decoded {
            MpcMessage::Data {
                source_party_id,
                payload,
            } => {
                assert_eq!(source_party_id, 3);
                assert!(payload.is_empty());
            }
            other => panic!("Expected Data, got {other:?}"),
        }
    }

    #[test]
    fn test_base_port_calculation() {
        assert_eq!(base_port_for_offset(5), 5050);
        assert_eq!(base_port_for_offset(6), 5060);
        assert_eq!(base_port_for_offset(7), 5070);
        assert_eq!(base_port_for_offset(0), 5000);
    }

    #[test]
    fn test_listen_port_calculation() {
        let base = base_port_for_offset(5); // 5050
        assert_eq!(party_port(base, 0), 5050);
        assert_eq!(party_port(base, 1), 5051);
        assert_eq!(party_port(base, 2), 5052);

        let base = base_port_for_offset(7); // 5070
        assert_eq!(party_port(base, 0), 5070);
        assert_eq!(party_port(base, 3), 5073);
    }
}
