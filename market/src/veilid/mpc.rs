use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use veilid_core::{RouteId, SafetySelection, SafetySpec, Sequencing, Stability, Target, VeilidAPI};

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
    base_port: u16,
    /// Active sessions: Map<RemotePartyID, TcpStream>
    /// Note: This simple model assumes one connection per party pair (which MP-SPDZ usually does).
    /// If multiple connections are needed, we need SessionIDs.
    sessions: Mutex<HashMap<usize, tokio::net::tcp::OwnedWriteHalf>>,
}

impl MpcTunnelProxy {
    pub fn new(
        api: VeilidAPI,
        party_id: usize,
        party_routes: HashMap<usize, RouteId>,
        node_offset: u16,
    ) -> Self {
        // Offset ports based on node to avoid conflicts when running multiple nodes on same machine
        // Node 5: base 5050
        // Node 6: base 5060
        // Node 7: base 5070
        let base_port = 5000 + (node_offset * 10);

        info!(
            "MPC TunnelProxy for Party {}: using base port {} (node offset {})",
            party_id, base_port, node_offset
        );

        Self {
            inner: Arc::new(MpcTunnelProxyInner {
                api,
                party_id,
                party_routes,
                base_port,
                sessions: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn run(&self) -> Result<()> {
        info!("Starting MPC TunnelProxy for Party {}", self.inner.party_id);

        // Setup outgoing proxies - listen directly on MP-SPDZ ports
        for (&pid, route_id) in &self.inner.party_routes {
            if pid == self.inner.party_id {
                continue;
            }

            #[allow(clippy::cast_possible_truncation)] // party count fits in u16
            let listen_port = self.inner.base_port + (pid as u16);
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

    /// Cleanup resources (no-op since we no longer spawn external processes)
    pub fn cleanup(&self) {
        info!(
            "Cleaning up MPC tunnel proxy for Party {}",
            self.inner.party_id
        );
    }

    async fn run_outgoing_proxy(
        &self,
        listen_port: u16,
        target_pid: usize,
        target_route: RouteId,
    ) -> Result<()> {
        let addr = format!("127.0.0.1:{listen_port}");
        let listener = TcpListener::bind(&addr)
            .await
            .context(format!("Failed to bind proxy at {addr}"))?;

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
                .routing_context()
                .map_err(|e| anyhow::anyhow!("Failed to create routing context: {e}"))?
                .with_safety(SafetySelection::Safe(SafetySpec {
                    preferred_route: None,
                    hop_count: 2, // Default
                    stability: Stability::Reliable,
                    sequencing: Sequencing::PreferOrdered,
                }))
                .map_err(|e| anyhow::anyhow!("Failed to set safety: {e}"))?;

            // Send Open message
            let open_msg = MpcMessage::Open {
                source_party_id: self.inner.party_id,
            };
            let data = bincode::serialize(&open_msg)?;
            ctx.app_message(Target::RouteId(target_route.clone()), data)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to send Open: {e}"))?;

            // Read loop: TCP -> Veilid
            let ctx_clone = ctx.clone();
            let target_route_clone = target_route.clone();
            let my_pid = self.inner.party_id;

            let mut buf = vec![0u8; 32000]; // Keep under 32KB
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
                let data = bincode::serialize(&msg)?;

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
            let data = bincode::serialize(&close_msg)?;
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

    /// Process incoming Veilid message (AppMessage)
    pub async fn process_message(&self, message: Vec<u8>) -> Result<()> {
        let mpc_msg: MpcMessage = bincode::deserialize(&message)?;

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

                #[allow(clippy::cast_possible_truncation)] // party count fits in u16
                let local_target_port = self.inner.base_port + (self.inner.party_id as u16);
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

                                let mut buf = vec![0u8; 32000];
                                loop {
                                    let n = match rd.read(&mut buf).await {
                                        Ok(0) | Err(_) => break,
                                        Ok(n) => n,
                                    };

                                    let msg = MpcMessage::Data {
                                        source_party_id: my_pid,
                                        payload: buf[..n].to_vec(),
                                    };
                                    if let Ok(data) = bincode::serialize(&msg) {
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
                        // Close session?
                    }
                } else {
                    // warn!("Received Data for unknown session from Party {}", source_party_id);
                    // This might happen if OPEN hasn't processed yet or race condition.
                    // For now, ignore.
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
