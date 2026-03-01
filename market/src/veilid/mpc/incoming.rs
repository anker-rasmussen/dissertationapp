//! Incoming MPC tunnel operations: message processing and connection handling.

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

use super::{party_port, MpcMessage, MpcTunnelProxy, SessionKey, SEND_PIPELINE_DEPTH};
use crate::error::{MarketError, MarketResult};
use crate::veilid::bid_announcement::bincode_deserialize_limited;

impl MpcTunnelProxy {
    /// Process incoming MPC message bytes (inner `MpcMessage` already extracted
    /// from the [`MpcEnvelope`] by the coordinator's routing layer).
    ///
    /// `signer` is the Ed25519 verifying key from the envelope. The method
    /// verifies that the claimed `source_party_id` matches the signer via the
    /// `signer_to_party` reverse mapping built at construction.
    #[allow(clippy::too_many_lines)]
    #[tracing::instrument(skip_all)]
    pub async fn process_message(&self, message: Vec<u8>, signer: [u8; 32]) -> MarketResult<()> {
        let mpc_msg: MpcMessage = bincode_deserialize_limited(&message)?;

        // Authenticate: verify the signer corresponds to the claimed party ID.
        let claimed_party = match &mpc_msg {
            MpcMessage::Open {
                source_party_id, ..
            }
            | MpcMessage::Data {
                source_party_id, ..
            }
            | MpcMessage::Close {
                source_party_id, ..
            }
            | MpcMessage::Ping {
                source_party_id, ..
            }
            | MpcMessage::RouteUpdate {
                source_party_id, ..
            } => *source_party_id,
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
            MpcMessage::Open {
                source_party_id,
                stream_id,
            } => {
                self.handle_open(source_party_id, stream_id).await?;
            }
            MpcMessage::Data {
                source_party_id,
                stream_id,
                seq,
                payload,
            } => {
                self.handle_data(source_party_id, stream_id, seq, payload)
                    .await?;
            }
            MpcMessage::Close {
                source_party_id,
                stream_id,
            } => {
                let session_key: SessionKey = (source_party_id, stream_id);
                info!(
                    "Received CLOSE from Party {} stream {}",
                    source_party_id, stream_id
                );
                let mut sessions = self.inner.sessions.lock().await;
                sessions.remove(&session_key);
                drop(sessions);
                let mut pending = self.inner.pending_data.lock().await;
                pending.remove(&session_key);
            }
            MpcMessage::Ping { source_party_id } => {
                debug!("Received Ping from Party {}", source_party_id);
                let was_new = self
                    .inner
                    .syn_ack_received
                    .lock()
                    .await
                    .insert(source_party_id);
                self.inner.syn_ack_notify.notify_waiters();

                // Echo back a Ping so the sender also sees us (ACK).
                // Only on first receipt to avoid infinite ping-pong.
                if was_new {
                    let ack = MpcMessage::Ping {
                        source_party_id: self.inner.party_id,
                    };
                    if let Ok(data) = self.sign_mpc_message(&ack) {
                        let label = format!("Ack P{}→P{}", self.inner.party_id, source_party_id);
                        let proxy = self.clone();
                        tokio::spawn(async move {
                            proxy.send_reliable(source_party_id, &data, &label).await;
                        });
                    }
                }
            }
            MpcMessage::RouteUpdate {
                source_party_id,
                route_blob,
            } => {
                info!(
                    "Received RouteUpdate from Party {} ({} bytes)",
                    source_party_id,
                    route_blob.len()
                );
                match self
                    .inner
                    .api
                    .import_remote_private_route(route_blob.clone())
                {
                    Ok(new_route) => {
                        self.inner
                            .party_routes
                            .lock()
                            .await
                            .insert(source_party_id, new_route.clone());
                        self.inner
                            .party_blobs
                            .lock()
                            .await
                            .insert(source_party_id, route_blob);
                        info!(
                            "Updated route for Party {} to {}",
                            source_party_id, new_route
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to import RouteUpdate blob from Party {}: {}",
                            source_party_id, e
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// Handle an incoming Open message: connect to local MP-SPDZ and set up session.
    #[allow(clippy::too_many_lines)]
    async fn handle_open(&self, source_party_id: usize, stream_id: u64) -> MarketResult<()> {
        let session_key: SessionKey = (source_party_id, stream_id);

        // Dedup: if a session already exists, this Open is a retransmit.
        if self.inner.sessions.lock().await.contains_key(&session_key) {
            debug!(
                "Duplicate Open for ({}, {}), ignoring",
                source_party_id, stream_id
            );
            return Ok(());
        }

        info!(
            "Received OPEN from Party {} stream {}",
            source_party_id, stream_id
        );

        let local_target_port = party_port(self.inner.base_port, self.inner.party_id);
        let addr = format!("127.0.0.1:{local_target_port}");

        // Serialize connections to local MP-SPDZ.  The mutex ensures one TCP
        // connect at a time.  The delay after connect gives the ServerSocket
        // time to accept() and read the identity before we release the lock.
        let stream = {
            let _guard = self.inner.local_connect_mutex.lock().await;
            let mut attempt = 0u8;
            let result = loop {
                match TcpStream::connect(&addr).await {
                    Ok(s) => break Some(s),
                    Err(e) if attempt < 30 => {
                        debug!(
                            "Local MP-SPDZ not ready at {} (attempt {}): {}",
                            addr,
                            attempt + 1,
                            e
                        );
                        attempt += 1;
                        tokio::time::sleep(std::time::Duration::from_secs(
                            crate::config::MPC_ACCEPT_RETRY_SECS,
                        ))
                        .await;
                    }
                    Err(e) => {
                        error!(
                            "Failed to connect to local MP-SPDZ at {} after {} attempts: {}",
                            addr,
                            attempt + 1,
                            e
                        );
                        break None;
                    }
                }
            };
            if result.is_some() {
                tokio::time::sleep(std::time::Duration::from_millis(
                    crate::config::MPC_POST_ACCEPT_DELAY_MS,
                ))
                .await;
            }
            result
        };

        if let Some(stream) = stream {
            info!(
                "Open: connected to local MP-SPDZ at {} for ({}, {})",
                addr, source_party_id, stream_id
            );
            let (mut rd, wr) = stream.into_split();

            let session_count = {
                let mut sessions = self.inner.sessions.lock().await;
                sessions.insert(session_key, wr);
                sessions.len()
            };
            info!(
                "Open handler: session ({}, {}) created (total: {})",
                source_party_id, stream_id, session_count
            );

            // Flush any buffered data that arrived before this Open.
            let buffered = {
                let mut pending = self.inner.pending_data.lock().await;
                pending.remove(&session_key)
            };
            if let Some(buffered) = buffered {
                let mut to_write = Vec::new();
                for (seq, payload) in buffered {
                    let ordered = self.deliver_ordered(session_key, seq, payload).await;
                    to_write.extend(ordered);
                }
                if !to_write.is_empty() {
                    info!(
                        "Open: flushing {} pending payloads ({} bytes total) for ({}, {})",
                        to_write.len(),
                        to_write.iter().map(Vec::len).sum::<usize>(),
                        source_party_id,
                        stream_id
                    );
                    let mut sessions = self.inner.sessions.lock().await;
                    if let Some(wr) = sessions.get_mut(&session_key) {
                        for payload in to_write {
                            if let Err(e) = wr.write_all(&payload).await {
                                error!(
                                    "Failed to flush buffered data for ({}, {}): {}",
                                    source_party_id, stream_id, e
                                );
                                break;
                            }
                        }
                    }
                }
            }

            // Spawn response read loop (local MP-SPDZ → Veilid → source party)
            let proxy = self.clone();
            let target_pid = source_party_id;
            let my_pid = self.inner.party_id;

            tokio::spawn(async move {
                let mut buf = vec![0u8; 31000];
                let reply_sem = Arc::new(Semaphore::new(SEND_PIPELINE_DEPTH as usize));
                loop {
                    let n = match rd.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };

                    debug!(
                        "Response relay: read {} bytes from local MP-SPDZ for ({}, {})",
                        n, target_pid, stream_id
                    );

                    let seq = proxy.next_send_seq(target_pid, stream_id).await;
                    let msg = MpcMessage::Data {
                        source_party_id: my_pid,
                        stream_id,
                        seq,
                        payload: buf[..n].to_vec(),
                    };
                    let Ok(data) = proxy.sign_mpc_message(&msg) else {
                        break;
                    };

                    let label = format!("Reply P{my_pid}→P{target_pid} s{stream_id} seq{seq}");

                    let Ok(permit) = Arc::clone(&reply_sem).acquire_owned().await else {
                        break;
                    };
                    let p = proxy.clone();
                    tokio::spawn(async move {
                        p.send_reliable(target_pid, &data, &label).await;
                        drop(permit);
                    });
                }
                // Drain reply pipeline
                let _ = reply_sem.acquire_many(SEND_PIPELINE_DEPTH).await;
            });
        }
        Ok(())
    }

    /// Handle an incoming Data message: reorder and write to local session.
    async fn handle_data(
        &self,
        source_party_id: usize,
        stream_id: u64,
        seq: u64,
        payload: Vec<u8>,
    ) -> MarketResult<()> {
        let data_start = std::time::Instant::now();
        let session_key: SessionKey = (source_party_id, stream_id);

        let session_exists = self.inner.sessions.lock().await.contains_key(&session_key);

        if !session_exists {
            // Session not yet open — buffer with seq for reorder on flush
            debug!(
                "Data: buffering seq {} ({} bytes) for pending session ({}, {})",
                seq,
                payload.len(),
                source_party_id,
                stream_id
            );
            let mut pending = self.inner.pending_data.lock().await;
            let total: usize = pending
                .values()
                .flat_map(|v| v.iter())
                .map(|(_, p)| p.len())
                .sum();
            if total + payload.len() > 10 * 1024 * 1024 {
                return Err(MarketError::Network(
                    "MPC pending data buffer exceeded 10MB limit".into(),
                ));
            }
            pending.entry(session_key).or_default().push((seq, payload));
            drop(pending);
            return Ok(());
        }

        // Reorder: deliver_ordered returns payloads ready to write in seq order
        let t_reorder = std::time::Instant::now();
        let ordered = self.deliver_ordered(session_key, seq, payload).await;
        let reorder_elapsed = t_reorder.elapsed();
        if ordered.is_empty() {
            debug!(
                "Data handler ({}, {}) seq {}: reorder {:?}, buffered (total {:?})",
                source_party_id,
                stream_id,
                seq,
                reorder_elapsed,
                data_start.elapsed()
            );
            return Ok(());
        }

        {
            let mut sessions = self.inner.sessions.lock().await;
            if let Some(wr) = sessions.get_mut(&session_key) {
                for payload in ordered {
                    debug!(
                        "Data: writing {} bytes to session ({}, {})",
                        payload.len(),
                        source_party_id,
                        stream_id
                    );
                    if let Err(e) = wr.write_all(&payload).await {
                        error!(
                            "Failed to write to local MP-SPDZ for ({}, {}): {}",
                            source_party_id, stream_id, e
                        );
                        break;
                    }
                }
            }
        }
        debug!(
            "Data handler ({}, {}) seq {}: reorder {:?}, total {:?}",
            source_party_id,
            stream_id,
            seq,
            reorder_elapsed,
            data_start.elapsed()
        );
        Ok(())
    }

    /// Process an incoming `AppCall` for MPC tunnel messages.
    ///
    /// `message` contains the inner `MpcMessage` bytes (already extracted from
    /// the [`MpcEnvelope`] by the coordinator's routing layer).
    ///
    /// All MPC data is sent via `app_call` for confirmed delivery.
    /// Returns ACK `[0x01]` on success so the sender knows the data arrived.
    #[tracing::instrument(skip_all)]
    pub async fn process_call(&self, message: Vec<u8>, signer: [u8; 32]) -> MarketResult<Vec<u8>> {
        let call_start = std::time::Instant::now();
        // Peek at message type — Open still needs async spawn (30s+ TCP connect).
        let is_open = {
            let peek: MpcMessage = bincode_deserialize_limited(&message)?;
            matches!(peek, MpcMessage::Open { .. })
        };

        if is_open {
            let proxy = self.clone();
            tokio::spawn(async move {
                if let Err(e) = proxy.process_message(message, signer).await {
                    warn!("MPC tunnel Open processing error: {}", e);
                }
            });
        } else {
            self.process_message(message, signer).await?;
        }
        debug!("process_call: {:?}", call_start.elapsed());
        Ok(vec![0x01])
    }
}
