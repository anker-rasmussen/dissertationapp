//! Outgoing MPC tunnel operations: route management and data sending.

use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::{MpcMessage, MpcTunnelProxy, SEND_PIPELINE_DEPTH};
use crate::error::{MarketError, MarketResult};
use veilid_core::RouteId;

impl MpcTunnelProxy {
    #[tracing::instrument(skip_all, fields(listen_port, target_pid))]
    pub(super) async fn run_outgoing_proxy(
        &self,
        listen_port: u16,
        target_pid: usize,
        cancel_token: CancellationToken,
    ) -> MarketResult<()> {
        let addr = format!("127.0.0.1:{listen_port}");
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| MarketError::Network(format!("Failed to bind proxy at {addr}: {e}")))?;

        info!(
            "Listening for MP-SPDZ connection to Party {} on port {}",
            target_pid, listen_port
        );

        // Accept loop: each MP-SPDZ connection is handled concurrently.
        // MP-SPDZ creates 2 connections per party pair ("machine" + "thread0"
        // players), so we must accept and forward all of them in parallel.
        loop {
            let socket = tokio::select! {
                result = listener.accept() => {
                    let (socket, _) = result?;
                    socket
                }
                () = cancel_token.cancelled() => {
                    info!("Proxy for Party {} shutting down via cancellation", target_pid);
                    break Ok(());
                }
            };

            let stream_id = self
                .inner
                .next_stream_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            debug!(
                "MP-SPDZ connected to proxy for Party {} (stream {})",
                target_pid, stream_id
            );

            let proxy = self.clone();
            let cancel = cancel_token.clone();
            tokio::spawn(async move {
                if let Err(e) = proxy
                    .handle_outgoing_connection(socket, target_pid, stream_id, cancel)
                    .await
                {
                    warn!(
                        "Outgoing connection to Party {} stream {} failed: {}",
                        target_pid, stream_id, e
                    );
                }
            });
        }
    }

    /// Handle a single outgoing MP-SPDZ connection through the tunnel.
    pub(super) async fn handle_outgoing_connection(
        &self,
        socket: tokio::net::TcpStream,
        target_pid: usize,
        stream_id: u64,
        _cancel: CancellationToken,
    ) -> MarketResult<()> {
        let (mut rd, wr) = socket.into_split();
        let session_key = (target_pid, stream_id);

        // Register write half so reverse data (Veilid → local MP-SPDZ) works
        {
            let mut sessions = self.inner.sessions.lock().await;
            sessions.insert(session_key, wr);
        }

        // Send Open via app_call (confirmed delivery).
        let open_msg = MpcMessage::Open {
            source_party_id: self.inner.party_id,
            stream_id,
        };
        let data = self.sign_mpc_message(&open_msg)?;
        let label = format!("Open to Party {target_pid} stream {stream_id}");
        self.send_reliable(target_pid, &data, &label).await;
        info!("Open sent to Party {} stream {}", target_pid, stream_id);

        // Spawn background re-sender: re-send Open every 5s to handle lost messages.
        {
            let proxy = self.clone();
            let data = data.clone();
            let cancel = self.inner.cancel_token.clone();
            tokio::spawn(async move {
                for _ in 0..12 {
                    tokio::time::sleep(std::time::Duration::from_secs(
                        crate::config::MPC_OPEN_RESEND_INTERVAL_SECS,
                    ))
                    .await;
                    if cancel.is_cancelled() {
                        break;
                    }
                    proxy.send_reliable(target_pid, &data, "Open re-send").await;
                }
            });
        }

        // Read loop: local TCP → Veilid
        let my_pid = self.inner.party_id;
        let mut buf = vec![0u8; 31000];
        let mut total_bytes_sent: u64 = 0;
        let mut last_progress_log = std::time::Instant::now();
        let send_sem = Arc::new(Semaphore::new(SEND_PIPELINE_DEPTH as usize));
        loop {
            let n = match rd.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    error!("TCP read error (stream {}): {}", stream_id, e);
                    break;
                }
            };

            debug!(
                "Outgoing: read {} bytes from TCP for Party {} stream {}",
                n, target_pid, stream_id
            );

            let seq = self.next_send_seq(target_pid, stream_id).await;
            let msg = MpcMessage::Data {
                source_party_id: my_pid,
                stream_id,
                seq,
                payload: buf[..n].to_vec(),
            };
            let signed_data = self.sign_mpc_message(&msg)?;
            let label = format!("Data P{my_pid}→P{target_pid} s{stream_id} seq{seq}");

            // Acquire pipeline slot, then send concurrently
            let Ok(permit) = Arc::clone(&send_sem).acquire_owned().await else {
                break; // semaphore closed — shutting down
            };
            let proxy = self.clone();
            tokio::spawn(async move {
                proxy.send_reliable(target_pid, &signed_data, &label).await;
                drop(permit);
            });

            total_bytes_sent += n as u64;

            // Log progress every 30s to confirm data flow at info level
            if last_progress_log.elapsed()
                >= std::time::Duration::from_secs(crate::config::MPC_PROGRESS_LOG_INTERVAL_SECS)
            {
                info!(
                    "MPC data flow P{}→P{} s{}: {} bytes sent ({} msgs)",
                    my_pid,
                    target_pid,
                    stream_id,
                    total_bytes_sent,
                    seq + 1
                );
                last_progress_log = std::time::Instant::now();
            }
        }

        // Drain pipeline: wait for all in-flight sends before Close
        let _ = send_sem.acquire_many(SEND_PIPELINE_DEPTH).await;

        // Send Close via app_call (best-effort)
        let close_msg = MpcMessage::Close {
            source_party_id: my_pid,
            stream_id,
        };
        if let Ok(close_data) = self.sign_mpc_message(&close_msg) {
            let label = format!("Close P{my_pid}→P{target_pid} s{stream_id}");
            self.send_reliable(target_pid, &close_data, &label).await;
        }

        // Cleanup
        self.inner.sessions.lock().await.remove(&session_key);
        Ok(())
    }

    /// Re-import a party's route blob to refresh Veilid's LRU cache.
    pub(super) async fn reimport_party_route(&self, party_id: usize) -> Option<RouteId> {
        let blob = self
            .inner
            .party_blobs
            .lock()
            .await
            .get(&party_id)
            .cloned()?;
        match self.inner.api.import_remote_private_route(blob) {
            Ok(route) => {
                self.inner
                    .party_routes
                    .lock()
                    .await
                    .insert(party_id, route.clone());
                Some(route)
            }
            Err(e) => {
                warn!("Failed to re-import route for Party {}: {}", party_id, e);
                None
            }
        }
    }

    /// Broadcast a RouteUpdate to all peers when our own receiving route changes.
    pub async fn broadcast_route_update(&self, new_blob: Vec<u8>) {
        let msg = MpcMessage::RouteUpdate {
            source_party_id: self.inner.party_id,
            route_blob: new_blob,
        };
        let Ok(data) = self.sign_mpc_message(&msg) else {
            warn!("Failed to sign RouteUpdate message");
            return;
        };

        let peer_pids: Vec<usize> = self
            .inner
            .signer_to_party
            .values()
            .filter(|&&pid| pid != self.inner.party_id)
            .copied()
            .collect();

        for pid in peer_pids {
            let label = format!("RouteUpdate P{}→P{}", self.inner.party_id, pid);
            self.send_reliable(pid, &data, &label).await;
        }
        info!(
            "Broadcast RouteUpdate to {} peers",
            self.inner.signer_to_party.len().saturating_sub(1)
        );
    }

    /// Proactively reimport all party route blobs when route deaths are detected.
    pub async fn handle_dead_routes(&self, dead: &[veilid_core::RouteId]) {
        if dead.is_empty() {
            return;
        }
        // Snapshot blobs without holding the lock across import calls.
        let blobs: Vec<(usize, Vec<u8>)> = self
            .inner
            .party_blobs
            .lock()
            .await
            .iter()
            .map(|(&pid, blob)| (pid, blob.clone()))
            .collect();
        for (pid, blob) in blobs {
            match self.inner.api.import_remote_private_route(blob) {
                Ok(new_route) => {
                    self.inner.party_routes.lock().await.insert(pid, new_route);
                }
                Err(e) => {
                    warn!(
                        "Failed to reimport route for Party {} after route death: {}",
                        pid, e
                    );
                }
            }
        }
    }

    /// Get the current route for a party, preferring live routes from the
    /// route manager over the stale snapshot taken at tunnel proxy construction time.
    pub(super) async fn get_party_route(&self, party_id: usize) -> Option<RouteId> {
        // Try live routes first (shared with MpcRouteManager, continuously updated)
        if let Some(live) = &self.inner.live_routes {
            if let Some(pubkey) = self.inner.party_pubkeys.get(&party_id) {
                let route = live.lock().await.get(pubkey).cloned();
                if let Some(route) = route {
                    return Some(route);
                }
            }
        }
        // Fall back to snapshot routes from construction time
        self.inner.party_routes.lock().await.get(&party_id).cloned()
    }

    /// Get the next send sequence number for a (target, stream) pair.
    pub(super) async fn next_send_seq(&self, target_pid: usize, stream_id: u64) -> u64 {
        let mut seqs = self.inner.send_seqs.lock().await;
        let entry = seqs.entry((target_pid, stream_id)).or_insert(0);
        let seq = *entry;
        *entry += 1;
        drop(seqs);
        seq
    }
}
