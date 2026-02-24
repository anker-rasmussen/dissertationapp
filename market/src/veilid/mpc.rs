//! TCP tunnel proxy: bridges MP-SPDZ localhost TCP connections over Veilid `app_call` routes.
//!
//! Each party runs a local TCP listener that MP-SPDZ connects to. Outgoing data is chunked,
//! signed, and sent via pipelined `app_call` (depth 8) to the remote party's private route.
//! Incoming data is reorder-buffered per stream to handle out-of-order delivery. Supports
//! mid-execution route updates (`RouteUpdate`) and pre-MPC warmup pings.

use bincode::Options;
use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use veilid_core::{PublicKey, RouteId, Target, VeilidAPI};

use super::bid_announcement::{bincode_deserialize_limited, SignedEnvelope};
use crate::error::{MarketError, MarketResult};

/// Calculate the base port for a given node offset.
/// Formula: `5000 + (node_offset * 10)`
///
/// # Panics
/// Panics if `node_offset > 6000` (would overflow valid port range).
pub(crate) const fn base_port_for_offset(node_offset: u16) -> u16 {
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
        stream_id: u64,
    },
    Data {
        source_party_id: usize,
        stream_id: u64,
        seq: u64,
        payload: Vec<u8>,
    },
    Close {
        source_party_id: usize,
        stream_id: u64,
    },
    /// Route warmup ping: sent before starting MP-SPDZ to confirm that
    /// MPC private routes are alive and delivering messages.
    Ping { source_party_id: usize },
    /// Mid-execution route update: a party's receiving route died and was
    /// recreated.  Peers must re-import the new blob and update their
    /// route table to continue delivering data.
    RouteUpdate {
        source_party_id: usize,
        route_blob: Vec<u8>,
    },
}

/// Manages TCP tunnel proxying between MP-SPDZ parties over Veilid routes
#[derive(Clone)]
pub struct MpcTunnelProxy {
    inner: Arc<MpcTunnelProxyInner>,
}

/// Session key: (party_id, stream_id).
///
/// MP-SPDZ creates multiple TCP connections between each party pair
/// (e.g. "machine" player + "thread0" player in MP-SPDZ).  The stream_id
/// distinguishes these concurrent connections.
type SessionKey = (usize, u64);

/// Per-stream reorder buffer for incoming Data messages.
///
/// Veilid's `app_message` (fire-and-forget) does not guarantee ordering.
/// Without reordering, out-of-order bytes written to the TCP stream cause
/// MP-SPDZ MAC check failures → SIGABRT.
///
/// **Gap detection**: If the buffer accumulates more than `MAX_BUFFER_GAP`
/// entries, we assume the missing messages were permanently lost (e.g. due
/// to a dead route) and skip ahead.  This prevents permanent deadlock at
/// the cost of feeding corrupt data to MP-SPDZ, which will crash rather
/// than hang forever.
struct ReorderBuffer {
    next_seq: u64,
    buffer: std::collections::BTreeMap<u64, Vec<u8>>,
}

/// Maximum number of out-of-order messages to buffer before assuming a
/// permanent gap (lost messages from a dead route) and force-flushing.
const MAX_REORDER_BUFFER_GAP: usize = 32;

/// Maximum retries for `app_call` sends before giving up.
/// At 50ms initial backoff (doubling to 1s max), 10 retries ≈ 10s worst case.
const MAX_SEND_RETRIES: u32 = 10;

/// Number of concurrent `app_call` sends allowed per stream direction.
/// With ~10ms RTT and 31KB chunks, 8 in-flight calls overlap the latency,
/// yielding ~8x throughput improvement on the data path.
const SEND_PIPELINE_DEPTH: u32 = 8;

struct MpcTunnelProxyInner {
    api: VeilidAPI,
    party_id: usize,
    /// Imported remote private route handles per party.
    party_routes: Mutex<HashMap<usize, RouteId>>,
    /// Raw route blobs per party — for re-import when a route expires.
    /// Re-importing is safe and idempotent (returns same RouteId, refreshes TTL).
    /// Shared so the orchestrator can update blobs when peers recreate routes.
    party_blobs: Arc<Mutex<HashMap<usize, Vec<u8>>>>,
    /// Reverse mapping: signer pubkey bytes → party id.
    /// Built from `party_signers` at construction time.
    signer_to_party: HashMap<[u8; 32], usize>,
    /// Signing key for outgoing messages.
    signing_key: SigningKey,
    base_port: u16,
    /// Active sessions keyed by (party_id, stream_id).
    ///
    /// MP-SPDZ opens 2 connection sets per party pair (one for the global
    /// "machine" player, one for the per-thread "thread0" player).  Each
    /// connection is a separate stream with its own stream_id.
    sessions: Mutex<HashMap<SessionKey, tokio::net::tcp::OwnedWriteHalf>>,
    /// Buffer for data received before Open message (race condition).
    /// Stores (seq, payload) tuples so they can be reordered when the session opens.
    pending_data: Mutex<HashMap<SessionKey, Vec<(u64, Vec<u8>)>>>,
    /// Per-stream send sequence counters: (target_party, stream_id) → next seq.
    send_seqs: Mutex<HashMap<(usize, u64), u64>>,
    /// Per-session receive reorder state.
    recv_state: Mutex<HashMap<SessionKey, ReorderBuffer>>,
    /// Monotonically increasing stream ID counter for outgoing connections.
    next_stream_id: AtomicU64,
    /// Cancellation token for graceful shutdown of TCP listener tasks.
    cancel_token: CancellationToken,
    /// Serializes TCP connections from Open handlers to the local MP-SPDZ
    /// server port.  Without this, all Open handlers connect simultaneously
    /// and the wrong `ServerSocket` (phase) accepts the connection.
    local_connect_mutex: Mutex<()>,
    /// Live route map from the MPC route manager, shared with the coordinator.
    /// Updated when peers recreate their routes and re-broadcast announcements.
    /// If set, `get_party_route` prefers these over the stale `party_routes`.
    live_routes: Option<Arc<Mutex<HashMap<PublicKey, RouteId>>>>,
    /// Mapping from party_id → PublicKey for looking up live routes.
    party_pubkeys: HashMap<usize, PublicKey>,
    /// Parties from which we've received a Ping during warmup.
    warmup_received: Mutex<std::collections::HashSet<usize>>,
    /// Notified when a new Ping is received (wakes the warmup waiter).
    warmup_notify: Notify,
}

impl MpcTunnelProxy {
    /// Create a new MPC tunnel proxy.
    ///
    /// `party_signers` maps party ID → Ed25519 verifying key bytes, used
    /// to authenticate incoming MPC messages (the envelope signer must match
    /// the expected party).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        api: VeilidAPI,
        party_id: usize,
        party_routes: HashMap<usize, RouteId>,
        party_blobs: HashMap<usize, Vec<u8>>,
        node_offset: u16,
        signing_key: SigningKey,
        party_signers: &HashMap<usize, [u8; 32]>,
        _my_route_id: Option<RouteId>,
        live_routes: Option<Arc<Mutex<HashMap<PublicKey, RouteId>>>>,
        party_pubkeys: HashMap<usize, PublicKey>,
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
            "MPC TunnelProxy for Party {}: using base port {} (node offset {}), {} authenticated peers, live_routes={}",
            party_id, base_port, node_offset, signer_to_party.len(), live_routes.is_some()
        );

        Self {
            inner: Arc::new(MpcTunnelProxyInner {
                api,
                party_id,
                party_routes: Mutex::new(party_routes),
                party_blobs: Arc::new(Mutex::new(party_blobs)),
                signer_to_party,
                signing_key,
                base_port,
                sessions: Mutex::new(HashMap::new()),
                pending_data: Mutex::new(HashMap::new()),
                send_seqs: Mutex::new(HashMap::new()),
                recv_state: Mutex::new(HashMap::new()),
                next_stream_id: AtomicU64::new(0),
                cancel_token: CancellationToken::new(),
                local_connect_mutex: Mutex::new(()),
                live_routes,
                party_pubkeys,
                warmup_received: Mutex::new(std::collections::HashSet::new()),
                warmup_notify: Notify::new(),
            }),
        }
    }

    /// Start the tunnel proxy: spawn one outgoing TCP listener per peer party.
    ///
    /// Each listener accepts connections from the local MP-SPDZ process, chunks
    /// outgoing data into signed `MpcMessage::Data` frames, and sends them via
    /// pipelined `app_call` to the peer's private route.
    pub fn run(&self) -> MarketResult<()> {
        info!("Starting MPC TunnelProxy for Party {}", self.inner.party_id);

        // Spawn one outgoing proxy task per peer party.
        // Use signer_to_party to enumerate peers — it's a plain HashMap
        // (not behind a mutex), unlike party_blobs which is now Arc<Mutex<>>.
        let peer_pids: Vec<usize> = self
            .inner
            .signer_to_party
            .values()
            .filter(|&&pid| pid != self.inner.party_id)
            .copied()
            .collect();

        for pid in peer_pids {
            let listen_port = party_port(self.inner.base_port, pid);
            let proxy = self.clone();
            let cancel_token = self.inner.cancel_token.clone();

            tokio::spawn(async move {
                if let Err(e) = proxy
                    .run_outgoing_proxy(listen_port, pid, cancel_token)
                    .await
                {
                    error!("Proxy for Party {} failed: {}", pid, e);
                }
            });
        }

        Ok(())
    }

    /// Send Pings to all peers and wait until Pings from all peers arrive.
    /// This confirms that MPC private routes are alive in both directions
    /// before starting MP-SPDZ.  Without this warmup, Veilid route
    /// propagation delay (~50s in devnet) causes MP-SPDZ timeouts.
    pub async fn warmup(&self, timeout: std::time::Duration) -> MarketResult<()> {
        let peers: Vec<usize> = self
            .inner
            .signer_to_party
            .values()
            .filter(|&&pid| pid != self.inner.party_id)
            .copied()
            .collect();

        info!(
            "Party {}: warming up MPC routes to {} peers (timeout {}s)",
            self.inner.party_id,
            peers.len(),
            timeout.as_secs()
        );

        let deadline = tokio::time::Instant::now() + timeout;
        let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(3));
        ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            // Check if all peers have pinged us
            let received = self.inner.warmup_received.lock().await.clone();
            let missing: Vec<usize> = peers
                .iter()
                .filter(|p| !received.contains(p))
                .copied()
                .collect();
            if missing.is_empty() {
                info!(
                    "Party {}: all {} peers confirmed reachable via MPC routes",
                    self.inner.party_id,
                    peers.len()
                );
                return Ok(());
            }

            if tokio::time::Instant::now() >= deadline {
                warn!(
                    "Party {}: warmup timed out, missing pings from {:?}. Proceeding anyway.",
                    self.inner.party_id, missing
                );
                return Ok(());
            }

            // Send Ping to all peers (including those already confirmed, in case they need our ping)
            let ping = MpcMessage::Ping {
                source_party_id: self.inner.party_id,
            };
            if let Ok(data) = self.sign_mpc_message(&ping) {
                for &pid in &peers {
                    let label = format!("Ping P{}→P{}", self.inner.party_id, pid);
                    self.send_reliable(pid, &data, &label).await;
                }
            }

            // Wait for next ping interval or notification
            tokio::select! {
                _instant = ping_interval.tick() => {}
                () = self.inner.warmup_notify.notified() => {}
                () = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    /// Serialize an MPC message and wrap it in a signed envelope.
    fn sign_mpc_message(&self, msg: &MpcMessage) -> MarketResult<Vec<u8>> {
        let t = std::time::Instant::now();
        let payload = bincode::options()
            .with_limit(super::bid_announcement::MAX_BINCODE_SIZE)
            .serialize(msg)
            .map_err(|e| {
                MarketError::Serialization(format!("Failed to serialize MPC message: {e}"))
            })?;
        let envelope = SignedEnvelope::sign(payload, &self.inner.signing_key)?;
        let result = envelope.to_bytes();
        debug!("sign_mpc_message: {:?}", t.elapsed());
        result
    }

    /// Cleanup resources: signal shutdown and clear active sessions.
    pub fn cleanup(&self) {
        info!(
            "Cleaning up MPC tunnel proxy for Party {}",
            self.inner.party_id
        );
        // Signal all listener tasks to shut down
        self.inner.cancel_token.cancel();
        // Clear all active sessions to drop TCP connections
        // Note: We can't async here (Drop context), so we use try_lock
        if let Ok(mut sessions) = self.inner.sessions.try_lock() {
            sessions.clear();
        }
        if let Ok(mut pending) = self.inner.pending_data.try_lock() {
            pending.clear();
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn run_outgoing_proxy(
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

            let stream_id = self.inner.next_stream_id.fetch_add(1, Ordering::Relaxed);
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

    /// Re-import a party's route blob to refresh Veilid's LRU cache.
    ///
    /// Safe and idempotent: returns the same `RouteId` and refreshes
    /// the 5-minute TTL.  Updates `party_routes` with the (same) handle.
    async fn reimport_party_route(&self, party_id: usize) -> Option<RouteId> {
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
    ///
    /// Called by the route change handler when `handle_dead_own_route` recreates
    /// our route.  Peers import the new blob and update their sending table.
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
    ///
    /// `app_message` to a dead route silently succeeds (fire-and-forget), so
    /// data is lost without errors.  By reimporting all blobs whenever any
    /// route death is reported, we refresh Veilid's internal route cache and
    /// restore deliverability.
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
    /// route manager (updated by ongoing announcements) over the stale
    /// snapshot taken at tunnel proxy construction time.
    async fn get_party_route(&self, party_id: usize) -> Option<RouteId> {
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
    async fn next_send_seq(&self, target_pid: usize, stream_id: u64) -> u64 {
        let mut seqs = self.inner.send_seqs.lock().await;
        let entry = seqs.entry((target_pid, stream_id)).or_insert(0);
        let seq = *entry;
        *entry += 1;
        drop(seqs);
        seq
    }

    /// Deliver a Data payload through the reorder buffer.
    ///
    /// Returns payloads that are ready to write to TCP in order.
    /// Buffers out-of-order payloads until the gap is filled.
    async fn deliver_ordered(
        &self,
        session_key: SessionKey,
        seq: u64,
        payload: Vec<u8>,
    ) -> Vec<Vec<u8>> {
        let mut state = self.inner.recv_state.lock().await;
        let buf = state.entry(session_key).or_insert_with(|| ReorderBuffer {
            next_seq: 0,
            buffer: std::collections::BTreeMap::new(),
        });

        let result = match seq.cmp(&buf.next_seq) {
            std::cmp::Ordering::Equal => {
                // In order — deliver immediately plus any consecutive buffered
                let mut result = vec![payload];
                buf.next_seq += 1;
                while let Some(p) = buf.buffer.remove(&buf.next_seq) {
                    result.push(p);
                    buf.next_seq += 1;
                }
                result
            }
            std::cmp::Ordering::Greater => {
                // Out of order — buffer
                debug!(
                    "Reorder: buffering seq {} for ({}, {}), expecting {}",
                    seq, session_key.0, session_key.1, buf.next_seq
                );
                buf.buffer.insert(seq, payload);

                // Gap detection: if too many messages are buffered, the missing
                // ones were likely lost (dead route). Skip ahead to avoid
                // permanent deadlock.
                if buf.buffer.len() > MAX_REORDER_BUFFER_GAP {
                    // SAFETY: we just checked len() > 0
                    let Some(&first_buffered) = buf.buffer.keys().next() else {
                        return vec![];
                    };
                    warn!(
                        "Reorder: gap detected for ({}, {}): {} buffered msgs, \
                         expected seq {} but lowest buffered is {}. \
                         Skipping ahead (lost messages from dead route).",
                        session_key.0,
                        session_key.1,
                        buf.buffer.len(),
                        buf.next_seq,
                        first_buffered,
                    );
                    buf.next_seq = first_buffered;
                    let mut result = Vec::new();
                    while let Some(p) = buf.buffer.remove(&buf.next_seq) {
                        result.push(p);
                        buf.next_seq += 1;
                    }
                    result
                } else {
                    vec![]
                }
            }
            std::cmp::Ordering::Less => {
                // Duplicate (seq < next_seq) — discard
                warn!(
                    "Reorder: discarding duplicate seq {} for ({}, {})",
                    seq, session_key.0, session_key.1
                );
                vec![]
            }
        };
        drop(state);
        result
    }

    /// Send a signed message via `app_message` with retry logic.
    /// Fire-and-forget: returns once Veilid accepts the message for delivery.
    /// Retries up to `MAX_SEND_RETRIES` times on transient failures.
    /// Send data to a peer using `app_call` (request/response) for confirmed
    /// delivery.  Retries with exponential backoff on failure.
    ///
    /// `app_call` is strongly preferred over `app_message` because:
    /// - `app_message` silently drops data when routes are dead
    /// - `app_call` returns an error or NACK, enabling retry logic
    /// - Matches best practice from VeilidChat (DHT) and Stigmerge (`app_call`)
    async fn send_reliable(&self, target_pid: usize, data: &[u8], label: &str) {
        let send_start = std::time::Instant::now();
        let mut backoff_ms = 50u64;
        for attempt in 0u32..MAX_SEND_RETRIES {
            let ctx = match self.inner.api.routing_context() {
                Ok(c) => c,
                Err(e) => {
                    warn!("{label} attempt {attempt}: no routing context: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(1000);
                    continue;
                }
            };
            let Some(route) = self.get_party_route(target_pid).await else {
                warn!("{label} attempt {attempt}: no route for Party {target_pid}");
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(1000);
                continue;
            };

            match ctx.app_call(Target::RouteId(route), data.to_vec()).await {
                Ok(reply) => {
                    // ACK = [0x01], NACK = [0x00] (tunnel not ready)
                    if reply.first() == Some(&0x01) {
                        debug!(
                            "{label}: delivered in {:?} ({attempt} retries)",
                            send_start.elapsed()
                        );
                        return;
                    }
                    // NACK — peer's tunnel proxy not ready yet; retry
                    debug!("{label} attempt {attempt}: NACK from Party {target_pid} — retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(1000);
                }
                Err(e) => {
                    debug!("{label} attempt {attempt}: app_call failed: {e} — reimporting route");
                    self.reimport_party_route(target_pid).await;
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(1000);
                }
            }
        }
        error!(
            "{label}: gave up after {MAX_SEND_RETRIES} retries ({:?} elapsed)",
            send_start.elapsed()
        );
    }

    /// Handle a single outgoing MP-SPDZ connection through the tunnel.
    #[allow(clippy::too_many_lines)]
    async fn handle_outgoing_connection(
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

        // Send Open via app_message (fire-and-forget).
        // The background re-sender handles retries, so we just need one
        // successful send to get started.
        let open_msg = MpcMessage::Open {
            source_party_id: self.inner.party_id,
            stream_id,
        };
        let data = self.sign_mpc_message(&open_msg)?;
        let label = format!("Open to Party {target_pid} stream {stream_id}");
        self.send_reliable(target_pid, &data, &label).await;
        info!("Open sent to Party {} stream {}", target_pid, stream_id);

        // Spawn background re-sender: re-send Open via app_message every 5s
        // to handle the case where the first Open was lost due to route death.
        // The receiver deduplicates (skips if session exists). 12 x 5s = 60s
        // covers the ~40-65s party desynchronization window in devnets.
        {
            let proxy = self.clone();
            let data = data.clone();
            let cancel = self.inner.cancel_token.clone();
            tokio::spawn(async move {
                for _ in 0..12 {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    if cancel.is_cancelled() {
                        break;
                    }
                    proxy.send_reliable(target_pid, &data, "Open re-send").await;
                }
            });
        }

        // Read loop: local TCP → Veilid
        // Pipelined: spawns up to SEND_PIPELINE_DEPTH concurrent sends to
        // overlap app_call RTT (~10ms) with TCP reads.  Sequence numbers are
        // allocated before spawn to preserve ordering at the receiver.
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
            if last_progress_log.elapsed() >= std::time::Duration::from_secs(30) {
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
                let session_key: SessionKey = (source_party_id, stream_id);

                // Dedup: if a session already exists for this stream, the Open
                // is a retransmit from the background re-sender.  Skip it to
                // avoid creating duplicate TCP connections to local MP-SPDZ.
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

                // Serialize connections to local MP-SPDZ.  MP-SPDZ creates
                // sequential ServerSocket instances on the same port for
                // each protocol phase (machine, thread0, OT, …).  If
                // multiple Open handlers connect simultaneously, the wrong
                // ServerSocket accepts a connection intended for a later
                // phase, causing identity mismatch and a 60s timeout.
                //
                // The mutex ensures one TCP connect at a time.  The 100ms
                // delay after connect gives the ServerSocket time to
                // accept() and read the identity before we release the lock.
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
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
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
                    // Brief pause while holding mutex — gives the
                    // ServerSocket time to accept + read identity before
                    // the next Open handler connects.  20ms is generous
                    // for localhost TCP accept.
                    if result.is_some() {
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
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
                    // Feed each buffered (seq, payload) through the reorder buffer
                    // so that the receiver-side next_seq is correctly initialized.
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
                    // Uses fire-and-forget app_message (same as outgoing).
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

                            let label =
                                format!("Reply P{my_pid}→P{target_pid} s{stream_id} seq{seq}");

                            // Acquire pipeline slot, then send concurrently
                            let Ok(permit) = Arc::clone(&reply_sem).acquire_owned().await else {
                                break; // semaphore closed — shutting down
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
            }
            MpcMessage::Data {
                source_party_id,
                stream_id,
                seq,
                payload,
            } => {
                let data_start = std::time::Instant::now();
                let session_key: SessionKey = (source_party_id, stream_id);

                // Check if session exists (quick non-blocking check)
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
                // Record that this peer's route is alive
                self.inner
                    .warmup_received
                    .lock()
                    .await
                    .insert(source_party_id);
                self.inner.warmup_notify.notify_waiters();
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
                // Re-import the new route blob and update our route table.
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

    /// Process an incoming `AppCall` for MPC tunnel messages.
    ///
    /// All MPC data is sent via `app_call` for confirmed delivery.
    /// Returns ACK `[0x01]` on success so the sender knows the data arrived.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mpc_message_open_roundtrip() {
        let msg = MpcMessage::Open {
            source_party_id: 5,
            stream_id: 42,
        };
        let bytes = bincode::serialize(&msg).unwrap();
        let decoded: MpcMessage = bincode::deserialize(&bytes).unwrap();
        match decoded {
            MpcMessage::Open {
                source_party_id,
                stream_id,
            } => {
                assert_eq!(source_party_id, 5);
                assert_eq!(stream_id, 42);
            }
            other => panic!("Expected Open, got {other:?}"),
        }
    }

    #[test]
    fn test_mpc_message_data_roundtrip() {
        let payload = vec![1, 2, 3, 4, 5];
        let msg = MpcMessage::Data {
            source_party_id: 2,
            stream_id: 0,
            seq: 0,
            payload: payload.clone(),
        };
        let bytes = bincode::serialize(&msg).unwrap();
        let decoded: MpcMessage = bincode::deserialize(&bytes).unwrap();
        match decoded {
            MpcMessage::Data {
                source_party_id,
                stream_id,
                seq,
                payload: decoded_payload,
            } => {
                assert_eq!(source_party_id, 2);
                assert_eq!(stream_id, 0);
                assert_eq!(seq, 0);
                assert_eq!(decoded_payload, payload);
            }
            other => panic!("Expected Data, got {other:?}"),
        }
    }

    #[test]
    fn test_mpc_message_close_roundtrip() {
        let msg = MpcMessage::Close {
            source_party_id: 0,
            stream_id: 1,
        };
        let bytes = bincode::serialize(&msg).unwrap();
        let decoded: MpcMessage = bincode::deserialize(&bytes).unwrap();
        match decoded {
            MpcMessage::Close {
                source_party_id,
                stream_id,
            } => {
                assert_eq!(source_party_id, 0);
                assert_eq!(stream_id, 1);
            }
            other => panic!("Expected Close, got {other:?}"),
        }
    }

    #[test]
    fn test_mpc_message_data_large_payload() {
        let payload = vec![0xAB; 32 * 1024]; // 32KB (Veilid max)
        let msg = MpcMessage::Data {
            source_party_id: 1,
            stream_id: 0,
            seq: 0,
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
            stream_id: 7,
            seq: 0,
            payload: vec![],
        };
        let bytes = bincode::serialize(&msg).unwrap();
        let decoded: MpcMessage = bincode::deserialize(&bytes).unwrap();
        match decoded {
            MpcMessage::Data {
                source_party_id,
                stream_id,
                seq,
                payload,
            } => {
                assert_eq!(source_party_id, 3);
                assert_eq!(stream_id, 7);
                assert_eq!(seq, 0);
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
