//! TCP tunnel proxy: bridges MP-SPDZ localhost TCP connections over Veilid `app_call` routes.
//!
//! Each party runs a local TCP listener that MP-SPDZ connects to. Outgoing data is chunked,
//! signed, and sent via pipelined `app_call` (depth 8) to the remote party's private route.
//! Incoming data is reorder-buffered per stream to handle out-of-order delivery. Supports
//! mid-execution route updates (`RouteUpdate`) and pre-MPC warmup pings.

mod incoming;
mod outgoing;

use bincode::Options;
use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use veilid_core::{PublicKey, RouteId, Target, VeilidAPI};

use super::bid_announcement::SignedEnvelope;
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
pub(super) const fn party_port(base_port: u16, party_id: usize) -> u16 {
    base_port + party_id as u16
}

/// Envelope wrapping serialized [`MpcMessage`] bytes with a session
/// identifier so concurrent auctions can be routed to the correct
/// tunnel proxy.
///
/// `session_id` is the listing key string — deterministic and known to
/// all parties in the same auction without extra coordination.
///
/// The coordinator peek-deserializes this to extract `session_id` for
/// routing, then forwards the inner `message` bytes to the matching
/// tunnel proxy which deserializes and processes the `MpcMessage`.
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct MpcEnvelope {
    pub session_id: String,
    /// Bincode-serialized [`MpcMessage`].
    pub message: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(super) enum MpcMessage {
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
pub(super) type SessionKey = (usize, u64);

/// Per-stream reorder buffer for incoming Data messages.
///
/// **Gap detection**: If the buffer accumulates more than `MAX_BUFFER_GAP`
/// entries, we assume the missing messages were permanently lost and skip ahead.
struct ReorderBuffer {
    next_seq: u64,
    buffer: std::collections::BTreeMap<u64, Vec<u8>>,
}

/// Maximum number of out-of-order messages to buffer before assuming a
/// permanent gap (lost messages from a dead route) and force-flushing.
const MAX_REORDER_BUFFER_GAP: usize = 32;

/// Maximum retries for `app_call` sends before giving up.
const MAX_SEND_RETRIES: u32 = 10;

/// Number of concurrent `app_call` sends allowed per stream direction.
pub(super) const SEND_PIPELINE_DEPTH: u32 = 8;

pub(super) struct MpcTunnelProxyInner {
    pub(super) api: VeilidAPI,
    pub(super) party_id: usize,
    /// Session identifier (listing key string) for MPC envelope tagging.
    pub(super) session_id: String,
    /// Imported remote private route handles per party.
    pub(super) party_routes: Mutex<HashMap<usize, RouteId>>,
    /// Raw route blobs per party — for re-import when a route expires.
    pub(super) party_blobs: Arc<Mutex<HashMap<usize, Vec<u8>>>>,
    /// Reverse mapping: signer pubkey bytes → party id.
    pub(super) signer_to_party: HashMap<[u8; 32], usize>,
    /// Signing key for outgoing messages.
    pub(super) signing_key: SigningKey,
    pub(super) base_port: u16,
    /// Active sessions keyed by (party_id, stream_id).
    pub(super) sessions: Mutex<HashMap<SessionKey, tokio::net::tcp::OwnedWriteHalf>>,
    /// Buffer for data received before Open message (race condition).
    pub(super) pending_data: Mutex<HashMap<SessionKey, Vec<(u64, Vec<u8>)>>>,
    /// Per-stream send sequence counters: (target_party, stream_id) → next seq.
    pub(super) send_seqs: Mutex<HashMap<(usize, u64), u64>>,
    /// Per-session receive reorder state.
    recv_state: Mutex<HashMap<SessionKey, ReorderBuffer>>,
    /// Monotonically increasing stream ID counter for outgoing connections.
    pub(super) next_stream_id: AtomicU64,
    /// Cancellation token for graceful shutdown of TCP listener tasks.
    pub(super) cancel_token: CancellationToken,
    /// Serializes TCP connections from Open handlers to the local MP-SPDZ server port.
    pub(super) local_connect_mutex: Mutex<()>,
    /// Live route map from the MPC route manager, shared with the coordinator.
    pub(super) live_routes: Option<Arc<Mutex<HashMap<PublicKey, RouteId>>>>,
    /// Mapping from party_id → PublicKey for looking up live routes.
    pub(super) party_pubkeys: HashMap<usize, PublicKey>,
    /// Parties from which we've received a Ping during warmup.
    pub(super) warmup_received: Mutex<std::collections::HashSet<usize>>,
    /// Notified when a new Ping is received (wakes the warmup waiter).
    pub(super) warmup_notify: Notify,
}

/// Configuration for constructing an [`MpcTunnelProxy`].
pub struct MpcTunnelConfig {
    pub api: VeilidAPI,
    pub party_id: usize,
    /// Session identifier (listing key string) — tags every outgoing
    /// [`MpcEnvelope`] so the receiver can route to the correct tunnel.
    pub session_id: String,
    pub party_routes: HashMap<usize, RouteId>,
    pub party_blobs: HashMap<usize, Vec<u8>>,
    pub node_offset: u16,
    pub signing_key: SigningKey,
    pub party_signers: HashMap<usize, [u8; 32]>,
    pub live_routes: Option<Arc<Mutex<HashMap<PublicKey, RouteId>>>>,
    pub party_pubkeys: HashMap<usize, PublicKey>,
}

impl MpcTunnelProxy {
    /// Create a new MPC tunnel proxy from the given configuration.
    ///
    /// `config.party_signers` maps party ID → Ed25519 verifying key bytes, used
    /// to authenticate incoming MPC messages (the envelope signer must match
    /// the expected party).
    pub fn new(config: MpcTunnelConfig) -> Self {
        let base_port = base_port_for_offset(config.node_offset);

        // Build reverse mapping: signer → party_id
        let signer_to_party: HashMap<[u8; 32], usize> = config
            .party_signers
            .iter()
            .map(|(&pid, &key)| (key, pid))
            .collect();

        info!(
            "MPC TunnelProxy for Party {} session {}: base port {} (offset {}), {} peers, live_routes={}",
            config.party_id, config.session_id, base_port, config.node_offset,
            signer_to_party.len(), config.live_routes.is_some()
        );

        Self {
            inner: Arc::new(MpcTunnelProxyInner {
                api: config.api,
                party_id: config.party_id,
                session_id: config.session_id,
                party_routes: Mutex::new(config.party_routes),
                party_blobs: Arc::new(Mutex::new(config.party_blobs)),
                signer_to_party,
                signing_key: config.signing_key,
                base_port,
                sessions: Mutex::new(HashMap::new()),
                pending_data: Mutex::new(HashMap::new()),
                send_seqs: Mutex::new(HashMap::new()),
                recv_state: Mutex::new(HashMap::new()),
                next_stream_id: AtomicU64::new(0),
                cancel_token: CancellationToken::new(),
                local_connect_mutex: Mutex::new(()),
                live_routes: config.live_routes,
                party_pubkeys: config.party_pubkeys,
                warmup_received: Mutex::new(std::collections::HashSet::new()),
                warmup_notify: Notify::new(),
            }),
        }
    }

    /// Start the tunnel proxy: spawn one outgoing TCP listener per peer party.
    pub fn run(&self) -> MarketResult<()> {
        info!("Starting MPC TunnelProxy for Party {}", self.inner.party_id);

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
        let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(
            crate::config::MPC_PING_INTERVAL_SECS,
        ));
        ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
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

            let ping = MpcMessage::Ping {
                source_party_id: self.inner.party_id,
            };
            if let Ok(data) = self.sign_mpc_message(&ping) {
                for &pid in &peers {
                    let label = format!("Ping P{}→P{}", self.inner.party_id, pid);
                    self.send_reliable(pid, &data, &label).await;
                }
            }

            tokio::select! {
                _instant = ping_interval.tick() => {}
                () = self.inner.warmup_notify.notified() => {}
                () = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    /// Serialize an MPC message, wrap it in an [`MpcEnvelope`] tagged with
    /// this tunnel's session ID, and sign the result.
    pub(super) fn sign_mpc_message(&self, msg: &MpcMessage) -> MarketResult<Vec<u8>> {
        let t = std::time::Instant::now();
        let msg_bytes = bincode::options()
            .with_limit(super::bid_announcement::MAX_BINCODE_SIZE)
            .serialize(msg)
            .map_err(|e| {
                MarketError::Serialization(format!("Failed to serialize MPC message: {e}"))
            })?;
        let mpc_envelope = MpcEnvelope {
            session_id: self.inner.session_id.clone(),
            message: msg_bytes,
        };
        let payload = bincode::options()
            .with_limit(super::bid_announcement::MAX_BINCODE_SIZE)
            .serialize(&mpc_envelope)
            .map_err(|e| {
                MarketError::Serialization(format!("Failed to serialize MPC envelope: {e}"))
            })?;
        let envelope = SignedEnvelope::sign(payload, &self.inner.signing_key);
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
        self.inner.cancel_token.cancel();
        if let Ok(mut sessions) = self.inner.sessions.try_lock() {
            sessions.clear();
        }
        if let Ok(mut pending) = self.inner.pending_data.try_lock() {
            pending.clear();
        }
    }

    /// Send data to a peer using `app_call` (request/response) for confirmed
    /// delivery.  Retries with exponential backoff on failure.
    pub(super) async fn send_reliable(&self, target_pid: usize, data: &[u8], label: &str) {
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

    /// Deliver a Data payload through the reorder buffer.
    ///
    /// Returns payloads that are ready to write to TCP in order.
    pub(super) async fn deliver_ordered(
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
                // ones were likely lost (dead route). Skip ahead.
                if buf.buffer.len() > MAX_REORDER_BUFFER_GAP {
                    // len() > 0 verified above
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

    #[test]
    fn test_mpc_envelope_roundtrip() {
        let msg = MpcMessage::Data {
            source_party_id: 1,
            stream_id: 7,
            seq: 42,
            payload: vec![0xDE, 0xAD],
        };
        let msg_bytes = bincode::serialize(&msg).unwrap();
        let envelope = MpcEnvelope {
            session_id: "VLD0:abc123".to_string(),
            message: msg_bytes.clone(),
        };
        let env_bytes = bincode::serialize(&envelope).unwrap();
        let decoded: MpcEnvelope = bincode::deserialize(&env_bytes).unwrap();
        assert_eq!(decoded.session_id, "VLD0:abc123");
        assert_eq!(decoded.message, msg_bytes);

        let inner: MpcMessage = bincode::deserialize(&decoded.message).unwrap();
        match inner {
            MpcMessage::Data {
                source_party_id,
                stream_id,
                seq,
                payload,
            } => {
                assert_eq!(source_party_id, 1);
                assert_eq!(stream_id, 7);
                assert_eq!(seq, 42);
                assert_eq!(payload, vec![0xDE, 0xAD]);
            }
            other => panic!("Expected Data, got {other:?}"),
        }
    }

    #[test]
    fn test_mpc_envelope_session_id_routing() {
        let msg = MpcMessage::Ping { source_party_id: 0 };
        let msg_bytes = bincode::serialize(&msg).unwrap();

        let env_a = MpcEnvelope {
            session_id: "auction-A".to_string(),
            message: msg_bytes.clone(),
        };
        let env_b = MpcEnvelope {
            session_id: "auction-B".to_string(),
            message: msg_bytes,
        };

        let bytes_a = bincode::serialize(&env_a).unwrap();
        let bytes_b = bincode::serialize(&env_b).unwrap();
        assert_ne!(bytes_a, bytes_b);

        let dec_a: MpcEnvelope = bincode::deserialize(&bytes_a).unwrap();
        let dec_b: MpcEnvelope = bincode::deserialize(&bytes_b).unwrap();
        assert_eq!(dec_a.session_id, "auction-A");
        assert_eq!(dec_b.session_id, "auction-B");
    }
}
