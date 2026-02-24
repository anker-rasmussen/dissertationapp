//! Real Veilid coordinator wrapping [`AuctionLogic`] with actual network I/O.
//!
//! [`AuctionCoordinator`] manages the Veilid API lifecycle, DHT operations,
//! broadcast routing, MPC orchestration, and message dispatch. It instantiates
//! [`AuctionLogic`] with real implementations and delegates pure auction logic
//! to that layer.

use async_trait::async_trait;
use ed25519_dalek::SigningKey;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use veilid_core::{PublicKey, RecordKey, RouteBlob, RouteId, VeilidAPI};

use super::auction_logic::AuctionLogic;
use super::bid_announcement::{validate_timestamp, AuctionMessage, BidAnnouncementRegistry};
use super::bid_storage::BidStorage;
use super::dht::{DHTOperations, OwnedDHTRecord};
use super::listing_ops::ListingOperations;
use super::mpc_orchestrator::MpcOrchestrator;
use super::registry::RegistryOperations;
use super::registry_types::RegistryEntry;
use crate::config::{now_unix, subkeys};
use crate::error::{MarketError, MarketResult};
use crate::marketplace::PublicListing;
use crate::traits::{
    DhtStore, MessageTransport, MpcResult, MpcRunner, SystemTimeProvider, TransportTarget,
};

mod broadcast;
mod handlers;
mod monitor;
mod signing;

/// An MPC signal received before the listing was watched.
pub(super) enum BufferedMpcSignal {
    RouteAnnouncement {
        party_pubkey: PublicKey,
        route_blob: RouteBlob,
        received_at: u64,
    },
    Ready {
        party_pubkey: PublicKey,
        num_parties: u32,
        timestamp: u64,
        received_at: u64,
    },
}

/// Max buffered signals per listing before oldest entries are evicted.
const MPC_SIGNAL_BUFFER_CAP: usize = 50;
/// Buffered signals older than this (seconds) are discarded on replay.
const MPC_SIGNAL_BUFFER_TTL: u64 = 60;

// ---------------------------------------------------------------------------
// Stub transport and MPC runner for the embedded AuctionLogic.
//
// AuctionCoordinator delegates **state management** (watched_listings,
// bid_announcements, decryption_keys) to AuctionLogic so that the same code
// tested with mocks runs in production.  Transport and MPC execution stay in
// AuctionCoordinator (via VeilidAPI / MpcOrchestrator), so these stubs exist
// solely to satisfy the generic type parameters.
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(super) struct CoordinatorTransport;

#[async_trait]
impl MessageTransport for CoordinatorTransport {
    async fn send(&self, _: TransportTarget, _: Vec<u8>) -> MarketResult<()> {
        Err(MarketError::InvalidState(
            "Use AuctionCoordinator broadcast methods".into(),
        ))
    }
    async fn create_private_route(&self) -> MarketResult<(RouteId, RouteBlob)> {
        Err(MarketError::InvalidState(
            "Use MpcOrchestrator for routes".into(),
        ))
    }
    fn import_remote_route(&self, _: RouteBlob) -> MarketResult<RouteId> {
        Err(MarketError::InvalidState(
            "Use MpcOrchestrator for routes".into(),
        ))
    }
    async fn get_peers(&self) -> MarketResult<Vec<PublicKey>> {
        Ok(Vec::new())
    }
}

#[derive(Clone)]
pub(super) struct CoordinatorMpcRunner;

#[async_trait]
impl MpcRunner for CoordinatorMpcRunner {
    async fn compile(&self, _: &str, _: usize) -> MarketResult<()> {
        Err(MarketError::InvalidState(
            "Use MpcOrchestrator for MPC".into(),
        ))
    }
    async fn execute(&self, _: usize, _: usize, _: u64) -> MarketResult<MpcResult> {
        Err(MarketError::InvalidState(
            "Use MpcOrchestrator for MPC".into(),
        ))
    }
    async fn write_input(&self, _: usize, _: u64) -> MarketResult<()> {
        Err(MarketError::InvalidState(
            "Use MpcOrchestrator for MPC".into(),
        ))
    }
    async fn write_hosts(&self, _: &str, _: usize) -> MarketResult<()> {
        Err(MarketError::InvalidState(
            "Use MpcOrchestrator for MPC".into(),
        ))
    }
}

/// Type alias for the embedded AuctionLogic with production implementations.
type CoordinatorLogic =
    AuctionLogic<DHTOperations, CoordinatorTransport, CoordinatorMpcRunner, SystemTimeProvider>;

/// Coordinates auction execution: monitors deadlines, triggers MPC.
///
/// Delegates shared state management (watched listings, bid announcements,
/// decryption keys) to [`AuctionLogic`] so the same code path tested with
/// mocks is used in production.
///
/// # Lock ordering
///
/// When acquiring multiple locks, always follow this order to prevent deadlocks:
///
/// 1. `registry_ops`
/// 2. `pending_seller_registrations`
/// 3. `owned_listings`
/// 4. `pending_bid_announcements`
/// 5. `my_route_blob`
///
/// Never hold any of these locks across an `.await` point if possible.
/// If an async call is needed under lock, re-acquire the lock after awaiting.
pub struct AuctionCoordinator {
    pub(super) api: VeilidAPI,
    pub(super) dht: DHTOperations,
    pub(super) my_node_id: PublicKey,
    /// Ed25519 signing key for authenticating outgoing messages.
    pub(super) signing_key: SigningKey,
    /// Core auction state machine — the tested generic layer.
    pub(super) logic: CoordinatorLogic,
    /// Per-seller registry operations (shared via `Arc<Mutex>`).
    pub(super) registry_ops: Arc<Mutex<RegistryOperations>>,
    /// Listings we own (as seller): Map<listing_key, OwnedDHTRecord>
    pub(super) owned_listings: Arc<Mutex<HashMap<RecordKey, OwnedDHTRecord>>>,
    /// MPC orchestrator (owns tunnel proxy, routes, verifications)
    pub(super) mpc: Arc<MpcOrchestrator>,
    /// Buffered SellerRegistrations received before the registry key was known.
    /// Tuple: (seller_pubkey, catalog_key, signing_pubkey_hex)
    pub(super) pending_seller_registrations: Arc<Mutex<Vec<(PublicKey, RecordKey, String)>>>,
    /// Bid announcements that failed to broadcast (no routes yet) and need retry.
    pub(super) pending_bid_announcements: Mutex<Vec<(RecordKey, RecordKey)>>,
    /// Our own broadcast private route blob (published to the registry).
    pub(super) my_route_blob: Mutex<Option<Vec<u8>>>,
    /// RouteId of the broadcast route (used as MPC route for keepalive reuse).
    pub(super) broadcast_route_id: Mutex<Option<RouteId>>,
    /// MPC signals received before the listing was watched (no route manager yet).
    pub(super) buffered_mpc_signals: Mutex<HashMap<RecordKey, Vec<BufferedMpcSignal>>>,
    /// Token used to signal graceful shutdown of background tasks.
    pub(super) shutdown: CancellationToken,
    /// Handle for the background monitoring task.
    pub(super) monitor_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl AuctionCoordinator {
    /// Create a new coordinator bound to a Veilid node.
    ///
    /// Generates an Ed25519 signing key for message authentication, initialises
    /// the embedded [`MpcOrchestrator`] and [`AuctionLogic`], and sets up DHT
    /// registry operations scoped to the given `network_key`.
    pub fn new(
        api: VeilidAPI,
        dht: DHTOperations,
        my_node_id: PublicKey,
        bid_storage: BidStorage,
        node_offset: u16,
        network_key: &str,
        shutdown: CancellationToken,
    ) -> Self {
        let signing_key = SigningKey::generate(&mut rand::thread_rng());
        info!(
            "Generated Ed25519 signing key: {}",
            hex::encode(signing_key.verifying_key().to_bytes())
        );

        let mpc = Arc::new(MpcOrchestrator::new(
            api.clone(),
            dht.clone(),
            my_node_id.clone(),
            bid_storage.clone(),
            node_offset,
            signing_key.clone(),
        ));

        let registry_ops = Arc::new(Mutex::new(RegistryOperations::new(
            dht.clone(),
            network_key,
        )));

        let logic = AuctionLogic::new(
            dht.clone(),
            CoordinatorTransport,
            CoordinatorMpcRunner,
            SystemTimeProvider,
            my_node_id.clone(),
            bid_storage,
        );

        Self {
            api,
            dht,
            my_node_id,
            signing_key,
            logic,
            registry_ops,
            owned_listings: Arc::new(Mutex::new(HashMap::new())),
            mpc,
            pending_seller_registrations: Arc::new(Mutex::new(Vec::new())),
            pending_bid_announcements: Mutex::new(Vec::new()),
            my_route_blob: Mutex::new(None),
            broadcast_route_id: Mutex::new(None),
            buffered_mpc_signals: Mutex::new(HashMap::new()),
            shutdown,
            monitor_handle: std::sync::Mutex::new(None),
        }
    }

    /// Process an incoming AppMessage.
    ///
    /// All messages must be wrapped in a `SignedEnvelope`. The signature is
    /// verified first; then the payload is dispatched as either an
    /// `AuctionMessage` or an MPC tunnel message.
    pub async fn process_app_message(&self, message: Vec<u8>) -> MarketResult<()> {
        // Step 1: verify the signed envelope
        let (payload, signer) =
            super::bid_announcement::SignedEnvelope::verify_and_unwrap(&message)?;

        // Step 2: try to parse as AuctionMessage
        if let Ok(auction_msg) = AuctionMessage::from_bytes(&payload) {
            self.handle_auction_message(auction_msg, signer).await?;
            return Ok(());
        }

        // Step 3: forward to active MPC tunnel proxy.
        // Clone the proxy out of the lock before awaiting process_message
        // to avoid holding the Mutex across the .await point.
        let proxy = self.mpc.active_tunnel_proxy().lock().await.clone();
        if let Some(proxy) = proxy.as_ref() {
            proxy.process_message(payload, signer).await?;
        } else {
            // Tunnel proxy not ready yet — buffer the message so it's
            // delivered once the proxy is activated.  This handles the
            // case where faster parties send Opens/Data before this
            // node has started its MPC execution.
            debug!("MPC message buffered (tunnel proxy not ready)");
            self.mpc.buffer_mpc_message(payload, signer).await;
        }
        Ok(())
    }

    /// Handle auction coordination messages.
    ///
    /// `signer` is the Ed25519 verifying key bytes from the [`SignedEnvelope`].
    /// Identity binding to Veilid pubkey happens via `BidRecord.signing_pubkey`
    /// or `SellerEntry.signing_pubkey` stored in the DHT — the signed envelope
    /// itself proves the sender owns the signing key.
    async fn handle_auction_message(
        &self,
        message: AuctionMessage,
        signer: [u8; 32],
    ) -> MarketResult<()> {
        if !validate_timestamp(message.timestamp(), now_unix()) {
            warn!(
                "Rejecting message with stale/future timestamp (drift > 5 min): {}",
                message.timestamp()
            );
            return Ok(());
        }

        match message {
            AuctionMessage::BidAnnouncement {
                listing_key,
                bidder,
                bid_record_key,
                timestamp,
            } => {
                self.handle_bid_announcement(listing_key, bidder, bid_record_key, timestamp, signer)
                    .await
            }
            AuctionMessage::WinnerDecryptionRequest {
                listing_key,
                winner,
                timestamp: _,
            } => {
                self.handle_winner_decryption_request(listing_key, winner, signer)
                    .await
            }
            AuctionMessage::DecryptionHashTransfer {
                listing_key,
                winner,
                decryption_hash,
                timestamp: _,
            } => {
                self.handle_decryption_hash_transfer(listing_key, winner, decryption_hash, signer)
                    .await
            }
            AuctionMessage::MpcRouteAnnouncement {
                listing_key,
                party_pubkey,
                route_blob,
                timestamp: _,
            } => {
                self.handle_mpc_route_announcement(listing_key, party_pubkey, route_blob, signer)
                    .await
            }
            AuctionMessage::MpcReady {
                listing_key,
                party_pubkey,
                num_parties,
                timestamp,
            } => {
                self.handle_mpc_ready(listing_key, party_pubkey, num_parties, timestamp, signer)
                    .await
            }
            AuctionMessage::WinnerBidReveal {
                listing_key,
                winner,
                bid_value,
                nonce,
                timestamp: _,
            } => {
                self.handle_winner_bid_reveal(listing_key, winner, bid_value, nonce, signer)
                    .await
            }
            AuctionMessage::SellerRegistration {
                seller_pubkey,
                catalog_key,
                timestamp: _,
            } => {
                self.handle_seller_registration(seller_pubkey, catalog_key, signer)
                    .await
            }
            AuctionMessage::RegistryAnnouncement {
                registry_key,
                timestamp: _,
            } => self.handle_registry_announcement(registry_key).await,
        }
    }

    /// Push a signal into the per-listing buffer, evicting the oldest entry
    /// if the cap is reached.
    #[allow(clippy::significant_drop_tightening)]
    pub(super) async fn buffer_mpc_signal(
        &self,
        listing_key: RecordKey,
        signal: BufferedMpcSignal,
    ) {
        let mut buf = self.buffered_mpc_signals.lock().await;
        let entry = buf.entry(listing_key).or_default();
        if entry.len() >= MPC_SIGNAL_BUFFER_CAP {
            entry.remove(0);
        }
        entry.push(signal);
    }

    /// Watch a listing for deadline (if we're a bidder)
    pub async fn watch_listing(&self, listing: PublicListing) {
        let key = listing.key.clone();
        self.logic.watch_listing(listing).await;
        self.mpc.ensure_route_manager(&key).await;

        // Replay any MPC signals that arrived before the route manager existed.
        let signals = {
            let mut buf = self.buffered_mpc_signals.lock().await;
            buf.remove(&key).unwrap_or_default()
        };
        if !signals.is_empty() {
            let now = now_unix();
            let manager = {
                let managers = self.mpc.route_managers().lock().await;
                managers.get(&key).cloned()
            };
            if let Some(manager) = manager {
                let mut replayed = 0usize;
                for signal in signals {
                    match signal {
                        BufferedMpcSignal::RouteAnnouncement {
                            party_pubkey,
                            route_blob,
                            received_at,
                        } => {
                            if now.saturating_sub(received_at) <= MPC_SIGNAL_BUFFER_TTL {
                                if let Err(e) = manager
                                    .lock()
                                    .await
                                    .register_route_announcement(party_pubkey, route_blob)
                                    .await
                                {
                                    warn!("Failed to replay buffered route announcement: {}", e);
                                } else {
                                    replayed += 1;
                                }
                            }
                        }
                        BufferedMpcSignal::Ready {
                            party_pubkey,
                            num_parties,
                            timestamp,
                            received_at,
                        } => {
                            if now.saturating_sub(received_at) <= MPC_SIGNAL_BUFFER_TTL {
                                manager
                                    .lock()
                                    .await
                                    .register_ready(party_pubkey, num_parties, timestamp)
                                    .await;
                                replayed += 1;
                            }
                        }
                    }
                }
                if replayed > 0 {
                    info!(
                        "Replayed {} buffered MPC signals for listing {}",
                        replayed, key
                    );
                }
            }
        }
    }

    /// Register an owned listing (for sellers who created it)
    pub async fn register_owned_listing(&self, record: OwnedDHTRecord) -> MarketResult<()> {
        let key = record.key.clone();

        // Initialize empty bid registry
        let registry = BidAnnouncementRegistry::new();
        let data = registry.to_bytes()?;

        self.dht
            .set_value_at_subkey(&record, subkeys::BID_ANNOUNCEMENTS, data)
            .await?;
        info!("Initialized bid registry for owned listing: {}", key);

        let mut owned = self.owned_listings.lock().await;
        owned.insert(key.clone(), record.clone());
        drop(owned);

        let listing_ops = ListingOperations::new(self.dht.clone());
        if let Ok(Some(listing)) = listing_ops.get_listing(&key).await {
            self.watch_listing(listing).await;
            info!("Seller now watching their own listing: {}", key);
        }

        Ok(())
    }

    /// Add our own bid to the DHT registry
    pub async fn add_own_bid_to_registry(
        &self,
        listing_key: &RecordKey,
        bidder: PublicKey,
        bid_record_key: RecordKey,
        timestamp: u64,
    ) -> MarketResult<()> {
        let record = {
            let owned = self.owned_listings.lock().await;
            owned.get(listing_key).cloned()
        };

        if let Some(record) = record {
            info!("We own this listing and bid on it, adding our bid to DHT registry");

            let lk = listing_key.clone();
            self.dht
                .read_modify_write_subkey(
                    &record,
                    subkeys::BID_ANNOUNCEMENTS,
                    |existing| {
                        let mut registry = existing
                            .and_then(|d| BidAnnouncementRegistry::from_bytes(&d).ok())
                            .unwrap_or_else(BidAnnouncementRegistry::new);
                        registry.add(bidder.clone(), bid_record_key.clone(), timestamp);
                        info!(
                            "Added our own bid to DHT registry for listing {}, now has {} announcements",
                            lk,
                            registry.announcements.len()
                        );
                        registry.to_bytes()
                    },
                )
                .await?;
        }

        Ok(())
    }

    /// Register a bid announcement locally
    pub async fn register_local_bid(
        &self,
        listing_key: &RecordKey,
        bidder: PublicKey,
        bid_record_key: RecordKey,
    ) {
        self.logic
            .register_bid_announcement(listing_key, bidder, bid_record_key)
            .await;
    }

    /// Get the number of bids for a listing from DHT registry (authoritative),
    /// falling back to local announcements if the DHT read fails.
    pub async fn get_bid_count(&self, listing_key: &RecordKey) -> usize {
        // Try DHT bid registry first (authoritative source written by seller)
        if let Ok(Some(data)) = self
            .dht
            .get_value_at_subkey(listing_key, subkeys::BID_ANNOUNCEMENTS, true)
            .await
        {
            if let Ok(registry) = BidAnnouncementRegistry::from_bytes(&data) {
                return registry.announcements.len();
            }
        }

        // Fall back to local announcements (delegated to AuctionLogic)
        self.logic.get_local_bid_count(listing_key).await
    }

    /// Check if the current node received a decryption key for a listing
    pub async fn get_decryption_key(&self, listing_key: &RecordKey) -> Option<String> {
        self.logic.get_decryption_key(listing_key).await
    }

    /// Store a decryption key locally (e.g. seller stores it at listing creation time).
    /// This is necessary because the DHT only stores `PublicListing` (no decryption key).
    pub async fn store_decryption_key(&self, listing_key: &RecordKey, decryption_key: String) {
        self.logic
            .store_decryption_key(listing_key, decryption_key)
            .await;
    }

    /// Fetch a listing and decrypt its content if we have the decryption key
    pub async fn fetch_and_decrypt_listing(&self, listing_key: &RecordKey) -> MarketResult<String> {
        let listing_data = self
            .dht
            .get_value(listing_key)
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to fetch listing: {e}")))?
            .ok_or_else(|| MarketError::NotFound("Listing not found".into()))?;

        let listing = PublicListing::from_cbor(&listing_data).map_err(|e| {
            MarketError::Serialization(format!("Failed to deserialize listing: {e}"))
        })?;

        let decryption_key = self.get_decryption_key(listing_key).await.ok_or_else(|| {
            MarketError::NotFound(
                "No decryption key available for this listing. You must win the auction first."
                    .into(),
            )
        })?;

        listing
            .decrypt_content_with_key(&decryption_key)
            .map_err(|e| MarketError::Crypto(format!("Failed to decrypt listing content: {e}")))
    }

    /// Ensure the master registry is available (create or use already-known key).
    pub async fn ensure_master_registry(&self) -> MarketResult<RecordKey> {
        // Fast path: check if key is already known without holding lock across .await
        {
            let ops = self.registry_ops.lock().await;
            if let Some(key) = ops.master_registry_key() {
                return Ok(key);
            }
        }
        // Slow path: need to create/open registry (startup only, infrequent)
        let mut ops = self.registry_ops.lock().await;
        ops.ensure_master_registry().await
    }

    /// Get a reference to the shared registry operations.
    pub const fn registry_ops(&self) -> &Arc<Mutex<RegistryOperations>> {
        &self.registry_ops
    }

    /// Get the signing key (for callers that need to sign on behalf of this node).
    pub const fn signing_key(&self) -> &SigningKey {
        &self.signing_key
    }

    /// Hex-encoded Ed25519 verifying key for this node's signing identity.
    pub fn signing_pubkey_hex(&self) -> String {
        hex::encode(self.signing_key.verifying_key().to_bytes())
    }

    /// Raw verifying key bytes.
    pub fn signing_pubkey_bytes(&self) -> [u8; 32] {
        self.signing_key.verifying_key().to_bytes()
    }

    /// Fetch all listings via two-hop discovery. Delegates to `registry_ops`.
    pub async fn fetch_all_listings(&self) -> MarketResult<Vec<RegistryEntry>> {
        let ops = self.registry_ops.lock().await;
        ops.fetch_all_listings().await
    }

    /// Get a reference to the Veilid API (needed for `app_call_reply` in main).
    pub const fn api(&self) -> &VeilidAPI {
        &self.api
    }

    /// Process an incoming `AppCall` (request/response pattern).
    ///
    /// Both MPC messages and AuctionMessages share the `SignedEnvelope` wire
    /// format.  We first try the MPC tunnel proxy (if active).  If the tunnel
    /// can't handle it — either because the payload isn't an `MpcMessage` or
    /// because the tunnel was already shut down — we try parsing as an
    /// `AuctionMessage` (covers `WinnerDecryptionRequest`, etc.).
    /// Returns NACK `[0x00]` only for genuine MPC messages when no tunnel
    /// exists (sender should retry).
    pub async fn process_app_call(&self, message: Vec<u8>) -> MarketResult<Vec<u8>> {
        debug!("process_app_call: {} bytes received", message.len());

        if let Ok((payload, signer)) =
            super::bid_announcement::SignedEnvelope::verify_and_unwrap(&message)
        {
            // Try AuctionMessage first (WinnerDecryptionRequest, etc.)
            if let Ok(auction_msg) = AuctionMessage::from_bytes(&payload) {
                self.handle_auction_message(auction_msg, signer).await?;
                return Ok(vec![0x01]);
            }

            // Not an AuctionMessage — must be an MPC tunnel message.
            let tunnel = self.mpc.active_tunnel_proxy().lock().await.clone();
            if let Some(tunnel) = tunnel {
                return tunnel.process_call(payload, signer).await;
            }

            // MPC message but tunnel not ready — NACK so sender retries.
            debug!("MPC message received but tunnel proxy not ready — sending NACK");
            return Ok(vec![0x00]);
        }

        // Not a signed envelope — fall back to normal processing.
        self.process_app_message(message).await?;
        Ok(vec![0x01])
    }
}
