use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use veilid_core::{
    PublicKey, RecordKey, RouteBlob, RouteId, Sequencing, Stability, Target, VeilidAPI,
    CRYPTO_KIND_VLD0,
};

use super::auction_logic::AuctionLogic;
use super::bid_announcement::{validate_timestamp, AuctionMessage, BidAnnouncementRegistry};
use super::bid_storage::BidStorage;
use super::dht::{DHTOperations, OwnedDHTRecord};
use super::listing_ops::ListingOperations;
use super::mpc_orchestrator::MpcOrchestrator;
use super::registry::{RegistryEntry, RegistryOperations};
use crate::config::{now_unix, subkeys};
use crate::error::{MarketError, MarketResult};
use crate::marketplace::PublicListing;
use crate::traits::{
    DhtStore, MessageTransport, MpcResult, MpcRunner, SystemTimeProvider, TransportTarget,
};

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
struct CoordinatorTransport;

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
struct CoordinatorMpcRunner;

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
pub struct AuctionCoordinator {
    api: VeilidAPI,
    dht: DHTOperations,
    my_node_id: PublicKey,
    /// Core auction state machine — the tested generic layer.
    logic: CoordinatorLogic,
    /// Per-seller registry operations (shared via Arc<Mutex>)
    registry_ops: Arc<Mutex<RegistryOperations>>,
    /// Listings we own (as seller): Map<listing_key, OwnedDHTRecord>
    owned_listings: Arc<Mutex<HashMap<RecordKey, OwnedDHTRecord>>>,
    /// MPC orchestrator (owns tunnel proxy, routes, verifications)
    mpc: Arc<MpcOrchestrator>,
    /// Buffered SellerRegistrations received before the registry key was known.
    pending_seller_registrations: Arc<Mutex<Vec<(PublicKey, RecordKey)>>>,
    /// Our own broadcast private route blob (published to the registry).
    my_route_blob: Mutex<Option<Vec<u8>>>,
    /// Token used to signal graceful shutdown of background tasks.
    shutdown: CancellationToken,
}

impl AuctionCoordinator {
    pub fn new(
        api: VeilidAPI,
        dht: DHTOperations,
        my_node_id: PublicKey,
        bid_storage: BidStorage,
        node_offset: u16,
        network_key: &str,
        shutdown: CancellationToken,
    ) -> Self {
        let mpc = Arc::new(MpcOrchestrator::new(
            api.clone(),
            dht.clone(),
            my_node_id.clone(),
            bid_storage.clone(),
            node_offset,
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
            logic,
            registry_ops,
            owned_listings: Arc::new(Mutex::new(HashMap::new())),
            mpc,
            pending_seller_registrations: Arc::new(Mutex::new(Vec::new())),
            my_route_blob: Mutex::new(None),
            shutdown,
        }
    }

    /// Process an incoming AppMessage
    pub async fn process_app_message(&self, message: Vec<u8>) -> MarketResult<()> {
        // Try to parse as AuctionMessage first
        if let Ok(auction_msg) = AuctionMessage::from_bytes(&message) {
            self.handle_auction_message(auction_msg).await?;
            return Ok(());
        }

        // Otherwise, forward to active MPC tunnel proxy
        if let Some(proxy) = self.mpc.active_tunnel_proxy().lock().await.as_ref() {
            proxy.process_message(message).await?;
        }
        Ok(())
    }

    /// Handle auction coordination messages
    async fn handle_auction_message(&self, message: AuctionMessage) -> MarketResult<()> {
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
                self.handle_bid_announcement(listing_key, bidder, bid_record_key, timestamp)
                    .await
            }
            AuctionMessage::WinnerDecryptionRequest {
                listing_key,
                winner,
                timestamp: _,
            } => {
                self.handle_winner_decryption_request(listing_key, winner)
                    .await
            }
            AuctionMessage::DecryptionHashTransfer {
                listing_key,
                winner,
                decryption_hash,
                timestamp: _,
            } => {
                self.handle_decryption_hash_transfer(listing_key, winner, decryption_hash)
                    .await
            }
            AuctionMessage::MpcRouteAnnouncement {
                listing_key,
                party_pubkey,
                route_blob,
                timestamp: _,
            } => {
                self.handle_mpc_route_announcement(listing_key, party_pubkey, route_blob)
                    .await
            }
            AuctionMessage::WinnerBidReveal {
                listing_key,
                winner,
                bid_value,
                nonce,
                timestamp: _,
            } => {
                self.handle_winner_bid_reveal(listing_key, winner, bid_value, nonce)
                    .await
            }
            AuctionMessage::SellerRegistration {
                seller_pubkey,
                catalog_key,
                timestamp: _,
            } => {
                self.handle_seller_registration(seller_pubkey, catalog_key)
                    .await
            }
            AuctionMessage::RegistryAnnouncement {
                registry_key,
                timestamp: _,
            } => self.handle_registry_announcement(registry_key).await,
        }
    }

    async fn handle_bid_announcement(
        &self,
        listing_key: RecordKey,
        bidder: PublicKey,
        bid_record_key: RecordKey,
        timestamp: u64,
    ) -> MarketResult<()> {
        info!(
            "Received bid announcement for listing {} from bidder {}",
            listing_key, bidder
        );

        // Delegate local announcement tracking to AuctionLogic
        self.logic
            .register_bid_announcement(&listing_key, bidder.clone(), bid_record_key.clone())
            .await;

        // If we own this listing, update DHT bid registry
        let record = {
            let owned = self.owned_listings.lock().await;
            owned.get(&listing_key).cloned()
        };
        if let Some(record) = record {
            info!("We own this listing, updating DHT bid registry");

            let current_data = self
                .dht
                .get_value_at_subkey(&listing_key, subkeys::BID_ANNOUNCEMENTS, true)
                .await?;
            let mut registry = if let Some(data) = current_data {
                BidAnnouncementRegistry::from_bytes(&data)?
            } else {
                warn!("No existing registry found, creating new one");
                BidAnnouncementRegistry::new()
            };

            registry.add(bidder.clone(), bid_record_key.clone(), timestamp);

            let data = registry.to_bytes()?;
            self.dht
                .set_value_at_subkey(&record, subkeys::BID_ANNOUNCEMENTS, data)
                .await?;

            info!(
                "Updated DHT bid registry for listing {}, now has {} announcements",
                listing_key,
                registry.announcements.len()
            );
        }
        Ok(())
    }

    async fn handle_winner_decryption_request(
        &self,
        listing_key: RecordKey,
        _sender: PublicKey,
    ) -> MarketResult<()> {
        info!(
            "Received WinnerDecryptionRequest (challenge) for listing {}",
            listing_key
        );

        let bid_data = self.mpc.bid_storage().get_bid(&listing_key).await;
        match bid_data {
            Some((bid_value, nonce)) => {
                info!(
                    "Responding to seller's challenge with bid reveal (value: {})",
                    bid_value
                );

                let managers = self.mpc.route_managers().lock().await;

                if let Some(route_manager) = managers.get(&listing_key) {
                    let mgr = route_manager.lock().await;
                    let routes = mgr.received_routes.lock().await;

                    let listing_ops = ListingOperations::new(self.dht.clone());
                    let seller_pubkey = match listing_ops.get_listing(&listing_key).await {
                        Ok(Some(listing)) => Some(listing.seller),
                        _ => None,
                    };

                    let seller_route = seller_pubkey
                        .as_ref()
                        .and_then(|seller| routes.get(seller).cloned());

                    drop(routes);
                    drop(mgr);
                    drop(managers);

                    if let Some(route_id) = seller_route {
                        let message = AuctionMessage::winner_bid_reveal(
                            listing_key.clone(),
                            self.my_node_id.clone(),
                            bid_value,
                            nonce,
                        );

                        let data = message.to_bytes()?;
                        let routing_context = self.mpc.safe_routing_context()?;

                        match routing_context
                            .app_message(Target::RouteId(route_id), data)
                            .await
                        {
                            Ok(()) => {
                                info!("Sent WinnerBidReveal to seller via MPC route");
                            }
                            Err(e) => {
                                error!("Failed to send WinnerBidReveal to seller: {}", e);
                            }
                        }
                    } else {
                        warn!("Seller's MPC route not found, cannot respond to challenge");
                    }
                } else {
                    warn!(
                        "No route manager for listing {}, cannot respond to challenge",
                        listing_key
                    );
                }
            }
            None => {
                debug!(
                    "No bid stored for listing {}, ignoring challenge",
                    listing_key
                );
            }
        }
        Ok(())
    }

    async fn handle_decryption_hash_transfer(
        &self,
        listing_key: RecordKey,
        winner: PublicKey,
        decryption_hash: String,
    ) -> MarketResult<()> {
        info!(
            "Received decryption hash transfer for listing {} (winner: {})",
            listing_key, winner
        );

        // Delegate state management to AuctionLogic
        self.logic
            .process_decryption_transfer(listing_key.clone(), winner.clone(), decryption_hash)
            .await?;

        if winner == self.my_node_id {
            self.mpc.cleanup_route_manager(&listing_key).await;
        }
        Ok(())
    }

    async fn handle_mpc_route_announcement(
        &self,
        listing_key: RecordKey,
        party_pubkey: PublicKey,
        route_blob: RouteBlob,
    ) -> MarketResult<()> {
        info!(
            "Received MPC route announcement for listing {} from party {}",
            listing_key, party_pubkey
        );

        let manager = {
            let managers = self.mpc.route_managers().lock().await;
            managers.get(&listing_key).cloned()
        };

        if let Some(manager) = manager {
            manager
                .lock()
                .await
                .register_route_announcement(party_pubkey, route_blob)
                .await?;
        } else {
            debug!("Received route for unwatched auction {}", listing_key);
        }
        Ok(())
    }

    async fn handle_winner_bid_reveal(
        &self,
        listing_key: RecordKey,
        winner: PublicKey,
        bid_value: u64,
        nonce: [u8; 32],
    ) -> MarketResult<()> {
        info!(
            "Received WinnerBidReveal for listing {} from winner {}",
            listing_key, winner
        );

        let verified = self
            .mpc
            .verify_winner_reveal(&listing_key, &winner, bid_value, &nonce)
            .await;

        // Store verification result
        {
            let mut verifications = self.mpc.pending_verifications().lock().await;
            if let Some(state) = verifications.get_mut(&listing_key) {
                state.verified = Some(verified);
            } else {
                warn!(
                    "No pending verification entry for listing {} — result dropped",
                    listing_key
                );
            }
        }

        if verified {
            info!(
                "Winner bid VERIFIED for listing {} — sending decryption key immediately",
                listing_key
            );

            // The decryption key is stored locally by the seller when they create the listing.
            // It is never published to the DHT (PublicListing omits it).
            match self.get_decryption_key(&listing_key).await {
                Some(dec_key) => {
                    if let Err(e) = self
                        .mpc
                        .send_decryption_hash(&listing_key, &winner, &dec_key)
                        .await
                    {
                        error!("Failed to send decryption key to winner: {}", e);
                    }
                }
                None => {
                    warn!(
                        "Decryption key not found locally for listing {}",
                        listing_key
                    );
                }
            }
        } else {
            warn!(
                "Winner bid verification FAILED for listing {} — withholding decryption key",
                listing_key
            );
        }
        Ok(())
    }

    async fn handle_seller_registration(
        &self,
        seller_pubkey: PublicKey,
        catalog_key: RecordKey,
    ) -> MarketResult<()> {
        info!(
            "Received SellerRegistration from {} with catalog {}",
            seller_pubkey, catalog_key
        );
        let mut ops = self.registry_ops.lock().await;
        if ops.master_registry_key().is_some() {
            if let Err(e) = ops
                .register_seller(&seller_pubkey.to_string(), &catalog_key.to_string())
                .await
            {
                warn!("Failed to register seller {}: {}", seller_pubkey, e);
            }
        } else {
            info!(
                "Master registry key not yet known, buffering SellerRegistration from {}",
                seller_pubkey
            );
            let mut pending = self.pending_seller_registrations.lock().await;
            pending.push((seller_pubkey, catalog_key));
        }
        Ok(())
    }

    async fn handle_registry_announcement(&self, registry_key: RecordKey) -> MarketResult<()> {
        let mut ops = self.registry_ops.lock().await;
        let existing = ops.master_registry_key();
        if existing.is_none() {
            info!(
                "Received RegistryAnnouncement, setting master registry key: {}",
                registry_key
            );
            ops.set_master_registry_key(registry_key);

            // Replay any SellerRegistrations that arrived before we knew the key
            let pending = {
                let mut buf = self.pending_seller_registrations.lock().await;
                std::mem::take(&mut *buf)
            };
            if !pending.is_empty() {
                info!("Replaying {} buffered SellerRegistrations", pending.len());
                for (seller_pubkey, catalog_key) in pending {
                    if let Err(e) = ops
                        .register_seller(&seller_pubkey.to_string(), &catalog_key.to_string())
                        .await
                    {
                        warn!(
                            "Failed to register buffered seller {}: {}",
                            seller_pubkey, e
                        );
                    }
                }
            }
        } else if existing.as_ref() == Some(&registry_key) {
            debug!("Received duplicate RegistryAnnouncement, ignoring");
        } else {
            debug!(
                "Received RegistryAnnouncement with different key {}, keeping first-write-wins key",
                registry_key
            );
        }
        drop(ops);
        Ok(())
    }

    /// Watch a listing for deadline (if we're a bidder)
    pub async fn watch_listing(&self, listing: PublicListing) {
        let key = listing.key.clone();
        self.logic.watch_listing(listing).await;
        self.mpc.ensure_route_manager(&key).await;
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

            let current_data = self
                .dht
                .get_value_at_subkey(listing_key, subkeys::BID_ANNOUNCEMENTS, true)
                .await?;
            let mut registry = if let Some(data) = current_data {
                BidAnnouncementRegistry::from_bytes(&data)?
            } else {
                warn!("No existing registry found for owned listing, creating new one");
                BidAnnouncementRegistry::new()
            };

            registry.add(bidder.clone(), bid_record_key.clone(), timestamp);

            let data = registry.to_bytes()?;
            self.dht
                .set_value_at_subkey(&record, subkeys::BID_ANNOUNCEMENTS, data)
                .await?;

            info!(
                "Added our own bid to DHT registry for listing {}, now has {} announcements",
                listing_key,
                registry.announcements.len()
            );
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

    /// Broadcast our bid announcement to all known peers via AppMessage
    pub async fn broadcast_bid_announcement(
        &self,
        listing_key: &RecordKey,
        bid_record_key: &RecordKey,
    ) -> MarketResult<()> {
        info!(
            "Broadcasting bid announcement for listing {} to all peers",
            listing_key
        );

        let announcement = AuctionMessage::bid_announcement(
            listing_key.clone(),
            self.my_node_id.clone(),
            bid_record_key.clone(),
        );

        let data = announcement.to_bytes()?;
        let sent = self.broadcast_message(&data).await?;

        if sent == 0 {
            warn!("No peers found to send bid announcement");
        } else {
            info!("Broadcast completed: sent to {} peers", sent);
        }

        Ok(())
    }

    /// Broadcast a message to all known peers via private routes stored in the
    /// master registry.  Without the `footgun` feature Veilid rejects
    /// `Target::NodeId` for `app_message`, so we import each peer's route blob,
    /// send to `Target::RouteId`, and release the imported route afterwards.
    async fn broadcast_message(&self, data: &[u8]) -> MarketResult<usize> {
        let route_blobs = {
            let ops = self.registry_ops.lock().await;
            ops.fetch_route_blobs(&self.my_node_id.to_string()).await?
        };

        if route_blobs.is_empty() {
            warn!("No peer route blobs in registry, nothing to broadcast");
            return Ok(0);
        }

        let routing_context = self.api.routing_context()?;

        let mut sent_count = 0;
        for (node_id, blob) in &route_blobs {
            match self.api.import_remote_private_route(blob.clone()) {
                Ok(route_id) => {
                    match routing_context
                        .app_message(Target::RouteId(route_id.clone()), data.to_vec())
                        .await
                    {
                        Ok(()) => {
                            sent_count += 1;
                        }
                        Err(e) => {
                            debug!("Failed to send to node {} via route: {}", node_id, e);
                        }
                    }
                    let _ = self.api.release_private_route(route_id);
                }
                Err(e) => {
                    debug!("Failed to import route for node {}: {}", node_id, e);
                }
            }
        }

        if sent_count == 0 {
            warn!("Broadcast failed for all {} route blobs", route_blobs.len());
        }

        Ok(sent_count)
    }

    /// Ensure the master registry is available (create or use already-known key).
    pub async fn ensure_master_registry(&self) -> MarketResult<RecordKey> {
        let mut ops = self.registry_ops.lock().await;
        ops.ensure_master_registry().await
    }

    /// Broadcast a `SellerRegistration` to all peers.
    pub async fn broadcast_seller_registration(&self, catalog_key: &RecordKey) -> MarketResult<()> {
        info!(
            "Broadcasting SellerRegistration with catalog {}",
            catalog_key
        );
        let msg = AuctionMessage::seller_registration(self.my_node_id.clone(), catalog_key.clone());
        let data = msg.to_bytes()?;
        let sent = self.broadcast_message(&data).await?;
        info!("Sent SellerRegistration to {} peers", sent);
        Ok(())
    }

    /// Broadcast the master registry DHT key to all peers.
    ///
    /// If the registry key is not yet known, this is a no-op.
    pub async fn broadcast_registry_announcement(&self) -> MarketResult<()> {
        let key = {
            let ops = self.registry_ops.lock().await;
            ops.master_registry_key()
        };
        let Some(registry_key) = key else {
            debug!("No master registry key to broadcast");
            return Ok(());
        };
        info!(
            "Broadcasting RegistryAnnouncement with key {}",
            registry_key
        );
        let msg = AuctionMessage::registry_announcement(registry_key);
        let data = msg.to_bytes()?;
        let sent = self.broadcast_message(&data).await?;
        info!("Sent RegistryAnnouncement to {} peers", sent);
        Ok(())
    }

    /// Get a reference to the shared registry operations.
    pub const fn registry_ops(&self) -> &Arc<Mutex<RegistryOperations>> {
        &self.registry_ops
    }

    /// Fetch all listings via two-hop discovery. Delegates to `registry_ops`.
    pub async fn fetch_all_listings(&self) -> MarketResult<Vec<RegistryEntry>> {
        let ops = self.registry_ops.lock().await;
        ops.fetch_all_listings().await
    }

    /// Return a clone of the shutdown token.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Create a private route for this node and register it in the master registry
    /// so that other nodes can send us messages via `Target::RouteId`.
    async fn create_and_register_broadcast_route(&self) -> MarketResult<()> {
        let route_blob = self
            .api
            .new_custom_private_route(
                &[CRYPTO_KIND_VLD0],
                Stability::LowLatency,
                Sequencing::PreferOrdered,
            )
            .await
            .map_err(|e| MarketError::Network(format!("Failed to create broadcast route: {e}")))?;

        let blob_bytes = route_blob.blob.clone();
        info!(
            "Created broadcast private route: {} ({} bytes)",
            route_blob.route_id,
            blob_bytes.len()
        );

        // Store locally
        *self.my_route_blob.lock().await = Some(blob_bytes.clone());

        // Publish to master registry
        let mut ops = self.registry_ops.lock().await;
        ops.register_route(&self.my_node_id.to_string(), blob_bytes)
            .await?;
        drop(ops);

        info!("Broadcast route registered in master registry");
        Ok(())
    }

    /// Start background deadline monitoring.
    pub fn start_monitoring(self: Arc<Self>) {
        info!("Starting auction deadline monitor");
        let token = self.shutdown.clone();

        tokio::spawn(async move {
            // Every node independently opens/creates the deterministic master
            // registry on startup.  The key is derived from the shared network
            // keypair, so all nodes converge on the same DHT record without
            // needing a broadcast.
            match self.ensure_master_registry().await {
                Ok(key) => info!("Master registry ready at startup: {}", key),
                Err(e) => warn!("Failed to initialise master registry at startup: {}", e),
            }

            let mut broadcast_route_ready = false;

            let mut tick_count: u32 = 0;
            loop {
                tokio::select! {
                    () = token.cancelled() => {
                        info!("Auction monitor shutting down");
                        break;
                    }
                    () = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {}
                }
                tick_count = tick_count.wrapping_add(1);

                // Create broadcast route on first tick, then refresh every 6 ticks
                // (~60s) to prevent relay nodes from dropping stale routes.
                let should_refresh_route = !broadcast_route_ready || (tick_count % 6 == 0);
                if should_refresh_route {
                    match self.create_and_register_broadcast_route().await {
                        Ok(()) => {
                            if broadcast_route_ready {
                                debug!("Broadcast route refreshed (tick {})", tick_count);
                            } else {
                                info!("Broadcast route ready (tick {})", tick_count);
                            }
                            broadcast_route_ready = true;
                        }
                        Err(e) => {
                            warn!("Broadcast route not ready yet (tick {}): {}", tick_count, e);
                        }
                    }
                }

                // Re-broadcast registry key for late joiners:
                // every tick for the first 6 ticks (~60s), then every 3rd tick (~30s)
                let should_broadcast = tick_count <= 6 || tick_count % 3 == 0;
                if should_broadcast {
                    if let Err(e) = self.broadcast_registry_announcement().await {
                        debug!("Periodic registry broadcast failed: {}", e);
                    }
                }

                let expired = self.logic.get_expired_listings().await;

                for (key, listing) in expired {
                    info!("Auction deadline reached for listing '{}'", listing.title);

                    if let Err(e) = self.handle_auction_end_wrapper(&key, &listing).await {
                        error!(
                            "Failed to handle auction end for '{}': {}",
                            listing.title, e
                        );
                    }

                    self.logic.unwatch_listing(&key).await;
                }
            }
        });
    }

    /// Wrapper that prepares data for MpcOrchestrator::handle_auction_end
    async fn handle_auction_end_wrapper(
        &self,
        listing_key: &RecordKey,
        listing: &PublicListing,
    ) -> MarketResult<()> {
        // Check if we have a bid for this listing
        if !self.mpc.bid_storage().has_bid(listing_key).await {
            info!("We didn't bid on this listing, skipping MPC");
            return Ok(());
        }

        let our_bid_key = self
            .mpc
            .bid_storage()
            .get_bid_key(listing_key)
            .await
            .ok_or_else(|| MarketError::NotFound("Bid key not found in storage".into()))?;

        info!("We bid on this listing, our bid record: {}", our_bid_key);

        // Get local announcements as fallback (delegated to AuctionLogic)
        let local_announcements = self.logic.get_bid_announcements(listing_key).await;

        let bid_index = self
            .mpc
            .discover_bids_from_storage(listing_key, local_announcements.as_ref())
            .await?;

        // Refresh our broadcast route before MPC exchange — routes go stale
        // if relay nodes drop them, so create a fresh one now.
        if let Err(e) = self.create_and_register_broadcast_route().await {
            warn!("Failed to refresh broadcast route before MPC: {}", e);
        }

        // Small delay for peers to also refresh their routes
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        // Fetch peer route blobs for MPC route broadcasting (with force_refresh
        // to pick up any recently re-registered routes from peers)
        let peer_route_blobs = {
            let ops = self.registry_ops.lock().await;
            ops.fetch_route_blobs(&self.my_node_id.to_string())
                .await
                .unwrap_or_default()
        };

        self.mpc
            .handle_auction_end(listing_key, bid_index, &listing.title, &peer_route_blobs)
            .await
    }
}
