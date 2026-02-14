use async_trait::async_trait;
use ed25519_dalek::SigningKey;
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
use super::bid_ops::BidOperations;
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
    api: VeilidAPI,
    dht: DHTOperations,
    my_node_id: PublicKey,
    /// Ed25519 signing key for authenticating outgoing messages.
    signing_key: SigningKey,
    /// Core auction state machine — the tested generic layer.
    logic: CoordinatorLogic,
    /// Per-seller registry operations (shared via Arc<Mutex>)
    registry_ops: Arc<Mutex<RegistryOperations>>,
    /// Listings we own (as seller): Map<listing_key, OwnedDHTRecord>
    owned_listings: Arc<Mutex<HashMap<RecordKey, OwnedDHTRecord>>>,
    /// MPC orchestrator (owns tunnel proxy, routes, verifications)
    mpc: Arc<MpcOrchestrator>,
    /// Buffered SellerRegistrations received before the registry key was known.
    /// Tuple: (seller_pubkey, catalog_key, signing_pubkey_hex)
    pending_seller_registrations: Arc<Mutex<Vec<(PublicKey, RecordKey, String)>>>,
    /// Bid announcements that failed to broadcast (no routes yet) and need retry.
    pending_bid_announcements: Mutex<Vec<(RecordKey, RecordKey)>>,
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
            shutdown,
        }
    }

    /// Process an incoming AppMessage.
    ///
    /// All messages must be wrapped in a [`SignedEnvelope`]. The signature is
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

    /// Resolve the expected signing key for a bidder in this listing, if known.
    async fn expected_bidder_signing_key(
        &self,
        listing_key: &RecordKey,
        bidder: &PublicKey,
    ) -> MarketResult<Option<[u8; 32]>> {
        let Some(registry_data) = self
            .dht
            .get_value_at_subkey(listing_key, subkeys::BID_ANNOUNCEMENTS, true)
            .await?
        else {
            return Ok(None);
        };

        let registry = BidAnnouncementRegistry::from_bytes(&registry_data)?;
        let Some(bid_record_key) = registry
            .announcements
            .iter()
            .find(|(b, _, _)| b == bidder)
            .map(|(_, key, _)| key.clone())
        else {
            return Ok(None);
        };

        let bid_ops = BidOperations::new(self.dht.clone());
        let Some(bid_record) = bid_ops.fetch_bid(&bid_record_key).await? else {
            return Ok(None);
        };

        if bid_record.listing_key != *listing_key || bid_record.bidder != *bidder {
            warn!(
                "Bid record identity mismatch for bidder {} on listing {}",
                bidder, listing_key
            );
            return Ok(None);
        }

        if bid_record.signing_pubkey == [0u8; 32] {
            warn!(
                "Bid record for bidder {} has empty signing key; rejecting",
                bidder
            );
            return Ok(None);
        }

        Ok(Some(bid_record.signing_pubkey))
    }

    /// Resolve the expected signing key for a listing's seller, if known.
    async fn seller_signing_key_for_listing(
        &self,
        listing_key: &RecordKey,
    ) -> MarketResult<Option<[u8; 32]>> {
        let listing_ops = ListingOperations::new(self.dht.clone());
        let Some(listing) = listing_ops.get_listing(listing_key).await? else {
            return Ok(None);
        };

        // Try the master registry first (populated by SellerRegistration broadcasts).
        let seller_pubkey = listing.seller.to_string();
        let registry_result = {
            let ops = self.registry_ops.lock().await;
            ops.get_seller_signing_pubkey(&seller_pubkey).await?
        };
        if registry_result.is_some() {
            return Ok(registry_result);
        }

        // Fallback: look up the seller's signing key from their bid record
        // in the bid announcement registry.  The seller always places a
        // reserve-price bid, so their signing_pubkey is available there.
        self.expected_bidder_signing_key(listing_key, &listing.seller)
            .await
    }

    /// Validate that a bid announcement message is signed by the bid owner.
    async fn validate_bid_announcement_signer(
        &self,
        listing_key: &RecordKey,
        bidder: &PublicKey,
        bid_record_key: &RecordKey,
        signer: [u8; 32],
    ) -> MarketResult<bool> {
        let bid_ops = BidOperations::new(self.dht.clone());
        let Some(bid_record) = bid_ops.fetch_bid(bid_record_key).await? else {
            warn!(
                "Rejecting bid announcement: bid record {} not found",
                bid_record_key
            );
            return Ok(false);
        };

        if bid_record.listing_key != *listing_key {
            warn!(
                "Rejecting bid announcement: listing mismatch (msg {}, record {})",
                listing_key, bid_record.listing_key
            );
            return Ok(false);
        }

        if bid_record.bidder != *bidder {
            warn!(
                "Rejecting bid announcement: bidder mismatch (msg {}, record {})",
                bidder, bid_record.bidder
            );
            return Ok(false);
        }

        if bid_record.bid_key != *bid_record_key {
            warn!(
                "Rejecting bid announcement: bid key mismatch (msg {}, record {})",
                bid_record_key, bid_record.bid_key
            );
            return Ok(false);
        }

        if bid_record.signing_pubkey == [0u8; 32] {
            warn!("Rejecting bid announcement: empty signing key in bid record");
            return Ok(false);
        }

        if bid_record.signing_pubkey != signer {
            warn!("Rejecting bid announcement: signer mismatch");
            return Ok(false);
        }

        Ok(true)
    }

    async fn handle_bid_announcement(
        &self,
        listing_key: RecordKey,
        bidder: PublicKey,
        bid_record_key: RecordKey,
        timestamp: u64,
        signer: [u8; 32],
    ) -> MarketResult<()> {
        info!(
            "Received bid announcement for listing {} from bidder {}",
            listing_key, bidder
        );

        if !self
            .validate_bid_announcement_signer(&listing_key, &bidder, &bid_record_key, signer)
            .await?
        {
            return Ok(());
        }

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
        claimed_winner: PublicKey,
        signer: [u8; 32],
    ) -> MarketResult<()> {
        info!(
            "Received WinnerDecryptionRequest (challenge) for listing {}",
            listing_key
        );

        if claimed_winner != self.my_node_id {
            debug!(
                "Ignoring WinnerDecryptionRequest for different winner {}",
                claimed_winner
            );
            return Ok(());
        }

        let Some(expected_seller_signer) =
            self.seller_signing_key_for_listing(&listing_key).await?
        else {
            warn!("Rejecting challenge for listing {listing_key}: seller signing key not found");
            return Ok(());
        };
        if expected_seller_signer != signer {
            warn!("Rejecting challenge for listing {listing_key}: signer does not match seller");
            return Ok(());
        }

        if !self.mpc.is_expected_winner(&listing_key).await {
            warn!("Rejecting challenge for listing {listing_key}: node is not expected winner");
            return Ok(());
        }
        info!("Challenge accepted for listing {listing_key}: node is expected winner");

        let Some((bid_value, nonce)) = self.mpc.bid_storage().get_bid(&listing_key).await else {
            let has_bid_key = self
                .mpc
                .bid_storage()
                .get_bid_key(&listing_key)
                .await
                .is_some();
            warn!(
                "Cannot respond to challenge for listing {listing_key}: local bid value/nonce missing (bid key present: {has_bid_key})"
            );
            return Ok(());
        };
        info!("Challenge handling has local bid material for listing {listing_key}");

        info!("Responding to seller's challenge with bid reveal (value: {bid_value})");
        if let Err(e) = self
            .send_bid_reveal_to_seller(&listing_key, bid_value, nonce)
            .await
        {
            error!("Failed responding to challenge for listing {listing_key}: {e}");
            return Ok(());
        }
        Ok(())
    }

    /// Send a WinnerBidReveal to the seller via their MPC route.
    async fn send_bid_reveal_to_seller(
        &self,
        listing_key: &RecordKey,
        bid_value: u64,
        nonce: [u8; 32],
    ) -> MarketResult<()> {
        let listing_ops = ListingOperations::new(self.dht.clone());
        let seller_pubkey = match listing_ops.get_listing(listing_key).await {
            Ok(Some(listing)) => {
                info!("Resolved seller pubkey from listing for {listing_key}");
                Some(listing.seller)
            }
            Ok(None) => {
                warn!("Listing missing while sending reveal for {listing_key}");
                None
            }
            Err(e) => {
                warn!("Failed to resolve listing for reveal on {listing_key}: {e}");
                None
            }
        };

        let message = AuctionMessage::winner_bid_reveal(
            listing_key.clone(),
            self.my_node_id.clone(),
            bid_value,
            nonce,
        );

        let data = message.to_signed_bytes(&self.signing_key)?;
        let start = std::time::Instant::now();
        let max_wait = std::time::Duration::from_secs(20);

        loop {
            let route_manager = {
                let managers = self.mpc.route_managers().lock().await;
                managers.get(listing_key).cloned()
            };

            let Some(route_manager) = route_manager else {
                if start.elapsed() >= max_wait {
                    return Err(MarketError::Network(format!(
                        "No route manager for listing {listing_key} after waiting {max_wait:?}"
                    )));
                }
                warn!("No route manager for listing {listing_key}, retrying reveal send");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            };

            let seller_route = {
                let received_routes = {
                    let mgr = route_manager.lock().await;
                    mgr.received_routes.clone()
                };
                let routes = received_routes.lock().await;
                seller_pubkey
                    .as_ref()
                    .and_then(|seller| routes.get(seller).cloned())
            };

            let Some(route_id) = seller_route else {
                if start.elapsed() >= max_wait {
                    return Err(MarketError::Network(format!(
                        "Seller route unavailable for listing {listing_key} after waiting {max_wait:?}"
                    )));
                }
                warn!("Seller route not found for listing {listing_key}, retrying reveal send");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            };

            let routing_context = self.mpc.safe_routing_context()?;
            match routing_context
                .app_message(Target::RouteId(route_id), data.clone())
                .await
            {
                Ok(()) => {
                    info!("Sent WinnerBidReveal to seller via MPC route");
                    return Ok(());
                }
                Err(e) => {
                    if start.elapsed() >= max_wait {
                        return Err(MarketError::Network(format!(
                            "Failed to send WinnerBidReveal within {max_wait:?}: {e}"
                        )));
                    }
                    warn!("Failed to send WinnerBidReveal, retrying: {e}");
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn handle_decryption_hash_transfer(
        &self,
        listing_key: RecordKey,
        winner: PublicKey,
        decryption_hash: String,
        signer: [u8; 32],
    ) -> MarketResult<()> {
        info!(
            "Received decryption hash transfer for listing {} (winner: {})",
            listing_key, winner
        );

        let Some(expected_seller_signer) =
            self.seller_signing_key_for_listing(&listing_key).await?
        else {
            warn!(
                "Rejecting decryption transfer for listing {listing_key}: seller signing key not found"
            );
            return Ok(());
        };
        if expected_seller_signer != signer {
            warn!(
                "Rejecting decryption transfer for listing {listing_key}: signer does not match seller"
            );
            return Ok(());
        }

        // Delegate state management to AuctionLogic
        self.logic
            .process_decryption_transfer(listing_key.clone(), winner.clone(), decryption_hash)
            .await?;

        if winner == self.my_node_id {
            self.mpc.clear_expected_winner(&listing_key).await;
            self.mpc.cleanup_route_manager(&listing_key).await;
        }
        Ok(())
    }

    async fn handle_mpc_route_announcement(
        &self,
        listing_key: RecordKey,
        party_pubkey: PublicKey,
        route_blob: RouteBlob,
        signer: [u8; 32],
    ) -> MarketResult<()> {
        info!(
            "Received MPC route announcement for listing {} from party {}",
            listing_key, party_pubkey
        );

        let Some(expected_signer) = self
            .expected_bidder_signing_key(&listing_key, &party_pubkey)
            .await?
        else {
            warn!(
                "Rejecting MPC route announcement for listing {listing_key}: signer binding not found for party {party_pubkey}"
            );
            return Ok(());
        };
        if expected_signer != signer {
            warn!(
                "Rejecting MPC route announcement for listing {listing_key}: signer mismatch for party {party_pubkey}"
            );
            return Ok(());
        }

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
        signer: [u8; 32],
    ) -> MarketResult<()> {
        info!(
            "Received WinnerBidReveal for listing {} from winner {}",
            listing_key, winner
        );

        let Some(expected_signer) = self
            .expected_bidder_signing_key(&listing_key, &winner)
            .await?
        else {
            warn!(
                "Rejecting WinnerBidReveal for listing {listing_key}: signer binding not found for winner {winner}"
            );
            return Ok(());
        };
        if expected_signer != signer {
            warn!(
                "Rejecting WinnerBidReveal for listing {listing_key}: signer mismatch for winner {winner}"
            );
            return Ok(());
        }

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
        signer: [u8; 32],
    ) -> MarketResult<()> {
        let signing_hex = hex::encode(signer);
        info!(
            "Received SellerRegistration from {} with catalog {} (signer: {})",
            seller_pubkey, catalog_key, signing_hex
        );

        // Check registry state without holding lock across .await
        let has_registry = {
            let ops = self.registry_ops.lock().await;
            ops.master_registry_key().is_some()
        };

        if has_registry {
            // Re-acquire for the async register_seller call
            let mut ops = self.registry_ops.lock().await;
            if let Err(e) = ops
                .register_seller(
                    &seller_pubkey.to_string(),
                    &catalog_key.to_string(),
                    &signing_hex,
                )
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
            pending.push((seller_pubkey, catalog_key, signing_hex));
        }
        Ok(())
    }

    async fn handle_registry_announcement(&self, registry_key: RecordKey) -> MarketResult<()> {
        // Phase 1: set the key (short lock scope, no .await inside)
        let should_replay = {
            let mut ops = self.registry_ops.lock().await;
            let existing = ops.master_registry_key();
            if existing.is_none() {
                info!(
                    "Received RegistryAnnouncement, setting master registry key: {}",
                    registry_key
                );
                ops.set_master_registry_key(registry_key);
                drop(ops);
                true
            } else if existing.as_ref() == Some(&registry_key) {
                debug!("Received duplicate RegistryAnnouncement, ignoring");
                false
            } else {
                debug!(
                    "Received RegistryAnnouncement with different key {}, keeping first-write-wins key",
                    registry_key
                );
                false
            }
        };

        // Phase 2: drain pending registrations and replay (locks acquired separately)
        if should_replay {
            let pending = {
                let mut buf = self.pending_seller_registrations.lock().await;
                std::mem::take(&mut *buf)
            };
            if !pending.is_empty() {
                info!("Replaying {} buffered SellerRegistrations", pending.len());
                for (seller_pubkey, catalog_key, signing_hex) in pending {
                    let mut ops = self.registry_ops.lock().await;
                    if let Err(e) = ops
                        .register_seller(
                            &seller_pubkey.to_string(),
                            &catalog_key.to_string(),
                            &signing_hex,
                        )
                        .await
                    {
                        warn!(
                            "Failed to register buffered seller {}: {}",
                            seller_pubkey, e
                        );
                    }
                }
            }
        }
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

        let data = announcement.to_signed_bytes(&self.signing_key)?;
        let sent = self.broadcast_message(&data).await?;

        if sent == 0 {
            warn!("No peers found to send bid announcement, queuing for retry");
            self.pending_bid_announcements
                .lock()
                .await
                .push((listing_key.clone(), bid_record_key.clone()));
        } else {
            info!("Broadcast completed: sent to {} peers", sent);
        }

        Ok(())
    }

    /// Retry bid announcements that failed to broadcast (e.g. routes weren't ready).
    async fn retry_pending_bid_announcements(&self) {
        let pending = {
            let mut queue = self.pending_bid_announcements.lock().await;
            if queue.is_empty() {
                return;
            }
            std::mem::take(&mut *queue)
        };

        info!("Retrying {} pending bid announcement(s)", pending.len());

        for (listing_key, bid_record_key) in pending {
            let announcement = AuctionMessage::bid_announcement(
                listing_key.clone(),
                self.my_node_id.clone(),
                bid_record_key.clone(),
            );

            match announcement.to_signed_bytes(&self.signing_key) {
                Ok(data) => match self.broadcast_message(&data).await {
                    Ok(sent) if sent > 0 => {
                        info!(
                            "Retry succeeded: bid announcement for {} sent to {} peers",
                            listing_key, sent
                        );
                    }
                    Ok(_) => {
                        warn!("Retry failed: still no peers, re-queuing bid announcement");
                        self.pending_bid_announcements
                            .lock()
                            .await
                            .push((listing_key, bid_record_key));
                    }
                    Err(e) => {
                        warn!("Retry error for bid announcement: {}", e);
                        self.pending_bid_announcements
                            .lock()
                            .await
                            .push((listing_key, bid_record_key));
                    }
                },
                Err(e) => {
                    warn!("Failed to serialize bid announcement for retry: {}", e);
                }
            }
        }
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
        let data = msg.to_signed_bytes(&self.signing_key)?;
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
        let data = msg.to_signed_bytes(&self.signing_key)?;
        let sent = self.broadcast_message(&data).await?;
        info!("Sent RegistryAnnouncement to {} peers", sent);
        Ok(())
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
                    if self.mpc.has_active_auctions().await {
                        debug!(
                            "Skipping broadcast route refresh at tick {} while MPC is active",
                            tick_count
                        );
                    } else {
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
                }

                // Re-broadcast registry key for late joiners:
                // every tick for the first 6 ticks (~60s), then every 3rd tick (~30s)
                let should_broadcast = tick_count <= 6 || tick_count % 3 == 0;
                if should_broadcast {
                    if let Err(e) = self.broadcast_registry_announcement().await {
                        debug!("Periodic registry broadcast failed: {}", e);
                    }
                }

                // Retry any bid announcements that failed due to missing routes
                if broadcast_route_ready {
                    self.retry_pending_bid_announcements().await;
                }

                let expired = self.logic.get_expired_listings().await;

                for (key, listing) in expired {
                    info!("Auction deadline reached for listing '{}'", listing.title);

                    match self.handle_auction_end_wrapper(&key, &listing).await {
                        Ok(()) => self.logic.unwatch_listing(&key).await,
                        Err(e) => {
                            // Keep listing watched so it retries on the next tick.
                            // Transient failures (e.g. bid key not stored yet) resolve
                            // within a few seconds.
                            error!(
                                "Failed to handle auction end for '{}': {} (will retry)",
                                listing.title, e
                            );
                        }
                    }
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

        // Ensure we have a broadcast route published before MPC route exchange.
        // Avoid forced re-allocation here to reduce route churn during exchange.
        if self.my_route_blob.lock().await.is_none() {
            if let Err(e) = self.create_and_register_broadcast_route().await {
                warn!("Failed to create broadcast route before MPC: {}", e);
            }
        }

        // Fetch peer route blobs for MPC route broadcasting, waiting briefly for
        // peers to publish routes so party 0 doesn't start with an empty view.
        let expected_peer_routes = bid_index.bids.len().saturating_sub(1);
        let start = std::time::Instant::now();
        let max_wait = std::time::Duration::from_secs(15);
        let peer_route_blobs = loop {
            let blobs = {
                let ops = self.registry_ops.lock().await;
                ops.fetch_route_blobs(&self.my_node_id.to_string())
                    .await
                    .unwrap_or_default()
            };

            let found = blobs.len();
            if found >= expected_peer_routes {
                info!(
                    "Found {found}/{expected_peer_routes} peer route blobs before MPC for listing {}",
                    listing_key
                );
                break blobs;
            }

            if start.elapsed() >= max_wait {
                warn!(
                    "Proceeding with {found}/{expected_peer_routes} peer route blobs after {:?} wait for listing {}",
                    max_wait, listing_key
                );
                break blobs;
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        };

        self.mpc
            .handle_auction_end(listing_key, bid_index, &listing.title, &peer_route_blobs)
            .await
    }
}
