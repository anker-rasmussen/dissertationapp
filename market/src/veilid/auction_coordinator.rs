use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use veilid_core::{
    PublicKey, RecordKey, RouteBlob, SafetySelection, Sequencing, Target, VeilidAPI,
};

use super::bid_announcement::{AuctionMessage, BidAnnouncementRegistry};
use super::bid_storage::BidStorage;
use super::dht::{DHTOperations, OwnedDHTRecord};
use super::listing_ops::ListingOperations;
use super::mpc_orchestrator::MpcOrchestrator;
use super::registry::{RegistryEntry, RegistryOperations};
use crate::config::subkeys;
use crate::marketplace::PublicListing;
use crate::traits::DhtStore;

type BidAnnouncementMap = Arc<Mutex<HashMap<String, Vec<(PublicKey, RecordKey)>>>>;

/// Coordinates auction execution: monitors deadlines, triggers MPC
pub struct AuctionCoordinator {
    api: VeilidAPI,
    dht: DHTOperations,
    my_node_id: PublicKey,
    /// Per-seller registry operations (shared via Arc<Mutex>)
    registry_ops: Arc<Mutex<RegistryOperations>>,
    /// Listings we're monitoring
    watched_listings: Arc<Mutex<HashMap<RecordKey, PublicListing>>>,
    /// Listings we own (as seller): Map<listing_key, OwnedDHTRecord>
    owned_listings: Arc<Mutex<HashMap<RecordKey, OwnedDHTRecord>>>,
    /// Collected bid announcements: Map<listing_key, Vec<(bidder, bid_record_key)>>
    bid_announcements: BidAnnouncementMap,
    /// Received decryption keys for won auctions: Map<listing_key, decryption_key_hex>
    decryption_keys: Arc<Mutex<HashMap<String, String>>>,
    /// MPC orchestrator (owns tunnel proxy, routes, verifications)
    mpc: Arc<MpcOrchestrator>,
    /// Buffered SellerRegistrations received before the registry key was known.
    /// Replayed once a `RegistryAnnouncement` sets the key.
    pending_seller_registrations: Arc<Mutex<Vec<(PublicKey, RecordKey)>>>,
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
            bid_storage,
            node_offset,
        ));

        let registry_ops = Arc::new(Mutex::new(RegistryOperations::new(
            dht.clone(),
            network_key,
        )));

        Self {
            api,
            dht,
            my_node_id,
            registry_ops,
            watched_listings: Arc::new(Mutex::new(HashMap::new())),
            owned_listings: Arc::new(Mutex::new(HashMap::new())),
            bid_announcements: Arc::new(Mutex::new(HashMap::new())),
            decryption_keys: Arc::new(Mutex::new(HashMap::new())),
            mpc,
            pending_seller_registrations: Arc::new(Mutex::new(Vec::new())),
            shutdown,
        }
    }

    /// Process an incoming AppMessage
    pub async fn process_app_message(&self, message: Vec<u8>) -> anyhow::Result<()> {
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
    async fn handle_auction_message(&self, message: AuctionMessage) -> Result<()> {
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
    ) -> Result<()> {
        info!(
            "Received bid announcement for listing {} from bidder {}",
            listing_key, bidder
        );

        // Store the announcement locally
        let key = listing_key.to_string();
        let mut announcements = self.bid_announcements.lock().await;
        let list = announcements.entry(key.clone()).or_insert_with(Vec::new);

        if !list.iter().any(|(b, _)| b == &bidder) {
            list.push((bidder.clone(), bid_record_key.clone()));
            info!(
                "Stored bid announcement, total announcements for this listing: {}",
                list.len()
            );
        }
        drop(announcements);

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
    ) -> Result<()> {
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
    ) -> Result<()> {
        info!(
            "Received decryption hash transfer for listing {} (winner: {})",
            listing_key, winner
        );

        if winner == self.my_node_id {
            info!(
                "I am the auction winner! Received decryption key: {}",
                decryption_hash
            );

            let key = listing_key.to_string();
            let mut keys = self.decryption_keys.lock().await;
            keys.insert(key, decryption_hash);
            info!("Stored decryption key for listing {}", listing_key);

            drop(keys);
            self.mpc.cleanup_route_manager(&listing_key).await;
        } else {
            debug!("Decryption hash transfer not for me, ignoring");
        }
        Ok(())
    }

    async fn handle_mpc_route_announcement(
        &self,
        listing_key: RecordKey,
        party_pubkey: PublicKey,
        route_blob: RouteBlob,
    ) -> Result<()> {
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
    ) -> Result<()> {
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
    ) -> Result<()> {
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

    async fn handle_registry_announcement(&self, registry_key: RecordKey) -> Result<()> {
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
        let mut watched = self.watched_listings.lock().await;
        watched.insert(listing.key.clone(), listing.clone());
        info!(
            "Now watching listing '{}' for auction deadline",
            listing.title
        );
        drop(watched);

        self.mpc.ensure_route_manager(&listing.key).await;
    }

    /// Register an owned listing (for sellers who created it)
    pub async fn register_owned_listing(&self, record: OwnedDHTRecord) -> Result<()> {
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
    ) -> Result<()> {
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
        let key = listing_key.to_string();
        let mut announcements = self.bid_announcements.lock().await;
        let list = announcements.entry(key).or_insert_with(Vec::new);

        if !list.iter().any(|(b, _)| b == &bidder) {
            list.push((bidder, bid_record_key));
            info!(
                "Registered local bid announcement, total announcements for this listing: {}",
                list.len()
            );
        }
        drop(announcements);
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

        // Fall back to local announcements
        let key = listing_key.to_string();
        let announcements = self.bid_announcements.lock().await;
        announcements.get(&key).map_or(0, std::vec::Vec::len)
    }

    /// Check if the current node received a decryption key for a listing
    pub async fn get_decryption_key(&self, listing_key: &RecordKey) -> Option<String> {
        let key = listing_key.to_string();
        let keys = self.decryption_keys.lock().await;
        keys.get(&key).cloned()
    }

    /// Store a decryption key locally (e.g. seller stores it at listing creation time).
    /// This is necessary because the DHT only stores `PublicListing` (no decryption key).
    pub async fn store_decryption_key(&self, listing_key: &RecordKey, decryption_key: String) {
        let key = listing_key.to_string();
        let mut keys = self.decryption_keys.lock().await;
        keys.insert(key, decryption_key);
    }

    /// Fetch a listing and decrypt its content if we have the decryption key
    pub async fn fetch_and_decrypt_listing(&self, listing_key: &RecordKey) -> Result<String> {
        let listing_data = self
            .dht
            .get_value(listing_key)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch listing: {e}"))?
            .ok_or_else(|| anyhow::anyhow!("Listing not found"))?;

        let listing = PublicListing::from_cbor(&listing_data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize listing: {e}"))?;

        let decryption_key = self.get_decryption_key(listing_key).await.ok_or_else(|| {
            anyhow::anyhow!(
                "No decryption key available for this listing. You must win the auction first."
            )
        })?;

        listing
            .decrypt_content_with_key(&decryption_key)
            .map_err(|e| anyhow::anyhow!("Failed to decrypt listing content: {e}"))
    }

    /// Broadcast our bid announcement to all known peers via AppMessage
    pub async fn broadcast_bid_announcement(
        &self,
        listing_key: &RecordKey,
        bid_record_key: &RecordKey,
    ) -> Result<()> {
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

    /// Broadcast a message to all known peers.
    async fn broadcast_message(&self, data: &[u8]) -> Result<usize> {
        let routing_context = self
            .api
            .routing_context()
            .map_err(|e| anyhow::anyhow!("Failed to create routing context: {e}"))?
            .with_safety(SafetySelection::Unsafe(Sequencing::PreferOrdered))
            .map_err(|e| anyhow::anyhow!("Failed to set safety: {e}"))?;

        let state = self
            .api
            .get_state()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get state: {e}"))?;

        let mut sent_count = 0;
        for peer in &state.network.peers {
            for node_id in &peer.node_ids {
                match routing_context
                    .app_message(Target::NodeId(node_id.clone()), data.to_vec())
                    .await
                {
                    Ok(()) => {
                        sent_count += 1;
                        break;
                    }
                    Err(e) => {
                        debug!("Failed to send to peer {}: {}", node_id, e);
                    }
                }
            }
        }
        Ok(sent_count)
    }

    /// Ensure the master registry is available (create or use already-known key).
    pub async fn ensure_master_registry(&self) -> Result<RecordKey> {
        let mut ops = self.registry_ops.lock().await;
        Ok(ops.ensure_master_registry().await?)
    }

    /// Broadcast a `SellerRegistration` to all peers.
    pub async fn broadcast_seller_registration(&self, catalog_key: &RecordKey) -> Result<()> {
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
    pub async fn broadcast_registry_announcement(&self) -> Result<()> {
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
    pub async fn fetch_all_listings(&self) -> Result<Vec<RegistryEntry>> {
        let ops = self.registry_ops.lock().await;
        Ok(ops.fetch_all_listings().await?)
    }

    /// Return a clone of the shutdown token.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Start background deadline monitoring.
    pub fn start_monitoring(self: Arc<Self>) {
        info!("Starting auction deadline monitor");
        let token = self.shutdown.clone();

        tokio::spawn(async move {
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

                // Re-broadcast registry key for late joiners:
                // every tick for the first 6 ticks (~60s), then every 3rd tick (~30s)
                let should_broadcast = tick_count <= 6 || tick_count % 3 == 0;
                if should_broadcast {
                    if let Err(e) = self.broadcast_registry_announcement().await {
                        debug!("Periodic registry broadcast failed: {}", e);
                    }
                }

                let watched = self.watched_listings.lock().await.clone();

                for (key, listing) in watched {
                    if listing.time_remaining() == 0 {
                        info!("Auction deadline reached for listing '{}'", listing.title);

                        if let Err(e) = self.handle_auction_end_wrapper(&key, &listing).await {
                            error!(
                                "Failed to handle auction end for '{}': {}",
                                listing.title, e
                            );
                        }

                        self.watched_listings.lock().await.remove(&key);
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
    ) -> Result<()> {
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
            .ok_or_else(|| anyhow::anyhow!("Bid key not found in storage"))?;

        info!("We bid on this listing, our bid record: {}", our_bid_key);

        // Get local announcements as fallback
        let local_announcements = {
            let key = listing_key.to_string();
            let announcements = self.bid_announcements.lock().await;
            announcements.get(&key).cloned()
        };

        let bid_index = self
            .mpc
            .discover_bids_from_storage(listing_key, local_announcements.as_ref())
            .await?;

        self.mpc
            .handle_auction_end(listing_key, bid_index, &listing.title)
            .await
    }
}
