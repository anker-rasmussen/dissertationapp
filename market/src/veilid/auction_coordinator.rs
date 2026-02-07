use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use veilid_core::{PublicKey, RecordKey, SafetySelection, Sequencing, Target, VeilidAPI};

use super::bid_announcement::{AuctionMessage, BidAnnouncementRegistry};
use super::bid_storage::BidStorage;
use super::dht::{DHTOperations, OwnedDHTRecord};
use super::listing_ops::ListingOperations;
use super::mpc_orchestrator::MpcOrchestrator;
use crate::marketplace::Listing;
use crate::traits::DhtStore;

type BidAnnouncementMap = Arc<Mutex<HashMap<String, Vec<(PublicKey, RecordKey)>>>>;

/// Coordinates auction execution: monitors deadlines, triggers MPC
pub struct AuctionCoordinator {
    api: VeilidAPI,
    dht: DHTOperations,
    my_node_id: PublicKey,
    /// Listings we're monitoring
    watched_listings: Arc<Mutex<HashMap<RecordKey, Listing>>>,
    /// Listings we own (as seller): Map<listing_key, OwnedDHTRecord>
    owned_listings: Arc<Mutex<HashMap<RecordKey, OwnedDHTRecord>>>,
    /// Collected bid announcements: Map<listing_key, Vec<(bidder, bid_record_key)>>
    bid_announcements: BidAnnouncementMap,
    /// Received decryption keys for won auctions: Map<listing_key, decryption_key_hex>
    decryption_keys: Arc<Mutex<HashMap<String, String>>>,
    /// MPC orchestrator (owns sidecar, routes, verifications)
    mpc: Arc<MpcOrchestrator>,
}

impl AuctionCoordinator {
    pub fn new(
        api: VeilidAPI,
        dht: DHTOperations,
        my_node_id: PublicKey,
        bid_storage: BidStorage,
        node_offset: u16,
    ) -> Self {
        let mpc = Arc::new(MpcOrchestrator::new(
            api.clone(),
            dht.clone(),
            my_node_id.clone(),
            bid_storage,
            node_offset,
        ));

        Self {
            api,
            dht,
            my_node_id,
            watched_listings: Arc::new(Mutex::new(HashMap::new())),
            owned_listings: Arc::new(Mutex::new(HashMap::new())),
            bid_announcements: Arc::new(Mutex::new(HashMap::new())),
            decryption_keys: Arc::new(Mutex::new(HashMap::new())),
            mpc,
        }
    }

    /// Process an incoming AppMessage
    pub async fn process_app_message(&self, message: Vec<u8>) -> anyhow::Result<()> {
        // Try to parse as AuctionMessage first
        if let Ok(auction_msg) = AuctionMessage::from_bytes(&message) {
            self.handle_auction_message(auction_msg).await?;
            return Ok(());
        }

        // Otherwise, forward to active MPC sidecar
        if let Some(sidecar) = self.mpc.active_sidecar().lock().await.as_ref() {
            sidecar.process_message(message).await?;
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
                let owned = self.owned_listings.lock().await;
                if let Some(record) = owned.get(&listing_key) {
                    info!("We own this listing, updating DHT bid registry");

                    let current_data = self.dht.get_value_at_subkey(&listing_key, 2).await?;
                    let mut registry = match current_data {
                        Some(data) => BidAnnouncementRegistry::from_bytes(&data)?,
                        None => {
                            warn!("No existing registry found, creating new one");
                            BidAnnouncementRegistry::new()
                        }
                    };

                    registry.add(bidder.clone(), bid_record_key.clone(), timestamp);

                    let data = registry.to_bytes()?;
                    self.dht.set_value_at_subkey(record, 2, data).await?;

                    info!(
                        "Updated DHT bid registry for listing {}, now has {} announcements",
                        listing_key,
                        registry.announcements.len()
                    );
                }
            }
            AuctionMessage::WinnerDecryptionRequest {
                listing_key,
                winner: _sender,
                timestamp: _,
            } => {
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

                        let key = listing_key.to_string();
                        let managers = self.mpc.route_managers().lock().await;

                        if let Some(route_manager) = managers.get(&key) {
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
                                    Ok(_) => {
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
            }
            AuctionMessage::DecryptionHashTransfer {
                listing_key,
                winner,
                decryption_hash,
                timestamp: _,
            } => {
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
            }
            AuctionMessage::MpcRouteAnnouncement {
                listing_key,
                party_pubkey,
                route_blob,
                timestamp: _,
            } => {
                info!(
                    "Received MPC route announcement for listing {} from party {}",
                    listing_key, party_pubkey
                );

                let key = listing_key.to_string();
                let managers = self.mpc.route_managers().lock().await;

                if let Some(manager) = managers.get(&key) {
                    let mgr = manager.lock().await;
                    mgr.register_route_announcement(party_pubkey, route_blob)
                        .await?;
                } else {
                    debug!("Received route for unwatched auction {}", listing_key);
                }
            }
            AuctionMessage::WinnerBidReveal {
                listing_key,
                winner,
                bid_value,
                nonce,
                timestamp: _,
            } => {
                info!(
                    "Received WinnerBidReveal for listing {} from winner {}",
                    listing_key, winner
                );

                let verified = self
                    .mpc
                    .verify_winner_reveal(&listing_key, &winner, bid_value, &nonce)
                    .await;

                // Store verification result
                let key = listing_key.to_string();
                {
                    let mut verifications = self.mpc.pending_verifications().lock().await;
                    verifications
                        .entry(key.clone())
                        .and_modify(|(_, _, v)| *v = Some(verified));
                }

                if verified {
                    info!(
                        "Winner bid VERIFIED for listing {} — sending decryption key immediately",
                        listing_key
                    );

                    let listing_ops = ListingOperations::new(self.dht.clone());
                    match listing_ops.get_listing(&listing_key).await {
                        Ok(Some(listing)) => {
                            if let Err(e) = self
                                .mpc
                                .send_decryption_hash(
                                    &listing_key,
                                    &winner,
                                    &listing.decryption_key,
                                )
                                .await
                            {
                                error!("Failed to send decryption key to winner: {}", e);
                            }
                        }
                        Ok(None) => {
                            warn!("Listing not found when trying to send decryption key");
                        }
                        Err(e) => {
                            error!("Failed to retrieve listing: {}", e);
                        }
                    }
                } else {
                    warn!("Winner bid verification FAILED for listing {} — withholding decryption key", listing_key);
                }
            }
        }
        Ok(())
    }

    /// Watch a listing for deadline (if we're a bidder)
    pub async fn watch_listing(&self, listing: Listing) {
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

        // Initialize empty bid registry at subkey 2
        let registry = BidAnnouncementRegistry::new();
        let data = registry.to_bytes()?;

        self.dht.set_value_at_subkey(&record, 2, data).await?;
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
        let owned = self.owned_listings.lock().await;

        if let Some(record) = owned.get(listing_key) {
            info!("We own this listing and bid on it, adding our bid to DHT registry");

            let current_data = self.dht.get_value_at_subkey(listing_key, 2).await?;
            let mut registry = match current_data {
                Some(data) => BidAnnouncementRegistry::from_bytes(&data)?,
                None => {
                    warn!("No existing registry found for owned listing, creating new one");
                    BidAnnouncementRegistry::new()
                }
            };

            registry.add(bidder.clone(), bid_record_key.clone(), timestamp);

            let data = registry.to_bytes()?;
            self.dht.set_value_at_subkey(record, 2, data).await?;

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
    }

    /// Get the number of bids for a listing from local announcements
    pub async fn get_bid_count(&self, listing_key: &RecordKey) -> usize {
        let key = listing_key.to_string();
        let announcements = self.bid_announcements.lock().await;
        announcements.get(&key).map(|list| list.len()).unwrap_or(0)
    }

    /// Check if the current node received a decryption key for a listing
    pub async fn get_decryption_key(&self, listing_key: &RecordKey) -> Option<String> {
        let key = listing_key.to_string();
        let keys = self.decryption_keys.lock().await;
        keys.get(&key).cloned()
    }

    /// Fetch a listing and decrypt its content if we have the decryption key
    pub async fn fetch_and_decrypt_listing(&self, listing_key: &RecordKey) -> Result<String> {
        let listing_data = self
            .dht
            .get_value(listing_key)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch listing: {}", e))?
            .ok_or_else(|| anyhow::anyhow!("Listing not found"))?;

        let listing = Listing::from_cbor(&listing_data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize listing: {}", e))?;

        let decryption_key = self.get_decryption_key(listing_key).await.ok_or_else(|| {
            anyhow::anyhow!(
                "No decryption key available for this listing. You must win the auction first."
            )
        })?;

        listing
            .decrypt_content_with_key(&decryption_key)
            .map_err(|e| anyhow::anyhow!("Failed to decrypt listing content: {}", e))
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

        let routing_context = self
            .api
            .routing_context()
            .map_err(|e| anyhow::anyhow!("Failed to create routing context: {}", e))?
            .with_safety(SafetySelection::Unsafe(Sequencing::PreferOrdered))
            .map_err(|e| anyhow::anyhow!("Failed to set safety: {}", e))?;

        let state = self
            .api
            .get_state()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get state: {}", e))?;

        let mut sent_count = 0;

        info!("Found {} peers to broadcast to", state.network.peers.len());

        for peer in &state.network.peers {
            info!("Sending bid announcement to peer {:?}", peer.node_ids);

            for node_id in &peer.node_ids {
                match routing_context
                    .app_message(Target::NodeId(node_id.clone()), data.clone())
                    .await
                {
                    Ok(_) => {
                        info!("Successfully sent bid announcement to peer {}", node_id);
                        sent_count += 1;
                        break;
                    }
                    Err(e) => {
                        debug!(
                            "Failed to send bid announcement to peer node {}: {}",
                            node_id, e
                        );
                    }
                }
            }
        }

        if sent_count == 0 {
            warn!(
                "No peers found to send bid announcement. Network has {} peers",
                state.network.peers.len()
            );
        } else {
            info!("Broadcast completed: sent to {} peers", sent_count);
        }

        Ok(())
    }

    /// Start background deadline monitoring
    pub async fn start_monitoring(self: Arc<Self>) {
        info!("Starting auction deadline monitor");

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

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
        listing: &Listing,
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
