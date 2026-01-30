use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::process::Command;
use tokio::sync::Mutex;
use tracing::{info, warn, error, debug};
use veilid_core::{VeilidAPI, RecordKey, PublicKey, RouteId, Target, SafetySelection, Sequencing};

use crate::marketplace::{Listing, BidIndex};
use super::bid_ops::BidOperations;
use super::bid_storage::BidStorage;
use super::mpc_routes::MpcRouteManager;
use super::mpc::MpcSidecar;
use super::dht::{DHTOperations, OwnedDHTRecord};
use super::bid_announcement::{AuctionMessage, BidAnnouncementRegistry};
use super::listing_ops::ListingOperations;

/// Coordinates auction execution: monitors deadlines, triggers MPC
pub struct AuctionCoordinator {
    api: VeilidAPI,
    dht: DHTOperations,
    my_node_id: PublicKey,
    bid_storage: BidStorage,
    /// Node offset for port allocation (from MARKET_NODE_OFFSET env var)
    node_offset: u16,
    /// Listings we're monitoring
    watched_listings: Arc<Mutex<HashMap<RecordKey, Listing>>>,
    /// Listings we own (as seller): Map<listing_key, OwnedDHTRecord>
    owned_listings: Arc<Mutex<HashMap<RecordKey, OwnedDHTRecord>>>,
    /// Currently active MPC sidecar (if any)
    active_sidecar: Arc<Mutex<Option<MpcSidecar>>>,
    /// Collected bid announcements: Map<listing_key, Vec<(bidder, bid_record_key)>>
    bid_announcements: Arc<Mutex<HashMap<String, Vec<(PublicKey, RecordKey)>>>>,
    /// Received decryption keys for won auctions: Map<listing_key, decryption_key_hex>
    decryption_keys: Arc<Mutex<HashMap<String, String>>>,
    /// MPC route managers per auction: Map<listing_key, MpcRouteManager>
    route_managers: Arc<Mutex<HashMap<String, Arc<Mutex<MpcRouteManager>>>>>,
}

impl AuctionCoordinator {
    pub fn new(api: VeilidAPI, dht: DHTOperations, my_node_id: PublicKey, bid_storage: BidStorage, node_offset: u16) -> Self {
        Self {
            api,
            dht,
            my_node_id,
            bid_storage,
            node_offset,
            watched_listings: Arc::new(Mutex::new(HashMap::new())),
            owned_listings: Arc::new(Mutex::new(HashMap::new())),
            active_sidecar: Arc::new(Mutex::new(None)),
            bid_announcements: Arc::new(Mutex::new(HashMap::new())),
            decryption_keys: Arc::new(Mutex::new(HashMap::new())),
            route_managers: Arc::new(Mutex::new(HashMap::new())),
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
        if let Some(sidecar) = self.active_sidecar.lock().await.as_ref() {
            sidecar.process_message(message).await?;
        }
        Ok(())
    }

    /// Handle auction coordination messages
    async fn handle_auction_message(&self, message: AuctionMessage) -> Result<()> {
        match message {
            AuctionMessage::BidAnnouncement { listing_key, bidder, bid_record_key, timestamp } => {
                info!("Received bid announcement for listing {} from bidder {}", listing_key, bidder);

                // Store the announcement locally (for backward compatibility)
                let key = listing_key.to_string();
                let mut announcements = self.bid_announcements.lock().await;
                let list = announcements.entry(key.clone()).or_insert_with(Vec::new);

                // Avoid duplicates
                if !list.iter().any(|(b, _)| b == &bidder) {
                    list.push((bidder.clone(), bid_record_key.clone()));
                    info!("Stored bid announcement, total announcements for this listing: {}", list.len());
                }
                drop(announcements); // Release lock

                // If we own this listing, update DHT bid registry
                let owned = self.owned_listings.lock().await;
                if let Some(record) = owned.get(&listing_key) {
                    info!("We own this listing, updating DHT bid registry");

                    // Read current registry
                    let current_data = self.dht.get_value_at_subkey(&listing_key, 2).await?;
                    let mut registry = match current_data {
                        Some(data) => BidAnnouncementRegistry::from_bytes(&data)?,
                        None => {
                            warn!("No existing registry found, creating new one");
                            BidAnnouncementRegistry::new()
                        }
                    };

                    // Add announcement to registry
                    registry.add(bidder.clone(), bid_record_key.clone(), timestamp);

                    // Write back to DHT
                    let data = registry.to_bytes()?;
                    self.dht.set_value_at_subkey(record, 2, data).await?;

                    info!("Updated DHT bid registry for listing {}, now has {} announcements",
                          listing_key, registry.announcements.len());
                }
            }
            AuctionMessage::WinnerDecryptionRequest { listing_key, winner, timestamp: _ } => {
                info!("Received decryption key request from winner {} for listing {}", winner, listing_key);

                // Check if we own this listing (are the seller)
                let owned = self.owned_listings.lock().await;
                if let Some(_record) = owned.get(&listing_key) {
                    drop(owned); // Release lock before async call

                    info!("I am the seller, fetching listing to send decryption key");

                    // Get the listing to retrieve decryption key
                    let listing_ops = ListingOperations::new(self.dht.clone());
                    match listing_ops.get_listing(&listing_key).await {
                        Ok(Some(listing)) => {
                            info!("Sending decryption key to winner {}", winner);
                            if let Err(e) = self.send_decryption_hash(&listing_key, &winner, &listing.decryption_key).await {
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
                    debug!("Not the seller for listing {}, ignoring request", listing_key);
                }
            }
            AuctionMessage::DecryptionHashTransfer { listing_key, winner, decryption_hash, timestamp: _ } => {
                info!("Received decryption hash transfer for listing {} (winner: {})", listing_key, winner);

                // Check if we are the winner
                if winner == self.my_node_id {
                    info!("I am the auction winner! Received decryption key: {}", decryption_hash);

                    // Store the decryption key for this listing
                    let key = listing_key.to_string();
                    let mut keys = self.decryption_keys.lock().await;
                    keys.insert(key, decryption_hash);
                    info!("Stored decryption key for listing {}", listing_key);
                } else {
                    debug!("Decryption hash transfer not for me, ignoring");
                }
            }
            AuctionMessage::MpcRouteAnnouncement { listing_key, party_pubkey, route_blob, timestamp: _ } => {
                info!("Received MPC route announcement for listing {} from party {}", listing_key, party_pubkey);

                let key = listing_key.to_string();
                let managers = self.route_managers.lock().await;

                if let Some(manager) = managers.get(&key) {
                    let mgr = manager.lock().await;
                    mgr.register_route_announcement(party_pubkey, route_blob).await?;
                } else {
                    debug!("Received route for unwatched auction {}", listing_key);
                }
            }
        }
        Ok(())
    }

    /// Watch a listing for deadline (if we're a bidder)
    pub async fn watch_listing(&self, listing: Listing) {
        let mut watched = self.watched_listings.lock().await;
        watched.insert(listing.key.clone(), listing.clone());
        info!("Now watching listing '{}' for auction deadline", listing.title);
        drop(watched);

        // Pre-create route manager for this auction so we can receive route announcements
        // even before the auction ends. Party ID will be determined later.
        let key = listing.key.to_string();
        let mut managers = self.route_managers.lock().await;

        if !managers.contains_key(&key) {
            let route_manager = Arc::new(Mutex::new(MpcRouteManager::new(
                self.api.clone(),
                self.dht.clone(),
                0, // Placeholder party ID, will be determined when auction ends
            )));
            managers.insert(key.clone(), route_manager);
            info!("Created route manager for listing {} to receive route announcements", listing.key);
        }
    }

    /// Register an owned listing (for sellers who created it)
    /// This allows the seller to update the DHT bid registry
    pub async fn register_owned_listing(&self, record: OwnedDHTRecord) -> Result<()> {
        let key = record.key.clone();

        // Initialize empty bid registry at subkey 2
        let registry = BidAnnouncementRegistry::new();
        let data = registry.to_bytes()?;

        self.dht.set_value_at_subkey(&record, 2, data).await?;
        info!("Initialized bid registry for owned listing: {}", key);

        // Store the record for future updates
        let mut owned = self.owned_listings.lock().await;
        owned.insert(key.clone(), record.clone());
        drop(owned);

        // Sellers must also watch their own listing to:
        // 1. Monitor auction deadline
        // 2. Create route manager to receive MPC route announcements
        let listing_ops = ListingOperations::new(self.dht.clone());
        if let Ok(Some(listing)) = listing_ops.get_listing(&key).await {
            self.watch_listing(listing).await;
            info!("Seller now watching their own listing: {}", key);
        }

        Ok(())
    }

    /// Add our own bid to the DHT registry (for when seller bids on own listing)
    /// This is needed because we don't receive our own broadcast announcements
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

            // Read current registry
            let current_data = self.dht.get_value_at_subkey(listing_key, 2).await?;
            let mut registry = match current_data {
                Some(data) => BidAnnouncementRegistry::from_bytes(&data)?,
                None => {
                    warn!("No existing registry found for owned listing, creating new one");
                    BidAnnouncementRegistry::new()
                }
            };

            // Add our bid to registry
            registry.add(bidder.clone(), bid_record_key.clone(), timestamp);

            // Write back to DHT
            let data = registry.to_bytes()?;
            self.dht.set_value_at_subkey(record, 2, data).await?;

            info!("Added our own bid to DHT registry for listing {}, now has {} announcements",
                  listing_key, registry.announcements.len());
        }

        Ok(())
    }

    /// Register a bid announcement locally (called when submitting a bid)
    pub async fn register_local_bid(&self, listing_key: &RecordKey, bidder: PublicKey, bid_record_key: RecordKey) {
        let key = listing_key.to_string();
        let mut announcements = self.bid_announcements.lock().await;
        let list = announcements.entry(key).or_insert_with(Vec::new);

        // Avoid duplicates
        if !list.iter().any(|(b, _)| b == &bidder) {
            list.push((bidder, bid_record_key));
            info!("Registered local bid announcement, total announcements for this listing: {}", list.len());
        }
    }

    /// Get the number of bids for a listing from local announcements
    pub async fn get_bid_count(&self, listing_key: &RecordKey) -> usize {
        let key = listing_key.to_string();
        let announcements = self.bid_announcements.lock().await;
        announcements.get(&key).map(|list| list.len()).unwrap_or(0)
    }

    /// Check if the current node received a decryption key for a listing
    /// Returns the decryption key (hex-encoded) if available
    pub async fn get_decryption_key(&self, listing_key: &RecordKey) -> Option<String> {
        let key = listing_key.to_string();
        let keys = self.decryption_keys.lock().await;
        keys.get(&key).cloned()
    }

    /// Fetch a listing and decrypt its content if we have the decryption key
    /// Returns the decrypted content as a UTF-8 string
    /// Returns error if listing not found, decryption key not available, or decryption fails
    pub async fn fetch_and_decrypt_listing(&self, listing_key: &RecordKey) -> Result<String> {
        // Fetch the listing from DHT
        let listing_data = self.dht.get_value(listing_key).await
            .map_err(|e| anyhow::anyhow!("Failed to fetch listing: {}", e))?
            .ok_or_else(|| anyhow::anyhow!("Listing not found"))?;

        let listing = Listing::from_cbor(&listing_data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize listing: {}", e))?;

        // Get the decryption key
        let decryption_key = self.get_decryption_key(listing_key).await
            .ok_or_else(|| anyhow::anyhow!("No decryption key available for this listing. You must win the auction first."))?;

        // Decrypt the content
        listing.decrypt_content_with_key(&decryption_key)
            .map_err(|e| anyhow::anyhow!("Failed to decrypt listing content: {}", e))
    }

    /// Start background deadline monitoring
    pub async fn start_monitoring(self: Arc<Self>) {
        info!("Starting auction deadline monitor");

        tokio::spawn(async move {
            loop {
                // Check every 10 seconds
                tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

                let watched = self.watched_listings.lock().await.clone();

                for (key, listing) in watched {
                    // Check if auction has ended
                    if listing.time_remaining() == 0 {
                        info!("Auction deadline reached for listing '{}'", listing.title);

                        // Check if we're a bidder
                        if let Err(e) = self.handle_auction_end(&key, &listing).await {
                            error!("Failed to handle auction end for '{}': {}", listing.title, e);
                        }

                        // Remove from watched list
                        self.watched_listings.lock().await.remove(&key);
                    }
                }
            }
        });
    }

    /// Handle auction end: coordinate MPC execution
    async fn handle_auction_end(&self, listing_key: &RecordKey, listing: &Listing) -> Result<()> {
        info!("Handling auction end for listing '{}'", listing.title);

        // Check if we have a bid for this listing
        if !self.bid_storage.has_bid(listing_key).await {
            info!("We didn't bid on this listing, skipping MPC");
            return Ok(());
        }

        // Get our bid record key
        let our_bid_key = self.bid_storage.get_bid_key(listing_key).await
            .ok_or_else(|| anyhow::anyhow!("Bid key not found in storage"))?;

        info!("We bid on this listing, our bid record: {}", our_bid_key);

        // For devnet: Discover bids by checking all known participants
        // In production, this would use AppMessage announcements
        let bid_index = self.discover_bids_from_storage(listing_key).await?;

        if bid_index.bids.is_empty() {
            warn!("No bids discovered for listing '{}'", listing.title);
            return Ok(());
        }

        info!("Discovered {} bids for auction", bid_index.bids.len());

        // Check if we're a bidder
        let sorted_bidders = bid_index.sorted_bidders();
        let my_party_id = sorted_bidders.iter().position(|b| b == &self.my_node_id);

        match my_party_id {
            Some(party_id) => {
                info!("I am Party {} in this {}-party auction", party_id, sorted_bidders.len());

                // Exchange routes with other parties
                let routes = self.exchange_mpc_routes(listing_key, party_id, &sorted_bidders).await?;

                // Wait for all parties to be ready
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

                // Trigger MPC execution
                self.execute_mpc_auction(party_id, sorted_bidders.len(), &bid_index, routes).await?;
            }
            None => {
                debug!("Not a bidder in this auction, skipping MPC participation");
            }
        }

        Ok(())
    }

    /// Broadcast our bid announcement to all known peers via AppMessage
    pub async fn broadcast_bid_announcement(&self, listing_key: &RecordKey, bid_record_key: &RecordKey) -> Result<()> {
        info!("Broadcasting bid announcement for listing {} to all peers", listing_key);

        let announcement = AuctionMessage::bid_announcement(
            listing_key.clone(),
            self.my_node_id.clone(),
            bid_record_key.clone(),
        );

        let data = announcement.to_bytes()?;

        // Create routing context for sending messages
        let routing_context = self.api.routing_context()
            .map_err(|e| anyhow::anyhow!("Failed to create routing context: {}", e))?
            .with_safety(SafetySelection::Unsafe(Sequencing::PreferOrdered))
            .map_err(|e| anyhow::anyhow!("Failed to set safety: {}", e))?;

        // Get all peer node IDs from network state
        let state = self.api.get_state().await
            .map_err(|e| anyhow::anyhow!("Failed to get state: {}", e))?;

        let mut sent_count = 0;

        // Send to all peers from network state
        info!("Found {} peers to broadcast to", state.network.peers.len());

        for peer in &state.network.peers {
            info!("Sending bid announcement to peer {:?}", peer.node_ids);

            // Try each node ID for this peer
            for node_id in &peer.node_ids {
                match routing_context.app_message(Target::NodeId(node_id.clone()), data.clone()).await {
                    Ok(_) => {
                        info!("Successfully sent bid announcement to peer {}", node_id);
                        sent_count += 1;
                        break; // Successfully sent to this peer, move to next
                    }
                    Err(e) => {
                        debug!("Failed to send bid announcement to peer node {}: {}", node_id, e);
                        // Try next node ID for this peer
                    }
                }
            }
        }

        if sent_count == 0 {
            warn!("No peers found to send bid announcement. Network has {} peers", state.network.peers.len());
        } else {
            info!("Broadcast completed: sent to {} peers", sent_count);
        }

        Ok(())
    }

    /// Send decryption key request to seller
    async fn send_decryption_request(&self, listing_key: &RecordKey, seller: &PublicKey) -> Result<()> {
        info!("Sending decryption key request to seller {} for listing {}", seller, listing_key);

        let message = AuctionMessage::winner_decryption_request(
            listing_key.clone(),
            self.my_node_id.clone(),
        );

        let data = message.to_bytes()?;

        // Broadcast to all peers (seller will receive and respond)
        let routing_context = self.api.routing_context()
            .map_err(|e| anyhow::anyhow!("Failed to create routing context: {}", e))?
            .with_safety(SafetySelection::Unsafe(Sequencing::PreferOrdered))
            .map_err(|e| anyhow::anyhow!("Failed to set safety: {}", e))?;

        let state = self.api.get_state().await
            .map_err(|e| anyhow::anyhow!("Failed to get state: {}", e))?;

        let mut sent_count = 0;
        for peer in &state.network.peers {
            for node_id in &peer.node_ids {
                match routing_context.app_message(Target::NodeId(node_id.clone()), data.clone()).await {
                    Ok(_) => {
                        sent_count += 1;
                        break;
                    }
                    Err(_) => continue,
                }
            }
        }

        info!("Sent decryption request to {} peers", sent_count);
        Ok(())
    }

    /// Send decryption hash to auction winner via their MPC route
    async fn send_decryption_hash(&self, listing_key: &RecordKey, winner: &PublicKey, decryption_hash: &str) -> Result<()> {
        info!("Sending decryption hash to winner {} for listing {}", winner, listing_key);

        let message = AuctionMessage::decryption_hash_transfer(
            listing_key.clone(),
            winner.clone(),
            decryption_hash.to_string(),
        );

        let data = message.to_bytes()?;

        // Get the route manager for this listing to look up winner's MPC route
        let key = listing_key.to_string();
        let managers = self.route_managers.lock().await;

        if let Some(route_manager) = managers.get(&key) {
            let mgr = route_manager.lock().await;
            let routes = mgr.received_routes.lock().await;

            // Look up winner's MPC route that was announced during route exchange
            if let Some(winner_route) = routes.get(winner) {
                let winner_route = winner_route.clone();
                info!("Found winner's MPC route: {}", winner_route);
                drop(routes);
                drop(mgr);
                drop(managers);

                // Send directly to winner's private route
                let routing_context = self.api.routing_context()
                    .map_err(|e| anyhow::anyhow!("Failed to create routing context: {}", e))?
                    .with_safety(SafetySelection::Unsafe(Sequencing::PreferOrdered))
                    .map_err(|e| anyhow::anyhow!("Failed to set safety: {}", e))?;

                let route_display = winner_route.clone();
                match routing_context.app_message(Target::RouteId(winner_route), data).await {
                    Ok(_) => {
                        info!("Successfully sent decryption hash to winner via MPC route {}", route_display);
                        return Ok(());
                    }
                    Err(e) => {
                        error!("Failed to send to winner's MPC route: {}", e);
                        return Err(anyhow::anyhow!("Failed to send decryption hash: {}", e));
                    }
                }
            } else {
                drop(routes);
                drop(mgr);
                drop(managers);
                warn!("Winner's MPC route not found in route manager");
            }
        } else {
            drop(managers);
            warn!("Route manager not found for listing {}", listing_key);
        }

        // Fallback: broadcast to all peers if route not found
        warn!("Using fallback broadcast to all peers");
        let routing_context = self.api.routing_context()
            .map_err(|e| anyhow::anyhow!("Failed to create routing context: {}", e))?
            .with_safety(SafetySelection::Unsafe(Sequencing::PreferOrdered))
            .map_err(|e| anyhow::anyhow!("Failed to set safety: {}", e))?;

        let state = self.api.get_state().await
            .map_err(|e| anyhow::anyhow!("Failed to get state: {}", e))?;

        for peer in &state.network.peers {
            for node_id in &peer.node_ids {
                let _ = routing_context.app_message(Target::NodeId(node_id.clone()), data.clone()).await;
            }
        }

        Ok(())
    }

    /// Discover bids from DHT bid registry (n-party approach)
    /// Reads from listing's DHT subkey 2 for authoritative bid list
    async fn discover_bids_from_storage(&self, listing_key: &RecordKey) -> Result<BidIndex> {
        info!("Discovering bids from DHT bid registry");

        let mut bid_index = BidIndex::new(listing_key.clone());
        let bid_ops = BidOperations::new(self.dht.clone());

        // Read bid registry from DHT subkey 2
        let registry_data = self.dht.get_value_at_subkey(listing_key, 2).await?;

        let bidder_list = match registry_data {
            Some(data) => {
                let registry = BidAnnouncementRegistry::from_bytes(&data)?;
                info!("Found {} bidders in DHT bid registry", registry.announcements.len());
                registry.announcements
            }
            None => {
                info!("No DHT bid registry found, trying local announcements as fallback");

                // Fallback to local announcements for backward compatibility
                let key = listing_key.to_string();
                let announcements = self.bid_announcements.lock().await;

                let local_list = match announcements.get(&key) {
                    Some(list) => {
                        info!("Found {} bidders in local announcements", list.len());
                        list.iter()
                            .map(|(bidder, bid_record_key)| (bidder.clone(), bid_record_key.clone(), 0))
                            .collect()
                    }
                    None => {
                        info!("No local announcements found either");
                        Vec::new()
                    }
                };
                drop(announcements);
                local_list
            }
        };

        // Fetch each bidder's BidRecord from the DHT
        for (bidder, bid_record_key, _timestamp) in &bidder_list {
            info!("Fetching bid record for bidder {} at {}", bidder, bid_record_key);

            match bid_ops.fetch_bid(bid_record_key).await {
                Ok(Some(bid_record)) => {
                    info!("Successfully fetched bid record for bidder {}", bidder);
                    bid_index.add_bid(bid_record);
                }
                Ok(None) => {
                    warn!("No bid record found for bidder {} at {}", bidder, bid_record_key);
                }
                Err(e) => {
                    warn!("Failed to fetch bid record for bidder {}: {}", bidder, e);
                }
            }
        }

        info!("Built BidIndex with {} bids from DHT registry", bid_index.bids.len());
        Ok(bid_index)
    }

    /// Exchange MPC routes with all other bidders
    async fn exchange_mpc_routes(
        &self,
        listing_key: &RecordKey,
        my_party_id: usize,
        bidders: &[PublicKey],
    ) -> Result<HashMap<usize, RouteId>> {
        info!("Exchanging MPC routes with {} parties for listing {}", bidders.len(), listing_key);

        // Get existing route manager (created when we started watching)
        // or create a new one if somehow it doesn't exist
        let key = listing_key.to_string();
        let route_manager = {
            let mut managers = self.route_managers.lock().await;
            managers.entry(key.clone()).or_insert_with(|| {
                info!("Creating new route manager for listing {} (should have been created earlier)", listing_key);
                Arc::new(Mutex::new(MpcRouteManager::new(
                    self.api.clone(),
                    self.dht.clone(),
                    my_party_id,
                )))
            }).clone()
        };

        // Create our route
        let my_route_id = {
            let mut mgr = route_manager.lock().await;
            mgr.create_route().await?
        };

        info!("Created Veilid route for Party {}: {}", my_party_id, my_route_id);

        // Broadcast our route
        {
            let mgr = route_manager.lock().await;
            let my_pubkey = &bidders[my_party_id];
            mgr.broadcast_route_announcement(listing_key, my_pubkey).await?;
        }

        // Register our own route (so we include ourselves in the assembly)
        {
            let mgr = route_manager.lock().await;
            let my_pubkey = bidders[my_party_id].clone();
            let my_route_blob = mgr.get_my_route_blob()
                .ok_or_else(|| anyhow::anyhow!("Route blob not found"))?
                .clone();
            mgr.register_route_announcement(my_pubkey, my_route_blob).await?;
        }

        // Wait for routes (2s initial + check every 1s up to 5s total)
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let start = std::time::Instant::now();
        let max_wait = std::time::Duration::from_secs(5);

        loop {
            let mgr = route_manager.lock().await;
            let routes = mgr.assemble_party_routes(bidders).await?;
            drop(mgr);

            let expected = bidders.len();
            let received = routes.len();

            info!("Route collection: have {}/{} routes", received, expected);

            if received >= expected || start.elapsed() >= max_wait {
                return Ok(routes);
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    /// Execute the MPC auction computation
    async fn execute_mpc_auction(
        &self,
        party_id: usize,
        num_parties: usize,
        bid_index: &BidIndex,
        routes: HashMap<usize, RouteId>,
    ) -> Result<()> {
        info!("Executing {}-party MPC auction as Party {}", num_parties, party_id);

        // Get my bid value from storage
        let listing_key = &bid_index.listing_key;
        let (bid_value, _nonce) = self.bid_storage.get_bid(listing_key).await
            .ok_or_else(|| anyhow::anyhow!("Bid value not found in storage"))?;

        info!("My bid value: {}", bid_value);

        // Start MPC sidecar with the routes
        let sidecar = MpcSidecar::new(self.api.clone(), party_id, routes, self.node_offset);
        sidecar.run().await?;

        // Store sidecar so AppMessages can be routed to it
        *self.active_sidecar.lock().await = Some(sidecar.clone());

        // Write input file for MP-SPDZ
        let mp_spdz_dir = std::env::var("MP_SPDZ_DIR")
            .unwrap_or_else(|_| "/home/broadcom/Repos/Dissertation/Repos/MP-SPDZ".to_string());

        let input_path = format!("{}/Player-Data/Input-P{}-0", mp_spdz_dir, party_id);
        std::fs::write(&input_path, format!("{}\n", bid_value))
            .map_err(|e| anyhow::anyhow!("Failed to write input file {}: {}", input_path, e))?;

        info!("Wrote bid value to {}", input_path);

        // Generate hosts file for this node using Veilid node ID prefix
        let node_id_str = self.my_node_id.to_string();
        let node_prefix = &node_id_str[5..10]; // Extract first 5 chars after "VLD0:"
        let hosts_file_name = format!("HOSTS-{}", node_prefix);
        let hosts_file_path = format!("{}/{}", mp_spdz_dir, hosts_file_name);

        // Write hosts file with all parties as localhost (Veilid handles actual routing)
        let mut hosts_content = String::new();
        for _ in 0..num_parties {
            hosts_content.push_str("127.0.0.1\n");
        }
        std::fs::write(&hosts_file_path, hosts_content)
            .map_err(|e| anyhow::anyhow!("Failed to write hosts file {}: {}", hosts_file_path, e))?;

        info!("Wrote hosts file to {} for {} parties", hosts_file_path, num_parties);

        // Wait for all parties to be ready
        info!("Waiting 5 seconds for all parties to be ready...");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        // Compile MPC program for the specific number of parties
        info!("Compiling auction_n program for {} parties...", num_parties);
        let compile_output = Command::new(format!("{}/compile.py", mp_spdz_dir))
            .current_dir(&mp_spdz_dir)
            .arg("-R")
            .arg("64")
            .arg("auction_n")
            .arg("--")
            .arg(num_parties.to_string())
            .output();

        match compile_output {
            Ok(result) => {
                let stdout = String::from_utf8_lossy(&result.stdout);
                let stderr = String::from_utf8_lossy(&result.stderr);

                if result.status.success() {
                    info!("Successfully compiled auction_n for {} parties", num_parties);
                    if !stdout.is_empty() {
                        info!("Compile output: {}", stdout);
                    }
                } else {
                    error!("Compilation failed!");
                    if !stderr.is_empty() {
                        error!("Compile errors: {}", stderr);
                    }
                    return Err(anyhow::anyhow!("Failed to compile MPC program for {} parties", num_parties));
                }
            }
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to run compile.py: {}", e));
            }
        }

        // Execute MP-SPDZ
        info!("Executing MP-SPDZ auction_n-{} program...", num_parties);

        let program_name = format!("auction_n-{}", num_parties);
        let output = Command::new(format!("{}/replicated-ring-party.x", mp_spdz_dir))
            .current_dir(&mp_spdz_dir)
            .arg("-p")
            .arg(party_id.to_string())
            .arg("-OF")
            .arg(".")  // Output to stdout for all parties (not just Party 0)
            .arg("-ip")
            .arg(&hosts_file_name)  // Use node-specific hosts file
            .arg(&program_name)
            .output();

        match output {
            Ok(result) => {
                let stdout = String::from_utf8_lossy(&result.stdout);
                let stderr = String::from_utf8_lossy(&result.stderr);

                info!("MP-SPDZ execution completed");
                info!("Exit code: {:?}", result.status.code());

                if !stdout.is_empty() {
                    info!("STDOUT:\n{}", stdout);
                }
                if !stderr.is_empty() {
                    warn!("STDERR:\n{}", stderr);
                }

                // Parse result from output
                // Each party privately learns: "You won: 0" or "You won: 1"
                // Winner will contact seller to request decryption key
                let mut i_won = false;

                for line in stdout.lines() {
                    if line.contains("You won:") {
                        // Each party privately learns if they won
                        if line.contains("You won: 1") {
                            i_won = true;
                            info!("Result: I won the auction!");
                        } else {
                            info!("Result: I did not win");
                        }
                    }
                }

                // If I won, request decryption key from seller
                if i_won {
                    info!("I won - requesting decryption key from seller");

                    // Get listing to find seller
                    let listing_ops = ListingOperations::new(self.dht.clone());
                    match listing_ops.get_listing(&bid_index.listing_key).await {
                        Ok(Some(listing)) => {
                            // Check if I am also the seller
                            if listing.seller == self.my_node_id {
                                info!("I am both the seller and the winner - I already have the decryption key");
                            } else {
                                // Send request to seller
                                info!("Sending decryption key request to seller {}", listing.seller);
                                if let Err(e) = self.send_decryption_request(&bid_index.listing_key, &listing.seller).await {
                                    error!("Failed to send decryption request to seller: {}", e);
                                }
                            }
                        }
                        Ok(None) => {
                            warn!("Listing not found when trying to request decryption key");
                        }
                        Err(e) => {
                            error!("Failed to retrieve listing: {}", e);
                        }
                    }
                }

                // Cleanup sidecar
                sidecar.cleanup().await;
                *self.active_sidecar.lock().await = None;

                // Cleanup route manager
                {
                    let key = bid_index.listing_key.to_string();
                    let mut managers = self.route_managers.lock().await;
                    managers.remove(&key);
                }

                Ok(())
            }
            Err(e) => {
                error!("Failed to execute MP-SPDZ: {}", e);
                sidecar.cleanup().await;
                *self.active_sidecar.lock().await = None;

                // Cleanup route manager
                {
                    let key = bid_index.listing_key.to_string();
                    let mut managers = self.route_managers.lock().await;
                    managers.remove(&key);
                }

                Err(anyhow::anyhow!("MP-SPDZ execution failed: {}", e))
            }
        }
    }
}
