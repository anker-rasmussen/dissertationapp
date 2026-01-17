use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::process::Command;
use tokio::sync::Mutex;
use tracing::{info, warn, error, debug};
use veilid_core::{VeilidAPI, RecordKey, PublicKey, RouteId, Target, SafetySelection, Sequencing};

use crate::marketplace::{Listing, BidIndex, BidRecord};
use super::bid_ops::BidOperations;
use super::bid_storage::BidStorage;
use super::mpc_routes::MpcRouteManager;
use super::mpc::MpcSidecar;
use super::dht::DHTOperations;
use super::bid_announcement::AuctionMessage;
use super::listing_ops::ListingOperations;

/// Coordinates auction execution: monitors deadlines, triggers MPC
pub struct AuctionCoordinator {
    api: VeilidAPI,
    dht: DHTOperations,
    my_node_id: PublicKey,
    bid_storage: BidStorage,
    /// Listings we're monitoring
    watched_listings: Arc<Mutex<HashMap<RecordKey, Listing>>>,
    /// Currently active MPC sidecar (if any)
    active_sidecar: Arc<Mutex<Option<MpcSidecar>>>,
    /// Collected bid announcements: Map<listing_key, Vec<(bidder, bid_record_key)>>
    bid_announcements: Arc<Mutex<HashMap<String, Vec<(PublicKey, RecordKey)>>>>,
    /// Received decryption keys for won auctions: Map<listing_key, decryption_key_hex>
    decryption_keys: Arc<Mutex<HashMap<String, String>>>,
}

impl AuctionCoordinator {
    pub fn new(api: VeilidAPI, dht: DHTOperations, my_node_id: PublicKey, bid_storage: BidStorage) -> Self {
        Self {
            api,
            dht,
            my_node_id,
            bid_storage,
            watched_listings: Arc::new(Mutex::new(HashMap::new())),
            active_sidecar: Arc::new(Mutex::new(None)),
            bid_announcements: Arc::new(Mutex::new(HashMap::new())),
            decryption_keys: Arc::new(Mutex::new(HashMap::new())),
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

                // Store the announcement
                let key = listing_key.to_string();
                let mut announcements = self.bid_announcements.lock().await;
                let list = announcements.entry(key).or_insert_with(Vec::new);

                // Avoid duplicates
                if !list.iter().any(|(b, _)| b == &bidder) {
                    list.push((bidder, bid_record_key));
                    info!("Stored bid announcement, total announcements for this listing: {}", list.len());
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
        }
        Ok(())
    }

    /// Watch a listing for deadline (if we're a bidder)
    pub async fn watch_listing(&self, listing: Listing) {
        let mut watched = self.watched_listings.lock().await;
        watched.insert(listing.key.clone(), listing.clone());
        info!("Now watching listing '{}' for auction deadline", listing.title);
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
                let routes = self.exchange_mpc_routes(party_id, &sorted_bidders).await?;

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

    /// Send decryption hash to auction winner
    async fn send_decryption_hash(&self, listing_key: &RecordKey, winner: &PublicKey, decryption_hash: &str) -> Result<()> {
        info!("Sending decryption hash to winner {} for listing {}", winner, listing_key);

        let message = AuctionMessage::decryption_hash_transfer(
            listing_key.clone(),
            winner.clone(),
            decryption_hash.to_string(),
        );

        let data = message.to_bytes()?;

        // Create routing context for sending messages
        let routing_context = self.api.routing_context()
            .map_err(|e| anyhow::anyhow!("Failed to create routing context: {}", e))?
            .with_safety(SafetySelection::Unsafe(Sequencing::PreferOrdered))
            .map_err(|e| anyhow::anyhow!("Failed to set safety: {}", e))?;

        // Get network state to find the winner's NodeId
        let state = self.api.get_state().await
            .map_err(|e| anyhow::anyhow!("Failed to get state: {}", e))?;

        // Try to send to all node IDs that match the winner's public key
        // In Veilid, we need to find the NodeId(s) for routing
        let mut sent = false;
        for peer in &state.network.peers {
            // Try each node ID for this peer
            for node_id in &peer.node_ids {
                match routing_context.app_message(Target::NodeId(node_id.clone()), data.clone()).await {
                    Ok(_) => {
                        info!("Successfully sent decryption hash to winner via node {}", node_id);
                        sent = true;
                        break;
                    }
                    Err(e) => {
                        debug!("Failed to send to node {}: {}", node_id, e);
                    }
                }
            }
            if sent {
                break;
            }
        }

        if !sent {
            warn!("Could not find NodeId for winner {}, broadcasting instead", winner);
            // Fallback: broadcast to all peers (winner will filter it)
            for peer in &state.network.peers {
                for node_id in &peer.node_ids {
                    let _ = routing_context.app_message(Target::NodeId(node_id.clone()), data.clone()).await;
                }
            }
        }

        Ok(())
    }

    /// Discover bids from local announcements (n-party approach)
    /// Uses locally stored bid announcements since DHT registry write fails due to permissions
    async fn discover_bids_from_storage(&self, listing_key: &RecordKey) -> Result<BidIndex> {
        info!("Discovering bids from local announcement storage");

        let mut bid_index = BidIndex::new(listing_key.clone());
        let bid_ops = BidOperations::new(self.dht.clone());

        // Get locally stored announcements
        let key = listing_key.to_string();
        let announcements = self.bid_announcements.lock().await;

        let bidder_list = match announcements.get(&key) {
            Some(list) => {
                info!("Found {} bidders in local announcements", list.len());
                list.clone()
            }
            None => {
                info!("No local announcements found for this listing");
                Vec::new()
            }
        };

        drop(announcements); // Release lock

        // Fetch each bidder's BidRecord from the DHT
        for (bidder, bid_record_key) in &bidder_list {
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

        info!("Built BidIndex with {} bids from local announcements", bid_index.bids.len());
        Ok(bid_index)
    }

    /// Build BidIndex from collected announcements
    async fn build_bid_index_from_announcements(&self, listing_key: &RecordKey) -> Result<BidIndex> {
        let key = listing_key.to_string();
        let announcements = self.bid_announcements.lock().await;
        let mut bid_index = BidIndex::new(listing_key.clone());

        if let Some(list) = announcements.get(&key) {
            info!("Building BidIndex from {} announcements", list.len());

            // Fetch each BidRecord from DHT
            let bid_ops = BidOperations::new(self.dht.clone());
            for (bidder, bid_record_key) in list {
                match bid_ops.fetch_bid(bid_record_key).await? {
                    Some(bid_record) => {
                        info!("Fetched bid record for bidder {}", bidder);
                        bid_index.add_bid(bid_record);
                    }
                    None => {
                        warn!("Failed to fetch bid record for bidder {} at {}", bidder, bid_record_key);
                    }
                }
            }
        } else {
            info!("No announcements collected for listing {}", listing_key);
        }

        Ok(bid_index)
    }

    /// Exchange MPC routes with all other bidders
    async fn exchange_mpc_routes(
        &self,
        my_party_id: usize,
        bidders: &[PublicKey],
    ) -> Result<HashMap<usize, RouteId>> {
        info!("Exchanging MPC routes with {} parties", bidders.len());

        // Create dynamic route manager for this auction
        let mut route_manager = MpcRouteManager::new(
            self.api.clone(),
            self.dht.clone(),
            my_party_id,
        );

        // Create and publish our route
        let _ = route_manager.create_route().await?;
        route_manager.publish_route().await?;

        // Fetch routes from other bidders
        let routes = route_manager.fetch_party_routes(bidders.len()).await?;

        info!("Successfully exchanged routes with {} parties", routes.len());
        Ok(routes)
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
        let sidecar = MpcSidecar::new(self.api.clone(), party_id, routes);
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

        // Wait for all parties to be ready
        info!("Waiting 5 seconds for all parties to be ready...");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        // Execute MP-SPDZ
        info!("Executing MP-SPDZ auction_n program...");

        let output = Command::new(format!("{}/replicated-ring-party.x", mp_spdz_dir))
            .current_dir(&mp_spdz_dir)
            .arg("-p")
            .arg(party_id.to_string())
            .arg("-ip")
            .arg("HOSTS-localhost")
            .arg("auction_n")
            .arg(num_parties.to_string())
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
                // Look for lines like "Winner: Party X" and "Winning bid: Y"
                let mut winner_party_id: Option<usize> = None;

                for line in stdout.lines() {
                    if line.contains("Winner:") || line.contains("Winning bid:") {
                        info!("Result: {}", line);

                        // Extract winner party ID
                        if line.contains("Winner: Party") {
                            if let Some(id_str) = line.split("Party").nth(1) {
                                if let Ok(id) = id_str.trim().parse::<usize>() {
                                    winner_party_id = Some(id);
                                }
                            }
                        }
                    }
                }

                // Handle decryption hash transfer if we have a winner
                if let Some(winner_id) = winner_party_id {
                    // Get sorted bidders from bid index
                    let sorted_bidders = bid_index.sorted_bidders();
                    let winner_pubkey = &sorted_bidders[winner_id];
                    info!("Winner is Party {} with pubkey {}", winner_id, winner_pubkey);

                    // Check if we are the seller
                    let listing_ops = ListingOperations::new(self.dht.clone());
                    match listing_ops.get_listing(&bid_index.listing_key).await {
                        Ok(Some(listing)) => {
                            info!("Successfully retrieved listing for seller check (seller: {})", listing.seller);
                            if listing.seller == self.my_node_id {
                                // Check if seller won their own auction
                                if winner_pubkey == &self.my_node_id {
                                    info!("I am both the seller and the winner - I already have the decryption key");
                                } else {
                                    info!("I am the seller, sending decryption key to winner");
                                    if let Err(e) = self.send_decryption_hash(&bid_index.listing_key, winner_pubkey, &listing.decryption_key).await {
                                        error!("Failed to send decryption key to winner: {}", e);
                                    }
                                }
                            } else {
                                info!("Not the seller (seller is {}, I am {}), skipping decryption key transfer", listing.seller, self.my_node_id);
                            }
                        }
                        Ok(None) => {
                            warn!("Listing not found when trying to check seller status");
                        }
                        Err(e) => {
                            error!("Failed to retrieve listing for seller check: {}", e);
                        }
                    }
                }

                // Cleanup sidecar
                sidecar.cleanup().await;
                *self.active_sidecar.lock().await = None;

                Ok(())
            }
            Err(e) => {
                error!("Failed to execute MP-SPDZ: {}", e);
                sidecar.cleanup().await;
                *self.active_sidecar.lock().await = None;
                Err(anyhow::anyhow!("MP-SPDZ execution failed: {}", e))
            }
        }
    }
}
