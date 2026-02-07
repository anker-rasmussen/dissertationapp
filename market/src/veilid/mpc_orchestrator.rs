use anyhow::Result;
use std::collections::HashMap;
use std::process::{Command, Stdio};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use veilid_core::{
    PublicKey, RecordKey, RouteId, SafetySelection, SafetySpec, Sequencing, Stability, Target,
    VeilidAPI,
};

use super::bid_announcement::{AuctionMessage, BidAnnouncementRegistry};
use super::bid_ops::BidOperations;
use super::bid_storage::BidStorage;
use super::dht::DHTOperations;
use super::mpc::MpcTunnelProxy;
use super::mpc_routes::MpcRouteManager;
use crate::config;
use crate::marketplace::BidIndex;

type RouteManagerMap = Arc<Mutex<HashMap<RecordKey, Arc<Mutex<MpcRouteManager>>>>>;
type VerificationMap = Arc<Mutex<HashMap<RecordKey, VerificationState>>>;

/// Named struct replacing the tuple `(PublicKey, u64, Option<bool>)` for pending verifications.
#[derive(Debug, Clone)]
pub struct VerificationState {
    pub winner_pubkey: PublicKey,
    pub mpc_winning_bid: u64,
    pub verified: Option<bool>,
}

/// Orchestrates MPC execution: route exchange, tunnel proxy lifecycle, bid verification.
///
/// Extracted from `AuctionCoordinator` to separate MPC concerns from auction
/// coordination (bid announcements, listing management, DHT updates).
pub struct MpcOrchestrator {
    api: VeilidAPI,
    dht: DHTOperations,
    my_node_id: PublicKey,
    bid_storage: BidStorage,
    node_offset: u16,
    /// Currently active MPC tunnel proxy (if any)
    active_tunnel_proxy: Arc<Mutex<Option<MpcTunnelProxy>>>,
    /// MPC route managers per auction: Map<listing_key, MpcRouteManager>
    route_managers: RouteManagerMap,
    /// Pending verifications: listing_key -> (winner_pubkey, mpc_winning_bid, verified?)
    pending_verifications: VerificationMap,
}

impl MpcOrchestrator {
    pub fn new(
        api: VeilidAPI,
        dht: DHTOperations,
        my_node_id: PublicKey,
        bid_storage: BidStorage,
        node_offset: u16,
    ) -> Self {
        Self {
            api,
            dht,
            my_node_id,
            bid_storage,
            node_offset,
            active_tunnel_proxy: Arc::new(Mutex::new(None)),
            route_managers: Arc::new(Mutex::new(HashMap::new())),
            pending_verifications: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Access to route managers (needed by AuctionCoordinator for message handling)
    pub fn route_managers(&self) -> &RouteManagerMap {
        &self.route_managers
    }

    /// Access to pending verifications (needed by AuctionCoordinator for message handling)
    pub fn pending_verifications(&self) -> &VerificationMap {
        &self.pending_verifications
    }

    /// Access to active tunnel proxy (needed by AuctionCoordinator for message forwarding)
    pub fn active_tunnel_proxy(&self) -> &Arc<Mutex<Option<MpcTunnelProxy>>> {
        &self.active_tunnel_proxy
    }

    /// Pre-create a route manager for an auction so we can receive route announcements
    /// even before the auction ends.
    pub async fn ensure_route_manager(&self, listing_key: &RecordKey) {
        let key = listing_key.clone();
        let mut managers = self.route_managers.lock().await;

        if !managers.contains_key(&key) {
            let route_manager = Arc::new(Mutex::new(MpcRouteManager::new(
                self.api.clone(),
                self.dht.clone(),
                0, // Placeholder party ID, will be determined when auction ends
            )));
            managers.insert(key.clone(), route_manager);
            info!(
                "Created route manager for listing {} to receive route announcements",
                listing_key
            );
        }
    }

    /// Handle auction end: coordinate MPC execution
    pub async fn handle_auction_end(
        &self,
        listing_key: &RecordKey,
        bid_index: BidIndex,
        listing_title: &str,
    ) -> Result<()> {
        info!("Handling auction end for listing '{}'", listing_title);

        if bid_index.bids.is_empty() {
            warn!("No bids discovered for listing '{}'", listing_title);
            return Ok(());
        }

        info!("Discovered {} bids for auction", bid_index.bids.len());

        // Sort bidders by timestamp (seller's auto-bid has earliest timestamp = party 0)
        let sorted_bidders = bid_index.sorted_bidders();

        // Shamir secret sharing requires at least 3 parties
        if sorted_bidders.len() < 3 {
            warn!(
                "Auction for '{}' has only {} parties, but Shamir requires at least 3. Aborting MPC.",
                listing_title, sorted_bidders.len()
            );
            self.cleanup_route_manager(listing_key).await;
            return Ok(());
        }

        // Check if we're a bidder
        let my_party_id = sorted_bidders.iter().position(|b| b == &self.my_node_id);

        match my_party_id {
            Some(party_id) => {
                info!(
                    "I am Party {} in this {}-party auction",
                    party_id,
                    sorted_bidders.len()
                );

                // Exchange routes with other parties
                let routes = self
                    .exchange_mpc_routes(listing_key, party_id, &sorted_bidders)
                    .await?;

                // Wait for all parties to be ready
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

                // Trigger MPC execution
                self.execute_mpc_auction(
                    party_id,
                    sorted_bidders.len(),
                    &bid_index,
                    routes,
                    &sorted_bidders,
                )
                .await?;
            }
            None => {
                debug!("Not a bidder in this auction, skipping MPC participation");
            }
        }

        Ok(())
    }

    /// Exchange MPC routes with all other bidders
    pub async fn exchange_mpc_routes(
        &self,
        listing_key: &RecordKey,
        my_party_id: usize,
        bidders: &[PublicKey],
    ) -> Result<HashMap<usize, RouteId>> {
        info!(
            "Exchanging MPC routes with {} parties for listing {}",
            bidders.len(),
            listing_key
        );

        // Get existing route manager or create a new one
        let key = listing_key.clone();
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

        info!(
            "Created Veilid route for Party {}: {}",
            my_party_id, my_route_id
        );

        // Broadcast our route
        {
            let mgr = route_manager.lock().await;
            let my_pubkey = &bidders[my_party_id];
            mgr.broadcast_route_announcement(listing_key, my_pubkey)
                .await?;
        }

        // Register our own route (so we include ourselves in the assembly)
        {
            let mgr = route_manager.lock().await;
            let my_pubkey = bidders[my_party_id].clone();
            let my_route_blob = mgr
                .get_my_route_blob()
                .ok_or_else(|| anyhow::anyhow!("Route blob not found"))?
                .clone();
            mgr.register_route_announcement(my_pubkey, my_route_blob)
                .await?;
        }

        // Wait for routes (2s initial + check every 1s up to 5s total)
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let start = std::time::Instant::now();
        let max_wait = std::time::Duration::from_secs(5);

        loop {
            let routes = {
                let mgr = route_manager.lock().await;
                mgr.assemble_party_routes(bidders).await?
            };

            let expected = bidders.len();
            let received = routes.len();

            info!("Route collection: have {}/{} routes", received, expected);

            if received >= expected || start.elapsed() >= max_wait {
                return Ok(routes);
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    /// Execute the MPC auction computation using Shamir secret sharing
    async fn execute_mpc_auction(
        &self,
        party_id: usize,
        num_parties: usize,
        bid_index: &BidIndex,
        routes: HashMap<usize, RouteId>,
        all_parties: &[PublicKey],
    ) -> Result<()> {
        info!(
            "Executing {}-party MPC auction as Party {} (Shamir)",
            num_parties, party_id
        );

        // Get my bid value from storage
        let listing_key = &bid_index.listing_key;
        let (bid_value, _nonce) = self
            .bid_storage
            .get_bid(listing_key)
            .await
            .ok_or_else(|| anyhow::anyhow!("Bid value not found in storage"))?;

        info!("My bid value: {}", bid_value);

        // Start MPC tunnel proxy with the routes
        let tunnel_proxy = MpcTunnelProxy::new(self.api.clone(), party_id, routes, self.node_offset);
        tunnel_proxy.run().await?;

        // Store tunnel proxy so AppMessages can be routed to it
        *self.active_tunnel_proxy.lock().await = Some(tunnel_proxy.clone());

        let mp_spdz_dir = std::env::var(config::MP_SPDZ_DIR_ENV)
            .unwrap_or_else(|_| config::DEFAULT_MP_SPDZ_DIR.to_string());

        // Generate hosts file in temp dir (MP-SPDZ needs a real file for -ip)
        let node_id_str = self.my_node_id.to_string();
        let node_prefix = node_id_str.get(5..10).unwrap_or("unknown"); // Extract first 5 chars after "VLD0:"
        let hosts_file_name = format!("HOSTS-{}", node_prefix);
        let hosts_file_path = std::env::temp_dir().join(&hosts_file_name);

        // Write hosts file with all parties as localhost (Veilid handles actual routing)
        let mut hosts_content = String::new();
        for _ in 0..num_parties {
            hosts_content.push_str("127.0.0.1\n");
        }
        std::fs::write(&hosts_file_path, &hosts_content).map_err(|e| {
            anyhow::anyhow!("Failed to write hosts file {:?}: {}", hosts_file_path, e)
        })?;

        info!(
            "Wrote hosts file to {:?} for {} parties",
            hosts_file_path, num_parties
        );

        // Wait for all parties to be ready
        info!("Waiting 5 seconds for all parties to be ready...");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        // Compile MPC program for the specific number of parties (Shamir uses prime field, no -R flag)
        info!("Compiling auction_n program for {} parties...", num_parties);
        let compile_output = Command::new(format!("{}/compile.py", mp_spdz_dir))
            .current_dir(&mp_spdz_dir)
            .arg("auction_n")
            .arg("--")
            .arg(num_parties.to_string())
            .output();

        match compile_output {
            Ok(result) => {
                let stdout = String::from_utf8_lossy(&result.stdout);
                let stderr = String::from_utf8_lossy(&result.stderr);

                if result.status.success() {
                    info!(
                        "Successfully compiled auction_n for {} parties",
                        num_parties
                    );
                    if !stdout.is_empty() {
                        info!("Compile output: {}", stdout);
                    }
                } else {
                    error!("Compilation failed!");
                    if !stderr.is_empty() {
                        error!("Compile errors: {}", stderr);
                    }
                    return Err(anyhow::anyhow!(
                        "Failed to compile MPC program for {} parties",
                        num_parties
                    ));
                }
            }
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to run compile.py: {}", e));
            }
        }

        // Execute MP-SPDZ with Shamir party executable (interactive mode: bid via stdin)
        info!(
            "Executing MP-SPDZ auction_n-{} program (Shamir, interactive)...",
            num_parties
        );

        let program_name = format!("auction_n-{}", num_parties);
        let spawn_result = Command::new(format!("{}/shamir-party.x", mp_spdz_dir))
            .current_dir(&mp_spdz_dir)
            .arg("-p")
            .arg(party_id.to_string())
            .arg("-N")
            .arg(num_parties.to_string())
            .arg("-OF")
            .arg(".") // Output to stdout
            .arg("-ip")
            .arg(hosts_file_path.to_str().unwrap_or("HOSTS"))
            .arg("-I") // Interactive mode: read inputs from stdin
            .arg(&program_name)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn();

        let mut child = match spawn_result {
            Ok(child) => child,
            Err(e) => {
                let _ = std::fs::remove_file(&hosts_file_path);
                return Err(anyhow::anyhow!("Failed to spawn shamir-party.x: {}", e));
            }
        };

        // Write bid value to stdin, then close the pipe (EOF)
        {
            use std::io::Write;
            let stdin = child
                .stdin
                .take()
                .ok_or_else(|| anyhow::anyhow!("Failed to open stdin pipe to shamir-party.x"))?;
            let mut stdin = stdin;
            stdin
                .write_all(format!("{}\n", bid_value).as_bytes())
                .map_err(|e| anyhow::anyhow!("Failed to write bid value to stdin: {}", e))?;
            // stdin is dropped here, sending EOF
        }

        let output = child.wait_with_output();

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

                let is_seller = party_id == 0;

                if is_seller {
                    // === SELLER (party 0): Parse winner ID and winning bid ===
                    let mut winning_bid: Option<u64> = None;
                    let mut winner_party_id: Option<usize> = None;

                    for line in stdout.lines() {
                        if line.contains("Winning bid:") {
                            if let Some(bid_str) = line.split(':').next_back() {
                                winning_bid = bid_str.trim().parse().ok();
                                if let Some(bid) = winning_bid {
                                    info!("Parsed winning bid from MPC output: {}", bid);
                                }
                            }
                        }
                        if line.contains("Winner: Party") {
                            #[allow(clippy::double_ended_iterator_last)]
                            if let Some(party_str) = line.split("Party").last() {
                                winner_party_id = party_str.trim().parse().ok();
                                if let Some(pid) = winner_party_id {
                                    info!("Parsed winner party ID from MPC output: {}", pid);
                                }
                            }
                        }
                    }

                    if let (Some(bid), Some(winner_pid)) = (winning_bid, winner_party_id) {
                        if winner_pid == 0 {
                            // Winner is the seller (reserve price was highest) = no sale
                            info!("No bidder exceeded the reserve price — no sale");
                            self.cleanup_route_manager(listing_key).await;
                        } else if winner_pid < all_parties.len() {
                            let winner_pubkey = all_parties[winner_pid].clone();
                            let key = listing_key.clone();

                            // Store pending verification entry
                            self.pending_verifications
                                .lock()
                                .await
                                .insert(key, VerificationState {
                                    winner_pubkey: winner_pubkey.clone(),
                                    mpc_winning_bid: bid,
                                    verified: None,
                                });
                            info!(
                                "Stored pending verification: winner={}, bid={}",
                                winner_pubkey, bid
                            );

                            // Send WinnerDecryptionRequest to winner via their MPC route (challenge)
                            info!(
                                "Sending challenge (WinnerDecryptionRequest) to winner {}",
                                winner_pubkey
                            );
                            self.send_winner_challenge(listing_key, &winner_pubkey)
                                .await;
                        } else {
                            error!(
                                "Winner party ID {} out of range (only {} parties)",
                                winner_pid,
                                all_parties.len()
                            );
                        }
                    } else {
                        warn!("Could not parse winner/bid from MPC output");
                    }
                } else {
                    // === BIDDER (party 1..N): Parse only "You won: 0/1" ===
                    let mut i_won = false;

                    for line in stdout.lines() {
                        if line.contains("You won:") {
                            if line.contains("You won: 1") {
                                i_won = true;
                                info!("Result: I won the auction!");
                            } else {
                                info!("Result: I did not win");
                            }
                        }
                    }

                    if i_won {
                        // Wait for seller's WinnerDecryptionRequest challenge
                        info!("I won — waiting for seller's challenge (WinnerDecryptionRequest)");
                    } else {
                        // Lost — cleanup route manager
                        info!("I lost — cleaning up route manager");
                        self.cleanup_route_manager(listing_key).await;
                    }
                }

                // Cleanup tunnel proxy
                tunnel_proxy.cleanup().await;
                *self.active_tunnel_proxy.lock().await = None;

                // Clean up temp hosts file
                let _ = std::fs::remove_file(&hosts_file_path);

                Ok(())
            }
            Err(e) => {
                error!("Failed to execute MP-SPDZ: {}", e);
                tunnel_proxy.cleanup().await;
                *self.active_tunnel_proxy.lock().await = None;

                // Clean up temp hosts file
                let _ = std::fs::remove_file(&hosts_file_path);

                // On error, cleanup route manager immediately
                {
                    let mut managers = self.route_managers.lock().await;
                    managers.remove(&bid_index.listing_key);
                }

                Err(anyhow::anyhow!("MP-SPDZ execution failed: {}", e))
            }
        }
    }

    /// Create a routing context suitable for sending to private routes.
    pub fn safe_routing_context(&self) -> Result<veilid_core::RoutingContext> {
        self.api
            .routing_context()
            .map_err(|e| anyhow::anyhow!("Failed to create routing context: {}", e))?
            .with_safety(SafetySelection::Safe(SafetySpec {
                preferred_route: None,
                hop_count: 2,
                stability: Stability::Reliable,
                sequencing: Sequencing::PreferOrdered,
            }))
            .map_err(|e| anyhow::anyhow!("Failed to set safety: {}", e))
    }

    /// Cleanup route manager for a listing
    pub async fn cleanup_route_manager(&self, listing_key: &RecordKey) {
        let key = listing_key.clone();
        let mut managers = self.route_managers.lock().await;
        if managers.remove(&key).is_some() {
            info!("Cleaned up route manager for listing {}", listing_key);
        }
    }

    /// Send decryption hash to auction winner via their MPC route
    pub async fn send_decryption_hash(
        &self,
        listing_key: &RecordKey,
        winner: &PublicKey,
        decryption_hash: &str,
    ) -> Result<()> {
        info!(
            "Sending decryption hash to winner {} for listing {}",
            winner, listing_key
        );

        let message = AuctionMessage::decryption_hash_transfer(
            listing_key.clone(),
            winner.clone(),
            decryption_hash.to_string(),
        );

        let data = message.to_bytes()?;

        // Look up the winner's route while holding locks, then release before async work
        let winner_route = {
            let managers = self.route_managers.lock().await;
            let route_manager = managers.get(listing_key).ok_or_else(|| {
                anyhow::anyhow!("Route manager not found for listing {}", listing_key)
            })?;
            let mgr = route_manager.lock().await;
            let routes = mgr.received_routes.lock().await;
            routes.get(winner).cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "Winner's MPC route not found in route manager for listing {}",
                    listing_key
                )
            })?
        };

        info!("Found winner's MPC route: {}", winner_route);
        let routing_context = self.safe_routing_context()?;

        match routing_context
            .app_message(Target::RouteId(winner_route.clone()), data)
            .await
        {
            Ok(_) => {
                info!(
                    "Successfully sent decryption hash to winner via MPC route {}",
                    winner_route
                );
                self.cleanup_route_manager(listing_key).await;
                Ok(())
            }
            Err(e) => {
                error!("Failed to send to winner's MPC route: {}", e);
                Err(anyhow::anyhow!("Failed to send decryption hash: {}", e))
            }
        }
    }

    /// Verify winner's revealed bid against stored commitment (Danish Sugar Beet style)
    pub async fn verify_winner_reveal(
        &self,
        listing_key: &RecordKey,
        winner: &PublicKey,
        bid_value: u64,
        nonce: &[u8; 32],
    ) -> bool {
        use sha2::{Digest, Sha256};

        // 1. Get pending verification to check expected winning bid
        let expected_bid = {
            let verifications = self.pending_verifications.lock().await;
            match verifications.get(listing_key) {
                Some(state) => {
                    if &state.winner_pubkey != winner {
                        warn!(
                            "Winner mismatch: expected {}, got {}",
                            state.winner_pubkey, winner
                        );
                        return false;
                    }
                    state.mpc_winning_bid
                }
                None => {
                    warn!("No pending verification found for listing {}", listing_key);
                    return false;
                }
            }
        };

        // 2. Check bid value matches MPC output
        if bid_value != expected_bid {
            warn!(
                "Bid value mismatch: revealed {} but MPC output was {}",
                bid_value, expected_bid
            );
            return false;
        }

        // 3. Fetch winner's BidRecord from DHT to get stored commitment
        let bid_ops = BidOperations::new(self.dht.clone());

        let registry_data = match self.dht.get_value_at_subkey(listing_key, 2).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                warn!("No bid registry found for listing {}", listing_key);
                return false;
            }
            Err(e) => {
                warn!("Failed to fetch bid registry: {}", e);
                return false;
            }
        };

        let registry = match BidAnnouncementRegistry::from_bytes(&registry_data) {
            Ok(r) => r,
            Err(e) => {
                warn!("Failed to parse bid registry: {}", e);
                return false;
            }
        };

        let winner_bid_record_key =
            match registry.announcements.iter().find(|(b, _, _)| b == winner) {
                Some((_, key, _)) => key.clone(),
                None => {
                    warn!("Winner {} not found in bid registry", winner);
                    return false;
                }
            };

        let bid_record = match bid_ops.fetch_bid(&winner_bid_record_key).await {
            Ok(Some(record)) => record,
            Ok(None) => {
                warn!("Winner's bid record not found at {}", winner_bid_record_key);
                return false;
            }
            Err(e) => {
                warn!("Failed to fetch winner's bid record: {}", e);
                return false;
            }
        };

        // 4. Compute commitment: SHA256(bid_value || nonce)
        let mut hasher = Sha256::new();
        hasher.update(bid_value.to_le_bytes());
        hasher.update(nonce);
        let computed_commitment: [u8; 32] = hasher.finalize().into();

        // 5. Compare with stored commitment
        if computed_commitment != bid_record.commitment {
            warn!("Commitment mismatch for winner {}", winner);
            return false;
        }

        info!(
            "Commitment verified for winner {} with bid value {}",
            winner, bid_value
        );
        true
    }

    /// Discover bids from DHT bid registry (n-party approach)
    pub async fn discover_bids_from_storage(
        &self,
        listing_key: &RecordKey,
        local_announcements: Option<&Vec<(PublicKey, RecordKey)>>,
    ) -> Result<BidIndex> {
        info!("Discovering bids from DHT bid registry");

        let mut bid_index = BidIndex::new(listing_key.clone());
        let bid_ops = BidOperations::new(self.dht.clone());

        // Read bid registry from DHT subkey 2
        let registry_data = self.dht.get_value_at_subkey(listing_key, 2).await?;

        let bidder_list = match registry_data {
            Some(data) => {
                let registry = BidAnnouncementRegistry::from_bytes(&data)?;
                info!(
                    "Found {} bidders in DHT bid registry",
                    registry.announcements.len()
                );
                registry.announcements
            }
            None => {
                info!("No DHT bid registry found, trying local announcements as fallback");

                match local_announcements {
                    Some(list) => {
                        info!("Found {} bidders in local announcements", list.len());
                        list.iter()
                            .map(|(bidder, bid_record_key)| {
                                (bidder.clone(), bid_record_key.clone(), 0)
                            })
                            .collect()
                    }
                    None => {
                        info!("No local announcements found either");
                        Vec::new()
                    }
                }
            }
        };

        // Fetch each bidder's BidRecord from the DHT
        for (bidder, bid_record_key, _timestamp) in &bidder_list {
            info!(
                "Fetching bid record for bidder {} at {}",
                bidder, bid_record_key
            );

            match bid_ops.fetch_bid(bid_record_key).await {
                Ok(Some(bid_record)) => {
                    info!("Successfully fetched bid record for bidder {}", bidder);
                    bid_index.add_bid(bid_record);
                }
                Ok(None) => {
                    warn!(
                        "No bid record found for bidder {} at {}",
                        bidder, bid_record_key
                    );
                }
                Err(e) => {
                    warn!("Failed to fetch bid record for bidder {}: {}", bidder, e);
                }
            }
        }

        info!(
            "Built BidIndex with {} bids from DHT registry",
            bid_index.bids.len()
        );
        Ok(bid_index)
    }

    /// Send WinnerDecryptionRequest challenge to winner via their MPC route
    pub async fn send_winner_challenge(&self, listing_key: &RecordKey, winner: &PublicKey) {
        let message =
            AuctionMessage::winner_decryption_request(listing_key.clone(), self.my_node_id.clone());

        let data = match message.to_bytes() {
            Ok(d) => d,
            Err(e) => {
                error!("Failed to serialize WinnerDecryptionRequest: {}", e);
                return;
            }
        };

        // Look up the winner's route while holding locks, then release before async work
        let winner_route = {
            let managers = self.route_managers.lock().await;
            let route_manager = match managers.get(listing_key) {
                Some(rm) => rm.clone(),
                None => {
                    warn!("Route manager not found for listing {}", listing_key);
                    return;
                }
            };
            let mgr = route_manager.lock().await;
            let routes = mgr.received_routes.lock().await;
            match routes.get(winner).cloned() {
                Some(route) => route,
                None => {
                    warn!("Winner's MPC route not found in route manager");
                    return;
                }
            }
        };

        let routing_context = match self.safe_routing_context() {
            Ok(ctx) => ctx,
            Err(e) => {
                error!("Failed to create routing context: {}", e);
                return;
            }
        };

        match routing_context
            .app_message(Target::RouteId(winner_route), data)
            .await
        {
            Ok(_) => {
                info!("Sent WinnerDecryptionRequest challenge to winner via MPC route")
            }
            Err(e) => error!("Failed to send challenge to winner: {}", e),
        }
    }

    /// Get the node ID for this orchestrator
    pub fn my_node_id(&self) -> &PublicKey {
        &self.my_node_id
    }

    /// Get the bid storage
    pub fn bid_storage(&self) -> &BidStorage {
        &self.bid_storage
    }

    /// Get the DHT operations handle
    pub fn dht(&self) -> &DHTOperations {
        &self.dht
    }
}
