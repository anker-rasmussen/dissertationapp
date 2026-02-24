//! MPC session lifecycle: route exchange, readiness barrier, execution, and post-MPC verification.
//!
//! [`MpcOrchestrator`] is extracted from `AuctionCoordinator` to separate MPC concerns
//! (tunnel proxy, route management, MP-SPDZ process spawning) from auction coordination
//! (bid announcements, listing management, DHT updates).

use ed25519_dalek::SigningKey;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use veilid_core::{PublicKey, RecordKey, RouteId, SafetySelection, Sequencing, VeilidAPI};

use super::bid_announcement::BidAnnouncementRegistry;
use super::bid_ops::BidOperations;
use super::bid_storage::BidStorage;
use super::dht::DHTOperations;
use super::mpc::MpcTunnelProxy;
use super::mpc_execution::{
    compile_mpc_program, spawn_mpc_party, MpcCleanupGuard, MpcResultContract,
};
use super::mpc_routes::MpcRouteManager;
pub(crate) use super::mpc_verification::VerificationState;
use crate::config;
use crate::config::subkeys;
use crate::error::{MarketError, MarketResult};
use crate::marketplace::BidIndex;

pub(crate) type RouteManagerMap = Arc<Mutex<HashMap<RecordKey, Arc<Mutex<MpcRouteManager>>>>>;
pub(crate) type VerificationMap = Arc<Mutex<HashMap<RecordKey, VerificationState>>>;

/// Intermediate state from building party maps for MPC execution.
struct MpcPartyState {
    bid_value: u64,
    party_signers: HashMap<usize, [u8; 32]>,
    party_blobs: HashMap<usize, Vec<u8>>,
    live_routes: Option<Arc<Mutex<HashMap<PublicKey, RouteId>>>>,
    party_pubkeys: HashMap<usize, PublicKey>,
}

/// Validate that the number of parties is sufficient for MPC.
/// Requires at least 2 parties (seller + 1 bidder).
pub const fn validate_auction_parties(num_parties: usize) -> Result<(), &'static str> {
    if num_parties < 2 {
        Err("MPC requires at least 2 parties")
    } else {
        Ok(())
    }
}

/// Generate hosts file content for MP-SPDZ.
/// All parties use localhost since Veilid handles actual routing.
pub(crate) fn generate_hosts_content(num_parties: usize) -> String {
    "127.0.0.1\n".repeat(num_parties)
}

/// Orchestrates MPC execution: route exchange, tunnel proxy lifecycle, bid verification.
///
/// Extracted from `AuctionCoordinator` to separate MPC concerns from auction
/// coordination (bid announcements, listing management, DHT updates).
pub struct MpcOrchestrator {
    pub(crate) api: VeilidAPI,
    pub(crate) dht: DHTOperations,
    pub(crate) my_node_id: PublicKey,
    pub(crate) bid_storage: BidStorage,
    pub(crate) node_offset: u16,
    /// Ed25519 signing key for authenticating outgoing messages.
    pub(crate) signing_key: SigningKey,
    /// Currently active MPC tunnel proxy (if any)
    pub(crate) active_tunnel_proxy: Arc<Mutex<Option<MpcTunnelProxy>>>,
    /// MPC route managers per auction: Map<listing_key, MpcRouteManager>
    pub(crate) route_managers: RouteManagerMap,
    /// Pending verifications: listing_key -> (winner_pubkey, mpc_winning_bid, verified?)
    pub(crate) pending_verifications: VerificationMap,
    /// Listings where this node is the expected winner (set from bidder MPC output).
    pub(crate) expected_winner_listings: Arc<Mutex<HashSet<RecordKey>>>,
    /// Listings currently in MPC route-exchange / execution flow.
    pub(crate) active_auctions: Arc<Mutex<HashSet<RecordKey>>>,
    /// MPC messages that arrived before the tunnel proxy was created.
    /// Flushed to the proxy once it's activated.
    #[allow(clippy::type_complexity)]
    pub(crate) pending_mpc_messages: Arc<Mutex<Vec<(Vec<u8>, [u8; 32])>>>,
    /// Set after MPC completes so the monitoring loop can refresh broadcast
    /// routes immediately.  Routes go stale during MPC (keepalive suppressed)
    /// and peers in a subsequent auction need fresh blobs.
    needs_route_refresh: Arc<std::sync::atomic::AtomicBool>,
}

impl MpcOrchestrator {
    /// Create a new orchestrator bound to a Veilid node.
    ///
    /// The orchestrator shares the same `DHTOperations` and `BidStorage` as
    /// the parent `AuctionCoordinator`, and generates its own MPC route
    /// managers per auction.
    pub fn new(
        api: VeilidAPI,
        dht: DHTOperations,
        my_node_id: PublicKey,
        bid_storage: BidStorage,
        node_offset: u16,
        signing_key: SigningKey,
    ) -> Self {
        Self {
            api,
            dht,
            my_node_id,
            bid_storage,
            node_offset,
            signing_key,
            active_tunnel_proxy: Arc::new(Mutex::new(None)),
            route_managers: Arc::new(Mutex::new(HashMap::new())),
            pending_verifications: Arc::new(Mutex::new(HashMap::new())),
            expected_winner_listings: Arc::new(Mutex::new(HashSet::new())),
            active_auctions: Arc::new(Mutex::new(HashSet::new())),
            pending_mpc_messages: Arc::new(Mutex::new(Vec::new())),
            needs_route_refresh: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Access to route managers (needed by AuctionCoordinator for message handling)
    pub const fn route_managers(&self) -> &RouteManagerMap {
        &self.route_managers
    }

    /// Access to pending verifications (needed by AuctionCoordinator for message handling)
    pub(crate) const fn pending_verifications(&self) -> &VerificationMap {
        &self.pending_verifications
    }

    /// Access to active tunnel proxy (needed by AuctionCoordinator for message forwarding)
    pub const fn active_tunnel_proxy(&self) -> &Arc<Mutex<Option<MpcTunnelProxy>>> {
        &self.active_tunnel_proxy
    }

    /// Buffer an MPC message that arrived before the tunnel proxy was created.
    pub async fn buffer_mpc_message(&self, payload: Vec<u8>, signer: [u8; 32]) {
        self.pending_mpc_messages
            .lock()
            .await
            .push((payload, signer));
    }

    /// Flush buffered MPC messages to the now-active tunnel proxy.
    pub async fn flush_buffered_mpc_messages(&self) {
        let buffered: Vec<_> = self.pending_mpc_messages.lock().await.drain(..).collect();
        if buffered.is_empty() {
            return;
        }
        let proxy = self.active_tunnel_proxy.lock().await.clone();
        if let Some(proxy) = proxy {
            info!(
                "Flushing {} buffered MPC messages to tunnel proxy",
                buffered.len()
            );
            for (payload, signer) in buffered {
                if let Err(e) = proxy.process_message(payload, signer).await {
                    debug!("Error processing buffered MPC message: {}", e);
                }
            }
        }
    }

    /// Mark a listing where this node is the expected winner.
    pub async fn mark_expected_winner(&self, listing_key: &RecordKey) {
        self.expected_winner_listings
            .lock()
            .await
            .insert(listing_key.clone());
    }

    /// Clear expected-winner state for a listing.
    pub async fn clear_expected_winner(&self, listing_key: &RecordKey) {
        self.expected_winner_listings
            .lock()
            .await
            .remove(listing_key);
    }

    /// Returns whether this node is the expected winner for the listing.
    pub async fn is_expected_winner(&self, listing_key: &RecordKey) -> bool {
        self.expected_winner_listings
            .lock()
            .await
            .contains(listing_key)
    }

    /// Returns true when at least one auction is actively running MPC flow.
    pub async fn has_active_auctions(&self) -> bool {
        !self.active_auctions.lock().await.is_empty()
    }

    /// Check and clear the "needs route refresh" flag.  Returns true once
    /// after each MPC completion, then resets to false.
    pub fn take_needs_route_refresh(&self) -> bool {
        self.needs_route_refresh
            .swap(false, std::sync::atomic::Ordering::Relaxed)
    }

    /// Pre-create a route manager for an auction so we can receive route announcements
    /// even before the auction ends.
    pub async fn ensure_route_manager(&self, listing_key: &RecordKey) {
        let key = listing_key.clone();
        let mut managers = self.route_managers.lock().await;

        if !managers.contains_key(&key) {
            let route_manager = Arc::new(Mutex::new(MpcRouteManager::new(
                self.api.clone(),
                0, // Placeholder party ID, will be determined when auction ends
            )));
            managers.insert(key.clone(), route_manager);
            drop(managers);
            info!(
                "Created route manager for listing {} to receive route announcements",
                listing_key
            );
        }
    }

    /// Handle auction end: the full MPC lifecycle from route exchange to verification.
    ///
    /// Steps: validate party count, assign party IDs from `bid_index`, exchange MPC
    /// routes, run readiness barrier, compile MP-SPDZ program, spawn tunnel proxy,
    /// execute `mascot-party.x`, parse output, and (for seller) initiate winner
    /// verification. Sets `needs_route_refresh` on completion.
    pub async fn handle_auction_end(
        &self,
        listing_key: &RecordKey,
        bid_index: BidIndex,
        listing_title: &str,
        peer_route_blobs: &[(String, Vec<u8>)],
        seller_pubkey: &PublicKey,
        broadcast_route: Option<&(RouteId, Vec<u8>)>,
    ) -> MarketResult<()> {
        info!("Handling auction end for listing '{}'", listing_title);

        if bid_index.bids.is_empty() {
            // We only reach here when we know we bid on this listing
            // (checked by handle_auction_end_wrapper), so an empty index
            // means the DHT hasn't propagated yet — retry.
            warn!(
                "No bids discovered for listing '{}' — DHT may not have propagated",
                listing_title
            );
            return Err(MarketError::NotFound(
                "No bids discovered yet, DHT may not have propagated".into(),
            ));
        }

        info!("Discovered {} bids for auction", bid_index.bids.len());

        // Sort bidders: seller = party 0, remaining sorted by pubkey
        let sorted_bidders = bid_index.sorted_bidders(seller_pubkey);

        // MPC requires at least 2 parties.
        // Return Err so the monitoring loop retries — DHT propagation may
        // not have completed yet, and more bids may appear on the next tick.
        if validate_auction_parties(sorted_bidders.len()).is_err() {
            warn!(
                "Auction for '{}' has only {} parties, but MPC requires at least 2 — will retry.",
                listing_title,
                sorted_bidders.len()
            );
            return Err(MarketError::NotFound(format!(
                "Only {} parties discovered (need ≥ 2), DHT may not have propagated yet",
                sorted_bidders.len()
            )));
        }

        // Check if we're a bidder
        let my_party_id = sorted_bidders.iter().position(|b| b == &self.my_node_id);

        match my_party_id {
            Some(party_id) => {
                let active_key = listing_key.clone();
                self.active_auctions.lock().await.insert(active_key.clone());

                info!(
                    "I am Party {} in this {}-party auction",
                    party_id,
                    sorted_bidders.len()
                );

                let result = async {
                    // Exchange routes + readiness barrier with other parties
                    let routes = self
                        .exchange_mpc_routes(
                            listing_key,
                            party_id,
                            &sorted_bidders,
                            peer_route_blobs,
                            broadcast_route,
                        )
                        .await?;

                    // Trigger MPC execution
                    self.execute_mpc_auction(
                        party_id,
                        sorted_bidders.len(),
                        &bid_index,
                        routes,
                        &sorted_bidders,
                        seller_pubkey,
                    )
                    .await
                }
                .await;

                self.active_auctions.lock().await.remove(&active_key);
                // Signal the monitoring loop to refresh broadcast routes
                // immediately — they went stale during MPC execution.
                self.needs_route_refresh
                    .store(true, std::sync::atomic::Ordering::Relaxed);

                // On failure (barrier timeout, route collection timeout),
                // reset stale route state so the next retry starts fresh.
                // Ready signals are preserved — see Phase 2 race comment.
                if result.is_err() {
                    let managers = self.route_managers.lock().await;
                    if let Some(manager) = managers.get(listing_key) {
                        manager.lock().await.reset_routes().await;
                    }
                }

                result?;
            }
            None => {
                debug!("Not a bidder in this auction, skipping MPC participation");
            }
        }

        Ok(())
    }

    /// Exchange MPC routes with all other bidders, then wait for all parties
    /// to signal readiness before returning.
    pub async fn exchange_mpc_routes(
        &self,
        listing_key: &RecordKey,
        my_party_id: usize,
        bidders: &[PublicKey],
        peer_route_blobs: &[(String, Vec<u8>)],
        broadcast_route: Option<&(RouteId, Vec<u8>)>,
    ) -> MarketResult<HashMap<usize, RouteId>> {
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
                    my_party_id,
                )))
            }).clone()
        };

        // Use the broadcast route (maintained by keepalive) if available,
        // otherwise create a fresh MPC-specific route.  Broadcast routes
        // survive much longer because the coordinator refreshes them.
        let my_route_id = {
            let mut mgr = route_manager.lock().await;
            if let Some((route_id, blob)) = broadcast_route {
                mgr.use_existing_route(route_id.clone(), blob.clone());
                route_id.clone()
            } else {
                mgr.create_route().await?
            }
        };

        info!(
            "Created Veilid route for Party {}: {}",
            my_party_id, my_route_id
        );

        // Broadcast our route via peer private routes
        {
            let my_pubkey = &bidders[my_party_id];
            route_manager
                .lock()
                .await
                .broadcast_route_announcement(
                    listing_key,
                    my_pubkey,
                    peer_route_blobs,
                    &self.signing_key,
                )
                .await?;
        }

        // Register our own route (so we include ourselves in the assembly)
        {
            let mgr = route_manager.lock().await;
            let my_pubkey = bidders[my_party_id].clone();
            let my_route_blob = mgr
                .get_my_route_blob()
                .ok_or_else(|| MarketError::InvalidState("Route blob not found".into()))?
                .clone();
            drop(mgr);
            route_manager
                .lock()
                .await
                .register_route_announcement(my_pubkey, my_route_blob)
                .await?;
        }

        // Phase 1 — Route collection
        self.collect_party_routes(
            &route_manager,
            listing_key,
            my_party_id,
            bidders,
            peer_route_blobs,
        )
        .await?;

        // Phase 2 — Readiness barrier
        self.run_readiness_barrier(
            &route_manager,
            listing_key,
            my_party_id,
            bidders,
            peer_route_blobs,
        )
        .await?;

        // Phase 3 — Post-barrier route refresh
        self.refresh_routes_post_barrier(
            &route_manager,
            listing_key,
            my_party_id,
            bidders,
            peer_route_blobs,
        )
        .await
    }

    /// Execute the MPC auction computation
    async fn execute_mpc_auction(
        &self,
        party_id: usize,
        num_parties: usize,
        bid_index: &BidIndex,
        routes: HashMap<usize, RouteId>,
        all_parties: &[PublicKey],
        seller_pubkey: &PublicKey,
    ) -> MarketResult<()> {
        info!(
            "Executing {}-party MPC auction as Party {}",
            num_parties, party_id
        );

        let listing_key = &bid_index.listing_key;
        let maps = self
            .build_party_maps(bid_index, seller_pubkey, all_parties)
            .await?;

        // Start MPC tunnel proxy with the routes
        let tunnel_proxy = MpcTunnelProxy::new(super::mpc::MpcTunnelConfig {
            api: self.api.clone(),
            party_id,
            party_routes: routes,
            party_blobs: maps.party_blobs,
            node_offset: self.node_offset,
            signing_key: self.signing_key.clone(),
            party_signers: maps.party_signers,
            live_routes: maps.live_routes,
            party_pubkeys: maps.party_pubkeys,
        });
        tunnel_proxy.run()?;

        // Store tunnel proxy so AppMessages can be routed to it
        *self.active_tunnel_proxy.lock().await = Some(tunnel_proxy.clone());

        // Flush any MPC messages that arrived before the tunnel proxy was created
        // (from faster parties that already started their MPC execution).
        self.flush_buffered_mpc_messages().await;

        // Warm up MPC routes: send Pings to all peers and wait until all
        // peers have pinged us back.  This ensures routes are alive and
        // delivering messages before starting MP-SPDZ (which has no tolerance
        // for message loss during the initial handshake).
        tunnel_proxy
            .warmup(std::time::Duration::from_secs(
                config::MPC_TUNNEL_WARMUP_TIMEOUT_SECS,
            ))
            .await?;

        let mp_spdz_dir = std::env::var(config::MP_SPDZ_DIR_ENV)
            .unwrap_or_else(|_| config::DEFAULT_MP_SPDZ_DIR.to_string());

        // Generate hosts file in temp dir (MP-SPDZ needs a real file for -ip)
        let listing_key_str = listing_key.to_string();
        let listing_prefix = listing_key_str.get(5..15).unwrap_or("unknown");
        let hosts_file_name = format!("HOSTS-{listing_prefix}");
        let hosts_file_path = std::env::temp_dir().join(&hosts_file_name);

        // Write hosts file with all parties as localhost (Veilid handles actual routing)
        let hosts_content = generate_hosts_content(num_parties);
        tokio::fs::write(&hosts_file_path, &hosts_content)
            .await
            .map_err(|e| {
                MarketError::Process(format!(
                    "Failed to write hosts file {}: {e}",
                    hosts_file_path.display()
                ))
            })?;

        info!(
            "Wrote hosts file to {:?} for {} parties",
            hosts_file_path, num_parties
        );

        // RAII guard ensures tunnel proxy cleanup + hosts file removal on all exit paths
        let _cleanup_guard = MpcCleanupGuard::new(tunnel_proxy, hosts_file_path.clone());

        // Compile MPC program for the specific number of parties
        compile_mpc_program(&mp_spdz_dir, num_parties).await?;

        // Spawn MP-SPDZ process and feed bid value via stdin
        let base_port = super::mpc::base_port_for_offset(self.node_offset);
        let result = spawn_mpc_party(
            &mp_spdz_dir,
            party_id,
            num_parties,
            &hosts_file_path,
            maps.bid_value,
            base_port,
            1800, // 30-minute timeout for MASCOT OT over Veilid routes
        )
        .await;

        // On spawn/execution error, also cleanup route manager before propagating
        let process_output = match result {
            Ok(output) => output,
            Err(e) => {
                *self.active_tunnel_proxy.lock().await = None;
                self.cleanup_route_manager(listing_key).await;
                return Err(e);
            }
        };

        self.process_mpc_result(party_id, &process_output, listing_key, all_parties)
            .await?;

        // Clear active tunnel proxy reference before guard drops
        *self.active_tunnel_proxy.lock().await = None;
        // _cleanup_guard drops here, calling tunnel_proxy.cleanup() + removing hosts file

        // Route manager cleanup is handled by the per-role handlers:
        //   - Losers: handle_bidder_mpc_result() cleans up immediately
        //   - No-sale sellers: handle_seller_mpc_result() cleans up immediately
        //   - Winners: cleaned up after receiving decryption hash (auction_coordinator)
        //   - Sellers with a sale: cleaned up after sending decryption hash (mpc_verification)
        Ok(())
    }

    /// Phase 1 — Wait for all parties' route announcements.
    async fn collect_party_routes(
        &self,
        route_manager: &Arc<Mutex<MpcRouteManager>>,
        listing_key: &RecordKey,
        my_party_id: usize,
        bidders: &[PublicKey],
        peer_route_blobs: &[(String, Vec<u8>)],
    ) -> MarketResult<()> {
        // The seller (party 0) may take extra time to fetch bid records
        // from DHT before creating its MPC route, so allow initial delay.
        tokio::time::sleep(std::time::Duration::from_secs(
            config::MPC_ROUTE_ANNOUNCE_DELAY_SECS,
        ))
        .await;

        let start = std::time::Instant::now();
        let max_wait = std::time::Duration::from_secs(config::MPC_ROUTE_COLLECTION_TIMEOUT_SECS);
        let mut rebroadcast_counter: u8 = 0;

        loop {
            let received = {
                let mgr = route_manager.lock().await;
                mgr.assemble_party_routes(bidders).await?.len()
            };

            let expected = bidders.len();

            info!("Route collection: have {}/{} routes", received, expected);

            if received >= expected {
                break;
            }
            if start.elapsed() >= max_wait {
                return Err(MarketError::Network(format!(
                    "Incomplete route exchange: got {received}/{expected} routes after {max_wait:?}"
                )));
            }

            // Periodically re-broadcast our route using the peer routes we
            // already have, so late-joining parties can receive our MPC
            // announcement.  Uses pre-fetched peer_route_blobs (no DHT
            // overhead).
            rebroadcast_counter = rebroadcast_counter.wrapping_add(1);
            if rebroadcast_counter.is_multiple_of(3) && !peer_route_blobs.is_empty() {
                let my_pubkey = &bidders[my_party_id];
                route_manager
                    .lock()
                    .await
                    .broadcast_route_announcement(
                        listing_key,
                        my_pubkey,
                        peer_route_blobs,
                        &self.signing_key,
                    )
                    .await?;
            }

            tokio::time::sleep(std::time::Duration::from_secs(
                config::MPC_BARRIER_POLL_SECS,
            ))
            .await;
        }

        Ok(())
    }

    /// Phase 2 — Readiness barrier: wait for all parties to signal `MpcReady`.
    ///
    /// Ready signals are accumulated (not cleared) to avoid a race where a
    /// party re-entering the barrier clears another party's signal.
    async fn run_readiness_barrier(
        &self,
        route_manager: &Arc<Mutex<MpcRouteManager>>,
        listing_key: &RecordKey,
        my_party_id: usize,
        bidders: &[PublicKey],
        peer_route_blobs: &[(String, Vec<u8>)],
    ) -> MarketResult<()> {
        #[allow(clippy::cast_possible_truncation)] // party count always fits in u32
        let num_parties = bidders.len() as u32;
        {
            let mgr = route_manager.lock().await;
            mgr.register_ready(
                bidders[my_party_id].clone(),
                num_parties,
                crate::config::now_unix(),
            )
            .await;
            drop(mgr);
        }

        let my_pubkey = &bidders[my_party_id];
        {
            let mgr = route_manager.lock().await;
            // Send via broadcast routes (reaches peers still in Phase 1)
            mgr.broadcast_mpc_ready(
                listing_key,
                my_pubkey,
                num_parties,
                peer_route_blobs,
                &self.signing_key,
            )
            .await?;
            // Also send via MPC routes (known-good from Phase 1)
            let _ = mgr
                .send_ready_via_mpc_routes(listing_key, my_pubkey, num_parties, &self.signing_key)
                .await;
            drop(mgr);
        }

        let barrier_start = std::time::Instant::now();
        // Allow 120s for the barrier: when a peer's barrier times out, it
        // retries the full handle_auction_end flow (DHT fetch + route
        // exchange) which takes ~30s.  120s accommodates multiple retry cycles.
        let barrier_timeout =
            std::time::Duration::from_secs(config::MPC_READINESS_BARRIER_TIMEOUT_SECS);
        let mut barrier_tick: u8 = 0;

        loop {
            let ready = route_manager.lock().await.ready_count().await;
            let expected = bidders.len();

            info!("Readiness barrier: {}/{} parties ready", ready, expected);

            if ready >= expected {
                info!("All parties ready, proceeding to MPC execution");
                // Synchronization pause: give every party time to also
                // pass the barrier before any party creates a tunnel
                // proxy.  Without this, the fastest party sends Opens
                // to peers whose tunnel proxy doesn't exist yet,
                // causing silent message drops.
                tokio::time::sleep(std::time::Duration::from_secs(
                    config::MPC_POST_BARRIER_SETTLE_SECS,
                ))
                .await;
                break;
            }
            if barrier_start.elapsed() >= barrier_timeout {
                return Err(MarketError::Network(format!(
                    "MPC readiness barrier timed out: {ready}/{expected} parties ready after {barrier_timeout:?}"
                )));
            }

            // Re-broadcast every 3 ticks (~3s).  Use the MPC routes
            // collected in Phase 1 (known-good) as the primary delivery
            // path; broadcast routes from the registry may be stale.
            barrier_tick = barrier_tick.wrapping_add(1);
            if barrier_tick.is_multiple_of(3) {
                let mgr = route_manager.lock().await;
                // Primary: send via already-collected MPC routes (Phase 1)
                let _ = mgr
                    .send_ready_via_mpc_routes(
                        listing_key,
                        my_pubkey,
                        num_parties,
                        &self.signing_key,
                    )
                    .await;
                // Fallback: also try broadcast routes for peers that
                // haven't finished Phase 1 yet.
                if !peer_route_blobs.is_empty() {
                    let _ = mgr
                        .broadcast_mpc_ready(
                            listing_key,
                            my_pubkey,
                            num_parties,
                            peer_route_blobs,
                            &self.signing_key,
                        )
                        .await;
                }
                drop(mgr);
            }

            tokio::time::sleep(std::time::Duration::from_secs(
                config::MPC_BARRIER_POLL_SECS,
            ))
            .await;
        }

        Ok(())
    }

    /// Phase 3 — Create fresh routes after the readiness barrier and reassemble.
    ///
    /// MPC routes created in Phase 1 likely died during the 30-120s barrier
    /// (Veilid routes expire every 60-90s in devnets).
    async fn refresh_routes_post_barrier(
        &self,
        route_manager: &Arc<Mutex<MpcRouteManager>>,
        listing_key: &RecordKey,
        my_party_id: usize,
        bidders: &[PublicKey],
        peer_route_blobs: &[(String, Vec<u8>)],
    ) -> MarketResult<HashMap<usize, RouteId>> {
        info!("Post-barrier route refresh: creating fresh route");
        {
            let mut mgr = route_manager.lock().await;
            let my_pk = bidders[my_party_id].clone();
            mgr.refresh_own_route(&my_pk).await?;

            // Broadcast fresh route to peers via their broadcast routes
            // (the broadcast route registry is maintained by the coordinator's
            // keepalive task and stays reachable longer than MPC routes).
            if !peer_route_blobs.is_empty() {
                let _ = mgr
                    .broadcast_route_announcement(
                        listing_key,
                        &my_pk,
                        peer_route_blobs,
                        &self.signing_key,
                    )
                    .await;
            }
            drop(mgr);
        }

        // Wait for peers to also refresh and for their announcements to arrive.
        tokio::time::sleep(std::time::Duration::from_secs(
            config::MPC_ROUTE_PROPAGATION_WAIT_SECS,
        ))
        .await;

        // Re-import all received blobs to refresh Veilid's route cache,
        // then reassemble routes with any updates received during the wait.
        let routes = {
            let mgr = route_manager.lock().await;
            mgr.refresh_routes().await;
            mgr.assemble_party_routes(bidders).await?
        };

        info!(
            "Post-barrier route refresh complete: {} routes assembled",
            routes.len()
        );

        Ok(routes)
    }

    /// Build party maps (signers, blobs, live routes, pubkeys) for MPC execution.
    async fn build_party_maps(
        &self,
        bid_index: &BidIndex,
        seller_pubkey: &PublicKey,
        all_parties: &[PublicKey],
    ) -> MarketResult<MpcPartyState> {
        let listing_key = &bid_index.listing_key;
        let (bid_value, _nonce) = self
            .bid_storage
            .get_bid(listing_key)
            .await
            .ok_or_else(|| MarketError::NotFound("Bid value not found in storage".into()))?;

        debug!("Bid value retrieved from local storage");

        // Build party_signers map from bid_index: party_id → signing_pubkey.
        let party_signers: HashMap<usize, [u8; 32]> = {
            let sorted = bid_index.sorted_bidders(seller_pubkey);
            sorted
                .iter()
                .enumerate()
                .filter_map(|(pid, pubkey)| {
                    bid_index
                        .bids
                        .iter()
                        .find(|b| &b.bidder == pubkey)
                        .map(|bid| (pid, bid.signing_pubkey))
                })
                .collect()
        };

        // Gather route blobs for tunnel proxy re-import support
        let party_blobs = {
            let managers = self.route_managers.lock().await;
            if let Some(mgr) = managers.get(listing_key) {
                let mgr = mgr.lock().await;
                let blobs = mgr.assemble_party_blobs(all_parties).await;
                drop(mgr);
                blobs
            } else {
                HashMap::new()
            }
        };

        // Refresh imported route handles right before tunnel proxy use
        // to ensure 5-minute LRU cache is fresh.
        let live_routes = {
            let managers = self.route_managers.lock().await;
            if let Some(mgr) = managers.get(listing_key) {
                let mgr = mgr.lock().await;
                mgr.refresh_routes().await;
                Some(mgr.received_routes.clone())
            } else {
                None
            }
        };

        // Build party_id → PublicKey mapping for live route lookups
        let party_pubkeys: HashMap<usize, PublicKey> = all_parties
            .iter()
            .enumerate()
            .map(|(pid, pk)| (pid, pk.clone()))
            .collect();

        Ok(MpcPartyState {
            bid_value,
            party_signers,
            party_blobs,
            live_routes,
            party_pubkeys,
        })
    }

    /// Parse MPC output and handle role-specific result (seller vs bidder).
    async fn process_mpc_result(
        &self,
        party_id: usize,
        process_output: &std::process::Output,
        listing_key: &RecordKey,
        all_parties: &[PublicKey],
    ) -> MarketResult<()> {
        let stdout = String::from_utf8_lossy(&process_output.stdout);
        let stderr = String::from_utf8_lossy(&process_output.stderr);
        info!(
            "MP-SPDZ execution completed (exit: {:?})",
            process_output.status.code()
        );
        if !stdout.is_empty() {
            debug!("STDOUT:\n{}", stdout);
        }
        if !stderr.is_empty() {
            warn!("STDERR:\n{}", stderr);
        }

        // Parse a machine-readable MPC result contract from stdout.
        let result_contract = if party_id == 0 {
            MpcResultContract::from_seller_stdout(&stdout)?
        } else {
            MpcResultContract::from_bidder_stdout(&stdout)?
        };

        if party_id == 0 {
            self.handle_seller_mpc_result(&result_contract, listing_key, all_parties)
                .await;
        } else {
            self.handle_bidder_mpc_result(&result_contract, listing_key)
                .await;
        }

        Ok(())
    }

    /// Create a routing context suitable for sending to private routes.
    ///
    /// Uses `Safe` with 1 hop and `LowLatency` stability for better
    /// relay selection in small devnets.
    pub fn safe_routing_context(&self) -> MarketResult<veilid_core::RoutingContext> {
        Ok(self
            .api
            .routing_context()?
            .with_safety(SafetySelection::Safe(veilid_core::SafetySpec {
                preferred_route: None,
                hop_count: 1,
                stability: veilid_core::Stability::LowLatency,
                sequencing: Sequencing::PreferOrdered,
            }))?)
    }

    /// Cleanup route manager for a listing
    pub async fn cleanup_route_manager(&self, listing_key: &RecordKey) {
        let key = listing_key.clone();
        let mut managers = self.route_managers.lock().await;
        if managers.remove(&key).is_some() {
            info!("Cleaned up route manager for listing {}", listing_key);
        }
    }

    /// Discover bids from DHT bid registry (n-party approach)
    pub async fn discover_bids_from_storage(
        &self,
        listing_key: &RecordKey,
        local_announcements: Option<&Vec<(PublicKey, RecordKey)>>,
    ) -> MarketResult<BidIndex> {
        info!("Discovering bids from DHT bid registry");

        let mut bid_index = BidIndex::new(listing_key.clone());
        let bid_ops = BidOperations::new(self.dht.clone());

        // Read bid registry from DHT
        let registry_data = self
            .dht
            .get_value_at_subkey(listing_key, subkeys::BID_ANNOUNCEMENTS, true)
            .await?;

        let mut bidder_list = if let Some(data) = registry_data {
            let registry = BidAnnouncementRegistry::from_bytes(&data)?;
            info!(
                "Found {} bidders in DHT bid registry",
                registry.announcements.len()
            );
            registry.announcements
        } else {
            info!("No DHT bid registry found, trying local announcements as fallback");

            if let Some(list) = local_announcements {
                info!("Found {} bidders in local announcements", list.len());
                list.iter()
                    .map(|(bidder, bid_record_key)| (bidder.clone(), bid_record_key.clone(), 0))
                    .collect()
            } else {
                info!("No local announcements found either");
                Vec::new()
            }
        };

        // Supplement with locally-received bid announcements that DHT hasn't
        // propagated yet.  Broadcast routes deliver messages within seconds
        // while DHT value propagation can lag behind, causing some nodes to
        // see fewer bids than actually exist.
        if let Some(local) = local_announcements {
            let dht_bidders: std::collections::HashSet<PublicKey> =
                bidder_list.iter().map(|(pk, _, _)| pk.clone()).collect();
            let extras: Vec<_> = local
                .iter()
                .filter(|(bidder, _)| !dht_bidders.contains(bidder))
                .map(|(bidder, bid_key)| (bidder.clone(), bid_key.clone(), 0))
                .collect();
            for extra in extras {
                info!(
                    "Adding locally-known bidder {} missing from DHT registry",
                    extra.0
                );
                bidder_list.push(extra);
            }
        }

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

        let fetched = bid_index.bids.len();
        let announced = bidder_list.len();
        info!(
            "Built BidIndex with {}/{} bids from DHT registry",
            fetched, announced
        );

        // All announced bids must be fetchable. A partial view causes parties
        // to disagree on the party count, running incompatible MPC programs
        // (e.g. auction_n-2 vs auction_n-3).  Return an error so the
        // monitoring loop retries after DHT propagation catches up.
        if fetched < announced {
            return Err(MarketError::NotFound(format!(
                "Only fetched {fetched}/{announced} bid records from DHT — retrying"
            )));
        }

        Ok(bid_index)
    }

    /// Get the node ID for this orchestrator
    pub const fn my_node_id(&self) -> &PublicKey {
        &self.my_node_id
    }

    /// Get the bid storage
    pub const fn bid_storage(&self) -> &BidStorage {
        &self.bid_storage
    }

    /// Get the DHT operations handle
    pub const fn dht(&self) -> &DHTOperations {
        &self.dht
    }
}
