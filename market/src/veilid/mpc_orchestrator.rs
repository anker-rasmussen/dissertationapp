use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use veilid_core::{
    PublicKey, RecordKey, RouteId, SafetySelection, SafetySpec, Sequencing, Stability, VeilidAPI,
};

use super::bid_announcement::BidAnnouncementRegistry;
use super::bid_ops::BidOperations;
use super::bid_storage::BidStorage;
use super::dht::DHTOperations;
use super::mpc::MpcTunnelProxy;
use super::mpc_execution::{compile_mpc_program, spawn_mascot_party, MpcCleanupGuard};
use super::mpc_routes::MpcRouteManager;
pub use super::mpc_verification::VerificationState;
use crate::config;
use crate::config::subkeys;
use crate::error::{MarketError, MarketResult};
use crate::marketplace::BidIndex;

pub(crate) type RouteManagerMap = Arc<Mutex<HashMap<RecordKey, Arc<Mutex<MpcRouteManager>>>>>;
pub(crate) type VerificationMap = Arc<Mutex<HashMap<RecordKey, VerificationState>>>;

/// Validate that the number of parties is sufficient for MASCOT MPC.
/// Requires at least 2 parties (seller + 1 bidder).
pub const fn validate_auction_parties(num_parties: usize) -> Result<(), &'static str> {
    if num_parties < 2 {
        Err("MASCOT MPC requires at least 2 parties")
    } else {
        Ok(())
    }
}

/// Generate hosts file content for MP-SPDZ.
/// All parties use localhost since Veilid handles actual routing.
pub fn generate_hosts_content(num_parties: usize) -> String {
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
    /// Currently active MPC tunnel proxy (if any)
    pub(crate) active_tunnel_proxy: Arc<Mutex<Option<MpcTunnelProxy>>>,
    /// MPC route managers per auction: Map<listing_key, MpcRouteManager>
    pub(crate) route_managers: RouteManagerMap,
    /// Pending verifications: listing_key -> (winner_pubkey, mpc_winning_bid, verified?)
    pub(crate) pending_verifications: VerificationMap,
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
    pub const fn route_managers(&self) -> &RouteManagerMap {
        &self.route_managers
    }

    /// Access to pending verifications (needed by AuctionCoordinator for message handling)
    pub const fn pending_verifications(&self) -> &VerificationMap {
        &self.pending_verifications
    }

    /// Access to active tunnel proxy (needed by AuctionCoordinator for message forwarding)
    pub const fn active_tunnel_proxy(&self) -> &Arc<Mutex<Option<MpcTunnelProxy>>> {
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

    /// Handle auction end: coordinate MPC execution
    pub async fn handle_auction_end(
        &self,
        listing_key: &RecordKey,
        bid_index: BidIndex,
        listing_title: &str,
        peer_route_blobs: &[(String, Vec<u8>)],
    ) -> MarketResult<()> {
        info!("Handling auction end for listing '{}'", listing_title);

        if bid_index.bids.is_empty() {
            warn!("No bids discovered for listing '{}'", listing_title);
            return Ok(());
        }

        info!("Discovered {} bids for auction", bid_index.bids.len());

        // Sort bidders by timestamp (seller's auto-bid has earliest timestamp = party 0)
        let sorted_bidders = bid_index.sorted_bidders();

        // MASCOT MPC requires at least 2 parties
        if validate_auction_parties(sorted_bidders.len()).is_err() {
            warn!(
                "Auction for '{}' has only {} parties, but MASCOT requires at least 2. Aborting MPC.",
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
                    .exchange_mpc_routes(listing_key, party_id, &sorted_bidders, peer_route_blobs)
                    .await?;

                // Wait for all parties to be ready
                info!("Waiting 2s for route propagation...");
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

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
        peer_route_blobs: &[(String, Vec<u8>)],
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

        // Create our route
        let my_route_id = {
            let mut mgr = route_manager.lock().await;
            mgr.create_route().await?
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
                .broadcast_route_announcement(listing_key, my_pubkey, peer_route_blobs)
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

        // Wait for routes: the seller (party 0) may take extra time to fetch
        // bid records from DHT before creating its MPC route, so allow up to 30s.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let start = std::time::Instant::now();
        let max_wait = std::time::Duration::from_secs(30);

        loop {
            let routes = {
                let mgr = route_manager.lock().await;
                mgr.assemble_party_routes(bidders).await?
            };

            let expected = bidders.len();
            let received = routes.len();

            info!("Route collection: have {}/{} routes", received, expected);

            if received >= expected {
                return Ok(routes);
            }
            if start.elapsed() >= max_wait {
                if received < expected {
                    return Err(MarketError::Network(format!(
                        "Incomplete route exchange: got {received}/{expected} routes after {max_wait:?}"
                    )));
                }
                return Ok(routes);
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    /// Execute the MPC auction computation using MASCOT protocol
    async fn execute_mpc_auction(
        &self,
        party_id: usize,
        num_parties: usize,
        bid_index: &BidIndex,
        routes: HashMap<usize, RouteId>,
        all_parties: &[PublicKey],
    ) -> MarketResult<()> {
        info!(
            "Executing {}-party MPC auction as Party {} (MASCOT)",
            num_parties, party_id
        );

        // Get my bid value from storage
        let listing_key = &bid_index.listing_key;
        let (bid_value, _nonce) = self
            .bid_storage
            .get_bid(listing_key)
            .await
            .ok_or_else(|| MarketError::NotFound("Bid value not found in storage".into()))?;

        debug!("Bid value retrieved from local storage");

        // Start MPC tunnel proxy with the routes
        let tunnel_proxy =
            MpcTunnelProxy::new(self.api.clone(), party_id, routes, self.node_offset);
        tunnel_proxy.run()?;

        // Store tunnel proxy so AppMessages can be routed to it
        *self.active_tunnel_proxy.lock().await = Some(tunnel_proxy.clone());

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

        // Wait for all parties to be ready
        info!("Waiting 3 seconds for all parties to be ready...");
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        // Compile MPC program for the specific number of parties
        compile_mpc_program(&mp_spdz_dir, num_parties).await?;

        // Spawn mascot-party.x and feed bid value via stdin
        let result = spawn_mascot_party(
            &mp_spdz_dir,
            party_id,
            num_parties,
            &hosts_file_path,
            bid_value,
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

        if party_id == 0 {
            self.handle_seller_mpc_result(&stdout, listing_key, all_parties)
                .await;
        } else {
            self.handle_bidder_mpc_result(&stdout, listing_key).await;
        }

        // Clear active tunnel proxy reference before guard drops
        *self.active_tunnel_proxy.lock().await = None;
        // _cleanup_guard drops here, calling tunnel_proxy.cleanup() + removing hosts file
        Ok(())
    }

    /// Create a routing context suitable for sending to private routes.
    pub fn safe_routing_context(&self) -> MarketResult<veilid_core::RoutingContext> {
        Ok(self
            .api
            .routing_context()?
            .with_safety(SafetySelection::Safe(SafetySpec {
                preferred_route: None,
                hop_count: 1,
                stability: Stability::Reliable,
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

        let bidder_list = if let Some(data) = registry_data {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_auction_parties_minimum() {
        assert!(validate_auction_parties(2).is_ok());
    }

    #[test]
    fn test_validate_auction_parties_too_few() {
        assert!(validate_auction_parties(0).is_err());
        assert!(validate_auction_parties(1).is_err());
    }

    #[test]
    fn test_validate_auction_parties_large() {
        assert!(validate_auction_parties(10).is_ok());
        assert!(validate_auction_parties(50).is_ok());
    }

    #[test]
    fn test_hosts_file_content_generation() {
        let content = generate_hosts_content(3);
        assert_eq!(content, "127.0.0.1\n127.0.0.1\n127.0.0.1\n");
        assert_eq!(content.lines().count(), 3);
    }

    #[test]
    fn test_hosts_file_content_single_party() {
        let content = generate_hosts_content(1);
        assert_eq!(content, "127.0.0.1\n");
        assert_eq!(content.lines().count(), 1);
    }
}
