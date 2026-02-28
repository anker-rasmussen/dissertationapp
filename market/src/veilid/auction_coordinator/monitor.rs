//! Background monitoring, route management, and auction-end orchestration.

use std::sync::Arc;
use tracing::{debug, error, info, warn};
use veilid_core::{RecordKey, Sequencing, Stability, CRYPTO_KIND_VLD0};

use super::AuctionCoordinator;
use crate::config;
use crate::error::{MarketError, MarketResult};
use crate::marketplace::PublicListing;

impl AuctionCoordinator {
    /// Handle `VeilidUpdate::RouteChange` — re-import dead remote routes and
    /// recreate dead own routes for all active MPC route managers.
    pub async fn handle_route_change(
        &self,
        dead_routes: Vec<veilid_core::RouteId>,
        dead_remote_routes: Vec<veilid_core::RouteId>,
    ) {
        if dead_routes.is_empty() && dead_remote_routes.is_empty() {
            return;
        }

        if !dead_routes.is_empty() {
            debug!("RouteChange: {} dead own routes", dead_routes.len());
        }
        if !dead_remote_routes.is_empty() {
            debug!(
                "RouteChange: {} dead remote routes",
                dead_remote_routes.len()
            );
        }

        // Refresh route managers (MPC coordination routes)
        let mut new_route_blob: Option<Vec<u8>> = None;
        let managers = self.mpc.route_managers().lock().await;
        for (_key, manager) in managers.iter() {
            let mut mgr = manager.lock().await;
            if !dead_remote_routes.is_empty() {
                mgr.handle_dead_remote_routes(&dead_remote_routes).await;
            }
            if !dead_routes.is_empty() || !dead_remote_routes.is_empty() {
                if let Some(blob) = mgr
                    .handle_dead_own_route(&dead_routes, &dead_remote_routes)
                    .await
                {
                    new_route_blob = Some(blob.blob);
                }
            }
            drop(mgr);
        }
        drop(managers);

        // Refresh active MPC tunnel proxy routes.
        // app_message to a dead route silently succeeds (fire-and-forget),
        // so we proactively reimport all party blobs on any route death.
        let all_dead: Vec<_> = dead_routes
            .iter()
            .chain(dead_remote_routes.iter())
            .cloned()
            .collect();
        if let Some(tunnel) = self.mpc.active_tunnel_proxy().lock().await.as_ref() {
            tunnel.handle_dead_routes(&all_dead).await;
            // If our own receiving route was recreated, broadcast the new
            // blob to all peers so they update their sending table.
            if let Some(blob) = new_route_blob {
                tunnel.broadcast_route_update(blob).await;
            }
        }
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

        // Self-import the route blob so Veilid marks it as deliverable.
        // Without this, only one of N created routes actually works.
        // See: https://gitlab.com/nicator/veilid — confirmed by Veilid core team.
        let _self_route = self
            .api
            .import_remote_private_route(blob_bytes.clone())
            .map_err(|e| {
                MarketError::Network(format!("Failed to self-import broadcast route: {e}"))
            })?;

        info!(
            "Created broadcast private route: {} ({} bytes)",
            route_blob.route_id,
            blob_bytes.len()
        );

        // Store locally (blob + route ID for MPC reuse)
        *self.broadcast_route_id.lock().await = Some(route_blob.route_id.clone());
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
        let monitor_self = self.clone();

        let handle = tokio::spawn(async move {
            // Every node independently opens/creates the deterministic master
            // registry on startup.  The key is derived from the shared network
            // keypair, so all nodes converge on the same DHT record without
            // needing a broadcast.
            match monitor_self.ensure_master_registry().await {
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
                    () = tokio::time::sleep(tokio::time::Duration::from_secs(config::MONITOR_POLL_INTERVAL_SECS)) => {}
                }
                tick_count = tick_count.wrapping_add(1);

                // The MPC orchestrator sets this flag after MPC execution
                // finishes (active_auctions.remove).  Routes go stale during
                // MPC (keepalive is suppressed) and peers in a subsequent
                // auction need fresh blobs immediately.
                let mpc_just_finished = monitor_self.mpc.take_needs_route_refresh();

                // Create broadcast route on first tick, then refresh every 12 ticks
                // (~60s) to prevent relay nodes from dropping stale routes.
                let should_refresh_route =
                    !broadcast_route_ready || tick_count.is_multiple_of(12) || mpc_just_finished;
                if should_refresh_route {
                    if monitor_self.mpc.has_active_auctions().await {
                        debug!(
                            "Skipping broadcast route refresh at tick {} while MPC is active",
                            tick_count
                        );
                    } else {
                        if mpc_just_finished {
                            info!("MPC finished — forcing immediate broadcast route refresh");
                        }
                        match monitor_self.create_and_register_broadcast_route().await {
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
                // every tick for the first 12 ticks (~60s), then every 6th tick (~30s)
                let should_broadcast = tick_count <= 12 || tick_count.is_multiple_of(6);
                if should_broadcast {
                    if let Err(e) = monitor_self.broadcast_registry_announcement().await {
                        debug!("Periodic registry broadcast failed: {}", e);
                    }
                }

                // Retry any bid announcements that failed due to missing routes
                if broadcast_route_ready {
                    monitor_self.retry_pending_bid_announcements().await;
                }

                let expired = monitor_self.logic.get_expired_listings().await;

                for (key, listing) in expired {
                    info!("Auction deadline reached for listing '{}'", listing.title);

                    match monitor_self
                        .handle_auction_end_wrapper(&key, &listing)
                        .await
                    {
                        Ok(()) => monitor_self.logic.unwatch_listing(&key).await,
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

        // Store the handle for potential awaiting on shutdown
        if let Ok(mut guard) = self.monitor_handle.lock() {
            *guard = Some(handle);
        }
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
        let max_wait = std::time::Duration::from_secs(config::PEER_ROUTE_WAIT_SECS);
        let peer_route_blobs = loop {
            let blobs = {
                let mut ops = self.registry_ops.lock().await;
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

            tokio::time::sleep(tokio::time::Duration::from_secs(
                config::WINNER_REVEAL_RETRY_SECS,
            ))
            .await;
        };

        // Pass our broadcast route (kept alive by keepalive task) for MPC reuse.
        // Freshly created MPC routes die within seconds without keepalive.
        let broadcast_route = {
            let rid = self.broadcast_route_id.lock().await.clone();
            let blob = self.my_route_blob.lock().await.clone();
            rid.zip(blob)
        };

        self.mpc
            .handle_auction_end(
                listing_key,
                bid_index,
                &listing.title,
                &peer_route_blobs,
                &listing.seller,
                broadcast_route.as_ref(),
            )
            .await
    }
}
