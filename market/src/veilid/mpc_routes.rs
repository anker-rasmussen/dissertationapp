use ed25519_dalek::SigningKey;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use veilid_core::{
    PublicKey, RecordKey, RouteBlob, RouteId, SafetySelection, SafetySpec, Sequencing, Stability,
    Target, VeilidAPI, CRYPTO_KIND_VLD0,
};

use super::bid_announcement::AuctionMessage;
use crate::error::{MarketError, MarketResult};

/// Manages Veilid route exchange for MPC parties
pub struct MpcRouteManager {
    api: VeilidAPI,
    party_id: usize,
    my_route_id: Option<RouteId>,
    my_route_blob: Option<RouteBlob>, // Route blob for sharing with other parties
    pub(crate) received_routes: Arc<Mutex<HashMap<PublicKey, RouteId>>>,
    pub(crate) ready_parties: Arc<Mutex<HashSet<PublicKey>>>,
}

impl MpcRouteManager {
    pub fn new(api: VeilidAPI, party_id: usize) -> Self {
        Self {
            api,
            party_id,
            my_route_id: None,
            my_route_blob: None,
            received_routes: Arc::new(Mutex::new(HashMap::new())),
            ready_parties: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Create a private route for this party
    pub async fn create_route(&mut self) -> MarketResult<RouteId> {
        if let Some(route) = &self.my_route_id {
            return Ok(route.clone());
        }

        // Create a private route for receiving MPC traffic
        // Use reliable sequencing for MPC (order matters)
        let route_blob = self
            .api
            .new_custom_private_route(
                &[CRYPTO_KIND_VLD0],
                Stability::Reliable,
                Sequencing::PreferOrdered,
            )
            .await
            .map_err(|e| MarketError::Network(format!("Failed to create route: {e}")))?;

        let route_id = route_blob.route_id.clone();

        info!(
            "Created Veilid route for MPC Party {}: {}",
            self.party_id, route_id
        );

        self.my_route_id = Some(route_id.clone());
        self.my_route_blob = Some(route_blob);
        Ok(route_id)
    }

    /// Broadcast this party's route announcement to peers using their
    /// private route blobs (since `Target::NodeId` is blocked without Veilid's
    /// footgun feature).
    pub async fn broadcast_route_announcement(
        &self,
        listing_key: &RecordKey,
        my_pubkey: &PublicKey,
        peer_route_blobs: &[(String, Vec<u8>)],
        signing_key: &SigningKey,
    ) -> MarketResult<()> {
        let route_blob = self
            .my_route_blob
            .as_ref()
            .ok_or_else(|| MarketError::InvalidState("Route not created yet".into()))?;

        let route_id = self
            .my_route_id
            .as_ref()
            .ok_or_else(|| MarketError::InvalidState("Route ID not set (internal error)".into()))?;
        info!(
            "Broadcasting MPC route {} for party {}",
            route_id, my_pubkey
        );

        let announcement = AuctionMessage::mpc_route_announcement(
            listing_key.clone(),
            my_pubkey.clone(),
            route_blob.clone(),
        );

        let data = announcement.to_signed_bytes(signing_key)?;

        let routing_context = self
            .api
            .routing_context()?
            .with_safety(SafetySelection::Safe(SafetySpec {
                preferred_route: None,
                hop_count: 1,
                stability: Stability::Reliable,
                sequencing: Sequencing::PreferOrdered,
            }))?;

        let mut sent_count = 0;
        for (node_id, blob) in peer_route_blobs {
            match self.api.import_remote_private_route(blob.clone()) {
                Ok(imported_route) => {
                    match routing_context
                        .app_message(Target::RouteId(imported_route.clone()), data.clone())
                        .await
                    {
                        Ok(()) => {
                            debug!("Sent route announcement to peer {}", node_id);
                            sent_count += 1;
                        }
                        Err(e) => warn!("Failed to send route announcement to {}: {}", node_id, e),
                    }
                    // Allow Veilid to flush the message before tearing down the route
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    let _ = self.api.release_private_route(imported_route);
                }
                Err(e) => {
                    debug!("Failed to import route for {}: {}", node_id, e);
                }
            }
        }

        info!("Broadcasted route to {} peers", sent_count);
        Ok(())
    }

    /// Register a route announcement received from another party.
    ///
    /// Always re-imports the route blob so Veilid refreshes its internal
    /// relay state — this recovers from transient `dead_remote_routes` events
    /// that would otherwise leave a stale handle in [`received_routes`].
    pub async fn register_route_announcement(
        &self,
        party_pubkey: PublicKey,
        route_blob: RouteBlob,
    ) -> MarketResult<()> {
        // Always import — re-importing a known blob refreshes Veilid's
        // internal route state, which recovers dead routes.
        let imported_route = self
            .api
            .import_remote_private_route(route_blob.blob.clone())
            .map_err(|e| MarketError::Network(format!("Failed to import remote route: {e}")))?;

        let mut routes = self.received_routes.lock().await;

        if let Some(existing) = routes.get(&party_pubkey) {
            if *existing == imported_route {
                // Same route handle — re-import refreshed it, no map change.
                return Ok(());
            }
            // Different route (party recreated it); release old handle.
            info!(
                "Replacing route for party {} (old: {}, new: {})",
                party_pubkey, existing, imported_route
            );
            let _ = self.api.release_private_route(existing.clone());
        } else {
            info!(
                "Registered and imported route for party {}: {}",
                party_pubkey, imported_route
            );
        }

        routes.insert(party_pubkey.clone(), imported_route);
        drop(routes);
        Ok(())
    }

    /// Assemble party routes from received announcements
    /// Maps PublicKeys to party IDs based on sorted bidder order
    pub async fn assemble_party_routes(
        &self,
        sorted_bidders: &[PublicKey],
    ) -> MarketResult<HashMap<usize, RouteId>> {
        let routes_by_pubkey = self.received_routes.lock().await;
        let mut routes_by_party_id = HashMap::new();

        for (party_id, pubkey) in sorted_bidders.iter().enumerate() {
            if let Some(route) = routes_by_pubkey.get(pubkey) {
                routes_by_party_id.insert(party_id, route.clone());
                info!("Party {} ({}): has route {}", party_id, pubkey, route);
            } else {
                warn!("Missing route for Party {} ({})", party_id, pubkey);
            }
        }

        info!(
            "Assembled {} routes from {} bidders",
            routes_by_party_id.len(),
            sorted_bidders.len()
        );

        Ok(routes_by_party_id)
    }

    /// Get my route ID
    pub const fn get_my_route(&self) -> Option<&RouteId> {
        self.my_route_id.as_ref()
    }

    /// Get my route blob for sharing
    pub const fn get_my_route_blob(&self) -> Option<&RouteBlob> {
        self.my_route_blob.as_ref()
    }

    /// Broadcast an `MpcReady` signal to peers using their private route blobs.
    pub async fn broadcast_mpc_ready(
        &self,
        listing_key: &RecordKey,
        my_pubkey: &PublicKey,
        peer_route_blobs: &[(String, Vec<u8>)],
        signing_key: &SigningKey,
    ) -> MarketResult<()> {
        info!(
            "Broadcasting MpcReady for party {} on listing {}",
            my_pubkey, listing_key
        );

        let ready_msg = AuctionMessage::mpc_ready(listing_key.clone(), my_pubkey.clone());
        let data = ready_msg.to_signed_bytes(signing_key)?;

        let routing_context = self
            .api
            .routing_context()?
            .with_safety(SafetySelection::Safe(SafetySpec {
                preferred_route: None,
                hop_count: 1,
                stability: Stability::Reliable,
                sequencing: Sequencing::PreferOrdered,
            }))?;

        let mut sent_count = 0;
        for (node_id, blob) in peer_route_blobs {
            match self.api.import_remote_private_route(blob.clone()) {
                Ok(imported_route) => {
                    match routing_context
                        .app_message(Target::RouteId(imported_route.clone()), data.clone())
                        .await
                    {
                        Ok(()) => {
                            debug!("Sent MpcReady to peer {}", node_id);
                            sent_count += 1;
                        }
                        Err(e) => warn!("Failed to send MpcReady to {}: {}", node_id, e),
                    }
                    // Allow Veilid to flush the message before tearing down the route
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    let _ = self.api.release_private_route(imported_route);
                }
                Err(e) => {
                    debug!("Failed to import route for {}: {}", node_id, e);
                }
            }
        }

        info!("Broadcasted MpcReady to {} peers", sent_count);
        Ok(())
    }

    /// Register a party as ready. Returns `true` if the party was newly inserted.
    pub async fn register_ready(&self, party_pubkey: PublicKey) -> bool {
        self.ready_parties.lock().await.insert(party_pubkey)
    }

    /// Number of parties that have signalled readiness.
    pub async fn ready_count(&self) -> usize {
        self.ready_parties.lock().await.len()
    }
}
