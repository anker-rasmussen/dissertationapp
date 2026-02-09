use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use veilid_core::{
    PublicKey, RecordKey, RouteBlob, RouteId, SafetySelection, Sequencing, Stability, Target,
    VeilidAPI, CRYPTO_KIND_VLD0,
};

use super::bid_announcement::AuctionMessage;
use super::dht::DHTOperations;

/// Manages Veilid route exchange for MPC parties
pub struct MpcRouteManager {
    api: VeilidAPI,
    _dht: DHTOperations,
    party_id: usize,
    my_route_id: Option<RouteId>,
    my_route_blob: Option<RouteBlob>, // Route blob for sharing with other parties
    pub(crate) received_routes: Arc<Mutex<HashMap<PublicKey, RouteId>>>,
}

impl MpcRouteManager {
    pub fn new(api: VeilidAPI, dht: DHTOperations, party_id: usize) -> Self {
        Self {
            api,
            _dht: dht,
            party_id,
            my_route_id: None,
            my_route_blob: None,
            received_routes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Set a pre-existing route for reuse across auctions.
    pub fn set_existing_route(&mut self, route_id: RouteId, route_blob: RouteBlob) {
        self.my_route_id = Some(route_id);
        self.my_route_blob = Some(route_blob);
    }

    /// Create a private route for this party (retries on transient failures).
    pub async fn create_route(&mut self) -> Result<RouteId> {
        if let Some(route) = &self.my_route_id {
            return Ok(route.clone());
        }

        // Retry up to 6 times with 3s between attempts (~15s max wait).
        // `TryAgain` is common on small devnets where the routing table
        // hasn't yet discovered enough relay-capable peers.
        let mut last_err = String::new();
        for attempt in 0..6 {
            if attempt > 0 {
                warn!(
                    "Route creation attempt {} failed ({}), retrying in 3s...",
                    attempt, last_err
                );
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }

            match self
                .api
                .new_custom_private_route(
                    &[CRYPTO_KIND_VLD0],
                    Stability::Reliable,
                    Sequencing::PreferOrdered,
                )
                .await
            {
                Ok(route_blob) => {
                    let route_id = route_blob.route_id.clone();
                    info!(
                        "Created Veilid route for MPC Party {}: {} (attempt {})",
                        self.party_id,
                        route_id,
                        attempt + 1
                    );
                    self.my_route_id = Some(route_id.clone());
                    self.my_route_blob = Some(route_blob);
                    return Ok(route_id);
                }
                Err(e) => {
                    last_err = e.to_string();
                }
            }
        }

        Err(anyhow::anyhow!(
            "Failed to create route after 6 attempts: {last_err}"
        ))
    }

    /// Broadcast this party's route announcement to all peers
    pub async fn broadcast_route_announcement(
        &self,
        listing_key: &RecordKey,
        my_pubkey: &PublicKey,
    ) -> Result<()> {
        let route_blob = self
            .my_route_blob
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Route not created yet"))?;

        let route_id = self
            .my_route_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Route ID not set (internal error)"))?;
        info!(
            "Broadcasting MPC route {} for party {}",
            route_id, my_pubkey
        );

        let announcement = AuctionMessage::mpc_route_announcement(
            listing_key.clone(),
            my_pubkey.clone(),
            route_blob.clone(),
        );

        let data = announcement.to_bytes()?;

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
                    .app_message(Target::NodeId(node_id.clone()), data.clone())
                    .await
                {
                    Ok(()) => {
                        debug!("Sent route announcement to peer {}", node_id);
                        sent_count += 1;
                        break;
                    }
                    Err(e) => debug!("Failed to send to {}: {}", node_id, e),
                }
            }
        }

        info!("Broadcasted route to {} peers", sent_count);
        Ok(())
    }

    /// Register a route announcement received from another party
    pub async fn register_route_announcement(
        &self,
        party_pubkey: PublicKey,
        route_blob: RouteBlob,
    ) -> Result<()> {
        let mut routes = self.received_routes.lock().await;

        if routes.contains_key(&party_pubkey) {
            debug!(
                "Already have route for party {}, ignoring duplicate",
                party_pubkey
            );
            return Ok(());
        }

        // Import the remote private route so we can send to it
        let _ = self
            .api
            .import_remote_private_route(route_blob.blob.clone())
            .map_err(|e| anyhow::anyhow!("Failed to import remote route: {e}"))?;

        let route_id = route_blob.route_id.clone();

        routes.insert(party_pubkey.clone(), route_id.clone());
        drop(routes);
        info!(
            "Registered and imported route for party {}: {}",
            party_pubkey, route_id
        );
        Ok(())
    }

    /// Assemble party routes from received announcements
    /// Maps PublicKeys to party IDs based on sorted bidder order
    pub async fn assemble_party_routes(
        &self,
        sorted_bidders: &[PublicKey],
    ) -> Result<HashMap<usize, RouteId>> {
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
}
