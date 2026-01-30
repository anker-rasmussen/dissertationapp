use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, debug, warn};
use veilid_core::{VeilidAPI, RouteId, RouteBlob, RecordKey, CRYPTO_KIND_VLD0, Stability, Sequencing, PublicKey, SafetySelection, Target};

use super::bid_announcement::AuctionMessage;
use super::dht::DHTOperations;

/// Manages Veilid route exchange for MPC parties
pub struct MpcRouteManager {
    api: VeilidAPI,
    _dht: DHTOperations,
    party_id: usize,
    my_route_id: Option<RouteId>,
    my_route_blob: Option<RouteBlob>,  // Route blob for sharing with other parties
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

    /// Create a private route for this party
    pub async fn create_route(&mut self) -> Result<RouteId> {
        if let Some(route) = &self.my_route_id {
            return Ok(route.clone());
        }

        // Create a private route for receiving MPC traffic
        // Use reliable sequencing for MPC (order matters)
        let route_blob = self.api
            .new_custom_private_route(
                &[CRYPTO_KIND_VLD0],
                Stability::Reliable,
                Sequencing::PreferOrdered,
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create route: {}", e))?;

        let route_id = route_blob.route_id.clone();

        info!("Created Veilid route for MPC Party {}: {}", self.party_id, route_id);

        self.my_route_id = Some(route_id.clone());
        self.my_route_blob = Some(route_blob);
        Ok(route_id)
    }

    /// Broadcast this party's route announcement to all peers
    pub async fn broadcast_route_announcement(
        &self,
        listing_key: &RecordKey,
        my_pubkey: &PublicKey,
    ) -> Result<()> {
        let route_blob = self.my_route_blob.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Route not created yet"))?;

        let route_id = self.my_route_id.as_ref().unwrap();
        info!("Broadcasting MPC route {} for party {}", route_id, my_pubkey);

        let announcement = AuctionMessage::mpc_route_announcement(
            listing_key.clone(),
            my_pubkey.clone(),
            route_blob.clone(),
        );

        let data = announcement.to_bytes()?;

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
            debug!("Already have route for party {}, ignoring duplicate", party_pubkey);
            return Ok(());
        }

        // Import the remote private route so we can send to it
        let _ = self.api.import_remote_private_route(route_blob.blob.clone())
            .map_err(|e| anyhow::anyhow!("Failed to import remote route: {}", e))?;

        let route_id = route_blob.route_id.clone();

        routes.insert(party_pubkey.clone(), route_id.clone());
        info!("Registered and imported route for party {}: {}", party_pubkey, route_id);
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

        info!("Assembled {} routes from {} bidders",
              routes_by_party_id.len(), sorted_bidders.len());

        Ok(routes_by_party_id)
    }

    /// Get my route ID
    pub fn get_my_route(&self) -> Option<&RouteId> {
        self.my_route_id.as_ref()
    }

    /// Get my route blob for sharing
    pub fn get_my_route_blob(&self) -> Option<&RouteBlob> {
        self.my_route_blob.as_ref()
    }
}
