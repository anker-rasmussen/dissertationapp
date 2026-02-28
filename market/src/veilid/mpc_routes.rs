use ed25519_dalek::SigningKey;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use veilid_core::{
    KeyPair, PublicKey, RecordKey, RouteBlob, RouteId, SafetySelection, SafetySpec, Sequencing,
    Stability, Target, VeilidAPI, CRYPTO_KIND_VLD0,
};

use super::bid_announcement::{AuctionMessage, MpcRouteEntry};
use super::dht::{DHTOperations, OwnedDHTRecord};
use crate::config::{bid_subkeys, now_unix};
use crate::error::{MarketError, MarketResult};

/// Manages Veilid private route lifecycle for MPC parties.
///
/// Each auction participant creates a private route for receiving MPC tunnel traffic,
/// exchanges route blobs with peers, and tracks readiness signals. Routes are
/// self-imported after creation (required by Veilid for deliverability) and
/// re-imported before tunnel proxy use to refresh the 5-minute expiry timer.
pub struct MpcRouteManager {
    api: VeilidAPI,
    party_id: usize,
    my_route_id: Option<RouteId>,
    my_route_blob: Option<RouteBlob>, // Route blob for sharing with other parties
    pub(crate) received_routes: Arc<Mutex<HashMap<PublicKey, RouteId>>>,
    /// Raw route blobs keyed by party pubkey — kept for re-import to refresh
    /// the 5-minute expiry timer before tunnel proxy use.
    received_blobs: Arc<Mutex<HashMap<PublicKey, Vec<u8>>>>,
    /// Maps party pubkey → (num_parties, timestamp) for recency filtering.
    pub(crate) ready_parties: Arc<Mutex<HashMap<PublicKey, (u32, u64)>>>,
}

impl MpcRouteManager {
    /// Create a new route manager for the given party.
    pub fn new(api: VeilidAPI, party_id: usize) -> Self {
        Self {
            api,
            party_id,
            my_route_id: None,
            my_route_blob: None,
            received_routes: Arc::new(Mutex::new(HashMap::new())),
            received_blobs: Arc::new(Mutex::new(HashMap::new())),
            ready_parties: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a private route for this party
    pub async fn create_route(&mut self) -> MarketResult<RouteId> {
        if let Some(route) = &self.my_route_id {
            return Ok(route.clone());
        }

        // Create a private route for receiving MPC traffic.
        // LowLatency: less restrictive relay selection in small devnets
        // (Reliable constrains which nodes can serve as hops).
        let route_blob = self
            .api
            .new_custom_private_route(
                &[CRYPTO_KIND_VLD0],
                Stability::LowLatency,
                Sequencing::PreferOrdered,
            )
            .await
            .map_err(|e| MarketError::Network(format!("Failed to create route: {e}")))?;

        let route_id = route_blob.route_id.clone();
        let blob_bytes = route_blob.blob.clone();

        // Self-import the route blob so Veilid marks it as deliverable.
        // Without this, only one of N created routes actually works.
        // See: https://gitlab.com/nicator/veilid — confirmed by Veilid core team.
        let _self_route = self
            .api
            .import_remote_private_route(blob_bytes)
            .map_err(|e| MarketError::Network(format!("Failed to self-import MPC route: {e}")))?;

        info!(
            "Created Veilid route for MPC Party {}: {}",
            self.party_id, route_id
        );

        self.my_route_id = Some(route_id.clone());
        self.my_route_blob = Some(route_blob);
        Ok(route_id)
    }

    /// Reset route state for a retry attempt.
    ///
    /// Clears stale route IDs, blobs, and own route so that the next
    /// `exchange_mpc_routes` call starts with fresh routes.  Ready signals
    /// are preserved to avoid losing signals from parties that already
    /// passed the barrier (see race condition comment in Phase 2).
    pub async fn reset_routes(&mut self) {
        if let Some(route_id) = self.my_route_id.take() {
            let _ = self.api.release_private_route(route_id);
        }
        self.my_route_blob = None;
        self.received_routes.lock().await.clear();
        self.received_blobs.lock().await.clear();
        info!(
            "Route manager reset for retry (ready signals preserved: {})",
            self.ready_parties.lock().await.len()
        );
    }

    /// Reuse an existing route (e.g., the broadcast route) for MPC traffic.
    ///
    /// The broadcast route is maintained by the coordinator's keepalive task,
    /// so it stays alive much longer than a freshly created MPC route.
    pub fn use_existing_route(&mut self, route_id: RouteId, blob: Vec<u8>) {
        info!(
            "Using existing (broadcast) route for MPC Party {}: {}",
            self.party_id, route_id
        );
        self.my_route_id = Some(route_id.clone());
        self.my_route_blob = Some(RouteBlob { route_id, blob });
    }

    /// Import each peer's route blob, send `data` via `app_message`, then
    /// release the imported route.  Returns the number of successful sends.
    async fn broadcast_via_peer_routes(
        &self,
        data: &[u8],
        peer_route_blobs: &[(String, Vec<u8>)],
        label: &str,
    ) -> MarketResult<usize> {
        let routing_context = self
            .api
            .routing_context()?
            .with_safety(SafetySelection::Safe(SafetySpec {
                preferred_route: None,
                hop_count: 1,
                stability: Stability::LowLatency,
                sequencing: Sequencing::PreferOrdered,
            }))?;

        let mut sent_count = 0;
        for (node_id, blob) in peer_route_blobs {
            match self.api.import_remote_private_route(blob.clone()) {
                Ok(imported_route) => {
                    match routing_context
                        .app_message(Target::RouteId(imported_route.clone()), data.to_vec())
                        .await
                    {
                        Ok(()) => {
                            debug!("Sent {} to peer {}", label, node_id);
                            sent_count += 1;
                        }
                        Err(e) => warn!("Failed to send {} to {}: {}", label, node_id, e),
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(
                        crate::config::MPC_ROUTE_RELEASE_DELAY_MS,
                    ))
                    .await;
                    let _ = self.api.release_private_route(imported_route);
                }
                Err(e) => {
                    debug!("Failed to import route for {}: {}", node_id, e);
                }
            }
        }
        Ok(sent_count)
    }

    /// Broadcast this party's route announcement to peers using their
    /// private route blobs (`Target::NodeId` requires unsafe routing, which
    /// bypasses Veilid's privacy guarantees).
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
            now_unix(),
        );

        let data = announcement.to_signed_bytes(signing_key)?;
        let sent = self
            .broadcast_via_peer_routes(&data, peer_route_blobs, "route announcement")
            .await?;
        info!("Broadcasted route to {} peers", sent);
        Ok(())
    }

    /// Register a route announcement received from another party.
    ///
    /// Always re-imports the route blob so Veilid refreshes its internal
    /// relay state — this recovers from transient `dead_remote_routes` events
    /// that would otherwise leave a stale handle in `received_routes`.
    /// Returns `true` if this is a new or changed route, `false` if the same
    /// route was already registered (re-import only refreshed Veilid state).
    pub async fn register_route_announcement(
        &self,
        party_pubkey: PublicKey,
        route_blob: RouteBlob,
    ) -> MarketResult<bool> {
        // Remote route import goes through Veilid's LRU cache (not the
        // hop_cache for allocated routes) and is safe to call repeatedly.
        let imported_route = self
            .api
            .import_remote_private_route(route_blob.blob.clone())
            .map_err(|e| MarketError::Network(format!("Failed to import remote route: {e}")))?;

        let mut routes = self.received_routes.lock().await;

        if let Some(existing) = routes.get(&party_pubkey) {
            if *existing == imported_route {
                return Ok(false);
            }
            info!(
                "Replacing route for party {} (old: {}, new: {})",
                party_pubkey, existing, imported_route
            );
        } else {
            info!(
                "Registered route for party {}: {}",
                party_pubkey, imported_route
            );
        }

        routes.insert(party_pubkey.clone(), imported_route);
        drop(routes);

        // Store the raw blob for later re-import (refreshes 5-min expiry timer)
        self.received_blobs
            .lock()
            .await
            .insert(party_pubkey, route_blob.blob);

        Ok(true)
    }

    /// Re-import all stored route blobs to refresh Veilid's 5-minute
    /// LRU cache.  Call this right before creating the tunnel proxy
    /// to ensure routes are fresh.  Returns the number refreshed.
    ///
    /// Remote route import is idempotent (goes through Veilid's LRU
    /// cache, not the hop_cache for allocated routes).
    pub async fn refresh_routes(&self) -> usize {
        let snapshot: Vec<_> = {
            let blobs = self.received_blobs.lock().await;
            blobs
                .iter()
                .map(|(pk, b)| (pk.clone(), b.clone()))
                .collect()
        };
        let mut count = 0;
        let mut routes = self.received_routes.lock().await;
        for (pubkey, blob) in &snapshot {
            match self.api.import_remote_private_route(blob.clone()) {
                Ok(new_route) => {
                    routes.insert(pubkey.clone(), new_route);
                    count += 1;
                }
                Err(e) => {
                    debug!("Failed to refresh route for party {}: {}", pubkey, e);
                }
            }
        }
        drop(routes);
        if count > 0 {
            info!("Refreshed {} route handles", count);
        }
        count
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

    /// Assemble raw route blobs keyed by party ID.
    ///
    /// Used by `MpcTunnelProxy` to re-import routes on retry.
    pub async fn assemble_party_blobs(
        &self,
        sorted_bidders: &[PublicKey],
    ) -> HashMap<usize, Vec<u8>> {
        let blobs = self.received_blobs.lock().await;
        let mut result = HashMap::new();
        for (party_id, pubkey) in sorted_bidders.iter().enumerate() {
            if let Some(blob) = blobs.get(pubkey) {
                result.insert(party_id, blob.clone());
            }
        }
        result
    }

    /// Force-create a fresh route, replacing any existing one.
    /// Also updates `received_routes` so the new route is visible
    /// to `assemble_party_routes`.  Returns the new route blob for
    /// broadcasting to peers.
    pub async fn refresh_own_route(&mut self, my_pubkey: &PublicKey) -> MarketResult<RouteBlob> {
        // Release old route
        if let Some(old) = self.my_route_id.take() {
            let _ = self.api.release_private_route(old);
        }
        self.my_route_blob = None;

        let new_id = self.create_route().await?;
        info!("Refreshed own MPC route: {}", new_id);

        let blob = self
            .my_route_blob
            .as_ref()
            .ok_or_else(|| MarketError::InvalidState("Route blob missing after create".into()))?
            .clone();

        // Update received_routes for self
        let mut routes = self.received_routes.lock().await;
        routes.insert(my_pubkey.clone(), new_id);
        drop(routes);

        // Update received_blobs for self
        self.received_blobs
            .lock()
            .await
            .insert(my_pubkey.clone(), blob.blob.clone());

        Ok(blob)
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
        num_parties: u32,
        peer_route_blobs: &[(String, Vec<u8>)],
        signing_key: &SigningKey,
    ) -> MarketResult<()> {
        info!(
            "Broadcasting MpcReady for party {} on listing {} (num_parties={})",
            my_pubkey, listing_key, num_parties
        );

        let ready_msg = AuctionMessage::mpc_ready(
            listing_key.clone(),
            my_pubkey.clone(),
            num_parties,
            now_unix(),
        );
        let data = ready_msg.to_signed_bytes(signing_key)?;
        let sent = self
            .broadcast_via_peer_routes(&data, peer_route_blobs, "MpcReady")
            .await?;
        info!("Broadcasted MpcReady to {} peers", sent);
        Ok(())
    }

    /// Send MpcReady directly via already-imported MPC routes using `app_call`
    /// for confirmed delivery.
    ///
    /// Unlike `broadcast_mpc_ready` which imports broadcast route blobs
    /// (potentially stale), this sends through the MPC routes collected
    /// during Phase 1 — these are known-good since route collection
    /// succeeded.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn send_ready_via_mpc_routes(
        &self,
        listing_key: &RecordKey,
        my_pubkey: &PublicKey,
        num_parties: u32,
        signing_key: &SigningKey,
    ) -> MarketResult<usize> {
        let ready_msg = AuctionMessage::mpc_ready(
            listing_key.clone(),
            my_pubkey.clone(),
            num_parties,
            now_unix(),
        );
        let data = ready_msg.to_signed_bytes(signing_key)?;

        let routing_context = self
            .api
            .routing_context()?
            .with_safety(SafetySelection::Safe(SafetySpec {
                preferred_route: None,
                hop_count: 1,
                stability: Stability::LowLatency,
                sequencing: Sequencing::PreferOrdered,
            }))?;

        let routes = self.received_routes.lock().await;
        let mut sent = 0usize;
        for (pubkey, route_id) in routes.iter() {
            if pubkey == my_pubkey {
                continue; // Don't send to self
            }
            match routing_context
                .app_call(Target::RouteId(route_id.clone()), data.clone())
                .await
            {
                Ok(_reply) => sent += 1,
                Err(e) => warn!("Failed to send MpcReady via MPC route to {}: {}", pubkey, e),
            }
        }
        Ok(sent)
    }

    /// Register a party as ready with their expected party count and timestamp.
    /// Returns `true` if the party was newly inserted.
    pub async fn register_ready(
        &self,
        party_pubkey: PublicKey,
        num_parties: u32,
        timestamp: u64,
    ) -> bool {
        let mut map = self.ready_parties.lock().await;
        let is_new = !map.contains_key(&party_pubkey);
        debug!(
            "register_ready: party={} num_parties={} ts={} is_new={} total_in_map={}",
            party_pubkey,
            num_parties,
            timestamp,
            is_new,
            map.len() + usize::from(is_new),
        );
        map.insert(party_pubkey, (num_parties, timestamp));
        is_new
    }

    /// Total number of parties that have signalled readiness.
    pub async fn ready_count(&self) -> usize {
        self.ready_parties.lock().await.len()
    }

    /// Handle dead remote routes reported by `VeilidUpdate::RouteChange`.
    ///
    /// For each dead `RouteId`, finds which party pubkey maps to it in
    /// `received_routes`, removes the stale handle, re-imports from the
    /// stored blob in `received_blobs`, and inserts the fresh handle.
    /// Handle dead remote routes by re-importing their blobs.
    ///
    /// IMPORTANT: Does NOT call `release_private_route` before re-import.
    /// Releasing would invalidate the RouteId globally in Veilid's store,
    /// breaking any concurrent senders (e.g. the tunnel proxy) using the
    /// same RouteId.  Re-import is safe and idempotent — it refreshes
    /// the LRU cache entry without disrupting active senders.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn handle_dead_remote_routes(&self, dead: &[RouteId]) {
        if dead.is_empty() {
            return;
        }

        let mut routes = self.received_routes.lock().await;
        let blobs = self.received_blobs.lock().await;

        for dead_route in dead {
            // Find which pubkey owns this dead route
            let pubkey = routes
                .iter()
                .find(|(_, rid)| *rid == dead_route)
                .map(|(pk, _)| pk.clone());

            let Some(pubkey) = pubkey else {
                continue;
            };

            // Re-import without releasing — idempotent for remote routes
            if let Some(blob) = blobs.get(&pubkey) {
                match self.api.import_remote_private_route(blob.clone()) {
                    Ok(new_route) => {
                        debug!(
                            "Re-imported dead remote route for party {}: {}",
                            pubkey, new_route
                        );
                        routes.insert(pubkey, new_route);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to re-import route for party {} after dead_remote_routes: {}",
                            pubkey, e
                        );
                    }
                }
            }
        }
    }

    /// Check whether we already have a route for the given party.
    pub async fn has_route_for(&self, pubkey: &PublicKey) -> bool {
        self.received_routes.lock().await.contains_key(pubkey)
    }

    /// Register a route announcement from raw blob bytes (DHT fallback path).
    ///
    /// Same as `register_route_announcement` but takes raw bytes instead of
    /// a `RouteBlob`. Constructs a `RouteBlob` by importing to get the `RouteId`.
    pub async fn register_route_announcement_from_blob(
        &self,
        party_pubkey: PublicKey,
        blob_bytes: &[u8],
    ) -> MarketResult<bool> {
        let imported_route = self
            .api
            .import_remote_private_route(blob_bytes.to_vec())
            .map_err(|e| MarketError::Network(format!("Failed to import DHT route blob: {e}")))?;

        let route_blob = RouteBlob {
            route_id: imported_route.clone(),
            blob: blob_bytes.to_vec(),
        };

        self.register_route_announcement(party_pubkey, route_blob)
            .await
    }

    /// Write this party's MPC route blob to its bid record subkey 1.
    ///
    /// This provides a DHT-backed fallback for route exchange: if `app_message`
    /// announcements fail (stale broadcast routes), peers can read the route
    /// blob from DHT instead.
    pub async fn write_route_to_dht(
        dht: &DHTOperations,
        bid_record_key: &RecordKey,
        bid_owner: &KeyPair,
        route_blob: &[u8],
    ) -> MarketResult<()> {
        let record = OwnedDHTRecord {
            key: bid_record_key.clone(),
            owner: bid_owner.clone(),
        };
        let entry = MpcRouteEntry {
            route_blob: route_blob.to_vec(),
            timestamp: now_unix(),
        };
        let data = entry.to_bytes()?;
        dht.set_value_at_subkey(&record, bid_subkeys::MPC_ROUTE, data)
            .await
    }

    /// Read a party's MPC route blob from their bid record subkey 1.
    ///
    /// Returns `None` if the party hasn't published a route blob yet.
    pub async fn read_route_from_dht(
        dht: &DHTOperations,
        bid_record_key: &RecordKey,
    ) -> MarketResult<Option<Vec<u8>>> {
        match dht
            .get_value_at_subkey(bid_record_key, bid_subkeys::MPC_ROUTE, true)
            .await?
        {
            Some(data) => {
                let entry = MpcRouteEntry::from_bytes(&data)?;
                Ok(Some(entry.route_blob))
            }
            None => Ok(None),
        }
    }

    /// Handle dead own routes reported by `VeilidUpdate::RouteChange`.
    ///
    /// Checks both `dead_routes` and `dead_remote_routes` — Veilid sometimes
    /// reports a node's own allocated route in `dead_remote_routes` rather
    /// than `dead_routes` (confirmed by core team on Discord).
    ///
    /// If `my_route_id` is in either dead list, clears it so that
    /// `create_route` will allocate a fresh one.  Returns the new
    /// `RouteBlob` if a route was recreated (caller should re-broadcast).
    pub async fn handle_dead_own_route(
        &mut self,
        dead: &[RouteId],
        dead_remote: &[RouteId],
    ) -> Option<RouteBlob> {
        let my_route = self.my_route_id.as_ref()?;
        if !dead.contains(my_route) && !dead_remote.contains(my_route) {
            return None;
        }

        info!(
            "Own MPC route {} died — recreating for party {}",
            my_route, self.party_id
        );

        // Clear so create_route() will allocate a new one
        self.my_route_id = None;
        self.my_route_blob = None;

        match self.create_route().await {
            Ok(new_id) => {
                info!("Recreated own MPC route: {}", new_id);
                self.my_route_blob.clone()
            }
            Err(e) => {
                warn!("Failed to recreate own MPC route: {}", e);
                None
            }
        }
    }
}
