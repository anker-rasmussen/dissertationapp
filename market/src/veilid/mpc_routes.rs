use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{info, debug, warn};
use veilid_core::{VeilidAPI, RouteId, KeyPair, RecordKey, DHTSchema, CRYPTO_KIND_VLD0, Stability, Sequencing};

use super::dht::DHTOperations;

// Hardcoded DHT keys for each MPC party in devnet
// Party 0 (Market Node 5)
const PARTY_0_KEYPAIR: &str =
    "VLD0:mJ8KQxNbT3wD9kYvP2lX6oZ1rS4uWt5xN7cV8bM9fH0:qL5pR2tY6uW8xZ0cV3bN4mJ7kQ9sT1wX5yA8dF0gH2j";
const PARTY_0_RECORD_KEY: &str =
    "VLD0:A1B2C3D4E5F6G7H8I9J0K1L2M3N4O5P6Q7R8S9T0U1V2";

// Party 1 (Market Node 6)
const PARTY_1_KEYPAIR: &str =
    "VLD0:xY9zW8vU7tS6rQ5pO4nM3lK2jH1gF0eD9cB8aZ7yX6w:V5u4T3s2R1q0P9o8N7m6L5k4J3h2G1f0E9d8C7b6A5z";
const PARTY_1_RECORD_KEY: &str =
    "VLD0:W2X3Y4Z5A6B7C8D9E0F1G2H3I4J5K6L7M8N9O0P1Q2R3";

// Party 2 (Market Node 7)
const PARTY_2_KEYPAIR: &str =
    "VLD0:F0g1H2i3J4k5L6m7N8o9P0q1R2s3T4u5V6w7X8y9Z0a:B1c2D3e4F5g6H7i8J9k0L1m2N3o4P5q6R7s8T9u0V1w";
const PARTY_2_RECORD_KEY: &str =
    "VLD0:S3T4U5V6W7X8Y9Z0A1B2C3D4E5F6G7H8I9J0K1L2M3N4";

/// MPC party route information for discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MpcPartyInfo {
    /// Party ID (0, 1, 2)
    pub party_id: usize,
    /// Veilid route for this party
    pub route_id: String,
    /// Node ID for verification
    pub node_id: String,
}

/// Manages Veilid route exchange for MPC parties
pub struct MpcRouteManager {
    api: VeilidAPI,
    dht: DHTOperations,
    party_id: usize,
    my_route_id: Option<RouteId>,
}

impl MpcRouteManager {
    pub fn new(api: VeilidAPI, dht: DHTOperations, party_id: usize) -> Self {
        Self {
            api,
            dht,
            party_id,
            my_route_id: None,
        }
    }

    /// Get the hardcoded keypair for a specific party
    fn get_party_keypair(party_id: usize) -> Result<KeyPair> {
        let keypair_str = match party_id {
            0 => PARTY_0_KEYPAIR,
            1 => PARTY_1_KEYPAIR,
            2 => PARTY_2_KEYPAIR,
            _ => return Err(anyhow::anyhow!("Invalid party ID: {}", party_id)),
        };

        KeyPair::try_from(keypair_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse party {} keypair: {}", party_id, e))
    }

    /// Get the hardcoded record key for a specific party
    fn get_party_record_key(party_id: usize) -> Result<RecordKey> {
        let key_str = match party_id {
            0 => PARTY_0_RECORD_KEY,
            1 => PARTY_1_RECORD_KEY,
            2 => PARTY_2_RECORD_KEY,
            _ => return Err(anyhow::anyhow!("Invalid party ID: {}", party_id)),
        };

        RecordKey::try_from(key_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse party {} record key: {}", party_id, e))
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

        let route_id = route_blob.route_id;
        info!("Created Veilid route for MPC Party {}: {}", self.party_id, route_id);
        self.my_route_id = Some(route_id.clone());
        Ok(route_id)
    }

    /// Publish this party's route to the DHT
    /// For n-party auctions, routes are exchanged directly, not via DHT
    pub async fn publish_route(&self) -> Result<()> {
        // In the n-party system, routes are exchanged via AppMessage
        // This function is a no-op for now
        debug!("Route publishing skipped - will exchange routes directly");
        Ok(())
    }

    /// Fetch routes for all other parties
    pub async fn fetch_party_routes(&self, num_parties: usize) -> Result<HashMap<usize, RouteId>> {
        // In the n-party system, routes would be exchanged via AppMessage
        // For now, return empty routes (allows single-party MPC to proceed)
        debug!("Route fetching skipped for {}-party auction - route exchange not yet implemented", num_parties);
        Ok(HashMap::new())
    }

    /// Get my route ID
    pub fn get_my_route(&self) -> Option<&RouteId> {
        self.my_route_id.as_ref()
    }
}
