//! Post-MPC verification: bid commitment checks and winner communication.

use anyhow::Result;
use sha2::{Digest, Sha256};
use tracing::{error, info, warn};
use veilid_core::{PublicKey, RecordKey, Target};

use super::bid_announcement::{AuctionMessage, BidAnnouncementRegistry};
use super::bid_ops::BidOperations;
use super::mpc_execution::{parse_bidder_mpc_output, parse_seller_mpc_output};
use super::mpc_orchestrator::MpcOrchestrator;
use crate::config::subkeys;

/// Named struct replacing the tuple `(PublicKey, u64, Option<bool>)` for pending verifications.
#[derive(Debug, Clone)]
pub struct VerificationState {
    pub winner_pubkey: PublicKey,
    pub mpc_winning_bid: u64,
    pub verified: Option<bool>,
}

impl MpcOrchestrator {
    /// Handle seller (party 0) MPC result: parse output, store verification, send challenge.
    pub(crate) async fn handle_seller_mpc_result(
        &self,
        stdout: &str,
        listing_key: &RecordKey,
        all_parties: &[PublicKey],
    ) {
        let Some((winner_pid, bid)) = parse_seller_mpc_output(stdout) else {
            warn!("Could not parse winner/bid from MPC output");
            return;
        };

        if winner_pid == 0 {
            // Winner is the seller (reserve price was highest) = no sale
            info!("No bidder exceeded the reserve price — no sale");
            self.cleanup_route_manager(listing_key).await;
        } else if winner_pid < all_parties.len() {
            let winner_pubkey = all_parties[winner_pid].clone();
            let key = listing_key.clone();

            // Store pending verification entry
            self.pending_verifications.lock().await.insert(
                key,
                VerificationState {
                    winner_pubkey: winner_pubkey.clone(),
                    mpc_winning_bid: bid,
                    verified: None,
                },
            );
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
    }

    /// Handle bidder (party 1..N) MPC result: parse output, cleanup if lost.
    pub(crate) async fn handle_bidder_mpc_result(&self, stdout: &str, listing_key: &RecordKey) {
        let i_won = parse_bidder_mpc_output(stdout);

        if i_won {
            info!("I won — waiting for seller's challenge (WinnerDecryptionRequest)");
        } else {
            info!("I lost — cleaning up route manager");
            self.cleanup_route_manager(listing_key).await;
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
        // 1. Get pending verification to check expected winning bid
        let expected_bid = {
            let verifications = self.pending_verifications.lock().await;
            if let Some(state) = verifications.get(listing_key) {
                if &state.winner_pubkey != winner {
                    warn!(
                        "Winner mismatch: expected {}, got {}",
                        state.winner_pubkey, winner
                    );
                    return false;
                }
                state.mpc_winning_bid
            } else {
                warn!("No pending verification found for listing {}", listing_key);
                return false;
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

        let registry_data = match self
            .dht
            .get_value_at_subkey(listing_key, subkeys::BID_ANNOUNCEMENTS, true)
            .await
        {
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

        let winner_bid_record_key = if let Some((_, key, _)) =
            registry.announcements.iter().find(|(b, _, _)| b == winner)
        {
            key.clone()
        } else {
            warn!("Winner {} not found in bid registry", winner);
            return false;
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
            let route_manager = {
                let managers = self.route_managers.lock().await;
                if let Some(rm) = managers.get(listing_key) {
                    rm.clone()
                } else {
                    warn!("Route manager not found for listing {}", listing_key);
                    return;
                }
            };
            let received_routes = {
                let mgr = route_manager.lock().await;
                mgr.received_routes.clone()
            };
            let routes = received_routes.lock().await;
            if let Some(route) = routes.get(winner).cloned() {
                route
            } else {
                warn!("Winner's MPC route not found in route manager");
                return;
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
            Ok(()) => {
                info!("Sent WinnerDecryptionRequest challenge to winner via MPC route");
            }
            Err(e) => error!("Failed to send challenge to winner: {}", e),
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
            let route_manager = {
                let managers = self.route_managers.lock().await;
                managers
                    .get(listing_key)
                    .ok_or_else(|| {
                        anyhow::anyhow!("Route manager not found for listing {listing_key}")
                    })?
                    .clone()
            };
            let received_routes = {
                let mgr = route_manager.lock().await;
                mgr.received_routes.clone()
            };
            let routes = received_routes.lock().await;
            routes.get(winner).cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "Winner's MPC route not found in route manager for listing {listing_key}"
                )
            })?
        };

        info!("Found winner's MPC route: {}", winner_route);
        let routing_context = self.safe_routing_context()?;

        match routing_context
            .app_message(Target::RouteId(winner_route.clone()), data)
            .await
        {
            Ok(()) => {
                info!(
                    "Successfully sent decryption hash to winner via MPC route {}",
                    winner_route
                );
                self.cleanup_route_manager(listing_key).await;
                Ok(())
            }
            Err(e) => {
                error!("Failed to send to winner's MPC route: {}", e);
                Err(anyhow::anyhow!("Failed to send decryption hash: {e}"))
            }
        }
    }
}
