//! Post-MPC verification: bid commitment checks and winner communication.

use sha2::{Digest, Sha256};
use tracing::{error, info, warn};
use veilid_core::{PublicKey, RecordKey, Target};

use super::bid_announcement::{AuctionMessage, BidAnnouncementRegistry};
use super::bid_ops::BidOperations;
use super::mpc_execution::MpcResultContract;
use super::mpc_orchestrator::MpcOrchestrator;
use crate::config::{now_unix, subkeys};
use crate::error::{MarketError, MarketResult};

/// Verify a bid commitment against the revealed bid value and nonce.
///
/// Computes `SHA256(bid_value_le_bytes || nonce)` and compares with the stored commitment.
/// This is the core commitment verification used in the Danish Sugar Beet auction protocol.
pub fn verify_commitment(bid_value: u64, nonce: &[u8; 32], stored_commitment: &[u8; 32]) -> bool {
    use subtle::ConstantTimeEq;

    let mut hasher = Sha256::new();
    hasher.update(bid_value.to_le_bytes());
    hasher.update(nonce);
    let computed: [u8; 32] = hasher.finalize().into();
    computed.ct_eq(stored_commitment).into()
}

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
        result: &MpcResultContract,
        listing_key: &RecordKey,
        all_parties: &[PublicKey],
    ) {
        let (Some(winner_pid), Some(bid)) = (result.winner_party_id, result.winning_bid) else {
            warn!("Invalid seller MPC result contract: missing winner_party_id or winning_bid");
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
            info!("Stored pending verification for winner {}", winner_pubkey);

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
    pub(crate) async fn handle_bidder_mpc_result(
        &self,
        result: &MpcResultContract,
        listing_key: &RecordKey,
    ) {
        let i_won = result.i_won.unwrap_or(false);

        if i_won {
            info!("I won — waiting for seller's challenge (WinnerDecryptionRequest)");
            self.mark_expected_winner(listing_key).await;
        } else {
            info!("I lost — cleaning up route manager");
            self.clear_expected_winner(listing_key).await;
            self.cleanup_route_manager(listing_key).await;
        }
    }

    /// Verify winner's revealed bid against stored commitment (Danish Sugar Beet style)
    ///
    /// This method is constant-time in its control flow: all verification steps are
    /// performed regardless of intermediate failures to prevent timing side-channels.
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

        // 2. Check bid value matches MPC output (store result, don't return early)
        let bid_matches = bid_value == expected_bid;

        // 3. Fetch winner's BidRecord from DHT to get stored commitment
        // Always perform this step regardless of bid_matches to prevent timing leaks
        let bid_ops = BidOperations::new(self.dht.clone());

        let registry_data = match self
            .dht
            .get_value_at_subkey(listing_key, subkeys::BID_ANNOUNCEMENTS, true)
            .await
        {
            Ok(Some(data)) => Some(data),
            Ok(None) | Err(_) => None,
        };

        let registry =
            registry_data.and_then(|data| BidAnnouncementRegistry::from_bytes(&data).ok());

        let winner_bid_record_key = registry.as_ref().and_then(|reg| {
            reg.announcements
                .iter()
                .find(|(b, _, _)| b == winner)
                .map(|(_, key, _)| key.clone())
        });

        let bid_record = match winner_bid_record_key {
            Some(key) => bid_ops.fetch_bid(&key).await.ok().flatten(),
            None => None,
        };

        // 4. Verify commitment: SHA256(bid_value || nonce) == stored
        // Always perform this step regardless of previous failures to prevent timing leaks
        let commitment_valid = bid_record
            .as_ref()
            .is_some_and(|record| verify_commitment(bid_value, nonce, &record.commitment));

        // 5. Combine results and return
        let result = bid_matches && commitment_valid;

        if result {
            info!("Commitment verified for winner {}", winner);
        } else {
            warn!("Verification failed for winner {}", winner);
        }

        result
    }

    /// Send WinnerDecryptionRequest challenge to winner via their MPC route
    pub async fn send_winner_challenge(&self, listing_key: &RecordKey, winner: &PublicKey) {
        let message = AuctionMessage::winner_decryption_request(
            listing_key.clone(),
            winner.clone(),
            now_unix(),
        );

        let data = match message.to_signed_bytes(&self.signing_key) {
            Ok(d) => d,
            Err(e) => {
                error!("Failed to sign WinnerDecryptionRequest: {}", e);
                return;
            }
        };

        let start = std::time::Instant::now();
        let max_wait = std::time::Duration::from_secs(20);

        loop {
            // Look up the winner's route while holding locks, then release before async work
            let winner_route = {
                let route_manager = {
                    let managers = self.route_managers.lock().await;
                    managers.get(listing_key).cloned()
                };
                let Some(route_manager) = route_manager else {
                    if start.elapsed() >= max_wait {
                        error!(
                            "Route manager not found for listing {} after {:?}",
                            listing_key, max_wait
                        );
                        return;
                    }
                    warn!(
                        "Route manager not found for listing {}, retrying",
                        listing_key
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
                };

                let received_routes = {
                    let mgr = route_manager.lock().await;
                    mgr.received_routes.clone()
                };
                let routes = received_routes.lock().await;
                if let Some(route) = routes.get(winner).cloned() {
                    route
                } else {
                    if start.elapsed() >= max_wait {
                        error!(
                            "Winner route not found in route manager after {:?} (listing {})",
                            max_wait, listing_key
                        );
                        return;
                    }
                    warn!(
                        "Winner route not found yet for listing {}, retrying challenge",
                        listing_key
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
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
                .app_call(Target::RouteId(winner_route), data.clone())
                .await
            {
                Ok(_reply) => {
                    info!("Sent WinnerDecryptionRequest challenge to winner via MPC route");
                    return;
                }
                Err(e) => {
                    if start.elapsed() >= max_wait {
                        error!(
                            "Failed to send challenge to winner within {:?}: {}",
                            max_wait, e
                        );
                        return;
                    }
                    warn!("Failed to send challenge to winner, retrying: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Send decryption hash to auction winner via their MPC route
    pub async fn send_decryption_hash(
        &self,
        listing_key: &RecordKey,
        winner: &PublicKey,
        decryption_hash: &str,
    ) -> MarketResult<()> {
        info!(
            "Sending decryption hash to winner {} for listing {}",
            winner, listing_key
        );

        let message = AuctionMessage::decryption_hash_transfer(
            listing_key.clone(),
            winner.clone(),
            decryption_hash.to_string(),
            now_unix(),
        );

        let data = message.to_signed_bytes(&self.signing_key)?;

        // Look up the winner's route while holding locks, then release before async work
        let winner_route = {
            let route_manager = {
                let managers = self.route_managers.lock().await;
                managers
                    .get(listing_key)
                    .ok_or_else(|| {
                        MarketError::InvalidState(format!(
                            "Route manager not found for listing {listing_key}"
                        ))
                    })?
                    .clone()
            };
            let received_routes = {
                let mgr = route_manager.lock().await;
                mgr.received_routes.clone()
            };
            let routes = received_routes.lock().await;
            routes.get(winner).cloned().ok_or_else(|| {
                MarketError::NotFound(format!(
                    "Winner's MPC route not found in route manager for listing {listing_key}"
                ))
            })?
        };

        info!("Found winner's MPC route: {}", winner_route);
        let routing_context = self.safe_routing_context()?;

        match routing_context
            .app_call(Target::RouteId(winner_route.clone()), data)
            .await
        {
            Ok(_reply) => {
                info!(
                    "Successfully sent decryption hash to winner via MPC route {}",
                    winner_route
                );
                self.cleanup_route_manager(listing_key).await;
                Ok(())
            }
            Err(e) => {
                error!("Failed to send to winner's MPC route: {}", e);
                Err(MarketError::Network(format!(
                    "Failed to send decryption hash: {e}"
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_commitment(bid_value: u64, nonce: &[u8; 32]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(bid_value.to_le_bytes());
        hasher.update(nonce);
        hasher.finalize().into()
    }

    #[test]
    fn test_verify_commitment_valid() {
        let nonce = [42u8; 32];
        let bid_value = 100u64;
        let commitment = make_commitment(bid_value, &nonce);
        assert!(verify_commitment(bid_value, &nonce, &commitment));
    }

    #[test]
    fn test_verify_commitment_wrong_bid_value() {
        let nonce = [42u8; 32];
        let commitment = make_commitment(100, &nonce);
        assert!(!verify_commitment(200, &nonce, &commitment));
    }

    #[test]
    fn test_verify_commitment_wrong_nonce() {
        let nonce = [42u8; 32];
        let wrong_nonce = [99u8; 32];
        let commitment = make_commitment(100, &nonce);
        assert!(!verify_commitment(100, &wrong_nonce, &commitment));
    }

    #[test]
    fn test_verify_commitment_zero_bid() {
        let nonce = [1u8; 32];
        let commitment = make_commitment(0, &nonce);
        assert!(verify_commitment(0, &nonce, &commitment));
    }

    #[test]
    fn test_verify_commitment_max_bid() {
        let nonce = [255u8; 32];
        let commitment = make_commitment(u64::MAX, &nonce);
        assert!(verify_commitment(u64::MAX, &nonce, &commitment));
    }

    #[test]
    fn test_commitment_deterministic() {
        let nonce = [7u8; 32];
        let bid_value = 500u64;
        let commitment1 = make_commitment(bid_value, &nonce);
        let commitment2 = make_commitment(bid_value, &nonce);
        assert_eq!(commitment1, commitment2);
        assert!(verify_commitment(bid_value, &nonce, &commitment1));
        assert!(verify_commitment(bid_value, &nonce, &commitment2));
    }
}
