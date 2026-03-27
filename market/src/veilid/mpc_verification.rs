//! Post-MPC verification: bid commitment checks and winner communication.

use sha2::{Digest, Sha256};
use tracing::{error, info, warn};
use veilid_core::{PublicKey, RecordKey};

use super::bid_announcement::{AuctionMessage, BidAnnouncementRegistry};
use super::bid_ops::BidOperations;
use super::mpc_execution::MpcResultContract;
use super::mpc_orchestrator::{AuctionPhase, MpcOrchestrator};
use crate::config::{now_unix, subkeys};
use crate::error::MarketResult;

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
pub(crate) struct VerificationState {
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
            if let Err(e) = self
                .send_winner_challenge(listing_key, &winner_pubkey)
                .await
            {
                error!("Failed to send winner challenge: {}", e);
            }
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
            self.set_auction_phase(listing_key, AuctionPhase::Completed)
                .await;
        }
    }

    /// Verify winner's revealed bid against stored commitment (Danish Sugar Beet style)
    ///
    /// Steps 1-2 (local data lookup) may return early on mismatches.  Steps 3-4
    /// (DHT fetch + commitment check) always execute together when step 2 passes.
    /// The early returns are not a remote timing side-channel: Veilid `app_call`
    /// round-trip variance (~50-2000ms) dominates local verification time (~µs).
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

    /// Send WinnerDecryptionRequest challenge to winner via their MPC route.
    pub async fn send_winner_challenge(
        &self,
        listing_key: &RecordKey,
        winner: &PublicKey,
    ) -> MarketResult<()> {
        let message = AuctionMessage::winner_decryption_request(
            listing_key.clone(),
            winner.clone(),
            now_unix(),
        );

        let data = message.to_signed_bytes(&self.signing_key)?;

        self.send_via_mpc_route(
            listing_key,
            winner,
            data,
            std::time::Duration::from_secs(crate::config::WINNER_REVEAL_TIMEOUT_SECS),
            "WinnerDecryptionRequest",
        )
        .await
    }

    /// Send decryption hash to auction winner via their MPC route.
    ///
    /// Uses the same retry loop as `send_winner_challenge` — if the route
    /// dies between challenge and key delivery the winner would prove they
    /// won but never receive the decryption key.
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

        self.send_via_mpc_route(
            listing_key,
            winner,
            data,
            std::time::Duration::from_secs(crate::config::WINNER_REVEAL_TIMEOUT_SECS),
            "DecryptionHashTransfer",
        )
        .await?;

        self.cleanup_route_manager(listing_key).await;
        Ok(())
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
