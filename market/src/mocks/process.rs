//! Shared bid registry for multi-party MPC simulation in tests.

use std::collections::HashMap;
use tokio::sync::RwLock;

/// Shared bid registry for multi-party MPC simulation.
///
/// This allows multiple test parties to share bid information
/// so winner determination works correctly across all parties.
#[derive(Debug)]
pub struct SharedBidRegistry {
    /// Map<party_id, (bid_value, timestamp)>
    bids: RwLock<HashMap<usize, (u64, u64)>>,
    /// Counter for timestamps (used for tie-breaking)
    timestamp_counter: std::sync::atomic::AtomicU64,
}

impl SharedBidRegistry {
    /// Create a new shared bid registry.
    pub fn new() -> Self {
        Self {
            bids: RwLock::new(HashMap::new()),
            timestamp_counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Register a bid from a party.
    pub async fn register_bid(&self, party_id: usize, bid_value: u64) {
        let timestamp = self
            .timestamp_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.bids
            .write()
            .await
            .insert(party_id, (bid_value, timestamp));
    }

    /// Register a bid with an explicit timestamp (for tie-break testing).
    pub async fn register_bid_with_timestamp(
        &self,
        party_id: usize,
        bid_value: u64,
        timestamp: u64,
    ) {
        self.bids
            .write()
            .await
            .insert(party_id, (bid_value, timestamp));
    }

    /// Calculate the winner based on highest bid.
    /// Ties are broken by timestamp (earliest wins).
    pub async fn calculate_winner(&self) -> Option<usize> {
        let bids = self.bids.read().await;
        if bids.is_empty() {
            return None;
        }

        let mut winner: Option<(usize, u64, u64)> = None; // (party_id, bid, timestamp)

        for (&party_id, &(bid, timestamp)) in bids.iter() {
            match winner {
                None => winner = Some((party_id, bid, timestamp)),
                Some((_, best_bid, best_ts)) => {
                    // Higher bid wins, or earlier timestamp if tied
                    if bid > best_bid || (bid == best_bid && timestamp < best_ts) {
                        winner = Some((party_id, bid, timestamp));
                    }
                }
            }
        }
        drop(bids);

        winner.map(|(party_id, _, _)| party_id)
    }

    /// Calculate the winner and return both party ID and winning bid.
    pub async fn calculate_winner_with_bid(&self) -> Option<(usize, u64)> {
        let bids = self.bids.read().await;
        if bids.is_empty() {
            return None;
        }

        let mut winner: Option<(usize, u64, u64)> = None;

        for (&party_id, &(bid, timestamp)) in bids.iter() {
            match winner {
                None => winner = Some((party_id, bid, timestamp)),
                Some((_, best_bid, best_ts)) => {
                    if bid > best_bid || (bid == best_bid && timestamp < best_ts) {
                        winner = Some((party_id, bid, timestamp));
                    }
                }
            }
        }
        drop(bids);

        winner.map(|(party_id, bid, _)| (party_id, bid))
    }

    /// Get the winning bid value (highest bid).
    pub async fn get_winning_bid(&self) -> Option<u64> {
        self.calculate_winner_with_bid().await.map(|(_, bid)| bid)
    }

    /// Get all registered bids.
    pub async fn get_bids(&self) -> HashMap<usize, (u64, u64)> {
        self.bids.read().await.clone()
    }

    /// Clear all bids.
    pub async fn clear(&self) {
        self.bids.write().await.clear();
    }
}

impl Default for SharedBidRegistry {
    fn default() -> Self {
        Self::new()
    }
}
