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
    /// Ties are broken by timestamp (earliest wins), then by lowest party_id.
    pub async fn calculate_winner(&self) -> Option<usize> {
        let bids = self.bids.read().await;
        if bids.is_empty() {
            return None;
        }

        let mut winner: Option<(usize, u64, u64)> = None; // (party_id, bid, timestamp)

        for (&party_id, &(bid, timestamp)) in bids.iter() {
            match winner {
                None => winner = Some((party_id, bid, timestamp)),
                Some((best_pid, best_bid, best_ts)) => {
                    // Higher bid wins, earlier timestamp breaks ties,
                    // lowest party_id as final deterministic tiebreak.
                    if bid > best_bid
                        || (bid == best_bid && timestamp < best_ts)
                        || (bid == best_bid && timestamp == best_ts && party_id < best_pid)
                    {
                        winner = Some((party_id, bid, timestamp));
                    }
                }
            }
        }
        drop(bids);

        winner.map(|(party_id, _, _)| party_id)
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
