//! MPC result type for auction computations.

/// Result of an MPC auction computation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MpcResult {
    /// Whether this party won the auction.
    pub is_winner: bool,
    /// The party ID of this participant.
    pub party_id: usize,
    /// Total number of parties in the computation.
    pub num_parties: usize,
    /// Winning bid value (only available to party 0 / seller).
    pub winner_bid: Option<u64>,
    /// Winner's party ID (only available to party 0 / seller).
    pub winner_party_id: Option<usize>,
}
