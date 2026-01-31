//! MPC process runner abstraction for testable MPC operations.

use anyhow::Result;
use async_trait::async_trait;

/// Result of an MPC auction computation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MpcResult {
    /// Whether this party won the auction.
    pub is_winner: bool,
    /// The party ID of this participant.
    pub party_id: usize,
    /// Total number of parties in the computation.
    pub num_parties: usize,
}

/// Abstraction over MPC process execution.
///
/// This trait enables testing of MPC-dependent code without requiring
/// actual MP-SPDZ processes.
#[async_trait]
pub trait MpcRunner: Send + Sync + Clone {
    /// Compile an MPC program for the specified number of parties.
    ///
    /// # Arguments
    /// * `program` - Name of the MPC program to compile
    /// * `num_parties` - Number of parties participating
    async fn compile(&self, program: &str, num_parties: usize) -> Result<()>;

    /// Execute an MPC auction computation.
    ///
    /// # Arguments
    /// * `party_id` - This party's ID (0-indexed)
    /// * `num_parties` - Total number of parties
    /// * `input_value` - This party's private bid value
    ///
    /// # Returns
    /// The result of the MPC computation indicating whether this party won.
    async fn execute(
        &self,
        party_id: usize,
        num_parties: usize,
        input_value: u64,
    ) -> Result<MpcResult>;

    /// Write input file for MP-SPDZ.
    ///
    /// # Arguments
    /// * `party_id` - This party's ID
    /// * `value` - The input value to write
    async fn write_input(&self, party_id: usize, value: u64) -> Result<()>;

    /// Write hosts file for MP-SPDZ.
    ///
    /// # Arguments
    /// * `hosts_name` - Name of the hosts file
    /// * `num_parties` - Number of parties (one localhost entry per party)
    async fn write_hosts(&self, hosts_name: &str, num_parties: usize) -> Result<()>;
}
