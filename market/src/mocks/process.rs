//! Mock MPC runner for testing.

use crate::error::{MarketError, MarketResult};
use crate::traits::{MpcResult, MpcRunner};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Recorded MPC execution for test assertions.
#[derive(Debug, Clone)]
pub struct RecordedExecution {
    pub party_id: usize,
    pub num_parties: usize,
    pub input_value: u64,
}

/// Strategy for determining the auction winner.
#[derive(Debug, Clone)]
pub enum WinnerStrategy {
    /// Always return the specified party as winner.
    Fixed(usize),
    /// Calculate winner based on registered bids (highest bid wins).
    /// Ties are broken by party_id (lowest wins, simulating earliest timestamp).
    CalculateFromBids,
}

/// Shared bid registry for multi-party MPC simulation.
///
/// This allows multiple MockMpcRunner instances to share bid information
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

/// Mock MPC runner for testing.
#[derive(Debug, Clone)]
pub struct MockMpcRunner {
    /// Recorded compilations: Map<program_name, num_parties>
    compilations: Arc<RwLock<HashMap<String, usize>>>,
    /// Recorded executions.
    executions: Arc<RwLock<Vec<RecordedExecution>>>,
    /// Recorded input writes: Map<party_id, value>
    inputs: Arc<RwLock<HashMap<usize, u64>>>,
    /// Recorded hosts files: Map<hosts_name, num_parties>
    hosts_files: Arc<RwLock<HashMap<String, usize>>>,
    /// Strategy for determining winner.
    winner_strategy: Arc<RwLock<WinnerStrategy>>,
    /// Shared bid registry for multi-party winner calculation.
    bid_registry: Arc<SharedBidRegistry>,
    /// Whether to fail operations.
    fail_mode: Arc<RwLock<bool>>,
}

impl MockMpcRunner {
    /// Create a new mock MPC runner.
    pub fn new() -> Self {
        Self {
            compilations: Arc::new(RwLock::new(HashMap::new())),
            executions: Arc::new(RwLock::new(Vec::new())),
            inputs: Arc::new(RwLock::new(HashMap::new())),
            hosts_files: Arc::new(RwLock::new(HashMap::new())),
            winner_strategy: Arc::new(RwLock::new(WinnerStrategy::Fixed(0))),
            bid_registry: Arc::new(SharedBidRegistry::new()),
            fail_mode: Arc::new(RwLock::new(false)),
        }
    }

    /// Create a new mock MPC runner with a shared bid registry.
    ///
    /// Use this when simulating multi-party auctions where all parties
    /// need to see the same winner result.
    pub fn with_shared_registry(registry: Arc<SharedBidRegistry>) -> Self {
        Self {
            compilations: Arc::new(RwLock::new(HashMap::new())),
            executions: Arc::new(RwLock::new(Vec::new())),
            inputs: Arc::new(RwLock::new(HashMap::new())),
            hosts_files: Arc::new(RwLock::new(HashMap::new())),
            winner_strategy: Arc::new(RwLock::new(WinnerStrategy::CalculateFromBids)),
            bid_registry: registry,
            fail_mode: Arc::new(RwLock::new(false)),
        }
    }

    /// Set the predetermined winner (legacy API).
    pub async fn set_winner(&self, party_id: usize) {
        *self.winner_strategy.write().await = WinnerStrategy::Fixed(party_id);
    }

    /// Set the winner strategy.
    pub async fn set_winner_strategy(&self, strategy: WinnerStrategy) {
        *self.winner_strategy.write().await = strategy;
    }

    /// Get the bid registry for registering bids.
    pub fn bid_registry(&self) -> &SharedBidRegistry {
        &self.bid_registry
    }

    /// Set failure mode.
    pub async fn set_fail_mode(&self, fail: bool) {
        *self.fail_mode.write().await = fail;
    }

    /// Get recorded compilations.
    pub async fn get_compilations(&self) -> HashMap<String, usize> {
        self.compilations.read().await.clone()
    }

    /// Get recorded executions.
    pub async fn get_executions(&self) -> Vec<RecordedExecution> {
        self.executions.read().await.clone()
    }

    /// Get recorded inputs.
    pub async fn get_inputs(&self) -> HashMap<usize, u64> {
        self.inputs.read().await.clone()
    }

    /// Get recorded hosts files.
    pub async fn get_hosts_files(&self) -> HashMap<String, usize> {
        self.hosts_files.read().await.clone()
    }

    /// Check if a program was compiled.
    pub async fn was_compiled(&self, program: &str) -> bool {
        self.compilations.read().await.contains_key(program)
    }

    /// Clear all recorded data.
    pub async fn clear(&self) {
        self.compilations.write().await.clear();
        self.executions.write().await.clear();
        self.inputs.write().await.clear();
        self.hosts_files.write().await.clear();
    }
}

impl Default for MockMpcRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MpcRunner for MockMpcRunner {
    async fn compile(&self, program: &str, num_parties: usize) -> MarketResult<()> {
        if *self.fail_mode.read().await {
            return Err(MarketError::Mpc("simulated compile failure".into()));
        }

        self.compilations
            .write()
            .await
            .insert(program.to_string(), num_parties);
        Ok(())
    }

    async fn execute(
        &self,
        party_id: usize,
        num_parties: usize,
        input_value: u64,
    ) -> MarketResult<MpcResult> {
        if *self.fail_mode.read().await {
            return Err(MarketError::Mpc("simulated execute failure".into()));
        }

        self.executions.write().await.push(RecordedExecution {
            party_id,
            num_parties,
            input_value,
        });

        // Determine winner based on strategy
        let strategy = self.winner_strategy.read().await.clone();
        let (is_winner, winner_party_id_val, winner_bid_val) = match strategy {
            WinnerStrategy::Fixed(winner_id) => {
                let bid = if party_id == 0 {
                    // Seller gets to see the winning bid
                    self.bid_registry.get_winning_bid().await
                } else {
                    None
                };
                (party_id == winner_id, Some(winner_id), bid)
            }
            WinnerStrategy::CalculateFromBids => {
                match self.bid_registry.calculate_winner_with_bid().await {
                    Some((winner_id, bid)) => {
                        let bid = if party_id == 0 { Some(bid) } else { None };
                        (party_id == winner_id, Some(winner_id), bid)
                    }
                    None => (false, None, None),
                }
            }
        };

        // Party 0 (seller) sees winner_bid and winner_party_id; others don't
        Ok(MpcResult {
            is_winner,
            party_id,
            num_parties,
            winner_bid: if party_id == 0 { winner_bid_val } else { None },
            winner_party_id: if party_id == 0 {
                winner_party_id_val
            } else {
                None
            },
        })
    }

    async fn write_input(&self, party_id: usize, value: u64) -> MarketResult<()> {
        if *self.fail_mode.read().await {
            return Err(MarketError::Mpc("simulated write_input failure".into()));
        }

        self.inputs.write().await.insert(party_id, value);
        Ok(())
    }

    async fn write_hosts(&self, hosts_name: &str, num_parties: usize) -> MarketResult<()> {
        if *self.fail_mode.read().await {
            return Err(MarketError::Mpc("simulated write_hosts failure".into()));
        }

        self.hosts_files
            .write()
            .await
            .insert(hosts_name.to_string(), num_parties);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_mpc_compile() {
        let runner = MockMpcRunner::new();

        runner.compile("auction_n", 3).await.unwrap();

        assert!(runner.was_compiled("auction_n").await);
        let comps = runner.get_compilations().await;
        assert_eq!(comps.get("auction_n"), Some(&3));
    }

    #[tokio::test]
    async fn test_mock_mpc_execute() {
        let runner = MockMpcRunner::new();
        runner.set_winner(1).await;

        let result0 = runner.execute(0, 3, 100).await.unwrap();
        let result1 = runner.execute(1, 3, 200).await.unwrap();
        let result2 = runner.execute(2, 3, 150).await.unwrap();

        assert!(!result0.is_winner);
        assert!(result1.is_winner);
        assert!(!result2.is_winner);
    }

    #[tokio::test]
    async fn test_mock_mpc_write_input() {
        let runner = MockMpcRunner::new();

        runner.write_input(0, 100).await.unwrap();
        runner.write_input(1, 200).await.unwrap();

        let inputs = runner.get_inputs().await;
        assert_eq!(inputs.get(&0), Some(&100));
        assert_eq!(inputs.get(&1), Some(&200));
    }

    #[tokio::test]
    async fn test_mock_mpc_fail_mode() {
        let runner = MockMpcRunner::new();
        runner.set_fail_mode(true).await;

        assert!(runner.compile("test", 2).await.is_err());
        assert!(runner.execute(0, 2, 100).await.is_err());
        assert!(runner.write_input(0, 100).await.is_err());
    }

    #[tokio::test]
    async fn test_shared_bid_registry_highest_wins() {
        let registry = SharedBidRegistry::new();

        // Register bids
        registry.register_bid(0, 100).await;
        registry.register_bid(1, 200).await; // Highest
        registry.register_bid(2, 150).await;

        // Party 1 should win (highest bid)
        assert_eq!(registry.calculate_winner().await, Some(1));
    }

    #[tokio::test]
    async fn test_shared_bid_registry_tie_breaks_by_timestamp() {
        let registry = SharedBidRegistry::new();

        // Same bids, but party 0 registers first
        registry.register_bid_with_timestamp(0, 200, 100).await;
        registry.register_bid_with_timestamp(1, 200, 200).await;
        registry.register_bid_with_timestamp(2, 200, 300).await;

        // Party 0 should win (earliest timestamp)
        assert_eq!(registry.calculate_winner().await, Some(0));
    }

    #[tokio::test]
    async fn test_mock_mpc_calculate_from_bids() {
        let registry = Arc::new(SharedBidRegistry::new());
        let runner = MockMpcRunner::with_shared_registry(registry.clone());

        // Register bids
        registry.register_bid(0, 100).await;
        registry.register_bid(1, 300).await; // Highest
        registry.register_bid(2, 200).await;

        // Execute for each party
        let result0 = runner.execute(0, 3, 100).await.unwrap();
        let result1 = runner.execute(1, 3, 300).await.unwrap();
        let result2 = runner.execute(2, 3, 200).await.unwrap();

        // Only party 1 should be winner
        assert!(!result0.is_winner);
        assert!(result1.is_winner);
        assert!(!result2.is_winner);
    }

    #[tokio::test]
    async fn test_shared_registry_multiple_runners() {
        let registry = Arc::new(SharedBidRegistry::new());

        // Create separate runners sharing the same registry
        let runner0 = MockMpcRunner::with_shared_registry(registry.clone());
        let runner1 = MockMpcRunner::with_shared_registry(registry.clone());
        let runner2 = MockMpcRunner::with_shared_registry(registry.clone());

        // Register bids through the shared registry
        registry.register_bid(0, 150).await;
        registry.register_bid(1, 250).await; // Winner
        registry.register_bid(2, 200).await;

        // Each runner should agree on the winner
        let result0 = runner0.execute(0, 3, 150).await.unwrap();
        let result1 = runner1.execute(1, 3, 250).await.unwrap();
        let result2 = runner2.execute(2, 3, 200).await.unwrap();

        assert!(!result0.is_winner);
        assert!(result1.is_winner);
        assert!(!result2.is_winner);
    }

    #[tokio::test]
    async fn test_mock_mpc_write_hosts() {
        let runner = MockMpcRunner::new();

        runner.write_hosts("hosts_test", 3).await.unwrap();
        runner.write_hosts("hosts_auction", 5).await.unwrap();

        let hosts = runner.get_hosts_files().await;
        assert_eq!(hosts.get("hosts_test"), Some(&3));
        assert_eq!(hosts.get("hosts_auction"), Some(&5));
    }

    #[tokio::test]
    async fn test_mock_mpc_runner_new() {
        let runner = MockMpcRunner::new();

        // Verify defaults
        assert!(runner.get_compilations().await.is_empty());
        assert!(runner.get_executions().await.is_empty());
        assert!(runner.get_inputs().await.is_empty());
        assert!(runner.get_hosts_files().await.is_empty());
    }

    #[tokio::test]
    async fn test_mock_mpc_runner_shared_state() {
        let runner1 = MockMpcRunner::new();

        // Record some operations on runner1
        runner1.compile("test_prog", 2).await.unwrap();
        runner1.execute(0, 2, 100).await.unwrap();

        // Clone the runner
        let runner2 = runner1.clone();

        // Both should see the same compilations and executions
        assert!(runner2.was_compiled("test_prog").await);
        assert_eq!(runner2.get_compilations().await.len(), 1);
        assert_eq!(runner2.get_executions().await.len(), 1);

        // Operations on runner2 should affect runner1
        runner2.write_input(1, 200).await.unwrap();
        assert_eq!(runner1.get_inputs().await.get(&1), Some(&200));
    }

    #[tokio::test]
    async fn test_mock_mpc_runner_clear() {
        let runner = MockMpcRunner::new();

        // Add some data
        runner.compile("test", 2).await.unwrap();
        runner.execute(0, 2, 100).await.unwrap();
        runner.write_input(0, 100).await.unwrap();
        runner.write_hosts("hosts", 2).await.unwrap();

        // Clear everything
        runner.clear().await;

        // All should be empty
        assert!(runner.get_compilations().await.is_empty());
        assert!(runner.get_executions().await.is_empty());
        assert!(runner.get_inputs().await.is_empty());
        assert!(runner.get_hosts_files().await.is_empty());
    }

    #[tokio::test]
    async fn test_shared_bid_registry_clear() {
        let registry = SharedBidRegistry::new();

        registry.register_bid(0, 100).await;
        registry.register_bid(1, 200).await;

        assert_eq!(registry.calculate_winner().await, Some(1));

        registry.clear().await;

        // After clear, no winner
        assert_eq!(registry.calculate_winner().await, None);
        assert!(registry.get_bids().await.is_empty());
    }

    #[tokio::test]
    async fn test_shared_bid_registry_get_bids() {
        let registry = SharedBidRegistry::new();

        registry.register_bid_with_timestamp(0, 100, 10).await;
        registry.register_bid_with_timestamp(1, 200, 20).await;

        let bids = registry.get_bids().await;
        assert_eq!(bids.get(&0), Some(&(100, 10)));
        assert_eq!(bids.get(&1), Some(&(200, 20)));
    }

    #[tokio::test]
    async fn test_shared_bid_registry_default() {
        let registry = SharedBidRegistry::default();

        registry.register_bid(0, 100).await;
        assert_eq!(registry.calculate_winner().await, Some(0));
    }

    #[tokio::test]
    async fn test_shared_bid_registry_empty_winner() {
        let registry = SharedBidRegistry::new();

        // No bids registered, no winner
        assert_eq!(registry.calculate_winner().await, None);
    }

    #[tokio::test]
    async fn test_mock_mpc_runner_default() {
        let runner = MockMpcRunner::default();

        // Should be able to use it normally
        runner.compile("test", 2).await.unwrap();
        assert!(runner.was_compiled("test").await);
    }

    #[tokio::test]
    async fn test_mock_mpc_fail_mode_write_hosts() {
        let runner = MockMpcRunner::new();
        runner.set_fail_mode(true).await;

        assert!(runner.write_hosts("test", 2).await.is_err());
    }

    #[tokio::test]
    async fn test_mock_mpc_runner_set_winner_strategy() {
        let runner = MockMpcRunner::new();

        // Set to fixed strategy
        runner.set_winner_strategy(WinnerStrategy::Fixed(2)).await;

        let result = runner.execute(2, 3, 100).await.unwrap();
        assert!(result.is_winner);

        // Set to calculate from bids
        runner
            .set_winner_strategy(WinnerStrategy::CalculateFromBids)
            .await;

        runner.bid_registry().register_bid(0, 100).await;
        runner.bid_registry().register_bid(1, 200).await;

        let result0 = runner.execute(0, 2, 100).await.unwrap();
        let result1 = runner.execute(1, 2, 200).await.unwrap();

        assert!(!result0.is_winner);
        assert!(result1.is_winner);
    }

    #[tokio::test]
    async fn test_mock_mpc_runner_bid_registry_accessor() {
        let runner = MockMpcRunner::new();

        runner.bid_registry().register_bid(0, 100).await;
        runner.bid_registry().register_bid(1, 200).await;

        let bids = runner.bid_registry().get_bids().await;
        assert_eq!(bids.len(), 2);
    }
}
