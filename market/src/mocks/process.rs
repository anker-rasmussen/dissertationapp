//! Mock MPC runner for testing.

use crate::traits::{MpcResult, MpcRunner};
use anyhow::{anyhow, Result};
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
    /// Predetermined winner (party_id that should win).
    winner: Arc<RwLock<Option<usize>>>,
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
            winner: Arc::new(RwLock::new(None)),
            fail_mode: Arc::new(RwLock::new(false)),
        }
    }

    /// Set the predetermined winner.
    pub async fn set_winner(&self, party_id: usize) {
        *self.winner.write().await = Some(party_id);
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
    async fn compile(&self, program: &str, num_parties: usize) -> Result<()> {
        if *self.fail_mode.read().await {
            return Err(anyhow!("MockMpcRunner: simulated compile failure"));
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
    ) -> Result<MpcResult> {
        if *self.fail_mode.read().await {
            return Err(anyhow!("MockMpcRunner: simulated execute failure"));
        }

        self.executions.write().await.push(RecordedExecution {
            party_id,
            num_parties,
            input_value,
        });

        // Determine winner
        let winner = self.winner.read().await;
        let is_winner = winner.map(|w| w == party_id).unwrap_or(false);

        Ok(MpcResult {
            is_winner,
            party_id,
            num_parties,
        })
    }

    async fn write_input(&self, party_id: usize, value: u64) -> Result<()> {
        if *self.fail_mode.read().await {
            return Err(anyhow!("MockMpcRunner: simulated write_input failure"));
        }

        self.inputs.write().await.insert(party_id, value);
        Ok(())
    }

    async fn write_hosts(&self, hosts_name: &str, num_parties: usize) -> Result<()> {
        if *self.fail_mode.read().await {
            return Err(anyhow!("MockMpcRunner: simulated write_hosts failure"));
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
}
