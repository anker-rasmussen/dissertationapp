//! MP-SPDZ program compilation, process spawning, and output parsing.

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Stdio;
use tokio::process::Command;
use tracing::{error, info};

use super::mpc::MpcTunnelProxy;
use crate::error::{MarketError, MarketResult};

/// Compile the MPC program for the given number of parties using MP-SPDZ's `compile.py`.
pub(crate) async fn compile_mpc_program(mp_spdz_dir: &str, num_parties: usize) -> MarketResult<()> {
    info!("Compiling auction_n program for {} parties...", num_parties);

    let compile_output = Command::new(format!("{mp_spdz_dir}/compile.py"))
        .current_dir(mp_spdz_dir)
        .arg("auction_n")
        .arg("--")
        .arg(num_parties.to_string())
        .output()
        .await;

    match compile_output {
        Ok(result) => {
            let stdout = String::from_utf8_lossy(&result.stdout);
            let stderr = String::from_utf8_lossy(&result.stderr);

            if result.status.success() {
                info!(
                    "Successfully compiled auction_n for {} parties",
                    num_parties
                );
                if !stdout.is_empty() {
                    info!("Compile output: {}", stdout);
                }
                Ok(())
            } else {
                error!("Compilation failed!");
                if !stderr.is_empty() {
                    error!("Compile errors: {}", stderr);
                }
                Err(MarketError::Process(format!(
                    "Failed to compile MPC program for {num_parties} parties"
                )))
            }
        }
        Err(e) => Err(MarketError::Process(format!(
            "Failed to run compile.py: {e}"
        ))),
    }
}

/// Spawn `mascot-party.x`, write the bid value to its stdin, and collect output.
pub(crate) async fn spawn_mascot_party(
    mp_spdz_dir: &str,
    party_id: usize,
    num_parties: usize,
    hosts_file: &std::path::Path,
    bid_value: u64,
) -> MarketResult<std::process::Output> {
    info!(
        "Executing MP-SPDZ auction_n-{} program (MASCOT, interactive)...",
        num_parties
    );

    let program_name = format!("auction_n-{num_parties}");
    let spawn_result = Command::new(format!("{mp_spdz_dir}/mascot-party.x"))
        .current_dir(mp_spdz_dir)
        .arg("-p")
        .arg(party_id.to_string())
        .arg("-N")
        .arg(num_parties.to_string())
        .arg("-OF")
        .arg(".") // Output to stdout
        .arg("-ip")
        .arg(hosts_file.to_str().unwrap_or("HOSTS"))
        .arg("-I") // Interactive mode: read inputs from stdin
        .arg(&program_name)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();

    let mut child = match spawn_result {
        Ok(child) => child,
        Err(e) => {
            return Err(MarketError::Process(format!(
                "Failed to spawn mascot-party.x: {e}"
            )));
        }
    };

    // Write bid value to stdin, then close the pipe (EOF)
    {
        use tokio::io::AsyncWriteExt;
        let mut stdin = child.stdin.take().ok_or_else(|| {
            MarketError::Process("Failed to open stdin pipe to mascot-party.x".into())
        })?;
        stdin
            .write_all(format!("{bid_value}\n").as_bytes())
            .await
            .map_err(|e| {
                MarketError::Process(format!("Failed to write bid value to stdin: {e}"))
            })?;
        // stdin is dropped here, sending EOF
    }

    // Collect stdout/stderr in background tasks so we keep &mut child for kill()
    let mut stdout_pipe = child.stdout.take().ok_or_else(|| {
        MarketError::Process("stdout pipe not available on mascot-party.x process".into())
    })?;
    let mut stderr_pipe = child.stderr.take().ok_or_else(|| {
        MarketError::Process("stderr pipe not available on mascot-party.x process".into())
    })?;

    let stdout_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut stdout_pipe, &mut buf)
            .await
            .unwrap_or(0);
        buf
    });
    let stderr_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut stderr_pipe, &mut buf)
            .await
            .unwrap_or(0);
        buf
    });

    match tokio::time::timeout(std::time::Duration::from_secs(120), child.wait()).await {
        Ok(Ok(status)) => {
            let stdout = stdout_task.await.unwrap_or_default();
            let stderr = stderr_task.await.unwrap_or_default();
            Ok(std::process::Output {
                status,
                stdout,
                stderr,
            })
        }
        Ok(Err(e)) => Err(MarketError::Process(format!(
            "Failed to execute mascot-party.x: {e}"
        ))),
        Err(_) => {
            let _ = child.kill().await;
            // Reap the zombie process to prevent resource leak
            match tokio::time::timeout(std::time::Duration::from_secs(5), child.wait()).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => tracing::error!("Failed to reap MPC process: {}", e),
                Err(_) => tracing::error!("Timeout waiting for MPC process to exit after kill"),
            }
            Err(MarketError::Timeout(
                "MPC execution timed out after 120 seconds".into(),
            ))
        }
    }
}

/// Parse seller (party 0) MPC output to extract the winner party ID and winning bid.
///
/// Looks for lines like `"Winning bid: 42"` and `"Winner: Party 2"`.
/// Returns `Some((winner_party_id, winning_bid))` or `None` if parsing fails.
pub(crate) fn parse_seller_mpc_output(stdout: &str) -> Option<(usize, u64)> {
    let mut winning_bid: Option<u64> = None;
    let mut winner_party_id: Option<usize> = None;

    for line in stdout.lines() {
        if line.contains("Winning bid:") {
            if let Some(bid_str) = line.split(':').next_back() {
                winning_bid = bid_str.trim().parse().ok();
                if winning_bid.is_some() {
                    info!("Parsed winning bid from MPC output");
                }
            }
        }
        if line.contains("Winner: Party") {
            #[allow(clippy::double_ended_iterator_last)]
            if let Some(party_str) = line.split("Party").last() {
                winner_party_id = party_str.trim().parse().ok();
                if let Some(pid) = winner_party_id {
                    info!("Parsed winner party ID from MPC output: {}", pid);
                }
            }
        }
    }

    match (winning_bid, winner_party_id) {
        (Some(bid), Some(pid)) => Some((pid, bid)),
        _ => None,
    }
}

/// Parse bidder (party 1..N) MPC output.
///
/// Returns `Some(true|false)` when the result marker is present, otherwise `None`.
pub(crate) fn parse_bidder_mpc_output(stdout: &str) -> Option<bool> {
    for line in stdout.lines() {
        if line.contains("You won:") {
            if line.contains("You won: 1") {
                info!("Result: I won the auction!");
                return Some(true);
            }
            info!("Result: I did not win");
            return Some(false);
        }
    }
    None
}

/// Parse the MPC attestation value from stdout
pub(crate) fn parse_attestation(stdout: &str) -> Option<String> {
    for line in stdout.lines() {
        if line.contains("MPC_ATTESTATION:") {
            if let Some(val_str) = line.split(':').next_back() {
                let trimmed = val_str.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
    }
    None
}

/// Structured machine-readable MPC result contract written after process execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct MpcResultContract {
    pub schema_version: u8,
    pub role: String,
    pub winner_party_id: Option<usize>,
    pub winning_bid: Option<u64>,
    pub i_won: Option<bool>,
    pub result_attestation: Option<String>,
}

impl MpcResultContract {
    const SCHEMA_VERSION: u8 = 2;

    pub(crate) fn from_seller_stdout(stdout: &str) -> MarketResult<Self> {
        let (winner_party_id, winning_bid) = parse_seller_mpc_output(stdout).ok_or_else(|| {
            MarketError::Process(
                "Seller MPC result contract parse failed: missing winner or bid fields".into(),
            )
        })?;
        let result_attestation = parse_attestation(stdout);

        Ok(Self {
            schema_version: Self::SCHEMA_VERSION,
            role: "seller".to_string(),
            winner_party_id: Some(winner_party_id),
            winning_bid: Some(winning_bid),
            i_won: None,
            result_attestation,
        })
    }

    pub(crate) fn from_bidder_stdout(stdout: &str) -> MarketResult<Self> {
        let i_won = parse_bidder_mpc_output(stdout).ok_or_else(|| {
            MarketError::Process(
                "Bidder MPC result contract parse failed: missing 'You won:' marker".into(),
            )
        })?;
        let result_attestation = parse_attestation(stdout);

        Ok(Self {
            schema_version: Self::SCHEMA_VERSION,
            role: "bidder".to_string(),
            winner_party_id: None,
            winning_bid: None,
            i_won: Some(i_won),
            result_attestation,
        })
    }
}

/// Persist the machine-readable MPC result contract to JSON.
pub(crate) async fn write_result_contract(
    path: &Path,
    result: &MpcResultContract,
) -> MarketResult<()> {
    let bytes = serde_json::to_vec_pretty(result).map_err(|e| {
        MarketError::Serialization(format!("Failed to serialize MPC result contract: {e}"))
    })?;
    tokio::fs::write(path, bytes).await.map_err(|e| {
        MarketError::Process(format!(
            "Failed to write MPC result contract {}: {e}",
            path.display()
        ))
    })
}

/// Read and validate the machine-readable MPC result contract from JSON.
pub(crate) async fn read_result_contract(path: &Path) -> MarketResult<MpcResultContract> {
    let data = tokio::fs::read(path).await.map_err(|e| {
        MarketError::Process(format!(
            "Failed to read MPC result contract {}: {e}",
            path.display()
        ))
    })?;
    let result: MpcResultContract = serde_json::from_slice(&data).map_err(|e| {
        MarketError::Serialization(format!(
            "Failed to deserialize MPC result contract {}: {e}",
            path.display()
        ))
    })?;
    if result.schema_version != MpcResultContract::SCHEMA_VERSION {
        return Err(MarketError::Serialization(format!(
            "Unsupported MPC result contract schema version {}",
            result.schema_version
        )));
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// RAII cleanup guard for tunnel proxy + hosts file
// ---------------------------------------------------------------------------

/// Ensures MPC tunnel proxy cleanup and hosts file removal on all exit paths.
///
/// The guard calls `tunnel_proxy.cleanup()` and removes the temporary hosts file
/// when it is dropped, eliminating duplicated cleanup code in success/error branches.
pub(crate) struct MpcCleanupGuard {
    tunnel_proxy: Option<MpcTunnelProxy>,
    hosts_file: std::path::PathBuf,
}

impl MpcCleanupGuard {
    pub(crate) const fn new(tunnel_proxy: MpcTunnelProxy, hosts_file: std::path::PathBuf) -> Self {
        Self {
            tunnel_proxy: Some(tunnel_proxy),
            hosts_file,
        }
    }
}

impl Drop for MpcCleanupGuard {
    fn drop(&mut self) {
        if let Some(proxy) = self.tunnel_proxy.take() {
            proxy.cleanup();
        }
        let _ = std::fs::remove_file(&self.hosts_file);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_seller_output_valid() {
        let output = r#"
MP-SPDZ starting...
Loading program...
MPC_ATTESTATION: 36893488147419103274
Winning bid: 42
Winner: Party 2
MPC execution completed
"#;
        let result = parse_seller_mpc_output(output);
        assert_eq!(result, Some((2, 42)));
    }

    #[test]
    fn test_parse_seller_output_no_winner() {
        let output = r#"
MP-SPDZ starting...
Winning bid: 100
MPC execution completed
"#;
        let result = parse_seller_mpc_output(output);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_seller_output_no_bid() {
        let output = r#"
MP-SPDZ starting...
Winner: Party 1
MPC execution completed
"#;
        let result = parse_seller_mpc_output(output);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_seller_output_empty() {
        let output = "";
        let result = parse_seller_mpc_output(output);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_seller_output_party_zero() {
        let output = r#"
MP-SPDZ starting...
MPC_ATTESTATION: 999
Winning bid: 999
Winner: Party 0
MPC execution completed
"#;
        let result = parse_seller_mpc_output(output);
        assert_eq!(result, Some((0, 999)));
    }

    #[test]
    fn test_parse_bidder_output_won() {
        let output = r#"
MP-SPDZ starting...
Loading program...
MPC_ATTESTATION: 123456789
You won: 1
MPC execution completed
"#;
        let result = parse_bidder_mpc_output(output);
        assert_eq!(result, Some(true));
    }

    #[test]
    fn test_parse_bidder_output_lost() {
        let output = r#"
MP-SPDZ starting...
Loading program...
MPC_ATTESTATION: 123456789
You won: 0
MPC execution completed
"#;
        let result = parse_bidder_mpc_output(output);
        assert_eq!(result, Some(false));
    }

    #[test]
    fn test_parse_bidder_output_empty() {
        let output = "";
        let result = parse_bidder_mpc_output(output);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_bidder_output_no_result_line() {
        let output = r#"
MP-SPDZ starting...
Loading program...
MPC execution completed
"#;
        let result = parse_bidder_mpc_output(output);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_attestation() {
        let output = "MPC_ATTESTATION: 123456789\n";
        let result = parse_attestation(output);
        assert_eq!(result, Some("123456789".to_string()));
    }

    #[test]
    fn test_parse_attestation_empty() {
        let output = "";
        let result = parse_attestation(output);
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_attestation_not_found() {
        let output = "Some other output\nNo attestation here\n";
        let result = parse_attestation(output);
        assert_eq!(result, None);
    }

    #[test]
    fn test_contract_from_seller_stdout() {
        let output = "MPC_ATTESTATION: 55340232221128778\nWinning bid: 77\nWinner: Party 3\n";
        let contract = MpcResultContract::from_seller_stdout(output).unwrap();
        assert_eq!(contract.role, "seller");
        assert_eq!(contract.winner_party_id, Some(3));
        assert_eq!(contract.winning_bid, Some(77));
        assert_eq!(contract.i_won, None);
        assert_eq!(
            contract.result_attestation,
            Some("55340232221128778".to_string())
        );
    }

    #[test]
    fn test_contract_from_bidder_stdout() {
        let output = "MPC_ATTESTATION: 55340232221128778\nYou won: 1\n";
        let contract = MpcResultContract::from_bidder_stdout(output).unwrap();
        assert_eq!(contract.role, "bidder");
        assert_eq!(contract.i_won, Some(true));
        assert_eq!(contract.winner_party_id, None);
        assert_eq!(contract.winning_bid, None);
        assert_eq!(
            contract.result_attestation,
            Some("55340232221128778".to_string())
        );
    }

    #[tokio::test]
    async fn test_result_contract_roundtrip() {
        let contract = MpcResultContract {
            schema_version: MpcResultContract::SCHEMA_VERSION,
            role: "seller".to_string(),
            winner_party_id: Some(2),
            winning_bid: Some(500),
            i_won: None,
            result_attestation: Some("test_attestation_123".to_string()),
        };

        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join(format!("test_result_{}.json", std::process::id()));

        write_result_contract(&path, &contract).await.unwrap();
        let restored = read_result_contract(&path).await.unwrap();

        assert_eq!(contract, restored);

        // Cleanup
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_result_contract_rejects_old_schema() {
        // Create a contract with old schema version 1
        let old_contract = MpcResultContract {
            schema_version: 1,
            role: "seller".to_string(),
            winner_party_id: Some(1),
            winning_bid: Some(100),
            i_won: None,
            result_attestation: None,
        };

        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join(format!("test_old_result_{}.json", std::process::id()));

        // Write the old contract
        write_result_contract(&path, &old_contract).await.unwrap();

        // Reading should fail due to schema version mismatch
        let result = read_result_contract(&path).await;
        assert!(result.is_err());
        match result {
            Err(MarketError::Serialization(msg)) => {
                assert!(msg.contains("Unsupported MPC result contract schema version 1"));
            }
            _ => panic!("Expected MarketError::Serialization for schema version mismatch"),
        }

        // Cleanup
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_result_contract_malformed_json() {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join(format!("test_malformed_{}.json", std::process::id()));

        // Write malformed JSON
        tokio::fs::write(&path, b"{ this is not valid json }")
            .await
            .unwrap();

        let result = read_result_contract(&path).await;
        assert!(result.is_err());
        match result {
            Err(MarketError::Serialization(msg)) => {
                assert!(msg.contains("Failed to deserialize MPC result contract"));
            }
            _ => panic!("Expected MarketError::Serialization for malformed JSON"),
        }

        // Cleanup
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn test_result_contract_missing_file() {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join(format!("test_nonexistent_{}.json", std::process::id()));

        // Ensure file doesn't exist
        let _ = tokio::fs::remove_file(&path).await;

        let result = read_result_contract(&path).await;
        assert!(result.is_err());
        match result {
            Err(MarketError::Process(msg)) => {
                assert!(msg.contains("Failed to read MPC result contract"));
            }
            _ => panic!("Expected MarketError::Process for missing file"),
        }
    }
}
