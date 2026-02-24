//! MP-SPDZ program compilation, process spawning, and output parsing.

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Stdio;
use tokio::process::Command;
use tracing::{error, info};

use super::mpc::MpcTunnelProxy;
use crate::error::{MarketError, MarketResult};

/// Resolve the MP-SPDZ protocol binary from env or default.
///
/// Default is `mascot-party.x` (dishonest majority, malicious security, N >= 2).
/// Override via `MPC_PROTOCOL` env var.
fn resolve_protocol(_num_parties: usize) -> String {
    std::env::var(crate::config::MPC_PROTOCOL_ENV)
        .unwrap_or_else(|_| crate::config::DEFAULT_MPC_PROTOCOL.to_string())
}

/// Compile the MPC program for the given number of parties using MP-SPDZ's `compile.py`.
pub(crate) async fn compile_mpc_program(mp_spdz_dir: &str, num_parties: usize) -> MarketResult<()> {
    let protocol_binary = resolve_protocol(num_parties);
    let is_ring = protocol_binary.contains("ring");

    info!(
        "Compiling auction_n program for {} parties (ring={}, binary={})...",
        num_parties, is_ring, protocol_binary
    );

    let mut cmd = Command::new(format!("{mp_spdz_dir}/compile.py"));
    cmd.current_dir(mp_spdz_dir);
    if is_ring {
        cmd.arg("-R").arg("64");
    }
    cmd.arg("auction_n").arg("--").arg(num_parties.to_string());

    let compile_output = cmd.output().await;

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

/// Spawn the resolved MP-SPDZ protocol binary, write the bid value to its stdin, and collect output.
#[allow(clippy::too_many_lines)]
pub(crate) async fn spawn_mpc_party(
    mp_spdz_dir: &str,
    party_id: usize,
    num_parties: usize,
    hosts_file: &std::path::Path,
    bid_value: u64,
    base_port: u16,
    timeout_secs: u64,
) -> MarketResult<std::process::Output> {
    let protocol_binary = resolve_protocol(num_parties);

    info!(
        "Executing MP-SPDZ auction_n-{} program ({}, interactive)...",
        num_parties, protocol_binary
    );

    let program_name = format!("auction_n-{num_parties}");
    let mut cmd = Command::new(format!("{mp_spdz_dir}/{protocol_binary}"));
    cmd.current_dir(mp_spdz_dir)
        .arg("-p")
        .arg(party_id.to_string());
    // replicated-ring-party.x is fixed at 3 parties (no -N flag).
    // All other protocols (mascot, semi, shamir) accept -N.
    if !protocol_binary.contains("replicated-ring") {
        cmd.arg("-N").arg(num_parties.to_string());
    }
    cmd.arg("-pn")
        .arg(base_port.to_string())
        .arg("-OF")
        .arg(".") // Output to stdout
        .arg("-ip")
        .arg(hosts_file.to_str().unwrap_or("HOSTS"))
        .arg("-I") // Interactive mode: read inputs from stdin
        .arg(&program_name)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let spawn_result = cmd.spawn();

    let mut child = match spawn_result {
        Ok(child) => child,
        Err(e) => {
            return Err(MarketError::Process(format!(
                "Failed to spawn {protocol_binary}: {e}"
            )));
        }
    };

    // Write bid value to stdin, then close the pipe (EOF)
    {
        use tokio::io::AsyncWriteExt;
        let mut stdin = child.stdin.take().ok_or_else(|| {
            MarketError::Process(format!("Failed to open stdin pipe to {protocol_binary}"))
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
        MarketError::Process(format!(
            "stdout pipe not available on {protocol_binary} process"
        ))
    })?;
    let mut stderr_pipe = child.stderr.take().ok_or_else(|| {
        MarketError::Process(format!(
            "stderr pipe not available on {protocol_binary} process"
        ))
    })?;

    let stdout_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Err(e) = tokio::io::AsyncReadExt::read_to_end(&mut stdout_pipe, &mut buf).await {
            tracing::warn!("Failed to read MPC process stdout: {}", e);
        }
        buf
    });
    let stderr_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Err(e) = tokio::io::AsyncReadExt::read_to_end(&mut stderr_pipe, &mut buf).await {
            tracing::warn!("Failed to read MPC process stderr: {}", e);
        }
        buf
    });

    match tokio::time::timeout(std::time::Duration::from_secs(timeout_secs), child.wait()).await {
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
            "Failed to execute {protocol_binary}: {e}"
        ))),
        Err(_) => {
            let _ = child.kill().await;
            // Reap the zombie process to prevent resource leak
            match tokio::time::timeout(
                std::time::Duration::from_secs(crate::config::MPC_ZOMBIE_REAP_TIMEOUT_SECS),
                child.wait(),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => tracing::error!("Failed to reap MPC process: {}", e),
                Err(_) => tracing::error!("Timeout waiting for MPC process to exit after kill"),
            }
            // Log any output captured before kill for diagnostics
            let stderr = stderr_task.await.unwrap_or_default();
            if !stderr.is_empty() {
                let stderr_str = String::from_utf8_lossy(&stderr);
                error!("MP-SPDZ stderr on timeout:\n{}", stderr_str);
            }
            Err(MarketError::Timeout(format!(
                "MPC execution timed out after {timeout_secs} seconds"
            )))
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

}
