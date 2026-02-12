//! MP-SPDZ program compilation, process spawning, and output parsing.

use anyhow::Result;
use std::process::Stdio;
use tokio::process::Command;
use tracing::{error, info};

use super::mpc::MpcTunnelProxy;

/// Compile the MPC program for the given number of parties using MP-SPDZ's `compile.py`.
pub(crate) async fn compile_mpc_program(mp_spdz_dir: &str, num_parties: usize) -> Result<()> {
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
                Err(anyhow::anyhow!(
                    "Failed to compile MPC program for {num_parties} parties"
                ))
            }
        }
        Err(e) => Err(anyhow::anyhow!("Failed to run compile.py: {e}")),
    }
}

/// Spawn `shamir-party.x`, write the bid value to its stdin, and collect output.
pub(crate) async fn spawn_shamir_party(
    mp_spdz_dir: &str,
    party_id: usize,
    num_parties: usize,
    hosts_file: &std::path::Path,
    bid_value: u64,
) -> Result<std::process::Output> {
    info!(
        "Executing MP-SPDZ auction_n-{} program (Shamir, interactive)...",
        num_parties
    );

    let program_name = format!("auction_n-{num_parties}");
    let spawn_result = Command::new(format!("{mp_spdz_dir}/shamir-party.x"))
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
            return Err(anyhow::anyhow!("Failed to spawn shamir-party.x: {e}"));
        }
    };

    // Write bid value to stdin, then close the pipe (EOF)
    {
        use tokio::io::AsyncWriteExt;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to open stdin pipe to shamir-party.x"))?;
        stdin
            .write_all(format!("{bid_value}\n").as_bytes())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write bid value to stdin: {e}"))?;
        // stdin is dropped here, sending EOF
    }

    match tokio::time::timeout(
        std::time::Duration::from_secs(120),
        child.wait_with_output(),
    )
    .await
    {
        Ok(result) => result.map_err(|e| anyhow::anyhow!("Failed to execute shamir-party.x: {e}")),
        Err(_) => Err(anyhow::anyhow!("MPC execution timed out after 120 seconds")),
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
                if let Some(bid) = winning_bid {
                    info!("Parsed winning bid from MPC output: {}", bid);
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
/// Returns `true` if the output contains `"You won: 1"`, `false` otherwise.
pub(crate) fn parse_bidder_mpc_output(stdout: &str) -> bool {
    for line in stdout.lines() {
        if line.contains("You won:") {
            if line.contains("You won: 1") {
                info!("Result: I won the auction!");
                return true;
            }
            info!("Result: I did not win");
            return false;
        }
    }
    false
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
You won: 1
MPC execution completed
"#;
        let result = parse_bidder_mpc_output(output);
        assert!(result);
    }

    #[test]
    fn test_parse_bidder_output_lost() {
        let output = r#"
MP-SPDZ starting...
Loading program...
You won: 0
MPC execution completed
"#;
        let result = parse_bidder_mpc_output(output);
        assert!(!result);
    }

    #[test]
    fn test_parse_bidder_output_empty() {
        let output = "";
        let result = parse_bidder_mpc_output(output);
        assert!(!result);
    }

    #[test]
    fn test_parse_bidder_output_no_result_line() {
        let output = r#"
MP-SPDZ starting...
Loading program...
MPC execution completed
"#;
        let result = parse_bidder_mpc_output(output);
        assert!(!result);
    }
}
