//! MPC (Multi-Party Computation) integration tests.
//!
//! Tests the complete MPC auction execution with MP-SPDZ.
//!
//! # Prerequisites
//! - Devnet must be running
//! - MP-SPDZ must be compiled and available at $MP_SPDZ_DIR
//! - The `auction_n` program must exist in MP-SPDZ
//!
//! # Running
//! ```bash
//! # Set MP-SPDZ directory
//! export MP_SPDZ_DIR=/path/to/MP-SPDZ
//!
//! # Run MPC tests (must be sequential)
//! cargo test --test mpc_integration -- --test-threads=1
//! ```

mod common;

use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};
use market::marketplace::{Bid, BidRecord, Listing};
use market::traits::DhtStore;
use market::{BidOperations, DHTOperations, ListingOperations, MpcResult, SystemTimeProvider, ThreadRng, TimeProvider};

use crate::common::{check_devnet_running, TestCluster};

/// Get the appropriate MPC protocol binary for the party count.
///
/// - `replicated-ring-party.x`: Exactly 3 parties (fastest, ring-based)
/// - `shamir-party.x`: 3+ parties (Shamir secret sharing over prime field)
fn get_mpc_binary(num_parties: usize) -> &'static str {
    if num_parties == 3 {
        "replicated-ring-party.x"
    } else {
        "shamir-party.x"
    }
}

/// Check if MP-SPDZ is available for a given party count.
fn check_mpc_available() -> bool {
    check_mpc_available_for_parties(3)
}

/// Check if MP-SPDZ is available for a specific number of parties.
fn check_mpc_available_for_parties(num_parties: usize) -> bool {
    let mp_spdz_dir = std::env::var("MP_SPDZ_DIR")
        .unwrap_or_else(|_| "/home/broadcom/Repos/Dissertation/Repos/MP-SPDZ".to_string());

    let compile_script = std::path::Path::new(&mp_spdz_dir).join("compile.py");
    let binary = get_mpc_binary(num_parties);
    let party_binary = std::path::Path::new(&mp_spdz_dir).join(binary);

    if !compile_script.exists() {
        eprintln!("MP-SPDZ compile.py not found at {:?}", compile_script);
        return false;
    }
    if !party_binary.exists() {
        eprintln!("MP-SPDZ binary {} not found at {:?}", binary, party_binary);
        return false;
    }
    true
}

/// Clean old bytecode files for the auction program.
fn clean_old_bytecode(mp_spdz_dir: &str) -> Result<()> {
    let bytecode_dir = std::path::Path::new(mp_spdz_dir).join("Programs/Bytecode");
    let schedules_dir = std::path::Path::new(mp_spdz_dir).join("Programs/Schedules");

    // Clean bytecode files
    if bytecode_dir.exists() {
        for entry in std::fs::read_dir(&bytecode_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with("auction_n-") && name_str.ends_with(".bc") {
                println!("  Removing old bytecode: {}", name_str);
                std::fs::remove_file(entry.path())?;
            }
        }
    }

    // Clean schedule files
    if schedules_dir.exists() {
        for entry in std::fs::read_dir(&schedules_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with("auction_n-") && name_str.ends_with(".sch") {
                println!("  Removing old schedule: {}", name_str);
                std::fs::remove_file(entry.path())?;
            }
        }
    }

    Ok(())
}

/// Compile the auction program for N parties.
fn compile_auction_program(num_parties: usize) -> Result<()> {
    let mp_spdz_dir = std::env::var("MP_SPDZ_DIR")
        .unwrap_or_else(|_| "/home/broadcom/Repos/Dissertation/Repos/MP-SPDZ".to_string());

    println!("Compiling auction_n for {} parties...", num_parties);

    // Clean old bytecode first to avoid stale party count issues
    println!("Cleaning old bytecode...");
    clean_old_bytecode(&mp_spdz_dir)?;

    // Use ring arithmetic (-R 64) only for 3-party replicated protocol
    // Use field arithmetic (no -R) for Shamir protocol (4+ parties)
    let mut cmd = Command::new("python3");
    cmd.arg(format!("{}/compile.py", mp_spdz_dir))
        .current_dir(&mp_spdz_dir);

    if num_parties == 3 {
        cmd.arg("-R").arg("64");
    }

    let output = cmd
        .arg("auction_n")
        .arg("--")
        .arg(num_parties.to_string())
        .output()
        .context("Failed to run compile.py")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("Compilation failed: {}", stderr);
    }

    println!("Compilation successful");
    Ok(())
}

/// Write MP-SPDZ input file for a party.
fn write_mpc_input(party_id: usize, bid_value: u64) -> Result<()> {
    let mp_spdz_dir = std::env::var("MP_SPDZ_DIR")
        .unwrap_or_else(|_| "/home/broadcom/Repos/Dissertation/Repos/MP-SPDZ".to_string());

    let input_path = format!("{}/Player-Data/Input-P{}-0", mp_spdz_dir, party_id);
    std::fs::write(&input_path, format!("{}\n", bid_value))
        .with_context(|| format!("Failed to write input file: {}", input_path))?;

    println!("  Party {}: input={} written to {}", party_id, bid_value, input_path);
    Ok(())
}

/// Run MP-SPDZ party in a separate process.
async fn run_mpc_party(
    party_id: usize,
    num_parties: usize,
    port: u16,
) -> Result<MpcResult> {
    let mp_spdz_dir = std::env::var("MP_SPDZ_DIR")
        .unwrap_or_else(|_| "/home/broadcom/Repos/Dissertation/Repos/MP-SPDZ".to_string());

    let program_name = format!("auction_n-{}", num_parties);
    let binary = get_mpc_binary(num_parties);

    println!("  Party {}: starting MPC execution ({}) on port {}", party_id, binary, port);

    let mut cmd = tokio::process::Command::new(format!("{}/{}", mp_spdz_dir, binary));
    cmd.current_dir(&mp_spdz_dir)
        .arg(party_id.to_string())
        .arg(&program_name)
        .arg("-pn")
        .arg(port.to_string())
        .arg("-h")
        .arg("localhost")
        .arg("-OF")
        .arg(".");

    // Add -N flag for Shamir protocol (non-3-party)
    if num_parties != 3 {
        cmd.arg("-N").arg(num_parties.to_string());
    }

    let output = cmd
        .output()
        .await
        .context("Failed to run MPC party")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !output.status.success() {
        println!("  Party {} STDERR: {}", party_id, stderr);
        anyhow::bail!("MPC party {} failed: {}", party_id, stderr);
    }

    // Parse result
    let is_winner = stdout.contains("You won: 1");

    println!(
        "  Party {}: {} (exit={})",
        party_id,
        if is_winner { "WINNER" } else { "lost" },
        output.status.code().unwrap_or(-1)
    );

    Ok(MpcResult {
        is_winner,
        party_id,
        num_parties,
    })
}

/// Run the complete MPC protocol with all parties.
async fn run_mpc_auction(
    bid_values: &[u64],
) -> Result<Vec<MpcResult>> {
    use std::sync::atomic::{AtomicU16, Ordering};
    static PORT_COUNTER: AtomicU16 = AtomicU16::new(0);

    let num_parties = bid_values.len();

    // Generate a unique port for this auction (10000-20000 range)
    let base_port = 10000 + (PORT_COUNTER.fetch_add(1, Ordering::Relaxed) % 10000);

    // Compile program
    compile_auction_program(num_parties)?;

    // Write inputs for all parties
    println!("\nWriting MPC inputs:");
    for (party_id, &bid) in bid_values.iter().enumerate() {
        write_mpc_input(party_id, bid)?;
    }

    // Run all parties concurrently using localhost coordination
    println!("\nRunning MPC protocol with {} parties on port {}:", num_parties, base_port);
    let mut handles = Vec::new();

    for party_id in 0..num_parties {
        let port = base_port;
        let handle = tokio::spawn(async move {
            run_mpc_party(party_id, num_parties, port).await
        });
        handles.push(handle);

        // Stagger party starts slightly
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Collect results
    let mut results = Vec::new();
    for handle in handles {
        let result = handle.await.context("Party task panicked")??;
        results.push(result);
    }

    Ok(results)
}

// ============================================================================
// MPC-Only Tests (no Veilid, just MP-SPDZ)
// ============================================================================

/// Test 3-party MPC auction (standalone, no Veilid).
#[tokio::test]
async fn test_mpc_3_party_standalone() -> Result<()> {
    if !check_mpc_available() {
        eprintln!("Skipping: MP-SPDZ not available");
        return Ok(());
    }

    println!("\n=== 3-Party MPC Standalone Test ===\n");

    // Bids: [100, 250, 175] - Party 1 should win
    let bid_values = vec![100, 250, 175];
    let results = run_mpc_auction(&bid_values).await?;

    // Verify exactly one winner
    let winners: Vec<_> = results.iter().filter(|r| r.is_winner).collect();
    assert_eq!(winners.len(), 1, "Should have exactly one winner");
    assert_eq!(winners[0].party_id, 1, "Party 1 should win with bid 250");

    println!("\n=== 3-Party MPC Test PASSED ===\n");
    Ok(())
}

/// Test 5-party MPC auction (standalone).
#[tokio::test]
async fn test_mpc_5_party_standalone() -> Result<()> {
    if !check_mpc_available_for_parties(5) {
        eprintln!("Skipping: MP-SPDZ not available for 5 parties");
        return Ok(());
    }

    println!("\n=== 5-Party MPC Standalone Test ===\n");

    // Bids: [100, 150, 200, 300, 250] - Party 3 should win
    let bid_values = vec![100, 150, 200, 300, 250];
    let results = run_mpc_auction(&bid_values).await?;

    let winners: Vec<_> = results.iter().filter(|r| r.is_winner).collect();
    assert_eq!(winners.len(), 1, "Should have exactly one winner");
    assert_eq!(winners[0].party_id, 3, "Party 3 should win with bid 300");

    println!("\n=== 5-Party MPC Test PASSED ===\n");
    Ok(())
}

/// Test 10-party MPC auction (standalone).
#[tokio::test]
async fn test_mpc_10_party_standalone() -> Result<()> {
    if !check_mpc_available_for_parties(10) {
        eprintln!("Skipping: MP-SPDZ not available for 10 parties");
        return Ok(());
    }

    println!("\n=== 10-Party MPC Standalone Test ===\n");

    // Party 7 has the highest bid
    let bid_values = vec![100, 150, 200, 250, 300, 350, 400, 999, 450, 500];
    let results = run_mpc_auction(&bid_values).await?;

    let winners: Vec<_> = results.iter().filter(|r| r.is_winner).collect();
    assert_eq!(winners.len(), 1, "Should have exactly one winner");
    assert_eq!(winners[0].party_id, 7, "Party 7 should win with bid 999");

    println!("\n=== 10-Party MPC Test PASSED ===\n");
    Ok(())
}

// ============================================================================
// Full Integration Tests (Veilid + MPC)
// ============================================================================
//
// NOTE: These tests require multiple Veilid nodes in separate processes.
// They are marked as ignored because Veilid's API is a singleton.
// To run these tests, use the integration test script or run separately.
// ============================================================================

/// Full end-to-end test: 3 nodes, DHT coordination, MPC execution.
///
/// This test spawns 3 Veilid nodes which requires running in a separate process.
#[tokio::test]
#[ignore = "Requires separate process - Veilid API is a singleton"]
async fn test_full_auction_3_party() -> Result<()> {
    if !check_devnet_running() {
        eprintln!("Skipping: devnet not running");
        return Ok(());
    }
    if !check_mpc_available() {
        eprintln!("Skipping: MP-SPDZ not available");
        return Ok(());
    }

    println!("\n=== Full 3-Party Auction (Veilid + MPC) ===\n");

    // Spawn cluster
    let bid_values = vec![100, 300, 200]; // Party 1 should win
    let mut cluster = TestCluster::spawn(&bid_values, 800).await?;

    // Create listing
    let seller = cluster.get(0).unwrap();
    let listing_ops = ListingOperations::new(seller.dht.clone());
    let time = SystemTimeProvider::new();

    // Create placeholder key for listing
    let placeholder = seller.dht.create_record().await?;
    let placeholder_key = DHTOperations::record_key(&placeholder);

    let listing = Listing::builder_with_time(time)
        .key(placeholder_key)
        .seller(seller.node_id.clone())
        .title("Full Integration Test Auction")
        .encrypted_content(b"secret".to_vec(), [0u8; 12], "key".to_string())
        .min_bid(50)
        .auction_duration(300)
        .build()
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    let record = listing_ops.publish_listing(&listing).await?;
    let listing_key = DHTOperations::record_key(&record);
    println!("Listing published: {}", listing_key);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Submit bids
    println!("\nSubmitting bids via DHT:");
    for i in 0..3 {
        let node = cluster.get_mut(i).unwrap();
        let rng = ThreadRng::new();
        let time = SystemTimeProvider::new();
        let bid = Bid::new_with_providers(
            listing_key.clone(),
            node.node_id.clone(),
            node.config.bid_value,
            &rng,
            &time,
        );

        let nonce = bid.reveal_nonce.expect("Bid should have reveal nonce");
        node.bid_storage.store_bid(&listing_key, node.config.bid_value, nonce).await;

        // Create placeholder for bid key
        let bid_placeholder = node.dht.create_record().await?;
        let bid_placeholder_key = DHTOperations::record_key(&bid_placeholder);

        let bid_ops = BidOperations::new(node.dht.clone());
        let bid_record = BidRecord {
            listing_key: listing_key.clone(),
            bidder: node.node_id.clone(),
            commitment: bid.commitment,
            timestamp: bid.timestamp,
            bid_key: bid_placeholder_key,
        };

        let bid_rec = bid_ops.publish_bid(bid_record.clone()).await?;
        let _bid_key = DHTOperations::record_key(&bid_rec);
        bid_ops.register_bid(&record, bid_record).await?;

        println!("  Node {}: bid {} registered", i, node.config.bid_value);
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify bid index
    let bid_ops = BidOperations::new(cluster.get(0).unwrap().dht.clone());
    let index = bid_ops.fetch_bid_index(&listing_key).await?;
    assert_eq!(index.bids.len(), 3, "Should have 3 bids in index");

    // Determine party IDs
    println!("\nParty assignment:");
    let sorted_bidders = index.sorted_bidders();
    let mut party_bids: Vec<(usize, u64)> = Vec::new();

    for node in &cluster.nodes {
        let party_id = index.get_party_id(&node.node_id).unwrap();
        party_bids.push((party_id, node.config.bid_value));
        println!("  Node {} -> Party {} (bid {})", node.config.id, party_id, node.config.bid_value);
    }

    // Sort by party ID for MPC input
    party_bids.sort_by_key(|(id, _)| *id);
    let ordered_bids: Vec<u64> = party_bids.iter().map(|(_, bid)| *bid).collect();

    println!("\nOrdered bids for MPC: {:?}", ordered_bids);

    // Run MPC
    let results = run_mpc_auction(&ordered_bids).await?;

    // Find winner
    let winner = results.iter().find(|r| r.is_winner).unwrap();
    let winning_bid = ordered_bids[winner.party_id];
    println!("\nMPC Result: Party {} won with bid {}", winner.party_id, winning_bid);

    // Verify highest bid won
    let max_bid = *ordered_bids.iter().max().unwrap();
    assert_eq!(winning_bid, max_bid, "Winner should have the highest bid");

    println!("\n=== Full 3-Party Auction PASSED ===\n");

    cluster.shutdown().await?;
    Ok(())
}

/// Full end-to-end test with 5 parties.
///
/// This test spawns 5 Veilid nodes which requires running in a separate process.
#[tokio::test]
#[ignore = "Requires separate process - Veilid API is a singleton"]
async fn test_full_auction_5_party() -> Result<()> {
    if !check_devnet_running() {
        eprintln!("Skipping: devnet not running");
        return Ok(());
    }
    if !check_mpc_available() {
        eprintln!("Skipping: MP-SPDZ not available");
        return Ok(());
    }

    println!("\n=== Full 5-Party Auction (Veilid + MPC) ===\n");

    // Different bid values - node 2 should win (bid 500)
    let bid_values = vec![150, 200, 500, 350, 400];
    let mut cluster = TestCluster::spawn(&bid_values, 900).await?;

    // Create listing
    let seller = cluster.get(0).unwrap();
    let listing_ops = ListingOperations::new(seller.dht.clone());
    let time = SystemTimeProvider::new();

    // Create placeholder key
    let placeholder = seller.dht.create_record().await?;
    let placeholder_key = DHTOperations::record_key(&placeholder);

    let listing = Listing::builder_with_time(time)
        .key(placeholder_key)
        .seller(seller.node_id.clone())
        .title("5-Party Auction")
        .encrypted_content(b"secret".to_vec(), [0u8; 12], "key".to_string())
        .min_bid(100)
        .auction_duration(300)
        .build()
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    let record = listing_ops.publish_listing(&listing).await?;
    let listing_key = DHTOperations::record_key(&record);

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Submit all bids
    for i in 0..5 {
        let node = cluster.get_mut(i).unwrap();
        let rng = ThreadRng::new();
        let time = SystemTimeProvider::new();
        let bid = Bid::new_with_providers(
            listing_key.clone(),
            node.node_id.clone(),
            node.config.bid_value,
            &rng,
            &time,
        );
        let nonce = bid.reveal_nonce.expect("Bid should have reveal nonce");
        node.bid_storage.store_bid(&listing_key, node.config.bid_value, nonce).await;

        // Create placeholder for bid key
        let bid_placeholder = node.dht.create_record().await?;
        let bid_placeholder_key = DHTOperations::record_key(&bid_placeholder);

        let bid_ops = BidOperations::new(node.dht.clone());
        let bid_record = BidRecord {
            listing_key: listing_key.clone(),
            bidder: node.node_id.clone(),
            commitment: bid.commitment,
            timestamp: bid.timestamp,
            bid_key: bid_placeholder_key,
        };
        bid_ops.publish_bid(bid_record.clone()).await?;
        bid_ops.register_bid(&record, bid_record).await?;
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Get ordered bids based on party assignment
    let bid_ops = BidOperations::new(cluster.get(0).unwrap().dht.clone());
    let index = bid_ops.fetch_bid_index(&listing_key).await?;

    let mut party_bids: Vec<(usize, u64)> = cluster.nodes.iter()
        .map(|n| (index.get_party_id(&n.node_id).unwrap(), n.config.bid_value))
        .collect();
    party_bids.sort_by_key(|(id, _)| *id);
    let ordered_bids: Vec<u64> = party_bids.iter().map(|(_, b)| *b).collect();

    println!("Ordered bids: {:?}", ordered_bids);

    // Run MPC
    let results = run_mpc_auction(&ordered_bids).await?;

    let winner = results.iter().find(|r| r.is_winner).unwrap();
    let winning_bid = ordered_bids[winner.party_id];

    assert_eq!(winning_bid, 500, "Highest bid (500) should win");
    println!("Winner: Party {} with bid {}", winner.party_id, winning_bid);

    println!("\n=== Full 5-Party Auction PASSED ===\n");

    cluster.shutdown().await?;
    Ok(())
}
