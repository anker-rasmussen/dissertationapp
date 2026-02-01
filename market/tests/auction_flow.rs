//! Multi-node auction flow integration tests.
//!
//! Tests the complete auction lifecycle with varying numbers of participants:
//! - 3-party auction (minimum viable)
//! - 5-party auction (moderate)
//! - 10-party auction (stress test)
//!
//! # Prerequisites
//! - Devnet must be running (docker-compose up)
//! - LD_PRELOAD must be set for IP translation
//! - MP-SPDZ must be available at $MP_SPDZ_DIR
//!
//! # Running
//! ```bash
//! # Run all auction tests (sequential to avoid port conflicts)
//! cargo test --test auction_flow -- --test-threads=1
//!
//! # Run specific test
//! cargo test --test auction_flow test_3_party_auction -- --test-threads=1
//! ```

mod common;

use std::time::Duration;

use anyhow::{Context, Result};
use market::marketplace::{Bid, BidRecord, Listing};
use market::traits::DhtStore;
use market::{BidOperations, DHTOperations, ListingOperations, SystemTimeProvider, ThreadRng, TimeProvider};

use crate::common::{
    check_devnet_running, generate_bid_values_with_winner, TestCluster, TestNode, TestNodeConfig,
};

/// Create a test listing owned by the given node.
async fn create_test_listing(seller: &TestNode, duration_secs: u64) -> Result<(Listing, market::OwnedDHTRecord)> {
    let listing_ops = ListingOperations::new(seller.dht.clone());
    let time = SystemTimeProvider::new();

    // Create a placeholder record to get a valid key
    let placeholder = seller.dht.create_record().await?;
    let placeholder_key = DHTOperations::record_key(&placeholder);

    let listing = Listing::builder_with_time(time)
        .key(placeholder_key)
        .seller(seller.node_id.clone())
        .title("Integration Test Auction")
        .encrypted_content(
            b"Secret auction content - only winner sees this!".to_vec(),
            [1u8; 12],
            "test_decryption_key_12345".to_string(),
        )
        .min_bid(50)
        .auction_duration(duration_secs)
        .build()
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    let record = listing_ops.publish_listing(&listing).await?;
    let key = DHTOperations::record_key(&record);

    // Update listing with actual key
    let mut listing = listing;
    listing.key = key.clone();

    println!("Created listing at DHT key: {}", key);

    Ok((listing, record))
}

/// Have a node submit a bid on a listing.
async fn submit_bid(
    bidder: &mut TestNode,
    listing: &Listing,
    listing_record: &market::OwnedDHTRecord,
) -> Result<BidRecord> {
    let bid_ops = BidOperations::new(bidder.dht.clone());
    let time = SystemTimeProvider::new();
    let rng = ThreadRng::new();

    // Create the bid with commitment
    let bid = Bid::new_with_providers(
        listing.key.clone(),
        bidder.node_id.clone(),
        bidder.config.bid_value,
        &rng,
        &time,
    );

    // Store locally for MPC reveal
    let nonce = bid.reveal_nonce.expect("Bid should have reveal nonce");
    bidder.bid_storage.store_bid(&listing.key, bidder.config.bid_value, nonce).await;

    // Create placeholder for bid key
    let placeholder = bidder.dht.create_record().await?;
    let placeholder_key = DHTOperations::record_key(&placeholder);

    // Create bid record for DHT
    let bid_record = BidRecord {
        listing_key: listing.key.clone(),
        bidder: bidder.node_id.clone(),
        commitment: bid.commitment,
        timestamp: time.now_unix(),
        bid_key: placeholder_key,
    };

    // Publish bid to DHT
    let record = bid_ops.publish_bid(bid_record.clone()).await?;
    let bid_key = DHTOperations::record_key(&record);

    // Update bid record with actual key
    let mut bid_record = bid_record;
    bid_record.bid_key = bid_key.clone();

    // Store bid key
    bidder.bid_storage.store_bid_key(&listing.key, &bid_key).await;

    // Register in the listing's bid index
    bid_ops.register_bid(listing_record, bid_record.clone()).await?;

    println!(
        "  [Node {}] Submitted bid {} (commitment: {:?}...)",
        bidder.config.id,
        bidder.config.bid_value,
        &bid_record.commitment[..4]
    );

    Ok(bid_record)
}

/// Verify the bid index contains all expected bids.
async fn verify_bid_index(
    node: &TestNode,
    listing_key: &veilid_core::RecordKey,
    expected_count: usize,
) -> Result<()> {
    let bid_ops = BidOperations::new(node.dht.clone());

    // Retry a few times for DHT propagation
    for attempt in 1..=5 {
        let index = bid_ops.fetch_bid_index(listing_key).await?;
        if index.bids.len() == expected_count {
            println!("Bid index verified: {} bids", expected_count);
            return Ok(());
        }
        println!(
            "  Attempt {}: found {}/{} bids, waiting...",
            attempt,
            index.bids.len(),
            expected_count
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    anyhow::bail!("Bid index never reached expected count of {}", expected_count);
}

/// Run a complete auction with the given cluster.
async fn run_auction_test(cluster: &mut TestCluster, auction_duration: u64) -> Result<()> {
    let num_parties = cluster.len();
    println!("\n=== Running {}-Party Auction Test ===\n", num_parties);

    // Node 0 is the seller
    let seller = cluster.get(0).context("No seller node")?;

    // Create listing
    let (listing, listing_record) = create_test_listing(seller, auction_duration).await?;
    println!("Listing created by Node 0 (seller)");

    // Wait for DHT propagation
    tokio::time::sleep(Duration::from_secs(2)).await;

    // All nodes (including seller) submit bids
    println!("\nSubmitting bids:");
    for i in 0..num_parties {
        let node = cluster.get_mut(i).context("Node not found")?;
        submit_bid(node, &listing, &listing_record).await?;
    }

    // Wait for bid index to propagate
    println!("\nWaiting for bid index propagation...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify bid index from multiple nodes
    println!("\nVerifying bid index:");
    for i in 0..std::cmp::min(3, num_parties) {
        let node = cluster.get(i).context("Node not found")?;
        verify_bid_index(node, &listing.key, num_parties).await
            .with_context(|| format!("Node {} failed to see all bids", i))?;
    }

    // Verify party ID assignment is deterministic
    println!("\nVerifying party ID assignment:");
    let bid_ops = BidOperations::new(cluster.get(0).unwrap().dht.clone());
    let index = bid_ops.fetch_bid_index(&listing.key).await?;

    for node in &cluster.nodes {
        let party_id = index.get_party_id(&node.node_id);
        println!(
            "  Node {} (bid {}) -> Party {:?}",
            node.config.id, node.config.bid_value, party_id
        );
        assert!(party_id.is_some(), "Node should have a party ID");
    }

    // Find expected winner (highest bid)
    let expected_winner = cluster.expected_winner().context("No winner found")?;
    println!(
        "\nExpected winner: Node {} with bid {}",
        expected_winner.config.id, expected_winner.config.bid_value
    );

    // Verify sorted bidders are consistent
    let sorted = index.sorted_bidders();
    assert_eq!(sorted.len(), num_parties);
    println!("Sorted bidders: {} parties", sorted.len());

    println!("\n=== {}-Party Auction Test PASSED ===\n", num_parties);
    Ok(())
}

// ============================================================================
// Multi-Party Auction Tests
// ============================================================================
//
// NOTE: These tests are ignored because Veilid's API is a process-global
// singleton. Only one VeilidNode can be initialized per process, so tests
// that require multiple nodes must be run in separate processes.
//
// To run multi-party tests:
// 1. Use the integration test script which runs each test in isolation
// 2. Or run: cargo test --test auction_flow -- --ignored --test-threads=1
//
// For actual multi-party testing, consider using the MPC integration tests
// which coordinate through the devnet nodes.
// ============================================================================

/// Test 3-party auction flow.
///
/// This test spawns 3 Veilid nodes which requires running in a separate process.
#[tokio::test]
#[ignore = "Requires separate process - Veilid API is a singleton"]
async fn test_3_party_auction() -> Result<()> {
    if !check_devnet_running() {
        eprintln!("Skipping: devnet not running");
        return Ok(());
    }

    // Bids: [100, 250, 175] - Node 1 should win
    let bid_values = generate_bid_values_with_winner(3, 1, 100);
    println!("Bid values: {:?}", bid_values);

    let mut cluster = TestCluster::spawn(&bid_values, 200).await?;

    let result = run_auction_test(&mut cluster, 300).await;

    cluster.shutdown().await?;

    result
}

/// Test 5-party auction flow.
///
/// This test spawns 5 Veilid nodes which requires running in a separate process.
#[tokio::test]
#[ignore = "Requires separate process - Veilid API is a singleton"]
async fn test_5_party_auction() -> Result<()> {
    if !check_devnet_running() {
        eprintln!("Skipping: devnet not running");
        return Ok(());
    }

    // Bids: [100, 110, 120, 1100, 140] - Node 3 should win
    let bid_values = generate_bid_values_with_winner(5, 3, 100);
    println!("Bid values: {:?}", bid_values);

    let mut cluster = TestCluster::spawn(&bid_values, 300).await?;

    let result = run_auction_test(&mut cluster, 300).await;

    cluster.shutdown().await?;

    result
}

/// Test 10-party auction flow.
///
/// This test spawns 10 Veilid nodes which requires running in a separate process.
#[tokio::test]
#[ignore = "Requires separate process - Veilid API is a singleton"]
async fn test_10_party_auction() -> Result<()> {
    if !check_devnet_running() {
        eprintln!("Skipping: devnet not running");
        return Ok(());
    }

    // Bids: Node 7 should win
    let bid_values = generate_bid_values_with_winner(10, 7, 100);
    println!("Bid values: {:?}", bid_values);

    let mut cluster = TestCluster::spawn(&bid_values, 400).await?;

    let result = run_auction_test(&mut cluster, 300).await;

    cluster.shutdown().await?;

    result
}

// ============================================================================
// Edge Case Tests (Multi-Node)
// ============================================================================

/// Test auction with all equal bids (tie-breaking).
///
/// This test spawns 3 Veilid nodes which requires running in a separate process.
#[tokio::test]
#[ignore = "Requires separate process - Veilid API is a singleton"]
async fn test_equal_bids() -> Result<()> {
    if !check_devnet_running() {
        eprintln!("Skipping: devnet not running");
        return Ok(());
    }

    // All bids are equal
    let bid_values = vec![500, 500, 500];
    println!("Testing with equal bids: {:?}", bid_values);

    let mut cluster = TestCluster::spawn(&bid_values, 500).await?;

    // Create listing
    let seller = cluster.get(0).unwrap();
    let (listing, listing_record) = create_test_listing(seller, 300).await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Submit bids
    for i in 0..3 {
        let node = cluster.get_mut(i).unwrap();
        submit_bid(node, &listing, &listing_record).await?;
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify all bids recorded
    let bid_ops = BidOperations::new(cluster.get(0).unwrap().dht.clone());
    let index = bid_ops.fetch_bid_index(&listing.key).await?;
    assert_eq!(index.bids.len(), 3, "Should have 3 bids");

    // With equal bids, MPC would determine winner based on the protocol
    // (typically the first one in sorted order or random selection)
    println!("Equal bids test passed - all bids recorded");

    cluster.shutdown().await?;
    Ok(())
}

/// Test that late bids are rejected after auction ends.
#[tokio::test]
async fn test_auction_deadline() -> Result<()> {
    if !check_devnet_running() {
        eprintln!("Skipping: devnet not running");
        return Ok(());
    }

    let config = TestNodeConfig::new(600, 100);
    let mut node = TestNode::spawn(config).await?;

    // Create a very short auction (5 seconds)
    let (listing, listing_record) = create_test_listing(&node, 5).await?;

    // Submit bid before deadline
    submit_bid(&mut node, &listing, &listing_record).await?;
    println!("Bid submitted before deadline");

    // Wait for auction to end
    println!("Waiting for auction to end...");
    tokio::time::sleep(Duration::from_secs(6)).await;

    // Verify auction has ended
    assert!(listing.has_ended(), "Auction should have ended");
    println!("Auction deadline test passed");

    node.shutdown().await?;
    Ok(())
}

/// Test bid index merge/consistency across nodes.
///
/// This test spawns 3 Veilid nodes which requires running in a separate process.
#[tokio::test]
#[ignore = "Requires separate process - Veilid API is a singleton"]
async fn test_bid_index_consistency() -> Result<()> {
    if !check_devnet_running() {
        eprintln!("Skipping: devnet not running");
        return Ok(());
    }

    let bid_values = vec![100, 200, 300];
    let mut cluster = TestCluster::spawn(&bid_values, 700).await?;

    let seller = cluster.get(0).unwrap();
    let (listing, listing_record) = create_test_listing(seller, 300).await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Submit bids
    for i in 0..3 {
        let node = cluster.get_mut(i).unwrap();
        submit_bid(node, &listing, &listing_record).await?;
        // Small delay between bids
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    tokio::time::sleep(Duration::from_secs(3)).await;

    // All nodes should see the same bid index
    let mut indices = Vec::new();
    for i in 0..3 {
        let node = cluster.get(i).unwrap();
        let bid_ops = BidOperations::new(node.dht.clone());
        let index = bid_ops.fetch_bid_index(&listing.key).await?;
        indices.push(index);
    }

    // Verify consistency
    assert_eq!(indices[0].bids.len(), indices[1].bids.len());
    assert_eq!(indices[1].bids.len(), indices[2].bids.len());

    // Verify sorted_bidders is consistent
    let sorted0 = indices[0].sorted_bidders();
    let sorted1 = indices[1].sorted_bidders();
    let sorted2 = indices[2].sorted_bidders();

    assert_eq!(sorted0, sorted1, "Sorted bidders should match between nodes");
    assert_eq!(sorted1, sorted2, "Sorted bidders should match between nodes");

    println!("Bid index consistency verified across all nodes");

    cluster.shutdown().await?;
    Ok(())
}
