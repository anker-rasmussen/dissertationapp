//! E2E smoke tests with real Veilid devnets.
//!
//! These tests validate that the mock-based tests accurately reflect
//! real system behavior. They automatically start and stop the devnet.
//!
//! Run smoke subset with: `cargo integration-fast`
//! Run full subset with: `cargo integration`
//!
//! Prerequisites:
//! - Docker installed and running
//! - veilid repo at expected path (for libipspoof.so and docker-compose)
//! - LD_PRELOAD set to libipspoof.so path
//!
//! All E2E tests run serially to avoid devnet conflicts.

use std::time::Duration;

use market::error::MarketError;
use market::marketplace::bid_record::BidRecord;
use market::veilid::bid_ops::BidOperations;
use market::veilid::bid_storage::BidStorage;
use serial_test::serial;

use super::helpers::{
    check_mp_spdz_available, create_encrypted_listing, create_test_listing, init_test_tracing,
    libipspoof_path, make_real_commitment, print_error_chain, run_e2e_test, setup_e2e_environment,
    E2EParticipant, TestNode,
};

// ============================================================================
// BASIC CONNECTIVITY TESTS
// ============================================================================

/// Basic connectivity test - verifies nodes can start and attach to devnet.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_smoke_node_attachment() {
    run_e2e_test("test_e2e_smoke_node_attachment", 180, || async {
        let mut node = TestNode::new(20);
        node.start().await?;
        node.wait_for_ready(90).await?;
        assert!(
            node.node_id().is_some(),
            "Node should have an ID after attachment"
        );
        let _ = node.shutdown().await;
        Ok(())
    })
    .await;
}

/// Multi-node test - verifies multiple nodes can coexist and see each other.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_smoke_multi_node_connectivity() {
    run_e2e_test("test_e2e_smoke_multi_node_connectivity", 180, || async {
        let mut nodes = vec![TestNode::new(21), TestNode::new(22), TestNode::new(23)];
        for node in &mut nodes {
            node.start().await?;
        }
        for node in &nodes {
            node.wait_for_ready(90).await?;
        }
        for node in &mut nodes {
            let _ = node.shutdown().await;
        }
        Ok(())
    })
    .await;
}

/// DHT operations test - verifies DHT read/write works across nodes.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_smoke_dht_operations() {
    run_e2e_test("test_e2e_smoke_dht_operations", 240, || async {
        let mut node1 = TestNode::new(24);
        let mut node2 = TestNode::new(25);
        node1.start().await?;
        node2.start().await?;
        node1.wait_for_ready(90).await?;
        node2.wait_for_ready(90).await?;

        let dht1 = node1
            .node
            .dht_operations()
            .ok_or_else(|| MarketError::Dht("Failed to get DHT operations from node1".into()))?;

        let dht2 = node2
            .node
            .dht_operations()
            .ok_or_else(|| MarketError::Dht("Failed to get DHT operations from node2".into()))?;

        let record = dht1.create_dht_record().await?;
        let key = record.key.clone();

        let test_data = b"Hello from E2E test!".to_vec();
        dht1.set_dht_value(&record, test_data.clone()).await?;

        tokio::time::sleep(Duration::from_secs(5)).await;

        let read_data = dht2.get_dht_value(&key).await?;

        match read_data {
            Some(data) => {
                assert_eq!(data, test_data, "DHT data mismatch");
            }
            None => {
                return Err(MarketError::Dht(
                    "Failed to read DHT data from node2".into(),
                ));
            }
        }

        let _ = node1.shutdown().await;
        let _ = node2.shutdown().await;
        Ok::<_, MarketError>(())
    })
    .await;
}

// ============================================================================
// COORDINATOR TESTS
// ============================================================================

/// Coordinator messaging test - verifies bid announcements work.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_smoke_coordinator_local_registration() {
    run_e2e_test(
        "test_e2e_smoke_coordinator_local_registration",
        240,
        || async {
            let mut node1 = TestNode::new(30);
            let mut node2 = TestNode::new(31);
            node1.start().await?;
            node2.start().await?;
            node1.wait_for_ready(90).await?;
            node2.wait_for_ready(90).await?;

            let node1_id = node1
                .node_id()
                .ok_or_else(|| MarketError::InvalidState("Node1 has no ID".into()))?;

            let dht1 = node1
                .node
                .dht_operations()
                .ok_or_else(|| MarketError::Dht("Failed to get DHT1".into()))?;

            let api1 = node1
                .node
                .api()
                .ok_or_else(|| MarketError::InvalidState("Failed to get API1".into()))?
                .clone();

            use market::veilid::auction_coordinator::AuctionCoordinator;
            use std::sync::Arc;
            use tokio_util::sync::CancellationToken;

            let coordinator1 = Arc::new(AuctionCoordinator::new(
                api1,
                dht1.clone(),
                node1_id.clone(),
                BidStorage::new(),
                node1.offset,
                market::config::DEFAULT_NETWORK_KEY,
                CancellationToken::new(),
            ));

            let listing_record = dht1.create_dht_record().await?;
            let bid_record = dht1.create_dht_record().await?;

            coordinator1
                .register_local_bid(
                    &listing_record.key,
                    node1_id.clone(),
                    bid_record.key.clone(),
                )
                .await;

            let count = coordinator1.get_bid_count(&listing_record.key).await;
            assert_eq!(count, 1, "Should have 1 registered bid");

            eprintln!("[E2E] Bid registration successful");

            let _ = node1.shutdown().await;
            let _ = node2.shutdown().await;
            Ok::<_, MarketError>(())
        },
    )
    .await;
}

/// Diagnostic test for isolated debugging of single node startup.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_smoke_single_node_diagnostic() {
    init_test_tracing();

    eprintln!("\n=== DIAGNOSTIC TEST: Single Node Startup ===\n");

    let libipspoof = libipspoof_path();
    eprintln!("1. Checking libipspoof.so: {}", libipspoof.display());
    if !libipspoof.exists() {
        panic!("libipspoof.so not found at {}", libipspoof.display());
    }
    eprintln!("   [OK] Found libipspoof.so");

    let preload = std::env::var("LD_PRELOAD").unwrap_or_default();
    eprintln!("2. Checking LD_PRELOAD: {}", preload);
    if !preload.contains(libipspoof.to_str().unwrap()) {
        panic!(
            "LD_PRELOAD not set correctly.\n   Expected to contain: {}\n   Actual: {}",
            libipspoof.display(),
            preload
        );
    }
    eprintln!("   [OK] LD_PRELOAD is set");

    eprintln!("3. Starting devnet...");
    let _devnet = match setup_e2e_environment() {
        Ok(d) => {
            eprintln!("   [OK] Devnet started");
            d
        }
        Err(e) => {
            panic!("Failed to start devnet: {:?}", e);
        }
    };

    eprintln!("4. Creating test node (offset 35)...");
    let data_dir = std::env::temp_dir().join("market-e2e-diagnostic-200");
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).expect("Failed to create test data dir");
    eprintln!("   Data dir: {}", data_dir.display());

    use market::veilid::node::{DevNetConfig as DC, VeilidNode};
    let config = DC {
        network_key: "development-network-2025".to_string(),
        bootstrap_nodes: vec!["udp://1.2.3.1:5160".to_string()],
        port_offset: 35,
        limit_over_attached: 8,
    };
    eprintln!("   Network key: {}", config.network_key);
    eprintln!("   Bootstrap: {:?}", config.bootstrap_nodes);
    eprintln!("   Port offset: {} (port 5195)", config.port_offset);

    let mut market_config = market::config::MarketConfig::default();
    market_config.insecure_storage = true;
    let mut node = VeilidNode::new(data_dir.clone(), &market_config).with_devnet(config);

    eprintln!("5. Starting Veilid node...");
    match node.start().await {
        Ok(()) => {
            eprintln!("   [OK] Node started successfully!");

            eprintln!("6. Attaching to network...");
            match node.attach().await {
                Ok(()) => {
                    eprintln!("   [OK] Attached to network");

                    eprintln!("7. Waiting for network state...");
                    tokio::time::sleep(Duration::from_secs(5)).await;

                    let state = node.state();
                    eprintln!("   is_attached: {}", state.is_attached);
                    eprintln!("   peer_count: {}", state.peer_count);
                    eprintln!("   node_ids: {:?}", state.node_ids);
                }
                Err(e) => {
                    eprintln!("   [FAIL] Attach failed:");
                    print_error_chain(&e);
                }
            }

            eprintln!("8. Shutting down...");
            let _ = node.shutdown().await;
        }
        Err(e) => {
            eprintln!("   [FAIL] Node start failed:");
            print_error_chain(&e);
            panic!("Node startup failed - see error chain above");
        }
    }

    let _ = std::fs::remove_dir_all(&data_dir);
    eprintln!("\n=== DIAGNOSTIC TEST COMPLETE ===\n");
}

// ============================================================================
// REAL AUCTION E2E TESTS
// ============================================================================

/// Test AppMessage-based bid announcements between 3 nodes with wired-up message loops.
///
/// Validates: Real Veilid AppMessage delivery, process_app_message() dispatching,
/// seller updating DHT registry in response to announcements.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_smoke_appmessage_bid_announcements() {
    run_e2e_test(
        "test_e2e_smoke_appmessage_bid_announcements",
        360,
        || async {
            let mut seller = E2EParticipant::new(33).await?;
            let mut bidder1 = E2EParticipant::new(34).await?;
            let mut bidder2 = E2EParticipant::new(35).await?;

            let seller_id = seller.node_id().unwrap();
            let bidder1_id = bidder1.node_id().unwrap();
            let bidder2_id = bidder2.node_id().unwrap();
            let seller_signing = seller.signing_pubkey_bytes();
            let bidder1_signing = bidder1.signing_pubkey_bytes();
            let bidder2_signing = bidder2.signing_pubkey_bytes();

            let seller_dht = seller.dht().unwrap();

            let listing_record = seller_dht.create_dht_record().await?;
            let listing =
                create_test_listing(listing_record.key.clone(), seller_id.clone(), 100, 3600);

            use market::veilid::listing_ops::ListingOperations;
            let listing_ops = ListingOperations::new(seller_dht.clone());
            listing_ops
                .update_listing(&listing_record, &listing)
                .await?;

            seller
                .coordinator
                .register_owned_listing(listing_record.clone())
                .await?;

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let seller_nonce: [u8; 32] = rand::random();
            let seller_commitment = make_real_commitment(100, &seller_nonce);
            seller
                .bid_storage
                .store_bid(&listing_record.key, 100, seller_nonce)
                .await;

            let seller_bid_record_dht = seller_dht.create_dht_record().await?;
            let seller_bid = BidRecord {
                listing_key: listing_record.key.clone(),
                bidder: seller_id.clone(),
                commitment: seller_commitment,
                timestamp: now,
                bid_key: seller_bid_record_dht.key.clone(),
                signing_pubkey: seller_signing,
            };
            let seller_bid_ops = BidOperations::new(seller_dht.clone());
            seller_bid_ops
                .register_bid(&listing_record, seller_bid.clone())
                .await?;
            seller_dht
                .set_dht_value(&seller_bid_record_dht, seller_bid.to_cbor()?)
                .await?;
            seller
                .bid_storage
                .store_bid_key(&listing_record.key, &seller_bid_record_dht.key)
                .await;

            seller
                .coordinator
                .add_own_bid_to_registry(
                    &listing_record.key,
                    seller_id.clone(),
                    seller_bid_record_dht.key.clone(),
                    now,
                )
                .await?;

            eprintln!("[E2E] Seller placed own bid and initialized registry");

            let bidder1_dht = bidder1.dht().unwrap();
            let bidder1_nonce: [u8; 32] = rand::random();
            let bidder1_commitment = make_real_commitment(200, &bidder1_nonce);
            bidder1
                .bid_storage
                .store_bid(&listing_record.key, 200, bidder1_nonce)
                .await;

            let bid1_record_dht = bidder1_dht.create_dht_record().await?;
            let bid1 = BidRecord {
                listing_key: listing_record.key.clone(),
                bidder: bidder1_id.clone(),
                commitment: bidder1_commitment,
                timestamp: now + 1,
                bid_key: bid1_record_dht.key.clone(),
                signing_pubkey: bidder1_signing,
            };
            bidder1_dht
                .set_dht_value(&bid1_record_dht, bid1.to_cbor()?)
                .await?;
            bidder1
                .bid_storage
                .store_bid_key(&listing_record.key, &bid1_record_dht.key)
                .await;

            let bidder2_dht = bidder2.dht().unwrap();
            let bidder2_nonce: [u8; 32] = rand::random();
            let bidder2_commitment = make_real_commitment(150, &bidder2_nonce);
            bidder2
                .bid_storage
                .store_bid(&listing_record.key, 150, bidder2_nonce)
                .await;

            let bid2_record_dht = bidder2_dht.create_dht_record().await?;
            let bid2 = BidRecord {
                listing_key: listing_record.key.clone(),
                bidder: bidder2_id.clone(),
                commitment: bidder2_commitment,
                timestamp: now + 2,
                bid_key: bid2_record_dht.key.clone(),
                signing_pubkey: bidder2_signing,
            };
            bidder2_dht
                .set_dht_value(&bid2_record_dht, bid2.to_cbor()?)
                .await?;
            bidder2
                .bid_storage
                .store_bid_key(&listing_record.key, &bid2_record_dht.key)
                .await;

            eprintln!("[E2E] Both bidders published bid records to DHT");

            tokio::time::sleep(Duration::from_secs(5)).await;

            bidder1
                .coordinator
                .broadcast_bid_announcement(&listing_record.key, &bid1_record_dht.key)
                .await?;
            bidder2
                .coordinator
                .broadcast_bid_announcement(&listing_record.key, &bid2_record_dht.key)
                .await?;

            eprintln!("[E2E] Bid announcements broadcast, waiting for propagation...");
            tokio::time::sleep(Duration::from_secs(15)).await;

            let seller_bid_count = seller.coordinator.get_bid_count(&listing_record.key).await;
            eprintln!(
                "[E2E] Seller's local bid announcement count: {}",
                seller_bid_count
            );

            assert!(
                seller_bid_count >= 2,
                "Seller should have received at least 2 bid announcements via AppMessage, got {}",
                seller_bid_count
            );

            let _ = seller.shutdown().await;
            let _ = bidder1.shutdown().await;
            let _ = bidder2.shutdown().await;

            Ok::<_, MarketError>(())
        },
    )
    .await;
}

/// Test real bid flow with SHA256 commitments, deterministic party ordering, and BidStorage.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_smoke_real_bid_flow_with_commitments() {
    run_e2e_test(
        "test_e2e_smoke_real_bid_flow_with_commitments",
        360,
        || async {
            let mut seller = E2EParticipant::new(24).await?;
            let mut bidder1 = E2EParticipant::new(25).await?;
            let mut bidder2 = E2EParticipant::new(26).await?;

            let seller_id = seller.node_id().unwrap();
            let bidder1_id = bidder1.node_id().unwrap();
            let bidder2_id = bidder2.node_id().unwrap();
            let seller_signing = seller.signing_pubkey_bytes();
            let bidder1_signing = bidder1.signing_pubkey_bytes();
            let bidder2_signing = bidder2.signing_pubkey_bytes();

            let seller_dht = seller.dht().unwrap();

            let listing_record = seller_dht.create_dht_record().await?;
            let plaintext = "Secret auction item: Rare first-edition book";
            let listing = create_encrypted_listing(
                listing_record.key.clone(),
                seller_id.clone(),
                100,
                3600,
                plaintext,
            );

            use market::veilid::listing_ops::ListingOperations;
            let listing_ops = ListingOperations::new(seller_dht.clone());
            listing_ops
                .update_listing(&listing_record, &listing)
                .await?;

            seller
                .coordinator
                .register_owned_listing(listing_record.clone())
                .await?;

            seller
                .coordinator
                .store_decryption_key(&listing_record.key, listing.decryption_key.clone())
                .await;

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            // Seller bid (reserve)
            let seller_nonce: [u8; 32] = rand::random();
            let seller_commitment = make_real_commitment(100, &seller_nonce);
            seller
                .bid_storage
                .store_bid(&listing_record.key, 100, seller_nonce)
                .await;

            let seller_bid_record_dht = seller_dht.create_dht_record().await?;
            let seller_bid = BidRecord {
                listing_key: listing_record.key.clone(),
                bidder: seller_id.clone(),
                commitment: seller_commitment,
                timestamp: now,
                bid_key: seller_bid_record_dht.key.clone(),
                signing_pubkey: seller_signing,
            };
            let seller_bid_ops = BidOperations::new(seller_dht.clone());
            seller_bid_ops
                .register_bid(&listing_record, seller_bid.clone())
                .await?;
            seller_dht
                .set_dht_value(&seller_bid_record_dht, seller_bid.to_cbor()?)
                .await?;
            seller
                .bid_storage
                .store_bid_key(&listing_record.key, &seller_bid_record_dht.key)
                .await;

            seller
                .coordinator
                .add_own_bid_to_registry(
                    &listing_record.key,
                    seller_id.clone(),
                    seller_bid_record_dht.key.clone(),
                    now,
                )
                .await?;

            // Bidder 1
            let bidder1_dht = bidder1.dht().unwrap();
            let bidder1_nonce: [u8; 32] = rand::random();
            let bidder1_bid_value: u64 = 200;
            let bidder1_commitment = make_real_commitment(bidder1_bid_value, &bidder1_nonce);
            bidder1
                .bid_storage
                .store_bid(&listing_record.key, bidder1_bid_value, bidder1_nonce)
                .await;

            let bid1_record_dht = bidder1_dht.create_dht_record().await?;
            let bid1 = BidRecord {
                listing_key: listing_record.key.clone(),
                bidder: bidder1_id.clone(),
                commitment: bidder1_commitment,
                timestamp: now + 1,
                bid_key: bid1_record_dht.key.clone(),
                signing_pubkey: bidder1_signing,
            };
            seller_bid_ops
                .register_bid(&listing_record, bid1.clone())
                .await?;
            bidder1
                .bid_storage
                .store_bid_key(&listing_record.key, &bid1_record_dht.key)
                .await;
            bidder1_dht
                .set_dht_value(&bid1_record_dht, bid1.to_cbor()?)
                .await?;

            // Bidder 2
            let bidder2_dht = bidder2.dht().unwrap();
            let bidder2_nonce: [u8; 32] = rand::random();
            let bidder2_bid_value: u64 = 150;
            let bidder2_commitment = make_real_commitment(bidder2_bid_value, &bidder2_nonce);
            bidder2
                .bid_storage
                .store_bid(&listing_record.key, bidder2_bid_value, bidder2_nonce)
                .await;

            let bid2_record_dht = bidder2_dht.create_dht_record().await?;
            let bid2 = BidRecord {
                listing_key: listing_record.key.clone(),
                bidder: bidder2_id.clone(),
                commitment: bidder2_commitment,
                timestamp: now + 2,
                bid_key: bid2_record_dht.key.clone(),
                signing_pubkey: bidder2_signing,
            };
            seller_bid_ops
                .register_bid(&listing_record, bid2.clone())
                .await?;
            bidder2
                .bid_storage
                .store_bid_key(&listing_record.key, &bid2_record_dht.key)
                .await;
            bidder2_dht
                .set_dht_value(&bid2_record_dht, bid2.to_cbor()?)
                .await?;

            // Broadcast announcements
            tokio::time::sleep(Duration::from_secs(5)).await;
            bidder1
                .coordinator
                .broadcast_bid_announcement(&listing_record.key, &bid1_record_dht.key)
                .await?;
            bidder2
                .coordinator
                .broadcast_bid_announcement(&listing_record.key, &bid2_record_dht.key)
                .await?;
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Verify party ordering
            let bid_index = seller_bid_ops.fetch_bid_index(&listing_record.key).await?;
            assert_eq!(bid_index.bids.len(), 3, "Should have 3 bids");

            let sorted = bid_index.sorted_bidders(&seller_id);
            assert_eq!(sorted.len(), 3);
            assert_eq!(sorted[0], seller_id, "Seller should always be party 0");
            eprintln!("[E2E] Party ordering verified: seller is party 0");

            // Verify commitments
            for bid in &bid_index.bids {
                let recomputed = if bid.bidder == seller_id {
                    make_real_commitment(100, &seller_nonce)
                } else if bid.bidder == bidder1_id {
                    make_real_commitment(bidder1_bid_value, &bidder1_nonce)
                } else {
                    make_real_commitment(bidder2_bid_value, &bidder2_nonce)
                };
                assert_eq!(
                    bid.commitment, recomputed,
                    "Commitment mismatch for bidder {}",
                    bid.bidder
                );
            }
            eprintln!("[E2E] All SHA256 commitments verified");

            // Verify BidStorage
            assert!(seller.bid_storage.has_bid(&listing_record.key).await);
            assert!(bidder1.bid_storage.has_bid(&listing_record.key).await);
            assert!(bidder2.bid_storage.has_bid(&listing_record.key).await);

            let (stored_val, stored_nonce) = bidder1
                .bid_storage
                .get_bid(&listing_record.key)
                .await
                .unwrap();
            assert_eq!(stored_val, bidder1_bid_value);
            assert_eq!(stored_nonce, bidder1_nonce);
            eprintln!("[E2E] BidStorage correctly populated for all participants");

            // Verify listing retrievable from another node
            let bidder1_listing_ops = ListingOperations::new(bidder1_dht.clone());
            let retrieved = bidder1_listing_ops.get_listing(&listing_record.key).await?;
            assert!(
                retrieved.is_some(),
                "Listing should be retrievable by bidder1"
            );
            let retrieved_listing = retrieved.unwrap();
            assert_eq!(retrieved_listing.title, listing.title);
            eprintln!("[E2E] Listing retrievable from bidder's DHT perspective");

            let _ = seller.shutdown().await;
            let _ = bidder1.shutdown().await;
            let _ = bidder2.shutdown().await;

            Ok::<_, MarketError>(())
        },
    )
    .await;
}

// ============================================================================
// FULL MPC E2E TESTS (process-per-node via market-headless)
// ============================================================================
//
// These tests spawn each Veilid node as a separate OS process using the
// `market-headless` binary.  Each process gets its own tokio runtime,
// eliminating the in-process runtime contention that caused a 40x MPC
// slowdown (~350s → ~8s for MASCOT execution).

use super::helpers::HeadlessParticipant;

/// Helper: poll a headless node for a decryption key until received or timeout.
async fn poll_decryption_key(
    node: &mut HeadlessParticipant,
    listing_key: &str,
    timeout_secs: u64,
) -> Option<String> {
    let start = tokio::time::Instant::now();
    let max_wait = Duration::from_secs(timeout_secs);
    loop {
        match node.get_decryption_key(listing_key).await {
            Ok(Some(key)) => return Some(key),
            Ok(None) => {}
            Err(e) => eprintln!("[E2E] GetDecryptionKey error (retrying): {}", e),
        }
        if start.elapsed() > max_wait {
            return None;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Test full MPC execution with real MP-SPDZ using separate processes.
///
/// Happy path: 3-party MPC auction with full post-MPC verification.
///
/// Seller creates listing, two bidders bid, MPC determines winner, seller
/// challenges winner, winner reveals bid, seller verifies and sends decryption
/// key. Asserts the key matches and the loser gets nothing.
///
/// Each node runs in its own process with its own tokio runtime, so MASCOT
/// should complete in ~60-90s instead of ~350s with in-process tests.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_full_happy_path() {
    if !check_mp_spdz_available() {
        eprintln!(
            "[E2E] SKIPPING test_e2e_full_happy_path: MP-SPDZ protocol binary not found at {}",
            market::config::DEFAULT_MP_SPDZ_DIR
        );
        return;
    }

    run_e2e_test("test_e2e_full_happy_path", 900, || async {
        let mut seller = HeadlessParticipant::new(36).await?;
        let mut bidder1 = HeadlessParticipant::new(37).await?;
        let mut bidder2 = HeadlessParticipant::new(38).await?;

        // Seller creates listing (encryption, DHT, registry, auto-bid at reserve)
        eprintln!("[E2E] Seller creating listing...");
        let (listing_key, expected_key) = seller
            .create_listing("MPC Test Item", "quantum computing blueprint", 100, 30)
            .await?;
        eprintln!("[E2E] Listing created: {}", &listing_key[..20]);

        tokio::time::sleep(Duration::from_secs(15)).await;

        // Bidder1 bids 200 (will win), bidder2 bids 150
        eprintln!("[E2E] Bidder1 placing bid (200)...");
        bidder1.place_bid(&listing_key, 200).await?;
        eprintln!("[E2E] Bidder2 placing bid (150)...");
        bidder2.place_bid(&listing_key, 150).await?;

        // Poll for winner's decryption key
        eprintln!("[E2E] Polling for MPC + post-MPC verification (max 600s)...");
        let mpc_start = tokio::time::Instant::now();

        let winner_key = poll_decryption_key(&mut bidder1, &listing_key, 600).await;
        eprintln!(
            "[E2E] Winner key received after {:?}: {}",
            mpc_start.elapsed(),
            winner_key.is_some()
        );

        let loser_key = match bidder2.get_decryption_key(&listing_key).await {
            Ok(k) => k,
            Err(_) => None,
        };

        assert_eq!(
            winner_key.as_deref(),
            Some(expected_key.as_str()),
            "Winner's decryption key should match seller's key"
        );
        assert!(loser_key.is_none(), "Loser should NOT have decryption key");

        eprintln!("[E2E] PASSED: MPC -> challenge -> reveal -> verify -> key transfer");

        seller.shutdown().await?;
        bidder1.shutdown().await?;
        bidder2.shutdown().await?;

        Ok::<_, MarketError>(())
    })
    .await;
}

/// Test sequential auctions using separate processes.
///
/// Bidder nodes persist across auctions; sellers are fresh per auction.
/// Validates that bidder state from auction 1 does not interfere with auction 2.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_full_sequential_auctions() {
    if !check_mp_spdz_available() {
        eprintln!(
            "[E2E] SKIPPING test_e2e_full_sequential_auctions: MP-SPDZ protocol binary not found"
        );
        return;
    }

    run_e2e_test("test_e2e_full_sequential_auctions", 1800, || async {
        // Shared bidders across both auctions
        let mut bidder1 = HeadlessParticipant::new(21).await?;
        let mut bidder2 = HeadlessParticipant::new(22).await?;

        // ════════════════════════════════════════════════════════════════
        // AUCTION 1
        // ════════════════════════════════════════════════════════════════
        eprintln!("\n[E2E] ═══ AUCTION 1 START ═══");
        let mut seller1 = HeadlessParticipant::new(20).await?;

        let (listing1_key, _) = seller1
            .create_listing("Auction 1 Item", "first sale secret content", 100, 30)
            .await?;

        tokio::time::sleep(Duration::from_secs(15)).await;

        bidder1.place_bid(&listing1_key, 250).await?;
        bidder2.place_bid(&listing1_key, 150).await?;

        eprintln!("[E2E] Auction 1: polling for MPC completion (max 600s)...");
        let a1_winner_key = poll_decryption_key(&mut bidder1, &listing1_key, 600).await;
        assert!(a1_winner_key.is_some(), "Auction 1: winner should have key");

        let a1_loser_key = match bidder2.get_decryption_key(&listing1_key).await {
            Ok(k) => k,
            Err(_) => None,
        };
        assert!(
            a1_loser_key.is_none(),
            "Auction 1: loser should NOT have key"
        );
        eprintln!("[E2E] ═══ AUCTION 1 PASSED ═══\n");

        seller1.shutdown().await?;

        // ════════════════════════════════════════════════════════════════
        // AUCTION 2 — new seller, same bidders
        // ════════════════════════════════════════════════════════════════
        eprintln!("[E2E] ═══ AUCTION 2 START (new seller, same bidders) ═══");
        let mut seller2 = HeadlessParticipant::new(23).await?;

        let (listing2_key, _) = seller2
            .create_listing("Auction 2 Item", "second sale from new seller", 200, 30)
            .await?;

        tokio::time::sleep(Duration::from_secs(15)).await;

        bidder1.place_bid(&listing2_key, 500).await?;
        bidder2.place_bid(&listing2_key, 350).await?;

        eprintln!("[E2E] Auction 2: polling for MPC completion (max 600s)...");
        let a2_winner_key = poll_decryption_key(&mut bidder1, &listing2_key, 600).await;
        assert!(a2_winner_key.is_some(), "Auction 2: winner should have key");

        let a2_loser_key = match bidder2.get_decryption_key(&listing2_key).await {
            Ok(k) => k,
            Err(_) => None,
        };
        assert!(
            a2_loser_key.is_none(),
            "Auction 2: loser should NOT have key"
        );
        eprintln!("[E2E] ═══ AUCTION 2 PASSED ═══");

        seller2.shutdown().await?;
        bidder1.shutdown().await?;
        bidder2.shutdown().await?;

        Ok::<_, MarketError>(())
    })
    .await;
}

// ── Concurrent auctions ────────────────────────────────────────────

/// Two auctions run at the same time on overlapping participants.
///
/// seller1 creates listing A, seller2 creates listing B (both 30s deadline).
/// Both bidders bid on BOTH listings.  Both MPC executions run concurrently.
/// Verifies that both winners receive their decryption keys.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_full_concurrent_auctions() {
    if !check_mp_spdz_available() {
        eprintln!(
            "[E2E] SKIPPING test_e2e_full_concurrent_auctions: MP-SPDZ protocol binary not found"
        );
        return;
    }

    run_e2e_test("test_e2e_full_concurrent_auctions", 900, || async {
        // 3 nodes: node_a and node_b each create a listing, node_c is a pure bidder.
        // All 3 bid on both listings → two 3-party MPC auctions run concurrently.
        let mut node_a = HeadlessParticipant::new(20).await?;
        let mut node_b = HeadlessParticipant::new(21).await?;
        let mut node_c = HeadlessParticipant::new(22).await?;

        // Both sellers create listings at roughly the same time
        eprintln!("[E2E] Creating two listings concurrently...");
        let (listing_a, listing_b) = tokio::try_join!(
            node_a.create_listing("Concurrent A", "secret content A", 100, 30),
            node_b.create_listing("Concurrent B", "secret content B", 100, 30),
        )?;
        let (listing_a_key, _) = listing_a;
        let (listing_b_key, _) = listing_b;
        eprintln!("[E2E] Listing A: {listing_a_key}");
        eprintln!("[E2E] Listing B: {listing_b_key}");

        // Wait for DHT propagation
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Each non-seller bids on the other's listing; node_c bids on both.
        // Listing A: seller=node_a (auto-bid 100), node_b bids 200, node_c bids 150
        // Listing B: seller=node_b (auto-bid 100), node_a bids 300, node_c bids 400
        eprintln!("[E2E] Placing bids on both listings...");
        node_b.place_bid(&listing_a_key, 200).await?;
        node_c.place_bid(&listing_a_key, 150).await?;
        node_a.place_bid(&listing_b_key, 300).await?;
        node_c.place_bid(&listing_b_key, 400).await?;
        eprintln!("[E2E] All bids placed, waiting for deadlines + concurrent MPC...");

        // Poll both auctions concurrently.
        // Winner A = node_b (200), Winner B = node_c (400)
        let (key_a, key_b) = tokio::join!(
            poll_decryption_key(&mut node_b, &listing_a_key, 600),
            poll_decryption_key(&mut node_c, &listing_b_key, 600),
        );

        assert!(key_a.is_some(), "Auction A: winner should have decryption key");
        assert!(key_b.is_some(), "Auction B: winner should have decryption key");

        eprintln!("[E2E] Both concurrent auctions completed successfully!");

        node_a.shutdown().await?;
        node_b.shutdown().await?;
        node_c.shutdown().await?;

        Ok::<_, MarketError>(())
    })
    .await;
}

// ── N-party MPC helper (headless) ───────────────────────────────────

/// Run an N-party MPC auction test using headless processes.
///
/// Creates 1 seller + `num_bidders` bidder nodes as separate processes,
/// places bids, and verifies that the highest bidder receives the key.
async fn run_n_party_headless_test(
    num_bidders: usize,
    base_offset: u16,
    mpc_wait_secs: u64,
) -> Result<(), MarketError> {
    let num_parties = num_bidders + 1;
    eprintln!(
        "[E2E] Starting {num_parties}-party headless MPC test (offsets {base_offset}..={})",
        base_offset + num_parties as u16 - 1
    );

    // Spawn seller
    let mut seller = HeadlessParticipant::new(base_offset).await?;

    // Spawn bidders
    let mut bidders = Vec::with_capacity(num_bidders);
    for i in 0..num_bidders {
        let offset = base_offset + 1 + i as u16;
        bidders.push(HeadlessParticipant::new(offset).await?);
    }

    // Seller creates listing with duration long enough for N-party DHT setup
    let duration = 120 + (num_bidders as u64) * 30;
    let (listing_key, _) = seller
        .create_listing(
            "N-Party Test Item",
            "N-party MPC test content",
            100,
            duration,
        )
        .await?;

    // Wait for listing propagation
    tokio::time::sleep(Duration::from_secs(15)).await;

    // Each bidder places a bid: 200, 300, 400, ...
    // Last bidder always has highest bid (= winner)
    for (i, bidder) in bidders.iter_mut().enumerate() {
        let bid_value = 200 + (i as u64) * 100;
        eprintln!("[E2E] Bidder{} placing bid ({})...", i + 1, bid_value);
        bidder.place_bid(&listing_key, bid_value).await?;
    }

    eprintln!("[E2E] All {num_bidders} bids placed. Polling for MPC (max {mpc_wait_secs}s)...");

    // Poll winner (last bidder = highest bid)
    let winner_idx = num_bidders - 1;
    let winner_key =
        poll_decryption_key(&mut bidders[winner_idx], &listing_key, mpc_wait_secs).await;

    // Verify non-winners don't have keys
    for (i, bidder) in bidders.iter_mut().enumerate() {
        if i == winner_idx {
            continue;
        }
        let key = match bidder.get_decryption_key(&listing_key).await {
            Ok(k) => k,
            Err(_) => None,
        };
        eprintln!("[E2E] Bidder{} decryption key: {}", i + 1, key.is_some());
        assert!(
            key.is_none(),
            "Non-winner bidder{} should NOT have decryption key",
            i + 1
        );
    }

    if winner_key.is_some() {
        eprintln!("[E2E] {num_parties}-party headless MPC + post-MPC flow completed!");
    } else {
        seller.shutdown().await?;
        for b in &mut bidders {
            b.shutdown().await?;
        }
        return Err(MarketError::Timeout(format!(
            "{num_parties}-party MPC did not complete: winner did not receive decryption key"
        )));
    }

    seller.shutdown().await?;
    for b in &mut bidders {
        b.shutdown().await?;
    }

    eprintln!("[E2E] {num_parties}-party headless MPC test PASSED");
    Ok(())
}

/// 5-party MPC auction test (1 seller + 4 bidders) using headless processes.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_scale_mpc_5_party() {
    if !check_mp_spdz_available() {
        eprintln!(
            "[E2E] SKIPPING test_e2e_scale_mpc_5_party: MP-SPDZ protocol binary not found at {}",
            market::config::DEFAULT_MP_SPDZ_DIR
        );
        return;
    }

    run_e2e_test("test_e2e_scale_mpc_5_party", 1800, || async {
        run_n_party_headless_test(4, 20, 600).await
    })
    .await;
}

/// 10-party MPC auction test (1 seller + 9 bidders) using headless processes.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_scale_mpc_10_party() {
    if !check_mp_spdz_available() {
        eprintln!(
            "[E2E] SKIPPING test_e2e_scale_mpc_10_party: MP-SPDZ protocol binary not found at {}",
            market::config::DEFAULT_MP_SPDZ_DIR
        );
        return;
    }

    run_e2e_test("test_e2e_scale_mpc_10_party", 3600, || async {
        run_n_party_headless_test(9, 20, 1800).await
    })
    .await;
}
