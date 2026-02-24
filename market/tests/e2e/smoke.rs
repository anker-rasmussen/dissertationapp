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
    wait_for_broadcast_routes, E2EParticipant, TestNode,
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
// FULL MPC E2E TESTS (require MP-SPDZ)
// ============================================================================

/// Test full MPC execution with real MP-SPDZ.
/// Skips gracefully if MP-SPDZ is not available.
#[tokio::test]
#[ignore]
#[serial]
#[allow(clippy::too_many_lines)]
async fn test_e2e_full_mpc_execution_happy_path() {
    if !check_mp_spdz_available() {
        eprintln!(
            "[E2E] SKIPPING test_e2e_full_mpc_execution: MP-SPDZ protocol binary not found at {}",
            market::config::DEFAULT_MP_SPDZ_DIR
        );
        return;
    }

    // MASCOT OT setup takes 10-25 minutes; allow 45 min total.
    run_e2e_test("test_e2e_full_mpc_execution_happy_path", 2700, || async {
        let mut seller = E2EParticipant::new(36).await?;
        let mut bidder1 = E2EParticipant::new(37).await?;
        let mut bidder2 = E2EParticipant::new(38).await?;

        let seller_id = seller.node_id().unwrap();
        let bidder1_id = bidder1.node_id().unwrap();
        let bidder2_id = bidder2.node_id().unwrap();
        let seller_signing = seller.signing_pubkey_bytes();
        let bidder1_signing = bidder1.signing_pubkey_bytes();
        let bidder2_signing = bidder2.signing_pubkey_bytes();

        let seller_dht = seller.dht().unwrap();

        let listing_record = seller_dht.create_dht_record().await?;
        let plaintext = "MPC test content: quantum computing blueprint";
        let listing = create_encrypted_listing(
            listing_record.key.clone(),
            seller_id.clone(),
            100,
            50, // DHT ops take 5-10s each; 50s is enough for 3 bids before deadline
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

        let seller_nonce: [u8; 32] = rand::random();
        seller
            .bid_storage
            .store_bid(&listing_record.key, 100, seller_nonce)
            .await;
        let seller_bid_rec = seller_dht.create_dht_record().await?;
        let seller_bid = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: seller_id.clone(),
            commitment: make_real_commitment(100, &seller_nonce),
            timestamp: now,
            bid_key: seller_bid_rec.key.clone(),
            signing_pubkey: seller_signing,
        };
        let seller_bid_ops = BidOperations::new(seller_dht.clone());
        seller_bid_ops
            .register_bid(&listing_record, seller_bid.clone())
            .await?;
        seller_dht
            .set_dht_value(&seller_bid_rec, seller_bid.to_cbor()?)
            .await?;
        seller
            .bid_storage
            .store_bid_key(&listing_record.key, &seller_bid_rec.key)
            .await;
        seller
            .coordinator
            .add_own_bid_to_registry(
                &listing_record.key,
                seller_id.clone(),
                seller_bid_rec.key.clone(),
                now,
            )
            .await?;

        let bidder1_dht = bidder1.dht().unwrap();
        let b1_nonce: [u8; 32] = rand::random();
        bidder1
            .bid_storage
            .store_bid(&listing_record.key, 200, b1_nonce)
            .await;
        let b1_rec = bidder1_dht.create_dht_record().await?;
        let bid1 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder1_id.clone(),
            commitment: make_real_commitment(200, &b1_nonce),
            timestamp: now + 1,
            bid_key: b1_rec.key.clone(),
            signing_pubkey: bidder1_signing,
        };
        seller_bid_ops
            .register_bid(&listing_record, bid1.clone())
            .await?;
        bidder1
            .bid_storage
            .store_bid_key(&listing_record.key, &b1_rec.key)
            .await;
        bidder1_dht.set_dht_value(&b1_rec, bid1.to_cbor()?).await?;

        let bidder2_dht = bidder2.dht().unwrap();
        let b2_nonce: [u8; 32] = rand::random();
        bidder2
            .bid_storage
            .store_bid(&listing_record.key, 150, b2_nonce)
            .await;
        let b2_rec = bidder2_dht.create_dht_record().await?;
        let bid2 = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder2_id.clone(),
            commitment: make_real_commitment(150, &b2_nonce),
            timestamp: now + 2,
            bid_key: b2_rec.key.clone(),
            signing_pubkey: bidder2_signing,
        };
        seller_bid_ops
            .register_bid(&listing_record, bid2.clone())
            .await?;
        bidder2
            .bid_storage
            .store_bid_key(&listing_record.key, &b2_rec.key)
            .await;
        bidder2_dht.set_dht_value(&b2_rec, bid2.to_cbor()?).await?;

        // Add bidder bids to seller's DHT registry so the registry is
        // complete before the 1-second listing deadline fires.  In
        // production the seller receives these via broadcast; here we
        // write them directly to avoid the broadcast propagation race.
        seller
            .coordinator
            .add_own_bid_to_registry(
                &listing_record.key,
                bidder1_id.clone(),
                b1_rec.key.clone(),
                now + 1,
            )
            .await?;
        seller
            .coordinator
            .add_own_bid_to_registry(
                &listing_record.key,
                bidder2_id.clone(),
                b2_rec.key.clone(),
                now + 2,
            )
            .await?;

        // Register local bid announcements so each bidder knows about
        // all other bids (simulates broadcast receipt).
        seller
            .coordinator
            .register_local_bid(&listing_record.key, bidder1_id.clone(), b1_rec.key.clone())
            .await;
        seller
            .coordinator
            .register_local_bid(&listing_record.key, bidder2_id.clone(), b2_rec.key.clone())
            .await;
        bidder1
            .coordinator
            .register_local_bid(
                &listing_record.key,
                seller_id.clone(),
                seller_bid_rec.key.clone(),
            )
            .await;
        bidder1
            .coordinator
            .register_local_bid(&listing_record.key, bidder2_id.clone(), b2_rec.key.clone())
            .await;
        bidder2
            .coordinator
            .register_local_bid(
                &listing_record.key,
                seller_id.clone(),
                seller_bid_rec.key.clone(),
            )
            .await;
        bidder2
            .coordinator
            .register_local_bid(&listing_record.key, bidder1_id.clone(), b1_rec.key.clone())
            .await;

        listing_ops
            .update_listing(&listing_record, &listing)
            .await?;

        // Watch listing early so route managers exist before MPC signals arrive.
        bidder1.coordinator.watch_listing(listing.to_public()).await;
        bidder2.coordinator.watch_listing(listing.to_public()).await;

        // Wait for all 3 nodes' broadcast routes to appear in the registry.
        assert!(
            wait_for_broadcast_routes(&bidder1.coordinator, &bidder1_id.to_string(), 2, 120).await,
            "Broadcast routes not ready within timeout"
        );

        bidder1
            .coordinator
            .broadcast_bid_announcement(&listing_record.key, &b1_rec.key)
            .await?;
        bidder2
            .coordinator
            .broadcast_bid_announcement(&listing_record.key, &b2_rec.key)
            .await?;
        tokio::time::sleep(Duration::from_secs(2)).await;

        eprintln!("[E2E] All bids placed, announcements broadcast. Starting monitoring...");

        eprintln!("[E2E] Polling for MPC completion (max 2400s)...");

        let mpc_start = tokio::time::Instant::now();
        let mpc_max_wait = Duration::from_secs(2400);
        loop {
            let key = bidder1
                .coordinator
                .get_decryption_key(&listing_record.key)
                .await;
            if key.is_some() {
                eprintln!(
                    "[E2E] Bidder1 received decryption key after {:?}",
                    mpc_start.elapsed()
                );
                break;
            }
            if mpc_start.elapsed() > mpc_max_wait {
                eprintln!("[E2E] MPC did not complete within 2400s, checking results anyway...");
                break;
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }

        let b1_key = bidder1
            .coordinator
            .get_decryption_key(&listing_record.key)
            .await;
        let b2_key = bidder2
            .coordinator
            .get_decryption_key(&listing_record.key)
            .await;

        eprintln!("[E2E] Bidder1 decryption key: {:?}", b1_key.is_some());
        eprintln!("[E2E] Bidder2 decryption key: {:?}", b2_key.is_some());

        let bid_index = seller_bid_ops.fetch_bid_index(&listing_record.key).await?;
        assert_eq!(
            bid_index.bids.len(),
            3,
            "Bid index should still have 3 bids after MPC"
        );

        if b1_key.is_some() {
            eprintln!("[E2E] Full MPC + post-MPC flow completed! Winner got decryption key.");
            assert!(
                b2_key.is_none(),
                "Non-winner should NOT have a decryption key"
            );
        } else {
            return Err(MarketError::Timeout(
                "Full MPC flow did not complete: winner did not receive decryption key".into(),
            ));
        }

        let _ = seller.shutdown().await;
        let _ = bidder1.shutdown().await;
        let _ = bidder2.shutdown().await;

        Ok::<_, MarketError>(())
    })
    .await;
}

/// Test complete winner verification and content decryption.
/// Skips gracefully if MP-SPDZ is not available.
#[tokio::test]
#[ignore]
#[serial]
#[allow(clippy::too_many_lines)]
async fn test_e2e_full_winner_verification_and_decryption() {
    if !check_mp_spdz_available() {
        eprintln!(
            "[E2E] SKIPPING test_e2e_winner_verification_and_decryption: MP-SPDZ protocol binary not found"
        );
        return;
    }

    // MASCOT OT setup takes 10-25 minutes.
    run_e2e_test(
        "test_e2e_full_winner_verification_and_decryption",
        2700,
        || async {
            let mut seller = E2EParticipant::new(27).await?;
            let mut bidder1 = E2EParticipant::new(28).await?;
            let mut bidder2 = E2EParticipant::new(29).await?;

            let seller_id = seller.node_id().unwrap();
            let bidder1_id = bidder1.node_id().unwrap();
            let bidder2_id = bidder2.node_id().unwrap();
            let seller_signing = seller.signing_pubkey_bytes();
            let bidder1_signing = bidder1.signing_pubkey_bytes();
            let bidder2_signing = bidder2.signing_pubkey_bytes();

            let seller_dht = seller.dht().unwrap();

            let listing_record = seller_dht.create_dht_record().await?;
            let plaintext_content =
                "TOP SECRET: Coordinates to buried treasure at 51.5074N, 0.1278W";
            let listing = create_encrypted_listing(
                listing_record.key.clone(),
                seller_id.clone(),
                50,
                50, // 50s is enough for 3 bids before deadline
                plaintext_content,
            );
            let expected_decryption_key = listing.decryption_key.clone();

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

            let s_nonce: [u8; 32] = rand::random();
            seller
                .bid_storage
                .store_bid(&listing_record.key, 50, s_nonce)
                .await;
            let s_bid_rec = seller_dht.create_dht_record().await?;
            let s_bid = BidRecord {
                listing_key: listing_record.key.clone(),
                bidder: seller_id.clone(),
                commitment: make_real_commitment(50, &s_nonce),
                timestamp: now,
                bid_key: s_bid_rec.key.clone(),
                signing_pubkey: seller_signing,
            };
            let seller_bid_ops = BidOperations::new(seller_dht.clone());
            seller_bid_ops
                .register_bid(&listing_record, s_bid.clone())
                .await?;
            seller_dht
                .set_dht_value(&s_bid_rec, s_bid.to_cbor()?)
                .await?;
            seller
                .bid_storage
                .store_bid_key(&listing_record.key, &s_bid_rec.key)
                .await;
            seller
                .coordinator
                .add_own_bid_to_registry(
                    &listing_record.key,
                    seller_id.clone(),
                    s_bid_rec.key.clone(),
                    now,
                )
                .await?;

            let bidder1_dht = bidder1.dht().unwrap();
            let b1_nonce: [u8; 32] = rand::random();
            bidder1
                .bid_storage
                .store_bid(&listing_record.key, 300, b1_nonce)
                .await;
            let b1_rec = bidder1_dht.create_dht_record().await?;
            let bid1 = BidRecord {
                listing_key: listing_record.key.clone(),
                bidder: bidder1_id.clone(),
                commitment: make_real_commitment(300, &b1_nonce),
                timestamp: now + 1,
                bid_key: b1_rec.key.clone(),
                signing_pubkey: bidder1_signing,
            };
            seller_bid_ops
                .register_bid(&listing_record, bid1.clone())
                .await?;
            bidder1
                .bid_storage
                .store_bid_key(&listing_record.key, &b1_rec.key)
                .await;
            bidder1_dht.set_dht_value(&b1_rec, bid1.to_cbor()?).await?;

            let bidder2_dht = bidder2.dht().unwrap();
            let b2_nonce: [u8; 32] = rand::random();
            bidder2
                .bid_storage
                .store_bid(&listing_record.key, 100, b2_nonce)
                .await;
            let b2_rec = bidder2_dht.create_dht_record().await?;
            let bid2 = BidRecord {
                listing_key: listing_record.key.clone(),
                bidder: bidder2_id.clone(),
                commitment: make_real_commitment(100, &b2_nonce),
                timestamp: now + 2,
                bid_key: b2_rec.key.clone(),
                signing_pubkey: bidder2_signing,
            };
            seller_bid_ops
                .register_bid(&listing_record, bid2.clone())
                .await?;
            bidder2
                .bid_storage
                .store_bid_key(&listing_record.key, &b2_rec.key)
                .await;
            bidder2_dht.set_dht_value(&b2_rec, bid2.to_cbor()?).await?;

            // Add bidder bids to seller's DHT registry so the registry is
            // complete before the 1-second listing deadline fires.  In
            // production the seller receives these via broadcast; here we
            // write them directly to avoid the broadcast propagation race.
            seller
                .coordinator
                .add_own_bid_to_registry(
                    &listing_record.key,
                    bidder1_id.clone(),
                    b1_rec.key.clone(),
                    now + 1,
                )
                .await?;
            seller
                .coordinator
                .add_own_bid_to_registry(
                    &listing_record.key,
                    bidder2_id.clone(),
                    b2_rec.key.clone(),
                    now + 2,
                )
                .await?;

            // Register local bid announcements so each bidder knows about
            // all other bids (simulates broadcast receipt).
            seller
                .coordinator
                .register_local_bid(&listing_record.key, bidder1_id.clone(), b1_rec.key.clone())
                .await;
            seller
                .coordinator
                .register_local_bid(&listing_record.key, bidder2_id.clone(), b2_rec.key.clone())
                .await;
            bidder1
                .coordinator
                .register_local_bid(
                    &listing_record.key,
                    seller_id.clone(),
                    s_bid_rec.key.clone(),
                )
                .await;
            bidder1
                .coordinator
                .register_local_bid(&listing_record.key, bidder2_id.clone(), b2_rec.key.clone())
                .await;
            bidder2
                .coordinator
                .register_local_bid(
                    &listing_record.key,
                    seller_id.clone(),
                    s_bid_rec.key.clone(),
                )
                .await;
            bidder2
                .coordinator
                .register_local_bid(&listing_record.key, bidder1_id.clone(), b1_rec.key.clone())
                .await;

            listing_ops
                .update_listing(&listing_record, &listing)
                .await?;

            // Watch listing early so route managers exist before MPC signals arrive.
            bidder1.coordinator.watch_listing(listing.to_public()).await;
            bidder2.coordinator.watch_listing(listing.to_public()).await;

            // Wait for all 3 nodes' broadcast routes to appear in the registry.
            assert!(
                wait_for_broadcast_routes(&bidder1.coordinator, &bidder1_id.to_string(), 2, 60)
                    .await,
                "Broadcast routes not ready within timeout"
            );

            bidder1
                .coordinator
                .broadcast_bid_announcement(&listing_record.key, &b1_rec.key)
                .await?;
            bidder2
                .coordinator
                .broadcast_bid_announcement(&listing_record.key, &b2_rec.key)
                .await?;
            tokio::time::sleep(Duration::from_secs(2)).await;

            eprintln!("[E2E] Polling for MPC + post-MPC flow (max 2400s)...");

            let mpc_start = tokio::time::Instant::now();
            let mpc_max_wait = Duration::from_secs(2400);
            loop {
                let key = bidder1
                    .coordinator
                    .get_decryption_key(&listing_record.key)
                    .await;
                if key.is_some() {
                    eprintln!(
                        "[E2E] Winner received decryption key after {:?}",
                        mpc_start.elapsed()
                    );
                    break;
                }
                if mpc_start.elapsed() > mpc_max_wait {
                    eprintln!("[E2E] MPC + post-MPC did not complete within 2400s");
                    break;
                }
                tokio::time::sleep(Duration::from_secs(10)).await;
            }

            let winner_key = bidder1
                .coordinator
                .get_decryption_key(&listing_record.key)
                .await;
            let loser_key = bidder2
                .coordinator
                .get_decryption_key(&listing_record.key)
                .await;

            eprintln!(
                "[E2E] Bidder1 (expected winner, bid=300) has key: {}",
                winner_key.is_some()
            );
            eprintln!(
                "[E2E] Bidder2 (expected loser, bid=100) has key: {}",
                loser_key.is_some()
            );

            if let Some(key) = &winner_key {
                assert_eq!(
                    key, &expected_decryption_key,
                    "Winner's decryption key should match seller's key"
                );
                eprintln!("[E2E] Decryption key matches!");

                assert!(
                    loser_key.is_none(),
                    "Non-winner should NOT have a decryption key"
                );

                let decrypted = bidder1
                    .coordinator
                    .fetch_and_decrypt_listing(&listing_record.key)
                    .await;
                match decrypted {
                    Ok(content) => {
                        assert_eq!(
                            content, plaintext_content,
                            "Decrypted content should match original plaintext"
                        );
                        eprintln!("[E2E] Content successfully decrypted by winner!");
                        eprintln!("[E2E] Decrypted: \"{}\"", content);
                    }
                    Err(e) => {
                        return Err(MarketError::Crypto(format!(
                            "Winner could not decrypt listing content: {e}"
                        )));
                    }
                }

                eprintln!(
                "[E2E] FULL VERIFICATION PASSED: MPC -> challenge -> reveal -> verify -> decrypt"
            );
            } else {
                return Err(MarketError::Timeout(
                    "Winner did not receive decryption key within timeout".into(),
                ));
            }

            let _ = seller.shutdown().await;
            let _ = bidder1.shutdown().await;
            let _ = bidder2.shutdown().await;

            Ok::<_, MarketError>(())
        },
    )
    .await;
}

/// Test sequential auctions: after auction 1 completes, a new seller runs
/// auction 2 on the same devnet with the same bidder nodes.  Validates that
/// bidder state from auction 1 does not interfere with auction 2.
#[tokio::test]
#[ignore]
#[serial]
#[allow(clippy::too_many_lines)]
async fn test_e2e_full_sequential_auctions() {
    if !check_mp_spdz_available() {
        eprintln!(
            "[E2E] SKIPPING test_e2e_full_sequential_auctions: MP-SPDZ protocol binary not found"
        );
        return;
    }

    // Two MASCOT auctions back-to-back.
    run_e2e_test("test_e2e_full_sequential_auctions", 5400, || async {
        // ── Shared bidders across both auctions ──────────────────────────
        let mut bidder1 = E2EParticipant::new(21).await?;
        let mut bidder2 = E2EParticipant::new(22).await?;
        let bidder1_id = bidder1.node_id().unwrap();
        let bidder2_id = bidder2.node_id().unwrap();
        let bidder1_signing = bidder1.signing_pubkey_bytes();
        let bidder2_signing = bidder2.signing_pubkey_bytes();
        let bidder1_dht = bidder1.dht().unwrap();
        let bidder2_dht = bidder2.dht().unwrap();

        // ════════════════════════════════════════════════════════════════
        // AUCTION 1 — seller1 lists, bidder1 wins with highest bid
        // ════════════════════════════════════════════════════════════════
        eprintln!("\n[E2E] ═══ AUCTION 1 START ═══");
        let mut seller1 = E2EParticipant::new(20).await?;
        let seller1_id = seller1.node_id().unwrap();
        let seller1_signing = seller1.signing_pubkey_bytes();
        let seller1_dht = seller1.dht().unwrap();

        let listing1_record = seller1_dht.create_dht_record().await?;
        let listing1 = create_encrypted_listing(
            listing1_record.key.clone(),
            seller1_id.clone(),
            100,
            50, // 50s is enough for bids before deadline
            "Auction 1 secret content: first sale",
        );

        use market::veilid::listing_ops::ListingOperations;
        let listing1_ops = ListingOperations::new(seller1_dht.clone());
        listing1_ops
            .update_listing(&listing1_record, &listing1)
            .await?;
        seller1
            .coordinator
            .register_owned_listing(listing1_record.clone())
            .await?;
        seller1
            .coordinator
            .store_decryption_key(&listing1_record.key, listing1.decryption_key.clone())
            .await;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Seller1 reserve bid
        let s1_nonce: [u8; 32] = rand::random();
        seller1
            .bid_storage
            .store_bid(&listing1_record.key, 100, s1_nonce)
            .await;
        let s1_bid_rec = seller1_dht.create_dht_record().await?;
        let s1_bid = BidRecord {
            listing_key: listing1_record.key.clone(),
            bidder: seller1_id.clone(),
            commitment: make_real_commitment(100, &s1_nonce),
            timestamp: now,
            bid_key: s1_bid_rec.key.clone(),
            signing_pubkey: seller1_signing,
        };
        let bid_ops1 = BidOperations::new(seller1_dht.clone());
        bid_ops1
            .register_bid(&listing1_record, s1_bid.clone())
            .await?;
        seller1_dht
            .set_dht_value(&s1_bid_rec, s1_bid.to_cbor()?)
            .await?;
        seller1
            .bid_storage
            .store_bid_key(&listing1_record.key, &s1_bid_rec.key)
            .await;
        seller1
            .coordinator
            .add_own_bid_to_registry(
                &listing1_record.key,
                seller1_id.clone(),
                s1_bid_rec.key.clone(),
                now,
            )
            .await?;

        // Bidder1 bids 250 on auction 1
        let b1_nonce1: [u8; 32] = rand::random();
        bidder1
            .bid_storage
            .store_bid(&listing1_record.key, 250, b1_nonce1)
            .await;
        let b1_rec1 = bidder1_dht.create_dht_record().await?;
        let bid1_a1 = BidRecord {
            listing_key: listing1_record.key.clone(),
            bidder: bidder1_id.clone(),
            commitment: make_real_commitment(250, &b1_nonce1),
            timestamp: now + 1,
            bid_key: b1_rec1.key.clone(),
            signing_pubkey: bidder1_signing,
        };
        bid_ops1
            .register_bid(&listing1_record, bid1_a1.clone())
            .await?;
        bidder1
            .bid_storage
            .store_bid_key(&listing1_record.key, &b1_rec1.key)
            .await;
        bidder1_dht
            .set_dht_value(&b1_rec1, bid1_a1.to_cbor()?)
            .await?;

        // Bidder2 bids 150 on auction 1
        let b2_nonce1: [u8; 32] = rand::random();
        bidder2
            .bid_storage
            .store_bid(&listing1_record.key, 150, b2_nonce1)
            .await;
        let b2_rec1 = bidder2_dht.create_dht_record().await?;
        let bid2_a1 = BidRecord {
            listing_key: listing1_record.key.clone(),
            bidder: bidder2_id.clone(),
            commitment: make_real_commitment(150, &b2_nonce1),
            timestamp: now + 2,
            bid_key: b2_rec1.key.clone(),
            signing_pubkey: bidder2_signing,
        };
        bid_ops1
            .register_bid(&listing1_record, bid2_a1.clone())
            .await?;
        bidder2
            .bid_storage
            .store_bid_key(&listing1_record.key, &b2_rec1.key)
            .await;
        bidder2_dht
            .set_dht_value(&b2_rec1, bid2_a1.to_cbor()?)
            .await?;

        // Add bidder bids to seller's DHT registry so the registry is
        // complete before the deadline fires (simulates broadcast receipt).
        seller1
            .coordinator
            .add_own_bid_to_registry(
                &listing1_record.key,
                bidder1_id.clone(),
                b1_rec1.key.clone(),
                now + 1,
            )
            .await?;
        seller1
            .coordinator
            .add_own_bid_to_registry(
                &listing1_record.key,
                bidder2_id.clone(),
                b2_rec1.key.clone(),
                now + 2,
            )
            .await?;

        // Register local bid announcements so each party knows about all
        // other bids (simulates broadcast receipt).
        seller1
            .coordinator
            .register_local_bid(
                &listing1_record.key,
                bidder1_id.clone(),
                b1_rec1.key.clone(),
            )
            .await;
        seller1
            .coordinator
            .register_local_bid(
                &listing1_record.key,
                bidder2_id.clone(),
                b2_rec1.key.clone(),
            )
            .await;
        bidder1
            .coordinator
            .register_local_bid(
                &listing1_record.key,
                seller1_id.clone(),
                s1_bid_rec.key.clone(),
            )
            .await;
        bidder1
            .coordinator
            .register_local_bid(
                &listing1_record.key,
                bidder2_id.clone(),
                b2_rec1.key.clone(),
            )
            .await;
        bidder2
            .coordinator
            .register_local_bid(
                &listing1_record.key,
                seller1_id.clone(),
                s1_bid_rec.key.clone(),
            )
            .await;
        bidder2
            .coordinator
            .register_local_bid(
                &listing1_record.key,
                bidder1_id.clone(),
                b1_rec1.key.clone(),
            )
            .await;

        listing1_ops
            .update_listing(&listing1_record, &listing1)
            .await?;

        // Watch listing early so route managers exist before MPC signals arrive.
        bidder1
            .coordinator
            .watch_listing(listing1.to_public())
            .await;
        bidder2
            .coordinator
            .watch_listing(listing1.to_public())
            .await;

        // Wait for routes (3 nodes → bidder1 sees 2 peers)
        assert!(
            wait_for_broadcast_routes(&bidder1.coordinator, &bidder1_id.to_string(), 2, 120).await,
            "Auction 1: broadcast routes not ready within timeout"
        );

        bidder1
            .coordinator
            .broadcast_bid_announcement(&listing1_record.key, &b1_rec1.key)
            .await?;
        bidder2
            .coordinator
            .broadcast_bid_announcement(&listing1_record.key, &b2_rec1.key)
            .await?;
        tokio::time::sleep(Duration::from_secs(2)).await;

        eprintln!("[E2E] Auction 1: polling for MPC completion (max 2400s)...");
        let mpc1_start = tokio::time::Instant::now();
        loop {
            if bidder1
                .coordinator
                .get_decryption_key(&listing1_record.key)
                .await
                .is_some()
            {
                eprintln!(
                    "[E2E] Auction 1: bidder1 got key after {:?}",
                    mpc1_start.elapsed()
                );
                break;
            }
            if mpc1_start.elapsed() > Duration::from_secs(2400) {
                return Err(MarketError::Timeout(
                    "Auction 1 MPC did not complete within 2400s".into(),
                ));
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }

        let a1_b1_key = bidder1
            .coordinator
            .get_decryption_key(&listing1_record.key)
            .await;
        let a1_b2_key = bidder2
            .coordinator
            .get_decryption_key(&listing1_record.key)
            .await;
        assert!(a1_b1_key.is_some(), "Auction 1: winner should have key");
        assert!(a1_b2_key.is_none(), "Auction 1: loser should NOT have key");
        eprintln!("[E2E] ═══ AUCTION 1 PASSED ═══\n");

        // ════════════════════════════════════════════════════════════════
        // AUCTION 2 — new seller2, same bidders, fresh listing
        // ════════════════════════════════════════════════════════════════
        eprintln!("[E2E] ═══ AUCTION 2 START (new seller, same bidders) ═══");
        let mut seller2 = E2EParticipant::new(23).await?;
        let seller2_id = seller2.node_id().unwrap();
        let seller2_signing = seller2.signing_pubkey_bytes();
        let seller2_dht = seller2.dht().unwrap();

        let listing2_record = seller2_dht.create_dht_record().await?;
        let listing2 = create_encrypted_listing(
            listing2_record.key.clone(),
            seller2_id.clone(),
            200,
            50, // 50s is enough for bids before deadline
            "Auction 2 secret content: second sale from new seller",
        );

        let listing2_ops = ListingOperations::new(seller2_dht.clone());
        listing2_ops
            .update_listing(&listing2_record, &listing2)
            .await?;
        seller2
            .coordinator
            .register_owned_listing(listing2_record.clone())
            .await?;
        seller2
            .coordinator
            .store_decryption_key(&listing2_record.key, listing2.decryption_key.clone())
            .await;

        let now2 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Seller2 reserve bid
        let s2_nonce: [u8; 32] = rand::random();
        seller2
            .bid_storage
            .store_bid(&listing2_record.key, 200, s2_nonce)
            .await;
        let s2_bid_rec = seller2_dht.create_dht_record().await?;
        let s2_bid = BidRecord {
            listing_key: listing2_record.key.clone(),
            bidder: seller2_id.clone(),
            commitment: make_real_commitment(200, &s2_nonce),
            timestamp: now2,
            bid_key: s2_bid_rec.key.clone(),
            signing_pubkey: seller2_signing,
        };
        let bid_ops2 = BidOperations::new(seller2_dht.clone());
        bid_ops2
            .register_bid(&listing2_record, s2_bid.clone())
            .await?;
        seller2_dht
            .set_dht_value(&s2_bid_rec, s2_bid.to_cbor()?)
            .await?;
        seller2
            .bid_storage
            .store_bid_key(&listing2_record.key, &s2_bid_rec.key)
            .await;
        seller2
            .coordinator
            .add_own_bid_to_registry(
                &listing2_record.key,
                seller2_id.clone(),
                s2_bid_rec.key.clone(),
                now2,
            )
            .await?;

        // Bidder1 bids 500 on auction 2
        let b1_nonce2: [u8; 32] = rand::random();
        bidder1
            .bid_storage
            .store_bid(&listing2_record.key, 500, b1_nonce2)
            .await;
        let b1_rec2 = bidder1_dht.create_dht_record().await?;
        let bid1_a2 = BidRecord {
            listing_key: listing2_record.key.clone(),
            bidder: bidder1_id.clone(),
            commitment: make_real_commitment(500, &b1_nonce2),
            timestamp: now2 + 1,
            bid_key: b1_rec2.key.clone(),
            signing_pubkey: bidder1_signing,
        };
        bid_ops2
            .register_bid(&listing2_record, bid1_a2.clone())
            .await?;
        bidder1
            .bid_storage
            .store_bid_key(&listing2_record.key, &b1_rec2.key)
            .await;
        bidder1_dht
            .set_dht_value(&b1_rec2, bid1_a2.to_cbor()?)
            .await?;

        // Bidder2 bids 350 on auction 2
        let b2_nonce2: [u8; 32] = rand::random();
        bidder2
            .bid_storage
            .store_bid(&listing2_record.key, 350, b2_nonce2)
            .await;
        let b2_rec2 = bidder2_dht.create_dht_record().await?;
        let bid2_a2 = BidRecord {
            listing_key: listing2_record.key.clone(),
            bidder: bidder2_id.clone(),
            commitment: make_real_commitment(350, &b2_nonce2),
            timestamp: now2 + 2,
            bid_key: b2_rec2.key.clone(),
            signing_pubkey: bidder2_signing,
        };
        bid_ops2
            .register_bid(&listing2_record, bid2_a2.clone())
            .await?;
        bidder2
            .bid_storage
            .store_bid_key(&listing2_record.key, &b2_rec2.key)
            .await;
        bidder2_dht
            .set_dht_value(&b2_rec2, bid2_a2.to_cbor()?)
            .await?;

        // Add bidder bids to seller2's DHT registry (simulates broadcast receipt).
        seller2
            .coordinator
            .add_own_bid_to_registry(
                &listing2_record.key,
                bidder1_id.clone(),
                b1_rec2.key.clone(),
                now2 + 1,
            )
            .await?;
        seller2
            .coordinator
            .add_own_bid_to_registry(
                &listing2_record.key,
                bidder2_id.clone(),
                b2_rec2.key.clone(),
                now2 + 2,
            )
            .await?;

        // Register local bid announcements for auction 2.
        seller2
            .coordinator
            .register_local_bid(
                &listing2_record.key,
                bidder1_id.clone(),
                b1_rec2.key.clone(),
            )
            .await;
        seller2
            .coordinator
            .register_local_bid(
                &listing2_record.key,
                bidder2_id.clone(),
                b2_rec2.key.clone(),
            )
            .await;
        bidder1
            .coordinator
            .register_local_bid(
                &listing2_record.key,
                seller2_id.clone(),
                s2_bid_rec.key.clone(),
            )
            .await;
        bidder1
            .coordinator
            .register_local_bid(
                &listing2_record.key,
                bidder2_id.clone(),
                b2_rec2.key.clone(),
            )
            .await;
        bidder2
            .coordinator
            .register_local_bid(
                &listing2_record.key,
                seller2_id.clone(),
                s2_bid_rec.key.clone(),
            )
            .await;
        bidder2
            .coordinator
            .register_local_bid(
                &listing2_record.key,
                bidder1_id.clone(),
                b1_rec2.key.clone(),
            )
            .await;

        listing2_ops
            .update_listing(&listing2_record, &listing2)
            .await?;

        // Watch listing early so route managers exist before MPC signals arrive.
        bidder1
            .coordinator
            .watch_listing(listing2.to_public())
            .await;
        bidder2
            .coordinator
            .watch_listing(listing2.to_public())
            .await;

        // Wait for seller2's route to appear (4 nodes total → bidder1 sees 3 peers)
        assert!(
            wait_for_broadcast_routes(&bidder1.coordinator, &bidder1_id.to_string(), 3, 60).await,
            "Auction 2: broadcast routes not ready within timeout"
        );

        bidder1
            .coordinator
            .broadcast_bid_announcement(&listing2_record.key, &b1_rec2.key)
            .await?;
        bidder2
            .coordinator
            .broadcast_bid_announcement(&listing2_record.key, &b2_rec2.key)
            .await?;
        tokio::time::sleep(Duration::from_secs(2)).await;

        eprintln!("[E2E] Auction 2: polling for MPC completion (max 2400s)...");
        let mpc2_start = tokio::time::Instant::now();
        loop {
            if bidder1
                .coordinator
                .get_decryption_key(&listing2_record.key)
                .await
                .is_some()
            {
                eprintln!(
                    "[E2E] Auction 2: bidder1 got key after {:?}",
                    mpc2_start.elapsed()
                );
                break;
            }
            if mpc2_start.elapsed() > Duration::from_secs(2400) {
                return Err(MarketError::Timeout(
                    "Auction 2 MPC did not complete within 2400s".into(),
                ));
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }

        let a2_b1_key = bidder1
            .coordinator
            .get_decryption_key(&listing2_record.key)
            .await;
        let a2_b2_key = bidder2
            .coordinator
            .get_decryption_key(&listing2_record.key)
            .await;
        assert!(a2_b1_key.is_some(), "Auction 2: winner should have key");
        assert!(a2_b2_key.is_none(), "Auction 2: loser should NOT have key");

        eprintln!("[E2E] ═══ AUCTION 2 PASSED ═══");
        let _ = seller1.shutdown().await;
        let _ = seller2.shutdown().await;
        let _ = bidder1.shutdown().await;
        let _ = bidder2.shutdown().await;

        Ok::<_, MarketError>(())
    })
    .await;
}

// ── N-party MPC helper ──────────────────────────────────────────────

/// Run an N-party MPC auction test.
///
/// Creates 1 seller + `num_bidders` bidder nodes, places bids, and
/// verifies that the highest bidder receives the decryption key.
///
/// `base_offset`: starting offset for Veilid nodes (must leave room
/// for `num_bidders + 1` consecutive offsets within 1.2.3.x range).
#[allow(clippy::too_many_lines)]
async fn run_n_party_mpc_test(
    num_bidders: usize,
    base_offset: u16,
    mpc_wait_secs: u64,
) -> Result<(), MarketError> {
    use market::veilid::listing_ops::ListingOperations;

    let num_parties = num_bidders + 1; // seller + bidders
    eprintln!(
        "[E2E] Starting {num_parties}-party MPC test (offsets {base_offset}..={})",
        base_offset + num_parties as u16 - 1
    );

    // Create seller
    let mut seller = E2EParticipant::new(base_offset).await?;
    let seller_id = seller.node_id().unwrap();
    let seller_signing = seller.signing_pubkey_bytes();
    let seller_dht = seller.dht().unwrap();

    // Create bidders
    let mut bidders = Vec::with_capacity(num_bidders);
    for i in 0..num_bidders {
        let offset = base_offset + 1 + i as u16;
        let b = E2EParticipant::new(offset).await?;
        bidders.push(b);
    }

    // Create listing
    eprintln!("[E2E] Creating listing DHT record...");
    let listing_record = seller_dht.create_dht_record().await?;
    // Deadline = created_at + auction_duration.  With many parties, DHT setup
    // can take minutes (each node does DHT creates/writes).  Make the duration
    // long enough so the deadline fires AFTER setup completes.
    let auction_duration_secs = 120 + (num_bidders as u64) * 60;
    let listing = create_encrypted_listing(
        listing_record.key.clone(),
        seller_id.clone(),
        100,
        auction_duration_secs,
        "N-party MPC test content",
    );

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

    // Seller places reserve-price bid
    let seller_nonce: [u8; 32] = rand::random();
    seller
        .bid_storage
        .store_bid(&listing_record.key, 100, seller_nonce)
        .await;
    let seller_bid_rec = seller_dht.create_dht_record().await?;
    let seller_bid = BidRecord {
        listing_key: listing_record.key.clone(),
        bidder: seller_id.clone(),
        commitment: make_real_commitment(100, &seller_nonce),
        timestamp: now,
        bid_key: seller_bid_rec.key.clone(),
        signing_pubkey: seller_signing,
    };
    let seller_bid_ops = BidOperations::new(seller_dht.clone());
    seller_bid_ops
        .register_bid(&listing_record, seller_bid.clone())
        .await?;
    seller_dht
        .set_dht_value(&seller_bid_rec, seller_bid.to_cbor()?)
        .await?;
    seller
        .bid_storage
        .store_bid_key(&listing_record.key, &seller_bid_rec.key)
        .await;
    seller
        .coordinator
        .add_own_bid_to_registry(
            &listing_record.key,
            seller_id.clone(),
            seller_bid_rec.key.clone(),
            now,
        )
        .await?;

    eprintln!("[E2E] Seller bid placed. Placing {num_bidders} bidder bids...");
    // Each bidder places a bid with increasing values: 200, 300, 400, ...
    // so the LAST bidder is always the winner.
    let mut bid_rec_keys = Vec::with_capacity(num_bidders);
    for (i, bidder) in bidders.iter().enumerate() {
        let bid_value = 200 + (i as u64) * 100;
        let bidder_id = bidder.node_id().unwrap();
        let bidder_signing = bidder.signing_pubkey_bytes();
        let bidder_dht = bidder.dht().unwrap();

        let nonce: [u8; 32] = rand::random();
        bidder
            .bid_storage
            .store_bid(&listing_record.key, bid_value, nonce)
            .await;
        let b_rec = bidder_dht.create_dht_record().await?;
        let bid = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: bidder_id.clone(),
            commitment: make_real_commitment(bid_value, &nonce),
            timestamp: now + 1 + i as u64,
            bid_key: b_rec.key.clone(),
            signing_pubkey: bidder_signing,
        };
        seller_bid_ops
            .register_bid(&listing_record, bid.clone())
            .await?;
        bidder
            .bid_storage
            .store_bid_key(&listing_record.key, &b_rec.key)
            .await;
        bidder_dht.set_dht_value(&b_rec, bid.to_cbor()?).await?;
        bid_rec_keys.push(b_rec.key.clone());
    }

    eprintln!("[E2E] All bidder bids placed. Adding to seller's DHT registry...");
    // Add all bidder bids to seller's DHT registry (simulates broadcast
    // receipt) so the registry is complete before the deadline fires.
    for (i, bidder) in bidders.iter().enumerate() {
        let bidder_id = bidder.node_id().unwrap();
        seller
            .coordinator
            .add_own_bid_to_registry(
                &listing_record.key,
                bidder_id.clone(),
                bid_rec_keys[i].clone(),
                now + 1 + i as u64,
            )
            .await?;
    }

    eprintln!("[E2E] DHT registry updated. Registering local bid announcements...");
    // Register local bid announcements on all participants so each
    // knows about every other bid (simulates broadcast receipt).
    // Seller learns about all bidders:
    for (i, bidder) in bidders.iter().enumerate() {
        let bidder_id = bidder.node_id().unwrap();
        seller
            .coordinator
            .register_local_bid(
                &listing_record.key,
                bidder_id.clone(),
                bid_rec_keys[i].clone(),
            )
            .await;
    }
    // Each bidder learns about seller + all other bidders:
    for (i, bidder) in bidders.iter().enumerate() {
        bidder
            .coordinator
            .register_local_bid(
                &listing_record.key,
                seller_id.clone(),
                seller_bid_rec.key.clone(),
            )
            .await;
        for (j, other) in bidders.iter().enumerate() {
            if i != j {
                let other_id = other.node_id().unwrap();
                bidder
                    .coordinator
                    .register_local_bid(
                        &listing_record.key,
                        other_id.clone(),
                        bid_rec_keys[j].clone(),
                    )
                    .await;
            }
        }
    }

    listing_ops
        .update_listing(&listing_record, &listing)
        .await?;

    eprintln!("[E2E] Watching listing from all bidders...");
    // Watch listing from all bidders
    for bidder in &bidders {
        bidder.coordinator.watch_listing(listing.to_public()).await;
    }

    eprintln!("[E2E] Waiting for broadcast routes...");
    // Wait for broadcast routes to be visible
    let first_bidder_id = bidders[0].node_id().unwrap().to_string();
    let expected_peers = num_parties - 1;
    assert!(
        wait_for_broadcast_routes(
            &bidders[0].coordinator,
            &first_bidder_id,
            expected_peers,
            90,
        )
        .await,
        "Broadcast routes not ready within timeout"
    );

    // All bidders broadcast their bid announcements
    for (i, bidder) in bidders.iter().enumerate() {
        bidder
            .coordinator
            .broadcast_bid_announcement(&listing_record.key, &bid_rec_keys[i])
            .await?;
    }
    tokio::time::sleep(Duration::from_secs(15)).await;

    eprintln!(
        "[E2E] All {num_bidders} bids placed, announcements broadcast. Starting monitoring..."
    );
    eprintln!("[E2E] Polling for MPC completion (max {mpc_wait_secs}s)...");

    // Poll for decryption key on the last bidder (highest bid = winner)
    let winner_idx = num_bidders - 1;
    let mpc_start = tokio::time::Instant::now();
    let mpc_max_wait = Duration::from_secs(mpc_wait_secs);
    loop {
        let key = bidders[winner_idx]
            .coordinator
            .get_decryption_key(&listing_record.key)
            .await;
        if key.is_some() {
            eprintln!(
                "[E2E] Winner (bidder{}) received decryption key after {:?}",
                winner_idx + 1,
                mpc_start.elapsed()
            );
            break;
        }
        if mpc_start.elapsed() > mpc_max_wait {
            eprintln!(
                "[E2E] MPC did not complete within {mpc_wait_secs}s, checking results anyway..."
            );
            break;
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    // Verify winner got key, non-winners didn't
    let winner_key = bidders[winner_idx]
        .coordinator
        .get_decryption_key(&listing_record.key)
        .await;

    for (i, bidder) in bidders.iter().enumerate() {
        if i == winner_idx {
            continue;
        }
        let key = bidder
            .coordinator
            .get_decryption_key(&listing_record.key)
            .await;
        eprintln!("[E2E] Bidder{} decryption key: {}", i + 1, key.is_some());
        assert!(
            key.is_none(),
            "Non-winner bidder{} should NOT have decryption key",
            i + 1
        );
    }

    if winner_key.is_some() {
        eprintln!("[E2E] {num_parties}-party MPC + post-MPC flow completed!");
    } else {
        // Shutdown all nodes before returning error
        let _ = seller.shutdown().await;
        for b in &mut bidders {
            let _ = b.shutdown().await;
        }
        return Err(MarketError::Timeout(format!(
            "{num_parties}-party MPC did not complete: winner did not receive decryption key"
        )));
    }

    let _ = seller.shutdown().await;
    for b in &mut bidders {
        let _ = b.shutdown().await;
    }

    eprintln!("[E2E] {num_parties}-party MPC test PASSED");
    Ok(())
}

/// 5-party MPC auction test (1 seller + 4 bidders).
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

    // 5 nodes at offsets 20-24 (IPs 1.2.3.21-1.2.3.25)
    // mpc_wait must exceed auction_duration (120 + 4*60 = 360s) + MPC execution time
    run_e2e_test("test_e2e_scale_mpc_5_party", 3600, || async {
        run_n_party_mpc_test(4, 20, 2400).await
    })
    .await;
}

/// 10-party MPC auction test (1 seller + 9 bidders).
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

    // 10 nodes at offsets 20-29 (IPs 1.2.3.21-1.2.3.30)
    run_e2e_test("test_e2e_scale_mpc_10_party", 5400, || async {
        run_n_party_mpc_test(9, 20, 3600).await
    })
    .await;
}
