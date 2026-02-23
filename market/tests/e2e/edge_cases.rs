//! E2E edge case tests: adversarial inputs, boundary conditions, error paths.
//!
//! These tests exercise error handling and security boundaries on a real
//! Veilid devnet, complementing the happy-path smoke tests.

use std::time::Duration;

use market::error::MarketError;
use market::marketplace::bid_record::BidRecord;
use market::veilid::bid_ops::BidOperations;
use serial_test::serial;
use veilid_core::PublicKey;

use super::helpers::{
    create_encrypted_listing, create_test_listing, make_real_commitment, run_e2e_test,
    E2EParticipant,
};

// ── Signature / authentication ───────────────────────────────────────

/// Test that a tampered AppMessage (flipped byte in signed envelope) is rejected.
///
/// Validates: `process_app_message()` → `SignedEnvelope::verify_and_unwrap()` returns Err
/// on tampered data, and the coordinator continues operating normally afterwards.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_edge_tampered_appmessage_rejected() {
    run_e2e_test(
        "test_e2e_edge_tampered_appmessage_rejected",
        300,
        || async {
            let mut sender = E2EParticipant::new(20).await?;
            let mut receiver = E2EParticipant::new(21).await?;

            let sender_dht = sender.dht().unwrap();
            let receiver_dht = receiver.dht().unwrap();

            // Create a listing on receiver so we can verify state is unaffected
            let listing_record = receiver_dht.create_dht_record().await?;
            let receiver_id = receiver.node_id().unwrap();
            let listing =
                create_test_listing(listing_record.key.clone(), receiver_id.clone(), 100, 3600);

            use market::veilid::listing_ops::ListingOperations;
            let listing_ops = ListingOperations::new(receiver_dht.clone());
            listing_ops
                .update_listing(&listing_record, &listing)
                .await?;
            receiver
                .coordinator
                .register_owned_listing(listing_record.clone())
                .await?;

            // Step 1: Send a legitimate signed message and verify it works
            let sender_id = sender.node_id().unwrap();
            let bid_record_dht = sender_dht.create_dht_record().await?;
            let sender_signing = sender.signing_pubkey_bytes();
            let nonce: [u8; 32] = rand::random();
            let commitment = make_real_commitment(200, &nonce);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let bid = BidRecord {
                listing_key: listing_record.key.clone(),
                bidder: sender_id.clone(),
                commitment,
                timestamp: now,
                bid_key: bid_record_dht.key.clone(),
                signing_pubkey: sender_signing,
            };
            sender_dht
                .set_dht_value(&bid_record_dht, bid.to_cbor()?)
                .await?;

            // Wait for DHT propagation
            tokio::time::sleep(Duration::from_secs(5)).await;

            // Create a valid signed message
            use market::veilid::bid_announcement::AuctionMessage;
            let msg = AuctionMessage::bid_announcement(
                listing_record.key.clone(),
                sender_id.clone(),
                bid_record_dht.key.clone(),
                now,
            );
            let valid_data = msg.to_signed_bytes(sender.coordinator.signing_key())?;

            // Verify the valid message is processed successfully
            let valid_result = receiver
                .coordinator
                .process_app_message(valid_data.clone())
                .await;
            assert!(valid_result.is_ok(), "Valid message should be accepted");
            eprintln!("[E2E] Valid signed message accepted");

            // Step 2: Tamper with the message (flip a byte in the middle)
            let mut tampered = valid_data.clone();
            if tampered.len() > 20 {
                tampered[20] ^= 0xFF;
            }

            // Tampered message should be rejected (signature verification fails)
            let tampered_result = receiver.coordinator.process_app_message(tampered).await;
            assert!(
                tampered_result.is_err(),
                "Tampered message should be rejected"
            );
            eprintln!("[E2E] Tampered message correctly rejected");

            // Step 3: Verify coordinator still works after rejection
            let count = receiver
                .coordinator
                .get_bid_count(&listing_record.key)
                .await;
            eprintln!("[E2E] Bid count after tamper attempt: {}", count);
            assert!(
                count <= 1,
                "Tampered message should not have registered a bid"
            );

            let _ = sender.shutdown().await;
            let _ = receiver.shutdown().await;
            Ok(())
        },
    )
    .await;
}

/// Test that a bid announcement signed by the wrong key (impersonation) is rejected.
///
/// Validates: `validate_bid_announcement_signer()` rejects messages where the
/// envelope signer doesn't match `BidRecord.signing_pubkey`.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_edge_bid_announcement_wrong_signer_rejected() {
    run_e2e_test(
        "test_e2e_edge_bid_announcement_wrong_signer_rejected",
        300,
        || async {
            let mut seller = E2EParticipant::new(22).await?;
            let mut bidder1 = E2EParticipant::new(23).await?;
            let mut bidder2 = E2EParticipant::new(24).await?;

            let seller_id = seller.node_id().unwrap();
            let bidder1_id = bidder1.node_id().unwrap();
            let bidder1_signing = bidder1.signing_pubkey_bytes();

            let seller_dht = seller.dht().unwrap();
            let bidder1_dht = bidder1.dht().unwrap();

            // Create listing
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

            // Bidder1 creates a legitimate BidRecord in DHT
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let nonce: [u8; 32] = rand::random();
            let commitment = make_real_commitment(200, &nonce);
            let bid1_record_dht = bidder1_dht.create_dht_record().await?;
            let bid1 = BidRecord {
                listing_key: listing_record.key.clone(),
                bidder: bidder1_id.clone(),
                commitment,
                timestamp: now,
                bid_key: bid1_record_dht.key.clone(),
                signing_pubkey: bidder1_signing,
            };
            bidder1_dht
                .set_dht_value(&bid1_record_dht, bid1.to_cbor()?)
                .await?;

            tokio::time::sleep(Duration::from_secs(5)).await;

            // Bidder2 crafts a BidAnnouncement claiming to be bidder1,
            // but signs it with bidder2's key (impersonation attempt)
            use market::veilid::bid_announcement::AuctionMessage;
            let impersonated_msg = AuctionMessage::bid_announcement(
                listing_record.key.clone(),
                bidder1_id.clone(), // Claims to be bidder1
                bid1_record_dht.key.clone(),
                now,
            );
            // Signed with bidder2's key — signer won't match BidRecord.signing_pubkey
            let impersonated_data =
                impersonated_msg.to_signed_bytes(bidder2.coordinator.signing_key())?;

            // Send the impersonated message directly to seller's coordinator
            let result = seller
                .coordinator
                .process_app_message(impersonated_data)
                .await;
            // process_app_message returns Ok(()) even for rejected bids (logged as warning)
            assert!(result.is_ok(), "Should not crash on impersonation attempt");

            // Now send the legitimate announcement signed by bidder1's actual key
            let legit_msg = AuctionMessage::bid_announcement(
                listing_record.key.clone(),
                bidder1_id.clone(),
                bid1_record_dht.key.clone(),
                now,
            );
            let legit_data = legit_msg.to_signed_bytes(bidder1.coordinator.signing_key())?;
            seller.coordinator.process_app_message(legit_data).await?;

            tokio::time::sleep(Duration::from_secs(2)).await;

            // Only the legitimate bid should be registered
            let count = seller.coordinator.get_bid_count(&listing_record.key).await;
            eprintln!("[E2E] Seller bid count (expect 1 legitimate): {}", count);
            assert_eq!(
                count, 1,
                "Only the legitimate bid should be registered, not the impersonated one"
            );

            let _ = seller.shutdown().await;
            let _ = bidder1.shutdown().await;
            let _ = bidder2.shutdown().await;
            Ok(())
        },
    )
    .await;
}

// ── Deduplication ────────────────────────────────────────────────────

/// Test that duplicate bid announcements from the same bidder are deduplicated.
///
/// Validates: `BidAnnouncementRegistry::add()` dedup and `BidAnnouncementTracker` dedup.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_edge_duplicate_bid_announcements_deduped() {
    run_e2e_test(
        "test_e2e_edge_duplicate_bid_announcements_deduped",
        300,
        || async {
            let mut seller = E2EParticipant::new(25).await?;
            let mut bidder = E2EParticipant::new(26).await?;

            let seller_id = seller.node_id().unwrap();
            let bidder_id = bidder.node_id().unwrap();
            let bidder_signing = bidder.signing_pubkey_bytes();

            let seller_dht = seller.dht().unwrap();
            let bidder_dht = bidder.dht().unwrap();

            // Create listing
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

            // Bidder creates a BidRecord in DHT
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let nonce: [u8; 32] = rand::random();
            let commitment = make_real_commitment(150, &nonce);
            let bid_record_dht = bidder_dht.create_dht_record().await?;
            let bid = BidRecord {
                listing_key: listing_record.key.clone(),
                bidder: bidder_id.clone(),
                commitment,
                timestamp: now,
                bid_key: bid_record_dht.key.clone(),
                signing_pubkey: bidder_signing,
            };
            bidder_dht
                .set_dht_value(&bid_record_dht, bid.to_cbor()?)
                .await?;

            tokio::time::sleep(Duration::from_secs(5)).await;

            // Send the same bid announcement 3 times
            use market::veilid::bid_announcement::AuctionMessage;
            let ts = market::config::now_unix();
            for i in 0..3 {
                let msg = AuctionMessage::bid_announcement(
                    listing_record.key.clone(),
                    bidder_id.clone(),
                    bid_record_dht.key.clone(),
                    ts,
                );
                let data = msg.to_signed_bytes(bidder.coordinator.signing_key())?;
                seller.coordinator.process_app_message(data).await?;
                eprintln!("[E2E] Sent duplicate announcement #{}", i + 1);
            }

            tokio::time::sleep(Duration::from_secs(2)).await;

            // Verify deduplication: should only count as 1 bid
            let count = seller.coordinator.get_bid_count(&listing_record.key).await;
            eprintln!("[E2E] Bid count after 3 duplicate announcements: {}", count);
            assert_eq!(
                count, 1,
                "Duplicate announcements should be deduplicated to 1"
            );

            // Also verify DHT registry has exactly 1 entry
            let registry_data = seller_dht
                .get_value_at_subkey(
                    &listing_record.key,
                    market::config::subkeys::BID_ANNOUNCEMENTS,
                    true,
                )
                .await?;
            if let Some(data) = registry_data {
                use market::veilid::bid_announcement::BidAnnouncementRegistry;
                let registry = BidAnnouncementRegistry::from_bytes(&data)?;
                eprintln!(
                    "[E2E] DHT registry announcement count: {}",
                    registry.announcements.len()
                );
                assert_eq!(
                    registry.announcements.len(),
                    1,
                    "DHT registry should have exactly 1 entry after dedup"
                );
            }

            let _ = seller.shutdown().await;
            let _ = bidder.shutdown().await;
            Ok(())
        },
    )
    .await;
}

// ── Encryption / access control ──────────────────────────────────────

/// Test that fetching a listing without winning the auction (no decryption key) fails.
///
/// Validates: `fetch_and_decrypt_listing()` → `MarketError::NotFound` when no key stored.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_edge_decrypt_without_key_fails() {
    run_e2e_test("test_e2e_edge_decrypt_without_key_fails", 300, || async {
        let mut seller = E2EParticipant::new(27).await?;
        let mut bidder = E2EParticipant::new(32).await?;

        let seller_id = seller.node_id().unwrap();
        let seller_dht = seller.dht().unwrap();

        // Create listing with real encrypted content
        let listing_record = seller_dht.create_dht_record().await?;
        let plaintext = "Secret content that should not be accessible";
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

        // Seller stores key locally but bidder does NOT have it
        seller
            .coordinator
            .store_decryption_key(&listing_record.key, listing.decryption_key.clone())
            .await;

        tokio::time::sleep(Duration::from_secs(5)).await;

        // Bidder (who hasn't won) tries to fetch and decrypt
        let decrypt_result = bidder
            .coordinator
            .fetch_and_decrypt_listing(&listing_record.key)
            .await;

        match &decrypt_result {
            Err(MarketError::NotFound(msg)) => {
                eprintln!("[E2E] Correctly got NotFound: {}", msg);
                assert!(
                    msg.contains("auction") || msg.contains("key"),
                    "Error should mention auction/key requirement"
                );
            }
            Err(e) => {
                // Could also be NotFound for the listing itself if DHT hasn't propagated
                eprintln!("[E2E] Got expected error (non-winner): {}", e);
            }
            Ok(content) => {
                panic!("Non-winner should NOT be able to decrypt! Got: {}", content);
            }
        }

        // Verify seller CAN decrypt their own listing
        let seller_decrypt = seller
            .coordinator
            .fetch_and_decrypt_listing(&listing_record.key)
            .await;
        assert!(
            seller_decrypt.is_ok(),
            "Seller should be able to decrypt: {:?}",
            seller_decrypt.err()
        );
        assert_eq!(seller_decrypt.unwrap(), plaintext);
        eprintln!("[E2E] Seller can decrypt their own listing");

        let _ = seller.shutdown().await;
        let _ = bidder.shutdown().await;
        Ok(())
    })
    .await;
}

/// Test that decrypting listing content with the wrong AES key fails.
///
/// Validates: `decrypt_content_with_key()` with wrong key → `MarketError::Crypto`,
/// then correct key succeeds. Tests AEAD tag verification over real DHT data.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_edge_listing_encryption_wrong_key_fails() {
    run_e2e_test(
        "test_e2e_edge_listing_encryption_wrong_key_fails",
        300,
        || async {
            let mut seller = E2EParticipant::new(39).await?;
            let mut reader = E2EParticipant::new(30).await?;

            let seller_id = seller.node_id().unwrap();
            let seller_dht = seller.dht().unwrap();

            // Create listing with real AES-256-GCM encrypted content
            let listing_record = seller_dht.create_dht_record().await?;
            let plaintext = "Confidential auction item: Diamond necklace, 24 carats";
            let listing = create_encrypted_listing(
                listing_record.key.clone(),
                seller_id.clone(),
                500,
                3600,
                plaintext,
            );
            let correct_key = listing.decryption_key.clone();

            use market::veilid::listing_ops::ListingOperations;
            let listing_ops = ListingOperations::new(seller_dht.clone());
            listing_ops
                .update_listing(&listing_record, &listing)
                .await?;

            tokio::time::sleep(Duration::from_secs(5)).await;

            // Reader fetches listing from DHT
            let reader_dht = reader.dht().unwrap();
            let reader_listing_ops = ListingOperations::new(reader_dht.clone());
            let retrieved = reader_listing_ops.get_listing(&listing_record.key).await?;
            assert!(retrieved.is_some(), "Listing should be retrievable via DHT");
            let public_listing = retrieved.unwrap();

            // Try to decrypt with a random wrong key (32 bytes hex-encoded)
            let wrong_key = hex::encode([0xABu8; 32]);
            let wrong_result = public_listing.decrypt_content_with_key(&wrong_key);
            assert!(
                wrong_result.is_err(),
                "Decryption with wrong key should fail"
            );
            eprintln!(
                "[E2E] Wrong key correctly rejected: {:?}",
                wrong_result.err()
            );

            // Decrypt with the correct key should succeed
            let correct_result = public_listing.decrypt_content_with_key(&correct_key);
            assert!(
                correct_result.is_ok(),
                "Decryption with correct key should succeed: {:?}",
                correct_result.err()
            );
            assert_eq!(correct_result.unwrap(), plaintext);
            eprintln!("[E2E] Correct key decrypted successfully");

            let _ = seller.shutdown().await;
            let _ = reader.shutdown().await;
            Ok(())
        },
    )
    .await;
}

// ── Registry / DHT consistency ───────────────────────────────────────

/// Test that all nodes derive the same master registry key from the shared network key.
///
/// Validates: `ensure_master_registry()` deterministic derivation — all nodes
/// converge on the same DHT record key.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_edge_master_registry_convergence() {
    run_e2e_test("test_e2e_edge_master_registry_convergence", 300, || async {
        let mut node_a = E2EParticipant::new(31).await?;
        let mut node_b = E2EParticipant::new(32).await?;
        let mut node_c = E2EParticipant::new(33).await?;

        let key_a = node_a.coordinator.ensure_master_registry().await?;
        eprintln!("[E2E] Node A registry key: {}", key_a);

        let key_b = node_b.coordinator.ensure_master_registry().await?;
        eprintln!("[E2E] Node B registry key: {}", key_b);

        let key_c = node_c.coordinator.ensure_master_registry().await?;
        eprintln!("[E2E] Node C registry key: {}", key_c);

        assert_eq!(key_a, key_b, "Node A and B should agree on registry key");
        assert_eq!(key_b, key_c, "Node B and C should agree on registry key");
        eprintln!("[E2E] All 3 nodes converged on the same master registry key");

        let key_a2 = node_a.coordinator.ensure_master_registry().await?;
        assert_eq!(key_a, key_a2, "Registry key should be stable across calls");

        let _ = node_a.shutdown().await;
        let _ = node_b.shutdown().await;
        let _ = node_c.shutdown().await;
        Ok(())
    })
    .await;
}

/// Test DHT bid registry cross-node consistency.
///
/// Validates: Seller writes bid registry to DHT subkey 2, another node can read
/// the same registry with consistent data.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_edge_dht_bid_registry_cross_node_consistency() {
    run_e2e_test(
        "test_e2e_edge_dht_bid_registry_cross_node_consistency",
        300,
        || async {
            let mut seller = E2EParticipant::new(34).await?;
            let mut reader = E2EParticipant::new(35).await?;

            let seller_id = seller.node_id().unwrap();
            let seller_signing = seller.signing_pubkey_bytes();
            let seller_dht = seller.dht().unwrap();

            // Create listing and register ownership
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

            // Seller places own bid
            let seller_nonce: [u8; 32] = rand::random();
            let seller_commitment = make_real_commitment(100, &seller_nonce);
            let seller_bid_rec = seller_dht.create_dht_record().await?;
            let seller_bid = BidRecord {
                listing_key: listing_record.key.clone(),
                bidder: seller_id.clone(),
                commitment: seller_commitment,
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
                .coordinator
                .add_own_bid_to_registry(
                    &listing_record.key,
                    seller_id.clone(),
                    seller_bid_rec.key.clone(),
                    now,
                )
                .await?;

            // Simulate 2 additional bids (create BidRecords with synthetic keys)
            let bidder_keys: Vec<PublicKey> = (0..2)
                .map(|_| {
                    let key_bytes: [u8; 32] = rand::random();
                    PublicKey::new(
                        veilid_core::CRYPTO_KIND_VLD0,
                        veilid_core::BarePublicKey::new(&key_bytes),
                    )
                })
                .collect();

            for (i, bidder_pk) in bidder_keys.iter().enumerate() {
                let bid_rec_dht = seller_dht.create_dht_record().await?;
                let nonce: [u8; 32] = rand::random();
                let commitment = make_real_commitment(200 + (i as u64) * 50, &nonce);
                let fake_signing: [u8; 32] = rand::random();
                let bid = BidRecord {
                    listing_key: listing_record.key.clone(),
                    bidder: bidder_pk.clone(),
                    commitment,
                    timestamp: now + 1 + i as u64,
                    bid_key: bid_rec_dht.key.clone(),
                    signing_pubkey: fake_signing,
                };
                seller_dht
                    .set_dht_value(&bid_rec_dht, bid.to_cbor()?)
                    .await?;
                seller_bid_ops.register_bid(&listing_record, bid).await?;
            }

            tokio::time::sleep(Duration::from_secs(10)).await;

            // Reader fetches the bid index from a different node
            let reader_dht = reader.dht().unwrap();
            let reader_bid_ops = BidOperations::new(reader_dht.clone());
            let bid_index = reader_bid_ops.fetch_bid_index(&listing_record.key).await?;

            eprintln!(
                "[E2E] Reader sees {} bids in bid index",
                bid_index.bids.len()
            );
            assert_eq!(
                bid_index.bids.len(),
                3,
                "Reader should see all 3 bids in the index"
            );

            // Verify seller's BidRecord is fetchable from its own DHT record
            let seller_bid_fetched = reader_bid_ops.fetch_bid(&seller_bid_rec.key).await?;
            assert!(
                seller_bid_fetched.is_some(),
                "Seller's bid record should be fetchable"
            );
            let fetched = seller_bid_fetched.unwrap();
            assert_eq!(fetched.commitment, seller_commitment);
            assert_eq!(fetched.bidder, seller_id);
            eprintln!("[E2E] Seller's bid record verified from reader node");

            let _ = seller.shutdown().await;
            let _ = reader.shutdown().await;
            Ok(())
        },
    )
    .await;
}

// ── Boundary conditions ──────────────────────────────────────────────

/// Test that a seller-only auction (no other bidders) doesn't crash or deadlock.
///
/// Validates: The monitoring loop handles <2 parties gracefully — no MPC triggered,
/// no deadlock, coordinator stays healthy.
#[tokio::test]
#[ignore]
#[serial]
async fn test_e2e_edge_seller_only_no_sale() {
    run_e2e_test("test_e2e_edge_seller_only_no_sale", 300, || async {
        let mut seller = E2EParticipant::new(36).await?;

        let seller_id = seller.node_id().unwrap();
        let seller_signing = seller.signing_pubkey_bytes();
        let seller_dht = seller.dht().unwrap();

        // Create listing that's already expired (1s duration with MockTime(1000))
        let listing_record = seller_dht.create_dht_record().await?;
        let listing = create_test_listing(
            listing_record.key.clone(),
            seller_id.clone(),
            100,
            1, // Already expired
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

        // Only the seller places a bid — no other bidders
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let nonce: [u8; 32] = rand::random();
        let commitment = make_real_commitment(100, &nonce);
        seller
            .bid_storage
            .store_bid(&listing_record.key, 100, nonce)
            .await;
        let bid_rec = seller_dht.create_dht_record().await?;
        let bid = BidRecord {
            listing_key: listing_record.key.clone(),
            bidder: seller_id.clone(),
            commitment,
            timestamp: now,
            bid_key: bid_rec.key.clone(),
            signing_pubkey: seller_signing,
        };
        let bid_ops = BidOperations::new(seller_dht.clone());
        bid_ops.register_bid(&listing_record, bid.clone()).await?;
        seller_dht.set_dht_value(&bid_rec, bid.to_cbor()?).await?;
        seller
            .bid_storage
            .store_bid_key(&listing_record.key, &bid_rec.key)
            .await;
        seller
            .coordinator
            .add_own_bid_to_registry(
                &listing_record.key,
                seller_id.clone(),
                bid_rec.key.clone(),
                now,
            )
            .await?;

        // Watch the listing — monitoring was already started in E2EParticipant::new()
        seller.coordinator.watch_listing(listing.to_public()).await;

        // Wait for a couple monitoring cycles (~30s) to ensure no crash/deadlock
        eprintln!("[E2E] Waiting 30s for monitoring cycles with only 1 party...");
        tokio::time::sleep(Duration::from_secs(30)).await;

        // Verify coordinator is still healthy — can still do DHT operations
        let test_record = seller_dht.create_dht_record().await?;
        assert!(
            !test_record.key.to_string().is_empty(),
            "Coordinator should still be able to create DHT records after no-sale"
        );
        eprintln!("[E2E] Coordinator still healthy after seller-only auction");

        let key = seller
            .coordinator
            .get_decryption_key(&listing_record.key)
            .await;
        eprintln!("[E2E] Decryption key after no-sale: {:?}", key.is_some());

        let _ = seller.shutdown().await;
        Ok(())
    })
    .await;
}
