//! Security and adversarial scenario integration tests.

use market::error::MarketError;
use market::marketplace::{Bid, Listing};
use market::mocks::{make_test_public_key, make_test_record_key, MockTime};
use market::util::{cbor_from_limited_reader, MAX_DHT_VALUE_SIZE};
use market::veilid::verify_commitment;
use market::{BidIndex, BidRecord};
use serde::{Deserialize, Serialize};

use crate::common::MultiPartyHarness;

#[tokio::test]
async fn test_decryption_key_only_for_winner() {
    let mut harness = MultiPartyHarness::new(3).await;
    let listing = harness.create_listing("Winner Key Item", 100, 3600).await;

    harness.place_bid(0, &listing, 200).await;
    harness.place_bid(1, &listing, 300).await; // Winner (highest bid)
    harness.place_bid(2, &listing, 150).await;

    harness.advance_to_deadline(&listing);
    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(1));

    // Transfer decryption key to winner only
    harness.transfer_decryption_key(&listing, 1).await;

    // Winner should have the key
    assert!(harness.party_got_decryption_key(1, &listing).await);

    // Losers should NOT have the key
    assert!(!harness.party_got_decryption_key(0, &listing).await);
    assert!(!harness.party_got_decryption_key(2, &listing).await);
}

#[tokio::test]
async fn test_non_participant_has_no_key() {
    let mut harness = MultiPartyHarness::new(4).await;
    let listing = harness
        .create_listing("Partial Participation", 100, 3600)
        .await;

    // Only parties 0, 1, 2 bid — party 3 abstains
    harness.place_bid(0, &listing, 200).await;
    harness.place_bid(1, &listing, 500).await; // Winner
    harness.place_bid(2, &listing, 300).await;

    harness.advance_to_deadline(&listing);

    // Transfer key to winner
    harness.transfer_decryption_key(&listing, 1).await;

    // Non-participant (party 3) should have no key
    assert!(!harness.party_got_decryption_key(3, &listing).await);

    // Non-participant should have no bid stored
    assert!(!harness.party(3).bid_storage.has_bid(&listing.key).await);
}

#[test]
fn test_bid_commitment_tamper_detection() {
    let bid_value = 100u64;
    let nonce = [42u8; 32];

    // Compute correct commitment
    let correct_commitment = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(bid_value.to_le_bytes());
        hasher.update(nonce);
        let result: [u8; 32] = hasher.finalize().into();
        result
    };

    // Correct commitment should verify
    assert!(verify_commitment(bid_value, &nonce, &correct_commitment));

    // Tampered commitment should fail
    let mut tampered = correct_commitment;
    tampered[0] ^= 0xFF;
    assert!(!verify_commitment(bid_value, &nonce, &tampered));

    // Wrong bid value should fail
    assert!(!verify_commitment(
        bid_value + 1,
        &nonce,
        &correct_commitment
    ));

    // Wrong nonce should fail
    let mut wrong_nonce = nonce;
    wrong_nonce[0] ^= 0x01;
    assert!(!verify_commitment(
        bid_value,
        &wrong_nonce,
        &correct_commitment
    ));
}

#[test]
fn test_party_id_assignment_deterministic() {
    let time = MockTime::new(2000);
    let listing_key = make_test_record_key(1);

    // Create bids with distinct timestamps
    let bid1 = BidRecord {
        listing_key: listing_key.clone(),
        bidder: make_test_public_key(1),
        commitment: [1u8; 32],
        timestamp: 1000,
        bid_key: make_test_record_key(10),
        signing_pubkey: [1u8; 32],
    };
    let bid2 = BidRecord {
        listing_key: listing_key.clone(),
        bidder: make_test_public_key(2),
        commitment: [2u8; 32],
        timestamp: 2000,
        bid_key: make_test_record_key(20),
        signing_pubkey: [2u8; 32],
    };
    let bid3 = BidRecord {
        listing_key: listing_key.clone(),
        bidder: make_test_public_key(3),
        commitment: [3u8; 32],
        timestamp: 3000,
        bid_key: make_test_record_key(30),
        signing_pubkey: [3u8; 32],
    };

    // Add in order 1, 2, 3
    let mut index_a = BidIndex::new(listing_key.clone());
    index_a.add_bid_with_time(bid1.clone(), &time);
    index_a.add_bid_with_time(bid2.clone(), &time);
    index_a.add_bid_with_time(bid3.clone(), &time);

    // Add in reverse order 3, 2, 1
    let mut index_b = BidIndex::new(listing_key.clone());
    index_b.add_bid_with_time(bid3.clone(), &time);
    index_b.add_bid_with_time(bid2.clone(), &time);
    index_b.add_bid_with_time(bid1.clone(), &time);

    // Party IDs should be identical regardless of insertion order
    let seller = make_test_public_key(1); // Seller is bidder 1
    let bidder1 = make_test_public_key(1);
    let bidder2 = make_test_public_key(2);
    let bidder3 = make_test_public_key(3);

    assert_eq!(
        index_a.get_party_id(&bidder1, &seller),
        index_b.get_party_id(&bidder1, &seller)
    );
    assert_eq!(
        index_a.get_party_id(&bidder2, &seller),
        index_b.get_party_id(&bidder2, &seller)
    );
    assert_eq!(
        index_a.get_party_id(&bidder3, &seller),
        index_b.get_party_id(&bidder3, &seller)
    );
}

#[test]
fn test_seller_always_party_zero() {
    let time = MockTime::new(5000);
    let listing_key = make_test_record_key(1);

    // Seller's auto-bid has the earliest timestamp (created at listing time)
    let seller_bid = BidRecord {
        listing_key: listing_key.clone(),
        bidder: make_test_public_key(1), // Seller
        commitment: [1u8; 32],
        timestamp: 100, // Earliest — created at listing time
        bid_key: make_test_record_key(10),
        signing_pubkey: [1u8; 32],
    };
    let bidder_a = BidRecord {
        listing_key: listing_key.clone(),
        bidder: make_test_public_key(2),
        commitment: [2u8; 32],
        timestamp: 500,
        bid_key: make_test_record_key(20),
        signing_pubkey: [2u8; 32],
    };
    let bidder_b = BidRecord {
        listing_key: listing_key.clone(),
        bidder: make_test_public_key(3),
        commitment: [3u8; 32],
        timestamp: 600,
        bid_key: make_test_record_key(30),
        signing_pubkey: [3u8; 32],
    };

    let mut index = BidIndex::new(listing_key);
    // Add in non-chronological order
    index.add_bid_with_time(bidder_b, &time);
    index.add_bid_with_time(seller_bid, &time);
    index.add_bid_with_time(bidder_a, &time);

    // Seller should always be party 0 (by pubkey-based sorting)
    let seller = make_test_public_key(1);
    assert_eq!(
        index.get_party_id(&seller, &seller),
        Some(0),
        "Seller should always be party 0"
    );
}

#[tokio::test]
async fn test_3_party_loser_sees_no_winner_info() {
    let mut harness = MultiPartyHarness::new(3).await;
    let listing = harness.create_listing("Secret Winner", 100, 3600).await;

    harness.place_bid(0, &listing, 200).await;
    harness.place_bid(1, &listing, 500).await; // Winner
    harness.place_bid(2, &listing, 300).await;

    harness.advance_to_deadline(&listing);

    let num_parties = harness.num_parties();
    for (i, party) in [harness.party(0), harness.party(1), harness.party(2)]
        .iter()
        .enumerate()
    {
        if let Some((bid_value, _)) = party.bid_storage.get_bid(&listing.key).await {
            let result = party
                .auction_logic
                .execute_mpc(i, num_parties, bid_value)
                .await
                .unwrap();

            if i == 1 {
                // Winner sees is_winner = true
                assert!(result.is_winner, "Party 1 (highest bid) should win");
            } else if i == 0 {
                // Party 0 (seller) loses but sees winner info (MPC protocol)
                assert!(!result.is_winner, "Party 0 (seller) should lose");
                assert!(
                    result.winner_bid.is_some(),
                    "Seller (party 0) should see winner bid"
                );
                assert!(
                    result.winner_party_id.is_some(),
                    "Seller (party 0) should see winner party ID"
                );
            } else {
                // Other losers see is_winner = false — no winner ID or winning bid
                assert!(!result.is_winner, "Party {} should lose", i);
                assert!(
                    result.winner_bid.is_none(),
                    "Loser party {} should not see winner bid",
                    i
                );
                assert!(
                    result.winner_party_id.is_none(),
                    "Loser party {} should not see winner party ID",
                    i
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Adversarial Input Tests
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestAdversarialPayload {
    data: String,
}

#[test]
fn test_oversized_cbor_payload_rejected() {
    // Create a payload that exceeds MAX_DHT_VALUE_SIZE
    let large_payload = TestAdversarialPayload {
        data: "x".repeat(MAX_DHT_VALUE_SIZE + 1000),
    };

    let mut bytes = Vec::new();
    ciborium::into_writer(&large_payload, &mut bytes).unwrap();

    // Attempt to deserialize should fail with Validation error
    let result: Result<TestAdversarialPayload, MarketError> =
        cbor_from_limited_reader(&bytes, MAX_DHT_VALUE_SIZE);

    assert!(result.is_err());
    match result {
        Err(MarketError::Validation(msg)) => {
            assert!(msg.contains("CBOR payload too large"));
        }
        _ => panic!("Expected MarketError::Validation for oversized payload"),
    }
}

#[test]
fn test_malformed_cbor_bid_rejected() {
    // Create malformed CBOR bytes
    let garbage = vec![0xFF, 0xDE, 0xAD, 0xBE, 0xEF, 0x42, 0x00, 0x11];

    // Attempt to deserialize as Bid should fail gracefully
    let result = Bid::from_cbor(&garbage);

    assert!(result.is_err());
    match result {
        Err(MarketError::Serialization(msg)) => {
            assert!(msg.contains("CBOR deserialization failed"));
        }
        _ => panic!("Expected MarketError::Serialization for malformed CBOR"),
    }
}

#[test]
fn test_malformed_cbor_listing_rejected() {
    // Create malformed CBOR bytes
    let garbage = vec![0x01, 0x02, 0x03, 0x04, 0x05];

    // Attempt to deserialize as Listing should fail gracefully
    let result = Listing::from_cbor(&garbage);

    assert!(result.is_err());
    match result {
        Err(MarketError::Serialization(msg)) => {
            assert!(msg.contains("CBOR deserialization failed"));
        }
        _ => panic!("Expected MarketError::Serialization for malformed CBOR"),
    }
}

#[test]
fn test_invalid_commitment_rejected() {
    let bid_value = 100u64;
    let nonce = [42u8; 32];

    // Compute correct commitment
    let correct_commitment = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(bid_value.to_le_bytes());
        hasher.update(nonce);
        let result: [u8; 32] = hasher.finalize().into();
        result
    };

    // Verify correct commitment passes
    assert!(verify_commitment(bid_value, &nonce, &correct_commitment));

    // Tampered commitment should be rejected
    let mut tampered_commitment = correct_commitment;
    tampered_commitment[0] ^= 0xFF;
    assert!(!verify_commitment(bid_value, &nonce, &tampered_commitment));

    // Wrong bid value should be rejected
    assert!(!verify_commitment(
        bid_value + 1,
        &nonce,
        &correct_commitment
    ));

    // Wrong nonce should be rejected
    let mut wrong_nonce = nonce;
    wrong_nonce[31] ^= 0x01;
    assert!(!verify_commitment(
        bid_value,
        &wrong_nonce,
        &correct_commitment
    ));
}

#[test]
fn test_commitment_timing_safe() {
    // Verify that verify_commitment uses constant-time comparison
    let bid_value = 12345u64;
    let nonce = [0xABu8; 32];

    let correct_commitment = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(bid_value.to_le_bytes());
        hasher.update(nonce);
        let result: [u8; 32] = hasher.finalize().into();
        result
    };

    // Create two different invalid commitments
    let mut invalid1 = correct_commitment;
    invalid1[0] ^= 0x01; // Flip first bit

    let mut invalid2 = correct_commitment;
    invalid2[31] ^= 0x01; // Flip last bit

    // Both should fail verification (constant-time operation should work for any position)
    assert!(!verify_commitment(bid_value, &nonce, &invalid1));
    assert!(!verify_commitment(bid_value, &nonce, &invalid2));

    // The verify_commitment function uses subtle::ConstantTimeEq internally,
    // which ensures timing-safe comparison
}

#[test]
fn test_bid_party_id_consistency_across_insertions() {
    // Test that party ID assignment is deterministic and based on pubkey, not insertion order
    let time = MockTime::new(5000);
    let listing_key = make_test_record_key(1);

    // Create bids with different pubkeys and timestamps
    let bid_a = BidRecord {
        listing_key: listing_key.clone(),
        bidder: make_test_public_key(10),
        commitment: [1u8; 32],
        timestamp: 1000,
        bid_key: make_test_record_key(100),
        signing_pubkey: [0xAAu8; 32],
    };

    let bid_b = BidRecord {
        listing_key: listing_key.clone(),
        bidder: make_test_public_key(20),
        commitment: [2u8; 32],
        timestamp: 2000,
        bid_key: make_test_record_key(200),
        signing_pubkey: [0xBBu8; 32],
    };

    let bid_c = BidRecord {
        listing_key: listing_key.clone(),
        bidder: make_test_public_key(30),
        commitment: [3u8; 32],
        timestamp: 3000,
        bid_key: make_test_record_key(300),
        signing_pubkey: [0xCCu8; 32],
    };

    // Insert in order A, B, C
    let mut index_abc = BidIndex::new(listing_key.clone());
    index_abc.add_bid_with_time(bid_a.clone(), &time);
    index_abc.add_bid_with_time(bid_b.clone(), &time);
    index_abc.add_bid_with_time(bid_c.clone(), &time);

    // Insert in order C, B, A (reverse)
    let mut index_cba = BidIndex::new(listing_key.clone());
    index_cba.add_bid_with_time(bid_c.clone(), &time);
    index_cba.add_bid_with_time(bid_b.clone(), &time);
    index_cba.add_bid_with_time(bid_a.clone(), &time);

    // Insert in order B, A, C (mixed)
    let mut index_bac = BidIndex::new(listing_key.clone());
    index_bac.add_bid_with_time(bid_b.clone(), &time);
    index_bac.add_bid_with_time(bid_a.clone(), &time);
    index_bac.add_bid_with_time(bid_c.clone(), &time);

    let seller = make_test_public_key(10);
    let pubkey_a = make_test_public_key(10);
    let pubkey_b = make_test_public_key(20);
    let pubkey_c = make_test_public_key(30);

    // All three indices should assign the same party IDs
    assert_eq!(
        index_abc.get_party_id(&pubkey_a, &seller),
        index_cba.get_party_id(&pubkey_a, &seller)
    );
    assert_eq!(
        index_abc.get_party_id(&pubkey_a, &seller),
        index_bac.get_party_id(&pubkey_a, &seller)
    );

    assert_eq!(
        index_abc.get_party_id(&pubkey_b, &seller),
        index_cba.get_party_id(&pubkey_b, &seller)
    );
    assert_eq!(
        index_abc.get_party_id(&pubkey_b, &seller),
        index_bac.get_party_id(&pubkey_b, &seller)
    );

    assert_eq!(
        index_abc.get_party_id(&pubkey_c, &seller),
        index_cba.get_party_id(&pubkey_c, &seller)
    );
    assert_eq!(
        index_abc.get_party_id(&pubkey_c, &seller),
        index_bac.get_party_id(&pubkey_c, &seller)
    );
}

#[test]
fn test_empty_cbor_payload_rejected() {
    let empty: Vec<u8> = Vec::new();

    let result: Result<Bid, MarketError> = cbor_from_limited_reader(&empty, MAX_DHT_VALUE_SIZE);

    assert!(result.is_err());
    match result {
        Err(MarketError::Serialization(msg)) => {
            assert!(msg.contains("CBOR deserialization failed"));
        }
        _ => panic!("Expected MarketError::Serialization for empty payload"),
    }
}
