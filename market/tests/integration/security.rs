//! Security and adversarial scenario integration tests.

use market::mocks::{make_test_public_key, make_test_record_key, MockTime};
use market::veilid::verify_commitment;
use market::{BidIndex, BidRecord};

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
    };
    let bid2 = BidRecord {
        listing_key: listing_key.clone(),
        bidder: make_test_public_key(2),
        commitment: [2u8; 32],
        timestamp: 2000,
        bid_key: make_test_record_key(20),
    };
    let bid3 = BidRecord {
        listing_key: listing_key.clone(),
        bidder: make_test_public_key(3),
        commitment: [3u8; 32],
        timestamp: 3000,
        bid_key: make_test_record_key(30),
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
    let bidder1 = make_test_public_key(1);
    let bidder2 = make_test_public_key(2);
    let bidder3 = make_test_public_key(3);

    assert_eq!(
        index_a.get_party_id(&bidder1),
        index_b.get_party_id(&bidder1)
    );
    assert_eq!(
        index_a.get_party_id(&bidder2),
        index_b.get_party_id(&bidder2)
    );
    assert_eq!(
        index_a.get_party_id(&bidder3),
        index_b.get_party_id(&bidder3)
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
    };
    let bidder_a = BidRecord {
        listing_key: listing_key.clone(),
        bidder: make_test_public_key(2),
        commitment: [2u8; 32],
        timestamp: 500,
        bid_key: make_test_record_key(20),
    };
    let bidder_b = BidRecord {
        listing_key: listing_key.clone(),
        bidder: make_test_public_key(3),
        commitment: [3u8; 32],
        timestamp: 600,
        bid_key: make_test_record_key(30),
    };

    let mut index = BidIndex::new(listing_key);
    // Add in non-chronological order
    index.add_bid_with_time(bidder_b, &time);
    index.add_bid_with_time(seller_bid, &time);
    index.add_bid_with_time(bidder_a, &time);

    // Seller should always be party 0 (earliest timestamp)
    assert_eq!(
        index.get_party_id(&make_test_public_key(1)),
        Some(0),
        "Seller (earliest timestamp) should be party 0"
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
