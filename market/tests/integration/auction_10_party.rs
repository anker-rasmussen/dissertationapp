//! 10-party auction integration tests.
//!
//! Tests with larger party counts to verify scalability.

use crate::common::MultiPartyHarness;

#[tokio::test]
async fn test_10_party_highest_bid_wins() {
    let mut harness = MultiPartyHarness::new(10).await;

    let listing = harness.create_listing("10-Party Item", 100, 3600).await;

    // Place bids with party 7 having the highest
    let bids = [150, 200, 250, 300, 350, 400, 450, 999, 500, 550];
    for (i, &bid) in bids.iter().enumerate() {
        harness.place_bid(i, &listing, bid).await;
    }

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(7)); // Party 7 bid 999
}

#[tokio::test]
async fn test_10_party_sequential_bids() {
    let mut harness = MultiPartyHarness::new(10).await;

    let listing = harness.create_listing("Sequential Item", 50, 3600).await;

    // Each party bids 100 more than their index
    for i in 0..10 {
        harness.place_bid(i, &listing, (i as u64 + 1) * 100).await;
    }

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(9)); // Party 9 bid 1000
}

#[tokio::test]
async fn test_10_party_reverse_sequential_bids() {
    let mut harness = MultiPartyHarness::new(10).await;

    let listing = harness.create_listing("Reverse Item", 50, 3600).await;

    // Higher party IDs bid less
    for i in 0..10 {
        harness.place_bid(i, &listing, (10 - i as u64) * 100).await;
    }

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(0)); // Party 0 bid 1000
}

#[tokio::test]
async fn test_10_party_all_same_bid() {
    let mut harness = MultiPartyHarness::new(10).await;

    let listing = harness.create_listing("All Same Item", 100, 3600).await;

    // All parties bid the same with sequential timestamps
    for i in 0..10 {
        harness
            .place_bid_with_timestamp(i, &listing, 500, (i as u64 + 1) * 10)
            .await;
    }

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(0)); // First timestamp wins
}

#[tokio::test]
async fn test_10_party_random_winner() {
    let mut harness = MultiPartyHarness::new(10).await;

    let listing = harness
        .create_listing("Random Winner Item", 100, 3600)
        .await;

    // Bids in pseudo-random order, party 4 wins
    let bids = [300, 250, 100, 400, 750, 200, 350, 500, 600, 150];
    for (i, &bid) in bids.iter().enumerate() {
        harness.place_bid(i, &listing, bid).await;
    }

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(4)); // Party 4 bid 750 (highest)
}

#[tokio::test]
async fn test_10_party_all_bids_announced() {
    let mut harness = MultiPartyHarness::new(10).await;

    let listing = harness.create_listing("Announced Item", 100, 3600).await;

    for i in 0..10 {
        harness.place_bid(i, &listing, (i as u64 + 1) * 50).await;
    }

    // All parties should see all 10 bids
    for i in 0..10 {
        let count = harness
            .party(i)
            .auction_logic
            .get_local_bid_count(&listing.key)
            .await;
        assert_eq!(count, 10, "Party {} should see 10 bids", i);
    }
}

#[tokio::test]
async fn test_10_party_decryption_key_distribution() {
    let mut harness = MultiPartyHarness::new(10).await;

    let listing = harness
        .create_listing("Premium Secret Item", 100, 3600)
        .await;

    // Party 6 wins
    let bids = [100, 150, 200, 250, 300, 350, 999, 400, 450, 500];
    for (i, &bid) in bids.iter().enumerate() {
        harness.place_bid(i, &listing, bid).await;
    }

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(6));

    harness.transfer_decryption_key(&listing, 6).await;

    // Only party 6 should have the key
    for i in 0..10 {
        let has_key = harness.party_got_decryption_key(i, &listing).await;
        if i == 6 {
            assert!(has_key, "Winner (party 6) should have decryption key");
        } else {
            assert!(!has_key, "Party {} should not have decryption key", i);
        }
    }
}

#[tokio::test]
async fn test_10_party_multiple_ties() {
    let mut harness = MultiPartyHarness::new(10).await;

    let listing = harness.create_listing("Many Ties Item", 100, 3600).await;

    // 5 parties tie for highest (500), 5 parties tie for lower (200)
    // Timestamps determine winner among highest bidders
    harness
        .place_bid_with_timestamp(0, &listing, 500, 500)
        .await;
    harness
        .place_bid_with_timestamp(1, &listing, 200, 100)
        .await;
    harness
        .place_bid_with_timestamp(2, &listing, 500, 300)
        .await;
    harness
        .place_bid_with_timestamp(3, &listing, 200, 200)
        .await;
    harness
        .place_bid_with_timestamp(4, &listing, 500, 100)
        .await; // Earliest high bid
    harness
        .place_bid_with_timestamp(5, &listing, 200, 300)
        .await;
    harness
        .place_bid_with_timestamp(6, &listing, 500, 400)
        .await;
    harness
        .place_bid_with_timestamp(7, &listing, 200, 400)
        .await;
    harness
        .place_bid_with_timestamp(8, &listing, 500, 200)
        .await;
    harness
        .place_bid_with_timestamp(9, &listing, 200, 500)
        .await;

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(4)); // Party 4: 500 at ts=100 (earliest of high bidders)
}

#[tokio::test]
async fn test_10_party_large_bid_values() {
    let mut harness = MultiPartyHarness::new(10).await;

    let listing = harness
        .create_listing("High Value Item", 1_000_000, 3600)
        .await;

    // Large bid values (simulating real currency amounts)
    let bids: [u64; 10] = [
        1_000_000, 2_500_000, 5_000_000, 3_750_000, 10_000_000, // Highest
        7_500_000, 4_200_000, 6_800_000, 8_900_000, 1_500_000,
    ];

    for (i, &bid) in bids.iter().enumerate() {
        harness.place_bid(i, &listing, bid).await;
    }

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(4)); // Party 4 bid 10_000_000
}

#[tokio::test]
async fn test_10_party_listing_expiration() {
    let mut harness = MultiPartyHarness::new(10).await;

    let listing = harness.create_listing("Expiring Item", 100, 3600).await;

    // All parties watching, none expired yet
    for i in 0..10 {
        let expired = harness.party(i).auction_logic.get_expired_listings().await;
        assert!(
            expired.is_empty(),
            "Party {} should see no expired listings",
            i
        );
    }

    // All parties bid
    for i in 0..10 {
        harness.place_bid(i, &listing, (i as u64 + 1) * 100).await;
    }

    harness.advance_to_deadline(&listing);

    // All parties should now see it as expired
    for i in 0..10 {
        let expired = harness.party(i).auction_logic.get_expired_listings().await;
        assert_eq!(expired.len(), 1, "Party {} should see 1 expired listing", i);
    }
}
