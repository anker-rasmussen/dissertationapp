//! 3-party auction integration tests.

use crate::common::MultiPartyHarness;

#[tokio::test]
async fn test_3_party_highest_bid_wins() {
    let mut harness = MultiPartyHarness::new(3).await;

    // Party 0 creates listing
    let listing = harness.create_listing("Test Item", 100, 3600).await;

    // All parties bid
    harness.place_bid(0, &listing, 150).await; // Party 0: 150
    harness.place_bid(1, &listing, 200).await; // Party 1: 200 (highest)
    harness.place_bid(2, &listing, 175).await; // Party 2: 175

    // Advance time past deadline
    harness.advance_to_deadline(&listing);

    // Execute auction
    let winner = harness.execute_auction(&listing).await;

    // Party 1 should win (highest bid)
    assert_eq!(winner, Some(1));
}

#[tokio::test]
async fn test_3_party_decryption_key_transfer() {
    let mut harness = MultiPartyHarness::new(3).await;

    let listing = harness.create_listing("Premium Item", 50, 3600).await;

    harness.place_bid(0, &listing, 100).await;
    harness.place_bid(1, &listing, 150).await; // Winner
    harness.place_bid(2, &listing, 120).await;

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(1));

    // Simulate decryption key transfer
    harness.transfer_decryption_key(&listing, 1).await;

    // Winner should have the key
    assert!(harness.party_got_decryption_key(1, &listing).await);

    // Others should not
    assert!(!harness.party_got_decryption_key(0, &listing).await);
    assert!(!harness.party_got_decryption_key(2, &listing).await);
}

#[tokio::test]
async fn test_3_party_all_same_bid_first_wins() {
    let mut harness = MultiPartyHarness::new(3).await;

    let listing = harness.create_listing("Popular Item", 100, 3600).await;

    // All bid the same amount, with explicit timestamps
    harness.place_bid_with_timestamp(0, &listing, 200, 1000).await; // First
    harness.place_bid_with_timestamp(1, &listing, 200, 2000).await; // Second
    harness.place_bid_with_timestamp(2, &listing, 200, 3000).await; // Third

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;

    // Party 0 wins (earliest bid)
    assert_eq!(winner, Some(0));
}

#[tokio::test]
async fn test_3_party_two_way_tie() {
    let mut harness = MultiPartyHarness::new(3).await;

    let listing = harness.create_listing("Contest Item", 100, 3600).await;

    // Two parties tie for highest, one party lower
    harness.place_bid_with_timestamp(0, &listing, 250, 2000).await; // Tied highest, later
    harness.place_bid_with_timestamp(1, &listing, 250, 1000).await; // Tied highest, earlier
    harness.place_bid_with_timestamp(2, &listing, 150, 500).await;  // Lower bid

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;

    // Party 1 wins (same bid as 0, but earlier timestamp)
    assert_eq!(winner, Some(1));
}

#[tokio::test]
async fn test_3_party_bid_at_minimum() {
    let mut harness = MultiPartyHarness::new(3).await;

    let listing = harness.create_listing("Budget Item", 100, 3600).await;

    // All bid at minimum
    harness.place_bid_with_timestamp(0, &listing, 100, 1000).await;
    harness.place_bid_with_timestamp(1, &listing, 100, 2000).await;
    harness.place_bid_with_timestamp(2, &listing, 100, 3000).await;

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;

    // First bidder wins
    assert_eq!(winner, Some(0));
}

#[tokio::test]
async fn test_3_party_wide_bid_range() {
    let mut harness = MultiPartyHarness::new(3).await;

    let listing = harness.create_listing("Varied Bids Item", 10, 3600).await;

    harness.place_bid(0, &listing, 10).await;      // Minimum
    harness.place_bid(1, &listing, 10000).await;   // Very high
    harness.place_bid(2, &listing, 500).await;     // Medium

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;

    // Highest bid wins
    assert_eq!(winner, Some(1));
}

#[tokio::test]
async fn test_3_party_listing_watched_by_all() {
    let mut harness = MultiPartyHarness::new(3).await;

    let listing = harness.create_listing("Watched Item", 100, 3600).await;

    // All parties should be watching the listing
    for i in 0..3 {
        let expired = harness
            .party(i)
            .auction_logic
            .get_expired_listings()
            .await;
        // Not expired yet
        assert!(expired.is_empty());
    }

    // Advance past deadline
    harness.advance_to_deadline(&listing);

    // Now all parties should see it as expired
    for i in 0..3 {
        let expired = harness
            .party(i)
            .auction_logic
            .get_expired_listings()
            .await;
        assert_eq!(expired.len(), 1);
    }
}

#[tokio::test]
async fn test_3_party_bid_announcements_shared() {
    let mut harness = MultiPartyHarness::new(3).await;

    let listing = harness.create_listing("Announced Item", 100, 3600).await;

    harness.place_bid(0, &listing, 150).await;
    harness.place_bid(1, &listing, 200).await;
    harness.place_bid(2, &listing, 175).await;

    // All parties should know about all 3 bids
    for i in 0..3 {
        let count = harness
            .party(i)
            .auction_logic
            .get_local_bid_count(&listing.key)
            .await;
        assert_eq!(count, 3, "Party {} should see 3 bids", i);
    }
}
