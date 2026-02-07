//! 5-party auction integration tests.

use crate::common::MultiPartyHarness;

#[tokio::test]
async fn test_5_party_highest_bid_wins() {
    let mut harness = MultiPartyHarness::new(5).await;

    let listing = harness.create_listing("5-Party Item", 100, 3600).await;

    harness.place_bid(0, &listing, 150).await;
    harness.place_bid(1, &listing, 200).await;
    harness.place_bid(2, &listing, 350).await; // Highest
    harness.place_bid(3, &listing, 175).await;
    harness.place_bid(4, &listing, 300).await;

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(2));
}

#[tokio::test]
async fn test_5_party_last_party_wins() {
    let mut harness = MultiPartyHarness::new(5).await;

    let listing = harness.create_listing("Last Winner Item", 100, 3600).await;

    harness.place_bid(0, &listing, 100).await;
    harness.place_bid(1, &listing, 150).await;
    harness.place_bid(2, &listing, 200).await;
    harness.place_bid(3, &listing, 250).await;
    harness.place_bid(4, &listing, 500).await; // Highest

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(4));
}

#[tokio::test]
async fn test_5_party_first_party_wins() {
    let mut harness = MultiPartyHarness::new(5).await;

    let listing = harness.create_listing("First Winner Item", 100, 3600).await;

    harness.place_bid(0, &listing, 999).await; // Highest
    harness.place_bid(1, &listing, 100).await;
    harness.place_bid(2, &listing, 200).await;
    harness.place_bid(3, &listing, 300).await;
    harness.place_bid(4, &listing, 400).await;

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(0));
}

#[tokio::test]
async fn test_5_party_middle_wins() {
    let mut harness = MultiPartyHarness::new(5).await;

    let listing = harness
        .create_listing("Middle Winner Item", 100, 3600)
        .await;

    harness.place_bid(0, &listing, 100).await;
    harness.place_bid(1, &listing, 200).await;
    harness.place_bid(2, &listing, 500).await; // Highest (middle party)
    harness.place_bid(3, &listing, 300).await;
    harness.place_bid(4, &listing, 400).await;

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(2));
}

#[tokio::test]
async fn test_5_party_all_same_bid() {
    let mut harness = MultiPartyHarness::new(5).await;

    let listing = harness.create_listing("Equal Bids Item", 100, 3600).await;

    // All bid the same, timestamps determine winner
    for i in 0..5 {
        harness
            .place_bid_with_timestamp(i, &listing, 200, (i as u64 + 1) * 100)
            .await;
    }

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;

    // Party 0 has earliest timestamp (100)
    assert_eq!(winner, Some(0));
}

#[tokio::test]
async fn test_5_party_three_way_tie() {
    let mut harness = MultiPartyHarness::new(5).await;

    let listing = harness
        .create_listing("Three-Way Tie Item", 100, 3600)
        .await;

    harness
        .place_bid_with_timestamp(0, &listing, 500, 300)
        .await; // Tied, ts=300
    harness
        .place_bid_with_timestamp(1, &listing, 500, 100)
        .await; // Tied, ts=100 (earliest)
    harness
        .place_bid_with_timestamp(2, &listing, 500, 200)
        .await; // Tied, ts=200
    harness.place_bid_with_timestamp(3, &listing, 400, 50).await; // Lower
    harness.place_bid_with_timestamp(4, &listing, 300, 10).await; // Lower

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;

    // Party 1 wins (highest bid with earliest timestamp)
    assert_eq!(winner, Some(1));
}

#[tokio::test]
async fn test_5_party_decryption_key_only_for_winner() {
    let mut harness = MultiPartyHarness::new(5).await;

    let listing = harness.create_listing("Secret Item", 100, 3600).await;

    harness.place_bid(0, &listing, 100).await;
    harness.place_bid(1, &listing, 200).await;
    harness.place_bid(2, &listing, 300).await;
    harness.place_bid(3, &listing, 400).await; // Winner
    harness.place_bid(4, &listing, 350).await;

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(3));

    harness.transfer_decryption_key(&listing, 3).await;

    // Only winner has key
    for i in 0..5 {
        let has_key = harness.party_got_decryption_key(i, &listing).await;
        if i == 3 {
            assert!(has_key, "Winner should have decryption key");
        } else {
            assert!(!has_key, "Party {} should not have decryption key", i);
        }
    }
}

#[tokio::test]
async fn test_5_party_all_bids_visible() {
    let mut harness = MultiPartyHarness::new(5).await;

    let listing = harness.create_listing("Visible Bids Item", 100, 3600).await;

    for i in 0..5 {
        harness.place_bid(i, &listing, (i as u64 + 1) * 100).await;
    }

    // All parties should see all 5 bids
    for i in 0..5 {
        let count = harness
            .party(i)
            .auction_logic
            .get_local_bid_count(&listing.key)
            .await;
        assert_eq!(count, 5, "Party {} should see 5 bids", i);
    }
}
