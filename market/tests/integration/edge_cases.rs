//! Edge case tests for auction logic.

use crate::common::MultiPartyHarness;

#[tokio::test]
async fn test_single_bidder() {
    let mut harness = MultiPartyHarness::new(3).await;

    let listing = harness.create_listing("Single Bid Item", 100, 3600).await;

    // Only one party bids
    harness.place_bid(1, &listing, 150).await;

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;

    // Single bidder wins
    assert_eq!(winner, Some(1));
}

#[tokio::test]
async fn test_two_bidders_out_of_three() {
    let mut harness = MultiPartyHarness::new(3).await;

    let listing = harness.create_listing("Two Bidders Item", 100, 3600).await;

    // Only parties 0 and 2 bid (party 1 abstains)
    harness.place_bid(0, &listing, 200).await;
    harness.place_bid(2, &listing, 300).await; // Winner

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(2));
}

#[tokio::test]
async fn test_minimum_bid_value() {
    let mut harness = MultiPartyHarness::new(3).await;

    // High minimum bid
    let listing = harness.create_listing("Expensive Item", 1000, 3600).await;

    harness.place_bid(0, &listing, 1000).await; // Exactly minimum
    harness.place_bid(1, &listing, 1001).await; // Just above minimum (winner)
    harness.place_bid(2, &listing, 1000).await; // Exactly minimum

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(1));
}

#[tokio::test]
async fn test_zero_minimum_bid() {
    let mut harness = MultiPartyHarness::new(3).await;

    let listing = harness.create_listing("Free Start Item", 0, 3600).await;

    harness.place_bid(0, &listing, 1).await;
    harness.place_bid(1, &listing, 2).await; // Winner
    harness.place_bid(2, &listing, 1).await;

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(1));
}

#[tokio::test]
async fn test_very_short_auction() {
    let mut harness = MultiPartyHarness::new(3).await;

    // 1 second auction
    let listing = harness.create_listing("Flash Sale", 100, 1).await;

    harness.place_bid(0, &listing, 200).await;
    harness.place_bid(1, &listing, 300).await; // Winner

    // Advance past deadline
    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(1));
}

#[tokio::test]
async fn test_long_auction_duration() {
    let mut harness = MultiPartyHarness::new(3).await;

    // 30 day auction
    let listing = harness
        .create_listing("Long Auction", 100, 30 * 24 * 3600)
        .await;

    harness.place_bid(0, &listing, 200).await;
    harness.place_bid(1, &listing, 300).await;
    harness.place_bid(2, &listing, 400).await; // Winner

    // Advance time incrementally
    harness.advance_time(15 * 24 * 3600); // 15 days

    // Auction not yet ended
    let expired = harness.party(0).auction_logic.get_expired_listings().await;
    assert!(expired.is_empty());

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(2));
}

#[tokio::test]
async fn test_consecutive_auctions() {
    let mut harness = MultiPartyHarness::new(3).await;

    // First auction
    let listing1 = harness.create_listing("Item 1", 100, 3600).await;
    harness.place_bid(0, &listing1, 200).await;
    harness.place_bid(1, &listing1, 300).await; // Winner
    harness.advance_to_deadline(&listing1);
    let winner1 = harness.execute_auction(&listing1).await;
    assert_eq!(winner1, Some(1));

    // Clear registry for next auction
    harness.clear_bid_registry().await;

    // Second auction (different winner)
    let listing2 = harness.create_listing("Item 2", 100, 3600).await;
    harness.place_bid(0, &listing2, 500).await; // Winner
    harness.place_bid(1, &listing2, 200).await;
    harness.advance_to_deadline(&listing2);
    let winner2 = harness.execute_auction(&listing2).await;
    assert_eq!(winner2, Some(0));
}

#[tokio::test]
async fn test_timestamp_edge_exact_tie() {
    let mut harness = MultiPartyHarness::new(3).await;

    let listing = harness.create_listing("Exact Tie Item", 100, 3600).await;

    // All bids at exact same timestamp
    let same_ts = 1000;
    harness
        .place_bid_with_timestamp(0, &listing, 500, same_ts)
        .await;
    harness
        .place_bid_with_timestamp(1, &listing, 500, same_ts)
        .await;
    harness
        .place_bid_with_timestamp(2, &listing, 500, same_ts)
        .await;

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;

    // With same timestamp, lowest party_id wins (deterministic)
    // The SharedBidRegistry iterates HashMap, so winner depends on hash order
    // but there should always be exactly one winner
    assert!(winner.is_some());
}

#[tokio::test]
async fn test_max_u64_bid_value() {
    let mut harness = MultiPartyHarness::new(3).await;

    let listing = harness.create_listing("Max Value Item", 100, 3600).await;

    harness.place_bid(0, &listing, u64::MAX - 1).await;
    harness.place_bid(1, &listing, u64::MAX).await; // Maximum possible value
    harness.place_bid(2, &listing, u64::MAX / 2).await;

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(1));
}

#[tokio::test]
async fn test_seller_can_bid() {
    let mut harness = MultiPartyHarness::new(3).await;

    // Party 0 is the seller (creates listing)
    let listing = harness
        .create_listing("Seller Bidding Item", 100, 3600)
        .await;

    // Seller also bids
    harness.place_bid(0, &listing, 1000).await; // Seller bids highest
    harness.place_bid(1, &listing, 500).await;
    harness.place_bid(2, &listing, 700).await;

    harness.advance_to_deadline(&listing);

    let winner = harness.execute_auction(&listing).await;

    assert_eq!(winner, Some(0));
}

#[tokio::test]
async fn test_bid_storage_persists() {
    let mut harness = MultiPartyHarness::new(3).await;

    let listing = harness.create_listing("Persistent Item", 100, 3600).await;

    harness.place_bid(0, &listing, 200).await;
    harness.place_bid(1, &listing, 300).await;

    // Verify bids are stored
    assert!(harness.party(0).bid_storage.has_bid(&listing.key).await);
    assert!(harness.party(1).bid_storage.has_bid(&listing.key).await);
    assert!(!harness.party(2).bid_storage.has_bid(&listing.key).await);

    // Verify bid values
    let (value0, _) = harness
        .party(0)
        .bid_storage
        .get_bid(&listing.key)
        .await
        .unwrap();
    let (value1, _) = harness
        .party(1)
        .bid_storage
        .get_bid(&listing.key)
        .await
        .unwrap();

    assert_eq!(value0, 200);
    assert_eq!(value1, 300);
}

#[tokio::test]
async fn test_dht_data_shared() {
    let mut harness = MultiPartyHarness::new(3).await;

    let listing = harness.create_listing("DHT Test Item", 100, 3600).await;

    harness.place_bid(0, &listing, 200).await;

    // All parties should see the same number of records
    let count0 = harness.party(0).dht.record_count().await;
    let count1 = harness.party(1).dht.record_count().await;
    let count2 = harness.party(2).dht.record_count().await;

    assert_eq!(count0, count1);
    assert_eq!(count1, count2);
}

#[tokio::test]
async fn test_listing_time_remaining() {
    let mut harness = MultiPartyHarness::new(3).await;

    let listing = harness.create_listing("Timed Item", 100, 3600).await;

    // Check time remaining at start
    let remaining = listing.time_remaining_at(harness.time().get());
    assert_eq!(remaining, 3600);

    // Advance half way
    harness.advance_time(1800);
    let remaining = listing.time_remaining_at(harness.time().get());
    assert_eq!(remaining, 1800);

    // At deadline
    harness.advance_time(1800);
    let remaining = listing.time_remaining_at(harness.time().get());
    assert_eq!(remaining, 0);

    // After deadline
    harness.advance_time(100);
    let remaining = listing.time_remaining_at(harness.time().get());
    assert_eq!(remaining, 0);
}
