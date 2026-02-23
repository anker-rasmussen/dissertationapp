//! N-party auction integration tests.
//!
//! Parameterized helpers run the same scenarios across 3, 5, and 10 party counts.
//! Tests unique to a specific count are kept as standalone test functions.

use crate::common::MultiPartyHarness;

// ── Shared helpers ──────────────────────────────────────────────────

async fn run_highest_bid_wins(n: usize, bids: &[u64], expected_winner: usize) {
    let mut harness = MultiPartyHarness::new(n).await;
    let listing = harness.create_listing("Highest Bid Item", 100, 3600).await;
    for (i, &bid) in bids.iter().enumerate() {
        harness.place_bid(i, &listing, bid).await;
    }
    harness.advance_to_deadline(&listing);
    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(expected_winner));
}

async fn run_decryption_key_only_winner(n: usize, bids: &[u64], expected_winner: usize) {
    let mut harness = MultiPartyHarness::new(n).await;
    let listing = harness.create_listing("Secret Item", 100, 3600).await;
    for (i, &bid) in bids.iter().enumerate() {
        harness.place_bid(i, &listing, bid).await;
    }
    harness.advance_to_deadline(&listing);
    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(expected_winner));

    harness
        .transfer_decryption_key(&listing, expected_winner)
        .await;
    for i in 0..n {
        let has_key = harness.party_got_decryption_key(i, &listing).await;
        if i == expected_winner {
            assert!(has_key, "Winner should have decryption key");
        } else {
            assert!(!has_key, "Party {} should not have decryption key", i);
        }
    }
}

async fn run_all_same_bid_first_wins(n: usize) {
    let mut harness = MultiPartyHarness::new(n).await;
    let listing = harness.create_listing("Equal Bids Item", 100, 3600).await;
    for i in 0..n {
        harness
            .place_bid_with_timestamp(i, &listing, 200, (i as u64 + 1) * 100)
            .await;
    }
    harness.advance_to_deadline(&listing);
    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(0));
}

async fn run_all_bids_visible(n: usize) {
    let mut harness = MultiPartyHarness::new(n).await;
    let listing = harness.create_listing("Visible Bids Item", 100, 3600).await;
    for i in 0..n {
        harness.place_bid(i, &listing, (i as u64 + 1) * 100).await;
    }
    for i in 0..n {
        let count = harness
            .party(i)
            .auction_logic
            .get_local_bid_count(&listing.key)
            .await;
        assert_eq!(count, n, "Party {} should see {} bids", i, n);
    }
}

async fn run_listing_expiration(n: usize) {
    let mut harness = MultiPartyHarness::new(n).await;
    let listing = harness.create_listing("Expiring Item", 100, 3600).await;
    for i in 0..n {
        let expired = harness.party(i).auction_logic.get_expired_listings().await;
        assert!(
            expired.is_empty(),
            "Party {} should see no expired listings",
            i
        );
    }
    for i in 0..n {
        harness.place_bid(i, &listing, (i as u64 + 1) * 100).await;
    }
    harness.advance_to_deadline(&listing);
    for i in 0..n {
        let expired = harness.party(i).auction_logic.get_expired_listings().await;
        assert_eq!(expired.len(), 1, "Party {} should see 1 expired listing", i);
    }
}

// ── 3-party tests ───────────────────────────────────────────────────

#[tokio::test]
async fn test_3_party_highest_bid_wins() {
    run_highest_bid_wins(3, &[150, 200, 175], 1).await;
}

#[tokio::test]
async fn test_3_party_decryption_key_transfer() {
    run_decryption_key_only_winner(3, &[100, 150, 120], 1).await;
}

#[tokio::test]
async fn test_3_party_all_same_bid_first_wins() {
    run_all_same_bid_first_wins(3).await;
}

#[tokio::test]
async fn test_3_party_two_way_tie() {
    let mut harness = MultiPartyHarness::new(3).await;
    let listing = harness.create_listing("Contest Item", 100, 3600).await;
    harness
        .place_bid_with_timestamp(0, &listing, 250, 2000)
        .await;
    harness
        .place_bid_with_timestamp(1, &listing, 250, 1000)
        .await;
    harness
        .place_bid_with_timestamp(2, &listing, 150, 500)
        .await;
    harness.advance_to_deadline(&listing);
    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(1));
}

#[tokio::test]
async fn test_3_party_bid_at_minimum() {
    let mut harness = MultiPartyHarness::new(3).await;
    let listing = harness.create_listing("Budget Item", 100, 3600).await;
    for i in 0..3 {
        harness
            .place_bid_with_timestamp(i, &listing, 100, (i as u64 + 1) * 1000)
            .await;
    }
    harness.advance_to_deadline(&listing);
    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(0));
}

#[tokio::test]
async fn test_3_party_wide_bid_range() {
    run_highest_bid_wins(3, &[10, 10000, 500], 1).await;
}

#[tokio::test]
async fn test_3_party_listing_watched_by_all() {
    let mut harness = MultiPartyHarness::new(3).await;
    let listing = harness.create_listing("Watched Item", 100, 3600).await;
    for i in 0..3 {
        let expired = harness.party(i).auction_logic.get_expired_listings().await;
        assert!(expired.is_empty());
    }
    harness.advance_to_deadline(&listing);
    for i in 0..3 {
        let expired = harness.party(i).auction_logic.get_expired_listings().await;
        assert_eq!(expired.len(), 1);
    }
}

#[tokio::test]
async fn test_3_party_bid_announcements_shared() {
    run_all_bids_visible(3).await;
}

// ── 5-party tests ───────────────────────────────────────────────────

#[tokio::test]
async fn test_5_party_highest_bid_wins() {
    run_highest_bid_wins(5, &[150, 200, 350, 175, 300], 2).await;
}

#[tokio::test]
async fn test_5_party_last_party_wins() {
    run_highest_bid_wins(5, &[100, 150, 200, 250, 500], 4).await;
}

#[tokio::test]
async fn test_5_party_first_party_wins() {
    run_highest_bid_wins(5, &[999, 100, 200, 300, 400], 0).await;
}

#[tokio::test]
async fn test_5_party_middle_wins() {
    run_highest_bid_wins(5, &[100, 200, 500, 300, 400], 2).await;
}

#[tokio::test]
async fn test_5_party_all_same_bid() {
    run_all_same_bid_first_wins(5).await;
}

#[tokio::test]
async fn test_5_party_three_way_tie() {
    let mut harness = MultiPartyHarness::new(5).await;
    let listing = harness
        .create_listing("Three-Way Tie Item", 100, 3600)
        .await;
    harness
        .place_bid_with_timestamp(0, &listing, 500, 300)
        .await;
    harness
        .place_bid_with_timestamp(1, &listing, 500, 100)
        .await;
    harness
        .place_bid_with_timestamp(2, &listing, 500, 200)
        .await;
    harness.place_bid_with_timestamp(3, &listing, 400, 50).await;
    harness.place_bid_with_timestamp(4, &listing, 300, 10).await;
    harness.advance_to_deadline(&listing);
    let winner = harness.execute_auction(&listing).await;
    assert_eq!(winner, Some(1));
}

#[tokio::test]
async fn test_5_party_decryption_key_only_for_winner() {
    run_decryption_key_only_winner(5, &[100, 200, 300, 400, 350], 3).await;
}

#[tokio::test]
async fn test_5_party_all_bids_visible() {
    run_all_bids_visible(5).await;
}

// ── 10-party tests ──────────────────────────────────────────────────

#[tokio::test]
async fn test_10_party_highest_bid_wins() {
    run_highest_bid_wins(10, &[150, 200, 250, 300, 350, 400, 450, 999, 500, 550], 7).await;
}

#[tokio::test]
async fn test_10_party_sequential_bids() {
    let bids: Vec<u64> = (1..=10).map(|i| i * 100).collect();
    run_highest_bid_wins(10, &bids, 9).await;
}

#[tokio::test]
async fn test_10_party_reverse_sequential_bids() {
    let bids: Vec<u64> = (0..10).map(|i| (10 - i) * 100).collect();
    run_highest_bid_wins(10, &bids, 0).await;
}

#[tokio::test]
async fn test_10_party_all_same_bid() {
    run_all_same_bid_first_wins(10).await;
}

#[tokio::test]
async fn test_10_party_random_winner() {
    run_highest_bid_wins(10, &[300, 250, 100, 400, 750, 200, 350, 500, 600, 150], 4).await;
}

#[tokio::test]
async fn test_10_party_all_bids_announced() {
    run_all_bids_visible(10).await;
}

#[tokio::test]
async fn test_10_party_decryption_key_distribution() {
    run_decryption_key_only_winner(10, &[100, 150, 200, 250, 300, 350, 999, 400, 450, 500], 6)
        .await;
}

#[tokio::test]
async fn test_10_party_multiple_ties() {
    let mut harness = MultiPartyHarness::new(10).await;
    let listing = harness.create_listing("Many Ties Item", 100, 3600).await;
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
        .await;
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
    assert_eq!(winner, Some(4));
}

#[tokio::test]
async fn test_10_party_large_bid_values() {
    run_highest_bid_wins(
        10,
        &[
            1_000_000, 2_500_000, 5_000_000, 3_750_000, 10_000_000, 7_500_000, 4_200_000,
            6_800_000, 8_900_000, 1_500_000,
        ],
        4,
    )
    .await;
}

#[tokio::test]
async fn test_10_party_listing_expiration() {
    run_listing_expiration(10).await;
}
