//! Error and failure case integration tests.

use market::mocks::{
    make_test_public_key, make_test_record_key, MockDht, MockDhtFailure, MockMpcRunner, MockTime,
    MockTransport,
};
use market::veilid::auction_logic::AuctionLogic;
use market::veilid::bid_storage::BidStorage;
use market::veilid::mpc_orchestrator::validate_auction_parties;
use market::DhtStore;

use crate::common::MultiPartyHarness;

fn create_standalone_logic() -> (
    AuctionLogic<MockDht, MockTransport, MockMpcRunner, MockTime>,
    MockDht,
    MockMpcRunner,
    MockTime,
) {
    let dht = MockDht::new();
    let transport = MockTransport::new();
    let mpc = MockMpcRunner::new();
    let time = MockTime::new(1000);
    let logic = AuctionLogic::new(
        dht.clone(),
        transport,
        mpc.clone(),
        time.clone(),
        make_test_public_key(1),
        BidStorage::new(),
    );
    (logic, dht, mpc, time)
}

#[tokio::test]
async fn test_mpc_fail_all_parties_see_failure() {
    let (logic, _dht, mpc, _time) = create_standalone_logic();
    mpc.set_fail_mode(true).await;

    let result = logic.execute_mpc(0, 3, 100).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("failure"));
}

#[tokio::test]
async fn test_bid_announcement_after_deadline() {
    let mut harness = MultiPartyHarness::new(3).await;
    let listing = harness.create_listing("Expired Item", 100, 3600).await;

    // Advance past deadline
    harness.advance_to_deadline(&listing);

    // Verify listing is expired
    let expired = harness.party(0).auction_logic.get_expired_listings().await;
    assert_eq!(expired.len(), 1);

    // Register a bid after deadline — should still be accepted at storage level
    // (enforcement happens at auction execution, not at bid registration)
    harness
        .party(1)
        .auction_logic
        .register_bid_announcement(
            &listing.key,
            make_test_public_key(5),
            make_test_record_key(50),
        )
        .await;

    assert_eq!(
        harness
            .party(1)
            .auction_logic
            .get_local_bid_count(&listing.key)
            .await,
        1
    );
}

#[tokio::test]
async fn test_duplicate_listing_watch() {
    let mut harness = MultiPartyHarness::new(3).await;
    let listing = harness.create_listing("Watch Twice", 100, 3600).await;

    // Watch the same listing again on party 0
    harness
        .party(0)
        .auction_logic
        .watch_listing(listing.to_public())
        .await;

    // Should not duplicate — get_expired_listings returns at most one entry per key
    harness.advance_to_deadline(&listing);

    let expired = harness.party(0).auction_logic.get_expired_listings().await;
    assert_eq!(expired.len(), 1);
}

#[tokio::test]
async fn test_execute_auction_with_zero_bids() {
    let harness = MultiPartyHarness::new(3).await;

    // Create a listing key but don't create an actual listing or place any bids
    let listing_key = make_test_record_key(99);

    // No bids stored — all parties return None for bid value
    for (i, party) in [harness.party(0), harness.party(1), harness.party(2)]
        .iter()
        .enumerate()
    {
        let bid = party.bid_storage.get_bid(&listing_key).await;
        assert!(
            bid.is_none(),
            "Party {} should have no bid for unregistered listing",
            i
        );
    }
}

#[tokio::test]
async fn test_dht_failure_on_bid_store() {
    let (logic, dht, _mpc, _time) = create_standalone_logic();

    // Create a record first (success)
    let record = dht.create_record().await.unwrap();

    // Enable write failure mode
    dht.set_fail_mode(Some(MockDhtFailure::Writes)).await;

    // Writing to the record should fail
    let result = dht.set_value(&record, b"test data".to_vec()).await;
    assert!(result.is_err());

    // Disable failure mode
    dht.set_fail_mode(None).await;

    // Should succeed now
    let result = dht.set_value(&record, b"test data".to_vec()).await;
    assert!(result.is_ok());

    // Verify discover_bids still works with empty DHT
    let listing_key = make_test_record_key(1);
    let bid_index = logic.discover_bids(&listing_key).await.unwrap();
    assert!(bid_index.bids.is_empty());
}

#[tokio::test]
async fn test_dht_failure_on_listing_read() {
    let (logic, dht, _mpc, _time) = create_standalone_logic();

    let listing_key = make_test_record_key(1);

    // Enable read failure mode
    dht.set_fail_mode(Some(MockDhtFailure::Reads)).await;

    // discover_bids reads from DHT — should propagate error
    let result = logic.discover_bids(&listing_key).await;
    assert!(result.is_err());

    // Disable failure mode
    dht.set_fail_mode(None).await;

    // Should succeed now (empty result)
    let result = logic.discover_bids(&listing_key).await;
    assert!(result.is_ok());
}

#[test]
fn test_n2_mpc_validation() {
    // MPC requires N >= 2
    assert!(validate_auction_parties(1).is_err());
    assert!(validate_auction_parties(0).is_err());

    // N = 2 and above should be accepted
    assert!(validate_auction_parties(2).is_ok());
    assert!(validate_auction_parties(3).is_ok());
    assert!(validate_auction_parties(4).is_ok());
    assert!(validate_auction_parties(10).is_ok());
}
