//! Testable auction coordination logic.
//!
//! This module extracts the business logic from AuctionCoordinator
//! into generic, testable components.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use veilid_core::{PublicKey, RecordKey};

use crate::config::subkeys;
use crate::error::{MarketError, MarketResult};
use crate::marketplace::{BidIndex, Listing};
use crate::traits::{
    DhtStore, MessageTransport, MpcResult, MpcRunner, TimeProvider, TransportTarget,
};
use crate::veilid::bid_announcement::{
    AuctionMessage, BidAnnouncementRegistry, BidAnnouncementTracker,
};
use crate::veilid::bid_ops::BidOperations;
use crate::veilid::bid_storage::BidStorage;

/// Testable auction coordination logic.
///
/// This struct contains the business logic for auction coordination,
/// abstracted over the DHT, transport, MPC, and time dependencies.
pub struct AuctionLogic<D, T, M, C>
where
    D: DhtStore,
    T: MessageTransport,
    M: MpcRunner,
    C: TimeProvider,
{
    dht: D,
    transport: T,
    mpc: M,
    time: C,
    my_node_id: PublicKey,
    bid_storage: BidStorage,
    /// Listings we're monitoring
    watched_listings: Arc<Mutex<HashMap<RecordKey, Listing>>>,
    /// Collected bid announcements (deduped per listing)
    bid_announcements: BidAnnouncementTracker,
    /// Received decryption keys for won auctions: Map<listing_key, decryption_key_hex>
    decryption_keys: Arc<Mutex<HashMap<RecordKey, String>>>,
}

impl<D, T, M, C> AuctionLogic<D, T, M, C>
where
    D: DhtStore,
    T: MessageTransport,
    M: MpcRunner,
    C: TimeProvider,
{
    /// Create a new auction logic instance.
    pub fn new(
        dht: D,
        transport: T,
        mpc: M,
        time: C,
        my_node_id: PublicKey,
        bid_storage: BidStorage,
    ) -> Self {
        Self {
            dht,
            transport,
            mpc,
            time,
            my_node_id,
            bid_storage,
            watched_listings: Arc::new(Mutex::new(HashMap::new())),
            bid_announcements: BidAnnouncementTracker::new(),
            decryption_keys: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Watch a listing for deadline.
    pub async fn watch_listing(&self, listing: Listing) {
        let mut watched = self.watched_listings.lock().await;
        info!(
            "Now watching listing '{}' for auction deadline",
            listing.title
        );
        watched.insert(listing.key.clone(), listing);
    }

    /// Get all watched listings that have reached their deadline.
    pub async fn get_expired_listings(&self) -> Vec<(RecordKey, Listing)> {
        let now = self.time.now_unix();
        let watched = self.watched_listings.lock().await;
        watched
            .iter()
            .filter(|(_, listing)| listing.time_remaining_at(now) == 0)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Remove a listing from the watch list.
    pub async fn unwatch_listing(&self, key: &RecordKey) {
        let mut watched = self.watched_listings.lock().await;
        watched.remove(key);
    }

    /// Register a bid announcement locally.
    pub async fn register_bid_announcement(
        &self,
        listing_key: &RecordKey,
        bidder: PublicKey,
        bid_record_key: RecordKey,
    ) {
        if self
            .bid_announcements
            .register(listing_key, bidder, bid_record_key)
            .await
        {
            let count = self.bid_announcements.count(listing_key).await;
            info!("Registered bid announcement, total: {}", count);
        }
    }

    /// Get the number of bids for a listing from local announcements.
    pub async fn get_local_bid_count(&self, listing_key: &RecordKey) -> usize {
        self.bid_announcements.count(listing_key).await
    }

    /// Store a received decryption key.
    pub async fn store_decryption_key(&self, listing_key: &RecordKey, key: String) {
        self.decryption_keys
            .lock()
            .await
            .insert(listing_key.clone(), key);
        info!("Stored decryption key for listing {}", listing_key);
    }

    /// Get a stored decryption key.
    pub async fn get_decryption_key(&self, listing_key: &RecordKey) -> Option<String> {
        let keys = self.decryption_keys.lock().await;
        keys.get(listing_key).cloned()
    }

    /// Broadcast a bid announcement to all peers.
    pub async fn broadcast_bid_announcement(
        &self,
        listing_key: &RecordKey,
        bid_record_key: &RecordKey,
    ) -> MarketResult<usize> {
        let announcement = AuctionMessage::BidAnnouncement {
            listing_key: listing_key.clone(),
            bidder: self.my_node_id.clone(),
            bid_record_key: bid_record_key.clone(),
            timestamp: self.time.now_unix(),
        };

        let data = announcement.to_bytes()?;
        let count = self.transport.broadcast(data).await?;
        info!("Broadcast bid announcement to {} peers", count);
        Ok(count)
    }

    /// Discover bids from DHT registry.
    pub async fn discover_bids(&self, listing_key: &RecordKey) -> MarketResult<BidIndex> {
        info!("Discovering bids from DHT bid registry");

        let mut bid_index = BidIndex::new(listing_key.clone());
        let bid_ops = BidOperations::new(self.dht.clone());

        // Read bid registry from DHT subkey
        let registry_data = self
            .dht
            .get_subkey(listing_key, subkeys::BID_ANNOUNCEMENTS)
            .await?;

        let bidder_list: Vec<(PublicKey, RecordKey, u64)> = if let Some(data) = registry_data {
            let registry = BidAnnouncementRegistry::from_bytes(&data)?;
            info!(
                "Found {} bidders in DHT bid registry",
                registry.announcements.len()
            );
            registry.announcements
        } else {
            info!("No DHT bid registry found, using local announcements");
            match self.bid_announcements.get(listing_key).await {
                Some(list) => list
                    .iter()
                    .map(|(bidder, bid_key)| (bidder.clone(), bid_key.clone(), 0))
                    .collect(),
                None => Vec::new(),
            }
        };

        // Fetch each bidder's BidRecord
        for (bidder, bid_record_key, _) in &bidder_list {
            match bid_ops.fetch_bid(bid_record_key).await {
                Ok(Some(bid_record)) => {
                    info!("Fetched bid record for bidder {}", bidder);
                    bid_index.add_bid(bid_record);
                }
                Ok(None) => {
                    warn!(
                        "No bid record found for bidder {} at {}",
                        bidder, bid_record_key
                    );
                }
                Err(e) => {
                    warn!("Failed to fetch bid for {}: {}", bidder, e);
                }
            }
        }

        info!("Built BidIndex with {} bids", bid_index.bids.len());
        Ok(bid_index)
    }

    /// Determine party ID from bid index.
    pub fn get_my_party_id(&self, bid_index: &BidIndex) -> Option<usize> {
        let sorted = bid_index.sorted_bidders();
        sorted.iter().position(|b| b == &self.my_node_id)
    }

    /// Check if we have a bid for a listing.
    pub async fn has_bid(&self, listing_key: &RecordKey) -> bool {
        self.bid_storage.has_bid(listing_key).await
    }

    /// Get our bid value.
    pub async fn get_my_bid(&self, listing_key: &RecordKey) -> Option<(u64, [u8; 32])> {
        self.bid_storage.get_bid(listing_key).await
    }

    /// Execute MPC auction.
    ///
    /// Returns the MPC result indicating whether we won.
    pub async fn execute_mpc(
        &self,
        party_id: usize,
        num_parties: usize,
        bid_value: u64,
    ) -> MarketResult<MpcResult> {
        // Write input
        self.mpc.write_input(party_id, bid_value).await?;

        // Write hosts file
        let hosts_name = format!("HOSTS-{party_id}");
        self.mpc.write_hosts(&hosts_name, num_parties).await?;

        // Compile program
        self.mpc.compile("auction_n", num_parties).await?;

        // Execute
        let result = self.mpc.execute(party_id, num_parties, bid_value).await?;

        info!(
            "MPC result: party {} {} the auction",
            party_id,
            if result.is_winner { "won" } else { "lost" }
        );

        Ok(result)
    }

    /// Process a bid announcement message.
    pub async fn process_bid_announcement(
        &self,
        listing_key: RecordKey,
        bidder: PublicKey,
        bid_record_key: RecordKey,
        _timestamp: u64,
    ) -> MarketResult<()> {
        info!(
            "Processing bid announcement for listing {} from {}",
            listing_key, bidder
        );
        self.register_bid_announcement(&listing_key, bidder, bid_record_key)
            .await;
        Ok(())
    }

    /// Process a decryption hash transfer message.
    pub async fn process_decryption_transfer(
        &self,
        listing_key: RecordKey,
        winner: PublicKey,
        decryption_hash: String,
    ) -> MarketResult<()> {
        if winner == self.my_node_id {
            info!("I am the winner! Received decryption key");
            self.store_decryption_key(&listing_key, decryption_hash)
                .await;
        } else {
            debug!("Decryption transfer not for me, ignoring");
        }
        Ok(())
    }

    /// Send a winner decryption request.
    pub async fn send_decryption_request(&self, listing_key: &RecordKey) -> MarketResult<usize> {
        let message = AuctionMessage::WinnerDecryptionRequest {
            listing_key: listing_key.clone(),
            winner: self.my_node_id.clone(),
            timestamp: self.time.now_unix(),
        };

        let data = message.to_bytes()?;
        let count = self.transport.broadcast(data).await?;
        info!("Sent decryption request to {} peers", count);
        Ok(count)
    }

    /// Send decryption hash to winner.
    pub async fn send_decryption_hash(
        &self,
        listing_key: &RecordKey,
        winner: &PublicKey,
        decryption_hash: &str,
    ) -> MarketResult<usize> {
        let message = AuctionMessage::DecryptionHashTransfer {
            listing_key: listing_key.clone(),
            winner: winner.clone(),
            decryption_hash: decryption_hash.to_string(),
            timestamp: self.time.now_unix(),
        };

        let data = message.to_bytes()?;

        // Send to winner's node directly
        self.transport
            .send(TransportTarget::Node(winner.clone()), data)
            .await
            .map_err(|e| {
                MarketError::Network(format!("Failed to send decryption hash to winner: {e}"))
            })?;

        info!("Sent decryption hash directly to winner");
        Ok(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marketplace::BidRecord;
    use crate::mocks::{
        make_test_public_key, make_test_record_key, MockDht, MockMpcRunner, MockTime, MockTransport,
    };

    fn create_test_logic() -> AuctionLogic<MockDht, MockTransport, MockMpcRunner, MockTime> {
        let dht = MockDht::new();
        let transport = MockTransport::new();
        let mpc = MockMpcRunner::new();
        let time = MockTime::new(1000);
        let my_node_id = make_test_public_key(1);
        let bid_storage = BidStorage::new();

        AuctionLogic::new(dht, transport, mpc, time, my_node_id, bid_storage)
    }

    fn make_test_listing(time: &MockTime) -> Listing {
        Listing::builder_with_time(time.clone())
            .key(make_test_record_key(1))
            .seller(make_test_public_key(2))
            .title("Test Auction")
            .encrypted_content(vec![1, 2, 3], [0u8; 12], "abc123".to_string())
            .reserve_price(100)
            .auction_duration(3600)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_watch_listing() {
        let logic = create_test_logic();
        let time = MockTime::new(1000);
        let listing = make_test_listing(&time);

        logic.watch_listing(listing.clone()).await;

        let expired = logic.get_expired_listings().await;
        assert!(expired.is_empty()); // Not expired yet
    }

    #[tokio::test]
    async fn test_register_bid_announcement() {
        let logic = create_test_logic();
        let listing_key = make_test_record_key(1);
        let bidder = make_test_public_key(5);
        let bid_key = make_test_record_key(10);

        logic
            .register_bid_announcement(&listing_key, bidder.clone(), bid_key.clone())
            .await;

        assert_eq!(logic.get_local_bid_count(&listing_key).await, 1);
    }

    #[tokio::test]
    async fn test_register_duplicate_bid_announcement() {
        let logic = create_test_logic();
        let listing_key = make_test_record_key(1);
        let bidder = make_test_public_key(5);
        let bid_key = make_test_record_key(10);

        logic
            .register_bid_announcement(&listing_key, bidder.clone(), bid_key.clone())
            .await;
        logic
            .register_bid_announcement(&listing_key, bidder.clone(), bid_key.clone())
            .await;

        // Should not count duplicates
        assert_eq!(logic.get_local_bid_count(&listing_key).await, 1);
    }

    #[tokio::test]
    async fn test_multiple_bidders() {
        let logic = create_test_logic();
        let listing_key = make_test_record_key(1);

        for i in 1..=3 {
            let bidder = make_test_public_key(i);
            let bid_key = make_test_record_key(i as u64 * 10);
            logic
                .register_bid_announcement(&listing_key, bidder, bid_key)
                .await;
        }

        assert_eq!(logic.get_local_bid_count(&listing_key).await, 3);
    }

    #[tokio::test]
    async fn test_store_and_get_decryption_key() {
        let logic = create_test_logic();
        let listing_key = make_test_record_key(1);

        assert!(logic.get_decryption_key(&listing_key).await.is_none());

        logic
            .store_decryption_key(&listing_key, "secret123".to_string())
            .await;

        assert_eq!(
            logic.get_decryption_key(&listing_key).await,
            Some("secret123".to_string())
        );
    }

    #[tokio::test]
    async fn test_process_decryption_transfer_for_me() {
        let logic = create_test_logic();
        let listing_key = make_test_record_key(1);
        let my_id = make_test_public_key(1); // Same as logic's my_node_id

        logic
            .process_decryption_transfer(listing_key.clone(), my_id, "key123".to_string())
            .await
            .unwrap();

        assert_eq!(
            logic.get_decryption_key(&listing_key).await,
            Some("key123".to_string())
        );
    }

    #[tokio::test]
    async fn test_process_decryption_transfer_not_for_me() {
        let logic = create_test_logic();
        let listing_key = make_test_record_key(1);
        let other_id = make_test_public_key(99); // Different from logic's my_node_id

        logic
            .process_decryption_transfer(listing_key.clone(), other_id, "key123".to_string())
            .await
            .unwrap();

        // Should not store since we're not the winner
        assert!(logic.get_decryption_key(&listing_key).await.is_none());
    }

    #[tokio::test]
    async fn test_get_my_party_id() {
        let logic = create_test_logic();
        let listing_key = make_test_record_key(1);

        let mut index = BidIndex::new(listing_key.clone());

        // Add bids including our node
        let my_bid = BidRecord {
            listing_key: listing_key.clone(),
            bidder: make_test_public_key(1), // Our node
            commitment: [1u8; 32],
            timestamp: 1000,
            bid_key: make_test_record_key(10),
        };
        let other_bid = BidRecord {
            listing_key: listing_key.clone(),
            bidder: make_test_public_key(2),
            commitment: [2u8; 32],
            timestamp: 1000,
            bid_key: make_test_record_key(20),
        };

        index.bids.push(my_bid);
        index.bids.push(other_bid);

        let party_id = logic.get_my_party_id(&index);
        assert!(party_id.is_some());
        // Party ID should be in range [0, 2)
        assert!(party_id.unwrap() < 2);
    }

    #[tokio::test]
    async fn test_get_my_party_id_not_participating() {
        let logic = create_test_logic();
        let listing_key = make_test_record_key(1);

        let mut index = BidIndex::new(listing_key.clone());

        // Only other bidders, not us
        let other_bid = BidRecord {
            listing_key: listing_key.clone(),
            bidder: make_test_public_key(99),
            commitment: [99u8; 32],
            timestamp: 1000,
            bid_key: make_test_record_key(990),
        };
        index.bids.push(other_bid);

        let party_id = logic.get_my_party_id(&index);
        assert!(party_id.is_none());
    }

    #[tokio::test]
    async fn test_execute_mpc() {
        let dht = MockDht::new();
        let transport = MockTransport::new();
        let mpc = MockMpcRunner::new();
        let time = MockTime::new(1000);
        let my_node_id = make_test_public_key(1);
        let bid_storage = BidStorage::new();

        // Set party 0 as winner
        mpc.set_winner(0).await;

        let logic = AuctionLogic::new(dht, transport, mpc.clone(), time, my_node_id, bid_storage);

        let result = logic.execute_mpc(0, 3, 100).await.unwrap();

        assert!(result.is_winner);
        assert_eq!(result.party_id, 0);
        assert_eq!(result.num_parties, 3);

        // Verify MPC runner was called correctly
        assert!(mpc.was_compiled("auction_n").await);
        let inputs = mpc.get_inputs().await;
        assert_eq!(inputs.get(&0), Some(&100));
    }

    #[tokio::test]
    async fn test_execute_mpc_loser() {
        let dht = MockDht::new();
        let transport = MockTransport::new();
        let mpc = MockMpcRunner::new();
        let time = MockTime::new(1000);
        let my_node_id = make_test_public_key(1);
        let bid_storage = BidStorage::new();

        // Set party 2 as winner (not us)
        mpc.set_winner(2).await;

        let logic = AuctionLogic::new(dht, transport, mpc, time, my_node_id, bid_storage);

        let result = logic.execute_mpc(0, 3, 100).await.unwrap();

        assert!(!result.is_winner);
    }

    #[tokio::test]
    async fn test_broadcast_bid_announcement() {
        let dht = MockDht::new();
        let transport = MockTransport::new();
        let mpc = MockMpcRunner::new();
        let time = MockTime::new(1000);
        let my_node_id = make_test_public_key(1);
        let bid_storage = BidStorage::new();

        // Add peers
        transport.add_peer(make_test_public_key(2)).await;
        transport.add_peer(make_test_public_key(3)).await;

        let logic = AuctionLogic::new(dht, transport.clone(), mpc, time, my_node_id, bid_storage);

        let listing_key = make_test_record_key(1);
        let bid_key = make_test_record_key(10);

        let count = logic
            .broadcast_bid_announcement(&listing_key, &bid_key)
            .await
            .unwrap();

        assert_eq!(count, 2);
        assert_eq!(transport.message_count().await, 2);
    }

    #[tokio::test]
    async fn test_unwatch_listing() {
        let logic = create_test_logic();
        let time = MockTime::new(1000);
        let listing = make_test_listing(&time);
        let key = listing.key.clone();

        logic.watch_listing(listing).await;
        logic.unwatch_listing(&key).await;

        // Listing should be removed
        let expired = logic.get_expired_listings().await;
        assert!(expired.is_empty());
    }

    #[tokio::test]
    async fn test_has_bid() {
        let logic = create_test_logic();
        let listing_key = make_test_record_key(1);

        assert!(!logic.has_bid(&listing_key).await);

        // Store a bid
        logic
            .bid_storage
            .store_bid(&listing_key, 100, [0u8; 32])
            .await;

        assert!(logic.has_bid(&listing_key).await);
    }

    #[tokio::test]
    async fn test_get_my_bid() {
        let logic = create_test_logic();
        let listing_key = make_test_record_key(1);
        let nonce = [42u8; 32];

        logic.bid_storage.store_bid(&listing_key, 500, nonce).await;

        let (value, retrieved_nonce) = logic.get_my_bid(&listing_key).await.unwrap();
        assert_eq!(value, 500);
        assert_eq!(retrieved_nonce, nonce);
    }

    #[tokio::test]
    async fn test_expired_listing_detection() {
        let dht = MockDht::new();
        let transport = MockTransport::new();
        let mpc = MockMpcRunner::new();
        let time = MockTime::new(1000);
        let my_node_id = make_test_public_key(1);
        let bid_storage = BidStorage::new();

        let logic = AuctionLogic::new(dht, transport, mpc, time.clone(), my_node_id, bid_storage);

        // Create a listing that ends at time 4600 (1000 + 3600)
        let listing = make_test_listing(&time);
        let listing_key = listing.key.clone();
        logic.watch_listing(listing).await;

        // At time 1000, the listing is not expired
        let expired = logic.get_expired_listings().await;
        assert!(expired.is_empty());

        // Advance time past the deadline
        time.set(5000);

        // Now the listing should be expired
        let expired = logic.get_expired_listings().await;
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, listing_key);
    }

    #[tokio::test]
    async fn test_send_decryption_request() {
        let dht = MockDht::new();
        let transport = MockTransport::new();
        let mpc = MockMpcRunner::new();
        let time = MockTime::new(1000);
        let my_node_id = make_test_public_key(1);
        let bid_storage = BidStorage::new();

        // Add peers
        transport.add_peer(make_test_public_key(2)).await;
        transport.add_peer(make_test_public_key(3)).await;

        let logic = AuctionLogic::new(dht, transport.clone(), mpc, time, my_node_id, bid_storage);

        let listing_key = make_test_record_key(1);
        let count = logic.send_decryption_request(&listing_key).await.unwrap();

        assert_eq!(count, 2);

        // Verify messages were sent
        let messages = transport.get_sent_messages().await;
        assert_eq!(messages.len(), 2);
    }

    #[tokio::test]
    async fn test_send_decryption_hash() {
        let dht = MockDht::new();
        let transport = MockTransport::new();
        let mpc = MockMpcRunner::new();
        let time = MockTime::new(1000);
        let my_node_id = make_test_public_key(1);
        let bid_storage = BidStorage::new();

        // Add winner as peer
        transport.add_peer(make_test_public_key(5)).await;

        let logic = AuctionLogic::new(dht, transport.clone(), mpc, time, my_node_id, bid_storage);

        let listing_key = make_test_record_key(1);
        let winner = make_test_public_key(5);
        let count = logic
            .send_decryption_hash(&listing_key, &winner, "secret_key_123")
            .await
            .unwrap();

        assert_eq!(count, 1);

        // Verify message content
        let messages = transport.get_messages_to_nodes().await;
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn test_discover_bids_from_local_announcements() {
        let dht = MockDht::new();
        let transport = MockTransport::new();
        let mpc = MockMpcRunner::new();
        let time = MockTime::new(1000);
        let my_node_id = make_test_public_key(1);
        let bid_storage = BidStorage::new();

        let logic = AuctionLogic::new(dht.clone(), transport, mpc, time, my_node_id, bid_storage);

        let listing_key = make_test_record_key(1);

        // Register some local announcements
        logic
            .register_bid_announcement(
                &listing_key,
                make_test_public_key(2),
                make_test_record_key(20),
            )
            .await;
        logic
            .register_bid_announcement(
                &listing_key,
                make_test_public_key(3),
                make_test_record_key(30),
            )
            .await;

        // Discover bids - should find from local announcements
        // (but won't find actual bid records since they're not in DHT)
        let bid_index = logic.discover_bids(&listing_key).await.unwrap();

        // The bids won't be in the index because the bid records aren't in the DHT
        // but the function should not fail
        assert!(bid_index.bids.is_empty());
    }

    #[tokio::test]
    async fn test_mpc_failure_handling() {
        let dht = MockDht::new();
        let transport = MockTransport::new();
        let mpc = MockMpcRunner::new();
        let time = MockTime::new(1000);
        let my_node_id = make_test_public_key(1);
        let bid_storage = BidStorage::new();

        // Set MPC to fail
        mpc.set_fail_mode(true).await;

        let logic = AuctionLogic::new(dht, transport, mpc, time, my_node_id, bid_storage);

        let result = logic.execute_mpc(0, 3, 100).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_process_bid_announcement() {
        let logic = create_test_logic();
        let listing_key = make_test_record_key(1);
        let bidder = make_test_public_key(5);
        let bid_key = make_test_record_key(50);

        logic
            .process_bid_announcement(listing_key.clone(), bidder, bid_key, 1000)
            .await
            .unwrap();

        assert_eq!(logic.get_local_bid_count(&listing_key).await, 1);
    }

    #[tokio::test]
    async fn test_register_bid_announcement_for_unwatched_listing() {
        let logic = create_test_logic();
        // Register a bid for a listing we never watched
        let unwatched_key = make_test_record_key(999);
        let bidder = make_test_public_key(5);
        let bid_key = make_test_record_key(50);

        logic
            .register_bid_announcement(&unwatched_key, bidder, bid_key)
            .await;

        // Bid should still be tracked (announcements are independent of watching)
        assert_eq!(logic.get_local_bid_count(&unwatched_key).await, 1);
    }

    #[tokio::test]
    async fn test_watch_listing_already_expired() {
        let dht = MockDht::new();
        let transport = MockTransport::new();
        let mpc = MockMpcRunner::new();
        let time = MockTime::new(10000); // Current time far past any deadline
        let my_node_id = make_test_public_key(1);
        let bid_storage = BidStorage::new();

        let logic = AuctionLogic::new(dht, transport, mpc, time.clone(), my_node_id, bid_storage);

        // Create a listing that was created at time=100 with 1s duration (expired at 101)
        let early_time = MockTime::new(100);
        let listing = Listing::builder_with_time(early_time)
            .key(make_test_record_key(1))
            .seller(make_test_public_key(2))
            .title("Already Expired")
            .encrypted_content(vec![1, 2, 3], [0u8; 12], "abc123".to_string())
            .reserve_price(100)
            .auction_duration(1)
            .build()
            .unwrap();

        logic.watch_listing(listing).await;

        // Should immediately appear as expired since current time (10000) > deadline (101)
        let expired = logic.get_expired_listings().await;
        assert_eq!(expired.len(), 1);
    }

    #[tokio::test]
    async fn test_get_expired_listings_none_expired() {
        let dht = MockDht::new();
        let transport = MockTransport::new();
        let mpc = MockMpcRunner::new();
        let time = MockTime::new(1000);
        let my_node_id = make_test_public_key(1);
        let bid_storage = BidStorage::new();

        let logic = AuctionLogic::new(dht, transport, mpc, time.clone(), my_node_id, bid_storage);

        // Add two listings, both with long durations
        let listing1 = Listing::builder_with_time(time.clone())
            .key(make_test_record_key(1))
            .seller(make_test_public_key(2))
            .title("Item A")
            .encrypted_content(vec![1], [0u8; 12], "a".to_string())
            .reserve_price(100)
            .auction_duration(7200)
            .build()
            .unwrap();
        let listing2 = Listing::builder_with_time(time.clone())
            .key(make_test_record_key(2))
            .seller(make_test_public_key(3))
            .title("Item B")
            .encrypted_content(vec![2], [0u8; 12], "b".to_string())
            .reserve_price(200)
            .auction_duration(7200)
            .build()
            .unwrap();

        logic.watch_listing(listing1).await;
        logic.watch_listing(listing2).await;

        let expired = logic.get_expired_listings().await;
        assert!(expired.is_empty());
    }

    #[tokio::test]
    async fn test_get_expired_listings_mixed() {
        let dht = MockDht::new();
        let transport = MockTransport::new();
        let mpc = MockMpcRunner::new();
        let time = MockTime::new(1000);
        let my_node_id = make_test_public_key(1);
        let bid_storage = BidStorage::new();

        let logic = AuctionLogic::new(dht, transport, mpc, time.clone(), my_node_id, bid_storage);

        // Short auction: expires at 1000 + 60 = 1060
        let short = Listing::builder_with_time(time.clone())
            .key(make_test_record_key(1))
            .seller(make_test_public_key(2))
            .title("Short Auction")
            .encrypted_content(vec![1], [0u8; 12], "a".to_string())
            .reserve_price(100)
            .auction_duration(60)
            .build()
            .unwrap();

        // Long auction: expires at 1000 + 7200 = 8200
        let long = Listing::builder_with_time(time.clone())
            .key(make_test_record_key(2))
            .seller(make_test_public_key(3))
            .title("Long Auction")
            .encrypted_content(vec![2], [0u8; 12], "b".to_string())
            .reserve_price(200)
            .auction_duration(7200)
            .build()
            .unwrap();

        logic.watch_listing(short).await;
        logic.watch_listing(long).await;

        // Advance past short deadline but before long deadline
        time.set(2000);

        let expired = logic.get_expired_listings().await;
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].1.title, "Short Auction");
    }

    #[tokio::test]
    async fn test_multiple_listings_independent_bids() {
        let logic = create_test_logic();
        let listing_a = make_test_record_key(1);
        let listing_b = make_test_record_key(2);

        // Register bids for listing A
        logic
            .register_bid_announcement(
                &listing_a,
                make_test_public_key(10),
                make_test_record_key(100),
            )
            .await;
        logic
            .register_bid_announcement(
                &listing_a,
                make_test_public_key(11),
                make_test_record_key(110),
            )
            .await;

        // Register bids for listing B
        logic
            .register_bid_announcement(
                &listing_b,
                make_test_public_key(20),
                make_test_record_key(200),
            )
            .await;

        // Bids should be independent
        assert_eq!(logic.get_local_bid_count(&listing_a).await, 2);
        assert_eq!(logic.get_local_bid_count(&listing_b).await, 1);
    }

    #[tokio::test]
    async fn test_store_decryption_key_overwrite() {
        let logic = create_test_logic();
        let listing_key = make_test_record_key(1);

        logic
            .store_decryption_key(&listing_key, "first_key".to_string())
            .await;
        assert_eq!(
            logic.get_decryption_key(&listing_key).await,
            Some("first_key".to_string())
        );

        logic
            .store_decryption_key(&listing_key, "second_key".to_string())
            .await;
        assert_eq!(
            logic.get_decryption_key(&listing_key).await,
            Some("second_key".to_string())
        );
    }
}
