//! Multi-party test harness for integration testing.
//!
//! This harness simulates an N-party auction using mock implementations,
//! allowing fast, deterministic testing of the auction protocol.

use std::sync::Arc;

use market::mocks::{
    make_test_public_key, make_test_record_key, MockMpcRunner, MockTime, MockTransport,
    SharedBidRegistry, SharedDhtHandle, SharedMockDht,
};
use market::veilid::auction_logic::AuctionLogic;
use market::veilid::bid_storage::BidStorage;
use market::{BidRecord, DhtStore, Listing, TimeProvider};
use veilid_core::PublicKey;

/// Context for a single party in the auction.
#[allow(dead_code)]
pub struct PartyContext {
    pub party_id: usize,
    pub node_id: PublicKey,
    pub auction_logic: AuctionLogic<SharedMockDht, MockTransport, MockMpcRunner, MockTime>,
    pub bid_storage: BidStorage,
    pub transport: MockTransport,
    pub dht: SharedMockDht,
}

/// Simulates an N-party auction using mocks.
pub struct MultiPartyHarness {
    parties: Vec<PartyContext>,
    time: MockTime,
    bid_registry: Arc<SharedBidRegistry>,
    listing_counter: u64,
}

#[allow(dead_code)]
impl MultiPartyHarness {
    /// Create a new harness with the specified number of parties.
    pub async fn new(num_parties: usize) -> Self {
        let time = MockTime::new(1000);
        let bid_registry = Arc::new(SharedBidRegistry::new());
        let dht_handle = SharedDhtHandle::new();

        let mut parties = Vec::with_capacity(num_parties);

        for i in 0..num_parties {
            let node_id = make_test_public_key(i as u8 + 1);
            let dht = dht_handle.create_party_view(node_id.clone());
            let transport = MockTransport::new();
            let mpc = MockMpcRunner::with_shared_registry(bid_registry.clone());
            let bid_storage = BidStorage::new();

            // Add all other parties as peers
            for j in 0..num_parties {
                if i != j {
                    let peer_id = make_test_public_key(j as u8 + 1);
                    transport.add_peer(peer_id).await;
                }
            }

            let logic = AuctionLogic::new(
                dht.clone(),
                transport.clone(),
                mpc,
                time.clone(),
                node_id.clone(),
                bid_storage.clone(),
            );

            parties.push(PartyContext {
                party_id: i,
                node_id,
                auction_logic: logic,
                bid_storage,
                transport,
                dht,
            });
        }

        Self {
            parties,
            time,
            bid_registry,
            listing_counter: 1,
        }
    }

    /// Get a reference to a party by index.
    pub fn party(&self, index: usize) -> &PartyContext {
        &self.parties[index]
    }

    /// Get a mutable reference to a party by index.
    pub fn party_mut(&mut self, index: usize) -> &mut PartyContext {
        &mut self.parties[index]
    }

    /// Get the number of parties.
    pub fn num_parties(&self) -> usize {
        self.parties.len()
    }

    /// Get the shared time provider.
    pub fn time(&self) -> &MockTime {
        &self.time
    }

    /// Advance time by the specified number of seconds.
    pub fn advance_time(&self, seconds: u64) {
        self.time.advance(seconds);
    }

    /// Set the current time.
    pub fn set_time(&self, timestamp: u64) {
        self.time.set(timestamp);
    }

    /// Party 0 creates a listing.
    pub async fn create_listing(
        &mut self,
        title: &str,
        reserve_price: u64,
        duration: u64,
    ) -> Listing {
        let record_key = make_test_record_key(self.listing_counter);
        self.listing_counter += 1;

        let seller = self.parties[0].node_id.clone();

        let listing = Listing::builder_with_time(self.time.clone())
            .key(record_key.clone())
            .seller(seller)
            .title(title)
            .encrypted_content(
                vec![1, 2, 3, 4, 5], // Dummy encrypted content
                [0u8; 12],
                "decryption_key_hex".to_string(),
            )
            .reserve_price(reserve_price)
            .auction_duration(duration)
            .build()
            .expect("Failed to build listing");

        // Store listing in shared DHT
        let record = self.parties[0].dht.create_record().await.unwrap();
        let cbor = listing.to_cbor().unwrap();
        self.parties[0].dht.set_value(&record, cbor).await.unwrap();

        // All parties watch the listing (PublicListing — no decryption key in watched state)
        for party in &self.parties {
            party.auction_logic.watch_listing(listing.to_public()).await;
        }

        listing
    }

    /// A party places a bid on a listing.
    pub async fn place_bid(&mut self, party_id: usize, listing: &Listing, amount: u64) {
        let timestamp = self.time.now_unix();
        self.place_bid_inner(party_id, listing, amount, timestamp)
            .await;
        self.bid_registry.register_bid(party_id, amount).await;
    }

    /// Place a bid with a specific timestamp (for tie-break testing).
    pub async fn place_bid_with_timestamp(
        &mut self,
        party_id: usize,
        listing: &Listing,
        amount: u64,
        timestamp: u64,
    ) {
        self.place_bid_inner(party_id, listing, amount, timestamp)
            .await;
        self.bid_registry
            .register_bid_with_timestamp(party_id, amount, timestamp)
            .await;
    }

    /// Shared implementation for placing a bid: creates commitment, stores in
    /// DHT, and broadcasts the announcement to all parties.
    async fn place_bid_inner(
        &mut self,
        party_id: usize,
        listing: &Listing,
        amount: u64,
        timestamp: u64,
    ) {
        // Create commitment (simplified for testing)
        let mut commitment = [0u8; 32];
        commitment[..8].copy_from_slice(&amount.to_le_bytes());
        commitment[8..16].copy_from_slice(&(party_id as u64).to_le_bytes());

        // Store bid locally
        self.parties[party_id]
            .bid_storage
            .store_bid(&listing.key, amount, commitment)
            .await;

        // Create bid record in DHT
        let bid_record = self.parties[party_id].dht.create_record().await.unwrap();
        let bid_key = SharedMockDht::record_key(&bid_record);

        let bidder_node_id = self.parties[party_id].node_id.clone();

        let bid = BidRecord {
            listing_key: listing.key.clone(),
            bidder: bidder_node_id.clone(),
            commitment,
            timestamp,
            bid_key: bid_key.clone(),
        };

        // Serialize and store bid record
        let mut buffer = Vec::new();
        ciborium::into_writer(&bid, &mut buffer).unwrap();
        self.parties[party_id]
            .dht
            .set_value(&bid_record, buffer)
            .await
            .unwrap();

        // Register bid announcement with all parties
        for p in &self.parties {
            p.auction_logic
                .register_bid_announcement(&listing.key, bidder_node_id.clone(), bid_key.clone())
                .await;
        }
    }

    /// Advance time past the listing deadline.
    pub fn advance_to_deadline(&self, listing: &Listing) {
        let deadline = listing.auction_end;
        self.time.set(deadline + 1);
    }

    /// Execute the auction MPC for all participating parties.
    ///
    /// Returns the winning party ID.
    pub async fn execute_auction(&self, listing: &Listing) -> Option<usize> {
        let num_parties = self.parties.len();
        let mut results = Vec::new();

        for (i, party) in self.parties.iter().enumerate() {
            if let Some((bid_value, _)) = party.bid_storage.get_bid(&listing.key).await {
                let result = party
                    .auction_logic
                    .execute_mpc(i, num_parties, bid_value)
                    .await
                    .expect("MPC execution failed");
                results.push((i, result));
            }
        }

        // Find the winner
        results.iter().find(|(_, r)| r.is_winner).map(|(i, _)| *i)
    }

    /// Check if a party received the decryption key.
    pub async fn party_got_decryption_key(&self, party_id: usize, listing: &Listing) -> bool {
        self.parties[party_id]
            .auction_logic
            .get_decryption_key(&listing.key)
            .await
            .is_some()
    }

    /// Simulate the winner requesting and receiving the decryption key.
    pub async fn transfer_decryption_key(&self, listing: &Listing, winner_id: usize) {
        let winner = &self.parties[winner_id];

        // Store decryption key for winner
        winner
            .auction_logic
            .store_decryption_key(&listing.key, listing.decryption_key.clone())
            .await;
    }

    /// Get the bid registry for inspection.
    pub fn bid_registry(&self) -> &SharedBidRegistry {
        &self.bid_registry
    }

    /// Clear bid registry (for running multiple auctions).
    pub async fn clear_bid_registry(&self) {
        self.bid_registry.clear().await;
    }

    /// Get bids registered for a specific party.
    pub async fn get_registered_bids(&self) -> std::collections::HashMap<usize, (u64, u64)> {
        self.bid_registry.get_bids().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_harness_creation() {
        let harness = MultiPartyHarness::new(3).await;
        assert_eq!(harness.num_parties(), 3);
    }

    #[tokio::test]
    async fn test_harness_create_listing() {
        let mut harness = MultiPartyHarness::new(3).await;
        let listing = harness.create_listing("Test Item", 100, 3600).await;

        assert_eq!(listing.title, "Test Item");
        assert_eq!(listing.reserve_price, 100);
    }

    #[tokio::test]
    async fn test_harness_place_bid() {
        let mut harness = MultiPartyHarness::new(3).await;
        let listing = harness.create_listing("Test Item", 100, 3600).await;

        harness.place_bid(0, &listing, 150).await;

        // Party 0 should have the bid stored
        assert!(harness.party(0).bid_storage.has_bid(&listing.key).await);

        // All parties should have the bid announcement
        for i in 0..3 {
            let count = harness
                .party(i)
                .auction_logic
                .get_local_bid_count(&listing.key)
                .await;
            assert_eq!(count, 1);
        }
    }
}
