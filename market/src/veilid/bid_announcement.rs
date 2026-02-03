use anyhow::Result;
use serde::{Deserialize, Serialize};
use veilid_core::{PublicKey, RecordKey, RouteBlob};

use crate::traits::{SystemTimeProvider, TimeProvider};

/// Registry of bid announcements stored in DHT (listing subkey 2)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BidAnnouncementRegistry {
    /// List of (bidder_pubkey, bid_record_key, timestamp) tuples
    pub announcements: Vec<(PublicKey, RecordKey, u64)>,
}

impl BidAnnouncementRegistry {
    pub fn new() -> Self {
        Self {
            announcements: Vec::new(),
        }
    }

    /// Add a bid announcement to the registry
    pub fn add(&mut self, bidder: PublicKey, bid_record_key: RecordKey, timestamp: u64) {
        // Check if already exists (avoid duplicates)
        if !self.announcements.iter().any(|(b, _, _)| b == &bidder) {
            self.announcements.push((bidder, bid_record_key, timestamp));
        }
    }

    /// Serialize to bytes for DHT storage
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self)
            .map_err(|e| anyhow::anyhow!("Failed to serialize bid registry: {}", e))
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize bid registry: {}", e))
    }
}

/// Message types for auction coordination via Veilid AppMessages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuctionMessage {
    /// Announce participation in an auction
    BidAnnouncement {
        /// Listing this bid is for
        listing_key: RecordKey,
        /// Bidder's public key
        bidder: PublicKey,
        /// DHT key where BidRecord is stored
        bid_record_key: RecordKey,
        /// Timestamp of announcement
        timestamp: u64,
    },
    /// Winner requests decryption key from seller
    WinnerDecryptionRequest {
        /// Listing this request is for
        listing_key: RecordKey,
        /// Winner's public key
        winner: PublicKey,
        /// Timestamp of request
        timestamp: u64,
    },
    /// Transfer decryption hash from seller to winner
    DecryptionHashTransfer {
        /// Listing this hash is for
        listing_key: RecordKey,
        /// Winner's public key
        winner: PublicKey,
        /// Decryption hash for the encrypted description
        decryption_hash: String,
        /// Timestamp of transfer
        timestamp: u64,
    },
    /// Announce MPC route for coordination
    MpcRouteAnnouncement {
        /// Listing this route is for
        listing_key: RecordKey,
        /// Party's public key
        party_pubkey: PublicKey,
        /// Veilid route blob for importing
        route_blob: RouteBlob,
        /// Timestamp of announcement
        timestamp: u64,
    },
    /// Winner reveals bid value and nonce for verification (Danish Sugar Beet style)
    WinnerBidReveal {
        /// Listing this reveal is for
        listing_key: RecordKey,
        /// Winner's public key
        winner: PublicKey,
        /// The bid value being revealed
        bid_value: u64,
        /// The nonce used in the commitment
        nonce: [u8; 32],
        /// Timestamp of reveal
        timestamp: u64,
    },
}

impl AuctionMessage {
    /// Create a bid announcement using system time.
    pub fn bid_announcement(
        listing_key: RecordKey,
        bidder: PublicKey,
        bid_record_key: RecordKey,
    ) -> Self {
        Self::bid_announcement_with_time(listing_key, bidder, bid_record_key, &SystemTimeProvider::new())
    }

    /// Create a bid announcement with a custom time provider.
    pub fn bid_announcement_with_time<T: TimeProvider>(
        listing_key: RecordKey,
        bidder: PublicKey,
        bid_record_key: RecordKey,
        time: &T,
    ) -> Self {
        Self::BidAnnouncement {
            listing_key,
            bidder,
            bid_record_key,
            timestamp: time.now_unix(),
        }
    }

    /// Create a winner decryption request using system time.
    pub fn winner_decryption_request(
        listing_key: RecordKey,
        winner: PublicKey,
    ) -> Self {
        Self::winner_decryption_request_with_time(listing_key, winner, &SystemTimeProvider::new())
    }

    /// Create a winner decryption request with a custom time provider.
    pub fn winner_decryption_request_with_time<T: TimeProvider>(
        listing_key: RecordKey,
        winner: PublicKey,
        time: &T,
    ) -> Self {
        Self::WinnerDecryptionRequest {
            listing_key,
            winner,
            timestamp: time.now_unix(),
        }
    }

    /// Create a decryption hash transfer using system time.
    pub fn decryption_hash_transfer(
        listing_key: RecordKey,
        winner: PublicKey,
        decryption_hash: String,
    ) -> Self {
        Self::decryption_hash_transfer_with_time(listing_key, winner, decryption_hash, &SystemTimeProvider::new())
    }

    /// Create a decryption hash transfer with a custom time provider.
    pub fn decryption_hash_transfer_with_time<T: TimeProvider>(
        listing_key: RecordKey,
        winner: PublicKey,
        decryption_hash: String,
        time: &T,
    ) -> Self {
        Self::DecryptionHashTransfer {
            listing_key,
            winner,
            decryption_hash,
            timestamp: time.now_unix(),
        }
    }

    /// Create an MPC route announcement using system time.
    pub fn mpc_route_announcement(
        listing_key: RecordKey,
        party_pubkey: PublicKey,
        route_blob: RouteBlob,
    ) -> Self {
        Self::mpc_route_announcement_with_time(listing_key, party_pubkey, route_blob, &SystemTimeProvider::new())
    }

    /// Create an MPC route announcement with a custom time provider.
    pub fn mpc_route_announcement_with_time<T: TimeProvider>(
        listing_key: RecordKey,
        party_pubkey: PublicKey,
        route_blob: RouteBlob,
        time: &T,
    ) -> Self {
        Self::MpcRouteAnnouncement {
            listing_key,
            party_pubkey,
            route_blob,
            timestamp: time.now_unix(),
        }
    }

    /// Create a winner bid reveal using system time.
    pub fn winner_bid_reveal(
        listing_key: RecordKey,
        winner: PublicKey,
        bid_value: u64,
        nonce: [u8; 32],
    ) -> Self {
        Self::winner_bid_reveal_with_time(listing_key, winner, bid_value, nonce, &SystemTimeProvider::new())
    }

    /// Create a winner bid reveal with a custom time provider.
    pub fn winner_bid_reveal_with_time<T: TimeProvider>(
        listing_key: RecordKey,
        winner: PublicKey,
        bid_value: u64,
        nonce: [u8; 32],
        time: &T,
    ) -> Self {
        Self::WinnerBidReveal {
            listing_key,
            winner,
            bid_value,
            nonce,
            timestamp: time.now_unix(),
        }
    }

    /// Serialize to bytes for transmission
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self)
            .map_err(|e| anyhow::anyhow!("Failed to serialize auction message: {}", e))
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize auction message: {}", e))
    }
}
