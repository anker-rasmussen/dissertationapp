use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use veilid_core::{PublicKey, RecordKey};

use crate::crypto::ContentNonce;

/// Status of a listing in the auction marketplace
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ListingStatus {
    /// Auction is open and accepting bids
    Active,
    /// Auction has ended, waiting for winner determination
    Closed,
    /// Winner has been determined
    Completed,
    /// Auction was cancelled by seller
    Cancelled,
}

/// A marketplace listing for a sealed-bid auction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Listing {
    /// DHT record key where this listing is stored
    pub key: RecordKey,

    /// Public key of the seller
    pub seller: PublicKey,

    /// Title of the item (publicly visible)
    pub title: String,

    /// AES-256-GCM encrypted text content (will be decrypted via MPC for winner)
    /// The plaintext is UTF-8 text that describes the item/service being auctioned
    pub encrypted_content: Vec<u8>,

    /// AES-256-GCM nonce (12 bytes) - required for decryption
    pub content_nonce: ContentNonce,

    /// Minimum bid amount (in atomic units, e.g., piconeros for XMR)
    pub min_bid: u64,

    /// Unix timestamp when the auction ends
    pub auction_end: u64,

    /// Unix timestamp when the listing was created
    pub created_at: u64,

    /// Current status of the auction
    pub status: ListingStatus,

    /// Number of bids received (optional, for display purposes)
    pub bid_count: u32,
}

impl Listing {
    /// Create a new listing builder
    pub fn builder() -> ListingBuilder {
        ListingBuilder::default()
    }

    /// Check if the auction is still active (hasn't ended yet)
    pub fn is_active(&self) -> bool {
        self.status == ListingStatus::Active && self.auction_end > current_timestamp()
    }

    /// Check if the auction has ended
    pub fn has_ended(&self) -> bool {
        self.auction_end <= current_timestamp()
    }

    /// Get time remaining in seconds (0 if ended)
    pub fn time_remaining(&self) -> u64 {
        let now = current_timestamp();
        if now >= self.auction_end {
            0
        } else {
            self.auction_end - now
        }
    }

    /// Serialize the listing to CBOR bytes
    pub fn to_cbor(&self) -> Result<Vec<u8>, ciborium::ser::Error<std::io::Error>> {
        let mut buffer = Vec::new();
        ciborium::into_writer(self, &mut buffer)?;
        Ok(buffer)
    }

    /// Deserialize a listing from CBOR bytes
    pub fn from_cbor(data: &[u8]) -> Result<Self, ciborium::de::Error<std::io::Error>> {
        ciborium::from_reader(data)
    }
}

/// Builder for creating new listings
#[derive(Default)]
pub struct ListingBuilder {
    key: Option<RecordKey>,
    seller: Option<PublicKey>,
    title: Option<String>,
    encrypted_content: Option<Vec<u8>>,
    content_nonce: Option<ContentNonce>,
    min_bid: Option<u64>,
    auction_duration_secs: Option<u64>,
}

impl ListingBuilder {
    pub fn key(mut self, key: RecordKey) -> Self {
        self.key = Some(key);
        self
    }

    pub fn seller(mut self, seller: PublicKey) -> Self {
        self.seller = Some(seller);
        self
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn encrypted_content(mut self, content: Vec<u8>, nonce: ContentNonce) -> Self {
        self.encrypted_content = Some(content);
        self.content_nonce = Some(nonce);
        self
    }

    pub fn min_bid(mut self, amount: u64) -> Self {
        self.min_bid = Some(amount);
        self
    }

    /// Set auction duration in seconds from now
    pub fn auction_duration(mut self, seconds: u64) -> Self {
        self.auction_duration_secs = Some(seconds);
        self
    }

    /// Build the listing (returns error if required fields are missing)
    pub fn build(self) -> Result<Listing, String> {
        let created_at = current_timestamp();

        Ok(Listing {
            key: self.key.ok_or("key is required")?,
            seller: self.seller.ok_or("seller is required")?,
            title: self.title.ok_or("title is required")?,
            encrypted_content: self.encrypted_content.ok_or("encrypted_content is required")?,
            content_nonce: self.content_nonce.ok_or("content_nonce is required")?,
            min_bid: self.min_bid.ok_or("min_bid is required")?,
            auction_end: created_at + self.auction_duration_secs.ok_or("auction_duration is required")?,
            created_at,
            status: ListingStatus::Active,
            bid_count: 0,
        })
    }
}

/// Get current Unix timestamp in seconds
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time is before Unix epoch")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_listing_builder() {
        // This test requires actual Veilid types, so it's more of a compilation test
        // Real tests should be in integration tests with a running Veilid node
    }

    #[test]
    fn test_listing_serialization() {
        // Test CBOR serialization round-trip
        // Will be implemented with actual data in integration tests
    }
}
