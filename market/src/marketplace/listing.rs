use serde::{Deserialize, Serialize};
use veilid_core::{PublicKey, RecordKey};

use crate::config::now_unix;
use crate::crypto::ContentNonce;
use crate::traits::{SystemTimeProvider, TimeProvider};

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

    /// Hex-encoded AES-256 key for decryption (sent to winner after auction)
    /// The seller keeps this private and only reveals it to the auction winner
    pub decryption_key: String,

    /// Reserve price (in atomic units) - seller automatically bids this amount
    pub reserve_price: u64,

    /// Unix timestamp when the auction ends
    pub auction_end: u64,

    /// Unix timestamp when the listing was created
    pub created_at: u64,

    /// Current status of the auction
    pub status: ListingStatus,
}

/// A listing with the decryption key stripped out, safe for DHT publication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicListing {
    pub key: RecordKey,
    pub seller: PublicKey,
    pub title: String,
    pub encrypted_content: Vec<u8>,
    pub content_nonce: ContentNonce,
    pub reserve_price: u64,
    pub auction_end: u64,
    pub created_at: u64,
    pub status: ListingStatus,
}

impl PublicListing {
    /// Check if the auction is still active (hasn't ended yet)
    pub fn is_active(&self) -> bool {
        self.is_active_at(now_unix())
    }

    /// Check if the auction is still active at a specific timestamp
    pub fn is_active_at(&self, now: u64) -> bool {
        self.status == ListingStatus::Active && self.auction_end > now
    }

    /// Check if the auction has ended
    pub fn has_ended(&self) -> bool {
        self.has_ended_at(now_unix())
    }

    /// Check if the auction has ended at a specific timestamp
    pub const fn has_ended_at(&self, now: u64) -> bool {
        self.auction_end <= now
    }

    /// Get time remaining in seconds (0 if ended)
    pub fn time_remaining(&self) -> u64 {
        self.time_remaining_at(now_unix())
    }

    /// Get time remaining at a specific timestamp
    pub const fn time_remaining_at(&self, now: u64) -> u64 {
        self.auction_end.saturating_sub(now)
    }

    /// Decrypt the encrypted content using the provided hex-encoded decryption key
    /// Returns the decrypted content as a UTF-8 string
    pub fn decrypt_content_with_key(
        &self,
        decryption_key_hex: &str,
    ) -> Result<String, crate::error::MarketError> {
        use crate::crypto::{decrypt_content, ContentKey};
        use crate::error::MarketError;

        // Decode hex string to bytes
        let key_bytes = hex::decode(decryption_key_hex)
            .map_err(|e| MarketError::Crypto(format!("Invalid hex key: {e}")))?;

        // Convert to 32-byte array
        let len = key_bytes.len();
        let key: ContentKey = key_bytes
            .try_into()
            .map_err(|_| MarketError::Crypto(format!("Key must be 32 bytes, got {len} bytes")))?;

        // Decrypt using the crypto module
        decrypt_content(&self.encrypted_content, &key, &self.content_nonce)
    }

    /// Serialize the listing to CBOR bytes
    pub fn to_cbor(&self) -> Result<Vec<u8>, ciborium::ser::Error<std::io::Error>> {
        let mut buffer = Vec::new();
        ciborium::into_writer(self, &mut buffer)?;
        Ok(buffer)
    }

    /// Deserialize a public listing from CBOR bytes
    pub fn from_cbor(data: &[u8]) -> Result<Self, ciborium::de::Error<std::io::Error>> {
        ciborium::from_reader(data)
    }
}

impl Listing {
    /// Create a new listing builder
    pub const fn builder() -> ListingBuilder<SystemTimeProvider> {
        ListingBuilder::new(SystemTimeProvider::new())
    }

    /// Create a new listing builder with a custom time provider
    pub const fn builder_with_time<T: TimeProvider>(time: T) -> ListingBuilder<T> {
        ListingBuilder::new(time)
    }

    /// Check if the auction is still active (hasn't ended yet)
    pub fn is_active(&self) -> bool {
        self.is_active_at(now_unix())
    }

    /// Check if the auction is still active at a specific timestamp
    pub fn is_active_at(&self, now: u64) -> bool {
        self.status == ListingStatus::Active && self.auction_end > now
    }

    /// Check if the auction has ended
    pub fn has_ended(&self) -> bool {
        self.has_ended_at(now_unix())
    }

    /// Check if the auction has ended at a specific timestamp
    pub const fn has_ended_at(&self, now: u64) -> bool {
        self.auction_end <= now
    }

    /// Get time remaining in seconds (0 if ended)
    pub fn time_remaining(&self) -> u64 {
        self.time_remaining_at(now_unix())
    }

    /// Get time remaining at a specific timestamp
    pub const fn time_remaining_at(&self, now: u64) -> u64 {
        self.auction_end.saturating_sub(now)
    }

    /// Decrypt the encrypted content using the provided hex-encoded decryption key
    /// Returns the decrypted content as a UTF-8 string
    pub fn decrypt_content_with_key(
        &self,
        decryption_key_hex: &str,
    ) -> Result<String, crate::error::MarketError> {
        use crate::crypto::{decrypt_content, ContentKey};
        use crate::error::MarketError;

        // Decode hex string to bytes
        let key_bytes = hex::decode(decryption_key_hex)
            .map_err(|e| MarketError::Crypto(format!("Invalid hex key: {e}")))?;

        // Convert to 32-byte array
        let len = key_bytes.len();
        let key: ContentKey = key_bytes
            .try_into()
            .map_err(|_| MarketError::Crypto(format!("Key must be 32 bytes, got {len} bytes")))?;

        // Decrypt using the crypto module
        decrypt_content(&self.encrypted_content, &key, &self.content_nonce)
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

    /// Create a `PublicListing` with the decryption key stripped out.
    pub fn to_public(&self) -> PublicListing {
        PublicListing {
            key: self.key.clone(),
            seller: self.seller.clone(),
            title: self.title.clone(),
            encrypted_content: self.encrypted_content.clone(),
            content_nonce: self.content_nonce,
            reserve_price: self.reserve_price,
            auction_end: self.auction_end,
            created_at: self.created_at,
            status: self.status,
        }
    }
}

/// Builder for creating new listings
pub struct ListingBuilder<T: TimeProvider> {
    time: T,
    key: Option<RecordKey>,
    seller: Option<PublicKey>,
    title: Option<String>,
    encrypted_content: Option<Vec<u8>>,
    content_nonce: Option<ContentNonce>,
    decryption_key: Option<String>,
    reserve_price: Option<u64>,
    auction_duration_secs: Option<u64>,
}

impl<T: TimeProvider> ListingBuilder<T> {
    /// Create a new builder with a time provider
    pub const fn new(time: T) -> Self {
        Self {
            time,
            key: None,
            seller: None,
            title: None,
            encrypted_content: None,
            content_nonce: None,
            decryption_key: None,
            reserve_price: None,
            auction_duration_secs: None,
        }
    }
}

impl<T: TimeProvider> ListingBuilder<T> {
    #[must_use]
    pub fn key(mut self, key: RecordKey) -> Self {
        self.key = Some(key);
        self
    }

    #[must_use]
    pub fn seller(mut self, seller: PublicKey) -> Self {
        self.seller = Some(seller);
        self
    }

    #[must_use]
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    #[must_use]
    pub fn encrypted_content(mut self, content: Vec<u8>, nonce: ContentNonce, key: String) -> Self {
        self.encrypted_content = Some(content);
        self.content_nonce = Some(nonce);
        self.decryption_key = Some(key);
        self
    }

    #[must_use]
    pub const fn reserve_price(mut self, amount: u64) -> Self {
        self.reserve_price = Some(amount);
        self
    }

    /// Set auction duration in seconds from now
    #[must_use]
    pub const fn auction_duration(mut self, seconds: u64) -> Self {
        self.auction_duration_secs = Some(seconds);
        self
    }

    /// Build the listing (returns error if required fields are missing)
    pub fn build(self) -> Result<Listing, String> {
        let created_at = self.time.now_unix();

        Ok(Listing {
            key: self.key.ok_or("key is required")?,
            seller: self.seller.ok_or("seller is required")?,
            title: self.title.ok_or("title is required")?,
            encrypted_content: self
                .encrypted_content
                .ok_or("encrypted_content is required")?,
            content_nonce: self.content_nonce.ok_or("content_nonce is required")?,
            decryption_key: self.decryption_key.ok_or("decryption_key is required")?,
            reserve_price: self.reserve_price.ok_or("reserve_price is required")?,
            auction_end: created_at
                + self
                    .auction_duration_secs
                    .ok_or("auction_duration is required")?,
            created_at,
            status: ListingStatus::Active,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocks::{make_test_public_key, make_test_record_key, MockTime};

    fn make_test_key() -> RecordKey {
        make_test_record_key(1)
    }

    fn make_test_pubkey() -> PublicKey {
        make_test_public_key(2)
    }

    fn make_test_listing(time: &MockTime) -> Listing {
        Listing::builder_with_time(time.clone())
            .key(make_test_key())
            .seller(make_test_pubkey())
            .title("Test Auction")
            .encrypted_content(vec![1, 2, 3], [0u8; 12], "abc123".to_string())
            .reserve_price(100)
            .auction_duration(3600) // 1 hour
            .build()
            .unwrap()
    }

    #[test]
    fn test_listing_builder_valid() {
        let time = MockTime::new(1000);
        let listing = make_test_listing(&time);

        assert_eq!(listing.title, "Test Auction");
        assert_eq!(listing.reserve_price, 100);
        assert_eq!(listing.created_at, 1000);
        assert_eq!(listing.auction_end, 4600); // 1000 + 3600
        assert_eq!(listing.status, ListingStatus::Active);
    }

    #[test]
    fn test_listing_builder_missing_key() {
        let time = MockTime::new(1000);
        let result = Listing::builder_with_time(time)
            .seller(make_test_pubkey())
            .title("Test")
            .encrypted_content(vec![1], [0u8; 12], "key".to_string())
            .reserve_price(100)
            .auction_duration(3600)
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("key is required"));
    }

    #[test]
    fn test_listing_builder_missing_seller() {
        let time = MockTime::new(1000);
        let result = Listing::builder_with_time(time)
            .key(make_test_key())
            .title("Test")
            .encrypted_content(vec![1], [0u8; 12], "key".to_string())
            .reserve_price(100)
            .auction_duration(3600)
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("seller is required"));
    }

    #[test]
    fn test_listing_builder_missing_title() {
        let time = MockTime::new(1000);
        let result = Listing::builder_with_time(time)
            .key(make_test_key())
            .seller(make_test_pubkey())
            .encrypted_content(vec![1], [0u8; 12], "key".to_string())
            .reserve_price(100)
            .auction_duration(3600)
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("title is required"));
    }

    #[test]
    fn test_listing_builder_missing_auction_duration() {
        let time = MockTime::new(1000);
        let result = Listing::builder_with_time(time)
            .key(make_test_key())
            .seller(make_test_pubkey())
            .title("Test")
            .encrypted_content(vec![1], [0u8; 12], "key".to_string())
            .reserve_price(100)
            .build();

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("auction_duration is required"));
    }

    #[test]
    fn test_listing_is_active() {
        let time = MockTime::new(1000);
        let listing = make_test_listing(&time);

        // Active during auction
        assert!(listing.is_active_at(1000));
        assert!(listing.is_active_at(4599));

        // Not active after auction ends
        assert!(!listing.is_active_at(4600));
        assert!(!listing.is_active_at(5000));
    }

    #[test]
    fn test_listing_has_ended() {
        let time = MockTime::new(1000);
        let listing = make_test_listing(&time);

        // Not ended during auction
        assert!(!listing.has_ended_at(1000));
        assert!(!listing.has_ended_at(4599));

        // Ended at and after deadline
        assert!(listing.has_ended_at(4600));
        assert!(listing.has_ended_at(5000));
    }

    #[test]
    fn test_listing_time_remaining() {
        let time = MockTime::new(1000);
        let listing = make_test_listing(&time);

        // Full time at start
        assert_eq!(listing.time_remaining_at(1000), 3600);

        // Half time
        assert_eq!(listing.time_remaining_at(2800), 1800);

        // Zero at end
        assert_eq!(listing.time_remaining_at(4600), 0);

        // Zero after end
        assert_eq!(listing.time_remaining_at(5000), 0);
    }

    #[test]
    fn test_listing_inactive_when_cancelled() {
        let time = MockTime::new(1000);
        let mut listing = make_test_listing(&time);
        listing.status = ListingStatus::Cancelled;

        // Not active even during auction window
        assert!(!listing.is_active_at(2000));
    }

    #[test]
    fn test_listing_serialization_roundtrip() {
        let time = MockTime::new(1000);
        let original = make_test_listing(&time);

        let cbor = original.to_cbor().unwrap();
        let restored = Listing::from_cbor(&cbor).unwrap();

        assert_eq!(original.key, restored.key);
        assert_eq!(original.seller, restored.seller);
        assert_eq!(original.title, restored.title);
        assert_eq!(original.encrypted_content, restored.encrypted_content);
        assert_eq!(original.content_nonce, restored.content_nonce);
        assert_eq!(original.decryption_key, restored.decryption_key);
        assert_eq!(original.reserve_price, restored.reserve_price);
        assert_eq!(original.auction_end, restored.auction_end);
        assert_eq!(original.created_at, restored.created_at);
        assert_eq!(original.status, restored.status);
    }

    #[test]
    fn test_listing_decrypt_content() {
        use crate::crypto::{encrypt_content, generate_key};

        let time = MockTime::new(1000);
        let key = generate_key();
        let plaintext = "Secret auction item details";
        let (ciphertext, nonce) = encrypt_content(plaintext, &key).unwrap();

        let listing = Listing::builder_with_time(time)
            .key(make_test_key())
            .seller(make_test_pubkey())
            .title("Test")
            .encrypted_content(ciphertext, nonce, hex::encode(key))
            .reserve_price(100)
            .auction_duration(3600)
            .build()
            .unwrap();

        let decrypted = listing
            .decrypt_content_with_key(&listing.decryption_key)
            .unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_listing_decrypt_with_wrong_key() {
        use crate::crypto::{encrypt_content, generate_key};

        let time = MockTime::new(1000);
        let key = generate_key();
        let wrong_key = generate_key();
        let plaintext = "Secret";
        let (ciphertext, nonce) = encrypt_content(plaintext, &key).unwrap();

        let listing = Listing::builder_with_time(time)
            .key(make_test_key())
            .seller(make_test_pubkey())
            .title("Test")
            .encrypted_content(ciphertext, nonce, hex::encode(key))
            .reserve_price(100)
            .auction_duration(3600)
            .build()
            .unwrap();

        let result = listing.decrypt_content_with_key(&hex::encode(wrong_key));
        assert!(result.is_err());
    }

    #[test]
    fn test_public_listing_serialization_roundtrip() {
        let time = MockTime::new(1000);
        let original = make_test_listing(&time);
        let decryption_key = original.decryption_key.clone();

        // Convert to PublicListing (strips decryption_key)
        let public = original.to_public();

        // Serialize to CBOR
        let cbor = public.to_cbor().unwrap();

        // Deserialize back
        let restored = PublicListing::from_cbor(&cbor).unwrap();

        // Verify all fields match
        assert_eq!(public.key, restored.key);
        assert_eq!(public.seller, restored.seller);
        assert_eq!(public.title, restored.title);
        assert_eq!(public.encrypted_content, restored.encrypted_content);
        assert_eq!(public.content_nonce, restored.content_nonce);
        assert_eq!(public.reserve_price, restored.reserve_price);
        assert_eq!(public.auction_end, restored.auction_end);
        assert_eq!(public.created_at, restored.created_at);
        assert_eq!(public.status, restored.status);

        // Verify the CBOR bytes do NOT contain the decryption key
        let cbor_str = String::from_utf8_lossy(&cbor);
        assert!(
            !cbor_str.contains(&decryption_key),
            "CBOR bytes must not contain the decryption key"
        );
    }

    #[test]
    fn test_public_listing_is_active() {
        let time = MockTime::new(1000);
        let listing = make_test_listing(&time);
        let public = listing.to_public();

        // Active during auction
        assert!(public.is_active_at(1000));
        assert!(public.is_active_at(4599));

        // Not active after auction ends
        assert!(!public.is_active_at(4600));
        assert!(!public.is_active_at(5000));
    }

    #[test]
    fn test_public_listing_has_ended() {
        let time = MockTime::new(1000);
        let listing = make_test_listing(&time);
        let public = listing.to_public();

        // Not ended during auction
        assert!(!public.has_ended_at(1000));
        assert!(!public.has_ended_at(4599));

        // Ended at and after deadline
        assert!(public.has_ended_at(4600));
        assert!(public.has_ended_at(5000));
    }

    #[test]
    fn test_public_listing_time_remaining() {
        let time = MockTime::new(1000);
        let listing = make_test_listing(&time);
        let public = listing.to_public();

        // Full time at start
        assert_eq!(public.time_remaining_at(1000), 3600);

        // Half time
        assert_eq!(public.time_remaining_at(2800), 1800);

        // Zero at end
        assert_eq!(public.time_remaining_at(4600), 0);

        // Zero after end
        assert_eq!(public.time_remaining_at(5000), 0);
    }

    #[test]
    fn test_public_listing_decrypt_content_wrong_key() {
        use crate::crypto::{encrypt_content, generate_key};

        let time = MockTime::new(1000);
        let key = generate_key();
        let wrong_key = generate_key();
        let plaintext = "Secret";
        let (ciphertext, nonce) = encrypt_content(plaintext, &key).unwrap();

        let listing = Listing::builder_with_time(time)
            .key(make_test_key())
            .seller(make_test_pubkey())
            .title("Test")
            .encrypted_content(ciphertext, nonce, hex::encode(key))
            .reserve_price(100)
            .auction_duration(3600)
            .build()
            .unwrap();

        let public = listing.to_public();

        let result = public.decrypt_content_with_key(&hex::encode(wrong_key));
        assert!(result.is_err());
    }

    #[test]
    fn test_listing_status_display() {
        // Verify Debug formatting works for all ListingStatus variants
        assert_eq!(format!("{:?}", ListingStatus::Active), "Active");
        assert_eq!(format!("{:?}", ListingStatus::Closed), "Closed");
        assert_eq!(format!("{:?}", ListingStatus::Completed), "Completed");
        assert_eq!(format!("{:?}", ListingStatus::Cancelled), "Cancelled");
    }

    #[test]
    fn test_listing_builder_all_fields() {
        use crate::crypto::{encrypt_content, generate_key};

        let time = MockTime::new(5000);
        let key = generate_key();
        let plaintext = "Detailed item description";
        let (ciphertext, nonce) = encrypt_content(plaintext, &key).unwrap();

        let listing = Listing::builder_with_time(time)
            .key(make_test_key())
            .seller(make_test_pubkey())
            .title("Comprehensive Test Listing")
            .encrypted_content(ciphertext, nonce, hex::encode(key))
            .reserve_price(500)
            .auction_duration(7200) // 2 hours
            .build()
            .unwrap();

        assert_eq!(listing.title, "Comprehensive Test Listing");
        assert_eq!(listing.reserve_price, 500);
        assert_eq!(listing.created_at, 5000);
        assert_eq!(listing.auction_end, 12200); // 5000 + 7200
        assert_eq!(listing.status, ListingStatus::Active);
        assert!(!listing.encrypted_content.is_empty());
        assert_eq!(listing.content_nonce.len(), 12);
        assert!(!listing.decryption_key.is_empty());
    }
}
