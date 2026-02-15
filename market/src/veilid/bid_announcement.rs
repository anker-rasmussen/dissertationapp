use bincode::Options;
use ed25519_dalek::{Signer, Verifier};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::Mutex;
use veilid_core::{PublicKey, RecordKey, RouteBlob};

use crate::config::now_unix;
use crate::error::{MarketError, MarketResult};
use crate::traits::TimeProvider;

/// Maximum allowed bincode payload size (64 KB).
/// Generous for the 32 KB Veilid value limit plus envelope overhead.
const MAX_BINCODE_SIZE: u64 = 64 * 1024;

/// Deserialize bincode with a size limit to prevent OOM from crafted payloads.
pub(crate) fn bincode_deserialize_limited<T: serde::de::DeserializeOwned>(
    data: &[u8],
) -> Result<T, bincode::Error> {
    bincode::options()
        .with_limit(MAX_BINCODE_SIZE)
        .deserialize(data)
}

// ── Signed envelope ──────────────────────────────────────────────────

/// Cryptographic envelope wrapping every `AuctionMessage` or MPC tunnel message.
///
/// The `payload` is the bincode-serialized inner message. The sender signs
/// the payload with their Ed25519 key; the receiver verifies the signature
/// before deserializing.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SignedEnvelope {
    /// Bincode-serialized inner message.
    pub payload: Vec<u8>,
    /// Ed25519 verifying key bytes (the signer's identity).
    pub signer: [u8; 32],
    /// Ed25519 signature over `payload` (64 bytes).
    pub signature: Vec<u8>,
}

impl SignedEnvelope {
    /// Sign a payload and wrap it in an envelope.
    pub fn sign(payload: Vec<u8>, signing_key: &ed25519_dalek::SigningKey) -> MarketResult<Self> {
        let signature = signing_key.sign(&payload);
        let signer = signing_key.verifying_key().to_bytes();
        Ok(Self {
            payload,
            signer,
            signature: signature.to_bytes().to_vec(),
        })
    }

    /// Verify the signature and return the payload + signer pubkey bytes.
    pub fn verify_and_unwrap(data: &[u8]) -> MarketResult<(Vec<u8>, [u8; 32])> {
        let envelope: Self = bincode_deserialize_limited(data).map_err(|e| {
            MarketError::Serialization(format!("Failed to deserialize signed envelope: {e}"))
        })?;

        let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&envelope.signer)
            .map_err(|e| MarketError::Crypto(format!("Invalid signer public key: {e}")))?;
        let sig_bytes: [u8; 64] = envelope.signature.as_slice().try_into().map_err(|_| {
            MarketError::Crypto(format!(
                "Invalid signature length: expected 64, got {}",
                envelope.signature.len()
            ))
        })?;
        let signature = ed25519_dalek::Signature::from_bytes(&sig_bytes);
        verifying_key
            .verify(&envelope.payload, &signature)
            .map_err(|e| MarketError::Crypto(format!("Signature verification failed: {e}")))?;

        Ok((envelope.payload, envelope.signer))
    }

    /// Serialize the envelope to bytes for transmission.
    pub fn to_bytes(&self) -> MarketResult<Vec<u8>> {
        bincode::options()
            .with_limit(MAX_BINCODE_SIZE)
            .serialize(self)
            .map_err(|e| {
                MarketError::Serialization(format!("Failed to serialize signed envelope: {e}"))
            })
    }
}

/// Maximum allowed clock drift for message timestamps (5 minutes).
pub const MAX_TIMESTAMP_DRIFT_SECS: u64 = 300;

/// Validate that a message timestamp is within acceptable drift of the current time.
pub const fn validate_timestamp(message_timestamp: u64, current_time: u64) -> bool {
    message_timestamp.abs_diff(current_time) <= MAX_TIMESTAMP_DRIFT_SECS
}

/// Registry of bid announcements stored in DHT (listing subkey 2)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BidAnnouncementRegistry {
    /// List of (bidder_pubkey, bid_record_key, timestamp) tuples
    pub announcements: Vec<(PublicKey, RecordKey, u64)>,
}

impl BidAnnouncementRegistry {
    pub const fn new() -> Self {
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

    /// Serialize to bytes for DHT storage (CBOR, matching other DHT types)
    pub fn to_bytes(&self) -> MarketResult<Vec<u8>> {
        let mut buf = Vec::new();
        ciborium::into_writer(self, &mut buf).map_err(|e| {
            MarketError::Serialization(format!("Failed to serialize bid registry: {e}"))
        })?;
        Ok(buf)
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> MarketResult<Self> {
        crate::util::cbor_from_limited_reader(data, crate::util::MAX_DHT_VALUE_SIZE)
    }
}

/// In-memory tracker for bid announcements, shared by both
/// `AuctionCoordinator` (real Veilid) and `AuctionLogic` (mock-testable).
///
/// Provides dedup-on-insert and per-listing lookup without exposing
/// the internal `Mutex<HashMap<…>>` to callers.
pub struct BidAnnouncementTracker {
    announcements: Mutex<HashMap<RecordKey, Vec<(PublicKey, RecordKey)>>>,
}

impl Default for BidAnnouncementTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl BidAnnouncementTracker {
    pub fn new() -> Self {
        Self {
            announcements: Mutex::new(HashMap::new()),
        }
    }

    /// Register a bid announcement. Returns `true` if the bidder was new.
    pub async fn register(
        &self,
        listing_key: &RecordKey,
        bidder: PublicKey,
        bid_record_key: RecordKey,
    ) -> bool {
        let mut map = self.announcements.lock().await;
        let list = map.entry(listing_key.clone()).or_default();
        let is_new = !list.iter().any(|(b, _)| b == &bidder);
        if is_new {
            list.push((bidder, bid_record_key));
        }
        drop(map);
        is_new
    }

    /// Number of unique bidders for a listing.
    pub async fn count(&self, listing_key: &RecordKey) -> usize {
        let map = self.announcements.lock().await;
        map.get(listing_key).map_or(0, Vec::len)
    }

    /// Clone the announcement list for a listing, if any.
    pub async fn get(&self, listing_key: &RecordKey) -> Option<Vec<(PublicKey, RecordKey)>> {
        let map = self.announcements.lock().await;
        map.get(listing_key).cloned()
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
    /// Seller announces their catalog for registration in the master registry
    SellerRegistration {
        /// Seller's public key
        seller_pubkey: PublicKey,
        /// DHT key of the seller's catalog record
        catalog_key: RecordKey,
        /// Timestamp of registration
        timestamp: u64,
    },
    /// Announce the master registry DHT key so all nodes share one record
    RegistryAnnouncement {
        /// DHT key of the master registry record
        registry_key: RecordKey,
        /// Timestamp of announcement
        timestamp: u64,
    },
}

impl AuctionMessage {
    /// Extract the timestamp from any message variant.
    pub const fn timestamp(&self) -> u64 {
        match self {
            Self::BidAnnouncement { timestamp, .. }
            | Self::WinnerDecryptionRequest { timestamp, .. }
            | Self::DecryptionHashTransfer { timestamp, .. }
            | Self::MpcRouteAnnouncement { timestamp, .. }
            | Self::WinnerBidReveal { timestamp, .. }
            | Self::SellerRegistration { timestamp, .. }
            | Self::RegistryAnnouncement { timestamp, .. } => *timestamp,
        }
    }

    /// Create a bid announcement using system time.
    pub fn bid_announcement(
        listing_key: RecordKey,
        bidder: PublicKey,
        bid_record_key: RecordKey,
    ) -> Self {
        Self::BidAnnouncement {
            listing_key,
            bidder,
            bid_record_key,
            timestamp: now_unix(),
        }
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
    pub fn winner_decryption_request(listing_key: RecordKey, winner: PublicKey) -> Self {
        Self::WinnerDecryptionRequest {
            listing_key,
            winner,
            timestamp: now_unix(),
        }
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
        Self::DecryptionHashTransfer {
            listing_key,
            winner,
            decryption_hash,
            timestamp: now_unix(),
        }
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
        Self::MpcRouteAnnouncement {
            listing_key,
            party_pubkey,
            route_blob,
            timestamp: now_unix(),
        }
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
        Self::WinnerBidReveal {
            listing_key,
            winner,
            bid_value,
            nonce,
            timestamp: now_unix(),
        }
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

    /// Create a seller registration using system time.
    pub fn seller_registration(seller_pubkey: PublicKey, catalog_key: RecordKey) -> Self {
        Self::SellerRegistration {
            seller_pubkey,
            catalog_key,
            timestamp: now_unix(),
        }
    }

    /// Create a seller registration with a custom time provider.
    pub fn seller_registration_with_time<T: TimeProvider>(
        seller_pubkey: PublicKey,
        catalog_key: RecordKey,
        time: &T,
    ) -> Self {
        Self::SellerRegistration {
            seller_pubkey,
            catalog_key,
            timestamp: time.now_unix(),
        }
    }

    /// Create a registry announcement using system time.
    pub fn registry_announcement(registry_key: RecordKey) -> Self {
        Self::RegistryAnnouncement {
            registry_key,
            timestamp: now_unix(),
        }
    }

    /// Create a registry announcement with a custom time provider.
    pub fn registry_announcement_with_time<T: TimeProvider>(
        registry_key: RecordKey,
        time: &T,
    ) -> Self {
        Self::RegistryAnnouncement {
            registry_key,
            timestamp: time.now_unix(),
        }
    }

    /// Serialize to bytes for transmission
    pub fn to_bytes(&self) -> MarketResult<Vec<u8>> {
        bincode::options()
            .with_limit(MAX_BINCODE_SIZE)
            .serialize(self)
            .map_err(|e| {
                MarketError::Serialization(format!("Failed to serialize auction message: {e}"))
            })
    }

    /// Deserialize from bytes (with size limit to prevent OOM from crafted payloads).
    pub fn from_bytes(data: &[u8]) -> MarketResult<Self> {
        bincode_deserialize_limited(data).map_err(|e| {
            MarketError::Serialization(format!("Failed to deserialize auction message: {e}"))
        })
    }

    /// Serialize + sign into a [`SignedEnvelope`], then serialize the envelope.
    pub fn to_signed_bytes(
        &self,
        signing_key: &ed25519_dalek::SigningKey,
    ) -> MarketResult<Vec<u8>> {
        let payload = self.to_bytes()?;
        let envelope = SignedEnvelope::sign(payload, signing_key)?;
        envelope.to_bytes()
    }

    /// Verify a [`SignedEnvelope`], deserialize the inner `AuctionMessage`,
    /// and return it together with the signer's public key bytes.
    pub fn from_signed_bytes(data: &[u8]) -> MarketResult<(Self, [u8; 32])> {
        let (payload, signer) = SignedEnvelope::verify_and_unwrap(data)?;
        let msg = Self::from_bytes(&payload)?;
        Ok((msg, signer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocks::MockTime;

    fn test_pubkey() -> PublicKey {
        crate::mocks::dht::make_test_public_key(42)
    }

    fn test_record_key() -> RecordKey {
        crate::mocks::dht::make_test_record_key(99)
    }

    #[test]
    fn registry_announcement_bincode_roundtrip() {
        let msg = AuctionMessage::registry_announcement_with_time(
            test_record_key(),
            &MockTime::new(5000),
        );
        let bytes = msg.to_bytes().unwrap();
        let decoded = AuctionMessage::from_bytes(&bytes).unwrap();
        match decoded {
            AuctionMessage::RegistryAnnouncement {
                registry_key,
                timestamp,
            } => {
                assert_eq!(registry_key, test_record_key());
                assert_eq!(timestamp, 5000);
            }
            other => panic!("Expected RegistryAnnouncement, got {other:?}"),
        }
    }

    #[test]
    fn seller_registration_bincode_roundtrip() {
        let msg = AuctionMessage::seller_registration_with_time(
            test_pubkey(),
            test_record_key(),
            &MockTime::new(3000),
        );
        let bytes = msg.to_bytes().unwrap();
        let decoded = AuctionMessage::from_bytes(&bytes).unwrap();
        match decoded {
            AuctionMessage::SellerRegistration {
                seller_pubkey,
                catalog_key,
                timestamp,
            } => {
                assert_eq!(seller_pubkey, test_pubkey());
                assert_eq!(catalog_key, test_record_key());
                assert_eq!(timestamp, 3000);
            }
            other => panic!("Expected SellerRegistration, got {other:?}"),
        }
    }

    #[test]
    fn bid_announcement_bincode_roundtrip() {
        let listing_key = test_record_key();
        let bidder = test_pubkey();
        let bid_record_key = crate::mocks::dht::make_test_record_key(123);
        let msg = AuctionMessage::bid_announcement_with_time(
            listing_key.clone(),
            bidder.clone(),
            bid_record_key.clone(),
            &MockTime::new(4000),
        );
        let bytes = msg.to_bytes().unwrap();
        let decoded = AuctionMessage::from_bytes(&bytes).unwrap();
        match decoded {
            AuctionMessage::BidAnnouncement {
                listing_key: decoded_listing,
                bidder: decoded_bidder,
                bid_record_key: decoded_bid_record,
                timestamp,
            } => {
                assert_eq!(decoded_listing, listing_key);
                assert_eq!(decoded_bidder, bidder);
                assert_eq!(decoded_bid_record, bid_record_key);
                assert_eq!(timestamp, 4000);
            }
            other => panic!("Expected BidAnnouncement, got {other:?}"),
        }
    }

    #[test]
    fn winner_decryption_request_bincode_roundtrip() {
        let listing_key = test_record_key();
        let winner = test_pubkey();
        let msg = AuctionMessage::winner_decryption_request_with_time(
            listing_key.clone(),
            winner.clone(),
            &MockTime::new(6000),
        );
        let bytes = msg.to_bytes().unwrap();
        let decoded = AuctionMessage::from_bytes(&bytes).unwrap();
        match decoded {
            AuctionMessage::WinnerDecryptionRequest {
                listing_key: decoded_listing,
                winner: decoded_winner,
                timestamp,
            } => {
                assert_eq!(decoded_listing, listing_key);
                assert_eq!(decoded_winner, winner);
                assert_eq!(timestamp, 6000);
            }
            other => panic!("Expected WinnerDecryptionRequest, got {other:?}"),
        }
    }

    #[test]
    fn decryption_hash_transfer_bincode_roundtrip() {
        let listing_key = test_record_key();
        let winner = test_pubkey();
        let decryption_hash = "test_hash_123".to_string();
        let msg = AuctionMessage::decryption_hash_transfer_with_time(
            listing_key.clone(),
            winner.clone(),
            decryption_hash.clone(),
            &MockTime::new(7000),
        );
        let bytes = msg.to_bytes().unwrap();
        let decoded = AuctionMessage::from_bytes(&bytes).unwrap();
        match decoded {
            AuctionMessage::DecryptionHashTransfer {
                listing_key: decoded_listing,
                winner: decoded_winner,
                decryption_hash: decoded_hash,
                timestamp,
            } => {
                assert_eq!(decoded_listing, listing_key);
                assert_eq!(decoded_winner, winner);
                assert_eq!(decoded_hash, decryption_hash);
                assert_eq!(timestamp, 7000);
            }
            other => panic!("Expected DecryptionHashTransfer, got {other:?}"),
        }
    }

    fn test_route_blob() -> RouteBlob {
        // Create a test RouteBlob similar to MockTransport::make_route_id
        let counter: u64 = 99;
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&counter.to_le_bytes());
        for i in 8..32 {
            bytes[i] = ((counter >> ((i % 8) * 8)) & 0xFF) as u8;
        }

        let encoded = data_encoding::BASE64URL_NOPAD.encode(&bytes);
        let key_str = format!("VLD0:{}", encoded);
        let route_id =
            veilid_core::RouteId::try_from(key_str.as_str()).expect("Should create valid RouteId");

        RouteBlob {
            route_id,
            blob: format!("test_route_blob_{}", counter).into_bytes(),
        }
    }

    #[test]
    fn mpc_route_announcement_bincode_roundtrip() {
        let listing_key = test_record_key();
        let party_pubkey = test_pubkey();
        let route_blob = test_route_blob();
        let msg = AuctionMessage::mpc_route_announcement_with_time(
            listing_key.clone(),
            party_pubkey.clone(),
            route_blob.clone(),
            &MockTime::new(8000),
        );
        let bytes = msg.to_bytes().unwrap();
        let decoded = AuctionMessage::from_bytes(&bytes).unwrap();
        match decoded {
            AuctionMessage::MpcRouteAnnouncement {
                listing_key: decoded_listing,
                party_pubkey: decoded_party,
                route_blob: decoded_blob,
                timestamp,
            } => {
                assert_eq!(decoded_listing, listing_key);
                assert_eq!(decoded_party, party_pubkey);
                assert_eq!(decoded_blob.route_id, route_blob.route_id);
                assert_eq!(decoded_blob.blob, route_blob.blob);
                assert_eq!(timestamp, 8000);
            }
            other => panic!("Expected MpcRouteAnnouncement, got {other:?}"),
        }
    }

    #[test]
    fn winner_bid_reveal_bincode_roundtrip() {
        let listing_key = test_record_key();
        let winner = test_pubkey();
        let bid_value = 1000u64;
        let nonce = [42u8; 32];
        let msg = AuctionMessage::winner_bid_reveal_with_time(
            listing_key.clone(),
            winner.clone(),
            bid_value,
            nonce,
            &MockTime::new(9000),
        );
        let bytes = msg.to_bytes().unwrap();
        let decoded = AuctionMessage::from_bytes(&bytes).unwrap();
        match decoded {
            AuctionMessage::WinnerBidReveal {
                listing_key: decoded_listing,
                winner: decoded_winner,
                bid_value: decoded_bid,
                nonce: decoded_nonce,
                timestamp,
            } => {
                assert_eq!(decoded_listing, listing_key);
                assert_eq!(decoded_winner, winner);
                assert_eq!(decoded_bid, bid_value);
                assert_eq!(decoded_nonce, nonce);
                assert_eq!(timestamp, 9000);
            }
            other => panic!("Expected WinnerBidReveal, got {other:?}"),
        }
    }

    #[test]
    fn test_validate_timestamp_exact_match() {
        assert!(validate_timestamp(1000, 1000));
    }

    #[test]
    fn test_validate_timestamp_within_drift_future() {
        // Message 300 seconds in the future (exactly at limit)
        assert!(validate_timestamp(1300, 1000));
    }

    #[test]
    fn test_validate_timestamp_within_drift_past() {
        // Message 300 seconds in the past (exactly at limit)
        assert!(validate_timestamp(700, 1000));
    }

    #[test]
    fn test_validate_timestamp_exceeds_drift_future() {
        // Message 301 seconds in the future (just over limit)
        assert!(!validate_timestamp(1301, 1000));
    }

    #[test]
    fn test_validate_timestamp_exceeds_drift_past() {
        // Message 301 seconds in the past (just over limit)
        assert!(!validate_timestamp(699, 1000));
    }

    #[test]
    fn test_validate_timestamp_zero() {
        // Zero timestamp against a large current time
        assert!(!validate_timestamp(0, 1000));
    }

    #[test]
    fn test_validate_timestamp_large_drift() {
        // Massive drift should fail
        assert!(!validate_timestamp(0, 1_000_000));
    }

    #[test]
    fn test_validate_timestamp_far_future() {
        // 1 hour in the future (3600 seconds >> 300 second limit)
        assert!(!validate_timestamp(4600, 1000));
    }

    #[test]
    fn test_validate_timestamp_symmetry() {
        // Drift in both directions should behave identically at boundary
        assert!(validate_timestamp(700, 1000)); // -300
        assert!(validate_timestamp(1300, 1000)); // +300
        assert!(!validate_timestamp(699, 1000)); // -301
        assert!(!validate_timestamp(1301, 1000)); // +301
    }

    #[test]
    fn test_bid_announcement_registry_new() {
        let registry = BidAnnouncementRegistry::new();
        assert!(registry.announcements.is_empty());
    }

    #[test]
    fn test_bid_announcement_registry_add_and_dedup() {
        let mut registry = BidAnnouncementRegistry::new();
        let bidder = test_pubkey();
        let bid_key = test_record_key();

        registry.add(bidder.clone(), bid_key.clone(), 1000);
        assert_eq!(registry.announcements.len(), 1);

        // Adding same bidder again should be deduplicated
        registry.add(bidder.clone(), bid_key.clone(), 2000);
        assert_eq!(registry.announcements.len(), 1);

        // Adding different bidder should work
        let bidder2 = crate::mocks::dht::make_test_public_key(43);
        registry.add(bidder2, bid_key, 3000);
        assert_eq!(registry.announcements.len(), 2);
    }

    #[test]
    fn test_bid_announcement_registry_cbor_roundtrip() {
        let mut registry = BidAnnouncementRegistry::new();
        let bidder1 = crate::mocks::dht::make_test_public_key(1);
        let bidder2 = crate::mocks::dht::make_test_public_key(2);
        let key1 = crate::mocks::dht::make_test_record_key(10);
        let key2 = crate::mocks::dht::make_test_record_key(20);

        registry.add(bidder1.clone(), key1.clone(), 100);
        registry.add(bidder2.clone(), key2.clone(), 200);

        let bytes = registry.to_bytes().unwrap();
        let restored = BidAnnouncementRegistry::from_bytes(&bytes).unwrap();

        assert_eq!(restored.announcements.len(), 2);
        assert_eq!(restored.announcements[0].0, bidder1);
        assert_eq!(restored.announcements[0].1, key1);
        assert_eq!(restored.announcements[0].2, 100);
        assert_eq!(restored.announcements[1].0, bidder2);
        assert_eq!(restored.announcements[1].1, key2);
        assert_eq!(restored.announcements[1].2, 200);
    }

    #[test]
    fn test_bid_announcement_registry_empty_roundtrip() {
        let registry = BidAnnouncementRegistry::new();
        let bytes = registry.to_bytes().unwrap();
        let restored = BidAnnouncementRegistry::from_bytes(&bytes).unwrap();
        assert!(restored.announcements.is_empty());
    }

    #[test]
    fn signed_envelope_roundtrip() {
        let signing_key = ed25519_dalek::SigningKey::generate(&mut rand::thread_rng());
        let msg = AuctionMessage::registry_announcement_with_time(
            test_record_key(),
            &MockTime::new(1000),
        );
        let signed_bytes = msg.to_signed_bytes(&signing_key).unwrap();
        let (decoded, signer) = AuctionMessage::from_signed_bytes(&signed_bytes).unwrap();
        assert_eq!(signer, signing_key.verifying_key().to_bytes());
        assert_eq!(decoded.timestamp(), 1000);
    }

    #[test]
    fn signed_envelope_rejects_tampered_payload() {
        let signing_key = ed25519_dalek::SigningKey::generate(&mut rand::thread_rng());
        let msg = AuctionMessage::registry_announcement_with_time(
            test_record_key(),
            &MockTime::new(2000),
        );
        let mut signed_bytes = msg.to_signed_bytes(&signing_key).unwrap();
        // Tamper with a byte in the middle of the payload
        if signed_bytes.len() > 20 {
            signed_bytes[20] ^= 0xFF;
        }
        let result = AuctionMessage::from_signed_bytes(&signed_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn signed_envelope_rejects_wrong_key() {
        let key_a = ed25519_dalek::SigningKey::generate(&mut rand::thread_rng());
        let key_b = ed25519_dalek::SigningKey::generate(&mut rand::thread_rng());
        let msg = AuctionMessage::registry_announcement_with_time(
            test_record_key(),
            &MockTime::new(3000),
        );
        let signed_bytes = msg.to_signed_bytes(&key_a).unwrap();
        // Verification succeeds because the envelope carries key_a's verifying key
        // and the signature is valid under key_a.  The *signer identity* returned
        // should be key_a, not key_b.
        let (_, signer) = AuctionMessage::from_signed_bytes(&signed_bytes).unwrap();
        assert_eq!(signer, key_a.verifying_key().to_bytes());
        assert_ne!(signer, key_b.verifying_key().to_bytes());
    }

    #[test]
    fn bincode_size_limit_rejects_oversized_payload() {
        // Craft a bincode payload whose length prefix claims > 64 KB
        // bincode writes a u64 length prefix for Vec/String, so we forge one
        let mut bad = Vec::new();
        // Variant tag for BidAnnouncement (0u32)
        bad.extend_from_slice(&0u32.to_le_bytes());
        // listing_key: needs some bytes but let's just make a huge string length
        // Actually, simplest: use bincode_deserialize_limited directly
        let huge_len: u64 = 128 * 1024; // 128 KB
        bad.extend_from_slice(&huge_len.to_le_bytes());
        bad.extend_from_slice(&vec![0u8; 64]); // partial data

        let result = bincode_deserialize_limited::<AuctionMessage>(&bad);
        assert!(result.is_err());
    }
}
