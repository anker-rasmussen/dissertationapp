//! Bid types: plaintext [`Bid`] with SHA256 commitment.
//!
//! Bids use a commit-reveal scheme: `commitment = SHA256(amount || 32-byte nonce)`.
//! The nonce is stored locally and revealed after the auction ends for MPC input verification.

use serde::{Deserialize, Serialize};
use veilid_core::{PublicKey, RecordKey};

use crate::config::now_unix;
use crate::traits::{RandomSource, TimeProvider};

/// A sealed bid for an auction listing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bid {
    /// The listing this bid is for
    pub listing_key: RecordKey,

    /// Public key of the bidder
    pub bidder: PublicKey,

    /// Bid amount in atomic units
    pub amount: u64,

    /// Unix timestamp when bid was submitted
    pub timestamp: u64,

    /// Hash commitment for sealed-bid (SHA256 of amount + secret nonce)
    /// Used to verify bid wasn't changed after submission
    pub commitment: [u8; 32],

    /// Secret nonce used in commitment (revealed after auction ends)
    /// Only set when bid is revealed.  Skipped during serialization to
    /// prevent accidental leakage over the wire.
    #[serde(skip_serializing)]
    pub(crate) reveal_nonce: Option<[u8; 32]>,
}

impl Bid {
    /// Create a new sealed bid with commitment using default providers
    pub fn new(listing_key: RecordKey, bidder: PublicKey, amount: u64) -> Self {
        use rand::RngCore;
        use sha2::{Digest, Sha256};
        assert!(amount > 0, "Bid amount must be greater than zero");

        let mut nonce = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut nonce);

        let mut hasher = Sha256::new();
        hasher.update(amount.to_le_bytes());
        hasher.update(nonce);
        let commitment: [u8; 32] = hasher.finalize().into();

        Self {
            listing_key,
            bidder,
            amount,
            timestamp: now_unix(),
            commitment,
            reveal_nonce: Some(nonce),
        }
    }

    /// Create a new sealed bid with custom providers for testing
    pub fn new_with_providers<R: RandomSource, T: TimeProvider>(
        listing_key: RecordKey,
        bidder: PublicKey,
        amount: u64,
        rng: &R,
        time: &T,
    ) -> Self {
        use sha2::{Digest, Sha256};
        assert!(amount > 0, "Bid amount must be greater than zero");

        // Generate random nonce for commitment
        let nonce = rng.random_bytes_32();

        // Create commitment: H(amount || nonce)
        let mut hasher = Sha256::new();
        hasher.update(amount.to_le_bytes());
        hasher.update(nonce);
        let commitment: [u8; 32] = hasher.finalize().into();

        Self {
            listing_key,
            bidder,
            amount,
            timestamp: time.now_unix(),
            commitment,
            reveal_nonce: Some(nonce), // Store nonce for later reveal
        }
    }

    /// Verify that a revealed bid matches its commitment
    pub fn verify_commitment(&self) -> bool {
        use sha2::{Digest, Sha256};
        use subtle::ConstantTimeEq;

        if let Some(nonce) = &self.reveal_nonce {
            let mut hasher = Sha256::new();
            hasher.update(self.amount.to_le_bytes());
            hasher.update(nonce);
            let computed: [u8; 32] = hasher.finalize().into();
            computed.ct_eq(&self.commitment).into()
        } else {
            false
        }
    }

    /// Serialize the bid to CBOR bytes
    pub fn to_cbor(&self) -> Result<Vec<u8>, ciborium::ser::Error<std::io::Error>> {
        let mut buffer = Vec::new();
        ciborium::into_writer(self, &mut buffer)?;
        Ok(buffer)
    }

    /// Deserialize a bid from CBOR bytes
    pub fn from_cbor(data: &[u8]) -> crate::error::MarketResult<Self> {
        crate::util::cbor_from_limited_reader(data, crate::util::MAX_DHT_VALUE_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocks::{make_test_public_key, make_test_record_key, MockRandom, MockTime};

    fn make_test_key() -> RecordKey {
        make_test_record_key(1)
    }

    fn make_test_pubkey() -> PublicKey {
        make_test_public_key(2)
    }

    #[test]
    fn test_bid_new_creates_commitment() {
        let rng = MockRandom::new(42);
        let time = MockTime::new(1000);

        let bid = Bid::new_with_providers(make_test_key(), make_test_pubkey(), 100, &rng, &time);

        assert_eq!(bid.amount, 100);
        assert_eq!(bid.timestamp, 1000);
        assert!(bid.reveal_nonce.is_some());
        // Commitment should not be all zeros
        assert!(bid.commitment.iter().any(|&b| b != 0));
    }

    #[test]
    fn test_bid_verify_commitment_valid() {
        let rng = MockRandom::new(42);
        let time = MockTime::new(1000);

        let bid = Bid::new_with_providers(make_test_key(), make_test_pubkey(), 100, &rng, &time);

        assert!(bid.verify_commitment());
    }

    #[test]
    fn test_bid_verify_commitment_no_nonce() {
        let rng = MockRandom::new(42);
        let time = MockTime::new(1000);

        let mut bid =
            Bid::new_with_providers(make_test_key(), make_test_pubkey(), 100, &rng, &time);
        bid.reveal_nonce = None;

        assert!(!bid.verify_commitment());
    }

    #[test]
    fn test_bid_verify_commitment_wrong_amount() {
        let rng = MockRandom::new(42);
        let time = MockTime::new(1000);

        let mut bid =
            Bid::new_with_providers(make_test_key(), make_test_pubkey(), 100, &rng, &time);
        // Tamper with the amount
        bid.amount = 200;

        assert!(!bid.verify_commitment());
    }

    #[test]
    fn test_bid_verify_commitment_wrong_nonce() {
        let rng = MockRandom::new(42);
        let time = MockTime::new(1000);

        let mut bid =
            Bid::new_with_providers(make_test_key(), make_test_pubkey(), 100, &rng, &time);
        // Tamper with the nonce
        bid.reveal_nonce = Some([99u8; 32]);

        assert!(!bid.verify_commitment());
    }

    #[test]
    fn test_bid_deterministic_with_same_random() {
        let rng1 = MockRandom::new(42);
        let rng2 = MockRandom::new(42);
        let time = MockTime::new(1000);

        let bid1 = Bid::new_with_providers(make_test_key(), make_test_pubkey(), 100, &rng1, &time);

        let bid2 = Bid::new_with_providers(make_test_key(), make_test_pubkey(), 100, &rng2, &time);

        assert_eq!(bid1.commitment, bid2.commitment);
        assert_eq!(bid1.reveal_nonce, bid2.reveal_nonce);
    }

    #[test]
    fn test_bid_different_with_different_random() {
        let rng1 = MockRandom::new(1);
        let rng2 = MockRandom::new(2);
        let time = MockTime::new(1000);

        let bid1 = Bid::new_with_providers(make_test_key(), make_test_pubkey(), 100, &rng1, &time);

        let bid2 = Bid::new_with_providers(make_test_key(), make_test_pubkey(), 100, &rng2, &time);

        assert_ne!(bid1.commitment, bid2.commitment);
        assert_ne!(bid1.reveal_nonce, bid2.reveal_nonce);
    }

    #[test]
    fn test_bid_serialization_roundtrip() {
        let rng = MockRandom::new(42);
        let time = MockTime::new(1000);

        let original =
            Bid::new_with_providers(make_test_key(), make_test_pubkey(), 100, &rng, &time);

        let cbor = original.to_cbor().unwrap();
        let restored = Bid::from_cbor(&cbor).unwrap();

        assert_eq!(original.listing_key, restored.listing_key);
        assert_eq!(original.bidder, restored.bidder);
        assert_eq!(original.amount, restored.amount);
        assert_eq!(original.timestamp, restored.timestamp);
        assert_eq!(original.commitment, restored.commitment);
        // reveal_nonce is skip_serializing — must be None after round-trip
        assert_eq!(restored.reveal_nonce, None);
    }

    #[test]
    #[should_panic(expected = "Bid amount must be greater than zero")]
    fn test_bid_new_zero_amount_panics() {
        let rng = MockRandom::new(42);
        let time = MockTime::new(1000);

        // This should panic due to the assert!(amount > 0) in Bid::new_with_providers
        Bid::new_with_providers(make_test_key(), make_test_pubkey(), 0, &rng, &time);
    }

    #[test]
    #[should_panic(expected = "Bid amount must be greater than zero")]
    fn test_bid_new_default_zero_amount_panics() {
        // Also test the non-providers version
        Bid::new(make_test_key(), make_test_pubkey(), 0);
    }
}
