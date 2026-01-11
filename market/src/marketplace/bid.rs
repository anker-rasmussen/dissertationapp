use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use veilid_core::{PublicKey, RecordKey};

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
    /// Only set when bid is revealed
    pub reveal_nonce: Option<[u8; 32]>,
}

impl Bid {
    /// Create a new sealed bid with commitment
    pub fn new(listing_key: RecordKey, bidder: PublicKey, amount: u64) -> Self {
        use rand::RngCore;
        use sha2::{Digest, Sha256};

        // Generate random nonce for commitment
        let mut nonce = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut nonce);

        // Create commitment: H(amount || nonce)
        let mut hasher = Sha256::new();
        hasher.update(amount.to_le_bytes());
        hasher.update(&nonce);
        let commitment: [u8; 32] = hasher.finalize().into();

        Self {
            listing_key,
            bidder,
            amount,
            timestamp: current_timestamp(),
            commitment,
            reveal_nonce: Some(nonce), // Store nonce for later reveal
        }
    }

    /// Create a bid with only the commitment visible (for sending to others)
    /// The amount is hidden until reveal
    pub fn sealed(&self) -> SealedBid {
        SealedBid {
            listing_key: self.listing_key.clone(),
            bidder: self.bidder.clone(),
            timestamp: self.timestamp,
            commitment: self.commitment,
        }
    }

    /// Verify that a revealed bid matches its commitment
    pub fn verify_commitment(&self) -> bool {
        use sha2::{Digest, Sha256};

        if let Some(nonce) = &self.reveal_nonce {
            let mut hasher = Sha256::new();
            hasher.update(self.amount.to_le_bytes());
            hasher.update(nonce);
            let computed: [u8; 32] = hasher.finalize().into();
            computed == self.commitment
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
    pub fn from_cbor(data: &[u8]) -> Result<Self, ciborium::de::Error<std::io::Error>> {
        ciborium::from_reader(data)
    }
}

/// A sealed bid with hidden amount (only commitment visible)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SealedBid {
    /// The listing this bid is for
    pub listing_key: RecordKey,

    /// Public key of the bidder
    pub bidder: PublicKey,

    /// Unix timestamp when bid was submitted
    pub timestamp: u64,

    /// Hash commitment (amount hidden until reveal)
    pub commitment: [u8; 32],
}

impl SealedBid {
    /// Serialize to CBOR bytes
    pub fn to_cbor(&self) -> Result<Vec<u8>, ciborium::ser::Error<std::io::Error>> {
        let mut buffer = Vec::new();
        ciborium::into_writer(self, &mut buffer)?;
        Ok(buffer)
    }

    /// Deserialize from CBOR bytes
    pub fn from_cbor(data: &[u8]) -> Result<Self, ciborium::de::Error<std::io::Error>> {
        ciborium::from_reader(data)
    }
}

/// Collection of bids for a listing (stored in DHT)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BidCollection {
    /// The listing these bids are for
    pub listing_key: RecordKey,

    /// All sealed bids received
    pub bids: Vec<SealedBid>,
}

impl BidCollection {
    pub fn new(listing_key: RecordKey) -> Self {
        Self {
            listing_key,
            bids: Vec::new(),
        }
    }

    pub fn add_bid(&mut self, bid: SealedBid) {
        self.bids.push(bid);
    }

    pub fn bid_count(&self) -> usize {
        self.bids.len()
    }

    /// Serialize to CBOR bytes
    pub fn to_cbor(&self) -> Result<Vec<u8>, ciborium::ser::Error<std::io::Error>> {
        let mut buffer = Vec::new();
        ciborium::into_writer(self, &mut buffer)?;
        Ok(buffer)
    }

    /// Deserialize from CBOR bytes
    pub fn from_cbor(data: &[u8]) -> Result<Self, ciborium::de::Error<std::io::Error>> {
        ciborium::from_reader(data)
    }
}

/// Get current Unix timestamp in seconds
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time is before Unix epoch")
        .as_secs()
}
