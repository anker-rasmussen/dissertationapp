use serde::{Deserialize, Serialize};
use veilid_core::{PublicKey, RecordKey};

/// A bid record published to the DHT for auction participation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BidRecord {
    /// The listing this bid is for
    pub listing_key: RecordKey,

    /// Bidder's node ID (public key)
    pub bidder: PublicKey,

    /// Commitment to the bid value (hash of bid + nonce)
    pub commitment: [u8; 32],

    /// Timestamp when bid was submitted
    pub timestamp: u64,

    /// DHT key where this bid record is stored
    pub bid_key: RecordKey,
}

impl BidRecord {
    /// Serialize to CBOR for DHT storage
    pub fn to_cbor(&self) -> anyhow::Result<Vec<u8>> {
        let mut data = Vec::new();
        ciborium::ser::into_writer(self, &mut data)
            .map_err(|e| anyhow::anyhow!("Failed to serialize bid record: {}", e))?;
        Ok(data)
    }

    /// Deserialize from CBOR
    pub fn from_cbor(data: &[u8]) -> anyhow::Result<Self> {
        ciborium::de::from_reader(data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize bid record: {}", e))
    }
}

/// Index of all bids for a specific listing
/// Published to a well-known DHT location derived from listing key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BidIndex {
    /// Listing this index is for
    pub listing_key: RecordKey,

    /// All bid records for this listing
    pub bids: Vec<BidRecord>,

    /// Last update timestamp
    pub last_updated: u64,
}

impl BidIndex {
    pub fn new(listing_key: RecordKey) -> Self {
        Self {
            listing_key,
            bids: Vec::new(),
            last_updated: 0,
        }
    }

    pub fn add_bid(&mut self, bid: BidRecord) {
        // Check for duplicates (same bidder)
        if !self.bids.iter().any(|b| b.bidder == bid.bidder) {
            self.bids.push(bid);
            self.last_updated = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
        }
    }

    /// Get bidders sorted by public key (deterministic party assignment)
    pub fn sorted_bidders(&self) -> Vec<PublicKey> {
        let mut bidders: Vec<_> = self.bids.iter().map(|b| b.bidder.clone()).collect();
        bidders.sort_by(|a, b| {
            // Compare using string representation
            let a_str = a.to_string();
            let b_str = b.to_string();
            a_str.cmp(&b_str)
        });
        bidders
    }

    /// Get party ID for a specific bidder
    pub fn get_party_id(&self, bidder: &PublicKey) -> Option<usize> {
        let sorted = self.sorted_bidders();
        sorted.iter().position(|b| b == bidder)
    }

    pub fn to_cbor(&self) -> anyhow::Result<Vec<u8>> {
        let mut data = Vec::new();
        ciborium::ser::into_writer(self, &mut data)
            .map_err(|e| anyhow::anyhow!("Failed to serialize bid index: {}", e))?;
        Ok(data)
    }

    pub fn from_cbor(data: &[u8]) -> anyhow::Result<Self> {
        ciborium::de::from_reader(data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize bid index: {}", e))
    }
}
