//! Domain types for the auction marketplace: listings, bids, bid records, and sealed-bid commitments.
//!
//! All types derive `Serialize`/`Deserialize` for CBOR persistence to the Veilid DHT.

pub mod bid;
pub mod bid_record;
pub mod listing;

pub use bid::Bid;
pub use bid_record::{BidIndex, BidRecord};
pub use listing::{Listing, ListingStatus, PublicListing};
