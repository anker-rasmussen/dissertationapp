//! Signing key resolution helpers for auction message authentication.

use tracing::warn;
use veilid_core::{PublicKey, RecordKey};

use super::AuctionCoordinator;
use crate::config::subkeys;
use crate::error::MarketResult;

use super::super::bid_announcement::BidAnnouncementRegistry;
use super::super::bid_ops::BidOperations;
use super::super::listing_ops::ListingOperations;

impl AuctionCoordinator {
    /// Resolve the expected signing key for a bidder in this listing, if known.
    pub(super) async fn expected_bidder_signing_key(
        &self,
        listing_key: &RecordKey,
        bidder: &PublicKey,
    ) -> MarketResult<Option<[u8; 32]>> {
        let Some(registry_data) = self
            .dht
            .get_value_at_subkey(listing_key, subkeys::BID_ANNOUNCEMENTS, true)
            .await?
        else {
            return Ok(None);
        };

        let registry = BidAnnouncementRegistry::from_bytes(&registry_data)?;
        let Some(bid_record_key) = registry
            .announcements
            .iter()
            .find(|(b, _, _)| b == bidder)
            .map(|(_, key, _)| key.clone())
        else {
            return Ok(None);
        };

        let bid_ops = BidOperations::new(self.dht.clone());
        let Some(bid_record) = bid_ops.fetch_bid(&bid_record_key).await? else {
            return Ok(None);
        };

        if bid_record.listing_key != *listing_key || bid_record.bidder != *bidder {
            warn!(
                "Bid record identity mismatch for bidder {} on listing {}",
                bidder, listing_key
            );
            return Ok(None);
        }

        if bid_record.signing_pubkey == [0u8; 32] {
            warn!(
                "Bid record for bidder {} has empty signing key; rejecting",
                bidder
            );
            return Ok(None);
        }

        Ok(Some(bid_record.signing_pubkey))
    }

    /// Resolve the expected signing key for a listing's seller, if known.
    pub(super) async fn seller_signing_key_for_listing(
        &self,
        listing_key: &RecordKey,
    ) -> MarketResult<Option<[u8; 32]>> {
        let listing_ops = ListingOperations::new(self.dht.clone());
        let Some(listing) = listing_ops.get_listing(listing_key).await? else {
            return Ok(None);
        };

        // Try the master registry first (populated by SellerRegistration broadcasts).
        let seller_pubkey = listing.seller.to_string();
        let registry_result = {
            let mut ops = self.registry_ops.lock().await;
            ops.get_seller_signing_pubkey(&seller_pubkey).await?
        };
        if registry_result.is_some() {
            return Ok(registry_result);
        }

        // Fallback: look up the seller's signing key from their bid record
        // in the bid announcement registry.  The seller always places a
        // reserve-price bid, so their signing_pubkey is available there.
        self.expected_bidder_signing_key(listing_key, &listing.seller)
            .await
    }

    /// Validate that a bid announcement message is signed by the bid owner.
    pub(super) async fn validate_bid_announcement_signer(
        &self,
        listing_key: &RecordKey,
        bidder: &PublicKey,
        bid_record_key: &RecordKey,
        signer: [u8; 32],
    ) -> MarketResult<bool> {
        let bid_ops = BidOperations::new(self.dht.clone());
        let Some(bid_record) = bid_ops.fetch_bid(bid_record_key).await? else {
            warn!(
                "Rejecting bid announcement: bid record {} not found",
                bid_record_key
            );
            return Ok(false);
        };

        if bid_record.listing_key != *listing_key {
            warn!(
                "Rejecting bid announcement: listing mismatch (msg {}, record {})",
                listing_key, bid_record.listing_key
            );
            return Ok(false);
        }

        if bid_record.bidder != *bidder {
            warn!(
                "Rejecting bid announcement: bidder mismatch (msg {}, record {})",
                bidder, bid_record.bidder
            );
            return Ok(false);
        }

        if bid_record.bid_key != *bid_record_key {
            warn!(
                "Rejecting bid announcement: bid key mismatch (msg {}, record {})",
                bid_record_key, bid_record.bid_key
            );
            return Ok(false);
        }

        if bid_record.signing_pubkey == [0u8; 32] {
            warn!("Rejecting bid announcement: empty signing key in bid record");
            return Ok(false);
        }

        if bid_record.signing_pubkey != signer {
            warn!("Rejecting bid announcement: signer mismatch");
            return Ok(false);
        }

        Ok(true)
    }
}
