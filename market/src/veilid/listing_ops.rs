use tracing::info;
use veilid_core::RecordKey;

use crate::error::{MarketError, MarketResult};
use crate::marketplace::{Listing, PublicListing};
use crate::traits::DhtStore;

/// DHT operations specialized for marketplace listings.
/// Generic over the DHT store implementation for testability.
pub struct ListingOperations<D: DhtStore> {
    dht: D,
}

impl<D: DhtStore> ListingOperations<D> {
    pub const fn new(dht: D) -> Self {
        Self { dht }
    }

    /// Publish a new listing to the DHT
    /// Returns the DHT record with owner keypair for future updates
    pub async fn publish_listing(&self, listing: &Listing) -> MarketResult<D::OwnedRecord> {
        // Create a new DHT record
        let record = self.dht.create_record().await?;

        // Serialize the public listing (without decryption key) to CBOR
        let listing_data = listing
            .to_public()
            .to_cbor()
            .map_err(|e| MarketError::Serialization(format!("Failed to serialize listing: {e}")))?;

        // Store the listing in the DHT
        self.dht.set_value(&record, listing_data).await?;

        let key = D::record_key(&record);
        info!(
            "Published listing '{}' to DHT at key: {}",
            listing.title, key
        );

        Ok(record)
    }

    /// Update an existing listing in the DHT
    pub async fn update_listing(
        &self,
        record: &D::OwnedRecord,
        listing: &Listing,
    ) -> MarketResult<()> {
        // Serialize the public listing (without decryption key) to CBOR
        let listing_data = listing
            .to_public()
            .to_cbor()
            .map_err(|e| MarketError::Serialization(format!("Failed to serialize listing: {e}")))?;

        // Update the value in the DHT
        self.dht.set_value(record, listing_data).await?;

        info!("Updated listing '{}' in DHT", listing.title);

        Ok(())
    }

    /// Retrieve a listing from the DHT by its record key.
    /// Returns a `PublicListing` (without decryption key) because the DHT
    /// never stores the seller's private decryption key.
    pub async fn get_listing(&self, key: &RecordKey) -> MarketResult<Option<PublicListing>> {
        // Get the value from the DHT
        let data = self.dht.get_value(key).await?;

        match data {
            Some(cbor_data) => {
                // Deserialize from CBOR
                let listing = PublicListing::from_cbor(&cbor_data).map_err(|e| {
                    MarketError::Serialization(format!(
                        "Failed to deserialize listing from DHT: {e}"
                    ))
                })?;

                info!("Retrieved listing '{}' from DHT", listing.title);
                Ok(Some(listing))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marketplace::ListingStatus;
    use crate::mocks::{make_test_public_key, make_test_record_key, MockDht, MockTime};

    fn make_test_listing(time: &MockTime) -> Listing {
        Listing::builder_with_time(time.clone())
            .key(make_test_record_key(1))
            .seller(make_test_public_key(2))
            .title("Test Auction")
            .encrypted_content(vec![1, 2, 3], [0u8; 12], "abc123".to_string())
            .reserve_price(100)
            .auction_duration(3600)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_publish_listing() {
        let dht = MockDht::new();
        let ops = ListingOperations::new(dht.clone());
        let time = MockTime::new(1000);
        let listing = make_test_listing(&time);

        let record = ops.publish_listing(&listing).await.unwrap();
        let key = MockDht::record_key(&record);

        // Verify the listing was stored
        assert!(dht.has_record(&key).await);
    }

    #[tokio::test]
    async fn test_get_listing() {
        let dht = MockDht::new();
        let ops = ListingOperations::new(dht.clone());
        let time = MockTime::new(1000);
        let listing = make_test_listing(&time);

        let record = ops.publish_listing(&listing).await.unwrap();
        let key = MockDht::record_key(&record);

        // Retrieve the listing
        let retrieved = ops.get_listing(&key).await.unwrap().unwrap();

        assert_eq!(retrieved.title, "Test Auction");
        assert_eq!(retrieved.reserve_price, 100);
    }

    #[tokio::test]
    async fn test_get_nonexistent_listing() {
        let dht = MockDht::new();
        let ops = ListingOperations::new(dht);

        let key = make_test_record_key(99);
        let result = ops.get_listing(&key).await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_update_listing() {
        let dht = MockDht::new();
        let ops = ListingOperations::new(dht.clone());
        let time = MockTime::new(1000);
        let mut listing = make_test_listing(&time);

        let record = ops.publish_listing(&listing).await.unwrap();
        let key = MockDht::record_key(&record);

        // Update the listing status
        listing.status = ListingStatus::Closed;
        ops.update_listing(&record, &listing).await.unwrap();

        // Verify the update
        let retrieved = ops.get_listing(&key).await.unwrap().unwrap();
        assert_eq!(retrieved.status, ListingStatus::Closed);
    }

    #[tokio::test]
    async fn test_publish_handles_dht_failure() {
        use crate::mocks::dht::MockDhtFailure;

        let dht = MockDht::new();
        let ops = ListingOperations::new(dht.clone());
        let time = MockTime::new(1000);
        let listing = make_test_listing(&time);

        dht.set_fail_mode(Some(MockDhtFailure::All)).await;

        let result = ops.publish_listing(&listing).await;
        assert!(result.is_err());
    }
}
