use anyhow::Result;
use tracing::info;

use crate::marketplace::Listing;
use crate::veilid::dht::{DHTOperations, OwnedDHTRecord};

/// DHT operations specialized for marketplace listings
pub struct ListingOperations {
    dht: DHTOperations,
}

impl ListingOperations {
    pub fn new(dht: DHTOperations) -> Self {
        Self { dht }
    }

    /// Publish a new listing to the DHT
    /// Returns the DHT record with owner keypair for future updates
    pub async fn publish_listing(&self, listing: &Listing) -> Result<OwnedDHTRecord> {
        // Create a new DHT record
        let record = self.dht.create_record().await?;

        // Serialize the listing to CBOR
        let listing_data = listing
            .to_cbor()
            .map_err(|e| anyhow::anyhow!("Failed to serialize listing: {}", e))?;

        // Store the listing in the DHT
        self.dht.set_value(&record, listing_data).await?;

        info!(
            "Published listing '{}' to DHT at key: {}",
            listing.title, record.key
        );

        Ok(record)
    }

    /// Update an existing listing in the DHT
    pub async fn update_listing(&self, record: &OwnedDHTRecord, listing: &Listing) -> Result<()> {
        // Serialize the listing to CBOR
        let listing_data = listing
            .to_cbor()
            .map_err(|e| anyhow::anyhow!("Failed to serialize listing: {}", e))?;

        // Update the value in the DHT
        self.dht.set_value(record, listing_data).await?;

        info!("Updated listing '{}' in DHT", listing.title);

        Ok(())
    }

    /// Retrieve a listing from the DHT by its record key
    pub async fn get_listing(&self, key: &veilid_core::RecordKey) -> Result<Option<Listing>> {
        // Get the value from the DHT
        let data = self.dht.get_value(key).await?;

        match data {
            Some(cbor_data) => {
                // Deserialize from CBOR
                let listing = Listing::from_cbor(&cbor_data).map_err(|e| {
                    anyhow::anyhow!("Failed to deserialize listing from DHT: {}", e)
                })?;

                info!("Retrieved listing '{}' from DHT", listing.title);
                Ok(Some(listing))
            }
            None => Ok(None),
        }
    }

    /// Delete a listing from the DHT
    pub async fn delete_listing(&self, key: &veilid_core::RecordKey) -> Result<()> {
        self.dht.delete_record(key).await?;
        info!("Deleted listing from DHT at key: {}", key);
        Ok(())
    }

    /// Watch a listing for updates (e.g., bid count changes)
    pub async fn watch_listing(&self, key: &veilid_core::RecordKey) -> Result<bool> {
        self.dht.watch_record(key).await
    }

    /// Cancel watching a listing
    pub async fn cancel_watch_listing(&self, key: &veilid_core::RecordKey) -> Result<bool> {
        self.dht.cancel_watch(key).await
    }
}
