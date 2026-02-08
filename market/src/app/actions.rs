//! Business logic for auction operations.

use market::{
    encrypt_content, generate_key, Bid, BidOperations, BidRecord, DHTOperations, DhtStore, Listing,
    ListingOperations, RegistryEntry, RegistryOperations,
};
use tracing::{info, warn};
use veilid_core::RecordKey;

use crate::app::state::SharedAppState;

/// Result of creating a listing.
pub struct CreateListingResult {
    pub key: String,
    pub title: String,
}

/// Create and publish a new auction listing.
/// The seller automatically bids at the reserve price.
pub async fn create_and_publish_listing(
    state: &SharedAppState,
    dht: &DHTOperations,
    title: &str,
    content: &str,
    reserve_price_str: &str,
    duration_str: &str,
) -> anyhow::Result<CreateListingResult> {
    // Parse inputs
    let reserve_price: u64 = reserve_price_str.parse().unwrap_or(10);
    let duration: u64 = duration_str.parse().unwrap_or(360);

    // Generate encryption key and encrypt content
    let key = generate_key();
    let (ciphertext, nonce) = encrypt_content(content, &key)?;
    let key_hex = hex::encode(key);

    // Create DHT record
    let record = dht.create_record().await?;

    // Get seller's public key
    let node_state = state.get_node_state();
    let seller_str = node_state
        .node_ids
        .first()
        .ok_or_else(|| anyhow::anyhow!("No node ID available"))?;
    let seller = veilid_core::PublicKey::try_from(seller_str.as_str())?;

    // Build listing
    let listing = Listing::builder()
        .key(record.key.clone())
        .seller(seller.clone())
        .title(title)
        .encrypted_content(ciphertext, nonce, key_hex)
        .reserve_price(reserve_price)
        .auction_duration(duration)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build listing: {}", e))?;

    // Publish to DHT
    let listing_ops = ListingOperations::new(dht.clone());
    listing_ops.update_listing(&record, &listing).await?;

    // Register in the shared listing registry
    let mut registry_ops = RegistryOperations::new(dht.clone());
    let registry_entry = RegistryEntry {
        key: record.key.to_string(),
        title: title.to_string(),
        seller: seller.to_string(),
        reserve_price,
        auction_end: listing.auction_end,
    };

    registry_ops
        .register_listing(registry_entry)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to register listing in shared registry: {}", e))?;

    // Register with auction coordinator
    if let Some(coordinator) = state.coordinator() {
        if let Err(e) = coordinator.register_owned_listing(record.clone()).await {
            tracing::warn!("Failed to register owned listing with coordinator: {}", e);
        } else {
            tracing::info!("Registered owned listing with auction coordinator");
        }
    }

    // Automatically place seller's bid at the reserve price
    let listing_key_str = record.key.to_string();
    info!(
        "Placing seller's reserve bid at {} for listing {}",
        reserve_price, listing_key_str
    );
    submit_bid(state, dht, &listing_key_str, reserve_price)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to place seller's reserve bid: {}", e))?;
    info!("Seller's reserve bid placed successfully");

    Ok(CreateListingResult {
        key: listing_key_str,
        title: title.to_string(),
    })
}

/// Fetch a listing from DHT by key string.
pub async fn fetch_listing(dht: &DHTOperations, key_str: &str) -> anyhow::Result<Listing> {
    let key = RecordKey::try_from(key_str)?;
    let listing_ops = ListingOperations::new(dht.clone());

    match listing_ops.get_listing(&key).await? {
        Some(listing) => Ok(listing),
        None => anyhow::bail!("Listing not found"),
    }
}

/// Submit a bid on a listing.
pub async fn submit_bid(
    state: &SharedAppState,
    dht: &DHTOperations,
    listing_key: &str,
    amount: u64,
) -> anyhow::Result<String> {
    // Get bidder's public key
    let node_state = state.get_node_state();
    let bidder_str = node_state
        .node_ids
        .first()
        .ok_or_else(|| anyhow::anyhow!("No node ID available"))?;
    let bidder = veilid_core::PublicKey::try_from(bidder_str.as_str())?;

    let listing_record_key = RecordKey::try_from(listing_key)?;

    // Create the bid with commitment
    let bid = Bid::new(listing_record_key.clone(), bidder.clone(), amount);

    info!(
        "Created bid: amount={}, commitment={:?}",
        amount,
        hex::encode(&bid.commitment[..8])
    );

    // Store bid value locally for later reveal
    if let Some(nonce) = bid.reveal_nonce {
        state
            .bid_storage
            .store_bid(&listing_record_key, amount, nonce)
            .await;
        info!("Stored bid value locally for MPC execution");
    }

    // Create BidRecord and publish to DHT
    let bid_ops = BidOperations::new(dht.clone());
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut bid_record = BidRecord {
        listing_key: listing_record_key.clone(),
        bidder: bidder.clone(),
        commitment: bid.commitment,
        timestamp,
        // Placeholder — will be updated with the actual DHT key after publish
        bid_key: listing_record_key.clone(),
    };
    let bid_record_own = bid_ops.publish_bid(bid_record.clone()).await?;

    // Update bid_key with the actual DHT record key
    bid_record.bid_key = bid_record_own.key.clone();

    // Store bid_key in local storage
    state
        .bid_storage
        .store_bid_key(&listing_record_key, &bid_record.bid_key)
        .await;
    info!("Stored bid key locally for MPC coordination");

    // Broadcast bid announcement
    if let Some(coordinator) = state.coordinator() {
        coordinator
            .register_local_bid(
                &listing_record_key,
                bidder.clone(),
                bid_record.bid_key.clone(),
            )
            .await;
        info!("Registered bid announcement locally");

        match coordinator
            .broadcast_bid_announcement(&listing_record_key, &bid_record.bid_key)
            .await
        {
            Ok(_) => info!("Broadcasted bid announcement to peers"),
            Err(e) => warn!("Failed to broadcast bid announcement: {}", e),
        }

        if let Err(e) = coordinator
            .add_own_bid_to_registry(
                &listing_record_key,
                bidder.clone(),
                bid_record.bid_key.clone(),
                bid_record.timestamp,
            )
            .await
        {
            tracing::warn!("Failed to add own bid to DHT registry: {}", e);
        }
    }

    // Watch listing for deadline
    let listing_ops = ListingOperations::new(dht.clone());
    if let Some(listing) = listing_ops.get_listing(&listing_record_key).await? {
        if let Some(coordinator) = state.coordinator() {
            coordinator.watch_listing(listing).await;
            info!("Watching listing for auction deadline");
        }
    }

    Ok(format!(
        "Bid of {} submitted! Commitment: {}... (Will execute MPC automatically at deadline)",
        amount,
        hex::encode(&bid.commitment[..8])
    ))
}

/// Fetch all listings from the shared registry.
pub async fn fetch_registry_listings(dht: &DHTOperations) -> anyhow::Result<Vec<(String, String)>> {
    let mut registry_ops = RegistryOperations::new(dht.clone());
    let registry = registry_ops.fetch_registry().await?;

    Ok(registry
        .listings
        .into_iter()
        .map(|e| (e.key, e.title))
        .collect())
}
