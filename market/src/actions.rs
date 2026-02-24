//! Business logic for auction operations.
//!
//! These functions drive the full create-listing and place-bid flows,
//! shared by the GUI binary and the headless test binary.

use crate::error::{MarketError, MarketResult};
use crate::shared_state::SharedAppState;
use crate::{
    encrypt_content, generate_key, Bid, BidOperations, BidRecord, CatalogEntry, DHTOperations,
    DhtStore, Listing, ListingOperations, PublicListing,
};
use tracing::{info, warn};
use veilid_core::RecordKey;

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
) -> MarketResult<CreateListingResult> {
    // Parse inputs
    let reserve_price: u64 = reserve_price_str.parse().map_err(|e| {
        MarketError::Validation(format!("Invalid reserve price '{reserve_price_str}': {e}"))
    })?;
    let duration: u64 = duration_str
        .parse()
        .map_err(|e| MarketError::Validation(format!("Invalid duration '{duration_str}': {e}")))?;

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
        .ok_or_else(|| MarketError::InvalidState("No node ID available".into()))?;
    let seller = veilid_core::PublicKey::try_from(seller_str.as_str())?;

    // Build listing
    let listing = Listing::builder()
        .key(record.key.clone())
        .seller(seller.clone())
        .title(title)
        .encrypted_content(ciphertext, nonce, key_hex)
        .reserve_price(reserve_price)
        .auction_duration(duration)
        .build()?;

    // Publish to DHT
    let listing_ops = ListingOperations::new(dht.clone());
    listing_ops.update_listing(&record, &listing).await?;

    // Register with auction coordinator and per-seller catalog
    if let Some(coordinator) = state.coordinator() {
        // Store decryption key locally so the seller can send it to the winner later.
        // The DHT only stores PublicListing (no decryption key).
        coordinator
            .store_decryption_key(&record.key, listing.decryption_key.clone())
            .await;

        // Ensure the master registry exists (creates on first listing)
        let _registry_key = coordinator
            .ensure_master_registry()
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to ensure master registry: {e}")))?;

        // Ensure seller has a catalog DHT record
        let catalog_key = {
            let mut ops = coordinator.registry_ops().lock().await;
            ops.get_or_create_seller_catalog(&seller.to_string())
                .await
                .map_err(|e| MarketError::Dht(format!("Failed to create seller catalog: {e}")))?
        };

        // Add listing to seller's own catalog
        coordinator
            .registry_ops()
            .lock()
            .await
            .add_listing_to_catalog(CatalogEntry {
                listing_key: record.key.to_string(),
                title: title.to_string(),
                reserve_price,
                auction_end: listing.auction_end,
            })
            .await
            .map_err(|e| MarketError::Dht(format!("Failed to add listing to catalog: {e}")))?;

        // Register seller directly in the master registry (shared keypair)
        {
            let signing_hex = coordinator.signing_pubkey_hex();
            let mut ops = coordinator.registry_ops().lock().await;
            if let Err(e) = ops
                .register_seller(&seller.to_string(), &catalog_key.to_string(), &signing_hex)
                .await
            {
                tracing::warn!("Failed to register seller in master registry: {}", e);
            }
        }

        // Broadcast registry key AFTER DHT writes complete, so peers can read
        // the populated registry immediately upon learning the key
        if let Err(e) = coordinator.broadcast_registry_announcement().await {
            tracing::warn!("Failed to broadcast registry announcement: {}", e);
        }

        // Also broadcast seller registration for real-time peer notification
        if let Err(e) = coordinator
            .broadcast_seller_registration(&catalog_key)
            .await
        {
            tracing::warn!("Failed to broadcast seller registration: {}", e);
        }

        // Register owned listing with coordinator (for bid tracking / MPC)
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
        .map_err(|e| MarketError::Dht(format!("Failed to place seller's reserve bid: {e}")))?;
    info!("Seller's reserve bid placed successfully");

    Ok(CreateListingResult {
        key: listing_key_str,
        title: title.to_string(),
    })
}

/// Fetch a listing from DHT by key string.
pub async fn fetch_listing(dht: &DHTOperations, key_str: &str) -> MarketResult<PublicListing> {
    let key = RecordKey::try_from(key_str)?;
    let listing_ops = ListingOperations::new(dht.clone());

    match listing_ops.get_listing(&key).await? {
        Some(listing) => Ok(listing),
        None => Err(MarketError::NotFound("Listing not found".into())),
    }
}

/// Submit a bid on a listing.
pub async fn submit_bid(
    state: &SharedAppState,
    dht: &DHTOperations,
    listing_key: &str,
    amount: u64,
) -> MarketResult<String> {
    // Get bidder's public key
    let node_state = state.get_node_state();
    let bidder_str = node_state
        .node_ids
        .first()
        .ok_or_else(|| MarketError::InvalidState("No node ID available".into()))?;
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
    let timestamp = crate::config::now_unix();
    let signing_pubkey = state
        .coordinator()
        .map_or([0u8; 32], |c| c.signing_pubkey_bytes());
    let mut bid_record = BidRecord {
        listing_key: listing_record_key.clone(),
        bidder: bidder.clone(),
        commitment: bid.commitment,
        timestamp,
        // Placeholder — will be updated with the actual DHT key after publish
        bid_key: listing_record_key.clone(),
        signing_pubkey,
    };
    let bid_record_own = bid_ops.publish_bid(bid_record.clone()).await?;

    // Update bid_key with the actual DHT record key
    bid_record.bid_key = bid_record_own.key.clone();
    // Re-write finalized BidRecord so persisted data carries the real bid_key.
    dht.set_value(&bid_record_own, bid_record.to_cbor()?)
        .await?;

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
            Ok(()) => info!("Broadcasted bid announcement to peers"),
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

/// Fetch all listings via two-hop discovery (master registry → seller catalogs).
///
/// Returns an empty list if the coordinator is unavailable or the registry key
/// has not been received yet (no `RegistryAnnouncement` from a peer).
pub async fn fetch_registry_listings(
    state: &SharedAppState,
) -> MarketResult<Vec<(String, String)>> {
    let Some(coordinator) = state.coordinator() else {
        return Ok(Vec::new());
    };

    let listings = coordinator.fetch_all_listings().await?;
    Ok(listings.into_iter().map(|e| (e.key, e.title)).collect())
}
