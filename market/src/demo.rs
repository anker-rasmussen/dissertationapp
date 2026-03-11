//! Auto-demo mode: drives a full auction flow without manual interaction.
//!
//! The seller task creates a listing and waits; bidder tasks discover
//! the listing via registry polling and place random bids. All use
//! the same `actions` functions as the GUI, so the demo exercises
//! the real code path.

use market::actions::{create_and_publish_listing, fetch_registry_listings, submit_bid};
use market::shared_state::SharedAppState;
use tracing::{error, info, warn};

/// Run the seller demo: create a listing and log its key.
pub async fn demo_seller_task(state: SharedAppState, duration: u64) {
    info!(
        "=== DEMO: Seller creating listing (duration={}s) ===",
        duration
    );

    let Some(dht) = state.dht_operations() else {
        error!("=== DEMO: DHT not available ===");
        return;
    };

    match create_and_publish_listing(
        &state,
        &dht,
        "Demo Auction",
        "Secret content \u{2014} only the winner can read this!",
        "10",
        &duration.to_string(),
    )
    .await
    {
        Ok(result) => {
            info!("=== DEMO: Listing published! Key: {} ===", result.key);
        }
        Err(e) => {
            error!("=== DEMO: Failed to create listing: {} ===", e);
        }
    }
}

/// Run the bidder demo: poll for listings, then bid on the first one found.
pub async fn demo_bidder_task(state: SharedAppState) {
    info!("=== DEMO: Bidder waiting for listings... ===");

    let timeout = std::time::Duration::from_secs(300);
    let start = std::time::Instant::now();

    let (key, title) = loop {
        if start.elapsed() >= timeout {
            error!("=== DEMO: Timed out waiting for listings ===");
            return;
        }

        match fetch_registry_listings(&state).await {
            Ok(listings) if !listings.is_empty() => {
                let listing = listings.into_iter().next().expect("non-empty");
                break listing;
            }
            Ok(_) => {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
            Err(e) => {
                warn!("DEMO: Registry fetch error (retrying): {}", e);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    };

    info!("=== DEMO: Found listing '{}' ({}) ===", title, key);

    let Some(dht) = state.dht_operations() else {
        error!("=== DEMO: DHT not available ===");
        return;
    };

    // Random bid between 15 and 99
    let bid_amount: u64 = 15 + (rand::random::<u64>() % 85);

    info!("=== DEMO: Placing bid of {} on '{}' ===", bid_amount, title);

    match submit_bid(&state, &dht, &key, bid_amount).await {
        Ok(msg) => info!("=== DEMO: Bid placed! {} ===", msg),
        Err(e) => error!("=== DEMO: Failed to place bid: {} ===", e),
    }
}
