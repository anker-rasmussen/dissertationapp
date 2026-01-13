use std::path::PathBuf;
use std::sync::Arc;

use dioxus::prelude::*;
use market::veilid::node::NodeState;
use market::{Bid, DevNetConfig, Listing, ListingOperations, RegistryEntry, RegistryOperations, VeilidNode};
use parking_lot::RwLock;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use veilid_core::RecordKey;

// Global node state for UI access
static NODE: once_cell::sync::OnceCell<Arc<RwLock<Option<VeilidNode>>>> =
    once_cell::sync::OnceCell::new();

fn init_logging() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();
}

fn get_data_dir() -> PathBuf {
    let base_dir = dirs::data_local_dir().unwrap_or_else(|| PathBuf::from("."));

    // Check if running in public network mode
    if std::env::var("MARKET_MODE").as_deref() == Ok("public") {
        return base_dir.join("smpc-auction-public");
    }

    // Use node-specific data directory for devnet based on MARKET_NODE_OFFSET
    // This allows running multiple devnet instances without conflicts
    let node_offset = std::env::var("MARKET_NODE_OFFSET")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(5);

    base_dir.join(format!("smpc-auction-node-{}", node_offset))
}

fn main() {
    init_logging();
    info!("Starting SMPC Auction Marketplace");

    // Initialize global node holder
    NODE.set(Arc::new(RwLock::new(None))).ok();

    // Start Veilid node in background thread
    let node_holder = NODE.get().unwrap().clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let data_dir = get_data_dir();
            info!("Using data directory: {:?}", data_dir);

            // Check if running in public network mode
            let mut node = if std::env::var("MARKET_MODE").as_deref() == Ok("public") {
                info!("Connecting to PUBLIC Veilid network");
                VeilidNode::new(data_dir).with_public_network()
            } else {
                info!("Connecting to LOCAL devnet");
                VeilidNode::new(data_dir).with_devnet(DevNetConfig::default())
            };

            if let Err(e) = node.start().await {
                tracing::error!("Failed to start Veilid node: {}", e);
                return;
            }

            if let Err(e) = node.attach().await {
                tracing::error!("Failed to attach to network: {}", e);
                let _ = node.shutdown().await;
                return;
            }

            // Store node for UI access
            *node_holder.write() = Some(node);

            // Keep the runtime alive
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            }
        });
    });

    // Launch Dioxus UI (synchronous)
    dioxus::launch(app);
}

fn get_node_state() -> NodeState {
    if let Some(node_holder) = NODE.get() {
        if let Some(node) = node_holder.read().as_ref() {
            return node.state();
        }
    }
    NodeState::default()
}

async fn run_dht_test(dht: &market::DHTOperations) -> anyhow::Result<String> {
    // Create a new DHT record (returns owned record with keypair)
    let record = dht.create_record().await?;

    // Set a test value (requires owner keypair)
    let test_data = b"Hello from DHT!".to_vec();
    dht.set_value(&record, test_data.clone()).await?;

    // Retrieve the value (anyone can read)
    let retrieved = dht.get_value(&record.key).await?;

    match retrieved {
        Some(data) if data == test_data => {
            Ok(format!("Created record {}, stored and retrieved {} bytes",
                record.key.to_string().chars().take(16).collect::<String>(),
                data.len()))
        }
        Some(data) => {
            anyhow::bail!("Data mismatch: expected {} bytes, got {} bytes", test_data.len(), data.len())
        }
        None => {
            anyhow::bail!("Failed to retrieve data from DHT")
        }
    }
}

/// Result of creating a listing - includes full key for browser
struct CreateListingResult {
    key: String,
    title: String,
}

async fn create_and_publish_listing(
    dht: &market::DHTOperations,
    title: &str,
    content: &str,
    min_bid_str: &str,
    duration_str: &str,
) -> anyhow::Result<CreateListingResult> {
    use market::{encrypt_content, generate_key, Listing, ListingOperations};

    // Parse inputs
    let min_bid: u64 = min_bid_str.parse().unwrap_or(1000);
    let duration: u64 = duration_str.parse().unwrap_or(3600);

    // Generate encryption key and encrypt content
    let key = generate_key();
    let (ciphertext, nonce) = encrypt_content(content, &key)?;

    // Create DHT record first
    let record = dht.create_record().await?;

    // Get seller's public key (first node ID)
    let node_state = get_node_state();
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
        .encrypted_content(ciphertext, nonce)
        .min_bid(min_bid)
        .auction_duration(duration)
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build listing: {}", e))?;

    // Publish to DHT
    let listing_ops = ListingOperations::new(dht.clone());
    listing_ops.update_listing(&record, &listing).await?;

    // Register in the shared listing registry for discovery
    let mut registry_ops = RegistryOperations::new(dht.clone());
    let registry_entry = RegistryEntry {
        key: record.key.to_string(),
        title: title.to_string(),
        seller: seller.to_string(),
        min_bid,
        auction_end: listing.auction_end,
    };

    if let Err(e) = registry_ops.register_listing(registry_entry).await {
        tracing::warn!("Failed to register listing in shared registry: {}", e);
        // Continue anyway - listing is still published to DHT
    }

    Ok(CreateListingResult {
        key: record.key.to_string(),
        title: title.to_string(),
    })
}

/// Fetch a listing from DHT by key string
async fn fetch_listing(dht: &market::DHTOperations, key_str: &str) -> anyhow::Result<Listing> {
    let key = RecordKey::try_from(key_str)?;
    let listing_ops = ListingOperations::new(dht.clone());

    match listing_ops.get_listing(&key).await? {
        Some(listing) => Ok(listing),
        None => anyhow::bail!("Listing not found"),
    }
}

/// Submit a bid on a listing
async fn submit_bid(
    _dht: &market::DHTOperations,
    listing_key: &str,
    amount: u64,
) -> anyhow::Result<String> {
    // Get bidder's public key
    let node_state = get_node_state();
    let bidder_str = node_state
        .node_ids
        .first()
        .ok_or_else(|| anyhow::anyhow!("No node ID available"))?;
    let bidder = veilid_core::PublicKey::try_from(bidder_str.as_str())?;

    let key = RecordKey::try_from(listing_key)?;

    // Create the bid with commitment
    let bid = Bid::new(key.clone(), bidder, amount);

    info!(
        "Created bid: amount={}, commitment={:?}",
        amount,
        hex::encode(&bid.commitment[..8])
    );

    // For now, we store the bid locally and log it
    // In a full implementation, this would be sent to the listing owner
    // or stored in a separate DHT record for the auction
    Ok(format!(
        "Bid of {} submitted for listing. Commitment: {}...",
        amount,
        hex::encode(&bid.commitment[..8])
    ))
}

/// Fetch all listings from the shared registry
async fn fetch_registry_listings(
    dht: &market::DHTOperations,
) -> anyhow::Result<Vec<(String, String)>> {
    let mut registry_ops = RegistryOperations::new(dht.clone());
    let registry = registry_ops.fetch_registry().await?;

    Ok(registry
        .listings
        .into_iter()
        .map(|e| (e.key, e.title))
        .collect())
}

/// Display info for a listing in the browser
#[derive(Clone, Default)]
struct ListingInfo {
    key: String,
    title: String,
    seller: String,
    min_bid: u64,
    time_remaining: u64,
    status: String,
    bid_count: u32,
}

fn app() -> Element {
    let mut node_state = use_signal(NodeState::default);
    let mut dht_test_result = use_signal(|| String::from("Not tested yet"));

    // Listing creation form state
    let mut listing_title = use_signal(|| String::new());
    let mut listing_content = use_signal(|| String::new());
    let mut listing_min_bid = use_signal(|| String::from("1000"));
    let mut listing_duration = use_signal(|| String::from("3600"));
    let mut listing_result = use_signal(|| String::new());

    // Listing browser state
    let mut known_listings: Signal<Vec<(String, String)>> = use_signal(Vec::new); // (key, title)
    let mut browse_key = use_signal(|| String::new());
    let mut current_listing = use_signal(|| Option::<ListingInfo>::None);
    let mut browse_result = use_signal(|| String::new());

    // Bidding state
    let mut bid_amount = use_signal(|| String::from("1000"));
    let mut bid_result = use_signal(|| String::new());

    // Use Dioxus's use_future for polling
    let _state_poller = use_resource(move || async move {
        loop {
            // Poll every second
            async_std::task::sleep(std::time::Duration::from_secs(1)).await;
            let state = get_node_state();
            node_state.set(state);
        }
    });

    let state = node_state.read();

    let connection_status = if state.is_attached {
        "Connected"
    } else {
        "Connecting..."
    };

    let node_id_display = if state.node_ids.is_empty() {
        "Waiting for network...".to_string()
    } else {
        state.node_ids.first().cloned().unwrap_or_default()
    };

    let test_dht = move |_| {
        spawn(async move {
            dht_test_result.set("Testing DHT operations...".to_string());

            if let Some(node_holder) = NODE.get() {
                if let Some(node) = node_holder.read().as_ref() {
                    if let Some(dht) = node.dht_operations() {
                        match run_dht_test(&dht).await {
                            Ok(msg) => dht_test_result.set(format!("✓ Success: {}", msg)),
                            Err(e) => dht_test_result.set(format!("✗ Error: {}", e)),
                        }
                    } else {
                        dht_test_result.set("✗ Node not started yet".to_string());
                    }
                } else {
                    dht_test_result.set("✗ Node not initialized".to_string());
                }
            } else {
                dht_test_result.set("✗ Node holder not found".to_string());
            }
        });
    };

    let create_listing = move |_| {
        let title = listing_title.read().clone();
        let content = listing_content.read().clone();
        let min_bid = listing_min_bid.read().clone();
        let duration = listing_duration.read().clone();

        spawn(async move {
            listing_result.set("Creating listing...".to_string());

            if title.is_empty() {
                listing_result.set("✗ Error: Title is required".to_string());
                return;
            }

            if content.is_empty() {
                listing_result.set("✗ Error: Content is required".to_string());
                return;
            }

            if let Some(node_holder) = NODE.get() {
                if let Some(node) = node_holder.read().as_ref() {
                    if let Some(dht) = node.dht_operations() {
                        match create_and_publish_listing(&dht, &title, &content, &min_bid, &duration)
                            .await
                        {
                            Ok(result) => {
                                listing_result.set(format!(
                                    "✓ Published '{}'. Key: {}",
                                    result.title, result.key
                                ));
                                // Add to known listings
                                known_listings
                                    .write()
                                    .push((result.key.clone(), result.title.clone()));
                                // Clear form
                                listing_title.set(String::new());
                                listing_content.set(String::new());
                            }
                            Err(e) => listing_result.set(format!("✗ Error: {}", e)),
                        }
                    } else {
                        listing_result.set("✗ Node not started yet".to_string());
                    }
                } else {
                    listing_result.set("✗ Node not initialized".to_string());
                }
            } else {
                listing_result.set("✗ Node holder not found".to_string());
            }
        });
    };

    // Browse listing handler
    let browse_listing = move |_| {
        let key_str = browse_key.read().clone();

        spawn(async move {
            browse_result.set("Fetching listing...".to_string());

            if key_str.is_empty() {
                browse_result.set("✗ Error: Enter a listing key".to_string());
                return;
            }

            if let Some(node_holder) = NODE.get() {
                if let Some(node) = node_holder.read().as_ref() {
                    if let Some(dht) = node.dht_operations() {
                        match fetch_listing(&dht, &key_str).await {
                            Ok(listing) => {
                                let status = format!("{:?}", listing.status);
                                let info = ListingInfo {
                                    key: key_str.clone(),
                                    title: listing.title.clone(),
                                    seller: listing.seller.to_string(),
                                    min_bid: listing.min_bid,
                                    time_remaining: listing.time_remaining(),
                                    status,
                                    bid_count: listing.bid_count,
                                };
                                current_listing.set(Some(info));
                                browse_result.set("✓ Listing loaded".to_string());

                                // Add to known listings if not already there
                                let mut listings = known_listings.write();
                                if !listings.iter().any(|(k, _)| k == &key_str) {
                                    listings.push((key_str, listing.title));
                                }
                            }
                            Err(e) => {
                                browse_result.set(format!("✗ Error: {}", e));
                                current_listing.set(None);
                            }
                        }
                    } else {
                        browse_result.set("✗ Node not started yet".to_string());
                    }
                } else {
                    browse_result.set("✗ Node not initialized".to_string());
                }
            } else {
                browse_result.set("✗ Node holder not found".to_string());
            }
        });
    };

    // Submit bid handler
    let submit_bid_handler = move |_| {
        let listing = current_listing.read().clone();
        let amount_str = bid_amount.read().clone();

        spawn(async move {
            if listing.is_none() {
                bid_result.set("✗ Error: No listing selected".to_string());
                return;
            }

            let listing = listing.unwrap();
            let amount: u64 = match amount_str.parse() {
                Ok(a) => a,
                Err(_) => {
                    bid_result.set("✗ Error: Invalid bid amount".to_string());
                    return;
                }
            };

            if amount < listing.min_bid {
                bid_result.set(format!(
                    "✗ Error: Bid must be at least {}",
                    listing.min_bid
                ));
                return;
            }

            bid_result.set("Submitting bid...".to_string());

            if let Some(node_holder) = NODE.get() {
                if let Some(node) = node_holder.read().as_ref() {
                    if let Some(dht) = node.dht_operations() {
                        match submit_bid(&dht, &listing.key, amount).await {
                            Ok(msg) => bid_result.set(format!("✓ {}", msg)),
                            Err(e) => bid_result.set(format!("✗ Error: {}", e)),
                        }
                    } else {
                        bid_result.set("✗ Node not started yet".to_string());
                    }
                } else {
                    bid_result.set("✗ Node not initialized".to_string());
                }
            } else {
                bid_result.set("✗ Node holder not found".to_string());
            }
        });
    };

    // Refresh listings from shared registry
    let refresh_listings = move |_| {
        spawn(async move {
            browse_result.set("Fetching listings from registry...".to_string());

            if let Some(node_holder) = NODE.get() {
                if let Some(node) = node_holder.read().as_ref() {
                    if let Some(dht) = node.dht_operations() {
                        match fetch_registry_listings(&dht).await {
                            Ok(listings) => {
                                let count = listings.len();
                                // Merge with existing known listings
                                let mut current = known_listings.write();
                                for (key, title) in listings {
                                    if !current.iter().any(|(k, _)| k == &key) {
                                        current.push((key, title));
                                    }
                                }
                                browse_result.set(format!(
                                    "✓ Found {} listings in registry",
                                    count
                                ));
                            }
                            Err(e) => browse_result.set(format!("✗ Error: {}", e)),
                        }
                    } else {
                        browse_result.set("✗ Node not started yet".to_string());
                    }
                } else {
                    browse_result.set("✗ Node not initialized".to_string());
                }
            } else {
                browse_result.set("✗ Node holder not found".to_string());
            }
        });
    };

    rsx! {
        document::Stylesheet { href: asset!("/assets/styles.css") }
        
        div {
            class: "container",

            h1 {
                "SMPC Sealed-Bid Auction"
            }

            div {
                class: "status-card",

                h2 {
                    "Network Status"
                }

                div {
                    class: "status-grid",

                    span { class: "label", "Status:" }
                    span {
                        class: if state.is_attached { "connected" } else { "connecting" },
                        "{connection_status}"
                    }

                    span { class: "label", "Peers:" }
                    span { "{state.peer_count}" }

                    span { class: "label", "Node ID:" }
                    span {
                        class: "node-id",
                        "{node_id_display}"
                    }
                }
            }

            div {
                class: "create-listing",

                h2 {
                    "Create Auction Listing"
                }

                form {
                    onsubmit: move |e| {
                        e.prevent_default();
                        create_listing(());
                    },

                    div {
                        class: "form-group",
                        label {
                            "Title (publicly visible):"
                        }
                        input {
                            r#type: "text",
                            value: "{listing_title}",
                            oninput: move |e| listing_title.set(e.value().clone()),
                            placeholder: "e.g., Not confidential market making information",
                        }
                    }

                    div {
                        class: "form-group",
                        label {
                            "Content (will be encrypted):"
                        }
                        textarea {
                            value: "{listing_content}",
                            oninput: move |e| listing_content.set(e.value().clone()),
                            placeholder: "content (encrypted), will be decryptable via MPC",
                        }
                    }

                    div {
                        class: "form-row",

                        div {
                            class: "form-group",
                            label {
                                "Minimum Bid (atomic units):"
                            }
                            input {
                                r#type: "number",
                                value: "{listing_min_bid}",
                                oninput: move |e| listing_min_bid.set(e.value().clone()),
                                placeholder: "10",
                            }
                        }

                        div {
                            class: "form-group",
                            label {
                                "Duration (seconds):"
                            }
                            input {
                                r#type: "number",
                                value: "{listing_duration}",
                                oninput: move |e| listing_duration.set(e.value().clone()),
                                placeholder: "360",
                            }
                        }
                    }

                    button {
                        class: "submit-btn",
                        r#type: "submit",
                        disabled: !state.is_attached,
                        "Create & Publish Listing"
                    }

                    if !listing_result.read().is_empty() {
                        div {
                            class: "result",
                            "{listing_result}"
                        }
                    }
                }
            }

            // Listing Browser Section
            div {
                class: "listing-browser",

                div {
                    class: "header",
                    h2 {
                        "Browse Listings"
                    }
                    button {
                        class: "refresh-btn",
                        onclick: refresh_listings,
                        disabled: !state.is_attached,
                        "Refresh from Registry"
                    }
                }

                // Known listings list
                if !known_listings.read().is_empty() {
                    div {
                        class: "known-listings",
                        h3 { "Known Listings:" }
                        div {
                            class: "listing-buttons",
                            for (key, title) in known_listings.read().iter() {
                                button {
                                    class: "listing-btn",
                                    onclick: {
                                        let key = key.clone();
                                        move |_| {
                                            browse_key.set(key.clone());
                                        }
                                    },
                                    "{title} ({key})"
                                }
                            }
                        }
                    }
                }

                // Manual key input
                div {
                    class: "manual-input",
                    input {
                        r#type: "text",
                        value: "{browse_key}",
                        oninput: move |e| browse_key.set(e.value().clone()),
                        placeholder: "Enter listing key (VLD0:...)",
                    }
                    button {
                        class: "fetch-btn",
                        onclick: browse_listing,
                        disabled: !state.is_attached,
                        "Fetch"
                    }
                }

                if !browse_result.read().is_empty() {
                    div {
                        class: "browse-result",
                        "{browse_result}"
                    }
                }

                // Current listing display
                if let Some(listing) = current_listing.read().as_ref() {
                    div {
                        class: "current-listing",

                        h3 {
                            "{listing.title}"
                        }

                        div {
                            class: "listing-grid",

                            span { class: "label", "Status:" }
                            span {
                                class: if listing.status == "Active" { "active" } else { "inactive" },
                                "{listing.status}"
                            }

                            span { class: "label", "Min Bid:" }
                            span { "{listing.min_bid} units" }

                            span { class: "label", "Time Left:" }
                            span {
                                if listing.time_remaining > 0 {
                                    "{listing.time_remaining} seconds"
                                } else {
                                    "Auction ended"
                                }
                            }

                            span { class: "label", "Bids:" }
                            span { "{listing.bid_count}" }

                            span { class: "label", "Seller:" }
                            span {
                                class: "seller-id",
                                "{listing.seller}"
                            }
                        }

                        // Bidding form
                        if listing.time_remaining > 0 {
                            div {
                                class: "bidding-form",

                                h4 { "Place Your Bid" }

                                div {
                                    class: "bid-input-group",
                                    input {
                                        class: "bid-input",
                                        r#type: "number",
                                        value: "{bid_amount}",
                                        oninput: move |e| bid_amount.set(e.value().clone()),
                                        placeholder: "Bid amount",
                                    }
                                    button {
                                        class: "bid-btn",
                                        onclick: submit_bid_handler,
                                        disabled: !state.is_attached,
                                        "Submit Bid"
                                    }
                                }

                                if !bid_result.read().is_empty() {
                                    div {
                                        class: "bid-result",
                                        "{bid_result}"
                                    }
                                }
                            }
                        }
                    }
                }
            }

            div {
                class: "dht-test",

                h2 {
                    "DHT Operations Test"
                }

                button {
                    class: "test-btn",
                    onclick: test_dht,
                    disabled: !state.is_attached,
                    "Test DHT Create/Set/Get"
                }

                div {
                    class: "test-result",
                    "{dht_test_result}"
                }
            }
        }
    }
}
