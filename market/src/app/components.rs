//! UI components for the SMPC Auction Marketplace.

use dioxus::prelude::*;
use market::veilid::node::NodeState;
use veilid_core::RecordKey;

use crate::app::state::{SharedAppState, SHARED_STATE};
use market::actions::{
    create_and_publish_listing, fetch_listing, fetch_registry_listings, submit_bid,
};
use market::marketplace::listing::PublicListing;

/// Build a `ListingInfo` from a fetched listing + coordinator state.
async fn build_listing_info(
    state: &SharedAppState,
    listing: &PublicListing,
    key_str: &str,
) -> ListingInfo {
    let status = format!("{:?}", listing.status);
    let listing_key = listing.key.clone();

    let (bid_count, has_decryption_key, auction_phase, is_seller, mpc_bytes_sent, mpc_bytes_recv) =
        if let Some(coordinator) = state.coordinator() {
            let count = coordinator.get_bid_count(&listing_key).await;
            let has_key = coordinator.get_decryption_key(&listing_key).await.is_some();
            let phase = coordinator.get_auction_phase(&listing_key).await;
            let seller = coordinator.is_listing_owner(&listing_key).await;
            let phase_label = phase.display_label().to_string();
            let (sent, recv) = coordinator.get_mpc_traffic(&listing_key).await;
            (count, has_key, phase_label, seller, sent, recv)
        } else {
            (0, false, "Unknown".to_string(), false, 0, 0)
        };

    let your_bid = state
        .bid_storage
        .get_bid(&listing_key)
        .await
        .map(|(amount, _)| amount);

    ListingInfo {
        key: key_str.to_string(),
        title: listing.title.clone(),
        reserve_price: listing.reserve_price,
        time_remaining: listing.time_remaining(),
        status,
        bid_count,
        has_decryption_key,
        decrypted_content: None,
        auction_phase,
        your_bid,
        is_seller,
        mpc_bytes_sent,
        mpc_bytes_recv,
    }
}

/// Display info for a listing in the browser.
#[derive(Clone, Default, PartialEq)]
pub struct ListingInfo {
    pub key: String,
    pub title: String,
    pub reserve_price: u64,
    pub time_remaining: u64,
    pub status: String,
    pub bid_count: usize,
    pub has_decryption_key: bool,
    pub decrypted_content: Option<String>,
    pub auction_phase: String,
    pub your_bid: Option<u64>,
    pub is_seller: bool,
    pub mpc_bytes_sent: u64,
    pub mpc_bytes_recv: u64,
}

/// Format a byte count as a human-readable string (e.g., "1.2 MB").
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * 1024;
    if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

/// Network status display component.
#[component]
pub fn NetworkStatus(state: NodeState) -> Element {
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

    rsx! {
        div {
            class: "status-card",

            h2 { "Network Status" }

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
    }
}

/// Create listing form component.
#[component]
pub fn CreateListingForm(
    is_attached: bool,
    mut known_listings: Signal<Vec<(String, String)>>,
) -> Element {
    let app_state = use_context::<SharedAppState>();
    let mut listing_title = use_signal(String::new);
    let mut listing_content = use_signal(String::new);
    let mut listing_reserve_price = use_signal(|| String::from("10"));
    let mut listing_duration = use_signal(|| String::from("100"));
    let mut listing_result = use_signal(String::new);
    let mut is_creating = use_signal(|| false);

    let mut create_listing = move |_| {
        if *is_creating.read() {
            return;
        }
        let title = listing_title.read().clone();
        let content = listing_content.read().clone();
        let reserve_price = listing_reserve_price.read().clone();
        let duration = listing_duration.read().clone();
        let state = app_state.clone();

        is_creating.set(true);
        spawn(async move {
            listing_result.set("Creating listing...".to_string());

            if title.is_empty() {
                listing_result.set("Error: Title is required".to_string());
                is_creating.set(false);
                return;
            }

            if content.is_empty() {
                listing_result.set("Error: Content is required".to_string());
                is_creating.set(false);
                return;
            }

            if let Some(dht) = state.dht_operations() {
                match create_and_publish_listing(
                    &state,
                    &dht,
                    &title,
                    &content,
                    &reserve_price,
                    &duration,
                )
                .await
                {
                    Ok(result) => {
                        listing_result
                            .set(format!("Published '{}'. Key: {}", result.title, result.key));
                        known_listings
                            .write()
                            .push((result.key.clone(), result.title.clone()));
                        listing_title.set(String::new());
                        listing_content.set(String::new());
                    }
                    Err(e) => listing_result.set(format!("Error: {}", e)),
                }
            } else {
                listing_result.set("Node not started yet".to_string());
            }
            is_creating.set(false);
        });
    };

    rsx! {
        div {
            class: "create-listing",

            h2 { "Create Auction Listing" }

            form {
                onsubmit: move |e| {
                    e.prevent_default();
                    create_listing(());
                },

                div {
                    class: "form-group",
                    label { "Title (publicly visible):" }
                    input {
                        r#type: "text",
                        value: "{listing_title}",
                        oninput: move |e| listing_title.set(e.value().clone()),
                        placeholder: "e.g., Not confidential market making information",
                    }
                }

                div {
                    class: "form-group",
                    label { "Content (will be encrypted):" }
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
                        label { "Reserve Price (atomic units):" }
                        input {
                            r#type: "number",
                            value: "{listing_reserve_price}",
                            oninput: move |e| listing_reserve_price.set(e.value().clone()),
                            placeholder: "10",
                        }
                    }

                    div {
                        class: "form-group",
                        label { "Duration (seconds):" }
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
                    disabled: !is_attached || *is_creating.read(),
                    if *is_creating.read() { "Creating..." } else { "Create & Publish Listing" }
                }

                if !listing_result.read().is_empty() {
                    div {
                        class: "result",
                        "{listing_result}"
                    }
                }
            }
        }
    }
}

/// Listing browser component.
#[component]
pub fn ListingBrowser(
    is_attached: bool,
    mut known_listings: Signal<Vec<(String, String)>>,
) -> Element {
    let app_state = use_context::<SharedAppState>();
    let mut browse_key = use_signal(String::new);
    let mut current_listing = use_signal(|| Option::<ListingInfo>::None);
    let mut browse_result = use_signal(String::new);
    let bid_amount = use_signal(|| String::from("10"));
    let mut bid_result = use_signal(String::new);

    // Auto-discover listings from registry every 5s.
    let registry_state = app_state.clone();
    let _registry_poller = use_resource(move || {
        let state = registry_state.clone();
        async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                if let Ok(listings) = fetch_registry_listings(&state).await {
                    let mut current = known_listings.write();
                    for (key, title) in &listings {
                        if !current.iter().any(|(k, _)| k == key) {
                            current.push((key.clone(), title.clone()));
                        }
                    }
                    // Auto-select first listing if none selected yet
                    if browse_key.read().is_empty() {
                        if let Some((key, _)) = listings.first() {
                            drop(current);
                            browse_key.set(key.clone());
                        }
                    }
                }
            }
        }
    });

    // Auto-fetch listing details when browse_key changes, countdown timer, live updates.
    let fetch_state = app_state.clone();
    let _auto_fetch = use_resource(move || {
        let state = fetch_state.clone();
        async move {
            let mut tick: u64 = 0;
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                tick += 1;
                let key_str = browse_key.read().clone();
                if key_str.is_empty() {
                    continue;
                }
                // Fetch listing details if not yet loaded
                if current_listing
                    .read()
                    .as_ref()
                    .is_none_or(|l| l.key != key_str)
                {
                    if let Some(dht) = state.dht_operations() {
                        if let Ok(listing) = fetch_listing(&dht, &key_str).await {
                            let info = build_listing_info(&state, &listing, &key_str).await;
                            current_listing.set(Some(info));
                        }
                    }
                }

                // Countdown timer — decrement every tick
                {
                    let mut cur = current_listing.write();
                    if let Some(info) = cur.as_mut() {
                        if info.key == key_str && info.time_remaining > 0 {
                            info.time_remaining = info.time_remaining.saturating_sub(1);
                            if info.time_remaining == 0 {
                                info.status = "Ended".to_string();
                            }
                        }
                    }
                }

                // Full re-fetch from DHT every 10s (like clicking Fetch)
                if tick.is_multiple_of(10) {
                    if let Some(dht) = state.dht_operations() {
                        if let Ok(listing) = fetch_listing(&dht, &key_str).await {
                            let info = build_listing_info(&state, &listing, &key_str).await;
                            // Preserve decrypted_content if already set
                            let decrypted = current_listing
                                .read()
                                .as_ref()
                                .and_then(|l| l.decrypted_content.clone());
                            let mut refreshed = info;
                            refreshed.decrypted_content = decrypted;
                            current_listing.set(Some(refreshed));
                        }
                    }
                    continue;
                }

                // Update phase/bid count every tick for MPC visibility
                let listing_key = match RecordKey::try_from(key_str.as_str()) {
                    Ok(k) => k,
                    Err(_) => continue,
                };
                if let Some(coordinator) = state.coordinator() {
                    let count = coordinator.get_bid_count(&listing_key).await;
                    let has_key = coordinator.get_decryption_key(&listing_key).await.is_some();
                    let phase = coordinator.get_auction_phase(&listing_key).await;
                    let seller = coordinator.is_listing_owner(&listing_key).await;
                    let phase_label = phase.display_label().to_string();
                    let (sent, recv) = coordinator.get_mpc_traffic(&listing_key).await;
                    let your_bid = state
                        .bid_storage
                        .get_bid(&listing_key)
                        .await
                        .map(|(amount, _)| amount);
                    let mut cur = current_listing.write();
                    if let Some(info) = cur.as_mut() {
                        if info.key == key_str {
                            info.bid_count = count;
                            info.has_decryption_key = has_key;
                            info.auction_phase = phase_label;
                            info.your_bid = your_bid;
                            info.is_seller = seller;
                            // Keep last nonzero traffic so totals persist after MPC ends
                            if sent > 0 {
                                info.mpc_bytes_sent = sent;
                            }
                            if recv > 0 {
                                info.mpc_bytes_recv = recv;
                            }
                        }
                    }
                }
            }
        }
    });

    let browse_listing = {
        let state = app_state.clone();
        move |_| {
            let key_str = browse_key.read().clone();
            let state = state.clone();

            spawn(async move {
                browse_result.set("Fetching listing...".to_string());

                if key_str.is_empty() {
                    browse_result.set("Error: Enter a listing key".to_string());
                    return;
                }

                if let Some(dht) = state.dht_operations() {
                    match fetch_listing(&dht, &key_str).await {
                        Ok(listing) => {
                            let info = build_listing_info(&state, &listing, &key_str).await;
                            current_listing.set(Some(info));
                            browse_result.set("Listing loaded".to_string());

                            let mut listings = known_listings.write();
                            if !listings.iter().any(|(k, _)| k == &key_str) {
                                listings.push((key_str, listing.title));
                            }
                        }
                        Err(e) => {
                            browse_result.set(format!("Error: {}", e));
                            current_listing.set(None);
                        }
                    }
                } else {
                    browse_result.set("Node not started yet".to_string());
                }
            });
        }
    };

    let submit_bid_handler = {
        let state = app_state.clone();
        move |_| {
            let listing = current_listing.read().clone();
            let amount_str = bid_amount.read().clone();
            let state = state.clone();

            spawn(async move {
                let Some(listing) = listing else {
                    bid_result.set("Error: No listing selected".to_string());
                    return;
                };
                let amount: u64 = match amount_str.parse() {
                    Ok(a) => a,
                    Err(_) => {
                        bid_result.set("Error: Invalid bid amount".to_string());
                        return;
                    }
                };

                if amount < listing.reserve_price {
                    bid_result.set(format!(
                        "Error: Bid must be at least {}",
                        listing.reserve_price
                    ));
                    return;
                }

                bid_result.set("Submitting bid...".to_string());

                if let Some(dht) = state.dht_operations() {
                    match submit_bid(&state, &dht, &listing.key, amount).await {
                        Ok(msg) => bid_result.set(msg),
                        Err(e) => bid_result.set(format!("Error: {}", e)),
                    }
                } else {
                    bid_result.set("Node not started yet".to_string());
                }
            });
        }
    };

    let decrypt_content_handler = {
        let state = app_state.clone();
        move |_| {
            let listing_info = current_listing.read().clone();
            let state = state.clone();

            spawn(async move {
                browse_result.set("Decrypting content...".to_string());

                let Some(listing_info) = listing_info else {
                    browse_result.set("Error: No listing selected".to_string());
                    return;
                };

                let listing_key = match RecordKey::try_from(listing_info.key.as_str()) {
                    Ok(key) => key,
                    Err(_) => {
                        browse_result.set("Error: Invalid listing key".to_string());
                        return;
                    }
                };

                if let Some(coordinator) = state.coordinator() {
                    match coordinator.fetch_and_decrypt_listing(&listing_key).await {
                        Ok(plaintext) => {
                            let mut updated_info = listing_info.clone();
                            updated_info.decrypted_content = Some(plaintext);
                            current_listing.set(Some(updated_info));
                            browse_result.set("Content decrypted successfully!".to_string());
                        }
                        Err(e) => {
                            browse_result.set(format!("Decryption failed: {}", e));
                        }
                    }
                } else {
                    browse_result.set("Error: Auction coordinator not available".to_string());
                }
            });
        }
    };

    let refresh_listings = {
        let state = app_state.clone();
        move |_| {
            let state = state.clone();
            spawn(async move {
                browse_result.set("Fetching listings from registry...".to_string());

                match fetch_registry_listings(&state).await {
                    Ok(listings) => {
                        let count = listings.len();
                        let mut current = known_listings.write();
                        for (key, title) in listings {
                            if !current.iter().any(|(k, _)| k == &key) {
                                current.push((key, title));
                            }
                        }
                        browse_result.set(format!("Found {} listings in registry", count));
                    }
                    Err(e) => browse_result.set(format!("Error: {}", e)),
                }
            });
        }
    };

    rsx! {
        div {
            class: "listing-browser",

            div {
                class: "header",
                h2 { "Browse Listings" }
                button {
                    class: "refresh-btn",
                    onclick: refresh_listings,
                    disabled: !is_attached,
                    "Refresh from Registry"
                }
            }

            // Known listings
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
                                    move |_| browse_key.set(key.clone())
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
                    disabled: !is_attached,
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
                ListingDisplay {
                    listing: listing.clone(),
                    is_attached: is_attached,
                    bid_amount: bid_amount,
                    bid_result: bid_result,
                    on_submit_bid: submit_bid_handler,
                    on_decrypt: decrypt_content_handler,
                }
            }
        }
    }
}

/// Listing display component.
#[component]
fn ListingDisplay(
    listing: ListingInfo,
    is_attached: bool,
    mut bid_amount: Signal<String>,
    bid_result: Signal<String>,
    on_submit_bid: EventHandler<()>,
    on_decrypt: EventHandler<()>,
) -> Element {
    rsx! {
        div {
            class: "current-listing",

            h3 { "{listing.title}" }

            div {
                class: "listing-grid",

                span { class: "label", "Status:" }
                span {
                    class: if listing.status == "Active" { "active" } else { "inactive" },
                    "{listing.status}"
                }

                span { class: "label", "Reserve:" }
                span { "{listing.reserve_price} units" }

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

                span { class: "label", "Your Bid:" }
                span {
                    class: if listing.your_bid.is_some() { "your-bid active" } else { "your-bid" },
                    if let Some(amount) = listing.your_bid {
                        "{amount} units"
                    } else {
                        "None"
                    }
                }

                span { class: "label", "MPC Phase:" }
                span {
                    class: if listing.auction_phase.starts_with("Completed") { "mpc-phase completed" } else if listing.auction_phase.starts_with("Waiting") { "mpc-phase" } else { "mpc-phase in-progress" },
                    "{listing.auction_phase}"
                }

                span { class: "label", "Listing ID:" }
                span {
                    class: "listing-id",
                    "{listing.key}"
                }
            }

            // MPC progress banner when actively executing
            if !listing.auction_phase.starts_with("Completed") && !listing.auction_phase.starts_with("Waiting") {
                div {
                    class: "mpc-banner",
                    div { class: "mpc-spinner" }
                    span { class: "mpc-label", "{listing.auction_phase}" }
                    if listing.mpc_bytes_sent > 0 || listing.mpc_bytes_recv > 0 {
                        span {
                            class: "mpc-traffic",
                            "\u{2191} {format_bytes(listing.mpc_bytes_sent)}  \u{2193} {format_bytes(listing.mpc_bytes_recv)}"
                        }
                    }
                }
            }

            // MPC traffic summary after completion
            if listing.auction_phase.starts_with("Completed") && (listing.mpc_bytes_sent > 0 || listing.mpc_bytes_recv > 0) {
                div {
                    class: if listing.has_decryption_key || listing.is_seller { "mpc-banner completed" } else { "mpc-banner lost" },
                    span { class: "mpc-label",
                        if listing.is_seller {
                            "MPC complete — seller"
                        } else if listing.has_decryption_key {
                            "MPC complete — you won!"
                        } else {
                            "MPC complete — lost"
                        }
                    }
                    span {
                        class: "mpc-traffic",
                        "\u{2191} {format_bytes(listing.mpc_bytes_sent)}  \u{2193} {format_bytes(listing.mpc_bytes_recv)}"
                    }
                }
            }

            // Decrypt button for winners/sellers
            if listing.has_decryption_key && listing.decrypted_content.is_none() {
                div {
                    class: "decrypt-section",
                    h4 {
                        if listing.is_seller {
                            "You are the seller"
                        } else {
                            "You won this auction!"
                        }
                    }
                    button {
                        class: "decrypt-btn",
                        onclick: move |_| on_decrypt.call(()),
                        disabled: !is_attached,
                        "Decrypt Content"
                    }
                }
            }

            // Decrypted content
            if let Some(content) = &listing.decrypted_content {
                div {
                    class: "decrypted-content",
                    h4 { "Decrypted Content:" }
                    div {
                        class: "content-box",
                        "{content}"
                    }
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
                            onclick: move |_| on_submit_bid.call(()),
                            disabled: !is_attached,
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

/// Main application component.
pub fn app() -> Element {
    // Provide shared state to all child components via Dioxus context
    let app_state = SHARED_STATE
        .get()
        .expect("SHARED_STATE must be initialized before launching UI")
        .clone();
    use_context_provider(|| app_state.clone());

    let mut node_state = use_signal(NodeState::default);
    let known_listings: Signal<Vec<(String, String)>> = use_signal(Vec::new);

    // Poll node state
    let _state_poller = use_resource(move || {
        let state = app_state.clone();
        async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                node_state.set(state.get_node_state());
            }
        }
    });

    let state = node_state.read();

    rsx! {
        document::Stylesheet { href: asset!("/assets/styles.css") }

        div {
            class: "container",

            h1 { "SMPC Sealed-Bid Auction" }

            NetworkStatus { state: state.clone() }

            CreateListingForm {
                is_attached: state.is_attached,
                known_listings: known_listings,
            }

            ListingBrowser {
                is_attached: state.is_attached,
                known_listings: known_listings,
            }
        }
    }
}
