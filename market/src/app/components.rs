//! UI components for the SMPC Auction Marketplace.

use dioxus::prelude::*;
use market::veilid::node::NodeState;
use veilid_core::RecordKey;

use crate::app::actions::{
    create_and_publish_listing, fetch_listing, fetch_registry_listings, submit_bid,
};
use crate::app::state::{SharedAppState, SHARED_STATE};

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

    let create_listing = move |_| {
        let title = listing_title.read().clone();
        let content = listing_content.read().clone();
        let reserve_price = listing_reserve_price.read().clone();
        let duration = listing_duration.read().clone();
        let state = app_state.clone();

        spawn(async move {
            listing_result.set("Creating listing...".to_string());

            if title.is_empty() {
                listing_result.set("Error: Title is required".to_string());
                return;
            }

            if content.is_empty() {
                listing_result.set("Error: Content is required".to_string());
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
                    disabled: !is_attached,
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
                            let status = format!("{:?}", listing.status);
                            let listing_key = listing.key.clone();

                            let (bid_count, has_decryption_key) = if let Some(coordinator) =
                                state.coordinator()
                            {
                                let count = coordinator.get_bid_count(&listing_key).await;
                                let has_key =
                                    coordinator.get_decryption_key(&listing_key).await.is_some();
                                (count, has_key)
                            } else {
                                (0, false)
                            };

                            let info = ListingInfo {
                                key: key_str.clone(),
                                title: listing.title.clone(),
                                reserve_price: listing.reserve_price,
                                time_remaining: listing.time_remaining(),
                                status,
                                bid_count,
                                has_decryption_key,
                                decrypted_content: None,
                            };
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
                if listing.is_none() {
                    bid_result.set("Error: No listing selected".to_string());
                    return;
                }

                let listing = listing.unwrap();
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

                if listing_info.is_none() {
                    browse_result.set("Error: No listing selected".to_string());
                    return;
                }

                let listing_info = listing_info.unwrap();

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

                span { class: "label", "Listing ID:" }
                span {
                    class: "listing-id",
                    "{listing.key}"
                }
            }

            // Decrypt button for winners
            if listing.has_decryption_key && listing.decrypted_content.is_none() {
                div {
                    class: "decrypt-section",
                    h4 { "You won this auction!" }
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
