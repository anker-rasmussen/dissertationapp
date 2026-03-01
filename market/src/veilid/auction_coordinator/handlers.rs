//! Auction message handlers for each `AuctionMessage` variant.

use tracing::{debug, error, info, warn};
use veilid_core::{PublicKey, RecordKey, RouteBlob, Target};

use super::{AuctionCoordinator, BufferedMpcSignal};
use crate::config::{self, now_unix, subkeys};
use crate::error::{MarketError, MarketResult};

use super::super::bid_announcement::{AuctionMessage, BidAnnouncementRegistry};

impl AuctionCoordinator {
    pub(super) async fn handle_bid_announcement(
        &self,
        listing_key: RecordKey,
        bidder: PublicKey,
        bid_record_key: RecordKey,
        timestamp: u64,
        signer: [u8; 32],
    ) -> MarketResult<()> {
        info!(
            "Received bid announcement for listing {} from bidder {}",
            listing_key, bidder
        );

        if !self
            .validate_bid_announcement_signer(&listing_key, &bidder, &bid_record_key, signer)
            .await?
        {
            return Ok(());
        }

        // Delegate local announcement tracking to AuctionLogic
        self.logic
            .register_bid_announcement(&listing_key, bidder.clone(), bid_record_key.clone())
            .await;

        // If we own this listing, update DHT bid registry
        let record = {
            let owned = self.owned_listings.lock().await;
            owned.get(&listing_key).cloned()
        };
        if let Some(record) = record {
            info!("We own this listing, updating DHT bid registry");
            let _guard = self.bid_registry_lock.lock().await;

            self.dht
                .read_modify_write_subkey(&record, subkeys::BID_ANNOUNCEMENTS, |existing| {
                    let mut registry = existing
                        .and_then(|d| BidAnnouncementRegistry::from_bytes(&d).ok())
                        .unwrap_or_else(BidAnnouncementRegistry::new);
                    registry.add(bidder.clone(), bid_record_key.clone(), timestamp);
                    info!(
                        "Updated DHT bid registry for listing {}, now has {} announcements",
                        listing_key,
                        registry.announcements.len()
                    );
                    registry.to_bytes()
                })
                .await?;
        }
        Ok(())
    }

    pub(super) async fn handle_winner_decryption_request(
        &self,
        listing_key: RecordKey,
        claimed_winner: PublicKey,
        signer: [u8; 32],
    ) -> MarketResult<()> {
        info!(
            "Received WinnerDecryptionRequest (challenge) for listing {}",
            listing_key
        );

        if claimed_winner != self.my_node_id {
            debug!(
                "Ignoring WinnerDecryptionRequest for different winner {}",
                claimed_winner
            );
            return Ok(());
        }

        let Some(expected_seller_signer) =
            self.seller_signing_key_for_listing(&listing_key).await?
        else {
            warn!("Rejecting challenge for listing {listing_key}: seller signing key not found");
            return Ok(());
        };
        if expected_seller_signer != signer {
            warn!("Rejecting challenge for listing {listing_key}: signer does not match seller");
            return Ok(());
        }

        if !self.mpc.is_expected_winner(&listing_key).await {
            warn!("Rejecting challenge for listing {listing_key}: node is not expected winner");
            return Ok(());
        }
        info!("Challenge accepted for listing {listing_key}: node is expected winner");

        let Some((bid_value, nonce)) = self.mpc.bid_storage().get_bid(&listing_key).await else {
            let has_bid_key = self
                .mpc
                .bid_storage()
                .get_bid_key(&listing_key)
                .await
                .is_some();
            warn!(
                "Cannot respond to challenge for listing {listing_key}: local bid value/nonce missing (bid key present: {has_bid_key})"
            );
            return Ok(());
        };
        info!("Challenge handling has local bid material for listing {listing_key}");

        info!("Responding to seller's challenge with bid reveal (value: {bid_value})");
        if let Err(e) = self
            .send_bid_reveal_to_seller(&listing_key, bid_value, nonce)
            .await
        {
            error!("Failed responding to challenge for listing {listing_key}: {e}");
            return Ok(());
        }
        Ok(())
    }

    /// Send a WinnerBidReveal to the seller via their MPC route.
    async fn send_bid_reveal_to_seller(
        &self,
        listing_key: &RecordKey,
        bid_value: u64,
        nonce: [u8; 32],
    ) -> MarketResult<()> {
        let listing_ops = super::super::listing_ops::ListingOperations::new(self.dht.clone());
        let seller_pubkey = match listing_ops.get_listing(listing_key).await {
            Ok(Some(listing)) => {
                info!("Resolved seller pubkey from listing for {listing_key}");
                Some(listing.seller)
            }
            Ok(None) => {
                warn!("Listing missing while sending reveal for {listing_key}");
                None
            }
            Err(e) => {
                warn!("Failed to resolve listing for reveal on {listing_key}: {e}");
                None
            }
        };

        let message = AuctionMessage::winner_bid_reveal(
            listing_key.clone(),
            self.my_node_id.clone(),
            bid_value,
            nonce,
            now_unix(),
        );

        let data = message.to_signed_bytes(&self.signing_key)?;
        let start = std::time::Instant::now();
        let max_wait = std::time::Duration::from_secs(config::WINNER_REVEAL_TIMEOUT_SECS);

        loop {
            let route_manager = {
                let managers = self.mpc.route_managers().lock().await;
                managers.get(listing_key).cloned()
            };

            let Some(route_manager) = route_manager else {
                if start.elapsed() >= max_wait {
                    return Err(MarketError::Network(format!(
                        "No route manager for listing {listing_key} after waiting {max_wait:?}"
                    )));
                }
                warn!("No route manager for listing {listing_key}, retrying reveal send");
                tokio::time::sleep(tokio::time::Duration::from_secs(
                    config::WINNER_REVEAL_RETRY_SECS,
                ))
                .await;
                continue;
            };

            let seller_route = {
                let received_routes = route_manager.lock().await.received_routes.clone();
                let routes = received_routes.lock().await;
                seller_pubkey
                    .as_ref()
                    .and_then(|seller| routes.get(seller).cloned())
            };

            let Some(route_id) = seller_route else {
                if start.elapsed() >= max_wait {
                    return Err(MarketError::Network(format!(
                        "Seller route unavailable for listing {listing_key} after waiting {max_wait:?}"
                    )));
                }
                warn!("Seller route not found for listing {listing_key}, retrying reveal send");
                tokio::time::sleep(tokio::time::Duration::from_secs(
                    config::WINNER_REVEAL_RETRY_SECS,
                ))
                .await;
                continue;
            };

            let routing_context = self.mpc.safe_routing_context()?;
            match routing_context
                .app_call(Target::RouteId(route_id), data.clone())
                .await
            {
                Ok(_reply) => {
                    info!("Sent WinnerBidReveal to seller via MPC route (confirmed)");
                    return Ok(());
                }
                Err(e) => {
                    if start.elapsed() >= max_wait {
                        return Err(MarketError::Network(format!(
                            "Failed to send WinnerBidReveal within {max_wait:?}: {e}"
                        )));
                    }
                    warn!("Failed to send WinnerBidReveal, retrying: {e}");
                    tokio::time::sleep(tokio::time::Duration::from_secs(
                        config::WINNER_REVEAL_RETRY_SECS,
                    ))
                    .await;
                }
            }
        }
    }

    pub(super) async fn handle_decryption_hash_transfer(
        &self,
        listing_key: RecordKey,
        winner: PublicKey,
        decryption_hash: String,
        signer: [u8; 32],
    ) -> MarketResult<()> {
        info!(
            "Received decryption hash transfer for listing {} (winner: {})",
            listing_key, winner
        );

        let Some(expected_seller_signer) =
            self.seller_signing_key_for_listing(&listing_key).await?
        else {
            warn!(
                "Rejecting decryption transfer for listing {listing_key}: seller signing key not found"
            );
            return Ok(());
        };
        if expected_seller_signer != signer {
            warn!(
                "Rejecting decryption transfer for listing {listing_key}: signer does not match seller"
            );
            return Ok(());
        }

        // Delegate state management to AuctionLogic
        self.logic
            .process_decryption_transfer(listing_key.clone(), winner.clone(), decryption_hash)
            .await?;

        if winner == self.my_node_id {
            self.mpc.clear_expected_winner(&listing_key).await;
            self.mpc.cleanup_route_manager(&listing_key).await;
        }
        Ok(())
    }

    pub(super) async fn handle_mpc_route_announcement(
        &self,
        listing_key: RecordKey,
        party_pubkey: PublicKey,
        route_blob: RouteBlob,
        signer: [u8; 32],
    ) -> MarketResult<()> {
        info!(
            "Received MPC route announcement for listing {} from party {}",
            listing_key, party_pubkey
        );

        let Some(expected_signer) = self
            .expected_bidder_signing_key(&listing_key, &party_pubkey)
            .await?
        else {
            warn!(
                "Rejecting MPC route announcement for listing {listing_key}: signer binding not found for party {party_pubkey}"
            );
            return Ok(());
        };
        if expected_signer != signer {
            warn!(
                "Rejecting MPC route announcement for listing {listing_key}: signer mismatch for party {party_pubkey}"
            );
            return Ok(());
        }

        let manager = {
            let managers = self.mpc.route_managers().lock().await;
            managers.get(&listing_key).cloned()
        };

        if let Some(manager) = manager {
            manager
                .lock()
                .await
                .register_route_announcement(party_pubkey, route_blob)
                .await?;
        } else {
            debug!(
                "Buffering MPC route announcement for unwatched auction {}",
                listing_key
            );
            self.buffer_mpc_signal(
                listing_key,
                BufferedMpcSignal::RouteAnnouncement {
                    party_pubkey,
                    route_blob,
                    received_at: now_unix(),
                },
            )
            .await;
        }
        Ok(())
    }

    pub(super) async fn handle_mpc_ready(
        &self,
        listing_key: RecordKey,
        party_pubkey: PublicKey,
        num_parties: u32,
        timestamp: u64,
        signer: [u8; 32],
    ) -> MarketResult<()> {
        info!(
            "Received MpcReady for listing {} from party {}",
            listing_key, party_pubkey
        );

        let Some(expected_signer) = self
            .expected_bidder_signing_key(&listing_key, &party_pubkey)
            .await?
        else {
            warn!(
                "Rejecting MpcReady for listing {listing_key}: signer binding not found for party {party_pubkey}"
            );
            return Ok(());
        };
        if expected_signer != signer {
            warn!(
                "Rejecting MpcReady for listing {listing_key}: signer mismatch for party {party_pubkey}"
            );
            return Ok(());
        }

        let manager = {
            let managers = self.mpc.route_managers().lock().await;
            managers.get(&listing_key).cloned()
        };

        if let Some(manager) = manager {
            manager
                .lock()
                .await
                .register_ready(party_pubkey, num_parties, timestamp)
                .await;
        } else {
            debug!("Buffering MpcReady for unwatched auction {}", listing_key);
            self.buffer_mpc_signal(
                listing_key,
                BufferedMpcSignal::Ready {
                    party_pubkey,
                    num_parties,
                    timestamp,
                    received_at: now_unix(),
                },
            )
            .await;
        }
        Ok(())
    }

    pub(super) async fn handle_winner_bid_reveal(
        &self,
        listing_key: RecordKey,
        winner: PublicKey,
        bid_value: u64,
        nonce: [u8; 32],
        signer: [u8; 32],
    ) -> MarketResult<()> {
        info!(
            "Received WinnerBidReveal for listing {} from winner {}",
            listing_key, winner
        );

        let Some(expected_signer) = self
            .expected_bidder_signing_key(&listing_key, &winner)
            .await?
        else {
            warn!(
                "Rejecting WinnerBidReveal for listing {listing_key}: signer binding not found for winner {winner}"
            );
            return Ok(());
        };
        if expected_signer != signer {
            warn!(
                "Rejecting WinnerBidReveal for listing {listing_key}: signer mismatch for winner {winner}"
            );
            return Ok(());
        }

        let verified = self
            .mpc
            .verify_winner_reveal(&listing_key, &winner, bid_value, &nonce)
            .await;

        // Store verification result
        {
            let mut verifications = self.mpc.pending_verifications().lock().await;
            if let Some(state) = verifications.get_mut(&listing_key) {
                state.verified = Some(verified);
            } else {
                warn!(
                    "No pending verification entry for listing {} — result dropped",
                    listing_key
                );
            }
        }

        if verified {
            info!(
                "Winner bid VERIFIED for listing {} — sending decryption key immediately",
                listing_key
            );

            // The decryption key is stored locally by the seller when they create the listing.
            // It is never published to the DHT (PublicListing omits it).
            match self.get_decryption_key(&listing_key).await {
                Some(dec_key) => {
                    if let Err(e) = self
                        .mpc
                        .send_decryption_hash(&listing_key, &winner, &dec_key)
                        .await
                    {
                        error!("Failed to send decryption key to winner: {}", e);
                    }
                }
                None => {
                    warn!(
                        "Decryption key not found locally for listing {}",
                        listing_key
                    );
                }
            }
        } else {
            warn!(
                "Winner bid verification FAILED for listing {} — withholding decryption key",
                listing_key
            );
        }
        Ok(())
    }

    pub(super) async fn handle_seller_registration(
        &self,
        seller_pubkey: PublicKey,
        catalog_key: RecordKey,
        signer: [u8; 32],
    ) -> MarketResult<()> {
        let signing_hex = hex::encode(signer);
        info!(
            "Received SellerRegistration from {} with catalog {} (signer: {})",
            seller_pubkey, catalog_key, signing_hex
        );

        // Check registry state without holding lock across .await
        let has_registry = {
            let ops = self.registry_ops.lock().await;
            ops.master_registry_key().is_some()
        };

        if has_registry {
            // Re-acquire for the async register_seller call
            let mut ops = self.registry_ops.lock().await;
            if let Err(e) = ops
                .register_seller(
                    &seller_pubkey.to_string(),
                    &catalog_key.to_string(),
                    &signing_hex,
                )
                .await
            {
                warn!("Failed to register seller {}: {}", seller_pubkey, e);
            }
        } else {
            info!(
                "Master registry key not yet known, buffering SellerRegistration from {}",
                seller_pubkey
            );
            let mut pending = self.pending_seller_registrations.lock().await;
            pending.push((seller_pubkey, catalog_key, signing_hex));
        }
        Ok(())
    }

    pub(super) async fn handle_registry_announcement(
        &self,
        registry_key: RecordKey,
    ) -> MarketResult<()> {
        // Phase 1: set the key (short lock scope, no .await inside)
        let should_replay = {
            let mut ops = self.registry_ops.lock().await;
            let existing = ops.master_registry_key();
            if existing.is_none() {
                info!(
                    "Received RegistryAnnouncement, setting master registry key: {}",
                    registry_key
                );
                ops.set_master_registry_key(registry_key);
                drop(ops);
                true
            } else if existing.as_ref() == Some(&registry_key) {
                debug!("Received duplicate RegistryAnnouncement, ignoring");
                false
            } else {
                debug!(
                    "Received RegistryAnnouncement with different key {}, keeping first-write-wins key",
                    registry_key
                );
                false
            }
        };

        // Phase 1b: seed local replica from DHT now that we know the key
        if should_replay {
            let mut ops = self.registry_ops.lock().await;
            if let Err(e) = ops.fetch_registry(true).await {
                warn!("Failed to seed local replica from DHT: {}", e);
            }
            drop(ops);
        }

        // Phase 2: drain pending registrations and replay (locks acquired separately)
        if should_replay {
            let pending = {
                let mut buf = self.pending_seller_registrations.lock().await;
                std::mem::take(&mut *buf)
            };
            if !pending.is_empty() {
                info!("Replaying {} buffered SellerRegistrations", pending.len());
                for (seller_pubkey, catalog_key, signing_hex) in pending {
                    let mut ops = self.registry_ops.lock().await;
                    if let Err(e) = ops
                        .register_seller(
                            &seller_pubkey.to_string(),
                            &catalog_key.to_string(),
                            &signing_hex,
                        )
                        .await
                    {
                        warn!(
                            "Failed to register buffered seller {}: {}",
                            seller_pubkey, e
                        );
                    }
                }
            }
        }
        Ok(())
    }
}
