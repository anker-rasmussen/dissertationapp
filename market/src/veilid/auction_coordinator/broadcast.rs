//! Broadcast and registry announcement methods.

use tracing::{debug, info, warn};
use veilid_core::{RecordKey, Target};

use super::AuctionCoordinator;
use crate::config::now_unix;
use crate::error::MarketResult;

use super::super::bid_announcement::AuctionMessage;

impl AuctionCoordinator {
    /// Broadcast our bid announcement to all known peers via AppMessage
    pub async fn broadcast_bid_announcement(
        &self,
        listing_key: &RecordKey,
        bid_record_key: &RecordKey,
    ) -> MarketResult<()> {
        info!(
            "Broadcasting bid announcement for listing {} to all peers",
            listing_key
        );

        let announcement = AuctionMessage::bid_announcement(
            listing_key.clone(),
            self.my_node_id.clone(),
            bid_record_key.clone(),
            now_unix(),
        );

        let data = announcement.to_signed_bytes(&self.signing_key)?;
        let sent = self.broadcast_message(&data).await?;

        if sent == 0 {
            warn!("No peers found to send bid announcement, queuing for retry");
            self.pending_bid_announcements
                .lock()
                .await
                .push((listing_key.clone(), bid_record_key.clone()));
        } else {
            info!("Broadcast completed: sent to {} peers", sent);
        }

        Ok(())
    }

    /// Retry bid announcements that failed to broadcast (e.g. routes weren't ready).
    pub(super) async fn retry_pending_bid_announcements(&self) {
        let pending = {
            let mut queue = self.pending_bid_announcements.lock().await;
            if queue.is_empty() {
                return;
            }
            std::mem::take(&mut *queue)
        };

        info!("Retrying {} pending bid announcement(s)", pending.len());

        for (listing_key, bid_record_key) in pending {
            let announcement = AuctionMessage::bid_announcement(
                listing_key.clone(),
                self.my_node_id.clone(),
                bid_record_key.clone(),
                now_unix(),
            );

            match announcement.to_signed_bytes(&self.signing_key) {
                Ok(data) => match self.broadcast_message(&data).await {
                    Ok(sent) if sent > 0 => {
                        info!(
                            "Retry succeeded: bid announcement for {} sent to {} peers",
                            listing_key, sent
                        );
                    }
                    Ok(_) => {
                        warn!("Retry failed: still no peers, re-queuing bid announcement");
                        self.pending_bid_announcements
                            .lock()
                            .await
                            .push((listing_key, bid_record_key));
                    }
                    Err(e) => {
                        warn!("Retry error for bid announcement: {}", e);
                        self.pending_bid_announcements
                            .lock()
                            .await
                            .push((listing_key, bid_record_key));
                    }
                },
                Err(e) => {
                    warn!("Failed to serialize bid announcement for retry: {}", e);
                }
            }
        }
    }

    /// Broadcast a message to all known peers via private routes stored in the
    /// master registry.  Uses `app_call` for confirmed delivery — `app_message`
    /// is fire-and-forget and silently drops data on stale routes.
    async fn broadcast_message(&self, data: &[u8]) -> MarketResult<usize> {
        let route_blobs = {
            let mut ops = self.registry_ops.lock().await;
            ops.fetch_route_blobs(&self.my_node_id.to_string()).await?
        };

        if route_blobs.is_empty() {
            warn!("No peer route blobs in registry, nothing to broadcast");
            return Ok(0);
        }

        let routing_context = self.api.routing_context()?;

        let mut sent_count = 0;
        for (node_id, blob) in &route_blobs {
            match self.api.import_remote_private_route(blob.clone()) {
                Ok(route_id) => {
                    match routing_context
                        .app_call(Target::RouteId(route_id.clone()), data.to_vec())
                        .await
                    {
                        Ok(_response) => {
                            sent_count += 1;
                        }
                        Err(e) => {
                            debug!("Failed to send to node {} via route: {}", node_id, e);
                        }
                    }
                    let _ = self.api.release_private_route(route_id);
                }
                Err(e) => {
                    debug!("Failed to import route for node {}: {}", node_id, e);
                }
            }
        }

        if sent_count == 0 {
            warn!("Broadcast failed for all {} route blobs", route_blobs.len());
        }

        Ok(sent_count)
    }

    /// Broadcast a `SellerRegistration` to all peers.
    pub async fn broadcast_seller_registration(&self, catalog_key: &RecordKey) -> MarketResult<()> {
        info!(
            "Broadcasting SellerRegistration with catalog {}",
            catalog_key
        );
        let msg = AuctionMessage::seller_registration(
            self.my_node_id.clone(),
            catalog_key.clone(),
            now_unix(),
        );
        let data = msg.to_signed_bytes(&self.signing_key)?;
        let sent = self.broadcast_message(&data).await?;
        info!("Sent SellerRegistration to {} peers", sent);
        Ok(())
    }

    /// Broadcast the master registry DHT key to all peers.
    ///
    /// If the registry key is not yet known, this is a no-op.
    pub async fn broadcast_registry_announcement(&self) -> MarketResult<()> {
        let key = {
            let ops = self.registry_ops.lock().await;
            ops.master_registry_key()
        };
        let Some(registry_key) = key else {
            debug!("No master registry key to broadcast");
            return Ok(());
        };
        info!(
            "Broadcasting RegistryAnnouncement with key {}",
            registry_key
        );
        let msg = AuctionMessage::registry_announcement(registry_key, now_unix());
        let data = msg.to_signed_bytes(&self.signing_key)?;
        let sent = self.broadcast_message(&data).await?;
        info!("Sent RegistryAnnouncement to {} peers", sent);
        Ok(())
    }
}
