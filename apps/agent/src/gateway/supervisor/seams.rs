//! Store/runtime adapters for the T17 and T26 integration boundaries.

use std::fmt;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use sqlx::Row;
use tokio::sync::{mpsc, watch};

use super::{
    CommandCursors, CredentialProvider, DeliveryEpoch, DurableSource, EventCursors, EventSender,
    GatewayCredential, HydrationLatch, HydrationReady, OutboundFrame,
};
use crate::gateway::Envelope;
use crate::runtime::contracts::ProcessGeneration;
use crate::store::{
    DeliveryChannelBuilder, DeliveryFrame, DeliveryMode, DeliveryPump, HydrationReceiptIdentity,
    Store, current_event_head_seq, raw_events_after,
};

/// T17's typed hydration receipt projected into T24's latched readiness boundary.
#[derive(Clone)]
pub struct T17HydrationLatch {
    rx: watch::Receiver<Option<HydrationReceiptIdentity>>,
    observed: Arc<Mutex<Option<HydrationReady>>>,
}

impl fmt::Debug for T17HydrationLatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("T17HydrationLatch").finish_non_exhaustive()
    }
}

impl T17HydrationLatch {
    pub(crate) fn new(rx: watch::Receiver<Option<HydrationReceiptIdentity>>) -> Self {
        Self {
            rx,
            observed: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl HydrationLatch for T17HydrationLatch {
    async fn wait_for(&self, generation: ProcessGeneration) -> Result<HydrationReady> {
        let mut rx = self.rx.clone();
        loop {
            let receipt = rx.borrow().clone();
            if let Some(receipt) = receipt {
                if receipt.generation != generation {
                    bail!(
                        "T17 hydration receipt generation mismatch: expected {generation}, got {}",
                        receipt.generation
                    );
                }
                let ready = HydrationReady {
                    generation,
                    receipt_identity: receipt.stable_id(),
                };
                let mut observed = self.observed.lock().unwrap();
                if let Some(previous) = observed.as_ref()
                    && previous.generation == generation
                    && previous.receipt_identity != ready.receipt_identity
                {
                    bail!("T17 hydration receipt changed for generation {generation}");
                }
                if observed
                    .as_ref()
                    .is_some_and(|previous| previous.generation == generation)
                {
                    return Ok(observed.clone().expect("observed receipt exists"));
                }
                *observed = Some(ready.clone());
                return Ok(ready);
            }
            drop(receipt);
            rx.changed()
                .await
                .context("T17 hydration receipt channel dropped before Ready")?;
        }
    }
}

/// Real T17 Store adapter used by the T24 supervisor for durable cursors,
/// catch-up pages, and the DeliveryPump epoch lifecycle.
#[derive(Clone)]
pub struct T17StoreAdapter {
    store: Arc<Store>,
    pump: Arc<tokio::sync::Mutex<DeliveryPump>>,
    delivery_rx: Arc<Mutex<Option<mpsc::Receiver<DeliveryFrame>>>>,
    #[cfg(test)]
    replay_page_lengths: Arc<Mutex<Vec<usize>>>,
}

impl fmt::Debug for T17StoreAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("T17StoreAdapter").finish_non_exhaustive()
    }
}

impl T17StoreAdapter {
    pub(crate) fn new(store: Arc<Store>) -> Self {
        let (channel, delivery_rx) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw).build();
        Self {
            store: store.clone(),
            pump: Arc::new(tokio::sync::Mutex::new(DeliveryPump::new(store, channel))),
            delivery_rx: Arc::new(Mutex::new(Some(delivery_rx))),
            #[cfg(test)]
            replay_page_lengths: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn start_forwarder(&self, sink: EventSender) {
        let Some(mut rx) = self.delivery_rx.lock().unwrap().take() else {
            return;
        };
        let conversation_id = self.store.scope().conversation_id.clone();
        tokio::spawn(async move {
            while let Some(frame) = rx.recv().await {
                let (epoch, seq, event) = match frame {
                    DeliveryFrame::Durable {
                        seq,
                        epoch,
                        raw: Some(event),
                        projection: None,
                    } => (epoch, Some(seq), event),
                    DeliveryFrame::Volatile { epoch, event } => (epoch, None, event),
                    DeliveryFrame::Durable { .. } => {
                        tracing::error!(
                            "raw T17 gateway delivery received a projection-only frame"
                        );
                        break;
                    }
                };
                let event = match serde_json::to_value(event) {
                    Ok(event) => event,
                    Err(error) => {
                        tracing::error!(%error, "failed to serialize T17 delivery event");
                        break;
                    }
                };
                let outbound = OutboundFrame::Event {
                    envelope: Envelope {
                        seq,
                        conversation_id: conversation_id.clone(),
                        event,
                    },
                };
                if sink.send((epoch, outbound)).await.is_err() {
                    break;
                }
            }
        });
    }

    pub(crate) async fn on_durable_committed(&self, seq: u64) -> Result<()> {
        self.pump.lock().await.on_durable_committed(seq).await
    }

    #[cfg(test)]
    pub(crate) async fn active_delivery_epoch(&self) -> Option<DeliveryEpoch> {
        self.pump.lock().await.epoch().copied()
    }

    #[cfg(test)]
    pub(crate) fn replay_page_lengths(&self) -> Vec<usize> {
        self.replay_page_lengths.lock().unwrap().clone()
    }
}

#[async_trait]
impl DurableSource for T17StoreAdapter {
    async fn event_cursor(&self) -> Result<EventCursors> {
        Ok(EventCursors {
            last_sent: current_event_head_seq(self.store.pool()).await?,
        })
    }

    async fn events_after(&self, after_seq: u64, limit: usize) -> Result<Vec<OutboundFrame>> {
        let page: Vec<_> = raw_events_after(&self.store, after_seq, limit)
            .await?
            .into_iter()
            .map(|(seq, event)| {
                Ok(OutboundFrame::Event {
                    envelope: Envelope {
                        seq: Some(seq),
                        conversation_id: self.store.scope().conversation_id.clone(),
                        event: serde_json::to_value(event)
                            .context("serialize durable T17 event for gateway")?,
                    },
                })
            })
            .collect::<Result<_>>()?;
        #[cfg(test)]
        self.replay_page_lengths.lock().unwrap().push(page.len());
        Ok(page)
    }

    async fn command_cursors(&self) -> Result<CommandCursors> {
        let row = sqlx::query(
            "SELECT COUNT(*) AS row_count,
                    COALESCE(MIN(seq), 0) AS min_seq,
                    COALESCE(MAX(seq), 0) AS max_seq,
                    MIN(CASE WHEN status NOT IN ('applied', 'superseded', 'rejected')
                             THEN seq END) AS first_nonterminal
             FROM inbound_commands",
        )
        .fetch_one(self.store.pool())
        .await
        .context("failed to read durable command cursors")?;
        let row_count: i64 = row.try_get("row_count")?;
        let min_seq: i64 = row.try_get("min_seq")?;
        let max_seq: i64 = row.try_get("max_seq")?;
        let first_nonterminal: Option<i64> = row.try_get("first_nonterminal")?;
        if row_count < 0 || min_seq < 0 || max_seq < 0 {
            bail!("stored command cursor contains a negative value");
        }
        if row_count == 0 {
            return Ok(CommandCursors::default());
        }
        if min_seq != 1 || row_count != max_seq {
            bail!(
                "stored commands do not form a complete prefix: count={row_count}, min={min_seq}, max={max_seq}"
            );
        }
        let received = u64::try_from(max_seq).context("command received cursor is negative")?;
        let applied = match first_nonterminal {
            Some(seq) if seq <= 0 => bail!("first nonterminal command seq is not positive"),
            Some(seq) => u64::try_from(seq - 1).context("command applied cursor is negative")?,
            None => received,
        };
        Ok(CommandCursors { received, applied })
    }

    async fn install_delivery_epoch(
        &self,
        epoch: DeliveryEpoch,
        _catch_up_from_seq: u64,
        sink: EventSender,
    ) -> Result<()> {
        self.start_forwarder(sink);
        self.pump.lock().await.install_epoch(epoch);
        Ok(())
    }

    async fn invalidate_delivery_epoch(&self, epoch: DeliveryEpoch) -> Result<()> {
        if !self.pump.lock().await.invalidate_epoch(epoch) {
            return Err(anyhow!(
                "T17 DeliveryPump epoch invalidation mismatch for {}",
                epoch.as_u64()
            ));
        }
        Ok(())
    }
}

/// Placeholder credential provider for T26's workload-identity integration.
#[derive(Clone, Debug)]
pub struct T26CredentialProvider;

#[async_trait]
impl CredentialProvider for T26CredentialProvider {
    async fn fresh_credential(&mut self) -> Result<GatewayCredential> {
        bail!(
            "T26 integration seam: CredentialProvider is not wired. \
             Contract: read the current short-lived agent token from the control-plane source."
        )
    }
}
