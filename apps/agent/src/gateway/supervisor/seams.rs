//! Store/runtime adapters for the T17 and T26 integration boundaries.

use std::fmt;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use sqlx::Row;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::{
    CommandCursors, CredentialProvider, DeliveryAuthorization, DeliveryEpoch, DeliveryEpochRuntime,
    DurableSource, EventCursors, EventSender, GatewayCredential, HydrationLatch, HydrationReady,
    OutboundFrame,
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
    authorization: Option<DeliveryAuthorization>,
    pump: Arc<tokio::sync::Mutex<Option<DeliveryPump>>>,
    #[cfg(test)]
    replay_page_lengths: Arc<Mutex<Vec<usize>>>,
    #[cfg(test)]
    delivery_epoch_installs: Arc<AtomicU64>,
    #[cfg(test)]
    delivery_epoch_invalidations: Arc<AtomicU64>,
}

impl fmt::Debug for T17StoreAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("T17StoreAdapter").finish_non_exhaustive()
    }
}

impl T17StoreAdapter {
    /// Delivery authorization is deliberately absent here. The supervisor
    /// binds it from each authenticated credential before replay or install.
    pub(crate) fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            authorization: None,
            pump: Arc::new(tokio::sync::Mutex::new(None)),
            #[cfg(test)]
            replay_page_lengths: Arc::new(Mutex::new(Vec::new())),
            #[cfg(test)]
            delivery_epoch_installs: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            delivery_epoch_invalidations: Arc::new(AtomicU64::new(0)),
        }
    }

    fn start_forwarder(
        &self,
        mut rx: mpsc::Receiver<DeliveryFrame>,
        sink: EventSender,
        mode: DeliveryMode,
        cancel: CancellationToken,
        failure_tx: mpsc::UnboundedSender<String>,
    ) -> tokio::task::JoinHandle<()> {
        let conversation_id = self.store.scope().conversation_id.clone();
        tokio::spawn(async move {
            loop {
                let frame = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    frame = rx.recv() => match frame {
                        Some(frame) => frame,
                        None => {
                            if !cancel.is_cancelled() {
                                let _ = failure_tx.send(
                                    "delivery channel closed before epoch cancellation".to_owned()
                                );
                            }
                            break;
                        }
                    }
                };
                let (epoch, seq, event) = match (mode, frame) {
                    (
                        DeliveryMode::Raw,
                        DeliveryFrame::Durable {
                            seq,
                            epoch,
                            raw: Some(event),
                            projection: None,
                        },
                    ) => (
                        epoch,
                        Some(seq),
                        serde_json::to_value(event).context("serialize raw T17 delivery event"),
                    ),
                    (
                        DeliveryMode::RedactionOnly,
                        DeliveryFrame::Durable {
                            seq,
                            epoch,
                            raw: None,
                            projection: Some(projection),
                        },
                    ) => (
                        epoch,
                        Some(seq),
                        serde_json::from_str(&projection)
                            .context("parse projected T17 delivery event"),
                    ),
                    (DeliveryMode::Raw, DeliveryFrame::Volatile { epoch, event }) => (
                        epoch,
                        None,
                        serde_json::to_value(event)
                            .context("serialize volatile T17 delivery event"),
                    ),
                    (DeliveryMode::RedactionOnly, DeliveryFrame::Volatile { .. }) => {
                        let _ = failure_tx
                            .send("redaction-only delivery received a volatile frame".to_owned());
                        break;
                    }
                    (DeliveryMode::Raw, DeliveryFrame::Durable { .. })
                    | (DeliveryMode::RedactionOnly, DeliveryFrame::Durable { .. }) => {
                        let _ = failure_tx
                            .send("delivery frame did not match epoch authorization".to_owned());
                        break;
                    }
                };
                let event = match event {
                    Ok(event) => event,
                    Err(error) => {
                        let _ =
                            failure_tx.send(format!("failed to project delivery event: {error:#}"));
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
                let send = sink.send((epoch, outbound));
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    result = send => {
                        if result.is_err() {
                            let _ = failure_tx.send("supervisor event sink closed".to_owned());
                            break;
                        }
                    }
                }
            }
        })
    }

    /// Called by T26's ordered post-commit pump task. EventWriter must not
    /// await this bounded delivery path inside its database transaction.
    pub(crate) async fn on_durable_committed(&self, seq: u64) -> Result<()> {
        let pump = self.pump.lock().await.as_ref().cloned();
        let Some(pump) = pump else {
            return Ok(());
        };
        pump.on_durable_committed(seq).await
    }

    /// Deliver an Online-only delta through the same pump/FIFO as durable
    /// notifications. Redaction-only authorization suppresses it in the pump.
    pub(crate) async fn on_volatile(&self, event: crate::agent::AgentEvent) -> Result<()> {
        let pump = self.pump.lock().await.as_ref().cloned();
        let Some(pump) = pump else {
            return Ok(());
        };
        pump.on_volatile(event).await
    }

    #[cfg(test)]
    pub(crate) async fn active_delivery_epoch(&self) -> Option<DeliveryEpoch> {
        self.pump
            .lock()
            .await
            .as_ref()
            .and_then(DeliveryPump::epoch)
    }

    #[cfg(test)]
    pub(crate) fn replay_page_lengths(&self) -> Vec<usize> {
        self.replay_page_lengths.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub(crate) fn delivery_epoch_lifecycle_counts(&self) -> (u64, u64) {
        (
            self.delivery_epoch_installs.load(Ordering::SeqCst),
            self.delivery_epoch_invalidations.load(Ordering::SeqCst),
        )
    }
}

async fn projected_events_after(
    store: &Store,
    after_seq: u64,
    limit: usize,
) -> Result<Vec<(u64, String)>> {
    if limit == 0 {
        bail!("delivery event page size must be positive");
    }
    let rows = sqlx::query(
        "SELECT seq, envelope
         FROM agent_events
         WHERE seq > ?
         ORDER BY seq
         LIMIT ?",
    )
    .bind(i64::try_from(after_seq).context("after_seq exceeds SQLite INTEGER range")?)
    .bind(i64::try_from(limit).context("event page size exceeds SQLite INTEGER range")?)
    .fetch_all(store.pool())
    .await
    .context("failed to fetch projected durable event page")?;

    rows.into_iter()
        .map(|row| {
            let seq: i64 = row.try_get("seq")?;
            let seq = u64::try_from(seq).context("stored event seq is negative")?;
            let projection: String = row.try_get("envelope")?;
            Ok((seq, projection))
        })
        .collect()
}

#[async_trait]
impl DurableSource for T17StoreAdapter {
    fn bind_delivery_authorization(&self, authorization: DeliveryAuthorization) -> Result<Self> {
        let mut bound = self.clone();
        bound.authorization = Some(authorization);
        Ok(bound)
    }

    async fn event_cursor(&self) -> Result<EventCursors> {
        Ok(EventCursors {
            last_sent: current_event_head_seq(self.store.pool()).await?,
        })
    }

    async fn events_after(&self, after_seq: u64, limit: usize) -> Result<Vec<OutboundFrame>> {
        let authorization = self
            .authorization
            .context("delivery source used before authenticated authorization was bound")?;
        let page: Vec<_> = match authorization {
            DeliveryAuthorization::Raw => raw_events_after(&self.store, after_seq, limit)
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
                .collect::<Result<_>>()?,
            DeliveryAuthorization::RedactionOnly => {
                projected_events_after(&self.store, after_seq, limit)
                    .await?
                    .into_iter()
                    .map(|(seq, projection)| {
                        Ok(OutboundFrame::Event {
                            envelope: Envelope {
                                seq: Some(seq),
                                conversation_id: self.store.scope().conversation_id.clone(),
                                event: serde_json::from_str(&projection)
                                    .context("parse projected durable T17 event for gateway")?,
                            },
                        })
                    })
                    .collect::<Result<_>>()?
            }
        };
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
        cancel: CancellationToken,
    ) -> Result<Option<DeliveryEpochRuntime>> {
        #[cfg(test)]
        self.delivery_epoch_installs.fetch_add(1, Ordering::SeqCst);
        let authorization = self
            .authorization
            .context("delivery epoch installed before authenticated authorization was bound")?;
        let mode = match authorization {
            DeliveryAuthorization::Raw => DeliveryMode::Raw,
            DeliveryAuthorization::RedactionOnly => DeliveryMode::RedactionOnly,
        };
        let (channel, delivery_rx) = DeliveryChannelBuilder::with_mode(mode).build();
        let (failure_tx, failure_rx) = mpsc::unbounded_channel();
        let pump = DeliveryPump::new(self.store.clone(), channel);
        pump.install_supervised_epoch(epoch, failure_tx.clone());
        let mut slot = self.pump.lock().await;
        if let Some(current) = slot.as_ref()
            && current.epoch().is_some()
        {
            bail!("delivery epoch installed while another epoch is active");
        }
        *slot = Some(pump);
        drop(slot);
        let task = self.start_forwarder(delivery_rx, sink, mode, cancel, failure_tx);
        Ok(Some(DeliveryEpochRuntime::new(failure_rx, task)))
    }

    async fn invalidate_delivery_epoch(&self, epoch: DeliveryEpoch) -> Result<()> {
        #[cfg(test)]
        self.delivery_epoch_invalidations
            .fetch_add(1, Ordering::SeqCst);
        let mut slot = self.pump.lock().await;
        let Some(pump) = slot.as_mut() else {
            return Ok(());
        };
        if pump.epoch().is_none() {
            *slot = None;
            return Ok(());
        }
        if !pump.invalidate_epoch(epoch) {
            return Err(anyhow!(
                "T17 DeliveryPump epoch invalidation mismatch for {}",
                epoch.as_u64()
            ));
        }
        *slot = None;
        Ok(())
    }

    async fn mark_delivery_online(&self, epoch: DeliveryEpoch) -> Result<()> {
        let pump = self
            .pump
            .lock()
            .await
            .as_ref()
            .cloned()
            .context("delivery online barrier has no active pump")?;
        pump.mark_online(epoch)
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
             Contract: read the current short-lived agent token and its typed delivery \
             authorization from the same authenticated control-plane credential."
        )
    }
}
