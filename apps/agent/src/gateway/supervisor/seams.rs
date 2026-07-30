//! Store/runtime adapters for the T17 and T26 integration boundaries.

use std::collections::HashMap;
use std::fmt;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use sqlx::Row;
#[cfg(test)]
use tokio::sync::Notify;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use super::post_commit::{PostCommitAdmissionTarget, PostCommitDispatcherClient};
use super::session::{DurableEventAdmission, SessionEventDelivery, SessionEventSink};
use super::{
    CommandCursors, CredentialProvider, DeliveryAuthorization, DeliveryEpoch, DeliveryEpochFailure,
    DeliveryEpochRuntime, DurableSource, EventCursors, EventSender, GatewayCredential,
    HydrationLatch, HydrationReady, OutboundFrame,
};
use crate::agent::AgentEvent;
use crate::gateway::Envelope;
use crate::runtime::contracts::ProcessGeneration;
use crate::store::{
    DeliveryChannelBuilder, DeliveryFrame, DeliveryMode, DeliveryPump, DeliveryTransportError,
    DurableDeliveryOutcome, HydrationReceiptIdentity, PostCommitEpochCapability, Store,
    current_event_head_seq, raw_events_after,
};

#[derive(Debug)]
enum DurableForwardFailure {
    Transport,
    Permanent(anyhow::Error),
}

type DurableFenceSender = oneshot::Sender<std::result::Result<(), DurableForwardFailure>>;
type DurableFences = Arc<Mutex<HashMap<(u64, u64), DurableFenceSender>>>;

struct DurableFenceRegistration {
    fences: DurableFences,
    key: (u64, u64),
}

impl Drop for DurableFenceRegistration {
    fn drop(&mut self) {
        self.fences.lock().unwrap().remove(&self.key);
    }
}

#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct DurableAdmissionHook {
    pub(crate) reserved: Arc<Notify>,
    pub(crate) allow_registration: Arc<Notify>,
    pub(crate) registered: Arc<Notify>,
    pub(crate) allow_delivery: Arc<Notify>,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "redaction-only projection for durable event seq {seq} is not a valid AgentEvent: {source}"
)]
pub(crate) struct DeliveryProjectionError {
    seq: u64,
    #[source]
    source: serde_json::Error,
}

fn parse_projected_event(seq: u64, projection: &str) -> Result<serde_json::Value> {
    let event = serde_json::from_str::<AgentEvent>(projection)
        .map_err(|source| anyhow::Error::new(DeliveryProjectionError { seq, source }))?;
    serde_json::to_value(event).context("serialize validated projected T17 delivery event")
}

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
    durable_fences: DurableFences,
    post_commit_epoch: Option<PostCommitEpochCapability>,
    post_commit_dispatcher: Option<PostCommitDispatcherClient>,
    #[cfg(test)]
    replay_page_lengths: Arc<Mutex<Vec<usize>>>,
    #[cfg(test)]
    delivery_epoch_installs: Arc<AtomicU64>,
    #[cfg(test)]
    delivery_epoch_invalidations: Arc<AtomicU64>,
    #[cfg(test)]
    durable_admission_hook: Arc<Mutex<Option<DurableAdmissionHook>>>,
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
            durable_fences: Arc::new(Mutex::new(HashMap::new())),
            post_commit_epoch: None,
            post_commit_dispatcher: None,
            #[cfg(test)]
            replay_page_lengths: Arc::new(Mutex::new(Vec::new())),
            #[cfg(test)]
            delivery_epoch_installs: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            delivery_epoch_invalidations: Arc::new(AtomicU64::new(0)),
            #[cfg(test)]
            durable_admission_hook: Arc::new(Mutex::new(None)),
        }
    }

    /// Bind the exact Store-hydrated runtime epoch before this adapter can be
    /// used as a production post-COMMIT admission target.
    pub(crate) fn bind_post_commit_epoch(&self, epoch: PostCommitEpochCapability) -> Result<Self> {
        self.store.validate_post_commit_epoch(&epoch)?;
        let authority = epoch.authority()?;
        if authority.personality_agent_id() != self.store.scope().personality_agent_id() {
            bail!(
                "post-commit runtime epoch personality-agent mismatch: expected {}, got {}",
                self.store.scope().personality_agent_id,
                authority.personality_agent_id()
            );
        }
        if self.post_commit_epoch.is_some() {
            bail!("T17 Store adapter already has a post-commit runtime epoch");
        }
        let mut bound = self.clone();
        bound.post_commit_epoch = Some(epoch);
        Ok(bound)
    }

    /// Bind the one T26 dispatcher proof client used by Session.
    ///
    /// The dispatcher target is an unbound clone of this adapter. Binding only
    /// adds the wait capability; pump/epoch state remains shared by all clones.
    pub(crate) fn bind_post_commit_dispatcher(
        &self,
        dispatcher: PostCommitDispatcherClient,
    ) -> Result<Self> {
        if dispatcher.personality_agent_id() != self.store.scope().personality_agent_id() {
            bail!(
                "post-commit dispatcher personality-agent mismatch: expected {}, got {}",
                self.store.scope().personality_agent_id,
                dispatcher.personality_agent_id()
            );
        }
        if self.post_commit_dispatcher.is_some() {
            bail!("T17 Store adapter already has a post-commit dispatcher");
        }
        match self.post_commit_epoch.as_ref() {
            Some(epoch) if epoch.same_instance(dispatcher.epoch()) => {}
            None if dispatcher.epoch().is_unbound_test() => {}
            _ => bail!(
                "post-commit dispatcher client is not bound to this T17 adapter runtime epoch"
            ),
        }
        let mut bound = self.clone();
        bound.post_commit_dispatcher = Some(dispatcher);
        Ok(bound)
    }

    fn start_forwarder(
        &self,
        mut rx: mpsc::Receiver<DeliveryFrame>,
        sink: EventSender,
        mode: DeliveryMode,
        cancel: CancellationToken,
        failure_tx: mpsc::UnboundedSender<DeliveryEpochFailure>,
    ) -> tokio::task::JoinHandle<()> {
        let personality_agent_id = self.store.scope().personality_agent_id.clone();
        let durable_fences = self.durable_fences.clone();
        tokio::spawn(async move {
            loop {
                let frame = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    frame = rx.recv() => match frame {
                        Some(frame) => frame,
                        None => {
                            if !cancel.is_cancelled() {
                                let _ = failure_tx.send(DeliveryEpochFailure::Reconnect(
                                    "delivery channel closed before epoch cancellation".to_owned()
                                ));
                            }
                            break;
                        }
                    }
                };
                let (epoch, seq, event, mut durable_fence) = match (mode, frame) {
                    (
                        DeliveryMode::Raw,
                        DeliveryFrame::Durable {
                            seq,
                            epoch,
                            raw: Some(event),
                            projection: None,
                        },
                    ) => {
                        let fence = durable_fences
                            .lock()
                            .unwrap()
                            .remove(&(epoch.as_u64(), seq));
                        (
                            epoch,
                            Some(seq),
                            serde_json::to_value(event).context("serialize raw T17 delivery event"),
                            fence,
                        )
                    }
                    (
                        DeliveryMode::RedactionOnly,
                        DeliveryFrame::Durable {
                            seq,
                            epoch,
                            raw: None,
                            projection: Some(projection),
                        },
                    ) => {
                        let fence = durable_fences
                            .lock()
                            .unwrap()
                            .remove(&(epoch.as_u64(), seq));
                        let event = parse_projected_event(seq, &projection);
                        (epoch, Some(seq), event, fence)
                    }
                    (DeliveryMode::Raw, DeliveryFrame::Volatile { epoch, event }) => (
                        epoch,
                        None,
                        serde_json::to_value(event)
                            .context("serialize volatile T17 delivery event"),
                        None,
                    ),
                    (DeliveryMode::RedactionOnly, DeliveryFrame::Volatile { .. }) => {
                        let _ = failure_tx.send(DeliveryEpochFailure::Fatal(
                            "redaction-only delivery received a volatile frame".to_owned(),
                        ));
                        break;
                    }
                    (
                        DeliveryMode::Raw | DeliveryMode::RedactionOnly,
                        DeliveryFrame::Durable { epoch, seq, .. },
                    ) => {
                        let reason = "delivery frame did not match epoch authorization".to_owned();
                        if let Some(fence) = durable_fences
                            .lock()
                            .unwrap()
                            .remove(&(epoch.as_u64(), seq))
                        {
                            let _ = fence.send(Err(DurableForwardFailure::Permanent(anyhow!(
                                reason.clone()
                            ))));
                        }
                        let _ = failure_tx.send(DeliveryEpochFailure::Fatal(reason));
                        break;
                    }
                };
                let event = match event {
                    Ok(event) => event,
                    Err(error) => {
                        let reason = format!("failed to project delivery event: {error:#}");
                        if let Some(fence) = durable_fence.take() {
                            let _ = fence.send(Err(DurableForwardFailure::Permanent(error)));
                        }
                        let _ = failure_tx.send(DeliveryEpochFailure::Fatal(reason));
                        break;
                    }
                };
                let outbound = OutboundFrame::Event {
                    envelope: Envelope {
                        seq,
                        personality_agent_id: personality_agent_id.clone(),
                        event,
                    },
                };
                let send = sink.send_from_delivery_pump((epoch, outbound));
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => {
                        if let Some(fence) = durable_fence.take() {
                            let _ = fence.send(Err(DurableForwardFailure::Transport));
                        }
                        break;
                    },
                    result = send => {
                        if result.is_err() {
                            if let Some(fence) = durable_fence.take() {
                                let _ = fence.send(Err(DurableForwardFailure::Transport));
                            }
                            let _ = failure_tx.send(DeliveryEpochFailure::Reconnect(
                                "supervisor event sink closed".to_owned()
                            ));
                            break;
                        }
                        if let Some(fence) = durable_fence.take() {
                            let _ = fence.send(Ok(()));
                        }
                    }
                }
            }
        })
    }

    /// Called only by T26's ordered post-commit dispatcher. EventWriter never
    /// awaits this bounded delivery path inside its database transaction.
    pub(super) async fn admit_ordered_commit(&self, seq: u64) -> Result<DurableEventAdmission> {
        let (fence_tx, fence_rx) = oneshot::channel();
        let (reservation, epoch, key) = {
            // Reservation and fence registration are one admission operation
            // with respect to epoch invalidation. Invalidation takes this same
            // slot lock before sweeping fences, so it cannot pass between the
            // epoch proof and ownership of the completion fence.
            let slot = self.pump.lock().await;
            let Some(pump) = slot.as_ref() else {
                return Ok(DurableEventAdmission::Deferred { after_epoch: None });
            };
            let Some(reservation) = pump.reserve_durable(seq) else {
                return Ok(DurableEventAdmission::Deferred { after_epoch: None });
            };
            let epoch = reservation.epoch();

            #[cfg(test)]
            let hook = self.durable_admission_hook.lock().unwrap().clone();
            #[cfg(test)]
            if let Some(hook) = hook.as_ref() {
                hook.reserved.notify_one();
                hook.allow_registration.notified().await;
            }

            let key = (epoch.as_u64(), seq);
            {
                use std::collections::hash_map::Entry;
                match self.durable_fences.lock().unwrap().entry(key) {
                    Entry::Vacant(entry) => {
                        entry.insert(fence_tx);
                    }
                    Entry::Occupied(_) => {
                        bail!(
                            "duplicate durable delivery fence for epoch {} seq {seq}",
                            epoch.as_u64()
                        );
                    }
                }
            }
            (reservation, epoch, key)
        };
        // Dropping the dispatcher admission future (for example during T26
        // shutdown) removes the completion sender. The DeliveryPump
        // reservation has its own RAII pending-count guard, so cancellation
        // cannot strand an unowned fence or wedge volatile delivery.
        let _fence_registration = DurableFenceRegistration {
            fences: self.durable_fences.clone(),
            key,
        };

        #[cfg(test)]
        let hook = self.durable_admission_hook.lock().unwrap().clone();
        #[cfg(test)]
        if let Some(hook) = hook.as_ref() {
            hook.registered.notify_one();
            hook.allow_delivery.notified().await;
        }

        match reservation.deliver().await {
            Ok(DurableDeliveryOutcome::Enqueued) => {}
            Ok(DurableDeliveryOutcome::EpochLost) => {
                return Ok(DurableEventAdmission::Deferred {
                    after_epoch: Some(epoch),
                });
            }
            Err(error) => {
                if error.is::<DeliveryTransportError>() {
                    return Ok(DurableEventAdmission::Deferred {
                        after_epoch: Some(epoch),
                    });
                }
                return Err(error);
            }
        }
        match fence_rx.await {
            Ok(Ok(())) => Ok(DurableEventAdmission::Enqueued { epoch }),
            Ok(Err(DurableForwardFailure::Transport)) | Err(_) => {
                Ok(DurableEventAdmission::Deferred {
                    after_epoch: Some(epoch),
                })
            }
            Ok(Err(DurableForwardFailure::Permanent(error))) => Err(error),
        }
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

    #[cfg(test)]
    pub(crate) fn set_durable_admission_hook(&self, hook: Option<DurableAdmissionHook>) {
        *self.durable_admission_hook.lock().unwrap() = hook;
    }

    #[cfg(test)]
    pub(crate) fn durable_fence_count(&self) -> usize {
        self.durable_fences.lock().unwrap().len()
    }
}

#[async_trait]
impl SessionEventDelivery for T17StoreAdapter {
    async fn on_durable_committed(
        &self,
        personality_agent_id: &crate::runtime::contracts::PersonalityAgentId,
        seq: u64,
    ) -> Result<DurableEventAdmission> {
        if personality_agent_id != &self.store.scope().personality_agent_id {
            bail!(
                "Session durable event personality-agent mismatch: expected {}, got {personality_agent_id}",
                self.store.scope().personality_agent_id
            );
        }
        self.post_commit_dispatcher
            .as_ref()
            .context("Session durable event reached T17 without T26's ordered dispatcher")?
            .admission_for(personality_agent_id, seq)
            .await
    }

    async fn on_volatile(
        &self,
        personality_agent_id: &crate::runtime::contracts::PersonalityAgentId,
        event: crate::agent::AgentEvent,
    ) -> Result<()> {
        if personality_agent_id != &self.store.scope().personality_agent_id {
            bail!(
                "Session volatile event personality-agent mismatch: expected {}, got {personality_agent_id}",
                self.store.scope().personality_agent_id
            );
        }
        match T17StoreAdapter::on_volatile(self, event).await {
            Err(error) if error.is::<DeliveryTransportError>() => {
                // Volatile transport output has no replay obligation. T24
                // reconnects the epoch; Session continues.
                Ok(())
            }
            result => result,
        }
    }
}

#[async_trait]
impl PostCommitAdmissionTarget for T17StoreAdapter {
    fn bind_post_commit_epoch(&self, epoch: &PostCommitEpochCapability) -> Result<()> {
        match self.post_commit_epoch.as_ref() {
            Some(bound) if bound.same_instance(epoch) => bound.ensure_active(),
            None if epoch.is_unbound_test() => Ok(()),
            _ => bail!("T17 admission target is not bound to the dispatcher runtime epoch"),
        }
    }

    async fn admit_committed(
        &self,
        epoch: &PostCommitEpochCapability,
        personality_agent_id: &crate::runtime::contracts::PersonalityAgentId,
        seq: u64,
    ) -> Result<DurableEventAdmission> {
        <Self as PostCommitAdmissionTarget>::bind_post_commit_epoch(self, epoch)?;
        if personality_agent_id != &self.store.scope().personality_agent_id {
            bail!(
                "post-commit dispatcher targets personality agent {personality_agent_id}, but T17 owns {}",
                self.store.scope().personality_agent_id,
            );
        }
        self.admit_ordered_commit(seq).await
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

    fn session_event_sink(&self) -> Option<SessionEventSink> {
        self.post_commit_dispatcher
            .as_ref()
            .map(|_| SessionEventSink::new(self.clone()))
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
                            personality_agent_id: self.store.scope().personality_agent_id.clone(),
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
                                personality_agent_id: self
                                    .store
                                    .scope()
                                    .personality_agent_id
                                    .clone(),
                                event: parse_projected_event(seq, &projection)?,
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

        // Keep the lock order identical to durable admission:
        // pump slot -> pump state -> fence map. Once this slot lock is held no
        // callback can prove this epoch and register a new fence behind the
        // invalidation sweep.
        let mut slot = self.pump.lock().await;
        let mismatch = match slot.as_mut() {
            None => false,
            Some(pump) if pump.epoch().is_none() => {
                *slot = None;
                false
            }
            Some(pump) if pump.invalidate_epoch(epoch) => {
                *slot = None;
                false
            }
            Some(_) => true,
        };
        let stale_fences = {
            let mut fences = self.durable_fences.lock().unwrap();
            let keys: Vec<_> = fences
                .keys()
                .copied()
                .filter(|(fence_epoch, _)| *fence_epoch == epoch.as_u64())
                .collect();
            keys.into_iter()
                .filter_map(|key| fences.remove(&key))
                .collect::<Vec<_>>()
        };
        drop(slot);
        for fence in stale_fences {
            let _ = fence.send(Err(DurableForwardFailure::Transport));
        }
        if mismatch {
            return Err(anyhow!(
                "T17 DeliveryPump epoch invalidation mismatch for {}",
                epoch.as_u64()
            ));
        }
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
