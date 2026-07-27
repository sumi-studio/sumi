//! Agent session orchestration and turn lifecycle.
#![allow(
    dead_code,
    reason = "the Session actor is intentionally left unwired until T26 production composition"
)]

use std::{
    any::Any,
    future::Future,
    marker::PhantomData,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{
    gateway::{
        Command, CommandAckStatus, CommandEnvelope, Gateway, GatewayClosed, GatewayReader,
        GatewayWriter, InboundCommand, OutboundFrame,
    },
    provider::{
        overflow::OverflowSource,
        types::{ContextMessage, PublicMessage, StopReason},
    },
    runtime::contracts::{HydrationReady, ProcessGeneration},
    store::{
        ApplicationKind, DataKeyPurpose, EventWriter, InboundAdmission, InboundReceiptOrigin,
        RecoveryRequired as AdmissionRecoveryRequired, RecoveryStep, Store, SuffixRecovery,
    },
};

mod driver;
mod durable_bridge;
mod events;
mod provider_projection;
mod queue;
mod run;
mod steer;

pub(crate) use durable_bridge::DurableRunBinding;

use durable_bridge::{
    CommittedOutput, DurableBridge, MessageCommitBarrier, MessageCommitReceipt,
    RetryWaitCommitBarrier, RunOutput, ToolStartCommitBarrier,
};
use queue::MessageQueue;

#[allow(
    unused_imports,
    reason = "T26 constructs the injected production runtime"
)]
pub(crate) use driver::{InjectedRunDriver, RunTimingSample, RunTimingSamples};
pub(crate) use events::{
    AgentEvent, ApprovalRequest, ApprovalResolution, AuditDecision, AuditOutcome, MemoryMaintKind,
    PublicStreamEvent, ReviewProjection, RiskLevel, SteerMode, UserAuthorization,
};
#[allow(unused_imports, reason = "consumed by the later T15 Session run loop")]
pub(crate) use provider_projection::{
    ProjectedProviderEvent, ProviderEventProjector, ProviderTerminal, ProviderTerminalKind,
};
#[allow(
    unused_imports,
    reason = "production provider/executor wiring lands in T26 composition; T15 consumes injected boundaries"
)]
pub(crate) use run::{
    OverflowRecoveryOutcome, OverflowRecoveryRequest, ProviderAttempt, RunDriver,
    SequentialRunWorker,
};
#[allow(
    unused_imports,
    reason = "T16 classification and group injection consumed by Session and DurableBridge"
)]
pub(crate) use steer::{
    AttemptCancellation, AttemptGuard, AttemptReservation, SteerGroup, SteerGroupSnapshot,
    SteerStage, bound_steer_group, hard_steer_step_zero_batch, steer_group_injection_batch,
};

const CONTROL_CHANNEL_CAPACITY: usize = 32;
const EVENT_CHANNEL_CAPACITY: usize = 64;
const OUTBOUND_CHANNEL_CAPACITY: usize = 64;
const VOLATILE_OUTBOUND_BUDGET: usize = 32;
/// Bounds the in-process Session↔worker retry-steer authorization rendezvous.
/// This is not provider retry backoff; it only prevents one stalled local task
/// from blocking the Session event lane indefinitely.
const RETRY_STEER_HANDSHAKE_TIMEOUT: Duration = Duration::from_millis(250);
/// Bounds the Session↔worker hard/soft-steer and abort authorization rendezvous.
/// This is not provider retry backoff; it only prevents one stalled local task
/// from blocking the Session event lane indefinitely.
const STEER_HANDSHAKE_TIMEOUT: Duration = Duration::from_millis(250);
/// Bounds the generation-bound hydration latch so a lost publisher cannot
/// leave Session startup pending forever.
const HYDRATION_READY_TIMEOUT: Duration = Duration::from_secs(30);
/// API admission permits 32 ordinary commands plus one reserved Abort.
const PENDING_ORDINARY_CONTROL_CAPACITY: usize = 32;
const PENDING_CONTROL_CAPACITY: usize = PENDING_ORDINARY_CONTROL_CAPACITY + 1;

type WorkerFuture = Pin<Box<dyn Future<Output = RunCompletion> + Send + 'static>>;

struct OutboundItem {
    frames: Vec<OutboundFrame>,
    volatile: bool,
}

#[derive(Clone)]
struct OutboundHandle {
    tx: mpsc::Sender<OutboundItem>,
    volatile_in_flight: Arc<AtomicUsize>,
    progress: Arc<OutboundProgress>,
}

#[derive(Default)]
struct OutboundProgress {
    enqueued: AtomicUsize,
    completed: AtomicUsize,
    completed_notify: tokio::sync::Notify,
}

impl OutboundHandle {
    fn enqueue_reliable(&self, frames: Vec<OutboundFrame>) -> Result<(), SessionFailure> {
        self.tx
            .try_send(OutboundItem {
                frames,
                volatile: false,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => SessionFailure::OutboundFull,
                mpsc::error::TrySendError::Closed(_) => SessionFailure::OutboundClosed,
            })?;
        self.progress.enqueued.fetch_add(1, Ordering::Release);
        Ok(())
    }

    fn enqueue_volatile(&self, frame: OutboundFrame) {
        if self
            .volatile_in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < VOLATILE_OUTBOUND_BUDGET).then_some(current + 1)
            })
            .is_err()
        {
            return;
        }
        if self
            .tx
            .try_send(OutboundItem {
                frames: vec![frame],
                volatile: true,
            })
            .is_err()
        {
            self.volatile_in_flight.fetch_sub(1, Ordering::AcqRel);
        } else {
            self.progress.enqueued.fetch_add(1, Ordering::Release);
        }
    }
}

async fn own_gateway_writer<W: GatewayWriter>(
    mut writer: W,
    mut outbound: mpsc::Receiver<OutboundItem>,
    volatile_in_flight: Arc<AtomicUsize>,
    progress: Arc<OutboundProgress>,
) -> Result<()> {
    while let Some(item) = outbound.recv().await {
        let volatile = item.volatile;
        for frame in item.frames {
            if let Err(error) = writer.send(frame).await {
                if volatile {
                    volatile_in_flight.fetch_sub(1, Ordering::AcqRel);
                }
                return Err(error);
            }
        }
        if volatile {
            volatile_in_flight.fetch_sub(1, Ordering::AcqRel);
        }
        progress.completed.fetch_add(1, Ordering::Release);
        progress.completed_notify.notify_waiters();
    }
    Ok(())
}

/// The sole mutable conversation value transferred into and out of a worker.
/// It is intentionally neither `Clone` nor wrapped in shared mutability.
pub(crate) struct RunCore {
    ownership_id: Uuid,
    mutation_epoch: u64,
    pending_controls: MessageQueue<AdmittedCommand>,
    pending_overflow_apply: Option<OverflowSource>,
    /// In-memory persisted send context returned with the unique core. T21
    /// defines the `ThreeLayerMemory` replacement, and T26 composes it into
    /// production; keeping this injected representation in `RunCore` prevents
    /// a second Session run from silently losing the first run.
    runtime_context: Vec<ContextMessage>,
    /// Hydrated recovery steps supplied by T17. When present, the Session
    /// consumes them directly instead of recomputing a T12-safe prefix.
    recovery_steps: Option<Vec<RecoveryStep>>,
    /// Generation-bound hydration latch. The Session waits here before it
    /// admits any command, and rejects a stale-generation Ready.
    hydration: Option<watch::Receiver<HydrationReady>>,
    durable_binding: Option<DurableRunBinding>,
    worker_phase: Option<watch::Sender<WorkerPhase>>,
    /// Shared cancellation registry for the one live provider attempt. The
    /// Session reserves the token around `bind_hard_steer` so the provider is
    /// only cancelled after the durable step-zero commit succeeds.
    attempt_cancellation: Option<Arc<AttemptCancellation>>,
}

impl RunCore {
    pub(crate) fn new() -> Self {
        Self {
            ownership_id: Uuid::now_v7(),
            mutation_epoch: 0,
            pending_controls: MessageQueue::bounded(PENDING_CONTROL_CAPACITY),
            pending_overflow_apply: None,
            runtime_context: Vec::new(),
            recovery_steps: None,
            hydration: None,
            durable_binding: None,
            worker_phase: None,
            attempt_cancellation: None,
        }
    }

    pub(crate) fn with_runtime_context(mut self, context: Vec<ContextMessage>) -> Self {
        self.runtime_context = context;
        self
    }

    pub(crate) fn with_recovery_steps(mut self, steps: Vec<RecoveryStep>) -> Self {
        self.recovery_steps = Some(steps);
        self
    }

    pub(crate) fn with_hydration(mut self, hydration: watch::Receiver<HydrationReady>) -> Self {
        self.hydration = Some(hydration);
        self
    }

    pub(crate) fn ownership_id(&self) -> Uuid {
        self.ownership_id
    }

    pub(crate) fn mutation_epoch(&self) -> u64 {
        self.mutation_epoch
    }

    pub(crate) fn mark_mutated(&mut self) {
        self.mutation_epoch = self.mutation_epoch.saturating_add(1);
    }

    pub(crate) fn queue_followup(&mut self, command: AdmittedCommand) -> Result<()> {
        self.pending_controls.push(command)?;
        Ok(())
    }

    pub(crate) fn next_followup(&mut self) -> Option<AdmittedCommand> {
        self.pending_controls.pop_one()
    }

    pub(crate) fn requeue_followup_front(&mut self, command: AdmittedCommand) -> Result<()> {
        self.pending_controls.push_front(command)?;
        Ok(())
    }

    pub(crate) fn defer_overflow_apply(&mut self, source: OverflowSource) {
        self.pending_overflow_apply.get_or_insert(source);
    }

    pub(crate) fn pending_overflow_apply(&self) -> Option<OverflowSource> {
        self.pending_overflow_apply
    }
}

impl std::fmt::Debug for RunCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunCore")
            .field("ownership_id", &self.ownership_id)
            .field("mutation_epoch", &self.mutation_epoch)
            .field("pending_controls", &self.pending_controls)
            .field("pending_overflow_apply", &self.pending_overflow_apply)
            .field("runtime_context_len", &self.runtime_context.len())
            .field("recovery_steps", &self.recovery_steps.is_some())
            .field("hydration", &self.hydration.is_some())
            .field("durable_binding", &self.durable_binding)
            .field("worker_phase", &self.worker_phase.is_some())
            .field("attempt_cancellation", &self.attempt_cancellation.is_some())
            .finish()
    }
}

impl Default for RunCore {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AdmittedCommand {
    envelope: CommandEnvelope,
    received_at: DateTime<Utc>,
    received_monotonic: Option<Instant>,
}

impl AdmittedCommand {
    pub(crate) fn new(envelope: CommandEnvelope, received_at: DateTime<Utc>) -> Self {
        Self {
            envelope,
            received_at,
            received_monotonic: None,
        }
    }

    pub(crate) fn live(
        envelope: CommandEnvelope,
        received_at: DateTime<Utc>,
        received_monotonic: Instant,
    ) -> Self {
        Self {
            envelope,
            received_at,
            received_monotonic: Some(received_monotonic),
        }
    }

    pub(crate) fn envelope(&self) -> &CommandEnvelope {
        &self.envelope
    }

    pub(crate) fn received_at(&self) -> DateTime<Utc> {
        self.received_at
    }

    pub(crate) fn received_monotonic(&self) -> Option<Instant> {
        self.received_monotonic
    }
}

pub(crate) enum RunControl {
    Command(AdmittedCommand),
    HardSteer {
        command: AdmittedCommand,
        accepted: oneshot::Sender<bool>,
    },
    SoftSteer {
        command: AdmittedCommand,
        accepted: oneshot::Sender<bool>,
        committed: oneshot::Receiver<()>,
    },
    Abort {
        command: AdmittedCommand,
        accepted: oneshot::Sender<bool>,
        committed: oneshot::Receiver<()>,
    },
    RetrySteer {
        command: AdmittedCommand,
        accepted: oneshot::Sender<bool>,
        committed: oneshot::Receiver<()>,
    },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum WorkerPhase {
    #[default]
    Active,
    RetryWait,
}

pub(crate) enum RunCompletion {
    Completed(RunCore),
    Failed {
        core: RunCore,
        failure: WorkerFailure,
    },
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum WorkerFailure {
    #[error("run worker was cancelled")]
    Cancelled,
    #[error("run worker event channel closed")]
    EventChannelClosed,
    #[error("run worker failed: {0}")]
    Error(String),
}

pub(crate) trait RunWorker: Send + Sync + 'static {
    fn validate_executor_generation(&self, generation: ProcessGeneration) -> Result<()>;

    fn run(
        &self,
        core: RunCore,
        initial: AdmittedCommand,
        controls: mpsc::Receiver<RunControl>,
        events: mpsc::Sender<RunOutput>,
    ) -> WorkerFuture;
}

#[cfg(test)]
impl<F, Fut> RunWorker for F
where
    F: Fn(RunCore, AdmittedCommand, mpsc::Receiver<RunControl>, mpsc::Sender<AgentEvent>) -> Fut
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = RunCompletion> + Send + 'static,
{
    fn validate_executor_generation(&self, _generation: ProcessGeneration) -> Result<()> {
        // Test-only closure workers explicitly opt into the Session identity;
        // production workers delegate to their injected driver below.
        Ok(())
    }

    fn run(
        &self,
        core: RunCore,
        initial: AdmittedCommand,
        controls: mpsc::Receiver<RunControl>,
        events: mpsc::Sender<RunOutput>,
    ) -> WorkerFuture {
        let mut binding = core
            .durable_binding
            .clone()
            .expect("Session must bind RunCore before starting a worker");
        let mut emitted_turn = false;
        let (fixture_tx, mut fixture_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let future = (self)(core, initial, controls, fixture_tx);
        Box::pin(async move {
            tokio::pin!(future);
            loop {
                tokio::select! {
                    biased;
                    completion = &mut future => {
                        while let Ok(event) = fixture_rx.try_recv() {
                            if matches!(event, AgentEvent::TurnStart) {
                                if emitted_turn {
                                    binding.turn_id = Uuid::now_v7().to_string();
                                }
                                emitted_turn = true;
                            }
                            let commit_barrier = matches!(event, AgentEvent::ToolExecutionStart { .. })
                                .then(|| ToolStartCommitBarrier::channel().0);
                            if events.send(RunOutput::detached(binding.clone(), event, commit_barrier)).await.is_err() {
                                return event_channel_lost(completion);
                            }
                        }
                        return completion;
                    }
                    event = fixture_rx.recv() => {
                        let Some(event) = event else {
                            drop(events);
                            return future.await;
                        };
                        if matches!(event, AgentEvent::TurnStart) {
                            if emitted_turn {
                                binding.turn_id = Uuid::now_v7().to_string();
                            }
                            emitted_turn = true;
                        }
                        let commit_barrier = matches!(event, AgentEvent::ToolExecutionStart { .. })
                            .then(|| ToolStartCommitBarrier::channel().0);
                        if events.send(RunOutput::detached(binding.clone(), event, commit_barrier)).await.is_err() {
                            // The fixture future owns the real RunCore. Keep
                            // draining its bounded event lane while it settles;
                            // simply awaiting it here can deadlock once that
                            // lane fills, while fabricating a replacement core
                            // lies about ownership.
                            loop {
                                tokio::select! {
                                    completion = &mut future => {
                                        return event_channel_lost(completion);
                                    }
                                    event = fixture_rx.recv() => {
                                        if event.is_none() {
                                            return event_channel_lost(future.await);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
fn event_channel_lost(completion: RunCompletion) -> RunCompletion {
    let core = match completion {
        RunCompletion::Completed(core) | RunCompletion::Failed { core, .. } => core,
    };
    RunCompletion::Failed {
        core,
        failure: WorkerFailure::EventChannelClosed,
    }
}

pub(crate) struct ActiveRun {
    control_tx: mpsc::Sender<RunControl>,
    phase_rx: watch::Receiver<WorkerPhase>,
    events_rx: mpsc::Receiver<RunOutput>,
    completion_rx: oneshot::Receiver<RunCompletion>,
    join: JoinHandle<()>,
    bridge: DurableBridge,
    attempt_cancellation: Arc<AttemptCancellation>,
}

impl Drop for ActiveRun {
    fn drop(&mut self) {
        self.join.abort();
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum RunOwnership {
    Recovered(RunCore),
    Lost,
}

#[derive(Debug)]
pub(crate) enum SessionResult {
    Completed(RunCore),
    Failed {
        failure: SessionFailure,
        ownership: RunOwnership,
    },
}

#[derive(Debug, Error)]
pub(crate) enum SessionFailure {
    #[error("session startup is recovery-gated by T17-owned suffix: {steps:?}")]
    RecoveryRequired { steps: Vec<RecoveryStep> },
    #[error("run worker failed: {0}")]
    Worker(WorkerFailure),
    #[error("run worker panicked: {message}")]
    WorkerPanicked { message: String },
    #[error("run worker completion channel closed")]
    CompletionChannelClosed,
    #[error("run worker event channel closed")]
    EventChannelClosed,
    #[error("gateway closed while a run owned RunCore")]
    GatewayClosedDuringRun,
    #[error("gateway {operation} failed: {source}")]
    Gateway {
        operation: &'static str,
        #[source]
        source: anyhow::Error,
    },
    #[error("bounded outbound queue is full after durable state was committed")]
    OutboundFull,
    #[error("bounded outbound queue is closed after durable state was committed")]
    OutboundClosed,
    #[error("received a control command while idle; command remains durably received")]
    IdleControl,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Gateway/control-plane owner. `EventWriter` and `InboundAdmission` never
/// leave this value; workers receive only already-admitted typed commands.
pub(crate) struct Session<G: Gateway> {
    gateway_reader: G::Reader,
    outbound: Option<OutboundHandle>,
    writer_done: oneshot::Receiver<Result<()>>,
    writer_join: Option<JoinHandle<()>>,
    gateway_type: PhantomData<G>,
    conversation_id: String,
    writer: EventWriter,
    admission: InboundAdmission,
    recovery_steps: Vec<RecoveryStep>,
    hydration: Option<watch::Receiver<HydrationReady>>,
    core: Option<RunCore>,
    active: Option<ActiveRun>,
    worker: Arc<dyn RunWorker>,
    executor_generation: ProcessGeneration,
    /// T15 already applies the idle/post-run Abort cutoff and supplies the
    /// injected cancellation and phase-observation seams. Commands received
    /// during an active run otherwise remain durably `received` in this
    /// sequence-ordered queue. T16 owns active/live classification, cutoff,
    /// and the full control semantics without allowing a user to overtake an
    /// earlier/later control.
    deferred_commands: MessageQueue<AdmittedCommand>,
    /// A bridge/Store refusal means the worker's returned core may be ahead of
    /// the durable transcript and must never be exposed as recovered.
    durable_core_invalidated: bool,
}

impl<G: Gateway + 'static> Session<G> {
    pub(crate) async fn start(
        store: Store,
        gateway: G,
        core: RunCore,
        worker: Arc<dyn RunWorker>,
        executor_generation: ProcessGeneration,
    ) -> Result<Self> {
        let mut session = Self::prepare(store, gateway, core, worker, executor_generation).await?;
        session.await_hydration_ready().await?;
        Ok(session)
    }

    /// Install every fallible Store, command-gate, and Gateway component while
    /// hydration is still NotReady. Production bootstrap uses this boundary so
    /// durable Ready cannot be published before the Session is actually built.
    pub(crate) async fn prepare(
        store: Store,
        gateway: G,
        core: RunCore,
        worker: Arc<dyn RunWorker>,
        executor_generation: ProcessGeneration,
    ) -> Result<Self> {
        worker.validate_executor_generation(executor_generation)?;

        let mut core = core;
        let hydration = core.hydration.take();

        let conversation_id = store.scope().conversation_id.clone();
        let store = Arc::new(store);
        for purpose in [
            DataKeyPurpose::Command,
            DataKeyPurpose::Event,
            DataKeyPurpose::Transcript,
        ] {
            store.conversation_key(purpose).await?;
        }
        let writer = EventWriter::new(store.clone());
        writer.initialize_recovery_checkpoint().await?;
        let recovery_steps = match core.recovery_steps.take() {
            Some(steps) => steps,
            None => SuffixRecovery::recover_t12_prefix(&store, &writer).await?,
        };
        let admission = InboundAdmission::after_t12_recovery(!recovery_steps.is_empty());
        let (gateway_reader, gateway_writer) = gateway.split();
        let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);
        let volatile_in_flight = Arc::new(AtomicUsize::new(0));
        let outbound_progress = Arc::new(OutboundProgress::default());
        let (writer_done_tx, writer_done) = oneshot::channel();
        let writer_counters = volatile_in_flight.clone();
        let writer_progress = outbound_progress.clone();
        let writer_join = tokio::spawn(async move {
            let result = own_gateway_writer(
                gateway_writer,
                outbound_rx,
                writer_counters,
                writer_progress,
            )
            .await;
            let _ = writer_done_tx.send(result);
        });
        Ok(Self {
            gateway_reader,
            outbound: Some(OutboundHandle {
                tx: outbound_tx,
                volatile_in_flight,
                progress: outbound_progress,
            }),
            writer_done,
            writer_join: Some(writer_join),
            gateway_type: PhantomData,
            conversation_id,
            writer,
            admission,
            recovery_steps,
            hydration,
            core: Some(core),
            active: None,
            worker,
            executor_generation,
            deferred_commands: MessageQueue::bounded(PENDING_CONTROL_CAPACITY),
            durable_core_invalidated: false,
        })
    }

    /// Complete the in-process command gate after the Session has been
    /// installed. No command reader is polled until this returns successfully.
    pub(crate) async fn await_hydration_ready(&mut self) -> Result<()> {
        if let Some(mut hydration) = self.hydration.take() {
            tokio::time::timeout(HYDRATION_READY_TIMEOUT, async {
                loop {
                    let state = hydration.borrow_and_update().clone();
                    if let Some(ready_generation) = state.generation() {
                        if ready_generation != self.executor_generation {
                            return Err(anyhow::anyhow!(
                                "hydration ready latched for generation {ready_generation}, expected {}",
                                self.executor_generation
                            ));
                        }
                        return Ok(());
                    }
                    if hydration.changed().await.is_err() {
                        return Err(anyhow::anyhow!(
                            "hydration ready signal closed before becoming ready"
                        ));
                    }
                }
            })
            .await
            .map_err(|_| anyhow::anyhow!("timed out waiting for hydration ready"))??;
        }
        Ok(())
    }

    pub(crate) async fn run(mut self) -> SessionResult {
        match self.run_until_exit().await {
            Ok(()) => {
                // Gateway EOF is terminal. Do not wait for an arbitrary
                // transport send to finish after the reader has closed.
                self.abort_writer().await;
                SessionResult::Completed(
                    self.core
                        .take()
                        .expect("clean idle exit retains the unique RunCore"),
                )
            }
            Err(failure) => {
                if self.active.is_some() {
                    self.shutdown_active().await;
                }
                self.abort_writer().await;
                let ownership = if self.durable_core_invalidated {
                    self.core.take();
                    RunOwnership::Lost
                } else {
                    self.core
                        .take()
                        .map_or(RunOwnership::Lost, RunOwnership::Recovered)
                };
                SessionResult::Failed { failure, ownership }
            }
        }
    }

    async fn run_until_exit(&mut self) -> Result<(), SessionFailure> {
        loop {
            if self.active.is_none() {
                enum IdleSelected {
                    Command(Result<InboundCommand>),
                    Writer(std::result::Result<Result<()>, oneshot::error::RecvError>),
                }
                let selected = tokio::select! {
                    command = self.gateway_reader.next_command() => IdleSelected::Command(command),
                    writer = &mut self.writer_done => IdleSelected::Writer(writer),
                };
                let inbound = match selected {
                    IdleSelected::Command(Ok(inbound)) => inbound,
                    IdleSelected::Command(Err(error))
                        if error.downcast_ref::<GatewayClosed>().is_some() =>
                    {
                        // Preserve a writer failure that won the race with
                        // EOF without waiting for a transport still in send.
                        return self.gateway_closed_result(false);
                    }
                    IdleSelected::Command(Err(error)) => {
                        return Err(gateway_failure("receive", error));
                    }
                    IdleSelected::Writer(writer) => return Err(writer_failure(writer)),
                };
                self.admit_and_route(inbound).await?;
                continue;
            }

            enum Selected {
                Completion(std::result::Result<RunCompletion, oneshot::error::RecvError>),
                Command(Result<InboundCommand>),
                Event(Option<RunOutput>),
                Writer(std::result::Result<Result<()>, oneshot::error::RecvError>),
            }

            let selected = {
                let active = self.active.as_mut().expect("active run checked above");
                tokio::select! {
                    biased;
                    completion = &mut active.completion_rx => Selected::Completion(completion),
                    command = self.gateway_reader.next_command() => Selected::Command(command),
                    event = active.events_rx.recv() => Selected::Event(event),
                    writer = &mut self.writer_done => Selected::Writer(writer),
                }
            };

            match selected {
                Selected::Completion(completion) => self.finish_run(completion).await?,
                Selected::Command(Ok(inbound)) => self.admit_and_route(inbound).await?,
                Selected::Command(Err(error))
                    if error.downcast_ref::<GatewayClosed>().is_some() =>
                {
                    return self.gateway_closed_result(true);
                }
                Selected::Command(Err(error)) => {
                    return Err(gateway_failure("receive", error));
                }
                Selected::Event(Some(event)) => self.persist_active_event(event).await?,
                Selected::Event(None) => self.resolve_closed_event_channel().await?,
                Selected::Writer(writer) => return Err(writer_failure(writer)),
            }
        }
    }

    async fn abort_writer(&mut self) {
        self.outbound.take();
        if let Some(join) = self.writer_join.take() {
            join.abort();
            let _ = join.await;
        }
    }

    fn gateway_closed_result(&mut self, active: bool) -> Result<(), SessionFailure> {
        match self.writer_done.try_recv() {
            Ok(Ok(())) => {
                if active {
                    Err(SessionFailure::GatewayClosedDuringRun)
                } else {
                    Ok(())
                }
            }
            Ok(Err(error)) => Err(gateway_failure("send", error)),
            Err(oneshot::error::TryRecvError::Closed) => {
                if active {
                    Err(SessionFailure::GatewayClosedDuringRun)
                } else {
                    Err(SessionFailure::OutboundClosed)
                }
            }
            Err(oneshot::error::TryRecvError::Empty) => {
                if active {
                    Err(SessionFailure::GatewayClosedDuringRun)
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn admit_and_route(&mut self, inbound: InboundCommand) -> Result<(), SessionFailure> {
        // Capture live ingress before durable admission. Replay returns before
        // construction below and therefore never fabricates a monotonic span.
        let received_monotonic = Instant::now();
        let receipt = match self
            .admission
            .receive_with_origin(&self.writer, &inbound)
            .await
        {
            Ok(receipt) => receipt,
            Err(error) if error.downcast_ref::<AdmissionRecoveryRequired>().is_some() => {
                return Err(SessionFailure::RecoveryRequired {
                    steps: self.recovery_steps.clone(),
                });
            }
            Err(error) => return Err(error.into()),
        };
        let ack = receipt.ack;
        let receipt_origin = receipt.origin;
        let received_at = receipt.received_at;
        self.enqueue_reliable(vec![OutboundFrame::CommandAck { ack: ack.clone() }])?;
        if receipt_origin == InboundReceiptOrigin::Replay {
            return Ok(());
        }
        if ack.status != CommandAckStatus::Received
            || matches!(inbound, InboundCommand::Invalid { .. })
        {
            return Ok(());
        }
        if !self.recovery_steps.is_empty() {
            // Replay-only admission has just authenticated an existing identity
            // and reconstructed its stored ACK. Execution remains gated.
            return Ok(());
        }
        let InboundCommand::Valid(command) = inbound else {
            unreachable!("invalid commands return above");
        };
        let command = AdmittedCommand::live(command, received_at, received_monotonic);
        if self.active.is_some() {
            if self.route_retry_wait_command(&command).await? {
                return Ok(());
            }
            if self.route_active_control(command.clone()).await? {
                return Ok(());
            }
            self.defer_active_command(command)?;
            return Ok(());
        }
        self.route_idle(command).await
    }

    async fn route_retry_wait_command(
        &mut self,
        command: &AdmittedCommand,
    ) -> Result<bool, SessionFailure> {
        if !matches!(command.envelope().command, Command::UserMessage { .. })
            || !self.deferred_commands.is_empty()
        {
            return Ok(false);
        }
        let eligible = self.active.as_ref().is_some_and(|active| {
            *active.phase_rx.borrow() == WorkerPhase::RetryWait
                && active.bridge.can_bind_retry_steer(&self.writer, command)
        });
        if !eligible {
            return Ok(false);
        }

        let mut phase_rx = self
            .active
            .as_ref()
            .expect("retry eligibility requires an active run")
            .phase_rx
            .clone();
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let (committed_tx, committed_rx) = oneshot::channel();
        let control = RunControl::RetrySteer {
            command: command.clone(),
            accepted: accepted_tx,
            committed: committed_rx,
        };
        let sent = self
            .active
            .as_ref()
            .expect("retry eligibility requires an active run")
            .control_tx
            .try_send(control);
        if sent.is_err() {
            return Ok(false);
        }
        if !await_retry_steer_acceptance(&mut phase_rx, accepted_rx).await {
            self.active
                .as_mut()
                .expect("retry eligibility requires an active run")
                .bridge
                .set_retry_steer_accept_failed(true);
            return Ok(false);
        }
        self.active
            .as_mut()
            .expect("accepted retry steer retains the active run")
            .bridge
            .bind_retry_steer(&self.writer, command.clone())
            .await?;
        committed_tx.send(()).map_err(|_| {
            SessionFailure::Worker(WorkerFailure::Error(
                "retry worker exited before durable steer authorization".to_owned(),
            ))
        })?;
        Ok(true)
    }

    async fn route_active_control(
        &mut self,
        command: AdmittedCommand,
    ) -> Result<bool, SessionFailure> {
        if matches!(command.envelope().command, Command::Abort {}) {
            return self.route_active_abort(command).await;
        }
        if !matches!(command.envelope().command, Command::UserMessage { .. }) {
            return Ok(false);
        }
        let Some(active) = self.active.as_ref() else {
            return Ok(false);
        };
        let stage = active.bridge.steer_stage();
        let Some(application_kind) = stage.classify_user_command() else {
            return Ok(false);
        };
        match application_kind {
            ApplicationKind::HardSteer => self.route_hard_steer(command).await,
            ApplicationKind::SoftSteer => self.route_soft_steer(command).await,
            ApplicationKind::RetrySteer => Ok(false), // handled by route_retry_wait_command
            _ => Ok(false),
        }
    }

    async fn route_hard_steer(&mut self, command: AdmittedCommand) -> Result<bool, SessionFailure> {
        let Some(active) = self.active.as_mut() else {
            return Ok(false);
        };
        if !active.bridge.can_bind_hard_steer() {
            return Ok(false);
        }
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let control = RunControl::HardSteer {
            command: command.clone(),
            accepted: accepted_tx,
        };
        let mut phase_rx = active.phase_rx.clone();
        if active.control_tx.try_send(control).is_err() {
            return Ok(false);
        }
        if !await_control_acceptance(&mut phase_rx, accepted_rx).await {
            return Ok(false);
        }
        // Reserve the provider attempt cancellation token before the durable
        // step-zero commit. If the commit succeeds, cancel the provider; if it
        // fails, the reservation restores the token on drop so the provider can
        // be cancelled later by an abort/EOF.
        let reservation = active
            .attempt_cancellation
            .reserve()
            .map_err(|error| SessionFailure::Worker(WorkerFailure::Error(error.to_string())))?;
        active
            .bridge
            .bind_hard_steer(&self.writer, command)
            .await
            .map_err(|error| SessionFailure::Worker(WorkerFailure::Error(error.to_string())))?;
        reservation.cancel_after_commit();
        Ok(true)
    }

    async fn route_soft_steer(&mut self, command: AdmittedCommand) -> Result<bool, SessionFailure> {
        let Some(active) = self.active.as_mut() else {
            return Ok(false);
        };
        if !active.bridge.can_bind_soft_steer(&self.writer, &command) {
            return Ok(false);
        }
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let (committed_tx, committed_rx) = oneshot::channel();
        let control = RunControl::SoftSteer {
            command: command.clone(),
            accepted: accepted_tx,
            committed: committed_rx,
        };
        let mut phase_rx = active.phase_rx.clone();
        if active.control_tx.try_send(control).is_err() {
            return Ok(false);
        }
        if !await_control_acceptance(&mut phase_rx, accepted_rx).await {
            return Ok(false);
        }
        active
            .bridge
            .bind_soft_steer(&self.writer, command)
            .await
            .map_err(|error| SessionFailure::Worker(WorkerFailure::Error(error.to_string())))?;
        committed_tx.send(()).map_err(|_| {
            SessionFailure::Worker(WorkerFailure::Error(
                "soft steer worker exited before durability authorization".to_owned(),
            ))
        })?;
        Ok(true)
    }

    async fn route_active_abort(
        &mut self,
        command: AdmittedCommand,
    ) -> Result<bool, SessionFailure> {
        let Some(active) = self.active.as_mut() else {
            return Ok(false);
        };
        if !active.bridge.can_bind_abort() {
            return Ok(false);
        }
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let (committed_tx, committed_rx) = oneshot::channel();
        let control = RunControl::Abort {
            command: command.clone(),
            accepted: accepted_tx,
            committed: committed_rx,
        };
        let mut phase_rx = active.phase_rx.clone();
        if active.control_tx.try_send(control).is_err() {
            return Ok(false);
        }
        if !await_control_acceptance(&mut phase_rx, accepted_rx).await {
            return Ok(false);
        }
        let mut acks = active
            .bridge
            .bind_abort(&self.writer, command)
            .await
            .map_err(|error| SessionFailure::Worker(WorkerFailure::Error(error.to_string())))?;
        committed_tx.send(()).map_err(|_| {
            SessionFailure::Worker(WorkerFailure::Error(
                "abort worker exited before durability authorization".to_owned(),
            ))
        })?;
        let superseded: std::collections::HashSet<String> = acks
            .iter()
            .filter(|ack| ack.status == CommandAckStatus::Superseded)
            .map(|ack| ack.command_id.clone())
            .collect();
        for ack in acks.drain(..) {
            self.enqueue_reliable(vec![OutboundFrame::CommandAck { ack }])?;
        }
        // Drop deferred commands that were durably superseded by this Abort.
        let mut retained = Vec::with_capacity(self.deferred_commands.len());
        while let Some(pending) = self.deferred_commands.pop_one() {
            if !superseded.contains(pending.envelope().command_id.as_str()) {
                retained.push(pending);
            }
        }
        for pending in retained {
            self.deferred_commands
                .push(pending)
                .map_err(anyhow::Error::from)?;
        }
        // Durable CancelRequested commits while the worker is still awaiting the
        // MessageEnd receipt, so the cutoff ordering and final AgentEnd owner
        // are correct when the Session event loop resumes.
        Ok(true)
    }

    fn defer_active_command(&mut self, command: AdmittedCommand) -> Result<(), SessionFailure> {
        let is_abort = matches!(command.envelope().command, Command::Abort {});
        let ordinary_count = self
            .deferred_commands
            .iter()
            .filter(|pending| !matches!(pending.envelope().command, Command::Abort {}))
            .count();
        if !is_abort && ordinary_count >= PENDING_ORDINARY_CONTROL_CAPACITY {
            return Err(anyhow::anyhow!(
                "Session deferred non-Abort window exceeds its 32-command invariant; command remains durably received"
            )
            .into());
        }
        if is_abort
            && self
                .deferred_commands
                .iter()
                .any(|pending| matches!(pending.envelope().command, Command::Abort {}))
        {
            return Err(anyhow::anyhow!(
                "Session deferred Abort reservation is already occupied; command remains durably received"
            )
            .into());
        }
        self.deferred_commands
            .push(command)
            .map_err(anyhow::Error::from)?;
        Ok(())
    }

    /// Re-examine deferred controls in sequence order whenever the active run
    /// reaches a phase where an earlier deferred command could now be routed.
    /// Only processes the front of the queue so later commands can never
    /// overtake an earlier deferred one.
    async fn reclassify_deferred(&mut self) -> Result<(), SessionFailure> {
        while let Some(command) = self.deferred_commands.pop_one() {
            let routed = match &command.envelope().command {
                Command::UserMessage { .. } => {
                    if self.route_retry_wait_command(&command).await? {
                        true
                    } else {
                        self.route_active_control(command.clone()).await?
                    }
                }
                Command::Abort {} => self.route_active_abort(command.clone()).await?,
                _ => false,
            };
            if !routed {
                self.deferred_commands
                    .push_front(command)
                    .map_err(anyhow::Error::from)?;
                break;
            }
        }
        Ok(())
    }

    async fn route_idle(&mut self, command: AdmittedCommand) -> Result<(), SessionFailure> {
        if matches!(command.envelope().command, Command::Abort {}) {
            let mut terminal = self
                .writer
                .apply_idle_abort_cutoff(
                    command.envelope().command_id.as_str(),
                    command.envelope().seq,
                )
                .await?;
            for ack in terminal.drain(..) {
                self.enqueue_reliable(vec![OutboundFrame::CommandAck { ack }])?;
            }
            return Ok(());
        }
        if !matches!(command.envelope().command, Command::UserMessage { .. }) {
            return Err(SessionFailure::IdleControl);
        }
        self.spawn_worker(command).await
    }

    async fn resolve_closed_event_channel(&mut self) -> Result<(), SessionFailure> {
        // Dropping the final event sender and publishing completion happen next
        // to each other on normal/error/panic exit. Give the task one scheduling
        // turn, then distinguish that race from a worker that abandoned its
        // event channel while retaining RunCore indefinitely.
        tokio::task::yield_now().await;
        let completion = {
            let active = self
                .active
                .as_mut()
                .expect("closed event channel requires active run");
            active.completion_rx.try_recv()
        };
        match completion {
            Ok(completion) => self.finish_run(Ok(completion)).await,
            Err(oneshot::error::TryRecvError::Closed) => {
                let mut active = self.active.take().expect("active run checked above");
                match (&mut active.join).await {
                    Err(error) => Err(worker_join_failure(error)),
                    Ok(()) => Err(SessionFailure::CompletionChannelClosed),
                }
            }
            Err(oneshot::error::TryRecvError::Empty) => {
                let mut active = self.active.take().expect("active run checked above");
                if active.join.is_finished() {
                    match (&mut active.join).await {
                        Err(error) => Err(worker_join_failure(error)),
                        Ok(()) => Err(SessionFailure::CompletionChannelClosed),
                    }
                } else {
                    active.join.abort();
                    (&mut active.join).await.ok();
                    Err(SessionFailure::EventChannelClosed)
                }
            }
        }
    }

    async fn spawn_worker(&mut self, initial: AdmittedCommand) -> Result<(), SessionFailure> {
        let binding = DurableRunBinding::idle(&initial, self.executor_generation);
        self.writer
            .apply(crate::store::EventBatch {
                writes: vec![crate::store::EventWrite {
                    event: None,
                    projections: vec![crate::store::Projection::CommandClassified {
                        command_id: binding.command_id.clone(),
                        application_kind: crate::store::ApplicationKind::IdleRun,
                        run_id: binding.run_id.clone(),
                        turn_id: binding.turn_id.clone(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await?;
        let mut core = self
            .core
            .take()
            .ok_or(SessionFailure::CompletionChannelClosed)?;
        core.durable_binding = Some(binding.clone());
        let (control_tx, control_rx) = mpsc::channel(CONTROL_CHANNEL_CAPACITY);
        let (events_tx, events_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let (phase_tx, phase_rx) = watch::channel(WorkerPhase::Active);
        core.worker_phase = Some(phase_tx);
        let attempt_cancellation = Arc::new(AttemptCancellation::default());
        core.attempt_cancellation = Some(attempt_cancellation.clone());
        let (completion_tx, completion_rx) = oneshot::channel();
        let future = catch_unwind(AssertUnwindSafe(|| {
            self.worker.run(core, initial, control_rx, events_tx)
        }))
        .map_err(|panic| SessionFailure::WorkerPanicked {
            message: panic_message(panic),
        })?;
        let join = tokio::spawn(async move {
            let completion = future.await;
            let _ = completion_tx.send(completion);
        });
        self.active = Some(ActiveRun {
            control_tx,
            phase_rx,
            events_rx,
            completion_rx,
            join,
            bridge: DurableBridge::new(binding),
            attempt_cancellation,
        });
        Ok(())
    }

    async fn finish_run(
        &mut self,
        completion: std::result::Result<RunCompletion, oneshot::error::RecvError>,
    ) -> std::result::Result<(), SessionFailure> {
        let mut active = self.active.take().expect("completion requires active run");
        let completion = match completion {
            Ok(completion) => completion,
            Err(_) => {
                return match (&mut active.join).await {
                    Err(error) => Err(worker_join_failure(error)),
                    Ok(()) => Err(SessionFailure::CompletionChannelClosed),
                };
            }
        };
        if let Err(error) = (&mut active.join).await {
            return Err(worker_join_failure(error));
        }
        let (core, worker_failure) = match completion {
            RunCompletion::Completed(core) => (core, None),
            RunCompletion::Failed { core, failure } => (core, Some(failure)),
        };
        let delivery_failure = self.drain_disconnected_outputs(&mut active, true).await?;
        // A completed RunCore includes every output already produced by the
        // worker. Do not expose it until the disconnected bounded event lane
        // has been drained into SQLite, even when Gateway delivery was lost.
        self.core = Some(core);
        if let Some(failure) = delivery_failure {
            return Err(failure);
        }
        match worker_failure {
            Some(failure) => Err(SessionFailure::Worker(failure)),
            None => self.route_deferred_after_run().await,
        }
    }

    async fn route_deferred_after_run(&mut self) -> Result<(), SessionFailure> {
        loop {
            let abort = self
                .deferred_commands
                .iter()
                .find(|command| matches!(command.envelope().command, Command::Abort {}))
                .cloned();
            let Some(abort) = abort else {
                break;
            };
            let abort_seq = abort.envelope().seq;
            let mut terminal = self
                .writer
                .apply_idle_abort_cutoff(abort.envelope().command_id.as_str(), abort_seq)
                .await?;
            for ack in terminal.drain(..) {
                self.enqueue_reliable(vec![OutboundFrame::CommandAck { ack }])?;
            }
            while self
                .deferred_commands
                .front()
                .is_some_and(|command| command.envelope().seq <= abort_seq)
            {
                self.deferred_commands.pop_one();
            }
        }

        let Some(next) = self.deferred_commands.pop_one() else {
            return Ok(());
        };
        if !matches!(next.envelope().command, Command::UserMessage { .. }) {
            self.deferred_commands
                .push_front(next)
                .map_err(anyhow::Error::from)?;
            return Err(SessionFailure::IdleControl);
        }
        if self
            .deferred_commands
            .iter()
            .any(|command| !matches!(command.envelope().command, Command::UserMessage { .. }))
        {
            self.deferred_commands
                .push_front(next)
                .map_err(anyhow::Error::from)?;
            return Err(SessionFailure::IdleControl);
        }
        self.spawn_worker(next).await
    }

    async fn shutdown_active(&mut self) {
        let Some(mut active) = self.active.take() else {
            return;
        };
        tokio::task::yield_now().await;
        match active.completion_rx.try_recv() {
            Ok(RunCompletion::Completed(core) | RunCompletion::Failed { core, .. }) => {
                if (&mut active.join).await.is_err() {
                    return;
                }
                // The caller already holds the primary Session failure. Commit
                // the suffix, but do not re-enter a failed Gateway during shutdown.
                match self.drain_disconnected_outputs(&mut active, false).await {
                    Ok(_) => self.core = Some(core),
                    Err(_) => self.durable_core_invalidated = true,
                }
            }
            Err(oneshot::error::TryRecvError::Closed) => {
                let _ = (&mut active.join).await;
            }
            Err(oneshot::error::TryRecvError::Empty) => {
                active.join.abort();
                let _ = (&mut active.join).await;
            }
        }
    }

    async fn drain_disconnected_outputs(
        &mut self,
        active: &mut ActiveRun,
        deliver: bool,
    ) -> Result<Option<SessionFailure>, SessionFailure> {
        let mut delivery_failure = None;
        while let Ok(output) = active.events_rx.try_recv() {
            let committed = match active.bridge.commit(&self.writer, output).await {
                Ok(committed) => committed,
                Err(error) => {
                    self.durable_core_invalidated = true;
                    return Err(error.into());
                }
            };
            let (outputs, tool_start_barrier, retry_wait_commit_barrier, terminal_command_ids) =
                committed.resolve_message_receipts();
            if let Some(barrier) = tool_start_barrier {
                barrier.committed();
            }
            if let Some(barrier) = retry_wait_commit_barrier {
                barrier.committed();
            }
            if deliver && delivery_failure.is_none() {
                delivery_failure = self
                    .send_committed(
                        outputs,
                        Some(active.bridge.command_id().to_owned()),
                        terminal_command_ids,
                    )
                    .await
                    .err();
            }
        }
        Ok(delivery_failure)
    }

    async fn persist_active_event(&mut self, output: RunOutput) -> Result<(), SessionFailure> {
        let committed = {
            let active = self.active.as_mut().expect("event requires active run");
            match active.bridge.commit(&self.writer, output).await {
                Ok(committed) => committed,
                Err(error) => {
                    self.durable_core_invalidated = true;
                    return Err(error.into());
                }
            }
        };
        let (outputs, tool_start_barrier, retry_wait_commit_barrier, terminal_command_ids) =
            committed.resolve_message_receipts();
        if let Some(barrier) = tool_start_barrier {
            barrier.committed();
        }
        if let Some(barrier) = retry_wait_commit_barrier {
            barrier.committed();
        }
        let assistant_started = outputs.iter().any(|output| {
            matches!(
                &output.event,
                AgentEvent::MessageStart { message, .. }
                    if matches!(
                        message.as_ref(),
                        PublicMessage::Assistant(assistant)
                            if !matches!(
                                assistant.stop_reason,
                                StopReason::Error | StopReason::Aborted
                            )
                    )
            )
        });
        let command_id = self
            .active
            .as_ref()
            .map(|active| active.bridge.command_id().to_owned());
        self.send_committed(outputs, command_id, terminal_command_ids)
            .await?;
        if assistant_started {
            self.reclassify_deferred().await?;
        }
        Ok(())
    }

    async fn send_committed(
        &mut self,
        committed: Vec<CommittedOutput>,
        command_id: Option<String>,
        terminal_command_ids: Vec<String>,
    ) -> Result<(), SessionFailure> {
        let applied_command = committed
            .iter()
            .any(|output| matches!(output.event, AgentEvent::AgentEnd));
        let volatile = committed.iter().all(|output| output.seq.is_none());
        if !committed.is_empty() && committed.iter().any(|output| output.seq.is_none()) != volatile
        {
            return Err(
                anyhow::anyhow!("committed output mixed durable and volatile frames").into(),
            );
        }
        let reliable =
            committed_delivery_is_reliable(&committed, applied_command, terminal_command_ids.len());
        let mut frames = Vec::with_capacity(
            committed.len() + usize::from(applied_command) + terminal_command_ids.len(),
        );
        for output in committed {
            frames.push(OutboundFrame::Event {
                envelope: crate::gateway::Envelope {
                    seq: output.seq,
                    conversation_id: self.conversation_id.clone(),
                    event: serde_json::to_value(output.event).map_err(anyhow::Error::from)?,
                },
            });
        }
        for command_id in &terminal_command_ids {
            let command_id = command_id.clone();
            let ack = self
                .writer
                .ack_for_command(&command_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("committed handoff command disappeared"))?;
            if ack.status != CommandAckStatus::Applied {
                return Err(anyhow::anyhow!(
                    "committed handoff command did not resolve to Applied"
                )
                .into());
            }
            frames.push(OutboundFrame::CommandAck { ack });
        }
        if applied_command
            && let Some(command_id) = command_id
            && !terminal_command_ids.iter().any(|id| id == &command_id)
        {
            let ack = self
                .writer
                .ack_for_command(&command_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("applied command disappeared after AgentEnd"))?;
            frames.push(OutboundFrame::CommandAck { ack });
        }
        if reliable {
            if !frames.is_empty() {
                self.enqueue_reliable(frames)?;
            }
        } else if volatile {
            for frame in frames {
                self.outbound_handle()?.enqueue_volatile(frame);
            }
        }
        Ok(())
    }

    fn outbound_handle(&self) -> Result<&OutboundHandle, SessionFailure> {
        self.outbound.as_ref().ok_or(SessionFailure::OutboundClosed)
    }

    fn enqueue_reliable(&mut self, frames: Vec<OutboundFrame>) -> Result<(), SessionFailure> {
        match self.outbound_handle()?.enqueue_reliable(frames) {
            Err(SessionFailure::OutboundClosed) => match self.writer_done.try_recv() {
                Ok(result) => result.map_err(|error| gateway_failure("send", error)),
                Err(oneshot::error::TryRecvError::Closed) => Err(SessionFailure::OutboundClosed),
                Err(oneshot::error::TryRecvError::Empty) => Err(SessionFailure::OutboundClosed),
            },
            result => result,
        }
    }

    #[cfg(test)]
    async fn wait_outbound_idle(&self) {
        let progress = &self.outbound_handle().expect("outbound handle").progress;
        let target = progress.enqueued.load(Ordering::Acquire);
        loop {
            let notified = progress.completed_notify.notified();
            if progress.completed.load(Ordering::Acquire) >= target {
                break;
            }
            notified.await;
        }
    }
}

impl<G: Gateway> Drop for Session<G> {
    fn drop(&mut self) {
        if let Some(join) = self.writer_join.as_ref() {
            join.abort();
        }
        if let Some(active) = self.active.as_ref() {
            active.join.abort();
        }
    }
}

fn gateway_failure(operation: &'static str, source: anyhow::Error) -> SessionFailure {
    SessionFailure::Gateway { operation, source }
}

fn writer_failure(
    result: std::result::Result<Result<()>, oneshot::error::RecvError>,
) -> SessionFailure {
    match result {
        Ok(Err(error)) => gateway_failure("send", error),
        Ok(Ok(())) | Err(_) => SessionFailure::OutboundClosed,
    }
}

fn worker_join_failure(error: tokio::task::JoinError) -> SessionFailure {
    let message = if error.is_panic() {
        "run worker panicked".to_owned()
    } else if error.is_cancelled() {
        "run worker task was cancelled".to_owned()
    } else {
        error.to_string()
    };
    SessionFailure::WorkerPanicked { message }
}

async fn await_retry_steer_acceptance(
    phase_rx: &mut watch::Receiver<WorkerPhase>,
    accepted_rx: oneshot::Receiver<bool>,
) -> bool {
    if *phase_rx.borrow_and_update() != WorkerPhase::RetryWait {
        return false;
    }
    let accepted = tokio::select! {
        biased;
        accepted = accepted_rx => accepted.unwrap_or(false),
        changed = phase_rx.changed() => {
            let _ = changed;
            false
        }
        () = tokio::time::sleep(RETRY_STEER_HANDSHAKE_TIMEOUT) => false,
    };
    accepted
}

async fn await_control_acceptance(
    phase_rx: &mut watch::Receiver<WorkerPhase>,
    accepted_rx: oneshot::Receiver<bool>,
) -> bool {
    tokio::select! {
        biased;
        accepted = accepted_rx => accepted.unwrap_or(false),
        changed = phase_rx.changed() => {
            let _ = changed;
            false
        }
        () = tokio::time::sleep(STEER_HANDSHAKE_TIMEOUT) => false,
    }
}

fn committed_delivery_is_reliable(
    committed: &[CommittedOutput],
    applied_command: bool,
    terminal_command_count: usize,
) -> bool {
    applied_command
        || terminal_command_count != 0
        || committed.iter().any(|output| output.seq.is_some())
}

fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "run worker factory panicked".to_owned()
    }
}

#[cfg(test)]
mod session_tests;
