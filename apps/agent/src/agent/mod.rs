//! Agent session orchestration and turn lifecycle.
#![allow(
    dead_code,
    reason = "the Session actor is intentionally left unwired until the final T15 integration slice"
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
};

use anyhow::Result;
use chrono::{DateTime, Utc};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{
    gateway::{
        Command, CommandAckStatus, CommandEnvelope, Gateway, GatewayClosed, GatewayReader,
        GatewayWriter, InboundCommand, OutboundFrame,
    },
    provider::{overflow::OverflowSource, types::PublicMessage},
    store::{
        DataKeyPurpose, EventWriter, InboundAdmission, InboundReceiptOrigin,
        RecoveryRequired as AdmissionRecoveryRequired, RecoveryStep, Store, SuffixRecovery,
    },
    tools::executor::validate_process_generation,
};

mod durable_bridge;
mod events;
mod provider_projection;
mod queue;
mod run;

use durable_bridge::{
    CommittedOutput, DurableBridge, DurableRunBinding, RunOutput, ToolStartCommitBarrier,
};
use queue::MessageQueue;

pub(crate) use events::{
    AgentEvent, ApprovalRequest, ApprovalResolution, PublicStreamEvent, SteerMode,
};
#[allow(unused_imports, reason = "consumed by the later T15 Session run loop")]
pub(crate) use provider_projection::{
    ProjectedProviderEvent, ProviderEventProjector, ProviderTerminal, ProviderTerminalKind,
};
#[allow(
    unused_imports,
    reason = "production provider/executor wiring lands in the final T15 integration slice"
)]
pub(crate) use run::{
    OverflowRecoveryOutcome, OverflowRecoveryRequest, ProviderAttempt, RunDriver,
    SequentialRunWorker,
};

const CONTROL_CHANNEL_CAPACITY: usize = 32;
const EVENT_CHANNEL_CAPACITY: usize = 64;
const OUTBOUND_CHANNEL_CAPACITY: usize = 64;
const VOLATILE_OUTBOUND_BUDGET: usize = 32;
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
#[derive(Debug)]
pub(crate) struct RunCore {
    ownership_id: Uuid,
    mutation_epoch: u64,
    pending_controls: MessageQueue<AdmittedCommand>,
    pending_overflow_apply: Option<OverflowSource>,
    /// In-memory send context returned with the unique core. T17 replaces this
    /// flat representation with `ThreeLayerMemory`; keeping it in `RunCore`
    /// prevents a second Session run from silently losing the first run.
    runtime_context: Vec<PublicMessage>,
    durable_binding: Option<DurableRunBinding>,
}

impl RunCore {
    pub(crate) fn new() -> Self {
        Self {
            ownership_id: Uuid::now_v7(),
            mutation_epoch: 0,
            pending_controls: MessageQueue::bounded(PENDING_CONTROL_CAPACITY),
            pending_overflow_apply: None,
            runtime_context: Vec::new(),
            durable_binding: None,
        }
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

impl Default for RunCore {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AdmittedCommand {
    envelope: CommandEnvelope,
    received_at: DateTime<Utc>,
}

impl AdmittedCommand {
    pub(crate) fn new(envelope: CommandEnvelope, received_at: DateTime<Utc>) -> Self {
        Self {
            envelope,
            received_at,
        }
    }

    pub(crate) fn envelope(&self) -> &CommandEnvelope {
        &self.envelope
    }

    pub(crate) fn received_at(&self) -> DateTime<Utc> {
        self.received_at
    }
}

pub(crate) enum RunControl {
    Command(AdmittedCommand),
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
                            if events.send(RunOutput { binding: binding.clone(), event, commit_barrier }).await.is_err() {
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
                        if events.send(RunOutput { binding: binding.clone(), event, commit_barrier }).await.is_err() {
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
    #[allow(
        dead_code,
        reason = "T15 retains the sender so an active worker never observes a false control-lane close"
    )]
    control_tx: mpsc::Sender<RunControl>,
    events_rx: mpsc::Receiver<RunOutput>,
    completion_rx: oneshot::Receiver<RunCompletion>,
    join: JoinHandle<()>,
    bridge: DurableBridge,
}

#[derive(Debug)]
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
    #[error("session startup is recovery-gated by T15-owned suffix: {steps:?}")]
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
    core: Option<RunCore>,
    active: Option<ActiveRun>,
    worker: Arc<dyn RunWorker>,
    executor_generation: u64,
    /// T16 owns active-run classification and control semantics. Until then
    /// every command received during a run remains durably `received` in one
    /// sequence-ordered queue. After AgentEnd, T15 may start a fresh user run
    /// or apply an idle Abort cutoff, but it must not let a user overtake an
    /// earlier/later control that only T16 can apply safely.
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
        executor_generation: u64,
    ) -> Result<Self> {
        validate_process_generation(executor_generation)?;
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
        let recovery_steps = SuffixRecovery::recover_t12_prefix(&store, &writer).await?;
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
            core: Some(core),
            active: None,
            worker,
            executor_generation,
            deferred_commands: MessageQueue::bounded(PENDING_CONTROL_CAPACITY),
            durable_core_invalidated: false,
        })
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
        let command = AdmittedCommand::new(command, received_at);
        if self.active.is_some() {
            self.defer_active_command(command)?;
            return Ok(());
        }
        self.route_idle(command).await
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
                let active = self.active.take().expect("active run checked above");
                match active.join.await {
                    Err(error) => Err(worker_join_failure(error)),
                    Ok(()) => Err(SessionFailure::CompletionChannelClosed),
                }
            }
            Err(oneshot::error::TryRecvError::Empty) => {
                let active = self.active.take().expect("active run checked above");
                if active.join.is_finished() {
                    match active.join.await {
                        Err(error) => Err(worker_join_failure(error)),
                        Ok(()) => Err(SessionFailure::CompletionChannelClosed),
                    }
                } else {
                    active.join.abort();
                    active.join.await.ok();
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
            events_rx,
            completion_rx,
            join,
            bridge: DurableBridge::new(binding),
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
                return match active.join.await {
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
                let _ = active.join.await;
            }
            Err(oneshot::error::TryRecvError::Empty) => {
                active.join.abort();
                let _ = active.join.await;
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
            if let Some(barrier) = committed.tool_start_barrier {
                barrier.committed();
            }
            if deliver && delivery_failure.is_none() {
                delivery_failure = self
                    .send_committed(
                        committed.outputs,
                        Some(active.bridge.command_id().to_owned()),
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
        if let Some(barrier) = committed.tool_start_barrier {
            barrier.committed();
        }
        let command_id = self
            .active
            .as_ref()
            .map(|active| active.bridge.command_id().to_owned());
        self.send_committed(committed.outputs, command_id).await
    }

    async fn send_committed(
        &mut self,
        committed: Vec<CommittedOutput>,
        command_id: Option<String>,
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
        let mut frames = Vec::with_capacity(committed.len() + usize::from(applied_command));
        for output in committed {
            frames.push(OutboundFrame::Event {
                envelope: crate::gateway::Envelope {
                    seq: output.seq,
                    conversation_id: self.conversation_id.clone(),
                    event: serde_json::to_value(output.event).map_err(anyhow::Error::from)?,
                },
            });
        }
        if applied_command {
            let command_id = command_id.ok_or(SessionFailure::CompletionChannelClosed)?;
            let ack = self
                .writer
                .ack_for_command(&command_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("applied command disappeared after AgentEnd"))?;
            frames.push(OutboundFrame::CommandAck { ack });
        }
        if volatile {
            for frame in frames {
                self.outbound_handle()?.enqueue_volatile(frame);
            }
        } else if !frames.is_empty() {
            self.enqueue_reliable(frames)?;
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
