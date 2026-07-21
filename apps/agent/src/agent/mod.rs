//! Agent session orchestration and turn lifecycle.
#![allow(
    dead_code,
    reason = "the Session actor is intentionally left unwired until the final T15 integration slice"
)]

use std::{
    any::Any,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::Arc,
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
        Command, CommandAckStatus, CommandEnvelope, Gateway, GatewayClosed, InboundCommand,
        OutboundFrame,
    },
    provider::{overflow::OverflowSource, types::PublicMessage},
    store::{
        DataKeyPurpose, EventWriter, InboundAdmission, InboundReceiptOrigin,
        RecoveryRequired as AdmissionRecoveryRequired, RecoveryStep, Store, SuffixRecovery,
    },
};

mod durable_bridge;
mod events;
mod provider_projection;
mod queue;
mod run;

use durable_bridge::{CommittedOutput, DurableBridge, DurableRunBinding};
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
/// API admission permits 32 ordinary commands plus one reserved Abort.
const PENDING_CONTROL_CAPACITY: usize = 33;

type WorkerFuture = Pin<Box<dyn Future<Output = RunCompletion> + Send + 'static>>;

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
        events: mpsc::Sender<AgentEvent>,
    ) -> WorkerFuture;
}

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
        events: mpsc::Sender<AgentEvent>,
    ) -> WorkerFuture {
        Box::pin((self)(core, initial, controls, events))
    }
}

pub(crate) struct ActiveRun {
    control_tx: mpsc::Sender<RunControl>,
    events_rx: mpsc::Receiver<AgentEvent>,
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
    #[error("run worker control channel closed before completion")]
    ControlChannelClosed,
    #[error("gateway closed while a run owned RunCore")]
    GatewayClosedDuringRun,
    #[error("gateway {operation} failed: {source}")]
    Gateway {
        operation: &'static str,
        #[source]
        source: anyhow::Error,
    },
    #[error("received a control command while idle; command remains durably received")]
    IdleControl,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Gateway/control-plane owner. `EventWriter` and `InboundAdmission` never
/// leave this value; workers receive only already-admitted typed commands.
pub(crate) struct Session<G: Gateway> {
    gateway: G,
    conversation_id: String,
    writer: EventWriter,
    admission: InboundAdmission,
    recovery_steps: Vec<RecoveryStep>,
    core: Option<RunCore>,
    active: Option<ActiveRun>,
    worker: Arc<dyn RunWorker>,
}

impl<G: Gateway + 'static> Session<G> {
    pub(crate) async fn start(
        store: Store,
        gateway: G,
        core: RunCore,
        worker: Arc<dyn RunWorker>,
    ) -> Result<Self> {
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
        Ok(Self {
            gateway,
            conversation_id,
            writer,
            admission,
            recovery_steps,
            core: Some(core),
            active: None,
            worker,
        })
    }

    pub(crate) async fn run(mut self) -> SessionResult {
        match self.run_until_exit().await {
            Ok(()) => SessionResult::Completed(
                self.core
                    .take()
                    .expect("clean idle exit retains the unique RunCore"),
            ),
            Err(failure) => {
                if self.active.is_some() {
                    self.shutdown_active().await;
                }
                let ownership = self
                    .core
                    .take()
                    .map_or(RunOwnership::Lost, RunOwnership::Recovered);
                SessionResult::Failed { failure, ownership }
            }
        }
    }

    async fn run_until_exit(&mut self) -> Result<(), SessionFailure> {
        loop {
            if self.active.is_none() {
                let inbound = match self.gateway.next_command().await {
                    Ok(inbound) => inbound,
                    Err(error) if error.downcast_ref::<GatewayClosed>().is_some() => {
                        return Ok(());
                    }
                    Err(error) => return Err(gateway_failure("receive", error)),
                };
                self.admit_and_route(inbound).await?;
                continue;
            }

            enum Selected {
                Completion(std::result::Result<RunCompletion, oneshot::error::RecvError>),
                Command(Result<InboundCommand>),
                Event(Option<AgentEvent>),
            }

            let selected = {
                let active = self.active.as_mut().expect("active run checked above");
                tokio::select! {
                    biased;
                    completion = &mut active.completion_rx => Selected::Completion(completion),
                    command = self.gateway.next_command() => Selected::Command(command),
                    event = active.events_rx.recv() => Selected::Event(event),
                }
            };

            match selected {
                Selected::Completion(completion) => self.finish_run(completion).await?,
                Selected::Command(Ok(inbound)) => self.admit_and_route(inbound).await?,
                Selected::Command(Err(error))
                    if error.downcast_ref::<GatewayClosed>().is_some() =>
                {
                    return Err(SessionFailure::GatewayClosedDuringRun);
                }
                Selected::Command(Err(error)) => {
                    return Err(gateway_failure("receive", error));
                }
                Selected::Event(Some(event)) => self.persist_active_event(event).await?,
                Selected::Event(None) => self.resolve_closed_event_channel().await?,
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
        self.gateway
            .send(OutboundFrame::CommandAck { ack: ack.clone() })
            .await
            .map_err(|error| gateway_failure("send", error))?;
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
        if let Some(active) = self.active.as_mut() {
            if let Err(error) = active.control_tx.send(RunControl::Command(command)).await {
                let RunControl::Command(command) = error.0;
                self.harvest_after_closed_control().await?;
                return self.route_idle(command).await;
            }
            return Ok(());
        }
        self.route_idle(command).await
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
                self.gateway
                    .send(OutboundFrame::CommandAck { ack })
                    .await
                    .map_err(|error| gateway_failure("send", error))?;
            }
            return Ok(());
        }
        if !matches!(command.envelope().command, Command::UserMessage { .. }) {
            return Err(SessionFailure::IdleControl);
        }
        self.spawn_worker(command).await
    }

    async fn harvest_after_closed_control(&mut self) -> Result<(), SessionFailure> {
        tokio::task::yield_now().await;
        let completion = {
            let active = self
                .active
                .as_mut()
                .expect("closed control requires active run");
            active.completion_rx.try_recv()
        };
        match completion {
            Ok(completion) => self.finish_run(Ok(completion)).await,
            Err(oneshot::error::TryRecvError::Closed) => {
                let active = self
                    .active
                    .take()
                    .expect("closed control requires active run");
                match active.join.await {
                    Err(error) => Err(worker_join_failure(error)),
                    Ok(()) => Err(SessionFailure::CompletionChannelClosed),
                }
            }
            Err(oneshot::error::TryRecvError::Empty) => {
                self.shutdown_active().await;
                Err(SessionFailure::ControlChannelClosed)
            }
        }
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
        let binding = DurableRunBinding::idle(&initial);
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
        if let Err(error) = active.join.await {
            return Err(worker_join_failure(error));
        }
        let worker_failure = match completion {
            RunCompletion::Completed(core) => {
                self.core = Some(core);
                None
            }
            RunCompletion::Failed { core, failure } => {
                self.core = Some(core);
                Some(failure)
            }
        };
        while let Ok(event) = active.events_rx.try_recv() {
            let output = active.bridge.output(event);
            let committed = active.bridge.commit(&self.writer, output).await?;
            self.send_committed(committed, Some(active.bridge.command_id().to_owned()))
                .await?;
        }
        match worker_failure {
            Some(failure) => Err(SessionFailure::Worker(failure)),
            None => Ok(()),
        }
    }

    async fn shutdown_active(&mut self) {
        let Some(mut active) = self.active.take() else {
            return;
        };
        tokio::task::yield_now().await;
        match active.completion_rx.try_recv() {
            Ok(RunCompletion::Completed(core) | RunCompletion::Failed { core, .. }) => {
                self.core = Some(core);
                let _ = active.join.await;
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

    async fn persist_active_event(&mut self, event: AgentEvent) -> Result<(), SessionFailure> {
        let committed = {
            let active = self.active.as_mut().expect("event requires active run");
            let output = active.bridge.output(event);
            active.bridge.commit(&self.writer, output).await?
        };
        let command_id = self
            .active
            .as_ref()
            .map(|active| active.bridge.command_id().to_owned());
        self.send_committed(committed, command_id).await
    }

    async fn send_committed(
        &mut self,
        committed: Vec<CommittedOutput>,
        command_id: Option<String>,
    ) -> Result<(), SessionFailure> {
        let applied_command = committed
            .iter()
            .any(|output| matches!(output.event, AgentEvent::AgentEnd));
        for output in committed {
            self.gateway
                .send(OutboundFrame::Event {
                    envelope: crate::gateway::Envelope {
                        seq: output.seq,
                        conversation_id: self.conversation_id.clone(),
                        event: serde_json::to_value(output.event).map_err(anyhow::Error::from)?,
                    },
                })
                .await
                .map_err(|error| gateway_failure("send", error))?;
        }
        if applied_command {
            let command_id = command_id.ok_or(SessionFailure::CompletionChannelClosed)?;
            let ack = self
                .writer
                .ack_for_command(&command_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("applied command disappeared after AgentEnd"))?;
            self.gateway
                .send(OutboundFrame::CommandAck { ack })
                .await
                .map_err(|error| gateway_failure("send", error))?;
        }
        Ok(())
    }
}

fn gateway_failure(operation: &'static str, source: anyhow::Error) -> SessionFailure {
    SessionFailure::Gateway { operation, source }
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
