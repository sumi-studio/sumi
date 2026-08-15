//! Agent session orchestration and turn lifecycle.
#![allow(
    dead_code,
    reason = "the Session actor is intentionally left unwired until T26 production composition"
)]

use std::{
    any::Any,
    collections::HashSet,
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

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde_json::json;
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    approval::route_broker::RouteApprovalBroker,
    gateway::{
        Command, CommandAck, CommandAckStatus, CommandEnvelope, Gateway, GatewayClosed,
        GatewayReader, GatewayWriter, InboundCommand, OutboundFrame,
    },
    memory::estimate::ProviderContextItemWithFootprint,
    provider::{
        overflow::OverflowSource,
        types::{ContextMessage, PublicMessage, StopReason, ToolResultMessage, UserContent},
    },
    runtime::{
        authority::RuntimeEpochAuthority,
        contracts::{PersonalityAgentId, ProcessGeneration, RpcIdentity},
    },
    store::{
        ApplicationKind, ApprovalMutation, DataKeyPurpose, DurableEvent, EventBatch, EventWrite,
        EventWriter, HydratedRunState, HydrationReceiptIdentity, InboundAdmission,
        InboundReceiptOrigin, Projection, RecoveryRequired as AdmissionRecoveryRequired,
        RecoveryStep, ResumeDirective, Store, ToolExecutionMutation,
    },
};

#[cfg(test)]
use crate::approval::ApprovalBroker;

#[derive(Clone, Debug)]
pub(crate) enum ApprovalRuntime {
    Route(Arc<RouteApprovalBroker>),
    #[cfg(test)]
    Legacy(Arc<ApprovalBroker>),
}

impl From<Arc<RouteApprovalBroker>> for ApprovalRuntime {
    fn from(broker: Arc<RouteApprovalBroker>) -> Self {
        Self::Route(broker)
    }
}

#[cfg(test)]
impl From<Arc<ApprovalBroker>> for ApprovalRuntime {
    fn from(broker: Arc<ApprovalBroker>) -> Self {
        Self::Legacy(broker)
    }
}

impl ApprovalRuntime {
    pub(crate) fn route(&self) -> Option<&Arc<RouteApprovalBroker>> {
        match self {
            Self::Route(broker) => Some(broker),
            #[cfg(test)]
            Self::Legacy(_) => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn legacy(&self) -> Option<&Arc<ApprovalBroker>> {
        match self {
            Self::Route(_) => None,
            Self::Legacy(broker) => Some(broker),
        }
    }

    fn is_resolving(&self, request_id: &str) -> bool {
        match self {
            Self::Route(broker) => broker.is_resolving(request_id),
            #[cfg(test)]
            Self::Legacy(broker) => broker.is_resolving(request_id),
        }
    }

    fn has_pending(&self, request_id: &str) -> bool {
        match self {
            Self::Route(broker) => broker.has_pending(request_id),
            #[cfg(test)]
            Self::Legacy(broker) => broker.has_pending(request_id),
        }
    }

    fn pending_summary(&self, request_id: &str) -> Option<ApprovalPendingSummary> {
        match self {
            Self::Route(broker) => {
                broker
                    .pending_summary(request_id)
                    .map(|summary| ApprovalPendingSummary {
                        tool_call_id: summary.tool_call_id,
                        tool_name: summary.tool_name,
                    })
            }
            #[cfg(test)]
            Self::Legacy(broker) => {
                broker
                    .pending_summary(request_id)
                    .map(|summary| ApprovalPendingSummary {
                        tool_call_id: summary.tool_call_id,
                        tool_name: summary.tool_name,
                    })
            }
        }
    }

    fn finish_resolution(&self, request_id: &str) {
        match self {
            Self::Route(broker) => broker.commit_resolution(request_id),
            #[cfg(test)]
            Self::Legacy(broker) => broker.finish_resolution(request_id),
        }
    }

    fn cancel(&self, request_id: &str) -> bool {
        match self {
            Self::Route(broker) => broker.cancel(request_id),
            #[cfg(test)]
            Self::Legacy(broker) => broker.cancel(request_id),
        }
    }

    fn cancel_all(&self) {
        match self {
            Self::Route(broker) => {
                broker.cancel_all();
            }
            #[cfg(test)]
            Self::Legacy(broker) => {
                broker.cancel_all();
            }
        }
    }
}

struct ApprovalPendingSummary {
    tool_call_id: String,
    tool_name: String,
}

#[cfg(test)]
use crate::store::SuffixRecovery;

mod driver;
mod durable_bridge;
pub(crate) mod events;
mod provider_projection;
mod queue;
mod run;
#[cfg(test)]
mod start_authority_tests;
mod steer;

pub(crate) use durable_bridge::DurableRunBinding;

use durable_bridge::{
    ApprovalDecisionOutput, ApprovalNotStartedContext, ApprovalOutputContext, CommittedOutput,
    DurableBridge, MessageCommitBarrier, MessageCommitReceipt, RetryWaitCommitBarrier, RunOutput,
    ToolStartCommitBarrier, ToolStartCommitResult,
};
use queue::MessageQueue;

#[cfg(test)]
pub(crate) use driver::StreamStarter;
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
const RUNTIME_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// Once the graceful shutdown deadline has made the durable boundary
/// indeterminate, do not detach the still-owned worker while reporting that
/// outcome. The abort itself is not settlement: retain and join the exact
/// handle within this independent ownership bound.
const RUNTIME_SHUTDOWN_ABORT_JOIN_TIMEOUT: Duration = Duration::from_secs(2);
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

    async fn enqueue_reliable_wait(
        &self,
        frames: Vec<OutboundFrame>,
    ) -> Result<(), SessionFailure> {
        self.tx
            .send(OutboundItem {
                frames,
                volatile: false,
            })
            .await
            .map_err(|_| SessionFailure::OutboundClosed)?;
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
        if let Err(error) = writer.send_batch(item.frames).await {
            if volatile {
                volatile_in_flight.fetch_sub(1, Ordering::AcqRel);
            }
            return Err(error);
        }
        if volatile {
            volatile_in_flight.fetch_sub(1, Ordering::AcqRel);
        }
        progress.completed.fetch_add(1, Ordering::Release);
        progress.completed_notify.notify_waiters();
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct HydratedSessionBinding {
    binding_id: Uuid,
    runtime: RuntimeEpochAuthority,
    receipt: HydrationReceiptIdentity,
    core_ownership_id: Uuid,
    core_mutation_epoch: u64,
}

impl HydratedSessionBinding {
    fn validate_receipt(&self) -> Result<()> {
        if self.receipt.intent_count != 0
            || self.receipt.personality_agent_id != *self.runtime.personality_agent_id()
            || self.receipt.generation != self.runtime.generation()
            || self.receipt.lease_id != self.runtime.lease().lease_id()
            || self.receipt.fence_id != self.runtime.fence().fence_id()
        {
            bail!(
                "hydration receipt does not prove the authenticated runtime epoch at a clean recovery fixed point"
            );
        }
        Ok(())
    }
}

/// The sole mutable conversation value transferred into and out of a worker.
/// It is intentionally neither `Clone` nor wrapped in shared mutability.
#[derive(Debug)]
pub(crate) struct RunCore {
    ownership_id: Uuid,
    mutation_epoch: u64,
    /// Private proof that this exact, still-unmutated core was constructed
    /// from the authenticated T17 snapshot accepted for one runtime epoch.
    /// Session rechecks the proof before touching Store keys or gateway state.
    hydrated_session_binding_id: Option<Uuid>,
    pending_controls: MessageQueue<AdmittedCommand>,
    pending_overflow_apply: Option<OverflowSource>,
    /// In-memory persisted send context returned with the unique core. T21
    /// defines the `ThreeLayerMemory` replacement, and T26 composes it into
    /// production; keeping this injected representation in `RunCore` prevents
    /// a second Session run from silently losing the first run.
    runtime_context: Vec<ContextMessage>,
    /// Authenticated provider-context fragments with their authoritative saved
    /// eviction footprints, carried from T17 cold-boot hydration into the T21
    /// runtime assembler. This is the production seam that replaces test-only
    /// `ContextAssembler::set_provider_context` calls.
    provider_context: Vec<ProviderContextItemWithFootprint>,
    durable_binding: Option<DurableRunBinding>,
    worker_phase: Option<watch::Sender<WorkerPhase>>,
    /// Shared cancellation registry for the one live provider attempt. The
    /// Session reserves the token around `bind_hard_steer` so the provider is
    /// only cancelled after the durable step-zero commit succeeds.
    attempt_cancellation: Option<Arc<AttemptCancellation>>,
    /// One Session-owned runtime shutdown lineage. Each worker receives a
    /// child token, and every externally backed phase derives from that child.
    /// This is runtime-only state and is never used as durable replay proof.
    runtime_shutdown: CancellationToken,
    approval: Option<ApprovalRuntime>,
    #[cfg(test)]
    fixture_bypass_approval: bool,
}

impl RunCore {
    pub(crate) fn new() -> Self {
        Self {
            ownership_id: Uuid::now_v7(),
            mutation_epoch: 0,
            hydrated_session_binding_id: None,
            pending_controls: MessageQueue::bounded(PENDING_CONTROL_CAPACITY),
            pending_overflow_apply: None,
            runtime_context: Vec::new(),
            provider_context: Vec::new(),
            durable_binding: None,
            worker_phase: None,
            attempt_cancellation: None,
            runtime_shutdown: CancellationToken::new(),
            approval: None,
            #[cfg(test)]
            fixture_bypass_approval: false,
        }
    }

    /// Hydrated T17 cold-boot provider context. The saved eviction footprints
    /// are authoritative and are handed to the T21 assembler at worker start.
    pub(crate) fn with_hydrated_provider_context(
        mut self,
        provider_context: Vec<ProviderContextItemWithFootprint>,
    ) -> Self {
        self.hydrated_session_binding_id = None;
        self.provider_context = provider_context;
        self
    }

    pub(crate) fn ownership_id(&self) -> Uuid {
        self.ownership_id
    }

    pub(crate) fn mutation_epoch(&self) -> u64 {
        self.mutation_epoch
    }

    pub(crate) fn mark_mutated(&mut self) {
        self.hydrated_session_binding_id = None;
        self.mutation_epoch = self.mutation_epoch.saturating_add(1);
    }

    pub(crate) fn set_approval(&mut self, broker: impl Into<ApprovalRuntime>) {
        self.approval = Some(broker.into());
        self.mark_mutated();
    }

    #[cfg(test)]
    pub(crate) fn fixture_with_unapproved_tools() -> Self {
        let mut core = Self::new();
        core.fixture_bypass_approval = true;
        core
    }

    pub(crate) fn queue_followup(&mut self, command: AdmittedCommand) -> Result<()> {
        self.pending_controls.push(command)?;
        self.hydrated_session_binding_id = None;
        Ok(())
    }

    pub(crate) fn next_followup(&mut self) -> Option<AdmittedCommand> {
        let command = self.pending_controls.pop_one();
        if command.is_some() {
            self.hydrated_session_binding_id = None;
        }
        command
    }

    pub(crate) fn requeue_followup_front(&mut self, command: AdmittedCommand) -> Result<()> {
        self.pending_controls.push_front(command)?;
        self.hydrated_session_binding_id = None;
        Ok(())
    }

    pub(crate) fn has_pending_controls(&self) -> bool {
        !self.pending_controls.is_empty()
    }

    pub(crate) fn defer_overflow_apply(&mut self, source: OverflowSource) {
        self.pending_overflow_apply.get_or_insert(source);
        self.hydrated_session_binding_id = None;
    }

    pub(crate) fn pending_overflow_apply(&self) -> Option<OverflowSource> {
        self.pending_overflow_apply
    }

    pub(crate) fn clear_pending_overflow_apply(&mut self) {
        self.pending_overflow_apply = None;
        self.hydrated_session_binding_id = None;
    }

    pub(crate) fn install_hydrated_context(
        &mut self,
        messages: Vec<ContextMessage>,
        provider_context: Vec<ProviderContextItemWithFootprint>,
    ) {
        self.runtime_context = messages;
        self.provider_context = provider_context;
        self.mark_mutated();
    }

    #[cfg(test)]
    pub(crate) fn runtime_context(&self) -> &[ContextMessage] {
        &self.runtime_context
    }

    #[cfg(test)]
    pub(crate) fn provider_context(&self) -> &[ProviderContextItemWithFootprint] {
        &self.provider_context
    }
}

enum SessionStartAuthorityKind {
    Hydrated(Box<HydratedSessionBinding>),
    #[cfg(test)]
    UnhydratedFixture(ProcessGeneration),
}

/// Authenticated authority required to transfer one hydrated `RunCore` into a
/// Session. It is deliberately neither `Clone` nor constructible from bare
/// identity values in production.
pub(crate) struct SessionStartAuthority {
    kind: SessionStartAuthorityKind,
}

impl SessionStartAuthority {
    /// Bind a completed T17 hydration result to the exact runtime epoch that
    /// requested it and construct the only RunCore that this authority admits.
    ///
    /// T26 may inspect `hydrated` first to compose approval, memory, and driver
    /// dependencies, but Session state itself is always initialized from the
    /// authenticated messages and provider context here.
    pub(crate) fn from_hydrated(
        runtime: RuntimeEpochAuthority,
        hydrated: &HydratedRunState,
        approval: impl Into<ApprovalRuntime>,
    ) -> Result<(RunCore, Self)> {
        if hydrated.scope.personality_agent_id != *runtime.personality_agent_id() {
            bail!("hydrated Store scope does not match the authenticated runtime PAID");
        }
        if hydrated.lease != *runtime.lease() {
            bail!("hydrated process-generation lease is stale for the authenticated runtime epoch");
        }
        if hydrated.fence != *runtime.fence() {
            bail!("hydrated recovery fence is stale for the authenticated runtime epoch");
        }
        if hydrated.resume != ResumeDirective::AdmitCommands {
            bail!("hydration result does not authorize command admission");
        }

        let mut core = RunCore::new();
        core.runtime_context = hydrated.messages.clone();
        core.provider_context = hydrated.provider_context.clone();
        // Approval is a security-sensitive dependency of this exact RunCore.
        // Compose it before minting the binding; every later replacement goes
        // through `set_approval` and invalidates the binding.
        core.approval = Some(approval.into());
        let binding = HydratedSessionBinding {
            binding_id: Uuid::now_v7(),
            runtime,
            receipt: hydrated.receipt.clone(),
            core_ownership_id: core.ownership_id,
            core_mutation_epoch: core.mutation_epoch,
        };
        binding.validate_receipt()?;
        core.hydrated_session_binding_id = Some(binding.binding_id);
        Ok((
            core,
            Self {
                kind: SessionStartAuthorityKind::Hydrated(Box::new(binding)),
            },
        ))
    }

    fn validate_for(&self, store: &Store, core: &RunCore) -> Result<ProcessGeneration> {
        match &self.kind {
            SessionStartAuthorityKind::Hydrated(binding) => {
                binding.validate_receipt()?;
                if store.scope().personality_agent_id != *binding.runtime.personality_agent_id() {
                    bail!("Session Store PAID does not match the authenticated runtime epoch");
                }
                if core.hydrated_session_binding_id != Some(binding.binding_id)
                    || core.ownership_id != binding.core_ownership_id
                    || core.mutation_epoch != binding.core_mutation_epoch
                {
                    bail!(
                        "RunCore is not the exact unmutated core bound to this hydration authority"
                    );
                }
                Ok(binding.runtime.generation())
            }
            #[cfg(test)]
            SessionStartAuthorityKind::UnhydratedFixture(generation) => Ok(*generation),
        }
    }

    fn uses_completed_hydration(&self) -> bool {
        matches!(&self.kind, SessionStartAuthorityKind::Hydrated(_))
    }

    #[cfg(test)]
    fn unhydrated_fixture(generation: ProcessGeneration) -> Self {
        // Existing T15/T16 actor tests exercise post-start behavior with
        // synthetic stores and cores. Production has no conversion from a
        // bare generation; focused authority tests use `start_hydrated`.
        Self {
            kind: SessionStartAuthorityKind::UnhydratedFixture(generation),
        }
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
    Approval,
    RetryWait,
}

pub(crate) enum RunCompletion {
    Completed(RunCore),
    Failed {
        core: RunCore,
        failure: WorkerFailure,
    },
    /// The durable life log advanced beyond the worker's in-memory replay
    /// state. There is deliberately no RunCore to recover: the next owner must
    /// hydrate from the authoritative Store before another run can start.
    RehydrationRequired {
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
    /// Hydrated production starts must preserve the complete executor RPC
    /// identity through the worker/driver boundary.
    fn validate_runtime_identity(&self, _identity: &RpcIdentity) -> Result<()> {
        Err(anyhow::anyhow!(
            "run worker is not bound to a production executor RPC identity"
        ))
    }

    /// Generation-only validation is retained for explicit unhydrated test
    /// fixtures; it cannot admit a hydrated production Session.
    fn validate_executor_generation(&self, generation: ProcessGeneration) -> Result<()>;

    fn apply_idle_memory_maintenance<'a>(
        &'a self,
        _core: &'a mut RunCore,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>> {
        Box::pin(async { Ok(false) })
    }

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
    match completion {
        RunCompletion::Completed(core) | RunCompletion::Failed { core, .. } => {
            RunCompletion::Failed {
                core,
                failure: WorkerFailure::EventChannelClosed,
            }
        }
        RunCompletion::RehydrationRequired { .. } => RunCompletion::RehydrationRequired {
            failure: WorkerFailure::EventChannelClosed,
        },
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
    approval: Option<ApprovalRuntime>,
    resolving_approvals: HashSet<String>,
}

impl Drop for ActiveRun {
    fn drop(&mut self) {
        self.join.abort();
    }
}

impl ActiveRun {
    fn finish_committed_approval_resolutions(&mut self, outputs: &[CommittedOutput]) {
        let committed: Vec<_> = outputs
            .iter()
            .filter_map(|output| match &output.event {
                AgentEvent::ApprovalResolved { request_id, .. } => Some(request_id.clone()),
                _ => None,
            })
            .collect();
        for request_id in committed {
            self.resolving_approvals.remove(&request_id);
            if let Some(broker) = self.approval.as_ref() {
                broker.finish_resolution(&request_id);
            }
        }
    }
}

#[derive(Debug)]
// RunCore owns the durable replay state and is moved through this enum only at
// worker completion; boxing it would add an allocation to every run.
#[allow(clippy::large_enum_variant)]
pub(crate) enum RunOwnership {
    Recovered(Box<RunCore>),
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionLoopExit {
    GatewayClosed,
    ShutdownRequested,
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
    #[error("runtime shutdown exceeded its bounded grace period; active RunCore ownership is lost")]
    RuntimeShutdownOwnershipLost,
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
    personality_agent_id: PersonalityAgentId,
    writer: EventWriter,
    admission: InboundAdmission,
    recovery_steps: Vec<RecoveryStep>,
    core: Option<RunCore>,
    active: Option<ActiveRun>,
    worker: Arc<dyn RunWorker>,
    /// Retains the authenticated runtime/hydration proof for the whole
    /// Session lifetime; `executor_generation` is derived from this proof.
    start_authority: SessionStartAuthority,
    executor_generation: ProcessGeneration,
    /// T15 already applies the idle/post-run Abort cutoff and supplies the
    /// injected cancellation and phase-observation seams. Commands received
    /// during an active run otherwise remain durably `received` in this
    /// sequence-ordered queue. T16 owns active/live classification, cutoff,
    /// and the full control semantics without allowing a user to overtake an
    /// earlier/later control.
    deferred_commands: MessageQueue<AdmittedCommand>,
    /// A completed shelf is durable, but its wake signal can arrive while a
    /// worker exclusively owns `RunCore`. Retain it until the next idle
    /// boundary; it is independent of a run-local overflow marker.
    maintenance_ready_pending: bool,
    /// A bridge/Store refusal can leave a returned core ahead of durability;
    /// a post-receipt worker failure can leave it behind. Neither state is a
    /// recoverable life-log snapshot.
    durable_core_invalidated: bool,
    /// The root of the cancellation lineage installed by `run` or
    /// `run_until_cancelled`. Workers receive children, so completing one run
    /// cannot cancel a later run or the Session itself.
    runtime_shutdown: CancellationToken,
    #[cfg(test)]
    active_take_observer: Option<oneshot::Sender<bool>>,
}

impl<G: Gateway + 'static> Session<G> {
    /// All-cfg typed entry point exercised directly by the hydration-authority
    /// tests and used by T26 production composition.
    pub(crate) async fn start_hydrated(
        store: Store,
        gateway: G,
        core: RunCore,
        worker: Arc<dyn RunWorker>,
        start_authority: SessionStartAuthority,
    ) -> Result<Self> {
        Self::start_inner(store, gateway, core, worker, start_authority).await
    }

    #[cfg(not(test))]
    pub(crate) async fn start(
        store: Store,
        gateway: G,
        core: RunCore,
        worker: Arc<dyn RunWorker>,
        start_authority: SessionStartAuthority,
    ) -> Result<Self> {
        Self::start_hydrated(store, gateway, core, worker, start_authority).await
    }

    #[cfg(test)]
    pub(crate) async fn start(
        store: Store,
        gateway: G,
        core: RunCore,
        worker: Arc<dyn RunWorker>,
        executor_generation: ProcessGeneration,
    ) -> Result<Self> {
        Self::start_fixture(store, gateway, core, worker, executor_generation).await
    }

    #[cfg(test)]
    async fn start_fixture(
        store: Store,
        gateway: G,
        core: RunCore,
        worker: Arc<dyn RunWorker>,
        executor_generation: ProcessGeneration,
    ) -> Result<Self> {
        Self::start_inner(
            store,
            gateway,
            core,
            worker,
            SessionStartAuthority::unhydrated_fixture(executor_generation),
        )
        .await
    }

    async fn start_inner(
        store: Store,
        gateway: G,
        core: RunCore,
        worker: Arc<dyn RunWorker>,
        start_authority: SessionStartAuthority,
    ) -> Result<Self> {
        // Every authority/core/Store check precedes key creation, recovery,
        // gateway splitting, task creation, and worker validation.
        let executor_generation = start_authority.validate_for(&store, &core)?;
        match &start_authority.kind {
            SessionStartAuthorityKind::Hydrated(binding) => {
                worker.validate_runtime_identity(binding.runtime.rpc_identity())?;
            }
            #[cfg(test)]
            SessionStartAuthorityKind::UnhydratedFixture(_) => {
                worker.validate_executor_generation(executor_generation)?;
            }
        }
        let personality_agent_id = store.scope().personality_agent_id.clone();
        let store = Arc::new(store);
        for purpose in [
            DataKeyPurpose::Command,
            DataKeyPurpose::Event,
            DataKeyPurpose::Transcript,
        ] {
            store.private_key(purpose).await?;
        }
        let writer = EventWriter::new(store.clone());
        writer.initialize_recovery_checkpoint().await?;
        let recovery_steps = match &start_authority.kind {
            // `HydratedRunState::resume == AdmitCommands` is T17's unique
            // fixed-point authority. Re-running T12 recovery here would make a
            // second bootstrap decision after the authenticated snapshot.
            SessionStartAuthorityKind::Hydrated(_) => Vec::new(),
            #[cfg(test)]
            SessionStartAuthorityKind::UnhydratedFixture(_) => {
                SuffixRecovery::recover_t12_prefix(&store, &writer).await?
            }
        };
        debug_assert!(
            !start_authority.uses_completed_hydration() || recovery_steps.is_empty(),
            "completed hydration must not produce a second recovery plan"
        );
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
            personality_agent_id,
            writer,
            admission,
            recovery_steps,
            core: Some(core),
            active: None,
            worker,
            start_authority,
            executor_generation,
            deferred_commands: MessageQueue::bounded(PENDING_CONTROL_CAPACITY),
            maintenance_ready_pending: false,
            durable_core_invalidated: false,
            runtime_shutdown: CancellationToken::new(),
            #[cfg(test)]
            active_take_observer: None,
        })
    }

    pub(crate) async fn run(mut self) -> SessionResult {
        let shutdown = CancellationToken::new();
        self.runtime_shutdown = shutdown.clone();
        match self.run_until_exit(&shutdown).await {
            Ok(SessionLoopExit::GatewayClosed) => {
                // Gateway EOF is terminal. Do not wait for an arbitrary
                // transport send to finish after the reader has closed.
                self.abort_writer().await;
                SessionResult::Completed(
                    self.core
                        .take()
                        .expect("clean idle exit retains the unique RunCore"),
                )
            }
            Ok(SessionLoopExit::ShutdownRequested) => {
                unreachable!("the private run token is never cancelled")
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
                    self.core.take().map_or(RunOwnership::Lost, |core| {
                        RunOwnership::Recovered(Box::new(core))
                    })
                };
                SessionResult::Failed { failure, ownership }
            }
        }
    }

    /// Install shutdown as an input to the Session event loop. No outer
    /// `select!` cancels `run_until_exit`, so mutation-heavy admission,
    /// persistence, completion, and ownership-transfer handlers always run to
    /// a boundary before shutdown is observed.
    pub(crate) async fn run_until_cancelled(
        mut self,
        shutdown: CancellationToken,
    ) -> SessionResult {
        self.runtime_shutdown = shutdown.clone();
        match self.run_until_exit(&shutdown).await {
            Ok(SessionLoopExit::GatewayClosed) => {
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
                    self.core.take().map_or(RunOwnership::Lost, |core| {
                        RunOwnership::Recovered(Box::new(core))
                    })
                };
                SessionResult::Failed { failure, ownership }
            }
            Ok(SessionLoopExit::ShutdownRequested) => {
                if self.active.is_some() {
                    if let Err(failure) = self.shutdown_active_gracefully().await {
                        self.abort_writer().await;
                        let ownership = if self.durable_core_invalidated {
                            self.core.take();
                            RunOwnership::Lost
                        } else {
                            self.core.take().map_or(RunOwnership::Lost, |core| {
                                RunOwnership::Recovered(Box::new(core))
                            })
                        };
                        return SessionResult::Failed { failure, ownership };
                    }
                }
                self.abort_writer().await;
                match self.core.take() {
                    Some(core) => SessionResult::Completed(core),
                    None => SessionResult::Failed {
                        failure: SessionFailure::RuntimeShutdownOwnershipLost,
                        ownership: RunOwnership::Lost,
                    },
                }
            }
        }
    }

    /// T26 forwards the maintainer's durable `MaintenanceReady` wake signal
    /// here.  The transition runs only while no worker owns the RunCore.
    pub(crate) async fn maintenance_ready(&mut self) -> Result<(), SessionFailure> {
        self.maintenance_ready_pending = true;
        if self.active.is_none() {
            self.apply_idle_memory_maintenance().await?;
        }
        Ok(())
    }

    async fn run_until_exit(
        &mut self,
        shutdown: &CancellationToken,
    ) -> Result<SessionLoopExit, SessionFailure> {
        loop {
            if self.active.is_none() {
                self.apply_idle_memory_maintenance().await?;
                enum IdleSelected {
                    Shutdown,
                    Command(Result<InboundCommand>),
                    Writer(std::result::Result<Result<()>, oneshot::error::RecvError>),
                }
                let selected = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => IdleSelected::Shutdown,
                    command = self.gateway_reader.next_command() => IdleSelected::Command(command),
                    writer = &mut self.writer_done => IdleSelected::Writer(writer),
                };
                let inbound = match selected {
                    IdleSelected::Shutdown => return Ok(SessionLoopExit::ShutdownRequested),
                    IdleSelected::Command(Ok(inbound)) => inbound,
                    IdleSelected::Command(Err(error))
                        if error.downcast_ref::<GatewayClosed>().is_some() =>
                    {
                        // Preserve a writer failure that won the race with
                        // EOF without waiting for a transport still in send.
                        self.gateway_closed_result(false)?;
                        return Ok(SessionLoopExit::GatewayClosed);
                    }
                    IdleSelected::Command(Err(error)) => {
                        return Err(gateway_failure("receive", error));
                    }
                    IdleSelected::Writer(writer) => return Err(writer_failure(writer)),
                };
                self.admit_and_route(inbound).await?;
                continue;
            }

            #[allow(clippy::large_enum_variant)]
            enum Selected {
                Shutdown,
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
                    _ = shutdown.cancelled() => Selected::Shutdown,
                    command = self.gateway_reader.next_command() => Selected::Command(command),
                    event = active.events_rx.recv() => Selected::Event(event),
                    writer = &mut self.writer_done => Selected::Writer(writer),
                }
            };

            match selected {
                Selected::Shutdown => return Ok(SessionLoopExit::ShutdownRequested),
                Selected::Completion(completion) => self.finish_run(completion).await?,
                Selected::Command(Ok(inbound)) => self.admit_and_route(inbound).await?,
                Selected::Command(Err(error))
                    if error.downcast_ref::<GatewayClosed>().is_some() =>
                {
                    self.gateway_closed_result(true)?;
                    return Ok(SessionLoopExit::GatewayClosed);
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
        if inbound.personality_agent_id() != &self.personality_agent_id
            || inbound.provenance().personality_agent_id() != &self.personality_agent_id
        {
            return Err(anyhow::anyhow!(
                "command target mismatch before Store admission: session={}, command={}, provenance={}",
                self.personality_agent_id,
                inbound.personality_agent_id(),
                inbound.provenance().personality_agent_id(),
            )
            .into());
        }
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
        self.enqueue_durable_events(receipt.events).await?;
        self.enqueue_reliable(vec![OutboundFrame::CommandAck { ack: ack.clone() }])
            .await?;
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
            if matches!(command.envelope().command, Command::ApprovalDecision { .. }) {
                if !self.deferred_commands.is_empty() {
                    self.defer_active_command(command)?;
                    return Ok(());
                }
                if !self.route_active_approval_decision(command.clone()).await? {
                    self.defer_active_command(command)?;
                }
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

    async fn await_active_control_acceptance(
        &mut self,
        phase_rx: &mut watch::Receiver<WorkerPhase>,
        accepted_rx: oneshot::Receiver<bool>,
    ) -> Result<bool, SessionFailure> {
        let mut accepted_rx = accepted_rx;
        let timeout = tokio::time::sleep(STEER_HANDSHAKE_TIMEOUT);
        tokio::pin!(timeout);
        loop {
            let progress = {
                let active = self
                    .active
                    .as_mut()
                    .ok_or(SessionFailure::CompletionChannelClosed)?;
                await_control_acceptance(
                    phase_rx,
                    &mut accepted_rx,
                    timeout.as_mut(),
                    &mut active.events_rx,
                )
                .await
            };
            match progress {
                ControlAcceptanceProgress::Accepted(accepted) => return Ok(accepted),
                ControlAcceptanceProgress::PhaseChanged | ControlAcceptanceProgress::TimedOut => {
                    return Ok(false);
                }
                ControlAcceptanceProgress::Event(Some(event)) => {
                    // Persistence can reclassify a deferred control and
                    // re-enter this wait, so this edge needs type indirection.
                    Box::pin(self.persist_active_event(event)).await?;
                }
                ControlAcceptanceProgress::Event(None) => {
                    self.resolve_closed_event_channel().await?;
                    return Ok(false);
                }
            }
        }
    }

    async fn route_hard_steer(&mut self, command: AdmittedCommand) -> Result<bool, SessionFailure> {
        let (mut phase_rx, accepted_rx) = {
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
            let phase_rx = active.phase_rx.clone();
            if active.control_tx.try_send(control).is_err() {
                return Ok(false);
            }
            (phase_rx, accepted_rx)
        };
        if !self
            .await_active_control_acceptance(&mut phase_rx, accepted_rx)
            .await?
        {
            return Ok(false);
        }
        let active = self
            .active
            .as_mut()
            .ok_or(SessionFailure::CompletionChannelClosed)?;
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
        let (committed_tx, committed_rx) = oneshot::channel();
        let (mut phase_rx, accepted_rx) = {
            let Some(active) = self.active.as_mut() else {
                return Ok(false);
            };
            if !active.bridge.can_bind_soft_steer(&self.writer, &command) {
                return Ok(false);
            }
            let (accepted_tx, accepted_rx) = oneshot::channel();
            let control = RunControl::SoftSteer {
                command: command.clone(),
                accepted: accepted_tx,
                committed: committed_rx,
            };
            let phase_rx = active.phase_rx.clone();
            if active.control_tx.try_send(control).is_err() {
                return Ok(false);
            }
            (phase_rx, accepted_rx)
        };
        if !self
            .await_active_control_acceptance(&mut phase_rx, accepted_rx)
            .await?
        {
            return Ok(false);
        }
        let active = self
            .active
            .as_mut()
            .ok_or(SessionFailure::CompletionChannelClosed)?;
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
        let (committed_tx, committed_rx) = oneshot::channel();
        let (mut phase_rx, accepted_rx) = {
            let Some(active) = self.active.as_mut() else {
                return Ok(false);
            };
            if !active.bridge.can_bind_abort() {
                return Ok(false);
            }
            let (accepted_tx, accepted_rx) = oneshot::channel();
            let control = RunControl::Abort {
                command: command.clone(),
                accepted: accepted_tx,
                committed: committed_rx,
            };
            let phase_rx = active.phase_rx.clone();
            if active.control_tx.try_send(control).is_err() {
                return Ok(false);
            }
            (phase_rx, accepted_rx)
        };
        if !self
            .await_active_control_acceptance(&mut phase_rx, accepted_rx)
            .await?
        {
            return Ok(false);
        }
        let active = self
            .active
            .as_mut()
            .ok_or(SessionFailure::CompletionChannelClosed)?;
        let (dispositions, mut acks) = active
            .bridge
            .bind_abort(&self.writer, command)
            .await
            .map_err(|error| SessionFailure::Worker(WorkerFailure::Error(error.to_string())))?;
        committed_tx.send(()).map_err(|_| {
            SessionFailure::Worker(WorkerFailure::Error(
                "abort worker exited before durability authorization".to_owned(),
            ))
        })?;
        self.send_committed(dispositions, None, Vec::new()).await?;
        let superseded: std::collections::HashSet<String> = acks
            .iter()
            .filter(|ack| ack.status == CommandAckStatus::Superseded)
            .map(|ack| ack.command_id.clone())
            .collect();
        for ack in acks.drain(..) {
            self.enqueue_reliable(vec![OutboundFrame::CommandAck { ack }])
                .await?;
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

    async fn route_active_approval_decision(
        &mut self,
        command: AdmittedCommand,
    ) -> Result<bool, SessionFailure> {
        let active = self
            .active
            .as_ref()
            .ok_or(SessionFailure::CompletionChannelClosed)?;
        let request_id = match &command.envelope().command {
            Command::ApprovalDecision { request_id, .. } => request_id,
            _ => unreachable!("caller matched ApprovalDecision"),
        };
        if active.resolving_approvals.contains(request_id)
            || active
                .approval
                .as_ref()
                .is_some_and(|broker| broker.is_resolving(request_id))
        {
            return Ok(false);
        }
        let is_pending = active
            .approval
            .as_ref()
            .is_some_and(|broker| broker.has_pending(request_id));
        if !is_pending {
            self.apply_idle_approval_decision(command).await?;
            return Ok(true);
        }
        let request_id = request_id.clone();
        let control = RunControl::Command(command);
        self.active
            .as_mut()
            .expect("active approval route retains its run")
            .resolving_approvals
            .insert(request_id);
        self.active
            .as_ref()
            .expect("active approval route retains its run")
            .control_tx
            .send(control)
            .await
            .map_err(|_| SessionFailure::CompletionChannelClosed)?;
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
            let was_user_message =
                matches!(command.envelope().command, Command::UserMessage { .. });
            let routed = match &command.envelope().command {
                Command::UserMessage { .. } => {
                    if self.route_retry_wait_command(&command).await? {
                        true
                    } else {
                        self.route_active_control(command.clone()).await?
                    }
                }
                Command::Abort {} => self.route_active_abort(command.clone()).await?,
                Command::ApprovalDecision { .. } => {
                    self.route_active_approval_decision(command.clone()).await?
                }
            };
            if !routed {
                self.deferred_commands
                    .push_front(command)
                    .map_err(anyhow::Error::from)?;
                break;
            }
            if was_user_message
                && self.deferred_commands.front().is_some_and(|next| {
                    matches!(next.envelope().command, Command::ApprovalDecision { .. })
                })
            {
                // The user message has just cancelled or superseded the
                // pending action. Wait for its durable ApprovalResolved before
                // applying a later decision as a terminal no-op.
                break;
            }
        }
        Ok(())
    }

    async fn apply_idle_approval_decision(
        &mut self,
        command: AdmittedCommand,
    ) -> Result<(), SessionFailure> {
        let command_id = command.envelope().command_id.to_string();
        let seq = command.envelope().seq;
        let request_id = match &command.envelope().command {
            Command::ApprovalDecision { request_id, .. } => request_id,
            _ => unreachable!("caller matched ApprovalDecision"),
        };

        let pending_broker = self.core.as_ref().and_then(|core| core.approval.clone());
        let pending_summary = pending_broker
            .as_ref()
            .and_then(|broker| broker.pending_summary(request_id));
        if let Some(summary) = pending_summary.as_ref() {
            // Keep the in-memory pending entry intact until this entire durable
            // cancellation batch commits. A failed transaction must leave both
            // the broker and approval_log/tool state pending for an idempotent
            // retry; only the successful commit may release its waiter.
            //
            // The late user decision itself is deliberately not part of this
            // batch. EventWriter correctly requires an active approval command
            // to resolve to that command's exact decision; this run has already
            // ended. Terminalize the abandoned request as a runtime cancellation
            // first, then apply the now-terminal command as a durable no-op.
            // In particular, a late ApproveAlways must not install a rule.
            let mut writes = Vec::with_capacity(4);

            let result_message = ToolResultMessage {
                tool_call_id: summary.tool_call_id.clone(),
                tool_name: summary.tool_name.clone(),
                content: vec![UserContent::Text {
                    text: "Approval decision arrived after the owning run ended; the tool was not started.".to_owned(),
                }],
                details: json!({"error": "approval_cancelled"}),
                is_error: true,
                timestamp: Utc::now(),
            };
            let result_value = serde_json::to_value(&result_message)
                .context("failed to serialize idle tool result")?;
            let message_id = format!("{}-idle-result", summary.tool_call_id);
            let tool_result = PublicMessage::ToolResult(result_message);

            writes.push(EventWrite {
                event: Some(DurableEvent::tool_execution_end(
                    summary.tool_call_id.clone(),
                    result_value,
                    true,
                    "cancelled".to_owned(),
                    Some("approval_cancelled".to_owned()),
                )?),
                projections: vec![Projection::ToolExecution(ToolExecutionMutation::Finish {
                    tool_call_id: summary.tool_call_id.clone(),
                    expected: "prepared",
                    state: "cancelled",
                    error_code: Some("approval_cancelled"),
                })],
            });
            writes.push(EventWrite {
                event: Some(DurableEvent::message(
                    "message_start",
                    &message_id,
                    &tool_result,
                )?),
                projections: vec![],
            });
            writes.push(EventWrite {
                event: Some(DurableEvent::message(
                    "message_end",
                    &message_id,
                    &tool_result,
                )?),
                projections: vec![Projection::MessageEnd {
                    message_id,
                    role: "tool_result",
                    message: tool_result,
                    append_to_l0: true,
                    provider_context: Vec::new(),
                    eviction_footprint_tokens: 0,
                }],
            });
            writes.push(EventWrite {
                event: Some(DurableEvent::approval_resolved(
                    request_id.clone(),
                    ApprovalResolution::Cancelled,
                    "runtime".to_owned(),
                )?),
                projections: vec![Projection::Approval(ApprovalMutation::Resolve {
                    request_id: request_id.clone(),
                    state: "cancelled",
                    actor: "runtime".to_owned(),
                })],
            });
            if let Err(error) = self
                .writer
                .apply(EventBatch {
                    writes,
                    injected_commands: Vec::new(),
                })
                .await
            {
                tracing::error!(%error, %command_id, "idle approval cancellation could not be committed");
                return Err(error.into());
            }

            pending_broker
                .as_ref()
                .expect("pending summary has its broker")
                .cancel(request_id);
        }

        // Once the cancellation transaction is durable, this command cannot
        // carry ApprovalResolved and is validated as the intended terminal
        // no-op. Retrying after a failure here is safe: the broker and durable
        // approval/tool state already agree on the cancellation.
        let disposition_events = self
            .writer
            .apply_with_events(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandApplied {
                        command_id: command_id.clone(),
                        command_seq: seq,
                        run_id: None,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .map_err(|error| {
                tracing::error!(%error, %command_id, "approval decision could not be applied");
                SessionFailure::from(error)
            })?;
        self.enqueue_durable_events(disposition_events).await?;
        let ack = CommandAck {
            seq,
            command_id,
            personality_agent_id: self.personality_agent_id.clone(),
            status: CommandAckStatus::Applied,
            reject_reason: None,
        };
        self.enqueue_reliable(vec![OutboundFrame::CommandAck { ack }])
            .await?;
        Ok(())
    }

    async fn route_idle(&mut self, command: AdmittedCommand) -> Result<(), SessionFailure> {
        self.apply_idle_memory_maintenance().await?;
        if matches!(command.envelope().command, Command::Abort {}) {
            let terminal = self
                .writer
                .apply_idle_abort_cutoff_with_events(
                    command.envelope().command_id.as_str(),
                    command.envelope().seq,
                )
                .await?;
            self.enqueue_durable_events(terminal.events).await?;
            for ack in terminal.acks {
                self.enqueue_reliable(vec![OutboundFrame::CommandAck { ack }])
                    .await?;
            }
            return Ok(());
        }
        if matches!(command.envelope().command, Command::ApprovalDecision { .. }) {
            return self.apply_idle_approval_decision(command).await;
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
        core.runtime_shutdown = self.runtime_shutdown.child_token();
        let approval = core.approval.clone();
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
            approval,
            resolving_approvals: HashSet::new(),
        });
        Ok(())
    }

    async fn finish_run(
        &mut self,
        completion: std::result::Result<RunCompletion, oneshot::error::RecvError>,
    ) -> std::result::Result<(), SessionFailure> {
        self.finish_run_with_route(completion, true).await
    }

    fn install_run_completion_ownership(
        &mut self,
        completion: RunCompletion,
    ) -> Option<WorkerFailure> {
        match completion {
            RunCompletion::Completed(core) => {
                self.core = Some(core);
                None
            }
            RunCompletion::Failed { core, failure } => {
                self.core = Some(core);
                Some(failure)
            }
            RunCompletion::RehydrationRequired { failure } => {
                self.core = None;
                self.durable_core_invalidated = true;
                Some(failure)
            }
        }
    }

    async fn finish_run_with_route(
        &mut self,
        completion: std::result::Result<RunCompletion, oneshot::error::RecvError>,
        route_after_completion: bool,
    ) -> std::result::Result<(), SessionFailure> {
        // A successful completion transfer carries the unique RunCore or a
        // durable invalidation verdict. Install that ownership state before
        // the first await, while retaining ActiveRun through join and drain.
        let worker_failure = match completion {
            Ok(completion) => self.install_run_completion_ownership(completion),
            Err(_) => {
                let join_result = {
                    let active = self
                        .active
                        .as_mut()
                        .expect("completion requires active run");
                    (&mut active.join).await
                };
                self.active.take();
                return match join_result {
                    Err(error) => Err(worker_join_failure(error)),
                    Ok(()) => Err(SessionFailure::CompletionChannelClosed),
                };
            }
        };
        let join_result = {
            let active = self
                .active
                .as_mut()
                .expect("completion requires active run");
            (&mut active.join).await
        };
        if let Err(error) = join_result {
            self.active.take();
            return Err(worker_join_failure(error));
        }
        let delivery_failure = match self.drain_active_outputs(route_after_completion).await {
            Ok(failure) => failure,
            Err(error) => {
                // The joined worker cannot be awaited twice. The durable
                // invalidation flag prevents the returned-but-ahead core from
                // being reported as recovered.
                self.active.take();
                return Err(error);
            }
        };
        // A completed RunCore includes every output already produced by the
        // worker. Do not expose it until the disconnected bounded event lane
        // has been drained into SQLite, even when Gateway delivery was lost.
        self.active.take();
        #[cfg(test)]
        if let Some(observer) = self.active_take_observer.take() {
            let _ = observer.send(self.core.is_some());
        }
        if let Some(failure) = delivery_failure {
            return Err(failure);
        }
        match worker_failure {
            Some(failure) => Err(SessionFailure::Worker(failure)),
            None => {
                if route_after_completion {
                    self.apply_idle_memory_maintenance().await?;
                    self.route_deferred_after_run().await
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn apply_idle_memory_maintenance(&mut self) -> Result<(), SessionFailure> {
        let Some(core) = self.core.as_mut() else {
            return Ok(());
        };
        if core.pending_overflow_apply().is_none() && !self.maintenance_ready_pending {
            return Ok(());
        }
        let applied = self
            .worker
            .apply_idle_memory_maintenance(core)
            .await
            .map_err(|error| SessionFailure::Worker(WorkerFailure::Error(error.to_string())))?;
        if applied {
            core.clear_pending_overflow_apply();
            self.maintenance_ready_pending = false;
            return Ok(());
        }
        Err(SessionFailure::Worker(WorkerFailure::Error(
            "idle memory maintenance is pending but did not commit a refreshed transition"
                .to_owned(),
        )))
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
            let terminal = self
                .writer
                .apply_idle_abort_cutoff_with_events(
                    abort.envelope().command_id.as_str(),
                    abort_seq,
                )
                .await?;
            self.enqueue_durable_events(terminal.events).await?;
            for ack in terminal.acks {
                self.enqueue_reliable(vec![OutboundFrame::CommandAck { ack }])
                    .await?;
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
        if self.active.is_none() {
            return;
        }
        tokio::task::yield_now().await;
        let completion = self
            .active
            .as_mut()
            .expect("active run checked above")
            .completion_rx
            .try_recv();
        match completion {
            Ok(completion) => {
                // Consume the completion's RunCore or invalidation verdict
                // synchronously before joining the task that published it.
                let _ = self.install_run_completion_ownership(completion);
                let join_result = {
                    let active = self.active.as_mut().expect("active run checked above");
                    (&mut active.join).await
                };
                if join_result.is_err() {
                    self.active.take();
                    return;
                }
                // The caller already holds the primary Session failure. Commit
                // the suffix, but do not re-enter a failed Gateway during shutdown.
                match self.drain_active_outputs(false).await {
                    Ok(_) => {
                        self.active.take();
                    }
                    Err(_) => {
                        self.active.take();
                        self.durable_core_invalidated = true;
                    }
                }
            }
            Err(oneshot::error::TryRecvError::Closed) => {
                {
                    let active = self.active.as_mut().expect("active run checked above");
                    let _ = (&mut active.join).await;
                }
                self.active.take();
            }
            Err(oneshot::error::TryRecvError::Empty) => {
                {
                    let active = self.active.as_mut().expect("active run checked above");
                    active.join.abort();
                    let _ = (&mut active.join).await;
                }
                self.active.take();
            }
        }
    }

    async fn shutdown_active_gracefully(&mut self) -> Result<(), SessionFailure> {
        if self.active.is_none() {
            return Ok(());
        }
        self.runtime_shutdown.cancel();
        if let Some(approval) = self
            .active
            .as_ref()
            .and_then(|active| active.approval.as_ref())
        {
            approval.cancel_all();
        }
        let deadline = tokio::time::Instant::now() + RUNTIME_SHUTDOWN_GRACE;
        match tokio::time::timeout_at(deadline, self.settle_active_shutdown()).await {
            Ok(result) => result,
            Err(_) => {
                // The grace period is absolute: it includes Store commits,
                // worker settlement, output draining, and every other await in
                // the graceful path. Cancellation of any of those operations
                // makes the durable/worker boundary indeterminate. Abort and
                // settle the exact retained task before reporting lost RunCore
                // ownership; the two ownership questions are distinct.
                self.abort_and_join_active_worker_or_fail_stop().await;
                self.active.take();
                self.core.take();
                self.durable_core_invalidated = true;
                Err(SessionFailure::RuntimeShutdownOwnershipLost)
            }
        }
    }

    async fn abort_and_join_active_worker_or_fail_stop(&mut self) {
        let joined = {
            let active = self
                .active
                .as_mut()
                .expect("shutdown timeout requires an active run");
            active.join.abort();
            tokio::time::timeout(RUNTIME_SHUTDOWN_ABORT_JOIN_TIMEOUT, &mut active.join).await
        };
        if joined.is_err() {
            // Returning would drop the JoinHandle and detach a task that may
            // still own RunCore. The durable outcome is already
            // indeterminate, but only a process fail-stop preserves the
            // single-owner invariant when task settlement misses its bound.
            tracing::error!(
                timeout_millis = RUNTIME_SHUTDOWN_ABORT_JOIN_TIMEOUT.as_millis(),
                "aborted active run did not join within its ownership bound"
            );
            std::process::abort();
        }
    }

    async fn settle_active_shutdown(&mut self) -> Result<(), SessionFailure> {
        let mut events_open = true;
        loop {
            enum ShutdownSelected {
                Completion(std::result::Result<RunCompletion, oneshot::error::RecvError>),
                Event(Option<RunOutput>),
            }
            let selected = {
                let active = self.active.as_mut().expect("active run checked above");
                tokio::select! {
                    biased;
                    completion = &mut active.completion_rx => {
                        ShutdownSelected::Completion(completion)
                    }
                    event = active.events_rx.recv(), if events_open => {
                        ShutdownSelected::Event(event)
                    }
                }
            };
            match selected {
                ShutdownSelected::Completion(completion) => {
                    return match self.finish_run_with_route(completion, false).await {
                        Err(SessionFailure::Worker(WorkerFailure::Cancelled))
                            if self.core.is_some() =>
                        {
                            Ok(())
                        }
                        result => result,
                    };
                }
                ShutdownSelected::Event(Some(output)) => {
                    self.persist_shutdown_event(output).await?;
                }
                ShutdownSelected::Event(None) => {
                    // Completion publication immediately follows sender drop.
                    // Disable the now-always-ready branch so the bounded
                    // deadline remains observable.
                    events_open = false;
                }
            }
        }
    }

    async fn drain_active_outputs(
        &mut self,
        deliver: bool,
    ) -> Result<Option<SessionFailure>, SessionFailure> {
        let mut delivery_failure = None;
        loop {
            let output = {
                let active = self.active.as_mut().expect("drain requires active run");
                active.events_rx.try_recv()
            };
            let Ok(output) = output else {
                break;
            };
            let committed = {
                let active = self.active.as_mut().expect("drain requires active run");
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
            self.active
                .as_mut()
                .expect("drain retains active run")
                .finish_committed_approval_resolutions(&outputs);
            if deliver && delivery_failure.is_none() {
                let command_id = self
                    .active
                    .as_ref()
                    .expect("drain retains active run")
                    .bridge
                    .command_id()
                    .to_owned();
                delivery_failure = self
                    .send_committed(outputs, Some(command_id), terminal_command_ids)
                    .await
                    .err();
            }
        }
        Ok(delivery_failure)
    }

    async fn persist_shutdown_event(&mut self, output: RunOutput) -> Result<(), SessionFailure> {
        let committed = {
            let active = self
                .active
                .as_mut()
                .expect("shutdown event requires active run");
            match active.bridge.commit(&self.writer, output).await {
                Ok(committed) => committed,
                Err(error) => {
                    self.durable_core_invalidated = true;
                    return Err(error.into());
                }
            }
        };
        let (outputs, tool_start_barrier, retry_wait_commit_barrier, _) =
            committed.resolve_message_receipts();
        if let Some(barrier) = tool_start_barrier {
            barrier.committed();
        }
        if let Some(barrier) = retry_wait_commit_barrier {
            barrier.committed();
        }
        self.active
            .as_mut()
            .expect("shutdown event retains active run")
            .finish_committed_approval_resolutions(&outputs);
        Ok(())
    }

    async fn persist_active_event(&mut self, output: RunOutput) -> Result<(), SessionFailure> {
        // Approved decisions are intentionally staged until the matching
        // ToolExecutionStart. Even though that ApprovalResolved produces no
        // public output yet, it is still a routing boundary: a queued steer or
        // Abort must get the chance to win before the tool start commits.
        let staged_approval_boundary = matches!(&output.event, AgentEvent::ApprovalResolved { .. });
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
        self.active
            .as_mut()
            .expect("committed event retains active run")
            .finish_committed_approval_resolutions(&outputs);
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
        let approval_boundary = staged_approval_boundary
            || outputs.iter().any(|output| {
                matches!(
                    &output.event,
                    AgentEvent::ApprovalRequested { .. } | AgentEvent::ApprovalResolved { .. }
                )
            });
        let command_id = self
            .active
            .as_ref()
            .map(|active| active.bridge.command_id().to_owned());
        self.send_committed(outputs, command_id, terminal_command_ids)
            .await?;
        if assistant_started || approval_boundary {
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
                    personality_agent_id: self.personality_agent_id.clone(),
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
                self.enqueue_reliable(frames).await?;
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

    async fn enqueue_reliable(&mut self, frames: Vec<OutboundFrame>) -> Result<(), SessionFailure> {
        for frame in &frames {
            let frame_personality_agent_id = match frame {
                OutboundFrame::Event { envelope } => &envelope.personality_agent_id,
                OutboundFrame::CommandAck { ack } => &ack.personality_agent_id,
            };
            if frame_personality_agent_id != &self.personality_agent_id {
                return Err(anyhow::anyhow!(
                    "outbound frame personality-agent mismatch: session={}, frame={}",
                    self.personality_agent_id,
                    frame_personality_agent_id,
                )
                .into());
            }
        }
        let outbound = self.outbound_handle()?.clone();
        match outbound.enqueue_reliable_wait(frames).await {
            Err(SessionFailure::OutboundClosed) => match self.writer_done.try_recv() {
                Ok(result) => result.map_err(|error| gateway_failure("send", error)),
                Err(oneshot::error::TryRecvError::Closed) => Err(SessionFailure::OutboundClosed),
                Err(oneshot::error::TryRecvError::Empty) => Err(SessionFailure::OutboundClosed),
            },
            result => result,
        }
    }

    async fn enqueue_durable_events(
        &mut self,
        events: Vec<(u64, AgentEvent)>,
    ) -> Result<(), SessionFailure> {
        let frames = events
            .into_iter()
            .map(|(seq, event)| {
                Ok(OutboundFrame::Event {
                    envelope: crate::gateway::Envelope {
                        seq: Some(seq),
                        personality_agent_id: self.personality_agent_id.clone(),
                        event: serde_json::to_value(event).map_err(anyhow::Error::from)?,
                    },
                })
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;
        if !frames.is_empty() {
            self.enqueue_reliable(frames).await?;
        }
        Ok(())
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

#[allow(
    clippy::large_enum_variant,
    reason = "the one-iteration select result moves RunOutput directly without per-event allocation"
)]
enum ControlAcceptanceProgress {
    Accepted(bool),
    PhaseChanged,
    TimedOut,
    Event(Option<RunOutput>),
}

async fn await_control_acceptance(
    phase_rx: &mut watch::Receiver<WorkerPhase>,
    accepted_rx: &mut oneshot::Receiver<bool>,
    mut timeout: Pin<&mut tokio::time::Sleep>,
    events_rx: &mut mpsc::Receiver<RunOutput>,
) -> ControlAcceptanceProgress {
    tokio::select! {
        biased;
        accepted = &mut *accepted_rx => {
            ControlAcceptanceProgress::Accepted(accepted.unwrap_or(false))
        }
        changed = phase_rx.changed() => {
            let _ = changed;
            ControlAcceptanceProgress::PhaseChanged
        }
        // Keep the original bounded wait authoritative even when the worker
        // continuously produces ready events.
        () = timeout.as_mut() => ControlAcceptanceProgress::TimedOut,
        event = events_rx.recv() => ControlAcceptanceProgress::Event(event),
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
// The binary test composition root calls the real live harness through this
// re-export. The library mirror intentionally supplies a non-live stub, so the
// same re-export is unused only in that test target.
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use session_tests::run_canonical_live_responses_roundtrip;
