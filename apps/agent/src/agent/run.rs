//! Sequential provider/tool lifecycle for one active run.
//!
//! Durable command phase transitions remain owned by `Session`/`EventWriter`.
//! This module owns only the in-memory lifecycle after an admitted user command
//! has been transferred together with the unique [`RunCore`].

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    gateway::Command,
    memory::{
        context_assembler::ProviderCallTrigger,
        estimate::{
            ProviderContextItemWithFootprint, eviction_footprint_for_payload,
            observed_prompt_tokens,
        },
    },
    provider::{
        model::ModelSpec,
        overflow::{OverflowClassification, OverflowSource, classify_context_overflow},
        retry::{is_retryable, retry_delay, sleep_or_cancel},
        types::{
            AssistantContent, AssistantMessage, ContextMessage, Message, ProviderContextFragment,
            ProviderEvent, ProviderEventStream, PublicAssistantContent, PublicMessage, StopReason,
            ToolCall, ToolResultMessage, UserContent, UserMessage,
        },
    },
    runtime::contracts::{ProcessGeneration, RpcIdentity},
    store::user_message_id,
    tools::ToolError,
};

use super::{
    AdmittedCommand, AgentEvent, DurableRunBinding, MessageCommitBarrier, MessageCommitReceipt,
    ProjectedProviderEvent, ProviderEventProjector, ProviderTerminalKind, RetryWaitCommitBarrier,
    RunCompletion, RunControl, RunCore, RunOutput, RunWorker, SteerMode, ToolStartCommitBarrier,
    ToolStartCommitResult, WorkerFailure, WorkerFuture, WorkerPhase, steer,
};
use crate::approval::{ApprovalOutcome, ExecutableGrant, WaiterResult};

const LENGTH_TOOL_FAILURE: &str = "Tool call was not executed: the response hit the output token limit, so its arguments may be truncated. Re-issue the tool call with complete arguments.";
const LENGTH_LOOP_FAILURE: &str = "provider produced tool calls at the output token limit twice consecutively; refusing a third provider call";
pub(super) const LENGTH_LOOP_CODE: &str = "consecutive_length_tool_guard";
const LENGTH_OVERFLOW_ERROR: &str = "provider response reached the context window before producing output; immediate recovery required";
const LENGTH_OVERFLOW_CODE: &str = "context_overflow_length_usage";
const MAX_OVERFLOW_RECOVERIES: u8 = 2;
pub(super) const TOOL_RESULT_MESSAGE_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x73, 0x75, 0x6d, 0x69, 0xa4, 0xc1, 0x48, 0x22, 0x91, 0x5d, 0xb5, 0xd2, 0x5a, 0x69, 0x9f, 0x31,
]);
const SYNTHETIC_ATTEMPT_MESSAGE_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x94, 0x76, 0x9e, 0x72, 0xc9, 0x5b, 0x4d, 0xa8, 0x9c, 0x59, 0x8e, 0x36, 0xa2, 0x53, 0xa1, 0x70,
]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OverflowRecoveryRequest {
    pub(crate) source: OverflowSource,
    pub(crate) ordinal: u8,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum OverflowRecoveryOutcome {
    ReplacementContext(Vec<ContextMessage>),
    /// The driver proved that a changed bounded send view exists, but the
    /// canonical in-memory transcript must remain intact. The next provider
    /// attempt derives that view again from the retained life log.
    RetainCanonicalContext {
        validated_send_view: Vec<ContextMessage>,
    },
}

/// Result of attempting to durably commit a `ToolExecutionStart`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ToolStartOutcome {
    /// The start was durably committed and the tool may execute.
    Started,
    /// A control arrived before the tool could durably start; the start must not
    /// be committed and the current call should be skipped.
    Preempted,
    /// The signed-policy authority changed or expired before durable start.
    /// The same call must pass through the broker again.
    Reauthorize,
}

/// One provider attempt. The initial public message supplies stable model and
/// origin metadata for `MessageStart`; the stream remains the authority for
/// the terminal message.
pub(crate) struct ProviderAttempt {
    /// Stable, conversation-global durable message identity. Reusing an ID in
    /// another run would collide with the Store's globally keyed message row.
    pub(crate) message_id: String,
    pub(crate) initial_message: PublicMessage,
    /// Uncalibrated prompt-token estimate produced by the assembler before the
    /// provider call.  This is the denominator for the calibration EMA.
    pub(crate) uncalibrated_prompt_estimate: u64,
    pub(crate) events: ProviderEventStream,
}

/// Ordinals and context-assembly boundary for one provider call.
///
/// `attempt_sequence` remains run-global for durable attempt identity, while
/// `user_turn_attempt` restarts only after a newly injected user message.
/// `trigger` independently distinguishes the first call after that message
/// from retries and tool continuations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ProviderCallAttempt {
    pub(crate) attempt_sequence: usize,
    pub(crate) user_turn_attempt: usize,
    pub(crate) trigger: ProviderCallTrigger,
}

/// Narrow runtime boundary. Production wiring may build provider context from
/// the supplied snapshot and dispatch tools through the existing executor;
/// unit fixtures can remain transport- and credential-free.
#[async_trait]
pub(crate) trait RunDriver: Send + Sync + 'static {
    /// Fail closed unless the driver can prove that its immutable executor
    /// client is bound to the exact authenticated runtime identity.
    fn validate_runtime_identity(&self, _identity: &RpcIdentity) -> Result<()> {
        Err(anyhow::anyhow!(
            "run driver is not bound to a production executor RPC identity"
        ))
    }

    /// Fail closed before Session creates keys, recovery state, or a worker.
    /// This narrower check exists for unhydrated test fixtures only.
    fn validate_executor_generation(&self, generation: ProcessGeneration) -> Result<()>;

    /// T21 idle maintenance must return true only after its durable transition
    /// and ContextAssembler refresh have both completed.
    async fn apply_idle_memory_maintenance(&self, _core: &mut RunCore) -> Result<bool> {
        Err(anyhow::anyhow!(
            "idle memory maintenance is not wired to the authoritative Store/EventWriter path"
        ))
    }

    async fn start_provider_for_command(
        &self,
        attempt: usize,
        context: &[ContextMessage],
        command_received_at: Option<std::time::Instant>,
        cancel: CancellationToken,
    ) -> Result<ProviderAttempt>;

    async fn start_provider_with_context(
        &self,
        attempt: usize,
        context: &[ContextMessage],
        _provider_context: &[ProviderContextItemWithFootprint],
        _trigger: ProviderCallTrigger,
        command_received_at: Option<std::time::Instant>,
        cancel: CancellationToken,
    ) -> Result<ProviderAttempt> {
        self.start_provider_for_command(attempt, context, command_received_at, cancel)
            .await
    }

    /// Starts a provider request while keeping the Runner-global attempt
    /// sequence distinct from the retry ordinal of the active user turn.
    /// Drivers that derive request policy from the latter can override this;
    /// the default preserves the established global attempt identity.
    async fn start_provider_for_user_turn(
        &self,
        call: ProviderCallAttempt,
        context: &[ContextMessage],
        provider_context: &[ProviderContextItemWithFootprint],
        command_received_at: Option<std::time::Instant>,
        cancel: CancellationToken,
    ) -> Result<ProviderAttempt> {
        self.start_provider_with_context(
            call.attempt_sequence,
            context,
            provider_context,
            call.trigger,
            command_received_at,
            cancel,
        )
        .await
    }

    async fn execute_tool_observed(
        &self,
        flow_id: &str,
        call: &ToolCall,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<ToolResultMessage, ToolError>;

    fn synthetic_error(&self, message: &str) -> PublicMessage;

    fn context_window(&self) -> Option<u64> {
        None
    }

    /// Seed the driver's `ContextAssembler` with the hydrated provider context
    /// carried in the `RunCore`. Default is a no-op for test drivers that do
    /// not use the T21 assembler.
    fn set_hydrated_provider_context(
        &self,
        _provider_context: Vec<ProviderContextItemWithFootprint>,
    ) {
    }

    /// Plans one bounded emergency recovery without mutating runtime state.
    /// There is intentionally no default. Implementations must be side-effect
    /// free; the runner validates the plan and installs it after scheduling.
    async fn plan_overflow_recovery(
        &self,
        core: &RunCore,
        request: OverflowRecoveryRequest,
        active_context: &[ContextMessage],
    ) -> Result<OverflowRecoveryOutcome>;

    /// Install the exact token calibration committed with a successful
    /// provider terminal. Default is a no-op for test drivers that do not use
    /// the T21 assembler.
    fn install_committed_calibration(&self, _ratio_bits: [u8; 8]) -> Result<()> {
        Ok(())
    }

    /// Apply the durable results of a terminal assistant turn.  The runner
    /// supplies the authoritative assistant message, its durable identity, and
    /// the opaque provider-context fragments generated for the turn.
    async fn apply_terminal(
        &self,
        _message_id: &str,
        _message_seq: u64,
        _message: &AssistantMessage,
        _provider_context: &[ProviderContextFragment],
    ) -> Result<()> {
        Ok(())
    }

    async fn wait_retry(&self, delay: Duration, cancel: &CancellationToken) -> bool {
        sleep_or_cancel(delay, cancel).await
    }
}

/// `RunWorker` implementation that never overlaps provider attempts or tool
/// calls. Every recoverable runtime failure is converted to canonical events;
/// only loss of the event consumer escapes as `RunCompletion::Failed`.
pub(crate) struct SequentialRunWorker {
    driver: Arc<dyn RunDriver>,
}

impl SequentialRunWorker {
    pub(crate) fn new(driver: Arc<dyn RunDriver>) -> Self {
        Self { driver }
    }
}

impl RunWorker for SequentialRunWorker {
    fn validate_runtime_identity(&self, identity: &RpcIdentity) -> Result<()> {
        self.driver.validate_runtime_identity(identity)
    }

    fn validate_executor_generation(&self, generation: ProcessGeneration) -> Result<()> {
        self.driver.validate_executor_generation(generation)
    }

    fn apply_idle_memory_maintenance<'a>(
        &'a self,
        core: &'a mut RunCore,
    ) -> Pin<Box<dyn Future<Output = Result<bool>> + Send + 'a>> {
        Box::pin(async move { self.driver.apply_idle_memory_maintenance(core).await })
    }

    fn run(
        &self,
        core: RunCore,
        initial: AdmittedCommand,
        controls: mpsc::Receiver<RunControl>,
        events: mpsc::Sender<RunOutput>,
    ) -> WorkerFuture {
        let driver = self.driver.clone();
        let provider_context = core.provider_context.clone();
        Box::pin(async move {
            driver.set_hydrated_provider_context(provider_context);
            Runner::new(core, driver, controls, events)
                .run(initial)
                .await
        })
    }
}

struct Runner {
    core: RunCore,
    driver: Arc<dyn RunDriver>,
    controls: mpsc::Receiver<RunControl>,
    events: mpsc::Sender<RunOutput>,
    phase: watch::Sender<WorkerPhase>,
    context: Vec<ContextMessage>,
    provider_context: Vec<ProviderContextItemWithFootprint>,
    pending_provider_context: HashMap<
        String,
        (
            crate::provider::types::ProviderOrigin,
            Vec<ProviderContextFragment>,
        ),
    >,
    /// Runner-global provider ordinal used for durable attempt identity.
    attempt_sequence: usize,
    /// Provider ordinal within the latest injected user turn. Drivers use this
    /// to scope retry-only request transformations without reusing identities.
    user_turn_attempt: usize,
    ordinary_retries: usize,
    overflow_recoveries: u8,
    consecutive_length_batches: usize,
    in_flight_controls: Vec<AdmittedCommand>,
    pending_command_received_at: Option<std::time::Instant>,
    first_provider_call_after_user: bool,
    provider_cancel: Option<CancellationToken>,
    hard_steer_command: Option<AdmittedCommand>,
    abort_requested: bool,
    /// Run-wide cancellation token. Dropping the runner cancels all child
    /// operations, including in-flight reviewer calls and tool executions.
    cancel: CancellationToken,
    /// A successful MessageEnd receipt means the authoritative life log is
    /// ahead of RunCore until every terminal side effect and replay-state
    /// retention step succeeds. A failure in this interval must discard the
    /// core and force hydration instead of returning stale recoverable state.
    durable_terminal_pending: bool,
}

#[derive(Debug, thiserror::Error)]
enum ExecuteToolError {
    #[error("tool execution failed: {0}")]
    Tool(ToolError),
    #[error(transparent)]
    Worker(WorkerFailure),
    #[error("tool execution cancelled by a control")]
    Cancelled,
}

impl From<WorkerFailure> for ExecuteToolError {
    fn from(failure: WorkerFailure) -> Self {
        Self::Worker(failure)
    }
}

enum CallDisposition {
    Allowed {
        grant: Option<ExecutableGrant>,
    },
    Denied {
        reason: String,
        approval_denied: bool,
    },
    Pending {
        pending: crate::approval::broker::PendingApproval,
    },
}

enum ApprovalWaitOutcome {
    Resolved {
        decision: crate::approval::policy::ResolvedDecision,
        command: Box<AdmittedCommand>,
    },
    Cancelled,
}

impl Runner {
    fn new(
        mut core: RunCore,
        driver: Arc<dyn RunDriver>,
        controls: mpsc::Receiver<RunControl>,
        events: mpsc::Sender<RunOutput>,
    ) -> Self {
        let phase = core
            .worker_phase
            .take()
            .unwrap_or_else(|| watch::channel(WorkerPhase::Active).0);
        let context = std::mem::take(&mut core.runtime_context);
        let provider_context = std::mem::take(&mut core.provider_context);
        let cancel = core.runtime_shutdown.child_token();
        Self {
            core,
            driver,
            controls,
            events,
            phase,
            context,
            provider_context,
            pending_provider_context: HashMap::new(),
            attempt_sequence: 0,
            user_turn_attempt: 0,
            ordinary_retries: 0,
            overflow_recoveries: 0,
            consecutive_length_batches: 0,
            in_flight_controls: Vec::new(),
            pending_command_received_at: None,
            first_provider_call_after_user: false,
            provider_cancel: None,
            hard_steer_command: None,
            abort_requested: false,
            cancel,
            durable_terminal_pending: false,
        }
    }

    async fn run(mut self, initial: AdmittedCommand) -> RunCompletion {
        let mut result = match self.claim_ordered_initial(initial) {
            Ok(()) => self.run_inner().await,
            Err(failure) => Err(failure),
        };
        if let Err(failure) = self.recover_received_controls() {
            result = Err(failure);
        }
        self.core.runtime_context = std::mem::take(&mut self.context);
        self.core.provider_context = std::mem::take(&mut self.provider_context);
        self.core.mark_mutated();
        match result {
            Ok(()) if !self.durable_terminal_pending => {
                RunCompletion::Completed(std::mem::take(&mut self.core))
            }
            Ok(()) => RunCompletion::RehydrationRequired {
                failure: WorkerFailure::Error(
                    "run completed with an unreconciled durable assistant terminal".to_owned(),
                ),
            },
            Err(failure) if self.durable_terminal_pending => {
                RunCompletion::RehydrationRequired { failure }
            }
            Err(failure) => RunCompletion::Failed {
                core: std::mem::take(&mut self.core),
                failure,
            },
        }
    }

    fn claim_ordered_initial(&mut self, initial: AdmittedCommand) -> Result<(), WorkerFailure> {
        self.core
            .queue_followup(initial)
            .map_err(|error| WorkerFailure::Error(error.to_string()))?;
        let oldest = self
            .core
            .next_followup()
            .expect("newly queued initial makes pending controls non-empty");
        if matches!(oldest.envelope().command, Command::UserMessage { .. }) {
            self.claim_control(oldest)
        } else {
            self.core
                .requeue_followup_front(oldest)
                .map_err(|error| WorkerFailure::Error(error.to_string()))?;
            Err(WorkerFailure::Error(
                "pending T16 control must be applied before a later run can start".to_owned(),
            ))
        }
    }

    async fn run_inner(&mut self) -> Result<(), WorkerFailure> {
        self.emit(AgentEvent::AgentStart).await?;
        self.emit(AgentEvent::TurnStart).await?;
        self.inject_in_flight().await?;

        loop {
            self.receive_control_safe_point().await?;
            let outcome = self.provider_attempt().await?;
            self.attempt_sequence = self.attempt_sequence.saturating_add(1);
            self.user_turn_attempt = self.user_turn_attempt.saturating_add(1);
            match outcome {
                AttemptOutcome::Retry {
                    assistant_message_id,
                    message,
                    receipt,
                    rejected_results,
                } => {
                    let receipts = self
                        .emit_rejected_results(&assistant_message_id, &rejected_results)
                        .await?;
                    self.retain_tool_results(&receipts, &rejected_results)?;
                    self.await_message_receipt(receipt).await?;
                    self.consecutive_length_batches = 0;
                    let Some(delay) = retry_delay(self.ordinary_retries) else {
                        self.close_turn(message, Vec::new()).await?;
                        break;
                    };
                    self.ordinary_retries += 1;
                    let _ = self.phase.send(WorkerPhase::RetryWait);
                    if let Err(failure) = self
                        .emit_retry_scheduled(
                            self.attempt_sequence as u32,
                            delay,
                            assistant_error(&message),
                        )
                        .await
                    {
                        let _ = self.phase.send(WorkerPhase::Active);
                        return Err(failure);
                    }
                    let injected = match self.wait_retry_or_control(delay).await {
                        Ok(injected) => injected,
                        Err(WorkerFailure::Cancelled) if self.abort_requested => {
                            self.in_flight_controls.clear();
                            self.close_turn(message, Vec::new()).await?;
                            break;
                        }
                        Err(failure) => {
                            let _ = self.phase.send(WorkerPhase::Active);
                            return Err(failure);
                        }
                    };
                    let _ = self.phase.send(WorkerPhase::Active);
                    if injected {
                        self.emit(AgentEvent::Steered {
                            mode: SteerMode::Soft,
                        })
                        .await?;
                        self.inject_in_flight().await?;
                    }
                }
                AttemptOutcome::ImmediateOverflow {
                    assistant_message_id,
                    message,
                    receipt,
                    source,
                    rejected_results,
                } => {
                    let receipts = self
                        .emit_rejected_results(&assistant_message_id, &rejected_results)
                        .await?;
                    self.retain_tool_results(&receipts, &rejected_results)?;
                    self.await_message_receipt(receipt).await?;
                    self.consecutive_length_batches = 0;
                    if self.overflow_recoveries >= MAX_OVERFLOW_RECOVERIES {
                        self.close_turn_without_context(message).await?;
                        break;
                    }
                    self.overflow_recoveries += 1;
                    tracing::error!(
                        ?source,
                        ordinal = self.overflow_recoveries,
                        "provider context overflow requires immediate recovery"
                    );
                    let request = OverflowRecoveryRequest {
                        source,
                        ordinal: self.overflow_recoveries,
                    };
                    let outcome = match self
                        .driver
                        .plan_overflow_recovery(&self.core, request, &self.context)
                        .await
                    {
                        Ok(outcome) => outcome,
                        Err(error) => {
                            tracing::error!(%error, ?source, "immediate overflow recovery failed");
                            self.close_turn_without_context(message).await?;
                            break;
                        }
                    };
                    let (replacement, retain_canonical) = match outcome {
                        OverflowRecoveryOutcome::ReplacementContext(replacement) => {
                            (replacement, false)
                        }
                        OverflowRecoveryOutcome::RetainCanonicalContext {
                            validated_send_view,
                        } => (validated_send_view, true),
                    };
                    if let Err(error) = self.validate_recovered_context(&replacement) {
                        tracing::error!(%error, ?source, "immediate overflow recovery was invalid");
                        self.close_turn_without_context(message).await?;
                        break;
                    }
                    self.emit_retry_scheduled(
                        self.attempt_sequence as u32,
                        Duration::ZERO,
                        format!("context overflow: {source:?}"),
                    )
                    .await?;
                    if !retain_canonical {
                        self.context = replacement;
                        self.core.mark_mutated();
                    }
                }
                AttemptOutcome::Terminal {
                    assistant_message_id,
                    message,
                    assistant_message,
                    provider_context,
                    receipt,
                    rejected_results,
                    deferred_overflow,
                    length_guarded,
                } => {
                    let assistant_receipt_waiter = receipt;
                    self.ordinary_retries = 0;
                    self.overflow_recoveries = 0;
                    if let Some(source) = deferred_overflow {
                        tracing::error!(
                            ?source,
                            "provider context overflow deferred until the next memory apply boundary"
                        );
                        self.core.defer_overflow_apply(source);
                    }
                    let calls = tool_calls(&message);
                    if calls.is_empty() && rejected_results.is_empty() {
                        self.consecutive_length_batches = 0;
                        let receipt = self.await_message_receipt(assistant_receipt_waiter).await?;
                        self.durable_terminal_pending = true;
                        if let Some(ratio_bits) = receipt.calibration_ratio_bits {
                            self.driver
                                .install_committed_calibration(ratio_bits)
                                .map_err(|error| WorkerFailure::Error(error.to_string()))?;
                        }
                        self.driver
                            .apply_terminal(
                                &receipt.message_id,
                                receipt.message_seq,
                                &assistant_message,
                                &provider_context,
                            )
                            .await
                            .map_err(|error| WorkerFailure::Error(error.to_string()))?;
                        self.retain_committed(receipt, &message)?;
                        self.durable_terminal_pending = false;
                        self.close_turn(message, Vec::new()).await?;
                        if !self.advance_followup().await? {
                            break;
                        }
                        continue;
                    }

                    let is_length = length_guarded
                        || (!calls.is_empty() && stop_reason(&message) == Some(StopReason::Length));
                    // The assistant canonical snapshot and every rejected-call
                    // result must become durable before a valid call can enter
                    // Prepare/Start (or the private Length Skip path). The
                    // bridge commits the rejected pair atomically once the
                    // final result arrives.
                    let rejected_receipts = self
                        .emit_rejected_results(&assistant_message_id, &rejected_results)
                        .await?;
                    let receipt = self.await_message_receipt(assistant_receipt_waiter).await?;
                    self.durable_terminal_pending = true;
                    if let Some(ratio_bits) = receipt.calibration_ratio_bits {
                        self.driver
                            .install_committed_calibration(ratio_bits)
                            .map_err(|error| WorkerFailure::Error(error.to_string()))?;
                    }
                    self.driver
                        .apply_terminal(
                            &receipt.message_id,
                            receipt.message_seq,
                            &assistant_message,
                            &provider_context,
                        )
                        .await
                        .map_err(|error| WorkerFailure::Error(error.to_string()))?;
                    let (executable_results, executable_receipts) = if is_length {
                        self.consecutive_length_batches += 1;
                        self.fail_length_calls(&assistant_message_id, &calls, length_guarded)
                            .await?
                    } else {
                        self.consecutive_length_batches = 0;
                        self.execute_calls(&assistant_message_id, &message, &calls)
                            .await?
                    };
                    if !length_guarded {
                        let mut committed = vec![(receipt, message.clone())];
                        committed.extend(
                            rejected_receipts.into_iter().zip(
                                rejected_results
                                    .iter()
                                    .cloned()
                                    .map(PublicMessage::ToolResult),
                            ),
                        );
                        committed.extend(
                            executable_receipts.into_iter().zip(
                                executable_results
                                    .iter()
                                    .cloned()
                                    .map(PublicMessage::ToolResult),
                            ),
                        );
                        committed.sort_by_key(|(receipt, _)| receipt.message_seq);
                        for (receipt, committed_message) in committed {
                            self.retain_committed(receipt, &committed_message)?;
                        }
                        // Normal and non-guarded length receipts are retained in
                        // the sorted committed batch above; guarded-length
                        // receipts are retained separately below.
                    } else {
                        self.retain_tool_results(&rejected_receipts, &rejected_results)?;
                        self.retain_tool_results(&executable_receipts, &executable_results)?;
                    }
                    self.durable_terminal_pending = false;
                    self.emit(AgentEvent::TurnEnd {
                        message: Some(Box::new(message)),
                        tool_results: executable_results,
                    })
                    .await?;
                    self.receive_control_safe_point().await?;

                    if self.abort_requested {
                        self.in_flight_controls.clear();
                        break;
                    }

                    if length_guarded {
                        break;
                    }

                    // A provider terminal carrying executable calls always
                    // continues with a fresh turn after every result settles.
                    self.start_next_turn().await?;
                    if self.claim_pending_user()? {
                        self.inject_in_flight().await?;
                    }
                }
                AttemptOutcome::ClosedError {
                    assistant_message_id,
                    message,
                    receipt,
                    rejected_results,
                } => {
                    let receipts = self
                        .emit_rejected_results(&assistant_message_id, &rejected_results)
                        .await?;
                    self.retain_tool_results(&receipts, &rejected_results)?;
                    let receipt = self.await_message_receipt(receipt).await?;
                    if self
                        .pending_provider_context
                        .contains_key(&receipt.message_id)
                    {
                        self.retain_committed(receipt, &message)?;
                    }
                    self.close_turn(message, Vec::new()).await?;
                    break;
                }
                AttemptOutcome::HardSteer => {
                    self.emit(AgentEvent::Steered {
                        mode: SteerMode::Hard,
                    })
                    .await?;
                    self.inject_in_flight().await?;
                }
            }
        }
        self.emit(AgentEvent::AgentEnd).await
    }

    async fn provider_attempt(&mut self) -> Result<AttemptOutcome, WorkerFailure> {
        self.hard_steer_command = None;
        // abort_requested is preserved when receive_control_safe_point consumes
        // an Abort control before provider_attempt begins.
        if self.abort_requested {
            return self
                .synthetic_attempt_error(
                    "Run aborted before provider start".to_owned(),
                    SyntheticAttemptFailure::Abort,
                )
                .await;
        }
        let attempt_cancellation = self
            .core
            .attempt_cancellation
            .as_ref()
            .ok_or_else(|| {
                WorkerFailure::Error("RunCore has no attempt cancellation registry".to_owned())
            })?
            .clone();
        let cancel = self.cancel.child_token();
        let _guard = attempt_cancellation
            .register(cancel.clone())
            .map_err(|error| WorkerFailure::Error(error.to_string()))?;
        self.provider_cancel = Some(cancel.clone());
        let outcome = self.provider_attempt_loop(cancel).await;
        if outcome.is_ok() {
            attempt_cancellation
                .retire_committed()
                .map_err(|error| WorkerFailure::Error(error.to_string()))?;
        }
        outcome
    }

    async fn provider_attempt_loop(
        &mut self,
        cancel: CancellationToken,
    ) -> Result<AttemptOutcome, WorkerFailure> {
        let start_cancel = cancel.clone();
        // A command ingress timestamp has exactly one causal consumer: the
        // first provider request started after that command is injected.
        // Retries and tool continuations keep their own TTFT observation, but
        // must not fold provider/backoff/tool time into agent internal p95.
        let command_received_at = self.pending_command_received_at.take();
        let trigger = if std::mem::take(&mut self.first_provider_call_after_user) {
            ProviderCallTrigger::FirstAfterUser
        } else {
            ProviderCallTrigger::Continuation
        };
        let start = self.driver.start_provider_for_user_turn(
            ProviderCallAttempt {
                attempt_sequence: self.attempt_sequence,
                user_turn_attempt: self.user_turn_attempt,
                trigger,
            },
            &self.context,
            &self.provider_context,
            command_received_at,
            cancel.clone(),
        );
        let mut attempt = match CancelOnDrop::new(start, start_cancel).await {
            Ok(attempt) => attempt,
            Err(error) => {
                return self
                    .synthetic_attempt_error(error.to_string(), SyntheticAttemptFailure::Start)
                    .await;
            }
        };
        let mut projector = match ProviderEventProjector::new(attempt.message_id.clone()) {
            Ok(projector) => projector,
            Err(error) => {
                return self
                    .synthetic_attempt_error(
                        error.to_string(),
                        SyntheticAttemptFailure::InvalidMessageId,
                    )
                    .await;
            }
        };
        let mut message_started = false;
        let mut rejected_results = Vec::new();
        let mut cancellation_observed = false;
        let runtime_cancel = self.cancel.clone();
        let mut runtime_shutdown_observed = false;

        loop {
            tokio::select! {
                biased;
                _ = runtime_cancel.cancelled(), if !runtime_shutdown_observed => {
                    runtime_shutdown_observed = true;
                    self.abort_requested = true;
                    cancel.cancel();
                }
                control = self.controls.recv() => {
                    let Some(control) = control else {
                        return Err(WorkerFailure::Cancelled);
                    };
                    match control {
                        RunControl::HardSteer { command, accepted } => {
                            if accepted.send(true).is_ok() {
                                self.hard_steer_command = Some(command);
                                // The Session will cancel the provider attempt
                                // only after `bind_hard_steer` commits.
                            }
                        }
                        RunControl::Abort {
                            accepted,
                            committed,
                            ..
                        } => {
                            if self.accept_abort_control(accepted, committed).await? {
                                self.abort_requested = true;
                                self.cancel_provider();
                            }
                        }
                        RunControl::SoftSteer { accepted, committed, .. }
                        | RunControl::RetrySteer { accepted, committed, .. } => {
                            let _ = accepted.send(false);
                            drop(committed);
                        }
                        RunControl::Command(command) => {
                            self.core
                                .queue_followup(command)
                                .map_err(|error| WorkerFailure::Error(error.to_string()))?;
                        }
                    }
                }
                event = attempt.events.recv(), if self.hard_steer_command.is_none() || cancellation_observed => {
                    let Some(event) = event else {
                        // EOF while not hard-steering.
                        drop(rejected_results);
                        if self.abort_requested {
                            return self.close_aborted_attempt(
                                &attempt.message_id,
                                message_started,
                                attempt.initial_message.clone(),
                                Vec::new(),
                            ).await;
                        }
                        return self.close_broken_attempt(
                            &attempt.message_id,
                            message_started,
                            "provider stream ended without a terminal event".to_owned(),
                        ).await;
                    };
                    let terminal_message = match &event {
                        ProviderEvent::Done { output, .. } | ProviderEvent::Error { output, .. } => {
                            Some(output.message.clone())
                        }
                        _ => None,
                    };
                    let terminal_overflow = terminal_message.as_ref().and_then(|message| {
                        classify_context_overflow(message, self.driver.context_window())
                    });
                    let projected = match projector.project(event) {
                        Ok(projected) => projected,
                        Err(error) => {
                            // No authoritative terminal retained the rejected call in
                            // its assistant snapshot. Emitting its buffered result
                            // here would create a durable orphan.
                            drop(rejected_results);
                            return self
                                .close_broken_attempt(
                                    &attempt.message_id,
                                    message_started,
                                    format!("provider projection failed: {error}"),
                                )
                                .await;
                        }
                    };
                    match projected {
                        ProjectedProviderEvent::Started => {
                            self.emit(AgentEvent::MessageStart {
                                message_id: attempt.message_id.clone(),
                                message: Box::new(attempt.initial_message.clone()),
                            })
                            .await?;
                            message_started = true;
                        }
                        ProjectedProviderEvent::Update(event) => self.emit(event).await?,
                        ProjectedProviderEvent::RejectedToolCall {
                            event,
                            synthetic_result,
                        } => {
                            self.emit(event).await?;
                            rejected_results.push(synthetic_result);
                        }
                        ProjectedProviderEvent::Terminal(terminal) => {
                            let provider_context = terminal.provider_context().to_vec();
                            let kind = terminal.kind();
                            let internal =
                                terminal_message.expect("terminal projection has provider output");
                            // Internal stream/projection failures copy the volatile
                            // shadow into a synthesized terminal, but that shadow is
                            // not an authoritative provider snapshot. Its buffered
                            // rejection results must not survive as durable orphans.
                            let internal_projection_failure = matches!(
                                internal.provider_code.as_deref(),
                                Some(
                                    "stream_ended_without_terminal_event"
                                        | "invalid_provider_event"
                                        | "invalid_provider_terminal"
                                        | "invalid_provider_stream"
                                )
                            );
                            if internal_projection_failure {
                                rejected_results.clear();
                            }
                            if let Err(error) =
                                validate_and_order_rejected_results(&internal, &mut rejected_results)
                            {
                                rejected_results.clear();
                                return self
                                    .close_broken_attempt(
                                        &attempt.message_id,
                                        message_started,
                                        format!(
                                            "provider terminal rejection/result correspondence failed: {error}"
                                        ),
                                    )
                                    .await;
                            }
                            let overflow = terminal_overflow;
                            let length_guarded = kind == ProviderTerminalKind::Done
                                && !matches!(
                                    overflow,
                                    Some(OverflowClassification::ImmediateRecovery(
                                        OverflowSource::LengthUsage
                                    ))
                                )
                                && self.consecutive_length_batches >= 1
                                && internal.stop_reason == StopReason::Length
                                && internal.content.iter().any(|content| {
                                    matches!(
                                        content,
                                        crate::provider::types::AssistantContent::ToolCall { .. }
                                    )
                                });
                            let public = match overflow {
                                Some(OverflowClassification::ImmediateRecovery(source)) => {
                                    normalize_immediate_overflow(
                                        terminal.message(),
                                        source,
                                        &rejected_results,
                                    )
                                }
                                _ if length_guarded => normalize_length_loop_guard(terminal.message()),
                                _ => terminal.message().clone(),
                            };
                            if self.abort_requested {
                                self.hard_steer_command.take();
                                return self
                                    .close_aborted_attempt(
                                        &attempt.message_id,
                                        message_started,
                                        public,
                                        provider_context,
                                    )
                                    .await;
                            }
                            if let Some(command) = self.hard_steer_command.take() {
                                return self.close_hard_steer_attempt(
                                    &attempt.message_id,
                                    message_started,
                                    public,
                                    provider_context,
                                    command,
                                )
                                .await;
                            }
                            let (terminal_message_id, terminal_message) = match terminal.event() {
                                AgentEvent::MessageEnd { message_id, .. } => {
                                    (message_id.clone(), public.clone())
                                }
                                _ => unreachable!("provider terminal is always MessageEnd"),
                            };
                            let durable_provider_context = if kind != ProviderTerminalKind::Error
                                && (matches!(
                                    overflow,
                                    Some(OverflowClassification::ImmediateRecovery(_))
                                ) || length_guarded)
                            {
                                Vec::new()
                            } else {
                                provider_context
                            };
                            // Error assistants and their provider context remain
                            // outside live L0/replay, but the authoritative
                            // terminal still carries the verified fragments to
                            // the durable MessageEnd transaction below.
                            if kind != ProviderTerminalKind::Error {
                                self.stage_provider_context(
                                    &terminal_message_id,
                                    &terminal_message,
                                    &durable_provider_context,
                                )?;
                            }
                            let receipt = self
                                .emit_provider_message_end(
                                    terminal_message_id,
                                    terminal_message,
                                    durable_provider_context,
                                    kind,
                                    attempt.uncalibrated_prompt_estimate,
                                )
                                .await?;
                            if let Some(OverflowClassification::ImmediateRecovery(source)) = overflow {
                                return Ok(AttemptOutcome::ImmediateOverflow {
                                    assistant_message_id: attempt.message_id.clone(),
                                    message: public,
                                    receipt,
                                    source,
                                    rejected_results,
                                });
                            }
                            if kind == ProviderTerminalKind::Error {
                                // Error assistants remain observable but never enter L0/context.
                                if internal.stop_reason == StopReason::Error && is_retryable(&internal) {
                                    return Ok(AttemptOutcome::Retry {
                                        assistant_message_id: attempt.message_id.clone(),
                                        message: public,
                                        receipt,
                                        rejected_results,
                                    });
                                }
                                return Ok(AttemptOutcome::ClosedError {
                                    assistant_message_id: attempt.message_id.clone(),
                                    message: public,
                                    receipt,
                                    rejected_results,
                                });
                            }
                            return Ok(AttemptOutcome::Terminal {
                                assistant_message_id: attempt.message_id.clone(),
                                message: public,
                                assistant_message: internal.clone(),
                                provider_context: terminal.provider_context().to_vec(),
                                receipt,
                                rejected_results,
                                deferred_overflow: match overflow {
                                    Some(OverflowClassification::DeferredApply(source)) => Some(source),
                                    _ => None,
                                },
                                length_guarded,
                            });
                        }
                    }
                }
                () = cancel.cancelled(), if !cancellation_observed => {
                    // The ProviderEventStream owns the authoritative assembler
                    // shadow. Poll it once more after cancellation so its
                    // synthesized terminal carries every accumulated Text and
                    // adapter-approved Thinking block; `initial_message` is
                    // metadata only and must never replace that snapshot.
                    cancellation_observed = true;
                }
            }
        }
    }

    async fn synthetic_attempt_error(
        &mut self,
        error: String,
        failure: SyntheticAttemptFailure,
    ) -> Result<AttemptOutcome, WorkerFailure> {
        let message = self.driver.synthetic_error(&error);
        let binding = self.core.durable_binding.as_ref().ok_or_else(|| {
            WorkerFailure::Error("RunCore has no durable worker binding".to_owned())
        })?;
        let message_id = synthetic_attempt_message_id(binding, self.attempt_sequence, failure)?;
        self.emit(AgentEvent::MessageStart {
            message_id: message_id.clone(),
            message: Box::new(message.clone()),
        })
        .await?;
        let receipt = self
            .emit_message_end(message_id.clone(), message.clone(), None, None)
            .await?;
        Ok(AttemptOutcome::ClosedError {
            assistant_message_id: message_id,
            message,
            receipt,
            rejected_results: Vec::new(),
        })
    }

    async fn close_hard_steer_attempt(
        &mut self,
        message_id: &str,
        started: bool,
        partial: PublicMessage,
        provider_context: Vec<ProviderContextFragment>,
        command: AdmittedCommand,
    ) -> Result<AttemptOutcome, WorkerFailure> {
        let partial = steer::normalize_partial_assistant(partial)
            .map_err(|error| WorkerFailure::Error(error.to_string()))?;
        self.stage_provider_context(message_id, &partial, &provider_context)?;
        if !started {
            self.emit(AgentEvent::MessageStart {
                message_id: message_id.to_owned(),
                message: Box::new(partial.clone()),
            })
            .await?;
        }
        let receipt = self
            .emit_message_end_with_provider_context(
                message_id.to_owned(),
                partial.clone(),
                provider_context,
                None,
                None,
                None,
            )
            .await?;
        let receipt = self.await_message_receipt(receipt).await?;
        // MessageEnd is now durable. Promote its staged context before any
        // post-commit control can close the live run, so warm continuation
        // remains identical to cold hydration even when Abort wins below.
        self.retain_committed(receipt.clone(), &partial)?;
        // Give a queued Abort its durable authorization turn before deciding
        // whether this close can hand off to a new turn.
        self.receive_control_safe_point().await?;
        // An Abort can win after this MessageEnd was sent but before the
        // hard-steer close path resumes.  The bridge has already closed the
        // original assistant (with or without the staged new turn identity),
        // so do not claim the superseded steer or inject it.
        if let Some(ref new_turn_id) = receipt.new_turn_id {
            let binding = self.core.durable_binding.as_mut().ok_or_else(|| {
                WorkerFailure::Error("RunCore has no durable worker binding".to_owned())
            })?;
            binding.turn_id = new_turn_id.clone();
        }
        if self.abort_requested {
            let (committed_tx, committed_rx) = oneshot::channel();
            let _ = committed_tx.send(receipt);
            return Ok(AttemptOutcome::ClosedError {
                assistant_message_id: message_id.to_owned(),
                message: partial,
                receipt: committed_rx,
                rejected_results: Vec::new(),
            });
        }
        self.claim_control(command)?;
        Ok(AttemptOutcome::HardSteer)
    }

    async fn close_aborted_attempt(
        &mut self,
        message_id: &str,
        started: bool,
        partial: PublicMessage,
        provider_context: Vec<ProviderContextFragment>,
    ) -> Result<AttemptOutcome, WorkerFailure> {
        let partial = steer::normalize_partial_assistant(partial)
            .map_err(|error| WorkerFailure::Error(error.to_string()))?;
        self.stage_provider_context(message_id, &partial, &provider_context)?;
        if !started {
            self.emit(AgentEvent::MessageStart {
                message_id: message_id.to_owned(),
                message: Box::new(partial.clone()),
            })
            .await?;
        }
        let receipt = self
            .emit_message_end_with_provider_context(
                message_id.to_owned(),
                partial.clone(),
                provider_context,
                None,
                None,
                None,
            )
            .await?;
        Ok(AttemptOutcome::ClosedError {
            assistant_message_id: message_id.to_owned(),
            message: partial,
            receipt,
            rejected_results: Vec::new(),
        })
    }

    fn stage_provider_context(
        &mut self,
        message_id: &str,
        message: &PublicMessage,
        provider_context: &[ProviderContextFragment],
    ) -> Result<(), WorkerFailure> {
        if provider_context.is_empty() {
            return Ok(());
        }
        let PublicMessage::Assistant(assistant) = message else {
            return Err(WorkerFailure::Error(
                "provider context requires an assistant terminal".to_owned(),
            ));
        };
        if self
            .pending_provider_context
            .insert(
                message_id.to_owned(),
                (assistant.origin.clone(), provider_context.to_vec()),
            )
            .is_some()
        {
            return Err(WorkerFailure::Error(
                "duplicate pending provider-context message identity".to_owned(),
            ));
        }
        Ok(())
    }

    async fn close_broken_attempt(
        &mut self,
        message_id: &str,
        started: bool,
        error: String,
    ) -> Result<AttemptOutcome, WorkerFailure> {
        let message = self.driver.synthetic_error(&error);
        if !started {
            self.emit(AgentEvent::MessageStart {
                message_id: message_id.to_owned(),
                message: Box::new(message.clone()),
            })
            .await?;
        }
        let receipt = self
            .emit_message_end(message_id.to_owned(), message.clone(), None, None)
            .await?;
        Ok(AttemptOutcome::ClosedError {
            assistant_message_id: message_id.to_owned(),
            message,
            receipt,
            rejected_results: Vec::new(),
        })
    }

    async fn fail_length_calls(
        &mut self,
        assistant_message_id: &str,
        calls: &[ToolCall],
        terminal_guard: bool,
    ) -> Result<(Vec<ToolResultMessage>, Vec<MessageCommitReceipt>), WorkerFailure> {
        // These synthetic results deliberately have no execution lifecycle.
        // The durable bridge must map them to skipped/not-started transactions.
        let mut results = Vec::with_capacity(calls.len());
        let mut receipts = Vec::with_capacity(calls.len());
        for call in calls {
            let message = if terminal_guard {
                format!("{LENGTH_TOOL_FAILURE} {LENGTH_LOOP_FAILURE}")
            } else {
                LENGTH_TOOL_FAILURE.to_owned()
            };
            let result = error_tool_result(call, &message);
            let waiter = self
                .emit_result_message(assistant_message_id, &result, None, None)
                .await?;
            let receipt = self.await_message_receipt(waiter).await?;
            receipts.push(receipt);
            results.push(result);
        }
        Ok((results, receipts))
    }

    async fn execute_calls(
        &mut self,
        assistant_message_id: &str,
        assistant_message: &PublicMessage,
        calls: &[ToolCall],
    ) -> Result<(Vec<ToolResultMessage>, Vec<MessageCommitReceipt>), WorkerFailure> {
        let base_epoch = self.core.mutation_epoch();
        let mut transcript: Vec<PublicMessage> = self
            .context
            .iter()
            .map(|ctx| message_to_public(context_message(ctx).clone()))
            .collect();
        transcript.push(assistant_message.clone());
        let mut results = Vec::with_capacity(calls.len());
        let mut receipts = Vec::with_capacity(calls.len());
        let mut cancel_reason: Option<String> = None;
        for (index, call) in calls.iter().enumerate() {
            if let Some(ref reason) = cancel_reason {
                let result = error_tool_result(call, reason);
                receipts.push(
                    self.emit_result_message(assistant_message_id, &result, None, None)
                        .await?
                        .await
                        .map_err(|_| {
                            WorkerFailure::Error("tool result receipt dropped".to_owned())
                        })?,
                );
                results.push(result);
                continue;
            }
            let context_version = base_epoch
                .saturating_add(index as u64)
                .saturating_add(1)
                .to_string();
            'authorize: loop {
                match self
                    .evaluate_call(call, &transcript, &context_version)
                    .await?
                {
                    CallDisposition::Allowed { grant } => {
                        match self.emit_tool_start_and_wait_committed(call, grant).await? {
                            ToolStartOutcome::Started => {
                                let result = match self
                                    .execute_tool_with_updates(assistant_message_id, call)
                                    .await
                                {
                                    Ok(mut result) => {
                                        result.tool_call_id.clone_from(&call.id);
                                        result.tool_name.clone_from(&call.name);
                                        result
                                    }
                                    Err(ExecuteToolError::Worker(failure)) => return Err(failure),
                                    Err(ExecuteToolError::Tool(ToolError::RpcIndeterminate(
                                        message,
                                    ))) => {
                                        return Err(WorkerFailure::Error(format!(
                                            "tool RPC outcome is indeterminate: {message}"
                                        )));
                                    }
                                    Err(ExecuteToolError::Tool(error)) => error_tool_result(
                                        call,
                                        &format!("Tool execution failed: {error}"),
                                    ),
                                    Err(ExecuteToolError::Cancelled) => error_tool_result(
                                        call,
                                        "Tool execution was cancelled by a user control",
                                    ),
                                };
                                let result_message = PublicMessage::ToolResult(result.clone());
                                receipts.push(
                                    self.emit_tool_result(assistant_message_id, &result).await?,
                                );
                                results.push(result);
                                transcript.push(result_message);
                            }
                            ToolStartOutcome::Preempted => {
                                let reason = if self.abort_requested {
                                    "Tool execution was cancelled by a user control".to_owned()
                                } else if !self.in_flight_controls.is_empty() {
                                    "ユーザーの新しい指示により実行前に取り消された".to_owned()
                                } else {
                                    "Tool execution was cancelled by a user control".to_owned()
                                };
                                let result = error_tool_result(call, &reason);
                                let result_message = PublicMessage::ToolResult(result.clone());
                                receipts.push(
                                    self.emit_result_message(
                                        assistant_message_id,
                                        &result,
                                        None,
                                        None,
                                    )
                                    .await?
                                    .await
                                    .map_err(|_| {
                                        WorkerFailure::Error(
                                            "tool result receipt dropped".to_owned(),
                                        )
                                    })?,
                                );
                                results.push(result);
                                transcript.push(result_message);
                                cancel_reason = Some(reason);
                            }
                            ToolStartOutcome::Reauthorize => continue 'authorize,
                        }
                    }
                    CallDisposition::Denied {
                        reason,
                        approval_denied,
                    } => {
                        let result = error_tool_result(call, &reason);
                        let result_message = PublicMessage::ToolResult(result.clone());
                        receipts.push(
                            self.emit_result_message(
                                assistant_message_id,
                                &result,
                                approval_denied.then(|| call.id.clone()),
                                None,
                            )
                            .await?
                            .await
                            .map_err(|_| {
                                WorkerFailure::Error("tool result receipt dropped".to_owned())
                            })?,
                        );
                        results.push(result);
                        transcript.push(result_message);
                    }
                    CallDisposition::Pending { mut pending } => {
                        let request = pending.request().clone();
                        self.emit(AgentEvent::ApprovalRequested {
                            request: request.clone(),
                        })
                        .await?;
                        self.phase.send(WorkerPhase::Approval).ok();
                        match self
                            .wait_for_approval(request.id.clone(), pending.receiver_mut())
                            .await?
                        {
                            ApprovalWaitOutcome::Resolved { decision, command } => {
                                self.emit_approval_resolved(
                                    request.id.clone(),
                                    &decision,
                                    Some(*command),
                                )
                                .await?;
                                let committed_decision = decision.clone();
                                match decision {
                                    crate::approval::policy::ResolvedDecision::ApproveOnce
                                    | crate::approval::policy::ResolvedDecision::ApproveAlways(_) => {
                                        match self
                                            .emit_tool_start_and_wait_committed(call, None)
                                            .await?
                                        {
                                            ToolStartOutcome::Started => {
                                                if let Some(broker) = self.core.approval.as_ref() {
                                                    broker
                                                    .commit_resolution(
                                                        &request.id,
                                                        &committed_decision,
                                                    )
                                                    .map_err(|error| {
                                                        WorkerFailure::Error(format!(
                                                            "committed approval rule activation failed: {error}"
                                                        ))
                                                    })?;
                                                }
                                                let result = match self
                                                    .execute_tool_with_updates(
                                                        assistant_message_id,
                                                        call,
                                                    )
                                                    .await
                                                {
                                                    Ok(mut result) => {
                                                        result.tool_call_id.clone_from(&call.id);
                                                        result.tool_name.clone_from(&call.name);
                                                        result
                                                    }
                                                    Err(ExecuteToolError::Worker(failure)) => {
                                                        let _ =
                                                            self.phase.send(WorkerPhase::Active);
                                                        return Err(failure);
                                                    }
                                                    Err(ExecuteToolError::Tool(
                                                        ToolError::RpcIndeterminate(message),
                                                    )) => {
                                                        let _ =
                                                            self.phase.send(WorkerPhase::Active);
                                                        return Err(WorkerFailure::Error(format!(
                                                            "tool RPC outcome is indeterminate: {message}"
                                                        )));
                                                    }
                                                    Err(ExecuteToolError::Tool(error)) => {
                                                        error_tool_result(
                                                            call,
                                                            &format!(
                                                                "Tool execution failed: {error}"
                                                            ),
                                                        )
                                                    }
                                                    Err(ExecuteToolError::Cancelled) => {
                                                        error_tool_result(
                                                            call,
                                                            "Tool execution was cancelled by a user control",
                                                        )
                                                    }
                                                };
                                                let result_message =
                                                    PublicMessage::ToolResult(result.clone());
                                                receipts.push(
                                                    self.emit_tool_result(
                                                        assistant_message_id,
                                                        &result,
                                                    )
                                                    .await?,
                                                );
                                                results.push(result);
                                                transcript.push(result_message);
                                            }
                                            ToolStartOutcome::Preempted => {
                                                self.emit(AgentEvent::ApprovalResolved {
                                                request_id: request.id.clone(),
                                                resolution: crate::agent::events::ApprovalResolution::Cancelled,
                                            })
                                            .await?;
                                                let reason = if self.abort_requested {
                                                    "Tool execution was cancelled by a user control"
                                                        .to_owned()
                                                } else if !self.in_flight_controls.is_empty() {
                                                    "ユーザーの新しい指示により実行前に取り消された"
                                                        .to_owned()
                                                } else {
                                                    "Tool execution was cancelled by a user control"
                                                        .to_owned()
                                                };
                                                let result = error_tool_result(call, &reason);
                                                let result_message =
                                                    PublicMessage::ToolResult(result.clone());
                                                receipts.push(
                                                    self.emit_result_message(
                                                        assistant_message_id,
                                                        &result,
                                                        None,
                                                        Some(call.id.clone()),
                                                    )
                                                    .await?
                                                    .await
                                                    .map_err(|_| {
                                                        WorkerFailure::Error(
                                                            "tool result receipt dropped"
                                                                .to_owned(),
                                                        )
                                                    })?,
                                                );
                                                results.push(result);
                                                transcript.push(result_message);
                                                cancel_reason = Some(reason);
                                            }
                                            ToolStartOutcome::Reauthorize => {
                                                return Err(WorkerFailure::Error(
                                                "user-approved ToolExecutionStart unexpectedly requested signed-policy reauthorization"
                                                    .to_owned(),
                                            ));
                                            }
                                        }
                                    }
                                    crate::approval::policy::ResolvedDecision::Deny
                                    | crate::approval::policy::ResolvedDecision::Rejected {
                                        ..
                                    } => {
                                        let reason = match decision {
                                        crate::approval::policy::ResolvedDecision::Rejected {
                                            reason,
                                        } => reason,
                                        _ => "Approval denied".to_owned(),
                                    };
                                        let result = error_tool_result(call, &reason);
                                        let result_message =
                                            PublicMessage::ToolResult(result.clone());
                                        receipts.push(
                                            self.emit_result_message(
                                                assistant_message_id,
                                                &result,
                                                Some(call.id.clone()),
                                                None,
                                            )
                                            .await?
                                            .await
                                            .map_err(
                                                |_| {
                                                    WorkerFailure::Error(
                                                        "tool result receipt dropped".to_owned(),
                                                    )
                                                },
                                            )?,
                                        );
                                        results.push(result);
                                        transcript.push(result_message);
                                    }
                                }
                            }
                            ApprovalWaitOutcome::Cancelled => {
                                self.emit(AgentEvent::ApprovalResolved {
                                    request_id: request.id.clone(),
                                    resolution: crate::agent::events::ApprovalResolution::Cancelled,
                                })
                                .await?;
                                cancel_reason = Some("Tool execution cancelled".to_owned());
                                let result = error_tool_result(call, "Tool execution cancelled");
                                let result_message = PublicMessage::ToolResult(result.clone());
                                receipts.push(
                                    self.emit_result_message(
                                        assistant_message_id,
                                        &result,
                                        None,
                                        Some(call.id.clone()),
                                    )
                                    .await?
                                    .await
                                    .map_err(|_| {
                                        WorkerFailure::Error(
                                            "tool result receipt dropped".to_owned(),
                                        )
                                    })?,
                                );
                                results.push(result);
                                transcript.push(result_message);
                            }
                        }
                        self.phase.send(WorkerPhase::Active).ok();
                    }
                }
                break 'authorize;
            }
            self.receive_control_safe_point().await?;
            if self.abort_requested || !self.in_flight_controls.is_empty() {
                // A soft steer never cancels a running tool. Once it reaches
                // this boundary, every later call is durably skipped.
                let cancellation_message = if self.abort_requested {
                    "Tool execution was cancelled by a user control"
                } else {
                    "ユーザーの新しい指示により実行前に取り消された"
                };
                for remaining in calls.iter().skip(index + 1) {
                    let result = error_tool_result(remaining, cancellation_message);
                    let waiter = self
                        .emit_result_message(assistant_message_id, &result, None, None)
                        .await?;
                    receipts.push(self.await_message_receipt(waiter).await?);
                    results.push(result);
                }
                break;
            }
        }
        Ok((results, receipts))
    }

    async fn evaluate_call(
        &mut self,
        call: &ToolCall,
        transcript: &[PublicMessage],
        context_version: &str,
    ) -> Result<CallDisposition, WorkerFailure> {
        let Some(broker) = self.core.approval.clone() else {
            #[cfg(test)]
            if self.core.fixture_bypass_approval {
                return Ok(CallDisposition::Allowed { grant: None });
            }
            return Err(WorkerFailure::Error(
                "tool execution requires a configured ApprovalBroker".to_owned(),
            ));
        };
        let binding = self.core.durable_binding.as_ref().ok_or_else(|| {
            WorkerFailure::Error("RunCore has no durable worker binding".to_owned())
        })?;
        let run_id = binding.run_id.clone();
        let turn_id = binding.turn_id.clone();
        let review_cancel = self.cancel.child_token();
        let request = broker.start_request(
            call,
            transcript,
            &run_id,
            &turn_id,
            context_version,
            review_cancel.clone(),
        );
        tokio::pin!(request);
        let runtime_cancel = self.cancel.clone();
        let outcome = loop {
            tokio::select! {
                _ = runtime_cancel.cancelled() => {
                    review_cancel.cancel();
                    return Err(WorkerFailure::Cancelled);
                }
                outcome = &mut request => {
                    break outcome.map_err(|error| {
                        WorkerFailure::Error(format!("approval start failed: {error}"))
                    })?;
                }
                control = self.controls.recv() => {
                    match control {
                        Some(RunControl::SoftSteer { command, accepted, committed }) => {
                            if self.accept_steer_control(command, accepted, committed).await? {
                                review_cancel.cancel();
                                return Ok(CallDisposition::Denied {
                                    reason: "ユーザーの新しい指示により実行前に取り消された".to_owned(),
                                    approval_denied: false,
                                });
                            }
                        }
                        Some(RunControl::Abort { accepted, committed, .. }) => {
                            if self.accept_abort_control(accepted, committed).await? {
                                self.abort_requested = true;
                                review_cancel.cancel();
                                return Ok(CallDisposition::Denied {
                                    reason: "Tool execution was cancelled by a user control".to_owned(),
                                    approval_denied: false,
                                });
                            }
                        }
                        Some(RunControl::HardSteer { accepted, .. })
                        | Some(RunControl::RetrySteer { accepted, .. }) => {
                            let _ = accepted.send(false);
                        }
                        Some(RunControl::Command(command)) => {
                            self.core.queue_followup(command).map_err(|error| {
                                WorkerFailure::Error(error.to_string())
                            })?;
                        }
                        None => {
                            review_cancel.cancel();
                            return Err(WorkerFailure::Cancelled);
                        }
                    }
                }
            }
        };
        Ok(match outcome {
            ApprovalOutcome::Allowed { grant } => CallDisposition::Allowed { grant: Some(grant) },
            ApprovalOutcome::Denied { reason, .. } => CallDisposition::Denied {
                reason,
                approval_denied: true,
            },
            ApprovalOutcome::Pending { pending } => CallDisposition::Pending { pending },
        })
    }

    async fn wait_for_approval(
        &mut self,
        request_id: String,
        receiver: &mut oneshot::Receiver<WaiterResult>,
    ) -> Result<ApprovalWaitOutcome, WorkerFailure> {
        use crate::gateway::Command;
        let runtime_cancel = self.cancel.clone();
        loop {
            tokio::select! {
                _ = runtime_cancel.cancelled() => {
                    if let Some(broker) = self.core.approval.as_ref() {
                        broker.cancel(&request_id);
                    }
                    return Err(WorkerFailure::Cancelled);
                }
                result = &mut *receiver => {
                    return match result {
                        Ok(WaiterResult::Resolved(_)) => Err(WorkerFailure::Error(
                            "approval resolved without an authenticated command".to_owned(),
                        )),
                        Ok(WaiterResult::Cancelled) | Err(_) => {
                            Ok(ApprovalWaitOutcome::Cancelled)
                        }
                    };
                }
                control = self.controls.recv() => {
                    match control {
                        Some(RunControl::Command(command)) => {
                            match &command.envelope().command {
                                Command::ApprovalDecision { request_id: rid, decision }
                                    if rid == &request_id =>
                                {
                                    if let Some(broker) = self.core.approval.as_ref() {
                                        return Ok(match broker.resolve(rid, decision).await {
                                            Some(decision) => {
                                                ApprovalWaitOutcome::Resolved {
                                                    decision,
                                                    command: Box::new(command),
                                                }
                                            }
                                            // The matching request became terminal between the
                                            // Session's pending check and worker delivery. It is
                                            // consumed here; treating this decision as a generic
                                            // follow-up would leave a non-User command at the
                                            // front of RunCore and block the next run.
                                            None => ApprovalWaitOutcome::Cancelled,
                                        });
                                    }
                                    return Ok(ApprovalWaitOutcome::Cancelled);
                                }
                                Command::ApprovalDecision { request_id: rid, decision } => {
                                    // An approval decision for a different request must not be
                                    // retained in pending_controls: it would sit at the front of
                                    // RunCore and block the next run. Mirror the matched path by
                                    // attempting to resolve the broker's pending entry; if there
                                    // is none, the decision is a stale no-op and is discarded.
                                    if let Some(broker) = self.core.approval.as_ref() {
                                        let _ = broker.resolve(rid, decision).await;
                                    }
                                    continue;
                                }
                                Command::UserMessage { .. } | Command::Abort {} => {
                                    if let Some(broker) = self.core.approval.as_ref() {
                                        broker.cancel_all();
                                    }
                                    self.core.queue_followup(command).map_err(|error| {
                                        WorkerFailure::Error(error.to_string())
                                    })?;
                                    return Ok(ApprovalWaitOutcome::Cancelled);
                                }
                            }
                        }
                        Some(RunControl::RetrySteer { accepted, .. }) => {
                            let _ = accepted.send(false);
                        }
                        Some(RunControl::HardSteer { accepted, .. }) => {
                            let _ = accepted.send(false);
                        }
                        Some(RunControl::SoftSteer {
                            command,
                            accepted,
                            committed,
                        }) => {
                            if self
                                .accept_steer_control(command, accepted, committed)
                                .await?
                            {
                                if let Some(broker) = self.core.approval.as_ref() {
                                    broker.cancel(&request_id);
                                }
                                return Ok(ApprovalWaitOutcome::Cancelled);
                            }
                        }
                        Some(RunControl::Abort {
                            accepted,
                            committed,
                            ..
                        }) => {
                            if self.accept_abort_control(accepted, committed).await? {
                                self.abort_requested = true;
                                if let Some(broker) = self.core.approval.as_ref() {
                                    broker.cancel(&request_id);
                                }
                                return Ok(ApprovalWaitOutcome::Cancelled);
                            }
                        }
                        None => {
                            if let Some(broker) = self.core.approval.as_ref() {
                                broker.cancel(&request_id);
                            }
                            return Ok(ApprovalWaitOutcome::Cancelled);
                        }
                    }
                }
            }
        }
    }

    async fn emit_approval_resolved(
        &mut self,
        request_id: String,
        decision: &crate::approval::ResolvedDecision,
        command: Option<AdmittedCommand>,
    ) -> Result<(), WorkerFailure> {
        use crate::agent::events::ApprovalResolution;
        use crate::approval::ResolvedDecision;
        use crate::gateway::ApprovalDecision;
        let resolution = match decision {
            ResolvedDecision::ApproveOnce => {
                ApprovalResolution::Decision(ApprovalDecision::ApproveOnce)
            }
            ResolvedDecision::ApproveAlways(rule) => {
                let rule_value = serde_json::to_value(rule).map_err(|error| {
                    WorkerFailure::Error(format!("approval rule serialization failed: {error}"))
                })?;
                let decision_value = serde_json::json!({
                    "type": "approve_always",
                    "rule": rule_value,
                });
                ApprovalResolution::Decision(serde_json::from_value(decision_value).map_err(
                    |error| {
                        WorkerFailure::Error(format!(
                            "approval decision deserialization failed: {error}"
                        ))
                    },
                )?)
            }
            ResolvedDecision::Deny | ResolvedDecision::Rejected { .. } => {
                ApprovalResolution::Decision(ApprovalDecision::Deny)
            }
        };
        let binding = self.core.durable_binding.clone().ok_or_else(|| {
            WorkerFailure::Error("RunCore has no durable worker binding".to_owned())
        })?;
        self.events
            .send(RunOutput {
                binding,
                event: AgentEvent::ApprovalResolved {
                    request_id,
                    resolution,
                },
                commit_barrier: None,
                message_commit_barrier: None,
                retry_wait_commit_barrier: None,
                approval_command: command,
                approval_not_started: None,
                approval_cancelled: None,
            })
            .await
            .map_err(|_| WorkerFailure::EventChannelClosed)
    }

    async fn execute_tool_with_updates(
        &mut self,
        flow_id: &str,
        call: &ToolCall,
    ) -> Result<ToolResultMessage, ExecuteToolError> {
        const TOOL_UPDATE_CAPACITY: usize = 32;
        let (updates_tx, mut updates_rx) = mpsc::channel(TOOL_UPDATE_CAPACITY);
        let callback_call_id = call.id.clone();
        let on_update: Arc<dyn Fn(Value) + Send + Sync> = Arc::new(move |partial| {
            // Progress is volatile. Never block a tool or bypass the bounded
            // Session event lane; a saturated progress lane coalesces by drop.
            let _ = updates_tx.try_send((callback_call_id.clone(), partial));
        });
        let driver = self.driver.clone();
        let cancel = self.cancel.child_token();
        let future = CancelOnDrop::new(
            driver.execute_tool_observed(flow_id, call, cancel.clone(), on_update),
            cancel.clone(),
        );
        tokio::pin!(future);
        if self.abort_requested {
            return Err(ExecuteToolError::Cancelled);
        }
        let result = loop {
            let update = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => {
                    cancel.cancel();
                    // Keep ownership of the driver future until it observes
                    // cancellation and runs its process/reaper teardown. The
                    // Session owns the outer bounded abort fallback.
                    let _ = future.await;
                    return Err(ExecuteToolError::Cancelled);
                }
                result = &mut future => break result,
                update = updates_rx.recv() => update,
                control = self.controls.recv() => {
                    let Some(control) = control else {
                        return Err(WorkerFailure::Cancelled.into());
                    };
                    match control {
                        RunControl::SoftSteer {
                            command,
                            accepted,
                            committed,
                        }
                        | RunControl::RetrySteer {
                            command,
                            accepted,
                            committed,
                        } => {
                            // The active tool is allowed to finish. Durably
                            // classify the steer now; `execute_calls` observes
                            // the claimed group after this result and cancels
                            // only calls that have not started.
                            self.accept_steer_control(command, accepted, committed)
                                .await?;
                            continue;
                        }
                        RunControl::Abort {
                            accepted,
                            committed,
                            ..
                        } => {
                            if self.accept_abort_control(accepted, committed).await? {
                                cancel.cancel();
                                // Wait for the driver to observe cancellation,
                                // then record that this run has been aborted.
                                let _ = future.await;
                                self.abort_requested = true;
                                return Err(ExecuteToolError::Cancelled);
                            }
                            continue;
                        }
                        RunControl::HardSteer { command, accepted } => {
                            self.claim_control(command)?;
                            if accepted.send(true).is_ok() {
                                cancel.cancel();
                                // Wait for the driver to observe cancellation,
                                // then claim the hard-steer command for the
                                // next turn's durable injection.
                                let _ = future.await;
                                return Err(ExecuteToolError::Cancelled);
                            }
                            self.in_flight_controls.pop();
                            continue;
                        }
                        RunControl::Command(command) => {
                            cancel.cancel();
                            // Wait for the driver to observe cancellation, then
                            // queue the ordinary follow-up that interrupted this
                            // tool.
                            let _ = future.await;
                            self.core
                                .queue_followup(command)
                                .map_err(|error| WorkerFailure::Error(error.to_string()))?;
                            return Err(ExecuteToolError::Cancelled);
                        }
                    }
                }
            };
            if let Some((tool_call_id, partial)) = update {
                self.emit(AgentEvent::ToolExecutionUpdate {
                    tool_call_id,
                    partial,
                })
                .await?;
            } else {
                // The on_update callback (and therefore updates_tx) has been
                // dropped while the tool future is still pending. There will
                // never be more progress events, so stop polling this branch
                // and wait only for the tool result.
                break future.await;
            }
        };
        while let Ok((tool_call_id, partial)) = updates_rx.try_recv() {
            self.emit(AgentEvent::ToolExecutionUpdate {
                tool_call_id,
                partial,
            })
            .await?;
        }
        result.map_err(ExecuteToolError::Tool)
    }

    async fn emit_tool_start_and_wait_committed(
        &mut self,
        call: &ToolCall,
        grant: Option<ExecutableGrant>,
    ) -> Result<ToolStartOutcome, WorkerFailure> {
        // Observe any controls that arrived before we durably start the tool.
        self.receive_control_safe_point().await?;
        if self.abort_requested
            || !self.in_flight_controls.is_empty()
            || self.core.has_pending_controls()
        {
            return Ok(ToolStartOutcome::Preempted);
        }

        let binding = self.core.durable_binding.clone().ok_or_else(|| {
            WorkerFailure::Error("RunCore has no durable worker binding".to_owned())
        })?;
        let (commit_barrier, mut committed) = match grant {
            Some(grant) => ToolStartCommitBarrier::channel_with_grant(grant),
            None => ToolStartCommitBarrier::channel(),
        };
        self.events
            .send(RunOutput {
                binding,
                event: AgentEvent::ToolExecutionStart {
                    tool_call_id: call.id.clone(),
                    tool_name: call.name.clone(),
                    args: Value::Object(call.arguments.as_object().clone()),
                },
                commit_barrier: Some(commit_barrier),
                message_commit_barrier: None,
                retry_wait_commit_barrier: None,
                approval_command: None,
                approval_not_started: None,
                approval_cancelled: None,
            })
            .await
            .map_err(|_| WorkerFailure::EventChannelClosed)?;

        tokio::select! {
            biased;
            result = &mut committed => {
                let result = result.map_err(|_| {
                    WorkerFailure::Error("ToolExecutionStart durability commit failed".to_owned())
                })?;
                Ok(match result {
                    ToolStartCommitResult::Committed => ToolStartOutcome::Started,
                    ToolStartCommitResult::Reauthorize => ToolStartOutcome::Reauthorize,
                })
            }
            control = self.controls.recv() => {
                let Some(control) = control else {
                    return Err(WorkerFailure::Cancelled);
                };
                match self.apply_pre_start_control(control, &mut committed).await? {
                    ToolStartOutcome::Started => Ok(ToolStartOutcome::Started),
                    ToolStartOutcome::Reauthorize => Ok(ToolStartOutcome::Reauthorize),
                    ToolStartOutcome::Preempted => {
                        // Drain any additional controls that arrived while we were
                        // authorizing the first one.
                        self.receive_control_safe_point().await?;
                        Ok(ToolStartOutcome::Preempted)
                    }
                }
            }
        }
    }

    async fn emit_tool_result(
        &mut self,
        assistant_message_id: &str,
        result: &ToolResultMessage,
    ) -> Result<MessageCommitReceipt, WorkerFailure> {
        let durable_result = serde_json::to_value(result).map_err(|error| {
            WorkerFailure::Error(format!("tool result serialization failed: {error}"))
        })?;
        self.emit(AgentEvent::ToolExecutionEnd {
            tool_call_id: result.tool_call_id.clone(),
            result: durable_result,
            is_error: result.is_error,
        })
        .await?;
        let receipt = self
            .emit_result_message(assistant_message_id, result, None, None)
            .await?;
        let receipt = self.await_message_receipt(receipt).await?;
        Ok(receipt)
    }

    async fn emit_result_message(
        &mut self,
        assistant_message_id: &str,
        result: &ToolResultMessage,
        approval_not_started: Option<String>,
        approval_cancelled: Option<String>,
    ) -> Result<oneshot::Receiver<MessageCommitReceipt>, WorkerFailure> {
        let message = PublicMessage::ToolResult(result.clone());
        let message_id = tool_result_message_id(assistant_message_id, &result.tool_call_id);
        self.emit(AgentEvent::MessageStart {
            message_id: message_id.clone(),
            message: Box::new(message.clone()),
        })
        .await?;
        self.emit_message_end(
            message_id,
            message,
            approval_not_started,
            approval_cancelled,
        )
        .await
    }

    async fn emit_rejected_results(
        &mut self,
        assistant_message_id: &str,
        results: &[ToolResultMessage],
    ) -> Result<Vec<MessageCommitReceipt>, WorkerFailure> {
        let mut pending = Vec::with_capacity(results.len());
        for result in results {
            pending.push(
                self.emit_result_message(assistant_message_id, result, None, None)
                    .await?,
            );
        }
        let mut receipts = Vec::with_capacity(pending.len());
        for waiter in pending {
            receipts.push(self.await_message_receipt(waiter).await?);
        }
        Ok(receipts)
    }

    async fn inject_user(&mut self, command: &AdmittedCommand) -> Result<(), WorkerFailure> {
        let Command::UserMessage { text, attachments } = &command.envelope().command else {
            return Err(WorkerFailure::Error(
                "non-user command reached a user injection boundary".to_owned(),
            ));
        };
        debug_assert!(attachments.is_empty());
        let message = PublicMessage::User(UserMessage {
            content: vec![UserContent::Text { text: text.clone() }],
            timestamp: command.received_at(),
        });
        let message_id = user_message_id(
            &command.envelope().personality_agent_id,
            &command.envelope().command_id,
        );
        self.emit(AgentEvent::MessageStart {
            message_id: message_id.clone(),
            message: Box::new(message.clone()),
        })
        .await?;
        let receipt = self
            .emit_message_end(message_id, message.clone(), None, None)
            .await?;
        let receipt = self.await_message_receipt(receipt).await?;
        self.retain_committed(receipt, &message)?;
        Ok(())
    }

    fn claim_control(&mut self, command: AdmittedCommand) -> Result<(), WorkerFailure> {
        self.in_flight_controls.push(command);
        Ok(())
    }

    async fn inject_in_flight(&mut self) -> Result<(), WorkerFailure> {
        if self.in_flight_controls.is_empty() {
            return Err(WorkerFailure::Error(
                "caller must claim at least one control before injection".to_owned(),
            ));
        }
        // Clone so an event-channel failure leaves the controls in the runner
        // for recovery.
        let injectables = self.in_flight_controls.clone();
        let mut pending_receipts = Vec::with_capacity(injectables.len());
        let mut messages = Vec::with_capacity(injectables.len());
        for command in &injectables {
            let message = super::steer::build_user_message(command)
                .map_err(|error| WorkerFailure::Error(error.to_string()))?;
            let message_id = crate::store::user_message_id(
                &command.envelope().personality_agent_id,
                &command.envelope().command_id,
            );
            self.emit(AgentEvent::MessageStart {
                message_id: message_id.clone(),
                message: Box::new(message.clone()),
            })
            .await?;
            let receipt = self
                .emit_message_end(message_id, message.clone(), None, None)
                .await?;
            pending_receipts.push(receipt);
            messages.push(message);
        }
        let mut receipts = Vec::with_capacity(pending_receipts.len());
        for receipt in pending_receipts {
            receipts.push(self.await_message_receipt(receipt).await?);
        }
        for (receipt, message) in receipts.into_iter().zip(messages) {
            self.retain_committed(receipt, &message)?;
        }
        self.pending_command_received_at = injectables
            .first()
            .and_then(|command| command.received_monotonic());
        // TurnStart is also emitted for tool-result continuations. A durable
        // user injection, not that internal lifecycle event, is the boundary
        // that restores first-attempt request policy and first-user-call
        // context assembly.
        self.user_turn_attempt = 0;
        self.first_provider_call_after_user = true;
        self.in_flight_controls.clear();
        Ok(())
    }

    async fn close_turn(
        &mut self,
        message: PublicMessage,
        tool_results: Vec<ToolResultMessage>,
    ) -> Result<(), WorkerFailure> {
        self.emit(AgentEvent::TurnEnd {
            message: Some(Box::new(message)),
            tool_results,
        })
        .await
    }

    async fn close_turn_without_context(
        &mut self,
        message: PublicMessage,
    ) -> Result<(), WorkerFailure> {
        self.emit(AgentEvent::TurnEnd {
            message: Some(Box::new(message)),
            tool_results: Vec::new(),
        })
        .await
    }

    fn validate_recovered_context(
        &self,
        replacement: &[ContextMessage],
    ) -> Result<(), WorkerFailure> {
        if replacement.is_empty() || replacement == self.context {
            return Err(WorkerFailure::Error(
                "overflow recovery did not establish a changed send context".to_owned(),
            ));
        }
        if let Some(active_user) = self
            .context
            .iter()
            .rev()
            .find(|message| matches!(context_message(message), Message::User(_)))
            && !replacement.contains(active_user)
        {
            return Err(WorkerFailure::Error(
                "overflow recovery dropped the active user from the send context".to_owned(),
            ));
        }
        Ok(())
    }

    async fn advance_followup(&mut self) -> Result<bool, WorkerFailure> {
        self.receive_control_safe_point().await?;
        if !self.claim_pending_user()? {
            return Ok(false);
        }
        self.start_next_turn().await?;
        self.inject_in_flight().await?;
        Ok(true)
    }

    fn claim_pending_user(&mut self) -> Result<bool, WorkerFailure> {
        if !self.in_flight_controls.is_empty() {
            // A steer group is already claimed for injection.
            return Ok(true);
        }
        let Some(command) = self.core.next_followup() else {
            return Ok(false);
        };
        if matches!(command.envelope().command, Command::UserMessage { .. }) {
            self.claim_control(command)?;
            Ok(true)
        } else {
            self.core
                .requeue_followup_front(command)
                .map_err(|error| WorkerFailure::Error(error.to_string()))?;
            Ok(false)
        }
    }

    /// Applies a single control that arrived while waiting for a tool start to
    /// be durably committed, racing the control's durable authorization against
    /// the `ToolExecutionStart` commit barrier. The `ToolExecutionStart`
    /// barrier owns the tie because it marks the durable start; a control that
    /// is authorized before the barrier completes preempts the tool, and a
    /// control that is still being authorized when the barrier completes is
    /// deferred and processed after the tool starts.
    async fn apply_pre_start_control(
        &mut self,
        control: RunControl,
        committed: &mut oneshot::Receiver<ToolStartCommitResult>,
    ) -> Result<ToolStartOutcome, WorkerFailure> {
        match control {
            RunControl::Command(command) => {
                self.core
                    .queue_followup(command)
                    .map_err(|error| WorkerFailure::Error(error.to_string()))?;
                Ok(ToolStartOutcome::Preempted)
            }
            RunControl::HardSteer { command, accepted } => {
                if accepted.send(true).is_ok() {
                    self.cancel_provider();
                    self.core
                        .queue_followup(command)
                        .map_err(|error| WorkerFailure::Error(error.to_string()))?;
                }
                Ok(ToolStartOutcome::Preempted)
            }
            RunControl::SoftSteer {
                command,
                accepted,
                committed: control_committed,
            }
            | RunControl::RetrySteer {
                command,
                accepted,
                committed: control_committed,
            } => {
                let command_id = command.envelope().command_id.clone();
                self.claim_control(command)?;
                if accepted.send(true).is_err() {
                    self.in_flight_controls.pop();
                    return Ok(ToolStartOutcome::Preempted);
                }

                let mut control_committed = control_committed;
                tokio::select! {
                    biased;
                    result = &mut *committed => {
                        let result = result.map_err(|_| {
                            WorkerFailure::Error("ToolExecutionStart durability commit failed".to_owned())
                        })?;
                        if result == ToolStartCommitResult::Reauthorize {
                            if (&mut control_committed).await.is_ok() {
                                return Ok(ToolStartOutcome::Preempted);
                            }
                            let released = self
                                .in_flight_controls
                                .pop()
                                .expect("pre-start steer remains claimed until authorization");
                            debug_assert_eq!(released.envelope().command_id, command_id);
                            return Ok(ToolStartOutcome::Reauthorize);
                        }
                        if (&mut control_committed).await.is_err() {
                            let released = self
                                .in_flight_controls
                                .pop()
                                .expect("pre-start steer remains claimed until authorization");
                            debug_assert_eq!(
                                released.envelope().command_id,
                                command_id
                            );
                        }
                        Ok(ToolStartOutcome::Started)
                    }
                    authorization = &mut control_committed => {
                        if authorization.is_ok() {
                            Ok(ToolStartOutcome::Preempted)
                        } else {
                            let released = self
                                .in_flight_controls
                                .pop()
                                .expect("pre-start steer remains claimed until authorization");
                            debug_assert_eq!(
                                released.envelope().command_id,
                                command_id
                            );
                            let result = (&mut *committed).await.map_err(|_| {
                                WorkerFailure::Error(
                                    "ToolExecutionStart durability commit failed".to_owned(),
                                )
                            })?;
                            Ok(match result {
                                ToolStartCommitResult::Committed => ToolStartOutcome::Started,
                                ToolStartCommitResult::Reauthorize => {
                                    ToolStartOutcome::Reauthorize
                                }
                            })
                        }
                    }
                }
            }
            RunControl::Abort {
                accepted,
                committed: control_committed,
                ..
            } => {
                if accepted.send(true).is_err() {
                    return Ok(ToolStartOutcome::Preempted);
                }

                let mut control_committed = control_committed;
                tokio::select! {
                    biased;
                    result = &mut *committed => {
                        let result = result.map_err(|_| {
                            WorkerFailure::Error("ToolExecutionStart durability commit failed".to_owned())
                        })?;
                        if result == ToolStartCommitResult::Reauthorize {
                            if (&mut control_committed).await.is_ok() {
                                self.abort_requested = true;
                                return Ok(ToolStartOutcome::Preempted);
                            }
                            return Ok(ToolStartOutcome::Reauthorize);
                        }
                        if (&mut control_committed).await.is_ok() {
                            self.abort_requested = true;
                        }
                        Ok(ToolStartOutcome::Started)
                    }
                    authorization = &mut control_committed => {
                        if authorization.is_ok() {
                            self.cancel_provider();
                            self.abort_requested = true;
                            Ok(ToolStartOutcome::Preempted)
                        } else {
                            let result = (&mut *committed).await.map_err(|_| {
                                WorkerFailure::Error(
                                    "ToolExecutionStart durability commit failed".to_owned(),
                                )
                            })?;
                            Ok(match result {
                                ToolStartCommitResult::Committed => ToolStartOutcome::Started,
                                ToolStartCommitResult::Reauthorize => {
                                    ToolStartOutcome::Reauthorize
                                }
                            })
                        }
                    }
                }
            }
        }
    }

    async fn receive_control_safe_point(&mut self) -> Result<(), WorkerFailure> {
        // Drain the control lane. Soft/retry steer commands join the in-flight
        // group; ordinary commands and hard-steer/abort stop after one.
        while let Ok(control) = self.controls.try_recv() {
            match control {
                RunControl::Command(command) => {
                    self.core
                        .queue_followup(command)
                        .map_err(|error| WorkerFailure::Error(error.to_string()))?;
                    return Ok(());
                }
                RunControl::HardSteer { accepted, .. } => {
                    // A hard steer is valid only when the active provider
                    // attempt consumes it in `provider_attempt_loop`. Reaching
                    // a later safe point means that attempt has already
                    // terminalized, so accepting here would hand Session a
                    // stale cancellation reservation.
                    if accepted.send(false).is_ok() {
                        return Ok(());
                    }
                }
                RunControl::SoftSteer {
                    command,
                    accepted,
                    committed,
                } => {
                    self.accept_steer_control(command, accepted, committed)
                        .await?;
                }
                RunControl::Abort {
                    accepted,
                    committed,
                    ..
                } => {
                    if self.accept_abort_control(accepted, committed).await? {
                        self.cancel_provider();
                        self.abort_requested = true;
                        return Ok(());
                    }
                }
                RunControl::RetrySteer {
                    accepted,
                    committed,
                    ..
                } => {
                    let _ = accepted.send(false);
                    drop(committed);
                }
            }
        }
        Ok(())
    }

    fn cancel_provider(&mut self) {
        if let Some(cancel) = self.provider_cancel.as_ref() {
            cancel.cancel();
        }
    }

    async fn accept_steer_control(
        &mut self,
        command: AdmittedCommand,
        accepted: oneshot::Sender<bool>,
        committed: oneshot::Receiver<()>,
    ) -> Result<bool, WorkerFailure> {
        let command_id = command.envelope().command_id.clone();
        self.claim_control(command)?;
        if accepted.send(true).is_err() {
            // Session timed out or observed a phase change and will durably
            // defer this same command. Do not claim it into RunCore or wait on
            // an authorization that cannot arrive.
            let released = self
                .in_flight_controls
                .pop()
                .expect("steer control accepted only after exact claim");
            debug_assert_eq!(released.envelope().command_id, command_id);
            return Ok(false);
        }
        if committed.await.is_err() {
            let released = self
                .in_flight_controls
                .pop()
                .expect("steer control remains claimed until durable authorization");
            debug_assert_eq!(released.envelope().command_id, command_id);
            return Ok(false);
        }
        Ok(true)
    }

    async fn accept_abort_control(
        &mut self,
        accepted: oneshot::Sender<bool>,
        committed: oneshot::Receiver<()>,
    ) -> Result<bool, WorkerFailure> {
        if accepted.send(true).is_err() {
            return Ok(false);
        }
        // A dropped sender is not authorization. The Session may have failed
        // its transaction and will recover the still-durable command; the
        // worker must not convert that control-plane failure into an external
        // provider/tool cancellation.
        Ok(committed.await.is_ok())
    }

    async fn wait_retry_or_control(&mut self, delay: Duration) -> Result<bool, WorkerFailure> {
        if self.claim_pending_user()? {
            return Ok(true);
        }
        let cancel = self.cancel.child_token();
        let driver = self.driver.clone();
        let retry = driver.wait_retry(delay, &cancel);
        tokio::pin!(retry);
        let mut collected = false;
        const COLLECT_GRACE: Duration = Duration::from_millis(50);
        let mut grace: Option<Pin<Box<tokio::time::Sleep>>> = None;
        let runtime_cancel = self.cancel.clone();
        loop {
            let control = tokio::select! {
                biased;
                _ = runtime_cancel.cancelled() => {
                    cancel.cancel();
                    return Err(WorkerFailure::Cancelled);
                }
                control = self.controls.recv() => {
                    let Some(control) = control else {
                        return Err(WorkerFailure::Cancelled);
                    };
                    control
                }
                completed = &mut retry => {
                    if collected {
                        return Ok(true);
                    }
                    return if completed {
                        Ok(false)
                    } else {
                        Err(WorkerFailure::Cancelled)
                    };
                }
                _ = async {
                    if let Some(g) = grace.as_mut() {
                        g.as_mut().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                }, if collected && grace.is_some() => {
                    return Ok(true);
                }
            };
            match control {
                RunControl::Command(command) => {
                    self.core
                        .queue_followup(command)
                        .map_err(|error| WorkerFailure::Error(error.to_string()))?;
                    if self.claim_pending_user()? {
                        return Ok(true);
                    }
                    continue;
                }
                RunControl::HardSteer { command, accepted } => {
                    if accepted.send(true).is_ok() {
                        self.cancel_provider();
                        self.claim_control(command)?;
                        return Ok(true);
                    }
                }
                RunControl::Abort {
                    accepted,
                    committed,
                    ..
                } => {
                    if self.accept_abort_control(accepted, committed).await? {
                        self.cancel_provider();
                        self.abort_requested = true;
                        return Err(WorkerFailure::Cancelled);
                    }
                }
                RunControl::SoftSteer {
                    command,
                    accepted,
                    committed,
                }
                | RunControl::RetrySteer {
                    command,
                    accepted,
                    committed,
                } => {
                    if self
                        .accept_steer_control(command, accepted, committed)
                        .await?
                    {
                        collected = true;
                        let deadline = tokio::time::Instant::now() + COLLECT_GRACE;
                        grace = Some(Box::pin(tokio::time::sleep_until(deadline)));
                    }
                    continue;
                }
            }
        }
    }

    fn recover_received_controls(&mut self) -> Result<(), WorkerFailure> {
        self.controls.close();
        for command in self.in_flight_controls.drain(..).rev() {
            self.core
                .requeue_followup_front(command)
                .map_err(|error| WorkerFailure::Error(error.to_string()))?;
        }
        while let Ok(control) = self.controls.try_recv() {
            match control {
                RunControl::Command(command) | RunControl::HardSteer { command, .. } => self
                    .core
                    .queue_followup(command)
                    .map_err(|error| WorkerFailure::Error(error.to_string()))?,
                RunControl::SoftSteer {
                    command, accepted, ..
                } => {
                    let _ = accepted.send(false);
                    self.core
                        .queue_followup(command)
                        .map_err(|error| WorkerFailure::Error(error.to_string()))?;
                }
                RunControl::Abort { accepted, .. } => {
                    let _ = accepted.send(false);
                }
                RunControl::RetrySteer {
                    command, accepted, ..
                } => {
                    let _ = accepted.send(false);
                    self.core
                        .queue_followup(command)
                        .map_err(|error| WorkerFailure::Error(error.to_string()))?;
                }
            }
        }
        Ok(())
    }

    async fn emit(&mut self, event: AgentEvent) -> Result<(), WorkerFailure> {
        let binding = self.core.durable_binding.clone().ok_or_else(|| {
            WorkerFailure::Error("RunCore has no durable worker binding".to_owned())
        })?;
        self.events
            .send(RunOutput {
                binding,
                event,
                commit_barrier: None,
                message_commit_barrier: None,
                retry_wait_commit_barrier: None,
                approval_command: None,
                approval_not_started: None,
                approval_cancelled: None,
            })
            .await
            .map_err(|_| WorkerFailure::EventChannelClosed)
    }

    async fn emit_message_end(
        &mut self,
        message_id: String,
        message: PublicMessage,
        approval_not_started: Option<String>,
        approval_cancelled: Option<String>,
    ) -> Result<oneshot::Receiver<MessageCommitReceipt>, WorkerFailure> {
        self.emit_message_end_with_provider_context(
            message_id,
            message,
            Vec::new(),
            None,
            approval_not_started,
            approval_cancelled,
        )
        .await
    }

    async fn emit_provider_message_end(
        &mut self,
        message_id: String,
        message: PublicMessage,
        provider_context: Vec<crate::provider::types::ProviderContextFragment>,
        terminal_kind: ProviderTerminalKind,
        uncalibrated_prompt_estimate: u64,
    ) -> Result<oneshot::Receiver<MessageCommitReceipt>, WorkerFailure> {
        let calibration_estimate = match &message {
            PublicMessage::Assistant(assistant)
                if terminal_kind == ProviderTerminalKind::Done
                    && !matches!(
                        assistant.stop_reason,
                        StopReason::Error | StopReason::Aborted
                    )
                    && uncalibrated_prompt_estimate > 0 =>
            {
                let observed = observed_prompt_tokens(&assistant.usage)
                    .map_err(|error| WorkerFailure::Error(error.to_string()))?;
                (observed > 0).then_some(uncalibrated_prompt_estimate)
            }
            _ => None,
        };
        self.emit_message_end_with_provider_context(
            message_id,
            message,
            provider_context,
            calibration_estimate,
            None,
            None,
        )
        .await
    }

    async fn emit_message_end_with_provider_context(
        &mut self,
        message_id: String,
        message: PublicMessage,
        provider_context: Vec<crate::provider::types::ProviderContextFragment>,
        calibration_estimate: Option<u64>,
        approval_not_started: Option<String>,
        approval_cancelled: Option<String>,
    ) -> Result<oneshot::Receiver<MessageCommitReceipt>, WorkerFailure> {
        let binding = self.core.durable_binding.clone().ok_or_else(|| {
            WorkerFailure::Error("RunCore has no durable worker binding".to_owned())
        })?;
        let (barrier, receipt) = match calibration_estimate {
            Some(estimate) => MessageCommitBarrier::channel_with_provider_context_and_calibration(
                provider_context,
                estimate,
            ),
            None => MessageCommitBarrier::channel_with_provider_context(provider_context),
        };
        self.events
            .send(RunOutput {
                binding,
                event: AgentEvent::MessageEnd {
                    message_id,
                    message: Box::new(message),
                },
                commit_barrier: None,
                message_commit_barrier: Some(barrier),
                retry_wait_commit_barrier: None,
                approval_command: None,
                approval_not_started,
                approval_cancelled,
            })
            .await
            .map_err(|_| WorkerFailure::EventChannelClosed)?;
        Ok(receipt)
    }

    async fn emit_retry_scheduled(
        &mut self,
        attempt: u32,
        delay: Duration,
        error_message: String,
    ) -> Result<(), WorkerFailure> {
        let binding = self.core.durable_binding.clone().ok_or_else(|| {
            WorkerFailure::Error("RunCore has no durable worker binding".to_owned())
        })?;
        let (barrier, committed) = RetryWaitCommitBarrier::channel();
        self.events
            .send(RunOutput {
                binding,
                event: AgentEvent::RetryScheduled {
                    attempt,
                    delay_ms: delay.as_millis() as u64,
                    retry_at: Utc::now() + chrono::Duration::from_std(delay).unwrap_or_default(),
                    error_message,
                },
                commit_barrier: None,
                message_commit_barrier: None,
                retry_wait_commit_barrier: Some(barrier),
                approval_command: None,
                approval_not_started: None,
                approval_cancelled: None,
            })
            .await
            .map_err(|_| WorkerFailure::EventChannelClosed)?;
        committed
            .await
            .map_err(|_| WorkerFailure::Error("RetryScheduled durability commit failed".to_owned()))
    }

    async fn await_message_receipt(
        &self,
        receipt: oneshot::Receiver<MessageCommitReceipt>,
    ) -> Result<MessageCommitReceipt, WorkerFailure> {
        receipt
            .await
            .map_err(|_| WorkerFailure::Error("MessageEnd durability commit failed".to_owned()))
    }

    fn retain_committed(
        &mut self,
        receipt: MessageCommitReceipt,
        message: &PublicMessage,
    ) -> Result<(), WorkerFailure> {
        if stop_reason(message) == Some(StopReason::Error) {
            return Ok(());
        }
        if let Some((origin, fragments)) = self.pending_provider_context.remove(&receipt.message_id)
        {
            let items = crate::provider::types::bind_provider_context_fragments(
                fragments,
                crate::provider::types::ProviderContextAnchor {
                    message_id: receipt.message_id.clone(),
                    message_seq: receipt.message_seq,
                },
                origin.clone(),
            )
            .map_err(WorkerFailure::Error)?;
            let Some(spec) = ModelSpec::from_origin(&origin) else {
                return Err(WorkerFailure::Error(format!(
                    "no canonical ModelSpec for provider origin {origin:?}"
                )));
            };
            let items: Result<Vec<_>, _> = items
                .into_iter()
                .map(|item| {
                    let footprint = eviction_footprint_for_payload(&spec, &item.payload)
                        .map_err(|e| WorkerFailure::Error(e.to_string()))?;
                    Ok(ProviderContextItemWithFootprint::new(item, footprint))
                })
                .collect();
            self.provider_context.extend(items?);
            crate::provider::types::validate_provider_context_ordinals(&self.provider_context)
                .map_err(WorkerFailure::Error)?;
        }
        self.context.push(ContextMessage::Persisted {
            id: receipt.message_id,
            seq: receipt.message_seq,
            message: public_to_message(message.clone()),
        });
        self.core.mark_mutated();
        Ok(())
    }

    fn retain_tool_results(
        &mut self,
        receipts: &[MessageCommitReceipt],
        results: &[ToolResultMessage],
    ) -> Result<(), WorkerFailure> {
        if receipts.len() != results.len() {
            return Err(WorkerFailure::Error(
                "tool-result receipt cardinality mismatch".to_owned(),
            ));
        }
        for (receipt, result) in receipts.iter().cloned().zip(results) {
            self.retain_committed(receipt, &PublicMessage::ToolResult(result.clone()))?;
        }
        Ok(())
    }

    async fn start_next_turn(&mut self) -> Result<(), WorkerFailure> {
        let binding = self.core.durable_binding.as_mut().ok_or_else(|| {
            WorkerFailure::Error("RunCore has no durable worker binding".to_owned())
        })?;
        binding.turn_id = Uuid::now_v7().to_string();
        self.emit(AgentEvent::TurnStart).await
    }
}

impl Drop for Runner {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Cancels an externally-backed operation before its future is dropped. This
/// ordering lets child producers/process reapers observe cancellation even
/// when the owning worker task itself is aborted.
struct CancelOnDrop<F> {
    future: Pin<Box<F>>,
    cancel: Option<CancellationToken>,
}

impl<F> CancelOnDrop<F> {
    fn new(future: F, cancel: CancellationToken) -> Self {
        Self {
            future: Box::pin(future),
            cancel: Some(cancel),
        }
    }
}

impl<F: Future> Future for CancelOnDrop<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.future.as_mut().poll(cx) {
            Poll::Ready(output) => {
                self.cancel = None;
                Poll::Ready(output)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F> Drop for CancelOnDrop<F> {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel.cancel();
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum AttemptOutcome {
    Retry {
        assistant_message_id: String,
        message: PublicMessage,
        receipt: oneshot::Receiver<MessageCommitReceipt>,
        rejected_results: Vec<ToolResultMessage>,
    },
    ImmediateOverflow {
        assistant_message_id: String,
        message: PublicMessage,
        receipt: oneshot::Receiver<MessageCommitReceipt>,
        source: OverflowSource,
        rejected_results: Vec<ToolResultMessage>,
    },
    Terminal {
        assistant_message_id: String,
        message: PublicMessage,
        assistant_message: AssistantMessage,
        provider_context: Vec<ProviderContextFragment>,
        receipt: oneshot::Receiver<MessageCommitReceipt>,
        rejected_results: Vec<ToolResultMessage>,
        deferred_overflow: Option<OverflowSource>,
        length_guarded: bool,
    },
    ClosedError {
        assistant_message_id: String,
        message: PublicMessage,
        receipt: oneshot::Receiver<MessageCommitReceipt>,
        rejected_results: Vec<ToolResultMessage>,
    },
    HardSteer,
}

#[derive(Clone, Copy)]
enum SyntheticAttemptFailure {
    Start,
    InvalidMessageId,
    Abort,
}

fn tool_calls(message: &PublicMessage) -> Vec<ToolCall> {
    let PublicMessage::Assistant(message) = message else {
        return Vec::new();
    };
    message
        .content
        .iter()
        .filter_map(|content| match content {
            PublicAssistantContent::ToolCall { tool_call, .. } => Some(tool_call.clone()),
            _ => None,
        })
        .collect()
}

fn validate_and_order_rejected_results(
    message: &crate::provider::types::AssistantMessage,
    results: &mut [ToolResultMessage],
) -> Result<(), &'static str> {
    let mut terminal_rejections = Vec::new();
    let mut executable_ids = HashSet::new();
    for content in &message.content {
        match content {
            AssistantContent::ToolCall { tool_call, .. } => {
                executable_ids.insert(tool_call.id.as_str());
            }
            AssistantContent::RejectedToolCall { rejected, .. } => {
                terminal_rejections.push((rejected.id.as_str(), rejected.name.as_str()));
            }
            _ => {}
        }
    }

    let unique_terminal_ids: HashSet<_> = terminal_rejections.iter().map(|(id, _)| *id).collect();
    if unique_terminal_ids.len() != terminal_rejections.len() {
        return Err("terminal contains duplicate rejected tool-call IDs");
    }
    if terminal_rejections
        .iter()
        .any(|(id, _)| executable_ids.contains(id))
    {
        return Err("a terminal tool-call ID is both executable and rejected");
    }
    if terminal_rejections.len() != results.len() {
        return Err("terminal rejection/result cardinality differs");
    }

    let unique_result_ids: HashSet<_> = results
        .iter()
        .map(|result| result.tool_call_id.as_str())
        .collect();
    if unique_result_ids.len() != results.len() {
        return Err("stream contains duplicate rejected-result tool-call IDs");
    }
    for result in results.iter() {
        let Some((_, terminal_name)) = terminal_rejections
            .iter()
            .find(|(terminal_id, _)| *terminal_id == result.tool_call_id)
        else {
            return Err("terminal rejection/result identities differ");
        };
        if *terminal_name != result.tool_name {
            return Err("terminal rejection/result tool names differ");
        }
        if !result.is_error {
            return Err("rejected synthetic result is not an error");
        }
    }
    results.sort_by_key(|result| {
        terminal_rejections
            .iter()
            .position(|(terminal_id, _)| *terminal_id == result.tool_call_id)
            .expect("validated rejected result identity")
    });
    Ok(())
}

fn stop_reason(message: &PublicMessage) -> Option<StopReason> {
    match message {
        PublicMessage::Assistant(message) => Some(message.stop_reason),
        _ => None,
    }
}

fn context_message(message: &ContextMessage) -> &Message {
    match message {
        ContextMessage::Persisted { message, .. } | ContextMessage::Synthetic { message } => {
            message
        }
    }
}

pub(super) fn public_to_message(message: PublicMessage) -> Message {
    message.into()
}

fn message_to_public(message: Message) -> PublicMessage {
    match message {
        Message::User(message) => PublicMessage::User(message),
        Message::ToolResult(message) => PublicMessage::ToolResult(message),
        Message::Assistant(message) => {
            PublicMessage::Assistant(crate::provider::types::PublicAssistantMessage {
                content: message
                    .content
                    .into_iter()
                    .map(|content| match content {
                        AssistantContent::Text {
                            text,
                            wire_item_index,
                        } => PublicAssistantContent::Text {
                            text,
                            wire_item_index,
                        },
                        AssistantContent::Thinking {
                            thinking,
                            signature_field,
                            wire_item_index,
                        } => PublicAssistantContent::Thinking {
                            thinking,
                            signature_field,
                            wire_item_index,
                        },
                        AssistantContent::ToolCall {
                            tool_call,
                            wire_item_index,
                        } => PublicAssistantContent::ToolCall {
                            tool_call,
                            wire_item_index,
                        },
                        AssistantContent::RejectedToolCall {
                            rejected,
                            wire_item_index,
                        } => PublicAssistantContent::RejectedToolCall {
                            rejected,
                            wire_item_index,
                        },
                    })
                    .collect(),
                model: message.model,
                provider: message.provider,
                origin: message.origin,
                usage: message.usage,
                stop_reason: message.stop_reason,
                error_message: message.error_message,
                provider_code: message.provider_code,
                interrupted: message.interrupted,
                timestamp: message.timestamp,
            })
        }
    }
}

fn assistant_error(message: &PublicMessage) -> String {
    match message {
        PublicMessage::Assistant(message) => message
            .error_message
            .clone()
            .unwrap_or_else(|| "provider error".to_owned()),
        _ => "provider error".to_owned(),
    }
}

fn normalize_immediate_overflow(
    message: &PublicMessage,
    source: OverflowSource,
    rejected_results: &[ToolResultMessage],
) -> PublicMessage {
    let PublicMessage::Assistant(message) = message else {
        unreachable!("provider terminal message is always assistant")
    };
    let mut normalized = message.clone();
    normalized.content.retain(|content| match content {
        PublicAssistantContent::ToolCall { .. } => false,
        PublicAssistantContent::RejectedToolCall { rejected, .. } => rejected_results
            .iter()
            .any(|result| result.tool_call_id == rejected.id),
        PublicAssistantContent::Text { .. } | PublicAssistantContent::Thinking { .. } => true,
    });
    normalized.stop_reason = StopReason::Error;
    if source == OverflowSource::LengthUsage {
        normalized.error_message = Some(LENGTH_OVERFLOW_ERROR.to_owned());
        normalized.provider_code = Some(LENGTH_OVERFLOW_CODE.to_owned());
    } else if normalized.error_message.is_none() {
        normalized.error_message = Some(format!(
            "provider context overflow requires immediate recovery ({source:?})"
        ));
    }
    normalized.interrupted = false;
    PublicMessage::Assistant(normalized)
}

fn normalize_length_loop_guard(message: &PublicMessage) -> PublicMessage {
    let PublicMessage::Assistant(message) = message else {
        unreachable!("provider terminal message is always assistant")
    };
    let mut normalized = message.clone();
    normalized.stop_reason = StopReason::Error;
    normalized.error_message = Some(LENGTH_LOOP_FAILURE.to_owned());
    normalized.provider_code = Some(LENGTH_LOOP_CODE.to_owned());
    normalized.interrupted = false;
    PublicMessage::Assistant(normalized)
}

fn error_tool_result(call: &ToolCall, message: &str) -> ToolResultMessage {
    ToolResultMessage {
        tool_call_id: call.id.clone(),
        tool_name: call.name.clone(),
        content: vec![UserContent::Text {
            text: message.to_owned(),
        }],
        details: json!({ "error": message }),
        is_error: true,
        timestamp: Utc::now(),
    }
}

fn tool_result_message_id(assistant_message_id: &str, tool_call_id: &str) -> String {
    // Hash each variable-length identity independently so pair framing is
    // unambiguous without constructing an unbounded concatenated name.
    let assistant_digest = Sha256::digest(assistant_message_id.as_bytes());
    let tool_call_digest = Sha256::digest(tool_call_id.as_bytes());
    let mut pair_digest = [0_u8; 64];
    pair_digest[..32].copy_from_slice(&assistant_digest);
    pair_digest[32..].copy_from_slice(&tool_call_digest);
    Uuid::new_v5(&TOOL_RESULT_MESSAGE_ID_NAMESPACE, &pair_digest).to_string()
}

fn synthetic_attempt_message_id(
    binding: &DurableRunBinding,
    attempt: usize,
    failure: SyntheticAttemptFailure,
) -> Result<String, WorkerFailure> {
    let attempt = u64::try_from(attempt).map_err(|_| {
        WorkerFailure::Error(
            "provider attempt ordinal exceeds its durable identity range".to_owned(),
        )
    })?;
    let run_digest = Sha256::digest(binding.run_id.as_bytes());
    let turn_digest = Sha256::digest(binding.turn_id.as_bytes());
    let mut name = [0_u8; 73];
    name[..32].copy_from_slice(&run_digest);
    name[32..64].copy_from_slice(&turn_digest);
    name[64..72].copy_from_slice(&attempt.to_be_bytes());
    name[72] = match failure {
        SyntheticAttemptFailure::Start => 0,
        SyntheticAttemptFailure::InvalidMessageId => 1,
        SyntheticAttemptFailure::Abort => 2,
    };
    Ok(Uuid::new_v5(&SYNTHETIC_ATTEMPT_MESSAGE_ID_NAMESPACE, &name).to_string())
}

#[cfg(test)]
mod tests;
