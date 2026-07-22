//! Sequential provider/tool lifecycle for one active run.
//!
//! Durable command phase transitions remain owned by `Session`/`EventWriter`.
//! This module owns only the in-memory lifecycle after an admitted user command
//! has been transferred together with the unique [`RunCore`].

use std::{
    collections::HashSet,
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
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    gateway::Command,
    provider::{
        overflow::{OverflowClassification, OverflowSource, classify_context_overflow},
        retry::{is_retryable, retry_delay, sleep_or_cancel},
        types::{
            AssistantContent, ContextMessage, Message, ProviderEvent, ProviderEventStream,
            PublicAssistantContent, PublicMessage, StopReason, ToolCall, ToolResultMessage,
            UserContent, UserMessage,
        },
    },
    runtime::contracts::ProcessGeneration,
    store::user_message_id,
};

use super::{
    AdmittedCommand, AgentEvent, DurableRunBinding, MessageCommitBarrier, MessageCommitReceipt,
    ProjectedProviderEvent, ProviderEventProjector, ProviderTerminalKind, RunCompletion,
    RunControl, RunCore, RunOutput, RunWorker, SteerMode, ToolStartCommitBarrier, WorkerFailure,
    WorkerFuture,
};

const LENGTH_TOOL_FAILURE: &str = "Tool call was not executed: the response hit the output token limit, so its arguments may be truncated. Re-issue the tool call with complete arguments.";
const LENGTH_LOOP_FAILURE: &str = "provider produced tool calls at the output token limit twice consecutively; refusing a third provider call";
pub(super) const LENGTH_LOOP_CODE: &str = "consecutive_length_tool_guard";
const LENGTH_OVERFLOW_ERROR: &str = "provider response reached the context window before producing output; immediate recovery required";
const LENGTH_OVERFLOW_CODE: &str = "context_overflow_length_usage";
const MAX_OVERFLOW_RECOVERIES: u8 = 2;
const TOOL_RESULT_MESSAGE_ID_NAMESPACE: Uuid = Uuid::from_bytes([
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
}

/// One provider attempt. The initial public message supplies stable model and
/// origin metadata for `MessageStart`; the stream remains the authority for
/// the terminal message.
pub(crate) struct ProviderAttempt {
    /// Stable, conversation-global durable message identity. Reusing an ID in
    /// another run would collide with the Store's globally keyed message row.
    pub(crate) message_id: String,
    pub(crate) initial_message: PublicMessage,
    pub(crate) events: ProviderEventStream,
}

/// Narrow runtime boundary. Production wiring may build provider context from
/// the supplied snapshot and dispatch tools through the existing executor;
/// unit fixtures can remain transport- and credential-free.
#[async_trait]
pub(crate) trait RunDriver: Send + Sync + 'static {
    /// Fail closed before Session creates keys, recovery state, or a worker.
    fn validate_executor_generation(&self, generation: ProcessGeneration) -> Result<()>;

    async fn start_provider(
        &self,
        attempt: usize,
        context: &[ContextMessage],
        cancel: CancellationToken,
    ) -> Result<ProviderAttempt>;

    async fn start_provider_for_command(
        &self,
        attempt: usize,
        context: &[ContextMessage],
        _command_received_at: Option<std::time::Instant>,
        cancel: CancellationToken,
    ) -> Result<ProviderAttempt> {
        self.start_provider(attempt, context, cancel).await
    }

    async fn execute_tool(
        &self,
        call: &ToolCall,
        cancel: CancellationToken,
    ) -> Result<ToolResultMessage>;

    async fn execute_tool_observed(
        &self,
        _flow_id: &str,
        call: &ToolCall,
        cancel: CancellationToken,
        _on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<ToolResultMessage> {
        self.execute_tool(call, cancel).await
    }

    fn synthetic_error(&self, message: &str) -> PublicMessage;

    fn context_window(&self) -> Option<u64> {
        None
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
    fn validate_executor_generation(&self, generation: ProcessGeneration) -> Result<()> {
        self.driver.validate_executor_generation(generation)
    }

    fn run(
        &self,
        core: RunCore,
        initial: AdmittedCommand,
        controls: mpsc::Receiver<RunControl>,
        events: mpsc::Sender<RunOutput>,
    ) -> WorkerFuture {
        let driver = self.driver.clone();
        Box::pin(async move {
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
    context: Vec<ContextMessage>,
    attempt_sequence: usize,
    ordinary_retries: usize,
    overflow_recoveries: u8,
    consecutive_length_batches: usize,
    in_flight_control: Option<AdmittedCommand>,
    pending_command_received_at: Option<std::time::Instant>,
}

impl Runner {
    fn new(
        mut core: RunCore,
        driver: Arc<dyn RunDriver>,
        controls: mpsc::Receiver<RunControl>,
        events: mpsc::Sender<RunOutput>,
    ) -> Self {
        let context = std::mem::take(&mut core.runtime_context);
        Self {
            core,
            driver,
            controls,
            events,
            context,
            attempt_sequence: 0,
            ordinary_retries: 0,
            overflow_recoveries: 0,
            consecutive_length_batches: 0,
            in_flight_control: None,
            pending_command_received_at: None,
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
        self.core.runtime_context = self.context;
        self.core.mark_mutated();
        match result {
            Ok(()) => RunCompletion::Completed(self.core),
            Err(failure) => RunCompletion::Failed {
                core: self.core,
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
            self.receive_control_safe_point()?;
            let outcome = self.provider_attempt().await?;
            self.attempt_sequence = self.attempt_sequence.saturating_add(1);
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
                    self.emit(AgentEvent::RetryScheduled {
                        attempt: self.attempt_sequence as u32,
                        delay_ms: delay.as_millis() as u64,
                        retry_at: Utc::now()
                            + chrono::Duration::from_std(delay).unwrap_or_default(),
                        error_message: assistant_error(&message),
                    })
                    .await?;
                    if self.wait_retry_or_control(delay).await? {
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
                    let OverflowRecoveryOutcome::ReplacementContext(replacement) = outcome;
                    if let Err(error) = self.validate_recovered_context(&replacement) {
                        tracing::error!(%error, ?source, "immediate overflow recovery was invalid");
                        self.close_turn_without_context(message).await?;
                        break;
                    }
                    self.emit(AgentEvent::RetryScheduled {
                        attempt: self.attempt_sequence as u32,
                        delay_ms: 0,
                        retry_at: Utc::now(),
                        error_message: format!("context overflow: {source:?}"),
                    })
                    .await?;
                    self.context = replacement;
                }
                AttemptOutcome::Terminal {
                    assistant_message_id,
                    message,
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
                        self.retain_committed(receipt, &message)?;
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
                    let (executable_results, executable_receipts) = if is_length {
                        self.consecutive_length_batches += 1;
                        self.fail_length_calls(&assistant_message_id, &calls, length_guarded)
                            .await?
                    } else {
                        self.consecutive_length_batches = 0;
                        self.execute_calls(&assistant_message_id, &calls).await?
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
                    self.emit(AgentEvent::TurnEnd {
                        message: Some(Box::new(message)),
                        tool_results: executable_results,
                    })
                    .await?;
                    self.receive_control_safe_point()?;

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
                    self.await_message_receipt(receipt).await?;
                    self.close_turn(message, Vec::new()).await?;
                    break;
                }
            }
        }
        self.emit(AgentEvent::AgentEnd).await
    }

    async fn provider_attempt(&mut self) -> Result<AttemptOutcome, WorkerFailure> {
        let cancel = CancellationToken::new();
        let start_cancel = cancel.clone();
        // A command ingress timestamp has exactly one causal consumer: the
        // first provider request started after that command is injected.
        // Retries and tool continuations keep their own TTFT observation, but
        // must not fold provider/backoff/tool time into agent internal p95.
        let command_received_at = self.pending_command_received_at.take();
        let start = self.driver.start_provider_for_command(
            self.attempt_sequence,
            &self.context,
            command_received_at,
            cancel,
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

        while let Some(event) = attempt.events.recv().await {
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
                    if !terminal.provider_context().is_empty() {
                        rejected_results.clear();
                        return self
                            .close_broken_attempt(
                                &attempt.message_id,
                                message_started,
                                "provider terminal context requires the T17 durable hand-off; refusing to persist opaque context"
                                    .to_owned(),
                            )
                            .await;
                    }
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
                    let (terminal_message_id, terminal_message) = match terminal.event() {
                        AgentEvent::MessageEnd { message_id, .. } => {
                            (message_id.clone(), public.clone())
                        }
                        _ => unreachable!("provider terminal is always MessageEnd"),
                    };
                    let receipt = self
                        .emit_message_end(terminal_message_id, terminal_message)
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
        // EOF has no authoritative assistant snapshot containing buffered
        // rejections, so their synthetic results must not be emitted.
        drop(rejected_results);
        self.close_broken_attempt(
            &attempt.message_id,
            message_started,
            "provider stream ended without a terminal event".to_owned(),
        )
        .await
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
            .emit_message_end(message_id.clone(), message.clone())
            .await?;
        Ok(AttemptOutcome::ClosedError {
            assistant_message_id: message_id,
            message,
            receipt,
            rejected_results: Vec::new(),
        })
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
            .emit_message_end(message_id.to_owned(), message.clone())
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
                .emit_result_message(assistant_message_id, &result)
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
        calls: &[ToolCall],
    ) -> Result<(Vec<ToolResultMessage>, Vec<MessageCommitReceipt>), WorkerFailure> {
        let mut results = Vec::with_capacity(calls.len());
        let mut receipts = Vec::with_capacity(calls.len());
        for call in calls {
            self.emit_tool_start_and_wait_committed(call).await?;
            let result = match self
                .execute_tool_with_updates(assistant_message_id, call)
                .await
            {
                Ok(mut result) => {
                    // The invocation identity is authoritative at this seam.
                    result.tool_call_id.clone_from(&call.id);
                    result.tool_name.clone_from(&call.name);
                    result
                }
                Err(error) => error_tool_result(call, &format!("Tool execution failed: {error}")),
            };
            receipts.push(self.emit_tool_result(assistant_message_id, &result).await?);
            results.push(result);
        }
        Ok((results, receipts))
    }

    async fn execute_tool_with_updates(
        &mut self,
        flow_id: &str,
        call: &ToolCall,
    ) -> Result<ToolResultMessage> {
        const TOOL_UPDATE_CAPACITY: usize = 32;
        let (updates_tx, mut updates_rx) = mpsc::channel(TOOL_UPDATE_CAPACITY);
        let callback_call_id = call.id.clone();
        let on_update: Arc<dyn Fn(Value) + Send + Sync> = Arc::new(move |partial| {
            // Progress is volatile. Never block a tool or bypass the bounded
            // Session event lane; a saturated progress lane coalesces by drop.
            let _ = updates_tx.try_send((callback_call_id.clone(), partial));
        });
        let driver = self.driver.clone();
        let cancel = CancellationToken::new();
        let future = CancelOnDrop::new(
            driver.execute_tool_observed(flow_id, call, cancel.clone(), on_update),
            cancel,
        );
        tokio::pin!(future);
        let result = loop {
            tokio::select! {
                biased;
                result = &mut future => break result,
                update = updates_rx.recv() => {
                    if let Some((tool_call_id, partial)) = update {
                        self.emit(AgentEvent::ToolExecutionUpdate { tool_call_id, partial }).await?;
                    }
                }
            }
        };
        while let Ok((tool_call_id, partial)) = updates_rx.try_recv() {
            self.emit(AgentEvent::ToolExecutionUpdate {
                tool_call_id,
                partial,
            })
            .await?;
        }
        result
    }

    async fn emit_tool_start_and_wait_committed(
        &mut self,
        call: &ToolCall,
    ) -> Result<(), WorkerFailure> {
        let binding = self.core.durable_binding.clone().ok_or_else(|| {
            WorkerFailure::Error("RunCore has no durable worker binding".to_owned())
        })?;
        let (commit_barrier, committed) = ToolStartCommitBarrier::channel();
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
            })
            .await
            .map_err(|_| WorkerFailure::EventChannelClosed)?;
        committed.await.map_err(|_| {
            WorkerFailure::Error("ToolExecutionStart durability commit failed".to_owned())
        })
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
            .emit_result_message(assistant_message_id, result)
            .await?;
        let receipt = self.await_message_receipt(receipt).await?;
        Ok(receipt)
    }

    async fn emit_result_message(
        &mut self,
        assistant_message_id: &str,
        result: &ToolResultMessage,
    ) -> Result<oneshot::Receiver<MessageCommitReceipt>, WorkerFailure> {
        let message = PublicMessage::ToolResult(result.clone());
        let message_id = tool_result_message_id(assistant_message_id, &result.tool_call_id);
        self.emit(AgentEvent::MessageStart {
            message_id: message_id.clone(),
            message: Box::new(message.clone()),
        })
        .await?;
        self.emit_message_end(message_id, message).await
    }

    async fn emit_rejected_results(
        &mut self,
        assistant_message_id: &str,
        results: &[ToolResultMessage],
    ) -> Result<Vec<MessageCommitReceipt>, WorkerFailure> {
        let mut pending = Vec::with_capacity(results.len());
        for result in results {
            pending.push(
                self.emit_result_message(assistant_message_id, result)
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
        let message_id = user_message_id(&command.envelope().command_id);
        self.emit(AgentEvent::MessageStart {
            message_id: message_id.clone(),
            message: Box::new(message.clone()),
        })
        .await?;
        let receipt = self.emit_message_end(message_id, message.clone()).await?;
        let receipt = self.await_message_receipt(receipt).await?;
        self.retain_committed(receipt, &message)?;
        Ok(())
    }

    fn claim_control(&mut self, command: AdmittedCommand) -> Result<(), WorkerFailure> {
        if self.in_flight_control.is_some() {
            return Err(WorkerFailure::Error(
                "a second control cannot be claimed while injection is in flight".to_owned(),
            ));
        }
        self.in_flight_control = Some(command);
        Ok(())
    }

    async fn inject_in_flight(&mut self) -> Result<(), WorkerFailure> {
        let injectable = self
            .in_flight_control
            .as_ref()
            .expect("caller must claim a control before injection")
            .clone();
        let result = self.inject_user(&injectable).await;
        if result.is_ok() {
            self.pending_command_received_at = injectable.received_monotonic();
            self.in_flight_control = None;
        }
        result
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
        self.receive_control_safe_point()?;
        if !self.claim_pending_user()? {
            return Ok(false);
        }
        self.start_next_turn().await?;
        self.inject_in_flight().await?;
        Ok(true)
    }

    fn claim_pending_user(&mut self) -> Result<bool, WorkerFailure> {
        if self.in_flight_control.is_some() {
            return Err(WorkerFailure::Error(
                "pending control cannot be popped while injection is in flight".to_owned(),
            ));
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

    fn receive_control_safe_point(&mut self) -> Result<(), WorkerFailure> {
        // Deliberately consume at most one ordinary message: one-at-a-time is
        // the product contract and keeps each queued instruction an answer turn.
        if let Ok(RunControl::Command(command)) = self.controls.try_recv() {
            self.core
                .queue_followup(command)
                .map_err(|error| WorkerFailure::Error(error.to_string()))?;
        }
        Ok(())
    }

    async fn wait_retry_or_control(&mut self, delay: Duration) -> Result<bool, WorkerFailure> {
        if self.claim_pending_user()? {
            return Ok(true);
        }
        let cancel = CancellationToken::new();
        let injected = tokio::select! {
            biased;
            control = self.controls.recv() => {
                let Some(RunControl::Command(command)) = control else {
                    return Err(WorkerFailure::Cancelled);
                };
                self.core.queue_followup(command)
                    .map_err(|error| WorkerFailure::Error(error.to_string()))?;
                if self.claim_pending_user()? {
                    true
                } else if self.driver.wait_retry(delay, &cancel).await {
                    false
                } else {
                    return Err(WorkerFailure::Cancelled);
                }
            }
            completed = self.driver.wait_retry(delay, &cancel) => {
                if completed {
                    false
                } else {
                    return Err(WorkerFailure::Cancelled);
                }
            }
        };
        Ok(injected)
    }

    fn recover_received_controls(&mut self) -> Result<(), WorkerFailure> {
        self.controls.close();
        if let Some(command) = self.in_flight_control.take() {
            self.core
                .requeue_followup_front(command)
                .map_err(|error| WorkerFailure::Error(error.to_string()))?;
        }
        while let Ok(RunControl::Command(command)) = self.controls.try_recv() {
            self.core
                .queue_followup(command)
                .map_err(|error| WorkerFailure::Error(error.to_string()))?;
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
            })
            .await
            .map_err(|_| WorkerFailure::EventChannelClosed)
    }

    async fn emit_message_end(
        &mut self,
        message_id: String,
        message: PublicMessage,
    ) -> Result<oneshot::Receiver<MessageCommitReceipt>, WorkerFailure> {
        let binding = self.core.durable_binding.clone().ok_or_else(|| {
            WorkerFailure::Error("RunCore has no durable worker binding".to_owned())
        })?;
        let (barrier, receipt) = MessageCommitBarrier::channel();
        self.events
            .send(RunOutput {
                binding,
                event: AgentEvent::MessageEnd {
                    message_id,
                    message: Box::new(message),
                },
                commit_barrier: None,
                message_commit_barrier: Some(barrier),
            })
            .await
            .map_err(|_| WorkerFailure::EventChannelClosed)?;
        Ok(receipt)
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
        self.context.push(ContextMessage::Persisted {
            id: receipt.message_id,
            seq: receipt.message_seq,
            message: public_to_message(message.clone()),
        });
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
}

#[derive(Clone, Copy)]
enum SyntheticAttemptFailure {
    Start,
    InvalidMessageId,
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
    match message {
        PublicMessage::User(message) => Message::User(message),
        PublicMessage::ToolResult(message) => Message::ToolResult(message),
        PublicMessage::Assistant(message) => {
            Message::Assistant(crate::provider::types::AssistantMessage {
                content: message
                    .content
                    .into_iter()
                    .map(|content| match content {
                        PublicAssistantContent::Text {
                            text,
                            wire_item_index,
                        } => AssistantContent::Text {
                            text,
                            wire_item_index,
                        },
                        PublicAssistantContent::Thinking {
                            thinking,
                            signature_field,
                            wire_item_index,
                        } => AssistantContent::Thinking {
                            thinking,
                            signature_field,
                            wire_item_index,
                        },
                        PublicAssistantContent::ToolCall {
                            tool_call,
                            wire_item_index,
                        } => AssistantContent::ToolCall {
                            tool_call,
                            wire_item_index,
                        },
                        PublicAssistantContent::RejectedToolCall {
                            rejected,
                            wire_item_index,
                        } => AssistantContent::RejectedToolCall {
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
    };
    Ok(Uuid::new_v5(&SYNTHETIC_ATTEMPT_MESSAGE_ID_NAMESPACE, &name).to_string())
}

#[cfg(test)]
mod tests;
