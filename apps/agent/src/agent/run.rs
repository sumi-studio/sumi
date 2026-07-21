//! Sequential provider/tool lifecycle for one active run.
//!
//! Durable command phase transitions remain owned by `Session`/`EventWriter`.
//! This module owns only the in-memory lifecycle after an admitted user command
//! has been transferred together with the unique [`RunCore`].

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    gateway::Command,
    provider::{
        overflow::{OverflowClassification, OverflowSource, classify_context_overflow},
        retry::{is_retryable, retry_delay, sleep_or_cancel},
        types::{
            ProviderEvent, ProviderEventStream, PublicAssistantContent, PublicMessage, StopReason,
            ToolCall, ToolResultMessage, UserContent, UserMessage,
        },
    },
    store::user_message_id,
};

use super::{
    AdmittedCommand, AgentEvent, ProjectedProviderEvent, ProviderEventProjector,
    ProviderTerminalKind, RunCompletion, RunControl, RunCore, RunWorker, SteerMode, WorkerFailure,
    WorkerFuture,
};

const LENGTH_TOOL_FAILURE: &str = "Tool call was not executed: the response hit the output token limit, so its arguments may be truncated. Re-issue the tool call with complete arguments.";
const LENGTH_LOOP_FAILURE: &str = "provider produced tool calls at the output token limit twice consecutively; refusing a third provider call";
const LENGTH_OVERFLOW_ERROR: &str = "provider response reached the context window before producing output; immediate recovery required";
const LENGTH_OVERFLOW_CODE: &str = "context_overflow_length_usage";
const MAX_OVERFLOW_RECOVERIES: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OverflowRecoveryRequest {
    pub(crate) source: OverflowSource,
    pub(crate) ordinal: u8,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum OverflowRecoveryOutcome {
    ReplacementContext(Vec<PublicMessage>),
}

/// One provider attempt. The initial public message supplies stable model and
/// origin metadata for `MessageStart`; the stream remains the authority for
/// the terminal message.
pub(crate) struct ProviderAttempt {
    pub(crate) message_id: String,
    pub(crate) initial_message: PublicMessage,
    pub(crate) events: ProviderEventStream,
}

/// Narrow runtime boundary. Production wiring may build provider context from
/// the supplied snapshot and dispatch tools through the existing executor;
/// unit fixtures can remain transport- and credential-free.
#[async_trait]
pub(crate) trait RunDriver: Send + Sync + 'static {
    async fn start_provider(
        &self,
        attempt: usize,
        context: &[PublicMessage],
        cancel: CancellationToken,
    ) -> Result<ProviderAttempt>;

    async fn execute_tool(
        &self,
        call: &ToolCall,
        cancel: CancellationToken,
    ) -> Result<ToolResultMessage>;

    fn synthetic_error(&self, message: &str) -> PublicMessage;

    fn context_window(&self) -> Option<u64> {
        None
    }

    /// Applies one bounded emergency memory recovery before the next provider
    /// attempt. There is intentionally no default: T21 production wiring must
    /// mutate the supplied core and return the exact replacement send context.
    async fn recover_overflow(
        &self,
        core: &mut RunCore,
        request: OverflowRecoveryRequest,
        active_context: &[PublicMessage],
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
    fn run(
        &self,
        core: RunCore,
        initial: AdmittedCommand,
        controls: mpsc::Receiver<RunControl>,
        events: mpsc::Sender<AgentEvent>,
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
    events: mpsc::Sender<AgentEvent>,
    context: Vec<PublicMessage>,
    attempt_sequence: usize,
    ordinary_retries: usize,
    overflow_recoveries: u8,
    consecutive_length_batches: usize,
    in_flight_control: Option<AdmittedCommand>,
}

impl Runner {
    fn new(
        mut core: RunCore,
        driver: Arc<dyn RunDriver>,
        controls: mpsc::Receiver<RunControl>,
        events: mpsc::Sender<AgentEvent>,
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
                AttemptOutcome::Retry { message } => {
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
                AttemptOutcome::ImmediateOverflow { message, source } => {
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
                        .recover_overflow(&mut self.core, request, &self.context)
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
                    if let Err(error) = self.install_recovered_context(replacement) {
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
                }
                AttemptOutcome::Terminal {
                    message,
                    rejected_results,
                    deferred_overflow,
                } => {
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
                        self.close_turn(message, Vec::new()).await?;
                        if !self.advance_followup().await? {
                            break;
                        }
                        continue;
                    }

                    let is_length =
                        !calls.is_empty() && stop_reason(&message) == Some(StopReason::Length);
                    let mut results = if is_length {
                        self.consecutive_length_batches += 1;
                        self.fail_length_calls(&calls, self.consecutive_length_batches >= 2)
                            .await?
                    } else {
                        self.consecutive_length_batches = 0;
                        self.execute_calls(&calls).await?
                    };
                    for result in rejected_results {
                        self.emit_result_message(&result).await?;
                        results.push(result);
                    }
                    self.context.push(message.clone());
                    self.context
                        .extend(results.iter().cloned().map(PublicMessage::ToolResult));
                    self.emit(AgentEvent::TurnEnd {
                        message: Some(Box::new(message)),
                        tool_results: results,
                    })
                    .await?;
                    self.receive_control_safe_point()?;

                    if is_length && self.consecutive_length_batches >= 2 {
                        break;
                    }

                    // A provider terminal carrying executable calls always
                    // continues with a fresh turn after every result settles.
                    self.emit(AgentEvent::TurnStart).await?;
                    if self.claim_pending_user()? {
                        self.inject_in_flight().await?;
                    }
                }
                AttemptOutcome::ClosedError { message } => {
                    self.close_turn(message, Vec::new()).await?;
                    break;
                }
            }
        }
        self.emit(AgentEvent::AgentEnd).await
    }

    async fn provider_attempt(&mut self) -> Result<AttemptOutcome, WorkerFailure> {
        let cancel = CancellationToken::new();
        let mut attempt = match self
            .driver
            .start_provider(self.attempt_sequence, &self.context, cancel)
            .await
        {
            Ok(attempt) => attempt,
            Err(error) => return self.synthetic_attempt_error(error.to_string()).await,
        };
        let mut projector = match ProviderEventProjector::new(attempt.message_id.clone()) {
            Ok(projector) => projector,
            Err(error) => return self.synthetic_attempt_error(error.to_string()).await,
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
                    let kind = terminal.kind();
                    let internal =
                        terminal_message.expect("terminal projection has provider output");
                    let overflow = terminal_overflow;
                    let public = match overflow {
                        Some(OverflowClassification::ImmediateRecovery(
                            OverflowSource::LengthUsage,
                        )) => normalize_length_overflow(terminal.message()),
                        _ => terminal.message().clone(),
                    };
                    let terminal_event = match terminal.event() {
                        AgentEvent::MessageEnd { message_id, .. } => AgentEvent::MessageEnd {
                            message_id: message_id.clone(),
                            message: Box::new(public.clone()),
                        },
                        _ => unreachable!("provider terminal is always MessageEnd"),
                    };
                    self.emit(terminal_event).await?;
                    if let Some(OverflowClassification::ImmediateRecovery(source)) = overflow {
                        return Ok(AttemptOutcome::ImmediateOverflow {
                            message: public,
                            source,
                        });
                    }
                    if kind == ProviderTerminalKind::Error {
                        // Error assistants remain observable but never enter L0/context.
                        if internal.stop_reason == StopReason::Error && is_retryable(&internal) {
                            return Ok(AttemptOutcome::Retry { message: public });
                        }
                        return Ok(AttemptOutcome::ClosedError { message: public });
                    }
                    return Ok(AttemptOutcome::Terminal {
                        message: public,
                        rejected_results,
                        deferred_overflow: match overflow {
                            Some(OverflowClassification::DeferredApply(source)) => Some(source),
                            _ => None,
                        },
                    });
                }
            }
        }
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
    ) -> Result<AttemptOutcome, WorkerFailure> {
        let message = self.driver.synthetic_error(&error);
        let message_id = format!("synthetic-error-{}", self.attempt_sequence);
        self.emit(AgentEvent::MessageStart {
            message_id: message_id.clone(),
            message: Box::new(message.clone()),
        })
        .await?;
        self.emit(AgentEvent::MessageEnd {
            message_id,
            message: Box::new(message.clone()),
        })
        .await?;
        Ok(AttemptOutcome::ClosedError { message })
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
        self.emit(AgentEvent::MessageEnd {
            message_id: message_id.to_owned(),
            message: Box::new(message.clone()),
        })
        .await?;
        Ok(AttemptOutcome::ClosedError { message })
    }

    async fn fail_length_calls(
        &mut self,
        calls: &[ToolCall],
        terminal_guard: bool,
    ) -> Result<Vec<ToolResultMessage>, WorkerFailure> {
        let mut results = Vec::with_capacity(calls.len());
        for call in calls {
            let message = if terminal_guard {
                format!("{LENGTH_TOOL_FAILURE} {LENGTH_LOOP_FAILURE}")
            } else {
                LENGTH_TOOL_FAILURE.to_owned()
            };
            let result = error_tool_result(call, &message);
            self.emit_result_message(&result).await?;
            results.push(result);
        }
        Ok(results)
    }

    async fn execute_calls(
        &mut self,
        calls: &[ToolCall],
    ) -> Result<Vec<ToolResultMessage>, WorkerFailure> {
        let mut results = Vec::with_capacity(calls.len());
        for call in calls {
            self.emit_tool_start(call).await?;
            let result = match self
                .driver
                .execute_tool(call, CancellationToken::new())
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
            self.emit_tool_result(&result).await?;
            results.push(result);
        }
        Ok(results)
    }

    async fn emit_tool_start(&mut self, call: &ToolCall) -> Result<(), WorkerFailure> {
        self.emit(AgentEvent::ToolExecutionStart {
            tool_call_id: call.id.clone(),
            tool_name: call.name.clone(),
            args: Value::Object(call.arguments.as_object().clone()),
        })
        .await
    }

    async fn emit_tool_result(&mut self, result: &ToolResultMessage) -> Result<(), WorkerFailure> {
        self.emit(AgentEvent::ToolExecutionEnd {
            tool_call_id: result.tool_call_id.clone(),
            result: json!({
                "content": result.content,
                "details": result.details,
            }),
            is_error: result.is_error,
        })
        .await?;
        self.emit_result_message(result).await
    }

    async fn emit_result_message(
        &mut self,
        result: &ToolResultMessage,
    ) -> Result<(), WorkerFailure> {
        let message = PublicMessage::ToolResult(result.clone());
        let message_id = format!("tool-result:{}", result.tool_call_id);
        self.emit(AgentEvent::MessageStart {
            message_id: message_id.clone(),
            message: Box::new(message.clone()),
        })
        .await?;
        self.emit(AgentEvent::MessageEnd {
            message_id,
            message: Box::new(message),
        })
        .await
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
        self.emit(AgentEvent::MessageEnd {
            message_id,
            message: Box::new(message.clone()),
        })
        .await?;
        self.context.push(message);
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
            self.in_flight_control = None;
        }
        result
    }

    async fn close_turn(
        &mut self,
        message: PublicMessage,
        tool_results: Vec<ToolResultMessage>,
    ) -> Result<(), WorkerFailure> {
        if stop_reason(&message) != Some(StopReason::Error) {
            self.context.push(message.clone());
        }
        self.context
            .extend(tool_results.iter().cloned().map(PublicMessage::ToolResult));
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

    fn install_recovered_context(
        &mut self,
        replacement: Vec<PublicMessage>,
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
            .find(|message| matches!(message, PublicMessage::User(_)))
            && !replacement.contains(active_user)
        {
            return Err(WorkerFailure::Error(
                "overflow recovery dropped the active user from the send context".to_owned(),
            ));
        }
        self.context = replacement;
        Ok(())
    }

    async fn advance_followup(&mut self) -> Result<bool, WorkerFailure> {
        self.receive_control_safe_point()?;
        if !self.claim_pending_user()? {
            return Ok(false);
        }
        self.emit(AgentEvent::TurnStart).await?;
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
        self.events
            .send(event)
            .await
            .map_err(|_| WorkerFailure::EventChannelClosed)
    }
}

enum AttemptOutcome {
    Retry {
        message: PublicMessage,
    },
    ImmediateOverflow {
        message: PublicMessage,
        source: OverflowSource,
    },
    Terminal {
        message: PublicMessage,
        rejected_results: Vec<ToolResultMessage>,
        deferred_overflow: Option<OverflowSource>,
    },
    ClosedError {
        message: PublicMessage,
    },
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

fn stop_reason(message: &PublicMessage) -> Option<StopReason> {
    match message {
        PublicMessage::Assistant(message) => Some(message.stop_reason),
        _ => None,
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

fn normalize_length_overflow(message: &PublicMessage) -> PublicMessage {
    let PublicMessage::Assistant(message) = message else {
        unreachable!("provider terminal message is always assistant")
    };
    let mut normalized = message.clone();
    normalized.stop_reason = StopReason::Error;
    normalized.error_message = Some(LENGTH_OVERFLOW_ERROR.to_owned());
    normalized.provider_code = Some(LENGTH_OVERFLOW_CODE.to_owned());
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

#[cfg(test)]
mod tests;
