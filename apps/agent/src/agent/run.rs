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
    gateway::{Command, CommandEnvelope},
    provider::{
        overflow::classify_context_overflow,
        retry::{is_retryable, retry_delay, sleep_or_cancel},
        types::{
            ProviderEvent, ProviderEventStream, PublicAssistantContent, PublicMessage, StopReason,
            ToolCall, ToolResultMessage, UserContent, UserMessage,
        },
    },
    store::user_message_id,
};

use super::{
    AgentEvent, ProjectedProviderEvent, ProviderEventProjector, ProviderTerminalKind,
    RunCompletion, RunControl, RunCore, RunWorker, SteerMode, WorkerFailure, WorkerFuture,
};

const LENGTH_TOOL_FAILURE: &str = "Tool call was not executed: the response hit the output token limit, so its arguments may be truncated. Re-issue the tool call with complete arguments.";
const LENGTH_LOOP_FAILURE: &str = "provider produced tool calls at the output token limit twice consecutively; refusing a third provider call";

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
        initial: CommandEnvelope,
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
    attempt: usize,
    consecutive_length_batches: usize,
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
            attempt: 0,
            consecutive_length_batches: 0,
        }
    }

    async fn run(mut self, initial: CommandEnvelope) -> RunCompletion {
        let result = self.run_inner(initial).await;
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

    async fn run_inner(&mut self, initial: CommandEnvelope) -> Result<(), WorkerFailure> {
        self.emit(AgentEvent::AgentStart).await?;
        self.emit(AgentEvent::TurnStart).await?;
        self.inject_user(initial).await?;

        loop {
            if let Some(command) = self.poll_control_safe_point()? {
                self.emit(AgentEvent::Steered {
                    mode: SteerMode::Soft,
                })
                .await?;
                self.inject_user(command).await?;
            }
            let outcome = self.provider_attempt().await?;
            match outcome {
                AttemptOutcome::Retry { message } => {
                    let Some(delay) = retry_delay(self.attempt) else {
                        self.close_turn(message, Vec::new()).await?;
                        break;
                    };
                    self.attempt += 1;
                    self.emit(AgentEvent::RetryScheduled {
                        attempt: self.attempt as u32,
                        delay_ms: delay.as_millis() as u64,
                        retry_at: Utc::now()
                            + chrono::Duration::from_std(delay).unwrap_or_default(),
                        error_message: assistant_error(&message),
                    })
                    .await?;
                    if let Some(command) = self.wait_retry_or_control(delay).await? {
                        self.emit(AgentEvent::Steered {
                            mode: SteerMode::Soft,
                        })
                        .await?;
                        self.inject_user(command).await?;
                    }
                }
                AttemptOutcome::Terminal {
                    message,
                    rejected_results,
                } => {
                    self.attempt = 0;
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
                    if let Some(command) = self.poll_control_safe_point()? {
                        self.core
                            .queue_followup(command)
                            .map_err(|error| WorkerFailure::Error(error.to_string()))?;
                    }

                    if is_length && self.consecutive_length_batches >= 2 {
                        break;
                    }

                    // A provider terminal carrying executable calls always
                    // continues with a fresh turn after every result settles.
                    self.emit(AgentEvent::TurnStart).await?;
                    if let Some(command) = self.core.next_followup() {
                        self.inject_user(command).await?;
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
            .start_provider(self.attempt, &self.context, cancel)
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
                    let public = terminal.message().clone();
                    self.emit(terminal.event().clone()).await?;
                    let internal =
                        terminal_message.expect("terminal projection has provider output");
                    if kind == ProviderTerminalKind::Error {
                        // Error assistants remain observable but never enter L0/context.
                        if internal.stop_reason == StopReason::Error
                            && classify_context_overflow(&internal, self.driver.context_window())
                                .is_none()
                            && is_retryable(&internal)
                        {
                            return Ok(AttemptOutcome::Retry { message: public });
                        }
                        return Ok(AttemptOutcome::ClosedError { message: public });
                    }
                    return Ok(AttemptOutcome::Terminal {
                        message: public,
                        rejected_results,
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
        let message_id = format!("synthetic-error-{}", self.attempt);
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
            self.emit_tool_start(call).await?;
            let message = if terminal_guard {
                format!("{LENGTH_TOOL_FAILURE} {LENGTH_LOOP_FAILURE}")
            } else {
                LENGTH_TOOL_FAILURE.to_owned()
            };
            let result = error_tool_result(call, &message);
            self.emit_tool_result(&result).await?;
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

    async fn inject_user(&mut self, command: CommandEnvelope) -> Result<(), WorkerFailure> {
        let Command::UserMessage { text, attachments } = command.command else {
            return Err(WorkerFailure::Error(
                "non-user command reached a user injection boundary".to_owned(),
            ));
        };
        debug_assert!(attachments.is_empty());
        let message = PublicMessage::User(UserMessage {
            content: vec![UserContent::Text { text }],
            timestamp: Utc::now(),
        });
        let message_id = user_message_id(&command.command_id);
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

    async fn advance_followup(&mut self) -> Result<bool, WorkerFailure> {
        if let Some(command) = self.poll_control_safe_point()? {
            self.core
                .queue_followup(command)
                .map_err(|error| WorkerFailure::Error(error.to_string()))?;
        }
        let Some(command) = self.core.next_followup() else {
            return Ok(false);
        };
        self.emit(AgentEvent::TurnStart).await?;
        self.inject_user(command).await?;
        Ok(true)
    }

    fn poll_control_safe_point(&mut self) -> Result<Option<CommandEnvelope>, WorkerFailure> {
        // Deliberately consume at most one ordinary message: one-at-a-time is
        // the product contract and keeps each queued instruction an answer turn.
        if let Ok(RunControl::Command(command)) = self.controls.try_recv() {
            if matches!(command.command, Command::UserMessage { .. }) {
                return Ok(Some(command));
            } else {
                // T16 owns application of Abort/ApprovalDecision. Retaining the
                // already-admitted command in the returned unique core avoids
                // silently consuming it in this earlier lifecycle slice.
                self.core
                    .defer_control(command)
                    .map_err(|error| WorkerFailure::Error(error.to_string()))?;
            }
        }
        Ok(None)
    }

    async fn wait_retry_or_control(
        &mut self,
        delay: Duration,
    ) -> Result<Option<CommandEnvelope>, WorkerFailure> {
        let cancel = CancellationToken::new();
        let injected = tokio::select! {
            biased;
            control = self.controls.recv() => {
                if let Some(RunControl::Command(command)) = control
                {
                    if matches!(command.command, Command::UserMessage { .. }) {
                        Some(command)
                    } else {
                        self.core.defer_control(command)
                            .map_err(|error| WorkerFailure::Error(error.to_string()))?;
                        None
                    }
                } else {
                    None
                }
            }
            _ = self.driver.wait_retry(delay, &cancel) => None
        };
        Ok(injected)
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
    Terminal {
        message: PublicMessage,
        rejected_results: Vec<ToolResultMessage>,
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
