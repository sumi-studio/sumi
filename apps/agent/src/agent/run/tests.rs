use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use anyhow::{Result, anyhow};
use chrono::{TimeZone, Utc};
use serde_json::{Value, json};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{
    agent::DurableRunBinding,
    gateway::{CommandEnvelope, CommandId},
    provider::types::{
        ApiProtocol, AssistantContent, AssistantMessage, ContextMessage, ProviderContextFragment,
        ProviderContextPayload, ProviderOrigin, ProviderOutput, PublicAssistantMessage,
        RejectedToolCall, ToolArgumentError, Usage, ValidatedToolArguments,
    },
    runtime::contracts::ProcessGeneration,
    tools::ToolError,
};

fn test_executor_generation() -> ProcessGeneration {
    ProcessGeneration::from_wire(73).expect("valid test generation")
}

#[derive(Clone)]
enum Script {
    Output(Box<AssistantMessage>),
    Events(Vec<ProviderEvent>),
    StartFailure(&'static str),
}

fn output(message: AssistantMessage) -> Script {
    Script::Output(Box::new(message))
}

struct FixtureDriver {
    scripts: Mutex<VecDeque<Script>>,
    started_contexts: Mutex<Vec<Vec<ContextMessage>>>,
    started_command_times: Mutex<Vec<Option<std::time::Instant>>>,
    tool_order: Mutex<Vec<String>>,
    tool_failures: Mutex<VecDeque<Option<&'static str>>>,
    active_tools: AtomicUsize,
    max_active_tools: AtomicUsize,
    retry_waits: AtomicUsize,
    retry_delays: Mutex<Vec<Duration>>,
    retry_waiting: Notify,
    block_retry: bool,
    retry_result: bool,
    context_window: Option<u64>,
    overflow_recoveries: Mutex<Vec<OverflowRecoveryRequest>>,
    overflow_core_epochs: Mutex<Vec<u64>>,
    overflow_contexts: Mutex<VecDeque<Vec<PublicMessage>>>,
}

impl FixtureDriver {
    fn new(scripts: Vec<Script>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            started_contexts: Mutex::new(Vec::new()),
            started_command_times: Mutex::new(Vec::new()),
            tool_order: Mutex::new(Vec::new()),
            tool_failures: Mutex::new(VecDeque::new()),
            active_tools: AtomicUsize::new(0),
            max_active_tools: AtomicUsize::new(0),
            retry_waits: AtomicUsize::new(0),
            retry_delays: Mutex::new(Vec::new()),
            retry_waiting: Notify::new(),
            block_retry: false,
            retry_result: true,
            context_window: None,
            overflow_recoveries: Mutex::new(Vec::new()),
            overflow_core_epochs: Mutex::new(Vec::new()),
            overflow_contexts: Mutex::new(VecDeque::new()),
        }
    }

    fn with_tool_failures(self, failures: Vec<Option<&'static str>>) -> Self {
        *self.tool_failures.lock().expect("tool failures") = failures.into();
        self
    }

    fn blocking_retry(mut self) -> Self {
        self.block_retry = true;
        self
    }

    fn cancelled_retry(mut self) -> Self {
        self.retry_result = false;
        self
    }

    fn with_context_window(mut self, context_window: u64) -> Self {
        self.context_window = Some(context_window);
        self
    }

    fn with_overflow_contexts(self, contexts: Vec<Vec<PublicMessage>>) -> Self {
        *self.overflow_contexts.lock().expect("overflow contexts") = contexts.into();
        self
    }
}

fn recovered_context_from_active(
    messages: Vec<PublicMessage>,
    active_context: &[ContextMessage],
) -> Vec<ContextMessage> {
    messages
        .into_iter()
        .map(|message| {
            let message = public_to_message(message);
            active_context
                .iter()
                .find(|candidate| context_message(candidate) == &message)
                .cloned()
                .unwrap_or(ContextMessage::Synthetic { message })
        })
        .collect()
}

#[async_trait]
impl RunDriver for FixtureDriver {
    fn validate_executor_generation(&self, generation: ProcessGeneration) -> Result<()> {
        (generation == test_executor_generation())
            .then_some(())
            .ok_or_else(|| anyhow!("fixture executor generation mismatch"))
    }

    async fn start_provider_for_command(
        &self,
        attempt: usize,
        context: &[ContextMessage],
        command_received_at: Option<std::time::Instant>,
        _cancel: CancellationToken,
    ) -> Result<ProviderAttempt> {
        self.started_command_times
            .lock()
            .expect("command times")
            .push(command_received_at);
        self.started_contexts
            .lock()
            .expect("contexts")
            .push(context.to_vec());
        let script = self
            .scripts
            .lock()
            .expect("scripts")
            .pop_front()
            .expect("provider script");
        match script {
            Script::Output(message) => Ok(provider_attempt(attempt, *message)),
            Script::Events(events) => Ok(provider_attempt_from_events(attempt, events)),
            Script::StartFailure(error) => Err(anyhow!(error)),
        }
    }

    async fn execute_tool_observed(
        &self,
        _flow_id: &str,
        call: &ToolCall,
        _cancel: CancellationToken,
        _on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<ToolResultMessage, ToolError> {
        let active = self.active_tools.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active_tools.fetch_max(active, Ordering::SeqCst);
        self.tool_order
            .lock()
            .expect("tool order")
            .push(call.id.clone());
        tokio::task::yield_now().await;
        self.active_tools.fetch_sub(1, Ordering::SeqCst);
        if let Some(Some(error)) = self
            .tool_failures
            .lock()
            .expect("tool failures")
            .pop_front()
        {
            return Err(ToolError::Protocol(error.to_owned()));
        }
        Ok(ToolResultMessage {
            tool_call_id: call.id.clone(),
            tool_name: call.name.clone(),
            content: Vec::new(),
            details: json!({"ok": call.id}),
            is_error: false,
            timestamp: timestamp(),
        })
    }

    fn synthetic_error(&self, message: &str) -> PublicMessage {
        public_message(&assistant(
            StopReason::Error,
            Vec::new(),
            Some(message),
            None,
        ))
    }

    fn context_window(&self) -> Option<u64> {
        self.context_window
    }

    async fn plan_overflow_recovery(
        &self,
        core: &RunCore,
        request: OverflowRecoveryRequest,
        active_context: &[ContextMessage],
    ) -> Result<OverflowRecoveryOutcome> {
        self.overflow_recoveries
            .lock()
            .expect("overflow recoveries")
            .push(request);
        self.overflow_core_epochs
            .lock()
            .expect("overflow core epochs")
            .push(core.mutation_epoch());
        let replacement = self
            .overflow_contexts
            .lock()
            .expect("overflow contexts")
            .pop_front()
            .ok_or_else(|| anyhow!("missing fixture overflow context"))?;
        Ok(OverflowRecoveryOutcome::ReplacementContext(
            recovered_context_from_active(replacement, active_context),
        ))
    }

    async fn wait_retry(&self, delay: Duration, _cancel: &CancellationToken) -> bool {
        self.retry_waits.fetch_add(1, Ordering::SeqCst);
        self.retry_delays.lock().expect("retry delays").push(delay);
        self.retry_waiting.notify_one();
        if self.block_retry {
            std::future::pending().await
        } else {
            self.retry_result
        }
    }
}

fn timestamp() -> chrono::DateTime<Utc> {
    Utc.timestamp_millis_opt(1_700_000_000_000)
        .single()
        .expect("timestamp")
}

fn origin() -> ProviderOrigin {
    ProviderOrigin {
        provider_instance_id: "fixture:https://example.invalid".to_owned(),
        protocol: ApiProtocol::OpenAiChatCompletions,
        model: "fixture-model".to_owned(),
    }
}

fn assistant(
    reason: StopReason,
    content: Vec<AssistantContent>,
    error: Option<&str>,
    code: Option<&str>,
) -> AssistantMessage {
    AssistantMessage {
        content,
        model: origin().model.clone(),
        provider: "fixture".to_owned(),
        origin: origin(),
        usage: Usage::default(),
        stop_reason: reason,
        error_message: error.map(str::to_owned),
        provider_code: code.map(str::to_owned),
        interrupted: reason == StopReason::Aborted,
        timestamp: timestamp(),
    }
}

fn assistant_with_usage(
    reason: StopReason,
    content: Vec<AssistantContent>,
    error: Option<&str>,
    code: Option<&str>,
    input: u64,
    output: u64,
) -> AssistantMessage {
    let mut message = assistant(reason, content, error, code);
    message.usage.input = input;
    message.usage.output = output;
    message.usage.total_tokens = input.saturating_add(output);
    message
}

fn public_message(message: &AssistantMessage) -> PublicMessage {
    PublicMessage::Assistant(PublicAssistantMessage {
        content: message
            .content
            .iter()
            .map(|content| match content {
                AssistantContent::Text {
                    text,
                    wire_item_index,
                } => PublicAssistantContent::Text {
                    text: text.clone(),
                    wire_item_index: *wire_item_index,
                },
                AssistantContent::Thinking {
                    thinking,
                    signature_field,
                    wire_item_index,
                } => PublicAssistantContent::Thinking {
                    thinking: thinking.clone(),
                    signature_field: signature_field.clone(),
                    wire_item_index: *wire_item_index,
                },
                AssistantContent::ToolCall {
                    tool_call,
                    wire_item_index,
                } => PublicAssistantContent::ToolCall {
                    tool_call: tool_call.clone(),
                    wire_item_index: *wire_item_index,
                },
                AssistantContent::RejectedToolCall {
                    rejected,
                    wire_item_index,
                } => PublicAssistantContent::RejectedToolCall {
                    rejected: rejected.clone(),
                    wire_item_index: *wire_item_index,
                },
            })
            .collect(),
        model: message.model.clone(),
        provider: message.provider.clone(),
        origin: message.origin.clone(),
        usage: message.usage.clone(),
        stop_reason: message.stop_reason,
        error_message: message.error_message.clone(),
        provider_code: message.provider_code.clone(),
        interrupted: message.interrupted,
        timestamp: message.timestamp,
    })
}

fn call(id: &str) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        name: format!("tool-{id}"),
        arguments: serde_json::from_value::<ValidatedToolArguments>(json!({"id": id}))
            .expect("arguments"),
    }
}

fn rejected(id: &str) -> RejectedToolCall {
    RejectedToolCall {
        id: id.to_owned(),
        name: format!("tool-{id}"),
        error: ToolArgumentError::InvalidJson,
    }
}

fn rejected_result(rejected: &RejectedToolCall) -> ToolResultMessage {
    ToolResultMessage {
        tool_call_id: rejected.id.clone(),
        tool_name: rejected.name.clone(),
        content: vec![UserContent::Text {
            text: "Tool arguments were rejected. Regenerate the tool call with complete, schema-valid arguments.".to_owned(),
        }],
        details: json!({
            "category": "invalid_json",
            "instance_path": "",
            "constraint": "json_syntax",
        }),
        is_error: true,
        timestamp: timestamp(),
    }
}

#[test]
fn rejected_results_follow_authoritative_terminal_order() {
    let first = rejected("ordered-first");
    let second = rejected("ordered-second");
    let message = assistant(
        StopReason::ToolUse,
        vec![
            AssistantContent::RejectedToolCall {
                rejected: first.clone(),
                wire_item_index: 0,
            },
            AssistantContent::RejectedToolCall {
                rejected: second.clone(),
                wire_item_index: 1,
            },
        ],
        None,
        None,
    );
    let mut results = vec![rejected_result(&second), rejected_result(&first)];

    validate_and_order_rejected_results(&message, &mut results)
        .expect("unique exact identities are unambiguous");

    assert_eq!(
        results
            .iter()
            .map(|result| result.tool_call_id.as_str())
            .collect::<Vec<_>>(),
        [first.id.as_str(), second.id.as_str()]
    );
}

fn provider_attempt(attempt: usize, message: AssistantMessage) -> ProviderAttempt {
    let (tx, rx) = mpsc::channel(16);
    tx.try_send(ProviderEvent::Start).expect("start");
    for content in &message.content {
        match content {
            AssistantContent::Text {
                text,
                wire_item_index,
            } => {
                let index = *wire_item_index as usize;
                tx.try_send(ProviderEvent::TextStart {
                    content_index: index,
                })
                .expect("text start");
                tx.try_send(ProviderEvent::TextDelta {
                    content_index: index,
                    delta: text.clone(),
                })
                .expect("text delta");
                tx.try_send(ProviderEvent::TextEnd {
                    content_index: index,
                    content: text.clone(),
                })
                .expect("text end");
            }
            AssistantContent::ToolCall {
                tool_call,
                wire_item_index,
            } => {
                let index = *wire_item_index as usize;
                tx.try_send(ProviderEvent::ToolCallStart {
                    content_index: index,
                })
                .expect("tool start");
                tx.try_send(ProviderEvent::ToolCallEnd {
                    content_index: index,
                    tool_call: tool_call.clone(),
                })
                .expect("tool end");
            }
            AssistantContent::RejectedToolCall {
                rejected,
                wire_item_index,
            } => {
                tx.try_send(ProviderEvent::ToolCallStart {
                    content_index: *wire_item_index as usize,
                })
                .expect("rejected tool start");
                tx.try_send(ProviderEvent::ToolCallRejected {
                    content_index: *wire_item_index as usize,
                    rejected: rejected.clone(),
                    synthetic_result: rejected_result(rejected),
                })
                .expect("tool rejected");
            }
            AssistantContent::Thinking { .. } => panic!("fixture helper does not need thinking"),
        }
    }
    let output = ProviderOutput {
        message: message.clone(),
        provider_context: Vec::new(),
    };
    let terminal = if matches!(message.stop_reason, StopReason::Error | StopReason::Aborted) {
        ProviderEvent::Error {
            reason: message.stop_reason,
            output,
        }
    } else {
        ProviderEvent::Done {
            reason: message.stop_reason,
            output,
        }
    };
    tx.try_send(terminal).expect("terminal");
    drop(tx);
    ProviderAttempt {
        message_id: format!("assistant-{attempt}"),
        initial_message: public_message(&assistant(StopReason::Stop, Vec::new(), None, None)),
        events: ProviderEventStream::new(rx, CancellationToken::new(), "fixture", origin()),
    }
}

fn provider_attempt_from_events(attempt: usize, events: Vec<ProviderEvent>) -> ProviderAttempt {
    let (tx, rx) = mpsc::channel(16);
    for event in events {
        tx.try_send(event).expect("fixture event");
    }
    drop(tx);
    ProviderAttempt {
        message_id: format!("assistant-{attempt}"),
        initial_message: public_message(&assistant(StopReason::Stop, Vec::new(), None, None)),
        events: ProviderEventStream::new(rx, CancellationToken::new(), "fixture", origin()),
    }
}

fn user(seq: u64) -> CommandEnvelope {
    CommandEnvelope {
        seq,
        command_id: CommandId::parse(&format!("00000000-0000-4000-8000-{seq:012}"))
            .expect("command id"),
        command: Command::UserMessage {
            text: format!("message {seq}"),
            attachments: Vec::new(),
        },
    }
}

fn admitted_user(seq: u64) -> AdmittedCommand {
    AdmittedCommand::new(user(seq), timestamp())
}

fn live_admitted_user(seq: u64) -> AdmittedCommand {
    AdmittedCommand::live(user(seq), timestamp(), std::time::Instant::now())
}

fn admitted_abort(seq: u64) -> AdmittedCommand {
    AdmittedCommand::new(
        CommandEnvelope {
            seq,
            command_id: CommandId::parse(&format!("00000000-0000-4000-8000-{seq:012}"))
                .expect("command id"),
            command: Command::Abort {},
        },
        timestamp(),
    )
}

fn runtime_user(seq: u64) -> PublicMessage {
    PublicMessage::User(UserMessage {
        content: vec![UserContent::Text {
            text: format!("message {seq}"),
        }],
        timestamp: timestamp(),
    })
}

fn recovered_context(label: &str) -> Vec<PublicMessage> {
    vec![
        runtime_user(1),
        public_message(&assistant(
            StopReason::Stop,
            vec![AssistantContent::Text {
                text: format!("recovered {label}"),
                wire_item_index: 0,
            }],
            None,
            None,
        )),
    ]
}

fn recovered_core(completion: RunCompletion) -> RunCore {
    match completion {
        RunCompletion::Completed(core) | RunCompletion::Failed { core, .. } => core,
    }
}

fn pending_sequences(core: &mut RunCore) -> Vec<u64> {
    let mut sequences = Vec::new();
    while let Some(command) = core.next_followup() {
        sequences.push(command.envelope().seq);
    }
    sequences
}

fn bound_core(seq: u64) -> RunCore {
    let command = admitted_user(seq);
    let mut core = RunCore::new();
    core.durable_binding = Some(DurableRunBinding::idle(
        &command,
        test_executor_generation(),
    ));
    core
}

async fn run_fixture(driver: Arc<FixtureDriver>) -> (RunCompletion, Vec<AgentEvent>) {
    run_fixture_with_initial(driver, admitted_user(1)).await
}

async fn run_fixture_with_initial(
    driver: Arc<FixtureDriver>,
    initial: AdmittedCommand,
) -> (RunCompletion, Vec<AgentEvent>) {
    let worker = SequentialRunWorker::new(driver);
    let (_control_tx, control_rx) = mpsc::channel(8);
    let (events_tx, mut events_rx) = mpsc::channel(256);
    let completion = tokio::spawn(async move {
        worker
            .run(bound_core(1), initial, control_rx, events_tx)
            .await
    });
    let mut events = Vec::new();
    let mut message_seq = 1;
    while let Some(mut output) = events_rx.recv().await {
        resolve_message_output(&mut output, &mut message_seq);
        if let Some(barrier) = output.commit_barrier.take() {
            barrier.committed();
        }
        events.push(output.event);
    }
    (completion.await.expect("worker join"), events)
}

fn resolve_message_output(output: &mut RunOutput, next_seq: &mut u64) {
    if let Some(barrier) = output.message_commit_barrier.take() {
        let AgentEvent::MessageEnd { message_id, .. } = &output.event else {
            panic!("message receipt barrier without MessageEnd");
        };
        barrier.resolve(MessageCommitReceipt {
            message_id: message_id.clone(),
            message_seq: *next_seq,
        });
        *next_seq += 1;
    }
    if let Some(barrier) = output.retry_wait_commit_barrier.take() {
        barrier.committed();
    }
}

async fn complete_with_receipts(
    future: WorkerFuture,
    mut events_rx: mpsc::Receiver<RunOutput>,
) -> RunCompletion {
    let completion = tokio::spawn(future);
    let mut message_seq = 1;
    while let Some(mut output) = events_rx.recv().await {
        resolve_message_output(&mut output, &mut message_seq);
        if let Some(barrier) = output.commit_barrier.take() {
            barrier.committed();
        }
    }
    completion.await.expect("worker join")
}

fn assert_completed(completion: RunCompletion) {
    assert!(matches!(completion, RunCompletion::Completed(_)));
}

#[tokio::test]
async fn successful_run_has_canonical_outer_order() {
    let driver = Arc::new(FixtureDriver::new(vec![output(assistant(
        StopReason::Stop,
        vec![AssistantContent::Text {
            text: "answer".to_owned(),
            wire_item_index: 0,
        }],
        None,
        None,
    ))]));
    let (completion, events) = run_fixture(driver).await;
    assert_completed(completion);
    assert!(matches!(events[0], AgentEvent::AgentStart));
    assert!(matches!(events[1], AgentEvent::TurnStart));
    assert!(matches!(events[2], AgentEvent::MessageStart { .. }));
    assert!(matches!(events[3], AgentEvent::MessageEnd { .. }));
    assert!(matches!(
        &events[3],
        AgentEvent::MessageEnd { message, .. }
            if matches!(message.as_ref(), PublicMessage::User(user) if user.timestamp == timestamp())
    ));
    assert!(matches!(events[4], AgentEvent::MessageStart { .. }));
    assert!(matches!(events.last(), Some(AgentEvent::AgentEnd)));
    assert!(matches!(
        events[events.len() - 2],
        AgentEvent::TurnEnd { .. }
    ));
}

#[tokio::test]
async fn retry_closes_error_before_schedule_and_does_not_append_error_context() {
    let driver = Arc::new(FixtureDriver::new(vec![
        output(assistant(
            StopReason::Error,
            Vec::new(),
            Some("network error"),
            Some("network_error"),
        )),
        output(assistant(StopReason::Stop, Vec::new(), None, None)),
    ]));
    let (completion, events) =
        run_fixture_with_initial(driver.clone(), live_admitted_user(1)).await;
    assert_completed(completion);
    let retry = events
        .iter()
        .position(|event| matches!(event, AgentEvent::RetryScheduled { .. }))
        .expect("retry event");
    assert!(matches!(events[retry - 1], AgentEvent::MessageEnd { .. }));
    assert!(matches!(events[retry + 1], AgentEvent::MessageStart { .. }));
    let contexts = driver.started_contexts.lock().expect("contexts");
    assert_eq!(contexts.len(), 2);
    assert_eq!(contexts[1].len(), 1, "only the user is replayable");
    assert!(matches!(context_message(&contexts[1][0]), Message::User(_)));
    assert_eq!(driver.retry_waits.load(Ordering::SeqCst), 1);
    let command_times = driver.started_command_times.lock().expect("command times");
    assert_eq!(command_times.len(), 2);
    assert!(command_times[0].is_some());
    assert_eq!(
        command_times[1], None,
        "retry backoff is not internal overhead"
    );
}

#[tokio::test]
async fn nonempty_provider_context_fails_closed_instead_of_being_dropped() {
    let message = assistant(StopReason::Stop, Vec::new(), None, None);
    let events = vec![
        ProviderEvent::Start,
        ProviderEvent::Done {
            reason: StopReason::Stop,
            output: ProviderOutput {
                message,
                provider_context: vec![ProviderContextFragment {
                    wire_item_index: Some(0),
                    payload: ProviderContextPayload::EncryptedReasoning {
                        protocol: ApiProtocol::OpenAiResponses,
                        item: json!({"encrypted_content":"opaque"}),
                    },
                }],
            },
        },
    ];
    let driver = Arc::new(FixtureDriver::new(vec![Script::Events(events)]));
    let (completion, emitted) = run_fixture(driver).await;
    assert_completed(completion);
    let end = emitted
        .iter()
        .find_map(|event| match event {
            AgentEvent::MessageEnd {
                message_id,
                message,
            } if message_id == "assistant-0" => Some(message.as_ref()),
            _ => None,
        })
        .expect("synthetic assistant close");
    assert!(matches!(
        end,
        PublicMessage::Assistant(assistant)
            if assistant.stop_reason == StopReason::Error
                && assistant.error_message.as_deref().is_some_and(|message| {
                    message.contains("T17 durable hand-off")
                        && message.contains("refusing to persist opaque context")
                })
    ));
    assert!(matches!(
        emitted[emitted.len() - 2],
        AgentEvent::TurnEnd { .. }
    ));
    assert!(matches!(emitted.last(), Some(AgentEvent::AgentEnd)));
}

#[tokio::test]
async fn retry_wait_control_is_injected_mid_turn_before_next_attempt() {
    let driver = Arc::new(
        FixtureDriver::new(vec![
            output(assistant(
                StopReason::Error,
                Vec::new(),
                Some("network error"),
                Some("network_error"),
            )),
            output(assistant(StopReason::Stop, Vec::new(), None, None)),
        ])
        .blocking_retry(),
    );
    let worker = SequentialRunWorker::new(driver.clone());
    let (control_tx, control_rx) = mpsc::channel(8);
    let (events_tx, mut events_rx) = mpsc::channel(256);
    let completion = tokio::spawn(async move {
        worker
            .run(bound_core(1), admitted_user(1), control_rx, events_tx)
            .await
    });
    let event_collector = tokio::spawn(async move {
        let mut events = Vec::new();
        let mut message_seq = 1;
        while let Some(mut output) = events_rx.recv().await {
            resolve_message_output(&mut output, &mut message_seq);
            if let Some(barrier) = output.commit_barrier.take() {
                barrier.committed();
            }
            events.push(output.event);
        }
        events
    });
    driver.retry_waiting.notified().await;
    control_tx
        .send(RunControl::Command(admitted_user(2)))
        .await
        .expect("retry steer");
    let completion = completion.await.expect("worker join");
    assert_completed(completion);
    let events = event_collector.await.expect("event collector");
    let retry = events
        .iter()
        .position(|event| matches!(event, AgentEvent::RetryScheduled { .. }))
        .expect("retry");
    assert!(matches!(events[retry + 1], AgentEvent::Steered { .. }));
    assert!(matches!(events[retry + 2], AgentEvent::MessageStart { .. }));
    assert!(matches!(events[retry + 3], AgentEvent::MessageEnd { .. }));
    assert!(matches!(events[retry + 4], AgentEvent::MessageStart { .. }));
    assert!(
        !events[retry + 1..retry + 5]
            .iter()
            .any(|event| matches!(event, AgentEvent::TurnStart))
    );
    let contexts = driver.started_contexts.lock().expect("contexts");
    assert_eq!(contexts[1].len(), 2);
    assert!(
        contexts[1]
            .iter()
            .all(|message| matches!(context_message(message), Message::User(_)))
    );
}

#[tokio::test]
async fn two_consecutive_length_tool_batches_prevent_third_provider_call() {
    let length = || {
        output(assistant(
            StopReason::Length,
            vec![AssistantContent::ToolCall {
                tool_call: call("truncated"),
                wire_item_index: 0,
            }],
            None,
            None,
        ))
    };
    let driver = Arc::new(FixtureDriver::new(vec![length(), length()]));
    let (completion, events) = run_fixture(driver.clone()).await;
    let core = match completion {
        RunCompletion::Completed(core) => core,
        RunCompletion::Failed { failure, .. } => panic!("unexpected failure: {failure}"),
    };
    assert_eq!(driver.started_contexts.lock().expect("contexts").len(), 2);
    assert!(driver.tool_order.lock().expect("tool order").is_empty());
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, AgentEvent::ToolExecutionStart { .. }))
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, AgentEvent::ToolExecutionEnd { .. })),
        "a skipped call has no ordinary execution lifecycle"
    );
    assert_eq!(
        events
            .iter()
            .filter(
                |event| matches!(event, AgentEvent::MessageEnd { message, .. }
                if matches!(message.as_ref(), PublicMessage::ToolResult(result) if result.is_error))
            )
            .count(),
        2,
        "each truncated call closes only as a synthetic result message"
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, AgentEvent::Error { .. }))
    );
    assert!(events.iter().any(|event| {
        matches!(event, AgentEvent::MessageEnd { message, .. }
            if matches!(message.as_ref(), PublicMessage::ToolResult(result)
                if result.content.iter().any(|content| matches!(content,
                    UserContent::Text { text } if text.contains(LENGTH_LOOP_FAILURE)))))
    }));
    let guarded_end = events
        .iter()
        .position(|event| {
            matches!(
                event,
                AgentEvent::MessageEnd { message, .. }
                    if matches!(message.as_ref(), PublicMessage::Assistant(assistant)
                        if assistant.stop_reason == StopReason::Error
                            && assistant.provider_code.as_deref() == Some(LENGTH_LOOP_CODE)
                            && assistant.error_message.as_deref() == Some(LENGTH_LOOP_FAILURE))
            )
        })
        .expect("guarded assistant MessageEnd");
    let guarded_turn = events
        .iter()
        .skip(guarded_end + 1)
        .find(|event| matches!(event, AgentEvent::TurnEnd { .. }))
        .expect("guarded TurnEnd");
    assert!(matches!(
        guarded_turn,
        AgentEvent::TurnEnd { message: Some(message), tool_results }
            if matches!(message.as_ref(), PublicMessage::Assistant(assistant)
                if assistant.stop_reason == StopReason::Error
                    && assistant.provider_code.as_deref() == Some(LENGTH_LOOP_CODE))
                && tool_results.len() == 1
                && tool_results[0].is_error
    ));
    assert_eq!(
        core.runtime_context.len(),
        4,
        "both durable Length result batches remain anchored while the guard Error assistant is excluded"
    );
    assert!(!core.runtime_context.iter().any(|message| matches!(
        context_message(message),
        Message::Assistant(assistant)
            if assistant.provider_code.as_deref() == Some(LENGTH_LOOP_CODE)
    )));
    assert!(matches!(events.last(), Some(AgentEvent::AgentEnd)));
}

#[tokio::test]
async fn tool_calls_execute_strictly_sequentially_and_continue_provider() {
    let driver = Arc::new(FixtureDriver::new(vec![
        output(assistant(
            StopReason::ToolUse,
            vec![
                AssistantContent::ToolCall {
                    tool_call: call("a"),
                    wire_item_index: 0,
                },
                AssistantContent::ToolCall {
                    tool_call: call("b"),
                    wire_item_index: 1,
                },
            ],
            None,
            None,
        )),
        output(assistant(StopReason::Stop, Vec::new(), None, None)),
    ]));
    let (completion, events) =
        run_fixture_with_initial(driver.clone(), live_admitted_user(1)).await;
    assert_completed(completion);
    assert_eq!(
        *driver.tool_order.lock().expect("tool order"),
        vec!["a", "b"]
    );
    assert_eq!(driver.max_active_tools.load(Ordering::SeqCst), 1);
    assert_eq!(driver.started_contexts.lock().expect("contexts").len(), 2);
    let command_times = driver.started_command_times.lock().expect("command times");
    assert_eq!(command_times.len(), 2);
    assert!(command_times[0].is_some());
    assert_eq!(
        command_times[1], None,
        "tool execution is not internal overhead"
    );
    for event in &events {
        let AgentEvent::ToolExecutionEnd {
            tool_call_id,
            result,
            ..
        } = event
        else {
            continue;
        };
        let durable_message = events
            .iter()
            .find_map(|candidate| match candidate {
                AgentEvent::MessageEnd { message, .. }
                    if matches!(message.as_ref(), PublicMessage::ToolResult(tool_result)
                        if &tool_result.tool_call_id == tool_call_id) =>
                {
                    let PublicMessage::ToolResult(tool_result) = message.as_ref() else {
                        unreachable!()
                    };
                    Some(tool_result)
                }
                _ => None,
            })
            .expect("tool result MessageEnd");
        assert_eq!(
            result,
            &serde_json::to_value(durable_message).expect("serialize durable result"),
            "ToolExecutionEnd must carry the exact durable ToolResultMessage payload"
        );
    }
}

#[tokio::test]
async fn reused_tool_call_id_gets_turn_scoped_stable_result_message_ids() {
    let tool_turn = || {
        output(assistant(
            StopReason::ToolUse,
            vec![AssistantContent::ToolCall {
                tool_call: call("reused"),
                wire_item_index: 0,
            }],
            None,
            None,
        ))
    };
    let driver = Arc::new(FixtureDriver::new(vec![
        tool_turn(),
        tool_turn(),
        output(assistant(StopReason::Stop, Vec::new(), None, None)),
    ]));
    let (completion, events) = run_fixture(driver).await;
    assert_completed(completion);

    let starts: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::MessageStart {
                message_id,
                message,
            } if matches!(message.as_ref(), PublicMessage::ToolResult(result)
                if result.tool_call_id == "reused") =>
            {
                Some(message_id.clone())
            }
            _ => None,
        })
        .collect();
    let ends: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::MessageEnd {
                message_id,
                message,
            } if matches!(message.as_ref(), PublicMessage::ToolResult(result)
                if result.tool_call_id == "reused") =>
            {
                Some(message_id.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(starts, ends, "each result must close under its start ID");
    assert_eq!(starts.len(), 2);
    assert_ne!(starts[0], starts[1]);
    let first_pair_id = tool_result_message_id("assistant-0", "reused");
    assert_eq!(starts[0], first_pair_id);
    assert_eq!(starts[1], tool_result_message_id("assistant-1", "reused"));
    assert_eq!(
        first_pair_id,
        tool_result_message_id("assistant-0", "reused"),
        "replaying the same assistant/call pair must reproduce the ID"
    );
    assert!(
        starts
            .iter()
            .all(|message_id| Uuid::parse_str(message_id).is_ok())
    );
}

#[test]
fn synthetic_attempt_message_ids_are_stable_and_scoped_by_durable_identity_and_failure_role() {
    let binding = DurableRunBinding {
        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
        command_seq: 1,
        run_id: "01900000-0000-7000-8000-000000000001".to_owned(),
        turn_id: "01900000-0000-7000-8000-000000000002".to_owned(),
        executor_generation: test_executor_generation(),
    };
    let start = synthetic_attempt_message_id(&binding, 0, SyntheticAttemptFailure::Start)
        .expect("synthetic start identity");
    assert_eq!(
        start,
        synthetic_attempt_message_id(&binding, 0, SyntheticAttemptFailure::Start)
            .expect("stable synthetic start identity")
    );
    assert_ne!(
        start,
        synthetic_attempt_message_id(&binding, 0, SyntheticAttemptFailure::InvalidMessageId)
            .expect("failure role identity")
    );
    let mut next_turn = binding.clone();
    next_turn.turn_id = "01900000-0000-7000-8000-000000000003".to_owned();
    assert_ne!(
        start,
        synthetic_attempt_message_id(&next_turn, 0, SyntheticAttemptFailure::Start)
            .expect("next turn identity")
    );
}

#[tokio::test]
async fn tool_failure_is_synthetic_result_and_preserves_normal_form() {
    let driver = Arc::new(
        FixtureDriver::new(vec![
            output(assistant(
                StopReason::ToolUse,
                vec![AssistantContent::ToolCall {
                    tool_call: call("fails"),
                    wire_item_index: 0,
                }],
                None,
                None,
            )),
            output(assistant(StopReason::Stop, Vec::new(), None, None)),
        ])
        .with_tool_failures(vec![Some("fixture failure")]),
    );
    let (completion, events) = run_fixture(driver).await;
    assert_completed(completion);
    assert!(
        events
            .iter()
            .any(|event| matches!(event, AgentEvent::ToolExecutionEnd { is_error: true, .. }))
    );
    assert!(matches!(events.last(), Some(AgentEvent::AgentEnd)));
}

#[tokio::test]
async fn mixed_rejections_precede_valid_lifecycle_and_only_valid_results_enter_turn_results() {
    let rejected_first = rejected("invalid");
    let rejected_second = rejected("invalid-2");
    let valid = call("valid");
    let driver = Arc::new(FixtureDriver::new(vec![
        output(assistant(
            StopReason::ToolUse,
            vec![
                AssistantContent::ToolCall {
                    tool_call: valid.clone(),
                    wire_item_index: 0,
                },
                AssistantContent::RejectedToolCall {
                    rejected: rejected_first.clone(),
                    wire_item_index: 1,
                },
                AssistantContent::RejectedToolCall {
                    rejected: rejected_second.clone(),
                    wire_item_index: 2,
                },
            ],
            None,
            None,
        )),
        output(assistant(StopReason::Stop, Vec::new(), None, None)),
    ]));
    let (completion, events) = run_fixture(driver.clone()).await;
    assert_completed(completion);
    let valid_start = events
        .iter()
        .position(|event| matches!(event, AgentEvent::ToolExecutionStart { tool_call_id, .. } if tool_call_id == &valid.id))
        .expect("valid execution start");
    for rejected in [&rejected_first, &rejected_second] {
        let rejected_end = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    AgentEvent::MessageEnd { message, .. }
                        if matches!(message.as_ref(), PublicMessage::ToolResult(result)
                            if result.tool_call_id == rejected.id)
                )
            })
            .expect("rejected result end");
        assert!(rejected_end < valid_start);
    }
    let first_turn = events
        .iter()
        .find(|event| matches!(event, AgentEvent::TurnEnd { .. }))
        .expect("first TurnEnd");
    assert!(
        matches!(
            first_turn,
            AgentEvent::TurnEnd { message: Some(message), tool_results }
                if tool_results.len() == 1 && tool_results[0].tool_call_id == valid.id
                    && matches!(message.as_ref(), PublicMessage::Assistant(assistant)
                        if assistant.content.iter().any(|content| matches!(
                            content,
                            PublicAssistantContent::RejectedToolCall { rejected: value, .. }
                                if value == &rejected_first
                        )))
        ),
        "unexpected first turn: {first_turn:#?}"
    );
    let contexts = driver.started_contexts.lock().expect("contexts");
    assert_eq!(contexts[1].len(), 5);
    assert!(matches!(
        context_message(&contexts[1][1]),
        Message::Assistant(assistant)
            if assistant.content.iter().any(|content| matches!(
                content,
                AssistantContent::RejectedToolCall { rejected: value, .. }
                    if value == &rejected_first
            ))
    ));
    assert!(matches!(
        context_message(&contexts[1][2]),
        Message::ToolResult(result)
            if result.tool_call_id == rejected_first.id && result.is_error
    ));
    assert!(matches!(
        context_message(&contexts[1][3]),
        Message::ToolResult(result)
            if result.tool_call_id == rejected_second.id && result.is_error
    ));
}

#[tokio::test]
async fn error_and_immediate_overflow_emit_rejection_pair_without_context_or_turn_results() {
    for overflow in [false, true] {
        let rejected = rejected(if overflow { "overflow" } else { "error" });
        let (error, code) = if overflow {
            ("maximum context length exceeded", "context_length_exceeded")
        } else {
            ("invalid request", "http_400")
        };
        let driver = Arc::new(FixtureDriver::new(vec![output(assistant(
            StopReason::Error,
            vec![AssistantContent::RejectedToolCall {
                rejected: rejected.clone(),
                wire_item_index: 0,
            }],
            Some(error),
            Some(code),
        ))]));
        let (completion, events) = run_fixture(driver).await;
        let core = match completion {
            RunCompletion::Completed(core) => core,
            RunCompletion::Failed { failure, .. } => panic!("unexpected failure: {failure}"),
        };
        assert_eq!(core.runtime_context.len(), 2);
        assert!(matches!(
            context_message(&core.runtime_context[0]),
            Message::User(_)
        ));
        assert!(matches!(
            context_message(&core.runtime_context[1]),
            Message::ToolResult(_)
        ));
        let result_end = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    AgentEvent::MessageEnd { message, .. }
                        if matches!(message.as_ref(), PublicMessage::ToolResult(result)
                            if result.tool_call_id == rejected.id && result.is_error)
                )
            })
            .unwrap_or_else(|| panic!("rejected result MessageEnd missing: {events:#?}"));
        let turn = events
            .iter()
            .skip(result_end + 1)
            .find(|event| matches!(event, AgentEvent::TurnEnd { .. }))
            .expect("error TurnEnd");
        assert!(matches!(
            turn,
            AgentEvent::TurnEnd { message: Some(message), tool_results }
                if tool_results.is_empty()
                    && matches!(message.as_ref(), PublicMessage::Assistant(assistant)
                        if assistant.stop_reason == StopReason::Error)
        ));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, AgentEvent::RetryScheduled { .. }))
        );
    }
}

#[tokio::test]
async fn retryable_error_commits_rejected_result_before_scheduling_next_attempt() {
    let rejected = rejected("retry-rejected");
    let driver = Arc::new(FixtureDriver::new(vec![
        output(assistant(
            StopReason::Error,
            vec![AssistantContent::RejectedToolCall {
                rejected: rejected.clone(),
                wire_item_index: 0,
            }],
            Some("temporary network error"),
            Some("http_500"),
        )),
        output(assistant(StopReason::Stop, Vec::new(), None, None)),
    ]));
    let (completion, events) = run_fixture(driver.clone()).await;
    assert_completed(completion);

    let result_end = events
        .iter()
        .position(|event| {
            matches!(event, AgentEvent::MessageEnd { message, .. }
                if matches!(message.as_ref(), PublicMessage::ToolResult(result)
                    if result.tool_call_id == rejected.id && result.is_error))
        })
        .expect("rejected result MessageEnd");
    let retry = events
        .iter()
        .position(|event| matches!(event, AgentEvent::RetryScheduled { .. }))
        .expect("retry schedule");
    assert!(result_end < retry);

    let contexts = driver.started_contexts.lock().expect("contexts");
    assert_eq!(contexts.len(), 2);
    assert_eq!(contexts[1].len(), 2, "error assistant stays outside L0");
    assert!(matches!(
        context_message(&contexts[1][1]),
        Message::ToolResult(result) if result.tool_call_id == rejected.id && result.is_error
    ));
}

#[tokio::test]
async fn non_authoritative_projection_failure_and_eof_discard_rejected_results() {
    for suffix in [vec![ProviderEvent::Start], Vec::new()] {
        let rejected = rejected("volatile");
        let mut events = vec![
            ProviderEvent::Start,
            ProviderEvent::ToolCallStart { content_index: 0 },
            ProviderEvent::ToolCallRejected {
                content_index: 0,
                rejected: rejected.clone(),
                synthetic_result: rejected_result(&rejected),
            },
        ];
        events.extend(suffix);
        let driver = Arc::new(FixtureDriver::new(vec![
            Script::Events(events),
            output(assistant(StopReason::Stop, Vec::new(), None, None)),
        ]));
        let (completion, events) = run_fixture(driver).await;
        let core = match completion {
            RunCompletion::Completed(core) => core,
            RunCompletion::Failed { failure, .. } => panic!("unexpected failure: {failure}"),
        };
        assert!(
            !core
                .runtime_context
                .iter()
                .any(|message| matches!(context_message(message), Message::ToolResult(_)))
        );
        assert!(!core.runtime_context.iter().any(|message| matches!(
            context_message(message),
            Message::Assistant(assistant)
                if assistant.content.iter().any(|item| matches!(
                    item,
                    AssistantContent::RejectedToolCall { .. }
                ))
        )));
        assert!(
            !events.iter().any(|event| matches!(
                event,
                AgentEvent::MessageStart { message, .. }
                    | AgentEvent::MessageEnd { message, .. }
                    if matches!(message.as_ref(), PublicMessage::ToolResult(_))
            )),
            "without an authoritative terminal rejection snapshot, its result would be orphaned: {events:#?}"
        );
    }
}

#[tokio::test]
async fn provider_start_failure_is_closed_with_synthetic_assistant() {
    let driver = Arc::new(FixtureDriver::new(vec![Script::StartFailure("no route")]));
    let (completion, events) = run_fixture(driver).await;
    assert_completed(completion);
    assert!(matches!(events[4], AgentEvent::MessageStart { .. }));
    assert!(matches!(events[5], AgentEvent::MessageEnd { .. }));
    assert!(matches!(events[6], AgentEvent::TurnEnd { .. }));
    assert!(matches!(events[7], AgentEvent::AgentEnd));
}

#[tokio::test]
async fn provider_start_failure_recovers_received_controls_and_next_run_consumes_oldest() {
    let first_driver = Arc::new(FixtureDriver::new(vec![Script::StartFailure("no route")]));
    let first_worker = SequentialRunWorker::new(first_driver);
    let (control_tx, control_rx) = mpsc::channel(8);
    control_tx
        .send(RunControl::Command(admitted_user(2)))
        .await
        .expect("first received control");
    control_tx
        .send(RunControl::Command(admitted_user(3)))
        .await
        .expect("second received control");
    let (events_tx, events_rx) = mpsc::channel(256);
    let first = complete_with_receipts(
        first_worker.run(bound_core(1), admitted_user(1), control_rx, events_tx),
        events_rx,
    )
    .await;
    let core = recovered_core(first);
    assert_eq!(
        core.runtime_context.len(),
        1,
        "only the active seq 1 was applied before the provider failed"
    );

    let second_driver = Arc::new(FixtureDriver::new(vec![
        output(assistant(StopReason::Stop, Vec::new(), None, None)),
        output(assistant(StopReason::Stop, Vec::new(), None, None)),
        output(assistant(StopReason::Stop, Vec::new(), None, None)),
    ]));
    let second_worker = SequentialRunWorker::new(second_driver.clone());
    let (_control_tx, control_rx) = mpsc::channel(8);
    let (events_tx, events_rx) = mpsc::channel(256);
    let second = complete_with_receipts(
        second_worker.run(core, admitted_user(4), control_rx, events_tx),
        events_rx,
    )
    .await;
    assert_completed(second);
    let contexts = second_driver.started_contexts.lock().expect("contexts");
    assert!(matches!(
        context_message(&contexts[0][1]),
        Message::User(user)
            if matches!(&user.content[0], UserContent::Text { text } if text == "message 2")
    ));
    assert!(matches!(
        context_message(&contexts[1][3]),
        Message::User(user)
            if matches!(&user.content[0], UserContent::Text { text } if text == "message 3")
    ));
    assert!(matches!(
        context_message(&contexts[2][5]),
        Message::User(user)
            if matches!(&user.content[0], UserContent::Text { text } if text == "message 4")
    ));
}

#[tokio::test]
async fn closed_error_recovers_cross_kind_controls_without_reordering_or_preemption() {
    let driver = Arc::new(FixtureDriver::new(vec![output(assistant(
        StopReason::Error,
        Vec::new(),
        Some("invalid request"),
        Some("http_400"),
    ))]));
    let worker = SequentialRunWorker::new(driver);
    let (control_tx, control_rx) = mpsc::channel(8);
    control_tx
        .send(RunControl::Command(admitted_user(2)))
        .await
        .expect("safe-point user");
    control_tx
        .send(RunControl::Command(admitted_abort(3)))
        .await
        .expect("deferred abort");
    control_tx
        .send(RunControl::Command(admitted_user(4)))
        .await
        .expect("later user");
    let (events_tx, mut events_rx) = mpsc::channel(256);
    let completion = tokio::spawn(async move {
        worker
            .run(bound_core(1), admitted_user(1), control_rx, events_tx)
            .await
    });
    let mut message_seq = 1;
    while let Some(mut output) = events_rx.recv().await {
        resolve_message_output(&mut output, &mut message_seq);
        if let Some(barrier) = output.commit_barrier.take() {
            barrier.committed();
        }
    }
    let completion = completion.await.expect("worker join");
    let core = recovered_core(completion);

    let blocked_driver = Arc::new(FixtureDriver::new(vec![output(assistant(
        StopReason::Stop,
        Vec::new(),
        None,
        None,
    ))]));
    let blocked_worker = SequentialRunWorker::new(blocked_driver);
    let (_control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, events_rx) = mpsc::channel(8);
    let blocked = complete_with_receipts(
        blocked_worker.run(core, admitted_user(5), control_rx, events_tx),
        events_rx,
    )
    .await;
    let mut core = recovered_core(blocked);
    assert_eq!(
        pending_sequences(&mut core),
        vec![3, 4, 5],
        "unimplemented Abort remains ahead of later users and is not preempted"
    );
}

#[tokio::test]
async fn deferred_t16_control_blocks_later_user_at_every_safe_point() {
    let driver = Arc::new(FixtureDriver::new(vec![
        output(assistant(
            StopReason::ToolUse,
            vec![AssistantContent::ToolCall {
                tool_call: call("continue"),
                wire_item_index: 0,
            }],
            None,
            None,
        )),
        output(assistant(StopReason::Stop, Vec::new(), None, None)),
    ]));
    let worker = SequentialRunWorker::new(driver);
    let (control_tx, control_rx) = mpsc::channel(8);
    control_tx
        .send(RunControl::Command(admitted_abort(2)))
        .await
        .expect("deferred abort");
    control_tx
        .send(RunControl::Command(admitted_user(3)))
        .await
        .expect("later user");
    let (events_tx, mut events_rx) = mpsc::channel(256);
    let completion = tokio::spawn(async move {
        worker
            .run(bound_core(1), admitted_user(1), control_rx, events_tx)
            .await
    });
    let mut message_seq = 1;
    while let Some(mut output) = events_rx.recv().await {
        resolve_message_output(&mut output, &mut message_seq);
        if let Some(barrier) = output.commit_barrier.take() {
            barrier.committed();
        }
    }
    let completion = completion.await.expect("worker join");
    let mut core = recovered_core(completion);
    assert_eq!(pending_sequences(&mut core), vec![2, 3]);
    assert_eq!(
        core.runtime_context
            .iter()
            .filter(|message| matches!(context_message(message), Message::User(_)))
            .count(),
        1,
        "seq 3 cannot overtake the unimplemented seq 2 Abort"
    );
}

#[tokio::test]
async fn event_failure_race_recovers_every_control_accepted_before_receiver_close() {
    let driver = Arc::new(FixtureDriver::new(vec![Script::StartFailure("unused")]));
    let worker = SequentialRunWorker::new(driver);
    let (control_tx, control_rx) = mpsc::channel(super::super::CONTROL_CHANNEL_CAPACITY);
    for seq in 2..=32 {
        control_tx
            .send(RunControl::Command(admitted_user(seq)))
            .await
            .expect("admission-bounded user control");
    }
    control_tx
        .send(RunControl::Command(admitted_abort(33)))
        .await
        .expect("reserved abort control");
    let (events_tx, events_rx) = mpsc::channel(1);
    drop(events_rx);
    let completion = worker
        .run(bound_core(1), admitted_user(1), control_rx, events_tx)
        .await;
    assert!(matches!(
        &completion,
        RunCompletion::Failed {
            failure: WorkerFailure::EventChannelClosed,
            ..
        }
    ));
    let mut core = recovered_core(completion);
    assert_eq!(
        pending_sequences(&mut core),
        (1..=33).collect::<Vec<_>>(),
        "the uncommitted initial command and all 32 accepted controls are recovered exactly once"
    );
}

#[tokio::test]
async fn initial_command_is_recovered_before_each_fallible_injection_boundary() {
    for boundary in 0..=3 {
        let driver = Arc::new(FixtureDriver::new(vec![Script::StartFailure("unused")]));
        let (_control_tx, control_rx) = mpsc::channel(1);
        let (events_tx, mut events_rx) = mpsc::channel(8);
        let mut runner = Runner::new(bound_core(1), driver, control_rx, events_tx);
        runner
            .claim_ordered_initial(admitted_user(1))
            .expect("claim initial");
        if boundary > 0 {
            runner
                .emit(AgentEvent::AgentStart)
                .await
                .expect("agent start");
            events_rx.recv().await.expect("agent start event");
        }
        if boundary > 1 {
            runner
                .emit(AgentEvent::TurnStart)
                .await
                .expect("turn start");
            events_rx.recv().await.expect("turn start event");
        }

        let failure = match boundary {
            0 => {
                drop(events_rx);
                runner
                    .emit(AgentEvent::AgentStart)
                    .await
                    .expect_err("closed before AgentStart")
            }
            1 => {
                drop(events_rx);
                runner
                    .emit(AgentEvent::TurnStart)
                    .await
                    .expect_err("closed before TurnStart")
            }
            2 => {
                drop(events_rx);
                runner
                    .inject_in_flight()
                    .await
                    .expect_err("closed before user MessageStart")
            }
            3 => {
                let message = PublicMessage::User(UserMessage {
                    content: vec![UserContent::Text {
                        text: "message 1".to_owned(),
                    }],
                    timestamp: timestamp(),
                });
                let message_id = user_message_id(&user(1).command_id);
                runner
                    .emit(AgentEvent::MessageStart {
                        message_id: message_id.clone(),
                        message: Box::new(message.clone()),
                    })
                    .await
                    .expect("user MessageStart");
                events_rx.recv().await.expect("user MessageStart event");
                drop(events_rx);
                runner
                    .emit(AgentEvent::MessageEnd {
                        message_id,
                        message: Box::new(message),
                    })
                    .await
                    .expect_err("closed before user MessageEnd")
            }
            _ => unreachable!(),
        };
        assert_eq!(failure, WorkerFailure::EventChannelClosed);
        runner.recover_received_controls().expect("recover initial");
        assert_eq!(
            pending_sequences(&mut runner.core),
            vec![1],
            "initial command must be recovered at event boundary {boundary}"
        );
    }
}

#[tokio::test]
async fn claimed_followups_survive_turn_steer_and_injection_event_failures() {
    let cases = ["turn_start", "steered", "message_start"];
    for case in cases {
        let driver = Arc::new(FixtureDriver::new(Vec::new()));
        let mut core = bound_core(1);
        core.queue_followup(admitted_user(2)).expect("followup");
        let (_control_tx, control_rx) = mpsc::channel(1);
        let (events_tx, events_rx) = mpsc::channel(1);
        drop(events_rx);
        let mut runner = Runner::new(core, driver, control_rx, events_tx);

        let failure = match case {
            "turn_start" => runner.advance_followup().await.expect_err("closed events"),
            "steered" => {
                assert!(runner.claim_pending_user().expect("claim"));
                runner
                    .emit(AgentEvent::Steered {
                        mode: SteerMode::Soft,
                    })
                    .await
                    .expect_err("closed events")
            }
            "message_start" => {
                assert!(runner.claim_pending_user().expect("claim"));
                runner.inject_in_flight().await.expect_err("closed events")
            }
            _ => unreachable!(),
        };
        assert_eq!(failure, WorkerFailure::EventChannelClosed);
        runner.recover_received_controls().expect("recover control");
        assert_eq!(
            pending_sequences(&mut runner.core),
            vec![2],
            "claimed followup must survive {case} failure"
        );
    }
}

#[tokio::test]
async fn retry_wait_channel_close_and_cancelled_hook_never_start_another_attempt() {
    for cancelled_hook in [false, true] {
        let mut fixture = FixtureDriver::new(vec![
            output(assistant(
                StopReason::Error,
                Vec::new(),
                Some("network error"),
                Some("network_error"),
            )),
            output(assistant(StopReason::Stop, Vec::new(), None, None)),
        ]);
        if cancelled_hook {
            fixture = fixture.cancelled_retry();
        }
        let driver = Arc::new(fixture);
        let worker = SequentialRunWorker::new(driver.clone());
        let (control_tx, control_rx) = mpsc::channel(1);
        let open_control = if cancelled_hook {
            Some(control_tx)
        } else {
            drop(control_tx);
            None
        };
        let (events_tx, events_rx) = mpsc::channel(64);
        let completion = complete_with_receipts(
            worker.run(bound_core(1), admitted_user(1), control_rx, events_tx),
            events_rx,
        )
        .await;
        assert!(matches!(
            completion,
            RunCompletion::Failed {
                failure: WorkerFailure::Cancelled,
                ..
            }
        ));
        assert_eq!(
            driver.started_contexts.lock().expect("contexts").len(),
            1,
            "neither closure nor a false wait outcome may bypass backoff into another attempt"
        );
        if cancelled_hook {
            assert_eq!(driver.retry_waits.load(Ordering::SeqCst), 1);
        }
        drop(open_control);
    }
}

#[tokio::test]
async fn retry_wait_uses_all_three_backoff_delays_before_the_fourth_attempt() {
    let retryable = || {
        output(assistant(
            StopReason::Error,
            Vec::new(),
            Some("network error"),
            Some("network_error"),
        ))
    };
    let driver = Arc::new(FixtureDriver::new(vec![
        retryable(),
        retryable(),
        retryable(),
        output(assistant(StopReason::Stop, Vec::new(), None, None)),
    ]));
    let (completion, _) = run_fixture(driver.clone()).await;
    assert_completed(completion);
    assert_eq!(
        *driver.retry_delays.lock().expect("retry delays"),
        vec![
            Duration::from_secs(2),
            Duration::from_secs(4),
            Duration::from_secs(8),
        ]
    );
    assert_eq!(driver.started_contexts.lock().expect("contexts").len(), 4);
}

#[tokio::test]
async fn immediate_overflow_recovers_twice_then_closes_without_appending_attempts() {
    let overflow = || {
        output(assistant(
            StopReason::Error,
            vec![
                AssistantContent::Text {
                    text: "display-safe prefix".to_owned(),
                    wire_item_index: 0,
                },
                AssistantContent::ToolCall {
                    tool_call: call("must-not-start"),
                    wire_item_index: 1,
                },
            ],
            Some("maximum context length exceeded"),
            Some("context_length_exceeded"),
        ))
    };
    let recovered_one = recovered_context("one");
    let recovered_two = recovered_context("two");
    let driver = Arc::new(
        FixtureDriver::new(vec![overflow(), overflow(), overflow()])
            .with_context_window(100)
            .with_overflow_contexts(vec![recovered_one.clone(), recovered_two.clone()]),
    );
    let (completion, events) = run_fixture(driver.clone()).await;
    let core = match completion {
        RunCompletion::Completed(core) => core,
        RunCompletion::Failed { failure, .. } => panic!("unexpected failure: {failure}"),
    };
    let recoveries = driver
        .overflow_recoveries
        .lock()
        .expect("overflow recoveries");
    assert_eq!(
        *recoveries,
        vec![
            OverflowRecoveryRequest {
                source: OverflowSource::ProviderCode,
                ordinal: 1,
            },
            OverflowRecoveryRequest {
                source: OverflowSource::ProviderCode,
                ordinal: 2,
            },
        ],
        "unexpected events: {events:#?}"
    );
    assert_eq!(
        *driver
            .overflow_core_epochs
            .lock()
            .expect("overflow core epochs"),
        vec![0, 0],
        "planning receives immutable core state and cannot advance it"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::RetryScheduled { delay_ms: 0, .. }))
            .count(),
        2
    );
    assert_eq!(driver.started_contexts.lock().expect("contexts").len(), 3);
    assert!(driver.tool_order.lock().expect("tool order").is_empty());
    assert!(!events.iter().any(|event| matches!(
        event,
        AgentEvent::ToolExecutionStart { .. } | AgentEvent::ToolExecutionEnd { .. }
    )));
    assert!(events.iter().filter_map(|event| match event {
        AgentEvent::MessageEnd { message, .. }
            if matches!(message.as_ref(), PublicMessage::Assistant(_)) => Some(message.as_ref()),
        _ => None,
    }).all(|message| matches!(message, PublicMessage::Assistant(assistant)
        if assistant.stop_reason == StopReason::Error
            && assistant.content.iter().any(|content| matches!(content,
                PublicAssistantContent::Text { text, .. } if text == "display-safe prefix"))
            && !assistant.content.iter().any(|content| matches!(content,
                PublicAssistantContent::ToolCall { .. }))
    )));
    assert!(matches!(
        events.iter().find(|event| matches!(event, AgentEvent::TurnEnd { .. })),
        Some(AgentEvent::TurnEnd { message: Some(message), tool_results })
            if tool_results.is_empty()
                && matches!(message.as_ref(), PublicMessage::Assistant(assistant)
                    if assistant.stop_reason == StopReason::Error
                        && !assistant.content.iter().any(|content| matches!(content,
                            PublicAssistantContent::ToolCall { .. })))
    ));
    let contexts = driver.started_contexts.lock().expect("contexts");
    assert_eq!(
        core.runtime_context,
        recovered_context_from_active(recovered_two.clone(), &contexts[0])
    );
    assert_eq!(
        contexts[1],
        recovered_context_from_active(recovered_one, &contexts[0])
    );
    assert_eq!(
        contexts[2],
        recovered_context_from_active(recovered_two, &contexts[1])
    );
    for retry in events.iter().enumerate().filter_map(|(index, event)| {
        matches!(event, AgentEvent::RetryScheduled { delay_ms: 0, .. }).then_some(index)
    }) {
        assert!(matches!(
            events.get(retry + 1),
            Some(AgentEvent::MessageStart { .. })
        ));
    }
    assert!(matches!(events.last(), Some(AgentEvent::AgentEnd)));
}

#[tokio::test]
async fn failed_or_noop_overflow_recovery_closes_normally_without_scheduling_retry() {
    for replacement in [None, Some(vec![runtime_user(1)])] {
        let mut fixture = FixtureDriver::new(vec![
            output(assistant(
                StopReason::Error,
                vec![AssistantContent::ToolCall {
                    tool_call: call("pattern-overflow-must-not-start"),
                    wire_item_index: 0,
                }],
                Some("maximum context length exceeded"),
                None,
            )),
            output(assistant(StopReason::Stop, Vec::new(), None, None)),
        ])
        .with_context_window(100);
        if let Some(replacement) = replacement {
            fixture = fixture.with_overflow_contexts(vec![replacement]);
        }
        let driver = Arc::new(fixture);
        let (completion, events) = run_fixture(driver.clone()).await;
        assert_completed(completion);
        assert_eq!(driver.started_contexts.lock().expect("contexts").len(), 1);
        assert!(driver.tool_order.lock().expect("tool order").is_empty());
        assert!(!events.iter().any(|event| matches!(
            event,
            AgentEvent::ToolExecutionStart { .. } | AgentEvent::ToolExecutionEnd { .. }
        )));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, AgentEvent::RetryScheduled { .. }))
        );
        let error_end = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    AgentEvent::MessageEnd { message, .. }
                        if matches!(message.as_ref(), PublicMessage::Assistant(assistant)
                            if assistant.stop_reason == StopReason::Error)
                )
            })
            .expect("overflow error MessageEnd");
        assert!(matches!(
            events.get(error_end + 1),
            Some(AgentEvent::TurnEnd { message: Some(message), tool_results })
                if tool_results.is_empty()
                    && matches!(message.as_ref(), PublicMessage::Assistant(assistant)
                        if !assistant.content.iter().any(|content| matches!(content,
                            PublicAssistantContent::ToolCall { .. })))
        ));
        assert!(matches!(
            events.get(error_end + 2),
            Some(AgentEvent::AgentEnd)
        ));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, AgentEvent::TurnEnd { .. }))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn length_usage_overflow_recovers_before_any_tool_or_context_append() {
    let length = assistant_with_usage(
        StopReason::Length,
        vec![AssistantContent::ToolCall {
            tool_call: call("incomplete"),
            wire_item_index: 0,
        }],
        None,
        None,
        99,
        0,
    );
    let recovered = recovered_context("length");
    let driver = Arc::new(
        FixtureDriver::new(vec![
            output(length),
            output(assistant(StopReason::Stop, Vec::new(), None, None)),
        ])
        .with_context_window(100)
        .with_overflow_contexts(vec![recovered.clone()]),
    );
    let (completion, events) = run_fixture(driver.clone()).await;
    assert_completed(completion);
    assert_eq!(
        *driver
            .overflow_recoveries
            .lock()
            .expect("overflow recoveries"),
        vec![OverflowRecoveryRequest {
            source: OverflowSource::LengthUsage,
            ordinal: 1,
        }]
    );
    assert!(driver.tool_order.lock().expect("tools").is_empty());
    assert!(!events.iter().any(|event| matches!(
        event,
        AgentEvent::ToolExecutionStart { .. } | AgentEvent::ToolExecutionEnd { .. }
    )));
    assert!(!events.iter().any(|event| matches!(
        event,
        AgentEvent::MessageEnd { message, .. }
            if matches!(message.as_ref(), PublicMessage::ToolResult(_))
    )));
    let contexts = driver.started_contexts.lock().expect("contexts");
    assert_eq!(contexts.len(), 2);
    assert_eq!(
        contexts[1],
        recovered_context_from_active(recovered, &contexts[0]),
        "next attempt uses recovered context"
    );

    let overflow_end = events
        .iter()
        .position(|event| {
            matches!(
                event,
                AgentEvent::MessageEnd { message, .. }
                    if matches!(message.as_ref(), PublicMessage::Assistant(assistant)
                        if assistant.stop_reason == StopReason::Error
                            && assistant.provider_code.as_deref() == Some(LENGTH_OVERFLOW_CODE)
                            && assistant.error_message.as_deref() == Some(LENGTH_OVERFLOW_ERROR)
                            && assistant.usage.input == 99
                            && assistant.usage.output == 0
                            && !assistant.content.iter().any(|content| matches!(
                                content,
                                PublicAssistantContent::ToolCall { .. }
                            )))
            )
        })
        .expect("normalized LengthUsage error MessageEnd");
    assert!(matches!(
        events.get(overflow_end + 1),
        Some(AgentEvent::RetryScheduled { delay_ms: 0, .. })
    ));
}

#[tokio::test]
async fn length_usage_recovery_resets_the_consecutive_bulk_failure_guard() {
    let ordinary_length = || {
        output(assistant(
            StopReason::Length,
            vec![AssistantContent::ToolCall {
                tool_call: call("ordinary"),
                wire_item_index: 0,
            }],
            None,
            None,
        ))
    };
    let overflow_length = output(assistant_with_usage(
        StopReason::Length,
        vec![AssistantContent::ToolCall {
            tool_call: call("overflow"),
            wire_item_index: 0,
        }],
        None,
        None,
        99,
        0,
    ));
    let driver = Arc::new(
        FixtureDriver::new(vec![
            ordinary_length(),
            overflow_length,
            ordinary_length(),
            output(assistant(StopReason::Stop, Vec::new(), None, None)),
        ])
        .with_context_window(100)
        .with_overflow_contexts(vec![recovered_context("guard reset")]),
    );
    let (completion, events) = run_fixture(driver.clone()).await;
    assert_completed(completion);
    assert_eq!(driver.started_contexts.lock().expect("contexts").len(), 4);
    assert!(!events.iter().any(|event| matches!(
        event,
        AgentEvent::MessageEnd { message, .. }
            if matches!(message.as_ref(), PublicMessage::ToolResult(result)
                if result.content.iter().any(|content| matches!(content,
                    UserContent::Text { text } if text.contains(LENGTH_LOOP_FAILURE))))
    )));
}

#[tokio::test]
async fn successful_stop_overflow_returns_a_typed_deferred_apply_marker() {
    let stop = assistant_with_usage(StopReason::Stop, Vec::new(), None, None, 101, 1);
    let driver = Arc::new(FixtureDriver::new(vec![output(stop)]).with_context_window(100));
    let (completion, _) = run_fixture(driver).await;
    let core = match completion {
        RunCompletion::Completed(core) => core,
        RunCompletion::Failed { failure, .. } => panic!("unexpected failure: {failure}"),
    };
    assert_eq!(
        core.pending_overflow_apply(),
        Some(OverflowSource::StopUsage)
    );
    assert_eq!(core.runtime_context.len(), 2, "successful Stop is retained");
}

struct UpdateDriver {
    notify: Arc<Notify>,
}

#[async_trait]
impl RunDriver for UpdateDriver {
    fn validate_executor_generation(&self, _generation: ProcessGeneration) -> Result<()> {
        Ok(())
    }

    async fn start_provider_for_command(
        &self,
        _attempt: usize,
        _context: &[ContextMessage],
        _command_received_at: Option<std::time::Instant>,
        _cancel: CancellationToken,
    ) -> Result<ProviderAttempt> {
        Err(anyhow!("UpdateDriver has no provider"))
    }

    async fn execute_tool_observed(
        &self,
        _flow_id: &str,
        call: &ToolCall,
        _cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<ToolResultMessage, ToolError> {
        self.notify.notified().await;
        on_update(json!({"phase":"half"}));
        Ok(ToolResultMessage {
            tool_call_id: call.id.clone(),
            tool_name: call.name.clone(),
            content: vec![UserContent::Text {
                text: "done".to_owned(),
            }],
            details: json!({"ok":true}),
            is_error: false,
            timestamp: timestamp(),
        })
    }

    fn synthetic_error(&self, message: &str) -> PublicMessage {
        public_message(&assistant(
            StopReason::Error,
            Vec::new(),
            Some(message),
            None,
        ))
    }

    async fn plan_overflow_recovery(
        &self,
        _core: &RunCore,
        _request: OverflowRecoveryRequest,
        _active_context: &[ContextMessage],
    ) -> Result<OverflowRecoveryOutcome> {
        Err(anyhow!("UpdateDriver has no overflow recovery"))
    }
}

#[tokio::test]
async fn progress_event_channel_close_propagates_worker_failure_without_synthetic_result() {
    let (events_tx, events_rx) = mpsc::channel(8);
    let (_controls_tx, controls_rx) = mpsc::channel(1);
    let notify = Arc::new(Notify::new());
    let core = bound_core(1);
    let driver: Arc<dyn RunDriver> = Arc::new(UpdateDriver {
        notify: notify.clone(),
    });
    let mut runner = Runner::new(core, driver, controls_rx, events_tx);
    let call = call("call-1");
    drop(events_rx);
    let result =
        tokio::spawn(async move { runner.execute_tool_with_updates("assistant-1", &call).await });
    notify.notify_one();
    let result = result
        .await
        .expect("runner task join")
        .expect_err("progress emission must fail");
    assert!(
        matches!(
            result,
            ExecuteToolError::Worker(WorkerFailure::EventChannelClosed)
        ),
        "progress channel loss must remain typed as a worker failure, not a tool error: {result:?}"
    );
}

struct ReleaseDriver {
    dropped: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl RunDriver for ReleaseDriver {
    fn validate_executor_generation(&self, _generation: ProcessGeneration) -> Result<()> {
        Ok(())
    }

    async fn start_provider_for_command(
        &self,
        _attempt: usize,
        _context: &[ContextMessage],
        _command_received_at: Option<std::time::Instant>,
        _cancel: CancellationToken,
    ) -> Result<ProviderAttempt> {
        Err(anyhow!("ReleaseDriver has no provider"))
    }

    async fn execute_tool_observed(
        &self,
        _flow_id: &str,
        call: &ToolCall,
        _cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<ToolResultMessage, ToolError> {
        drop(on_update);
        self.dropped.notify_one();
        self.release.notified().await;
        Ok(ToolResultMessage {
            tool_call_id: call.id.clone(),
            tool_name: call.name.clone(),
            content: vec![UserContent::Text {
                text: "released".to_owned(),
            }],
            details: json!({"ok": true}),
            is_error: false,
            timestamp: timestamp(),
        })
    }

    fn synthetic_error(&self, message: &str) -> PublicMessage {
        public_message(&assistant(
            StopReason::Error,
            Vec::new(),
            Some(message),
            None,
        ))
    }

    async fn plan_overflow_recovery(
        &self,
        _core: &RunCore,
        _request: OverflowRecoveryRequest,
        _active_context: &[ContextMessage],
    ) -> Result<OverflowRecoveryOutcome> {
        Err(anyhow!("ReleaseDriver has no overflow recovery"))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn update_channel_close_yields_to_tool_future_without_spinning() {
    let (events_tx, _events_rx) = mpsc::channel(8);
    let (_controls_tx, controls_rx) = mpsc::channel(1);
    let dropped = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let driver = Arc::new(ReleaseDriver {
        dropped: dropped.clone(),
        release: release.clone(),
    });
    let core = bound_core(1);
    let mut runner = Runner::new(core, driver, controls_rx, events_tx);
    let tool_call = call("release-call");
    let expected_id = tool_call.id.clone();

    let handle = tokio::spawn(async move {
        runner
            .execute_tool_with_updates("assistant-1", &tool_call)
            .await
    });

    // Wait until the driver has dropped on_update, then release the pending
    // tool. On the buggy implementation this notification is never received
    // because execute_tool_with_update would spin on the closed updates_rx.
    dropped.notified().await;
    release.notify_one();

    let result = handle
        .await
        .expect("runner task join")
        .expect("tool execution should succeed");
    assert_eq!(result.tool_call_id, expected_id);
    assert_eq!(result.tool_name, "tool-release-call");
}

#[tokio::test]
async fn retryable_error_breaks_the_consecutive_length_guard() {
    let length = || {
        output(assistant(
            StopReason::Length,
            vec![AssistantContent::ToolCall {
                tool_call: call("truncated"),
                wire_item_index: 0,
            }],
            None,
            None,
        ))
    };
    let driver = Arc::new(FixtureDriver::new(vec![
        length(),
        output(assistant(
            StopReason::Error,
            Vec::new(),
            Some("network error"),
            Some("network_error"),
        )),
        length(),
        output(assistant(StopReason::Stop, Vec::new(), None, None)),
    ]));
    let (completion, events) = run_fixture(driver.clone()).await;
    assert_completed(completion);
    assert_eq!(driver.started_contexts.lock().expect("contexts").len(), 4);
    assert!(!events.iter().any(|event| matches!(
        event,
        AgentEvent::MessageEnd { message, .. }
            if matches!(message.as_ref(), PublicMessage::ToolResult(result)
                if result.content.iter().any(|content| matches!(content,
                    UserContent::Text { text } if text.contains(LENGTH_LOOP_FAILURE))))
    )));
}
