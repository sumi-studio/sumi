use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use anyhow::{Result, anyhow};
use chrono::{TimeZone, Utc};
use serde_json::{Value, json};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{
    agent::{AttemptCancellation, DurableRunBinding, PublicStreamEvent},
    approval::{
        ApprovalBroker,
        action::{SandboxSummary, SecretAwareActionProjector, SecretDigestKey},
        policy::Policy,
        prompt::{ReviewerPrompt, ReviewerRole, TrustedEnvironment},
        reviewer::{
            Reviewer, ReviewerMode, ReviewerModelSpec, ReviewerTransport, ReviewerTransportError,
            ReviewerTrustSet,
        },
    },
    gateway::{ApprovalDecision, CommandEnvelope, CommandId},
    provider::types::{
        ApiProtocol, AssistantContent, AssistantMessage, ContextMessage, ProviderContextFragment,
        ProviderContextPayload, ProviderOrigin, ProviderOutput, PublicAssistantMessage,
        RejectedToolCall, ToolArgumentError, Usage, ValidatedToolArguments,
    },
    runtime::contracts::ProcessGeneration,
    store::Redactor,
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

fn admitted_approval(seq: u64, request_id: &str) -> AdmittedCommand {
    AdmittedCommand::new(
        CommandEnvelope {
            seq,
            command_id: CommandId::parse(&format!("00000000-0000-4000-8000-{seq:012}"))
                .expect("command id"),
            command: Command::ApprovalDecision {
                request_id: request_id.to_owned(),
                decision: ApprovalDecision::ApproveOnce,
            },
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
    core.attempt_cancellation = Some(Arc::new(AttemptCancellation::default()));
    core
}

struct BlockingReviewer {
    started: Arc<Notify>,
}

#[async_trait]
impl ReviewerTransport for BlockingReviewer {
    async fn complete(
        &self,
        _prompt: &ReviewerPrompt,
        cancel: CancellationToken,
    ) -> Result<String, ReviewerTransportError> {
        self.started.notify_one();
        cancel.cancelled().await;
        Err(ReviewerTransportError::Fatal(
            "cancelled by control".to_owned(),
        ))
    }
}

#[tokio::test]
async fn abort_is_processed_while_reviewer_start_request_is_awaited() {
    let started = Arc::new(Notify::new());
    let projector = SecretAwareActionProjector::new(Redactor::v1(), SecretDigestKey::fixture());
    let reviewer_model = ReviewerModelSpec::new(
        "audit",
        "fixture",
        "https://reviewer.invalid",
        "test",
        "trusted",
        "test-policy",
    );
    let reviewer = Arc::new(Reviewer::new(
        reviewer_model.clone(),
        ReviewerTrustSet::new(reviewer_model, vec![]),
        Arc::new(BlockingReviewer {
            started: started.clone(),
        }),
        Arc::new(SecretAwareActionProjector::new(
            Redactor::v1(),
            SecretDigestKey::fixture(),
        )),
    ));
    let broker = Arc::new(ApprovalBroker::new(
        Policy::new("/workspace"),
        projector,
        Some(reviewer),
        ReviewerMode::AutoReview,
        false,
        TrustedEnvironment {
            workspace_root: "/workspace".to_owned(),
            sandbox: SandboxSummary::workspace(),
            denied_paths: Vec::new(),
            denied_network_domains: Vec::new(),
            repo_visibility: None,
            git_status: None,
        },
    ));
    let driver = Arc::new(FixtureDriver::new(Vec::new()));
    let mut core = bound_core(1);
    core.set_approval(broker);
    let (control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, _events_rx) = mpsc::channel(1);
    let mut runner = Runner::new(core, driver, control_rx, events_tx);
    let call = ToolCall {
        id: "call-review-wait".to_owned(),
        name: "bash".to_owned(),
        arguments: serde_json::from_value(json!({"command": "git status"}))
            .expect("validated arguments"),
    };
    let task = tokio::spawn(async move { runner.evaluate_call(&call, &[], "0").await });
    started.notified().await;
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let (committed_tx, committed_rx) = oneshot::channel();
    control_tx
        .send(RunControl::Abort {
            command: admitted_abort(2),
            accepted: accepted_tx,
            committed: committed_rx,
        })
        .await
        .expect("send abort");
    assert!(accepted_rx.await.expect("abort acceptance"));
    committed_tx.send(()).expect("authorize durable abort");
    let outcome = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("abort must not wait for reviewer")
        .expect("runner task")
        .expect("evaluate call");
    assert!(matches!(outcome, CallDisposition::Denied { .. }));
}

#[tokio::test]
async fn committed_context_mutation_advances_reviewer_cache_version_within_run() {
    let driver = Arc::new(FixtureDriver::new(Vec::new()));
    let core = bound_core(1);
    let (_control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, _events_rx) = mpsc::channel(1);
    let mut runner = Runner::new(core, driver, control_rx, events_tx);
    assert_eq!(runner.core.mutation_epoch(), 0);
    let message = runtime_user(2);
    runner
        .retain_committed(
            MessageCommitReceipt {
                message_id: "persisted-user-2".to_owned(),
                message_seq: 22,
                new_turn_id: None,
            },
            &message,
        )
        .expect("retain committed steer");
    assert_eq!(
        runner.core.mutation_epoch(),
        1,
        "a same-run user/context mutation must invalidate cached reviewer allows"
    );
}

#[tokio::test]
async fn resolved_matching_approval_is_consumed_without_blocking_followup_queue() {
    let projector = SecretAwareActionProjector::new(Redactor::v1(), SecretDigestKey::fixture());
    let broker = Arc::new(ApprovalBroker::headless(
        Policy::new("/workspace"),
        projector,
    ));
    let driver = Arc::new(FixtureDriver::new(Vec::new()));
    let mut core = bound_core(1);
    core.set_approval(broker);
    let (control_tx, control_rx) = mpsc::channel(2);
    let (events_tx, _events_rx) = mpsc::channel(1);
    let mut runner = Runner::new(core, driver, control_rx, events_tx);
    let (_waiter_tx, mut waiter_rx) = oneshot::channel();
    let send_controls = async {
        control_tx
            .send(RunControl::Command(admitted_approval(2, "unrelated")))
            .await
            .expect("send unrelated decision");
        control_tx
            .send(RunControl::Command(admitted_approval(
                3,
                "already-terminal",
            )))
            .await
            .expect("send matching terminal decision");
    };
    let (outcome, ()) = tokio::join!(
        runner.wait_for_approval("already-terminal".to_owned(), &mut waiter_rx),
        send_controls
    );
    assert!(matches!(
        outcome.expect("wait outcome"),
        ApprovalWaitOutcome::Cancelled
    ));
    assert!(
        pending_sequences(&mut runner.core).is_empty(),
        "unmatched approval decisions must not be queued as follow-up controls"
    );
}

async fn run_fixture(driver: Arc<FixtureDriver>) -> (RunCompletion, Vec<AgentEvent>) {
    run_fixture_with(driver, bound_core(1), admitted_user(1)).await
}

async fn run_fixture_with(
    driver: Arc<FixtureDriver>,
    core: RunCore,
    initial: AdmittedCommand,
) -> (RunCompletion, Vec<AgentEvent>) {
    let worker = SequentialRunWorker::new(driver);
    let (_control_tx, control_rx) = mpsc::channel(8);
    let (events_tx, mut events_rx) = mpsc::channel(256);
    let completion =
        tokio::spawn(async move { worker.run(core, initial, control_rx, events_tx).await });
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
            new_turn_id: None,
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
        run_fixture_with(driver.clone(), bound_core(1), live_admitted_user(1)).await;
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
async fn stale_retry_steer_acceptance_releases_exact_claim_without_loss_or_duplicate() {
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

    let (accepted_tx, accepted_rx) = oneshot::channel();
    drop(accepted_rx);
    let (_committed_tx, committed_rx) = oneshot::channel();
    control_tx
        .send(RunControl::RetrySteer {
            command: admitted_user(2),
            accepted: accepted_tx,
            committed: committed_rx,
        })
        .await
        .expect("stale retry steer");
    control_tx
        .send(RunControl::Command(admitted_user(2)))
        .await
        .expect("normal durable deferral retry");

    assert_completed(completion.await.expect("worker join"));
    let events = event_collector.await.expect("event collector");
    let user_message_id = user_message_id(&admitted_user(2).envelope().command_id);
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                matches!(event, AgentEvent::MessageEnd { message_id, .. }
                    if message_id == &user_message_id)
            })
            .count(),
        1
    );
    let contexts = driver.started_contexts.lock().expect("contexts");
    assert_eq!(contexts.len(), 2);
    assert_eq!(
        contexts[1]
            .iter()
            .filter(|message| matches!(context_message(message), Message::User(_)))
            .count(),
        2
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
        run_fixture_with(driver.clone(), bound_core(1), live_admitted_user(1)).await;
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
        vec![1, 2],
        "each committed context boundary advances the reviewer cache version"
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

#[tokio::test]
async fn abort_requested_before_provider_start_skips_start_and_closes_normally() {
    let driver = Arc::new(FixtureDriver::new(vec![output(assistant(
        StopReason::Stop,
        Vec::new(),
        None,
        None,
    ))]));
    let (_control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(8);
    let mut runner = super::Runner::new(bound_core(1), driver.clone(), control_rx, events_tx);
    runner
        .claim_ordered_initial(admitted_user(1))
        .expect("claim initial");
    runner.abort_requested = true;

    let outcome = runner.provider_attempt().await.expect("provider attempt");
    assert!(
        matches!(outcome, super::AttemptOutcome::ClosedError { .. }),
        "abort before provider start must close with a synthetic error message"
    );
    assert_eq!(
        driver.started_contexts.lock().expect("contexts").len(),
        0,
        "provider must not be started once abort was already requested"
    );
    while events_rx.try_recv().is_ok() {}
}

#[tokio::test]
async fn accept_steer_control_releases_claim_when_durable_authorization_drops() {
    let driver = Arc::new(FixtureDriver::new(vec![]));
    let (_control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, _events_rx) = mpsc::channel(8);
    let mut runner = super::Runner::new(bound_core(1), driver, control_rx, events_tx);
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let (committed_tx, committed_rx) = oneshot::channel::<()>();
    drop(committed_tx);

    let command = admitted_user(2);
    let authorized = runner
        .accept_steer_control(command, accepted_tx, committed_rx)
        .await
        .expect("dropped authorization is a no-op");
    assert!(!authorized);
    assert!(runner.in_flight_controls.is_empty());
    assert!(accepted_rx.await.expect("accepted must still be sent"));
}

// The following drivers and tests cover the timeout/dropped-acceptance path
// described in the RunControl handshake audit. They ensure HardSteer and Abort
// are not applied when the Session `accepted` receiver has already been dropped.

struct CancellingProbeDriver {
    cancelled: Arc<AtomicBool>,
    senders: Mutex<Vec<mpsc::Sender<ProviderEvent>>>,
    started_contexts: Mutex<Vec<Vec<ContextMessage>>>,
    emit_partial: bool,
}

impl CancellingProbeDriver {
    fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            senders: Mutex::new(Vec::new()),
            started_contexts: Mutex::new(Vec::new()),
            emit_partial: false,
        }
    }

    fn with_partial(mut self) -> Self {
        self.emit_partial = true;
        self
    }
}

#[async_trait]
impl RunDriver for CancellingProbeDriver {
    fn validate_executor_generation(&self, _generation: ProcessGeneration) -> Result<()> {
        Ok(())
    }

    async fn start_provider_for_command(
        &self,
        attempt: usize,
        context: &[ContextMessage],
        _command_received_at: Option<std::time::Instant>,
        cancel: CancellationToken,
    ) -> Result<ProviderAttempt> {
        self.started_contexts
            .lock()
            .expect("contexts")
            .push(context.to_vec());
        let cancelled = self.cancelled.clone();
        let cancel_watch = cancel.clone();
        tokio::spawn(async move {
            cancel_watch.cancelled().await;
            cancelled.store(true, Ordering::SeqCst);
        });

        let (tx, rx) = mpsc::channel(8);
        tx.try_send(ProviderEvent::Start).expect("start");
        if self.emit_partial {
            tx.try_send(ProviderEvent::TextStart { content_index: 0 })
                .expect("text start");
            tx.try_send(ProviderEvent::TextDelta {
                content_index: 0,
                delta: "authoritative text".to_owned(),
            })
            .expect("text delta");
            tx.try_send(ProviderEvent::ThinkingStart {
                content_index: 1,
                signature_field: "reasoning_content".to_owned(),
            })
            .expect("thinking start");
            tx.try_send(ProviderEvent::ThinkingDelta {
                content_index: 1,
                delta: "authoritative thinking".to_owned(),
            })
            .expect("thinking delta");
            tx.try_send(ProviderEvent::ThinkingEnd {
                content_index: 1,
                content: "authoritative thinking".to_owned(),
            })
            .expect("thinking end");
        }
        self.senders.lock().expect("senders").push(tx);

        Ok(ProviderAttempt {
            message_id: format!("assistant-{attempt}"),
            initial_message: public_message(&assistant(StopReason::Stop, Vec::new(), None, None)),
            events: crate::provider::types::ProviderEventStream::new(
                rx,
                cancel,
                "fixture",
                origin(),
            ),
        })
    }

    async fn execute_tool_observed(
        &self,
        _flow_id: &str,
        _call: &ToolCall,
        _cancel: CancellationToken,
        _on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<ToolResultMessage, ToolError> {
        Err(ToolError::Protocol(
            "CancellingProbeDriver has no tools".to_owned(),
        ))
    }

    fn synthetic_error(&self, _message: &str) -> PublicMessage {
        unreachable!("CancellingProbeDriver has no synthetic errors")
    }

    async fn plan_overflow_recovery(
        &self,
        _core: &RunCore,
        _request: OverflowRecoveryRequest,
        _active_context: &[ContextMessage],
    ) -> Result<OverflowRecoveryOutcome> {
        unreachable!("CancellingProbeDriver has no overflow recovery")
    }
}

/// Drain events from `events_rx` until the worker has emitted the assistant
/// `MessageStart` for the first provider attempt. The worker is blocked on the
/// bounded event channel at capacity 1 after that emit.
async fn drain_to_first_assistant_start(events_rx: &mut mpsc::Receiver<RunOutput>) -> bool {
    let mut found_assistant = false;
    let mut seq = 1;
    while let Some(mut output) = events_rx.recv().await {
        resolve_message_output(&mut output, &mut seq);
        if let AgentEvent::MessageStart { message, .. } = output.event
            && matches!(message.as_ref(), PublicMessage::Assistant(_))
        {
            found_assistant = true;
            break;
        }
    }
    found_assistant
}

async fn drain_to_partial_thinking_end(events_rx: &mut mpsc::Receiver<RunOutput>) {
    while let Some(output) = events_rx.recv().await {
        if matches!(
            output.event,
            AgentEvent::MessageUpdate {
                event: PublicStreamEvent::ThinkingEnd { .. },
                ..
            }
        ) {
            return;
        }
    }
    panic!("provider closed before the authoritative partial was projected");
}

async fn receive_partial_message_end(
    events_rx: &mut mpsc::Receiver<RunOutput>,
) -> PublicAssistantMessage {
    let mut message_seq = 1;
    loop {
        let mut output = events_rx.recv().await.expect("partial MessageEnd");
        resolve_message_output(&mut output, &mut message_seq);
        if let AgentEvent::MessageEnd { message, .. } = output.event {
            let PublicMessage::Assistant(message) = *message else {
                panic!("partial terminal must be assistant");
            };
            return message;
        }
    }
}

#[tokio::test]
async fn provider_streaming_abort_dropped_accept_is_no_op() {
    let driver = Arc::new(CancellingProbeDriver::new());
    let worker = SequentialRunWorker::new(driver.clone());
    let (control_tx, control_rx) = mpsc::channel(8);
    let (events_tx, mut events_rx) = mpsc::channel(1);

    let handle = tokio::spawn(async move {
        worker
            .run(bound_core(1), admitted_user(1), control_rx, events_tx)
            .await
    });

    let found = drain_to_first_assistant_start(&mut events_rx).await;
    if !found {
        if handle.is_finished() {
            let completion = handle.await.expect("worker join");
            let completion = match completion {
                RunCompletion::Completed(_) => "completed".to_owned(),
                RunCompletion::Failed { failure, .. } => format!("{failure}"),
            };
            panic!("worker completed before assistant MessageStart: {completion}");
        }
        panic!("worker must emit assistant MessageStart");
    }

    let (accepted_tx, accepted_rx) = oneshot::channel();
    drop(accepted_rx);
    control_tx
        .send(RunControl::Abort {
            command: admitted_abort(2),
            accepted: accepted_tx,
            committed: oneshot::channel().1,
        })
        .await
        .expect("abort control");

    // The worker has already unblocked; there is no extra event to receive.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !driver.cancelled.load(Ordering::SeqCst),
        "dropped accepted receiver must not cancel the provider"
    );

    handle.abort();
    let _ = handle.await;
}

#[tokio::test]
async fn provider_abort_waits_for_durable_authorization_before_cancelling() {
    let driver = Arc::new(CancellingProbeDriver::new());
    let worker = SequentialRunWorker::new(driver.clone());
    let (control_tx, control_rx) = mpsc::channel(8);
    let (events_tx, mut events_rx) = mpsc::channel(8);

    let handle = tokio::spawn(async move {
        worker
            .run(bound_core(1), admitted_user(1), control_rx, events_tx)
            .await
    });
    assert!(drain_to_first_assistant_start(&mut events_rx).await);

    let (accepted_tx, accepted_rx) = oneshot::channel();
    let (committed_tx, committed_rx) = oneshot::channel();
    control_tx
        .send(RunControl::Abort {
            command: admitted_abort(2),
            accepted: accepted_tx,
            committed: committed_rx,
        })
        .await
        .expect("abort control");
    assert!(accepted_rx.await.expect("worker accepts abort"));
    tokio::task::yield_now().await;
    assert!(
        !driver.cancelled.load(Ordering::SeqCst),
        "provider cancellation must remain behind the durable cutoff"
    );

    committed_tx.send(()).expect("authorize durable abort");
    tokio::time::timeout(Duration::from_secs(1), async {
        while !driver.cancelled.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("provider observes authorized cancellation");

    handle.abort();
    let _ = handle.await;
}

#[tokio::test]
async fn provider_abort_dropped_durable_authorization_is_a_no_op() {
    let driver = Arc::new(CancellingProbeDriver::new());
    let worker = SequentialRunWorker::new(driver.clone());
    let (control_tx, control_rx) = mpsc::channel(8);
    let (events_tx, mut events_rx) = mpsc::channel(8);
    let handle = tokio::spawn(async move {
        worker
            .run(bound_core(1), admitted_user(1), control_rx, events_tx)
            .await
    });
    assert!(drain_to_first_assistant_start(&mut events_rx).await);

    let (accepted_tx, accepted_rx) = oneshot::channel();
    let (committed_tx, committed_rx) = oneshot::channel::<()>();
    control_tx
        .send(RunControl::Abort {
            command: admitted_abort(2),
            accepted: accepted_tx,
            committed: committed_rx,
        })
        .await
        .expect("abort control");
    assert!(accepted_rx.await.expect("worker accepts abort"));
    drop(committed_tx);
    tokio::task::yield_now().await;
    assert!(
        !driver.cancelled.load(Ordering::SeqCst),
        "failed durable Abort must not cancel the provider"
    );

    handle.abort();
    let _ = handle.await;
}

#[tokio::test]
async fn hard_steer_persists_authoritative_accumulated_text_and_thinking_without_marker() {
    let driver = Arc::new(CancellingProbeDriver::new().with_partial());
    let cancellation = Arc::new(AttemptCancellation::default());
    let mut core = bound_core(1);
    core.attempt_cancellation = Some(cancellation.clone());
    let (control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(16);
    let mut runner = Runner::new(core, driver, control_rx, events_tx);
    let handle = tokio::spawn(async move { runner.provider_attempt().await });

    drain_to_partial_thinking_end(&mut events_rx).await;
    let (accepted_tx, accepted_rx) = oneshot::channel();
    control_tx
        .send(RunControl::HardSteer {
            command: admitted_user(2),
            accepted: accepted_tx,
        })
        .await
        .expect("hard steer control");
    assert!(accepted_rx.await.expect("worker accepts hard steer"));
    cancellation
        .reserve()
        .expect("reserve provider attempt")
        .cancel_after_commit();

    let partial = receive_partial_message_end(&mut events_rx).await;
    assert_eq!(
        partial.content,
        vec![
            PublicAssistantContent::Text {
                text: "authoritative text".to_owned(),
                wire_item_index: 0,
            },
            PublicAssistantContent::Thinking {
                thinking: "authoritative thinking".to_owned(),
                signature_field: "reasoning_content".to_owned(),
                wire_item_index: 1,
            },
        ],
        "durable hard-steer content must come from the stream assembler, not initial metadata"
    );
    assert!(partial.interrupted);
    assert_eq!(partial.stop_reason, StopReason::Aborted);
    let replay = crate::memory::transform::transform(
        &[ContextMessage::Synthetic {
            message: public_to_message(PublicMessage::Assistant(partial.clone())),
        }],
        &origin(),
    );
    let Message::Assistant(replayed) = context_message(&replay[0]) else {
        panic!("hard-steer partial must replay as assistant");
    };
    assert_eq!(
        replayed
            .content
            .iter()
            .filter(|content| matches!(
                content,
                AssistantContent::Text { text, .. }
                    if text == crate::memory::transform::INTERRUPTION_MARKER
            ))
            .count(),
        1,
        "persistence plus replay must append exactly one interruption marker"
    );

    assert!(matches!(
        handle
            .await
            .expect("provider attempt join")
            .expect("provider attempt"),
        AttemptOutcome::HardSteer
    ));
}

#[tokio::test]
async fn abort_persists_authoritative_accumulated_content_without_steer_marker() {
    let driver = Arc::new(CancellingProbeDriver::new().with_partial());
    let cancellation = Arc::new(AttemptCancellation::default());
    let mut core = bound_core(1);
    core.attempt_cancellation = Some(cancellation);
    let (control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(16);
    let mut runner = Runner::new(core, driver, control_rx, events_tx);
    let handle = tokio::spawn(async move { runner.provider_attempt().await });

    drain_to_partial_thinking_end(&mut events_rx).await;
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let (committed_tx, committed_rx) = oneshot::channel();
    control_tx
        .send(RunControl::Abort {
            command: admitted_abort(2),
            accepted: accepted_tx,
            committed: committed_rx,
        })
        .await
        .expect("abort control");
    assert!(accepted_rx.await.expect("worker accepts abort"));
    committed_tx.send(()).expect("authorize durable abort");

    let partial = receive_partial_message_end(&mut events_rx).await;
    assert_eq!(partial.content.len(), 2);
    assert!(matches!(
        &partial.content[0],
        PublicAssistantContent::Text { text, .. } if text == "authoritative text"
    ));
    assert!(
        partial.content.iter().all(|content| !matches!(
            content,
            PublicAssistantContent::Text { text, .. }
                if text == crate::memory::transform::INTERRUPTION_MARKER
        )),
        "Abort must not persist the hard-steer replay marker"
    );
    assert!(partial.interrupted);

    assert!(matches!(
        handle
            .await
            .expect("provider attempt join")
            .expect("provider attempt"),
        AttemptOutcome::ClosedError { .. }
    ));
}

#[tokio::test]
async fn provider_streaming_hard_steer_dropped_accept_is_no_op() {
    let driver = Arc::new(CancellingProbeDriver::new());
    let worker = SequentialRunWorker::new(driver.clone());
    let (control_tx, control_rx) = mpsc::channel(8);
    let (events_tx, mut events_rx) = mpsc::channel(1);

    let handle = tokio::spawn(async move {
        worker
            .run(bound_core(1), admitted_user(1), control_rx, events_tx)
            .await
    });

    let found = drain_to_first_assistant_start(&mut events_rx).await;
    assert!(found, "worker must emit assistant MessageStart");

    let (accepted_tx, accepted_rx) = oneshot::channel();
    drop(accepted_rx);
    control_tx
        .send(RunControl::HardSteer {
            command: admitted_user(2),
            accepted: accepted_tx,
        })
        .await
        .expect("hard steer control");

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !driver.cancelled.load(Ordering::SeqCst),
        "dropped accepted receiver must not cancel the provider"
    );

    handle.abort();
    let _ = handle.await;
}

struct ControlProbeDriver {
    released: Arc<Notify>,
    update_dropped: Arc<Notify>,
    cancelled: Arc<AtomicBool>,
}

impl ControlProbeDriver {
    fn new() -> Self {
        Self {
            released: Arc::new(Notify::new()),
            update_dropped: Arc::new(Notify::new()),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl RunDriver for ControlProbeDriver {
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
        Err(anyhow!("ControlProbeDriver has no provider"))
    }

    async fn execute_tool_observed(
        &self,
        _flow_id: &str,
        call: &ToolCall,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<ToolResultMessage, ToolError> {
        self.update_dropped.notify_one();
        let cancelled = self.cancelled.clone();
        tokio::select! {
            _ = self.released.notified() => {}
            _ = cancel.cancelled() => {
                cancelled.store(true, Ordering::SeqCst);
            }
        }
        drop(on_update);
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

    fn synthetic_error(&self, _message: &str) -> PublicMessage {
        unreachable!("ControlProbeDriver has no synthetic errors")
    }

    async fn plan_overflow_recovery(
        &self,
        _core: &RunCore,
        _request: OverflowRecoveryRequest,
        _active_context: &[ContextMessage],
    ) -> Result<OverflowRecoveryOutcome> {
        unreachable!("ControlProbeDriver has no overflow recovery")
    }
}

#[tokio::test]
async fn tool_execution_abort_dropped_accept_is_no_op() {
    let driver = Arc::new(ControlProbeDriver::new());
    let (control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(8);
    let mut runner = super::Runner::new(bound_core(1), driver.clone(), control_rx, events_tx);
    let call = call("probe");

    let handle =
        tokio::spawn(async move { runner.execute_tool_with_updates("assistant-1", &call).await });

    driver.update_dropped.notified().await;

    let (accepted_tx, accepted_rx) = oneshot::channel();
    drop(accepted_rx);
    control_tx
        .send(RunControl::Abort {
            command: admitted_abort(2),
            accepted: accepted_tx,
            committed: oneshot::channel().1,
        })
        .await
        .expect("abort control");

    tokio::time::sleep(Duration::from_millis(50)).await;
    driver.released.notify_one();

    let result = tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("tool should complete")
        .expect("runner task join");

    assert!(
        result.is_ok(),
        "dropped accepted receiver must not cancel the tool: {result:?}"
    );
    assert!(
        !driver.cancelled.load(Ordering::SeqCst),
        "dropped accepted receiver must not cancel the tool"
    );

    // Drain any progress/terminal events to avoid a hung event collector.
    while events_rx.try_recv().is_ok() {}
}

#[tokio::test]
async fn tool_abort_waits_for_durable_authorization_before_cancelling() {
    let driver = Arc::new(ControlProbeDriver::new());
    let (control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(8);
    let mut runner = super::Runner::new(bound_core(1), driver.clone(), control_rx, events_tx);
    let call = call("probe");

    let handle =
        tokio::spawn(async move { runner.execute_tool_with_updates("assistant-1", &call).await });
    driver.update_dropped.notified().await;

    let (accepted_tx, accepted_rx) = oneshot::channel();
    let (committed_tx, committed_rx) = oneshot::channel();
    control_tx
        .send(RunControl::Abort {
            command: admitted_abort(2),
            accepted: accepted_tx,
            committed: committed_rx,
        })
        .await
        .expect("abort control");
    assert!(accepted_rx.await.expect("worker accepts abort"));
    tokio::task::yield_now().await;
    assert!(
        !driver.cancelled.load(Ordering::SeqCst),
        "tool cancellation must remain behind the durable cutoff"
    );

    committed_tx.send(()).expect("authorize durable abort");
    let result = tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("authorized abort completes")
        .expect("runner task join");
    assert!(matches!(result, Err(ExecuteToolError::Cancelled)));
    assert!(driver.cancelled.load(Ordering::SeqCst));

    while events_rx.try_recv().is_ok() {}
}

#[tokio::test]
async fn tool_abort_dropped_durable_authorization_is_a_no_op() {
    let driver = Arc::new(ControlProbeDriver::new());
    let (control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(8);
    let mut runner = super::Runner::new(bound_core(1), driver.clone(), control_rx, events_tx);
    let call = call("probe");
    let handle =
        tokio::spawn(async move { runner.execute_tool_with_updates("assistant-1", &call).await });
    driver.update_dropped.notified().await;

    let (accepted_tx, accepted_rx) = oneshot::channel();
    let (committed_tx, committed_rx) = oneshot::channel::<()>();
    control_tx
        .send(RunControl::Abort {
            command: admitted_abort(2),
            accepted: accepted_tx,
            committed: committed_rx,
        })
        .await
        .expect("abort control");
    assert!(accepted_rx.await.expect("worker accepts abort"));
    drop(committed_tx);
    tokio::task::yield_now().await;
    assert!(
        !driver.cancelled.load(Ordering::SeqCst),
        "failed durable Abort must not cancel the tool"
    );

    driver.released.notify_one();
    assert!(
        handle
            .await
            .expect("runner task join")
            .expect("tool completes after failed Abort")
            .content
            .iter()
            .any(|content| matches!(content, UserContent::Text { text } if text == "released"))
    );
    while events_rx.try_recv().is_ok() {}
}

#[tokio::test]
async fn tool_execution_hard_steer_dropped_accept_is_no_op() {
    let driver = Arc::new(ControlProbeDriver::new());
    let (control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(8);
    let mut runner = super::Runner::new(bound_core(1), driver.clone(), control_rx, events_tx);
    let call = call("probe");

    let handle =
        tokio::spawn(async move { runner.execute_tool_with_updates("assistant-1", &call).await });

    driver.update_dropped.notified().await;

    let (accepted_tx, accepted_rx) = oneshot::channel();
    drop(accepted_rx);
    control_tx
        .send(RunControl::HardSteer {
            command: admitted_user(2),
            accepted: accepted_tx,
        })
        .await
        .expect("hard steer control");

    tokio::time::sleep(Duration::from_millis(50)).await;
    driver.released.notify_one();

    let result = tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("tool should complete")
        .expect("runner task join");

    assert!(
        result.is_ok(),
        "dropped accepted receiver must not cancel the tool: {result:?}"
    );
    assert!(!driver.cancelled.load(Ordering::SeqCst));

    while events_rx.try_recv().is_ok() {}
}

struct HardSteerToolDriver {
    started_contexts: Mutex<Vec<Vec<ContextMessage>>>,
    executed_tools: Mutex<Vec<String>>,
    tool_started: Arc<Notify>,
    tool_released: Arc<Notify>,
    tool_cancelled: Arc<AtomicBool>,
}

impl HardSteerToolDriver {
    fn new() -> Self {
        Self {
            started_contexts: Mutex::new(Vec::new()),
            executed_tools: Mutex::new(Vec::new()),
            tool_started: Arc::new(Notify::new()),
            tool_released: Arc::new(Notify::new()),
            tool_cancelled: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl RunDriver for HardSteerToolDriver {
    fn validate_executor_generation(&self, _generation: ProcessGeneration) -> Result<()> {
        Ok(())
    }

    async fn start_provider_for_command(
        &self,
        attempt: usize,
        context: &[ContextMessage],
        _command_received_at: Option<std::time::Instant>,
        _cancel: CancellationToken,
    ) -> Result<ProviderAttempt> {
        let mut contexts = self.started_contexts.lock().expect("contexts");
        let first = contexts.is_empty();
        contexts.push(context.to_vec());
        let message = if first {
            assistant(
                StopReason::ToolUse,
                vec![
                    AssistantContent::ToolCall {
                        tool_call: call("probe"),
                        wire_item_index: 0,
                    },
                    AssistantContent::ToolCall {
                        tool_call: call("not-started"),
                        wire_item_index: 1,
                    },
                ],
                None,
                None,
            )
        } else {
            assistant(StopReason::Stop, Vec::new(), None, None)
        };
        Ok(provider_attempt(attempt, message))
    }

    async fn execute_tool_observed(
        &self,
        _flow_id: &str,
        call: &ToolCall,
        cancel: CancellationToken,
        _on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<ToolResultMessage, ToolError> {
        self.executed_tools
            .lock()
            .expect("executed tools")
            .push(call.id.clone());
        self.tool_started.notify_one();
        tokio::select! {
            _ = self.tool_released.notified() => {}
            _ = cancel.cancelled() => {
                self.tool_cancelled.store(true, Ordering::SeqCst);
            }
        }
        Ok(ToolResultMessage {
            tool_call_id: call.id.clone(),
            tool_name: call.name.clone(),
            content: vec![UserContent::Text {
                text: "cancelled".to_owned(),
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
        Err(anyhow!("HardSteerToolDriver has no overflow recovery"))
    }
}

#[tokio::test]
async fn soft_steer_during_tool_execution_lets_active_tool_finish() {
    let driver = Arc::new(HardSteerToolDriver::new());
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

    driver.tool_started.notified().await;

    let (accepted_tx, accepted_rx) = oneshot::channel();
    let (committed_tx, committed_rx) = oneshot::channel();
    control_tx
        .send(RunControl::SoftSteer {
            command: admitted_user(2),
            accepted: accepted_tx,
            committed: committed_rx,
        })
        .await
        .expect("soft steer control");
    assert!(accepted_rx.await.expect("worker accepts soft steer"));
    committed_tx.send(()).expect("authorize durable soft steer");
    driver.tool_released.notify_one();

    let completion = tokio::time::timeout(Duration::from_secs(2), completion)
        .await
        .expect("worker should complete")
        .expect("worker join");
    let events = tokio::time::timeout(Duration::from_secs(2), event_collector)
        .await
        .expect("collector should complete")
        .expect("event collector join");

    assert_completed(completion);
    assert!(
        !driver.tool_cancelled.load(Ordering::SeqCst),
        "soft steer must not cancel the running tool"
    );
    assert_eq!(
        *driver.executed_tools.lock().expect("executed tools"),
        vec!["probe".to_owned()],
        "soft steer must prevent the not-started call from reaching the executor"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::ToolExecutionStart { .. }))
            .count(),
        1,
        "not-started calls must not emit ToolExecutionStart: {events:#?}"
    );
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::MessageEnd {
            message,
            ..
        } if matches!(
            message.as_ref(),
            PublicMessage::ToolResult(result)
                if result.tool_call_id == "not-started"
                    && result.is_error
                    && matches!(
                        &result.content[0],
                        UserContent::Text { text }
                            if text == "ユーザーの新しい指示により実行前に取り消された"
                    )
        )
    )));

    let hard_steer_id = user_message_id(&admitted_user(2).envelope().command_id);
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    AgentEvent::MessageEnd { message_id, .. } if message_id == &hard_steer_id
                )
            })
            .count(),
        1,
        "hard-steer user message must be injected exactly once on the next turn: {events:#?}"
    );

    let contexts = driver.started_contexts.lock().expect("contexts");
    assert_eq!(
        contexts.len(),
        2,
        "there must be a second provider attempt after the hard steer"
    );
    assert!(
        contexts[1].iter().any(|message| matches!(
            context_message(message),
            Message::User(user)
                if matches!(&user.content[0], UserContent::Text { text } if text == "message 2")
        )),
        "second provider context must include the hard-steer user message: {contexts:?}"
    );
}

#[tokio::test]
async fn emit_tool_start_preempts_by_controls_queued_before_commit() {
    let driver = Arc::new(FixtureDriver::new(vec![]));
    let (control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(8);
    let mut runner = super::Runner::new(bound_core(1), driver, control_rx, events_tx);

    // Queue a soft-steer control before the worker attempts to durably commit
    // the ToolExecutionStart. The worker must observe it, classify the new
    // instruction, and return Preempted without emitting ToolExecutionStart.
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let (committed_tx, committed_rx) = oneshot::channel();
    let bind_commit = tokio::spawn(async move {
        if accepted_rx.await.is_ok() {
            let _ = committed_tx.send(());
        }
    });
    control_tx
        .send(RunControl::SoftSteer {
            command: admitted_user(2),
            accepted: accepted_tx,
            committed: committed_rx,
        })
        .await
        .expect("enqueue soft steer");

    let outcome = runner
        .emit_tool_start_and_wait_committed(&call("preempted"))
        .await
        .expect("preempted start should not fail");
    assert_eq!(outcome, ToolStartOutcome::Preempted);

    bind_commit.await.expect("steer bind commit task");

    assert!(
        runner.in_flight_controls.len() == 1,
        "queued soft steer must be claimed for injection"
    );
    assert!(
        events_rx.try_recv().is_err(),
        "ToolExecutionStart must not be committed when preempted"
    );
}

/// Simulates the race where the `ToolExecutionStart` output has been sent and
/// its `ToolStartCommitBarrier` is deliberately held, then a soft-steer control
/// arrives and is durably authorized before the barrier completes. The worker
/// must return `Preempted` and the start output must never be committed.
#[tokio::test]
async fn tool_start_barrier_held_by_soft_steer_preempts() {
    let driver = Arc::new(FixtureDriver::new(Vec::new()));
    let (control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(8);
    let mut runner = super::Runner::new(bound_core(1), driver, control_rx, events_tx);

    // Collector receives the ToolExecutionStart and holds the barrier so the
    // start is not durably committed until the test decides.
    let (start_held_tx, start_held_rx) = oneshot::channel();
    let (drop_tx, drop_rx) = oneshot::channel::<()>();
    let collector = tokio::spawn(async move {
        let output = events_rx.recv().await.expect("ToolExecutionStart output");
        assert!(matches!(
            output.event,
            AgentEvent::ToolExecutionStart { .. }
        ));
        let _ = start_held_tx.send(());
        let _ = drop_rx.await;
    });

    // Send the soft-steer control only after the start output is held.
    let send_control = tokio::spawn(async move {
        start_held_rx.await.expect("start held");
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let (committed_tx, committed_rx) = oneshot::channel();
        control_tx
            .send(RunControl::SoftSteer {
                command: admitted_user(2),
                accepted: accepted_tx,
                committed: committed_rx,
            })
            .await
            .expect("send soft steer");
        assert!(accepted_rx.await.expect("accepted"));
        committed_tx.send(()).expect("authorize soft steer");
    });

    let outcome = runner
        .emit_tool_start_and_wait_committed(&call("held"))
        .await
        .expect("preempted start should not fail");
    assert_eq!(outcome, ToolStartOutcome::Preempted);

    // Allow the collector to drop the held output without resolving the start
    // barrier.
    drop(drop_tx);
    collector.await.expect("collector");
    send_control.await.expect("control sender");

    assert_eq!(
        runner.in_flight_controls.len(),
        1,
        "soft steer must be claimed for injection"
    );
    assert!(!runner.abort_requested);
    assert!(runner.core.next_followup().is_none());
}

/// Same race as `tool_start_barrier_held_by_soft_steer_preempts`, but with an
/// abort control. The tool must not execute and the start output must not be
/// durably committed.
#[tokio::test]
async fn tool_start_barrier_held_by_abort_preempts() {
    let driver = Arc::new(FixtureDriver::new(Vec::new()));
    let (control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(8);
    let mut runner = super::Runner::new(bound_core(1), driver, control_rx, events_tx);

    let (start_held_tx, start_held_rx) = oneshot::channel();
    let (drop_tx, drop_rx) = oneshot::channel::<()>();
    let collector = tokio::spawn(async move {
        let output = events_rx.recv().await.expect("ToolExecutionStart output");
        assert!(matches!(
            output.event,
            AgentEvent::ToolExecutionStart { .. }
        ));
        let _ = start_held_tx.send(());
        let _ = drop_rx.await;
    });

    let send_control = tokio::spawn(async move {
        start_held_rx.await.expect("start held");
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let (committed_tx, committed_rx) = oneshot::channel();
        control_tx
            .send(RunControl::Abort {
                command: admitted_abort(2),
                accepted: accepted_tx,
                committed: committed_rx,
            })
            .await
            .expect("send abort");
        assert!(accepted_rx.await.expect("accepted"));
        committed_tx.send(()).expect("authorize abort");
    });

    let outcome = runner
        .emit_tool_start_and_wait_committed(&call("held"))
        .await
        .expect("preempted start should not fail");
    assert_eq!(outcome, ToolStartOutcome::Preempted);

    drop(drop_tx);
    collector.await.expect("collector");
    send_control.await.expect("control sender");

    assert!(runner.abort_requested, "abort must be requested");
    assert!(runner.in_flight_controls.is_empty());
    assert!(runner.core.next_followup().is_none());
}

#[tokio::test]
async fn dropped_soft_steer_authorization_cannot_preempt_tool_start() {
    let driver = Arc::new(FixtureDriver::new(Vec::new()));
    let (control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(8);
    let mut runner = super::Runner::new(bound_core(1), driver, control_rx, events_tx);

    let (start_held_tx, start_held_rx) = oneshot::channel();
    let (commit_start_tx, commit_start_rx) = oneshot::channel();
    let collector = tokio::spawn(async move {
        let output = events_rx.recv().await.expect("ToolExecutionStart output");
        let barrier = output
            .commit_barrier
            .expect("ToolExecutionStart commit barrier");
        start_held_tx.send(()).expect("announce held start");
        commit_start_rx.await.expect("release held start");
        barrier.committed();
    });
    let send_control = tokio::spawn(async move {
        start_held_rx.await.expect("start held");
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let (committed_tx, committed_rx) = oneshot::channel::<()>();
        control_tx
            .send(RunControl::SoftSteer {
                command: admitted_user(2),
                accepted: accepted_tx,
                committed: committed_rx,
            })
            .await
            .expect("send soft steer");
        assert!(accepted_rx.await.expect("accepted"));
        drop(committed_tx);
        commit_start_tx.send(()).expect("release held start");
    });

    let outcome = runner
        .emit_tool_start_and_wait_committed(&call("authorization-dropped"))
        .await
        .expect("dropped authorization must leave the committed start authoritative");
    assert_eq!(outcome, ToolStartOutcome::Started);
    assert!(runner.in_flight_controls.is_empty());
    collector.await.expect("collector");
    send_control.await.expect("control sender");
}

#[tokio::test]
async fn dropped_abort_authorization_cannot_cancel_committed_tool_start() {
    let driver = Arc::new(FixtureDriver::new(Vec::new()));
    let (control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(8);
    let mut runner = super::Runner::new(bound_core(1), driver, control_rx, events_tx);

    let (start_held_tx, start_held_rx) = oneshot::channel();
    let (commit_start_tx, commit_start_rx) = oneshot::channel();
    let collector = tokio::spawn(async move {
        let output = events_rx.recv().await.expect("ToolExecutionStart output");
        let barrier = output
            .commit_barrier
            .expect("ToolExecutionStart commit barrier");
        start_held_tx.send(()).expect("announce held start");
        commit_start_rx.await.expect("release held start");
        barrier.committed();
    });
    let send_control = tokio::spawn(async move {
        start_held_rx.await.expect("start held");
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let (committed_tx, committed_rx) = oneshot::channel::<()>();
        control_tx
            .send(RunControl::Abort {
                command: admitted_abort(2),
                accepted: accepted_tx,
                committed: committed_rx,
            })
            .await
            .expect("send abort");
        assert!(accepted_rx.await.expect("accepted"));
        drop(committed_tx);
        commit_start_tx.send(()).expect("release held start");
    });

    let outcome = runner
        .emit_tool_start_and_wait_committed(&call("abort-authorization-dropped"))
        .await
        .expect("dropped authorization must leave the committed start authoritative");
    assert_eq!(outcome, ToolStartOutcome::Started);
    assert!(!runner.abort_requested);
    collector.await.expect("collector");
    send_control.await.expect("control sender");
}

#[tokio::test]
async fn safe_point_abort_dropped_accept_is_no_op() {
    let driver = Arc::new(FixtureDriver::new(vec![]));
    let (control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, _events_rx) = mpsc::channel(8);
    let mut runner = super::Runner::new(bound_core(1), driver, control_rx, events_tx);

    let cancel = CancellationToken::new();
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel_watch = cancel.clone();
    let cancelled_flag = cancelled.clone();
    tokio::spawn(async move {
        cancel_watch.cancelled().await;
        cancelled_flag.store(true, Ordering::SeqCst);
    });
    runner.provider_cancel = Some(cancel);

    let (accepted_tx, accepted_rx) = oneshot::channel();
    drop(accepted_rx);
    control_tx
        .send(RunControl::Abort {
            command: admitted_abort(2),
            accepted: accepted_tx,
            committed: oneshot::channel().1,
        })
        .await
        .expect("abort control");

    runner
        .receive_control_safe_point()
        .await
        .expect("safe point");

    assert!(!runner.abort_requested, "abort must not be applied");
    assert!(
        !cancelled.load(Ordering::SeqCst),
        "provider must not be cancelled"
    );
}

#[tokio::test]
async fn safe_point_hard_steer_dropped_accept_is_no_op() {
    let driver = Arc::new(FixtureDriver::new(vec![]));
    let (control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, _events_rx) = mpsc::channel(8);
    let mut runner = super::Runner::new(bound_core(1), driver, control_rx, events_tx);

    let cancel = CancellationToken::new();
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel_watch = cancel.clone();
    let cancelled_flag = cancelled.clone();
    tokio::spawn(async move {
        cancel_watch.cancelled().await;
        cancelled_flag.store(true, Ordering::SeqCst);
    });
    runner.provider_cancel = Some(cancel);

    let (accepted_tx, accepted_rx) = oneshot::channel();
    drop(accepted_rx);
    control_tx
        .send(RunControl::HardSteer {
            command: admitted_user(2),
            accepted: accepted_tx,
        })
        .await
        .expect("hard steer control");

    runner
        .receive_control_safe_point()
        .await
        .expect("safe point");

    assert!(
        runner.core.pending_controls.is_empty(),
        "hard steer must not be queued"
    );
    assert!(
        !cancelled.load(Ordering::SeqCst),
        "provider must not be cancelled"
    );
}

#[tokio::test]
async fn retry_wait_hard_steer_dropped_accept_continues_without_duplicate() {
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

    let (accepted_tx, accepted_rx) = oneshot::channel();
    drop(accepted_rx);
    control_tx
        .send(RunControl::HardSteer {
            command: admitted_user(2),
            accepted: accepted_tx,
        })
        .await
        .expect("stale hard steer");

    control_tx
        .send(RunControl::Command(admitted_user(2)))
        .await
        .expect("normal durable deferral");

    assert_completed(completion.await.expect("worker join"));
    let events = event_collector.await.expect("event collector");
    let user_message_id = user_message_id(&admitted_user(2).envelope().command_id);
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                matches!(event, AgentEvent::MessageEnd { message_id, .. } if message_id == &user_message_id)
            })
            .count(),
        1,
        "dropped hard steer must not be injected in addition to the durable deferral"
    );
}

#[tokio::test]
async fn retry_wait_abort_dropped_accept_continues_without_cancelling() {
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

    let (accepted_tx, accepted_rx) = oneshot::channel();
    drop(accepted_rx);
    control_tx
        .send(RunControl::Abort {
            command: admitted_abort(2),
            accepted: accepted_tx,
            committed: oneshot::channel().1,
        })
        .await
        .expect("stale abort");

    control_tx
        .send(RunControl::Command(admitted_user(2)))
        .await
        .expect("normal durable deferral");

    assert_completed(completion.await.expect("worker join"));
    let events = event_collector.await.expect("event collector");
    let user_message_id = user_message_id(&admitted_user(2).envelope().command_id);
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                matches!(event, AgentEvent::MessageEnd { message_id, .. } if message_id == &user_message_id)
            })
            .count(),
        1,
        "dropped abort must not cancel the worker before the durable deferral"
    );
}

#[tokio::test]
async fn provider_attempt_cancellation_deferred_until_hard_steer_commit() {
    let driver = Arc::new(CancellingProbeDriver::new());
    let cancellation = Arc::new(AttemptCancellation::default());
    let mut core = bound_core(1);
    core.attempt_cancellation = Some(cancellation.clone());
    let (control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(1);
    let mut runner = Runner::new(core, driver.clone(), control_rx, events_tx);
    let handle = tokio::spawn(async move { runner.provider_attempt().await });

    assert!(drain_to_first_assistant_start(&mut events_rx).await);
    assert!(!driver.cancelled.load(Ordering::SeqCst));

    let (accepted_tx, _accepted_rx) = oneshot::channel();
    control_tx
        .send(RunControl::HardSteer {
            command: admitted_user(2),
            accepted: accepted_tx,
        })
        .await
        .expect("send hard steer");

    let reservation = cancellation.reserve().expect("reserve active attempt");
    assert!(!driver.cancelled.load(Ordering::SeqCst));
    reservation.cancel_after_commit();
    tokio::task::yield_now().await;
    assert!(driver.cancelled.load(Ordering::SeqCst));

    let mut seq = 1;
    let mut output = events_rx
        .recv()
        .await
        .expect("hard-steer partial MessageEnd");
    resolve_message_output(&mut output, &mut seq);

    let outcome = handle
        .await
        .expect("provider attempt join")
        .expect("provider attempt");
    assert!(matches!(outcome, super::AttemptOutcome::HardSteer));
}

#[tokio::test]
async fn failed_hard_steer_reservation_restores_provider_for_abort() {
    let driver = Arc::new(CancellingProbeDriver::new());
    let cancellation = Arc::new(AttemptCancellation::default());
    let mut core = bound_core(1);
    core.attempt_cancellation = Some(cancellation.clone());
    let (control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(1);
    let mut runner = Runner::new(core, driver.clone(), control_rx, events_tx);
    let handle = tokio::spawn(async move { runner.provider_attempt().await });

    assert!(drain_to_first_assistant_start(&mut events_rx).await);
    assert!(!driver.cancelled.load(Ordering::SeqCst));

    let (accepted_tx, _accepted_rx) = oneshot::channel();
    control_tx
        .send(RunControl::HardSteer {
            command: admitted_user(2),
            accepted: accepted_tx,
        })
        .await
        .expect("send hard steer");

    let reservation = cancellation.reserve().expect("reserve active attempt");
    assert!(!driver.cancelled.load(Ordering::SeqCst));
    reservation.restore().expect("restore reservation");
    assert!(!driver.cancelled.load(Ordering::SeqCst));

    let (abort_accepted_tx, _abort_accepted_rx) = oneshot::channel();
    let (abort_committed_tx, abort_committed_rx) = oneshot::channel();
    abort_committed_tx
        .send(())
        .expect("authorize durable abort");
    control_tx
        .send(RunControl::Abort {
            command: admitted_abort(3),
            accepted: abort_accepted_tx,
            committed: abort_committed_rx,
        })
        .await
        .expect("send abort");

    let mut seq = 1;
    let mut output = events_rx
        .recv()
        .await
        .expect("aborted assistant MessageEnd");
    resolve_message_output(&mut output, &mut seq);

    let outcome = handle
        .await
        .expect("provider attempt join")
        .expect("provider attempt");
    assert!(matches!(outcome, super::AttemptOutcome::ClosedError { .. }));
    tokio::task::yield_now().await;
    assert!(driver.cancelled.load(Ordering::SeqCst));
}

async fn abort_during_tool_with_steer_event_sequence(
    steer: Option<(RunControl, oneshot::Receiver<bool>, oneshot::Sender<()>)>,
) -> (RunCompletion, Vec<AgentEvent>, Arc<HardSteerToolDriver>) {
    let driver = Arc::new(HardSteerToolDriver::new());
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

    driver.tool_started.notified().await;

    if let Some((steer, accepted_rx, committed_tx)) = steer {
        control_tx.send(steer).await.expect("send steer control");
        assert!(accepted_rx.await.expect("worker accepts steer"));
        committed_tx.send(()).expect("authorize durable steer");
    }

    let (abort_accepted_tx, abort_accepted_rx) = oneshot::channel();
    let (abort_committed_tx, abort_committed_rx) = oneshot::channel();
    control_tx
        .send(RunControl::Abort {
            command: admitted_abort(3),
            accepted: abort_accepted_tx,
            committed: abort_committed_rx,
        })
        .await
        .expect("send abort");
    assert!(abort_accepted_rx.await.expect("worker accepts abort"));
    abort_committed_tx
        .send(())
        .expect("authorize durable abort");

    let completion = tokio::time::timeout(Duration::from_secs(2), completion)
        .await
        .expect("worker should complete")
        .expect("worker join");
    let events = tokio::time::timeout(Duration::from_secs(2), event_collector)
        .await
        .expect("event collector should complete")
        .expect("event collector join");

    (completion, events, driver)
}

#[tokio::test]
async fn abort_during_tool_execution_closes_normally_without_steer() {
    let (completion, events, driver) = abort_during_tool_with_steer_event_sequence(None).await;

    assert_completed(completion);
    assert!(driver.tool_cancelled.load(Ordering::SeqCst));

    let user_message_id = user_message_id(&admitted_user(2).envelope().command_id);
    assert!(
        !events.iter().any(|event| matches!(
            event,
            AgentEvent::MessageEnd { message_id, .. } if message_id == &user_message_id
        )),
        "abort must not inject a steer user message: {events:#?}"
    );

    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::TurnEnd { .. }))
            .count(),
        1,
        "abort during tool must emit exactly one TurnEnd: {events:#?}"
    );
    assert_eq!(
        events.last(),
        Some(&AgentEvent::AgentEnd),
        "abort during tool must end with AgentEnd: {events:#?}"
    );
}

#[tokio::test]
async fn abort_during_tool_execution_drops_claimed_soft_steer_and_closes_normally() {
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let (committed_tx, committed_rx) = oneshot::channel();
    let steer = RunControl::SoftSteer {
        command: admitted_user(2),
        accepted: accepted_tx,
        committed: committed_rx,
    };
    let (completion, events, driver) =
        abort_during_tool_with_steer_event_sequence(Some((steer, accepted_rx, committed_tx))).await;

    assert_completed(completion);
    assert!(driver.tool_cancelled.load(Ordering::SeqCst));

    let user_message_id = user_message_id(&admitted_user(2).envelope().command_id);
    assert!(
        !events.iter().any(|event| matches!(
            event,
            AgentEvent::MessageEnd { message_id, .. } if message_id == &user_message_id
        )),
        "abort must discard an already-claimed soft steer and not inject it: {events:#?}"
    );

    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::TurnEnd { .. }))
            .count(),
        1,
        "abort during tool with soft steer must emit exactly one TurnEnd: {events:#?}"
    );
    assert_eq!(
        events.last(),
        Some(&AgentEvent::AgentEnd),
        "abort during tool with soft steer must end with AgentEnd: {events:#?}"
    );
}

#[tokio::test]
async fn abort_during_tool_execution_drops_claimed_retry_steer_and_closes_normally() {
    let (accepted_tx, accepted_rx) = oneshot::channel();
    let (committed_tx, committed_rx) = oneshot::channel();
    let steer = RunControl::RetrySteer {
        command: admitted_user(2),
        accepted: accepted_tx,
        committed: committed_rx,
    };
    let (completion, events, driver) =
        abort_during_tool_with_steer_event_sequence(Some((steer, accepted_rx, committed_tx))).await;

    assert_completed(completion);
    assert!(driver.tool_cancelled.load(Ordering::SeqCst));

    let user_message_id = user_message_id(&admitted_user(2).envelope().command_id);
    assert!(
        !events.iter().any(|event| matches!(
            event,
            AgentEvent::MessageEnd { message_id, .. } if message_id == &user_message_id
        )),
        "abort must discard an already-claimed retry steer and not inject it: {events:#?}"
    );

    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::TurnEnd { .. }))
            .count(),
        1,
        "abort during tool with retry steer must emit exactly one TurnEnd: {events:#?}"
    );
    assert_eq!(
        events.last(),
        Some(&AgentEvent::AgentEnd),
        "abort during tool with retry steer must end with AgentEnd: {events:#?}"
    );
}

#[tokio::test]
async fn abort_during_retry_wait_reaches_turn_end_and_agent_end() {
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

    let (abort_accepted_tx, abort_accepted_rx) = oneshot::channel();
    let (abort_committed_tx, abort_committed_rx) = oneshot::channel();
    control_tx
        .send(RunControl::Abort {
            command: admitted_abort(2),
            accepted: abort_accepted_tx,
            committed: abort_committed_rx,
        })
        .await
        .expect("send abort during retry wait");
    assert!(abort_accepted_rx.await.expect("worker accepts abort"));
    abort_committed_tx
        .send(())
        .expect("authorize durable abort during retry wait");

    let completion = tokio::time::timeout(Duration::from_secs(2), completion)
        .await
        .expect("worker should complete")
        .expect("worker join");
    let events = tokio::time::timeout(Duration::from_secs(2), event_collector)
        .await
        .expect("event collector should complete")
        .expect("event collector join");

    assert_completed(completion);
    let turn_end_events: Vec<_> = events
        .iter()
        .filter(|event| matches!(event, AgentEvent::TurnEnd { .. }))
        .collect();
    assert_eq!(
        turn_end_events.len(),
        1,
        "abort during retry wait must emit exactly one TurnEnd: {events:#?}"
    );
    assert_eq!(
        events.last(),
        Some(&AgentEvent::AgentEnd),
        "abort during retry wait must end with AgentEnd: {events:#?}"
    );
}

#[derive(Clone)]
struct RecordingReviewer {
    prompts: Arc<Mutex<Vec<ReviewerPrompt>>>,
}

impl RecordingReviewer {
    fn new() -> Self {
        Self {
            prompts: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn prompts(&self) -> Vec<ReviewerPrompt> {
        self.prompts.lock().expect("prompts").clone()
    }
}

#[async_trait]
impl ReviewerTransport for RecordingReviewer {
    async fn complete(
        &self,
        prompt: &ReviewerPrompt,
        _cancel: CancellationToken,
    ) -> Result<String, ReviewerTransportError> {
        self.prompts.lock().expect("prompts").push(prompt.clone());
        Ok(
            r#"{"outcome":"allow","risk":"low","authorization":"high","rationale":"ok"}"#
                .to_owned(),
        )
    }
}

fn bash_call(id: &str) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        name: "bash".to_owned(),
        arguments: serde_json::from_value::<ValidatedToolArguments>(
            json!({"command": "git status"}),
        )
        .expect("valid bash args"),
    }
}

#[tokio::test]
async fn multi_tool_batch_reviewer_sees_current_call_and_prior_finalized_result() {
    let transport = Arc::new(RecordingReviewer::new());
    let reviewer_projector = Arc::new(SecretAwareActionProjector::new(
        Redactor::v1(),
        SecretDigestKey::fixture(),
    ));
    let model = ReviewerModelSpec::new(
        "audit",
        "fixture",
        "https://reviewer.invalid",
        "test",
        "trusted",
        "test-policy",
    );
    let reviewer = Arc::new(Reviewer::new(
        model.clone(),
        ReviewerTrustSet::new(model, vec![]),
        transport.clone(),
        reviewer_projector,
    ));
    let projector = SecretAwareActionProjector::new(Redactor::v1(), SecretDigestKey::fixture());
    let broker = Arc::new(ApprovalBroker::new(
        Policy::new("/workspace"),
        projector,
        Some(reviewer),
        ReviewerMode::AutoReview,
        false,
        TrustedEnvironment {
            workspace_root: "/workspace".to_owned(),
            sandbox: SandboxSummary::workspace(),
            denied_paths: Vec::new(),
            denied_network_domains: Vec::new(),
            repo_visibility: None,
            git_status: None,
        },
    ));
    let driver = Arc::new(FixtureDriver::new(vec![
        output(assistant(
            StopReason::ToolUse,
            vec![
                AssistantContent::ToolCall {
                    tool_call: bash_call("first"),
                    wire_item_index: 0,
                },
                AssistantContent::ToolCall {
                    tool_call: bash_call("second"),
                    wire_item_index: 1,
                },
            ],
            None,
            None,
        )),
        output(assistant(StopReason::Stop, Vec::new(), None, None)),
    ]));
    let mut core = bound_core(1);
    core.set_approval(broker);
    let (completion, _) = run_fixture_with(driver, core, admitted_user(1)).await;
    assert_completed(completion);

    let prompts = transport.prompts();
    assert_eq!(
        prompts.len(),
        2,
        "same projection with advancing context/cache version must call reviewer twice"
    );

    fn assistant_tool_call_count(prompt: &ReviewerPrompt) -> usize {
        prompt
            .messages
            .iter()
            .filter(|m| {
                matches!(m.role, ReviewerRole::Assistant) && m.content.contains("tool_call")
            })
            .count()
    }
    fn tool_evidence_count(prompt: &ReviewerPrompt) -> usize {
        prompt
            .messages
            .iter()
            .filter(|m| {
                matches!(m.role, ReviewerRole::ToolEvidence) && m.content.contains("tool_result")
            })
            .count()
    }

    assert_eq!(
        assistant_tool_call_count(&prompts[0]),
        2,
        "first review must see both tool calls from the committed assistant message"
    );
    assert_eq!(
        tool_evidence_count(&prompts[0]),
        0,
        "first review must not see uncommitted tool results"
    );
    assert_eq!(
        assistant_tool_call_count(&prompts[1]),
        2,
        "second review must still see the current committed assistant tool-call message"
    );
    assert_eq!(
        tool_evidence_count(&prompts[1]),
        1,
        "second review must see the first finalized tool result"
    );
    assert!(
        prompts[1].messages.iter().any(|m| {
            matches!(m.role, ReviewerRole::ToolEvidence)
                && m.content.contains("bash")
                && m.content.contains("outcome=ok")
        }),
        "second review transcript must include the first bash tool result"
    );
}
