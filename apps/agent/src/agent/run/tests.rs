use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use anyhow::{Result, anyhow};
use chrono::{TimeZone, Utc};
use serde_json::json;
use tokio::sync::{Notify, mpsc};

use super::*;
use crate::{
    gateway::{CommandEnvelope, CommandId},
    provider::types::{
        ApiProtocol, AssistantContent, AssistantMessage, ProviderOrigin, ProviderOutput,
        PublicAssistantMessage, Usage, ValidatedToolArguments,
    },
};

#[derive(Clone)]
enum Script {
    Output(Box<AssistantMessage>),
    StartFailure(&'static str),
}

fn output(message: AssistantMessage) -> Script {
    Script::Output(Box::new(message))
}

struct FixtureDriver {
    scripts: Mutex<VecDeque<Script>>,
    started_contexts: Mutex<Vec<Vec<PublicMessage>>>,
    tool_order: Mutex<Vec<String>>,
    tool_failures: Mutex<VecDeque<Option<&'static str>>>,
    active_tools: AtomicUsize,
    max_active_tools: AtomicUsize,
    retry_waits: AtomicUsize,
    retry_waiting: Notify,
    block_retry: bool,
}

impl FixtureDriver {
    fn new(scripts: Vec<Script>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into()),
            started_contexts: Mutex::new(Vec::new()),
            tool_order: Mutex::new(Vec::new()),
            tool_failures: Mutex::new(VecDeque::new()),
            active_tools: AtomicUsize::new(0),
            max_active_tools: AtomicUsize::new(0),
            retry_waits: AtomicUsize::new(0),
            retry_waiting: Notify::new(),
            block_retry: false,
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
}

#[async_trait]
impl RunDriver for FixtureDriver {
    async fn start_provider(
        &self,
        attempt: usize,
        context: &[PublicMessage],
        _cancel: CancellationToken,
    ) -> Result<ProviderAttempt> {
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
        let Script::Output(message) = script else {
            let Script::StartFailure(error) = script else {
                unreachable!()
            };
            return Err(anyhow!(error));
        };
        Ok(provider_attempt(attempt, *message))
    }

    async fn execute_tool(
        &self,
        call: &ToolCall,
        _cancel: CancellationToken,
    ) -> Result<ToolResultMessage> {
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
            return Err(anyhow!(error));
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

    async fn wait_retry(&self, _delay: Duration, _cancel: &CancellationToken) -> bool {
        self.retry_waits.fetch_add(1, Ordering::SeqCst);
        self.retry_waiting.notify_one();
        if self.block_retry {
            std::future::pending().await
        } else {
            true
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
            AssistantContent::Thinking { .. } | AssistantContent::RejectedToolCall { .. } => {
                panic!("fixture helper does not need this content")
            }
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

async fn run_fixture(driver: Arc<FixtureDriver>) -> (RunCompletion, Vec<AgentEvent>) {
    let worker = SequentialRunWorker::new(driver);
    let (_control_tx, control_rx) = mpsc::channel(8);
    let (events_tx, mut events_rx) = mpsc::channel(256);
    let completion = worker
        .run(RunCore::new(), admitted_user(1), control_rx, events_tx)
        .await;
    let mut events = Vec::new();
    while let Some(event) = events_rx.recv().await {
        events.push(event);
    }
    (completion, events)
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
    let (completion, events) = run_fixture(driver.clone()).await;
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
    assert!(matches!(contexts[1][0], PublicMessage::User(_)));
    assert_eq!(driver.retry_waits.load(Ordering::SeqCst), 1);
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
            .run(RunCore::new(), admitted_user(1), control_rx, events_tx)
            .await
    });
    driver.retry_waiting.notified().await;
    control_tx
        .send(RunControl::Command(admitted_user(2)))
        .await
        .expect("retry steer");
    let completion = completion.await.expect("worker join");
    assert_completed(completion);
    let mut events = Vec::new();
    while let Some(event) = events_rx.recv().await {
        events.push(event);
    }
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
            .all(|message| matches!(message, PublicMessage::User(_)))
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
    assert_completed(completion);
    assert_eq!(driver.started_contexts.lock().expect("contexts").len(), 2);
    assert!(driver.tool_order.lock().expect("tool order").is_empty());
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, AgentEvent::ToolExecutionStart { .. }))
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, AgentEvent::ToolExecutionEnd { is_error: true, .. }))
            .count(),
        2
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
    for (index, event) in events.iter().enumerate() {
        if matches!(event, AgentEvent::ToolExecutionEnd { .. }) {
            assert!(matches!(
                events.get(index + 1),
                Some(AgentEvent::MessageStart { message, .. })
                    if matches!(message.as_ref(), PublicMessage::ToolResult(result) if result.is_error)
            ));
        }
    }
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
    let (completion, _) = run_fixture(driver.clone()).await;
    assert_completed(completion);
    assert_eq!(
        *driver.tool_order.lock().expect("tool order"),
        vec!["a", "b"]
    );
    assert_eq!(driver.max_active_tools.load(Ordering::SeqCst), 1);
    assert_eq!(driver.started_contexts.lock().expect("contexts").len(), 2);
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
    let (events_tx, mut events_rx) = mpsc::channel(256);
    let first = first_worker
        .run(RunCore::new(), admitted_user(1), control_rx, events_tx)
        .await;
    while events_rx.recv().await.is_some() {}
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
    let (events_tx, mut events_rx) = mpsc::channel(256);
    let second = second_worker
        .run(core, admitted_user(4), control_rx, events_tx)
        .await;
    while events_rx.recv().await.is_some() {}
    assert_completed(second);
    let contexts = second_driver.started_contexts.lock().expect("contexts");
    assert!(matches!(
        &contexts[0][1],
        PublicMessage::User(user)
            if matches!(&user.content[0], UserContent::Text { text } if text == "message 2")
    ));
    assert!(matches!(
        &contexts[1][3],
        PublicMessage::User(user)
            if matches!(&user.content[0], UserContent::Text { text } if text == "message 3")
    ));
    assert!(matches!(
        &contexts[2][5],
        PublicMessage::User(user)
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
    let completion = worker
        .run(RunCore::new(), admitted_user(1), control_rx, events_tx)
        .await;
    while events_rx.recv().await.is_some() {}
    let core = recovered_core(completion);

    let blocked_driver = Arc::new(FixtureDriver::new(vec![output(assistant(
        StopReason::Stop,
        Vec::new(),
        None,
        None,
    ))]));
    let blocked_worker = SequentialRunWorker::new(blocked_driver);
    let (_control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, mut events_rx) = mpsc::channel(8);
    let blocked = blocked_worker
        .run(core, admitted_user(5), control_rx, events_tx)
        .await;
    while events_rx.recv().await.is_some() {}
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
    let completion = worker
        .run(RunCore::new(), admitted_user(1), control_rx, events_tx)
        .await;
    while events_rx.recv().await.is_some() {}
    let mut core = recovered_core(completion);
    assert_eq!(pending_sequences(&mut core), vec![2, 3]);
    assert_eq!(
        core.runtime_context
            .iter()
            .filter(|message| matches!(message, PublicMessage::User(_)))
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
        .run(RunCore::new(), admitted_user(1), control_rx, events_tx)
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
        (2..=33).collect::<Vec<_>>(),
        "all 32 accepted controls fit the 33-command admission-total bound"
    );
}
