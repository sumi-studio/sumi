use std::{
    future::{Future, pending},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{
    gateway::{CommandAck, CommandId},
    provider::types::{
        ApiProtocol, AssistantContent, AssistantMessage, ContextMessage, ProviderContextFragment,
        ProviderContextPayload, ProviderEvent, ProviderEventStream, ProviderOrigin, ProviderOutput,
        PublicAssistantContent, PublicAssistantMessage, PublicMessage, RejectedToolCall,
        StopReason, ToolArgumentError, ToolCall, ToolResultMessage, Usage, UserContent,
        UserMessage, ValidatedToolArguments,
    },
    runtime::contracts::{MAX_PROCESS_GENERATION, ProcessGeneration},
    store::{Store, user_message_id},
};

fn test_executor_generation() -> ProcessGeneration {
    ProcessGeneration::from_wire(73).expect("valid test generation")
}

#[test]
fn session_start_composition_boundary_requires_process_generation() {
    fn assert_signature<Future>(
        _start: fn(Store, MockGateway, RunCore, Arc<dyn RunWorker>, ProcessGeneration) -> Future,
    ) {
    }

    assert_signature(Session::<MockGateway>::start);
}

fn persisted_runtime_context(messages: Vec<PublicMessage>) -> Vec<ContextMessage> {
    messages
        .into_iter()
        .enumerate()
        .map(|(index, message)| ContextMessage::Persisted {
            id: format!("session-fixture-{index}"),
            seq: index as u64 + 1,
            message: super::run::public_to_message(message),
        })
        .collect()
}

struct MockGateway {
    commands: mpsc::Receiver<InboundCommand>,
    frames: Arc<Mutex<Vec<OutboundFrame>>>,
    next_failure: Option<mpsc::Receiver<()>>,
    fail_send: Arc<AtomicBool>,
    send_failure_observed: Arc<Notify>,
}

impl Gateway for MockGateway {
    type Reader = MockGatewayReader;
    type Writer = MockGatewayWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        (
            MockGatewayReader {
                commands: self.commands,
                next_failure: self.next_failure,
            },
            MockGatewayWriter {
                frames: self.frames,
                fail_send: self.fail_send,
                send_failure_observed: self.send_failure_observed,
            },
        )
    }
}

struct MockGatewayReader {
    commands: mpsc::Receiver<InboundCommand>,
    next_failure: Option<mpsc::Receiver<()>>,
}

#[async_trait]
impl GatewayReader for MockGatewayReader {
    async fn next_command(&mut self) -> Result<InboundCommand> {
        if let Some(failures) = self.next_failure.as_mut() {
            tokio::select! {
                biased;
                Some(()) = failures.recv() => Err(anyhow!("fixture gateway receive failure")),
                command = self.commands.recv() => command.ok_or_else(|| GatewayClosed.into()),
            }
        } else {
            self.commands
                .recv()
                .await
                .ok_or_else(|| GatewayClosed.into())
        }
    }
}

struct MockGatewayWriter {
    frames: Arc<Mutex<Vec<OutboundFrame>>>,
    fail_send: Arc<AtomicBool>,
    send_failure_observed: Arc<Notify>,
}

#[async_trait]
impl GatewayWriter for MockGatewayWriter {
    async fn send(&mut self, frame: OutboundFrame) -> Result<()> {
        if self.fail_send.load(Ordering::SeqCst) {
            self.send_failure_observed.notify_one();
            return Err(anyhow!("fixture gateway send failure"));
        }
        self.frames.lock().expect("frame mutex").push(frame);
        Ok(())
    }
}

fn gateway() -> (
    MockGateway,
    mpsc::Sender<InboundCommand>,
    Arc<Mutex<Vec<OutboundFrame>>>,
) {
    let (commands_tx, commands) = mpsc::channel(40);
    let frames = Arc::new(Mutex::new(Vec::new()));
    let fail_send = Arc::new(AtomicBool::new(false));
    let send_failure_observed = Arc::new(Notify::new());
    (
        MockGateway {
            commands,
            frames: frames.clone(),
            next_failure: None,
            fail_send,
            send_failure_observed,
        },
        commands_tx,
        frames,
    )
}

struct ControlledGateway {
    gateway: MockGateway,
    commands: mpsc::Sender<InboundCommand>,
    next_failure: mpsc::Sender<()>,
    fail_send: Arc<AtomicBool>,
    send_failure_observed: Arc<Notify>,
}

fn controlled_gateway() -> ControlledGateway {
    let (commands_tx, commands) = mpsc::channel(40);
    let (next_failure_tx, next_failure) = mpsc::channel(1);
    let frames = Arc::new(Mutex::new(Vec::new()));
    let fail_send = Arc::new(AtomicBool::new(false));
    let send_failure_observed = Arc::new(Notify::new());
    ControlledGateway {
        gateway: MockGateway {
            commands,
            frames: frames.clone(),
            next_failure: Some(next_failure),
            fail_send: fail_send.clone(),
            send_failure_observed: send_failure_observed.clone(),
        },
        commands: commands_tx,
        next_failure: next_failure_tx,
        fail_send,
        send_failure_observed,
    }
}

struct FailFirstEventGateway {
    commands: mpsc::Receiver<InboundCommand>,
    event_attempts: Arc<AtomicUsize>,
    attempted: Arc<Notify>,
}

impl Gateway for FailFirstEventGateway {
    type Reader = SimpleReader;
    type Writer = FailFirstEventWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        (
            SimpleReader(self.commands),
            FailFirstEventWriter {
                event_attempts: self.event_attempts,
                attempted: self.attempted,
            },
        )
    }
}

struct SimpleReader(mpsc::Receiver<InboundCommand>);

#[async_trait]
impl GatewayReader for SimpleReader {
    async fn next_command(&mut self) -> Result<InboundCommand> {
        self.0.recv().await.ok_or_else(|| GatewayClosed.into())
    }
}

struct FailFirstEventWriter {
    event_attempts: Arc<AtomicUsize>,
    attempted: Arc<Notify>,
}

#[async_trait]
impl GatewayWriter for FailFirstEventWriter {
    async fn send(&mut self, frame: OutboundFrame) -> Result<()> {
        if matches!(frame, OutboundFrame::Event { .. })
            && self.event_attempts.fetch_add(1, Ordering::SeqCst) == 0
        {
            self.attempted.notify_one();
            return Err(anyhow!("fixture first event delivery failure"));
        }
        Ok(())
    }
}

struct ShutdownDrainGateway {
    commands: mpsc::Receiver<InboundCommand>,
    release_worker: Arc<Notify>,
    worker_ready: Arc<Notify>,
    failed_event: bool,
}

struct BlockingWriterGateway {
    commands: mpsc::Receiver<InboundCommand>,
    frames: Arc<Mutex<Vec<OutboundFrame>>>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl Gateway for BlockingWriterGateway {
    type Reader = SimpleReader;
    type Writer = BlockingWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        (
            SimpleReader(self.commands),
            BlockingWriter {
                frames: self.frames,
                entered: self.entered,
                release: self.release,
                blocked_once: false,
            },
        )
    }
}

struct BlockingWriter {
    frames: Arc<Mutex<Vec<OutboundFrame>>>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    blocked_once: bool,
}

#[async_trait]
impl GatewayWriter for BlockingWriter {
    async fn send(&mut self, frame: OutboundFrame) -> Result<()> {
        if !self.blocked_once {
            self.blocked_once = true;
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.frames.lock().expect("frames").push(frame);
        Ok(())
    }
}

struct EofBlockingGateway {
    commands: mpsc::Receiver<InboundCommand>,
    idle: Arc<Notify>,
    writer_entered: Arc<Notify>,
    writer_dropped: Arc<Notify>,
}

impl Gateway for EofBlockingGateway {
    type Reader = EofBlockingReader;
    type Writer = EofBlockingWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        (
            EofBlockingReader {
                commands: self.commands,
                idle: self.idle,
                polls: 0,
            },
            EofBlockingWriter {
                entered: self.writer_entered,
                dropped: self.writer_dropped,
                blocked_once: false,
            },
        )
    }
}

struct EofBlockingReader {
    commands: mpsc::Receiver<InboundCommand>,
    idle: Arc<Notify>,
    polls: usize,
}

#[async_trait]
impl GatewayReader for EofBlockingReader {
    async fn next_command(&mut self) -> Result<InboundCommand> {
        self.polls += 1;
        if self.polls == 2 {
            self.idle.notify_one();
        }
        self.commands
            .recv()
            .await
            .ok_or_else(|| GatewayClosed.into())
    }
}

struct EofBlockingWriter {
    entered: Arc<Notify>,
    dropped: Arc<Notify>,
    blocked_once: bool,
}

struct DropNotifier(Arc<Notify>);

impl Drop for DropNotifier {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

impl Drop for EofBlockingWriter {
    fn drop(&mut self) {
        self.dropped.notify_one();
    }
}

#[async_trait]
impl GatewayWriter for EofBlockingWriter {
    async fn send(&mut self, _frame: OutboundFrame) -> Result<()> {
        if !self.blocked_once {
            self.blocked_once = true;
            self.entered.notify_one();
            pending::<()>().await;
        }
        Ok(())
    }
}

impl Gateway for ShutdownDrainGateway {
    type Reader = SimpleReader;
    type Writer = ShutdownDrainWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        (
            SimpleReader(self.commands),
            ShutdownDrainWriter {
                release_worker: self.release_worker,
                worker_ready: self.worker_ready,
                failed_event: self.failed_event,
            },
        )
    }
}

struct ShutdownDrainWriter {
    release_worker: Arc<Notify>,
    worker_ready: Arc<Notify>,
    failed_event: bool,
}

#[async_trait]
impl GatewayWriter for ShutdownDrainWriter {
    async fn send(&mut self, frame: OutboundFrame) -> Result<()> {
        if matches!(frame, OutboundFrame::Event { .. }) && !self.failed_event {
            self.failed_event = true;
            self.release_worker.notify_one();
            self.worker_ready.notified().await;
            tokio::task::yield_now().await;
            return Err(anyhow!("fixture gateway send failure racing completion"));
        }
        Ok(())
    }
}

fn completed(result: SessionResult) -> RunCore {
    match result {
        SessionResult::Completed(core) => core,
        SessionResult::Failed { failure, ownership } => {
            panic!("expected clean completion, got {failure:?} with {ownership:?}")
        }
    }
}

fn failed(result: SessionResult) -> (SessionFailure, RunOwnership) {
    match result {
        SessionResult::Failed { failure, ownership } => (failure, ownership),
        SessionResult::Completed(core) => {
            panic!("expected failure, got core {}", core.ownership_id())
        }
    }
}

fn user(seq: u64) -> InboundCommand {
    let command_id = format!("00000000-0000-4000-8000-{seq:012}");
    InboundCommand::Valid(CommandEnvelope {
        seq,
        command_id: CommandId::parse(&command_id).expect("canonical command id"),
        command: Command::UserMessage {
            text: format!("message {seq}"),
            attachments: Vec::new(),
        },
    })
}

fn abort(seq: u64) -> InboundCommand {
    let command_id = format!("10000000-0000-4000-8000-{seq:012}");
    InboundCommand::Valid(CommandEnvelope {
        seq,
        command_id: CommandId::parse(&command_id).expect("canonical command id"),
        command: Command::Abort {},
    })
}

async fn emit_idle_injection(events: &mpsc::Sender<AgentEvent>, initial: &AdmittedCommand) {
    let Command::UserMessage { text, .. } = &initial.envelope().command else {
        panic!("idle fixture requires user command")
    };
    let message = PublicMessage::User(UserMessage {
        content: vec![UserContent::Text { text: text.clone() }],
        timestamp: initial.received_at(),
    });
    let message_id = user_message_id(&initial.envelope().command_id);
    for event in [
        AgentEvent::AgentStart,
        AgentEvent::TurnStart,
        AgentEvent::MessageStart {
            message_id: message_id.clone(),
            message: Box::new(message.clone()),
        },
        AgentEvent::MessageEnd {
            message_id,
            message: Box::new(message),
        },
    ] {
        events.send(event).await.expect("event receiver");
    }
}

fn received_acks(frames: &Arc<Mutex<Vec<OutboundFrame>>>) -> Vec<CommandAck> {
    frames
        .lock()
        .expect("frame mutex")
        .iter()
        .filter_map(|frame| match frame {
            OutboundFrame::CommandAck { ack } if ack.status == CommandAckStatus::Received => {
                Some(ack.clone())
            }
            _ => None,
        })
        .collect()
}

async fn session(gateway: MockGateway, worker: Arc<dyn RunWorker>) -> Session<MockGateway> {
    session_with_core(gateway, worker, RunCore::new()).await
}

async fn session_with_core(
    gateway: MockGateway,
    worker: Arc<dyn RunWorker>,
    core: RunCore,
) -> Session<MockGateway> {
    Session::start(
        Store::session_test_store("session-actor-test")
            .await
            .expect("test store"),
        gateway,
        core,
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session startup")
}

async fn finish_active(session: &mut Session<MockGateway>) {
    let completion = {
        let active = session.active.as_mut().expect("active worker");
        (&mut active.completion_rx).await
    };
    session
        .finish_run(completion)
        .await
        .expect("worker completion");
}

async fn drive_active_to_completion<G: Gateway>(
    session: &mut Session<G>,
) -> Result<(), SessionFailure> {
    loop {
        let completion = session
            .active
            .as_mut()
            .expect("active worker")
            .completion_rx
            .try_recv();
        match completion {
            Ok(completion) => return session.finish_run(Ok(completion)).await,
            Err(oneshot::error::TryRecvError::Closed) => {
                return session.resolve_closed_event_channel().await;
            }
            Err(oneshot::error::TryRecvError::Empty) => {}
        }
        let output = session
            .active
            .as_mut()
            .expect("active worker")
            .events_rx
            .recv()
            .await;
        match output {
            Some(output) => session.persist_active_event(output).await?,
            None => session.resolve_closed_event_channel().await?,
        }
    }
}

#[derive(Clone, Copy)]
enum StaleBinding {
    PriorRun,
    PriorTurn,
    ExecutorGeneration,
}

struct StaleBindingWorker(StaleBinding);

impl RunWorker for StaleBindingWorker {
    fn run(
        &self,
        core: RunCore,
        initial: AdmittedCommand,
        _controls: mpsc::Receiver<RunControl>,
        events: mpsc::Sender<RunOutput>,
    ) -> WorkerFuture {
        let kind = self.0;
        Box::pin(async move {
            let mut binding = core.durable_binding.clone().expect("Session-bound RunCore");
            match kind {
                StaleBinding::PriorRun => {
                    binding.run_id = Uuid::now_v7().to_string();
                    events
                        .send(RunOutput::detached(binding, AgentEvent::AgentStart, None))
                        .await
                        .expect("session output receiver");
                }
                StaleBinding::PriorTurn => {
                    let user = PublicMessage::User(UserMessage {
                        content: vec![UserContent::Text {
                            text: "message 1".to_owned(),
                        }],
                        timestamp: initial.received_at(),
                    });
                    let assistant = bridge_assistant(StopReason::Stop);
                    let user_id = user_message_id(&initial.envelope().command_id);
                    for event in [
                        AgentEvent::AgentStart,
                        AgentEvent::TurnStart,
                        AgentEvent::MessageStart {
                            message_id: user_id.clone(),
                            message: Box::new(user.clone()),
                        },
                        AgentEvent::MessageEnd {
                            message_id: user_id,
                            message: Box::new(user),
                        },
                        AgentEvent::MessageStart {
                            message_id: "stale-turn-first".to_owned(),
                            message: Box::new(assistant.clone()),
                        },
                        AgentEvent::MessageEnd {
                            message_id: "stale-turn-first".to_owned(),
                            message: Box::new(assistant.clone()),
                        },
                        AgentEvent::TurnEnd {
                            message: Some(Box::new(assistant)),
                            tool_results: Vec::new(),
                        },
                    ] {
                        events
                            .send(RunOutput::detached(binding.clone(), event, None))
                            .await
                            .expect("session output receiver");
                    }
                    let stale = binding.clone();
                    binding.turn_id = Uuid::now_v7().to_string();
                    events
                        .send(RunOutput::detached(binding, AgentEvent::TurnStart, None))
                        .await
                        .expect("new turn output");
                    events
                        .send(RunOutput::detached(
                            stale,
                            AgentEvent::MessageStart {
                                message_id: "stale-prior-turn-output".to_owned(),
                                message: Box::new(bridge_assistant(StopReason::Stop)),
                            },
                            None,
                        ))
                        .await
                        .expect("stale turn output");
                }
                StaleBinding::ExecutorGeneration => {
                    binding.executor_generation =
                        ProcessGeneration::from_wire(binding.executor_generation.to_wire() + 1)
                            .expect("next test generation");
                    events
                        .send(RunOutput::detached(binding, AgentEvent::AgentStart, None))
                        .await
                        .expect("session output receiver");
                }
            }
            RunCompletion::Completed(core)
        })
    }
}

#[tokio::test]
async fn session_rejects_worker_output_with_any_changed_durable_binding() {
    for (label, kind) in [
        ("run", StaleBinding::PriorRun),
        ("turn", StaleBinding::PriorTurn),
        ("executor-generation", StaleBinding::ExecutorGeneration),
    ] {
        let store = Store::session_test_store(&format!("stale-worker-{label}"))
            .await
            .expect("test store");
        let pool = store.pool().clone();
        let (gateway, commands, frames) = gateway();
        let session = Session::start(
            store,
            gateway,
            RunCore::new(),
            Arc::new(StaleBindingWorker(kind)),
            test_executor_generation(),
        )
        .await
        .expect("session");
        let task = tokio::spawn(session.run());
        commands.send(user(1)).await.expect("command");
        let (failure, ownership) = failed(task.await.expect("session join"));
        assert!(matches!(failure, SessionFailure::Other(_)));
        assert!(matches!(ownership, RunOwnership::Lost));
        let durable_events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(&pool)
            .await
            .expect("event count");
        let expected_prefix = match kind {
            StaleBinding::PriorRun | StaleBinding::ExecutorGeneration => 0,
            StaleBinding::PriorTurn => 8,
        };
        assert_eq!(
            durable_events, expected_prefix,
            "stale {label} output itself must not persist"
        );
        let public_events = frames
            .lock()
            .expect("frame mutex")
            .iter()
            .filter(|frame| matches!(frame, OutboundFrame::Event { .. }))
            .count();
        assert!(
            public_events <= expected_prefix as usize,
            "writer shutdown may leave a committed replayable prefix undelivered"
        );
        assert!(frames.lock().expect("frame mutex").iter().all(|frame| {
            !matches!(frame, OutboundFrame::Event { envelope }
                if envelope.event.get("message_id").and_then(serde_json::Value::as_str)
                    == Some("stale-prior-turn-output"))
        }));
    }
}

#[tokio::test]
async fn active_received_replay_acks_without_duplicate_control_delivery() {
    let (gateway, _commands, frames) = gateway();
    let starts = Arc::new(AtomicUsize::new(0));
    let (release_tx, release_rx) = oneshot::channel();
    let release = Arc::new(Mutex::new(Some(release_rx)));
    let worker: Arc<dyn RunWorker> = Arc::new({
        let starts = starts.clone();
        move |core: RunCore,
              _initial: AdmittedCommand,
              mut controls: mpsc::Receiver<RunControl>,
              events: mpsc::Sender<AgentEvent>| {
            starts.fetch_add(1, Ordering::SeqCst);
            let release = release
                .lock()
                .expect("release mutex")
                .take()
                .expect("single worker");
            async move {
                let _events = events;
                release.await.expect("release worker");
                controls.close();
                RunCompletion::Completed(core)
            }
        }
    });
    let mut session = session(gateway, worker).await;

    session
        .admit_and_route(user(1))
        .await
        .expect("fresh receipt");
    assert_eq!(
        session
            .active
            .as_ref()
            .expect("active worker")
            .control_tx
            .capacity(),
        CONTROL_CHANNEL_CAPACITY
    );
    session
        .admit_and_route(user(1))
        .await
        .expect("exact replay");
    session.wait_outbound_idle().await;

    assert_eq!(received_acks(&frames).len(), 2, "fresh and stored ACK");
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(
        session
            .active
            .as_ref()
            .expect("same active worker")
            .control_tx
            .capacity(),
        CONTROL_CHANNEL_CAPACITY,
        "replay must not occupy a control channel slot"
    );
    release_tx.send(()).expect("release worker");
    finish_active(&mut session).await;
}

#[tokio::test]
async fn idle_received_replay_acks_without_spawning_a_second_worker() {
    let (gateway, _commands, frames) = gateway();
    let starts = Arc::new(AtomicUsize::new(0));
    let worker: Arc<dyn RunWorker> = Arc::new({
        let starts = starts.clone();
        move |core: RunCore,
              _initial: AdmittedCommand,
              mut controls: mpsc::Receiver<RunControl>,
              events: mpsc::Sender<AgentEvent>| {
            starts.fetch_add(1, Ordering::SeqCst);
            async move {
                let _events = events;
                controls.close();
                RunCompletion::Completed(core)
            }
        }
    });
    let mut session = session(gateway, worker).await;

    session
        .admit_and_route(user(1))
        .await
        .expect("fresh receipt");
    finish_active(&mut session).await;
    assert!(session.active.is_none());
    session
        .admit_and_route(user(1))
        .await
        .expect("idle exact replay");
    session.wait_outbound_idle().await;

    assert_eq!(received_acks(&frames).len(), 2, "fresh and stored ACK");
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert!(session.active.is_none(), "replay must not spawn a worker");
}

#[tokio::test]
async fn pending_worker_does_not_block_deferred_control_received_ack() {
    let (gateway, _commands, frames) = gateway();
    let worker: Arc<dyn RunWorker> = Arc::new(
        |_core: RunCore,
         _initial: AdmittedCommand,
         _controls: mpsc::Receiver<RunControl>,
         _events: mpsc::Sender<AgentEvent>| async move { pending::<RunCompletion>().await },
    );
    let mut session = session(gateway, worker).await;

    session
        .admit_and_route(user(1))
        .await
        .expect("first command");
    session
        .admit_and_route(abort(2))
        .await
        .expect("second command");
    session.wait_outbound_idle().await;
    assert_eq!(
        received_acks(&frames)
            .into_iter()
            .map(|ack| ack.seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(session.deferred_commands.len(), 1);
    assert_eq!(
        session
            .deferred_commands
            .front()
            .expect("deferred Abort")
            .envelope()
            .seq,
        2
    );
    assert_eq!(
        session
            .active
            .as_ref()
            .expect("active worker")
            .control_tx
            .capacity(),
        CONTROL_CHANNEL_CAPACITY,
        "T15 must not send an unimplemented active Abort to the worker"
    );
    session.shutdown_active().await;
}

#[tokio::test]
async fn active_session_uses_durable_backpressure_before_its_bounded_fifo_can_overflow() {
    let store = Store::session_test_store("active-bounded-fifo-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (gateway, _commands, frames) = gateway();
    let worker: Arc<dyn RunWorker> = Arc::new(
        |_core: RunCore,
         _initial: AdmittedCommand,
         _controls: mpsc::Receiver<RunControl>,
         _events: mpsc::Sender<AgentEvent>| async move { pending::<RunCompletion>().await },
    );
    let mut session = Session::start(
        store,
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session");

    session
        .admit_and_route(user(1))
        .await
        .expect("active owner");
    for seq in 2..=32 {
        session
            .admit_and_route(user(seq))
            .await
            .expect("31 deferred ordinary commands fit beside the active owner");
    }
    assert_eq!(session.deferred_commands.len(), 31);

    let overflow = session
        .admit_and_route(user(33))
        .await
        .expect_err("33rd live ordinary command must backpressure before persistence");
    assert!(overflow.to_string().contains("admission window is full"));
    assert_eq!(session.deferred_commands.len(), 31);

    session
        .admit_and_route(abort(33))
        .await
        .expect("reserved Abort remains admissible at the ordinary limit");
    assert_eq!(session.deferred_commands.len(), 32);
    assert_eq!(
        session
            .deferred_commands
            .iter()
            .map(|command| command.envelope().seq)
            .collect::<Vec<_>>(),
        (2..=33).collect::<Vec<_>>()
    );
    assert!(matches!(
        session
            .deferred_commands
            .front()
            .expect("oldest deferred command")
            .envelope()
            .command,
        Command::UserMessage { .. }
    ));

    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT seq, command_id, status FROM inbound_commands ORDER BY seq, command_id",
    )
    .fetch_all(&pool)
    .await
    .expect("bounded durable window");
    assert_eq!(rows.len(), 33);
    assert!(
        !rows
            .iter()
            .any(|(_, command_id, _)| { command_id == "00000000-0000-4000-8000-000000000033" })
    );
    assert!(rows.iter().any(|(seq, command_id, status)| {
        *seq == 33 && command_id == "10000000-0000-4000-8000-000000000033" && status == "received"
    }));
    session.wait_outbound_idle().await;
    assert_eq!(received_acks(&frames).len(), 33);
    session.shutdown_active().await;
}

#[tokio::test]
async fn active_session_keeps_early_reserved_abort_and_remaining_ordinary_window_fifo_bounded() {
    let store = Store::session_test_store("active-early-abort-bounded-fifo-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (gateway, _commands, _frames) = gateway();
    let worker: Arc<dyn RunWorker> = Arc::new(
        |_core: RunCore,
         _initial: AdmittedCommand,
         _controls: mpsc::Receiver<RunControl>,
         _events: mpsc::Sender<AgentEvent>| async move { pending::<RunCompletion>().await },
    );
    let mut session = Session::start(
        store,
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session");

    session
        .admit_and_route(user(1))
        .await
        .expect("active owner");
    session
        .admit_and_route(abort(2))
        .await
        .expect("early reserved Abort");
    for seq in 3..=33 {
        session
            .admit_and_route(user(seq))
            .await
            .expect("remaining 31 ordinary commands fit beside owner and Abort");
    }
    assert_eq!(session.deferred_commands.len(), 32);
    assert_eq!(
        session
            .deferred_commands
            .iter()
            .map(|command| command.envelope().seq)
            .collect::<Vec<_>>(),
        (2..=33).collect::<Vec<_>>()
    );
    assert!(matches!(
        session
            .deferred_commands
            .front()
            .expect("reserved Abort remains at FIFO head")
            .envelope()
            .command,
        Command::Abort {}
    ));

    let overflow = session
        .admit_and_route(user(34))
        .await
        .expect_err("next ordinary command must backpressure before persistence");
    assert!(overflow.to_string().contains("admission window is full"));
    let persisted: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inbound_commands")
        .fetch_one(&pool)
        .await
        .expect("durable admission count");
    assert_eq!(
        persisted, 33,
        "overflow command was not assigned durable state"
    );
    assert_eq!(session.deferred_commands.len(), 32);
    session.shutdown_active().await;
}

#[tokio::test]
async fn ready_completion_event_and_next_command_all_progress_with_one_core() {
    let (gateway, commands, frames) = gateway();
    let starts = Arc::new(AtomicUsize::new(0));
    let ownership = Arc::new(Mutex::new(Vec::new()));
    let first_release = Arc::new(Notify::new());
    let (started_tx, mut started_rx) = mpsc::channel(2);
    let worker: Arc<dyn RunWorker> = Arc::new({
        let starts = starts.clone();
        let ownership = ownership.clone();
        let first_release = first_release.clone();
        move |mut core: RunCore,
              initial: AdmittedCommand,
              mut controls: mpsc::Receiver<RunControl>,
              events: mpsc::Sender<AgentEvent>| {
            let invocation = starts.fetch_add(1, Ordering::SeqCst);
            let ownership = ownership.clone();
            let first_release = first_release.clone();
            let started_tx = started_tx.clone();
            async move {
                ownership
                    .lock()
                    .expect("ownership mutex")
                    .push(core.ownership_id());
                started_tx
                    .send(initial.envelope().seq)
                    .await
                    .expect("start observer");
                if invocation == 0 {
                    first_release.notified().await;
                    events
                        .try_send(AgentEvent::AgentStart)
                        .expect("bounded event");
                }
                controls.close();
                while let Ok(RunControl::Command(command)) = controls.try_recv() {
                    core.queue_followup(command).expect("bounded follow-up");
                }
                core.mark_mutated();
                RunCompletion::Completed(core)
            }
        }
    });
    let task = tokio::spawn(session(gateway, worker).await.run());

    commands.send(user(1)).await.expect("first command");
    assert_eq!(started_rx.recv().await, Some(1));
    first_release.notify_one();
    commands.send(user(2)).await.expect("simultaneous command");
    assert_eq!(started_rx.recv().await, Some(2));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if received_acks(&frames).len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("writer observed both Received ACKs");
    drop(commands);

    let core = completed(task.await.expect("session join"));
    assert_eq!(
        starts.load(Ordering::SeqCst),
        2,
        "never more than one worker at a time"
    );
    assert_eq!(core.mutation_epoch(), 2);
    let ids = ownership.lock().expect("ownership mutex");
    assert_eq!(ids.len(), 2);
    assert_eq!(ids[0], ids[1], "the same non-cloned RunCore moved twice");
    assert_eq!(received_acks(&frames).len(), 2);
    assert!(
        !frames
            .lock()
            .expect("frame mutex")
            .iter()
            .any(|frame| { matches!(frame, OutboundFrame::Event { .. }) }),
        "an incomplete startup must not publish a partial AgentStart"
    );
}

#[tokio::test]
async fn typed_worker_failures_report_recovered_ownership() {
    for failure in [
        WorkerFailure::Error("fixture error".to_owned()),
        WorkerFailure::Cancelled,
        WorkerFailure::EventChannelClosed,
    ] {
        let (gateway, commands, _frames) = gateway();
        let expected = failure.clone();
        let worker: Arc<dyn RunWorker> = Arc::new(
            move |core: RunCore,
                  _initial: AdmittedCommand,
                  mut controls: mpsc::Receiver<RunControl>,
                  events: mpsc::Sender<AgentEvent>| {
                let failure = failure.clone();
                async move {
                    let _events = events;
                    controls.close();
                    RunCompletion::Failed { core, failure }
                }
            },
        );
        let core = RunCore::new();
        let ownership_id = core.ownership_id();
        let task = tokio::spawn(session_with_core(gateway, worker, core).await.run());
        commands.send(user(1)).await.expect("command");
        let (error, ownership) = failed(task.await.expect("session join"));
        let RunOwnership::Recovered(core) = ownership else {
            panic!("recoverable worker failure lost RunCore");
        };
        assert_eq!(core.ownership_id(), ownership_id);
        assert_eq!(core.mutation_epoch(), 0);
        assert!(matches!(error, SessionFailure::Worker(failure) if failure == expected));
    }
}

struct RunningGuard(Arc<AtomicBool>);

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn fixture_adapter_event_loss_drains_bounded_lane_and_returns_the_actual_core() {
    let worker: Arc<dyn RunWorker> = Arc::new(
        |mut core: RunCore,
         _initial: AdmittedCommand,
         _controls: mpsc::Receiver<RunControl>,
         events: mpsc::Sender<AgentEvent>| async move {
            core.mark_mutated();
            for ordinal in 0..(EVENT_CHANNEL_CAPACITY + 8) {
                events
                    .send(AgentEvent::Error {
                        message: format!("fixture event {ordinal}"),
                    })
                    .await
                    .expect("adapter must drain after outer delivery loss");
            }
            RunCompletion::Completed(core)
        },
    );
    let InboundCommand::Valid(envelope) = user(1) else {
        unreachable!()
    };
    let initial = AdmittedCommand::new(envelope, Utc::now());
    let mut core = RunCore::new();
    let ownership_id = core.ownership_id();
    core.durable_binding = Some(DurableRunBinding::idle(
        &initial,
        test_executor_generation(),
    ));
    let (_control_tx, control_rx) = mpsc::channel(1);
    let (events_tx, events_rx) = mpsc::channel(1);
    drop(events_rx);

    let completion = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        worker.run(core, initial, control_rx, events_tx),
    )
    .await
    .expect("event-loss adapter completion is bounded");
    let RunCompletion::Failed { core, failure } = completion else {
        panic!("outer event loss must fail with the actual core")
    };
    assert_eq!(failure, WorkerFailure::EventChannelClosed);
    assert_eq!(core.ownership_id(), ownership_id);
    assert_eq!(core.mutation_epoch(), 1);
}

#[tokio::test]
async fn active_gateway_receive_failure_aborts_and_awaits_worker() {
    let ControlledGateway {
        gateway,
        commands,
        next_failure,
        ..
    } = controlled_gateway();
    let running = Arc::new(AtomicBool::new(false));
    let (started_tx, mut started_rx) = mpsc::channel(1);
    let worker: Arc<dyn RunWorker> = Arc::new({
        let running = running.clone();
        move |_core: RunCore,
              _initial: AdmittedCommand,
              _controls: mpsc::Receiver<RunControl>,
              events: mpsc::Sender<AgentEvent>| {
            let running = running.clone();
            let started_tx = started_tx.clone();
            async move {
                running.store(true, Ordering::SeqCst);
                let _guard = RunningGuard(running);
                let _events = events;
                started_tx.send(()).await.expect("start observer");
                pending::<RunCompletion>().await
            }
        }
    });
    let task = tokio::spawn(session(gateway, worker).await.run());
    commands.send(user(1)).await.expect("initial command");
    started_rx.recv().await.expect("worker started");
    next_failure
        .send(())
        .await
        .expect("receive failure trigger");

    let (failure, ownership) = failed(task.await.expect("session join"));
    assert!(matches!(
        failure,
        SessionFailure::Gateway {
            operation: "receive",
            ..
        }
    ));
    assert!(matches!(ownership, RunOwnership::Lost));
    assert!(
        !running.load(Ordering::SeqCst),
        "worker must be joined before return"
    );
}

#[tokio::test]
async fn active_gateway_send_failure_aborts_and_awaits_worker() {
    let ControlledGateway {
        gateway,
        commands,
        fail_send,
        ..
    } = controlled_gateway();
    let running = Arc::new(AtomicBool::new(false));
    let emit = Arc::new(Notify::new());
    let (started_tx, mut started_rx) = mpsc::channel(1);
    let worker: Arc<dyn RunWorker> = Arc::new({
        let running = running.clone();
        let emit = emit.clone();
        move |_core: RunCore,
              initial: AdmittedCommand,
              _controls: mpsc::Receiver<RunControl>,
              events: mpsc::Sender<AgentEvent>| {
            let running = running.clone();
            let emit = emit.clone();
            let started_tx = started_tx.clone();
            async move {
                running.store(true, Ordering::SeqCst);
                let _guard = RunningGuard(running);
                started_tx.send(()).await.expect("start observer");
                emit.notified().await;
                emit_idle_injection(&events, &initial).await;
                pending::<RunCompletion>().await
            }
        }
    });
    let task = tokio::spawn(session(gateway, worker).await.run());
    commands.send(user(1)).await.expect("initial command");
    started_rx.recv().await.expect("worker started");
    fail_send.store(true, Ordering::SeqCst);
    emit.notify_one();

    let (failure, ownership) = failed(task.await.expect("session join"));
    assert!(matches!(
        failure,
        SessionFailure::Gateway {
            operation: "send",
            ..
        }
    ));
    assert!(matches!(ownership, RunOwnership::Lost));
    assert!(
        !running.load(Ordering::SeqCst),
        "worker must be joined before return"
    );
}

#[tokio::test]
async fn active_gateway_eof_after_writer_failure_preserves_send_error() {
    let ControlledGateway {
        gateway,
        commands,
        fail_send,
        send_failure_observed,
        ..
    } = controlled_gateway();
    let emit = Arc::new(Notify::new());
    let (started_tx, mut started_rx) = mpsc::channel(1);
    let worker: Arc<dyn RunWorker> = Arc::new({
        let emit = emit.clone();
        move |_core: RunCore,
              initial: AdmittedCommand,
              _controls: mpsc::Receiver<RunControl>,
              events: mpsc::Sender<AgentEvent>| {
            let emit = emit.clone();
            let started_tx = started_tx.clone();
            async move {
                started_tx.send(()).await.expect("start observer");
                emit.notified().await;
                emit_idle_injection(&events, &initial).await;
                pending::<RunCompletion>().await
            }
        }
    });
    let task = tokio::spawn(session(gateway, worker).await.run());
    commands.send(user(1)).await.expect("initial command");
    started_rx.recv().await.expect("worker started");
    fail_send.store(true, Ordering::SeqCst);
    emit.notify_one();

    // The failing send path has signalled. Let the writer task publish its
    // completion result, then close the reader so EOF and writer completion
    // are ready in the same active select without a scheduling sleep.
    send_failure_observed.notified().await;
    tokio::task::yield_now().await;
    drop(commands);

    let (failure, ownership) = failed(task.await.expect("session join"));
    assert!(matches!(
        failure,
        SessionFailure::Gateway {
            operation: "send",
            ref source,
        } if source.to_string() == "fixture gateway send failure"
    ));
    assert!(!matches!(failure, SessionFailure::GatewayClosedDuringRun));
    assert!(matches!(ownership, RunOwnership::Lost));
}

#[tokio::test]
async fn shutdown_drains_ready_completion_outputs_before_recovering_core_after_gateway_failure() {
    let store = Store::session_test_store("shutdown-ready-completion-drain-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (commands_tx, commands) = mpsc::channel(1);
    let release_worker = Arc::new(Notify::new());
    let worker_ready = Arc::new(Notify::new());
    let gateway = ShutdownDrainGateway {
        commands,
        release_worker: release_worker.clone(),
        worker_ready: worker_ready.clone(),
        failed_event: false,
    };
    let worker: Arc<dyn RunWorker> = Arc::new(
        move |mut core: RunCore,
              initial: AdmittedCommand,
              mut controls: mpsc::Receiver<RunControl>,
              events: mpsc::Sender<AgentEvent>| {
            let release_worker = release_worker.clone();
            let worker_ready = worker_ready.clone();
            async move {
                let Command::UserMessage { text, .. } = &initial.envelope().command else {
                    panic!("idle fixture requires user command")
                };
                let user_context = PublicMessage::User(UserMessage {
                    content: vec![UserContent::Text { text: text.clone() }],
                    timestamp: initial.received_at(),
                });
                emit_idle_injection(&events, &initial).await;
                release_worker.notified().await;
                let assistant = bridge_assistant(StopReason::Stop);
                for event in [
                    AgentEvent::MessageStart {
                        message_id: "shutdown-drain-assistant".to_owned(),
                        message: Box::new(assistant.clone()),
                    },
                    AgentEvent::MessageEnd {
                        message_id: "shutdown-drain-assistant".to_owned(),
                        message: Box::new(assistant.clone()),
                    },
                    AgentEvent::TurnEnd {
                        message: Some(Box::new(assistant.clone())),
                        tool_results: Vec::new(),
                    },
                    AgentEvent::AgentEnd,
                ] {
                    events.send(event).await.expect("session event receiver");
                }
                core.runtime_context = persisted_runtime_context(vec![user_context, assistant]);
                core.mark_mutated();
                controls.close();
                worker_ready.notify_one();
                RunCompletion::Completed(core)
            }
        },
    );
    let task = tokio::spawn(
        Session::start(
            store,
            gateway,
            RunCore::new(),
            worker,
            test_executor_generation(),
        )
        .await
        .expect("session")
        .run(),
    );
    commands_tx.send(user(1)).await.expect("initial command");

    let (failure, ownership) = failed(task.await.expect("session join"));
    assert!(matches!(
        failure,
        SessionFailure::Gateway {
            operation: "send",
            ..
        }
    ));
    let RunOwnership::Recovered(core) = ownership else {
        panic!("fully drained shutdown completion must remain recoverable")
    };
    assert_eq!(core.mutation_epoch(), 1);
    assert_eq!(core.runtime_context.len(), 2);
    let durable_messages: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
        .fetch_one(&pool)
        .await
        .expect("durable message count");
    assert_eq!(durable_messages, 2);
    let durable_tail: Vec<String> =
        sqlx::query_scalar("SELECT event_type FROM agent_events ORDER BY seq DESC LIMIT 2")
            .fetch_all(&pool)
            .await
            .expect("durable shutdown tail");
    assert_eq!(durable_tail, vec!["agent_end", "turn_end"]);
}

#[tokio::test]
async fn event_drain_send_failure_preserves_ready_completion_core() {
    let ControlledGateway {
        gateway,
        commands,
        fail_send,
        ..
    } = controlled_gateway();
    let release = Arc::new(Notify::new());
    let (started_tx, mut started_rx) = mpsc::channel(1);
    let core = RunCore::new();
    let ownership_id = core.ownership_id();
    let worker: Arc<dyn RunWorker> = Arc::new({
        let release = release.clone();
        move |core: RunCore,
              initial: AdmittedCommand,
              mut controls: mpsc::Receiver<RunControl>,
              events: mpsc::Sender<AgentEvent>| {
            let release = release.clone();
            let started_tx = started_tx.clone();
            async move {
                started_tx.send(()).await.expect("start observer");
                release.notified().await;
                emit_idle_injection(&events, &initial).await;
                controls.close();
                RunCompletion::Completed(core)
            }
        }
    });
    let task = tokio::spawn(session_with_core(gateway, worker, core).await.run());
    commands.send(user(1)).await.expect("initial command");
    started_rx.recv().await.expect("worker started");
    fail_send.store(true, Ordering::SeqCst);
    release.notify_one();

    let (failure, ownership) = failed(task.await.expect("session join"));
    assert!(matches!(
        failure,
        SessionFailure::Gateway {
            operation: "send",
            ..
        }
    ));
    let RunOwnership::Recovered(core) = ownership else {
        panic!("ready completion core was discarded during event drain");
    };
    assert_eq!(core.ownership_id(), ownership_id);
}

#[tokio::test]
async fn completion_drain_persists_all_outputs_before_recovering_mutated_core_after_delivery_loss()
{
    let store = Store::session_test_store("completion-drain-delivery-loss-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (commands_tx, commands) = mpsc::channel(1);
    let event_attempts = Arc::new(AtomicUsize::new(0));
    let attempted = Arc::new(Notify::new());
    let gateway = FailFirstEventGateway {
        commands,
        event_attempts: event_attempts.clone(),
        attempted: attempted.clone(),
    };
    let worker: Arc<dyn RunWorker> = Arc::new(
        |mut core: RunCore,
         initial: AdmittedCommand,
         mut controls: mpsc::Receiver<RunControl>,
         events: mpsc::Sender<AgentEvent>| async move {
            let Command::UserMessage { text, .. } = &initial.envelope().command else {
                panic!("idle fixture requires user command")
            };
            let user_context = PublicMessage::User(UserMessage {
                content: vec![UserContent::Text { text: text.clone() }],
                timestamp: initial.received_at(),
            });
            emit_idle_injection(&events, &initial).await;
            let assistant = bridge_assistant(StopReason::Stop);
            for event in [
                AgentEvent::MessageStart {
                    message_id: "completion-drain-assistant".to_owned(),
                    message: Box::new(assistant.clone()),
                },
                AgentEvent::MessageEnd {
                    message_id: "completion-drain-assistant".to_owned(),
                    message: Box::new(assistant.clone()),
                },
                AgentEvent::TurnEnd {
                    message: Some(Box::new(assistant.clone())),
                    tool_results: Vec::new(),
                },
                AgentEvent::AgentEnd,
            ] {
                events.send(event).await.expect("session event receiver");
            }
            core.runtime_context = persisted_runtime_context(vec![user_context, assistant]);
            core.mark_mutated();
            controls.close();
            RunCompletion::Completed(core)
        },
    );
    let session = Session::start(
        store,
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session");

    let send_attempted = attempted.notified();
    let task = tokio::spawn(session.run());
    commands_tx.send(user(1)).await.expect("start active run");
    send_attempted.await;
    drop(commands_tx);
    let (failure, ownership) = failed(task.await.expect("session join"));
    assert!(matches!(
        failure,
        SessionFailure::Gateway {
            operation: "send",
            ..
        }
    ));
    assert_eq!(event_attempts.load(Ordering::SeqCst), 1);

    let RunOwnership::Recovered(recovered) = ownership else {
        panic!("fully drained completed core remains recoverable")
    };
    assert_eq!(recovered.mutation_epoch(), 1);
    assert_eq!(recovered.runtime_context.len(), 2);
    let durable_messages: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
        .fetch_one(&pool)
        .await
        .expect("durable message count");
    assert_eq!(durable_messages, 2);
    let durable_tail: Vec<String> =
        sqlx::query_scalar("SELECT event_type FROM agent_events ORDER BY seq DESC LIMIT 2")
            .fetch_all(&pool)
            .await
            .expect("durable completion tail");
    assert_eq!(durable_tail, vec!["agent_end", "turn_end"]);
}

#[tokio::test]
async fn synchronous_worker_factory_panic_is_typed_and_lost() {
    let (gateway, commands, _frames) = gateway();
    let worker: Arc<dyn RunWorker> = Arc::new(
        |_core: RunCore,
         _initial: AdmittedCommand,
         _controls: mpsc::Receiver<RunControl>,
         _events: mpsc::Sender<AgentEvent>|
         -> std::future::Pending<RunCompletion> {
            panic!("synchronous factory panic fixture");
        },
    );
    let task = tokio::spawn(session(gateway, worker).await.run());
    commands.send(user(1)).await.expect("initial command");

    let (failure, ownership) = failed(task.await.expect("session join"));
    assert!(matches!(
        failure,
        SessionFailure::WorkerPanicked { ref message }
            if message.contains("synchronous factory panic fixture")
    ));
    assert!(matches!(ownership, RunOwnership::Lost));
}

#[tokio::test]
async fn panic_and_unpaired_event_channel_close_report_lost_ownership() {
    let (panic_gateway, commands, _frames) = gateway();
    let panic_worker: Arc<dyn RunWorker> = Arc::new(
        |_core: RunCore,
         _initial: AdmittedCommand,
         _controls: mpsc::Receiver<RunControl>,
         events: mpsc::Sender<AgentEvent>| async move {
            let _events = events;
            panic!("worker panic fixture");
        },
    );
    let task = tokio::spawn(session(panic_gateway, panic_worker).await.run());
    commands.send(user(1)).await.expect("command");
    let (panic_error, ownership) = failed(task.await.expect("session join"));
    assert!(matches!(ownership, RunOwnership::Lost));
    assert!(
        matches!(panic_error, SessionFailure::WorkerPanicked { .. }),
        "unexpected panic outcome: {panic_error:?}"
    );

    let (gateway, commands, _frames) = gateway();
    let close_worker: Arc<dyn RunWorker> = Arc::new(
        |_core: RunCore,
         _initial: AdmittedCommand,
         _controls: mpsc::Receiver<RunControl>,
         events: mpsc::Sender<AgentEvent>| async move {
            drop(events);
            pending::<RunCompletion>().await
        },
    );
    let task = tokio::spawn(session(gateway, close_worker).await.run());
    commands.send(user(1)).await.expect("command");
    let (error, ownership) = failed(task.await.expect("session join"));
    assert!(matches!(error, SessionFailure::EventChannelClosed));
    assert!(matches!(ownership, RunOwnership::Lost));
}

#[tokio::test]
async fn t15_recovery_gate_allows_only_t12_prefix_exact_retransmission() {
    let store = Store::session_test_store("recovery-gated-session")
        .await
        .expect("test store");
    let store = Arc::new(store);
    for purpose in [
        DataKeyPurpose::Command,
        DataKeyPurpose::Event,
        DataKeyPurpose::Transcript,
    ] {
        store
            .conversation_key(purpose)
            .await
            .expect("conversation key");
    }
    let writer = EventWriter::new(store.clone());
    writer
        .initialize_recovery_checkpoint()
        .await
        .expect("checkpoint");
    writer
        .persist_inbound(&user(1))
        .await
        .expect("durable receipt");
    drop(writer);
    let store = Arc::try_unwrap(store).unwrap_or_else(|_| panic!("sole test store owner"));

    let (gateway, commands, frames) = gateway();
    let worker: Arc<dyn RunWorker> = Arc::new(
        |core: RunCore,
         _initial: AdmittedCommand,
         mut controls: mpsc::Receiver<RunControl>,
         events: mpsc::Sender<AgentEvent>| async move {
            let _events = events;
            controls.close();
            RunCompletion::Completed(core)
        },
    );
    let session = Session::start(
        store,
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("recovery-gated session constructs");
    assert!(!session.recovery_steps.is_empty());
    let task = tokio::spawn(session.run());

    commands.send(user(1)).await.expect("exact retransmission");
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if received_acks(&frames).len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retransmission ACK");
    commands.send(user(2)).await.expect("fresh identity");
    let (error, ownership) = failed(task.await.expect("session join"));
    assert!(matches!(error, SessionFailure::RecoveryRequired { .. }));
    assert!(matches!(ownership, RunOwnership::Recovered(_)));
    assert_eq!(received_acks(&frames).len(), 1);

    // A fresh identity is rejected by InboundAdmission before CommandReceived;
    // this is separately frozen by the T12 admission tests. This actor test
    // proves the T15 Session gate never admits unseen work while T17-owned
    // full-suffix hydration remains required.
}

struct CommitCheckingGateway {
    commands: mpsc::Receiver<InboundCommand>,
    pool: sqlx::SqlitePool,
    observed: Arc<Mutex<Vec<(u64, String)>>>,
}

impl Gateway for CommitCheckingGateway {
    type Reader = SimpleReader;
    type Writer = CommitCheckingWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        (
            SimpleReader(self.commands),
            CommitCheckingWriter {
                pool: self.pool,
                observed: self.observed,
            },
        )
    }
}

struct CommitCheckingWriter {
    pool: sqlx::SqlitePool,
    observed: Arc<Mutex<Vec<(u64, String)>>>,
}

#[async_trait]
impl GatewayWriter for CommitCheckingWriter {
    async fn send(&mut self, frame: OutboundFrame) -> Result<()> {
        if let OutboundFrame::Event { envelope } = frame {
            let seq = envelope
                .seq
                .ok_or_else(|| anyhow!("durable fixture event lost seq"))?;
            let kind: String =
                sqlx::query_scalar("SELECT event_type FROM agent_events WHERE seq = ?")
                    .bind(i64::try_from(seq)?)
                    .fetch_one(&self.pool)
                    .await?;
            self.observed
                .lock()
                .expect("observed mutex")
                .push((seq, kind));
        }
        Ok(())
    }
}

fn bridge_assistant(reason: StopReason) -> PublicMessage {
    PublicMessage::Assistant(PublicAssistantMessage {
        content: Vec::new(),
        model: "bridge-model".to_owned(),
        provider: "fixture".to_owned(),
        origin: ProviderOrigin {
            provider_instance_id: "bridge-fixture".to_owned(),
            protocol: ApiProtocol::OpenAiChatCompletions,
            model: "bridge-model".to_owned(),
        },
        usage: Usage::default(),
        stop_reason: reason,
        error_message: None,
        provider_code: None,
        interrupted: false,
        timestamp: Utc::now(),
    })
}

#[tokio::test]
async fn active_second_user_stays_received_then_runs_after_the_current_agent_end() {
    let store = Store::session_test_store("durable-deferred-second-user-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (gateway, commands, frames) = gateway();
    let run_count = Arc::new(AtomicUsize::new(0));
    let first_started = Arc::new(Notify::new());
    let release_first = Arc::new(Notify::new());
    let worker: Arc<dyn RunWorker> = Arc::new({
        let run_count = run_count.clone();
        let first_started = first_started.clone();
        let release_first = release_first.clone();
        move |core: RunCore,
              initial: AdmittedCommand,
              _controls: mpsc::Receiver<RunControl>,
              events: mpsc::Sender<AgentEvent>| {
            let run_count = run_count.clone();
            let first_started = first_started.clone();
            let release_first = release_first.clone();
            async move {
                let ordinal = run_count.fetch_add(1, Ordering::SeqCst);
                emit_idle_injection(&events, &initial).await;
                if ordinal == 0 {
                    first_started.notify_one();
                    release_first.notified().await;
                }
                let assistant = bridge_assistant(StopReason::Stop);
                let assistant_id = format!("deferred-assistant-{}", ordinal + 1);
                for event in [
                    AgentEvent::MessageStart {
                        message_id: assistant_id.clone(),
                        message: Box::new(bridge_assistant(StopReason::Stop)),
                    },
                    AgentEvent::MessageEnd {
                        message_id: assistant_id,
                        message: Box::new(assistant.clone()),
                    },
                    AgentEvent::TurnEnd {
                        message: Some(Box::new(assistant)),
                        tool_results: Vec::new(),
                    },
                    AgentEvent::AgentEnd,
                ] {
                    events.send(event).await.expect("session event receiver");
                }
                RunCompletion::Completed(core)
            }
        }
    });
    let session = Session::start(
        store,
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session");
    let task = tokio::spawn(session.run());
    commands.send(user(1)).await.expect("first command");
    tokio::time::timeout(std::time::Duration::from_secs(2), first_started.notified())
        .await
        .expect("first run started");
    commands.send(user(2)).await.expect("second command");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while received_acks(&frames).len() < 2 && !task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second Received ACK");

    let before_release: (String, String, Option<String>) =
        sqlx::query_as("SELECT status, run_phase, run_id FROM inbound_commands WHERE seq=2")
            .fetch_one(&pool)
            .await
            .expect("deferred command row");
    assert_eq!(
        before_release,
        ("received".to_owned(), "received".to_owned(), None)
    );
    assert_eq!(run_count.load(Ordering::SeqCst), 1);
    assert!(!frames.lock().expect("frame mutex").iter().any(|frame| {
        matches!(frame, OutboundFrame::CommandAck { ack }
            if ack.command_id.as_str() == "00000000-0000-4000-8000-000000000002"
                && ack.status == CommandAckStatus::Applied)
    }));

    release_first.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let applied = frames
                .lock()
                .expect("frame mutex")
                .iter()
                .filter(|frame| {
                    matches!(frame, OutboundFrame::CommandAck { ack }
                        if ack.status == CommandAckStatus::Applied)
                })
                .count();
            if applied == 2 || task.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both commands applied");
    drop(commands);
    completed(task.await.expect("session join"));

    let statuses: Vec<(i64, String, String)> =
        sqlx::query_as("SELECT seq, status, application_kind FROM inbound_commands ORDER BY seq")
            .fetch_all(&pool)
            .await
            .expect("both durable command outcomes");
    assert_eq!(
        statuses,
        vec![
            (1, "applied".to_owned(), "idle_run".to_owned()),
            (2, "applied".to_owned(), "idle_run".to_owned()),
        ]
    );
    assert_eq!(run_count.load(Ordering::SeqCst), 2);
}

fn deferred_boundary_worker(
    run_count: Arc<AtomicUsize>,
    first_started: Arc<Notify>,
    release_first: Arc<Notify>,
) -> Arc<dyn RunWorker> {
    Arc::new(
        move |core: RunCore,
              initial: AdmittedCommand,
              _controls: mpsc::Receiver<RunControl>,
              events: mpsc::Sender<AgentEvent>| {
            let run_count = run_count.clone();
            let first_started = first_started.clone();
            let release_first = release_first.clone();
            async move {
                let ordinal = run_count.fetch_add(1, Ordering::SeqCst);
                emit_idle_injection(&events, &initial).await;
                if ordinal == 0 {
                    first_started.notify_one();
                    release_first.notified().await;
                }
                let assistant = bridge_assistant(StopReason::Stop);
                let assistant_id = format!("fifo-boundary-assistant-{ordinal}");
                for event in [
                    AgentEvent::MessageStart {
                        message_id: assistant_id.clone(),
                        message: Box::new(bridge_assistant(StopReason::Stop)),
                    },
                    AgentEvent::MessageEnd {
                        message_id: assistant_id,
                        message: Box::new(assistant.clone()),
                    },
                    AgentEvent::TurnEnd {
                        message: Some(Box::new(assistant)),
                        tool_results: Vec::new(),
                    },
                    AgentEvent::AgentEnd,
                ] {
                    events.send(event).await.expect("session event receiver");
                }
                RunCompletion::Completed(core)
            }
        },
    )
}

#[tokio::test]
async fn active_user_then_abort_is_cut_off_after_agent_end_without_starting_user() {
    let store = Store::session_test_store("active-user-abort-fifo-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (gateway, commands, frames) = gateway();
    let run_count = Arc::new(AtomicUsize::new(0));
    let first_started = Arc::new(Notify::new());
    let release_first = Arc::new(Notify::new());
    let worker = deferred_boundary_worker(
        run_count.clone(),
        first_started.clone(),
        release_first.clone(),
    );
    let session = Session::start(
        store,
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session");
    let task = tokio::spawn(session.run());
    commands.send(user(1)).await.expect("active command");
    tokio::time::timeout(std::time::Duration::from_secs(2), first_started.notified())
        .await
        .expect("first run started");
    commands.send(user(2)).await.expect("deferred user");
    commands.send(abort(3)).await.expect("deferred Abort");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while received_acks(&frames).len() < 3 && !task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both active-run commands admitted");
    release_first.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let terminal = frames
                .lock()
                .expect("frame mutex")
                .iter()
                .filter(|frame| {
                    matches!(frame, OutboundFrame::CommandAck { ack }
                        if matches!(ack.status, CommandAckStatus::Applied | CommandAckStatus::Superseded))
                })
                .count();
            if terminal == 3 || task.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Abort cutoff terminal ACKs");
    drop(commands);
    completed(task.await.expect("session join"));

    let states: Vec<(i64, String, Option<String>)> =
        sqlx::query_as("SELECT seq, status, run_id FROM inbound_commands ORDER BY seq")
            .fetch_all(&pool)
            .await
            .expect("FIFO command states");
    assert_eq!(states[1], (2, "superseded".to_owned(), None));
    assert_eq!(states[2], (3, "applied".to_owned(), None));
    assert_eq!(run_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn active_abort_then_user_applies_abort_before_starting_later_user() {
    let store = Store::session_test_store("active-abort-user-fifo-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (gateway, commands, frames) = gateway();
    let run_count = Arc::new(AtomicUsize::new(0));
    let first_started = Arc::new(Notify::new());
    let release_first = Arc::new(Notify::new());
    let worker = deferred_boundary_worker(
        run_count.clone(),
        first_started.clone(),
        release_first.clone(),
    );
    let session = Session::start(
        store,
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session");
    let task = tokio::spawn(session.run());
    commands.send(user(1)).await.expect("active command");
    tokio::time::timeout(std::time::Duration::from_secs(2), first_started.notified())
        .await
        .expect("first run started");
    commands.send(abort(2)).await.expect("deferred Abort");
    commands.send(user(3)).await.expect("later user");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while received_acks(&frames).len() < 3 && !task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both active-run commands admitted");
    release_first.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while run_count.load(Ordering::SeqCst) < 2 && !task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("later user run");
    drop(commands);
    completed(task.await.expect("session join"));

    let states: Vec<(i64, String, Option<String>)> =
        sqlx::query_as("SELECT seq, status, run_id FROM inbound_commands ORDER BY seq")
            .fetch_all(&pool)
            .await
            .expect("FIFO command states");
    assert_eq!(states[1], (2, "applied".to_owned(), None));
    assert_eq!(states[2].1, "applied");
    assert!(states[2].2.is_some());
    assert_eq!(run_count.load(Ordering::SeqCst), 2);
}

struct OpaqueContextDriver;

struct DurableToolBarrierDriver {
    pool: sqlx::SqlitePool,
    executions: AtomicUsize,
    observed_running: AtomicBool,
    observed_contexts: Mutex<Vec<Vec<ContextMessage>>>,
}

impl DurableToolBarrierDriver {
    fn new(pool: sqlx::SqlitePool) -> Self {
        Self {
            pool,
            executions: AtomicUsize::new(0),
            observed_running: AtomicBool::new(false),
            observed_contexts: Mutex::new(Vec::new()),
        }
    }

    fn origin() -> ProviderOrigin {
        ProviderOrigin {
            provider_instance_id: "durable-tool-barrier".to_owned(),
            protocol: ApiProtocol::OpenAiChatCompletions,
            model: "bridge-model".to_owned(),
        }
    }
}

#[async_trait]
impl RunDriver for DurableToolBarrierDriver {
    async fn start_provider(
        &self,
        attempt: usize,
        context: &[ContextMessage],
        cancel: CancellationToken,
    ) -> Result<ProviderAttempt> {
        self.observed_contexts
            .lock()
            .expect("observed contexts")
            .push(context.to_vec());
        let (tx, rx) = mpsc::channel(8);
        tx.try_send(ProviderEvent::Start).expect("provider start");
        let mut content = Vec::new();
        let reason = if attempt == 0 {
            let call = ToolCall {
                id: "barrier-call".to_owned(),
                name: "fixture-tool".to_owned(),
                arguments: serde_json::from_value::<ValidatedToolArguments>(
                    serde_json::json!({"safe":true}),
                )?,
            };
            tx.try_send(ProviderEvent::ToolCallStart { content_index: 0 })?;
            tx.try_send(ProviderEvent::ToolCallEnd {
                content_index: 0,
                tool_call: call.clone(),
            })?;
            content.push(AssistantContent::ToolCall {
                tool_call: call,
                wire_item_index: 0,
            });
            StopReason::ToolUse
        } else {
            StopReason::Stop
        };
        let message = AssistantMessage {
            content,
            model: "bridge-model".to_owned(),
            provider: "fixture".to_owned(),
            origin: Self::origin(),
            usage: Usage::default(),
            stop_reason: reason,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: Utc::now(),
        };
        tx.try_send(ProviderEvent::Done {
            reason,
            output: ProviderOutput {
                message,
                provider_context: Vec::new(),
            },
        })?;
        drop(tx);
        Ok(ProviderAttempt {
            message_id: format!("barrier-assistant-{attempt}"),
            initial_message: bridge_assistant(StopReason::Stop),
            events: ProviderEventStream::new(rx, cancel, "fixture", Self::origin()),
        })
    }

    async fn execute_tool(
        &self,
        call: &ToolCall,
        _cancel: CancellationToken,
    ) -> Result<ToolResultMessage> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        let state: String =
            sqlx::query_scalar("SELECT state FROM tool_executions WHERE tool_call_id = ?")
                .bind(&call.id)
                .fetch_one(&self.pool)
                .await?;
        self.observed_running
            .store(state == "running", Ordering::SeqCst);
        Ok(ToolResultMessage {
            tool_call_id: call.id.clone(),
            tool_name: call.name.clone(),
            content: Vec::new(),
            details: serde_json::json!({"ok":true}),
            is_error: false,
            timestamp: Utc::now(),
        })
    }

    fn synthetic_error(&self, message: &str) -> PublicMessage {
        let PublicMessage::Assistant(mut assistant) = bridge_assistant(StopReason::Error) else {
            unreachable!()
        };
        assistant.error_message = Some(message.to_owned());
        PublicMessage::Assistant(assistant)
    }

    async fn plan_overflow_recovery(
        &self,
        _core: &RunCore,
        _request: OverflowRecoveryRequest,
        _active_context: &[ContextMessage],
    ) -> Result<OverflowRecoveryOutcome> {
        Err(anyhow!("barrier fixture has no overflow recovery"))
    }
}

#[tokio::test]
async fn tool_driver_observes_running_only_after_start_commit() {
    let store = Store::session_test_store("tool-start-commit-barrier")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let driver = Arc::new(DurableToolBarrierDriver::new(pool.clone()));
    let (gateway, _commands, _frames) = gateway();
    let mut session = Session::start(
        store,
        gateway,
        RunCore::new(),
        Arc::new(SequentialRunWorker::new(driver.clone())),
        test_executor_generation(),
    )
    .await
    .expect("session");
    session.admit_and_route(user(1)).await.expect("command");
    drive_active_to_completion(&mut session)
        .await
        .expect("run completion");
    session.wait_outbound_idle().await;

    assert_eq!(driver.executions.load(Ordering::SeqCst), 1);
    assert!(driver.observed_running.load(Ordering::SeqCst));
    let second = {
        let contexts = driver.observed_contexts.lock().expect("observed contexts");
        assert_eq!(contexts.len(), 2);
        contexts[1].clone()
    };
    assert_eq!(
        second.len(),
        3,
        "user, assistant, and tool result are retained"
    );
    let durable: Vec<(String, u64)> =
        sqlx::query_as("SELECT id, seq FROM messages ORDER BY seq LIMIT 3")
            .fetch_all(&pool)
            .await
            .expect("durable message anchors");
    let observed: Vec<(String, u64)> = second
        .iter()
        .map(|message| match message {
            ContextMessage::Persisted { id, seq, .. } => (id.clone(), *seq),
            ContextMessage::Synthetic { .. } => panic!("live durable context became synthetic"),
        })
        .collect();
    assert_eq!(observed, durable);
    assert_eq!(
        crate::memory::transform::transform(&second, &DurableToolBarrierDriver::origin()),
        second,
        "send normalization preserves exact durable anchors",
    );
    let state: String =
        sqlx::query_scalar("SELECT state FROM tool_executions WHERE tool_call_id='barrier-call'")
            .fetch_one(&pool)
            .await
            .expect("tool row");
    assert_eq!(state, "succeeded");
}

#[tokio::test]
async fn rejected_running_transition_never_calls_driver_or_publishes_start() {
    let store = Store::session_test_store("tool-start-commit-barrier-failure")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    sqlx::query(
        "CREATE TRIGGER reject_tool_running BEFORE UPDATE OF state ON tool_executions
         WHEN NEW.state = 'running'
         BEGIN SELECT RAISE(ABORT, 'fixture rejects running transition'); END",
    )
    .execute(&pool)
    .await
    .expect("failure trigger");
    let driver = Arc::new(DurableToolBarrierDriver::new(pool.clone()));
    let (gateway, _commands, frames) = gateway();
    let mut session = Session::start(
        store,
        gateway,
        RunCore::new(),
        Arc::new(SequentialRunWorker::new(driver.clone())),
        test_executor_generation(),
    )
    .await
    .expect("session");
    session.admit_and_route(user(1)).await.expect("command");
    let failure = drive_active_to_completion(&mut session)
        .await
        .expect_err("running transition must fail");
    session.shutdown_active().await;
    session.wait_outbound_idle().await;

    assert!(
        failure
            .to_string()
            .contains("fixture rejects running transition")
    );
    assert_eq!(driver.executions.load(Ordering::SeqCst), 0);
    let state: String =
        sqlx::query_scalar("SELECT state FROM tool_executions WHERE tool_call_id='barrier-call'")
            .fetch_one(&pool)
            .await
            .expect("prepared tool row");
    assert_eq!(state, "prepared");
    assert!(frames.lock().expect("frames").iter().all(|frame| {
        !matches!(frame, OutboundFrame::Event { envelope }
            if envelope.event["type"] == "tool_execution_start")
    }));
}

#[tokio::test]
async fn blocked_writer_drops_only_volatile_suffix_and_preserves_terminal_reserve() {
    let store = Store::session_test_store("blocked-writer-volatile-reserve")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (_commands_tx, commands) = mpsc::channel(1);
    let frames = Arc::new(Mutex::new(Vec::new()));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let gateway = BlockingWriterGateway {
        commands,
        frames: frames.clone(),
        entered: entered.clone(),
        release: release.clone(),
    };
    let worker: Arc<dyn RunWorker> = Arc::new(
        |core: RunCore,
         initial: AdmittedCommand,
         mut controls: mpsc::Receiver<RunControl>,
         events: mpsc::Sender<AgentEvent>| async move {
            emit_idle_injection(&events, &initial).await;
            let message_id = "volatile-reserve-assistant".to_owned();
            let assistant = bridge_assistant(StopReason::Stop);
            events
                .send(AgentEvent::MessageStart {
                    message_id: message_id.clone(),
                    message: Box::new(assistant.clone()),
                })
                .await
                .expect("message start");
            for sequence in 0..(VOLATILE_OUTBOUND_BUDGET + 8) {
                events
                    .send(AgentEvent::MessageUpdate {
                        message_id: message_id.clone(),
                        event: PublicStreamEvent::TextDelta {
                            content_index: 0,
                            delta: format!("{sequence}"),
                        },
                    })
                    .await
                    .expect("volatile update");
            }
            for event in [
                AgentEvent::MessageEnd {
                    message_id,
                    message: Box::new(assistant.clone()),
                },
                AgentEvent::TurnEnd {
                    message: Some(Box::new(assistant)),
                    tool_results: Vec::new(),
                },
                AgentEvent::AgentEnd,
            ] {
                events.send(event).await.expect("terminal lifecycle");
            }
            controls.close();
            RunCompletion::Completed(core)
        },
    );
    let mut session = Session::start(
        store,
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session");
    session.admit_and_route(user(1)).await.expect("command");
    entered.notified().await;
    drive_active_to_completion(&mut session)
        .await
        .expect("durable terminal suffix enqueues while writer is blocked");

    let volatile_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_events WHERE event_type='message_update'")
            .fetch_one(&pool)
            .await
            .expect("volatile row count");
    assert_eq!(volatile_rows, 0);
    release.notify_one();
    session.wait_outbound_idle().await;
    let frames = frames.lock().expect("frames");
    let update_count = frames
        .iter()
        .filter(|frame| {
            matches!(frame, OutboundFrame::Event { envelope }
            if envelope.event["type"] == "message_update")
        })
        .count();
    assert_eq!(update_count, VOLATILE_OUTBOUND_BUDGET);
    let kinds: Vec<&str> = frames
        .iter()
        .filter_map(|frame| match frame {
            OutboundFrame::Event { envelope } => envelope.event["type"].as_str(),
            OutboundFrame::CommandAck { .. } => None,
        })
        .collect();
    assert!(
        kinds.iter().position(|kind| *kind == "message_start")
            < kinds.iter().position(|kind| *kind == "message_update")
    );
    assert!(
        kinds.iter().rposition(|kind| *kind == "message_update")
            < kinds.iter().rposition(|kind| *kind == "message_end")
    );
    assert!(
        kinds.iter().rposition(|kind| *kind == "message_end")
            < kinds.iter().position(|kind| *kind == "agent_end")
    );
}

#[tokio::test]
async fn idle_gateway_eof_aborts_a_blocked_writer_without_hanging() {
    let store = Store::session_test_store("idle-eof-blocked-writer")
        .await
        .expect("test store");
    let (commands_tx, commands) = mpsc::channel(1);
    let idle = Arc::new(Notify::new());
    let writer_entered = Arc::new(Notify::new());
    let writer_dropped = Arc::new(Notify::new());
    let gateway = EofBlockingGateway {
        commands,
        idle: idle.clone(),
        writer_entered: writer_entered.clone(),
        writer_dropped: writer_dropped.clone(),
    };
    let worker: Arc<dyn RunWorker> = Arc::new(
        |core: RunCore,
         _initial: AdmittedCommand,
         _controls: mpsc::Receiver<RunControl>,
         _events: mpsc::Sender<AgentEvent>| async move { RunCompletion::Completed(core) },
    );
    let session = Session::start(
        store,
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session");
    let mut task = tokio::spawn(session.run());

    commands_tx.send(user(1)).await.expect("command");
    writer_entered.notified().await;
    // The second reader poll is entered only after the worker completion has
    // been handled and Session has returned to its idle control branch.
    idle.notified().await;
    drop(commands_tx);

    let result = tokio::time::timeout(std::time::Duration::from_secs(2), &mut task).await;
    if result.is_err() {
        task.abort();
        let _ = task.await;
        panic!("idle EOF must abort a blocked writer instead of waiting for it");
    }
    let result = result
        .expect("session timeout already checked")
        .expect("session join");
    completed(result);
    tokio::time::timeout(std::time::Duration::from_secs(2), writer_dropped.notified())
        .await
        .expect("writer half dropped after idle EOF");
}

#[tokio::test]
async fn aborting_session_drops_blocked_writer_and_active_worker() {
    let (commands_tx, commands) = mpsc::channel(1);
    let writer_entered = Arc::new(Notify::new());
    let writer_dropped = Arc::new(Notify::new());
    let gateway = EofBlockingGateway {
        commands,
        idle: Arc::new(Notify::new()),
        writer_entered: writer_entered.clone(),
        writer_dropped: writer_dropped.clone(),
    };
    let worker_entered = Arc::new(Notify::new());
    let worker_dropped = Arc::new(Notify::new());
    let worker: Arc<dyn RunWorker> = Arc::new({
        let worker_entered = worker_entered.clone();
        let worker_dropped = worker_dropped.clone();
        move |_core: RunCore,
              _initial: AdmittedCommand,
              _controls: mpsc::Receiver<RunControl>,
              events: mpsc::Sender<AgentEvent>| {
            let worker_entered = worker_entered.clone();
            let worker_dropped = worker_dropped.clone();
            async move {
                let _events = events;
                let _drop_notifier = DropNotifier(worker_dropped);
                worker_entered.notify_one();
                pending::<RunCompletion>().await
            }
        }
    });
    let session = Session::start(
        Store::session_test_store("aborting-session-drops-children")
            .await
            .expect("test store"),
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session startup");
    let task = tokio::spawn(session.run());

    commands_tx.send(user(1)).await.expect("command");
    tokio::time::timeout(std::time::Duration::from_secs(2), writer_entered.notified())
        .await
        .expect("writer entered blocked send");
    tokio::time::timeout(std::time::Duration::from_secs(2), worker_entered.notified())
        .await
        .expect("active worker entered");

    task.abort();
    assert!(
        task.await
            .expect_err("outer Session task must be cancelled")
            .is_cancelled()
    );

    tokio::time::timeout(std::time::Duration::from_secs(2), writer_dropped.notified())
        .await
        .expect("aborting Session drops the blocked writer");
    tokio::time::timeout(std::time::Duration::from_secs(2), worker_dropped.notified())
        .await
        .expect("aborting Session drops the active worker");
}

#[tokio::test]
async fn cancelling_shutdown_active_aborts_the_taken_worker() {
    let (gateway, _commands, _frames) = gateway();
    let worker_entered = Arc::new(Notify::new());
    let worker_dropped = Arc::new(Notify::new());
    let worker: Arc<dyn RunWorker> = Arc::new({
        let worker_entered = worker_entered.clone();
        let worker_dropped = worker_dropped.clone();
        move |_core: RunCore,
              _initial: AdmittedCommand,
              _controls: mpsc::Receiver<RunControl>,
              _events: mpsc::Sender<AgentEvent>| {
            let worker_entered = worker_entered.clone();
            let worker_dropped = worker_dropped.clone();
            async move {
                let _drop_notifier = DropNotifier(worker_dropped);
                worker_entered.notify_one();
                pending::<RunCompletion>().await
            }
        }
    });
    let mut session = session(gateway, worker).await;
    session
        .admit_and_route(user(1))
        .await
        .expect("start active worker");
    worker_entered.notified().await;

    let mut shutdown = Box::pin(session.shutdown_active());
    let waker = futures_util::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        shutdown.as_mut().poll(&mut context),
        Poll::Pending
    ));
    drop(shutdown);

    assert!(session.active.is_none(), "shutdown took the active run");
    tokio::time::timeout(std::time::Duration::from_secs(2), worker_dropped.notified())
        .await
        .expect("cancellation during shutdown aborts the taken worker");
}

#[test]
fn reliable_outbound_admission_fails_explicitly_when_full_or_closed() {
    let (tx, rx) = mpsc::channel(1);
    let handle = OutboundHandle {
        tx,
        volatile_in_flight: Arc::new(AtomicUsize::new(0)),
        progress: Arc::new(OutboundProgress::default()),
    };
    let ack = || OutboundFrame::CommandAck {
        ack: CommandAck {
            seq: 1,
            command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            status: CommandAckStatus::Received,
            reject_reason: None,
        },
    };
    handle
        .enqueue_reliable(vec![ack()])
        .expect("first reliable item");
    assert!(matches!(
        handle.enqueue_reliable(vec![ack()]),
        Err(SessionFailure::OutboundFull)
    ));
    drop(rx);
    assert!(matches!(
        handle.enqueue_reliable(vec![ack()]),
        Err(SessionFailure::OutboundClosed)
    ));
}

struct StartFailureDriver;

#[async_trait]
impl RunDriver for StartFailureDriver {
    async fn start_provider(
        &self,
        _attempt: usize,
        _context: &[ContextMessage],
        _cancel: CancellationToken,
    ) -> Result<ProviderAttempt> {
        Err(anyhow!("fixture provider start failure"))
    }

    async fn execute_tool(
        &self,
        _call: &ToolCall,
        _cancel: CancellationToken,
    ) -> Result<ToolResultMessage> {
        Err(anyhow!("start-failure fixture has no tools"))
    }

    fn synthetic_error(&self, message: &str) -> PublicMessage {
        let mut assistant = match bridge_assistant(StopReason::Error) {
            PublicMessage::Assistant(message) => message,
            _ => unreachable!(),
        };
        assistant.error_message = Some(message.to_owned());
        PublicMessage::Assistant(assistant)
    }

    async fn plan_overflow_recovery(
        &self,
        _core: &RunCore,
        _request: OverflowRecoveryRequest,
        _active_context: &[ContextMessage],
    ) -> Result<OverflowRecoveryOutcome> {
        Err(anyhow!("start-failure fixture has no overflow recovery"))
    }
}

#[async_trait]
impl RunDriver for OpaqueContextDriver {
    async fn start_provider(
        &self,
        _attempt: usize,
        _context: &[ContextMessage],
        _cancel: CancellationToken,
    ) -> Result<ProviderAttempt> {
        let origin = ProviderOrigin {
            provider_instance_id: "opaque-fixture".to_owned(),
            protocol: ApiProtocol::OpenAiResponses,
            model: "bridge-model".to_owned(),
        };
        let rejected = RejectedToolCall {
            id: "opaque-rejected-call".to_owned(),
            name: "opaque-tool".to_owned(),
            error: ToolArgumentError::InvalidJson,
        };
        let message = AssistantMessage {
            content: vec![AssistantContent::RejectedToolCall {
                rejected: rejected.clone(),
                wire_item_index: 0,
            }],
            model: origin.model.clone(),
            provider: "fixture".to_owned(),
            origin: origin.clone(),
            usage: Usage::default(),
            stop_reason: StopReason::Stop,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: Utc::now(),
        };
        let synthetic_result = ToolResultMessage {
            tool_call_id: rejected.id.clone(),
            tool_name: rejected.name.clone(),
            content: vec![UserContent::Text {
                text: "Tool arguments were rejected.".to_owned(),
            }],
            details: serde_json::json!({"category":"invalid_json"}),
            is_error: true,
            timestamp: Utc::now(),
        };
        let (tx, rx) = mpsc::channel(4);
        tx.try_send(ProviderEvent::Start).expect("provider start");
        tx.try_send(ProviderEvent::ToolCallStart { content_index: 0 })
            .expect("rejected tool start");
        tx.try_send(ProviderEvent::ToolCallRejected {
            content_index: 0,
            rejected,
            synthetic_result,
        })
        .expect("rejected tool terminal");
        tx.try_send(ProviderEvent::Done {
            reason: StopReason::Stop,
            output: ProviderOutput {
                message,
                provider_context: vec![ProviderContextFragment {
                    wire_item_index: Some(0),
                    payload: ProviderContextPayload::EncryptedReasoning {
                        protocol: ApiProtocol::OpenAiResponses,
                        item: serde_json::json!({"encrypted_content":"must-not-persist"}),
                    },
                }],
            },
        })
        .expect("provider terminal");
        drop(tx);
        Ok(ProviderAttempt {
            message_id: "opaque-refusal".to_owned(),
            initial_message: bridge_assistant(StopReason::Stop),
            events: ProviderEventStream::new(
                rx,
                CancellationToken::new(),
                "opaque-fixture",
                origin,
            ),
        })
    }

    async fn execute_tool(
        &self,
        _call: &ToolCall,
        _cancel: CancellationToken,
    ) -> Result<ToolResultMessage> {
        Err(anyhow!("opaque fixture has no tools"))
    }

    fn synthetic_error(&self, message: &str) -> PublicMessage {
        let mut assistant = match bridge_assistant(StopReason::Error) {
            PublicMessage::Assistant(message) => message,
            _ => unreachable!(),
        };
        assistant.error_message = Some(message.to_owned());
        PublicMessage::Assistant(assistant)
    }

    async fn plan_overflow_recovery(
        &self,
        _core: &RunCore,
        _request: OverflowRecoveryRequest,
        _active_context: &[ContextMessage],
    ) -> Result<OverflowRecoveryOutcome> {
        Err(anyhow!("opaque fixture has no overflow recovery"))
    }
}

#[tokio::test]
async fn durable_bridge_commits_each_event_before_gateway_delivery_with_exact_seq() {
    let store = Store::session_test_store("durable-bridge-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (commands_tx, commands) = mpsc::channel(2);
    let observed = Arc::new(Mutex::new(Vec::new()));
    let gateway = CommitCheckingGateway {
        commands,
        pool: pool.clone(),
        observed: observed.clone(),
    };
    let worker: Arc<dyn RunWorker> = Arc::new(
        |core: RunCore,
         initial: AdmittedCommand,
         _controls: mpsc::Receiver<RunControl>,
         events: mpsc::Sender<AgentEvent>| async move {
            let user = PublicMessage::User(UserMessage {
                content: vec![UserContent::Text {
                    text: "message 1".to_owned(),
                }],
                timestamp: initial.received_at(),
            });
            let assistant = bridge_assistant(StopReason::Stop);
            for event in [
                AgentEvent::AgentStart,
                AgentEvent::TurnStart,
                AgentEvent::MessageStart {
                    message_id: user_message_id(&initial.envelope().command_id),
                    message: Box::new(user.clone()),
                },
                AgentEvent::MessageEnd {
                    message_id: user_message_id(&initial.envelope().command_id),
                    message: Box::new(user),
                },
                AgentEvent::MessageStart {
                    message_id: "assistant-bridge".to_owned(),
                    message: Box::new(bridge_assistant(StopReason::Stop)),
                },
                AgentEvent::MessageEnd {
                    message_id: "assistant-bridge".to_owned(),
                    message: Box::new(assistant.clone()),
                },
                AgentEvent::TurnEnd {
                    message: Some(Box::new(assistant)),
                    tool_results: Vec::new(),
                },
                AgentEvent::AgentEnd,
            ] {
                events.send(event).await.expect("session event receiver");
            }
            RunCompletion::Completed(core)
        },
    );
    let session = Session::start(
        store,
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session");
    let task = tokio::spawn(session.run());
    commands_tx.send(user(1)).await.expect("command");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if observed.lock().expect("observed mutex").len() == 8 {
                break;
            }
            if task.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all durable frames");
    drop(commands_tx);
    completed(task.await.expect("session join"));

    let observed = observed.lock().expect("observed mutex").clone();
    assert_eq!(observed.len(), 8);
    assert!(observed.windows(2).all(|pair| pair[0].0 < pair[1].0));
    let stored: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
        .fetch_one(&pool)
        .await
        .expect("stored events");
    assert_eq!(stored, 8);
    let projected: String = sqlx::query_scalar(
        "SELECT json_extract(payload, '$.stop_reason') FROM messages WHERE id='assistant-bridge'",
    )
    .fetch_one(&pool)
    .await
    .expect("assistant projection");
    assert_eq!(projected, "stop");
}

async fn assert_first_length_tool_call_persists_generation(executor_generation: ProcessGeneration) {
    let store = Store::session_test_store(&format!("durable-length-session-{executor_generation}"))
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (gateway, commands, frames) = gateway();
    let worker: Arc<dyn RunWorker> = Arc::new(
        |core: RunCore,
         initial: AdmittedCommand,
         _controls: mpsc::Receiver<RunControl>,
         events: mpsc::Sender<AgentEvent>| async move {
            let user = PublicMessage::User(UserMessage {
                content: vec![UserContent::Text {
                    text: "message 1".to_owned(),
                }],
                timestamp: initial.received_at(),
            });
            let call = ToolCall {
                id: "length-call".to_owned(),
                name: "fixture-tool".to_owned(),
                arguments: serde_json::from_value::<ValidatedToolArguments>(
                    serde_json::json!({"safe":true}),
                )
                .expect("validated arguments"),
            };
            let mut length = match bridge_assistant(StopReason::Length) {
                PublicMessage::Assistant(message) => message,
                _ => unreachable!(),
            };
            length.content.push(PublicAssistantContent::ToolCall {
                tool_call: call.clone(),
                wire_item_index: 0,
            });
            let length = PublicMessage::Assistant(length);
            let result = ToolResultMessage {
                tool_call_id: call.id.clone(),
                tool_name: call.name.clone(),
                content: vec![UserContent::Text {
                    text: "Tool call was not executed: output token limit".to_owned(),
                }],
                details: serde_json::json!({"error":"output token limit"}),
                is_error: true,
                timestamp: Utc::now(),
            };
            let result_message = PublicMessage::ToolResult(result.clone());
            let result_id = "length-result".to_owned();
            for event in [
                AgentEvent::AgentStart,
                AgentEvent::TurnStart,
                AgentEvent::MessageStart {
                    message_id: user_message_id(&initial.envelope().command_id),
                    message: Box::new(user.clone()),
                },
                AgentEvent::MessageEnd {
                    message_id: user_message_id(&initial.envelope().command_id),
                    message: Box::new(user),
                },
                AgentEvent::MessageStart {
                    message_id: "length-assistant".to_owned(),
                    message: Box::new(bridge_assistant(StopReason::Stop)),
                },
                AgentEvent::MessageEnd {
                    message_id: "length-assistant".to_owned(),
                    message: Box::new(length.clone()),
                },
                AgentEvent::MessageStart {
                    message_id: result_id.clone(),
                    message: Box::new(result_message.clone()),
                },
                AgentEvent::MessageEnd {
                    message_id: result_id,
                    message: Box::new(result_message),
                },
                AgentEvent::TurnEnd {
                    message: Some(Box::new(length)),
                    tool_results: vec![result],
                },
                AgentEvent::AgentEnd,
            ] {
                events.send(event).await.expect("session event receiver");
            }
            RunCompletion::Completed(core)
        },
    );
    let session = Session::start(store, gateway, RunCore::new(), worker, executor_generation)
        .await
        .expect("session");
    let task = tokio::spawn(session.run());
    commands.send(user(1)).await.expect("command");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if frames.lock().expect("frame mutex").iter().any(|frame| {
                matches!(frame, OutboundFrame::Event { envelope }
                    if envelope.event["type"] == "agent_end")
            }) {
                break;
            }
            if task.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal frame");
    drop(commands);
    completed(task.await.expect("session join"));

    type LengthAuditRow = (String, i64, String, String, Option<String>, Option<String>);
    let rows: Vec<LengthAuditRow> = sqlx::query_as(
        "SELECT tool_call_id, executor_generation, idempotency_key, state, started_at, error_code
         FROM tool_executions",
    )
    .fetch_all(&pool)
    .await
    .expect("not-started audit row");
    assert_eq!(
        rows,
        vec![(
            "length-call".to_owned(),
            executor_generation.as_i64(),
            "00000000-0000-4000-8000-000000000001/length-call".to_owned(),
            "not_started".to_owned(),
            None,
            Some("length_guard".to_owned()),
        )]
    );
    assert!(!frames.lock().expect("frame mutex").iter().any(|frame| {
        matches!(frame, OutboundFrame::Event { envelope }
            if matches!(envelope.event["type"].as_str(),
                Some("tool_execution_start" | "tool_execution_end")))
    }));
}

#[tokio::test]
async fn first_length_tool_call_is_durably_not_started_without_public_execution_lifecycle() {
    for generation in [0, MAX_PROCESS_GENERATION] {
        assert_first_length_tool_call_persists_generation(
            ProcessGeneration::from_wire(generation).expect("valid boundary generation"),
        )
        .await;
    }
}

#[tokio::test]
async fn consecutive_length_guard_error_is_durably_not_started_and_closes_normally() {
    let store = Store::session_test_store("durable-consecutive-length-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (gateway, commands, frames) = gateway();
    let worker: Arc<dyn RunWorker> = Arc::new(
        |core: RunCore,
         initial: AdmittedCommand,
         _controls: mpsc::Receiver<RunControl>,
         events: mpsc::Sender<AgentEvent>| async move {
            emit_idle_injection(&events, &initial).await;
            for (ordinal, stop_reason, provider_code) in [
                (1, StopReason::Length, None),
                (2, StopReason::Error, Some(run::LENGTH_LOOP_CODE)),
            ] {
                if ordinal == 2 {
                    events.send(AgentEvent::TurnStart).await.expect("next turn");
                }
                let call = ToolCall {
                    id: format!("length-call-{ordinal}"),
                    name: "fixture-tool".to_owned(),
                    arguments: serde_json::from_value::<ValidatedToolArguments>(
                        serde_json::json!({"safe":true}),
                    )
                    .expect("validated arguments"),
                };
                let mut assistant = match bridge_assistant(stop_reason) {
                    PublicMessage::Assistant(message) => message,
                    _ => unreachable!(),
                };
                assistant.provider_code = provider_code.map(str::to_owned);
                assistant.content.push(PublicAssistantContent::ToolCall {
                    tool_call: call.clone(),
                    wire_item_index: 0,
                });
                let assistant = PublicMessage::Assistant(assistant);
                let result = ToolResultMessage {
                    tool_call_id: call.id.clone(),
                    tool_name: call.name,
                    content: vec![UserContent::Text {
                        text: "Tool call was not executed by the Length guard".to_owned(),
                    }],
                    details: serde_json::json!({"error":"length_guard"}),
                    is_error: true,
                    timestamp: Utc::now(),
                };
                let result_message = PublicMessage::ToolResult(result.clone());
                let assistant_id = format!("length-assistant-{ordinal}");
                let result_id = format!("length-result-{ordinal}");
                for event in [
                    AgentEvent::MessageStart {
                        message_id: assistant_id.clone(),
                        message: Box::new(bridge_assistant(StopReason::Stop)),
                    },
                    AgentEvent::MessageEnd {
                        message_id: assistant_id,
                        message: Box::new(assistant.clone()),
                    },
                    AgentEvent::MessageStart {
                        message_id: result_id.clone(),
                        message: Box::new(result_message.clone()),
                    },
                    AgentEvent::MessageEnd {
                        message_id: result_id,
                        message: Box::new(result_message),
                    },
                    AgentEvent::TurnEnd {
                        message: Some(Box::new(assistant)),
                        tool_results: vec![result],
                    },
                ] {
                    events.send(event).await.expect("session event receiver");
                }
            }
            events
                .send(AgentEvent::AgentEnd)
                .await
                .expect("session event receiver");
            RunCompletion::Completed(core)
        },
    );
    let session = Session::start(
        store,
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session");
    let task = tokio::spawn(session.run());
    commands.send(user(1)).await.expect("command");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !frames.lock().expect("frame mutex").iter().any(|frame| {
            matches!(frame, OutboundFrame::Event { envelope }
                if envelope.event["type"] == "agent_end")
        }) && !task.is_finished()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal frame");
    drop(commands);
    completed(task.await.expect("session join"));

    type LengthAuditRow = (String, i64, String, Option<String>, Option<String>);
    let rows: Vec<LengthAuditRow> = sqlx::query_as(
        "SELECT tool_call_id, executor_generation, state, started_at, error_code
         FROM tool_executions ORDER BY tool_call_id",
    )
    .fetch_all(&pool)
    .await
    .expect("two not-started audit rows");
    assert_eq!(
        rows,
        vec![
            (
                "length-call-1".to_owned(),
                test_executor_generation().as_i64(),
                "not_started".to_owned(),
                None,
                Some("length_guard".to_owned()),
            ),
            (
                "length-call-2".to_owned(),
                test_executor_generation().as_i64(),
                "not_started".to_owned(),
                None,
                Some("length_guard".to_owned()),
            ),
        ]
    );
    assert!(!frames.lock().expect("frame mutex").iter().any(|frame| {
        matches!(frame, OutboundFrame::Event { envelope }
            if matches!(envelope.event["type"].as_str(),
                Some("tool_execution_start" | "tool_execution_end")))
    }));
    let second_stop: String = sqlx::query_scalar(
        "SELECT json_extract(payload, '$.stop_reason') FROM messages WHERE id='length-assistant-2'",
    )
    .fetch_one(&pool)
    .await
    .expect("Error assistant persisted outside L0");
    assert_eq!(second_stop, "error");
}

#[tokio::test]
async fn mixed_valid_and_rejected_calls_commit_the_rejected_pair_before_valid_lifecycle() {
    let store = Store::session_test_store("durable-rejected-tool-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (gateway, commands, frames) = gateway();
    let worker: Arc<dyn RunWorker> = Arc::new(
        |core: RunCore,
         initial: AdmittedCommand,
         _controls: mpsc::Receiver<RunControl>,
         events: mpsc::Sender<AgentEvent>| async move {
            emit_idle_injection(&events, &initial).await;
            let rejected = RejectedToolCall {
                id: "rejected-call".to_owned(),
                name: "fixture-tool".to_owned(),
                error: ToolArgumentError::SchemaViolation,
            };
            let rejected_second = RejectedToolCall {
                id: "rejected-call-2".to_owned(),
                name: "fixture-tool".to_owned(),
                error: ToolArgumentError::InvalidJson,
            };
            let mut first = match bridge_assistant(StopReason::ToolUse) {
                PublicMessage::Assistant(message) => message,
                _ => unreachable!(),
            };
            let valid = ToolCall {
                id: "valid-call".to_owned(),
                name: "fixture-tool".to_owned(),
                arguments: serde_json::from_value::<ValidatedToolArguments>(
                    serde_json::json!({"safe":true}),
                )
                .expect("validated arguments"),
            };
            first.content.push(PublicAssistantContent::ToolCall {
                tool_call: valid.clone(),
                wire_item_index: 0,
            });
            first
                .content
                .push(PublicAssistantContent::RejectedToolCall {
                    rejected: rejected.clone(),
                    wire_item_index: 1,
                });
            first
                .content
                .push(PublicAssistantContent::RejectedToolCall {
                    rejected: rejected_second.clone(),
                    wire_item_index: 2,
                });
            let first = PublicMessage::Assistant(first);
            let result = ToolResultMessage {
                tool_call_id: rejected.id.clone(),
                tool_name: rejected.name.clone(),
                content: vec![UserContent::Text {
                    text: "Tool arguments were rejected; regenerate the call".to_owned(),
                }],
                details: serde_json::json!({"error":"schema_invalid"}),
                is_error: true,
                timestamp: Utc::now(),
            };
            let result_message = PublicMessage::ToolResult(result.clone());
            let result_second = ToolResultMessage {
                tool_call_id: rejected_second.id.clone(),
                tool_name: rejected_second.name.clone(),
                content: vec![UserContent::Text {
                    text: "Tool arguments were rejected; regenerate the call".to_owned(),
                }],
                details: serde_json::json!({"error":"invalid_json"}),
                is_error: true,
                timestamp: Utc::now(),
            };
            let result_second_message = PublicMessage::ToolResult(result_second);
            let valid_result = ToolResultMessage {
                tool_call_id: valid.id.clone(),
                tool_name: valid.name.clone(),
                content: vec![UserContent::Text {
                    text: "done".to_owned(),
                }],
                details: serde_json::json!({"ok":true}),
                is_error: false,
                timestamp: Utc::now(),
            };
            let valid_result_message = PublicMessage::ToolResult(valid_result.clone());
            let final_message = bridge_assistant(StopReason::Stop);
            for event in [
                AgentEvent::MessageStart {
                    message_id: "rejected-assistant".to_owned(),
                    message: Box::new(bridge_assistant(StopReason::Stop)),
                },
                AgentEvent::MessageEnd {
                    message_id: "rejected-assistant".to_owned(),
                    message: Box::new(first.clone()),
                },
                AgentEvent::MessageStart {
                    message_id: "rejected-result".to_owned(),
                    message: Box::new(result_message.clone()),
                },
                AgentEvent::MessageEnd {
                    message_id: "rejected-result".to_owned(),
                    message: Box::new(result_message),
                },
                AgentEvent::MessageStart {
                    message_id: "rejected-result-2".to_owned(),
                    message: Box::new(result_second_message.clone()),
                },
                AgentEvent::MessageEnd {
                    message_id: "rejected-result-2".to_owned(),
                    message: Box::new(result_second_message),
                },
                AgentEvent::ToolExecutionStart {
                    tool_call_id: valid.id.clone(),
                    tool_name: valid.name.clone(),
                    args: serde_json::json!({"safe":true}),
                },
                AgentEvent::ToolExecutionEnd {
                    tool_call_id: valid.id.clone(),
                    result: serde_json::to_value(&valid_result).expect("valid result"),
                    is_error: false,
                },
                AgentEvent::MessageStart {
                    message_id: "valid-result".to_owned(),
                    message: Box::new(valid_result_message.clone()),
                },
                AgentEvent::MessageEnd {
                    message_id: "valid-result".to_owned(),
                    message: Box::new(valid_result_message),
                },
                AgentEvent::TurnEnd {
                    message: Some(Box::new(first)),
                    tool_results: vec![valid_result],
                },
                AgentEvent::TurnStart,
                AgentEvent::MessageStart {
                    message_id: "post-rejection-attempt".to_owned(),
                    message: Box::new(final_message.clone()),
                },
                AgentEvent::MessageEnd {
                    message_id: "post-rejection-attempt".to_owned(),
                    message: Box::new(final_message.clone()),
                },
                AgentEvent::TurnEnd {
                    message: Some(Box::new(final_message)),
                    tool_results: Vec::new(),
                },
                AgentEvent::AgentEnd,
            ] {
                events.send(event).await.expect("session event receiver");
            }
            RunCompletion::Completed(core)
        },
    );
    let session = Session::start(
        store,
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session");
    let task = tokio::spawn(session.run());
    commands.send(user(1)).await.expect("command");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if frames.lock().expect("frame mutex").iter().any(|frame| {
                matches!(frame, OutboundFrame::CommandAck { ack }
                    if ack.status == CommandAckStatus::Applied)
            }) || task.is_finished()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("applied ACK");
    drop(commands);
    completed(task.await.expect("session join"));

    let stored: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, role FROM messages WHERE id IN ('rejected-assistant','rejected-result','rejected-result-2','valid-result','post-rejection-attempt') ORDER BY seq",
    )
    .fetch_all(&pool)
    .await
    .expect("rejection pair and next attempt");
    assert_eq!(
        stored,
        vec![
            ("rejected-assistant".to_owned(), "assistant".to_owned()),
            ("rejected-result".to_owned(), "tool_result".to_owned()),
            ("rejected-result-2".to_owned(), "tool_result".to_owned()),
            ("valid-result".to_owned(), "tool_result".to_owned()),
            ("post-rejection-attempt".to_owned(), "assistant".to_owned()),
        ]
    );
    let result_error: i64 = sqlx::query_scalar(
        "SELECT json_extract(payload, '$.is_error') FROM messages WHERE id='rejected-result'",
    )
    .fetch_one(&pool)
    .await
    .expect("synthetic result projection");
    assert_eq!(result_error, 1);
    let executions: Vec<(String, String)> =
        sqlx::query_as("SELECT tool_call_id, state FROM tool_executions ORDER BY tool_call_id")
            .fetch_all(&pool)
            .await
            .expect("only the valid execution lifecycle");
    assert_eq!(
        executions,
        vec![("valid-call".to_owned(), "succeeded".to_owned())]
    );
}

#[tokio::test]
async fn failed_idle_injection_batch_publishes_no_partial_event_frame() {
    let store = Store::session_test_store("durable-rollback-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (gateway, commands, frames) = gateway();
    let worker: Arc<dyn RunWorker> = Arc::new(
        |mut core: RunCore,
         initial: AdmittedCommand,
         _controls: mpsc::Receiver<RunControl>,
         events: mpsc::Sender<AgentEvent>| async move {
            let invalid = PublicMessage::User(UserMessage {
                content: vec![UserContent::Text {
                    text: "message 1".to_owned(),
                }],
                // Exact start/end, but deliberately not the durable receipt timestamp.
                timestamp: initial.received_at() + chrono::Duration::seconds(1),
            });
            for event in [
                AgentEvent::AgentStart,
                AgentEvent::TurnStart,
                AgentEvent::MessageStart {
                    message_id: user_message_id(&initial.envelope().command_id),
                    message: Box::new(invalid.clone()),
                },
                AgentEvent::MessageEnd {
                    message_id: user_message_id(&initial.envelope().command_id),
                    message: Box::new(invalid),
                },
            ] {
                events.send(event).await.expect("session event receiver");
            }
            core.mark_mutated();
            RunCompletion::Completed(core)
        },
    );
    let session = Session::start(
        store,
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session");
    let task = tokio::spawn(session.run());
    commands.send(user(1)).await.expect("command");
    let (failure, ownership) = failed(task.await.expect("session join"));
    assert!(matches!(failure, SessionFailure::Other(_)));
    assert!(matches!(ownership, RunOwnership::Lost));
    assert!(
        !frames
            .lock()
            .expect("frame mutex")
            .iter()
            .any(|frame| { matches!(frame, OutboundFrame::Event { .. }) })
    );
    let durable_events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
        .fetch_one(&pool)
        .await
        .expect("event count");
    assert_eq!(
        durable_events, 0,
        "the four-event injection batch rolled back"
    );
}

#[tokio::test]
async fn retry_error_is_excluded_and_retry_schedule_precedes_next_attempt() {
    let store = Store::session_test_store("durable-retry-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (gateway, commands, frames) = gateway();
    let worker: Arc<dyn RunWorker> = Arc::new(
        |core: RunCore,
         initial: AdmittedCommand,
         _controls: mpsc::Receiver<RunControl>,
         events: mpsc::Sender<AgentEvent>| async move {
            let user = PublicMessage::User(UserMessage {
                content: vec![UserContent::Text {
                    text: "message 1".to_owned(),
                }],
                timestamp: initial.received_at(),
            });
            let mut error = match bridge_assistant(StopReason::Error) {
                PublicMessage::Assistant(message) => message,
                _ => unreachable!(),
            };
            error.error_message = Some("network error".to_owned());
            error.provider_code = Some("network_error".to_owned());
            let error = PublicMessage::Assistant(error);
            let success = bridge_assistant(StopReason::Stop);
            for event in [
                AgentEvent::AgentStart,
                AgentEvent::TurnStart,
                AgentEvent::MessageStart {
                    message_id: user_message_id(&initial.envelope().command_id),
                    message: Box::new(user.clone()),
                },
                AgentEvent::MessageEnd {
                    message_id: user_message_id(&initial.envelope().command_id),
                    message: Box::new(user),
                },
                AgentEvent::MessageStart {
                    message_id: "retry-error".to_owned(),
                    message: Box::new(bridge_assistant(StopReason::Stop)),
                },
                AgentEvent::MessageEnd {
                    message_id: "retry-error".to_owned(),
                    message: Box::new(error),
                },
                AgentEvent::RetryScheduled {
                    attempt: 1,
                    delay_ms: 2_000,
                    retry_at: Utc::now(),
                    error_message: "network error".to_owned(),
                },
                AgentEvent::MessageStart {
                    message_id: "retry-success".to_owned(),
                    message: Box::new(bridge_assistant(StopReason::Stop)),
                },
                AgentEvent::MessageEnd {
                    message_id: "retry-success".to_owned(),
                    message: Box::new(success.clone()),
                },
                AgentEvent::TurnEnd {
                    message: Some(Box::new(success)),
                    tool_results: Vec::new(),
                },
                AgentEvent::AgentEnd,
            ] {
                events.send(event).await.expect("session event receiver");
            }
            RunCompletion::Completed(core)
        },
    );
    let session = Session::start(
        store,
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session");
    let task = tokio::spawn(session.run());
    commands.send(user(1)).await.expect("command");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if frames.lock().expect("frame mutex").iter().any(|frame| {
                matches!(frame, OutboundFrame::Event { envelope }
                    if envelope.event["type"] == "agent_end")
            }) || task.is_finished()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal frame");
    drop(commands);
    completed(task.await.expect("session join"));

    let kinds: Vec<String> = sqlx::query_scalar(
        "SELECT event_type FROM agent_events WHERE event_type IN ('message_end','retry_scheduled') ORDER BY seq",
    )
    .fetch_all(&pool)
    .await
    .expect("retry durable sequence");
    assert_eq!(
        kinds,
        vec![
            "message_end",
            "message_end",
            "retry_scheduled",
            "message_end"
        ]
    );
    let error_stop: String = sqlx::query_scalar(
        "SELECT json_extract(payload, '$.stop_reason') FROM messages WHERE id='retry-error'",
    )
    .fetch_one(&pool)
    .await
    .expect("retry error projection");
    assert_eq!(error_stop, "error");
}

#[tokio::test]
async fn provider_start_failures_in_two_runs_use_distinct_stable_durable_message_ids() {
    let store = Store::session_test_store("two-run-provider-start-failure-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (gateway, commands, frames) = gateway();
    let worker: Arc<dyn RunWorker> =
        Arc::new(SequentialRunWorker::new(Arc::new(StartFailureDriver)));
    let session = Session::start(
        store,
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session");
    let task = tokio::spawn(session.run());

    commands.send(user(1)).await.expect("first command");
    commands.send(user(2)).await.expect("second command");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let applied = frames
                .lock()
                .expect("frame mutex")
                .iter()
                .filter(|frame| {
                    matches!(frame, OutboundFrame::CommandAck { ack }
                        if ack.status == CommandAckStatus::Applied)
                })
                .count();
            if applied == 2 || task.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both failed provider starts close durably");
    drop(commands);
    completed(task.await.expect("session join"));

    let synthetic_ids: Vec<String> =
        sqlx::query_scalar("SELECT id FROM messages WHERE role='assistant' ORDER BY seq")
            .fetch_all(&pool)
            .await
            .expect("synthetic durable message IDs");
    assert_eq!(synthetic_ids.len(), 2);
    assert_ne!(synthetic_ids[0], synthetic_ids[1]);
    assert!(
        synthetic_ids
            .iter()
            .all(|message_id| Uuid::parse_str(message_id).is_ok()),
        "synthetic identities use the bounded UUIDv5 form"
    );
}

#[tokio::test]
async fn opaque_context_refusal_closes_durably_before_applied_ack() {
    let store = Store::session_test_store("durable-opaque-refusal-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (gateway, commands, frames) = gateway();
    let worker: Arc<dyn RunWorker> =
        Arc::new(SequentialRunWorker::new(Arc::new(OpaqueContextDriver)));
    let session = Session::start(
        store,
        gateway,
        RunCore::new(),
        worker,
        test_executor_generation(),
    )
    .await
    .expect("session");
    let task = tokio::spawn(session.run());
    commands.send(user(1)).await.expect("command");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if frames.lock().expect("frame mutex").iter().any(|frame| {
                matches!(frame, OutboundFrame::CommandAck { ack }
                    if ack.status == CommandAckStatus::Applied)
            }) || task.is_finished()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("applied ACK");
    drop(commands);
    let core = completed(task.await.expect("session join"));
    // EventWriter rejects a persisted Error assistant when append_to_l0=true;
    // the durable row asserted below plus this send-context exclusion proves
    // the bridge supplied append_to_l0=false at the real Store boundary.
    assert_eq!(core.runtime_context.len(), 1);
    assert!(matches!(
        core.runtime_context.first(),
        Some(ContextMessage::Persisted {
            message: crate::provider::types::Message::User(_),
            ..
        })
    ));

    let kinds: Vec<String> = sqlx::query_scalar(
        "SELECT event_type FROM agent_events WHERE event_type IN ('message_start','message_end','turn_end','agent_end') ORDER BY seq",
    )
    .fetch_all(&pool)
    .await
    .expect("closed durable lifecycle");
    assert_eq!(
        &kinds[kinds.len() - 4..],
        ["message_start", "message_end", "turn_end", "agent_end"]
    );
    let error_stop: String = sqlx::query_scalar(
        "SELECT json_extract(payload, '$.stop_reason') FROM messages WHERE id='opaque-refusal'",
    )
    .fetch_one(&pool)
    .await
    .expect("error projection");
    assert_eq!(error_stop, "error");
    let opaque_leaks: i64 = sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM messages WHERE payload LIKE '%encrypted_content%') + (SELECT COUNT(*) FROM agent_events WHERE envelope LIKE '%encrypted_content%')",
    )
    .fetch_one(&pool)
    .await
    .expect("opaque leak check");
    assert_eq!(opaque_leaks, 0);
    let rejected_leaks: i64 = sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM messages WHERE id='opaque-rejected-call') + (SELECT COUNT(*) FROM messages WHERE json_extract(payload, '$.tool_call_id')='opaque-rejected-call') + (SELECT COUNT(*) FROM agent_events WHERE envelope LIKE '%opaque-rejected-call%')",
    )
    .fetch_one(&pool)
    .await
    .expect("rejected pair leak check");
    assert_eq!(rejected_leaks, 0);
    assert!(!frames.lock().expect("frame mutex").iter().any(|frame| {
        matches!(frame, OutboundFrame::Event { envelope }
            if envelope.event.to_string().contains("\"tool_call_id\":\"opaque-rejected-call\""))
    }));
    let frames = frames.lock().expect("frame mutex");
    let agent_end = frames
        .iter()
        .position(|frame| {
            matches!(frame, OutboundFrame::Event { envelope }
            if envelope.event["type"] == "agent_end")
        })
        .expect("AgentEnd frame");
    let applied = frames
        .iter()
        .position(|frame| {
            matches!(frame, OutboundFrame::CommandAck { ack }
            if ack.status == CommandAckStatus::Applied)
        })
        .expect("Applied ACK");
    assert!(agent_end < applied);
}

async fn assert_normal_tool_lifecycle_persists_generation(executor_generation: ProcessGeneration) {
    let store = Store::session_test_store(&format!(
        "durable-normal-tool-session-{executor_generation}"
    ))
    .await
    .expect("test store");
    let pool = store.pool().clone();
    let (gateway, commands, frames) = gateway();
    let worker: Arc<dyn RunWorker> = Arc::new(
        |core: RunCore,
         initial: AdmittedCommand,
         _controls: mpsc::Receiver<RunControl>,
         events: mpsc::Sender<AgentEvent>| async move {
            emit_idle_injection(&events, &initial).await;
            let call = ToolCall {
                id: "normal-call".to_owned(),
                name: "fixture-tool".to_owned(),
                arguments: serde_json::from_value::<ValidatedToolArguments>(
                    serde_json::json!({"safe":true}),
                )
                .expect("validated arguments"),
            };
            let mut tool_use = match bridge_assistant(StopReason::ToolUse) {
                PublicMessage::Assistant(message) => message,
                _ => unreachable!(),
            };
            tool_use.content.push(PublicAssistantContent::ToolCall {
                tool_call: call.clone(),
                wire_item_index: 0,
            });
            let tool_use = PublicMessage::Assistant(tool_use);
            let result = ToolResultMessage {
                tool_call_id: call.id.clone(),
                tool_name: call.name.clone(),
                content: Vec::new(),
                details: serde_json::json!({"ok":true}),
                is_error: false,
                timestamp: Utc::now(),
            };
            let result_message = PublicMessage::ToolResult(result.clone());
            let final_message = bridge_assistant(StopReason::Stop);
            for event in [
                AgentEvent::MessageStart {
                    message_id: "normal-assistant".to_owned(),
                    message: Box::new(bridge_assistant(StopReason::Stop)),
                },
                AgentEvent::MessageEnd {
                    message_id: "normal-assistant".to_owned(),
                    message: Box::new(tool_use.clone()),
                },
                AgentEvent::ToolExecutionStart {
                    tool_call_id: call.id.clone(),
                    tool_name: call.name.clone(),
                    args: serde_json::json!({"safe":true}),
                },
                AgentEvent::ToolExecutionEnd {
                    tool_call_id: call.id.clone(),
                    result: serde_json::to_value(&result).expect("tool result"),
                    is_error: false,
                },
                AgentEvent::MessageStart {
                    message_id: "normal-result".to_owned(),
                    message: Box::new(result_message.clone()),
                },
                AgentEvent::MessageEnd {
                    message_id: "normal-result".to_owned(),
                    message: Box::new(result_message),
                },
                AgentEvent::TurnEnd {
                    message: Some(Box::new(tool_use)),
                    tool_results: vec![result],
                },
                AgentEvent::TurnStart,
                AgentEvent::MessageStart {
                    message_id: "normal-final".to_owned(),
                    message: Box::new(final_message.clone()),
                },
                AgentEvent::MessageEnd {
                    message_id: "normal-final".to_owned(),
                    message: Box::new(final_message.clone()),
                },
                AgentEvent::TurnEnd {
                    message: Some(Box::new(final_message)),
                    tool_results: Vec::new(),
                },
                AgentEvent::AgentEnd,
            ] {
                events.send(event).await.expect("session event receiver");
            }
            RunCompletion::Completed(core)
        },
    );
    let session = Session::start(store, gateway, RunCore::new(), worker, executor_generation)
        .await
        .expect("session");
    let task = tokio::spawn(session.run());
    commands.send(user(1)).await.expect("command");
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if frames.lock().expect("frame mutex").iter().any(|frame| {
                matches!(frame, OutboundFrame::Event { envelope }
                    if envelope.event["type"] == "agent_end")
            }) || task.is_finished()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal frame");
    drop(commands);
    completed(task.await.expect("session join"));

    let row: (String, i64, String) = sqlx::query_as(
        "SELECT state, executor_generation, idempotency_key
         FROM tool_executions WHERE tool_call_id='normal-call'",
    )
    .fetch_one(&pool)
    .await
    .expect("tool audit row");
    assert_eq!(
        row,
        (
            "succeeded".to_owned(),
            executor_generation.as_i64(),
            "00000000-0000-4000-8000-000000000001/normal-call".to_owned(),
        )
    );
    let lifecycle: Vec<String> = frames
        .lock()
        .expect("frame mutex")
        .iter()
        .filter_map(|frame| match frame {
            OutboundFrame::Event { envelope }
                if matches!(
                    envelope.event["type"].as_str(),
                    Some("tool_execution_start" | "tool_execution_end")
                ) =>
            {
                envelope.event["type"].as_str().map(str::to_owned)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        lifecycle,
        vec!["tool_execution_start", "tool_execution_end"]
    );
}

#[tokio::test]
async fn normal_tool_lifecycle_is_prepared_started_finished_and_paired() {
    for generation in [0, MAX_PROCESS_GENERATION] {
        assert_normal_tool_lifecycle_persists_generation(
            ProcessGeneration::from_wire(generation).expect("valid boundary generation"),
        )
        .await;
    }
}

#[tokio::test]
async fn tool_execution_update_after_end_is_rejected_while_result_pairing_is_pending() {
    let store = Store::session_test_store("tool-update-after-end-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (gateway, commands, _frames) = gateway();
    let worker: Arc<dyn RunWorker> = Arc::new(
        |core: RunCore,
         initial: AdmittedCommand,
         _controls: mpsc::Receiver<RunControl>,
         events: mpsc::Sender<AgentEvent>| async move {
            emit_idle_injection(&events, &initial).await;
            let call = ToolCall {
                id: "ended-call".to_owned(),
                name: "fixture-tool".to_owned(),
                arguments: serde_json::from_value::<ValidatedToolArguments>(
                    serde_json::json!({"safe":true}),
                )
                .expect("validated arguments"),
            };
            let mut tool_use = match bridge_assistant(StopReason::ToolUse) {
                PublicMessage::Assistant(message) => message,
                _ => unreachable!(),
            };
            tool_use.content.push(PublicAssistantContent::ToolCall {
                tool_call: call.clone(),
                wire_item_index: 0,
            });
            let result = ToolResultMessage {
                tool_call_id: call.id.clone(),
                tool_name: call.name.clone(),
                content: Vec::new(),
                details: serde_json::json!({"ok":true}),
                is_error: false,
                timestamp: Utc::now(),
            };
            for event in [
                AgentEvent::MessageStart {
                    message_id: "ended-call-assistant".to_owned(),
                    message: Box::new(bridge_assistant(StopReason::Stop)),
                },
                AgentEvent::MessageEnd {
                    message_id: "ended-call-assistant".to_owned(),
                    message: Box::new(PublicMessage::Assistant(tool_use)),
                },
                AgentEvent::ToolExecutionStart {
                    tool_call_id: call.id.clone(),
                    tool_name: call.name,
                    args: serde_json::json!({"safe":true}),
                },
                AgentEvent::ToolExecutionEnd {
                    tool_call_id: call.id.clone(),
                    result: serde_json::to_value(result).expect("tool result"),
                    is_error: false,
                },
                AgentEvent::ToolExecutionUpdate {
                    tool_call_id: call.id,
                    partial: serde_json::json!({"late":true}),
                },
            ] {
                events.send(event).await.expect("session event receiver");
            }
            RunCompletion::Completed(core)
        },
    );
    let task = tokio::spawn(
        Session::start(
            store,
            gateway,
            RunCore::new(),
            worker,
            test_executor_generation(),
        )
        .await
        .expect("session")
        .run(),
    );
    commands.send(user(1)).await.expect("command");
    let (failure, ownership) = failed(task.await.expect("session join"));
    assert!(failure.to_string().contains("after ToolExecutionEnd"));
    assert!(matches!(ownership, RunOwnership::Lost));
    let state: String =
        sqlx::query_scalar("SELECT state FROM tool_executions WHERE tool_call_id='ended-call'")
            .fetch_one(&pool)
            .await
            .expect("running tool audit row");
    assert_eq!(state, "running");
}
