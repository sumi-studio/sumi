use std::{
    future::pending,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::{
    gateway::{CommandAck, CommandId},
    provider::types::{
        ApiProtocol, AssistantMessage, ProviderContextFragment, ProviderContextPayload,
        ProviderEvent, ProviderEventStream, ProviderOrigin, ProviderOutput, PublicAssistantContent,
        PublicAssistantMessage, PublicMessage, RejectedToolCall, StopReason, ToolArgumentError,
        ToolCall, ToolResultMessage, Usage, UserContent, UserMessage, ValidatedToolArguments,
    },
    store::{Store, user_message_id},
};

struct MockGateway {
    commands: mpsc::Receiver<InboundCommand>,
    frames: Arc<Mutex<Vec<OutboundFrame>>>,
    next_failure: Option<mpsc::Receiver<()>>,
    fail_send: Arc<AtomicBool>,
}

#[async_trait]
impl Gateway for MockGateway {
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

    async fn send(&mut self, frame: OutboundFrame) -> Result<()> {
        if self.fail_send.load(Ordering::SeqCst) {
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
    (
        MockGateway {
            commands,
            frames: frames.clone(),
            next_failure: None,
            fail_send,
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
}

fn controlled_gateway() -> ControlledGateway {
    let (commands_tx, commands) = mpsc::channel(40);
    let (next_failure_tx, next_failure) = mpsc::channel(1);
    let frames = Arc::new(Mutex::new(Vec::new()));
    let fail_send = Arc::new(AtomicBool::new(false));
    ControlledGateway {
        gateway: MockGateway {
            commands,
            frames: frames.clone(),
            next_failure: Some(next_failure),
            fail_send: fail_send.clone(),
        },
        commands: commands_tx,
        next_failure: next_failure_tx,
        fail_send,
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

#[derive(Clone, Copy)]
enum StaleBinding {
    PriorRun,
    PriorTurn,
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
                        .send(RunOutput {
                            binding,
                            event: AgentEvent::AgentStart,
                        })
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
                            .send(RunOutput {
                                binding: binding.clone(),
                                event,
                            })
                            .await
                            .expect("session output receiver");
                    }
                    let stale = binding.clone();
                    binding.turn_id = Uuid::now_v7().to_string();
                    events
                        .send(RunOutput {
                            binding,
                            event: AgentEvent::TurnStart,
                        })
                        .await
                        .expect("new turn output");
                    events
                        .send(RunOutput {
                            binding: stale,
                            event: AgentEvent::MessageStart {
                                message_id: "stale-prior-turn-output".to_owned(),
                                message: Box::new(bridge_assistant(StopReason::Stop)),
                            },
                        })
                        .await
                        .expect("stale turn output");
                }
            }
            RunCompletion::Completed(core)
        })
    }
}

#[tokio::test]
async fn session_rejects_worker_output_bound_to_a_stale_run_or_turn() {
    for (label, kind) in [
        ("run", StaleBinding::PriorRun),
        ("turn", StaleBinding::PriorTurn),
    ] {
        let store = Store::session_test_store(&format!("stale-worker-{label}"))
            .await
            .expect("test store");
        let pool = store.pool().clone();
        let (gateway, commands, _frames) = gateway();
        let session = Session::start(
            store,
            gateway,
            RunCore::new(),
            Arc::new(StaleBindingWorker(kind)),
        )
        .await
        .expect("session");
        let task = tokio::spawn(session.run());
        commands.send(user(1)).await.expect("command");
        let (failure, ownership) = failed(task.await.expect("session join"));
        assert!(matches!(failure, SessionFailure::Other(_)));
        assert!(matches!(ownership, RunOwnership::Recovered(_)));
        let durable_events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(&pool)
            .await
            .expect("event count");
        let expected_prefix = match kind {
            StaleBinding::PriorRun => 0,
            StaleBinding::PriorTurn => 8,
        };
        assert_eq!(
            durable_events, expected_prefix,
            "stale {label} output itself must not persist"
        );
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

    assert_eq!(received_acks(&frames).len(), 2, "fresh and stored ACK");
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert!(session.active.is_none(), "replay must not spawn a worker");
}

#[tokio::test]
async fn pending_worker_does_not_block_next_durable_received_ack() {
    let (gateway, commands, frames) = gateway();
    let (started_tx, mut started_rx) = mpsc::channel(1);
    let (control_tx, mut control_rx) = mpsc::channel(1);
    let (release_tx, release_rx) = oneshot::channel();
    let release = Arc::new(Mutex::new(Some(release_rx)));
    let worker: Arc<dyn RunWorker> = Arc::new({
        let release = release.clone();
        move |mut core: RunCore,
              initial: AdmittedCommand,
              mut controls: mpsc::Receiver<RunControl>,
              events: mpsc::Sender<AgentEvent>| {
            let started_tx = started_tx.clone();
            let control_tx = control_tx.clone();
            let release = release
                .lock()
                .expect("release mutex")
                .take()
                .expect("one worker");
            async move {
                let _events = events;
                started_tx
                    .send(initial.envelope().seq)
                    .await
                    .expect("started observer");
                let mut release = Box::pin(release);
                loop {
                    tokio::select! {
                        released = &mut release => {
                            released.expect("release signal");
                            controls.close();
                            while let Ok(RunControl::Command(command)) = controls.try_recv() {
                                core.queue_followup(command).expect("bounded follow-up");
                            }
                            core.mark_mutated();
                            return RunCompletion::Completed(core);
                        }
                        control = controls.recv() => {
                            let Some(RunControl::Command(command)) = control else {
                                return RunCompletion::Failed { core, failure: WorkerFailure::Cancelled };
                            };
                            core.queue_followup(command).expect("bounded follow-up");
                            let observed = core.next_followup().expect("one-at-a-time follow-up");
                            control_tx
                                .send(observed.envelope().seq)
                                .await
                                .expect("control observer");
                        }
                    }
                }
            }
        }
    });
    let task = tokio::spawn(session(gateway, worker).await.run());

    commands.send(user(1)).await.expect("first command");
    assert_eq!(started_rx.recv().await, Some(1));
    commands.send(user(2)).await.expect("second command");
    assert_eq!(control_rx.recv().await, Some(2));
    assert_eq!(
        received_acks(&frames)
            .into_iter()
            .map(|ack| ack.seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    release_tx.send(()).expect("release worker");
    tokio::task::yield_now().await;
    drop(commands);
    let core = completed(task.await.expect("session join"));
    assert_eq!(core.mutation_epoch(), 1);
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
    tokio::task::yield_now().await;
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
async fn closed_control_with_pending_worker_is_bounded_and_joined() {
    let (gateway, commands, _frames) = gateway();
    let running = Arc::new(AtomicBool::new(false));
    let (started_tx, mut started_rx) = mpsc::channel(1);
    let worker: Arc<dyn RunWorker> = Arc::new({
        let running = running.clone();
        move |_core: RunCore,
              _initial: AdmittedCommand,
              controls: mpsc::Receiver<RunControl>,
              events: mpsc::Sender<AgentEvent>| {
            let running = running.clone();
            let started_tx = started_tx.clone();
            drop(controls);
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
    commands.send(user(2)).await.expect("control command");

    let (failure, ownership) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        failed(task.await.expect("session join"))
    })
    .await
    .expect("closed control resolution is bounded");
    assert!(matches!(failure, SessionFailure::ControlChannelClosed));
    assert!(matches!(ownership, RunOwnership::Lost));
    assert!(
        !running.load(Ordering::SeqCst),
        "worker must be joined before return"
    );
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
async fn pending_t15_suffix_allows_only_t12_exact_retransmission() {
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
    let session = Session::start(store, gateway, RunCore::new(), worker)
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
    // proves the Session never resumes the gate for its T15-owned suffix.
}

struct CommitCheckingGateway {
    commands: mpsc::Receiver<InboundCommand>,
    pool: sqlx::SqlitePool,
    observed: Arc<Mutex<Vec<(u64, String)>>>,
}

#[async_trait]
impl Gateway for CommitCheckingGateway {
    async fn next_command(&mut self) -> Result<InboundCommand> {
        self.commands
            .recv()
            .await
            .ok_or_else(|| GatewayClosed.into())
    }

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

struct OpaqueContextDriver;

#[async_trait]
impl RunDriver for OpaqueContextDriver {
    async fn start_provider(
        &self,
        _attempt: usize,
        _context: &[PublicMessage],
        _cancel: CancellationToken,
    ) -> Result<ProviderAttempt> {
        let origin = ProviderOrigin {
            provider_instance_id: "opaque-fixture".to_owned(),
            protocol: ApiProtocol::OpenAiResponses,
            model: "bridge-model".to_owned(),
        };
        let message = AssistantMessage {
            content: Vec::new(),
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
        let (tx, rx) = mpsc::channel(2);
        tx.try_send(ProviderEvent::Start).expect("provider start");
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
        _active_context: &[PublicMessage],
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
    let session = Session::start(store, gateway, RunCore::new(), worker)
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

#[tokio::test]
async fn first_length_tool_call_is_durably_not_started_without_public_execution_lifecycle() {
    let store = Store::session_test_store("durable-length-session")
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
    let session = Session::start(store, gateway, RunCore::new(), worker)
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

    let rows: Vec<(String, String, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT tool_call_id, state, started_at, error_code FROM tool_executions")
            .fetch_all(&pool)
            .await
            .expect("not-started audit row");
    assert_eq!(
        rows,
        vec![(
            "length-call".to_owned(),
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
async fn rejected_tool_call_and_synthetic_result_commit_before_the_next_attempt() {
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
            let mut first = match bridge_assistant(StopReason::ToolUse) {
                PublicMessage::Assistant(message) => message,
                _ => unreachable!(),
            };
            first
                .content
                .push(PublicAssistantContent::RejectedToolCall {
                    rejected: rejected.clone(),
                    wire_item_index: 0,
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
                AgentEvent::TurnEnd {
                    message: Some(Box::new(first)),
                    tool_results: Vec::new(),
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
    let session = Session::start(store, gateway, RunCore::new(), worker)
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
        "SELECT id, role FROM messages WHERE id IN ('rejected-assistant','rejected-result','post-rejection-attempt') ORDER BY seq",
    )
    .fetch_all(&pool)
    .await
    .expect("rejection pair and next attempt");
    assert_eq!(
        stored,
        vec![
            ("rejected-assistant".to_owned(), "assistant".to_owned()),
            ("rejected-result".to_owned(), "tool_result".to_owned()),
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
    let executions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tool_executions")
        .fetch_one(&pool)
        .await
        .expect("no rejected execution lifecycle");
    assert_eq!(executions, 0);
}

#[tokio::test]
async fn failed_idle_injection_batch_publishes_no_partial_event_frame() {
    let store = Store::session_test_store("durable-rollback-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (gateway, commands, frames) = gateway();
    let worker: Arc<dyn RunWorker> = Arc::new(
        |core: RunCore,
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
            RunCompletion::Completed(core)
        },
    );
    let session = Session::start(store, gateway, RunCore::new(), worker)
        .await
        .expect("session");
    let task = tokio::spawn(session.run());
    commands.send(user(1)).await.expect("command");
    let (failure, ownership) = failed(task.await.expect("session join"));
    assert!(matches!(failure, SessionFailure::Other(_)));
    assert!(matches!(ownership, RunOwnership::Recovered(_)));
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
    let session = Session::start(store, gateway, RunCore::new(), worker)
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
async fn opaque_context_refusal_closes_durably_before_applied_ack() {
    let store = Store::session_test_store("durable-opaque-refusal-session")
        .await
        .expect("test store");
    let pool = store.pool().clone();
    let (gateway, commands, frames) = gateway();
    let worker: Arc<dyn RunWorker> =
        Arc::new(SequentialRunWorker::new(Arc::new(OpaqueContextDriver)));
    let session = Session::start(store, gateway, RunCore::new(), worker)
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
        Some(PublicMessage::User(_))
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

#[tokio::test]
async fn normal_tool_lifecycle_is_prepared_started_finished_and_paired() {
    let store = Store::session_test_store("durable-normal-tool-session")
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
    let session = Session::start(store, gateway, RunCore::new(), worker)
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

    let state: String =
        sqlx::query_scalar("SELECT state FROM tool_executions WHERE tool_call_id='normal-call'")
            .fetch_one(&pool)
            .await
            .expect("tool audit row");
    assert_eq!(state, "succeeded");
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
