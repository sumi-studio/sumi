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

use super::*;
use crate::{
    gateway::{CommandAck, CommandId},
    store::Store,
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

#[tokio::test]
async fn active_received_replay_acks_without_duplicate_control_delivery() {
    let (gateway, _commands, frames) = gateway();
    let starts = Arc::new(AtomicUsize::new(0));
    let (release_tx, release_rx) = oneshot::channel();
    let release = Arc::new(Mutex::new(Some(release_rx)));
    let worker: Arc<dyn RunWorker> = Arc::new({
        let starts = starts.clone();
        move |core: RunCore,
              _initial: CommandEnvelope,
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
              _initial: CommandEnvelope,
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
              initial: CommandEnvelope,
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
                    .send(initial.seq)
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
                            control_tx.send(observed.seq).await.expect("control observer");
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
              initial: CommandEnvelope,
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
                started_tx.send(initial.seq).await.expect("start observer");
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
        frames
            .lock()
            .expect("frame mutex")
            .iter()
            .any(|frame| matches!(
                frame,
                OutboundFrame::Event { envelope } if envelope.event["type"] == "agent_start"
            ))
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
                  _initial: CommandEnvelope,
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
              _initial: CommandEnvelope,
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
              _initial: CommandEnvelope,
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
                events
                    .send(AgentEvent::AgentStart)
                    .await
                    .expect("event receiver");
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
              _initial: CommandEnvelope,
              mut controls: mpsc::Receiver<RunControl>,
              events: mpsc::Sender<AgentEvent>| {
            let release = release.clone();
            let started_tx = started_tx.clone();
            async move {
                started_tx.send(()).await.expect("start observer");
                release.notified().await;
                events
                    .try_send(AgentEvent::AgentStart)
                    .expect("event capacity");
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
              _initial: CommandEnvelope,
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
         _initial: CommandEnvelope,
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
         _initial: CommandEnvelope,
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
         _initial: CommandEnvelope,
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
         _initial: CommandEnvelope,
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
