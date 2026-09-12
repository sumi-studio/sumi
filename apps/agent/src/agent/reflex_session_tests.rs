use super::*;
use crate::agent::reflex::{ReflexDecision, ReflexError, ReflexLimits};
use crate::provider::types::ParentContextSnapshot;

struct GatedReflexWorker {
    snapshot: ParentContextSnapshot,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    started: mpsc::UnboundedSender<AdmittedCommand>,
    decision: ReflexDecision,
}

impl RunWorker for GatedReflexWorker {
    fn validate_executor_generation(&self, _: ProcessGeneration) -> Result<()> {
        Ok(())
    }
    fn latest_reflex_snapshot(&self) -> Option<ParentContextSnapshot> {
        Some(self.snapshot.clone())
    }
    fn idle_reflex_snapshot<'a>(
        &'a self,
        _: &'a RunCore,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ParentContextSnapshot>>> + Send + 'a>> {
        Box::pin(async { Ok(Some(self.snapshot.clone())) })
    }
    fn evaluate_reflex<'a>(
        &'a self,
        parent: &'a ParentContextSnapshot,
        event: &'a UserMessage,
        _: ReflexLimits,
        _: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<ReflexDecision, ReflexError>> + Send + 'a>> {
        Box::pin(async move {
            assert_eq!(parent, &self.snapshot);
            assert_eq!(
                event.incoming_source,
                Some(crate::gateway::test_messaging_provenance())
            );
            assert_eq!(
                event.content,
                vec![UserContent::Text {
                    text: "Exact original notification".into()
                }]
            );
            self.entered.notify_one();
            self.release.notified().await;
            Ok(self.decision.clone())
        })
    }
    fn run(
        &self,
        _: RunCore,
        initial: AdmittedCommand,
        _: mpsc::Receiver<RunControl>,
        _: mpsc::Sender<RunOutput>,
    ) -> WorkerFuture {
        self.started.send(initial).unwrap();
        Box::pin(pending())
    }
}

fn gated_reflex_worker(
    decision: ReflexDecision,
) -> (
    Arc<GatedReflexWorker>,
    mpsc::UnboundedReceiver<AdmittedCommand>,
) {
    let prompt = PromptContext::new("same parent system".into(), vec![], vec![], vec![], vec![]);
    let snapshot = ParentContextSnapshot::capture(
        &prompt,
        &ModelSpec::preset("openai-responses").unwrap(),
        &RequestOptions::default(),
    );
    let (started, received) = mpsc::unbounded_channel();
    (
        Arc::new(GatedReflexWorker {
            snapshot,
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            started,
            decision,
        }),
        received,
    )
}

fn reflex_notification(seq: u64) -> InboundCommand {
    let InboundCommand::Valid(mut command) = user(seq) else {
        unreachable!()
    };
    command.provenance = crate::gateway::test_messaging_provenance();
    command.command = Command::ExternalEvent {
        content: "Exact original notification".into(),
    };
    InboundCommand::Valid(command)
}

#[tokio::test]
async fn soft_reflex_waiting_before_assistant_start_does_not_interrupt_generation() {
    let store = Store::session_test_store("soft-reflex-assistant-start")
        .await
        .unwrap();
    let pool = store.pool().clone();
    let (gateway, _commands, _) = gateway();
    let driver = Arc::new(SaturatedTerminalTailDriver::new());
    let mut session = Session::start(
        store,
        gateway,
        RunCore::fixture_with_unapproved_tools(),
        Arc::new(SequentialRunWorker::new(driver.clone())),
        test_executor_generation(),
    )
    .await
    .unwrap();
    // Both notifications belong to the same Messaging audience. Keep the first
    // provider running, with its lifecycle events not yet observed by Session.
    session
        .admit_and_route(reflex_notification(1))
        .await
        .unwrap();
    let incoming = reflex_notification(2);
    let receipt = session
        .admission
        .receive_with_origin(&session.writer, &incoming)
        .await
        .unwrap();
    let InboundCommand::Valid(envelope) = incoming else {
        unreachable!()
    };
    // Reuse the durable assessment path: the restored decision must be soft,
    // even when assistant_start later makes a hard steer mechanically possible.
    session
        .writer
        .store()
        .record_reflex_decision(
            &envelope,
            ReflexDecision::Soft {
                interpretation: None,
            },
        )
        .await
        .unwrap();
    assert!(
        session
            .begin_reflex(
                AdmittedCommand::new(envelope, receipt.received_at)
                    .with_incoming_timing(receipt.incoming_timing),
            )
            .await
            .unwrap()
    );
    let result = tokio::time::timeout(Duration::from_secs(3), session.reflex_jobs.join_next())
        .await
        .unwrap();
    session.finish_reflex(result).await.unwrap();
    assert_eq!(session.deferred_commands.len(), 1);

    tokio::time::timeout(Duration::from_secs(3), async {
        while !session
            .active
            .as_ref()
            .unwrap()
            .bridge
            .steer_stage()
            .eq(&SteerStage::AssistantGeneration)
        {
            let output = session
                .active
                .as_mut()
                .unwrap()
                .events_rx
                .recv()
                .await
                .unwrap();
            session.persist_active_event(output).await.unwrap();
        }
    })
    .await
    .unwrap();
    driver.release_terminal_tail.notify_one();
    tokio::time::timeout(Duration::from_secs(3), async {
        while session.active.is_some() {
            drive_active_to_completion(&mut session).await.unwrap();
        }
    })
    .await
    .unwrap();
    session.wait_outbound_idle().await;
    let states: Vec<(i64, String, String)> =
        sqlx::query_as("SELECT seq, application_kind, status FROM inbound_commands ORDER BY seq")
            .fetch_all(&pool)
            .await
            .unwrap();
    session.abort_writer().await;
    assert_eq!(
        states,
        vec![
            (1, "idle_run".into(), "applied".into()),
            (2, "idle_run".into(), "applied".into()),
        ],
        "a soft notification must wait for the current response, never turn into a hard steer"
    );
    let first: (String, i64, String) = sqlx::query_as(
        "SELECT json_extract(payload, '$.stop_reason'), interrupted, json_extract(payload, '$.content[0].text') FROM messages WHERE id='saturated-terminal-tail-assistant-0'"
    ).fetch_one(&pool).await.unwrap();
    assert_eq!(
        first,
        (
            "stop".into(),
            0,
            (0..61).map(|n| format!("{n:02}")).collect::<String>()
        )
    );
    assert_eq!(driver.starts.load(Ordering::SeqCst), 2);
}

async fn stop_reflex_fixture(session: &mut Session<MockGateway>) {
    session.reflex_jobs.abort_all();
    if let Some(active) = session.active.take() {
        active.join.abort();
    }
    session.abort_writer().await;
}

#[tokio::test]
async fn pending_reflex_does_not_block_direct_human_input() {
    let (worker, mut started) = gated_reflex_worker(ReflexDecision::Hard {
        interpretation: None,
    });
    let (gateway, _commands, _) = gateway();
    let mut session = session(gateway, worker.clone()).await;
    session
        .admit_and_route(reflex_notification(1))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker.entered.notified())
        .await
        .unwrap();
    assert!(session.active.is_none());
    let assessed_epoch = session.reflex_epoch;
    tokio::time::timeout(Duration::from_secs(2), session.admit_and_route(user(2)))
        .await
        .unwrap()
        .unwrap();
    let actual = started.recv().await.unwrap();
    assert_eq!(actual.envelope().seq, 2);
    assert_ne!(
        session.reflex_epoch, assessed_epoch,
        "new input invalidates stale hard advice before a new snapshot exists"
    );
    assert_eq!(session.reflex_pending.len(), 1);
    stop_reflex_fixture(&mut session).await;
}

#[tokio::test]
async fn deferred_reflex_delivers_original_once_after_durable_deadline() {
    let (worker, mut started) = gated_reflex_worker(ReflexDecision::Defer {
        delay_ms: 100,
        interpretation: Some("This can wait until the current response is finished".into()),
    });
    let (gateway, _commands, _) = gateway();
    let mut session = session(gateway, worker.clone()).await;
    session
        .admit_and_route(reflex_notification(1))
        .await
        .unwrap();
    worker.entered.notified().await;
    worker.release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(2), session.reflex_jobs.join_next())
        .await
        .unwrap();
    let stored = session
        .writer
        .store()
        .pending_reflex_commands()
        .await
        .unwrap();
    assert_eq!(stored.len(), 1);
    assert!(stored[0].1.ready_at_ms <= Utc::now().timestamp_millis());
    assert!(started.try_recv().is_err());
    session.finish_reflex(result).await.unwrap();
    let actual = started.recv().await.unwrap();
    assert_eq!(actual.envelope(), &stored[0].0);
    let PublicMessage::User(rendered) = steer::build_user_message(&actual).unwrap() else {
        unreachable!()
    };
    let UserContent::Text { text } = &rendered.content[0] else {
        unreachable!()
    };
    assert!(text.starts_with("Exact original notification\n\n"));
    assert!(text.contains("This can wait until the current response is finished"));
    assert!(started.try_recv().is_err());
    assert!(session.reflex_pending.is_empty());
    stop_reflex_fixture(&mut session).await;
}

#[tokio::test]
async fn abort_during_reflex_wait_does_not_revive_superseded_notification() {
    let (worker, mut started) = gated_reflex_worker(ReflexDecision::Soft {
        interpretation: None,
    });
    let (gateway, _commands, _) = gateway();
    let mut session = session(gateway, worker.clone()).await;
    session
        .admit_and_route(reflex_notification(1))
        .await
        .unwrap();
    worker.entered.notified().await;
    session.admit_and_route(abort(2)).await.unwrap();
    worker.release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(2), session.reflex_jobs.join_next())
        .await
        .unwrap();
    session.finish_reflex(result).await.unwrap();
    assert!(session.active.is_none());
    assert!(started.try_recv().is_err());
    assert!(session.reflex_pending.is_empty());
    stop_reflex_fixture(&mut session).await;
}

#[tokio::test]
async fn stalled_assessment_delivers_original_after_deadline() {
    let (worker, mut started) = gated_reflex_worker(ReflexDecision::Hard {
        interpretation: None,
    });
    let (gateway, _commands, _) = gateway();
    let mut session = session(gateway, worker.clone()).await;
    session
        .admit_and_route(reflex_notification(1))
        .await
        .unwrap();
    worker.entered.notified().await;
    tokio::time::pause();
    tokio::time::advance(crate::agent::reflex_gate::ASSESSMENT_TIMEOUT).await;
    tokio::time::resume();
    let result = session.reflex_jobs.join_next().await;
    session.finish_reflex(result).await.unwrap();
    let actual = started.recv().await.unwrap();
    assert_eq!(actual.envelope().seq, 1);
    assert!(actual.event_interpretation.is_none());
    assert!(session.reflex_pending.is_empty());
    stop_reflex_fixture(&mut session).await;
}
