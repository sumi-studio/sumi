use super::*;
use crate::agent::run::BoundToolResult;
use crate::approval::authority::AuthorizedBoundInvocation;
use crate::approval::{
    route_broker::RouteApprovalBroker, route_policy::RoutePolicy, route_reviewer::*,
};
use crate::tools::*;
use serde_json::json;

struct Review {
    model: crate::approval::route_reviewer::ReviewerModelSpec,
    seen: AtomicUsize,
}
#[async_trait]
impl ExecutionReviewerTransport for Review {
    fn model_spec(&self) -> &crate::approval::route_reviewer::ReviewerModelSpec {
        &self.model
    }
    async fn complete(
        &self,
        _prompt: &ExecutionReviewerPrompt,
        _: usize,
        _: ReviewerAttemptTrace,
        _: CancellationToken,
    ) -> std::result::Result<
        ReviewerTransportOutput,
        crate::approval::route_reviewer::ReviewerTransportError,
    > {
        self.seen.fetch_add(1, Ordering::SeqCst);
        Ok(ReviewerTransportOutput { text: r#"{"outcome":"allow","risk":"low","rationale":"read the artifact after the unknown write"}"#.to_owned(), tool_trace: vec![] })
    }
}
#[async_trait]
impl EscalationReviewerTransport for Review {
    fn model_spec(&self) -> &crate::approval::route_reviewer::ReviewerModelSpec {
        &self.model
    }
    async fn complete(
        &self,
        _: &EscalationReviewerPrompt,
        _: usize,
        _: ReviewerAttemptTrace,
        _: CancellationToken,
    ) -> std::result::Result<
        ReviewerTransportOutput,
        crate::approval::route_reviewer::ReviewerTransportError,
    > {
        Ok(ReviewerTransportOutput { text: r#"{"outcome":"ask_human","risk":"low","misunderstanding":null,"rationale":"explicit elevated test action"}"#.into(), tool_trace: vec![] })
    }
}

struct Artifact {
    path: std::path::PathBuf,
    reads: AtomicUsize,
    write: bool,
    entered: Notify,
    hold: AtomicBool,
}
#[async_trait]
impl Tool for Artifact {
    fn def(&self) -> ToolDefinition {
        ToolDefinition {
            name: if self.write {
                "write_artifact"
            } else {
                "check_artifact"
            }
            .to_owned(),
            description: "Read the existing artifact".to_owned(),
            parameters: json!({"type":"object","properties":{},"additionalProperties":false}),
        }
    }
    fn risk(&self) -> ToolRisk {
        if self.write {
            ToolRisk::Mutating
        } else {
            ToolRisk::ReadOnly
        }
    }
    fn bound_adapter(self: Arc<Self>) -> Option<Arc<dyn BoundToolAdapter>> {
        Some(self)
    }
    async fn execute(&self, _: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
        panic!("raw execution is forbidden")
    }
}
#[async_trait]
impl BoundToolAdapter for Artifact {
    fn identity(&self) -> AdapterIdentity {
        AdapterIdentity::new("sumi.recovery_read", 1).unwrap()
    }
    async fn bind(&self, _: ToolBindCtx<'_>) -> std::result::Result<ToolBinding, DescribeError> {
        Ok(ToolBinding::new(
            AppActionDescriptor::new(
                if self.write {
                    "artifact.write"
                } else {
                    "artifact.read"
                },
                if self.write {
                    CapabilityClass::Mutate
                } else {
                    CapabilityClass::Read
                },
                vec![ResourceScope::resource("workspace", "file", "artifact.txt")],
            )?,
            crate::tools::ReviewProjection::from_value(json!({"path":"artifact.txt"}))?,
            BoundExecutionArguments::from_value(json!({}))?,
        ))
    }
    async fn execute(&self, ctx: BoundToolCtx<'_>) -> Result<BoundToolExecutionOutcome, ToolError> {
        let receipt = ctx
            .committed_effect_permit
            .begin_local_effect()
            .complete(|| async {
                self.reads.fetch_add(1, Ordering::SeqCst);
                if self.write {
                    std::fs::write(&self.path, "one committed write").unwrap();
                    self.entered.notify_one();
                    if self.hold.load(Ordering::SeqCst) {
                        std::future::pending::<()>().await;
                    }
                }
                let text =
                    std::fs::read_to_string(&self.path).unwrap_or_else(|_| "not written".into());
                Ok::<_, ToolError>(text_output(text, json!({"path":"artifact.txt"})))
            })
            .await?;
        Ok(BoundToolExecutionOutcome::without_live_post_commit(receipt))
    }
}

struct Driver {
    identity: RpcIdentity,
    registry: Arc<ToolRegistry>,
    workspace: WorkspacePaths,
    contexts: Arc<Mutex<Vec<Vec<ContextMessage>>>>,
    resume: bool,
    hold_final: Arc<AtomicBool>,
}
#[async_trait]
impl RunDriver for Driver {
    fn validate_executor_generation(&self, generation: ProcessGeneration) -> Result<()> {
        assert_eq!(generation, self.identity.generation());
        Ok(())
    }
    fn validate_runtime_identity(&self, identity: &RpcIdentity) -> Result<()> {
        assert_eq!(identity, &self.identity);
        Ok(())
    }
    async fn start_provider_for_command(
        &self,
        _: usize,
        context: &[ContextMessage],
        _: Option<Instant>,
        cancel: CancellationToken,
    ) -> Result<ProviderAttempt> {
        let count = {
            let mut seen = self.contexts.lock().unwrap();
            seen.push(context.to_vec());
            seen.len()
        };
        if self.resume {
            return Ok(provider_attempt_stop(900 + count));
        }
        match count {
            1 => Ok(provider_attempt_from_tool_call(
                801,
                ToolCall {
                    id: "elevated-write".into(),
                    provider_call_id: None,
                    name: "write_artifact".into(),
                    route: crate::provider::types::ToolInvocationRoute::Elevated,
                    arguments: serde_json::from_value(json!({})).unwrap(),
                },
            )),
            2 => {
                let receipt = context
                    .iter()
                    .find_map(|entry| {
                        match crate::memory::overflow::context_message_to_public(entry) {
                            PublicMessage::ToolResult(result)
                                if result.tool_name == "write_artifact" =>
                            {
                                Some(result)
                            }
                            _ => None,
                        }
                    })
                    .expect("model sees a normal pending ToolResult before independent read");
                assert!(!receipt.is_error);
                assert_eq!(receipt.details["status"], "awaiting_approval");
                assert_eq!(receipt.details["executed"], false);
                assert!(receipt.details["operation_id"].is_string());
                Ok(provider_attempt_from_tool_call(
                    802,
                    ToolCall {
                        id: "independent-read".into(),
                        provider_call_id: None,
                        name: "check_artifact".into(),
                        route: crate::provider::types::ToolInvocationRoute::Normal,
                        arguments: serde_json::from_value(json!({})).unwrap(),
                    },
                ))
            }
            3 => {
                assert!(context.iter().any(|entry| matches!(crate::memory::overflow::context_message_to_public(entry), PublicMessage::ToolResult(result) if result.tool_name=="check_artifact" && !result.is_error)));
                if self.hold_final.load(Ordering::SeqCst) {
                    let (tx, rx) = mpsc::channel(8);
                    tx.try_send(ProviderEvent::Start)?;
                    tx.try_send(ProviderEvent::TextStart { content_index: 0 })?;
                    tx.try_send(ProviderEvent::TextDelta {
                        content_index: 0,
                        delta: "draft-before-new-input".into(),
                    })?;
                    let end = cancel.clone();
                    tokio::spawn(async move {
                        end.cancelled().await;
                        drop(tx);
                    });
                    Ok(ProviderAttempt {
                        message_id: "assistant-803".into(),
                        initial_message: public_initial_message(),
                        uncalibrated_prompt_estimate: 0,
                        events: ProviderEventStream::new(rx, cancel, "fixture", fixture_origin()),
                    })
                } else {
                    Ok(provider_attempt_stop(803))
                }
            }
            _ => Ok(provider_attempt_stop(800 + count)),
        }
    }

    async fn bind_tool_invocation(
        &self,
        flow: &str,
        call: &ToolCall,
    ) -> std::result::Result<SealedBoundToolInvocation, DescribeError> {
        self.registry.bind(call, flow, &self.workspace).await
    }
    async fn execute_bound_tool_observed(
        &self,
        invocation: AuthorizedBoundInvocation,
        cancel: CancellationToken,
        update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> std::result::Result<BoundToolResult, BoundExecutionError> {
        let id = invocation.tool_call_id().to_owned();
        let name = invocation.tool_name().to_owned();
        let output = self
            .registry
            .execute_bound(invocation, cancel, update)
            .await?;
        Ok(BoundToolResult {
            result: ToolResultMessage {
                tool_call_id: id,
                provider_call_id: None,
                tool_name: name,
                content: output.output.content,
                details: output.output.details,
                is_error: output.output.is_error,
                timestamp: chrono::Utc::now(),
            },
            live_post_commit: output.live_post_commit,
        })
    }
    async fn execute_tool_observed(
        &self,
        _: &str,
        _: &ToolCall,
        _: CancellationToken,
        _: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<ToolResultMessage, ToolError> {
        panic!("raw unbound tool path is forbidden")
    }
    fn synthetic_error(&self, message: &str) -> PublicMessage {
        panic!("unexpected recovery failure: {message}")
    }
    async fn plan_overflow_recovery(
        &self,
        _: &RunCore,
        _: OverflowRecoveryRequest,
        _: &[ContextMessage],
    ) -> Result<OverflowRecoveryOutcome> {
        bail!("no overflow in fixture")
    }
}

fn broker() -> Arc<RouteApprovalBroker> {
    let model = crate::approval::route_reviewer::ReviewerModelSpec::new(
        "receipt",
        "fixture",
        "https://reviewer.invalid",
        "account",
        "trust",
        "policy",
    );
    let review = Arc::new(Review {
        model: model.clone(),
        seen: AtomicUsize::new(0),
    });
    let trust = crate::approval::route_reviewer::ReviewerTrustSet::new(vec![model.clone()]);
    Arc::new(RouteApprovalBroker::new(
        RoutePolicy::baseline_only_v1(),
        Redactor::v1(),
        Arc::new(
            ExecutionReviewer::new(
                model.clone(),
                trust.clone(),
                review.clone(),
                ReviewerBudgetV1::execution(),
            )
            .unwrap(),
        ),
        Arc::new(
            EscalationReviewer::new(model, trust, review, ReviewerBudgetV1::escalation()).unwrap(),
        ),
    ))
}

struct Fixture {
    store: Arc<Store>,
    directory: std::path::PathBuf,
    write: Arc<Artifact>,
    read: Arc<Artifact>,
    contexts: Arc<Mutex<Vec<Vec<ContextMessage>>>>,
    hold_final: Arc<AtomicBool>,
}
impl Fixture {
    async fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("sumi-receipt-e2e-{}", Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        let make = |write| {
            Arc::new(Artifact {
                path: directory.join("artifact.txt"),
                reads: AtomicUsize::new(0),
                write,
                entered: Notify::new(),
                hold: AtomicBool::new(false),
            })
        };
        Self {
            store: Arc::new(
                Store::session_test_store("approval-receipt-e2e")
                    .await
                    .unwrap(),
            ),
            write: make(true),
            read: make(false),
            contexts: Arc::new(Mutex::new(vec![])),
            hold_final: Arc::new(AtomicBool::new(false)),
            directory,
        }
    }
    async fn start(
        &self,
        generation: u64,
        resume: bool,
    ) -> (
        tokio::task::JoinHandle<SessionResult>,
        mpsc::Sender<InboundCommand>,
        Arc<Mutex<Vec<OutboundFrame>>>,
    ) {
        let authority = queued_recovery_authority(&self.store, generation);
        let mut recovery_rounds = 0;
        let hydrated = loop {
            recovery_rounds += 1;
            assert!(
                recovery_rounds <= 8,
                "boot recovery must make bounded progress"
            );
            match self
                .store
                .hydrate(authority.lease(), authority.fence())
                .await
                .unwrap()
            {
                HydrationOutcome::Complete(hydrated) => break hydrated,
                HydrationOutcome::LogicalRecoveryRequired { steps } => {
                    if let Err(error) = crate::store::LogicalRecoveryExecutor
                        .execute(&self.store, &steps, authority.lease(), authority.fence())
                        .await
                    {
                        let commands: Vec<(i64,String,String,Option<String>,Option<String>)> = sqlx::query_as("SELECT seq,status,run_phase,run_id,turn_id FROM inbound_commands ORDER BY seq").fetch_all(self.store.pool()).await.unwrap();
                        let events: Vec<(i64,String,String,String)> = sqlx::query_as("SELECT seq,event_type,internal_metadata,envelope FROM agent_events WHERE event_type IN ('turn_start','turn_end','agent_end') ORDER BY seq DESC LIMIT 12").fetch_all(self.store.pool()).await.unwrap();
                        panic!(
                            "logical recovery failed: {error}; steps={steps:?}; commands={commands:?}; turn_events={events:?}"
                        );
                    }
                }
                other => panic!("unexpected receipt hydration outcome: {other:?}"),
            }
        };
        let (core, start) =
            SessionStartAuthority::from_hydrated(authority.clone(), &hydrated, broker()).unwrap();
        let mut registry = ToolRegistryBuilder::default();
        registry.register(self.write.clone()).unwrap();
        registry.register(self.read.clone()).unwrap();
        let driver = Arc::new(Driver {
            identity: authority.rpc_identity().clone(),
            registry: Arc::new(registry.build()),
            workspace: WorkspacePaths::new(&self.directory).unwrap(),
            contexts: self.contexts.clone(),
            resume,
            hold_final: self.hold_final.clone(),
        });
        let (gateway, commands, frames) = gateway();
        let session = Session::start_hydrated(
            self.store.as_ref().clone(),
            gateway,
            core,
            Arc::new(SequentialRunWorker::new(driver)),
            start,
        )
        .await
        .unwrap();
        (tokio::spawn(session.run()), commands, frames)
    }
    async fn scalar(&self, sql: &str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(self.store.pool())
            .await
            .unwrap()
    }
    async fn pending_id(&self) -> String {
        sqlx::query_scalar("SELECT id FROM approval_log WHERE state='pending'")
            .fetch_one(self.store.pool())
            .await
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

async fn wait_until(mut condition: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition().await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("receipt E2E condition reached");
}

#[tokio::test]
async fn elevated_receipt_allows_independent_read_and_idle_then_one_typed_outcome() {
    let fixture = Fixture::new().await;
    let (task, commands, _frames) = fixture.start(73, false).await;
    commands.send(user(1)).await.unwrap();
    wait_until(async || {
        fixture
            .scalar("SELECT COUNT(*) FROM messages WHERE id='assistant-803'")
            .await
            == 1
    })
    .await;
    assert!(
        !task.is_finished(),
        "Session stays available while operation awaits approval"
    );
    assert_eq!(fixture.read.reads.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.write.reads.load(Ordering::SeqCst), 0);
    assert!(!fixture.directory.join("artifact.txt").exists());
    let id = fixture.pending_id().await;
    assert_eq!(fixture.scalar("SELECT COUNT(*) FROM messages WHERE role='tool_result' AND json_extract(payload,'$.tool_name')='write_artifact'").await,1);
    commands.send(approval_decision(2, &id)).await.unwrap();
    wait_until(async || {
        fixture
            .scalar(
                "SELECT COUNT(*) FROM agent_events WHERE event_type='approval_operation_outcome'",
            )
            .await
            == 1
    })
    .await;
    wait_until(async || fixture.contexts.lock().unwrap().len() >= 4).await;
    {
        let contexts = fixture.contexts.lock().unwrap();
        let receipt_input = &contexts[1];
        let read_completed_input = &contexts[2];
        let outcome_input = &contexts[3];
        assert!(
            read_completed_input.starts_with(receipt_input),
            "ordinary read appends without rewriting the receipt prefix"
        );
        assert!(
            outcome_input.starts_with(read_completed_input),
            "operation outcome appends without rewriting earlier canonical input"
        );
        let receipt_position = receipt_input.iter().position(|entry| matches!(crate::memory::overflow::context_message_to_public(entry), PublicMessage::ToolResult(result) if result.tool_name == "write_artifact")).unwrap();
        assert_eq!(
            outcome_input[receipt_position], receipt_input[receipt_position],
            "pending receipt remains exact at its original sequence position"
        );
        assert!(outcome_input.iter().skip(read_completed_input.len()).any(|entry| match crate::memory::overflow::context_message_to_public(entry) {
            PublicMessage::User(message) => message.incoming_source.as_ref().is_some_and(|source| matches!(source.source(), crate::runtime::contracts::IncomingSource::ApprovalOperation(operation) if operation.operation_id == id)),
            _ => false,
        }), "the next model input actually receives the typed completion event");
    }
    assert_eq!(fixture.write.reads.load(Ordering::SeqCst), 1);
    assert_eq!(
        std::fs::read_to_string(fixture.directory.join("artifact.txt")).unwrap(),
        "one committed write"
    );
    assert_eq!(fixture.scalar("SELECT COUNT(*) FROM messages WHERE role='tool_result' AND json_extract(payload,'$.tool_name')='write_artifact'").await,1, "completion must not fabricate a second result for the original provider call");
    let outcome: String = sqlx::query_scalar(
        "SELECT envelope FROM agent_events WHERE event_type='approval_operation_outcome'",
    )
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    let outcome: Value = serde_json::from_str(&outcome).unwrap();
    assert_eq!(outcome["operation_id"], id);
    assert_eq!(outcome["status"], "succeeded");
    assert_eq!(outcome["executed"], true);
    let source = crate::runtime::contracts::ApprovalOperationSource {
        operation_id: id.clone(),
        tool_call_id: outcome["tool_call_id"].as_str().unwrap().into(),
        status: crate::runtime::contracts::ApprovalOperationStatus::Succeeded,
        executed: Some(true),
        result: outcome["result"].clone(),
    };
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM messages WHERE id=?")
            .bind(source.message_id())
            .fetch_one(fixture.store.pool())
            .await
            .unwrap(),
        1
    );
    commands.send(approval_decision(3, &id)).await.unwrap();
    wait_until(async || {
        fixture
            .scalar("SELECT COUNT(*) FROM inbound_commands WHERE seq=3 AND status!='received'")
            .await
            == 1
    })
    .await;
    assert_eq!(fixture.write.reads.load(Ordering::SeqCst), 1);
    task.abort();
    let _ = task.await;
    drop(commands);
    let (restarted, commands, _) = fixture.start(74, true).await;
    commands.send(approval_decision(4, &id)).await.unwrap();
    wait_until(async || {
        fixture
            .scalar("SELECT COUNT(*) FROM inbound_commands WHERE seq=4 AND status!='received'")
            .await
            == 1
    })
    .await;
    assert_eq!(fixture.write.reads.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .scalar(
                "SELECT COUNT(*) FROM agent_events WHERE event_type='approval_operation_outcome'"
            )
            .await,
        1
    );
    restarted.abort();
    let _ = restarted.await;
}

#[tokio::test]
async fn pending_receipt_survives_restart_and_grant_executes_once() {
    let fixture = Fixture::new().await;
    let (task, commands, _) = fixture.start(73, false).await;
    commands.send(user(1)).await.unwrap();
    wait_until(async || {
        fixture
            .scalar("SELECT COUNT(*) FROM messages WHERE id='assistant-803'")
            .await
            == 1
    })
    .await;
    let id = fixture.pending_id().await;
    task.abort();
    let _ = task.await;
    drop(commands);
    let (task, commands, _) = fixture.start(74, true).await;
    commands.send(approval_decision(2, &id)).await.unwrap();
    wait_until(async || {
        fixture
            .scalar(
                "SELECT COUNT(*) FROM agent_events WHERE event_type='approval_operation_outcome'",
            )
            .await
            == 1
    })
    .await;
    assert_eq!(fixture.write.reads.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.read.reads.load(Ordering::SeqCst),
        1,
        "restart did not replay the independent read"
    );
    assert_eq!(fixture.scalar("SELECT COUNT(*) FROM messages WHERE role='tool_result' AND json_extract(payload,'$.tool_name')='write_artifact'").await,1);
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn crash_after_receipted_effect_start_reports_unknown_without_repeating_effect() {
    let fixture = Fixture::new().await;
    fixture.write.hold.store(true, Ordering::SeqCst);
    let (task, commands, _) = fixture.start(73, false).await;
    commands.send(user(1)).await.unwrap();
    wait_until(async || {
        fixture
            .scalar("SELECT COUNT(*) FROM messages WHERE id='assistant-803'")
            .await
            == 1
    })
    .await;
    let id = fixture.pending_id().await;
    commands.send(approval_decision(2, &id)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), fixture.write.entered.notified())
        .await
        .unwrap();
    assert_eq!(fixture.write.reads.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture
            .scalar("SELECT COUNT(*) FROM tool_executions WHERE state='running'")
            .await,
        1
    );
    task.abort();
    let _ = task.await;
    drop(commands);
    let authority = queued_recovery_authority(&fixture.store, 74);
    if let HydrationOutcome::PhysicalRecoveryRequired(intents) = fixture
        .store
        .hydrate(authority.lease(), authority.fence())
        .await
        .unwrap()
    {
        // This is a synthetic local adapter, with no external executor process.
        // The held effect has already written once and cannot perform another
        // write; use the same explicit old-epoch receipt as recovery fixtures.
        let attestation = crate::store::PhysicalReapAttestation::from_wire(
            fixture.store.scope().personality_agent_id.as_str(),
            74,
            "queued-recovery-nonce".into(),
            73,
        )
        .unwrap();
        SuffixRecovery::apply_boot_physical_receipt(
            &fixture.store,
            authority.lease(),
            authority.fence(),
            &attestation,
            &intents,
        )
        .await
        .unwrap();
    }
    let (restarted, commands, _) = fixture.start(74, true).await;
    commands.send(approval_decision(3, &id)).await.unwrap();
    wait_until(async || {
        fixture
            .scalar("SELECT COUNT(*) FROM inbound_commands WHERE seq=3 AND status!='received'")
            .await
            == 1
    })
    .await;
    assert_eq!(
        fixture.write.reads.load(Ordering::SeqCst),
        1,
        "unknown outcome must not retry mutation"
    );
    assert_eq!(fixture.scalar("SELECT COUNT(*) FROM messages WHERE role='tool_result' AND json_extract(payload,'$.tool_name')='write_artifact'").await,1,"unknown completion is an operation event, not a second ToolResult");
    let outcomes: Vec<String> = sqlx::query_scalar(
        "SELECT envelope FROM agent_events WHERE event_type='approval_operation_outcome'",
    )
    .fetch_all(fixture.store.pool())
    .await
    .unwrap();
    assert_eq!(outcomes.len(), 1);
    let outcome: Value = serde_json::from_str(&outcomes[0]).unwrap();
    assert_eq!(outcome["operation_id"], id);
    assert_eq!(outcome["result"]["details"]["error"], "indeterminate");
    restarted.abort();
    let _ = restarted.await;
}

#[tokio::test]
async fn denied_receipt_reports_one_outcome_without_starting_mutation() {
    let fixture = Fixture::new().await;
    let (task, commands, _) = fixture.start(73, false).await;
    commands.send(user(1)).await.unwrap();
    wait_until(async || {
        fixture
            .scalar("SELECT COUNT(*) FROM messages WHERE id='assistant-803'")
            .await
            == 1
    })
    .await;
    let id = fixture.pending_id().await;
    let InboundCommand::Valid(mut denial) = approval_decision(2, &id) else {
        unreachable!()
    };
    denial.command = Command::ApprovalDecision {
        request_id: id.clone(),
        decision: crate::gateway::ApprovalDecision::DenyOnce,
    };
    commands.send(InboundCommand::Valid(denial)).await.unwrap();
    wait_until(async || {
        fixture
            .scalar(
                "SELECT COUNT(*) FROM agent_events WHERE event_type='approval_operation_outcome'",
            )
            .await
            == 1
    })
    .await;
    let outcome: String = sqlx::query_scalar(
        "SELECT envelope FROM agent_events WHERE event_type='approval_operation_outcome'",
    )
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    let outcome: Value = serde_json::from_str(&outcome).unwrap();
    assert_eq!(outcome["operation_id"], id);
    assert_eq!(outcome["status"], "denied");
    assert_eq!(outcome["executed"], false);
    assert_eq!(fixture.write.reads.load(Ordering::SeqCst), 0);
    assert!(!fixture.directory.join("artifact.txt").exists());
    assert_eq!(fixture.scalar("SELECT COUNT(*) FROM messages WHERE role='tool_result' AND json_extract(payload,'$.tool_name')='write_artifact'").await,1);
    commands.send(approval_decision(3, &id)).await.unwrap();
    wait_until(async || {
        fixture
            .scalar("SELECT COUNT(*) FROM inbound_commands WHERE seq=3 AND status!='received'")
            .await
            == 1
    })
    .await;
    assert_eq!(fixture.write.reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        fixture
            .scalar(
                "SELECT COUNT(*) FROM agent_events WHERE event_type='approval_operation_outcome'"
            )
            .await,
        1
    );
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn durably_received_approval_replays_after_restart_without_human_resend() {
    let fixture = Fixture::new().await;
    let (task, commands, _) = fixture.start(73, false).await;
    commands.send(user(1)).await.unwrap();
    wait_until(async || {
        fixture
            .scalar("SELECT COUNT(*) FROM messages WHERE id='assistant-803'")
            .await
            == 1
    })
    .await;
    let id = fixture.pending_id().await;
    task.abort();
    let _ = task.await;
    drop(commands);
    // Emulate the precise crash seam: authenticated input was durably admitted,
    // but Session never applied the decision to the pending operation.
    crate::store::EventWriter::new(fixture.store.clone())
        .persist_inbound(&approval_decision(2, &id))
        .await
        .unwrap();
    assert_eq!(
        fixture
            .scalar("SELECT COUNT(*) FROM inbound_commands WHERE seq=2 AND status='received'")
            .await,
        1
    );
    let (task, _commands, _) = fixture.start(74, true).await;
    wait_until(async || {
        fixture
            .scalar(
                "SELECT COUNT(*) FROM agent_events WHERE event_type='approval_operation_outcome'",
            )
            .await
            == 1
    })
    .await;
    assert_eq!(fixture.write.reads.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.read.reads.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.scalar("SELECT COUNT(*) FROM messages WHERE role='tool_result' AND json_extract(payload,'$.tool_name')='write_artifact'").await,1);
    assert_eq!(
        fixture
            .scalar("SELECT COUNT(*) FROM inbound_commands WHERE seq=2 AND status='received'")
            .await,
        0
    );
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn hard_new_input_interrupts_inference_but_keeps_receipted_operation_approvable() {
    hard_input_with_pending_receipt(false).await;
}

#[tokio::test]
async fn hard_input_pending_receipt_survives_restart_after_owner_handoff() {
    hard_input_with_pending_receipt(true).await;
}

async fn hard_input_with_pending_receipt(restart_before_grant: bool) {
    let fixture = Fixture::new().await;
    fixture.hold_final.store(true, Ordering::SeqCst);
    let (mut task, mut commands, frames) = fixture.start(73, false).await;
    commands.send(user(1)).await.unwrap();
    wait_until(async || frames.lock().unwrap().iter().any(|frame| matches!(frame,OutboundFrame::Event { envelope } if envelope.event.to_string().contains("draft-before-new-input")))).await;
    let id = fixture.pending_id().await;
    assert_eq!(fixture.read.reads.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.write.reads.load(Ordering::SeqCst), 0);
    commands.send(user(2)).await.unwrap();
    let replacement = tokio::time::timeout(Duration::from_secs(9), async {
        while fixture
            .scalar("SELECT COUNT(*) FROM messages WHERE id='assistant-804'")
            .await
            != 1
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if replacement.is_err() {
        if task.is_finished() {
            panic!(
                "hard receipt session terminated before replacement: {:?}",
                task.await
            );
        }
        panic!("hard receipt session still running without replacement inference");
    }
    assert_eq!(fixture.scalar("SELECT COUNT(*) FROM inbound_commands WHERE seq=2 AND application_kind='hard_steer'").await,1,"test must exercise actual hard steering, not merely a queued message");
    assert_eq!(
        fixture.pending_id().await,
        id,
        "new input does not revoke the earlier operation"
    );
    assert_eq!(fixture.write.reads.load(Ordering::SeqCst), 0);
    if restart_before_grant {
        task.abort();
        let _ = task.await;
        let (restarted_task, restarted_commands, _) = fixture.start(74, true).await;
        task = restarted_task;
        commands = restarted_commands;
    }
    commands.send(approval_decision(3, &id)).await.unwrap();
    wait_until(async || {
        fixture
            .scalar(
                "SELECT COUNT(*) FROM agent_events WHERE event_type='approval_operation_outcome'",
            )
            .await
            == 1
    })
    .await;
    assert_eq!(fixture.write.reads.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.scalar("SELECT COUNT(*) FROM messages WHERE role='tool_result' AND json_extract(payload,'$.tool_name')='write_artifact'").await,1);
    let outcome: String = sqlx::query_scalar(
        "SELECT envelope FROM agent_events WHERE event_type='approval_operation_outcome'",
    )
    .fetch_one(fixture.store.pool())
    .await
    .unwrap();
    let outcome: Value = serde_json::from_str(&outcome).unwrap();
    assert_eq!(outcome["operation_id"], id);
    assert_eq!(outcome["status"], "succeeded");
    task.abort();
    let _ = task.await;
}
