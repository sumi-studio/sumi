//! Production-shaped, dependency-injected provider/tool runtime boundary.
//!
//! T26 owns construction of these dependencies. This module deliberately has
//! no environment/config/store hydration policy beyond the provider's existing
//! credential lookup at the real `stream_observed` seam.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    memory::transform,
    provider::{
        ModelSpec, ProviderTimingObservation, ProviderTimingObservations, ProviderTimingObserver,
        RequestOptions, stream_observed, timing_observation_channel,
        types::{
            ContextMessage, PromptContext, ProviderEventStream, PublicAssistantMessage,
            PublicMessage, StopReason, ToolCall, ToolResultMessage, Usage,
        },
    },
    runtime::contracts::ProcessGeneration,
    tools::{ToolCtx, ToolError, ToolRegistry, WorkspacePaths},
};

use super::{
    OverflowRecoveryOutcome, OverflowRecoveryRequest, ProviderAttempt, RunCore, RunDriver,
};

type StreamStarter = dyn Fn(
        ModelSpec,
        PromptContext,
        RequestOptions,
        CancellationToken,
        ProviderTimingObserver,
    ) -> ProviderEventStream
    + Send
    + Sync;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RunTimingSample {
    pub(crate) command_received_to_request_sent: Option<Duration>,
    pub(crate) request_sent_to_first_public_delta: Option<Duration>,
}

const RUN_TIMING_SAMPLE_WINDOW: usize = 256;

#[derive(Clone, Default)]
pub(crate) struct RunTimingSamples {
    // A Session can be long-lived. Retain only a rolling observability window
    // so p95 remains recent and memory does not grow with the conversation.
    inner: Arc<Mutex<RunTimingWindows>>,
}

#[derive(Default)]
struct RunTimingWindows {
    attempts: VecDeque<RunTimingSample>,
    command_to_request: VecDeque<Duration>,
}

impl RunTimingSamples {
    fn record(&self, sample: RunTimingSample) {
        let mut windows = self.inner.lock().expect("timing samples lock");
        if windows.attempts.len() == RUN_TIMING_SAMPLE_WINDOW {
            windows.attempts.pop_front();
        }
        windows.attempts.push_back(sample);
        if let Some(internal) = sample.command_received_to_request_sent {
            if windows.command_to_request.len() == RUN_TIMING_SAMPLE_WINDOW {
                windows.command_to_request.pop_front();
            }
            windows.command_to_request.push_back(internal);
        }
        drop(windows);

        let internal_p95 = self.internal_p95();
        tracing::info!(
            command_received_to_request_sent_ms = sample
                .command_received_to_request_sent
                .map(|duration| duration.as_secs_f64() * 1_000.0),
            request_sent_to_first_public_delta_ms = sample
                .request_sent_to_first_public_delta
                .map(|duration| duration.as_secs_f64() * 1_000.0),
            internal_p95_ms = internal_p95.map(|duration| duration.as_secs_f64() * 1_000.0),
            internal_p95_under_30ms =
                internal_p95.is_some_and(|duration| duration < Duration::from_millis(30)),
            "stdio provider timing"
        );
    }

    pub(crate) fn snapshot(&self) -> Vec<RunTimingSample> {
        self.inner
            .lock()
            .expect("timing samples lock")
            .attempts
            .iter()
            .copied()
            .collect()
    }

    pub(crate) fn internal_p95(&self) -> Option<Duration> {
        let mut samples: Vec<_> = self
            .inner
            .lock()
            .expect("timing samples lock")
            .command_to_request
            .iter()
            .copied()
            .collect();
        if samples.is_empty() {
            return None;
        }
        samples.sort_unstable();
        let rank = (samples.len() * 95).div_ceil(100);
        samples.get(rank.saturating_sub(1)).copied()
    }
}

/// Fully supplied run dependencies. Construction is fail-closed so T26 cannot
/// accidentally start a no-tool session or invent context/generation identity.
pub(crate) struct InjectedRunDriver {
    spec: ModelSpec,
    options: RequestOptions,
    prompt: PromptContext,
    registry: ToolRegistry,
    workspace: WorkspacePaths,
    executor_generation: ProcessGeneration,
    stream_starter: Arc<StreamStarter>,
    timings: RunTimingSamples,
    timing_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl InjectedRunDriver {
    pub(crate) fn new(
        spec: ModelSpec,
        options: RequestOptions,
        prompt: Option<PromptContext>,
        registry: Option<ToolRegistry>,
        workspace: Option<WorkspacePaths>,
        executor_generation: Option<ProcessGeneration>,
    ) -> Result<Self> {
        Self::with_stream_starter(
            spec,
            options,
            prompt,
            registry,
            workspace,
            executor_generation,
            Arc::new(stream_observed),
        )
    }

    pub(crate) fn with_stream_starter(
        spec: ModelSpec,
        options: RequestOptions,
        prompt: Option<PromptContext>,
        registry: Option<ToolRegistry>,
        workspace: Option<WorkspacePaths>,
        executor_generation: Option<ProcessGeneration>,
        stream_starter: Arc<StreamStarter>,
    ) -> Result<Self> {
        validate_spec(&spec)?;
        let prompt = prompt.ok_or_else(|| anyhow!("provider prompt context was not supplied"))?;
        let registry = registry.ok_or_else(|| anyhow!("frozen tool registry was not supplied"))?;
        if prompt.tools != registry.definitions() {
            bail!("prompt tool definitions do not exactly match the frozen tool registry");
        }
        let workspace = workspace.ok_or_else(|| anyhow!("workspace paths were not supplied"))?;
        let executor_generation = executor_generation
            .ok_or_else(|| anyhow!("executor generation identity was not supplied"))?;
        registry.validate_executor_generation(executor_generation)?;
        Ok(Self {
            spec,
            options,
            prompt,
            registry,
            workspace,
            executor_generation,
            stream_starter,
            timings: RunTimingSamples::default(),
            timing_tasks: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn timings(&self) -> RunTimingSamples {
        self.timings.clone()
    }

    pub(crate) fn executor_generation(&self) -> ProcessGeneration {
        self.executor_generation
    }

    fn initial_message(&self) -> PublicMessage {
        PublicMessage::Assistant(PublicAssistantMessage {
            content: Vec::new(),
            model: self.spec.id.clone(),
            provider: self.spec.provider.clone(),
            origin: self.spec.origin(),
            usage: Usage::default(),
            stop_reason: StopReason::Stop,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: Utc::now(),
        })
    }
}

#[async_trait]
impl RunDriver for InjectedRunDriver {
    fn validate_executor_generation(&self, generation: ProcessGeneration) -> Result<()> {
        if generation != self.executor_generation {
            bail!(
                "injected driver executor generation {} does not match Session generation {generation}",
                self.executor_generation
            );
        }
        Ok(())
    }

    async fn start_provider_for_command(
        &self,
        _attempt: usize,
        context: &[ContextMessage],
        command_received_at: Option<Instant>,
        cancel: CancellationToken,
    ) -> Result<ProviderAttempt> {
        let mut prompt = self.prompt.clone();
        // Derive the send view immediately before the provider call.  The
        // runner's retained runtime_context anchors must not be mutated.
        prompt.messages = transform::transform(context, &self.spec.origin());
        // Re-check at use time: the frozen registry remains the authority.
        if prompt.tools != self.registry.definitions() {
            bail!("provider prompt tools diverged from the frozen registry");
        }
        let (observer, observations) = timing_observation_channel();
        let timing_cancel = cancel.clone();
        let events = (self.stream_starter)(
            self.spec.clone(),
            prompt,
            self.options.clone(),
            cancel,
            observer,
        );
        let samples = self.timings.clone();
        let timing_task = tokio::spawn(collect_timing(
            observations,
            samples,
            command_received_at,
            timing_cancel,
        ));
        let mut timing_tasks = self.timing_tasks.lock().expect("timing tasks lock");
        timing_tasks.retain(|task| !task.is_finished());
        timing_tasks.push(timing_task);
        Ok(ProviderAttempt {
            message_id: Uuid::now_v7().to_string(),
            initial_message: self.initial_message(),
            events,
        })
    }

    async fn execute_tool_observed(
        &self,
        flow_id: &str,
        call: &ToolCall,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<ToolResultMessage, ToolError> {
        if flow_id.is_empty() || call.id.is_empty() {
            return Err(ToolError::Protocol(
                "tool execution identity must be non-empty".to_owned(),
            ));
        }
        let tool = self
            .registry
            .get(&call.name)
            .ok_or_else(|| ToolError::Protocol(format!("unknown frozen tool: {}", call.name)))?;
        let output = tool
            .execute(ToolCtx {
                flow_id,
                call_id: &call.id,
                args: &call.arguments,
                cancel,
                on_update,
                workspace: &self.workspace,
            })
            .await?;
        Ok(ToolResultMessage {
            tool_call_id: call.id.clone(),
            tool_name: call.name.clone(),
            content: output.content,
            details: output.details,
            is_error: false,
            timestamp: Utc::now(),
        })
    }

    fn synthetic_error(&self, message: &str) -> PublicMessage {
        let mut assistant = match self.initial_message() {
            PublicMessage::Assistant(assistant) => assistant,
            _ => unreachable!(),
        };
        assistant.stop_reason = StopReason::Error;
        assistant.error_message = Some(message.to_owned());
        PublicMessage::Assistant(assistant)
    }

    fn context_window(&self) -> Option<u64> {
        Some(self.spec.context_window)
    }

    async fn plan_overflow_recovery(
        &self,
        _core: &RunCore,
        _request: OverflowRecoveryRequest,
        _active_context: &[ContextMessage],
    ) -> Result<OverflowRecoveryOutcome> {
        bail!("overflow context assembly is not supplied; T21 must provide it explicitly")
    }
}

impl Drop for InjectedRunDriver {
    fn drop(&mut self) {
        for task in self
            .timing_tasks
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .drain(..)
        {
            task.abort();
        }
    }
}

async fn collect_timing(
    mut observations: ProviderTimingObservations,
    samples: RunTimingSamples,
    command_received_at: Option<Instant>,
    cancel: CancellationToken,
) {
    let mut request_sent = None;
    loop {
        let observation = tokio::select! {
            biased;
            observation = observations.recv() => observation,
            () = cancel.cancelled() => None,
        };
        let Some(observation) = observation else {
            break;
        };
        match observation {
            ProviderTimingObservation::RequestSent(at) => request_sent = Some(at),
            ProviderTimingObservation::FirstPublicDelta(first) => {
                if let Some(sent) = request_sent {
                    samples.record(timing_sample(command_received_at, sent, Some(first)));
                }
                return;
            }
        }
    }
    if let Some(sent) = request_sent {
        samples.record(timing_sample(command_received_at, sent, None));
    }
}

fn timing_sample(
    command_received_at: Option<Instant>,
    request_sent: Instant,
    first_public: Option<Instant>,
) -> RunTimingSample {
    RunTimingSample {
        command_received_to_request_sent: command_received_at
            .map(|received| request_sent.saturating_duration_since(received)),
        request_sent_to_first_public_delta: first_public
            .map(|first| first.saturating_duration_since(request_sent)),
    }
}

fn validate_spec(spec: &ModelSpec) -> Result<()> {
    if spec.id.trim().is_empty()
        || spec.provider.trim().is_empty()
        || spec.api_key_env.trim().is_empty()
        || spec.account_scope.trim().is_empty()
    {
        bail!("model identity fields must be non-empty");
    }
    let url = reqwest::Url::parse(&spec.base_url)
        .map_err(|error| anyhow!("model base URL is invalid: {error}"))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        bail!("model base URL must be an absolute HTTP(S) URL");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("model base URL must not contain credentials");
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("model base URL must not contain a query or fragment");
    }
    if spec.context_window == 0
        || spec.max_output_tokens == 0
        || spec.default_output_tokens == 0
        || spec.default_output_tokens > spec.max_output_tokens
    {
        bail!("model token limits are invalid");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use axum::{Json, Router, body::Body, http::Response, routing::post};
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::sync::{mpsc, oneshot};

    use super::*;
    use crate::{
        agent::{RunCore, SequentialRunWorker, Session, SessionResult},
        gateway::InjectedStdioGateway,
        provider::{
            ProviderTimingObservation, stream_with_api_key_observed,
            types::{
                AssistantContent, AssistantMessage, ContextMessage, Message, ProviderEvent,
                ProviderOutput, StopReason, ToolDefinition, Usage, UserContent, UserMessage,
                ValidatedToolArguments,
            },
        },
        runtime::contracts::RpcIdentity,
        store::{Store, user_message_id},
        tools::{
            Tool, ToolError, ToolOutput, ToolRegistryBuilder, ToolRisk,
            executor::{ExecutorClient, remote_executor_registry},
        },
    };

    fn generation(raw: u64) -> ProcessGeneration {
        ProcessGeneration::from_wire(raw).expect("valid test generation")
    }

    fn expected_tool_result_message_id(assistant_message_id: &str, tool_call_id: &str) -> String {
        let assistant_digest = Sha256::digest(assistant_message_id.as_bytes());
        let tool_call_digest = Sha256::digest(tool_call_id.as_bytes());
        let mut pair_digest = [0_u8; 64];
        pair_digest[..32].copy_from_slice(&assistant_digest);
        pair_digest[32..].copy_from_slice(&tool_call_digest);
        let namespace = uuid::Uuid::from_bytes([
            0x73, 0x75, 0x6d, 0x69, 0xa4, 0xc1, 0x48, 0x22, 0x91, 0x5d, 0xb5, 0xd2, 0x5a, 0x69,
            0x9f, 0x31,
        ]);
        uuid::Uuid::new_v5(&namespace, &pair_digest).to_string()
    }

    struct FakeTool {
        executions: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Tool for FakeTool {
        fn def(&self) -> ToolDefinition {
            ToolDefinition {
                name: "fixture_tool".to_owned(),
                description: "deterministic fixture".to_owned(),
                parameters: json!({
                    "type":"object",
                    "properties":{"value":{"type":"string"}},
                    "required":["value"],
                    "additionalProperties":false
                }),
            }
        }

        fn risk(&self) -> ToolRisk {
            ToolRisk::ReadOnly
        }

        async fn execute(&self, ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
            self.executions.fetch_add(1, Ordering::SeqCst);
            (ctx.on_update)(json!({"phase":"half"}));
            Ok(ToolOutput {
                content: vec![UserContent::Text {
                    text: "done".to_owned(),
                }],
                details: json!({"flow":ctx.flow_id,"call":ctx.call_id}),
            })
        }
    }

    fn dependencies_with_counter() -> (
        ModelSpec,
        PromptContext,
        ToolRegistry,
        WorkspacePaths,
        Arc<AtomicUsize>,
    ) {
        let executions = Arc::new(AtomicUsize::new(0));
        let mut builder = ToolRegistryBuilder::default();
        builder
            .register(Arc::new(FakeTool {
                executions: executions.clone(),
            }))
            .expect("register");
        let registry = builder.build();
        let prompt = PromptContext {
            system_prompt: "fixture".to_owned(),
            memory_blocks: Vec::new(),
            messages: Vec::new(),
            provider_context: Vec::new(),
            tools: registry.definitions(),
        };
        (
            ModelSpec::preset("kimi-k3").expect("preset"),
            prompt,
            registry,
            WorkspacePaths::new("/workspace").expect("workspace"),
            executions,
        )
    }

    fn dependencies() -> (ModelSpec, PromptContext, ToolRegistry, WorkspacePaths) {
        let (spec, prompt, registry, workspace, _) = dependencies_with_counter();
        (spec, prompt, registry, workspace)
    }

    fn inert_starter() -> Arc<StreamStarter> {
        Arc::new(|spec, _context, _options, cancel, observer| {
            let sent = Instant::now();
            observer.observe(ProviderTimingObservation::RequestSent(sent));
            observer.observe(ProviderTimingObservation::FirstPublicDelta(
                sent + Duration::from_millis(100),
            ));
            let (tx, rx) = mpsc::channel(1);
            tx.try_send(ProviderEvent::Start).expect("start");
            drop(tx);
            ProviderEventStream::new(rx, cancel, spec.provider.clone(), spec.origin())
        })
    }

    #[test]
    fn construction_is_fail_closed_but_accepts_explicit_empty_registry() {
        let (spec, prompt, registry, workspace) = dependencies();
        assert!(
            InjectedRunDriver::with_stream_starter(
                spec.clone(),
                RequestOptions::default(),
                None,
                Some(registry.clone()),
                Some(workspace.clone()),
                Some(generation(1)),
                inert_starter(),
            )
            .is_err()
        );
        assert!(
            InjectedRunDriver::with_stream_starter(
                spec.clone(),
                RequestOptions::default(),
                Some(prompt.clone()),
                None,
                Some(workspace.clone()),
                Some(generation(1)),
                inert_starter(),
            )
            .is_err()
        );
        assert!(
            InjectedRunDriver::with_stream_starter(
                spec.clone(),
                RequestOptions::default(),
                Some(prompt.clone()),
                Some(registry.clone()),
                None,
                Some(generation(1)),
                inert_starter(),
            )
            .is_err()
        );
        assert!(
            InjectedRunDriver::with_stream_starter(
                spec.clone(),
                RequestOptions::default(),
                Some(prompt),
                Some(registry),
                Some(workspace.clone()),
                None,
                inert_starter(),
            )
            .is_err()
        );

        let empty = ToolRegistryBuilder::default().build();
        let empty_prompt = PromptContext {
            system_prompt: "tool-less but explicit".to_owned(),
            memory_blocks: vec![],
            messages: vec![],
            provider_context: vec![],
            tools: vec![],
        };
        let driver = InjectedRunDriver::with_stream_starter(
            spec,
            RequestOptions::default(),
            Some(empty_prompt),
            Some(empty),
            Some(workspace),
            Some(generation(0)),
            inert_starter(),
        )
        .expect("generation zero and explicit empty registry are valid");
        assert_eq!(driver.executor_generation(), generation(0));
    }

    #[test]
    fn construction_rejects_noncanonical_or_secret_bearing_base_urls() {
        let (spec, prompt, registry, workspace) = dependencies();
        for base_url in [
            "https://user@example.test/v1",
            "https://user:password@example.test/v1",
            "https://example.test/v1?api_key=secret",
            "https://example.test/v1#fragment",
        ] {
            let mut invalid = spec.clone();
            invalid.base_url = base_url.to_owned();
            assert!(
                InjectedRunDriver::with_stream_starter(
                    invalid,
                    RequestOptions::default(),
                    Some(prompt.clone()),
                    Some(registry.clone()),
                    Some(workspace.clone()),
                    Some(generation(0)),
                    inert_starter(),
                )
                .is_err(),
                "accepted {base_url}"
            );
        }
    }

    #[test]
    fn construction_binds_remote_registry_to_executor_generation() {
        let client = Arc::new(ExecutorClient::new(
            "/tmp/sumi-unused-executor.sock",
            RpcIdentity::from_wire(7, "boot-nonce").expect("identity"),
            "conversation-1",
        ));
        let registry = remote_executor_registry(client).expect("remote registry");
        let prompt = PromptContext {
            system_prompt: "fixture".to_owned(),
            memory_blocks: vec![],
            messages: vec![],
            provider_context: vec![],
            tools: registry.definitions(),
        };
        let spec = ModelSpec::preset("kimi-k3").expect("preset");
        let workspace = WorkspacePaths::new("/workspace").expect("workspace");

        let error = InjectedRunDriver::with_stream_starter(
            spec.clone(),
            RequestOptions::default(),
            Some(prompt.clone()),
            Some(registry.clone()),
            Some(workspace.clone()),
            Some(generation(11)),
            inert_starter(),
        )
        .err()
        .expect("remote generation mismatch must fail construction");
        assert!(error.to_string().contains(
            "remote tool registry executor generation 7 does not match injected generation 11"
        ));

        let driver = InjectedRunDriver::with_stream_starter(
            spec,
            RequestOptions::default(),
            Some(prompt),
            Some(registry),
            Some(workspace),
            Some(generation(7)),
            inert_starter(),
        )
        .expect("matching remote generation");
        assert_eq!(driver.executor_generation(), generation(7));
    }

    #[test]
    fn timing_samples_use_a_bounded_rolling_window_for_p95() {
        let samples = RunTimingSamples::default();
        for millis in 0..=RUN_TIMING_SAMPLE_WINDOW {
            samples.record(RunTimingSample {
                command_received_to_request_sent: Some(Duration::from_millis(millis as u64)),
                request_sent_to_first_public_delta: None,
            });
        }
        let snapshot = samples.snapshot();
        assert_eq!(snapshot.len(), RUN_TIMING_SAMPLE_WINDOW);
        assert_eq!(
            snapshot
                .first()
                .and_then(|sample| sample.command_received_to_request_sent),
            Some(Duration::from_millis(1))
        );
        assert_eq!(samples.internal_p95(), Some(Duration::from_millis(244)));
    }

    #[test]
    fn continuation_attempts_cannot_evict_internal_overhead_window() {
        let samples = RunTimingSamples::default();
        samples.record(RunTimingSample {
            command_received_to_request_sent: Some(Duration::from_millis(5)),
            request_sent_to_first_public_delta: Some(Duration::from_millis(80)),
        });
        for millis in 0..RUN_TIMING_SAMPLE_WINDOW {
            samples.record(RunTimingSample {
                command_received_to_request_sent: None,
                request_sent_to_first_public_delta: Some(Duration::from_millis(millis as u64)),
            });
        }

        assert_eq!(samples.snapshot().len(), RUN_TIMING_SAMPLE_WINDOW);
        assert!(
            samples
                .snapshot()
                .iter()
                .all(|sample| sample.command_received_to_request_sent.is_none())
        );
        assert_eq!(samples.internal_p95(), Some(Duration::from_millis(5)));
    }

    #[tokio::test]
    async fn session_generation_binding_accepts_zero_and_rejects_mismatch_before_side_effects() {
        async fn gateway(
            store: &Store,
        ) -> InjectedStdioGateway<BufReader<tokio::io::DuplexStream>, tokio::io::DuplexStream>
        {
            let (command_read, _command_write) = tokio::io::duplex(1024);
            let (event_write, _event_read) = tokio::io::duplex(1024);
            InjectedStdioGateway::new(
                BufReader::new(command_read),
                event_write,
                store
                    .command_digest_factory()
                    .await
                    .expect("digest factory"),
            )
        }

        let empty = ToolRegistryBuilder::default().build();
        let (spec, _, _, workspace) = dependencies();
        let prompt = PromptContext {
            system_prompt: "fixture".to_owned(),
            memory_blocks: vec![],
            messages: vec![],
            provider_context: vec![],
            tools: vec![],
        };
        let driver = Arc::new(
            InjectedRunDriver::with_stream_starter(
                spec.clone(),
                RequestOptions::default(),
                Some(prompt.clone()),
                Some(empty.clone()),
                Some(workspace.clone()),
                Some(generation(0)),
                inert_starter(),
            )
            .expect("zero driver"),
        );
        let store = Store::session_test_store("injected-generation-zero")
            .await
            .expect("store");
        let zero_gateway = gateway(&store).await;
        let session = Session::start(
            store,
            zero_gateway,
            RunCore::new(),
            Arc::new(SequentialRunWorker::new(driver)),
            generation(0),
        )
        .await
        .expect("0 == 0 must start");
        drop(session);

        let starts = Arc::new(AtomicUsize::new(0));
        let observed_starts = starts.clone();
        let starter: Arc<StreamStarter> = Arc::new(move |spec, _, _, cancel, _| {
            observed_starts.fetch_add(1, Ordering::SeqCst);
            let (_tx, rx) = mpsc::channel(1);
            ProviderEventStream::new(rx, cancel, spec.provider.clone(), spec.origin())
        });
        let driver = Arc::new(
            InjectedRunDriver::with_stream_starter(
                spec,
                RequestOptions::default(),
                Some(prompt),
                Some(empty),
                Some(workspace),
                Some(generation(7)),
                starter,
            )
            .expect("driver"),
        );
        let store = Store::session_test_store("injected-generation-mismatch")
            .await
            .expect("store");
        let pool = store.pool().clone();
        let gateway_store = Store::session_test_store("injected-generation-mismatch-gateway")
            .await
            .expect("gateway store");
        let mismatch_gateway = gateway(&gateway_store).await;
        let error = match Session::start(
            store,
            mismatch_gateway,
            RunCore::new(),
            Arc::new(SequentialRunWorker::new(driver)),
            generation(11),
        )
        .await
        {
            Ok(_) => panic!("generation mismatch must fail"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("does not match Session generation")
        );
        assert_eq!(starts.load(Ordering::SeqCst), 0);
        for table in [
            "data_keys",
            "agent_events",
            "inbound_commands",
            "tool_executions",
        ] {
            let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(&pool)
                .await
                .expect("count");
            assert_eq!(count, 0, "mismatch mutated {table}");
        }
    }

    #[tokio::test]
    async fn frozen_registry_executes_with_progress_and_unknown_tools_fail_closed() {
        let (spec, prompt, registry, workspace) = dependencies();
        let driver = InjectedRunDriver::with_stream_starter(
            spec,
            RequestOptions::default(),
            Some(prompt),
            Some(registry),
            Some(workspace),
            Some(generation(7)),
            inert_starter(),
        )
        .expect("driver");
        let call = ToolCall {
            id: "call-1".to_owned(),
            name: "fixture_tool".to_owned(),
            arguments: serde_json::from_value::<ValidatedToolArguments>(json!({"value":"x"}))
                .expect("validated-shaped arguments"),
        };
        let updates = Arc::new(Mutex::new(Vec::new()));
        let sink = updates.clone();
        let result = driver
            .execute_tool_observed(
                "flow-1",
                &call,
                CancellationToken::new(),
                Arc::new(move |value| sink.lock().expect("updates").push(value)),
            )
            .await
            .expect("tool result");
        assert_eq!(result.tool_call_id, "call-1");
        assert_eq!(
            result.content,
            vec![UserContent::Text {
                text: "done".to_owned()
            }]
        );
        assert_eq!(
            *updates.lock().expect("updates"),
            vec![json!({"phase":"half"})]
        );

        let mut unknown = call;
        unknown.name = "not_frozen".to_owned();
        assert!(
            driver
                .execute_tool_observed(
                    "flow-1",
                    &unknown,
                    CancellationToken::new(),
                    Arc::new(|_| {}),
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("unknown frozen tool")
        );
    }

    #[tokio::test]
    async fn continuation_attempts_keep_ttft_without_entering_internal_p95() {
        let (spec, prompt, registry, workspace) = dependencies();
        let driver = InjectedRunDriver::with_stream_starter(
            spec,
            RequestOptions::default(),
            Some(prompt),
            Some(registry),
            Some(workspace),
            Some(generation(1)),
            inert_starter(),
        )
        .expect("driver");
        for attempt in 0..20 {
            let received = (attempt == 0).then(|| Instant::now() - Duration::from_millis(5));
            let _attempt = driver
                .start_provider_for_command(attempt, &[], received, CancellationToken::new())
                .await
                .expect("attempt");
        }
        for _ in 0..20 {
            if driver.timings().snapshot().len() == 20 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let samples = driver.timings().snapshot();
        assert_eq!(samples.len(), 20);
        assert!(driver.timings().internal_p95().expect("p95") < Duration::from_millis(30));
        assert_eq!(
            samples
                .iter()
                .filter(|sample| sample.command_received_to_request_sent.is_some())
                .count(),
            1,
            "only the causally first request contributes internal overhead"
        );
        assert!(
            samples
                .iter()
                .all(|sample| sample.request_sent_to_first_public_delta
                    == Some(Duration::from_millis(100)))
        );
    }

    async fn serve_delayed_sse() -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route("/chat/completions", post(|| async {
            tokio::time::sleep(Duration::from_millis(35)).await;
            let body = concat!(
                "data: {\"id\":\"text-1\",\"model\":\"kimi-k3\",\"choices\":[{\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}]}\n\n",
                "data: {\"id\":\"text-1\",\"model\":\"kimi-k3\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n",
                "data: [DONE]\n\n"
            );
            Response::builder().status(200).header("content-type", "text/event-stream")
                .body(Body::from(body)).expect("response")
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        let task = tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        (format!("http://{address}"), task)
    }

    async fn serve_delayed_thinking_sse() -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/chat/completions",
            post(|| async {
                tokio::time::sleep(Duration::from_millis(35)).await;
                let body = concat!(
                    "data: {\"id\":\"thinking-1\",\"model\":\"kimi-k3\",\"choices\":[{\"delta\":{\"reasoning_content\":\"considering\"},\"finish_reason\":null}]}\n\n",
                    "data: {\"id\":\"thinking-1\",\"model\":\"kimi-k3\",\"choices\":[{\"delta\":{\"content\":\"answer\"},\"finish_reason\":null}]}\n\n",
                    "data: {\"id\":\"thinking-1\",\"model\":\"kimi-k3\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                    "data: [DONE]\n\n"
                );
                Response::builder()
                    .status(200)
                    .header("content-type", "text/event-stream")
                    .body(Body::from(body))
                    .expect("response")
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        let task = tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        (format!("http://{address}"), task)
    }

    #[tokio::test]
    async fn real_observed_provider_seam_reports_request_and_public_delta_phases() {
        let (base_url, server) = serve_delayed_sse().await;
        let (mut spec, prompt, registry, workspace) = dependencies();
        spec.base_url = base_url;
        let starter: Arc<StreamStarter> = Arc::new(|spec, context, options, cancel, observer| {
            stream_with_api_key_observed(
                spec,
                context,
                options,
                cancel,
                Some("test".to_owned()),
                Some(observer),
            )
        });
        let driver = InjectedRunDriver::with_stream_starter(
            spec,
            RequestOptions::default(),
            Some(prompt),
            Some(registry),
            Some(workspace),
            Some(generation(9)),
            starter,
        )
        .expect("driver");
        let mut attempt = driver
            .start_provider_for_command(0, &[], Some(Instant::now()), CancellationToken::new())
            .await
            .expect("attempt");
        assert_eq!(
            Uuid::parse_str(&attempt.message_id)
                .expect("uuid")
                .get_version_num(),
            7
        );
        while attempt.events.recv().await.is_some() {}
        for _ in 0..20 {
            if !driver.timings().snapshot().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        let sample = driver
            .timings()
            .snapshot()
            .into_iter()
            .next()
            .expect("timing");
        assert!(sample.command_received_to_request_sent.is_some());
        assert!(
            sample
                .request_sent_to_first_public_delta
                .expect("first delta")
                >= Duration::from_millis(30)
        );
        server.abort();
    }

    #[tokio::test]
    async fn real_injected_seam_times_thinking_as_the_first_public_delta() {
        let (base_url, server) = serve_delayed_thinking_sse().await;
        let (mut spec, prompt, registry, workspace) = dependencies();
        spec.base_url = base_url;
        let starter: Arc<StreamStarter> = Arc::new(|spec, context, options, cancel, observer| {
            stream_with_api_key_observed(
                spec,
                context,
                options,
                cancel,
                Some("test".to_owned()),
                Some(observer),
            )
        });
        let driver = InjectedRunDriver::with_stream_starter(
            spec,
            RequestOptions::default(),
            Some(prompt),
            Some(registry),
            Some(workspace),
            Some(generation(9)),
            starter,
        )
        .expect("driver");
        let mut attempt = driver
            .start_provider_for_command(0, &[], Some(Instant::now()), CancellationToken::new())
            .await
            .expect("attempt");
        let mut saw_thinking_before_text = false;
        while let Some(event) = attempt.events.recv().await {
            match event {
                ProviderEvent::ThinkingDelta { .. } => saw_thinking_before_text = true,
                ProviderEvent::TextDelta { .. } => {
                    assert!(saw_thinking_before_text);
                    break;
                }
                _ => {}
            }
        }
        assert!(saw_thinking_before_text);
        let sample = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(sample) = driver.timings().snapshot().into_iter().next() {
                    break sample;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timing collector hung");
        assert!(
            sample
                .request_sent_to_first_public_delta
                .expect("first public delta")
                >= Duration::from_millis(30)
        );
        server.abort();
    }

    async fn serve_tool_then_text() -> (String, tokio::task::JoinHandle<()>, Arc<Mutex<Vec<Value>>>)
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new().route(
            "/chat/completions",
            post({
                let calls = calls.clone();
                let requests = requests.clone();
                move |Json(request): Json<Value>| {
                    let ordinal = calls.fetch_add(1, Ordering::SeqCst);
                    requests.lock().expect("requests").push(request);
                    async move {
                        let body = if ordinal == 0 {
                            concat!(
                                "data: {\"id\":\"tool-1\",\"model\":\"kimi-k3\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"function\":{\"name\":\"fixture_tool\",\"arguments\":\"{\\\"value\\\":\\\"x\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
                                "data: [DONE]\n\n"
                            )
                        } else {
                            concat!(
                                "data: {\"id\":\"text-2\",\"model\":\"kimi-k3\",\"choices\":[{\"delta\":{\"content\":\"complete\"},\"finish_reason\":null}]}\n\n",
                                "data: {\"id\":\"text-2\",\"model\":\"kimi-k3\",\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                                "data: [DONE]\n\n"
                            )
                        };
                        Response::builder()
                            .status(200)
                            .header("content-type", "text/event-stream")
                            .body(Body::from(body))
                            .expect("response")
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        let task = tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        (format!("http://{address}"), task, requests)
    }

    async fn serve_start_failure() -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/chat/completions",
            post(|| async {
                Response::builder()
                    .status(400)
                    .body(Body::from("fixture invalid request"))
                    .expect("response")
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        let task = tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
        (format!("http://{address}"), task)
    }

    async fn run_failing_json_lines_case(
        name: &str,
        spec: ModelSpec,
        starter: Arc<StreamStarter>,
        expected_code: &str,
    ) {
        let store = Store::session_test_store(name).await.expect("store");
        let digest = store.command_digest_factory().await.expect("digest");
        let (command_read, mut command_write) = tokio::io::duplex(4096);
        let (event_write, event_read) = tokio::io::duplex(64 * 1024);
        let gateway = InjectedStdioGateway::new(BufReader::new(command_read), event_write, digest);
        let (_, prompt, registry, workspace) = dependencies();
        let driver = Arc::new(
            InjectedRunDriver::with_stream_starter(
                spec,
                RequestOptions::default(),
                Some(prompt),
                Some(registry),
                Some(workspace),
                Some(generation(11)),
                starter,
            )
            .expect("driver"),
        );
        let session = Session::start(
            store,
            gateway,
            RunCore::new(),
            Arc::new(SequentialRunWorker::new(driver)),
            generation(11),
        )
        .await
        .expect("session");
        let session_task = tokio::spawn(session.run());
        command_write.write_all(b"{\"seq\":1,\"command_id\":\"018f6f75-43f7-7c2e-8d9a-0f6c83e75b1a\",\"command\":{\"type\":\"user_message\",\"text\":\"fail\",\"attachments\":[]}}\n").await.expect("command");
        let mut lines = BufReader::new(event_read).lines();
        let mut frames = Vec::new();
        loop {
            let line = tokio::time::timeout(Duration::from_secs(2), lines.next_line())
                .await
                .unwrap_or_else(|_| panic!("failure case {name} hung"))
                .expect("read")
                .expect("EOF before applied ACK");
            let frame: Value = serde_json::from_str(&line).expect("JSON frame");
            let applied = frame.get("frame_type") == Some(&json!("command_ack"))
                && frame.pointer("/ack/status") == Some(&json!("applied"));
            frames.push(frame);
            if applied {
                break;
            }
        }
        drop(command_write);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), session_task)
                .await
                .expect("shutdown hung")
                .expect("join"),
            SessionResult::Completed(_)
        ));
        let event_types: Vec<_> = frames
            .iter()
            .filter_map(|frame| {
                frame
                    .pointer("/envelope/event/type")
                    .and_then(Value::as_str)
            })
            .collect();
        assert_eq!(
            event_types,
            vec![
                "agent_start",
                "turn_start",
                "message_start",
                "message_end",
                "message_start",
                "message_end",
                "turn_end",
                "agent_end",
            ]
        );
        assert!(
            frames.iter().any(|frame| {
                frame.pointer("/envelope/event/message/provider_code")
                    == Some(&json!(expected_code))
            }),
            "missing terminal provider code {expected_code}: {frames:?}"
        );
        assert_eq!(
            frames
                .iter()
                .filter(|frame| frame.get("frame_type") == Some(&json!("command_ack")))
                .filter_map(|frame| frame.pointer("/ack/status").and_then(Value::as_str))
                .collect::<Vec<_>>(),
            vec!["received", "applied"]
        );
    }

    #[tokio::test]
    async fn injected_json_lines_failures_close_in_exact_normal_form() {
        let (base_url, server) = serve_start_failure().await;
        let mut start_failure_spec = ModelSpec::preset("kimi-k3").expect("preset");
        start_failure_spec.base_url = base_url;
        let real_starter: Arc<StreamStarter> =
            Arc::new(|spec, context, options, cancel, observer| {
                stream_with_api_key_observed(
                    spec,
                    context,
                    options,
                    cancel,
                    Some("test".to_owned()),
                    Some(observer),
                )
            });
        run_failing_json_lines_case(
            "injected-start-failure",
            start_failure_spec,
            real_starter,
            "http_400",
        )
        .await;
        server.abort();

        let spec = ModelSpec::preset("kimi-k3").expect("preset");
        let eof_starter: Arc<StreamStarter> = Arc::new(|spec, _, _, cancel, _| {
            let (tx, rx) = mpsc::channel(1);
            drop(tx);
            cancel.cancel();
            ProviderEventStream::new(rx, cancel, spec.provider.clone(), spec.origin())
        });
        run_failing_json_lines_case(
            "injected-eof-failure",
            spec.clone(),
            eof_starter,
            "cancelled",
        )
        .await;

        let invalid_starter: Arc<StreamStarter> = Arc::new(|spec, _, _, cancel, _| {
            let (tx, rx) = mpsc::channel(1);
            let mut wrong_origin = spec.origin();
            wrong_origin.model = "wrong-model".to_owned();
            tx.try_send(ProviderEvent::Done {
                reason: StopReason::Stop,
                output: ProviderOutput {
                    message: AssistantMessage {
                        content: vec![],
                        model: wrong_origin.model.clone(),
                        provider: spec.provider.clone(),
                        origin: wrong_origin,
                        usage: Usage::default(),
                        stop_reason: StopReason::Stop,
                        error_message: None,
                        provider_code: None,
                        interrupted: false,
                        timestamp: Utc::now(),
                    },
                    provider_context: vec![],
                },
            })
            .expect("terminal");
            ProviderEventStream::new(rx, cancel, spec.provider.clone(), spec.origin())
        });
        run_failing_json_lines_case(
            "injected-invalid-terminal",
            spec,
            invalid_starter,
            "invalid_provider_terminal",
        )
        .await;
    }

    #[tokio::test]
    async fn injected_json_lines_loop_covers_real_provider_and_tool_lifecycle() {
        let store = Store::session_test_store("injected-stdio-loop")
            .await
            .expect("store");
        let digest_factory = store
            .command_digest_factory()
            .await
            .expect("digest factory");
        let (command_read, mut command_write) = tokio::io::duplex(16 * 1024);
        let (event_write, event_read) = tokio::io::duplex(64 * 1024);
        let gateway =
            InjectedStdioGateway::new(BufReader::new(command_read), event_write, digest_factory);

        let (base_url, server, requests) = serve_tool_then_text().await;
        let (mut spec, prompt, registry, workspace, tool_executions) = dependencies_with_counter();
        spec.base_url = base_url;
        let starter: Arc<StreamStarter> = Arc::new(|spec, context, options, cancel, observer| {
            stream_with_api_key_observed(
                spec,
                context,
                options,
                cancel,
                Some("test".to_owned()),
                Some(observer),
            )
        });
        let driver = Arc::new(
            InjectedRunDriver::with_stream_starter(
                spec,
                RequestOptions::default(),
                Some(prompt),
                Some(registry),
                Some(workspace),
                Some(generation(11)),
                starter,
            )
            .expect("driver"),
        );
        let worker = Arc::new(SequentialRunWorker::new(driver.clone()));
        let session = Session::start(store, gateway, RunCore::new(), worker, generation(11))
            .await
            .expect("session");
        let session_task = tokio::spawn(session.run());

        command_write
            .write_all(
                b"{\"seq\":1,\"command_id\":\"018f6f75-43f7-7c2e-8d9a-0f6c83e75b1a\",\"command\":{\"type\":\"user_message\",\"text\":\"run\",\"attachments\":[]}}\n",
            )
            .await
            .expect("command");
        let mut lines = BufReader::new(event_read).lines();
        let mut frames = Vec::new();
        loop {
            let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
                .await
                .expect("frame timeout")
                .expect("frame read")
                .expect("frame EOF before applied ACK");
            let frame: Value = serde_json::from_str(&line).expect("frame JSON");
            let applied = frame.get("frame_type") == Some(&json!("command_ack"))
                && frame.pointer("/ack/status") == Some(&json!("applied"));
            frames.push(frame);
            if applied {
                break;
            }
        }
        drop(command_write);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), session_task)
                .await
                .expect("session timeout")
                .expect("session join"),
            SessionResult::Completed(_)
        ));

        let event_frames: Vec<_> = frames
            .iter()
            .filter(|frame| frame.get("frame_type") == Some(&json!("event")))
            .collect();
        let event_types: Vec<_> = event_frames
            .iter()
            .map(|frame| {
                frame
                    .pointer("/envelope/event/type")
                    .and_then(Value::as_str)
                    .expect("public event type")
            })
            .collect();
        assert_eq!(
            event_types,
            vec![
                "agent_start",
                "turn_start",
                "message_start",
                "message_end",
                "message_start",
                "message_update",
                "message_update",
                "message_update",
                "message_update",
                "message_end",
                "tool_execution_start",
                "tool_execution_update",
                "tool_execution_end",
                "message_start",
                "message_end",
                "turn_end",
                "turn_start",
                "message_start",
                "message_update",
                "message_update",
                "message_update",
                "message_end",
                "turn_end",
                "agent_end",
            ],
            "user_message -> public event sequence changed: {event_types:?}"
        );
        let durable_types: Vec<_> = event_frames
            .iter()
            .filter(|frame| frame.pointer("/envelope/seq").is_some())
            .map(|frame| {
                frame
                    .pointer("/envelope/event/type")
                    .and_then(Value::as_str)
                    .expect("durable event type")
            })
            .collect();
        assert_eq!(
            durable_types,
            vec![
                "agent_start",
                "turn_start",
                "message_start",
                "message_end",
                "message_start",
                "message_end",
                "tool_execution_start",
                "tool_execution_end",
                "message_start",
                "message_end",
                "turn_end",
                "turn_start",
                "message_start",
                "message_end",
                "turn_end",
                "agent_end",
            ],
            "durable event sequence changed: {durable_types:?}"
        );

        let message_starts: Vec<_> = event_frames
            .iter()
            .filter(|frame| frame.pointer("/envelope/event/type") == Some(&json!("message_start")))
            .collect();
        let message_ends: Vec<_> = event_frames
            .iter()
            .filter(|frame| frame.pointer("/envelope/event/type") == Some(&json!("message_end")))
            .collect();
        assert_eq!(message_starts.len(), 4);
        assert_eq!(message_ends.len(), 4);
        let start_ids: Vec<_> = message_starts
            .iter()
            .map(|frame| {
                frame
                    .pointer("/envelope/event/message_id")
                    .and_then(Value::as_str)
                    .expect("message start ID")
            })
            .collect();
        let end_ids: Vec<_> = message_ends
            .iter()
            .map(|frame| {
                frame
                    .pointer("/envelope/event/message_id")
                    .and_then(Value::as_str)
                    .expect("message end ID")
            })
            .collect();
        assert_eq!(
            start_ids, end_ids,
            "each MessageStart must close exactly once"
        );
        assert_eq!(
            start_ids[0],
            user_message_id("018f6f75-43f7-7c2e-8d9a-0f6c83e75b1a")
        );
        assert_eq!(
            message_ends[2].pointer("/envelope/event/message/content"),
            Some(&json!([{ "text": "done", "type": "text" }]))
        );
        assert_eq!(
            message_ends[2].pointer("/envelope/event/message/role"),
            Some(&json!("tool_result"))
        );
        assert_eq!(
            message_ends[2].pointer("/envelope/event/message/tool_call_id"),
            Some(&json!("call-1"))
        );
        assert_eq!(
            message_ends[2].pointer("/envelope/event/message/tool_name"),
            Some(&json!("fixture_tool"))
        );
        assert_eq!(
            message_ends[2].pointer("/envelope/event/message/is_error"),
            Some(&json!(false))
        );
        assert_eq!(
            start_ids[2],
            expected_tool_result_message_id(start_ids[1], "call-1"),
            "tool-result message ID must be bound to its assistant/tool-call pair"
        );
        assert_ne!(start_ids[1], start_ids[3]);
        assert!(!start_ids[3].is_empty());
        assert_eq!(
            message_ends[3].pointer("/envelope/event/message/content"),
            Some(&json!([{ "text": "complete", "type": "text", "wire_item_index": 0 }]))
        );
        assert_eq!(
            message_ends[3].pointer("/envelope/event/message/stop_reason"),
            Some(&json!("stop"))
        );
        assert_eq!(
            message_ends[3].pointer("/envelope/event/message/provider_code"),
            Some(&json!("stop"))
        );
        assert_eq!(tool_executions.load(Ordering::SeqCst), 1);
        let progress = frames
            .iter()
            .find(|frame| {
                frame.pointer("/envelope/event/type") == Some(&json!("tool_execution_update"))
            })
            .expect("real FakeTool progress event");
        assert_eq!(
            progress.pointer("/envelope/event/tool_call_id"),
            Some(&json!("call-1"))
        );
        assert_eq!(
            progress.pointer("/envelope/event/partial/phase"),
            Some(&json!("half"))
        );
        let ack_indices: Vec<_> = frames
            .iter()
            .enumerate()
            .filter_map(|(index, frame)| {
                (frame.get("frame_type") == Some(&json!("command_ack"))).then_some(index)
            })
            .collect();
        assert_eq!(ack_indices, vec![0, frames.len() - 1]);
        assert_eq!(
            ack_indices
                .iter()
                .filter_map(|index| frames[*index]
                    .pointer("/ack/status")
                    .and_then(Value::as_str))
                .collect::<Vec<_>>(),
            vec!["received", "applied"]
        );
        assert!(ack_indices.iter().all(|index| {
            frames[*index].pointer("/ack/command_id")
                == Some(&json!("018f6f75-43f7-7c2e-8d9a-0f6c83e75b1a"))
                && frames[*index].pointer("/ack/seq") == Some(&json!(1))
        }));

        let requests = requests.lock().expect("requests").clone();
        assert_eq!(
            requests.len(),
            2,
            "tool lifecycle must issue exactly two requests"
        );
        assert_eq!(requests[0].pointer("/model"), Some(&json!("kimi-k3")));
        assert_eq!(requests[0].pointer("/stream"), Some(&json!(true)));
        assert_eq!(
            requests[0].pointer("/messages/0"),
            Some(&json!({"role":"system", "content":"fixture"}))
        );
        assert_eq!(
            requests[0].pointer("/messages/1"),
            Some(&json!({
                "role":"user",
                "content":[{"text":"run", "type":"text"}]
            }))
        );
        assert_eq!(
            requests[0].pointer("/tools/0/function/name"),
            Some(&json!("fixture_tool"))
        );
        assert_eq!(
            requests[1].pointer("/messages/2"),
            Some(&json!({
                "role":"assistant",
                "reasoning_content":"",
                "tool_calls":[{
                    "id":"call-1",
                    "type":"function",
                    "function":{"name":"fixture_tool", "arguments":"{\"value\":\"x\"}"}
                }]
            }))
        );
        assert_eq!(
            requests[1].pointer("/messages/3/role"),
            Some(&json!("tool"))
        );
        assert_eq!(
            requests[1].pointer("/messages/3/content"),
            Some(&json!("done"))
        );
        assert_eq!(
            requests[1].pointer("/messages/3/tool_call_id"),
            Some(&json!("call-1"))
        );
        let timings = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let samples = driver.timings().snapshot();
                if samples.len() >= 2 {
                    break samples;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real JSON-lines timing sample");
        assert!(
            timings[0].command_received_to_request_sent.is_some(),
            "JSON-lines ingress must supply the first request's real monotonic command timestamp"
        );
        assert_eq!(
            timings[1].command_received_to_request_sent, None,
            "tool continuation must not reuse command ingress"
        );
        assert!(
            timings
                .iter()
                .any(|sample| sample.request_sent_to_first_public_delta.is_some()),
            "the text-producing attempt must expose provider TTFT"
        );
        server.abort();
    }

    // Wall-clock performance is meaningful only for the optimized binary.
    // Debug builds still exercise the same real ingress wiring in the lifecycle
    // E2E above, without pretending their instrumentation cost is production.
    #[cfg(not(debug_assertions))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn json_lines_real_ingress_internal_overhead_p95_is_under_30ms() {
        // Exercise the production rolling metric past one complete window.
        // This ages out the full gate's startup contention without subtracting
        // scheduler delay from any retained wall-clock ingress measurement.
        const COMMAND_COUNT: usize = RUN_TIMING_SAMPLE_WINDOW + 128;

        let store = Store::session_test_store("injected-stdio-p95")
            .await
            .expect("store");
        let digest_factory = store.command_digest_factory().await.expect("digest");
        let (command_read, mut command_write) = tokio::io::duplex(64 * 1024);
        let (event_write, event_read) = tokio::io::duplex(256 * 1024);
        let gateway =
            InjectedStdioGateway::new(BufReader::new(command_read), event_write, digest_factory);
        let (base_url, server) = serve_delayed_sse().await;
        let (mut spec, prompt, registry, workspace) = dependencies();
        spec.base_url = base_url;
        let starter: Arc<StreamStarter> = Arc::new(|spec, context, options, cancel, observer| {
            stream_with_api_key_observed(
                spec,
                context,
                options,
                cancel,
                Some("test".to_owned()),
                Some(observer),
            )
        });
        let driver = Arc::new(
            InjectedRunDriver::with_stream_starter(
                spec,
                RequestOptions::default(),
                Some(prompt),
                Some(registry),
                Some(workspace),
                Some(generation(11)),
                starter,
            )
            .expect("driver"),
        );
        let session = Session::start(
            store,
            gateway,
            RunCore::new(),
            Arc::new(SequentialRunWorker::new(driver.clone())),
            generation(11),
        )
        .await
        .expect("session");
        let session_task = tokio::spawn(session.run());
        let mut lines = BufReader::new(event_read).lines();

        for seq in 1..=COMMAND_COUNT {
            let command = json!({
                "seq": seq,
                "command_id": Uuid::now_v7().to_string(),
                "command": {"type":"user_message", "text":"measure", "attachments":[]}
            });
            let mut encoded = serde_json::to_vec(&command).expect("command JSON");
            encoded.push(b'\n');
            command_write.write_all(&encoded).await.expect("command");
            loop {
                let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
                    .await
                    .expect("frame timeout")
                    .expect("frame read")
                    .expect("frame EOF before applied ACK");
                let frame: Value = serde_json::from_str(&line).expect("frame JSON");
                if frame.get("frame_type") == Some(&json!("command_ack"))
                    && frame.pointer("/ack/status") == Some(&json!("applied"))
                    && frame.pointer("/ack/seq") == Some(&json!(seq))
                {
                    break;
                }
            }
        }

        drop(command_write);
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), session_task)
                .await
                .expect("session timeout")
                .expect("session join"),
            SessionResult::Completed(_)
        ));
        let samples = driver.timings().snapshot();
        assert_eq!(samples.len(), RUN_TIMING_SAMPLE_WINDOW);
        assert!(samples.iter().all(|sample| {
            sample.command_received_to_request_sent.is_some()
                && sample.request_sent_to_first_public_delta.is_some()
        }));
        let internal_p95 = driver.timings().internal_p95().expect("real ingress p95");
        assert!(
            internal_p95 < Duration::from_millis(30),
            "real JSON-lines command ingress to request_sent p95 {internal_p95:?} exceeded 30ms"
        );
        server.abort();
    }

    #[tokio::test]
    async fn json_lines_shutdown_cancels_held_provider_without_detaching_producer() {
        let store = Store::session_test_store("injected-held-provider")
            .await
            .expect("store");
        let digest_factory = store.command_digest_factory().await.expect("digest");
        let (command_read, mut command_write) = tokio::io::duplex(4096);
        let (event_write, _event_read) = tokio::io::duplex(64 * 1024);
        let gateway =
            InjectedStdioGateway::new(BufReader::new(command_read), event_write, digest_factory);
        let (started_tx, started_rx) = oneshot::channel();
        let started_tx = Arc::new(Mutex::new(Some(started_tx)));
        let (cancelled_tx, cancelled_rx) = oneshot::channel();
        let cancelled_tx = Arc::new(Mutex::new(Some(cancelled_tx)));
        let starter: Arc<StreamStarter> = Arc::new(move |spec, _, _, cancel, _| {
            if let Some(started) = started_tx.lock().expect("started").take() {
                let _ = started.send(());
            }
            let (tx, rx) = mpsc::channel(1);
            let cancelled_tx = cancelled_tx.clone();
            let producer_cancel = cancel.clone();
            tokio::spawn(async move {
                producer_cancel.cancelled().await;
                if let Some(cancelled) = cancelled_tx.lock().expect("cancelled").take() {
                    let _ = cancelled.send(());
                }
                drop(tx);
            });
            ProviderEventStream::new(rx, cancel, spec.provider.clone(), spec.origin())
        });
        let (spec, prompt, registry, workspace) = dependencies();
        let driver = Arc::new(
            InjectedRunDriver::with_stream_starter(
                spec,
                RequestOptions::default(),
                Some(prompt),
                Some(registry),
                Some(workspace),
                Some(generation(11)),
                starter,
            )
            .expect("driver"),
        );
        let session = Session::start(
            store,
            gateway,
            RunCore::new(),
            Arc::new(SequentialRunWorker::new(driver)),
            generation(11),
        )
        .await
        .expect("session");
        let session_task = tokio::spawn(session.run());
        command_write.write_all(b"{\"seq\":1,\"command_id\":\"018f6f75-43f7-7c2e-8d9a-0f6c83e75b1a\",\"command\":{\"type\":\"user_message\",\"text\":\"hold\",\"attachments\":[]}}\n").await.expect("command");
        started_rx.await.expect("provider started");
        drop(command_write);
        let result = tokio::time::timeout(Duration::from_secs(2), session_task)
            .await
            .expect("Session shutdown hung")
            .expect("session join");
        assert!(matches!(result, SessionResult::Failed { .. }));
        tokio::time::timeout(Duration::from_secs(1), cancelled_rx)
            .await
            .expect("provider producer remained detached")
            .expect("producer observer");
    }

    struct HeldTool {
        entered: Mutex<Option<oneshot::Sender<()>>>,
        dropped: Mutex<Option<oneshot::Sender<bool>>>,
    }

    struct ObserveToolDrop {
        cancel: CancellationToken,
        dropped: Option<oneshot::Sender<bool>>,
    }

    impl Drop for ObserveToolDrop {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                let _ = dropped.send(self.cancel.is_cancelled());
            }
        }
    }

    #[async_trait]
    impl Tool for HeldTool {
        fn def(&self) -> ToolDefinition {
            FakeTool {
                executions: Arc::new(AtomicUsize::new(0)),
            }
            .def()
        }

        fn risk(&self) -> ToolRisk {
            ToolRisk::ReadOnly
        }

        async fn execute(&self, ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
            let _drop = ObserveToolDrop {
                cancel: ctx.cancel.clone(),
                dropped: self.dropped.lock().expect("dropped").take(),
            };
            if let Some(entered) = self.entered.lock().expect("entered").take() {
                let _ = entered.send(());
            }
            pending::<()>().await;
            unreachable!()
        }
    }

    #[tokio::test]
    async fn json_lines_shutdown_cancels_held_tool_before_dropping_execution_future() {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let mut builder = ToolRegistryBuilder::default();
        builder
            .register(Arc::new(HeldTool {
                entered: Mutex::new(Some(entered_tx)),
                dropped: Mutex::new(Some(dropped_tx)),
            }))
            .expect("register");
        let registry = builder.build();
        let prompt = PromptContext {
            system_prompt: "fixture".to_owned(),
            memory_blocks: vec![],
            messages: vec![],
            provider_context: vec![],
            tools: registry.definitions(),
        };
        let (base_url, server, _) = serve_tool_then_text().await;
        let mut spec = ModelSpec::preset("kimi-k3").expect("preset");
        spec.base_url = base_url;
        let starter: Arc<StreamStarter> = Arc::new(|spec, context, options, cancel, observer| {
            stream_with_api_key_observed(
                spec,
                context,
                options,
                cancel,
                Some("test".to_owned()),
                Some(observer),
            )
        });
        let driver = Arc::new(
            InjectedRunDriver::with_stream_starter(
                spec,
                RequestOptions::default(),
                Some(prompt),
                Some(registry),
                Some(WorkspacePaths::new("/workspace").expect("workspace")),
                Some(generation(11)),
                starter,
            )
            .expect("driver"),
        );
        let store = Store::session_test_store("injected-held-tool")
            .await
            .expect("store");
        let digest = store.command_digest_factory().await.expect("digest");
        let (command_read, mut command_write) = tokio::io::duplex(4096);
        let (event_write, _event_read) = tokio::io::duplex(64 * 1024);
        let gateway = InjectedStdioGateway::new(BufReader::new(command_read), event_write, digest);
        let session = Session::start(
            store,
            gateway,
            RunCore::new(),
            Arc::new(SequentialRunWorker::new(driver)),
            generation(11),
        )
        .await
        .expect("session");
        let session_task = tokio::spawn(session.run());
        command_write.write_all(b"{\"seq\":1,\"command_id\":\"018f6f75-43f7-7c2e-8d9a-0f6c83e75b1a\",\"command\":{\"type\":\"user_message\",\"text\":\"hold tool\",\"attachments\":[]}}\n").await.expect("command");
        entered_rx.await.expect("tool entered");
        drop(command_write);
        let result = tokio::time::timeout(Duration::from_secs(2), session_task)
            .await
            .expect("Session shutdown hung")
            .expect("join");
        assert!(matches!(result, SessionResult::Failed { .. }));
        assert!(
            tokio::time::timeout(Duration::from_secs(1), dropped_rx)
                .await
                .expect("tool future stayed detached")
                .expect("drop observer"),
            "tool future was dropped before cancellation"
        );
        server.abort();
    }

    #[tokio::test]
    async fn provider_send_view_is_transformed_without_mutating_active_context() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let captured = sent.clone();
        let starter: Arc<StreamStarter> =
            Arc::new(move |spec, prompt, _options, cancel, _observer| {
                *captured.lock().expect("sent") = prompt.messages;
                let (tx, rx) = mpsc::channel(1);
                drop(tx);
                ProviderEventStream::new(rx, cancel, spec.provider.clone(), spec.origin())
            });
        let (spec, prompt, registry, workspace) = dependencies();
        let driver = InjectedRunDriver::with_stream_starter(
            spec.clone(),
            RequestOptions::default(),
            Some(prompt),
            Some(registry),
            Some(workspace),
            Some(generation(1)),
            starter,
        )
        .expect("driver");

        let user = ContextMessage::Persisted {
            id: "u1".to_owned(),
            seq: 1,
            message: Message::User(UserMessage {
                content: vec![UserContent::Text {
                    text: "hello".to_owned(),
                }],
                timestamp: Utc::now(),
            }),
        };
        let error = ContextMessage::Persisted {
            id: "e1".to_owned(),
            seq: 2,
            message: Message::Assistant(AssistantMessage {
                content: Vec::new(),
                model: "fixture-model".to_owned(),
                provider: "fixture".to_owned(),
                origin: spec.origin(),
                usage: Usage::default(),
                stop_reason: StopReason::Error,
                error_message: Some("boom".to_owned()),
                provider_code: None,
                interrupted: false,
                timestamp: Utc::now(),
            }),
        };
        let cross_model = ContextMessage::Persisted {
            id: "a1".to_owned(),
            seq: 3,
            message: Message::Assistant(AssistantMessage {
                content: vec![
                    AssistantContent::Thinking {
                        thinking: "private".to_owned(),
                        signature_field: "reasoning_content".to_owned(),
                        wire_item_index: 0,
                    },
                    AssistantContent::Text {
                        text: "visible".to_owned(),
                        wire_item_index: 1,
                    },
                ],
                model: "different-model".to_owned(),
                provider: spec.provider.clone(),
                origin: spec.origin(),
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                provider_code: None,
                interrupted: false,
                timestamp: Utc::now(),
            }),
        };
        let active_context = vec![user, error, cross_model];
        let active_context_clone = active_context.clone();
        driver
            .start_provider_for_command(0, &active_context, None, CancellationToken::new())
            .await
            .expect("start");

        let captured = sent.lock().expect("captured").clone();
        let expected = crate::memory::transform::transform(&active_context, &spec.origin());
        assert_eq!(
            captured, expected,
            "provider must receive the transformed send view"
        );
        assert_eq!(
            active_context, active_context_clone,
            "retained active context must not be mutated"
        );
        assert_eq!(
            captured.len(),
            2,
            "Error assistant must be excluded while cross-model visible text is retained"
        );
        let ContextMessage::Persisted {
            message: Message::Assistant(assistant),
            ..
        } = &captured[1]
        else {
            panic!("expected transformed cross-model assistant");
        };
        assert_eq!(
            assistant.content,
            vec![AssistantContent::Text {
                text: "visible".to_owned(),
                wire_item_index: 1,
            }],
            "destination-sensitive cross-model thinking must not be replayed"
        );
    }
}
