//! T26 production runtime bootstrap and `RunCore` composition boundary.
//!
//! This is the only place that assembles a production `RunCore` from the
//! `ProcessGeneration` lease, `RpcBootNonce`, `GenerationRecoveryFence`, store
//! scope, `ToolRegistry`, provider, and Gateway.  It is intentionally
//! fail-closed: missing identity, lease/fence, or T17/T21/T23/T24 pieces stop
//! startup before any command is admitted.

use std::{env, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use crate::{
    agent::{InjectedRunDriver, RunCore, RunWorker, SequentialRunWorker, Session},
    approval::{FailClosedApprovalBroker, RuntimeApprovalBroker},
    config::Config,
    gateway::StdioGateway,
    provider::{
        ProviderTimingObserver,
        model::{ModelSpec, RequestOptions},
        types::{MemoryBlock, PromptContext, ProviderEventStream},
    },
    runtime::{
        contracts::{
            GenerationRecoveryFence, HydrationReady, HydrationReceiptIdentity, ProcessGeneration,
            ProcessGenerationLease, RpcBootNonce,
        },
        publisher::{RuntimeHeartbeatPublisher, RuntimeStatePublisher},
    },
    store::{
        AgentScope, EnvironmentKeyProvider, EventWriter, HydrationOutcome, Store, SuffixRecovery,
    },
    t27_recovery,
    tools::{
        ToolRegistry, WorkspacePaths,
        executor::{ExecutorClient, remote_executor_registry, set_dumpable, wait_for_unix_socket},
    },
};

/// Production bootstrap context, parsed from the supervisor-supplied environment.
struct BootstrapContext {
    generation: ProcessGeneration,
    nonce: RpcBootNonce,
    lease: ProcessGenerationLease,
    fence: GenerationRecoveryFence,
    tenant_id: String,
    agent_id: String,
    conversation_id: String,
    state_dir: PathBuf,
    runtime_state_dir: PathBuf,
    executor_socket: PathBuf,
    t27_recovery_request_dir: PathBuf,
    t27_supervisor_proof_dir: PathBuf,
    wrapping_key_id: String,
}

impl BootstrapContext {
    fn from_env() -> Result<Self> {
        let generation = parse_generation(&required("SUMI_RPC_GENERATION")?)?;
        let nonce = RpcBootNonce::new(required("SUMI_RPC_NONCE")?)
            .context("SUMI_RPC_NONCE is not a valid RPC boot nonce")?;
        let lease_id = required("SUMI_PROCESS_GENERATION_LEASE_ID")?;
        let lease = ProcessGenerationLease::new(generation, lease_id)
            .context("SUMI_PROCESS_GENERATION_LEASE_ID is not a valid lease identity")?;
        let fence_id = required("SUMI_GENERATION_RECOVERY_FENCE_ID")?;
        let fence = GenerationRecoveryFence::new(&lease, fence_id)
            .context("SUMI_GENERATION_RECOVERY_FENCE_ID is not a valid fence identity")?;

        let tenant_id = required("SUMI_TENANT_ID")?;
        let agent_id = required("SUMI_AGENT_ID")?;
        let conversation_id = required("SUMI_CONVERSATION_ID")?;

        let state_dir = required_path("SUMI_STATE_DIR")?;
        let runtime_state_dir = required_path("SUMI_AGENT_RUNTIME_STATE_DIR")?;
        let executor_socket = required_path("SUMI_EXECUTOR_SOCKET")?;
        let t27_recovery_request_dir = required_path("SUMI_T27_RECOVERY_REQUEST_DIR")?;
        let t27_supervisor_proof_dir = required_path("SUMI_T27_SUPERVISOR_PROOF_DIR")?;

        let wrapping_key_id = env::var("SUMI_AGENT_WRAPPING_KEY_ID")
            .unwrap_or_else(|_| "env-wrapping-key/v1".to_owned());

        // Validate presence of the wrapping key without retaining the raw string.
        // `EnvironmentKeyProvider::from_env` re-reads and zeroizes the value
        // when the bootstrap builds the sole key provider.
        let _ = Zeroizing::new(required("SUMI_AGENT_WRAPPING_KEY")?);

        Ok(Self {
            generation,
            nonce,
            lease,
            fence,
            tenant_id,
            agent_id,
            conversation_id,
            state_dir,
            runtime_state_dir,
            executor_socket,
            t27_recovery_request_dir,
            t27_supervisor_proof_dir,
            wrapping_key_id,
        })
    }

    fn rpc_identity(&self) -> crate::runtime::contracts::RpcIdentity {
        crate::runtime::contracts::RpcIdentity::new(self.generation, self.nonce.clone())
    }

    fn scope(&self) -> AgentScope {
        AgentScope {
            tenant_id: self.tenant_id.clone(),
            agent_id: self.agent_id.clone(),
            conversation_id: self.conversation_id.clone(),
        }
    }
}

fn required(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} is required for production bootstrap"))
}

fn required_path(name: &str) -> Result<PathBuf> {
    let value = required(name)?;
    let path = PathBuf::from(&value);
    if !path.is_absolute() {
        bail!("{name} must be an absolute path: {value}");
    }
    Ok(path)
}

fn parse_generation(value: &str) -> Result<ProcessGeneration> {
    let raw = value
        .parse::<u64>()
        .with_context(|| format!("SUMI_RPC_GENERATION is not an unsigned integer: {value}"))?;
    ProcessGeneration::from_wire(raw)
        .with_context(|| format!("SUMI_RPC_GENERATION {raw} is outside the valid domain"))
}

/// Run the production bootstrap with the real provider stream starter.
pub async fn run_production() -> Result<()> {
    run_production_with_driver_and_broker(
        Arc::new(crate::provider::stream_observed),
        None,
        Some(Arc::new(FailClosedApprovalBroker::new())),
    )
    .await
}

/// Run the production bootstrap with an injected stream starter and optional
/// tool registry.  `tool_registry` is intended for tests; when `None` the
/// production remote executor registry is built from `SUMI_EXECUTOR_SOCKET`.
#[allow(dead_code)]
pub async fn run_production_with_driver(
    stream_starter: Arc<
        dyn Fn(
                ModelSpec,
                PromptContext,
                RequestOptions,
                CancellationToken,
                ProviderTimingObserver,
            ) -> ProviderEventStream
            + Send
            + Sync,
    >,
    tool_registry: Option<ToolRegistry>,
) -> Result<()> {
    run_production_with_driver_and_broker(stream_starter, tool_registry, None).await
}

/// Internal bootstrap seam that lets tests inject a `RuntimeApprovalBroker`.
pub(crate) async fn run_production_with_driver_and_broker(
    stream_starter: Arc<
        dyn Fn(
                ModelSpec,
                PromptContext,
                RequestOptions,
                CancellationToken,
                ProviderTimingObserver,
            ) -> ProviderEventStream
            + Send
            + Sync,
    >,
    tool_registry: Option<ToolRegistry>,
    approval_broker: Option<Arc<dyn RuntimeApprovalBroker>>,
) -> Result<()> {
    set_dumpable(0).context("failed to set PR_SET_DUMPABLE")?;
    // Do this before reading the bootstrap identity, lease, fence, or wrapping
    // key from the environment. Production runtime processes must never hold
    // those secrets while dumpable.
    let ctx = BootstrapContext::from_env()?;

    // Publish the current generation as NotReady before any ready state can be
    // observed or admitted. The file is the durable boundary T28 uses; the
    // in-process watch channel alone is not enough.
    let runtime_state_publisher =
        RuntimeStatePublisher::new(&ctx.runtime_state_dir, &ctx.agent_id, ctx.generation)
            .context("failed to create runtime state publisher")?;
    runtime_state_publisher
        .publish_not_ready()
        .context("failed to publish initial not-ready runtime state")?;
    // T27 liveness is a separate, lease/fence-bound record.  It intentionally
    // does not extend the strict T28 RuntimeState schema.
    let heartbeat = RuntimeHeartbeatPublisher::new(
        &ctx.runtime_state_dir,
        &ctx.agent_id,
        ctx.generation,
        ctx.lease.clone(),
        ctx.fence.clone(),
    )?;
    heartbeat
        .pulse()
        .context("failed to publish initial runtime heartbeat")?;
    let _heartbeat_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            if let Err(error) = heartbeat.pulse() {
                tracing::error!(%error, "runtime heartbeat pulse failed");
                break;
            }
        }
    });

    // Load config last so missing model/system files are surfaced after the
    // identity boundary is validated.
    let config = Config::load()
        .await
        .context("failed to load runtime configuration")?;

    // The conversation_id from config must match the authenticated scope; do
    // not let config-derived defaults diverge from the supervisor credential.
    if config.conversation_id != ctx.conversation_id {
        bail!(
            "config conversation_id {} does not match SUMI_CONVERSATION_ID {}",
            config.conversation_id,
            ctx.conversation_id
        );
    }

    let model_spec = config
        .model_spec()
        .context("failed to resolve production model spec")?;

    let key_provider: Arc<dyn crate::store::KeyProvider> = Arc::new(
        EnvironmentKeyProvider::from_env("SUMI_AGENT_WRAPPING_KEY", &ctx.wrapping_key_id)
            .context("failed to initialize wrapping key provider")?,
    );

    let database_path = ctx.state_dir.join("agent.db");
    let store = Store::open(&database_path, ctx.scope(), key_provider)
        .await
        .context("failed to open durable store")?;

    let event_writer = EventWriter::new(Arc::new(store.clone()));
    event_writer
        .initialize_recovery_checkpoint()
        .await
        .context("failed to initialize recovery checkpoint")?;
    let pending_recovery = SuffixRecovery::recover_t12_prefix(&store, &event_writer)
        .await
        .context("T12 prefix recovery failed")?;
    if !pending_recovery.is_empty() {
        bail!(
            "durable suffix recovery is required; T17 production hydration must resolve {:?} before T26 composition",
            pending_recovery
        );
    }

    let (state, runtime_receipt) =
        hydrate_store(&store, &event_writer, &ctx.lease, &ctx.fence, &ctx)
            .await
            .context("T17/T27 durable hydration failed")?;
    if !state.recovery_steps.is_empty() {
        bail!(
            "durable suffix recovery steps remain after hydration; T17/T27 must resolve {:?} before T26 composition",
            state.recovery_steps
        );
    }

    let registry = match tool_registry {
        Some(registry) => registry,
        None => build_remote_tool_registry(&ctx).await?,
    };

    let prompt = PromptContext {
        system_prompt: config.system_prompt.clone(),
        // T21 owns memory-block assembly from the hydrated memory records.
        // We preserve the raw `HydratedRunState` in the driver so T21 can
        // project it without introducing a fresh-only/empty-context fallback.
        memory_blocks: Vec::<MemoryBlock>::new(),
        messages: state.messages.clone(),
        provider_context: state.provider_context.clone(),
        tools: registry.definitions(),
    };

    // The runtime deliberately uses an empty rootfs-local placeholder.  The
    // production deployment never mounts the tenant workspace into this
    // container; all real workspace access is delegated to the remote
    // executor.  `InjectedRunDriver` still carries a `WorkspacePaths` value
    // because that is the neutral Tool trait context, but the frozen remote
    // registry never dereferences it.
    let workspace = WorkspacePaths::new("/workspace")
        .context("runtime rootfs is missing the inert workspace placeholder")?;

    let driver = InjectedRunDriver::with_stream_starter(
        model_spec,
        RequestOptions::default(),
        Some(prompt),
        Some(registry),
        Some(workspace),
        Some(ctx.generation),
        stream_starter,
    )
    .context("failed to construct the production run driver")?
    .with_approval_broker(approval_broker)
    .with_hydrated_state(Some(state.clone()));

    // Latch Ready only after all composition checks succeed. The identity is
    // the stable T17 hydration receipt for a clean conversation or the durable
    // T27 physical-recovery receipt when running tools were recovered.
    let (hydration_tx, hydration_rx) = watch::channel(HydrationReady::not_ready());
    let mut ready = HydrationReady::not_ready();
    ready
        .latch(ctx.generation, runtime_receipt.clone())
        .context("failed to latch hydration ready state")?;

    let core = RunCore::new()
        .with_runtime_context(state.messages)
        .with_recovery_steps(state.recovery_steps)
        .with_hydration(hydration_rx);
    let worker: Arc<dyn RunWorker> = Arc::new(SequentialRunWorker::new(Arc::new(driver)));

    let command_digest_factory = store
        .command_digest_factory()
        .await
        .context("failed to build command digest factory")?;
    let gateway = StdioGateway::new(command_digest_factory);

    let mut session = Session::prepare(store, gateway, core, worker, ctx.generation)
        .await
        .context("failed to install production command gate and session")?;

    // This branch does not yet contain the T21/T23/T24 production composition.
    // Keep the durable state explicitly NotReady until those dependencies are
    // integrated; the local stdio/fail-closed placeholders are not release
    // substitutes and must never publish production readiness.
    ensure_production_dependencies_integrated()?;

    // Install and validate the in-process command gate first. Session::run has
    // not started, so no command can be admitted. Durable Ready is the final
    // fallible startup operation; a publication failure drops the prepared
    // Session while the prior NotReady file remains authoritative.
    hydration_tx.send_replace(ready);
    session
        .await_hydration_ready()
        .await
        .context("failed to open installed hydration command gate")?;
    runtime_state_publisher
        .publish_ready(&runtime_receipt)
        .context("failed to publish ready runtime state")?;
    tracing::info!(
        generation = ctx.generation.as_u64(),
        lease_id = ctx.lease.lease_id(),
        fence_id = ctx.fence.fence_id(),
        hydration_receipt_identity = %runtime_receipt.as_str(),
        "production hydration ready latched"
    );

    session.run().await;
    Ok(())
}

fn ensure_production_dependencies_integrated() -> Result<()> {
    bail!(
        "production composition remains NotReady until T21 memory, T23 approval, and T24 Gateway dependencies are integrated"
    )
}

async fn build_remote_tool_registry(ctx: &BootstrapContext) -> Result<ToolRegistry> {
    wait_for_unix_socket(&ctx.executor_socket, "executor").await?;
    let identity = ctx.rpc_identity();
    let client = Arc::new(ExecutorClient::new(
        &ctx.executor_socket,
        identity,
        &ctx.conversation_id,
    ));
    remote_executor_registry(client).context("failed to build remote tool registry")
}

async fn hydrate_store(
    store: &Store,
    writer: &EventWriter,
    lease: &ProcessGenerationLease,
    fence: &GenerationRecoveryFence,
    ctx: &BootstrapContext,
) -> Result<(crate::store::HydratedRunState, HydrationReceiptIdentity)> {
    let mut recovery_identity = None;
    loop {
        match store.hydrate(lease, fence).await? {
            HydrationOutcome::RecoveryRequired(intents) => {
                let receipt = t27_recovery::consume_physical_recovery(
                    writer,
                    lease,
                    fence,
                    intents,
                    &ctx.tenant_id,
                    &ctx.agent_id,
                    &ctx.conversation_id,
                    &ctx.t27_recovery_request_dir,
                    &ctx.t27_supervisor_proof_dir,
                )
                .await?;
                recovery_identity = Some(
                    HydrationReceiptIdentity::new(receipt.receipt_id)
                        .context("failed to construct recovery hydration receipt identity")?,
                );
            }
            HydrationOutcome::Complete(state) => {
                let identity = match recovery_identity {
                    Some(identity) => identity,
                    None => HydrationReceiptIdentity::new(format!(
                        "{}:{}:{}:{}",
                        state.receipt.lease_id,
                        state.receipt.fence_id,
                        state.receipt.generation.as_u64(),
                        state.receipt.intent_count
                    ))
                    .context("failed to construct stable hydration receipt identity")?,
                };
                return Ok((state, identity));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::*;
    use crate::tools::ToolRegistryBuilder;
    use uuid::Uuid;

    // Serialize the environment-mutating tests. `set_var`/`remove_var` are
    // process-global and `unsafe` in Rust 2024; concurrent tests race on the
    // same environment and cause observed root-gate transient failures.
    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    // Environment mutation is `unsafe` in Rust 2024; centralize it so tests
    // stay readable and the unsafe boundary is explicit.
    unsafe fn set_env(key: &str, value: impl AsRef<str>) {
        unsafe { env::set_var(key, value.as_ref()) };
    }

    unsafe fn remove_env(key: &str) {
        unsafe { env::remove_var(key) };
    }

    unsafe fn apply_env(vars: &[(&'static str, String)]) {
        for (key, value) in vars {
            unsafe { set_env(key, value) };
        }
    }

    unsafe fn clear_env(vars: &[(&'static str, String)]) {
        for (key, _) in vars {
            unsafe { remove_env(key) };
        }
    }

    fn test_env_prefix(dir: &Path) -> Vec<(&'static str, String)> {
        vec![
            ("SUMI_TENANT_ID", "tenant-1".to_owned()),
            ("SUMI_AGENT_ID", "agent-1".to_owned()),
            ("SUMI_CONVERSATION_ID", "conversation-1".to_owned()),
            (
                "SUMI_STATE_DIR",
                dir.join("state").to_string_lossy().into_owned(),
            ),
            (
                "SUMI_AGENT_RUNTIME_STATE_DIR",
                dir.join("runtime-state").to_string_lossy().into_owned(),
            ),
            (
                "SUMI_WORKSPACE",
                dir.join("workspace").to_string_lossy().into_owned(),
            ),
            (
                "SUMI_EXECUTOR_SOCKET",
                dir.join("executor.sock").to_string_lossy().into_owned(),
            ),
            (
                "SUMI_ARTIFACT_BROKER_SOCKET",
                dir.join("broker.sock").to_string_lossy().into_owned(),
            ),
            (
                "SUMI_T27_RECOVERY_REQUEST_DIR",
                dir.join("t27")
                    .join("requests")
                    .to_string_lossy()
                    .into_owned(),
            ),
            (
                "SUMI_T27_SUPERVISOR_PROOF_DIR",
                dir.join("t27")
                    .join("supervisor-proofs")
                    .to_string_lossy()
                    .into_owned(),
            ),
            (
                "SUMI_ARTIFACT_ROOT",
                dir.join("artifacts").to_string_lossy().into_owned(),
            ),
            (
                "SUMI_AGENT_WRAPPING_KEY",
                "4242424242424242424242424242424242424242424242424242424242424242".to_owned(),
            ),
        ]
    }

    fn fresh_dir() -> PathBuf {
        std::env::temp_dir().join(format!("sumi-bootstrap-{}", Uuid::now_v7()))
    }

    fn assert_durable_not_ready(dir: &Path, generation: ProcessGeneration) {
        let publisher =
            RuntimeStatePublisher::new(dir, "agent-1", generation).expect("state publisher");
        let state: crate::runtime::publisher::RuntimeState = serde_json::from_str(
            &fs::read_to_string(publisher.file_path()).expect("runtime state"),
        )
        .expect("runtime state JSON");
        assert_eq!(state.generation, generation.as_u64());
        assert_eq!(state.hydration_receipt_identity, None);
    }

    #[test]
    fn hydration_ready_latches_exactly_once_and_rejects_rollover_without_invalidation() {
        let mut ready = HydrationReady::not_ready();
        let gen1 = ProcessGeneration::from_wire(1).unwrap();
        let gen2 = ProcessGeneration::from_wire(2).unwrap();
        let id1 = HydrationReceiptIdentity::new("receipt-1".to_owned()).unwrap();
        let id2 = HydrationReceiptIdentity::new("receipt-2".to_owned()).unwrap();

        assert!(ready.latch(gen1, id1.clone()).is_ok());
        assert!(ready.latch(gen1, id1.clone()).is_err());
        assert!(ready.latch(gen2, id2.clone()).is_err());
        ready.invalidate();
        assert!(ready.latch(gen2, id2).is_ok());
    }

    #[test]
    fn missing_production_dependencies_leave_durable_state_not_ready() {
        let dir = fresh_dir();
        let generation = ProcessGeneration::from_wire(7).unwrap();
        let publisher = RuntimeStatePublisher::new(&dir, "agent-1", generation).expect("publisher");
        publisher.publish_not_ready().expect("initial NotReady");

        let error = ensure_production_dependencies_integrated()
            .expect_err("incomplete production composition must fail closed");
        assert!(error.to_string().contains("T21 memory"));

        assert_durable_not_ready(&dir, generation);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn bootstrap_context_rejects_missing_identity() {
        let _guard = ENV_LOCK.blocking_lock();
        let dir = fresh_dir();
        unsafe {
            apply_env(&test_env_prefix(&dir));
            remove_env("SUMI_RPC_GENERATION");
        }
        assert!(BootstrapContext::from_env().is_err());
        unsafe {
            clear_env(&test_env_prefix(&dir));
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bootstrap_fails_closed_with_mismatched_rpc_identity() {
        let _guard = ENV_LOCK.lock().await;
        let dir = fresh_dir();
        unsafe {
            apply_env(&test_env_prefix(&dir));
            set_env("SUMI_RPC_GENERATION", "1");
            set_env("SUMI_RPC_NONCE", "nonce-1");
            set_env("SUMI_PROCESS_GENERATION_LEASE_ID", "lease-1");
            set_env("SUMI_GENERATION_RECOVERY_FENCE_ID", "fence-for-other-lease");
        }

        let result = run_production_with_driver(inert_starter(), Some(empty_registry())).await;
        assert!(result.is_err(), "mismatched lease/fence must fail closed");
        assert!(
            !dir.join("runtime-state").exists(),
            "invalid bootstrap identity must fail before publishing runtime state"
        );

        unsafe {
            clear_env(&test_env_prefix(&dir));
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bootstrap_fails_closed_when_executor_socket_missing() {
        let _guard = ENV_LOCK.lock().await;
        let dir = fresh_dir();
        unsafe {
            apply_env(&test_env_prefix(&dir));
            set_env("SUMI_RPC_GENERATION", "1");
            set_env("SUMI_RPC_NONCE", "nonce-1");
            set_env("SUMI_PROCESS_GENERATION_LEASE_ID", "lease-1");
            set_env("SUMI_GENERATION_RECOVERY_FENCE_ID", "fence-for-lease-1");
        }

        let result = run_production_with_driver(inert_starter(), None).await;
        assert!(
            result.is_err(),
            "missing executor socket must fail closed before session start"
        );
        assert_durable_not_ready(
            &dir.join("runtime-state"),
            ProcessGeneration::from_wire(1).unwrap(),
        );

        unsafe {
            clear_env(&test_env_prefix(&dir));
        }
        let _ = fs::remove_dir_all(&dir);
    }

    fn inert_starter() -> Arc<
        dyn Fn(
                ModelSpec,
                PromptContext,
                RequestOptions,
                CancellationToken,
                ProviderTimingObserver,
            ) -> ProviderEventStream
            + Send
            + Sync,
    > {
        use crate::provider::types::{ApiProtocol, ProviderEventStream, ProviderOrigin};

        Arc::new(
            |_spec: ModelSpec,
             _prompt: PromptContext,
             _options: RequestOptions,
             cancel: CancellationToken,
             _observer: ProviderTimingObserver| {
                // The closure is only exercised when the bootstrap reaches
                // `InjectedRunDriver` construction; the fail-closed tests stop
                // before that.  A closed channel yields an empty stream.
                let (_tx, rx) =
                    tokio::sync::mpsc::channel::<crate::provider::types::ProviderEvent>(1);
                drop(_tx);
                ProviderEventStream::new(
                    rx,
                    cancel,
                    "inert".to_owned(),
                    ProviderOrigin {
                        provider_instance_id: "inert".to_owned(),
                        protocol: ApiProtocol::OpenAiChatCompletions,
                        model: "inert".to_owned(),
                    },
                )
            },
        )
    }

    fn empty_registry() -> ToolRegistry {
        ToolRegistryBuilder::default().build()
    }
}
