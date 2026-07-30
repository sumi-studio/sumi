//! T26 production composition root for one PersonalityAgent runtime.
//!
//! Every authority-bearing dependency is constructed from one exact
//! supervisor allocation.  The normal process path has no stdio, synthetic
//! provider, local workspace tool, or fresh-only memory fallback.

use std::{
    env,
    ffi::OsString,
    net::IpAddr,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use futures_util::future::BoxFuture;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use crate::{
    agent::{
        InjectedRunDriver, RunWorker, SequentialRunWorker, Session, SessionResult,
        SessionStartAuthority,
    },
    approval::{
        ApprovalBroker, Policy, SandboxSummary, SecretAwareActionProjector, SecretDigestKey,
        TrustedEnvironment,
    },
    config::Config,
    gateway::{
        local_runtime::{
            FIRST_BROWSER_VERTICAL_COMPONENTS, LocalControlCredential, LocalControlHttpClient,
            LocalControlReadyPublisher as LocalRuntimePublisher, LocalCredentialProvider,
            LocalReadyPublisher, LocalRuntimeComponent, first_browser_vertical_ready_gate,
        },
        supervisor::{
            ConnectionSupervisor, DeliveryAuthorization, SupervisorConfig, seams::T17StoreAdapter,
            session::SessionGateway,
        },
        ws::WebSocketConnector,
    },
    provider::{RequestOptions, types::PromptContext},
    runtime::{allocator::SupervisorAllocation, authority::RuntimeEpochAuthority},
    store::{
        AgentScope, EnvironmentKeyProvider, HydratedRunState, HydrationOutcome, RecoveryStep,
        Redactor, Store,
    },
    tools::{
        WorkspacePaths,
        executor::{ArtifactBrokerClient, ExecutorClient, remote_executor_registry},
    },
};

struct BootstrapContext {
    authority: RuntimeEpochAuthority,
    state_dir: PathBuf,
    executor_socket: PathBuf,
    artifact_broker_socket: PathBuf,
    gateway_url: String,
    allow_insecure_loopback_gateway: bool,
    local_control_endpoint: LocalControlEndpoint,
    local_control_bearer: Zeroizing<String>,
    local_control_bearer_expires_at: SystemTime,
    wrapping_key_id: String,
    approval_secret_digest_key: [u8; 32],
}

enum LocalControlEndpoint {
    Unix {
        socket: PathBuf,
        server_uid: u32,
        socket_gid: u32,
    },
    Loopback(String),
}

impl BootstrapContext {
    fn from_process_env() -> Result<Self> {
        Self::from_source(|name| env::var_os(name))
    }

    fn from_source(mut get: impl FnMut(&str) -> Option<OsString>) -> Result<Self> {
        let personality_agent_id = required_value(&mut get, "SUMI_PERSONALITY_AGENT_ID")?;
        let generation = required_value(&mut get, "SUMI_RPC_GENERATION")?;
        let nonce = required_value(&mut get, "SUMI_RPC_NONCE")?;
        let lease_id = required_value(&mut get, "SUMI_PROCESS_GENERATION_LEASE_ID")?;
        let fence_id = required_value(&mut get, "SUMI_GENERATION_RECOVERY_FENCE_ID")?;
        let allocation = SupervisorAllocation::from_wire(
            &personality_agent_id,
            &generation,
            nonce,
            lease_id,
            fence_id,
        )?;

        let state_dir = required_absolute_path(&mut get, "SUMI_STATE_DIR")?;
        let executor_socket = required_absolute_path(&mut get, "SUMI_EXECUTOR_SOCKET")?;
        let artifact_broker_socket =
            required_absolute_path(&mut get, "SUMI_ARTIFACT_BROKER_SOCKET")?;
        let gateway_url = required_value(&mut get, "SUMI_GATEWAY_URL")?;
        let local_control_endpoint = local_control_endpoint_from_env(&mut get)?;
        let local_control_bearer = required_value(&mut get, "SUMI_LOCAL_CONTROL_BEARER")?;
        let local_control_bearer_expires_at = parse_unix_time(
            "SUMI_LOCAL_CONTROL_BEARER_EXPIRES_AT_UNIX",
            &required_value(&mut get, "SUMI_LOCAL_CONTROL_BEARER_EXPIRES_AT_UNIX")?,
        )?;
        let wrapping_key_id = required_value(&mut get, "SUMI_AGENT_WRAPPING_KEY_ID")?;
        let approval_secret_digest_key = decode_key(
            "SUMI_APPROVAL_SECRET_DIGEST_KEY",
            &required_value(&mut get, "SUMI_APPROVAL_SECRET_DIGEST_KEY")?,
        )?;
        let allow_insecure_loopback_gateway = match get("SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY") {
            None => false,
            Some(value) if value == "true" => true,
            Some(_) => {
                bail!("SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY must be exactly `true` or unset")
            }
        };
        if get("SUMI_CONFIG").is_none() && get("SUMI_MODEL_PRESET").is_none() {
            bail!("production provider configuration requires SUMI_CONFIG or SUMI_MODEL_PRESET");
        }

        Ok(Self {
            authority: allocation.into_authority(),
            state_dir,
            executor_socket,
            artifact_broker_socket,
            gateway_url,
            allow_insecure_loopback_gateway,
            local_control_endpoint,
            local_control_bearer: Zeroizing::new(local_control_bearer),
            local_control_bearer_expires_at,
            wrapping_key_id,
            approval_secret_digest_key,
        })
    }
}

fn local_control_endpoint_from_env(
    get: &mut impl FnMut(&str) -> Option<OsString>,
) -> Result<LocalControlEndpoint> {
    let unix_socket = optional_value(get, "SUMI_LOCAL_CONTROL_UNIX_SOCKET")?;
    let loopback_url = optional_value(get, "SUMI_LOCAL_CONTROL_URL")?;
    let server_uid = optional_value(get, "SUMI_LOCAL_CONTROL_SERVER_UID")?;
    let socket_gid = optional_value(get, "SUMI_LOCAL_CONTROL_SOCKET_GID")?;
    match (unix_socket, loopback_url) {
        (Some(socket), None) => {
            let socket = PathBuf::from(socket);
            if !socket.is_absolute() {
                bail!("SUMI_LOCAL_CONTROL_UNIX_SOCKET must be an absolute path");
            }
            let server_uid = server_uid.context(
                "SUMI_LOCAL_CONTROL_SERVER_UID is required with SUMI_LOCAL_CONTROL_UNIX_SOCKET",
            )?;
            let server_uid = server_uid
                .parse::<u32>()
                .context("SUMI_LOCAL_CONTROL_SERVER_UID must be a decimal UID")?;
            let socket_gid = socket_gid.context(
                "SUMI_LOCAL_CONTROL_SOCKET_GID is required with SUMI_LOCAL_CONTROL_UNIX_SOCKET",
            )?;
            let socket_gid = socket_gid
                .parse::<u32>()
                .context("SUMI_LOCAL_CONTROL_SOCKET_GID must be a decimal GID")?;
            Ok(LocalControlEndpoint::Unix {
                socket,
                server_uid,
                socket_gid,
            })
        }
        (None, Some(url)) => {
            if server_uid.is_some() {
                bail!(
                    "SUMI_LOCAL_CONTROL_SERVER_UID is only valid with SUMI_LOCAL_CONTROL_UNIX_SOCKET"
                );
            }
            if socket_gid.is_some() {
                bail!(
                    "SUMI_LOCAL_CONTROL_SOCKET_GID is only valid with SUMI_LOCAL_CONTROL_UNIX_SOCKET"
                );
            }
            Ok(LocalControlEndpoint::Loopback(url))
        }
        (Some(_), Some(_)) => {
            bail!(
                "SUMI_LOCAL_CONTROL_UNIX_SOCKET and SUMI_LOCAL_CONTROL_URL are mutually exclusive"
            )
        }
        (None, None) => {
            if server_uid.is_some() {
                bail!("SUMI_LOCAL_CONTROL_SERVER_UID requires SUMI_LOCAL_CONTROL_UNIX_SOCKET");
            }
            if socket_gid.is_some() {
                bail!("SUMI_LOCAL_CONTROL_SOCKET_GID requires SUMI_LOCAL_CONTROL_UNIX_SOCKET");
            }
            bail!(
                "exactly one of SUMI_LOCAL_CONTROL_UNIX_SOCKET or SUMI_LOCAL_CONTROL_URL is required for production bootstrap"
            )
        }
    }
}

fn optional_value(
    get: &mut impl FnMut(&str) -> Option<OsString>,
    name: &str,
) -> Result<Option<String>> {
    get(name)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| anyhow!("{name} must be valid UTF-8"))
                .and_then(|value| {
                    if value.is_empty() {
                        bail!("{name} must not be empty when set");
                    }
                    Ok(value)
                })
        })
        .transpose()
}

fn required_value(get: &mut impl FnMut(&str) -> Option<OsString>, name: &str) -> Result<String> {
    get(name)
        .with_context(|| format!("{name} is required for production bootstrap"))?
        .into_string()
        .map_err(|_| anyhow!("{name} must be valid UTF-8"))
}

fn required_absolute_path(
    get: &mut impl FnMut(&str) -> Option<OsString>,
    name: &str,
) -> Result<PathBuf> {
    let value = required_value(get, name)?;
    let path = PathBuf::from(&value);
    if !path.is_absolute() {
        bail!("{name} must be an absolute path");
    }
    Ok(path)
}

fn parse_unix_time(name: &str, value: &str) -> Result<SystemTime> {
    let seconds = value
        .parse::<u64>()
        .with_context(|| format!("{name} must be a nonnegative Unix timestamp"))?;
    UNIX_EPOCH
        .checked_add(Duration::from_secs(seconds))
        .with_context(|| format!("{name} overflows SystemTime"))
}

fn decode_key(name: &str, value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{name} must be exactly 64 hexadecimal characters");
    }
    let mut key = [0_u8; 32];
    for (index, slot) in key.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .with_context(|| format!("{name} contains invalid hexadecimal"))?;
    }
    Ok(key)
}

/// T17 authenticates and orders this plan, but intentionally does not assign
/// runtime semantics to it. The Store/EventWriter owner must supply the
/// executor that applies each step and reaches a new authenticated hydration
/// fixed point. Bootstrap never translates a step into ad-hoc projections.
#[async_trait]
trait LogicalRecoveryExecutor: Send + Sync {
    async fn execute(
        &self,
        store: &Store,
        steps: &[RecoveryStep],
        authority: &RuntimeEpochAuthority,
    ) -> Result<()>;
}

struct LogicalRecoveryExecutorUnavailable;

#[async_trait]
impl LogicalRecoveryExecutor for LogicalRecoveryExecutorUnavailable {
    async fn execute(
        &self,
        _store: &Store,
        steps: &[RecoveryStep],
        _authority: &RuntimeEpochAuthority,
    ) -> Result<()> {
        bail!(
            "authenticated logical recovery has {} ordered step(s), but no Store-owned LogicalRecoveryExecutor is composed; runtime remains NotReady",
            steps.len()
        )
    }
}

async fn hydrate_to_fixed_point(
    store: &Store,
    authority: &RuntimeEpochAuthority,
    recovery: &dyn LogicalRecoveryExecutor,
) -> Result<HydratedRunState> {
    let mut previous_plan = None;
    loop {
        match store
            .hydrate(authority.lease(), authority.fence())
            .await
            .context("hydrate authenticated Store")?
        {
            HydrationOutcome::Complete(hydrated) => return Ok(hydrated),
            HydrationOutcome::PhysicalRecoveryRequired(intents) => {
                bail!(
                    "physical recovery is required for {} execution(s); runtime remains NotReady",
                    intents.len()
                )
            }
            HydrationOutcome::LogicalRecoveryRequired { steps } => {
                if steps.is_empty() {
                    bail!("T17 returned an empty LogicalRecoveryRequired plan");
                }
                if previous_plan.as_ref() == Some(&steps) {
                    bail!(
                        "LogicalRecoveryExecutor returned success without advancing the authenticated recovery plan"
                    );
                }
                previous_plan = Some(steps.clone());
                recovery
                    .execute(store, &steps, authority)
                    .await
                    .context("execute authenticated ordered logical recovery")?;
            }
        }
    }
}

/// A monitor is constructed before Ready and completes only when an already
/// authenticated dependency becomes unavailable. Merely probing a socket inode
/// is not sufficient because it cannot bind PAID/generation/boot nonce.
trait AuthenticatedDependencyMonitor: Send {
    fn failure(self: Box<Self>) -> BoxFuture<'static, Result<()>>;
}

#[derive(Debug, thiserror::Error)]
#[error(
    "authenticated runtime dependency monitoring is unavailable; executor and artifact broker Health streams must bind the exact RpcIdentity before Ready"
)]
struct AuthenticatedDependencyMonitoringUnavailable;

fn authenticated_dependency_monitor(
    _executor: Arc<ExecutorClient>,
    _broker: ArtifactBrokerClient,
) -> Result<Box<dyn AuthenticatedDependencyMonitor>> {
    Err(anyhow::Error::new(
        AuthenticatedDependencyMonitoringUnavailable,
    ))
}

fn artifact_broker_client(context: &BootstrapContext) -> ArtifactBrokerClient {
    ArtifactBrokerClient::new(
        &context.artifact_broker_socket,
        context.authority.rpc_identity().clone(),
    )
}

pub(crate) async fn run_production() -> Result<()> {
    disable_process_dumping()?;
    load_explicit_env_file()?;
    let context = BootstrapContext::from_process_env()?;
    run_with_context(context).await
}

fn load_explicit_env_file() -> Result<()> {
    let Some(path) = env::var_os("SUMI_ENV_FILE") else {
        return Ok(());
    };
    dotenvy::from_path(&path).with_context(|| {
        format!(
            "failed to load environment file {}",
            Path::new(&path).display()
        )
    })
}

fn disable_process_dumping() -> Result<()> {
    let result = unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) };
    if result != 0 {
        bail!(
            "failed to disable process dumping: {}",
            std::io::Error::last_os_error()
        );
    }
    Ok(())
}

async fn run_with_context(mut context: BootstrapContext) -> Result<()> {
    let control_credential = LocalControlCredential::new(
        std::mem::take(&mut *context.local_control_bearer),
        context.authority.rpc_identity().clone(),
        context.local_control_bearer_expires_at,
    )
    .context("invalid local-control bearer")?;
    let control_client = match &context.local_control_endpoint {
        LocalControlEndpoint::Unix {
            socket,
            server_uid,
            socket_gid,
        } => LocalControlHttpClient::new_unix(
            socket,
            *server_uid,
            *socket_gid,
            context.authority.clone(),
            control_credential,
        ),
        LocalControlEndpoint::Loopback(url) => {
            LocalControlHttpClient::new_loopback(url, context.authority.clone(), control_credential)
        }
    }
    .context("construct local-control HTTP client")?;
    let control: Arc<dyn crate::gateway::local_runtime::LocalControlPlane> =
        Arc::new(control_client);
    let publisher = LocalRuntimePublisher::new(context.authority.clone(), control.clone());
    publisher
        .publish_not_ready()
        .await
        .context("publish startup NotReady")?;

    run_after_not_ready(&context, control, &publisher).await
}

async fn run_after_not_ready(
    context: &BootstrapContext,
    control: Arc<dyn crate::gateway::local_runtime::LocalControlPlane>,
    publisher: &LocalRuntimePublisher,
) -> Result<()> {
    let config = Config::load().await.context("load production config")?;
    if config.personality_agent_id != *context.authority.personality_agent_id() {
        bail!("config PAID does not match the supervisor runtime allocation");
    }
    if config.database_path != context.state_dir.join("agent.db") {
        bail!("config state directory does not match SUMI_STATE_DIR");
    }
    let model_spec = config.model_spec().context("resolve production provider")?;
    validate_production_provider_endpoint(&model_spec.base_url)?;
    validate_provider_credential(&model_spec.api_key_env)?;

    let key_provider = Arc::new(EnvironmentKeyProvider::from_env(
        "SUMI_AGENT_WRAPPING_KEY",
        &context.wrapping_key_id,
    )?);
    let store = Store::open(
        &config.database_path,
        AgentScope::new(context.authority.personality_agent_id().clone()),
        key_provider,
    )
    .await
    .context("open authenticated Store")?;
    let hydrated = hydrate_to_fixed_point(
        &store,
        &context.authority,
        &LogicalRecoveryExecutorUnavailable,
    )
    .await?;

    let approval = Arc::new(ApprovalBroker::new(
        Policy::new(&config.workspace),
        SecretAwareActionProjector::new(
            Redactor::v1(),
            SecretDigestKey::new(context.approval_secret_digest_key),
        ),
        None,
        crate::approval::ReviewerMode::User,
        false,
        TrustedEnvironment {
            workspace_root: config.workspace.to_string_lossy().into_owned(),
            sandbox: SandboxSummary::workspace(),
            denied_paths: Vec::new(),
            denied_network_domains: Vec::new(),
            repo_visibility: None,
            git_status: None,
        },
    ));

    let executor_client = Arc::new(ExecutorClient::new(
        &context.executor_socket,
        context.authority.rpc_identity().clone(),
    ));
    wait_for_authenticated_executor_ready(&executor_client).await?;
    let artifact_broker = artifact_broker_client(context);
    let registry = remote_executor_registry(executor_client.clone())
        .context("build exact remote executor registry")?;
    let prompt = PromptContext {
        system_prompt: config.system_prompt.clone(),
        memory_blocks: Vec::new(),
        messages: Vec::new(),
        provider_context: Vec::new(),
        tools: registry.definitions(),
        replay_provenance: None,
    };
    let driver = InjectedRunDriver::new(
        model_spec,
        RequestOptions::default(),
        Some(prompt),
        Some(registry),
        Some(WorkspacePaths::new(config.workspace.clone())?),
        Some(context.authority.generation()),
    )
    .context("compose real provider RunDriver")?
    .with_broker(artifact_broker)
    .with_hydrated_memory(
        Arc::new(store.clone()),
        context.authority.lease(),
        context.authority.fence(),
        &hydrated,
    )
    .context("install authenticated memory/provider context")?;
    let worker: Arc<dyn RunWorker> = Arc::new(SequentialRunWorker::new(Arc::new(driver)));
    let dependency_monitor =
        authenticated_dependency_monitor(executor_client, artifact_broker_client(context))?;

    let command_digest_factory = store.command_digest_factory().await?;
    let connector = if context.allow_insecure_loopback_gateway {
        tracing::warn!(
            mode = "production-like-local-loopback",
            "plaintext WebSocket gateway mode is explicitly enabled"
        );
        WebSocketConnector::new_loopback_insecure(&context.gateway_url, command_digest_factory)?
    } else {
        let connector = WebSocketConnector::new(&context.gateway_url, command_digest_factory);
        connector.validate_configuration()?;
        connector
    };

    let (hydration_tx, hydration_rx) = watch::channel(None);
    let (ready_controller, ready_latch) =
        first_browser_vertical_ready_gate(context.authority.clone(), hydration_rx);
    let store_adapter = T17StoreAdapter::new(Arc::new(store.clone()));
    let credentials = LocalCredentialProvider::new(
        context.authority.clone(),
        DeliveryAuthorization::Raw,
        control,
    );
    let supervisor = ConnectionSupervisor::new(
        connector,
        credentials,
        store_adapter,
        ready_latch.clone(),
        supervisor_config(&context.authority),
    );

    for component in FIRST_BROWSER_VERTICAL_COMPONENTS {
        if component != LocalRuntimeComponent::Session {
            ready_controller.mark_ready(context.authority.rpc_identity(), component)?;
        }
    }
    let supervisor_handle = supervisor.start();
    let mut supervisor_online = supervisor_handle.online.clone();
    let gateway = SessionGateway::from(supervisor_handle);
    let (core, start_authority) =
        SessionStartAuthority::from_hydrated(context.authority.clone(), &hydrated, approval)
            .context("bind required ApprovalBroker before Session")?;
    let session = Session::start_hydrated(store, gateway, core, worker, start_authority)
        .await
        .context("start exact hydrated Session")?;
    ready_controller.mark_ready(
        context.authority.rpc_identity(),
        LocalRuntimeComponent::Session,
    )?;
    hydration_tx.send_replace(Some(hydrated.receipt.clone()));
    let ready_proof = ready_latch
        .wait_for_proof(context.authority.generation())
        .await
        .context("wait for exact local runtime Ready proof")?;
    wait_for_supervisor_online(&mut supervisor_online)
        .await
        .context("wait for authenticated Gateway catch-up")?;
    publish_ready_reconciling(|| publisher.publish_ready(&ready_proof))
        .await
        .context("publish exact Ready")?;

    supervise_ready_session(
        publisher,
        session,
        shutdown_signal(),
        dependency_monitor.failure(),
    )
    .await
}

async fn publish_ready_reconciling<'a, Publish, Published>(mut publish: Publish) -> Result<()>
where
    Publish: FnMut() -> Published,
    Published: std::future::Future<Output = Result<()>> + 'a,
{
    loop {
        match publish().await {
            Ok(()) => return Ok(()),
            Err(error) => {
                tracing::warn!(
                    %error,
                    "Ready publication response was indeterminate; reconciling the same publication identity"
                );
                // LocalControlReadyPublisher retains its pending publication,
                // including publication_id and expected_revision, until an
                // exact ACK is validated. Never construct a replacement here.
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn publish_shutdown_not_ready_reconciling<P>(publisher: &P) -> Result<()>
where
    P: LocalReadyPublisher,
{
    loop {
        match publisher.publish_shutdown_not_ready().await {
            Ok(()) => return Ok(()),
            Err(error) => {
                tracing::warn!(
                    %error,
                    "shutdown NotReady response was indeterminate; reconciling before stopping Session"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn cancel_session_after_not_ready<'a, Publish, Published, SessionRun>(
    mut publish: Publish,
    shutdown: &CancellationToken,
    session_run: SessionRun,
) -> Result<SessionResult>
where
    Publish: FnMut() -> Published,
    Published: std::future::Future<Output = Result<()>> + 'a,
    SessionRun: std::future::Future<Output = SessionResult>,
{
    loop {
        match publish().await {
            Ok(()) => break,
            Err(error) => {
                tracing::warn!(
                    %error,
                    "shutdown NotReady response was indeterminate; reconciling before stopping Session"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    shutdown.cancel();
    Ok(session_run.await)
}

async fn supervise_ready_session<Signal, Dependency>(
    publisher: &LocalRuntimePublisher,
    session: Session<SessionGateway>,
    signal: Signal,
    dependency_failure: Dependency,
) -> Result<()>
where
    Signal: std::future::Future<Output = Result<()>>,
    Dependency: std::future::Future<Output = Result<()>>,
{
    let shutdown = CancellationToken::new();
    let session_run = session.run_until_cancelled(shutdown.clone());
    tokio::pin!(session_run);
    tokio::pin!(signal);
    tokio::pin!(dependency_failure);

    enum Exit {
        Session(SessionResult),
        Signal(Result<()>),
        Dependency(Result<()>),
    }

    let exit = tokio::select! {
        result = &mut session_run => Exit::Session(result),
        result = &mut signal => Exit::Signal(result),
        result = &mut dependency_failure => Exit::Dependency(result),
    };
    match exit {
        Exit::Session(result) => {
            publish_shutdown_not_ready_reconciling(publisher)
                .await
                .context("publish NotReady after Session termination")?;
            session_result(result)
        }
        Exit::Signal(result) => {
            result.context("wait for shutdown signal")?;
            // Preserve the serving runtime until the registry has durably
            // stopped routing new commands to this generation.
            let result = cancel_session_after_not_ready(
                || publisher.publish_shutdown_not_ready(),
                &shutdown,
                &mut session_run,
            )
            .await
            .context("publish signal shutdown NotReady")?;
            session_result(result)
        }
        Exit::Dependency(result) => {
            let failure = match result {
                Ok(()) => anyhow!("authenticated runtime dependency became unavailable"),
                Err(error) => error.context("authenticated runtime dependency monitor failed"),
            };
            let _ = cancel_session_after_not_ready(
                || publisher.publish_shutdown_not_ready(),
                &shutdown,
                &mut session_run,
            )
            .await
            .context("publish dependency-failure NotReady")?;
            Err(failure)
        }
    }
}

fn session_result(result: SessionResult) -> Result<()> {
    match result {
        SessionResult::Completed(_) => Ok(()),
        SessionResult::Failed { failure, .. } => Err(anyhow!(failure)),
    }
}

async fn wait_for_supervisor_online(online: &mut watch::Receiver<bool>) -> Result<()> {
    loop {
        if *online.borrow() {
            return Ok(());
        }
        online
            .changed()
            .await
            .context("Gateway supervisor ended before reaching Online")?;
    }
}

fn validate_provider_credential(name: &str) -> Result<()> {
    let value =
        Zeroizing::new(env::var(name).with_context(|| {
            format!("provider credential environment variable {name} is required")
        })?);
    if value.is_empty() {
        bail!("provider credential environment variable {name} is empty");
    }
    reqwest::header::HeaderValue::from_bytes(value.as_bytes())
        .with_context(|| format!("provider credential environment variable {name} is invalid"))?;
    Ok(())
}

fn validate_production_provider_endpoint(value: &str) -> Result<()> {
    let url = reqwest::Url::parse(value).context("invalid production provider base URL")?;
    if url.scheme() == "https" {
        return Ok(());
    }
    if url.scheme() != "http" {
        bail!("production provider base URL must use https or explicit loopback http");
    }
    let host = url
        .host_str()
        .and_then(|host| {
            host.strip_prefix('[')
                .and_then(|host| host.strip_suffix(']'))
                .unwrap_or(host)
                .parse::<IpAddr>()
                .ok()
        })
        .context("http provider host must be a literal loopback IP address")?;
    if !host.is_loopback() {
        bail!("http provider host must be loopback");
    }
    Ok(())
}

fn supervisor_config(authority: &RuntimeEpochAuthority) -> SupervisorConfig {
    SupervisorConfig {
        personality_agent_id: authority.personality_agent_id().clone(),
        generation: authority.generation(),
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(5),
        send_timeout: Duration::from_secs(10),
        event_buffer_size: NonZeroUsize::new(256).expect("nonzero"),
        command_buffer_size: NonZeroUsize::new(32).expect("nonzero"),
        catch_up_page_size: NonZeroUsize::new(128).expect("nonzero"),
        max_reconnect_attempts: None,
        max_auth_attempts: Some(3),
        hello_timeout: Duration::from_secs(10),
        connect_timeout: Duration::from_secs(10),
    }
}

#[derive(Debug, thiserror::Error)]
#[error(
    "authenticated executor readiness is unavailable; an exact RpcIdentity Health/Hello round-trip is required before Ready"
)]
struct AuthenticatedExecutorReadinessUnavailable;

/// Single replacement point for the executor-owned Health/Hello API.
///
/// Constructing a typed client proves only local composition. A socket inode
/// check or bare connect/accept cannot authenticate the listener, so the
/// interim production bootstrap remains NotReady.
async fn wait_for_authenticated_executor_ready(_client: &ExecutorClient) -> Result<()> {
    Err(anyhow::Error::new(
        AuthenticatedExecutorReadinessUnavailable,
    ))
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {}
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    const PAID: &str = "0198f0f4-9b72-7000-8000-000000000001";

    fn valid_env() -> HashMap<String, OsString> {
        HashMap::from([
            ("SUMI_PERSONALITY_AGENT_ID".to_owned(), PAID.into()),
            ("SUMI_RPC_GENERATION".to_owned(), "7".into()),
            ("SUMI_RPC_NONCE".to_owned(), "boot-a".into()),
            (
                "SUMI_PROCESS_GENERATION_LEASE_ID".to_owned(),
                "lease-a".into(),
            ),
            (
                "SUMI_GENERATION_RECOVERY_FENCE_ID".to_owned(),
                "fence-a".into(),
            ),
            ("SUMI_STATE_DIR".to_owned(), "/tmp/sumi-state".into()),
            (
                "SUMI_EXECUTOR_SOCKET".to_owned(),
                "/tmp/sumi-executor.sock".into(),
            ),
            (
                "SUMI_ARTIFACT_BROKER_SOCKET".to_owned(),
                "/tmp/sumi-artifact-broker.sock".into(),
            ),
            (
                "SUMI_GATEWAY_URL".to_owned(),
                "wss://gateway.example.test/agent".into(),
            ),
            (
                "SUMI_LOCAL_CONTROL_UNIX_SOCKET".to_owned(),
                "/run/sumi/local-control/control.sock".into(),
            ),
            ("SUMI_LOCAL_CONTROL_SERVER_UID".to_owned(), "10001".into()),
            ("SUMI_LOCAL_CONTROL_SOCKET_GID".to_owned(), "10002".into()),
            (
                "SUMI_LOCAL_CONTROL_BEARER".to_owned(),
                "local-control-secret".into(),
            ),
            (
                "SUMI_LOCAL_CONTROL_BEARER_EXPIRES_AT_UNIX".to_owned(),
                "1800000000".into(),
            ),
            (
                "SUMI_AGENT_WRAPPING_KEY_ID".to_owned(),
                "wrapping-key-a".into(),
            ),
            (
                "SUMI_APPROVAL_SECRET_DIGEST_KEY".to_owned(),
                "11".repeat(32).into(),
            ),
            ("SUMI_MODEL_PRESET".to_owned(), "kimi-k3".into()),
        ])
    }

    fn parse(map: &HashMap<String, OsString>) -> Result<BootstrapContext> {
        BootstrapContext::from_source(|name| map.get(name).cloned())
    }

    #[test]
    fn production_environment_is_explicit_and_exact() {
        let context = parse(&valid_env()).unwrap();
        assert_eq!(context.authority.personality_agent_id().as_str(), PAID);
        assert_eq!(context.authority.generation().as_u64(), 7);
        assert_eq!(context.authority.nonce().as_str(), "boot-a");
        assert!(!context.allow_insecure_loopback_gateway);
    }

    #[test]
    fn every_authority_field_is_required_before_side_effectful_composition() {
        for name in [
            "SUMI_PERSONALITY_AGENT_ID",
            "SUMI_RPC_GENERATION",
            "SUMI_RPC_NONCE",
            "SUMI_PROCESS_GENERATION_LEASE_ID",
            "SUMI_GENERATION_RECOVERY_FENCE_ID",
            "SUMI_STATE_DIR",
            "SUMI_EXECUTOR_SOCKET",
            "SUMI_ARTIFACT_BROKER_SOCKET",
            "SUMI_GATEWAY_URL",
            "SUMI_LOCAL_CONTROL_UNIX_SOCKET",
            "SUMI_LOCAL_CONTROL_SERVER_UID",
            "SUMI_LOCAL_CONTROL_SOCKET_GID",
            "SUMI_LOCAL_CONTROL_BEARER",
            "SUMI_LOCAL_CONTROL_BEARER_EXPIRES_AT_UNIX",
            "SUMI_AGENT_WRAPPING_KEY_ID",
            "SUMI_APPROVAL_SECRET_DIGEST_KEY",
        ] {
            let mut env = valid_env();
            env.remove(name);
            let error = parse(&env).err().expect("missing value must fail");
            assert!(error.to_string().contains(name), "{name}: {error:#}");
        }
    }

    #[test]
    fn invalid_paid_generation_and_insecure_mode_fail_closed() {
        for (name, value) in [
            ("SUMI_PERSONALITY_AGENT_ID", "agent-local"),
            ("SUMI_RPC_GENERATION", "not-a-generation"),
            ("SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY", "yes"),
        ] {
            let mut env = valid_env();
            env.insert(name.to_owned(), value.into());
            assert!(parse(&env).is_err(), "{name}");
        }
    }

    #[test]
    fn local_control_transport_rejects_both_or_neither() {
        let mut neither = valid_env();
        neither.remove("SUMI_LOCAL_CONTROL_UNIX_SOCKET");
        neither.remove("SUMI_LOCAL_CONTROL_SERVER_UID");
        neither.remove("SUMI_LOCAL_CONTROL_SOCKET_GID");
        let error = parse(&neither).err().expect("missing transport must fail");
        assert!(error.to_string().contains("exactly one"));

        let mut both = valid_env();
        both.insert(
            "SUMI_LOCAL_CONTROL_URL".to_owned(),
            "http://127.0.0.1:4321/".into(),
        );
        let error = parse(&both).err().expect("ambiguous transport must fail");
        assert!(error.to_string().contains("mutually exclusive"));
    }

    #[test]
    fn explicit_loopback_local_control_remains_a_developer_option() {
        let mut env = valid_env();
        env.remove("SUMI_LOCAL_CONTROL_UNIX_SOCKET");
        env.remove("SUMI_LOCAL_CONTROL_SERVER_UID");
        env.remove("SUMI_LOCAL_CONTROL_SOCKET_GID");
        env.insert(
            "SUMI_LOCAL_CONTROL_URL".to_owned(),
            "http://127.0.0.1:4321/".into(),
        );
        let context = parse(&env).unwrap();
        assert!(matches!(
            context.local_control_endpoint,
            LocalControlEndpoint::Loopback(_)
        ));
    }

    #[test]
    fn unix_local_control_requires_decimal_expected_server_identity() {
        let mut missing = valid_env();
        missing.remove("SUMI_LOCAL_CONTROL_SERVER_UID");
        let error = parse(&missing).err().expect("missing server UID must fail");
        assert!(error.to_string().contains("SUMI_LOCAL_CONTROL_SERVER_UID"));

        let mut malformed = valid_env();
        malformed.insert(
            "SUMI_LOCAL_CONTROL_SERVER_UID".to_owned(),
            "not-a-uid".into(),
        );
        let error = parse(&malformed)
            .err()
            .expect("malformed server UID must fail");
        assert!(error.to_string().contains("decimal UID"));

        let mut missing = valid_env();
        missing.remove("SUMI_LOCAL_CONTROL_SOCKET_GID");
        let error = parse(&missing).err().expect("missing socket GID must fail");
        assert!(error.to_string().contains("SUMI_LOCAL_CONTROL_SOCKET_GID"));

        let mut malformed = valid_env();
        malformed.insert(
            "SUMI_LOCAL_CONTROL_SOCKET_GID".to_owned(),
            "not-a-gid".into(),
        );
        let error = parse(&malformed)
            .err()
            .expect("malformed socket GID must fail");
        assert!(error.to_string().contains("decimal GID"));
    }

    #[test]
    fn local_provider_bridge_requires_literal_loopback_http() {
        for url in [
            "http://127.0.0.1:4321/v1",
            "http://[::1]:4321/v1",
            "https://provider.example.test/v1",
        ] {
            assert!(validate_production_provider_endpoint(url).is_ok(), "{url}");
        }
        for url in ["http://localhost:4321/v1", "http://192.168.1.20:4321/v1"] {
            assert!(validate_production_provider_endpoint(url).is_err(), "{url}");
        }
    }

    #[tokio::test]
    async fn typed_client_alone_cannot_satisfy_executor_ready() {
        let identity = crate::runtime::contracts::RpcIdentity::from_wire(PAID, 7, "boot-a")
            .expect("fixture identity");
        let client = ExecutorClient::new("/tmp/untrusted-executor.sock", identity);
        let error = wait_for_authenticated_executor_ready(&client)
            .await
            .expect_err("an unprobed client must remain NotReady");
        assert!(
            error
                .downcast_ref::<AuthenticatedExecutorReadinessUnavailable>()
                .is_some()
        );
        assert!(error.to_string().contains("exact RpcIdentity Health/Hello"));
    }

    #[tokio::test]
    async fn logical_received_recovery_is_typed_and_fails_closed_without_store_executor() {
        let context = parse(&valid_env()).unwrap();
        let store = Store::session_test_store("bootstrap-received-recovery")
            .await
            .expect("test Store");
        let steps = vec![RecoveryStep::Reclassify {
            command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
        }];
        let error = LogicalRecoveryExecutorUnavailable
            .execute(&store, &steps, &context.authority)
            .await
            .expect_err("bootstrap must not invent Received recovery projections");
        assert!(error.to_string().contains("LogicalRecoveryExecutor"));
        assert!(error.to_string().contains("1 ordered step"));
    }

    #[tokio::test]
    async fn pending_approval_recovery_is_typed_and_fails_closed_without_store_executor() {
        let context = parse(&valid_env()).unwrap();
        let store = Store::session_test_store("bootstrap-pending-approval-recovery")
            .await
            .expect("test Store");
        let steps = vec![RecoveryStep::CancelPendingApproval {
            command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            run_id: "run-a".to_owned(),
            turn_id: "turn-a".to_owned(),
            request_id: "request-a".to_owned(),
            tool_call_id: "tool-a".to_owned(),
        }];
        let error = LogicalRecoveryExecutorUnavailable
            .execute(&store, &steps, &context.authority)
            .await
            .expect_err("bootstrap must not invent approval cancellation events");
        assert!(error.to_string().contains("LogicalRecoveryExecutor"));
        assert!(error.to_string().contains("1 ordered step"));
    }

    #[test]
    fn oversized_input_path_composes_the_exact_artifact_broker_epoch() {
        let context = parse(&valid_env()).unwrap();
        let broker = artifact_broker_client(&context);
        let oversized_input = vec![b'x'; 50 * 1024 + 1];
        assert!(oversized_input.len() > 50 * 1024);
        assert_eq!(broker.socket(), Path::new("/tmp/sumi-artifact-broker.sock"));
        assert_eq!(broker.identity(), context.authority.rpc_identity());
    }

    #[tokio::test]
    async fn lost_ready_ack_retries_the_same_publisher_operation() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let attempts = AtomicUsize::new(0);
        publish_ready_reconciling(|| async {
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                bail!("simulated lost Ready ACK");
            }
            Ok(())
        })
        .await
        .expect("second attempt reconciles the retained publication");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn shutdown_publishes_not_ready_before_cancelling_and_joining_session() {
        use std::sync::Mutex;

        let order = Arc::new(Mutex::new(Vec::new()));
        let shutdown = CancellationToken::new();
        let session = {
            let order = order.clone();
            let shutdown = shutdown.clone();
            async move {
                shutdown.cancelled().await;
                order.lock().unwrap().push("session_cancelled");
                SessionResult::Completed(crate::agent::RunCore::new())
            }
        };
        let result = cancel_session_after_not_ready(
            {
                let order = order.clone();
                move || {
                    let order = order.clone();
                    async move {
                        order.lock().unwrap().push("not_ready");
                        Ok(())
                    }
                }
            },
            &shutdown,
            session,
        )
        .await
        .expect("ordered shutdown");
        assert!(matches!(result, SessionResult::Completed(_)));
        assert_eq!(
            order.lock().unwrap().as_slice(),
            ["not_ready", "session_cancelled"]
        );
    }

    #[test]
    fn dependency_monitor_is_fail_closed_until_authenticated_health_exists() {
        let context = parse(&valid_env()).unwrap();
        let executor = Arc::new(ExecutorClient::new(
            &context.executor_socket,
            context.authority.rpc_identity().clone(),
        ));
        let error = authenticated_dependency_monitor(executor, artifact_broker_client(&context))
            .err()
            .expect("missing Health API must prevent Ready");
        assert!(
            error
                .downcast_ref::<AuthenticatedDependencyMonitoringUnavailable>()
                .is_some()
        );
    }
}
