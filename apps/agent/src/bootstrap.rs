//! T26 production composition root for one PersonalityAgent runtime.
//!
//! Every authority-bearing dependency is constructed from one exact
//! supervisor allocation.  The normal process path has no stdio, synthetic
//! provider, local workspace tool, or fresh-only memory fallback.

use std::{
    env,
    ffi::OsString,
    fs::OpenOptions,
    net::IpAddr,
    num::NonZeroUsize,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
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

const CONTROL_PLANE_RECONCILIATION_ATTEMPTS: usize = 4;
const CONTROL_PLANE_RECONCILIATION_DELAY: Duration = Duration::from_millis(100);

#[derive(Debug, thiserror::Error)]
#[error(
    "{transition} control-plane state is indeterminate after {attempts} exact reconciliation attempts"
)]
struct IndeterminateControlPlaneState {
    transition: &'static str,
    attempts: usize,
    #[source]
    source: anyhow::Error,
}

#[derive(Debug)]
struct ReadyPublicationOutcome {
    shutdown_requested: bool,
    signal_failure: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SessionOwnershipReport {
    Recovered,
    Lost,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SessionTerminationReport {
    status: &'static str,
    ownership: SessionOwnershipReport,
    failure: Option<String>,
}

impl std::fmt::Display for SessionTerminationReport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Session status={}, ownership={:?}",
            self.status, self.ownership
        )?;
        if let Some(failure) = self.failure.as_deref() {
            write!(formatter, ", failure={failure}")?;
        }
        Ok(())
    }
}

impl SessionTerminationReport {
    fn from_result(result: SessionResult) -> Self {
        use crate::agent::RunOwnership;

        match result {
            SessionResult::Completed(_) => Self {
                status: "completed",
                ownership: SessionOwnershipReport::Recovered,
                failure: None,
            },
            SessionResult::Failed { failure, ownership } => Self {
                status: "failed",
                ownership: match ownership {
                    RunOwnership::Recovered(_) => SessionOwnershipReport::Recovered,
                    RunOwnership::Lost => SessionOwnershipReport::Lost,
                },
                failure: Some(failure.to_string()),
            },
        }
    }

    fn into_result(self) -> Result<()> {
        match self.failure {
            None => Ok(()),
            Some(_) => Err(anyhow!(self)),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error(
    "authenticated dependency failed: {dependency}; shutdown control-plane result: {control_plane}; {session}"
)]
struct DependencyTeardownFailure {
    dependency: String,
    control_plane: String,
    session: SessionTerminationReport,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "indeterminate control-plane state escalated after local generation fencing: {control}; {session}"
)]
struct IndeterminateControlPlaneStateEscalation {
    #[source]
    control: anyhow::Error,
    session: SessionTerminationReport,
}

/// Validate a UDS parent with the same trust properties as executor-owned IPC:
/// normalized absolute path, no symlinked directory component, current-uid
/// ownership, owner write/execute, and no group/other write access.
fn validate_trusted_ipc_socket_parent(path: &Path, label: &'static str) -> Result<()> {
    use std::path::Component;

    let parent = path
        .parent()
        .with_context(|| format!("{label} socket path has no parent"))?;
    if !parent.is_absolute() {
        bail!("{label} socket parent must be absolute");
    }
    let mut current = PathBuf::from("/");
    for component in parent.components() {
        match component {
            Component::RootDir => continue,
            Component::Normal(component) => current.push(component),
            Component::CurDir => continue,
            Component::ParentDir | Component::Prefix(_) => {
                bail!("{label} socket parent must be a normalized absolute Unix path")
            }
        }
        let metadata = std::fs::symlink_metadata(&current)
            .with_context(|| format!("inspect {label} socket parent {}", current.display()))?;
        if metadata.file_type().is_symlink() {
            bail!(
                "{label} socket parent contains a symlink component: {}",
                current.display()
            );
        }
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(parent)
        .with_context(|| format!("open trusted {label} socket parent {}", parent.display()))?;
    let descriptor = file
        .metadata()
        .with_context(|| format!("stat trusted {label} socket parent {}", parent.display()))?;
    let pathname = std::fs::symlink_metadata(parent)
        .with_context(|| format!("restat trusted {label} socket parent {}", parent.display()))?;
    if !descriptor.file_type().is_dir()
        || descriptor.uid() != unsafe { libc::geteuid() }
        || descriptor.nlink() == 0
        || descriptor.mode() & 0o300 != 0o300
        || descriptor.mode() & 0o022 != 0
        || pathname.file_type().is_symlink()
        || !pathname.file_type().is_dir()
        || pathname.dev() != descriptor.dev()
        || pathname.ino() != descriptor.ino()
    {
        bail!("{label} socket parent is not a stable uid-owned non-peer-writable directory");
    }
    Ok(())
}

fn fence_generation_locally(
    authority: &RuntimeEpochAuthority,
    shutdown: &CancellationToken,
    reason: &'static str,
) {
    tracing::error!(
        personality_agent_id = %authority.personality_agent_id(),
        generation = authority.generation().as_u64(),
        lease_id = authority.lease().lease_id(),
        fence_id = authority.fence().fence_id(),
        reason,
        "locally fencing the authenticated runtime generation"
    );
    shutdown.cancel();
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
    validate_trusted_ipc_socket_parent(&context.executor_socket, "executor")?;
    validate_trusted_ipc_socket_parent(&context.artifact_broker_socket, "artifact broker")?;
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
    wait_for_authenticated_executor_ready(
        &executor_client,
        &context.authority,
        &ExecutorHealthApiUnavailable,
    )
    .await?;
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
    // This is the sole Store/post-commit dispatcher seam. Bootstrap does not
    // synthesize an in-process fallback while the coordinated dispatcher fix
    // is integrated; the supervisor must receive the real adapter.
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
    // Unix handlers are synchronously installed here, and the future is
    // polled inside the very first Ready attempt. A signal cannot slip through
    // the Ready transition before bootstrap owns the shutdown path.
    let mut signal = install_shutdown_signal().context("install shutdown signal handlers")?;
    let shutdown = CancellationToken::new();
    let ready_outcome = match publish_ready_reconciling(
        || publisher.publish_ready(&ready_proof),
        &mut signal,
        &shutdown,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(control) => {
            fence_generation_locally(
                &context.authority,
                &shutdown,
                "Ready could not be reconciled",
            );
            let report =
                SessionTerminationReport::from_result(session.run_until_cancelled(shutdown).await);
            return Err(anyhow::Error::new(
                IndeterminateControlPlaneStateEscalation {
                    control,
                    session: report,
                },
            ))
            .context("publish exact Ready");
        }
    };

    if ready_outcome.shutdown_requested {
        // Even when the first Ready response was lost after commit, the
        // publisher has now reconciled that exact pending publication. Only
        // then is the subsequent shutdown NotReady transition legal.
        let control_result = publish_shutdown_not_ready_reconciling(publisher).await;
        if control_result.is_err() {
            fence_generation_locally(
                &context.authority,
                &shutdown,
                "shutdown NotReady could not be reconciled",
            );
        } else {
            shutdown.cancel();
        }
        let report =
            SessionTerminationReport::from_result(session.run_until_cancelled(shutdown).await);
        if let Err(control) = control_result {
            return Err(anyhow::Error::new(
                IndeterminateControlPlaneStateEscalation {
                    control,
                    session: report,
                },
            ));
        }
        if let Some(signal_failure) = ready_outcome.signal_failure {
            return Err(anyhow!(
                "shutdown signal monitor failed during Ready reconciliation: {signal_failure}; {report}"
            ));
        }
        return report.into_result();
    }

    supervise_ready_session(
        &context.authority,
        publisher,
        session,
        signal,
        dependency_monitor.failure(),
        shutdown,
    )
    .await
}

async fn publish_ready_reconciling<'a, Publish, Published>(
    mut publish: Publish,
    signal: &mut BoxFuture<'static, Result<()>>,
    shutdown: &CancellationToken,
) -> Result<ReadyPublicationOutcome>
where
    Publish: FnMut() -> Published,
    Published: std::future::Future<Output = Result<()>> + 'a,
{
    let mut shutdown_requested = false;
    let mut signal_failure = None;
    let mut last_error = None;
    for attempt in 1..=CONTROL_PLANE_RECONCILIATION_ATTEMPTS {
        let publication = if shutdown_requested {
            publish().await
        } else {
            tokio::select! {
                published = publish() => published,
                observed = signal.as_mut() => {
                    shutdown_requested = true;
                    shutdown.cancel();
                    if let Err(error) = observed {
                        signal_failure = Some(error.to_string());
                    }
                    Err(anyhow!(
                        "shutdown was observed while Ready publication was in flight"
                    ))
                }
            }
        };
        match publication {
            Ok(()) => {
                return Ok(ReadyPublicationOutcome {
                    shutdown_requested,
                    signal_failure,
                });
            }
            Err(error) => {
                tracing::warn!(
                    attempt,
                    max_attempts = CONTROL_PLANE_RECONCILIATION_ATTEMPTS,
                    %error,
                    "Ready publication response was indeterminate; reconciling the same publication identity"
                );
                // LocalControlReadyPublisher retains its pending publication,
                // including publication_id and expected_revision, until an
                // exact ACK is validated. Never construct a replacement here.
                last_error = Some(error);
            }
        }
        if attempt < CONTROL_PLANE_RECONCILIATION_ATTEMPTS {
            if shutdown_requested {
                tokio::time::sleep(CONTROL_PLANE_RECONCILIATION_DELAY).await;
            } else {
                tokio::select! {
                    _ = tokio::time::sleep(CONTROL_PLANE_RECONCILIATION_DELAY) => {}
                    observed = signal.as_mut() => {
                        shutdown_requested = true;
                        shutdown.cancel();
                        if let Err(error) = observed {
                            signal_failure = Some(error.to_string());
                        }
                    }
                }
            }
        }
    }
    Err(anyhow::Error::new(IndeterminateControlPlaneState {
        transition: "Ready",
        attempts: CONTROL_PLANE_RECONCILIATION_ATTEMPTS,
        source: last_error.unwrap_or_else(|| anyhow!("Ready publication produced no result")),
    }))
}

async fn publish_shutdown_not_ready_reconciling<P>(publisher: &P) -> Result<()>
where
    P: LocalReadyPublisher,
{
    let mut last_error = None;
    for attempt in 1..=CONTROL_PLANE_RECONCILIATION_ATTEMPTS {
        match publisher.publish_shutdown_not_ready().await {
            Ok(()) => return Ok(()),
            Err(error) => {
                tracing::warn!(
                    attempt,
                    max_attempts = CONTROL_PLANE_RECONCILIATION_ATTEMPTS,
                    %error,
                    "shutdown NotReady response was indeterminate; bounded reconciliation remains"
                );
                last_error = Some(error);
            }
        }
        if attempt < CONTROL_PLANE_RECONCILIATION_ATTEMPTS {
            tokio::time::sleep(CONTROL_PLANE_RECONCILIATION_DELAY).await;
        }
    }
    Err(anyhow::Error::new(IndeterminateControlPlaneState {
        transition: "shutdown NotReady",
        attempts: CONTROL_PLANE_RECONCILIATION_ATTEMPTS,
        source: last_error
            .unwrap_or_else(|| anyhow!("shutdown NotReady publication produced no result")),
    }))
}

async fn supervise_ready_session<Signal, Dependency>(
    authority: &RuntimeEpochAuthority,
    publisher: &LocalRuntimePublisher,
    session: Session<SessionGateway>,
    signal: Signal,
    dependency_failure: Dependency,
    shutdown: CancellationToken,
) -> Result<()>
where
    Signal: std::future::Future<Output = Result<()>>,
    Dependency: std::future::Future<Output = Result<()>>,
{
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
            let control_result = publish_shutdown_not_ready_reconciling(publisher).await;
            let report = SessionTerminationReport::from_result(result);
            if let Err(control) = control_result {
                fence_generation_locally(
                    authority,
                    &shutdown,
                    "post-Session NotReady could not be reconciled",
                );
                return Err(anyhow::Error::new(
                    IndeterminateControlPlaneStateEscalation {
                        control,
                        session: report,
                    },
                ));
            }
            report.into_result()
        }
        Exit::Signal(result) => {
            // Preserve the serving runtime until the registry has durably
            // stopped routing new commands to this generation. If the bounded
            // transition is indeterminate, fence locally before joining.
            let control_result = publish_shutdown_not_ready_reconciling(publisher).await;
            if control_result.is_err() {
                fence_generation_locally(
                    authority,
                    &shutdown,
                    "signal shutdown NotReady could not be reconciled",
                );
            } else {
                shutdown.cancel();
            }
            let report = SessionTerminationReport::from_result((&mut session_run).await);
            if let Err(control) = control_result {
                return Err(anyhow::Error::new(
                    IndeterminateControlPlaneStateEscalation {
                        control,
                        session: report,
                    },
                ));
            }
            result.context("wait for shutdown signal")?;
            report.into_result()
        }
        Exit::Dependency(result) => {
            let failure = match result {
                Ok(()) => anyhow!("authenticated runtime dependency became unavailable"),
                Err(error) => error.context("authenticated runtime dependency monitor failed"),
            };
            let control_result = publish_shutdown_not_ready_reconciling(publisher).await;
            if control_result.is_err() {
                fence_generation_locally(
                    authority,
                    &shutdown,
                    "dependency-failure NotReady could not be reconciled",
                );
            } else {
                shutdown.cancel();
            }
            let report = SessionTerminationReport::from_result((&mut session_run).await);
            Err(anyhow::Error::new(DependencyTeardownFailure {
                dependency: failure.to_string(),
                control_plane: control_result.err().map_or_else(
                    || "acknowledged NotReady".to_owned(),
                    |error| error.to_string(),
                ),
                session: report,
            }))
        }
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

#[async_trait]
trait AuthenticatedExecutorHealthApi: Send + Sync {
    async fn ready(&self, client: &ExecutorClient, authority: &RuntimeEpochAuthority)
    -> Result<()>;
}

struct ExecutorHealthApiUnavailable;

#[async_trait]
impl AuthenticatedExecutorHealthApi for ExecutorHealthApiUnavailable {
    async fn ready(
        &self,
        _client: &ExecutorClient,
        _authority: &RuntimeEpochAuthority,
    ) -> Result<()> {
        Err(anyhow::Error::new(
            AuthenticatedExecutorReadinessUnavailable,
        ))
    }
}

/// Narrow adapter point for the executor-owned Health API. The coordinated
/// executor commit implements this without changing bootstrap ownership.
/// Constructing a typed client or probing an inode is intentionally
/// insufficient in the standalone branch.
async fn wait_for_authenticated_executor_ready(
    client: &ExecutorClient,
    authority: &RuntimeEpochAuthority,
    health: &dyn AuthenticatedExecutorHealthApi,
) -> Result<()> {
    authority
        .validate_rpc_identity(client.identity())
        .context("executor Health client identity differs from runtime authority")?;
    health.ready(client, authority).await
}

fn install_shutdown_signal() -> Result<BoxFuture<'static, Result<()>>> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        Ok(Box::pin(async move {
            tokio::select! {
                _ = interrupt.recv() => {}
                _ = terminate.recv() => {}
            }
            Ok(())
        }))
    }
    #[cfg(not(unix))]
    {
        Ok(Box::pin(async move {
            tokio::signal::ctrl_c().await?;
            Ok(())
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    struct UnavailableShutdownPublisher {
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl LocalReadyPublisher for UnavailableShutdownPublisher {
        async fn publish_not_ready(&self) -> Result<()> {
            unreachable!("test exercises only shutdown NotReady")
        }

        async fn publish_ready(
            &self,
            _proof: &crate::gateway::local_runtime::LocalReadyProof,
        ) -> Result<()> {
            unreachable!("test exercises only shutdown NotReady")
        }

        async fn publish_shutdown_not_ready(&self) -> Result<()> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            bail!("local control unavailable")
        }
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

    #[test]
    fn broker_socket_parent_uses_trusted_ipc_contract() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!(
            "sumi-bootstrap-ipc-parent-{}",
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir(&root).expect("create isolated socket parent");
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .expect("restrict isolated socket parent");
        let socket = root.join("broker.sock");
        validate_trusted_ipc_socket_parent(&socket, "artifact broker")
            .expect("uid-owned non-peer-writable parent");

        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o770))
            .expect("make fixture peer-writable");
        assert!(
            validate_trusted_ipc_socket_parent(&socket, "artifact broker")
                .unwrap_err()
                .to_string()
                .contains("non-peer-writable")
        );
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .expect("restore fixture permissions");
        std::fs::remove_dir(&root).expect("remove isolated socket parent");
    }

    #[tokio::test]
    async fn typed_client_alone_cannot_satisfy_executor_ready() {
        let identity = crate::runtime::contracts::RpcIdentity::from_wire(PAID, 7, "boot-a")
            .expect("fixture identity");
        let client = ExecutorClient::new("/tmp/untrusted-executor.sock", identity);
        let context = parse(&valid_env()).unwrap();
        let error = wait_for_authenticated_executor_ready(
            &client,
            &context.authority,
            &ExecutorHealthApiUnavailable,
        )
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

    #[tokio::test]
    async fn signal_during_lost_ready_ack_reconciles_before_shutdown() {
        use std::sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        };

        let attempts = Arc::new(AtomicUsize::new(0));
        let order = Arc::new(Mutex::new(Vec::new()));
        let (committed_tx, committed_rx) = tokio::sync::oneshot::channel();
        let committed_tx = Arc::new(Mutex::new(Some(committed_tx)));
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            committed_rx.await.expect("first Ready committed");
            signal_tx.send(()).expect("signal observer remains");
        });
        let mut signal: BoxFuture<'static, Result<()>> =
            Box::pin(async move { signal_rx.await.context("test signal sender dropped") });
        let shutdown = CancellationToken::new();
        let outcome = publish_ready_reconciling(
            {
                let attempts = attempts.clone();
                let order = order.clone();
                move || {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    let order = order.clone();
                    let committed_tx = committed_tx.clone();
                    async move {
                        if attempt == 0 {
                            order.lock().unwrap().push("ready_committed_ack_lost");
                            committed_tx
                                .lock()
                                .unwrap()
                                .take()
                                .expect("one commit signal")
                                .send(())
                                .expect("signal task remains");
                            std::future::pending::<Result<()>>().await
                        } else {
                            order.lock().unwrap().push("ready_reconciled");
                            Ok(())
                        }
                    }
                }
            },
            &mut signal,
            &shutdown,
        )
        .await
        .expect("second attempt reconciles the retained Ready publication");
        assert!(outcome.shutdown_requested);
        assert!(shutdown.is_cancelled());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        order.lock().unwrap().push("shutdown_not_ready");
        assert_eq!(
            order.lock().unwrap().as_slice(),
            [
                "ready_committed_ack_lost",
                "ready_reconciled",
                "shutdown_not_ready"
            ]
        );
    }

    #[tokio::test]
    async fn ready_reconciliation_is_bounded_and_typed() {
        let attempts = AtomicUsize::new(0);
        let mut signal: BoxFuture<'static, Result<()>> =
            Box::pin(std::future::pending::<Result<()>>());
        let shutdown = CancellationToken::new();
        let error = publish_ready_reconciling(
            || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                bail!("local control unavailable")
            },
            &mut signal,
            &shutdown,
        )
        .await
        .expect_err("reconciliation must escalate instead of serving forever");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            CONTROL_PLANE_RECONCILIATION_ATTEMPTS
        );
        assert!(
            error
                .downcast_ref::<IndeterminateControlPlaneState>()
                .is_some()
        );
    }

    #[tokio::test]
    async fn shutdown_not_ready_reconciliation_is_bounded_and_typed() {
        let publisher = UnavailableShutdownPublisher {
            attempts: AtomicUsize::new(0),
        };
        let error = publish_shutdown_not_ready_reconciling(&publisher)
            .await
            .expect_err("permanent local-control loss must escalate");
        assert_eq!(
            publisher.attempts.load(Ordering::SeqCst),
            CONTROL_PLANE_RECONCILIATION_ATTEMPTS
        );
        assert!(
            error
                .downcast_ref::<IndeterminateControlPlaneState>()
                .is_some()
        );
    }

    #[test]
    fn dependency_teardown_error_preserves_trigger_and_ownership_loss() {
        let report = SessionTerminationReport::from_result(SessionResult::Failed {
            failure: crate::agent::SessionFailure::RuntimeShutdownOwnershipLost,
            ownership: crate::agent::RunOwnership::Lost,
        });
        let error = DependencyTeardownFailure {
            dependency: "executor Health failed".to_owned(),
            control_plane: "shutdown NotReady indeterminate".to_owned(),
            session: report,
        };
        let rendered = error.to_string();
        assert!(rendered.contains("executor Health failed"));
        assert!(rendered.contains("ownership=Lost"));
        assert!(rendered.contains("runtime shutdown"));
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
