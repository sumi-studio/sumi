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
            LocalPublicationError, LocalPublicationResult, LocalReadyPublisher,
            LocalRuntimeComponent, first_browser_vertical_ready_gate,
        },
        supervisor::{
            ConnectionSupervisor, DeliveryAuthorization, SupervisorConfig, SupervisorRuntime,
            SupervisorTermination, post_commit::ProductionPostCommitRuntime,
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

#[cfg(test)]
use crate::gateway::local_runtime::LocalPublicationError;

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

enum ReadyReconciliationOutcome<T> {
    Published,
    InterruptedBeforePublication(T),
    InterruptedAfterReconciliation(T),
}

struct ReadyReconciliationFailure<T> {
    control: anyhow::Error,
    interruption: Option<T>,
}

enum PreReadyWait<T> {
    Completed(T),
    Signal(Result<()>),
}

enum PreReadyRuntimeExit {
    Signal(Result<()>),
    Dependency(Result<()>),
    PostCommit(Result<()>),
    Supervisor(Result<SupervisorTermination>),
}

enum OwnedPreReadyWait<T> {
    Completed(T),
    Exit(PreReadyRuntimeExit),
}

async fn wait_pre_ready_or_signal<T>(
    operation: impl std::future::Future<Output = T>,
    signal: &mut BoxFuture<'static, Result<()>>,
) -> PreReadyWait<T> {
    tokio::pin!(operation);
    tokio::select! {
        biased;
        observed = signal.as_mut() => PreReadyWait::Signal(observed),
        completed = &mut operation => PreReadyWait::Completed(completed),
    }
}

async fn wait_owned_pre_ready<T>(
    operation: impl std::future::Future<Output = T>,
    signal: &mut BoxFuture<'static, Result<()>>,
    dependency: &mut BoxFuture<'static, Result<()>>,
    post_commit: &mut BoxFuture<'static, Result<()>>,
    supervisor: &mut SupervisorRuntime,
) -> OwnedPreReadyWait<T> {
    tokio::pin!(operation);
    tokio::select! {
        biased;
        exit = wait_required_runtime_exit(signal, dependency, post_commit, supervisor) => {
            OwnedPreReadyWait::Exit(exit)
        }
        completed = &mut operation => OwnedPreReadyWait::Completed(completed),
    }
}

async fn wait_required_runtime_exit(
    signal: &mut BoxFuture<'static, Result<()>>,
    dependency: &mut BoxFuture<'static, Result<()>>,
    post_commit: &mut BoxFuture<'static, Result<()>>,
    supervisor: &mut SupervisorRuntime,
) -> PreReadyRuntimeExit {
    tokio::select! {
        biased;
        observed = signal.as_mut() => PreReadyRuntimeExit::Signal(observed),
        observed = dependency.as_mut() => PreReadyRuntimeExit::Dependency(observed),
        observed = post_commit.as_mut() => PreReadyRuntimeExit::PostCommit(observed),
        observed = supervisor.termination() => PreReadyRuntimeExit::Supervisor(observed),
    }
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

#[derive(Debug, thiserror::Error)]
#[error("terminal control-plane publication failure: {control}; {session}")]
struct TerminalControlPlaneStateEscalation {
    #[source]
    control: anyhow::Error,
    session: SessionTerminationReport,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "shutdown signal monitor failed: {signal}; shutdown control-plane result: {control_plane}; {session}"
)]
struct SignalTeardownFailure {
    signal: String,
    control_plane: String,
    session: SessionTerminationReport,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "post-commit dispatcher failed: {dispatcher}; shutdown control-plane result: {control_plane}; {session}"
)]
struct PostCommitTeardownFailure {
    dispatcher: String,
    control_plane: String,
    session: SessionTerminationReport,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "connection supervisor terminated: {supervisor}; shutdown control-plane result: {control_plane}; {session}"
)]
struct SupervisorTeardownFailure {
    supervisor: String,
    control_plane: String,
    session: SessionTerminationReport,
}

#[derive(Debug, thiserror::Error)]
#[error("Session terminated without a selected shutdown or required-runtime failure: {session}")]
struct UnexpectedSessionTermination {
    session: SessionTerminationReport,
}

fn unexpected_session_result(report: SessionTerminationReport) -> Result<()> {
    Err(anyhow::Error::new(UnexpectedSessionTermination {
        session: report,
    }))
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

fn control_failure_is_indeterminate(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<IndeterminateControlPlaneState>()
        .is_some()
}

fn stop_for_control_failure(
    authority: &RuntimeEpochAuthority,
    shutdown: &CancellationToken,
    error: &anyhow::Error,
    indeterminate_reason: &'static str,
) {
    if control_failure_is_indeterminate(error) {
        fence_generation_locally(authority, shutdown, indeterminate_reason);
    } else {
        // A terminal preflight/auth/validation/rejection result is known, so
        // stop admission without pretending its outcome needs reconciliation.
        shutdown.cancel();
    }
}

fn control_teardown_error(
    control: anyhow::Error,
    session: SessionTerminationReport,
) -> anyhow::Error {
    if control_failure_is_indeterminate(&control) {
        anyhow::Error::new(IndeterminateControlPlaneStateEscalation { control, session })
    } else {
        anyhow::Error::new(TerminalControlPlaneStateEscalation { control, session })
    }
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
    // Own and poll the process signal before any Session construction,
    // supervisor catch-up, or other pre-Ready await can begin.
    let mut signal = install_shutdown_signal().context("install shutdown signal handlers")?;
    let shutdown = CancellationToken::new();
    let preparation = async {
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
        let store = Arc::new(
            Store::open(
                &config.database_path,
                AgentScope::new(context.authority.personality_agent_id().clone()),
                key_provider,
            )
            .await
            .context("open authenticated Store")?,
        );
        let hydrated = hydrate_to_fixed_point(
            store.as_ref(),
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
            store.clone(),
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
        let credentials = LocalCredentialProvider::new(
            context.authority.clone(),
            DeliveryAuthorization::Raw,
            control.clone(),
        );
        for component in FIRST_BROWSER_VERTICAL_COMPONENTS {
            if component != LocalRuntimeComponent::Session {
                ready_controller.mark_ready(context.authority.rpc_identity(), component)?;
            }
        }
        let (core, start_authority) =
            SessionStartAuthority::from_hydrated(context.authority.clone(), &hydrated, approval)
                .context("bind required ApprovalBroker before Session")?;
        Ok::<_, anyhow::Error>((
            store,
            hydrated,
            worker,
            dependency_monitor,
            connector,
            credentials,
            hydration_tx,
            ready_controller,
            ready_latch,
            core,
            start_authority,
        ))
    };
    let (
        store,
        hydrated,
        worker,
        dependency_monitor,
        connector,
        credentials,
        hydration_tx,
        ready_controller,
        ready_latch,
        core,
        start_authority,
    ) = match wait_pre_ready_or_signal(preparation, &mut signal).await {
        PreReadyWait::Completed(result) => result?,
        PreReadyWait::Signal(result) => {
            let control_result =
                publish_shutdown_not_ready_after_local_cancel(publisher, &shutdown).await;
            return pre_ready_signal_result(result, control_result);
        }
    };

    // This is the only production post-COMMIT composition seam. It mints one
    // Store-local authenticated epoch and starts its receiver before Session
    // can admit any command or create an EventWriter commit.
    let (post_commit, store_adapter) =
        ProductionPostCommitRuntime::start(store.clone(), &context.authority).await?;
    let post_commit_client = post_commit.client();
    let mut post_commit_failure: BoxFuture<'static, Result<()>> = Box::pin({
        let client = post_commit_client.clone();
        async move { client.termination().await }
    });
    let mut dependency_failure = dependency_monitor.failure();
    let supervisor = ConnectionSupervisor::new(
        connector,
        credentials,
        store_adapter,
        ready_latch.clone(),
        supervisor_config(&context.authority),
    );
    let supervisor_handle = supervisor.start();
    let mut supervisor_online = supervisor_handle.online.clone();
    let (gateway, mut supervisor_runtime) = SessionGateway::from_supervisor(supervisor_handle);

    // Session construction owns the sole RunCore in a retained task. A signal
    // can stop admission immediately without cancelling and dropping that
    // in-flight ownership transfer.
    let mut session_start = tokio::spawn(async move {
        Session::start_hydrated(
            store.as_ref().clone(),
            gateway,
            core,
            worker,
            start_authority,
        )
        .await
        .context("start exact hydrated Session")
    });
    let session = tokio::select! {
        biased;
        exit = wait_required_runtime_exit(
            &mut signal,
            &mut dependency_failure,
            &mut post_commit_failure,
            &mut supervisor_runtime,
        ) => {
            let started = (&mut session_start)
                .await
                .context("join exact hydrated Session startup task");
            return match started {
                Ok(Ok(session)) => {
                    teardown_owned_pre_ready_runtime(
                        publisher,
                        session,
                        post_commit,
                        supervisor_runtime,
                        shutdown,
                        exit,
                    )
                    .await
                }
                Ok(Err(start_error)) | Err(start_error) => {
                    teardown_failed_session_start(
                        publisher,
                        post_commit,
                        supervisor_runtime,
                        shutdown,
                        exit,
                        start_error,
                    )
                    .await
                }
            };
        }
        started = &mut session_start => {
            match started.context("join exact hydrated Session startup task") {
                Ok(Ok(session)) => session,
                Ok(Err(start_error)) | Err(start_error) => {
                    return finish_runtime(
                        Err(start_error),
                        post_commit,
                        supervisor_runtime,
                        true,
                    )
                    .await;
                }
            }
        }
    };

    if let Err(error) = ready_controller.mark_ready(
        context.authority.rpc_identity(),
        LocalRuntimeComponent::Session,
    ) {
        return teardown_pre_ready_operation_failure(
            session,
            post_commit,
            supervisor_runtime,
            shutdown,
            error.context("mark exact Session component Ready"),
        )
        .await;
    }
    hydration_tx.send_replace(Some(hydrated.receipt.clone()));
    let ready_proof = match wait_owned_pre_ready(
        ready_latch.wait_for_proof(context.authority.generation()),
        &mut signal,
        &mut dependency_failure,
        &mut post_commit_failure,
        &mut supervisor_runtime,
    )
    .await
    {
        OwnedPreReadyWait::Completed(result) => match result {
            Ok(proof) => proof,
            Err(error) => {
                return teardown_pre_ready_operation_failure(
                    session,
                    post_commit,
                    supervisor_runtime,
                    shutdown,
                    error.context("wait for exact local runtime Ready proof"),
                )
                .await;
            }
        },
        OwnedPreReadyWait::Exit(exit) => {
            return teardown_owned_pre_ready_runtime(
                publisher,
                session,
                post_commit,
                supervisor_runtime,
                shutdown,
                exit,
            )
            .await;
        }
    };
    match wait_owned_pre_ready(
        wait_for_supervisor_online(&mut supervisor_online),
        &mut signal,
        &mut dependency_failure,
        &mut post_commit_failure,
        &mut supervisor_runtime,
    )
    .await
    {
        OwnedPreReadyWait::Completed(Ok(())) => {}
        OwnedPreReadyWait::Completed(Err(error)) => {
            return teardown_pre_ready_operation_failure(
                session,
                post_commit,
                supervisor_runtime,
                shutdown,
                error.context("wait for authenticated Gateway catch-up"),
            )
            .await;
        }
        OwnedPreReadyWait::Exit(exit) => {
            return teardown_owned_pre_ready_runtime(
                publisher,
                session,
                post_commit,
                supervisor_runtime,
                shutdown,
                exit,
            )
            .await;
        }
    }

    let ready_outcome = publish_ready_reconciling(
        || publisher.publish_ready(&ready_proof),
        wait_required_runtime_exit(
            &mut signal,
            &mut dependency_failure,
            &mut post_commit_failure,
            &mut supervisor_runtime,
        ),
        &shutdown,
    )
    .await;
    match ready_outcome {
        Ok(ReadyReconciliationOutcome::Published) => {}
        Ok(
            ReadyReconciliationOutcome::InterruptedBeforePublication(exit)
            | ReadyReconciliationOutcome::InterruptedAfterReconciliation(exit),
        ) => {
            // If Ready might have committed, the helper above reconciled that
            // exact publication identity before returning. Shutdown NotReady
            // is therefore the next legal transition.
            return teardown_owned_pre_ready_runtime(
                publisher,
                session,
                post_commit,
                supervisor_runtime,
                shutdown,
                exit,
            )
            .await;
        }
        Err(failure) => {
            let interruption = failure.interruption;
            let control = failure.control;
            let control_description = control.to_string();
            stop_for_control_failure(
                &context.authority,
                &shutdown,
                &control,
                "Ready could not be reconciled",
            );
            let report =
                SessionTerminationReport::from_result(session.run_until_cancelled(shutdown).await);
            let mut primary =
                Err(control_teardown_error(control, report.clone())).context("publish exact Ready");
            let emergency = if let Some(exit) = interruption {
                let (exit_result, emergency) = runtime_exit_result(
                    exit,
                    Err(anyhow!(
                        "Ready transition failed before shutdown NotReady: {control_description}"
                    )),
                    report,
                );
                primary = combine_results(
                    primary,
                    exit_result,
                    "required runtime termination during Ready reconciliation",
                );
                emergency
            } else {
                false
            };
            return finish_runtime(primary, post_commit, supervisor_runtime, emergency).await;
        }
    }

    supervise_ready_session(
        &context.authority,
        publisher,
        session,
        signal,
        dependency_failure,
        post_commit_failure,
        post_commit,
        supervisor_runtime,
        shutdown,
    )
    .await
}

async fn publish_ready_reconciling<'a, Publish, Published, Interruption>(
    mut publish: Publish,
    interrupt: impl std::future::Future<Output = Interruption>,
    shutdown: &CancellationToken,
) -> std::result::Result<
    ReadyReconciliationOutcome<Interruption>,
    ReadyReconciliationFailure<Interruption>,
>
where
    Publish: FnMut() -> Published,
    Published: std::future::Future<Output = LocalPublicationResult<()>> + 'a,
{
    tokio::pin!(interrupt);
    // A required component may already have failed after the final pre-Ready
    // check. Poll that state before constructing or polling a new Ready
    // publication so bootstrap never initiates a known-stale transition.
    tokio::select! {
        biased;
        interruption = &mut interrupt => {
            shutdown.cancel();
            return Ok(ReadyReconciliationOutcome::InterruptedBeforePublication(interruption));
        }
        _ = std::future::ready(()) => {}
    }

    let mut interruption = None;
    let mut last_error = None;
    let mut completed_attempts = 0;
    while completed_attempts < CONTROL_PLANE_RECONCILIATION_ATTEMPTS {
        let publication = if interruption.is_some() {
            publish().await
        } else {
            tokio::select! {
                biased;
                observed = &mut interrupt => {
                    shutdown.cancel();
                    interruption = Some(observed);
                    // The cancelled call may already have committed. It did
                    // not produce a reconciliation result, so retry the same
                    // retained identity without consuming the bounded result
                    // budget.
                    continue;
                }
                published = publish() => published,
            }
        };
        completed_attempts += 1;
        match publication {
            Ok(()) => {
                return Ok(match interruption {
                    Some(interruption) => {
                        ReadyReconciliationOutcome::InterruptedAfterReconciliation(interruption)
                    }
                    None => ReadyReconciliationOutcome::Published,
                });
            }
            Err(error) if error.is_indeterminate() => {
                tracing::warn!(
                    attempt = completed_attempts,
                    max_attempts = CONTROL_PLANE_RECONCILIATION_ATTEMPTS,
                    %error,
                    "Ready publication response was indeterminate; reconciling the same publication identity"
                );
                // LocalControlReadyPublisher retains its pending publication,
                // including publication_id and expected_revision, until an
                // exact ACK is validated. Never construct a replacement here.
                last_error = Some(anyhow::Error::new(error));
            }
            Err(error) => {
                return Err(ReadyReconciliationFailure {
                    control: anyhow::Error::new(error)
                        .context("Ready publication failed terminal validation or was rejected"),
                    interruption,
                });
            }
        }
        if completed_attempts < CONTROL_PLANE_RECONCILIATION_ATTEMPTS {
            if interruption.is_some() {
                tokio::time::sleep(CONTROL_PLANE_RECONCILIATION_DELAY).await;
            } else {
                tokio::select! {
                    biased;
                    observed = &mut interrupt => {
                        shutdown.cancel();
                        interruption = Some(observed);
                    }
                    _ = tokio::time::sleep(CONTROL_PLANE_RECONCILIATION_DELAY) => {}
                }
            }
        }
    }
    Err(ReadyReconciliationFailure {
        control: anyhow::Error::new(IndeterminateControlPlaneState {
            transition: "Ready",
            attempts: CONTROL_PLANE_RECONCILIATION_ATTEMPTS,
            source: last_error.unwrap_or_else(|| anyhow!("Ready publication produced no result")),
        }),
        interruption,
    })
}

async fn publish_shutdown_not_ready_reconciling<P>(publisher: &P) -> Result<()>
where
    P: LocalReadyPublisher,
{
    let mut last_error = None;
    for attempt in 1..=CONTROL_PLANE_RECONCILIATION_ATTEMPTS {
        match publisher.publish_shutdown_not_ready().await {
            Ok(()) => return Ok(()),
            Err(error) if error.is_indeterminate() => {
                tracing::warn!(
                    attempt,
                    max_attempts = CONTROL_PLANE_RECONCILIATION_ATTEMPTS,
                    %error,
                    "shutdown NotReady response was indeterminate; bounded reconciliation remains"
                );
                last_error = Some(anyhow::Error::new(error));
            }
            Err(error) => {
                return Err(anyhow::Error::new(error)
                    .context("shutdown NotReady failed terminal validation or was rejected"));
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

async fn publish_shutdown_not_ready_after_local_cancel<P>(
    publisher: &P,
    shutdown: &CancellationToken,
) -> Result<()>
where
    P: LocalReadyPublisher,
{
    // CancellationToken children already held by an active run are cancelled
    // synchronously here. The Session future also remains suspended while the
    // control-plane transition is reconciled, so no further command admission
    // can occur during that bounded I/O.
    shutdown.cancel();
    publish_shutdown_not_ready_reconciling(publisher).await
}

fn pre_ready_signal_result(signal: Result<()>, control: Result<()>) -> Result<()> {
    match (signal, control) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(signal), Ok(())) => Err(signal.context("wait for pre-Ready shutdown signal")),
        (Ok(()), Err(control)) => Err(control.context("publish pre-Ready shutdown NotReady")),
        (Err(signal), Err(control)) => Err(anyhow!(
            "pre-Ready shutdown signal monitor failed: {signal:#}; shutdown NotReady failed: {control:#}"
        )),
    }
}

fn signal_teardown_result(
    signal_failure: Option<String>,
    control: Result<()>,
    session: SessionTerminationReport,
) -> Result<()> {
    if let Some(signal) = signal_failure {
        return Err(anyhow::Error::new(SignalTeardownFailure {
            signal,
            control_plane: control.err().map_or_else(
                || "acknowledged NotReady".to_owned(),
                |error| error.to_string(),
            ),
            session,
        }));
    }
    if let Err(control) = control {
        return Err(control_teardown_error(control, session));
    }
    session.into_result()
}

fn combine_results(
    primary: Result<()>,
    secondary: Result<()>,
    secondary_label: &'static str,
) -> Result<()> {
    match (primary, secondary) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(secondary)) => Err(secondary.context(secondary_label)),
        (Err(primary), Err(secondary)) => Err(anyhow!(
            "{primary:#}; {secondary_label} also failed: {secondary:#}"
        )),
    }
}

async fn finish_runtime(
    primary: Result<()>,
    post_commit: ProductionPostCommitRuntime,
    supervisor: SupervisorRuntime,
    emergency: bool,
) -> Result<()> {
    let post_commit_result = if emergency {
        post_commit.invalidate_and_join().await
    } else {
        post_commit.shutdown_orderly().await
    };
    let primary = combine_results(
        primary,
        post_commit_result,
        if emergency {
            "post-commit emergency teardown"
        } else {
            "post-commit teardown"
        },
    );
    combine_results(
        primary,
        supervisor.cancel_and_join().await,
        "connection supervisor teardown",
    )
}

async fn teardown_pre_ready_operation_failure(
    session: Session<SessionGateway>,
    post_commit: ProductionPostCommitRuntime,
    supervisor: SupervisorRuntime,
    shutdown: CancellationToken,
    failure: anyhow::Error,
) -> Result<()> {
    shutdown.cancel();
    let report =
        SessionTerminationReport::from_result(session.run_until_cancelled(shutdown.clone()).await);
    let primary = combine_results(
        Err(failure),
        report.into_result(),
        "pre-Ready Session shutdown",
    );
    finish_runtime(primary, post_commit, supervisor, false).await
}

async fn teardown_owned_pre_ready_runtime(
    publisher: &LocalRuntimePublisher,
    session: Session<SessionGateway>,
    post_commit: ProductionPostCommitRuntime,
    supervisor: SupervisorRuntime,
    shutdown: CancellationToken,
    exit: PreReadyRuntimeExit,
) -> Result<()> {
    let control_result = publish_shutdown_not_ready_after_local_cancel(publisher, &shutdown).await;
    let report =
        SessionTerminationReport::from_result(session.run_until_cancelled(shutdown.clone()).await);
    let (primary, emergency) = runtime_exit_result(exit, control_result, report);
    finish_runtime(primary, post_commit, supervisor, emergency).await
}

fn runtime_exit_result(
    exit: PreReadyRuntimeExit,
    control_result: Result<()>,
    report: SessionTerminationReport,
) -> (Result<()>, bool) {
    match exit {
        PreReadyRuntimeExit::Signal(signal) => {
            let primary = signal_teardown_result(
                signal
                    .err()
                    .map(|error| format!("wait for pre-Ready shutdown signal: {error:#}")),
                control_result,
                report,
            );
            (primary, false)
        }
        PreReadyRuntimeExit::Dependency(dependency) => {
            let failure = match dependency {
                Ok(()) => anyhow!("authenticated runtime dependency became unavailable"),
                Err(error) => error.context("authenticated runtime dependency monitor failed"),
            };
            let primary = Err(anyhow::Error::new(DependencyTeardownFailure {
                dependency: failure.to_string(),
                control_plane: control_result.err().map_or_else(
                    || "acknowledged NotReady".to_owned(),
                    |error| error.to_string(),
                ),
                session: report,
            }));
            (primary, false)
        }
        PreReadyRuntimeExit::PostCommit(dispatcher) => {
            let failure = match dispatcher {
                Ok(()) => "post-commit dispatcher stopped unexpectedly".to_owned(),
                Err(error) => error.to_string(),
            };
            let primary = Err(anyhow::Error::new(PostCommitTeardownFailure {
                dispatcher: failure,
                control_plane: control_result.err().map_or_else(
                    || "acknowledged NotReady".to_owned(),
                    |error| error.to_string(),
                ),
                session: report,
            }));
            (primary, true)
        }
        PreReadyRuntimeExit::Supervisor(supervisor_exit) => {
            let failure = match supervisor_exit {
                Ok(termination) => termination.to_string(),
                Err(error) => format!("connection supervisor monitor failed: {error:#}"),
            };
            let primary = Err(anyhow::Error::new(SupervisorTeardownFailure {
                supervisor: failure,
                control_plane: control_result.err().map_or_else(
                    || "acknowledged NotReady".to_owned(),
                    |error| error.to_string(),
                ),
                session: report,
            }));
            (primary, true)
        }
    }
}

async fn teardown_failed_session_start(
    publisher: &LocalRuntimePublisher,
    post_commit: ProductionPostCommitRuntime,
    supervisor: SupervisorRuntime,
    shutdown: CancellationToken,
    exit: PreReadyRuntimeExit,
    start_error: anyhow::Error,
) -> Result<()> {
    let control_result = publish_shutdown_not_ready_after_local_cancel(publisher, &shutdown).await;
    let report = SessionTerminationReport {
        status: "startup failed",
        ownership: SessionOwnershipReport::Lost,
        failure: Some(start_error.to_string()),
    };
    let (primary, _) = runtime_exit_result(exit, control_result, report);
    // Session never began admitting commands, so no orderly producer proof
    // exists. Invalidate rather than mint quiescence on its behalf.
    finish_runtime(primary, post_commit, supervisor, true).await
}

async fn supervise_ready_session(
    authority: &RuntimeEpochAuthority,
    publisher: &LocalRuntimePublisher,
    session: Session<SessionGateway>,
    mut signal: BoxFuture<'static, Result<()>>,
    mut dependency_failure: BoxFuture<'static, Result<()>>,
    mut post_commit_failure: BoxFuture<'static, Result<()>>,
    post_commit: ProductionPostCommitRuntime,
    mut supervisor: SupervisorRuntime,
    shutdown: CancellationToken,
) -> Result<()> {
    let session_run = session.run_until_cancelled(shutdown.clone());
    tokio::pin!(session_run);

    enum Exit {
        Session(SessionResult),
        Runtime(PreReadyRuntimeExit),
    }

    let exit = tokio::select! {
        biased;
        exit = wait_required_runtime_exit(
            &mut signal,
            &mut dependency_failure,
            &mut post_commit_failure,
            &mut supervisor,
        ) => {
            Exit::Runtime(exit)
        },
        result = &mut session_run => Exit::Session(result),
    };
    match exit {
        Exit::Session(result) => {
            let control_result = publish_shutdown_not_ready_reconciling(publisher).await;
            let report = SessionTerminationReport::from_result(result);
            if let Err(control) = control_result.as_ref() {
                stop_for_control_failure(
                    authority,
                    &shutdown,
                    control,
                    "post-Session NotReady could not be reconciled",
                );
            } else {
                shutdown.cancel();
            }
            let primary = unexpected_session_result(report.clone());
            let primary = match control_result {
                Ok(()) => primary,
                Err(control) => combine_results(
                    primary,
                    Err(control_teardown_error(control, report)),
                    "unexpected Session shutdown control-plane transition",
                ),
            };
            finish_runtime(primary, post_commit, supervisor, false).await
        }
        Exit::Runtime(exit) => {
            // Fail closed locally before waiting on control-plane I/O. Keep
            // every owned task retained until the bounded NotReady transition
            // completes or local generation fencing escalates.
            let control_result =
                publish_shutdown_not_ready_after_local_cancel(publisher, &shutdown).await;
            if let Err(control) = control_result.as_ref() {
                stop_for_control_failure(
                    authority,
                    &shutdown,
                    control,
                    "required-runtime failure NotReady could not be reconciled",
                );
            }
            let report = SessionTerminationReport::from_result((&mut session_run).await);
            let (primary, emergency) = runtime_exit_result(exit, control_result, report);
            finish_runtime(primary, post_commit, supervisor, emergency).await
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
    use std::sync::{Arc, Mutex};

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
        async fn publish_not_ready(&self) -> LocalPublicationResult<()> {
            unreachable!("test exercises only shutdown NotReady")
        }

        async fn publish_ready(
            &self,
            _proof: &crate::gateway::local_runtime::LocalReadyProof,
        ) -> LocalPublicationResult<()> {
            unreachable!("test exercises only shutdown NotReady")
        }

        async fn publish_shutdown_not_ready(&self) -> LocalPublicationResult<()> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Err(LocalPublicationError::indeterminate(anyhow!(
                "local control unavailable"
            )))
        }
    }

    struct RejectedShutdownPublisher {
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl LocalReadyPublisher for RejectedShutdownPublisher {
        async fn publish_not_ready(&self) -> LocalPublicationResult<()> {
            unreachable!("test exercises only shutdown NotReady")
        }

        async fn publish_ready(
            &self,
            _proof: &crate::gateway::local_runtime::LocalReadyProof,
        ) -> LocalPublicationResult<()> {
            unreachable!("test exercises only shutdown NotReady")
        }

        async fn publish_shutdown_not_ready(&self) -> LocalPublicationResult<()> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Err(LocalPublicationError::terminal(anyhow!(
                "local control rejected shutdown publication"
            )))
        }
    }

    struct BlockingShutdownPublisher {
        shutdown: CancellationToken,
        entered: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl LocalReadyPublisher for BlockingShutdownPublisher {
        async fn publish_not_ready(&self) -> LocalPublicationResult<()> {
            unreachable!("test exercises only shutdown NotReady")
        }

        async fn publish_ready(
            &self,
            _proof: &crate::gateway::local_runtime::LocalReadyProof,
        ) -> LocalPublicationResult<()> {
            unreachable!("test exercises only shutdown NotReady")
        }

        async fn publish_shutdown_not_ready(&self) -> LocalPublicationResult<()> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(())
        }
    }

    struct ReconciledShutdownPublisher {
        attempts: AtomicUsize,
        order: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl LocalReadyPublisher for ReconciledShutdownPublisher {
        async fn publish_not_ready(&self) -> LocalPublicationResult<()> {
            unreachable!("test exercises only shutdown NotReady")
        }

        async fn publish_ready(
            &self,
            _proof: &crate::gateway::local_runtime::LocalReadyProof,
        ) -> LocalPublicationResult<()> {
            unreachable!("test exercises only shutdown NotReady")
        }

        async fn publish_shutdown_not_ready(&self) -> LocalPublicationResult<()> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                self.order
                    .lock()
                    .unwrap()
                    .push("shutdown_not_ready_indeterminate");
                Err(LocalPublicationError::indeterminate(anyhow!(
                    "shutdown NotReady committed but its ACK was lost"
                )))
            } else {
                self.order
                    .lock()
                    .unwrap()
                    .push("shutdown_not_ready_reconciled");
                Ok(())
            }
        }
    }

    async fn assert_required_runtime_failure_reconciles_ready_then_not_ready(
        exit: PreReadyRuntimeExit,
        expected: &'static str,
    ) {
        let attempts = Arc::new(AtomicUsize::new(0));
        let order = Arc::new(Mutex::new(Vec::new()));
        let (ready_started_tx, ready_started_rx) = tokio::sync::oneshot::channel();
        let ready_started_tx = Arc::new(Mutex::new(Some(ready_started_tx)));
        let interrupt = async move {
            ready_started_rx
                .await
                .expect("first Ready publication starts");
            exit
        };
        let shutdown = CancellationToken::new();

        let outcome = publish_ready_reconciling(
            {
                let attempts = attempts.clone();
                let order = order.clone();
                move || {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    let order = order.clone();
                    let ready_started_tx = ready_started_tx.clone();
                    async move {
                        if attempt == 0 {
                            order
                                .lock()
                                .unwrap()
                                .push("ready_same_identity_committed_ack_lost");
                            ready_started_tx
                                .lock()
                                .unwrap()
                                .take()
                                .expect("one Ready start signal")
                                .send(())
                                .expect("required-runtime observer remains");
                            std::future::pending::<LocalPublicationResult<()>>().await
                        } else {
                            order.lock().unwrap().push("ready_same_identity_reconciled");
                            Ok(())
                        }
                    }
                }
            },
            interrupt,
            &shutdown,
        )
        .await
        .unwrap_or_else(|failure| {
            panic!(
                "the retained Ready publication must reconcile: {:#}",
                failure.control
            )
        });

        let observed = match outcome {
            ReadyReconciliationOutcome::InterruptedAfterReconciliation(observed) => observed,
            ReadyReconciliationOutcome::InterruptedBeforePublication(_) => {
                panic!("failure was injected after Ready began")
            }
            ReadyReconciliationOutcome::Published => {
                panic!("required-runtime failure must prevent serving")
            }
        };
        match (expected, observed) {
            ("dependency", PreReadyRuntimeExit::Dependency(Err(error))) => {
                assert!(error.to_string().contains("dependency"))
            }
            ("post-commit", PreReadyRuntimeExit::PostCommit(Err(error))) => {
                assert!(error.to_string().contains("post-commit"))
            }
            (
                "supervisor",
                PreReadyRuntimeExit::Supervisor(Ok(SupervisorTermination::Failed(reason))),
            ) => assert!(reason.contains("supervisor")),
            (expected, _) => panic!("unexpected {expected} runtime exit shape"),
        }
        assert!(shutdown.is_cancelled());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        let publisher = ReconciledShutdownPublisher {
            attempts: AtomicUsize::new(0),
            order: order.clone(),
        };
        publish_shutdown_not_ready_reconciling(&publisher)
            .await
            .expect("shutdown NotReady must reconcile after exact Ready");
        assert_eq!(publisher.attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            order.lock().unwrap().as_slice(),
            [
                "ready_same_identity_committed_ack_lost",
                "ready_same_identity_reconciled",
                "shutdown_not_ready_indeterminate",
                "shutdown_not_ready_reconciled",
            ]
        );
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
                            std::future::pending::<LocalPublicationResult<()>>().await
                        } else {
                            order.lock().unwrap().push("ready_reconciled");
                            Ok(())
                        }
                    }
                }
            },
            signal.as_mut(),
            &shutdown,
        )
        .await
        .unwrap_or_else(|failure| {
            panic!(
                "second attempt must reconcile the retained Ready publication: {:#}",
                failure.control
            )
        });
        assert!(matches!(
            outcome,
            ReadyReconciliationOutcome::InterruptedAfterReconciliation(Ok(()))
        ));
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
    async fn required_runtime_failures_during_lost_ready_ack_reconcile_before_not_ready() {
        assert_required_runtime_failure_reconciles_ready_then_not_ready(
            PreReadyRuntimeExit::Dependency(Err(anyhow!("dependency failure"))),
            "dependency",
        )
        .await;
        assert_required_runtime_failure_reconciles_ready_then_not_ready(
            PreReadyRuntimeExit::PostCommit(Err(anyhow!("post-commit failure"))),
            "post-commit",
        )
        .await;
        assert_required_runtime_failure_reconciles_ready_then_not_ready(
            PreReadyRuntimeExit::Supervisor(Ok(SupervisorTermination::Failed(
                "supervisor failure".to_owned(),
            ))),
            "supervisor",
        )
        .await;
    }

    #[tokio::test]
    async fn already_known_required_runtime_failure_never_initiates_ready() {
        let attempts = AtomicUsize::new(0);
        let shutdown = CancellationToken::new();
        let outcome = publish_ready_reconciling(
            || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
            std::future::ready(PreReadyRuntimeExit::Supervisor(Ok(
                SupervisorTermination::Completed,
            ))),
            &shutdown,
        )
        .await
        .unwrap_or_else(|failure| {
            panic!(
                "known failure must stop before Ready: {:#}",
                failure.control
            )
        });

        assert!(matches!(
            outcome,
            ReadyReconciliationOutcome::InterruptedBeforePublication(
                PreReadyRuntimeExit::Supervisor(Ok(SupervisorTermination::Completed))
            )
        ));
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        assert!(shutdown.is_cancelled());
    }

    #[tokio::test]
    async fn local_runtime_is_cancelled_before_blocked_shutdown_not_ready_io() {
        let shutdown = CancellationToken::new();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let publisher = Arc::new(BlockingShutdownPublisher {
            shutdown: shutdown.clone(),
            entered: entered.clone(),
            release: release.clone(),
        });
        let publish = tokio::spawn({
            let publisher = publisher.clone();
            let shutdown = shutdown.clone();
            async move {
                publish_shutdown_not_ready_after_local_cancel(publisher.as_ref(), &shutdown).await
            }
        });

        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("shutdown NotReady I/O must begin");
        let cancelled_before_release = publisher.shutdown.is_cancelled() && shutdown.is_cancelled();
        release.notify_one();
        publish
            .await
            .expect("shutdown publication task must join")
            .expect("shutdown NotReady publication must complete");
        assert!(
            cancelled_before_release,
            "local cancellation must precede blocked control-plane I/O"
        );
    }

    #[tokio::test]
    async fn pre_ready_signal_interrupts_a_blocked_preparation_wait() {
        let (preparation_entered_tx, preparation_entered_rx) = tokio::sync::oneshot::channel();
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            preparation_entered_rx
                .await
                .expect("pre-Ready preparation starts");
            signal_tx.send(()).expect("signal future remains");
        });
        let preparation = async move {
            preparation_entered_tx
                .send(())
                .expect("preparation observer remains");
            std::future::pending::<Result<()>>().await
        };
        let mut signal: BoxFuture<'static, Result<()>> =
            Box::pin(async move { signal_rx.await.context("test signal sender dropped") });

        match wait_pre_ready_or_signal(preparation, &mut signal).await {
            PreReadyWait::Signal(Ok(())) => {}
            PreReadyWait::Signal(Err(error)) => panic!("signal monitor failed: {error:#}"),
            PreReadyWait::Completed(_) => {
                panic!("blocked preparation completed before the owned signal")
            }
        }
    }

    #[tokio::test]
    async fn ready_reconciliation_is_bounded_and_typed() {
        let attempts = AtomicUsize::new(0);
        let mut signal: BoxFuture<'static, Result<()>> =
            Box::pin(std::future::pending::<Result<()>>());
        let shutdown = CancellationToken::new();
        let failure = publish_ready_reconciling(
            || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(LocalPublicationError::indeterminate(anyhow!(
                    "local control unavailable"
                )))
            },
            signal.as_mut(),
            &shutdown,
        )
        .await
        .err()
        .expect("reconciliation must escalate instead of serving forever");
        let error = failure.control;
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
    async fn ready_terminal_rejection_is_not_retried() {
        let attempts = AtomicUsize::new(0);
        let mut signal: BoxFuture<'static, Result<()>> =
            Box::pin(std::future::pending::<Result<()>>());
        let shutdown = CancellationToken::new();
        let failure = publish_ready_reconciling(
            || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(LocalPublicationError::terminal(anyhow!(
                    "local control rejected Ready"
                )))
            },
            signal.as_mut(),
            &shutdown,
        )
        .await
        .err()
        .expect("terminal rejection must fail immediately");
        let error = failure.control;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(
            error
                .downcast_ref::<IndeterminateControlPlaneState>()
                .is_none()
        );
        assert!(error.to_string().contains("terminal validation"));
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

    #[tokio::test]
    async fn shutdown_not_ready_terminal_rejection_is_not_retried() {
        let publisher = RejectedShutdownPublisher {
            attempts: AtomicUsize::new(0),
        };
        let error = publish_shutdown_not_ready_reconciling(&publisher)
            .await
            .expect_err("terminal shutdown rejection must fail immediately");
        assert_eq!(publisher.attempts.load(Ordering::SeqCst), 1);
        assert!(
            error
                .downcast_ref::<IndeterminateControlPlaneState>()
                .is_none()
        );
        assert!(error.to_string().contains("terminal validation"));
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
    fn signal_teardown_error_preserves_simultaneous_session_ownership_loss() {
        let report = SessionTerminationReport::from_result(SessionResult::Failed {
            failure: crate::agent::SessionFailure::RuntimeShutdownOwnershipLost,
            ownership: crate::agent::RunOwnership::Lost,
        });
        let error =
            signal_teardown_result(Some("signal receiver failed".to_owned()), Ok(()), report)
                .expect_err("both failures must be reported");
        assert!(error.downcast_ref::<SignalTeardownFailure>().is_some());
        let rendered = error.to_string();
        assert!(rendered.contains("signal receiver failed"));
        assert!(rendered.contains("ownership=Lost"));
        assert!(rendered.contains("runtime shutdown"));
    }

    #[test]
    fn clean_session_exit_without_a_selected_runtime_trigger_is_an_error() {
        let report = SessionTerminationReport {
            status: "completed",
            ownership: SessionOwnershipReport::Recovered,
            failure: None,
        };
        let error = unexpected_session_result(report)
            .expect_err("unselected Session completion must never be clean bootstrap shutdown");
        assert!(
            error
                .downcast_ref::<UnexpectedSessionTermination>()
                .is_some()
        );
        assert!(error.to_string().contains("without a selected shutdown"));
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
