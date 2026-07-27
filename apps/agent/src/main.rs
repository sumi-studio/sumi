mod agent;
mod apiclient;
mod approval;
mod bootstrap;
mod config;
mod gateway;
mod memory;
mod prompts;
pub mod provider;
pub mod runtime;
mod store;
mod t27_recovery;
mod tools;

use std::{
    env, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use gateway::{
    CommandAckStatus, Envelope, Gateway, GatewayClosed, GatewayReader, GatewayWriter,
    InboundCommand, InvalidCommand, OutboundFrame, StdioGateway,
};
use runtime::{
    allocator::{acquire_generation, print_shell_exports},
    contracts::{GenerationRecoveryFence, ProcessGeneration, ProcessGenerationLease},
};
use store::{
    AgentScope, DataKeyPurpose, EnvironmentKeyProvider, EventWriter, InboundAdmission, Store,
    SuffixRecovery,
};

fn main() -> Result<()> {
    let mode = env::args().nth(1);
    harden_process_entry(mode.as_deref())?;
    if env::var_os("SUMI_ISOLATION_PREFLIGHT").is_some()
        && matches!(
            mode.as_deref(),
            Some("--tool-executor") | Some("--tool-executor-socket")
        )
    {
        // CLONE_NEWUSER is rejected once Tokio has started worker threads.
        // Run this short-lived host prerequisite probe before constructing the
        // runtime so deployed executors fail closed instead of degrading.
        return tools::bash::preflight_namespace_isolation()
            .context("broker-socket namespace isolation preflight failed");
    }

    tokio::runtime::Runtime::new()
        .context("failed to construct Tokio runtime")?
        .block_on(async_main(mode))
}

fn harden_process_entry(_mode: Option<&str>) -> Result<()> {
    // Every production role receives secret-bearing environment variables at
    // exec time. Disable ptrace/core-dump access before Tokio, tracing, the
    // namespace preflight, or any environment parsing can extend that exposure.
    tools::executor::set_dumpable(0).context("failed to set PR_SET_DUMPABLE at process entry")
}

async fn async_main(mode: Option<String>) -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_writer(io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("SUMI_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("sumi_agent=info")),
        )
        .init();

    match mode.as_deref() {
        Some("--low-trust") => {
            tracing::warn!(
                service = "sumi-agent",
                trust = "low-trust-local",
                "starting low-trust admission harness; not for production"
            );
            return run_low_trust_admission().await;
        }
        Some("--tool-executor") => {
            tracing::warn!(
                service = "tool-executor",
                trust = "low-trust-local",
                "starting service mode"
            );
            return tools::executor::run_tool_executor_mode().await;
        }
        Some("--tool-executor-socket") => {
            tracing::info!(
                service = "tool-executor-socket",
                "starting socket-bound executor"
            );
            return tools::executor::run_tool_executor_socket_mode().await;
        }
        Some("--artifact-broker") => {
            tracing::warn!(
                service = "artifact-broker",
                trust = "low-trust-local",
                "starting service mode"
            );
            return tools::executor::run_artifact_broker_mode().await;
        }
        Some("--allocate-generation") => {
            let state_dir = env::var_os("SUMI_STATE_DIR")
                .ok_or_else(|| anyhow!("SUMI_STATE_DIR is required for generation allocation"))?;
            let allocation =
                acquire_generation(&state_dir).context("generation allocation failed")?;
            print_shell_exports(&allocation);
            return Ok(());
        }
        Some("--check-unix-socket") => {
            let socket = env::var_os("SUMI_READINESS_SOCKET")
                .ok_or_else(|| anyhow!("SUMI_READINESS_SOCKET is required for socket readiness"))?;
            return tools::executor::wait_for_unix_socket(Path::new(&socket), "readiness").await;
        }
        Some("--supervisor-heartbeat-check") => {
            let state_dir = PathBuf::from(
                env::args()
                    .nth(2)
                    .context("runtime state directory is required")?,
            );
            let agent_id = env::args().nth(3).context("agent id is required")?;
            let generation = ProcessGeneration::from_wire(
                env::args()
                    .nth(4)
                    .context("generation is required")?
                    .parse()
                    .context("generation must be an integer")?,
            )?;
            let lease = ProcessGenerationLease::new(
                generation,
                env::args().nth(5).context("lease id is required")?,
            )?;
            let fence = GenerationRecoveryFence::new(
                &lease,
                env::args().nth(6).context("fence id is required")?,
            )?;
            let max_age_ms: u64 = env::args()
                .nth(7)
                .context("heartbeat max age milliseconds is required")?
                .parse()
                .context("heartbeat max age must be an integer")?;
            runtime::publisher::RuntimeHeartbeatPublisher::validate_fresh(
                state_dir,
                &agent_id,
                generation,
                &lease,
                &fence,
                std::time::Duration::from_millis(max_age_ms),
            )?;
            return Ok(());
        }
        Some("--supervisor-prepare-cgroup") => {
            let tenant_id = env::var("SUMI_TENANT_ID")
                .context("SUMI_TENANT_ID is required for cgroup preparation")?;
            let agent_id = env::var("SUMI_AGENT_ID")
                .context("SUMI_AGENT_ID is required for cgroup preparation")?;
            let conversation_id = env::var("SUMI_CONVERSATION_ID")
                .context("SUMI_CONVERSATION_ID is required for cgroup preparation")?;
            let generation = env::var("SUMI_RPC_GENERATION")
                .context("SUMI_RPC_GENERATION is required for cgroup preparation")?
                .parse::<u64>()
                .context("SUMI_RPC_GENERATION must be an integer")?;
            let executor_base = runtime::supervisor::prepare_cgroup_base(
                &tenant_id,
                &agent_id,
                &conversation_id,
                generation,
            )
            .context("failed to prepare executor cgroup base")?;
            let service_base = runtime::supervisor::prepare_service_cgroup_base(
                &tenant_id,
                &agent_id,
                &conversation_id,
                generation,
            )
            .context("failed to prepare service cgroup base")?;
            println!(
                "export SUMI_EXECUTOR_CGROUP_BASE={}",
                executor_base.display()
            );
            println!("export SUMI_SERVICE_CGROUP_BASE={}", service_base.display());
            let parent = executor_base
                .parent()
                .context("executor cgroup base has no parent; cannot derive SUMI_CGROUP_PARENT")?;
            println!("export SUMI_CGROUP_PARENT={}", parent.display());
            return Ok(());
        }
        Some("--supervisor-recovery-scan") => {
            let base = PathBuf::from(
                env::args()
                    .nth(2)
                    .context("cgroup base path is required as the first argument")?,
            );
            let generation = env::args()
                .nth(3)
                .context("current generation is required as the second argument")?
                .parse::<u64>()
                .context("current generation must be an integer")?;
            let removed = runtime::supervisor::scan_and_kill_stale(&base, generation)
                .context("stale-generation recovery scan failed")?;
            let serialized: Vec<String> = removed.iter().map(|p| p.display().to_string()).collect();
            println!("{}", serde_json::to_string(&serialized)?);
            return Ok(());
        }
        Some("--supervisor-kill-stale-processes") => {
            let base = PathBuf::from(
                env::args()
                    .nth(2)
                    .context("cgroup base path is required as the first argument")?,
            );
            let generation = env::args()
                .nth(3)
                .context("current generation is required as the second argument")?
                .parse::<u64>()
                .context("current generation must be an integer")?;
            let removed = runtime::supervisor::scan_and_kill_stale_services(&base, generation)
                .context("stale service recovery scan failed")?;
            let serialized: Vec<String> = removed.iter().map(|p| p.display().to_string()).collect();
            println!("{}", serde_json::to_string(&serialized)?);
            return Ok(());
        }
        Some("--supervisor-reap-generation") => {
            let executor = PathBuf::from(
                env::args()
                    .nth(2)
                    .context("executor cgroup path is required")?,
            );
            let service = PathBuf::from(
                env::args()
                    .nth(3)
                    .context("service cgroup path is required")?,
            );
            let proof_dir = PathBuf::from(
                env::args()
                    .nth(4)
                    .context("host proof directory is required")?,
            );
            let (tenant_id, agent_id, conversation_id, lease, fence) = supervisor_identity()?;
            t27_recovery::supervisor_reap_generation(
                &proof_dir,
                &tenant_id,
                &agent_id,
                &conversation_id,
                &lease,
                &fence,
                &executor,
                &service,
            )?;
            return Ok(());
        }
        Some("--supervisor-fulfill-recovery") => {
            let request = PathBuf::from(
                env::args()
                    .nth(2)
                    .context("runtime recovery request path is required")?,
            );
            let proof_dir = PathBuf::from(
                env::args()
                    .nth(3)
                    .context("host proof directory is required")?,
            );
            let (tenant_id, agent_id, conversation_id, lease, fence) = supervisor_identity()?;
            t27_recovery::supervisor_fulfill_recovery_request(
                &request,
                &proof_dir,
                &tenant_id,
                &agent_id,
                &conversation_id,
                &lease,
                &fence,
            )?;
            return Ok(());
        }
        _ => {}
    }

    // Production bootstrap boundary. The supervisor must issue all required
    // identity, lease, and fence variables. Missing identity is fail-closed;
    // the low-trust admission harness is only selectable explicitly with
    // `--low-trust` and must never be reachable by accident in production.
    if env::var("SUMI_RPC_GENERATION").is_ok() && env::var("SUMI_RPC_NONCE").is_ok() {
        return bootstrap::run_production().await;
    }

    Err(anyhow!(
        "production identity (SUMI_RPC_GENERATION and SUMI_RPC_NONCE) is required; \
         use --low-trust only for test/development"
    ))
}

fn supervisor_identity() -> Result<(
    String,
    String,
    String,
    ProcessGenerationLease,
    GenerationRecoveryFence,
)> {
    let tenant_id = env::var("SUMI_TENANT_ID").context("SUMI_TENANT_ID is required")?;
    let agent_id = env::var("SUMI_AGENT_ID").context("SUMI_AGENT_ID is required")?;
    let conversation_id =
        env::var("SUMI_CONVERSATION_ID").context("SUMI_CONVERSATION_ID is required")?;
    let generation = env::var("SUMI_RPC_GENERATION")
        .context("SUMI_RPC_GENERATION is required")?
        .parse::<u64>()
        .context("SUMI_RPC_GENERATION must be an integer")?;
    let lease = ProcessGenerationLease::new(
        ProcessGeneration::from_wire(generation).context("invalid supervisor generation")?,
        env::var("SUMI_PROCESS_GENERATION_LEASE_ID")
            .context("SUMI_PROCESS_GENERATION_LEASE_ID is required")?,
    )
    .context("invalid supervisor generation lease")?;
    let fence = GenerationRecoveryFence::new(
        &lease,
        env::var("SUMI_GENERATION_RECOVERY_FENCE_ID")
            .context("SUMI_GENERATION_RECOVERY_FENCE_ID is required")?,
    )
    .context("invalid supervisor generation fence")?;
    Ok((tenant_id, agent_id, conversation_id, lease, fence))
}

async fn run_low_trust_admission() -> Result<()> {
    let config = config::Config::load().await?;

    let model_spec = config.model_spec()?;
    let conversation_id = config.conversation_id.clone();
    let scope = AgentScope {
        tenant_id: env::var("SUMI_TENANT_ID").unwrap_or_else(|_| "local-tenant".to_owned()),
        agent_id: env::var("SUMI_AGENT_ID").unwrap_or_else(|_| "local-agent".to_owned()),
        conversation_id: conversation_id.clone(),
    };
    let key_provider = Arc::new(EnvironmentKeyProvider::from_env(
        "SUMI_AGENT_WRAPPING_KEY",
        env::var("SUMI_AGENT_WRAPPING_KEY_ID")
            .unwrap_or_else(|_| "local-env-wrapping-key/v1".to_owned()),
    )?);
    let store = Arc::new(Store::open(&config.database_path, scope, key_provider).await?);
    // EventWriter never mints a key halfway through a command/event transaction.
    for purpose in [
        DataKeyPurpose::Command,
        DataKeyPurpose::Event,
        DataKeyPurpose::Transcript,
    ] {
        store.conversation_key(purpose).await?;
    }
    let event_writer = EventWriter::new(store.clone());
    event_writer.initialize_recovery_checkpoint().await?;
    let pending_recovery = SuffixRecovery::recover_t12_prefix(&store, &event_writer).await?;
    if !pending_recovery.is_empty() {
        tracing::warn!(
            pending_steps = ?pending_recovery,
            "durable suffix remains after the T12 prefix; T17 production hydration must resolve it before T26 composition"
        );
    }
    let mut admission = InboundAdmission::after_t12_recovery(!pending_recovery.is_empty());
    if admission.is_replay_only() {
        tracing::warn!(
            "gateway command admission is replay-only; the T15 injected seam does not replace T17 production suffix hydration"
        );
    }
    let command_digest_factory = store.command_digest_factory().await?;
    let (mut gateway_reader, mut gateway_writer) =
        StdioGateway::new(command_digest_factory).split();

    tracing::info!(
        conversation_id,
        workspace = %config.workspace.display(),
        database_path = %config.database_path.display(),
        model_preset = ?config.model.preset,
        model_id = %model_spec.id,
        model_provider = %model_spec.provider,
        system_prompt_version = prompts::SYSTEM_PROMPT_VERSION,
        system_prompt_configured = !config.system_prompt.is_empty(),
        "sumi-agent starting"
    );

    loop {
        let inbound = match gateway_reader.next_command().await {
            Ok(inbound) => inbound,
            Err(error) if error.downcast_ref::<GatewayClosed>().is_some() => break,
            Err(error) if error.downcast_ref::<InvalidCommand>().is_some() => {
                tracing::warn!(%error, "rejected invalid stdio command");
                gateway_writer
                    .send(OutboundFrame::Event {
                        envelope: Envelope {
                            seq: None,
                            conversation_id: conversation_id.clone(),
                            event: serde_json::json!({
                                "type": "error",
                                "message": "invalid command envelope",
                            }),
                        },
                    })
                    .await?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };

        let receipt_ack = admission.receive(&event_writer, &inbound).await?;
        if receipt_ack.status != CommandAckStatus::Received {
            gateway_writer
                .send(OutboundFrame::CommandAck { ack: receipt_ack })
                .await?;
            continue;
        }
        match &inbound {
            InboundCommand::Invalid { .. } => {
                gateway_writer
                    .send(OutboundFrame::CommandAck { ack: receipt_ack })
                    .await?;
                continue;
            }
            InboundCommand::Valid(command)
                if matches!(&command.command, gateway::Command::Abort {}) =>
            {
                // A fully idle Abort is a durable no-op. Commit terminal state
                // before either ACK is attempted so a writer failure is replay-safe.
                let mut terminal_acks = event_writer
                    .apply_idle_abort_cutoff(command.command_id.as_str(), command.seq)
                    .await?;
                let terminal_ack = terminal_acks
                    .pop()
                    .ok_or_else(|| anyhow!("terminal Abort ACK disappeared"))?;
                for prior_ack in terminal_acks {
                    gateway_writer
                        .send(OutboundFrame::CommandAck { ack: prior_ack })
                        .await?;
                }
                gateway_writer
                    .send(OutboundFrame::CommandAck { ack: receipt_ack })
                    .await?;
                gateway_writer
                    .send(OutboundFrame::CommandAck { ack: terminal_ack })
                    .await?;
                admission.resume_after_suffix_recovery();
                continue;
            }
            InboundCommand::Valid(_) => {}
        }

        gateway_writer
            .send(OutboundFrame::CommandAck { ack: receipt_ack })
            .await?;
    }

    tracing::info!("sumi-agent stopped");
    Ok(())
}

#[cfg(test)]
mod process_entry_tests {
    use super::*;

    const PR_GET_DUMPABLE: libc::c_int = 3;

    fn dumpable() -> libc::c_int {
        unsafe { libc::prctl(PR_GET_DUMPABLE, 0, 0, 0, 0) }
    }

    #[test]
    fn every_secret_bearing_role_disables_dumpability_before_async_startup() {
        for mode in [
            None,
            Some("--tool-executor"),
            Some("--tool-executor-socket"),
            Some("--artifact-broker"),
        ] {
            assert_eq!(unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 1, 0, 0, 0) }, 0);
            harden_process_entry(mode).expect("entry hardening");
            assert_eq!(dumpable(), 0, "mode {mode:?} remained dumpable");
        }
    }
}
