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
mod tools;

use std::{env, io, sync::Arc};

use anyhow::{Context, Result, anyhow};
use gateway::stdio::{InvalidCommand, StdioGateway};
use gateway::{
    CommandAckStatus, Envelope, Gateway, GatewayClosed, GatewayReader, GatewayWriter,
    InboundCommand, OutboundFrame,
};
use store::{
    AgentScope, DataKeyPurpose, EnvironmentKeyProvider, EventWriter, InboundAdmission, Store,
    SuffixRecovery,
};

#[cfg(test)]
pub(crate) const fn canonical_live_responses_harness_available() -> bool {
    true
}

#[cfg(test)]
pub(crate) async fn run_canonical_live_responses_roundtrip(
    spec: provider::ModelSpec,
    api_key: String,
) {
    agent::run_canonical_live_responses_roundtrip(spec, api_key).await;
}

fn main() -> Result<()> {
    let mut arguments = env::args();
    let _program = arguments.next();
    let mode = arguments.next();
    if arguments.next().is_some() {
        anyhow::bail!("sumi-agent accepts at most one mode argument");
    }
    if mode.as_deref() == Some("--supervisor-allocate") {
        let config = runtime::supervisor_allocator::SupervisorAllocatorConfig::from_process_env()?;
        let metadata = runtime::supervisor_allocator::allocate(&config)?;
        println!("{}", serde_json::to_string(&metadata)?);
        return Ok(());
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to construct Tokio runtime")?;
    runtime.block_on(async_main(mode))
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
        Some("--tool-executor") => {
            tracing::warn!(
                service = "tool-executor",
                trust = "low-trust-local",
                "starting service mode"
            );
            return tools::executor::run_tool_executor_mode().await;
        }
        Some("--tool-executor-socket") => {
            tracing::warn!(
                service = "tool-executor-socket",
                trust = "low-trust-local",
                "starting service mode"
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
        Some("--low-trust") => {
            tracing::warn!(
                service = "agent",
                trust = "low-trust-stdio",
                "starting explicit stdio mode"
            );
        }
        Some(other) => anyhow::bail!("unknown sumi-agent mode {other}"),
        None => return bootstrap::run_production().await,
    }
    let config = config::Config::load().await?;

    let model_spec = config.model_spec()?;
    let personality_agent_id = config.personality_agent_id.clone();
    let scope = AgentScope {
        personality_agent_id: personality_agent_id.clone(),
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
        store.private_key(purpose).await?;
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
        personality_agent_id = %personality_agent_id,
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
                            personality_agent_id: personality_agent_id.clone(),
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

        if inbound.personality_agent_id() != &personality_agent_id
            || inbound.provenance().personality_agent_id() != &personality_agent_id
        {
            anyhow::bail!(
                "stdio command target/provenance does not match configured personality_agent_id"
            );
        }

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
