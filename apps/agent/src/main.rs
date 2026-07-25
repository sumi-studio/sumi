mod agent;
mod apiclient;
mod approval;
mod config;
mod gateway;
mod memory;
mod prompts;
pub mod provider;
pub mod runtime;
mod store;
mod tools;

use std::{env, io, sync::Arc};

use anyhow::{Result, anyhow};
use gateway::stdio::{InvalidCommand, StdioGateway};
use gateway::{
    CommandAckStatus, Envelope, Gateway, GatewayClosed, GatewayReader, GatewayWriter,
    InboundCommand, OutboundFrame,
};
use store::{
    AgentScope, DataKeyPurpose, EnvironmentKeyProvider, EventWriter, InboundAdmission, Store,
    SuffixRecovery,
};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_writer(io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("SUMI_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("sumi_agent=info")),
        )
        .init();
    match env::args().nth(1).as_deref() {
        Some("--tool-executor") => {
            tracing::warn!(
                service = "tool-executor",
                trust = "low-trust-local",
                "starting service mode"
            );
            return tools::executor::run_tool_executor_mode().await;
        }
        Some("--artifact-broker") => {
            tracing::warn!(
                service = "artifact-broker",
                trust = "low-trust-local",
                "starting service mode"
            );
            return tools::executor::run_artifact_broker_mode().await;
        }
        _ => {}
    }
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
