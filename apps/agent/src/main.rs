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

use std::{env, io, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use gateway::{
    CommandAck, CommandAckStatus, Envelope, Gateway, GatewayClosed, GatewayReader, GatewayWriter,
    InboundCommand, InvalidCommand, OutboundFrame, StdioGateway,
};
use store::{
    AgentScope, DataKeyPurpose, EnvironmentKeyProvider, EventWriter, InboundAdmission, Store,
    SuffixRecovery, WrappingKey, decode_hex_key,
};
use store::{
    ArtifactLifecycleBroker, DirectArtifactBroker, HttpKmsClient, KeyProvider, KmsClient,
    KmsKeyProvider, LifecycleWorker, MockKmsClient, SqliteTombstoneRepository, TombstoneRepository,
};
use tools::executor::ArtifactBroker;

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
    let key_provider = build_key_provider()?;
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
    let lifecycle_worker = build_lifecycle_worker(&config, store.clone()).await?;
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

        // Lifecycle commands reset/delete/export/search/rotate keys are handled
        // at the runtime boundary before any T17 composition or tool execution.
        if let InboundCommand::Valid(command) = &inbound
            && is_lifecycle_command(&command.command)
        {
            let result = lifecycle_worker
                .handle_command(&command.command)
                .await
                .with_context(|| {
                    format!("lifecycle command {} failed", command.command_id.as_str())
                })?;

            lifecycle_worker
                .apply_command(command.command_id.as_str(), command.seq)
                .await?;

            gateway_writer
                .send(OutboundFrame::CommandAck {
                    ack: CommandAck {
                        seq: command.seq,
                        command_id: command.command_id.as_str().to_owned(),
                        status: CommandAckStatus::Applied,
                        reject_reason: None,
                    },
                })
                .await?;

            if let Some(payload) = result {
                let event = serde_json::json!({
                    "type": "lifecycle_result",
                    "command": command.command_id.as_str(),
                    "data": String::from_utf8_lossy(&payload),
                });
                gateway_writer
                    .send(OutboundFrame::Event {
                        envelope: Envelope {
                            seq: Some(command.seq),
                            conversation_id: conversation_id.clone(),
                            event,
                        },
                    })
                    .await?;
            }

            continue;
        }

        gateway_writer
            .send(OutboundFrame::CommandAck { ack: receipt_ack })
            .await?;
    }

    tracing::info!("sumi-agent stopped");
    Ok(())
}

/// Select the `KeyProvider` implementation based on `SUMI_KEY_PROVIDER`.
///
/// * `env`  -> environment-variable wrapping key (local/test only).
/// * `mock` -> fail-closed `MockKmsClient` wrapping a local KEK.
/// * `kms`  -> production `HttpKmsClient` over TLS against `SUMI_KMS_URL`.
fn build_key_provider() -> Result<Arc<dyn KeyProvider>> {
    let provider = env::var("SUMI_KEY_PROVIDER").unwrap_or_else(|_| "env".to_owned());
    match provider.as_str() {
        "env" => {
            let key_id = env::var("SUMI_AGENT_WRAPPING_KEY_ID")
                .unwrap_or_else(|_| "local-env-wrapping-key/v1".to_owned());
            Ok(Arc::new(EnvironmentKeyProvider::from_env(
                "SUMI_AGENT_WRAPPING_KEY",
                key_id,
            )?))
        }
        "mock" => {
            let kek_hex = env::var("SUMI_MOCK_KMS_KEK")
                .context("SUMI_MOCK_KMS_KEK is required when SUMI_KEY_PROVIDER=mock")?;
            let kek_bytes = decode_hex_key(&kek_hex)?;
            let kek = WrappingKey::new("mock-tenant-kek/v1", kek_bytes);
            let client = Arc::new(MockKmsClient::new(
                env::var("SUMI_TENANT_ID").unwrap_or_else(|_| "local-tenant".to_owned()),
                env::var("SUMI_AGENT_ID").unwrap_or_else(|_| "local-agent".to_owned()),
                kek,
            ));
            let agent_key_id = env::var("SUMI_MOCK_KMS_AGENT_KEY_ID")
                .unwrap_or_else(|_| "mock-agent-key/v1".to_owned());
            let agent_key_bytes = env::var("SUMI_MOCK_KMS_AGENT_KEY")
                .context("SUMI_MOCK_KMS_AGENT_KEY is required when SUMI_KEY_PROVIDER=mock")
                .and_then(|hex| decode_hex_key(&hex))?;
            let agent_key = WrappingKey::new(&agent_key_id, agent_key_bytes);
            client
                .register_agent_key(&agent_key_id, &agent_key)
                .context("failed to register mock agent key")?;
            client.set_current_key_id(&agent_key_id);
            let kms_client: Arc<dyn KmsClient> = client;
            Ok(Arc::new(KmsKeyProvider::new(kms_client)?))
        }
        "kms" => {
            let client =
                Arc::new(HttpKmsClient::from_env().context(
                    "SUMI_KEY_PROVIDER=kms requires SUMI_KMS_URL and SUMI_KMS_API_TOKEN",
                )?);
            Ok(Arc::new(KmsKeyProvider::new(client)?))
        }
        other => {
            bail!("unknown SUMI_KEY_PROVIDER={other}; expected `env`, `mock`, or `kms`");
        }
    }
}

/// Build a `LifecycleWorker` bound to a sidecar compliance DB and the local
/// artifact volume.  The default artifact root is `<workspace>/artifacts`.
async fn build_lifecycle_worker(
    config: &config::Config,
    store: Arc<Store>,
) -> Result<LifecycleWorker> {
    let compliance_path = config.database_path.with_file_name("compliance.db");
    let tombstones: Arc<dyn TombstoneRepository> = Arc::new(
        SqliteTombstoneRepository::open(&compliance_path)
            .await
            .context("failed to open lifecycle compliance database")?,
    );

    let mut artifact_root = env::var_os("SUMI_ARTIFACT_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| config.workspace.join("artifacts"));
    if artifact_root.is_relative() {
        artifact_root = std::env::current_dir()?.join(artifact_root);
    }
    tokio::fs::create_dir_all(&artifact_root).await.ok();

    let broker: Arc<dyn ArtifactLifecycleBroker> = Arc::new(DirectArtifactBroker::new(
        ArtifactBroker::open(&artifact_root)?,
    ));
    Ok(LifecycleWorker::new(
        store,
        tombstones,
        broker,
        Some(artifact_root),
    ))
}

fn is_lifecycle_command(command: &gateway::Command) -> bool {
    matches!(
        command,
        gateway::Command::ConversationReset { .. }
            | gateway::Command::DeleteAgent {}
            | gateway::Command::Export { .. }
            | gateway::Command::Search { .. }
            | gateway::Command::RotateKeys {}
    )
}
