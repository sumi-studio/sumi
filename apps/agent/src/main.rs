mod agent;
mod apiclient;
mod approval;
mod config;
mod gateway;
mod memory;
pub mod provider;
mod store;
mod tools;

use std::io;

use anyhow::Result;
use gateway::{Envelope, Gateway, GatewayClosed, StdioGateway};

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

    let config = config::Config::load().await?;
    let conversation_id = config.conversation_id.clone();
    let mut gateway = StdioGateway::new();

    tracing::info!(
        conversation_id,
        workspace = %config.workspace.display(),
        database_path = %config.database_path.display(),
        model_preset = ?config.model.preset,
        model_id = ?config.model.id,
        system_prompt_configured = !config.system_prompt.is_empty(),
        "sumi-agent starting"
    );

    loop {
        let command = match gateway.next_command().await {
            Ok(command) => command,
            Err(error) if error.downcast_ref::<GatewayClosed>().is_some() => break,
            Err(error) => return Err(error),
        };

        if let gateway::Command::UserMessage { text, .. } = command {
            gateway
                .send(Envelope {
                    seq: None,
                    conversation_id: conversation_id.clone(),
                    event: serde_json::json!({
                        "type": "echo",
                        "text": text,
                    }),
                })
                .await?;
        }
    }

    tracing::info!("sumi-agent stopped");
    Ok(())
}
