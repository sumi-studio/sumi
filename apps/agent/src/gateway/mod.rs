//! Connections between an agent session and its external command/event transport.

mod stdio;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use stdio::StdioGateway;

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    UserMessage {
        text: String,
        #[serde(default)]
        attachments: Vec<Attachment>,
    },
    Abort,
    ApprovalDecision {
        request_id: String,
        decision: serde_json::Value,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Attachment {
    pub name: String,
    pub media_type: String,
    pub data: String,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct Envelope {
    pub seq: Option<u64>,
    pub conversation_id: String,
    pub event: serde_json::Value,
}

#[derive(Debug, Error)]
#[error("gateway input closed")]
pub struct GatewayClosed;

#[async_trait]
pub trait Gateway: Send {
    async fn next_command(&mut self) -> Result<Command>;
    async fn send(&mut self, envelope: Envelope) -> Result<()>;
}
