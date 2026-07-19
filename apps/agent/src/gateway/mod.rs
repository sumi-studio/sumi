//! Connections between an agent session and its external command/event transport.

mod stdio;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

pub use stdio::{InvalidCommand, StdioGateway};

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    UserMessage {
        text: String,
        #[serde(default, deserialize_with = "deserialize_empty_attachments")]
        attachments: Vec<Attachment>,
    },
    Abort,
    ApprovalDecision {
        request_id: String,
        decision: serde_json::Value,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct Attachment(pub serde_json::Value);

fn deserialize_empty_attachments<'de, D>(deserializer: D) -> Result<Vec<Attachment>, D::Error>
where
    D: Deserializer<'de>,
{
    let attachments = Vec::<Attachment>::deserialize(deserializer)?;
    if attachments.is_empty() {
        Ok(attachments)
    } else {
        Err(de::Error::custom("attachments must be empty"))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct CommandEnvelope {
    pub seq: u64,
    pub command_id: String,
    pub command: Command,
}

#[derive(Clone, Debug, PartialEq)]
pub enum InboundCommand {
    Valid(CommandEnvelope),
    Invalid {
        seq: u64,
        command_id: String,
        reason: CommandRejectReason,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandRejectReason {
    UnknownCommand,
    SchemaViolation,
    AttachmentsNotEmpty,
    Oversized { actual_bytes: u64 },
}

impl CommandRejectReason {
    pub fn code(&self) -> &'static str {
        match self {
            Self::UnknownCommand => "unknown_command",
            Self::SchemaViolation => "schema_violation",
            Self::AttachmentsNotEmpty => "attachments_not_empty",
            Self::Oversized { .. } => "oversized",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CommandAckStatus {
    #[expect(dead_code, reason = "emitted after durable receipt in T11")]
    Received,
    Applied,
    #[expect(dead_code, reason = "emitted by abort handling in T11")]
    Superseded,
    Rejected,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct CommandAck {
    pub seq: u64,
    pub command_id: String,
    pub status: CommandAckStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reject_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct Envelope {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    pub conversation_id: String,
    pub event: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "frame_type", rename_all = "snake_case")]
pub enum OutboundFrame {
    Event { envelope: Envelope },
    CommandAck { ack: CommandAck },
}

#[derive(Debug, Error)]
#[error("gateway input closed")]
pub struct GatewayClosed;

#[async_trait]
pub trait Gateway: Send {
    async fn next_command(&mut self) -> Result<InboundCommand>;
    async fn send(&mut self, frame: OutboundFrame) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn user_message_rejects_non_empty_attachments() {
        let error = serde_json::from_value::<Command>(json!({
            "type": "user_message",
            "text": "inspect this",
            "attachments": [{"name": "secret.txt"}]
        }))
        .expect_err("attachments are not supported");

        assert!(error.to_string().contains("attachments must be empty"));
    }

    #[test]
    fn transient_envelope_omits_null_sequence() {
        let encoded = serde_json::to_value(OutboundFrame::Event {
            envelope: Envelope {
                seq: None,
                conversation_id: "conversation-1".to_owned(),
                event: json!({"type": "text_delta"}),
            },
        })
        .expect("serialize envelope");

        assert_eq!(encoded["frame_type"], "event");
        assert_eq!(encoded["envelope"].get("seq"), None);
    }

    #[test]
    fn command_ack_uses_the_contract_wire_shape() {
        let encoded = serde_json::to_value(OutboundFrame::CommandAck {
            ack: CommandAck {
                seq: 7,
                command_id: "command-7".to_owned(),
                status: CommandAckStatus::Rejected,
                reject_reason: Some("oversized".to_owned()),
            },
        })
        .expect("serialize ACK");

        assert_eq!(
            encoded,
            json!({
                "frame_type": "command_ack",
                "ack": {
                    "seq": 7,
                    "command_id": "command-7",
                    "status": "rejected",
                    "reject_reason": "oversized",
                }
            })
        );
    }
}
