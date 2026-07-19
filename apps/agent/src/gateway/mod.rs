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

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct Envelope {
    #[serde(skip_serializing_if = "Option::is_none")]
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
        let encoded = serde_json::to_value(Envelope {
            seq: None,
            conversation_id: "conversation-1".to_owned(),
            event: json!({"type": "text_delta"}),
        })
        .expect("serialize envelope");

        assert_eq!(encoded.get("seq"), None);
    }
}
