//! Connections between an agent session and its external command/event transport.

#[allow(dead_code)]
pub mod wire;

pub mod stdio;
pub mod supervisor;
pub mod ws;

/// Maximum size in bytes of a single gateway frame/message.
pub(crate) const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

use std::fmt;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;
use uuid::Uuid;
use zeroize::Zeroize;

#[allow(
    unused_imports,
    reason = "T15 injected loop harness; T26 constructs production IO"
)]
pub(crate) use stdio::{InjectedStdioGateway, read_command};

pub use supervisor::{AgentHello, ApiHello, ConnectorError, GatewayConnector, GatewayCredential};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    UserMessage {
        text: String,
        #[serde(deserialize_with = "deserialize_empty_attachments")]
        attachments: Vec<Attachment>,
    },
    Abort {},
    ApprovalDecision {
        request_id: String,
        decision: ApprovalDecision,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ApprovalDecision {
    ApproveOnce,
    /// T12 authenticates this closed decision shape but cannot apply it: the
    /// durable approval-rule/policy mutation is owned by T22/T23.
    ApproveAlways {
        rule: DeferredApprovalRule,
    },
    Deny,
}

/// Authenticated, uninterpreted T22/T23 input. T12 requires an object so the
/// decision cannot be confused with a scalar/null control value, and never
/// applies or persists a policy from this payload.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(transparent)]
pub struct DeferredApprovalRule(serde_json::Value);

impl<'de> Deserialize<'de> for DeferredApprovalRule {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.is_object() {
            Ok(Self(value))
        } else {
            Err(de::Error::custom("approval rule must be an object"))
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
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
#[serde(deny_unknown_fields)]
pub struct CommandEnvelope {
    pub seq: u64,
    pub command_id: CommandId,
    pub command: Command,
}

/// Canonical external command identity. Only lower-case hyphenated UUID text is
/// accepted so one UUID cannot acquire multiple durable identities.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CommandId {
    value: Uuid,
    canonical: String,
}

impl CommandId {
    pub fn parse(value: &str) -> std::result::Result<Self, &'static str> {
        let uuid = Uuid::parse_str(value).map_err(|_| "command_id is not a UUID")?;
        let canonical = uuid.hyphenated().to_string();
        if value != canonical {
            return Err("command_id is not in canonical lower-case hyphenated form");
        }
        Ok(Self {
            value: uuid,
            canonical,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.value
    }
}

impl fmt::Display for CommandId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for CommandId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CommandId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(|_| de::Error::custom("command_id must be a canonical UUID"))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum InboundCommand {
    Valid(CommandEnvelope),
    Invalid {
        seq: u64,
        command_id: CommandId,
        reason: CommandRejectReason,
        /// Transient bytes used only to authenticate the durable receipt. The
        /// EventWriter encrypts valid-size rejects and discards oversized bytes
        /// after computing the conversation-keyed digest.
        raw_command: RejectedCommandPayload,
        /// Present only when the size-limit reader discarded an oversized
        /// command value after incrementally authenticating its exact raw bytes.
        payload_digest: Option<KeyedCommandDigest>,
    },
}

pub(crate) const MISSING_COMMAND_PAYLOAD: &[u8] = b"\0sumi/inbound-command/missing-field/v1";

#[derive(Clone, PartialEq, Eq)]
pub enum RejectedCommandPayload {
    Present(SensitiveCommandPayload),
    Missing,
    DiscardedOversized,
}

impl RejectedCommandPayload {
    pub(crate) fn authenticated_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Present(payload) => Some(payload.as_bytes()),
            Self::Missing => Some(MISSING_COMMAND_PAYLOAD),
            Self::DiscardedOversized => None,
        }
    }
}

impl fmt::Debug for RejectedCommandPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Present(payload) => formatter.debug_tuple("Present").field(payload).finish(),
            Self::Missing => formatter.write_str("Missing"),
            Self::DiscardedOversized => formatter.write_str("DiscardedOversized"),
        }
    }
}

pub(crate) trait CommandDigestFactory: Send + Sync {
    fn start(&self) -> Box<dyn IncrementalCommandDigest>;
}

pub(crate) trait IncrementalCommandDigest: Send {
    fn update(&mut self, bytes: &[u8]);
    fn finish(self: Box<Self>) -> KeyedCommandDigest;
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct KeyedCommandDigest {
    key_ref: String,
    hmac: [u8; 32],
}

impl KeyedCommandDigest {
    pub(crate) fn new(key_ref: impl Into<String>, hmac: [u8; 32]) -> Self {
        Self {
            key_ref: key_ref.into(),
            hmac,
        }
    }

    pub(crate) fn key_ref(&self) -> &str {
        &self.key_ref
    }

    pub(crate) fn hmac(&self) -> &[u8; 32] {
        &self.hmac
    }
}

impl fmt::Debug for KeyedCommandDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KeyedCommandDigest")
            .field("key_ref", &self.key_ref)
            .field("hmac", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SensitiveCommandPayload(Vec<u8>);

impl SensitiveCommandPayload {
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SensitiveCommandPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SensitiveCommandPayload")
            .field(&format_args!("[REDACTED {} bytes]", self.0.len()))
            .finish()
    }
}

impl Drop for SensitiveCommandPayload {
    fn drop(&mut self) {
        self.0.zeroize();
    }
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
    Received,
    Applied,
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
pub trait GatewayReader: Send {
    async fn next_command(&mut self) -> Result<InboundCommand>;
}

#[async_trait]
pub trait GatewayWriter: Send {
    async fn send(&mut self, frame: OutboundFrame) -> Result<()>;
}

/// An established transport that can transfer each half to its sole owner.
/// Neither half is shared behind a mutex: the Session owns the reader and a
/// dedicated task owns the writer.
#[async_trait]
pub trait Gateway: Send + 'static {
    type Reader: GatewayReader + 'static;
    type Writer: GatewayWriter + 'static;

    async fn authenticate_hello(&mut self, hello: AgentHello) -> Result<ApiHello>;
    fn split(self) -> (Self::Reader, Self::Writer);
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
    fn user_message_requires_attachments_field() {
        let error = serde_json::from_value::<Command>(json!({
            "type": "user_message",
            "text": "inspect this"
        }))
        .expect_err("attachments must be present even while only empty arrays are supported");

        assert!(error.to_string().contains("missing field `attachments`"));
    }

    #[test]
    fn abort_rejects_unknown_fields() {
        let error = serde_json::from_value::<Command>(json!({
            "type": "abort",
            "extra": true
        }))
        .expect_err("abort has no payload fields");

        assert!(error.to_string().contains("unknown field `extra`"));
    }

    #[test]
    fn approval_decision_rejects_untyped_values() {
        let error = serde_json::from_value::<Command>(json!({
            "type": "approval_decision",
            "request_id": "request-1",
            "decision": {"totally_unknown": true}
        }))
        .expect_err("approval decisions require their typed wire contract");

        assert!(error.to_string().contains("missing field `type`"));

        for decision in [
            json!({"type":"approve_once"}),
            json!({"type":"deny"}),
            json!({
                "type":"approve_always",
                "rule":{"tool_name":"test","literal_prefix":["test"]}
            }),
        ] {
            serde_json::from_value::<Command>(json!({
                "type":"approval_decision",
                "request_id":"request-1",
                "decision":decision
            }))
            .expect("closed typed approval decision");
        }
        let scalar_rule = serde_json::from_value::<Command>(json!({
            "type":"approval_decision",
            "request_id":"request-1",
            "decision":{"type":"approve_always","rule":"test"}
        }))
        .expect_err("deferred rule must retain an object boundary");
        assert!(
            scalar_rule
                .to_string()
                .contains("approval rule must be an object")
        );
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
                command_id: "00000000-0000-4000-8000-000000000007".to_owned(),
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
                    "command_id": "00000000-0000-4000-8000-000000000007",
                    "status": "rejected",
                    "reject_reason": "oversized",
                }
            })
        );
    }

    #[test]
    fn sensitive_command_payload_debug_never_reveals_bytes() {
        let secret = "sk-abcdefghijklmnop";
        let payload = SensitiveCommandPayload::new(secret.as_bytes().to_vec());
        let diagnostic = format!("{payload:?}");
        assert!(!diagnostic.contains(secret));
        assert!(diagnostic.contains("[REDACTED"));
    }
}
