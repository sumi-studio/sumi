//! T18 wire DTOs and explicit conversions.
//!
//! This module owns the public JSON contract for agent events, commands,
//! command acknowledgements, and outbound frames. The internal `serde`
//! representations in `crate::agent` and `crate::provider` are intentionally
//! *not* the public contract; every wire shape is declared here and converted
//! explicitly.
//!
//! Two T17 dependencies are deliberately left narrow so they can be updated
//! after T17 without a compatibility break:
//!
//! * `MemoryMaintenance.kind` is a plain string in this draft.
//! * Provider-context payload vocabulary is not in the current event envelope
//!   surface; the opaque values inside `provider/types.rs` are not wired to
//!   the public contract yet.

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

use super::{
    Command, CommandAck, CommandAckStatus, CommandEnvelope, CommandId, Envelope, OutboundFrame,
};
use crate::agent::{AgentEvent, PublicStreamEvent};
use crate::provider::types::PublicMessage;

/// UUIDv5 namespace for deriving a user `message_id` from a canonical
/// `command_id`. API and web consumers must use the same namespace so they can
/// compute the same `message_id` at command admission time.
pub const USER_MESSAGE_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x78, 0xf6, 0x2d, 0x15, 0xb9, 0x45, 0x4a, 0x4f, 0x9d, 0x84, 0xd7, 0x3c, 0x7f, 0x93, 0x2b, 0x51,
]);

/// Errors that can occur while converting an internal value to its wire form.
#[derive(Debug, Error)]
pub enum WireError {
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("durable event `{event_type}` requires seq")]
    SeqRequired { event_type: &'static str },
    #[error("volatile event `{event_type}` forbids seq")]
    SeqForbidden { event_type: &'static str },
    #[error("command_ack with status `{status}` requires reject_reason")]
    RejectReasonRequired { status: String },
    #[error("command_ack with status `{status}` must not have reject_reason")]
    UnexpectedRejectReason { status: String },
    #[error("invalid reject_reason `{reason}`")]
    InvalidRejectReason { reason: String },
    #[error("command_id `{0}` is not a canonical UUID")]
    InvalidCommandId(String),
}

/// Derive a durable user `message_id` from a canonical `command_id` using the
/// UUIDv5 namespace defined in this contract.
pub fn user_message_id_from_command_id(command_id: &str) -> Result<String, WireError> {
    let id = CommandId::parse(command_id)
        .map_err(|_| WireError::InvalidCommandId(command_id.to_owned()))?;
    Ok(Uuid::new_v5(&USER_MESSAGE_ID_NAMESPACE, id.as_uuid().as_bytes()).to_string())
}

/// Convert an outbound frame to its explicit wire DTO, validating the seq and
/// command-ack rules from the contract.
pub fn to_wire_frame(frame: OutboundFrame) -> Result<WireOutboundFrame, WireError> {
    frame.try_into()
}

/// Attachment placeholder. v1 accepts only an empty array.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WireAttachment(Value);

fn deserialize_empty_attachments<'de, D>(deserializer: D) -> Result<Vec<WireAttachment>, D::Error>
where
    D: Deserializer<'de>,
{
    let attachments = Vec::<WireAttachment>::deserialize(deserializer)?;
    if attachments.is_empty() {
        Ok(attachments)
    } else {
        Err(serde::de::Error::custom("attachments must be empty"))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Commands and ACKs
// ═══════════════════════════════════════════════════════════════════════════

/// Public command wire DTO.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WireCommand {
    UserMessage {
        text: String,
        #[serde(deserialize_with = "deserialize_empty_attachments")]
        attachments: Vec<WireAttachment>,
    },
    Abort,
    ApprovalDecision {
        request_id: String,
        decision: WireApprovalDecision,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireCommandEnvelope {
    pub seq: u64,
    pub command_id: String,
    pub command: WireCommand,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireCommandAckStatus {
    Received,
    Applied,
    Superseded,
    Rejected,
}

impl WireCommandAckStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Applied => "applied",
            Self::Superseded => "superseded",
            Self::Rejected => "rejected",
        }
    }
}

impl From<CommandAckStatus> for WireCommandAckStatus {
    fn from(status: CommandAckStatus) -> Self {
        match status {
            CommandAckStatus::Received => Self::Received,
            CommandAckStatus::Applied => Self::Applied,
            CommandAckStatus::Superseded => Self::Superseded,
            CommandAckStatus::Rejected => Self::Rejected,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireRejectReason {
    UnknownCommand,
    SchemaViolation,
    AttachmentsNotEmpty,
    Oversized,
}

fn parse_reject_reason(reason: &str) -> Result<WireRejectReason, WireError> {
    match reason {
        "unknown_command" => Ok(WireRejectReason::UnknownCommand),
        "schema_violation" => Ok(WireRejectReason::SchemaViolation),
        "attachments_not_empty" => Ok(WireRejectReason::AttachmentsNotEmpty),
        "oversized" => Ok(WireRejectReason::Oversized),
        _ => Err(WireError::InvalidRejectReason {
            reason: reason.to_owned(),
        }),
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "WireCommandAckInput")]
pub struct WireCommandAck {
    pub seq: u64,
    pub command_id: String,
    pub status: WireCommandAckStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reject_reason: Option<WireRejectReason>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireCommandAckInput {
    seq: u64,
    command_id: String,
    status: WireCommandAckStatus,
    reject_reason: Option<WireRejectReason>,
}

impl TryFrom<WireCommandAckInput> for WireCommandAck {
    type Error = WireError;
    fn try_from(input: WireCommandAckInput) -> Result<Self, WireError> {
        let reject_reason = match input.status {
            WireCommandAckStatus::Rejected => {
                Some(
                    input
                        .reject_reason
                        .ok_or_else(|| WireError::RejectReasonRequired {
                            status: input.status.as_str().to_owned(),
                        })?,
                )
            }
            _ => {
                if input.reject_reason.is_some() {
                    return Err(WireError::UnexpectedRejectReason {
                        status: input.status.as_str().to_owned(),
                    });
                }
                None
            }
        };
        Ok(Self {
            seq: input.seq,
            command_id: input.command_id,
            status: input.status,
            reject_reason,
        })
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Outbound frames and envelope
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "WireEnvelopeInput")]
pub struct WireEnvelope {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    pub conversation_id: String,
    pub event: WireAgentEvent,
}

impl WireEnvelope {
    /// Build a wire envelope from an internal agent event, enforcing the
    /// durable-seq-required / volatile-seq-forbidden rule.
    pub(crate) fn try_new(
        seq: Option<u64>,
        conversation_id: String,
        event: AgentEvent,
    ) -> Result<Self, WireError> {
        let event: WireAgentEvent = event.try_into()?;
        validate_seq(seq, &event)?;
        Ok(Self {
            seq,
            conversation_id,
            event,
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireEnvelopeInput {
    seq: Option<u64>,
    conversation_id: String,
    event: WireAgentEvent,
}

impl TryFrom<WireEnvelopeInput> for WireEnvelope {
    type Error = WireError;
    fn try_from(input: WireEnvelopeInput) -> Result<Self, WireError> {
        validate_seq(input.seq, &input.event)?;
        Ok(Self {
            seq: input.seq,
            conversation_id: input.conversation_id,
            event: input.event,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "frame_type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WireOutboundFrame {
    Event { envelope: WireEnvelope },
    CommandAck { ack: WireCommandAck },
}

// ═══════════════════════════════════════════════════════════════════════════
// Agent events
// ═══════════════════════════════════════════════════════════════════════════

/// Public agent event wire DTO. This is the contract surface for
/// `Envelope.event`; it matches the JSON Schema in `contracts/agent-events.yaml`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WireAgentEvent {
    AgentStart,
    AgentEnd,
    TurnStart,
    TurnEnd {
        message: Option<Box<WirePublicMessage>>,
        tool_results: Vec<WireToolResultMessage>,
    },
    MessageStart {
        message_id: String,
        message: Box<WirePublicMessage>,
    },
    MessageUpdate {
        message_id: String,
        event: WirePublicStreamEvent,
    },
    MessageEnd {
        message_id: String,
        message: Box<WirePublicMessage>,
    },
    ToolExecutionStart {
        tool_call_id: String,
        tool_name: String,
        args: Value,
    },
    ToolExecutionUpdate {
        tool_call_id: String,
        partial: Value,
    },
    ToolExecutionEnd {
        tool_call_id: String,
        result: Value,
        is_error: bool,
    },
    ApprovalRequested {
        request: WireApprovalRequest,
    },
    ApprovalResolved {
        request_id: String,
        resolution: WireApprovalResolution,
    },
    Steered {
        mode: WireSteerMode,
    },
    MemoryMaintenance {
        kind: WireMemoryMaintKind,
    },
    RetryScheduled {
        attempt: u32,
        delay_ms: u64,
        retry_at: String,
        error_message: String,
    },
    Error {
        message: String,
    },
}

impl WireAgentEvent {
    fn is_volatile(&self) -> bool {
        matches!(
            self,
            Self::MessageUpdate { .. } | Self::ToolExecutionUpdate { .. } | Self::Error { .. }
        )
    }

    fn event_type(&self) -> &'static str {
        match self {
            Self::AgentStart => "agent_start",
            Self::AgentEnd => "agent_end",
            Self::TurnStart => "turn_start",
            Self::TurnEnd { .. } => "turn_end",
            Self::MessageStart { .. } => "message_start",
            Self::MessageUpdate { .. } => "message_update",
            Self::MessageEnd { .. } => "message_end",
            Self::ToolExecutionStart { .. } => "tool_execution_start",
            Self::ToolExecutionUpdate { .. } => "tool_execution_update",
            Self::ToolExecutionEnd { .. } => "tool_execution_end",
            Self::ApprovalRequested { .. } => "approval_requested",
            Self::ApprovalResolved { .. } => "approval_resolved",
            Self::Steered { .. } => "steered",
            Self::MemoryMaintenance { .. } => "memory_maintenance",
            Self::RetryScheduled { .. } => "retry_scheduled",
            Self::Error { .. } => "error",
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Public stream events
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WirePublicStreamEvent {
    TextStart {
        content_index: usize,
    },
    TextDelta {
        content_index: usize,
        delta: String,
    },
    TextEnd {
        content_index: usize,
        content: String,
    },
    ThinkingStart {
        content_index: usize,
    },
    ThinkingDelta {
        content_index: usize,
        delta: String,
    },
    ThinkingEnd {
        content_index: usize,
        content: String,
    },
    ToolCallStart {
        content_index: usize,
    },
    ToolCallDelta {
        content_index: usize,
        delta: String,
    },
    ToolCallPreview {
        content_index: usize,
        preview: Value,
    },
    ToolCallEnd {
        content_index: usize,
        tool_call: WireToolCall,
    },
    ToolCallRejected {
        content_index: usize,
        rejected: WireRejectedToolCall,
    },
    ReasoningSummaryStart {
        content_index: usize,
    },
    ReasoningSummaryDelta {
        content_index: usize,
        delta: String,
    },
    ReasoningSummaryEnd {
        content_index: usize,
        content: String,
    },
}

// ═══════════════════════════════════════════════════════════════════════════
// Messages and content blocks
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case", deny_unknown_fields)]
pub enum WirePublicMessage {
    User {
        content: Vec<WireUserContent>,
        timestamp: String,
    },
    Assistant {
        content: Vec<WirePublicAssistantContent>,
        model: String,
        provider: String,
        origin: WireProviderOrigin,
        usage: WireUsage,
        stop_reason: WireStopReason,
        error_message: Option<String>,
        provider_code: Option<String>,
        interrupted: bool,
        timestamp: String,
    },
    ToolResult {
        tool_call_id: String,
        tool_name: String,
        content: Vec<WireUserContent>,
        details: Value,
        is_error: bool,
        timestamp: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WireUserContent {
    Text { text: String },
    Image { data: String, mime_type: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WirePublicAssistantContent {
    Text {
        text: String,
        wire_item_index: u32,
    },
    Thinking {
        thinking: String,
        signature_field: String,
        wire_item_index: u32,
    },
    ToolCall {
        tool_call: WireToolCall,
        wire_item_index: u32,
    },
    RejectedToolCall {
        rejected: WireRejectedToolCall,
        wire_item_index: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireRejectedToolCall {
    pub id: String,
    pub name: String,
    pub error: WireToolArgumentError,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireToolArgumentError {
    InvalidJson,
    NonObject,
    SchemaViolation,
    IncompleteResponse,
    TooLarge,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireProviderOrigin {
    pub provider_instance_id: String,
    pub protocol: WireApiProtocol,
    pub model: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireApiProtocol {
    OpenAiChatCompletions,
    OpenAiResponses,
    AnthropicMessages,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireUsage {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub reasoning: u64,
    pub total_tokens: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireStopReason {
    Stop,
    Length,
    ToolUse,
    Error,
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireToolResultMessage {
    pub tool_call_id: String,
    pub tool_name: String,
    pub content: Vec<WireUserContent>,
    pub details: Value,
    pub is_error: bool,
    pub timestamp: String,
}

// ═══════════════════════════════════════════════════════════════════════════
// Approval and steering
// ═══════════════════════════════════════════════════════════════════════════

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireApprovalRequest {
    pub id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub action: WireReviewProjection,
    pub args_summary: Value,
    pub reason: Option<String>,
    pub audit: Option<WireAuditDecision>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WireReviewProjection {
    Reviewable(Value),
    InsufficientEvidence { reason: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireAuditDecision {
    pub outcome: WireAuditOutcome,
    pub risk: WireRiskLevel,
    pub authorization: WireUserAuthorization,
    pub rationale: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireAuditOutcome {
    Allow,
    Deny,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireRiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireUserAuthorization {
    Unknown,
    Low,
    Medium,
    High,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum WireApprovalResolution {
    Decision(WireApprovalDecision),
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WireApprovalDecision {
    ApproveOnce,
    ApproveAlways { rule: Map<String, Value> },
    Deny,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireSteerMode {
    Hard,
    Soft,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WireMemoryMaintKind(String);

// ═══════════════════════════════════════════════════════════════════════════
// Conversions
// ═══════════════════════════════════════════════════════════════════════════

impl TryFrom<AgentEvent> for WireAgentEvent {
    type Error = WireError;
    fn try_from(event: AgentEvent) -> Result<Self, WireError> {
        serde_json::from_value(serde_json::to_value(&event)?).map_err(WireError::Json)
    }
}

impl TryFrom<PublicStreamEvent> for WirePublicStreamEvent {
    type Error = WireError;
    fn try_from(event: PublicStreamEvent) -> Result<Self, WireError> {
        serde_json::from_value(serde_json::to_value(&event)?).map_err(WireError::Json)
    }
}

impl TryFrom<PublicMessage> for WirePublicMessage {
    type Error = WireError;
    fn try_from(message: PublicMessage) -> Result<Self, WireError> {
        serde_json::from_value(serde_json::to_value(&message)?).map_err(WireError::Json)
    }
}

impl TryFrom<Command> for WireCommand {
    type Error = WireError;
    fn try_from(command: Command) -> Result<Self, WireError> {
        serde_json::from_value(serde_json::to_value(&command)?).map_err(WireError::Json)
    }
}

impl TryFrom<CommandEnvelope> for WireCommandEnvelope {
    type Error = WireError;
    fn try_from(envelope: CommandEnvelope) -> Result<Self, WireError> {
        Ok(Self {
            seq: envelope.seq,
            command_id: envelope.command_id.as_str().to_owned(),
            command: envelope.command.try_into()?,
        })
    }
}

impl TryFrom<CommandAck> for WireCommandAck {
    type Error = WireError;
    fn try_from(ack: CommandAck) -> Result<Self, WireError> {
        let status = WireCommandAckStatus::from(ack.status);
        let reject_reason = match status {
            WireCommandAckStatus::Rejected => {
                let reason = ack.reject_reason.as_deref().ok_or_else(|| {
                    WireError::RejectReasonRequired {
                        status: status.as_str().to_owned(),
                    }
                })?;
                Some(parse_reject_reason(reason)?)
            }
            _ => {
                if ack.reject_reason.is_some() {
                    return Err(WireError::UnexpectedRejectReason {
                        status: status.as_str().to_owned(),
                    });
                }
                None
            }
        };
        Ok(Self {
            seq: ack.seq,
            command_id: ack.command_id,
            status,
            reject_reason,
        })
    }
}

impl TryFrom<Envelope> for WireEnvelope {
    type Error = WireError;
    fn try_from(envelope: Envelope) -> Result<Self, WireError> {
        let event: WireAgentEvent = serde_json::from_value(envelope.event)?;
        validate_seq(envelope.seq, &event)?;
        Ok(Self {
            seq: envelope.seq,
            conversation_id: envelope.conversation_id,
            event,
        })
    }
}

impl TryFrom<OutboundFrame> for WireOutboundFrame {
    type Error = WireError;
    fn try_from(frame: OutboundFrame) -> Result<Self, WireError> {
        match frame {
            OutboundFrame::Event { envelope } => Ok(Self::Event {
                envelope: envelope.try_into()?,
            }),
            OutboundFrame::CommandAck { ack } => Ok(Self::CommandAck {
                ack: ack.try_into()?,
            }),
        }
    }
}

fn validate_seq(seq: Option<u64>, event: &WireAgentEvent) -> Result<(), WireError> {
    if event.is_volatile() {
        if seq.is_some() {
            return Err(WireError::SeqForbidden {
                event_type: event.event_type(),
            });
        }
    } else if seq.is_none() {
        return Err(WireError::SeqRequired {
            event_type: event.event_type(),
        });
    }
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use serde_json::json;

    use super::*;
    use crate::agent::{
        AgentEvent, ApprovalRequest, ApprovalResolution, PublicStreamEvent, SteerMode,
    };
    use crate::provider::types::{
        ApiProtocol, ProviderOrigin, PublicAssistantContent, PublicAssistantMessage, PublicMessage,
        StopReason, ToolCall, ToolResultMessage, Usage, UserContent, UserMessage,
    };
    use std::path::Path;

    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    fn uuid_command_id() -> CommandId {
        CommandId::parse("00000000-0000-4000-8000-000000000001").expect("canonical UUID")
    }

    fn validated_args() -> crate::provider::types::ValidatedToolArguments {
        serde_json::from_value(json!({"path": "notes.txt"})).expect("object")
    }

    fn tool_call() -> ToolCall {
        ToolCall {
            id: "call-1".to_owned(),
            name: "read_file".to_owned(),
            arguments: validated_args(),
        }
    }

    fn provider_origin() -> ProviderOrigin {
        ProviderOrigin {
            provider_instance_id: "moonshot:https://api.moonshot.ai/v1".to_owned(),
            protocol: ApiProtocol::OpenAiChatCompletions,
            model: "kimi-k3".to_owned(),
        }
    }

    fn usage() -> Usage {
        Usage {
            input: 90,
            output: 12,
            cache_read: 10,
            cache_write: 0,
            reasoning: 4,
            total_tokens: 112,
        }
    }

    #[test]
    fn user_message_id_is_uuidv5_from_canonical_command_id() {
        let command_id = "00000000-0000-4000-8000-000000000001";
        let expected = Uuid::new_v5(
            &USER_MESSAGE_ID_NAMESPACE,
            uuid_command_id().as_uuid().as_bytes(),
        )
        .to_string();
        assert_eq!(
            user_message_id_from_command_id(command_id).unwrap(),
            expected
        );
        assert_ne!(
            user_message_id_from_command_id(command_id).unwrap(),
            command_id
        );
        assert!(user_message_id_from_command_id("not-a-uuid").is_err());
    }

    #[test]
    fn durable_event_requires_seq_and_volatile_forbids_it() {
        let durable = AgentEvent::AgentStart;
        assert!(WireEnvelope::try_new(Some(1), "c".to_owned(), durable).is_ok());
        assert!(WireEnvelope::try_new(None, "c".to_owned(), AgentEvent::AgentStart).is_err());

        let volatile = AgentEvent::Error {
            message: "boom".to_owned(),
        };
        assert!(WireEnvelope::try_new(None, "c".to_owned(), volatile.clone()).is_ok());
        assert!(WireEnvelope::try_new(Some(1), "c".to_owned(), volatile).is_err());

        let update = AgentEvent::MessageUpdate {
            message_id: "00000000-0000-4000-8000-000000000002".to_owned(),
            event: PublicStreamEvent::TextDelta {
                content_index: 0,
                delta: "hi".to_owned(),
            },
        };
        assert!(WireEnvelope::try_new(None, "c".to_owned(), update.clone()).is_ok());
        assert!(WireEnvelope::try_new(Some(1), "c".to_owned(), update).is_err());
    }

    #[test]
    fn command_rejects_non_empty_or_missing_attachments() {
        let valid = Command::UserMessage {
            text: "inspect".to_owned(),
            attachments: vec![],
        };
        assert!(matches!(
            WireCommand::try_from(valid).unwrap(),
            WireCommand::UserMessage { text, .. } if text == "inspect"
        ));

        let non_empty = json!({
            "type": "user_message",
            "text": "inspect",
            "attachments": [{"name": "secret.txt"}]
        });
        let err = serde_json::from_value::<WireCommand>(non_empty).unwrap_err();
        assert!(err.to_string().contains("attachments must be empty"));

        let missing = json!({"type": "user_message", "text": "inspect"});
        assert!(serde_json::from_value::<WireCommand>(missing).is_err());
    }

    #[test]
    fn command_envelope_round_trips() {
        let envelope = CommandEnvelope {
            seq: 7,
            command_id: uuid_command_id(),
            command: Command::UserMessage {
                text: "hi".to_owned(),
                attachments: vec![],
            },
        };
        let wire = WireCommandEnvelope::try_from(envelope).unwrap();
        let json = serde_json::to_string(&wire).unwrap();
        let back: WireCommandEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(wire, back);
        assert_eq!(back.command_id, "00000000-0000-4000-8000-000000000001");
    }

    #[test]
    fn command_ack_reject_reason_rules() {
        let rejected = CommandAck {
            seq: 1,
            command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            status: CommandAckStatus::Rejected,
            reject_reason: Some("oversized".to_owned()),
        };
        assert!(to_wire_frame(OutboundFrame::CommandAck { ack: rejected }).is_ok());

        let rejected_missing = CommandAck {
            seq: 1,
            command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            status: CommandAckStatus::Rejected,
            reject_reason: None,
        };
        assert!(
            to_wire_frame(OutboundFrame::CommandAck {
                ack: rejected_missing
            })
            .is_err()
        );

        let received_with_reason = CommandAck {
            seq: 2,
            command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            status: CommandAckStatus::Received,
            reject_reason: Some("oversized".to_owned()),
        };
        assert!(
            to_wire_frame(OutboundFrame::CommandAck {
                ack: received_with_reason
            })
            .is_err()
        );
    }

    #[test]
    fn outbound_event_frame_enforces_seq_rules() {
        let durable = OutboundFrame::Event {
            envelope: Envelope {
                seq: Some(1),
                conversation_id: "conv-1".to_owned(),
                event: json!({"type": "agent_start"}),
            },
        };
        let wire = to_wire_frame(durable).unwrap();
        assert!(matches!(wire, WireOutboundFrame::Event { envelope } if envelope.seq == Some(1)));

        let durable_missing_seq = OutboundFrame::Event {
            envelope: Envelope {
                seq: None,
                conversation_id: "conv-1".to_owned(),
                event: json!({"type": "agent_start"}),
            },
        };
        assert!(to_wire_frame(durable_missing_seq).is_err());

        let volatile_with_seq = OutboundFrame::Event {
            envelope: Envelope {
                seq: Some(1),
                conversation_id: "conv-1".to_owned(),
                event: json!({"type": "error", "message": "x"}),
            },
        };
        assert!(to_wire_frame(volatile_with_seq).is_err());

        let volatile_ok = OutboundFrame::Event {
            envelope: Envelope {
                seq: None,
                conversation_id: "conv-1".to_owned(),
                event: json!({"type": "error", "message": "x"}),
            },
        };
        assert!(to_wire_frame(volatile_ok).is_ok());
    }

    #[test]
    fn agent_event_round_trips() {
        let user_message = PublicMessage::User(UserMessage {
            content: vec![UserContent::Text {
                text: "hello".to_owned(),
            }],
            timestamp: now(),
        });

        let start = AgentEvent::MessageStart {
            message_id: "00000000-0000-4000-8000-000000000002".to_owned(),
            message: Box::new(user_message),
        };
        round_trip_agent_event(start);

        let update = AgentEvent::MessageUpdate {
            message_id: "00000000-0000-4000-8000-000000000002".to_owned(),
            event: PublicStreamEvent::TextDelta {
                content_index: 1,
                delta: "world".to_owned(),
            },
        };
        round_trip_agent_event(update);

        let tool_end = AgentEvent::ToolExecutionEnd {
            tool_call_id: "call-1".to_owned(),
            result: json!({"ok": true}),
            is_error: false,
        };
        round_trip_agent_event(tool_end);

        let approval: ApprovalRequest = serde_json::from_value(json!({
            "id": "req-1",
            "tool_call_id": "call-1",
            "tool_name": "bash",
            "action": {"reviewable": {"cmd": "ls"}},
            "args_summary": {"cmd": "ls"},
            "reason": null,
            "audit": null
        }))
        .unwrap();
        round_trip_agent_event(AgentEvent::ApprovalRequested { request: approval });

        let resolved: ApprovalResolution =
            serde_json::from_value(json!({"decision": {"type": "approve_once"}})).unwrap();
        round_trip_agent_event(AgentEvent::ApprovalResolved {
            request_id: "req-1".to_owned(),
            resolution: resolved,
        });

        round_trip_agent_event(AgentEvent::Steered {
            mode: SteerMode::Hard,
        });

        let memory: AgentEvent =
            serde_json::from_value(json!({"type": "memory_maintenance", "kind": "compact"}))
                .unwrap();
        round_trip_agent_event(memory);

        let retry = AgentEvent::RetryScheduled {
            attempt: 1,
            delay_ms: 2000,
            retry_at: now(),
            error_message: "rate limited".to_owned(),
        };
        round_trip_agent_event(retry);

        let tool_result = ToolResultMessage {
            tool_call_id: "call-1".to_owned(),
            tool_name: "read_file".to_owned(),
            content: vec![UserContent::Text {
                text: "file contents".to_owned(),
            }],
            details: json!({"exit": 0}),
            is_error: false,
            timestamp: now(),
        };
        let turn_end = AgentEvent::TurnEnd {
            message: None,
            tool_results: vec![tool_result],
        };
        round_trip_agent_event(turn_end);

        round_trip_agent_event(AgentEvent::Error {
            message: "bad".to_owned(),
        });
    }

    fn round_trip_agent_event(event: AgentEvent) {
        let wire = WireAgentEvent::try_from(event).unwrap();
        let json = serde_json::to_value(&wire).unwrap();
        let back: WireAgentEvent = serde_json::from_value(json).unwrap();
        assert_eq!(wire, back);
    }

    #[test]
    fn public_stream_event_round_trips() {
        let preview = PublicStreamEvent::ToolCallPreview {
            content_index: 0,
            preview: crate::provider::types::ToolArgsPreview::new(json!({"path": "x"})),
        };
        round_trip_stream_event(preview);

        let end = PublicStreamEvent::ToolCallEnd {
            content_index: 0,
            tool_call: tool_call(),
        };
        round_trip_stream_event(end);

        round_trip_stream_event(PublicStreamEvent::TextDelta {
            content_index: 0,
            delta: "d".to_owned(),
        });
    }

    fn round_trip_stream_event(event: PublicStreamEvent) {
        let wire = WirePublicStreamEvent::try_from(event).unwrap();
        let json = serde_json::to_value(&wire).unwrap();
        let back: WirePublicStreamEvent = serde_json::from_value(json).unwrap();
        assert_eq!(wire, back);
    }

    #[test]
    fn public_message_round_trips() {
        let user = PublicMessage::User(UserMessage {
            content: vec![UserContent::Text {
                text: "hi".to_owned(),
            }],
            timestamp: now(),
        });
        round_trip_public_message(user);

        let assistant = PublicMessage::Assistant(PublicAssistantMessage {
            content: vec![
                PublicAssistantContent::Thinking {
                    thinking: "...".to_owned(),
                    signature_field: "reasoning_content".to_owned(),
                    wire_item_index: 0,
                },
                PublicAssistantContent::Text {
                    text: "ok".to_owned(),
                    wire_item_index: 1,
                },
                PublicAssistantContent::ToolCall {
                    tool_call: tool_call(),
                    wire_item_index: 2,
                },
            ],
            model: "kimi-k3".to_owned(),
            provider: "moonshot".to_owned(),
            origin: provider_origin(),
            usage: usage(),
            stop_reason: StopReason::ToolUse,
            error_message: None,
            provider_code: Some("tool_calls".to_owned()),
            interrupted: false,
            timestamp: now(),
        });
        round_trip_public_message(assistant);
    }

    fn round_trip_public_message(message: PublicMessage) {
        let wire = WirePublicMessage::try_from(message).unwrap();
        let json = serde_json::to_value(&wire).unwrap();
        let back: WirePublicMessage = serde_json::from_value(json).unwrap();
        assert_eq!(wire, back);
    }

    #[test]
    fn contract_schema_validates_representative_wire_fixtures() {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let schema_path = manifest_dir.join("../../contracts/agent-events.yaml");
        let yaml = std::fs::read_to_string(&schema_path).expect("read contract schema");
        let schema: serde_json::Value = serde_yaml::from_str(&yaml).expect("parse contract schema");

        fn validator_for_def(schema: &serde_json::Value, def: &str) -> jsonschema::Validator {
            let mut root = schema.clone();
            let obj = root.as_object_mut().expect("schema root is an object");
            obj.insert(
                "$ref".to_owned(),
                serde_json::Value::String(format!("#/$defs/{def}")),
            );
            jsonschema::validator_for(&root).unwrap_or_else(|e| panic!("compile {def}: {e}"))
        }

        let outbound = validator_for_def(&schema, "OutboundFrame");
        let command_envelope = validator_for_def(&schema, "CommandEnvelope");

        let durable = to_wire_frame(OutboundFrame::Event {
            envelope: Envelope {
                seq: Some(1),
                conversation_id: "conv-1".to_owned(),
                event: json!({"type": "agent_start"}),
            },
        })
        .expect("durable event");
        outbound
            .validate(&serde_json::to_value(durable).unwrap())
            .expect("agent_start should validate");

        let volatile = to_wire_frame(OutboundFrame::Event {
            envelope: Envelope {
                seq: None,
                conversation_id: "conv-1".to_owned(),
                event: json!({
                    "type": "message_update",
                    "message_id": "00000000-0000-4000-8000-000000000002",
                    "event": {"type": "text_delta", "content_index": 0, "delta": "x"}
                }),
            },
        })
        .expect("volatile event");
        outbound
            .validate(&serde_json::to_value(volatile).unwrap())
            .expect("message_update should validate");

        let received = to_wire_frame(OutboundFrame::CommandAck {
            ack: CommandAck {
                seq: 1,
                command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                status: CommandAckStatus::Received,
                reject_reason: None,
            },
        })
        .expect("received ack");
        outbound
            .validate(&serde_json::to_value(received).unwrap())
            .expect("received ack should validate");

        let rejected = to_wire_frame(OutboundFrame::CommandAck {
            ack: CommandAck {
                seq: 2,
                command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                status: CommandAckStatus::Rejected,
                reject_reason: Some("oversized".to_owned()),
            },
        })
        .expect("rejected ack");
        outbound
            .validate(&serde_json::to_value(rejected).unwrap())
            .expect("rejected ack should validate");

        let command = WireCommandEnvelope::try_from(CommandEnvelope {
            seq: 1,
            command_id: uuid_command_id(),
            command: Command::UserMessage {
                text: "hi".to_owned(),
                attachments: vec![],
            },
        })
        .expect("command envelope");
        command_envelope
            .validate(&serde_json::to_value(command).unwrap())
            .expect("command envelope should validate");
    }
}
