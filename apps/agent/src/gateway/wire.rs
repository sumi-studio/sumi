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

#[cfg(test)]
use super::DeferredApprovalRule;
use super::{
    ApprovalDecision, Command, CommandAck, CommandAckStatus, CommandEnvelope, CommandId, Envelope,
    OutboundFrame,
};
use crate::agent::{
    AgentEvent, ApprovalRequest, ApprovalResolution, AuditDecision, AuditOutcome, MemoryMaintKind,
    PublicStreamEvent, ReviewProjection, RiskLevel, SteerMode, UserAuthorization,
};
use crate::provider::types::{
    ApiProtocol, ProviderOrigin, PublicAssistantContent, PublicAssistantMessage, PublicMessage,
    RejectedToolCall, StopReason, ToolArgumentError, ToolCall, ToolResultMessage, Usage,
    UserContent, UserMessage,
};

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
    seq: u64,
    command_id: String,
    status: WireCommandAckStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    reject_reason: Option<WireRejectReason>,
}

impl WireCommandAck {
    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn command_id(&self) -> &str {
        &self.command_id
    }

    pub fn status(&self) -> WireCommandAckStatus {
        self.status
    }

    pub fn reject_reason(&self) -> Option<WireRejectReason> {
        self.reject_reason
    }
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
        let command_id = canonical_command_id(&input.command_id)?;
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
            command_id,
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
    seq: Option<u64>,
    conversation_id: String,
    event: WireAgentEvent,
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

    pub fn seq(&self) -> Option<u64> {
        self.seq
    }

    pub fn conversation_id(&self) -> &str {
        &self.conversation_id
    }

    pub fn event(&self) -> &WireAgentEvent {
        &self.event
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
        Ok(match event {
            AgentEvent::AgentStart => Self::AgentStart,
            AgentEvent::AgentEnd => Self::AgentEnd,
            AgentEvent::TurnStart => Self::TurnStart,
            AgentEvent::TurnEnd {
                message,
                tool_results,
            } => Self::TurnEnd {
                message: match message {
                    Some(message) => Some(Box::new((*message).try_into()?)),
                    None => None,
                },
                tool_results: tool_results
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
            },
            AgentEvent::MessageStart {
                message_id,
                message,
            } => Self::MessageStart {
                message_id,
                message: Box::new((*message).try_into()?),
            },
            AgentEvent::MessageUpdate { message_id, event } => Self::MessageUpdate {
                message_id,
                event: event.try_into()?,
            },
            AgentEvent::MessageEnd {
                message_id,
                message,
            } => Self::MessageEnd {
                message_id,
                message: Box::new((*message).try_into()?),
            },
            AgentEvent::ToolExecutionStart {
                tool_call_id,
                tool_name,
                args,
            } => Self::ToolExecutionStart {
                tool_call_id,
                tool_name,
                args,
            },
            AgentEvent::ToolExecutionUpdate {
                tool_call_id,
                partial,
            } => Self::ToolExecutionUpdate {
                tool_call_id,
                partial,
            },
            AgentEvent::ToolExecutionEnd {
                tool_call_id,
                result,
                is_error,
            } => Self::ToolExecutionEnd {
                tool_call_id,
                result,
                is_error,
            },
            AgentEvent::ApprovalRequested { request } => Self::ApprovalRequested {
                request: request.try_into()?,
            },
            AgentEvent::ApprovalResolved {
                request_id,
                resolution,
            } => Self::ApprovalResolved {
                request_id,
                resolution: resolution.try_into()?,
            },
            AgentEvent::Steered { mode } => Self::Steered {
                mode: mode.try_into()?,
            },
            AgentEvent::MemoryMaintenance { kind } => Self::MemoryMaintenance {
                kind: kind.try_into()?,
            },
            AgentEvent::RetryScheduled {
                attempt,
                delay_ms,
                retry_at,
                error_message,
            } => Self::RetryScheduled {
                attempt,
                delay_ms,
                retry_at: retry_at.to_rfc3339(),
                error_message,
            },
            AgentEvent::Error { message } => Self::Error { message },
        })
    }
}

impl TryFrom<PublicStreamEvent> for WirePublicStreamEvent {
    type Error = WireError;
    fn try_from(event: PublicStreamEvent) -> Result<Self, WireError> {
        Ok(match event {
            PublicStreamEvent::TextStart { content_index } => Self::TextStart { content_index },
            PublicStreamEvent::TextDelta {
                content_index,
                delta,
            } => Self::TextDelta {
                content_index,
                delta,
            },
            PublicStreamEvent::TextEnd {
                content_index,
                content,
            } => Self::TextEnd {
                content_index,
                content,
            },
            PublicStreamEvent::ThinkingStart { content_index } => {
                Self::ThinkingStart { content_index }
            }
            PublicStreamEvent::ThinkingDelta {
                content_index,
                delta,
            } => Self::ThinkingDelta {
                content_index,
                delta,
            },
            PublicStreamEvent::ThinkingEnd {
                content_index,
                content,
            } => Self::ThinkingEnd {
                content_index,
                content,
            },
            PublicStreamEvent::ToolCallStart { content_index } => {
                Self::ToolCallStart { content_index }
            }
            PublicStreamEvent::ToolCallDelta {
                content_index,
                delta,
            } => Self::ToolCallDelta {
                content_index,
                delta,
            },
            PublicStreamEvent::ToolCallPreview {
                content_index,
                preview,
            } => Self::ToolCallPreview {
                content_index,
                preview: preview.as_value().clone(),
            },
            PublicStreamEvent::ToolCallEnd {
                content_index,
                tool_call,
            } => Self::ToolCallEnd {
                content_index,
                tool_call: tool_call.try_into()?,
            },
            PublicStreamEvent::ToolCallRejected {
                content_index,
                rejected,
            } => Self::ToolCallRejected {
                content_index,
                rejected: rejected.try_into()?,
            },
            PublicStreamEvent::ReasoningSummaryStart { content_index } => {
                Self::ReasoningSummaryStart { content_index }
            }
            PublicStreamEvent::ReasoningSummaryDelta {
                content_index,
                delta,
            } => Self::ReasoningSummaryDelta {
                content_index,
                delta,
            },
            PublicStreamEvent::ReasoningSummaryEnd {
                content_index,
                content,
            } => Self::ReasoningSummaryEnd {
                content_index,
                content,
            },
        })
    }
}

impl TryFrom<PublicMessage> for WirePublicMessage {
    type Error = WireError;
    fn try_from(message: PublicMessage) -> Result<Self, WireError> {
        Ok(match message {
            PublicMessage::User(UserMessage { content, timestamp }) => Self::User {
                content: content
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
                timestamp: timestamp.to_rfc3339(),
            },
            PublicMessage::Assistant(PublicAssistantMessage {
                content,
                model,
                provider,
                origin,
                usage,
                stop_reason,
                error_message,
                provider_code,
                interrupted,
                timestamp,
            }) => Self::Assistant {
                content: content
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
                model,
                provider,
                origin: origin.try_into()?,
                usage: usage.try_into()?,
                stop_reason: stop_reason.try_into()?,
                error_message,
                provider_code,
                interrupted,
                timestamp: timestamp.to_rfc3339(),
            },
            PublicMessage::ToolResult(ToolResultMessage {
                tool_call_id,
                tool_name,
                content,
                details,
                is_error,
                timestamp,
            }) => Self::ToolResult {
                tool_call_id,
                tool_name,
                content: content
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
                details,
                is_error,
                timestamp: timestamp.to_rfc3339(),
            },
        })
    }
}

impl TryFrom<UserContent> for WireUserContent {
    type Error = WireError;
    fn try_from(content: UserContent) -> Result<Self, WireError> {
        Ok(match content {
            UserContent::Text { text } => Self::Text { text },
            UserContent::Image { data, mime_type } => Self::Image { data, mime_type },
        })
    }
}

impl TryFrom<PublicAssistantContent> for WirePublicAssistantContent {
    type Error = WireError;
    fn try_from(content: PublicAssistantContent) -> Result<Self, WireError> {
        Ok(match content {
            PublicAssistantContent::Text {
                text,
                wire_item_index,
            } => Self::Text {
                text,
                wire_item_index,
            },
            PublicAssistantContent::Thinking {
                thinking,
                signature_field,
                wire_item_index,
            } => Self::Thinking {
                thinking,
                signature_field,
                wire_item_index,
            },
            PublicAssistantContent::ToolCall {
                tool_call,
                wire_item_index,
            } => Self::ToolCall {
                tool_call: tool_call.try_into()?,
                wire_item_index,
            },
            PublicAssistantContent::RejectedToolCall {
                rejected,
                wire_item_index,
            } => Self::RejectedToolCall {
                rejected: rejected.try_into()?,
                wire_item_index,
            },
        })
    }
}

impl TryFrom<ToolCall> for WireToolCall {
    type Error = WireError;
    fn try_from(tool_call: ToolCall) -> Result<Self, WireError> {
        Ok(Self {
            id: tool_call.id,
            name: tool_call.name,
            arguments: tool_call.arguments.as_object().clone(),
        })
    }
}

impl TryFrom<RejectedToolCall> for WireRejectedToolCall {
    type Error = WireError;
    fn try_from(rejected: RejectedToolCall) -> Result<Self, WireError> {
        Ok(Self {
            id: rejected.id,
            name: rejected.name,
            error: rejected.error.try_into()?,
        })
    }
}

impl TryFrom<ToolArgumentError> for WireToolArgumentError {
    type Error = WireError;
    fn try_from(error: ToolArgumentError) -> Result<Self, WireError> {
        Ok(match error {
            ToolArgumentError::InvalidJson => Self::InvalidJson,
            ToolArgumentError::NonObject => Self::NonObject,
            ToolArgumentError::SchemaViolation => Self::SchemaViolation,
            ToolArgumentError::IncompleteResponse => Self::IncompleteResponse,
            ToolArgumentError::TooLarge => Self::TooLarge,
        })
    }
}

impl TryFrom<ProviderOrigin> for WireProviderOrigin {
    type Error = WireError;
    fn try_from(origin: ProviderOrigin) -> Result<Self, WireError> {
        Ok(Self {
            provider_instance_id: origin.provider_instance_id,
            protocol: origin.protocol.try_into()?,
            model: origin.model,
        })
    }
}

impl TryFrom<ApiProtocol> for WireApiProtocol {
    type Error = WireError;
    fn try_from(protocol: ApiProtocol) -> Result<Self, WireError> {
        Ok(match protocol {
            ApiProtocol::OpenAiChatCompletions => Self::OpenAiChatCompletions,
            ApiProtocol::OpenAiResponses => Self::OpenAiResponses,
            ApiProtocol::AnthropicMessages => Self::AnthropicMessages,
        })
    }
}

impl TryFrom<Usage> for WireUsage {
    type Error = WireError;
    fn try_from(usage: Usage) -> Result<Self, WireError> {
        Ok(Self {
            input: usage.input,
            output: usage.output,
            cache_read: usage.cache_read,
            cache_write: usage.cache_write,
            reasoning: usage.reasoning,
            total_tokens: usage.total_tokens,
        })
    }
}

impl TryFrom<StopReason> for WireStopReason {
    type Error = WireError;
    fn try_from(reason: StopReason) -> Result<Self, WireError> {
        Ok(match reason {
            StopReason::Stop => Self::Stop,
            StopReason::Length => Self::Length,
            StopReason::ToolUse => Self::ToolUse,
            StopReason::Error => Self::Error,
            StopReason::Aborted => Self::Aborted,
        })
    }
}

impl TryFrom<ToolResultMessage> for WireToolResultMessage {
    type Error = WireError;
    fn try_from(message: ToolResultMessage) -> Result<Self, WireError> {
        Ok(Self {
            tool_call_id: message.tool_call_id,
            tool_name: message.tool_name,
            content: message
                .content
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
            details: message.details,
            is_error: message.is_error,
            timestamp: message.timestamp.to_rfc3339(),
        })
    }
}

impl TryFrom<ApprovalRequest> for WireApprovalRequest {
    type Error = WireError;
    fn try_from(request: ApprovalRequest) -> Result<Self, WireError> {
        Ok(Self {
            id: request.id,
            tool_call_id: request.tool_call_id,
            tool_name: request.tool_name,
            action: request.action.try_into()?,
            args_summary: request.args_summary,
            reason: request.reason,
            audit: request.audit.map(TryInto::try_into).transpose()?,
        })
    }
}

impl TryFrom<ReviewProjection> for WireReviewProjection {
    type Error = WireError;
    fn try_from(projection: ReviewProjection) -> Result<Self, WireError> {
        Ok(match projection {
            ReviewProjection::Reviewable(value) => Self::Reviewable(value),
            ReviewProjection::InsufficientEvidence { reason } => {
                Self::InsufficientEvidence { reason }
            }
        })
    }
}

impl TryFrom<AuditDecision> for WireAuditDecision {
    type Error = WireError;
    fn try_from(decision: AuditDecision) -> Result<Self, WireError> {
        Ok(Self {
            outcome: decision.outcome.try_into()?,
            risk: decision.risk.try_into()?,
            authorization: decision.authorization.try_into()?,
            rationale: decision.rationale,
        })
    }
}

impl TryFrom<AuditOutcome> for WireAuditOutcome {
    type Error = WireError;
    fn try_from(outcome: AuditOutcome) -> Result<Self, WireError> {
        Ok(match outcome {
            AuditOutcome::Allow => Self::Allow,
            AuditOutcome::Deny => Self::Deny,
        })
    }
}

impl TryFrom<RiskLevel> for WireRiskLevel {
    type Error = WireError;
    fn try_from(risk: RiskLevel) -> Result<Self, WireError> {
        Ok(match risk {
            RiskLevel::Low => Self::Low,
            RiskLevel::Medium => Self::Medium,
            RiskLevel::High => Self::High,
            RiskLevel::Critical => Self::Critical,
        })
    }
}

impl TryFrom<UserAuthorization> for WireUserAuthorization {
    type Error = WireError;
    fn try_from(authorization: UserAuthorization) -> Result<Self, WireError> {
        Ok(match authorization {
            UserAuthorization::Unknown => Self::Unknown,
            UserAuthorization::Low => Self::Low,
            UserAuthorization::Medium => Self::Medium,
            UserAuthorization::High => Self::High,
        })
    }
}

impl TryFrom<ApprovalResolution> for WireApprovalResolution {
    type Error = WireError;
    fn try_from(resolution: ApprovalResolution) -> Result<Self, WireError> {
        Ok(match resolution {
            ApprovalResolution::Decision(decision) => Self::Decision(decision.try_into()?),
            ApprovalResolution::Cancelled => Self::Cancelled,
        })
    }
}

impl TryFrom<ApprovalDecision> for WireApprovalDecision {
    type Error = WireError;
    fn try_from(decision: ApprovalDecision) -> Result<Self, WireError> {
        Ok(match decision {
            ApprovalDecision::ApproveOnce => Self::ApproveOnce,
            ApprovalDecision::ApproveAlways { rule } => Self::ApproveAlways {
                rule: rule.0.as_object().cloned().unwrap_or_default(),
            },
            ApprovalDecision::Deny => Self::Deny,
        })
    }
}

impl TryFrom<SteerMode> for WireSteerMode {
    type Error = WireError;
    fn try_from(mode: SteerMode) -> Result<Self, WireError> {
        Ok(match mode {
            SteerMode::Hard => Self::Hard,
            SteerMode::Soft => Self::Soft,
        })
    }
}

impl TryFrom<MemoryMaintKind> for WireMemoryMaintKind {
    type Error = WireError;
    fn try_from(kind: MemoryMaintKind) -> Result<Self, WireError> {
        Ok(WireMemoryMaintKind(kind.as_str().to_owned()))
    }
}

impl TryFrom<Command> for WireCommand {
    type Error = WireError;
    fn try_from(command: Command) -> Result<Self, WireError> {
        Ok(match command {
            Command::UserMessage { text, attachments } => Self::UserMessage {
                text,
                attachments: attachments
                    .into_iter()
                    .map(|attachment| WireAttachment(attachment.0))
                    .collect(),
            },
            Command::Abort {} => Self::Abort,
            Command::ApprovalDecision {
                request_id,
                decision,
            } => Self::ApprovalDecision {
                request_id,
                decision: decision.try_into()?,
            },
        })
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
        let command_id = canonical_command_id(&ack.command_id)?;
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
            command_id,
            status,
            reject_reason,
        })
    }
}

impl TryFrom<Envelope> for WireEnvelope {
    type Error = WireError;
    fn try_from(envelope: Envelope) -> Result<Self, WireError> {
        let event: AgentEvent = serde_json::from_value(envelope.event)?;
        let event: WireAgentEvent = event.try_into()?;
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

fn canonical_command_id(value: &str) -> Result<String, WireError> {
    CommandId::parse(value)
        .map(|id| id.as_str().to_owned())
        .map_err(|_| WireError::InvalidCommandId(value.to_owned()))
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
        RejectedToolCall, StopReason, ToolArgumentError, ToolCall, ToolResultMessage, Usage,
        UserContent, UserMessage,
    };

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
    fn wire_command_ack_rejects_invalid_input() {
        let valid_id = "00000000-0000-4000-8000-000000000001";

        let invalid_uuid = json!({
            "seq": 1,
            "command_id": "not-a-uuid",
            "status": "received"
        });
        assert!(serde_json::from_value::<WireCommandAck>(invalid_uuid).is_err());

        assert!(
            to_wire_frame(OutboundFrame::CommandAck {
                ack: CommandAck {
                    seq: 1,
                    command_id: "not-a-uuid".to_owned(),
                    status: CommandAckStatus::Received,
                    reject_reason: None,
                }
            })
            .is_err()
        );

        let rejected_without_reason = json!({
            "seq": 1,
            "command_id": valid_id,
            "status": "rejected"
        });
        assert!(serde_json::from_value::<WireCommandAck>(rejected_without_reason).is_err());

        let non_rejected_with_reason = json!({
            "seq": 1,
            "command_id": valid_id,
            "status": "received",
            "reject_reason": "oversized"
        });
        assert!(serde_json::from_value::<WireCommandAck>(non_rejected_with_reason).is_err());
    }

    #[test]
    fn wire_envelope_rejects_invalid_input() {
        let durable_missing_seq = json!({
            "conversation_id": "conv-1",
            "event": {"type": "agent_start"}
        });
        assert!(serde_json::from_value::<WireEnvelope>(durable_missing_seq).is_err());

        let volatile_with_seq = json!({
            "seq": 1,
            "conversation_id": "conv-1",
            "event": {"type": "error", "message": "x"}
        });
        assert!(serde_json::from_value::<WireEnvelope>(volatile_with_seq).is_err());
    }

    #[test]
    fn agent_event_round_trips() {
        let user_message = PublicMessage::User(UserMessage {
            content: vec![UserContent::Text {
                text: "hello".to_owned(),
            }],
            timestamp: now(),
        });

        round_trip_agent_event(AgentEvent::AgentStart);
        round_trip_agent_event(AgentEvent::AgentEnd);
        round_trip_agent_event(AgentEvent::TurnStart);

        let start = AgentEvent::MessageStart {
            message_id: "00000000-0000-4000-8000-000000000002".to_owned(),
            message: Box::new(user_message.clone()),
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

        let end = AgentEvent::MessageEnd {
            message_id: "00000000-0000-4000-8000-000000000002".to_owned(),
            message: Box::new(user_message),
        };
        round_trip_agent_event(end);

        let tool_start = AgentEvent::ToolExecutionStart {
            tool_call_id: "call-1".to_owned(),
            tool_name: "read_file".to_owned(),
            args: json!({"path": "notes.txt"}),
        };
        round_trip_agent_event(tool_start);

        let tool_update = AgentEvent::ToolExecutionUpdate {
            tool_call_id: "call-1".to_owned(),
            partial: json!({"chunk": "x"}),
        };
        round_trip_agent_event(tool_update);

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

        let approval_with_audit: ApprovalRequest = serde_json::from_value(json!({
            "id": "req-2",
            "tool_call_id": "call-1",
            "tool_name": "bash",
            "action": {"insufficient_evidence": {"reason": "unknown"}},
            "args_summary": {"cmd": "ls"},
            "reason": "need evidence",
            "audit": {
                "outcome": "deny",
                "risk": "high",
                "authorization": "low",
                "rationale": "unsafe"
            }
        }))
        .unwrap();
        round_trip_agent_event(AgentEvent::ApprovalRequested {
            request: approval_with_audit,
        });

        let resolved: ApprovalResolution =
            serde_json::from_value(json!({"decision": {"type": "approve_once"}})).unwrap();
        round_trip_agent_event(AgentEvent::ApprovalResolved {
            request_id: "req-1".to_owned(),
            resolution: resolved,
        });

        round_trip_agent_event(AgentEvent::ApprovalResolved {
            request_id: "req-1".to_owned(),
            resolution: ApprovalResolution::Cancelled,
        });

        round_trip_agent_event(AgentEvent::Steered {
            mode: SteerMode::Hard,
        });
        round_trip_agent_event(AgentEvent::Steered {
            mode: SteerMode::Soft,
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
        fn rejected_tool_call() -> RejectedToolCall {
            RejectedToolCall {
                id: "call-2".to_owned(),
                name: "read_file".to_owned(),
                error: ToolArgumentError::SchemaViolation,
            }
        }

        round_trip_stream_event(PublicStreamEvent::TextStart { content_index: 0 });
        round_trip_stream_event(PublicStreamEvent::TextDelta {
            content_index: 0,
            delta: "d".to_owned(),
        });
        round_trip_stream_event(PublicStreamEvent::TextEnd {
            content_index: 0,
            content: "text".to_owned(),
        });

        round_trip_stream_event(PublicStreamEvent::ThinkingStart { content_index: 0 });
        round_trip_stream_event(PublicStreamEvent::ThinkingDelta {
            content_index: 0,
            delta: "t".to_owned(),
        });
        round_trip_stream_event(PublicStreamEvent::ThinkingEnd {
            content_index: 0,
            content: "thought".to_owned(),
        });

        round_trip_stream_event(PublicStreamEvent::ToolCallStart { content_index: 0 });
        round_trip_stream_event(PublicStreamEvent::ToolCallDelta {
            content_index: 0,
            delta: "{\"path".to_owned(),
        });
        round_trip_stream_event(PublicStreamEvent::ToolCallPreview {
            content_index: 0,
            preview: crate::provider::types::ToolArgsPreview::new(json!({"path": "x"})),
        });
        round_trip_stream_event(PublicStreamEvent::ToolCallEnd {
            content_index: 0,
            tool_call: tool_call(),
        });
        round_trip_stream_event(PublicStreamEvent::ToolCallRejected {
            content_index: 0,
            rejected: rejected_tool_call(),
        });

        round_trip_stream_event(PublicStreamEvent::ReasoningSummaryStart { content_index: 0 });
        round_trip_stream_event(PublicStreamEvent::ReasoningSummaryDelta {
            content_index: 0,
            delta: "s".to_owned(),
        });
        round_trip_stream_event(PublicStreamEvent::ReasoningSummaryEnd {
            content_index: 0,
            content: "summary".to_owned(),
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

        let user_with_image = PublicMessage::User(UserMessage {
            content: vec![UserContent::Image {
                data: "aGVsbG8=".to_owned(),
                mime_type: "image/png".to_owned(),
            }],
            timestamp: now(),
        });
        round_trip_public_message(user_with_image);

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
                PublicAssistantContent::RejectedToolCall {
                    rejected: RejectedToolCall {
                        id: "call-2".to_owned(),
                        name: "read_file".to_owned(),
                        error: ToolArgumentError::SchemaViolation,
                    },
                    wire_item_index: 3,
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

        let tool_result = PublicMessage::ToolResult(ToolResultMessage {
            tool_call_id: "call-1".to_owned(),
            tool_name: "read_file".to_owned(),
            content: vec![UserContent::Text {
                text: "contents".to_owned(),
            }],
            details: json!({"exit": 0}),
            is_error: false,
            timestamp: now(),
        });
        round_trip_public_message(tool_result);
    }

    fn round_trip_public_message(message: PublicMessage) {
        let wire = WirePublicMessage::try_from(message).unwrap();
        let json = serde_json::to_value(&wire).unwrap();
        let back: WirePublicMessage = serde_json::from_value(json).unwrap();
        assert_eq!(wire, back);
    }

    #[test]
    fn command_round_trips() {
        let user_message = Command::UserMessage {
            text: "hi".to_owned(),
            attachments: vec![],
        };
        round_trip_command(user_message);

        round_trip_command(Command::Abort {});

        let approve_once = Command::ApprovalDecision {
            request_id: "req-1".to_owned(),
            decision: ApprovalDecision::ApproveOnce,
        };
        round_trip_command(approve_once);

        let approve_always = Command::ApprovalDecision {
            request_id: "req-1".to_owned(),
            decision: ApprovalDecision::ApproveAlways {
                rule: DeferredApprovalRule(json!({"tool_name": "test"})),
            },
        };
        round_trip_command(approve_always);

        let deny = Command::ApprovalDecision {
            request_id: "req-1".to_owned(),
            decision: ApprovalDecision::Deny,
        };
        round_trip_command(deny);
    }

    fn round_trip_command(command: Command) {
        let wire = WireCommand::try_from(command).unwrap();
        let json = serde_json::to_value(&wire).unwrap();
        let back: WireCommand = serde_json::from_value(json).unwrap();
        assert_eq!(wire, back);
    }
}
