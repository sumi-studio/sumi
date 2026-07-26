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

use chrono::{DateTime, Utc};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, DeserializeOwned, IgnoredAny, SeqAccess, Visitor},
};
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

use super::{
    ApprovalDecision, Command, CommandAck, CommandAckStatus, CommandEnvelope, CommandId, Envelope,
    OutboundFrame,
};
#[cfg(test)]
use super::{Attachment, DeferredApprovalRule};
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

/// JSON numbers above this value cannot be represented exactly by JavaScript's
/// `number` type. Wire indices are bounded to this value so generated clients
/// have one architecture-independent, lossless representation.
pub const MAX_JSON_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// A wire field that must be present but may be JSON `null`.
#[derive(Clone, Debug, PartialEq)]
pub struct RequiredNullable<T>(Option<T>);

impl<T> RequiredNullable<T> {
    pub fn some(value: T) -> Self {
        Self(Some(value))
    }

    pub fn none() -> Self {
        Self(None)
    }

    pub fn into_option(self) -> Option<T> {
        self.0
    }

    pub fn as_ref(&self) -> Option<&T> {
        self.0.as_ref()
    }

    pub fn is_some(&self) -> bool {
        self.0.is_some()
    }
}

impl<T> From<T> for RequiredNullable<T> {
    fn from(value: T) -> Self {
        Self(Some(value))
    }
}

impl<T> From<Option<T>> for RequiredNullable<T> {
    fn from(value: Option<T>) -> Self {
        Self(value)
    }
}

impl<T: Serialize> Serialize for RequiredNullable<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self.0 {
            Some(value) => value.serialize(serializer),
            None => serializer.serialize_none(),
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for RequiredNullable<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        if value.is_null() {
            Ok(Self(None))
        } else {
            T::deserialize(value)
                .map(Self::some)
                .map_err(de::Error::custom)
        }
    }
}

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
    #[error("message_id `{0}` is not a canonical UUID")]
    InvalidMessageId(String),
    #[error("approval rule must be an object")]
    NonObjectApprovalRule,
    #[error("tool execution args must be an object")]
    NonObjectToolExecutionArgs,
    #[error("content_index `{0}` exceeds the JSON-safe integer range")]
    ContentIndexOutOfRange(u64),
    #[error("usage value `{0}` exceeds the JSON-safe integer range")]
    UsageValueOutOfRange(u64),
    #[error("AnyJSON contains a number outside the JavaScript-safe range")]
    AnyJSONNumberOutOfRange,
    #[error("integer `{0}` exceeds the JSON-safe integer range")]
    JsonSafeIntegerOutOfRange(u64),
    #[error("seq `{0}` exceeds the JSON-safe integer range")]
    SeqOutOfRange(u64),
    #[error("user message attachments must be empty")]
    NonEmptyAttachments,
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
    let wire: WireOutboundFrame = frame.try_into()?;
    // Internal event values contain opaque JSON. Validate the final public
    // representation at the production writer boundary so nested values cannot
    // lose integer precision in the TypeScript consumer.
    let value = serde_json::to_value(&wire)?;
    validate_json_safe_numbers(&value)?;
    Ok(wire)
}

/// Parse a raw JSON byte slice into a wire DTO while rejecting duplicate object
/// keys and trailing tokens. This is the production raw boundary for `Wire*` DTOs
/// when they are decoded directly from bytes.
pub(crate) fn from_json_bytes<T>(bytes: &[u8]) -> Result<T, WireError>
where
    T: DeserializeOwned,
{
    let value = super::duplicate::parse_duplicate_checked_bytes(bytes).map_err(WireError::Json)?;
    serde_json::from_value(value).map_err(WireError::Json)
}

/// Canonical lower-case hyphenated UUID used for `message_id` in message events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageId(String);

impl MessageId {
    pub fn parse(value: &str) -> Result<Self, &'static str> {
        let uuid = Uuid::parse_str(value).map_err(|_| "message_id is not a UUID")?;
        let canonical = uuid.hyphenated().to_string();
        if value != canonical {
            return Err("message_id is not in canonical lower-case hyphenated form");
        }
        Ok(Self(canonical))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for MessageId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for MessageId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(|_| de::Error::custom("message_id must be a canonical UUID"))
    }
}

/// Attachment placeholder. v1 accepts only an empty array.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WireAttachment(Value);

fn deserialize_empty_attachments<'de, D>(deserializer: D) -> Result<Vec<WireAttachment>, D::Error>
where
    D: Deserializer<'de>,
{
    struct EmptyAttachmentsVisitor;

    impl<'de> Visitor<'de> for EmptyAttachmentsVisitor {
        type Value = Vec<WireAttachment>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an empty attachments array")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            if sequence.next_element::<IgnoredAny>()?.is_some() {
                return Err(de::Error::custom("attachments must be empty"));
            }
            Ok(Vec::new())
        }
    }

    deserializer.deserialize_seq(EmptyAttachmentsVisitor)
}

/// Helper for optional fields that are forbidden to appear as explicit `null`.
///
/// Use with `#[serde(default, deserialize_with = "present_or_error_on_null")]`.
/// A missing field uses serde's default (`None` for `Option`). A present `null`
/// is rejected by delegating to `T::deserialize`, which fails on `null`. A
/// present non-null value is deserialized and wrapped in `Some`.
fn present_or_error_on_null<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
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
    Abort {},
    ApprovalDecision {
        request_id: String,
        decision: WireApprovalDecision,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "WireCommandEnvelopeInput")]
pub struct WireCommandEnvelope {
    seq: u64,
    command_id: String,
    command: WireCommand,
}

impl WireCommandEnvelope {
    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn command_id(&self) -> &str {
        &self.command_id
    }

    pub fn command(&self) -> &WireCommand {
        &self.command
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireCommandEnvelopeInput {
    seq: u64,
    command_id: String,
    command: WireCommand,
}

impl TryFrom<WireCommandEnvelopeInput> for WireCommandEnvelope {
    type Error = WireError;

    fn try_from(input: WireCommandEnvelopeInput) -> Result<Self, Self::Error> {
        if input.seq > MAX_JSON_SAFE_INTEGER {
            return Err(WireError::SeqOutOfRange(input.seq));
        }
        Ok(Self {
            seq: input.seq,
            command_id: canonical_command_id(&input.command_id)?,
            command: input.command,
        })
    }
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
    // `present_or_error_on_null` rejects an explicit `reject_reason: null`;
    // the default attribute means a missing `reject_reason` yields `None`.
    // The status-based rules below then enforce the conditional schema.
    #[serde(default, deserialize_with = "present_or_error_on_null")]
    reject_reason: Option<WireRejectReason>,
}

impl TryFrom<WireCommandAckInput> for WireCommandAck {
    type Error = WireError;
    fn try_from(input: WireCommandAckInput) -> Result<Self, WireError> {
        if input.seq > MAX_JSON_SAFE_INTEGER {
            return Err(WireError::SeqOutOfRange(input.seq));
        }
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
    // `present_or_error_on_null` rejects an explicit `seq: null`; the default
    // attribute means a missing `seq` yields `None`. `validate_seq` then
    // enforces the durable/volatile presence rules.
    #[serde(default, deserialize_with = "present_or_error_on_null")]
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
    AgentStart {},
    AgentEnd {},
    TurnStart {},
    TurnEnd {
        message: RequiredNullable<Box<WirePublicMessage>>,
        tool_results: Vec<WireToolResultMessage>,
    },
    MessageStart {
        message_id: MessageId,
        message: Box<WirePublicMessage>,
    },
    MessageUpdate {
        message_id: MessageId,
        event: WirePublicStreamEvent,
    },
    MessageEnd {
        message_id: MessageId,
        message: Box<WirePublicMessage>,
    },
    ToolExecutionStart {
        tool_call_id: String,
        tool_name: String,
        args: Map<String, Value>,
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
        #[serde(deserialize_with = "deserialize_json_safe_integer")]
        attempt: u64,
        #[serde(deserialize_with = "deserialize_json_safe_integer")]
        delay_ms: u64,
        retry_at: DateTime<Utc>,
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
            Self::AgentStart {} => "agent_start",
            Self::AgentEnd {} => "agent_end",
            Self::TurnStart {} => "turn_start",
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
        #[serde(deserialize_with = "deserialize_json_safe_index")]
        content_index: u64,
    },
    TextDelta {
        #[serde(deserialize_with = "deserialize_json_safe_index")]
        content_index: u64,
        delta: String,
    },
    TextEnd {
        #[serde(deserialize_with = "deserialize_json_safe_index")]
        content_index: u64,
        content: String,
    },
    ThinkingStart {
        #[serde(deserialize_with = "deserialize_json_safe_index")]
        content_index: u64,
    },
    ThinkingDelta {
        #[serde(deserialize_with = "deserialize_json_safe_index")]
        content_index: u64,
        delta: String,
    },
    ThinkingEnd {
        #[serde(deserialize_with = "deserialize_json_safe_index")]
        content_index: u64,
        content: String,
    },
    ToolCallStart {
        #[serde(deserialize_with = "deserialize_json_safe_index")]
        content_index: u64,
    },
    ToolCallDelta {
        #[serde(deserialize_with = "deserialize_json_safe_index")]
        content_index: u64,
        delta: String,
    },
    ToolCallPreview {
        #[serde(deserialize_with = "deserialize_json_safe_index")]
        content_index: u64,
        preview: Value,
    },
    ToolCallEnd {
        #[serde(deserialize_with = "deserialize_json_safe_index")]
        content_index: u64,
        tool_call: WireToolCall,
    },
    ToolCallRejected {
        #[serde(deserialize_with = "deserialize_json_safe_index")]
        content_index: u64,
        rejected: WireRejectedToolCall,
    },
    ReasoningSummaryStart {
        #[serde(deserialize_with = "deserialize_json_safe_index")]
        content_index: u64,
    },
    ReasoningSummaryDelta {
        #[serde(deserialize_with = "deserialize_json_safe_index")]
        content_index: u64,
        delta: String,
    },
    ReasoningSummaryEnd {
        #[serde(deserialize_with = "deserialize_json_safe_index")]
        content_index: u64,
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
        timestamp: DateTime<Utc>,
    },
    Assistant {
        content: Vec<WirePublicAssistantContent>,
        model: String,
        provider: String,
        origin: WireProviderOrigin,
        usage: WireUsage,
        stop_reason: WireStopReason,
        error_message: RequiredNullable<String>,
        provider_code: RequiredNullable<String>,
        interrupted: bool,
        timestamp: DateTime<Utc>,
    },
    ToolResult {
        tool_call_id: String,
        tool_name: String,
        content: Vec<WireUserContent>,
        details: Value,
        is_error: bool,
        timestamp: DateTime<Utc>,
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
        #[serde(deserialize_with = "deserialize_json_safe_integer")]
        wire_item_index: u64,
    },
    Thinking {
        thinking: String,
        signature_field: String,
        #[serde(deserialize_with = "deserialize_json_safe_integer")]
        wire_item_index: u64,
    },
    ToolCall {
        tool_call: WireToolCall,
        #[serde(deserialize_with = "deserialize_json_safe_integer")]
        wire_item_index: u64,
    },
    RejectedToolCall {
        rejected: WireRejectedToolCall,
        #[serde(deserialize_with = "deserialize_json_safe_integer")]
        wire_item_index: u64,
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
    #[serde(deserialize_with = "deserialize_json_safe_integer")]
    pub input: u64,
    #[serde(deserialize_with = "deserialize_json_safe_integer")]
    pub output: u64,
    #[serde(deserialize_with = "deserialize_json_safe_integer")]
    pub cache_read: u64,
    #[serde(deserialize_with = "deserialize_json_safe_integer")]
    pub cache_write: u64,
    #[serde(deserialize_with = "deserialize_json_safe_integer")]
    pub reasoning: u64,
    #[serde(deserialize_with = "deserialize_json_safe_integer")]
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
    pub timestamp: DateTime<Utc>,
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
    ApproveOnce {},
    ApproveAlways { rule: Map<String, Value> },
    Deny {},
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
            AgentEvent::AgentStart => Self::AgentStart {},
            AgentEvent::AgentEnd => Self::AgentEnd {},
            AgentEvent::TurnStart => Self::TurnStart {},
            AgentEvent::TurnEnd {
                message,
                tool_results,
            } => Self::TurnEnd {
                message: match message {
                    Some(message) => RequiredNullable::some(Box::new((*message).try_into()?)),
                    None => RequiredNullable::none(),
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
                message_id: MessageId::parse(&message_id)
                    .map_err(|_| WireError::InvalidMessageId(message_id))?,
                message: Box::new((*message).try_into()?),
            },
            AgentEvent::MessageUpdate { message_id, event } => Self::MessageUpdate {
                message_id: MessageId::parse(&message_id)
                    .map_err(|_| WireError::InvalidMessageId(message_id))?,
                event: event.try_into()?,
            },
            AgentEvent::MessageEnd {
                message_id,
                message,
            } => Self::MessageEnd {
                message_id: MessageId::parse(&message_id)
                    .map_err(|_| WireError::InvalidMessageId(message_id))?,
                message: Box::new((*message).try_into()?),
            },
            AgentEvent::ToolExecutionStart {
                tool_call_id,
                tool_name,
                args,
            } => Self::ToolExecutionStart {
                tool_call_id,
                tool_name,
                args: args
                    .as_object()
                    .cloned()
                    .ok_or(WireError::NonObjectToolExecutionArgs)?,
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
                attempt: wire_json_safe_integer(u64::from(attempt))?,
                delay_ms: wire_json_safe_integer(delay_ms)?,
                retry_at,
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
            PublicStreamEvent::TextStart { content_index } => Self::TextStart {
                content_index: wire_content_index(content_index)?,
            },
            PublicStreamEvent::TextDelta {
                content_index,
                delta,
            } => Self::TextDelta {
                content_index: wire_content_index(content_index)?,
                delta,
            },
            PublicStreamEvent::TextEnd {
                content_index,
                content,
            } => Self::TextEnd {
                content_index: wire_content_index(content_index)?,
                content,
            },
            PublicStreamEvent::ThinkingStart { content_index } => Self::ThinkingStart {
                content_index: wire_content_index(content_index)?,
            },
            PublicStreamEvent::ThinkingDelta {
                content_index,
                delta,
            } => Self::ThinkingDelta {
                content_index: wire_content_index(content_index)?,
                delta,
            },
            PublicStreamEvent::ThinkingEnd {
                content_index,
                content,
            } => Self::ThinkingEnd {
                content_index: wire_content_index(content_index)?,
                content,
            },
            PublicStreamEvent::ToolCallStart { content_index } => Self::ToolCallStart {
                content_index: wire_content_index(content_index)?,
            },
            PublicStreamEvent::ToolCallDelta {
                content_index,
                delta,
            } => Self::ToolCallDelta {
                content_index: wire_content_index(content_index)?,
                delta,
            },
            PublicStreamEvent::ToolCallPreview {
                content_index,
                preview,
            } => Self::ToolCallPreview {
                content_index: wire_content_index(content_index)?,
                preview: preview.as_value().clone(),
            },
            PublicStreamEvent::ToolCallEnd {
                content_index,
                tool_call,
            } => Self::ToolCallEnd {
                content_index: wire_content_index(content_index)?,
                tool_call: tool_call.try_into()?,
            },
            PublicStreamEvent::ToolCallRejected {
                content_index,
                rejected,
            } => Self::ToolCallRejected {
                content_index: wire_content_index(content_index)?,
                rejected: rejected.try_into()?,
            },
            PublicStreamEvent::ReasoningSummaryStart { content_index } => {
                Self::ReasoningSummaryStart {
                    content_index: wire_content_index(content_index)?,
                }
            }
            PublicStreamEvent::ReasoningSummaryDelta {
                content_index,
                delta,
            } => Self::ReasoningSummaryDelta {
                content_index: wire_content_index(content_index)?,
                delta,
            },
            PublicStreamEvent::ReasoningSummaryEnd {
                content_index,
                content,
            } => Self::ReasoningSummaryEnd {
                content_index: wire_content_index(content_index)?,
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
                timestamp,
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
                error_message: error_message.into(),
                provider_code: provider_code.into(),
                interrupted,
                timestamp,
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
                timestamp,
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
                wire_item_index: wire_json_safe_integer(u64::from(wire_item_index))?,
            },
            PublicAssistantContent::Thinking {
                thinking,
                signature_field,
                wire_item_index,
            } => Self::Thinking {
                thinking,
                signature_field,
                wire_item_index: wire_json_safe_integer(u64::from(wire_item_index))?,
            },
            PublicAssistantContent::ToolCall {
                tool_call,
                wire_item_index,
            } => Self::ToolCall {
                tool_call: tool_call.try_into()?,
                wire_item_index: wire_json_safe_integer(u64::from(wire_item_index))?,
            },
            PublicAssistantContent::RejectedToolCall {
                rejected,
                wire_item_index,
            } => Self::RejectedToolCall {
                rejected: rejected.try_into()?,
                wire_item_index: wire_json_safe_integer(u64::from(wire_item_index))?,
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
            input: wire_usage_value(usage.input)?,
            output: wire_usage_value(usage.output)?,
            cache_read: wire_usage_value(usage.cache_read)?,
            cache_write: wire_usage_value(usage.cache_write)?,
            reasoning: wire_usage_value(usage.reasoning)?,
            total_tokens: wire_usage_value(usage.total_tokens)?,
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
            timestamp: message.timestamp,
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
            ApprovalDecision::ApproveOnce => Self::ApproveOnce {},
            ApprovalDecision::ApproveAlways { rule } => Self::ApproveAlways {
                rule: match rule.0 {
                    Value::Object(m) => m,
                    _ => return Err(WireError::NonObjectApprovalRule),
                },
            },
            ApprovalDecision::Deny => Self::Deny {},
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
            Command::UserMessage { text, attachments } => {
                if !attachments.is_empty() {
                    return Err(WireError::NonEmptyAttachments);
                }
                Self::UserMessage {
                    text,
                    attachments: vec![],
                }
            }
            Command::Abort {} => Self::Abort {},
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
        if envelope.seq > MAX_JSON_SAFE_INTEGER {
            return Err(WireError::SeqOutOfRange(envelope.seq));
        }
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

fn wire_content_index(value: usize) -> Result<u64, WireError> {
    let value = u64::try_from(value).map_err(|_| WireError::ContentIndexOutOfRange(u64::MAX))?;
    if value > MAX_JSON_SAFE_INTEGER {
        return Err(WireError::ContentIndexOutOfRange(value));
    }
    Ok(value)
}

fn deserialize_json_safe_index<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    if value > MAX_JSON_SAFE_INTEGER {
        return Err(de::Error::custom(
            "content_index exceeds the JSON-safe integer range",
        ));
    }
    Ok(value)
}

fn deserialize_json_safe_integer<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    if value > MAX_JSON_SAFE_INTEGER {
        return Err(de::Error::custom(
            "integer exceeds the JSON-safe integer range",
        ));
    }
    Ok(value)
}

fn wire_usage_value(value: u64) -> Result<u64, WireError> {
    if value > MAX_JSON_SAFE_INTEGER {
        return Err(WireError::UsageValueOutOfRange(value));
    }
    Ok(value)
}

fn wire_json_safe_integer(value: u64) -> Result<u64, WireError> {
    if value > MAX_JSON_SAFE_INTEGER {
        return Err(WireError::JsonSafeIntegerOutOfRange(value));
    }
    Ok(value)
}

fn validate_json_safe_numbers(value: &Value) -> Result<(), WireError> {
    match value {
        Value::Null | Value::Bool(_) | Value::String(_) => Ok(()),
        Value::Number(number) => {
            let within_range = number
                .as_i64()
                .map(|value| {
                    value >= -(MAX_JSON_SAFE_INTEGER as i64)
                        && value <= MAX_JSON_SAFE_INTEGER as i64
                })
                .or_else(|| number.as_u64().map(|value| value <= MAX_JSON_SAFE_INTEGER))
                .or_else(|| {
                    number.as_f64().map(|value| {
                        value.is_finite() && value.abs() <= MAX_JSON_SAFE_INTEGER as f64
                    })
                })
                .unwrap_or(false);
            if within_range {
                Ok(())
            } else {
                Err(WireError::AnyJSONNumberOutOfRange)
            }
        }
        Value::Array(values) => values.iter().try_for_each(validate_json_safe_numbers),
        Value::Object(values) => values.values().try_for_each(validate_json_safe_numbers),
    }
}

fn validate_seq(seq: Option<u64>, event: &WireAgentEvent) -> Result<(), WireError> {
    if event.is_volatile() {
        if seq.is_some() {
            return Err(WireError::SeqForbidden {
                event_type: event.event_type(),
            });
        }
        return Ok(());
    }
    let Some(seq) = seq else {
        return Err(WireError::SeqRequired {
            event_type: event.event_type(),
        });
    };
    if seq > MAX_JSON_SAFE_INTEGER {
        return Err(WireError::SeqOutOfRange(seq));
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
    use crate::gateway::{AgentHello, ApiHello};
    use crate::provider::types::{
        ApiProtocol, ProviderOrigin, PublicAssistantContent, PublicAssistantMessage, PublicMessage,
        RejectedToolCall, StopReason, ToolArgumentError, ToolCall, ToolResultMessage, Usage,
        UserContent, UserMessage,
    };

    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    fn canonical_contract_is_valid(definition: &str, value: &Value) -> bool {
        let contract: Value = serde_yaml::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../contracts/agent-events.yaml"
        )))
        .expect("canonical agent-events schema is valid YAML");
        let schema = json!({
            "$schema": contract["$schema"].clone(),
            "$ref": format!("#/$defs/{definition}"),
            "$defs": contract["$defs"].clone(),
        });
        let validator = jsonschema::options()
            .should_validate_formats(true)
            .build(&schema)
            .expect("canonical agent-events schema compiles");
        validator.is_valid(value)
    }

    fn assert_canonical_contract(definition: &str, value: &Value) {
        assert!(
            canonical_contract_is_valid(definition, value),
            "{definition} sample violates canonical schema"
        );
    }

    #[test]
    fn any_json_number_validation_is_recursive_and_keeps_fractions() {
        let valid = json!({
            "minimum": -9_007_199_254_740_991i64,
            "maximum": 9_007_199_254_740_991u64,
            "fraction": 0.5,
            "nested": [{ "fraction": -1.25 }],
        });
        assert!(validate_json_safe_numbers(&valid).is_ok());
        assert!(matches!(
            validate_json_safe_numbers(&json!({"nested": [9_007_199_254_740_992u64]})),
            Err(WireError::AnyJSONNumberOutOfRange)
        ));
        assert!(matches!(
            validate_json_safe_numbers(&json!({"nested": [-9_007_199_254_740_992i64]})),
            Err(WireError::AnyJSONNumberOutOfRange)
        ));
    }

    #[test]
    fn object_valued_wire_fields_reject_unsafe_nested_numbers() {
        let tool_start = OutboundFrame::Event {
            envelope: Envelope {
                seq: Some(1),
                conversation_id: "conv-1".to_owned(),
                event: json!({
                    "type": "tool_execution_start",
                    "tool_call_id": "call-1",
                    "tool_name": "read_file",
                    "args": {"overflow": 9_007_199_254_740_992u64}
                }),
            },
        };
        assert!(matches!(
            to_wire_frame(tool_start),
            Err(WireError::AnyJSONNumberOutOfRange)
        ));

        let tool_call_end = OutboundFrame::Event {
            envelope: Envelope {
                seq: None,
                conversation_id: "conv-1".to_owned(),
                event: json!({
                    "type": "message_update",
                    "message_id": "00000000-0000-4000-8000-000000000003",
                    "event": {
                        "type": "tool_call_end",
                        "content_index": 0,
                        "tool_call": {
                            "id": "call-1",
                            "name": "read_file",
                            "arguments": {"overflow": 9_007_199_254_740_992u64}
                        }
                    }
                }),
            },
        };
        assert!(matches!(
            to_wire_frame(tool_call_end),
            Err(WireError::AnyJSONNumberOutOfRange)
        ));

        let approval_rule = json!({"overflow": 9_007_199_254_740_992u64});
        assert!(matches!(
            validate_json_safe_numbers(&approval_rule),
            Err(WireError::AnyJSONNumberOutOfRange)
        ));
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
    fn attachments_reject_non_empty_array() {
        let malformed_tail = br#"{"type":"user_message","text":"inspect","attachments":[null,{}]}"#;
        let error = serde_json::from_slice::<WireCommand>(malformed_tail)
            .expect_err("non-empty attachments must be rejected");
        assert!(error.to_string().contains("attachments must be empty"));
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
        assert_canonical_contract("CommandEnvelope", &serde_json::to_value(&wire).unwrap());
        let back: WireCommandEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(wire, back);
        assert_eq!(back.command_id(), "00000000-0000-4000-8000-000000000001");
    }

    #[test]
    fn command_envelope_requires_canonical_command_id() {
        for command_id in [
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".to_uppercase(),
            "{aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa}".to_owned(),
        ] {
            let value = json!({
                "seq": 1,
                "command_id": command_id,
                "command": {"type": "abort"},
            });
            assert!(
                serde_json::from_value::<WireCommandEnvelope>(value).is_err(),
                "non-canonical command_id must be rejected"
            );
        }
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
        assert_canonical_contract("OutboundFrame", &serde_json::to_value(&wire).unwrap());
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

        let non_rejected_with_null_reason = json!({
            "seq": 1,
            "command_id": valid_id,
            "status": "received",
            "reject_reason": null
        });
        assert!(serde_json::from_value::<WireCommandAck>(non_rejected_with_null_reason).is_err());

        let rejected_with_null_reason = json!({
            "seq": 1,
            "command_id": valid_id,
            "status": "rejected",
            "reject_reason": null
        });
        assert!(serde_json::from_value::<WireCommandAck>(rejected_with_null_reason).is_err());
    }

    #[test]
    fn wire_envelope_rejects_invalid_input() {
        let durable_missing_seq = json!({
            "conversation_id": "conv-1",
            "event": {"type": "agent_start"}
        });
        assert!(serde_json::from_value::<WireEnvelope>(durable_missing_seq).is_err());

        let durable_null_seq = json!({
            "seq": null,
            "conversation_id": "conv-1",
            "event": {"type": "agent_start"}
        });
        assert!(serde_json::from_value::<WireEnvelope>(durable_null_seq).is_err());

        let volatile_with_seq = json!({
            "seq": 1,
            "conversation_id": "conv-1",
            "event": {"type": "error", "message": "x"}
        });
        assert!(serde_json::from_value::<WireEnvelope>(volatile_with_seq).is_err());

        let volatile_null_seq = json!({
            "seq": null,
            "conversation_id": "conv-1",
            "event": {"type": "error", "message": "x"}
        });
        assert!(serde_json::from_value::<WireEnvelope>(volatile_null_seq).is_err());
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
            message: Some(Box::new(PublicMessage::User(UserMessage {
                content: vec![UserContent::Text {
                    text: "assistant context".to_owned(),
                }],
                timestamp: now(),
            }))),
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
        assert_canonical_contract("AgentEvent", &json);
        let back: WireAgentEvent = serde_json::from_value(json).unwrap();
        assert_eq!(wire, back);
    }

    #[test]
    fn tool_execution_start_args_must_be_an_object() {
        for args in [json!(null), json!([]), json!("scalar")] {
            let value = json!({
                "type": "tool_execution_start",
                "tool_call_id": "call-1",
                "tool_name": "read_file",
                "args": args,
            });
            assert!(
                serde_json::from_value::<WireAgentEvent>(value).is_err(),
                "non-object tool args must be rejected by the wire DTO"
            );
        }

        let error = WireAgentEvent::try_from(AgentEvent::ToolExecutionStart {
            tool_call_id: "call-1".to_owned(),
            tool_name: "read_file".to_owned(),
            args: json!([]),
        })
        .expect_err("internal non-object args must not cross the wire boundary");
        assert!(matches!(error, WireError::NonObjectToolExecutionArgs));
    }

    #[test]
    fn canonical_schema_distinguishes_nested_and_top_level_tool_results() {
        let payload = json!({
            "tool_call_id": "call-1",
            "tool_name": "read_file",
            "content": [{"type": "text", "text": "contents"}],
            "details": {"exit": 0},
            "is_error": false,
            "timestamp": now().to_rfc3339(),
        });
        let nested_event = json!({
            "type": "turn_end",
            "message": null,
            "tool_results": [payload.clone()],
        });
        assert_canonical_contract("AgentEvent", &nested_event);
        assert!(
            !canonical_contract_is_valid("PublicMessage", &payload),
            "nested tool result payload must not be accepted as a top-level message"
        );

        let mut top_level = payload.as_object().cloned().expect("object payload");
        top_level.insert("role".to_owned(), json!("tool_result"));
        assert_canonical_contract("PublicMessage", &Value::Object(top_level));
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
        assert_canonical_contract("PublicStreamEvent", &json);
        let back: WirePublicStreamEvent = serde_json::from_value(json).unwrap();
        assert_eq!(wire, back);
    }

    #[test]
    fn content_index_is_bounded_to_json_safe_integer_range() {
        let event = json!({
            "type": "text_start",
            "content_index": MAX_JSON_SAFE_INTEGER + 1,
        });
        assert!(serde_json::from_value::<WirePublicStreamEvent>(event).is_err());

        let event = PublicStreamEvent::TextStart {
            content_index: usize::MAX,
        };
        if usize::BITS > 53 {
            assert!(matches!(
                WirePublicStreamEvent::try_from(event),
                Err(WireError::ContentIndexOutOfRange(_))
            ));
        } else {
            assert!(WirePublicStreamEvent::try_from(event).is_ok());
        }
    }

    #[test]
    fn usage_values_are_bounded_to_json_safe_integer_range() {
        let value = json!({
            "input": MAX_JSON_SAFE_INTEGER + 1,
            "output": 0,
            "cache_read": 0,
            "cache_write": 0,
            "reasoning": 0,
            "total_tokens": 0,
        });
        assert!(serde_json::from_value::<WireUsage>(value).is_err());

        let mut internal = usage();
        internal.total_tokens = MAX_JSON_SAFE_INTEGER + 1;
        assert!(matches!(
            WireUsage::try_from(internal),
            Err(WireError::UsageValueOutOfRange(_))
        ));
    }

    #[test]
    fn json_safe_integer_fields_reject_overflow_with_correct_variant() {
        let retry = AgentEvent::RetryScheduled {
            attempt: 1,
            delay_ms: MAX_JSON_SAFE_INTEGER + 1,
            retry_at: now(),
            error_message: "rate limited".to_owned(),
        };
        assert!(matches!(
            WireAgentEvent::try_from(retry),
            Err(WireError::JsonSafeIntegerOutOfRange(_))
        ));

        let mut inbound = json!({
            "type": "retry_scheduled",
            "attempt": 1,
            "delay_ms": MAX_JSON_SAFE_INTEGER + 1,
            "retry_at": now().to_rfc3339(),
            "error_message": "x",
        });
        assert!(serde_json::from_value::<WireAgentEvent>(inbound.clone()).is_err());

        inbound["delay_ms"] = 1.into();
        inbound["attempt"] = (MAX_JSON_SAFE_INTEGER + 1).into();
        assert!(serde_json::from_value::<WireAgentEvent>(inbound).is_err());
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
        assert_canonical_contract("PublicMessage", &json);
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

    #[test]
    fn approval_decision_rejects_non_object_deferred_rule() {
        for bad in [json!([]), json!("literal"), json!(null)] {
            let err = WireApprovalDecision::try_from(ApprovalDecision::ApproveAlways {
                rule: DeferredApprovalRule(bad),
            })
            .expect_err("non-object deferred approval rule must be rejected");
            assert!(matches!(err, WireError::NonObjectApprovalRule));
        }
    }

    #[test]
    fn approval_decision_converts_all_branches() {
        assert_eq!(
            WireApprovalDecision::try_from(ApprovalDecision::ApproveOnce).unwrap(),
            WireApprovalDecision::ApproveOnce {}
        );
        assert_eq!(
            WireApprovalDecision::try_from(ApprovalDecision::Deny).unwrap(),
            WireApprovalDecision::Deny {}
        );

        let object = json!({"tool_name": "test"});
        assert_eq!(
            WireApprovalDecision::try_from(ApprovalDecision::ApproveAlways {
                rule: DeferredApprovalRule(object.clone()),
            })
            .unwrap(),
            WireApprovalDecision::ApproveAlways {
                rule: object.as_object().cloned().unwrap(),
            }
        );
    }

    fn round_trip_command(command: Command) {
        let wire = WireCommand::try_from(command).unwrap();
        let json = serde_json::to_value(&wire).unwrap();
        assert_canonical_contract("Command", &json);
        let back: WireCommand = serde_json::from_value(json).unwrap();
        assert_eq!(wire, back);
    }

    #[test]
    fn message_event_message_id_is_canonical_uuid() {
        let valid_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
        let user_message = PublicMessage::User(UserMessage {
            content: vec![UserContent::Text {
                text: "hi".to_owned(),
            }],
            timestamp: now(),
        });
        let user_message_json = serde_json::to_value(&user_message).unwrap();

        for event_type in ["message_start", "message_update", "message_end"] {
            let mut payload = match event_type {
                "message_start" => json!({
                    "type": "message_start",
                    "message_id": "__ID__",
                    "message": user_message_json.clone(),
                }),
                "message_update" => json!({
                    "type": "message_update",
                    "message_id": "__ID__",
                    "event": {
                        "type": "text_delta",
                        "content_index": 0,
                        "delta": "d",
                    },
                }),
                "message_end" => json!({
                    "type": "message_end",
                    "message_id": "__ID__",
                    "message": user_message_json.clone(),
                }),
                _ => unreachable!(),
            };

            for bad_id in [
                "not-a-uuid".to_owned(),
                valid_id.to_uppercase(),
                format!("{{{valid_id}}}"),
            ] {
                payload
                    .as_object_mut()
                    .unwrap()
                    .insert("message_id".to_owned(), json!(bad_id));
                assert!(
                    serde_json::from_value::<WireAgentEvent>(payload.clone()).is_err(),
                    "{event_type} must reject non-canonical message_id {bad_id}"
                );
            }

            payload
                .as_object_mut()
                .unwrap()
                .insert("message_id".to_owned(), json!(valid_id));
            let wire: WireAgentEvent = serde_json::from_value(payload).unwrap();
            let json = serde_json::to_value(&wire).unwrap();
            assert_canonical_contract("AgentEvent", &json);
            assert_eq!(json["message_id"].as_str().unwrap(), valid_id);
        }

        // Outbound conversion also validates the internal message_id.
        for bad_id in ["not-a-uuid".to_owned(), valid_id.to_uppercase()] {
            let start = AgentEvent::MessageStart {
                message_id: bad_id.clone(),
                message: Box::new(user_message.clone()),
            };
            assert!(
                matches!(
                    WireAgentEvent::try_from(start),
                    Err(WireError::InvalidMessageId(id)) if id == bad_id
                ),
                "message_start outbound must reject {bad_id}"
            );

            let update = AgentEvent::MessageUpdate {
                message_id: bad_id.clone(),
                event: PublicStreamEvent::TextDelta {
                    content_index: 0,
                    delta: "d".to_owned(),
                },
            };
            assert!(
                matches!(
                    WireAgentEvent::try_from(update),
                    Err(WireError::InvalidMessageId(id)) if id == bad_id
                ),
                "message_update outbound must reject {bad_id}"
            );

            let end = AgentEvent::MessageEnd {
                message_id: bad_id.clone(),
                message: Box::new(user_message.clone()),
            };
            assert!(
                matches!(
                    WireAgentEvent::try_from(end),
                    Err(WireError::InvalidMessageId(id)) if id == bad_id
                ),
                "message_end outbound must reject {bad_id}"
            );
        }
    }

    #[test]
    fn required_nullable_fields_reject_omission() {
        // TurnEnd.message is required and nullable.
        let missing_message = json!({
            "type": "turn_end",
            "tool_results": []
        });
        assert!(
            serde_json::from_value::<WireAgentEvent>(missing_message).is_err(),
            "turn_end must reject omitted message"
        );

        let null_message = json!({
            "type": "turn_end",
            "message": null,
            "tool_results": []
        });
        let event: WireAgentEvent = serde_json::from_value(null_message).unwrap();
        assert!(
            matches!(event, WireAgentEvent::TurnEnd { message, .. } if message.as_ref().is_none()),
            "turn_end must accept explicit null message"
        );

        // Assistant error_message and provider_code are required and nullable.
        let base_assistant = json!({
            "role": "assistant",
            "content": [],
            "model": "kimi-k3",
            "provider": "moonshot",
            "origin": {
                "provider_instance_id": "p",
                "protocol": "open_ai_chat_completions",
                "model": "kimi-k3"
            },
            "usage": {
                "input": 0,
                "output": 0,
                "cache_read": 0,
                "cache_write": 0,
                "reasoning": 0,
                "total_tokens": 0
            },
            "stop_reason": "stop",
            "interrupted": false,
            "timestamp": now().to_rfc3339()
        });

        for omitted in ["error_message", "provider_code"] {
            let mut payload = base_assistant.as_object().cloned().unwrap();
            payload.remove(omitted);
            assert!(
                serde_json::from_value::<WirePublicMessage>(Value::Object(payload)).is_err(),
                "assistant must reject omitted {omitted}"
            );
        }

        for (error_message, provider_code) in
            [(Value::Null, Value::Null), (json!("err"), json!("code"))]
        {
            let mut payload = base_assistant.as_object().cloned().unwrap();
            payload.insert("error_message".to_owned(), error_message);
            payload.insert("provider_code".to_owned(), provider_code);
            let wire: WirePublicMessage = serde_json::from_value(Value::Object(payload)).unwrap();
            assert!(
                matches!(wire, WirePublicMessage::Assistant { .. }),
                "assistant must accept explicit null or string values"
            );
        }
    }

    #[test]
    fn wire_date_time_fields_reject_malformed_strings() {
        let bad_timestamp = "not-a-date";

        // WirePublicMessage::User
        let mut user = json!({
            "role": "user",
            "content": [{"type": "text", "text": "hi"}],
            "timestamp": now().to_rfc3339()
        });
        assert!(
            serde_json::from_value::<WirePublicMessage>(user.clone()).is_ok(),
            "user must accept a valid RFC3339 timestamp"
        );
        user["timestamp"] = json!(bad_timestamp);
        assert!(
            serde_json::from_value::<WirePublicMessage>(user).is_err(),
            "user must reject a malformed timestamp"
        );

        // WirePublicMessage::ToolResult
        let mut tool_result = json!({
            "role": "tool_result",
            "tool_call_id": "00000000-0000-4000-8000-000000000001",
            "tool_name": "read",
            "content": [{"type": "text", "text": "x"}],
            "details": null,
            "is_error": false,
            "timestamp": now().to_rfc3339()
        });
        assert!(
            serde_json::from_value::<WirePublicMessage>(tool_result.clone()).is_ok(),
            "tool_result must accept a valid RFC3339 timestamp"
        );
        tool_result["timestamp"] = json!(bad_timestamp);
        assert!(
            serde_json::from_value::<WirePublicMessage>(tool_result).is_err(),
            "tool_result must reject a malformed timestamp"
        );

        // WireToolResultMessage (payload used inside TurnEnd.tool_results)
        let mut payload = json!({
            "tool_call_id": "00000000-0000-4000-8000-000000000001",
            "tool_name": "read",
            "content": [{"type": "text", "text": "x"}],
            "details": null,
            "is_error": false,
            "timestamp": now().to_rfc3339()
        });
        assert!(
            serde_json::from_value::<WireToolResultMessage>(payload.clone()).is_ok(),
            "tool_result payload must accept a valid RFC3339 timestamp"
        );
        payload["timestamp"] = json!(bad_timestamp);
        assert!(
            serde_json::from_value::<WireToolResultMessage>(payload).is_err(),
            "tool_result payload must reject a malformed timestamp"
        );

        // WirePublicMessage::Assistant
        let mut assistant = json!({
            "role": "assistant",
            "content": [],
            "model": "kimi-k3",
            "provider": "moonshot",
            "origin": {
                "provider_instance_id": "p",
                "protocol": "open_ai_chat_completions",
                "model": "kimi-k3"
            },
            "usage": {
                "input": 0,
                "output": 0,
                "cache_read": 0,
                "cache_write": 0,
                "reasoning": 0,
                "total_tokens": 0
            },
            "stop_reason": "stop",
            "error_message": null,
            "provider_code": null,
            "interrupted": false,
            "timestamp": now().to_rfc3339()
        });
        assert!(
            serde_json::from_value::<WirePublicMessage>(assistant.clone()).is_ok(),
            "assistant must accept a valid RFC3339 timestamp"
        );
        assistant["timestamp"] = json!(bad_timestamp);
        assert!(
            serde_json::from_value::<WirePublicMessage>(assistant).is_err(),
            "assistant must reject a malformed timestamp"
        );

        // WireAgentEvent::RetryScheduled
        let mut retry = json!({
            "type": "retry_scheduled",
            "attempt": 1,
            "delay_ms": 100,
            "retry_at": now().to_rfc3339(),
            "error_message": "x"
        });
        assert!(
            serde_json::from_value::<WireAgentEvent>(retry.clone()).is_ok(),
            "retry_scheduled must accept a valid RFC3339 retry_at"
        );
        retry["retry_at"] = json!(bad_timestamp);
        assert!(
            serde_json::from_value::<WireAgentEvent>(retry).is_err(),
            "retry_scheduled must reject a malformed retry_at"
        );
    }

    #[test]
    fn try_from_command_rejects_non_empty_attachments() {
        let non_empty = Command::UserMessage {
            text: "inspect".to_owned(),
            attachments: vec![Attachment(json!({"name": "secret.txt"}))],
        };
        assert!(
            matches!(
                WireCommand::try_from(non_empty),
                Err(WireError::NonEmptyAttachments)
            ),
            "TryFrom<Command> must reject non-empty attachments"
        );
    }

    #[test]
    fn try_from_command_envelope_rejects_non_empty_attachments() {
        let envelope = CommandEnvelope {
            seq: 1,
            command_id: uuid_command_id(),
            command: Command::UserMessage {
                text: "inspect".to_owned(),
                attachments: vec![Attachment(json!({"name": "secret.txt"}))],
            },
        };
        assert!(
            matches!(
                WireCommandEnvelope::try_from(envelope),
                Err(WireError::NonEmptyAttachments)
            ),
            "TryFrom<CommandEnvelope> must reject non-empty attachments"
        );
    }

    fn assert_unit_variant_rejects_extras<T>(
        name: &str,
        extra: Value,
        canonical: Value,
        definition: &str,
    ) where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        assert!(
            serde_json::from_value::<T>(extra).is_err(),
            "{name} must reject extra fields"
        );
        let parsed: T = serde_json::from_value(canonical.clone())
            .unwrap_or_else(|_| panic!("{name} canonical form must parse"));
        let back =
            serde_json::to_value(&parsed).unwrap_or_else(|_| panic!("{name} must serialize"));
        assert_eq!(back, canonical, "{name} canonical round trip");
        assert_canonical_contract(definition, &back);
    }

    #[test]
    fn unit_wire_variants_reject_unknown_fields_and_round_trip() {
        assert_unit_variant_rejects_extras::<WireCommand>(
            "WireCommand::Abort",
            json!({"type": "abort", "extra": true}),
            json!({"type": "abort"}),
            "Command",
        );

        assert_unit_variant_rejects_extras::<WireAgentEvent>(
            "WireAgentEvent::AgentStart",
            json!({"type": "agent_start", "extra": true}),
            json!({"type": "agent_start"}),
            "AgentEvent",
        );
        assert_unit_variant_rejects_extras::<WireAgentEvent>(
            "WireAgentEvent::AgentEnd",
            json!({"type": "agent_end", "extra": true}),
            json!({"type": "agent_end"}),
            "AgentEvent",
        );
        assert_unit_variant_rejects_extras::<WireAgentEvent>(
            "WireAgentEvent::TurnStart",
            json!({"type": "turn_start", "extra": true}),
            json!({"type": "turn_start"}),
            "AgentEvent",
        );

        assert_unit_variant_rejects_extras::<WireApprovalDecision>(
            "WireApprovalDecision::ApproveOnce",
            json!({"type": "approve_once", "extra": true}),
            json!({"type": "approve_once"}),
            "ApprovalDecision",
        );
        assert_unit_variant_rejects_extras::<WireApprovalDecision>(
            "WireApprovalDecision::Deny",
            json!({"type": "deny", "extra": true}),
            json!({"type": "deny"}),
            "ApprovalDecision",
        );
    }
    #[test]
    fn contract_fixtures_round_trip_through_wire_types() {
        use std::path::PathBuf;

        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let path = manifest.join("../../contracts/agent-events-fixtures.json");
        let raw = std::fs::read_to_string(&path).expect("read fixtures");
        let fixtures: Value = serde_json::from_str(&raw).expect("parse fixtures");
        let fixtures = fixtures.as_object().expect("fixtures object");

        let mut passed = 0;
        for (name, fixture) in fixtures {
            let kind = fixture
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let wire = fixture.get("wire").expect("wire field").clone();

            match kind {
                "outbound_frame" => round_trip_value::<WireOutboundFrame>(name, &wire),
                "command_envelope" => round_trip_value::<WireCommandEnvelope>(name, &wire),
                "agent_hello" => round_trip_value::<AgentHello>(name, &wire),
                "api_hello" => round_trip_value::<ApiHello>(name, &wire),
                "agent_event" => round_trip_value::<WireAgentEvent>(name, &wire),
                "public_message" => round_trip_value::<WirePublicMessage>(name, &wire),
                other => panic!("unknown fixture kind '{other}' for '{name}'"),
            }
            passed += 1;
        }
        assert!(passed >= 10, "expected at least 10 fixtures, got {passed}");
    }

    #[test]
    fn seq_exceeds_json_safe_integer_is_rejected() {
        let oversized = json!({
            "seq": MAX_JSON_SAFE_INTEGER + 1,
            "command_id": "00000000-0000-4000-8000-000000000001",
            "command": { "type": "abort" }
        });
        assert!(
            serde_json::from_value::<WireCommandEnvelope>(oversized).is_err(),
            "command envelope seq above JSON-safe max must be rejected"
        );

        let ack = json!({
            "seq": MAX_JSON_SAFE_INTEGER + 1,
            "command_id": "00000000-0000-4000-8000-000000000001",
            "status": "received"
        });
        assert!(
            serde_json::from_value::<WireCommandAck>(ack).is_err(),
            "command ack seq above JSON-safe max must be rejected"
        );

        let event = json!({
            "seq": MAX_JSON_SAFE_INTEGER + 1,
            "conversation_id": "conversation-1",
            "event": { "type": "agent_start" }
        });
        assert!(
            serde_json::from_value::<WireEnvelope>(event).is_err(),
            "envelope seq above JSON-safe max must be rejected"
        );
    }

    fn round_trip_value<T: for<'de> Deserialize<'de> + Serialize>(name: &str, wire: &Value) {
        let typed: T = serde_json::from_value(wire.clone())
            .unwrap_or_else(|e| panic!("deserialize fixture '{name}' failed: {e}"));
        let serialized = serde_json::to_value(&typed)
            .unwrap_or_else(|e| panic!("serialize fixture '{name}' failed: {e}"));
        let normalized_original = normalize_fixture(wire.clone());
        let normalized_roundtrip = normalize_fixture(serialized);
        assert_eq!(
            normalized_original, normalized_roundtrip,
            "fixture '{name}' round-trip mismatch"
        );
    }

    fn normalize_fixture(value: Value) -> Value {
        match value {
            Value::Object(map) => {
                let sorted: std::collections::BTreeMap<String, Value> = map
                    .into_iter()
                    .map(|(k, v)| (k, normalize_fixture(v)))
                    .collect();
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(arr) => Value::Array(arr.into_iter().map(normalize_fixture).collect()),
            other => other,
        }
    }

    #[test]
    fn from_json_bytes_rejects_duplicate_keys_in_wire_inputs() {
        let command_envelope = br#"{"seq":1,"seq":2,"command_id":"00000000-0000-4000-8000-000000000001","command":{"type":"abort"}}"#;
        let err = from_json_bytes::<WireCommandEnvelope>(command_envelope).unwrap_err();
        assert!(err.to_string().contains("duplicate object key"), "{err}");

        let command_ack = br#"{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","command_id":"00000000-0000-4000-8000-000000000002","status":"received"}"#;
        let err = from_json_bytes::<WireCommandAck>(command_ack).unwrap_err();
        assert!(err.to_string().contains("duplicate object key"), "{err}");

        let envelope = br#"{"seq":1,"conversation_id":"c","conversation_id":"d","event":{"type":"agent_start"}}"#;
        let err = from_json_bytes::<WireEnvelope>(envelope).unwrap_err();
        assert!(err.to_string().contains("duplicate object key"), "{err}");
    }

    #[test]
    fn from_json_bytes_rejects_duplicate_keys_in_api_hello() {
        let api_hello = br#"{"accepted_generation":1,"accepted_generation":2,"last_received_event_seq":0,"next_command_seq":1}"#;
        let err = from_json_bytes::<ApiHello>(api_hello).unwrap_err();
        assert!(err.to_string().contains("duplicate object key"), "{err}");
    }

    #[test]
    fn from_json_bytes_rejects_trailing_tokens() {
        let trailing = br#"{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","command":{"type":"abort"}}extra"#;
        let err = from_json_bytes::<WireCommandEnvelope>(trailing).unwrap_err();
        assert!(err.to_string().contains("trailing"), "{err}");
    }
}
