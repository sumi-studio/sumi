use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    gateway::ApprovalDecision,
    provider::types::{
        PublicMessage, RejectedToolCall, ToolArgsPreview, ToolCall, ToolResultMessage,
    },
};

/// Canonical public agent event carried on the wire and stored as the raw
/// durable event. Runtime correlation and lifecycle state are deliberately not
/// fields of this type; EventWriter stores those in typed internal columns.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum AgentEvent {
    AgentStart,
    AgentEnd,
    TurnStart,
    TurnEnd {
        message: Option<Box<PublicMessage>>,
        tool_results: Vec<ToolResultMessage>,
    },
    MessageStart {
        message_id: String,
        message: Box<PublicMessage>,
    },
    MessageUpdate {
        message_id: String,
        event: PublicStreamEvent,
    },
    MessageEnd {
        message_id: String,
        message: Box<PublicMessage>,
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
        request: ApprovalRequest,
    },
    ApprovalResolved {
        request_id: String,
        resolution: ApprovalResolution,
    },
    Steered {
        mode: SteerMode,
    },
    MemoryMaintenance {
        kind: MemoryMaintKind,
    },
    RetryScheduled {
        attempt: u32,
        delay_ms: u64,
        retry_at: DateTime<Utc>,
        error_message: String,
    },
    CommandDisposition(CommandDispositionEvent),
    Error {
        message: String,
    },
}

impl AgentEvent {
    pub(crate) fn durable_kind(&self) -> Option<&'static str> {
        Some(match self {
            Self::AgentStart => "agent_start",
            Self::AgentEnd => "agent_end",
            Self::TurnStart => "turn_start",
            Self::TurnEnd { .. } => "turn_end",
            Self::MessageStart { .. } => "message_start",
            Self::MessageEnd { .. } => "message_end",
            Self::ToolExecutionStart { .. } => "tool_execution_start",
            Self::ToolExecutionEnd { .. } => "tool_execution_end",
            Self::ApprovalRequested { .. } => "approval_requested",
            Self::ApprovalResolved { .. } => "approval_resolved",
            Self::Steered { .. } => "steered",
            Self::MemoryMaintenance { .. } => "memory_maintenance",
            Self::RetryScheduled { .. } => "retry_scheduled",
            Self::CommandDisposition(_) => "command_disposition",
            Self::MessageUpdate { .. } | Self::ToolExecutionUpdate { .. } | Self::Error { .. } => {
                return None;
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct CommandDispositionEvent {
    pub(crate) command_id: String,
    pub(crate) command_seq: u64,
    #[serde(flatten)]
    pub(crate) disposition: CommandDisposition,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CommandDisposition {
    Applied {},
    Superseded {},
    Rejected {
        reject_reason: CommandDispositionRejectReason,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CommandDispositionRejectReason {
    UnknownCommand,
    SchemaViolation,
    AttachmentsNotEmpty,
    Oversized,
    NotAllowed,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PublicStreamEvent {
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
        preview: ToolArgsPreview,
    },
    ToolCallEnd {
        content_index: usize,
        tool_call: ToolCall,
    },
    ToolCallRejected {
        content_index: usize,
        rejected: RejectedToolCall,
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

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalRequest {
    pub id: String,
    pub tool_call_id: String,
    pub tool_name: String,
    pub action: ReviewProjection,
    pub args_summary: Value,
    pub reason: Option<String>,
    pub audit: Option<AuditDecision>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewProjection {
    Reviewable(Value),
    InsufficientEvidence { reason: String },
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuditDecision {
    pub outcome: AuditOutcome,
    pub risk: RiskLevel,
    pub authorization: UserAuthorization,
    pub rationale: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Allow,
    Deny,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UserAuthorization {
    Unknown,
    Low,
    Medium,
    High,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ApprovalResolution {
    Decision(ApprovalDecision),
    /// The Human made this exact current-call decision, but the foundation
    /// refused to start the operation after commit-time reauthorization. This
    /// is an execution disposition, not a rewritten Human denial.
    Rejected {
        decision: ApprovalDecision,
    },
    Cancelled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SteerMode {
    Hard,
    Soft,
}

/// T17 owns the `MemoryMaintKind` vocabulary. T12 must deserialize the public
/// envelope so it can reject premature writes without inventing T17 variants.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(transparent)]
pub(crate) struct MemoryMaintKind(String);

impl MemoryMaintKind {
    pub(crate) fn new(kind: impl Into<String>) -> Self {
        Self(kind.into())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::AgentEvent;

    #[test]
    fn command_disposition_round_trips_under_closed_durable_schema() {
        let raw = json!({
            "type": "command_disposition",
            "command_id": "00000000-0000-4000-8000-000000000001",
            "command_seq": 1,
            "status": "rejected",
            "reject_reason": "schema_violation"
        });
        let event: AgentEvent =
            serde_json::from_value(raw.clone()).expect("deserialize durable command disposition");
        assert_eq!(
            serde_json::to_value(event).expect("serialize durable command disposition"),
            raw
        );

        let with_private_payload = json!({
            "type": "command_disposition",
            "command_id": "00000000-0000-4000-8000-000000000001",
            "command_seq": 1,
            "status": "applied",
            "provenance": {"source": "browser"}
        });
        assert!(
            serde_json::from_value::<AgentEvent>(with_private_payload).is_err(),
            "closed durable event must reject private provenance"
        );
    }
}
