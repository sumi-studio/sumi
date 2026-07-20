use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    hash::Hash,
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, Transaction};
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};

use crate::{
    agent::{AgentEvent, ApprovalRequest, ApprovalResolution, SteerMode},
    gateway::{
        ApprovalDecision, Command, CommandAck, CommandAckStatus, CommandEnvelope, CommandId,
        CommandRejectReason, InboundCommand, KeyedCommandDigest, RejectedCommandPayload,
    },
    provider::types::{PublicMessage, StopReason, ToolResultMessage},
};

use super::{
    BatchBounds, DURABLE_ROW_OVERHEAD_BYTES, DataKeyPurpose, EventBatchSizer, InjectionApplication,
    InjectionBatchSizeInput, InjectionCommandSizeInput, PublicProjectionBuilder, Redactor, Store,
    event_log::{
        EVENT_DIGEST_BYTES, EventChainEntry, authenticate_event_head, extend_event_chain,
        verify_event_head,
    },
    keyed_digest,
    redactor::search_text_from_projection,
    verify_keyed_digest,
};

const PREPARED_KEY_MATERIAL_PROOF: &[u8] = b"\0sumi/event-batch/prepared-key-material/v1";

#[derive(Clone)]
pub(crate) struct DurableEvent {
    value: AgentEvent,
    metadata: DurableEventMetadata,
    raw_json: Vec<u8>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DurableEventMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) command_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) tool_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) tool_error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) approval_actor: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub(super) empty_turn: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl DurableEvent {
    #[allow(
        dead_code,
        reason = "constructed by the T15 run loop through the T12-frozen boundary"
    )]
    #[cfg(test)]
    pub(crate) fn new(event: &Value) -> Result<Self> {
        let (value, metadata) = normalize_test_event(event.clone())?;
        Self::from_parts(value, metadata)
    }

    fn from_parts(value: AgentEvent, metadata: DurableEventMetadata) -> Result<Self> {
        let kind = value
            .durable_kind()
            .ok_or_else(|| anyhow!("durable event does not match the closed T12 schema"))?;
        if kind == "memory_maintenance" {
            bail!(
                "durable event does not match the closed T12 schema: MemoryMaintenance is owned by T17"
            );
        }
        if matches!(&value, AgentEvent::ToolExecutionStart { args, .. } if !args.is_object()) {
            bail!(
                "durable event does not match the closed T12 schema: tool args must be an object"
            );
        }
        Ok(Self {
            raw_json: serde_json::to_vec(&value)
                .context("failed to serialize typed durable event")?,
            value,
            metadata,
        })
    }

    #[allow(dead_code, reason = "T15 consumes the T12-frozen lifecycle builders")]
    pub(crate) fn agent_start(run_id: impl Into<String>) -> Result<Self> {
        Self::from_parts(
            AgentEvent::AgentStart,
            DurableEventMetadata {
                run_id: Some(run_id.into()),
                ..DurableEventMetadata::default()
            },
        )
    }

    fn agent_end(run_id: impl Into<String>) -> Result<Self> {
        Self::from_parts(
            AgentEvent::AgentEnd,
            DurableEventMetadata {
                run_id: Some(run_id.into()),
                ..DurableEventMetadata::default()
            },
        )
    }

    #[allow(dead_code, reason = "T15 consumes the T12-frozen lifecycle builders")]
    pub(crate) fn turn_start(
        run_id: impl Into<String>,
        turn_id: impl Into<String>,
    ) -> Result<Self> {
        Self::from_parts(
            AgentEvent::TurnStart,
            DurableEventMetadata {
                run_id: Some(run_id.into()),
                turn_id: Some(turn_id.into()),
                ..DurableEventMetadata::default()
            },
        )
    }

    #[allow(dead_code, reason = "T15 consumes the T12-frozen lifecycle builders")]
    pub(crate) fn turn_end(
        run_id: impl Into<String>,
        turn_id: impl Into<String>,
        message: PublicMessage,
        tool_results: Vec<ToolResultMessage>,
    ) -> Result<Self> {
        Self::from_parts(
            AgentEvent::TurnEnd {
                message: Some(Box::new(message)),
                tool_results,
            },
            DurableEventMetadata {
                run_id: Some(run_id.into()),
                turn_id: Some(turn_id.into()),
                ..DurableEventMetadata::default()
            },
        )
    }

    fn empty_turn_end(run_id: impl Into<String>, turn_id: impl Into<String>) -> Result<Self> {
        Self::from_parts(
            AgentEvent::TurnEnd {
                message: None,
                tool_results: Vec::new(),
            },
            DurableEventMetadata {
                run_id: Some(run_id.into()),
                turn_id: Some(turn_id.into()),
                empty_turn: true,
                ..DurableEventMetadata::default()
            },
        )
    }

    #[allow(
        dead_code,
        reason = "T12 canonical serializer is consumed by the T15 run-loop event builders"
    )]
    pub(crate) fn message(
        event_type: &'static str,
        message_id: &str,
        message: &PublicMessage,
    ) -> Result<Self> {
        let value = match event_type {
            "message_start" => AgentEvent::MessageStart {
                message_id: message_id.to_owned(),
                message: Box::new(message.clone()),
            },
            "message_end" => AgentEvent::MessageEnd {
                message_id: message_id.to_owned(),
                message: Box::new(message.clone()),
            },
            _ => bail!("unsupported durable message event type {event_type}"),
        };
        Self::from_parts(value, DurableEventMetadata::default())
    }

    #[allow(dead_code, reason = "T15 consumes the T12-frozen tool builder")]
    pub(crate) fn tool_execution_start(
        tool_call_id: String,
        tool_name: String,
        args: Value,
    ) -> Result<Self> {
        Self::from_parts(
            AgentEvent::ToolExecutionStart {
                tool_call_id,
                tool_name,
                args,
            },
            DurableEventMetadata {
                tool_state: Some("running".to_owned()),
                ..DurableEventMetadata::default()
            },
        )
    }

    #[allow(dead_code, reason = "T15 consumes the T12-frozen tool builder")]
    pub(crate) fn tool_execution_end(
        tool_call_id: String,
        result: Value,
        is_error: bool,
        state: String,
        error_code: Option<String>,
    ) -> Result<Self> {
        Self::from_parts(
            AgentEvent::ToolExecutionEnd {
                tool_call_id,
                result,
                is_error,
            },
            DurableEventMetadata {
                tool_state: Some(state),
                tool_error_code: error_code,
                ..DurableEventMetadata::default()
            },
        )
    }

    #[allow(dead_code, reason = "T15 consumes the T12-frozen approval builder")]
    pub(crate) fn approval_requested(request: ApprovalRequest) -> Result<Self> {
        Self::from_parts(
            AgentEvent::ApprovalRequested { request },
            DurableEventMetadata::default(),
        )
    }

    #[allow(dead_code, reason = "T15 consumes the T12-frozen approval builder")]
    pub(crate) fn approval_resolved(
        request_id: String,
        resolution: ApprovalResolution,
        actor: String,
    ) -> Result<Self> {
        Self::from_parts(
            AgentEvent::ApprovalResolved {
                request_id,
                resolution,
            },
            DurableEventMetadata {
                approval_actor: Some(actor),
                ..DurableEventMetadata::default()
            },
        )
    }

    #[allow(dead_code, reason = "T15 consumes the T12-frozen steer builder")]
    pub(crate) fn steered(
        mode: SteerMode,
        command_id: String,
        run_id: String,
        turn_id: String,
    ) -> Result<Self> {
        Self::from_parts(
            AgentEvent::Steered { mode },
            DurableEventMetadata {
                command_id: Some(command_id),
                run_id: Some(run_id),
                turn_id: Some(turn_id),
                ..DurableEventMetadata::default()
            },
        )
    }

    #[cfg(test)]
    fn from_raw(raw_json: Vec<u8>) -> Result<Self> {
        let value: AgentEvent = serde_json::from_slice(&raw_json)
            .context("durable event does not match the closed T12 schema")?;
        Self::from_parts(value, DurableEventMetadata::default())
    }
}

impl std::fmt::Debug for DurableEvent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("DurableEvent")
            .field(&format_args!("[REDACTED {} bytes]", self.raw_json.len()))
            .finish()
    }
}

impl Drop for DurableEvent {
    fn drop(&mut self) {
        self.raw_json.zeroize();
    }
}

#[cfg(test)]
fn normalize_test_event(mut raw: Value) -> Result<(AgentEvent, DurableEventMetadata)> {
    let object = raw
        .as_object_mut()
        .ok_or_else(|| anyhow!("durable event must be an object"))?;
    let event_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("durable event must have a string type"))?
        .to_owned();
    let take_string =
        |object: &mut serde_json::Map<String, Value>, field: &str| match object.remove(field) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => serde_json::from_value::<String>(value).map(Some),
        };
    let mut metadata = DurableEventMetadata::default();
    match event_type.as_str() {
        "agent_start" | "agent_end" => {
            metadata.run_id = take_string(object, "run_id")?;
        }
        "turn_start" | "turn_end" => {
            metadata.run_id = take_string(object, "run_id")?;
            metadata.turn_id = take_string(object, "turn_id")?;
            if event_type == "turn_end" && !object.contains_key("message") {
                object.insert("message".to_owned(), Value::Null);
                object.insert("tool_results".to_owned(), Value::Array(Vec::new()));
                metadata.empty_turn = true;
            }
        }
        "tool_execution_start" => {
            metadata.tool_state = take_string(object, "state")?;
        }
        "tool_execution_end" => {
            metadata.tool_state = take_string(object, "state")?;
            metadata.tool_error_code = take_string(object, "error_code")?;
        }
        "approval_requested" => {
            if let Some(request) = object.get_mut("request").and_then(Value::as_object_mut) {
                let legacy_risk = request.remove("risk");
                request
                    .entry("tool_name")
                    .or_insert_with(|| Value::String("test".to_owned()));
                request.entry("action").or_insert_with(|| {
                    serde_json::json!({
                        "reviewable":{"risk":legacy_risk.unwrap_or(Value::Null)}
                    })
                });
                request
                    .entry("args_summary")
                    .or_insert_with(|| Value::Object(serde_json::Map::new()));
                request.entry("reason").or_insert(Value::Null);
                request.entry("audit").or_insert(Value::Null);
            }
        }
        "approval_resolved" => {
            metadata.approval_actor = take_string(object, "actor")?;
            if let Some(Value::String(resolution)) = object.get("resolution").cloned() {
                let canonical = match resolution.as_str() {
                    "cancelled" => serde_json::json!("cancelled"),
                    "approved_once" => {
                        serde_json::json!({"decision":{"type":"approve_once"}})
                    }
                    "approved_always" => serde_json::json!({
                        "decision":{"type":"approve_always","rule":{}}
                    }),
                    "denied" => serde_json::json!({"decision":{"type":"deny"}}),
                    _ => Value::String(resolution),
                };
                object.insert("resolution".to_owned(), canonical);
            }
        }
        "steered" => {
            metadata.command_id = take_string(object, "command_id")?;
            metadata.run_id = take_string(object, "run_id")?;
            metadata.turn_id = take_string(object, "turn_id")?;
            object
                .entry("mode")
                .or_insert_with(|| Value::String("soft".to_owned()));
        }
        _ => {}
    }
    let value: AgentEvent = serde_json::from_value(raw.clone())
        .context("durable event does not match the closed T12 schema")?;
    if serde_json::to_value(&value)? != raw {
        bail!("durable event does not match the closed T12 schema: non-canonical fields");
    }
    Ok((value, metadata))
}

pub(super) struct DurableEventIdentity<'a> {
    pub(super) kind: &'static str,
    pub(super) command_id: Option<&'a str>,
    pub(super) run_id: Option<&'a str>,
    pub(super) turn_id: Option<&'a str>,
    pub(super) message_id: Option<&'a str>,
    pub(super) message_role: Option<&'static str>,
}

impl DurableEvent {
    pub(super) fn identity(&self) -> DurableEventIdentity<'_> {
        let empty = |kind| DurableEventIdentity {
            kind,
            command_id: None,
            run_id: None,
            turn_id: None,
            message_id: None,
            message_role: None,
        };
        match &self.value {
            AgentEvent::AgentStart => DurableEventIdentity {
                run_id: self.metadata.run_id.as_deref(),
                ..empty("agent_start")
            },
            AgentEvent::AgentEnd => DurableEventIdentity {
                run_id: self.metadata.run_id.as_deref(),
                ..empty("agent_end")
            },
            AgentEvent::TurnStart => DurableEventIdentity {
                run_id: self.metadata.run_id.as_deref(),
                turn_id: self.metadata.turn_id.as_deref(),
                ..empty("turn_start")
            },
            AgentEvent::TurnEnd { .. } => DurableEventIdentity {
                run_id: self.metadata.run_id.as_deref(),
                turn_id: self.metadata.turn_id.as_deref(),
                ..empty("turn_end")
            },
            AgentEvent::MessageStart {
                message_id,
                message,
            } => DurableEventIdentity {
                message_id: Some(message_id),
                message_role: Some(public_message_role(message)),
                ..empty("message_start")
            },
            AgentEvent::MessageEnd {
                message_id,
                message,
            } => DurableEventIdentity {
                message_id: Some(message_id),
                message_role: Some(public_message_role(message)),
                ..empty("message_end")
            },
            AgentEvent::ToolExecutionStart { .. } => empty("tool_execution_start"),
            AgentEvent::ToolExecutionEnd { .. } => empty("tool_execution_end"),
            AgentEvent::ApprovalRequested { .. } => empty("approval_requested"),
            AgentEvent::ApprovalResolved { .. } => empty("approval_resolved"),
            AgentEvent::Steered { .. } => DurableEventIdentity {
                command_id: self.metadata.command_id.as_deref(),
                run_id: self.metadata.run_id.as_deref(),
                turn_id: self.metadata.turn_id.as_deref(),
                ..empty("steered")
            },
            AgentEvent::RetryScheduled { .. } => empty("retry_scheduled"),
            AgentEvent::MemoryMaintenance { .. }
            | AgentEvent::MessageUpdate { .. }
            | AgentEvent::ToolExecutionUpdate { .. }
            | AgentEvent::Error { .. } => unreachable!("non-T12 durable event was rejected"),
        }
    }
}

fn public_message_role(message: &PublicMessage) -> &'static str {
    match message {
        PublicMessage::User(_) => "user",
        PublicMessage::Assistant(_) => "assistant",
        PublicMessage::ToolResult(_) => "tool_result",
    }
}

#[derive(Clone, Default)]
pub(crate) struct EventBatch {
    pub writes: Vec<EventWrite>,
    /// Durable identities for every user command injected by this batch, in
    /// the same order as the user MessageEnd projections. EventWriter obtains
    /// canonical plaintext from inbound_commands inside the write transaction;
    /// callers cannot assert command sizes or supply substitute plaintext.
    pub injected_commands: Vec<InjectedCommand>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InjectedCommand {
    seq: u64,
    command_id: CommandId,
    message_id: String,
}

/// T18 freezes this same value into the generated cross-language contracts.
pub(crate) const USER_MESSAGE_ID_NAMESPACE: Uuid = Uuid::from_bytes([
    0x78, 0xf6, 0x2d, 0x15, 0xb9, 0x45, 0x4a, 0x4f, 0x9d, 0x84, 0xd7, 0x3c, 0x7f, 0x93, 0x2b, 0x51,
]);

pub(crate) trait CanonicalCommandIdentity {
    fn canonical_command_uuid(&self) -> Uuid;
}

impl CanonicalCommandIdentity for CommandId {
    fn canonical_command_uuid(&self) -> Uuid {
        *self.as_uuid()
    }
}

pub(crate) trait IntoCanonicalCommandId {
    fn into_canonical_command_id(self) -> CommandId;
}

impl IntoCanonicalCommandId for CommandId {
    fn into_canonical_command_id(self) -> CommandId {
        self
    }
}

#[cfg(test)]
impl CanonicalCommandIdentity for str {
    fn canonical_command_uuid(&self) -> Uuid {
        CommandId::parse(self)
            .expect("test command_id must be a canonical UUID")
            .canonical_command_uuid()
    }
}

#[cfg(test)]
impl IntoCanonicalCommandId for &str {
    fn into_canonical_command_id(self) -> CommandId {
        CommandId::parse(self).expect("test command_id must be a canonical UUID")
    }
}

#[cfg(test)]
impl IntoCanonicalCommandId for String {
    fn into_canonical_command_id(self) -> CommandId {
        CommandId::parse(&self).expect("test command_id must be a canonical UUID")
    }
}

pub(crate) fn user_message_id(command_id: &(impl CanonicalCommandIdentity + ?Sized)) -> String {
    Uuid::new_v5(
        &USER_MESSAGE_ID_NAMESPACE,
        command_id.canonical_command_uuid().as_bytes(),
    )
    .to_string()
}

impl InjectedCommand {
    #[allow(
        dead_code,
        reason = "the T15 run loop consumes the T12-frozen private injection builder"
    )]
    pub(crate) fn new(seq: u64, command_id: impl IntoCanonicalCommandId) -> Self {
        let command_id = command_id.into_canonical_command_id();
        let message_id = user_message_id(&command_id);
        Self {
            seq,
            command_id,
            message_id,
        }
    }

    #[cfg(test)]
    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    #[cfg(test)]
    pub(crate) fn command_id(&self) -> &str {
        self.command_id.as_str()
    }

    #[cfg(test)]
    pub(crate) fn message_id(&self) -> &str {
        &self.message_id
    }

    #[cfg(test)]
    fn with_caller_message_id(
        seq: u64,
        command_id: CommandId,
        message_id: impl Into<String>,
    ) -> Self {
        Self {
            seq,
            command_id,
            message_id: message_id.into(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct EventWrite {
    pub event: Option<DurableEvent>,
    pub projections: Vec<Projection>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ApplicationKind {
    IdleRun,
    HardSteer,
    SoftSteer,
    RetrySteer,
}

impl ApplicationKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::IdleRun => "idle_run",
            Self::HardSteer => "hard_steer",
            Self::SoftSteer => "soft_steer",
            Self::RetrySteer => "retry_steer",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value {
            "idle_run" => Ok(Self::IdleRun),
            "hard_steer" => Ok(Self::HardSteer),
            "soft_steer" => Ok(Self::SoftSteer),
            "retry_steer" => Ok(Self::RetrySteer),
            _ => bail!("unknown application kind {value}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RunPhase {
    Received,
    Classified,
    RunStarted,
    TurnStarted,
    UserStarted,
    UserCommitted,
    AssistantStarted,
    HardSteerRequested,
    CancelRequested,
    Finished,
}

impl RunPhase {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Classified => "classified",
            Self::RunStarted => "run_started",
            Self::TurnStarted => "turn_started",
            Self::UserStarted => "user_started",
            Self::UserCommitted => "user_committed",
            Self::AssistantStarted => "assistant_started",
            Self::HardSteerRequested => "hard_steer_requested",
            Self::CancelRequested => "cancel_requested",
            Self::Finished => "finished",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value {
            "received" => Ok(Self::Received),
            "classified" => Ok(Self::Classified),
            "run_started" => Ok(Self::RunStarted),
            "turn_started" => Ok(Self::TurnStarted),
            "user_started" => Ok(Self::UserStarted),
            "user_committed" => Ok(Self::UserCommitted),
            "assistant_started" => Ok(Self::AssistantStarted),
            "hard_steer_requested" => Ok(Self::HardSteerRequested),
            "cancel_requested" => Ok(Self::CancelRequested),
            "finished" => Ok(Self::Finished),
            _ => bail!("unknown durable run phase {value}"),
        }
    }

    fn is_owner(self) -> bool {
        matches!(
            self,
            Self::UserStarted
                | Self::UserCommitted
                | Self::AssistantStarted
                | Self::HardSteerRequested
                | Self::CancelRequested
        )
    }
}

#[allow(
    dead_code,
    reason = "T12 freezes projections that the T15 run loop will construct"
)]
#[derive(Clone)]
pub(crate) enum Projection {
    MessageEnd {
        message_id: String,
        role: &'static str,
        message: PublicMessage,
        append_to_l0: bool,
    },
    CommandReceived {
        envelope: CommandEnvelope,
    },
    CommandRejected {
        seq: u64,
        command_id: String,
        reason: CommandRejectReason,
        raw_command: RejectedCommandPayload,
        payload_digest: Option<KeyedCommandDigest>,
    },
    CommandClassified {
        command_id: String,
        application_kind: ApplicationKind,
        run_id: String,
        turn_id: String,
    },
    RunPhase {
        command_id: String,
        run_id: String,
        expected: RunPhase,
        next: RunPhase,
    },
    CommandApplied {
        command_id: String,
        command_seq: u64,
        run_id: Option<String>,
    },
    CommandSuperseded {
        command_id: String,
        command_seq: u64,
        run_id: Option<String>,
    },
    ToolExecution(ToolExecutionMutation),
    Approval(ApprovalMutation),
    #[cfg(test)]
    SizePadding(usize),
}

#[allow(
    dead_code,
    reason = "T12 freezes tool durability transitions before T15 execution wiring"
)]
#[derive(Clone)]
pub(crate) enum ToolExecutionMutation {
    Prepare {
        tool_call_id: String,
        command_id: String,
        run_id: String,
        executor_generation: u64,
        idempotency_key: String,
    },
    Start {
        tool_call_id: String,
    },
    Finish {
        tool_call_id: String,
        expected: &'static str,
        state: &'static str,
        error_code: Option<&'static str>,
    },
}

#[allow(
    dead_code,
    reason = "T12 freezes approval durability transitions before T15 broker wiring"
)]
#[derive(Clone)]
pub(crate) enum ApprovalMutation {
    Pending {
        request_id: String,
        tool_call_id: String,
        run_id: String,
        turn_id: String,
        request_projection: String,
        redaction_version: u32,
    },
    Resolve {
        request_id: String,
        state: &'static str,
        actor: String,
    },
}

struct PreparedEvent {
    seq: u64,
    kind: String,
    internal_metadata: String,
    command_id: Option<String>,
    run_id: Option<String>,
    turn_id: Option<String>,
    message_id: Option<String>,
    message_role: Option<String>,
    raw_key_ref: String,
    raw_key_proof: Vec<u8>,
    raw_ciphertext: Vec<u8>,
    envelope: String,
    redaction_version: u32,
    message_end: Option<MessageEndIdentity>,
}

struct MessageEndIdentity {
    message_id: String,
    message_digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum L0Disposition {
    Append,
    ExcludeRetryError,
}

enum PreparedProjection {
    MessageEnd {
        event_seq: u64,
        message_id: String,
        role: &'static str,
        raw_key_ref: String,
        raw_key_proof: Vec<u8>,
        raw_ciphertext: Vec<u8>,
        payload: String,
        search_text: String,
        redaction_version: u32,
        interrupted: bool,
        l0_disposition: L0Disposition,
    },
    CommandInsert {
        seq: u64,
        command_id: String,
        command_kind: &'static str,
        payload_key_ref: String,
        payload_key_proof: Vec<u8>,
        payload_ciphertext: Option<Vec<u8>>,
        payload_hmac: Vec<u8>,
        status: &'static str,
        reject_reason: Option<&'static str>,
        reject_actual_bytes: Option<u64>,
    },
    Plain(Projection),
}

struct PreparedWrite {
    event: Option<PreparedEvent>,
    projections: Vec<PreparedProjection>,
}

struct ExpectedInjection {
    text: Zeroizing<String>,
    timestamp: DateTime<Utc>,
}

struct InjectionSizing {
    size: super::sizer::BatchSize,
    application: InjectionApplication,
    run_id: String,
    turn_id: String,
    previous_owner_command_id: Option<CommandId>,
}

struct CommandInsertInput<'a> {
    key: &'a super::crypto::DataKeyMaterial,
    seq: u64,
    command_id: String,
    command_kind: &'static str,
    canonical_payload: &'a [u8],
    rejection: Option<CommandRejectReason>,
    provided_digest: Option<&'a KeyedCommandDigest>,
}

#[derive(Clone)]
struct ToolExecutionStartEvent {
    state: String,
}

#[derive(Clone)]
struct ToolExecutionEndEvent {
    state: String,
    result: Value,
    is_error: bool,
    error_code: Option<String>,
}

#[derive(Clone)]
struct ApprovalRequestedEvent {
    request: ApprovalRequest,
}

#[derive(Clone)]
struct ApprovalResolvedEvent {
    resolution: String,
    actor: String,
}

#[derive(Clone)]
struct ApprovalPendingEvidence {
    tool_call_id: String,
    request_projection: String,
}

#[derive(Clone)]
struct ApprovalResolveEvidence {
    resolution: String,
    actor: String,
}

#[derive(Clone)]
struct ToolFinishEvidence {
    expected: String,
    state: String,
    error_code: Option<String>,
}

pub(crate) struct EventWriter {
    store: Arc<Store>,
    gate: Mutex<()>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EventLogHead {
    last_seq: u64,
    event_count: u64,
    chain_digest: [u8; EVENT_DIGEST_BYTES],
    key_ref: String,
    head_hmac: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InboundAdmissionMode {
    Open,
    ReplayOnly,
}

/// Startup command-receiver boundary. While a durable suffix remains, exact
/// at-least-once replays may recover their stored ACK, but a new identity can
/// never reach CommandReceived persistence.
pub(crate) struct InboundAdmission {
    mode: InboundAdmissionMode,
}

impl InboundAdmission {
    pub(crate) fn after_t12_recovery(has_pending_suffix: bool) -> Self {
        Self {
            mode: if has_pending_suffix {
                InboundAdmissionMode::ReplayOnly
            } else {
                InboundAdmissionMode::Open
            },
        }
    }

    /// The T12 receiver uses this after a terminal control leaves no suffix;
    /// T15 uses the same transition only after its fresh recovery plan is empty.
    pub(crate) fn resume_after_suffix_recovery(&mut self) {
        self.mode = InboundAdmissionMode::Open;
    }

    pub(crate) fn is_replay_only(&self) -> bool {
        self.mode == InboundAdmissionMode::ReplayOnly
    }

    pub(crate) async fn receive(
        &mut self,
        writer: &EventWriter,
        inbound: &InboundCommand,
    ) -> Result<CommandAck> {
        let ack = writer
            .persist_inbound_with_admission(inbound, self.mode)
            .await?;
        if self.mode == InboundAdmissionMode::Open && ack.status == CommandAckStatus::Received {
            self.mode = InboundAdmissionMode::ReplayOnly;
        }
        Ok(ack)
    }
}

#[derive(Debug, Error)]
#[error("durable suffix recovery is required before accepting a new command")]
pub(crate) struct RecoveryRequired;

impl EventWriter {
    pub(crate) fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            gate: Mutex::new(()),
        }
    }

    #[cfg(test)]
    pub(crate) async fn persist_inbound(&self, inbound: &InboundCommand) -> Result<CommandAck> {
        self.persist_inbound_with_admission(inbound, InboundAdmissionMode::Open)
            .await
    }

    async fn persist_inbound_with_admission(
        &self,
        inbound: &InboundCommand,
        admission: InboundAdmissionMode,
    ) -> Result<CommandAck> {
        let _guard = self.gate.lock().await;
        if let InboundCommand::Invalid {
            reason,
            raw_command,
            payload_digest,
            ..
        } = inbound
        {
            match reason {
                CommandRejectReason::Oversized { .. }
                    if !matches!(raw_command, RejectedCommandPayload::DiscardedOversized)
                        || payload_digest.is_none() =>
                {
                    bail!(
                        "oversized command must discard raw bytes and carry its incremental digest"
                    );
                }
                CommandRejectReason::Oversized { .. } => {}
                _ if matches!(raw_command, RejectedCommandPayload::DiscardedOversized)
                    || payload_digest.is_some() =>
                {
                    bail!("only oversized commands may carry an incremental digest");
                }
                _ => {}
            }
        }
        let (seq, command_id, command_kind, rejection, canonical_payload, payload_digest) =
            match inbound {
                InboundCommand::Valid(envelope) => (
                    envelope.seq,
                    envelope.command_id.as_str(),
                    command_kind(&envelope.command),
                    None,
                    Zeroizing::new(
                        serde_json::to_vec(&envelope.command)
                            .context("failed to serialize canonical command payload")?,
                    ),
                    None,
                ),
                InboundCommand::Invalid {
                    seq,
                    command_id,
                    raw_command,
                    payload_digest,
                    ..
                } => (
                    *seq,
                    command_id.as_str(),
                    "invalid",
                    match inbound {
                        InboundCommand::Invalid { reason, .. } => Some(reason),
                        InboundCommand::Valid(_) => unreachable!(),
                    },
                    Zeroizing::new(
                        raw_command
                            .authenticated_bytes()
                            .map_or_else(Vec::new, <[u8]>::to_vec),
                    ),
                    payload_digest.as_ref(),
                ),
            };

        if let Some(ack) = self
            .verify_replay(
                seq,
                command_id,
                command_kind,
                rejection,
                &canonical_payload,
                payload_digest,
            )
            .await?
        {
            return Ok(ack);
        }
        if admission == InboundAdmissionMode::ReplayOnly {
            return Err(RecoveryRequired.into());
        }
        self.validate_next_command_seq(seq).await?;

        let projection = match inbound {
            InboundCommand::Valid(envelope) => Projection::CommandReceived {
                envelope: envelope.clone(),
            },
            InboundCommand::Invalid {
                seq,
                command_id,
                reason,
                raw_command,
                payload_digest,
            } => Projection::CommandRejected {
                seq: *seq,
                command_id: command_id.to_string(),
                reason: reason.clone(),
                raw_command: raw_command.clone(),
                payload_digest: payload_digest.clone(),
            },
        };
        self.apply_locked(EventBatch {
            writes: vec![EventWrite {
                event: None,
                projections: vec![projection],
            }],
            injected_commands: Vec::new(),
        })
        .await?;
        self.ack_for_command(command_id)
            .await?
            .ok_or_else(|| anyhow!("committed command row is missing"))
    }

    #[allow(
        dead_code,
        reason = "T12 freezes the EventBatch entry point consumed by the T15 run loop"
    )]
    pub(crate) async fn apply(&self, batch: EventBatch) -> Result<Vec<u64>> {
        let _guard = self.gate.lock().await;
        self.apply_locked(batch).await
    }

    #[cfg(test)]
    async fn apply_with_failpoint(
        &self,
        batch: EventBatch,
        fail_after_writes: usize,
    ) -> Result<Vec<u64>> {
        let _guard = self.gate.lock().await;
        self.apply_locked_with_failpoint(batch, Some(fail_after_writes), None, None)
            .await
    }

    #[cfg(all(test, unix))]
    async fn apply_with_abrupt_transaction_failpoint(
        &self,
        batch: EventBatch,
        name: &str,
        after_commit: bool,
        readiness_path: &std::path::Path,
    ) -> Result<Vec<u64>> {
        let _guard = self.gate.lock().await;
        self.apply_locked_with_failpoint(
            batch,
            None,
            Some((name, after_commit, readiness_path)),
            None,
        )
        .await
    }

    #[cfg(test)]
    async fn apply_after_prepare_destroy_key(
        &self,
        batch: EventBatch,
        key_ref: &str,
    ) -> Result<Vec<u64>> {
        let _guard = self.gate.lock().await;
        self.apply_locked_with_failpoint(batch, None, None, Some(key_ref))
            .await
    }

    pub(crate) async fn apply_idle_abort_cutoff(
        &self,
        abort_command_id: &str,
        abort_seq: u64,
    ) -> Result<Vec<CommandAck>> {
        let _guard = self.gate.lock().await;
        let abort_seq = sqlite_i64(abort_seq, "Abort command sequence")?;
        let owner_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM inbound_commands
             WHERE command_kind='user_message' AND status='applying'
               AND run_phase IN (
                 'user_started','user_committed','assistant_started',
                 'hard_steer_requested','cancel_requested'
               )",
        )
        .fetch_one(self.store.pool())
        .await?;
        if owner_count != 0 {
            bail!("idle Abort path cannot run while a live owner exists");
        }

        let pending = sqlx::query(
            "SELECT seq, command_id, command_kind, status, application_kind,
                    run_id, turn_id, run_phase
             FROM inbound_commands
             WHERE seq < ? AND status IN ('received','applying')
             ORDER BY seq
             LIMIT 33",
        )
        .bind(abort_seq)
        .fetch_all(self.store.pool())
        .await?;
        if pending.len() > 32 {
            bail!("idle Abort cutoff exceeds the bounded 32-command window");
        }
        let mut projections = Vec::with_capacity(pending.len() + 1);
        let mut terminal_ids = Vec::with_capacity(pending.len() + 1);
        let mut startup: Option<(String, String, RunPhase)> = None;
        for row in pending {
            let seq = sqlite_u64(row.get::<i64, _>("seq"), "stored command sequence")?;
            let command_id: String = row.try_get("command_id")?;
            let kind: String = row.try_get("command_kind")?;
            let status: String = row.try_get("status")?;
            match (kind.as_str(), status.as_str()) {
                ("user_message", "received") => {
                    projections.push(Projection::CommandSuperseded {
                        command_id: command_id.clone(),
                        command_seq: seq,
                        run_id: None,
                    });
                }
                ("approval_decision", "received") => {
                    projections.push(Projection::CommandApplied {
                        command_id: command_id.clone(),
                        command_seq: seq,
                        run_id: None,
                    });
                }
                ("user_message", "applying") => {
                    let application_kind: String = row.try_get("application_kind")?;
                    let run_id: String = row.try_get("run_id")?;
                    let turn_id: String = row.try_get("turn_id")?;
                    let phase = RunPhase::parse(row.try_get("run_phase")?)?;
                    if application_kind != "idle_run"
                        || !matches!(
                            phase,
                            RunPhase::Classified | RunPhase::RunStarted | RunPhase::TurnStarted
                        )
                        || startup.is_some()
                    {
                        bail!(
                            "idle Abort cutoff requires at most one pre-user idle startup; found {command_id} in {application_kind}/{}",
                            phase.as_str()
                        );
                    }
                    projections.push(Projection::CommandSuperseded {
                        command_id: command_id.clone(),
                        command_seq: seq,
                        run_id: Some(run_id.clone()),
                    });
                    startup = Some((run_id, turn_id, phase));
                }
                _ => {
                    bail!(
                        "idle Abort cutoff found unsupported pending command {command_id}: {kind}/{status}"
                    );
                }
            }
            terminal_ids.push(command_id);
        }
        let abort_run_id = startup.as_ref().map(|(run_id, _, _)| run_id.clone());
        projections.push(Projection::CommandApplied {
            command_id: abort_command_id.to_owned(),
            command_seq: sqlite_u64(abort_seq, "Abort command sequence")?,
            run_id: abort_run_id,
        });
        terminal_ids.push(abort_command_id.to_owned());
        let mut writes = Vec::new();
        if let Some((run_id, turn_id, phase)) = &startup {
            if *phase == RunPhase::TurnStarted {
                writes.push(EventWrite {
                    event: Some(DurableEvent::empty_turn_end(run_id, turn_id)?),
                    projections: Vec::new(),
                });
            }
            if matches!(*phase, RunPhase::RunStarted | RunPhase::TurnStarted) {
                writes.push(EventWrite {
                    event: Some(DurableEvent::agent_end(run_id)?),
                    projections: Vec::new(),
                });
            }
        }
        writes.push(EventWrite {
            event: None,
            projections,
        });
        self.apply_locked(EventBatch {
            writes,
            injected_commands: Vec::new(),
        })
        .await?;

        let mut acks = Vec::with_capacity(terminal_ids.len());
        for command_id in terminal_ids {
            acks.push(
                self.ack_for_command(&command_id)
                    .await?
                    .ok_or_else(|| anyhow!("terminal command {command_id} disappeared"))?,
            );
        }
        Ok(acks)
    }

    async fn apply_locked(&self, batch: EventBatch) -> Result<Vec<u64>> {
        self.apply_locked_with_failpoint(batch, None, None, None)
            .await
    }

    async fn apply_locked_with_failpoint(
        &self,
        batch: EventBatch,
        fail_after_writes: Option<usize>,
        abrupt_failpoint: Option<(&str, bool, &std::path::Path)>,
        destroy_after_prepare: Option<&str>,
    ) -> Result<Vec<u64>> {
        preflight_materialization_bounds(self.store.redactor(), &batch)?;
        let expected_injections = validate_batch_shape(self.store.redactor(), &batch)?;
        let injected_commands = batch.injected_commands.clone();
        let previous_event_head = load_verified_event_head(self.store.as_ref()).await?;
        let next_seq = previous_event_head
            .as_ref()
            .map_or(0, |head| head.last_seq)
            .checked_add(1)
            .ok_or_else(|| anyhow!("durable event sequence overflow"))?;
        let (prepared, transaction_bytes, event_seqs) = self.prepare_batch(batch, next_seq).await?;
        if let Some(key_ref) = destroy_after_prepare {
            self.store.destroy_conversation_key_ref(key_ref).await?;
        }

        let mut transaction = self.store.pool().begin().await?;
        revalidate_prepared_key_refs(self.store.as_ref(), &mut transaction, &prepared).await?;
        let transaction_event_head =
            load_verified_event_head_in_transaction(self.store.as_ref(), &mut transaction).await?;
        if transaction_event_head != previous_event_head {
            bail!("event-log head changed while EventBatch was prepared");
        }
        let mut command_bounds = BatchBounds::default();
        if !injected_commands.is_empty() {
            let command_size = self
                .derive_injected_command_size(
                    &mut transaction,
                    &injected_commands,
                    &expected_injections,
                )
                .await?;
            command_bounds = BatchBounds {
                command_count: command_size.size.command_count,
                command_plaintext_bytes: command_size.size.command_plaintext_bytes,
            };
            EventBatchSizer::validate(command_bounds, command_size.size.transaction_bytes)?;
            let prepared_injection_bytes =
                prepared_injection_bytes(&prepared, &injected_commands, &command_size)?;
            if prepared_injection_bytes != command_size.size.transaction_bytes {
                bail!(
                    "EventBatchSizer drift: predicted {} durable injection bytes, prepared write-set has {}",
                    command_size.size.transaction_bytes,
                    prepared_injection_bytes
                );
            }
        }
        EventBatchSizer::validate(command_bounds, transaction_bytes)?;
        let mut owner_preconditions = HashSet::new();
        let mut owner_postconditions = HashSet::new();
        collect_owner_conditions(
            &prepared,
            &mut owner_preconditions,
            &mut owner_postconditions,
        );
        let classification_owner_conditions = collect_classification_owner_conditions(&prepared)?;
        for (run_id, expected) in &classification_owner_conditions {
            require_owner_count(&mut transaction, run_id, *expected).await?;
        }
        validate_owner_open_preconditions(&mut transaction, &prepared).await?;
        validate_required_projection_sets(self.store.as_ref(), &mut transaction, &prepared).await?;
        for run_id in &owner_preconditions {
            require_owner_count(&mut transaction, run_id, 1).await?;
        }

        let mut applied_writes = 0usize;
        let mut updated_event_head = previous_event_head.clone();
        for write in prepared {
            if let Some(event) = write.event {
                let (previous_digest, previous_count, head_key_ref) =
                    match updated_event_head.as_ref() {
                        Some(head) => {
                            if head.key_ref != event.raw_key_ref {
                                bail!("event-log key changed without an explicit rotation");
                            }
                            (head.chain_digest, head.event_count, head.key_ref.clone())
                        }
                        None => ([0_u8; EVENT_DIGEST_BYTES], 0, event.raw_key_ref.clone()),
                    };
                let expected_seq = updated_event_head
                    .as_ref()
                    .map_or(1, |head| head.last_seq.saturating_add(1));
                if event.seq != expected_seq {
                    bail!(
                        "durable event sequence is not contiguous: expected {expected_seq}, prepared {}",
                        event.seq
                    );
                }
                let chain_digest = extend_event_chain(
                    &previous_digest,
                    EventChainEntry {
                        seq: event.seq,
                        event_type: &event.kind,
                        internal_metadata: &event.internal_metadata,
                        key_ref: &event.raw_key_ref,
                        ciphertext: &event.raw_ciphertext,
                        envelope: &event.envelope,
                        redaction_version: event.redaction_version,
                    },
                );
                sqlx::query(
                    "INSERT INTO agent_events(
                        seq, event_type, internal_metadata, raw_key_ref, raw_ciphertext,
                        envelope, redaction_version, created_at
                     ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(sqlite_i64(event.seq, "durable event sequence")?)
                .bind(event.kind)
                .bind(event.internal_metadata)
                .bind(event.raw_key_ref)
                .bind(event.raw_ciphertext)
                .bind(event.envelope)
                .bind(event.redaction_version as i64)
                .bind(Utc::now().to_rfc3339())
                .execute(&mut *transaction)
                .await
                .context("failed to append durable event")?;
                let event_count = previous_count
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("durable event count overflow"))?;
                let key = self
                    .store
                    .data_key_by_ref_in_transaction(&mut transaction, &head_key_ref)
                    .await?;
                let head_hmac = authenticate_event_head(
                    self.store.scope(),
                    &key,
                    event.seq,
                    event_count,
                    &chain_digest,
                )?;
                updated_event_head = Some(EventLogHead {
                    last_seq: event.seq,
                    event_count,
                    chain_digest,
                    key_ref: head_key_ref,
                    head_hmac,
                });
            }
            for projection in write.projections {
                apply_projection(&mut transaction, projection).await?;
            }
            applied_writes = applied_writes.saturating_add(1);
            if fail_after_writes == Some(applied_writes) {
                bail!("EventWriter test failpoint after {applied_writes} writes");
            }
        }

        for (run_id, expected) in &classification_owner_conditions {
            require_owner_count(&mut transaction, run_id, *expected).await?;
        }
        for run_id in &owner_postconditions {
            require_owner_count(&mut transaction, run_id, 1).await?;
        }
        if updated_event_head != previous_event_head {
            persist_event_head(
                self.store.as_ref(),
                &mut transaction,
                previous_event_head.as_ref(),
                updated_event_head
                    .as_ref()
                    .expect("changed event-log head must be present"),
            )
            .await?;
        }
        if let Some((name, false, readiness_path)) = abrupt_failpoint {
            abrupt_transaction_exit(name, "before_commit", readiness_path);
        }
        transaction
            .commit()
            .await
            .context("failed to commit EventBatch")?;
        if let Some((name, true, readiness_path)) = abrupt_failpoint {
            abrupt_transaction_exit(name, "after_commit", readiness_path);
        }
        Ok(event_seqs)
    }

    async fn prepare_batch(
        &self,
        batch: EventBatch,
        first_seq: u64,
    ) -> Result<(Vec<PreparedWrite>, usize, Vec<u64>)> {
        preflight_materialization_bounds(self.store.redactor(), &batch)?;
        let bounds = BatchBounds {
            command_count: batch.injected_commands.len(),
            command_plaintext_bytes: 0,
        };
        EventBatchSizer::validate(bounds, 0)?;
        let event_key = if batch.writes.iter().any(|write| write.event.is_some()) {
            Some(self.store.conversation_key(DataKeyPurpose::Event).await?)
        } else {
            None
        };
        let transcript_key = if batch.writes.iter().any(|write| {
            write
                .projections
                .iter()
                .any(|projection| matches!(projection, Projection::MessageEnd { .. }))
        }) {
            Some(
                self.store
                    .conversation_key(DataKeyPurpose::Transcript)
                    .await?,
            )
        } else {
            None
        };
        let command_key = if batch.writes.iter().any(|write| {
            write.projections.iter().any(|projection| {
                matches!(
                    projection,
                    Projection::CommandReceived { .. } | Projection::CommandRejected { .. }
                )
            })
        }) {
            Some(self.store.conversation_key(DataKeyPurpose::Command).await?)
        } else {
            None
        };

        let mut next_seq = first_seq;
        let mut prepared = Vec::with_capacity(batch.writes.len());
        let mut transaction_bytes = 0usize;
        let mut event_seqs = Vec::new();
        for write in batch.writes {
            let assigned_seq = if write.event.is_some() {
                let seq = next_seq;
                sqlite_i64(seq, "durable event sequence")?;
                next_seq = next_seq
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("durable event sequence overflow"))?;
                event_seqs.push(seq);
                Some(seq)
            } else {
                None
            };
            let event = match (write.event, assigned_seq) {
                (Some(event), Some(seq)) => {
                    let key = event_key.as_ref().expect("event key was loaded");
                    let aad = self.store.scope().row_aad(
                        "agent_events",
                        seq.to_string(),
                        DataKeyPurpose::Event,
                    );
                    let protected = PublicProjectionBuilder::new(self.store.redactor(), key)
                        .build_serialized(&event.raw_json, &aad)
                        .context("failed to build raw/redacted durable event atomically")?;
                    let identity = event.identity();
                    let kind = identity.kind.to_owned();
                    let command_id = identity.command_id.map(str::to_owned);
                    let run_id = identity.run_id.map(str::to_owned);
                    let turn_id = identity.turn_id.map(str::to_owned);
                    let message_id = identity.message_id.map(str::to_owned);
                    let message_role = identity.message_role.map(str::to_owned);
                    let internal_metadata = serde_json::to_string(&event.metadata)
                        .context("failed to serialize durable event internal metadata")?;
                    let message_end = message_end_identity(&event.value)?;
                    charge_transaction_bytes(
                        &mut transaction_bytes,
                        protected
                            .ciphertext
                            .len()
                            .checked_add(protected.projection.len())
                            .and_then(|bytes| bytes.checked_add(kind.len()))
                            .and_then(|bytes| bytes.checked_add(internal_metadata.len()))
                            .and_then(|bytes| bytes.checked_add(DURABLE_ROW_OVERHEAD_BYTES))
                            .ok_or_else(|| anyhow!("durable event byte count overflow"))?,
                    )?;
                    Some(PreparedEvent {
                        seq,
                        kind,
                        internal_metadata,
                        command_id,
                        run_id,
                        turn_id,
                        message_id,
                        message_role,
                        raw_key_ref: key.key_ref.clone(),
                        raw_key_proof: keyed_digest(key, PREPARED_KEY_MATERIAL_PROOF),
                        raw_ciphertext: protected.ciphertext,
                        envelope: protected.projection,
                        redaction_version: protected.redaction_version,
                        message_end,
                    })
                }
                (None, None) => None,
                _ => unreachable!("event sequence assignment is paired"),
            };

            let mut projections = Vec::with_capacity(write.projections.len());
            for projection in write.projections {
                match projection {
                    Projection::MessageEnd {
                        message_id,
                        role,
                        message,
                        append_to_l0,
                    } => {
                        let l0_disposition = l0_disposition(&message, append_to_l0)?;
                        let event_seq = assigned_seq
                            .ok_or_else(|| anyhow!("MessageEnd projection requires an event"))?;
                        let key = transcript_key.as_ref().expect("transcript key was loaded");
                        let aad = self.store.scope().row_aad(
                            "messages",
                            &message_id,
                            DataKeyPurpose::Transcript,
                        );
                        let protected = PublicProjectionBuilder::new(self.store.redactor(), key)
                            .build(&message, &aad)
                            .context(
                                "failed to build raw/redacted message projection atomically",
                            )?;
                        let canonical_message = serde_json::to_value(&message)?;
                        let message_digest: [u8; 32] =
                            Sha256::digest(serde_json::to_vec(&canonical_message)?).into();
                        validate_message_end_event(
                            event.as_ref().expect("event is prepared"),
                            &message_id,
                            message_digest,
                            &protected.projection,
                        )?;
                        let search_text = search_text_from_projection(&protected.projection)?;
                        let interrupted = matches!(
                            &message,
                            PublicMessage::Assistant(message) if message.interrupted
                        );
                        charge_transaction_bytes(
                            &mut transaction_bytes,
                            protected
                                .ciphertext
                                .len()
                                .checked_add(protected.projection.len())
                                .and_then(|bytes| bytes.checked_add(search_text.len()))
                                .and_then(|bytes| bytes.checked_add(DURABLE_ROW_OVERHEAD_BYTES))
                                .ok_or_else(|| anyhow!("message projection byte count overflow"))?,
                        )?;
                        projections.push(PreparedProjection::MessageEnd {
                            event_seq,
                            message_id,
                            role,
                            raw_key_ref: key.key_ref.clone(),
                            raw_key_proof: keyed_digest(key, PREPARED_KEY_MATERIAL_PROOF),
                            raw_ciphertext: protected.ciphertext,
                            payload: protected.projection,
                            search_text,
                            redaction_version: protected.redaction_version,
                            interrupted,
                            l0_disposition,
                        });
                    }
                    Projection::CommandReceived { envelope } => {
                        let payload = Zeroizing::new(serde_json::to_vec(&envelope.command)?);
                        let prepared = self.prepare_command_insert(CommandInsertInput {
                            key: command_key.as_ref().expect("command key was loaded"),
                            seq: envelope.seq,
                            command_id: envelope.command_id.to_string(),
                            command_kind: command_kind(&envelope.command),
                            canonical_payload: &payload,
                            rejection: None,
                            provided_digest: None,
                        })?;
                        charge_transaction_bytes(
                            &mut transaction_bytes,
                            prepared_projection_size(&prepared),
                        )?;
                        projections.push(prepared);
                    }
                    Projection::CommandRejected {
                        seq,
                        command_id,
                        reason,
                        raw_command,
                        payload_digest,
                    } => {
                        let prepared = self.prepare_command_insert(CommandInsertInput {
                            key: command_key.as_ref().expect("command key was loaded"),
                            seq,
                            command_id,
                            command_kind: "invalid",
                            canonical_payload: raw_command.authenticated_bytes().unwrap_or(&[]),
                            rejection: Some(reason),
                            provided_digest: payload_digest.as_ref(),
                        })?;
                        charge_transaction_bytes(
                            &mut transaction_bytes,
                            prepared_projection_size(&prepared),
                        )?;
                        projections.push(prepared);
                    }
                    Projection::Approval(ApprovalMutation::Pending {
                        request_projection,
                        redaction_version,
                        ..
                    }) if redaction_version != self.store.redactor().version()
                        || self.store.redactor().redact_text(&request_projection)
                            != request_projection =>
                    {
                        bail!(
                            "approval projection must already be redacted with the current version"
                        );
                    }
                    projection => {
                        charge_transaction_bytes(
                            &mut transaction_bytes,
                            projection_size_upper_bound(&projection)?,
                        )?;
                        projections.push(PreparedProjection::Plain(projection));
                    }
                }
            }
            prepared.push(PreparedWrite { event, projections });
        }
        EventBatchSizer::validate(bounds, transaction_bytes)?;
        Ok((prepared, transaction_bytes, event_seqs))
    }

    fn prepare_command_insert(&self, input: CommandInsertInput<'_>) -> Result<PreparedProjection> {
        let CommandInsertInput {
            key,
            seq,
            command_id,
            command_kind,
            canonical_payload,
            rejection,
            provided_digest,
        } = input;
        let aad = self.store.scope().row_aad(
            "inbound_commands",
            seq.to_string(),
            DataKeyPurpose::Command,
        );
        let oversized = matches!(rejection, Some(CommandRejectReason::Oversized { .. }));
        let payload_hmac = match (oversized, provided_digest) {
            (true, Some(digest)) if digest.key_ref() == key.key_ref => digest.hmac().to_vec(),
            (true, Some(digest)) => bail!(
                "oversized command digest key {} does not match active command key {}",
                digest.key_ref(),
                key.key_ref
            ),
            (true, None) => bail!("oversized command requires an incremental payload digest"),
            (false, Some(_)) => bail!("only oversized commands may carry a precomputed digest"),
            (false, None) => keyed_digest(key, canonical_payload),
        };
        let payload_ciphertext = if oversized {
            None
        } else {
            Some(super::crypto::encrypt_content(
                key,
                canonical_payload,
                &aad,
            )?)
        };
        let (status, reject_reason, reject_actual_bytes) = match rejection {
            Some(reason) => (
                "rejected",
                Some(reason.code()),
                match reason {
                    CommandRejectReason::Oversized { actual_bytes } => Some(actual_bytes),
                    _ => None,
                },
            ),
            None => ("received", None, None),
        };
        Ok(PreparedProjection::CommandInsert {
            seq,
            command_id,
            command_kind,
            payload_key_ref: key.key_ref.clone(),
            payload_key_proof: keyed_digest(key, PREPARED_KEY_MATERIAL_PROOF),
            payload_ciphertext,
            payload_hmac,
            status,
            reject_reason,
            reject_actual_bytes,
        })
    }

    async fn derive_injected_command_size(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        commands: &[InjectedCommand],
        expected: &[ExpectedInjection],
    ) -> Result<InjectionSizing> {
        let mut canonical_payloads = Vec::with_capacity(commands.len());
        let mut group: Option<(InjectionApplication, String, String)> = None;
        for (command, expected) in commands.iter().zip(expected) {
            let row = sqlx::query(
                "SELECT command_kind, payload_key_ref, payload_ciphertext, payload_hmac,
                        status, run_phase, application_kind, run_id, turn_id, received_at
                 FROM inbound_commands
                 WHERE seq = ? AND command_id = ?",
            )
            .bind(sqlite_i64(command.seq, "injected command sequence")?)
            .bind(command.command_id.as_str())
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "injected command {} at sequence {} is not durable",
                    command.command_id,
                    command.seq
                )
            })?;
            let command_kind: String = row.try_get("command_kind")?;
            let status: String = row.try_get("status")?;
            let phase: String = row.try_get("run_phase")?;
            let application = match row.try_get::<String, _>("application_kind")?.as_str() {
                "idle_run" => InjectionApplication::IdleRun,
                "hard_steer" => InjectionApplication::HardSteer,
                "soft_steer" => InjectionApplication::SoftSteer,
                "retry_steer" => InjectionApplication::RetrySteer,
                value => bail!("injected command has unknown application kind {value}"),
            };
            let run_id: String = row.try_get("run_id")?;
            let turn_id: String = row.try_get("turn_id")?;
            if let Some((group_application, group_run, group_turn)) = &group {
                if *group_application != application
                    || group_run != &run_id
                    || group_turn != &turn_id
                {
                    bail!("injected commands do not form one application/run/turn group");
                }
            } else {
                group = Some((application, run_id.clone(), turn_id.clone()));
            }
            if command_kind != "user_message"
                || status != "applying"
                || !matches!(
                    phase.as_str(),
                    "classified" | "turn_started" | "user_started"
                )
            {
                bail!(
                    "injected command {} has invalid durable state {command_kind}/{status}/{phase}",
                    command.command_id
                );
            }

            let key_ref: String = row.try_get("payload_key_ref")?;
            let key = self
                .store
                .data_key_by_ref_in_transaction(transaction, &key_ref)
                .await?;
            let ciphertext: Vec<u8> = row.try_get("payload_ciphertext")?;
            let aad = self.store.scope().row_aad(
                "inbound_commands",
                command.seq.to_string(),
                DataKeyPurpose::Command,
            );
            let plaintext =
                Zeroizing::new(super::crypto::decrypt_content(&key, &ciphertext, &aad)?);
            let digest: Vec<u8> = row.try_get("payload_hmac")?;
            verify_keyed_digest(&key, &plaintext, &digest)?;

            let mut parsed: Command = serde_json::from_slice(&plaintext)
                .context("durable injected command payload is invalid")?;
            let matches_message = match &mut parsed {
                Command::UserMessage { text, attachments } => {
                    let matches = attachments.is_empty() && text.as_str() == expected.text.as_str();
                    text.zeroize();
                    matches
                }
                Command::Abort {} | Command::ApprovalDecision { .. } => false,
            };
            if !matches_message {
                bail!(
                    "injected command {} does not match its user MessageEnd",
                    command.command_id
                );
            }
            let received_at: String = row.try_get("received_at")?;
            let durable_timestamp = DateTime::parse_from_rfc3339(&received_at)
                .with_context(|| {
                    format!(
                        "injected command {} has invalid durable received_at",
                        command.command_id
                    )
                })?
                .with_timezone(&Utc);
            if expected.timestamp != durable_timestamp {
                bail!(
                    "injected command {} timestamp does not match durable received_at",
                    command.command_id
                );
            }
            canonical_payloads.push(plaintext);
        }
        let Some((application, run_id, turn_id)) = group else {
            bail!("injection sizing requires at least one durable command");
        };
        let owner_rows: Vec<String> = sqlx::query_scalar(
            "SELECT command_id FROM inbound_commands
             WHERE run_id=? AND command_kind='user_message' AND status='applying'
               AND run_phase IN (
                 'user_started','user_committed','assistant_started',
                 'hard_steer_requested','cancel_requested'
               )
             ORDER BY seq",
        )
        .bind(&run_id)
        .fetch_all(&mut **transaction)
        .await?;
        let injected_ids: HashSet<&str> = commands
            .iter()
            .map(|command| command.command_id.as_str())
            .collect();
        let previous_owners: Vec<CommandId> = owner_rows
            .into_iter()
            .filter(|owner| !injected_ids.contains(owner.as_str()))
            .map(|owner| {
                CommandId::parse(&owner)
                    .map_err(|_| anyhow!("durable owner command_id is not canonical"))
            })
            .collect::<Result<_>>()?;
        let previous_owner = match application {
            InjectionApplication::IdleRun if previous_owners.is_empty() => None,
            InjectionApplication::IdleRun => {
                bail!("idle_run injection cannot have a previous run owner")
            }
            _ if previous_owners.len() == 1 => previous_owners.first(),
            _ => bail!(
                "steer injection requires exactly one previous owner, found {}",
                previous_owners.len()
            ),
        };
        let sizing_commands: Vec<_> = canonical_payloads
            .iter()
            .zip(commands)
            .zip(expected)
            .map(|((payload, command), expected)| InjectionCommandSizeInput {
                command_id: &command.command_id,
                canonical_payload: payload.as_slice(),
                message_id: &command.message_id,
                text: &expected.text,
                timestamp: &expected.timestamp,
            })
            .collect();
        let size = EventBatchSizer::injection_batch(
            self.store.redactor(),
            InjectionBatchSizeInput {
                application,
                run_id: &run_id,
                turn_id: &turn_id,
                previous_owner_command_id: previous_owner,
                commands: &sizing_commands,
            },
        )?;
        Ok(InjectionSizing {
            size,
            application,
            run_id,
            turn_id,
            previous_owner_command_id: previous_owner.cloned(),
        })
    }

    async fn validate_next_command_seq(&self, incoming: u64) -> Result<()> {
        sqlite_i64(incoming, "incoming command sequence")?;
        let last = sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(seq) FROM inbound_commands")
            .fetch_one(self.store.pool())
            .await?
            .unwrap_or(0);
        let last = sqlite_u64(last, "stored command sequence")?;
        let expected = last
            .checked_add(1)
            .ok_or_else(|| anyhow!("command sequence overflow"))?;
        if incoming != expected {
            bail!("command sequence gap: expected {expected}, received {incoming}");
        }
        Ok(())
    }

    async fn verify_replay(
        &self,
        incoming_seq: u64,
        command_id: &str,
        incoming_kind: &str,
        incoming_rejection: Option<&CommandRejectReason>,
        canonical_payload: &[u8],
        incoming_digest: Option<&KeyedCommandDigest>,
    ) -> Result<Option<CommandAck>> {
        let by_id = sqlx::query(
            "SELECT seq, command_kind, payload_key_ref, payload_ciphertext, payload_hmac,
                    reject_reason, reject_actual_bytes
             FROM inbound_commands WHERE command_id = ?",
        )
        .bind(command_id)
        .fetch_optional(self.store.pool())
        .await?;
        let by_seq: Option<String> =
            sqlx::query_scalar("SELECT command_id FROM inbound_commands WHERE seq = ?")
                .bind(sqlite_i64(incoming_seq, "incoming command sequence")?)
                .fetch_optional(self.store.pool())
                .await?;

        let Some(row) = by_id else {
            if let Some(existing_id) = by_seq {
                bail!("command sequence {incoming_seq} is already bound to command {existing_id}");
            }
            return Ok(None);
        };
        let stored_seq = sqlite_u64(row.get::<i64, _>("seq"), "stored command sequence")?;
        if stored_seq != incoming_seq {
            bail!(
                "command replay sequence mismatch: command {command_id} is bound to {stored_seq}, received {incoming_seq}"
            );
        }
        if by_seq.as_deref() != Some(command_id) {
            bail!("command replay identity is inconsistent");
        }
        let stored_kind: String = row.try_get("command_kind")?;
        if stored_kind != incoming_kind {
            bail!("command replay kind mismatch: stored {stored_kind}, received {incoming_kind}");
        }
        let stored_reason: Option<String> = row.try_get("reject_reason")?;
        let stored_actual: Option<i64> = row.try_get("reject_actual_bytes")?;
        match incoming_rejection {
            Some(reason)
                if stored_reason.as_deref() == Some(reason.code())
                    && stored_actual
                        == match reason {
                            CommandRejectReason::Oversized { actual_bytes } => {
                                Some(sqlite_i64(*actual_bytes, "rejected command byte count")?)
                            }
                            _ => None,
                        } => {}
            None if stored_reason.is_none() && stored_actual.is_none() => {}
            _ => bail!("command replay rejection metadata mismatch"),
        }
        let key_ref: String = row.try_get("payload_key_ref")?;
        let key = self.store.data_key_by_ref(&key_ref).await?;
        let digest: Vec<u8> = row.try_get("payload_hmac")?;
        let ciphertext = row.try_get::<Option<Vec<u8>>, _>("payload_ciphertext")?;
        if let Some(incoming_digest) = incoming_digest {
            if incoming_digest.key_ref() != key_ref {
                bail!("command replay digest key mismatch");
            }
            verify_digest_bytes(incoming_digest.hmac(), &digest)?;
            if ciphertext.is_some() {
                bail!("oversized command replay unexpectedly has durable ciphertext");
            }
        } else {
            verify_keyed_digest(&key, canonical_payload, &digest)?;
        }
        if let Some(ciphertext) = ciphertext {
            let aad = self.store.scope().row_aad(
                "inbound_commands",
                stored_seq.to_string(),
                DataKeyPurpose::Command,
            );
            let decrypted = decrypt_replay_payload(&key, &ciphertext, &aad)?;
            if decrypted.as_slice() != canonical_payload {
                bail!("command replay decrypted payload mismatch");
            }
        }
        self.ack_for_command(command_id).await
    }

    pub(crate) async fn ack_for_command(&self, command_id: &str) -> Result<Option<CommandAck>> {
        let row = sqlx::query(
            "SELECT seq, status, reject_reason FROM inbound_commands WHERE command_id = ?",
        )
        .bind(command_id)
        .fetch_optional(self.store.pool())
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let status: String = row.try_get("status")?;
        let status = match status.as_str() {
            "received" | "applying" => CommandAckStatus::Received,
            "applied" => CommandAckStatus::Applied,
            "superseded" => CommandAckStatus::Superseded,
            "rejected" => CommandAckStatus::Rejected,
            value => bail!("unknown persisted command status {value}"),
        };
        Ok(Some(CommandAck {
            seq: sqlite_u64(row.get::<i64, _>("seq"), "stored command sequence")?,
            command_id: command_id.to_owned(),
            status,
            reject_reason: row.try_get("reject_reason")?,
        }))
    }
}

fn decrypt_replay_payload(
    key: &super::crypto::DataKeyMaterial,
    ciphertext: &[u8],
    aad: &super::crypto::RowAad,
) -> Result<Zeroizing<Vec<u8>>> {
    Ok(Zeroizing::new(super::crypto::decrypt_content(
        key, ciphertext, aad,
    )?))
}

async fn load_verified_event_head(store: &Store) -> Result<Option<EventLogHead>> {
    let row = sqlx::query(
        "SELECT last_seq, event_count, chain_digest, key_ref, head_hmac
         FROM event_log_heads WHERE conversation_id = ?",
    )
    .bind(&store.scope().conversation_id)
    .fetch_optional(store.pool())
    .await
    .context("failed to load event-log head")?;
    let Some(row) = row else {
        let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(store.pool())
            .await?;
        if event_count != 0 {
            bail!("durable events exist without an authenticated event-log head");
        }
        return Ok(None);
    };
    let key_ref: String = row.try_get("key_ref")?;
    let key = store.data_key_by_ref(&key_ref).await?;
    decode_verified_event_head(store, &row, &key_ref, &key)
        .map(Some)
        .context("event-log head failed authenticated validation")
}

async fn load_verified_event_head_in_transaction(
    store: &Store,
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<Option<EventLogHead>> {
    let row = sqlx::query(
        "SELECT last_seq, event_count, chain_digest, key_ref, head_hmac
         FROM event_log_heads WHERE conversation_id = ?",
    )
    .bind(&store.scope().conversation_id)
    .fetch_optional(&mut **transaction)
    .await
    .context("failed to load event-log head in EventBatch")?;
    let Some(row) = row else {
        let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(&mut **transaction)
            .await?;
        if event_count != 0 {
            bail!("durable events exist without an authenticated event-log head");
        }
        return Ok(None);
    };
    let key_ref: String = row.try_get("key_ref")?;
    let key = store
        .data_key_by_ref_in_transaction(transaction, &key_ref)
        .await?;
    decode_verified_event_head(store, &row, &key_ref, &key)
        .map(Some)
        .context("event-log head failed authenticated EventBatch validation")
}

fn decode_verified_event_head(
    store: &Store,
    row: &sqlx::sqlite::SqliteRow,
    key_ref: &str,
    key: &super::crypto::DataKeyMaterial,
) -> Result<EventLogHead> {
    let last_seq = sqlite_u64(row.try_get("last_seq")?, "event-log head last sequence")?;
    let event_count = sqlite_u64(row.try_get("event_count")?, "event-log event count")?;
    if last_seq == 0 || event_count == 0 || event_count > last_seq {
        bail!("event-log head contains impossible sequence/count metadata");
    }
    let head_hmac: Vec<u8> = row.try_get("head_hmac")?;
    let chain_digest = verify_event_head(
        store.scope(),
        key,
        last_seq,
        event_count,
        row.try_get::<Vec<u8>, _>("chain_digest")?.as_slice(),
        &head_hmac,
    )?;
    Ok(EventLogHead {
        last_seq,
        event_count,
        chain_digest,
        key_ref: key_ref.to_owned(),
        head_hmac,
    })
}

async fn persist_event_head(
    store: &Store,
    transaction: &mut Transaction<'_, Sqlite>,
    previous: Option<&EventLogHead>,
    next: &EventLogHead,
) -> Result<()> {
    let result = if let Some(previous) = previous {
        sqlx::query(
            "UPDATE event_log_heads
             SET last_seq=?, event_count=?, chain_digest=?, key_ref=?, head_hmac=?, updated_at=?
             WHERE conversation_id=? AND last_seq=? AND event_count=?
               AND chain_digest=? AND key_ref=? AND head_hmac=?",
        )
        .bind(sqlite_i64(next.last_seq, "event-log head last sequence")?)
        .bind(sqlite_i64(next.event_count, "event-log event count")?)
        .bind(next.chain_digest.as_slice())
        .bind(&next.key_ref)
        .bind(&next.head_hmac)
        .bind(Utc::now().to_rfc3339())
        .bind(&store.scope().conversation_id)
        .bind(sqlite_i64(
            previous.last_seq,
            "previous event-log head last sequence",
        )?)
        .bind(sqlite_i64(
            previous.event_count,
            "previous event-log event count",
        )?)
        .bind(previous.chain_digest.as_slice())
        .bind(&previous.key_ref)
        .bind(&previous.head_hmac)
        .execute(&mut **transaction)
        .await?
    } else {
        sqlx::query(
            "INSERT INTO event_log_heads(
                conversation_id, last_seq, event_count, chain_digest, key_ref, head_hmac, updated_at
             ) VALUES(?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&store.scope().conversation_id)
        .bind(sqlite_i64(next.last_seq, "event-log head last sequence")?)
        .bind(sqlite_i64(next.event_count, "event-log event count")?)
        .bind(next.chain_digest.as_slice())
        .bind(&next.key_ref)
        .bind(&next.head_hmac)
        .bind(Utc::now().to_rfc3339())
        .execute(&mut **transaction)
        .await?
    };
    require_single_cas(result.rows_affected(), "event-log head update")
}

async fn revalidate_prepared_key_refs(
    store: &Store,
    transaction: &mut Transaction<'_, Sqlite>,
    prepared: &[PreparedWrite],
) -> Result<()> {
    let scope_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_scope
         WHERE singleton=1 AND tenant_id=? AND agent_id=? AND conversation_id=?",
    )
    .bind(&store.scope().tenant_id)
    .bind(&store.scope().agent_id)
    .bind(&store.scope().conversation_id)
    .fetch_one(&mut **transaction)
    .await?;
    if scope_count != 1 {
        bail!("EventBatch transaction scope no longer matches authenticated agent scope");
    }

    let mut refs: HashMap<&str, (DataKeyPurpose, &[u8])> = HashMap::new();
    for write in prepared {
        if let Some(event) = &write.event {
            insert_prepared_key_expectation(
                &mut refs,
                &event.raw_key_ref,
                DataKeyPurpose::Event,
                &event.raw_key_proof,
            )?;
        }
        for projection in &write.projections {
            match projection {
                PreparedProjection::MessageEnd {
                    raw_key_ref,
                    raw_key_proof,
                    ..
                } => {
                    insert_prepared_key_expectation(
                        &mut refs,
                        raw_key_ref,
                        DataKeyPurpose::Transcript,
                        raw_key_proof,
                    )?;
                }
                PreparedProjection::CommandInsert {
                    payload_key_ref,
                    payload_key_proof,
                    ..
                } => insert_prepared_key_expectation(
                    &mut refs,
                    payload_key_ref,
                    DataKeyPurpose::Command,
                    payload_key_proof,
                )?,
                PreparedProjection::Plain(_) => {}
            }
        }
    }

    for (key_ref, (purpose, expected_proof)) in refs {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM data_keys
             WHERE key_ref=? AND scope='conversation' AND purpose=?
               AND conversation_id=? AND state='active' AND algorithm=?
               AND wrap_key_id <> '' AND wrap_nonce IS NOT NULL AND wrapped_key IS NOT NULL
               AND destroyed_at IS NULL",
        )
        .bind(key_ref)
        .bind(purpose.as_str())
        .bind(&store.scope().conversation_id)
        .bind(super::crypto::WRAP_ALGORITHM)
        .fetch_one(&mut **transaction)
        .await?;
        if count != 1 {
            bail!(
                "prepared {} key {key_ref} is not active with complete wrapped material in EventBatch transaction",
                purpose.as_str()
            );
        }
        let key = store
            .data_key_by_ref_in_transaction(transaction, key_ref)
            .await?;
        if key.purpose != purpose {
            bail!(
                "prepared {} key {key_ref} changed purpose in EventBatch transaction",
                purpose.as_str()
            );
        }
        verify_keyed_digest(&key, PREPARED_KEY_MATERIAL_PROOF, expected_proof).with_context(
            || {
                format!(
                    "prepared {} key {key_ref} changed material before EventBatch transaction",
                    purpose.as_str()
                )
            },
        )?;
    }
    Ok(())
}

fn insert_prepared_key_expectation<'a>(
    refs: &mut HashMap<&'a str, (DataKeyPurpose, &'a [u8])>,
    key_ref: &'a str,
    purpose: DataKeyPurpose,
    proof: &'a [u8],
) -> Result<()> {
    if let Some((expected_purpose, expected_proof)) = refs.insert(key_ref, (purpose, proof))
        && (expected_purpose != purpose || expected_proof != proof)
    {
        bail!("prepared key {key_ref} has conflicting purpose or material expectations");
    }
    Ok(())
}

fn abrupt_transaction_exit(name: &str, boundary: &str, readiness_path: &std::path::Path) -> ! {
    #[cfg(all(test, unix))]
    {
        use std::io::Write as _;

        let mut readiness = std::fs::File::create(readiness_path)
            .expect("create hard-kill transaction readiness marker");
        writeln!(readiness, "{name}.{boundary}")
            .expect("write hard-kill transaction readiness marker");
        readiness
            .sync_all()
            .expect("sync hard-kill transaction readiness marker");
        // This is intentionally not a Rust error/panic: no destructors run, so the
        // file-backed SQLite connection is abandoned exactly like a process kill.
        unsafe { libc::_exit(86) }
    }
    #[cfg(not(all(test, unix)))]
    panic!(
        "abrupt transaction failpoint {name}.{boundary} at {} is test-only",
        readiness_path.display()
    )
}

fn verify_digest_bytes(incoming: &[u8], stored: &[u8]) -> Result<()> {
    let mut difference = incoming.len() ^ stored.len();
    for (incoming, stored) in incoming.iter().zip(stored) {
        difference |= usize::from(incoming ^ stored);
    }
    if difference != 0 {
        bail!("command payload digest mismatch");
    }
    Ok(())
}

fn validate_batch_shape(redactor: &Redactor, batch: &EventBatch) -> Result<Vec<ExpectedInjection>> {
    if batch.writes.is_empty() {
        bail!("EventBatch must contain at least one write");
    }
    EventBatchSizer::validate(
        BatchBounds {
            command_count: batch.injected_commands.len(),
            command_plaintext_bytes: 0,
        },
        0,
    )?;
    let mut command_ids = HashSet::new();
    let mut message_ids = HashSet::new();
    let mut previous_seq = None;
    for command in &batch.injected_commands {
        let canonical_message_id = user_message_id(&command.command_id);
        if command.message_id != canonical_message_id {
            bail!(
                "injected command {} message_id is not the canonical UUIDv5 derivation",
                command.command_id
            );
        }
        if !command_ids.insert(command.command_id.as_str()) {
            bail!("duplicate injected command_id {}", command.command_id);
        }
        if !message_ids.insert(command.message_id.as_str()) {
            bail!("duplicate injected message_id {}", command.message_id);
        }
        if previous_seq.is_some_and(|previous| previous >= command.seq) {
            bail!("injected commands must be in strict durable sequence order");
        }
        previous_seq = Some(command.seq);
    }

    let mut expected_phases: HashMap<&str, RunPhase> = HashMap::new();
    let mut expected_injections = Vec::new();
    let mut projected_message_ids = HashSet::new();
    let mut projected_message_digests = HashMap::new();
    let mut command_insert_ids = HashSet::new();
    let mut command_insert_seqs = HashSet::new();
    let mut command_classification_ids = HashSet::new();
    let mut hard_classifications = HashMap::new();
    let mut phase_transitions = Vec::new();
    let mut command_terminal_ids = HashSet::new();
    let mut command_terminal_seqs = HashSet::new();
    let mut message_start_event_ids = HashSet::new();
    let mut message_start_event_digests = HashMap::new();
    let mut message_end_event_ids = HashSet::new();
    let mut approval_requested_event_ids: HashSet<String> = HashSet::new();
    let mut approval_resolved_event_ids: HashSet<String> = HashSet::new();
    let mut tool_start_event_ids: HashSet<String> = HashSet::new();
    let mut tool_end_event_ids: HashSet<String> = HashSet::new();
    let mut approval_requested_events = HashMap::new();
    let mut approval_resolved_events = HashMap::new();
    let mut tool_start_events = HashMap::new();
    let mut tool_end_events = HashMap::new();
    let mut tool_result_ids = HashSet::new();
    let mut tool_result_message_ids = HashMap::new();
    let mut tool_mutation_ids = HashSet::new();
    let mut tool_start_mutation_ids: HashSet<String> = HashSet::new();
    let mut tool_finish_mutation_ids: HashSet<String> = HashSet::new();
    let mut approval_mutation_ids = HashSet::new();
    let mut approval_pending_mutation_ids: HashSet<String> = HashSet::new();
    let mut approval_resolve_mutation_ids: HashSet<String> = HashSet::new();
    let mut approval_pending_mutations = HashMap::new();
    let mut approval_resolve_mutations = HashMap::new();
    let mut tool_finish_mutations = HashMap::new();
    let mut empty_turn_runs = HashSet::new();
    let mut agent_end_runs = HashSet::new();
    let mut superseded_runs = HashSet::new();
    for write in &batch.writes {
        if let Some(event) = &write.event {
            match &event.value {
                AgentEvent::MessageStart {
                    message_id,
                    message,
                } => {
                    if !message_start_event_ids.insert(message_id.as_str()) {
                        bail!("duplicate message_start event for message {message_id}");
                    }
                    message_start_event_digests
                        .insert(message_id.as_str(), Some(serde_json::to_value(message)?));
                }
                AgentEvent::MessageEnd { message_id, .. } => {
                    if !message_end_event_ids.insert(message_id.as_str()) {
                        bail!("duplicate message_end event for message {message_id}");
                    }
                }
                AgentEvent::ApprovalRequested { request } => {
                    if request.id.is_empty()
                        || request.tool_call_id.is_empty()
                        || request.tool_name.is_empty()
                    {
                        bail!("approval_requested request identity must not be empty");
                    }
                    let request_id = request.id.clone();
                    if !approval_requested_event_ids.insert(request_id.clone()) {
                        bail!("duplicate approval_requested event for request {request_id}");
                    }
                    approval_requested_events.insert(
                        request_id,
                        ApprovalRequestedEvent {
                            request: request.clone(),
                        },
                    );
                }
                AgentEvent::ApprovalResolved {
                    request_id,
                    resolution,
                } => {
                    let resolution = approval_resolution_state(resolution);
                    let actor = event.metadata.approval_actor.as_deref().ok_or_else(|| {
                        anyhow!("approval_resolved requires internal actor metadata")
                    })?;
                    if request_id.is_empty() || actor.is_empty() {
                        bail!("approval_resolved identity and actor must not be empty");
                    }
                    if !approval_resolved_event_ids.insert(request_id.clone()) {
                        bail!("duplicate approval_resolved event for request {request_id}");
                    }
                    approval_resolved_events.insert(
                        request_id.clone(),
                        ApprovalResolvedEvent {
                            resolution: resolution.to_owned(),
                            actor: actor.to_owned(),
                        },
                    );
                }
                AgentEvent::ToolExecutionStart {
                    tool_call_id,
                    tool_name,
                    args,
                } => {
                    if tool_call_id.is_empty() || tool_name.is_empty() {
                        bail!("tool_execution_start identity and tool name must not be empty");
                    }
                    if !args.is_object() || event.metadata.tool_state.as_deref() != Some("running")
                    {
                        bail!("tool_execution_start must carry state=running and object arguments");
                    }
                    if !tool_start_event_ids.insert(tool_call_id.clone()) {
                        bail!("duplicate tool_execution_start event for tool {tool_call_id}");
                    }
                    tool_start_events.insert(
                        tool_call_id.clone(),
                        ToolExecutionStartEvent {
                            state: "running".to_owned(),
                        },
                    );
                }
                AgentEvent::ToolExecutionEnd {
                    tool_call_id,
                    result,
                    is_error,
                } => {
                    let state = event.metadata.tool_state.as_deref().ok_or_else(|| {
                        anyhow!("tool_execution_end requires internal state metadata")
                    })?;
                    let error_code = event.metadata.tool_error_code.as_deref();
                    validate_terminal_tool_semantics(state, *is_error, error_code)?;
                    if tool_call_id.is_empty() {
                        bail!("tool_execution_end identity must not be empty");
                    }
                    if !tool_end_event_ids.insert(tool_call_id.clone()) {
                        bail!("duplicate tool_execution_end event for tool {tool_call_id}");
                    }
                    tool_end_events.insert(
                        tool_call_id.clone(),
                        ToolExecutionEndEvent {
                            state: state.to_owned(),
                            result: result.clone(),
                            is_error: *is_error,
                            error_code: error_code.map(str::to_owned),
                        },
                    );
                }
                AgentEvent::AgentStart | AgentEvent::AgentEnd => {
                    let run_id = event.metadata.run_id.as_deref().unwrap_or_default();
                    if run_id.is_empty() {
                        bail!("durable agent event run_id must not be empty");
                    }
                    if matches!(event.value, AgentEvent::AgentEnd) {
                        agent_end_runs.insert(run_id.to_owned());
                    }
                }
                AgentEvent::TurnStart | AgentEvent::TurnEnd { .. } => {
                    let run_id = event.metadata.run_id.as_deref().unwrap_or_default();
                    let turn_id = event.metadata.turn_id.as_deref().unwrap_or_default();
                    if run_id.is_empty() || turn_id.is_empty() {
                        bail!("durable turn event identity must not be empty");
                    }
                    if let AgentEvent::TurnEnd {
                        message,
                        tool_results,
                    } = &event.value
                    {
                        if message.is_none() {
                            if !event.metadata.empty_turn || !tool_results.is_empty() {
                                bail!(
                                    "TurnEnd message=None is reserved for a true empty idle-startup turn"
                                );
                            }
                            empty_turn_runs.insert(run_id.to_owned());
                        } else if event.metadata.empty_turn {
                            bail!("non-empty TurnEnd must not carry empty-turn metadata");
                        }
                    }
                }
                AgentEvent::Steered { .. } => {
                    let command_id = event.metadata.command_id.as_deref().unwrap_or_default();
                    let run_id = event.metadata.run_id.as_deref().unwrap_or_default();
                    let turn_id = event.metadata.turn_id.as_deref().unwrap_or_default();
                    if command_id.is_empty() || run_id.is_empty() || turn_id.is_empty() {
                        bail!("durable Steered identity must not be empty");
                    }
                }
                AgentEvent::RetryScheduled {
                    attempt,
                    delay_ms,
                    error_message,
                    ..
                } => {
                    if *attempt == 0 || *delay_ms == 0 || error_message.is_empty() {
                        bail!("durable RetryScheduled fields must be non-zero/non-empty");
                    }
                }
                AgentEvent::MemoryMaintenance { .. }
                | AgentEvent::MessageUpdate { .. }
                | AgentEvent::ToolExecutionUpdate { .. }
                | AgentEvent::Error { .. } => {
                    bail!("volatile or future AgentEvent cannot be persisted by T12");
                }
            }
        }
        for projection in &write.projections {
            if let Projection::CommandReceived { envelope } = projection
                && (!command_insert_ids.insert(envelope.command_id.as_str())
                    || !command_insert_seqs.insert(envelope.seq))
            {
                bail!(
                    "duplicate command receipt projection for command {} at sequence {}",
                    envelope.command_id,
                    envelope.seq
                );
            }
            if let Projection::CommandRejected {
                seq, command_id, ..
            } = projection
                && (!command_insert_ids.insert(command_id.as_str())
                    || !command_insert_seqs.insert(*seq))
            {
                bail!(
                    "duplicate command receipt projection for command {command_id} at sequence {seq}"
                );
            }
            if let Projection::CommandClassified { command_id, .. } = projection
                && !command_classification_ids.insert(command_id.as_str())
            {
                bail!("duplicate CommandClassified projection for command {command_id}");
            }
            if let Projection::CommandClassified {
                command_id,
                application_kind: ApplicationKind::HardSteer,
                run_id,
                ..
            } = projection
            {
                hard_classifications.insert(command_id.as_str(), run_id.as_str());
            }
            if let Projection::CommandApplied {
                command_id,
                command_seq,
                ..
            }
            | Projection::CommandSuperseded {
                command_id,
                command_seq,
                ..
            } = projection
                && (!command_terminal_ids.insert(command_id.as_str())
                    || !command_terminal_seqs.insert(*command_seq))
            {
                bail!(
                    "duplicate command terminal projection for command {command_id} at sequence {command_seq}"
                );
            }
            if let Projection::CommandSuperseded {
                run_id: Some(run_id),
                ..
            } = projection
            {
                superseded_runs.insert(run_id.clone());
            }
            if let Projection::Approval(mutation) = projection {
                let request_id = match mutation {
                    ApprovalMutation::Pending { request_id, .. }
                    | ApprovalMutation::Resolve { request_id, .. } => request_id,
                };
                if !approval_mutation_ids.insert(request_id.as_str()) {
                    bail!("duplicate approval mutation for request {request_id}");
                }
            }
            if let Projection::MessageEnd {
                message_id,
                role,
                message,
                append_to_l0,
                ..
            } = projection
            {
                l0_disposition(message, *append_to_l0)?;
                if !projected_message_ids.insert(message_id.as_str()) {
                    bail!("duplicate MessageEnd projection for message {message_id}");
                }
                projected_message_digests
                    .insert(message_id.as_str(), serde_json::to_value(message)?);
                let actual_role = match message {
                    PublicMessage::User(_) => "user",
                    PublicMessage::Assistant(_) => "assistant",
                    PublicMessage::ToolResult(message) => {
                        if !tool_result_ids.insert(message.tool_call_id.as_str()) {
                            bail!(
                                "duplicate tool-result MessageEnd for tool {}",
                                message.tool_call_id
                            );
                        }
                        tool_result_message_ids
                            .insert(message.tool_call_id.as_str(), message_id.as_str());
                        "tool_result"
                    }
                };
                if *role != actual_role {
                    bail!("MessageEnd role {role} does not match its {actual_role} message");
                }
                if *role == "user" {
                    let command = batch
                        .injected_commands
                        .get(expected_injections.len())
                        .ok_or_else(|| {
                            anyhow!("user MessageEnd has no injected command binding")
                        })?;
                    if command.message_id != *message_id {
                        bail!(
                            "injected command {} is bound to message {}, not {}",
                            command.command_id,
                            command.message_id,
                            message_id
                        );
                    }
                    let PublicMessage::User(message) = message else {
                        unreachable!("role and message variant were checked");
                    };
                    let [crate::provider::types::UserContent::Text { text }] =
                        message.content.as_slice()
                    else {
                        bail!(
                            "injected user MessageEnd must contain exactly one text content item"
                        );
                    };
                    expected_injections.push(ExpectedInjection {
                        text: Zeroizing::new(text.clone()),
                        timestamp: message.timestamp,
                    });
                }
            }
            if let Projection::ToolExecution(mutation) = projection {
                let tool_call_id = match mutation {
                    ToolExecutionMutation::Prepare { tool_call_id, .. }
                    | ToolExecutionMutation::Start { tool_call_id }
                    | ToolExecutionMutation::Finish { tool_call_id, .. } => tool_call_id,
                };
                if !tool_mutation_ids.insert(tool_call_id.as_str()) {
                    bail!("duplicate tool mutation for tool {tool_call_id}");
                }
                match mutation {
                    ToolExecutionMutation::Start { .. } => {
                        tool_start_mutation_ids.insert(tool_call_id.clone());
                    }
                    ToolExecutionMutation::Finish { .. } => {
                        tool_finish_mutation_ids.insert(tool_call_id.clone());
                        let ToolExecutionMutation::Finish {
                            expected,
                            state,
                            error_code,
                            ..
                        } = mutation
                        else {
                            unreachable!()
                        };
                        validate_tool_transition(expected, state)?;
                        validate_terminal_tool_semantics(
                            state,
                            *state != "succeeded",
                            *error_code,
                        )?;
                        tool_finish_mutations.insert(
                            tool_call_id.clone(),
                            ToolFinishEvidence {
                                expected: (*expected).to_owned(),
                                state: (*state).to_owned(),
                                error_code: error_code.map(str::to_owned),
                            },
                        );
                    }
                    ToolExecutionMutation::Prepare { .. } => {}
                }
            }
            if let Projection::Approval(mutation) = projection {
                match mutation {
                    ApprovalMutation::Pending { request_id, .. } => {
                        approval_pending_mutation_ids.insert(request_id.clone());
                        let ApprovalMutation::Pending {
                            tool_call_id,
                            request_projection,
                            ..
                        } = mutation
                        else {
                            unreachable!()
                        };
                        approval_pending_mutations.insert(
                            request_id.clone(),
                            ApprovalPendingEvidence {
                                tool_call_id: tool_call_id.clone(),
                                request_projection: request_projection.clone(),
                            },
                        );
                    }
                    ApprovalMutation::Resolve {
                        request_id,
                        state,
                        actor,
                    } => {
                        validate_approval_resolution(state)?;
                        if actor.is_empty() {
                            bail!("Approval Resolve actor must not be empty");
                        }
                        approval_resolve_mutation_ids.insert(request_id.clone());
                        approval_resolve_mutations.insert(
                            request_id.clone(),
                            ApprovalResolveEvidence {
                                resolution: (*state).to_owned(),
                                actor: actor.clone(),
                            },
                        );
                    }
                }
            }
            if let Projection::RunPhase {
                command_id,
                run_id,
                expected,
                next,
                ..
            } = projection
            {
                phase_transitions.push((command_id.as_str(), run_id.as_str(), *expected, *next));
                if let Some(previous_next) = expected_phases.insert(command_id, *next)
                    && previous_next != *expected
                {
                    bail!("conflicting expected phases for command {command_id}");
                }
                if !allowed_phase_transition(*expected, *next) {
                    bail!(
                        "invalid run phase transition {} -> {}",
                        expected.as_str(),
                        next.as_str()
                    );
                }
            }
        }
    }
    for run_id in &empty_turn_runs {
        if !agent_end_runs.contains(run_id) || !superseded_runs.contains(run_id) {
            bail!(
                "TurnEnd message=None requires same-batch idle-startup supersede and AgentEnd for run {run_id}"
            );
        }
    }
    let injected_command_ids: HashSet<&str> = batch
        .injected_commands
        .iter()
        .map(|command| command.command_id.as_str())
        .collect();
    for (command_id, _, expected, next) in &phase_transitions {
        if matches!(
            (*expected, *next),
            (RunPhase::TurnStarted, RunPhase::UserStarted)
                | (RunPhase::UserStarted, RunPhase::UserCommitted)
        ) && !injected_command_ids.contains(command_id)
        {
            bail!(
                "{} -> {} for {command_id} requires its canonical injected command binding and user message events",
                expected.as_str(),
                next.as_str()
            );
        }
    }
    if expected_injections.len() != batch.injected_commands.len() {
        bail!(
            "injected user message count {} does not match durable command binding count {}",
            expected_injections.len(),
            batch.injected_commands.len()
        );
    }
    for (classification_id, run_id) in &hard_classifications {
        if !phase_transitions
            .iter()
            .any(|(owner_id, transition_run, expected, next)| {
                owner_id != classification_id
                    && transition_run == run_id
                    && *expected == RunPhase::AssistantStarted
                    && *next == RunPhase::HardSteerRequested
            })
        {
            bail!(
                "hard steer classification {classification_id} requires the active owner's assistant_started -> hard_steer_requested transition"
            );
        }
    }
    for (_, run_id, expected, next) in &phase_transitions {
        if *expected == RunPhase::AssistantStarted
            && *next == RunPhase::HardSteerRequested
            && !hard_classifications
                .values()
                .any(|classification_run| classification_run == run_id)
        {
            bail!(
                "assistant_started -> hard_steer_requested requires a hard steer classification in the same EventBatch"
            );
        }
    }
    for command in &batch.injected_commands {
        if !message_start_event_ids.contains(command.message_id.as_str()) {
            bail!(
                "injected command {} requires its user MessageStart in the same EventBatch",
                command.command_id
            );
        }
        if !phase_transitions
            .iter()
            .any(|(command_id, _, expected, next)| {
                *command_id == command.command_id.as_str()
                    && *expected == RunPhase::UserStarted
                    && *next == RunPhase::UserCommitted
            })
        {
            bail!(
                "user MessageEnd for command {} requires user_started -> user_committed in the same EventBatch",
                command.command_id
            );
        }
        let projected = projected_message_digests
            .get(command.message_id.as_str())
            .ok_or_else(|| {
                anyhow!(
                    "injected command {} has no canonical user message projection",
                    command.command_id
                )
            })?;
        let started = message_start_event_digests
            .get(command.message_id.as_str())
            .and_then(Option::as_ref)
            .ok_or_else(|| {
                anyhow!(
                    "injected command {} MessageStart has no complete message",
                    command.command_id
                )
            })?;
        if started != projected {
            bail!(
                "injected command {} MessageStart does not match its user MessageEnd",
                command.command_id
            );
        }
    }
    require_matching_targets(
        "message_end event",
        &message_end_event_ids,
        "MessageEnd projection",
        &projected_message_ids,
    )?;
    require_matching_targets(
        "approval_requested event",
        &approval_requested_event_ids,
        "Approval Pending mutation",
        &approval_pending_mutation_ids,
    )?;
    require_matching_targets(
        "approval_resolved event",
        &approval_resolved_event_ids,
        "Approval Resolve mutation",
        &approval_resolve_mutation_ids,
    )?;
    require_matching_targets(
        "tool_execution_start event",
        &tool_start_event_ids,
        "ToolExecution Start mutation",
        &tool_start_mutation_ids,
    )?;
    require_matching_targets(
        "tool_execution_end event",
        &tool_end_event_ids,
        "ToolExecution Finish mutation",
        &tool_finish_mutation_ids,
    )?;
    for tool_call_id in &tool_finish_mutation_ids {
        let Some(message_id) = tool_result_message_ids.get(tool_call_id.as_str()) else {
            bail!(
                "terminal tool mutation for {tool_call_id} requires its tool-result MessageEnd in the same EventBatch"
            );
        };
        if !message_start_event_ids.contains(message_id) {
            bail!(
                "terminal tool mutation for {tool_call_id} requires tool-result MessageStart and MessageEnd in the same EventBatch"
            );
        }
    }
    for (tool_call_id, event) in &tool_start_events {
        if !tool_start_mutation_ids.contains(tool_call_id.as_str()) {
            continue;
        }
        if event.state != "running" {
            bail!("tool start event and mutation disagree for {tool_call_id}");
        }
    }
    for (tool_call_id, mutation) in &tool_finish_mutations {
        let event = tool_end_events
            .get(tool_call_id)
            .ok_or_else(|| anyhow!("missing typed tool_execution_end event for {tool_call_id}"))?;
        if event.state != mutation.state || event.error_code != mutation.error_code {
            bail!("tool terminal event and mutation disagree for {tool_call_id}");
        }
        if mutation.expected == "prepared" && mutation.state != "cancelled" {
            bail!("only cancellation may terminate a prepared tool");
        }
        let message_id = tool_result_message_ids
            .get(tool_call_id.as_str())
            .expect("terminal message presence was checked");
        let result = projected_message_digests
            .get(message_id)
            .ok_or_else(|| anyhow!("missing tool-result message for {tool_call_id}"))?;
        let result_message: crate::provider::types::ToolResultMessage =
            serde_json::from_value(result.clone())
                .context("tool-result MessageEnd projection is invalid")?;
        if event.result != *result || event.is_error != result_message.is_error {
            bail!("tool terminal event result does not match result message for {tool_call_id}");
        }
    }
    for (request_id, mutation) in &approval_pending_mutations {
        let event = approval_requested_events
            .get(request_id)
            .ok_or_else(|| anyhow!("missing typed approval_requested event for {request_id}"))?;
        if event.request.tool_call_id != mutation.tool_call_id {
            bail!("approval request identity does not match mutation for {request_id}");
        }
        let raw_request = serde_json::to_value(&event.request)?;
        let redacted_request = redactor.redact_value(&raw_request)?;
        let supplied_projection: Value = serde_json::from_str(&mutation.request_projection)
            .context("approval request projection is invalid JSON")?;
        if redacted_request != supplied_projection {
            bail!("approval request projection does not match its event for {request_id}");
        }
    }
    for (request_id, mutation) in &approval_resolve_mutations {
        let event = approval_resolved_events
            .get(request_id)
            .ok_or_else(|| anyhow!("missing typed approval_resolved event for {request_id}"))?;
        if event.resolution != mutation.resolution || event.actor != mutation.actor {
            bail!("approval resolution event and mutation disagree for {request_id}");
        }
    }
    Ok(expected_injections)
}

fn validate_approval_resolution(resolution: &str) -> Result<()> {
    if !matches!(
        resolution,
        "approved_once" | "approved_always" | "denied" | "cancelled"
    ) {
        bail!("invalid terminal approval resolution");
    }
    Ok(())
}

fn approval_resolution_state(resolution: &ApprovalResolution) -> &'static str {
    match resolution {
        ApprovalResolution::Decision(ApprovalDecision::ApproveOnce) => "approved_once",
        ApprovalResolution::Decision(ApprovalDecision::ApproveAlways { .. }) => "approved_always",
        ApprovalResolution::Decision(ApprovalDecision::Deny) => "denied",
        ApprovalResolution::Cancelled => "cancelled",
    }
}

fn validate_tool_transition(expected: &str, state: &str) -> Result<()> {
    match (expected, state) {
        ("running", "succeeded" | "failed" | "cancelled" | "indeterminate")
        | ("prepared", "cancelled") => Ok(()),
        _ => bail!("invalid terminal tool transition"),
    }
}

fn validate_terminal_tool_semantics(
    state: &str,
    is_error: bool,
    error_code: Option<&str>,
) -> Result<()> {
    let state_is_error = matches!(state, "failed" | "cancelled" | "indeterminate");
    if state == "succeeded" {
        if is_error || error_code.is_some() {
            bail!("succeeded tool result must be non-error and have no error_code");
        }
        return Ok(());
    }
    if !state_is_error || !is_error || error_code.is_none() {
        bail!("failed tool result must be is_error=true with an error_code");
    }
    if !matches!(
        error_code,
        Some("executor_failed" | "cancelled" | "indeterminate" | "invalid_result" | "internal")
    ) {
        bail!("unknown terminal tool error_code");
    }
    Ok(())
}

fn require_matching_targets<T>(
    event_kind: &str,
    event_ids: &HashSet<T>,
    projection_kind: &str,
    projection_ids: &HashSet<T>,
) -> Result<()>
where
    T: Display + Eq + Hash,
{
    if let Some(id) = event_ids.difference(projection_ids).next() {
        bail!("{event_kind} for {id} has no matching {projection_kind}");
    }
    if let Some(id) = projection_ids.difference(event_ids).next() {
        bail!("{projection_kind} for {id} has no matching {event_kind}");
    }
    Ok(())
}

fn l0_disposition(message: &PublicMessage, append_to_l0: bool) -> Result<L0Disposition> {
    match message {
        PublicMessage::Assistant(message) if message.stop_reason == StopReason::Error => {
            if append_to_l0 {
                bail!("assistant MessageEnd with stop_reason=error must use append_to_l0=false");
            }
            Ok(L0Disposition::ExcludeRetryError)
        }
        PublicMessage::User(_) | PublicMessage::Assistant(_) | PublicMessage::ToolResult(_) => {
            if !append_to_l0 {
                bail!(
                    "non-error MessageEnd must use append_to_l0=true; only retry Error assistant messages are excluded"
                );
            }
            Ok(L0Disposition::Append)
        }
    }
}

fn allowed_phase_transition(expected: RunPhase, next: RunPhase) -> bool {
    matches!(
        (expected, next),
        (RunPhase::Classified, RunPhase::RunStarted)
            | (RunPhase::RunStarted, RunPhase::TurnStarted)
            | (RunPhase::Classified, RunPhase::TurnStarted)
            | (RunPhase::TurnStarted, RunPhase::UserStarted)
            | (RunPhase::UserStarted, RunPhase::UserCommitted)
            | (RunPhase::UserCommitted, RunPhase::AssistantStarted)
            | (RunPhase::AssistantStarted, RunPhase::HardSteerRequested)
            | (RunPhase::AssistantStarted, RunPhase::CancelRequested)
            | (RunPhase::HardSteerRequested, RunPhase::CancelRequested)
            | (RunPhase::UserStarted, RunPhase::CancelRequested)
            | (RunPhase::UserCommitted, RunPhase::CancelRequested)
    )
}

fn validate_message_end_event(
    event: &PreparedEvent,
    message_id: &str,
    message_digest: [u8; 32],
    message_projection: &str,
) -> Result<()> {
    let identity = event
        .message_end
        .as_ref()
        .ok_or_else(|| anyhow!("MessageEnd projection must accompany a message_end event"))?;
    if identity.message_id != message_id {
        bail!("MessageEnd projection message_id does not match durable raw event");
    }
    if message_digest != identity.message_digest {
        bail!("MessageEnd event and message projection contain different raw messages");
    }

    let value: Value =
        serde_json::from_str(&event.envelope).context("redacted event is invalid JSON")?;
    if value.get("type").and_then(Value::as_str) != Some("message_end") {
        bail!("MessageEnd projection must accompany a message_end event");
    }
    if value.get("message_id").and_then(Value::as_str) != Some(message_id) {
        bail!("MessageEnd projection message_id does not match durable event");
    }
    let projected_message: Value = serde_json::from_str(message_projection)
        .context("redacted MessageEnd payload is invalid JSON")?;
    if value.get("message") != Some(&projected_message) {
        bail!("MessageEnd event and message projection contain different messages");
    }
    Ok(())
}

fn message_end_identity(event: &AgentEvent) -> Result<Option<MessageEndIdentity>> {
    let AgentEvent::MessageEnd {
        message_id,
        message,
    } = event
    else {
        return Ok(None);
    };
    Ok(Some(MessageEndIdentity {
        message_id: message_id.clone(),
        message_digest: Sha256::digest(serde_json::to_vec(&serde_json::to_value(message)?)?).into(),
    }))
}

fn preflight_materialization_bounds(redactor: &Redactor, batch: &EventBatch) -> Result<()> {
    let max_components = super::sizer::EVENT_BATCH_MAX_BYTES / DURABLE_ROW_OVERHEAD_BYTES;
    if batch.writes.len() > max_components {
        bail!(
            "EventBatch has {} writes, exceeding bounded materialization count {max_components}",
            batch.writes.len()
        );
    }
    let mut components = batch.writes.len();
    let mut preflight_bytes = 0usize;
    for write in &batch.writes {
        components = components
            .checked_add(write.projections.len())
            .ok_or_else(|| anyhow!("EventBatch component count overflow"))?;
        if components > max_components {
            bail!("EventBatch has more than {max_components} event/projection components");
        }
        if let Some(event) = &write.event {
            let metadata_bytes = event
                .metadata
                .command_id
                .as_ref()
                .map_or(0, String::len)
                .checked_add(event.metadata.run_id.as_ref().map_or(0, String::len))
                .and_then(|bytes| {
                    bytes.checked_add(event.metadata.turn_id.as_ref().map_or(0, String::len))
                })
                .and_then(|bytes| {
                    bytes.checked_add(event.metadata.tool_state.as_ref().map_or(0, String::len))
                })
                .and_then(|bytes| {
                    bytes.checked_add(
                        event
                            .metadata
                            .tool_error_code
                            .as_ref()
                            .map_or(0, String::len),
                    )
                })
                .and_then(|bytes| {
                    bytes.checked_add(
                        event
                            .metadata
                            .approval_actor
                            .as_ref()
                            .map_or(0, String::len),
                    )
                })
                .ok_or_else(|| anyhow!("durable event metadata byte count overflow"))?;
            if event.raw_json.len() > super::sizer::EVENT_BATCH_MAX_BYTES
                || metadata_bytes > super::sizer::EVENT_BATCH_MAX_BYTES
            {
                bail!("one durable event exceeds the EventBatch materialization bound");
            }
            let projection = redactor
                .redact_serialized(&event.raw_json)
                .context("failed durable event redaction preflight")?;
            let internal_metadata = serde_json::to_vec(&event.metadata)
                .context("failed durable event metadata preflight")?;
            let event_bytes = event
                .raw_json
                .len()
                .checked_add(1 + 24 + 16)
                .and_then(|bytes| bytes.checked_add(projection.len()))
                .and_then(|bytes| {
                    bytes.checked_add(
                        event
                            .value
                            .durable_kind()
                            .expect("DurableEvent contains a durable kind")
                            .len(),
                    )
                })
                .and_then(|bytes| bytes.checked_add(internal_metadata.len()))
                .and_then(|bytes| bytes.checked_add(DURABLE_ROW_OVERHEAD_BYTES))
                .ok_or_else(|| anyhow!("durable event preflight byte count overflow"))?;
            charge_transaction_bytes(&mut preflight_bytes, event_bytes)?;
        }
        for projection in &write.projections {
            let projection_bytes = match projection {
                Projection::MessageEnd { message, .. } => {
                    let raw = serde_json::to_vec(message)
                        .context("failed bounded MessageEnd preflight")?;
                    let projected = redactor.redact_serialized(&raw)?;
                    let search_text = search_text_from_projection(&projected)?;
                    raw.len()
                        .checked_add(1 + 24 + 16)
                        .and_then(|bytes| bytes.checked_add(projected.len()))
                        .and_then(|bytes| bytes.checked_add(search_text.len()))
                        .and_then(|bytes| bytes.checked_add(DURABLE_ROW_OVERHEAD_BYTES))
                        .ok_or_else(|| anyhow!("MessageEnd preflight byte count overflow"))?
                }
                projection => projection_size_upper_bound(projection)?,
            };
            if projection_bytes > super::sizer::EVENT_BATCH_MAX_BYTES {
                bail!("one projection exceeds the EventBatch durable bytes bound");
            }
            charge_transaction_bytes(&mut preflight_bytes, projection_bytes)?;
        }
    }
    Ok(())
}

fn charge_transaction_bytes(total: &mut usize, bytes: usize) -> Result<()> {
    *total = total
        .checked_add(bytes)
        .ok_or_else(|| anyhow!("EventBatch durable byte count overflow"))?;
    EventBatchSizer::validate(BatchBounds::default(), *total)?;
    Ok(())
}

fn projection_size_upper_bound(projection: &Projection) -> Result<usize> {
    let content_bytes = match projection {
        Projection::CommandReceived { envelope } => serde_json::to_vec(&envelope.command)?.len(),
        Projection::CommandRejected { raw_command, .. } => {
            raw_command.authenticated_bytes().map_or(0, <[u8]>::len)
        }
        Projection::CommandClassified {
            command_id,
            run_id,
            turn_id,
            ..
        } => command_id
            .len()
            .saturating_add(run_id.len())
            .saturating_add(turn_id.len()),
        Projection::RunPhase {
            command_id, run_id, ..
        } => command_id.len().saturating_add(run_id.len()),
        Projection::CommandApplied {
            command_id, run_id, ..
        }
        | Projection::CommandSuperseded {
            command_id, run_id, ..
        } => command_id
            .len()
            .saturating_add(run_id.as_ref().map_or(0, String::len)),
        Projection::ToolExecution(mutation) => match mutation {
            ToolExecutionMutation::Prepare {
                tool_call_id,
                command_id,
                run_id,
                idempotency_key,
                ..
            } => tool_call_id
                .len()
                .saturating_add(command_id.len())
                .saturating_add(run_id.len())
                .saturating_add(idempotency_key.len()),
            ToolExecutionMutation::Start { tool_call_id }
            | ToolExecutionMutation::Finish { tool_call_id, .. } => tool_call_id.len(),
        },
        Projection::Approval(mutation) => match mutation {
            ApprovalMutation::Pending {
                request_id,
                tool_call_id,
                run_id,
                turn_id,
                request_projection,
                ..
            } => request_id
                .len()
                .saturating_add(tool_call_id.len())
                .saturating_add(run_id.len())
                .saturating_add(turn_id.len())
                .saturating_add(request_projection.len()),
            ApprovalMutation::Resolve { request_id, .. } => request_id.len(),
        },
        Projection::MessageEnd { .. } => 0,
        #[cfg(test)]
        Projection::SizePadding(bytes) => return Ok(*bytes),
    };
    Ok(content_bytes.saturating_add(512))
}

fn prepared_projection_size(projection: &PreparedProjection) -> usize {
    match projection {
        PreparedProjection::CommandInsert {
            command_id,
            payload_key_ref,
            payload_ciphertext,
            payload_hmac,
            ..
        } => command_id
            .len()
            .saturating_add(payload_key_ref.len())
            .saturating_add(payload_ciphertext.as_ref().map_or(0, Vec::len))
            .saturating_add(payload_hmac.len())
            .saturating_add(512),
        _ => 0,
    }
}

fn prepared_injection_bytes(
    prepared: &[PreparedWrite],
    commands: &[InjectedCommand],
    sizing: &InjectionSizing,
) -> Result<usize> {
    let message_ids: HashSet<&str> = commands
        .iter()
        .map(|command| command.message_id.as_str())
        .collect();
    let command_ids: HashSet<&str> = commands
        .iter()
        .map(|command| command.command_id.as_str())
        .collect();
    let owner_transfer_ids: HashSet<&str> = sizing
        .previous_owner_command_id
        .iter()
        .map(CommandId::as_str)
        .chain(
            commands
                .iter()
                .take(commands.len().saturating_sub(1))
                .map(|command| command.command_id.as_str()),
        )
        .collect();
    let mut bytes = 0usize;
    let mut message_event_rows = 0usize;
    let mut message_rows = 0usize;
    for write in prepared {
        let related_projection = write.projections.iter().any(|projection| match projection {
            PreparedProjection::MessageEnd { message_id, .. } => {
                message_ids.contains(message_id.as_str())
            }
            PreparedProjection::Plain(Projection::RunPhase {
                command_id, run_id, ..
            }) => command_ids.contains(command_id.as_str()) && run_id == &sizing.run_id,
            PreparedProjection::Plain(Projection::CommandApplied {
                command_id, run_id, ..
            }) => {
                owner_transfer_ids.contains(command_id.as_str())
                    && run_id.as_deref() == Some(sizing.run_id.as_str())
            }
            _ => false,
        });
        let related_event = write.event.as_ref().is_some_and(|event| {
            event
                .message_id
                .as_deref()
                .is_some_and(|message_id| message_ids.contains(message_id))
                || event
                    .command_id
                    .as_deref()
                    .is_some_and(|command_id| command_ids.contains(command_id))
                || (event.run_id.as_deref() == Some(sizing.run_id.as_str())
                    && match event.kind.as_str() {
                        "agent_start" => sizing.application == InjectionApplication::IdleRun,
                        "turn_start" => {
                            sizing.application != InjectionApplication::RetrySteer
                                && event.turn_id.as_deref() == Some(sizing.turn_id.as_str())
                        }
                        _ => false,
                    })
        });
        if !related_projection && !related_event {
            continue;
        }
        if let Some(event) = &write.event {
            bytes = bytes
                .saturating_add(event.raw_ciphertext.len())
                .saturating_add(event.envelope.len())
                .saturating_add(event.kind.len())
                .saturating_add(event.internal_metadata.len())
                .saturating_add(DURABLE_ROW_OVERHEAD_BYTES);
            if matches!(event.kind.as_str(), "message_start" | "message_end")
                && event
                    .message_id
                    .as_deref()
                    .is_some_and(|message_id| message_ids.contains(message_id))
            {
                message_event_rows = message_event_rows.saturating_add(1);
            }
        }
        for projection in &write.projections {
            match projection {
                PreparedProjection::MessageEnd {
                    message_id,
                    raw_ciphertext,
                    payload,
                    search_text,
                    ..
                } => {
                    bytes = bytes
                        .saturating_add(raw_ciphertext.len())
                        .saturating_add(payload.len())
                        .saturating_add(search_text.len())
                        .saturating_add(DURABLE_ROW_OVERHEAD_BYTES);
                    if message_ids.contains(message_id.as_str()) {
                        message_rows = message_rows.saturating_add(1);
                    }
                }
                PreparedProjection::CommandInsert { .. } => {
                    bytes = bytes.saturating_add(prepared_projection_size(projection));
                }
                PreparedProjection::Plain(projection) => {
                    bytes = bytes.saturating_add(projection_size_upper_bound(projection)?);
                }
            }
        }
    }
    if message_event_rows != commands.len().saturating_mul(2) || message_rows != commands.len() {
        bail!(
            "prepared injection write-set is incomplete: {} commands, {message_event_rows} message events, {message_rows} messages",
            commands.len()
        );
    }
    Ok(bytes)
}

fn collect_owner_conditions(
    prepared: &[PreparedWrite],
    pre: &mut HashSet<String>,
    post: &mut HashSet<String>,
) {
    let mut chains: HashMap<&str, (&str, RunPhase, RunPhase)> = HashMap::new();
    for write in prepared {
        for projection in &write.projections {
            if let PreparedProjection::Plain(Projection::RunPhase {
                command_id,
                run_id,
                expected,
                next,
                ..
            }) = projection
            {
                chains
                    .entry(command_id)
                    .and_modify(|(_, _, final_phase)| *final_phase = *next)
                    .or_insert((run_id, *expected, *next));
            }
        }
    }
    for (_, (run_id, initial_phase, final_phase)) in chains {
        if initial_phase.is_owner() {
            pre.insert(run_id.to_owned());
        }
        if final_phase.is_owner() {
            post.insert(run_id.to_owned());
        }
    }
}

fn has_durable_event(
    prepared: &[PreparedWrite],
    kind: &str,
    command_id: Option<&str>,
    run_id: Option<&str>,
    turn_id: Option<&str>,
    message_role: Option<&str>,
) -> bool {
    prepared.iter().any(|write| {
        write.event.as_ref().is_some_and(|event| {
            event.kind == kind
                && command_id.is_none_or(|value| event.command_id.as_deref() == Some(value))
                && run_id.is_none_or(|value| event.run_id.as_deref() == Some(value))
                && turn_id.is_none_or(|value| event.turn_id.as_deref() == Some(value))
                && message_role.is_none_or(|value| event.message_role.as_deref() == Some(value))
        })
    })
}

fn durable_event_position(
    prepared: &[PreparedWrite],
    kind: &str,
    run_id: &str,
    turn_id: Option<&str>,
) -> Option<usize> {
    prepared.iter().position(|write| {
        write.event.as_ref().is_some_and(|event| {
            event.kind == kind
                && event.run_id.as_deref() == Some(run_id)
                && turn_id.is_none_or(|value| event.turn_id.as_deref() == Some(value))
        })
    })
}

fn require_durable_event(
    prepared: &[PreparedWrite],
    kind: &str,
    command_id: Option<&str>,
    run_id: Option<&str>,
    turn_id: Option<&str>,
    message_role: Option<&str>,
) -> Result<()> {
    if !has_durable_event(prepared, kind, command_id, run_id, turn_id, message_role) {
        bail!(
            "run phase transition requires exact {kind} event pair (command={command_id:?}, run={run_id:?}, turn={turn_id:?}, role={message_role:?})"
        );
    }
    Ok(())
}

async fn validate_zero_owner_startup_abort(
    transaction: &mut Transaction<'_, Sqlite>,
    prepared: &[PreparedWrite],
    phase_transitions: &[(&str, &str, RunPhase, RunPhase)],
    contextual_supersedes: &[(&str, u64, &str)],
    run_id: &str,
) -> Result<()> {
    let startups = sqlx::query(
        "SELECT command_id, application_kind, turn_id, run_phase
         FROM inbound_commands
         WHERE run_id = ? AND command_kind = 'user_message' AND status = 'applying'",
    )
    .bind(run_id)
    .fetch_all(&mut **transaction)
    .await?;
    if startups.len() != 1 {
        bail!(
            "zero-owner Abort for run {run_id} requires exactly one pre-user idle startup, found {}",
            startups.len()
        );
    }
    let startup = &startups[0];
    let command_id: String = startup.try_get("command_id")?;
    let application_kind: String = startup.try_get("application_kind")?;
    let turn_id: String = startup.try_get("turn_id")?;
    let phase = RunPhase::parse(startup.try_get("run_phase")?)?;
    if application_kind != "idle_run"
        || !matches!(
            phase,
            RunPhase::Classified | RunPhase::RunStarted | RunPhase::TurnStarted
        )
    {
        bail!(
            "zero-owner Abort target {command_id} is not a pre-user idle startup: {application_kind}/{}",
            phase.as_str()
        );
    }
    if phase_transitions
        .iter()
        .any(|(_, transition_run, _, next)| {
            *transition_run == run_id && *next == RunPhase::CancelRequested
        })
    {
        bail!("zero-owner startup Abort must not emit cancel_requested");
    }
    if !contextual_supersedes
        .iter()
        .any(|(superseded_id, _, superseded_run)| {
            *superseded_id == command_id && *superseded_run == run_id
        })
    {
        bail!(
            "zero-owner Abort for run {run_id} requires CommandSuperseded for startup {command_id}"
        );
    }

    let has_turn_end = has_durable_event(
        prepared,
        "turn_end",
        None,
        Some(run_id),
        Some(&turn_id),
        None,
    );
    let has_agent_end = has_durable_event(prepared, "agent_end", None, Some(run_id), None, None);
    match phase {
        RunPhase::Classified if has_turn_end || has_agent_end => {
            bail!("classified idle startup Abort must not close events that never started");
        }
        RunPhase::Classified => {}
        RunPhase::RunStarted if !has_agent_end || has_turn_end => {
            bail!("run_started idle startup Abort requires AgentEnd without TurnEnd");
        }
        RunPhase::RunStarted => {}
        RunPhase::TurnStarted if !has_agent_end || !has_turn_end => {
            bail!("turn_started idle startup Abort requires TurnEnd followed by AgentEnd");
        }
        RunPhase::TurnStarted => {
            let turn_end = durable_event_position(prepared, "turn_end", run_id, Some(&turn_id))
                .expect("TurnEnd presence was checked");
            let agent_end = durable_event_position(prepared, "agent_end", run_id, None)
                .expect("AgentEnd presence was checked");
            if turn_end >= agent_end {
                bail!("turn_started idle startup Abort must order TurnEnd before AgentEnd");
            }
        }
        _ => unreachable!("startup phase was validated"),
    }
    Ok(())
}

async fn validate_required_projection_sets(
    store: &Store,
    transaction: &mut Transaction<'_, Sqlite>,
    prepared: &[PreparedWrite],
) -> Result<()> {
    let mut phase_transitions = Vec::new();
    let mut approval_resolutions = HashMap::new();
    let mut tool_starts = HashSet::new();
    let mut applied_controls = Vec::new();
    let mut contextual_supersedes = Vec::new();
    for write in prepared {
        for projection in &write.projections {
            match projection {
                PreparedProjection::Plain(Projection::RunPhase {
                    command_id,
                    run_id,
                    expected,
                    next,
                }) => {
                    phase_transitions.push((command_id.as_str(), run_id.as_str(), *expected, *next))
                }
                PreparedProjection::Plain(Projection::Approval(ApprovalMutation::Resolve {
                    request_id,
                    state,
                    actor,
                    ..
                })) => {
                    approval_resolutions.insert(request_id.as_str(), (*state, actor.as_str()));
                }
                PreparedProjection::Plain(Projection::ToolExecution(
                    ToolExecutionMutation::Start { tool_call_id },
                )) => {
                    tool_starts.insert(tool_call_id.as_str());
                }
                PreparedProjection::Plain(Projection::CommandApplied {
                    command_id,
                    command_seq,
                    run_id,
                }) => {
                    applied_controls.push((command_id.as_str(), *command_seq, run_id.as_deref()));
                }
                PreparedProjection::Plain(Projection::CommandSuperseded {
                    command_id,
                    command_seq,
                    run_id: Some(run_id),
                }) => {
                    contextual_supersedes.push((command_id.as_str(), *command_seq, run_id.as_str()))
                }
                _ => {}
            }
        }
    }

    for (command_id, run_id, expected, next) in &phase_transitions {
        let row = sqlx::query(
            "SELECT application_kind, turn_id
             FROM inbound_commands
             WHERE command_id = ? AND run_id = ? AND command_kind = 'user_message'
               AND status = 'applying'",
        )
        .bind(command_id)
        .bind(run_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| anyhow!("RunPhase target {command_id} has no durable command binding"))?;
        let application_kind: String = row.try_get("application_kind")?;
        let turn_id: String = row.try_get("turn_id")?;
        match (*expected, *next) {
            (RunPhase::Classified, RunPhase::RunStarted) => {
                require_durable_event(prepared, "agent_start", None, Some(run_id), None, None)?;
            }
            (RunPhase::RunStarted, RunPhase::TurnStarted) => {
                require_durable_event(
                    prepared,
                    "turn_start",
                    None,
                    Some(run_id),
                    Some(&turn_id),
                    None,
                )?;
            }
            (RunPhase::Classified, RunPhase::TurnStarted) => {
                require_durable_event(
                    prepared,
                    "steered",
                    Some(command_id),
                    Some(run_id),
                    Some(&turn_id),
                    None,
                )?;
                let has_turn_start = has_durable_event(
                    prepared,
                    "turn_start",
                    None,
                    Some(run_id),
                    Some(&turn_id),
                    None,
                );
                match application_kind.as_str() {
                    "hard_steer" | "soft_steer" if !has_turn_start => {
                        bail!(
                            "{application_kind} classified -> turn_started requires its TurnStart"
                        );
                    }
                    "retry_steer" if has_turn_start => {
                        bail!("retry_steer classified -> turn_started must not emit TurnStart");
                    }
                    "hard_steer" | "soft_steer" | "retry_steer" => {}
                    _ => bail!(
                        "classified -> turn_started is invalid for application kind {application_kind}"
                    ),
                }
            }
            (RunPhase::UserCommitted, RunPhase::AssistantStarted) => {
                require_durable_event(
                    prepared,
                    "message_start",
                    None,
                    None,
                    None,
                    Some("assistant"),
                )?;
            }
            _ => {}
        }
    }

    for write in prepared {
        let Some(event) = &write.event else {
            continue;
        };
        match event.kind.as_str() {
            "agent_start" => {
                let run_id = event
                    .run_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("agent_start event has no run_id"))?;
                if !phase_transitions
                    .iter()
                    .any(|(_, transition_run, expected, next)| {
                        *transition_run == run_id
                            && *expected == RunPhase::Classified
                            && *next == RunPhase::RunStarted
                    })
                {
                    bail!("AgentStart for run {run_id} has no classified -> run_started pair");
                }
            }
            "turn_start" => {
                let run_id = event
                    .run_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("turn_start event has no run_id"))?;
                let turn_id = event
                    .turn_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("turn_start event has no turn_id"))?;
                let mut paired = false;
                for (command_id, transition_run, expected, next) in &phase_transitions {
                    if *transition_run != run_id
                        || !matches!(
                            (*expected, *next),
                            (RunPhase::RunStarted, RunPhase::TurnStarted)
                                | (RunPhase::Classified, RunPhase::TurnStarted)
                        )
                    {
                        continue;
                    }
                    let stored_turn: Option<String> = sqlx::query_scalar(
                        "SELECT turn_id FROM inbound_commands WHERE command_id = ?",
                    )
                    .bind(command_id)
                    .fetch_optional(&mut **transaction)
                    .await?;
                    if stored_turn.as_deref() == Some(turn_id) {
                        paired = true;
                        break;
                    }
                }
                if !paired {
                    bail!("TurnStart for {run_id}/{turn_id} has no exact phase transition pair");
                }
            }
            "steered" => {
                let command_id = event
                    .command_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("steered event has no command_id"))?;
                let run_id = event
                    .run_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("steered event has no run_id"))?;
                if !phase_transitions.iter().any(
                    |(transition_command, transition_run, expected, next)| {
                        *transition_command == command_id
                            && *transition_run == run_id
                            && *expected == RunPhase::Classified
                            && *next == RunPhase::TurnStarted
                    },
                ) {
                    bail!("Steered for {command_id} has no classified -> turn_started pair");
                }
            }
            _ => {}
        }
    }

    let mut consumed_approval_resolutions = HashSet::new();
    let mut active_abort_runs = HashSet::new();
    let mut user_owner_close_runs = HashSet::new();
    for (command_id, command_seq, contextual_run_id) in applied_controls {
        let row = sqlx::query(
            "SELECT command_kind, payload_key_ref, payload_ciphertext, payload_hmac
             FROM inbound_commands WHERE command_id = ? AND seq = ?",
        )
        .bind(command_id)
        .bind(sqlite_i64(command_seq, "command sequence")?)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| anyhow!("CommandApplied target does not exist"))?;
        let command_kind: String = row.try_get("command_kind")?;
        match command_kind.as_str() {
            "abort" => {
                if let Some(run_id) = contextual_run_id {
                    active_abort_runs.insert(run_id.to_owned());
                    let owner_count: i64 = sqlx::query_scalar(
                        "SELECT COUNT(*) FROM inbound_commands
                         WHERE run_id = ? AND command_kind = 'user_message'
                           AND status = 'applying'
                           AND run_phase IN (
                             'user_started', 'user_committed', 'assistant_started',
                             'hard_steer_requested', 'cancel_requested'
                           )",
                    )
                    .bind(run_id)
                    .fetch_one(&mut **transaction)
                    .await?;
                    if owner_count == 1
                        && !phase_transitions
                            .iter()
                            .any(|(_, transition_run, _, next)| {
                                *transition_run == run_id && *next == RunPhase::CancelRequested
                            })
                    {
                        bail!(
                            "active Abort CommandApplied requires the owner's cancel_requested transition"
                        );
                    }
                    if owner_count > 1 {
                        bail!("active Abort run {run_id} has multiple durable owners");
                    }
                    if owner_count == 0 {
                        validate_zero_owner_startup_abort(
                            transaction,
                            prepared,
                            &phase_transitions,
                            &contextual_supersedes,
                            run_id,
                        )
                        .await?;
                    }
                } else {
                    let live_or_starting_runs: i64 = sqlx::query_scalar(
                        "SELECT COUNT(*) FROM inbound_commands
                         WHERE command_kind='user_message' AND status='applying'",
                    )
                    .fetch_one(&mut **transaction)
                    .await?;
                    if live_or_starting_runs != 0 {
                        bail!(
                            "run_id=None Abort is valid only in true Idle with no live owner or startup"
                        );
                    }
                }
            }
            "approval_decision" => {
                let key_ref: String = row.try_get("payload_key_ref")?;
                let ciphertext: Vec<u8> = row.try_get("payload_ciphertext")?;
                let payload_hmac: Vec<u8> = row.try_get("payload_hmac")?;
                let key = store
                    .data_key_by_ref_in_transaction(transaction, &key_ref)
                    .await?;
                let aad = store.scope().row_aad(
                    "inbound_commands",
                    command_seq.to_string(),
                    DataKeyPurpose::Command,
                );
                let plaintext =
                    Zeroizing::new(super::crypto::decrypt_content(&key, &ciphertext, &aad)?);
                verify_keyed_digest(&key, &plaintext, &payload_hmac)
                    .context("durable ApprovalDecision HMAC is invalid")?;
                let command: Command = serde_json::from_slice(&plaintext)
                    .context("durable ApprovalDecision payload is invalid")?;
                let Command::ApprovalDecision {
                    request_id,
                    decision,
                } = command
                else {
                    bail!("durable approval_decision row contains a different command variant");
                };
                match contextual_run_id {
                    Some(run_id) => {
                        let expected_resolution = match decision {
                            ApprovalDecision::ApproveOnce => "approved_once",
                            ApprovalDecision::Deny => "denied",
                            ApprovalDecision::ApproveAlways { .. } => {
                                bail!(
                                    "active ApproveAlways requires the T22/T23 durable policy mutation path"
                                )
                            }
                        };
                        let Some((resolution, actor)) =
                            approval_resolutions.get(request_id.as_str())
                        else {
                            bail!(
                                "active ApprovalDecision CommandApplied requires ApprovalResolved for {request_id}"
                            );
                        };
                        if *resolution != expected_resolution {
                            bail!(
                                "ApprovalDecision {request_id} maps to {expected_resolution}, not {resolution}"
                            );
                        }
                        if *actor == "runtime" {
                            bail!("user ApprovalDecision cannot use the runtime resolution actor");
                        }
                        let approval = sqlx::query(
                            "SELECT run_id, tool_call_id FROM approval_log
                             WHERE id = ? AND state = 'pending'",
                        )
                        .bind(&request_id)
                        .fetch_optional(&mut **transaction)
                        .await?;
                        let Some(approval) = approval else {
                            bail!(
                                "ApprovalDecision {request_id} does not resolve a pending approval"
                            );
                        };
                        let approval_run: String = approval.try_get("run_id")?;
                        let tool_call_id: String = approval.try_get("tool_call_id")?;
                        if approval_run != run_id {
                            bail!(
                                "ApprovalDecision {request_id} does not resolve a pending approval in run {run_id}"
                            );
                        }
                        let tool_state: Option<String> = sqlx::query_scalar(
                            "SELECT state FROM tool_executions WHERE tool_call_id = ?",
                        )
                        .bind(&tool_call_id)
                        .fetch_optional(&mut **transaction)
                        .await?;
                        if tool_state.as_deref() != Some("prepared") {
                            bail!(
                                "ApprovalDecision {request_id} is not bound to a prepared tool execution"
                            );
                        }
                        if expected_resolution == "denied" && !tool_starts.is_empty() {
                            bail!("denied ApprovalDecision cannot co-commit ToolExecutionStart");
                        }
                        if !tool_starts.is_empty()
                            && (!tool_starts.contains(tool_call_id.as_str())
                                || tool_starts.len() != 1)
                        {
                            bail!(
                                "ApprovalDecision {request_id} can start only its pending tool {tool_call_id}"
                            );
                        }
                        consumed_approval_resolutions.insert(request_id);
                    }
                    None if approval_resolutions.contains_key(request_id.as_str()) => {
                        bail!(
                            "no-op ApprovalDecision cannot carry ApprovalResolved for {request_id}"
                        );
                    }
                    None => {}
                }
            }
            "user_message" => {
                let run_id = contextual_run_id
                    .ok_or_else(|| anyhow!("UserMessage CommandApplied requires run_id"))?;
                user_owner_close_runs.insert(run_id.to_owned());
                let normal_close =
                    has_durable_event(prepared, "agent_end", None, Some(run_id), None, None);
                let handoff =
                    phase_transitions
                        .iter()
                        .any(|(next_owner, transition_run, expected, next)| {
                            *next_owner != command_id
                                && *transition_run == run_id
                                && *expected == RunPhase::TurnStarted
                                && *next == RunPhase::UserStarted
                        });
                if !normal_close && !handoff {
                    bail!(
                        "UserMessage owner {command_id} may finish only with AgentEnd or same-run atomic owner handoff"
                    );
                }
            }
            value => bail!("CommandApplied cannot target command kind {value}"),
        }
    }
    for (request_id, (resolution, actor)) in &approval_resolutions {
        if *resolution == "cancelled" {
            if *actor != "runtime" {
                bail!("cancelled ApprovalResolved must use the runtime actor");
            }
            if !tool_starts.is_empty() {
                bail!("cancelled ApprovalResolved cannot co-commit ToolExecutionStart");
            }
        } else if !consumed_approval_resolutions.contains(*request_id) {
            bail!(
                "ApprovalResolved for {request_id} requires its active ApprovalDecision CommandApplied"
            );
        }
    }
    for (_, run_id, _, next) in &phase_transitions {
        if *next == RunPhase::CancelRequested && !active_abort_runs.contains(*run_id) {
            bail!(
                "owner cancel_requested transition for run {run_id} requires active Abort CommandApplied"
            );
        }
    }
    for write in prepared {
        let Some(event) = &write.event else {
            continue;
        };
        if event.kind != "agent_end" {
            continue;
        }
        let run_id = event
            .run_id
            .as_deref()
            .ok_or_else(|| anyhow!("agent_end event has no run_id"))?;
        let closes_owner = user_owner_close_runs.contains(run_id);
        let closes_startup = contextual_supersedes
            .iter()
            .any(|(_, _, superseded_run)| *superseded_run == run_id);
        if !closes_owner && !closes_startup {
            bail!("AgentEnd for run {run_id} has no owner close or startup supersede");
        }
    }
    for (command_id, command_seq, contextual_run_id) in contextual_supersedes {
        let status: String = sqlx::query_scalar(
            "SELECT status FROM inbound_commands
             WHERE command_id = ? AND seq = ? AND command_kind = 'user_message'",
        )
        .bind(command_id)
        .bind(sqlite_i64(command_seq, "command sequence")?)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| anyhow!("CommandSuperseded target does not exist"))?;
        if status == "received" && !active_abort_runs.contains(contextual_run_id) {
            bail!(
                "unclassified UserMessage contextual supersede requires active Abort for run {contextual_run_id}"
            );
        }
    }
    Ok(())
}

async fn validate_owner_open_preconditions(
    transaction: &mut Transaction<'_, Sqlite>,
    prepared: &[PreparedWrite],
) -> Result<()> {
    for write in prepared {
        for projection in &write.projections {
            let PreparedProjection::Plain(Projection::RunPhase {
                command_id,
                run_id,
                expected: RunPhase::TurnStarted,
                next: RunPhase::UserStarted,
            }) = projection
            else {
                continue;
            };
            let application_kind: String = sqlx::query_scalar(
                "SELECT application_kind FROM inbound_commands
                 WHERE command_id = ? AND run_id = ? AND status = 'applying'
                   AND run_phase IN ('classified', 'turn_started')",
            )
            .bind(command_id)
            .bind(run_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "owner-opening transition target {command_id} has no durable pre-injection phase"
                )
            })?;
            let expected_owners = i64::from(application_kind != "idle_run");
            require_owner_count(transaction, run_id, expected_owners).await?;
        }
    }
    Ok(())
}

fn collect_classification_owner_conditions(
    prepared: &[PreparedWrite],
) -> Result<HashMap<String, i64>> {
    let mut conditions = HashMap::new();
    for write in prepared {
        for projection in &write.projections {
            let PreparedProjection::Plain(Projection::CommandClassified {
                application_kind,
                run_id,
                ..
            }) = projection
            else {
                continue;
            };
            let expected_owners = i64::from(*application_kind != ApplicationKind::IdleRun);
            if let Some(previous) = conditions.insert(run_id.clone(), expected_owners)
                && previous != expected_owners
            {
                bail!("EventBatch has conflicting owner conditions for run {run_id}");
            }
        }
    }
    Ok(conditions)
}

async fn require_owner_count(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &str,
    expected: i64,
) -> Result<()> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inbound_commands
         WHERE run_id = ? AND command_kind = 'user_message' AND status = 'applying'
           AND run_phase IN (
             'user_started', 'user_committed', 'assistant_started',
             'hard_steer_requested', 'cancel_requested'
           )",
    )
    .bind(run_id)
    .fetch_one(&mut **transaction)
    .await?;
    if count != expected {
        bail!("run {run_id} owner invariant failed: expected {expected} owner, found {count}");
    }
    Ok(())
}

async fn apply_projection(
    transaction: &mut Transaction<'_, Sqlite>,
    projection: PreparedProjection,
) -> Result<()> {
    match projection {
        PreparedProjection::MessageEnd {
            event_seq,
            message_id,
            role,
            raw_key_ref,
            raw_key_proof: _,
            raw_ciphertext,
            payload,
            search_text,
            redaction_version,
            interrupted,
            l0_disposition,
        } => {
            if !matches!(role, "user" | "assistant" | "tool_result") {
                bail!("invalid message role {role}");
            }
            // T12 freezes the exact L0 membership decision at the MessageEnd
            // boundary. A later membership projection must consume this typed
            // value rather than reinterpret role or stop_reason from payload.
            match l0_disposition {
                L0Disposition::Append | L0Disposition::ExcludeRetryError => {}
            }
            sqlx::query(
                "INSERT INTO messages(
                    id, seq, role, raw_key_ref, raw_ciphertext, payload, search_text,
                    redaction_version, interrupted, created_at
                 ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(message_id)
            .bind(sqlite_i64(event_seq, "message event sequence")?)
            .bind(role)
            .bind(raw_key_ref)
            .bind(raw_ciphertext)
            .bind(payload)
            .bind(search_text)
            .bind(redaction_version as i64)
            .bind(i64::from(interrupted))
            .bind(Utc::now().to_rfc3339())
            .execute(&mut **transaction)
            .await
            .context("failed to apply MessageEnd projection")?;
        }
        PreparedProjection::CommandInsert {
            seq,
            command_id,
            command_kind,
            payload_key_ref,
            payload_key_proof: _,
            payload_ciphertext,
            payload_hmac,
            status,
            reject_reason,
            reject_actual_bytes,
        } => {
            sqlx::query(
                "INSERT INTO inbound_commands(
                    seq, command_id, command_kind, payload_ciphertext, payload_key_ref,
                    payload_hmac, status, reject_reason, reject_actual_bytes,
                    application_kind, run_id, turn_id, run_phase, received_at, applied_at
                 ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 'received', ?, ?)",
            )
            .bind(sqlite_i64(seq, "command sequence")?)
            .bind(command_id)
            .bind(command_kind)
            .bind(payload_ciphertext)
            .bind(payload_key_ref)
            .bind(payload_hmac)
            .bind(status)
            .bind(reject_reason)
            .bind(
                reject_actual_bytes
                    .map(|value| sqlite_i64(value, "rejected command byte count"))
                    .transpose()?,
            )
            .bind(Utc::now().to_rfc3339())
            .bind(if status == "rejected" {
                Some(Utc::now().to_rfc3339())
            } else {
                None
            })
            .execute(&mut **transaction)
            .await
            .context("failed to persist inbound command")?;
        }
        PreparedProjection::Plain(projection) => {
            apply_plain_projection(transaction, projection).await?;
        }
    }
    Ok(())
}

async fn apply_plain_projection(
    transaction: &mut Transaction<'_, Sqlite>,
    projection: Projection,
) -> Result<()> {
    match projection {
        Projection::MessageEnd { .. } => unreachable!("MessageEnd is prepared separately"),
        Projection::CommandReceived { .. } | Projection::CommandRejected { .. } => {
            unreachable!("command insert is prepared separately")
        }
        Projection::CommandClassified {
            command_id,
            application_kind,
            run_id,
            turn_id,
        } => {
            let result = sqlx::query(
                "UPDATE inbound_commands
                 SET status = 'applying', application_kind = ?, run_id = ?, turn_id = ?,
                     run_phase = 'classified'
                 WHERE command_id = ? AND command_kind = 'user_message'
                   AND status = 'received' AND run_phase = 'received'
                   AND application_kind IS NULL AND run_id IS NULL AND turn_id IS NULL",
            )
            .bind(application_kind.as_str())
            .bind(run_id)
            .bind(turn_id)
            .bind(command_id)
            .execute(&mut **transaction)
            .await?;
            require_single_cas(result.rows_affected(), "CommandClassified")?;
        }
        Projection::RunPhase {
            command_id,
            run_id,
            expected,
            next,
        } => {
            let result = sqlx::query(
                "UPDATE inbound_commands SET run_phase = ?
                 WHERE command_id = ? AND command_kind = 'user_message'
                   AND status = 'applying' AND run_id = ? AND run_phase = ?",
            )
            .bind(next.as_str())
            .bind(command_id)
            .bind(run_id)
            .bind(expected.as_str())
            .execute(&mut **transaction)
            .await?;
            require_single_cas(result.rows_affected(), "RunPhase")?;
        }
        Projection::CommandApplied {
            command_id,
            command_seq,
            run_id,
        } => {
            let command_kind: String = sqlx::query_scalar(
                "SELECT command_kind FROM inbound_commands WHERE command_id = ? AND seq = ?",
            )
            .bind(&command_id)
            .bind(sqlite_i64(command_seq, "command sequence")?)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or_else(|| anyhow!("CommandApplied target does not exist"))?;
            let affected = if command_kind == "user_message" {
                let run_id = run_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("UserMessage CommandApplied requires run_id"))?;
                sqlx::query(
                    "UPDATE inbound_commands
                     SET status = 'applied', run_phase = 'finished', applied_at = ?
                     WHERE command_id = ? AND seq = ? AND command_kind = 'user_message'
                       AND status = 'applying' AND run_id = ?
                       AND run_phase IN (
                         'user_started', 'user_committed', 'assistant_started',
                         'hard_steer_requested', 'cancel_requested'
                       )",
                )
                .bind(Utc::now().to_rfc3339())
                .bind(command_id)
                .bind(sqlite_i64(command_seq, "command sequence")?)
                .bind(run_id)
                .execute(&mut **transaction)
                .await?
                .rows_affected()
            } else if matches!(command_kind.as_str(), "abort" | "approval_decision") {
                sqlx::query(
                    "UPDATE inbound_commands SET status = 'applied', applied_at = ?
                     WHERE command_id = ? AND seq = ?
                       AND command_kind IN ('abort', 'approval_decision')
                       AND status = 'received' AND run_phase = 'received'",
                )
                .bind(Utc::now().to_rfc3339())
                .bind(command_id)
                .bind(sqlite_i64(command_seq, "command sequence")?)
                .execute(&mut **transaction)
                .await?
                .rows_affected()
            } else {
                bail!("CommandApplied cannot target command kind {command_kind}");
            };
            require_single_cas(affected, "CommandApplied")?;
        }
        Projection::CommandSuperseded {
            command_id,
            command_seq,
            run_id,
        } => {
            let row = sqlx::query(
                "SELECT status, application_kind, run_id, turn_id, run_phase
                 FROM inbound_commands
                 WHERE command_id = ? AND seq = ? AND command_kind = 'user_message'",
            )
            .bind(&command_id)
            .bind(sqlite_i64(command_seq, "command sequence")?)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or_else(|| anyhow!("CommandSuperseded target does not exist"))?;
            let stored_status: String = row.try_get("status")?;
            let result = match stored_status.as_str() {
                "received"
                    if row
                        .try_get::<Option<String>, _>("application_kind")?
                        .is_none()
                        && row.try_get::<Option<String>, _>("run_id")?.is_none()
                        && row.try_get::<Option<String>, _>("turn_id")?.is_none()
                        && row.try_get::<String, _>("run_phase")? == "received" =>
                {
                    // `run_id` is projection context for the active aborted run.
                    // It must not become the stored classification/binding.
                    sqlx::query(
                        "UPDATE inbound_commands SET status = 'superseded', applied_at = ?
                         WHERE command_id = ? AND seq = ? AND command_kind = 'user_message'
                           AND status = 'received' AND run_id IS NULL AND turn_id IS NULL
                           AND application_kind IS NULL AND run_phase = 'received'",
                    )
                    .bind(Utc::now().to_rfc3339())
                    .bind(command_id)
                    .bind(sqlite_i64(command_seq, "command sequence")?)
                    .execute(&mut **transaction)
                    .await?
                }
                "applying" => {
                    let run_id = run_id.as_deref().ok_or_else(|| {
                        anyhow!("classified CommandSuperseded requires its stored run binding")
                    })?;
                    sqlx::query(
                        "UPDATE inbound_commands SET status = 'superseded', applied_at = ?
                         WHERE command_id = ? AND seq = ? AND command_kind = 'user_message'
                           AND status = 'applying' AND run_id = ?
                           AND run_phase IN ('classified', 'run_started', 'turn_started')",
                    )
                    .bind(Utc::now().to_rfc3339())
                    .bind(command_id)
                    .bind(sqlite_i64(command_seq, "command sequence")?)
                    .bind(run_id)
                    .execute(&mut **transaction)
                    .await?
                }
                _ => bail!("CommandSuperseded target has invalid durable state {stored_status}"),
            };
            require_single_cas(result.rows_affected(), "CommandSuperseded")?;
        }
        Projection::ToolExecution(mutation) => {
            apply_tool_mutation(transaction, mutation).await?;
        }
        Projection::Approval(mutation) => {
            apply_approval_mutation(transaction, mutation).await?;
        }
        #[cfg(test)]
        Projection::SizePadding(_) => {}
    }
    Ok(())
}

fn command_kind(command: &Command) -> &'static str {
    match command {
        Command::UserMessage { .. } => "user_message",
        Command::Abort {} => "abort",
        Command::ApprovalDecision { .. } => "approval_decision",
    }
}

async fn apply_tool_mutation(
    transaction: &mut Transaction<'_, Sqlite>,
    mutation: ToolExecutionMutation,
) -> Result<()> {
    match mutation {
        ToolExecutionMutation::Prepare {
            tool_call_id,
            command_id,
            run_id,
            executor_generation,
            idempotency_key,
        } => {
            sqlx::query(
                "INSERT INTO tool_executions(
                    tool_call_id, command_id, run_id, executor_generation, state,
                    idempotency_key, started_at, finished_at, error_code
                 ) VALUES(?, ?, ?, ?, 'prepared', ?, NULL, NULL, NULL)",
            )
            .bind(tool_call_id)
            .bind(command_id)
            .bind(run_id)
            .bind(sqlite_i64(executor_generation, "executor generation")?)
            .bind(idempotency_key)
            .execute(&mut **transaction)
            .await?;
        }
        ToolExecutionMutation::Start { tool_call_id } => {
            let result = sqlx::query(
                "UPDATE tool_executions SET state = 'running', started_at = ?
                 WHERE tool_call_id = ? AND state = 'prepared'",
            )
            .bind(Utc::now().to_rfc3339())
            .bind(tool_call_id)
            .execute(&mut **transaction)
            .await?;
            require_single_cas(result.rows_affected(), "ToolExecutionStart")?;
        }
        ToolExecutionMutation::Finish {
            tool_call_id,
            expected,
            state,
            error_code,
        } => {
            if !matches!(
                state,
                "succeeded" | "failed" | "cancelled" | "indeterminate"
            ) {
                bail!("invalid terminal tool state {state}");
            }
            let result = sqlx::query(
                "UPDATE tool_executions
                 SET state = ?, finished_at = ?, error_code = ?
                 WHERE tool_call_id = ? AND state = ?",
            )
            .bind(state)
            .bind(Utc::now().to_rfc3339())
            .bind(error_code)
            .bind(tool_call_id)
            .bind(expected)
            .execute(&mut **transaction)
            .await?;
            require_single_cas(result.rows_affected(), "ToolExecutionEnd")?;
        }
    }
    Ok(())
}

async fn apply_approval_mutation(
    transaction: &mut Transaction<'_, Sqlite>,
    mutation: ApprovalMutation,
) -> Result<()> {
    match mutation {
        ApprovalMutation::Pending {
            request_id,
            tool_call_id,
            run_id,
            turn_id,
            request_projection,
            redaction_version,
        } => {
            sqlx::query(
                "INSERT INTO approval_log(
                    id, tool_call_id, run_id, turn_id, state, request_projection,
                    redaction_version, created_at, decided_at
                 ) VALUES(?, ?, ?, ?, 'pending', ?, ?, ?, NULL)",
            )
            .bind(request_id)
            .bind(tool_call_id)
            .bind(run_id)
            .bind(turn_id)
            .bind(request_projection)
            .bind(redaction_version as i64)
            .bind(Utc::now().to_rfc3339())
            .execute(&mut **transaction)
            .await?;
        }
        ApprovalMutation::Resolve {
            request_id,
            state,
            actor: _,
        } => {
            if !matches!(
                state,
                "approved_once" | "approved_always" | "denied" | "cancelled"
            ) {
                bail!("invalid terminal approval state {state}");
            }
            let result = sqlx::query(
                "UPDATE approval_log SET state = ?, decided_at = ?
                 WHERE id = ? AND state = 'pending'",
            )
            .bind(state)
            .bind(Utc::now().to_rfc3339())
            .bind(request_id)
            .execute(&mut **transaction)
            .await?;
            require_single_cas(result.rows_affected(), "ApprovalResolved")?;
        }
    }
    Ok(())
}

fn require_single_cas(rows_affected: u64, operation: &str) -> Result<()> {
    if rows_affected != 1 {
        bail!("{operation} CAS expected one row, updated {rows_affected}");
    }
    Ok(())
}

fn sqlite_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{field} exceeds SQLite INTEGER range"))
}

fn sqlite_u64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("{field} is negative"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::{Result, bail};
    use chrono::Utc;
    use serde_json::json;
    use sqlx::Row;

    use super::*;
    use crate::{
        gateway::{Command, SensitiveCommandPayload},
        provider::types::{
            PublicAssistantMessage, PublicMessage, StopReason, ToolResultMessage, Usage,
            UserContent, UserMessage,
        },
        store::{
            AgentScope, KeyProvider, RecoveryStep, SuffixRecovery,
            crypto::{
                DATA_KEY_BYTES, DataKeyMaterial, DataKeyScope, KeyWrapAad, WrappingKey,
                decrypt_content, encrypt_content, wrap_data_key,
            },
            sizer::{
                EVENT_BATCH_MAX_BYTES, InjectionApplication, InjectionBatchSizeInput,
                InjectionCommandSizeInput, canonical_user_message,
            },
        },
    };

    struct TestKeyProvider {
        key: WrappingKey,
    }

    #[async_trait::async_trait]
    impl KeyProvider for TestKeyProvider {
        async fn current_key(&self) -> Result<WrappingKey> {
            Ok(self.key.clone())
        }

        async fn key_by_id(&self, key_id: &str) -> Result<WrappingKey> {
            if key_id != self.key.key_id() {
                bail!("unknown test key");
            }
            Ok(self.key.clone())
        }
    }

    fn scope() -> AgentScope {
        AgentScope {
            tenant_id: "tenant-1".to_owned(),
            agent_id: "agent-1".to_owned(),
            conversation_id: "conversation-1".to_owned(),
        }
    }

    fn test_provider() -> Arc<dyn KeyProvider> {
        Arc::new(TestKeyProvider {
            key: WrappingKey::new("test-wrap-v1", [0x53; DATA_KEY_BYTES]),
        })
    }

    async fn test_store() -> Arc<Store> {
        Store::in_memory(scope(), test_provider())
            .await
            .expect("open test store")
            .into()
    }

    fn user_command(seq: u64, command_id: &str, text: &str) -> InboundCommand {
        InboundCommand::Valid(CommandEnvelope {
            seq,
            command_id: CommandId::parse(command_id).expect("canonical test command UUID"),
            command: Command::UserMessage {
                text: text.to_owned(),
                attachments: Vec::new(),
            },
        })
    }

    fn abort_command(seq: u64, command_id: &str) -> InboundCommand {
        InboundCommand::Valid(CommandEnvelope {
            seq,
            command_id: CommandId::parse(command_id).expect("canonical test command UUID"),
            command: Command::Abort {},
        })
    }

    fn approval_command(seq: u64, command_id: &str, request_id: &str) -> InboundCommand {
        approval_command_with_decision(seq, command_id, request_id, ApprovalDecision::ApproveOnce)
    }

    fn approval_command_with_decision(
        seq: u64,
        command_id: &str,
        request_id: &str,
        decision: ApprovalDecision,
    ) -> InboundCommand {
        InboundCommand::Valid(CommandEnvelope {
            seq,
            command_id: CommandId::parse(command_id).expect("canonical test command UUID"),
            command: Command::ApprovalDecision {
                request_id: request_id.to_owned(),
                decision,
            },
        })
    }

    fn durable_test_timestamp() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-07-20T01:02:03.456789Z")
            .expect("valid test timestamp")
            .with_timezone(&Utc)
    }

    fn approval_request(id: &str, tool_call_id: &str, risk: &str) -> Value {
        json!({
            "id": id,
            "tool_call_id": tool_call_id,
            "tool_name": "test",
            "action": {"reviewable":{"risk":risk}},
            "args_summary": {"path":"/workspace/report.txt"},
            "reason": null,
            "audit": null
        })
    }

    fn approval_request_projection(id: &str, tool_call_id: &str, risk: &str) -> String {
        approval_request(id, tool_call_id, risk).to_string()
    }

    async fn seed_pending_approval(
        store: &Arc<Store>,
        writer: &EventWriter,
        request_id: &str,
        tool_call_id: &str,
        run_id: &str,
    ) {
        let request = approval_request(request_id, tool_call_id, "mutating");
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({
                            "type":"approval_requested",
                            "request":request.clone()
                        }))
                        .expect("typed pending approval event"),
                    ),
                    projections: vec![
                        Projection::ToolExecution(ToolExecutionMutation::Prepare {
                            tool_call_id: tool_call_id.to_owned(),
                            command_id: format!("tool-command-{tool_call_id}"),
                            run_id: run_id.to_owned(),
                            executor_generation: 1,
                            idempotency_key: format!("idem-{tool_call_id}"),
                        }),
                        Projection::Approval(ApprovalMutation::Pending {
                            request_id: request_id.to_owned(),
                            tool_call_id: tool_call_id.to_owned(),
                            run_id: run_id.to_owned(),
                            turn_id: "turn-1".to_owned(),
                            request_projection: request.to_string(),
                            redaction_version: store.redactor().version(),
                        }),
                    ],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("seed pending approval and prepared tool");
    }

    fn approval_resolution_write(
        request_id: &str,
        state: &'static str,
        actor: &str,
        command_id: Option<(&str, u64, &str)>,
    ) -> EventWrite {
        let mut projections = vec![Projection::Approval(ApprovalMutation::Resolve {
            request_id: request_id.to_owned(),
            state,
            actor: actor.to_owned(),
        })];
        if let Some((command_id, command_seq, run_id)) = command_id {
            projections.push(Projection::CommandApplied {
                command_id: command_id.to_owned(),
                command_seq,
                run_id: Some(run_id.to_owned()),
            });
        }
        EventWrite {
            event: Some(
                DurableEvent::new(&json!({
                    "type":"approval_resolved",
                    "request_id":request_id,
                    "resolution":state,
                    "actor":actor
                }))
                .expect("typed approval resolution"),
            ),
            projections,
        }
    }

    fn tool_start_write(tool_call_id: &str) -> EventWrite {
        EventWrite {
            event: Some(
                DurableEvent::new(&json!({
                    "type":"tool_execution_start",
                    "tool_call_id":tool_call_id,
                    "tool_name":"test",
                    "args":{},
                    "state":"running"
                }))
                .expect("typed tool start"),
            ),
            projections: vec![Projection::ToolExecution(ToolExecutionMutation::Start {
                tool_call_id: tool_call_id.to_owned(),
            })],
        }
    }

    fn tool_result(tool_call_id: &str, text: &str, is_error: bool) -> PublicMessage {
        PublicMessage::ToolResult(ToolResultMessage {
            tool_call_id: tool_call_id.to_owned(),
            tool_name: "test".to_owned(),
            content: vec![UserContent::Text {
                text: text.to_owned(),
            }],
            details: json!({"text":text}),
            is_error,
            timestamp: durable_test_timestamp(),
        })
    }

    fn assistant_message(stop_reason: StopReason) -> PublicMessage {
        PublicMessage::Assistant(PublicAssistantMessage {
            content: Vec::new(),
            model: "test-model".to_owned(),
            provider: "test-provider".to_owned(),
            usage: Usage::default(),
            stop_reason,
            error_message: (stop_reason == StopReason::Error)
                .then(|| "retryable fixture".to_owned()),
            provider_code: None,
            interrupted: false,
            timestamp: durable_test_timestamp(),
        })
    }

    fn user_message(text: &str) -> PublicMessage {
        PublicMessage::User(UserMessage {
            content: vec![UserContent::Text {
                text: text.to_owned(),
            }],
            timestamp: durable_test_timestamp(),
        })
    }

    fn injection_writes(command_id: &str, _message_id: &str, text: &str) -> Vec<EventWrite> {
        injection_writes_at(command_id, "", text, durable_test_timestamp())
    }

    fn injection_writes_at(
        command_id: &str,
        _message_id: &str,
        text: &str,
        timestamp: DateTime<Utc>,
    ) -> Vec<EventWrite> {
        let message_id = user_message_id(command_id);
        let message = PublicMessage::User(UserMessage {
            content: vec![UserContent::Text {
                text: text.to_owned(),
            }],
            timestamp,
        });
        let run_id = format!("run-{command_id}");
        let turn_id = format!("turn-{command_id}");
        vec![
            EventWrite {
                event: Some(
                    DurableEvent::new(&json!({"type":"agent_start","run_id":run_id.clone()}))
                        .expect("serialize AgentStart"),
                ),
                projections: vec![Projection::RunPhase {
                    command_id: command_id.to_owned(),
                    run_id: run_id.clone(),
                    expected: RunPhase::Classified,
                    next: RunPhase::RunStarted,
                }],
            },
            EventWrite {
                event: Some(
                    DurableEvent::new(&json!({
                        "type":"turn_start",
                        "run_id":run_id.clone(),
                        "turn_id":turn_id,
                    }))
                    .expect("serialize TurnStart"),
                ),
                projections: vec![Projection::RunPhase {
                    command_id: command_id.to_owned(),
                    run_id: run_id.clone(),
                    expected: RunPhase::RunStarted,
                    next: RunPhase::TurnStarted,
                }],
            },
            EventWrite {
                event: Some(
                    DurableEvent::message("message_start", &message_id, &message)
                        .expect("serialize message start"),
                ),
                projections: vec![Projection::RunPhase {
                    command_id: command_id.to_owned(),
                    run_id: run_id.clone(),
                    expected: RunPhase::TurnStarted,
                    next: RunPhase::UserStarted,
                }],
            },
            EventWrite {
                event: Some(
                    DurableEvent::message("message_end", &message_id, &message)
                        .expect("serialize message end"),
                ),
                projections: vec![
                    Projection::MessageEnd {
                        message_id: message_id.clone(),
                        role: "user",
                        message,
                        append_to_l0: true,
                    },
                    Projection::RunPhase {
                        command_id: command_id.to_owned(),
                        run_id,
                        expected: RunPhase::UserStarted,
                        next: RunPhase::UserCommitted,
                    },
                ],
            },
        ]
    }

    async fn classified_injection(
        writer: &EventWriter,
        seq: u64,
        command_id: &str,
        _message_id: &str,
        text: &str,
    ) -> InjectedCommand {
        writer
            .persist_inbound(&user_command(seq, command_id, text))
            .await
            .expect("persist injected command");
        sqlx::query("UPDATE inbound_commands SET received_at=? WHERE command_id=?")
            .bind(durable_test_timestamp().to_rfc3339())
            .bind(command_id)
            .execute(writer.store.pool())
            .await
            .expect("pin durable receipt timestamp");
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: command_id.to_owned(),
                        application_kind: ApplicationKind::IdleRun,
                        run_id: format!("run-{command_id}"),
                        turn_id: format!("turn-{command_id}"),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("classify injected command");
        InjectedCommand::new(
            seq,
            CommandId::parse(command_id).expect("canonical test command UUID"),
        )
    }

    #[tokio::test]
    async fn event_batch_assigns_ordered_sequences_and_message_end_is_atomic() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let first = classified_injection(
            &writer,
            1,
            "00000000-0000-4000-8000-000000000001",
            "message-1",
            "first",
        )
        .await;
        let sequences = writer
            .apply(EventBatch {
                writes: injection_writes(
                    "00000000-0000-4000-8000-000000000001",
                    "message-1",
                    "first",
                ),
                injected_commands: vec![first],
            })
            .await
            .expect("commit batch");
        assert_eq!(sequences, vec![1, 2, 3, 4]);

        let events: Vec<i64> = sqlx::query_scalar("SELECT seq FROM agent_events ORDER BY seq")
            .fetch_all(store.pool())
            .await
            .expect("read event sequences");
        let messages: Vec<i64> = sqlx::query_scalar("SELECT seq FROM messages ORDER BY seq")
            .fetch_all(store.pool())
            .await
            .expect("read message sequences");
        assert_eq!(events, vec![1, 2, 3, 4]);
        assert_eq!(messages, vec![4]);
        let first_event = sqlx::query(
            "SELECT event_type, internal_metadata, envelope FROM agent_events WHERE seq=1",
        )
        .fetch_one(store.pool())
        .await
        .expect("read canonical AgentStart");
        assert_eq!(first_event.get::<String, _>("event_type"), "agent_start");
        assert_eq!(
            serde_json::from_str::<Value>(&first_event.get::<String, _>("envelope"))
                .expect("public event projection"),
            json!({"type":"agent_start"})
        );
        assert_eq!(
            serde_json::from_str::<Value>(&first_event.get::<String, _>("internal_metadata"))
                .expect("typed internal metadata"),
            json!({"run_id":"run-00000000-0000-4000-8000-000000000001"})
        );
    }

    #[tokio::test]
    async fn normal_idle_injection_full_sizer_matches_prepared_write_set() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let command_id =
            CommandId::parse("00000000-0000-4000-8000-000000000027").expect("canonical UUID");
        let text = "full write-set";
        let injected = classified_injection(&writer, 1, command_id.as_str(), "ignored", text).await;
        let batch = EventBatch {
            writes: injection_writes(command_id.as_str(), "ignored", text),
            injected_commands: vec![injected.clone()],
        };
        validate_batch_shape(store.redactor(), &batch).expect("valid injection shape");
        let (prepared, _, _) = writer
            .prepare_batch(batch, 1)
            .await
            .expect("prepare injection");
        let payload = serde_json::to_vec(&Command::UserMessage {
            text: text.to_owned(),
            attachments: Vec::new(),
        })
        .expect("canonical payload");
        let timestamp = durable_test_timestamp();
        let message_id = user_message_id(&command_id);
        let run_id = format!("run-{}", command_id.as_str());
        let turn_id = format!("turn-{}", command_id.as_str());
        let commands = [InjectionCommandSizeInput {
            command_id: &command_id,
            canonical_payload: &payload,
            message_id: &message_id,
            text,
            timestamp: &timestamp,
        }];
        let predicted = EventBatchSizer::injection_batch(
            store.redactor(),
            InjectionBatchSizeInput {
                application: InjectionApplication::IdleRun,
                run_id: &run_id,
                turn_id: &turn_id,
                previous_owner_command_id: None,
                commands: &commands,
            },
        )
        .expect("size full idle injection");
        let actual = prepared_injection_bytes(
            &prepared,
            &[injected],
            &InjectionSizing {
                size: predicted,
                application: InjectionApplication::IdleRun,
                run_id,
                turn_id,
                previous_owner_command_id: None,
            },
        )
        .expect("measure prepared write-set");
        assert_eq!(predicted.transaction_bytes, actual);
    }

    #[tokio::test]
    async fn steer_application_sizers_match_real_prepared_write_sets() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let command_id =
            CommandId::parse("00000000-0000-4000-8000-000000000033").expect("canonical UUID");
        let previous_owner =
            CommandId::parse("00000000-0000-4000-8000-000000000034").expect("canonical UUID");
        let message_id = user_message_id(&command_id);
        let run_id = "run-application-sizer";
        let turn_id = "turn-application-sizer";
        let text = "application-specific write-set";
        let timestamp = durable_test_timestamp();
        let message = canonical_user_message(text, timestamp);
        let payload = serde_json::to_vec(&Command::UserMessage {
            text: text.to_owned(),
            attachments: Vec::new(),
        })
        .expect("canonical payload");
        let commands = [InjectionCommandSizeInput {
            command_id: &command_id,
            canonical_payload: &payload,
            message_id: &message_id,
            text,
            timestamp: &timestamp,
        }];

        for application in [
            InjectionApplication::HardSteer,
            InjectionApplication::SoftSteer,
            InjectionApplication::RetrySteer,
        ] {
            let mut writes = vec![EventWrite {
                event: Some(
                    DurableEvent::new(&json!({
                        "type":"steered",
                        "command_id":command_id,
                        "run_id":run_id,
                        "turn_id":turn_id,
                        "mode":if application == InjectionApplication::HardSteer {
                            "hard"
                        } else {
                            "soft"
                        },
                    }))
                    .expect("Steered"),
                ),
                projections: vec![Projection::RunPhase {
                    command_id: command_id.as_str().to_owned(),
                    run_id: run_id.to_owned(),
                    expected: RunPhase::Classified,
                    next: RunPhase::TurnStarted,
                }],
            }];
            if application != InjectionApplication::RetrySteer {
                writes.push(EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({
                            "type":"turn_start",
                            "run_id":run_id,
                            "turn_id":turn_id,
                        }))
                        .expect("TurnStart"),
                    ),
                    projections: Vec::new(),
                });
            }
            writes.extend([
                EventWrite {
                    event: Some(
                        DurableEvent::message("message_start", &message_id, &message)
                            .expect("MessageStart"),
                    ),
                    projections: vec![
                        Projection::CommandApplied {
                            command_id: previous_owner.as_str().to_owned(),
                            command_seq: 1,
                            run_id: Some(run_id.to_owned()),
                        },
                        Projection::RunPhase {
                            command_id: command_id.as_str().to_owned(),
                            run_id: run_id.to_owned(),
                            expected: RunPhase::TurnStarted,
                            next: RunPhase::UserStarted,
                        },
                    ],
                },
                EventWrite {
                    event: Some(
                        DurableEvent::message("message_end", &message_id, &message)
                            .expect("MessageEnd"),
                    ),
                    projections: vec![
                        Projection::MessageEnd {
                            message_id: message_id.clone(),
                            role: "user",
                            message: message.clone(),
                            append_to_l0: true,
                        },
                        Projection::RunPhase {
                            command_id: command_id.as_str().to_owned(),
                            run_id: run_id.to_owned(),
                            expected: RunPhase::UserStarted,
                            next: RunPhase::UserCommitted,
                        },
                    ],
                },
            ]);
            let injected = InjectedCommand::new(2, command_id.clone());
            let batch = EventBatch {
                writes,
                injected_commands: vec![injected.clone()],
            };
            validate_batch_shape(store.redactor(), &batch).expect("valid steer injection shape");
            let (prepared, _, _) = writer
                .prepare_batch(batch, 1)
                .await
                .expect("prepare steer injection");
            let predicted = EventBatchSizer::injection_batch(
                store.redactor(),
                InjectionBatchSizeInput {
                    application,
                    run_id,
                    turn_id,
                    previous_owner_command_id: Some(&previous_owner),
                    commands: &commands,
                },
            )
            .expect("size complete steer injection");
            let actual = prepared_injection_bytes(
                &prepared,
                &[injected],
                &InjectionSizing {
                    size: predicted,
                    application,
                    run_id: run_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    previous_owner_command_id: Some(previous_owner.clone()),
                },
            )
            .expect("measure real prepared steer write-set");
            assert_eq!(
                predicted.transaction_bytes, actual,
                "{application:?} sizing drift"
            );
        }
    }

    #[tokio::test]
    async fn real_prepared_rows_enforce_exact_32_mib_boundary_without_size_padding() {
        let store = test_store().await;
        let writer = EventWriter::new(store);
        let command_id = "00000000-0000-4000-8000-000000000028";
        let make_batch = |event_run_id: String, projection_run_id: String| EventBatch {
            writes: vec![EventWrite {
                event: Some(
                    DurableEvent::new(&json!({
                        "type":"agent_start",
                        "run_id":event_run_id,
                    }))
                    .expect("typed real event"),
                ),
                projections: vec![Projection::CommandSuperseded {
                    command_id: command_id.to_owned(),
                    command_seq: 1,
                    run_id: Some(projection_run_id),
                }],
            }],
            injected_commands: Vec::new(),
        };
        let projection_run_id = String::new();
        let (_, adjusted_base, _) = writer
            .prepare_batch(make_batch(String::new(), projection_run_id.clone()), 1)
            .await
            .expect("measure parity-adjusted real row base");
        let event_run_bytes = EVENT_BATCH_MAX_BYTES - adjusted_base;
        let exact_event_run_id = "x".repeat(event_run_bytes);
        let (_, exact, _) = writer
            .prepare_batch(
                make_batch(exact_event_run_id.clone(), projection_run_id.clone()),
                1,
            )
            .await
            .expect("exact 32MiB real write-set is admitted");
        assert_eq!(exact, EVENT_BATCH_MAX_BYTES);
        let error = match writer
            .prepare_batch(
                make_batch(exact_event_run_id, format!("{projection_run_id}p")),
                1,
            )
            .await
        {
            Ok(_) => panic!("one real serialized byte above 32MiB must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("durable bytes"));
    }

    #[tokio::test]
    async fn event_batch_rolls_back_every_event_and_projection_on_late_cas_failure() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let injection = classified_injection(
            &writer,
            1,
            "00000000-0000-4000-8000-000000000029",
            "message-1",
            "must roll back",
        )
        .await;
        let error = writer
            .apply(EventBatch {
                writes: injection_writes(
                    "00000000-0000-4000-8000-000000000029",
                    "message-1",
                    "must roll back",
                )
                .into_iter()
                .chain([EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({"type":"agent_start","run_id":"missing-run"}))
                            .expect("serialize event"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: "missing-command".to_owned(),
                        run_id: "missing-run".to_owned(),
                        expected: RunPhase::Classified,
                        next: RunPhase::RunStarted,
                    }],
                }])
                .collect(),
                injected_commands: vec![injection],
            })
            .await
            .expect_err("late projection failure must roll back");
        assert!(
            error.to_string().contains("no durable command binding"),
            "{error:#}"
        );
        let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(store.pool())
            .await
            .expect("count events");
        let message_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
            .fetch_one(store.pool())
            .await
            .expect("count messages");
        assert_eq!((event_count, message_count), (0, 0));
    }

    #[tokio::test]
    async fn user_message_end_requires_atomic_user_committed_transition() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let injection = classified_injection(
            &writer,
            1,
            "00000000-0000-4000-8000-000000000001",
            "message-1",
            "hello",
        )
        .await;
        let mut incomplete =
            injection_writes("00000000-0000-4000-8000-000000000001", "message-1", "hello");
        incomplete
            .last_mut()
            .expect("message end write")
            .projections
            .retain(|projection| !matches!(projection, Projection::RunPhase { .. }));
        let error = writer
            .apply(EventBatch {
                writes: incomplete,
                injected_commands: vec![injection.clone()],
            })
            .await
            .expect_err("MessageEnd without user_committed must fail");
        assert!(error.to_string().contains("user_started -> user_committed"));
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT run_phase FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000001'",
            )
            .fetch_one(store.pool())
            .await
            .expect("phase after rollback"),
            "classified"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM messages")
                .fetch_one(store.pool())
                .await
                .expect("message count after rollback"),
            0
        );

        writer
            .apply(EventBatch {
                writes: injection_writes(
                    "00000000-0000-4000-8000-000000000001",
                    "message-1",
                    "hello",
                ),
                injected_commands: vec![injection],
            })
            .await
            .expect("recovery applies the complete user injection once");
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT run_phase FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000001'",
            )
            .fetch_one(store.pool())
            .await
            .expect("committed phase"),
            "user_committed"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM messages WHERE id=?")
                .bind(user_message_id("00000000-0000-4000-8000-000000000001"))
                .fetch_one(store.pool())
                .await
                .expect("committed message count"),
            1
        );
    }

    #[tokio::test]
    async fn injected_user_timestamp_is_exactly_the_durable_receipt_across_restart() {
        let root = std::env::temp_dir().join(format!(
            "sumi-durable-user-timestamp-{}",
            uuid::Uuid::now_v7()
        ));
        let path = root.join("agent.db");
        let store: Arc<Store> = Store::open(&path, scope(), test_provider())
            .await
            .expect("open store")
            .into();
        let writer = EventWriter::new(store.clone());
        writer
            .persist_inbound(&user_command(
                1,
                "00000000-0000-4000-8000-000000000001",
                "timestamped",
            ))
            .await
            .expect("persist command");
        let received_at: String = sqlx::query_scalar(
            "SELECT received_at FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000001'",
        )
        .fetch_one(store.pool())
        .await
        .expect("durable received_at");
        let durable_timestamp = DateTime::parse_from_rfc3339(&received_at)
            .expect("parse durable timestamp")
            .with_timezone(&Utc);
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        application_kind: ApplicationKind::IdleRun,
                        run_id: "run-00000000-0000-4000-8000-000000000001".to_owned(),
                        turn_id: "turn-00000000-0000-4000-8000-000000000001".to_owned(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("classify");
        let message_id_before_restart = user_message_id("00000000-0000-4000-8000-000000000001");
        drop(writer);
        store.pool().close().await;
        drop(store);

        let reopened: Arc<Store> = Store::open(&path, scope(), test_provider())
            .await
            .expect("reopen before injection")
            .into();
        let reopened_writer = EventWriter::new(reopened.clone());
        assert_eq!(
            message_id_before_restart,
            user_message_id("00000000-0000-4000-8000-000000000001"),
            "restart must not change the UUIDv5 projection anchor"
        );
        let invented = durable_timestamp + chrono::TimeDelta::seconds(1);
        let mismatch = reopened_writer
            .apply(EventBatch {
                writes: injection_writes_at(
                    "00000000-0000-4000-8000-000000000001",
                    "message-1",
                    "timestamped",
                    invented,
                ),
                injected_commands: vec![InjectedCommand::new(
                    1,
                    "00000000-0000-4000-8000-000000000001",
                )],
            })
            .await
            .expect_err("caller-invented timestamp must fail closed");
        assert!(mismatch.to_string().contains("durable received_at"));
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT run_phase FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000001'",
            )
            .fetch_one(reopened.pool())
            .await
            .expect("phase after timestamp rollback"),
            "classified"
        );

        reopened_writer
            .apply(EventBatch {
                writes: injection_writes_at(
                    "00000000-0000-4000-8000-000000000001",
                    "message-1",
                    "timestamped",
                    durable_timestamp,
                ),
                injected_commands: vec![InjectedCommand::new(
                    1,
                    "00000000-0000-4000-8000-000000000001",
                )],
            })
            .await
            .expect("inject using durable timestamp");
        let payload: String = sqlx::query_scalar("SELECT payload FROM messages WHERE id=?")
            .bind(user_message_id("00000000-0000-4000-8000-000000000001"))
            .fetch_one(reopened.pool())
            .await
            .expect("stored user message");
        let stored: PublicMessage =
            serde_json::from_str(&payload).expect("parse stored projection");
        let PublicMessage::User(stored) = stored else {
            panic!("stored message must be user");
        };
        assert_eq!(stored.timestamp, durable_timestamp);
        reopened.pool().close().await;
        tokio::fs::remove_dir_all(root)
            .await
            .expect("remove timestamp fixture");
    }

    #[tokio::test]
    async fn invalid_durable_received_at_fails_closed_before_injection() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let injection = classified_injection(
            &writer,
            1,
            "00000000-0000-4000-8000-000000000001",
            "message-1",
            "hello",
        )
        .await;
        sqlx::query(
            "UPDATE inbound_commands SET received_at='not-a-timestamp'
             WHERE command_id='00000000-0000-4000-8000-000000000001'",
        )
        .execute(store.pool())
        .await
        .expect("corrupt received_at fixture");
        let error = writer
            .apply(EventBatch {
                writes: injection_writes(
                    "00000000-0000-4000-8000-000000000001",
                    "message-1",
                    "hello",
                ),
                injected_commands: vec![injection],
            })
            .await
            .expect_err("invalid received_at must fail closed");
        assert!(error.to_string().contains("invalid durable received_at"));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(store.pool())
                .await
                .expect("event count"),
            0
        );
    }

    #[tokio::test]
    async fn sizer_drift_against_prepared_injection_write_set_fails_closed() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let _injection = classified_injection(
            &writer,
            1,
            "00000000-0000-4000-8000-000000000001",
            "message-1",
            "hello",
        )
        .await;
        let writes = injection_writes("00000000-0000-4000-8000-000000000001", "message-1", "hello");
        let mut drifted: Value = serde_json::from_slice(
            &writes[2]
                .event
                .as_ref()
                .expect("message start event")
                .raw_json,
        )
        .expect("parse canonical message start");
        drifted
            .as_object_mut()
            .expect("message start object")
            .insert(
                "sizing_drift".to_owned(),
                Value::String("unexpected writer field".to_owned()),
            );
        let error = DurableEvent::new(&drifted)
            .expect_err("extra event fields must fail at the closed typed boundary");
        assert!(format!("{error:#}").contains("closed T12 schema"));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(store.pool())
                .await
                .expect("event count after drift"),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT run_phase FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000001'",
            )
            .fetch_one(store.pool())
            .await
            .expect("phase after drift"),
            "classified"
        );
    }

    #[tokio::test]
    async fn failpoint_mid_batch_rolls_back_before_store_restart() {
        let root = std::env::temp_dir().join(format!(
            "sumi-event-writer-failpoint-{}",
            uuid::Uuid::now_v7()
        ));
        let path = root.join("agent.db");
        let store: Arc<Store> = Store::open(&path, scope(), test_provider())
            .await
            .expect("open fresh file-backed store")
            .into();
        let writer = EventWriter::new(store.clone());
        let first = classified_injection(
            &writer,
            1,
            "00000000-0000-4000-8000-000000000001",
            "message-1",
            "first",
        )
        .await;
        let batch = EventBatch {
            writes: injection_writes("00000000-0000-4000-8000-000000000001", "message-1", "first"),
            injected_commands: vec![first],
        };

        let error = writer
            .apply_with_failpoint(batch.clone(), 1)
            .await
            .expect_err("failpoint must interrupt the transaction");
        assert!(error.to_string().contains("test failpoint"));
        drop(writer);
        drop(store);

        let reopened: Arc<Store> = Store::open(&path, scope(), test_provider())
            .await
            .expect("restart store after interrupted EventBatch")
            .into();
        let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(reopened.pool())
            .await
            .expect("count events after restart");
        let messages: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
            .fetch_one(reopened.pool())
            .await
            .expect("count messages after restart");
        assert_eq!((events, messages), (0, 0));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM event_log_heads")
                .fetch_one(reopened.pool())
                .await
                .expect("count event heads after rollback"),
            0
        );

        EventWriter::new(reopened.clone())
            .apply(batch)
            .await
            .expect("same EventBatch succeeds once after restart");
        let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(reopened.pool())
            .await
            .expect("count committed events");
        let messages: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
            .fetch_one(reopened.pool())
            .await
            .expect("count committed messages");
        assert_eq!((events, messages), (4, 1));
        assert_eq!(
            sqlx::query_as::<_, (i64, i64)>("SELECT last_seq,event_count FROM event_log_heads")
                .fetch_one(reopened.pool())
                .await
                .expect("committed event-log head"),
            (4, 4)
        );
        reopened.pool().close().await;
        tokio::fs::remove_dir_all(root)
            .await
            .expect("remove failpoint fixture");
    }

    #[tokio::test]
    async fn malformed_event_fails_before_transaction_and_persists_nothing() {
        let store = test_store().await;
        let error = DurableEvent::from_raw(br#"{"type":"message_end""#.to_vec())
            .expect_err("invalid raw event must fail at the typed constructor");
        assert!(
            format!("{error:#}").contains("closed T12 schema"),
            "{error:#}"
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(store.pool())
            .await
            .expect("count events");
        assert_eq!(count, 0);
    }

    #[test]
    fn durable_event_schema_is_closed_and_canonical_round_trips() {
        let malformed = [
            json!({"type":"message_start","message_id":"message-1"}),
            json!({"type":"message_start","message_id":7,"message":user_message("hello")}),
            json!({
                "type":"tool_execution_start",
                "tool_call_id":"tool-1",
                "tool_name":"test",
                "args":[],
                "state":"running"
            }),
            json!({"type":"agent_start","run_id":"run-1","dangerous_extra":true}),
            json!({"type":"future_durable_variant","payload":{}}),
            json!({"type":"error","message":"must remain volatile"}),
            json!({"type":"message_update","message_id":"message-1","delta":"volatile"}),
            json!({"type":"memory_maintenance","kind":"T17 extension"}),
        ];
        for value in malformed {
            let error = DurableEvent::new(&value)
                .expect_err("unknown, malformed, volatile, and future events must fail closed");
            assert!(format!("{error:#}").contains("closed T12 schema"));
        }

        let tool_result = tool_result("tool-1", "done", false);
        let tool_result_message = match &tool_result {
            PublicMessage::ToolResult(message) => message.clone(),
            _ => unreachable!("tool_result helper returns a tool result"),
        };
        let canonical = [
            json!({"type":"agent_start"}),
            json!({"type":"agent_end"}),
            json!({"type":"turn_start"}),
            json!({
                "type":"turn_end",
                "message":user_message("hello"),
                "tool_results":[tool_result_message]
            }),
            json!({"type":"turn_end","message":null,"tool_results":[]}),
            json!({
                "type":"message_start",
                "message_id":"message-1",
                "message":user_message("hello")
            }),
            json!({
                "type":"message_end",
                "message_id":"message-1",
                "message":user_message("hello")
            }),
            json!({
                "type":"tool_execution_start",
                "tool_call_id":"tool-1",
                "tool_name":"test",
                "args":{"path":"/workspace/report.txt"}
            }),
            json!({
                "type":"tool_execution_end",
                "tool_call_id":"tool-1",
                "result":{"stdout":"done"},
                "is_error":false
            }),
            json!({
                "type":"approval_requested",
                "request":{
                    "id":"request-1",
                    "tool_call_id":"tool-1",
                    "tool_name":"write_file",
                    "action":{"reviewable":{"operation":"write"}},
                    "args_summary":{"path":"/workspace/report.txt"},
                    "reason":"update report",
                    "audit":null
                }
            }),
            json!({
                "type":"approval_resolved",
                "request_id":"request-1",
                "resolution":{"decision":{"type":"deny"}}
            }),
            json!({"type":"steered","mode":"hard"}),
            json!({"type":"steered","mode":"soft"}),
            json!({
                "type":"retry_scheduled",
                "attempt":1,
                "delay_ms":100,
                "retry_at":"2026-07-20T00:00:00Z",
                "error_message":"retry"
            }),
        ];
        for value in canonical {
            let event = DurableEvent::new(&value)
                .unwrap_or_else(|error| panic!("canonical event {value} failed: {error:#}"));
            assert_eq!(
                serde_json::from_slice::<Value>(&event.raw_json).expect("canonical raw JSON"),
                value,
                "durable raw event must contain only canonical public AgentEvent fields"
            );
            let recovered = DurableEvent::from_raw(event.raw_json.clone())
                .expect("canonical event survives encrypted recovery decode");
            assert_eq!(recovered.raw_json, event.raw_json);
        }
    }

    #[test]
    fn empty_turn_end_requires_the_idle_startup_abort_closure_shape() {
        let event = DurableEvent::new(&json!({
            "type":"turn_end",
            "run_id":"run-empty",
            "turn_id":"turn-empty"
        }))
        .expect("typed empty TurnEnd fixture");
        let error = validate_batch_shape(
            &Redactor::v1(),
            &EventBatch {
                writes: vec![EventWrite {
                    event: Some(event),
                    projections: Vec::new(),
                }],
                injected_commands: Vec::new(),
            },
        )
        .err()
        .expect("empty TurnEnd outside idle-startup Abort must fail");
        assert!(
            error
                .to_string()
                .contains("same-batch idle-startup supersede and AgentEnd"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn plaintext_secret_never_appears_in_event_or_message_projections() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let secret = "sk-abcdefghijklmnop";
        let injection = classified_injection(
            &writer,
            1,
            "00000000-0000-4000-8000-000000000030",
            "message-secret",
            secret,
        )
        .await;
        writer
            .apply(EventBatch {
                writes: injection_writes(
                    "00000000-0000-4000-8000-000000000030",
                    "message-secret",
                    secret,
                ),
                injected_commands: vec![injection],
            })
            .await
            .expect("commit secret fixture");

        let row = sqlx::query(
            "SELECT e.envelope, m.payload, m.search_text
             FROM agent_events e JOIN messages m ON m.seq = e.seq",
        )
        .fetch_one(store.pool())
        .await
        .expect("read projections");
        for column in ["envelope", "payload", "search_text"] {
            let value: String = row.get(column);
            assert!(!value.contains(secret), "{column} leaked plaintext secret");
        }
    }

    #[tokio::test]
    async fn secret_bearing_json_keys_are_absent_from_db_projections_and_dump() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let secrets = [
            "sk-abcdefghijklmnop",
            "supersecretvalue",
            "abcdefghijklmnop",
            "abcdef1234567890",
        ];
        let message = PublicMessage::ToolResult(ToolResultMessage {
            tool_call_id: "tool-key-redaction".to_owned(),
            tool_name: "test".to_owned(),
            content: vec![UserContent::Text {
                text: "safe".to_owned(),
            }],
            details: json!({
                "args sk-abcdefghijklmnop":{
                    "details api_key=supersecretvalue":{
                        "message Bearer abcdefghijklmnop":{
                            "event X-Amz-Signature=abcdef1234567890":"safe"
                        }
                    }
                }
            }),
            is_error: false,
            timestamp: durable_test_timestamp(),
        });
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({
                            "type":"message_end",
                            "message_id":"message-key-redaction",
                            "message":message.clone()
                        }))
                        .expect("MessageEnd"),
                    ),
                    projections: vec![Projection::MessageEnd {
                        message_id: "message-key-redaction".to_owned(),
                        role: "tool_result",
                        message,
                        append_to_l0: true,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist key-redaction fixture");

        let dump: String = sqlx::query_scalar(
            "SELECT e.envelope || char(10) || m.payload || char(10) || m.search_text
             FROM agent_events e JOIN messages m ON m.seq=e.seq
             WHERE m.id='message-key-redaction'",
        )
        .fetch_one(store.pool())
        .await
        .expect("projection dump");
        for secret in secrets {
            assert!(!dump.contains(secret), "projection dump leaked {secret}");
        }
    }

    #[tokio::test]
    async fn redacted_json_key_collision_rolls_back_event_and_message() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let message = PublicMessage::ToolResult(ToolResultMessage {
            tool_call_id: "tool-collision".to_owned(),
            tool_name: "test".to_owned(),
            content: Vec::new(),
            details: json!({
                "sk-abcdefghijklmnop":1,
                "sk-ponmlkjihgfedcba":2
            }),
            is_error: false,
            timestamp: durable_test_timestamp(),
        });
        let error = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message("message_end", "message-collision", &message)
                            .expect("MessageEnd"),
                    ),
                    projections: vec![Projection::MessageEnd {
                        message_id: "message-collision".to_owned(),
                        role: "tool_result",
                        message,
                        append_to_l0: true,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("redacted key collision must fail closed");
        assert!(format!("{error:#}").contains("keys collide"));
        let rows: i64 = sqlx::query_scalar(
            "SELECT (SELECT COUNT(*) FROM agent_events) + (SELECT COUNT(*) FROM messages)",
        )
        .fetch_one(store.pool())
        .await
        .expect("row count");
        assert_eq!(rows, 0);
    }

    #[tokio::test]
    async fn command_receipt_replay_requires_sequence_digest_and_decrypted_payload() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let command = user_command(1, "00000000-0000-4000-8000-000000000001", "original");
        let received = writer
            .persist_inbound(&command)
            .await
            .expect("persist command");
        assert_eq!(received.status, CommandAckStatus::Received);
        assert_eq!(
            writer
                .persist_inbound(&command)
                .await
                .expect("replay exact command"),
            received
        );

        let wrong_seq = user_command(2, "00000000-0000-4000-8000-000000000001", "original");
        assert!(
            writer
                .persist_inbound(&wrong_seq)
                .await
                .unwrap_err()
                .to_string()
                .contains("sequence mismatch")
        );
        let replacement = user_command(1, "00000000-0000-4000-8000-000000000001", "replacement");
        assert!(
            writer
                .persist_inbound(&replacement)
                .await
                .unwrap_err()
                .to_string()
                .contains("digest mismatch")
        );

        let key_ref: String = sqlx::query_scalar(
            "SELECT payload_key_ref FROM inbound_commands WHERE command_id = '00000000-0000-4000-8000-000000000001'",
        )
        .fetch_one(store.pool())
        .await
        .expect("read key ref");
        let key = store
            .data_key_by_ref(&key_ref)
            .await
            .expect("unwrap command key");
        let aad = scope().row_aad("inbound_commands", "1", DataKeyPurpose::Command);
        let tampered = encrypt_content(&key, br#"{"type":"abort"}"#, &aad)
            .expect("encrypt authenticated replacement");
        sqlx::query(
            "UPDATE inbound_commands SET payload_ciphertext = ? WHERE command_id = '00000000-0000-4000-8000-000000000001'",
        )
        .bind(tampered)
        .execute(store.pool())
        .await
        .expect("install ciphertext fixture");
        assert!(
            writer
                .persist_inbound(&command)
                .await
                .unwrap_err()
                .to_string()
                .contains("decrypted payload mismatch")
        );
    }

    #[tokio::test]
    async fn command_sequence_outside_sqlite_integer_range_is_rejected_without_a_row() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let error = writer
            .persist_inbound(&abort_command(
                u64::MAX,
                "00000000-0000-4000-8000-000000000031",
            ))
            .await
            .expect_err("SQLite sequence overflow must fail closed");
        assert!(error.to_string().contains("SQLite INTEGER range"));
        let commands: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inbound_commands")
            .fetch_one(store.pool())
            .await
            .expect("count command rows");
        assert_eq!(commands, 0);
    }

    #[tokio::test]
    async fn command_ciphertext_rejects_wrong_sequence_and_conversation_aad() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        writer
            .persist_inbound(&abort_command(1, "00000000-0000-4000-8000-000000000012"))
            .await
            .expect("persist abort");
        let row = sqlx::query(
            "SELECT payload_key_ref, payload_ciphertext FROM inbound_commands WHERE seq = 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("read command row");
        let key = store
            .data_key_by_ref(row.get("payload_key_ref"))
            .await
            .expect("unwrap key");
        let ciphertext: Vec<u8> = row.get("payload_ciphertext");
        let wrong_seq = scope().row_aad("inbound_commands", "2", DataKeyPurpose::Command);
        assert!(decrypt_content(&key, &ciphertext, &wrong_seq).is_err());
        let wrong_conversation = AgentScope {
            conversation_id: "conversation-2".to_owned(),
            ..scope()
        }
        .row_aad("inbound_commands", "1", DataKeyPurpose::Command);
        assert!(decrypt_content(&key, &ciphertext, &wrong_conversation).is_err());
    }

    #[tokio::test]
    async fn oversized_rejection_discards_body_but_reconstructs_terminal_ack() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let bytes = vec![b'x'; 1024 * 1024 + 1];
        let key = store
            .conversation_key(DataKeyPurpose::Command)
            .await
            .expect("command key");
        let inbound = InboundCommand::Invalid {
            seq: 1,
            command_id: CommandId::parse("00000000-0000-4000-8000-000000000010")
                .expect("canonical test command UUID"),
            reason: CommandRejectReason::Oversized {
                actual_bytes: bytes.len() as u64,
            },
            raw_command: RejectedCommandPayload::DiscardedOversized,
            payload_digest: Some(KeyedCommandDigest::new(
                key.key_ref.clone(),
                keyed_digest(&key, &bytes)
                    .try_into()
                    .expect("HMAC-SHA256 digest length"),
            )),
        };
        let ack = writer
            .persist_inbound(&inbound)
            .await
            .expect("persist rejection");
        assert_eq!(ack.status, CommandAckStatus::Rejected);
        assert_eq!(ack.reject_reason.as_deref(), Some("oversized"));

        let row = sqlx::query(
            "SELECT payload_ciphertext, payload_hmac, reject_actual_bytes
             FROM inbound_commands WHERE command_id = '00000000-0000-4000-8000-000000000010'",
        )
        .fetch_one(store.pool())
        .await
        .expect("read rejection");
        assert!(
            row.get::<Option<Vec<u8>>, _>("payload_ciphertext")
                .is_none()
        );
        assert_eq!(row.get::<Vec<u8>, _>("payload_hmac").len(), 32);
        assert_eq!(row.get::<i64, _>("reject_actual_bytes"), bytes.len() as i64);
        assert_eq!(
            writer
                .persist_inbound(&inbound)
                .await
                .expect("reconstruct rejected ack"),
            ack
        );

        let mut changed_bytes = bytes;
        changed_bytes[0] = b'y';
        let changed = InboundCommand::Invalid {
            seq: 1,
            command_id: CommandId::parse("00000000-0000-4000-8000-000000000010")
                .expect("canonical test command UUID"),
            reason: CommandRejectReason::Oversized {
                actual_bytes: changed_bytes.len() as u64,
            },
            raw_command: RejectedCommandPayload::DiscardedOversized,
            payload_digest: Some(KeyedCommandDigest::new(
                key.key_ref.clone(),
                keyed_digest(&key, &changed_bytes)
                    .try_into()
                    .expect("HMAC-SHA256 digest length"),
            )),
        };
        assert!(
            writer
                .persist_inbound(&changed)
                .await
                .expect_err("one-byte oversized payload replacement must fail")
                .to_string()
                .contains("digest mismatch")
        );
    }

    #[tokio::test]
    async fn rejected_null_and_missing_payloads_are_distinct_in_both_replay_directions() {
        for (first, replay) in [
            (
                RejectedCommandPayload::Present(SensitiveCommandPayload::new(b"null".to_vec())),
                RejectedCommandPayload::Missing,
            ),
            (
                RejectedCommandPayload::Missing,
                RejectedCommandPayload::Present(SensitiveCommandPayload::new(b"null".to_vec())),
            ),
        ] {
            let store = test_store().await;
            let writer = EventWriter::new(store);
            let command_id =
                CommandId::parse("00000000-0000-4000-8000-000000000026").expect("canonical UUID");
            let original = InboundCommand::Invalid {
                seq: 1,
                command_id: command_id.clone(),
                reason: CommandRejectReason::SchemaViolation,
                raw_command: first,
                payload_digest: None,
            };
            let ack = writer
                .persist_inbound(&original)
                .await
                .expect("persist rejected command");
            assert_eq!(
                writer
                    .persist_inbound(&original)
                    .await
                    .expect("identical raw replay"),
                ack
            );
            let changed = InboundCommand::Invalid {
                seq: 1,
                command_id,
                reason: CommandRejectReason::SchemaViolation,
                raw_command: replay,
                payload_digest: None,
            };
            assert!(
                writer
                    .persist_inbound(&changed)
                    .await
                    .expect_err("null/missing replacement must fail")
                    .to_string()
                    .contains("digest mismatch")
            );
        }
    }

    #[tokio::test]
    async fn ack_send_failure_after_commit_is_recovered_by_replay() {
        let store = test_store().await;
        let writer = EventWriter::new(store);
        let inbound = abort_command(1, "00000000-0000-4000-8000-000000000012");
        let committed_ack = writer
            .persist_inbound(&inbound)
            .await
            .expect("commit before send");
        let send_result: Result<()> = Err(anyhow!("simulated writer epoch failure"));
        assert!(send_result.is_err());
        assert_eq!(
            writer
                .persist_inbound(&inbound)
                .await
                .expect("API replay after failed send"),
            committed_ack
        );
    }

    #[tokio::test]
    async fn idle_abort_cutoff_terminals_prior_commands_in_sequence_order() {
        let store = test_store().await;
        let writer = EventWriter::new(store);
        let user = user_command(1, "00000000-0000-4000-8000-000000000001", "pending");
        let abort = abort_command(2, "00000000-0000-4000-8000-000000000013");
        writer
            .persist_inbound(&user)
            .await
            .expect("persist pending user");
        writer.persist_inbound(&abort).await.expect("persist abort");

        let acks = writer
            .apply_idle_abort_cutoff("00000000-0000-4000-8000-000000000013", 2)
            .await
            .expect("apply ordered cutoff");
        assert_eq!(
            acks.iter().map(|ack| ack.seq).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(acks[0].status, CommandAckStatus::Superseded);
        assert_eq!(acks[1].status, CommandAckStatus::Applied);
        assert_eq!(
            writer
                .persist_inbound(&user)
                .await
                .expect("replay superseded"),
            acks[0]
        );
        assert_eq!(
            writer
                .persist_inbound(&abort)
                .await
                .expect("replay applied abort"),
            acks[1]
        );
    }

    #[tokio::test]
    async fn classified_idle_startup_abort_is_terminal_across_restart_and_cannot_resume() {
        let root =
            std::env::temp_dir().join(format!("sumi-idle-startup-abort-{}", uuid::Uuid::now_v7()));
        let path = root.join("agent.db");
        let store: Arc<Store> = Store::open(&path, scope(), test_provider())
            .await
            .expect("open store")
            .into();
        let writer = EventWriter::new(store.clone());
        let startup = user_command(1, "00000000-0000-4000-8000-000000000011", "do not start");
        let abort = abort_command(2, "00000000-0000-4000-8000-000000000013");
        writer
            .persist_inbound(&startup)
            .await
            .expect("persist startup");
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: "00000000-0000-4000-8000-000000000011".to_owned(),
                        application_kind: ApplicationKind::IdleRun,
                        run_id: "run-startup".to_owned(),
                        turn_id: "turn-startup".to_owned(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("classify idle startup");
        writer.persist_inbound(&abort).await.expect("persist Abort");
        let acks = writer
            .apply_idle_abort_cutoff("00000000-0000-4000-8000-000000000013", 2)
            .await
            .expect("abort classified startup");
        assert_eq!(
            acks.iter()
                .map(|ack| (ack.command_id.as_str(), ack.status))
                .collect::<Vec<_>>(),
            vec![
                (
                    "00000000-0000-4000-8000-000000000011",
                    CommandAckStatus::Superseded
                ),
                (
                    "00000000-0000-4000-8000-000000000013",
                    CommandAckStatus::Applied
                ),
            ]
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(store.pool())
                .await
                .expect("no unstarted close events"),
            0
        );
        drop(writer);
        store.pool().close().await;
        drop(store);

        let reopened: Arc<Store> = Store::open(&path, scope(), test_provider())
            .await
            .expect("reopen store")
            .into();
        let reopened_writer = EventWriter::new(reopened.clone());
        assert!(
            SuffixRecovery::plan(&reopened)
                .await
                .expect("recovery")
                .is_empty()
        );
        assert_eq!(
            reopened_writer
                .persist_inbound(&startup)
                .await
                .expect("startup ACK replay")
                .status,
            CommandAckStatus::Superseded
        );
        assert_eq!(
            reopened_writer
                .persist_inbound(&abort)
                .await
                .expect("Abort ACK replay")
                .status,
            CommandAckStatus::Applied
        );
        let resume = reopened_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({"type":"agent_start","run_id":"run-startup"}))
                            .expect("AgentStart"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: "00000000-0000-4000-8000-000000000011".to_owned(),
                        run_id: "run-startup".to_owned(),
                        expected: RunPhase::Classified,
                        next: RunPhase::RunStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("superseded startup cannot execute");
        assert!(
            resume.to_string().contains("no durable command binding"),
            "{resume:#}"
        );
        reopened.pool().close().await;
        tokio::fs::remove_dir_all(root)
            .await
            .expect("remove startup Abort fixture");
    }

    #[tokio::test]
    async fn zero_owner_abort_with_run_context_rejects_missing_or_incomplete_startup() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        writer
            .persist_inbound(&abort_command(1, "00000000-0000-4000-8000-000000000012"))
            .await
            .expect("persist Abort");
        let missing = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandApplied {
                        command_id: "00000000-0000-4000-8000-000000000012".to_owned(),
                        command_seq: 1,
                        run_id: Some("invented-run".to_owned()),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("run-bound zero-owner Abort needs a startup target");
        assert!(missing.to_string().contains("pre-user idle startup"));

        let active_store = test_store().await;
        let active_writer = EventWriter::new(active_store.clone());
        active_writer
            .persist_inbound(&user_command(
                1,
                "00000000-0000-4000-8000-000000000015",
                "running",
            ))
            .await
            .expect("persist owner");
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='idle_run', run_id='run-active',
                 turn_id='turn-active', run_phase='assistant_started'
             WHERE command_id='00000000-0000-4000-8000-000000000015'",
        )
        .execute(active_store.pool())
        .await
        .expect("open owner");
        active_writer
            .persist_inbound(&abort_command(2, "00000000-0000-4000-8000-000000000013"))
            .await
            .expect("persist active Abort");
        let false_idle = active_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandApplied {
                        command_id: "00000000-0000-4000-8000-000000000013".to_owned(),
                        command_seq: 2,
                        run_id: None,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("run_id=None cannot bypass an active owner");
        assert!(false_idle.to_string().contains("true Idle"));

        let startup_store = test_store().await;
        let startup_writer = EventWriter::new(startup_store.clone());
        startup_writer
            .persist_inbound(&user_command(
                1,
                "00000000-0000-4000-8000-000000000011",
                "pending",
            ))
            .await
            .expect("persist startup");
        startup_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: "00000000-0000-4000-8000-000000000011".to_owned(),
                        application_kind: ApplicationKind::IdleRun,
                        run_id: "run-startup".to_owned(),
                        turn_id: "turn-startup".to_owned(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("classify startup");
        startup_writer
            .persist_inbound(&abort_command(2, "00000000-0000-4000-8000-000000000013"))
            .await
            .expect("persist Abort");
        let incomplete = startup_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandApplied {
                        command_id: "00000000-0000-4000-8000-000000000013".to_owned(),
                        command_seq: 2,
                        run_id: Some("run-startup".to_owned()),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("startup Abort requires atomic supersede");
        assert!(incomplete.to_string().contains("CommandSuperseded"));
        let states: (String, String) = sqlx::query_as(
            "SELECT
                (SELECT status FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000011'),
                (SELECT status FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000013')",
        )
        .fetch_one(startup_store.pool())
        .await
        .expect("rollback states");
        assert_eq!(states, ("applying".to_owned(), "received".to_owned()));
    }

    #[tokio::test]
    async fn active_abort_commits_control_terminal_and_owner_cas_together() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        writer
            .persist_inbound(&user_command(
                1,
                "00000000-0000-4000-8000-000000000001",
                "running",
            ))
            .await
            .expect("persist owner");
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='idle_run', run_id='run-1',
                 turn_id='turn-1', run_phase='user_started'
             WHERE command_id='00000000-0000-4000-8000-000000000001'",
        )
        .execute(store.pool())
        .await
        .expect("open owner fixture");
        writer
            .persist_inbound(&abort_command(2, "00000000-0000-4000-8000-000000000013"))
            .await
            .expect("persist abort");
        let incomplete = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandApplied {
                        command_id: "00000000-0000-4000-8000-000000000013".to_owned(),
                        command_seq: 2,
                        run_id: Some("run-1".to_owned()),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("active Abort without owner cancellation must roll back");
        assert!(incomplete.to_string().contains("cancel_requested"));
        let unchanged: (String, String) = sqlx::query_as(
            "SELECT
                (SELECT status FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000013'),
                (SELECT run_phase FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000001')",
        )
        .fetch_one(store.pool())
        .await
        .expect("read rollback state");
        assert_eq!(
            unchanged,
            ("received".to_owned(), "user_started".to_owned())
        );
        let reverse_incomplete = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::RunPhase {
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        run_id: "run-1".to_owned(),
                        expected: RunPhase::UserStarted,
                        next: RunPhase::CancelRequested,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("owner cancellation without active Abort must roll back");
        assert!(
            reverse_incomplete
                .to_string()
                .contains("requires active Abort")
        );
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![
                        Projection::CommandApplied {
                            command_id: "00000000-0000-4000-8000-000000000013".to_owned(),
                            command_seq: 2,
                            run_id: Some("run-1".to_owned()),
                        },
                        Projection::RunPhase {
                            command_id: CommandId::parse("00000000-0000-4000-8000-000000000001")
                                .expect("canonical test command UUID")
                                .to_string(),
                            run_id: "run-1".to_owned(),
                            expected: RunPhase::UserStarted,
                            next: RunPhase::CancelRequested,
                        },
                    ],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("commit Abort and owner phase");
        assert_eq!(
            writer
                .ack_for_command("00000000-0000-4000-8000-000000000013")
                .await
                .expect("ack")
                .expect("abort row")
                .status,
            CommandAckStatus::Applied
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT run_phase FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000001'"
            )
            .fetch_one(store.pool())
            .await
            .expect("owner phase"),
            "cancel_requested"
        );
    }

    #[tokio::test]
    async fn active_abort_supersedes_unclassified_null_bound_user_with_run_context() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        writer
            .persist_inbound(&user_command(
                1,
                "00000000-0000-4000-8000-000000000015",
                "running",
            ))
            .await
            .expect("persist owner");
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='idle_run', run_id='run-1',
                 turn_id='turn-1', run_phase='assistant_started'
             WHERE command_id='00000000-0000-4000-8000-000000000015'",
        )
        .execute(store.pool())
        .await
        .expect("open owner fixture");
        let pending = user_command(2, "00000000-0000-4000-8000-000000000016", "return me");
        let abort = abort_command(3, "00000000-0000-4000-8000-000000000014");
        writer
            .persist_inbound(&pending)
            .await
            .expect("persist unclassified user");
        writer.persist_inbound(&abort).await.expect("persist Abort");

        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![
                        Projection::CommandSuperseded {
                            command_id: "00000000-0000-4000-8000-000000000016".to_owned(),
                            command_seq: 2,
                            run_id: Some("run-1".to_owned()),
                        },
                        Projection::RunPhase {
                            command_id: "00000000-0000-4000-8000-000000000015".to_owned(),
                            run_id: "run-1".to_owned(),
                            expected: RunPhase::AssistantStarted,
                            next: RunPhase::CancelRequested,
                        },
                        Projection::CommandApplied {
                            command_id: "00000000-0000-4000-8000-000000000014".to_owned(),
                            command_seq: 3,
                            run_id: Some("run-1".to_owned()),
                        },
                    ],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("commit active Abort cutoff");

        let pending_row = sqlx::query(
            "SELECT status, application_kind, run_id, turn_id, run_phase
             FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000016'",
        )
        .fetch_one(store.pool())
        .await
        .expect("read superseded user");
        assert_eq!(pending_row.get::<String, _>("status"), "superseded");
        assert!(
            pending_row
                .get::<Option<String>, _>("application_kind")
                .is_none()
        );
        assert!(pending_row.get::<Option<String>, _>("run_id").is_none());
        assert!(pending_row.get::<Option<String>, _>("turn_id").is_none());
        assert_eq!(pending_row.get::<String, _>("run_phase"), "received");
        assert_eq!(
            writer
                .persist_inbound(&pending)
                .await
                .expect("replay superseded user")
                .status,
            CommandAckStatus::Superseded
        );
        assert_eq!(
            writer
                .persist_inbound(&abort)
                .await
                .expect("replay applied Abort")
                .status,
            CommandAckStatus::Applied
        );
    }

    #[tokio::test]
    async fn hard_steer_classification_requires_atomic_owner_transition() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        writer
            .persist_inbound(&user_command(
                1,
                "00000000-0000-4000-8000-000000000015",
                "running",
            ))
            .await
            .expect("persist owner");
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='idle_run', run_id='run-1',
                 turn_id='turn-1', run_phase='assistant_started'
             WHERE command_id='00000000-0000-4000-8000-000000000015'",
        )
        .execute(store.pool())
        .await
        .expect("open owner fixture");
        writer
            .persist_inbound(&user_command(
                2,
                "00000000-0000-4000-8000-000000000018",
                "change course",
            ))
            .await
            .expect("persist steer");
        let classification = Projection::CommandClassified {
            command_id: "00000000-0000-4000-8000-000000000018".to_owned(),
            application_kind: ApplicationKind::HardSteer,
            run_id: "run-1".to_owned(),
            turn_id: "turn-2".to_owned(),
        };
        let incomplete = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![classification.clone()],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("incomplete hard steer projection set must fail");
        assert!(incomplete.to_string().contains("hard steer classification"));
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000018'",
            )
            .fetch_one(store.pool())
            .await
            .expect("steer remains received"),
            "received"
        );

        let complete = EventBatch {
            writes: vec![EventWrite {
                event: None,
                projections: vec![
                    classification,
                    Projection::RunPhase {
                        command_id: "00000000-0000-4000-8000-000000000015".to_owned(),
                        run_id: "run-1".to_owned(),
                        expected: RunPhase::AssistantStarted,
                        next: RunPhase::HardSteerRequested,
                    },
                ],
            }],
            injected_commands: Vec::new(),
        };
        writer
            .apply(complete.clone())
            .await
            .expect("commit complete hard steer projection set");
        let phases: (String, String) = sqlx::query_as(
            "SELECT
                (SELECT run_phase FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000015'),
                (SELECT run_phase FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000018')",
        )
        .fetch_one(store.pool())
        .await
        .expect("read hard steer phases");
        assert_eq!(
            phases,
            ("hard_steer_requested".to_owned(), "classified".to_owned())
        );
        writer
            .apply(complete)
            .await
            .expect_err("reapplying a committed set must not duplicate it");
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM inbound_commands
                 WHERE run_id='run-1' AND status='applying'",
            )
            .fetch_one(store.pool())
            .await
            .expect("count stable rows"),
            2
        );
    }

    #[tokio::test]
    async fn phase_transitions_are_cas_valid_and_owner_required_transition_fails_without_owner() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        writer
            .persist_inbound(&user_command(
                1,
                "00000000-0000-4000-8000-000000000001",
                "hello",
            ))
            .await
            .expect("persist command");
        sqlx::query("UPDATE inbound_commands SET received_at=? WHERE command_id='00000000-0000-4000-8000-000000000001'")
            .bind(durable_test_timestamp().to_rfc3339())
            .execute(store.pool())
            .await
            .expect("pin durable receipt timestamp");
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        application_kind: ApplicationKind::IdleRun,
                        run_id: "run-00000000-0000-4000-8000-000000000001".to_owned(),
                        turn_id: "turn-00000000-0000-4000-8000-000000000001".to_owned(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("classify");
        let stale = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({
                            "type":"turn_start",
                            "run_id":"run-00000000-0000-4000-8000-000000000001",
                            "turn_id":"turn-00000000-0000-4000-8000-000000000001",
                        }))
                        .expect("serialize TurnStart"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        run_id: "run-00000000-0000-4000-8000-000000000001".to_owned(),
                        expected: RunPhase::RunStarted,
                        next: RunPhase::TurnStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("stale expected phase must fail CAS");
        assert!(stale.to_string().contains("RunPhase CAS"));

        let ownerless = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::RunPhase {
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        run_id: "run-00000000-0000-4000-8000-000000000001".to_owned(),
                        expected: RunPhase::AssistantStarted,
                        next: RunPhase::HardSteerRequested,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("owner-required phase must reject owner count zero");
        assert!(
            ownerless
                .to_string()
                .contains("requires a hard steer classification")
        );

        writer
            .apply(EventBatch {
                writes: injection_writes(
                    "00000000-0000-4000-8000-000000000001",
                    "message-1",
                    "hello",
                ),
                injected_commands: vec![InjectedCommand::new(
                    1,
                    "00000000-0000-4000-8000-000000000001",
                )],
            })
            .await
            .expect("idle owner opens from an owner-free run");
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT run_phase FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000001'"
            )
            .fetch_one(store.pool())
            .await
            .expect("idle owner phase"),
            "user_committed"
        );

        let ownerless_store = test_store().await;
        let ownerless_writer = EventWriter::new(ownerless_store);
        ownerless_writer
            .persist_inbound(&user_command(
                1,
                "00000000-0000-4000-8000-000000000017",
                "steer",
            ))
            .await
            .expect("persist steer");
        let error = ownerless_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: "00000000-0000-4000-8000-000000000017".to_owned(),
                        application_kind: ApplicationKind::SoftSteer,
                        run_id: "missing-run".to_owned(),
                        turn_id: "turn-2".to_owned(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("steer classification requires an existing run owner");
        assert!(error.to_string().contains("expected 1 owner"));
    }

    #[tokio::test]
    async fn phase_event_pairs_roll_back_before_recovery_can_observe_divergence() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let _ = classified_injection(
            &writer,
            1,
            "00000000-0000-4000-8000-000000000001",
            "message-1",
            "hello",
        )
        .await;

        let missing_agent_start = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::RunPhase {
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        run_id: "run-00000000-0000-4000-8000-000000000001".to_owned(),
                        expected: RunPhase::Classified,
                        next: RunPhase::RunStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("classified -> run_started without AgentStart must fail");
        assert!(missing_agent_start.to_string().contains("agent_start"));
        assert_eq!(
            SuffixRecovery::plan(&store).await.expect("recovery plan"),
            vec![RecoveryStep::EmitAgentStart {
                command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                run_id: "run-00000000-0000-4000-8000-000000000001".to_owned(),
            }]
        );

        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({
                            "type":"agent_start",
                            "run_id":"run-00000000-0000-4000-8000-000000000001",
                        }))
                        .expect("AgentStart"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        run_id: "run-00000000-0000-4000-8000-000000000001".to_owned(),
                        expected: RunPhase::Classified,
                        next: RunPhase::RunStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("canonical AgentStart pair");

        let missing_turn_start = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::RunPhase {
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        run_id: "run-00000000-0000-4000-8000-000000000001".to_owned(),
                        expected: RunPhase::RunStarted,
                        next: RunPhase::TurnStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("run_started -> turn_started without TurnStart must fail");
        assert!(missing_turn_start.to_string().contains("turn_start"));
        assert_eq!(
            SuffixRecovery::plan(&store).await.expect("recovery plan"),
            vec![RecoveryStep::EmitTurnStart {
                command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                run_id: "run-00000000-0000-4000-8000-000000000001".to_owned(),
                turn_id: "turn-00000000-0000-4000-8000-000000000001".to_owned(),
            }]
        );
    }

    #[tokio::test]
    async fn user_owner_closes_only_with_agent_end_or_atomic_handoff() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        writer
            .persist_inbound(&user_command(
                1,
                "00000000-0000-4000-8000-000000000015",
                "running",
            ))
            .await
            .expect("persist owner");
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='idle_run', run_id='run-1',
                 turn_id='turn-1', run_phase='assistant_started'
             WHERE command_id='00000000-0000-4000-8000-000000000015'",
        )
        .execute(store.pool())
        .await
        .expect("open owner");

        let unpaired = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandApplied {
                        command_id: "00000000-0000-4000-8000-000000000015".to_owned(),
                        command_seq: 1,
                        run_id: Some("run-1".to_owned()),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("owner close without AgentEnd/handoff must fail");
        assert!(unpaired.to_string().contains("AgentEnd"));
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT run_phase FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000015'",
            )
            .fetch_one(store.pool())
            .await
            .expect("owner phase"),
            "assistant_started"
        );

        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({"type":"agent_end","run_id":"run-1"}))
                            .expect("AgentEnd"),
                    ),
                    projections: vec![Projection::CommandApplied {
                        command_id: "00000000-0000-4000-8000-000000000015".to_owned(),
                        command_seq: 1,
                        run_id: Some("run-1".to_owned()),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("canonical AgentEnd owner close");
        assert!(
            SuffixRecovery::plan(&store)
                .await
                .expect("recovery")
                .is_empty()
        );

        let handoff_store = test_store().await;
        let handoff_writer = EventWriter::new(handoff_store.clone());
        handoff_writer
            .persist_inbound(&user_command(
                1,
                "00000000-0000-4000-8000-000000000019",
                "old",
            ))
            .await
            .expect("persist old owner");
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='idle_run', run_id='run-handoff',
                 turn_id='turn-old', run_phase='assistant_started'
             WHERE command_id='00000000-0000-4000-8000-000000000019'",
        )
        .execute(handoff_store.pool())
        .await
        .expect("open old owner");
        handoff_writer
            .persist_inbound(&user_command(
                2,
                "00000000-0000-4000-8000-000000000018",
                "new",
            ))
            .await
            .expect("persist steer");
        sqlx::query("UPDATE inbound_commands SET received_at=? WHERE command_id='00000000-0000-4000-8000-000000000018'")
            .bind(durable_test_timestamp().to_rfc3339())
            .execute(handoff_store.pool())
            .await
            .expect("pin steer timestamp");
        handoff_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: "00000000-0000-4000-8000-000000000018".to_owned(),
                        application_kind: ApplicationKind::SoftSteer,
                        run_id: "run-handoff".to_owned(),
                        turn_id: "turn-new".to_owned(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("classify steer");
        let message = user_message("new");
        handoff_writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type":"steered",
                                "command_id":"00000000-0000-4000-8000-000000000018",
                                "run_id":"run-handoff",
                                "turn_id":"turn-new",
                                "mode":"soft",
                            }))
                            .expect("Steered"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: "00000000-0000-4000-8000-000000000018".to_owned(),
                            run_id: "run-handoff".to_owned(),
                            expected: RunPhase::Classified,
                            next: RunPhase::TurnStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type":"turn_start",
                                "run_id":"run-handoff",
                                "turn_id":"turn-new",
                            }))
                            .expect("TurnStart"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message(
                                "message_start",
                                &user_message_id("00000000-0000-4000-8000-000000000018"),
                                &message,
                            )
                            .expect("MessageStart"),
                        ),
                        projections: vec![
                            Projection::CommandApplied {
                                command_id: "00000000-0000-4000-8000-000000000019".to_owned(),
                                command_seq: 1,
                                run_id: Some("run-handoff".to_owned()),
                            },
                            Projection::RunPhase {
                                command_id: "00000000-0000-4000-8000-000000000018".to_owned(),
                                run_id: "run-handoff".to_owned(),
                                expected: RunPhase::TurnStarted,
                                next: RunPhase::UserStarted,
                            },
                        ],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message(
                                "message_end",
                                &user_message_id("00000000-0000-4000-8000-000000000018"),
                                &message,
                            )
                            .expect("MessageEnd"),
                        ),
                        projections: vec![
                            Projection::MessageEnd {
                                message_id: user_message_id("00000000-0000-4000-8000-000000000018"),
                                role: "user",
                                message,
                                append_to_l0: true,
                            },
                            Projection::RunPhase {
                                command_id: "00000000-0000-4000-8000-000000000018".to_owned(),
                                run_id: "run-handoff".to_owned(),
                                expected: RunPhase::UserStarted,
                                next: RunPhase::UserCommitted,
                            },
                        ],
                    },
                ],
                injected_commands: vec![InjectedCommand::new(
                    2,
                    "00000000-0000-4000-8000-000000000018",
                )],
            })
            .await
            .expect("canonical atomic owner handoff");
        let states: (String, String) = sqlx::query_as(
            "SELECT
                (SELECT status FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000019'),
                (SELECT run_phase FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000018')",
        )
        .fetch_one(handoff_store.pool())
        .await
        .expect("handoff states");
        assert_eq!(states, ("applied".to_owned(), "user_committed".to_owned()));
    }

    #[tokio::test]
    async fn partial_unique_index_rejects_two_live_owners() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        writer
            .persist_inbound(&user_command(
                1,
                "00000000-0000-4000-8000-000000000001",
                "one",
            ))
            .await
            .expect("persist first");
        writer
            .persist_inbound(&user_command(
                2,
                "00000000-0000-4000-8000-000000000002",
                "two",
            ))
            .await
            .expect("persist second");
        for command_id in [
            "00000000-0000-4000-8000-000000000001",
            "00000000-0000-4000-8000-000000000002",
        ] {
            sqlx::query(
                "UPDATE inbound_commands
                 SET status='applying', application_kind='idle_run', run_id='run-1',
                     turn_id=?, run_phase='turn_started'
                 WHERE command_id=?",
            )
            .bind(format!("turn-{command_id}"))
            .bind(command_id)
            .execute(store.pool())
            .await
            .expect("prepare owner fixture");
        }
        sqlx::query(
            "UPDATE inbound_commands SET run_phase='user_started' WHERE command_id='00000000-0000-4000-8000-000000000001'",
        )
        .execute(store.pool())
        .await
        .expect("open first owner");
        assert!(
            sqlx::query(
                "UPDATE inbound_commands SET run_phase='user_started' WHERE command_id='00000000-0000-4000-8000-000000000002'",
            )
            .execute(store.pool())
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn transaction_byte_limit_is_revalidated_before_begin() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let error = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::SizePadding(EVENT_BATCH_MAX_BYTES + 1)],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("writer must revalidate actual durable bytes");
        assert!(error.to_string().contains("durable bytes"));
        let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(store.pool())
            .await
            .expect("count events");
        assert_eq!(event_count, 0);
    }

    #[tokio::test]
    async fn multi_write_materialization_stops_at_the_batch_limit() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let per_write = EVENT_BATCH_MAX_BYTES / 2 + 1;
        let error = writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: None,
                        projections: vec![Projection::SizePadding(per_write)],
                    },
                    EventWrite {
                        event: None,
                        projections: vec![Projection::SizePadding(per_write)],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("two individually bounded writes must not exceed the batch");
        assert!(error.to_string().contains("durable bytes"), "{error:#}");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(store.pool())
                .await
                .expect("event count"),
            0
        );
    }

    #[tokio::test]
    async fn oversized_tool_result_is_rejected_during_bounded_preflight() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let result = "x".repeat(EVENT_BATCH_MAX_BYTES / 2);
        let error = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({
                            "type":"tool_execution_end",
                            "tool_call_id":"tool-oversized",
                            "state":"succeeded",
                            "result":{"output":result},
                            "is_error":false
                        }))
                        .expect("typed oversized tool result"),
                    ),
                    projections: Vec::new(),
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("raw plus redacted tool result must exceed one EventBatch");
        assert!(error.to_string().contains("durable bytes"), "{error:#}");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(store.pool())
                .await
                .expect("event count"),
            0
        );
    }

    #[tokio::test]
    async fn key_destroyed_after_prepare_prevents_any_ciphertext_row_from_committing() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let event_key = store
            .conversation_key(DataKeyPurpose::Event)
            .await
            .expect("event key");
        let batch = EventBatch {
            writes: vec![EventWrite {
                event: Some(
                    DurableEvent::new(&json!({"type":"agent_start","run_id":"run-key-race"}))
                        .expect("durable event"),
                ),
                projections: Vec::new(),
            }],
            injected_commands: Vec::new(),
        };
        let error = writer
            .apply_after_prepare_destroy_key(batch, &event_key.key_ref)
            .await
            .expect_err("destroy commit between prepare and begin must fail closed");
        assert!(error.to_string().contains("is not active"));
        let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(store.pool())
            .await
            .expect("count event rows");
        assert_eq!(events, 0);
        let state: String = sqlx::query_scalar("SELECT state FROM data_keys WHERE key_ref=?")
            .bind(&event_key.key_ref)
            .fetch_one(store.pool())
            .await
            .expect("read destroyed key state");
        assert_eq!(state, "destroyed");
    }

    #[tokio::test]
    async fn key_material_replaced_after_prepare_is_rejected_in_transaction() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let event_key = store
            .conversation_key(DataKeyPurpose::Event)
            .await
            .expect("event key");
        let batch = EventBatch {
            writes: vec![EventWrite {
                event: Some(
                    DurableEvent::new(&json!({
                        "type":"agent_start",
                        "run_id":"run-key-material-race"
                    }))
                    .expect("durable event"),
                ),
                projections: Vec::new(),
            }],
            injected_commands: Vec::new(),
        };
        let (prepared, _, _) = writer
            .prepare_batch(batch, 1)
            .await
            .expect("prepare ciphertext with original key material");

        let wrapping_key = WrappingKey::new("test-wrap-v1", [0x53; DATA_KEY_BYTES]);
        let replacement =
            DataKeyMaterial::generate(event_key.key_ref.clone(), DataKeyPurpose::Event)
                .expect("replacement data key");
        let aad = KeyWrapAad {
            key_ref: event_key.key_ref.clone(),
            scope: DataKeyScope::Conversation,
            purpose: DataKeyPurpose::Event,
            conversation_id: Some(scope().conversation_id),
            wrap_key_id: wrapping_key.key_id().to_owned(),
        };
        let (wrap_nonce, wrapped_key) =
            wrap_data_key(&replacement, &wrapping_key, &aad).expect("wrap replacement key");
        sqlx::query(
            "UPDATE data_keys SET wrap_nonce=?, wrapped_key=? WHERE key_ref=? AND state='active'",
        )
        .bind(wrap_nonce.as_slice())
        .bind(wrapped_key)
        .bind(&event_key.key_ref)
        .execute(store.pool())
        .await
        .expect("replace wrapped key material between prepare and transaction");

        let mut transaction = store.pool().begin().await.expect("begin transaction");
        let error = revalidate_prepared_key_refs(&store, &mut transaction, &prepared)
            .await
            .expect_err("changed material must fail closed");
        assert!(error.to_string().contains("changed material"), "{error:#}");
        transaction.rollback().await.expect("roll back validation");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(store.pool())
                .await
                .expect("count event rows"),
            0
        );
    }

    #[tokio::test]
    async fn writer_derives_command_limits_from_durable_inbound_payloads() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let count_error = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::SizePadding(0)],
                }],
                injected_commands: (0_u64..17)
                    .map(|index| {
                        InjectedCommand::new(
                            index + 1,
                            format!("00000000-0000-4000-8000-{index:012}"),
                        )
                    })
                    .collect(),
            })
            .await
            .expect_err("actual command count must be revalidated");
        assert!(count_error.to_string().contains("commands"));

        let oversized_text =
            "x".repeat(super::super::sizer::STEER_GROUP_MAX_BYTES.saturating_add(1));
        let oversized = classified_injection(
            &writer,
            1,
            "00000000-0000-4000-8000-000000000032",
            "message-oversized",
            &oversized_text,
        )
        .await;
        let plaintext_error = writer
            .apply(EventBatch {
                writes: injection_writes(
                    "00000000-0000-4000-8000-000000000032",
                    "message-oversized",
                    &oversized_text,
                ),
                injected_commands: vec![oversized],
            })
            .await
            .expect_err("actual plaintext bytes must be revalidated");
        assert!(plaintext_error.to_string().contains("plaintext"));
        let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(store.pool())
            .await
            .expect("count events");
        assert_eq!(event_count, 0);
    }

    #[tokio::test]
    async fn injected_command_bindings_reject_mismatch_reorder_and_duplicates() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let first = classified_injection(
            &writer,
            1,
            "00000000-0000-4000-8000-000000000001",
            "message-1",
            "first",
        )
        .await;
        let second = classified_injection(
            &writer,
            2,
            "00000000-0000-4000-8000-000000000002",
            "message-2",
            "second",
        )
        .await;
        let writes: Vec<_> =
            injection_writes("00000000-0000-4000-8000-000000000001", "message-1", "first")
                .into_iter()
                .chain(injection_writes(
                    "00000000-0000-4000-8000-000000000002",
                    "message-2",
                    "second",
                ))
                .collect();

        let reorder = writer
            .apply(EventBatch {
                writes: writes.clone(),
                injected_commands: vec![second.clone(), first.clone()],
            })
            .await
            .expect_err("command bindings must preserve durable sequence order");
        assert!(reorder.to_string().contains("strict durable sequence"));

        let duplicate = writer
            .apply(EventBatch {
                writes: writes.clone(),
                injected_commands: vec![first.clone(), first.clone()],
            })
            .await
            .expect_err("duplicate durable command bindings must be rejected");
        assert!(
            duplicate
                .to_string()
                .contains("duplicate injected command_id")
        );

        let mismatch = writer
            .apply(EventBatch {
                writes: injection_writes(
                    "00000000-0000-4000-8000-000000000001",
                    "message-1",
                    "different",
                )
                .into_iter()
                .chain(injection_writes(
                    "00000000-0000-4000-8000-000000000002",
                    "message-2",
                    "second",
                ))
                .collect(),
                injected_commands: vec![first, second],
            })
            .await
            .expect_err("durable payload and MessageEnd text must match");
        assert!(
            mismatch
                .to_string()
                .contains("does not match its user MessageEnd")
        );
        let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(store.pool())
            .await
            .expect("count events");
        assert_eq!(event_count, 0);
    }

    #[tokio::test]
    async fn injected_user_message_uuid_v5_is_canonical_and_replay_stable() {
        let command_id = "018f0000-0000-7000-8000-000000000001";
        let canonical = user_message_id(command_id);
        assert_eq!(canonical, user_message_id(command_id));
        assert_eq!(
            Uuid::parse_str(&canonical)
                .expect("derived message UUID")
                .get_version_num(),
            5
        );
        assert_ne!(
            canonical,
            user_message_id("018f0000-0000-7000-8000-000000000002")
        );
        assert_ne!(
            canonical,
            Uuid::new_v5(&Uuid::NAMESPACE_URL, command_id.as_bytes()).to_string(),
            "the named Sumi collision domain must not alias a standard UUID namespace"
        );

        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let canonical_binding =
            classified_injection(&writer, 1, command_id, "caller-choice", "stable").await;
        assert_eq!(canonical_binding.seq(), 1);
        assert_eq!(canonical_binding.command_id(), command_id);
        assert_eq!(canonical_binding.message_id(), canonical);

        let wrong_binding = InjectedCommand::with_caller_message_id(
            1,
            CommandId::parse(command_id).expect("canonical test command UUID"),
            "caller-choice",
        );
        let rejected = writer
            .apply(EventBatch {
                writes: injection_writes(command_id, "ignored", "stable"),
                injected_commands: vec![wrong_binding],
            })
            .await
            .expect_err("caller-selected user message ID must fail closed");
        assert!(rejected.to_string().contains("canonical UUIDv5"));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(store.pool())
                .await
                .expect("event count after caller-ID rejection"),
            0
        );

        writer
            .apply(EventBatch {
                writes: injection_writes(command_id, "ignored", "stable"),
                injected_commands: vec![InjectedCommand::new(1, command_id)],
            })
            .await
            .expect("canonical UUIDv5 injection");
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT id FROM messages WHERE id=? AND role='user'",)
                .bind(&canonical)
                .fetch_one(store.pool())
                .await
                .expect("canonical message projection"),
            canonical
        );
        assert_eq!(
            user_message_id(command_id),
            InjectedCommand::new(1, command_id).message_id(),
            "reclassification/replay must derive the same ID"
        );
    }

    #[test]
    fn duplicate_projection_variants_are_rejected_before_write() {
        let received = Projection::CommandReceived {
            envelope: CommandEnvelope {
                seq: 1,
                command_id: CommandId::parse("00000000-0000-4000-8000-000000000001")
                    .expect("canonical test command UUID"),
                command: Command::Abort {},
            },
        };
        let duplicate_receipt = validate_batch_shape(
            &Redactor::v1(),
            &EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![received.clone(), received],
                }],
                injected_commands: Vec::new(),
            },
        )
        .err()
        .expect("duplicate receipt projection must fail");
        assert!(
            duplicate_receipt
                .to_string()
                .contains("duplicate command receipt")
        );

        let classified = Projection::CommandClassified {
            command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            application_kind: ApplicationKind::IdleRun,
            run_id: "run-1".to_owned(),
            turn_id: "turn-1".to_owned(),
        };
        let duplicate_classification = validate_batch_shape(
            &Redactor::v1(),
            &EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![classified.clone(), classified],
                }],
                injected_commands: Vec::new(),
            },
        )
        .err()
        .expect("duplicate classification projection must fail");
        assert!(
            duplicate_classification
                .to_string()
                .contains("duplicate CommandClassified")
        );

        let duplicate_terminal = validate_batch_shape(
            &Redactor::v1(),
            &EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![
                        Projection::CommandApplied {
                            command_id: CommandId::parse("00000000-0000-4000-8000-000000000001")
                                .expect("canonical test command UUID")
                                .to_string(),
                            command_seq: 1,
                            run_id: Some("run-1".to_owned()),
                        },
                        Projection::CommandSuperseded {
                            command_id: CommandId::parse("00000000-0000-4000-8000-000000000001")
                                .expect("canonical test command UUID")
                                .to_string(),
                            command_seq: 1,
                            run_id: Some("run-1".to_owned()),
                        },
                    ],
                }],
                injected_commands: Vec::new(),
            },
        )
        .err()
        .expect("conflicting command terminal projections must fail");
        assert!(
            duplicate_terminal
                .to_string()
                .contains("duplicate command terminal")
        );

        let duplicate_approval = validate_batch_shape(
            &Redactor::v1(),
            &EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![
                        Projection::Approval(ApprovalMutation::Pending {
                            request_id: "request-1".to_owned(),
                            tool_call_id: "tool-1".to_owned(),
                            run_id: "run-1".to_owned(),
                            turn_id: "turn-1".to_owned(),
                            request_projection: "safe request".to_owned(),
                            redaction_version: 1,
                        }),
                        Projection::Approval(ApprovalMutation::Resolve {
                            request_id: "request-1".to_owned(),
                            state: "cancelled",
                            actor: "recovery".to_owned(),
                        }),
                    ],
                }],
                injected_commands: Vec::new(),
            },
        )
        .err()
        .expect("duplicate approval mutations must fail");
        assert!(
            duplicate_approval
                .to_string()
                .contains("duplicate approval mutation")
        );
    }

    #[test]
    fn public_events_and_durable_projections_require_exact_pairs() {
        let cases = [
            (
                EventBatch {
                    writes: vec![EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type":"message_end",
                                "message_id":"message-1",
                                "message":user_message("hello"),
                            }))
                            .expect("message end"),
                        ),
                        projections: Vec::new(),
                    }],
                    injected_commands: Vec::new(),
                },
                "message_end event for message-1 has no matching MessageEnd projection",
            ),
            (
                EventBatch {
                    writes: vec![EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type":"approval_requested",
                                "request":approval_request("request-1", "tool-1", "mutating"),
                            }))
                            .expect("approval request"),
                        ),
                        projections: Vec::new(),
                    }],
                    injected_commands: Vec::new(),
                },
                "approval_requested event for request-1 has no matching Approval Pending mutation",
            ),
            (
                EventBatch {
                    writes: vec![EventWrite {
                        event: None,
                        projections: vec![Projection::Approval(ApprovalMutation::Pending {
                            request_id: "request-1".to_owned(),
                            tool_call_id: "tool-1".to_owned(),
                            run_id: "run-1".to_owned(),
                            turn_id: "turn-1".to_owned(),
                            request_projection: approval_request_projection(
                                "request-1",
                                "tool-1",
                                "mutating",
                            ),
                            redaction_version: 1,
                        })],
                    }],
                    injected_commands: Vec::new(),
                },
                "Approval Pending mutation for request-1 has no matching approval_requested event",
            ),
            (
                EventBatch {
                    writes: vec![EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type":"tool_execution_start",
                                "tool_call_id":"tool-1",
                                "tool_name":"test",
                                "args":{},
                                "state":"running"
                            }))
                            .expect("tool start"),
                        ),
                        projections: Vec::new(),
                    }],
                    injected_commands: Vec::new(),
                },
                "tool_execution_start event for tool-1 has no matching ToolExecution Start mutation",
            ),
            (
                EventBatch {
                    writes: vec![EventWrite {
                        event: None,
                        projections: vec![Projection::ToolExecution(
                            ToolExecutionMutation::Start {
                                tool_call_id: "tool-1".to_owned(),
                            },
                        )],
                    }],
                    injected_commands: Vec::new(),
                },
                "ToolExecution Start mutation for tool-1 has no matching tool_execution_start event",
            ),
        ];

        for (batch, expected) in cases {
            let error = validate_batch_shape(&Redactor::v1(), &batch)
                .err()
                .expect("unpaired event/projection must fail");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[tokio::test]
    async fn approval_and_tool_transitions_share_event_writer_transactions() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({
                            "type":"approval_requested",
                            "request":approval_request("request-1", "tool-1", "mutating"),
                        }))
                        .expect("approval event"),
                    ),
                    projections: vec![
                        Projection::ToolExecution(ToolExecutionMutation::Prepare {
                            tool_call_id: "tool-1".to_owned(),
                            command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                            run_id: "run-1".to_owned(),
                            executor_generation: 1,
                            idempotency_key: "00000000-0000-4000-8000-000000000001/tool-1"
                                .to_owned(),
                        }),
                        Projection::Approval(ApprovalMutation::Pending {
                            request_id: "request-1".to_owned(),
                            tool_call_id: "tool-1".to_owned(),
                            run_id: "run-1".to_owned(),
                            turn_id: "turn-1".to_owned(),
                            request_projection: approval_request_projection(
                                "request-1",
                                "tool-1",
                                "mutating",
                            ),
                            redaction_version: store.redactor().version(),
                        }),
                    ],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("commit approval request and pending row");
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT state FROM approval_log WHERE id='request-1'")
                .fetch_one(store.pool())
                .await
                .expect("approval state"),
            "pending"
        );
        writer
            .persist_inbound(&approval_command(
                1,
                "00000000-0000-4000-8000-000000000020",
                "request-1",
            ))
            .await
            .expect("persist approval decision");
        let incomplete = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandApplied {
                        command_id: "00000000-0000-4000-8000-000000000020".to_owned(),
                        command_seq: 1,
                        run_id: Some("run-1".to_owned()),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("active approval without resolution must roll back");
        assert!(incomplete.to_string().contains("ApprovalResolved"));
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000020'",
            )
            .fetch_one(store.pool())
            .await
            .expect("approval command state"),
            "received"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT state FROM approval_log WHERE id='request-1'")
                .fetch_one(store.pool())
                .await
                .expect("approval remains pending"),
            "pending"
        );

        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type":"approval_resolved",
                                "request_id":"request-1",
                                "resolution":"approved_once",
                                "actor":"user-1"
                            }))
                            .expect("approval resolution"),
                        ),
                        projections: vec![
                            Projection::Approval(ApprovalMutation::Resolve {
                                request_id: "request-1".to_owned(),
                                state: "approved_once",
                                actor: "user-1".to_owned(),
                            }),
                            Projection::CommandApplied {
                                command_id: "00000000-0000-4000-8000-000000000020".to_owned(),
                                command_seq: 1,
                                run_id: Some("run-1".to_owned()),
                            },
                        ],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type":"tool_execution_start",
                                "tool_call_id":"tool-1",
                                "tool_name":"test",
                                "args":{},
                                "state":"running"
                            }))
                            .expect("tool start"),
                        ),
                        projections: vec![Projection::ToolExecution(
                            ToolExecutionMutation::Start {
                                tool_call_id: "tool-1".to_owned(),
                            },
                        )],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("commit approval resolution and tool start");

        let tool_result = PublicMessage::ToolResult(ToolResultMessage {
            tool_call_id: "tool-1".to_owned(),
            tool_name: "test".to_owned(),
            content: vec![UserContent::Text {
                text: "done".to_owned(),
            }],
            details: json!({"ok":true}),
            is_error: false,
            timestamp: Utc::now(),
        });
        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type":"tool_execution_end",
                                "tool_call_id":"tool-1",
                                "state":"succeeded",
                                "result":tool_result.clone(),
                                "is_error":false,
                                "error_code":null
                            }))
                            .expect("tool end"),
                        ),
                        projections: vec![Projection::ToolExecution(
                            ToolExecutionMutation::Finish {
                                tool_call_id: "tool-1".to_owned(),
                                expected: "running",
                                state: "succeeded",
                                error_code: None,
                            },
                        )],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type":"message_start",
                                "message_id":"tool-result-1",
                                "message":tool_result.clone(),
                            }))
                            .expect("tool result start"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type":"message_end",
                                "message_id":"tool-result-1",
                                "message": tool_result.clone(),
                            }))
                            .expect("tool result event"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: "tool-result-1".to_owned(),
                            role: "tool_result",
                            message: tool_result,
                            append_to_l0: true,
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("tool terminal state and result message commit together");
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM tool_executions WHERE tool_call_id='tool-1'"
            )
            .fetch_one(store.pool())
            .await
            .expect("tool state"),
            "succeeded"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM messages WHERE id='tool-result-1'")
                .fetch_one(store.pool())
                .await
                .expect("tool result"),
            1
        );
    }

    #[tokio::test]
    async fn approval_decision_is_cryptographically_and_semantically_bound() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_pending_approval(&store, &writer, "request-1", "tool-1", "run-1").await;
        writer
            .persist_inbound(&approval_command_with_decision(
                1,
                "00000000-0000-4000-8000-000000000022",
                "request-1",
                ApprovalDecision::Deny,
            ))
            .await
            .expect("persist typed deny");

        let contradictory = writer
            .apply(EventBatch {
                writes: vec![approval_resolution_write(
                    "request-1",
                    "approved_once",
                    "user-1",
                    Some(("00000000-0000-4000-8000-000000000022", 1, "run-1")),
                )],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("deny cannot map to approval");
        assert!(contradictory.to_string().contains("maps to denied"));

        let denied_start = writer
            .apply(EventBatch {
                writes: vec![
                    approval_resolution_write(
                        "request-1",
                        "denied",
                        "user-1",
                        Some(("00000000-0000-4000-8000-000000000022", 1, "run-1")),
                    ),
                    tool_start_write("tool-1"),
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("deny and tool start must roll back together");
        assert!(
            denied_start
                .to_string()
                .contains("cannot co-commit ToolExecutionStart")
        );
        assert_eq!(
            sqlx::query_as::<_, (String, String, String)>(
                "SELECT
                    (SELECT state FROM approval_log WHERE id='request-1'),
                    (SELECT state FROM tool_executions WHERE tool_call_id='tool-1'),
                    (SELECT status FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000022')",
            )
            .fetch_one(store.pool())
            .await
            .expect("rollback state"),
            (
                "pending".to_owned(),
                "prepared".to_owned(),
                "received".to_owned()
            )
        );

        writer
            .apply(EventBatch {
                writes: vec![approval_resolution_write(
                    "request-1",
                    "denied",
                    "user-1",
                    Some(("00000000-0000-4000-8000-000000000022", 1, "run-1")),
                )],
                injected_commands: Vec::new(),
            })
            .await
            .expect("canonical denial");
        let replay = writer
            .persist_inbound(&approval_command_with_decision(
                1,
                "00000000-0000-4000-8000-000000000022",
                "request-1",
                ApprovalDecision::Deny,
            ))
            .await
            .expect("canonical deny replay");
        assert_eq!(replay.status, CommandAckStatus::Applied);

        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_pending_approval(&store, &writer, "request-2", "tool-2", "run-2").await;
        writer
            .persist_inbound(&approval_command(
                1,
                "00000000-0000-4000-8000-000000000033",
                "request-wrong",
            ))
            .await
            .expect("persist wrong-request command");
        let wrong_request = writer
            .apply(EventBatch {
                writes: vec![approval_resolution_write(
                    "request-2",
                    "approved_once",
                    "user-2",
                    Some(("00000000-0000-4000-8000-000000000033", 1, "run-2")),
                )],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("command request must match resolution request");
        assert!(wrong_request.to_string().contains("request-wrong"));

        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_pending_approval(&store, &writer, "request-2b", "tool-2b", "run-2b").await;
        writer
            .persist_inbound(&approval_command(
                1,
                "00000000-0000-4000-8000-000000000034",
                "request-2b",
            ))
            .await
            .expect("persist approve-once command");
        let approved_to_denied = writer
            .apply(EventBatch {
                writes: vec![approval_resolution_write(
                    "request-2b",
                    "denied",
                    "user-2b",
                    Some(("00000000-0000-4000-8000-000000000034", 1, "run-2b")),
                )],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("approve-once cannot map to denial");
        assert!(
            approved_to_denied
                .to_string()
                .contains("maps to approved_once")
        );

        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Prepare {
                        tool_call_id: "wrong-tool".to_owned(),
                        command_id: "00000000-0000-4000-8000-000000000024".to_owned(),
                        run_id: "run-2b".to_owned(),
                        executor_generation: 1,
                        idempotency_key: "wrong-tool-idem".to_owned(),
                    })],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("prepare unrelated tool");
        let wrong_tool = writer
            .apply(EventBatch {
                writes: vec![
                    approval_resolution_write(
                        "request-2b",
                        "approved_once",
                        "user-2b",
                        Some(("00000000-0000-4000-8000-000000000034", 1, "run-2b")),
                    ),
                    tool_start_write("wrong-tool"),
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("approval can start only its pending tool");
        assert!(wrong_tool.to_string().contains("pending tool tool-2b"));

        let approve_always: ApprovalDecision = serde_json::from_value(json!({
            "type":"approve_always",
            "rule":{"tool_name":"test","literal_prefix":["test"]}
        }))
        .expect("closed deferred ApproveAlways decision");
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_pending_approval(&store, &writer, "request-3", "tool-3", "run-3").await;
        writer
            .persist_inbound(&approval_command_with_decision(
                1,
                "00000000-0000-4000-8000-000000000035",
                "request-3",
                approve_always,
            ))
            .await
            .expect("persist authenticated ApproveAlways");
        let unsupported = writer
            .apply(EventBatch {
                writes: vec![approval_resolution_write(
                    "request-3",
                    "approved_always",
                    "user-3",
                    Some(("00000000-0000-4000-8000-000000000035", 1, "run-3")),
                )],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("T12 must not invent a durable policy mutation");
        assert!(unsupported.to_string().contains("T22/T23"));

        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_pending_approval(&store, &writer, "request-4", "tool-4", "run-4").await;
        let cancelled_start = writer
            .apply(EventBatch {
                writes: vec![
                    approval_resolution_write("request-4", "cancelled", "runtime", None),
                    tool_start_write("tool-4"),
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("runtime cancellation cannot start a tool");
        assert!(
            cancelled_start
                .to_string()
                .contains("cannot co-commit ToolExecutionStart")
        );

        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_pending_approval(&store, &writer, "request-5", "tool-5", "run-5").await;
        writer
            .persist_inbound(&approval_command(
                1,
                "00000000-0000-4000-8000-000000000023",
                "request-5",
            ))
            .await
            .expect("persist approval before HMAC tamper");
        sqlx::query(
            "UPDATE inbound_commands SET payload_hmac=zeroblob(32)
             WHERE command_id='00000000-0000-4000-8000-000000000023'",
        )
        .execute(store.pool())
        .await
        .expect("tamper command HMAC fixture");
        let tampered = writer
            .apply(EventBatch {
                writes: vec![approval_resolution_write(
                    "request-5",
                    "approved_once",
                    "user-5",
                    Some(("00000000-0000-4000-8000-000000000023", 1, "run-5")),
                )],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("tampered durable command must not resolve approval");
        assert!(format!("{tampered:#}").contains("HMAC"));
    }

    #[tokio::test]
    async fn contradictory_tool_event_mutation_and_result_semantics_roll_back() {
        let store = test_store().await;
        for (index, state) in ["prepared", "running", "running", "running"]
            .into_iter()
            .enumerate()
        {
            sqlx::query(
                "INSERT INTO tool_executions(
                    tool_call_id, command_id, run_id, executor_generation, state,
                    idempotency_key, started_at, finished_at, error_code
                 ) VALUES(?, ?, 'run-1', 1, ?, ?, ?, NULL, NULL)",
            )
            .bind(format!("tool-{index}"))
            .bind(format!("command-{index}"))
            .bind(state)
            .bind(format!("idem-{index}"))
            .bind((state == "running").then_some("start"))
            .execute(store.pool())
            .await
            .expect("insert tool state fixture");
        }
        let writer = EventWriter::new(store.clone());

        let invalid_start = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({
                            "type":"tool_execution_start",
                            "tool_call_id":"tool-0",
                            "tool_name":"test",
                            "args":{},
                            "state":"prepared"
                        }))
                        .expect("start event"),
                    ),
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Start {
                        tool_call_id: "tool-0".to_owned(),
                    })],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("start event state cannot contradict prepared-to-running mutation");
        assert!(invalid_start.to_string().contains("state=running"));

        let cases = [
            (
                "tool-1",
                "succeeded",
                true,
                Some("internal"),
                "succeeded",
                None,
                tool_result("tool-1", "actual", true),
                tool_result("tool-1", "actual", true),
                "succeeded tool result",
            ),
            (
                "tool-2",
                "failed",
                true,
                Some("internal"),
                "failed",
                Some("executor_failed"),
                tool_result("tool-2", "actual", true),
                tool_result("tool-2", "actual", true),
                "event and mutation disagree",
            ),
            (
                "tool-3",
                "succeeded",
                false,
                None,
                "succeeded",
                None,
                tool_result("tool-3", "forged", false),
                tool_result("tool-3", "actual", false),
                "does not match result message",
            ),
        ];
        for (
            tool_call_id,
            event_state,
            event_is_error,
            event_error_code,
            mutation_state,
            mutation_error_code,
            event_result,
            message,
            expected,
        ) in cases
        {
            let message_id = format!("{tool_call_id}-result");
            let error = writer
                .apply(EventBatch {
                    writes: vec![
                        EventWrite {
                            event: Some(
                                DurableEvent::new(&json!({
                                    "type":"tool_execution_end",
                                    "tool_call_id":tool_call_id,
                                    "state":event_state,
                                    "result":event_result,
                                    "is_error":event_is_error,
                                    "error_code":event_error_code
                                }))
                                .expect("terminal event"),
                            ),
                            projections: vec![Projection::ToolExecution(
                                ToolExecutionMutation::Finish {
                                    tool_call_id: tool_call_id.to_owned(),
                                    expected: "running",
                                    state: mutation_state,
                                    error_code: mutation_error_code,
                                },
                            )],
                        },
                        EventWrite {
                            event: Some(
                                DurableEvent::message("message_start", &message_id, &message)
                                    .expect("result start"),
                            ),
                            projections: Vec::new(),
                        },
                        EventWrite {
                            event: Some(
                                DurableEvent::message("message_end", &message_id, &message)
                                    .expect("result end"),
                            ),
                            projections: vec![Projection::MessageEnd {
                                message_id,
                                role: "tool_result",
                                message,
                                append_to_l0: true,
                            }],
                        },
                    ],
                    injected_commands: Vec::new(),
                })
                .await
                .expect_err("contradictory terminal semantics must fail");
            assert!(
                error.to_string().contains(expected),
                "unexpected error: {error:#}"
            );
        }

        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(store.pool())
                .await
                .expect("event count"),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM messages")
                .fetch_one(store.pool())
                .await
                .expect("message count"),
            0
        );
        let states: Vec<String> =
            sqlx::query_scalar("SELECT state FROM tool_executions ORDER BY tool_call_id")
                .fetch_all(store.pool())
                .await
                .expect("tool states");
        assert_eq!(states, ["prepared", "running", "running", "running"]);
    }

    #[tokio::test]
    async fn contradictory_approval_request_resolution_and_actor_semantics_roll_back() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let pending_cases = [
            (
                approval_request("request-1", "tool-event", "mutating"),
                "request-1",
                "tool-mutation",
                approval_request_projection("request-1", "tool-event", "mutating"),
                "request identity",
            ),
            (
                approval_request("request-3", "tool-3", "exec"),
                "request-3",
                "tool-3",
                approval_request_projection("unrelated", "tool-3", "exec"),
                "projection does not match",
            ),
        ];
        for (request, request_id, tool_call_id, request_projection, expected) in pending_cases {
            let error = writer
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type":"approval_requested",
                                "request":request
                            }))
                            .expect("request event"),
                        ),
                        projections: vec![Projection::Approval(ApprovalMutation::Pending {
                            request_id: request_id.to_owned(),
                            tool_call_id: tool_call_id.to_owned(),
                            run_id: "run-1".to_owned(),
                            turn_id: "turn-1".to_owned(),
                            request_projection,
                            redaction_version: store.redactor().version(),
                        })],
                    }],
                    injected_commands: Vec::new(),
                })
                .await
                .expect_err("contradictory approval request must fail");
            assert!(
                error.to_string().contains(expected),
                "unexpected error: {error:#}"
            );
        }

        let resolve_cases = [
            ("approved_once", "user-1", "denied", "user-1"),
            (
                "approved_once",
                "user-event",
                "approved_once",
                "user-mutation",
            ),
        ];
        for (event_resolution, event_actor, mutation_state, mutation_actor) in resolve_cases {
            let error = writer
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type":"approval_resolved",
                                "request_id":"request-resolve",
                                "resolution":event_resolution,
                                "actor":event_actor
                            }))
                            .expect("resolution event"),
                        ),
                        projections: vec![Projection::Approval(ApprovalMutation::Resolve {
                            request_id: "request-resolve".to_owned(),
                            state: mutation_state,
                            actor: mutation_actor.to_owned(),
                        })],
                    }],
                    injected_commands: Vec::new(),
                })
                .await
                .expect_err("contradictory approval resolution must fail");
            assert!(error.to_string().contains("event and mutation disagree"));
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(store.pool())
                .await
                .expect("event count"),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM approval_log")
                .fetch_one(store.pool())
                .await
                .expect("approval count"),
            0
        );
    }

    #[tokio::test]
    async fn terminal_tool_mutation_without_result_message_is_rejected_before_write() {
        let store = test_store().await;
        sqlx::query(
            "INSERT INTO tool_executions(
                tool_call_id, command_id, run_id, executor_generation, state,
                idempotency_key, started_at, finished_at, error_code
             ) VALUES('tool-1','00000000-0000-4000-8000-000000000001','run-1',1,'running','idem','start',NULL,NULL)",
        )
        .execute(store.pool())
        .await
        .expect("prepare running tool fixture");
        let writer = EventWriter::new(store.clone());
        let error = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({
                            "type":"tool_execution_end",
                            "tool_call_id":"tool-1",
                            "state":"succeeded",
                            "result":{
                                "role":"tool_result",
                                "tool_call_id":"tool-1",
                                "tool_name":"test",
                                "content":[],
                                "details":{},
                                "is_error":false,
                                "timestamp":"2026-07-20T00:00:00Z"
                            },
                            "is_error":false,
                            "error_code":null
                        }))
                        .expect("tool end"),
                    ),
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Finish {
                        tool_call_id: "tool-1".to_owned(),
                        expected: "running",
                        state: "succeeded",
                        error_code: None,
                    })],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("terminal tool state cannot commit without result MessageEnd");
        assert!(
            error
                .to_string()
                .contains("requires its tool-result MessageEnd")
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM tool_executions WHERE tool_call_id='tool-1'"
            )
            .fetch_one(store.pool())
            .await
            .expect("tool state remains running"),
            "running"
        );
    }

    #[tokio::test]
    async fn append_to_l0_contract_rejects_contradictions_and_preserves_canonical_values() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let invalid = [
            (
                "error-invalid",
                assistant_message(StopReason::Error),
                true,
                "stop_reason=error",
            ),
            (
                "normal-invalid",
                assistant_message(StopReason::Stop),
                false,
                "non-error MessageEnd",
            ),
            (
                "tool-invalid",
                tool_result("tool-invalid", "done", false),
                false,
                "non-error MessageEnd",
            ),
        ];
        for (message_id, message, append_to_l0, expected) in invalid {
            let role = match &message {
                PublicMessage::Assistant(_) => "assistant",
                PublicMessage::ToolResult(_) => "tool_result",
                PublicMessage::User(_) => unreachable!(),
            };
            let error = writer
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: Some(
                            DurableEvent::message("message_end", message_id, &message)
                                .expect("MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: message_id.to_owned(),
                            role,
                            message,
                            append_to_l0,
                        }],
                    }],
                    injected_commands: Vec::new(),
                })
                .await
                .expect_err("contradictory append_to_l0 must fail before transaction");
            assert!(
                error.to_string().contains(expected),
                "unexpected error: {error:#}"
            );
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(store.pool())
                .await
                .expect("event count"),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM messages")
                .fetch_one(store.pool())
                .await
                .expect("message count"),
            0
        );

        assert_eq!(
            l0_disposition(&user_message("user"), true).expect("normal user is appended"),
            L0Disposition::Append
        );
        let canonical = [
            (
                "error-valid",
                "assistant",
                assistant_message(StopReason::Error),
                false,
            ),
            (
                "normal-valid",
                "assistant",
                assistant_message(StopReason::Stop),
                true,
            ),
            (
                "tool-valid",
                "tool_result",
                tool_result("tool-valid", "done", false),
                true,
            ),
        ];
        for (message_id, role, message, append_to_l0) in canonical {
            writer
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: Some(
                            DurableEvent::message("message_end", message_id, &message)
                                .expect("MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: message_id.to_owned(),
                            role,
                            message,
                            append_to_l0,
                        }],
                    }],
                    injected_commands: Vec::new(),
                })
                .await
                .expect("canonical append_to_l0 value");
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM messages")
                .fetch_one(store.pool())
                .await
                .expect("message count"),
            3
        );
    }

    #[tokio::test]
    async fn unredacted_approval_projection_rolls_back_its_event() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let error = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({
                            "type":"approval_requested",
                            "request":{
                                "id":"request-secret",
                                "tool_call_id":"tool-secret",
                                "tool_name":"bash",
                                "action":{"reviewable":{"risk":"exec"}},
                                "args_summary":"Bearer abcdefghijklmnop",
                                "reason":null,
                                "audit":null
                            },
                        }))
                        .expect("event"),
                    ),
                    projections: vec![Projection::Approval(ApprovalMutation::Pending {
                        request_id: "request-secret".to_owned(),
                        tool_call_id: "tool-secret".to_owned(),
                        run_id: "run-1".to_owned(),
                        turn_id: "turn-1".to_owned(),
                        request_projection: json!({
                            "id":"request-secret",
                            "tool_call_id":"tool-secret",
                            "tool_name":"bash",
                            "action":{"reviewable":{"risk":"exec"}},
                            "args_summary":"Bearer abcdefghijklmnop",
                            "reason":null,
                            "audit":null
                        })
                        .to_string(),
                        redaction_version: store.redactor().version(),
                    })],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("unredacted projection must fail before transaction");
        assert!(error.to_string().contains("does not match its event"));
        let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(store.pool())
            .await
            .expect("events");
        let approvals: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM approval_log")
            .fetch_one(store.pool())
            .await
            .expect("approvals");
        assert_eq!((events, approvals), (0, 0));
    }

    #[tokio::test]
    async fn migration_rejects_invalid_command_and_tool_state_fixtures() {
        let store = test_store().await;
        store
            .conversation_key(DataKeyPurpose::Command)
            .await
            .expect("create command key");
        let key_ref: String = sqlx::query_scalar(
            "SELECT key_ref FROM data_keys WHERE purpose='command' AND state='active'",
        )
        .fetch_one(store.pool())
        .await
        .expect("key ref");
        let invalid_commands = [
            (
                "unknown-kind",
                "future",
                "received",
                None,
                None,
                None,
                "received",
                None,
            ),
            (
                "unknown-status",
                "user_message",
                "queued",
                None,
                None,
                None,
                "received",
                None,
            ),
            (
                "unknown-application",
                "user_message",
                "applying",
                Some("future"),
                Some("run"),
                Some("turn"),
                "classified",
                None,
            ),
            (
                "unknown-phase",
                "user_message",
                "applying",
                Some("idle_run"),
                Some("run"),
                Some("turn"),
                "future",
                None,
            ),
            (
                "received-with-applied-at",
                "user_message",
                "received",
                None,
                None,
                None,
                "received",
                Some("done"),
            ),
            (
                "applied-without-applied-at",
                "user_message",
                "applied",
                Some("idle_run"),
                Some("run"),
                Some("turn"),
                "finished",
                None,
            ),
            (
                "applying-without-run-id",
                "user_message",
                "applying",
                Some("idle_run"),
                None,
                Some("turn"),
                "classified",
                None,
            ),
            (
                "control-with-run-binding",
                "abort",
                "received",
                Some("idle_run"),
                Some("run"),
                Some("turn"),
                "received",
                None,
            ),
        ];
        for (
            index,
            (
                command_id,
                command_kind,
                status,
                application_kind,
                run_id,
                turn_id,
                run_phase,
                applied_at,
            ),
        ) in invalid_commands.into_iter().enumerate()
        {
            let result = sqlx::query(
                "INSERT INTO inbound_commands(
                    seq, command_id, command_kind, payload_ciphertext, payload_key_ref,
                    payload_hmac, status, reject_reason, reject_actual_bytes,
                    application_kind, run_id, turn_id, run_phase, received_at, applied_at
                 ) VALUES(?, ?, ?, X'00', ?, X'00', ?, NULL, NULL, ?, ?, ?, ?, 'now', ?)",
            )
            .bind((index + 1) as i64)
            .bind(command_id)
            .bind(command_kind)
            .bind(&key_ref)
            .bind(status)
            .bind(application_kind)
            .bind(run_id)
            .bind(turn_id)
            .bind(run_phase)
            .bind(applied_at)
            .execute(store.pool())
            .await;
            assert!(
                result.is_err(),
                "invalid inbound command fixture {command_id} must be rejected"
            );
        }

        let invalid_tools = [
            ("unknown-state", "future", None, None, None),
            ("prepared-with-start", "prepared", Some("start"), None, None),
            ("running-without-start", "running", None, None, None),
            (
                "succeeded-without-start",
                "succeeded",
                None,
                Some("end"),
                None,
            ),
            (
                "succeeded-with-error",
                "succeeded",
                Some("start"),
                Some("end"),
                Some("internal"),
            ),
            (
                "failed-without-error",
                "failed",
                Some("start"),
                Some("end"),
                None,
            ),
            (
                "invalid-error-code",
                "failed",
                Some("start"),
                Some("end"),
                Some("future"),
            ),
            (
                "cancelled-without-finish",
                "cancelled",
                None,
                None,
                Some("cancelled"),
            ),
        ];
        for (index, (tool_call_id, state, started_at, finished_at, error_code)) in
            invalid_tools.into_iter().enumerate()
        {
            let result = sqlx::query(
                "INSERT INTO tool_executions(
                    tool_call_id, command_id, run_id, executor_generation, state,
                    idempotency_key, started_at, finished_at, error_code
                 ) VALUES(?, '00000000-0000-4000-8000-000000000001', 'run-1', 1, ?, ?, ?, ?, ?)",
            )
            .bind(tool_call_id)
            .bind(state)
            .bind(format!("idem-{index}"))
            .bind(started_at)
            .bind(finished_at)
            .bind(error_code)
            .execute(store.pool())
            .await;
            assert!(
                result.is_err(),
                "invalid tool execution fixture {tool_call_id} must be rejected"
            );
        }
    }

    #[tokio::test]
    async fn migration_rejects_invalid_approval_state_time_combinations() {
        let store = test_store().await;
        let invalid = [
            ("unknown", "future", None),
            ("pending-decided", "pending", Some("done")),
            ("terminal-undecided", "denied", None),
        ];
        for (index, (request_id, state, decided_at)) in invalid.into_iter().enumerate() {
            let result = sqlx::query(
                "INSERT INTO approval_log(
                    id, tool_call_id, run_id, turn_id, state, request_projection,
                    redaction_version, created_at, decided_at
                 ) VALUES(?, ?, 'run', 'turn', ?, '{}', 1, 'now', ?)",
            )
            .bind(request_id)
            .bind(format!("tool-{index}"))
            .bind(state)
            .bind(decided_at)
            .execute(store.pool())
            .await;
            assert!(
                result.is_err(),
                "invalid approval fixture {request_id} must be rejected"
            );
        }
    }

    #[cfg(unix)]
    const HARD_KILL_SCENARIOS: &[&str] = &[
        "command_received",
        "command_rejected",
        "command_classified",
        "user_injection",
        "command_applied",
        "startup_abort",
        "approval_pending",
        "approval_resolved",
        "tool_prepared",
        "tool_running",
        "tool_terminal",
    ];

    #[cfg(unix)]
    async fn setup_hard_kill_scenario(writer: &EventWriter, store: &Arc<Store>, scenario: &str) {
        match scenario {
            "command_received" | "command_rejected" | "tool_prepared" => {}
            "command_classified" => {
                writer
                    .persist_inbound(&user_command(
                        1,
                        "00000000-0000-4000-8000-000000000001",
                        "classify",
                    ))
                    .await
                    .expect("persist classification target");
            }
            "user_injection" => {
                let _ = classified_injection(
                    writer,
                    1,
                    "00000000-0000-4000-8000-000000000001",
                    "message-1",
                    "inject",
                )
                .await;
            }
            "command_applied" => {
                writer
                    .persist_inbound(&abort_command(1, "00000000-0000-4000-8000-000000000012"))
                    .await
                    .expect("persist applied target");
            }
            "startup_abort" => {
                writer
                    .persist_inbound(&user_command(
                        1,
                        "00000000-0000-4000-8000-000000000011",
                        "pending",
                    ))
                    .await
                    .expect("persist startup");
                writer
                    .apply(EventBatch {
                        writes: vec![EventWrite {
                            event: None,
                            projections: vec![Projection::CommandClassified {
                                command_id: "00000000-0000-4000-8000-000000000011".to_owned(),
                                application_kind: ApplicationKind::IdleRun,
                                run_id: "run-startup".to_owned(),
                                turn_id: "turn-startup".to_owned(),
                            }],
                        }],
                        injected_commands: Vec::new(),
                    })
                    .await
                    .expect("classify startup");
                writer
                    .persist_inbound(&abort_command(2, "00000000-0000-4000-8000-000000000013"))
                    .await
                    .expect("persist startup Abort");
            }
            "approval_pending" => {}
            "approval_resolved" => {
                writer
                    .apply(hard_kill_target_batch("approval_pending"))
                    .await
                    .expect("prepare pending approval");
                writer
                    .persist_inbound(&approval_command(
                        1,
                        "00000000-0000-4000-8000-000000000020",
                        "request-1",
                    ))
                    .await
                    .expect("persist approval decision");
            }
            "tool_running" | "tool_terminal" => {
                writer
                    .apply(hard_kill_target_batch("tool_prepared"))
                    .await
                    .expect("prepare tool");
                if scenario == "tool_terminal" {
                    writer
                        .apply(hard_kill_target_batch("tool_running"))
                        .await
                        .expect("start tool");
                }
            }
            value => panic!("unknown hard-kill scenario {value}"),
        }
        // Ensure all setup transactions have released SQLite before the abrupt target.
        sqlx::query("SELECT 1")
            .execute(store.pool())
            .await
            .expect("setup connection remains usable");
    }

    #[cfg(unix)]
    fn hard_kill_target_batch(scenario: &str) -> EventBatch {
        match scenario {
            "command_received" => EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandReceived {
                        envelope: CommandEnvelope {
                            seq: 1,
                            command_id: CommandId::parse("00000000-0000-4000-8000-000000000001")
                                .expect("canonical test command UUID"),
                            command: Command::Abort {},
                        },
                    }],
                }],
                injected_commands: Vec::new(),
            },
            "command_rejected" => EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandRejected {
                        seq: 1,
                        command_id: "00000000-0000-4000-8000-000000000021".to_owned(),
                        reason: CommandRejectReason::SchemaViolation,
                        raw_command: RejectedCommandPayload::Present(
                            crate::gateway::SensitiveCommandPayload::new(
                                br#"{"type":"abort",}"#.to_vec(),
                            ),
                        ),
                        payload_digest: None,
                    }],
                }],
                injected_commands: Vec::new(),
            },
            "command_classified" => EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        application_kind: ApplicationKind::IdleRun,
                        run_id: "run-00000000-0000-4000-8000-000000000001".to_owned(),
                        turn_id: "turn-00000000-0000-4000-8000-000000000001".to_owned(),
                    }],
                }],
                injected_commands: Vec::new(),
            },
            "user_injection" => EventBatch {
                writes: injection_writes(
                    "00000000-0000-4000-8000-000000000001",
                    "message-1",
                    "inject",
                ),
                injected_commands: vec![InjectedCommand::new(
                    1,
                    "00000000-0000-4000-8000-000000000001",
                )],
            },
            "command_applied" => EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandApplied {
                        command_id: "00000000-0000-4000-8000-000000000012".to_owned(),
                        command_seq: 1,
                        run_id: None,
                    }],
                }],
                injected_commands: Vec::new(),
            },
            "startup_abort" => EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![
                        Projection::CommandSuperseded {
                            command_id: "00000000-0000-4000-8000-000000000011".to_owned(),
                            command_seq: 1,
                            run_id: Some("run-startup".to_owned()),
                        },
                        Projection::CommandApplied {
                            command_id: "00000000-0000-4000-8000-000000000013".to_owned(),
                            command_seq: 2,
                            run_id: Some("run-startup".to_owned()),
                        },
                    ],
                }],
                injected_commands: Vec::new(),
            },
            "approval_pending" => EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({
                            "type":"approval_requested",
                            "request":approval_request("request-1", "tool-1", "mutating"),
                        }))
                        .expect("ApprovalRequested"),
                    ),
                    projections: vec![
                        Projection::ToolExecution(ToolExecutionMutation::Prepare {
                            tool_call_id: "tool-1".to_owned(),
                            command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                            run_id: "run-1".to_owned(),
                            executor_generation: 1,
                            idempotency_key: "00000000-0000-4000-8000-000000000001/tool-1"
                                .to_owned(),
                        }),
                        Projection::Approval(ApprovalMutation::Pending {
                            request_id: "request-1".to_owned(),
                            tool_call_id: "tool-1".to_owned(),
                            run_id: "run-1".to_owned(),
                            turn_id: "turn-1".to_owned(),
                            request_projection: approval_request_projection(
                                "request-1",
                                "tool-1",
                                "mutating",
                            ),
                            redaction_version: 1,
                        }),
                    ],
                }],
                injected_commands: Vec::new(),
            },
            "approval_resolved" => EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({
                            "type":"approval_resolved",
                            "request_id":"request-1",
                            "resolution":"approved_once",
                            "actor":"user-1"
                        }))
                        .expect("ApprovalResolved"),
                    ),
                    projections: vec![
                        Projection::Approval(ApprovalMutation::Resolve {
                            request_id: "request-1".to_owned(),
                            state: "approved_once",
                            actor: "user-1".to_owned(),
                        }),
                        Projection::CommandApplied {
                            command_id: "00000000-0000-4000-8000-000000000020".to_owned(),
                            command_seq: 1,
                            run_id: Some("run-1".to_owned()),
                        },
                    ],
                }],
                injected_commands: Vec::new(),
            },
            "tool_prepared" => EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Prepare {
                        tool_call_id: "tool-1".to_owned(),
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        run_id: "run-1".to_owned(),
                        executor_generation: 1,
                        idempotency_key: "00000000-0000-4000-8000-000000000001/tool-1".to_owned(),
                    })],
                }],
                injected_commands: Vec::new(),
            },
            "tool_running" => EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({
                            "type":"tool_execution_start",
                            "tool_call_id":"tool-1",
                            "tool_name":"test",
                            "args":{},
                            "state":"running"
                        }))
                        .expect("ToolExecutionStart"),
                    ),
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Start {
                        tool_call_id: "tool-1".to_owned(),
                    })],
                }],
                injected_commands: Vec::new(),
            },
            "tool_terminal" => {
                let result = PublicMessage::ToolResult(ToolResultMessage {
                    tool_call_id: "tool-1".to_owned(),
                    tool_name: "test".to_owned(),
                    content: vec![UserContent::Text {
                        text: "done".to_owned(),
                    }],
                    details: json!({"ok":true}),
                    is_error: false,
                    timestamp: durable_test_timestamp(),
                });
                EventBatch {
                    writes: vec![
                        EventWrite {
                            event: Some(
                                DurableEvent::new(&json!({
                                    "type":"tool_execution_end",
                                    "tool_call_id":"tool-1",
                                    "state":"succeeded",
                                    "result":result.clone(),
                                    "is_error":false,
                                    "error_code":null
                                }))
                                .expect("ToolExecutionEnd"),
                            ),
                            projections: vec![Projection::ToolExecution(
                                ToolExecutionMutation::Finish {
                                    tool_call_id: "tool-1".to_owned(),
                                    expected: "running",
                                    state: "succeeded",
                                    error_code: None,
                                },
                            )],
                        },
                        EventWrite {
                            event: Some(
                                DurableEvent::message("message_start", "tool-result-1", &result)
                                    .expect("tool result MessageStart"),
                            ),
                            projections: Vec::new(),
                        },
                        EventWrite {
                            event: Some(
                                DurableEvent::message("message_end", "tool-result-1", &result)
                                    .expect("tool result MessageEnd"),
                            ),
                            projections: vec![Projection::MessageEnd {
                                message_id: "tool-result-1".to_owned(),
                                role: "tool_result",
                                message: result,
                                append_to_l0: true,
                            }],
                        },
                    ],
                    injected_commands: Vec::new(),
                }
            }
            value => panic!("unknown hard-kill scenario {value}"),
        }
    }

    #[cfg(unix)]
    async fn hard_kill_target_complete(store: &Store, scenario: &str) -> bool {
        match scenario {
            "command_received" => {
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM inbound_commands
                     WHERE command_id='00000000-0000-4000-8000-000000000001' AND status='received'",
                )
                .fetch_one(store.pool())
                .await
                .expect("command received state")
                    == 1
            }
            "command_rejected" => {
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM inbound_commands
                     WHERE command_id='00000000-0000-4000-8000-000000000021' AND status='rejected'
                       AND reject_reason='schema_violation' AND payload_ciphertext IS NOT NULL",
                )
                .fetch_one(store.pool())
                .await
                .expect("command rejected state")
                    == 1
            }
            "command_classified" => {
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM inbound_commands
                     WHERE command_id='00000000-0000-4000-8000-000000000001' AND status='applying'
                       AND run_phase='classified'",
                )
                .fetch_one(store.pool())
                .await
                .expect("classification state")
                    == 1
            }
            "user_injection" => {
                let state: (i64, i64, i64) = sqlx::query_as(
                    "SELECT
                        (SELECT COUNT(*) FROM agent_events),
                        (SELECT COUNT(*) FROM messages WHERE id=?),
                        (SELECT COUNT(*) FROM inbound_commands
                         WHERE command_id='00000000-0000-4000-8000-000000000001' AND run_phase='user_committed')",
                )
                .bind(user_message_id("00000000-0000-4000-8000-000000000001"))
                .fetch_one(store.pool())
                .await
                .expect("injection state");
                state == (4, 1, 1)
            }
            "command_applied" => {
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM inbound_commands
                     WHERE command_id='00000000-0000-4000-8000-000000000012' AND status='applied'",
                )
                .fetch_one(store.pool())
                .await
                .expect("command applied state")
                    == 1
            }
            "startup_abort" => {
                let state: (i64, i64, i64) = sqlx::query_as(
                    "SELECT
                        (SELECT COUNT(*) FROM inbound_commands
                         WHERE command_id='00000000-0000-4000-8000-000000000011' AND status='superseded'),
                        (SELECT COUNT(*) FROM inbound_commands
                         WHERE command_id='00000000-0000-4000-8000-000000000013' AND status='applied'),
                        (SELECT COUNT(*) FROM agent_events)",
                )
                .fetch_one(store.pool())
                .await
                .expect("startup Abort state");
                state == (1, 1, 0)
            }
            "approval_pending" => {
                let state: (i64, i64, i64) = sqlx::query_as(
                    "SELECT
                        (SELECT COUNT(*) FROM approval_log
                         WHERE id='request-1' AND state='pending'),
                        (SELECT COUNT(*) FROM tool_executions
                         WHERE tool_call_id='tool-1' AND state='prepared'),
                        (SELECT COUNT(*) FROM agent_events)",
                )
                .fetch_one(store.pool())
                .await
                .expect("approval pending state");
                state == (1, 1, 1)
            }
            "approval_resolved" => {
                let state: (i64, i64, i64) = sqlx::query_as(
                    "SELECT
                        (SELECT COUNT(*) FROM approval_log
                         WHERE id='request-1' AND state='approved_once'),
                        (SELECT COUNT(*) FROM inbound_commands
                         WHERE command_id='00000000-0000-4000-8000-000000000020' AND status='applied'),
                        (SELECT COUNT(*) FROM agent_events)",
                )
                .fetch_one(store.pool())
                .await
                .expect("approval resolved state");
                state == (1, 1, 2)
            }
            "tool_prepared" => {
                sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM tool_executions
                     WHERE tool_call_id='tool-1' AND state='prepared'",
                )
                .fetch_one(store.pool())
                .await
                .expect("tool prepared state")
                    == 1
            }
            "tool_running" => {
                let state: (i64, i64) = sqlx::query_as(
                    "SELECT
                        (SELECT COUNT(*) FROM tool_executions
                         WHERE tool_call_id='tool-1' AND state='running'),
                        (SELECT COUNT(*) FROM agent_events)",
                )
                .fetch_one(store.pool())
                .await
                .expect("tool running state");
                state == (1, 1)
            }
            "tool_terminal" => {
                let state: (i64, i64, i64) = sqlx::query_as(
                    "SELECT
                        (SELECT COUNT(*) FROM tool_executions
                         WHERE tool_call_id='tool-1' AND state='succeeded'),
                        (SELECT COUNT(*) FROM messages WHERE id='tool-result-1'),
                        (SELECT COUNT(*) FROM agent_events)",
                )
                .fetch_one(store.pool())
                .await
                .expect("tool terminal state");
                state == (1, 1, 4)
            }
            value => panic!("unknown hard-kill scenario {value}"),
        }
    }

    #[cfg(unix)]
    async fn replay_hard_kill_target_once(writer: &EventWriter, store: &Store, scenario: &str) {
        if !hard_kill_target_complete(store, scenario).await {
            writer
                .apply(hard_kill_target_batch(scenario))
                .await
                .unwrap_or_else(|error| panic!("replay {scenario} failed: {error:#}"));
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "subprocess entry point for abrupt transaction tests"]
    async fn hard_kill_transaction_child() {
        let scenario =
            std::env::var("SUMI_HARD_KILL_SCENARIO").expect("child scenario environment");
        let boundary =
            std::env::var("SUMI_HARD_KILL_BOUNDARY").expect("child boundary environment");
        let database_path = std::path::PathBuf::from(
            std::env::var("SUMI_HARD_KILL_DATABASE").expect("child database environment"),
        );
        let readiness_path = std::path::PathBuf::from(
            std::env::var("SUMI_HARD_KILL_READY").expect("child readiness environment"),
        );
        let store: Arc<Store> = Store::open(&database_path, scope(), test_provider())
            .await
            .expect("child opens store")
            .into();
        let writer = EventWriter::new(store.clone());
        setup_hard_kill_scenario(&writer, &store, &scenario).await;
        writer
            .apply_with_abrupt_transaction_failpoint(
                hard_kill_target_batch(&scenario),
                &scenario,
                boundary == "after_commit",
                &readiness_path,
            )
            .await
            .expect("abrupt failpoint must not return");
        panic!("abrupt failpoint returned");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn abrupt_subprocess_failpoints_are_atomic_before_and_after_every_t12_boundary() {
        for scenario in HARD_KILL_SCENARIOS {
            for boundary in ["before_commit", "after_commit"] {
                let root = std::env::temp_dir().join(format!(
                    "sumi-hard-kill-{scenario}-{boundary}-{}",
                    uuid::Uuid::now_v7()
                ));
                std::fs::create_dir_all(&root).expect("create hard-kill fixture root");
                let database_path = root.join("agent.db");
                let readiness_path = root.join("ready");
                let output = std::process::Command::new(
                    std::env::current_exe().expect("current unit test executable"),
                )
                .arg("--exact")
                .arg("store::event_writer::tests::hard_kill_transaction_child")
                .arg("--ignored")
                .arg("--nocapture")
                .env("SUMI_HARD_KILL_SCENARIO", scenario)
                .env("SUMI_HARD_KILL_BOUNDARY", boundary)
                .env("SUMI_HARD_KILL_DATABASE", &database_path)
                .env("SUMI_HARD_KILL_READY", &readiness_path)
                .output()
                .expect("run abrupt transaction child");
                assert_eq!(
                    output.status.code(),
                    Some(86),
                    "{scenario}.{boundary} child did not exit at failpoint:\nstdout={}\nstderr={}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                assert_eq!(
                    std::fs::read_to_string(&readiness_path).expect("read readiness marker"),
                    format!("{scenario}.{boundary}\n")
                );

                let reopened: Arc<Store> = Store::open(&database_path, scope(), test_provider())
                    .await
                    .expect("reopen after hard kill")
                    .into();
                let initially_complete = hard_kill_target_complete(&reopened, scenario).await;
                assert_eq!(
                    initially_complete,
                    boundary == "after_commit",
                    "{scenario}.{boundary} was not all-or-none"
                );
                let writer = EventWriter::new(reopened.clone());
                replay_hard_kill_target_once(&writer, &reopened, scenario).await;
                assert!(
                    hard_kill_target_complete(&reopened, scenario).await,
                    "{scenario}.{boundary} did not converge after replay"
                );
                replay_hard_kill_target_once(&writer, &reopened, scenario).await;
                assert!(
                    hard_kill_target_complete(&reopened, scenario).await,
                    "{scenario}.{boundary} duplicated or regressed on idempotent replay"
                );
                reopened.pool().close().await;
                tokio::fs::remove_dir_all(root)
                    .await
                    .expect("remove hard-kill fixture");
            }
        }
    }

    #[cfg(not(unix))]
    #[test]
    fn abrupt_subprocess_failpoints_are_unavailable_without_process_exit_semantics() {
        eprintln!(
            "T12 abrupt transaction acceptance is Unix-only because this target has no _exit/SIGKILL-equivalent harness"
        );
    }
}
