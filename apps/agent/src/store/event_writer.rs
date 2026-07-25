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
use subtle::ConstantTimeEq;
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
    provider::types::{
        ProviderContextAnchor, ProviderContextFragment, ProviderContextItem, PublicMessage,
        StopReason, ToolResultMessage,
    },
    runtime::contracts::{GenerationRecoveryFence, ProcessGeneration, ProcessGenerationLease},
};

use super::{
    BatchBounds, DURABLE_ROW_OVERHEAD_BYTES, DataKeyPurpose, EventBatchSizer, InjectionApplication,
    InjectionBatchSizeInput, InjectionCommandSizeInput, ProviderContextKeyAnchor,
    PublicProjectionBuilder, Redactor, Store, command_payload_digest,
    event_log::{
        EVENT_DIGEST_BYTES, EventChainEntry, authenticate_event_head, extend_event_chain,
        verify_event_head,
    },
    physical_recovery::{ApplyReceiptOutcome, PhysicalRecoveryApplier, PhysicalRecoveryReceipt},
    provider_context::EncryptedProviderContextRecord,
    redactor::search_text_from_projection,
    verify_command_payload_digest,
};

const PREPARED_KEY_MATERIAL_PROOF_DOMAIN: &[u8] = b"sumi-event-batch-prepared-key-material/v1";
const PREPARED_KEY_MATERIAL_PROOF: &[u8] = b"active-key-material";

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
        if value.durable_kind().is_none() {
            bail!("durable event does not match the closed T12 schema");
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

    #[allow(dead_code, reason = "T15 consumes the T12-frozen lifecycle builders")]
    pub(crate) fn agent_end(run_id: impl Into<String>) -> Result<Self> {
        let run_id = run_id.into();
        if run_id.is_empty() {
            bail!("durable AgentEnd run_id must not be empty");
        }
        Self::from_parts(
            AgentEvent::AgentEnd,
            DurableEventMetadata {
                run_id: Some(run_id),
                ..DurableEventMetadata::default()
            },
        )
    }

    #[allow(dead_code, reason = "T15 consumes the T12-frozen retry builder")]
    pub(crate) fn retry_scheduled(
        run_id: impl Into<String>,
        turn_id: impl Into<String>,
        attempt: u32,
        delay_ms: u64,
        retry_at: DateTime<Utc>,
        error_message: impl Into<String>,
    ) -> Result<Self> {
        let run_id = run_id.into();
        let turn_id = turn_id.into();
        let error_message = error_message.into();
        if run_id.is_empty() || turn_id.is_empty() || attempt == 0 || error_message.is_empty() {
            bail!("durable RetryScheduled identity and fields must be non-empty");
        }
        Self::from_parts(
            AgentEvent::RetryScheduled {
                attempt,
                delay_ms,
                retry_at,
                error_message,
            },
            DurableEventMetadata {
                run_id: Some(run_id),
                turn_id: Some(turn_id),
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

    #[allow(dead_code, reason = "T15 consumes the T12-frozen lifecycle builders")]
    pub(crate) fn empty_turn_end(
        run_id: impl Into<String>,
        turn_id: impl Into<String>,
    ) -> Result<Self> {
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
        Self::message_in_turn(event_type, message_id, message, None, None)
    }

    #[allow(
        dead_code,
        reason = "T15 binds assistant/tool lifecycle events to their open turn"
    )]
    pub(crate) fn message_in_turn(
        event_type: &'static str,
        message_id: &str,
        message: &PublicMessage,
        run_id: impl Into<Option<String>>,
        turn_id: impl Into<Option<String>>,
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
        Self::from_parts(
            value,
            DurableEventMetadata {
                run_id: run_id.into(),
                turn_id: turn_id.into(),
                ..DurableEventMetadata::default()
            },
        )
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
        "message_start" | "message_end" => {
            metadata.run_id = take_string(object, "run_id")?;
            metadata.turn_id = take_string(object, "turn_id")?;
        }
        "tool_execution_start" => {
            metadata.run_id = take_string(object, "run_id")?;
            metadata.turn_id = take_string(object, "turn_id")?;
            metadata.tool_state = take_string(object, "state")?;
        }
        "tool_execution_end" => {
            metadata.run_id = take_string(object, "run_id")?;
            metadata.turn_id = take_string(object, "turn_id")?;
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
        "retry_scheduled" => {
            metadata.run_id = take_string(object, "run_id")?;
            metadata.turn_id = take_string(object, "turn_id")?;
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
            AgentEvent::RetryScheduled { .. } => DurableEventIdentity {
                run_id: self.metadata.run_id.as_deref(),
                turn_id: self.metadata.turn_id.as_deref(),
                ..empty("retry_scheduled")
            },
            AgentEvent::MemoryMaintenance { .. } => empty("memory_maintenance"),
            AgentEvent::MessageUpdate { .. }
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
        provider_context: Vec<ProviderContextFragment>,
        eviction_footprint_tokens: u64,
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
    /// T27 supplies the physical proof; T17 validates and applies this
    /// projection only after the complete logical suffix is in the same
    /// EventWriter transaction.
    PhysicalRecovery(PhysicalRecoveryReceipt),
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
        executor_generation: ProcessGeneration,
        idempotency_key: String,
    },
    Start {
        tool_call_id: String,
        run_id: String,
    },
    Finish {
        tool_call_id: String,
        expected: &'static str,
        state: &'static str,
        error_code: Option<&'static str>,
    },
    /// Records a validated call that was deliberately never prepared or run.
    /// This mutation is eventless and must be paired with its error ToolResult
    /// MessageStart/End in the same EventBatch.
    Skip {
        tool_call_id: String,
        command_id: String,
        run_id: String,
        turn_id: String,
        executor_generation: ProcessGeneration,
        idempotency_key: String,
        error_code: &'static str,
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
    steer_mode: Option<&'static str>,
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
        provider_context: Vec<EncryptedProviderContextRecord>,
        eviction_footprint_tokens: u64,
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
    gate: Arc<Mutex<WriterState>>,
}

#[derive(Default)]
pub(super) struct WriterState {
    checkpoint: Option<LifecycleCheckpoint>,
}

#[derive(Clone)]
struct LifecycleCheckpoint {
    event_head: Option<EventLogHead>,
    lifecycle: DurableLifecycleState,
    historical_rows_visited: u64,
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
    LiveBounded,
    ReplayOnly,
    #[cfg(test)]
    UnboundedTestFixture,
}

const LIVE_PENDING_NON_ABORT_MAX_COMMANDS: usize = 32;
const LIVE_PENDING_NON_ABORT_MAX_BYTES: usize = 4 * 1024 * 1024;
const CONTENT_ENVELOPE_OVERHEAD_BYTES: usize = 1 + super::crypto::CONTENT_NONCE_BYTES + 16;
const EVENT_CHAIN_VERIFICATION_PAGE_ROWS: i64 = 64;

/// Startup command-receiver boundary. While a durable suffix remains, exact
/// at-least-once replays may recover their stored ACK, but a new identity can
/// never reach CommandReceived persistence.
pub(crate) struct InboundAdmission {
    mode: InboundAdmissionMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InboundReceiptOrigin {
    NewlyPersisted,
    Replay,
}

pub(crate) struct InboundReceipt {
    pub(crate) ack: CommandAck,
    pub(crate) origin: InboundReceiptOrigin,
    pub(crate) received_at: DateTime<Utc>,
}

impl InboundAdmission {
    pub(crate) fn after_t12_recovery(has_pending_suffix: bool) -> Self {
        Self {
            mode: if has_pending_suffix {
                InboundAdmissionMode::ReplayOnly
            } else {
                InboundAdmissionMode::LiveBounded
            },
        }
    }

    /// The T12 receiver uses this after a terminal control leaves no suffix;
    /// T15 uses the same transition only after its fresh recovery plan is empty.
    pub(crate) fn resume_after_suffix_recovery(&mut self) {
        self.mode = InboundAdmissionMode::LiveBounded;
    }

    pub(crate) fn is_replay_only(&self) -> bool {
        self.mode == InboundAdmissionMode::ReplayOnly
    }

    pub(crate) async fn receive(
        &mut self,
        writer: &EventWriter,
        inbound: &InboundCommand,
    ) -> Result<CommandAck> {
        Ok(self.receive_with_origin(writer, inbound).await?.ack)
    }

    pub(crate) async fn receive_with_origin(
        &mut self,
        writer: &EventWriter,
        inbound: &InboundCommand,
    ) -> Result<InboundReceipt> {
        writer
            .persist_inbound_with_admission(inbound, self.mode)
            .await
    }
}

#[derive(Debug, Error)]
#[error("durable suffix recovery is required before accepting a new command")]
pub(crate) struct RecoveryRequired;

#[derive(Debug, Error)]
#[error("live command admission window is full; retry after a terminal ACK")]
pub(crate) struct InboundBackpressure;

impl EventWriter {
    pub(crate) fn new(store: Arc<Store>) -> Self {
        let gate = store.event_writer_state();
        Self { store, gate }
    }

    pub(crate) fn store(&self) -> &Arc<Store> {
        &self.store
    }

    /// Authenticates and reconstructs the durable lifecycle prefix exactly once
    /// for all EventWriter handles sharing this Store. Startup/recovery must call
    /// this before command admission; write entry points also call it defensively
    /// for tests and non-main embedders.
    pub(crate) async fn initialize_recovery_checkpoint(&self) -> Result<()> {
        let mut state = self.gate.lock().await;
        self.ensure_checkpoint(&mut state).await
    }

    #[cfg(test)]
    async fn historical_rows_visited(&self) -> u64 {
        self.gate
            .lock()
            .await
            .checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.historical_rows_visited)
    }

    #[cfg(test)]
    async fn retained_turn_start_identities(&self) -> usize {
        self.gate
            .lock()
            .await
            .checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.lifecycle.seen_turn_starts.len())
    }

    #[cfg(test)]
    async fn reset_checkpoint_after_direct_fixture_mutation(&self) {
        self.gate.lock().await.checkpoint = None;
    }

    pub(crate) async fn has_recovery_lifecycle_evidence(
        &self,
        event_type: &str,
        run_id: &str,
        turn_id: Option<&str>,
    ) -> Result<bool> {
        let mut state = self.gate.lock().await;
        self.ensure_checkpoint(&mut state).await?;
        let lifecycle = &state
            .checkpoint
            .as_ref()
            .expect("checkpoint initialized for recovery evidence")
            .lifecycle;
        Ok(match event_type {
            "agent_start" => lifecycle.seen_agent_starts.contains(run_id),
            "turn_start" => turn_id.is_some_and(|turn_id| {
                lifecycle
                    .seen_turn_starts
                    .contains(&(run_id.to_owned(), turn_id.to_owned()))
            }),
            _ => false,
        })
    }

    async fn ensure_checkpoint(&self, state: &mut WriterState) -> Result<()> {
        if state.checkpoint.is_none() {
            state.checkpoint =
                Some(reconstruct_authenticated_checkpoint(self.store.as_ref()).await?);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn persist_inbound(&self, inbound: &InboundCommand) -> Result<CommandAck> {
        Ok(self
            .persist_inbound_with_admission(inbound, InboundAdmissionMode::UnboundedTestFixture)
            .await?
            .ack)
    }

    async fn persist_inbound_with_admission(
        &self,
        inbound: &InboundCommand,
        admission: InboundAdmissionMode,
    ) -> Result<InboundReceipt> {
        let mut guard = self.gate.lock().await;
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
            let received_at = self.received_at_for_command(command_id).await?;
            return Ok(InboundReceipt {
                ack,
                origin: InboundReceiptOrigin::Replay,
                received_at,
            });
        }
        if admission == InboundAdmissionMode::ReplayOnly {
            return Err(RecoveryRequired.into());
        }
        self.validate_next_command_seq(seq).await?;
        if admission == InboundAdmissionMode::LiveBounded {
            self.validate_live_admission(inbound, canonical_payload.len())
                .await?;
        }

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
        self.apply_locked(
            EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![projection],
                }],
                injected_commands: Vec::new(),
            },
            &mut guard,
        )
        .await?;
        let ack = self
            .ack_for_command(command_id)
            .await?
            .ok_or_else(|| anyhow!("committed command row is missing"))?;
        let received_at = self.received_at_for_command(command_id).await?;
        Ok(InboundReceipt {
            ack,
            origin: InboundReceiptOrigin::NewlyPersisted,
            received_at,
        })
    }

    async fn validate_live_admission(
        &self,
        inbound: &InboundCommand,
        canonical_payload_bytes: usize,
    ) -> Result<()> {
        let InboundCommand::Valid(envelope) = inbound else {
            // Invalid commands become terminal Rejected rows in the receipt
            // transaction and therefore do not occupy the live pending window.
            return Ok(());
        };

        let encrypted_lengths: Vec<i64> = sqlx::query_scalar(
            "SELECT length(payload_ciphertext)
             FROM inbound_commands
             WHERE status IN ('received', 'applying') AND command_kind <> 'abort'",
        )
        .fetch_all(self.store.pool())
        .await?;
        if encrypted_lengths.len() > LIVE_PENDING_NON_ABORT_MAX_COMMANDS {
            bail!("durable non-Abort command window exceeds its 32-command invariant");
        }
        let mut pending_plaintext_bytes = 0usize;
        for encrypted_bytes in encrypted_lengths.iter().copied() {
            let encrypted_bytes = usize::try_from(encrypted_bytes)
                .context("durable command ciphertext length is negative or too large")?;
            let plaintext_bytes = encrypted_bytes
                .checked_sub(CONTENT_ENVELOPE_OVERHEAD_BYTES)
                .ok_or_else(|| {
                    anyhow!("durable command ciphertext is shorter than its envelope")
                })?;
            pending_plaintext_bytes = pending_plaintext_bytes
                .checked_add(plaintext_bytes)
                .ok_or_else(|| anyhow!("durable command window byte count overflowed"))?;
        }
        if pending_plaintext_bytes > LIVE_PENDING_NON_ABORT_MAX_BYTES {
            bail!("durable non-Abort command window exceeds its 4 MiB invariant");
        }

        if matches!(&envelope.command, Command::Abort {}) {
            let pending_abort_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM inbound_commands
                 WHERE status IN ('received', 'applying') AND command_kind = 'abort'",
            )
            .fetch_one(self.store.pool())
            .await?;
            if pending_abort_count != 0 {
                return Err(InboundBackpressure.into());
            }
            return Ok(());
        }

        if encrypted_lengths.len() == LIVE_PENDING_NON_ABORT_MAX_COMMANDS
            || pending_plaintext_bytes
                .checked_add(canonical_payload_bytes)
                .is_none_or(|total| total > LIVE_PENDING_NON_ABORT_MAX_BYTES)
        {
            return Err(InboundBackpressure.into());
        }
        Ok(())
    }

    #[allow(
        dead_code,
        reason = "T12 freezes the EventBatch entry point consumed by the T15 run loop"
    )]
    pub(crate) async fn apply(&self, batch: EventBatch) -> Result<Vec<u64>> {
        let mut guard = self.gate.lock().await;
        self.apply_locked(batch, &mut guard).await
    }

    /// Hydration entry point for a T27 physical recovery proof.  The receipt
    /// projection must be part of the supplied batch (normally as its final
    /// eventless write); EventWriter then commits the logical suffix, terminal
    /// tool events/results, and T17 application ledger in one SQLite transaction.
    #[allow(dead_code, reason = "T17 hydration caller is composed by T26")]
    pub(crate) async fn apply_physical_recovery(
        &self,
        lease: &ProcessGenerationLease,
        fence: &GenerationRecoveryFence,
        receipt: PhysicalRecoveryReceipt,
        mut batch: EventBatch,
    ) -> Result<(ApplyReceiptOutcome, Vec<u64>)> {
        receipt.validate_for(lease, fence)?;
        let already_present: bool = sqlx::query_scalar(
            "SELECT COUNT(*) > 0 FROM physical_recovery_receipt_applications WHERE receipt_id = ?",
        )
        .bind(&receipt.receipt_id)
        .fetch_one(self.store.pool())
        .await?;
        if already_present {
            // Exact replay is deliberately reduced to an eventless receipt
            // projection.  A caller cannot append a second logical suffix for
            // an already-applied receipt.
            batch.writes = vec![EventWrite {
                event: None,
                projections: vec![Projection::PhysicalRecovery(receipt)],
            }];
            let seqs = self.apply(batch).await?;
            return Ok((ApplyReceiptOutcome::AlreadyApplied, seqs));
        }
        let mut has_receipt_projection = false;
        for projection in batch.writes.iter().flat_map(|write| &write.projections) {
            if let Projection::PhysicalRecovery(existing) = projection {
                if existing != &receipt {
                    bail!("EventBatch PhysicalRecovery projection disagrees with injected receipt");
                }
                has_receipt_projection = true;
            }
        }
        if !has_receipt_projection {
            batch.writes.push(EventWrite {
                event: None,
                projections: vec![Projection::PhysicalRecovery(receipt)],
            });
        }
        let seqs = self.apply(batch).await?;
        // A fresh application is the only outcome possible for a newly
        // appended suffix.  Replay-only batches are handled by the physical
        // projection as an authenticated no-op and return no event sequences.
        Ok((ApplyReceiptOutcome::Applied, seqs))
    }

    #[cfg(test)]
    async fn apply_with_failpoint(
        &self,
        batch: EventBatch,
        fail_after_writes: usize,
    ) -> Result<Vec<u64>> {
        let mut guard = self.gate.lock().await;
        self.apply_locked_with_failpoint(batch, Some(fail_after_writes), None, None, &mut guard)
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
        let mut guard = self.gate.lock().await;
        self.apply_locked_with_failpoint(
            batch,
            None,
            Some((name, after_commit, readiness_path)),
            None,
            &mut guard,
        )
        .await
    }

    #[cfg(test)]
    async fn apply_after_prepare_destroy_key(
        &self,
        batch: EventBatch,
        key_ref: &str,
    ) -> Result<Vec<u64>> {
        let mut guard = self.gate.lock().await;
        self.apply_locked_with_failpoint(batch, None, None, Some(key_ref), &mut guard)
            .await
    }

    pub(crate) async fn apply_idle_abort_cutoff(
        &self,
        abort_command_id: &str,
        abort_seq: u64,
    ) -> Result<Vec<CommandAck>> {
        self.apply_abort_cutoff(abort_command_id, abort_seq, None)
            .await
    }

    pub(crate) async fn apply_active_abort_cutoff(
        &self,
        abort_command_id: &str,
        abort_seq: u64,
        run_id: &str,
    ) -> Result<Vec<CommandAck>> {
        self.apply_abort_cutoff(abort_command_id, abort_seq, Some(run_id))
            .await
    }

    async fn apply_abort_cutoff(
        &self,
        abort_command_id: &str,
        abort_seq: u64,
        run_id: Option<&str>,
    ) -> Result<Vec<CommandAck>> {
        let mut guard = self.gate.lock().await;
        let mut authentication = self.store.pool().begin().await?;
        let command = load_authenticated_command(
            self.store.as_ref(),
            &mut authentication,
            abort_command_id,
            abort_seq,
            "abort",
        )
        .await
        .context("Abort cutoff failed authenticated command validation")?;
        if !matches!(command, Command::Abort {}) {
            bail!("durable abort row contains a different command variant");
        }
        authentication.rollback().await?;
        let abort_seq = sqlite_i64(abort_seq, "Abort command sequence")?;

        let live_rows = sqlx::query(
            "SELECT command_id, seq, run_phase FROM inbound_commands
             WHERE command_kind='user_message' AND status='applying'
               AND run_phase IN (
                 'user_started','user_committed','assistant_started',
                 'hard_steer_requested','cancel_requested'
               )
             ORDER BY seq
             LIMIT 2",
        )
        .fetch_all(self.store.pool())
        .await?;
        if live_rows.len() > 1 {
            bail!("multiple applying live owners violate the owner invariant");
        }
        let live_owner: Option<(String, u64, RunPhase)> = live_rows
            .into_iter()
            .next()
            .map(|row| {
                let command_id: String = row.try_get("command_id")?;
                let seq = sqlite_u64(row.try_get::<i64, _>("seq")?, "live owner sequence")?;
                let phase = RunPhase::parse(row.try_get("run_phase")?)?;
                Ok::<_, anyhow::Error>((command_id, seq, phase))
            })
            .transpose()?;

        if run_id.is_none() && live_owner.is_some() {
            bail!("idle Abort path cannot run while a live owner exists");
        }

        let owner_in_run: Option<(String, u64, RunPhase)> = if let Some(run_id) = run_id {
            let owner_rows = sqlx::query(
                "SELECT command_id, seq, run_phase FROM inbound_commands
                 WHERE run_id = ? AND command_kind='user_message' AND status='applying'
                   AND run_phase IN (
                     'classified','run_started','turn_started',
                     'user_started','user_committed','assistant_started',
                     'hard_steer_requested','cancel_requested'
                   )
                 ORDER BY seq
                 LIMIT 2",
            )
            .bind(run_id)
            .fetch_all(self.store.pool())
            .await?;
            let parsed_rows = owner_rows
                .into_iter()
                .map(|row| {
                    let command_id: String = row.try_get("command_id")?;
                    let seq = sqlite_u64(row.try_get::<i64, _>("seq")?, "run owner sequence")?;
                    let phase = RunPhase::parse(row.try_get("run_phase")?)?;
                    Ok::<_, anyhow::Error>((command_id, seq, phase))
                })
                .collect::<Result<Vec<_>>>()?;
            // A hard-steer step zero leaves the new command classified while
            // the original owner remains in hard_steer_requested.  That
            // classified handoff is pending work, not a second live owner;
            // prefer the one post-user active owner for the Abort cutoff.
            let active_rows = parsed_rows
                .iter()
                .filter(|(_, _, phase)| {
                    !matches!(
                        phase,
                        RunPhase::Classified | RunPhase::RunStarted | RunPhase::TurnStarted
                    )
                })
                .count();
            if active_rows > 1 || (active_rows == 0 && parsed_rows.len() > 1) {
                bail!("multiple applying owners in run {run_id} violate the owner invariant");
            }
            if active_rows == 1 {
                parsed_rows.into_iter().find(|(_, _, phase)| {
                    !matches!(
                        phase,
                        RunPhase::Classified | RunPhase::RunStarted | RunPhase::TurnStarted
                    )
                })
            } else {
                parsed_rows.into_iter().next()
            }
        } else {
            None
        };

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
            bail!("Abort cutoff exceeds the bounded 32-command window");
        }
        let mut pending_terminals = Vec::with_capacity(pending.len());
        let mut terminal_ids = Vec::with_capacity(pending.len() + 1);
        let mut startup: Option<(String, String, RunPhase)> = None;
        let mut owner_seen = false;
        for row in pending {
            let seq = sqlite_u64(row.get::<i64, _>("seq"), "stored command sequence")?;
            let command_id: String = row.try_get("command_id")?;
            let kind: String = row.try_get("command_kind")?;
            let status: String = row.try_get("status")?;
            match (kind.as_str(), status.as_str()) {
                ("user_message", "received") => {
                    pending_terminals.push((command_id.clone(), seq, true, None));
                }
                ("approval_decision", "received") => {
                    pending_terminals.push((command_id.clone(), seq, false, None));
                }
                ("user_message", "applying") => {
                    let application_kind: String = row.try_get("application_kind")?;
                    let row_run_id: String = row.try_get("run_id")?;
                    let turn_id: String = row.try_get("turn_id")?;
                    let phase = RunPhase::parse(row.try_get("run_phase")?)?;
                    let is_owner = owner_in_run
                        .as_ref()
                        .is_some_and(|(owner_id, _, _)| owner_id == &command_id);
                    if is_owner
                        && !matches!(
                            phase,
                            RunPhase::Classified | RunPhase::RunStarted | RunPhase::TurnStarted
                        )
                    {
                        owner_seen = true;
                        continue;
                    }
                    if application_kind == "idle_run"
                        && matches!(
                            phase,
                            RunPhase::Classified | RunPhase::RunStarted | RunPhase::TurnStarted
                        )
                    {
                        if startup.is_some() {
                            bail!(
                                "Abort cutoff requires at most one pre-user idle startup; found {command_id}"
                            );
                        }
                        startup = Some((row_run_id.clone(), turn_id, phase));
                        pending_terminals.push((command_id.clone(), seq, true, Some(row_run_id)));
                    } else if matches!(
                        phase,
                        RunPhase::Classified | RunPhase::RunStarted | RunPhase::TurnStarted
                    ) {
                        pending_terminals.push((command_id.clone(), seq, true, Some(row_run_id)));
                    } else {
                        bail!(
                            "Abort cutoff found unsupported pending user_message {command_id}: {application_kind}/{} {status}",
                            phase.as_str()
                        );
                    }
                }
                _ => {
                    bail!(
                        "Abort cutoff found unsupported pending command {command_id}: {kind}/{status}"
                    );
                }
            }
            terminal_ids.push(command_id);
        }

        let abort_run_id = run_id
            .map(str::to_owned)
            .or_else(|| startup.as_ref().map(|(r, _, _)| r.clone()));

        if let Some(run_id) = run_id {
            if owner_in_run.is_none() && startup.is_none() && live_owner.is_some() {
                bail!(
                    "active Abort cutoff run {run_id} has no durable owner or startup in that run"
                );
            }
            if let Some((owner_id, _, owner_phase)) = &owner_in_run {
                if matches!(
                    owner_phase,
                    RunPhase::Classified | RunPhase::RunStarted | RunPhase::TurnStarted
                ) {
                    // Pre-user idle startup is handled via supersede + empty close, not cancel_requested.
                } else if !owner_seen {
                    bail!(
                        "active Abort cutoff run {run_id} owner {owner_id} was not found in pending scan"
                    );
                } else if live_owner.is_none() {
                    bail!("active Abort cutoff run {run_id} has an owner in an unexpected phase");
                }
            }
        }

        let mut projections = Vec::with_capacity(pending_terminals.len() + 3);
        for (command_id, command_seq, is_user_message, stored_context) in pending_terminals {
            if is_user_message {
                projections.push(Projection::CommandSuperseded {
                    command_id,
                    command_seq,
                    // An unclassified command remains unbound in the database, but its
                    // terminal projection identifies the startup/live run cut off by Abort.
                    run_id: stored_context.or_else(|| abort_run_id.clone()),
                });
            } else {
                projections.push(Projection::CommandApplied {
                    command_id,
                    command_seq,
                    run_id: None,
                });
            }
        }

        if let Some((owner_id, _, owner_phase)) = &owner_in_run
            && !matches!(
                owner_phase,
                RunPhase::Classified | RunPhase::RunStarted | RunPhase::TurnStarted
            )
        {
            projections.push(Projection::RunPhase {
                command_id: owner_id.clone(),
                run_id: abort_run_id.clone().expect("live owner requires run_id"),
                expected: *owner_phase,
                next: RunPhase::CancelRequested,
            });
        }

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

        self.apply_locked(
            EventBatch {
                writes,
                injected_commands: Vec::new(),
            },
            &mut guard,
        )
        .await?;

        let expected_acks = terminal_ids.len();
        let mut acks = Vec::with_capacity(expected_acks);
        for command_id in terminal_ids {
            acks.push(
                self.ack_for_command(&command_id)
                    .await?
                    .ok_or_else(|| anyhow!("terminal command {command_id} disappeared"))?,
            );
        }
        assert_eq!(
            acks.len(),
            expected_acks,
            "Abort cutoff ACK count must match terminal command count"
        );
        Ok(acks)
    }

    async fn apply_locked(&self, batch: EventBatch, state: &mut WriterState) -> Result<Vec<u64>> {
        self.apply_locked_with_failpoint(batch, None, None, None, state)
            .await
    }

    async fn apply_locked_with_failpoint(
        &self,
        batch: EventBatch,
        fail_after_writes: Option<usize>,
        abrupt_failpoint: Option<(&str, bool, &std::path::Path)>,
        destroy_after_prepare: Option<&str>,
        state: &mut WriterState,
    ) -> Result<Vec<u64>> {
        self.ensure_checkpoint(state).await?;
        preflight_materialization_bounds(self.store.redactor(), &batch)?;
        let expected_injections = validate_batch_shape(self.store.redactor(), &batch)?;
        let injected_commands = batch.injected_commands.clone();
        let checkpoint = state
            .checkpoint
            .as_ref()
            .expect("checkpoint initialized before EventBatch")
            .clone();
        let previous_event_head = checkpoint.event_head.clone();
        let next_seq = previous_event_head
            .as_ref()
            .map_or(0, |head| head.last_seq)
            .checked_add(1)
            .ok_or_else(|| anyhow!("durable event sequence overflow"))?;
        let (prepared, transaction_bytes, event_seqs) = self.prepare_batch(batch, next_seq).await?;

        #[cfg(all(test, unix))]
        let env_failpoint_storage = test_env_abrupt_failpoint_for_prepared(&prepared);
        #[cfg(not(all(test, unix)))]
        let env_failpoint_storage: Option<(String, bool, std::path::PathBuf)> = None;
        #[cfg(all(test, unix))]
        let env_write_failpoint_storage = test_env_abrupt_failpoint_for_writes(&prepared);
        #[cfg(not(all(test, unix)))]
        let env_write_failpoint_storage: Option<(String, usize, std::path::PathBuf)> = None;
        let effective_abrupt_failpoint: Option<(&str, bool, &std::path::Path)> = abrupt_failpoint
            .or_else(|| {
                env_failpoint_storage
                    .as_ref()
                    .map(|(name, after_commit, path)| {
                        (name.as_str(), *after_commit, path.as_path())
                    })
            });

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
        let consumes_lifecycle_history = prepared_consumes_lifecycle_history(&prepared);
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
        validate_non_empty_turn_end_bindings(&mut transaction, &prepared).await?;
        validate_required_projection_sets(
            self.store.as_ref(),
            &mut transaction,
            &prepared,
            &checkpoint.lifecycle,
        )
        .await?;
        let next_lifecycle = if consumes_lifecycle_history {
            Some(
                validate_durable_lifecycle_suffix(
                    &mut transaction,
                    &prepared,
                    &checkpoint.lifecycle,
                )
                .await?,
            )
        } else {
            None
        };
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
                apply_projection(
                    self.store.as_ref(),
                    &mut transaction,
                    projection,
                    &event_seqs,
                )
                .await?;
            }
            applied_writes = applied_writes.saturating_add(1);
            if fail_after_writes == Some(applied_writes) {
                bail!("EventWriter test failpoint after {applied_writes} writes");
            }
            if let Some((name, count, readiness_path)) = env_write_failpoint_storage.as_ref()
                && applied_writes == *count
            {
                abrupt_transaction_exit(name, "after_writes", readiness_path.as_path());
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
        if let Some((name, false, readiness_path)) = effective_abrupt_failpoint {
            abrupt_transaction_exit(name, "before_commit", readiness_path);
        }
        transaction
            .commit()
            .await
            .context("failed to commit EventBatch")?;
        state.checkpoint = Some(LifecycleCheckpoint {
            event_head: updated_event_head,
            lifecycle: next_lifecycle.unwrap_or(checkpoint.lifecycle),
            historical_rows_visited: checkpoint.historical_rows_visited,
        });
        if let Some((name, true, readiness_path)) = effective_abrupt_failpoint {
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
                    let steer_mode = match &event.value {
                        AgentEvent::Steered {
                            mode: SteerMode::Hard,
                        } => Some("hard"),
                        AgentEvent::Steered {
                            mode: SteerMode::Soft,
                        } => Some("soft"),
                        _ => None,
                    };
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
                        steer_mode,
                        raw_key_ref: key.key_ref.clone(),
                        raw_key_proof: super::crypto::keyed_proof(
                            key,
                            PREPARED_KEY_MATERIAL_PROOF_DOMAIN,
                            PREPARED_KEY_MATERIAL_PROOF,
                        ),
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
                        provider_context,
                        eviction_footprint_tokens: expected_eviction_footprint_tokens,
                    } => {
                        let l0_disposition = l0_disposition(&message, append_to_l0)?;
                        if !append_to_l0 && !provider_context.is_empty() {
                            bail!(
                                "MessageEnd with append_to_l0=false must not carry provider_context"
                            );
                        }
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
                        let (provider_context_records, eviction_footprint_tokens) = self
                            .prepare_provider_context(
                                &message_id,
                                event_seq,
                                &message,
                                provider_context,
                            )
                            .await?;
                        if eviction_footprint_tokens != expected_eviction_footprint_tokens {
                            bail!(
                                "MessageEnd eviction_footprint_tokens mismatch: expected {expected_eviction_footprint_tokens}, computed {eviction_footprint_tokens}"
                            );
                        }
                        for record in &provider_context_records {
                            charge_transaction_bytes(
                                &mut transaction_bytes,
                                record
                                    .ciphertext()
                                    .len()
                                    .checked_add(record.key_ref().len())
                                    .and_then(|bytes| bytes.checked_add(record.id().len()))
                                    .and_then(|bytes| bytes.checked_add(DURABLE_ROW_OVERHEAD_BYTES))
                                    .ok_or_else(|| {
                                        anyhow!("provider-context record byte count overflow")
                                    })?,
                            )?;
                        }
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
                            raw_key_proof: super::crypto::keyed_proof(
                                key,
                                PREPARED_KEY_MATERIAL_PROOF_DOMAIN,
                                PREPARED_KEY_MATERIAL_PROOF,
                            ),
                            raw_ciphertext: protected.ciphertext,
                            payload: protected.projection,
                            search_text,
                            redaction_version: protected.redaction_version,
                            interrupted,
                            l0_disposition,
                            provider_context: provider_context_records,
                            eviction_footprint_tokens,
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
            (false, None) => command_payload_digest(key, canonical_payload),
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
            payload_key_proof: super::crypto::keyed_proof(
                key,
                PREPARED_KEY_MATERIAL_PROOF_DOMAIN,
                PREPARED_KEY_MATERIAL_PROOF,
            ),
            payload_ciphertext,
            payload_hmac,
            status,
            reject_reason,
            reject_actual_bytes,
        })
    }

    async fn prepare_provider_context(
        &self,
        message_id: &str,
        message_seq: u64,
        message: &PublicMessage,
        fragments: Vec<ProviderContextFragment>,
    ) -> Result<(Vec<EncryptedProviderContextRecord>, u64)> {
        if fragments.is_empty() {
            return Ok((Vec::new(), 0));
        }

        let PublicMessage::Assistant(assistant) = message else {
            bail!("provider context may only accompany an assistant MessageEnd");
        };

        let anchor_id = format!("{message_id}:{message_seq}");
        let key = self
            .store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: self.store.scope().conversation_id.clone(),
                anchor_id,
            })
            .await?;

        let mut records = Vec::with_capacity(fragments.len());
        let mut eviction_footprint_tokens = 0u64;
        let mut ordinal_counter: HashMap<Option<u32>, u32> = HashMap::new();
        for fragment in fragments {
            let next = ordinal_counter.entry(fragment.wire_item_index).or_insert(1);
            let ordinal = *next;
            *next += 1;

            let item = ProviderContextItem {
                origin_message: Some(ProviderContextAnchor {
                    message_id: message_id.to_owned(),
                    message_seq,
                }),
                wire_item_index: fragment.wire_item_index,
                ordinal,
                payload: fragment.payload,
            };

            let wire_label = fragment
                .wire_item_index
                .map_or_else(|| "_".to_owned(), |index| index.to_string());
            let id = format!("{message_id}:{message_seq}:{wire_label}:{ordinal}");
            let record = EncryptedProviderContextRecord::encrypt(
                &item,
                &assistant.origin.provider_instance_id,
                assistant.origin.protocol,
                &assistant.origin.model,
                &id,
                &id,
                &key,
                self.store.scope(),
            )
            .context("failed to encrypt provider-context record")?;
            eviction_footprint_tokens =
                eviction_footprint_tokens.saturating_add(record.eviction_tokens());
            records.push(record);
        }
        Ok((records, eviction_footprint_tokens))
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
            if key.purpose != DataKeyPurpose::Command {
                bail!("injected command references a non-command data key");
            }
            let ciphertext: Vec<u8> = row.try_get("payload_ciphertext")?;
            let aad = self.store.scope().row_aad(
                "inbound_commands",
                command.seq.to_string(),
                DataKeyPurpose::Command,
            );
            let plaintext =
                Zeroizing::new(super::crypto::decrypt_content(&key, &ciphertext, &aad)?);
            let digest: Vec<u8> = row.try_get("payload_hmac")?;
            verify_command_payload_digest(&key, &plaintext, &digest)?;

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
        if key.purpose != DataKeyPurpose::Command {
            bail!("command replay references a non-command data key");
        }
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
            verify_command_payload_digest(&key, canonical_payload, &digest)?;
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

    async fn received_at_for_command(&self, command_id: &str) -> Result<DateTime<Utc>> {
        let value: String =
            sqlx::query_scalar("SELECT received_at FROM inbound_commands WHERE command_id = ?")
                .bind(command_id)
                .fetch_optional(self.store.pool())
                .await?
                .ok_or_else(|| anyhow!("committed command row is missing"))?;
        DateTime::parse_from_rfc3339(&value)
            .map(|timestamp| timestamp.with_timezone(&Utc))
            .map_err(|error| anyhow!("persisted command received_at is invalid: {error}"))
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

async fn load_authenticated_command(
    store: &Store,
    transaction: &mut Transaction<'_, Sqlite>,
    command_id: &str,
    command_seq: u64,
    expected_kind: &str,
) -> Result<Command> {
    let row = sqlx::query(
        "SELECT command_kind, payload_key_ref, payload_ciphertext, payload_hmac
         FROM inbound_commands WHERE command_id=? AND seq=?",
    )
    .bind(command_id)
    .bind(sqlite_i64(command_seq, "command sequence")?)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| anyhow!("authenticated command target does not exist"))?;
    let stored_kind: String = row.try_get("command_kind")?;
    if stored_kind != expected_kind {
        bail!("expected {expected_kind} command, found durable kind {stored_kind}");
    }
    let key_ref: String = row.try_get("payload_key_ref")?;
    let key = store
        .data_key_by_ref_in_transaction(transaction, &key_ref)
        .await?;
    if key.purpose != DataKeyPurpose::Command {
        bail!("durable {expected_kind} references a non-command data key");
    }
    let ciphertext: Vec<u8> = row
        .try_get::<Option<Vec<u8>>, _>("payload_ciphertext")?
        .ok_or_else(|| anyhow!("durable {expected_kind} has no authenticated ciphertext"))?;
    let aad = store.scope().row_aad(
        "inbound_commands",
        command_seq.to_string(),
        DataKeyPurpose::Command,
    );
    let plaintext = Zeroizing::new(super::crypto::decrypt_content(&key, &ciphertext, &aad)?);
    let payload_hmac: Vec<u8> = row.try_get("payload_hmac")?;
    verify_command_payload_digest(&key, &plaintext, &payload_hmac)
        .with_context(|| format!("durable {expected_kind} HMAC is invalid"))?;
    serde_json::from_slice(&plaintext)
        .with_context(|| format!("durable {expected_kind} payload is invalid"))
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

fn prepared_consumes_lifecycle_history(prepared: &[PreparedWrite]) -> bool {
    prepared.iter().any(|write| {
        write.event.is_some()
            || write.projections.iter().any(|projection| {
                matches!(
                    projection,
                    PreparedProjection::Plain(Projection::ToolExecution(
                        ToolExecutionMutation::Prepare { .. }
                            | ToolExecutionMutation::Start { .. }
                            | ToolExecutionMutation::Skip { .. }
                    )) | PreparedProjection::Plain(Projection::CommandClassified { .. })
                        | PreparedProjection::Plain(Projection::RunPhase { .. })
                        | PreparedProjection::Plain(Projection::CommandApplied {
                            run_id: Some(_),
                            ..
                        })
                )
            })
    })
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
        super::crypto::verify_keyed_proof(
            &key,
            PREPARED_KEY_MATERIAL_PROOF_DOMAIN,
            PREPARED_KEY_MATERIAL_PROOF,
            expected_proof,
        )
        .with_context(|| {
            format!(
                "prepared {} key {key_ref} changed material before EventBatch transaction",
                purpose.as_str()
            )
        })?;
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

#[cfg(all(test, unix))]
fn test_env_abrupt_failpoint_for_prepared(
    prepared: &[PreparedWrite],
) -> Option<(String, bool, std::path::PathBuf)> {
    use std::env;

    let name = env::var("SUMI_EVENT_WRITER_FAILPOINT_NAME").ok()?;
    let boundary = env::var("SUMI_EVENT_WRITER_FAILPOINT_BOUNDARY").ok()?;
    let readiness = env::var("SUMI_EVENT_WRITER_FAILPOINT_READY").ok()?;
    let after_commit = match boundary.as_str() {
        "before_commit" => false,
        "after_commit" => true,
        _ => return None,
    };
    if !batch_matches_t16_failpoint(prepared, &name) {
        return None;
    }
    Some((name, after_commit, readiness.into()))
}

#[cfg(all(test, unix))]
fn test_env_abrupt_failpoint_for_writes(
    prepared: &[PreparedWrite],
) -> Option<(String, usize, std::path::PathBuf)> {
    use std::env;

    let name = env::var("SUMI_EVENT_WRITER_FAILPOINT_NAME").ok()?;
    let after_writes = env::var("SUMI_EVENT_WRITER_FAILPOINT_AFTER_WRITES").ok()?;
    let readiness = env::var("SUMI_EVENT_WRITER_FAILPOINT_READY").ok()?;
    let after_writes: usize = after_writes.parse().ok()?;
    if !batch_matches_t16_failpoint(prepared, &name) {
        return None;
    }
    Some((name, after_writes, readiness.into()))
}

#[cfg(all(test, unix))]
fn batch_matches_t16_failpoint(prepared: &[PreparedWrite], name: &str) -> bool {
    match name {
        "hard_steer_step_zero" => prepared.iter().any(|write| {
            write.projections.iter().any(|projection| {
                matches!(
                    projection,
                    PreparedProjection::Plain(Projection::RunPhase {
                        next: RunPhase::HardSteerRequested,
                        ..
                    })
                )
            }) && write.projections.iter().any(|projection| {
                matches!(
                    projection,
                    PreparedProjection::Plain(Projection::CommandClassified {
                        application_kind: ApplicationKind::HardSteer,
                        ..
                    })
                )
            })
        }),
        "hard_steer_partial_message_end" => prepared.iter().any(|write| {
            write.projections.iter().any(|projection| {
                matches!(
                    projection,
                    PreparedProjection::MessageEnd {
                        role: "assistant",
                        interrupted: true,
                        ..
                    }
                )
            })
        }),
        "hard_steer_user_injection" => {
            let projections: Vec<_> = prepared
                .iter()
                .flat_map(|write| write.projections.iter())
                .collect();
            projections.iter().copied().any(|projection| {
                matches!(
                    projection,
                    PreparedProjection::Plain(Projection::CommandApplied {
                        run_id: Some(_),
                        ..
                    })
                )
            }) && projections.iter().copied().any(|projection| {
                matches!(
                    projection,
                    PreparedProjection::MessageEnd { role: "user", .. }
                )
            })
        }
        "turn_end" => {
            let has_turn_end = prepared.iter().any(|write| {
                write
                    .event
                    .as_ref()
                    .is_some_and(|event| event.kind == "turn_end")
            });
            let has_message_end = prepared.iter().any(|write| {
                write
                    .projections
                    .iter()
                    .any(|projection| matches!(projection, PreparedProjection::MessageEnd { .. }))
            });
            has_turn_end && !has_message_end
        }
        "soft_steer_injection" => {
            prepared.iter().any(|write| {
                write.event.as_ref().is_some_and(|event| {
                    event.kind == "steered" && event.steer_mode == Some("soft")
                })
            }) && prepared.iter().any(|write| {
                write
                    .event
                    .as_ref()
                    .is_some_and(|event| event.kind == "turn_end")
            })
        }
        "retry_steer_injection" => {
            let has_soft_steer = prepared.iter().any(|write| {
                write.event.as_ref().is_some_and(|event| {
                    event.kind == "steered" && event.steer_mode == Some("soft")
                })
            });
            let has_turn_end = prepared.iter().any(|write| {
                write
                    .event
                    .as_ref()
                    .is_some_and(|event| event.kind == "turn_end")
            });
            has_soft_steer && !has_turn_end
        }
        "group_tail_owner_transfer" => {
            prepared.iter().any(|write| {
                write.event.as_ref().is_some_and(|event| {
                    event.kind == "steered" && event.steer_mode == Some("soft")
                })
            }) && prepared.iter().any(|write| {
                write.projections.iter().any(|projection| {
                    matches!(
                        projection,
                        PreparedProjection::Plain(Projection::CommandApplied { .. })
                    )
                })
            })
        }
        "active_abort_cutoff" => {
            prepared.iter().any(|write| {
                write.projections.iter().any(|projection| {
                    matches!(
                        projection,
                        PreparedProjection::Plain(Projection::CommandApplied {
                            run_id: Some(_),
                            ..
                        })
                    )
                })
            }) && prepared.iter().any(|write| {
                write.projections.iter().any(|projection| {
                    matches!(
                        projection,
                        PreparedProjection::Plain(Projection::RunPhase {
                            next: RunPhase::CancelRequested,
                            ..
                        })
                    )
                })
            })
        }
        "supersede_cutoff" => {
            let has_empty_or_normal_turn_end = prepared.iter().any(|write| {
                write
                    .event
                    .as_ref()
                    .is_some_and(|event| event.kind == "turn_end")
            });
            let has_agent_end = prepared.iter().any(|write| {
                write
                    .event
                    .as_ref()
                    .is_some_and(|event| event.kind == "agent_end")
            });
            let has_superseded = prepared.iter().any(|write| {
                write.projections.iter().any(|projection| {
                    matches!(
                        projection,
                        PreparedProjection::Plain(Projection::CommandSuperseded { .. })
                    )
                })
            });
            has_empty_or_normal_turn_end && has_agent_end && has_superseded
        }
        _ => false,
    }
}

fn verify_digest_bytes(incoming: &[u8], stored: &[u8]) -> Result<()> {
    if incoming.len() != stored.len() || incoming.ct_eq(stored).unwrap_u8() == 0 {
        bail!("command payload digest mismatch");
    }
    Ok(())
}

fn validate_batch_shape(
    _redactor: &Redactor,
    batch: &EventBatch,
) -> Result<Vec<ExpectedInjection>> {
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
    let mut tool_skip_mutation_ids: HashSet<String> = HashSet::new();
    let mut approval_mutation_ids = HashSet::new();
    let mut approval_pending_mutation_ids: HashSet<String> = HashSet::new();
    let mut approval_resolve_mutation_ids: HashSet<String> = HashSet::new();
    let mut approval_pending_mutations = HashMap::new();
    let mut approval_resolve_mutations = HashMap::new();
    let mut tool_finish_mutations = HashMap::new();
    let mut empty_turn_runs = HashSet::new();
    let mut agent_start_runs = HashSet::new();
    let mut agent_end_runs = HashSet::new();
    let mut turn_start_ids = HashSet::new();
    let mut turn_end_ids = HashSet::new();
    let mut steered_ids = HashSet::new();
    let mut superseded_runs = HashSet::new();
    let mut message_start_positions = HashMap::new();
    let mut message_start_roles = HashMap::new();
    let mut message_end_positions = HashMap::new();
    let mut approval_requested_positions = HashMap::new();
    let mut approval_resolved_positions = HashMap::new();
    let mut tool_end_positions = HashMap::new();
    let mut tool_result_positions = HashMap::new();
    let mut rejected_tool_calls = HashMap::new();
    let mut steered_positions = HashMap::new();
    let mut turn_start_positions = HashMap::new();
    let mut turn_end_positions = HashMap::new();
    let mut agent_end_positions = HashMap::new();
    let mut physical_recovery_positions = Vec::new();
    let mut assistant_start_positions = Vec::new();
    let mut injected_user_end_positions = Vec::new();
    for (write_position, write) in batch.writes.iter().enumerate() {
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
                    message_start_positions.insert(message_id.as_str(), write_position);
                    let role = match message.as_ref() {
                        PublicMessage::User(_) => "user",
                        PublicMessage::Assistant(_) => "assistant",
                        PublicMessage::ToolResult(_) => "tool_result",
                    };
                    message_start_roles.insert(message_id.as_str(), role);
                    if role == "assistant" {
                        assistant_start_positions.push(write_position);
                    }
                }
                AgentEvent::MessageEnd { message_id, .. } => {
                    if !message_end_event_ids.insert(message_id.as_str()) {
                        bail!("duplicate message_end event for message {message_id}");
                    }
                    message_end_positions.insert(message_id.as_str(), write_position);
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
                    approval_requested_positions.insert(request.id.as_str(), write_position);
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
                    approval_resolved_positions.insert(request_id.as_str(), write_position);
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
                    tool_end_positions.insert(tool_call_id.as_str(), write_position);
                }
                AgentEvent::AgentStart => {
                    let run_id = event.metadata.run_id.as_deref().unwrap_or_default();
                    if run_id.is_empty() {
                        bail!("durable agent event run_id must not be empty");
                    }
                    if !agent_start_runs.insert(run_id.to_owned()) {
                        bail!("duplicate AgentStart lifecycle event for run {run_id}");
                    }
                }
                AgentEvent::AgentEnd => {
                    let run_id = event.metadata.run_id.as_deref().unwrap_or_default();
                    if run_id.is_empty() {
                        bail!("durable agent event run_id must not be empty");
                    }
                    if !agent_end_runs.insert(run_id.to_owned()) {
                        bail!("duplicate AgentEnd lifecycle event for run {run_id}");
                    }
                    agent_end_positions.insert(run_id, write_position);
                }
                AgentEvent::TurnStart => {
                    let run_id = event.metadata.run_id.as_deref().unwrap_or_default();
                    let turn_id = event.metadata.turn_id.as_deref().unwrap_or_default();
                    if run_id.is_empty() || turn_id.is_empty() {
                        bail!("durable turn event identity must not be empty");
                    }
                    if !turn_start_ids.insert((run_id.to_owned(), turn_id.to_owned())) {
                        bail!(
                            "duplicate TurnStart lifecycle event for run {run_id} turn {turn_id}"
                        );
                    }
                    turn_start_positions
                        .insert((run_id.to_owned(), turn_id.to_owned()), write_position);
                }
                AgentEvent::TurnEnd {
                    message,
                    tool_results,
                } => {
                    let run_id = event.metadata.run_id.as_deref().unwrap_or_default();
                    let turn_id = event.metadata.turn_id.as_deref().unwrap_or_default();
                    if run_id.is_empty() || turn_id.is_empty() {
                        bail!("durable turn event identity must not be empty");
                    }
                    if !turn_end_ids.insert((run_id.to_owned(), turn_id.to_owned())) {
                        bail!("duplicate TurnEnd lifecycle event for run {run_id} turn {turn_id}");
                    }
                    turn_end_positions
                        .insert((run_id.to_owned(), turn_id.to_owned()), write_position);
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
                AgentEvent::Steered { .. } => {
                    let command_id = event.metadata.command_id.as_deref().unwrap_or_default();
                    let run_id = event.metadata.run_id.as_deref().unwrap_or_default();
                    let turn_id = event.metadata.turn_id.as_deref().unwrap_or_default();
                    if command_id.is_empty() || run_id.is_empty() || turn_id.is_empty() {
                        bail!("durable Steered identity must not be empty");
                    }
                    if !steered_ids.insert((
                        command_id.to_owned(),
                        run_id.to_owned(),
                        turn_id.to_owned(),
                    )) {
                        bail!(
                            "duplicate Steered lifecycle event for command {command_id} run {run_id} turn {turn_id}"
                        );
                    }
                    steered_positions.insert(command_id.to_owned(), write_position);
                }
                AgentEvent::RetryScheduled {
                    attempt,
                    error_message,
                    ..
                } => {
                    let run_id = event.metadata.run_id.as_deref().unwrap_or_default();
                    let turn_id = event.metadata.turn_id.as_deref().unwrap_or_default();
                    if run_id.is_empty()
                        || turn_id.is_empty()
                        || *attempt == 0
                        || error_message.is_empty()
                    {
                        bail!("durable RetryScheduled identity and fields must be non-empty");
                    }
                }
                AgentEvent::MemoryMaintenance { kind } => {
                    if kind.as_str().is_empty() {
                        bail!("durable MemoryMaintenance kind must not be empty");
                    }
                }
                AgentEvent::MessageUpdate { .. }
                | AgentEvent::ToolExecutionUpdate { .. }
                | AgentEvent::Error { .. } => {
                    bail!("volatile or future AgentEvent cannot be persisted by T12");
                }
            }
        }
        for projection in &write.projections {
            if let Projection::PhysicalRecovery(receipt) = projection {
                receipt.validate()?;
                if write.event.is_some() {
                    bail!("PhysicalRecovery projection must be eventless");
                }
                physical_recovery_positions.push(write_position);
            }
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
                        tool_result_positions.insert(
                            message.tool_call_id.as_str(),
                            (message_id.as_str(), write_position, message.is_error),
                        );
                        "tool_result"
                    }
                };
                if let PublicMessage::Assistant(message) = message {
                    for content in &message.content {
                        if let crate::provider::types::PublicAssistantContent::RejectedToolCall {
                            rejected,
                            ..
                        } = content
                        {
                            rejected_tool_calls.insert(rejected.id.as_str(), write_position);
                        }
                    }
                }
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
                    injected_user_end_positions.push(write_position);
                }
            }
            if let Projection::ToolExecution(mutation) = projection {
                let tool_call_id = match mutation {
                    ToolExecutionMutation::Prepare { tool_call_id, .. }
                    | ToolExecutionMutation::Start { tool_call_id, .. }
                    | ToolExecutionMutation::Finish { tool_call_id, .. }
                    | ToolExecutionMutation::Skip { tool_call_id, .. } => tool_call_id,
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
                    ToolExecutionMutation::Skip { error_code, .. } => {
                        if !matches!(*error_code, "length_guard" | "user_steer_cancelled") {
                            bail!(
                                "ToolExecution Skip only supports length_guard or user_steer_cancelled"
                            );
                        }
                        tool_skip_mutation_ids.insert(tool_call_id.clone());
                    }
                }
            }
            if let Projection::Approval(mutation) = projection {
                match mutation {
                    ApprovalMutation::Pending { request_id, .. } => {
                        approval_pending_mutation_ids.insert(request_id.clone());
                        let ApprovalMutation::Pending { tool_call_id, .. } = mutation else {
                            unreachable!()
                        };
                        approval_pending_mutations.insert(
                            request_id.clone(),
                            ApprovalPendingEvidence {
                                tool_call_id: tool_call_id.clone(),
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
    if physical_recovery_positions.len() > 1 {
        bail!("EventBatch may contain at most one PhysicalRecovery projection");
    }
    if let Some(position) = physical_recovery_positions.first()
        && *position != batch.writes.len().saturating_sub(1)
    {
        bail!("PhysicalRecovery projection must be the final EventBatch write");
    }
    for (message_id, start_position) in &message_start_positions {
        if let Some(end_position) = message_end_positions.get(message_id)
            && start_position >= end_position
        {
            bail!("MessageStart for {message_id} must precede its same-batch MessageEnd");
        }
        if message_start_roles.get(message_id) != Some(&"assistant") {
            let end_position = message_end_positions.get(message_id).ok_or_else(|| {
                anyhow!(
                    "non-assistant MessageStart for {message_id} requires its canonical MessageEnd in the same EventBatch"
                )
            })?;
            if start_position >= end_position {
                bail!("MessageStart for {message_id} must precede its same-batch MessageEnd");
            }
            let started = message_start_event_digests
                .get(message_id)
                .and_then(Option::as_ref)
                .expect("MessageStart digest was collected with its identity");
            let projected = projected_message_digests.get(message_id).ok_or_else(|| {
                anyhow!(
                    "non-assistant MessageStart for {message_id} requires its canonical MessageEnd projection"
                )
            })?;
            if started != projected {
                bail!(
                    "non-assistant MessageStart for {message_id} does not match its canonical MessageEnd"
                );
            }
        }
    }
    for (request_id, requested_position) in &approval_requested_positions {
        if let Some(resolved_position) = approval_resolved_positions.get(request_id)
            && requested_position >= resolved_position
        {
            bail!("ApprovalRequested for {request_id} must precede ApprovalResolved");
        }
    }
    for ((run_id, turn_id), turn_start_position) in &turn_start_positions {
        if let Some(turn_end_position) = turn_end_positions.get(&(run_id.clone(), turn_id.clone()))
            && turn_start_position >= turn_end_position
        {
            bail!("TurnStart for {run_id}/{turn_id} must precede TurnEnd");
        }
    }
    for ((run_id, _), turn_end_position) in &turn_end_positions {
        if let Some(agent_end_position) = agent_end_positions.get(run_id.as_str())
            && turn_end_position >= agent_end_position
        {
            bail!("TurnEnd for run {run_id} must precede same-batch AgentEnd");
        }
    }
    if injected_user_end_positions
        .windows(2)
        .any(|positions| positions[0] >= positions[1])
    {
        bail!("injected user MessageEnd events must follow durable command sequence order");
    }
    if let Some(last_user_end) = injected_user_end_positions.last()
        && assistant_start_positions
            .iter()
            .any(|assistant_start| assistant_start <= last_user_end)
    {
        bail!("injected user MessageEnd events must precede same-batch assistant MessageStart");
    }
    let mut previous_steered_position = None;
    for command in &batch.injected_commands {
        let Some(position) = steered_positions.get(command.command_id.as_str()) else {
            continue;
        };
        if previous_steered_position.is_some_and(|previous| previous >= *position) {
            bail!("Steered events must follow injected command durable sequence order");
        }
        previous_steered_position = Some(*position);
    }
    for (command_id, run_id, turn_id) in &steered_ids {
        if let Some(turn_start_position) =
            turn_start_positions.get(&(run_id.clone(), turn_id.clone()))
        {
            let steered_position = steered_positions
                .get(command_id)
                .expect("Steered position was collected with its identity");
            if steered_position >= turn_start_position {
                bail!("Steered for {command_id} must precede same-batch TurnStart");
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
    for tool_call_id in &tool_skip_mutation_ids {
        let Some(message_id) = tool_result_message_ids.get(tool_call_id.as_str()) else {
            bail!(
                "ToolExecution Skip for {tool_call_id} requires its tool-result MessageEnd in the same EventBatch"
            );
        };
        if !message_start_event_ids.contains(message_id) {
            bail!(
                "ToolExecution Skip for {tool_call_id} requires tool-result MessageStart and MessageEnd in the same EventBatch"
            );
        }
    }
    for (tool_call_id, (message_id, result_position, is_error)) in &tool_result_positions {
        let Some(start_position) = message_start_positions.get(message_id) else {
            bail!("tool-result MessageEnd for {tool_call_id} requires its same-batch MessageStart");
        };
        if start_position >= result_position {
            bail!("tool-result MessageStart for {tool_call_id} must precede MessageEnd");
        }
        if tool_finish_mutation_ids.contains(*tool_call_id) {
            let terminal_position = tool_end_positions
                .get(*tool_call_id)
                .expect("terminal event/mutation target equality was checked");
            if terminal_position >= start_position || terminal_position >= result_position {
                bail!(
                    "ToolExecutionEnd for {tool_call_id} must precede its result MessageStart/MessageEnd"
                );
            }
        } else if tool_skip_mutation_ids.contains(*tool_call_id) {
            if !*is_error {
                bail!("not-started tool result for {tool_call_id} must be an error");
            }
        } else {
            let rejected_position = rejected_tool_calls.get(*tool_call_id);
            if !*is_error
                || rejected_position.is_none_or(|rejected_position| {
                    rejected_position >= start_position || rejected_position >= result_position
                })
            {
                bail!(
                    "tool-result MessageEnd for {tool_call_id} requires same-batch ToolExecutionEnd and Finish or a preceding RejectedToolCall"
                );
            }
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
        let mut canonical_message = result.clone();
        canonical_message
            .as_object_mut()
            .expect("typed PublicMessage projection is an object")
            .remove("role");
        let result_message: crate::provider::types::ToolResultMessage =
            serde_json::from_value(canonical_message.clone())
                .context("tool-result MessageEnd projection is invalid")?;
        let mut canonical_event_result = event.result.clone();
        canonical_event_result
            .as_object_mut()
            .ok_or_else(|| anyhow!("tool terminal event result must be an object"))?
            .remove("role");
        if canonical_event_result != canonical_message || event.is_error != result_message.is_error
        {
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
            ToolExecutionMutation::Start {
                tool_call_id,
                run_id,
            } => tool_call_id.len().saturating_add(run_id.len()),
            ToolExecutionMutation::Finish { tool_call_id, .. } => tool_call_id.len(),
            ToolExecutionMutation::Skip {
                tool_call_id,
                command_id,
                run_id,
                turn_id,
                idempotency_key,
                ..
            } => tool_call_id
                .len()
                .saturating_add(command_id.len())
                .saturating_add(run_id.len())
                .saturating_add(turn_id.len())
                .saturating_add(idempotency_key.len()),
        },
        Projection::Approval(mutation) => match mutation {
            ApprovalMutation::Pending {
                request_id,
                tool_call_id,
                run_id,
                turn_id,
                ..
            } => request_id
                .len()
                .saturating_add(tool_call_id.len())
                .saturating_add(run_id.len())
                .saturating_add(turn_id.len()),
            ApprovalMutation::Resolve { request_id, .. } => request_id.len(),
        },
        Projection::PhysicalRecovery(receipt) => receipt
            .receipt_id
            .len()
            .saturating_add(receipt.digest.len())
            .saturating_add(
                receipt
                    .intents
                    .iter()
                    .map(|intent| {
                        intent
                            .tool_call_id
                            .len()
                            .saturating_add(intent.command_id.len())
                            .saturating_add(intent.run_id.len())
                    })
                    .sum::<usize>(),
            ),
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
    let mut terminal_commands = HashSet::new();
    for write in prepared {
        for projection in &write.projections {
            match projection {
                PreparedProjection::Plain(Projection::RunPhase {
                    command_id,
                    run_id,
                    expected,
                    next,
                    ..
                }) => {
                    chains
                        .entry(command_id)
                        .and_modify(|(_, _, final_phase)| *final_phase = *next)
                        .or_insert((run_id, *expected, *next));
                }
                PreparedProjection::Plain(Projection::CommandApplied { command_id, .. }) => {
                    terminal_commands.insert(command_id.as_str());
                }
                _ => {}
            }
        }
    }
    for (command_id, (run_id, initial_phase, final_phase)) in chains {
        if initial_phase.is_owner() {
            pre.insert(run_id.to_owned());
        }
        if final_phase.is_owner() && !terminal_commands.contains(command_id) {
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

fn durable_event_envelope_identity_position(
    prepared: &[PreparedWrite],
    kind: &str,
    identity_field: &str,
    identity: &str,
) -> Result<Option<usize>> {
    for (position, write) in prepared.iter().enumerate() {
        let Some(event) = &write.event else {
            continue;
        };
        if event.kind != kind {
            continue;
        }
        let envelope: Value = serde_json::from_str(&event.envelope)
            .context("prepared durable event envelope is invalid")?;
        if envelope.get(identity_field).and_then(Value::as_str) == Some(identity) {
            return Ok(Some(position));
        }
    }
    Ok(None)
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

async fn validate_abort_cutoff_completeness(
    transaction: &mut Transaction<'_, Sqlite>,
    applied_controls: &[(&str, u64, Option<&str>, usize)],
    supersedes: &[(&str, u64, Option<&str>, usize)],
    abort_command_id: &str,
    abort_seq: u64,
    abort_position: usize,
) -> Result<()> {
    let pending = sqlx::query(
        "SELECT seq, command_id, command_kind, status, run_phase
         FROM inbound_commands
         WHERE seq < ?
           AND (
             status = 'received'
             OR (
               command_kind = 'user_message'
               AND status = 'applying'
               AND run_phase IN ('classified', 'run_started', 'turn_started')
             )
           )
         ORDER BY seq",
    )
    .bind(sqlite_i64(abort_seq, "Abort command sequence")?)
    .fetch_all(&mut **transaction)
    .await?;

    let mut previous_position = None;
    for row in pending {
        let command_seq = sqlite_u64(row.try_get("seq")?, "stored command sequence")?;
        let command_id: String = row.try_get("command_id")?;
        let command_kind: String = row.try_get("command_kind")?;
        let status: String = row.try_get("status")?;
        let run_phase: String = row.try_get("run_phase")?;
        let terminal_position = match command_kind.as_str() {
            "user_message" => supersedes.iter().find_map(
                |(projected_id, projected_seq, _, projected_position)| {
                    (*projected_id == command_id
                        && *projected_seq == command_seq
                        && *projected_position < abort_position)
                        .then_some(*projected_position)
                },
            ),
            "approval_decision" if status == "received" => applied_controls.iter().find_map(
                |(projected_id, projected_seq, projected_run, projected_position)| {
                    (*projected_id == command_id
                        && *projected_seq == command_seq
                        && projected_run.is_none()
                        && *projected_position < abort_position)
                        .then_some(*projected_position)
                },
            ),
            _ => {
                bail!(
                    "Abort cutoff {abort_command_id} found unsupported earlier nonterminal command \
                     {command_id}: {command_kind}/{status}/{run_phase}"
                )
            }
        };
        let terminal_position = terminal_position.ok_or_else(|| {
            anyhow!(
                "Abort cutoff {abort_command_id} is incomplete: earlier nonterminal command \
                 {command_id} at seq {command_seq} has no terminal projection before Abort"
            )
        })?;
        if previous_position.is_some_and(|previous| previous >= terminal_position) {
            bail!(
                "Abort cutoff {abort_command_id} terminal projections must follow exact command \
                 sequence order"
            );
        }
        previous_position = Some(terminal_position);
    }
    Ok(())
}

async fn has_later_abort_cutoff(
    transaction: &mut Transaction<'_, Sqlite>,
    applied_controls: &[(&str, u64, Option<&str>, usize)],
    command_seq: u64,
    projection_position: usize,
) -> Result<bool> {
    for (candidate_id, candidate_seq, _, candidate_position) in applied_controls {
        if *candidate_seq <= command_seq || *candidate_position <= projection_position {
            continue;
        }
        let is_abort: bool = sqlx::query_scalar(
            "SELECT EXISTS(
               SELECT 1 FROM inbound_commands
               WHERE command_id = ? AND seq = ? AND command_kind = 'abort'
                 AND status = 'received' AND run_phase = 'received'
             )",
        )
        .bind(candidate_id)
        .bind(sqlite_i64(*candidate_seq, "Abort command sequence")?)
        .fetch_one(&mut **transaction)
        .await?;
        if is_abort {
            return Ok(true);
        }
    }
    Ok(false)
}

struct PreparedToolBinding {
    command_id: String,
    run_id: String,
}

#[derive(Clone, Copy)]
struct ToolFinishBinding<'a> {
    expected: &'a str,
    state: &'a str,
}

struct OwnerBatchState<'a> {
    phase_transitions: &'a [(&'a str, &'a str, RunPhase, RunPhase)],
    applied_controls: &'a [(&'a str, u64, Option<&'a str>, usize)],
}

async fn load_prepared_tool_binding(
    transaction: &mut Transaction<'_, Sqlite>,
    tool_call_id: &str,
) -> Result<Option<PreparedToolBinding>> {
    let row = sqlx::query(
        "SELECT command_id, run_id
         FROM tool_executions
         WHERE tool_call_id = ? AND state = 'prepared'",
    )
    .bind(tool_call_id)
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(|row| {
        Ok(PreparedToolBinding {
            command_id: row.try_get("command_id")?,
            run_id: row.try_get("run_id")?,
        })
    })
    .transpose()
}

async fn require_tool_owner_binding(
    transaction: &mut Transaction<'_, Sqlite>,
    tool_call_id: &str,
    command_id: &str,
    run_id: &str,
    batch_state: &OwnerBatchState<'_>,
    operation: &str,
    cancellation_cleanup: bool,
) -> Result<()> {
    let stored_phase: Option<String> = sqlx::query_scalar(
        "SELECT run_phase
         FROM inbound_commands
         WHERE command_id = ? AND run_id = ? AND command_kind = 'user_message'
           AND status = 'applying'",
    )
    .bind(command_id)
    .bind(run_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(stored_phase) = stored_phase else {
        bail!(
            "prepared tool {tool_call_id} has no matching durable owner command {command_id} in run {run_id}"
        );
    };
    let mut final_phase = RunPhase::parse(&stored_phase)?;
    for (transition_command, transition_run, expected, next) in batch_state.phase_transitions {
        if *transition_command != command_id || *transition_run != run_id {
            continue;
        }
        if final_phase != *expected {
            bail!(
                "{operation} owner {command_id} phase chain expected {}, found {}",
                expected.as_str(),
                final_phase.as_str()
            );
        }
        final_phase = *next;
    }
    let closes_owner =
        batch_state
            .applied_controls
            .iter()
            .any(|(applied_command, _, applied_run, _)| {
                *applied_command == command_id && *applied_run == Some(run_id)
            });
    if cancellation_cleanup {
        if !matches!(
            final_phase,
            RunPhase::AssistantStarted | RunPhase::HardSteerRequested | RunPhase::CancelRequested
        ) {
            bail!(
                "{operation} for {tool_call_id} requires a live cancellation-cleanup owner, found {}",
                final_phase.as_str()
            );
        }
    } else if final_phase != RunPhase::AssistantStarted || closes_owner {
        bail!(
            "{operation} for {tool_call_id} requires a live assistant/tool execution owner that remains open in the EventBatch, found {}{}",
            final_phase.as_str(),
            if closes_owner {
                " with same-batch owner close"
            } else {
                ""
            }
        );
    }
    Ok(())
}

async fn validate_tool_finish_owner(
    transaction: &mut Transaction<'_, Sqlite>,
    tool_call_id: &str,
    finish: ToolFinishBinding<'_>,
    batch_state: &OwnerBatchState<'_>,
    approval_resolutions: &HashMap<&str, (&str, &str)>,
) -> Result<()> {
    let row = sqlx::query(
        "SELECT command_id, run_id, state
         FROM tool_executions WHERE tool_call_id = ?",
    )
    .bind(tool_call_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| anyhow!("ToolExecutionEnd has no durable tool {tool_call_id}"))?;
    let command_id: String = row.try_get("command_id")?;
    let run_id: String = row.try_get("run_id")?;
    let stored_state: String = row.try_get("state")?;
    if stored_state != finish.expected {
        bail!(
            "ToolExecutionEnd for {tool_call_id} expected {}, found durable state {stored_state}",
            finish.expected
        );
    }

    let stored_phase: String = sqlx::query_scalar(
        "SELECT run_phase FROM inbound_commands
         WHERE command_id = ? AND run_id = ? AND command_kind = 'user_message'
           AND status = 'applying'",
    )
    .bind(&command_id)
    .bind(&run_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| {
        anyhow!(
            "ToolExecutionEnd for {tool_call_id} has no live durable owner {command_id} in run {run_id}"
        )
    })?;
    let mut final_phase = RunPhase::parse(&stored_phase)?;
    for (transition_command, transition_run, expected, next) in batch_state.phase_transitions {
        if *transition_command != command_id || *transition_run != run_id {
            continue;
        }
        if final_phase != *expected {
            bail!(
                "ToolExecutionEnd owner {command_id} phase chain expected {}, found {}",
                expected.as_str(),
                final_phase.as_str()
            );
        }
        final_phase = *next;
    }
    let closes_owner =
        batch_state
            .applied_controls
            .iter()
            .any(|(applied_command, _, applied_run, _)| {
                *applied_command == command_id && *applied_run == Some(run_id.as_str())
            });

    match finish.expected {
        "running"
            if final_phase == RunPhase::AssistantStarted
                || (closes_owner
                    && matches!(
                        final_phase,
                        RunPhase::HardSteerRequested | RunPhase::CancelRequested
                    )) => {}
        "running" => bail!(
            "running ToolExecutionEnd for {tool_call_id} requires its live assistant owner or exact same-batch owner close, found {}",
            final_phase.as_str()
        ),
        "prepared" => {
            let approval_cleanup = sqlx::query_scalar::<_, String>(
                "SELECT id FROM approval_log
                 WHERE tool_call_id = ? AND state = 'pending'",
            )
            .bind(tool_call_id)
            .fetch_optional(&mut **transaction)
            .await?
            .is_some_and(|request_id| {
                approval_resolutions
                    .get(request_id.as_str())
                    .is_some_and(|(resolution, _)| matches!(*resolution, "denied" | "cancelled"))
            });
            if finish.state != "cancelled"
                || !(closes_owner
                    || approval_cleanup
                    || matches!(
                        final_phase,
                        RunPhase::HardSteerRequested | RunPhase::CancelRequested
                    ))
            {
                bail!(
                    "prepared ToolExecutionEnd for {tool_call_id} is permitted only for cancellation or denial cleanup"
                );
            }
        }
        state => bail!("ToolExecutionEnd has invalid expected state {state}"),
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn validate_owner_active_work_terminalized(
    transaction: &mut Transaction<'_, Sqlite>,
    command_id: &str,
    run_id: &str,
    tool_prepares: &HashMap<&str, (&str, &str)>,
    tool_starts: &HashMap<&str, &str>,
    tool_finishes: &HashMap<&str, ToolFinishBinding<'_>>,
    approval_pendings: &[(&str, &str, &str)],
    approval_resolutions: &HashMap<&str, (&str, &str)>,
) -> Result<()> {
    let rows = sqlx::query(
        "SELECT tool_call_id, state FROM tool_executions
         WHERE command_id = ? AND run_id = ? AND state IN ('prepared', 'running')",
    )
    .bind(command_id)
    .bind(run_id)
    .fetch_all(&mut **transaction)
    .await?;
    let mut active_tools = HashMap::new();
    for row in rows {
        active_tools.insert(
            row.try_get::<String, _>("tool_call_id")?,
            row.try_get::<String, _>("state")?,
        );
    }
    for (tool_call_id, (prepared_command, prepared_run)) in tool_prepares {
        if *prepared_command == command_id && *prepared_run == run_id {
            active_tools.insert((*tool_call_id).to_owned(), "prepared".to_owned());
        }
    }
    for (tool_call_id, start_run) in tool_starts {
        if *start_run == run_id && active_tools.contains_key(*tool_call_id) {
            active_tools.insert((*tool_call_id).to_owned(), "running".to_owned());
        }
    }
    for (tool_call_id, state) in &active_tools {
        let finish = tool_finishes.get(tool_call_id.as_str()).ok_or_else(|| {
            anyhow!(
                "owner {command_id} cannot close run {run_id} with active {state} tool {tool_call_id}"
            )
        })?;
        if finish.expected != state {
            bail!(
                "owner {command_id} cleanup for {tool_call_id} expected {}, but its post-batch active state is {state}",
                finish.expected
            );
        }
    }

    let pending_rows = sqlx::query(
        "SELECT a.id, a.tool_call_id FROM approval_log a
         JOIN tool_executions t ON t.tool_call_id = a.tool_call_id
         WHERE t.command_id = ? AND t.run_id = ? AND a.state = 'pending'",
    )
    .bind(command_id)
    .bind(run_id)
    .fetch_all(&mut **transaction)
    .await?;
    let mut pending_approvals = HashMap::new();
    for row in pending_rows {
        pending_approvals.insert(
            row.try_get::<String, _>("id")?,
            row.try_get::<String, _>("tool_call_id")?,
        );
    }
    for (request_id, tool_call_id, pending_run) in approval_pendings {
        if *pending_run == run_id && active_tools.contains_key(*tool_call_id) {
            pending_approvals.insert((*request_id).to_owned(), (*tool_call_id).to_owned());
        }
    }
    for (request_id, tool_call_id) in pending_approvals {
        let resolution = approval_resolutions.get(request_id.as_str()).ok_or_else(|| {
            anyhow!(
                "owner {command_id} cannot close run {run_id} with pending approval {request_id} for {tool_call_id}"
            )
        })?;
        if !matches!(resolution.0, "denied" | "cancelled") {
            bail!(
                "owner {command_id} close requires denial/cancellation cleanup for pending approval {request_id}"
            );
        }
    }
    Ok(())
}

async fn validate_required_projection_sets(
    store: &Store,
    transaction: &mut Transaction<'_, Sqlite>,
    prepared: &[PreparedWrite],
    lifecycle: &DurableLifecycleState,
) -> Result<()> {
    let mut phase_transitions = Vec::new();
    let mut approval_resolutions = HashMap::new();
    let mut approval_resolution_positions = HashMap::new();
    let mut approval_pendings = Vec::new();
    let mut tool_prepares = HashMap::new();
    let mut tool_starts = HashMap::new();
    let mut tool_start_positions = HashMap::new();
    let mut tool_finishes = HashMap::new();
    let mut applied_controls = Vec::new();
    let mut supersedes = Vec::new();
    let mut projection_position = 0usize;
    for write in prepared {
        for projection in &write.projections {
            projection_position = projection_position.saturating_add(1);
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
                    approval_resolution_positions.insert(request_id.as_str(), projection_position);
                }
                PreparedProjection::Plain(Projection::Approval(ApprovalMutation::Pending {
                    request_id,
                    tool_call_id,
                    run_id,
                    ..
                })) => {
                    approval_pendings.push((
                        request_id.as_str(),
                        tool_call_id.as_str(),
                        run_id.as_str(),
                    ));
                }
                PreparedProjection::Plain(Projection::ToolExecution(
                    ToolExecutionMutation::Prepare {
                        tool_call_id,
                        command_id,
                        run_id,
                        ..
                    },
                )) => {
                    tool_prepares.insert(
                        tool_call_id.as_str(),
                        (command_id.as_str(), run_id.as_str()),
                    );
                }
                PreparedProjection::Plain(Projection::ToolExecution(
                    ToolExecutionMutation::Start {
                        tool_call_id,
                        run_id,
                    },
                )) => {
                    tool_starts.insert(tool_call_id.as_str(), run_id.as_str());
                    tool_start_positions.insert(tool_call_id.as_str(), projection_position);
                }
                PreparedProjection::Plain(Projection::ToolExecution(
                    ToolExecutionMutation::Finish {
                        tool_call_id,
                        expected,
                        state,
                        ..
                    },
                )) => {
                    tool_finishes
                        .insert(tool_call_id.as_str(), ToolFinishBinding { expected, state });
                }
                PreparedProjection::Plain(Projection::ToolExecution(
                    ToolExecutionMutation::Skip { .. },
                )) => {}
                PreparedProjection::Plain(Projection::CommandApplied {
                    command_id,
                    command_seq,
                    run_id,
                }) => {
                    applied_controls.push((
                        command_id.as_str(),
                        *command_seq,
                        run_id.as_deref(),
                        projection_position,
                    ));
                }
                PreparedProjection::Plain(Projection::CommandSuperseded {
                    command_id,
                    command_seq,
                    run_id,
                }) => supersedes.push((
                    command_id.as_str(),
                    *command_seq,
                    run_id.as_deref(),
                    projection_position,
                )),
                _ => {}
            }
        }
    }
    let contextual_supersedes: Vec<(&str, u64, &str)> = supersedes
        .iter()
        .filter_map(|(command_id, command_seq, run_id, _)| {
            run_id.map(|run_id| (*command_id, *command_seq, run_id))
        })
        .collect();
    let owner_batch_state = OwnerBatchState {
        phase_transitions: &phase_transitions,
        applied_controls: &applied_controls,
    };

    for (tool_call_id, (command_id, run_id)) in &tool_prepares {
        require_tool_owner_binding(
            transaction,
            tool_call_id,
            command_id,
            run_id,
            &owner_batch_state,
            "ToolExecutionPrepare",
            false,
        )
        .await?;
    }

    for (request_id, tool_call_id, approval_run_id) in &approval_pendings {
        let binding = if let Some((command_id, tool_run_id)) = tool_prepares.get(tool_call_id) {
            PreparedToolBinding {
                command_id: (*command_id).to_owned(),
                run_id: (*tool_run_id).to_owned(),
            }
        } else {
            load_prepared_tool_binding(transaction, tool_call_id)
                .await?
                .ok_or_else(|| {
                    anyhow!("Approval Pending {request_id} requires prepared tool {tool_call_id}")
                })?
        };
        if binding.run_id != *approval_run_id {
            bail!(
                "Approval Pending {request_id} run {approval_run_id} does not match prepared tool {tool_call_id} run {}",
                binding.run_id
            );
        }
        let approval_turn_id = prepared
            .iter()
            .flat_map(|write| &write.projections)
            .find_map(|projection| match projection {
                PreparedProjection::Plain(Projection::Approval(ApprovalMutation::Pending {
                    request_id: candidate,
                    turn_id,
                    ..
                })) if candidate == request_id => Some(turn_id.as_str()),
                _ => None,
            })
            .expect("approval pending projection was collected");
        let owner_turn_id: String = sqlx::query_scalar(
            "SELECT turn_id FROM inbound_commands
             WHERE command_id = ? AND run_id = ? AND command_kind = 'user_message'
               AND status = 'applying'",
        )
        .bind(&binding.command_id)
        .bind(approval_run_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "Approval Pending {request_id} has no durable owner turn for {}",
                binding.command_id
            )
        })?;
        if owner_turn_id != approval_turn_id {
            bail!(
                "Approval Pending {request_id} turn {approval_turn_id} does not match durable owner turn {owner_turn_id}"
            );
        }
        require_tool_owner_binding(
            transaction,
            tool_call_id,
            &binding.command_id,
            approval_run_id,
            &owner_batch_state,
            "Approval Pending",
            false,
        )
        .await?;
    }

    for (tool_call_id, contextual_run_id) in &tool_starts {
        let binding = load_prepared_tool_binding(transaction, tool_call_id)
            .await?
            .ok_or_else(|| anyhow!("ToolExecutionStart requires prepared tool {tool_call_id}"))?;
        if binding.run_id != *contextual_run_id {
            bail!(
                "ToolExecutionStart for {tool_call_id} run {contextual_run_id} does not match prepared run {}",
                binding.run_id
            );
        }
        require_tool_owner_binding(
            transaction,
            tool_call_id,
            &binding.command_id,
            contextual_run_id,
            &owner_batch_state,
            "ToolExecutionStart",
            false,
        )
        .await?;
    }

    let mut approval_resolution_bindings = HashMap::new();
    for (request_id, (resolution, _)) in &approval_resolutions {
        let approval = sqlx::query(
            "SELECT run_id, tool_call_id
             FROM approval_log
             WHERE id = ? AND state = 'pending'",
        )
        .bind(request_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| anyhow!("ApprovalResolved {request_id} has no pending approval"))?;
        let approval_run_id: String = approval.try_get("run_id")?;
        let tool_call_id: String = approval.try_get("tool_call_id")?;
        let binding = load_prepared_tool_binding(transaction, &tool_call_id)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "ApprovalResolved {request_id} is not bound to prepared tool {tool_call_id}"
                )
            })?;
        if binding.run_id != approval_run_id {
            bail!(
                "ApprovalResolved {request_id} run {approval_run_id} does not match prepared tool {tool_call_id} run {}",
                binding.run_id
            );
        }
        require_tool_owner_binding(
            transaction,
            &tool_call_id,
            &binding.command_id,
            &approval_run_id,
            &owner_batch_state,
            "ApprovalResolved",
            *resolution == "cancelled",
        )
        .await?;
        approval_resolution_bindings
            .insert((*request_id).to_owned(), (approval_run_id, tool_call_id));
    }

    for (tool_call_id, finish) in &tool_finishes {
        validate_tool_finish_owner(
            transaction,
            tool_call_id,
            *finish,
            &owner_batch_state,
            &approval_resolutions,
        )
        .await?;
    }

    for (tool_call_id, contextual_run_id) in &tool_starts {
        let approval =
            sqlx::query("SELECT id, state, run_id FROM approval_log WHERE tool_call_id = ?")
                .bind(tool_call_id)
                .fetch_optional(&mut **transaction)
                .await?;
        let Some(approval) = approval else {
            if approval_pendings
                .iter()
                .any(|(_, pending_tool, _)| *pending_tool == *tool_call_id)
            {
                bail!(
                    "ToolExecutionStart for {tool_call_id} cannot treat a same-batch Approval Pending as policy Allow"
                );
            }
            continue;
        };
        let request_id: String = approval.try_get("id")?;
        let state: String = approval.try_get("state")?;
        let approval_run_id: String = approval.try_get("run_id")?;
        let approved_in_batch = approval_resolutions
            .get(request_id.as_str())
            .is_some_and(|(resolution, _)| *resolution == "approved_once")
            && approval_resolution_bindings.get(&request_id).is_some_and(
                |(run_id, approved_tool)| {
                    run_id == *contextual_run_id && approved_tool == *tool_call_id
                },
            );
        let approved_event_before_start = durable_event_envelope_identity_position(
            prepared,
            "approval_resolved",
            "request_id",
            &request_id,
        )?
        .zip(durable_event_envelope_identity_position(
            prepared,
            "tool_execution_start",
            "tool_call_id",
            tool_call_id,
        )?)
        .is_some_and(|(approval_position, start_position)| approval_position < start_position);
        let approved_before_start = approved_in_batch
            && approval_resolution_positions
                .get(request_id.as_str())
                .zip(tool_start_positions.get(tool_call_id))
                .is_some_and(|(approval_position, start_position)| {
                    approval_position < start_position
                })
            && approved_event_before_start;
        let already_approved = state == "approved_once"
            && approval_run_id == *contextual_run_id
            && lifecycle
                .approved_once
                .get(&request_id)
                .is_some_and(|approved_tool| approved_tool == *tool_call_id);
        if !(already_approved
            || state == "pending" && approval_run_id == *contextual_run_id && approved_before_start)
        {
            bail!(
                "ToolExecutionStart for {tool_call_id} cannot bypass approval {request_id} in state {state}; approval must already be approved_once or its exact same-batch approved resolution must precede execution"
            );
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
                let pair_count = phase_transitions
                    .iter()
                    .filter(|(_, transition_run, expected, next)| {
                        *transition_run == run_id
                            && *expected == RunPhase::Classified
                            && *next == RunPhase::RunStarted
                    })
                    .count();
                if pair_count != 1 {
                    bail!(
                        "AgentStart for run {run_id} requires exactly one classified -> run_started pair, found {pair_count}"
                    );
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
                let mut run_start_pairs = 0usize;
                let mut steer_group_pairs = 0usize;
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
                        match (*expected, *next) {
                            (RunPhase::RunStarted, RunPhase::TurnStarted) => {
                                run_start_pairs = run_start_pairs.saturating_add(1);
                            }
                            (RunPhase::Classified, RunPhase::TurnStarted) => {
                                steer_group_pairs = steer_group_pairs.saturating_add(1);
                            }
                            _ => unreachable!("candidate phase pair was filtered"),
                        }
                    }
                }
                let continuation_owner_count = if run_start_pairs == 0 && steer_group_pairs == 0 {
                    sqlx::query_scalar::<_, i64>(
                        "SELECT COUNT(*) FROM inbound_commands
                         WHERE run_id = ? AND command_kind = 'user_message' AND status = 'applying'
                           AND run_phase IN (
                             'user_started', 'user_committed', 'assistant_started',
                             'hard_steer_requested', 'cancel_requested'
                           )",
                    )
                    .bind(run_id)
                    .fetch_one(&mut **transaction)
                    .await?
                } else {
                    0
                };
                if !matches!((run_start_pairs, steer_group_pairs), (1, 0) | (0, 1..))
                    && continuation_owner_count != 1
                {
                    bail!(
                        "TurnStart for {run_id}/{turn_id} requires one exact run-start pair, one non-empty steer-group transition set, or one live continuation owner; found {run_start_pairs}/{steer_group_pairs}/{continuation_owner_count}"
                    );
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
                let pair_count = phase_transitions
                    .iter()
                    .filter(|(transition_command, transition_run, expected, next)| {
                        *transition_command == command_id
                            && *transition_run == run_id
                            && *expected == RunPhase::Classified
                            && *next == RunPhase::TurnStarted
                    })
                    .count();
                if pair_count != 1 {
                    bail!(
                        "Steered for {command_id} requires exactly one classified -> turn_started pair, found {pair_count}"
                    );
                }
                let row = sqlx::query(
                    "SELECT application_kind, turn_id FROM inbound_commands
                     WHERE command_id = ? AND run_id = ? AND command_kind = 'user_message'
                       AND status = 'applying'",
                )
                .bind(command_id)
                .bind(run_id)
                .fetch_optional(&mut **transaction)
                .await?
                .ok_or_else(|| anyhow!("Steered event has no durable command binding"))?;
                let application_kind: String = row.try_get("application_kind")?;
                let durable_turn_id: String = row.try_get("turn_id")?;
                if event.turn_id.as_deref() != Some(durable_turn_id.as_str()) {
                    bail!("Steered turn does not match durable command turn {durable_turn_id}");
                }
                let steered_mode = event
                    .steer_mode
                    .ok_or_else(|| anyhow!("Steered event has no typed mode"))?;
                let expected_mode = match application_kind.as_str() {
                    "hard_steer" => "hard",
                    "soft_steer" | "retry_steer" => "soft",
                    _ => bail!("Steered is invalid for application kind {application_kind}"),
                };
                if steered_mode != expected_mode {
                    bail!(
                        "Steered mode {steered_mode} does not match application kind {application_kind}"
                    );
                }
                let steered_position = prepared
                    .iter()
                    .position(|write| {
                        write.event.as_ref().is_some_and(|candidate| {
                            candidate.kind == "steered"
                                && candidate.command_id.as_deref() == Some(command_id)
                        })
                    })
                    .expect("current Steered event is prepared");
                if matches!(application_kind.as_str(), "hard_steer" | "soft_steer") {
                    let turn_start_position = durable_event_position(
                        prepared,
                        "turn_start",
                        run_id,
                        Some(&durable_turn_id),
                    )
                    .expect("hard/soft steer TurnStart presence was checked");
                    if steered_position >= turn_start_position {
                        bail!("{application_kind} Steered for {command_id} must precede TurnStart");
                    }
                }
            }
            _ => {}
        }
    }

    let mut consumed_approval_resolutions = HashSet::new();
    let mut active_abort_runs = HashSet::new();
    let mut user_owner_close_runs = HashSet::new();
    let mut user_owner_closes = Vec::new();
    let mut abort_applications = Vec::new();
    for (command_id, command_seq, contextual_run_id, position) in &applied_controls {
        let row = sqlx::query(
            "SELECT command_kind, application_kind, status, run_id, run_phase,
                    payload_key_ref, payload_ciphertext, payload_hmac
             FROM inbound_commands WHERE command_id = ? AND seq = ?",
        )
        .bind(command_id)
        .bind(sqlite_i64(*command_seq, "command sequence")?)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| anyhow!("CommandApplied target does not exist"))?;
        let command_kind: String = row.try_get("command_kind")?;
        match command_kind.as_str() {
            "abort" => {
                let command = load_authenticated_command(
                    store,
                    transaction,
                    command_id,
                    *command_seq,
                    "abort",
                )
                .await
                .context("live Abort failed authenticated command validation")?;
                if !matches!(command, Command::Abort {}) {
                    bail!("durable abort row contains a different command variant");
                }
                validate_abort_cutoff_completeness(
                    transaction,
                    &applied_controls,
                    &supersedes,
                    command_id,
                    *command_seq,
                    *position,
                )
                .await?;
                abort_applications.push((*command_seq, *contextual_run_id, *position));
                if let Some(run_id) = *contextual_run_id {
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
                let command = load_authenticated_command(
                    store,
                    transaction,
                    command_id,
                    *command_seq,
                    "approval_decision",
                )
                .await?;
                let Command::ApprovalDecision {
                    request_id,
                    decision,
                } = command
                else {
                    bail!("durable approval_decision row contains a different command variant");
                };
                match *contextual_run_id {
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
                        let (approval_run, tool_call_id) = approval_resolution_bindings
                            .get(&request_id)
                            .ok_or_else(|| {
                                anyhow!(
                                    "ApprovalDecision {request_id} does not resolve a pending approval"
                                )
                            })?;
                        if approval_run != run_id {
                            bail!(
                                "ApprovalDecision {request_id} does not resolve a pending approval in run {run_id}"
                            );
                        }
                        if expected_resolution == "denied" && !tool_starts.is_empty() {
                            bail!("denied ApprovalDecision cannot co-commit ToolExecutionStart");
                        }
                        if !tool_starts.is_empty()
                            && (!tool_starts.contains_key(tool_call_id.as_str())
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
                    None => {
                        let approval_state: Option<String> =
                            sqlx::query_scalar("SELECT state FROM approval_log WHERE id = ?")
                                .bind(&request_id)
                                .fetch_optional(&mut **transaction)
                                .await?;
                        if approval_state.as_deref() == Some("pending")
                            && !has_later_abort_cutoff(
                                transaction,
                                &applied_controls,
                                *command_seq,
                                *position,
                            )
                            .await?
                        {
                            bail!(
                                "no-op ApprovalDecision cannot target pending approval {request_id}"
                            );
                        }
                        tracing::warn!(
                            audit_event = "approval_decision_noop",
                            %command_id,
                            %request_id,
                            approval_state = approval_state.as_deref().unwrap_or("unknown"),
                            "terminal or unknown ApprovalDecision committed as a durable no-op"
                        );
                    }
                }
            }
            "user_message" => {
                let run_id = contextual_run_id
                    .ok_or_else(|| anyhow!("UserMessage CommandApplied requires run_id"))?;
                if row.try_get::<String, _>("status")? != "applying"
                    || row.try_get::<Option<String>, _>("run_id")?.as_deref() != Some(run_id)
                {
                    bail!("UserMessage owner {command_id} has no matching live run binding");
                }
                let mut final_phase = RunPhase::parse(row.try_get("run_phase")?)?;
                for (transition_command, transition_run, expected, next) in &phase_transitions {
                    if *transition_command != *command_id || *transition_run != run_id {
                        continue;
                    }
                    if final_phase != *expected {
                        bail!(
                            "UserMessage owner {command_id} phase chain expected {}, found {}",
                            expected.as_str(),
                            final_phase.as_str()
                        );
                    }
                    final_phase = *next;
                }
                user_owner_close_runs.insert(run_id.to_owned());
                user_owner_closes.push((command_id.to_owned(), run_id.to_owned()));
                let normal_close =
                    has_durable_event(prepared, "agent_end", None, Some(run_id), None, None);
                let handoff =
                    phase_transitions
                        .iter()
                        .any(|(next_owner, transition_run, expected, next)| {
                            *next_owner != *command_id
                                && *transition_run == run_id
                                && *expected == RunPhase::TurnStarted
                                && *next == RunPhase::UserStarted
                        });
                let application_kind: String = row.try_get("application_kind")?;
                let steer_handoff =
                    matches!(application_kind.as_str(), "soft_steer" | "retry_steer");
                match (normal_close, handoff, final_phase) {
                    (true, false, RunPhase::AssistantStarted | RunPhase::CancelRequested) => {}
                    (false, true, RunPhase::AssistantStarted | RunPhase::HardSteerRequested) => {}
                    (false, true, RunPhase::UserCommitted) if steer_handoff => {}
                    (true, true, _) => {
                        bail!("AgentEnd cannot co-commit a same-run owner handoff")
                    }
                    (true, false, phase) => bail!(
                        "AgentEnd owner {command_id} must close from assistant_started or cancel_requested, found {}",
                        phase.as_str()
                    ),
                    (false, true, phase) => bail!(
                        "owner handoff for {command_id} must close from assistant_started or hard_steer_requested, found {}",
                        phase.as_str()
                    ),
                    (false, false, _) => {
                        bail!(
                            "UserMessage owner {command_id} may finish only with AgentEnd or same-run atomic owner handoff"
                        )
                    }
                }
            }
            value => bail!("CommandApplied cannot target command kind {value}"),
        }
    }
    for (command_id, run_id) in &user_owner_closes {
        validate_owner_active_work_terminalized(
            transaction,
            command_id,
            run_id,
            &tool_prepares,
            &tool_starts,
            &tool_finishes,
            &approval_pendings,
            &approval_resolutions,
        )
        .await?;
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
        } else if *resolution == "approved_once" {
            let (_, tool_call_id) = approval_resolution_bindings
                .get(*request_id)
                .expect("validated approval resolution binding");
            if !tool_starts.is_empty()
                && (tool_starts.len() != 1 || !tool_starts.contains_key(tool_call_id.as_str()))
            {
                bail!(
                    "approved ApprovalResolved for {request_id} may co-commit only its exact ToolExecutionStart"
                );
            }
        }
    }
    for (_, run_id, _, next) in &phase_transitions {
        if *next == RunPhase::CancelRequested && !active_abort_runs.contains(*run_id) {
            bail!(
                "owner cancel_requested transition for run {run_id} requires active Abort CommandApplied"
            );
        }
    }
    let mut previous_supersede_seq = None;
    for (command_id, command_seq, contextual_run_id, position) in &supersedes {
        if previous_supersede_seq.is_some_and(|previous| previous >= *command_seq) {
            bail!("CommandSuperseded projections must follow strict command sequence order");
        }
        previous_supersede_seq = Some(*command_seq);
        let row = sqlx::query(
            "SELECT status, application_kind, run_id, turn_id, run_phase
             FROM inbound_commands
             WHERE command_id = ? AND seq = ? AND command_kind = 'user_message'",
        )
        .bind(command_id)
        .bind(sqlite_i64(*command_seq, "command sequence")?)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| anyhow!("CommandSuperseded target does not exist"))?;
        let status: String = row.try_get("status")?;
        let phase = RunPhase::parse(row.try_get("run_phase")?)?;
        let stored_run_id: Option<String> = row.try_get("run_id")?;
        match status.as_str() {
            "received"
                if phase == RunPhase::Received
                    && row
                        .try_get::<Option<String>, _>("application_kind")?
                        .is_none()
                    && stored_run_id.is_none()
                    && row.try_get::<Option<String>, _>("turn_id")?.is_none() => {}
            "applying"
                if matches!(
                    phase,
                    RunPhase::Classified | RunPhase::RunStarted | RunPhase::TurnStarted
                ) =>
            {
                let run_id = contextual_run_id.ok_or_else(|| {
                    anyhow!("classified CommandSuperseded requires its stored run binding")
                })?;
                if stored_run_id.as_deref() != Some(run_id) {
                    bail!("classified CommandSuperseded run context does not match stored binding");
                }
            }
            _ => bail!(
                "CommandSuperseded target has invalid durable state {status}/{}",
                phase.as_str()
            ),
        }
        if !abort_applications
            .iter()
            .any(|(abort_seq, abort_run_id, abort_position)| {
                *abort_position > *position
                    && *abort_seq > *command_seq
                    && *abort_run_id == *contextual_run_id
            })
        {
            bail!(
                "CommandSuperseded for {command_id} requires a same-context later Abort CommandApplied cutoff"
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
        let pending_steers = sqlx::query(
            "SELECT command_id FROM inbound_commands
             WHERE run_id = ? AND command_kind = 'user_message' AND status = 'applying'
               AND application_kind IN ('hard_steer', 'soft_steer', 'retry_steer')
               AND run_phase IN ('classified', 'turn_started')",
        )
        .bind(run_id)
        .fetch_all(&mut **transaction)
        .await?;
        for pending in pending_steers {
            let pending_id: String = pending.try_get("command_id")?;
            if !supersedes.iter().any(|(command_id, _, superseded_run, _)| {
                *command_id == pending_id && *superseded_run == Some(run_id)
            }) {
                bail!(
                    "AgentEnd for run {run_id} cannot leave pending steer {pending_id} un-injected"
                );
            }
        }
        if closes_startup && !active_abort_runs.contains(run_id) {
            bail!("startup AgentEnd for run {run_id} requires its active Abort CommandApplied");
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

async fn validate_non_empty_turn_end_bindings(
    transaction: &mut Transaction<'_, Sqlite>,
    prepared: &[PreparedWrite],
) -> Result<()> {
    for (turn_end_position, write) in prepared.iter().enumerate() {
        let Some(event) = &write.event else {
            continue;
        };
        if event.kind != "turn_end" {
            continue;
        }
        let metadata: DurableEventMetadata = serde_json::from_str(&event.internal_metadata)
            .context("prepared TurnEnd metadata is invalid")?;
        if metadata.empty_turn {
            continue;
        }
        let run_id = event
            .run_id
            .as_deref()
            .ok_or_else(|| anyhow!("non-empty TurnEnd has no run_id"))?;
        let turn_id = event
            .turn_id
            .as_deref()
            .ok_or_else(|| anyhow!("non-empty TurnEnd has no turn_id"))?;

        let lifecycle_metadata = serde_json::to_string(&DurableEventMetadata {
            run_id: Some(run_id.to_owned()),
            turn_id: Some(turn_id.to_owned()),
            ..DurableEventMetadata::default()
        })?;
        let stored_open = sqlx::query_scalar::<_, String>(
            "SELECT event_type FROM agent_events
             WHERE event_type IN ('turn_start', 'turn_end') AND internal_metadata = ?
             ORDER BY seq DESC LIMIT 1",
        )
        .bind(lifecycle_metadata)
        .fetch_optional(&mut **transaction)
        .await?
        .is_some_and(|event_type| event_type == "turn_start");

        let same_batch_start = prepared.iter().position(|candidate| {
            candidate.event.as_ref().is_some_and(|candidate_event| {
                candidate_event.kind == "turn_start"
                    && candidate_event.run_id.as_deref() == Some(run_id)
                    && candidate_event.turn_id.as_deref() == Some(turn_id)
            })
        });
        match same_batch_start {
            Some(_) if stored_open => {
                bail!("TurnStart for {run_id}/{turn_id} is already durably open")
            }
            Some(start_position) if start_position >= turn_end_position => {
                bail!("TurnStart for {run_id}/{turn_id} must precede non-empty TurnEnd")
            }
            Some(_) => {}
            None if stored_open => {}
            None => {
                bail!("non-empty TurnEnd for {run_id}/{turn_id} requires an exact open TurnStart")
            }
        }

        let owner_count: i64 = sqlx::query_scalar(
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
        if owner_count > 1 {
            bail!("non-empty TurnEnd run {run_id} has multiple durable owners");
        }
        let same_batch_owner_open = prepared
            .iter()
            .take(turn_end_position + 1)
            .flat_map(|candidate| candidate.projections.iter())
            .any(|projection| {
                matches!(
                    projection,
                    PreparedProjection::Plain(Projection::RunPhase {
                        run_id: transition_run,
                        next,
                        ..
                    }) if transition_run == run_id && next.is_owner()
                )
            });
        if owner_count == 0 && !same_batch_owner_open {
            bail!("non-empty TurnEnd for {run_id}/{turn_id} requires a live durable run owner");
        }
    }
    Ok(())
}

struct AuthenticatedDurableEvent {
    kind: String,
    internal_metadata: String,
    metadata: DurableEventMetadata,
    key_ref: String,
    ciphertext: Vec<u8>,
    stored_envelope: String,
    redaction_version: u32,
    envelope: Value,
}

async fn load_authenticated_event(
    store: &Store,
    transaction: &mut Transaction<'_, Sqlite>,
    seq: i64,
) -> Result<AuthenticatedDurableEvent> {
    let row = sqlx::query(
        "SELECT rowid AS physical_row_id, event_type, internal_metadata, raw_key_ref,
                raw_ciphertext, envelope, redaction_version
         FROM agent_events WHERE seq=? LIMIT 1",
    )
    .bind(seq)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| anyhow!("durable lifecycle event {seq} disappeared"))?;
    if row.try_get::<i64, _>("physical_row_id")? != seq {
        bail!("durable lifecycle event physical identity does not match sequence {seq}");
    }
    let redaction_version = u32::try_from(row.try_get::<i64, _>("redaction_version")?)
        .context("durable lifecycle redaction version is outside u32")?;
    if redaction_version != store.redactor().version() {
        bail!("durable lifecycle event uses an unsupported redaction version");
    }
    let key_ref: String = row.try_get("raw_key_ref")?;
    let key = store
        .data_key_by_ref_in_transaction(transaction, &key_ref)
        .await?;
    if key.purpose != DataKeyPurpose::Event {
        bail!("durable lifecycle event {seq} references a non-event data key");
    }
    let aad = store
        .scope()
        .row_aad("agent_events", seq.to_string(), DataKeyPurpose::Event);
    let ciphertext: Vec<u8> = row.try_get("raw_ciphertext")?;
    let raw = Zeroizing::new(
        super::crypto::decrypt_content(&key, &ciphertext, &aad)
            .with_context(|| format!("durable lifecycle event {seq} failed authentication"))?,
    );
    let stored_envelope: String = row.try_get("envelope")?;
    if store.redactor().redact_serialized(&raw)? != stored_envelope {
        bail!("durable lifecycle event {seq} projection does not match authenticated raw event");
    }
    let event: AgentEvent = serde_json::from_slice(&raw)
        .with_context(|| format!("durable lifecycle event {seq} has invalid raw payload"))?;
    let kind: String = row.try_get("event_type")?;
    if event.durable_kind() != Some(kind.as_str()) {
        bail!("durable lifecycle event {seq} type disagrees with authenticated raw event");
    }
    let internal_metadata: String = row.try_get("internal_metadata")?;
    Ok(AuthenticatedDurableEvent {
        kind,
        internal_metadata: internal_metadata.clone(),
        metadata: serde_json::from_str(&internal_metadata)
            .context("stored lifecycle metadata is invalid")?,
        key_ref,
        ciphertext,
        stored_envelope: stored_envelope.clone(),
        redaction_version,
        envelope: serde_json::from_str(&stored_envelope)
            .context("stored lifecycle envelope is invalid")?,
    })
}

async fn reconstruct_authenticated_checkpoint(store: &Store) -> Result<LifecycleCheckpoint> {
    let mut transaction = store.pool().begin().await?;
    let event_head = load_verified_event_head_in_transaction(store, &mut transaction).await?;
    let mut lifecycle = DurableLifecycleState::default();
    lifecycle.live_runs.extend(
        sqlx::query_scalar::<_, String>(
            "SELECT DISTINCT run_id FROM inbound_commands
             WHERE run_id IS NOT NULL AND status = 'applying' AND run_phase <> 'finished'",
        )
        .fetch_all(&mut *transaction)
        .await?,
    );
    for row in sqlx::query(
        "SELECT run_id, turn_id FROM inbound_commands
         WHERE run_id IS NOT NULL AND turn_id IS NOT NULL AND status = 'applying'
           AND run_phase IN ('user_started','user_committed','assistant_started',
                             'hard_steer_requested','cancel_requested')",
    )
    .fetch_all(&mut *transaction)
    .await?
    {
        let run_id: String = row.try_get("run_id")?;
        lifecycle
            .open_turns
            .insert(run_id.clone(), row.try_get("turn_id")?);
        lifecycle.inferred_owner_turns.insert(run_id);
    }

    let mut after_seq = 0_i64;
    let mut expected_seq = 1_u64;
    let mut observed_count = 0_u64;
    let mut chain_digest = [0_u8; EVENT_DIGEST_BYTES];
    loop {
        let page: Vec<i64> =
            sqlx::query_scalar("SELECT seq FROM agent_events WHERE seq > ? ORDER BY seq LIMIT ?")
                .bind(after_seq)
                .bind(EVENT_CHAIN_VERIFICATION_PAGE_ROWS)
                .fetch_all(&mut *transaction)
                .await
                .context("failed to page durable event history during startup recovery")?;
        if page.is_empty() {
            break;
        }
        for physical_seq in page {
            let seq =
                u64::try_from(physical_seq).context("durable event sequence is outside u64")?;
            if seq != expected_seq {
                bail!(
                    "durable event chain is not contiguous: expected {expected_seq}, found {seq}"
                );
            }
            let event = load_authenticated_event(store, &mut transaction, physical_seq).await?;
            chain_digest = extend_event_chain(
                &chain_digest,
                EventChainEntry {
                    seq,
                    event_type: &event.kind,
                    internal_metadata: &event.internal_metadata,
                    key_ref: &event.key_ref,
                    ciphertext: &event.ciphertext,
                    envelope: &event.stored_envelope,
                    redaction_version: event.redaction_version,
                },
            );
            apply_lifecycle_event(
                &mut lifecycle,
                &event.kind,
                &event.metadata,
                &event.envelope,
                false,
            )?;
            observed_count = observed_count
                .checked_add(1)
                .ok_or_else(|| anyhow!("durable event count overflow"))?;
            expected_seq = expected_seq
                .checked_add(1)
                .ok_or_else(|| anyhow!("durable event sequence overflow"))?;
            after_seq = physical_seq;
        }
    }
    match &event_head {
        None if observed_count == 0 => {}
        None => bail!("durable events exist without an authenticated event-log head"),
        Some(head)
            if head.last_seq == expected_seq - 1
                && head.event_count == observed_count
                && head.chain_digest == chain_digest => {}
        Some(_) => bail!("durable event history does not match authenticated head"),
    }
    transaction.commit().await?;
    Ok(LifecycleCheckpoint {
        event_head,
        lifecycle,
        historical_rows_visited: observed_count,
    })
}

#[derive(Clone, Default)]
struct DurableLifecycleState {
    live_runs: HashSet<String>,
    open_turns: HashMap<String, String>,
    open_messages: HashMap<String, (String, String, String)>,
    seen_agent_starts: HashSet<String>,
    seen_turn_starts: HashSet<(String, String)>,
    seen_message_starts: HashSet<String>,
    last_assistant_end: HashMap<(String, String), (String, Value)>,
    last_retry_attempt: HashMap<(String, String), u32>,
    assistant_attempt_starts: HashMap<(String, String), u32>,
    tool_results: HashMap<String, Value>,
    tool_call_origins: HashMap<String, ToolCallOrigin>,
    inferred_owner_turns: HashSet<String>,
    pending_approvals: HashMap<String, String>,
    approved_once: HashMap<String, String>,
}

#[derive(Clone)]
struct ToolCallOrigin {
    run_id: String,
    turn_id: String,
    assistant_message_id: String,
}

#[derive(Clone, Copy)]
enum OwnerHandoffAccounting {
    Ignore,
    Account,
}

#[cfg(test)]
fn projection_closes_owner(
    projection: &PreparedProjection,
    command_id: &str,
    run_id: &str,
) -> bool {
    matches!(
        projection,
        PreparedProjection::Plain(Projection::CommandApplied {
            command_id: target,
            run_id: Some(target_run),
            ..
        }) if target == command_id && target_run == run_id
    )
}

/// Applies only the proposed suffix to the lifecycle state authenticated at
/// startup/recovery. Shape validation alone cannot distinguish a legal suffix
/// from a second start or an event bound to a different live turn.
async fn validate_durable_lifecycle_suffix(
    transaction: &mut Transaction<'_, Sqlite>,
    prepared: &[PreparedWrite],
    checkpoint: &DurableLifecycleState,
) -> Result<DurableLifecycleState> {
    let mut state = checkpoint.clone();

    let mut retry_steers = HashSet::new();
    let mut assistant_starts = Vec::new();
    let mut assistant_phase_starts = Vec::new();
    for write in prepared {
        if let Some(event) = &write.event {
            let metadata: DurableEventMetadata = serde_json::from_str(&event.internal_metadata)
                .context("prepared lifecycle metadata is invalid")?;
            let envelope: Value = serde_json::from_str(&event.envelope)
                .context("prepared lifecycle envelope is invalid")?;
            apply_lifecycle_event(&mut state, &event.kind, &metadata, &envelope, true)?;
            let role = envelope
                .get("message")
                .and_then(|message| message.get("role"))
                .and_then(Value::as_str);
            if event.kind == "retry_scheduled"
                || matches!(event.kind.as_str(), "message_start" | "message_end")
                    && role == Some("assistant")
            {
                let (run_id, turn_id) = lifecycle_binding(&metadata, &event.kind)?;
                require_exact_live_owner_turn(
                    transaction,
                    prepared,
                    &run_id,
                    &turn_id,
                    &event.kind,
                    OwnerHandoffAccounting::Ignore,
                )
                .await?;
            }
            if event.kind == "message_start" && role == Some("assistant") {
                assistant_starts.push(lifecycle_binding(&metadata, "assistant MessageStart")?);
            }
        }
        for projection in &write.projections {
            match projection {
                PreparedProjection::Plain(Projection::CommandClassified {
                    application_kind: ApplicationKind::RetrySteer,
                    run_id,
                    turn_id,
                    ..
                }) => {
                    retry_steers.insert((run_id.clone(), turn_id.clone()));
                }
                PreparedProjection::Plain(Projection::CommandClassified {
                    run_id,
                    turn_id,
                    ..
                }) if run_id.is_empty() || turn_id.is_empty() => {
                    bail!("CommandClassified run_id and turn_id must not be empty")
                }
                PreparedProjection::Plain(Projection::CommandClassified { run_id, .. }) => {
                    // The durable command binding establishes the recoverable
                    // run before AgentStart is emitted in the next suffix.
                    state.live_runs.insert(run_id.clone());
                }
                PreparedProjection::Plain(Projection::ToolExecution(
                    ToolExecutionMutation::Prepare {
                        tool_call_id,
                        command_id,
                        run_id,
                        idempotency_key,
                        ..
                    },
                )) if tool_call_id.is_empty()
                    || command_id.is_empty()
                    || run_id.is_empty()
                    || idempotency_key.is_empty() =>
                {
                    bail!("ToolExecutionPrepare identity and idempotency key must not be empty")
                }
                PreparedProjection::Plain(Projection::ToolExecution(
                    ToolExecutionMutation::Prepare {
                        tool_call_id,
                        run_id,
                        ..
                    },
                )) => require_canonical_tool_call_origin(
                    &state,
                    tool_call_id,
                    run_id,
                    "ToolExecutionPrepare",
                )?,
                PreparedProjection::Plain(Projection::ToolExecution(
                    ToolExecutionMutation::Start {
                        tool_call_id,
                        run_id,
                    },
                )) if tool_call_id.is_empty() || run_id.is_empty() => {
                    bail!("ToolExecutionStart identity must not be empty")
                }
                PreparedProjection::Plain(Projection::ToolExecution(
                    ToolExecutionMutation::Start {
                        tool_call_id,
                        run_id,
                    },
                )) => require_canonical_tool_call_origin(
                    &state,
                    tool_call_id,
                    run_id,
                    "ToolExecutionStart",
                )?,
                PreparedProjection::Plain(Projection::ToolExecution(
                    ToolExecutionMutation::Finish { tool_call_id, .. },
                )) if tool_call_id.is_empty() => {
                    bail!("ToolExecutionFinish identity must not be empty")
                }
                PreparedProjection::Plain(Projection::ToolExecution(
                    ToolExecutionMutation::Skip {
                        tool_call_id,
                        command_id,
                        run_id,
                        turn_id,
                        idempotency_key,
                        error_code,
                        ..
                    },
                )) if tool_call_id.is_empty()
                    || command_id.is_empty()
                    || run_id.is_empty()
                    || turn_id.is_empty()
                    || idempotency_key.is_empty()
                    || !matches!(*error_code, "length_guard" | "user_steer_cancelled") =>
                {
                    bail!(
                        "ToolExecutionSkip identity must be non-empty and use length_guard or user_steer_cancelled"
                    )
                }
                PreparedProjection::Plain(Projection::ToolExecution(
                    ToolExecutionMutation::Skip {
                        tool_call_id,
                        run_id,
                        turn_id,
                        ..
                    },
                )) => {
                    let origin = state.tool_call_origins.get(tool_call_id).ok_or_else(|| {
                        anyhow!(
                            "ToolExecutionSkip has no canonical assistant ToolCall {tool_call_id}"
                        )
                    })?;
                    if origin.run_id != *run_id || origin.turn_id != *turn_id {
                        bail!("ToolExecutionSkip does not match the canonical run/turn origin");
                    }
                }
                PreparedProjection::Plain(Projection::RunPhase {
                    run_id,
                    expected: RunPhase::UserCommitted,
                    next: RunPhase::AssistantStarted,
                    ..
                }) => assistant_phase_starts.push(run_id.as_str()),
                _ => {}
            }
        }
    }
    for write in prepared {
        for projection in &write.projections {
            let PreparedProjection::Plain(Projection::RunPhase {
                command_id,
                run_id,
                expected,
                next,
            }) = projection
            else {
                continue;
            };
            if !matches!(
                (*expected, *next),
                (RunPhase::Classified, RunPhase::TurnStarted)
                    | (RunPhase::TurnStarted, RunPhase::UserStarted)
            ) {
                continue;
            }
            let row = sqlx::query(
                "SELECT application_kind, turn_id FROM inbound_commands
                 WHERE command_id=? AND run_id=? AND command_kind='user_message'
                   AND status='applying'",
            )
            .bind(command_id)
            .bind(run_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or_else(|| {
                anyhow!("retry-steer injection member {command_id} has no durable binding")
            })?;
            if row.try_get::<String, _>("application_kind")? == "retry_steer" {
                retry_steers.insert((run_id.clone(), row.try_get("turn_id")?));
            }
        }
    }
    for run_id in assistant_phase_starts {
        let count = assistant_starts
            .iter()
            .filter(|(assistant_run, _)| assistant_run == run_id)
            .count();
        if count != 1 {
            bail!(
                "user_committed -> assistant_started for {run_id} requires exactly one matching assistant MessageStart; found {count}"
            );
        }
    }
    for (run_id, turn_id) in retry_steers {
        require_exact_live_owner_turn(
            transaction,
            prepared,
            &run_id,
            &turn_id,
            "retry_steer",
            OwnerHandoffAccounting::Account,
        )
        .await?;
        let binding = (run_id.clone(), turn_id.clone());
        if state.open_turns.get(&run_id) != Some(&turn_id) {
            bail!("retry_steer for {run_id}/{turn_id} requires that exact current open turn");
        }
        let scheduled = state.last_retry_attempt.get(&binding).copied();
        let started = state.assistant_attempt_starts.get(&binding).copied();
        if scheduled.is_none() || scheduled != started {
            bail!(
                "retry_steer for {run_id}/{turn_id} requires the latest RetryScheduled awaiting the next assistant MessageStart"
            );
        }
    }
    Ok(state)
}

fn require_canonical_tool_call_origin(
    state: &DurableLifecycleState,
    tool_call_id: &str,
    run_id: &str,
    operation: &str,
) -> Result<()> {
    #[cfg(test)]
    if state.inferred_owner_turns.contains(run_id) {
        return Ok(());
    }
    let origin = state.tool_call_origins.get(tool_call_id).ok_or_else(|| {
        anyhow!(
            "{operation} for {tool_call_id} requires its canonical preceding assistant MessageEnd"
        )
    })?;
    if origin.run_id != run_id {
        bail!(
            "{operation} for {tool_call_id} run {run_id} does not match assistant MessageEnd run {}",
            origin.run_id
        );
    }
    if state.open_turns.get(run_id) != Some(&origin.turn_id) {
        bail!(
            "{operation} for {tool_call_id} does not bind the exact open turn {} from assistant MessageEnd {}",
            origin.turn_id,
            origin.assistant_message_id
        );
    }
    Ok(())
}

async fn require_exact_live_owner_turn(
    transaction: &mut Transaction<'_, Sqlite>,
    prepared: &[PreparedWrite],
    run_id: &str,
    turn_id: &str,
    kind: &str,
    handoff_accounting: OwnerHandoffAccounting,
) -> Result<()> {
    let rows = sqlx::query(
        "SELECT command_id, run_phase FROM inbound_commands
         WHERE run_id = ? AND command_kind = 'user_message'
           AND status = 'applying'",
    )
    .bind(run_id)
    .fetch_all(&mut **transaction)
    .await?;
    let mut matches = 0usize;
    for row in &rows {
        let command_id: String = row.try_get("command_id")?;
        let phase: String = row.try_get("run_phase")?;
        let mut phase = RunPhase::parse(&phase)?;
        let mut status = "applying";

        if matches!(handoff_accounting, OwnerHandoffAccounting::Account) {
            for projection in prepared.iter().flat_map(|write| &write.projections) {
                match projection {
                    PreparedProjection::Plain(Projection::RunPhase {
                        command_id: target,
                        run_id: target_run,
                        expected,
                        next,
                    }) if target == &command_id
                        && target_run == run_id
                        && status == "applying"
                        && phase == *expected =>
                    {
                        phase = *next;
                    }
                    PreparedProjection::Plain(Projection::CommandApplied {
                        command_id: target,
                        run_id: target_run,
                        ..
                    }) if target == &command_id
                        && target_run.as_deref() == Some(run_id)
                        && status == "applying"
                        && phase.is_owner() =>
                    {
                        status = "applied";
                        phase = RunPhase::Finished;
                    }
                    PreparedProjection::Plain(Projection::CommandSuperseded {
                        command_id: target,
                        run_id: target_run,
                        ..
                    }) if target == &command_id
                        && target_run.as_deref() == Some(run_id)
                        && status == "applying" =>
                    {
                        status = "superseded";
                        phase = RunPhase::Finished;
                    }
                    _ => {}
                }
            }
            if status == "applying" && phase.is_owner() {
                matches += 1;
            }
        } else {
            let opens_owner =
                prepared
                    .iter()
                    .flat_map(|write| &write.projections)
                    .any(|projection| {
                        matches!(
                            projection,
                            PreparedProjection::Plain(Projection::RunPhase {
                                command_id: target,
                                run_id: target_run,
                                next,
                                ..
                            }) if target == &command_id && target_run == run_id && next.is_owner()
                        )
                    });
            if phase.is_owner() || opens_owner {
                matches += 1;
            }
        }
    }
    if matches != 1 {
        bail!("{kind} for {run_id}/{turn_id} requires exactly one live owner; found {matches}");
    }
    Ok(())
}

fn lifecycle_string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("durable lifecycle field {field} must not be empty"))
}

fn lifecycle_binding(metadata: &DurableEventMetadata, kind: &str) -> Result<(String, String)> {
    let run_id = metadata
        .run_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("{kind} requires non-empty internal run_id metadata"))?;
    let turn_id = metadata
        .turn_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("{kind} requires non-empty internal turn_id metadata"))?;
    Ok((run_id.to_owned(), turn_id.to_owned()))
}

fn apply_lifecycle_event(
    state: &mut DurableLifecycleState,
    kind: &str,
    metadata: &DurableEventMetadata,
    envelope: &Value,
    proposed: bool,
) -> Result<()> {
    match kind {
        "agent_start" => {
            let run_id = metadata
                .run_id
                .as_deref()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| anyhow!("AgentStart requires non-empty internal run_id metadata"))?;
            if !state.seen_agent_starts.insert(run_id.to_owned()) {
                bail!("duplicate AgentStart lifecycle event for run {run_id}");
            }
            state.live_runs.insert(run_id.to_owned());
        }
        "agent_end" => {
            let run_id = metadata
                .run_id
                .as_deref()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| anyhow!("AgentEnd requires non-empty internal run_id metadata"))?;
            if !state.live_runs.remove(run_id) {
                bail!("AgentEnd requires live run {run_id}");
            }
            if (state.open_turns.contains_key(run_id)
                && !state.inferred_owner_turns.contains(run_id))
                || state
                    .open_messages
                    .values()
                    .any(|(message_run, _, _)| message_run == run_id)
            {
                bail!("AgentEnd for {run_id} requires normal-form lifecycle closure");
            }
            state.seen_agent_starts.remove(run_id);
            state
                .seen_turn_starts
                .retain(|(seen_run, _)| seen_run != run_id);
            state
                .last_assistant_end
                .retain(|(seen_run, _), _| seen_run != run_id);
            state
                .last_retry_attempt
                .retain(|(seen_run, _), _| seen_run != run_id);
            state
                .assistant_attempt_starts
                .retain(|(seen_run, _), _| seen_run != run_id);
            state
                .tool_call_origins
                .retain(|_, origin| origin.run_id != run_id);
        }
        "turn_start" => {
            let (run_id, turn_id) = lifecycle_binding(metadata, "TurnStart")?;
            if !state.live_runs.contains(&run_id) {
                bail!("TurnStart for {run_id}/{turn_id} requires live AgentStart");
            }
            if proposed
                && !state.inferred_owner_turns.contains(&run_id)
                && let Some(open_turn) = state.open_turns.get(&run_id)
            {
                bail!("TurnStart for {run_id}/{turn_id} cannot open while {open_turn} is open");
            }
            if !state
                .seen_turn_starts
                .insert((run_id.clone(), turn_id.clone()))
            {
                bail!("duplicate TurnStart lifecycle event for run {run_id} turn {turn_id}");
            }
            state.open_turns.insert(run_id.clone(), turn_id);
            state.inferred_owner_turns.remove(&run_id);
        }
        "turn_end" => {
            let (run_id, turn_id) = lifecycle_binding(metadata, "TurnEnd")?;
            if state.open_turns.get(&run_id) != Some(&turn_id) {
                bail!("TurnEnd for {run_id}/{turn_id} requires that exact open turn");
            }
            if state
                .open_messages
                .values()
                .any(|(message_run, message_turn, _)| {
                    message_run == &run_id && message_turn == &turn_id
                })
            {
                bail!("TurnEnd for {run_id}/{turn_id} cannot close an open message");
            }
            if envelope
                .get("message")
                .is_some_and(|message| !message.is_null())
            {
                let message = envelope
                    .get("message")
                    .expect("message presence was checked");
                if message.get("role").and_then(Value::as_str) != Some("assistant") {
                    bail!("non-empty TurnEnd must carry an assistant message");
                }
                let (_, last_message) = state
                    .last_assistant_end
                    .get(&(run_id.clone(), turn_id.clone()))
                    .ok_or_else(|| {
                        anyhow!(
                            "non-empty TurnEnd for {run_id}/{turn_id} requires assistant MessageEnd"
                        )
                    })?;
                if last_message != message {
                    bail!("TurnEnd assistant message does not match durable MessageEnd");
                }
                let results = envelope
                    .get("tool_results")
                    .and_then(Value::as_array)
                    .ok_or_else(|| anyhow!("TurnEnd tool_results must be an array"))?;
                let mut expected = HashSet::new();
                if let Some(content) = last_message.get("content").and_then(Value::as_array) {
                    for item in content {
                        if item.get("type").and_then(Value::as_str) != Some("tool_call") {
                            continue;
                        }
                        let tool_call_id = item
                            .get("tool_call")
                            .and_then(|call| call.get("id"))
                            .and_then(Value::as_str)
                            .filter(|id| !id.is_empty())
                            .ok_or_else(|| {
                                anyhow!("assistant ToolCall identity must not be empty")
                            })?;
                        if !expected.insert(tool_call_id) {
                            bail!(
                                "assistant MessageEnd contains duplicate ToolCall {tool_call_id}"
                            );
                        }
                    }
                }
                let mut supplied = HashSet::new();
                for result in results {
                    let tool_call_id = lifecycle_string(result, "tool_call_id")?;
                    if !supplied.insert(tool_call_id) {
                        bail!("TurnEnd contains duplicate tool result {tool_call_id}");
                    }
                    let mut canonical_result = result.clone();
                    canonical_result
                        .as_object_mut()
                        .expect("typed TurnEnd tool result is an object")
                        .remove("role");
                    let Some(mut canonical_message) = state.tool_results.get(tool_call_id).cloned()
                    else {
                        bail!(
                            "TurnEnd tool result {tool_call_id} does not match durable tool-result MessageEnd"
                        );
                    };
                    canonical_message
                        .as_object_mut()
                        .expect("typed tool-result MessageEnd is an object")
                        .remove("role");
                    if canonical_message != canonical_result {
                        bail!(
                            "TurnEnd tool result {tool_call_id} does not match durable tool-result MessageEnd"
                        );
                    }
                }
                if supplied != expected {
                    bail!(
                        "TurnEnd tool result IDs must exactly match the current assistant MessageEnd ToolCall IDs"
                    );
                }
            }
            state.open_turns.remove(&run_id);
            state
                .seen_turn_starts
                .remove(&(run_id.clone(), turn_id.clone()));
            state.inferred_owner_turns.remove(&run_id);
            state
                .last_assistant_end
                .remove(&(run_id.clone(), turn_id.clone()));
            state
                .last_retry_attempt
                .remove(&(run_id.clone(), turn_id.clone()));
            state
                .assistant_attempt_starts
                .remove(&(run_id.clone(), turn_id.clone()));
            state
                .tool_call_origins
                .retain(|_, origin| origin.run_id != run_id || origin.turn_id != turn_id);
            state.tool_results.clear();
        }
        "message_start" => {
            let message_id = lifecycle_string(envelope, "message_id")?;
            if !state.seen_message_starts.insert(message_id.to_owned()) {
                bail!("duplicate MessageStart lifecycle event for message {message_id}");
            }
            let role = lifecycle_string(envelope.get("message").unwrap_or(&Value::Null), "role")?;
            if role == "assistant" {
                let (run_id, turn_id) = lifecycle_binding(metadata, "assistant MessageStart")?;
                if state.open_turns.get(&run_id) != Some(&turn_id) {
                    bail!(
                        "assistant MessageStart for {run_id}/{turn_id} requires that exact open turn"
                    );
                }
                if state
                    .open_messages
                    .values()
                    .any(|(message_run, message_turn, message_role)| {
                        message_run == &run_id
                            && message_turn == &turn_id
                            && message_role == "assistant"
                    })
                {
                    bail!(
                        "assistant MessageStart for {run_id}/{turn_id} is ambiguous with an open attempt"
                    );
                }
                let binding = (run_id.clone(), turn_id.clone());
                let prior_starts = state
                    .assistant_attempt_starts
                    .get(&binding)
                    .copied()
                    .unwrap_or(0);
                if prior_starts > 0
                    && state.last_retry_attempt.get(&binding).copied() != Some(prior_starts)
                {
                    bail!(
                        "assistant MessageStart for {run_id}/{turn_id} requires the exact RetryScheduled attempt after its prior MessageEnd"
                    );
                }
                state
                    .assistant_attempt_starts
                    .insert(binding, prior_starts.saturating_add(1));
                state
                    .open_messages
                    .insert(message_id.to_owned(), (run_id, turn_id, role.to_owned()));
            }
        }
        "message_end" => {
            let message_id = lifecycle_string(envelope, "message_id")?;
            let message = envelope
                .get("message")
                .ok_or_else(|| anyhow!("MessageEnd has no message"))?;
            let role = lifecycle_string(message, "role")?;
            if role == "assistant" {
                let (run_id, turn_id) = lifecycle_binding(metadata, "assistant MessageEnd")?;
                match state.open_messages.remove(message_id) {
                    Some((open_run, open_turn, open_role))
                        if open_run == run_id && open_turn == turn_id && open_role == role => {}
                    _ => bail!(
                        "assistant MessageEnd {message_id} does not close its exact open message"
                    ),
                }
                state.last_assistant_end.insert(
                    (run_id.clone(), turn_id.clone()),
                    (message_id.to_owned(), message.clone()),
                );
                if let Some(content) = message.get("content").and_then(Value::as_array) {
                    for item in content {
                        if item.get("type").and_then(Value::as_str) != Some("tool_call") {
                            continue;
                        }
                        let tool_call_id = item
                            .get("tool_call")
                            .and_then(|tool_call| tool_call.get("id"))
                            .and_then(Value::as_str)
                            .filter(|id| !id.is_empty())
                            .ok_or_else(|| {
                                anyhow!("assistant ToolCall identity must not be empty")
                            })?;
                        let origin = ToolCallOrigin {
                            run_id: run_id.clone(),
                            turn_id: turn_id.clone(),
                            assistant_message_id: message_id.to_owned(),
                        };
                        if let Some(previous) = state
                            .tool_call_origins
                            .insert(tool_call_id.to_owned(), origin)
                        {
                            bail!(
                                "assistant ToolCall {tool_call_id} is ambiguous between MessageEnd {} and {message_id}",
                                previous.assistant_message_id
                            );
                        }
                    }
                }
            } else if role == "tool_result" {
                let tool_call_id = lifecycle_string(message, "tool_call_id")?;
                if state
                    .tool_results
                    .insert(tool_call_id.to_owned(), message.clone())
                    .is_some()
                {
                    bail!("duplicate tool-result MessageEnd for {tool_call_id}");
                }
            }
            state.seen_message_starts.remove(message_id);
        }
        "approval_requested" => {
            let request = envelope
                .get("request")
                .ok_or_else(|| anyhow!("ApprovalRequested has no request"))?;
            let request_id = lifecycle_string(request, "id")?;
            let tool_call_id = lifecycle_string(request, "tool_call_id")?;
            if state
                .pending_approvals
                .insert(request_id.to_owned(), tool_call_id.to_owned())
                .is_some()
                || state.approved_once.contains_key(request_id)
            {
                bail!("duplicate ApprovalRequested lifecycle event for {request_id}");
            }
        }
        "approval_resolved" => {
            let request_id = lifecycle_string(envelope, "request_id")?;
            let tool_call_id = state.pending_approvals.remove(request_id).ok_or_else(|| {
                anyhow!("ApprovalResolved for {request_id} requires pending ApprovalRequested")
            })?;
            let decision = envelope
                .get("resolution")
                .and_then(|resolution| resolution.get("decision"))
                .and_then(|decision| decision.get("type"))
                .and_then(Value::as_str);
            if decision == Some("approve_once") {
                state
                    .approved_once
                    .insert(request_id.to_owned(), tool_call_id);
            } else {
                state.approved_once.remove(request_id);
            }
        }
        "tool_execution_start" => {
            let tool_call_id = lifecycle_string(envelope, "tool_call_id")?;
            state
                .pending_approvals
                .retain(|_, pending_tool| pending_tool != tool_call_id);
            state
                .approved_once
                .retain(|_, approved_tool| approved_tool != tool_call_id);
        }
        "retry_scheduled" => {
            let (run_id, turn_id) = lifecycle_binding(metadata, "RetryScheduled")?;
            if state.open_turns.get(&run_id) != Some(&turn_id) {
                bail!("RetryScheduled for {run_id}/{turn_id} requires that exact open turn");
            }
            let (_, message) = state
                .last_assistant_end
                .get(&(run_id.clone(), turn_id.clone()))
                .ok_or_else(|| {
                    anyhow!("RetryScheduled requires a preceding assistant MessageEnd")
                })?;
            if message.get("stop_reason").and_then(Value::as_str) != Some("error") {
                bail!("RetryScheduled requires a preceding error assistant MessageEnd");
            }
            let attempt = envelope
                .get("attempt")
                .and_then(Value::as_u64)
                .and_then(|attempt| u32::try_from(attempt).ok())
                .filter(|attempt| *attempt > 0)
                .ok_or_else(|| anyhow!("RetryScheduled attempt must be non-zero"))?;
            let previous = state
                .last_retry_attempt
                .get(&(run_id.clone(), turn_id.clone()))
                .copied()
                .unwrap_or(0);
            let assistant_attempt = state
                .assistant_attempt_starts
                .get(&(run_id.clone(), turn_id.clone()))
                .copied()
                .ok_or_else(|| anyhow!("RetryScheduled requires an assistant attempt start"))?;
            if previous >= assistant_attempt {
                bail!(
                    "RetryScheduled attempt is not monotonic: assistant error attempt {assistant_attempt} was already consumed"
                );
            }
            if attempt != assistant_attempt {
                bail!(
                    "RetryScheduled attempt {attempt} does not match latest assistant attempt {assistant_attempt}"
                );
            }
            state.last_retry_attempt.insert((run_id, turn_id), attempt);
        }
        _ => {
            let _ = proposed;
        }
    }
    Ok(())
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
    store: &Store,
    transaction: &mut Transaction<'_, Sqlite>,
    projection: PreparedProjection,
    batch_event_seqs: &[u64],
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
            provider_context,
            eviction_footprint_tokens,
        } => {
            if !matches!(role, "user" | "assistant" | "tool_result") {
                bail!("invalid message role {role}");
            }
            match l0_disposition {
                L0Disposition::Append | L0Disposition::ExcludeRetryError => {}
            }
            sqlx::query(
                "INSERT INTO messages(
                    id, seq, role, raw_key_ref, raw_ciphertext, payload, search_text,
                    redaction_version, interrupted, created_at
                 ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&message_id)
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

            for record in provider_context {
                record
                    .insert(&mut **transaction)
                    .await
                    .context("failed to apply provider-context record")?;
            }

            if eviction_footprint_tokens > 0
                && let Some(batch_id) = sqlx::query_scalar::<_, String>(
                    "SELECT batch_id FROM memory_batch_messages WHERE message_id = ?",
                )
                .bind(&message_id)
                .fetch_optional(&mut **transaction)
                .await
                .context("failed to locate memory batch for eviction footprint")?
            {
                sqlx::query(
                    "UPDATE memory_batches
                     SET eviction_footprint_tokens = eviction_footprint_tokens + ?
                     WHERE id = ?",
                )
                .bind(i64::try_from(eviction_footprint_tokens).unwrap_or(i64::MAX))
                .bind(batch_id)
                .execute(&mut **transaction)
                .await
                .context("failed to update memory batch eviction footprint")?;
            }
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
            apply_plain_projection(store, transaction, projection, batch_event_seqs).await?;
        }
    }
    Ok(())
}

async fn apply_plain_projection(
    store: &Store,
    transaction: &mut Transaction<'_, Sqlite>,
    projection: Projection,
    batch_event_seqs: &[u64],
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
        Projection::PhysicalRecovery(receipt) => {
            let outcome = PhysicalRecoveryApplier::new(store)
                .apply_in_transaction(transaction, &receipt, batch_event_seqs)
                .await?;
            if outcome == ApplyReceiptOutcome::AlreadyApplied {
                // A replay must not be allowed to re-emit or mutate logical
                // rows. EventWriter still validates the receipt, then this
                // projection is an authenticated no-op.
            }
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
            .bind(executor_generation.as_i64())
            .bind(idempotency_key)
            .execute(&mut **transaction)
            .await?;
        }
        ToolExecutionMutation::Start {
            tool_call_id,
            run_id,
        } => {
            let result = sqlx::query(
                "UPDATE tool_executions SET state = 'running', started_at = ?
                 WHERE tool_call_id = ? AND run_id = ? AND state = 'prepared'",
            )
            .bind(Utc::now().to_rfc3339())
            .bind(tool_call_id)
            .bind(run_id)
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
        ToolExecutionMutation::Skip {
            tool_call_id,
            command_id,
            run_id,
            turn_id: _,
            executor_generation,
            idempotency_key,
            error_code,
        } => {
            if !matches!(error_code, "length_guard" | "user_steer_cancelled") {
                bail!("ToolExecutionSkip only supports length_guard or user_steer_cancelled");
            }
            // user_steer_cancelled may resolve after a hard steer or Abort moved the
            // original owner out of assistant_started; length_guard remains restricted
            // to the assistant's own turn.
            let phase_condition = if error_code == "user_steer_cancelled" {
                "run_phase IN ('assistant_started','hard_steer_requested','cancel_requested')"
            } else {
                "run_phase = 'assistant_started'"
            };
            let sql = format!(
                "INSERT INTO tool_executions(
                    tool_call_id, command_id, run_id, executor_generation, state,
                    idempotency_key, started_at, finished_at, error_code
                 )
                 SELECT ?, command_id, run_id, ?, 'not_started', ?, NULL, ?, ?
                 FROM inbound_commands
                 WHERE command_id = ? AND run_id = ? AND command_kind = 'user_message'
                   AND status = 'applying' AND {phase_condition}"
            );
            let result = sqlx::query(&sql)
                .bind(tool_call_id)
                .bind(executor_generation.as_i64())
                .bind(idempotency_key)
                .bind(Utc::now().to_rfc3339())
                .bind(error_code)
                .bind(command_id)
                .bind(run_id)
                .execute(&mut **transaction)
                .await?;
            require_single_cas(result.rows_affected(), "ToolExecutionSkip")?;
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
        } => {
            let (request_projection, redaction_version): (String, i64) = sqlx::query_as(
                "SELECT json_extract(envelope, '$.request'), redaction_version
                 FROM agent_events
                 WHERE event_type='approval_requested'
                   AND json_extract(envelope, '$.request.id')=?
                 ORDER BY seq DESC LIMIT 1",
            )
            .bind(&request_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or_else(|| {
                anyhow!("Approval Pending requires its same-batch writer-generated request")
            })?;
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
            .bind(redaction_version)
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
    use chrono::{Duration, Utc};
    use serde_json::json;
    use sqlx::Row;

    use super::*;
    use crate::{
        agent::{
            AdmittedCommand, DurableRunBinding, ProjectedProviderEvent, ProviderEventProjector,
            ProviderTerminalKind, SteerGroup, steer_group_injection_batch,
        },
        gateway::{Command, CommandEnvelope, CommandId, SensitiveCommandPayload},
        provider::types::{
            ApiProtocol, AssistantContent, AssistantMessage, NativeCompactionCoverage,
            ProviderContextFragment, ProviderContextPayload, ProviderEvent, ProviderOrigin,
            ProviderOutput, PublicAssistantContent, PublicAssistantMessage, PublicMessage,
            RejectedToolCall, StopReason, ToolArgumentError, ToolCall, ToolResultMessage, Usage,
            UserContent, UserMessage,
        },
        runtime::contracts::ProcessGeneration,
        store::{
            AgentScope, KeyProvider, ProviderContextEvictionEstimate, RecoveryStep, SuffixRecovery,
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

    fn test_process_generation(raw: u64) -> ProcessGeneration {
        ProcessGeneration::from_wire(raw).expect("valid test process generation")
    }

    #[test]
    fn tool_execution_mutation_boundaries_require_process_generation() {
        fn prepare(generation: ProcessGeneration) -> ToolExecutionMutation {
            ToolExecutionMutation::Prepare {
                tool_call_id: "tool-call".to_owned(),
                command_id: "command".to_owned(),
                run_id: "run".to_owned(),
                executor_generation: generation,
                idempotency_key: "idempotency-key".to_owned(),
            }
        }

        fn skip(generation: ProcessGeneration) -> ToolExecutionMutation {
            ToolExecutionMutation::Skip {
                tool_call_id: "tool-call".to_owned(),
                command_id: "command".to_owned(),
                run_id: "run".to_owned(),
                turn_id: "turn".to_owned(),
                executor_generation: generation,
                idempotency_key: "idempotency-key".to_owned(),
                error_code: "length_guard",
            }
        }

        let _: fn(ProcessGeneration) -> ToolExecutionMutation = prepare;
        let _: fn(ProcessGeneration) -> ToolExecutionMutation = skip;
        let _ = prepare(ProcessGeneration::MIN);
        let _ = skip(ProcessGeneration::MAX);
    }

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

    #[test]
    fn verify_digest_bytes_rejects_length_and_content_mismatches() {
        verify_digest_bytes(b"digest", b"digest").expect("equal digests verify");
        assert!(verify_digest_bytes(b"digest", b"digest-extra").is_err());
        assert!(verify_digest_bytes(b"digest", b"digist").is_err());
    }

    #[test]
    fn owner_handoff_accounting_requires_the_same_command_and_run() {
        let projection = PreparedProjection::Plain(Projection::CommandApplied {
            command_id: "command-a".to_owned(),
            command_seq: 1,
            run_id: Some("run-a".to_owned()),
        });
        assert!(projection_closes_owner(&projection, "command-a", "run-a"));
        assert!(!projection_closes_owner(&projection, "command-a", "run-b"));
        assert!(!projection_closes_owner(&projection, "command-b", "run-a"));
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

    fn test_provider_origin() -> ProviderOrigin {
        ProviderOrigin {
            provider_instance_id: "test-provider-instance".to_owned(),
            protocol: ApiProtocol::OpenAiChatCompletions,
            model: "test-model".to_owned(),
        }
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

    const TOOL_OWNER_COMMAND_ID: &str = "00000000-0000-4000-8000-000000000001";

    async fn seed_tool_owner(store: &Arc<Store>, writer: &EventWriter, run_id: &str) {
        writer
            .persist_inbound(&user_command(1, TOOL_OWNER_COMMAND_ID, "tool owner"))
            .await
            .expect("persist tool owner");
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='idle_run', run_id=?,
                 turn_id='turn-1', run_phase='assistant_started'
             WHERE command_id=?",
        )
        .bind(run_id)
        .bind(TOOL_OWNER_COMMAND_ID)
        .execute(store.pool())
        .await
        .expect("open tool owner");
        writer
            .reset_checkpoint_after_direct_fixture_mutation()
            .await;
    }

    async fn seed_pending_approval(
        store: &Arc<Store>,
        writer: &EventWriter,
        request_id: &str,
        tool_call_id: &str,
        run_id: &str,
    ) {
        seed_tool_owner(store, writer, run_id).await;
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
                            command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                            run_id: run_id.to_owned(),
                            executor_generation: test_process_generation(1),
                            idempotency_key: format!("idem-{tool_call_id}"),
                        }),
                        Projection::Approval(ApprovalMutation::Pending {
                            request_id: request_id.to_owned(),
                            tool_call_id: tool_call_id.to_owned(),
                            run_id: run_id.to_owned(),
                            turn_id: "turn-1".to_owned(),
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

    fn tool_start_write(tool_call_id: &str, run_id: &str) -> EventWrite {
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
                run_id: run_id.to_owned(),
            })],
        }
    }

    fn pending_approval_write(request_id: &str, tool_call_id: &str, run_id: &str) -> EventWrite {
        EventWrite {
            event: Some(
                DurableEvent::new(&json!({
                    "type":"approval_requested",
                    "request":approval_request(request_id, tool_call_id, "mutating"),
                }))
                .expect("typed pending approval event"),
            ),
            projections: vec![Projection::Approval(ApprovalMutation::Pending {
                request_id: request_id.to_owned(),
                tool_call_id: tool_call_id.to_owned(),
                run_id: run_id.to_owned(),
                turn_id: "turn-1".to_owned(),
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

    fn tool_finish_writes(
        tool_call_id: &str,
        expected: &'static str,
        state: &'static str,
        error_code: Option<&'static str>,
        text: &str,
        is_error: bool,
    ) -> Vec<EventWrite> {
        let result = tool_result(tool_call_id, text, is_error);
        let message_id = format!("{tool_call_id}-result");
        vec![
            EventWrite {
                event: Some(
                    DurableEvent::tool_execution_end(
                        tool_call_id.to_owned(),
                        serde_json::to_value(&result).expect("serialize tool result"),
                        is_error,
                        state.to_owned(),
                        error_code.map(str::to_owned),
                    )
                    .expect("typed ToolExecutionEnd"),
                ),
                projections: vec![Projection::ToolExecution(ToolExecutionMutation::Finish {
                    tool_call_id: tool_call_id.to_owned(),
                    expected,
                    state,
                    error_code,
                })],
            },
            EventWrite {
                event: Some(
                    DurableEvent::message("message_start", &message_id, &result)
                        .expect("tool result MessageStart"),
                ),
                projections: Vec::new(),
            },
            EventWrite {
                event: Some(
                    DurableEvent::message("message_end", &message_id, &result)
                        .expect("tool result MessageEnd"),
                ),
                projections: vec![Projection::MessageEnd {
                    message_id,
                    role: "tool_result",
                    message: result,
                    append_to_l0: true,
                    provider_context: Vec::new(),
                    eviction_footprint_tokens: 0,
                }],
            },
        ]
    }

    fn assistant_message(stop_reason: StopReason) -> PublicMessage {
        PublicMessage::Assistant(PublicAssistantMessage {
            content: Vec::new(),
            model: "test-model".to_owned(),
            provider: "test-provider".to_owned(),
            origin: test_provider_origin(),
            usage: Usage::default(),
            stop_reason,
            error_message: (stop_reason == StopReason::Error)
                .then(|| "retryable fixture".to_owned()),
            provider_code: None,
            interrupted: false,
            timestamp: durable_test_timestamp(),
        })
    }

    fn assistant_tool_message(tool_call_ids: &[&str]) -> PublicMessage {
        PublicMessage::Assistant(PublicAssistantMessage {
            content: tool_call_ids
                .iter()
                .enumerate()
                .map(|(index, tool_call_id)| PublicAssistantContent::ToolCall {
                    tool_call: ToolCall {
                        id: (*tool_call_id).to_owned(),
                        name: "test".to_owned(),
                        arguments: serde_json::from_value(json!({}))
                            .expect("object tool arguments"),
                    },
                    wire_item_index: u32::try_from(index).expect("bounded fixture"),
                })
                .collect(),
            model: "test-model".to_owned(),
            provider: "test-provider".to_owned(),
            origin: test_provider_origin(),
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
            error_message: None,
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
                        provider_context: Vec::new(),
                        eviction_footprint_tokens: 0,
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
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
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
                2,
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
    async fn inbound_receipt_replays_the_exact_persisted_received_at() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        writer
            .initialize_recovery_checkpoint()
            .await
            .expect("checkpoint");
        let command = user_command(1, "00000000-0000-4000-8000-000000000001", "timestamped");
        let mut admission = InboundAdmission::after_t12_recovery(false);
        let first = admission
            .receive_with_origin(&writer, &command)
            .await
            .expect("fresh receipt");
        let replay = admission
            .receive_with_origin(&writer, &command)
            .await
            .expect("replayed receipt");
        let durable: String =
            sqlx::query_scalar("SELECT received_at FROM inbound_commands WHERE command_id = ?")
                .bind("00000000-0000-4000-8000-000000000001")
                .fetch_one(store.pool())
                .await
                .expect("durable timestamp");
        let durable = DateTime::parse_from_rfc3339(&durable)
            .expect("valid durable timestamp")
            .with_timezone(&Utc);
        assert_eq!(first.received_at, durable);
        assert_eq!(replay.received_at, durable);
        assert_eq!(first.received_at, replay.received_at);
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
    async fn steer_group_injection_survives_kill_restart_at_injection_boundary() {
        for application_kind in [ApplicationKind::SoftSteer, ApplicationKind::RetrySteer] {
            let root = std::env::temp_dir().join(format!(
                "sumi-steer-group-failpoint-{}-{}",
                application_kind.as_str(),
                uuid::Uuid::now_v7()
            ));
            let _ = tokio::fs::remove_dir_all(&root).await;
            let path = root.join("agent.db");
            let store: Arc<Store> = Store::open(&path, scope(), test_provider())
                .await
                .expect("open fresh file-backed store")
                .into();
            let writer = EventWriter::new(store.clone());

            let owner_id = "00000000-0000-4000-8000-000000000001";
            let run_id = format!("run-{owner_id}");
            let old_turn_id = format!("turn-{owner_id}");
            let group_turn_id = if application_kind == ApplicationKind::SoftSteer {
                "turn-00000000-0000-4000-8000-000000000002".to_owned()
            } else {
                old_turn_id.clone()
            };

            let owner_injected =
                classified_injection(&writer, 1, owner_id, "ignored", "owner").await;
            writer
                .apply(EventBatch {
                    writes: injection_writes(owner_id, "ignored", "owner"),
                    injected_commands: vec![owner_injected],
                })
                .await
                .expect("inject owner");

            let assistant_id = "assistant-1";
            let assistant_stop_reason = if application_kind == ApplicationKind::SoftSteer {
                StopReason::Stop
            } else {
                StopReason::Error
            };
            let assistant_msg = assistant_message(assistant_stop_reason);
            let assistant_append_to_l0 = assistant_stop_reason != StopReason::Error;
            writer
                .apply(EventBatch {
                    writes: vec![
                        EventWrite {
                            event: Some(
                                DurableEvent::message_in_turn(
                                    "message_start",
                                    assistant_id,
                                    &assistant_msg,
                                    Some(run_id.clone()),
                                    Some(old_turn_id.clone()),
                                )
                                .expect("assistant MessageStart"),
                            ),
                            projections: vec![Projection::RunPhase {
                                command_id: owner_id.to_owned(),
                                run_id: run_id.clone(),
                                expected: RunPhase::UserCommitted,
                                next: RunPhase::AssistantStarted,
                            }],
                        },
                        EventWrite {
                            event: Some(
                                DurableEvent::message_in_turn(
                                    "message_end",
                                    assistant_id,
                                    &assistant_msg,
                                    Some(run_id.clone()),
                                    Some(old_turn_id.clone()),
                                )
                                .expect("assistant MessageEnd"),
                            ),
                            projections: vec![Projection::MessageEnd {
                                message_id: assistant_id.to_owned(),
                                role: "assistant",
                                message: assistant_msg.clone(),
                                append_to_l0: assistant_append_to_l0,
                                provider_context: Vec::new(),
                                eviction_footprint_tokens: 0,
                            }],
                        },
                    ],
                    injected_commands: Vec::new(),
                })
                .await
                .expect("close assistant message");

            if application_kind == ApplicationKind::RetrySteer {
                let retry_at = durable_test_timestamp() + Duration::seconds(4);
                writer
                    .apply(EventBatch {
                        writes: vec![EventWrite {
                            event: Some(
                                DurableEvent::retry_scheduled(
                                    &run_id,
                                    &old_turn_id,
                                    1,
                                    4000,
                                    retry_at,
                                    "retryable fixture",
                                )
                                .expect("retry scheduled"),
                            ),
                            projections: Vec::new(),
                        }],
                        injected_commands: Vec::new(),
                    })
                    .await
                    .expect("schedule retry");
            }

            let steer_id = "00000000-0000-4000-8000-000000000002";
            writer
                .persist_inbound(&user_command(2, steer_id, "steer now"))
                .await
                .expect("persist steer command");
            sqlx::query("UPDATE inbound_commands SET received_at=? WHERE command_id=?")
                .bind(durable_test_timestamp().to_rfc3339())
                .bind(steer_id)
                .execute(store.pool())
                .await
                .expect("pin steer receipt timestamp");
            writer
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: None,
                        projections: vec![Projection::CommandClassified {
                            command_id: steer_id.to_owned(),
                            application_kind,
                            run_id: run_id.clone(),
                            turn_id: group_turn_id.to_owned(),
                        }],
                    }],
                    injected_commands: Vec::new(),
                })
                .await
                .expect("classify steer command");

            let steer_command = AdmittedCommand::new(
                CommandEnvelope {
                    seq: 2,
                    command_id: CommandId::parse(steer_id).expect("canonical test UUID"),
                    command: Command::UserMessage {
                        text: "steer now".to_owned(),
                        attachments: Vec::new(),
                    },
                },
                durable_test_timestamp(),
            );

            let previous_owner = DurableRunBinding {
                command_id: owner_id.to_owned(),
                command_seq: 1,
                run_id: run_id.clone(),
                turn_id: old_turn_id.clone(),
                executor_generation: ProcessGeneration::MIN,
            };
            let mut group = SteerGroup::new(application_kind, run_id.clone(), group_turn_id)
                .expect("create steer group");
            group
                .push(steer_command, store.redactor())
                .expect("push steer command");
            let closing_turn_message =
                (application_kind == ApplicationKind::SoftSteer).then(|| assistant_msg.clone());
            let snapshot = group.snapshot(previous_owner, closing_turn_message);
            let batch = steer_group_injection_batch(snapshot).expect("build steer group batch");

            let fail_after_writes = if application_kind == ApplicationKind::SoftSteer {
                2
            } else {
                1
            };
            let error = writer
                .apply_with_failpoint(batch.clone(), fail_after_writes)
                .await
                .expect_err("failpoint must interrupt the steer group injection");
            assert!(error.to_string().contains("test failpoint"));
            drop(writer);
            drop(store);

            let reopened: Arc<Store> = Store::open(&path, scope(), test_provider())
                .await
                .expect("restart store after interrupted steer group injection")
                .into();

            let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
                .fetch_one(reopened.pool())
                .await
                .expect("count events after restart");
            let messages: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
                .fetch_one(reopened.pool())
                .await
                .expect("count messages after restart");
            let (expected_pre_events, expected_pre_messages) =
                if application_kind == ApplicationKind::SoftSteer {
                    (6, 2)
                } else {
                    (7, 2)
                };
            assert_eq!(
                (events, messages),
                (expected_pre_events, expected_pre_messages),
                "partial {} steer group injection must roll back completely",
                application_kind.as_str()
            );

            let owner_status: (String, String) = sqlx::query_as(
                "SELECT status, run_phase FROM inbound_commands WHERE command_id = ?",
            )
            .bind(owner_id)
            .fetch_one(reopened.pool())
            .await
            .expect("owner status after restart");
            assert_eq!(owner_status.0, "applying", "owner must remain applying");
            assert_eq!(
                owner_status.1, "assistant_started",
                "owner must remain assistant_started"
            );

            let steer_status: (String, String) = sqlx::query_as(
                "SELECT status, run_phase FROM inbound_commands WHERE command_id = ?",
            )
            .bind(steer_id)
            .fetch_one(reopened.pool())
            .await
            .expect("steer status after restart");
            assert_eq!(steer_status.0, "applying", "steer must remain applying");
            assert_eq!(steer_status.1, "classified", "steer must remain classified");

            EventWriter::new(reopened.clone())
                .apply(batch)
                .await
                .expect("same steer group batch succeeds once after restart");

            let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
                .fetch_one(reopened.pool())
                .await
                .expect("count committed events after reapply");
            let messages: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
                .fetch_one(reopened.pool())
                .await
                .expect("count committed messages after reapply");
            let (expected_events, expected_messages) =
                if application_kind == ApplicationKind::SoftSteer {
                    (11, 3)
                } else {
                    (10, 3)
                };
            assert_eq!(
                (events, messages),
                (expected_events, expected_messages),
                "{} steer group must commit exactly after restart",
                application_kind.as_str()
            );

            let owner_status: (String, String) = sqlx::query_as(
                "SELECT status, run_phase FROM inbound_commands WHERE command_id = ?",
            )
            .bind(owner_id)
            .fetch_one(reopened.pool())
            .await
            .expect("owner status after reapply");
            assert_eq!(
                owner_status.0, "applied",
                "owner must close after steer injection"
            );
            assert_eq!(
                owner_status.1, "finished",
                "owner must finish after steer injection"
            );

            let steer_status: (String, String) = sqlx::query_as(
                "SELECT status, run_phase FROM inbound_commands WHERE command_id = ?",
            )
            .bind(steer_id)
            .fetch_one(reopened.pool())
            .await
            .expect("steer status after reapply");
            assert_eq!(steer_status.0, "applying", "steer must remain applying");
            assert_eq!(
                steer_status.1, "user_committed",
                "steer must reach user_committed"
            );

            reopened.pool().close().await;
            tokio::fs::remove_dir_all(root)
                .await
                .expect("remove steer group failpoint fixture");
        }
    }

    #[tokio::test]
    async fn restart_authenticates_existing_chain_and_never_advances_a_tampered_head() {
        fn assistant_start_batch(command_id: &str, label: &str) -> EventBatch {
            let run_id = format!("run-{command_id}");
            let turn_id = format!("turn-{command_id}");
            let message_id = format!("assistant-{label}");
            EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message_in_turn(
                            "message_start",
                            &message_id,
                            &assistant_message(StopReason::Stop),
                            Some(run_id.clone()),
                            Some(turn_id),
                        )
                        .expect("assistant append fixture"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: command_id.to_owned(),
                        run_id,
                        expected: RunPhase::UserCommitted,
                        next: RunPhase::AssistantStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            }
        }

        let valid_store = test_store().await;
        let valid_writer = EventWriter::new(valid_store.clone());
        let valid_id = "00000000-0000-4000-8000-000000000094";
        let valid_injection =
            classified_injection(&valid_writer, 1, valid_id, "valid", "chain-valid").await;
        valid_writer
            .apply(EventBatch {
                writes: injection_writes(valid_id, "valid", "chain-valid"),
                injected_commands: vec![valid_injection],
            })
            .await
            .expect("seed valid event chain");
        valid_writer
            .apply(assistant_start_batch(valid_id, "valid"))
            .await
            .expect("valid authenticated history accepts append");
        assert_eq!(
            sqlx::query_as::<_, (i64, i64)>("SELECT last_seq,event_count FROM event_log_heads")
                .fetch_one(valid_store.pool())
                .await
                .expect("valid advanced head"),
            (5, 5)
        );

        for tamper in [
            "envelope",
            "internal_metadata",
            "ciphertext",
            "deletion",
            "reorder",
        ] {
            let store = test_store().await;
            let writer = EventWriter::new(store.clone());
            let command_id = "00000000-0000-4000-8000-000000000095";
            let injected =
                classified_injection(&writer, 1, command_id, "tamper", "chain-tamper").await;
            writer
                .apply(EventBatch {
                    writes: injection_writes(command_id, "tamper", "chain-tamper"),
                    injected_commands: vec![injected],
                })
                .await
                .expect("seed chain to tamper");
            let head_before: (i64, i64, Vec<u8>, String, Vec<u8>) = sqlx::query_as(
                "SELECT last_seq,event_count,chain_digest,key_ref,head_hmac FROM event_log_heads",
            )
            .fetch_one(store.pool())
            .await
            .expect("head before tamper");
            match tamper {
                "envelope" => {
                    sqlx::query("UPDATE agent_events SET envelope='{}' WHERE seq=1")
                        .execute(store.pool())
                        .await
                        .expect("tamper envelope");
                }
                "internal_metadata" => {
                    sqlx::query("UPDATE agent_events SET internal_metadata='{}' WHERE seq=1")
                        .execute(store.pool())
                        .await
                        .expect("tamper internal metadata");
                }
                "ciphertext" => {
                    sqlx::query("UPDATE agent_events SET raw_ciphertext=zeroblob(1) WHERE seq=1")
                        .execute(store.pool())
                        .await
                        .expect("tamper ciphertext");
                }
                "deletion" => {
                    sqlx::query("DELETE FROM agent_events WHERE seq=2")
                        .execute(store.pool())
                        .await
                        .expect("delete event");
                }
                "reorder" => {
                    let mut transaction = store.pool().begin().await.expect("reorder transaction");
                    sqlx::query("UPDATE agent_events SET seq=1000 WHERE seq=1")
                        .execute(&mut *transaction)
                        .await
                        .expect("move first event aside");
                    sqlx::query("UPDATE agent_events SET seq=1 WHERE seq=2")
                        .execute(&mut *transaction)
                        .await
                        .expect("move second event first");
                    sqlx::query("UPDATE agent_events SET seq=2 WHERE seq=1000")
                        .execute(&mut *transaction)
                        .await
                        .expect("move first event second");
                    transaction.commit().await.expect("commit reorder");
                }
                _ => unreachable!(),
            }
            let rows_after_tamper: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
                .fetch_one(store.pool())
                .await
                .expect("tampered row count");
            writer
                .reset_checkpoint_after_direct_fixture_mutation()
                .await;
            drop(writer);
            let restarted_writer = EventWriter::new(store.clone());
            let error = restarted_writer
                .initialize_recovery_checkpoint()
                .await
                .expect_err("tampered history must reject startup recovery");
            assert!(
                !format!("{error:#}").is_empty(),
                "{tamper} must explain startup failure"
            );
            assert_eq!(
                sqlx::query_as::<_, (i64, i64, Vec<u8>, String, Vec<u8>)>(
                    "SELECT last_seq,event_count,chain_digest,key_ref,head_hmac FROM event_log_heads"
                )
                .fetch_one(store.pool())
                .await
                .expect("head after rejected append"),
                head_before,
                "{tamper} startup failure must not advance or rewrite the stored head"
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                    .fetch_one(store.pool())
                    .await
                    .expect("row count after rejected append"),
                rows_after_tamper,
                "{tamper} startup failure must not append an event"
            );
        }
    }

    #[tokio::test]
    async fn long_history_next_write_validates_only_checkpoint_and_new_suffix() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_tool_owner(&store, &writer, "run-paged-history").await;
        let mut writes = Vec::new();
        for attempt in 1..=22 {
            let message_id = format!("paged-assistant-{attempt}");
            let message = assistant_message(StopReason::Error);
            writes.push(EventWrite {
                event: Some(
                    DurableEvent::message_in_turn(
                        "message_start",
                        &message_id,
                        &message,
                        Some("run-paged-history".to_owned()),
                        Some("turn-1".to_owned()),
                    )
                    .expect("paged MessageStart"),
                ),
                projections: Vec::new(),
            });
            writes.push(EventWrite {
                event: Some(
                    DurableEvent::message_in_turn(
                        "message_end",
                        &message_id,
                        &message,
                        Some("run-paged-history".to_owned()),
                        Some("turn-1".to_owned()),
                    )
                    .expect("paged MessageEnd"),
                ),
                projections: vec![Projection::MessageEnd {
                    message_id,
                    role: "assistant",
                    message,
                    append_to_l0: false,
                    provider_context: Vec::new(),
                    eviction_footprint_tokens: 0,
                }],
            });
            writes.push(EventWrite {
                event: Some(
                    DurableEvent::retry_scheduled(
                        "run-paged-history".to_owned(),
                        "turn-1".to_owned(),
                        attempt,
                        0,
                        durable_test_timestamp(),
                        format!("paged retry {attempt}"),
                    )
                    .expect("paged RetryScheduled"),
                ),
                projections: Vec::new(),
            });
        }
        writer
            .apply(EventBatch {
                writes,
                injected_commands: Vec::new(),
            })
            .await
            .expect("seed more than one lifecycle validation page");
        assert!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(store.pool())
                .await
                .expect("count paged history")
                > EVENT_CHAIN_VERIFICATION_PAGE_ROWS
        );
        let visited_before = writer.historical_rows_visited().await;
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message_in_turn(
                            "message_start",
                            "paged-assistant-next",
                            &assistant_message(StopReason::Error),
                            Some("run-paged-history".to_owned()),
                            Some("turn-1".to_owned()),
                        )
                        .expect("next suffix MessageStart"),
                    ),
                    projections: Vec::new(),
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("next lifecycle event validates only the checkpoint-bound suffix");
        assert_eq!(
            writer.historical_rows_visited().await,
            visited_before,
            "ordinary writes after a long history must not reload historical event rows"
        );
    }

    #[tokio::test]
    async fn closed_turn_checkpoint_state_stays_bounded_and_unique_index_survives_restart() {
        let root = std::env::temp_dir().join(format!(
            "sumi-bounded-turn-checkpoint-{}",
            uuid::Uuid::now_v7()
        ));
        let path = root.join("agent.db");
        let store: Arc<Store> = Store::open(&path, scope(), test_provider())
            .await
            .expect("open bounded checkpoint store")
            .into();
        let writer = EventWriter::new(store.clone());
        let command_id = "00000000-0000-4000-8000-000000000090";
        let injected = classified_injection(&writer, 1, command_id, "ignored", "start").await;
        writer
            .apply(EventBatch {
                writes: injection_writes(command_id, "ignored", "start"),
                injected_commands: vec![injected],
            })
            .await
            .expect("open long-lived run");

        let run_id = format!("run-{command_id}");
        let mut current_turn = format!("turn-{command_id}");
        for turn_index in 0..128 {
            let assistant = assistant_message(StopReason::Stop);
            let message_id = format!("bounded-assistant-{turn_index}");
            let mut start_projections = Vec::new();
            if turn_index == 0 {
                start_projections.push(Projection::RunPhase {
                    command_id: command_id.to_owned(),
                    run_id: run_id.clone(),
                    expected: RunPhase::UserCommitted,
                    next: RunPhase::AssistantStarted,
                });
            }
            let mut writes = vec![
                EventWrite {
                    event: Some(
                        DurableEvent::message_in_turn(
                            "message_start",
                            &message_id,
                            &assistant,
                            Some(run_id.clone()),
                            Some(current_turn.clone()),
                        )
                        .expect("bounded assistant MessageStart"),
                    ),
                    projections: start_projections,
                },
                EventWrite {
                    event: Some(
                        DurableEvent::message_in_turn(
                            "message_end",
                            &message_id,
                            &assistant,
                            Some(run_id.clone()),
                            Some(current_turn.clone()),
                        )
                        .expect("bounded assistant MessageEnd"),
                    ),
                    projections: vec![Projection::MessageEnd {
                        message_id,
                        role: "assistant",
                        message: assistant.clone(),
                        append_to_l0: true,
                        provider_context: Vec::new(),
                        eviction_footprint_tokens: 0,
                    }],
                },
                EventWrite {
                    event: Some(
                        DurableEvent::turn_end(&run_id, &current_turn, assistant, Vec::new())
                            .expect("bounded TurnEnd"),
                    ),
                    projections: Vec::new(),
                },
            ];
            if turn_index < 127 {
                let next_turn = format!("bounded-turn-{}", turn_index + 1);
                writes.push(EventWrite {
                    event: Some(
                        DurableEvent::turn_start(&run_id, &next_turn)
                            .expect("bounded continuation TurnStart"),
                    ),
                    projections: Vec::new(),
                });
                current_turn = next_turn;
            }
            writer
                .apply(EventBatch {
                    writes,
                    injected_commands: Vec::new(),
                })
                .await
                .expect("advance bounded long-lived run");
            assert!(
                writer.retained_turn_start_identities().await <= 1,
                "checkpoint must retain at most the currently open turn identity"
            );
        }
        assert_eq!(writer.retained_turn_start_identities().await, 0);

        store.pool().close().await;
        drop(writer);
        drop(store);

        let reopened: Arc<Store> = Store::open(&path, scope(), test_provider())
            .await
            .expect("reopen bounded checkpoint store")
            .into();
        let restarted_writer = EventWriter::new(reopened.clone());
        restarted_writer
            .initialize_recovery_checkpoint()
            .await
            .expect("reconstruct bounded checkpoint");
        assert_eq!(restarted_writer.retained_turn_start_identities().await, 0);
        let events_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(reopened.pool())
            .await
            .expect("count events before duplicate");
        let duplicate = restarted_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::turn_start(&run_id, "bounded-turn-64")
                            .expect("duplicate historical TurnStart"),
                    ),
                    projections: Vec::new(),
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("durable lifecycle identity index must reject historical duplicate");
        assert!(
            format!("{duplicate:#}").contains("UNIQUE constraint failed"),
            "{duplicate:#}"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(reopened.pool())
                .await
                .expect("count events after duplicate"),
            events_before,
            "failed duplicate append must leave history unchanged"
        );
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
                "run_id":"run-1",
                "turn_id":"turn-1",
                "attempt":1,
                "delay_ms":100,
                "retry_at":"2026-07-20T00:00:00Z",
                "error_message":"retry"
            }),
        ];
        for value in canonical {
            let event = DurableEvent::new(&value)
                .unwrap_or_else(|error| panic!("canonical event {value} failed: {error:#}"));
            let mut expected_public_event = value.clone();
            if expected_public_event.get("type") == Some(&Value::String("retry_scheduled".into())) {
                expected_public_event
                    .as_object_mut()
                    .expect("event object")
                    .remove("run_id");
                expected_public_event
                    .as_object_mut()
                    .expect("event object")
                    .remove("turn_id");
            }
            assert_eq!(
                serde_json::from_slice::<Value>(&event.raw_json).expect("canonical raw JSON"),
                expected_public_event,
                "durable raw event must contain only canonical public AgentEvent fields"
            );
            let recovered = DurableEvent::from_raw(event.raw_json.clone())
                .expect("canonical event survives encrypted recovery decode");
            assert_eq!(recovered.raw_json, event.raw_json);
        }
    }

    #[test]
    fn duplicate_lifecycle_identities_cannot_reuse_one_transition_or_terminal_partner() {
        let cases = [
            (
                "AgentStart",
                DurableEvent::agent_start("run-duplicate").expect("AgentStart"),
            ),
            (
                "TurnStart",
                DurableEvent::turn_start("run-duplicate", "turn-duplicate").expect("TurnStart"),
            ),
            (
                "Steered",
                DurableEvent::steered(
                    SteerMode::Soft,
                    "00000000-0000-4000-8000-000000000071".to_owned(),
                    "run-duplicate".to_owned(),
                    "turn-duplicate".to_owned(),
                )
                .expect("Steered"),
            ),
            (
                "TurnEnd",
                DurableEvent::turn_end(
                    "run-duplicate",
                    "turn-duplicate",
                    user_message("done"),
                    Vec::new(),
                )
                .expect("TurnEnd"),
            ),
            (
                "AgentEnd",
                DurableEvent::agent_end("run-duplicate").expect("AgentEnd"),
            ),
        ];

        for (kind, event) in cases {
            let error = validate_batch_shape(
                &Redactor::v1(),
                &EventBatch {
                    writes: vec![
                        EventWrite {
                            event: Some(event.clone()),
                            projections: Vec::new(),
                        },
                        EventWrite {
                            event: Some(event),
                            projections: Vec::new(),
                        },
                    ],
                    injected_commands: Vec::new(),
                },
            )
            .err()
            .expect("duplicate lifecycle identity must fail before persistence");
            assert!(
                error.to_string().contains(&format!("duplicate {kind}")),
                "{kind}: {error:#}"
            );
        }
    }

    #[test]
    fn distinct_lifecycle_identities_and_multi_command_steer_group_are_not_duplicates() {
        let command_a = "00000000-0000-4000-8000-000000000072";
        let command_b = "00000000-0000-4000-8000-000000000073";
        let batch = EventBatch {
            writes: vec![
                EventWrite {
                    event: Some(
                        DurableEvent::steered(
                            SteerMode::Soft,
                            command_a.to_owned(),
                            "run-group".to_owned(),
                            "turn-group".to_owned(),
                        )
                        .expect("first Steered"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: command_a.to_owned(),
                        run_id: "run-group".to_owned(),
                        expected: RunPhase::Classified,
                        next: RunPhase::TurnStarted,
                    }],
                },
                EventWrite {
                    event: Some(
                        DurableEvent::steered(
                            SteerMode::Soft,
                            command_b.to_owned(),
                            "run-group".to_owned(),
                            "turn-group".to_owned(),
                        )
                        .expect("second Steered"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: command_b.to_owned(),
                        run_id: "run-group".to_owned(),
                        expected: RunPhase::Classified,
                        next: RunPhase::TurnStarted,
                    }],
                },
                EventWrite {
                    event: Some(
                        DurableEvent::turn_start("run-group", "turn-group").expect("TurnStart"),
                    ),
                    projections: Vec::new(),
                },
            ],
            injected_commands: Vec::new(),
        };

        validate_batch_shape(&Redactor::v1(), &batch)
            .expect("distinct group lifecycle identities remain valid");
    }

    #[test]
    fn same_batch_causal_orders_reject_reversed_events() {
        let assistant = assistant_message(StopReason::Stop);
        let reversed_message = validate_batch_shape(
            &Redactor::v1(),
            &EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_end", "assistant-1", &assistant)
                                .expect("MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: "assistant-1".to_owned(),
                            role: "assistant",
                            message: assistant.clone(),
                            append_to_l0: true,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_start", "assistant-1", &assistant)
                                .expect("MessageStart"),
                        ),
                        projections: Vec::new(),
                    },
                ],
                injected_commands: Vec::new(),
            },
        )
        .err()
        .expect("MessageEnd cannot precede same-batch MessageStart");
        assert!(reversed_message.to_string().contains("must precede"));

        let request = approval_request("request-order", "tool-order", "mutating");
        let reversed_approval = validate_batch_shape(
            &Redactor::v1(),
            &EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type":"approval_resolved",
                                "request_id":"request-order",
                                "resolution":"cancelled",
                                "actor":"system"
                            }))
                            .expect("ApprovalResolved"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type":"approval_requested",
                                "request":request
                            }))
                            .expect("ApprovalRequested"),
                        ),
                        projections: Vec::new(),
                    },
                ],
                injected_commands: Vec::new(),
            },
        )
        .err()
        .expect("ApprovalResolved cannot precede same-batch ApprovalRequested");
        assert!(reversed_approval.to_string().contains("must precede"));

        let reversed_steer = validate_batch_shape(
            &Redactor::v1(),
            &EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::turn_start("run-order", "turn-order").expect("TurnStart"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::steered(
                                SteerMode::Soft,
                                "00000000-0000-4000-8000-000000000080".to_owned(),
                                "run-order".to_owned(),
                                "turn-order".to_owned(),
                            )
                            .expect("Steered"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: "00000000-0000-4000-8000-000000000080".to_owned(),
                            run_id: "run-order".to_owned(),
                            expected: RunPhase::Classified,
                            next: RunPhase::TurnStarted,
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            },
        )
        .err()
        .expect("Steered cannot follow its same-batch TurnStart");
        assert!(reversed_steer.to_string().contains("must precede"));

        let reversed_run_close = validate_batch_shape(
            &Redactor::v1(),
            &EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(DurableEvent::agent_end("run-close").expect("AgentEnd")),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::turn_end(
                                "run-close",
                                "turn-close",
                                assistant,
                                Vec::new(),
                            )
                            .expect("TurnEnd"),
                        ),
                        projections: Vec::new(),
                    },
                ],
                injected_commands: Vec::new(),
            },
        )
        .err()
        .expect("AgentEnd cannot precede same-batch TurnEnd");
        assert!(reversed_run_close.to_string().contains("must precede"));
    }

    #[test]
    fn terminal_tool_results_require_bidirectional_pairing_and_order() {
        let mut reversed =
            tool_finish_writes("tool-order", "running", "succeeded", None, "done", false);
        let terminal = reversed.remove(0);
        reversed.push(terminal);
        let order_error = validate_batch_shape(
            &Redactor::v1(),
            &EventBatch {
                writes: reversed,
                injected_commands: Vec::new(),
            },
        )
        .err()
        .expect("ToolExecutionEnd must precede result messages");
        assert!(order_error.to_string().contains("ToolExecutionEnd"));

        let result = tool_result("tool-orphan", "fabricated", true);
        let orphan = validate_batch_shape(
            &Redactor::v1(),
            &EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_start", "orphan-result", &result)
                                .expect("MessageStart"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_end", "orphan-result", &result)
                                .expect("MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: "orphan-result".to_owned(),
                            role: "tool_result",
                            message: result,
                            append_to_l0: true,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            },
        )
        .err()
        .expect("fabricated tool result must not persist without executor terminal");
        assert!(
            orphan
                .to_string()
                .contains("requires same-batch ToolExecutionEnd")
        );
    }

    #[test]
    fn turn_end_tool_results_exactly_match_the_current_assistant_tool_calls() {
        fn lifecycle_state(
            assistant: &PublicMessage,
            results: &[PublicMessage],
        ) -> DurableLifecycleState {
            let mut state = DurableLifecycleState::default();
            let run = DurableEventMetadata {
                run_id: Some("run-exact-tools".to_owned()),
                ..DurableEventMetadata::default()
            };
            let turn = DurableEventMetadata {
                run_id: Some("run-exact-tools".to_owned()),
                turn_id: Some("turn-exact-tools".to_owned()),
                ..DurableEventMetadata::default()
            };
            apply_lifecycle_event(
                &mut state,
                "agent_start",
                &run,
                &json!({"type":"agent_start"}),
                false,
            )
            .expect("AgentStart");
            apply_lifecycle_event(
                &mut state,
                "turn_start",
                &turn,
                &json!({"type":"turn_start"}),
                false,
            )
            .expect("TurnStart");
            for (kind, message_id) in [
                ("message_start", "assistant-exact-tools"),
                ("message_end", "assistant-exact-tools"),
            ] {
                apply_lifecycle_event(
                    &mut state,
                    kind,
                    &turn,
                    &json!({"message_id":message_id,"message":assistant}),
                    false,
                )
                .expect("assistant lifecycle");
            }
            for (index, result) in results.iter().enumerate() {
                apply_lifecycle_event(
                    &mut state,
                    "message_end",
                    &DurableEventMetadata::default(),
                    &json!({"message_id":format!("tool-result-{index}"),"message":result}),
                    false,
                )
                .expect("tool-result MessageEnd");
            }
            state
        }

        let assistant = assistant_tool_message(&["tool-current-a", "tool-current-b"]);
        let current_a = tool_result("tool-current-a", "a", false);
        let current_b = tool_result("tool-current-b", "b", false);
        let prior = tool_result("tool-prior", "prior", false);
        let metadata = DurableEventMetadata {
            run_id: Some("run-exact-tools".to_owned()),
            turn_id: Some("turn-exact-tools".to_owned()),
            ..DurableEventMetadata::default()
        };

        let mut valid = lifecycle_state(
            &assistant,
            &[prior.clone(), current_a.clone(), current_b.clone()],
        );
        apply_lifecycle_event(
            &mut valid,
            "turn_end",
            &metadata,
            &json!({"message":assistant,"tool_results":[current_a.clone(),current_b.clone()]}),
            true,
        )
        .expect("multi-tool current turn closes with its exact result set");

        for (label, supplied) in [
            (
                "prior extra",
                vec![current_a.clone(), current_b.clone(), prior],
            ),
            ("omission", vec![current_a.clone()]),
            ("duplicate", vec![current_a.clone(), current_a.clone()]),
            (
                "wrong identity",
                vec![current_a.clone(), tool_result("tool-wrong", "b", false)],
            ),
            (
                "wrong value",
                vec![
                    tool_result("tool-current-a", "wrong", false),
                    current_b.clone(),
                ],
            ),
        ] {
            let mut state = lifecycle_state(
                &assistant,
                &[
                    tool_result("tool-prior", "prior", false),
                    current_a.clone(),
                    current_b.clone(),
                ],
            );
            let error = apply_lifecycle_event(
                &mut state,
                "turn_end",
                &metadata,
                &json!({"message":assistant,"tool_results":supplied}),
                true,
            )
            .expect_err(label);
            assert!(
                error.to_string().contains("exactly match")
                    || error.to_string().contains("duplicate tool result")
                    || error.to_string().contains("does not match"),
                "{label}: {error:#}"
            );
        }

        let no_tools = assistant_message(StopReason::Stop);
        let mut no_tool_state = lifecycle_state(&no_tools, &[]);
        apply_lifecycle_event(
            &mut no_tool_state,
            "turn_end",
            &metadata,
            &json!({"message":no_tools,"tool_results":[]}),
            true,
        )
        .expect("no-tool turn requires and accepts an empty result set");

        let rejected_only = PublicMessage::Assistant(PublicAssistantMessage {
            content: vec![PublicAssistantContent::RejectedToolCall {
                rejected: RejectedToolCall {
                    id: "rejected-not-executable".to_owned(),
                    name: "test".to_owned(),
                    error: ToolArgumentError::InvalidJson,
                },
                wire_item_index: 0,
            }],
            model: "test-model".to_owned(),
            provider: "test-provider".to_owned(),
            origin: test_provider_origin(),
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: durable_test_timestamp(),
        });
        let mut rejected_state = lifecycle_state(&rejected_only, &[]);
        apply_lifecycle_event(
            &mut rejected_state,
            "turn_end",
            &metadata,
            &json!({"message":rejected_only,"tool_results":[]}),
            true,
        )
        .expect("rejected ToolCall is not an executable ToolCall result obligation");
    }

    #[test]
    fn non_assistant_message_starts_require_their_exact_same_batch_terminal_message() {
        let user = user_message("orphan");
        let orphan_user = validate_batch_shape(
            &Redactor::v1(),
            &EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message("message_start", "orphan-user", &user)
                            .expect("user MessageStart"),
                    ),
                    projections: Vec::new(),
                }],
                injected_commands: Vec::new(),
            },
        )
        .err()
        .expect("standalone user MessageStart must fail");
        assert!(orphan_user.to_string().contains("canonical MessageEnd"));

        let start = tool_result("tool-exact", "start", false);
        let end = tool_result("tool-exact", "different terminal result", false);
        let mismatched_tool = validate_batch_shape(
            &Redactor::v1(),
            &EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_start", "tool-exact-result", &start)
                                .expect("tool-result MessageStart"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_end", "tool-exact-result", &end)
                                .expect("tool-result MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: "tool-exact-result".to_owned(),
                            role: "tool_result",
                            message: end,
                            append_to_l0: true,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            },
        )
        .err()
        .expect("tool-result start and terminal payload must be identical");
        assert!(mismatched_tool.to_string().contains("canonical MessageEnd"));

        validate_batch_shape(
            &Redactor::v1(),
            &EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message(
                            "message_start",
                            "assistant-streaming",
                            &assistant_message(StopReason::Stop),
                        )
                        .expect("assistant MessageStart"),
                    ),
                    projections: Vec::new(),
                }],
                injected_commands: Vec::new(),
            },
        )
        .expect("assistant streaming MessageStart may remain open across batches");
    }

    #[test]
    fn retry_scheduled_requires_identity_but_allows_immediate_recovery() {
        DurableEvent::retry_scheduled(
            "run-retry",
            "turn-retry",
            1,
            0,
            durable_test_timestamp(),
            "context overflow",
        )
        .expect("delay_ms=0 is canonical for immediate overflow recovery");
        for (run_id, turn_id, attempt, error_message) in [
            ("", "turn-retry", 1, "retry"),
            ("run-retry", "", 1, "retry"),
            ("run-retry", "turn-retry", 0, "retry"),
            ("run-retry", "turn-retry", 1, ""),
        ] {
            assert!(
                DurableEvent::retry_scheduled(
                    run_id,
                    turn_id,
                    attempt,
                    0,
                    durable_test_timestamp(),
                    error_message,
                )
                .is_err()
            );
        }
    }

    #[tokio::test]
    async fn durable_history_rejects_duplicate_starts_and_accepts_recovery_retry_suffix() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let command_id = "00000000-0000-4000-8000-000000000084";
        let injected = classified_injection(&writer, 1, command_id, "ignored", "retry").await;
        writer
            .apply(EventBatch {
                writes: injection_writes(command_id, "ignored", "retry"),
                injected_commands: vec![injected],
            })
            .await
            .expect("open retry turn");
        let run_id = format!("run-{command_id}");
        let turn_id = format!("turn-{command_id}");
        let error_message = assistant_message(StopReason::Error);
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message_in_turn(
                            "message_start",
                            "assistant-retry-1",
                            &error_message,
                            Some(run_id.clone()),
                            Some(turn_id.clone()),
                        )
                        .expect("retry MessageStart"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: command_id.to_owned(),
                        run_id: run_id.clone(),
                        expected: RunPhase::UserCommitted,
                        next: RunPhase::AssistantStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist assistant attempt start");
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message_in_turn(
                            "message_end",
                            "assistant-retry-1",
                            &error_message,
                            Some(run_id.clone()),
                            Some(turn_id.clone()),
                        )
                        .expect("retry MessageEnd"),
                    ),
                    projections: vec![Projection::MessageEnd {
                        message_id: "assistant-retry-1".to_owned(),
                        role: "assistant",
                        message: error_message,
                        append_to_l0: false,
                        provider_context: Vec::new(),
                        eviction_footprint_tokens: 0,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist retryable error before crash");
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::retry_scheduled(
                            run_id.clone(),
                            turn_id.clone(),
                            1,
                            0,
                            durable_test_timestamp(),
                            "context overflow",
                        )
                        .expect("recovery RetryScheduled"),
                    ),
                    projections: Vec::new(),
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("recovery may append the missing delay-zero schedule later");

        let duplicate_retry = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::retry_scheduled(
                            run_id.clone(),
                            turn_id.clone(),
                            1,
                            0,
                            durable_test_timestamp(),
                            "duplicate",
                        )
                        .expect("duplicate schedule fixture"),
                    ),
                    projections: Vec::new(),
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("retry attempt cannot be reused across transactions");
        assert!(duplicate_retry.to_string().contains("not monotonic"));

        let second_error = assistant_message(StopReason::Error);
        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_start",
                                "assistant-retry-2",
                                &second_error,
                                Some(run_id.clone()),
                                Some(turn_id.clone()),
                            )
                            .expect("second retry MessageStart"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_end",
                                "assistant-retry-2",
                                &second_error,
                                Some(run_id.clone()),
                                Some(turn_id.clone()),
                            )
                            .expect("second retry MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: "assistant-retry-2".to_owned(),
                            role: "assistant",
                            message: second_error,
                            append_to_l0: false,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::retry_scheduled(
                                run_id.clone(),
                                turn_id.clone(),
                                2,
                                0,
                                durable_test_timestamp(),
                                "second context overflow",
                            )
                            .expect("second RetryScheduled"),
                        ),
                        projections: Vec::new(),
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("a new errored assistant attempt permits exactly one next schedule");

        for event in [
            DurableEvent::agent_start(run_id.clone()).expect("duplicate AgentStart fixture"),
            DurableEvent::turn_start(run_id.clone(), turn_id.clone())
                .expect("duplicate TurnStart fixture"),
            DurableEvent::message_in_turn(
                "message_start",
                "assistant-retry-1",
                &assistant_message(StopReason::Stop),
                Some(run_id.clone()),
                Some(turn_id.clone()),
            )
            .expect("duplicate MessageStart fixture"),
        ] {
            writer
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: Some(event),
                        projections: Vec::new(),
                    }],
                    injected_commands: Vec::new(),
                })
                .await
                .expect_err("cross-transaction lifecycle start must be unique");
        }
    }

    #[tokio::test]
    async fn assistant_lifecycle_rejects_missing_or_wrong_turn_binding() {
        let store = test_store().await;
        let writer = EventWriter::new(store);
        let assistant = assistant_message(StopReason::Stop);
        let missing = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message("message_start", "assistant-unbound", &assistant)
                            .expect("unbound assistant fixture"),
                    ),
                    projections: Vec::new(),
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("assistant lifecycle metadata is mandatory");
        assert!(missing.to_string().contains("internal run_id"));
    }

    #[test]
    fn injected_commands_enforce_steered_and_user_before_assistant_order() {
        let command_a =
            CommandId::parse("00000000-0000-4000-8000-000000000081").expect("command A");
        let command_b =
            CommandId::parse("00000000-0000-4000-8000-000000000082").expect("command B");
        let reversed_steers = validate_batch_shape(
            &Redactor::v1(),
            &EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::steered(
                                SteerMode::Soft,
                                command_b.to_string(),
                                "run-order".to_owned(),
                                "turn-order".to_owned(),
                            )
                            .expect("Steered B"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: command_b.to_string(),
                            run_id: "run-order".to_owned(),
                            expected: RunPhase::Classified,
                            next: RunPhase::TurnStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::steered(
                                SteerMode::Soft,
                                command_a.to_string(),
                                "run-order".to_owned(),
                                "turn-order".to_owned(),
                            )
                            .expect("Steered A"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: command_a.to_string(),
                            run_id: "run-order".to_owned(),
                            expected: RunPhase::Classified,
                            next: RunPhase::TurnStarted,
                        }],
                    },
                ],
                injected_commands: vec![
                    InjectedCommand::new(1, command_a.clone()),
                    InjectedCommand::new(2, command_b),
                ],
            },
        )
        .err()
        .expect("Steered entries cannot reverse command sequence order");
        assert!(
            reversed_steers
                .to_string()
                .contains("durable sequence order")
        );

        let command_id = command_a.to_string();
        let message_id = user_message_id(command_id.as_str());
        let user = user_message("injected");
        let assistant = assistant_message(StopReason::Stop);
        let early_assistant = validate_batch_shape(
            &Redactor::v1(),
            &EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_start", &message_id, &user)
                                .expect("user MessageStart"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_start", "assistant-early", &assistant)
                                .expect("assistant MessageStart"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_end", &message_id, &user)
                                .expect("user MessageEnd"),
                        ),
                        projections: vec![
                            Projection::MessageEnd {
                                message_id,
                                role: "user",
                                message: user,
                                append_to_l0: true,
                                provider_context: Vec::new(),
                                eviction_footprint_tokens: 0,
                            },
                            Projection::RunPhase {
                                command_id: command_id.clone(),
                                run_id: "run-order".to_owned(),
                                expected: RunPhase::UserStarted,
                                next: RunPhase::UserCommitted,
                            },
                        ],
                    },
                ],
                injected_commands: vec![InjectedCommand::new(1, command_a)],
            },
        )
        .err()
        .expect("assistant MessageStart cannot precede injected user MessageEnd");
        assert!(
            early_assistant
                .to_string()
                .contains("assistant MessageStart")
        );
    }

    #[tokio::test]
    async fn non_empty_turn_end_requires_exact_open_turn_and_live_run_owner() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let command_id = "00000000-0000-4000-8000-000000000083";
        let injected = classified_injection(&writer, 1, command_id, "ignored", "hello").await;
        writer
            .apply(EventBatch {
                writes: injection_writes(command_id, "ignored", "hello"),
                injected_commands: vec![injected],
            })
            .await
            .expect("open initial durable turn and owner");
        let run_id = format!("run-{command_id}");
        let original_turn_id = format!("turn-{command_id}");
        let initial_assistant = assistant_message(StopReason::Stop);
        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_start",
                                "assistant-initial",
                                &initial_assistant,
                                Some(run_id.clone()),
                                Some(original_turn_id.clone()),
                            )
                            .expect("initial assistant MessageStart"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: command_id.to_owned(),
                            run_id: run_id.clone(),
                            expected: RunPhase::UserCommitted,
                            next: RunPhase::AssistantStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_end",
                                "assistant-initial",
                                &initial_assistant,
                                Some(run_id.clone()),
                                Some(original_turn_id.clone()),
                            )
                            .expect("initial assistant MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: "assistant-initial".to_owned(),
                            role: "assistant",
                            message: initial_assistant.clone(),
                            append_to_l0: true,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::turn_end(
                                &run_id,
                                &original_turn_id,
                                initial_assistant,
                                Vec::new(),
                            )
                            .expect("initial TurnEnd"),
                        ),
                        projections: Vec::new(),
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("close the exact initial open turn");

        let continuation_turn = "tool-continuation-turn";
        let continuation_assistant = assistant_message(StopReason::Stop);
        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::turn_start(&run_id, continuation_turn)
                                .expect("continuation TurnStart"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_start",
                                "assistant-continuation",
                                &continuation_assistant,
                                Some(run_id.clone()),
                                Some(continuation_turn.to_owned()),
                            )
                            .expect("continuation MessageStart"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_end",
                                "assistant-continuation",
                                &continuation_assistant,
                                Some(run_id.clone()),
                                Some(continuation_turn.to_owned()),
                            )
                            .expect("continuation MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: "assistant-continuation".to_owned(),
                            role: "assistant",
                            message: continuation_assistant.clone(),
                            append_to_l0: true,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::turn_end(
                                &run_id,
                                continuation_turn,
                                continuation_assistant,
                                Vec::new(),
                            )
                            .expect("continuation TurnEnd"),
                        ),
                        projections: Vec::new(),
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("owner command turn_id does not constrain later continuation turns");

        let already_ended = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::turn_end(
                            &run_id,
                            continuation_turn,
                            assistant_message(StopReason::Stop),
                            Vec::new(),
                        )
                        .expect("duplicate TurnEnd"),
                    ),
                    projections: Vec::new(),
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("an already-ended turn cannot be ended again");
        assert!(already_ended.to_string().contains("exact open TurnStart"));

        let nonexistent = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::turn_end(
                            &run_id,
                            "never-started",
                            assistant_message(StopReason::Stop),
                            Vec::new(),
                        )
                        .expect("orphan TurnEnd"),
                    ),
                    projections: Vec::new(),
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("a nonexistent turn cannot be ended");
        assert!(nonexistent.to_string().contains("exact open TurnStart"));
    }

    #[tokio::test]
    async fn lifecycle_events_consume_exact_transition_sets_instead_of_any_matching_transition() {
        for (stored_phase, event, expected, next, expected_error) in [
            (
                RunPhase::Classified,
                DurableEvent::agent_start("run-shared").expect("AgentStart"),
                RunPhase::Classified,
                RunPhase::RunStarted,
                "exactly one classified -> run_started pair",
            ),
            (
                RunPhase::RunStarted,
                DurableEvent::turn_start("run-shared", "turn-shared").expect("TurnStart"),
                RunPhase::RunStarted,
                RunPhase::TurnStarted,
                "found 2/0",
            ),
        ] {
            let store = test_store().await;
            let writer = EventWriter::new(store.clone());
            for seq in 1_u64..=2 {
                writer
                    .persist_inbound(&user_command(
                        seq,
                        &Uuid::from_u128(200 + seq as u128).to_string(),
                        "pending",
                    ))
                    .await
                    .expect("persist transition target");
            }
            sqlx::query(
                "UPDATE inbound_commands
                 SET status='applying', application_kind='idle_run', run_id='run-shared',
                     turn_id='turn-shared', run_phase=?",
            )
            .bind(stored_phase.as_str())
            .execute(store.pool())
            .await
            .expect("seed matching phase identities");
            let command_ids: Vec<String> =
                sqlx::query_scalar("SELECT command_id FROM inbound_commands ORDER BY seq")
                    .fetch_all(store.pool())
                    .await
                    .expect("read command identities");
            let error = writer
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: Some(event),
                        projections: command_ids
                            .into_iter()
                            .map(|command_id| Projection::RunPhase {
                                command_id,
                                run_id: "run-shared".to_owned(),
                                expected,
                                next,
                            })
                            .collect(),
                    }],
                    injected_commands: Vec::new(),
                })
                .await
                .expect_err("one lifecycle event cannot be shared by two transition identities");
            assert!(error.to_string().contains(expected_error), "{error:#}");
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                    .fetch_one(store.pool())
                    .await
                    .expect("count rolled-back events"),
                0
            );
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
        seed_tool_owner(&store, &writer, "run-key-redaction").await;
        let secrets = [
            "sk-abcdefghijklmnop",
            "supersecretvalue",
            "abcdefghijklmnop",
            "abcdef1234567890",
            "structured-api-value",
            "structured-access-value",
            "structured-secret-value",
            "structured-authorization-value",
            "structured-signature-value",
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
                },
                "api_key":"structured-api-value",
                "Access-Token":"structured-access-value",
                "secret":"structured-secret-value",
                "Authorization":"structured-authorization-value",
                "X-Amz-Signature":"structured-signature-value"
            }),
            is_error: true,
            timestamp: durable_test_timestamp(),
        });
        let rejected = PublicMessage::Assistant(PublicAssistantMessage {
            content: vec![PublicAssistantContent::RejectedToolCall {
                rejected: RejectedToolCall {
                    id: "tool-key-redaction".to_owned(),
                    name: "test".to_owned(),
                    error: ToolArgumentError::InvalidJson,
                },
                wire_item_index: 0,
            }],
            model: "test-model".to_owned(),
            provider: "test-provider".to_owned(),
            origin: test_provider_origin(),
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: durable_test_timestamp(),
        });
        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_start",
                                "rejected-assistant",
                                &rejected,
                                Some("run-key-redaction".to_owned()),
                                Some("turn-1".to_owned()),
                            )
                            .expect("rejected assistant MessageStart"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_end",
                                "rejected-assistant",
                                &rejected,
                                Some("run-key-redaction".to_owned()),
                                Some("turn-1".to_owned()),
                            )
                            .expect("rejected assistant MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: "rejected-assistant".to_owned(),
                            role: "assistant",
                            message: rejected,
                            append_to_l0: true,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message(
                                "message_start",
                                "message-key-redaction",
                                &message,
                            )
                            .expect("tool result MessageStart"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_end", "message-key-redaction", &message)
                                .expect("tool result MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: "message-key-redaction".to_owned(),
                            role: "tool_result",
                            message,
                            append_to_l0: true,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                ],
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
    async fn structured_approval_secrets_are_absent_from_event_and_approval_projections() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_tool_owner(&store, &writer, "run-structured-secret").await;
        let secrets = [
            "approval-api-value",
            "approval-access-value",
            "approval-secret-value",
            "approval-authorization-value",
        ];
        let request = json!({
            "id":"request-structured-secret",
            "tool_call_id":"tool-structured-secret",
            "tool_name":"bash",
            "action":{"reviewable":{
                "api_key":"approval-api-value",
                "accessToken":"approval-access-value",
                "secret":"approval-secret-value"
            }},
            "args_summary":{"Authorization":"approval-authorization-value"},
            "reason":null,
            "audit":null
        });
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(&json!({
                            "type":"approval_requested",
                            "request":request,
                        }))
                        .expect("ApprovalRequested"),
                    ),
                    projections: vec![
                        Projection::ToolExecution(ToolExecutionMutation::Prepare {
                            tool_call_id: "tool-structured-secret".to_owned(),
                            command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                            run_id: "run-structured-secret".to_owned(),
                            executor_generation: test_process_generation(1),
                            idempotency_key: "idem-structured-secret".to_owned(),
                        }),
                        Projection::Approval(ApprovalMutation::Pending {
                            request_id: "request-structured-secret".to_owned(),
                            tool_call_id: "tool-structured-secret".to_owned(),
                            run_id: "run-structured-secret".to_owned(),
                            turn_id: "turn-1".to_owned(),
                        }),
                    ],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist redacted approval");

        let dump: String = sqlx::query_scalar(
            "SELECT e.envelope || char(10) || a.request_projection
             FROM agent_events e JOIN approval_log a ON a.id='request-structured-secret'
             WHERE e.event_type='approval_requested'",
        )
        .fetch_one(store.pool())
        .await
        .expect("read approval projection dump");
        for secret in secrets {
            assert!(
                !dump.contains(secret),
                "approval projection leaked {secret}"
            );
        }
        assert!(dump.contains("[REDACTED:secret]"));
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
                        provider_context: Vec::new(),
                        eviction_footprint_tokens: 0,
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
    async fn live_admission_accepts_user_then_reserved_abort_without_reentering_replay_mode() {
        let store = test_store().await;
        let writer = EventWriter::new(store);
        let mut admission = InboundAdmission::after_t12_recovery(false);
        let user = user_command(1, "00000000-0000-4000-8000-000000000001", "start live work");
        let abort = abort_command(2, "00000000-0000-4000-8000-000000000002");

        assert_eq!(
            admission
                .receive(&writer, &user)
                .await
                .expect("receive live UserMessage")
                .status,
            CommandAckStatus::Received
        );
        assert_eq!(
            admission
                .receive(&writer, &abort)
                .await
                .expect("receive reserved live Abort")
                .status,
            CommandAckStatus::Received
        );
        let terminal = writer
            .apply_idle_abort_cutoff("00000000-0000-4000-8000-000000000002", 2)
            .await
            .expect("terminalize live cutoff");
        assert_eq!(
            terminal.iter().map(|ack| ack.status).collect::<Vec<_>>(),
            vec![CommandAckStatus::Superseded, CommandAckStatus::Applied]
        );
    }

    #[tokio::test]
    async fn event_writer_handles_share_checkpoint_across_event_and_projection_only_calls() {
        let store = test_store().await;
        let first = EventWriter::new(store.clone());
        let second = EventWriter::new(store.clone());
        first
            .initialize_recovery_checkpoint()
            .await
            .expect("initialize first handle");
        second
            .initialize_recovery_checkpoint()
            .await
            .expect("initialize second handle");

        let user_id = "00000000-0000-4000-8000-000000000091";
        let abort_id = "00000000-0000-4000-8000-000000000092";
        first
            .persist_inbound(&user_command(1, user_id, "pending startup"))
            .await
            .expect("first handle persists projection-only receipt");
        second
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: user_id.to_owned(),
                        application_kind: ApplicationKind::IdleRun,
                        run_id: "shared-run".to_owned(),
                        turn_id: "shared-turn".to_owned(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("second handle classifies projection-only startup");
        first
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::agent_start("shared-run").expect("shared handle AgentStart"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: user_id.to_owned(),
                        run_id: "shared-run".to_owned(),
                        expected: RunPhase::Classified,
                        next: RunPhase::RunStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("first handle appends lifecycle event after second handle projection");
        second
            .persist_inbound(&abort_command(2, abort_id))
            .await
            .expect("second handle preserves reserved Abort receipt after lifecycle append");

        let terminal = first
            .apply_idle_abort_cutoff(abort_id, 2)
            .await
            .expect("first handle closes shared pending startup and Abort window");
        assert_eq!(
            terminal.iter().map(|ack| ack.status).collect::<Vec<_>>(),
            vec![CommandAckStatus::Superseded, CommandAckStatus::Applied]
        );
        assert_eq!(
            second
                .ack_for_command(user_id)
                .await
                .expect("read shared user ACK")
                .expect("shared user ACK exists")
                .status,
            CommandAckStatus::Superseded
        );
        assert_eq!(
            second
                .ack_for_command(abort_id)
                .await
                .expect("read shared Abort ACK")
                .expect("shared Abort ACK exists")
                .status,
            CommandAckStatus::Applied
        );
    }

    #[tokio::test]
    async fn idle_abort_authenticates_key_purpose_hmac_and_exact_variant_before_mutation() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let abort_id = "00000000-0000-4000-8000-000000000082";
        writer
            .persist_inbound(&abort_command(1, abort_id))
            .await
            .expect("persist Abort fixture");
        let key_ref: String =
            sqlx::query_scalar("SELECT payload_key_ref FROM inbound_commands WHERE command_id=?")
                .bind(abort_id)
                .fetch_one(store.pool())
                .await
                .expect("load Abort key ref");
        let key = store
            .data_key_by_ref(&key_ref)
            .await
            .expect("load Abort key");
        let replacement = serde_json::to_vec(&Command::UserMessage {
            text: "not an abort".to_owned(),
            attachments: Vec::new(),
        })
        .expect("serialize authenticated wrong variant");
        let aad = store
            .scope()
            .row_aad("inbound_commands", "1", DataKeyPurpose::Command);
        sqlx::query(
            "UPDATE inbound_commands SET payload_ciphertext=?, payload_hmac=? WHERE command_id=?",
        )
        .bind(encrypt_content(&key, &replacement, &aad).expect("encrypt wrong variant"))
        .bind(command_payload_digest(&key, &replacement))
        .bind(abort_id)
        .execute(store.pool())
        .await
        .expect("install authenticated wrong variant");
        let wrong_variant = writer
            .apply_idle_abort_cutoff(abort_id, 1)
            .await
            .expect_err("Abort cutoff must reject another authenticated command variant");
        assert!(
            wrong_variant
                .to_string()
                .contains("different command variant")
        );
        assert_eq!(
            writer
                .ack_for_command(abort_id)
                .await
                .expect("read unchanged Abort ACK")
                .expect("Abort row remains")
                .status,
            CommandAckStatus::Received
        );

        let purpose_store = test_store().await;
        let purpose_writer = EventWriter::new(purpose_store.clone());
        let purpose_abort_id = "00000000-0000-4000-8000-000000000083";
        purpose_writer
            .persist_inbound(&abort_command(1, purpose_abort_id))
            .await
            .expect("persist key-purpose Abort fixture");
        sqlx::query(
            "UPDATE data_keys SET purpose='event'
             WHERE key_ref=(SELECT payload_key_ref FROM inbound_commands WHERE command_id=?)",
        )
        .bind(purpose_abort_id)
        .execute(purpose_store.pool())
        .await
        .expect("tamper Abort key purpose");
        let wrong_purpose = purpose_writer
            .apply_idle_abort_cutoff(purpose_abort_id, 1)
            .await
            .expect_err("Abort cutoff must reject a non-command key");
        let wrong_purpose = format!("{wrong_purpose:#}");
        assert!(
            wrong_purpose.contains("non-command data key")
                || wrong_purpose.contains("failed to unwrap data key")
                || wrong_purpose.contains("AEAD authentication failed"),
            "unexpected key-purpose error: {wrong_purpose}"
        );
    }

    #[tokio::test]
    async fn live_admission_bounds_non_abort_window_but_keeps_one_abort_slot() {
        let store = test_store().await;
        let writer = EventWriter::new(store);
        let mut admission = InboundAdmission::after_t12_recovery(false);
        let commands = (1_u64..=32)
            .map(|seq| {
                let command_id = Uuid::from_u128(seq as u128).to_string();
                user_command(seq, &command_id, "pending")
            })
            .collect::<Vec<_>>();
        for command in &commands {
            admission
                .receive(&writer, command)
                .await
                .expect("bounded live command");
        }
        assert_eq!(
            admission
                .receive(&writer, &commands[0])
                .await
                .expect("exact replay remains admissible at capacity")
                .status,
            CommandAckStatus::Received
        );

        let overflow = user_command(33, &Uuid::from_u128(33).to_string(), "overflow");
        let error = admission
            .receive(&writer, &overflow)
            .await
            .expect_err("33rd non-Abort command must backpressure");
        assert!(error.downcast_ref::<InboundBackpressure>().is_some());

        let abort_id = Uuid::from_u128(34).to_string();
        assert_eq!(
            admission
                .receive(&writer, &abort_command(33, &abort_id))
                .await
                .expect("reserved Abort remains admissible")
                .status,
            CommandAckStatus::Received
        );
        let terminal = writer
            .apply_idle_abort_cutoff(&abort_id, 33)
            .await
            .expect("bounded Abort cutoff");
        assert_eq!(terminal.len(), 33);

        assert_eq!(
            admission
                .receive(
                    &writer,
                    &user_command(34, &Uuid::from_u128(35).to_string(), "after cutoff"),
                )
                .await
                .expect("terminal ACKs release the durable window")
                .status,
            CommandAckStatus::Received
        );
    }

    #[tokio::test]
    async fn live_admission_enforces_four_mib_canonical_payload_window() {
        let store = test_store().await;
        let writer = EventWriter::new(store);
        let mut admission = InboundAdmission::after_t12_recovery(false);
        let near_wire_limit = "x".repeat(1_048_000);
        for seq in 1_u64..=4 {
            admission
                .receive(
                    &writer,
                    &user_command(
                        seq,
                        &Uuid::from_u128(100 + seq as u128).to_string(),
                        &near_wire_limit,
                    ),
                )
                .await
                .expect("four canonical payloads remain below 4 MiB");
        }

        let error = admission
            .receive(
                &writer,
                &user_command(5, &Uuid::from_u128(105).to_string(), &"y".repeat(3_000)),
            )
            .await
            .expect_err("canonical payload aggregate above 4 MiB must backpressure");
        assert!(error.downcast_ref::<InboundBackpressure>().is_some());
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
                command_payload_digest(&key, &bytes)
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
                command_payload_digest(&key, &changed_bytes)
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
        let approval =
            approval_command(2, "00000000-0000-4000-8000-000000000020", "unknown-request");
        let abort = abort_command(3, "00000000-0000-4000-8000-000000000013");
        writer
            .persist_inbound(&user)
            .await
            .expect("persist pending user");
        writer
            .persist_inbound(&approval)
            .await
            .expect("persist pending approval decision");
        writer.persist_inbound(&abort).await.expect("persist abort");

        let acks = writer
            .apply_idle_abort_cutoff("00000000-0000-4000-8000-000000000013", 3)
            .await
            .expect("apply ordered cutoff");
        assert_eq!(
            acks.iter().map(|ack| ack.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(acks[0].status, CommandAckStatus::Superseded);
        assert_eq!(acks[1].status, CommandAckStatus::Applied);
        assert_eq!(acks[2].status, CommandAckStatus::Applied);
        assert_eq!(
            writer
                .persist_inbound(&user)
                .await
                .expect("replay superseded"),
            acks[0]
        );
        assert_eq!(
            writer
                .persist_inbound(&approval)
                .await
                .expect("replay no-op approval"),
            acks[1]
        );
        assert_eq!(
            writer
                .persist_inbound(&abort)
                .await
                .expect("replay applied abort"),
            acks[2]
        );
    }

    #[tokio::test]
    async fn direct_idle_abort_rejects_an_incomplete_cutoff_and_rolls_back() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        writer
            .persist_inbound(&user_command(
                1,
                "00000000-0000-4000-8000-000000000001",
                "must be terminalized",
            ))
            .await
            .expect("persist earlier command");
        writer
            .persist_inbound(&abort_command(2, "00000000-0000-4000-8000-000000000013"))
            .await
            .expect("persist Abort");

        let error = writer
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
            .expect_err("Abort cannot skip an earlier received command");
        assert!(error.to_string().contains("cutoff"));
        assert_eq!(
            sqlx::query_as::<_, (String, String)>(
                "SELECT
                    (SELECT status FROM inbound_commands WHERE seq=1),
                    (SELECT status FROM inbound_commands WHERE seq=2)",
            )
            .fetch_one(store.pool())
            .await
            .expect("rollback states"),
            ("received".to_owned(), "received".to_owned())
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
        assert!(incomplete.to_string().contains("terminal projection"));
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
    async fn direct_active_abort_rejects_an_incomplete_cutoff_and_rolls_back() {
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
                 turn_id='turn-1', run_phase='assistant_started'
             WHERE command_id='00000000-0000-4000-8000-000000000001'",
        )
        .execute(store.pool())
        .await
        .expect("open owner fixture");
        writer
            .persist_inbound(&user_command(
                2,
                "00000000-0000-4000-8000-000000000016",
                "must be superseded",
            ))
            .await
            .expect("persist earlier pending command");
        writer
            .persist_inbound(&abort_command(3, "00000000-0000-4000-8000-000000000014"))
            .await
            .expect("persist Abort");

        let error = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![
                        Projection::RunPhase {
                            command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
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
            .expect_err("active Abort cannot skip an earlier received command");
        assert!(error.to_string().contains("cutoff"));
        assert_eq!(
            sqlx::query_as::<_, (String, String, String)>(
                "SELECT
                    (SELECT run_phase FROM inbound_commands WHERE seq=1),
                    (SELECT status FROM inbound_commands WHERE seq=2),
                    (SELECT status FROM inbound_commands WHERE seq=3)",
            )
            .fetch_one(store.pool())
            .await
            .expect("rollback states"),
            (
                "assistant_started".to_owned(),
                "received".to_owned(),
                "received".to_owned()
            )
        );
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
    async fn steered_mode_matches_every_durable_application_kind() {
        for (application_kind, expected_mode, wrong_mode, needs_turn_start) in [
            ("hard_steer", SteerMode::Hard, SteerMode::Soft, true),
            ("soft_steer", SteerMode::Soft, SteerMode::Hard, true),
        ] {
            let store = test_store().await;
            let writer = EventWriter::new(store.clone());
            seed_tool_owner(&store, &writer, "run-steer-mode").await;
            let command_id = "00000000-0000-4000-8000-000000000002";
            writer
                .persist_inbound(&user_command(2, command_id, "steer"))
                .await
                .expect("persist steer mode fixture");
            sqlx::query(
                "UPDATE inbound_commands
                 SET status='applying', application_kind=?, run_id='run-steer-mode',
                     turn_id='turn-steer-mode', run_phase='classified'
                 WHERE command_id=?",
            )
            .bind(application_kind)
            .bind(command_id)
            .execute(store.pool())
            .await
            .expect("classify steer mode fixture");

            let batch = |mode| {
                let mut writes = vec![EventWrite {
                    event: Some(
                        DurableEvent::steered(
                            mode,
                            command_id.to_owned(),
                            "run-steer-mode".to_owned(),
                            "turn-steer-mode".to_owned(),
                        )
                        .expect("typed Steered"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: command_id.to_owned(),
                        run_id: "run-steer-mode".to_owned(),
                        expected: RunPhase::Classified,
                        next: RunPhase::TurnStarted,
                    }],
                }];
                if needs_turn_start {
                    writes.push(EventWrite {
                        event: Some(
                            DurableEvent::turn_start("run-steer-mode", "turn-steer-mode")
                                .expect("typed TurnStart"),
                        ),
                        projections: Vec::new(),
                    });
                }
                EventBatch {
                    writes,
                    injected_commands: Vec::new(),
                }
            };

            let error = writer
                .apply(batch(wrong_mode))
                .await
                .expect_err("mismatched serialized steer mode must fail");
            assert!(
                error
                    .to_string()
                    .contains("does not match application kind"),
                "{application_kind}: {error:#}"
            );
            writer
                .apply(batch(expected_mode))
                .await
                .unwrap_or_else(|error| panic!("{application_kind} positive failed: {error:#}"));
        }
    }

    #[tokio::test]
    async fn retry_steer_requires_the_current_turns_latest_unconsumed_schedule_for_every_member() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let owner_id = "00000000-0000-4000-8000-000000000091";
        let injected = classified_injection(&writer, 1, owner_id, "owner", "retry-old").await;
        writer
            .apply(EventBatch {
                writes: injection_writes(owner_id, "owner", "retry-old"),
                injected_commands: vec![injected],
            })
            .await
            .expect("open historical retry turn");
        let run_id = format!("run-{owner_id}");
        let old_turn_id = format!("turn-{owner_id}");
        let old_error = assistant_message(StopReason::Error);
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message_in_turn(
                            "message_start",
                            "assistant-retry-old",
                            &old_error,
                            Some(run_id.clone()),
                            Some(old_turn_id.clone()),
                        )
                        .expect("old assistant start"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: owner_id.to_owned(),
                        run_id: run_id.clone(),
                        expected: RunPhase::UserCommitted,
                        next: RunPhase::AssistantStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("start old attempt");
        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_end",
                                "assistant-retry-old",
                                &old_error,
                                Some(run_id.clone()),
                                Some(old_turn_id.clone()),
                            )
                            .expect("old assistant end"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: "assistant-retry-old".to_owned(),
                            role: "assistant",
                            message: old_error.clone(),
                            append_to_l0: false,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::retry_scheduled(
                                run_id.clone(),
                                old_turn_id.clone(),
                                1,
                                0,
                                durable_test_timestamp(),
                                "old retry",
                            )
                            .expect("old retry schedule"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::turn_end(&run_id, &old_turn_id, old_error, Vec::new())
                                .expect("close old retry turn"),
                        ),
                        projections: Vec::new(),
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("retain the historical schedule while closing its turn");

        let current_turn_id = "retry-current";
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::turn_start(&run_id, current_turn_id)
                            .expect("current TurnStart"),
                    ),
                    projections: Vec::new(),
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("open current continuation turn");
        let current_error = assistant_message(StopReason::Error);
        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_start",
                                "assistant-retry-current",
                                &current_error,
                                Some(run_id.clone()),
                                Some(current_turn_id.to_owned()),
                            )
                            .expect("current assistant start"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_end",
                                "assistant-retry-current",
                                &current_error,
                                Some(run_id.clone()),
                                Some(current_turn_id.to_owned()),
                            )
                            .expect("current assistant end"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: "assistant-retry-current".to_owned(),
                            role: "assistant",
                            message: current_error,
                            append_to_l0: false,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::retry_scheduled(
                                run_id.clone(),
                                current_turn_id.to_owned(),
                                1,
                                0,
                                durable_test_timestamp(),
                                "current retry",
                            )
                            .expect("current retry schedule"),
                        ),
                        projections: Vec::new(),
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist current retry wait");

        let first = "00000000-0000-4000-8000-000000000092";
        let second = "00000000-0000-4000-8000-000000000093";
        writer
            .persist_inbound(&user_command(2, first, "first"))
            .await
            .expect("persist first retry steer");
        writer
            .persist_inbound(&user_command(3, second, "second"))
            .await
            .expect("persist second retry steer");
        let classify = |command_id: &str, turn_id: &str| Projection::CommandClassified {
            command_id: command_id.to_owned(),
            application_kind: ApplicationKind::RetrySteer,
            run_id: run_id.clone(),
            turn_id: turn_id.to_owned(),
        };
        let stale = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![
                        classify(first, current_turn_id),
                        classify(second, &old_turn_id),
                    ],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("one stale historical member must reject the whole group");
        assert!(stale.to_string().contains("exact current open turn"));
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![
                        classify(first, current_turn_id),
                        classify(second, current_turn_id),
                    ],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("all members bound to the current retry wait");
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
    async fn every_supersede_requires_a_later_same_context_abort_cutoff() {
        let received_store = test_store().await;
        let received_writer = EventWriter::new(received_store);
        received_writer
            .persist_inbound(&user_command(
                1,
                "00000000-0000-4000-8000-000000000041",
                "received",
            ))
            .await
            .expect("persist received user");
        let received_error = received_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandSuperseded {
                        command_id: "00000000-0000-4000-8000-000000000041".to_owned(),
                        command_seq: 1,
                        run_id: None,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("Idle supersede without Abort must fail");
        assert!(received_error.to_string().contains("later Abort"));

        let classified_store = test_store().await;
        let classified_writer = EventWriter::new(classified_store);
        classified_writer
            .persist_inbound(&user_command(
                1,
                "00000000-0000-4000-8000-000000000042",
                "classified",
            ))
            .await
            .expect("persist classified user");
        classified_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: "00000000-0000-4000-8000-000000000042".to_owned(),
                        application_kind: ApplicationKind::IdleRun,
                        run_id: "run-supersede".to_owned(),
                        turn_id: "turn-supersede".to_owned(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("classify user");
        let classified_error = classified_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandSuperseded {
                        command_id: "00000000-0000-4000-8000-000000000042".to_owned(),
                        command_seq: 1,
                        run_id: Some("run-supersede".to_owned()),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("classified supersede without Abort must fail");
        assert!(classified_error.to_string().contains("later Abort"));
    }

    #[tokio::test]
    async fn owner_close_rejects_pre_assistant_agent_end_and_handoff() {
        for (index, phase) in [RunPhase::UserStarted, RunPhase::UserCommitted]
            .into_iter()
            .enumerate()
        {
            let store = test_store().await;
            let writer = EventWriter::new(store.clone());
            let owner_id = format!("00000000-0000-4000-8000-00000000005{}", index * 2);
            let next_id = format!("00000000-0000-4000-8000-00000000005{}", index * 2 + 1);
            writer
                .persist_inbound(&user_command(1, &owner_id, "owner"))
                .await
                .expect("persist owner");
            sqlx::query(
                "UPDATE inbound_commands
                 SET status='applying', application_kind='idle_run', run_id='run-phase',
                     turn_id='turn-owner', run_phase=?
                 WHERE command_id=?",
            )
            .bind(phase.as_str())
            .bind(&owner_id)
            .execute(store.pool())
            .await
            .expect("seed pre-assistant owner");

            let agent_end_error = writer
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: Some(DurableEvent::agent_end("run-phase").expect("typed AgentEnd")),
                        projections: vec![Projection::CommandApplied {
                            command_id: owner_id.clone(),
                            command_seq: 1,
                            run_id: Some("run-phase".to_owned()),
                        }],
                    }],
                    injected_commands: Vec::new(),
                })
                .await
                .expect_err("pre-assistant AgentEnd must fail");
            assert!(agent_end_error.to_string().contains("must close from"));

            writer
                .persist_inbound(&user_command(2, &next_id, "next"))
                .await
                .expect("persist next owner");
            sqlx::query(
                "UPDATE inbound_commands
                 SET status='applying', application_kind='soft_steer', run_id='run-phase',
                     turn_id='turn-next', run_phase='classified', received_at=?
                 WHERE command_id=?",
            )
            .bind(durable_test_timestamp().to_rfc3339())
            .bind(&next_id)
            .execute(store.pool())
            .await
            .expect("seed next owner");
            let message = user_message("next");
            let message_id = user_message_id(next_id.as_str());
            let handoff_error = writer
                .apply(EventBatch {
                    writes: vec![
                        EventWrite {
                            event: Some(
                                DurableEvent::steered(
                                    SteerMode::Soft,
                                    next_id.clone(),
                                    "run-phase".to_owned(),
                                    "turn-next".to_owned(),
                                )
                                .expect("Steered"),
                            ),
                            projections: vec![Projection::RunPhase {
                                command_id: next_id.clone(),
                                run_id: "run-phase".to_owned(),
                                expected: RunPhase::Classified,
                                next: RunPhase::TurnStarted,
                            }],
                        },
                        EventWrite {
                            event: Some(
                                DurableEvent::turn_start("run-phase", "turn-next")
                                    .expect("TurnStart"),
                            ),
                            projections: Vec::new(),
                        },
                        EventWrite {
                            event: Some(
                                DurableEvent::message("message_start", &message_id, &message)
                                    .expect("MessageStart"),
                            ),
                            projections: vec![
                                Projection::CommandApplied {
                                    command_id: owner_id.clone(),
                                    command_seq: 1,
                                    run_id: Some("run-phase".to_owned()),
                                },
                                Projection::RunPhase {
                                    command_id: next_id.clone(),
                                    run_id: "run-phase".to_owned(),
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
                                    message_id,
                                    role: "user",
                                    message,
                                    append_to_l0: true,
                                    provider_context: Vec::new(),
                                    eviction_footprint_tokens: 0,
                                },
                                Projection::RunPhase {
                                    command_id: next_id.clone(),
                                    run_id: "run-phase".to_owned(),
                                    expected: RunPhase::UserStarted,
                                    next: RunPhase::UserCommitted,
                                },
                            ],
                        },
                    ],
                    injected_commands: vec![InjectedCommand::new(2, next_id)],
                })
                .await
                .expect_err("pre-assistant owner handoff must fail");
            assert!(
                handoff_error.to_string().contains("handoff"),
                "{handoff_error:#}"
            );
        }
    }

    #[tokio::test]
    async fn agent_end_rejects_pending_steer_but_abort_close_supersedes_it_atomically() {
        let normal_store = test_store().await;
        let normal_writer = EventWriter::new(normal_store.clone());
        normal_writer
            .persist_inbound(&user_command(
                1,
                "00000000-0000-4000-8000-000000000061",
                "owner",
            ))
            .await
            .expect("persist owner");
        normal_writer
            .persist_inbound(&user_command(
                2,
                "00000000-0000-4000-8000-000000000062",
                "pending",
            ))
            .await
            .expect("persist pending steer");
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='idle_run', run_id='run-pending',
                 turn_id='turn-owner', run_phase='assistant_started'
             WHERE seq=1",
        )
        .execute(normal_store.pool())
        .await
        .expect("seed owner");
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='soft_steer', run_id='run-pending',
                 turn_id='turn-next', run_phase='classified'
             WHERE seq=2",
        )
        .execute(normal_store.pool())
        .await
        .expect("seed pending steer");
        let normal_error = normal_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(DurableEvent::agent_end("run-pending").expect("AgentEnd")),
                    projections: vec![Projection::CommandApplied {
                        command_id: "00000000-0000-4000-8000-000000000061".to_owned(),
                        command_seq: 1,
                        run_id: Some("run-pending".to_owned()),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("normal AgentEnd must not strand a pending steer");
        assert!(normal_error.to_string().contains("pending steer"));

        let abort_store = test_store().await;
        let abort_writer = EventWriter::new(abort_store.clone());
        for command in [
            user_command(1, "00000000-0000-4000-8000-000000000063", "owner"),
            user_command(2, "00000000-0000-4000-8000-000000000064", "pending"),
            abort_command(3, "00000000-0000-4000-8000-000000000065"),
        ] {
            abort_writer
                .persist_inbound(&command)
                .await
                .expect("persist Abort-close command");
        }
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='idle_run', run_id='run-abort-close',
                 turn_id='turn-owner', run_phase='assistant_started'
             WHERE seq=1",
        )
        .execute(abort_store.pool())
        .await
        .expect("seed Abort-close owner");
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='soft_steer', run_id='run-abort-close',
                 turn_id='turn-next', run_phase='classified'
             WHERE seq=2",
        )
        .execute(abort_store.pool())
        .await
        .expect("seed Abort-close pending steer");
        abort_writer
            .reset_checkpoint_after_direct_fixture_mutation()
            .await;
        abort_writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: None,
                        projections: vec![Projection::CommandSuperseded {
                            command_id: "00000000-0000-4000-8000-000000000064".to_owned(),
                            command_seq: 2,
                            run_id: Some("run-abort-close".to_owned()),
                        }],
                    },
                    EventWrite {
                        event: None,
                        projections: vec![
                            Projection::RunPhase {
                                command_id: "00000000-0000-4000-8000-000000000063".to_owned(),
                                run_id: "run-abort-close".to_owned(),
                                expected: RunPhase::AssistantStarted,
                                next: RunPhase::CancelRequested,
                            },
                            Projection::CommandApplied {
                                command_id: "00000000-0000-4000-8000-000000000065".to_owned(),
                                command_seq: 3,
                                run_id: Some("run-abort-close".to_owned()),
                            },
                        ],
                    },
                    EventWrite {
                        event: Some(DurableEvent::agent_end("run-abort-close").expect("AgentEnd")),
                        projections: vec![Projection::CommandApplied {
                            command_id: "00000000-0000-4000-8000-000000000063".to_owned(),
                            command_seq: 1,
                            run_id: Some("run-abort-close".to_owned()),
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("Abort closes owner after superseding every pending steer");
        let statuses: (String, String, String) = sqlx::query_as(
            "SELECT
                (SELECT status FROM inbound_commands WHERE seq=1),
                (SELECT status FROM inbound_commands WHERE seq=2),
                (SELECT status FROM inbound_commands WHERE seq=3)",
        )
        .fetch_one(abort_store.pool())
        .await
        .expect("read Abort-close states");
        assert_eq!(
            statuses,
            (
                "applied".to_owned(),
                "superseded".to_owned(),
                "applied".to_owned()
            )
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
        writer
            .reset_checkpoint_after_direct_fixture_mutation()
            .await;

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
            .reset_checkpoint_after_direct_fixture_mutation()
            .await;
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
        handoff_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Prepare {
                        tool_call_id: "tool-handoff".to_owned(),
                        command_id: "00000000-0000-4000-8000-000000000019".to_owned(),
                        run_id: "run-handoff".to_owned(),
                        executor_generation: test_process_generation(1),
                        idempotency_key: "idem-handoff".to_owned(),
                    })],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("prepare handoff tool");
        handoff_writer
            .apply(EventBatch {
                writes: vec![tool_start_write("tool-handoff", "run-handoff")],
                injected_commands: Vec::new(),
            })
            .await
            .expect("start handoff tool");
        let message = user_message("new");
        let mut handoff_batch = EventBatch {
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
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
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
        };
        let active_error = handoff_writer
            .apply(handoff_batch.clone())
            .await
            .expect_err("owner handoff cannot leave a running tool behind");
        assert!(active_error.to_string().contains("active running tool"));
        let mut cleanup = tool_finish_writes(
            "tool-handoff",
            "running",
            "succeeded",
            None,
            "handoff complete",
            false,
        );
        cleanup.append(&mut handoff_batch.writes);
        handoff_batch.writes = cleanup;
        handoff_writer
            .apply(handoff_batch)
            .await
            .expect("canonical atomic owner handoff with tool cleanup");
        let states: (String, String) = sqlx::query_as(
            "SELECT
                (SELECT status FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000019'),
                (SELECT run_phase FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000018')",
        )
        .fetch_one(handoff_store.pool())
        .await
        .expect("handoff states");
        assert_eq!(states, ("applied".to_owned(), "user_committed".to_owned()));
        let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(handoff_store.pool())
            .await
            .expect("count events before late finish");
        let late_finish = handoff_writer
            .apply(EventBatch {
                writes: tool_finish_writes(
                    "tool-handoff",
                    "running",
                    "succeeded",
                    None,
                    "late duplicate",
                    false,
                ),
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("late ToolExecutionEnd after owner close must not leak result events");
        assert!(late_finish.to_string().contains("durable state succeeded"));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(handoff_store.pool())
                .await
                .expect("late finish rollback event count"),
            event_count
        );
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
                                run_id: "run-1".to_owned(),
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
    async fn pending_approval_requires_a_same_run_prepared_tool_and_owner() {
        let orphan_store = test_store().await;
        let orphan_writer = EventWriter::new(orphan_store.clone());
        seed_tool_owner(&orphan_store, &orphan_writer, "run-orphan").await;
        let orphan = orphan_writer
            .apply(EventBatch {
                writes: vec![pending_approval_write(
                    "request-orphan",
                    "tool-orphan",
                    "run-orphan",
                )],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("pending approval without a prepared tool must fail");
        assert!(orphan.to_string().contains("requires prepared tool"));
        assert_eq!(
            sqlx::query_as::<_, (i64, i64)>(
                "SELECT
                    (SELECT COUNT(*) FROM approval_log),
                    (SELECT COUNT(*) FROM agent_events)",
            )
            .fetch_one(orphan_store.pool())
            .await
            .expect("orphan rollback counts"),
            (0, 0)
        );

        let wrong_turn_store = test_store().await;
        let wrong_turn_writer = EventWriter::new(wrong_turn_store.clone());
        seed_tool_owner(&wrong_turn_store, &wrong_turn_writer, "run-wrong-turn").await;
        wrong_turn_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Prepare {
                        tool_call_id: "tool-wrong-turn".to_owned(),
                        command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                        run_id: "run-wrong-turn".to_owned(),
                        executor_generation: test_process_generation(1),
                        idempotency_key: "idem-wrong-turn".to_owned(),
                    })],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("prepare wrong-turn fixture");
        let mut wrong_turn =
            pending_approval_write("request-wrong-turn", "tool-wrong-turn", "run-wrong-turn");
        let Projection::Approval(ApprovalMutation::Pending { turn_id, .. }) =
            &mut wrong_turn.projections[0]
        else {
            panic!("pending approval helper shape changed")
        };
        *turn_id = "turn-other".to_owned();
        let error = wrong_turn_writer
            .apply(EventBatch {
                writes: vec![wrong_turn],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("approval turn cannot differ from its durable owner turn");
        assert!(error.to_string().contains("durable owner turn turn-1"));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM approval_log")
                .fetch_one(wrong_turn_store.pool())
                .await
                .expect("wrong-turn rollback"),
            0
        );

        let cross_run_store = test_store().await;
        let cross_run_writer = EventWriter::new(cross_run_store.clone());
        seed_tool_owner(&cross_run_store, &cross_run_writer, "run-a").await;
        let mut pending = pending_approval_write("request-cross-run", "tool-cross-run", "run-b");
        pending.projections.insert(
            0,
            Projection::ToolExecution(ToolExecutionMutation::Prepare {
                tool_call_id: "tool-cross-run".to_owned(),
                command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                run_id: "run-a".to_owned(),
                executor_generation: test_process_generation(1),
                idempotency_key: "idem-cross-run".to_owned(),
            }),
        );
        let cross_run = cross_run_writer
            .apply(EventBatch {
                writes: vec![pending],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("pending approval cannot change the prepared tool run");
        assert!(
            cross_run
                .to_string()
                .contains("does not match prepared tool")
        );
        assert_eq!(
            sqlx::query_as::<_, (i64, i64, i64)>(
                "SELECT
                    (SELECT COUNT(*) FROM approval_log),
                    (SELECT COUNT(*) FROM tool_executions),
                    (SELECT COUNT(*) FROM agent_events)",
            )
            .fetch_one(cross_run_store.pool())
            .await
            .expect("cross-run rollback counts"),
            (0, 0, 0)
        );

        let wrong_owner_store = test_store().await;
        let wrong_owner_writer = EventWriter::new(wrong_owner_store.clone());
        seed_tool_owner(&wrong_owner_store, &wrong_owner_writer, "run-owner").await;
        let mut pending =
            pending_approval_write("request-wrong-owner", "tool-wrong-owner", "run-owner");
        pending.projections.insert(
            0,
            Projection::ToolExecution(ToolExecutionMutation::Prepare {
                tool_call_id: "tool-wrong-owner".to_owned(),
                command_id: "00000000-0000-4000-8000-000000000099".to_owned(),
                run_id: "run-owner".to_owned(),
                executor_generation: test_process_generation(1),
                idempotency_key: "idem-wrong-owner".to_owned(),
            }),
        );
        let wrong_owner = wrong_owner_writer
            .apply(EventBatch {
                writes: vec![pending],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("prepared tool must name the durable run owner command");
        assert!(
            wrong_owner
                .to_string()
                .contains("no matching durable owner command")
        );
    }

    #[tokio::test]
    async fn prepared_tool_start_is_run_bound_and_valid_existing_prepare_flow_survives() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_tool_owner(&store, &writer, "run-a").await;
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Prepare {
                        tool_call_id: "tool-existing".to_owned(),
                        command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                        run_id: "run-a".to_owned(),
                        executor_generation: test_process_generation(1),
                        idempotency_key: "idem-existing".to_owned(),
                    })],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("prepare same-run tool before policy result");

        let cross_run_start = writer
            .apply(EventBatch {
                writes: vec![tool_start_write("tool-existing", "run-b")],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("ToolExecutionStart cannot substitute a different run");
        assert!(
            cross_run_start
                .to_string()
                .contains("does not match prepared run")
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM tool_executions WHERE tool_call_id='tool-existing'",
            )
            .fetch_one(store.pool())
            .await
            .expect("cross-run start rollback"),
            "prepared"
        );

        writer
            .apply(EventBatch {
                writes: vec![pending_approval_write(
                    "request-existing",
                    "tool-existing",
                    "run-a",
                )],
                injected_commands: Vec::new(),
            })
            .await
            .expect("existing prepared tool accepts a same-run pending approval");
        assert_eq!(
            sqlx::query_as::<_, (String, String, String)>(
                "SELECT a.state, a.run_id, t.run_id
                 FROM approval_log a
                 JOIN tool_executions t ON t.tool_call_id = a.tool_call_id
                 WHERE a.id='request-existing'",
            )
            .fetch_one(store.pool())
            .await
            .expect("same-run durable binding"),
            ("pending".to_owned(), "run-a".to_owned(), "run-a".to_owned())
        );

        let pending_start = writer
            .apply(EventBatch {
                writes: vec![tool_start_write("tool-existing", "run-a")],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("a pending approval cannot be treated as policy Allow");
        assert!(pending_start.to_string().contains("same-batch approved"));
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM tool_executions WHERE tool_call_id='tool-existing'",
            )
            .fetch_one(store.pool())
            .await
            .expect("pending start rollback"),
            "prepared"
        );

        sqlx::query("UPDATE approval_log SET run_id='run-b' WHERE id='request-existing'")
            .execute(store.pool())
            .await
            .expect("simulate a legacy cross-run approval binding");
        let cross_run_resolution = writer
            .apply(EventBatch {
                writes: vec![approval_resolution_write(
                    "request-existing",
                    "cancelled",
                    "runtime",
                    None,
                )],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("approval resolution must recheck the prepared tool run");
        assert!(
            cross_run_resolution
                .to_string()
                .contains("does not match prepared tool")
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM approval_log WHERE id='request-existing'",
            )
            .fetch_one(store.pool())
            .await
            .expect("cross-run resolution rollback"),
            "pending"
        );

        let allow_store = test_store().await;
        let allow_writer = EventWriter::new(allow_store.clone());
        seed_tool_owner(&allow_store, &allow_writer, "run-allow").await;
        allow_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Prepare {
                        tool_call_id: "tool-allow".to_owned(),
                        command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                        run_id: "run-allow".to_owned(),
                        executor_generation: test_process_generation(1),
                        idempotency_key: "idem-allow".to_owned(),
                    })],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("policy Allow prepares without an approval row");
        allow_writer
            .apply(EventBatch {
                writes: vec![tool_start_write("tool-allow", "run-allow")],
                injected_commands: Vec::new(),
            })
            .await
            .expect("policy Allow starts only when no approval row exists");
        assert_eq!(
            sqlx::query_as::<_, (String, i64)>(
                "SELECT
                    (SELECT state FROM tool_executions WHERE tool_call_id='tool-allow'),
                    (SELECT COUNT(*) FROM approval_log WHERE tool_call_id='tool-allow')",
            )
            .fetch_one(allow_store.pool())
            .await
            .expect("policy Allow durable state"),
            ("running".to_owned(), 0)
        );
    }

    #[tokio::test]
    async fn tool_prepare_and_start_require_ordered_canonical_assistant_message_end() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let command_id = "00000000-0000-4000-8000-000000000090";
        let injected = classified_injection(&writer, 1, command_id, "ignored", "tools").await;
        writer
            .apply(EventBatch {
                writes: injection_writes(command_id, "ignored", "tools"),
                injected_commands: vec![injected],
            })
            .await
            .expect("open tool turn");
        let run_id = format!("run-{command_id}");
        let turn_id = format!("turn-{command_id}");
        let assistant = assistant_tool_message(&["tool-origin-a", "tool-origin-b"]);
        let assistant_start = EventWrite {
            event: Some(
                DurableEvent::message_in_turn(
                    "message_start",
                    "assistant-tools",
                    &assistant,
                    Some(run_id.clone()),
                    Some(turn_id.clone()),
                )
                .expect("assistant MessageStart"),
            ),
            projections: vec![Projection::RunPhase {
                command_id: command_id.to_owned(),
                run_id: run_id.clone(),
                expected: RunPhase::UserCommitted,
                next: RunPhase::AssistantStarted,
            }],
        };
        let prepare = |tool_call_id: &str| {
            Projection::ToolExecution(ToolExecutionMutation::Prepare {
                tool_call_id: tool_call_id.to_owned(),
                command_id: command_id.to_owned(),
                run_id: run_id.clone(),
                executor_generation: test_process_generation(1),
                idempotency_key: format!("idem-{tool_call_id}"),
            })
        };
        let early = writer
            .apply(EventBatch {
                writes: vec![
                    assistant_start.clone(),
                    EventWrite {
                        event: None,
                        projections: vec![prepare("tool-origin-a")],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_end",
                                "assistant-tools",
                                &assistant,
                                Some(run_id.clone()),
                                Some(turn_id.clone()),
                            )
                            .expect("assistant MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: "assistant-tools".to_owned(),
                            role: "assistant",
                            message: assistant.clone(),
                            append_to_l0: true,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("Prepare cannot precede the assistant MessageEnd in one batch");
        assert!(early.to_string().contains("preceding assistant MessageEnd"));

        writer
            .apply(EventBatch {
                writes: vec![
                    assistant_start,
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_end",
                                "assistant-tools",
                                &assistant,
                                Some(run_id.clone()),
                                Some(turn_id.clone()),
                            )
                            .expect("assistant MessageEnd"),
                        ),
                        projections: vec![
                            Projection::MessageEnd {
                                message_id: "assistant-tools".to_owned(),
                                role: "assistant",
                                message: assistant,
                                append_to_l0: true,
                                provider_context: Vec::new(),
                                eviction_footprint_tokens: 0,
                            },
                            prepare("tool-origin-a"),
                            prepare("tool-origin-b"),
                        ],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("same-batch MessageEnd may prepare every tool in the response");
        writer
            .apply(EventBatch {
                writes: vec![tool_start_write("tool-origin-a", &run_id)],
                injected_commands: Vec::new(),
            })
            .await
            .expect("recovery transaction may start first prepared tool");
        writer
            .apply(EventBatch {
                writes: vec![tool_start_write("tool-origin-b", &run_id)],
                injected_commands: Vec::new(),
            })
            .await
            .expect("multiple tools from one assistant turn retain distinct origins");

        let unknown = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![prepare("tool-not-in-response")],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("a different tool_call_id cannot borrow the durable response");
        assert!(
            unknown
                .to_string()
                .contains("canonical preceding assistant MessageEnd")
        );

        let rejected_store = test_store().await;
        let rejected_writer = EventWriter::new(rejected_store);
        let rejected_command = "00000000-0000-4000-8000-000000000091";
        let rejected_injected =
            classified_injection(&rejected_writer, 1, rejected_command, "ignored", "reject").await;
        rejected_writer
            .apply(EventBatch {
                writes: injection_writes(rejected_command, "ignored", "reject"),
                injected_commands: vec![rejected_injected],
            })
            .await
            .expect("open rejected-tool turn");
        let rejected_run = format!("run-{rejected_command}");
        let rejected_turn = format!("turn-{rejected_command}");
        let rejected = PublicMessage::Assistant(PublicAssistantMessage {
            content: vec![PublicAssistantContent::RejectedToolCall {
                rejected: RejectedToolCall {
                    id: "rejected-no-execution".to_owned(),
                    name: "test".to_owned(),
                    error: ToolArgumentError::InvalidJson,
                },
                wire_item_index: 0,
            }],
            model: "test-model".to_owned(),
            provider: "test-provider".to_owned(),
            origin: test_provider_origin(),
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: durable_test_timestamp(),
        });
        rejected_writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_start",
                                "assistant-rejected-only",
                                &rejected,
                                Some(rejected_run.clone()),
                                Some(rejected_turn.clone()),
                            )
                            .expect("rejected MessageStart"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: rejected_command.to_owned(),
                            run_id: rejected_run.clone(),
                            expected: RunPhase::UserCommitted,
                            next: RunPhase::AssistantStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_end",
                                "assistant-rejected-only",
                                &rejected,
                                Some(rejected_run.clone()),
                                Some(rejected_turn),
                            )
                            .expect("rejected MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: "assistant-rejected-only".to_owned(),
                            role: "assistant",
                            message: rejected,
                            append_to_l0: true,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("rejected ToolCall remains a durable non-execution response");
        let rejected_prepare = rejected_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Prepare {
                        tool_call_id: "rejected-no-execution".to_owned(),
                        command_id: rejected_command.to_owned(),
                        run_id: rejected_run,
                        executor_generation: test_process_generation(1),
                        idempotency_key: "idem-rejected-no-execution".to_owned(),
                    })],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("rejected tool calls must never become execution origins");
        assert!(
            rejected_prepare
                .to_string()
                .contains("canonical preceding assistant MessageEnd")
        );
    }

    #[tokio::test]
    async fn tool_and_approval_opening_mutations_require_post_batch_assistant_owner() {
        for (phase, label) in [
            ("user_started", "before user commit"),
            ("user_committed", "before assistant"),
            ("hard_steer_requested", "after hard steer"),
            ("cancel_requested", "after abort"),
        ] {
            let store = test_store().await;
            let writer = EventWriter::new(store.clone());
            seed_tool_owner(&store, &writer, "run-phase").await;
            sqlx::query("UPDATE inbound_commands SET run_phase=? WHERE command_id=?")
                .bind(phase)
                .bind(TOOL_OWNER_COMMAND_ID)
                .execute(store.pool())
                .await
                .expect("set owner phase fixture");

            let error = writer
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: None,
                        projections: vec![Projection::ToolExecution(
                            ToolExecutionMutation::Prepare {
                                tool_call_id: format!("tool-{phase}"),
                                command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                                run_id: "run-phase".to_owned(),
                                executor_generation: test_process_generation(1),
                                idempotency_key: format!("idem-{phase}"),
                            },
                        )],
                    }],
                    injected_commands: Vec::new(),
                })
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("live assistant/tool execution owner"),
                "{label}: {error}"
            );
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tool_executions")
                    .fetch_one(store.pool())
                    .await
                    .expect("phase rejection rollback"),
                0,
                "{label}"
            );
        }

        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_tool_owner(&store, &writer, "run-close").await;
        let same_batch_close = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(DurableEvent::agent_end("run-close").expect("AgentEnd")),
                    projections: vec![
                        Projection::ToolExecution(ToolExecutionMutation::Prepare {
                            tool_call_id: "tool-close-race".to_owned(),
                            command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                            run_id: "run-close".to_owned(),
                            executor_generation: test_process_generation(1),
                            idempotency_key: "idem-close-race".to_owned(),
                        }),
                        Projection::CommandApplied {
                            command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                            command_seq: 1,
                            run_id: Some("run-close".to_owned()),
                        },
                    ],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("an owner close cannot race a new prepared execution");
        assert!(
            same_batch_close
                .to_string()
                .contains("same-batch owner close")
        );
        assert_eq!(
            sqlx::query_as::<_, (String, i64, i64)>(
                "SELECT
                    (SELECT run_phase FROM inbound_commands WHERE command_id=?),
                    (SELECT COUNT(*) FROM tool_executions),
                    (SELECT COUNT(*) FROM agent_events)",
            )
            .bind(TOOL_OWNER_COMMAND_ID)
            .fetch_one(store.pool())
            .await
            .expect("same-batch close rollback"),
            ("assistant_started".to_owned(), 0, 0)
        );

        let start_close_store = test_store().await;
        let start_close_writer = EventWriter::new(start_close_store.clone());
        seed_tool_owner(&start_close_store, &start_close_writer, "run-start-close").await;
        start_close_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Prepare {
                        tool_call_id: "tool-start-close".to_owned(),
                        command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                        run_id: "run-start-close".to_owned(),
                        executor_generation: test_process_generation(1),
                        idempotency_key: "idem-start-close".to_owned(),
                    })],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("prepare policy-Allow close-race fixture");
        let prepared_close = start_close_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::agent_end("run-start-close").expect("prepared AgentEnd"),
                    ),
                    projections: vec![Projection::CommandApplied {
                        command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                        command_seq: 1,
                        run_id: Some("run-start-close".to_owned()),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("policy-Allow prepared tool must be cleaned before owner close");
        assert!(prepared_close.to_string().contains("active prepared tool"));
        let start_close = start_close_writer
            .apply(EventBatch {
                writes: vec![
                    tool_start_write("tool-start-close", "run-start-close"),
                    EventWrite {
                        event: Some(DurableEvent::agent_end("run-start-close").expect("AgentEnd")),
                        projections: vec![Projection::CommandApplied {
                            command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                            command_seq: 1,
                            run_id: Some("run-start-close".to_owned()),
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("an owner close cannot race ToolExecutionStart");
        assert!(start_close.to_string().contains("same-batch owner close"));

        let pending_close_store = test_store().await;
        let pending_close_writer = EventWriter::new(pending_close_store.clone());
        seed_tool_owner(
            &pending_close_store,
            &pending_close_writer,
            "run-pending-close",
        )
        .await;
        pending_close_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Prepare {
                        tool_call_id: "tool-pending-close".to_owned(),
                        command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                        run_id: "run-pending-close".to_owned(),
                        executor_generation: test_process_generation(1),
                        idempotency_key: "idem-pending-close".to_owned(),
                    })],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("prepare approval close-race fixture");
        let pending_close = pending_close_writer
            .apply(EventBatch {
                writes: vec![
                    pending_approval_write(
                        "request-pending-close",
                        "tool-pending-close",
                        "run-pending-close",
                    ),
                    EventWrite {
                        event: Some(
                            DurableEvent::agent_end("run-pending-close").expect("AgentEnd"),
                        ),
                        projections: vec![Projection::CommandApplied {
                            command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                            command_seq: 1,
                            run_id: Some("run-pending-close".to_owned()),
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("an owner close cannot race Approval Pending");
        assert!(pending_close.to_string().contains("same-batch owner close"));

        let cleanup_store = test_store().await;
        let cleanup_writer = EventWriter::new(cleanup_store.clone());
        seed_pending_approval(
            &cleanup_store,
            &cleanup_writer,
            "request-cleanup",
            "tool-cleanup",
            "run-cleanup",
        )
        .await;
        let incomplete_cleanup = cleanup_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::agent_end("run-cleanup").expect("incomplete AgentEnd"),
                    ),
                    projections: vec![Projection::CommandApplied {
                        command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                        command_seq: 1,
                        run_id: Some("run-cleanup".to_owned()),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("pending prepared work must be terminalized before owner close");
        assert!(
            incomplete_cleanup
                .to_string()
                .contains("active prepared tool")
        );
        let result = tool_result("tool-cleanup", "approval cancelled", true);
        cleanup_writer
            .apply(EventBatch {
                writes: vec![
                    approval_resolution_write("request-cleanup", "cancelled", "runtime", None),
                    EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type":"tool_execution_end",
                                "tool_call_id":"tool-cleanup",
                                "state":"cancelled",
                                "result":result.clone(),
                                "is_error":true,
                                "error_code":"cancelled"
                            }))
                            .expect("cancelled tool event"),
                        ),
                        projections: vec![Projection::ToolExecution(
                            ToolExecutionMutation::Finish {
                                tool_call_id: "tool-cleanup".to_owned(),
                                expected: "prepared",
                                state: "cancelled",
                                error_code: Some("cancelled"),
                            },
                        )],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_start", "tool-cleanup-result", &result)
                                .expect("cleanup result MessageStart"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_end", "tool-cleanup-result", &result)
                                .expect("cleanup result MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: "tool-cleanup-result".to_owned(),
                            role: "tool_result",
                            message: result,
                            append_to_l0: true,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::agent_end("run-cleanup").expect("cleanup AgentEnd"),
                        ),
                        projections: vec![Projection::CommandApplied {
                            command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                            command_seq: 1,
                            run_id: Some("run-cleanup".to_owned()),
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("explicit cancellation cleanup may close its owner atomically");
        assert_eq!(
            sqlx::query_as::<_, (String, String, String)>(
                "SELECT
                    (SELECT state FROM approval_log WHERE id='request-cleanup'),
                    (SELECT state FROM tool_executions WHERE tool_call_id='tool-cleanup'),
                    (SELECT status FROM inbound_commands WHERE command_id=?)",
            )
            .bind(TOOL_OWNER_COMMAND_ID)
            .fetch_one(cleanup_store.pool())
            .await
            .expect("cancellation cleanup state"),
            (
                "cancelled".to_owned(),
                "cancelled".to_owned(),
                "applied".to_owned()
            )
        );

        let deny_store = test_store().await;
        let deny_writer = EventWriter::new(deny_store.clone());
        seed_pending_approval(
            &deny_store,
            &deny_writer,
            "request-deny-open",
            "tool-deny-open",
            "run-deny-open",
        )
        .await;
        deny_writer
            .persist_inbound(&approval_command_with_decision(
                2,
                "00000000-0000-4000-8000-000000000020",
                "request-deny-open",
                ApprovalDecision::Deny,
            ))
            .await
            .expect("persist standalone denial command");
        deny_writer
            .apply(EventBatch {
                writes: vec![approval_resolution_write(
                    "request-deny-open",
                    "denied",
                    "user-1",
                    Some(("00000000-0000-4000-8000-000000000020", 2, "run-deny-open")),
                )],
                injected_commands: Vec::new(),
            })
            .await
            .expect("standalone denial may leave the owner open for later cleanup");
        assert_eq!(
            sqlx::query_as::<_, (String, String, String)>(
                "SELECT
                    (SELECT state FROM approval_log WHERE id='request-deny-open'),
                    (SELECT state FROM tool_executions WHERE tool_call_id='tool-deny-open'),
                    (SELECT run_phase FROM inbound_commands WHERE command_id=?)",
            )
            .bind(TOOL_OWNER_COMMAND_ID)
            .fetch_one(deny_store.pool())
            .await
            .expect("standalone denial state"),
            (
                "denied".to_owned(),
                "prepared".to_owned(),
                "assistant_started".to_owned()
            )
        );
    }

    #[tokio::test]
    async fn approval_and_tool_transitions_share_event_writer_transactions() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_tool_owner(&store, &writer, "run-1").await;
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
                            executor_generation: test_process_generation(1),
                            idempotency_key: "00000000-0000-4000-8000-000000000001/tool-1"
                                .to_owned(),
                        }),
                        Projection::Approval(ApprovalMutation::Pending {
                            request_id: "request-1".to_owned(),
                            tool_call_id: "tool-1".to_owned(),
                            run_id: "run-1".to_owned(),
                            turn_id: "turn-1".to_owned(),
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
                2,
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
                        command_seq: 2,
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
                                command_seq: 2,
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
                                run_id: "run-1".to_owned(),
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
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
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
    async fn no_op_approval_decision_requires_unknown_or_terminal_request() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_pending_approval(&store, &writer, "request-1", "tool-1", "run-1").await;
        let pending_decision =
            approval_command(2, "00000000-0000-4000-8000-000000000020", "request-1");
        writer
            .persist_inbound(&pending_decision)
            .await
            .expect("persist pending approval decision");

        let pending_error = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandApplied {
                        command_id: "00000000-0000-4000-8000-000000000020".to_owned(),
                        command_seq: 2,
                        run_id: None,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("pending approval cannot be applied as a no-op");
        assert!(pending_error.to_string().contains("pending approval"));
        assert_eq!(
            sqlx::query_as::<_, (String, String)>(
                "SELECT
                    (SELECT status FROM inbound_commands WHERE seq=2),
                    (SELECT state FROM approval_log WHERE id='request-1')",
            )
            .fetch_one(store.pool())
            .await
            .expect("pending no-op rollback state"),
            ("received".to_owned(), "pending".to_owned())
        );

        writer
            .apply(EventBatch {
                writes: vec![approval_resolution_write(
                    "request-1",
                    "cancelled",
                    "runtime",
                    None,
                )],
                injected_commands: Vec::new(),
            })
            .await
            .expect("terminalize approval");
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandApplied {
                        command_id: "00000000-0000-4000-8000-000000000020".to_owned(),
                        command_seq: 2,
                        run_id: None,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("terminal approval permits a no-op");
        assert_eq!(
            writer
                .persist_inbound(&pending_decision)
                .await
                .expect("terminal request decision replay")
                .status,
            CommandAckStatus::Applied
        );

        let unknown_decision =
            approval_command(3, "00000000-0000-4000-8000-000000000021", "request-unknown");
        writer
            .persist_inbound(&unknown_decision)
            .await
            .expect("persist unknown approval decision");
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandApplied {
                        command_id: "00000000-0000-4000-8000-000000000021".to_owned(),
                        command_seq: 3,
                        run_id: None,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("unknown approval permits a no-op");
        assert_eq!(
            writer
                .persist_inbound(&unknown_decision)
                .await
                .expect("unknown request decision replay")
                .status,
            CommandAckStatus::Applied
        );

        let cutoff_store = test_store().await;
        let cutoff_writer = EventWriter::new(cutoff_store.clone());
        seed_pending_approval(
            &cutoff_store,
            &cutoff_writer,
            "request-cutoff",
            "tool-cutoff",
            "run-cutoff",
        )
        .await;
        cutoff_writer
            .persist_inbound(&approval_command(
                2,
                "00000000-0000-4000-8000-000000000022",
                "request-cutoff",
            ))
            .await
            .expect("persist abort-preempted approval decision");
        cutoff_writer
            .persist_inbound(&abort_command(3, "00000000-0000-4000-8000-000000000023"))
            .await
            .expect("persist cutoff Abort");
        cutoff_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![
                        Projection::CommandApplied {
                            command_id: "00000000-0000-4000-8000-000000000022".to_owned(),
                            command_seq: 2,
                            run_id: None,
                        },
                        Projection::RunPhase {
                            command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                            run_id: "run-cutoff".to_owned(),
                            expected: RunPhase::AssistantStarted,
                            next: RunPhase::CancelRequested,
                        },
                        Projection::CommandApplied {
                            command_id: "00000000-0000-4000-8000-000000000023".to_owned(),
                            command_seq: 3,
                            run_id: Some("run-cutoff".to_owned()),
                        },
                    ],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("same-batch later Abort permits the canonical pending no-op");
        assert_eq!(
            sqlx::query_as::<_, (String, String, String)>(
                "SELECT
                    (SELECT status FROM inbound_commands WHERE seq=2),
                    (SELECT status FROM inbound_commands WHERE seq=3),
                    (SELECT state FROM approval_log WHERE id='request-cutoff')",
            )
            .fetch_one(cutoff_store.pool())
            .await
            .expect("canonical Abort cutoff states"),
            (
                "applied".to_owned(),
                "applied".to_owned(),
                "pending".to_owned()
            )
        );

        let forged_store = test_store().await;
        let forged_writer = EventWriter::new(forged_store.clone());
        seed_pending_approval(
            &forged_store,
            &forged_writer,
            "request-forged",
            "tool-forged",
            "run-forged",
        )
        .await;
        forged_writer
            .persist_inbound(&approval_command(
                2,
                "00000000-0000-4000-8000-000000000024",
                "request-forged",
            ))
            .await
            .expect("persist pending approval decision");
        forged_writer
            .persist_inbound(&abort_command(3, "00000000-0000-4000-8000-000000000025"))
            .await
            .expect("persist later Abort");
        let forged = forged_writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![
                        Projection::RunPhase {
                            command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                            run_id: "run-forged".to_owned(),
                            expected: RunPhase::AssistantStarted,
                            next: RunPhase::CancelRequested,
                        },
                        Projection::CommandApplied {
                            command_id: "00000000-0000-4000-8000-000000000025".to_owned(),
                            command_seq: 3,
                            run_id: Some("run-forged".to_owned()),
                        },
                        Projection::CommandApplied {
                            command_id: "00000000-0000-4000-8000-000000000024".to_owned(),
                            command_seq: 2,
                            run_id: None,
                        },
                    ],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("an unrelated or earlier Abort projection cannot forge the exception");
        assert!(forged.to_string().contains("terminal projection"));
        assert_eq!(
            sqlx::query_as::<_, (String, String)>(
                "SELECT
                    (SELECT status FROM inbound_commands WHERE seq=2),
                    (SELECT status FROM inbound_commands WHERE seq=3)",
            )
            .fetch_one(forged_store.pool())
            .await
            .expect("forged exception rollback states"),
            ("received".to_owned(), "received".to_owned())
        );
    }

    #[tokio::test]
    async fn approval_decision_is_cryptographically_and_semantically_bound() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_pending_approval(&store, &writer, "request-1", "tool-1", "run-1").await;
        writer
            .persist_inbound(&approval_command_with_decision(
                2,
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
                    Some(("00000000-0000-4000-8000-000000000022", 2, "run-1")),
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
                        Some(("00000000-0000-4000-8000-000000000022", 2, "run-1")),
                    ),
                    tool_start_write("tool-1", "run-1"),
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("deny and tool start must roll back together");
        assert!(denied_start.to_string().contains("same-batch approved"));
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
                    Some(("00000000-0000-4000-8000-000000000022", 2, "run-1")),
                )],
                injected_commands: Vec::new(),
            })
            .await
            .expect("canonical denial");
        let replay = writer
            .persist_inbound(&approval_command_with_decision(
                2,
                "00000000-0000-4000-8000-000000000022",
                "request-1",
                ApprovalDecision::Deny,
            ))
            .await
            .expect("canonical deny replay");
        assert_eq!(replay.status, CommandAckStatus::Applied);
        let denied_after_commit = writer
            .apply(EventBatch {
                writes: vec![tool_start_write("tool-1", "run-1")],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("a terminal denial cannot later be treated as policy Allow");
        assert!(denied_after_commit.to_string().contains("state denied"));

        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_pending_approval(&store, &writer, "request-2", "tool-2", "run-2").await;
        writer
            .persist_inbound(&approval_command(
                2,
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
                    Some(("00000000-0000-4000-8000-000000000033", 2, "run-2")),
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
                2,
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
                    Some(("00000000-0000-4000-8000-000000000034", 2, "run-2b")),
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
                        command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                        run_id: "run-2b".to_owned(),
                        executor_generation: test_process_generation(1),
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
                        Some(("00000000-0000-4000-8000-000000000034", 2, "run-2b")),
                    ),
                    tool_start_write("wrong-tool", "run-2b"),
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
                2,
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
                    Some(("00000000-0000-4000-8000-000000000035", 2, "run-3")),
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
                    tool_start_write("tool-4", "run-4"),
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("runtime cancellation cannot start a tool");
        assert!(cancelled_start.to_string().contains("same-batch approved"));
        writer
            .apply(EventBatch {
                writes: vec![approval_resolution_write(
                    "request-4",
                    "cancelled",
                    "runtime",
                    None,
                )],
                injected_commands: Vec::new(),
            })
            .await
            .expect("commit canonical runtime cancellation");
        let cancelled_after_commit = writer
            .apply(EventBatch {
                writes: vec![tool_start_write("tool-4", "run-4")],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("a terminal cancellation cannot later be treated as policy Allow");
        assert!(
            cancelled_after_commit
                .to_string()
                .contains("state cancelled")
        );

        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_pending_approval(&store, &writer, "request-5", "tool-5", "run-5").await;
        writer
            .persist_inbound(&approval_command(
                2,
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
                    Some(("00000000-0000-4000-8000-000000000023", 2, "run-5")),
                )],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("tampered durable command must not resolve approval");
        assert!(format!("{tampered:#}").contains("HMAC"));
    }

    #[tokio::test]
    async fn approved_once_must_durably_precede_tool_execution_start() {
        let tampered_store = test_store().await;
        let tampered_writer = EventWriter::new(tampered_store.clone());
        seed_pending_approval(
            &tampered_store,
            &tampered_writer,
            "request-tampered",
            "tool-tampered",
            "run-tampered",
        )
        .await;
        sqlx::query(
            "UPDATE approval_log SET state='approved_once', decided_at='tampered'
             WHERE id='request-tampered'",
        )
        .execute(tampered_store.pool())
        .await
        .expect("tamper mutable approval projection");
        let missing_evidence = tampered_writer
            .apply(EventBatch {
                writes: vec![tool_start_write("tool-tampered", "run-tampered")],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("mutable approval projection cannot authorize execution");
        assert!(
            missing_evidence
                .to_string()
                .contains("cannot bypass approval")
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM tool_executions WHERE tool_call_id='tool-tampered'",
            )
            .fetch_one(tampered_store.pool())
            .await
            .expect("tampered projection start rolled back"),
            "prepared"
        );

        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_pending_approval(&store, &writer, "request-order", "tool-order", "run-order").await;
        let decision_id = "00000000-0000-4000-8000-000000000099";
        writer
            .persist_inbound(&approval_command(2, decision_id, "request-order"))
            .await
            .expect("persist approval decision");

        let reversed = writer
            .apply(EventBatch {
                writes: vec![
                    tool_start_write("tool-order", "run-order"),
                    approval_resolution_write(
                        "request-order",
                        "approved_once",
                        "user-order",
                        Some((decision_id, 2, "run-order")),
                    ),
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("execution-before-authorization must roll back");
        assert!(reversed.to_string().contains("must precede execution"));
        assert_eq!(
            sqlx::query_as::<_, (String, String, String)>(
                "SELECT
                    (SELECT state FROM approval_log WHERE id='request-order'),
                    (SELECT state FROM tool_executions WHERE tool_call_id='tool-order'),
                    (SELECT status FROM inbound_commands WHERE command_id=?)",
            )
            .bind(decision_id)
            .fetch_one(store.pool())
            .await
            .expect("reversed batch rollback"),
            (
                "pending".to_owned(),
                "prepared".to_owned(),
                "received".to_owned()
            )
        );

        writer
            .apply(EventBatch {
                writes: vec![approval_resolution_write(
                    "request-order",
                    "approved_once",
                    "user-order",
                    Some((decision_id, 2, "run-order")),
                )],
                injected_commands: Vec::new(),
            })
            .await
            .expect("authorization may commit before execution in an earlier transaction");
        writer
            .apply(EventBatch {
                writes: vec![tool_start_write("tool-order", "run-order")],
                injected_commands: Vec::new(),
            })
            .await
            .expect("already-approved prior-transaction flow remains valid");
        assert_eq!(
            sqlx::query_as::<_, (String, String)>(
                "SELECT
                    (SELECT state FROM approval_log WHERE id='request-order'),
                    (SELECT state FROM tool_executions WHERE tool_call_id='tool-order')",
            )
            .fetch_one(store.pool())
            .await
            .expect("authorized execution state"),
            ("approved_once".to_owned(), "running".to_owned())
        );
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
                        run_id: "run-1".to_owned(),
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
                                provider_context: Vec::new(),
                                eviction_footprint_tokens: 0,
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
                "request identity",
            ),
            (
                approval_request("request-3", "tool-3", "exec"),
                "unrelated",
                "tool-3",
                "no matching",
            ),
        ];
        for (request, request_id, tool_call_id, expected) in pending_cases {
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
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
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
        assert_eq!(
            l0_disposition(&assistant_message(StopReason::Error), false)
                .expect("retry error is excluded"),
            L0Disposition::ExcludeRetryError
        );
        assert_eq!(
            l0_disposition(&assistant_message(StopReason::Stop), true)
                .expect("normal assistant is appended"),
            L0Disposition::Append
        );
    }

    #[tokio::test]
    async fn approval_projection_and_version_are_writer_generated() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        seed_tool_owner(&store, &writer, "run-1").await;
        writer
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
                    projections: vec![
                        Projection::ToolExecution(ToolExecutionMutation::Prepare {
                            tool_call_id: "tool-secret".to_owned(),
                            command_id: TOOL_OWNER_COMMAND_ID.to_owned(),
                            run_id: "run-1".to_owned(),
                            executor_generation: test_process_generation(1),
                            idempotency_key: "idem-tool-secret".to_owned(),
                        }),
                        Projection::Approval(ApprovalMutation::Pending {
                            request_id: "request-secret".to_owned(),
                            tool_call_id: "tool-secret".to_owned(),
                            run_id: "run-1".to_owned(),
                            turn_id: "turn-1".to_owned(),
                        }),
                    ],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("writer derives the approval projection and version atomically");
        let (projection, version): (String, i64) = sqlx::query_as(
            "SELECT request_projection, redaction_version FROM approval_log
             WHERE id='request-secret'",
        )
        .fetch_one(store.pool())
        .await
        .expect("read writer-generated approval projection");
        assert!(!projection.contains("abcdefghijklmnop"));
        assert!(projection.contains("[REDACTED:"));
        assert_eq!(version, i64::from(store.redactor().version()));
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
            "command_received" | "command_rejected" => {}
            "tool_prepared" => {
                seed_tool_owner(store, writer, "run-1").await;
            }
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
            "approval_pending" => {
                seed_tool_owner(store, writer, "run-1").await;
            }
            "approval_resolved" => {
                seed_tool_owner(store, writer, "run-1").await;
                writer
                    .apply(hard_kill_target_batch("approval_pending"))
                    .await
                    .expect("prepare pending approval");
                writer
                    .persist_inbound(&approval_command(
                        2,
                        "00000000-0000-4000-8000-000000000020",
                        "request-1",
                    ))
                    .await
                    .expect("persist approval decision");
            }
            "tool_running" | "tool_terminal" => {
                seed_tool_owner(store, writer, "run-1").await;
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
                            executor_generation: test_process_generation(1),
                            idempotency_key: "00000000-0000-4000-8000-000000000001/tool-1"
                                .to_owned(),
                        }),
                        Projection::Approval(ApprovalMutation::Pending {
                            request_id: "request-1".to_owned(),
                            tool_call_id: "tool-1".to_owned(),
                            run_id: "run-1".to_owned(),
                            turn_id: "turn-1".to_owned(),
                        }),
                    ],
                }],
                injected_commands: Vec::new(),
            },
            "approval_resolved" => EventBatch {
                writes: vec![
                    EventWrite {
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
                                command_seq: 2,
                                run_id: Some("run-1".to_owned()),
                            },
                        ],
                    },
                    tool_start_write("tool-1", "run-1"),
                ],
                injected_commands: Vec::new(),
            },
            "tool_prepared" => EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Prepare {
                        tool_call_id: "tool-1".to_owned(),
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        run_id: "run-1".to_owned(),
                        executor_generation: test_process_generation(1),
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
                        run_id: "run-1".to_owned(),
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
                                provider_context: Vec::new(),
                                eviction_footprint_tokens: 0,
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
                let state: (i64, i64, i64, i64) = sqlx::query_as(
                    "SELECT
                        (SELECT COUNT(*) FROM approval_log
                         WHERE id='request-1' AND state='approved_once'),
                        (SELECT COUNT(*) FROM inbound_commands
                         WHERE command_id='00000000-0000-4000-8000-000000000020' AND status='applied'),
                        (SELECT COUNT(*) FROM tool_executions
                         WHERE tool_call_id='tool-1' AND state='running'),
                        (SELECT COUNT(*) FROM agent_events)",
                )
                .fetch_one(store.pool())
                .await
                .expect("approval resolved state");
                state == (1, 1, 1, 3)
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

    #[tokio::test]
    async fn projected_provider_terminal_round_trips_through_event_writer_with_full_metadata() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let command_id = "00000000-0000-4000-8000-000000000072";
        let run_id = format!("run-{command_id}");
        let turn_id = format!("turn-{command_id}");
        let injected =
            classified_injection(&writer, 1, command_id, "ignored", "metadata fixture").await;
        writer
            .apply(EventBatch {
                writes: injection_writes(command_id, "ignored", "metadata fixture"),
                injected_commands: vec![injected],
            })
            .await
            .expect("persist user injection");

        let timestamp = DateTime::parse_from_rfc3339("2026-07-21T01:02:03.456789Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let initial = PublicMessage::Assistant(PublicAssistantMessage {
            content: Vec::new(),
            model: "model-nondefault".to_owned(),
            provider: "provider-nondefault".to_owned(),
            origin: ProviderOrigin {
                provider_instance_id: "provider-instance-nondefault".to_owned(),
                protocol: ApiProtocol::AnthropicMessages,
                model: "model-nondefault".to_owned(),
            },
            usage: Usage::default(),
            stop_reason: StopReason::Error,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp,
        });
        let assistant_id = "assistant-provider-terminal";
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message_in_turn(
                            "message_start",
                            assistant_id,
                            &initial,
                            Some(run_id.clone()),
                            Some(turn_id.clone()),
                        )
                        .expect("assistant MessageStart"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: command_id.to_owned(),
                        run_id: run_id.clone(),
                        expected: RunPhase::UserCommitted,
                        next: RunPhase::AssistantStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("open assistant attempt");

        let message = AssistantMessage {
            content: vec![
                AssistantContent::Thinking {
                    thinking: "non-default plan".to_owned(),
                    signature_field: "thinking".to_owned(),
                    wire_item_index: 4,
                },
                AssistantContent::Text {
                    text: "partial answer".to_owned(),
                    wire_item_index: 5,
                },
            ],
            model: "model-nondefault".to_owned(),
            provider: "provider-nondefault".to_owned(),
            origin: ProviderOrigin {
                provider_instance_id: "provider-instance-nondefault".to_owned(),
                protocol: ApiProtocol::AnthropicMessages,
                model: "model-nondefault".to_owned(),
            },
            usage: Usage {
                input: 11,
                output: 22,
                cache_read: 33,
                cache_write: 44,
                reasoning: 55,
                total_tokens: 110,
            },
            stop_reason: StopReason::Error,
            error_message: Some("provider unavailable".to_owned()),
            provider_code: Some("http_503".to_owned()),
            interrupted: true,
            timestamp,
        };
        let mut projector = ProviderEventProjector::new(assistant_id).expect("projector");
        assert!(matches!(
            projector.project(ProviderEvent::Start).expect("Start"),
            ProjectedProviderEvent::Started
        ));
        let ProjectedProviderEvent::Terminal(terminal) = projector
            .project(ProviderEvent::Error {
                reason: StopReason::Error,
                output: ProviderOutput {
                    message,
                    provider_context: Vec::new(),
                },
            })
            .expect("terminal projection")
        else {
            panic!("expected terminal");
        };
        assert_eq!(terminal.kind(), ProviderTerminalKind::Error);
        let PublicMessage::Assistant(projected) = terminal.message() else {
            panic!("expected assistant projection");
        };
        assert_eq!(projected.usage.input, 11);
        assert_eq!(projected.usage.reasoning, 55);
        assert_eq!(projected.origin.protocol, ApiProtocol::AnthropicMessages);
        assert_eq!(
            projected.origin.provider_instance_id,
            "provider-instance-nondefault"
        );
        assert_eq!(
            projected.error_message.as_deref(),
            Some("provider unavailable")
        );
        assert_eq!(projected.provider_code.as_deref(), Some("http_503"));
        assert_eq!(projected.timestamp, timestamp);
        let terminal_message = terminal.message().clone();
        let terminal_write = terminal
            .into_t12_write(run_id.clone(), turn_id.clone(), false)
            .expect("context-free T12 terminal write");
        let terminal_sequences = writer
            .apply(EventBatch {
                writes: vec![
                    terminal_write,
                    EventWrite {
                        event: Some(
                            DurableEvent::turn_end(
                                run_id.clone(),
                                turn_id.clone(),
                                terminal_message.clone(),
                                Vec::new(),
                            )
                            .expect("TurnEnd"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(DurableEvent::agent_end(run_id.clone()).expect("AgentEnd")),
                        projections: vec![Projection::CommandApplied {
                            command_id: command_id.to_owned(),
                            command_seq: 1,
                            run_id: Some(run_id),
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist projected provider terminal");
        let [terminal_message_end_seq, _, _] = terminal_sequences.as_slice() else {
            panic!("terminal batch must persist MessageEnd, TurnEnd, and AgentEnd");
        };
        let terminal_message_end_seq = i64::try_from(*terminal_message_end_seq)
            .expect("terminal MessageEnd sequence fits SQLite INTEGER");

        let payload: String =
            sqlx::query_scalar("SELECT payload FROM messages WHERE id=? AND role='assistant'")
                .bind(assistant_id)
                .fetch_one(store.pool())
                .await
                .expect("durable message projection");
        assert_eq!(
            serde_json::from_str::<PublicMessage>(&payload).expect("message payload"),
            terminal_message
        );
        let mut transaction = store.pool().begin().await.expect("event read transaction");
        let durable = load_authenticated_event(&store, &mut transaction, terminal_message_end_seq)
            .await
            .expect("authenticated MessageEnd");
        transaction.rollback().await.expect("rollback event read");
        assert_eq!(durable.kind, "message_end");
        assert_eq!(
            durable.envelope,
            serde_json::to_value(&terminal_message)
                .map(|message| {
                    json!({"type":"message_end","message_id":assistant_id,"message":message})
                })
                .expect("terminal envelope")
        );
    }

    #[tokio::test]
    async fn message_end_persists_encrypted_provider_context_and_eviction_tokens() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let command_id = "00000000-0000-4000-8000-000000000073";
        let run_id = format!("run-{command_id}");
        let turn_id = format!("turn-{command_id}");
        let injected =
            classified_injection(&writer, 1, command_id, "ignored", "context fixture").await;
        writer
            .apply(EventBatch {
                writes: injection_writes(command_id, "ignored", "context fixture"),
                injected_commands: vec![injected],
            })
            .await
            .expect("persist user injection");

        let timestamp = durable_test_timestamp();
        let message = AssistantMessage {
            content: vec![AssistantContent::Text {
                text: "answer with reasoning".to_owned(),
                wire_item_index: 0,
            }],
            model: "model-context".to_owned(),
            provider: "provider-context".to_owned(),
            origin: ProviderOrigin {
                provider_instance_id: "provider-instance-context".to_owned(),
                protocol: ApiProtocol::OpenAiChatCompletions,
                model: "model-context".to_owned(),
            },
            usage: Usage::default(),
            stop_reason: StopReason::Stop,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp,
        };
        let assistant_id = "assistant-with-context";
        let mut projector = ProviderEventProjector::new(assistant_id).expect("projector");
        assert!(matches!(
            projector.project(ProviderEvent::Start).expect("Start"),
            ProjectedProviderEvent::Started
        ));

        let reasoning = ProviderContextFragment {
            wire_item_index: Some(0),
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::OpenAiChatCompletions,
                item: json!({"text": "plain reasoning"}),
            },
        };
        let window = ProviderContextFragment {
            wire_item_index: None,
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({"summary": "compact"})],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 1,
                    context_fingerprint: "fp-1".to_owned(),
                },
            },
        };

        let ProjectedProviderEvent::Terminal(terminal) = projector
            .project(ProviderEvent::Done {
                reason: StopReason::Stop,
                output: ProviderOutput {
                    message,
                    provider_context: vec![reasoning, window],
                },
            })
            .expect("terminal projection")
        else {
            panic!("expected terminal");
        };

        let terminal_message = terminal.message().clone();
        let terminal_write = terminal
            .into_t12_write(run_id.clone(), turn_id.clone(), true)
            .expect("terminal write with context");

        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message_in_turn(
                            "message_start",
                            assistant_id,
                            &terminal_message,
                            Some(run_id.clone()),
                            Some(turn_id.clone()),
                        )
                        .expect("assistant MessageStart"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: command_id.to_owned(),
                        run_id: run_id.clone(),
                        expected: RunPhase::UserCommitted,
                        next: RunPhase::AssistantStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("open assistant attempt");

        writer
            .apply(EventBatch {
                writes: vec![
                    terminal_write,
                    EventWrite {
                        event: Some(
                            DurableEvent::turn_end(
                                run_id.clone(),
                                turn_id.clone(),
                                terminal_message,
                                Vec::new(),
                            )
                            .expect("TurnEnd"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(DurableEvent::agent_end(run_id.clone()).expect("AgentEnd")),
                        projections: vec![Projection::CommandApplied {
                            command_id: command_id.to_owned(),
                            command_seq: 1,
                            run_id: Some(run_id),
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist terminal with provider context");

        let rows = sqlx::query(
            "SELECT id, message_id, message_seq, item_ordinal, wire_item_index, kind,
                    coverage_through_seq, context_fingerprint, eviction_tokens
             FROM provider_context
             WHERE message_id = ?
             ORDER BY id",
        )
        .bind(assistant_id)
        .fetch_all(store.pool())
        .await
        .expect("fetch provider context rows");
        assert_eq!(rows.len(), 2, "expected two provider-context records");

        for row in &rows {
            let ordinal: i64 = row.get("item_ordinal");
            assert!(ordinal >= 1, "item_ordinal must be positive");
        }

        let kinds: Vec<String> = rows
            .iter()
            .map(|row| row.get::<String, _>("kind"))
            .collect();
        assert!(kinds.contains(&"encrypted_reasoning".to_owned()));
        assert!(kinds.contains(&"open_ai_compacted_window".to_owned()));

        let reasoning_row = rows
            .iter()
            .find(|row| row.get::<String, _>("kind") == "encrypted_reasoning")
            .expect("reasoning row");
        let reasoning_eviction: i64 = reasoning_row.get("eviction_tokens");
        assert!(
            reasoning_eviction > 0,
            "encrypted reasoning must pay eviction tokens"
        );

        let window_row = rows
            .iter()
            .find(|row| row.get::<String, _>("kind") == "open_ai_compacted_window")
            .expect("window row");
        let window_eviction: i64 = window_row.get("eviction_tokens");
        assert_eq!(
            window_eviction, 0,
            "compaction window has zero eviction tokens"
        );
        assert_eq!(
            window_row
                .get::<Option<String>, _>("context_fingerprint")
                .as_deref(),
            Some("fp-1")
        );
        assert_eq!(
            window_row.get::<Option<i64>, _>("coverage_through_seq"),
            Some(1)
        );

        let message_seq: i64 = rows[0].get("message_seq");
        assert!(
            message_seq > 0,
            "provider context must be bound to message seq"
        );
    }

    #[cfg(not(unix))]
    #[test]
    fn abrupt_subprocess_failpoints_are_unavailable_without_process_exit_semantics() {
        eprintln!(
            "T12 abrupt transaction acceptance is Unix-only because this target has no _exit/SIGKILL-equivalent harness"
        );
    }

    #[tokio::test]
    async fn require_exact_live_owner_turn_rejects_multiple_applying_owners() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let owner = "00000000-0000-4000-8000-000000000001";
        let classified = "00000000-0000-4000-8000-000000000002";
        writer
            .persist_inbound(&user_command(1, owner, "owner one"))
            .await
            .expect("persist owner");
        writer
            .persist_inbound(&user_command(2, classified, "classified command"))
            .await
            .expect("persist classified command");
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='idle_run', run_id='run-1',
                 turn_id='turn-1', run_phase='user_started'
             WHERE command_id = ?",
        )
        .bind(owner)
        .execute(store.pool())
        .await
        .expect("mark owner applying");
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='idle_run', run_id='run-1',
                 turn_id='turn-1', run_phase='classified'
             WHERE command_id = ?",
        )
        .bind(classified)
        .execute(store.pool())
        .await
        .expect("mark classified applying");

        let prepared = vec![super::PreparedWrite {
            event: None,
            projections: vec![super::PreparedProjection::Plain(
                super::Projection::RunPhase {
                    command_id: classified.to_owned(),
                    run_id: "run-1".to_owned(),
                    expected: super::RunPhase::Classified,
                    next: super::RunPhase::UserStarted,
                },
            )],
        }];

        let mut transaction = store.pool().begin().await.expect("begin transaction");
        let error = super::require_exact_live_owner_turn(
            &mut transaction,
            &prepared,
            "run-1",
            "turn-1",
            "test",
            super::OwnerHandoffAccounting::Account,
        )
        .await
        .expect_err("multiple applying owners must fail closed");
        assert!(format!("{error:#}").contains("exactly one live owner"));
    }

    #[tokio::test]
    async fn tool_execution_skip_user_steer_cancelled_accepts_post_abort_phases() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let owner_id = "00000000-0000-4000-8000-000000000001";
        writer
            .persist_inbound(&user_command(1, owner_id, "original owner"))
            .await
            .expect("persist original owner");
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='idle_run', run_id='run-1',
                 turn_id='turn-1', run_phase='cancel_requested'
             WHERE command_id = ?",
        )
        .bind(owner_id)
        .execute(store.pool())
        .await
        .expect("seed owner in cancel_requested");
        let mut transaction = store.pool().begin().await.expect("begin transaction");

        super::apply_tool_mutation(
            &mut transaction,
            super::ToolExecutionMutation::Skip {
                tool_call_id: "tool-1".to_owned(),
                command_id: owner_id.to_owned(),
                run_id: "run-1".to_owned(),
                turn_id: "turn-1".to_owned(),
                executor_generation: test_process_generation(1),
                idempotency_key: "idem-skip-1".to_owned(),
                error_code: "user_steer_cancelled",
            },
        )
        .await
        .expect("user_steer_cancelled must resolve after abort moves owner to cancel_requested");

        let row = sqlx::query(
            "SELECT command_id, run_id, state, error_code FROM tool_executions WHERE tool_call_id = ?",
        )
        .bind("tool-1")
        .fetch_one(&mut *transaction)
        .await
        .expect("skip row exists");
        assert_eq!(row.try_get::<String, _>("command_id").unwrap(), owner_id);
        assert_eq!(row.try_get::<String, _>("run_id").unwrap(), "run-1");
        assert_eq!(row.try_get::<String, _>("state").unwrap(), "not_started");
        assert_eq!(
            row.try_get::<Option<String>, _>("error_code")
                .unwrap()
                .as_deref(),
            Some("user_steer_cancelled")
        );
    }

    #[tokio::test]
    async fn tool_execution_skip_length_guard_requires_assistant_started() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let owner_id = "00000000-0000-4000-8000-000000000001";
        writer
            .persist_inbound(&user_command(1, owner_id, "original owner"))
            .await
            .expect("persist original owner");
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='idle_run', run_id='run-1',
                 turn_id='turn-1', run_phase='cancel_requested'
             WHERE command_id = ?",
        )
        .bind(owner_id)
        .execute(store.pool())
        .await
        .expect("seed owner in cancel_requested");
        let mut transaction = store.pool().begin().await.expect("begin transaction");

        let error = super::apply_tool_mutation(
            &mut transaction,
            super::ToolExecutionMutation::Skip {
                tool_call_id: "tool-1".to_owned(),
                command_id: owner_id.to_owned(),
                run_id: "run-1".to_owned(),
                turn_id: "turn-1".to_owned(),
                executor_generation: test_process_generation(1),
                idempotency_key: "idem-skip-1".to_owned(),
                error_code: "length_guard",
            },
        )
        .await
        .expect_err("length_guard must not match cancel_requested owner");
        assert!(format!("{error:#}").contains("ToolExecutionSkip"));
    }

    #[tokio::test]
    async fn memory_maintenance_event_is_persisted_and_recovered() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let batch = EventBatch {
            writes: vec![EventWrite {
                event: Some(
                    DurableEvent::new(
                        &json!({"type": "memory_maintenance", "kind": "compact_applied"}),
                    )
                    .expect("typed memory maintenance event"),
                ),
                projections: Vec::new(),
            }],
            injected_commands: Vec::new(),
        };
        let seqs = writer.apply(batch).await.expect("apply memory maintenance");
        assert_eq!(seqs, vec![1]);
        SuffixRecovery::plan(&store)
            .await
            .expect("recovery accepts memory maintenance");
    }

    #[tokio::test]
    async fn error_message_end_with_append_to_l0_false_rejects_provider_context() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let command_id = "00000000-0000-4000-8000-000000000050";
        let injected = classified_injection(&writer, 1, command_id, "ignored", "error").await;
        writer
            .apply(EventBatch {
                writes: injection_writes(command_id, "ignored", "error"),
                injected_commands: vec![injected],
            })
            .await
            .expect("open assistant owner turn");

        let run_id = format!("run-{command_id}");
        let turn_id = format!("turn-{command_id}");
        let error_message = assistant_message(StopReason::Error);
        let message_id = "assistant-error-with-context";
        let fragment = ProviderContextFragment {
            wire_item_index: Some(0),
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::OpenAiChatCompletions,
                item: json!({"text": "opaque reasoning"}),
            },
        };
        let footprint = ProviderContextEvictionEstimate::from_payload(&fragment.payload).tokens;

        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message_in_turn(
                            "message_start",
                            message_id,
                            &error_message,
                            Some(run_id.clone()),
                            Some(turn_id.clone()),
                        )
                        .expect("error MessageStart"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: command_id.to_owned(),
                        run_id: run_id.clone(),
                        expected: RunPhase::UserCommitted,
                        next: RunPhase::AssistantStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist assistant start");

        let error = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message_in_turn(
                            "message_end",
                            message_id,
                            &error_message,
                            Some(run_id.clone()),
                            Some(turn_id.clone()),
                        )
                        .expect("error MessageEnd"),
                    ),
                    projections: vec![Projection::MessageEnd {
                        message_id: message_id.to_owned(),
                        role: "assistant",
                        message: error_message,
                        append_to_l0: false,
                        provider_context: vec![fragment],
                        eviction_footprint_tokens: footprint,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("error MessageEnd must not carry provider_context");
        assert!(error.to_string().contains("append_to_l0=false"));
    }

    #[tokio::test]
    async fn message_end_rejects_mismatched_eviction_footprint_tokens() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let command_id = "00000000-0000-4000-8000-000000000051";
        let injected = classified_injection(&writer, 1, command_id, "ignored", "hello").await;
        writer
            .apply(EventBatch {
                writes: injection_writes(command_id, "ignored", "hello"),
                injected_commands: vec![injected],
            })
            .await
            .expect("open assistant owner turn");

        let run_id = format!("run-{command_id}");
        let turn_id = format!("turn-{command_id}");
        let message = assistant_message(StopReason::Stop);
        let message_id = "assistant-stop-with-context";
        let fragment = ProviderContextFragment {
            wire_item_index: Some(0),
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::OpenAiChatCompletions,
                item: json!({"text": "opaque reasoning"}),
            },
        };
        let footprint = ProviderContextEvictionEstimate::from_payload(&fragment.payload).tokens;

        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message_in_turn(
                            "message_start",
                            message_id,
                            &message,
                            Some(run_id.clone()),
                            Some(turn_id.clone()),
                        )
                        .expect("assistant MessageStart"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: command_id.to_owned(),
                        run_id: run_id.clone(),
                        expected: RunPhase::UserCommitted,
                        next: RunPhase::AssistantStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist assistant start");

        let error = writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message_in_turn(
                            "message_end",
                            message_id,
                            &message,
                            Some(run_id.clone()),
                            Some(turn_id.clone()),
                        )
                        .expect("assistant MessageEnd"),
                    ),
                    projections: vec![Projection::MessageEnd {
                        message_id: message_id.to_owned(),
                        role: "assistant",
                        message,
                        append_to_l0: true,
                        provider_context: vec![fragment],
                        eviction_footprint_tokens: footprint + 1,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect_err("MessageEnd must reject mismatched eviction footprint");
        assert!(
            error
                .to_string()
                .contains("eviction_footprint_tokens mismatch")
        );
    }
}
