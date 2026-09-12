//! Checked runtime identities.
//!
//! This module only validates caller-supplied identities. Allocation,
//! issuance, persistence, lease acquisition, and bootstrap belong to later
//! production-runtime tasks.

use std::{fmt, str::FromStr};

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::{Uuid, Variant, Version};

pub const MAX_PROCESS_GENERATION: u64 = i64::MAX as u64;
pub const MAX_OPAQUE_ID_BYTES: usize = 128;
pub const DIRECT_CHAT_PROVENANCE_VERSION: u8 = 1;
pub const MAX_PROVENANCE_ID_BYTES: usize = 256;

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum RuntimeContractError {
    #[error("personality agent id must be a UUID")]
    PersonalityAgentIdNotUuid,
    #[error("personality agent id must use UUID version 7")]
    PersonalityAgentIdWrongVersion,
    #[error("personality agent id must use the RFC 4122 variant")]
    PersonalityAgentIdWrongVariant,
    #[error("personality agent id must use exact lowercase hyphenated UUID text")]
    PersonalityAgentIdNonCanonical,
    #[error("incoming provenance version, source, actor, or source fields are inconsistent")]
    InvalidIncomingProvenance,
    #[error("{kind} must contain 1..={MAX_PROVENANCE_ID_BYTES} bytes")]
    InvalidProvenanceIdentity { kind: &'static str },
    #[error("direct-chat provenance target personality agent does not match the private store")]
    DirectChatProvenanceTargetMismatch,
    #[error("process generation must be in 0..={MAX_PROCESS_GENERATION}")]
    ProcessGenerationOutOfRange,
    #[error("SQLite process generation must not be negative: {0}")]
    NegativeSqliteProcessGeneration(i64),
    #[error("{kind} must contain 1..={MAX_OPAQUE_ID_BYTES} bytes")]
    InvalidOpaqueIdentity { kind: &'static str },
    #[error("RPC personality agent, generation, or boot nonce mismatch")]
    RpcIdentityMismatch,
    #[error("process generation lease personality agent, generation, or opaque identity mismatch")]
    ProcessGenerationLeaseMismatch,
    #[error(
        "generation recovery fence personality agent, lease/generation, or opaque identity mismatch"
    )]
    GenerationRecoveryFenceMismatch,
}

/// Stable global identity of one personality agent.
///
/// Parsing rejects every textual representation except the exact lowercase
/// hyphenated RFC UUIDv7 form. Callers therefore cannot create multiple
/// persistent, authorization, or AAD identities by normalizing raw input.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PersonalityAgentId {
    value: Uuid,
    canonical: String,
}

impl PersonalityAgentId {
    pub fn parse(value: &str) -> Result<Self, RuntimeContractError> {
        Self::from_str(value)
    }

    pub fn as_str(&self) -> &str {
        &self.canonical
    }

    pub const fn as_uuid(&self) -> &Uuid {
        &self.value
    }
}

impl FromStr for PersonalityAgentId {
    type Err = RuntimeContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let uuid =
            Uuid::parse_str(value).map_err(|_| RuntimeContractError::PersonalityAgentIdNotUuid)?;
        if uuid.get_version() != Some(Version::SortRand) {
            return Err(RuntimeContractError::PersonalityAgentIdWrongVersion);
        }
        if uuid.get_variant() != Variant::RFC4122 {
            return Err(RuntimeContractError::PersonalityAgentIdWrongVariant);
        }
        let canonical = uuid.hyphenated().to_string();
        if value != canonical {
            return Err(RuntimeContractError::PersonalityAgentIdNonCanonical);
        }
        Ok(Self {
            value: uuid,
            canonical,
        })
    }
}

impl fmt::Display for PersonalityAgentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for PersonalityAgentId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for PersonalityAgentId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputAudience {
    DirectChat,
    Secretary,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "IncomingProvenanceWire")]
pub struct IncomingProvenance {
    version: u8,
    tenant_id: String,
    personality_agent_id: PersonalityAgentId,
    actor: IncomingActor,
    source: IncomingSource,
}

impl IncomingProvenance {
    pub fn new(
        tenant_id: impl Into<String>,
        personality_agent_id: PersonalityAgentId,
        human_principal_id: impl Into<String>,
    ) -> Result<Self, RuntimeContractError> {
        let value = Self {
            version: DIRECT_CHAT_PROVENANCE_VERSION,
            tenant_id: tenant_id.into(),
            personality_agent_id,
            actor: IncomingActor {
                kind: ActorKind::Human,
                principal_id: human_principal_id.into(),
                display_name: None,
            },
            source: IncomingSource::DirectChat {},
        };
        value.validate(&value.personality_agent_id)?;
        Ok(value)
    }
    pub const fn version(&self) -> u8 {
        self.version
    }
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
    pub const fn personality_agent_id(&self) -> &PersonalityAgentId {
        &self.personality_agent_id
    }
    pub const fn actor(&self) -> &IncomingActor {
        &self.actor
    }
    pub const fn source(&self) -> &IncomingSource {
        &self.source
    }
    pub fn messaging_source(&self) -> Option<&MessagingSource> {
        match &self.source {
            IncomingSource::Messaging(source) => Some(source.as_ref()),
            _ => None,
        }
    }
    pub fn is_external(&self) -> bool {
        !matches!(self.source, IncomingSource::DirectChat {})
    }
    pub fn output_audience(&self) -> OutputAudience {
        if self.is_external() {
            OutputAudience::Secretary
        } else {
            OutputAudience::DirectChat
        }
    }
    pub fn authenticated_direct_chat_human(&self) -> Option<&str> {
        if self.version == 1 && !self.is_external() && self.actor.kind == ActorKind::Human {
            Some(&self.actor.principal_id)
        } else {
            None
        }
    }
    pub fn validate(
        &self,
        expected_target: &PersonalityAgentId,
    ) -> Result<(), RuntimeContractError> {
        validate_provenance_identity(self.tenant_id.clone(), "tenant id")?;
        validate_provenance_identity(self.actor.principal_id.clone(), "actor principal id")?;
        if &self.personality_agent_id != expected_target {
            return Err(RuntimeContractError::DirectChatProvenanceTargetMismatch);
        }
        match &self.source {
            IncomingSource::DirectChat {}
                if self.version == 1
                    && self.actor.kind == ActorKind::Human
                    && self.actor.display_name.is_none() =>
            {
                Ok(())
            }
            IncomingSource::ApprovalOperation(source) if self.version == 2 => {
                if self.actor.kind != ActorKind::PersonalityAgent
                    || self.actor.principal_id != self.personality_agent_id.as_str()
                {
                    return Err(RuntimeContractError::InvalidIncomingProvenance);
                }
                source.validate()
            }
            IncomingSource::WorkspaceOperation(source) if self.version == 2 => {
                if self.actor.kind != ActorKind::PersonalityAgent
                    || self.actor.principal_id != self.personality_agent_id.as_str()
                {
                    return Err(RuntimeContractError::InvalidIncomingProvenance);
                }
                source.validate()
            }
            IncomingSource::Feedback(source) if self.version == 2 => source.validate(),
            IncomingSource::Messaging(source) if self.version == 2 => {
                source.validate()?;
                if source.kind == MessagingEventKind::ReplyLaterDue
                    && (self.actor.kind != ActorKind::PersonalityAgent
                        || self.actor.principal_id != self.personality_agent_id.as_str())
                {
                    return Err(RuntimeContractError::InvalidIncomingProvenance);
                }
                Ok(())
            }
            _ => Err(RuntimeContractError::InvalidIncomingProvenance),
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IncomingProvenanceWire {
    version: u8,
    tenant_id: String,
    personality_agent_id: PersonalityAgentId,
    actor: IncomingActor,
    source: IncomingSource,
}
impl TryFrom<IncomingProvenanceWire> for IncomingProvenance {
    type Error = RuntimeContractError;
    fn try_from(wire: IncomingProvenanceWire) -> Result<Self, Self::Error> {
        let value = Self {
            version: wire.version,
            tenant_id: wire.tenant_id,
            personality_agent_id: wire.personality_agent_id,
            actor: wire.actor,
            source: wire.source,
        };
        value.validate(&value.personality_agent_id)?;
        Ok(value)
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IncomingActor {
    kind: ActorKind,
    principal_id: String,
    #[serde(
        default,
        deserialize_with = "present_string",
        skip_serializing_if = "Option::is_none"
    )]
    display_name: Option<String>,
}
impl IncomingActor {
    pub const fn kind(&self) -> ActorKind {
        self.kind
    }
    pub fn principal_id(&self) -> &str {
        &self.principal_id
    }
    pub fn display_name(&self) -> Option<&str> {
        self.display_name.as_deref()
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorKind {
    Human,
    PersonalityAgent,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "surface", rename_all = "snake_case", deny_unknown_fields)]
pub enum IncomingSource {
    DirectChat {},
    // Source details are uncommon but IncomingProvenance is carried through
    // every command/message and their async futures, including DirectChat.
    Messaging(Box<MessagingSource>),
    WorkspaceOperation(Box<WorkspaceOperationSource>),
    ApprovalOperation(Box<ApprovalOperationSource>),
    Feedback(Box<FeedbackSource>),
}
/// An observed operation outcome, never a human instruction or a second tool output.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApprovalOperationSource {
    pub operation_id: String,
    pub tool_call_id: String,
    pub status: ApprovalOperationStatus,
    pub executed: Option<bool>,
    pub result: serde_json::Value,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalOperationStatus {
    Succeeded,
    Failed,
    Denied,
    Expired,
    Cancelled,
    Indeterminate,
}
impl ApprovalOperationSource {
    pub(crate) fn validate(&self) -> Result<(), RuntimeContractError> {
        let result: crate::provider::types::ToolResultMessage =
            serde_json::from_value(self.result.clone())
                .map_err(|_| RuntimeContractError::InvalidIncomingProvenance)?;
        if self.operation_id.is_empty()
            || self.tool_call_id.is_empty()
            || result.tool_call_id != self.tool_call_id
            || self.executed
                != match self.status {
                    ApprovalOperationStatus::Succeeded | ApprovalOperationStatus::Failed => {
                        Some(true)
                    }
                    ApprovalOperationStatus::Indeterminate => None,
                    _ => Some(false),
                }
            || result.is_error != (self.status != ApprovalOperationStatus::Succeeded)
        {
            return Err(RuntimeContractError::InvalidIncomingProvenance);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackSource {
    pub kind: FeedbackEventKind,
    pub event_id: String,
    pub thread_id: String,
    pub title: String,
    pub revision: u64,
    pub occurred_at: String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackEventKind {
    FeedbackCreated,
    FeedbackReply,
    FeedbackStatus,
}
impl FeedbackSource {
    fn validate(&self) -> Result<(), RuntimeContractError> {
        for id in [&self.event_id, &self.thread_id] {
            let parsed =
                Uuid::parse_str(id).map_err(|_| RuntimeContractError::InvalidIncomingProvenance)?;
            if parsed.hyphenated().to_string() != *id
                || parsed.get_version_num() != 7
                || parsed.get_variant() != uuid::Variant::RFC4122
            {
                return Err(RuntimeContractError::InvalidIncomingProvenance);
            }
        }
        if self.title.trim().is_empty()
            || self.title.chars().count() > 160
            || self.revision == 0
            || self.revision > 9_007_199_254_740_991
            || chrono::DateTime::parse_from_rfc3339(&self.occurred_at).is_err()
        {
            return Err(RuntimeContractError::InvalidIncomingProvenance);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceOperationSource {
    pub kind: WorkspaceOperationKind,
    pub event_id: String,
    pub operation_id: String,
    pub originating_tool_call_id: String,
    pub occurred_at: String,
    pub result: WorkspaceOperationResult,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceOperationKind {
    ProcessCompleted,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceOperationResult {
    pub state: WorkspaceOperationState,
    #[serde(deserialize_with = "required_nullable_exit_code")]
    pub exit_code: Option<i64>,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub output_truncated: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceOperationState {
    Succeeded,
    Failed,
    Cancelled,
    Indeterminate,
}
fn required_nullable_exit_code<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<i64>, D::Error> {
    Option::<i64>::deserialize(deserializer)
}
impl WorkspaceOperationSource {
    fn validate(&self) -> Result<(), RuntimeContractError> {
        let event_id = Uuid::parse_str(&self.event_id)
            .map_err(|_| RuntimeContractError::InvalidIncomingProvenance)?;
        if event_id.hyphenated().to_string() != self.event_id
            || event_id.get_version_num() != 7
            || event_id.get_variant() != uuid::Variant::RFC4122
        {
            return Err(RuntimeContractError::InvalidIncomingProvenance);
        }
        if self.operation_id.len() != 64
            || !self
                .operation_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || self.originating_tool_call_id.is_empty()
            || self.result.stdout_bytes > 9_007_199_254_740_991
            || self.result.stderr_bytes > 9_007_199_254_740_991
            || self
                .result
                .exit_code
                .is_some_and(|n| n.unsigned_abs() > 9_007_199_254_740_991)
            || chrono::DateTime::parse_from_rfc3339(&self.occurred_at).is_err()
        {
            return Err(RuntimeContractError::InvalidIncomingProvenance);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessagingSource {
    pub event_id: String,
    pub kind: MessagingEventKind,
    pub workspace_id: String,
    pub installation_id: String,
    pub authority_epoch: u64,
    pub place: MessagingPlace,
    pub message_id: String,
    #[serde(
        default,
        deserialize_with = "present_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub reply_to_message_id: Option<String>,
    pub message_revision: u64,
    pub message_seq: u64,
    pub occurred_at: String,
    #[serde(
        default,
        deserialize_with = "present_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub marker_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "present_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub due_at: Option<String>,
    #[serde(
        default,
        deserialize_with = "present_poll_vote",
        skip_serializing_if = "Option::is_none"
    )]
    pub poll_vote: Option<PollVote>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessagingEventKind {
    MessagingPollVote,
    MessagingMention,
    MessagingMessage,
    ReplyLaterDue,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessagingPlace {
    pub id: String,
    pub kind: MessagingPlaceKind,
    pub name: String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessagingPlaceKind {
    Channel,
    Thread,
    Dm,
    GroupDm,
}
fn present_string<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
    String::deserialize(deserializer).map(Some)
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollVote {
    pub poll_revision: u64,
    pub question: String,
    pub selected_options: Vec<PollVoteOption>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PollVoteOption {
    pub option_id: String,
    pub text: String,
}
fn present_poll_vote<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<PollVote>, D::Error> {
    PollVote::deserialize(deserializer).map(Some)
}
impl MessagingSource {
    fn validate(&self) -> Result<(), RuntimeContractError> {
        let fail = || RuntimeContractError::InvalidIncomingProvenance;
        for id in [
            &self.event_id,
            &self.workspace_id,
            &self.installation_id,
            &self.place.id,
            &self.message_id,
        ] {
            let uuid = Uuid::parse_str(id).map_err(|_| fail())?;
            if uuid.hyphenated().to_string() != *id {
                return Err(fail());
            }
        }
        if [
            self.authority_epoch,
            self.message_revision,
            self.message_seq,
        ]
        .iter()
        .any(|n| *n == 0 || *n > 9_007_199_254_740_991)
        {
            return Err(fail());
        }
        chrono::DateTime::parse_from_rfc3339(&self.occurred_at).map_err(|_| fail())?;
        if let Some(id) = &self.reply_to_message_id {
            if !matches!(
                self.kind,
                MessagingEventKind::MessagingMessage | MessagingEventKind::MessagingMention
            ) || Uuid::parse_str(id)
                .map_err(|_| fail())?
                .hyphenated()
                .to_string()
                != *id
            {
                return Err(fail());
            }
        }
        if self.kind != MessagingEventKind::MessagingPollVote && self.poll_vote.is_some() {
            return Err(fail());
        }
        match self.kind {
            MessagingEventKind::MessagingPollVote
                if self.marker_id.is_none() && self.due_at.is_none() =>
            {
                let vote = self.poll_vote.as_ref().ok_or_else(fail)?;
                if vote.poll_revision == 0 || vote.poll_revision > 9_007_199_254_740_991 {
                    return Err(fail());
                }
                let mut ids = std::collections::HashSet::new();
                for option in &vote.selected_options {
                    if !ids.insert(&option.option_id) {
                        return Err(fail());
                    }
                    if Uuid::parse_str(&option.option_id)
                        .map_err(|_| fail())?
                        .hyphenated()
                        .to_string()
                        != option.option_id
                    {
                        return Err(fail());
                    }
                }
            }
            MessagingEventKind::MessagingMention | MessagingEventKind::MessagingMessage
                if self.marker_id.is_none() && self.due_at.is_none() =>
            {
                ()
            }
            MessagingEventKind::ReplyLaterDue => {
                let id = self.marker_id.as_deref().ok_or_else(fail)?;
                if Uuid::parse_str(id)
                    .map_err(|_| fail())?
                    .hyphenated()
                    .to_string()
                    != id
                {
                    return Err(fail());
                }
                chrono::DateTime::parse_from_rfc3339(self.due_at.as_deref().ok_or_else(fail)?)
                    .map_err(|_| fail())?;
            }
            _ => return Err(fail()),
        }
        Ok(())
    }
}

fn validate_provenance_identity(
    value: String,
    kind: &'static str,
) -> Result<String, RuntimeContractError> {
    let bytes = value.as_bytes();
    let valid_first = bytes.first().is_some_and(u8::is_ascii_alphanumeric);
    let valid_rest = bytes.iter().skip(1).all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'@' | b'/' | b'-')
    });
    if bytes.len() > MAX_PROVENANCE_ID_BYTES || !valid_first || !valid_rest {
        return Err(RuntimeContractError::InvalidProvenanceIdentity { kind });
    }
    Ok(value)
}

/// A process generation that is exactly representable by SQLite `INTEGER`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessGeneration(i64);

impl ProcessGeneration {
    pub const MIN: Self = Self(0);
    pub const MAX: Self = Self(i64::MAX);

    pub fn from_wire(value: u64) -> Result<Self, RuntimeContractError> {
        Self::try_from(value)
    }

    pub fn from_sqlite(value: i64) -> Result<Self, RuntimeContractError> {
        Self::try_from(value)
    }

    pub fn as_u64(self) -> u64 {
        u64::try_from(self.0).expect("validated process generation is nonnegative")
    }

    pub const fn as_i64(self) -> i64 {
        self.0
    }

    pub fn to_wire(self) -> u64 {
        self.as_u64()
    }
}

impl TryFrom<u64> for ProcessGeneration {
    type Error = RuntimeContractError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        i64::try_from(value)
            .map(Self)
            .map_err(|_| RuntimeContractError::ProcessGenerationOutOfRange)
    }
}

impl TryFrom<i64> for ProcessGeneration {
    type Error = RuntimeContractError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value < 0 {
            return Err(RuntimeContractError::NegativeSqliteProcessGeneration(value));
        }
        Ok(Self(value))
    }
}

impl From<ProcessGeneration> for u64 {
    fn from(value: ProcessGeneration) -> Self {
        value.as_u64()
    }
}

impl From<ProcessGeneration> for i64 {
    fn from(value: ProcessGeneration) -> Self {
        value.as_i64()
    }
}

impl fmt::Display for ProcessGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for ProcessGeneration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.as_u64())
    }
}

impl<'de> Deserialize<'de> for ProcessGeneration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ProcessGenerationVisitor;

        impl<'de> Visitor<'de> for ProcessGenerationVisitor {
            type Value = ProcessGeneration;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(
                    formatter,
                    "a process generation in 0..={MAX_PROCESS_GENERATION}"
                )
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                ProcessGeneration::from_sqlite(value).map_err(|e| E::custom(e.to_string()))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                ProcessGeneration::from_wire(value).map_err(|e| E::custom(e.to_string()))
            }
        }

        deserializer.deserialize_u64(ProcessGenerationVisitor)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RpcBootNonce(String);

impl RpcBootNonce {
    pub fn new(value: impl Into<String>) -> Result<Self, RuntimeContractError> {
        Ok(Self(validate_opaque(value.into(), "RPC nonce")?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RpcIdentity {
    personality_agent_id: PersonalityAgentId,
    generation: ProcessGeneration,
    nonce: RpcBootNonce,
}

impl RpcIdentity {
    pub const fn new(
        personality_agent_id: PersonalityAgentId,
        generation: ProcessGeneration,
        nonce: RpcBootNonce,
    ) -> Self {
        Self {
            personality_agent_id,
            generation,
            nonce,
        }
    }

    pub fn from_wire(
        personality_agent_id: impl AsRef<str>,
        generation: u64,
        nonce: impl Into<String>,
    ) -> Result<Self, RuntimeContractError> {
        Ok(Self::new(
            PersonalityAgentId::parse(personality_agent_id.as_ref())?,
            ProcessGeneration::from_wire(generation)?,
            RpcBootNonce::new(nonce)?,
        ))
    }

    pub const fn personality_agent_id(&self) -> &PersonalityAgentId {
        &self.personality_agent_id
    }

    pub const fn generation(&self) -> ProcessGeneration {
        self.generation
    }

    pub fn nonce(&self) -> &RpcBootNonce {
        &self.nonce
    }

    pub fn validate_wire(
        &self,
        personality_agent_id: &str,
        generation: u64,
        nonce: &str,
    ) -> Result<(), RuntimeContractError> {
        let personality_agent_id = PersonalityAgentId::parse(personality_agent_id)?;
        let generation = ProcessGeneration::from_wire(generation)?;
        let nonce = RpcBootNonce::new(nonce)?;
        if personality_agent_id != self.personality_agent_id
            || generation != self.generation
            || nonce != self.nonce
        {
            return Err(RuntimeContractError::RpcIdentityMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessGenerationLease {
    personality_agent_id: PersonalityAgentId,
    generation: ProcessGeneration,
    lease_id: String,
}

impl ProcessGenerationLease {
    pub fn new(
        personality_agent_id: PersonalityAgentId,
        generation: ProcessGeneration,
        lease_id: impl Into<String>,
    ) -> Result<Self, RuntimeContractError> {
        Ok(Self {
            personality_agent_id,
            generation,
            lease_id: validate_opaque(lease_id.into(), "process generation lease identity")?,
        })
    }

    pub const fn personality_agent_id(&self) -> &PersonalityAgentId {
        &self.personality_agent_id
    }

    pub const fn generation(&self) -> ProcessGeneration {
        self.generation
    }

    pub fn lease_id(&self) -> &str {
        &self.lease_id
    }

    pub fn validate_exact(
        &self,
        personality_agent_id: &PersonalityAgentId,
        generation: ProcessGeneration,
        lease_id: &str,
    ) -> Result<(), RuntimeContractError> {
        let lease_id = validate_opaque(lease_id.to_owned(), "process generation lease identity")?;
        if personality_agent_id != &self.personality_agent_id
            || generation != self.generation
            || lease_id != self.lease_id
        {
            return Err(RuntimeContractError::ProcessGenerationLeaseMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenerationRecoveryFence {
    personality_agent_id: PersonalityAgentId,
    generation: ProcessGeneration,
    lease_id: String,
    fence_id: String,
}

impl GenerationRecoveryFence {
    pub fn new(
        lease: &ProcessGenerationLease,
        fence_id: impl Into<String>,
    ) -> Result<Self, RuntimeContractError> {
        Ok(Self {
            personality_agent_id: lease.personality_agent_id.clone(),
            generation: lease.generation,
            lease_id: lease.lease_id.clone(),
            fence_id: validate_opaque(fence_id.into(), "generation recovery fence identity")?,
        })
    }

    pub const fn personality_agent_id(&self) -> &PersonalityAgentId {
        &self.personality_agent_id
    }

    pub const fn generation(&self) -> ProcessGeneration {
        self.generation
    }

    pub fn lease_id(&self) -> &str {
        &self.lease_id
    }

    pub fn fence_id(&self) -> &str {
        &self.fence_id
    }

    pub fn validate_exact(
        &self,
        lease: &ProcessGenerationLease,
        fence_id: &str,
    ) -> Result<(), RuntimeContractError> {
        let fence_id = validate_opaque(fence_id.to_owned(), "generation recovery fence identity")?;
        if self.personality_agent_id != lease.personality_agent_id
            || self.generation != lease.generation
            || self.lease_id != lease.lease_id
            || self.fence_id != fence_id
        {
            return Err(RuntimeContractError::GenerationRecoveryFenceMismatch);
        }
        Ok(())
    }
}

fn validate_opaque(value: String, kind: &'static str) -> Result<String, RuntimeContractError> {
    if value.is_empty() || value.len() > MAX_OPAQUE_ID_BYTES {
        return Err(RuntimeContractError::InvalidOpaqueIdentity { kind });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {

    #[test]
    fn feedback_provenance_roundtrip_preserves_source_without_human_authority() {
        let value = serde_json::json!({"version":2,"tenant_id":"test","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab",
          "actor":{"kind":"human","principal_id":"author","display_name":"開発者"},
          "source":{"surface":"feedback","kind":"feedback_reply","event_id":"018f47a2-9b3c-7def-8abc-0123456789ac","thread_id":"018f47a2-9b3c-7def-8abc-0123456789ad","title":"通知について","revision":2,"occurred_at":"2026-09-11T10:00:00Z"}});
        let p: IncomingProvenance = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(&p).unwrap(), value);
        assert_eq!(p.output_audience(), OutputAudience::Secretary);
        assert!(p.authenticated_direct_chat_human().is_none());
        for (field, bad) in [
            ("revision", serde_json::json!(0)),
            ("thread_id", serde_json::json!("bad")),
            ("title", serde_json::json!("  ")),
            ("kind", serde_json::json!("messaging_message")),
            ("occurred_at", serde_json::json!("yesterday")),
            ("workspace_id", serde_json::json!("extra")),
        ] {
            let mut invalid = value.clone();
            invalid["source"][field] = bad;
            assert!(
                serde_json::from_value::<IncomingProvenance>(invalid).is_err(),
                "accepted {field}"
            );
        }
    }
    use super::*;

    const PAID: &str = "0198f0f4-9b72-7000-8000-000000000001";

    #[test]
    fn personality_agent_id_accepts_only_exact_canonical_rfc_uuid_v7() {
        let paid = PersonalityAgentId::from_str(PAID).expect("canonical UUIDv7");
        assert_eq!(paid.as_str(), PAID);
        assert_eq!(paid.to_string(), PAID);
        assert_eq!(
            serde_json::to_string(&paid).expect("serialize"),
            format!("\"{PAID}\"")
        );
        assert_eq!(
            serde_json::from_str::<PersonalityAgentId>(&format!("\"{PAID}\""))
                .expect("deserialize"),
            paid
        );
    }

    #[test]
    fn personality_agent_id_rejects_wrong_version_variant_and_text_forms() {
        let uppercase = PAID.to_ascii_uppercase();
        let compact = PAID.replace('-', "");
        let braced = format!("{{{PAID}}}");
        let padded = format!(" {PAID} ");
        for value in [
            uppercase.as_str(),
            compact.as_str(),
            braced.as_str(),
            padded.as_str(),
            "0198f0f4-9b72-4000-8000-000000000001",
            "0198f0f4-9b72-7000-c000-000000000001",
            "not-a-uuid",
        ] {
            assert!(
                PersonalityAgentId::from_str(value).is_err(),
                "unexpectedly accepted {value:?}"
            );
        }
    }

    #[test]
    fn direct_chat_provenance_is_closed_and_binds_authenticated_dimensions() {
        let paid = PersonalityAgentId::parse(PAID).unwrap();
        let provenance =
            IncomingProvenance::new("tenant-at-admission", paid.clone(), "human-123").unwrap();
        provenance.validate(&paid).unwrap();
        assert_eq!(provenance.version(), 1);
        assert_eq!(provenance.tenant_id(), "tenant-at-admission");
        assert_eq!(provenance.personality_agent_id(), &paid);
        assert_eq!(provenance.actor().kind(), ActorKind::Human);
        assert_eq!(provenance.actor().principal_id(), "human-123");
        assert!(matches!(provenance.source(), IncomingSource::DirectChat {}));
        assert_eq!(
            serde_json::to_value(&provenance).unwrap(),
            serde_json::json!({
                "version": 1,
                "tenant_id": "tenant-at-admission",
                "personality_agent_id": PAID,
                "actor": {"kind": "human", "principal_id": "human-123"},
                "source": {"surface": "direct_chat"}
            })
        );
    }

    #[test]
    fn direct_chat_provenance_rejects_unknown_shape_and_target_mismatch() {
        let wrong_target =
            PersonalityAgentId::parse("0198f0f4-9b72-7000-8000-000000000002").unwrap();
        let provenance = IncomingProvenance::new(
            "tenant-at-admission",
            PersonalityAgentId::parse(PAID).unwrap(),
            "human-123",
        )
        .unwrap();
        assert_eq!(
            provenance.validate(&wrong_target),
            Err(RuntimeContractError::DirectChatProvenanceTargetMismatch)
        );
        for raw in [
            format!(
                r#"{{"version":2,"tenant_id":"tenant","personality_agent_id":"{PAID}","actor":{{"kind":"human","principal_id":"human"}},"source":{{"surface":"direct_chat"}}}}"#
            ),
            format!(
                r#"{{"version":1,"tenant_id":"tenant","personality_agent_id":"{PAID}","actor":{{"kind":"human","principal_id":"human"}},"source":{{"surface":"direct_chat"}},"unknown":true}}"#
            ),
        ] {
            let parsed = serde_json::from_str::<IncomingProvenance>(&raw);
            assert!(parsed.is_err());
        }
    }

    #[test]
    fn direct_chat_provenance_deserialization_enforces_id_grammar_and_bounds() {
        for (tenant_id, principal_id) in [
            ("".to_owned(), "human".to_owned()),
            ("tenant".to_owned(), "".to_owned()),
            (" tenant".to_owned(), "human".to_owned()),
            ("tenant".to_owned(), "human name".to_owned()),
            ("tenant".to_owned(), "人間".to_owned()),
            ("tenant".to_owned(), "h".repeat(MAX_PROVENANCE_ID_BYTES + 1)),
        ] {
            let raw = serde_json::json!({
                "version": 1,
                "tenant_id": &tenant_id,
                "personality_agent_id": PAID,
                "actor": {"kind": "human", "principal_id": &principal_id},
                "source": {"surface": "direct_chat"}
            });
            assert!(
                serde_json::from_value::<IncomingProvenance>(raw).is_err(),
                "unexpectedly accepted tenant={tenant_id:?}, principal={principal_id:?}"
            );
        }
    }

    #[test]
    fn external_provenance_is_typed_and_never_grants_direct_human_authority() {
        let provenance = crate::gateway::test_messaging_provenance();
        assert!(provenance.is_external());
        assert_eq!(provenance.output_audience(), OutputAudience::Secretary);
        assert_eq!(provenance.authenticated_direct_chat_human(), None);
        assert_eq!(provenance.actor().display_name(), Some("Example Human"));
        let raw = serde_json::to_value(&provenance).unwrap();
        for (pointer, value) in [
            ("/version", serde_json::json!(1)),
            ("/actor/kind", serde_json::json!("system")),
            ("/actor/display_name", serde_json::Value::Null),
            ("/source/event_id", serde_json::json!("not-a-uuid")),
            ("/source/authority_epoch", serde_json::json!(0)),
            (
                "/source/message_seq",
                serde_json::json!(9_007_199_254_740_992u64),
            ),
            ("/source/occurred_at", serde_json::json!("yesterday")),
            ("/source/place/kind", serde_json::json!("unknown")),
        ] {
            let mut invalid = raw.clone();
            *invalid.pointer_mut(pointer).unwrap() = value;
            assert!(
                serde_json::from_value::<IncomingProvenance>(invalid).is_err(),
                "{pointer}"
            );
        }
        for pointer in ["", "/actor", "/source", "/source/place"] {
            let mut invalid = raw.clone();
            invalid
                .pointer_mut(pointer)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert("unknown".to_owned(), serde_json::json!(true));
            assert!(
                serde_json::from_value::<IncomingProvenance>(invalid).is_err(),
                "{pointer}"
            );
        }
        let mut dm = raw.clone();
        dm["source"]["kind"] = serde_json::json!("messaging_message");
        dm["source"]["place"]["kind"] = serde_json::json!("dm");
        let parsed = serde_json::from_value::<IncomingProvenance>(dm.clone()).unwrap();
        assert_eq!(parsed.authenticated_direct_chat_human(), None);
        assert_eq!(serde_json::to_value(parsed).unwrap(), dm);
        dm["source"]["marker_id"] = serde_json::json!("01992000-0000-7000-8000-000000000008");
        assert!(serde_json::from_value::<IncomingProvenance>(dm).is_err());
        for kind in ["messaging_message", "messaging_mention"] {
            let mut reply = raw.clone();
            reply["source"]["kind"] = serde_json::json!(kind);
            reply["source"]["reply_to_message_id"] =
                serde_json::json!("01992000-0000-7000-8000-000000000008");
            let parsed = serde_json::from_value::<IncomingProvenance>(reply.clone()).unwrap();
            assert_eq!(serde_json::to_value(parsed).unwrap(), reply);
            for invalid in [
                serde_json::Value::Null,
                serde_json::json!(""),
                serde_json::json!("not-a-uuid"),
            ] {
                reply["source"]["reply_to_message_id"] = invalid;
                assert!(serde_json::from_value::<IncomingProvenance>(reply.clone()).is_err());
            }
        }
        let mut reminder = raw.clone();
        reminder["source"]["kind"] = serde_json::json!("reply_later_due");
        reminder["source"]["marker_id"] = serde_json::json!("01992000-0000-7000-8000-000000000008");
        reminder["source"]["due_at"] = serde_json::json!("2026-09-08T13:00:00Z");
        assert!(serde_json::from_value::<IncomingProvenance>(reminder.clone()).is_err());
        reminder["actor"]["kind"] = serde_json::json!("personality_agent");
        reminder["actor"]["principal_id"] = reminder["personality_agent_id"].clone();
        let parsed = serde_json::from_value::<IncomingProvenance>(reminder.clone()).unwrap();
        assert_eq!(parsed.authenticated_direct_chat_human(), None);
        let mut invalid_reply = reminder.clone();
        invalid_reply["source"]["reply_to_message_id"] =
            serde_json::json!("01992000-0000-7000-8000-000000000008");
        assert!(serde_json::from_value::<IncomingProvenance>(invalid_reply).is_err());
        reminder["source"]["kind"] = serde_json::json!("messaging_mention");
        assert!(serde_json::from_value::<IncomingProvenance>(reminder).is_err());
        let mut direct =
            serde_json::to_value(crate::gateway::test_direct_chat_provenance()).unwrap();
        direct["source"]["event_id"] = raw["source"]["event_id"].clone();
        assert!(serde_json::from_value::<IncomingProvenance>(direct).is_err());
    }

    #[test]
    fn workspace_operation_provenance_preserves_terminal_result_without_human_authority() {
        let fixtures: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../contracts/agent-events-fixtures.json"
        ))
        .unwrap();
        for key in [
            "external_process_completed",
            "external_process_indeterminate",
        ] {
            let raw = fixtures[key]["wire"]["provenance"].clone();
            let parsed: IncomingProvenance = serde_json::from_value(raw.clone()).unwrap();
            assert!(parsed.is_external());
            assert_eq!(parsed.output_audience(), OutputAudience::Secretary);
            assert!(parsed.authenticated_direct_chat_human().is_none());
            assert!(parsed.messaging_source().is_none());
            assert_eq!(serde_json::to_value(parsed).unwrap(), raw);
        }
        let raw = fixtures["external_process_completed"]["wire"]["provenance"].clone();
        for (pointer, value) in [
            ("/actor/kind", serde_json::json!("human")),
            ("/actor/principal_id", serde_json::json!("another-pa")),
            (
                "/source/event_id",
                serde_json::json!("01992000-0000-4000-8000-000000000021"),
            ),
            ("/source/operation_id", serde_json::json!("ABC")),
            ("/source/originating_tool_call_id", serde_json::json!("")),
            ("/source/result/state", serde_json::json!("running")),
            (
                "/source/result/stdout_bytes",
                serde_json::json!(9_007_199_254_740_992u64),
            ),
            ("/source/result/output_truncated", serde_json::Value::Null),
            ("/source/surface", serde_json::json!("messaging")),
        ] {
            let mut bad = raw.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            assert!(
                serde_json::from_value::<IncomingProvenance>(bad).is_err(),
                "{pointer}"
            );
        }
        let mut bad = raw.clone();
        bad["source"]["result"]
            .as_object_mut()
            .unwrap()
            .remove("exit_code");
        assert!(serde_json::from_value::<IncomingProvenance>(bad).is_err());
        let mut bad = raw;
        bad["source"]["message_id"] = serde_json::json!("01992000-0000-7000-8000-000000000001");
        assert!(serde_json::from_value::<IncomingProvenance>(bad).is_err());
    }

    #[test]
    fn poll_vote_provenance_preserves_order_and_rejects_invalid_payloads() {
        let fixtures: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../contracts/agent-events-fixtures.json"
        ))
        .unwrap();
        for key in ["external_poll_vote", "external_poll_withdrawal"] {
            let envelope: crate::gateway::wire::WireCommandEnvelope =
                serde_json::from_value(fixtures[key]["wire"].clone()).unwrap();
            assert!(
                matches!(envelope.command(), crate::gateway::wire::WireCommand::ExternalEvent { content } if content.is_empty())
            );
            let raw = fixtures[key]["wire"]["provenance"].clone();
            let parsed: IncomingProvenance = serde_json::from_value(raw.clone()).unwrap();
            assert_eq!(serde_json::to_value(&parsed).unwrap(), raw);
            assert!(parsed.authenticated_direct_chat_human().is_none());
        }
        let raw = fixtures["external_poll_vote"]["wire"]["provenance"].clone();
        for (pointer, value) in [
            ("/source/poll_vote", serde_json::Value::Null),
            ("/source/poll_vote/poll_revision", serde_json::json!(0)),
            (
                "/source/poll_vote/poll_revision",
                serde_json::json!(9_007_199_254_740_992u64),
            ),
            ("/source/poll_vote/question", serde_json::Value::Null),
            (
                "/source/poll_vote/selected_options",
                serde_json::Value::Null,
            ),
            (
                "/source/poll_vote/selected_options/0/option_id",
                serde_json::json!("BAD-UUID"),
            ),
            ("/source/kind", serde_json::json!("messaging_mention")),
        ] {
            let mut bad = raw.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            assert!(
                serde_json::from_value::<IncomingProvenance>(bad).is_err(),
                "{pointer}"
            );
        }
        let mut missing = raw.clone();
        missing["source"]
            .as_object_mut()
            .unwrap()
            .remove("poll_vote");
        assert!(serde_json::from_value::<IncomingProvenance>(missing).is_err());
        for field in ["reply_to_message_id", "marker_id", "due_at"] {
            let mut bad = raw.clone();
            bad["source"][field] = if field == "due_at" {
                serde_json::json!("2026-09-08T13:00:00Z")
            } else {
                raw["source"]["message_id"].clone()
            };
            assert!(serde_json::from_value::<IncomingProvenance>(bad).is_err());
        }
        let mut duplicate = raw.clone();
        duplicate["source"]["poll_vote"]["selected_options"][1]["option_id"] =
            raw["source"]["poll_vote"]["selected_options"][0]["option_id"].clone();
        assert!(serde_json::from_value::<IncomingProvenance>(duplicate).is_err());
    }

    #[test]
    fn process_generation_accepts_exact_domain_and_converts_losslessly() {
        for raw in [0, MAX_PROCESS_GENERATION] {
            let generation = ProcessGeneration::from_wire(raw).expect("valid generation");
            assert_eq!(generation.as_u64(), raw);
            assert_eq!(generation.to_wire(), raw);
            assert_eq!(generation.as_i64(), raw as i64);
            assert_eq!(ProcessGeneration::from_sqlite(raw as i64), Ok(generation));
        }
        assert_eq!(
            ProcessGeneration::from_wire(MAX_PROCESS_GENERATION + 1),
            Err(RuntimeContractError::ProcessGenerationOutOfRange)
        );
        assert_eq!(
            ProcessGeneration::from_sqlite(-1),
            Err(RuntimeContractError::NegativeSqliteProcessGeneration(-1))
        );
    }

    #[test]
    fn rpc_nonce_preserves_existing_nonempty_128_byte_contract() {
        assert!(RpcBootNonce::new("n").is_ok());
        assert!(RpcBootNonce::new("n".repeat(MAX_OPAQUE_ID_BYTES)).is_ok());
        assert!(RpcBootNonce::new("").is_err());
        assert!(RpcBootNonce::new("n".repeat(MAX_OPAQUE_ID_BYTES + 1)).is_err());
    }

    #[test]
    fn rpc_identity_requires_exact_typed_wire_identity() {
        let identity = RpcIdentity::new(
            PersonalityAgentId::parse(PAID).unwrap(),
            ProcessGeneration::from_wire(7).unwrap(),
            RpcBootNonce::new("boot-nonce").unwrap(),
        );
        assert!(identity.validate_wire(PAID, 7, "boot-nonce").is_ok());
        assert!(
            identity
                .validate_wire("0198f0f4-9b72-7000-8000-000000000002", 7, "boot-nonce")
                .is_err()
        );
        assert!(identity.validate_wire(PAID, 8, "boot-nonce").is_err());
        assert!(identity.validate_wire(PAID, 7, "stale-nonce").is_err());
        assert!(
            identity
                .validate_wire(PAID, MAX_PROCESS_GENERATION + 1, "boot-nonce")
                .is_err()
        );
    }

    #[test]
    fn lease_and_fence_require_exact_generation_and_opaque_identities() {
        let generation = ProcessGeneration::from_wire(7).unwrap();
        let other_generation = ProcessGeneration::from_wire(8).unwrap();
        let paid = PersonalityAgentId::parse(PAID).unwrap();
        let other_paid = PersonalityAgentId::parse("0198f0f4-9b72-7000-8000-000000000002").unwrap();
        let lease = ProcessGenerationLease::new(paid.clone(), generation, "lease-1").unwrap();
        let other_generation_lease =
            ProcessGenerationLease::new(paid.clone(), other_generation, "lease-1").unwrap();
        let other_identity_lease =
            ProcessGenerationLease::new(paid.clone(), generation, "lease-2").unwrap();
        let other_paid_lease =
            ProcessGenerationLease::new(other_paid.clone(), generation, "lease-1").unwrap();
        assert!(lease.validate_exact(&paid, generation, "lease-1").is_ok());
        assert!(
            lease
                .validate_exact(&other_paid, generation, "lease-1")
                .is_err()
        );
        assert!(
            lease
                .validate_exact(&paid, other_generation, "lease-1")
                .is_err()
        );
        assert!(lease.validate_exact(&paid, generation, "lease-2").is_err());

        let fence = GenerationRecoveryFence::new(&lease, "fence-1").unwrap();
        assert!(fence.validate_exact(&lease, "fence-1").is_ok());
        assert!(
            fence
                .validate_exact(&other_generation_lease, "fence-1")
                .is_err()
        );
        assert!(
            fence
                .validate_exact(&other_identity_lease, "fence-1")
                .is_err()
        );
        assert!(fence.validate_exact(&other_paid_lease, "fence-1").is_err());
        assert!(fence.validate_exact(&lease, "fence-2").is_err());
        assert!(ProcessGenerationLease::new(paid, generation, "").is_err());
        assert!(GenerationRecoveryFence::new(&lease, "").is_err());
    }
}
