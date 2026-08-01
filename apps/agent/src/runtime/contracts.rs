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
pub const INBOUND_PROVENANCE_VERSION: u8 = 1;
pub const MAX_PROVENANCE_ID_BYTES: usize = 256;
/// 一件の配送が運べる解決済み宛先の上限。`@everyone` のような alias を
/// place 全体へ展開した結果を provenance に載せないための境界（ADR 0011 §2）。
pub const MAX_PROVENANCE_ADDRESSEES: usize = 64;
/// Place 単位 seq の上限。JSON で安全に運べる整数に収める（`MAX_SEQ` と同値）。
pub const MAX_PROVENANCE_SEQ: u64 = 9_007_199_254_740_991;

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
    #[error("human id must be a UUID")]
    HumanIdNotUuid,
    #[error("human id must use UUID version 7")]
    HumanIdWrongVersion,
    #[error("human id must use the RFC 4122 variant")]
    HumanIdWrongVariant,
    #[error("human id must use exact lowercase hyphenated UUID text")]
    HumanIdNonCanonical,
    #[error("inbound provenance version must be {INBOUND_PROVENANCE_VERSION}")]
    InboundProvenanceWrongVersion,
    #[error("{kind} must contain 1..={MAX_PROVENANCE_ID_BYTES} bytes")]
    InvalidProvenanceIdentity { kind: &'static str },
    #[error("inbound provenance target personality agent does not match the private store")]
    InboundProvenanceTargetMismatch,
    #[error(
        "inbound provenance must carry at most {MAX_PROVENANCE_ADDRESSEES} resolved addressees"
    )]
    TooManyProvenanceAddressees,
    #[error("place seq must be in 0..={MAX_PROVENANCE_SEQ}")]
    PlaceSeqOutOfRange,
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
        let (uuid, canonical) = parse_canonical_uuid_v7(value).map_err(|error| match error {
            CanonicalUuidError::NotUuid => RuntimeContractError::PersonalityAgentIdNotUuid,
            CanonicalUuidError::WrongVersion => {
                RuntimeContractError::PersonalityAgentIdWrongVersion
            }
            CanonicalUuidError::WrongVariant => {
                RuntimeContractError::PersonalityAgentIdWrongVariant
            }
            CanonicalUuidError::NonCanonical => {
                RuntimeContractError::PersonalityAgentIdNonCanonical
            }
        })?;
        Ok(Self {
            value: uuid,
            canonical,
        })
    }
}

enum CanonicalUuidError {
    NotUuid,
    WrongVersion,
    WrongVariant,
    NonCanonical,
}

/// Parse the exact lowercase hyphenated RFC UUIDv7 form and nothing else.
fn parse_canonical_uuid_v7(value: &str) -> Result<(Uuid, String), CanonicalUuidError> {
    let uuid = Uuid::parse_str(value).map_err(|_| CanonicalUuidError::NotUuid)?;
    if uuid.get_version() != Some(Version::SortRand) {
        return Err(CanonicalUuidError::WrongVersion);
    }
    if uuid.get_variant() != Variant::RFC4122 {
        return Err(CanonicalUuidError::WrongVariant);
    }
    let canonical = uuid.hyphenated().to_string();
    if value != canonical {
        return Err(CanonicalUuidError::NonCanonical);
    }
    Ok((uuid, canonical))
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

/// Canonical global identity of one human (ADR 0009 §1).
///
/// Firebase principals are credentials, not identity (ADR 0009 §2); they never
/// appear here. Parsing accepts only the exact lowercase hyphenated RFC UUIDv7
/// form, so callers cannot mint several identities for one person by
/// normalizing raw input.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HumanId {
    value: Uuid,
    canonical: String,
}

impl HumanId {
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

impl FromStr for HumanId {
    type Err = RuntimeContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (uuid, canonical) = parse_canonical_uuid_v7(value).map_err(|error| match error {
            CanonicalUuidError::NotUuid => RuntimeContractError::HumanIdNotUuid,
            CanonicalUuidError::WrongVersion => RuntimeContractError::HumanIdWrongVersion,
            CanonicalUuidError::WrongVariant => RuntimeContractError::HumanIdWrongVariant,
            CanonicalUuidError::NonCanonical => RuntimeContractError::HumanIdNonCanonical,
        })?;
        Ok(Self {
            value: uuid,
            canonical,
        })
    }
}

impl fmt::Display for HumanId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for HumanId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for HumanId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(de::Error::custom)
    }
}

/// Opaque identity carried by provenance: a place, a message, a correlation.
///
/// The agent never interprets the text; the shared Workspace API owns the
/// meaning of these ids (ADR 0011 §10).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProvenanceId(String);

impl ProvenanceId {
    pub fn new(value: impl Into<String>) -> Result<Self, RuntimeContractError> {
        Ok(Self(validate_provenance_identity(
            value.into(),
            "provenance id",
        )?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProvenanceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for ProvenanceId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ProvenanceId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// Place 単位の単調増加 seq。未読・replay・permalink の基準（ADR 0011 §6）。
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlaceSeq(u64);

impl PlaceSeq {
    pub fn new(value: u64) -> Result<Self, RuntimeContractError> {
        if value > MAX_PROVENANCE_SEQ {
            return Err(RuntimeContractError::PlaceSeqOutOfRange);
        }
        Ok(Self(value))
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for PlaceSeq {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for PlaceSeq {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for PlaceSeq {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// この inbound を発した主体（ADR 0011 §2）。
///
/// human と人格 agent は同型に扱う。これは **発話者** であって宛先ではない。
/// mention 先・宛先は [`DeliveryProvenance::addressees`] が持つ。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActorRef {
    Human {
        human_id: HumanId,
    },
    /// `agent` ではなく `personality_agent`: worker / subagent / app と混同しない。
    PersonalityAgent {
        personality_agent_id: PersonalityAgentId,
    },
}

impl ActorRef {
    pub const fn kind(&self) -> ActorKind {
        match self {
            Self::Human { .. } => ActorKind::Human,
            Self::PersonalityAgent { .. } => ActorKind::PersonalityAgent,
        }
    }

    /// Canonical identity text of the actor, for logging and stable keys.
    pub fn id(&self) -> &str {
        match self {
            Self::Human { human_id } => human_id.as_str(),
            Self::PersonalityAgent {
                personality_agent_id,
            } => personality_agent_id.as_str(),
        }
    }

    /// Stable key across both kinds: `human:<id>` / `personality_agent:<id>`.
    ///
    /// Human と人格 agent の id はどちらも UUIDv7 なので、id だけでは両者を
    /// 区別できない。参加者を指すキーは必ず kind を伴う。
    pub fn key(&self) -> String {
        format!(
            "{}:{}",
            match self.kind() {
                ActorKind::Human => "human",
                ActorKind::PersonalityAgent => "personality_agent",
            },
            self.id()
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorKind {
    Human,
    PersonalityAgent,
}

/// メッセージングの場所（ADR 0011 §1）。
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PlaceRef {
    Channel { channel_id: ProvenanceId },
    Dm { dm_id: ProvenanceId },
    GroupDm { dm_id: ProvenanceId },
}

/// この inbound が届いた Surface（ADR 0011 §1）。
///
/// messaging は必ず place と、配送された一件のメッセージを伴う。direct chat は
/// どちらも持たない。この非対称性は型で表し、実行時検査に落とさない。
///
/// messaging 側を `Box` に置くのは、provenance が admitted command に値で埋まり
/// 深い async state machine を通るためである。direct chat の経路を太らせない。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "surface", rename_all = "snake_case", deny_unknown_fields)]
pub enum InboundSource {
    /// Employer 本人だけの私信 Surface（ADR 0009 §5）。
    /// 空の struct variant にするのは、`{"surface":"direct_chat", ...}` に紛れ込む
    /// 未知のフィールドを serde に拒否させるため（unit variant は残りを無視する）。
    DirectChat {},
    /// 共有の場。
    Messaging(Box<MessagingSource>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessagingSource {
    /// Workspace channel のときだけ存在する。global DM に Workspace はない。
    pub workspace_id: Option<ProvenanceId>,
    pub place: PlaceRef,
    pub delivery: DeliveryProvenance,
}

impl InboundSource {
    pub const fn messaging(&self) -> Option<&MessagingSource> {
        match self {
            Self::DirectChat {} => None,
            Self::Messaging(source) => Some(source),
        }
    }

    pub const fn place(&self) -> Option<&PlaceRef> {
        match self {
            Self::DirectChat {} => None,
            Self::Messaging(source) => Some(&source.place),
        }
    }

    pub const fn workspace_id(&self) -> Option<&ProvenanceId> {
        match self {
            Self::DirectChat {} => None,
            Self::Messaging(source) => source.workspace_id.as_ref(),
        }
    }

    /// 配送された一件のメッセージ。direct chat には無い。
    pub const fn delivery(&self) -> Option<&DeliveryProvenance> {
        match self {
            Self::DirectChat {} => None,
            Self::Messaging(source) => Some(&source.delivery),
        }
    }
}

/// なぜこの配送が本人の注意の候補になったか（ADR 0011 §8）。
///
/// これは配送側の事実であって、注意を割くかどうかの判断ではない。判断は本人が
/// 持つ。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerReason {
    /// 名指しで呼ばれた。alias（`@everyone` など）の解決結果を含む。
    Mention,
    /// DM / グループ DM の新着。名指しでなくても本人宛である。
    DirectMessage,
    /// 名指しではない place の新着。
    PlaceActivity,
}

/// メッセージ単位の緊急度。人間には未読トリアージ、agent には覚醒トリガの
/// 優先度として働く。urgent は相手の設定・予算を突破する権限ではない。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Urgency {
    Urgent,
    #[default]
    Normal,
    Fyi,
}

/// 一件の配送そのものについての事実。messaging surface のときに必ず伴う。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "DeliveryProvenanceWire")]
pub struct DeliveryProvenance {
    message_id: ProvenanceId,
    seq: PlaceSeq,
    /// admission が解決した宛先。raw な `@名前` 一致は判定に使わない
    /// （ADR 0011 §2）。alias を place 全体へ展開した結果は載せない。
    addressees: Vec<ActorRef>,
    trigger_reason: TriggerReason,
    urgency: Urgency,
    correlation_id: Option<ProvenanceId>,
    causation_id: Option<ProvenanceId>,
}

impl DeliveryProvenance {
    pub const fn new(
        message_id: ProvenanceId,
        seq: PlaceSeq,
        trigger_reason: TriggerReason,
        urgency: Urgency,
    ) -> Self {
        Self {
            message_id,
            seq,
            addressees: Vec::new(),
            trigger_reason,
            urgency,
            correlation_id: None,
            causation_id: None,
        }
    }

    pub fn with_addressees(
        mut self,
        addressees: Vec<ActorRef>,
    ) -> Result<Self, RuntimeContractError> {
        if addressees.len() > MAX_PROVENANCE_ADDRESSEES {
            return Err(RuntimeContractError::TooManyProvenanceAddressees);
        }
        self.addressees = addressees;
        Ok(self)
    }

    pub fn with_correlation(
        mut self,
        correlation_id: Option<ProvenanceId>,
        causation_id: Option<ProvenanceId>,
    ) -> Self {
        self.correlation_id = correlation_id;
        self.causation_id = causation_id;
        self
    }

    pub const fn message_id(&self) -> &ProvenanceId {
        &self.message_id
    }

    pub const fn seq(&self) -> PlaceSeq {
        self.seq
    }

    pub fn addressees(&self) -> &[ActorRef] {
        &self.addressees
    }

    pub const fn trigger_reason(&self) -> TriggerReason {
        self.trigger_reason
    }

    pub const fn urgency(&self) -> Urgency {
        self.urgency
    }

    pub const fn correlation_id(&self) -> Option<&ProvenanceId> {
        self.correlation_id.as_ref()
    }

    pub const fn causation_id(&self) -> Option<&ProvenanceId> {
        self.causation_id.as_ref()
    }

    fn validate(&self) -> Result<(), RuntimeContractError> {
        if self.addressees.len() > MAX_PROVENANCE_ADDRESSEES {
            return Err(RuntimeContractError::TooManyProvenanceAddressees);
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeliveryProvenanceWire {
    message_id: ProvenanceId,
    seq: PlaceSeq,
    addressees: Vec<ActorRef>,
    trigger_reason: TriggerReason,
    urgency: Urgency,
    correlation_id: Option<ProvenanceId>,
    causation_id: Option<ProvenanceId>,
}

impl TryFrom<DeliveryProvenanceWire> for DeliveryProvenance {
    type Error = RuntimeContractError;

    fn try_from(wire: DeliveryProvenanceWire) -> Result<Self, Self::Error> {
        if wire.addressees.len() > MAX_PROVENANCE_ADDRESSEES {
            return Err(RuntimeContractError::TooManyProvenanceAddressees);
        }
        Ok(Self {
            message_id: wire.message_id,
            seq: wire.seq,
            addressees: wire.addressees,
            trigger_reason: wire.trigger_reason,
            urgency: wire.urgency,
            correlation_id: wire.correlation_id,
            causation_id: wire.causation_id,
        })
    }
}

/// admission 時にこの配送を許可した根拠のスナップショット。
///
/// 配送の可否は決定論的境界が判断し、agent 側は再判定しない（ADR 0011 §8）。
/// ここに残るのは「どの権利でこれが自分に届いたか」という事実である。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionAuthority {
    basis: AuthorityBasis,
    /// 判断を下した境界が発行した decision の識別子。後から監査で結び直せる。
    decision_id: Option<ProvenanceId>,
}

impl AdmissionAuthority {
    pub const fn new(basis: AuthorityBasis, decision_id: Option<ProvenanceId>) -> Self {
        Self { basis, decision_id }
    }

    pub const fn basis(&self) -> AuthorityBasis {
        self.basis
    }

    pub const fn decision_id(&self) -> Option<&ProvenanceId> {
        self.decision_id.as_ref()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityBasis {
    /// direct chat: Employer 本人の私信であること（ADR 0009 §5）。
    Employer,
    /// place の membership（channel / グループ DM）。
    PlaceMembership,
    /// 二者間の Connection（DM）。
    Connection,
}

/// 人格 agent へ届いた一件の inbound についての、admission 時点の事実。
///
/// Surface 一般の provenance であり、direct chat と messaging が同じ形を使う
/// （ADR 0011 §1・§2）。
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "InboundProvenanceWire")]
pub struct InboundProvenanceV1 {
    version: u8,
    tenant_id: String,
    /// 受け手。この private store の持ち主である人格 agent。
    personality_agent_id: PersonalityAgentId,
    /// 発話者。
    actor: ActorRef,
    source: InboundSource,
    authority: AdmissionAuthority,
}

/// [`InboundProvenanceV1::messaging`] の引数。
pub struct MessagingProvenance {
    pub tenant_id: String,
    pub personality_agent_id: PersonalityAgentId,
    pub actor: ActorRef,
    pub workspace_id: Option<ProvenanceId>,
    pub place: PlaceRef,
    pub delivery: DeliveryProvenance,
    pub authority: AdmissionAuthority,
}

impl InboundProvenanceV1 {
    /// direct chat: Employer 本人からの私信（ADR 0009 §5）。
    pub fn direct_chat(
        tenant_id: impl Into<String>,
        personality_agent_id: PersonalityAgentId,
        human_id: HumanId,
    ) -> Result<Self, RuntimeContractError> {
        Ok(Self {
            version: INBOUND_PROVENANCE_VERSION,
            tenant_id: validate_provenance_identity(tenant_id.into(), "tenant id")?,
            personality_agent_id,
            actor: ActorRef::Human { human_id },
            source: InboundSource::DirectChat {},
            authority: AdmissionAuthority::new(AuthorityBasis::Employer, None),
        })
    }

    /// messaging: 共有の場で起きた一件の配送（ADR 0011 §1）。
    pub fn messaging(provenance: MessagingProvenance) -> Result<Self, RuntimeContractError> {
        provenance.delivery.validate()?;
        Ok(Self {
            version: INBOUND_PROVENANCE_VERSION,
            tenant_id: validate_provenance_identity(provenance.tenant_id, "tenant id")?,
            personality_agent_id: provenance.personality_agent_id,
            actor: provenance.actor,
            source: InboundSource::Messaging(Box::new(MessagingSource {
                workspace_id: provenance.workspace_id,
                place: provenance.place,
                delivery: provenance.delivery,
            })),
            authority: provenance.authority,
        })
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

    pub const fn actor(&self) -> &ActorRef {
        &self.actor
    }

    pub const fn source(&self) -> &InboundSource {
        &self.source
    }

    /// 配送された一件のメッセージ。direct chat には無い。
    pub const fn delivery(&self) -> Option<&DeliveryProvenance> {
        self.source.delivery()
    }

    pub const fn authority(&self) -> &AdmissionAuthority {
        &self.authority
    }

    pub fn validate(
        &self,
        expected_target: &PersonalityAgentId,
    ) -> Result<(), RuntimeContractError> {
        if self.version != INBOUND_PROVENANCE_VERSION {
            return Err(RuntimeContractError::InboundProvenanceWrongVersion);
        }
        validate_provenance_identity(self.tenant_id.clone(), "tenant id")?;
        if let Some(delivery) = self.delivery() {
            delivery.validate()?;
        }
        if &self.personality_agent_id != expected_target {
            return Err(RuntimeContractError::InboundProvenanceTargetMismatch);
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InboundProvenanceWire {
    version: u8,
    tenant_id: String,
    personality_agent_id: PersonalityAgentId,
    actor: ActorRef,
    source: InboundSource,
    authority: AdmissionAuthority,
}

impl TryFrom<InboundProvenanceWire> for InboundProvenanceV1 {
    type Error = RuntimeContractError;

    fn try_from(wire: InboundProvenanceWire) -> Result<Self, Self::Error> {
        if wire.version != INBOUND_PROVENANCE_VERSION {
            return Err(RuntimeContractError::InboundProvenanceWrongVersion);
        }
        if let Some(delivery) = wire.source.delivery() {
            delivery.validate()?;
        }
        Ok(Self {
            version: wire.version,
            tenant_id: validate_provenance_identity(wire.tenant_id, "tenant id")?,
            personality_agent_id: wire.personality_agent_id,
            actor: wire.actor,
            source: wire.source,
            authority: wire.authority,
        })
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
    use super::*;

    const PAID: &str = "0198f0f4-9b72-7000-8000-000000000001";
    const OTHER_PAID: &str = "0198f0f4-9b72-7000-8000-0000000000a2";
    const HUMAN: &str = "0198f0f4-9b72-7000-8000-00000000ab01";

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
    fn human_id_accepts_only_exact_canonical_rfc_uuid_v7() {
        let human = HumanId::parse(HUMAN).expect("canonical UUIDv7");
        assert_eq!(human.as_str(), HUMAN);
        assert_eq!(human.to_string(), HUMAN);
        assert_eq!(
            serde_json::from_str::<HumanId>(&format!("\"{HUMAN}\"")).expect("deserialize"),
            human
        );
        // Firebase principal は credential であって identity ではない
        // （ADR 0009 §2）。
        for value in [
            "alice@example.com",
            "human-123",
            &HUMAN.to_ascii_uppercase(),
            "0198f0f4-9b72-4000-8000-00000000ab01",
        ] {
            assert!(
                HumanId::parse(value).is_err(),
                "unexpectedly accepted {value:?}"
            );
        }
    }

    #[test]
    fn direct_chat_provenance_is_closed_and_binds_authenticated_dimensions() {
        let paid = PersonalityAgentId::parse(PAID).unwrap();
        let provenance = InboundProvenanceV1::direct_chat(
            "tenant-at-admission",
            paid.clone(),
            HumanId::parse(HUMAN).unwrap(),
        )
        .unwrap();
        provenance.validate(&paid).unwrap();
        assert_eq!(provenance.version(), 1);
        assert_eq!(provenance.tenant_id(), "tenant-at-admission");
        assert_eq!(provenance.personality_agent_id(), &paid);
        assert_eq!(provenance.actor().kind(), ActorKind::Human);
        assert_eq!(provenance.actor().id(), HUMAN);
        assert_eq!(provenance.source(), &InboundSource::DirectChat {});
        assert_eq!(provenance.source().place(), None);
        assert_eq!(provenance.delivery(), None);
        assert_eq!(
            provenance.authority().basis(),
            AuthorityBasis::Employer,
            "direct chat は Employer 本人の私信である（ADR 0009 §5）"
        );
        assert_eq!(
            serde_json::to_value(&provenance).unwrap(),
            serde_json::json!({
                "version": 1,
                "tenant_id": "tenant-at-admission",
                "personality_agent_id": PAID,
                "actor": {"kind": "human", "human_id": HUMAN},
                "source": {"surface": "direct_chat"},
                "authority": {"basis": "employer", "decision_id": null}
            })
        );
    }

    #[test]
    fn messaging_provenance_carries_place_actor_and_resolved_addressees() {
        let paid = PersonalityAgentId::parse(PAID).unwrap();
        let speaker = PersonalityAgentId::parse(OTHER_PAID).unwrap();
        let provenance = InboundProvenanceV1::messaging(MessagingProvenance {
            tenant_id: "tenant-at-admission".to_owned(),
            personality_agent_id: paid.clone(),
            // 人格 agent も発話者になる（ADR 0011 §2）。
            actor: ActorRef::PersonalityAgent {
                personality_agent_id: speaker,
            },
            workspace_id: Some(ProvenanceId::new("ws-1").unwrap()),
            place: PlaceRef::Channel {
                channel_id: ProvenanceId::new("ch-general").unwrap(),
            },
            delivery: DeliveryProvenance::new(
                ProvenanceId::new("msg-1").unwrap(),
                PlaceSeq::new(42).unwrap(),
                TriggerReason::Mention,
                Urgency::Urgent,
            )
            .with_addressees(vec![ActorRef::Human {
                human_id: HumanId::parse(HUMAN).unwrap(),
            }])
            .unwrap()
            .with_correlation(Some(ProvenanceId::new("corr-1").unwrap()), None),
            authority: AdmissionAuthority::new(
                AuthorityBasis::PlaceMembership,
                Some(ProvenanceId::new("decision-1").unwrap()),
            ),
        })
        .unwrap();
        provenance.validate(&paid).unwrap();
        assert_eq!(provenance.actor().kind(), ActorKind::PersonalityAgent);
        assert_eq!(provenance.actor().id(), OTHER_PAID);
        assert_eq!(
            provenance.source().place(),
            Some(&PlaceRef::Channel {
                channel_id: ProvenanceId::new("ch-general").unwrap(),
            })
        );
        let delivery = provenance.delivery().expect("messaging carries a message");
        assert_eq!(delivery.seq().as_u64(), 42);
        assert_eq!(delivery.trigger_reason(), TriggerReason::Mention);
        assert_eq!(delivery.urgency(), Urgency::Urgent);
        assert_eq!(delivery.addressees().len(), 1);
        assert_eq!(delivery.causation_id(), None);

        let round_tripped: InboundProvenanceV1 =
            serde_json::from_value(serde_json::to_value(&provenance).unwrap()).unwrap();
        assert_eq!(round_tripped, provenance);
    }

    #[test]
    fn provenance_rejects_unknown_shape_and_target_mismatch() {
        let wrong_target =
            PersonalityAgentId::parse("0198f0f4-9b72-7000-8000-000000000002").unwrap();
        let provenance = InboundProvenanceV1::direct_chat(
            "tenant-at-admission",
            PersonalityAgentId::parse(PAID).unwrap(),
            HumanId::parse(HUMAN).unwrap(),
        )
        .unwrap();
        assert_eq!(
            provenance.validate(&wrong_target),
            Err(RuntimeContractError::InboundProvenanceTargetMismatch)
        );
        let mut valid = serde_json::to_value(&provenance).unwrap();
        for mutate in [
            |raw: &mut serde_json::Value| raw["version"] = serde_json::json!(2),
            |raw: &mut serde_json::Value| raw["unknown"] = serde_json::json!(true),
            |raw: &mut serde_json::Value| raw["actor"]["unknown"] = serde_json::json!(true),
            |raw: &mut serde_json::Value| raw["source"]["surface"] = serde_json::json!("slack"),
            |raw: &mut serde_json::Value| {
                raw["authority"]["basis"] = serde_json::json!("everyone");
            },
        ] {
            let mut raw = valid.clone();
            mutate(&mut raw);
            assert!(
                serde_json::from_value::<InboundProvenanceV1>(raw.clone()).is_err(),
                "unexpectedly accepted {raw}"
            );
        }
        // 変異させていない値は受理される（上の否定が空振りでないことの担保）。
        valid["version"] = serde_json::json!(1);
        serde_json::from_value::<InboundProvenanceV1>(valid).unwrap();
    }

    #[test]
    fn surface_decides_whether_a_delivered_message_exists() {
        let paid = PersonalityAgentId::parse(PAID).unwrap();
        let delivery = serde_json::json!({
            "message_id": "msg-1",
            "seq": 1,
            "addressees": [],
            "trigger_reason": "place_activity",
            "urgency": "normal",
            "correlation_id": null,
            "causation_id": null
        });
        let base = serde_json::to_value(
            InboundProvenanceV1::direct_chat("tenant", paid, HumanId::parse(HUMAN).unwrap())
                .unwrap(),
        )
        .unwrap();

        // direct chat は配送されたメッセージを持たない。
        let mut with_delivery = base.clone();
        with_delivery["source"]["delivery"] = delivery.clone();
        assert!(serde_json::from_value::<InboundProvenanceV1>(with_delivery).is_err());

        // messaging は必ず持つ。
        let mut messaging = base;
        messaging["source"] = serde_json::json!({
            "surface": "messaging",
            "workspace_id": null,
            "place": {"kind": "dm", "dm_id": "dm-1"}
        });
        assert!(serde_json::from_value::<InboundProvenanceV1>(messaging.clone()).is_err());
        messaging["source"]["delivery"] = delivery;
        let parsed = serde_json::from_value::<InboundProvenanceV1>(messaging).unwrap();
        assert!(parsed.delivery().is_some());
    }

    #[test]
    fn resolved_addressees_are_bounded_so_broadcasts_are_not_expanded() {
        let addressee = ActorRef::Human {
            human_id: HumanId::parse(HUMAN).unwrap(),
        };
        let delivery = || {
            DeliveryProvenance::new(
                ProvenanceId::new("msg-1").unwrap(),
                PlaceSeq::new(1).unwrap(),
                TriggerReason::Mention,
                Urgency::Normal,
            )
        };
        assert!(
            delivery()
                .with_addressees(vec![addressee.clone(); MAX_PROVENANCE_ADDRESSEES])
                .is_ok()
        );
        assert_eq!(
            delivery()
                .with_addressees(vec![addressee; MAX_PROVENANCE_ADDRESSEES + 1])
                .err(),
            Some(RuntimeContractError::TooManyProvenanceAddressees)
        );
        assert_eq!(
            PlaceSeq::new(MAX_PROVENANCE_SEQ + 1).err(),
            Some(RuntimeContractError::PlaceSeqOutOfRange)
        );
    }

    #[test]
    fn provenance_deserialization_enforces_id_grammar_and_bounds() {
        for (tenant_id, human_id) in [
            ("".to_owned(), HUMAN.to_owned()),
            ("tenant".to_owned(), String::new()),
            (" tenant".to_owned(), HUMAN.to_owned()),
            ("tenant".to_owned(), "human name".to_owned()),
            ("tenant".to_owned(), "人間".to_owned()),
            ("tenant".to_owned(), "h".repeat(MAX_PROVENANCE_ID_BYTES + 1)),
            ("t".repeat(MAX_PROVENANCE_ID_BYTES + 1), HUMAN.to_owned()),
        ] {
            let raw = serde_json::json!({
                "version": 1,
                "tenant_id": &tenant_id,
                "personality_agent_id": PAID,
                "actor": {"kind": "human", "human_id": &human_id},
                "source": {"surface": "direct_chat"},
                "authority": {"basis": "employer", "decision_id": null}
            });
            assert!(
                serde_json::from_value::<InboundProvenanceV1>(raw).is_err(),
                "unexpectedly accepted tenant={tenant_id:?}, human={human_id:?}"
            );
        }
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
