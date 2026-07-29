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
    #[error("direct-chat provenance version must be {DIRECT_CHAT_PROVENANCE_VERSION}")]
    DirectChatProvenanceWrongVersion,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "DirectChatProvenanceWire")]
pub struct DirectChatProvenanceV1 {
    version: u8,
    tenant_id: String,
    personality_agent_id: PersonalityAgentId,
    actor: HumanActorProvenance,
    source: DirectChatSource,
}

impl DirectChatProvenanceV1 {
    pub fn new(
        tenant_id: impl Into<String>,
        personality_agent_id: PersonalityAgentId,
        human_principal_id: impl Into<String>,
    ) -> Result<Self, RuntimeContractError> {
        Ok(Self {
            version: DIRECT_CHAT_PROVENANCE_VERSION,
            tenant_id: validate_provenance_identity(tenant_id.into(), "tenant id")?,
            personality_agent_id,
            actor: HumanActorProvenance::new(human_principal_id)?,
            source: DirectChatSource::default(),
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

    pub const fn actor(&self) -> &HumanActorProvenance {
        &self.actor
    }

    pub const fn source(&self) -> &DirectChatSource {
        &self.source
    }

    pub fn validate(
        &self,
        expected_target: &PersonalityAgentId,
    ) -> Result<(), RuntimeContractError> {
        if self.version != DIRECT_CHAT_PROVENANCE_VERSION {
            return Err(RuntimeContractError::DirectChatProvenanceWrongVersion);
        }
        validate_provenance_identity(self.tenant_id.clone(), "tenant id")?;
        self.actor.validate()?;
        if &self.personality_agent_id != expected_target {
            return Err(RuntimeContractError::DirectChatProvenanceTargetMismatch);
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectChatProvenanceWire {
    version: u8,
    tenant_id: String,
    personality_agent_id: PersonalityAgentId,
    actor: HumanActorProvenance,
    source: DirectChatSource,
}

impl TryFrom<DirectChatProvenanceWire> for DirectChatProvenanceV1 {
    type Error = RuntimeContractError;

    fn try_from(wire: DirectChatProvenanceWire) -> Result<Self, Self::Error> {
        if wire.version != DIRECT_CHAT_PROVENANCE_VERSION {
            return Err(RuntimeContractError::DirectChatProvenanceWrongVersion);
        }
        Ok(Self {
            version: wire.version,
            tenant_id: validate_provenance_identity(wire.tenant_id, "tenant id")?,
            personality_agent_id: wire.personality_agent_id,
            actor: wire.actor,
            source: wire.source,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "HumanActorProvenanceWire")]
pub struct HumanActorProvenance {
    kind: HumanActorKind,
    principal_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HumanActorProvenanceWire {
    kind: HumanActorKind,
    principal_id: String,
}

impl TryFrom<HumanActorProvenanceWire> for HumanActorProvenance {
    type Error = RuntimeContractError;

    fn try_from(wire: HumanActorProvenanceWire) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: wire.kind,
            principal_id: validate_provenance_identity(wire.principal_id, "human principal id")?,
        })
    }
}

impl HumanActorProvenance {
    fn new(principal_id: impl Into<String>) -> Result<Self, RuntimeContractError> {
        Ok(Self {
            kind: HumanActorKind::Human,
            principal_id: validate_provenance_identity(principal_id.into(), "human principal id")?,
        })
    }

    pub const fn kind(&self) -> HumanActorKind {
        self.kind
    }

    pub fn principal_id(&self) -> &str {
        &self.principal_id
    }

    fn validate(&self) -> Result<(), RuntimeContractError> {
        validate_provenance_identity(self.principal_id.clone(), "human principal id")?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HumanActorKind {
    Human,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectChatSource {
    surface: DirectChatSurface,
}

impl DirectChatSource {
    pub const fn surface(&self) -> DirectChatSurface {
        self.surface
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectChatSurface {
    #[default]
    DirectChat,
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
            DirectChatProvenanceV1::new("tenant-at-admission", paid.clone(), "human-123").unwrap();
        provenance.validate(&paid).unwrap();
        assert_eq!(provenance.version(), 1);
        assert_eq!(provenance.tenant_id(), "tenant-at-admission");
        assert_eq!(provenance.personality_agent_id(), &paid);
        assert_eq!(provenance.actor().kind(), HumanActorKind::Human);
        assert_eq!(provenance.actor().principal_id(), "human-123");
        assert_eq!(provenance.source().surface(), DirectChatSurface::DirectChat);
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
        let provenance = DirectChatProvenanceV1::new(
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
            let parsed = serde_json::from_str::<DirectChatProvenanceV1>(&raw);
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
                serde_json::from_value::<DirectChatProvenanceV1>(raw).is_err(),
                "unexpectedly accepted tenant={tenant_id:?}, principal={principal_id:?}"
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
