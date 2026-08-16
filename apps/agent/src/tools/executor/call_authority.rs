//! Signed, generation-bound authority for one exact executor operation.
//!
//! The runtime process identity authenticates the RPC peer, but it does not
//! authorize an effect. Production read/discovery calls additionally carry a
//! short-lived token minted from the live post-COMMIT execution permit.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::ExecutorOperation;
use crate::{
    approval::authority::{
        CommittedExecutionPermitParts, ExecutionAuthorityProvenance,
        ExecutorCommittedExecutionPermit,
    },
    provider::types::ToolInvocationRoute,
    runtime::contracts::RpcIdentity,
    tools::ToolError,
};

pub const EXECUTOR_CALL_AUTHORITY_VERSION: u8 = 1;
pub const EXECUTOR_CALL_AUTHORITY_AUDIENCE: &str = "sumi.tool-executor.read.v1";
const SIGNATURE_DOMAIN: &[u8] = b"sumi.executor.call-authority.signature.v1\0";
const OPERATION_DIGEST_DOMAIN: &[u8] = b"sumi.executor.operation-digest.v1\0";
const BOOT_NONCE_DIGEST_DOMAIN: &[u8] = b"sumi.executor.boot-nonce-digest.v1\0";
const MAX_AUTHORITY_LIFETIME: Duration = Duration::from_secs(5 * 60);
const MAX_CLOCK_SKEW: Duration = Duration::from_secs(5);
const DEFAULT_AUTHORITY_LIFETIME: Duration = Duration::from_secs(30);
const MAX_BOUND_TEXT_BYTES: usize = 1024;
const EXECUTOR_CALL_AUTHORITY_KEY_ID: &str = "sumi.executor.call-authority.ed25519.v1";

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum ExecutorInvocationRoute {
    Normal,
    Elevated,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum ExecutorAuthorityProvenance {
    AgentOwn,
    AgentOwnWithHumanConsent,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct CallAuthorityPermitClaims {
    pub(super) grant_digest: String,
    pub(super) bound_evidence_digest: String,
    pub(super) action_digest: String,
    pub(super) authorization_projection_digest: String,
    pub(super) route: ExecutorInvocationRoute,
    pub(super) resolved_authority: ExecutorAuthorityProvenance,
}

impl CallAuthorityPermitClaims {
    fn validate(&self) -> Result<(), CallAuthorityError> {
        validate_sha256(&self.grant_digest)?;
        validate_sha256(&self.bound_evidence_digest)?;
        validate_sha256(&self.action_digest)?;
        validate_sha256(&self.authorization_projection_digest)?;
        match (self.route, self.resolved_authority) {
            (ExecutorInvocationRoute::Normal, ExecutorAuthorityProvenance::AgentOwn)
            | (
                ExecutorInvocationRoute::Elevated,
                ExecutorAuthorityProvenance::AgentOwnWithHumanConsent,
            ) => Ok(()),
            _ => Err(CallAuthorityError::InvalidPermitTuple),
        }
    }
}

fn permit_claims(
    permit: CommittedExecutionPermitParts,
) -> Result<CallAuthorityPermitClaims, CallAuthorityError> {
    let route = match permit.route {
        ToolInvocationRoute::Normal => ExecutorInvocationRoute::Normal,
        ToolInvocationRoute::Elevated => ExecutorInvocationRoute::Elevated,
    };
    let resolved_authority = match permit.resolved_authority {
        ExecutionAuthorityProvenance::AgentOwn => ExecutorAuthorityProvenance::AgentOwn,
        ExecutionAuthorityProvenance::AgentOwnWithHumanConsent => {
            ExecutorAuthorityProvenance::AgentOwnWithHumanConsent
        }
        ExecutionAuthorityProvenance::HumanAccountOneShot => {
            return Err(CallAuthorityError::InvalidPermitTuple);
        }
    };
    Ok(CallAuthorityPermitClaims {
        grant_digest: permit.grant_digest,
        bound_evidence_digest: permit.bound_evidence_digest,
        action_digest: permit.action_digest,
        authorization_projection_digest: permit.authorization_projection_digest,
        route,
        resolved_authority,
    })
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CallAuthorityClaims {
    pub version: u8,
    pub authority_id: String,
    pub audience: String,
    pub generation: u64,
    pub boot_nonce_digest: String,
    pub request_id: String,
    pub execution_id: String,
    pub operation_digest: String,
    permit: CallAuthorityPermitClaims,
    pub issued_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SignedCallAuthority {
    key_id: String,
    claims: CallAuthorityClaims,
    signature: String,
}

pub struct Ed25519CallAuthorityIssuer {
    key_id: String,
    signing_key: SigningKey,
    identity: RpcIdentity,
    lifetime: Duration,
    clock: Arc<dyn AuthorityClock>,
}

impl Ed25519CallAuthorityIssuer {
    pub fn new(
        key_id: impl Into<String>,
        signing_key: SigningKey,
        identity: RpcIdentity,
    ) -> Result<Self, CallAuthorityError> {
        let key_id = key_id.into();
        validate_bounded_text(&key_id)?;
        Ok(Self {
            key_id,
            signing_key,
            identity,
            lifetime: DEFAULT_AUTHORITY_LIFETIME,
            clock: Arc::new(SystemAuthorityClock),
        })
    }

    #[cfg(test)]
    pub(super) fn with_clock(mut self, clock: Arc<dyn AuthorityClock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    pub(super) fn issue(
        &self,
        request_id: String,
        operation: ExecutorOperation,
        permit: ExecutorCommittedExecutionPermit,
    ) -> Result<SignedCallAuthority, CallAuthorityError> {
        let permit = permit_claims(permit.into_executor_parts())?;
        self.issue_claims(request_id, operation, permit)
    }

    fn issue_claims(
        &self,
        request_id: String,
        operation: ExecutorOperation,
        permit: CallAuthorityPermitClaims,
    ) -> Result<SignedCallAuthority, CallAuthorityError> {
        permit.validate()?;
        if !is_production_read_operation(&operation) {
            return Err(CallAuthorityError::UnsupportedOperation);
        }
        validate_bounded_text(&request_id)?;
        let execution_id = operation_execution_id(&operation)
            .ok_or(CallAuthorityError::UnsupportedOperation)?
            .to_owned();
        validate_bounded_text(&execution_id)?;
        let issued_at_unix_ms = self.clock.now_unix_ms()?;
        let lifetime_ms =
            u64::try_from(self.lifetime.as_millis()).map_err(|_| CallAuthorityError::Clock)?;
        let expires_at_unix_ms = issued_at_unix_ms
            .checked_add(lifetime_ms)
            .ok_or(CallAuthorityError::Clock)?;
        let claims = CallAuthorityClaims {
            version: EXECUTOR_CALL_AUTHORITY_VERSION,
            authority_id: Uuid::now_v7().hyphenated().to_string(),
            audience: EXECUTOR_CALL_AUTHORITY_AUDIENCE.to_owned(),
            generation: self.identity.generation().to_wire(),
            boot_nonce_digest: boot_nonce_digest(self.identity.nonce().as_str()),
            request_id,
            execution_id,
            operation_digest: operation_digest(&operation)?,
            permit,
            issued_at_unix_ms,
            expires_at_unix_ms,
        };
        let signature = self
            .signing_key
            .sign(&signature_payload(&self.key_id, &claims)?);
        Ok(SignedCallAuthority {
            key_id: self.key_id.clone(),
            claims,
            signature: hex_encode(&signature.to_bytes()),
        })
    }

    #[cfg(test)]
    pub(super) fn issue_for_test(
        &self,
        request_id: String,
        operation: ExecutorOperation,
        permit: CallAuthorityPermitClaims,
    ) -> Result<SignedCallAuthority, CallAuthorityError> {
        self.issue_claims(request_id, operation, permit)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedCallAuthority {
    authority_id: String,
    request_id: String,
    execution_id: String,
    operation_digest: String,
    permit: CallAuthorityPermitClaims,
    expiry: VerifiedAuthorityExpiry,
}

impl VerifiedCallAuthority {
    pub(super) fn authority_id(&self) -> &str {
        &self.authority_id
    }

    pub(super) fn request_id(&self) -> &str {
        &self.request_id
    }

    pub(super) fn execution_id(&self) -> &str {
        &self.execution_id
    }

    pub(super) fn replay_binding(&self) -> AuthorityReplayBinding {
        AuthorityReplayBinding {
            authority_id: self.authority_id.clone(),
            operation_digest: self.operation_digest.clone(),
            permit: self.permit.clone(),
        }
    }

    pub(super) fn expiry(&self) -> VerifiedAuthorityExpiry {
        self.expiry.clone()
    }
}

#[derive(Clone)]
pub(super) struct VerifiedAuthorityExpiry {
    expires_at_unix_ms: u64,
    clock: Arc<dyn AuthorityClock>,
}

impl VerifiedAuthorityExpiry {
    pub(super) fn ensure_fresh(&self) -> Result<(), CallAuthorityError> {
        if self.expires_at_unix_ms <= self.clock.now_unix_ms()? {
            return Err(CallAuthorityError::Stale);
        }
        Ok(())
    }
}

impl std::fmt::Debug for VerifiedAuthorityExpiry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerifiedAuthorityExpiry")
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .finish_non_exhaustive()
    }
}

impl PartialEq for VerifiedAuthorityExpiry {
    fn eq(&self, other: &Self) -> bool {
        self.expires_at_unix_ms == other.expires_at_unix_ms
    }
}

impl Eq for VerifiedAuthorityExpiry {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct AuthorityReplayBinding {
    pub authority_id: String,
    pub operation_digest: String,
    permit: CallAuthorityPermitClaims,
}

impl AuthorityReplayBinding {
    pub(super) fn grant_digest(&self) -> &str {
        &self.permit.grant_digest
    }
}

pub struct ExecutorCallAuthorityVerifier {
    key_id: String,
    verifying_key: VerifyingKey,
    identity: RpcIdentity,
    clock: Arc<dyn AuthorityClock>,
}

impl ExecutorCallAuthorityVerifier {
    pub fn new(
        key_id: impl Into<String>,
        verifying_key: VerifyingKey,
        identity: RpcIdentity,
    ) -> Result<Self, CallAuthorityError> {
        let key_id = key_id.into();
        validate_bounded_text(&key_id)?;
        Ok(Self {
            key_id,
            verifying_key,
            identity,
            clock: Arc::new(SystemAuthorityClock),
        })
    }

    #[cfg(test)]
    pub(super) fn with_clock(mut self, clock: Arc<dyn AuthorityClock>) -> Self {
        self.clock = clock;
        self
    }

    pub fn verify(
        &self,
        authority: Option<&SignedCallAuthority>,
        request_id: &str,
        operation: &ExecutorOperation,
    ) -> Result<Option<VerifiedCallAuthority>, CallAuthorityError> {
        if matches!(operation, ExecutorOperation::Health { .. }) {
            return if authority.is_none() {
                Ok(None)
            } else {
                Err(CallAuthorityError::UnsupportedOperation)
            };
        }
        if !is_production_read_operation(operation) {
            return Err(CallAuthorityError::UnsupportedOperation);
        }
        let authority = authority.ok_or(CallAuthorityError::Missing)?;
        validate_bounded_text(&authority.key_id)?;
        validate_bounded_text(request_id)?;
        validate_claims_shape(&authority.claims)?;
        if authority.key_id != self.key_id {
            return Err(CallAuthorityError::UnknownKey);
        }
        let signature_bytes =
            hex_decode_64(&authority.signature).ok_or(CallAuthorityError::Malformed)?;
        self.verifying_key
            .verify_strict(
                &signature_payload(&authority.key_id, &authority.claims)?,
                &Signature::from_bytes(&signature_bytes),
            )
            .map_err(|_| CallAuthorityError::InvalidSignature)?;

        let claims = &authority.claims;
        if claims.version != EXECUTOR_CALL_AUTHORITY_VERSION {
            return Err(CallAuthorityError::Malformed);
        }
        if claims.audience != EXECUTOR_CALL_AUTHORITY_AUDIENCE {
            return Err(CallAuthorityError::WrongAudience);
        }
        if claims.generation != self.identity.generation().to_wire() {
            return Err(CallAuthorityError::WrongGeneration);
        }
        if claims.boot_nonce_digest != boot_nonce_digest(self.identity.nonce().as_str()) {
            return Err(CallAuthorityError::WrongBootNonce);
        }
        if claims.request_id != request_id {
            return Err(CallAuthorityError::WrongRequest);
        }
        let execution_id =
            operation_execution_id(operation).ok_or(CallAuthorityError::UnsupportedOperation)?;
        if claims.execution_id != execution_id {
            return Err(CallAuthorityError::WrongExecution);
        }
        let expected_operation_digest = operation_digest(operation)?;
        if claims.operation_digest != expected_operation_digest {
            return Err(CallAuthorityError::WrongOperation);
        }
        let now = self.clock.now_unix_ms()?;
        let max_skew_ms = u64::try_from(MAX_CLOCK_SKEW.as_millis()).expect("clock skew fits u64");
        let max_lifetime_ms =
            u64::try_from(MAX_AUTHORITY_LIFETIME.as_millis()).expect("lifetime fits u64");
        if claims.expires_at_unix_ms <= claims.issued_at_unix_ms
            || claims.expires_at_unix_ms - claims.issued_at_unix_ms > max_lifetime_ms
            || claims.issued_at_unix_ms > now.saturating_add(max_skew_ms)
            || claims.expires_at_unix_ms <= now
        {
            return Err(CallAuthorityError::Stale);
        }
        Ok(Some(VerifiedCallAuthority {
            authority_id: claims.authority_id.clone(),
            request_id: claims.request_id.clone(),
            execution_id: claims.execution_id.clone(),
            operation_digest: claims.operation_digest.clone(),
            permit: claims.permit.clone(),
            expiry: VerifiedAuthorityExpiry {
                expires_at_unix_ms: claims.expires_at_unix_ms,
                clock: self.clock.clone(),
            },
        }))
    }
}

pub trait AuthorityClock: Send + Sync {
    fn now_unix_ms(&self) -> Result<u64, CallAuthorityError>;
}

struct SystemAuthorityClock;

impl AuthorityClock for SystemAuthorityClock {
    fn now_unix_ms(&self) -> Result<u64, CallAuthorityError> {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CallAuthorityError::Clock)?
            .as_millis();
        u64::try_from(millis).map_err(|_| CallAuthorityError::Clock)
    }
}

#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum CallAuthorityError {
    #[error("executor call authority is required")]
    Missing,
    #[error("executor call authority is malformed")]
    Malformed,
    #[error("executor call authority signature is invalid")]
    InvalidSignature,
    #[error("executor call authority key is unknown")]
    UnknownKey,
    #[error("executor call authority audience does not match")]
    WrongAudience,
    #[error("executor call authority generation does not match")]
    WrongGeneration,
    #[error("executor call authority boot nonce does not match")]
    WrongBootNonce,
    #[error("executor call authority request does not match")]
    WrongRequest,
    #[error("executor call authority execution does not match")]
    WrongExecution,
    #[error("executor call authority operation does not match")]
    WrongOperation,
    #[error("executor call authority has expired or is not yet valid")]
    Stale,
    #[error("executor call authority route and provenance do not match")]
    InvalidPermitTuple,
    #[error("executor call authority does not admit this operation")]
    UnsupportedOperation,
    #[error("executor call authority clock is unavailable")]
    Clock,
    #[error("executor call authority was already consumed")]
    Replay,
    #[error("executor call authority replay capacity is exhausted")]
    CapacityExhausted,
}

impl From<CallAuthorityError> for ToolError {
    fn from(value: CallAuthorityError) -> Self {
        Self::Protocol(value.to_string())
    }
}

pub fn operation_digest(operation: &ExecutorOperation) -> Result<String, CallAuthorityError> {
    let value = serde_json::to_value(operation).map_err(|_| CallAuthorityError::Malformed)?;
    let canonical = canonical_json(&value)?;
    Ok(domain_digest(OPERATION_DIGEST_DOMAIN, &canonical))
}

fn signature_payload(
    key_id: &str,
    claims: &CallAuthorityClaims,
) -> Result<Vec<u8>, CallAuthorityError> {
    #[derive(Serialize)]
    #[serde(deny_unknown_fields)]
    struct SignatureEnvelope<'a> {
        key_id: &'a str,
        claims: &'a CallAuthorityClaims,
    }

    let value = serde_json::to_value(SignatureEnvelope { key_id, claims })
        .map_err(|_| CallAuthorityError::Malformed)?;
    let canonical = canonical_json(&value)?;
    let mut payload = Vec::with_capacity(SIGNATURE_DOMAIN.len() + 8 + canonical.len());
    payload.extend_from_slice(SIGNATURE_DOMAIN);
    payload.extend_from_slice(&(canonical.len() as u64).to_be_bytes());
    payload.extend_from_slice(&canonical);
    Ok(payload)
}

fn canonical_json(value: &Value) -> Result<Vec<u8>, CallAuthorityError> {
    fn normalize(value: &Value) -> Value {
        match value {
            Value::Object(object) => {
                let mut entries = object.iter().collect::<Vec<_>>();
                entries.sort_by_key(|(key, _)| *key);
                Value::Object(
                    entries
                        .into_iter()
                        .map(|(key, value)| (key.clone(), normalize(value)))
                        .collect(),
                )
            }
            Value::Array(values) => Value::Array(values.iter().map(normalize).collect()),
            other => other.clone(),
        }
    }
    serde_json::to_vec(&normalize(value)).map_err(|_| CallAuthorityError::Malformed)
}

fn validate_claims_shape(claims: &CallAuthorityClaims) -> Result<(), CallAuthorityError> {
    validate_uuid_v7(&claims.authority_id)?;
    validate_bounded_text(&claims.audience)?;
    validate_bounded_text(&claims.boot_nonce_digest)?;
    validate_bounded_text(&claims.request_id)?;
    validate_bounded_text(&claims.execution_id)?;
    validate_sha256(&claims.operation_digest)?;
    validate_sha256(&claims.boot_nonce_digest)?;
    claims.permit.validate()
}

fn validate_uuid_v7(value: &str) -> Result<(), CallAuthorityError> {
    let uuid = Uuid::parse_str(value).map_err(|_| CallAuthorityError::Malformed)?;
    if uuid.get_version_num() != 7 || uuid.hyphenated().to_string() != value {
        return Err(CallAuthorityError::Malformed);
    }
    Ok(())
}

fn validate_bounded_text(value: &str) -> Result<(), CallAuthorityError> {
    if value.is_empty()
        || value.len() > MAX_BOUND_TEXT_BYTES
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(CallAuthorityError::Malformed);
    }
    Ok(())
}

fn validate_sha256(value: &str) -> Result<(), CallAuthorityError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CallAuthorityError::Malformed);
    }
    Ok(())
}

fn boot_nonce_digest(nonce: &str) -> String {
    domain_digest(BOOT_NONCE_DIGEST_DOMAIN, nonce.as_bytes())
}

fn domain_digest(domain: &[u8], bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
    hex_encode(&digest.finalize())
}

#[cfg(test)]
pub(super) fn test_grant_digest(value: &str) -> String {
    domain_digest(b"sumi.executor.test-grant.v1\0", value.as_bytes())
}

fn hex_encode(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn hex_decode_64(value: &str) -> Option<[u8; 64]> {
    if value.len() != 128 {
        return None;
    }
    let mut decoded = [0; 64];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Some(decoded)
}

pub(crate) fn decode_hex_32(value: &str) -> Result<[u8; 32], CallAuthorityError> {
    if value.len() != 64 {
        return Err(CallAuthorityError::Malformed);
    }
    let mut decoded = [0; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = (hex_nibble(pair[0]).ok_or(CallAuthorityError::Malformed)? << 4)
            | hex_nibble(pair[1]).ok_or(CallAuthorityError::Malformed)?;
    }
    Ok(decoded)
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

pub(crate) const fn call_authority_key_id() -> &'static str {
    EXECUTOR_CALL_AUTHORITY_KEY_ID
}

pub(crate) fn is_production_read_operation(operation: &ExecutorOperation) -> bool {
    matches!(
        operation,
        ExecutorOperation::ReadFile { .. }
            | ExecutorOperation::ListDir { .. }
            | ExecutorOperation::Glob { .. }
            | ExecutorOperation::Grep { .. }
            | ExecutorOperation::OpenSourceFiles { .. }
    )
}

pub(crate) fn operation_execution_id(operation: &ExecutorOperation) -> Option<&str> {
    match operation {
        ExecutorOperation::ReadFile { execution_id, .. }
        | ExecutorOperation::WriteFile { execution_id, .. }
        | ExecutorOperation::EditFile { execution_id, .. }
        | ExecutorOperation::RemoveFile { execution_id, .. }
        | ExecutorOperation::ListDir { execution_id, .. }
        | ExecutorOperation::Glob { execution_id, .. }
        | ExecutorOperation::Grep { execution_id, .. }
        | ExecutorOperation::OpenSourceFiles { execution_id, .. }
        | ExecutorOperation::Bash { execution_id, .. }
        | ExecutorOperation::Cancel { execution_id } => Some(execution_id),
        ExecutorOperation::Health { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{approval::authority::CommittedExecutionPermit, runtime::contracts::RpcIdentity};

    const PAID: &str = "018f47a2-9b3c-7def-8abc-0123456789ab";

    struct FixedClock(u64);

    impl AuthorityClock for FixedClock {
        fn now_unix_ms(&self) -> Result<u64, CallAuthorityError> {
            Ok(self.0)
        }
    }

    fn identity(generation: u64, nonce: &str) -> RpcIdentity {
        RpcIdentity::from_wire(PAID, generation, nonce).unwrap()
    }

    fn operation(path: &str) -> ExecutorOperation {
        ExecutorOperation::ReadFile {
            path: path.to_owned(),
            offset: 0,
            limit: 10,
            execution_id: "exec-1".to_owned(),
        }
    }

    fn permit() -> CallAuthorityPermitClaims {
        CallAuthorityPermitClaims {
            grant_digest: test_grant_digest("grant-1"),
            bound_evidence_digest: "11".repeat(32),
            action_digest: "33".repeat(32),
            authorization_projection_digest: "22".repeat(32),
            route: ExecutorInvocationRoute::Normal,
            resolved_authority: ExecutorAuthorityProvenance::AgentOwn,
        }
    }

    #[test]
    fn exact_operation_and_generation_are_signed_and_verified() {
        let rpc = identity(7, "nonce-a");
        let issuer = Ed25519CallAuthorityIssuer::new(
            call_authority_key_id(),
            SigningKey::from_bytes(&[7; 32]),
            rpc.clone(),
        )
        .unwrap()
        .with_clock(Arc::new(FixedClock(1_000)));
        let verifier = ExecutorCallAuthorityVerifier::new(
            call_authority_key_id(),
            issuer.verifying_key(),
            rpc.clone(),
        )
        .unwrap()
        .with_clock(Arc::new(FixedClock(1_001)));
        let operation = operation("alpha.txt");
        let token = issuer
            .issue(
                "request-1".to_owned(),
                operation.clone(),
                CommittedExecutionPermit::executor_fixture(
                    "grant-1",
                    ToolInvocationRoute::Normal,
                    ExecutionAuthorityProvenance::AgentOwn,
                )
                .begin_executor_effect()
                .into_permit_for_test(),
            )
            .unwrap();
        let serialized = serde_json::to_string(&token).unwrap();
        for forbidden in [
            PAID,
            "grant-1",
            "personality_agent_id",
            "grant_id",
            "authorization_evidence_digest",
            "raw_arguments",
            "conversation",
        ] {
            assert!(
                !serialized.contains(forbidden),
                "Executor token leaked forbidden input {forbidden}"
            );
        }
        assert_eq!(
            token.claims.permit.grant_digest,
            crate::approval::authority::executor_grant_digest("grant-1").unwrap()
        );
        assert_eq!(token.claims.permit.action_digest, "33".repeat(32));
        assert!(
            verifier
                .verify(Some(&token), "request-1", &operation)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            verifier.verify(Some(&token), "request-1", &self::operation("beta.txt")),
            Err(CallAuthorityError::WrongOperation)
        );
        let mut retagged = issuer
            .issue_for_test("request-2".to_owned(), operation.clone(), permit())
            .unwrap();
        retagged.key_id = "sumi.executor.call-authority.ed25519.test".to_owned();
        let retagged_verifier = ExecutorCallAuthorityVerifier::new(
            retagged.key_id.clone(),
            issuer.verifying_key(),
            rpc.clone(),
        )
        .unwrap()
        .with_clock(Arc::new(FixedClock(1_001)));
        assert_eq!(
            retagged_verifier.verify(Some(&retagged), "request-2", &operation),
            Err(CallAuthorityError::InvalidSignature),
            "key_id must be covered by the Ed25519 signature"
        );
        let next = identity(8, "nonce-b");
        let next_verifier = ExecutorCallAuthorityVerifier::new(
            call_authority_key_id(),
            SigningKey::from_bytes(&[9; 32]).verifying_key(),
            next,
        )
        .unwrap()
        .with_clock(Arc::new(FixedClock(1_001)));
        assert_eq!(
            next_verifier.verify(Some(&token), "request-1", &operation),
            Err(CallAuthorityError::InvalidSignature)
        );
        let generation_fence = ExecutorCallAuthorityVerifier::new(
            call_authority_key_id(),
            issuer.verifying_key(),
            identity(8, "nonce-b"),
        )
        .unwrap()
        .with_clock(Arc::new(FixedClock(1_001)));
        assert_eq!(
            generation_fence.verify(Some(&token), "request-1", &operation),
            Err(CallAuthorityError::WrongGeneration)
        );
        let nonce_fence = ExecutorCallAuthorityVerifier::new(
            call_authority_key_id(),
            issuer.verifying_key(),
            identity(7, "nonce-b"),
        )
        .unwrap()
        .with_clock(Arc::new(FixedClock(1_001)));
        assert_eq!(
            nonce_fence.verify(Some(&token), "request-1", &operation),
            Err(CallAuthorityError::WrongBootNonce)
        );
    }

    #[test]
    fn missing_tampered_stale_and_invalid_route_authority_fail_closed() {
        let rpc = identity(7, "nonce-a");
        let issuer = Ed25519CallAuthorityIssuer::new(
            call_authority_key_id(),
            SigningKey::from_bytes(&[7; 32]),
            rpc.clone(),
        )
        .unwrap()
        .with_clock(Arc::new(FixedClock(1_000)));
        let verifier = ExecutorCallAuthorityVerifier::new(
            call_authority_key_id(),
            issuer.verifying_key(),
            rpc,
        )
        .unwrap()
        .with_clock(Arc::new(FixedClock(100_000)));
        let operation = operation("alpha.txt");
        assert_eq!(
            verifier.verify(None, "request-1", &operation),
            Err(CallAuthorityError::Missing)
        );
        let mut token = issuer
            .issue_for_test("request-1".to_owned(), operation.clone(), permit())
            .unwrap();
        assert_eq!(
            verifier.verify(Some(&token), "request-1", &operation),
            Err(CallAuthorityError::Stale)
        );
        token.claims.operation_digest = "33".repeat(32);
        assert_eq!(
            verifier.verify(Some(&token), "request-1", &operation),
            Err(CallAuthorityError::InvalidSignature)
        );
        let invalid = CallAuthorityPermitClaims {
            route: ExecutorInvocationRoute::Normal,
            resolved_authority: ExecutorAuthorityProvenance::AgentOwnWithHumanConsent,
            ..permit()
        };
        assert_eq!(
            issuer.issue_for_test("request-2".to_owned(), operation, invalid),
            Err(CallAuthorityError::InvalidPermitTuple)
        );
        assert_eq!(
            issuer.issue(
                "request-human".to_owned(),
                self::operation("alpha.txt"),
                CommittedExecutionPermit::executor_fixture(
                    "grant-human",
                    ToolInvocationRoute::Elevated,
                    ExecutionAuthorityProvenance::HumanAccountOneShot,
                )
                .begin_executor_effect()
                .into_permit_for_test(),
            ),
            Err(CallAuthorityError::InvalidPermitTuple)
        );
    }
}
