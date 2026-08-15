//! Neutral, serializable evidence produced when an app binds a model-facing
//! tool proposal to the exact operation its current UI state denotes.
//!
//! This module deliberately knows nothing about approval routes, reviewers,
//! authority provenance, or execution. A later foundation boundary may bind
//! that metadata around this value, but must not reinterpret the app-owned
//! operation or resource identities recorded here.

use std::path::Path;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

const PROPOSAL_DIGEST_DOMAIN: &[u8] = b"sumi-tool-proposal/v1\0";
const DESCRIPTOR_DIGEST_DOMAIN: &[u8] = b"sumi-bound-tool-descriptor/v1\0";
const EVIDENCE_DIGEST_DOMAIN: &[u8] = b"sumi-bound-tool-evidence/v1\0";
const WORKSPACE_IDENTITY_DOMAIN: &[u8] = b"sumi-workspace-identity/v1\0";

pub(crate) const BOUND_TOOL_INVOCATION_SCHEMA_VERSION: u32 = 1;

/// Coarse capability selected by trusted app adapter code.
///
/// Foundation policy may reason about this small, stable class, but must not
/// switch on the app-owned operation string or derive this class from
/// [`crate::tools::ToolRisk`]. The app remains responsible for mapping its
/// complete action vocabulary and for commit-time authorization.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CapabilityClass {
    Read,
    Mutate,
    Administer,
    Execute,
}

/// Exact app-resolved target evidence for review, audit, and cache binding.
///
/// A resource scope is not standing permission, Approval, or execution
/// authority. The app must still re-check membership, visibility, roles, and
/// domain invariants when it commits an effect.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ResourceScope {
    Collection {
        namespace: String,
        kind: String,
    },
    Resource {
        namespace: String,
        kind: String,
        id: String,
    },
}

impl ResourceScope {
    pub(crate) fn collection(namespace: &str, kind: &str) -> Self {
        Self::Collection {
            namespace: namespace.to_owned(),
            kind: kind.to_owned(),
        }
    }

    pub(crate) fn resource(namespace: &str, kind: &str, id: &str) -> Self {
        Self::Resource {
            namespace: namespace.to_owned(),
            kind: kind.to_owned(),
            id: id.to_owned(),
        }
    }

    fn validate(&self) -> Result<(), DescribeError> {
        match self {
            Self::Collection { namespace, kind } => {
                validate_label(namespace, "resource namespace")?;
                validate_label(kind, "resource kind")
            }
            Self::Resource {
                namespace,
                kind,
                id,
            } => {
                validate_label(namespace, "resource namespace")?;
                validate_label(kind, "resource kind")?;
                validate_label(id, "resource id")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AdapterIdentity {
    pub id: String,
    pub version: u32,
}

impl AdapterIdentity {
    pub(crate) fn new(id: impl Into<String>, version: u32) -> Result<Self, DescribeError> {
        let identity = Self {
            id: id.into(),
            version,
        };
        identity.validate()?;
        Ok(identity)
    }

    pub(crate) fn validate(&self) -> Result<(), DescribeError> {
        validate_label(&self.id, "adapter id")?;
        if self.version == 0 {
            return Err(DescribeError::InvalidDescriptor {
                reason: "adapter version must be non-zero".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AppActionDescriptor {
    pub operation: String,
    pub capability: CapabilityClass,
    pub resource_scopes: Vec<ResourceScope>,
}

impl AppActionDescriptor {
    pub(crate) fn new(
        operation: impl Into<String>,
        capability: CapabilityClass,
        mut resource_scopes: Vec<ResourceScope>,
    ) -> Result<Self, DescribeError> {
        let operation = operation.into();
        validate_label(&operation, "operation")?;
        for scope in &resource_scopes {
            scope.validate()?;
        }
        resource_scopes.sort();
        resource_scopes.dedup();
        Ok(Self {
            operation,
            capability,
            resource_scopes,
        })
    }

    fn normalize_and_validate(&mut self) -> Result<(), DescribeError> {
        validate_label(&self.operation, "operation")?;
        for scope in &self.resource_scopes {
            scope.validate()?;
        }
        self.resource_scopes.sort();
        self.resource_scopes.dedup();
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct BoundExecutionArguments(Map<String, Value>);

impl BoundExecutionArguments {
    pub(crate) fn from_value(value: Value) -> Result<Self, DescribeError> {
        let Value::Object(arguments) = value else {
            return Err(DescribeError::InvalidBoundArguments {
                reason: "bound execution arguments must be a JSON object".to_owned(),
            });
        };
        Ok(Self(arguments))
    }

    pub(crate) fn as_object(&self) -> &Map<String, Value> {
        &self.0
    }
}

/// App-owned, deliberately bounded details suitable for later review UIs and
/// reviewers. This value is explicit rather than generically derived from
/// execution arguments: an app must retain the operation's meaning, target,
/// and reviewable payload while omitting credentials, opaque blobs, and other
/// fields that are neither safe nor useful to review.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct ReviewProjection(Map<String, Value>);

impl ReviewProjection {
    pub(crate) fn from_value(value: Value) -> Result<Self, DescribeError> {
        let Value::Object(projection) = value else {
            return Err(DescribeError::InvalidReviewProjection {
                reason: "review projection must be a JSON object".to_owned(),
            });
        };
        Ok(Self(projection))
    }

    pub(crate) fn as_object(&self) -> &Map<String, Value> {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ToolBinding {
    pub descriptor: AppActionDescriptor,
    pub review_projection: ReviewProjection,
    pub execution_arguments: BoundExecutionArguments,
}

impl ToolBinding {
    pub(crate) fn new(
        descriptor: AppActionDescriptor,
        review_projection: ReviewProjection,
        execution_arguments: BoundExecutionArguments,
    ) -> Self {
        Self {
            descriptor,
            review_projection,
            execution_arguments,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct InvocationDigest([u8; 32]);

impl InvocationDigest {
    pub(crate) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub(crate) fn to_hex(self) -> String {
        let mut encoded = String::with_capacity(64);
        for byte in self.0 {
            use std::fmt::Write as _;
            write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
        }
        encoded
    }

    fn parse_hex(encoded: &str) -> Result<Self, &'static str> {
        if encoded.len() != 64
            || !encoded
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
        {
            return Err("invocation digest must be exactly 64 lowercase hexadecimal characters");
        }
        let mut bytes = [0_u8; 32];
        for (index, chunk) in encoded.as_bytes().chunks_exact(2).enumerate() {
            bytes[index] = (hex_nibble(chunk[0]) << 4) | hex_nibble(chunk[1]);
        }
        Ok(Self(bytes))
    }
}

/// Serializable identity of the durable tool flow and workspace used while
/// the app resolved this invocation.
///
/// `flow_id` together with `BoundToolInvocation::tool_call_id` names the
/// durable execution. `workspace_digest` is a safe, domain-separated digest
/// of the exact workspace root; the root path itself is deliberately absent
/// from durable evidence. Neither field is live execution authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BoundExecutionIdentity {
    pub flow_id: String,
    pub workspace_digest: InvocationDigest,
}

impl BoundExecutionIdentity {
    pub(super) fn seal(flow_id: &str, workspace_root: &Path) -> Result<Self, DescribeError> {
        if flow_id.is_empty() || flow_id.chars().any(char::is_control) {
            return Err(DescribeError::InvalidExecutionIdentity {
                reason: "flow id must be non-empty and contain no control characters".to_owned(),
            });
        }
        Ok(Self {
            flow_id: flow_id.to_owned(),
            workspace_digest: digest_workspace_root(workspace_root),
        })
    }
}

impl std::fmt::Display for InvocationDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl std::fmt::Debug for InvocationDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("InvocationDigest")
            .field(&self.to_hex())
            .finish()
    }
}

impl Serialize for InvocationDigest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for InvocationDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        Self::parse_hex(&encoded).map_err(de::Error::custom)
    }
}

/// Durable evidence and review identity.
///
/// Every field in this serializable value is evidence only, including the
/// adapter, capability, resource scopes, review projection, exact execution
/// arguments, flow id, and workspace digest. Deserialization never restores
/// the live registry/registration binding, the concrete workspace paths, an
/// Approval, policy authority, or a right to execute.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BoundToolInvocation {
    pub schema_version: u32,
    pub tool_call_id: String,
    pub tool_name: String,
    pub adapter: AdapterIdentity,
    pub proposal_digest: InvocationDigest,
    pub descriptor_digest: InvocationDigest,
    pub execution_identity: BoundExecutionIdentity,
    pub descriptor: AppActionDescriptor,
    pub review_projection: ReviewProjection,
    pub execution_arguments: BoundExecutionArguments,
}

impl BoundToolInvocation {
    pub(super) fn seal(
        tool_call_id: &str,
        tool_name: &str,
        proposal_arguments: &Map<String, Value>,
        adapter: AdapterIdentity,
        execution_identity: BoundExecutionIdentity,
        mut binding: ToolBinding,
    ) -> Result<Self, DescribeError> {
        validate_proposal_label(tool_call_id, "tool call id")?;
        validate_proposal_label(tool_name, "tool name")?;
        adapter.validate()?;
        binding.descriptor.normalize_and_validate()?;

        let proposal_digest = digest_json(
            PROPOSAL_DIGEST_DOMAIN,
            &serde_json::json!({
                "schema_version": BOUND_TOOL_INVOCATION_SCHEMA_VERSION,
                "tool": tool_name,
                "arguments": proposal_arguments,
            }),
        )?;
        let descriptor_digest = digest_json(
            DESCRIPTOR_DIGEST_DOMAIN,
            &serde_json::json!({
                "schema_version": BOUND_TOOL_INVOCATION_SCHEMA_VERSION,
                "tool": tool_name,
                "adapter": &adapter,
                "execution_identity": &execution_identity,
                "descriptor": &binding.descriptor,
                "review_projection": &binding.review_projection,
                "execution_arguments": &binding.execution_arguments,
            }),
        )?;

        Ok(Self {
            schema_version: BOUND_TOOL_INVOCATION_SCHEMA_VERSION,
            tool_call_id: tool_call_id.to_owned(),
            tool_name: tool_name.to_owned(),
            adapter,
            proposal_digest,
            descriptor_digest,
            execution_identity,
            descriptor: binding.descriptor,
            review_projection: binding.review_projection,
            execution_arguments: binding.execution_arguments,
        })
    }

    pub(crate) fn recompute_descriptor_digest(&self) -> Result<InvocationDigest, DescribeError> {
        digest_json(
            DESCRIPTOR_DIGEST_DOMAIN,
            &serde_json::json!({
                "schema_version": self.schema_version,
                "tool": &self.tool_name,
                "adapter": &self.adapter,
                "execution_identity": &self.execution_identity,
                "descriptor": &self.descriptor,
                "review_projection": &self.review_projection,
                "execution_arguments": &self.execution_arguments,
            }),
        )
    }

    /// Read-only digest of the complete serializable bound evidence tuple.
    ///
    /// Approval/store routes may persist or compare this value, but it is not
    /// an authority token and cannot recreate the live execution seal.
    pub(crate) fn evidence_digest(&self) -> Result<InvocationDigest, DescribeError> {
        digest_json(
            EVIDENCE_DIGEST_DOMAIN,
            &serde_json::to_value(self).map_err(|error| DescribeError::InvalidDescriptor {
                reason: format!("bound evidence serialization failed: {error}"),
            })?,
        )
    }

    #[cfg(test)]
    pub(crate) fn test_fixture(tool_call_id: &str, capability: CapabilityClass) -> Self {
        let proposal = serde_json::json!({"target":"fixture-record"});
        let proposal_arguments = proposal.as_object().expect("fixture proposal is an object");
        Self::seal(
            tool_call_id,
            "fixture_tool",
            proposal_arguments,
            AdapterIdentity::new("sumi.fixture", 1).expect("fixture adapter"),
            BoundExecutionIdentity::seal("fixture-flow", Path::new("/workspace"))
                .expect("fixture execution identity"),
            ToolBinding::new(
                AppActionDescriptor::new(
                    "fixture.operation",
                    capability,
                    vec![ResourceScope::resource(
                        "fixture",
                        "record",
                        "fixture-record",
                    )],
                )
                .expect("fixture descriptor"),
                ReviewProjection::from_value(serde_json::json!({
                    "operation":"fixture.operation",
                    "target":"fixture-record"
                }))
                .expect("fixture review projection"),
                BoundExecutionArguments::from_value(proposal.clone())
                    .expect("fixture execution arguments"),
            ),
        )
        .expect("fixture bound invocation")
    }
}

/// App-owned, bounded explanation of why current local view state cannot bind
/// a proposal. The neutral layer carries an opaque stable code and safe review
/// message; it never enumerates any app's nouns or actions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AppPrecondition {
    pub code: String,
    pub message: String,
}

impl AppPrecondition {
    pub(crate) fn new(
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Result<Self, DescribeError> {
        let code = code.into();
        let message = message.into();
        if code.is_empty()
            || code.len() > 128
            || !code.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
            })
        {
            return Err(DescribeError::InvalidAppPrecondition {
                reason: "code must be 1..=128 lowercase ASCII letters, digits, '.', '_', or '-'"
                    .to_owned(),
            });
        }
        if message.is_empty() || message.len() > 1024 || message.chars().any(char::is_control) {
            return Err(DescribeError::InvalidAppPrecondition {
                reason: "message must be 1..=1024 bytes and contain no control characters"
                    .to_owned(),
            });
        }
        Ok(Self { code, message })
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum DescribeError {
    #[error("unknown frozen tool: {tool}")]
    UnknownTool { tool: String },
    #[error("registered tool has no complete bound adapter: {tool}")]
    MissingBoundAdapter { tool: String },
    #[error("tool proposal identity is invalid: {reason}")]
    InvalidProposalIdentity { reason: String },
    #[error("bound execution identity is invalid: {reason}")]
    InvalidExecutionIdentity { reason: String },
    #[error("tool arguments did not match the app-owned binding schema")]
    InvalidArguments,
    #[error("app binding is temporarily unavailable")]
    BindingUnavailable,
    #[error("app binding failed internal protocol validation")]
    BindingInternal,
    #[error("app precondition failed ({precondition:?})")]
    AppPrecondition { precondition: AppPrecondition },
    #[error("app precondition is invalid: {reason}")]
    InvalidAppPrecondition { reason: String },
    #[error("app-owned descriptor is invalid: {reason}")]
    InvalidDescriptor { reason: String },
    #[error("bound execution arguments are invalid: {reason}")]
    InvalidBoundArguments { reason: String },
    #[error("app-owned review projection is invalid: {reason}")]
    InvalidReviewProjection { reason: String },
    #[error("bound invocation belongs to a different frozen registry or registration")]
    RegistryIdentityMismatch,
    #[error("bound invocation digest or identity was altered after registry sealing")]
    SealedEvidenceMismatch,
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!("parse_hex validates lowercase hexadecimal input"),
    }
}

fn validate_label(value: &str, field: &str) -> Result<(), DescribeError> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(DescribeError::InvalidDescriptor {
            reason: format!("{field} must be non-empty and contain no control characters"),
        });
    }
    Ok(())
}

fn validate_proposal_label(value: &str, field: &str) -> Result<(), DescribeError> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(DescribeError::InvalidProposalIdentity {
            reason: format!("{field} must be non-empty and contain no control characters"),
        });
    }
    Ok(())
}

fn digest_json(domain: &[u8], value: &Value) -> Result<InvocationDigest, DescribeError> {
    let encoded = serde_json::to_vec(&canonical_json(value)).map_err(|error| {
        DescribeError::InvalidDescriptor {
            reason: format!("canonical JSON serialization failed: {error}"),
        }
    })?;
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update((encoded.len() as u64).to_be_bytes());
    digest.update(encoded);
    Ok(InvocationDigest(digest.finalize().into()))
}

fn digest_workspace_root(root: &Path) -> InvocationDigest {
    let mut digest = Sha256::new();
    digest.update(WORKSPACE_IDENTITY_DOMAIN);

    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;

        let bytes = root.as_os_str().as_bytes();
        digest.update(b"unix\0");
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }

    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;

        let units = root.as_os_str().encode_wide().collect::<Vec<_>>();
        digest.update(b"windows-utf16\0");
        digest.update((units.len() as u64).to_be_bytes());
        for unit in units {
            digest.update(unit.to_be_bytes());
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        let encoded = root.to_string_lossy();
        digest.update(b"lossy-utf8\0");
        digest.update((encoded.len() as u64).to_be_bytes());
        digest.update(encoded.as_bytes());
    }

    InvocationDigest(digest.finalize().into())
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical_json).collect()),
        Value::Object(object) => {
            let mut keys = object.keys().collect::<Vec<_>>();
            keys.sort();
            let mut canonical = Map::new();
            for key in keys {
                canonical.insert(key.clone(), canonical_json(&object[key]));
            }
            Value::Object(canonical)
        }
        scalar => scalar.clone(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn binding(arguments: Value) -> ToolBinding {
        ToolBinding::new(
            AppActionDescriptor::new(
                "update",
                CapabilityClass::Mutate,
                vec![ResourceScope::resource("example", "record", "record-a")],
            )
            .unwrap(),
            ReviewProjection::from_value(serde_json::json!({
                "action": "update",
                "record_id": "record-a",
                "content": "hello",
                "content_bytes": 5
            }))
            .unwrap(),
            BoundExecutionArguments::from_value(arguments).unwrap(),
        )
    }

    fn execution_identity(flow_id: &str, workspace: &str) -> BoundExecutionIdentity {
        BoundExecutionIdentity::seal(flow_id, Path::new(workspace)).unwrap()
    }

    fn raw_object(raw: &str) -> Map<String, Value> {
        serde_json::from_str::<Value>(raw)
            .unwrap()
            .as_object()
            .unwrap()
            .clone()
    }

    #[test]
    fn differently_ordered_raw_json_has_one_versioned_domain_digest() {
        let left_raw = r#"{"action":"update","content":"hello","options":{"z":1,"a":2}}"#;
        let reordered_raw = r#"{"options":{"a":2,"z":1},"content":"hello","action":"update"}"#;
        assert_ne!(left_raw.as_bytes(), reordered_raw.as_bytes());

        let left_arguments = raw_object(left_raw);
        let reordered_arguments = raw_object(reordered_raw);
        let left_execution = raw_object(
            r#"{"action":"update","record_id":"record-a","content":"hello","urgency":"normal"}"#,
        );
        let reordered_execution = raw_object(
            r#"{"urgency":"normal","content":"hello","record_id":"record-a","action":"update"}"#,
        );
        let adapter = AdapterIdentity::new("sumi.example", 1).unwrap();
        let identity = execution_identity("flow-1", "/workspace");
        let left = BoundToolInvocation::seal(
            "call-1",
            "example",
            &left_arguments,
            adapter.clone(),
            identity.clone(),
            binding(Value::Object(left_execution)),
        )
        .unwrap();
        let reordered = BoundToolInvocation::seal(
            "call-1",
            "example",
            &reordered_arguments,
            adapter.clone(),
            identity.clone(),
            binding(Value::Object(reordered_execution)),
        )
        .unwrap();
        let changed = BoundToolInvocation::seal(
            "call-1",
            "example",
            json!({"action":"update","content":"goodbye","options":{"z":1,"a":2}})
                .as_object()
                .unwrap(),
            adapter,
            identity.clone(),
            binding(json!({
                "action":"update", "record_id":"record-a", "content":"goodbye", "urgency":"normal"
            })),
        )
        .unwrap();
        let changed_adapter = BoundToolInvocation::seal(
            "call-1",
            "example",
            &left_arguments,
            AdapterIdentity::new("sumi.example", 2).unwrap(),
            identity.clone(),
            binding(json!({
                "action":"update", "record_id":"record-a", "content":"hello", "urgency":"normal"
            })),
        )
        .unwrap();
        let changed_tool = BoundToolInvocation::seal(
            "call-1",
            "other_example",
            &left_arguments,
            AdapterIdentity::new("sumi.example", 1).unwrap(),
            identity.clone(),
            binding(json!({
                "action":"update", "record_id":"record-a", "content":"hello", "urgency":"normal"
            })),
        )
        .unwrap();
        let changed_workspace = BoundToolInvocation::seal(
            "call-1",
            "example",
            &left_arguments,
            AdapterIdentity::new("sumi.example", 1).unwrap(),
            execution_identity("flow-1", "/other-workspace"),
            binding(json!({
                "action":"update", "record_id":"record-a", "content":"hello", "urgency":"normal"
            })),
        )
        .unwrap();

        assert_eq!(left.proposal_digest, reordered.proposal_digest);
        assert_eq!(left.descriptor_digest, reordered.descriptor_digest);
        assert_eq!(
            left.evidence_digest().unwrap(),
            reordered.evidence_digest().unwrap()
        );
        assert_ne!(left.proposal_digest, changed.proposal_digest);
        assert_ne!(left.descriptor_digest, changed.descriptor_digest);
        assert_ne!(
            left.evidence_digest().unwrap(),
            changed.evidence_digest().unwrap()
        );
        assert_eq!(left.proposal_digest, changed_adapter.proposal_digest);
        assert_ne!(left.descriptor_digest, changed_adapter.descriptor_digest);
        assert_ne!(left.proposal_digest, changed_tool.proposal_digest);
        assert_ne!(left.descriptor_digest, changed_tool.descriptor_digest);
        assert_eq!(left.proposal_digest, changed_workspace.proposal_digest);
        assert_ne!(left.descriptor_digest, changed_workspace.descriptor_digest);
        assert_ne!(
            left.evidence_digest().unwrap(),
            changed_workspace.evidence_digest().unwrap()
        );
        assert_eq!(
            left.proposal_digest.to_hex(),
            "fe6b65eb17a39149a8fdee722c6b4c4d32bd22022413bf02ef0d332f747bf4d2"
        );
        assert_eq!(
            left.descriptor_digest.to_hex(),
            "dd27183f568277f033835831ecee12fcaa18f924e6d76b954a0adb49a19aaa3c"
        );
        assert_eq!(left.proposal_digest.as_bytes()[0], 0xfe);

        let later_call = BoundToolInvocation::seal(
            "call-2",
            "example",
            &left_arguments,
            AdapterIdentity::new("sumi.example", 1).unwrap(),
            identity,
            binding(json!({
                "action":"update", "record_id":"record-a", "content":"hello", "urgency":"normal"
            })),
        )
        .unwrap();
        assert_eq!(left.proposal_digest, later_call.proposal_digest);
        assert_eq!(left.descriptor_digest, later_call.descriptor_digest);
        assert_ne!(left.tool_call_id, later_call.tool_call_id);
        assert_ne!(
            left.evidence_digest().unwrap(),
            later_call.evidence_digest().unwrap()
        );
    }

    #[test]
    fn bound_invocation_round_trips_as_untrusted_serializable_evidence() {
        let invocation = BoundToolInvocation::seal(
            "call-1",
            "example",
            json!({"action":"update","content":"hello"})
                .as_object()
                .unwrap(),
            AdapterIdentity::new("sumi.example", 1).unwrap(),
            execution_identity("flow-1", "/workspace"),
            binding(json!({
                "action":"update", "record_id":"record-a", "content":"hello", "urgency":"normal"
            })),
        )
        .unwrap();
        let encoded = serde_json::to_vec(&invocation).unwrap();
        let decoded: BoundToolInvocation = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, invocation);
        assert_eq!(
            decoded.evidence_digest().unwrap(),
            invocation.evidence_digest().unwrap()
        );

        let encoded_value: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            encoded_value["proposal_digest"],
            invocation.proposal_digest.to_hex()
        );
        assert_eq!(
            encoded_value["descriptor_digest"],
            invocation.descriptor_digest.to_hex()
        );
        assert_eq!(encoded_value["execution_identity"]["flow_id"], "flow-1");
        assert_eq!(
            encoded_value["execution_identity"]["workspace_digest"],
            invocation.execution_identity.workspace_digest.to_hex()
        );
        assert!(
            encoded_value.to_string().find("/workspace").is_none(),
            "durable evidence must not expose the workspace path"
        );

        let mut uppercase = encoded_value;
        uppercase["proposal_digest"] =
            Value::String(invocation.proposal_digest.to_hex().to_uppercase());
        assert!(serde_json::from_value::<BoundToolInvocation>(uppercase).is_err());
    }

    #[test]
    fn app_preconditions_are_generic_bounded_evidence() {
        let precondition = AppPrecondition::new("visible_target_required", "select one item")
            .expect("valid app-owned precondition");
        assert_eq!(precondition.code, "visible_target_required");
        assert_eq!(precondition.message, "select one item");

        for code in ["", "UPPERCASE", "contains space", "line\nbreak"] {
            assert!(AppPrecondition::new(code, "safe message").is_err());
        }
        assert!(AppPrecondition::new("valid_code", "line\nbreak").is_err());
        assert!(AppPrecondition::new("x".repeat(129), "safe message").is_err());
        assert!(AppPrecondition::new("valid_code", "x".repeat(1025)).is_err());
    }
}
