//! Neutral, serializable evidence produced when an app binds a model-facing
//! tool proposal to the exact operation its current UI state denotes.
//!
//! This module deliberately knows nothing about approval routes, reviewers,
//! authority provenance, or execution. A later foundation boundary may bind
//! that metadata around this value, but must not reinterpret the app-owned
//! operation or resource identities recorded here.

use std::{io, path::Path};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

const PROPOSAL_DIGEST_DOMAIN: &[u8] = b"sumi-tool-proposal/v1\0";
const DESCRIPTOR_DIGEST_DOMAIN: &[u8] = b"sumi-bound-tool-descriptor/v1\0";
const EVIDENCE_DIGEST_DOMAIN: &[u8] = b"sumi-bound-tool-evidence/v1\0";
const WORKSPACE_IDENTITY_DOMAIN: &[u8] = b"sumi-workspace-identity/v1\0";

pub(crate) const BOUND_TOOL_INVOCATION_SCHEMA_VERSION: u32 = 3;
const LEGACY_BOUND_TOOL_INVOCATION_SCHEMA_VERSION: u32 = 2;
const MAX_LABEL_BYTES: usize = 256;
const MAX_RESOURCE_ID_BYTES: usize = 16 * 1024;
const MAX_RESOURCE_SCOPES: usize = 256;
const MAX_BOUND_JSON_BYTES: usize = 1024 * 1024;
const MAX_BOUND_JSON_DEPTH: usize = 64;
const MAX_BOUND_JSON_NODES: usize = 65_536;
const MAX_BOUND_CONTAINER_ITEMS: usize = 4_096;
const MAX_BOUND_STRING_BYTES: usize = 256 * 1024;

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
                validate_resource_id(id)
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
        validate_scope_count(resource_scopes.len())?;
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
        validate_scope_count(self.resource_scopes.len())?;
        for scope in &self.resource_scopes {
            scope.validate()?;
        }
        self.resource_scopes.sort();
        self.resource_scopes.dedup();
        Ok(())
    }
}

/// Read-only decoder for provider-review evidence emitted by schema v2.
///
/// New bindings use the exact app-owned descriptor and the frozen live
/// registration. This legacy vocabulary is never consulted for admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LegacyProviderReviewIdentity {
    WorkspaceListV1,
    WorkspaceInvitationListV1,
    WorkspaceInvitationAcceptV1,
    /// Durable recovery identity for seals emitted before authority epochs.
    /// It is deliberately absent from `from_local`: new bindings are V2 only.
    MessagingV1,
    MessagingV2,
    MessagingV3,
    WorkspaceReadFileV1,
    WorkspaceListDirV1,
    WorkspaceGlobV1,
    WorkspaceGrepV1,
    #[cfg(test)]
    FixtureV1,
    #[cfg(test)]
    ExampleV1,
    #[cfg(test)]
    ExampleV2,
    #[cfg(test)]
    OtherExampleV1,
    #[cfg(test)]
    InspectFixtureV1,
    #[cfg(test)]
    AppActionFixtureV1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LegacyProviderReviewAction {
    ListMemberships,
    ListInvitations,
    AcceptInvitation,
    Overview,
    Open,
    OpenAttachment,
    Write,
    React,
    Status,
    ReplyLater,
    ResolveReplyLater,
    GetCallState,
    ReadFile,
    ListDir,
    Glob,
    Grep,
    #[cfg(test)]
    Fixture,
    #[cfg(test)]
    Update,
    #[cfg(test)]
    Inspect,
    #[cfg(test)]
    UpdateRecord,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LegacyProviderReviewNamespace {
    Workspace,
    Messaging,
    FoundationWorkspace,
    #[cfg(test)]
    Fixture,
    #[cfg(test)]
    Example,
    #[cfg(test)]
    Test,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LegacyProviderReviewResourceKind {
    Membership,
    Invitation,
    Workspace,
    Place,
    Message,
    Participant,
    ReplyLaterMarker,
    Attachment,
    Path,
    GlobSelector,
    #[cfg(test)]
    Record,
    #[cfg(test)]
    Item,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyProviderReviewDescriptor {
    pub schema_version: u32,
    pub operation: LegacyProviderReviewAction,
    pub capability: CapabilityClass,
    pub resource_scopes: Vec<LegacyProviderReviewResourceScope>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LegacyProviderReviewScopeType {
    Collection,
    Resource,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyProviderReviewResourceScope {
    pub scope_type: LegacyProviderReviewScopeType,
    pub namespace: LegacyProviderReviewNamespace,
    pub kind: LegacyProviderReviewResourceKind,
    pub count: u64,
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
        validate_bound_json(
            &Value::Object(arguments.clone()),
            "bound execution arguments",
            |reason| DescribeError::InvalidBoundArguments { reason },
        )?;
        Ok(Self(arguments))
    }

    pub(crate) fn as_object(&self) -> &Map<String, Value> {
        &self.0
    }
}

/// App-owned, deliberately bounded details suitable for authenticated Human
/// review and local durable binding. This value is explicit rather than
/// generically derived from execution arguments: an app must retain the
/// operation's meaning, target, and consent payload. AutoReview receives this
/// exact value through the bounded, redacted reviewer request.
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
        validate_bound_json(
            &Value::Object(projection.clone()),
            "review projection",
            |reason| DescribeError::InvalidReviewProjection { reason },
        )?;
        Ok(Self(projection))
    }

    pub(crate) fn as_object(&self) -> &Map<String, Value> {
        &self.0
    }
}

/// Read-only decoder for the structural summary emitted by schema v2.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyProviderReviewProjection {
    pub schema_version: u32,
    pub top_level_fields: u64,
    pub object_fields: u64,
    pub array_items: u64,
    pub text_values: u64,
    pub text_bytes: u64,
    pub text_characters: u64,
    pub number_values: u64,
    pub boolean_values: u64,
    pub null_values: u64,
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
        if flow_id.is_empty()
            || flow_id.len() > MAX_LABEL_BYTES
            || flow_id.chars().any(char::is_control)
        {
            return Err(DescribeError::InvalidExecutionIdentity {
                reason: format!(
                    "flow id must be 1..={MAX_LABEL_BYTES} bytes and contain no control characters"
                ),
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
    #[serde(
        rename = "provider_review_identity",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    legacy_provider_review_identity: Option<LegacyProviderReviewIdentity>,
    #[serde(
        rename = "provider_review_descriptor",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    legacy_provider_review_descriptor: Option<LegacyProviderReviewDescriptor>,
    pub review_projection: ReviewProjection,
    #[serde(
        rename = "provider_review_projection",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    legacy_provider_review_projection: Option<LegacyProviderReviewProjection>,
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
        validate_bound_json(
            &Value::Object(binding.review_projection.as_object().clone()),
            "review projection",
            |reason| DescribeError::InvalidReviewProjection { reason },
        )?;
        validate_bound_json(
            &Value::Object(binding.execution_arguments.as_object().clone()),
            "bound execution arguments",
            |reason| DescribeError::InvalidBoundArguments { reason },
        )?;

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
            legacy_provider_review_identity: None,
            legacy_provider_review_descriptor: None,
            review_projection: binding.review_projection,
            legacy_provider_review_projection: None,
            execution_arguments: binding.execution_arguments,
        })
    }

    pub(crate) fn recompute_descriptor_digest(&self) -> Result<InvocationDigest, DescribeError> {
        if self.schema_version == LEGACY_BOUND_TOOL_INVOCATION_SCHEMA_VERSION {
            let identity = self
                .legacy_provider_review_identity
                .as_ref()
                .ok_or_else(|| DescribeError::InvalidDescriptor {
                    reason: "schema v2 evidence is missing provider review identity".to_owned(),
                })?;
            let descriptor = self
                .legacy_provider_review_descriptor
                .as_ref()
                .ok_or_else(|| DescribeError::InvalidDescriptor {
                    reason: "schema v2 evidence is missing provider review descriptor".to_owned(),
                })?;
            let projection = self
                .legacy_provider_review_projection
                .as_ref()
                .ok_or_else(|| DescribeError::InvalidDescriptor {
                    reason: "schema v2 evidence is missing provider review projection".to_owned(),
                })?;
            return digest_json(
                DESCRIPTOR_DIGEST_DOMAIN,
                &serde_json::json!({
                    "schema_version": self.schema_version,
                    "tool": &self.tool_name,
                    "adapter": &self.adapter,
                    "execution_identity": &self.execution_identity,
                    "descriptor": &self.descriptor,
                    "provider_review_identity": identity,
                    "provider_review_descriptor": descriptor,
                    "review_projection": &self.review_projection,
                    "provider_review_projection": projection,
                    "execution_arguments": &self.execution_arguments,
                }),
            );
        }
        if self.schema_version != BOUND_TOOL_INVOCATION_SCHEMA_VERSION {
            return Err(DescribeError::InvalidDescriptor {
                reason: format!(
                    "unknown bound invocation schema version {}",
                    self.schema_version
                ),
            });
        }
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

    #[cfg(test)]
    pub(crate) fn test_fixture_with_private_values(
        tool_call_id: &str,
        flow_id: &str,
        resource_id: &str,
        private_text: &str,
        capability: CapabilityClass,
    ) -> Self {
        let proposal = serde_json::json!({
            "resource_id": resource_id,
            "private_text": private_text,
        });
        let proposal_arguments = proposal.as_object().expect("fixture proposal is an object");
        Self::seal(
            tool_call_id,
            "fixture_tool",
            proposal_arguments,
            AdapterIdentity::new("sumi.fixture", 1).expect("fixture adapter"),
            BoundExecutionIdentity::seal(flow_id, Path::new("/workspace"))
                .expect("fixture execution identity"),
            ToolBinding::new(
                AppActionDescriptor::new(
                    "fixture.operation",
                    capability,
                    vec![ResourceScope::resource("fixture", "record", resource_id)],
                )
                .expect("fixture descriptor"),
                ReviewProjection::from_value(serde_json::json!({
                    "operation": "fixture.operation",
                    "resource_id": resource_id,
                    "private_text": private_text,
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
    #[error("committed execution permit does not match the exact sealed invocation")]
    ExecutionPermitMismatch,
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!("parse_hex validates lowercase hexadecimal input"),
    }
}

fn validate_label(value: &str, field: &str) -> Result<(), DescribeError> {
    if value.is_empty() || value.len() > MAX_LABEL_BYTES || value.chars().any(char::is_control) {
        return Err(DescribeError::InvalidDescriptor {
            reason: format!(
                "{field} must be 1..={MAX_LABEL_BYTES} bytes and contain no control characters"
            ),
        });
    }
    Ok(())
}

fn validate_resource_id(value: &str) -> Result<(), DescribeError> {
    if value.is_empty()
        || value.len() > MAX_RESOURCE_ID_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(DescribeError::InvalidDescriptor {
            reason: format!(
                "resource id must be 1..={MAX_RESOURCE_ID_BYTES} bytes and contain no control characters"
            ),
        });
    }
    Ok(())
}

fn validate_scope_count(count: usize) -> Result<(), DescribeError> {
    if count > MAX_RESOURCE_SCOPES {
        return Err(DescribeError::InvalidDescriptor {
            reason: format!("resource scope count must not exceed {MAX_RESOURCE_SCOPES}"),
        });
    }
    Ok(())
}

fn validate_proposal_label(value: &str, field: &str) -> Result<(), DescribeError> {
    if value.is_empty() || value.len() > MAX_LABEL_BYTES || value.chars().any(char::is_control) {
        return Err(DescribeError::InvalidProposalIdentity {
            reason: format!(
                "{field} must be 1..={MAX_LABEL_BYTES} bytes and contain no control characters"
            ),
        });
    }
    Ok(())
}

fn validate_bound_json<E>(
    value: &Value,
    field: &str,
    invalid: impl Fn(String) -> E,
) -> Result<(), E> {
    let mut stack = vec![(value, 1_usize)];
    let mut nodes = 0_usize;
    while let Some((value, depth)) = stack.pop() {
        nodes = nodes.saturating_add(1);
        if nodes > MAX_BOUND_JSON_NODES {
            return Err(invalid(format!(
                "{field} must not exceed {MAX_BOUND_JSON_NODES} JSON values"
            )));
        }
        if depth > MAX_BOUND_JSON_DEPTH {
            return Err(invalid(format!(
                "{field} must not exceed JSON depth {MAX_BOUND_JSON_DEPTH}"
            )));
        }
        match value {
            Value::String(text) => {
                if text.len() > MAX_BOUND_STRING_BYTES {
                    return Err(invalid(format!(
                        "{field} string values must not exceed {MAX_BOUND_STRING_BYTES} bytes"
                    )));
                }
                if text.chars().any(|character| {
                    character.is_control() && !matches!(character, '\n' | '\r' | '\t')
                }) {
                    return Err(invalid(format!(
                        "{field} string values contain a disallowed control character"
                    )));
                }
            }
            Value::Array(values) => {
                if values.len() > MAX_BOUND_CONTAINER_ITEMS {
                    return Err(invalid(format!(
                        "{field} arrays must not exceed {MAX_BOUND_CONTAINER_ITEMS} items"
                    )));
                }
                stack.extend(values.iter().map(|value| (value, depth + 1)));
            }
            Value::Object(object) => {
                if object.len() > MAX_BOUND_CONTAINER_ITEMS {
                    return Err(invalid(format!(
                        "{field} objects must not exceed {MAX_BOUND_CONTAINER_ITEMS} fields"
                    )));
                }
                for (key, value) in object {
                    if key.len() > MAX_LABEL_BYTES || key.chars().any(char::is_control) {
                        return Err(invalid(format!(
                            "{field} field names must be at most {MAX_LABEL_BYTES} bytes and contain no control characters"
                        )));
                    }
                    stack.push((value, depth + 1));
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }
    let mut size = CappedJsonSizeWriter::default();
    if let Err(error) = serde_json::to_writer(&mut size, value) {
        if size.exceeded {
            return Err(invalid(format!(
                "{field} must not exceed {MAX_BOUND_JSON_BYTES} encoded bytes"
            )));
        }
        return Err(invalid(format!("{field} serialization failed: {error}")));
    }
    Ok(())
}

#[derive(Default)]
struct CappedJsonSizeWriter {
    written: usize,
    exceeded: bool,
}

impl io::Write for CappedJsonSizeWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_BOUND_JSON_BYTES.saturating_sub(self.written) {
            self.written = MAX_BOUND_JSON_BYTES + 1;
            self.exceeded = true;
            return Err(io::Error::other("bound JSON size limit exceeded"));
        }
        self.written += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
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

    fn seal_generic(
        tool_name: &str,
        adapter_id: &str,
        operation: &str,
        namespace: &str,
        kind: &str,
    ) -> Result<BoundToolInvocation, DescribeError> {
        let proposal = json!({"value": "fixture"});
        BoundToolInvocation::seal(
            "generic-call",
            tool_name,
            proposal.as_object().unwrap(),
            AdapterIdentity::new(adapter_id, 1)?,
            execution_identity("generic-flow", "/workspace"),
            ToolBinding::new(
                AppActionDescriptor::new(
                    operation,
                    CapabilityClass::Mutate,
                    vec![ResourceScope::resource(namespace, kind, "local-id")],
                )?,
                ReviewProjection::from_value(json!({"value":"fixture"}))?,
                BoundExecutionArguments::from_value(proposal.clone())?,
            ),
        )
    }

    #[test]
    fn generic_app_owned_labels_seal_without_foundation_vocabulary() {
        const SENTINEL: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopq";
        let invocation = seal_generic(
            "new_app_tool",
            "app.new-adapter",
            "archive_by_retention_rule",
            "new_app.private_namespace",
            "retention_subject",
        )
        .expect("opaque app-owned descriptor seals");
        assert_eq!(invocation.tool_name, "new_app_tool");
        assert_eq!(invocation.adapter.id, "app.new-adapter");
        assert_eq!(invocation.descriptor.operation, "archive_by_retention_rule");
        assert_eq!(invocation.descriptor.capability, CapabilityClass::Mutate);
        assert_eq!(
            invocation.descriptor.resource_scopes,
            vec![ResourceScope::resource(
                "new_app.private_namespace",
                "retention_subject",
                "local-id"
            )]
        );
        let wire = serde_json::to_value(&invocation).unwrap();
        assert_eq!(wire["schema_version"], BOUND_TOOL_INVOCATION_SCHEMA_VERSION);
        assert!(wire.get("provider_review_identity").is_none());
        assert!(wire.get("provider_review_descriptor").is_none());
        assert!(wire.get("provider_review_projection").is_none());

        assert!(
            seal_generic(
                "new_app_tool",
                "app.new-adapter",
                SENTINEL,
                SENTINEL,
                SENTINEL,
            )
            .is_ok()
        );
    }

    #[test]
    fn exact_descriptor_projection_and_arguments_are_digest_bound() {
        const SENTINEL: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopq";
        let proposal = json!({"content": SENTINEL});
        let invocation = BoundToolInvocation::seal(
            "call-safe-review",
            "example",
            proposal.as_object().unwrap(),
            AdapterIdentity::new("sumi.example", 1).unwrap(),
            execution_identity("flow-safe-review", "/workspace"),
            ToolBinding::new(
                AppActionDescriptor::new(
                    "update",
                    CapabilityClass::Mutate,
                    vec![
                        ResourceScope::resource("example", "record", SENTINEL),
                        ResourceScope::resource("example", "record", "record-b"),
                        ResourceScope::collection("example", "record"),
                    ],
                )
                .unwrap(),
                ReviewProjection::from_value(json!({
                    "action": "update",
                    "content": SENTINEL,
                    "has_content": true
                }))
                .unwrap(),
                BoundExecutionArguments::from_value(proposal.clone()).unwrap(),
            ),
        )
        .unwrap();

        let exact = serde_json::to_string(&invocation.review_projection).unwrap();
        assert!(exact.contains(SENTINEL));
        assert!(exact.contains("content"));
        let exact_descriptor = serde_json::to_string(&invocation.descriptor).unwrap();
        assert!(exact_descriptor.contains(SENTINEL));
        let baseline = invocation.descriptor_digest;
        let mut variants = Vec::new();
        let mut changed = invocation.clone();
        changed.descriptor.operation = "different_operation".to_owned();
        variants.push(changed);
        let mut changed = invocation.clone();
        changed.descriptor.capability = CapabilityClass::Read;
        variants.push(changed);
        let mut changed = invocation.clone();
        changed.descriptor.resource_scopes[0] =
            ResourceScope::resource("other.namespace", "other_kind", "other-id");
        variants.push(changed);
        let mut changed = invocation.clone();
        changed.review_projection = ReviewProjection::from_value(json!({"changed": true})).unwrap();
        variants.push(changed);
        let mut changed = invocation.clone();
        changed.execution_arguments =
            BoundExecutionArguments::from_value(json!({"content": "changed"})).unwrap();
        variants.push(changed);
        for changed in variants {
            assert_ne!(changed.recompute_descriptor_digest().unwrap(), baseline);
        }
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
            "a4db7fd28def17674c97734954ef13e148dd3cacb04bca662eedfb94a052b682"
        );
        assert_eq!(
            left.descriptor_digest.to_hex(),
            "b94d86d2332a8d6d6974069001117b95152fc4b624693f948b997f0bc102a973"
        );
        assert_eq!(left.proposal_digest.as_bytes()[0], 0xa4);

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
    fn schema_v2_provider_vocabulary_remains_read_only_decodable() {
        assert_eq!(
            serde_json::from_str::<LegacyProviderReviewIdentity>("\"messaging_v1\"").unwrap(),
            LegacyProviderReviewIdentity::MessagingV1,
            "durable v1 evidence must remain decodable during recovery"
        );
        assert_eq!(
            serde_json::from_str::<LegacyProviderReviewIdentity>("\"messaging_v2\"").unwrap(),
            LegacyProviderReviewIdentity::MessagingV2,
            "durable v2 evidence must remain decodable during recovery"
        );
        let current = seal_generic(
            "any_tool",
            "any.adapter",
            "new_operation",
            "new_namespace",
            "new_kind",
        )
        .expect("new seals do not consult legacy vocabulary");
        assert_eq!(current.schema_version, BOUND_TOOL_INVOCATION_SCHEMA_VERSION);

        let mut legacy = BoundToolInvocation::seal(
            "legacy-call",
            "example",
            json!({"action":"update","content":"hello"})
                .as_object()
                .unwrap(),
            AdapterIdentity::new("sumi.example", 1).unwrap(),
            execution_identity("legacy-flow", "/workspace"),
            binding(json!({
                "action":"update",
                "record_id":"record-a",
                "content":"hello",
                "urgency":"normal"
            })),
        )
        .unwrap();
        legacy.schema_version = LEGACY_BOUND_TOOL_INVOCATION_SCHEMA_VERSION;
        legacy.legacy_provider_review_identity = Some(LegacyProviderReviewIdentity::ExampleV1);
        legacy.legacy_provider_review_descriptor = Some(LegacyProviderReviewDescriptor {
            schema_version: 1,
            operation: LegacyProviderReviewAction::Update,
            capability: CapabilityClass::Mutate,
            resource_scopes: vec![LegacyProviderReviewResourceScope {
                scope_type: LegacyProviderReviewScopeType::Resource,
                namespace: LegacyProviderReviewNamespace::Example,
                kind: LegacyProviderReviewResourceKind::Record,
                count: 1,
            }],
        });
        legacy.legacy_provider_review_projection = Some(LegacyProviderReviewProjection {
            schema_version: 1,
            top_level_fields: 4,
            object_fields: 4,
            array_items: 0,
            text_values: 3,
            text_bytes: 19,
            text_characters: 19,
            number_values: 1,
            boolean_values: 0,
            null_values: 0,
        });
        legacy.descriptor_digest = legacy.recompute_descriptor_digest().unwrap();
        let encoded = serde_json::to_vec(&legacy).unwrap();
        let decoded: BoundToolInvocation = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, legacy);
        assert_eq!(
            decoded.recompute_descriptor_digest().unwrap(),
            legacy.descriptor_digest
        );
    }

    #[test]
    fn descriptor_and_payload_bounds_fail_closed() {
        assert!(
            AppActionDescriptor::new(
                "x".repeat(MAX_LABEL_BYTES + 1),
                CapabilityClass::Read,
                vec![]
            )
            .is_err()
        );
        assert!(AppActionDescriptor::new("bad\noperation", CapabilityClass::Read, vec![]).is_err());
        assert!(
            AppActionDescriptor::new(
                "read",
                CapabilityClass::Read,
                vec![ResourceScope::resource(
                    "app",
                    "record",
                    &"x".repeat(MAX_RESOURCE_ID_BYTES + 1)
                )],
            )
            .is_err()
        );
        assert!(
            AppActionDescriptor::new(
                "read",
                CapabilityClass::Read,
                (0..=MAX_RESOURCE_SCOPES)
                    .map(|index| ResourceScope::resource("app", "record", &format!("id-{index}")))
                    .collect(),
            )
            .is_err()
        );
        assert!(ReviewProjection::from_value(json!({"value": "bad\u{0}value"})).is_err());
        assert!(
            BoundExecutionArguments::from_value(json!({
                "value": "x".repeat(MAX_BOUND_JSON_BYTES)
            }))
            .is_err()
        );

        let individually_bounded_values = (0..5)
            .map(|_| Value::String("x".repeat(MAX_BOUND_STRING_BYTES)))
            .collect::<Vec<_>>();
        assert!(individually_bounded_values.len() < MAX_BOUND_CONTAINER_ITEMS);
        assert!(
            BoundExecutionArguments::from_value(json!({
                "values": individually_bounded_values
            }))
            .is_err(),
            "aggregate JSON above the cap must fail without allocating an encoded copy"
        );
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
