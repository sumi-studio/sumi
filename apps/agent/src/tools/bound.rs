//! Neutral, serializable evidence produced when an app binds a model-facing
//! tool proposal to the exact operation its current UI state denotes.
//!
//! This module deliberately knows nothing about approval routes, reviewers,
//! authority provenance, or execution. A later foundation boundary may bind
//! that metadata around this value, but must not reinterpret the app-owned
//! operation or resource identities recorded here.

use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

const PROPOSAL_DIGEST_DOMAIN: &[u8] = b"sumi-tool-proposal/v1\0";
const DESCRIPTOR_DIGEST_DOMAIN: &[u8] = b"sumi-bound-tool-descriptor/v1\0";
const EVIDENCE_DIGEST_DOMAIN: &[u8] = b"sumi-bound-tool-evidence/v1\0";
const WORKSPACE_IDENTITY_DOMAIN: &[u8] = b"sumi-workspace-identity/v1\0";

pub(crate) const BOUND_TOOL_INVOCATION_SCHEMA_VERSION: u32 = 2;
const PROVIDER_REVIEW_DESCRIPTOR_SCHEMA_VERSION: u32 = 1;
const PROVIDER_REVIEW_PROJECTION_SCHEMA_VERSION: u32 = 1;

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

/// Closed provider-visible identity for a production tool registration and
/// bound adapter pair. Local tool and adapter identities remain exact strings;
/// only an explicitly audited pair can cross the external review boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProviderReviewIdentity {
    WorkspaceListV1,
    WorkspaceInvitationListV1,
    WorkspaceInvitationAcceptV1,
    MessagingV1,
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

impl ProviderReviewIdentity {
    fn from_local(tool_name: &str, adapter: &AdapterIdentity) -> Result<Self, DescribeError> {
        let identity = match (tool_name, adapter.id.as_str(), adapter.version) {
            ("workspace_list", "sumi.workspace.list", 1) => Self::WorkspaceListV1,
            ("workspace_invitation_list", "sumi.workspace.invitation.list", 1) => {
                Self::WorkspaceInvitationListV1
            }
            ("workspace_invitation_accept", "sumi.workspace.invitation.accept", 1) => {
                Self::WorkspaceInvitationAcceptV1
            }
            ("messaging", "sumi.messaging", 1) => Self::MessagingV1,
            ("read_file", "sumi.foundation.workspace", 1) => Self::WorkspaceReadFileV1,
            ("list_dir", "sumi.foundation.workspace", 1) => Self::WorkspaceListDirV1,
            ("glob", "sumi.foundation.workspace", 1) => Self::WorkspaceGlobV1,
            ("grep", "sumi.foundation.workspace", 1) => Self::WorkspaceGrepV1,
            #[cfg(test)]
            ("fixture_tool", "sumi.fixture", 1) => Self::FixtureV1,
            #[cfg(test)]
            ("example", "sumi.example", 1) => Self::ExampleV1,
            #[cfg(test)]
            ("example", "sumi.example", 2) => Self::ExampleV2,
            #[cfg(test)]
            ("other_example", "sumi.example", 1) => Self::OtherExampleV1,
            #[cfg(test)]
            ("inspect", "test.binding", 1) => Self::InspectFixtureV1,
            #[cfg(test)]
            ("app_action", "test.app", 1) => Self::AppActionFixtureV1,
            _ => {
                return Err(DescribeError::InvalidDescriptor {
                    reason: "tool/adapter pair has no closed provider review identity".to_owned(),
                });
            }
        };
        Ok(identity)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProviderReviewOperation {
    ListMemberships,
    ListInvitations,
    AcceptInvitation,
    Overview,
    Open,
    Write,
    React,
    Status,
    ReplyLater,
    ResolveReplyLater,
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
pub(crate) enum ProviderReviewNamespace {
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
pub(crate) enum ProviderReviewResourceKind {
    Membership,
    Invitation,
    Workspace,
    Place,
    Message,
    Participant,
    ReplyLaterMarker,
    Path,
    GlobSelector,
    #[cfg(test)]
    Record,
    #[cfg(test)]
    Item,
}

/// Provider-visible shape of an exact local app action descriptor.
///
/// Every textual vocabulary member is converted through the closed production
/// mapping above. Exact resource identifiers remain local because paths,
/// patterns, opaque tokens, and other caller-controlled strings may appear in
/// that position. The external reviewer sees only whether each closed scope
/// class names a collection or concrete resource and how many distinct scopes
/// of that class were bound.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProviderReviewDescriptor {
    pub schema_version: u32,
    pub operation: ProviderReviewOperation,
    pub capability: CapabilityClass,
    pub resource_scopes: Vec<ProviderReviewResourceScope>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProviderReviewScopeType {
    Collection,
    Resource,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProviderReviewResourceScope {
    pub scope_type: ProviderReviewScopeType,
    pub namespace: ProviderReviewNamespace,
    pub kind: ProviderReviewResourceKind,
    pub count: u64,
}

impl ProviderReviewDescriptor {
    fn from_exact(
        identity: ProviderReviewIdentity,
        exact: &AppActionDescriptor,
    ) -> Result<Self, DescribeError> {
        let operation = provider_review_operation(identity, exact)?;
        let mut grouped = BTreeMap::<
            (
                ProviderReviewScopeType,
                ProviderReviewNamespace,
                ProviderReviewResourceKind,
            ),
            u64,
        >::new();
        for scope in &exact.resource_scopes {
            let (scope_type, namespace, kind) = provider_review_scope(identity, scope)?;
            let count = grouped.entry((scope_type, namespace, kind)).or_default();
            *count = count
                .checked_add(1)
                .ok_or_else(|| DescribeError::InvalidDescriptor {
                    reason: "provider review resource scope count overflowed".to_owned(),
                })?;
        }

        Ok(Self {
            schema_version: PROVIDER_REVIEW_DESCRIPTOR_SCHEMA_VERSION,
            operation,
            capability: exact.capability.clone(),
            resource_scopes: grouped
                .into_iter()
                .map(
                    |((scope_type, namespace, kind), count)| ProviderReviewResourceScope {
                        scope_type,
                        namespace,
                        kind,
                        count,
                    },
                )
                .collect(),
        })
    }
}

fn provider_review_operation(
    identity: ProviderReviewIdentity,
    exact: &AppActionDescriptor,
) -> Result<ProviderReviewOperation, DescribeError> {
    use CapabilityClass::{Mutate, Read};
    use ProviderReviewIdentity as Identity;
    use ProviderReviewOperation as Operation;

    let operation = match (identity, exact.operation.as_str(), &exact.capability) {
        (Identity::WorkspaceListV1, "list_memberships", Read) => Operation::ListMemberships,
        (Identity::WorkspaceInvitationListV1, "list_invitations", Read) => {
            Operation::ListInvitations
        }
        (Identity::WorkspaceInvitationAcceptV1, "accept_invitation", Mutate) => {
            Operation::AcceptInvitation
        }
        (Identity::MessagingV1, "overview", Read) => Operation::Overview,
        (Identity::MessagingV1, "open", Read) => Operation::Open,
        (Identity::MessagingV1, "write", Mutate) => Operation::Write,
        (Identity::MessagingV1, "react", Mutate) => Operation::React,
        (Identity::MessagingV1, "status", Mutate) => Operation::Status,
        (Identity::MessagingV1, "reply_later", Mutate) => Operation::ReplyLater,
        (Identity::MessagingV1, "resolve_reply_later", Mutate) => Operation::ResolveReplyLater,
        (Identity::WorkspaceReadFileV1, "read_file", Read) => Operation::ReadFile,
        (Identity::WorkspaceListDirV1, "list_dir", Read) => Operation::ListDir,
        (Identity::WorkspaceGlobV1, "glob", Read) => Operation::Glob,
        (Identity::WorkspaceGrepV1, "grep", Read) => Operation::Grep,
        #[cfg(test)]
        (Identity::FixtureV1, "fixture.operation", _) => Operation::Fixture,
        #[cfg(test)]
        (Identity::ExampleV1 | Identity::ExampleV2 | Identity::OtherExampleV1, "update", _) => {
            Operation::Update
        }
        #[cfg(test)]
        (Identity::InspectFixtureV1, "inspect", _) => Operation::Inspect,
        #[cfg(test)]
        (Identity::AppActionFixtureV1, "update_record", _) => Operation::UpdateRecord,
        _ => {
            return Err(DescribeError::InvalidDescriptor {
                reason: "operation/capability has no closed provider review vocabulary".to_owned(),
            });
        }
    };
    Ok(operation)
}

fn provider_review_scope(
    identity: ProviderReviewIdentity,
    exact: &ResourceScope,
) -> Result<
    (
        ProviderReviewScopeType,
        ProviderReviewNamespace,
        ProviderReviewResourceKind,
    ),
    DescribeError,
> {
    use ProviderReviewIdentity as Identity;
    use ProviderReviewNamespace as Namespace;
    use ProviderReviewResourceKind as Kind;
    use ProviderReviewScopeType::{Collection, Resource};

    let (scope_type, namespace, kind) = match exact {
        ResourceScope::Collection { namespace, kind } => {
            (Collection, namespace.as_str(), kind.as_str())
        }
        ResourceScope::Resource {
            namespace, kind, ..
        } => (Resource, namespace.as_str(), kind.as_str()),
    };
    let safe = match (identity, scope_type, namespace, kind) {
        (Identity::WorkspaceListV1, Collection, "workspace", "membership") => {
            (Collection, Namespace::Workspace, Kind::Membership)
        }
        (Identity::WorkspaceInvitationListV1, Collection, "workspace", "invitation") => {
            (Collection, Namespace::Workspace, Kind::Invitation)
        }
        (Identity::WorkspaceInvitationAcceptV1, Resource, "workspace", "invitation") => {
            (Resource, Namespace::Workspace, Kind::Invitation)
        }
        (Identity::WorkspaceInvitationAcceptV1, Resource, "workspace", "membership") => {
            (Resource, Namespace::Workspace, Kind::Membership)
        }
        (Identity::MessagingV1, Resource, "workspace", "workspace") => {
            (Resource, Namespace::Workspace, Kind::Workspace)
        }
        (Identity::MessagingV1, Collection, "messaging", "place") => {
            (Collection, Namespace::Messaging, Kind::Place)
        }
        (Identity::MessagingV1, Resource, "messaging", "place") => {
            (Resource, Namespace::Messaging, Kind::Place)
        }
        (Identity::MessagingV1, Resource, "messaging", "message") => {
            (Resource, Namespace::Messaging, Kind::Message)
        }
        (Identity::MessagingV1, Resource, "messaging", "participant") => {
            (Resource, Namespace::Messaging, Kind::Participant)
        }
        (Identity::MessagingV1, Resource, "messaging", "reply_later_marker") => {
            (Resource, Namespace::Messaging, Kind::ReplyLaterMarker)
        }
        (
            Identity::WorkspaceReadFileV1
            | Identity::WorkspaceListDirV1
            | Identity::WorkspaceGrepV1,
            Resource,
            "sumi.foundation.workspace",
            "path",
        ) => (Resource, Namespace::FoundationWorkspace, Kind::Path),
        (Identity::WorkspaceGlobV1, Resource, "sumi.foundation.workspace", "glob_selector") => {
            (Resource, Namespace::FoundationWorkspace, Kind::GlobSelector)
        }
        #[cfg(test)]
        (Identity::FixtureV1, Resource, "fixture", "record") => {
            (Resource, Namespace::Fixture, Kind::Record)
        }
        #[cfg(test)]
        (
            Identity::ExampleV1 | Identity::ExampleV2 | Identity::OtherExampleV1,
            Resource,
            "example",
            "record",
        ) => (Resource, Namespace::Example, Kind::Record),
        #[cfg(test)]
        (Identity::ExampleV1 | Identity::ExampleV2, Collection, "example", "record") => {
            (Collection, Namespace::Example, Kind::Record)
        }
        #[cfg(test)]
        (Identity::InspectFixtureV1, Collection, "test", "item") => {
            (Collection, Namespace::Test, Kind::Item)
        }
        #[cfg(test)]
        (Identity::AppActionFixtureV1, Resource, "test", "record") => {
            (Resource, Namespace::Test, Kind::Record)
        }
        _ => {
            return Err(DescribeError::InvalidDescriptor {
                reason: "resource namespace/kind has no closed provider review vocabulary"
                    .to_owned(),
            });
        }
    };
    Ok(safe)
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

/// App-owned, deliberately bounded details suitable for authenticated Human
/// review and local durable binding. This value is explicit rather than
/// generically derived from execution arguments: an app must retain the
/// operation's meaning, target, and consent payload. External reviewers receive
/// only [`ProviderReviewProjection`], never this exact local value.
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

/// A provider-safe structural summary of the exact local Human projection.
///
/// The exact projection remains in [`BoundToolInvocation::review_projection`]
/// for authenticated Human consent and local durable evidence. This separate
/// value deliberately contains no keys or scalar strings from that projection,
/// and no digest derived from its hidden values. A separately reduced
/// [`ProviderReviewDescriptor`] carries only trusted adapter vocabulary and
/// resource-shape counts; exact resource identifiers also remain local.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProviderReviewProjection {
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

impl ProviderReviewProjection {
    fn from_exact(exact: &ReviewProjection) -> Result<Self, DescribeError> {
        let mut projection = Self {
            schema_version: PROVIDER_REVIEW_PROJECTION_SCHEMA_VERSION,
            top_level_fields: u64::try_from(exact.as_object().len()).map_err(|_| {
                DescribeError::InvalidReviewProjection {
                    reason: "review projection field count exceeds u64".to_owned(),
                }
            })?,
            object_fields: 0,
            array_items: 0,
            text_values: 0,
            text_bytes: 0,
            text_characters: 0,
            number_values: 0,
            boolean_values: 0,
            null_values: 0,
        };
        summarize_provider_review_value(
            &Value::Object(exact.as_object().clone()),
            &mut projection,
        )?;
        Ok(projection)
    }
}

fn summarize_provider_review_value(
    value: &Value,
    summary: &mut ProviderReviewProjection,
) -> Result<(), DescribeError> {
    fn add(target: &mut u64, value: usize) -> Result<(), DescribeError> {
        let value = u64::try_from(value).map_err(|_| DescribeError::InvalidReviewProjection {
            reason: "review projection size exceeds u64".to_owned(),
        })?;
        *target =
            target
                .checked_add(value)
                .ok_or_else(|| DescribeError::InvalidReviewProjection {
                    reason: "review projection summary overflowed".to_owned(),
                })?;
        Ok(())
    }

    match value {
        Value::Null => add(&mut summary.null_values, 1),
        Value::Bool(_) => add(&mut summary.boolean_values, 1),
        Value::Number(_) => add(&mut summary.number_values, 1),
        Value::String(text) => {
            add(&mut summary.text_values, 1)?;
            add(&mut summary.text_bytes, text.len())?;
            add(&mut summary.text_characters, text.chars().count())
        }
        Value::Array(values) => {
            add(&mut summary.array_items, values.len())?;
            for value in values {
                summarize_provider_review_value(value, summary)?;
            }
            Ok(())
        }
        Value::Object(object) => {
            add(&mut summary.object_fields, object.len())?;
            for value in object.values() {
                summarize_provider_review_value(value, summary)?;
            }
            Ok(())
        }
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
    pub provider_review_identity: ProviderReviewIdentity,
    pub provider_review_descriptor: ProviderReviewDescriptor,
    pub review_projection: ReviewProjection,
    pub provider_review_projection: ProviderReviewProjection,
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
        let provider_review_identity = ProviderReviewIdentity::from_local(tool_name, &adapter)?;
        let provider_review_descriptor =
            ProviderReviewDescriptor::from_exact(provider_review_identity, &binding.descriptor)?;
        let provider_review_projection =
            ProviderReviewProjection::from_exact(&binding.review_projection)?;

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
                "provider_review_identity": &provider_review_identity,
                "provider_review_descriptor": &provider_review_descriptor,
                "review_projection": &binding.review_projection,
                "provider_review_projection": &provider_review_projection,
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
            provider_review_identity,
            provider_review_descriptor,
            review_projection: binding.review_projection,
            provider_review_projection,
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
                "provider_review_identity": &self.provider_review_identity,
                "provider_review_descriptor": &self.provider_review_descriptor,
                "review_projection": &self.review_projection,
                "provider_review_projection": &self.provider_review_projection,
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

    fn seal_with_vocabulary(
        tool_name: &str,
        adapter_id: &str,
        operation: &str,
        namespace: &str,
        kind: &str,
    ) -> Result<BoundToolInvocation, DescribeError> {
        let proposal = json!({"value": "fixture"});
        BoundToolInvocation::seal(
            "vocabulary-call",
            tool_name,
            proposal.as_object().unwrap(),
            AdapterIdentity::new(adapter_id, 1)?,
            execution_identity("vocabulary-flow", "/workspace"),
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
    fn provider_review_vocabulary_rejects_arbitrary_tool_adapter_operation_and_scope_labels() {
        const SENTINEL: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopq";
        assert_eq!(SENTINEL.chars().count(), 43);
        assert!(
            AppActionDescriptor::new(
                SENTINEL,
                CapabilityClass::Mutate,
                vec![ResourceScope::resource(SENTINEL, SENTINEL, "local-id")],
            )
            .is_ok(),
            "the exact local descriptor remains flexible"
        );

        for (label, result) in [
            (
                "tool name",
                seal_with_vocabulary(
                    SENTINEL,
                    "sumi.fixture",
                    "fixture.operation",
                    "fixture",
                    "record",
                ),
            ),
            (
                "adapter id",
                seal_with_vocabulary(
                    "fixture_tool",
                    SENTINEL,
                    "fixture.operation",
                    "fixture",
                    "record",
                ),
            ),
            (
                "operation",
                seal_with_vocabulary(
                    "fixture_tool",
                    "sumi.fixture",
                    SENTINEL,
                    "fixture",
                    "record",
                ),
            ),
            (
                "namespace",
                seal_with_vocabulary(
                    "fixture_tool",
                    "sumi.fixture",
                    "fixture.operation",
                    SENTINEL,
                    "record",
                ),
            ),
            (
                "resource kind",
                seal_with_vocabulary(
                    "fixture_tool",
                    "sumi.fixture",
                    "fixture.operation",
                    "fixture",
                    SENTINEL,
                ),
            ),
        ] {
            assert!(
                matches!(result, Err(DescribeError::InvalidDescriptor { .. })),
                "arbitrary {label} crossed the provider vocabulary boundary"
            );
        }
    }

    #[test]
    fn provider_projection_summarizes_exact_text_without_copying_keys_values_or_hashes() {
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
        let provider_descriptor =
            serde_json::to_string(&invocation.provider_review_descriptor).unwrap();
        assert_eq!(provider_descriptor.matches(SENTINEL).count(), 0);
        assert!(!provider_descriptor.contains("record-b"));
        assert_eq!(
            invocation.provider_review_descriptor.resource_scopes.len(),
            2
        );
        assert_eq!(
            invocation
                .provider_review_descriptor
                .resource_scopes
                .iter()
                .find(|scope| scope.scope_type == ProviderReviewScopeType::Resource)
                .expect("resource scope summary")
                .count,
            2
        );
        let provider = serde_json::to_string(&invocation.provider_review_projection).unwrap();
        assert_eq!(provider.matches(SENTINEL).count(), 0);
        assert!(!provider.contains("content"));
        assert!(!provider.contains("action"));
        assert!(!provider.contains(&invocation.proposal_digest.to_hex()));
        assert!(!provider.contains(&invocation.descriptor_digest.to_hex()));
        assert_eq!(invocation.provider_review_projection.text_values, 2);
        assert_eq!(invocation.provider_review_projection.boolean_values, 1);

        let mut tampered = invocation.clone();
        tampered.provider_review_projection.text_characters += 1;
        assert_ne!(
            tampered.recompute_descriptor_digest().unwrap(),
            invocation.descriptor_digest,
            "the local descriptor/evidence identity must bind the safe external summary"
        );

        let mut tampered = invocation.clone();
        tampered.provider_review_descriptor.resource_scopes[0].count += 1;
        assert_ne!(
            tampered.recompute_descriptor_digest().unwrap(),
            invocation.descriptor_digest,
            "the local descriptor/evidence identity must bind the safe external descriptor"
        );
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
            "8fd9e0ce15dab3a10c9d0d1bf0211fafe94bef24ec565bb42c696462301c71f4"
        );
        assert_eq!(
            left.descriptor_digest.to_hex(),
            "1260d135657fd045cecca256facb272f57557ef50c98db11d4adf24f17fab9f5"
        );
        assert_eq!(left.proposal_digest.as_bytes()[0], 0x8f);

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
