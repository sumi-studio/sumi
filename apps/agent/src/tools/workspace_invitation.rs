//! Exact targeted Workspace invitation tools for one PersonalityAgent.
//!
//! The authenticated local-control client fixes the actor. The list cursor and
//! invitation identity are the only model-authored inputs: Workspace scope,
//! PAID, defaults, app installation, and wake behavior are never inferred or
//! accepted.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::SecondsFormat;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::apiclient::workspace::{
    WorkspaceApiError, WorkspaceInvitationApi, WorkspaceInvitationListPage,
    WorkspaceMembershipTenure,
};
use crate::provider::types::{ToolDefinition, UserContent};

use super::{
    AdapterIdentity, AppActionDescriptor, BoundExecutionArguments, BoundToolAdapter, BoundToolCtx,
    BoundToolExecutionOutcome, CapabilityClass, DescribeError, ResourceScope, ReviewProjection,
    Tool, ToolBindCtx, ToolBinding, ToolCtx, ToolError, ToolOutput, ToolRisk,
};

const LIST_TOOL_NAME: &str = "workspace_invitation_list";
const LIST_ADAPTER_ID: &str = "sumi.workspace.invitation.list";
const ACCEPT_TOOL_NAME: &str = "workspace_invitation_accept";
const ACCEPT_ADAPTER_ID: &str = "sumi.workspace.invitation.accept";
const ADAPTER_VERSION: u32 = 1;
const CURSOR_BYTES: usize = 76;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ListArguments {
    #[serde(default, deserialize_with = "deserialize_optional_non_null_string")]
    cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct AcceptArguments {
    invitation_id: String,
}

pub(crate) struct WorkspaceInvitationListTool {
    api: Arc<dyn WorkspaceInvitationApi>,
}

impl WorkspaceInvitationListTool {
    pub(crate) fn new(api: Arc<dyn WorkspaceInvitationApi>) -> Self {
        Self { api }
    }

    async fn execute_list(
        &self,
        arguments: &ListArguments,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<ToolOutput, ToolError> {
        let page = tokio::select! {
            _ = cancel.cancelled() => return Err(ToolError::Cancelled),
            result = self.api.list_invitations(arguments.cursor.as_deref()) => result,
        }
        .map_err(map_list_error)?;
        render_invitation_list(page)
    }
}

#[async_trait]
impl Tool for WorkspaceInvitationListTool {
    fn def(&self) -> ToolDefinition {
        ToolDefinition {
            name: LIST_TOOL_NAME.to_owned(),
            description: concat!(
                "List targeted Sumi Workspace invitations addressed to you that are still ",
                "acceptable. Returns one bounded page. If next_cursor is present, call ",
                "again with that exact opaque cursor. This does not choose a default ",
                "Workspace, accept anything, install an app, or wake another participant."
            )
            .to_owned(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "cursor": {
                        "type": "string",
                        "description": "Opaque next_cursor returned by an earlier workspace_invitation_list page.",
                        "minLength": CURSOR_BYTES,
                        "maxLength": CURSOR_BYTES
                    }
                },
                "additionalProperties": false
            }),
        }
    }

    fn risk(&self) -> ToolRisk {
        ToolRisk::ReadOnly
    }

    fn bound_adapter(self: Arc<Self>) -> Option<Arc<dyn BoundToolAdapter>> {
        Some(self)
    }

    async fn execute(&self, ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
        let arguments =
            decode_list(ctx.args.as_object()).map_err(|_| ToolError::InvalidArguments)?;
        validate_list_arguments(&arguments).map_err(|_| ToolError::InvalidArguments)?;
        self.execute_list(&arguments, &ctx.cancel).await
    }
}

#[async_trait]
impl BoundToolAdapter for WorkspaceInvitationListTool {
    fn identity(&self) -> AdapterIdentity {
        AdapterIdentity::new(LIST_ADAPTER_ID, ADAPTER_VERSION)
            .expect("static Workspace invitation list adapter identity must be valid")
    }

    async fn bind(&self, ctx: ToolBindCtx<'_>) -> Result<ToolBinding, DescribeError> {
        let arguments =
            decode_list(ctx.args.as_object()).map_err(|_| DescribeError::InvalidArguments)?;
        validate_list_arguments(&arguments).map_err(|_| DescribeError::InvalidArguments)?;
        let execution_arguments = list_execution_arguments(&arguments);
        let mut review = json!({
            "operation": "list_invitations",
            "actor": "self"
        });
        if let Some(cursor) = &arguments.cursor {
            review
                .as_object_mut()
                .expect("static review projection is an object")
                .insert("cursor".to_owned(), Value::String(cursor.clone()));
        }
        Ok(ToolBinding::new(
            AppActionDescriptor::new(
                "list_invitations",
                CapabilityClass::Read,
                vec![ResourceScope::collection("workspace", "invitation")],
            )?,
            ReviewProjection::from_value(review)?,
            BoundExecutionArguments::from_value(execution_arguments)?,
        ))
    }

    async fn execute(&self, ctx: BoundToolCtx<'_>) -> Result<BoundToolExecutionOutcome, ToolError> {
        let arguments =
            decode_list(ctx.args.as_object()).map_err(|_| ToolError::InvalidArguments)?;
        validate_list_arguments(&arguments).map_err(|_| ToolError::InvalidArguments)?;
        if ctx.cancel.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        let effect_receipt = ctx
            .committed_effect_permit
            .begin_local_effect()
            .complete(|| self.execute_list(&arguments, &ctx.cancel))
            .await?;
        Ok(BoundToolExecutionOutcome::without_live_post_commit(
            effect_receipt,
        ))
    }
}

pub(crate) struct WorkspaceInvitationAcceptTool {
    api: Arc<dyn WorkspaceInvitationApi>,
}

impl WorkspaceInvitationAcceptTool {
    pub(crate) fn new(api: Arc<dyn WorkspaceInvitationApi>) -> Self {
        Self { api }
    }

    async fn execute_accept(
        &self,
        arguments: &AcceptArguments,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<ToolOutput, ToolError> {
        let membership = tokio::select! {
            _ = cancel.cancelled() => return Err(ToolError::Cancelled),
            result = self.api.accept_invitation(&arguments.invitation_id) => result,
        }
        .map_err(map_accept_error)?;
        render_membership(membership)
    }
}

#[async_trait]
impl Tool for WorkspaceInvitationAcceptTool {
    fn def(&self) -> ToolDefinition {
        ToolDefinition {
            name: ACCEPT_TOOL_NAME.to_owned(),
            description: concat!(
                "Accept one exact targeted Sumi Workspace invitation addressed to you. ",
                "The invitation determines the Workspace; no current/default Workspace, ",
                "app installation, participant identity, or wake behavior is accepted. ",
                "An exact retry returns the original membership tenure and never rejoins ",
                "after that tenure has closed."
            )
            .to_owned(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "invitation_id": {
                        "type": "string",
                        "format": "uuid",
                        "description": "Exact invitation_id returned by workspace_invitation_list."
                    }
                },
                "required": ["invitation_id"],
                "additionalProperties": false
            }),
        }
    }

    fn risk(&self) -> ToolRisk {
        ToolRisk::Mutating
    }

    fn bound_adapter(self: Arc<Self>) -> Option<Arc<dyn BoundToolAdapter>> {
        Some(self)
    }

    async fn execute(&self, ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
        let arguments =
            decode_accept(ctx.args.as_object()).map_err(|_| ToolError::InvalidArguments)?;
        validate_accept_arguments(&arguments).map_err(|_| ToolError::InvalidArguments)?;
        self.execute_accept(&arguments, &ctx.cancel).await
    }
}

#[async_trait]
impl BoundToolAdapter for WorkspaceInvitationAcceptTool {
    fn identity(&self) -> AdapterIdentity {
        AdapterIdentity::new(ACCEPT_ADAPTER_ID, ADAPTER_VERSION)
            .expect("static Workspace invitation accept adapter identity must be valid")
    }

    async fn bind(&self, ctx: ToolBindCtx<'_>) -> Result<ToolBinding, DescribeError> {
        let arguments =
            decode_accept(ctx.args.as_object()).map_err(|_| DescribeError::InvalidArguments)?;
        validate_accept_arguments(&arguments).map_err(|_| DescribeError::InvalidArguments)?;
        let exact = json!({"invitation_id": arguments.invitation_id});
        Ok(ToolBinding::new(
            AppActionDescriptor::new(
                "accept_invitation",
                CapabilityClass::Mutate,
                vec![
                    ResourceScope::resource("workspace", "invitation", &arguments.invitation_id),
                    ResourceScope::resource("workspace", "membership", "self"),
                ],
            )?,
            ReviewProjection::from_value(json!({
                "operation": "accept_invitation",
                "actor": "self",
                "invitation_id": arguments.invitation_id
            }))?,
            BoundExecutionArguments::from_value(exact)?,
        ))
    }

    async fn execute(&self, ctx: BoundToolCtx<'_>) -> Result<BoundToolExecutionOutcome, ToolError> {
        let arguments =
            decode_accept(ctx.args.as_object()).map_err(|_| ToolError::InvalidArguments)?;
        validate_accept_arguments(&arguments).map_err(|_| ToolError::InvalidArguments)?;
        if ctx.cancel.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        let effect_receipt = ctx
            .committed_effect_permit
            .begin_local_effect()
            .complete(|| self.execute_accept(&arguments, &ctx.cancel))
            .await?;
        Ok(BoundToolExecutionOutcome::without_live_post_commit(
            effect_receipt,
        ))
    }
}

fn decode_list(arguments: &Map<String, Value>) -> Result<ListArguments, serde_json::Error> {
    serde_json::from_value(Value::Object(arguments.clone()))
}

fn decode_accept(arguments: &Map<String, Value>) -> Result<AcceptArguments, serde_json::Error> {
    serde_json::from_value(Value::Object(arguments.clone()))
}

// `Option<String>` normally gives explicit JSON null the same meaning as an
// omitted property. The invitation cursor contract is presence-sensitive:
// omission means the first page, while every present value must be a string.
fn deserialize_optional_non_null_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    String::deserialize(deserializer).map(Some)
}

fn validate_list_arguments(arguments: &ListArguments) -> Result<(), ()> {
    if arguments.cursor.as_deref().is_some_and(|cursor| {
        cursor.len() != CURSOR_BYTES
            || !cursor
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    }) {
        return Err(());
    }
    Ok(())
}

fn validate_accept_arguments(arguments: &AcceptArguments) -> Result<(), ()> {
    if !is_canonical_uuid_v7(&arguments.invitation_id) {
        return Err(());
    }
    Ok(())
}

fn is_canonical_uuid_v7(value: &str) -> bool {
    let Ok(uuid) = uuid::Uuid::parse_str(value) else {
        return false;
    };
    uuid.get_version() == Some(uuid::Version::SortRand)
        && uuid.get_variant() == uuid::Variant::RFC4122
        && uuid.hyphenated().to_string() == value
}

fn list_execution_arguments(arguments: &ListArguments) -> Value {
    let mut value = Map::new();
    if let Some(cursor) = &arguments.cursor {
        value.insert("cursor".to_owned(), Value::String(cursor.clone()));
    }
    Value::Object(value)
}

fn map_list_error(error: WorkspaceApiError) -> ToolError {
    match error {
        WorkspaceApiError::InvalidRequest => ToolError::InvalidArguments,
        WorkspaceApiError::Protocol => ToolError::Protocol(
            "Workspace invitation list response violated its typed contract".to_owned(),
        ),
        WorkspaceApiError::NotFound | WorkspaceApiError::Conflict => ToolError::Rpc(
            "Workspace invitation list no longer matches current admission state".to_owned(),
        ),
        WorkspaceApiError::Unauthenticated | WorkspaceApiError::Forbidden => {
            ToolError::Rpc("Workspace invitation list authorization was rejected".to_owned())
        }
        WorkspaceApiError::ServiceUnavailable | WorkspaceApiError::Transport => {
            ToolError::Rpc("Workspace invitation list is temporarily unavailable".to_owned())
        }
    }
}

fn map_accept_error(error: WorkspaceApiError) -> ToolError {
    match error {
        WorkspaceApiError::InvalidRequest => ToolError::InvalidArguments,
        WorkspaceApiError::NotFound => {
            ToolError::Rpc("Targeted Workspace invitation is unavailable".to_owned())
        }
        WorkspaceApiError::Conflict => ToolError::Rpc(
            "Targeted Workspace invitation conflicts with an active membership".to_owned(),
        ),
        WorkspaceApiError::Protocol => ToolError::Protocol(
            "Workspace invitation acceptance response violated its typed contract".to_owned(),
        ),
        WorkspaceApiError::Unauthenticated | WorkspaceApiError::Forbidden => {
            ToolError::Rpc("Workspace invitation acceptance authorization was rejected".to_owned())
        }
        WorkspaceApiError::ServiceUnavailable | WorkspaceApiError::Transport => {
            ToolError::Rpc("Workspace invitation acceptance is temporarily unavailable".to_owned())
        }
    }
}

fn render_invitation_list(page: WorkspaceInvitationListPage) -> Result<ToolOutput, ToolError> {
    let mut value = json!({
        "invitations": page.invitations
            .into_iter()
            .map(|invitation| json!({
                "invitation_id": invitation.invitation_id,
                "workspace_id": invitation.workspace_id,
                "workspace_name": invitation.workspace_name,
                "expires_at": invitation.expires_at.to_rfc3339_opts(SecondsFormat::AutoSi, true),
                "created_at": invitation.created_at.to_rfc3339_opts(SecondsFormat::AutoSi, true)
            }))
            .collect::<Vec<_>>()
    });
    if let Some(cursor) = page.next_cursor {
        value
            .as_object_mut()
            .expect("static Workspace invitation page is an object")
            .insert("next_cursor".to_owned(), Value::String(cursor));
    }
    render_json_output(value)
}

fn render_membership(membership: WorkspaceMembershipTenure) -> Result<ToolOutput, ToolError> {
    let value = json!({
        "workspace_member_id": membership.workspace_member_id,
        "workspace_id": membership.workspace_id,
        "display_name": membership.display_name,
        "owner": membership.owner,
        "role_ids": membership.role_ids,
        "joined_at": membership.joined_at.to_rfc3339_opts(SecondsFormat::AutoSi, true),
        "left_at": membership.left_at.map(|timestamp| {
            timestamp.to_rfc3339_opts(SecondsFormat::AutoSi, true)
        })
    });
    render_json_output(value)
}

fn render_json_output(value: Value) -> Result<ToolOutput, ToolError> {
    let text = serde_json::to_string_pretty(&value)
        .map_err(|error| ToolError::Protocol(error.to_string()))?;
    Ok(ToolOutput {
        content: vec![UserContent::Text { text }],
        details: value,
        is_error: false,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use chrono::{TimeZone, Utc};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::apiclient::workspace::{
        WorkspaceApiResult, WorkspaceInvitationSummary, WorkspaceMembershipTenure,
    };
    use crate::provider::types::ValidatedToolArguments;
    use crate::tools::WorkspacePaths;

    const INVITATION_ID: &str = "0198f0f4-9b72-7000-8000-000000000811";
    const WORKSPACE_ID: &str = "0198f0f4-9b72-7000-8000-000000000011";
    const MEMBERSHIP_ID: &str = "0198f0f4-9b72-7000-8000-000000000911";
    const CURSOR: &str =
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    struct FakeInvitationApi {
        list_calls: AtomicUsize,
        accept_calls: AtomicUsize,
        cursors: Mutex<Vec<Option<String>>>,
        invitation_ids: Mutex<Vec<String>>,
        list_result: WorkspaceApiResult<WorkspaceInvitationListPage>,
        accept_result: WorkspaceApiResult<WorkspaceMembershipTenure>,
    }

    #[async_trait]
    impl WorkspaceInvitationApi for FakeInvitationApi {
        async fn list_invitations(
            &self,
            cursor: Option<&str>,
        ) -> WorkspaceApiResult<WorkspaceInvitationListPage> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            self.cursors.lock().unwrap().push(cursor.map(str::to_owned));
            self.list_result.clone()
        }

        async fn accept_invitation(
            &self,
            invitation_id: &str,
        ) -> WorkspaceApiResult<WorkspaceMembershipTenure> {
            self.accept_calls.fetch_add(1, Ordering::SeqCst);
            self.invitation_ids
                .lock()
                .unwrap()
                .push(invitation_id.to_owned());
            self.accept_result.clone()
        }
    }

    fn timestamp(hour: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 16, hour, 0, 0).unwrap()
    }

    fn invitation_page() -> WorkspaceInvitationListPage {
        WorkspaceInvitationListPage {
            invitations: vec![WorkspaceInvitationSummary {
                invitation_id: INVITATION_ID.to_owned(),
                workspace_id: WORKSPACE_ID.to_owned(),
                workspace_name: "Sumi developers".to_owned(),
                created_at: timestamp(1),
                expires_at: timestamp(2),
            }],
            next_cursor: None,
        }
    }

    fn membership() -> WorkspaceMembershipTenure {
        WorkspaceMembershipTenure {
            workspace_member_id: MEMBERSHIP_ID.to_owned(),
            workspace_id: WORKSPACE_ID.to_owned(),
            display_name: "Kuro".to_owned(),
            owner: false,
            role_ids: Vec::new(),
            joined_at: timestamp(1),
            left_at: None,
        }
    }

    fn fake_api() -> Arc<FakeInvitationApi> {
        Arc::new(FakeInvitationApi {
            list_calls: AtomicUsize::new(0),
            accept_calls: AtomicUsize::new(0),
            cursors: Mutex::new(Vec::new()),
            invitation_ids: Mutex::new(Vec::new()),
            list_result: Ok(invitation_page()),
            accept_result: Ok(membership()),
        })
    }

    fn arguments(value: Value) -> ValidatedToolArguments {
        serde_json::from_value(value).expect("object-shaped tool arguments")
    }

    fn workspace_paths() -> WorkspacePaths {
        WorkspacePaths::new("/workspace").expect("absolute fixture workspace")
    }

    fn committed_effect_permit(
        label: &str,
    ) -> crate::approval::authority::CommittedExecutionPermit {
        crate::approval::authority::CommittedExecutionPermit::executor_fixture(
            label,
            crate::provider::types::ToolInvocationRoute::Normal,
            crate::approval::authority::ExecutionAuthorityProvenance::AgentOwn,
        )
    }

    #[tokio::test]
    async fn list_binding_is_collection_read_and_seals_only_the_optional_cursor() {
        let api = fake_api();
        let tool = WorkspaceInvitationListTool::new(api.clone());
        let args = arguments(json!({"cursor": CURSOR}));
        let workspace = workspace_paths();

        let definition = tool.def();
        assert_eq!(definition.name, LIST_TOOL_NAME);
        assert_eq!(tool.risk(), ToolRisk::ReadOnly);
        assert_eq!(
            definition.parameters["properties"]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            vec!["cursor"]
        );
        let binding = BoundToolAdapter::bind(
            &tool,
            ToolBindCtx {
                args: &args,
                workspace: &workspace,
            },
        )
        .await
        .expect("bind invitation list");

        assert_eq!(
            api.list_calls.load(Ordering::SeqCst),
            0,
            "binding performed I/O"
        );
        assert_eq!(binding.descriptor.operation, "list_invitations");
        assert_eq!(binding.descriptor.capability, CapabilityClass::Read);
        assert_eq!(
            binding.descriptor.resource_scopes,
            vec![ResourceScope::collection("workspace", "invitation")]
        );
        assert_eq!(binding.review_projection.as_object()["actor"], "self");
        assert_eq!(binding.review_projection.as_object()["cursor"], CURSOR);
        assert_eq!(binding.execution_arguments.as_object()["cursor"], CURSOR);

        let outcome = BoundToolAdapter::execute(
            &tool,
            BoundToolCtx {
                flow_id: "flow-list",
                call_id: "call-list",
                args: &binding.execution_arguments,
                committed_effect_permit: committed_effect_permit("invitation-list"),
                cancel: CancellationToken::new(),
                on_update: Arc::new(|_| {}),
                workspace: &workspace,
            },
        )
        .await
        .expect("execute invitation list");
        assert_eq!(*api.cursors.lock().unwrap(), vec![Some(CURSOR.to_owned())]);
        assert_eq!(
            outcome.output.details["invitations"][0]["invitation_id"],
            INVITATION_ID
        );
        assert_eq!(
            outcome.output.details["invitations"][0]["workspace_id"],
            WORKSPACE_ID
        );
        assert!(!outcome.output.is_error);
    }

    #[tokio::test]
    async fn omitted_list_cursor_binds_and_executes_as_the_first_page() {
        let api = fake_api();
        let tool = WorkspaceInvitationListTool::new(api.clone());
        let args = arguments(json!({}));
        let workspace = workspace_paths();

        let binding = BoundToolAdapter::bind(
            &tool,
            ToolBindCtx {
                args: &args,
                workspace: &workspace,
            },
        )
        .await
        .expect("bind omitted invitation cursor");
        assert_eq!(api.list_calls.load(Ordering::SeqCst), 0);
        assert!(binding.execution_arguments.as_object().is_empty());
        assert!(
            binding
                .review_projection
                .as_object()
                .get("cursor")
                .is_none()
        );

        BoundToolAdapter::execute(
            &tool,
            BoundToolCtx {
                flow_id: "flow-list-first-page",
                call_id: "call-list-first-page",
                args: &binding.execution_arguments,
                committed_effect_permit: committed_effect_permit("invitation-list-first-page"),
                cancel: CancellationToken::new(),
                on_update: Arc::new(|_| {}),
                workspace: &workspace,
            },
        )
        .await
        .expect("execute omitted invitation cursor");
        assert_eq!(*api.cursors.lock().unwrap(), vec![None]);
    }

    #[tokio::test]
    async fn provider_review_vocabulary_is_closed_and_hides_exact_invitation_inputs() {
        let api = fake_api();
        let mut builder = crate::tools::ToolRegistryBuilder::default();
        builder
            .register(Arc::new(WorkspaceInvitationListTool::new(api.clone())))
            .expect("register invitation list");
        builder
            .register(Arc::new(WorkspaceInvitationAcceptTool::new(api)))
            .expect("register invitation acceptance");
        let registry = builder.build();
        let workspace = workspace_paths();

        let list = registry
            .bind(
                &crate::provider::types::ToolCall {
                    id: "provider-review-invitation-list".to_owned(),
                    name: LIST_TOOL_NAME.to_owned(),
                    route: crate::provider::types::ToolInvocationRoute::Normal,
                    arguments: arguments(json!({"cursor": CURSOR})),
                },
                "provider-review-invitation-list-flow",
                &workspace,
            )
            .await
            .expect("seal invitation list provider vocabulary");
        let list = registry
            .validate_bound(&list)
            .expect("validate invitation list binding");
        assert_eq!(
            serde_json::to_value(&list.provider_review_identity).unwrap(),
            json!("workspace_invitation_list_v1")
        );
        assert_eq!(
            serde_json::to_value(&list.provider_review_descriptor.operation).unwrap(),
            json!("list_invitations")
        );
        assert_eq!(
            serde_json::to_value(&list.provider_review_descriptor.resource_scopes).unwrap(),
            json!([{
                "scope_type": "collection",
                "namespace": "workspace",
                "kind": "invitation",
                "count": 1
            }])
        );
        let list_provider_wire = serde_json::to_string(&json!({
            "identity": &list.provider_review_identity,
            "descriptor": &list.provider_review_descriptor,
            "projection": &list.provider_review_projection
        }))
        .unwrap();
        assert!(!list_provider_wire.contains(CURSOR));

        let accept = registry
            .bind(
                &crate::provider::types::ToolCall {
                    id: "provider-review-invitation-accept".to_owned(),
                    name: ACCEPT_TOOL_NAME.to_owned(),
                    route: crate::provider::types::ToolInvocationRoute::Elevated,
                    arguments: arguments(json!({"invitation_id": INVITATION_ID})),
                },
                "provider-review-invitation-accept-flow",
                &workspace,
            )
            .await
            .expect("seal invitation acceptance provider vocabulary");
        let accept = registry
            .validate_bound(&accept)
            .expect("validate invitation acceptance binding");
        assert_eq!(
            serde_json::to_value(&accept.provider_review_identity).unwrap(),
            json!("workspace_invitation_accept_v1")
        );
        assert_eq!(
            serde_json::to_value(&accept.provider_review_descriptor.operation).unwrap(),
            json!("accept_invitation")
        );
        let accept_scopes =
            serde_json::to_value(&accept.provider_review_descriptor.resource_scopes).unwrap();
        assert_eq!(accept_scopes.as_array().unwrap().len(), 2);
        assert!(accept_scopes.as_array().unwrap().contains(&json!({
            "scope_type": "resource",
            "namespace": "workspace",
            "kind": "invitation",
            "count": 1
        })));
        assert!(accept_scopes.as_array().unwrap().contains(&json!({
            "scope_type": "resource",
            "namespace": "workspace",
            "kind": "membership",
            "count": 1
        })));
        let accept_provider_wire = serde_json::to_string(&json!({
            "identity": &accept.provider_review_identity,
            "descriptor": &accept.provider_review_descriptor,
            "projection": &accept.provider_review_projection
        }))
        .unwrap();
        assert!(!accept_provider_wire.contains(INVITATION_ID));
    }

    #[tokio::test]
    async fn accept_binding_is_exact_mutation_and_performs_no_bind_time_io() {
        let api = fake_api();
        let tool = WorkspaceInvitationAcceptTool::new(api.clone());
        let args = arguments(json!({"invitation_id": INVITATION_ID}));
        let workspace = workspace_paths();

        let definition = tool.def();
        assert_eq!(definition.name, ACCEPT_TOOL_NAME);
        assert_eq!(tool.risk(), ToolRisk::Mutating);
        assert_eq!(
            definition.parameters["properties"]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            vec!["invitation_id"]
        );
        let binding = BoundToolAdapter::bind(
            &tool,
            ToolBindCtx {
                args: &args,
                workspace: &workspace,
            },
        )
        .await
        .expect("bind invitation acceptance");

        assert_eq!(
            api.accept_calls.load(Ordering::SeqCst),
            0,
            "binding performed I/O"
        );
        assert_eq!(binding.descriptor.operation, "accept_invitation");
        assert_eq!(binding.descriptor.capability, CapabilityClass::Mutate);
        assert_eq!(
            binding.descriptor.resource_scopes,
            vec![
                ResourceScope::resource("workspace", "invitation", INVITATION_ID),
                ResourceScope::resource("workspace", "membership", "self"),
            ]
        );
        assert_eq!(binding.review_projection.as_object()["actor"], "self");
        assert_eq!(
            binding.review_projection.as_object()["invitation_id"],
            INVITATION_ID
        );
        assert_eq!(
            binding.execution_arguments.as_object()["invitation_id"],
            INVITATION_ID
        );

        let outcome = BoundToolAdapter::execute(
            &tool,
            BoundToolCtx {
                flow_id: "flow-accept",
                call_id: "call-accept",
                args: &binding.execution_arguments,
                committed_effect_permit: committed_effect_permit("invitation-accept"),
                cancel: CancellationToken::new(),
                on_update: Arc::new(|_| {}),
                workspace: &workspace,
            },
        )
        .await
        .expect("execute invitation acceptance");
        assert_eq!(
            *api.invitation_ids.lock().unwrap(),
            vec![INVITATION_ID.to_owned()]
        );
        assert_eq!(outcome.output.details["workspace_member_id"], MEMBERSHIP_ID);
        assert_eq!(outcome.output.details["workspace_id"], WORKSPACE_ID);
        assert_eq!(outcome.output.details["left_at"], Value::Null);
    }

    #[tokio::test]
    async fn cancelled_bound_invitation_tools_never_start_local_control_effects() {
        let api = fake_api();
        let list = WorkspaceInvitationListTool::new(api.clone());
        let accept = WorkspaceInvitationAcceptTool::new(api.clone());
        let workspace = workspace_paths();

        let list_arguments = arguments(json!({}));
        let list_binding = BoundToolAdapter::bind(
            &list,
            ToolBindCtx {
                args: &list_arguments,
                workspace: &workspace,
            },
        )
        .await
        .expect("bind cancelled invitation list");
        let list_cancel = CancellationToken::new();
        list_cancel.cancel();
        let list_result = BoundToolAdapter::execute(
            &list,
            BoundToolCtx {
                flow_id: "cancelled-invitation-list-flow",
                call_id: "cancelled-invitation-list-call",
                args: &list_binding.execution_arguments,
                committed_effect_permit: committed_effect_permit("cancelled-invitation-list"),
                cancel: list_cancel,
                on_update: Arc::new(|_| {}),
                workspace: &workspace,
            },
        )
        .await;
        assert!(matches!(list_result, Err(ToolError::Cancelled)));

        let accept_arguments = arguments(json!({"invitation_id": INVITATION_ID}));
        let accept_binding = BoundToolAdapter::bind(
            &accept,
            ToolBindCtx {
                args: &accept_arguments,
                workspace: &workspace,
            },
        )
        .await
        .expect("bind cancelled invitation acceptance");
        let accept_cancel = CancellationToken::new();
        accept_cancel.cancel();
        let accept_result = BoundToolAdapter::execute(
            &accept,
            BoundToolCtx {
                flow_id: "cancelled-invitation-accept-flow",
                call_id: "cancelled-invitation-accept-call",
                args: &accept_binding.execution_arguments,
                committed_effect_permit: committed_effect_permit("cancelled-invitation-accept"),
                cancel: accept_cancel,
                on_update: Arc::new(|_| {}),
                workspace: &workspace,
            },
        )
        .await;
        assert!(matches!(accept_result, Err(ToolError::Cancelled)));

        assert_eq!(api.list_calls.load(Ordering::SeqCst), 0);
        assert_eq!(api.accept_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn tools_reject_every_caller_authored_identity_scope_and_effect_field() {
        let api = fake_api();
        let list = WorkspaceInvitationListTool::new(api.clone());
        let accept = WorkspaceInvitationAcceptTool::new(api.clone());
        let workspace = workspace_paths();

        for extra in [
            json!({"personality_agent_id": "0198f0f4-9b72-7000-8000-000000000099"}),
            json!({"workspace_id": WORKSPACE_ID}),
            json!({"default": true}),
            json!({"installation_id": "0198f0f4-9b72-7000-8000-000000000099"}),
            json!({"wake": true}),
        ] {
            let error = Tool::execute(
                &list,
                ToolCtx {
                    flow_id: "flow-reject-list",
                    call_id: "call-reject-list",
                    args: &arguments(extra),
                    cancel: CancellationToken::new(),
                    on_update: Arc::new(|_| {}),
                    workspace: &workspace,
                },
            )
            .await
            .expect_err("list identity/scope input must be rejected");
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        for (field, extra_value) in [
            (
                "personality_agent_id",
                json!("0198f0f4-9b72-7000-8000-000000000099"),
            ),
            ("workspace_id", json!(WORKSPACE_ID)),
            ("default", json!(true)),
            (
                "installation_id",
                json!("0198f0f4-9b72-7000-8000-000000000099"),
            ),
            ("wake", json!(true)),
        ] {
            let mut value = json!({"invitation_id": INVITATION_ID});
            value
                .as_object_mut()
                .unwrap()
                .insert(field.to_owned(), extra_value);
            let error = Tool::execute(
                &accept,
                ToolCtx {
                    flow_id: "flow-reject-accept",
                    call_id: "call-reject-accept",
                    args: &arguments(value),
                    cancel: CancellationToken::new(),
                    on_update: Arc::new(|_| {}),
                    workspace: &workspace,
                },
            )
            .await
            .expect_err("accept identity/scope input must be rejected");
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        assert_eq!(api.list_calls.load(Ordering::SeqCst), 0);
        assert_eq!(api.accept_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn invalid_cursor_and_noncanonical_invitation_fail_before_api_io() {
        let api = fake_api();
        let list = WorkspaceInvitationListTool::new(api.clone());
        let accept = WorkspaceInvitationAcceptTool::new(api.clone());
        let workspace = workspace_paths();

        for value in ["short".to_owned(), format!("{}!", &CURSOR[..75])] {
            let error = Tool::execute(
                &list,
                ToolCtx {
                    flow_id: "flow-invalid-list",
                    call_id: "call-invalid-list",
                    args: &arguments(json!({"cursor": value})),
                    cancel: CancellationToken::new(),
                    on_update: Arc::new(|_| {}),
                    workspace: &workspace,
                },
            )
            .await
            .expect_err("invalid cursor must fail locally");
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        for invitation_id in [
            "not-a-uuid",
            "0198F0F4-9B72-7000-8000-000000000811",
            "0198f0f4-9b72-6000-8000-000000000811",
        ] {
            let error = Tool::execute(
                &accept,
                ToolCtx {
                    flow_id: "flow-invalid-accept",
                    call_id: "call-invalid-accept",
                    args: &arguments(json!({"invitation_id": invitation_id})),
                    cancel: CancellationToken::new(),
                    on_update: Arc::new(|_| {}),
                    workspace: &workspace,
                },
            )
            .await
            .expect_err("invalid invitation must fail locally");
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        assert_eq!(api.list_calls.load(Ordering::SeqCst), 0);
        assert_eq!(api.accept_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn present_invalid_list_cursors_are_rejected_by_bind_and_execution_before_io() {
        let api = fake_api();
        let list = WorkspaceInvitationListTool::new(api.clone());
        let workspace = workspace_paths();

        for value in [
            json!({"cursor": null}),
            json!({"cursor": ""}),
            json!({"cursor": 7}),
            json!({"cursor": CURSOR, "workspace_id": WORKSPACE_ID}),
        ] {
            let bound_args = BoundExecutionArguments::from_value(value.clone())
                .expect("invalid cursor fixture remains object-shaped");
            let args = arguments(value);
            let bind_error = BoundToolAdapter::bind(
                &list,
                ToolBindCtx {
                    args: &args,
                    workspace: &workspace,
                },
            )
            .await
            .expect_err("present invalid cursor must fail binding");
            assert!(matches!(bind_error, DescribeError::InvalidArguments));

            let execute_error = Tool::execute(
                &list,
                ToolCtx {
                    flow_id: "flow-invalid-list-cursor",
                    call_id: "call-invalid-list-cursor",
                    args: &args,
                    cancel: CancellationToken::new(),
                    on_update: Arc::new(|_| {}),
                    workspace: &workspace,
                },
            )
            .await
            .expect_err("present invalid cursor must fail raw execution");
            assert!(matches!(execute_error, ToolError::InvalidArguments));

            let Err(bound_execute_error) = BoundToolAdapter::execute(
                &list,
                BoundToolCtx {
                    flow_id: "flow-invalid-bound-list-cursor",
                    call_id: "call-invalid-bound-list-cursor",
                    args: &bound_args,
                    committed_effect_permit: committed_effect_permit(
                        "invalid-bound-invitation-list",
                    ),
                    cancel: CancellationToken::new(),
                    on_update: Arc::new(|_| {}),
                    workspace: &workspace,
                },
            )
            .await
            else {
                panic!("present invalid cursor did not fail bound execution");
            };
            assert!(matches!(bound_execute_error, ToolError::InvalidArguments));
        }
        assert_eq!(api.list_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn maximum_invitation_page_stays_below_local_control_output_budget() {
        let invitations = (0..32)
            .map(|index| WorkspaceInvitationSummary {
                invitation_id: format!("0198f0f4-9b72-7000-8000-{index:012x}"),
                workspace_id: format!("0198f0f4-9b72-7000-8001-{index:012x}"),
                workspace_name: "\u{1}".repeat(200),
                created_at: timestamp(1),
                expires_at: timestamp(2),
            })
            .collect();
        let output = render_invitation_list(WorkspaceInvitationListPage {
            invitations,
            next_cursor: Some(CURSOR.to_owned()),
        })
        .expect("render maximum invitation page");
        let UserContent::Text { text } = &output.content[0] else {
            panic!("Workspace invitation page output must be text");
        };
        assert!(
            text.len() < 64 * 1024,
            "rendered page was {} bytes",
            text.len()
        );
        assert_eq!(output.details["invitations"].as_array().unwrap().len(), 32);
        assert_eq!(output.details["next_cursor"], CURSOR);
    }
}
