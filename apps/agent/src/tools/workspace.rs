//! Explicit self-orientation tools for Sumi Workspace membership.
//!
//! These tools do not treat the private VM `/workspace` mount as a Sumi
//! Workspace and never infer a default or current Workspace. The authenticated
//! local-control client fixes the actor independently of model arguments.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::apiclient::workspace::{WorkspaceApi, WorkspaceApiError, WorkspaceListPage};
use crate::provider::types::{ToolDefinition, UserContent};

use super::{
    AdapterIdentity, AppActionDescriptor, BoundExecutionArguments, BoundToolAdapter, BoundToolCtx,
    BoundToolExecutionOutcome, CapabilityClass, DescribeError, ResourceScope, ReviewProjection,
    Tool, ToolBindCtx, ToolBinding, ToolCtx, ToolError, ToolOutput, ToolRisk,
};

const LIST_TOOL_NAME: &str = "workspace_list";
const LIST_ADAPTER_ID: &str = "sumi.workspace.list";
const LIST_ADAPTER_VERSION: u32 = 1;
const LIST_CURSOR_BYTES: usize = 76;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ListArguments {
    #[serde(default)]
    cursor: Option<String>,
}

pub(crate) struct WorkspaceListTool {
    api: Arc<dyn WorkspaceApi>,
}

impl WorkspaceListTool {
    pub(crate) fn new(api: Arc<dyn WorkspaceApi>) -> Self {
        Self { api }
    }

    async fn execute_list(
        &self,
        arguments: &ListArguments,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<ToolOutput, ToolError> {
        let page = tokio::select! {
            _ = cancel.cancelled() => return Err(ToolError::Cancelled),
            result = self.api.list_memberships(arguments.cursor.as_deref()) => result,
        }
        .map_err(map_list_error)?;
        render_workspace_list(page)
    }
}

#[async_trait]
impl Tool for WorkspaceListTool {
    fn def(&self) -> ToolDefinition {
        ToolDefinition {
            name: LIST_TOOL_NAME.to_owned(),
            description: concat!(
                "List the Sumi Workspaces where you currently have an active membership. ",
                "Returns one bounded page of canonical Workspace IDs and names. An empty ",
                "page is valid. If next_cursor is present, call again with that exact opaque ",
                "cursor to continue. This ",
                "does not choose a current/default Workspace or install or enable anything."
            )
            .to_owned(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "cursor": {
                        "type": "string",
                        "description": "Opaque next_cursor returned by an earlier workspace_list page.",
                        "minLength": LIST_CURSOR_BYTES,
                        "maxLength": LIST_CURSOR_BYTES
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
impl BoundToolAdapter for WorkspaceListTool {
    fn identity(&self) -> AdapterIdentity {
        AdapterIdentity::new(LIST_ADAPTER_ID, LIST_ADAPTER_VERSION)
            .expect("static Workspace list adapter identity must be valid")
    }

    async fn bind(&self, ctx: ToolBindCtx<'_>) -> Result<ToolBinding, DescribeError> {
        let arguments =
            decode_list(ctx.args.as_object()).map_err(|_| DescribeError::InvalidArguments)?;
        validate_list_arguments(&arguments).map_err(|_| DescribeError::InvalidArguments)?;
        let execution_arguments = list_execution_arguments(&arguments);
        let mut review = json!({
            "operation": "list_memberships",
            "actor": "self",
            "membership_state": "active"
        });
        if let Some(cursor) = &arguments.cursor {
            review
                .as_object_mut()
                .expect("static review projection is an object")
                .insert("cursor".to_owned(), Value::String(cursor.clone()));
        }
        Ok(ToolBinding::new(
            AppActionDescriptor::new(
                "list_memberships",
                CapabilityClass::Read,
                vec![ResourceScope::collection("workspace", "membership")],
            )?,
            ReviewProjection::from_value(review)?,
            BoundExecutionArguments::from_value(execution_arguments)?,
        ))
    }

    async fn execute(&self, ctx: BoundToolCtx<'_>) -> Result<BoundToolExecutionOutcome, ToolError> {
        let arguments =
            decode_list(ctx.args.as_object()).map_err(|_| ToolError::InvalidArguments)?;
        validate_list_arguments(&arguments).map_err(|_| ToolError::InvalidArguments)?;
        Ok(BoundToolExecutionOutcome::without_live_post_commit(
            self.execute_list(&arguments, &ctx.cancel).await?,
        ))
    }
}

fn decode_list(arguments: &Map<String, Value>) -> Result<ListArguments, serde_json::Error> {
    serde_json::from_value(Value::Object(arguments.clone()))
}

fn validate_list_arguments(arguments: &ListArguments) -> Result<(), ()> {
    if arguments.cursor.as_deref().is_some_and(|cursor| {
        cursor.len() != LIST_CURSOR_BYTES
            || !cursor
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    }) {
        return Err(());
    }
    Ok(())
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
        WorkspaceApiError::NotFound | WorkspaceApiError::Conflict | WorkspaceApiError::Protocol => {
            ToolError::Protocol(
                "Workspace membership list response violated its typed contract".to_owned(),
            )
        }
        WorkspaceApiError::Unauthenticated | WorkspaceApiError::Forbidden => {
            ToolError::Rpc("Workspace membership list authorization was rejected".to_owned())
        }
        WorkspaceApiError::ServiceUnavailable | WorkspaceApiError::Transport => {
            ToolError::Rpc("Workspace membership list is temporarily unavailable".to_owned())
        }
    }
}

fn render_workspace_list(page: WorkspaceListPage) -> Result<ToolOutput, ToolError> {
    let mut value = json!({
        "workspaces": page.workspaces
            .into_iter()
            .map(|workspace| json!({
                "workspace_id": workspace.workspace_id,
                "name": workspace.name
            }))
            .collect::<Vec<_>>()
    });
    if let Some(cursor) = page.next_cursor {
        value
            .as_object_mut()
            .expect("static Workspace page output is an object")
            .insert("next_cursor".to_owned(), Value::String(cursor));
    }
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

    use chrono::Utc;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        apiclient::workspace::{WorkspaceApiResult, WorkspaceSummary},
        approval::{
            authority::PolicyDecisionRecord,
            route_broker::{PendingApprovalRequest, provider_review_inputs_for_test},
            route_policy::{ElevatedPolicyEvaluation, RoutePolicy},
            route_reviewer::{EscalationReviewRequest, escalation_provider_wire_bodies_for_test},
        },
        provider::types::{ToolCall, ToolInvocationRoute, ValidatedToolArguments},
        store::Redactor,
        tools::{ToolRegistryBuilder, WorkspacePaths},
    };

    const WORKSPACE_ID: &str = "0198f0f4-9b72-7000-8000-000000000011";
    const CURSOR: &str =
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    struct FakeWorkspaceApi {
        calls: AtomicUsize,
        cursors: Mutex<Vec<Option<String>>>,
        result: WorkspaceApiResult<WorkspaceListPage>,
    }

    #[async_trait]
    impl WorkspaceApi for FakeWorkspaceApi {
        async fn list_memberships(
            &self,
            cursor: Option<&str>,
        ) -> WorkspaceApiResult<WorkspaceListPage> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.cursors.lock().unwrap().push(cursor.map(str::to_owned));
            self.result.clone()
        }
    }

    fn page(workspaces: Vec<WorkspaceSummary>, next_cursor: Option<&str>) -> WorkspaceListPage {
        WorkspaceListPage {
            workspaces,
            next_cursor: next_cursor.map(str::to_owned),
        }
    }

    fn arguments(value: Value) -> ValidatedToolArguments {
        serde_json::from_value(value).expect("object-shaped tool arguments")
    }

    fn workspace_paths() -> WorkspacePaths {
        WorkspacePaths::new("/workspace").expect("absolute fixture workspace")
    }

    #[tokio::test]
    async fn list_binding_is_read_only_side_effect_free_and_has_no_actor_argument() {
        let api = Arc::new(FakeWorkspaceApi {
            calls: AtomicUsize::new(0),
            cursors: Mutex::new(Vec::new()),
            result: Ok(page(
                vec![WorkspaceSummary {
                    workspace_id: WORKSPACE_ID.to_owned(),
                    name: "Canonical team".to_owned(),
                }],
                None,
            )),
        });
        let tool = WorkspaceListTool::new(api.clone());
        let args = arguments(json!({}));
        let workspace = workspace_paths();

        let definition = tool.def();
        assert_eq!(definition.name, "workspace_list");
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
        .expect("bind list");

        assert_eq!(api.calls.load(Ordering::SeqCst), 0, "binding performed I/O");
        assert_eq!(binding.descriptor.operation, "list_memberships");
        assert_eq!(binding.descriptor.capability, CapabilityClass::Read);
        assert_eq!(
            binding.descriptor.resource_scopes,
            vec![ResourceScope::collection("workspace", "membership")]
        );
        assert_eq!(binding.review_projection.as_object()["actor"], "self");
        assert!(binding.execution_arguments.as_object().is_empty());

        let outcome = BoundToolAdapter::execute(
            &tool,
            BoundToolCtx {
                flow_id: "flow-a",
                call_id: "call-a",
                args: &binding.execution_arguments,
                cancel: CancellationToken::new(),
                on_update: Arc::new(|_| {}),
                workspace: &workspace,
            },
        )
        .await
        .expect("execute exact list");

        assert_eq!(api.calls.load(Ordering::SeqCst), 1);
        assert_eq!(*api.cursors.lock().unwrap(), vec![None]);
        assert_eq!(
            outcome.output.details["workspaces"][0]["workspace_id"],
            WORKSPACE_ID
        );
        assert_eq!(
            outcome.output.details["workspaces"][0]["name"],
            "Canonical team"
        );
        assert_eq!(
            outcome.output.details["workspaces"][0]
                .as_object()
                .unwrap()
                .len(),
            2
        );
        assert!(!outcome.output.is_error);
    }

    #[tokio::test]
    async fn list_accepts_empty_membership_set_without_creating_a_default() {
        let api = Arc::new(FakeWorkspaceApi {
            calls: AtomicUsize::new(0),
            cursors: Mutex::new(Vec::new()),
            result: Ok(page(Vec::new(), None)),
        });
        let tool = WorkspaceListTool::new(api.clone());
        let args = arguments(json!({}));
        let workspace = workspace_paths();

        let output = Tool::execute(
            &tool,
            ToolCtx {
                flow_id: "flow-empty",
                call_id: "call-empty",
                args: &args,
                cancel: CancellationToken::new(),
                on_update: Arc::new(|_| {}),
                workspace: &workspace,
            },
        )
        .await
        .expect("empty membership list is valid");

        assert_eq!(api.calls.load(Ordering::SeqCst), 1);
        assert_eq!(output.details, json!({"workspaces": []}));
    }

    #[tokio::test]
    async fn list_rejects_every_model_supplied_identity_or_scope_field() {
        let api = Arc::new(FakeWorkspaceApi {
            calls: AtomicUsize::new(0),
            cursors: Mutex::new(Vec::new()),
            result: Ok(page(Vec::new(), None)),
        });
        let tool = WorkspaceListTool::new(api.clone());
        let args = arguments(json!({
            "personality_agent_id": "0198f0f4-9b72-7000-8000-000000000099",
            "workspace_id": WORKSPACE_ID
        }));
        let workspace = workspace_paths();

        let error = Tool::execute(
            &tool,
            ToolCtx {
                flow_id: "flow-rejected",
                call_id: "call-rejected",
                args: &args,
                cancel: CancellationToken::new(),
                on_update: Arc::new(|_| {}),
                workspace: &workspace,
            },
        )
        .await
        .expect_err("identity and scope fields are not accepted");

        assert!(matches!(error, ToolError::InvalidArguments));
        assert_eq!(api.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn list_seals_the_exact_optional_cursor_for_review_and_execution() {
        let api = Arc::new(FakeWorkspaceApi {
            calls: AtomicUsize::new(0),
            cursors: Mutex::new(Vec::new()),
            result: Ok(page(Vec::new(), Some(CURSOR))),
        });
        let tool = WorkspaceListTool::new(api.clone());
        let args = arguments(json!({"cursor": CURSOR}));
        let workspace = workspace_paths();

        let binding = BoundToolAdapter::bind(
            &tool,
            ToolBindCtx {
                args: &args,
                workspace: &workspace,
            },
        )
        .await
        .expect("bind cursor page");

        assert_eq!(binding.review_projection.as_object()["cursor"], CURSOR);
        assert_eq!(binding.execution_arguments.as_object()["cursor"], CURSOR);
        let outcome = BoundToolAdapter::execute(
            &tool,
            BoundToolCtx {
                flow_id: "flow-page",
                call_id: "call-page",
                args: &binding.execution_arguments,
                cancel: CancellationToken::new(),
                on_update: Arc::new(|_| {}),
                workspace: &workspace,
            },
        )
        .await
        .expect("execute cursor page");

        assert_eq!(*api.cursors.lock().unwrap(), vec![Some(CURSOR.to_owned())]);
        assert_eq!(outcome.output.details["next_cursor"], CURSOR);
    }

    #[tokio::test]
    async fn opaque_cursor_stays_exact_for_human_and_execution_but_never_reaches_reviewer_wire() {
        let api = Arc::new(FakeWorkspaceApi {
            calls: AtomicUsize::new(0),
            cursors: Mutex::new(Vec::new()),
            result: Ok(page(Vec::new(), None)),
        });
        let tool = Arc::new(WorkspaceListTool::new(api));
        let mut builder = ToolRegistryBuilder::default();
        builder
            .register(tool)
            .expect("register Workspace list tool");
        let registry = builder.build();
        let workspace = workspace_paths();
        let call = ToolCall {
            id: "cursor-review".to_owned(),
            name: LIST_TOOL_NAME.to_owned(),
            route: ToolInvocationRoute::Elevated,
            arguments: arguments(json!({"cursor": CURSOR})),
        };
        let sealed = registry
            .bind(&call, "flow-cursor-review", &workspace)
            .await
            .expect("bind real Workspace cursor");
        let bound = registry
            .validate_bound(&sealed)
            .expect("validate Workspace binding");

        assert_eq!(bound.review_projection.as_object()["cursor"], CURSOR);
        assert_eq!(bound.execution_arguments.as_object()["cursor"], CURSOR);
        assert_eq!(
            serde_json::to_string(&bound.provider_review_projection)
                .expect("provider-safe cursor projection")
                .matches(CURSOR)
                .count(),
            0
        );

        let human_request = PendingApprovalRequest::from_bound(
            "approval-cursor".to_owned(),
            ToolInvocationRoute::Elevated,
            bound,
            &Redactor::v1(),
        )
        .expect("Human cursor request")
        .public_request();
        assert!(
            serde_json::to_string(&human_request)
                .expect("encoded Human cursor request")
                .contains(CURSOR)
        );

        let policy = RoutePolicy::baseline_only_v1();
        let snapshot = match policy.evaluate_elevated(bound, Utc::now()) {
            ElevatedPolicyEvaluation::Ready { snapshot } => snapshot,
            other => panic!("Workspace cursor expected Elevated/Ready, got {other:?}"),
        };
        let (sealed_evidence, policy_evidence) = provider_review_inputs_for_test(
            bound,
            ToolInvocationRoute::Elevated,
            PolicyDecisionRecord::ElevatedPreflight,
            &snapshot,
        )
        .expect("Workspace cursor reviewer inputs");
        let request = EscalationReviewRequest {
            sealed_evidence,
            policy: policy_evidence,
        };
        let local_digests = [
            bound.proposal_digest.to_hex(),
            bound.descriptor_digest.to_hex(),
            bound
                .evidence_digest()
                .expect("local evidence digest")
                .to_hex(),
        ];
        for (provider, body) in escalation_provider_wire_bodies_for_test(request) {
            let encoded = body.to_string();
            assert_eq!(
                encoded.matches(CURSOR).count(),
                0,
                "Workspace cursor leaked through {provider}"
            );
            for digest in &local_digests {
                assert_eq!(
                    encoded.matches(digest).count(),
                    0,
                    "Workspace exact digest leaked through {provider}"
                );
            }
        }
    }

    #[tokio::test]
    async fn list_rejects_invalid_cursor_shape_before_api_io() {
        let api = Arc::new(FakeWorkspaceApi {
            calls: AtomicUsize::new(0),
            cursors: Mutex::new(Vec::new()),
            result: Ok(page(Vec::new(), None)),
        });
        let tool = WorkspaceListTool::new(api.clone());
        let workspace = workspace_paths();

        for value in ["short".to_owned(), format!("{}!", &CURSOR[..75])] {
            let args = arguments(json!({"cursor": value}));
            let error = Tool::execute(
                &tool,
                ToolCtx {
                    flow_id: "flow-invalid-cursor",
                    call_id: "call-invalid-cursor",
                    args: &args,
                    cancel: CancellationToken::new(),
                    on_update: Arc::new(|_| {}),
                    workspace: &workspace,
                },
            )
            .await
            .expect_err("invalid cursor must fail before API I/O");
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        assert_eq!(api.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn server_cursor_rejection_is_invalid_model_input() {
        let api = Arc::new(FakeWorkspaceApi {
            calls: AtomicUsize::new(0),
            cursors: Mutex::new(Vec::new()),
            result: Err(WorkspaceApiError::InvalidRequest),
        });
        let tool = WorkspaceListTool::new(api.clone());
        let args = arguments(json!({"cursor": CURSOR}));
        let workspace = workspace_paths();

        let error = Tool::execute(
            &tool,
            ToolCtx {
                flow_id: "flow-tampered-cursor",
                call_id: "call-tampered-cursor",
                args: &args,
                cancel: CancellationToken::new(),
                on_update: Arc::new(|_| {}),
                workspace: &workspace,
            },
        )
        .await
        .expect_err("server-rejected cursor is invalid input");

        assert!(matches!(error, ToolError::InvalidArguments));
        assert_eq!(api.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn maximum_page_stays_below_the_local_control_output_budget() {
        let workspaces = (0..32)
            .map(|index| WorkspaceSummary {
                workspace_id: format!("0198f0f4-9b72-7000-8000-{index:012x}"),
                // A control rune takes serde_json's longest six-byte escape.
                name: "\u{1}".repeat(200),
            })
            .collect();
        let output = render_workspace_list(page(workspaces, Some(CURSOR)))
            .expect("render maximum Workspace page");
        let UserContent::Text { text } = &output.content[0] else {
            panic!("Workspace page output must be text");
        };
        assert!(
            text.len() < 64 * 1024,
            "rendered page was {} bytes",
            text.len()
        );
        assert_eq!(output.details["workspaces"].as_array().unwrap().len(), 32);
        assert_eq!(output.details["next_cursor"], CURSOR);
    }
}
