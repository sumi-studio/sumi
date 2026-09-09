//! Start a durable workspace process, then inspect it without holding a model turn open.

use std::{
    path::{Component, Path},
    sync::Arc,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::{
    apiclient::process::{
        ProcessApi, ProcessApiError, ProcessOperationRequest, ProcessOutputRequest, ProcessStream,
        StartProcessRequest, valid_operation_id,
    },
    provider::types::{ToolDefinition, UserContent},
};

use super::{
    AdapterIdentity, AppActionDescriptor, BoundExecutionArguments, BoundToolAdapter, BoundToolCtx,
    BoundToolExecutionOutcome, CapabilityClass, DescribeError, ResourceScope, ReviewProjection,
    Tool, ToolBindCtx, ToolBinding, ToolCtx, ToolError, ToolOutput, ToolRisk,
};

fn default_cwd() -> String {
    ".".into()
}
fn default_timeout() -> u64 {
    600
}
fn default_limit() -> u64 {
    16_384
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum ProcessAction {
    Start {
        executable: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default = "default_cwd")]
        cwd: String,
        #[serde(default = "default_timeout")]
        timeout_seconds: u64,
    },
    Status {
        operation_id: String,
    },
    ReadOutput {
        operation_id: String,
        stream: ProcessStream,
        #[serde(default)]
        offset: u64,
        #[serde(default = "default_limit")]
        limit: u64,
    },
    Cancel {
        operation_id: String,
    },
}

impl ProcessAction {
    fn validate(&self) -> Result<(), ToolError> {
        let valid = match self {
            Self::Start {
                executable,
                args,
                cwd,
                timeout_seconds,
            } => {
                !executable.is_empty()
                    && executable.len() <= 1024
                    && !executable.contains('\0')
                    && args.len() <= 128
                    && args.iter().all(|arg| !arg.contains('\0'))
                    && args
                        .iter()
                        .fold(executable.len(), |size, arg| size.saturating_add(arg.len()))
                        <= 32 * 1024
                    && !cwd.is_empty()
                    && cwd.len() <= 1024
                    && !cwd.contains('\0')
                    && (cwd == "."
                        || cwd
                            .split('/')
                            .all(|part| !part.is_empty() && part != "." && part != ".."))
                    && !Path::new(cwd).is_absolute()
                    && !Path::new(cwd).components().any(|part| {
                        matches!(
                            part,
                            Component::ParentDir | Component::RootDir | Component::Prefix(_)
                        )
                    })
                    && (1..=3600).contains(timeout_seconds)
            }
            Self::Status { operation_id } | Self::Cancel { operation_id } => {
                valid_operation_id(operation_id)
            }
            Self::ReadOutput {
                operation_id,
                offset,
                limit,
                ..
            } => {
                valid_operation_id(operation_id)
                    && *offset <= 9_007_199_254_740_991
                    && (4..=65_536).contains(limit)
            }
        };
        if valid {
            Ok(())
        } else {
            Err(ToolError::InvalidArguments)
        }
    }

    fn operation(&self) -> &'static str {
        match self {
            Self::Start { .. } => "start",
            Self::Status { .. } => "status",
            Self::ReadOutput { .. } => "read_output",
            Self::Cancel { .. } => "cancel",
        }
    }

    fn capability(&self) -> CapabilityClass {
        match self {
            Self::Start { .. } => CapabilityClass::Execute,
            Self::Cancel { .. } => CapabilityClass::Mutate,
            _ => CapabilityClass::Read,
        }
    }

    fn resource(&self) -> ResourceScope {
        match self {
            Self::Start { .. } => ResourceScope::resource("process", "private_workspace", "self"),
            Self::Status { operation_id }
            | Self::ReadOutput { operation_id, .. }
            | Self::Cancel { operation_id } => {
                ResourceScope::resource("process", "operation", operation_id)
            }
        }
    }
}

pub(crate) struct ProcessTool {
    api: Arc<dyn ProcessApi>,
}

impl ProcessTool {
    pub(crate) fn new(api: Arc<dyn ProcessApi>) -> Self {
        Self { api }
    }

    async fn perform(
        &self,
        action: &ProcessAction,
        call_id: &str,
        cancel: &CancellationToken,
    ) -> Result<ToolOutput, ToolError> {
        let request = async {
            let value = match action {
                ProcessAction::Start {
                    executable,
                    args,
                    cwd,
                    timeout_seconds,
                } => {
                    // This identity is supplied by the sealed runtime invocation, not the model.
                    let request = StartProcessRequest {
                        originating_tool_call_id: call_id.to_owned(),
                        executable: executable.clone(),
                        args: args.clone(),
                        cwd: cwd.clone(),
                        timeout_seconds: *timeout_seconds,
                    };
                    operation_value(self.api.start(&request).await.map_err(map_error)?)?
                }
                ProcessAction::Status { operation_id } => operation_value(
                    self.api
                        .status(&ProcessOperationRequest {
                            operation_id: operation_id.clone(),
                        })
                        .await
                        .map_err(map_error)?,
                )?,
                ProcessAction::Cancel { operation_id } => operation_value(
                    self.api
                        .cancel(&ProcessOperationRequest {
                            operation_id: operation_id.clone(),
                        })
                        .await
                        .map_err(map_error)?,
                )?,
                ProcessAction::ReadOutput {
                    operation_id,
                    stream,
                    offset,
                    limit,
                } => serde_json::to_value(
                    self.api
                        .read_output(&ProcessOutputRequest {
                            operation_id: operation_id.clone(),
                            stream: *stream,
                            offset: *offset,
                            limit: *limit,
                        })
                        .await
                        .map_err(map_error)?,
                )
                .map_err(|_| ToolError::Protocol("process output could not be rendered".into()))?,
            };
            let text = serde_json::to_string(&value).map_err(|_| {
                ToolError::Protocol("process response could not be rendered".into())
            })?;
            Ok(ToolOutput {
                content: vec![UserContent::Text { text }],
                details: value,
                is_error: false,
            })
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(ToolError::Cancelled),
            result = request => result,
        }
    }
}

fn operation_value(
    operation: crate::apiclient::process::ProcessOperation,
) -> Result<Value, ToolError> {
    let mut value = serde_json::to_value(operation)
        .map_err(|_| ToolError::Protocol("process operation could not be rendered".into()))?;
    if let Some(fields) = value.as_object_mut() {
        fields.remove("personality_agent_id");
        fields.remove("event_id");
        fields.remove("originating_tool_call_id");
    }
    Ok(value)
}

fn map_error(error: ProcessApiError) -> ToolError {
    match error {
        ProcessApiError::InvalidRequest => ToolError::InvalidArguments,
        ProcessApiError::Protocol => ToolError::Protocol(error.to_string()),
        _ => ToolError::Rpc(error.to_string()),
    }
}

#[async_trait]
impl Tool for ProcessTool {
    fn def(&self) -> ToolDefinition {
        ToolDefinition {
            name: "process".into(),
            description: "Run a durable process in your private workspace. start returns an operation_id promptly; the process continues after this model turn and its completion is delivered automatically. Use status or bounded read_output when needed; cancel requests termination. The pinned image provides a shell and basic utilities, with no network. Only the workspace and /tmp are writable. Executable and args are passed directly, without shell expansion; cwd is workspace-relative. Output retains up to 1 MiB per stream. Offsets count bytes; continue from next_offset. Do not repeatedly poll or wait for the entire job in the current turn.".into(),
            parameters: json!({"type":"object", "oneOf":[
                {"type":"object","properties":{"action":{"const":"start"},"executable":{"type":"string","minLength":1,"maxLength":1024},"args":{"type":"array","maxItems":128,"items":{"type":"string"}},"cwd":{"type":"string","default":"."},"timeout_seconds":{"type":"integer","minimum":1,"maximum":3600,"default":600}},"required":["action","executable"],"additionalProperties":false},
                {"type":"object","properties":{"action":{"const":"status"},"operation_id":{"type":"string","pattern":"^[0-9a-f]{64}$"}},"required":["action","operation_id"],"additionalProperties":false},
                {"type":"object","properties":{"action":{"const":"read_output"},"operation_id":{"type":"string","pattern":"^[0-9a-f]{64}$"},"stream":{"enum":["stdout","stderr"]},"offset":{"type":"integer","minimum":0,"maximum":9007199254740991_u64,"default":0},"limit":{"type":"integer","minimum":4,"maximum":65536,"default":16384}},"required":["action","operation_id","stream"],"additionalProperties":false},
                {"type":"object","properties":{"action":{"const":"cancel"},"operation_id":{"type":"string","pattern":"^[0-9a-f]{64}$"}},"required":["action","operation_id"],"additionalProperties":false}
            ]}),
        }
    }
    fn risk(&self) -> ToolRisk {
        ToolRisk::Exec
    }
    fn bound_adapter(self: Arc<Self>) -> Option<Arc<dyn BoundToolAdapter>> {
        Some(self)
    }
    async fn execute(&self, _ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
        Err(ToolError::Protocol(
            "process operations require bound execution".into(),
        ))
    }
}

#[async_trait]
impl BoundToolAdapter for ProcessTool {
    fn identity(&self) -> AdapterIdentity {
        AdapterIdentity::new("sumi.process", 1).expect("static adapter identity")
    }
    fn reviewer_read_capable(&self) -> bool {
        true
    }
    async fn bind(&self, ctx: ToolBindCtx<'_>) -> Result<ToolBinding, DescribeError> {
        let action: ProcessAction =
            serde_json::from_value(Value::Object(ctx.args.as_object().clone()))
                .map_err(|_| DescribeError::InvalidArguments)?;
        action
            .validate()
            .map_err(|_| DescribeError::InvalidArguments)?;
        let exact = serde_json::to_value(&action).map_err(|_| DescribeError::InvalidArguments)?;
        let mut review = exact.clone();
        review["actor"] = json!("self");
        review["workspace"] = json!("private workspace");
        Ok(ToolBinding::new(
            AppActionDescriptor::new(
                action.operation(),
                action.capability(),
                vec![action.resource()],
            )?,
            ReviewProjection::from_value(review)?,
            BoundExecutionArguments::from_value(exact)?,
        ))
    }
    async fn execute(&self, ctx: BoundToolCtx<'_>) -> Result<BoundToolExecutionOutcome, ToolError> {
        let action: ProcessAction =
            serde_json::from_value(Value::Object(ctx.args.as_object().clone()))
                .map_err(|_| ToolError::InvalidArguments)?;
        action.validate()?;
        if ctx.cancel.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        let receipt = ctx
            .committed_effect_permit
            .begin_local_effect()
            .complete(|| self.perform(&action, ctx.call_id, &ctx.cancel))
            .await?;
        Ok(BoundToolExecutionOutcome::without_live_post_commit(receipt))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        apiclient::process::{ProcessApiResult, ProcessOperation, ProcessOutput, ProcessState},
        provider::types::{ToolCall, ToolInvocationRoute, ValidatedToolArguments},
        tools::{ToolRegistryBuilder, WorkspacePaths},
    };
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeApi {
        starts: Mutex<Vec<StartProcessRequest>>,
    }

    #[async_trait]
    impl ProcessApi for FakeApi {
        async fn start(&self, request: &StartProcessRequest) -> ProcessApiResult<ProcessOperation> {
            self.starts.lock().unwrap().push(request.clone());
            Ok(ProcessOperation {
                operation_id: "a".repeat(64),
                personality_agent_id: "private-pa".into(),
                originating_tool_call_id: request.originating_tool_call_id.clone(),
                executable: request.executable.clone(),
                args: request.args.clone(),
                cwd: request.cwd.clone(),
                timeout_seconds: request.timeout_seconds,
                state: ProcessState::Accepted,
                event_id: "0198f0f4-9b72-7000-8000-000000000011".into(),
                occurred_at: chrono::Utc::now(),
                started_at: None,
                finished_at: None,
                exit_code: None,
                stdout_bytes: 0,
                stderr_bytes: 0,
                stdout_truncated: false,
                stderr_truncated: false,
                error: None,
            })
        }
        async fn status(&self, _: &ProcessOperationRequest) -> ProcessApiResult<ProcessOperation> {
            panic!("start must not poll")
        }
        async fn read_output(&self, _: &ProcessOutputRequest) -> ProcessApiResult<ProcessOutput> {
            panic!("start must not read output")
        }
        async fn cancel(&self, _: &ProcessOperationRequest) -> ProcessApiResult<ProcessOperation> {
            panic!("start must not cancel")
        }
    }

    fn arguments(value: Value) -> ValidatedToolArguments {
        serde_json::from_value(value).unwrap()
    }

    #[tokio::test]
    async fn process_bound_start_returns_handle_with_trusted_internal_call_identity() {
        let api = Arc::new(FakeApi::default());
        let tool = Arc::new(ProcessTool::new(api.clone()));
        let mut builder = ToolRegistryBuilder::default();
        builder.register(tool).unwrap();
        let registry = builder.build();
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        let sealed = registry
            .bind(
                &ToolCall {
                    provider_call_id: Some("provider-id".into()),
                    id: "internal-start-id".into(),
                    name: "process".into(),
                    route: ToolInvocationRoute::Normal,
                    arguments: arguments(
                        json!({"action":"start","executable":"python3","args":["long_job.py"]}),
                    ),
                },
                "process-flow",
                &workspace,
            )
            .await
            .unwrap();
        assert!(api.starts.lock().unwrap().is_empty());
        let authorized = crate::approval::authority::AuthorizedBoundInvocation::for_test(sealed);
        let outcome = registry
            .execute_bound(authorized, CancellationToken::new(), Arc::new(|_| {}))
            .await
            .unwrap();
        let requests = api.starts.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].originating_tool_call_id, "internal-start-id");
        assert_eq!(requests[0].cwd, ".");
        assert_eq!(requests[0].timeout_seconds, 600);
        assert_eq!(outcome.output.details["state"], "accepted");
        assert_eq!(outcome.output.details["operation_id"], "a".repeat(64));
        assert!(outcome.output.details.get("personality_agent_id").is_none());
        assert!(
            outcome
                .output
                .details
                .get("originating_tool_call_id")
                .is_none()
        );
    }

    #[tokio::test]
    async fn process_binding_classifies_actions_and_rejects_forged_authority() {
        let api = Arc::new(FakeApi::default());
        let tool = ProcessTool::new(api.clone());
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        for (value, capability) in [
            (
                json!({"action":"start","executable":"python3"}),
                CapabilityClass::Execute,
            ),
            (
                json!({"action":"status","operation_id":"a".repeat(64)}),
                CapabilityClass::Read,
            ),
            (
                json!({"action":"read_output","operation_id":"a".repeat(64),"stream":"stderr"}),
                CapabilityClass::Read,
            ),
            (
                json!({"action":"cancel","operation_id":"a".repeat(64)}),
                CapabilityClass::Mutate,
            ),
        ] {
            let args = arguments(value);
            let bound = tool
                .bind(ToolBindCtx {
                    args: &args,
                    workspace: &workspace,
                    executor_identity: None,
                })
                .await
                .unwrap();
            assert_eq!(bound.descriptor.capability, capability);
        }
        for extra in ["originating_tool_call_id", "personality_agent_id"] {
            let mut value = json!({"action":"start","executable":"python3"});
            value[extra] = json!("forged");
            let args = arguments(value);
            assert!(
                tool.bind(ToolBindCtx {
                    args: &args,
                    workspace: &workspace,
                    executor_identity: None
                })
                .await
                .is_err()
            );
        }
        assert!(api.starts.lock().unwrap().is_empty());
    }

    #[test]
    fn process_rejects_invalid_paths_and_unbounded_requests() {
        for cwd in ["../other", "/absolute", "a/../b", "./a", "a//b", "a/", ""] {
            let action: ProcessAction =
                serde_json::from_value(json!({"action":"start","executable":"python3","cwd":cwd}))
                    .unwrap();
            assert!(action.validate().is_err(), "{cwd}");
        }
        for value in [
            json!({"action":"start","executable":"python3","args":["a".repeat(32768)]}),
            json!({"action":"start","executable":"python3","timeout_seconds":3601}),
            json!({"action":"read_output","operation_id":"a".repeat(64),"stream":"stdout","limit":65537}),
            json!({"action":"status","operation_id":"A".repeat(64)}),
        ] {
            assert!(
                serde_json::from_value::<ProcessAction>(value)
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn process_cancelled_invocation_does_not_start_a_job() {
        let api = Arc::new(FakeApi::default());
        let mut builder = ToolRegistryBuilder::default();
        builder
            .register(Arc::new(ProcessTool::new(api.clone())))
            .unwrap();
        let registry = builder.build();
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        let sealed = registry
            .bind(
                &ToolCall {
                    provider_call_id: None,
                    id: "cancelled-start".into(),
                    name: "process".into(),
                    route: ToolInvocationRoute::Normal,
                    arguments: arguments(json!({"action":"start","executable":"python3"})),
                },
                "cancelled-flow",
                &workspace,
            )
            .await
            .unwrap();
        let authorized = crate::approval::authority::AuthorizedBoundInvocation::for_test(sealed);
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(matches!(
            registry
                .execute_bound(authorized, cancel, Arc::new(|_| {}))
                .await,
            Err(crate::tools::BoundExecutionError::Tool(
                ToolError::Cancelled
            ))
        ));
        assert!(api.starts.lock().unwrap().is_empty());
    }
}
