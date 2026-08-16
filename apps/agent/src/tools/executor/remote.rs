//! Frozen model-tool adapters for a supervisor-supplied executor client.
//!
//! This module translates already-validated model arguments into the T13 RPC
//! contract. It deliberately owns neither executor bootstrap nor generation
//! rotation. The registry copies the client's immutable RPC identity only for
//! Session-start validation; the bounded execution identifier below remains a
//! retry-stable live invocation identity, not the durable T26 idempotency key.

use std::{
    fmt::Write as _,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use super::{ArtifactResponse, ExecutorClient, ExecutorOperation, ExecutorResponse};
use crate::{
    approval::authority::{CommittedEffectReceipt, CommittedExecutionPermit},
    provider::types::{ToolDefinition, ValidatedToolArguments},
    tools::{
        AdapterIdentity, AppActionDescriptor, BoundExecutionArguments, BoundToolAdapter,
        BoundToolCtx, BoundToolExecutionOutcome, CapabilityClass, DescribeError, ResourceScope,
        ReviewProjection, Tool, ToolBindCtx, ToolBinding, ToolCtx, ToolError, ToolOutput,
        ToolRegistry, ToolRegistryBuilder, ToolRisk, WorkspacePaths,
        fs::normalize_glob_pattern,
        text_output,
        truncate::{
            DEFAULT_MAX_BYTES, DEFAULT_MAX_LINES, RetainedOutput, TruncationOptions,
            render_bounded_output, truncate_head,
        },
    },
};

const EXECUTION_ID_DOMAIN: &[u8] = b"sumi-live-executor-v1";
const EXECUTION_ID_PREFIX: &str = "exec-";
const BINDING_ADAPTER_ID: &str = "sumi.foundation.workspace";
const BINDING_ADAPTER_VERSION: u32 = 1;
const MAX_WORKSPACE_PATH_BYTES: usize = 4 * 1024;
const MAX_GLOB_PATTERN_BYTES: usize = 4 * 1024;
const MAX_GREP_PATTERN_BYTES: usize = 16 * 1024;

#[async_trait]
trait ExecutorInvoker: Send + Sync {
    async fn execute(
        &self,
        operation: ExecutorOperation,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<ExecutorResponse, ToolError>;

    async fn execute_authorized(
        &self,
        operation: ExecutorOperation,
        permit: CommittedExecutionPermit,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<CommittedEffectReceipt<ExecutorResponse>, ToolError>;
}

#[async_trait]
impl ExecutorInvoker for ExecutorClient {
    async fn execute(
        &self,
        operation: ExecutorOperation,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<ExecutorResponse, ToolError> {
        ExecutorClient::execute(self, operation, cancel, on_update).await
    }

    async fn execute_authorized(
        &self,
        operation: ExecutorOperation,
        permit: CommittedExecutionPermit,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<CommittedEffectReceipt<ExecutorResponse>, ToolError> {
        ExecutorClient::execute_authorized(self, operation, permit, cancel, on_update).await
    }
}

/// Builds the production executor registry from one immutable,
/// supervisor-issued client. The critical Unix endpoint exposes only bounded,
/// workspace-confined read and discovery operations.
pub fn remote_executor_registry(client: Arc<ExecutorClient>) -> Result<ToolRegistry, ToolError> {
    remote_executor_registry_with_tools(client, std::iter::empty())
}

/// Compose the generation-bound executor tools with control-plane-backed
/// domain tools owned by the same PersonalityAgent runtime.
pub(crate) fn remote_executor_registry_with_tools(
    client: Arc<ExecutorClient>,
    extra_tools: impl IntoIterator<Item = Arc<dyn Tool>>,
) -> Result<ToolRegistry, ToolError> {
    let identity = client.identity().clone();
    let mut builder = ToolRegistryBuilder::default();
    for kind in PRODUCTION_REMOTE_TOOL_KINDS {
        builder.register(Arc::new(RemoteTool {
            kind,
            client: client.clone(),
        }))?;
    }
    for tool in extra_tools {
        builder.register(tool)?;
    }
    builder.build_bound_for_executor_identity(identity)
}

#[cfg(test)]
fn registry_from_invoker(client: Arc<dyn ExecutorInvoker>) -> Result<ToolRegistry, ToolError> {
    broad_test_registry_from_invoker(client)
}

#[cfg(test)]
fn broad_test_registry_from_invoker(
    client: Arc<dyn ExecutorInvoker>,
) -> Result<ToolRegistry, ToolError> {
    let mut builder = ToolRegistryBuilder::default();
    for kind in [
        RemoteToolKind::ReadFile,
        RemoteToolKind::WriteFile,
        RemoteToolKind::EditFile,
        RemoteToolKind::Delete,
        RemoteToolKind::ListDir,
        RemoteToolKind::Glob,
        RemoteToolKind::Grep,
        RemoteToolKind::Bash,
    ] {
        builder.register(Arc::new(RemoteTool {
            kind,
            client: client.clone(),
        }))?;
    }
    Ok(builder.build())
}

#[cfg(test)]
fn bound_test_registry_from_invoker(
    client: Arc<dyn ExecutorInvoker>,
) -> Result<ToolRegistry, ToolError> {
    let mut builder = ToolRegistryBuilder::default();
    for kind in PRODUCTION_REMOTE_TOOL_KINDS {
        builder.register(Arc::new(RemoteTool {
            kind,
            client: client.clone(),
        }))?;
    }
    Ok(builder.build())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RemoteToolKind {
    WorkspaceReadFile,
    WorkspaceGrep,
    ReadFile,
    WriteFile,
    EditFile,
    Delete,
    ListDir,
    Glob,
    Grep,
    Bash,
}

const PRODUCTION_REMOTE_TOOL_KINDS: [RemoteToolKind; 4] = [
    RemoteToolKind::WorkspaceReadFile,
    RemoteToolKind::ListDir,
    RemoteToolKind::Glob,
    RemoteToolKind::WorkspaceGrep,
];

struct RemoteTool {
    kind: RemoteToolKind,
    client: Arc<dyn ExecutorInvoker>,
}

#[async_trait]
impl Tool for RemoteTool {
    fn def(&self) -> ToolDefinition {
        match self.kind {
            RemoteToolKind::WorkspaceReadFile => definition::<WorkspaceReadFileArgs>(
                "read_file",
                "Read UTF-8 text from a workspace path. Artifact handles are not accepted.",
            ),
            RemoteToolKind::ReadFile => definition::<ReadFileArgs>(
                "read_file",
                "Read UTF-8 text from a workspace path or artifact handle.",
            ),
            RemoteToolKind::WorkspaceGrep => definition::<WorkspaceGrepArgs>(
                "grep",
                "Search a workspace path with a regular expression. Artifact handles are not accepted.",
            ),
            RemoteToolKind::WriteFile => definition::<WriteFileArgs>(
                "write_file",
                "Replace a workspace file with UTF-8 text.",
            ),
            RemoteToolKind::EditFile => definition::<EditFileArgs>(
                "edit_file",
                "Replace one unique text occurrence in a workspace file.",
            ),
            RemoteToolKind::Delete => {
                definition::<PathArgs>("delete", "Delete one workspace file.")
            }
            RemoteToolKind::ListDir => {
                definition::<PathArgs>("list_dir", "List one workspace directory.")
            }
            RemoteToolKind::Glob => {
                definition::<GlobArgs>("glob", "Find workspace paths matching a glob pattern.")
            }
            RemoteToolKind::Grep => definition::<GrepArgs>(
                "grep",
                "Search a workspace path or artifact handle with a regular expression.",
            ),
            RemoteToolKind::Bash => {
                definition::<BashArgs>("bash", "Run one command in the workspace shell.")
            }
        }
    }

    fn risk(&self) -> ToolRisk {
        match self.kind {
            RemoteToolKind::WorkspaceReadFile
            | RemoteToolKind::WorkspaceGrep
            | RemoteToolKind::ReadFile
            | RemoteToolKind::ListDir
            | RemoteToolKind::Glob
            | RemoteToolKind::Grep => ToolRisk::ReadOnly,
            RemoteToolKind::WriteFile | RemoteToolKind::EditFile | RemoteToolKind::Delete => {
                ToolRisk::Mutating
            }
            RemoteToolKind::Bash => ToolRisk::Exec,
        }
    }

    fn bound_adapter(self: Arc<Self>) -> Option<Arc<dyn BoundToolAdapter>> {
        self.kind
            .supports_production_binding()
            .then_some(self as Arc<dyn BoundToolAdapter>)
    }

    async fn execute(&self, ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
        let execution_id = execution_id(ctx.flow_id, ctx.call_id);
        let operation = self.kind.operation(ctx.args, execution_id)?;
        let read_context = self.kind.read_context(&operation);
        self.execute_operation(operation, read_context, ctx.cancel, ctx.on_update)
            .await
    }
}

#[async_trait]
impl BoundToolAdapter for RemoteTool {
    fn identity(&self) -> AdapterIdentity {
        AdapterIdentity::new(BINDING_ADAPTER_ID, BINDING_ADAPTER_VERSION)
            .expect("static foundation workspace binding adapter identity must be valid")
    }

    fn reviewer_read_capable(&self) -> bool {
        matches!(
            self.kind,
            RemoteToolKind::WorkspaceReadFile
                | RemoteToolKind::ListDir
                | RemoteToolKind::Glob
                | RemoteToolKind::WorkspaceGrep
        )
    }

    async fn bind(&self, ctx: ToolBindCtx<'_>) -> Result<ToolBinding, DescribeError> {
        self.kind.bind(ctx)
    }

    async fn execute(&self, ctx: BoundToolCtx<'_>) -> Result<BoundToolExecutionOutcome, ToolError> {
        let execution_id = execution_id(ctx.flow_id, ctx.call_id);
        let (operation, read_context) = self.kind.bound_operation(ctx.args, execution_id)?;
        if ctx.cancel.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        let effect_receipt = self
            .client
            .execute_authorized(
                operation,
                ctx.committed_effect_permit,
                ctx.cancel,
                ctx.on_update,
            )
            .await?;
        let effect_receipt =
            effect_receipt.try_map(|response| self.kind.output(response, read_context))?;
        Ok(BoundToolExecutionOutcome::without_live_post_commit(
            effect_receipt,
        ))
    }
}

impl RemoteTool {
    async fn execute_operation(
        &self,
        operation: ExecutorOperation,
        read_context: Option<ReadContext>,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<ToolOutput, ToolError> {
        let response = self.client.execute(operation, cancel, on_update).await?;
        self.kind.output(response, read_context)
    }
}

impl RemoteToolKind {
    fn supports_production_binding(self) -> bool {
        matches!(
            self,
            Self::WorkspaceReadFile | Self::ListDir | Self::Glob | Self::WorkspaceGrep
        )
    }

    fn read_context(self, operation: &ExecutorOperation) -> Option<ReadContext> {
        match operation {
            ExecutorOperation::ReadFile {
                path,
                offset,
                limit,
                ..
            } => Some(ReadContext {
                request_offset: *offset,
                rpc_limit: *limit,
                artifact: self == Self::ReadFile && path.starts_with("artifact://"),
            }),
            _ => None,
        }
    }

    fn bind(self, ctx: ToolBindCtx<'_>) -> Result<ToolBinding, DescribeError> {
        match self {
            Self::WorkspaceReadFile => {
                let args: WorkspaceReadFileArgs = decode_for_binding(ctx.args)?;
                let args = BoundReadFileArgs {
                    path: normalize_workspace_path(&args.path, ctx.workspace)?,
                    offset: args.offset,
                    limit: args.limit,
                };
                workspace_binding(
                    "read_file",
                    ResourceScope::resource(BINDING_ADAPTER_ID, "path", &args.path),
                    &args,
                )
            }
            Self::ListDir => {
                let args: PathArgs = decode_for_binding(ctx.args)?;
                let args = BoundPathArgs {
                    path: normalize_workspace_path(&args.path, ctx.workspace)?,
                };
                workspace_binding(
                    "list_dir",
                    ResourceScope::resource(BINDING_ADAPTER_ID, "path", &args.path),
                    &args,
                )
            }
            Self::Glob => {
                let args: GlobArgs = decode_for_binding(ctx.args)?;
                let args = BoundGlobArgs {
                    pattern: normalize_workspace_glob(&args.pattern)?,
                };
                workspace_binding(
                    "glob",
                    ResourceScope::resource(BINDING_ADAPTER_ID, "glob_selector", &args.pattern),
                    &args,
                )
            }
            Self::WorkspaceGrep => {
                let args: WorkspaceGrepArgs = decode_for_binding(ctx.args)?;
                validate_review_text(&args.pattern, MAX_GREP_PATTERN_BYTES)?;
                regex::Regex::new(&args.pattern).map_err(|_| DescribeError::InvalidArguments)?;
                let args = BoundGrepArgs {
                    path: normalize_workspace_path(&args.path, ctx.workspace)?,
                    pattern: args.pattern,
                };
                workspace_binding(
                    "grep",
                    ResourceScope::resource(BINDING_ADAPTER_ID, "path", &args.path),
                    &args,
                )
            }
            _ => Err(DescribeError::InvalidDescriptor {
                reason: "unpublished foundation tool has no bound adapter".to_owned(),
            }),
        }
    }

    fn bound_operation(
        self,
        args: &BoundExecutionArguments,
        execution_id: String,
    ) -> Result<(ExecutorOperation, Option<ReadContext>), ToolError> {
        let operation = match self {
            Self::WorkspaceReadFile => {
                let args: BoundReadFileArgs = decode_bound(args)?;
                ExecutorOperation::ReadFile {
                    path: args.path,
                    offset: args.offset,
                    limit: args.limit,
                    execution_id,
                }
            }
            Self::ListDir => {
                let args: BoundPathArgs = decode_bound(args)?;
                ExecutorOperation::ListDir {
                    path: args.path,
                    execution_id,
                }
            }
            Self::Glob => {
                let args: BoundGlobArgs = decode_bound(args)?;
                ExecutorOperation::Glob {
                    pattern: args.pattern,
                    execution_id,
                }
            }
            Self::WorkspaceGrep => {
                let args: BoundGrepArgs = decode_bound(args)?;
                ExecutorOperation::Grep {
                    path: args.path,
                    pattern: args.pattern,
                    execution_id,
                }
            }
            _ => {
                return Err(ToolError::Protocol(
                    "executor tool has no bound-operation adapter".to_owned(),
                ));
            }
        };
        let read_context = self.read_context(&operation);
        Ok((operation, read_context))
    }

    fn operation(
        self,
        args: &ValidatedToolArguments,
        execution_id: String,
    ) -> Result<ExecutorOperation, ToolError> {
        Ok(match self {
            Self::WorkspaceReadFile => {
                let args: WorkspaceReadFileArgs = decode(args)?;
                if args.path.starts_with("artifact://") {
                    return Err(ToolError::InvalidPath(
                        "production read_file accepts workspace paths only".to_owned(),
                    ));
                }
                ExecutorOperation::ReadFile {
                    path: args.path,
                    offset: args.offset,
                    limit: args.limit,
                    execution_id,
                }
            }
            Self::ReadFile => {
                let args: ReadFileArgs = decode(args)?;
                let limit = if args.path.starts_with("artifact://") {
                    args.limit.min(artifact_source_capacity())
                } else {
                    args.limit
                };
                ExecutorOperation::ReadFile {
                    path: args.path,
                    offset: args.offset,
                    limit,
                    execution_id,
                }
            }
            Self::WorkspaceGrep => {
                let args: WorkspaceGrepArgs = decode(args)?;
                if args.path.starts_with("artifact://") {
                    return Err(ToolError::InvalidPath(
                        "production grep accepts workspace paths only".to_owned(),
                    ));
                }
                ExecutorOperation::Grep {
                    path: args.path,
                    pattern: args.pattern,
                    execution_id,
                }
            }
            Self::WriteFile => {
                let args: WriteFileArgs = decode(args)?;
                ExecutorOperation::WriteFile {
                    path: args.path,
                    content: args.content,
                    execution_id,
                }
            }
            Self::EditFile => {
                let args: EditFileArgs = decode(args)?;
                ExecutorOperation::EditFile {
                    path: args.path,
                    old_string: args.old_string,
                    new_string: args.new_string,
                    execution_id,
                }
            }
            Self::Delete => ExecutorOperation::RemoveFile {
                path: decode::<PathArgs>(args)?.path,
                execution_id,
            },
            Self::ListDir => ExecutorOperation::ListDir {
                path: decode::<PathArgs>(args)?.path,
                execution_id,
            },
            Self::Glob => ExecutorOperation::Glob {
                pattern: decode::<GlobArgs>(args)?.pattern,
                execution_id,
            },
            Self::Grep => {
                let args: GrepArgs = decode(args)?;
                ExecutorOperation::Grep {
                    path: args.path,
                    pattern: args.pattern,
                    execution_id,
                }
            }
            Self::Bash => ExecutorOperation::Bash {
                command: decode::<BashArgs>(args)?.command,
                execution_id,
            },
        })
    }

    fn output(
        self,
        response: ExecutorResponse,
        read_context: Option<ReadContext>,
    ) -> Result<ToolOutput, ToolError> {
        match (self, response) {
            (Self::WorkspaceReadFile | Self::ReadFile, ExecutorResponse::ReadFile { result }) => {
                let content = render_bounded_output(&result, RetainedOutput::Head, None, &[]);
                Ok(text_output(content, to_value(result)?))
            }
            (
                Self::ReadFile,
                ExecutorResponse::Artifact {
                    response: ArtifactResponse::Read { content, eof },
                },
            ) => render_artifact_page(content, eof, read_context),
            (Self::WriteFile, ExecutorResponse::Written {}) => {
                Ok(text_output("File written.", json!({"written": true})))
            }
            (Self::EditFile, ExecutorResponse::Edited {}) => {
                Ok(text_output("File edited.", json!({"edited": true})))
            }
            (Self::Delete, ExecutorResponse::Removed {}) => {
                Ok(text_output("File deleted.", json!({"deleted": true})))
            }
            (Self::ListDir, ExecutorResponse::Listed { entries }) => {
                let rendered = bounded_lines(entries.iter().map(String::as_str));
                Ok(text_output(
                    rendered,
                    json!({"entries": entries, "count": entries.len()}),
                ))
            }
            (Self::Glob, ExecutorResponse::Globbed { paths }) => {
                let rendered = bounded_lines(paths.iter().map(String::as_str));
                Ok(text_output(
                    rendered,
                    json!({"paths": paths, "count": paths.len()}),
                ))
            }
            (Self::WorkspaceGrep | Self::Grep, ExecutorResponse::Grepped { matches }) => {
                let rendered = bounded_lines(
                    matches
                        .iter()
                        .map(|item| format!("{}:{}:{}", item.path, item.line_number, item.line)),
                );
                Ok(text_output(
                    rendered,
                    json!({"matches": matches, "count": matches.len(), "source": "workspace"}),
                ))
            }
            (
                Self::Grep,
                ExecutorResponse::Artifact {
                    response: ArtifactResponse::Grep { matches },
                },
            ) => {
                let rendered = bounded_lines(
                    matches
                        .iter()
                        .map(|item| format!("{}:{}", item.line_number, item.line)),
                );
                Ok(text_output(
                    rendered,
                    json!({"matches": matches, "count": matches.len(), "source": "artifact"}),
                ))
            }
            (Self::Bash, ExecutorResponse::Bash { result }) => {
                let is_error = result.exit_code != Some(0)
                    || result.cancelled
                    || result.resource_limit.is_some();
                let terminal = bash_terminal_lines(&result);
                let content = render_bounded_output(
                    &result.truncation,
                    RetainedOutput::Tail,
                    result.artifact_handle.as_deref(),
                    &terminal,
                );
                let mut output = text_output(content, to_value(result)?);
                output.is_error = is_error;
                Ok(output)
            }
            _ => Err(ToolError::Protocol(
                "executor adapter received a response for a different operation".to_owned(),
            )),
        }
    }
}

fn definition<P: JsonSchema>(name: &str, description: &str) -> ToolDefinition {
    ToolDefinition {
        name: name.to_owned(),
        description: description.to_owned(),
        parameters: serde_json::to_value(schemars::schema_for!(P))
            .unwrap_or_else(|_| Value::Object(Default::default())),
    }
}

fn decode<P: DeserializeOwned>(args: &ValidatedToolArguments) -> Result<P, ToolError> {
    serde_json::from_value(Value::Object(args.as_object().clone()))
        .map_err(|_| ToolError::InvalidArguments)
}

fn decode_for_binding<P: DeserializeOwned>(
    args: &ValidatedToolArguments,
) -> Result<P, DescribeError> {
    serde_json::from_value(Value::Object(args.as_object().clone()))
        .map_err(|_| DescribeError::InvalidArguments)
}

fn decode_bound<P: DeserializeOwned>(args: &BoundExecutionArguments) -> Result<P, ToolError> {
    serde_json::from_value(Value::Object(args.as_object().clone()))
        .map_err(|_| ToolError::InvalidArguments)
}

fn workspace_binding<P: Serialize>(
    operation: &str,
    scope: ResourceScope,
    args: &P,
) -> Result<ToolBinding, DescribeError> {
    let execution =
        serde_json::to_value(args).map_err(|error| DescribeError::InvalidBoundArguments {
            reason: format!("foundation workspace arguments could not be encoded: {error}"),
        })?;
    let Value::Object(execution_object) = execution else {
        return Err(DescribeError::InvalidBoundArguments {
            reason: "foundation workspace arguments must encode as an object".to_owned(),
        });
    };
    let mut review = execution_object.clone();
    review.insert("operation".to_owned(), Value::String(operation.to_owned()));
    Ok(ToolBinding::new(
        AppActionDescriptor::new(operation, CapabilityClass::Read, vec![scope])?,
        ReviewProjection::from_value(Value::Object(review))?,
        BoundExecutionArguments::from_value(Value::Object(execution_object))?,
    ))
}

fn validate_review_text(value: &str, max_bytes: usize) -> Result<(), DescribeError> {
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(DescribeError::InvalidArguments);
    }
    Ok(())
}

fn validate_workspace_selector(value: &str, max_bytes: usize) -> Result<(), DescribeError> {
    validate_review_text(value, max_bytes)?;
    if value.starts_with("artifact://") {
        return Err(DescribeError::InvalidArguments);
    }
    Ok(())
}

/// Bind a UTF-8 display path to one stable lexical pathname in the fixed
/// executor workspace. This deliberately performs no filesystem lookup: the
/// executor remains responsible for `openat2`/no-symlink enforcement when the
/// operation is eventually run.
fn normalize_workspace_path(
    input: &str,
    workspace: &WorkspacePaths,
) -> Result<String, DescribeError> {
    validate_workspace_selector(input, MAX_WORKSPACE_PATH_BYTES)?;
    let candidate = Path::new(input);
    let relative = if candidate.is_absolute() {
        let root = normalize_absolute_workspace_root(workspace.root())?;
        let absolute = normalize_absolute_candidate(candidate)?;
        absolute
            .strip_prefix(&root)
            .map_err(|_| DescribeError::InvalidArguments)?
            .to_path_buf()
    } else {
        normalize_relative_candidate(candidate)?
    };
    path_to_workspace_text(&relative)
}

fn normalize_absolute_workspace_root(root: &Path) -> Result<PathBuf, DescribeError> {
    if !root.is_absolute() {
        return Err(DescribeError::InvalidBoundArguments {
            reason: "foundation workspace root must be absolute".to_owned(),
        });
    }
    let root_text = root
        .to_str()
        .ok_or_else(|| DescribeError::InvalidBoundArguments {
            reason: "foundation workspace root must be valid UTF-8".to_owned(),
        })?;
    if root_text.chars().any(char::is_control) {
        return Err(DescribeError::InvalidBoundArguments {
            reason: "foundation workspace root contains a control character".to_owned(),
        });
    }
    normalize_absolute_candidate(root).map_err(|_| DescribeError::InvalidBoundArguments {
        reason: "foundation workspace root is not lexically normalized".to_owned(),
    })
}

fn normalize_absolute_candidate(path: &Path) -> Result<PathBuf, DescribeError> {
    let mut normalized = PathBuf::from("/");
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::ParentDir | Component::Prefix(_) => {
                return Err(DescribeError::InvalidArguments);
            }
        }
    }
    Ok(normalized)
}

fn normalize_relative_candidate(path: &Path) -> Result<PathBuf, DescribeError> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(DescribeError::InvalidArguments);
            }
        }
    }
    Ok(normalized)
}

fn path_to_workspace_text(path: &Path) -> Result<String, DescribeError> {
    if path.as_os_str().is_empty() {
        return Ok(".".to_owned());
    }
    path.to_str()
        .map(str::to_owned)
        .ok_or(DescribeError::InvalidArguments)
}

fn normalize_workspace_glob(input: &str) -> Result<String, DescribeError> {
    validate_workspace_selector(input, MAX_GLOB_PATTERN_BYTES)?;
    let normalized = normalize_glob_pattern(input).map_err(|_| DescribeError::InvalidArguments)?;
    if normalized.is_empty() {
        Ok(".".to_owned())
    } else {
        Ok(normalized)
    }
}

fn to_value(value: impl serde::Serialize) -> Result<Value, ToolError> {
    serde_json::to_value(value)
        .map_err(|error| ToolError::Protocol(format!("tool output encode failed: {error}")))
}

fn bounded_lines<I, S>(lines: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let joined = lines
        .into_iter()
        .map(|line| line.as_ref().to_owned())
        .collect::<Vec<_>>()
        .join("\n");
    let result = truncate_head(&joined, TruncationOptions::default());
    render_bounded_output(&result, RetainedOutput::Head, None, &[])
}

fn bash_terminal_lines(result: &crate::tools::bash::BashExecutionResult) -> Vec<String> {
    if result.cancelled {
        vec!["Command cancelled.".to_owned()]
    } else if let Some(limit) = &result.resource_limit {
        vec![format!("Command stopped by resource limit: {limit:?}.")]
    } else {
        vec![match result.exit_code {
            Some(code) => format!("Command exited with code {code}."),
            None => "Command ended without an exit code.".to_owned(),
        }]
    }
}

fn execution_id(flow_id: &str, call_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(EXECUTION_ID_DOMAIN);
    digest.update((flow_id.len() as u64).to_be_bytes());
    digest.update(flow_id.as_bytes());
    digest.update((call_id.len() as u64).to_be_bytes());
    digest.update(call_id.as_bytes());
    let digest = digest.finalize();
    let mut id = String::with_capacity(EXECUTION_ID_PREFIX.len() + digest.len() * 2);
    id.push_str(EXECUTION_ID_PREFIX);
    for byte in digest {
        write!(&mut id, "{byte:02x}").expect("writing to String cannot fail");
    }
    id
}

fn default_read_limit() -> usize {
    DEFAULT_MAX_BYTES
}

#[derive(Clone, Copy)]
struct ReadContext {
    request_offset: u64,
    rpc_limit: usize,
    artifact: bool,
}

fn continuation_line(offset: u64) -> String {
    format!("Artifact continues; call read_file again with offset {offset}.")
}

fn artifact_source_capacity() -> usize {
    DEFAULT_MAX_BYTES
        .checked_sub(continuation_line(u64::MAX).len())
        .and_then(|capacity| capacity.checked_sub(1))
        .expect("the fixed continuation annotation fits the output envelope")
}

fn render_artifact_page(
    raw: Vec<u8>,
    artifact_eof: bool,
    read_context: Option<ReadContext>,
) -> Result<ToolOutput, ToolError> {
    let context = read_context
        .filter(|context| context.artifact)
        .ok_or_else(|| {
            ToolError::Protocol("artifact read response missing request context".to_owned())
        })?;
    let returned_bytes = raw.len();
    if returned_bytes > context.rpc_limit {
        return Err(ToolError::Protocol(
            "artifact read exceeded the requested RPC limit".to_owned(),
        ));
    }

    let valid_bytes = valid_artifact_utf8_prefix(&raw, artifact_eof, context.rpc_limit)?;
    let valid = std::str::from_utf8(&raw[..valid_bytes])
        .map_err(|_| ToolError::Protocol("artifact UTF-8 prefix validation diverged".to_owned()))?;
    let can_finish = artifact_eof
        && valid_bytes == returned_bytes
        && visible_line_count(valid) <= DEFAULT_MAX_LINES;
    let shown_bytes = if can_finish {
        valid_bytes
    } else {
        continuation_fragment_len(valid)
    };

    if !can_finish && shown_bytes == 0 {
        return Err(ToolError::Protocol(
            "artifact page cannot advance; retry with a larger read limit".to_owned(),
        ));
    }
    let shown = &valid[..shown_bytes];
    let page_eof = artifact_eof && shown_bytes == returned_bytes;
    let shown_u64 = u64::try_from(shown_bytes)
        .map_err(|_| ToolError::Protocol("artifact shown byte count overflow".to_owned()))?;
    let interval_end = context
        .request_offset
        .checked_add(shown_u64)
        .ok_or_else(|| ToolError::Protocol("artifact read next offset overflow".to_owned()))?;
    let next_offset = (!page_eof).then_some(interval_end);
    let ends_in_line_fragment = !page_eof && !shown.is_empty() && !shown.ends_with('\n');
    let rendered = match next_offset {
        None => shown.to_owned(),
        Some(offset) => {
            let mut rendered = String::with_capacity(
                shown
                    .len()
                    .checked_add(1)
                    .and_then(|size| size.checked_add(continuation_line(offset).len()))
                    .ok_or_else(|| {
                        ToolError::Protocol("artifact page render length overflow".to_owned())
                    })?,
            );
            rendered.push_str(shown);
            if !shown.ends_with('\n') {
                rendered.push('\n');
            }
            rendered.push_str(&continuation_line(offset));
            rendered
        }
    };
    if rendered.len() > DEFAULT_MAX_BYTES || visible_line_count(&rendered) > DEFAULT_MAX_LINES {
        return Err(ToolError::Protocol(
            "artifact page exceeded the model-visible envelope".to_owned(),
        ));
    }

    Ok(text_output(
        rendered,
        json!({
            "request_offset": context.request_offset,
            "returned_bytes": returned_bytes,
            "shown_bytes": shown_bytes,
            "next_offset": next_offset,
            "artifact_eof": artifact_eof,
            "page_eof": page_eof,
            "ends_in_line_fragment": ends_in_line_fragment,
        }),
    ))
}

fn valid_artifact_utf8_prefix(
    raw: &[u8],
    artifact_eof: bool,
    rpc_limit: usize,
) -> Result<usize, ToolError> {
    match std::str::from_utf8(raw) {
        Ok(_) => Ok(raw.len()),
        Err(error) if error.error_len().is_some() => Err(ToolError::Protocol(
            "artifact read contained invalid UTF-8".to_owned(),
        )),
        Err(_) if artifact_eof => Err(ToolError::Protocol(
            "artifact ended with incomplete UTF-8".to_owned(),
        )),
        Err(error) if error.valid_up_to() > 0 => Ok(error.valid_up_to()),
        Err(_) if utf8_scalar_width(raw.first().copied()) > raw.len() && raw.len() == rpc_limit => {
            Err(ToolError::Protocol(
                "artifact read limit is too small for the next UTF-8 scalar; retry with a larger limit"
                    .to_owned(),
            ))
        }
        Err(_) => Err(ToolError::Protocol(
            "artifact read began inside or with invalid UTF-8".to_owned(),
        )),
    }
}

fn utf8_scalar_width(first: Option<u8>) -> usize {
    match first {
        Some(0x00..=0x7f) => 1,
        Some(0xc2..=0xdf) => 2,
        Some(0xe0..=0xef) => 3,
        Some(0xf0..=0xf4) => 4,
        _ => 0,
    }
}

fn continuation_fragment_len(valid: &str) -> usize {
    let mut shown_bytes = 0;
    let mut newlines = 0;
    for (index, character) in valid.char_indices() {
        let end = index + character.len_utf8();
        newlines += usize::from(character == '\n');
        let source_lines = newlines + usize::from(character != '\n');
        if source_lines + 1 > DEFAULT_MAX_LINES {
            break;
        }
        shown_bytes = end;
    }
    shown_bytes
}

fn visible_line_count(value: &str) -> usize {
    value.lines().count()
}

fn deserialize_read_limit<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let limit = usize::deserialize(deserializer)?;
    if !(1..=DEFAULT_MAX_BYTES).contains(&limit) {
        return Err(serde::de::Error::custom(
            "read limit must be between 1 and 51200 bytes",
        ));
    }
    Ok(limit)
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct WorkspaceReadFileArgs {
    /// A workspace path. `artifact://` handles are not accepted.
    #[schemars(length(min = 1, max = 4096))]
    path: String,
    #[serde(default)]
    offset: u64,
    #[serde(default = "default_read_limit")]
    #[serde(deserialize_with = "deserialize_read_limit")]
    #[schemars(range(min = 1, max = 51200))]
    limit: usize,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ReadFileArgs {
    #[schemars(length(min = 1))]
    path: String,
    #[serde(default)]
    offset: u64,
    #[serde(default = "default_read_limit")]
    #[serde(deserialize_with = "deserialize_read_limit")]
    #[schemars(range(min = 1, max = 51200))]
    limit: usize,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct WriteFileArgs {
    #[schemars(length(min = 1))]
    path: String,
    content: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EditFileArgs {
    #[schemars(length(min = 1))]
    path: String,
    #[schemars(length(min = 1))]
    old_string: String,
    new_string: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PathArgs {
    /// A workspace path. Artifact handles are not accepted.
    #[schemars(length(min = 1, max = 4096))]
    path: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GlobArgs {
    /// A workspace-relative glob pattern. Artifact handles are not accepted.
    #[schemars(length(min = 1, max = 4096))]
    pattern: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GrepArgs {
    #[schemars(length(min = 1))]
    path: String,
    #[schemars(length(min = 1))]
    pattern: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct WorkspaceGrepArgs {
    /// A workspace path. `artifact://` handles are not accepted.
    #[schemars(length(min = 1, max = 4096))]
    path: String,
    #[schemars(length(min = 1, max = 16384))]
    pattern: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BoundReadFileArgs {
    path: String,
    offset: u64,
    limit: usize,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BoundPathArgs {
    path: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BoundGlobArgs {
    pattern: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BoundGrepArgs {
    path: String,
    pattern: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BashArgs {
    #[schemars(length(min = 1))]
    command: String,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use chrono::Utc;
    use serde_json::json;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixListener,
        time::timeout,
    };
    use uuid::Uuid;

    use super::*;

    const PAID: &str = "0198f0f4-9b72-7000-8000-000000000001";
    const OTHER_PAID: &str = "0198f0f4-9b72-7000-8000-000000000002";
    use crate::runtime::contracts::RpcIdentity;
    use crate::tools::{
        ResourceLimit, WorkspacePaths,
        bash::BashExecutionResult,
        executor::{RpcFrame, RpcRequest},
        fs::GrepMatch,
        truncate::{TruncatedBy, TruncationResult},
    };
    use crate::{
        approval::{
            authority::PolicyDecisionRecord,
            route_broker::{PendingApprovalRequest, provider_review_inputs_for_test},
            route_policy::{ElevatedPolicyEvaluation, RoutePolicy},
            route_reviewer::{EscalationReviewRequest, escalation_provider_wire_bodies_for_test},
        },
        provider::types::{ToolCall, ToolInvocationRoute},
        store::Redactor,
    };

    #[derive(Default)]
    struct FakeInvoker {
        operations: Mutex<Vec<ExecutorOperation>>,
        responses: Mutex<VecDeque<Result<ExecutorResponse, ToolError>>>,
        updates: Mutex<VecDeque<Value>>,
        raw_calls: AtomicUsize,
        authorized_calls: AtomicUsize,
    }

    impl FakeInvoker {
        async fn respond(
            &self,
            operation: ExecutorOperation,
            cancel: CancellationToken,
            on_update: Arc<dyn Fn(Value) + Send + Sync>,
        ) -> Result<ExecutorResponse, ToolError> {
            self.operations.lock().unwrap().push(operation);
            while let Some(update) = self.updates.lock().unwrap().pop_front() {
                on_update(update);
            }
            if self.responses.lock().unwrap().is_empty() {
                cancel.cancelled().await;
                return Ok(ExecutorResponse::Bash {
                    result: bash_result("", None, None, true, None),
                });
            }
            self.responses.lock().unwrap().pop_front().unwrap()
        }
    }

    #[async_trait]
    impl ExecutorInvoker for FakeInvoker {
        async fn execute(
            &self,
            operation: ExecutorOperation,
            cancel: CancellationToken,
            on_update: Arc<dyn Fn(Value) + Send + Sync>,
        ) -> Result<ExecutorResponse, ToolError> {
            self.raw_calls.fetch_add(1, Ordering::Relaxed);
            self.respond(operation, cancel, on_update).await
        }

        async fn execute_authorized(
            &self,
            operation: ExecutorOperation,
            permit: CommittedExecutionPermit,
            cancel: CancellationToken,
            on_update: Arc<dyn Fn(Value) + Send + Sync>,
        ) -> Result<CommittedEffectReceipt<ExecutorResponse>, ToolError> {
            self.authorized_calls.fetch_add(1, Ordering::Relaxed);
            permit
                .begin_executor_effect()
                .complete(|permit| {
                    drop(permit);
                    self.respond(operation, cancel, on_update)
                })
                .await
        }
    }

    struct ArtifactSourceInvoker {
        path: String,
        source: Vec<u8>,
        reads: Mutex<Vec<(String, u64, usize)>>,
    }

    impl ArtifactSourceInvoker {
        fn new(path: &str, source: Vec<u8>) -> Self {
            Self {
                path: path.to_owned(),
                source,
                reads: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ExecutorInvoker for ArtifactSourceInvoker {
        async fn execute(
            &self,
            operation: ExecutorOperation,
            _cancel: CancellationToken,
            _on_update: Arc<dyn Fn(Value) + Send + Sync>,
        ) -> Result<ExecutorResponse, ToolError> {
            let ExecutorOperation::ReadFile {
                path,
                offset,
                limit,
                ..
            } = operation
            else {
                return Err(ToolError::Protocol(
                    "artifact source fake received a non-read operation".to_owned(),
                ));
            };
            self.reads
                .lock()
                .unwrap()
                .push((path.clone(), offset, limit));
            if path != self.path || !path.starts_with("artifact://") {
                return Err(ToolError::Protocol(
                    "artifact source fake received the wrong path".to_owned(),
                ));
            }
            let start = usize::try_from(offset).map_err(|_| {
                ToolError::Protocol("artifact source fake offset overflow".to_owned())
            })?;
            if start > self.source.len() {
                return Err(ToolError::Protocol(
                    "artifact source fake offset exceeded source".to_owned(),
                ));
            }
            let end = start
                .checked_add(limit)
                .map(|end| end.min(self.source.len()))
                .ok_or_else(|| {
                    ToolError::Protocol("artifact source fake limit overflow".to_owned())
                })?;
            Ok(ExecutorResponse::Artifact {
                response: ArtifactResponse::Read {
                    content: self.source[start..end].to_vec(),
                    eof: end == self.source.len(),
                },
            })
        }

        async fn execute_authorized(
            &self,
            operation: ExecutorOperation,
            permit: CommittedExecutionPermit,
            cancel: CancellationToken,
            on_update: Arc<dyn Fn(Value) + Send + Sync>,
        ) -> Result<CommittedEffectReceipt<ExecutorResponse>, ToolError> {
            permit
                .begin_executor_effect()
                .complete(|permit| {
                    drop(permit);
                    self.execute(operation, cancel, on_update)
                })
                .await
        }
    }

    fn validated(value: Value) -> ValidatedToolArguments {
        serde_json::from_value(value).unwrap()
    }

    fn tool_call(id: &str, name: &str, arguments: Value) -> ToolCall {
        ToolCall {
            id: id.to_owned(),
            name: name.to_owned(),
            route: ToolInvocationRoute::Normal,
            arguments: validated(arguments),
        }
    }

    fn text(output: &ToolOutput) -> &str {
        let crate::provider::types::UserContent::Text { text } = &output.content[0] else {
            panic!("tool should produce text output");
        };
        text
    }

    fn continuation_offset(output: &ToolOutput) -> u64 {
        let marker = "call read_file again with offset ";
        let suffix = text(output)
            .split_once(marker)
            .expect("continuation marker")
            .1;
        suffix
            .split_once('.')
            .expect("continuation terminator")
            .0
            .parse()
            .expect("decimal continuation offset")
    }

    fn artifact_context(request_offset: u64, rpc_limit: usize) -> Option<ReadContext> {
        Some(ReadContext {
            request_offset,
            rpc_limit,
            artifact: true,
        })
    }

    async fn run(
        registry: &ToolRegistry,
        name: &str,
        flow_id: &str,
        call_id: &str,
        args: Value,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<ToolOutput, ToolError> {
        let args = validated(args);
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        registry
            .get(name)
            .unwrap()
            .execute(ToolCtx {
                flow_id,
                call_id,
                args: &args,
                cancel,
                on_update,
                workspace: &workspace,
            })
            .await
    }

    fn truncation(content: &str) -> TruncationResult {
        TruncationResult {
            content: content.to_owned(),
            truncated: false,
            truncated_by: None,
            total_lines: usize::from(!content.is_empty()),
            total_bytes: content.len(),
            output_lines: usize::from(!content.is_empty()),
            output_bytes: content.len(),
            last_line_partial: false,
            first_line_exceeds_limit: false,
            max_lines: 2_000,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }

    fn bash_result(
        output: &str,
        artifact_handle: Option<&str>,
        exit_code: Option<i32>,
        cancelled: bool,
        resource_limit: Option<ResourceLimit>,
    ) -> BashExecutionResult {
        BashExecutionResult {
            output: output.to_owned(),
            truncation: truncation(output),
            artifact_handle: artifact_handle.map(str::to_owned),
            observed_bytes: output.len() as u64,
            exit_code,
            cancelled,
            resource_limit,
        }
    }

    #[tokio::test]
    async fn maps_every_frozen_operation_and_success_result() {
        let fake = Arc::new(FakeInvoker::default());
        fake.responses.lock().unwrap().extend([
            Ok(ExecutorResponse::ReadFile {
                result: truncation("note"),
            }),
            Ok(ExecutorResponse::Written {}),
            Ok(ExecutorResponse::Edited {}),
            Ok(ExecutorResponse::Removed {}),
            Ok(ExecutorResponse::Listed {
                entries: vec!["a.txt".to_owned()],
            }),
            Ok(ExecutorResponse::Globbed {
                paths: vec!["src/a.rs".to_owned()],
            }),
            Ok(ExecutorResponse::Grepped {
                matches: vec![GrepMatch {
                    path: "src/a.rs".to_owned(),
                    line_number: 3,
                    line: "needle".to_owned(),
                    line_truncated: false,
                }],
            }),
            Ok(ExecutorResponse::Bash {
                result: bash_result(
                    "done",
                    Some("artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/exec-log"),
                    Some(0),
                    false,
                    None,
                ),
            }),
        ]);
        let registry = registry_from_invoker(fake.clone()).unwrap();
        let noop: Arc<dyn Fn(Value) + Send + Sync> = Arc::new(|_| {});
        let cases = [
            (
                "read_file",
                json!({"path":"note.txt","offset":2,"limit":51200}),
            ),
            ("write_file", json!({"path":"note.txt","content":"new"})),
            (
                "edit_file",
                json!({"path":"note.txt","old_string":"old","new_string":"new"}),
            ),
            ("delete", json!({"path":"old.txt"})),
            ("list_dir", json!({"path":"."})),
            ("glob", json!({"pattern":"**/*.rs"})),
            ("grep", json!({"path":"src","pattern":"needle"})),
            ("bash", json!({"command":"printf done"})),
        ];
        let mut outputs = Vec::new();
        for (index, (name, args)) in cases.into_iter().enumerate() {
            outputs.push(
                run(
                    &registry,
                    name,
                    "flow-1",
                    &format!("call-{index}"),
                    args,
                    CancellationToken::new(),
                    noop.clone(),
                )
                .await
                .unwrap(),
            );
        }

        let operations = fake.operations.lock().unwrap();
        assert!(
            matches!(&operations[0], ExecutorOperation::ReadFile { path, offset: 2, limit: 51200, .. } if path == "note.txt")
        );
        assert!(
            matches!(&operations[1], ExecutorOperation::WriteFile { path, content, .. } if path == "note.txt" && content == "new")
        );
        assert!(
            matches!(&operations[2], ExecutorOperation::EditFile { old_string, new_string, .. } if old_string == "old" && new_string == "new")
        );
        assert!(
            matches!(&operations[3], ExecutorOperation::RemoveFile { path, .. } if path == "old.txt")
        );
        assert!(matches!(&operations[4], ExecutorOperation::ListDir { path, .. } if path == "."));
        assert!(
            matches!(&operations[5], ExecutorOperation::Glob { pattern, .. } if pattern == "**/*.rs")
        );
        assert!(
            matches!(&operations[6], ExecutorOperation::Grep { path, pattern, .. } if path == "src" && pattern == "needle")
        );
        assert!(
            matches!(&operations[7], ExecutorOperation::Bash { command, .. } if command == "printf done")
        );
        assert_eq!(
            outputs[0].content[0],
            crate::provider::types::UserContent::Text {
                text: "note".to_owned()
            }
        );
        assert_eq!(outputs[4].details["count"], 1);
        assert_eq!(outputs[6].details["matches"][0]["line_number"], 3);
        assert_eq!(
            outputs[7].details["artifact_handle"],
            "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/exec-log"
        );
        assert!(
            matches!(outputs[7].content[0], crate::provider::types::UserContent::Text { ref text } if text.contains("Command exited with code 0."))
        );
    }

    #[tokio::test]
    async fn supports_artifact_read_and_grep_outputs() {
        let fake = Arc::new(FakeInvoker::default());
        fake.responses.lock().unwrap().extend([
            Ok(ExecutorResponse::Artifact {
                response: ArtifactResponse::Read {
                    content: b"artifact text".to_vec(),
                    eof: true,
                },
            }),
            Ok(ExecutorResponse::Artifact {
                response: ArtifactResponse::Grep {
                    matches: vec![super::super::ArtifactGrepMatch {
                        line_number: 4,
                        line: "found".to_owned(),
                        line_truncated: false,
                    }],
                },
            }),
        ]);
        let registry = registry_from_invoker(fake.clone()).unwrap();
        let noop: Arc<dyn Fn(Value) + Send + Sync> = Arc::new(|_| {});
        let read = run(
            &registry,
            "read_file",
            "f",
            "r",
            json!({"path":"artifact://0198f0f4-9b72-7000-8000-000000000001/attachments/a","limit":100}),
            CancellationToken::new(),
            noop.clone(),
        )
        .await
        .unwrap();
        let grep = run(
            &registry,
            "grep",
            "f",
            "g",
            json!({"path":"artifact://0198f0f4-9b72-7000-8000-000000000001/attachments/a","pattern":"found"}),
            CancellationToken::new(),
            noop,
        )
        .await
        .unwrap();
        assert_eq!(
            read.details,
            json!({
                "request_offset":0,
                "returned_bytes":13,
                "shown_bytes":13,
                "next_offset":null,
                "artifact_eof":true,
                "page_eof":true,
                "ends_in_line_fragment":false,
            })
        );
        assert_eq!(grep.details["source"], "artifact");
        assert_eq!(grep.details["matches"][0]["line_number"], 4);
        assert!(matches!(
            &fake.operations.lock().unwrap()[0],
            ExecutorOperation::ReadFile { limit: 100, .. }
        ));
    }

    #[tokio::test]
    async fn artifact_read_reports_exact_non_final_and_final_page_state() {
        let fake = Arc::new(FakeInvoker::default());
        fake.responses.lock().unwrap().extend([
            Ok(ExecutorResponse::Artifact {
                response: ArtifactResponse::Read {
                    content: b"abc".to_vec(),
                    eof: false,
                },
            }),
            Ok(ExecutorResponse::Artifact {
                response: ArtifactResponse::Read {
                    content: b"done".to_vec(),
                    eof: true,
                },
            }),
        ]);
        let registry = registry_from_invoker(fake.clone()).unwrap();
        let first = run(
            &registry,
            "read_file",
            "f",
            "chunk-1",
            json!({"path":"artifact://0198f0f4-9b72-7000-8000-000000000001/attachments/a","offset":7,"limit":100}),
            CancellationToken::new(),
            Arc::new(|_| {}),
        )
        .await
        .unwrap();
        let second = run(
            &registry,
            "read_file",
            "f",
            "chunk-2",
            json!({"path":"artifact://0198f0f4-9b72-7000-8000-000000000001/attachments/a","offset":10,"limit":100}),
            CancellationToken::new(),
            Arc::new(|_| {}),
        )
        .await
        .unwrap();
        let crate::provider::types::UserContent::Text { text: first_text } = &first.content[0]
        else {
            panic!("artifact read should produce text output");
        };
        let crate::provider::types::UserContent::Text { text: second_text } = &second.content[0]
        else {
            panic!("artifact read should produce text output");
        };
        assert!(first_text.contains("call read_file again with offset 10"));
        assert!(!second_text.contains("call read_file again"));
        assert_eq!(
            first.details,
            json!({
                "request_offset":7,
                "returned_bytes":3,
                "shown_bytes":3,
                "next_offset":10,
                "artifact_eof":false,
                "page_eof":false,
                "ends_in_line_fragment":true,
            })
        );
        assert_eq!(
            second.details,
            json!({
                "request_offset":10,
                "returned_bytes":4,
                "shown_bytes":4,
                "next_offset":null,
                "artifact_eof":true,
                "page_eof":true,
                "ends_in_line_fragment":false,
            })
        );
    }

    #[tokio::test]
    async fn reconstructs_over_100_kib_single_line_across_exact_pages() {
        let path = "artifact://0198f0f4-9b72-7000-8000-000000000001/attachments/a";
        let source = "x".repeat(110 * 1024);
        let capacity = artifact_source_capacity();
        let fake = Arc::new(ArtifactSourceInvoker::new(path, source.as_bytes().to_vec()));
        let registry = registry_from_invoker(fake.clone()).unwrap();
        let mut offset = 0usize;
        let mut reconstructed = Vec::new();
        let mut requested_offsets = Vec::new();
        let mut page = 0;
        loop {
            requested_offsets.push(offset as u64);
            let output = run(
                &registry,
                "read_file",
                "f",
                &format!("single-line-{page}"),
                json!({"path":path,"offset":offset}),
                CancellationToken::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
            let shown = output.details["shown_bytes"].as_u64().unwrap() as usize;
            let returned = output.details["returned_bytes"].as_u64().unwrap() as usize;
            assert_eq!(output.details["request_offset"], offset);
            assert!(returned <= capacity);
            let visible_fragment = &text(&output).as_bytes()[..shown];
            assert_eq!(visible_fragment, &source.as_bytes()[offset..offset + shown]);
            reconstructed.extend_from_slice(visible_fragment);
            assert!(text(&output).len() <= DEFAULT_MAX_BYTES);
            assert!(text(&output).lines().count() <= DEFAULT_MAX_LINES);
            if output.details["page_eof"] == true {
                break;
            }
            let next = output.details["next_offset"].as_u64().unwrap() as usize;
            assert_eq!(next, offset + shown);
            assert!(next > offset);
            offset = next;
            page += 1;
        }
        assert_eq!(reconstructed, source.as_bytes());
        let reads = fake.reads.lock().unwrap();
        assert_eq!(reads.len(), source.len().div_ceil(capacity));
        assert_eq!(
            reads
                .iter()
                .map(|(_, offset, _)| *offset)
                .collect::<Vec<_>>(),
            requested_offsets
        );
        assert!(
            reads
                .iter()
                .all(|(actual_path, _, limit)| actual_path == path && *limit == capacity)
        );
    }

    #[test]
    fn utf8_scalar_splits_withhold_only_the_incomplete_tail() {
        for scalar in ["é", "界", "🙂"] {
            for split in 1..scalar.len() {
                let mut first_raw = b"a".to_vec();
                first_raw.extend_from_slice(&scalar.as_bytes()[..split]);
                let first = render_artifact_page(
                    first_raw.clone(),
                    false,
                    artifact_context(0, first_raw.len()),
                )
                .unwrap();
                assert_eq!(first.details["returned_bytes"], first_raw.len());
                assert_eq!(first.details["shown_bytes"], 1);
                assert_eq!(first.details["next_offset"], 1);
                assert!(text(&first).starts_with('a'));

                let second_raw = format!("{scalar}z").into_bytes();
                let second = render_artifact_page(
                    second_raw.clone(),
                    true,
                    artifact_context(1, second_raw.len()),
                )
                .unwrap();
                assert_eq!(second.details["shown_bytes"], second_raw.len());
                assert_eq!(format!("a{}", text(&second)), format!("a{scalar}z"));
                assert_eq!(second.details["page_eof"], true);
            }
        }
    }

    #[test]
    fn invalid_utf8_and_non_scalar_offsets_fail_closed() {
        for (raw, artifact_eof, expected) in [
            (vec![b'a', 0xff, b'b'], false, "invalid UTF-8"),
            (vec![b'a', 0xe2, 0x82], true, "incomplete UTF-8"),
            (vec![0x82, b'a'], false, "invalid UTF-8"),
        ] {
            let error =
                render_artifact_page(raw.clone(), artifact_eof, artifact_context(9, raw.len()))
                    .unwrap_err();
            assert!(matches!(error, ToolError::Protocol(message) if message.contains(expected)));
        }

        for (raw, width) in [
            (vec![0xc3], 2),
            (vec![0xe7, 0x95], 3),
            (vec![0xf0, 0x9f, 0x99], 4),
        ] {
            let error = render_artifact_page(raw.clone(), false, artifact_context(0, raw.len()))
                .unwrap_err();
            assert!(
                matches!(error, ToolError::Protocol(message) if message.contains("retry with a larger limit")),
                "width {width} should require a larger page"
            );
        }

        let leading_incomplete =
            render_artifact_page(vec![0xe2, 0x82], false, artifact_context(9, 3)).unwrap_err();
        assert!(
            matches!(leading_incomplete, ToolError::Protocol(message) if message.contains("began inside or with invalid UTF-8"))
        );
    }

    #[tokio::test]
    async fn artifact_eof_with_more_than_2000_lines_continues_without_loss() {
        let path = "artifact://0198f0f4-9b72-7000-8000-000000000001/attachments/lines";
        let source = "x\n".repeat(2_100);
        let capacity = artifact_source_capacity();
        let fake = Arc::new(ArtifactSourceInvoker::new(path, source.as_bytes().to_vec()));
        let registry = registry_from_invoker(fake.clone()).unwrap();
        let first = run(
            &registry,
            "read_file",
            "f",
            "lines-1",
            json!({"path":path}),
            CancellationToken::new(),
            Arc::new(|_| {}),
        )
        .await
        .unwrap();
        assert_eq!(first.details["artifact_eof"], true);
        assert_eq!(first.details["page_eof"], false);
        let shown = first.details["shown_bytes"].as_u64().unwrap() as usize;
        let next = first.details["next_offset"].as_u64().unwrap() as usize;
        assert_eq!(next, shown);
        assert!(next > 0 && next < source.len());
        assert_eq!(text(&first).lines().count(), DEFAULT_MAX_LINES);
        assert_eq!(
            &text(&first).as_bytes()[..shown],
            &source.as_bytes()[..shown]
        );

        let second = run(
            &registry,
            "read_file",
            "f",
            "lines-2",
            json!({"path":path,"offset":next}),
            CancellationToken::new(),
            Arc::new(|_| {}),
        )
        .await
        .unwrap();
        assert_eq!(second.details["page_eof"], true);
        let mut reconstructed = text(&first).as_bytes()[..shown].to_vec();
        reconstructed.extend_from_slice(text(&second).as_bytes());
        assert_eq!(reconstructed, source.as_bytes());
        assert_eq!(
            *fake.reads.lock().unwrap(),
            vec![
                (path.to_owned(), 0, capacity),
                (path.to_owned(), next as u64, capacity),
            ]
        );
    }

    #[tokio::test]
    async fn artifact_rpc_is_precapped_and_near_envelope_pages_progress() {
        let capacity = artifact_source_capacity();
        let fake = Arc::new(FakeInvoker::default());
        fake.responses
            .lock()
            .unwrap()
            .push_back(Ok(ExecutorResponse::Artifact {
                response: ArtifactResponse::Read {
                    content: vec![b'x'; capacity],
                    eof: false,
                },
            }));
        let registry = registry_from_invoker(fake.clone()).unwrap();
        let output = run(
            &registry,
            "read_file",
            "f",
            "near-envelope",
            json!({"path":"artifact://0198f0f4-9b72-7000-8000-000000000001/attachments/a"}),
            CancellationToken::new(),
            Arc::new(|_| {}),
        )
        .await
        .unwrap();
        assert_eq!(output.details["shown_bytes"], capacity);
        assert_eq!(output.details["next_offset"], capacity);
        assert!(text(&output).len() <= DEFAULT_MAX_BYTES);
        assert!(continuation_offset(&output) > 0);
        assert!(matches!(
            &fake.operations.lock().unwrap()[0],
            ExecutorOperation::ReadFile { limit, .. } if *limit == capacity
        ));
    }

    #[test]
    fn maximum_cursor_fits_and_overflow_fails_closed() {
        let capacity = artifact_source_capacity();
        let start = u64::MAX - u64::try_from(capacity).unwrap();
        let maximum = render_artifact_page(
            vec![b'x'; capacity],
            false,
            artifact_context(start, capacity),
        )
        .unwrap();
        assert_eq!(continuation_offset(&maximum), u64::MAX);
        assert_eq!(maximum.details["next_offset"], u64::MAX);
        assert_eq!(text(&maximum).len(), DEFAULT_MAX_BYTES);

        let error =
            render_artifact_page(b"x".to_vec(), false, artifact_context(u64::MAX, 1)).unwrap_err();
        assert!(
            matches!(error, ToolError::Protocol(message) if message.contains("next offset overflow"))
        );

        let final_error =
            render_artifact_page(b"x".to_vec(), true, artifact_context(u64::MAX, 1)).unwrap_err();
        assert!(
            matches!(final_error, ToolError::Protocol(message) if message.contains("next offset overflow"))
        );
    }

    #[test]
    fn artifact_response_cannot_exceed_requested_rpc_limit() {
        let error =
            render_artifact_page(b"ab".to_vec(), false, artifact_context(0, 1)).unwrap_err();
        assert!(
            matches!(error, ToolError::Protocol(message) if message.contains("requested RPC limit"))
        );
    }

    #[tokio::test]
    async fn out_of_range_read_limits_are_rejected_before_rpc() {
        let fake = Arc::new(FakeInvoker::default());
        let registry = registry_from_invoker(fake.clone()).unwrap();
        for limit in [0, DEFAULT_MAX_BYTES + 1] {
            let error = run(
                &registry,
                "read_file",
                "f",
                "invalid-limit",
                json!({"path":"artifact://0198f0f4-9b72-7000-8000-000000000001/attachments/a","limit":limit}),
                CancellationToken::new(),
                Arc::new(|_| {}),
            )
            .await
            .unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        assert!(fake.operations.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn wrong_typed_variant_is_protocol_error_without_output() {
        let fake = Arc::new(FakeInvoker::default());
        fake.responses
            .lock()
            .unwrap()
            .push_back(Ok(ExecutorResponse::Written {}));
        let registry = registry_from_invoker(fake).unwrap();
        let error = run(
            &registry,
            "read_file",
            "f",
            "c",
            json!({"path":"x"}),
            CancellationToken::new(),
            Arc::new(|_| {}),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::Protocol(_)));
    }

    #[test]
    fn execution_identity_is_stable_distinct_and_bounded() {
        let first = execution_id("flow", "call");
        assert_eq!(first, execution_id("flow", "call"));
        assert_ne!(first, execution_id("flow-2", "call"));
        assert_ne!(first, execution_id("flow", "call-2"));
        assert_ne!(execution_id("a", "bc"), execution_id("ab", "c"));
        assert_eq!(first.len(), 69);
        assert!(first.len() <= 128);
        assert_eq!(execution_id(&"x".repeat(1_000_000), "call").len(), 69);
    }

    #[tokio::test]
    async fn cancellation_and_ordered_updates_are_forwarded() {
        let fake = Arc::new(FakeInvoker::default());
        fake.updates
            .lock()
            .unwrap()
            .extend([json!({"output":"first"}), json!({"output":"second"})]);
        let registry = registry_from_invoker(fake).unwrap();
        let updates = Arc::new(Mutex::new(Vec::new()));
        let observed = updates.clone();
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        let future = run(
            &registry,
            "bash",
            "flow",
            "call",
            json!({"command":"sleep 30"}),
            cancel,
            Arc::new(move |value| observed.lock().unwrap().push(value)),
        );
        tokio::pin!(future);
        tokio::task::yield_now().await;
        trigger.cancel();
        let output = timeout(Duration::from_secs(1), future)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            *updates.lock().unwrap(),
            vec![json!({"output":"first"}), json!({"output":"second"})]
        );
        assert_eq!(output.details["cancelled"], true);
        assert!(
            matches!(output.content[0], crate::provider::types::UserContent::Text { ref text } if text.contains("Command cancelled."))
        );
    }

    #[tokio::test]
    async fn forged_remote_identity_remains_indeterminate() {
        let root = std::env::temp_dir().join(format!("sumi-remote-adapter-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let socket = root.join("executor.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let service = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut line = String::new();
            BufReader::new(read).read_line(&mut line).await.unwrap();
            let request: RpcRequest<ExecutorOperation> =
                serde_json::from_str(line.trim_end()).unwrap();
            let forged = RpcFrame::Terminal {
                personality_agent_id: PAID.parse().unwrap(),
                generation: request.generation,
                nonce: "stale-nonce".to_owned(),
                request_id: request.request_id,
                result: Ok(ExecutorResponse::ReadFile {
                    result: truncation("forged"),
                }),
            };
            write
                .write_all(&serde_json::to_vec(&forged).unwrap())
                .await
                .unwrap();
            write.write_all(b"\n").await.unwrap();
        });
        let client = Arc::new(ExecutorClient::new(
            socket,
            RpcIdentity::from_wire(PAID, 7, "current-nonce").unwrap(),
        ));
        let registry = remote_executor_registry(client).unwrap();
        let error = run(
            &registry,
            "read_file",
            "flow",
            "call",
            json!({"path":"x"}),
            CancellationToken::new(),
            Arc::new(|_| {}),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::RpcIndeterminate(_)));
        service.await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn production_registry_binds_the_clients_complete_rpc_identity() {
        let identity = RpcIdentity::from_wire(PAID, 7, "current-nonce").unwrap();
        let registry = remote_executor_registry(Arc::new(ExecutorClient::new(
            "/tmp/sumi-unused-executor.sock",
            identity.clone(),
        )))
        .unwrap();

        registry
            .validate_executor_identity(&identity)
            .expect("exact identity");
        let wrong_paid = RpcIdentity::from_wire(OTHER_PAID, 7, "current-nonce").unwrap();
        assert!(registry.validate_executor_identity(&wrong_paid).is_err());
        let wrong_nonce = RpcIdentity::from_wire(PAID, 7, "stale-nonce").unwrap();
        assert!(registry.validate_executor_identity(&wrong_nonce).is_err());

        let fixture_registry = registry_from_invoker(Arc::new(FakeInvoker::default())).unwrap();
        assert!(
            fixture_registry
                .validate_executor_identity(&identity)
                .is_err(),
            "an unbound fixture registry cannot satisfy production validation"
        );

        assert_eq!(registry.len(), 4);
        assert_eq!(
            registry
                .definitions()
                .into_iter()
                .map(|definition| definition.name)
                .collect::<Vec<_>>(),
            vec!["glob", "grep", "list_dir", "read_file"]
        );
        let definition = registry.get("read_file").unwrap().def();
        assert_eq!(
            definition.description,
            "Read UTF-8 text from a workspace path. Artifact handles are not accepted."
        );
        assert_eq!(
            definition.parameters["properties"]["path"]["description"],
            "A workspace path. `artifact://` handles are not accepted."
        );
        for forbidden in ["bash", "write_file", "edit_file", "delete"] {
            assert!(
                registry.get(forbidden).is_none(),
                "{forbidden} leaked into the production registry"
            );
        }
        for allowed in ["read_file", "list_dir", "glob", "grep"] {
            assert_eq!(registry.get(allowed).unwrap().risk(), ToolRisk::ReadOnly);
        }
        let grep = registry.get("grep").unwrap().def();
        assert_eq!(
            grep.description,
            "Search a workspace path with a regular expression. Artifact handles are not accepted."
        );
        assert_eq!(
            grep.parameters["properties"]["path"]["description"],
            "A workspace path. `artifact://` handles are not accepted."
        );
        let list_dir = registry.get("list_dir").unwrap().def();
        assert_eq!(
            list_dir.parameters["properties"]["path"]["description"],
            "A workspace path. Artifact handles are not accepted."
        );
        let glob = registry.get("glob").unwrap().def();
        assert_eq!(
            glob.parameters["properties"]["pattern"]["description"],
            "A workspace-relative glob pattern. Artifact handles are not accepted."
        );
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        for (name, arguments) in [
            ("read_file", json!({"path":"src/lib.rs"})),
            ("list_dir", json!({"path":"src"})),
            ("glob", json!({"pattern":"src/**/*.rs"})),
            ("grep", json!({"path":"src","pattern":"mod"})),
        ] {
            let sealed = registry
                .bind(
                    &tool_call("production-binder", name, arguments),
                    "flow-production-binder",
                    &workspace,
                )
                .await
                .unwrap();
            assert_eq!(
                sealed.invocation().adapter,
                AdapterIdentity::new(BINDING_ADAPTER_ID, 1).unwrap(),
                "{name} must be bindable before its definition is exposed"
            );
        }
    }

    #[tokio::test]
    async fn production_bindings_freeze_exact_normalized_operations_without_bind_rpc() {
        let fake = Arc::new(FakeInvoker::default());
        fake.responses.lock().unwrap().extend([
            Ok(ExecutorResponse::ReadFile {
                result: truncation("bound read"),
            }),
            Ok(ExecutorResponse::Listed {
                entries: vec!["lib.rs".to_owned()],
            }),
            Ok(ExecutorResponse::Globbed {
                paths: vec!["src/lib.rs".to_owned()],
            }),
            Ok(ExecutorResponse::Grepped { matches: vec![] }),
        ]);
        let registry = bound_test_registry_from_invoker(fake.clone()).unwrap();
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        let cases = [
            (
                "read_file",
                json!({"path":"/workspace/src/./lib.rs"}),
                json!({"path":"src/lib.rs","offset":0,"limit":51200}),
            ),
            (
                "list_dir",
                json!({"path":"./src//."}),
                json!({"path":"src"}),
            ),
            (
                "glob",
                json!({"pattern":"./src/**/*.rs"}),
                json!({"pattern":"src/**/*.rs"}),
            ),
            (
                "grep",
                json!({"path":"/workspace/src","pattern":"foo|bar"}),
                json!({"path":"src","pattern":"foo|bar"}),
            ),
        ];

        for (index, (name, proposal, expected_arguments)) in cases.into_iter().enumerate() {
            let sealed = registry
                .bind(
                    &tool_call(&format!("bound-{index}"), name, proposal),
                    "flow-bound",
                    &workspace,
                )
                .await
                .unwrap();
            assert_eq!(
                fake.operations.lock().unwrap().len(),
                index,
                "binding {name} must not contact the executor"
            );
            assert_eq!(
                Value::Object(sealed.invocation().execution_arguments.as_object().clone()),
                expected_arguments
            );
            let mut expected_review = expected_arguments.as_object().unwrap().clone();
            expected_review.insert("operation".to_owned(), Value::String(name.to_owned()));
            assert_eq!(
                sealed.invocation().review_projection.as_object(),
                &expected_review
            );
            assert_eq!(
                sealed.invocation().descriptor.capability,
                CapabilityClass::Read
            );
            let (scope_kind, scope_id) = if name == "glob" {
                (
                    "glob_selector",
                    expected_arguments["pattern"].as_str().unwrap(),
                )
            } else {
                ("path", expected_arguments["path"].as_str().unwrap())
            };
            assert_eq!(
                sealed.invocation().descriptor.resource_scopes,
                vec![ResourceScope::resource(
                    BINDING_ADAPTER_ID,
                    scope_kind,
                    scope_id
                )]
            );
            assert_eq!(
                sealed.invocation().adapter,
                AdapterIdentity::new(BINDING_ADAPTER_ID, 1).unwrap()
            );

            let authorized =
                crate::approval::authority::AuthorizedBoundInvocation::for_test(sealed);
            let outcome = registry
                .execute_bound(authorized, CancellationToken::new(), Arc::new(|_| {}))
                .await
                .unwrap();
            assert!(
                outcome.live_post_commit.is_none(),
                "foundation reads must not produce process-local post-commit work"
            );
            assert_eq!(
                fake.operations.lock().unwrap().len(),
                index + 1,
                "executing {name} must make exactly one executor call"
            );
            assert_eq!(fake.raw_calls.load(Ordering::Relaxed), 0);
            assert_eq!(
                fake.authorized_calls.load(Ordering::Relaxed),
                index + 1,
                "bound execution must use the move-only authorized invoker"
            );
        }

        let operations = fake.operations.lock().unwrap();
        assert!(matches!(
            &operations[0],
            ExecutorOperation::ReadFile { path, offset: 0, limit: 51200, .. }
                if path == "src/lib.rs"
        ));
        assert!(matches!(
            &operations[1],
            ExecutorOperation::ListDir { path, .. } if path == "src"
        ));
        assert!(matches!(
            &operations[2],
            ExecutorOperation::Glob { pattern, .. } if pattern == "src/**/*.rs"
        ));
        assert!(matches!(
            &operations[3],
            ExecutorOperation::Grep { path, pattern, .. }
                if path == "src" && pattern == "foo|bar"
        ));
    }

    #[tokio::test]
    async fn elevated_remote_bind_sends_exact_selectors_to_every_reviewer_wire() {
        const PATH_SENTINEL: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopq";
        const PATTERN_SENTINEL: &str = "remote-review-pattern-secret";
        assert_eq!(PATH_SENTINEL.chars().count(), 43);

        let registry = bound_test_registry_from_invoker(Arc::new(FakeInvoker::default())).unwrap();
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        let cases = [
            (
                "read_file",
                json!({"path": format!("read/{PATH_SENTINEL}.txt")}),
                false,
            ),
            (
                "list_dir",
                json!({"path": format!("list/{PATH_SENTINEL}")}),
                false,
            ),
            (
                "glob",
                json!({"pattern": format!("glob/**/{PATH_SENTINEL}*.txt")}),
                false,
            ),
            (
                "grep",
                json!({
                    "path": format!("grep/{PATH_SENTINEL}.txt"),
                    "pattern": PATTERN_SENTINEL,
                }),
                true,
            ),
        ];

        for (name, arguments, has_private_pattern) in cases {
            let call = ToolCall {
                id: format!("remote-elevated-{name}-secret"),
                name: name.to_owned(),
                route: ToolInvocationRoute::Elevated,
                arguments: validated(arguments),
            };
            let sealed = registry
                .bind(&call, "flow-remote-elevated-secret", &workspace)
                .await
                .unwrap_or_else(|error| panic!("bind real remote {name} adapter: {error}"));
            let bound = registry
                .validate_bound(&sealed)
                .expect("validate real remote binding");

            let exact_descriptor =
                serde_json::to_string(&bound.descriptor).expect("exact descriptor");
            assert!(
                exact_descriptor.contains(PATH_SENTINEL),
                "{name} lost exact selector"
            );
            for exact in [
                serde_json::to_string(&bound.review_projection).expect("exact Human projection"),
                serde_json::to_string(&bound.execution_arguments)
                    .expect("exact execution arguments"),
            ] {
                assert!(exact.contains(PATH_SENTINEL), "{name} lost exact selector");
                if has_private_pattern {
                    assert!(exact.contains(PATTERN_SENTINEL), "{name} lost exact regex");
                }
            }

            let provider_identity = serde_json::to_string(&bound.provider_review_identity)
                .expect("closed provider identity");
            let provider_descriptor = serde_json::to_string(&bound.provider_review_descriptor)
                .expect("provider-safe descriptor");
            let provider_projection = serde_json::to_string(&bound.provider_review_projection)
                .expect("provider-safe projection");
            for provider_safe in [
                provider_identity,
                provider_descriptor.clone(),
                provider_projection,
            ] {
                assert_eq!(provider_safe.matches(PATH_SENTINEL).count(), 0);
                assert_eq!(provider_safe.matches(PATTERN_SENTINEL).count(), 0);
                assert!(!provider_safe.contains("sumi.foundation.workspace"));
            }
            assert!(provider_descriptor.contains("foundation_workspace"));

            let human_request = PendingApprovalRequest::from_bound(
                format!("approval-remote-elevated-{name}-secret"),
                ToolInvocationRoute::Elevated,
                bound,
                &Redactor::v1(),
            )
            .expect("exact local Human request")
            .public_request();
            let human_encoded = serde_json::to_string(&human_request).expect("Human request wire");
            assert!(human_encoded.contains(PATH_SENTINEL));
            if has_private_pattern {
                assert!(human_encoded.contains(PATTERN_SENTINEL));
            }

            let policy = RoutePolicy::baseline_only_v1();
            let snapshot = match policy.evaluate_elevated(bound, Utc::now()) {
                ElevatedPolicyEvaluation::Ready { snapshot } => snapshot,
                other => panic!("remote Elevated review expected Ready, got {other:?}"),
            };
            let (transcript, action, policy_evidence) = provider_review_inputs_for_test(
                bound,
                &[],
                ToolInvocationRoute::Elevated,
                PolicyDecisionRecord::ElevatedPreflight,
                &snapshot,
                &Redactor::v1(),
            )
            .expect("remote reviewer inputs");
            let request = EscalationReviewRequest {
                participants: None,
                transcript,
                action,
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
            let bodies = escalation_provider_wire_bodies_for_test(request);
            assert_eq!(bodies.len(), 8, "four providers x initial/retry");
            for (provider, body) in bodies {
                let encoded = body.to_string();
                assert!(encoded.contains(PATH_SENTINEL));
                if has_private_pattern {
                    assert!(
                        encoded.contains(PATTERN_SENTINEL),
                        "{name} exact remote pattern missing from {provider}"
                    );
                }
                for digest in &local_digests {
                    assert_eq!(
                        encoded.matches(digest).count(),
                        0,
                        "{name} remote exact digest leaked through {provider}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn cancelled_bound_execution_never_begins_the_executor_effect() {
        let fake = Arc::new(FakeInvoker::default());
        let registry = bound_test_registry_from_invoker(fake.clone()).unwrap();
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        let sealed = registry
            .bind(
                &tool_call(
                    "cancel-before-executor-effect",
                    "list_dir",
                    json!({"path":"src"}),
                ),
                "flow-cancel-before-executor-effect",
                &workspace,
            )
            .await
            .unwrap();
        let authorized = crate::approval::authority::AuthorizedBoundInvocation::for_test(sealed);
        let cancel = CancellationToken::new();
        cancel.cancel();

        let error = match registry
            .execute_bound(authorized, cancel, Arc::new(|_| {}))
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("pre-effect cancellation must fail without an executor call"),
        };
        assert!(matches!(
            error,
            crate::tools::BoundExecutionError::Tool(ToolError::Cancelled)
        ));
        assert_eq!(fake.authorized_calls.load(Ordering::Relaxed), 0);
        assert!(fake.operations.lock().unwrap().is_empty());
    }

    #[test]
    fn workspace_binding_normalization_is_lexical_utf8_and_workspace_scoped() {
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        for (input, expected) in [
            (".", "."),
            ("./src//lib.rs", "src/lib.rs"),
            ("/workspace", "."),
            ("/workspace/src/界.rs", "src/界.rs"),
            (r"literal\backslash.txt", r"literal\backslash.txt"),
        ] {
            assert_eq!(
                normalize_workspace_path(input, &workspace).unwrap(),
                expected
            );
        }

        for input in [
            "",
            "..",
            "src/../secret",
            "/outside/workspace",
            "/workspace-other/file",
            "artifact://owner/tool-output/id",
            "nul\0byte",
            "line\nbreak",
        ] {
            assert!(
                normalize_workspace_path(input, &workspace).is_err(),
                "{input:?} must be rejected"
            );
        }
        assert!(
            normalize_workspace_path(&"x".repeat(MAX_WORKSPACE_PATH_BYTES + 1), &workspace)
                .is_err()
        );
    }

    #[test]
    fn glob_binding_reuses_executor_normalization_and_rejects_unreviewable_selectors() {
        for (input, expected) in [
            ("**/*.rs", "**/*.rs"),
            ("./src/./*.rs", "src/*.rs"),
            (".", "."),
        ] {
            assert_eq!(normalize_workspace_glob(input).unwrap(), expected);
            assert_eq!(
                normalize_glob_pattern(&normalize_workspace_glob(input).unwrap()).unwrap(),
                normalize_glob_pattern(input).unwrap()
            );
        }
        for input in [
            "",
            "/workspace/*.rs",
            "../*.rs",
            "src/../*.rs",
            "artifact://owner/tool-output/id",
            "line\nbreak",
        ] {
            assert!(normalize_workspace_glob(input).is_err(), "{input:?}");
        }
        assert!(normalize_workspace_glob(&"*".repeat(MAX_GLOB_PATTERN_BYTES + 1)).is_err());
    }

    #[tokio::test]
    async fn grep_binding_validates_regex_and_bounds_without_executor_rpc() {
        let fake = Arc::new(FakeInvoker::default());
        let registry = bound_test_registry_from_invoker(fake.clone()).unwrap();
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        for (call_id, pattern) in [
            ("invalid-regex", "[".to_owned()),
            ("control-regex", "line\nbreak".to_owned()),
            ("oversized-regex", "x".repeat(MAX_GREP_PATTERN_BYTES + 1)),
        ] {
            let result = registry
                .bind(
                    &tool_call(call_id, "grep", json!({"path":"src","pattern":pattern})),
                    "flow-grep-validation",
                    &workspace,
                )
                .await;
            assert!(matches!(result, Err(DescribeError::InvalidArguments)));
        }
        registry
            .bind(
                &tool_call(
                    "literal-artifact-regex",
                    "grep",
                    json!({"path":"src","pattern":"artifact://"}),
                ),
                "flow-grep-validation",
                &workspace,
            )
            .await
            .expect("artifact URI text is regex content here, not a target URI");
        assert!(fake.operations.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cross_workspace_absolute_selector_is_rejected_before_sealing_without_rpc() {
        let fake = Arc::new(FakeInvoker::default());
        let registry = bound_test_registry_from_invoker(fake.clone()).unwrap();
        let bound_workspace = WorkspacePaths::new("/workspace-a").unwrap();
        let result = registry
            .bind(
                &tool_call(
                    "cross-root",
                    "read_file",
                    json!({"path":"/workspace-b/private.txt"}),
                ),
                "flow-cross-root",
                &bound_workspace,
            )
            .await;
        assert!(matches!(result, Err(DescribeError::InvalidArguments)));
        assert!(fake.operations.lock().unwrap().is_empty());

        let sealed = registry
            .bind(
                &tool_call(
                    "bound-root",
                    "read_file",
                    json!({"path":"/workspace-a/private.txt"}),
                ),
                "flow-cross-root",
                &bound_workspace,
            )
            .await
            .unwrap();
        assert_eq!(
            Value::Object(sealed.invocation().execution_arguments.as_object().clone()),
            json!({"path":"private.txt","offset":0,"limit":51200})
        );
        assert!(
            !serde_json::to_string(sealed.invocation())
                .unwrap()
                .contains("workspace-a"),
            "the absolute workspace root must not leak into durable evidence"
        );

        let other_workspace = WorkspacePaths::new("/workspace-b").unwrap();
        let other_sealed = registry
            .bind(
                &tool_call("other-root", "read_file", json!({"path":"private.txt"})),
                "flow-cross-root",
                &other_workspace,
            )
            .await
            .unwrap();
        assert_ne!(
            sealed
                .invocation()
                .execution_identity
                .workspace_digest
                .as_bytes(),
            other_sealed
                .invocation()
                .execution_identity
                .workspace_digest
                .as_bytes(),
            "one relative selector under different workspace roots must have different identities"
        );
        assert_ne!(
            sealed.invocation().descriptor_digest.as_bytes(),
            other_sealed.invocation().descriptor_digest.as_bytes(),
            "review and approval evidence must remain workspace-bound"
        );
        assert!(fake.operations.lock().unwrap().is_empty());
    }

    struct UnboundProductionExtra;

    #[async_trait]
    impl Tool for UnboundProductionExtra {
        fn def(&self) -> ToolDefinition {
            definition::<PathArgs>("unbound_extra", "must fail production composition")
        }

        fn risk(&self) -> ToolRisk {
            ToolRisk::ReadOnly
        }

        async fn execute(&self, _ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
            unreachable!("production completeness test does not execute the fixture")
        }
    }

    #[test]
    fn production_composition_rejects_an_injected_unbound_tool() {
        let identity = RpcIdentity::from_wire(PAID, 7, "current-nonce").unwrap();
        let result = remote_executor_registry_with_tools(
            Arc::new(ExecutorClient::new(
                "/tmp/sumi-unused-executor.sock",
                identity,
            )),
            [Arc::new(UnboundProductionExtra) as Arc<dyn Tool>],
        );
        assert!(
            matches!(result, Err(ToolError::Protocol(message)) if message.contains("unbound_extra"))
        );
    }

    #[tokio::test]
    async fn unpublished_executor_tools_remain_unbound_and_out_of_production() {
        let broad = registry_from_invoker(Arc::new(FakeInvoker::default())).unwrap();
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        for (name, args) in [
            ("bash", json!({"command":"pwd"})),
            ("write_file", json!({"path":"a","content":"b"})),
            (
                "edit_file",
                json!({"path":"a","old_string":"b","new_string":"c"}),
            ),
            ("delete", json!({"path":"a"})),
            (
                "read_file",
                json!({"path":"artifact://owner/tool-output/id"}),
            ),
            (
                "grep",
                json!({"path":"artifact://owner/tool-output/id","pattern":"x"}),
            ),
        ] {
            assert!(broad.get(name).unwrap().bound_adapter().is_none());
            assert!(matches!(
                broad
                    .bind(
                        &tool_call("unpublished", name, args),
                        "flow-unpublished",
                        &workspace
                    )
                    .await,
                Err(DescribeError::MissingBoundAdapter { tool }) if tool == name
            ));
        }
        let production = remote_executor_registry(Arc::new(ExecutorClient::new(
            "/tmp/sumi-unused-executor.sock",
            RpcIdentity::from_wire(PAID, 7, "current-nonce").unwrap(),
        )))
        .unwrap();
        for name in ["bash", "write_file", "edit_file", "delete"] {
            assert!(production.get(name).is_none());
        }
        for (name, arguments) in [
            (
                "read_file",
                json!({"path":"artifact://owner/tool-output/id"}),
            ),
            (
                "grep",
                json!({"path":"artifact://owner/tool-output/id","pattern":"x"}),
            ),
        ] {
            assert!(matches!(
                production
                    .bind(
                        &tool_call("artifact-unpublished", name, arguments),
                        "flow-artifact-unpublished",
                        &workspace
                    )
                    .await,
                Err(DescribeError::InvalidArguments)
            ));
        }
    }

    #[tokio::test]
    async fn production_artifact_read_is_rejected_before_endpoint_interaction() {
        let root =
            std::env::temp_dir().join(format!("sumi-workspace-read-only-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let socket = root.join("executor.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let identity = RpcIdentity::from_wire(PAID, 7, "current-nonce").unwrap();
        let registry =
            remote_executor_registry(Arc::new(ExecutorClient::new(&socket, identity))).unwrap();

        let error = run(
            &registry,
            "read_file",
            "flow",
            "artifact-call",
            json!({
                "path":
                    "artifact://0198f0f4-9b72-7000-8000-000000000001/attachments/forbidden"
            }),
            CancellationToken::new(),
            Arc::new(|_| {}),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::InvalidPath(_)));
        assert!(
            timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err(),
            "production artifact input contacted the executor endpoint"
        );
        drop(listener);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn production_artifact_grep_is_rejected_before_endpoint_interaction() {
        let root =
            std::env::temp_dir().join(format!("sumi-workspace-grep-only-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let socket = root.join("executor.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let identity = RpcIdentity::from_wire(PAID, 7, "current-nonce").unwrap();
        let registry =
            remote_executor_registry(Arc::new(ExecutorClient::new(&socket, identity))).unwrap();

        let error = run(
            &registry,
            "grep",
            "flow",
            "artifact-call",
            json!({
                "path":
                    "artifact://0198f0f4-9b72-7000-8000-000000000001/attachments/forbidden",
                "pattern": "secret",
            }),
            CancellationToken::new(),
            Arc::new(|_| {}),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::InvalidPath(_)));
        assert!(
            timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err(),
            "production artifact input contacted the executor endpoint"
        );
        drop(listener);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn frozen_definitions_and_risks_are_complete() {
        let registry = registry_from_invoker(Arc::new(FakeInvoker::default())).unwrap();
        assert_eq!(registry.len(), 8);
        let definitions = registry.definitions();
        assert_eq!(
            definitions
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "bash",
                "delete",
                "edit_file",
                "glob",
                "grep",
                "list_dir",
                "read_file",
                "write_file"
            ]
        );
        let read = definitions
            .iter()
            .find(|definition| definition.name == "read_file")
            .unwrap();
        assert_eq!(read.parameters["properties"]["limit"]["default"], 51200);
        assert_eq!(read.parameters["properties"]["limit"]["minimum"], 1);
        assert_eq!(read.parameters["properties"]["limit"]["maximum"], 51200);
        for definition in definitions {
            assert_eq!(definition.parameters["type"], "object");
            assert_eq!(definition.parameters["additionalProperties"], false);
        }
        assert_eq!(
            registry.get("read_file").unwrap().risk(),
            ToolRisk::ReadOnly
        );
        assert_eq!(registry.get("delete").unwrap().risk(), ToolRisk::Mutating);
        assert_eq!(registry.get("bash").unwrap().risk(), ToolRisk::Exec);
        assert!(registry.get("remove_file").is_none());
    }

    #[test]
    fn bash_resource_limit_truth_is_preserved() {
        let mut result = bash_result(
            "partial",
            Some("artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/exec-log"),
            None,
            false,
            Some(ResourceLimit::WallTime { limit_seconds: 120 }),
        );
        result.observed_bytes = (DEFAULT_MAX_BYTES + 1) as u64;
        result.truncation.truncated = true;
        result.truncation.truncated_by = Some(TruncatedBy::Bytes);
        result.truncation.total_bytes = DEFAULT_MAX_BYTES + 1;
        let output = RemoteToolKind::Bash
            .output(ExecutorResponse::Bash { result }, None)
            .unwrap();
        assert_eq!(output.details["exit_code"], Value::Null);
        assert_eq!(output.details["resource_limit"]["type"], "wall_time");
        assert_eq!(output.details["resource_limit"]["limit_seconds"], 120);
        assert!(
            matches!(output.content[0], crate::provider::types::UserContent::Text { ref text } if text.contains("artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/exec-log") && text.contains("WallTime"))
        );
    }

    #[test]
    fn bash_output_is_error_matches_result_state() {
        for (exit_code, cancelled, resource_limit, expected_is_error, expected_text) in [
            (Some(0), false, None, false, "Command exited with code 0."),
            (Some(0), true, None, true, "Command cancelled."),
            (Some(1), false, None, true, "Command exited with code 1."),
            (
                None,
                false,
                None,
                true,
                "Command ended without an exit code.",
            ),
            (None, true, None, true, "Command cancelled."),
            (
                None,
                false,
                Some(ResourceLimit::WallTime { limit_seconds: 120 }),
                true,
                "WallTime",
            ),
        ] {
            let result = bash_result(
                "output",
                Some("artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/exec-log"),
                exit_code,
                cancelled,
                resource_limit.clone(),
            );
            let output = RemoteToolKind::Bash
                .output(ExecutorResponse::Bash { result }, None)
                .unwrap();
            assert_eq!(
                output.is_error, expected_is_error,
                "exit_code={exit_code:?}, cancelled={cancelled}, resource_limit={resource_limit:?}"
            );
            assert!(
                matches!(output.content[0], crate::provider::types::UserContent::Text { ref text } if text.contains(expected_text)),
                "unexpected terminal text for exit_code={exit_code:?}, cancelled={cancelled}, resource_limit={resource_limit:?}"
            );
        }
    }
}
