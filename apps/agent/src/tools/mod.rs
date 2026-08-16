//! Workspace tools and their execution boundary.

// This first T13 slice is wired into the runtime by the later executor/bash
// slice. Keep the independently tested public contracts warning-clean until
// those production call sites land.
#![allow(dead_code)]

#[cfg(target_os = "linux")]
pub mod bash;
#[cfg(not(target_os = "linux"))]
#[path = "bash_non_linux.rs"]
pub mod bash;
#[cfg(all(test, target_os = "linux"))]
#[allow(dead_code)]
#[path = "bash_non_linux.rs"]
mod bash_non_linux_compile_check;
pub(crate) mod bound;
#[cfg(target_os = "linux")]
pub mod executor;
#[cfg(target_os = "linux")]
pub mod fs;
pub(crate) mod messaging;
pub mod shell_capture;
pub mod truncate;
#[cfg(unix)]
mod unix_pipe;
pub(crate) mod workspace;
pub(crate) mod workspace_invitation;

use std::{
    collections::BTreeMap,
    future::Future,
    marker::PhantomData,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::approval::authority::{
    AuthorizedBoundInvocation, CommittedEffectReceipt, CommittedExecutionPermit,
};
use crate::provider::types::{ToolCall, ToolDefinition, UserContent, ValidatedToolArguments};
use crate::runtime::contracts::{ProcessGeneration, RpcIdentity};

pub(crate) use bound::{
    AdapterIdentity, AppActionDescriptor, AppPrecondition, BoundExecutionArguments,
    BoundExecutionIdentity, BoundToolInvocation, CapabilityClass, DescribeError, ResourceScope,
    ReviewProjection, ToolBinding,
};

/// Provider-facing scheduling metadata for the legacy raw tool path.
///
/// This is not an authorization decision and does not determine the trusted
/// app adapter's [`CapabilityClass`] mapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolRisk {
    ReadOnly,
    Mutating,
    Exec,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceLimit {
    OutputBytes { observed: u64, limit: u64 },
    InputBytes { observed: u64, limit: u64 },
    WallTime { limit_seconds: u64 },
    Concurrency,
    Cpu,
    Memory,
    Pids,
    DiskBytes,
    DiskInodes,
    ScanBytes,
    ScanEntries,
}

#[derive(Debug, thiserror::Error)]
pub enum ToolError {
    #[error("tool arguments did not match the typed schema")]
    InvalidArguments,
    #[error("tool execution was cancelled")]
    Cancelled,
    #[error("tool execution exceeded a resource limit: {0:?}")]
    ResourceLimit(ResourceLimit),
    #[error("workspace path was rejected: {0}")]
    InvalidPath(String),
    #[error("workspace operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("tool RPC failed: {0}")]
    Rpc(String),
    #[error("tool RPC outcome is indeterminate: {0}")]
    RpcIndeterminate(String),
    #[error("tool protocol violation: {0}")]
    Protocol(String),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BoundExecutionError {
    #[error("bound invocation was rejected: {0}")]
    InvalidInvocation(#[from] DescribeError),
    #[error("bound app operation failed: {0}")]
    Tool(#[from] ToolError),
}

impl From<crate::runtime::contracts::RuntimeContractError> for ToolError {
    fn from(error: crate::runtime::contracts::RuntimeContractError) -> Self {
        Self::Protocol(error.to_string())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToolOutput {
    pub content: Vec<UserContent>,
    pub details: Value,
    pub is_error: bool,
}

/// Observable result of best-effort app maintenance after a committed result.
/// A deferred hook is not a tool failure and must not rewrite the already
/// admitted tool result or fail an unrelated later action.
#[derive(Debug)]
pub(crate) enum LiveAppPostCommitOutcome {
    Applied,
    Deferred(ToolError),
}

type LiveAppPostCommitFuture =
    Pin<Box<dyn Future<Output = LiveAppPostCommitOutcome> + Send + 'static>>;

/// Non-serializable, non-authoritative process-local maintenance hook.
///
/// It is intentionally non-`Clone` and consumed once. The route may invoke it
/// only after receiving the durable commit receipt for the exact
/// `ToolExecutionEnd`/tool result. The hook emits no durable event, grants no
/// invocation authority, may be lost on crash, and is never recovered from
/// serialized evidence.
pub(crate) struct LiveAppPostCommit {
    callback: Box<dyn FnOnce(CancellationToken) -> LiveAppPostCommitFuture + Send + 'static>,
}

impl LiveAppPostCommit {
    pub(crate) fn new<F, Fut>(callback: F) -> Self
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = LiveAppPostCommitOutcome> + Send + 'static,
    {
        Self {
            callback: Box::new(move |cancel| Box::pin(callback(cancel))),
        }
    }

    pub(crate) async fn invoke_after_result_commit(
        self,
        cancel: CancellationToken,
    ) -> LiveAppPostCommitOutcome {
        (self.callback)(cancel).await
    }
}

mod bound_outcome {
    use super::{CommittedEffectReceipt, LiveAppPostCommit, ToolOutput};

    /// Exact tool output plus optional process-local app maintenance that
    /// becomes eligible only after the route durably commits that output.
    /// Construction requires a successful effect receipt, so a bound adapter
    /// cannot ignore its post-COMMIT permit and still return success.
    pub(crate) struct BoundToolExecutionOutcome {
        pub output: ToolOutput,
        pub live_post_commit: Option<LiveAppPostCommit>,
        _effect_receipt: CommittedEffectReceipt<()>,
    }

    impl BoundToolExecutionOutcome {
        pub(crate) fn new(
            effect_receipt: CommittedEffectReceipt<(ToolOutput, Option<LiveAppPostCommit>)>,
        ) -> Self {
            let ((output, live_post_commit), effect_receipt) = effect_receipt.into_parts();
            Self {
                output,
                live_post_commit,
                _effect_receipt: effect_receipt,
            }
        }

        pub(crate) fn without_live_post_commit(
            effect_receipt: CommittedEffectReceipt<ToolOutput>,
        ) -> Self {
            Self::new(effect_receipt.map(|output| (output, None)))
        }
    }
}

pub(crate) use bound_outcome::BoundToolExecutionOutcome;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspacePaths {
    root: PathBuf,
}

impl WorkspacePaths {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, ToolError> {
        let root = root.into();
        if !root.is_absolute() {
            return Err(ToolError::InvalidPath(
                "workspace root must be absolute".to_owned(),
            ));
        }
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

pub struct ToolCtx<'a> {
    /// Stable identity of the current assistant/tool flow. A caller must reuse
    /// it when retrying the same invocation and change it for a later flow.
    pub flow_id: &'a str,
    pub call_id: &'a str,
    pub args: &'a ValidatedToolArguments,
    pub cancel: CancellationToken,
    /// Synchronous progress delivery. The callback runs while the internal
    /// settlement gate is locked, so it must be prompt and nonblocking and
    /// must not synchronously re-enter this invocation's update gate. Queue
    /// any slow or asynchronous work in the callback owner.
    pub on_update: Arc<dyn Fn(Value) + Send + Sync>,
    pub workspace: &'a WorkspacePaths,
}

pub(crate) struct ToolBindCtx<'a> {
    pub args: &'a ValidatedToolArguments,
    pub workspace: &'a WorkspacePaths,
}

pub(crate) struct BoundToolCtx<'a> {
    pub flow_id: &'a str,
    pub call_id: &'a str,
    pub args: &'a BoundExecutionArguments,
    /// Move-only post-COMMIT authority for this exact sealed invocation.
    ///
    /// Local-control adapters retain it while parsing arguments and waiting on
    /// view locks, recheck cancellation, then call `begin_local_effect()`
    /// immediately before the network or filesystem effect. Executor adapters
    /// derive the complete `ExecutorOperation` from `args` first and pass this
    /// permit to the client, which calls `begin_executor_effect()` immediately
    /// before signing. Only the successful result-coupled receipt can construct
    /// a `BoundToolExecutionOutcome`.
    pub committed_effect_permit: CommittedExecutionPermit,
    pub cancel: CancellationToken,
    pub on_update: Arc<dyn Fn(Value) + Send + Sync>,
    pub workspace: &'a WorkspacePaths,
}

/// Unforgeable outside the registry module. Opening a post-COMMIT authorized
/// pair therefore cannot become a general crate-internal transport API.
pub(crate) struct AuthorizedBoundRegistryAccess(());

/// One complete app-owned binding/execution package.
///
/// Implementors must provide identity, proposal binding, and exact bound
/// execution together. A registry therefore cannot review through one
/// independently optional surface and discover a missing executor later.
/// The adapter maps its complete action vocabulary to coarse
/// [`CapabilityClass`] values and retains commit-time authorization.
#[async_trait]
pub(crate) trait BoundToolAdapter: Send + Sync {
    fn identity(&self) -> AdapterIdentity;
    /// Whether this frozen registration can bind at least one reviewer-safe
    /// read operation. The exact invocation is checked again after binding;
    /// mixed tools such as Messaging may expose one definition while only
    /// their Read variants are executable by a reviewer.
    fn reviewer_read_capable(&self) -> bool {
        false
    }
    async fn bind(&self, ctx: ToolBindCtx<'_>) -> Result<ToolBinding, DescribeError>;
    async fn execute(&self, ctx: BoundToolCtx<'_>) -> Result<BoundToolExecutionOutcome, ToolError>;
}

#[async_trait]
pub trait Tool: Send + Sync {
    fn def(&self) -> ToolDefinition;
    fn risk(&self) -> ToolRisk;
    /// Return the tool's complete app-owned bound adapter, if it has one.
    /// This single package is extracted and frozen during registration.
    fn bound_adapter(self: Arc<Self>) -> Option<Arc<dyn BoundToolAdapter>> {
        None
    }
    async fn execute(&self, ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError>;
}

struct GuardedTool {
    inner: Arc<dyn Tool>,
}

#[async_trait]
impl Tool for GuardedTool {
    fn def(&self) -> ToolDefinition {
        self.inner.def()
    }

    fn risk(&self) -> ToolRisk {
        self.inner.risk()
    }

    async fn execute(&self, ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
        let update = ToolUpdate {
            callback: ctx.on_update,
            settled: Arc::new(Mutex::new(false)),
        };
        let _settlement = ToolSettlementGuard::new(update.clone());
        let guarded_update = update.clone();
        self.inner
            .execute(ToolCtx {
                flow_id: ctx.flow_id,
                call_id: ctx.call_id,
                args: ctx.args,
                cancel: ctx.cancel,
                on_update: Arc::new(move |value| guarded_update.emit(value)),
                workspace: ctx.workspace,
            })
            .await
    }
}

struct GuardedBoundToolAdapter {
    inner: Arc<dyn BoundToolAdapter>,
}

#[async_trait]
impl BoundToolAdapter for GuardedBoundToolAdapter {
    fn identity(&self) -> AdapterIdentity {
        self.inner.identity()
    }

    fn reviewer_read_capable(&self) -> bool {
        self.inner.reviewer_read_capable()
    }

    async fn bind(&self, ctx: ToolBindCtx<'_>) -> Result<ToolBinding, DescribeError> {
        self.inner.bind(ctx).await
    }

    async fn execute(&self, ctx: BoundToolCtx<'_>) -> Result<BoundToolExecutionOutcome, ToolError> {
        let update = ToolUpdate {
            callback: ctx.on_update,
            settled: Arc::new(Mutex::new(false)),
        };
        let _settlement = ToolSettlementGuard::new(update.clone());
        let guarded_update = update.clone();
        self.inner
            .execute(BoundToolCtx {
                flow_id: ctx.flow_id,
                call_id: ctx.call_id,
                args: ctx.args,
                committed_effect_permit: ctx.committed_effect_permit,
                cancel: ctx.cancel,
                on_update: Arc::new(move |value| guarded_update.emit(value)),
                workspace: ctx.workspace,
            })
            .await
    }
}

#[derive(Default)]
pub struct ToolRegistryBuilder {
    tools: BTreeMap<String, RegisteredTool>,
}

impl ToolRegistryBuilder {
    pub fn register(&mut self, tool: Arc<dyn Tool>) -> Result<(), ToolError> {
        let definition = tool.def();
        let name = definition.name.clone();
        if self.tools.contains_key(&name) {
            return Err(ToolError::Protocol(format!(
                "duplicate frozen tool definition: {name}"
            )));
        }
        let bound_adapter = tool
            .clone()
            .bound_adapter()
            .map(|adapter| {
                let identity = adapter.identity();
                identity
                    .validate()
                    .map_err(|error| ToolError::Protocol(error.to_string()))?;
                Ok::<_, ToolError>(RegisteredBoundToolAdapter {
                    identity,
                    adapter: Arc::new(GuardedBoundToolAdapter { inner: adapter }),
                    // This pointer token binds a live invocation to this exact
                    // frozen registration. It is defense-in-depth registry
                    // identity only: not Approval, policy, or one-shot authority.
                    registration_seal: Arc::new(()),
                })
            })
            .transpose()?;
        self.tools.insert(
            name,
            RegisteredTool {
                definition,
                tool: Arc::new(GuardedTool { inner: tool }),
                bound_adapter,
            },
        );
        Ok(())
    }

    pub fn build(self) -> ToolRegistry {
        ToolRegistry {
            tools: self.tools,
            executor_identity: None,
            registry_seal: Arc::new(()),
        }
    }

    pub(crate) fn build_bound_for_executor_identity(
        self,
        identity: RpcIdentity,
    ) -> Result<ToolRegistry, ToolError> {
        let missing_binders = self
            .tools
            .iter()
            .filter_map(|(name, tool)| tool.bound_adapter.is_none().then_some(name.as_str()))
            .collect::<Vec<_>>();
        if !missing_binders.is_empty() {
            return Err(ToolError::Protocol(format!(
                "production tool registry contains tools without binding adapters: {}",
                missing_binders.join(", ")
            )));
        }
        Ok(ToolRegistry {
            tools: self.tools,
            executor_identity: Some(identity),
            registry_seal: Arc::new(()),
        })
    }
}

#[derive(Clone, Default)]
pub struct ToolRegistry {
    tools: BTreeMap<String, RegisteredTool>,
    // Local and in-memory registries do not cross an executor RPC boundary.
    // Production remote registries bind the immutable client's complete RPC
    // identity so neither PAID nor boot nonce can be erased at composition.
    executor_identity: Option<RpcIdentity>,
    registry_seal: Arc<()>,
}

#[derive(Clone)]
struct RegisteredTool {
    definition: ToolDefinition,
    tool: Arc<dyn Tool>,
    bound_adapter: Option<RegisteredBoundToolAdapter>,
}

#[derive(Clone)]
struct RegisteredBoundToolAdapter {
    identity: AdapterIdentity,
    adapter: Arc<dyn BoundToolAdapter>,
    registration_seal: Arc<()>,
}

/// Opaque same-process execution handle paired with durable invocation
/// evidence.
///
/// The serializable invocation is evidence. The registry and registration
/// pointer tokens, sealed flow id, and concrete `WorkspacePaths` are live
/// execution binding only. They are not Approval, policy, or one-shot
/// authority. This wrapper is deliberately non-`Clone` and is consumed by
/// execution as defense in depth against accidental in-process reuse.
/// ADR 0013's durable start barrier still owns exactly one committed
/// start/effect across crash, cancellation, and retry boundaries.
pub(crate) struct SealedBoundToolInvocation {
    invocation: BoundToolInvocation,
    sealed_evidence_digest: bound::InvocationDigest,
    flow_id: String,
    workspace: WorkspacePaths,
    registry_seal: Arc<()>,
    registration_seal: Arc<()>,
}

impl SealedBoundToolInvocation {
    pub(crate) fn invocation(&self) -> &BoundToolInvocation {
        &self.invocation
    }

    pub(crate) fn evidence_digest(&self) -> bound::InvocationDigest {
        self.sealed_evidence_digest
    }
}

impl ToolRegistry {
    pub(crate) fn validate_executor_generation(
        &self,
        generation: ProcessGeneration,
    ) -> Result<(), ToolError> {
        if let Some(bound) = &self.executor_identity
            && bound.generation() != generation
        {
            return Err(ToolError::Protocol(format!(
                "remote tool registry executor generation {} does not match injected generation {generation}",
                bound.generation()
            )));
        }
        Ok(())
    }

    /// Validate the immutable identity of a production remote registry.
    ///
    /// An unbound local/in-memory registry is intentionally rejected here.
    /// Generation-only validation remains available solely for explicit
    /// fixture composition and cannot satisfy a hydrated Session start.
    pub(crate) fn validate_executor_identity(
        &self,
        identity: &RpcIdentity,
    ) -> Result<(), ToolError> {
        let bound = self.executor_identity.as_ref().ok_or_else(|| {
            ToolError::Protocol(
                "tool registry is not bound to a production executor RPC identity".to_owned(),
            )
        })?;
        if bound != identity {
            return Err(ToolError::Protocol(
                "remote tool registry executor RPC identity does not match the authenticated Session runtime"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools.get(name).map(|entry| entry.tool.clone())
    }

    /// Ask the exact frozen app adapter to bind a model-facing proposal to a
    /// serializable operation. The production runner does not call this path
    /// yet; it is a neutral predecessor for later policy and route wiring.
    pub(crate) async fn bind(
        &self,
        call: &ToolCall,
        flow_id: &str,
        workspace: &WorkspacePaths,
    ) -> Result<SealedBoundToolInvocation, DescribeError> {
        if call.id.is_empty() || call.name.is_empty() {
            return Err(DescribeError::InvalidProposalIdentity {
                reason: "tool call id and name must be non-empty".to_owned(),
            });
        }
        let registered = self
            .tools
            .get(&call.name)
            .ok_or_else(|| DescribeError::UnknownTool {
                tool: call.name.clone(),
            })?;
        let execution_identity = BoundExecutionIdentity::seal(flow_id, workspace.root())?;
        let bound_adapter = registered.bound_adapter.as_ref().ok_or_else(|| {
            DescribeError::MissingBoundAdapter {
                tool: call.name.clone(),
            }
        })?;
        let binding = bound_adapter
            .adapter
            .bind(ToolBindCtx {
                args: &call.arguments,
                workspace,
            })
            .await?;
        let invocation = BoundToolInvocation::seal(
            &call.id,
            &call.name,
            call.arguments.as_object(),
            bound_adapter.identity.clone(),
            execution_identity,
            binding,
        )?;
        let sealed_evidence_digest = invocation.evidence_digest()?;
        Ok(SealedBoundToolInvocation {
            invocation,
            sealed_evidence_digest,
            flow_id: flow_id.to_owned(),
            workspace: workspace.clone(),
            registry_seal: self.registry_seal.clone(),
            registration_seal: bound_adapter.registration_seal.clone(),
        })
    }

    /// Re-establish that serialized evidence is still paired with the exact
    /// registry and registration that produced it.
    ///
    /// The pointer seals are registry-binding defense in depth. Validation is
    /// intentionally repeatable and confers neither Approval, policy, nor a
    /// one-shot right to execute. A deserialized invocation alone is
    /// deliberately not executable authority.
    pub(crate) fn validate_bound<'a>(
        &'a self,
        sealed: &'a SealedBoundToolInvocation,
    ) -> Result<&'a BoundToolInvocation, DescribeError> {
        if !Arc::ptr_eq(&self.registry_seal, &sealed.registry_seal) {
            return Err(DescribeError::RegistryIdentityMismatch);
        }
        let registered = self
            .tools
            .get(&sealed.invocation.tool_name)
            .ok_or(DescribeError::RegistryIdentityMismatch)?;
        let bound_adapter = registered
            .bound_adapter
            .as_ref()
            .ok_or(DescribeError::RegistryIdentityMismatch)?;
        if !Arc::ptr_eq(&bound_adapter.registration_seal, &sealed.registration_seal)
            || bound_adapter.identity != sealed.invocation.adapter
        {
            return Err(DescribeError::RegistryIdentityMismatch);
        }
        let sealed_execution_identity =
            BoundExecutionIdentity::seal(&sealed.flow_id, sealed.workspace.root())
                .map_err(|_| DescribeError::SealedEvidenceMismatch)?;
        let recomputed_descriptor_digest = sealed
            .invocation
            .recompute_descriptor_digest()
            .map_err(|_| DescribeError::SealedEvidenceMismatch)?;
        let recomputed_evidence_digest = sealed
            .invocation
            .evidence_digest()
            .map_err(|_| DescribeError::SealedEvidenceMismatch)?;
        if sealed.invocation.execution_identity != sealed_execution_identity
            || recomputed_descriptor_digest != sealed.invocation.descriptor_digest
            || recomputed_evidence_digest != sealed.sealed_evidence_digest
        {
            return Err(DescribeError::SealedEvidenceMismatch);
        }
        Ok(&sealed.invocation)
    }

    /// Consume and execute only the operation already sealed by this exact
    /// registry, durable flow, and concrete workspace.
    ///
    /// This path neither invokes `bind` again nor consults current app view
    /// state to reinterpret the model proposal. It accepts no caller-supplied
    /// flow or workspace substitution. A deserialized invocation has no live
    /// seal and therefore cannot enter this method after restart. Consuming the
    /// handle prevents accidental local reuse; the durable ADR 0013 route must
    /// still enforce the exactly-one committed start/effect barrier.
    pub(crate) async fn execute_bound(
        &self,
        authorized: AuthorizedBoundInvocation,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<BoundToolExecutionOutcome, BoundExecutionError> {
        self.validate_bound(authorized.sealed())?;
        let (sealed, committed_execution_permit) =
            authorized.into_registry_parts(AuthorizedBoundRegistryAccess(()))?;
        let invocation = sealed.invocation();
        let registered = self
            .tools
            .get(&invocation.tool_name)
            .ok_or(DescribeError::RegistryIdentityMismatch)?;
        let bound_adapter = registered
            .bound_adapter
            .as_ref()
            .ok_or(DescribeError::RegistryIdentityMismatch)?;
        bound_adapter
            .adapter
            .execute(BoundToolCtx {
                flow_id: &sealed.flow_id,
                call_id: &invocation.tool_call_id,
                args: &invocation.execution_arguments,
                committed_effect_permit: committed_execution_permit,
                cancel,
                on_update,
                workspace: &sealed.workspace,
            })
            .await
            .map_err(BoundExecutionError::from)
    }

    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.tools
            .values()
            .map(|entry| entry.definition.clone())
            .collect()
    }

    /// Provider definitions for bound registrations that can resolve a Read
    /// descriptor. Execution still rejects every non-Read bound invocation.
    pub(crate) fn reviewer_read_definitions(&self) -> Vec<ToolDefinition> {
        self.tools
            .values()
            .filter(|entry| {
                entry
                    .bound_adapter
                    .as_ref()
                    .is_some_and(|adapter| adapter.adapter.reviewer_read_capable())
            })
            .map(|entry| entry.definition.clone())
            .collect()
    }

    pub fn len(&self) -> usize {
        self.tools.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }
}

#[derive(Clone)]
pub struct ToolUpdate {
    callback: Arc<dyn Fn(Value) + Send + Sync>,
    settled: Arc<Mutex<bool>>,
}

impl ToolUpdate {
    /// Deliver one progress update while the settlement gate is held.
    /// Implementations supplied as the callback must therefore be prompt and
    /// nonblocking; waiting for settlement or re-entering this update gate
    /// synchronously can deadlock settlement.
    pub fn emit(&self, update: Value) {
        let Ok(settled) = self.settled.lock() else {
            return;
        };
        if !*settled {
            (self.callback)(update);
        }
    }

    fn settle(&self) {
        match self.settled.lock() {
            Ok(mut settled) => *settled = true,
            Err(poisoned) => *poisoned.into_inner() = true,
        }
    }
}

struct ToolSettlementGuard {
    update: ToolUpdate,
}

impl ToolSettlementGuard {
    fn new(update: ToolUpdate) -> Self {
        Self { update }
    }
}

impl Drop for ToolSettlementGuard {
    fn drop(&mut self) {
        self.update.settle();
    }
}

pub struct TypedToolCtx<'a> {
    pub flow_id: &'a str,
    pub call_id: &'a str,
    pub cancel: CancellationToken,
    pub on_update: ToolUpdate,
    pub workspace: &'a WorkspacePaths,
}

#[async_trait]
pub trait TypedToolHandler<P>: Send + Sync {
    async fn execute(&self, params: P, ctx: TypedToolCtx<'_>) -> Result<ToolOutput, ToolError>;
}

pub struct TypedTool<P, H> {
    name: String,
    description: String,
    risk: ToolRisk,
    handler: H,
    marker: PhantomData<fn(P)>,
}

impl<P, H> TypedTool<P, H> {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        risk: ToolRisk,
        handler: H,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            risk,
            handler,
            marker: PhantomData,
        }
    }
}

#[async_trait]
impl<P, H> Tool for TypedTool<P, H>
where
    P: JsonSchema + DeserializeOwned + Send + Sync + 'static,
    H: TypedToolHandler<P> + Send + Sync,
{
    fn def(&self) -> ToolDefinition {
        let schema = schemars::schema_for!(P);
        ToolDefinition {
            name: self.name.clone(),
            description: self.description.clone(),
            parameters: serde_json::to_value(schema)
                .unwrap_or_else(|_| Value::Object(Default::default())),
        }
    }

    fn risk(&self) -> ToolRisk {
        self.risk
    }

    async fn execute(&self, ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
        let value = Value::Object(ctx.args.as_object().clone());
        let params = serde_json::from_value::<P>(value).map_err(|_| ToolError::InvalidArguments)?;
        let update = ToolUpdate {
            callback: ctx.on_update,
            settled: Arc::new(Mutex::new(false)),
        };
        let _settlement = ToolSettlementGuard::new(update.clone());
        self.handler
            .execute(
                params,
                TypedToolCtx {
                    flow_id: ctx.flow_id,
                    call_id: ctx.call_id,
                    cancel: ctx.cancel,
                    on_update: update.clone(),
                    workspace: ctx.workspace,
                },
            )
            .await
    }
}

pub fn text_output(text: impl Into<String>, details: Value) -> ToolOutput {
    ToolOutput {
        content: vec![UserContent::Text { text: text.into() }],
        details,
        is_error: false,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use schemars::JsonSchema;
    use serde::Deserialize;
    use serde_json::json;

    use super::*;

    #[derive(Deserialize, JsonSchema)]
    struct Params {
        value: String,
    }

    struct Handler {
        retained: Arc<Mutex<Option<ToolUpdate>>>,
    }

    type RawUpdate = Arc<dyn Fn(Value) + Send + Sync>;

    struct RawRetainingTool {
        name: &'static str,
        retained: Arc<Mutex<Option<RawUpdate>>>,
        pending: bool,
    }

    struct BindingTool {
        name: &'static str,
        bind_count: Arc<AtomicUsize>,
        bound_executions: Arc<Mutex<Vec<Value>>>,
    }

    #[async_trait]
    impl Tool for RawRetainingTool {
        fn def(&self) -> ToolDefinition {
            ToolDefinition {
                name: self.name.to_owned(),
                description: "raw retaining tool".to_owned(),
                parameters: json!({"type": "object"}),
            }
        }

        fn risk(&self) -> ToolRisk {
            ToolRisk::ReadOnly
        }

        async fn execute(&self, ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
            (ctx.on_update)(json!({"phase": "running"}));
            *self.retained.lock().expect("retained raw update lock") = Some(ctx.on_update);
            if self.pending {
                pending().await
            } else {
                Ok(text_output("done", json!({"ok": true})))
            }
        }
    }

    #[async_trait]
    impl Tool for BindingTool {
        fn def(&self) -> ToolDefinition {
            ToolDefinition {
                name: self.name.to_owned(),
                description: "binding test tool".to_owned(),
                parameters: json!({"type": "object"}),
            }
        }

        fn risk(&self) -> ToolRisk {
            ToolRisk::ReadOnly
        }

        fn bound_adapter(self: Arc<Self>) -> Option<Arc<dyn BoundToolAdapter>> {
            Some(self)
        }

        async fn execute(&self, _ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
            unreachable!("binding contract tests never activate raw execution")
        }
    }

    #[async_trait]
    impl BoundToolAdapter for BindingTool {
        fn identity(&self) -> AdapterIdentity {
            AdapterIdentity::new("test.binding", 1).expect("valid test adapter")
        }

        async fn bind(&self, ctx: ToolBindCtx<'_>) -> Result<ToolBinding, DescribeError> {
            self.bind_count.fetch_add(1, Ordering::Relaxed);
            Ok(ToolBinding::new(
                AppActionDescriptor::new(
                    "inspect",
                    CapabilityClass::Read,
                    vec![ResourceScope::collection("test", "item")],
                )?,
                ReviewProjection::from_value(json!({"operation": "inspect"}))?,
                BoundExecutionArguments::from_value(Value::Object(ctx.args.as_object().clone()))?,
            ))
        }

        async fn execute(
            &self,
            ctx: BoundToolCtx<'_>,
        ) -> Result<BoundToolExecutionOutcome, ToolError> {
            let arguments = Value::Object(ctx.args.as_object().clone());
            let effect_start = ctx.committed_effect_permit.begin_local_effect();
            let effect_receipt = effect_start
                .complete(|| async {
                    self.bound_executions
                        .lock()
                        .expect("bound executions lock")
                        .push(json!({
                            "arguments": arguments.clone(),
                            "flow_id": ctx.flow_id,
                            "workspace": ctx.workspace.root(),
                        }));
                    Ok::<_, ToolError>(text_output("bound", arguments))
                })
                .await?;
            Ok(BoundToolExecutionOutcome::without_live_post_commit(
                effect_receipt,
            ))
        }
    }

    #[async_trait]
    impl TypedToolHandler<Params> for Handler {
        async fn execute(
            &self,
            params: Params,
            ctx: TypedToolCtx<'_>,
        ) -> Result<ToolOutput, ToolError> {
            ctx.on_update.emit(json!({"phase": "running"}));
            *self.retained.lock().expect("retained update lock") = Some(ctx.on_update);
            Ok(text_output(params.value, json!({"ok": true})))
        }
    }

    fn validated(value: Value) -> ValidatedToolArguments {
        serde_json::from_value(value).expect("object-shaped arguments")
    }

    fn call(id: &str, name: &str, arguments: Value) -> ToolCall {
        ToolCall {
            id: id.to_owned(),
            name: name.to_owned(),
            route: crate::provider::types::ToolInvocationRoute::Normal,
            arguments: validated(arguments),
        }
    }

    #[tokio::test]
    async fn typed_tool_ignores_update_after_settlement() {
        let retained = Arc::new(Mutex::new(None));
        let tool = TypedTool::<Params, _>::new(
            "echo",
            "echo",
            ToolRisk::ReadOnly,
            Handler {
                retained: retained.clone(),
            },
        );
        let updates = Arc::new(Mutex::new(Vec::new()));
        let callback_updates = updates.clone();
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");
        let result = tool
            .execute(ToolCtx {
                flow_id: "flow-1",
                call_id: "call-1",
                args: &validated(json!({"value": "ok"})),
                cancel: CancellationToken::new(),
                on_update: Arc::new(move |value| {
                    callback_updates.lock().expect("updates lock").push(value);
                }),
                workspace: &workspace,
            })
            .await
            .expect("typed tool output");
        assert_eq!(
            result.content,
            vec![UserContent::Text { text: "ok".into() }]
        );

        retained
            .lock()
            .expect("retained update lock")
            .as_ref()
            .expect("retained update")
            .emit(json!({"phase": "late"}));
        assert_eq!(updates.lock().expect("updates lock").len(), 1);
    }

    #[tokio::test]
    async fn dropping_typed_tool_future_settles_retained_updates() {
        struct PendingHandler {
            retained: Arc<Mutex<Option<ToolUpdate>>>,
        }

        #[async_trait]
        impl TypedToolHandler<Params> for PendingHandler {
            async fn execute(
                &self,
                _params: Params,
                ctx: TypedToolCtx<'_>,
            ) -> Result<ToolOutput, ToolError> {
                *self.retained.lock().expect("retained update lock") = Some(ctx.on_update);
                pending().await
            }
        }

        let retained = Arc::new(Mutex::new(None));
        let tool = TypedTool::<Params, _>::new(
            "pending",
            "pending",
            ToolRisk::ReadOnly,
            PendingHandler {
                retained: retained.clone(),
            },
        );
        let update_count = Arc::new(AtomicUsize::new(0));
        let callback_count = update_count.clone();
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");
        let args = validated(json!({"value": "ok"}));
        let mut future = Box::pin(tool.execute(ToolCtx {
            flow_id: "flow-pending",
            call_id: "call-pending",
            args: &args,
            cancel: CancellationToken::new(),
            on_update: Arc::new(move |_| {
                callback_count.fetch_add(1, Ordering::Relaxed);
            }),
            workspace: &workspace,
        }));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), future.as_mut())
                .await
                .is_err()
        );
        drop(future);

        retained
            .lock()
            .expect("retained update lock")
            .as_ref()
            .expect("retained update")
            .emit(json!({"phase": "late"}));
        assert_eq!(update_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn registry_suppresses_raw_tool_updates_after_return() {
        let retained = Arc::new(Mutex::new(None));
        let mut builder = ToolRegistryBuilder::default();
        builder
            .register(Arc::new(RawRetainingTool {
                name: "raw",
                retained: retained.clone(),
                pending: false,
            }))
            .expect("register raw tool");
        let tool = builder.build().get("raw").expect("registered raw tool");
        let update_count = Arc::new(AtomicUsize::new(0));
        let callback_count = update_count.clone();
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");
        let args = validated(json!({}));
        tool.execute(ToolCtx {
            flow_id: "raw-flow",
            call_id: "raw-call",
            args: &args,
            cancel: CancellationToken::new(),
            on_update: Arc::new(move |_| {
                callback_count.fetch_add(1, Ordering::Relaxed);
            }),
            workspace: &workspace,
        })
        .await
        .expect("raw tool result");

        retained
            .lock()
            .expect("retained raw update lock")
            .as_ref()
            .expect("retained raw update")(json!({"phase": "late"}));
        assert_eq!(update_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn dropping_registry_raw_tool_future_settles_updates() {
        let retained = Arc::new(Mutex::new(None));
        let mut builder = ToolRegistryBuilder::default();
        builder
            .register(Arc::new(RawRetainingTool {
                name: "raw-pending",
                retained: retained.clone(),
                pending: true,
            }))
            .expect("register pending raw tool");
        let tool = builder
            .build()
            .get("raw-pending")
            .expect("registered pending raw tool");
        let update_count = Arc::new(AtomicUsize::new(0));
        let callback_count = update_count.clone();
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");
        let args = validated(json!({}));
        let mut future = Box::pin(tool.execute(ToolCtx {
            flow_id: "raw-pending-flow",
            call_id: "raw-pending-call",
            args: &args,
            cancel: CancellationToken::new(),
            on_update: Arc::new(move |_| {
                callback_count.fetch_add(1, Ordering::Relaxed);
            }),
            workspace: &workspace,
        }));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), future.as_mut())
                .await
                .is_err()
        );
        drop(future);

        retained
            .lock()
            .expect("retained raw update lock")
            .as_ref()
            .expect("retained raw update")(json!({"phase": "late"}));
        assert_eq!(update_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn settlement_waits_for_an_in_flight_update_and_closes_the_gate() {
        let entered = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_entered = entered.clone();
        let callback_release = release.clone();
        let observed = callback_count.clone();
        let update = ToolUpdate {
            callback: Arc::new(move |_| {
                callback_entered.wait();
                callback_release.wait();
                observed.fetch_add(1, Ordering::Relaxed);
            }),
            settled: Arc::new(Mutex::new(false)),
        };

        let emitter = {
            let update = update.clone();
            std::thread::spawn(move || update.emit(json!({"phase": "running"})))
        };
        entered.wait();
        let (settled_tx, settled_rx) = std::sync::mpsc::channel();
        let settler = {
            let update = update.clone();
            std::thread::spawn(move || {
                update.settle();
                settled_tx.send(()).expect("settlement result receiver");
            })
        };
        assert!(settled_rx.try_recv().is_err());
        release.wait();
        emitter.join().expect("emitter thread");
        settler.join().expect("settler thread");
        settled_rx.recv().expect("settlement completed");

        update.emit(json!({"phase": "late"}));
        assert_eq!(callback_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn registry_is_frozen_after_build_and_rejects_duplicates() {
        struct Never;
        #[async_trait]
        impl TypedToolHandler<Params> for Never {
            async fn execute(
                &self,
                _params: Params,
                _ctx: TypedToolCtx<'_>,
            ) -> Result<ToolOutput, ToolError> {
                unreachable!("not executed")
            }
        }

        let mut builder = ToolRegistryBuilder::default();
        builder
            .register(Arc::new(TypedTool::<Params, _>::new(
                "one",
                "one",
                ToolRisk::ReadOnly,
                Never,
            )))
            .expect("first registration");
        assert!(
            builder
                .register(Arc::new(TypedTool::<Params, _>::new(
                    "one",
                    "duplicate",
                    ToolRisk::Exec,
                    Never,
                )))
                .is_err()
        );
        let registry = builder.build();
        assert_eq!(registry.len(), 1);
        assert!(registry.get("one").is_some());
        assert_eq!(registry.definitions()[0].description, "one");
    }

    #[tokio::test]
    async fn registry_binding_fails_closed_without_a_complete_adapter_package() {
        struct Never;
        #[async_trait]
        impl TypedToolHandler<Params> for Never {
            async fn execute(
                &self,
                _params: Params,
                _ctx: TypedToolCtx<'_>,
            ) -> Result<ToolOutput, ToolError> {
                unreachable!("missing-adapter test never executes")
            }
        }

        let mut builder = ToolRegistryBuilder::default();
        builder
            .register(Arc::new(TypedTool::<Params, _>::new(
                "unbound",
                "unbound",
                ToolRisk::ReadOnly,
                Never,
            )))
            .expect("register unbound tool");
        let registry = builder.build();
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");

        let result = registry
            .bind(
                &call("call-1", "unbound", json!({"value": "x"})),
                "flow-1",
                &workspace,
            )
            .await;
        assert!(matches!(
            result,
            Err(DescribeError::MissingBoundAdapter { tool }) if tool == "unbound"
        ));
    }

    #[tokio::test]
    async fn registration_seal_is_repeatable_binding_not_approval_or_mutable_identity() {
        fn registry() -> ToolRegistry {
            let mut builder = ToolRegistryBuilder::default();
            builder
                .register(Arc::new(BindingTool {
                    name: "inspect",
                    bind_count: Arc::new(AtomicUsize::new(0)),
                    bound_executions: Arc::new(Mutex::new(Vec::new())),
                }))
                .expect("register binding tool");
            builder.build()
        }

        let origin = registry();
        let other = registry();
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");
        let sealed = origin
            .bind(
                &call("call-1", "inspect", json!({"path": "alpha"})),
                "flow-1",
                &workspace,
            )
            .await
            .expect("bind invocation");

        assert_eq!(
            origin
                .validate_bound(&sealed)
                .expect("origin registry accepts its seal")
                .tool_call_id,
            "call-1"
        );
        assert!(origin.clone().validate_bound(&sealed).is_ok());
        assert!(matches!(
            other.validate_bound(&sealed),
            Err(DescribeError::RegistryIdentityMismatch)
        ));

        let mut altered_call = origin
            .bind(
                &call("call-1", "inspect", json!({"path": "alpha"})),
                "flow-1",
                &workspace,
            )
            .await
            .expect("bind call mutation fixture");
        altered_call.invocation.tool_call_id = "call-2".to_owned();
        assert!(matches!(
            origin.validate_bound(&altered_call),
            Err(DescribeError::SealedEvidenceMismatch)
        ));

        let mut altered_projection = origin
            .bind(
                &call("call-1", "inspect", json!({"path": "alpha"})),
                "flow-1",
                &workspace,
            )
            .await
            .expect("bind projection mutation fixture");
        altered_projection.invocation.review_projection =
            ReviewProjection::from_value(json!({"operation": "replace"}))
                .expect("valid replacement projection");
        assert!(matches!(
            origin.validate_bound(&altered_projection),
            Err(DescribeError::SealedEvidenceMismatch)
        ));

        let mut altered_arguments = origin
            .bind(
                &call("call-1", "inspect", json!({"path": "alpha"})),
                "flow-1",
                &workspace,
            )
            .await
            .expect("bind argument mutation fixture");
        altered_arguments.invocation.execution_arguments =
            BoundExecutionArguments::from_value(json!({"path": "beta"}))
                .expect("valid replacement arguments");
        assert!(matches!(
            origin.validate_bound(&altered_arguments),
            Err(DescribeError::SealedEvidenceMismatch)
        ));

        let mut altered_flow = origin
            .bind(
                &call("call-1", "inspect", json!({"path": "alpha"})),
                "flow-1",
                &workspace,
            )
            .await
            .expect("bind flow mutation fixture");
        altered_flow.flow_id = "flow-2".to_owned();
        assert!(matches!(
            origin.validate_bound(&altered_flow),
            Err(DescribeError::SealedEvidenceMismatch)
        ));

        let mut altered_workspace = origin
            .bind(
                &call("call-1", "inspect", json!({"path": "alpha"})),
                "flow-1",
                &workspace,
            )
            .await
            .expect("bind workspace mutation fixture");
        altered_workspace.workspace =
            WorkspacePaths::new("/other-workspace").expect("replacement workspace");
        assert!(matches!(
            origin.validate_bound(&altered_workspace),
            Err(DescribeError::SealedEvidenceMismatch)
        ));
    }

    #[tokio::test]
    async fn execute_bound_consumes_sealed_arguments_flow_and_workspace_without_rebinding() {
        let bind_count = Arc::new(AtomicUsize::new(0));
        let bound_executions = Arc::new(Mutex::new(Vec::new()));
        let mut builder = ToolRegistryBuilder::default();
        builder
            .register(Arc::new(BindingTool {
                name: "inspect",
                bind_count: bind_count.clone(),
                bound_executions: bound_executions.clone(),
            }))
            .expect("register binding tool");
        let registry = builder.build();
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");
        let sealed = registry
            .bind(
                &call("call-1", "inspect", json!({"path": "alpha"})),
                "flow-1",
                &workspace,
            )
            .await
            .expect("bind invocation");
        assert_eq!(bind_count.load(Ordering::Relaxed), 1);

        let authorized = AuthorizedBoundInvocation::for_test(sealed);
        let output = registry
            .execute_bound(authorized, CancellationToken::new(), Arc::new(|_| {}))
            .await
            .expect("execute sealed invocation");
        assert_eq!(output.output.details, json!({"path": "alpha"}));
        assert!(output.live_post_commit.is_none());
        assert_eq!(bind_count.load(Ordering::Relaxed), 1);
        assert_eq!(
            bound_executions
                .lock()
                .expect("bound executions lock")
                .as_slice(),
            &[json!({
                "arguments": {"path": "alpha"},
                "flow_id": "flow-1",
                "workspace": "/workspace"
            })]
        );

        let mut altered = registry
            .bind(
                &call("call-2", "inspect", json!({"path": "alpha"})),
                "flow-1",
                &workspace,
            )
            .await
            .expect("bind altered invocation");
        altered.invocation.descriptor.operation = "replace".to_owned();
        let authorized = AuthorizedBoundInvocation::for_test(altered);
        let rejected = registry
            .execute_bound(authorized, CancellationToken::new(), Arc::new(|_| {}))
            .await;
        assert!(matches!(
            rejected,
            Err(BoundExecutionError::InvalidInvocation(
                DescribeError::SealedEvidenceMismatch
            ))
        ));
        assert_eq!(
            bound_executions
                .lock()
                .expect("bound executions lock")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn execute_bound_rejects_crossed_permits_from_two_valid_calls_before_adapter_effect() {
        let bind_count = Arc::new(AtomicUsize::new(0));
        let bound_executions = Arc::new(Mutex::new(Vec::new()));
        let mut builder = ToolRegistryBuilder::default();
        builder
            .register(Arc::new(BindingTool {
                name: "inspect",
                bind_count,
                bound_executions: bound_executions.clone(),
            }))
            .expect("register binding tool");
        let registry = builder.build();
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");
        let first = registry
            .bind(
                &call("call-a", "inspect", json!({"path": "alpha"})),
                "flow-a",
                &workspace,
            )
            .await
            .expect("bind first valid call");
        let second = registry
            .bind(
                &call("call-b", "inspect", json!({"path": "beta"})),
                "flow-b",
                &workspace,
            )
            .await
            .expect("bind second valid call");
        let first = crate::approval::authority::ExecutableGrant::for_test(first, "grant-a")
            .into_authorized_bound();
        let second = crate::approval::authority::ExecutableGrant::for_test(second, "grant-b")
            .into_authorized_bound();
        let (crossed_first, crossed_second) =
            AuthorizedBoundInvocation::swap_permits_for_test(first, second);

        for crossed in [crossed_first, crossed_second] {
            assert!(matches!(
                registry
                    .execute_bound(crossed, CancellationToken::new(), Arc::new(|_| {}))
                    .await,
                Err(BoundExecutionError::InvalidInvocation(
                    DescribeError::ExecutionPermitMismatch
                ))
            ));
        }
        assert!(
            bound_executions
                .lock()
                .expect("bound executions lock")
                .is_empty(),
            "a crossed permit reached the app adapter"
        );
    }

    #[tokio::test]
    async fn deserialized_evidence_cannot_mint_a_restart_execution_seal() {
        fn registry(
            bind_count: Arc<AtomicUsize>,
            bound_executions: Arc<Mutex<Vec<Value>>>,
        ) -> ToolRegistry {
            let mut builder = ToolRegistryBuilder::default();
            builder
                .register(Arc::new(BindingTool {
                    name: "inspect",
                    bind_count,
                    bound_executions,
                }))
                .expect("register binding tool");
            builder.build()
        }

        let original = registry(
            Arc::new(AtomicUsize::new(0)),
            Arc::new(Mutex::new(Vec::new())),
        );
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");
        let sealed = original
            .bind(
                &call("call-1", "inspect", json!({"path": "alpha"})),
                "flow-1",
                &workspace,
            )
            .await
            .expect("bind invocation");
        let encoded = serde_json::to_vec(sealed.invocation()).expect("serialize evidence");
        drop(sealed);
        drop(original);

        let execution_count = Arc::new(Mutex::new(Vec::new()));
        let restarted = registry(Arc::new(AtomicUsize::new(0)), execution_count.clone());
        let invocation: BoundToolInvocation =
            serde_json::from_slice(&encoded).expect("deserialize durable evidence");
        let sealed_evidence_digest = invocation.evidence_digest().expect("valid evidence digest");
        let fabricated = SealedBoundToolInvocation {
            invocation,
            sealed_evidence_digest,
            flow_id: "flow-1".to_owned(),
            workspace: workspace.clone(),
            registry_seal: Arc::new(()),
            registration_seal: Arc::new(()),
        };

        let authorized = AuthorizedBoundInvocation::for_test(fabricated);
        let result = restarted
            .execute_bound(authorized, CancellationToken::new(), Arc::new(|_| {}))
            .await;
        assert!(matches!(
            result,
            Err(BoundExecutionError::InvalidInvocation(
                DescribeError::RegistryIdentityMismatch
            ))
        ));
        assert!(
            execution_count
                .lock()
                .expect("execution count lock")
                .is_empty()
        );
    }
}
