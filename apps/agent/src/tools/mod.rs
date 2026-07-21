//! Workspace tools and their execution boundary.

// This first T13 slice is wired into the runtime by the later executor/bash
// slice. Keep the independently tested public contracts warning-clean until
// those production call sites land.
#![allow(dead_code)]

#[cfg(target_os = "linux")]
pub mod fs;
pub mod shell_capture;
pub mod truncate;

use std::{
    collections::BTreeMap,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::provider::types::{ToolDefinition, UserContent, ValidatedToolArguments};

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
    #[error("tool protocol violation: {0}")]
    Protocol(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToolOutput {
    pub content: Vec<UserContent>,
    pub details: Value,
}

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

#[async_trait]
pub trait Tool: Send + Sync {
    fn def(&self) -> ToolDefinition;
    fn risk(&self) -> ToolRisk;
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
        self.tools.insert(
            name,
            RegisteredTool {
                definition,
                tool: Arc::new(GuardedTool { inner: tool }),
            },
        );
        Ok(())
    }

    pub fn build(self) -> ToolRegistry {
        ToolRegistry { tools: self.tools }
    }
}

#[derive(Clone, Default)]
pub struct ToolRegistry {
    tools: BTreeMap<String, RegisteredTool>,
}

#[derive(Clone)]
struct RegisteredTool {
    definition: ToolDefinition,
    tool: Arc<dyn Tool>,
}

impl ToolRegistry {
    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools.get(name).map(|entry| entry.tool.clone())
    }

    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.tools
            .values()
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
}
