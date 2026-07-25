//! T27 fault-injection acceptance: typed resource quotas and generation-owned
//! execution cleanup.
//!
//! These tests exercise the tool/runtime boundaries from the same crate so they
//! can use the internal `LowTrustLocalBash`, `ResourceQuotaPolicy`, and
//! `WorkspaceFs` types. Hosts without the required cgroup controllers are
//! skipped or fall back to rlimits.

#[cfg(test)]
mod tests {
    use std::{path::Path, path::PathBuf, process::Stdio, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use tokio_util::sync::CancellationToken;

    use crate::runtime::{
        contracts::ProcessGeneration,
        execution_registry::{ExecutionSandbox, GenerationExecutionRegistry},
    };
    use crate::tools::{
        ResourceLimit, ToolError,
        bash::LowTrustLocalBash,
        fs::WorkspaceFs,
        quota::{InMemoryDiskQuota, ResourceQuotaPolicy},
        shell_capture::ArtifactAppender,
    };

    struct NoopArtifacts;

    #[async_trait]
    impl ArtifactAppender for NoopArtifacts {
        async fn begin_tool_output(
            &self,
            _execution_id: &str,
            _initial_content: &[u8],
        ) -> Result<String, ToolError> {
            Ok("artifact://conversation/tool-output/none".to_owned())
        }

        async fn append_tool_output(
            &self,
            _handle: &str,
            _offset: u64,
            _content: &[u8],
        ) -> Result<(), ToolError> {
            Ok(())
        }

        async fn finish_tool_output(&self, _handle: &str) -> Result<(), ToolError> {
            Ok(())
        }
    }

    fn temp_workspace() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("sumi-t27-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).expect("create temp workspace");
        dir
    }

    fn cancel() -> CancellationToken {
        CancellationToken::new()
    }

    fn no_update() -> Arc<dyn Fn(serde_json::Value) + Send + Sync> {
        Arc::new(|_| {})
    }

    #[tokio::test]
    async fn cpu_time_quota_kills_bash_and_returns_typed_limit() {
        let workspace = temp_workspace();
        let policy = ResourceQuotaPolicy::new()
            .with_wall_time(10)
            .with_cpu_time_seconds(1);
        let result = LowTrustLocalBash::new(workspace, &NoopArtifacts)
            .with_quota_policy(policy)
            .execute("while :; do :; done", "cpu-time", cancel(), no_update())
            .await
            .expect("bash execution completed");

        assert!(
            result.exit_code.is_none(),
            "bash must be killed by the CPU time limit, not exit cleanly: {result:?}"
        );
        assert!(!result.cancelled);
        assert_eq!(
            result.resource_limit,
            Some(ResourceLimit::CpuTime { limit_seconds: 1 })
        );
    }

    #[tokio::test]
    async fn cpu_throttle_quota_kills_bash_and_returns_typed_limit() {
        let workspace = temp_workspace();
        // 1% of a single CPU. The bash process will be throttled and then
        // killed by the wall-time watchdog. Cgroup evidence (`nr_throttled`)
        // lets us classify the result as `CpuThrottle` rather than `WallTime`.
        let policy = ResourceQuotaPolicy::new()
            .with_wall_time(2)
            .with_cpu_throttle_percent(1);
        let result = LowTrustLocalBash::new(workspace, &NoopArtifacts)
            .with_quota_policy(policy)
            .execute("while :; do :; done", "cpu-throttle", cancel(), no_update())
            .await
            .expect("bash execution completed");

        assert!(
            result.exit_code.is_none(),
            "bash must be killed by the wall-time throttle watchdog: {result:?}"
        );
        assert!(!result.cancelled);
        assert_eq!(
            result.resource_limit,
            Some(ResourceLimit::CpuThrottle { limit: 1_000 })
        );
    }

    #[tokio::test]
    async fn memory_quota_kills_exec_child_and_returns_typed_limit() {
        let workspace = temp_workspace();
        let policy = ResourceQuotaPolicy::new()
            .with_wall_time(10)
            .with_memory_bytes(50 * 1024 * 1024);
        let result = LowTrustLocalBash::new(workspace, &NoopArtifacts)
            .with_quota_policy(policy)
            .execute(
                "exec python3 -c 'x=bytearray(100*1024*1024); [x.__setitem__(i,1) for i in range(0,len(x),4096)]'",
                "memory-limit",
                cancel(),
                no_update(),
            )
            .await
            .expect("bash execution completed");

        assert!(
            result.exit_code.is_none(),
            "memory-limited command must be killed, not exit cleanly: {result:?}"
        );
        assert!(!result.cancelled);
        assert_eq!(
            result.resource_limit,
            Some(ResourceLimit::Memory {
                limit: 50 * 1024 * 1024,
            })
        );
    }

    #[tokio::test]
    async fn pids_quota_blocks_fork_and_returns_typed_limit() {
        let workspace = temp_workspace();
        let policy = ResourceQuotaPolicy::new()
            .with_wall_time(2)
            .with_pids_max(3);
        let result = LowTrustLocalBash::new(workspace, &NoopArtifacts)
            .with_quota_policy(policy)
            .execute(
                "for i in 1 2 3 4 5; do sleep 60 & done; wait",
                "pids-limit",
                cancel(),
                no_update(),
            )
            .await
            .expect("bash execution completed");

        assert!(!result.cancelled);
        assert_eq!(
            result.resource_limit,
            Some(ResourceLimit::Pids { limit: 3 }),
            "pids limit must classify as Pids: {result:?}"
        );
    }

    #[tokio::test]
    async fn disk_bytes_quota_kills_bash_and_returns_typed_limit() {
        let workspace = temp_workspace();
        // In-memory backend is required to satisfy the fail-closed check; the
        // actual enforcement for bash uses RLIMIT_FSIZE.
        let backend = InMemoryDiskQuota::new(1024 * 1024, 10);
        let policy = ResourceQuotaPolicy::new()
            .with_wall_time(10)
            .with_disk_bytes(1024 * 1024)
            .with_disk_quota_backend(backend);
        let result = LowTrustLocalBash::new(workspace, &NoopArtifacts)
            .with_quota_policy(policy)
            .execute(
                "dd if=/dev/zero of=big bs=1M count=2",
                "disk-bytes",
                cancel(),
                no_update(),
            )
            .await
            .expect("bash execution completed");

        assert!(
            result.exit_code.is_none(),
            "bash must be killed when it exceeds the per-file size cap: {result:?}"
        );
        assert!(!result.cancelled);
        if let Some(ResourceLimit::DiskBytes { limit, .. }) = result.resource_limit {
            assert_eq!(limit, 1024 * 1024);
        } else {
            panic!("expected DiskBytes limit: {result:?}");
        }
    }

    #[tokio::test]
    async fn wall_time_quota_still_returns_typed_limit() {
        let workspace = temp_workspace();
        let policy = ResourceQuotaPolicy::new().with_wall_time(1);
        let result = LowTrustLocalBash::new(workspace, &NoopArtifacts)
            .with_quota_policy(policy)
            .execute("sleep 30", "wall-time", cancel(), no_update())
            .await
            .expect("bash execution completed");

        assert!(!result.cancelled);
        assert_eq!(
            result.resource_limit,
            Some(ResourceLimit::WallTime { limit_seconds: 1 })
        );
    }

    #[tokio::test]
    async fn workspace_disk_bytes_quota_returns_disk_bytes_limit() {
        let root = temp_workspace();
        let quota = InMemoryDiskQuota::new(10, 100);
        let fs = WorkspaceFs::open_with_disk_quota(&root, Some(quota)).unwrap();

        // First write fits; second write would exceed the byte cap.
        fs.write_file(Path::new("first"), &[1, 2, 3, 4, 5]).unwrap();
        let err = fs.write_file(Path::new("second"), &[0; 20]).unwrap_err();
        assert!(matches!(
            err,
            ToolError::ResourceLimit(ResourceLimit::DiskBytes { limit: 10, .. })
        ));
    }

    #[tokio::test]
    async fn workspace_disk_inode_quota_returns_disk_inode_limit() {
        let root = temp_workspace();
        let quota = InMemoryDiskQuota::new(1024 * 1024, 1);
        let fs = WorkspaceFs::open_with_disk_quota(&root, Some(quota)).unwrap();

        // First file consumes the single-inode budget.
        fs.write_file(Path::new("first"), b"hello").unwrap();
        let err = fs.write_file(Path::new("second"), b"world").unwrap_err();
        assert!(matches!(
            err,
            ToolError::ResourceLimit(ResourceLimit::DiskInodes { limit: 1, .. })
        ));
    }

    #[tokio::test]
    async fn cgroup_kill_reaps_setsid_descendant_before_workspace_mutation() {
        let workspace = temp_workspace();
        // Force a cgroup so that `cgroup.kill` can reap the whole tree,
        // including a `setsid` descendant that would otherwise be reparented
        // to init and continue running.
        let policy = ResourceQuotaPolicy::new()
            .with_wall_time(30)
            .with_cpu_throttle_percent(10)
            .with_pids_max(64);
        let marker = workspace.join("leaked");
        let command = format!(
            "(setsid sh -c 'sleep 0.5; touch {}' &) ; sleep 30",
            marker.display()
        );

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let _abort = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            cancel_clone.cancel();
        });

        let result = LowTrustLocalBash::new(workspace, &NoopArtifacts)
            .with_quota_policy(policy)
            .execute(&command, "setsid-kill", cancel, no_update())
            .await
            .expect("bash execution completed");

        assert!(result.cancelled);
        // Give the detached descendant enough time to execute its mutation
        // plan if it survived the cgroup kill.
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert!(
            !marker.exists(),
            "setsid descendant mutated workspace after cgroup kill"
        );
    }

    #[tokio::test]
    async fn generation_execution_registry_reaps_old_generation_descendants() {
        let current = ProcessGeneration::from_wire(7).expect("valid generation");
        let registry = GenerationExecutionRegistry::new(current);

        let mut child = tokio::process::Command::new("sleep")
            .arg("60")
            .process_group(0)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn descendant");
        let pid = child.id().expect("child pid");

        // Inject a stale sandbox directly to simulate a descendant that
        // outlived its original generation.
        registry
            .test_insert(ExecutionSandbox {
                generation: ProcessGeneration::from_wire(6).unwrap(),
                tool_call_id: "stale-tool".to_owned(),
                command_id: "stale-command".to_owned(),
                run_id: "stale-run".to_owned(),
                execution_id: "stale-exec".to_owned(),
                cgroup_path: None,
                pids: vec![pid],
                terminal: false,
            })
            .unwrap();

        let reaped = registry
            .reap_old_generation(current)
            .await
            .expect("reap completed");
        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].execution_id, "stale-exec");

        let _ = tokio::time::timeout(Duration::from_secs(2), child.wait())
            .await
            .expect("stale descendant was reaped");
    }

    #[tokio::test]
    async fn recovered_running_execution_is_closed_once_and_not_rerun() {
        let generation = ProcessGeneration::from_wire(7).expect("valid generation");
        let registry = GenerationExecutionRegistry::new(generation);

        let mut child = tokio::process::Command::new("sleep")
            .arg("60")
            .process_group(0)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn descendant");
        let pid = child.id().expect("child pid");

        let mut handle = registry
            .register(
                "tool-call-1".to_owned(),
                "command-1".to_owned(),
                "run-1".to_owned(),
                "running-exec".to_owned(),
                None,
                vec![pid],
            )
            .unwrap();

        // Recovery scans produce exactly one physical recovery intent per
        // non-terminal running execution.
        let intents = registry.recovery_intents();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].execution_id, "running-exec");

        // Closing the intent (here via kill_all) must mark the execution
        // terminal so it cannot be emitted or retried again.
        let killed = registry.kill_all().await.expect("kill all");
        assert_eq!(killed.len(), 1);
        assert!(registry.get("running-exec").unwrap().terminal);
        assert!(registry.recovery_intents().is_empty());

        let _ = tokio::time::timeout(Duration::from_secs(2), child.wait())
            .await
            .expect("recovered child was reaped");

        handle.disarm();
    }
}
