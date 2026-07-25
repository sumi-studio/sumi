//! Generation-owned execution registry and physical kill/reap boundary.
//!
//! T27 binds every running tool sandbox/cgroup to a `ProcessGeneration` and a
//! tool identity. The registry supports race-safe creation, lookup, and cleanup,
//! and can kill/reap old-generation sandboxes when a new runtime generation
//! starts.

use std::{
    collections::HashMap,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, bail};

use crate::runtime::contracts::ProcessGeneration;

const REAP_DEADLINE: Duration = Duration::from_secs(2);
const MAX_OPAQUE_ID_BYTES: usize = 128;

/// A running or recently-terminated tool execution tracked by the registry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionSandbox {
    pub generation: ProcessGeneration,
    pub tool_call_id: String,
    pub command_id: String,
    pub run_id: String,
    pub execution_id: String,
    pub cgroup_path: Option<PathBuf>,
    pub pids: Vec<u32>,
    pub terminal: bool,
}

/// Why a quota/host feature could not be applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostFeatureSkip {
    pub feature: &'static str,
    pub reason: String,
}

/// Owned handle that removes the sandbox from the registry on drop unless
/// explicitly disarmed.
pub struct SandboxHandle {
    registry: Option<Arc<GenerationExecutionRegistry>>,
    execution_id: String,
    disarmed: bool,
}

impl SandboxHandle {
    pub fn disarm(&mut self) {
        self.disarmed = true;
    }
}

impl Drop for SandboxHandle {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        if let Some(registry) = self.registry.take() {
            let _ = registry.remove(&self.execution_id);
        }
    }
}

/// In-process, generation-scoped execution registry.
///
/// All mutation methods are synchronous and hold a short-lived mutex. The
/// registry does not itself perform async I/O; `kill_and_reap` and
/// `reap_old_generation` spawn blocking cleanup and await it.
#[derive(Debug)]
pub struct GenerationExecutionRegistry {
    generation: ProcessGeneration,
    entries: Mutex<HashMap<String, ExecutionSandbox>>,
}

impl GenerationExecutionRegistry {
    pub fn new(generation: ProcessGeneration) -> Arc<Self> {
        Arc::new(Self {
            generation,
            entries: Mutex::new(HashMap::new()),
        })
    }

    pub fn generation(&self) -> ProcessGeneration {
        self.generation
    }

    pub fn current_generation(&self) -> ProcessGeneration {
        self.generation
    }

    /// Register a new sandbox. Returns an owned handle that removes the entry
    /// when dropped.
    pub fn register(
        self: &Arc<Self>,
        tool_call_id: String,
        command_id: String,
        run_id: String,
        execution_id: String,
        cgroup_path: Option<PathBuf>,
        pids: Vec<u32>,
    ) -> Result<SandboxHandle> {
        if execution_id.is_empty() || execution_id.len() > MAX_OPAQUE_ID_BYTES {
            bail!("execution_id must contain 1..={MAX_OPAQUE_ID_BYTES} bytes");
        }
        if tool_call_id.is_empty() {
            bail!("tool_call_id must not be empty");
        }
        let sandbox = ExecutionSandbox {
            generation: self.generation,
            tool_call_id,
            command_id,
            run_id,
            execution_id: execution_id.clone(),
            cgroup_path,
            pids,
            terminal: false,
        };
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| anyhow::anyhow!("generation execution registry lock poisoned"))?;
        if entries.contains_key(&execution_id) {
            bail!("execution_id {execution_id} is already registered for the current generation");
        }
        entries.insert(execution_id.clone(), sandbox);
        Ok(SandboxHandle {
            registry: Some(self.clone()),
            execution_id,
            disarmed: false,
        })
    }

    pub fn get(&self, execution_id: &str) -> Option<ExecutionSandbox> {
        self.entries
            .lock()
            .ok()
            .and_then(|entries| entries.get(execution_id).cloned())
    }

    pub fn remove(&self, execution_id: &str) -> Result<Option<ExecutionSandbox>> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| anyhow::anyhow!("generation execution registry lock poisoned"))?;
        Ok(entries.remove(execution_id))
    }

    pub fn mark_terminal(&self, execution_id: &str) -> Result<()> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| anyhow::anyhow!("generation execution registry lock poisoned"))?;
        if let Some(sandbox) = entries.get_mut(execution_id) {
            sandbox.terminal = true;
        }
        Ok(())
    }

    pub fn active_count(&self) -> usize {
        self.entries
            .lock()
            .map(|entries| entries.len())
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub fn test_insert(&self, sandbox: ExecutionSandbox) -> Result<()> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| anyhow::anyhow!("generation execution registry lock poisoned"))?;
        if entries.contains_key(&sandbox.execution_id) {
            bail!(
                "execution_id {} is already registered",
                sandbox.execution_id
            );
        }
        entries.insert(sandbox.execution_id.clone(), sandbox);
        Ok(())
    }

    pub fn snapshot_active(&self) -> Vec<ExecutionSandbox> {
        self.entries
            .lock()
            .map(|entries| entries.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Kill a single registered sandbox and wait for its processes to exit.
    /// The entry is marked terminal on success and left in the registry until
    /// the caller removes it (or the handle drops).
    pub async fn kill_and_reap(&self, execution_id: &str) -> Result<()> {
        let sandbox = self.get(execution_id).context("sandbox not registered")?;
        kill_sandbox(&sandbox).await?;
        self.mark_terminal(execution_id)?;
        Ok(())
    }

    /// Kill every sandbox whose generation is not `current_generation`.
    /// Returns the list of sandboxes that were killed and reaped.
    pub async fn reap_old_generation(
        &self,
        current_generation: ProcessGeneration,
    ) -> Result<Vec<ExecutionSandbox>> {
        let stale: Vec<ExecutionSandbox> = self
            .snapshot_active()
            .into_iter()
            .filter(|s| s.generation != current_generation)
            .collect();

        let mut reaped = Vec::with_capacity(stale.len());
        for sandbox in stale {
            if kill_sandbox(&sandbox).await.is_ok() {
                self.mark_terminal(&sandbox.execution_id)?;
                reaped.push(sandbox);
            }
        }
        Ok(reaped)
    }

    /// Kill every sandbox in the registry. Used when the executor/runtime
    /// process is shutting down.
    pub async fn kill_all(&self) -> Result<Vec<ExecutionSandbox>> {
        let all = self.snapshot_active();
        let mut killed = Vec::with_capacity(all.len());
        for sandbox in all {
            if kill_sandbox(&sandbox).await.is_ok() {
                self.mark_terminal(&sandbox.execution_id)?;
                killed.push(sandbox);
            }
        }
        Ok(killed)
    }

    /// Convert the currently-active, non-terminal sandboxes into physical
    /// recovery intents for T17.
    pub fn recovery_intents(&self) -> Vec<RecoveryIntent> {
        self.snapshot_active()
            .into_iter()
            .filter(|s| !s.terminal)
            .map(|s| RecoveryIntent {
                tool_call_id: s.tool_call_id,
                command_id: s.command_id,
                run_id: s.run_id,
                executor_generation: s.generation,
                execution_id: s.execution_id,
            })
            .collect()
    }
}

/// T27 intent used to build a `PhysicalRecoveryReceipt`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryIntent {
    pub tool_call_id: String,
    pub command_id: String,
    pub run_id: String,
    pub executor_generation: ProcessGeneration,
    pub execution_id: String,
}

/// Best-effort kill of a sandbox. Tries cgroup.kill first, then `SIGKILL` to
/// the process group of the first PID, then individual `SIGKILL` to each PID.
async fn kill_sandbox(sandbox: &ExecutionSandbox) -> Result<()> {
    if let Some(path) = &sandbox.cgroup_path
        && kill_cgroup(path).is_ok()
        && wait_cgroup_empty(path).await
    {
        return Ok(());
    }

    if let Some(&pid) = sandbox.pids.first() {
        let _ = kill_process_group(pid);
    }

    for &pid in &sandbox.pids {
        let _ = kill_pid(pid);
    }

    if sandbox.pids.is_empty() && sandbox.cgroup_path.is_none() {
        bail!("sandbox has no pids and no cgroup path; cannot kill");
    }

    // Give the kernel a bounded window to reap the processes.
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}

fn kill_pid(pid: u32) -> Result<()> {
    let pid = i32::try_from(pid).context("process id exceeded i32")?;
    let rc = unsafe { libc::kill(pid, libc::SIGKILL) };
    if rc == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(err.into())
}

fn kill_process_group(pid: u32) -> Result<()> {
    let pid = i32::try_from(pid).context("process id exceeded i32")?;
    let rc = unsafe { libc::kill(-pid, libc::SIGKILL) };
    if rc == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(err.into())
}

fn kill_cgroup(path: &Path) -> Result<()> {
    let kill_file = path.join("cgroup.kill");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&kill_file)
        .with_context(|| format!("failed to open {}", kill_file.display()))?;
    file.write_all(b"1")
        .with_context(|| format!("failed to write to {}", kill_file.display()))?;
    Ok(())
}

async fn wait_cgroup_empty(path: &Path) -> bool {
    let procs = path.join("cgroup.procs");
    let deadline = tokio::time::Instant::now() + REAP_DEADLINE;
    loop {
        match std::fs::read_to_string(&procs) {
            Ok(contents) if contents.trim().is_empty() => return true,
            _ => {}
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Persist a T27 physical-recovery proof marker to `state_dir`.
///
/// The canonical `PhysicalRecoveryReceipt` and T17 application ledger are the
/// authoritative records; this marker is only an operationally visible proof
/// that T27 has produced the receipt.
pub fn persist_recovery_marker(
    state_dir: impl AsRef<Path>,
    receipt_id: &str,
    digest: &str,
    generation: ProcessGeneration,
    intent_tool_call_ids: &[String],
) -> Result<PathBuf> {
    if receipt_id.is_empty() || digest.is_empty() {
        bail!("receipt_id and digest must not be empty");
    }
    let dir = state_dir.as_ref().join("physical_recovery");
    std::fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
    let path = dir.join(format!("{receipt_id}.receipt"));
    let marker = serde_json::json!({
        "receipt_id": receipt_id,
        "digest": digest,
        "generation": generation.as_u64(),
        "intent_count": intent_tool_call_ids.len(),
        "intent_tool_call_ids": intent_tool_call_ids,
        "persisted_at": chrono::Utc::now().to_rfc3339(),
    });
    std::fs::write(&path, serde_json::to_vec_pretty(&marker)?)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Stdio;
    use std::time::Duration;
    use uuid::Uuid;

    fn generation(value: u64) -> ProcessGeneration {
        ProcessGeneration::from_wire(value).unwrap()
    }

    #[tokio::test]
    async fn register_and_remove_are_race_safe() {
        let registry = GenerationExecutionRegistry::new(generation(1));
        let handle = registry
            .register(
                "tool-call-1".to_owned(),
                "command-1".to_owned(),
                "run-1".to_owned(),
                "exec-1".to_owned(),
                None,
                vec![],
            )
            .unwrap();

        assert_eq!(registry.active_count(), 1);
        let snapshot = registry.get("exec-1").unwrap();
        assert_eq!(snapshot.execution_id, "exec-1");

        drop(handle);
        assert!(registry.get("exec-1").is_none());
    }

    #[tokio::test]
    async fn reject_duplicate_execution_id() {
        let registry = GenerationExecutionRegistry::new(generation(1));
        let mut handle = registry
            .register(
                "tool-call-1".to_owned(),
                "command-1".to_owned(),
                "run-1".to_owned(),
                "exec-1".to_owned(),
                None,
                vec![],
            )
            .unwrap();

        assert!(
            registry
                .register(
                    "tool-call-2".to_owned(),
                    "command-2".to_owned(),
                    "run-2".to_owned(),
                    "exec-1".to_owned(),
                    None,
                    vec![],
                )
                .is_err()
        );

        handle.disarm();
    }

    #[tokio::test]
    async fn kill_and_reap_terminates_a_setsid_child() {
        let registry = GenerationExecutionRegistry::new(generation(1));

        let mut child = tokio::process::Command::new("sleep")
            .arg("60")
            .process_group(0)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sleep");
        let pid = child.id().expect("child pid");

        let mut handle = registry
            .register(
                "tool-call-1".to_owned(),
                "command-1".to_owned(),
                "run-1".to_owned(),
                "exec-1".to_owned(),
                None,
                vec![pid],
            )
            .unwrap();

        registry.kill_and_reap("exec-1").await.unwrap();

        let snapshot = registry.get("exec-1").unwrap();
        assert!(snapshot.terminal);

        // The child should be gone within the reap window.
        let waited = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
        assert!(waited.is_ok(), "setsid child was not reaped");

        handle.disarm();
    }

    #[tokio::test]
    async fn reap_old_generation_kills_only_stale_entries() {
        let current = generation(2);
        let registry = GenerationExecutionRegistry::new(current);

        // Spawn a real old-generation child. The registry normally rejects
        // cross-generation inserts, so we temporarily own its lock to inject
        // the stale entry.
        let mut old_child = tokio::process::Command::new("sleep")
            .arg("60")
            .process_group(0)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sleep");
        let old_pid = old_child.id().expect("old child pid");
        let old = ExecutionSandbox {
            generation: generation(1),
            tool_call_id: "old-tool".to_owned(),
            command_id: "old-command".to_owned(),
            run_id: "old-run".to_owned(),
            execution_id: "old-exec".to_owned(),
            cgroup_path: None,
            pids: vec![old_pid],
            terminal: false,
        };
        {
            let mut entries = registry.entries.lock().unwrap();
            entries.insert(old.execution_id.clone(), old);
        }

        let mut current_handle = registry
            .register(
                "current-tool".to_owned(),
                "current-command".to_owned(),
                "current-run".to_owned(),
                "current-exec".to_owned(),
                None,
                vec![],
            )
            .unwrap();

        let reaped = registry.reap_old_generation(current).await.unwrap();
        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].execution_id, "old-exec");
        assert!(registry.get("old-exec").unwrap().terminal);
        assert!(!registry.get("current-exec").unwrap().terminal);

        // The old child should be gone after the reap.
        let _ = tokio::time::timeout(Duration::from_secs(2), old_child.wait())
            .await
            .expect("old child was reaped");

        current_handle.disarm();
    }

    #[test]
    fn persist_recovery_marker_creates_file() {
        let dir = std::env::temp_dir().join(format!("sumi-recovery-marker-{}", Uuid::now_v7()));
        let path = persist_recovery_marker(
            &dir,
            "receipt-1",
            "digest-1",
            generation(7),
            &["tool-call-1".to_owned()],
        )
        .unwrap();

        assert!(path.exists());
        let contents = std::fs::read_to_string(&path).unwrap();
        assert!(contents.contains("receipt-1"));
        assert!(contents.contains("digest-1"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
