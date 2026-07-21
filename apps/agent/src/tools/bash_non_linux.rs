//! Development-only non-Linux bash fallback.
//!
//! Supported Unix hosts retain the canonical merged output and bounded
//! reader/capture contract, but `child.kill()` cannot stop escaped descendants.
//! Non-Unix hosts fail closed because this module cannot construct the required
//! single stdout/stderr stream there.

use std::{path::PathBuf, sync::Arc, time::Duration};

#[cfg(unix)]
use std::{
    collections::VecDeque,
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
};

use serde::Serialize;
use serde_json::Value;
#[cfg(unix)]
use serde_json::json;
#[cfg(unix)]
use tokio::{
    process::{Child, Command},
    sync::mpsc,
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;

#[cfg(unix)]
use super::shell_capture::{
    OUTPUT_QUEUE_CAPACITY, ShellCapture, copy_bounded_chunks, output_limit_if_reached,
};
#[cfg(unix)]
use super::unix_pipe::merged_output_pipe;
use super::{
    ResourceLimit, ToolError,
    shell_capture::{ArtifactAppender, ShellCaptureResult},
    truncate::TruncationResult,
};

pub const DEFAULT_WALL_TIMEOUT: Duration = Duration::from_secs(120);
#[cfg(unix)]
const STOPPED_PIPE_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BashExecutionResult {
    pub output: String,
    pub truncation: TruncationResult,
    pub artifact_handle: Option<String>,
    pub observed_bytes: u64,
    pub exit_code: Option<i32>,
    pub cancelled: bool,
    pub resource_limit: Option<ResourceLimit>,
}

pub struct LowTrustLocalBash<'a> {
    workspace: PathBuf,
    artifact: &'a dyn ArtifactAppender,
    wall_timeout: Duration,
}

impl<'a> LowTrustLocalBash<'a> {
    pub fn new(workspace: PathBuf, artifact: &'a dyn ArtifactAppender) -> Self {
        Self {
            workspace,
            artifact,
            wall_timeout: DEFAULT_WALL_TIMEOUT,
        }
    }

    pub async fn execute(
        &self,
        command: &str,
        execution_id: &str,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<BashExecutionResult, ToolError> {
        #[cfg(unix)]
        {
            self.execute_unix(command, execution_id, cancel, on_update)
                .await
        }
        #[cfg(not(unix))]
        {
            let _ = (command, execution_id, cancel, on_update);
            Err(ToolError::Protocol(
                "local bash is unsupported on non-Unix hosts: a single chronological stdout/stderr pipe and the required post-stop reader drain are unavailable"
                    .to_owned(),
            ))
        }
    }

    #[cfg(unix)]
    async fn execute_unix(
        &self,
        command: &str,
        execution_id: &str,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<BashExecutionResult, ToolError> {
        tracing::warn!(
            target: "sumi_agent::tools",
            "starting non-Linux low-trust bash; child.kill() cannot stop escaped descendants"
        );
        let (output_read, output_write) = merged_output_pipe()?;
        let output_stderr = output_write.try_clone()?;
        let mut process = Command::new("bash");
        process
            .arg("-c")
            .arg(command)
            .current_dir(&self.workspace)
            .env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .env("HOME", &self.workspace)
            .env("LANG", "C.UTF-8")
            .stdin(Stdio::null())
            .stdout(Stdio::from(output_write))
            .stderr(Stdio::from(output_stderr));

        let mut child = process.spawn()?;
        drop(process);
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(OUTPUT_QUEUE_CAPACITY);
        let pipe_observed_bytes = Arc::new(AtomicU64::new(0));
        let output_quota = CancellationToken::new();
        let mut output_task = Some(tokio::spawn(copy_bounded_chunks(
            output_read,
            tx,
            pipe_observed_bytes.clone(),
            output_quota.clone(),
        )));
        let mut capture = ShellCapture::new(execution_id, self.artifact);
        let deadline = Instant::now() + self.wall_timeout;
        let mut exit_status = None;
        let mut cancelled = false;
        let mut resource_limit = None;
        let mut streams_open = true;

        loop {
            if exit_status.is_some() && !streams_open {
                break;
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    cancelled = true;
                    exit_status = Some(kill_and_reap(&mut child).await?);
                    break;
                }
                _ = output_quota.cancelled() => {
                    resource_limit =
                        output_limit_if_reached(pipe_observed_bytes.load(Ordering::Acquire));
                    exit_status = Some(kill_and_reap(&mut child).await?);
                    break;
                }
                _ = tokio::time::sleep_until(deadline) => {
                    resource_limit = Some(ResourceLimit::WallTime {
                        limit_seconds: self.wall_timeout.as_secs(),
                    });
                    exit_status = Some(kill_and_reap(&mut child).await?);
                    break;
                }
                chunk = rx.recv(), if streams_open => {
                    let Some(chunk) = chunk else {
                        streams_open = false;
                        continue;
                    };
                    let recorded = capture.record_chunk(&chunk)?;
                    let push = capture.archive_recorded(recorded);
                    tokio::pin!(push);
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            cancelled = true;
                            exit_status = Some(kill_and_reap(&mut child).await?);
                            break;
                        }
                        _ = output_quota.cancelled() => {
                            resource_limit =
                                output_limit_if_reached(pipe_observed_bytes.load(Ordering::Acquire));
                            exit_status = Some(kill_and_reap(&mut child).await?);
                            break;
                        }
                        _ = tokio::time::sleep_until(deadline) => {
                            resource_limit = Some(ResourceLimit::WallTime {
                                limit_seconds: self.wall_timeout.as_secs(),
                            });
                            exit_status = Some(kill_and_reap(&mut child).await?);
                            break;
                        }
                        result = &mut push => match result {
                            Ok(text) => on_update(json!({"output": text})),
                            Err(ToolError::ResourceLimit(limit)) => {
                                resource_limit = Some(limit);
                                exit_status = Some(kill_and_reap(&mut child).await?);
                                break;
                            }
                            Err(error) => {
                                let _ = kill_and_reap(&mut child).await;
                                return Err(error);
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)), if exit_status.is_none() => {
                    exit_status = child.try_wait()?;
                }
            }
        }

        if exit_status.is_none() {
            exit_status = Some(child.wait().await?);
        }

        let mut output_task_result = None;
        if resource_limit.is_some() || cancelled {
            let drain_deadline = Instant::now() + STOPPED_PIPE_DRAIN_TIMEOUT;
            let mut stopped_chunks = VecDeque::new();
            let mut stopped_bytes = 0u64;
            loop {
                let chunk = match timeout_at(drain_deadline, rx.recv()).await {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        let task = output_task.take().ok_or_else(|| {
                            ToolError::Protocol("merged output task was already joined".to_owned())
                        })?;
                        task.abort();
                        output_task_result = Some(task.await);
                        while let Ok(chunk) = rx.try_recv() {
                            add_stopped_bytes(&mut stopped_bytes, &chunk)?;
                            stopped_chunks.push_back(capture.record_chunk(&chunk)?);
                        }
                        break;
                    }
                };
                let Some(chunk) = chunk else {
                    break;
                };
                add_stopped_bytes(&mut stopped_bytes, &chunk)?;
                stopped_chunks.push_back(capture.record_chunk(&chunk)?);
            }

            for recorded in stopped_chunks {
                match capture.archive_recorded(recorded).await {
                    Ok(text) => on_update(json!({"output": text})),
                    Err(ToolError::ResourceLimit(limit)) => {
                        resource_limit = Some(limit);
                    }
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            "failed to archive a queued bash output chunk after process stop"
                        );
                    }
                }
            }
            if let Some(limit) =
                output_limit_if_reached(pipe_observed_bytes.load(Ordering::Acquire))
            {
                resource_limit = Some(limit);
            }
        } else {
            debug_assert!(!streams_open);
        }

        let output_task_result = match output_task_result {
            Some(result) => result,
            None => {
                output_task
                    .take()
                    .ok_or_else(|| {
                        ToolError::Protocol("merged output task was already joined".to_owned())
                    })?
                    .await
            }
        };
        match output_task_result {
            Ok(result) => {
                let _observed_bytes = result?;
            }
            Err(error) if error.is_cancelled() && (resource_limit.is_some() || cancelled) => {}
            Err(error) => {
                return Err(ToolError::Protocol(format!(
                    "merged output capture task failed: {error}"
                )));
            }
        }

        let capture = if cancelled {
            capture.finish_after_abort().await
        } else if resource_limit.is_some() {
            capture.finish_after_limit().await?
        } else {
            capture.finish().await?
        };
        let result = to_execution_result(
            capture,
            exit_status.and_then(|status| status.code()),
            cancelled,
            resource_limit,
        );
        let pipe_observed = pipe_observed_bytes.load(Ordering::Acquire);
        if result.observed_bytes != pipe_observed {
            return Err(ToolError::Protocol(format!(
                "terminal bash byte accounting omitted reader-observed bytes: captured={} reader={pipe_observed}",
                result.observed_bytes
            )));
        }
        Ok(result)
    }
}

#[cfg(unix)]
fn add_stopped_bytes(total: &mut u64, chunk: &[u8]) -> Result<(), ToolError> {
    *total =
        total
            .checked_add(u64::try_from(chunk.len()).map_err(|_| {
                ToolError::Protocol("stopped bash output length overflow".to_owned())
            })?)
            .ok_or_else(|| ToolError::Protocol("stopped bash output length overflow".to_owned()))?;
    if *total > super::shell_capture::COMMAND_OUTPUT_LIMIT_BYTES {
        return Err(ToolError::Protocol(
            "stopped bash output exceeded the pipe-reader hard bound".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
async fn kill_and_reap(child: &mut Child) -> Result<std::process::ExitStatus, ToolError> {
    match child.kill().await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => {}
        Err(error) => return Err(ToolError::Io(error)),
    }
    child.wait().await.map_err(ToolError::Io)
}

fn to_execution_result(
    capture: ShellCaptureResult,
    exit_code: Option<i32>,
    cancelled: bool,
    resource_limit: Option<ResourceLimit>,
) -> BashExecutionResult {
    BashExecutionResult {
        output: capture.output,
        truncation: capture.truncation,
        artifact_handle: capture.artifact_handle,
        observed_bytes: capture.observed_bytes,
        exit_code: if cancelled || resource_limit.is_some() {
            None
        } else {
            exit_code
        },
        cancelled,
        resource_limit,
    }
}
