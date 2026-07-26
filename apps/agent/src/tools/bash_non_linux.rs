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
    truncate::{TruncationOptions, TruncationResult, truncate_tail},
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
    broker_socket: Option<PathBuf>,
}

impl<'a> LowTrustLocalBash<'a> {
    pub fn new(workspace: PathBuf, artifact: &'a dyn ArtifactAppender) -> Self {
        Self {
            workspace,
            artifact,
            wall_timeout: DEFAULT_WALL_TIMEOUT,
            broker_socket: None,
        }
    }

    pub fn with_broker_socket(mut self, socket: PathBuf) -> Self {
        self.broker_socket = Some(socket);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn preflight_namespace_isolation() -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "namespace isolation is only supported on Linux",
        ))
    }

    pub async fn execute(
        &self,
        command: &str,
        execution_id: &str,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<BashExecutionResult, ToolError> {
        if cancel.is_cancelled() {
            return Ok(cancelled_before_spawn_result());
        }
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
        let inherited_fd_limit = inherited_fd_limit()?;
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
        configure_child_fd_sanitizer(&mut process, inherited_fd_limit);

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

fn cancelled_before_spawn_result() -> BashExecutionResult {
    to_execution_result(
        ShellCaptureResult {
            output: String::new(),
            truncation: truncate_tail("", TruncationOptions::default()),
            artifact_handle: None,
            observed_bytes: 0,
        },
        None,
        true,
        None,
    )
}

#[cfg(unix)]
fn inherited_fd_limit() -> Result<libc::rlim_t, ToolError> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
    if result != 0 {
        return Err(ToolError::Io(std::io::Error::last_os_error()));
    }

    // A descriptor opened before the soft limit was lowered may remain above
    // rlim_cur, so the sanitizer must use the hard allocation ceiling.
    #[cfg(target_vendor = "apple")]
    let inherited_fd_limit = if limit.rlim_max == libc::RLIM_INFINITY {
        apple_max_files_per_process()?
    } else {
        limit.rlim_max
    };
    #[cfg(not(target_vendor = "apple"))]
    let inherited_fd_limit = limit.rlim_max;
    let largest_representable_bound = (libc::c_int::MAX as libc::rlim_t) + 1;
    if inherited_fd_limit == libc::RLIM_INFINITY
        || inherited_fd_limit < 3
        || inherited_fd_limit > largest_representable_bound
    {
        return Err(ToolError::Protocol(format!(
            "RLIMIT_NOFILE hard limit is not a finite supported descriptor bound: {inherited_fd_limit}"
        )));
    }
    Ok(inherited_fd_limit)
}

#[cfg(all(unix, target_vendor = "apple"))]
fn apple_max_files_per_process() -> Result<libc::rlim_t, ToolError> {
    let mut mib = [libc::CTL_KERN, libc::KERN_MAXFILESPERPROC];
    let mut maximum: libc::c_int = 0;
    let mut maximum_size = std::mem::size_of::<libc::c_int>();
    let result = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            mib.len() as libc::c_uint,
            (&raw mut maximum).cast(),
            &mut maximum_size,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 {
        return Err(ToolError::Io(std::io::Error::last_os_error()));
    }
    if maximum_size != std::mem::size_of::<libc::c_int>() || maximum < 3 {
        return Err(ToolError::Protocol(
            "kern.maxfilesperproc did not return a valid descriptor bound".to_owned(),
        ));
    }
    Ok(maximum as libc::rlim_t)
}

#[cfg(unix)]
fn configure_child_fd_sanitizer(process: &mut Command, inherited_fd_limit: libc::rlim_t) {
    #[allow(unsafe_code)]
    unsafe {
        process.pre_exec(move || mark_inherited_fds_close_on_exec(inherited_fd_limit));
    }
}

#[cfg(unix)]
fn mark_inherited_fds_close_on_exec(inherited_fd_limit: libc::rlim_t) -> std::io::Result<()> {
    let mut fd = 3;
    while (fd as libc::rlim_t) < inherited_fd_limit {
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if flags < 0 {
            let error = errno();
            if error != libc::EBADF {
                return Err(std::io::Error::from_raw_os_error(error));
            }
        } else if flags & libc::FD_CLOEXEC == 0 {
            let result = unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) };
            if result < 0 {
                let error = errno();
                if error != libc::EBADF {
                    return Err(std::io::Error::from_raw_os_error(error));
                }
            }
        }
        if fd == libc::c_int::MAX {
            break;
        }
        fd += 1;
    }
    Ok(())
}

#[cfg(all(unix, any(target_vendor = "apple", target_os = "freebsd")))]
fn errno() -> libc::c_int {
    unsafe { *libc::__error() }
}

#[cfg(all(
    unix,
    any(
        target_os = "linux",
        target_os = "android",
        target_os = "dragonfly",
        target_os = "emscripten",
        target_os = "hurd",
        target_os = "redox"
    )
))]
fn errno() -> libc::c_int {
    unsafe { *libc::__errno_location() }
}

#[cfg(all(
    unix,
    any(target_os = "netbsd", target_os = "openbsd", target_os = "cygwin")
))]
fn errno() -> libc::c_int {
    unsafe { *libc::__errno() }
}

#[cfg(all(unix, any(target_os = "solaris", target_os = "illumos")))]
fn errno() -> libc::c_int {
    unsafe { *libc::___errno() }
}

#[cfg(all(unix, target_os = "haiku"))]
fn errno() -> libc::c_int {
    unsafe { *libc::_errnop() }
}

#[cfg(all(unix, target_os = "nto"))]
fn errno() -> libc::c_int {
    unsafe { *libc::__get_errno_ptr() }
}

#[cfg(all(unix, target_os = "aix"))]
fn errno() -> libc::c_int {
    unsafe { *libc::_Errno() }
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

#[cfg(all(test, unix))]
mod tests {
    use std::{
        io::Read,
        os::fd::{AsRawFd, FromRawFd, OwnedFd},
        process::Stdio,
    };

    use super::*;

    struct UnusedArtifacts;

    #[async_trait::async_trait]
    impl ArtifactAppender for UnusedArtifacts {
        async fn begin_tool_output(
            &self,
            _execution_id: &str,
            _initial_content: &[u8],
        ) -> Result<String, ToolError> {
            panic!("pre-cancelled execution must not begin an artifact")
        }

        async fn append_tool_output(
            &self,
            _handle: &str,
            _offset: u64,
            _content: &[u8],
        ) -> Result<(), ToolError> {
            panic!("pre-cancelled execution must not append an artifact")
        }

        async fn finish_tool_output(&self, _handle: &str) -> Result<(), ToolError> {
            panic!("pre-cancelled execution must not finish an artifact")
        }
    }

    #[tokio::test]
    async fn pre_cancelled_execution_returns_empty_without_spawning() {
        let workspace = std::env::temp_dir().join(format!(
            "sumi-missing-non-linux-bash-workspace-{}",
            uuid::Uuid::now_v7()
        ));
        let cancel = CancellationToken::new();
        cancel.cancel();
        let result = LowTrustLocalBash::new(workspace, &UnusedArtifacts)
            .execute(
                ": > must-not-exist",
                "non-linux-bash-pre-cancelled",
                cancel,
                Arc::new(|_| panic!("pre-cancelled execution must not emit updates")),
            )
            .await
            .expect("pre-cancel must win before workspace/spawn failure");
        assert_eq!(
            result,
            BashExecutionResult {
                output: String::new(),
                truncation: truncate_tail("", TruncationOptions::default()),
                artifact_handle: None,
                observed_bytes: 0,
                exit_code: None,
                cancelled: true,
                resource_limit: None,
            }
        );
    }

    #[tokio::test]
    async fn inherited_non_cloexec_fd_is_closed_only_at_exec() {
        let inherited_fd_limit = inherited_fd_limit().expect("finite inherited FD bound");
        let mut pipe_fds = [-1; 2];
        assert_eq!(unsafe { libc::pipe(pipe_fds.as_mut_ptr()) }, 0);
        let read_fd = unsafe { OwnedFd::from_raw_fd(pipe_fds[0]) };
        let write_fd = unsafe { OwnedFd::from_raw_fd(pipe_fds[1]) };
        assert_eq!(
            unsafe { libc::fcntl(write_fd.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0,
            "fixture must deliberately be inherited across exec"
        );

        let mut process = Command::new("bash");
        process
            .arg("-c")
            .arg(format!("printf inherited >&{}", write_fd.as_raw_fd()))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        configure_child_fd_sanitizer(&mut process, inherited_fd_limit);
        let mut child = process.spawn().expect("spawn bash fixture");
        drop(write_fd);
        let _status = child.wait().await.expect("wait for bash fixture");

        let mut inherited_bytes = Vec::new();
        std::fs::File::from(read_fd)
            .read_to_end(&mut inherited_bytes)
            .expect("read inheritance probe");
        assert!(
            inherited_bytes.is_empty(),
            "bash child observed a non-CLOEXEC inherited descriptor"
        );
    }

    #[test]
    fn exec_failure_remains_visible_after_fd_sanitizer() {
        let inherited_fd_limit = inherited_fd_limit().expect("finite inherited FD bound");
        let mut process = Command::new("/sumi-test/no-such-non-linux-executable");
        configure_child_fd_sanitizer(&mut process, inherited_fd_limit);
        let error = process
            .spawn()
            .expect_err("exec error must reach the parent through Command's errpipe");
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    }
}
