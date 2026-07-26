//! Development-only low-trust bash execution harness.

#![cfg(target_os = "linux")]

use std::{
    collections::VecDeque,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    process::Command,
    sync::mpsc,
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;

use super::{
    ResourceLimit, ToolError,
    shell_capture::{
        ArtifactAppender, COMMAND_OUTPUT_LIMIT_BYTES, OUTPUT_QUEUE_CAPACITY, ShellCapture,
        ShellCaptureResult, copy_bounded_chunks, output_limit_if_reached,
    },
    truncate::{TruncationOptions, TruncationResult, truncate_tail},
    unix_pipe::merged_output_pipe,
};

pub const DEFAULT_WALL_TIMEOUT: Duration = Duration::from_secs(120);
const STOPPED_PIPE_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BashExecutionResult {
    pub output: String,
    pub truncation: TruncationResult,
    pub artifact_handle: Option<String>,
    pub observed_bytes: u64,
    pub exit_code: Option<i32>,
    pub cancelled: bool,
    pub resource_limit: Option<ResourceLimit>,
}

impl BashExecutionResult {
    pub(crate) fn is_consistent(&self) -> bool {
        // Sanitization may remove raw control bytes or expand each invalid byte
        // to one three-byte U+FFFD, but it cannot escape that raw-byte envelope.
        let sanitized_bytes_bound = usize::try_from(self.observed_bytes)
            .ok()
            .and_then(|observed| observed.checked_mul('\u{fffd}'.len_utf8()));
        let sanitized_lines_are_bounded = usize::try_from(self.observed_bytes)
            .is_ok_and(|observed| self.truncation.total_lines <= observed);
        if self.output != self.truncation.content
            || self.output.len() > super::truncate::DEFAULT_MAX_BYTES
            || self.observed_bytes > COMMAND_OUTPUT_LIMIT_BYTES
            || sanitized_bytes_bound.is_none_or(|bound| self.truncation.total_bytes > bound)
            || !sanitized_lines_are_bounded
            || self.truncation.max_lines != super::truncate::DEFAULT_MAX_LINES
            || self.truncation.max_bytes != super::truncate::DEFAULT_MAX_BYTES
            || !self
                .truncation
                .is_consistent(super::truncate::RetainedOutput::Tail)
            || (self.cancelled && self.resource_limit.is_some())
            || (self.resource_limit.is_some() && self.exit_code.is_some())
            || (self.observed_bytes == COMMAND_OUTPUT_LIMIT_BYTES
                && !(self.cancelled && self.resource_limit.is_none())
                && !matches!(self.resource_limit, Some(ResourceLimit::OutputBytes { .. })))
            || self
                .artifact_handle
                .as_deref()
                .is_some_and(|handle| !handle.starts_with("artifact://"))
        {
            return false;
        }

        match &self.resource_limit {
            Some(ResourceLimit::OutputBytes { observed, limit }) => {
                *observed == COMMAND_OUTPUT_LIMIT_BYTES
                    && *limit == COMMAND_OUTPUT_LIMIT_BYTES
                    && self.observed_bytes == COMMAND_OUTPUT_LIMIT_BYTES
            }
            _ => true,
        }
    }
}

pub struct LowTrustLocalBash<'a> {
    workspace: PathBuf,
    artifact: &'a dyn ArtifactAppender,
    broker_socket: Option<PathBuf>,
    wall_timeout: Duration,
    #[cfg(test)]
    cancel_stop_delay: Duration,
    #[cfg(test)]
    force_close_range_fallback: bool,
}

impl<'a> LowTrustLocalBash<'a> {
    pub fn new(workspace: PathBuf, artifact: &'a dyn ArtifactAppender) -> Self {
        Self {
            workspace,
            artifact,
            broker_socket: None,
            wall_timeout: DEFAULT_WALL_TIMEOUT,
            #[cfg(test)]
            cancel_stop_delay: Duration::ZERO,
            #[cfg(test)]
            force_close_range_fallback: false,
        }
    }

    pub fn with_broker_socket(mut self, socket: PathBuf) -> Self {
        self.broker_socket = Some(socket);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_timeout(mut self, wall_timeout: Duration) -> Self {
        self.wall_timeout = wall_timeout;
        self
    }

    #[cfg(test)]
    pub(super) fn with_cancel_stop_delay(mut self, delay: Duration) -> Self {
        self.cancel_stop_delay = delay;
        self
    }

    #[cfg(test)]
    fn with_forced_close_range_fallback(mut self) -> Self {
        self.force_close_range_fallback = true;
        self
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
        tracing::warn!(
            target: "sumi_agent::tools",
            "starting Linux low-trust local bash; process-group kill cannot stop setsid-escaped descendants"
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
        #[cfg(test)]
        let force_close_range_fallback = self.force_close_range_fallback;
        #[cfg(not(test))]
        let force_close_range_fallback = false;
        let enforce_broker_socket_isolation =
            std::env::var_os("SUMI_ENFORCE_BROKER_SOCKET_NAMESPACE_ISOLATION").is_some();
        configure_child_process(
            &mut process,
            inherited_fd_limit,
            force_close_range_fallback,
            self.broker_socket.as_deref(),
            &self.workspace,
            enforce_broker_socket_isolation,
        );

        process.kill_on_drop(true);
        let mut child = process.spawn()?;
        drop(process);
        let pid = child.id().ok_or_else(|| {
            ToolError::Protocol("spawned bash did not expose a process id".to_owned())
        })?;
        let mut process_group = ProcessGroupGuard::new(pid);
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(OUTPUT_QUEUE_CAPACITY);
        let pipe_observed_bytes = Arc::new(AtomicU64::new(0));
        let output_quota = CancellationToken::new();
        let mut output_task = AbortOnDropTask::new(tokio::spawn(copy_bounded_chunks(
            output_read,
            tx,
            pipe_observed_bytes.clone(),
            output_quota.clone(),
        )));
        let mut capture = ShellCapture::new(execution_id, self.artifact);
        let deadline = Instant::now() + self.wall_timeout;
        let mut wait = Box::pin(child.wait());
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
                    #[cfg(test)]
                    if !self.cancel_stop_delay.is_zero() {
                        tokio::time::sleep(self.cancel_stop_delay).await;
                    }
                    kill_process_group(pid)?;
                    break;
                }
                _ = output_quota.cancelled() => {
                    resource_limit =
                        output_limit_if_reached(pipe_observed_bytes.load(Ordering::Acquire));
                    kill_process_group(pid)?;
                    break;
                }
                _ = tokio::time::sleep_until(deadline) => {
                    resource_limit = Some(ResourceLimit::WallTime {
                        limit_seconds: self.wall_timeout.as_secs(),
                    });
                    kill_process_group(pid)?;
                    break;
                }
                status = &mut wait, if exit_status.is_none() => {
                    exit_status = Some(status?);
                }
                chunk = rx.recv(), if streams_open => {
                    let Some(chunk) = chunk else {
                        streams_open = false;
                        continue;
                    };
                    // This is deliberately synchronous. Once recv() transfers
                    // ownership, the bytes are accounted and decoded before a
                    // newly-ready higher-priority stop branch can win.
                    let recorded = capture.record_chunk(&chunk)?;
                    let push = capture.archive_recorded(recorded);
                    tokio::pin!(push);
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            cancelled = true;
                            #[cfg(test)]
                            if !self.cancel_stop_delay.is_zero() {
                                tokio::time::sleep(self.cancel_stop_delay).await;
                            }
                            kill_process_group(pid)?;
                            break;
                        }
                        _ = output_quota.cancelled() => {
                            resource_limit =
                                output_limit_if_reached(pipe_observed_bytes.load(Ordering::Acquire));
                            kill_process_group(pid)?;
                            break;
                        }
                        _ = tokio::time::sleep_until(deadline) => {
                            resource_limit = Some(ResourceLimit::WallTime {
                                limit_seconds: self.wall_timeout.as_secs(),
                            });
                            kill_process_group(pid)?;
                            break;
                        }
                        result = &mut push => match result {
                            Ok(text) => on_update(json!({"output": text})),
                            Err(ToolError::ResourceLimit(limit)) => {
                                resource_limit = Some(limit);
                                kill_process_group(pid)?;
                                break;
                            }
                            Err(error) => {
                                kill_process_group(pid)?;
                                if exit_status.is_none() {
                                    let _status = timeout_at(
                                        Instant::now() + Duration::from_secs(1),
                                        &mut wait,
                                    )
                                    .await
                                    .map_err(|_| {
                                        ToolError::Protocol(
                                            "low-trust process group was killed but bash was not reaped"
                                                .to_owned(),
                                        )
                                    })??;
                                }
                                process_group.disarm();
                                return Err(error);
                            }
                        }
                    }
                }
            }
        }

        if exit_status.is_none() {
            exit_status = Some(
                timeout_at(Instant::now() + Duration::from_secs(1), &mut wait)
                    .await
                    .map_err(|_| {
                        ToolError::Protocol(
                            "low-trust process group did not terminate after SIGKILL".to_owned(),
                        )
                    })??,
            );
        }

        drop(wait);
        process_group.disarm();
        let mut output_task_result = None;
        if resource_limit.is_some() || cancelled {
            let drain_deadline = Instant::now() + STOPPED_PIPE_DRAIN_TIMEOUT;
            let mut stopped_chunks = VecDeque::new();
            let mut stopped_bytes = 0u64;
            loop {
                let chunk = match timeout_at(drain_deadline, rx.recv()).await {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        let task = output_task.take()?;
                        task.abort();
                        output_task_result = Some(task.await);
                        while let Ok(chunk) = rx.try_recv() {
                            stopped_bytes = stopped_bytes
                                .checked_add(u64::try_from(chunk.len()).map_err(|_| {
                                    ToolError::Protocol(
                                        "stopped bash output length overflow".to_owned(),
                                    )
                                })?)
                                .ok_or_else(|| {
                                    ToolError::Protocol(
                                        "stopped bash output length overflow".to_owned(),
                                    )
                                })?;
                            stopped_chunks.push_back(capture.record_chunk(&chunk)?);
                        }
                        break;
                    }
                };
                let Some(chunk) = chunk else {
                    break;
                };
                stopped_bytes = stopped_bytes
                    .checked_add(u64::try_from(chunk.len()).map_err(|_| {
                        ToolError::Protocol("stopped bash output length overflow".to_owned())
                    })?)
                    .ok_or_else(|| {
                        ToolError::Protocol("stopped bash output length overflow".to_owned())
                    })?;
                if stopped_bytes > super::shell_capture::COMMAND_OUTPUT_LIMIT_BYTES {
                    return Err(ToolError::Protocol(
                        "stopped bash output exceeded the pipe-reader hard bound".to_owned(),
                    ));
                }
                stopped_chunks.push_back(capture.record_chunk(&chunk)?);
            }
            if stopped_bytes > super::shell_capture::COMMAND_OUTPUT_LIMIT_BYTES {
                return Err(ToolError::Protocol(
                    "stopped bash output exceeded the pipe-reader hard bound".to_owned(),
                ));
            }

            // First detach every reader-observed byte from the bounded channel.
            // Best-effort artifact publication then gets one bounded deadline
            // without consuming the one-second pipe-reader drain goal.
            for recorded in stopped_chunks {
                match capture.archive_recorded(recorded).await {
                    Ok(text) => on_update(json!({"output": text})),
                    Err(ToolError::ResourceLimit(limit)) if !cancelled => {
                        resource_limit = Some(limit);
                    }
                    Err(ToolError::ResourceLimit(_)) => {}
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            "failed to archive a queued bash output chunk after process stop"
                        );
                    }
                }
            }
            let observed = pipe_observed_bytes.load(Ordering::Acquire);
            if !cancelled && let Some(limit) = output_limit_if_reached(observed) {
                resource_limit = Some(limit);
            }
        } else {
            debug_assert!(!streams_open);
        }
        let output_task_result = match output_task_result {
            Some(result) => result,
            None => output_task.take()?.await,
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

        if cancelled {
            // Cancellation is the authoritative stop cause once its biased
            // branch wins. Post-stop accounting still drains every observed
            // byte, but a concurrently reached output quota must not replace
            // or coexist with the cancellation terminal.
            resource_limit = None;
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

struct ProcessGroupGuard {
    pid: Option<u32>,
}

impl ProcessGroupGuard {
    fn new(pid: u32) -> Self {
        Self { pid: Some(pid) }
    }

    fn disarm(&mut self) {
        self.pid = None;
    }
}

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.pid {
            let _ = kill_process_group(pid);
        }
    }
}

struct AbortOnDropTask<T> {
    task: Option<tokio::task::JoinHandle<T>>,
}

impl<T> AbortOnDropTask<T> {
    fn new(task: tokio::task::JoinHandle<T>) -> Self {
        Self { task: Some(task) }
    }

    fn take(&mut self) -> Result<tokio::task::JoinHandle<T>, ToolError> {
        self.task
            .take()
            .ok_or_else(|| ToolError::Protocol("merged output task was already joined".to_owned()))
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
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

fn inherited_fd_limit() -> Result<libc::rlim_t, ToolError> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };
    if result != 0 {
        return Err(ToolError::Io(std::io::Error::last_os_error()));
    }
    // An FD opened before the soft limit was lowered may remain above rlim_cur.
    // The finite hard allocation ceiling covers those descriptors as well as
    // the Command errpipe and stdio plumbing created immediately before fork.
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

fn configure_child_process(
    process: &mut Command,
    inherited_fd_limit: libc::rlim_t,
    force_close_range_fallback: bool,
    broker_socket: Option<&Path>,
    workspace: &Path,
    enforce_broker_socket_isolation: bool,
) {
    let broker_socket = broker_socket.map(|p| p.to_path_buf());
    let workspace = workspace.to_path_buf();
    #[allow(unsafe_code)]
    unsafe {
        process.process_group(0);
        process.pre_exec(move || {
            if enforce_broker_socket_isolation {
                isolate_broker_socket_path(broker_socket.as_deref(), &workspace)?;
            }
            sanitize_inherited_fds(inherited_fd_limit, force_close_range_fallback)
        });
    }
}

/// Fill `buf` with lowercase hex digits from getrandom(2). `buf.len()` must be
/// even. This is async-signal-safe because it calls a single raw syscall and
/// uses only a stack buffer.
fn fill_random_hex(buf: &mut [u8]) -> std::io::Result<()> {
    if !buf.len().is_multiple_of(2) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "random hex buffer length must be even",
        ));
    }

    let mut random = [0u8; 8];
    let mut filled = 0;
    while filled < random.len() {
        let n = unsafe {
            libc::getrandom(
                random.as_mut_ptr().add(filled) as *mut libc::c_void,
                random.len() - filled,
                0,
            )
        };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(err);
        }
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "getrandom returned 0 bytes",
            ));
        }
        filled += n as usize;
    }

    const HEX: &[u8] = b"0123456789abcdef";
    for (i, b) in random.iter().enumerate() {
        buf[i * 2] = HEX[(b >> 4) as usize];
        buf[i * 2 + 1] = HEX[(b & 0xf) as usize];
    }
    Ok(())
}

/// Make every inherited mount private so a shared parent mount cannot propagate
/// a later bind mount back to the executor/host namespace.
fn mount_private_root() -> std::io::Result<()> {
    let root = b"/\0";
    unsafe {
        if libc::mount(
            std::ptr::null(),
            root.as_ptr() as *const libc::c_char,
            std::ptr::null(),
            libc::MS_REC | libc::MS_PRIVATE,
            std::ptr::null(),
        ) != 0
        {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Create an empty source directory for a bind mount. `path` must be a
/// NUL-terminated absolute path. Fails closed if the path already exists.
fn create_isolation_dir(path: *const libc::c_char) -> std::io::Result<()> {
    unsafe {
        if libc::mkdir(path, 0o700) != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Create an empty source file for a bind mount. `path` must be a NUL-terminated
/// absolute path. Fails closed if the path already exists or is a symlink.
fn create_isolation_file(path: *const libc::c_char) -> std::io::Result<libc::c_int> {
    let fd = unsafe {
        libc::open(
            path,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o666,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(fd)
}

/// Make the broker socket pathname unreachable from the bash child while keeping
/// the executor's file-tool path intact. This is invoked in the child just
/// before `execve`, so it must be async-signal-safe: no allocation, no std
/// locks, only raw syscalls and stack buffers.
fn isolate_broker_socket_path(
    broker_socket: Option<&Path>,
    workspace: &Path,
) -> std::io::Result<()> {
    let socket = match broker_socket {
        Some(path) => path,
        None => return Ok(()),
    };

    let socket_bytes = socket.as_os_str().as_bytes();
    if socket_bytes.is_empty() || socket_bytes[0] != b'/' {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "broker socket path must be absolute",
        ));
    }

    let mut socket_buf = [0u8; 4096];
    if socket_bytes.len() + 1 > socket_buf.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "broker socket path too long",
        ));
    }
    socket_buf[..socket_bytes.len()].copy_from_slice(socket_bytes);
    socket_buf[socket_bytes.len()] = 0;

    // If the socket file does not exist there is nothing to mask (tests use
    // placeholder paths). Skip without entering a namespace.
    unsafe {
        let mut stat_buf: libc::stat = std::mem::zeroed();
        if libc::stat(socket_buf.as_ptr() as *const libc::c_char, &mut stat_buf) != 0 {
            return Ok(());
        }
    }

    let parent = match socket.parent() {
        Some(parent) => parent,
        None => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "broker socket path has no parent directory",
            ));
        }
    };
    let parent_bytes = parent.as_os_str().as_bytes();
    let workspace_bytes = workspace.as_os_str().as_bytes();
    let mask_parent = parent_bytes != workspace_bytes && parent_bytes != b"/";

    let target_bytes = if mask_parent {
        parent_bytes
    } else {
        socket_bytes
    };
    let mut target_buf = [0u8; 4096];
    if target_bytes.len() + 1 > target_buf.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "broker socket parent path too long",
        ));
    }
    target_buf[..target_bytes.len()].copy_from_slice(target_bytes);
    target_buf[target_bytes.len()] = 0;

    // Capture the UID/GID in the parent namespace before unsharing, because
    // getuid()/getgid() return the overflow user ID inside an unmapped child
    // namespace and writing that into uid_map is rejected.
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    // Build an unpredictable source path. getrandom(2) is async-signal-safe
    // and removes the symlink race inherent in a pid-suffix path.
    let mut source_buf = [0u8; 64];
    let source_prefix = b"/tmp/.sumi-broker-isolation-";
    let mut pos = 0;
    source_buf[pos..pos + source_prefix.len()].copy_from_slice(source_prefix);
    pos += source_prefix.len();
    fill_random_hex(&mut source_buf[pos..pos + 16])?;
    pos += 16;
    source_buf[pos] = 0;

    unsafe {
        // A user namespace must be created and mapped before creating the
        // mount/network namespaces.  Combining these flags is rejected on
        // supported kernels because the latter need the capabilities granted
        // by the completed user namespace.
        if libc::unshare(libc::CLONE_NEWUSER) != 0 {
            return Err(std::io::Error::last_os_error());
        }

        // The executor parent runs with dumpable=0 (hiding /proc/pid/environ).
        // A child needs dumpable=1 briefly to write /proc/self/uid_map; it will
        // be reset to 1 by the following setuid(0) and bash exec anyway.
        const PR_SET_DUMPABLE: libc::c_int = 4;
        if libc::syscall(libc::SYS_prctl, PR_SET_DUMPABLE, 1, 0, 0, 0) != 0 {
            return Err(std::io::Error::last_os_error());
        }

        write_id_map(b"/proc/self/uid_map\0", 0, uid)?;
        write_setgroups_deny()?;
        write_id_map(b"/proc/self/gid_map\0", 0, gid)?;

        if libc::setgid(0) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        if libc::setuid(0) != 0 {
            return Err(std::io::Error::last_os_error());
        }

        if libc::unshare(libc::CLONE_NEWNS | libc::CLONE_NEWNET) != 0 {
            return Err(std::io::Error::last_os_error());
        }

        // Make every inherited mount private before binding, so the mask does
        // not propagate to the executor/host namespace.
        mount_private_root()?;

        let is_dir = mask_parent;
        let source_fd: Option<libc::c_int> = if is_dir {
            create_isolation_dir(source_buf.as_ptr() as *const libc::c_char)?;
            None
        } else {
            Some(create_isolation_file(
                source_buf.as_ptr() as *const libc::c_char
            )?)
        };

        if libc::mount(
            source_buf.as_ptr() as *const libc::c_char,
            target_buf.as_ptr() as *const libc::c_char,
            std::ptr::null(),
            libc::MS_BIND,
            std::ptr::null(),
        ) != 0
        {
            if is_dir {
                libc::rmdir(source_buf.as_ptr() as *const libc::c_char);
            } else {
                libc::unlink(source_buf.as_ptr() as *const libc::c_char);
            }
            if let Some(fd) = source_fd {
                libc::close(fd);
            }
            return Err(std::io::Error::last_os_error());
        }
        if let Some(fd) = source_fd {
            libc::close(fd);
        }

        // The bind mount now pins the source inode. Remove the source path so
        // no host-visible per-command debris remains.
        if is_dir {
            if libc::rmdir(source_buf.as_ptr() as *const libc::c_char) != 0 {
                return Err(std::io::Error::last_os_error());
            }
        } else if libc::unlink(source_buf.as_ptr() as *const libc::c_char) != 0 {
            return Err(std::io::Error::last_os_error());
        }

        drop_all_capabilities()?;

        const PR_SET_NO_NEW_PRIVS: libc::c_int = 38;
        if libc::syscall(libc::SYS_prctl, PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }

    Ok(())
}

fn run_preflight_namespace(
    source: *const libc::c_char,
    target: *const libc::c_char,
    uid: libc::uid_t,
    gid: libc::gid_t,
) -> std::io::Result<()> {
    unsafe {
        if libc::unshare(libc::CLONE_NEWUSER) != 0 {
            let error = std::io::Error::last_os_error();
            return Err(std::io::Error::new(
                error.kind(),
                format!("preflight create user namespace: {error}"),
            ));
        }

        const PR_SET_DUMPABLE: libc::c_int = 4;
        if libc::syscall(libc::SYS_prctl, PR_SET_DUMPABLE, 1, 0, 0, 0) != 0 {
            let error = std::io::Error::last_os_error();
            return Err(std::io::Error::new(
                error.kind(),
                format!("preflight set dumpable: {error}"),
            ));
        }

        write_id_map(b"/proc/self/uid_map\0", 0, uid)
            .map_err(|error| std::io::Error::new(error.kind(), format!("uid map: {error}")))?;
        write_setgroups_deny().map_err(|error| {
            std::io::Error::new(error.kind(), format!("setgroups deny: {error}"))
        })?;
        write_id_map(b"/proc/self/gid_map\0", 0, gid)
            .map_err(|error| std::io::Error::new(error.kind(), format!("gid map: {error}")))?;

        if libc::setgid(0) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        if libc::setuid(0) != 0 {
            return Err(std::io::Error::last_os_error());
        }

        if libc::unshare(libc::CLONE_NEWNS | libc::CLONE_NEWNET) != 0 {
            let error = std::io::Error::last_os_error();
            return Err(std::io::Error::new(
                error.kind(),
                format!("preflight create mount/network namespaces: {error}"),
            ));
        }

        mount_private_root().map_err(|error| {
            std::io::Error::new(error.kind(), format!("make mounts private: {error}"))
        })?;

        if libc::mount(
            source,
            target,
            std::ptr::null(),
            libc::MS_BIND,
            std::ptr::null(),
        ) != 0
        {
            let error = std::io::Error::last_os_error();
            return Err(std::io::Error::new(
                error.kind(),
                format!("bind mount preflight paths: {error}"),
            ));
        }

        // This probe must leave no mount behind: its source/target names were
        // created in the original namespace and are removed by the caller.
        if libc::umount2(target, libc::MNT_DETACH) != 0 {
            let error = std::io::Error::last_os_error();
            return Err(std::io::Error::new(
                error.kind(),
                format!("unmount preflight path: {error}"),
            ));
        }
    }

    // The preflight child exits immediately; the bind mount is discarded with
    // its namespace. The host source/target directories are removed by the
    // caller.
    Ok(())
}

/// Validate that the host allows the namespace/bind-mount dance required by
/// `isolate_broker_socket_path`. This is intended to run in a short-lived child
/// process; it does not return to the original namespaces.
pub(crate) fn preflight_namespace_isolation() -> std::io::Result<()> {
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    let mut source_buf = [0u8; 64];
    let mut target_buf = [0u8; 64];
    const SOURCE_PREFIX: &[u8] = b"/tmp/.sumi-broker-preflight-src-";
    const TARGET_PREFIX: &[u8] = b"/tmp/.sumi-broker-preflight-tgt-";
    debug_assert!(SOURCE_PREFIX.len() + 16 < source_buf.len());
    debug_assert!(TARGET_PREFIX.len() + 16 < target_buf.len());
    source_buf[..SOURCE_PREFIX.len()].copy_from_slice(SOURCE_PREFIX);
    fill_random_hex(&mut source_buf[SOURCE_PREFIX.len()..SOURCE_PREFIX.len() + 16])?;
    source_buf[SOURCE_PREFIX.len() + 16] = 0;
    target_buf[..TARGET_PREFIX.len()].copy_from_slice(TARGET_PREFIX);
    fill_random_hex(&mut target_buf[TARGET_PREFIX.len()..TARGET_PREFIX.len() + 16])?;
    target_buf[TARGET_PREFIX.len() + 16] = 0;

    unsafe {
        create_isolation_dir(source_buf.as_ptr() as *const libc::c_char)?;
        if let Err(error) = create_isolation_dir(target_buf.as_ptr() as *const libc::c_char) {
            let _ = libc::rmdir(source_buf.as_ptr() as *const libc::c_char);
            return Err(error);
        }

        let result = run_preflight_namespace(
            source_buf.as_ptr() as *const libc::c_char,
            target_buf.as_ptr() as *const libc::c_char,
            uid,
            gid,
        );

        let source_cleanup = libc::rmdir(source_buf.as_ptr() as *const libc::c_char);
        let target_cleanup = libc::rmdir(target_buf.as_ptr() as *const libc::c_char);
        if result.is_ok() && (source_cleanup != 0 || target_cleanup != 0) {
            return Err(std::io::Error::last_os_error());
        }

        result
    }
}

fn write_decimal(buf: &mut [u8], mut n: u32) -> usize {
    if n == 0 {
        buf[0] = b'0';
        return 1;
    }
    let mut digits = [0u8; 10];
    let mut count = 0;
    while n > 0 {
        digits[count] = b'0' + (n % 10) as u8;
        n /= 10;
        count += 1;
    }
    for i in 0..count {
        buf[i] = digits[count - 1 - i];
    }
    count
}

fn write_id_map(path: &[u8], child_id: u32, parent_id: u32) -> std::io::Result<()> {
    unsafe {
        let fd = libc::open(
            path.as_ptr() as *const libc::c_char,
            libc::O_WRONLY | libc::O_CLOEXEC,
        );
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        let mut buf = [0u8; 64];
        let mut pos = 0;
        pos += write_decimal(&mut buf[pos..], child_id);
        buf[pos] = b' ';
        pos += 1;
        pos += write_decimal(&mut buf[pos..], parent_id);
        buf[pos] = b' ';
        pos += 1;
        buf[pos] = b'1';
        pos += 1;
        buf[pos] = b'\n';
        pos += 1;

        let mut written = 0;
        while written < pos {
            let n = libc::write(
                fd,
                buf.as_ptr().add(written) as *const libc::c_void,
                pos - written,
            );
            if n < 0 {
                libc::close(fd);
                return Err(std::io::Error::last_os_error());
            }
            written += n as usize;
        }
        libc::close(fd);
        Ok(())
    }
}

fn write_setgroups_deny() -> std::io::Result<()> {
    const DENY: &[u8] = b"deny\n";
    unsafe {
        let fd = libc::open(
            c"/proc/self/setgroups".as_ptr(),
            libc::O_WRONLY | libc::O_CLOEXEC,
        );
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut written = 0;
        while written < DENY.len() {
            let n = libc::write(
                fd,
                DENY.as_ptr().add(written) as *const libc::c_void,
                DENY.len() - written,
            );
            if n < 0 {
                libc::close(fd);
                return Err(std::io::Error::last_os_error());
            }
            written += n as usize;
        }
        libc::close(fd);
        Ok(())
    }
}

#[repr(C)]
struct CapUserHeader {
    version: u32,
    pid: i32,
}

#[repr(C)]
struct CapUserData {
    effective: u32,
    permitted: u32,
    inheritable: u32,
}

fn drop_all_capabilities() -> std::io::Result<()> {
    const LINUX_CAPABILITY_VERSION_3: u32 = 0x20080522;
    unsafe {
        let header = CapUserHeader {
            version: LINUX_CAPABILITY_VERSION_3,
            pid: 0,
        };
        let data: [CapUserData; 2] = [
            CapUserData {
                effective: 0,
                permitted: 0,
                inheritable: 0,
            },
            CapUserData {
                effective: 0,
                permitted: 0,
                inheritable: 0,
            },
        ];
        if libc::syscall(libc::SYS_capset, &header, &data) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
}

fn sanitize_inherited_fds(
    inherited_fd_limit: libc::rlim_t,
    force_close_range_fallback: bool,
) -> std::io::Result<()> {
    const CLOSE_RANGE_CLOEXEC: libc::c_uint = 1 << 2;

    let result = if force_close_range_fallback {
        -1
    } else {
        unsafe { libc::syscall(libc::SYS_close_range, 3u32, u32::MAX, CLOSE_RANGE_CLOEXEC) }
    };
    if result == 0 {
        return Ok(());
    }

    let close_range_errno = if force_close_range_fallback {
        libc::ENOSYS
    } else {
        errno()
    };
    if close_range_errno != libc::ENOSYS && close_range_errno != libc::EINVAL {
        return Err(std::io::Error::from_raw_os_error(close_range_errno));
    }

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

fn errno() -> libc::c_int {
    unsafe { *libc::__errno_location() }
}

fn to_execution_result(
    capture: ShellCaptureResult,
    exit_code: Option<i32>,
    cancelled: bool,
    resource_limit: Option<ResourceLimit>,
) -> BashExecutionResult {
    let ShellCaptureResult {
        output,
        truncation,
        artifact_handle,
        observed_bytes,
    } = capture;
    BashExecutionResult {
        output,
        truncation,
        artifact_handle,
        observed_bytes,
        exit_code: if cancelled || resource_limit.is_some() {
            None
        } else {
            exit_code
        },
        cancelled,
        resource_limit,
    }
}

fn kill_process_group(pid: u32) -> Result<(), ToolError> {
    let pid = i32::try_from(pid)
        .map_err(|_| ToolError::Protocol("bash process id exceeded i32".to_owned()))?;
    let result = unsafe { libc::kill(-pid, libc::SIGKILL) };
    if result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(ToolError::Io(error))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        os::fd::{AsRawFd, FromRawFd, OwnedFd},
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use async_trait::async_trait;
    use std::future::pending;
    use tokio::sync::Notify;

    use super::*;

    #[derive(Default)]
    struct MemoryArtifacts {
        content: Mutex<HashMap<String, Vec<u8>>>,
    }

    #[async_trait]
    impl ArtifactAppender for MemoryArtifacts {
        async fn begin_tool_output(
            &self,
            execution_id: &str,
            initial_content: &[u8],
        ) -> Result<String, ToolError> {
            let handle = format!("artifact://conversation/tool-output/{execution_id}");
            self.content
                .lock()
                .expect("artifact lock")
                .insert(handle.clone(), initial_content.to_vec());
            Ok(handle)
        }

        async fn append_tool_output(
            &self,
            handle: &str,
            offset: u64,
            content: &[u8],
        ) -> Result<(), ToolError> {
            let mut artifacts = self.content.lock().expect("artifact lock");
            let artifact = artifacts.get_mut(handle).expect("known handle");
            assert_eq!(
                u64::try_from(artifact.len()).expect("artifact length"),
                offset
            );
            artifact.extend_from_slice(content);
            Ok(())
        }

        async fn finish_tool_output(&self, _handle: &str) -> Result<(), ToolError> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct HangingArtifacts {
        begin_calls: AtomicUsize,
    }

    #[async_trait]
    impl ArtifactAppender for HangingArtifacts {
        async fn begin_tool_output(
            &self,
            _execution_id: &str,
            _initial_content: &[u8],
        ) -> Result<String, ToolError> {
            if self.begin_calls.fetch_add(1, Ordering::Relaxed) == 0 {
                pending().await
            } else {
                Err(ToolError::Rpc(
                    "injected reconnect failure after cancellation".to_owned(),
                ))
            }
        }

        async fn append_tool_output(
            &self,
            _handle: &str,
            _offset: u64,
            _content: &[u8],
        ) -> Result<(), ToolError> {
            pending().await
        }

        async fn finish_tool_output(&self, _handle: &str) -> Result<(), ToolError> {
            pending().await
        }
    }

    #[derive(Default)]
    struct BlockingBeginArtifacts {
        entered: Notify,
        begin_calls: AtomicUsize,
    }

    #[async_trait]
    impl ArtifactAppender for BlockingBeginArtifacts {
        async fn begin_tool_output(
            &self,
            execution_id: &str,
            _initial_content: &[u8],
        ) -> Result<String, ToolError> {
            if self.begin_calls.fetch_add(1, Ordering::Relaxed) == 0 {
                self.entered.notify_one();
                pending().await
            } else {
                Ok(format!(
                    "artifact://conversation/tool-output/{execution_id}"
                ))
            }
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

    #[derive(Default)]
    struct DelayedReplayArtifacts {
        entered: Notify,
        begin_calls: AtomicUsize,
        content: Mutex<HashMap<String, Vec<u8>>>,
    }

    #[async_trait]
    impl ArtifactAppender for DelayedReplayArtifacts {
        async fn begin_tool_output(
            &self,
            execution_id: &str,
            initial_content: &[u8],
        ) -> Result<String, ToolError> {
            if self.begin_calls.fetch_add(1, Ordering::Relaxed) == 0 {
                self.entered.notify_one();
                pending().await
            } else {
                tokio::time::sleep(STOPPED_PIPE_DRAIN_TIMEOUT + Duration::from_millis(200)).await;
                let handle = format!("artifact://conversation/tool-output/{execution_id}");
                self.content
                    .lock()
                    .expect("artifact lock")
                    .insert(handle.clone(), initial_content.to_vec());
                Ok(handle)
            }
        }

        async fn append_tool_output(
            &self,
            handle: &str,
            offset: u64,
            content: &[u8],
        ) -> Result<(), ToolError> {
            let mut artifacts = self.content.lock().expect("artifact lock");
            let artifact = artifacts.get_mut(handle).expect("known handle");
            assert_eq!(
                u64::try_from(artifact.len()).expect("artifact length"),
                offset
            );
            artifact.extend_from_slice(content);
            Ok(())
        }

        async fn finish_tool_output(&self, _handle: &str) -> Result<(), ToolError> {
            Ok(())
        }
    }

    struct TempWorkspace(PathBuf);

    impl TempWorkspace {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("sumi-bash-{}", uuid::Uuid::now_v7()));
            std::fs::create_dir_all(&path).expect("create temp workspace");
            Self(path)
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn wait_for_workspace_marker(marker: &std::path::Path) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if marker.exists() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("bash command did not create its completion marker");
    }

    async fn wait_for_workspace_pid(pid_file: &std::path::Path) -> libc::pid_t {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(contents) = std::fs::read_to_string(pid_file)
                    && let Ok(pid) = contents.trim().parse::<libc::pid_t>()
                    && pid > 0
                {
                    return pid;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("bash command did not publish a valid pid")
    }

    #[tokio::test]
    async fn executes_in_workspace_with_cleared_environment_and_streams_updates() {
        let workspace = TempWorkspace::new();
        let artifacts = MemoryArtifacts::default();
        let updates = Arc::new(AtomicUsize::new(0));
        let observed = updates.clone();
        let result = LowTrustLocalBash::new(workspace.0.clone(), &artifacts)
            .execute(
                "printf '%s:%s' \"$PWD\" \"${SUMI_SECRET-unset}\"",
                "bash-1",
                CancellationToken::new(),
                Arc::new(move |_| {
                    observed.fetch_add(1, Ordering::Relaxed);
                }),
            )
            .await
            .expect("bash result");
        assert_eq!(result.exit_code, Some(0));
        assert_eq!(result.output, format!("{}:unset", workspace.0.display()));
        assert!(updates.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn consistency_accepts_real_sanitized_output_accounting() {
        let workspace = TempWorkspace::new();
        let artifacts = MemoryArtifacts::default();

        for (command, execution_id, ordering) in [
            (
                "printf '\\001ok\\002'",
                "bash-sanitized-contract-shrinks",
                std::cmp::Ordering::Less,
            ),
            (
                "printf '\\377'",
                "bash-sanitized-contract-expands",
                std::cmp::Ordering::Greater,
            ),
        ] {
            let result = LowTrustLocalBash::new(workspace.0.clone(), &artifacts)
                .execute(
                    command,
                    execution_id,
                    CancellationToken::new(),
                    Arc::new(|_| {}),
                )
                .await
                .expect("real bash result");
            assert_eq!(
                result
                    .truncation
                    .total_bytes
                    .cmp(&usize::try_from(result.observed_bytes).expect("bounded observed bytes")),
                ordering
            );
            assert!(result.is_consistent());
        }
    }

    async fn assert_inherited_fd_is_closed(force_fallback: bool) {
        let workspace = TempWorkspace::new();
        let artifacts = MemoryArtifacts::default();
        let mut pipe_fds = [-1; 2];
        assert_eq!(unsafe { libc::pipe(pipe_fds.as_mut_ptr()) }, 0);
        let read_fd = unsafe { OwnedFd::from_raw_fd(pipe_fds[0]) };
        let write_fd = unsafe { OwnedFd::from_raw_fd(pipe_fds[1]) };
        assert_eq!(
            unsafe { libc::fcntl(write_fd.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0,
            "fixture must deliberately be inherited across exec"
        );
        let command = format!(
            "if [ -e /proc/self/fd/{} ]; then printf inherited; else printf closed; fi",
            write_fd.as_raw_fd()
        );
        let mut bash = LowTrustLocalBash::new(workspace.0.clone(), &artifacts);
        if force_fallback {
            bash = bash.with_forced_close_range_fallback();
        }
        let result = bash
            .execute(
                &command,
                if force_fallback {
                    "bash-fd-fallback"
                } else {
                    "bash-fd-close-range"
                },
                CancellationToken::new(),
                Arc::new(|_| {}),
            )
            .await
            .expect("bash result");
        assert_eq!(result.exit_code, Some(0));
        assert_eq!(result.output, "closed");
        drop((read_fd, write_fd));
    }

    #[tokio::test]
    async fn close_range_closes_inherited_non_cloexec_fd() {
        assert_inherited_fd_is_closed(false).await;
    }

    #[tokio::test]
    async fn enosys_fallback_closes_inherited_non_cloexec_fd() {
        assert_inherited_fd_is_closed(true).await;
    }

    #[tokio::test]
    async fn pre_cancelled_execution_returns_empty_without_spawning() {
        let workspace = TempWorkspace::new();
        let marker = workspace.0.join("must-not-exist");
        let artifacts = MemoryArtifacts::default();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let result = LowTrustLocalBash::new(workspace.0.clone(), &artifacts)
            .execute(
                ": > must-not-exist",
                "bash-pre-cancelled",
                cancel,
                Arc::new(|_| panic!("pre-cancelled execution must not emit updates")),
            )
            .await
            .expect("pre-cancelled result");
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
        assert!(!marker.exists(), "pre-cancelled bash command was spawned");
    }

    #[tokio::test]
    async fn pre_cancelled_execution_precedes_invalid_workspace_error() {
        let workspace = std::env::temp_dir().join(format!(
            "sumi-missing-bash-workspace-{}",
            uuid::Uuid::now_v7()
        ));
        let artifacts = MemoryArtifacts::default();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let result = LowTrustLocalBash::new(workspace, &artifacts)
            .execute(
                "exit 0",
                "bash-pre-cancelled-invalid-workspace",
                cancel,
                Arc::new(|_| {}),
            )
            .await
            .expect("pre-cancel must win before workspace validation");
        assert!(result.cancelled);
        assert_eq!(result.observed_bytes, 0);
    }

    #[test]
    fn exec_failure_is_reported_after_fd_sanitizer() {
        let inherited_fd_limit = inherited_fd_limit().expect("finite inherited FD bound");
        for force_fallback in [false, true] {
            let mut process = Command::new("/sumi-test/no-such-executable");
            configure_child_process(
                &mut process,
                inherited_fd_limit,
                force_fallback,
                None,
                std::path::Path::new("/"),
                false,
            );
            let error = process
                .spawn()
                .expect_err("exec failure must reach the parent through Command's errpipe");
            assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        }
    }

    #[tokio::test]
    async fn large_output_is_truncated_and_fully_archived() {
        let workspace = TempWorkspace::new();
        let artifacts = MemoryArtifacts::default();
        let result = LowTrustLocalBash::new(workspace.0.clone(), &artifacts)
            .execute(
                "head -c 70000 /dev/zero | tr '\\0' x",
                "bash-2",
                CancellationToken::new(),
                Arc::new(|_| {}),
            )
            .await
            .expect("bash result");
        assert_eq!(result.exit_code, Some(0));
        assert!(result.output.len() <= super::super::truncate::DEFAULT_MAX_BYTES);
        assert_eq!(result.truncation.total_bytes, 70_000);
        let handle = result.artifact_handle.expect("artifact handle");
        assert_eq!(
            artifacts
                .content
                .lock()
                .expect("artifact lock")
                .get(&handle)
                .expect("artifact")
                .len(),
            70_000
        );
    }

    #[tokio::test]
    async fn stdout_and_stderr_share_one_ordered_pipe() {
        let workspace = TempWorkspace::new();
        let artifacts = MemoryArtifacts::default();
        let result = LowTrustLocalBash::new(workspace.0.clone(), &artifacts)
            .execute(
                "printf A; printf B >&2; printf C; printf D >&2",
                "bash-ordered",
                CancellationToken::new(),
                Arc::new(|_| {}),
            )
            .await
            .expect("bash result");
        assert_eq!(result.output, "ABCD");
    }

    #[tokio::test]
    async fn nonzero_exit_code_is_preserved_for_terminal_rendering() {
        let workspace = TempWorkspace::new();
        let artifacts = MemoryArtifacts::default();
        let result = LowTrustLocalBash::new(workspace.0.clone(), &artifacts)
            .execute(
                "printf failed; exit 17",
                "bash-nonzero",
                CancellationToken::new(),
                Arc::new(|_| {}),
            )
            .await
            .expect("nonzero bash result");
        assert_eq!(result.output, "failed");
        assert_eq!(result.exit_code, Some(17));
    }

    #[tokio::test]
    async fn deadline_still_applies_after_parent_exits_with_descendant_pipe_open() {
        let workspace = TempWorkspace::new();
        let artifacts = MemoryArtifacts::default();
        let started = Instant::now();
        let result = LowTrustLocalBash::new(workspace.0.clone(), &artifacts)
            .with_timeout(Duration::from_millis(80))
            .execute(
                "(sleep 30) &",
                "bash-descendant-pipe",
                CancellationToken::new(),
                Arc::new(|_| {}),
            )
            .await
            .expect("timed result");
        assert!(matches!(
            result.resource_limit,
            Some(ResourceLimit::WallTime { .. })
        ));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn dropping_active_execution_kills_and_reaps_process_group() {
        let workspace = TempWorkspace::new();
        let artifacts = MemoryArtifacts::default();
        let pid_file = workspace.0.join("active.pid");
        let pid = {
            let bash = LowTrustLocalBash::new(workspace.0.clone(), &artifacts);
            let execution = bash.execute(
                "echo $$ > active.pid; sleep 120",
                "bash-drop-reap",
                CancellationToken::new(),
                Arc::new(|_| {}),
            );
            tokio::pin!(execution);
            tokio::select! {
                result = &mut execution => panic!("held bash exited early: {result:?}"),
                pid = wait_for_workspace_pid(&pid_file) => pid,
            }
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let result = unsafe { libc::kill(pid, 0) };
                if result == -1
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropped bash was not killed and reaped");
    }

    #[tokio::test]
    async fn stopped_reader_closes_fd_held_by_setsid_escaped_descendant() {
        let workspace = TempWorkspace::new();
        let artifacts = MemoryArtifacts::default();
        let marker = workspace.0.join("escaped-marker");
        let started = Instant::now();
        let result = LowTrustLocalBash::new(workspace.0.clone(), &artifacts)
            .with_timeout(Duration::from_millis(40))
            .execute(
                "setsid bash -c 'set -e; sleep 2; printf ESCAPED; : > escaped-marker' &",
                "bash-setsid-reader-abort",
                CancellationToken::new(),
                Arc::new(|_| {}),
            )
            .await
            .expect("timed result");
        assert!(matches!(
            result.resource_limit,
            Some(ResourceLimit::WallTime { .. })
        ));
        assert!(started.elapsed() < Duration::from_millis(1_500));

        // The escaped descendant retains the pipe writer beyond the bounded
        // drain. Aborting the AsyncFd reader must close the actual read FD, so
        // its later write fails and `set -e` prevents the marker operation.
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        assert!(!result.output.contains("ESCAPED"));
        assert!(
            !marker.exists(),
            "an escaped descendant wrote after the stopped reader returned"
        );
    }

    #[tokio::test]
    async fn cancellation_kills_process_group_and_returns_partial_result() {
        let workspace = TempWorkspace::new();
        let artifacts = MemoryArtifacts::default();
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            cancel_task.cancel();
        });
        let result = LowTrustLocalBash::new(workspace.0.clone(), &artifacts)
            .execute(
                "printf started; sleep 30; printf finished",
                "bash-3",
                cancel,
                Arc::new(|_| {}),
            )
            .await
            .expect("cancelled result");
        assert!(result.cancelled);
        assert_eq!(result.exit_code, None);
        assert!(
            !result.output.is_empty(),
            "output read before the stalled artifact await must remain as bounded partial output"
        );
        assert!(result.output.contains("started"));
        assert!(!result.output.contains("finished"));
    }

    #[tokio::test]
    async fn cancellation_drains_every_chunk_already_read_from_the_bounded_pipe() {
        let workspace = TempWorkspace::new();
        let artifacts = BlockingBeginArtifacts::default();
        let marker = workspace.0.join("queued-cancel-complete");
        let cancel = CancellationToken::new();
        let cancel_after_begin = cancel.clone();
        let bash = LowTrustLocalBash::new(workspace.0.clone(), &artifacts);
        let execution = bash.execute(
            "head -c 98304 /dev/zero | tr '\\0' x; printf END; : > queued-cancel-complete",
            "bash-queued-cancel",
            cancel,
            Arc::new(|_| {}),
        );
        let trigger = async {
            artifacts.entered.notified().await;
            wait_for_workspace_marker(&marker).await;
            cancel_after_begin.cancel();
        };
        let (result, ()) = tokio::join!(execution, trigger);
        let result = result.expect("cancelled result");
        assert!(result.cancelled);
        assert_eq!(result.observed_bytes, 98_307);
        assert!(
            result.output.ends_with("END"),
            "the terminal result must include bytes queued behind the stalled artifact exchange"
        );
    }

    #[tokio::test]
    async fn ready_stop_between_dequeue_and_archive_poll_cannot_omit_the_chunk() {
        let artifacts = MemoryArtifacts::default();
        let (tx, mut rx) = mpsc::channel(1);
        let reader_observed = vec![b'x'; 60_000];
        tx.send(reader_observed.clone())
            .await
            .expect("queue fixture chunk");
        drop(tx);

        let chunk = rx.recv().await.expect("dequeue fixture chunk");
        let mut capture = ShellCapture::new("bash-ready-stop", &artifacts);
        let recorded = capture
            .record_chunk(&chunk)
            .expect("synchronously account dequeued chunk");
        let cancel = CancellationToken::new();
        cancel.cancel();
        {
            let archive = capture.archive_recorded(recorded);
            tokio::pin!(archive);
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {}
                _ = &mut archive => panic!("ready stop must win before the first archive poll"),
            }
        }

        let result = capture.finish_after_abort().await;
        assert_eq!(result.observed_bytes, chunk.len() as u64);
        assert_eq!(result.truncation.total_bytes, chunk.len());
        let handle = result
            .artifact_handle
            .expect("recorded chunk must be replayed to the partial artifact");
        assert_eq!(
            artifacts
                .content
                .lock()
                .expect("artifact lock")
                .get(&handle)
                .expect("artifact"),
            &reader_observed
        );
    }

    #[tokio::test]
    async fn delayed_artifact_replay_cannot_consume_the_bounded_reader_drain() {
        let workspace = TempWorkspace::new();
        let artifacts = DelayedReplayArtifacts::default();
        let marker = workspace.0.join("delayed-replay-complete");
        let cancel = CancellationToken::new();
        let cancel_after_begin = cancel.clone();
        let bash = LowTrustLocalBash::new(workspace.0.clone(), &artifacts);
        let execution = bash.execute(
            "head -c 98304 /dev/zero | tr '\\0' x; printf END; : > delayed-replay-complete",
            "bash-delayed-replay",
            cancel,
            Arc::new(|_| {}),
        );
        let trigger = async {
            artifacts.entered.notified().await;
            wait_for_workspace_marker(&marker).await;
            cancel_after_begin.cancel();
        };
        let (result, ()) = tokio::join!(execution, trigger);
        let result = result.expect("cancelled result");
        assert!(result.cancelled);
        assert_eq!(result.observed_bytes, 98_307);
        assert_eq!(result.truncation.total_bytes, 98_307);
        assert!(result.output.ends_with("END"));
        let handle = result.artifact_handle.expect("complete replayed artifact");
        assert_eq!(
            artifacts
                .content
                .lock()
                .expect("artifact lock")
                .get(&handle)
                .expect("artifact")
                .len(),
            usize::try_from(result.observed_bytes).expect("observed length"),
            "artifact and terminal byte accounting must include all queued chunks"
        );
    }

    #[tokio::test]
    async fn cancellation_interrupts_a_stalled_artifact_rpc_and_reaps_bash() {
        let workspace = TempWorkspace::new();
        let artifacts = HangingArtifacts::default();
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            cancel_task.cancel();
        });
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            LowTrustLocalBash::new(workspace.0.clone(), &artifacts).execute(
                "head -c 60000 /dev/zero | tr '\\0' x; sleep 10",
                "bash-stalled-artifact",
                cancel,
                Arc::new(|_| {}),
            ),
        )
        .await
        .expect("bash cancellation deadline")
        .expect("cancelled bash result");
        assert!(result.cancelled);
        assert_eq!(result.exit_code, None);
        assert_eq!(
            result.artifact_handle, None,
            "an artifact whose replay/close was not acknowledged must not be exposed"
        );
        assert_eq!(
            artifacts.begin_calls.load(Ordering::Relaxed),
            2,
            "one failed post-stop replay must disable the artifact instead of retrying it for every queued chunk"
        );
    }

    #[tokio::test]
    async fn wall_timeout_is_typed_and_returns_partial_result() {
        let workspace = TempWorkspace::new();
        let artifacts = MemoryArtifacts::default();
        let result = LowTrustLocalBash::new(workspace.0.clone(), &artifacts)
            .with_timeout(Duration::from_millis(50))
            .execute(
                "printf started; sleep 30",
                "bash-4",
                CancellationToken::new(),
                Arc::new(|_| {}),
            )
            .await
            .expect("timed out result");
        assert!(matches!(
            result.resource_limit,
            Some(ResourceLimit::WallTime { .. })
        ));
        assert_eq!(result.exit_code, None);
        assert!(result.output.contains("started"));
    }

    #[tokio::test]
    async fn pipe_reader_quota_is_inclusive_at_exact_byte_boundary() {
        async fn copy_fixture(size: u64) -> (u64, bool, usize) {
            let raw = vec![b'x'; usize::try_from(size).expect("fixture size")];
            let (tx, mut rx) = mpsc::channel(32);
            let observed = Arc::new(AtomicU64::new(0));
            let quota = CancellationToken::new();
            let copy = copy_bounded_chunks(raw.as_slice(), tx, observed, quota.clone());
            let drain = async move {
                let mut drained = 0usize;
                while let Some(chunk) = rx.recv().await {
                    drained = drained.saturating_add(chunk.len());
                }
                drained
            };
            let (copied, drained) = tokio::join!(copy, drain);
            (copied.expect("copy fixture"), quota.is_cancelled(), drained)
        }

        for (size, limited) in [
            (
                super::super::shell_capture::COMMAND_OUTPUT_LIMIT_BYTES - 1,
                false,
            ),
            (
                super::super::shell_capture::COMMAND_OUTPUT_LIMIT_BYTES,
                true,
            ),
            (
                super::super::shell_capture::COMMAND_OUTPUT_LIMIT_BYTES + 1,
                true,
            ),
        ] {
            let (observed, cancelled, drained) = copy_fixture(size).await;
            let expected_observed =
                size.min(super::super::shell_capture::COMMAND_OUTPUT_LIMIT_BYTES);
            assert_eq!(observed, expected_observed);
            assert_eq!(cancelled, limited, "size={size}");
            assert_eq!(
                u64::try_from(drained).expect("drained size"),
                expected_observed
            );
        }
    }

    #[test]
    fn post_drain_quota_classification_is_inclusive() {
        let limit = super::super::shell_capture::COMMAND_OUTPUT_LIMIT_BYTES;
        assert_eq!(output_limit_if_reached(limit - 1), None);
        assert_eq!(
            output_limit_if_reached(limit),
            Some(ResourceLimit::OutputBytes {
                observed: limit,
                limit,
            })
        );
        assert_eq!(
            output_limit_if_reached(limit + 1),
            Some(ResourceLimit::OutputBytes {
                observed: limit + 1,
                limit,
            })
        );
    }

    #[tokio::test]
    async fn output_quota_drains_all_pipe_bytes_observed_before_kill() {
        let workspace = TempWorkspace::new();
        let artifacts = MemoryArtifacts::default();
        let result = tokio::time::timeout(
            Duration::from_secs(15),
            LowTrustLocalBash::new(workspace.0.clone(), &artifacts).execute(
                "head -c 11000000 /dev/zero | tr '\\0' x",
                "bash-pipe-quota",
                CancellationToken::new(),
                Arc::new(|_| {}),
            ),
        )
        .await
        .expect("quota execution deadline")
        .expect("quota result");
        assert!(matches!(
            result.resource_limit,
            Some(ResourceLimit::OutputBytes {
                observed,
                limit: super::super::shell_capture::COMMAND_OUTPUT_LIMIT_BYTES,
            }) if observed == result.observed_bytes
                && observed >= super::super::shell_capture::COMMAND_OUTPUT_LIMIT_BYTES
        ));
        assert!(result.is_consistent());
        let handle = result.artifact_handle.expect("quota artifact");
        assert_eq!(
            artifacts
                .content
                .lock()
                .expect("artifact lock")
                .get(&handle)
                .expect("artifact")
                .len(),
            usize::try_from(result.observed_bytes).expect("observed length"),
            "every byte measured at the pipe-reader boundary must reach the artifact"
        );
    }
}
