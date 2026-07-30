//! Same-binary executor and artifact-broker service modes.

use std::{
    env,
    ffi::{CString, OsStr, OsString},
    fs::{File, OpenOptions},
    future::{Future, poll_fn},
    mem::MaybeUninit,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    os::unix::ffi::OsStrExt,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Component, Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use regex::Regex;
use serde::Serialize;
use serde_json::Value;
use tokio::{
    io::unix::AsyncFd,
    io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf},
    net::{UnixListener, UnixStream},
    sync::{Semaphore, mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use super::{
    ArtifactBroker, ArtifactBrokerClient, ArtifactOperation, ArtifactResponse, ExecutorOperation,
    ExecutorResponse, InputRoute, MAX_RPC_LINE_BYTES, RpcError, RpcFrame, RpcLifecycleTracker,
    RpcRequest, decode_rpc_line, encode_rpc_frame,
    manager::{
        CancelDecision, ExecutionLease, ExecutionRegistration, ExecutorManager, PendingExecution,
        RPC_REPLAY_OUTCOME_UNAVAILABLE_CODE,
    },
    protocol::RPC_BOOT_UNIQUENESS_EXHAUSTED_CODE,
    resolve_input,
};
use crate::runtime::contracts::{PersonalityAgentId, RpcIdentity};
use crate::tools::{
    ResourceLimit, ToolError,
    bash::{BashExecutionResult, LowTrustLocalBash},
    fs::WorkspaceFs,
    truncate::{TruncationOptions, truncate_tail},
};

const UPDATE_CHANNEL_CAPACITY: usize = 32;
const BROKER_CONNECTION_CAPACITY: usize = 32;
const BROKER_BLOCKING_WORK_CAPACITY: usize = 8;
const BROKER_EXCHANGE_DEADLINE: Duration = Duration::from_secs(2);
const EXECUTOR_UPDATE_WRITE_DEADLINE: Duration = Duration::from_millis(5);
const EXECUTOR_TERMINAL_WRITE_DEADLINE: Duration = Duration::from_secs(2);
// Service stdout is a nonblocking pipe in production. Keeping each volatile
// update within PIPE_BUF makes a timed-out write all-or-nothing, so dropping
// progress can never leave a partial JSON frame ahead of the terminal.
const MAX_ATOMIC_UPDATE_FRAME_BYTES: usize = 4_096;
const EXECUTOR_REAP_DEADLINE: Duration = Duration::from_secs(2);
const EXECUTOR_CANCEL_SETTLEMENT_DEADLINE: Duration = Duration::from_secs(3);
const EXECUTOR_CONNECTION_CAPACITY: usize = 32;
const EXECUTOR_CONTROL_CONNECTION_CAPACITY: usize = 32;
const EXECUTOR_OPERATION_CAPACITY: usize = 8;
const EXECUTOR_BLOCKING_FS_WORKER_CAPACITY: usize = 4;
const EXECUTOR_BLOCKING_FS_TOTAL_CAPACITY: usize = 8;
const EXECUTOR_INITIAL_FRAME_DEADLINE: Duration = Duration::from_secs(1);
const EXECUTOR_CONNECTION_DEADLINE: Duration = Duration::from_secs(135);
const SOCKET_CONNECT_DEADLINE: Duration = Duration::from_secs(1);
const SOCKET_PATH_REVALIDATION_INTERVAL: Duration = Duration::from_secs(1);
const SOCKET_READINESS_RETRY_LIMIT: usize = 50;
const SOCKET_READINESS_RETRY_DELAY: Duration = Duration::from_millis(100);
type BashUpdateCallback = Arc<dyn Fn(Value) + Send + Sync>;

struct OwnedUnixListener {
    listener: UnixListener,
    trusted_path: TrustedSocketPath,
    bound_address: PathBuf,
    socket_identity: UnixMetadataSnapshot,
    // The stable adjacent inode is intentionally retained for the complete
    // listener lifetime. Cooperating starters may inspect it but never unlink
    // it, avoiding a lock-file replacement race during handoff.
    _ownership_lock: File,
}

struct TrustedSocketPath {
    path: PathBuf,
    file_name: OsString,
    directories: Vec<PinnedDirectory>,
}

struct PinnedDirectory {
    directory: File,
    name_in_parent: Option<OsString>,
    identity: UnixMetadataSnapshot,
    final_parent: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct UnixMetadataSnapshot {
    dev: u64,
    ino: u64,
    mode: u32,
    nlink: u64,
    uid: u32,
}

struct ExecutorInput<R> {
    read: R,
    first_line: Option<Vec<u8>>,
    close_after_primary: bool,
    #[cfg(test)]
    test_controls: ExecutorTestControls,
}

impl<R> ExecutorInput<R> {
    fn new(read: R) -> Self {
        Self {
            read,
            first_line: None,
            close_after_primary: false,
            #[cfg(test)]
            test_controls: ExecutorTestControls::default(),
        }
    }

    fn prefetched(read: R, first_line: Vec<u8>) -> Self {
        Self {
            read,
            first_line: Some(first_line),
            close_after_primary: true,
            #[cfg(test)]
            test_controls: ExecutorTestControls::default(),
        }
    }

    #[cfg(test)]
    fn with_test_controls(read: R, test_controls: ExecutorTestControls) -> Self {
        Self {
            read,
            first_line: None,
            close_after_primary: false,
            test_controls,
        }
    }
}

/// Wait until a Unix-domain endpoint accepts a connection. A missing endpoint
/// may still be starting; every other error fails closed.
async fn wait_for_unix_socket(path: &Path, label: &str) -> Result<()> {
    for attempt in 0..SOCKET_READINESS_RETRY_LIMIT {
        match timeout(SOCKET_CONNECT_DEADLINE, UnixStream::connect(path)).await {
            Ok(Ok(_)) => return Ok(()),
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(Err(error)) => {
                bail!(
                    "{label} socket {} is not accepting connections: {error}",
                    path.display()
                );
            }
            Err(_) => {}
        }
        if attempt + 1 < SOCKET_READINESS_RETRY_LIMIT {
            tokio::time::sleep(SOCKET_READINESS_RETRY_DELAY).await;
        }
    }
    bail!("{label} socket {} did not become accepting", path.display())
}

/// Bind without stealing a live endpoint. Only a connection-refused path that
/// is itself a Unix socket is eligible for stale-socket cleanup.
async fn bind_unix_listener(path: &Path, label: &str) -> Result<OwnedUnixListener> {
    let trusted_path = TrustedSocketPath::open(path, label)?;
    let ownership_lock = acquire_socket_ownership(&trusted_path, label)?;
    trusted_path.validate(label)?;
    match timeout(SOCKET_CONNECT_DEADLINE, UnixStream::connect(path)).await {
        Ok(Ok(_)) => {
            bail!(
                "{label} socket {} is already owned by a live listener",
                path.display()
            );
        }
        Ok(Err(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Ok(Err(error)) if error.raw_os_error() == Some(libc::ECONNREFUSED) => {
            trusted_path.validate(label)?;
            let metadata = trusted_path.socket_entry(label)?;
            if !metadata.is_socket()
                || metadata.uid != unsafe { libc::geteuid() }
                || metadata.nlink != 1
                || (metadata.mode & 0o777 != 0o660 && metadata.mode & 0o022 != 0)
            {
                bail!(
                    "{label} socket path {} is not a trusted stale Unix socket",
                    path.display()
                );
            }
            trusted_path.unlink_socket(label)?;
            trusted_path.validate(label)?;
        }
        Ok(Err(error)) => {
            bail!(
                "{label} socket {} is not safely replaceable: {error}",
                path.display()
            );
        }
        Err(_) => {
            bail!(
                "{label} socket {} connect timed out; refusing to steal a potentially live listener",
                path.display()
            );
        }
    }
    trusted_path.validate(label)?;
    // Linux has no bindat(2). Resolve the final socket name through the
    // retained parent descriptor so an ancestor rename between validation and
    // bind cannot redirect creation into a replacement path.
    let bound_address = trusted_path.descriptor_relative_socket_path();
    let listener = UnixListener::bind(&bound_address).with_context(|| {
        format!(
            "failed to bind {label} socket {} through its pinned parent",
            path.display()
        )
    })?;
    trusted_path.validate(label)?;
    // Executor/runtime and broker/executor run under separate service UIDs.
    // Deployment supplies a listener-UID-owned, non-peer-writable IPC
    // subdirectory whose group permits the authenticated peer to traverse it.
    chmod_at(
        trusted_path.parent_directory(),
        &trusted_path.file_name,
        0o660,
    )
    .with_context(|| format!("failed to restrict {label} socket {}", path.display()))?;
    let socket_identity = trusted_path.socket_entry(label)?;
    let owned = OwnedUnixListener {
        listener,
        trusted_path,
        bound_address,
        socket_identity,
        _ownership_lock: ownership_lock,
    };
    owned.verify(label)?;

    // Prove that the pinned directory entry routes to this listener before it
    // is exposed to the service loop. Pathname metadata and a socket fd use
    // different inode namespaces on Linux, so local_addr plus a real
    // connect/accept exchange is the authoritative binding check.
    let probe = timeout(SOCKET_CONNECT_DEADLINE, UnixStream::connect(path))
        .await
        .with_context(|| format!("{label} socket post-bind probe timed out"))?
        .with_context(|| format!("{label} socket post-bind probe failed"))?;
    let (accepted_probe, _) = timeout(SOCKET_CONNECT_DEADLINE, owned.listener.accept())
        .await
        .with_context(|| format!("{label} listener did not accept its post-bind probe"))?
        .with_context(|| format!("{label} listener post-bind accept failed"))?;
    drop((probe, accepted_probe));
    owned.verify(label)?;
    Ok(owned)
}

impl OwnedUnixListener {
    fn verify(&self, label: &str) -> Result<()> {
        self.trusted_path.validate(label)?;
        let entry = self.trusted_path.socket_entry(label)?;
        let effective_uid = unsafe { libc::geteuid() };
        if entry != self.socket_identity
            || !entry.is_socket()
            || entry.uid != effective_uid
            || entry.mode & 0o777 != 0o660
            || entry.nlink != 1
        {
            bail!(
                "{label} socket {} no longer matches its pinned bound directory entry",
                self.trusted_path.path.display()
            );
        }
        let local_path = self
            .listener
            .local_addr()
            .context("failed to inspect bound Unix listener address")?
            .as_pathname()
            .map(Path::to_owned);
        if local_path.as_deref() != Some(self.bound_address.as_path()) {
            bail!(
                "{label} listener address is not rooted at the pinned parent for {}",
                self.trusted_path.path.display()
            );
        }
        Ok(())
    }

    async fn accept_verified(
        &self,
        label: &str,
    ) -> Result<(UnixStream, tokio::net::unix::SocketAddr)> {
        loop {
            self.verify(label)?;
            match timeout(SOCKET_PATH_REVALIDATION_INTERVAL, self.listener.accept()).await {
                Ok(Ok(accepted)) => {
                    self.verify(label)?;
                    return Ok(accepted);
                }
                Ok(Err(error)) => {
                    return Err(error)
                        .with_context(|| format!("failed to accept {label} connection"));
                }
                Err(_) => {}
            }
        }
    }
}

impl TrustedSocketPath {
    fn open(path: &Path, label: &str) -> Result<Self> {
        if !path.is_absolute() {
            bail!("{label} socket path {} is not absolute", path.display());
        }
        let file_name = path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("{label} socket path has no file name"))?
            .to_os_string();
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("{label} socket path has no parent directory"))?;
        let names = parent
            .strip_prefix(Path::new("/"))
            .context("absolute socket parent did not have a root prefix")?
            .components()
            .map(|component| match component {
                Component::Normal(name) => Ok(name.to_os_string()),
                Component::CurDir => Ok(OsString::new()),
                Component::RootDir | Component::ParentDir | Component::Prefix(_) => {
                    Err(anyhow::anyhow!(
                        "{label} socket parent {} is not a normalized absolute Unix path",
                        parent.display()
                    ))
                }
            })
            .filter(|result| {
                result
                    .as_ref()
                    .map_or(true, |name| !name.as_os_str().is_empty())
            })
            .collect::<Result<Vec<_>>>()?;

        let root = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open("/")
            .with_context(|| format!("failed to open root while pinning {label} socket path"))?;
        let mut directories = vec![PinnedDirectory {
            identity: UnixMetadataSnapshot::from_file(&root)?,
            directory: root,
            name_in_parent: None,
            final_parent: names.is_empty(),
        }];
        for (index, name) in names.iter().enumerate() {
            let parent_directory = &directories
                .last()
                .expect("pinned socket chain always contains root")
                .directory;
            let directory = openat_file(
                parent_directory,
                name,
                libc::O_RDONLY
                    | libc::O_CLOEXEC
                    | libc::O_DIRECTORY
                    | libc::O_NOFOLLOW
                    | libc::O_NONBLOCK,
                0,
            )
            .with_context(|| {
                format!(
                    "failed to securely open {label} socket parent component {}",
                    name.to_string_lossy()
                )
            })?;
            directories.push(PinnedDirectory {
                identity: UnixMetadataSnapshot::from_file(&directory)?,
                directory,
                name_in_parent: Some(name.clone()),
                final_parent: index + 1 == names.len(),
            });
        }
        let trusted = Self {
            path: path.to_owned(),
            file_name,
            directories,
        };
        trusted.validate(label)?;
        Ok(trusted)
    }

    fn parent_directory(&self) -> &File {
        &self
            .directories
            .last()
            .expect("pinned socket chain always contains a parent")
            .directory
    }

    fn descriptor_relative_socket_path(&self) -> PathBuf {
        PathBuf::from("/proc/self/fd")
            .join(self.parent_directory().as_raw_fd().to_string())
            .join(&self.file_name)
    }

    fn validate(&self, label: &str) -> Result<()> {
        let effective_uid = unsafe { libc::geteuid() };
        for (index, pinned) in self.directories.iter().enumerate() {
            let current = UnixMetadataSnapshot::from_file(&pinned.directory)?;
            if current.dev != pinned.identity.dev
                || current.ino != pinned.identity.ino
                || current.uid != pinned.identity.uid
                || current.mode != pinned.identity.mode
            {
                bail!(
                    "{label} socket ancestor {} changed after it was pinned",
                    self.path.display()
                );
            }
            validate_pinned_directory(current, effective_uid, pinned.final_parent, label)?;
            if let Some(name) = &pinned.name_in_parent {
                let parent = &self.directories[index - 1].directory;
                let entry = stat_at(parent, name).with_context(|| {
                    format!(
                        "failed to revalidate {label} socket ancestor component {}",
                        name.to_string_lossy()
                    )
                })?;
                if !entry.same_directory_entry(current) {
                    bail!(
                        "{label} socket ancestor component {} was renamed or replaced",
                        name.to_string_lossy()
                    );
                }
            } else {
                let root = std::fs::symlink_metadata("/")
                    .context("failed to revalidate root directory")?;
                let root = UnixMetadataSnapshot::from_metadata(&root);
                if !root.same_directory_entry(current) {
                    bail!("root directory changed while validating {label} socket path");
                }
            }
        }
        Ok(())
    }

    fn socket_entry(&self, label: &str) -> Result<UnixMetadataSnapshot> {
        stat_at(self.parent_directory(), &self.file_name).with_context(|| {
            format!(
                "failed to inspect {label} socket directory entry {}",
                self.path.display()
            )
        })
    }

    fn unlink_socket(&self, label: &str) -> Result<()> {
        unlink_at(self.parent_directory(), &self.file_name).with_context(|| {
            format!(
                "failed to remove stale {label} socket {}",
                self.path.display()
            )
        })
    }
}

fn validate_pinned_directory(
    metadata: UnixMetadataSnapshot,
    effective_uid: u32,
    final_parent: bool,
    label: &str,
) -> Result<()> {
    if !metadata.is_directory() || metadata.nlink == 0 {
        bail!("{label} socket path contains a detached non-directory ancestor");
    }
    if final_parent {
        if metadata.uid != effective_uid
            || metadata.mode & 0o300 != 0o300
            || metadata.mode & 0o022 != 0
        {
            bail!(
                "{label} socket parent must be listener-uid-owned, owner-writable/executable, and non-peer-writable"
            );
        }
        return Ok(());
    }

    let listener_owned = metadata.uid == effective_uid && metadata.mode & 0o022 == 0;
    let root_owned_traversal = metadata.uid == 0 && metadata.mode & 0o022 == 0;
    // `/tmp`-style traversal is the sole peer-writable exception: root owns
    // the directory and the sticky bit prevents peers from replacing an
    // entry owned by the listener uid. The listener-owned descendant itself
    // remains non-peer-writable and is pinned by fd.
    let root_owned_sticky_traversal =
        metadata.uid == 0 && metadata.mode & libc::S_ISVTX != 0 && metadata.mode & 0o002 != 0;
    if metadata.mode & 0o111 == 0
        || !(listener_owned || root_owned_traversal || root_owned_sticky_traversal)
    {
        bail!(
            "{label} socket ancestor is neither trusted root-owned traversal nor listener-owned non-peer-writable traversal"
        );
    }
    Ok(())
}

fn acquire_socket_ownership(path: &TrustedSocketPath, label: &str) -> Result<File> {
    let mut lock_name = path.file_name.clone();
    lock_name.push(".lock");
    let lock = openat_file(
        path.parent_directory(),
        &lock_name,
        libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        0o600,
    )
    .with_context(|| {
        format!(
            "failed to securely open {label} socket ownership lock {}",
            path.path.with_file_name(&lock_name).display()
        )
    })?;
    validate_socket_ownership_lock(&lock, path, &lock_name, label)?;
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        let error = std::io::Error::last_os_error();
        bail!(
            "{label} socket {} is already owned by another starter: {error}",
            path.path.display()
        );
    }
    // Revalidate after locking so an unlink/replacement between open and
    // flock cannot establish a second cooperating ownership inode.
    path.validate(label)?;
    validate_socket_ownership_lock(&lock, path, &lock_name, label)?;
    Ok(lock)
}

fn validate_socket_ownership_lock(
    lock: &File,
    path: &TrustedSocketPath,
    lock_name: &OsStr,
    label: &str,
) -> Result<()> {
    let descriptor = UnixMetadataSnapshot::from_file(lock)?;
    let pathname = stat_at(path.parent_directory(), lock_name).with_context(|| {
        format!(
            "failed to inspect {label} socket ownership lock {}",
            path.path.with_file_name(lock_name).display()
        )
    })?;
    let effective_uid = unsafe { libc::geteuid() };
    if !descriptor.is_regular()
        || descriptor.uid != effective_uid
        || descriptor.nlink != 1
        || descriptor.mode & 0o777 != 0o600
        || pathname != descriptor
    {
        bail!(
            "{label} socket ownership lock {} is not a stable uid-owned 0600 regular file",
            path.path.with_file_name(lock_name).display()
        );
    }
    Ok(())
}

impl UnixMetadataSnapshot {
    fn from_file(file: &File) -> Result<Self> {
        let metadata = file
            .metadata()
            .context("failed to inspect pinned descriptor")?;
        Ok(Self::from_metadata(&metadata))
    }

    fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
            mode: metadata.mode(),
            nlink: metadata.nlink(),
            uid: metadata.uid(),
        }
    }

    fn from_stat(metadata: libc::stat) -> Self {
        Self {
            dev: metadata.st_dev,
            ino: metadata.st_ino,
            mode: metadata.st_mode,
            nlink: metadata.st_nlink,
            uid: metadata.st_uid,
        }
    }

    fn is_directory(self) -> bool {
        self.mode & libc::S_IFMT == libc::S_IFDIR
    }

    fn is_regular(self) -> bool {
        self.mode & libc::S_IFMT == libc::S_IFREG
    }

    fn is_socket(self) -> bool {
        self.mode & libc::S_IFMT == libc::S_IFSOCK
    }

    fn same_directory_entry(self, other: Self) -> bool {
        self.dev == other.dev
            && self.ino == other.ino
            && self.mode == other.mode
            && self.uid == other.uid
            && self.nlink > 0
            && other.nlink > 0
    }
}

fn os_str_cstring(value: &OsStr) -> std::io::Result<CString> {
    CString::new(value.as_bytes())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "path contained NUL"))
}

fn openat_file(
    parent: &File,
    name: &OsStr,
    flags: libc::c_int,
    mode: u32,
) -> std::io::Result<File> {
    let name = os_str_cstring(name)?;
    let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags, mode) };
    if fd == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

fn stat_at(parent: &File, name: &OsStr) -> std::io::Result<UnixMetadataSnapshot> {
    let name = os_str_cstring(name)?;
    let mut metadata = MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } == -1
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(UnixMetadataSnapshot::from_stat(unsafe {
        metadata.assume_init()
    }))
}

fn unlink_at(parent: &File, name: &OsStr) -> std::io::Result<()> {
    let name = os_str_cstring(name)?;
    if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) } == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn chmod_at(parent: &File, name: &OsStr, mode: u32) -> std::io::Result<()> {
    let name = os_str_cstring(name)?;
    if unsafe { libc::fchmodat(parent.as_raw_fd(), name.as_ptr(), mode, 0) } == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
pub(super) struct ExecutorTestControls {
    cancel_stop_delay: Duration,
    cancel_ingested: Option<oneshot::Sender<()>>,
}

#[cfg(test)]
impl ExecutorTestControls {
    pub(super) fn observe_cancel(
        cancel_stop_delay: Duration,
        cancel_ingested: oneshot::Sender<()>,
    ) -> Self {
        Self {
            cancel_stop_delay,
            cancel_ingested: Some(cancel_ingested),
        }
    }
}

#[cfg(test)]
impl Default for ExecutorTestControls {
    fn default() -> Self {
        Self {
            cancel_stop_delay: Duration::ZERO,
            cancel_ingested: None,
        }
    }
}

enum ActiveControl {
    Cancel(String),
    Fatal {
        error: ToolError,
        response: Option<(String, RpcError)>,
    },
}

enum BashExit {
    Completed(Result<BashExecutionResult, ToolError>),
    Cancelled {
        cancel_request_id: String,
        completed: Option<Result<BashExecutionResult, ToolError>>,
    },
    Fatal {
        error: ToolError,
        response: Option<(String, RpcError)>,
        completed: Option<Result<BashExecutionResult, ToolError>>,
    },
}

struct WriterMessage {
    bytes: Vec<u8>,
    acknowledgement: Option<oneshot::Sender<Result<(), String>>>,
}

struct ExecutorWriter {
    updates: mpsc::Sender<WriterMessage>,
    terminals: mpsc::Sender<WriterMessage>,
    // A terminal ACK is the service's sequential exchange boundary. Hold this
    // gate while admitting progress so no update can race the terminal fence
    // after the writer has selected it.
    terminal_started: Arc<Mutex<bool>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProgressWriteGuarantee {
    MayWritePartial,
    AtomicAllOrNothing,
}

struct NonblockingStdout {
    fd: AsyncFd<OwnedFd>,
}

struct NonblockingStdin {
    fd: AsyncFd<OwnedFd>,
}

impl AsyncRead for NonblockingStdin {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            let mut ready = match self.fd.poll_read_ready(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(ready)) => ready,
            };
            match ready.try_io(|fd| {
                let destination = buffer.initialize_unfilled();
                let read = unsafe {
                    libc::read(
                        fd.get_ref().as_raw_fd(),
                        destination.as_mut_ptr().cast(),
                        destination.len(),
                    )
                };
                if read == -1 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(read as usize)
                }
            }) {
                Ok(Ok(read)) => {
                    buffer.advance(read);
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(error)) if is_interrupted_read_error(&error) => continue,
                Ok(Err(error)) => return Poll::Ready(Err(error)),
                Err(_) => continue,
            }
        }
    }
}

fn is_interrupted_read_error(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::Interrupted
}

impl AsyncWrite for NonblockingStdout {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        loop {
            let mut ready = match self.fd.poll_write_ready(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(ready)) => ready,
            };
            match ready.try_io(|fd| {
                let written = unsafe {
                    libc::write(
                        fd.get_ref().as_raw_fd(),
                        buffer.as_ptr().cast(),
                        buffer.len(),
                    )
                };
                if written == -1 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(written as usize)
                }
            }) {
                Ok(result) => return Poll::Ready(result),
                Err(_) => continue,
            }
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _context: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _context: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl ExecutorWriter {
    fn start<W>(write: W) -> (Self, JoinHandle<()>)
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        Self::start_with_progress_guarantee(write, ProgressWriteGuarantee::MayWritePartial)
    }

    fn start_atomic_progress<W>(write: W) -> (Self, JoinHandle<()>)
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        Self::start_with_progress_guarantee(write, ProgressWriteGuarantee::AtomicAllOrNothing)
    }

    fn start_with_progress_guarantee<W>(
        mut write: W,
        progress_write_guarantee: ProgressWriteGuarantee,
    ) -> (Self, JoinHandle<()>)
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (updates, mut update_receiver) =
            mpsc::channel::<WriterMessage>(UPDATE_CHANNEL_CAPACITY);
        // Progress is volatile. A dedicated terminal slot ensures queued
        // progress can never consume the capacity needed by authoritative
        // completion.
        let (terminals, mut terminal_receiver) = mpsc::channel::<WriterMessage>(1);
        let terminal_started = Arc::new(Mutex::new(false));
        let writer_terminal_started = terminal_started.clone();
        let task = tokio::spawn(async move {
            let mut terminal_started = false;
            loop {
                let message = if terminal_started {
                    terminal_receiver.recv().await
                } else {
                    tokio::select! {
                        biased;
                        message = terminal_receiver.recv() => message,
                        message = update_receiver.recv() => message,
                    }
                };
                let Some(message) = message else {
                    return;
                };
                let terminal = message.acknowledgement.is_some();
                terminal_started |= terminal;
                if terminal {
                    // Progress admitted before the terminal was selected is
                    // volatile and must not be written after the authoritative
                    // frame. New progress is blocked by the shared gate until
                    // this terminal has been acknowledged.
                    while update_receiver.try_recv().is_ok() {}
                }
                let deadline = if terminal {
                    EXECUTOR_TERMINAL_WRITE_DEADLINE
                } else {
                    EXECUTOR_UPDATE_WRITE_DEADLINE
                };
                let result = if !terminal
                    && progress_write_guarantee == ProgressWriteGuarantee::MayWritePartial
                {
                    // Poll exactly one write future. If it remains Pending until
                    // the deadline, AsyncWrite has accepted no bytes and this
                    // volatile frame can be dropped without poisoning JSONL.
                    // Once Ready reports a prefix or error, the transport epoch
                    // is no longer safe for a later authoritative terminal.
                    match timeout(deadline, write.write(&message.bytes)).await {
                        Ok(Ok(written)) if written == message.bytes.len() => {
                            match timeout(deadline, write.flush()).await {
                                Ok(Ok(())) => Ok(()),
                                Ok(Err(error)) => Err(error.to_string()),
                                Err(_) => Err("executor output flush deadline elapsed".to_owned()),
                            }
                        }
                        Ok(Ok(_)) => Err("executor output write was partial".to_owned()),
                        Ok(Err(error)) => Err(error.to_string()),
                        Err(_) => {
                            tracing::warn!(
                                "dropping volatile executor progress update: write deadline elapsed before accepting bytes"
                            );
                            continue;
                        }
                    }
                } else {
                    match timeout(deadline, async {
                        write.write_all(&message.bytes).await?;
                        write.flush().await
                    })
                    .await
                    {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(error)) => Err(error.to_string()),
                        Err(_) => Err("executor output write deadline elapsed".to_owned()),
                    }
                };
                if terminal {
                    // Re-enable progress before sending the ACK. The service
                    // cannot begin its next request until that ACK is observed,
                    // so no next-request update can be drained in this epoch.
                    if let Ok(mut started) = writer_terminal_started.lock() {
                        *started = false;
                    }
                    terminal_started = false;
                }
                if let Some(acknowledgement) = message.acknowledgement {
                    let _ = acknowledgement.send(result.clone());
                }
                if result.is_err() {
                    if terminal {
                        return;
                    }
                    if progress_write_guarantee == ProgressWriteGuarantee::AtomicAllOrNothing {
                        tracing::warn!(
                            "dropping volatile executor progress update: atomic write unavailable"
                        );
                    } else {
                        tracing::warn!(
                            "executor progress write failed; permanently closing writer epoch"
                        );
                        // A generic AsyncWrite transport may have accepted a
                        // prefix before stalling or failing. Appending a later
                        // frame would turn that prefix into corrupt JSONL.
                        return;
                    }
                }
            }
        });
        (
            Self {
                updates,
                terminals,
                terminal_started,
            },
            task,
        )
    }

    fn try_update<T: Serialize>(&self, frame: &RpcFrame<T>) -> Result<(), ToolError> {
        let bytes = match encode_rpc_frame(frame) {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::warn!(error = %error, "dropping volatile executor progress update: encode failed");
                return Ok(());
            }
        };
        if bytes.len() > MAX_ATOMIC_UPDATE_FRAME_BYTES {
            tracing::warn!(
                "dropping volatile executor progress update: frame exceeds atomic pipe write"
            );
            return Ok(());
        }
        let terminal_started = self
            .terminal_started
            .lock()
            .map_err(|_| ToolError::Protocol("executor writer state lock poisoned".to_owned()))?;
        if *terminal_started {
            tracing::warn!("dropping volatile executor progress update: terminal in flight");
            return Ok(());
        }
        match self.updates.try_send(WriterMessage {
            bytes,
            acknowledgement: None,
        }) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!("dropping volatile executor progress update: writer queue full");
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!("dropping volatile executor progress update: writer unavailable");
                Ok(())
            }
        }
    }

    async fn terminal(
        &self,
        identity: &RpcIdentity,
        request_id: String,
        result: Result<ExecutorResponse, RpcError>,
    ) -> Result<(), ToolError> {
        let frame = RpcFrame::Terminal {
            personality_agent_id: identity.personality_agent_id().clone(),
            generation: identity.generation().to_wire(),
            nonce: identity.nonce().as_str().to_owned(),
            request_id: request_id.clone(),
            result,
        };
        let bytes = match encode_rpc_frame(&frame) {
            Ok(encoded) => encoded,
            Err(ToolError::Protocol(_)) => {
                encode_rpc_frame(&RpcFrame::<ExecutorResponse>::Terminal {
                    personality_agent_id: identity.personality_agent_id().clone(),
                    generation: identity.generation().to_wire(),
                    nonce: identity.nonce().as_str().to_owned(),
                    request_id,
                    result: Err(bounded_error("response_too_large")),
                })?
            }
            Err(error) => return Err(error),
        };
        let (acknowledgement, received) = oneshot::channel();
        {
            let mut terminal_started = self.terminal_started.lock().map_err(|_| {
                ToolError::Protocol("executor writer state lock poisoned".to_owned())
            })?;
            *terminal_started = true;
        }
        let sent = timeout(
            EXECUTOR_TERMINAL_WRITE_DEADLINE,
            self.terminals.send(WriterMessage {
                bytes,
                acknowledgement: Some(acknowledgement),
            }),
        )
        .await
        .map_err(|_| io_error("executor output queue deadline elapsed"))
        .and_then(|result| result.map_err(|_| io_error("executor output writer unavailable")));
        if let Err(error) = sent {
            if let Ok(mut terminal_started) = self.terminal_started.lock() {
                *terminal_started = false;
            }
            return Err(error);
        }
        timeout(EXECUTOR_TERMINAL_WRITE_DEADLINE, received)
            .await
            .map_err(|_| io_error("executor terminal write deadline elapsed"))?
            .map_err(|_| io_error("executor output writer stopped"))?
            .map_err(|message| io_error(&message))
    }
}

#[derive(Clone)]
struct BlockingWorkRegistry {
    permits: Arc<Semaphore>,
}

#[derive(Clone)]
struct BlockingFsRegistry {
    workers: Arc<Semaphore>,
    total: Arc<Semaphore>,
    #[cfg(test)]
    before_execute: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl BlockingFsRegistry {
    fn new(worker_capacity: usize, total_capacity: usize) -> Self {
        assert!(worker_capacity > 0);
        assert!(total_capacity >= worker_capacity);
        Self {
            workers: Arc::new(Semaphore::new(worker_capacity)),
            total: Arc::new(Semaphore::new(total_capacity)),
            #[cfg(test)]
            before_execute: None,
        }
    }

    fn production() -> Self {
        Self::new(
            EXECUTOR_BLOCKING_FS_WORKER_CAPACITY,
            EXECUTOR_BLOCKING_FS_TOTAL_CAPACITY,
        )
    }

    #[cfg(test)]
    fn with_test_hook(
        worker_capacity: usize,
        total_capacity: usize,
        before_execute: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self {
            before_execute: Some(before_execute),
            ..Self::new(worker_capacity, total_capacity)
        }
    }

    async fn execute<T, F>(&self, operation: F) -> Result<T, ToolError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, ToolError> + Send + 'static,
    {
        let total_permit = self
            .total
            .clone()
            .try_acquire_owned()
            .map_err(|_| ToolError::ResourceLimit(ResourceLimit::Concurrency))?;
        let worker_permit = self
            .workers
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| io_error("executor filesystem blocking-work registry closed"))?;
        #[cfg(test)]
        let before_execute = self.before_execute.clone();
        tokio::task::spawn_blocking(move || {
            let _permits = (total_permit, worker_permit);
            #[cfg(test)]
            if let Some(before_execute) = before_execute {
                before_execute();
            }
            operation()
        })
        .await
        .map_err(|_| {
            ToolError::RpcIndeterminate("executor filesystem blocking worker stopped".to_owned())
        })?
    }

    async fn open_workspace(&self, workspace: PathBuf) -> Result<Arc<WorkspaceFs>, ToolError> {
        self.execute(move || WorkspaceFs::open(&workspace).map(Arc::new))
            .await
    }

    #[cfg(test)]
    fn available_total_permits(&self) -> usize {
        self.total.available_permits()
    }

    #[cfg(test)]
    fn available_worker_permits(&self) -> usize {
        self.workers.available_permits()
    }
}

impl BlockingWorkRegistry {
    fn new(capacity: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(capacity)),
        }
    }

    async fn reserve(&self) -> Result<tokio::sync::OwnedSemaphorePermit, ToolError> {
        self.permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| io_error("artifact blocking-work registry closed"))
    }

    async fn execute_reserved(
        broker: Arc<ArtifactBroker>,
        personality_agent_id: PersonalityAgentId,
        operation: ArtifactOperation,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<ArtifactResponse, ToolError> {
        tokio::task::spawn_blocking(move || broker.execute(&personality_agent_id, operation))
            .await
            .map_err(|_| io_error("artifact blocking worker stopped"))?
    }
}

pub async fn run_tool_executor_mode() -> Result<()> {
    let identity = identity_from_env()?;
    let workspace = required_path("SUMI_WORKSPACE")?;
    let broker_socket = required_path("SUMI_ARTIFACT_BROKER_SOCKET")?;
    let blocking_fs = BlockingFsRegistry::production();
    let fs = blocking_fs
        .open_workspace(workspace.clone())
        .await
        .context("failed to open executor workspace")?;
    let broker = Arc::new(ArtifactBrokerClient::new(broker_socket, identity.clone()));
    let stdin = nonblocking_stdin().context("failed to take ownership of executor stdin")?;
    let stdout = nonblocking_stdout().context("failed to take ownership of executor stdout")?;
    let manager = ExecutorManager::new(EXECUTOR_OPERATION_CAPACITY);
    run_executor_service_with_writer(
        ExecutorInput::new(stdin),
        ExecutorWriter::start_atomic_progress(stdout),
        identity,
        workspace,
        fs,
        broker,
        manager,
        blocking_fs,
    )
    .await
}

/// Long-lived Unix executor endpoint for one personality-agent VM process.
///
/// Transport sessions are independent, while admission, request/execution
/// uniqueness, cancellation, and terminal-outcome retention are owned by the
/// one manager created before the accept loop.
pub async fn run_tool_executor_socket_mode() -> Result<()> {
    let identity = identity_from_env()?;
    let workspace = required_path("SUMI_WORKSPACE")?;
    let broker_socket = required_path("SUMI_ARTIFACT_BROKER_SOCKET")?;
    let executor_socket = required_path("SUMI_EXECUTOR_SOCKET")?;
    let blocking_fs = BlockingFsRegistry::production();
    let fs = blocking_fs
        .open_workspace(workspace.clone())
        .await
        .context("failed to open executor workspace")?;
    let broker = Arc::new(ArtifactBrokerClient::new(broker_socket, identity.clone()));

    wait_for_unix_socket(broker.socket(), "artifact broker")
        .await
        .context("artifact broker socket is not ready")?;
    let listener = bind_unix_listener(&executor_socket, "executor").await?;
    let handshakes = Arc::new(Semaphore::new(EXECUTOR_CONNECTION_CAPACITY));
    let ordinary_connections = Arc::new(Semaphore::new(EXECUTOR_CONNECTION_CAPACITY));
    let control_connections = Arc::new(Semaphore::new(EXECUTOR_CONTROL_CONNECTION_CAPACITY));
    let manager = ExecutorManager::new(EXECUTOR_OPERATION_CAPACITY);

    loop {
        // Stop accepting at the bounded handshake frontier instead of
        // accepting and dropping the next potentially valid peer. A queued
        // peer remains in the kernel backlog until an idle handshake expires.
        let handshake_permit = handshakes
            .clone()
            .acquire_owned()
            .await
            .context("executor initial-frame admission is closed")?;
        let (stream, _) = listener.accept_verified("executor").await?;
        let identity = identity.clone();
        let workspace = workspace.clone();
        let fs = fs.clone();
        let broker = broker.clone();
        let manager = manager.clone();
        let blocking_fs = blocking_fs.clone();
        let ordinary_connections = ordinary_connections.clone();
        let control_connections = control_connections.clone();
        tokio::spawn(async move {
            let (read, write) = stream.into_split();
            let mut read = BufReader::new(read);
            let first_line =
                match timeout(EXECUTOR_INITIAL_FRAME_DEADLINE, read_frame(&mut read)).await {
                    Ok(Ok(Some(line))) => line,
                    Ok(Ok(None)) => return,
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "executor socket rejected initial frame");
                        return;
                    }
                    Err(_) => {
                        tracing::warn!("executor socket initial-frame deadline elapsed");
                        return;
                    }
                };
            let decoded = match decode_executor_request(&first_line, &identity) {
                Ok(decoded) => decoded,
                Err(error) => {
                    tracing::warn!(%error, "executor socket rejected unauthenticated initial frame");
                    return;
                }
            };
            let control = matches!(
                decoded,
                Ok(RpcRequest {
                    operation: ExecutorOperation::Cancel { .. } | ExecutorOperation::Health {},
                    ..
                })
            );
            drop(handshake_permit);
            let permit = if control {
                control_connections.try_acquire_owned()
            } else {
                ordinary_connections.try_acquire_owned()
            };
            let Ok(connection_permit) = permit else {
                tracing::warn!(
                    control,
                    "executor authenticated connection capacity reached"
                );
                return;
            };
            let result = timeout(EXECUTOR_CONNECTION_DEADLINE, async {
                run_executor_service_with_writer(
                    ExecutorInput::prefetched(read, first_line),
                    ExecutorWriter::start(write),
                    identity,
                    workspace,
                    fs,
                    broker,
                    manager,
                    blocking_fs,
                )
                .await
            })
            .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(%error, "executor socket connection closed with error");
                }
                Err(_) => {
                    tracing::warn!(
                        "executor socket connection exceeded its bounded lifetime; active work was cancelled"
                    );
                }
            }
            drop(connection_permit);
        });
    }
}

pub async fn run_artifact_broker_mode() -> Result<()> {
    let identity = identity_from_env()?;
    let root = required_path("SUMI_ARTIFACT_ROOT")?;
    let socket = required_path("SUMI_ARTIFACT_BROKER_SOCKET")?;
    let broker = Arc::new(ArtifactBroker::open(&root).context("failed to open artifact root")?);
    let listener = bind_unix_listener(&socket, "artifact broker").await?;
    let lifecycle = Arc::new(Mutex::new(RpcLifecycleTracker::default()));
    let permits = Arc::new(Semaphore::new(BROKER_CONNECTION_CAPACITY));
    let blocking_work = BlockingWorkRegistry::new(BROKER_BLOCKING_WORK_CAPACITY);
    let identity = Arc::new(identity);
    loop {
        let (stream, _) = listener.accept_verified("artifact broker").await?;
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            tracing::warn!("artifact broker connection capacity reached");
            drop(stream);
            continue;
        };
        let identity = identity.clone();
        let broker = broker.clone();
        let lifecycle = lifecycle.clone();
        let blocking_work = blocking_work.clone();
        tokio::spawn(async move {
            let result = timeout(
                BROKER_EXCHANGE_DEADLINE,
                serve_broker_connection(stream, identity, broker, lifecycle, blocking_work),
            )
            .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(%error, "artifact broker rejected connection"),
                Err(_) => tracing::warn!("artifact broker connection deadline elapsed"),
            }
            drop(permit);
        });
    }
}

async fn serve_broker_connection(
    stream: UnixStream,
    identity: Arc<RpcIdentity>,
    broker: Arc<ArtifactBroker>,
    lifecycle: Arc<Mutex<RpcLifecycleTracker>>,
    blocking_work: BlockingWorkRegistry,
) -> Result<(), ToolError> {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let Some(line) = read_frame(&mut read).await? else {
        return Err(ToolError::Protocol(
            "artifact broker connection closed without a request".to_owned(),
        ));
    };
    let request = match decode_artifact_request(&line, &identity)? {
        Ok(request) => request,
        Err((request_id, error)) => {
            {
                let mut lifecycle = lifecycle.lock().map_err(|_| {
                    ToolError::Protocol("artifact lifecycle lock poisoned".to_owned())
                })?;
                lifecycle.begin_request(&request_id)?;
                lifecycle.accept_terminal(&request_id)?;
            }
            write_frame(
                &mut write,
                &RpcFrame::<ArtifactResponse>::Terminal {
                    personality_agent_id: identity.personality_agent_id().clone(),
                    generation: identity.generation().to_wire(),
                    nonce: identity.nonce().as_str().to_owned(),
                    request_id,
                    result: Err(error),
                },
            )
            .await?;
            write.shutdown().await?;
            return Ok(());
        }
    };
    let blocking_permit = blocking_work.reserve().await?;
    {
        let mut lifecycle = lifecycle
            .lock()
            .map_err(|_| ToolError::Protocol("artifact lifecycle lock poisoned".to_owned()))?;
        lifecycle.begin_request(&request.request_id)?;
    }
    let request_id = request.request_id;
    let personality_agent_id = identity.personality_agent_id().clone();
    let (result_tx, result_rx) = oneshot::channel();
    let job_request_id = request_id.clone();
    tokio::spawn(async move {
        let result = BlockingWorkRegistry::execute_reserved(
            broker,
            personality_agent_id,
            request.operation,
            blocking_permit,
        )
        .await
        .map_err(rpc_error);
        let lifecycle_result = lifecycle
            .lock()
            .map_err(|_| ToolError::Protocol("artifact lifecycle lock poisoned".to_owned()))
            .and_then(|mut lifecycle| lifecycle.accept_terminal(&job_request_id));
        let _ = result_tx.send(lifecycle_result.map(|()| result));
    });
    // Dropping this receiver on the connection deadline does not cancel the
    // accepted blocking job. It retains its operation receipt and releases the
    // registry permit only after mutation and lifecycle finalization finish.
    let result = result_rx
        .await
        .map_err(|_| io_error("artifact blocking job stopped"))??;
    write_frame(
        &mut write,
        &RpcFrame::Terminal {
            personality_agent_id: identity.personality_agent_id().clone(),
            generation: identity.generation().to_wire(),
            nonce: identity.nonce().as_str().to_owned(),
            request_id,
            result,
        },
    )
    .await?;
    write.shutdown().await?;
    Ok(())
}

pub(super) async fn run_executor_service<R, W>(
    read: R,
    write: W,
    identity: RpcIdentity,
    workspace: PathBuf,
    fs: WorkspaceFs,
    broker: ArtifactBrokerClient,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    run_executor_service_with_manager(
        read,
        write,
        identity,
        workspace,
        fs,
        broker,
        ExecutorManager::new(EXECUTOR_OPERATION_CAPACITY),
    )
    .await
}

async fn run_executor_service_with_manager<R, W>(
    read: R,
    write: W,
    identity: RpcIdentity,
    workspace: PathBuf,
    fs: WorkspaceFs,
    broker: ArtifactBrokerClient,
    manager: Arc<ExecutorManager>,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let blocking_fs = BlockingFsRegistry::production();
    run_executor_service_with_writer(
        ExecutorInput::new(read),
        ExecutorWriter::start(write),
        identity,
        workspace,
        Arc::new(fs),
        Arc::new(broker),
        manager,
        blocking_fs,
    )
    .await
}

#[cfg(test)]
pub(super) async fn run_executor_service_with_cancel_delay<R, W>(
    read: R,
    write: W,
    identity: RpcIdentity,
    workspace: PathBuf,
    fs: WorkspaceFs,
    broker: ArtifactBrokerClient,
    test_controls: ExecutorTestControls,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let blocking_fs = BlockingFsRegistry::production();
    run_executor_service_with_writer(
        ExecutorInput::with_test_controls(read, test_controls),
        ExecutorWriter::start(write),
        identity,
        workspace,
        Arc::new(fs),
        Arc::new(broker),
        ExecutorManager::new(EXECUTOR_OPERATION_CAPACITY),
        blocking_fs,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_executor_service_with_writer<R>(
    input: ExecutorInput<R>,
    (writer, writer_task): (ExecutorWriter, JoinHandle<()>),
    identity: RpcIdentity,
    workspace: PathBuf,
    fs: Arc<WorkspaceFs>,
    broker: Arc<ArtifactBrokerClient>,
    manager: Arc<ExecutorManager>,
    blocking_fs: BlockingFsRegistry,
) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let result = run_executor_loop(
        input,
        &writer,
        identity,
        workspace,
        fs,
        broker,
        manager,
        blocking_fs,
    )
    .await;
    writer_task.abort();
    let _ = timeout(EXECUTOR_TERMINAL_WRITE_DEADLINE, writer_task).await;
    result
}

#[allow(clippy::too_many_arguments)]
async fn run_executor_loop<R>(
    input: ExecutorInput<R>,
    writer: &ExecutorWriter,
    identity: RpcIdentity,
    workspace: PathBuf,
    fs: Arc<WorkspaceFs>,
    broker: Arc<ArtifactBrokerClient>,
    manager: Arc<ExecutorManager>,
    blocking_fs: BlockingFsRegistry,
) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let ExecutorInput {
        read,
        mut first_line,
        close_after_primary,
        #[cfg(test)]
        mut test_controls,
    } = input;
    let mut read = BufReader::new(read);
    loop {
        let line = match first_line.take() {
            Some(line) => line,
            None => {
                let Some(line) = read_frame(&mut read).await? else {
                    return Ok(());
                };
                line
            }
        };
        let request = match decode_executor_request(&line, &identity)? {
            Ok(request) => request,
            Err((request_id, error)) => {
                let result = match manager.reject_request(&request_id) {
                    Ok(()) => Err(error),
                    Err(error) if is_boot_uniqueness_exhausted(&error) => Err(rpc_error(error)),
                    Err(error) => return Err(error.into()),
                };
                writer.terminal(&identity, request_id, result).await?;
                if close_after_primary {
                    return Ok(());
                }
                continue;
            }
        };
        match request.operation {
            ExecutorOperation::Health {} => {
                writer
                    .terminal(
                        &identity,
                        request.request_id,
                        Ok(ExecutorResponse::Healthy {}),
                    )
                    .await?;
            }
            ExecutorOperation::Bash {
                command,
                execution_id,
            } => {
                let cancel = CancellationToken::new();
                let registration = match manager.register_execution(
                    request.request_id.clone(),
                    execution_id.clone(),
                    Some(cancel),
                ) {
                    Ok(registration) => registration,
                    Err(error) if is_typed_admission_error(&error) => {
                        writer
                            .terminal(&identity, request.request_id, Err(rpc_error(error)))
                            .await?;
                        if close_after_primary {
                            return Ok(());
                        }
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
                let mut execution = match registration {
                    ExecutionRegistration::Replay(result) => {
                        writer
                            .terminal(&identity, request.request_id, result)
                            .await?;
                        if close_after_primary {
                            return Ok(());
                        }
                        continue;
                    }
                    ExecutionRegistration::Pending(mut pending) => {
                        match admit_pending_bash(
                            &mut pending,
                            &mut read,
                            &identity,
                            &manager,
                            &execution_id,
                        )
                        .await?
                        {
                            PendingBashAdmission::Admitted(permit) => pending.promote(permit)?,
                            PendingBashAdmission::Cancelled(cancel_request_id) => {
                                let result = cancelled_before_spawn_response();
                                pending.complete(result.clone())?;
                                if let Some(cancel_request_id) = cancel_request_id {
                                    writer
                                        .terminal(
                                            &identity,
                                            cancel_request_id,
                                            Ok(ExecutorResponse::CancelAccepted {}),
                                        )
                                        .await?;
                                }
                                writer
                                    .terminal(&identity, request.request_id, result)
                                    .await?;
                                if close_after_primary {
                                    return Ok(());
                                }
                                continue;
                            }
                        }
                    }
                };
                run_bash_request(
                    &mut read,
                    writer,
                    &identity,
                    &workspace,
                    &broker,
                    &manager,
                    &mut execution,
                    request.request_id,
                    execution_id,
                    command,
                    #[cfg(test)]
                    ExecutorTestControls {
                        cancel_stop_delay: test_controls.cancel_stop_delay,
                        cancel_ingested: test_controls.cancel_ingested.take(),
                    },
                )
                .await?;
            }
            ExecutorOperation::Cancel { execution_id } => {
                let result = match manager.cancel_execution(&request.request_id, &execution_id) {
                    Ok(CancelDecision::Accepted(completed)) => {
                        settle_manager_cancel(completed).await
                    }
                    Ok(CancelDecision::TooLate) => Ok(ExecutorResponse::CancelTooLate {}),
                    Ok(CancelDecision::Unknown) => Err(RpcError {
                        code: "protocol".to_owned(),
                        resource_limit: None,
                    }),
                    Err(error) if is_boot_uniqueness_exhausted(&error) => Err(rpc_error(error)),
                    Err(error) => return Err(error.into()),
                };
                writer
                    .terminal(&identity, request.request_id, result)
                    .await?;
            }
            operation => {
                let execution_id = operation_execution_id(&operation).to_owned();
                let registration = match manager.register_execution(
                    request.request_id.clone(),
                    execution_id,
                    None,
                ) {
                    Ok(registration) => registration,
                    Err(error) if is_typed_admission_error(&error) => {
                        writer
                            .terminal(&identity, request.request_id, Err(rpc_error(error)))
                            .await?;
                        if close_after_primary {
                            return Ok(());
                        }
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
                let execution = match registration {
                    ExecutionRegistration::Replay(result) => {
                        writer
                            .terminal(&identity, request.request_id, result)
                            .await?;
                        if close_after_primary {
                            return Ok(());
                        }
                        continue;
                    }
                    ExecutionRegistration::Pending(mut pending) => {
                        let permit = pending.wait_for_admission().await?.ok_or_else(|| {
                            ToolError::Protocol(
                                "non-cancellable executor operation lost admission".to_owned(),
                            )
                        })?;
                        pending.promote(permit)?
                    }
                };
                let result = start_non_bash_execution(
                    execution,
                    fs.clone(),
                    broker.clone(),
                    blocking_fs.clone(),
                    operation,
                )
                .await
                .unwrap_or_else(|error| {
                    tracing::error!(%error, "executor non-Bash ownership task stopped");
                    Err(bounded_error("rpc_indeterminate"))
                });
                writer
                    .terminal(&identity, request.request_id, result)
                    .await?;
            }
        }
        if close_after_primary {
            return Ok(());
        }
    }
}

fn start_non_bash_execution(
    mut execution: ExecutionLease,
    fs: Arc<WorkspaceFs>,
    broker: Arc<ArtifactBrokerClient>,
    blocking_fs: BlockingFsRegistry,
    operation: ExecutorOperation,
) -> JoinHandle<Result<ExecutorResponse, RpcError>> {
    tokio::spawn(async move {
        let result = execute_non_bash(fs, broker, blocking_fs, operation)
            .await
            .map_err(rpc_error);
        if let Err(error) = execution.complete(result.clone()) {
            tracing::error!(%error, "failed to settle executor non-Bash operation ownership");
            return Err(bounded_error("rpc_indeterminate"));
        }
        result
    })
}

enum PendingBashAdmission {
    Admitted(tokio::sync::OwnedSemaphorePermit),
    Cancelled(Option<String>),
}

async fn admit_pending_bash<R>(
    pending: &mut PendingExecution,
    read: &mut R,
    identity: &RpcIdentity,
    manager: &ExecutorManager,
    execution_id: &str,
) -> Result<PendingBashAdmission, ToolError>
where
    R: AsyncBufRead + Unpin,
{
    tokio::select! {
        biased;
        next = read_frame(read) => {
            match classify_active_control(Some(next), identity, execution_id, manager) {
                ActiveControl::Cancel(request_id) => {
                    Ok(PendingBashAdmission::Cancelled(Some(request_id)))
                }
                ActiveControl::Fatal { error, .. } => Err(error),
            }
        }
        admission = pending.wait_for_admission() => {
            Ok(match admission? {
                Some(permit) => PendingBashAdmission::Admitted(permit),
                None => PendingBashAdmission::Cancelled(None),
            })
        }
    }
}

fn cancelled_before_spawn_response() -> Result<ExecutorResponse, RpcError> {
    Ok(ExecutorResponse::Bash {
        result: BashExecutionResult {
            output: String::new(),
            truncation: truncate_tail("", TruncationOptions::default()),
            artifact_handle: None,
            observed_bytes: 0,
            exit_code: None,
            cancelled: true,
            resource_limit: None,
        },
    })
}

async fn settle_manager_cancel(
    completed: oneshot::Receiver<Result<ExecutorResponse, RpcError>>,
) -> Result<ExecutorResponse, RpcError> {
    match timeout(EXECUTOR_CANCEL_SETTLEMENT_DEADLINE, completed).await {
        Ok(Ok(Ok(ExecutorResponse::Bash { result }))) if result.cancelled => {
            Ok(ExecutorResponse::CancelAccepted {})
        }
        Ok(Ok(Ok(ExecutorResponse::Bash { result }))) if !result.cancelled => {
            Ok(ExecutorResponse::CancelTooLate {})
        }
        Ok(Ok(Ok(_))) => Err(bounded_error("protocol")),
        Ok(Ok(Err(_))) | Ok(Err(_)) | Err(_) => Err(bounded_error("rpc_indeterminate")),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_bash_request<R>(
    read: &mut R,
    writer: &ExecutorWriter,
    identity: &RpcIdentity,
    workspace: &Path,
    broker: &ArtifactBrokerClient,
    manager: &Arc<ExecutorManager>,
    execution_lease: &mut ExecutionLease,
    request_id: String,
    execution_id: String,
    command: String,
    #[cfg(test)] mut test_controls: ExecutorTestControls,
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
{
    let cancel = execution_lease.cancellation_token().ok_or_else(|| {
        ToolError::Protocol("bash execution is missing its manager cancellation token".to_owned())
    })?;
    let (on_update, mut updates_rx) = bounded_bash_updates();
    let bash = LowTrustLocalBash::new(workspace.to_path_buf(), broker);
    #[cfg(test)]
    let bash = bash.with_cancel_stop_delay(test_controls.cancel_stop_delay);
    let execution = bash.execute(&command, &execution_id, cancel.clone(), on_update);
    tokio::pin!(execution);
    // Keep one persistent read future alive across select iterations. Recreating
    // `read_frame` would make a partially consumed Cancel frame disappear when
    // an update wins the select race.
    let (control_tx, mut control_rx) = mpsc::channel(1);
    let control_reader = async {
        loop {
            let next = read_frame(read).await;
            let terminal = !matches!(next, Ok(Some(_)));
            if control_tx.send(next).await.is_err() || terminal {
                break;
            }
        }
    };
    tokio::pin!(control_reader);
    let mut control_reader_done = false;
    let exit = loop {
        tokio::select! {
            biased;
            next = control_rx.recv(), if !control_reader_done || !control_rx.is_empty() => {
                match classify_active_control(next, identity, &execution_id, manager) {
                    ActiveControl::Cancel(cancel_request_id) => {
                        break BashExit::Cancelled {
                            cancel_request_id,
                            completed: None,
                        }
                    },
                    ActiveControl::Fatal { error, response } => break BashExit::Fatal {
                        error,
                        response,
                        completed: None,
                    },
                }
            }
            _ = &mut control_reader, if !control_reader_done => control_reader_done = true,
            result = &mut execution => {
                match take_queued_control_after_completion(
                    &mut control_rx,
                    control_reader.as_mut(),
                    &mut control_reader_done,
                ).await {
                    Ok(next) => match classify_active_control(
                        Some(next),
                        identity,
                        &execution_id,
                        manager,
                    ) {
                        ActiveControl::Cancel(cancel_request_id) => {
                            break BashExit::Cancelled {
                                cancel_request_id,
                                completed: Some(result),
                            }
                        },
                        ActiveControl::Fatal { error, response } => break BashExit::Fatal {
                            error,
                            response,
                            completed: Some(result),
                        },
                    },
                    Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                        break BashExit::Completed(result);
                    }
                }
            }
            update = updates_rx.recv() => {
                if let Some(value) = update
                    && let Err(error) = forward_bash_update(
                        writer,
                        identity,
                        manager,
                        &request_id,
                        value,
                    )
                {
                    tracing::warn!("executor output queue unavailable; closing service epoch");
                    break BashExit::Fatal {
                        error,
                        response: None,
                        completed: None,
                    };
                }
            }
        }
    };

    match exit {
        BashExit::Fatal {
            error,
            response,
            completed,
        } => {
            cancel.cancel();
            let result = match completed {
                Some(result) => result,
                None => match timeout(EXECUTOR_REAP_DEADLINE, &mut execution).await {
                    Ok(result) => result,
                    Err(_) => {
                        tracing::warn!("executor cancellation exceeded its service deadline");
                        let terminal = Err(rpc_error(ToolError::RpcIndeterminate(
                            "executor cancellation exceeded its service deadline".to_owned(),
                        )));
                        execution_lease.complete(terminal.clone())?;
                        writer.terminal(identity, request_id, terminal).await?;
                        if let Some((response_id, response_error)) = response {
                            writer
                                .terminal(identity, response_id, Err(response_error))
                                .await?;
                        }
                        return Err(error.into());
                    }
                },
            };
            drain_completed_bash_updates(writer, identity, manager, &request_id, &mut updates_rx)?;
            let terminal = result
                .map(|result| ExecutorResponse::Bash { result })
                .map_err(rpc_error);
            execution_lease.complete(terminal.clone())?;
            writer.terminal(identity, request_id, terminal).await?;
            if let Some((response_id, response_error)) = response {
                writer
                    .terminal(identity, response_id, Err(response_error))
                    .await?;
            }
            Err(error.into())
        }
        BashExit::Completed(result) => {
            drain_completed_bash_updates(writer, identity, manager, &request_id, &mut updates_rx)?;
            let terminal = result
                .map(|result| ExecutorResponse::Bash { result })
                .map_err(rpc_error);
            execution_lease.complete(terminal.clone())?;
            writer.terminal(identity, request_id, terminal).await?;
            Ok(())
        }
        BashExit::Cancelled {
            cancel_request_id,
            completed,
        } => {
            cancel.cancel();
            #[cfg(test)]
            signal_cancel_ingested(&mut test_controls.cancel_ingested);
            let result = match completed {
                Some(result) => result,
                None => match timeout(EXECUTOR_REAP_DEADLINE, &mut execution).await {
                    Ok(Ok(result)) => Ok(result),
                    Ok(Err(_reap_error)) => {
                        let terminal = Err(bounded_error("rpc_indeterminate"));
                        execution_lease.complete(terminal.clone())?;
                        writer
                            .terminal(
                                identity,
                                cancel_request_id,
                                Err(bounded_error("rpc_indeterminate")),
                            )
                            .await?;
                        writer.terminal(identity, request_id, terminal).await?;
                        return Err(anyhow::anyhow!(
                            "executor cancellation failed before cleanup was proven"
                        ));
                    }
                    Err(_) => {
                        tracing::warn!("executor cancellation exceeded its service deadline");
                        let terminal = Err(rpc_error(ToolError::RpcIndeterminate(
                            "executor cancellation exceeded its service deadline".to_owned(),
                        )));
                        execution_lease.complete(terminal.clone())?;
                        writer
                            .terminal(
                                identity,
                                cancel_request_id,
                                Err(bounded_error("rpc_indeterminate")),
                            )
                            .await?;
                        writer.terminal(identity, request_id, terminal).await?;
                        return Err(anyhow::anyhow!(
                            "executor cancellation exceeded its service deadline"
                        ));
                    }
                },
            };
            drain_completed_bash_updates(writer, identity, manager, &request_id, &mut updates_rx)?;
            let cancel_response = match &result {
                Ok(result) if result.cancelled => Ok(ExecutorResponse::CancelAccepted {}),
                Ok(_) => Ok(ExecutorResponse::CancelTooLate {}),
                Err(_) => Err(bounded_error("rpc_indeterminate")),
            };
            let terminal = result
                .map(|result| ExecutorResponse::Bash { result })
                .map_err(rpc_error);
            execution_lease.complete(terminal.clone())?;
            writer
                .terminal(identity, cancel_request_id, cancel_response)
                .await?;
            writer.terminal(identity, request_id, terminal).await?;
            Ok(())
        }
    }
}

fn bounded_bash_updates() -> (BashUpdateCallback, mpsc::Receiver<Value>) {
    let (updates_tx, updates_rx) = mpsc::channel::<Value>(UPDATE_CHANNEL_CAPACITY);
    let on_update = Arc::new(move |value| match updates_tx.try_send(value) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            tracing::warn!("dropping volatile executor progress update: callback queue full");
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            tracing::warn!("dropping volatile executor progress update: callback queue closed");
        }
    });
    (on_update, updates_rx)
}

#[cfg(test)]
fn signal_cancel_ingested(cancel_ingested: &mut Option<oneshot::Sender<()>>) {
    if let Some(sender) = cancel_ingested.take() {
        let _ = sender.send(());
    }
}

fn forward_bash_update(
    writer: &ExecutorWriter,
    identity: &RpcIdentity,
    manager: &ExecutorManager,
    request_id: &str,
    value: Value,
) -> Result<(), ToolError> {
    let output = match &value {
        Value::Object(fields) if fields.len() == 1 => fields.get("output").and_then(Value::as_str),
        _ => None,
    };
    let Some(output) = output else {
        return forward_single_bash_update(writer, identity, manager, request_id, value);
    };

    // Bash emits at most one bounded reader chunk per callback, but JSON
    // escaping and frame metadata can still push that update beyond PIPE_BUF.
    // Split only the output value, measuring the actual encoded frame at UTF-8
    // boundaries so every attempted progress write remains atomic.
    let mut pending = vec![output];
    while let Some(chunk) = pending.pop() {
        let value = serde_json::json!({"output": chunk});
        let frame = bash_update_frame(identity, request_id, value.clone());
        if encode_rpc_frame(&frame).is_ok_and(|bytes| bytes.len() <= MAX_ATOMIC_UPDATE_FRAME_BYTES)
        {
            forward_single_bash_update(writer, identity, manager, request_id, value)?;
            continue;
        }

        let midpoint = chunk.len() / 2;
        let split = (1..=midpoint)
            .rev()
            .find(|index| chunk.is_char_boundary(*index));
        let Some(split) = split else {
            // Metadata alone can make even one scalar unwriteable. Preserve
            // volatile-update semantics and let try_update drop it explicitly.
            forward_single_bash_update(writer, identity, manager, request_id, value)?;
            continue;
        };
        let (left, right) = chunk.split_at(split);
        pending.push(right);
        pending.push(left);
    }
    Ok(())
}

fn bash_update_frame(
    identity: &RpcIdentity,
    request_id: &str,
    value: Value,
) -> RpcFrame<ExecutorResponse> {
    RpcFrame::Update {
        personality_agent_id: identity.personality_agent_id().clone(),
        generation: identity.generation().to_wire(),
        nonce: identity.nonce().as_str().to_owned(),
        request_id: request_id.to_owned(),
        value,
    }
}

fn forward_single_bash_update(
    writer: &ExecutorWriter,
    identity: &RpcIdentity,
    manager: &ExecutorManager,
    request_id: &str,
    value: Value,
) -> Result<(), ToolError> {
    manager.accept_update(request_id)?;
    let frame = bash_update_frame(identity, request_id, value);
    writer.try_update(&frame)
}

fn drain_completed_bash_updates(
    writer: &ExecutorWriter,
    identity: &RpcIdentity,
    manager: &ExecutorManager,
    request_id: &str,
    updates_rx: &mut mpsc::Receiver<Value>,
) -> Result<(), ToolError> {
    // LowTrustLocalBash resolves only after its pipe reader and every callback
    // invocation complete. No producer can enqueue another value beyond this
    // point, so a nonblocking drain is a stable synchronization boundary, not
    // a racy snapshot.
    while let Ok(value) = updates_rx.try_recv() {
        forward_bash_update(writer, identity, manager, request_id, value)?;
    }
    Ok(())
}

async fn take_queued_control_after_completion<F, T>(
    control_rx: &mut mpsc::Receiver<T>,
    mut control_reader: Pin<&mut F>,
    control_reader_done: &mut bool,
) -> Result<T, mpsc::error::TryRecvError>
where
    F: Future<Output = ()>,
{
    match control_rx.try_recv() {
        Ok(next) => return Ok(next),
        Err(mpsc::error::TryRecvError::Disconnected) => {
            return Err(mpsc::error::TryRecvError::Disconnected);
        }
        Err(mpsc::error::TryRecvError::Empty) => {}
    }

    if !*control_reader_done {
        // The nonblocking stdin reader observes the pipe directly here. This
        // final priority poll closes the gap where completion became ready
        // after the select had already polled control once.
        let poll = poll_fn(|context| Poll::Ready(control_reader.as_mut().poll(context))).await;
        *control_reader_done = poll.is_ready();
    }
    control_rx.try_recv()
}

fn classify_active_control(
    next: Option<Result<Option<Vec<u8>>, ToolError>>,
    identity: &RpcIdentity,
    execution_id: &str,
    manager: &ExecutorManager,
) -> ActiveControl {
    let Some(next) = next else {
        return ActiveControl::Fatal {
            error: ToolError::Protocol(
                "executor control reader stopped without a terminal input state".to_owned(),
            ),
            response: None,
        };
    };
    let line = match next {
        Ok(Some(line)) => line,
        Ok(None) => {
            return ActiveControl::Fatal {
                error: ToolError::Protocol(
                    "executor input closed during active bash execution".to_owned(),
                ),
                response: None,
            };
        }
        Err(error) => {
            return ActiveControl::Fatal {
                error,
                response: None,
            };
        }
    };
    let incoming = match decode_executor_request(&line, identity) {
        Err(error) => {
            return ActiveControl::Fatal {
                error,
                response: None,
            };
        }
        Ok(Ok(request)) => request,
        Ok(Err((request_id, response_error))) => {
            return match manager.reject_request(&request_id) {
                Ok(()) => ActiveControl::Fatal {
                    error: ToolError::Protocol(
                        "invalid control request during active bash execution".to_owned(),
                    ),
                    response: Some((request_id, response_error)),
                },
                Err(error) => ActiveControl::Fatal {
                    error,
                    response: None,
                },
            };
        }
    };
    if let ExecutorOperation::Cancel {
        execution_id: target,
    } = &incoming.operation
        && target == execution_id
    {
        return match manager.cancel_execution(&incoming.request_id, target) {
            Ok(CancelDecision::Accepted(_completed)) => ActiveControl::Cancel(incoming.request_id),
            Ok(CancelDecision::TooLate | CancelDecision::Unknown) => ActiveControl::Fatal {
                error: ToolError::Protocol(
                    "matching cancel did not target an active cancellable execution".to_owned(),
                ),
                response: None,
            },
            Err(error) => ActiveControl::Fatal {
                error,
                response: None,
            },
        };
    }

    let request_id = incoming.request_id;
    match manager.reject_request(&request_id) {
        Ok(()) => ActiveControl::Fatal {
            error: ToolError::Protocol(
                "only a matching cancel is valid during active bash execution".to_owned(),
            ),
            response: Some((request_id, bounded_error("protocol"))),
        },
        Err(error) => ActiveControl::Fatal {
            error,
            response: None,
        },
    }
}

async fn execute_non_bash(
    fs: Arc<WorkspaceFs>,
    broker: Arc<ArtifactBrokerClient>,
    blocking_fs: BlockingFsRegistry,
    operation: ExecutorOperation,
) -> Result<ExecutorResponse, ToolError> {
    match operation {
        ExecutorOperation::ReadFile {
            path,
            offset,
            limit,
            ..
        } => match resolve_input("read_file", &path)? {
            InputRoute::Workspace => {
                blocking_fs
                    .execute(move || {
                        Ok(ExecutorResponse::ReadFile {
                            result: fs.read_file(Path::new(&path), offset, limit)?,
                        })
                    })
                    .await
            }
            InputRoute::Artifact => Ok(ExecutorResponse::Artifact {
                response: broker.read_artifact(&path, offset, limit).await?,
            }),
        },
        ExecutorOperation::WriteFile { path, content, .. } => {
            resolve_input("write_file", &path)?;
            blocking_fs
                .execute(move || {
                    fs.write_file(Path::new(&path), content.as_bytes())?;
                    Ok(ExecutorResponse::Written {})
                })
                .await
        }
        ExecutorOperation::EditFile {
            path,
            old_string,
            new_string,
            ..
        } => {
            resolve_input("edit_file", &path)?;
            blocking_fs
                .execute(move || {
                    fs.edit_file(Path::new(&path), &old_string, &new_string)?;
                    Ok(ExecutorResponse::Edited {})
                })
                .await
        }
        ExecutorOperation::RemoveFile { path, .. } => {
            resolve_input("remove_file", &path)?;
            blocking_fs
                .execute(move || {
                    fs.remove_file(Path::new(&path))?;
                    Ok(ExecutorResponse::Removed {})
                })
                .await
        }
        ExecutorOperation::ListDir { path, .. } => {
            resolve_input("list_dir", &path)?;
            blocking_fs
                .execute(move || {
                    Ok(ExecutorResponse::Listed {
                        entries: fs.list_dir(Path::new(&path))?,
                    })
                })
                .await
        }
        ExecutorOperation::Glob { pattern, .. } => {
            resolve_input("glob", &pattern)?;
            blocking_fs
                .execute(move || {
                    Ok(ExecutorResponse::Globbed {
                        paths: fs.glob(&pattern)?,
                    })
                })
                .await
        }
        ExecutorOperation::Grep { path, pattern, .. } => {
            if resolve_input("grep", &path)? == InputRoute::Artifact {
                Ok(ExecutorResponse::Artifact {
                    response: broker.grep_artifact(&path, &pattern).await?,
                })
            } else {
                let pattern = Regex::new(&pattern)
                    .map_err(|_| ToolError::Protocol("invalid grep pattern".to_owned()))?;
                blocking_fs
                    .execute(move || {
                        Ok(ExecutorResponse::Grepped {
                            matches: fs.grep(Path::new(&path), &pattern)?,
                        })
                    })
                    .await
            }
        }
        ExecutorOperation::Health {}
        | ExecutorOperation::Bash { .. }
        | ExecutorOperation::Cancel { .. } => Err(ToolError::Protocol(
            "operation belongs to executor control loop".to_owned(),
        )),
    }
}

fn operation_execution_id(operation: &ExecutorOperation) -> &str {
    match operation {
        ExecutorOperation::ReadFile { execution_id, .. }
        | ExecutorOperation::WriteFile { execution_id, .. }
        | ExecutorOperation::EditFile { execution_id, .. }
        | ExecutorOperation::RemoveFile { execution_id, .. }
        | ExecutorOperation::ListDir { execution_id, .. }
        | ExecutorOperation::Glob { execution_id, .. }
        | ExecutorOperation::Grep { execution_id, .. }
        | ExecutorOperation::Bash { execution_id, .. }
        | ExecutorOperation::Cancel { execution_id } => execution_id,
        ExecutorOperation::Health {} => {
            unreachable!("health requests do not carry execution identities")
        }
    }
}

fn decode_executor_request(
    line: &[u8],
    identity: &RpcIdentity,
) -> Result<Result<RpcRequest<ExecutorOperation>, (String, RpcError)>, ToolError> {
    match decode_rpc_line::<ExecutorOperation>(line, identity) {
        Ok(request) => Ok(Ok(request)),
        Err(validation_error) => {
            let request = match serde_json::from_slice::<RpcRequest<ExecutorOperation>>(line) {
                Ok(request) => request,
                Err(_) => return Err(validation_error),
            };
            if identity
                .validate_wire(
                    request.personality_agent_id.as_str(),
                    request.generation,
                    &request.nonce,
                )
                .is_err()
                || request.request_id.is_empty()
                || request.request_id.len() > 128
            {
                return Err(validation_error);
            }
            Ok(Err((request.request_id, rpc_error(validation_error))))
        }
    }
}

fn decode_artifact_request(
    line: &[u8],
    identity: &RpcIdentity,
) -> Result<Result<RpcRequest<ArtifactOperation>, (String, RpcError)>, ToolError> {
    match decode_rpc_line::<ArtifactOperation>(line, identity) {
        Ok(request) => match request
            .operation
            .validate_authenticated_owner(identity.personality_agent_id())
        {
            Ok(()) => Ok(Ok(request)),
            Err(validation_error) => Ok(Err((request.request_id, rpc_error(validation_error)))),
        },
        Err(validation_error) => {
            let request = match serde_json::from_slice::<RpcRequest<ArtifactOperation>>(line) {
                Ok(request) => request,
                Err(_) => return Err(validation_error),
            };
            if identity
                .validate_wire(
                    request.personality_agent_id.as_str(),
                    request.generation,
                    &request.nonce,
                )
                .is_err()
                || request.request_id.is_empty()
                || request.request_id.len() > 128
            {
                return Err(validation_error);
            }
            Ok(Err((request.request_id, rpc_error(validation_error))))
        }
    }
}

async fn read_frame<R: AsyncBufRead + Unpin>(read: &mut R) -> Result<Option<Vec<u8>>, ToolError> {
    let mut line = Vec::with_capacity(4096);
    loop {
        let buffer = read.fill_buf().await?;
        if buffer.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Err(ToolError::Protocol(
                    "RPC input ended before newline".to_owned(),
                ))
            };
        }
        let separator = buffer.iter().position(|byte| matches!(byte, b'\n' | b'\r'));
        let take = separator.unwrap_or(buffer.len());
        if line.len().saturating_add(take) > MAX_RPC_LINE_BYTES - 1 {
            return Err(ToolError::Protocol("RPC frame exceeds 1MiB".to_owned()));
        }
        line.extend_from_slice(&buffer[..take]);
        if let Some(position) = separator {
            let delimiter = buffer[position];
            read.consume(position + 1);
            if delimiter == b'\r' {
                return Err(ToolError::Protocol(
                    "RPC frame contained carriage return".to_owned(),
                ));
            }
            if line.is_empty() {
                return Err(ToolError::Protocol("empty RPC frame".to_owned()));
            }
            return Ok(Some(line));
        } else {
            read.consume(take);
        }
    }
}

async fn write_frame<W, T>(write: &mut W, frame: &RpcFrame<T>) -> Result<(), ToolError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let encoded = encode_rpc_frame(frame)?;
    write.write_all(&encoded).await?;
    write.flush().await?;
    Ok(())
}

fn rpc_error(error: ToolError) -> RpcError {
    match error {
        ToolError::InvalidArguments => bounded_error("invalid_arguments"),
        ToolError::Cancelled => bounded_error("cancelled"),
        ToolError::ResourceLimit(limit) => RpcError {
            code: "resource_limit".to_owned(),
            resource_limit: Some(limit),
        },
        ToolError::InvalidPath(_) => bounded_error("invalid_path"),
        ToolError::Io(_) => bounded_error("io"),
        ToolError::Rpc(_) => bounded_error("rpc"),
        ToolError::RpcIndeterminate(_) => bounded_error("rpc_indeterminate"),
        ToolError::Protocol(message) if message == RPC_BOOT_UNIQUENESS_EXHAUSTED_CODE => {
            bounded_error(RPC_BOOT_UNIQUENESS_EXHAUSTED_CODE)
        }
        ToolError::Protocol(message) if message == RPC_REPLAY_OUTCOME_UNAVAILABLE_CODE => {
            bounded_error(RPC_REPLAY_OUTCOME_UNAVAILABLE_CODE)
        }
        ToolError::Protocol(_) => bounded_error("protocol"),
    }
}

fn is_boot_uniqueness_exhausted(error: &ToolError) -> bool {
    matches!(
        error,
        ToolError::Protocol(message) if message == RPC_BOOT_UNIQUENESS_EXHAUSTED_CODE
    )
}

fn is_typed_admission_error(error: &ToolError) -> bool {
    is_boot_uniqueness_exhausted(error)
        || matches!(
            error,
            ToolError::Protocol(message) if message == RPC_REPLAY_OUTCOME_UNAVAILABLE_CODE
        )
}

fn bounded_error(code: &str) -> RpcError {
    RpcError {
        code: code.to_owned(),
        resource_limit: None,
    }
}

fn io_error(message: &str) -> ToolError {
    ToolError::Io(std::io::Error::other(message.to_owned()))
}

fn nonblocking_stdout() -> std::io::Result<NonblockingStdout> {
    Ok(NonblockingStdout {
        fd: nonblocking_stdio_fd(libc::STDOUT_FILENO)?,
    })
}

fn nonblocking_stdin() -> std::io::Result<NonblockingStdin> {
    Ok(NonblockingStdin {
        fd: nonblocking_stdio_fd(libc::STDIN_FILENO)?,
    })
}

fn nonblocking_stdio_fd(raw_fd: libc::c_int) -> std::io::Result<AsyncFd<OwnedFd>> {
    let duplicated = unsafe { libc::dup(raw_fd) };
    if duplicated == -1 {
        return Err(std::io::Error::last_os_error());
    }
    let fd = unsafe { OwnedFd::from_raw_fd(duplicated) };
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    if flags == -1 {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe { libc::close(raw_fd) } == -1 {
        return Err(std::io::Error::last_os_error());
    }
    AsyncFd::new(fd)
}

fn identity_from_env() -> Result<RpcIdentity> {
    identity_from_values(
        &required_text("SUMI_PERSONALITY_AGENT_ID")?,
        &required_text("SUMI_RPC_GENERATION")?,
        required_text("SUMI_RPC_NONCE")?,
    )
}

fn identity_from_values(
    personality_agent_id: &str,
    generation: &str,
    nonce: String,
) -> Result<RpcIdentity> {
    let generation = generation
        .parse::<u64>()
        .context("SUMI_RPC_GENERATION must be an unsigned integer")?;
    RpcIdentity::from_wire(personality_agent_id, generation, nonce)
        .map_err(anyhow::Error::from)
        .context("invalid executor RPC identity")
}

fn required_text(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required for service mode"))?;
    if value.is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(value)
}

fn required_path(name: &str) -> Result<PathBuf> {
    let path = PathBuf::from(required_text(name)?);
    if !path.is_absolute() {
        bail!("{name} must be an absolute path");
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Condvar;

    const PAID: &str = "0198f0f4-9b72-7000-8000-000000000001";
    use crate::{runtime::contracts::MAX_PROCESS_GENERATION, tools::executor::decode_rpc_frame};

    #[derive(Default)]
    struct BlockingFsGate {
        state: Mutex<BlockingFsGateState>,
        released: Condvar,
    }

    #[derive(Default)]
    struct BlockingFsGateState {
        started: usize,
        released: bool,
    }

    impl BlockingFsGate {
        fn block(&self) {
            let mut state = self.state.lock().expect("blocking fs gate");
            state.started += 1;
            while !state.released {
                state = self.released.wait(state).expect("blocking fs gate wait");
            }
        }

        fn started(&self) -> usize {
            self.state.lock().expect("blocking fs gate").started
        }

        fn release(&self) {
            let mut state = self.state.lock().expect("blocking fs gate");
            state.released = true;
            self.released.notify_all();
        }
    }

    async fn wait_for_blocking_fs_starts(gate: &BlockingFsGate, expected: usize) {
        timeout(Duration::from_secs(2), async {
            while gate.started() < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocking filesystem workers did not start");
    }

    fn test_identity() -> RpcIdentity {
        RpcIdentity::from_wire(PAID, 7, "blocking-fs-test").expect("test RPC identity")
    }

    fn start_prefetched_test_session(
        identity: RpcIdentity,
        workspace: PathBuf,
        fs: Arc<WorkspaceFs>,
        broker: Arc<ArtifactBrokerClient>,
        manager: Arc<ExecutorManager>,
        blocking_fs: BlockingFsRegistry,
        request_id: &str,
        operation: ExecutorOperation,
    ) -> (tokio::io::DuplexStream, JoinHandle<Result<()>>) {
        let request = RpcRequest {
            personality_agent_id: identity.personality_agent_id().clone(),
            generation: identity.generation().to_wire(),
            nonce: identity.nonce().as_str().to_owned(),
            request_id: request_id.to_owned(),
            operation,
        };
        let line = serde_json::to_vec(&request).expect("encode test request");
        let (client, server) = tokio::io::duplex(MAX_RPC_LINE_BYTES);
        let (read, write) = tokio::io::split(server);
        let task = tokio::spawn(run_executor_service_with_writer(
            ExecutorInput::prefetched(read, line),
            ExecutorWriter::start(write),
            identity,
            workspace,
            fs,
            broker,
            manager,
            blocking_fs,
        ));
        (client, task)
    }

    async fn read_test_terminal(stream: &mut tokio::io::DuplexStream) -> Value {
        let mut line = String::new();
        timeout(
            Duration::from_secs(2),
            BufReader::new(stream).read_line(&mut line),
        )
        .await
        .expect("test terminal deadline")
        .expect("read test terminal");
        assert!(!line.is_empty(), "service closed before terminal");
        serde_json::from_str(&line).expect("decode test terminal")
    }

    #[tokio::test]
    async fn socket_ownership_lock_rejects_symlink_and_unsafe_permissions_before_cleanup() {
        use std::os::unix::fs::{FileTypeExt, symlink};

        let root = std::env::temp_dir().join(format!(
            "sumi-executor-socket-lock-{}",
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir(&root).expect("create socket lock test directory");
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .expect("restrict socket lock test directory");

        let symlink_socket = root.join("symlink.sock");
        drop(
            std::os::unix::net::UnixListener::bind(&symlink_socket)
                .expect("bind stale symlink-case socket"),
        );
        let symlink_target = root.join("lock-target");
        std::fs::write(&symlink_target, b"do-not-follow").expect("write symlink target");
        std::fs::set_permissions(&symlink_target, std::fs::Permissions::from_mode(0o600))
            .expect("restrict symlink target");
        symlink(&symlink_target, root.join("symlink.sock.lock")).expect("install lock symlink");
        assert!(
            bind_unix_listener(&symlink_socket, "test").await.is_err(),
            "a symlink ownership lock must fail closed"
        );
        assert!(
            std::fs::symlink_metadata(&symlink_socket)
                .expect("stale socket remains")
                .file_type()
                .is_socket()
        );
        assert_eq!(
            std::fs::read(&symlink_target).expect("read untouched symlink target"),
            b"do-not-follow"
        );

        let permissive_socket = root.join("permissive.sock");
        drop(
            std::os::unix::net::UnixListener::bind(&permissive_socket)
                .expect("bind stale permission-case socket"),
        );
        let permissive_lock = root.join("permissive.sock.lock");
        std::fs::write(&permissive_lock, b"").expect("create permissive lock");
        std::fs::set_permissions(&permissive_lock, std::fs::Permissions::from_mode(0o666))
            .expect("make lock permissive");
        assert!(
            bind_unix_listener(&permissive_socket, "test")
                .await
                .is_err(),
            "an overly permissive ownership lock must fail closed"
        );
        assert!(
            std::fs::symlink_metadata(&permissive_socket)
                .expect("second stale socket remains")
                .file_type()
                .is_socket()
        );

        let unsafe_parent = root.join("unsafe-parent");
        std::fs::create_dir(&unsafe_parent).expect("create unsafe parent");
        std::fs::set_permissions(&unsafe_parent, std::fs::Permissions::from_mode(0o777))
            .expect("make parent peer-writable");
        let unsafe_socket = unsafe_parent.join("executor.sock");
        drop(
            std::os::unix::net::UnixListener::bind(&unsafe_socket)
                .expect("bind stale unsafe-parent socket"),
        );
        assert!(
            bind_unix_listener(&unsafe_socket, "test").await.is_err(),
            "a peer-writable socket parent must fail closed"
        );
        assert!(
            std::fs::symlink_metadata(&unsafe_socket)
                .expect("unsafe-parent stale socket remains")
                .file_type()
                .is_socket()
        );

        let unsafe_ancestor = root.join("unsafe-ancestor");
        let safe_descendant = unsafe_ancestor.join("safe-parent");
        std::fs::create_dir_all(&safe_descendant).expect("create unsafe ancestor chain");
        std::fs::set_permissions(&unsafe_ancestor, std::fs::Permissions::from_mode(0o777))
            .expect("make ancestor peer-writable");
        std::fs::set_permissions(&safe_descendant, std::fs::Permissions::from_mode(0o700))
            .expect("restrict final parent");
        assert!(
            bind_unix_listener(&safe_descendant.join("executor.sock"), "test")
                .await
                .is_err(),
            "a peer-writable ancestor must fail closed even when the final parent is safe"
        );

        let trusted_parent = root.join("trusted-parent");
        std::fs::create_dir(&trusted_parent).expect("create trusted parent");
        std::fs::set_permissions(&trusted_parent, std::fs::Permissions::from_mode(0o700))
            .expect("restrict trusted parent");
        let linked_parent = root.join("linked-parent");
        symlink(&trusted_parent, &linked_parent).expect("install parent symlink");
        let linked_socket = linked_parent.join("executor.sock");
        assert!(
            bind_unix_listener(&linked_socket, "test").await.is_err(),
            "a symlinked socket parent must fail closed"
        );

        let nested_parent = trusted_parent.join("nested");
        std::fs::create_dir(&nested_parent).expect("create nested trusted parent");
        let ancestor_link = root.join("ancestor-link");
        symlink(&trusted_parent, &ancestor_link).expect("install parent-component symlink");
        let ancestor_linked_socket = ancestor_link.join("nested/executor.sock");
        assert!(
            bind_unix_listener(&ancestor_linked_socket, "test")
                .await
                .is_err(),
            "a symlinked ancestor of the socket parent must fail closed"
        );

        std::fs::remove_dir_all(root).expect("remove socket lock test directory");
    }

    #[tokio::test]
    async fn bound_listener_detects_post_bind_ancestor_unlink_rename_and_entry_replacement() {
        let root = std::env::temp_dir().join(format!("sxr-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).expect("create socket revalidation root");
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .expect("restrict socket revalidation root");

        let rename_ancestor = root.join("rename/ancestor");
        let rename_parent = rename_ancestor.join("parent");
        std::fs::create_dir_all(&rename_parent).expect("create rename chain");
        let rename_socket = rename_parent.join("executor.sock");
        let renamed_listener = bind_unix_listener(&rename_socket, "test")
            .await
            .expect("bind rename listener");
        let detached_ancestor = root.join("rename/detached");
        std::fs::rename(&rename_ancestor, &detached_ancestor).expect("rename pinned ancestor");
        std::fs::create_dir_all(&rename_parent).expect("replace ancestor chain");
        assert!(
            renamed_listener.verify("test").is_err(),
            "renamed and replaced ancestor chain remained trusted"
        );
        drop(renamed_listener);

        let unlink_ancestor = root.join("unlink/ancestor");
        let unlink_parent = unlink_ancestor.join("parent");
        std::fs::create_dir_all(&unlink_parent).expect("create unlink chain");
        let unlink_socket = unlink_parent.join("executor.sock");
        let unlinked_listener = bind_unix_listener(&unlink_socket, "test")
            .await
            .expect("bind unlink listener");
        std::fs::remove_file(&unlink_socket).expect("unlink bound socket entry");
        std::fs::remove_file(unlink_parent.join("executor.sock.lock"))
            .expect("unlink ownership entry");
        std::fs::remove_dir(&unlink_parent).expect("unlink pinned parent directory");
        std::fs::remove_dir(&unlink_ancestor).expect("unlink pinned ancestor directory");
        assert!(
            unlinked_listener.verify("test").is_err(),
            "unlinked pinned ancestor chain remained trusted"
        );
        drop(unlinked_listener);

        let replacement_parent = root.join("replacement");
        std::fs::create_dir(&replacement_parent).expect("create replacement parent");
        let replacement_socket = replacement_parent.join("executor.sock");
        let original_listener = bind_unix_listener(&replacement_socket, "test")
            .await
            .expect("bind original listener");
        std::fs::remove_file(&replacement_socket).expect("unlink original socket entry");
        let replacement_listener = std::os::unix::net::UnixListener::bind(&replacement_socket)
            .expect("bind replacement socket entry");
        std::fs::set_permissions(&replacement_socket, std::fs::Permissions::from_mode(0o660))
            .expect("match replacement socket mode");
        assert!(
            original_listener.verify("test").is_err(),
            "replacement socket entry matched the pinned listener identity"
        );
        drop((replacement_listener, original_listener));

        std::fs::remove_dir_all(root).expect("remove socket revalidation root");
    }

    #[test]
    fn boot_uniqueness_exhaustion_has_a_rollover_specific_wire_code() {
        let error = rpc_error(ToolError::Protocol(
            RPC_BOOT_UNIQUENESS_EXHAUSTED_CODE.to_owned(),
        ));
        assert_eq!(error.code, RPC_BOOT_UNIQUENESS_EXHAUSTED_CODE);
        assert_eq!(error.resource_limit, None);
    }

    #[test]
    fn bootstrap_generation_accepts_sqlite_domain_and_rejects_the_next_value() {
        for generation in [0, MAX_PROCESS_GENERATION] {
            let identity =
                identity_from_values(PAID, &generation.to_string(), "boot-nonce".to_owned())
                    .expect("bootstrap identity");
            assert_eq!(identity.generation().to_wire(), generation);
        }
        assert!(
            identity_from_values(
                PAID,
                &(MAX_PROCESS_GENERATION + 1).to_string(),
                "boot-nonce".to_owned(),
            )
            .is_err()
        );
    }

    struct PrefixThenStall {
        bytes: Arc<Mutex<Vec<u8>>>,
        prefix: usize,
        wrote_prefix: bool,
    }

    struct AtomicBlockedThenWrite {
        bytes: Arc<Mutex<Vec<u8>>>,
        allow_write: Arc<Mutex<bool>>,
    }

    struct UpdatePendingTerminalWrite {
        bytes: Arc<Mutex<Vec<u8>>>,
        pending_polled: Option<oneshot::Sender<()>>,
    }

    impl AsyncWrite for UpdatePendingTerminalWrite {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _context: &mut TaskContext<'_>,
            buffer: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let frame: Value = serde_json::from_slice(buffer).expect("encoded RPC frame");
            if frame["type"] == "update" {
                if let Some(sender) = self.pending_polled.take() {
                    let _ = sender.send(());
                }
                return Poll::Pending;
            }
            self.bytes.lock().unwrap().extend_from_slice(buffer);
            Poll::Ready(Ok(buffer.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut TaskContext<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _context: &mut TaskContext<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for AtomicBlockedThenWrite {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut TaskContext<'_>,
            buffer: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if !*self.allow_write.lock().unwrap() {
                return Poll::Pending;
            }
            self.bytes.lock().unwrap().extend_from_slice(buffer);
            Poll::Ready(Ok(buffer.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut TaskContext<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _context: &mut TaskContext<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for PrefixThenStall {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _context: &mut TaskContext<'_>,
            buffer: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if self.wrote_prefix {
                return Poll::Pending;
            }
            let length = self.prefix.min(buffer.len());
            self.bytes
                .lock()
                .unwrap()
                .extend_from_slice(&buffer[..length]);
            self.wrote_prefix = true;
            Poll::Ready(Ok(length))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut TaskContext<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _context: &mut TaskContext<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    struct QueueControlOnSecondPoll {
        polls: usize,
        sender: mpsc::Sender<&'static str>,
    }

    impl Future for QueueControlOnSecondPoll {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, _context: &mut TaskContext<'_>) -> Poll<Self::Output> {
            self.polls += 1;
            if self.polls == 2 {
                self.sender.try_send("cancel").unwrap();
            }
            Poll::Pending
        }
    }

    #[test]
    fn update_callback_burst_drops_overflow_without_reordering_delivery() {
        let (callback, mut receiver) = bounded_bash_updates();
        for index in 0..=UPDATE_CHANNEL_CAPACITY {
            callback(serde_json::json!({"index": index}));
        }

        let received = std::iter::from_fn(|| receiver.try_recv().ok()).collect::<Vec<_>>();
        assert_eq!(received.len(), UPDATE_CHANNEL_CAPACITY);
        assert_eq!(received[0]["index"], 0);
        assert_eq!(received[UPDATE_CHANNEL_CAPACITY - 1]["index"], 31);
    }

    #[test]
    fn service_preserves_post_commit_indeterminate_error_code() {
        assert_eq!(
            rpc_error(ToolError::RpcIndeterminate(
                "mutation committed before directory sync failed".to_owned(),
            ))
            .code,
            "rpc_indeterminate"
        );
    }

    #[test]
    fn artifact_request_owner_comes_only_from_authenticated_rpc_identity() {
        let identity = RpcIdentity::from_wire(PAID, 7, "boot-nonce").unwrap();
        let matching = RpcRequest {
            personality_agent_id: PAID.parse().unwrap(),
            generation: 7,
            nonce: "boot-nonce".to_owned(),
            request_id: "matching".to_owned(),
            operation: ArtifactOperation::ReadArtifact {
                handle: format!("artifact://{PAID}/tool-output/execution-1"),
                offset: 0,
                limit: 1,
            },
        };
        let encoded = serde_json::to_vec(&matching).unwrap();
        assert!(matches!(
            decode_artifact_request(&encoded, &identity),
            Ok(Ok(request)) if request.request_id == "matching"
        ));

        let cross_owner = RpcRequest {
            personality_agent_id: PAID.parse().unwrap(),
            generation: 7,
            nonce: "boot-nonce".to_owned(),
            request_id: "cross-owner".to_owned(),
            operation: ArtifactOperation::ReadArtifact {
                handle: "artifact://0198f0f4-9b72-7000-8000-000000000002/tool-output/execution-1"
                    .to_owned(),
                offset: 0,
                limit: 1,
            },
        };
        let encoded = serde_json::to_vec(&cross_owner).unwrap();
        assert!(matches!(
            decode_artifact_request(&encoded, &identity),
            Ok(Err((request_id, RpcError { code, .. })))
                if request_id == "cross-owner" && code == "invalid_path"
        ));

        let nested_override = serde_json::json!({
            "personality_agent_id": PAID,
            "generation": 7,
            "nonce": "boot-nonce",
            "request_id": "nested-owner",
            "operation": {
                "type": "read_artifact",
                "personality_agent_id": "0198f0f4-9b72-7000-8000-000000000002",
                "handle": format!("artifact://{PAID}/tool-output/execution-1"),
                "offset": 0,
                "limit": 1,
            },
        });
        let encoded = serde_json::to_vec(&nested_override).unwrap();
        assert!(matches!(
            decode_artifact_request(&encoded, &identity),
            Err(ToolError::Protocol(message)) if message.contains("unknown field")
        ));
    }

    #[tokio::test]
    async fn completion_repolls_control_reader_before_settling() {
        let (sender, mut receiver) = mpsc::channel(1);
        let reader = QueueControlOnSecondPoll { polls: 0, sender };
        tokio::pin!(reader);
        let first_poll = poll_fn(|context| Poll::Ready(reader.as_mut().poll(context))).await;
        assert!(first_poll.is_pending());

        let mut reader_done = false;
        let control =
            take_queued_control_after_completion(&mut receiver, reader.as_mut(), &mut reader_done)
                .await;

        assert_eq!(control.unwrap(), "cancel");
        assert!(!reader_done);
    }

    #[tokio::test]
    async fn completed_bash_updates_are_emitted_before_terminal() {
        let identity = RpcIdentity::from_wire(PAID, 7, "boot-nonce").unwrap();
        let (read_side, write_side) = tokio::io::duplex(4096);
        let (writer, writer_task) = ExecutorWriter::start(write_side);
        let manager = ExecutorManager::new(1);
        let mut execution = manager
            .begin_execution("request-1".to_owned(), "execution-1".to_owned(), None)
            .await
            .expect("begin execution");
        let (updates_tx, mut updates_rx) = mpsc::channel(UPDATE_CHANNEL_CAPACITY);
        updates_tx
            .send(serde_json::json!({"output": "first"}))
            .await
            .expect("queue first update");
        updates_tx
            .send(serde_json::json!({"output": "second"}))
            .await
            .expect("queue second update");
        drop(updates_tx);

        drain_completed_bash_updates(&writer, &identity, &manager, "request-1", &mut updates_rx)
            .expect("drain completed updates");
        execution
            .complete(Ok(ExecutorResponse::Written {}))
            .expect("accept terminal");
        writer
            .terminal(
                &identity,
                "request-1".to_owned(),
                Ok(ExecutorResponse::Written {}),
            )
            .await
            .expect("write terminal");

        let mut read = BufReader::new(read_side);
        let mut delivered = Vec::new();
        loop {
            let frame = decode_rpc_frame::<ExecutorResponse>(
                &read_frame(&mut read)
                    .await
                    .expect("read frame")
                    .expect("frame"),
                &identity,
            )
            .expect("decode frame");
            match frame {
                RpcFrame::Update { value, .. } => delivered.push(value["output"].clone()),
                RpcFrame::Terminal { .. } => break,
            }
        }
        assert!(
            delivered == Vec::<Value>::new()
                || delivered == vec![serde_json::json!("first")]
                || delivered == vec![serde_json::json!("first"), serde_json::json!("second")]
        );
        writer_task.abort();
    }

    #[tokio::test]
    async fn terminal_fence_drops_old_updates_and_reenables_next_exchange() {
        let identity = RpcIdentity::from_wire(PAID, 7, "boot-nonce").unwrap();
        let (read_side, write_side) = tokio::io::duplex(4096);
        let (writer, writer_task) = ExecutorWriter::start(write_side);

        writer
            .try_update(&RpcFrame::<ExecutorResponse>::Update {
                personality_agent_id: PAID.parse().unwrap(),
                generation: identity.generation().to_wire(),
                nonce: identity.nonce().as_str().to_owned(),
                request_id: "request-1".to_owned(),
                value: serde_json::json!({"output": "stale"}),
            })
            .expect("queue stale progress");
        timeout(
            Duration::from_secs(1),
            writer.terminal(
                &identity,
                "request-1".to_owned(),
                Ok(ExecutorResponse::Written {}),
            ),
        )
        .await
        .expect("first terminal timeout")
        .expect("write first terminal");

        let mut read = BufReader::new(read_side);
        let first = decode_rpc_frame::<ExecutorResponse>(
            &read_frame(&mut read)
                .await
                .expect("read first frame")
                .expect("first frame"),
            &identity,
        )
        .expect("decode first frame");
        assert!(matches!(
            first,
            RpcFrame::Terminal { request_id, .. } if request_id == "request-1"
        ));

        writer
            .try_update(&RpcFrame::<ExecutorResponse>::Update {
                personality_agent_id: PAID.parse().unwrap(),
                generation: identity.generation().to_wire(),
                nonce: identity.nonce().as_str().to_owned(),
                request_id: "request-2".to_owned(),
                value: serde_json::json!({"output": "fresh"}),
            })
            .expect("queue next-request progress");
        let second = decode_rpc_frame::<ExecutorResponse>(
            &read_frame(&mut read)
                .await
                .expect("read second frame")
                .expect("second frame"),
            &identity,
        )
        .expect("decode second frame");
        assert!(matches!(
            second,
            RpcFrame::Update {
                request_id,
                value,
                ..
            } if request_id == "request-2" && value["output"] == "fresh"
        ));

        timeout(
            Duration::from_secs(1),
            writer.terminal(
                &identity,
                "request-2".to_owned(),
                Ok(ExecutorResponse::Written {}),
            ),
        )
        .await
        .expect("second terminal timeout")
        .expect("write second terminal");
        let third = decode_rpc_frame::<ExecutorResponse>(
            &read_frame(&mut read)
                .await
                .expect("read third frame")
                .expect("third frame"),
            &identity,
        )
        .expect("decode third frame");
        assert!(matches!(
            third,
            RpcFrame::Terminal { request_id, .. } if request_id == "request-2"
        ));
        writer_task.abort();
    }

    #[tokio::test]
    async fn oversized_bash_progress_is_split_into_atomic_utf8_frames() {
        let identity = RpcIdentity::from_wire(PAID, 7, "boot-nonce").unwrap();
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let transport = AtomicBlockedThenWrite {
            bytes: bytes.clone(),
            allow_write: Arc::new(Mutex::new(true)),
        };
        let (writer, writer_task) = ExecutorWriter::start_atomic_progress(transport);
        let manager = ExecutorManager::new(1);
        let _execution = manager
            .begin_execution("request-1".to_owned(), "execution-1".to_owned(), None)
            .await
            .expect("begin execution");
        let output = "界\"\\\n".repeat(2_000);

        forward_bash_update(
            &writer,
            &identity,
            &manager,
            "request-1",
            serde_json::json!({"output": output}),
        )
        .expect("forward oversized progress");

        let delivered = timeout(Duration::from_secs(1), async {
            loop {
                let delivered = bytes.lock().unwrap().clone();
                let reconstructed = delivered
                    .split(|byte| *byte == b'\n')
                    .filter(|line| !line.is_empty())
                    .map(|line| {
                        let RpcFrame::Update { value, .. } =
                            decode_rpc_frame::<ExecutorResponse>(line, &identity)
                                .expect("decode update")
                        else {
                            panic!("unexpected terminal")
                        };
                        value["output"].as_str().expect("output string").to_owned()
                    })
                    .collect::<String>();
                if reconstructed == output {
                    break delivered;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("split progress delivery");

        let frames = delivered
            .split_inclusive(|byte| *byte == b'\n')
            .collect::<Vec<_>>();
        assert!(frames.len() > 1);
        assert!(
            frames
                .iter()
                .all(|frame| frame.len() <= MAX_ATOMIC_UPDATE_FRAME_BYTES)
        );
        writer_task.abort();
    }

    #[tokio::test]
    async fn pending_zero_byte_progress_timeout_preserves_terminal() {
        let identity = RpcIdentity::from_wire(PAID, 7, "boot-nonce").unwrap();
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let (pending_polled, pending_polled_rx) = oneshot::channel();
        let transport = UpdatePendingTerminalWrite {
            bytes: bytes.clone(),
            pending_polled: Some(pending_polled),
        };
        let (writer, writer_task) = ExecutorWriter::start(transport);
        writer
            .try_update(&RpcFrame::<ExecutorResponse>::Update {
                personality_agent_id: PAID.parse().unwrap(),
                generation: identity.generation().to_wire(),
                nonce: identity.nonce().as_str().to_owned(),
                request_id: "request-1".to_owned(),
                value: serde_json::json!({"output": "dropped progress"}),
            })
            .expect("enqueue progress");
        timeout(Duration::from_secs(1), pending_polled_rx)
            .await
            .expect("progress write was not polled")
            .expect("progress writer stopped before polling transport");
        assert!(bytes.lock().unwrap().is_empty());

        // The transport keeps every update write Pending while accepting a
        // terminal immediately. Terminal acknowledgement therefore proves the
        // zero-byte update future timed out and was dropped without poisoning
        // the writer epoch.
        writer
            .terminal(
                &identity,
                "request-1".to_owned(),
                Ok(ExecutorResponse::Written {}),
            )
            .await
            .expect("zero-byte progress timeout must not poison terminal delivery");
        let delivered = bytes.lock().unwrap().clone();
        let frame = decode_rpc_frame::<ExecutorResponse>(
            delivered.strip_suffix(b"\n").expect("terminal newline"),
            &identity,
        )
        .expect("decode terminal");
        assert!(matches!(
            frame,
            RpcFrame::Terminal {
                result: Ok(ExecutorResponse::Written {}),
                ..
            }
        ));
        writer_task.abort();
    }

    #[tokio::test]
    async fn partial_progress_write_permanently_closes_writer_epoch() {
        let identity = RpcIdentity::from_wire(PAID, 7, "boot-nonce").unwrap();
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let prefix = 7;
        let transport = PrefixThenStall {
            bytes: bytes.clone(),
            prefix,
            wrote_prefix: false,
        };
        let (writer, writer_task) = ExecutorWriter::start(transport);
        writer
            .try_update(&RpcFrame::<ExecutorResponse>::Update {
                personality_agent_id: PAID.parse().unwrap(),
                generation: identity.generation().to_wire(),
                nonce: identity.nonce().as_str().to_owned(),
                request_id: "request-1".to_owned(),
                value: serde_json::json!({"output": "progress"}),
            })
            .expect("enqueue progress");

        timeout(Duration::from_millis(100), writer_task)
            .await
            .expect("writer epoch stops within its update deadline")
            .expect("writer task");
        assert!(
            writer
                .try_update(&RpcFrame::<ExecutorResponse>::Update {
                    personality_agent_id: PAID.parse().unwrap(),
                    generation: identity.generation().to_wire(),
                    nonce: identity.nonce().as_str().to_owned(),
                    request_id: "request-1".to_owned(),
                    value: serde_json::json!({"output": "later progress"}),
                })
                .is_ok(),
            "volatile backpressure must not fail the Bash side-effect path"
        );
        assert!(
            writer
                .terminal(
                    &identity,
                    "request-1".to_owned(),
                    Ok(ExecutorResponse::Written {}),
                )
                .await
                .is_err(),
            "terminal must not be appended after a partial progress frame"
        );
        assert_eq!(bytes.lock().unwrap().len(), prefix);
    }

    #[tokio::test]
    async fn atomic_progress_backpressure_drops_update_and_preserves_terminal() {
        let identity = RpcIdentity::from_wire(PAID, 7, "boot-nonce").unwrap();
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let allow_write = Arc::new(Mutex::new(false));
        let transport = AtomicBlockedThenWrite {
            bytes: bytes.clone(),
            allow_write: allow_write.clone(),
        };
        let (writer, writer_task) = ExecutorWriter::start_atomic_progress(transport);
        writer
            .try_update(&RpcFrame::<ExecutorResponse>::Update {
                personality_agent_id: PAID.parse().unwrap(),
                generation: identity.generation().to_wire(),
                nonce: identity.nonce().as_str().to_owned(),
                request_id: "request-1".to_owned(),
                value: serde_json::json!({"output": "dropped progress"}),
            })
            .expect("enqueue progress");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(bytes.lock().unwrap().is_empty());

        *allow_write.lock().unwrap() = true;
        writer
            .terminal(
                &identity,
                "request-1".to_owned(),
                Ok(ExecutorResponse::Written {}),
            )
            .await
            .expect("authoritative terminal survives atomic progress drop");
        let delivered = bytes.lock().unwrap().clone();
        let frame = decode_rpc_frame::<ExecutorResponse>(
            delivered.strip_suffix(b"\n").expect("terminal newline"),
            &identity,
        )
        .expect("decode terminal");
        assert!(matches!(
            frame,
            RpcFrame::Terminal {
                result: Ok(ExecutorResponse::Written {}),
                ..
            }
        ));
        writer_task.abort();
    }

    #[test]
    fn nonblocking_stdin_retries_only_interrupted_reads() {
        assert!(is_interrupted_read_error(
            &std::io::Error::from_raw_os_error(libc::EINTR)
        ));
        assert!(is_interrupted_read_error(&std::io::Error::from(
            std::io::ErrorKind::Interrupted,
        )));
        assert!(!is_interrupted_read_error(&std::io::Error::from(
            std::io::ErrorKind::WouldBlock,
        )));
        assert!(!is_interrupted_read_error(&std::io::Error::from(
            std::io::ErrorKind::BrokenPipe,
        )));
    }

    #[test]
    fn blocking_fs_saturation_does_not_starve_health_or_cancel_on_one_worker() {
        let root = std::env::temp_dir().join(format!("sumi-blocking-fs-{}", uuid::Uuid::now_v7()));
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).expect("create blocking filesystem workspace");
        let fs = Arc::new(WorkspaceFs::open(&workspace).expect("open test workspace"));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(4)
            .enable_all()
            .build()
            .expect("build one-worker runtime");
        let cleanup_root = root.clone();

        runtime.block_on(async move {
            let identity = test_identity();
            let broker = Arc::new(ArtifactBrokerClient::new(
                root.join("unused-broker.sock"),
                identity.clone(),
            ));
            let manager = ExecutorManager::new(EXECUTOR_OPERATION_CAPACITY);
            let gate = Arc::new(BlockingFsGate::default());
            let hook_gate = gate.clone();
            let blocking_fs =
                BlockingFsRegistry::with_test_hook(2, 4, Arc::new(move || hook_gate.block()));

            let mut filesystem_jobs = Vec::new();
            for index in 0..4 {
                let execution = manager
                    .begin_execution(
                        format!("request-blocking-fs-{index}"),
                        format!("execution-blocking-fs-{index}"),
                        None,
                    )
                    .await
                    .expect("admit blocking filesystem execution");
                filesystem_jobs.push(start_non_bash_execution(
                    execution,
                    fs.clone(),
                    broker.clone(),
                    blocking_fs.clone(),
                    ExecutorOperation::WriteFile {
                        path: format!("blocked-{index}.txt"),
                        content: format!("content-{index}"),
                        execution_id: format!("execution-blocking-fs-{index}"),
                    },
                ));
            }
            wait_for_blocking_fs_starts(&gate, 2).await;
            timeout(Duration::from_secs(2), async {
                while blocking_fs.available_total_permits() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("filesystem running and waiting capacity was not saturated");
            assert_eq!(blocking_fs.available_worker_permits(), 0);

            let overflow_execution = manager
                .begin_execution(
                    "request-blocking-fs-overflow".to_owned(),
                    "execution-blocking-fs-overflow".to_owned(),
                    None,
                )
                .await
                .expect("admit bounded overflow execution");
            let overflow = timeout(
                Duration::from_millis(250),
                start_non_bash_execution(
                    overflow_execution,
                    fs.clone(),
                    broker.clone(),
                    blocking_fs.clone(),
                    ExecutorOperation::WriteFile {
                        path: "must-not-write.txt".to_owned(),
                        content: "overflow".to_owned(),
                        execution_id: "execution-blocking-fs-overflow".to_owned(),
                    },
                ),
            )
            .await
            .expect("blocking filesystem overflow did not backpressure promptly")
            .expect("blocking filesystem overflow ownership task");
            assert!(matches!(
                overflow,
                Err(RpcError {
                    code,
                    resource_limit: Some(ResourceLimit::Concurrency),
                }) if code == "resource_limit"
            ));
            assert!(!workspace.join("must-not-write.txt").exists());

            let (mut health_client, health_task) = start_prefetched_test_session(
                identity.clone(),
                workspace.clone(),
                fs.clone(),
                broker.clone(),
                manager.clone(),
                blocking_fs.clone(),
                "request-health-under-fs-load",
                ExecutorOperation::Health {},
            );
            let health = timeout(
                Duration::from_millis(250),
                read_test_terminal(&mut health_client),
            )
            .await
            .expect("Health was starved by saturated filesystem work");
            assert_eq!(health["result"]["Ok"]["type"], "healthy");
            health_task
                .await
                .expect("join Health service")
                .expect("Health service result");

            let pid_path = workspace.join("blocking-fs-bash.pid");
            let (mut bash_client, bash_task) = start_prefetched_test_session(
                identity.clone(),
                workspace.clone(),
                fs.clone(),
                broker.clone(),
                manager.clone(),
                blocking_fs.clone(),
                "request-bash-under-fs-load",
                ExecutorOperation::Bash {
                    command: "printf '%s' \"$$\" > blocking-fs-bash.pid; sleep 60".to_owned(),
                    execution_id: "execution-bash-under-fs-load".to_owned(),
                },
            );
            let process_group = timeout(Duration::from_secs(2), async {
                loop {
                    if let Ok(pid) = tokio::fs::read_to_string(&pid_path).await
                        && let Ok(pid) = pid.trim().parse::<i32>()
                    {
                        break pid;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("Bash process did not start under filesystem load");

            let (mut cancel_client, cancel_task) = start_prefetched_test_session(
                identity.clone(),
                workspace.clone(),
                fs.clone(),
                broker.clone(),
                manager.clone(),
                blocking_fs.clone(),
                "request-cancel-under-fs-load",
                ExecutorOperation::Cancel {
                    execution_id: "execution-bash-under-fs-load".to_owned(),
                },
            );
            let cancel = timeout(
                Duration::from_secs(1),
                read_test_terminal(&mut cancel_client),
            )
            .await
            .expect("Cancel was starved by saturated filesystem work");
            assert_eq!(cancel["result"]["Ok"]["type"], "cancel_accepted");
            cancel_task
                .await
                .expect("join Cancel service")
                .expect("Cancel service result");

            let bash = timeout(Duration::from_secs(1), read_test_terminal(&mut bash_client))
                .await
                .expect("cancelled Bash terminal was starved by filesystem work");
            assert_eq!(bash["result"]["Ok"]["result"]["cancelled"], true);
            bash_task
                .await
                .expect("join Bash service")
                .expect("Bash service result");
            timeout(Duration::from_secs(1), async {
                loop {
                    let result = unsafe { libc::kill(-process_group, 0) };
                    if result != 0
                        && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("cancelled Bash process group remained alive");

            gate.release();
            for job in filesystem_jobs {
                assert_eq!(
                    job.await.expect("join blocking filesystem ownership task"),
                    Ok(ExecutorResponse::Written {})
                );
            }
            assert_eq!(blocking_fs.available_total_permits(), 4);
            assert_eq!(blocking_fs.available_worker_permits(), 2);
        });
        drop(runtime);
        std::fs::remove_dir_all(cleanup_root).expect("remove blocking filesystem fixture");
    }

    #[test]
    fn disconnected_non_bash_request_retains_blocking_operation_and_exact_outcome() {
        let root = std::env::temp_dir().join(format!("sumi-detached-fs-{}", uuid::Uuid::now_v7()));
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).expect("create detached filesystem workspace");
        let fs = Arc::new(WorkspaceFs::open(&workspace).expect("open test workspace"));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(2)
            .enable_all()
            .build()
            .expect("build one-worker runtime");
        let cleanup_root = root.clone();

        runtime.block_on(async move {
            let identity = test_identity();
            let broker = Arc::new(ArtifactBrokerClient::new(
                root.join("unused-broker.sock"),
                identity.clone(),
            ));
            let manager = ExecutorManager::new(1);
            let gate = Arc::new(BlockingFsGate::default());
            let hook_gate = gate.clone();
            let blocking_fs =
                BlockingFsRegistry::with_test_hook(1, 1, Arc::new(move || hook_gate.block()));
            let (client, service) = start_prefetched_test_session(
                identity,
                workspace.clone(),
                fs,
                broker,
                manager.clone(),
                blocking_fs.clone(),
                "request-detached-fs",
                ExecutorOperation::WriteFile {
                    path: "committed-after-disconnect.txt".to_owned(),
                    content: "durable result".to_owned(),
                    execution_id: "execution-detached-fs".to_owned(),
                },
            );

            wait_for_blocking_fs_starts(&gate, 1).await;
            service.abort();
            assert!(
                service
                    .await
                    .expect_err("service cancellation")
                    .is_cancelled()
            );
            drop(client);
            assert_eq!(manager.active_count(), 1);

            gate.release();
            timeout(Duration::from_secs(2), async {
                loop {
                    if manager.retained_outcome("execution-detached-fs")
                        == Some(Ok(ExecutorResponse::Written {}))
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("detached filesystem operation did not retain its exact outcome");
            assert_eq!(
                std::fs::read_to_string(workspace.join("committed-after-disconnect.txt"))
                    .expect("read detached filesystem result"),
                "durable result"
            );
            assert_eq!(manager.active_count(), 0);
            assert_eq!(blocking_fs.available_total_permits(), 1);
            assert_eq!(blocking_fs.available_worker_permits(), 1);
        });
        drop(runtime);
        std::fs::remove_dir_all(cleanup_root).expect("remove detached filesystem fixture");
    }

    #[tokio::test]
    async fn blocking_work_registry_has_no_unbounded_waiting_queue() {
        let registry = BlockingWorkRegistry::new(2);
        let first = registry.reserve().await.unwrap();
        let second = registry.reserve().await.unwrap();
        assert!(
            timeout(Duration::from_millis(10), registry.reserve())
                .await
                .is_err()
        );
        drop(first);
        let replacement = timeout(Duration::from_millis(10), registry.reserve())
            .await
            .expect("released blocking-work permit")
            .unwrap();
        drop((second, replacement));
    }
}
