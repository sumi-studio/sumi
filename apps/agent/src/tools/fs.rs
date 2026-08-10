//! Workspace filesystem operations rooted at a directory file descriptor.
//!
//! This implementation requires Linux `openat2(2)`, which was introduced in
//! Linux 5.6, and a runtime policy that permits the syscall. A seccomp policy
//! that denies `openat2` is unsupported; [`WorkspaceFs::open`] probes the
//! syscall with the same beneath/no-symlink policy used by every operation.

#![cfg(target_os = "linux")]

use std::{
    collections::{HashMap, VecDeque},
    ffi::{CStr, CString, OsStr},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
        unix::{
            ffi::OsStrExt,
            fs::{MetadataExt, OpenOptionsExt},
        },
    },
    path::{Component, Path, PathBuf},
    sync::Mutex,
};

use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    ResourceLimit, ToolError,
    truncate::{
        DEFAULT_MAX_BYTES, GREP_MAX_LINE_LENGTH, TruncationOptions, TruncationResult,
        truncate_head, truncate_line_total,
    },
};

const MAX_SCAN_BYTES: u64 = 10 * 1024 * 1024;
pub(crate) const MAX_SCAN_ENTRIES: usize = 4_096;
const MAX_SCAN_DEPTH: usize = 128;
pub(crate) const MAX_GREP_MATCHES: usize = 4_096;
pub(crate) const MAX_GREP_SERIALIZED_BYTES: usize = 50 * 1024;
const OPENAT2_UNAVAILABLE: &str = "workspace filesystem requires Linux openat2(2) (available since Linux 5.6) with the required beneath/no-symlink resolve policy; the syscall is missing or blocked by seccomp";
/// `edit_file` is a whole-file unique-replacement operation. Keep its snapshot
/// inside the same explicit 10 MiB local-input envelope used by bounded scans.
pub const MAX_EDIT_FILE_BYTES: u64 = MAX_SCAN_BYTES;
/// Keeps raw workspace reads comfortably inside the 1 MiB JSON-line RPC
/// envelope even though `Vec<u8>` is encoded as a JSON array.
pub const MAX_RAW_FILE_CHUNK_BYTES: usize = 64 * 1024;
/// The executor's private raw-read contract is intentionally limited to the
/// same one-file envelope accepted by Messaging attachments.
pub const MAX_WORKSPACE_DOWNLOAD_BYTES: u64 = 20 * 1024 * 1024;
pub const MAX_RAW_FILE_TOTAL_BYTES: u64 = MAX_WORKSPACE_DOWNLOAD_BYTES;
/// Keeps a streamed workspace write comfortably inside the 1 MiB JSON-line
/// executor envelope even when `Vec<u8>` is encoded as a JSON array.
pub const MAX_WORKSPACE_DOWNLOAD_CHUNK_BYTES: usize = 64 * 1024;
const MAX_ACTIVE_WORKSPACE_DOWNLOADS: usize = 16;
const RETAINED_WORKSPACE_DOWNLOAD_TERMINALS: usize = 256;

pub(crate) const WORKSPACE_DOWNLOAD_COLLISION: &str =
    "workspace download destination already exists";
pub(crate) const WORKSPACE_DOWNLOAD_MISMATCH: &str = "workspace download size or digest mismatch";
pub(crate) const WORKSPACE_DOWNLOAD_STATE_MISMATCH: &str = "workspace download state mismatch";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EditTemporaryOwnership {
    Replacement,
    OriginalQuarantine,
    Unknown,
    Removed,
}

#[derive(Default)]
struct WalkBudget {
    entries: usize,
    bytes: u64,
    max_bytes: Option<u64>,
}

pub(super) const RESOLVE_NO_MAGICLINKS: u64 = 0x02;
pub(super) const RESOLVE_NO_SYMLINKS: u64 = 0x04;
pub(super) const RESOLVE_BENEATH: u64 = 0x08;

#[repr(C)]
struct OpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrepMatch {
    pub path: String,
    pub line_number: u64,
    pub line: String,
    pub line_truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceFileChunk {
    pub filename: String,
    pub offset: u64,
    pub total_bytes: u64,
    pub version: String,
    pub content_digest: String,
    pub content: Vec<u8>,
    pub eof: bool,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WorkspaceGrepEnvelope<'a> {
    Grepped { matches: &'a [GrepMatch] },
}

/// Workspace operations rooted at an opened directory file descriptor.
pub struct WorkspaceFs {
    root: File,
    display_root: PathBuf,
    downloads: Mutex<WorkspaceDownloadRegistry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceDownloadReceipt {
    pub path: String,
    pub size: u64,
    pub sha256: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkspaceDownloadAbort {
    Aborted,
    TooLate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkspaceDownloadTerminal {
    Aborted,
    Committed,
}

#[derive(Default)]
struct WorkspaceDownloadRegistry {
    active: HashMap<String, WorkspaceDownload>,
    terminals: HashMap<String, WorkspaceDownloadTerminal>,
    terminal_order: VecDeque<String>,
}

impl WorkspaceDownloadRegistry {
    fn remember_terminal(&mut self, download_id: &str, terminal: WorkspaceDownloadTerminal) {
        if self
            .terminals
            .insert(download_id.to_owned(), terminal)
            .is_some()
        {
            self.terminal_order.retain(|known| known != download_id);
        }
        self.terminal_order.push_back(download_id.to_owned());
        while self.terminal_order.len() > RETAINED_WORKSPACE_DOWNLOAD_TERMINALS {
            if let Some(expired) = self.terminal_order.pop_front() {
                self.terminals.remove(&expired);
            }
        }
    }
}

/// One incomplete application attachment. The destination stays absent until
/// `finish_workspace_download` verifies the exact byte commitment and installs
/// the held anonymous `O_TMPFILE` inode with a no-replace `linkat(2)` publish.
struct WorkspaceDownload {
    parent: File,
    destination_name: CString,
    relative_path: String,
    file: File,
    expected_size: u64,
    expected_sha256: String,
    received: u64,
    digest: Sha256,
    published: bool,
}

struct WorkspaceDownloadFinish {
    result: Result<WorkspaceDownloadReceipt, ToolError>,
    committed: bool,
}

impl WorkspaceFs {
    pub fn open(root: &Path) -> Result<Self, ToolError> {
        let (base_path, relative_root) = if root.is_absolute() {
            (
                Path::new("/"),
                root.strip_prefix(Path::new("/")).map_err(|_| {
                    ToolError::InvalidPath(
                        "workspace root was not a valid absolute path".to_owned(),
                    )
                })?,
            )
        } else {
            (Path::new("."), root)
        };
        let base = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open(base_path)?;
        probe_openat2(&base).map_err(map_openat2_probe_error)?;
        let relative_root = if relative_root.as_os_str().is_empty() {
            Path::new(".")
        } else {
            relative_root
        };
        let file = File::from(openat2(
            base.as_raw_fd(),
            relative_root,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NONBLOCK,
            0,
            RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
        )?);
        let metadata = file.metadata()?;
        if !metadata.is_dir() {
            return Err(ToolError::InvalidPath(
                "workspace root is not a directory".to_owned(),
            ));
        }
        Ok(Self {
            root: file,
            display_root: root.to_owned(),
            downloads: Mutex::new(WorkspaceDownloadRegistry::default()),
        })
    }

    pub fn read_file(
        &self,
        path: &Path,
        offset: u64,
        max_bytes: usize,
    ) -> Result<TruncationResult, ToolError> {
        if max_bytes > DEFAULT_MAX_BYTES {
            return Err(ToolError::Protocol(format!(
                "read_file max_bytes exceeds the model-visible limit of {DEFAULT_MAX_BYTES} bytes"
            )));
        }
        let relative = self.relative(path)?;
        let fd = self.open_beneath(
            &relative,
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            0,
        )?;
        ensure_regular_file(&fd, "read_file source")?;
        let metadata = File::from(fd.try_clone()?).metadata()?;
        let available = metadata.len().saturating_sub(offset);
        if available > MAX_SCAN_BYTES {
            return Err(ToolError::ResourceLimit(ResourceLimit::InputBytes {
                observed: available,
                limit: MAX_SCAN_BYTES,
            }));
        }
        let mut file = File::from(fd);
        file.seek(SeekFrom::Start(offset))?;
        let mut file = file.take(MAX_SCAN_BYTES + 1);
        let retained_limit = max_bytes.saturating_add(1);
        let mut retained = Vec::with_capacity(retained_limit.min(64 * 1024));
        let mut total_bytes = 0usize;
        let mut newline_count = 0usize;
        let mut last_byte = None;
        let mut utf8_tail = Vec::with_capacity(3);
        let mut buffer = [0u8; 16 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            validate_utf8_chunk(&mut utf8_tail, &buffer[..read])?;
            total_bytes = total_bytes.saturating_add(read);
            last_byte = buffer.get(read - 1).copied();
            newline_count = newline_count
                .saturating_add(buffer[..read].iter().filter(|b| **b == b'\n').count());
            if retained.len() < retained_limit {
                let remaining = retained_limit - retained.len();
                retained.extend_from_slice(&buffer[..read.min(remaining)]);
            }
        }
        if !utf8_tail.is_empty() {
            return Err(ToolError::Protocol(
                "read_file accepts UTF-8 text only".to_owned(),
            ));
        }
        if u64::try_from(total_bytes).unwrap_or(u64::MAX) > MAX_SCAN_BYTES {
            return Err(ToolError::ResourceLimit(ResourceLimit::InputBytes {
                observed: u64::try_from(total_bytes).unwrap_or(u64::MAX),
                limit: MAX_SCAN_BYTES,
            }));
        }
        let retained = match std::str::from_utf8(&retained) {
            Ok(value) => value,
            Err(error) if error.error_len().is_none() && total_bytes > retained.len() => {
                std::str::from_utf8(&retained[..error.valid_up_to()]).map_err(|_| {
                    ToolError::Protocol("file prefix was not valid UTF-8".to_owned())
                })?
            }
            Err(_) => {
                return Err(ToolError::Protocol(
                    "read_file accepts UTF-8 text only".to_owned(),
                ));
            }
        };
        let total_lines = if total_bytes == 0 {
            0
        } else {
            newline_count.saturating_add(usize::from(last_byte != Some(b'\n')))
        };
        let mut result = truncate_head(
            retained,
            TruncationOptions {
                max_lines: super::truncate::DEFAULT_MAX_LINES,
                max_bytes,
            },
        );
        if !result.truncated
            && (total_bytes > result.output_bytes || total_lines > result.output_lines)
        {
            result.truncated = true;
            result.truncated_by = Some(if total_bytes > max_bytes {
                super::truncate::TruncatedBy::Bytes
            } else {
                super::truncate::TruncatedBy::Lines
            });
        }
        result.total_bytes = total_bytes;
        result.total_lines = total_lines;
        // `truncate_head` models a terminal newline as an additional empty
        // split segment, while read_file's line contract counts the newline
        // as terminating the preceding line. Keep the visible output metadata
        // consistent with that contract.
        result.output_lines = result.output_lines.min(total_lines);
        Ok(result)
    }

    /// Read one exact binary page from a regular workspace file.
    ///
    /// The version is derived from inode metadata and is checked before and
    /// after the page read. Passing the first page's version to later pages
    /// makes a multi-request read fail closed if the source is replaced or
    /// modified between requests.
    pub fn read_raw_file(
        &self,
        path: &Path,
        offset: u64,
        max_bytes: usize,
        max_total_bytes: u64,
        expected_version: Option<&str>,
        expected_content_digest: Option<&str>,
    ) -> Result<WorkspaceFileChunk, ToolError> {
        if max_bytes == 0 || max_bytes > MAX_RAW_FILE_CHUNK_BYTES {
            return Err(ToolError::Protocol(format!(
                "raw workspace read must request 1..={MAX_RAW_FILE_CHUNK_BYTES} bytes"
            )));
        }
        let relative = self.relative(path)?;
        let fd = self.open_beneath(
            &relative,
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            0,
        )?;
        ensure_regular_file(&fd, "raw workspace read source")?;
        let mut file = File::from(fd);
        let before = file.metadata()?;
        let version = raw_file_version(&before);
        if expected_version.is_some_and(|expected| expected != version) {
            return Err(ToolError::Protocol(
                "raw workspace file changed between pages".to_owned(),
            ));
        }
        let total_bytes = before.len();
        if total_bytes > max_total_bytes {
            return Err(ToolError::ResourceLimit(ResourceLimit::InputBytes {
                observed: total_bytes,
                limit: max_total_bytes,
            }));
        }
        if offset > total_bytes {
            return Err(ToolError::InvalidArguments);
        }

        let content_digest = match expected_content_digest {
            Some(expected) => expected.to_owned(),
            None => digest_raw_file(&mut file, total_bytes)?,
        };

        let remaining = total_bytes - offset;
        let page_bytes = remaining.min(u64::try_from(max_bytes).unwrap_or(u64::MAX));
        let page_bytes = usize::try_from(page_bytes).map_err(|_| {
            ToolError::Protocol("raw workspace page size was not representable".to_owned())
        })?;
        file.seek(SeekFrom::Start(offset))?;
        let mut content = vec![0; page_bytes];
        file.read_exact(&mut content)?;

        let after = file.metadata()?;
        if raw_file_version(&after) != version {
            return Err(ToolError::Protocol(
                "raw workspace file changed during page read".to_owned(),
            ));
        }
        let filename = relative
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(|| {
                ToolError::InvalidPath("workspace file has no UTF-8 basename".to_owned())
            })?
            .to_owned();
        let next_offset = offset
            .checked_add(u64::try_from(content.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| ToolError::Protocol("raw workspace offset overflowed".to_owned()))?;
        Ok(WorkspaceFileChunk {
            filename,
            offset,
            total_bytes,
            version,
            content_digest,
            content,
            eof: next_offset == total_bytes,
        })
    }

    pub fn begin_workspace_download(
        &self,
        download_id: &str,
        path: &Path,
        expected_size: u64,
        expected_sha256: &str,
    ) -> Result<(), ToolError> {
        if expected_size == 0 || expected_size > MAX_WORKSPACE_DOWNLOAD_BYTES {
            return Err(ToolError::Protocol(WORKSPACE_DOWNLOAD_MISMATCH.to_owned()));
        }
        if expected_sha256.len() != 64
            || !expected_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ToolError::Protocol(WORKSPACE_DOWNLOAD_MISMATCH.to_owned()));
        }
        let relative = self.relative(path)?;
        if path.is_absolute()
            || path.as_os_str() != relative.as_os_str()
            || !relative
                .components()
                .all(|component| matches!(component, Component::Normal(_)))
        {
            return Err(ToolError::InvalidPath(
                "workspace download destination must be a normal relative path".to_owned(),
            ));
        }

        let mut downloads = self.downloads.lock().map_err(|_| {
            ToolError::Protocol("workspace download registry lock poisoned".to_owned())
        })?;
        if downloads.active.contains_key(download_id)
            || downloads.terminals.contains_key(download_id)
        {
            return Err(ToolError::Protocol(
                WORKSPACE_DOWNLOAD_STATE_MISMATCH.to_owned(),
            ));
        }
        if downloads.active.len() >= MAX_ACTIVE_WORKSPACE_DOWNLOADS {
            return Err(ToolError::ResourceLimit(ResourceLimit::Concurrency));
        }
        let download =
            self.prepare_workspace_download(&relative, expected_size, expected_sha256.to_owned())?;
        downloads.active.insert(download_id.to_owned(), download);
        Ok(())
    }

    pub fn append_workspace_download(
        &self,
        download_id: &str,
        offset: u64,
        content: &[u8],
    ) -> Result<u64, ToolError> {
        if content.is_empty() || content.len() > MAX_WORKSPACE_DOWNLOAD_CHUNK_BYTES {
            return Err(ToolError::Protocol(WORKSPACE_DOWNLOAD_MISMATCH.to_owned()));
        }
        let mut downloads = self.downloads.lock().map_err(|_| {
            ToolError::Protocol("workspace download registry lock poisoned".to_owned())
        })?;
        let mut download = downloads
            .active
            .remove(download_id)
            .ok_or_else(|| ToolError::Protocol(WORKSPACE_DOWNLOAD_STATE_MISMATCH.to_owned()))?;
        match download.append(offset, content) {
            Ok(next_offset) => {
                downloads.active.insert(download_id.to_owned(), download);
                Ok(next_offset)
            }
            Err(operation_error) => {
                // The incomplete inode has never had a workspace name. Dropping
                // its last fd removes it, so cleanup cannot leave a partial file
                // or fail independently of this process.
                drop(download);
                downloads.remember_terminal(download_id, WorkspaceDownloadTerminal::Aborted);
                Err(operation_error)
            }
        }
    }

    pub fn finish_workspace_download(
        &self,
        download_id: &str,
    ) -> Result<WorkspaceDownloadReceipt, ToolError> {
        self.finish_workspace_download_with_hooks(download_id, || {}, || {}, || Ok(()))
    }

    fn finish_workspace_download_with_hook(
        &self,
        download_id: &str,
        before_finish: impl FnOnce(),
    ) -> Result<WorkspaceDownloadReceipt, ToolError> {
        self.finish_workspace_download_with_hooks(download_id, before_finish, || {}, || Ok(()))
    }

    fn finish_workspace_download_with_hooks(
        &self,
        download_id: &str,
        before_finish: impl FnOnce(),
        before_publish: impl FnOnce(),
        post_publish: impl FnOnce() -> Result<(), ToolError>,
    ) -> Result<WorkspaceDownloadReceipt, ToolError> {
        let mut downloads = self.downloads.lock().map_err(|_| {
            ToolError::Protocol("workspace download registry lock poisoned".to_owned())
        })?;
        let mut download = downloads
            .active
            .remove(download_id)
            .ok_or_else(|| ToolError::Protocol(WORKSPACE_DOWNLOAD_STATE_MISMATCH.to_owned()))?;
        // Keep this registry lock through the final fsync and no-replace publish.
        // An abort that acquired it first dropped the anonymous inode, so
        // this path cannot commit. An abort that arrives now waits and observes
        // the committed tombstone instead of falsely claiming cleanup.
        before_finish();
        let finish = download.finish_with_publish_hooks(before_publish, post_publish);
        if finish.committed {
            downloads.remember_terminal(download_id, WorkspaceDownloadTerminal::Committed);
            return finish.result;
        }
        drop(download);
        downloads.remember_terminal(download_id, WorkspaceDownloadTerminal::Aborted);
        finish.result
    }

    /// Cleanup is deliberately idempotent. A fresh cleanup RPC must be able to
    /// close a begun download after cancellation has settled the operation that
    /// was relaying its current chunk.
    pub fn abort_workspace_download(
        &self,
        download_id: &str,
    ) -> Result<WorkspaceDownloadAbort, ToolError> {
        self.abort_workspace_download_with_hook(download_id, || {})
    }

    fn abort_workspace_download_with_hook(
        &self,
        download_id: &str,
        before_abort: impl FnOnce(),
    ) -> Result<WorkspaceDownloadAbort, ToolError> {
        let mut downloads = self.downloads.lock().map_err(|_| {
            ToolError::Protocol("workspace download registry lock poisoned".to_owned())
        })?;
        if let Some(download) = downloads.active.remove(download_id) {
            before_abort();
            drop(download);
            downloads.remember_terminal(download_id, WorkspaceDownloadTerminal::Aborted);
            return Ok(WorkspaceDownloadAbort::Aborted);
        }
        Ok(match downloads.terminals.get(download_id) {
            Some(WorkspaceDownloadTerminal::Committed) => WorkspaceDownloadAbort::TooLate,
            Some(WorkspaceDownloadTerminal::Aborted) | None => WorkspaceDownloadAbort::Aborted,
        })
    }

    fn prepare_workspace_download(
        &self,
        relative: &Path,
        expected_size: u64,
        expected_sha256: String,
    ) -> Result<WorkspaceDownload, ToolError> {
        let relative_path = text_path(relative)?;
        let (parent, name) = split_parent_name(relative)?;
        let parent = File::from(self.open_beneath(
            &parent,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            0,
        )?);
        match openat2(
            parent.as_raw_fd(),
            Path::new(name),
            libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
            RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
        ) {
            Ok(_) => {
                return Err(ToolError::Protocol(WORKSPACE_DOWNLOAD_COLLISION.to_owned()));
            }
            Err(ToolError::Io(error)) if error.raw_os_error() == Some(libc::ENOENT) => {}
            Err(error) => return Err(error),
        }

        let destination_name = os_str_cstring(name)?;
        let current_directory = CString::new(".")
            .map_err(|_| ToolError::InvalidPath("temporary directory was invalid".to_owned()))?;
        let raw = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                current_directory.as_ptr(),
                libc::O_RDWR | libc::O_TMPFILE | libc::O_CLOEXEC,
                0o600,
            )
        };
        if raw < 0 {
            return Err(ToolError::Io(std::io::Error::last_os_error()));
        }
        let file = File::from(unsafe { OwnedFd::from_raw_fd(raw) });
        if unsafe { libc::fchmod(file.as_raw_fd(), 0o600) } != 0 {
            return Err(ToolError::Io(std::io::Error::last_os_error()));
        }
        Ok(WorkspaceDownload {
            parent,
            destination_name,
            relative_path,
            file,
            expected_size,
            expected_sha256,
            received: 0,
            digest: Sha256::new(),
            published: false,
        })
    }

    pub fn write_file(&self, path: &Path, content: &[u8]) -> Result<(), ToolError> {
        self.write_file_with_post_rename_hook(path, content, || Ok(()))
    }

    fn write_file_with_post_rename_hook(
        &self,
        path: &Path,
        content: &[u8],
        post_rename: impl FnOnce() -> Result<(), ToolError>,
    ) -> Result<(), ToolError> {
        let relative = self.relative(path)?;
        let (parent, name) = split_parent_name(&relative)?;
        let parent_fd = self.open_beneath(
            &parent,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            0,
        )?;
        match openat2(
            parent_fd.as_raw_fd(),
            Path::new(name),
            libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
            RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
        ) {
            Ok(existing) => ensure_regular_file(&existing, "write_file destination")?,
            Err(ToolError::Io(error)) if error.raw_os_error() == Some(libc::ENOENT) => {}
            Err(error) => return Err(error),
        }
        let temporary = CString::new(format!(".sumi-{}.tmp", Uuid::now_v7()))
            .map_err(|_| ToolError::InvalidPath("temporary filename was invalid".to_owned()))?;
        let name = os_str_cstring(name)?;

        let raw = unsafe {
            libc::openat(
                parent_fd.as_raw_fd(),
                temporary.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0o600,
            )
        };
        if raw < 0 {
            return Err(ToolError::Io(std::io::Error::last_os_error()));
        }
        let mut temporary_file = File::from(unsafe { OwnedFd::from_raw_fd(raw) });
        let write_result =
            (|| -> Result<(), ToolError> {
                temporary_file.write_all(content)?;
                let chmod = unsafe { libc::fchmod(temporary_file.as_raw_fd(), 0o600) };
                if chmod != 0 {
                    return Err(ToolError::Io(std::io::Error::last_os_error()));
                }
                temporary_file.sync_all()?;
                let renamed = unsafe {
                    libc::renameat(
                        parent_fd.as_raw_fd(),
                        temporary.as_ptr(),
                        parent_fd.as_raw_fd(),
                        name.as_ptr(),
                    )
                };
                if renamed != 0 {
                    return Err(ToolError::Io(std::io::Error::last_os_error()));
                }
                post_rename().map_err(|error| post_commit_error("write_file rename", error))?;
                File::from(parent_fd.try_clone().map_err(|error| {
                    post_commit_error("write_file rename", ToolError::Io(error))
                })?)
                .sync_all()
                .map_err(|error| post_commit_error("write_file rename", ToolError::Io(error)))?;
                Ok(())
            })();
        if write_result.is_err() {
            unsafe {
                libc::unlinkat(parent_fd.as_raw_fd(), temporary.as_ptr(), 0);
            }
        }
        write_result
    }

    pub fn edit_file(&self, path: &Path, old: &str, new: &str) -> Result<(), ToolError> {
        self.edit_file_with_hooks(path, old, new, || {}, || {})
    }

    fn edit_file_with_hooks(
        &self,
        path: &Path,
        old: &str,
        new: &str,
        before_identity_check: impl FnOnce(),
        before_post_write_check: impl FnOnce(),
    ) -> Result<(), ToolError> {
        if old.is_empty() {
            return Err(ToolError::Protocol(
                "old_string must not be empty".to_owned(),
            ));
        }
        let relative = self.relative(path)?;
        let (parent, name) = split_parent_name(&relative)?;
        let parent_fd = self.open_beneath(
            &parent,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            0,
        )?;
        let fd = openat2(
            parent_fd.as_raw_fd(),
            Path::new(name),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            0,
            RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
        )?;
        ensure_regular_file(&fd, "edit_file source")?;
        let original = File::from(fd);
        let original_metadata = original.metadata()?;
        if original_metadata.len() > MAX_EDIT_FILE_BYTES {
            return Err(ToolError::ResourceLimit(ResourceLimit::InputBytes {
                observed: original_metadata.len(),
                limit: MAX_EDIT_FILE_BYTES,
            }));
        }
        let mut content = String::new();
        original
            .try_clone()?
            .take(MAX_EDIT_FILE_BYTES + 1)
            .read_to_string(&mut content)?;
        if u64::try_from(content.len()).unwrap_or(u64::MAX) > MAX_EDIT_FILE_BYTES {
            return Err(ToolError::ResourceLimit(ResourceLimit::InputBytes {
                observed: u64::try_from(content.len()).unwrap_or(u64::MAX),
                limit: MAX_EDIT_FILE_BYTES,
            }));
        }
        let snapshot_metadata = original.metadata()?;
        if !same_file_version(&original_metadata, &snapshot_metadata) {
            return Err(ToolError::Protocol(
                "edit_file source changed while taking the snapshot".to_owned(),
            ));
        }
        let mut matches = content.match_indices(old);
        let Some((start, _)) = matches.next() else {
            return Err(ToolError::Protocol("old_string was not found".to_owned()));
        };
        if matches.next().is_some() {
            return Err(ToolError::Protocol(
                "old_string must match exactly once".to_owned(),
            ));
        }
        let mut replacement = String::with_capacity(content.len() - old.len() + new.len());
        replacement.push_str(&content[..start]);
        replacement.push_str(new);
        replacement.push_str(&content[start + old.len()..]);
        before_identity_check();
        let current = openat2(
            parent_fd.as_raw_fd(),
            Path::new(name),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            0,
            RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
        )?;
        ensure_regular_file(&current, "edit_file identity check")?;
        let mut current = File::from(current);
        let current_metadata = current.metadata()?;
        if current_metadata.len() > MAX_EDIT_FILE_BYTES {
            return Err(ToolError::ResourceLimit(ResourceLimit::InputBytes {
                observed: current_metadata.len(),
                limit: MAX_EDIT_FILE_BYTES,
            }));
        }
        let mut current_content = Vec::with_capacity(
            usize::try_from(current_metadata.len())
                .unwrap_or(0)
                .min(MAX_EDIT_FILE_BYTES as usize),
        );
        Read::by_ref(&mut current)
            .take(MAX_EDIT_FILE_BYTES + 1)
            .read_to_end(&mut current_content)?;
        if u64::try_from(current_content.len()).unwrap_or(u64::MAX) > MAX_EDIT_FILE_BYTES {
            return Err(ToolError::ResourceLimit(ResourceLimit::InputBytes {
                observed: u64::try_from(current_content.len()).unwrap_or(u64::MAX),
                limit: MAX_EDIT_FILE_BYTES,
            }));
        }
        let current_after_read = current.metadata()?;
        if !same_file_version(&snapshot_metadata, &current_metadata)
            || !same_file_version(&current_metadata, &current_after_read)
            || current_content != content.as_bytes()
        {
            return Err(ToolError::Protocol(
                "edit_file destination content changed while editing".to_owned(),
            ));
        }
        let temporary = CString::new(format!(".sumi-edit-{}.tmp", Uuid::now_v7()))
            .map_err(|_| ToolError::InvalidPath("temporary filename was invalid".to_owned()))?;
        let name = os_str_cstring(name)?;
        let raw = unsafe {
            libc::openat(
                parent_fd.as_raw_fd(),
                temporary.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                original_metadata.mode() & 0o7777,
            )
        };
        if raw < 0 {
            return Err(ToolError::Io(std::io::Error::last_os_error()));
        }
        let mut replacement_file = File::from(unsafe { OwnedFd::from_raw_fd(raw) });
        let mut temporary_ownership = EditTemporaryOwnership::Replacement;
        let replacement_result = (|| -> Result<(), ToolError> {
            replacement_file.write_all(replacement.as_bytes())?;
            if unsafe {
                libc::fchmod(
                    replacement_file.as_raw_fd(),
                    original_metadata.mode() & 0o7777,
                )
            } != 0
            {
                return Err(ToolError::Io(std::io::Error::last_os_error()));
            }
            replacement_file.sync_all()?;
            let replacement_metadata = replacement_file.metadata()?;

            // Exchange is the commit boundary: the validated inode is moved to
            // a private same-parent name and is never truncated or written.
            if unsafe {
                libc::renameat2(
                    parent_fd.as_raw_fd(),
                    temporary.as_ptr(),
                    parent_fd.as_raw_fd(),
                    name.as_ptr(),
                    libc::RENAME_EXCHANGE,
                )
            } != 0
            {
                return Err(ToolError::Io(std::io::Error::last_os_error()));
            }
            temporary_ownership = EditTemporaryOwnership::OriginalQuarantine;

            before_post_write_check();
            let quarantined = openat2(
                parent_fd.as_raw_fd(),
                Path::new(OsStr::from_bytes(temporary.as_bytes())),
                libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0,
                RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
            )
            .and_then(|fd| {
                ensure_regular_file(&fd, "edit_file quarantined source")?;
                Ok(File::from(fd).metadata()?)
            });
            let installed = openat2(
                parent_fd.as_raw_fd(),
                Path::new(OsStr::from_bytes(name.as_bytes())),
                libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0,
                RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
            )
            .and_then(|fd| {
                ensure_regular_file(&fd, "edit_file installed replacement")?;
                Ok(File::from(fd).metadata()?)
            });
            let original_is_quarantined = quarantined.as_ref().is_ok_and(|metadata| {
                metadata.dev() == original_metadata.dev()
                    && metadata.ino() == original_metadata.ino()
            });
            let replacement_is_installed = installed.as_ref().is_ok_and(|metadata| {
                metadata.dev() == replacement_metadata.dev()
                    && metadata.ino() == replacement_metadata.ino()
            });
            let identities_match = original_is_quarantined && replacement_is_installed;
            if !identities_match {
                // Best-effort deterministic rollback for local races. T26 owns
                // hostile same-UID post-quarantine isolation.
                let rollback = unsafe {
                    libc::renameat2(
                        parent_fd.as_raw_fd(),
                        temporary.as_ptr(),
                        parent_fd.as_raw_fd(),
                        name.as_ptr(),
                        libc::RENAME_EXCHANGE,
                    )
                };
                if rollback == 0 {
                    let rolled_back_temporary = openat2(
                        parent_fd.as_raw_fd(),
                        Path::new(OsStr::from_bytes(temporary.as_bytes())),
                        libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                        0,
                        RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
                    )
                    .and_then(|fd| {
                        ensure_regular_file(&fd, "edit_file rolled-back temporary")?;
                        Ok(File::from(fd).metadata()?)
                    });
                    let replacement_returned_to_temporary =
                        rolled_back_temporary.as_ref().is_ok_and(|metadata| {
                            metadata.dev() == replacement_metadata.dev()
                                && metadata.ino() == replacement_metadata.ino()
                        });
                    temporary_ownership = if replacement_returned_to_temporary {
                        EditTemporaryOwnership::Replacement
                    } else {
                        EditTemporaryOwnership::Unknown
                    };
                    if temporary_ownership == EditTemporaryOwnership::Replacement {
                        if unsafe { libc::unlinkat(parent_fd.as_raw_fd(), temporary.as_ptr(), 0) }
                            == 0
                        {
                            temporary_ownership = EditTemporaryOwnership::Removed;
                        } else {
                            temporary_ownership = EditTemporaryOwnership::Unknown;
                            return Err(ToolError::RpcIndeterminate(format!(
                                "edit_file atomic install became indeterminate; rollback completed but the verified replacement could not be removed from {} ({})",
                                temporary.to_string_lossy(),
                                std::io::Error::last_os_error(),
                            )));
                        }
                    } else {
                        return Err(ToolError::RpcIndeterminate(format!(
                            "edit_file atomic install became indeterminate; rollback completed but preserved an unknown temporary/quarantine inode at {}: expected replacement={}:{} actual={:?}",
                            temporary.to_string_lossy(),
                            replacement_metadata.dev(),
                            replacement_metadata.ino(),
                            rolled_back_temporary
                                .as_ref()
                                .map(|metadata| (metadata.dev(), metadata.ino())),
                        )));
                    }
                } else {
                    let preservation = if original_is_quarantined {
                        format!(
                            "the original was preserved in quarantine {}",
                            temporary.to_string_lossy()
                        )
                    } else {
                        "quarantine ownership could not be confirmed and no cleanup was attempted"
                            .to_owned()
                    };
                    return Err(ToolError::RpcIndeterminate(format!(
                        "edit_file atomic install became indeterminate; rollback failed ({}) and {preservation}",
                        std::io::Error::last_os_error(),
                    )));
                }
                return Err(ToolError::Protocol(format!(
                    "edit_file pathname ownership changed during atomic install: original={}:{} replacement={}:{} quarantined={:?} installed={:?}",
                    original_metadata.dev(),
                    original_metadata.ino(),
                    replacement_metadata.dev(),
                    replacement_metadata.ino(),
                    quarantined
                        .as_ref()
                        .map(|metadata| (metadata.dev(), metadata.ino())),
                    installed
                        .as_ref()
                        .map(|metadata| (metadata.dev(), metadata.ino())),
                )));
            }
            if unsafe { libc::unlinkat(parent_fd.as_raw_fd(), temporary.as_ptr(), 0) } != 0 {
                return Err(post_commit_error(
                    "edit_file exchange",
                    ToolError::Io(std::io::Error::last_os_error()),
                ));
            }
            temporary_ownership = EditTemporaryOwnership::Removed;
            Ok(())
        })();
        if replacement_result.is_err() && temporary_ownership == EditTemporaryOwnership::Replacement
        {
            let replacement_metadata = replacement_file.metadata();
            let temporary_metadata = openat2(
                parent_fd.as_raw_fd(),
                Path::new(OsStr::from_bytes(temporary.as_bytes())),
                libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                0,
                RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
            )
            .and_then(|fd| {
                ensure_regular_file(&fd, "edit_file replacement cleanup")?;
                Ok(File::from(fd).metadata()?)
            });
            if matches!(
                (replacement_metadata.as_ref(), temporary_metadata.as_ref()),
                (Ok(replacement), Ok(temporary))
                    if replacement.dev() == temporary.dev()
                        && replacement.ino() == temporary.ino()
            ) {
                unsafe {
                    libc::unlinkat(parent_fd.as_raw_fd(), temporary.as_ptr(), 0);
                }
            }
        }
        replacement_result?;
        File::from(parent_fd)
            .sync_all()
            .map_err(|error| post_commit_error("edit_file exchange", ToolError::Io(error)))?;
        Ok(())
    }

    pub fn remove_file(&self, path: &Path) -> Result<(), ToolError> {
        self.remove_file_with_hooks(path, || {}, || Ok(()))
    }

    fn remove_file_with_hooks(
        &self,
        path: &Path,
        before_quarantine: impl FnOnce(),
        post_unlink: impl FnOnce() -> Result<(), ToolError>,
    ) -> Result<(), ToolError> {
        let relative = self.relative(path)?;
        let (parent, name) = split_parent_name(&relative)?;
        let parent_fd = self.open_beneath(
            &parent,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            0,
        )?;
        let target = openat2(
            parent_fd.as_raw_fd(),
            Path::new(name),
            libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
            RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
        )?;
        ensure_regular_file(&target, "remove_file target")?;
        let target_metadata = File::from(target).metadata()?;
        let name = os_str_cstring(name)?;
        let quarantine = CString::new(format!(".sumi-remove-{}", Uuid::now_v7()))
            .map_err(|_| ToolError::InvalidPath("quarantine filename was invalid".to_owned()))?;

        before_quarantine();
        let renamed = unsafe {
            libc::renameat2(
                parent_fd.as_raw_fd(),
                name.as_ptr(),
                parent_fd.as_raw_fd(),
                quarantine.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if renamed != 0 {
            return Err(ToolError::Io(std::io::Error::last_os_error()));
        }

        let quarantined = openat2(
            parent_fd.as_raw_fd(),
            Path::new(OsStr::from_bytes(quarantine.as_bytes())),
            libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
            RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
        );
        let identity_matches = quarantined
            .and_then(|fd| {
                ensure_regular_file(&fd, "remove_file quarantined target")?;
                Ok(File::from(fd).metadata()?)
            })
            .map(|metadata| {
                metadata.dev() == target_metadata.dev() && metadata.ino() == target_metadata.ino()
            });
        if !matches!(identity_matches, Ok(true)) {
            let restored = unsafe {
                libc::renameat2(
                    parent_fd.as_raw_fd(),
                    quarantine.as_ptr(),
                    parent_fd.as_raw_fd(),
                    name.as_ptr(),
                    libc::RENAME_NOREPLACE,
                )
            };
            if restored != 0 {
                return Err(ToolError::RpcIndeterminate(format!(
                    "remove_file target changed and could not be restored: {}",
                    std::io::Error::last_os_error()
                )));
            }
            File::from(parent_fd).sync_all().map_err(|error| {
                post_commit_error("remove_file quarantine rollback", ToolError::Io(error))
            })?;
            return match identity_matches {
                Ok(false) => Err(ToolError::Protocol(
                    "remove_file target changed before deletion".to_owned(),
                )),
                Err(error) => Err(error),
                Ok(true) => unreachable!("handled by identity match"),
            };
        }

        let removed = unsafe { libc::unlinkat(parent_fd.as_raw_fd(), quarantine.as_ptr(), 0) };
        if removed != 0 {
            return Err(post_commit_error(
                "remove_file quarantine rename",
                ToolError::Io(std::io::Error::last_os_error()),
            ));
        }
        post_unlink().map_err(|error| post_commit_error("remove_file unlink", error))?;
        File::from(parent_fd)
            .sync_all()
            .map_err(|error| post_commit_error("remove_file unlink", ToolError::Io(error)))?;
        Ok(())
    }

    pub fn list_dir(&self, path: &Path) -> Result<Vec<String>, ToolError> {
        let relative = self.relative(path)?;
        let fd = self.open_beneath(
            &relative,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            0,
        )?;
        read_dir_names(fd)
    }

    pub fn glob(&self, pattern: &str) -> Result<Vec<String>, ToolError> {
        let pattern = normalize_glob_pattern(pattern)?;
        let matcher = glob_regex(&pattern)?;
        let mut paths = Vec::new();
        let root = self.open_beneath(
            Path::new("."),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            0,
        )?;
        // Glob is an entry/path query. It must retain its entry/depth limits,
        // but must not consume grep's aggregate file-byte scan budget.
        let mut budget = WalkBudget {
            max_bytes: None,
            ..Default::default()
        };
        walk_files(
            root,
            Path::new(""),
            &mut budget,
            0,
            &mut |path, _fd, _budget| {
                if paths.len() >= MAX_SCAN_ENTRIES {
                    return Err(ToolError::ResourceLimit(ResourceLimit::ScanEntries));
                }
                let rendered = text_path(path)?;
                if matcher.is_match(&rendered) {
                    paths.push(rendered);
                }
                Ok(())
            },
        )?;
        paths.sort();
        Ok(paths)
    }

    pub fn grep(&self, path: &Path, pattern: &Regex) -> Result<Vec<GrepMatch>, ToolError> {
        let relative = self.relative(path)?;
        let metadata_fd = self.open_beneath(
            &relative,
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            0,
        )?;
        let metadata = File::from(metadata_fd.try_clone()?).metadata()?;
        let mut matches = Vec::new();
        let mut serialized_bytes =
            serde_json::to_vec(&WorkspaceGrepEnvelope::Grepped { matches: &[] })
                .map_err(|error| ToolError::Protocol(format!("grep encode failed: {error}")))?
                .len();
        if metadata.is_dir() {
            let mut budget = WalkBudget {
                max_bytes: Some(MAX_SCAN_BYTES),
                ..Default::default()
            };
            let traversal_root = if relative == Path::new(".") {
                Path::new("")
            } else {
                &relative
            };
            walk_files(
                metadata_fd,
                traversal_root,
                &mut budget,
                0,
                &mut |file_path, fd, budget| {
                    grep_file(
                        file_path,
                        fd,
                        pattern,
                        &mut matches,
                        &mut serialized_bytes,
                        budget,
                    )
                },
            )?;
        } else if metadata.is_file() {
            let mut budget = WalkBudget {
                max_bytes: Some(MAX_SCAN_BYTES),
                ..Default::default()
            };
            grep_file(
                &relative,
                metadata_fd,
                pattern,
                &mut matches,
                &mut serialized_bytes,
                &mut budget,
            )?;
        } else {
            return Err(ToolError::InvalidPath(
                "grep source is not a regular file or directory".to_owned(),
            ));
        }
        Ok(matches)
    }

    pub fn display_path(&self, relative: &Path) -> PathBuf {
        self.display_root.join(relative)
    }

    fn relative(&self, path: &Path) -> Result<PathBuf, ToolError> {
        let candidate = if path.is_absolute() {
            path.strip_prefix(&self.display_root).map_err(|_| {
                ToolError::InvalidPath("absolute path is outside the workspace".to_owned())
            })?
        } else {
            path
        };
        let mut relative = PathBuf::new();
        for component in candidate.components() {
            match component {
                Component::Normal(value) => {
                    if value.to_str().is_none() {
                        return Err(ToolError::InvalidPath(
                            "workspace text path is not valid UTF-8".to_owned(),
                        ));
                    }
                    relative.push(value);
                }
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(ToolError::InvalidPath(
                        "workspace path contains a forbidden component".to_owned(),
                    ));
                }
            }
        }
        if relative.as_os_str().is_empty() {
            relative.push(".");
        }
        Ok(relative)
    }

    fn open_beneath(&self, path: &Path, flags: i32, mode: u32) -> Result<OwnedFd, ToolError> {
        openat2(
            self.root.as_raw_fd(),
            path,
            flags,
            mode,
            RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
        )
    }
}

impl WorkspaceDownload {
    fn append(&mut self, offset: u64, content: &[u8]) -> Result<u64, ToolError> {
        if offset != self.received {
            return Err(ToolError::Protocol(
                WORKSPACE_DOWNLOAD_STATE_MISMATCH.to_owned(),
            ));
        }
        let next = self
            .received
            .checked_add(u64::try_from(content.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| ToolError::Protocol(WORKSPACE_DOWNLOAD_MISMATCH.to_owned()))?;
        if next > self.expected_size {
            return Err(ToolError::Protocol(WORKSPACE_DOWNLOAD_MISMATCH.to_owned()));
        }
        self.file.write_all(content)?;
        self.digest.update(content);
        self.received = next;
        Ok(next)
    }

    fn finish_with_publish_hooks(
        &mut self,
        before_publish: impl FnOnce(),
        post_publish: impl FnOnce() -> Result<(), ToolError>,
    ) -> WorkspaceDownloadFinish {
        let result = (|| {
            let actual_sha256 = format!("{:x}", self.digest.clone().finalize());
            if self.received != self.expected_size || actual_sha256 != self.expected_sha256 {
                return Err(ToolError::Protocol(WORKSPACE_DOWNLOAD_MISMATCH.to_owned()));
            }
            self.file.sync_all()?;
            before_publish();
            // `AT_EMPTY_PATH` requires CAP_DAC_READ_SEARCH, while the executor
            // deliberately runs with every capability dropped. The documented
            // unprivileged O_TMPFILE publication form resolves this process's
            // held fd through procfs and follows only that exact fd symlink.
            let held_fd = CString::new(format!("/proc/self/fd/{}", self.file.as_raw_fd()))
                .map_err(|_| {
                    ToolError::InvalidPath("workspace download fd was invalid".to_owned())
                })?;
            let linked = unsafe {
                libc::linkat(
                    libc::AT_FDCWD,
                    held_fd.as_ptr(),
                    self.parent.as_raw_fd(),
                    self.destination_name.as_ptr(),
                    libc::AT_SYMLINK_FOLLOW,
                )
            };
            if linked != 0 {
                let error = std::io::Error::last_os_error();
                return if error.raw_os_error() == Some(libc::EEXIST) {
                    Err(ToolError::Protocol(WORKSPACE_DOWNLOAD_COLLISION.to_owned()))
                } else {
                    Err(ToolError::Io(error))
                };
            }
            self.published = true;
            post_publish()
                .map_err(|error| post_commit_error("workspace download publish", error))?;
            self.parent.sync_all().map_err(|error| {
                post_commit_error("workspace download publish", ToolError::Io(error))
            })?;
            Ok(WorkspaceDownloadReceipt {
                path: self.relative_path.clone(),
                size: self.received,
                sha256: actual_sha256,
            })
        })();
        WorkspaceDownloadFinish {
            committed: self.published,
            result,
        }
    }
}

fn post_commit_error(boundary: &str, error: ToolError) -> ToolError {
    ToolError::RpcIndeterminate(format!("{boundary} committed before failure: {error}"))
}

fn split_parent_name(path: &Path) -> Result<(PathBuf, &OsStr), ToolError> {
    let name = path.file_name().ok_or_else(|| {
        ToolError::InvalidPath("workspace path has no final component".to_owned())
    })?;
    if name == OsStr::new(".") || name == OsStr::new("..") {
        return Err(ToolError::InvalidPath(
            "workspace path has an invalid final component".to_owned(),
        ));
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_owned();
    Ok((parent, name))
}

fn os_str_cstring(value: &OsStr) -> Result<CString, ToolError> {
    CString::new(value.as_bytes())
        .map_err(|_| ToolError::InvalidPath("workspace path contains NUL".to_owned()))
}

fn validate_utf8_chunk(tail: &mut Vec<u8>, chunk: &[u8]) -> Result<(), ToolError> {
    tail.extend_from_slice(chunk);
    match std::str::from_utf8(tail) {
        Ok(_) => tail.clear(),
        Err(error) if error.error_len().is_none() => {
            let valid_up_to = error.valid_up_to();
            tail.drain(..valid_up_to);
        }
        Err(_) => {
            return Err(ToolError::Protocol(
                "read_file accepts UTF-8 text only".to_owned(),
            ));
        }
    }
    Ok(())
}

fn raw_file_version(metadata: &std::fs::Metadata) -> String {
    let mut digest = Sha256::new();
    digest.update(metadata.dev().to_le_bytes());
    digest.update(metadata.ino().to_le_bytes());
    digest.update(metadata.len().to_le_bytes());
    digest.update(metadata.mtime().to_le_bytes());
    digest.update(metadata.mtime_nsec().to_le_bytes());
    digest.update(metadata.ctime().to_le_bytes());
    digest.update(metadata.ctime_nsec().to_le_bytes());
    format!("{:x}", digest.finalize())
}

fn digest_raw_file(file: &mut File, total_bytes: u64) -> Result<String, ToolError> {
    file.seek(SeekFrom::Start(0))?;
    let mut digest = Sha256::new();
    let mut remaining = total_bytes;
    let mut buffer = [0u8; 64 * 1024];
    while remaining > 0 {
        let length = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| ToolError::Protocol("raw workspace digest size overflowed".to_owned()))?;
        file.read_exact(&mut buffer[..length])?;
        digest.update(&buffer[..length]);
        remaining -= length as u64;
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn text_path(path: &Path) -> Result<String, ToolError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| ToolError::InvalidPath("workspace text path is not valid UTF-8".to_owned()))
}

fn same_file_version(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.len() == right.len()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
        && left.ctime() == right.ctime()
        && left.ctime_nsec() == right.ctime_nsec()
}

pub(super) fn openat2(
    dirfd: RawFd,
    path: &Path,
    flags: i32,
    mode: u32,
    resolve: u64,
) -> Result<OwnedFd, ToolError> {
    let path = os_str_cstring(path.as_os_str())?;
    let how = OpenHow {
        flags: u64::try_from(flags)
            .map_err(|_| ToolError::InvalidPath("invalid open flags".to_owned()))?,
        mode: u64::from(mode),
        resolve,
    };
    let raw = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            dirfd,
            path.as_ptr(),
            &how,
            std::mem::size_of::<OpenHow>(),
        )
    };
    if raw < 0 {
        return Err(ToolError::Io(std::io::Error::last_os_error()));
    }
    let raw = i32::try_from(raw)
        .map_err(|_| ToolError::Protocol("openat2 returned an invalid descriptor".to_owned()))?;
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

fn probe_openat2(root: &File) -> Result<(), ToolError> {
    let probe = openat2(
        root.as_raw_fd(),
        Path::new("."),
        libc::O_PATH | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        0,
        RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
    )?;
    drop(probe);
    Ok(())
}

fn map_openat2_probe_error(error: ToolError) -> ToolError {
    match error {
        ToolError::Io(error) if openat2_unavailable_errno(error.raw_os_error()) => {
            ToolError::Protocol(OPENAT2_UNAVAILABLE.to_owned())
        }
        error => error,
    }
}

fn openat2_unavailable_errno(errno: Option<i32>) -> bool {
    matches!(
        errno,
        Some(errno)
            if errno == libc::ENOSYS
                || errno == libc::EOPNOTSUPP
                || errno == libc::EINVAL
                || errno == libc::EPERM
                || errno == libc::EACCES
    )
}

pub(super) fn read_dir_names(fd: OwnedFd) -> Result<Vec<String>, ToolError> {
    let duplicate = duplicate_cloexec(fd.as_raw_fd())?.into_raw_fd();
    let directory = unsafe { libc::fdopendir(duplicate) };
    if directory.is_null() {
        let error = std::io::Error::last_os_error();
        unsafe {
            libc::close(duplicate);
        }
        return Err(ToolError::Io(error));
    }
    let mut names = Vec::new();
    loop {
        unsafe {
            *libc::__errno_location() = 0;
        }
        let entry = unsafe { libc::readdir(directory) };
        if entry.is_null() {
            let errno = unsafe { *libc::__errno_location() };
            unsafe {
                libc::closedir(directory);
            }
            if errno != 0 {
                return Err(ToolError::Io(std::io::Error::from_raw_os_error(errno)));
            }
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if name.to_bytes() == b"." || name.to_bytes() == b".." {
            continue;
        }
        if names.len() >= MAX_SCAN_ENTRIES {
            unsafe {
                libc::closedir(directory);
            }
            return Err(ToolError::ResourceLimit(ResourceLimit::ScanEntries));
        }
        let name = match String::from_utf8(name.to_bytes().to_vec()) {
            Ok(name) => name,
            Err(_) => {
                unsafe {
                    libc::closedir(directory);
                }
                return Err(ToolError::InvalidPath(
                    "directory entry is not valid UTF-8".to_owned(),
                ));
            }
        };
        names.push(name);
    }
    names.sort();
    Ok(names)
}

fn duplicate_cloexec(fd: RawFd) -> Result<OwnedFd, ToolError> {
    let duplicate = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        return Err(ToolError::Io(std::io::Error::last_os_error()));
    }
    Ok(unsafe { OwnedFd::from_raw_fd(duplicate) })
}

fn walk_files(
    directory: OwnedFd,
    prefix: &Path,
    budget: &mut WalkBudget,
    depth: usize,
    visit: &mut dyn FnMut(&Path, OwnedFd, &mut WalkBudget) -> Result<(), ToolError>,
) -> Result<(), ToolError> {
    if depth > MAX_SCAN_DEPTH {
        return Err(ToolError::ResourceLimit(ResourceLimit::ScanEntries));
    }
    for name in read_dir_names(directory.try_clone()?)? {
        walk_entry(&directory, prefix, &name, budget, depth, visit)?;
    }
    Ok(())
}

fn walk_entry(
    directory: &OwnedFd,
    prefix: &Path,
    name: &str,
    budget: &mut WalkBudget,
    depth: usize,
    visit: &mut dyn FnMut(&Path, OwnedFd, &mut WalkBudget) -> Result<(), ToolError>,
) -> Result<(), ToolError> {
    budget.entries = budget.entries.saturating_add(1);
    if budget.entries > MAX_SCAN_ENTRIES {
        return Err(ToolError::ResourceLimit(ResourceLimit::ScanEntries));
    }
    let child = openat2(
        directory.as_raw_fd(),
        Path::new(name),
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        0,
        RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS,
    )?;
    let metadata = File::from(child.try_clone()?).metadata()?;
    let path = prefix.join(name);
    if metadata.is_dir() {
        walk_files(child, &path, budget, depth.saturating_add(1), visit)
    } else if metadata.is_file() {
        visit(&path, child, budget)
    } else {
        Err(ToolError::InvalidPath(format!(
            "directory traversal encountered non-regular entry: {}",
            path.display()
        )))
    }
}

fn grep_file(
    path: &Path,
    fd: OwnedFd,
    pattern: &Regex,
    matches: &mut Vec<GrepMatch>,
    serialized_bytes: &mut usize,
    budget: &mut WalkBudget,
) -> Result<(), ToolError> {
    ensure_regular_file(&fd, "grep source")?;
    let mut reader = BufReader::new(File::from(fd).take(MAX_SCAN_BYTES + 1));
    let mut line = Vec::new();
    let mut line_number = 0u64;
    let initial_budget = budget.bytes;
    loop {
        line.clear();
        let read = reader.read_until(b'\n', &mut line)?;
        let consumed = (MAX_SCAN_BYTES + 1).saturating_sub(reader.get_ref().limit());
        budget.bytes = initial_budget.saturating_add(consumed);
        if budget
            .max_bytes
            .is_some_and(|max_bytes| budget.bytes > max_bytes)
        {
            return Err(ToolError::ResourceLimit(ResourceLimit::ScanBytes));
        }
        if read == 0 {
            break;
        }
        line_number = line_number.saturating_add(1);
        if line.ends_with(b"\n") {
            line.pop();
            if line.ends_with(b"\r") {
                line.pop();
            }
        }
        let text = std::str::from_utf8(&line)
            .map_err(|_| ToolError::Protocol("grep accepts UTF-8 text lines only".to_owned()))?;
        if pattern.is_match(text) {
            let (line, line_truncated) = truncate_line_total(text, GREP_MAX_LINE_LENGTH);
            let candidate = GrepMatch {
                path: text_path(path)?,
                line_number,
                line,
                line_truncated,
            };
            if matches.len() >= MAX_GREP_MATCHES {
                return Err(ToolError::ResourceLimit(ResourceLimit::ScanEntries));
            }
            let encoded = serde_json::to_vec(&candidate)
                .map_err(|error| ToolError::Protocol(format!("grep encode failed: {error}")))?;
            let next_bytes = serialized_bytes
                .saturating_add(usize::from(!matches.is_empty()))
                .saturating_add(encoded.len());
            if next_bytes > MAX_GREP_SERIALIZED_BYTES {
                return Err(ToolError::ResourceLimit(ResourceLimit::ScanBytes));
            }
            *serialized_bytes = next_bytes;
            matches.push(candidate);
        }
    }
    Ok(())
}

fn ensure_regular_file(fd: &OwnedFd, operation: &str) -> Result<(), ToolError> {
    if File::from(fd.try_clone()?).metadata()?.is_file() {
        Ok(())
    } else {
        Err(ToolError::InvalidPath(format!(
            "{operation} is not a regular file"
        )))
    }
}

fn glob_regex(pattern: &str) -> Result<Regex, ToolError> {
    let mut regex = String::from("^");
    let mut chars = pattern.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            '*' if chars.peek() == Some(&'*') => {
                chars.next();
                if chars.peek() == Some(&'/') {
                    chars.next();
                    regex.push_str("(?:.*/)?");
                } else {
                    regex.push_str(".*");
                }
            }
            '*' => regex.push_str("[^/]*"),
            '?' => regex.push_str("[^/]"),
            '/' => regex.push('/'),
            other => regex.push_str(&regex::escape(&other.to_string())),
        }
    }
    regex.push('$');
    Regex::new(&regex).map_err(|error| ToolError::Protocol(error.to_string()))
}

fn normalize_glob_pattern(pattern: &str) -> Result<String, ToolError> {
    if pattern.starts_with('/') {
        return Err(ToolError::InvalidPath(
            "glob pattern must be workspace-relative".to_owned(),
        ));
    }
    let mut normalized = Vec::new();
    for component in pattern.split('/') {
        match component {
            "." => {}
            ".." => {
                return Err(ToolError::InvalidPath(
                    "glob pattern contains parent traversal".to_owned(),
                ));
            }
            other => normalized.push(other),
        }
    }
    Ok(normalized.join("/"))
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        ffi::OsString,
        os::unix::{
            ffi::OsStringExt,
            fs::{PermissionsExt, symlink},
        },
        sync::{Arc, Barrier},
        thread,
    };

    use super::*;

    struct TempWorkspace {
        path: PathBuf,
    }

    impl TempWorkspace {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("sumi-fs-{}", Uuid::now_v7()));
            std::fs::create_dir_all(&path).expect("create temp workspace");
            Self { path }
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn sha256(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn install_workspace_download(
        fs: &WorkspaceFs,
        download_id: &str,
        path: &str,
        bytes: &[u8],
    ) -> WorkspaceDownloadReceipt {
        let digest = sha256(bytes);
        fs.begin_workspace_download(download_id, Path::new(path), bytes.len() as u64, &digest)
            .expect("begin workspace download");
        let mut offset = 0u64;
        for chunk in bytes.chunks(MAX_WORKSPACE_DOWNLOAD_CHUNK_BYTES) {
            offset = fs
                .append_workspace_download(download_id, offset, chunk)
                .expect("append workspace download");
        }
        fs.finish_workspace_download(download_id)
            .expect("finish workspace download")
    }

    fn download_temporary_paths(root: &Path) -> Vec<PathBuf> {
        fn collect(directory: &Path, found: &mut Vec<PathBuf>) {
            for entry in std::fs::read_dir(directory).expect("read workspace directory") {
                let entry = entry.expect("workspace entry");
                let file_type = entry.file_type().expect("workspace entry type");
                if file_type.is_dir() {
                    collect(&entry.path(), found);
                } else if entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".sumi-download-")
                {
                    found.push(entry.path());
                }
            }
        }

        let mut found = Vec::new();
        collect(root, &mut found);
        found
    }

    #[test]
    fn open_probes_openat2_without_mutating_the_workspace() {
        let root = TempWorkspace::new();
        std::fs::write(root.path.join("existing.txt"), b"content").expect("write fixture");
        let before = std::fs::read_dir(&root.path)
            .expect("read workspace")
            .map(|entry| entry.expect("directory entry").file_name())
            .collect::<Vec<_>>();

        WorkspaceFs::open(&root.path).expect("workspace fs");

        let after = std::fs::read_dir(&root.path)
            .expect("read workspace")
            .map(|entry| entry.expect("directory entry").file_name())
            .collect::<Vec<_>>();
        assert_eq!(before, after);
    }

    #[test]
    fn open_accepts_ordinary_absolute_and_relative_workspace_roots() {
        let absolute = TempWorkspace::new();
        WorkspaceFs::open(&absolute.path).expect("absolute workspace root");
        WorkspaceFs::open(Path::new(".")).expect("relative workspace root");
    }

    #[test]
    fn open_rejects_a_symlinked_workspace_root() {
        let container = TempWorkspace::new();
        let real_root = container.path.join("real-root");
        let linked_root = container.path.join("linked-root");
        std::fs::create_dir(&real_root).expect("create real workspace root");
        symlink(&real_root, &linked_root).expect("create workspace-root symlink");

        assert!(matches!(
            WorkspaceFs::open(&linked_root),
            Err(ToolError::Io(error)) if error.raw_os_error() == Some(libc::ELOOP)
        ));
    }

    #[test]
    fn openat2_probe_maps_unavailable_errors_but_preserves_other_io_errors() {
        for errno in [
            libc::ENOSYS,
            libc::EOPNOTSUPP,
            libc::EINVAL,
            libc::EPERM,
            libc::EACCES,
        ] {
            let error =
                map_openat2_probe_error(ToolError::Io(std::io::Error::from_raw_os_error(errno)));
            assert!(
                matches!(error, ToolError::Protocol(message) if message == OPENAT2_UNAVAILABLE)
            );
        }

        let error = map_openat2_probe_error(ToolError::Io(std::io::Error::from_raw_os_error(
            libc::ENOENT,
        )));
        assert!(matches!(
            error,
            ToolError::Io(error) if error.raw_os_error() == Some(libc::ENOENT)
        ));
    }

    #[test]
    fn directory_fd_duplicates_are_close_on_exec() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        let duplicate = duplicate_cloexec(fs.root.as_raw_fd()).expect("duplicate directory fd");

        let flags = unsafe { libc::fcntl(duplicate.as_raw_fd(), libc::F_GETFD) };
        assert!(flags >= 0, "read duplicate descriptor flags");
        assert_ne!(flags & libc::FD_CLOEXEC, 0);
    }

    #[test]
    fn write_read_edit_delete_share_the_same_dirfd_boundary() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        fs.write_file(Path::new("note.txt"), b"alpha")
            .expect("write file");
        let mode = std::fs::metadata(root.path.join("note.txt"))
            .expect("file metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        fs.edit_file(Path::new("note.txt"), "alpha", "beta")
            .expect("edit file");
        assert_eq!(
            fs.read_file(Path::new("note.txt"), 0, 100)
                .expect("read file")
                .content,
            "beta"
        );
        fs.remove_file(Path::new("note.txt")).expect("remove file");
        assert!(!root.path.join("note.txt").exists());
    }

    #[test]
    fn edit_requires_exactly_one_match() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        fs.write_file(Path::new("note.txt"), b"x x")
            .expect("write file");
        assert!(fs.edit_file(Path::new("note.txt"), "x", "y").is_err());
        assert!(fs.edit_file(Path::new("note.txt"), "z", "y").is_err());
    }

    #[test]
    fn edit_fails_closed_if_destination_is_replaced_after_snapshot() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        fs.write_file(Path::new("note.txt"), b"alpha")
            .expect("write original");
        let replacement_path = root.path.join("replacement.txt");
        std::fs::write(&replacement_path, b"external").expect("write replacement");
        let result = fs.edit_file_with_hooks(
            Path::new("note.txt"),
            "alpha",
            "edited",
            || {
                std::fs::rename(&replacement_path, root.path.join("note.txt"))
                    .expect("replace destination");
            },
            || {},
        );
        assert!(result.is_err());
        assert_eq!(
            std::fs::read(root.path.join("note.txt")).expect("read replacement"),
            b"external"
        );
    }

    #[test]
    fn edit_fails_closed_if_the_snapshotted_inode_is_mutated_in_place() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        fs.write_file(Path::new("note.txt"), b"alpha")
            .expect("write original");
        let before = std::fs::metadata(root.path.join("note.txt")).expect("before metadata");

        let result = fs.edit_file_with_hooks(
            Path::new("note.txt"),
            "alpha",
            "edited",
            || std::fs::write(root.path.join("note.txt"), b"bravo").expect("mutate inode"),
            || {},
        );

        assert!(matches!(result, Err(ToolError::Protocol(_))));
        assert_eq!(
            std::fs::read(root.path.join("note.txt")).expect("mutated destination"),
            b"bravo"
        );
        use std::os::unix::fs::MetadataExt;
        let after = std::fs::metadata(root.path.join("note.txt")).expect("after metadata");
        assert_eq!((after.dev(), after.ino()), (before.dev(), before.ino()));
    }

    #[test]
    fn edit_preserves_original_quarantine_when_post_exchange_rollback_fails() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        fs.write_file(Path::new("note.txt"), b"alpha")
            .expect("write original");

        let result = fs.edit_file_with_hooks(
            Path::new("note.txt"),
            "alpha",
            "edited",
            || {},
            || std::fs::remove_file(root.path.join("note.txt")).expect("remove installed edit"),
        );

        assert!(matches!(
            result,
            Err(ToolError::RpcIndeterminate(message))
                if message.contains("original was preserved in quarantine")
        ));
        assert!(!root.path.join("note.txt").exists());
        let quarantines = std::fs::read_dir(&root.path)
            .expect("workspace entries")
            .filter_map(|entry| {
                let entry = entry.expect("workspace entry");
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(".sumi-edit-"))
                    .then_some(entry.path())
            })
            .collect::<Vec<_>>();
        assert_eq!(quarantines.len(), 1);
        assert_eq!(
            std::fs::read(&quarantines[0]).expect("preserved original quarantine"),
            b"alpha"
        );
    }

    #[test]
    fn edit_preserves_unknown_inode_moved_to_quarantine_by_rollback() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        fs.write_file(Path::new("note.txt"), b"alpha")
            .expect("write original");
        let original = std::fs::metadata(root.path.join("note.txt")).expect("original metadata");
        let unrelated_identity = Cell::new(None);

        let result = fs.edit_file_with_hooks(
            Path::new("note.txt"),
            "alpha",
            "edited",
            || {},
            || {
                std::fs::remove_file(root.path.join("note.txt"))
                    .expect("remove installed replacement");
                std::fs::write(root.path.join("note.txt"), b"unrelated")
                    .expect("write unrelated file");
                let metadata =
                    std::fs::metadata(root.path.join("note.txt")).expect("unrelated metadata");
                unrelated_identity.set(Some((metadata.dev(), metadata.ino())));
            },
        );

        assert!(matches!(
            result,
            Err(ToolError::RpcIndeterminate(message)) if message.contains("indeterminate")
        ));
        assert_eq!(
            std::fs::read(root.path.join("note.txt")).expect("restored original"),
            b"alpha"
        );
        let restored =
            std::fs::metadata(root.path.join("note.txt")).expect("restored original metadata");
        assert_eq!(
            (restored.dev(), restored.ino()),
            (original.dev(), original.ino())
        );

        let quarantines = std::fs::read_dir(&root.path)
            .expect("workspace entries")
            .filter_map(|entry| {
                let entry = entry.expect("workspace entry");
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(".sumi-edit-"))
                    .then_some(entry.path())
            })
            .collect::<Vec<_>>();
        assert_eq!(quarantines.len(), 1);
        assert_eq!(
            std::fs::read(&quarantines[0]).expect("preserved unrelated quarantine"),
            b"unrelated"
        );
        let quarantined = std::fs::metadata(&quarantines[0]).expect("preserved unrelated metadata");
        assert_eq!(
            Some((quarantined.dev(), quarantined.ino())),
            unrelated_identity.get()
        );
    }

    #[test]
    fn edit_atomically_replaces_the_path_without_mutating_hardlinked_original_inode() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        fs.write_file(Path::new("note.txt"), b"alpha")
            .expect("write original");
        std::fs::hard_link(root.path.join("note.txt"), root.path.join("off-path.txt"))
            .expect("hardlink original inode");
        let original =
            std::fs::metadata(root.path.join("off-path.txt")).expect("original metadata");

        fs.edit_file(Path::new("note.txt"), "alpha", "edited")
            .expect("atomic edit");

        assert_eq!(
            std::fs::read(root.path.join("note.txt")).expect("edited path"),
            b"edited"
        );
        assert_eq!(
            std::fs::read(root.path.join("off-path.txt")).expect("off-path hardlink"),
            b"alpha"
        );
        use std::os::unix::fs::MetadataExt;
        let after = std::fs::metadata(root.path.join("off-path.txt")).expect("hardlink metadata");
        assert_eq!((after.dev(), after.ino()), (original.dev(), original.ino()));
    }

    #[test]
    fn sparse_read_preflight_is_bounded_and_does_not_delay_the_next_read() {
        let root = TempWorkspace::new();
        let huge = File::create(root.path.join("huge.txt")).expect("huge sparse file");
        huge.set_len(MAX_SCAN_BYTES + 1).expect("size sparse file");
        std::fs::write(root.path.join("healthy.txt"), b"healthy").expect("healthy file");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        let started = std::time::Instant::now();
        assert!(matches!(
            fs.read_file(Path::new("huge.txt"), 0, 100),
            Err(ToolError::ResourceLimit(ResourceLimit::InputBytes {
                observed,
                limit: MAX_SCAN_BYTES,
            })) if observed == MAX_SCAN_BYTES + 1
        ));
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        assert_eq!(
            fs.read_file(Path::new("healthy.txt"), 0, 100)
                .expect("prompt follow-up read")
                .content,
            "healthy"
        );
    }

    #[test]
    fn remove_does_not_delete_a_replacement_between_validation_and_quarantine() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        fs.write_file(Path::new("note.txt"), b"original")
            .expect("write original");
        std::fs::write(root.path.join("replacement.txt"), b"replacement")
            .expect("write replacement");

        let result = fs.remove_file_with_hooks(
            Path::new("note.txt"),
            || {
                std::fs::rename(root.path.join("note.txt"), root.path.join("original.txt"))
                    .expect("move validated inode aside");
                std::fs::rename(
                    root.path.join("replacement.txt"),
                    root.path.join("note.txt"),
                )
                .expect("replace validated path");
            },
            || Ok(()),
        );

        assert!(matches!(result, Err(ToolError::Protocol(_))));
        assert_eq!(
            std::fs::read(root.path.join("note.txt"))
                .expect("replacement remains at requested path"),
            b"replacement"
        );
        assert_eq!(
            std::fs::read(root.path.join("original.txt")).expect("validated inode remains"),
            b"original"
        );
        assert!(
            std::fs::read_dir(&root.path)
                .expect("workspace entries")
                .all(|entry| !entry
                    .expect("workspace entry")
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".sumi-remove-"))
        );

        fs.write_file(Path::new("linked.txt"), b"validated")
            .expect("write second original");
        let outside = root.path.with_extension("outside");
        std::fs::write(&outside, b"outside").expect("write outside target");
        let result = fs.remove_file_with_hooks(
            Path::new("linked.txt"),
            || {
                std::fs::rename(
                    root.path.join("linked.txt"),
                    root.path.join("linked-original.txt"),
                )
                .expect("move second validated inode aside");
                symlink(&outside, root.path.join("linked.txt")).expect("install escape symlink");
            },
            || Ok(()),
        );
        assert!(result.is_err());
        assert!(
            std::fs::symlink_metadata(root.path.join("linked.txt"))
                .expect("replacement symlink remains")
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            std::fs::read(&outside).expect("outside target remains"),
            b"outside"
        );
        std::fs::remove_file(outside).expect("remove outside fixture");
    }

    #[test]
    fn committed_write_remove_and_sync_failpoints_are_indeterminate() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");

        let write = fs.write_file_with_post_rename_hook(Path::new("written.txt"), b"new", || {
            Err(ToolError::Io(std::io::Error::other(
                "injected post-rename failure",
            )))
        });
        assert!(matches!(write, Err(ToolError::RpcIndeterminate(_))));
        assert_eq!(
            std::fs::read(root.path.join("written.txt")).unwrap(),
            b"new"
        );

        fs.write_file(Path::new("removed.txt"), b"old").unwrap();
        let remove = fs.remove_file_with_hooks(
            Path::new("removed.txt"),
            || {},
            || {
                Err(ToolError::Io(std::io::Error::other(
                    "injected post-unlink failure",
                )))
            },
        );
        assert!(matches!(remove, Err(ToolError::RpcIndeterminate(_))));
        assert!(!root.path.join("removed.txt").exists());

        let sync = post_commit_error(
            "injected directory sync",
            ToolError::Io(std::io::Error::other("sync failed")),
        );
        assert!(matches!(sync, ToolError::RpcIndeterminate(_)));
    }

    #[test]
    fn actual_incomplete_utf8_at_eof_is_rejected_but_view_split_is_not() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        std::fs::write(root.path.join("invalid.txt"), [0xf0, 0x9f]).expect("write invalid");
        assert!(fs.read_file(Path::new("invalid.txt"), 0, 100).is_err());
        std::fs::write(root.path.join("valid.txt"), "a界").expect("write valid");
        assert_eq!(
            fs.read_file(Path::new("valid.txt"), 0, 1)
                .expect("bounded valid prefix")
                .content,
            "a"
        );
    }

    #[test]
    fn raw_file_pages_preserve_binary_bytes_and_reject_a_changed_source_version() {
        let root = TempWorkspace::new();
        let path = root.path.join("image.bin");
        std::fs::write(&path, [0, 0xff, 0x80, 1, 2]).expect("write binary fixture");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");

        let first = fs
            .read_raw_file(&path, 0, 3, 5, None, None)
            .expect("read first binary page");
        assert_eq!(first.filename, "image.bin");
        assert_eq!(first.content, [0, 0xff, 0x80]);
        assert_eq!(
            first.content_digest,
            format!("{:x}", Sha256::digest([0, 0xff, 0x80, 1, 2]))
        );
        assert!(!first.eof);

        std::fs::write(&path, [9, 8, 7, 6, 5, 4]).expect("replace binary fixture");
        assert!(matches!(
            fs.read_raw_file(
                Path::new("image.bin"),
                3,
                3,
                6,
                Some(&first.version),
                Some(&first.content_digest),
            ),
            Err(ToolError::Protocol(message))
                if message == "raw workspace file changed between pages"
        ));
    }

    #[test]
    fn raw_file_read_rejects_escape_symlink_and_nonregular_paths() {
        let root = TempWorkspace::new();
        std::fs::create_dir(root.path.join("directory")).expect("create directory fixture");
        symlink("/etc/passwd", root.path.join("linked-file")).expect("create escape symlink");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");

        for path in ["../outside", "linked-file", "directory"] {
            assert!(
                fs.read_raw_file(Path::new(path), 0, 1, 1, None, None)
                    .is_err(),
                "raw read accepted {path}"
            );
        }
    }

    #[test]
    fn invalid_utf8_after_the_retained_prefix_is_rejected() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        let mut content = vec![b'a'; 64 * 1024];
        content.push(0xff);
        std::fs::write(root.path.join("invalid-tail.txt"), content).expect("write invalid tail");

        assert!(matches!(
            fs.read_file(Path::new("invalid-tail.txt"), 0, 16),
            Err(ToolError::Protocol(_))
        ));
    }

    #[test]
    fn empty_read_reports_zero_lines_and_zero_output_lines() {
        let root = TempWorkspace::new();
        std::fs::File::create(root.path.join("empty.txt")).expect("empty file");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        let result = fs
            .read_file(Path::new("empty.txt"), 0, 100)
            .expect("empty read");
        assert_eq!(result.total_lines, 0);
        assert_eq!(result.output_lines, 0);
    }

    #[test]
    fn beneath_resolution_rejects_escape_symlink_and_parent_components() {
        let root = TempWorkspace::new();
        symlink("/etc", root.path.join("escape")).expect("create escape symlink");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        assert!(fs.read_file(Path::new("escape/passwd"), 0, 100).is_err());
        assert!(fs.read_file(Path::new("../outside"), 0, 100).is_err());
    }

    #[test]
    fn every_workspace_operation_rejects_parent_and_final_symlinks() {
        let root = TempWorkspace::new();
        std::fs::create_dir(root.path.join("real")).expect("real directory");
        std::fs::write(root.path.join("real/note.txt"), b"needle").expect("real file");
        symlink("real", root.path.join("linked-dir")).expect("parent symlink");
        symlink("real/note.txt", root.path.join("linked-file")).expect("final symlink");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        let needle = Regex::new("needle").expect("regex");

        assert!(
            fs.read_file(Path::new("linked-dir/note.txt"), 0, 100)
                .is_err()
        );
        assert!(fs.read_file(Path::new("linked-file"), 0, 100).is_err());
        assert!(
            fs.write_file(Path::new("linked-dir/new.txt"), b"new")
                .is_err()
        );
        assert!(fs.write_file(Path::new("linked-file"), b"new").is_err());
        assert!(
            fs.edit_file(Path::new("linked-dir/note.txt"), "needle", "new")
                .is_err()
        );
        assert!(
            fs.edit_file(Path::new("linked-file"), "needle", "new")
                .is_err()
        );
        assert!(fs.list_dir(Path::new("linked-dir")).is_err());
        assert!(fs.grep(Path::new("linked-dir"), &needle).is_err());
        assert!(fs.grep(Path::new("linked-file"), &needle).is_err());
        assert!(fs.glob("**").is_err());
        assert!(fs.remove_file(Path::new("linked-dir/note.txt")).is_err());
        assert!(fs.remove_file(Path::new("linked-file")).is_err());
        assert!(root.path.join("linked-file").symlink_metadata().is_ok());
        assert_eq!(
            std::fs::read(root.path.join("real/note.txt")).expect("target"),
            b"needle"
        );
    }

    #[test]
    fn list_glob_and_grep_are_fd_relative() {
        let root = TempWorkspace::new();
        std::fs::create_dir(root.path.join("nested")).expect("create nested directory");
        std::fs::create_dir(root.path.join("nested/deeper"))
            .expect("create deeper nested directory");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        fs.write_file(Path::new("root.txt"), b"root\n")
            .expect("write root");
        fs.write_file(Path::new("nested/a.txt"), b"needle\n")
            .expect("write a");
        fs.write_file(Path::new("nested/deeper/c.txt"), b"nested\n")
            .expect("write c");
        fs.write_file(
            Path::new("nested/b.log"),
            format!("needle {}\n", "界".repeat(600)).as_bytes(),
        )
        .expect("write b");

        assert_eq!(
            fs.list_dir(Path::new(".")).expect("list"),
            vec!["nested", "root.txt"]
        );
        assert_eq!(
            fs.glob("**/*.txt").expect("glob"),
            vec!["nested/a.txt", "nested/deeper/c.txt", "root.txt"]
        );
        assert_eq!(
            fs.glob("nested/**/c.txt")
                .expect("zero-or-more directories"),
            vec!["nested/deeper/c.txt"]
        );
        assert_eq!(
            fs.glob("**.txt").expect("globstar without slash"),
            vec!["nested/a.txt", "nested/deeper/c.txt", "root.txt"]
        );
        assert_eq!(
            fs.glob("*.txt").expect("single star remains segment-local"),
            vec!["root.txt"]
        );
        let matches = fs
            .grep(Path::new("nested"), &Regex::new("needle").expect("regex"))
            .expect("grep");
        assert_eq!(matches.len(), 2);
        assert!(matches.iter().any(|item| item.line_truncated));
        assert!(
            matches
                .iter()
                .all(|item| item.line.chars().count() <= GREP_MAX_LINE_LENGTH)
        );
    }

    #[test]
    fn text_path_outputs_preserve_backslashes_and_normalize_dot_prefixes() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        fs.write_file(Path::new(r"back\slash.txt"), b"needle\n")
            .expect("write backslash filename");

        assert_eq!(
            fs.list_dir(Path::new(".")).expect("list"),
            vec![r"back\slash.txt"]
        );
        assert_eq!(
            fs.glob(r"back\slash.txt").expect("glob"),
            vec![r"back\slash.txt"]
        );
        let matches = fs
            .grep(Path::new("."), &Regex::new("needle").expect("regex"))
            .expect("grep");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].path, r"back\slash.txt");
    }

    #[test]
    fn glob_normalizes_dot_components_and_rejects_escape_patterns() {
        let root = TempWorkspace::new();
        std::fs::create_dir(root.path.join("nested")).expect("nested directory");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        fs.write_file(Path::new("root.txt"), b"root")
            .expect("root file");
        fs.write_file(Path::new("nested/child.txt"), b"child")
            .expect("nested file");

        assert_eq!(fs.glob("./*.txt").expect("root dot"), vec!["root.txt"]);
        assert_eq!(
            fs.glob("nested/./*.txt").expect("nested dot"),
            vec!["nested/child.txt"]
        );
        for pattern in ["/*.txt", "../*.txt", "nested/../*.txt"] {
            assert!(matches!(fs.glob(pattern), Err(ToolError::InvalidPath(_))));
        }
    }

    #[test]
    fn list_glob_and_grep_reject_non_utf8_directory_entries() {
        let root = TempWorkspace::new();
        let invalid_name = OsString::from_vec(vec![b'i', 0xff]);
        std::fs::write(root.path.join(&invalid_name), b"needle").expect("write non-UTF-8 name");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");

        assert!(matches!(
            fs.list_dir(Path::new(".")),
            Err(ToolError::InvalidPath(_))
        ));
        assert!(matches!(fs.glob("**"), Err(ToolError::InvalidPath(_))));
        assert!(matches!(
            fs.grep(Path::new("."), &Regex::new("needle").expect("regex")),
            Err(ToolError::InvalidPath(_))
        ));
    }

    #[test]
    fn glob_does_not_consume_grep_scan_byte_budget() {
        let root = TempWorkspace::new();
        let large = File::create(root.path.join("large.bin")).expect("large file");
        large
            .set_len(MAX_SCAN_BYTES + 1)
            .expect("sparse large file");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        assert_eq!(fs.glob("*.bin").expect("glob"), vec!["large.bin"]);
        assert!(matches!(
            fs.grep(Path::new("large.bin"), &Regex::new("x").expect("regex")),
            Err(ToolError::ResourceLimit(ResourceLimit::ScanBytes))
        ));
    }

    #[test]
    fn grep_directory_budget_charges_bytes_actually_read() {
        let root = TempWorkspace::new();
        std::fs::write(root.path.join("growing.txt"), b"x").expect("seed file");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        let directory = fs
            .open_beneath(
                Path::new("."),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
                0,
            )
            .expect("open traversal root");
        let mut budget = WalkBudget {
            max_bytes: Some(MAX_SCAN_BYTES),
            ..Default::default()
        };
        let mut matches = Vec::new();
        let mut serialized_bytes = 2usize;
        let result = walk_entry(
            &directory,
            Path::new(""),
            "growing.txt",
            &mut budget,
            0,
            &mut |path, fd, budget| {
                // The inode grows after open/metadata, so stale pre-read
                // lengths cannot account for the bytes consumed by grep.
                let mut grown = OpenOptions::new()
                    .append(true)
                    .open(root.path.join("growing.txt"))
                    .expect("open growing file");
                grown
                    .write_all(&vec![b'x'; MAX_SCAN_BYTES as usize + 1])
                    .expect("grow file");
                grep_file(
                    path,
                    fd,
                    &Regex::new("needle").expect("regex"),
                    &mut matches,
                    &mut serialized_bytes,
                    budget,
                )
            },
        );
        assert!(matches!(
            result,
            Err(ToolError::ResourceLimit(ResourceLimit::ScanBytes))
        ));
    }

    #[test]
    fn read_file_terminal_newline_line_metadata_is_consistent() {
        let root = TempWorkspace::new();
        std::fs::write(root.path.join("terminal.txt"), b"a\n").expect("write file");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        let result = fs
            .read_file(Path::new("terminal.txt"), 0, 100)
            .expect("read file");
        assert_eq!(result.total_lines, 1);
        assert_eq!(result.output_lines, 1);
        assert_eq!(result.content, "a\n");
    }

    #[test]
    fn read_file_enforces_the_model_visible_byte_limit_at_its_public_boundary() {
        let root = TempWorkspace::new();
        std::fs::write(
            root.path.join("boundary.txt"),
            vec![b'x'; DEFAULT_MAX_BYTES],
        )
        .expect("write boundary file");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");

        let boundary = fs
            .read_file(Path::new("boundary.txt"), 0, DEFAULT_MAX_BYTES)
            .expect("exact byte limit is accepted");
        assert_eq!(boundary.output_bytes, DEFAULT_MAX_BYTES);
        assert_eq!(boundary.content.len(), DEFAULT_MAX_BYTES);

        for requested in [DEFAULT_MAX_BYTES + 1, usize::MAX] {
            assert!(matches!(
                fs.read_file(Path::new("missing.txt"), 0, requested),
                Err(ToolError::Protocol(message))
                    if message == format!(
                        "read_file max_bytes exceeds the model-visible limit of {DEFAULT_MAX_BYTES} bytes"
                    )
            ));
        }
    }

    #[test]
    fn read_file_line_boundary_preserves_fitting_terminal_newline() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        for lines in [1999usize, 2000, 2001] {
            for terminal_newline in [false, true] {
                let mut input = "x\n".repeat(lines);
                if !terminal_newline {
                    input.pop();
                }
                std::fs::write(root.path.join("boundary.txt"), &input).expect("write file");
                let result = fs
                    .read_file(
                        Path::new("boundary.txt"),
                        0,
                        super::super::truncate::DEFAULT_MAX_BYTES,
                    )
                    .expect("read file");
                assert_eq!(result.total_lines, lines);
                assert_eq!(
                    result.truncated,
                    lines > super::super::truncate::DEFAULT_MAX_LINES
                );
                if lines <= super::super::truncate::DEFAULT_MAX_LINES {
                    assert_eq!(result.content, input);
                }
            }
        }
    }

    #[test]
    fn fifo_and_other_non_regular_inputs_fail_without_waiting_for_a_writer() {
        let root = TempWorkspace::new();
        let fifo = CString::new("input.fifo").expect("fifo name");
        let root_c = CString::new(root.path.as_os_str().as_bytes()).expect("root path");
        let root_fd = unsafe {
            libc::open(
                root_c.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        };
        assert!(root_fd >= 0);
        let made = unsafe { libc::mkfifoat(root_fd, fifo.as_ptr(), 0o600) };
        unsafe {
            libc::close(root_fd);
        }
        assert_eq!(made, 0);
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        let started = std::time::Instant::now();
        assert!(fs.read_file(Path::new("input.fifo"), 0, 100).is_err());
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1),
            "FIFO open must be nonblocking"
        );
        assert!(
            fs.grep(
                Path::new("input.fifo"),
                &Regex::new("needle").expect("regex")
            )
            .is_err()
        );

        let (socket, _peer) = std::os::unix::net::UnixStream::pair().expect("create socket pair");
        let socket: OwnedFd = socket.into();
        assert!(ensure_regular_file(&socket, "test socket").is_err());

        let device = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NONBLOCK)
            .open("/dev/null")
            .expect("open character device nonblocking");
        let device: OwnedFd = device.into();
        assert!(ensure_regular_file(&device, "test device").is_err());
    }

    #[test]
    fn recursive_traversal_fails_closed_on_nonregular_and_open_errors() {
        let fifo_root = TempWorkspace::new();
        let fifo = CString::new(fifo_root.path.join("entry.fifo").as_os_str().as_bytes())
            .expect("FIFO path");
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        let fs = WorkspaceFs::open(&fifo_root.path).expect("workspace fs");
        assert!(matches!(fs.glob("**"), Err(ToolError::InvalidPath(_))));
        assert!(matches!(
            fs.grep(Path::new("."), &Regex::new(".").expect("regex")),
            Err(ToolError::InvalidPath(_))
        ));

        let open_error_root = TempWorkspace::new();
        symlink(
            open_error_root.path.join("missing-target"),
            open_error_root.path.join("dangling"),
        )
        .expect("create dangling symlink");
        let fs = WorkspaceFs::open(&open_error_root.path).expect("workspace fs");
        assert!(matches!(fs.glob("**"), Err(ToolError::Io(_))));
        assert!(matches!(
            fs.grep(Path::new("."), &Regex::new(".").expect("regex")),
            Err(ToolError::Io(_))
        ));
    }

    #[test]
    fn traversal_propagates_concurrent_disappearance() {
        let root = TempWorkspace::new();
        std::fs::write(root.path.join("raced.txt"), b"content").expect("write raced entry");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        let directory = fs
            .open_beneath(
                Path::new("."),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
                0,
            )
            .expect("open traversal root");
        std::fs::remove_file(root.path.join("raced.txt")).expect("remove raced entry");

        let mut budget = WalkBudget::default();
        let result = walk_entry(
            &directory,
            Path::new(""),
            "raced.txt",
            &mut budget,
            0,
            &mut |_path, _fd, _budget| Ok(()),
        );
        assert!(matches!(
            result,
            Err(ToolError::Io(error)) if error.raw_os_error() == Some(libc::ENOENT)
        ));
    }

    #[test]
    fn edit_reopens_reject_fifo_swaps_without_exhausting_thirty_two_workers() {
        fn replace_with_fifo(path: &Path) {
            std::fs::remove_file(path).expect("remove regular destination");
            let path = CString::new(path.as_os_str().as_bytes()).expect("FIFO path");
            assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        }

        let root = TempWorkspace::new();
        let (tx, rx) = std::sync::mpsc::channel();
        let mut workers = Vec::new();
        for index in 0..32 {
            let path = root.path.join(format!("swap-{index}.txt"));
            std::fs::write(&path, b"alpha").expect("write edit source");
            let tx = tx.clone();
            workers.push(std::thread::spawn(move || {
                let fs = WorkspaceFs::open(path.parent().expect("workspace parent"))
                    .expect("workspace fs");
                let relative = Path::new(path.file_name().expect("file name"));
                let result = if index % 2 == 0 {
                    fs.edit_file_with_hooks(
                        relative,
                        "alpha",
                        "edited",
                        || replace_with_fifo(&path),
                        || {},
                    )
                } else {
                    fs.edit_file_with_hooks(
                        relative,
                        "alpha",
                        "edited",
                        || {},
                        || replace_with_fifo(&path),
                    )
                };
                tx.send(result).expect("report edit result");
            }));
        }
        drop(tx);

        for _ in 0..32 {
            let result = rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("FIFO swap must not block a worker");
            assert!(result.is_err());
        }
        for worker in workers {
            worker.join().expect("edit worker");
        }

        std::fs::write(root.path.join("healthy.txt"), b"alpha").expect("write healthy source");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        fs.edit_file(Path::new("healthy.txt"), "alpha", "edited")
            .expect("worker capacity remains available");
    }

    #[test]
    fn edit_metadata_preflight_rejects_oversized_regular_file() {
        let root = TempWorkspace::new();
        let path = root.path.join("large.txt");
        let file = File::create(&path).expect("large file");
        file.set_len(MAX_EDIT_FILE_BYTES + 1)
            .expect("sparse large file");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        assert!(matches!(
            fs.edit_file(Path::new("large.txt"), "x", "y"),
            Err(ToolError::ResourceLimit(ResourceLimit::InputBytes {
                observed,
                limit: MAX_EDIT_FILE_BYTES,
            })) if observed == MAX_EDIT_FILE_BYTES + 1
        ));
    }

    #[test]
    fn grep_bounds_serialized_matches_during_the_scan() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        fs.write_file(
            Path::new("matches.txt"),
            ("x".repeat(500) + "\n").repeat(500).as_bytes(),
        )
        .expect("write matches");
        assert!(matches!(
            fs.grep(Path::new("matches.txt"), &Regex::new("x").expect("regex")),
            Err(ToolError::ResourceLimit(ResourceLimit::ScanBytes))
        ));
    }

    #[test]
    fn grep_rejects_invalid_utf8_line_without_returning_a_lossy_match() {
        let root = TempWorkspace::new();
        std::fs::write(root.path.join("invalid.txt"), b"needle\xff\n")
            .expect("write invalid grep input");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");

        assert!(matches!(
            fs.grep(
                Path::new("invalid.txt"),
                &Regex::new("needle").expect("regex")
            ),
            Err(ToolError::Protocol(_))
        ));
    }

    #[test]
    fn workspace_download_installs_binary_files_above_inline_limit_through_exact_maximum() {
        let root = TempWorkspace::new();
        std::fs::create_dir(root.path.join("downloads")).expect("create download directory");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");

        for (download_id, filename, size) in [
            ("over-inline", "over-inline.bin", 2 * 1024 * 1024 + 1),
            (
                "exact-maximum",
                "exact-maximum.bin",
                MAX_WORKSPACE_DOWNLOAD_BYTES as usize,
            ),
        ] {
            let bytes = (0..size)
                .map(|index| ((index * 31 + 17) % 251) as u8)
                .collect::<Vec<_>>();
            let path = format!("downloads/{filename}");
            let receipt = install_workspace_download(&fs, download_id, &path, &bytes);

            assert_eq!(receipt.path, path);
            assert_eq!(receipt.size, size as u64);
            assert_eq!(receipt.sha256, sha256(&bytes));
            assert_eq!(
                std::fs::read(root.path.join(&receipt.path)).expect("read installed download"),
                bytes
            );
            assert!(download_temporary_paths(&root.path).is_empty());
        }
    }

    #[test]
    fn workspace_download_rejects_path_and_symlink_escapes_without_external_mutation() {
        let root = TempWorkspace::new();
        let outside = TempWorkspace::new();
        let outside_target = outside.path.join("outside.bin");
        std::fs::write(&outside_target, b"outside").expect("write outside fixture");
        std::fs::create_dir(root.path.join("dir")).expect("create ordinary parent");
        symlink(&outside.path, root.path.join("escape")).expect("create parent escape symlink");
        symlink(&outside_target, root.path.join("linked.bin"))
            .expect("create final escape symlink");
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        let digest = sha256(b"x");

        for destination in [
            "../outside.bin",
            "escape/leak.bin",
            "linked.bin",
            "dir//file.bin",
            "dir/./file.bin",
            "file.bin/",
        ] {
            assert!(
                fs.begin_workspace_download("escape-attempt", Path::new(destination), 1, &digest)
                    .is_err(),
                "accepted escaped destination {destination}"
            );
        }

        assert!(!outside.path.join("leak.bin").exists());
        assert_eq!(std::fs::read(outside_target).unwrap(), b"outside");
        assert!(download_temporary_paths(&root.path).is_empty());
    }

    #[test]
    fn workspace_download_collision_preserves_destination_and_cleans_temporary() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        let destination = root.path.join("collision.bin");
        std::fs::write(&destination, b"original").expect("write original destination");
        let bytes = b"download";
        let digest = sha256(bytes);

        assert!(matches!(
            fs.begin_workspace_download(
                "initial-collision",
                Path::new("collision.bin"),
                bytes.len() as u64,
                &digest,
            ),
            Err(ToolError::Protocol(message)) if message == WORKSPACE_DOWNLOAD_COLLISION
        ));
        assert_eq!(std::fs::read(&destination).unwrap(), b"original");

        std::fs::remove_file(&destination).expect("remove initial destination");
        fs.begin_workspace_download(
            "finish-collision",
            Path::new("collision.bin"),
            bytes.len() as u64,
            &digest,
        )
        .expect("begin colliding download");
        fs.append_workspace_download("finish-collision", 0, bytes)
            .expect("append colliding download");
        std::fs::write(&destination, b"competitor").expect("install competing destination");
        assert!(matches!(
            fs.finish_workspace_download("finish-collision"),
            Err(ToolError::Protocol(message)) if message == WORKSPACE_DOWNLOAD_COLLISION
        ));
        assert_eq!(std::fs::read(&destination).unwrap(), b"competitor");
        assert!(download_temporary_paths(&root.path).is_empty());
    }

    #[test]
    fn workspace_download_publishes_held_inode_despite_named_temp_substitution_attempt() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        let expected = b"verified attachment bytes";
        let forged = b"attacker replacement";
        let digest = sha256(expected);
        fs.begin_workspace_download(
            "inode-bound",
            Path::new("inode-bound.bin"),
            expected.len() as u64,
            &digest,
        )
        .unwrap();
        fs.append_workspace_download("inode-bound", 0, expected)
            .unwrap();
        assert!(
            std::fs::read_dir(&root.path).unwrap().next().is_none(),
            "streaming must not expose a replaceable named temporary"
        );

        let forged_path = root.path.join(".sumi-download-attacker.tmp");
        let receipt = fs
            .finish_workspace_download_with_hooks(
                "inode-bound",
                || {},
                || {
                    // This is the exact old digest/fsync-to-rename race window.
                    // A same-workspace writer can create or replace any public
                    // temporary name, but the publisher links only its held fd.
                    std::fs::write(&forged_path, forged).unwrap();
                },
                || Ok(()),
            )
            .expect("publish held O_TMPFILE inode");

        assert_eq!(receipt.sha256, digest);
        assert_eq!(
            std::fs::read(root.path.join("inode-bound.bin")).unwrap(),
            expected
        );
        assert_eq!(std::fs::read(forged_path).unwrap(), forged);
    }

    #[test]
    fn workspace_download_abort_and_digest_mismatch_remove_partial_files() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        let bytes = b"complete attachment bytes";
        let digest = sha256(bytes);

        fs.begin_workspace_download(
            "cancelled",
            Path::new("cancelled.bin"),
            bytes.len() as u64,
            &digest,
        )
        .expect("begin cancelled download");
        fs.append_workspace_download("cancelled", 0, &bytes[..8])
            .expect("append partial download");
        assert_eq!(
            fs.abort_workspace_download("cancelled").unwrap(),
            WorkspaceDownloadAbort::Aborted
        );
        assert_eq!(
            fs.abort_workspace_download("cancelled").unwrap(),
            WorkspaceDownloadAbort::Aborted,
            "abort must be idempotent"
        );
        assert!(!root.path.join("cancelled.bin").exists());

        fs.begin_workspace_download(
            "digest-mismatch",
            Path::new("mismatch.bin"),
            bytes.len() as u64,
            &"0".repeat(64),
        )
        .expect("begin digest mismatch");
        fs.append_workspace_download("digest-mismatch", 0, bytes)
            .expect("append digest mismatch");
        assert!(matches!(
            fs.finish_workspace_download("digest-mismatch"),
            Err(ToolError::Protocol(message)) if message == WORKSPACE_DOWNLOAD_MISMATCH
        ));
        assert!(!root.path.join("mismatch.bin").exists());

        fs.begin_workspace_download(
            "append-error",
            Path::new("append-error.bin"),
            bytes.len() as u64,
            &digest,
        )
        .expect("begin append error");
        fs.append_workspace_download("append-error", 0, &bytes[..8])
            .expect("append before offset error");
        assert!(matches!(
            fs.append_workspace_download("append-error", 3, &bytes[8..]),
            Err(ToolError::Protocol(message)) if message == WORKSPACE_DOWNLOAD_STATE_MISMATCH
        ));
        assert!(!root.path.join("append-error.bin").exists());
        assert!(download_temporary_paths(&root.path).is_empty());
    }

    #[test]
    fn workspace_download_abort_drops_anonymous_partial_without_a_workspace_name() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        let bytes = b"partial attachment";
        let digest = sha256(bytes);
        fs.begin_workspace_download(
            "anonymous-abort",
            Path::new("anonymous-abort.bin"),
            bytes.len() as u64,
            &digest,
        )
        .unwrap();
        fs.append_workspace_download("anonymous-abort", 0, &bytes[..7])
            .unwrap();
        assert!(
            std::fs::read_dir(&root.path).unwrap().next().is_none(),
            "an incomplete O_TMPFILE download must have no workspace directory entry"
        );
        assert_eq!(
            fs.abort_workspace_download("anonymous-abort").unwrap(),
            WorkspaceDownloadAbort::Aborted
        );
        assert!(!root.path.join("anonymous-abort.bin").exists());
        assert!(download_temporary_paths(&root.path).is_empty());
    }

    #[test]
    fn workspace_download_rejects_duplicate_and_replayed_begin_ids() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        let digest = sha256(b"x");
        fs.begin_workspace_download("stable-id", Path::new("one.bin"), 1, &digest)
            .expect("first begin");
        assert!(matches!(
            fs.begin_workspace_download("stable-id", Path::new("two.bin"), 1, &digest),
            Err(ToolError::Protocol(message)) if message == WORKSPACE_DOWNLOAD_STATE_MISMATCH
        ));
        fs.abort_workspace_download("stable-id").unwrap();
        assert!(matches!(
            fs.begin_workspace_download("stable-id", Path::new("two.bin"), 1, &digest),
            Err(ToolError::Protocol(message)) if message == WORKSPACE_DOWNLOAD_STATE_MISMATCH
        ));
        assert!(download_temporary_paths(&root.path).is_empty());
    }

    #[test]
    fn workspace_download_finish_and_abort_have_one_serialized_winner() {
        let root = TempWorkspace::new();
        let fs = Arc::new(WorkspaceFs::open(&root.path).expect("workspace fs"));
        let bytes = b"race";
        let digest = sha256(bytes);

        fs.begin_workspace_download(
            "finish-wins",
            Path::new("finish-wins.bin"),
            bytes.len() as u64,
            &digest,
        )
        .unwrap();
        fs.append_workspace_download("finish-wins", 0, bytes)
            .unwrap();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let finish_fs = fs.clone();
        let finish_entered = entered.clone();
        let finish_release = release.clone();
        let finish = thread::spawn(move || {
            finish_fs.finish_workspace_download_with_hook("finish-wins", || {
                finish_entered.wait();
                finish_release.wait();
            })
        });
        entered.wait();
        let abort_fs = fs.clone();
        let abort = thread::spawn(move || abort_fs.abort_workspace_download("finish-wins"));
        release.wait();
        assert_eq!(finish.join().unwrap().unwrap().path, "finish-wins.bin");
        assert_eq!(
            abort.join().unwrap().unwrap(),
            WorkspaceDownloadAbort::TooLate
        );
        assert_eq!(
            std::fs::read(root.path.join("finish-wins.bin")).unwrap(),
            bytes
        );

        fs.begin_workspace_download(
            "abort-wins",
            Path::new("abort-wins.bin"),
            bytes.len() as u64,
            &digest,
        )
        .unwrap();
        fs.append_workspace_download("abort-wins", 0, bytes)
            .unwrap();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let abort_fs = fs.clone();
        let abort_entered = entered.clone();
        let abort_release = release.clone();
        let abort = thread::spawn(move || {
            abort_fs.abort_workspace_download_with_hook("abort-wins", || {
                abort_entered.wait();
                abort_release.wait();
            })
        });
        entered.wait();
        let finish_fs = fs.clone();
        let finish = thread::spawn(move || finish_fs.finish_workspace_download("abort-wins"));
        release.wait();
        assert_eq!(
            abort.join().unwrap().unwrap(),
            WorkspaceDownloadAbort::Aborted
        );
        assert!(matches!(
            finish.join().unwrap(),
            Err(ToolError::Protocol(message)) if message == WORKSPACE_DOWNLOAD_STATE_MISMATCH
        ));
        assert!(!root.path.join("abort-wins.bin").exists());
        assert!(download_temporary_paths(&root.path).is_empty());
    }

    #[test]
    fn workspace_download_post_publish_failure_is_indeterminate_and_too_late_to_abort() {
        let root = TempWorkspace::new();
        let fs = WorkspaceFs::open(&root.path).expect("workspace fs");
        let bytes = b"committed";
        let digest = sha256(bytes);
        fs.begin_workspace_download(
            "post-rename",
            Path::new("post-rename.bin"),
            bytes.len() as u64,
            &digest,
        )
        .unwrap();
        fs.append_workspace_download("post-rename", 0, bytes)
            .unwrap();

        let result = fs.finish_workspace_download_with_hooks(
            "post-rename",
            || {},
            || {},
            || {
                Err(ToolError::Io(std::io::Error::other(
                    "injected sync failure",
                )))
            },
        );
        assert!(matches!(result, Err(ToolError::RpcIndeterminate(_))));
        assert_eq!(
            std::fs::read(root.path.join("post-rename.bin")).unwrap(),
            bytes
        );
        assert_eq!(
            fs.abort_workspace_download("post-rename").unwrap(),
            WorkspaceDownloadAbort::TooLate
        );
        assert!(download_temporary_paths(&root.path).is_empty());
    }
}
