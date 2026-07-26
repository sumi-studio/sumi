//! Filesystem-backed artifact operations rooted at one stable directory FD.

use std::{
    collections::{HashMap, HashSet},
    ffi::{CStr, CString, OsStr},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::{ffi::OsStrExt, fs::OpenOptionsExt},
    },
    path::Path,
    sync::Mutex,
};

use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::protocol::{
    ArtifactKind, ArtifactOperation, RpcOperationValidation, parse_artifact_handle,
};
use crate::tools::{
    ResourceLimit, ToolError,
    fs::{MAX_GREP_MATCHES, MAX_GREP_SERIALIZED_BYTES},
    truncate::{GREP_MAX_LINE_LENGTH, truncate_line_total},
};

const MAX_SCAN_BYTES: u64 = 10 * 1024 * 1024;
const MAX_TRACKED_ARTIFACTS: usize = 4_096;
const RESOLVE_NO_MAGICLINKS: u64 = 0x02;
const RESOLVE_NO_SYMLINKS: u64 = 0x04;
const RESOLVE_BENEATH: u64 = 0x08;

#[repr(C)]
struct OpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactGrepMatch {
    pub line_number: u64,
    pub line: String,
    pub line_truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactResponse {
    Begun { handle: String, offset: u64 },
    Appended { offset: u64 },
    Finished,
    Deleted,
    Read { content: Vec<u8>, eof: bool },
    Grep { matches: Vec<ArtifactGrepMatch> },
}

/// A broker instance pins its root inode for its lifetime. All descendants are
/// opened relative to this FD with a no-symlink `openat2(2)` policy.
pub struct ArtifactBroker {
    root: OwnedFd,
    state: Mutex<BrokerState>,
}

#[derive(Default)]
struct BrokerState {
    // T13 keeps bounded retry metadata for this broker process only. T26 owns
    // the durable request journal/receipt needed to reconstruct it on restart.
    artifacts: HashMap<String, ArtifactRecord>,
    // Tombstones observed by this process.  A durable ledger is T29; this set
    // makes replay of the same `DeleteConversationArtifacts` idempotent within
    // a single broker lifetime. The map binds each tombstone_id to exactly one
    // old_conversation_id, so a replay for that conversation is idempotent but
    // cross-conversation reuse is rejected rather than silently accepted.
    applied_tombstones: HashMap<String, String>,
    deleted_conversations: HashSet<String>,
}

struct ArtifactRecord {
    initial_len: u64,
    initial_digest: [u8; 32],
    committed_offset: u64,
    last_append: Option<AppendReceipt>,
    finished: bool,
}

struct AppendReceipt {
    offset: u64,
    length: u64,
    digest: [u8; 32],
    next_offset: u64,
}

impl ArtifactBroker {
    pub fn open(root: &Path) -> Result<Self, ToolError> {
        if !root.is_absolute() {
            return Err(ToolError::InvalidPath(
                "artifact broker root must be absolute".to_owned(),
            ));
        }
        let filesystem_root = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open("/")?;
        let relative = root.strip_prefix("/").map_err(|_| {
            ToolError::InvalidPath("artifact broker root must be absolute".to_owned())
        })?;
        let relative = CString::new(relative.as_os_str().as_bytes())
            .map_err(|_| ToolError::InvalidPath("artifact root contains NUL".to_owned()))?;
        let root = openat2_cstr(
            filesystem_root.as_raw_fd(),
            &relative,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            0,
        )?;
        if !File::from(root.try_clone()?).metadata()?.is_dir() {
            return Err(ToolError::InvalidPath(
                "artifact broker root is not a directory".to_owned(),
            ));
        }
        fchmod(root.as_raw_fd(), 0o700)?;
        probe_openat2(root.as_raw_fd())?;
        Ok(Self {
            root,
            state: Mutex::new(BrokerState::default()),
        })
    }

    pub fn execute(&self, operation: ArtifactOperation) -> Result<ArtifactResponse, ToolError> {
        operation.validate()?;
        match operation {
            ArtifactOperation::BeginToolOutput {
                conversation_id,
                execution_id,
                content,
            } => self.begin_tool_output(&conversation_id, &execution_id, &content),
            ArtifactOperation::AppendToolOutput {
                conversation_id,
                handle,
                offset,
                content,
            } => self.append_tool_output(&conversation_id, &handle, offset, &content),
            ArtifactOperation::FinishToolOutput {
                conversation_id,
                handle,
            } => self.finish_tool_output(&conversation_id, &handle),
            ArtifactOperation::ReadArtifact {
                conversation_id,
                handle,
                offset,
                limit,
            } => self.read_artifact(&conversation_id, &handle, offset, limit),
            ArtifactOperation::GrepArtifact {
                conversation_id,
                handle,
                pattern,
            } => self.grep_artifact(&conversation_id, &handle, &pattern),
            ArtifactOperation::DeleteConversationArtifacts {
                old_conversation_id,
                tombstone_id,
            } => self.delete_conversation_artifacts(&old_conversation_id, &tombstone_id),
        }
    }

    fn begin_tool_output(
        &self,
        conversation_id: &str,
        execution_id: &str,
        content: &[u8],
    ) -> Result<ArtifactResponse, ToolError> {
        let handle = format!("artifact://{conversation_id}/tool-output/{execution_id}");
        let initial_len = u64::try_from(content.len()).map_err(|_| {
            ToolError::Protocol("artifact initial content length overflow".to_owned())
        })?;
        let initial_digest: [u8; 32] = Sha256::digest(content).into();
        let mut state = self.lock_state()?;
        if let Some(record) = state.artifacts.get(&handle) {
            if record.initial_len != initial_len || record.initial_digest != initial_digest {
                return Err(ToolError::Protocol(
                    "duplicate BeginToolOutput has conflicting content".to_owned(),
                ));
            }
            return Ok(ArtifactResponse::Begun {
                handle,
                offset: record.initial_len,
            });
        }
        if state.artifacts.len() >= MAX_TRACKED_ARTIFACTS {
            return Err(ToolError::Protocol(format!(
                "artifact process state capacity of {MAX_TRACKED_ARTIFACTS} was reached"
            )));
        }
        let (conversation, kind) =
            self.ensure_artifact_dirs(conversation_id, ArtifactKind::ToolOutput)?;
        let name = cstring(execution_id)?;
        let created = match openat2_cstr(
            kind.as_raw_fd(),
            &name,
            libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_CREAT | libc::O_EXCL,
            0o600,
        ) {
            Ok(fd) => (fd, true),
            Err(ToolError::Io(error)) if error.raw_os_error() == Some(libc::EEXIST) => (
                openat2_cstr(
                    kind.as_raw_fd(),
                    &name,
                    libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                    0,
                )?,
                false,
            ),
            Err(error) => return Err(error),
        };
        ensure_regular(&created.0, "artifact")?;
        fchmod(created.0.as_raw_fd(), 0o600)?;
        let mut file = File::from(created.0);
        lock_exclusive(&file)?;
        if created.1 {
            file.write_all(content)?;
            file.sync_all()?;
            fsync_fd(kind.as_raw_fd())?;
            fsync_fd(conversation.as_raw_fd())?;
            fsync_fd(self.root.as_raw_fd())?;
        } else if !file_equals(&mut file, content)? {
            return Err(ToolError::Protocol(
                "duplicate BeginToolOutput has conflicting content".to_owned(),
            ));
        }
        state.artifacts.insert(
            handle.clone(),
            ArtifactRecord {
                initial_len,
                initial_digest,
                committed_offset: initial_len,
                last_append: None,
                finished: false,
            },
        );
        Ok(ArtifactResponse::Begun {
            handle,
            offset: initial_len,
        })
    }

    fn append_tool_output(
        &self,
        conversation_id: &str,
        handle: &str,
        offset: u64,
        content: &[u8],
    ) -> Result<ArtifactResponse, ToolError> {
        let (kind, artifact_id) =
            checked_handle(conversation_id, handle, Some(ArtifactKind::ToolOutput))?;
        let mut state = self.lock_state()?;
        let record = state.artifacts.get_mut(handle).ok_or_else(|| {
            ToolError::Protocol("artifact append has no process-local Begin state".to_owned())
        })?;
        if record.finished {
            return Err(ToolError::Protocol(
                "cannot append to a finished artifact".to_owned(),
            ));
        }
        let content_len = u64::try_from(content.len())
            .map_err(|_| ToolError::Protocol("artifact append length overflow".to_owned()))?;
        let content_digest: [u8; 32] = Sha256::digest(content).into();
        if let Some(receipt) = record.last_append.as_ref()
            && record.committed_offset == receipt.next_offset
            && offset == receipt.offset
        {
            if content_len == receipt.length && content_digest == receipt.digest {
                return Ok(ArtifactResponse::Appended {
                    offset: receipt.next_offset,
                });
            }
            return Err(ToolError::Protocol(
                "artifact append replay has conflicting content".to_owned(),
            ));
        }
        if record.committed_offset != offset {
            return Err(ToolError::Protocol(format!(
                "artifact append offset mismatch: expected {}, received {offset}",
                record.committed_offset
            )));
        }
        let (_conversation, kind_dir) = self.open_artifact_dirs(conversation_id, kind)?;
        let fd = open_regular_file(&kind_dir, artifact_id, libc::O_RDWR)?;
        let mut file = File::from(fd);
        lock_exclusive(&file)?;
        let actual = file.metadata()?.len();
        if actual != offset {
            return Err(ToolError::Protocol(format!(
                "artifact append offset mismatch: expected {actual}, received {offset}"
            )));
        }
        let next_offset = offset
            .checked_add(content_len)
            .ok_or_else(|| ToolError::Protocol("artifact append offset overflow".to_owned()))?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(content)?;
        file.sync_all()?;
        record.committed_offset = next_offset;
        record.last_append = Some(AppendReceipt {
            offset,
            length: content_len,
            digest: content_digest,
            next_offset,
        });
        Ok(ArtifactResponse::Appended {
            offset: next_offset,
        })
    }

    fn finish_tool_output(
        &self,
        conversation_id: &str,
        handle: &str,
    ) -> Result<ArtifactResponse, ToolError> {
        let (kind, artifact_id) =
            checked_handle(conversation_id, handle, Some(ArtifactKind::ToolOutput))?;
        let mut state = self.lock_state()?;
        let record = state.artifacts.get_mut(handle).ok_or_else(|| {
            ToolError::Protocol("artifact finish has no process-local Begin state".to_owned())
        })?;
        if record.finished {
            return Ok(ArtifactResponse::Finished);
        }
        let (_conversation, kind_dir) = self.open_artifact_dirs(conversation_id, kind)?;
        let fd = open_regular_file(&kind_dir, artifact_id, libc::O_RDWR)?;
        let file = File::from(fd);
        lock_exclusive(&file)?;
        file.sync_all()?;
        fsync_fd(kind_dir.as_raw_fd())?;
        record.finished = true;
        Ok(ArtifactResponse::Finished)
    }

    fn read_artifact(
        &self,
        conversation_id: &str,
        handle: &str,
        offset: u64,
        limit: usize,
    ) -> Result<ArtifactResponse, ToolError> {
        let (kind, artifact_id) = checked_handle(conversation_id, handle, None)?;
        let limit_u64 = u64::try_from(limit)
            .map_err(|_| ToolError::Protocol("artifact read limit overflow".to_owned()))?;
        offset
            .checked_add(limit_u64)
            .ok_or_else(|| ToolError::Protocol("artifact read range overflow".to_owned()))?;
        let (_conversation, kind_dir) = self.open_artifact_dirs(conversation_id, kind)?;
        let fd = open_regular_file(&kind_dir, artifact_id, libc::O_RDONLY | libc::O_NONBLOCK)?;
        let mut file = File::from(fd);
        lock_shared(&file)?;
        let length = file.metadata()?.len();
        file.seek(SeekFrom::Start(offset))?;
        let mut content = vec![0; limit];
        let read = file.read(&mut content)?;
        content.truncate(read);
        let end = offset
            .checked_add(u64::try_from(read).map_err(|_| {
                ToolError::Protocol("artifact read result length overflow".to_owned())
            })?)
            .ok_or_else(|| ToolError::Protocol("artifact read result overflow".to_owned()))?;
        Ok(ArtifactResponse::Read {
            content,
            eof: end >= length,
        })
    }

    fn grep_artifact(
        &self,
        conversation_id: &str,
        handle: &str,
        pattern: &str,
    ) -> Result<ArtifactResponse, ToolError> {
        let (kind, artifact_id) = checked_handle(conversation_id, handle, None)?;
        let pattern = Regex::new(pattern)
            .map_err(|error| ToolError::Protocol(format!("invalid grep pattern: {error}")))?;
        let (_conversation, kind_dir) = self.open_artifact_dirs(conversation_id, kind)?;
        let fd = open_regular_file(&kind_dir, artifact_id, libc::O_RDONLY | libc::O_NONBLOCK)?;
        let file = File::from(fd);
        lock_shared(&file)?;
        if file.metadata()?.len() > MAX_SCAN_BYTES {
            return Err(ToolError::ResourceLimit(ResourceLimit::ScanBytes));
        }
        let mut reader = BufReader::new(file.take(MAX_SCAN_BYTES + 1));
        let mut raw = Vec::new();
        let mut matches = Vec::new();
        let empty_response = serde_json::to_vec(&ArtifactResponse::Grep {
            matches: Vec::new(),
        })
        .map_err(|error| ToolError::Protocol(format!("grep encode failed: {error}")))?;
        let mut serialized_bytes = empty_response.len();
        let mut line_number = 0u64;
        loop {
            raw.clear();
            let read = reader.read_until(b'\n', &mut raw)?;
            if reader.get_ref().limit() == 0 {
                return Err(ToolError::ResourceLimit(ResourceLimit::ScanBytes));
            }
            if read == 0 {
                break;
            }
            line_number = line_number
                .checked_add(1)
                .ok_or_else(|| ToolError::Protocol("artifact line count overflow".to_owned()))?;
            if raw.ends_with(b"\n") {
                raw.pop();
                if raw.ends_with(b"\r") {
                    raw.pop();
                }
            }
            let text = std::str::from_utf8(&raw).map_err(|_| {
                ToolError::Protocol("grep accepts UTF-8 text lines only".to_owned())
            })?;
            if !pattern.is_match(text) {
                continue;
            }
            if matches.len() >= MAX_GREP_MATCHES {
                return Err(ToolError::ResourceLimit(ResourceLimit::ScanEntries));
            }
            let (line, line_truncated) = truncate_line_total(text, GREP_MAX_LINE_LENGTH);
            let candidate = ArtifactGrepMatch {
                line_number,
                line,
                line_truncated,
            };
            let encoded = serde_json::to_vec(&candidate)
                .map_err(|error| ToolError::Protocol(format!("grep encode failed: {error}")))?;
            let next = serialized_bytes
                .checked_add(usize::from(!matches.is_empty()))
                .and_then(|bytes| bytes.checked_add(encoded.len()))
                .ok_or(ToolError::ResourceLimit(ResourceLimit::ScanBytes))?;
            if next > MAX_GREP_SERIALIZED_BYTES {
                return Err(ToolError::ResourceLimit(ResourceLimit::ScanBytes));
            }
            serialized_bytes = next;
            matches.push(candidate);
        }
        Ok(ArtifactResponse::Grep { matches })
    }

    fn delete_conversation_artifacts(
        &self,
        old_conversation_id: &str,
        tombstone_id: &str,
    ) -> Result<ArtifactResponse, ToolError> {
        let prefix = format!("artifact://{old_conversation_id}/");
        let mut state = self.lock_state()?;

        if let Some(recorded_conversation) = state.applied_tombstones.get(tombstone_id) {
            if recorded_conversation != old_conversation_id {
                return Err(ToolError::Protocol(
                    "tombstone_id is already bound to a different conversation".to_owned(),
                ));
            }
            // Idempotent replay: the conversation has already been deleted by
            // this tombstone in this broker lifetime.
            state
                .artifacts
                .retain(|handle, _| !handle.starts_with(&prefix));
            return Ok(ArtifactResponse::Deleted);
        }
        if state.deleted_conversations.contains(old_conversation_id) {
            // A distinct tombstone for an already-deleted target is a no-op.
            // Bind it only after the root directory has been fsync'd below.
            fsync_fd(self.root.as_raw_fd())?;
            state
                .artifacts
                .retain(|handle, _| !handle.starts_with(&prefix));
            state
                .applied_tombstones
                .insert(tombstone_id.to_owned(), old_conversation_id.to_owned());
            return Ok(ArtifactResponse::Deleted);
        }

        let name = cstring(old_conversation_id)?;
        let conversation_fd = match openat2_cstr(
            self.root.as_raw_fd(),
            &name,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        ) {
            Ok(fd) => fd,
            Err(ToolError::Io(error)) if error.raw_os_error() == Some(libc::ENOENT) => {
                // No directory to remove. Synchronize the root before
                // recording an in-memory idempotency claim.
                fsync_fd(self.root.as_raw_fd())?;
                state
                    .artifacts
                    .retain(|handle, _| !handle.starts_with(&prefix));
                state
                    .applied_tombstones
                    .insert(tombstone_id.to_owned(), old_conversation_id.to_owned());
                state
                    .deleted_conversations
                    .insert(old_conversation_id.to_owned());
                return Ok(ArtifactResponse::Deleted);
            }
            Err(error) => return Err(error),
        };

        if !std::fs::File::from(conversation_fd.try_clone()?)
            .metadata()?
            .is_dir()
        {
            return Err(ToolError::InvalidPath(
                "conversation artifact path is not a directory".to_owned(),
            ));
        }

        remove_dir_contents(conversation_fd.as_raw_fd())?;
        drop(conversation_fd);

        let rc =
            unsafe { libc::unlinkat(self.root.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) };
        if rc != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ENOENT) {
                return Err(error.into());
            }
        }

        // Make the deletion durable before recording it in memory. If fsync
        // fails, the in-process tombstone/deleted sets must not claim it.
        fsync_fd(self.root.as_raw_fd())?;
        state
            .artifacts
            .retain(|handle, _| !handle.starts_with(&prefix));
        state
            .applied_tombstones
            .insert(tombstone_id.to_owned(), old_conversation_id.to_owned());
        state
            .deleted_conversations
            .insert(old_conversation_id.to_owned());
        Ok(ArtifactResponse::Deleted)
    }

    fn ensure_artifact_dirs(
        &self,
        conversation_id: &str,
        kind: ArtifactKind,
    ) -> Result<(OwnedFd, OwnedFd), ToolError> {
        let conversation = ensure_dir(&self.root, conversation_id)?;
        let kind = ensure_dir(&conversation, kind_name(kind))?;
        Ok((conversation, kind))
    }

    fn open_artifact_dirs(
        &self,
        conversation_id: &str,
        kind: ArtifactKind,
    ) -> Result<(OwnedFd, OwnedFd), ToolError> {
        let conversation = open_dir(&self.root, conversation_id)?;
        let kind = open_dir(&conversation, kind_name(kind))?;
        Ok((conversation, kind))
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, BrokerState>, ToolError> {
        self.state
            .lock()
            .map_err(|_| ToolError::Protocol("artifact process state lock poisoned".to_owned()))
    }
}

fn remove_dir_contents(dir_fd: RawFd) -> Result<(), ToolError> {
    for name in read_dir_fd(dir_fd)? {
        remove_dir_entry(dir_fd, &name)?;
    }
    Ok(())
}

fn read_dir_fd(dir_fd: RawFd) -> Result<Vec<CString>, ToolError> {
    let mut buf = vec![0u8; 8192];
    let mut entries = Vec::new();
    loop {
        let n = unsafe { libc::syscall(libc::SYS_getdents64, dir_fd, buf.as_mut_ptr(), buf.len()) }
            as isize;
        if n < 0 {
            return Err(ToolError::Io(std::io::Error::last_os_error()));
        }
        if n == 0 {
            break;
        }
        let bytes_read = n as usize;
        let mut offset = 0usize;
        while offset + 19 <= bytes_read {
            let reclen = u16::from_ne_bytes([buf[offset + 16], buf[offset + 17]]) as usize;
            if reclen == 0 || offset + reclen > bytes_read {
                break;
            }
            let name_start = offset + 19;
            let name_bytes = &buf[name_start..offset + reclen];
            let name_len = name_bytes
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(name_bytes.len());
            let name = &name_bytes[..name_len];
            if name != b"." && name != b".." {
                entries.push(CString::new(name).map_err(|_| {
                    ToolError::InvalidPath("artifact path contains NUL".to_owned())
                })?);
            }
            offset += reclen;
        }
    }
    Ok(entries)
}

fn remove_dir_entry(parent_fd: RawFd, name: &CStr) -> Result<(), ToolError> {
    match openat2_cstr(
        parent_fd,
        name,
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        0,
    ) {
        Ok(dir_fd) => {
            remove_dir_contents(dir_fd.as_raw_fd())?;
            drop(dir_fd);
            let rc = unsafe { libc::unlinkat(parent_fd, name.as_ptr(), libc::AT_REMOVEDIR) };
            if rc != 0 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ENOENT) {
                    return Err(error.into());
                }
            }
            Ok(())
        }
        Err(ToolError::Io(error)) => match error.raw_os_error() {
            Some(libc::ENOTDIR) | Some(libc::ELOOP) | Some(libc::ENOENT) => {
                let rc = unsafe { libc::unlinkat(parent_fd, name.as_ptr(), 0) };
                if rc != 0 {
                    let error = std::io::Error::last_os_error();
                    if error.raw_os_error() != Some(libc::ENOENT) {
                        return Err(error.into());
                    }
                }
                Ok(())
            }
            _ => Err(error.into()),
        },
        Err(error) => Err(error),
    }
}

fn checked_handle<'a>(
    conversation_id: &str,
    handle: &'a str,
    required_kind: Option<ArtifactKind>,
) -> Result<(ArtifactKind, &'a str), ToolError> {
    let parsed = parse_artifact_handle(handle)?;
    if parsed.conversation_id != conversation_id {
        return Err(ToolError::InvalidPath(
            "artifact belongs to another conversation".to_owned(),
        ));
    }
    if required_kind.is_some_and(|kind| parsed.kind != kind) {
        return Err(ToolError::InvalidPath(
            "tool-output mutation requires a tool-output artifact handle".to_owned(),
        ));
    }
    Ok((parsed.kind, parsed.artifact_id))
}

fn kind_name(kind: ArtifactKind) -> &'static str {
    match kind {
        ArtifactKind::Attachments => "attachments",
        ArtifactKind::ToolOutput => "tool-output",
    }
}

fn ensure_dir(parent: &OwnedFd, name: &str) -> Result<OwnedFd, ToolError> {
    let name = cstring(name)?;
    let created = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) } == 0;
    if !created {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EEXIST) {
            return Err(error.into());
        }
    }
    let directory = openat2_cstr(
        parent.as_raw_fd(),
        &name,
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        0,
    )?;
    fchmod(directory.as_raw_fd(), 0o700)?;
    if created {
        fsync_fd(parent.as_raw_fd())?;
    }
    Ok(directory)
}

fn open_dir(parent: &OwnedFd, name: &str) -> Result<OwnedFd, ToolError> {
    let name = cstring(name)?;
    let directory = openat2_cstr(
        parent.as_raw_fd(),
        &name,
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        0,
    )?;
    fchmod(directory.as_raw_fd(), 0o700)?;
    Ok(directory)
}

fn open_regular_file(parent: &OwnedFd, name: &str, access: i32) -> Result<OwnedFd, ToolError> {
    let name = cstring(name)?;
    let fd = openat2_cstr(
        parent.as_raw_fd(),
        &name,
        access | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        0,
    )?;
    ensure_regular(&fd, "artifact")?;
    fchmod(fd.as_raw_fd(), 0o600)?;
    Ok(fd)
}

fn ensure_regular(fd: &OwnedFd, operation: &str) -> Result<(), ToolError> {
    if File::from(fd.try_clone()?).metadata()?.is_file() {
        Ok(())
    } else {
        Err(ToolError::InvalidPath(format!(
            "{operation} is not a regular file"
        )))
    }
}

fn probe_openat2(root: RawFd) -> Result<(), ToolError> {
    let dot = c".";
    openat2_cstr(
        root,
        dot,
        libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC,
        0,
    )?;
    Ok(())
}

fn openat2_cstr(
    directory: RawFd,
    path: &CStr,
    flags: i32,
    mode: libc::mode_t,
) -> Result<OwnedFd, ToolError> {
    let how = OpenHow {
        flags: flags as u64,
        mode: mode as u64,
        resolve: RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS | RESOLVE_NO_SYMLINKS,
    };
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            directory,
            path.as_ptr(),
            &how,
            std::mem::size_of::<OpenHow>(),
        )
    };
    if fd < 0 {
        Err(std::io::Error::last_os_error().into())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
    }
}

fn cstring(value: &str) -> Result<CString, ToolError> {
    CString::new(OsStr::new(value).as_bytes())
        .map_err(|_| ToolError::InvalidPath("artifact path contains NUL".to_owned()))
}

fn fchmod(fd: RawFd, mode: libc::mode_t) -> Result<(), ToolError> {
    if unsafe { libc::fchmod(fd, mode) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

fn fsync_fd(fd: RawFd) -> Result<(), ToolError> {
    if unsafe { libc::fsync(fd) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

fn lock_exclusive(file: &File) -> Result<(), ToolError> {
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

fn lock_shared(file: &File) -> Result<(), ToolError> {
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_SH) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

fn file_equals(file: &mut File, expected: &[u8]) -> Result<bool, ToolError> {
    if file.metadata()?.len()
        != u64::try_from(expected.len())
            .map_err(|_| ToolError::Protocol("artifact content length overflow".to_owned()))?
    {
        return Ok(false);
    }
    file.seek(SeekFrom::Start(0))?;
    let mut compared = 0usize;
    let mut buffer = [0u8; 8192];
    while compared < expected.len() {
        let read = file.read(&mut buffer)?;
        if read == 0 || buffer[..read] != expected[compared..compared + read] {
            return Ok(false);
        }
        compared = compared
            .checked_add(read)
            .ok_or_else(|| ToolError::Protocol("artifact comparison overflow".to_owned()))?;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::CString,
        fs,
        os::unix::{fs::MetadataExt, fs::PermissionsExt, fs::symlink},
        path::PathBuf,
        sync::Mutex,
    };

    use uuid::Uuid;

    use super::*;

    static UMASK_LOCK: Mutex<()> = Mutex::new(());

    struct UmaskGuard(libc::mode_t);

    impl UmaskGuard {
        fn set(mask: libc::mode_t) -> Self {
            Self(unsafe { libc::umask(mask) })
        }
    }

    impl Drop for UmaskGuard {
        fn drop(&mut self) {
            unsafe { libc::umask(self.0) };
        }
    }

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("sumi-artifact-broker-test-{}", Uuid::now_v7()));
            fs::create_dir(&path).expect("create test root");
            Self(path)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).expect("remove isolated test root");
        }
    }

    fn begin(content: impl Into<Vec<u8>>) -> ArtifactOperation {
        ArtifactOperation::BeginToolOutput {
            conversation_id: "conversation-1".to_owned(),
            execution_id: "execution-1".to_owned(),
            content: content.into(),
        }
    }

    fn handle() -> &'static str {
        "artifact://conversation-1/tool-output/execution-1"
    }

    fn append(offset: u64, content: impl Into<Vec<u8>>) -> ArtifactOperation {
        ArtifactOperation::AppendToolOutput {
            conversation_id: "conversation-1".to_owned(),
            handle: handle().to_owned(),
            offset,
            content: content.into(),
        }
    }

    fn read(offset: u64, limit: usize) -> ArtifactOperation {
        ArtifactOperation::ReadArtifact {
            conversation_id: "conversation-1".to_owned(),
            handle: handle().to_owned(),
            offset,
            limit,
        }
    }

    #[test]
    fn read_and_grep_reject_a_no_writer_fifo_without_blocking() {
        let root = TestRoot::new();
        let kind_dir = root.0.join("conversation-1/tool-output");
        fs::create_dir_all(&kind_dir).unwrap();
        let fifo = kind_dir.join("execution-1");
        let fifo = CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);

        let broker = ArtifactBroker::open(&root.0).unwrap();
        for operation in [
            read(0, 10),
            ArtifactOperation::GrepArtifact {
                conversation_id: "conversation-1".to_owned(),
                handle: handle().to_owned(),
                pattern: "needle".to_owned(),
            },
        ] {
            assert!(matches!(
                broker.execute(operation),
                Err(ToolError::InvalidPath(message))
                    if message == "artifact is not a regular file"
            ));
        }
    }

    #[test]
    fn root_with_a_symlinked_parent_component_is_rejected_without_touching_target() {
        use std::os::unix::fs::symlink;

        let container = TestRoot::new();
        let outside = container.0.join("outside");
        fs::create_dir(&outside).unwrap();
        let marker = outside.join("marker");
        fs::write(&marker, b"untouched").unwrap();
        let link = container.0.join("link");
        symlink(&outside, &link).unwrap();

        assert!(ArtifactBroker::open(&link).is_err());
        assert_eq!(fs::read(marker).unwrap(), b"untouched");
        assert!(fs::read_dir(outside).unwrap().count() == 1);
    }

    #[test]
    fn hostile_umasks_still_produce_private_root_directories_and_file() {
        let _guard = UMASK_LOCK.lock().unwrap();
        for mask in [0o000, 0o077] {
            let root = TestRoot::new();
            {
                let _umask = UmaskGuard::set(mask);
                let broker = ArtifactBroker::open(&root.0).unwrap();
                broker.execute(begin(b"prefix".to_vec())).unwrap();
            }

            assert_eq!(fs::metadata(&root.0).unwrap().mode() & 0o777, 0o700);
            assert_eq!(
                fs::metadata(root.0.join("conversation-1")).unwrap().mode() & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(root.0.join("conversation-1/tool-output"))
                    .unwrap()
                    .mode()
                    & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(root.0.join("conversation-1/tool-output/execution-1"))
                    .unwrap()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn begin_append_finish_have_deterministic_retry_and_offset_contracts() {
        let root = TestRoot::new();
        let broker = ArtifactBroker::open(&root.0).unwrap();
        let begun = ArtifactResponse::Begun {
            handle: handle().to_owned(),
            offset: 6,
        };
        assert_eq!(broker.execute(begin(b"prefix".to_vec())).unwrap(), begun);
        assert_eq!(broker.execute(begin(b"prefix".to_vec())).unwrap(), begun);
        assert!(matches!(
            broker.execute(begin(b"different".to_vec())),
            Err(ToolError::Protocol(message))
                if message == "duplicate BeginToolOutput has conflicting content"
        ));

        assert_eq!(
            broker.execute(append(6, b"-suffix".to_vec())).unwrap(),
            ArtifactResponse::Appended { offset: 13 }
        );
        assert_eq!(
            broker.execute(append(6, b"-suffix".to_vec())).unwrap(),
            ArtifactResponse::Appended { offset: 13 }
        );
        assert!(matches!(
            broker.execute(append(6, b"-different".to_vec())),
            Err(ToolError::Protocol(message))
                if message == "artifact append replay has conflicting content"
        ));
        assert_eq!(broker.execute(begin(b"prefix".to_vec())).unwrap(), begun);
        assert!(matches!(
            broker.execute(begin(b"different".to_vec())),
            Err(ToolError::Protocol(message))
                if message == "duplicate BeginToolOutput has conflicting content"
        ));
        assert_eq!(
            broker.execute(append(13, b"-tail".to_vec())).unwrap(),
            ArtifactResponse::Appended { offset: 18 }
        );
        assert!(matches!(
            broker.execute(append(6, b"-suffix".to_vec())),
            Err(ToolError::Protocol(message)) if message.contains("offset mismatch")
        ));
        assert!(matches!(
            broker.execute(append(12, b"x".to_vec())),
            Err(ToolError::Protocol(message)) if message.contains("offset mismatch")
        ));

        let finish = ArtifactOperation::FinishToolOutput {
            conversation_id: "conversation-1".to_owned(),
            handle: handle().to_owned(),
        };
        assert_eq!(
            broker.execute(finish.clone()).unwrap(),
            ArtifactResponse::Finished
        );
        assert_eq!(broker.execute(finish).unwrap(), ArtifactResponse::Finished);
        assert_eq!(broker.execute(begin(b"prefix".to_vec())).unwrap(), begun);
        assert!(matches!(
            broker.execute(append(13, b"late".to_vec())),
            Err(ToolError::Protocol(message)) if message.contains("finished artifact")
        ));
    }

    #[test]
    fn read_is_bounded_binary_safe_and_reports_utf8_split_bytes_exactly() {
        let root = TestRoot::new();
        let broker = ArtifactBroker::open(&root.0).unwrap();
        let content = [b"a".as_slice(), "界".as_bytes(), &[0, 0xff], b"z"].concat();
        broker.execute(begin(content.clone())).unwrap();

        assert_eq!(
            broker.execute(read(1, 4)).unwrap(),
            ArtifactResponse::Read {
                content: content[1..5].to_vec(),
                eof: false,
            }
        );
        assert_eq!(
            broker.execute(read(content.len() as u64, 50)).unwrap(),
            ArtifactResponse::Read {
                content: vec![],
                eof: true,
            }
        );
        assert!(matches!(
            broker.execute(read(u64::MAX, 1)),
            Err(ToolError::Protocol(message)) if message.contains("range overflow")
        ));
    }

    #[test]
    fn grep_is_utf8_strict_and_truncates_rendered_lines_at_five_hundred_chars() {
        let root = TestRoot::new();
        let broker = ArtifactBroker::open(&root.0).unwrap();
        let long = format!("needle{}\nother\nneedle-short\n", "界".repeat(600));
        broker.execute(begin(long.into_bytes())).unwrap();
        let result = broker
            .execute(ArtifactOperation::GrepArtifact {
                conversation_id: "conversation-1".to_owned(),
                handle: handle().to_owned(),
                pattern: "needle".to_owned(),
            })
            .unwrap();
        let ArtifactResponse::Grep { matches } = result else {
            panic!("unexpected grep response")
        };
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].line_number, 1);
        assert_eq!(matches[0].line.chars().count(), GREP_MAX_LINE_LENGTH);
        assert!(matches[0].line_truncated);
        assert_eq!(matches[1].line_number, 3);
        assert!(!matches[1].line_truncated);

        let binary_root = TestRoot::new();
        let binary = ArtifactBroker::open(&binary_root.0).unwrap();
        binary.execute(begin(vec![b'n', 0xff, b'\n'])).unwrap();
        assert!(matches!(
            binary.execute(ArtifactOperation::GrepArtifact {
                conversation_id: "conversation-1".to_owned(),
                handle: handle().to_owned(),
                pattern: "n".to_owned(),
            }),
            Err(ToolError::Protocol(message)) if message.contains("UTF-8")
        ));
    }

    #[test]
    fn grep_has_finite_scan_and_rendered_output_limits() {
        let root = TestRoot::new();
        let broker = ArtifactBroker::open(&root.0).unwrap();
        broker
            .execute(begin(vec![b'x'; (MAX_SCAN_BYTES + 1) as usize]))
            .unwrap();
        assert!(matches!(
            broker.execute(ArtifactOperation::GrepArtifact {
                conversation_id: "conversation-1".to_owned(),
                handle: handle().to_owned(),
                pattern: "x".to_owned(),
            }),
            Err(ToolError::ResourceLimit(ResourceLimit::ScanBytes))
        ));

        let output_root = TestRoot::new();
        let output_broker = ArtifactBroker::open(&output_root.0).unwrap();
        output_broker
            .execute(begin(
                ("match ".to_owned() + &"x".repeat(490) + "\n")
                    .repeat(200)
                    .into_bytes(),
            ))
            .unwrap();
        assert!(matches!(
            output_broker.execute(ArtifactOperation::GrepArtifact {
                conversation_id: "conversation-1".to_owned(),
                handle: handle().to_owned(),
                pattern: "match".to_owned(),
            }),
            Err(ToolError::ResourceLimit(ResourceLimit::ScanBytes))
        ));
    }

    #[test]
    fn grep_serialized_limit_includes_response_envelope_at_exact_boundary() {
        fn encoded_size(matches: &[ArtifactGrepMatch]) -> usize {
            serde_json::to_vec(&ArtifactResponse::Grep {
                matches: matches.to_vec(),
            })
            .unwrap()
            .len()
        }

        let full_line = format!("needle{}", "x".repeat(GREP_MAX_LINE_LENGTH - 6));
        let mut full_matches = Vec::new();
        loop {
            let line_number = full_matches.len() as u64 + 1;
            let mut candidate = full_matches.clone();
            candidate.push(ArtifactGrepMatch {
                line_number,
                line: full_line.clone(),
                line_truncated: false,
            });
            if encoded_size(&candidate) > MAX_GREP_SERIALIZED_BYTES {
                break;
            }
            full_matches = candidate;
        }

        let mut exact = None;
        for prefix_len in (0..=full_matches.len()).rev() {
            let prefix = &full_matches[..prefix_len];
            for suffix_len in 0..=(GREP_MAX_LINE_LENGTH - 6) {
                let mut candidate = prefix.to_vec();
                candidate.push(ArtifactGrepMatch {
                    line_number: prefix_len as u64 + 1,
                    line: format!("needle{}", "x".repeat(suffix_len)),
                    line_truncated: false,
                });
                if encoded_size(&candidate) == MAX_GREP_SERIALIZED_BYTES {
                    exact = Some((prefix_len, suffix_len));
                    break;
                }
            }
            if exact.is_some() {
                break;
            }
        }
        let (prefix_len, suffix_len) = exact.expect("construct exact grep boundary");

        let mut content = Vec::new();
        for _ in 0..prefix_len {
            content.extend_from_slice(full_line.as_bytes());
            content.push(b'\n');
        }
        content.extend_from_slice(format!("needle{}", "x".repeat(suffix_len)).as_bytes());
        content.push(b'\n');

        let root = TestRoot::new();
        let broker = ArtifactBroker::open(&root.0).unwrap();
        broker.execute(begin(content.clone())).unwrap();
        let operation = ArtifactOperation::GrepArtifact {
            conversation_id: "conversation-1".to_owned(),
            handle: handle().to_owned(),
            pattern: "needle".to_owned(),
        };
        let response = broker.execute(operation.clone()).unwrap();
        assert_eq!(
            serde_json::to_vec(&response).unwrap().len(),
            MAX_GREP_SERIALIZED_BYTES
        );

        let mut over_limit = content;
        over_limit.extend_from_slice(b"needle\n");
        let over_root = TestRoot::new();
        let over_broker = ArtifactBroker::open(&over_root.0).unwrap();
        over_broker.execute(begin(over_limit)).unwrap();
        assert!(matches!(
            over_broker.execute(operation),
            Err(ToolError::ResourceLimit(ResourceLimit::ScanBytes))
        ));
    }

    #[test]
    fn conversation_kind_and_file_symlinks_are_rejected() {
        let outside = TestRoot::new();

        let conversation_root = TestRoot::new();
        symlink(&outside.0, conversation_root.0.join("conversation-1")).unwrap();
        let broker = ArtifactBroker::open(&conversation_root.0).unwrap();
        assert!(broker.execute(begin(b"x".to_vec())).is_err());
        assert!(!outside.0.join("tool-output/execution-1").exists());

        let kind_root = TestRoot::new();
        fs::create_dir(kind_root.0.join("conversation-1")).unwrap();
        symlink(&outside.0, kind_root.0.join("conversation-1/tool-output")).unwrap();
        let broker = ArtifactBroker::open(&kind_root.0).unwrap();
        assert!(broker.execute(begin(b"x".to_vec())).is_err());
        assert!(!outside.0.join("execution-1").exists());

        let file_root = TestRoot::new();
        fs::create_dir_all(file_root.0.join("conversation-1/tool-output")).unwrap();
        let outside_file = outside.0.join("outside-file");
        fs::write(&outside_file, b"unchanged").unwrap();
        symlink(
            &outside_file,
            file_root.0.join("conversation-1/tool-output/execution-1"),
        )
        .unwrap();
        let broker = ArtifactBroker::open(&file_root.0).unwrap();
        assert!(broker.execute(begin(b"replacement".to_vec())).is_err());
        for operation in [
            read(0, 10),
            append(0, b"replacement".to_vec()),
            ArtifactOperation::FinishToolOutput {
                conversation_id: "conversation-1".to_owned(),
                handle: handle().to_owned(),
            },
            ArtifactOperation::GrepArtifact {
                conversation_id: "conversation-1".to_owned(),
                handle: handle().to_owned(),
                pattern: "unchanged".to_owned(),
            },
        ] {
            assert!(broker.execute(operation).is_err());
        }
        assert_eq!(fs::read(outside_file).unwrap(), b"unchanged");
    }

    #[test]
    fn claims_reject_cross_conversation_and_wrong_kind_mutation() {
        let root = TestRoot::new();
        let broker = ArtifactBroker::open(&root.0).unwrap();
        broker.execute(begin(b"content".to_vec())).unwrap();
        assert!(matches!(
            broker.execute(ArtifactOperation::ReadArtifact {
                conversation_id: "conversation-2".to_owned(),
                handle: handle().to_owned(),
                offset: 0,
                limit: 10,
            }),
            Err(ToolError::InvalidPath(message)) if message.contains("another conversation")
        ));
        assert!(matches!(
            broker.execute(ArtifactOperation::AppendToolOutput {
                conversation_id: "conversation-1".to_owned(),
                handle: "artifact://conversation-1/attachments/input-1".to_owned(),
                offset: 0,
                content: vec![],
            }),
            Err(ToolError::InvalidPath(message)) if message.contains("tool-output")
        ));
    }

    #[test]
    fn durable_boundaries_are_visible_through_an_independent_open() {
        let root = TestRoot::new();
        let broker = ArtifactBroker::open(&root.0).unwrap();
        broker.execute(begin(b"first".to_vec())).unwrap();
        let path = root.0.join("conversation-1/tool-output/execution-1");
        assert_eq!(fs::read(&path).unwrap(), b"first");
        broker.execute(append(5, b" second".to_vec())).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"first second");
        broker
            .execute(ArtifactOperation::FinishToolOutput {
                conversation_id: "conversation-1".to_owned(),
                handle: handle().to_owned(),
            })
            .unwrap();
        assert_eq!(fs::read(path).unwrap(), b"first second");
    }

    #[test]
    fn existing_permissions_are_repaired_when_descendants_are_opened() {
        let root = TestRoot::new();
        let broker = ArtifactBroker::open(&root.0).unwrap();
        broker.execute(begin(b"x".to_vec())).unwrap();
        let conversation = root.0.join("conversation-1");
        let kind = conversation.join("tool-output");
        let file = kind.join("execution-1");
        fs::set_permissions(&conversation, fs::Permissions::from_mode(0o777)).unwrap();
        fs::set_permissions(&kind, fs::Permissions::from_mode(0o777)).unwrap();
        fs::set_permissions(&file, fs::Permissions::from_mode(0o666)).unwrap();

        broker.execute(read(0, 1)).unwrap();
        assert_eq!(fs::metadata(conversation).unwrap().mode() & 0o777, 0o700);
        assert_eq!(fs::metadata(kind).unwrap().mode() & 0o777, 0o700);
        assert_eq!(fs::metadata(file).unwrap().mode() & 0o777, 0o600);
    }

    #[test]
    fn delete_conversation_artifacts_removes_whole_subtree_and_is_idempotent() {
        let root = TestRoot::new();
        let broker = ArtifactBroker::open(&root.0).unwrap();

        broker.execute(begin(b"one".to_vec())).unwrap();
        let other = ArtifactOperation::BeginToolOutput {
            conversation_id: "conversation-2".to_owned(),
            execution_id: "execution-1".to_owned(),
            content: b"two".to_vec(),
        };
        broker.execute(other).unwrap();

        let delete = ArtifactOperation::DeleteConversationArtifacts {
            old_conversation_id: "conversation-1".to_owned(),
            tombstone_id: "tombstone-1".to_owned(),
        };
        assert_eq!(
            broker.execute(delete.clone()).unwrap(),
            ArtifactResponse::Deleted
        );
        assert!(!root.0.join("conversation-1").exists());
        assert!(root.0.join("conversation-2").exists());

        assert_eq!(broker.execute(delete).unwrap(), ArtifactResponse::Deleted);
    }

    #[test]
    fn delete_conversation_artifacts_rejects_cross_conversation_tombstone_reuse() {
        let root = TestRoot::new();
        let broker = ArtifactBroker::open(&root.0).unwrap();
        broker.execute(begin(b"one".to_vec())).unwrap();
        broker
            .execute(ArtifactOperation::BeginToolOutput {
                conversation_id: "conversation-2".to_owned(),
                execution_id: "execution-1".to_owned(),
                content: b"two".to_vec(),
            })
            .unwrap();

        broker
            .execute(ArtifactOperation::DeleteConversationArtifacts {
                old_conversation_id: "conversation-1".to_owned(),
                tombstone_id: "tombstone-1".to_owned(),
            })
            .unwrap();
        assert!(matches!(
            broker.execute(ArtifactOperation::DeleteConversationArtifacts {
                old_conversation_id: "conversation-2".to_owned(),
                tombstone_id: "tombstone-1".to_owned(),
            }),
            Err(ToolError::Protocol(message)) if message.contains("different conversation")
        ));
        assert!(root.0.join("conversation-2").exists());
    }

    #[test]
    fn delete_conversation_artifacts_rejects_symlink_escape() {
        let outside = TestRoot::new();
        let marker = outside.0.join("marker");
        fs::write(&marker, b"untouched").unwrap();

        let root = TestRoot::new();
        fs::create_dir(root.0.join("conversation-1")).unwrap();
        symlink(
            outside.0.join("marker"),
            root.0.join("conversation-1/link-to-marker"),
        )
        .unwrap();

        let broker = ArtifactBroker::open(&root.0).unwrap();
        let delete = ArtifactOperation::DeleteConversationArtifacts {
            old_conversation_id: "conversation-1".to_owned(),
            tombstone_id: "tombstone-1".to_owned(),
        };
        assert_eq!(broker.execute(delete).unwrap(), ArtifactResponse::Deleted);
        assert!(
            marker.exists(),
            "symlink target outside the conversation subtree must not be followed"
        );
    }
}
