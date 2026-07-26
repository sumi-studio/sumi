//! T26 per-agent durable runtime-state publisher.
//!
//! Publishes `{generation, hydration_receipt_identity}` to one atomically-replaced file
//! per agent under `$SUMI_AGENT_RUNTIME_STATE_DIR` so the T28 API boundary can
//! observe the current generation-bound hydration latch without relying on an
//! in-process watch channel.
//!
//! The publisher is fail-closed: it validates the state directory, refuses path
//! traversal and symlinks, writes a temporary file with mode `0600`, fsyncs it,
//! performs an atomic rename, fsyncs the parent directory, and uses a per-agent
//! advisory lock plus a durable generation CAS so a stale process cannot
//! publish `Ready` over a newer generation.

use std::{
    fs::{self, File, OpenOptions, Permissions},
    io::{BufReader, Read, Write},
    os::fd::AsRawFd,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::contracts::{HydrationReceiptIdentity, ProcessGeneration};

const RUNTIME_STATE_FILE_PREFIX: &str = "runtime-";
const RUNTIME_STATE_FILE_SUFFIX: &str = ".json";
const RUNTIME_LOCK_SUFFIX: &str = ".lock";
const RUNTIME_TEMP_SUFFIX: &str = ".tmp";

/// URL-safe base64 alphabet without padding (Go `base64.RawURLEncoding`).
const BASE64URL_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Durable runtime state read by the T28 API boundary.
///
/// Fields are ordered `generation` then `hydration_receipt_identity` so the JSON serialization
/// matches the Go contract in `apps/api/internal/agentevents/runtime.go`.
/// `deny_unknown_fields` matches the strict decoding used by the Go side.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeState {
    pub generation: u64,
    pub hydration_receipt_identity: Option<String>,
}

/// Publishes one per-agent runtime-state file for the current process
/// generation. All writes are atomic, durable, and fail-closed.
pub struct RuntimeStatePublisher {
    state_dir: PathBuf,
    file_id: String,
    generation: ProcessGeneration,
}

impl RuntimeStatePublisher {
    /// Create a publisher bound to `state_dir`, `agent_id`, and `generation`.
    ///
    /// The state directory must be absolute. It is created (mode `0700`) if it
    /// does not exist, and rejected if it is a symlink or not a directory.
    pub fn new(
        state_dir: impl AsRef<Path>,
        agent_id: impl Into<String>,
        generation: ProcessGeneration,
    ) -> Result<Self> {
        let state_dir = prepare_runtime_state_dir(state_dir.as_ref())?;
        let agent_id = agent_id.into();
        if agent_id.is_empty() {
            bail!("runtime state publisher requires a non-empty agent_id");
        }
        let file_id = base64url_encode(agent_id.as_bytes());
        let publisher = Self {
            state_dir,
            file_id,
            generation,
        };
        publisher.validate_target_path()?;
        Ok(publisher)
    }

    /// The absolute path of the per-agent runtime state file.
    pub fn file_path(&self) -> PathBuf {
        self.state_dir.join(format!(
            "{RUNTIME_STATE_FILE_PREFIX}{}{RUNTIME_STATE_FILE_SUFFIX}",
            self.file_id
        ))
    }

    fn lock_path(&self) -> PathBuf {
        self.state_dir.join(format!(
            "{RUNTIME_STATE_FILE_PREFIX}{}{RUNTIME_LOCK_SUFFIX}",
            self.file_id
        ))
    }

    fn temp_path(&self) -> PathBuf {
        self.state_dir.join(format!(
            "{RUNTIME_STATE_FILE_PREFIX}{}.{}{RUNTIME_TEMP_SUFFIX}",
            self.file_id,
            Uuid::now_v7()
        ))
    }

    fn validate_target_path(&self) -> Result<()> {
        let file_path = self.file_path();
        let Some(parent) = file_path.parent() else {
            bail!("runtime state file path has no parent");
        };
        if parent != self.state_dir {
            bail!("runtime state file path escapes the state directory");
        }
        let Some(file_name) = file_path.file_name() else {
            bail!("runtime state file path has no file name");
        };
        let expected = format!(
            "{RUNTIME_STATE_FILE_PREFIX}{}{RUNTIME_STATE_FILE_SUFFIX}",
            self.file_id
        );
        if file_name.as_encoded_bytes() != expected.as_bytes() {
            bail!("runtime state file name was reconstructed incorrectly");
        }
        Ok(())
    }

    /// Atomically publish the generation-bound `NotReady` state.
    ///
    /// Overwrites any state for an older generation. Fails closed if a newer
    /// generation is already present or if the current generation is already
    /// `Ready`.
    pub fn publish_not_ready(&self) -> Result<()> {
        self.publish(None)
    }

    /// Atomically publish `Ready` with the stable T17 hydration receipt identity
    /// for the same generation that was previously published as `NotReady`.
    ///
    /// Fails closed if the current file is missing, belongs to a different
    /// generation, or contains a different ready receipt. Repeating the same
    /// ready receipt is idempotent. A stale old process therefore cannot
    /// overwrite a newer generation's `not_ready` with `ready`.
    pub fn publish_ready(&self, identity: &HydrationReceiptIdentity) -> Result<()> {
        self.publish(Some(identity.as_str()))
    }

    fn publish(&self, receipt_identity: Option<&str>) -> Result<()> {
        let generation = self.generation.as_u64();
        let payload = serde_json::to_vec(&RuntimeState {
            generation,
            hydration_receipt_identity: receipt_identity.map(ToOwned::to_owned),
        })
        .context("failed to serialize runtime state")?;

        let temp_path = self.temp_path();
        write_temp_file(&temp_path, &payload)?;

        let lock_file = open_lock_file(&self.lock_path())?;
        lock_exclusive(lock_file.as_raw_fd()).context("failed to lock runtime state file")?;

        // Hold the lock across read, CAS, rename, and directory fsync. If any
        // step fails, remove the temporary file before returning.
        let publish_result = self.publish_locked(receipt_identity, generation, &temp_path);

        // A successful rename removes the temporary pathname. Idempotent
        // publication does not rename, so clean up on every return path.
        let _ = fs::remove_file(&temp_path);

        publish_result
    }

    fn publish_locked(
        &self,
        receipt_identity: Option<&str>,
        generation: u64,
        temp_path: &Path,
    ) -> Result<()> {
        let current = read_current_state(&self.file_path())?;
        let proceed = match &current {
            None => {
                if receipt_identity.is_some() {
                    bail!("runtime state is missing; not-ready must be published before ready");
                }
                true
            }
            Some(state) => {
                if state.generation > generation {
                    bail!(
                        "stale generation: current file has generation {}, refusing {}",
                        state.generation,
                        generation
                    );
                }
                if state.generation == generation {
                    if state.hydration_receipt_identity.as_deref() == receipt_identity {
                        // Idempotent; no durable change required.
                        return Ok(());
                    }
                    if receipt_identity.is_none() && state.hydration_receipt_identity.is_some() {
                        bail!("cannot downgrade ready state for generation {}", generation);
                    }
                    if let (Some(current), Some(next)) = (
                        state.hydration_receipt_identity.as_deref(),
                        receipt_identity,
                    ) {
                        bail!(
                            "cannot replace hydration receipt identity {current:?} with {next:?} for generation {generation}"
                        );
                    }
                    // The remaining case is Some(receipt) over None for the
                    // current generation, which is the exact latch transition.
                    true
                } else {
                    // Older generation in file; allow newer generation to overwrite.
                    if receipt_identity.is_some() {
                        bail!(
                            "runtime state generation {} is older than {}; not-ready must be published first",
                            state.generation,
                            generation
                        );
                    }
                    true
                }
            }
        };

        if proceed {
            fs::rename(temp_path, self.file_path()).with_context(|| {
                format!(
                    "failed to commit runtime state to {}",
                    self.file_path().display()
                )
            })?;
            sync_dir(&self.state_dir).context("failed to fsync runtime state directory")?;
        }

        Ok(())
    }
}

fn prepare_runtime_state_dir(dir: &Path) -> Result<PathBuf> {
    if !dir.is_absolute() {
        bail!(
            "runtime state directory must be absolute: {}",
            dir.display()
        );
    }

    fs::create_dir_all(dir)
        .with_context(|| format!("failed to create runtime state directory {}", dir.display()))?;

    let meta = fs::symlink_metadata(dir).with_context(|| {
        format!(
            "failed to inspect runtime state directory {}",
            dir.display()
        )
    })?;
    if meta.file_type().is_symlink() {
        bail!(
            "runtime state directory must not be a symlink: {}",
            dir.display()
        );
    }
    if !meta.is_dir() {
        bail!(
            "runtime state directory must be a directory: {}",
            dir.display()
        );
    }

    fs::set_permissions(dir, Permissions::from_mode(0o700)).with_context(|| {
        format!(
            "failed to set permissions on runtime state directory {}",
            dir.display()
        )
    })?;

    fs::canonicalize(dir).with_context(|| {
        format!(
            "failed to canonicalize runtime state directory {}",
            dir.display()
        )
    })
}

fn write_temp_file(path: &Path, payload: &[u8]) -> Result<()> {
    let mut temp = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| {
            format!(
                "failed to open temporary runtime state file {}",
                path.display()
            )
        })?;

    temp.set_permissions(Permissions::from_mode(0o600))
        .with_context(|| {
            format!(
                "failed to set permissions on temporary runtime state file {}",
                path.display()
            )
        })?;

    temp.write_all(payload).with_context(|| {
        format!(
            "failed to write temporary runtime state file {}",
            path.display()
        )
    })?;

    temp.sync_all().with_context(|| {
        format!(
            "failed to fsync temporary runtime state file {}",
            path.display()
        )
    })?;

    Ok(())
}

fn open_lock_file(path: &Path) -> Result<File> {
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .with_context(|| format!("failed to open runtime state lock file {}", path.display()))?;

    lock.set_permissions(Permissions::from_mode(0o600))
        .with_context(|| format!("failed to set permissions on lock file {}", path.display()))?;

    Ok(lock)
}

fn read_current_state(path: &Path) -> Result<Option<RuntimeState>> {
    // Lstat the path before opening so a symlink is detected before it can be
    // followed.
    let meta = match fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e)
                .with_context(|| format!("failed to stat runtime state file {}", path.display()));
        }
    };
    if meta.file_type().is_symlink() {
        bail!(
            "runtime state file must not be a symlink: {}",
            path.display()
        );
    }
    if !meta.is_file() {
        bail!(
            "runtime state path is not a regular file: {}",
            path.display()
        );
    }

    let file = File::open(path)
        .with_context(|| format!("failed to open runtime state file {}", path.display()))?;

    let mut reader = BufReader::new(file);
    let mut contents = String::new();
    reader
        .read_to_string(&mut contents)
        .with_context(|| format!("failed to read runtime state file {}", path.display()))?;
    if contents.trim().is_empty() {
        bail!("runtime state file {} is empty", path.display());
    }

    let state: RuntimeState = serde_json::from_str(contents.trim()).with_context(|| {
        format!(
            "runtime state file {} contains invalid JSON",
            path.display()
        )
    })?;

    Ok(Some(state))
}

fn lock_exclusive(fd: std::os::fd::RawFd) -> Result<()> {
    let rc = unsafe { libc::flock(fd, libc::LOCK_EX) };
    if rc != 0 {
        bail!("flock(LOCK_EX) failed: {}", std::io::Error::last_os_error());
    }
    Ok(())
}

fn sync_dir(dir: &Path) -> Result<()> {
    let file = File::open(dir)
        .with_context(|| format!("failed to open directory {} for fsync", dir.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to fsync directory {}", dir.display()))?;
    Ok(())
}

fn base64url_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;

        out.push(BASE64URL_ALPHABET[((n >> 18) & 0x3f) as usize] as char);
        out.push(BASE64URL_ALPHABET[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(BASE64URL_ALPHABET[((n >> 6) & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(BASE64URL_ALPHABET[(n & 0x3f) as usize] as char);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("sumi-runtime-state-{}", Uuid::now_v7()))
    }

    fn publisher(dir: &Path, agent_id: &str, generation: u64) -> RuntimeStatePublisher {
        RuntimeStatePublisher::new(
            dir,
            agent_id,
            ProcessGeneration::from_wire(generation).unwrap(),
        )
        .unwrap()
    }

    fn read_file(path: &Path) -> RuntimeState {
        let raw = fs::read_to_string(path).unwrap();
        serde_json::from_str(&raw).unwrap()
    }

    fn receipt(value: &str) -> HydrationReceiptIdentity {
        HydrationReceiptIdentity::new(value.to_owned()).unwrap()
    }

    #[test]
    fn publishes_not_ready_then_ready_for_same_generation() {
        let dir = temp_dir();
        let p = publisher(&dir, "agent-1", 7);

        p.publish_not_ready().unwrap();

        let path = p.file_path();
        let state = read_file(&path);
        assert_eq!(state.generation, 7);
        assert_eq!(state.hydration_receipt_identity, None);

        p.publish_ready(&receipt("receipt-7")).unwrap();
        let state = read_file(&path);
        assert_eq!(state.generation, 7);
        assert_eq!(
            state.hydration_receipt_identity.as_deref(),
            Some("receipt-7")
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_name_is_url_safe_base64_without_padding() {
        let dir = temp_dir();
        let p = publisher(&dir, "agent-1", 1);
        let path = p.file_path();
        let file_name = path.file_name().unwrap().to_string_lossy();
        assert_eq!(file_name, "runtime-YWdlbnQtMQ.json");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_mode_is_0600() {
        let dir = temp_dir();
        let p = publisher(&dir, "agent-1", 1);
        p.publish_not_ready().unwrap();

        let meta = fs::metadata(p.file_path()).unwrap();
        assert_eq!(meta.mode() & 0o777, 0o600);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_publishing_ready_without_prior_not_ready() {
        let dir = temp_dir();
        let p = publisher(&dir, "agent-1", 5);
        assert!(p.publish_ready(&receipt("receipt-5")).is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_downgrading_ready_to_not_ready() {
        let dir = temp_dir();
        let p = publisher(&dir, "agent-1", 5);
        p.publish_not_ready().unwrap();
        p.publish_ready(&receipt("receipt-5")).unwrap();
        assert!(p.publish_not_ready().is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_replacing_receipt_identity_for_same_generation() {
        let dir = temp_dir();
        let p = publisher(&dir, "agent-1", 5);
        p.publish_not_ready().unwrap();
        p.publish_ready(&receipt("receipt-5-a")).unwrap();
        assert!(p.publish_ready(&receipt("receipt-5-b")).is_err());
        let state = read_file(&p.file_path());
        assert_eq!(
            state.hydration_receipt_identity.as_deref(),
            Some("receipt-5-a")
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn idempotent_publication_does_not_leave_temporary_files() {
        let dir = temp_dir();
        let p = publisher(&dir, "agent-1", 5);
        p.publish_not_ready().unwrap();
        p.publish_not_ready().unwrap();
        p.publish_ready(&receipt("receipt-5")).unwrap();
        p.publish_ready(&receipt("receipt-5")).unwrap();

        let temporary_files = fs::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
            .count();
        assert_eq!(temporary_files, 0);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_stale_generation_ready() {
        let dir = temp_dir();
        let p1 = publisher(&dir, "agent-1", 1);
        p1.publish_not_ready().unwrap();
        p1.publish_ready(&receipt("receipt-1")).unwrap();

        // Simulate a newer generation having been published.
        let p2 = publisher(&dir, "agent-1", 2);
        p2.publish_not_ready().unwrap();

        // The old generation publisher must not be able to (re-)publish ready.
        assert!(p1.publish_ready(&receipt("receipt-1")).is_err());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn replaces_old_ready_with_new_not_ready_on_rollover() {
        let dir = temp_dir();
        let p1 = publisher(&dir, "agent-1", 1);
        p1.publish_not_ready().unwrap();
        p1.publish_ready(&receipt("receipt-1")).unwrap();

        let p2 = publisher(&dir, "agent-1", 2);
        p2.publish_not_ready().unwrap();

        let state = read_file(&p2.file_path());
        assert_eq!(state.generation, 2);
        assert_eq!(state.hydration_receipt_identity, None);

        p2.publish_ready(&receipt("receipt-2")).unwrap();
        let state = read_file(&p2.file_path());
        assert_eq!(state.generation, 2);
        assert_eq!(
            state.hydration_receipt_identity.as_deref(),
            Some("receipt-2")
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_symlinked_state_directory() {
        let link_parent = temp_dir();
        fs::create_dir_all(&link_parent).unwrap();
        let real = link_parent.join("real");
        let link = link_parent.join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let result =
            RuntimeStatePublisher::new(&link, "agent-1", ProcessGeneration::from_wire(1).unwrap());
        assert!(result.is_err());

        let _ = fs::remove_file(&link);
        let _ = fs::remove_dir_all(&link_parent);
    }

    #[test]
    fn rejects_non_directory_state_path() {
        let parent = temp_dir();
        fs::create_dir_all(&parent).unwrap();
        let file = parent.join("not-a-dir");
        fs::write(&file, b"").unwrap();

        let result =
            RuntimeStatePublisher::new(&file, "agent-1", ProcessGeneration::from_wire(1).unwrap());
        assert!(result.is_err());

        let _ = fs::remove_file(&file);
        let _ = fs::remove_dir_all(&parent);
    }

    #[test]
    fn rejects_empty_agent_id() {
        let dir = temp_dir();
        let result = RuntimeStatePublisher::new(&dir, "", ProcessGeneration::from_wire(1).unwrap());
        assert!(result.is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn base64url_encode_matches_reference() {
        // Reference values from Go base64.RawURLEncoding.
        assert_eq!(base64url_encode(b"agent-1"), "YWdlbnQtMQ");
        assert_eq!(base64url_encode(b"a"), "YQ");
        assert_eq!(base64_url_encode_for_test(b"ab"), "YWI");
        assert_eq!(base64_url_encode_for_test(b"abc"), "YWJj");
        assert_eq!(base64url_encode(b"<>?"), "PD4_");
    }

    fn base64_url_encode_for_test(input: &[u8]) -> String {
        base64url_encode(input)
    }
}
