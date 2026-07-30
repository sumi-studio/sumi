//! Persistent supervisor-only process-generation allocation.
//!
//! This module is intentionally synchronous. The explicit supervisor CLI runs
//! it before a Tokio runtime, tracing subscriber, production bootstrap, or
//! sidecar service is constructed. Runtime consumers continue to parse the
//! resulting fixed environment vocabulary through `runtime::allocator`.

use std::{
    ffi::{CStr, CString, OsStr},
    fs::{File, Metadata, Permissions},
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, RawFd},
        unix::{
            ffi::OsStrExt,
            fs::{MetadataExt, PermissionsExt},
        },
    },
    path::{Component, Path, PathBuf},
    sync::{Mutex, OnceLock},
};

use anyhow::{Context, Result, anyhow, bail};
use rand::TryRngCore;
use serde::{Deserialize, Serialize};
use uuid::{Uuid, Variant, Version};

use super::contracts::{MAX_PROCESS_GENERATION, PersonalityAgentId, ProcessGeneration};

const STATE_DIR_MODE: u32 = 0o700;
const OUTPUT_DIR_MODE: u32 = 0o700;
const INTERNAL_DIR_MODE: u32 = 0o700;
const STATE_FILE_MODE: u32 = 0o600;
const IDENTITY_FILE_MODE: u32 = 0o400;
const LEDGER_FILE: &str = "allocator-ledger.json";
const LOCK_FILE: &str = "allocator.lock";
const MAX_CONTROL_FILE_BYTES: u64 = 16 * 1024;
const LEDGER_VERSION: u8 = 1;

static IN_PROCESS_ALLOCATOR_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Clone, Debug)]
pub(crate) struct SupervisorAllocatorConfig {
    pub(crate) personality_agent_id: PersonalityAgentId,
    pub(crate) state_dir: PathBuf,
    pub(crate) identity_output_root: PathBuf,
}

impl SupervisorAllocatorConfig {
    pub(crate) fn from_process_env() -> Result<Self> {
        let personality_agent_id = std::env::var("SUMI_PERSONALITY_AGENT_ID")
            .context("SUMI_PERSONALITY_AGENT_ID is required for supervisor allocation")
            .and_then(|value| {
                PersonalityAgentId::parse(&value)
                    .context("SUMI_PERSONALITY_AGENT_ID must be a canonical lowercase UUIDv7")
            })?;
        let state_dir = required_absolute_env_path("SUMI_ALLOCATOR_STATE_DIR")?;
        let identity_output_root = required_absolute_env_path("SUMI_IDENTITY_OUTPUT_ROOT")?;
        Ok(Self {
            personality_agent_id,
            state_dir,
            identity_output_root,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct AllocationMetadata {
    status: &'static str,
    generation: u64,
}

pub(crate) fn allocate(config: &SupervisorAllocatorConfig) -> Result<AllocationMetadata> {
    allocate_with_options(config, MAX_PROCESS_GENERATION, Failpoint::None)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Failpoint {
    None,
    AfterLedgerCommit,
    AfterRuntimeIdentity,
    AfterExecutorIdentity,
    AfterBrokerIdentity,
}

fn allocate_with_options(
    config: &SupervisorAllocatorConfig,
    max_generation: u64,
    failpoint: Failpoint,
) -> Result<AllocationMetadata> {
    ProcessGeneration::from_wire(max_generation)
        .context("allocator maximum is outside the process-generation domain")?;

    let process_guard = IN_PROCESS_ALLOCATOR_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| anyhow!("supervisor allocator in-process lock was poisoned"))?;

    let state_dir = open_trusted_absolute_dir(&config.state_dir, STATE_DIR_MODE)
        .context("SUMI_ALLOCATOR_STATE_DIR is not a trusted allocator directory")?;
    let output_root = open_trusted_absolute_dir(&config.identity_output_root, OUTPUT_DIR_MODE)
        .context("SUMI_IDENTITY_OUTPUT_ROOT is not a trusted identity directory")?;
    let lock_file =
        open_or_create_checked_file(&state_dir, LOCK_FILE, libc::O_RDWR, STATE_FILE_MODE)
            .context("allocator lock file is not trustworthy")?;
    let _file_guard = FlockGuard::exclusive(lock_file)?;
    let role_dirs = RoleIdentityDirs::open(&output_root)?;

    let allocation = FreshAllocation::new()?;
    let ledger = load_ledger(&state_dir)?;
    let generation = next_generation(
        ledger.as_ref(),
        &config.personality_agent_id,
        max_generation,
    )?;
    let next_ledger = LedgerWire {
        version: LEDGER_VERSION,
        personality_agent_id: config.personality_agent_id.as_str().to_owned(),
        state: if generation == max_generation {
            LedgerState::Exhausted
        } else {
            LedgerState::Next {
                generation: generation
                    .checked_add(1)
                    .expect("validated generation below maximum"),
            }
        },
    };
    atomic_replace_json(&state_dir, LEDGER_FILE, STATE_FILE_MODE, &next_ledger)
        .context("failed to durably advance the generation ledger")?;
    if failpoint == Failpoint::AfterLedgerCommit {
        bail!("injected failure after ledger commit");
    }

    materialize_role_identities(
        &role_dirs,
        &config.personality_agent_id,
        generation,
        &allocation,
        failpoint,
    )?;

    drop(process_guard);
    Ok(AllocationMetadata {
        status: "allocated",
        generation,
    })
}

fn required_absolute_env_path(name: &str) -> Result<PathBuf> {
    let value = std::env::var_os(name).with_context(|| format!("{name} is required"))?;
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        bail!("{name} must be an absolute path");
    }
    Ok(path)
}

#[derive(Debug)]
struct FreshAllocation {
    nonce: String,
    lease_id: String,
    fence_id: String,
}

impl FreshAllocation {
    fn new() -> Result<Self> {
        let mut nonce = [0_u8; 32];
        rand::rngs::OsRng
            .try_fill_bytes(&mut nonce)
            .map_err(|error| anyhow!("operating-system random source failed: {error}"))?;
        Ok(Self {
            nonce: hex(&nonce),
            lease_id: canonical_uuid_v7(Uuid::now_v7(), "lease UUID")?,
            fence_id: canonical_uuid_v7(Uuid::now_v7(), "recovery-fence UUID")?,
        })
    }
}

fn canonical_uuid_v7(uuid: Uuid, kind: &str) -> Result<String> {
    if uuid.get_version() != Some(Version::SortRand) || uuid.get_variant() != Variant::RFC4122 {
        bail!("{kind} is not an RFC UUIDv7");
    }
    Ok(uuid.hyphenated().to_string())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LedgerWire {
    version: u8,
    personality_agent_id: String,
    state: LedgerState,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum LedgerState {
    Next { generation: u64 },
    Exhausted,
}

fn load_ledger(state_dir: &File) -> Result<Option<LedgerWire>> {
    let Some(file) =
        open_optional_checked_file(state_dir, LEDGER_FILE, libc::O_RDONLY, STATE_FILE_MODE)?
    else {
        return Ok(None);
    };
    let bytes = read_bounded(file, LEDGER_FILE)?;
    let ledger: LedgerWire =
        serde_json::from_slice(&bytes).context("allocator ledger is corrupt or truncated")?;
    if ledger.version != LEDGER_VERSION {
        bail!("allocator ledger version is unsupported");
    }
    Ok(Some(ledger))
}

fn next_generation(
    ledger: Option<&LedgerWire>,
    expected_paid: &PersonalityAgentId,
    max_generation: u64,
) -> Result<u64> {
    let Some(ledger) = ledger else {
        return Ok(0);
    };
    let ledger_paid = PersonalityAgentId::parse(&ledger.personality_agent_id)
        .context("allocator ledger contains an invalid personality-agent binding")?;
    if &ledger_paid != expected_paid {
        bail!("allocator ledger is permanently bound to a different personality agent");
    }
    match ledger.state {
        LedgerState::Next { generation } => {
            ProcessGeneration::from_wire(generation)
                .context("allocator ledger generation is outside the supported domain")?;
            if generation > max_generation {
                bail!("allocator ledger exceeds the configured generation maximum");
            }
            Ok(generation)
        }
        LedgerState::Exhausted => bail!("process-generation allocator is exhausted"),
    }
}

struct RoleIdentityDirs {
    runtime: File,
    executor: File,
    broker: File,
}

impl RoleIdentityDirs {
    fn open(output_root: &File) -> Result<Self> {
        let runtime = open_checked_dir(output_root, "runtime", INTERNAL_DIR_MODE)
            .context("runtime identity output directory is not trustworthy")?;
        let executor = open_checked_dir(output_root, "executor", INTERNAL_DIR_MODE)
            .context("executor identity output directory is not trustworthy")?;
        let broker = open_checked_dir(output_root, "broker", INTERNAL_DIR_MODE)
            .context("broker identity output directory is not trustworthy")?;
        for (role, directory) in [
            ("runtime", &runtime),
            ("executor", &executor),
            ("broker", &broker),
        ] {
            if let Some(existing) = open_optional_checked_file(
                directory,
                "identity.env",
                libc::O_RDONLY,
                IDENTITY_FILE_MODE,
            )
            .with_context(|| format!("{role} identity target is not trustworthy"))?
            {
                drop(existing);
            }
        }
        Ok(Self {
            runtime,
            executor,
            broker,
        })
    }
}

fn materialize_role_identities(
    role_dirs: &RoleIdentityDirs,
    paid: &PersonalityAgentId,
    generation: u64,
    allocation: &FreshAllocation,
    failpoint: Failpoint,
) -> Result<()> {
    // The deployment supervisor has already stopped and joined every old role.
    // It starts no role unless this CLI exits successfully, so cross-volume
    // atomicity is neither available nor required. The ledger was committed
    // first: a crash below can leave mixed files, but retry skips that
    // generation and atomically replaces every role file before success.
    let common = format!(
        "SUMI_PERSONALITY_AGENT_ID={}\nSUMI_RPC_GENERATION={generation}\nSUMI_RPC_NONCE={}\n",
        paid.as_str(),
        allocation.nonce
    );
    let runtime_env = format!(
        "{common}SUMI_PROCESS_GENERATION_LEASE_ID={}\nSUMI_GENERATION_RECOVERY_FENCE_ID={}\n",
        allocation.lease_id, allocation.fence_id
    );
    atomic_replace_bytes(
        &role_dirs.runtime,
        "identity.env",
        IDENTITY_FILE_MODE,
        runtime_env.as_bytes(),
    )?;
    if failpoint == Failpoint::AfterRuntimeIdentity {
        bail!("injected failure after runtime identity commit");
    }
    atomic_replace_bytes(
        &role_dirs.executor,
        "identity.env",
        IDENTITY_FILE_MODE,
        common.as_bytes(),
    )?;
    if failpoint == Failpoint::AfterExecutorIdentity {
        bail!("injected failure after executor identity commit");
    }
    atomic_replace_bytes(
        &role_dirs.broker,
        "identity.env",
        IDENTITY_FILE_MODE,
        common.as_bytes(),
    )?;
    if failpoint == Failpoint::AfterBrokerIdentity {
        bail!("injected failure after broker identity commit");
    }
    Ok(())
}

fn atomic_replace_json<T: Serialize>(
    parent: &File,
    destination: &str,
    mode: u32,
    value: &T,
) -> Result<()> {
    let mut bytes = serde_json::to_vec(value).context("failed to serialize durable state")?;
    bytes.push(b'\n');
    atomic_replace_bytes(parent, destination, mode, &bytes)
}

fn atomic_replace_bytes(parent: &File, destination: &str, mode: u32, bytes: &[u8]) -> Result<()> {
    if let Some(existing) = open_optional_checked_file(parent, destination, libc::O_RDONLY, mode)? {
        drop(existing);
    }
    let temp_name = format!(".{destination}.tmp-{}", Uuid::now_v7().hyphenated());
    let mut temp = create_new_checked_file(parent, &temp_name, libc::O_WRONLY, mode)
        .with_context(|| format!("failed to create temporary {destination}"))?;
    temp.write_all(bytes)
        .with_context(|| format!("failed to write temporary {destination}"))?;
    temp.sync_all()
        .with_context(|| format!("failed to fsync temporary {destination}"))?;
    validate_file(&temp, mode, &format!("temporary {destination}"))?;
    rename_entry(parent, &temp_name, parent, destination)
        .with_context(|| format!("failed to atomically replace {destination}"))?;
    fsync_file(parent, "durable-state parent directory")?;
    Ok(())
}

fn open_trusted_absolute_dir(path: &Path, expected_mode: u32) -> Result<File> {
    if !path.is_absolute() {
        bail!("trusted directory path must be absolute");
    }
    let root_name = c"/";
    let root_fd = unsafe {
        libc::open(
            root_name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if root_fd < 0 {
        return Err(io::Error::last_os_error()).context("failed to open filesystem root");
    }
    let mut current = unsafe { File::from_raw_fd(root_fd) };
    let mut saw_normal = false;
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(name) => {
                saw_normal = true;
                current = openat(
                    current.as_raw_fd(),
                    name,
                    libc::O_RDONLY | libc::O_DIRECTORY,
                    0,
                )
                .with_context(|| {
                    format!(
                        "failed to open trusted directory component {}",
                        name.to_string_lossy()
                    )
                })?;
            }
            Component::ParentDir | Component::Prefix(_) => {
                bail!("trusted directory path contains a forbidden component")
            }
        }
    }
    if !saw_normal {
        bail!("filesystem root cannot be used as a trusted allocator directory");
    }
    validate_dir(&current, expected_mode, "trusted directory")?;
    Ok(current)
}

fn open_checked_dir(parent: &File, name: &str, mode: u32) -> Result<File> {
    let name = cstring(name)?;
    open_checked_dir_cstr(parent, &name, mode)
}

fn open_checked_dir_cstr(parent: &File, name: &CStr, mode: u32) -> Result<File> {
    let file = openat_cstr(
        parent.as_raw_fd(),
        name,
        libc::O_RDONLY | libc::O_DIRECTORY,
        0,
    )?;
    validate_dir(&file, mode, "internal directory")?;
    Ok(file)
}

fn open_or_create_checked_file(parent: &File, name: &str, access: i32, mode: u32) -> Result<File> {
    match create_new_checked_file(parent, name, access, mode) {
        Ok(file) => {
            fsync_file(parent, "new-file parent directory")?;
            Ok(file)
        }
        Err(error)
            if error
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.kind() == io::ErrorKind::AlreadyExists) =>
        {
            let file = openat(parent.as_raw_fd(), OsStr::new(name), access, 0)?;
            validate_file(&file, mode, name)?;
            Ok(file)
        }
        Err(error) => Err(error),
    }
}

fn create_new_checked_file(parent: &File, name: &str, access: i32, mode: u32) -> Result<File> {
    let file = openat(
        parent.as_raw_fd(),
        OsStr::new(name),
        access | libc::O_CREAT | libc::O_EXCL,
        mode,
    )?;
    set_exact_mode(&file, mode, name)?;
    validate_file(&file, mode, name)?;
    Ok(file)
}

fn open_optional_checked_file(
    parent: &File,
    name: &str,
    access: i32,
    mode: u32,
) -> Result<Option<File>> {
    match openat(parent.as_raw_fd(), OsStr::new(name), access, 0) {
        Ok(file) => {
            validate_file(&file, mode, name)?;
            Ok(Some(file))
        }
        Err(error)
            if error
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.kind() == io::ErrorKind::NotFound) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn openat(parent: RawFd, name: &OsStr, flags: i32, mode: u32) -> Result<File> {
    let name = CString::new(name.as_bytes()).context("path name contains NUL")?;
    openat_cstr(parent, &name, flags, mode)
}

fn openat_cstr(parent: RawFd, name: &CStr, flags: i32, mode: u32) -> Result<File> {
    let fd = unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            mode as libc::mode_t,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error()).context("openat failed");
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn rename_entry(
    old_parent: &File,
    old_name: &str,
    new_parent: &File,
    new_name: &str,
) -> Result<()> {
    let old_name = cstring(old_name)?;
    let new_name = cstring(new_name)?;
    let result = unsafe {
        libc::renameat(
            old_parent.as_raw_fd(),
            old_name.as_ptr(),
            new_parent.as_raw_fd(),
            new_name.as_ptr(),
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error()).context("renameat failed");
    }
    Ok(())
}

fn cstring(value: &str) -> Result<CString> {
    CString::new(value).context("path name contains NUL")
}

fn set_exact_mode(file: &File, mode: u32, kind: &str) -> Result<()> {
    file.set_permissions(Permissions::from_mode(mode))
        .with_context(|| format!("failed to set exact mode on {kind}"))
}

fn validate_dir(file: &File, expected_mode: u32, kind: &str) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect {kind}"))?;
    if !metadata.file_type().is_dir() {
        bail!("{kind} is not a directory");
    }
    validate_owner_and_mode(&metadata, expected_mode, kind)
}

fn validate_file(file: &File, expected_mode: u32, kind: &str) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect {kind}"))?;
    if !metadata.file_type().is_file() {
        bail!("{kind} is not a regular file");
    }
    if metadata.nlink() != 1 {
        bail!("{kind} must have exactly one hard link");
    }
    validate_owner_and_mode(&metadata, expected_mode, kind)
}

fn validate_owner_and_mode(metadata: &Metadata, expected_mode: u32, kind: &str) -> Result<()> {
    let effective_uid = unsafe { libc::geteuid() };
    if metadata.uid() != effective_uid {
        bail!("{kind} is not owned by the effective supervisor UID");
    }
    let observed_mode = metadata.mode() & 0o7777;
    if observed_mode != expected_mode {
        bail!("{kind} mode must be exactly {expected_mode:04o}");
    }
    Ok(())
}

fn read_bounded(mut file: File, kind: &str) -> Result<Vec<u8>> {
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect {kind}"))?;
    if metadata.len() > MAX_CONTROL_FILE_BYTES {
        bail!("{kind} exceeds the maximum control-file size");
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    Read::by_ref(&mut file)
        .take(MAX_CONTROL_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read {kind}"))?;
    if bytes.len() as u64 > MAX_CONTROL_FILE_BYTES {
        bail!("{kind} exceeds the maximum control-file size");
    }
    Ok(bytes)
}

fn fsync_file(file: &File, kind: &str) -> Result<()> {
    file.sync_all()
        .with_context(|| format!("failed to fsync {kind}"))
}

struct FlockGuard {
    file: File,
}

impl FlockGuard {
    fn exclusive(file: File) -> Result<Self> {
        // This synchronous, RAII-held flock cannot be dropped by async task
        // cancellation. Process exit also releases the kernel lock while the
        // separate lock inode remains stable across every ledger replacement.
        loop {
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0 {
                return Ok(Self { file });
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error).context("failed to acquire allocator file lock");
            }
        }
    }
}

impl Drop for FlockGuard {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs,
        os::unix::fs::{PermissionsExt, symlink},
    };

    use super::*;

    const PAID: &str = "0198f0f4-9b72-7000-8000-000000000001";
    const OTHER_PAID: &str = "0198f0f4-9b72-7000-8000-000000000002";

    struct Fixture {
        root: PathBuf,
        config: SupervisorAllocatorConfig,
    }

    impl Fixture {
        fn new(label: &str) -> Self {
            let root =
                std::env::temp_dir().join(format!("sumi-allocator-{label}-{}", Uuid::now_v7()));
            let state_dir = root.join("state");
            let identity_output_root = root.join("identities");
            fs::create_dir_all(&state_dir).unwrap();
            fs::create_dir_all(&identity_output_root).unwrap();
            for role in ["runtime", "executor", "broker"] {
                fs::create_dir(identity_output_root.join(role)).unwrap();
            }
            fs::set_permissions(&root, Permissions::from_mode(0o700)).unwrap();
            fs::set_permissions(&state_dir, Permissions::from_mode(0o700)).unwrap();
            fs::set_permissions(&identity_output_root, Permissions::from_mode(0o700)).unwrap();
            for role in ["runtime", "executor", "broker"] {
                fs::set_permissions(
                    identity_output_root.join(role),
                    Permissions::from_mode(0o700),
                )
                .unwrap();
            }
            Self {
                root,
                config: SupervisorAllocatorConfig {
                    personality_agent_id: PersonalityAgentId::parse(PAID).unwrap(),
                    state_dir,
                    identity_output_root,
                },
            }
        }

        fn role_generation(&self, role: &str) -> Option<u64> {
            let value = fs::read_to_string(
                self.config
                    .identity_output_root
                    .join(role)
                    .join("identity.env"),
            )
            .ok()?;
            value.lines().find_map(|line| {
                line.strip_prefix("SUMI_RPC_GENERATION=")
                    .and_then(|value| value.parse().ok())
            })
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn generation_zero_max_and_explicit_exhaustion_are_durable() {
        let fixture = Fixture::new("exhaustion");
        let first = allocate_with_options(&fixture.config, 1, Failpoint::None).unwrap();
        let second = allocate_with_options(&fixture.config, 1, Failpoint::None).unwrap();
        assert_eq!(first.generation, 0);
        assert_eq!(second.generation, 1);
        let ledger: LedgerWire =
            serde_json::from_slice(&fs::read(fixture.config.state_dir.join(LEDGER_FILE)).unwrap())
                .unwrap();
        assert!(matches!(ledger.state, LedgerState::Exhausted));
        assert!(allocate_with_options(&fixture.config, 1, Failpoint::None).is_err());
    }

    #[test]
    fn i64_max_is_issued_exactly_once_before_exhaustion() {
        let fixture = Fixture::new("i64-max");
        let ledger = LedgerWire {
            version: LEDGER_VERSION,
            personality_agent_id: PAID.to_owned(),
            state: LedgerState::Next {
                generation: MAX_PROCESS_GENERATION,
            },
        };
        let ledger_path = fixture.config.state_dir.join(LEDGER_FILE);
        fs::write(&ledger_path, serde_json::to_vec(&ledger).unwrap()).unwrap();
        fs::set_permissions(&ledger_path, Permissions::from_mode(STATE_FILE_MODE)).unwrap();

        assert_eq!(
            allocate(&fixture.config).unwrap().generation,
            MAX_PROCESS_GENERATION
        );
        let persisted: LedgerWire =
            serde_json::from_slice(&fs::read(&ledger_path).unwrap()).unwrap();
        assert!(matches!(persisted.state, LedgerState::Exhausted));
        assert!(allocate(&fixture.config).is_err());
    }

    #[test]
    fn crash_windows_fail_closed_and_recover_without_generation_reuse() {
        let fixture = Fixture::new("failpoints");
        assert!(
            allocate_with_options(
                &fixture.config,
                MAX_PROCESS_GENERATION,
                Failpoint::AfterLedgerCommit,
            )
            .is_err()
        );
        assert_eq!(fixture.role_generation("runtime"), None);
        assert_eq!(fixture.role_generation("executor"), None);
        assert_eq!(fixture.role_generation("broker"), None);
        assert_eq!(
            allocate_with_options(&fixture.config, MAX_PROCESS_GENERATION, Failpoint::None)
                .unwrap()
                .generation,
            1,
            "generation committed before the crash must never be reused"
        );

        assert!(
            allocate_with_options(
                &fixture.config,
                MAX_PROCESS_GENERATION,
                Failpoint::AfterRuntimeIdentity,
            )
            .is_err()
        );
        assert_eq!(fixture.role_generation("runtime"), Some(2));
        assert_eq!(fixture.role_generation("executor"), Some(1));
        assert_eq!(fixture.role_generation("broker"), Some(1));
        assert_eq!(
            allocate_with_options(&fixture.config, MAX_PROCESS_GENERATION, Failpoint::None)
                .unwrap()
                .generation,
            3
        );

        assert!(
            allocate_with_options(
                &fixture.config,
                MAX_PROCESS_GENERATION,
                Failpoint::AfterExecutorIdentity,
            )
            .is_err()
        );
        assert_eq!(fixture.role_generation("runtime"), Some(4));
        assert_eq!(fixture.role_generation("executor"), Some(4));
        assert_eq!(fixture.role_generation("broker"), Some(3));
        assert_eq!(
            allocate_with_options(&fixture.config, MAX_PROCESS_GENERATION, Failpoint::None)
                .unwrap()
                .generation,
            5
        );

        assert!(
            allocate_with_options(
                &fixture.config,
                MAX_PROCESS_GENERATION,
                Failpoint::AfterBrokerIdentity,
            )
            .is_err()
        );
        assert_eq!(fixture.role_generation("runtime"), Some(6));
        assert_eq!(fixture.role_generation("executor"), Some(6));
        assert_eq!(fixture.role_generation("broker"), Some(6));
        assert_eq!(
            allocate_with_options(&fixture.config, MAX_PROCESS_GENERATION, Failpoint::None)
                .unwrap()
                .generation,
            7
        );
    }

    #[test]
    fn ledger_binding_corruption_and_permissions_fail_closed() {
        let fixture = Fixture::new("ledger-validation");
        allocate(&fixture.config).unwrap();
        let ledger_path = fixture.config.state_dir.join(LEDGER_FILE);

        let mut ledger: serde_json::Value =
            serde_json::from_slice(&fs::read(&ledger_path).unwrap()).unwrap();
        ledger["personality_agent_id"] = serde_json::Value::String(OTHER_PAID.to_owned());
        fs::write(&ledger_path, serde_json::to_vec(&ledger).unwrap()).unwrap();
        assert!(allocate(&fixture.config).is_err());

        ledger["personality_agent_id"] = serde_json::Value::String(PAID.to_owned());
        fs::write(&ledger_path, b"{").unwrap();
        assert!(allocate(&fixture.config).is_err());

        fs::write(&ledger_path, serde_json::to_vec(&ledger).unwrap()).unwrap();
        fs::set_permissions(&ledger_path, Permissions::from_mode(0o644)).unwrap();
        assert!(allocate(&fixture.config).is_err());
    }

    #[test]
    fn symlink_and_hardlink_control_files_fail_closed() {
        let fixture = Fixture::new("links");
        let ledger_path = fixture.config.state_dir.join(LEDGER_FILE);
        symlink("/dev/null", &ledger_path).unwrap();
        assert!(allocate(&fixture.config).is_err());
        fs::remove_file(&ledger_path).unwrap();

        allocate(&fixture.config).unwrap();
        let hardlink = fixture.config.state_dir.join("ledger-hardlink");
        fs::hard_link(&ledger_path, &hardlink).unwrap();
        assert!(allocate(&fixture.config).is_err());
    }

    #[test]
    fn lock_inode_links_and_permissions_are_rejected_without_replacement() {
        let fixture = Fixture::new("lock-validation");
        let lock_path = fixture.config.state_dir.join(LOCK_FILE);
        symlink("/dev/null", &lock_path).unwrap();
        assert!(allocate(&fixture.config).is_err());
        fs::remove_file(&lock_path).unwrap();

        allocate(&fixture.config).unwrap();
        let original = fs::metadata(&lock_path).unwrap();
        let hardlink = fixture.root.join("allocator-lock-hardlink");
        fs::hard_link(&lock_path, &hardlink).unwrap();
        assert!(allocate(&fixture.config).is_err());
        assert_eq!(fs::metadata(&lock_path).unwrap().ino(), original.ino());
        fs::remove_file(&hardlink).unwrap();

        fs::set_permissions(&lock_path, Permissions::from_mode(0o644)).unwrap();
        assert!(allocate(&fixture.config).is_err());
        assert_eq!(fs::metadata(&lock_path).unwrap().ino(), original.ino());
    }

    #[test]
    fn linked_or_overpermissive_role_targets_fail_closed_before_consuming_generation() {
        let fixture = Fixture::new("role-targets");
        let runtime_identity = fixture
            .config
            .identity_output_root
            .join("runtime")
            .join("identity.env");
        symlink("/dev/null", &runtime_identity).unwrap();
        assert!(allocate(&fixture.config).is_err());
        fs::remove_file(&runtime_identity).unwrap();

        assert_eq!(allocate(&fixture.config).unwrap().generation, 0);
        let hardlink = fixture.root.join("runtime-identity-hardlink");
        fs::hard_link(&runtime_identity, &hardlink).unwrap();
        assert!(allocate(&fixture.config).is_err());
        fs::remove_file(&hardlink).unwrap();

        fs::set_permissions(&runtime_identity, Permissions::from_mode(0o600)).unwrap();
        assert!(allocate(&fixture.config).is_err());
    }

    #[test]
    fn runtime_identity_is_accepted_by_the_authoritative_consumer_parser() {
        let fixture = Fixture::new("consumer-parser");
        allocate(&fixture.config).unwrap();
        let identity = fs::read_to_string(
            fixture
                .config
                .identity_output_root
                .join("runtime")
                .join("identity.env"),
        )
        .unwrap();
        let values: BTreeMap<_, _> = identity
            .lines()
            .map(|line| line.split_once('=').unwrap())
            .collect();
        let parsed = crate::runtime::allocator::SupervisorAllocation::from_wire(
            values["SUMI_PERSONALITY_AGENT_ID"],
            values["SUMI_RPC_GENERATION"],
            values["SUMI_RPC_NONCE"].to_owned(),
            values["SUMI_PROCESS_GENERATION_LEASE_ID"].to_owned(),
            values["SUMI_GENERATION_RECOVERY_FENCE_ID"].to_owned(),
        )
        .unwrap();
        assert_eq!(parsed.authority().generation().as_u64(), 0);
    }
}
