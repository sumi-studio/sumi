//! Persistent supervisor-only process-generation allocation.
//!
//! This module is intentionally synchronous. The explicit supervisor CLI runs
//! it before a Tokio runtime, tracing subscriber, production bootstrap, or
//! sidecar service is constructed. Runtime consumers continue to parse the
//! resulting fixed environment vocabulary through `runtime::allocator`.

use std::{
    collections::BTreeSet,
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
const ROLE_DIR_HANDOFF_MODE: u32 = 0o550;
const ROLE_DIR_WRITING_MODE: u32 = 0o750;
const STATE_FILE_MODE: u32 = 0o600;
const IDENTITY_TEMP_MODE: u32 = 0o400;
const IDENTITY_FILE_MODE: u32 = 0o440;
const LEDGER_FILE: &str = "allocator-ledger.json";
const LOCK_FILE: &str = "allocator.lock";
const OUTPUT_BINDING_FILE: &str = "allocator-binding.json";
const MAX_CONTROL_FILE_BYTES: u64 = 16 * 1024;
const LEDGER_VERSION: u8 = 1;
const OUTPUT_BINDING_VERSION: u8 = 1;
const TEST_CRASH_ENV: &str = "SUMI_ALLOCATOR_TEST_CRASH_AT";

static IN_PROCESS_ALLOCATOR_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Clone, Debug)]
pub(crate) struct SupervisorAllocatorConfig {
    pub(crate) personality_agent_id: PersonalityAgentId,
    trust_root: PathBuf,
    pub(crate) state_dir: PathBuf,
    pub(crate) identity_output_root: PathBuf,
    role_gids: RoleGids,
    crash_failpoint: Option<CrashFailpoint>,
}

impl SupervisorAllocatorConfig {
    pub(crate) fn from_process_env() -> Result<Self> {
        let personality_agent_id = std::env::var("SUMI_PERSONALITY_AGENT_ID")
            .context("SUMI_PERSONALITY_AGENT_ID is required for supervisor allocation")
            .and_then(|value| {
                PersonalityAgentId::parse(&value)
                    .context("SUMI_PERSONALITY_AGENT_ID must be a canonical lowercase UUIDv7")
            })?;
        let trust_root = required_absolute_env_path("SUMI_ALLOCATOR_TRUST_ROOT")?;
        let state_dir = required_absolute_env_path("SUMI_ALLOCATOR_STATE_DIR")?;
        let identity_output_root = required_absolute_env_path("SUMI_IDENTITY_OUTPUT_ROOT")?;
        let role_gids = RoleGids::from_process_env()?;
        let crash_failpoint = match std::env::var(TEST_CRASH_ENV) {
            Ok(value) if cfg!(debug_assertions) => Some(CrashFailpoint::parse(&value)?),
            Ok(_) => bail!("{TEST_CRASH_ENV} is unavailable in release builds"),
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(_)) => {
                bail!("{TEST_CRASH_ENV} must be valid UTF-8")
            }
        };
        if !is_strict_descendant(&state_dir, &trust_root)
            || !is_strict_descendant(&identity_output_root, &trust_root)
        {
            bail!(
                "allocator state and identity output roots must be strict descendants of SUMI_ALLOCATOR_TRUST_ROOT"
            );
        }
        if paths_overlap(&state_dir, &identity_output_root) {
            bail!("allocator state and identity output roots must not overlap");
        }
        Ok(Self {
            personality_agent_id,
            trust_root,
            state_dir,
            identity_output_root,
            role_gids,
            crash_failpoint,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct RoleGids {
    runtime: libc::gid_t,
    executor: libc::gid_t,
    broker: libc::gid_t,
}

impl RoleGids {
    fn from_process_env() -> Result<Self> {
        let role_gids = Self {
            runtime: required_gid("SUMI_RUNTIME_IDENTITY_GID")?,
            executor: required_gid("SUMI_EXECUTOR_IDENTITY_GID")?,
            broker: required_gid("SUMI_BROKER_IDENTITY_GID")?,
        };
        role_gids.validate()?;
        Ok(role_gids)
    }

    fn validate(self) -> Result<()> {
        let real_gid = unsafe { libc::getgid() };
        let effective_gid = unsafe { libc::getegid() };
        let values = [self.runtime, self.executor, self.broker];
        if values.contains(&0) || values.contains(&libc::gid_t::MAX) {
            bail!("role identity GIDs must not use a reserved GID");
        }
        if values
            .iter()
            .any(|gid| *gid == real_gid || *gid == effective_gid)
        {
            bail!("role identity GIDs must not collide with the allocator primary GID");
        }
        if values.into_iter().collect::<BTreeSet<_>>().len() != values.len() {
            bail!("runtime, executor, and broker identity GIDs must be distinct");
        }
        if unsafe { libc::geteuid() } != 0 {
            let supplementary = supplementary_groups()?;
            if values.iter().any(|gid| !supplementary.contains(gid)) {
                bail!("non-root allocator must belong to every configured supplemental role group");
            }
        }
        Ok(())
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DurableTarget {
    OutputBinding,
    Ledger,
    Runtime,
    Executor,
    Broker,
}

impl DurableTarget {
    const fn name(self) -> &'static str {
        match self {
            Self::OutputBinding => "output_binding",
            Self::Ledger => "ledger",
            Self::Runtime => "runtime",
            Self::Executor => "executor",
            Self::Broker => "broker",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DurableStage {
    PartialWrite,
    FileFsync,
    Rename,
    ParentFsync,
}

impl DurableStage {
    const fn name(self) -> &'static str {
        match self {
            Self::PartialWrite => "partial_write",
            Self::FileFsync => "file_fsync",
            Self::Rename => "rename",
            Self::ParentFsync => "parent_fsync",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CrashFailpoint {
    target: DurableTarget,
    stage: DurableStage,
}

impl CrashFailpoint {
    fn parse(value: &str) -> Result<Self> {
        for target in [
            DurableTarget::OutputBinding,
            DurableTarget::Ledger,
            DurableTarget::Runtime,
            DurableTarget::Executor,
            DurableTarget::Broker,
        ] {
            for stage in [
                DurableStage::PartialWrite,
                DurableStage::FileFsync,
                DurableStage::Rename,
                DurableStage::ParentFsync,
            ] {
                if value == format!("{}.{}", target.name(), stage.name()) {
                    return Ok(Self { target, stage });
                }
            }
        }
        bail!("{TEST_CRASH_ENV} is not a recognized durable-write failpoint");
    }

    fn trigger(self, target: DurableTarget, stage: DurableStage) {
        if self.target == target && self.stage == stage {
            unsafe {
                libc::kill(libc::getpid(), libc::SIGKILL);
            }
            std::process::abort();
        }
    }
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

    let allocator_gid = unsafe { libc::getegid() };
    let trust_root = open_absolute_trust_root(&config.trust_root, STATE_DIR_MODE, allocator_gid)
        .context("SUMI_ALLOCATOR_TRUST_ROOT is not a trusted allocator root")?;
    let state_dir = open_trusted_descendant(
        &trust_root,
        &config.trust_root,
        &config.state_dir,
        STATE_DIR_MODE,
        allocator_gid,
    )
    .context("SUMI_ALLOCATOR_STATE_DIR is not a trusted allocator directory")?;
    let output_root = open_trusted_descendant(
        &trust_root,
        &config.trust_root,
        &config.identity_output_root,
        OUTPUT_DIR_MODE,
        allocator_gid,
    )
    .context("SUMI_IDENTITY_OUTPUT_ROOT is not a trusted identity directory")?;
    if DirectoryIdentity::from_file(&state_dir, "allocator state")?
        == DirectoryIdentity::from_file(&output_root, "identity output root")?
    {
        bail!("allocator state and identity output roots must not alias");
    }
    let lock_file =
        open_or_create_checked_file(&state_dir, LOCK_FILE, libc::O_RDWR, STATE_FILE_MODE)
            .context("allocator lock file is not trustworthy")?;
    let _file_guard = FlockGuard::exclusive(lock_file)?;
    let role_dirs = RoleIdentityDirs::open(&output_root, config.role_gids)?;
    let observed_bindings = role_dirs.bindings(&trust_root, &state_dir, &output_root)?;
    cleanup_stale_temps(&state_dir, TempKind::Ledger)?;
    cleanup_stale_temps(&output_root, TempKind::OutputBinding)?;

    let ledger = load_ledger(&state_dir)?;
    let output_binding = load_output_binding(&output_root)?;
    validate_persistent_bindings(
        ledger.as_ref(),
        output_binding.as_ref(),
        &config.personality_agent_id,
        &observed_bindings,
    )?;
    role_dirs.initialize_or_validate(ledger.is_some() || output_binding.is_some())?;
    let role_write_guard = role_dirs.begin_writing()?;
    let allocation_result = (|| -> Result<AllocationMetadata> {
        for (_, directory, gid) in role_dirs.entries() {
            cleanup_stale_temps(directory, TempKind::Identity { gid })?;
        }
        if output_binding.is_none() {
            let binding = OutputBindingWire {
                version: OUTPUT_BINDING_VERSION,
                personality_agent_id: config.personality_agent_id.as_str().to_owned(),
                directories: observed_bindings.clone(),
            };
            atomic_replace_json(
                &output_root,
                OUTPUT_BINDING_FILE,
                AtomicFileSpec::state(),
                &binding,
                Some(DurableTarget::OutputBinding),
                config.crash_failpoint,
            )
            .context("failed to durably bind the identity output root")?;
        }

        let allocation = FreshAllocation::new()?;
        let generation = next_generation(
            ledger.as_ref(),
            &config.personality_agent_id,
            max_generation,
        )?;
        let next_ledger = LedgerWire {
            version: LEDGER_VERSION,
            personality_agent_id: config.personality_agent_id.as_str().to_owned(),
            directories: observed_bindings,
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
        atomic_replace_json(
            &state_dir,
            LEDGER_FILE,
            AtomicFileSpec::state(),
            &next_ledger,
            Some(DurableTarget::Ledger),
            config.crash_failpoint,
        )
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
            config.crash_failpoint,
        )?;
        Ok(AllocationMetadata {
            status: "allocated",
            generation,
        })
    })();
    let handoff_result = role_write_guard
        .finish()
        .context("failed to restore role identity directories to handoff mode");
    drop(process_guard);
    match (allocation_result, handoff_result) {
        (Ok(metadata), Ok(())) => Ok(metadata),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(handoff_error)) => Err(error).context(format!(
            "role handoff restoration also failed: {handoff_error:#}"
        )),
    }
}

fn required_absolute_env_path(name: &str) -> Result<PathBuf> {
    let value = std::env::var_os(name).with_context(|| format!("{name} is required"))?;
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        bail!("{name} must be an absolute path");
    }
    Ok(path)
}

fn required_gid(name: &str) -> Result<libc::gid_t> {
    let value = std::env::var(name).with_context(|| format!("{name} is required"))?;
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        bail!("{name} must be a canonical unsigned decimal GID");
    }
    let gid = value
        .parse::<u32>()
        .with_context(|| format!("{name} is outside the platform GID domain"))?;
    if gid.to_string() != value {
        bail!("{name} must be a canonical unsigned decimal GID");
    }
    Ok(gid as libc::gid_t)
}

fn supplementary_groups() -> Result<BTreeSet<libc::gid_t>> {
    let count = unsafe { libc::getgroups(0, std::ptr::null_mut()) };
    if count < 0 {
        return Err(io::Error::last_os_error()).context("failed to inspect supplemental groups");
    }
    let mut groups = vec![0 as libc::gid_t; count as usize];
    if count > 0 {
        let observed = unsafe { libc::getgroups(count, groups.as_mut_ptr()) };
        if observed != count {
            return Err(io::Error::last_os_error())
                .context("supplemental group membership changed during validation");
        }
    }
    Ok(groups.into_iter().collect())
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn is_strict_descendant(path: &Path, root: &Path) -> bool {
    path != root && path.starts_with(root)
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LedgerWire {
    version: u8,
    personality_agent_id: String,
    directories: DirectoryBindings,
    state: LedgerState,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum LedgerState {
    Next { generation: u64 },
    Exhausted,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
struct DirectoryIdentity {
    device: u64,
    inode: u64,
}

impl DirectoryIdentity {
    fn from_file(directory: &File, kind: &str) -> Result<Self> {
        let metadata = directory
            .metadata()
            .with_context(|| format!("failed to inspect {kind} directory identity"))?;
        if !metadata.file_type().is_dir() {
            bail!("{kind} is not a directory");
        }
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DirectoryBindings {
    trust_root: DirectoryIdentity,
    state: DirectoryIdentity,
    output: DirectoryIdentity,
    runtime: DirectoryIdentity,
    executor: DirectoryIdentity,
    broker: DirectoryIdentity,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OutputBindingWire {
    version: u8,
    personality_agent_id: String,
    directories: DirectoryBindings,
}

fn load_ledger(state_dir: &File) -> Result<Option<LedgerWire>> {
    let Some(file) = open_optional_checked_file(
        state_dir,
        LEDGER_FILE,
        libc::O_RDONLY,
        STATE_FILE_MODE,
        unsafe { libc::getegid() },
    )?
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

fn load_output_binding(output_root: &File) -> Result<Option<OutputBindingWire>> {
    let Some(file) = open_optional_checked_file(
        output_root,
        OUTPUT_BINDING_FILE,
        libc::O_RDONLY,
        STATE_FILE_MODE,
        unsafe { libc::getegid() },
    )?
    else {
        return Ok(None);
    };
    let bytes = read_bounded(file, OUTPUT_BINDING_FILE)?;
    let binding: OutputBindingWire = serde_json::from_slice(&bytes)
        .context("allocator output binding is corrupt or truncated")?;
    if binding.version != OUTPUT_BINDING_VERSION {
        bail!("allocator output-binding version is unsupported");
    }
    Ok(Some(binding))
}

fn validate_persistent_bindings(
    ledger: Option<&LedgerWire>,
    output_binding: Option<&OutputBindingWire>,
    expected_paid: &PersonalityAgentId,
    observed: &DirectoryBindings,
) -> Result<()> {
    if let Some(ledger) = ledger {
        validate_bound_paid(
            &ledger.personality_agent_id,
            expected_paid,
            "allocator ledger",
        )?;
        if &ledger.directories != observed {
            bail!("allocator state, output, or role directory was replaced");
        }
    }
    if let Some(binding) = output_binding {
        validate_bound_paid(
            &binding.personality_agent_id,
            expected_paid,
            "allocator output binding",
        )?;
        if &binding.directories != observed {
            bail!("allocator state, output, or role directory was replaced");
        }
    }
    if ledger.is_some() && output_binding.is_none() {
        bail!("allocator output binding is missing for an initialized ledger");
    }
    Ok(())
}

fn validate_bound_paid(value: &str, expected_paid: &PersonalityAgentId, kind: &str) -> Result<()> {
    let paid = PersonalityAgentId::parse(value)
        .with_context(|| format!("{kind} contains an invalid personality-agent binding"))?;
    if &paid != expected_paid {
        bail!("{kind} is permanently bound to a different personality agent");
    }
    Ok(())
}

fn next_generation(
    ledger: Option<&LedgerWire>,
    expected_paid: &PersonalityAgentId,
    max_generation: u64,
) -> Result<u64> {
    let Some(ledger) = ledger else {
        return Ok(0);
    };
    validate_bound_paid(
        &ledger.personality_agent_id,
        expected_paid,
        "allocator ledger",
    )?;
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
    gids: RoleGids,
}

impl RoleIdentityDirs {
    fn open(output_root: &File, gids: RoleGids) -> Result<Self> {
        let runtime = open_unvalidated_dir(output_root, "runtime")
            .context("runtime identity output directory is not trustworthy")?;
        let executor = open_unvalidated_dir(output_root, "executor")
            .context("executor identity output directory is not trustworthy")?;
        let broker = open_unvalidated_dir(output_root, "broker")
            .context("broker identity output directory is not trustworthy")?;
        let directories = Self {
            runtime,
            executor,
            broker,
            gids,
        };
        directories.reject_aliases(output_root)?;
        Ok(directories)
    }

    fn initialize_or_validate(&self, persistent_binding_exists: bool) -> Result<()> {
        for (role, directory, gid) in self.entries() {
            let state = role_directory_state(directory, gid, role)?;
            if persistent_binding_exists && state != RoleDirectoryState::Ready {
                bail!("{role} identity directory lost its persistent role-group handoff");
            }
            if !persistent_binding_exists {
                if open_optional_unvalidated_file(directory, "identity.env")?.is_some() {
                    bail!("unbound {role} identity directory must not contain identity.env");
                }
                if state != RoleDirectoryState::Ready {
                    set_exact_gid(directory, gid, role)?;
                    set_exact_mode(directory, ROLE_DIR_HANDOFF_MODE, role)?;
                    fsync_file(directory, role)?;
                }
            }
            validate_dir_modes(
                directory,
                &[ROLE_DIR_HANDOFF_MODE, ROLE_DIR_WRITING_MODE],
                gid,
                role,
            )?;
            if let Some(existing) = open_optional_checked_file(
                directory,
                "identity.env",
                libc::O_RDONLY,
                IDENTITY_FILE_MODE,
                gid,
            )
            .with_context(|| format!("{role} identity target is not trustworthy"))?
            {
                drop(existing);
            }
        }
        Ok(())
    }

    fn reject_aliases(&self, output_root: &File) -> Result<()> {
        let output = DirectoryIdentity::from_file(output_root, "identity output root")?;
        let runtime = DirectoryIdentity::from_file(&self.runtime, "runtime identity")?;
        let executor = DirectoryIdentity::from_file(&self.executor, "executor identity")?;
        let broker = DirectoryIdentity::from_file(&self.broker, "broker identity")?;
        let identities = [output, runtime, executor, broker];
        if identities.into_iter().collect::<BTreeSet<_>>().len() != identities.len() {
            bail!("identity output and role directories must not alias the same inode");
        }
        Ok(())
    }

    fn bindings(
        &self,
        trust_root: &File,
        state: &File,
        output: &File,
    ) -> Result<DirectoryBindings> {
        let bindings = DirectoryBindings {
            trust_root: DirectoryIdentity::from_file(trust_root, "allocator trust root")?,
            state: DirectoryIdentity::from_file(state, "allocator state")?,
            output: DirectoryIdentity::from_file(output, "identity output root")?,
            runtime: DirectoryIdentity::from_file(&self.runtime, "runtime identity")?,
            executor: DirectoryIdentity::from_file(&self.executor, "executor identity")?,
            broker: DirectoryIdentity::from_file(&self.broker, "broker identity")?,
        };
        let all = [
            bindings.trust_root,
            bindings.state,
            bindings.output,
            bindings.runtime,
            bindings.executor,
            bindings.broker,
        ];
        if all.into_iter().collect::<BTreeSet<_>>().len() != all.len() {
            bail!("allocator state, output, and role directories must not alias");
        }
        Ok(bindings)
    }

    fn begin_writing(&self) -> Result<RoleDirectoryWriteGuard<'_>> {
        let mut guard = RoleDirectoryWriteGuard {
            directories: self,
            armed: true,
        };
        match guard.set_all_modes(ROLE_DIR_WRITING_MODE) {
            Ok(()) => Ok(guard),
            Err(error) => match guard.finish() {
                Ok(()) => Err(error),
                Err(handoff_error) => Err(error).context(format!(
                    "role handoff restoration also failed: {handoff_error:#}"
                )),
            },
        }
    }

    fn entries(&self) -> [(&'static str, &File, libc::gid_t); 3] {
        [
            ("runtime", &self.runtime, self.gids.runtime),
            ("executor", &self.executor, self.gids.executor),
            ("broker", &self.broker, self.gids.broker),
        ]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RoleDirectoryState {
    Fresh,
    Initializing,
    Ready,
}

fn role_directory_state(
    directory: &File,
    role_gid: libc::gid_t,
    role: &str,
) -> Result<RoleDirectoryState> {
    let metadata = directory
        .metadata()
        .with_context(|| format!("failed to inspect {role} identity directory"))?;
    if !metadata.file_type().is_dir() || metadata.uid() != unsafe { libc::geteuid() } {
        bail!("{role} identity directory has an unsafe type or owner");
    }
    let mode = metadata.mode() & 0o7777;
    let allocator_gid = unsafe { libc::getegid() };
    match (metadata.gid(), mode) {
        (gid, 0o700) if gid == allocator_gid => Ok(RoleDirectoryState::Fresh),
        (gid, 0o700) if gid == role_gid => Ok(RoleDirectoryState::Initializing),
        (gid, observed)
            if gid == role_gid
                && [ROLE_DIR_HANDOFF_MODE, ROLE_DIR_WRITING_MODE].contains(&observed) =>
        {
            Ok(RoleDirectoryState::Ready)
        }
        _ => bail!("{role} identity directory has unsafe ownership or mode"),
    }
}

struct RoleDirectoryWriteGuard<'a> {
    directories: &'a RoleIdentityDirs,
    armed: bool,
}

impl RoleDirectoryWriteGuard<'_> {
    fn set_all_modes(&mut self, mode: u32) -> Result<()> {
        for (role, directory, gid) in self.directories.entries() {
            validate_dir_modes(
                directory,
                &[ROLE_DIR_HANDOFF_MODE, ROLE_DIR_WRITING_MODE],
                gid,
                role,
            )?;
            set_exact_mode(directory, mode, role)?;
            fsync_file(directory, role)?;
            validate_dir(directory, mode, gid, role)?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<()> {
        self.set_all_modes(ROLE_DIR_HANDOFF_MODE)?;
        self.armed = false;
        Ok(())
    }
}

impl Drop for RoleDirectoryWriteGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            for (role, directory, _) in self.directories.entries() {
                let _ = set_exact_mode(directory, ROLE_DIR_HANDOFF_MODE, role);
                let _ = fsync_file(directory, role);
            }
        }
    }
}

fn materialize_role_identities(
    role_dirs: &RoleIdentityDirs,
    paid: &PersonalityAgentId,
    generation: u64,
    allocation: &FreshAllocation,
    failpoint: Failpoint,
    crash_failpoint: Option<CrashFailpoint>,
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
        AtomicFileSpec::identity(role_dirs.gids.runtime),
        runtime_env.as_bytes(),
        Some(DurableTarget::Runtime),
        crash_failpoint,
    )?;
    if failpoint == Failpoint::AfterRuntimeIdentity {
        bail!("injected failure after runtime identity commit");
    }
    atomic_replace_bytes(
        &role_dirs.executor,
        "identity.env",
        AtomicFileSpec::identity(role_dirs.gids.executor),
        common.as_bytes(),
        Some(DurableTarget::Executor),
        crash_failpoint,
    )?;
    if failpoint == Failpoint::AfterExecutorIdentity {
        bail!("injected failure after executor identity commit");
    }
    atomic_replace_bytes(
        &role_dirs.broker,
        "identity.env",
        AtomicFileSpec::identity(role_dirs.gids.broker),
        common.as_bytes(),
        Some(DurableTarget::Broker),
        crash_failpoint,
    )?;
    if failpoint == Failpoint::AfterBrokerIdentity {
        bail!("injected failure after broker identity commit");
    }
    Ok(())
}

fn atomic_replace_json<T: Serialize>(
    parent: &File,
    destination: &str,
    spec: AtomicFileSpec,
    value: &T,
    target: Option<DurableTarget>,
    crash_failpoint: Option<CrashFailpoint>,
) -> Result<()> {
    let mut bytes = serde_json::to_vec(value).context("failed to serialize durable state")?;
    bytes.push(b'\n');
    atomic_replace_bytes(parent, destination, spec, &bytes, target, crash_failpoint)
}

#[derive(Clone, Copy)]
struct AtomicFileSpec {
    final_mode: u32,
    temp_mode: u32,
    gid: libc::gid_t,
}

impl AtomicFileSpec {
    fn state() -> Self {
        Self {
            final_mode: STATE_FILE_MODE,
            temp_mode: STATE_FILE_MODE,
            gid: unsafe { libc::getegid() },
        }
    }

    const fn identity(gid: libc::gid_t) -> Self {
        Self {
            final_mode: IDENTITY_FILE_MODE,
            temp_mode: IDENTITY_TEMP_MODE,
            gid,
        }
    }
}

fn atomic_replace_bytes(
    parent: &File,
    destination: &str,
    spec: AtomicFileSpec,
    bytes: &[u8],
    target: Option<DurableTarget>,
    crash_failpoint: Option<CrashFailpoint>,
) -> Result<()> {
    if let Some(existing) = open_optional_checked_file(
        parent,
        destination,
        libc::O_RDONLY,
        spec.final_mode,
        spec.gid,
    )? {
        drop(existing);
    }
    let temp_name = format!(".{destination}.tmp-{}", Uuid::now_v7().hyphenated());
    let mut temp = openat(
        parent.as_raw_fd(),
        OsStr::new(&temp_name),
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
        spec.temp_mode,
    )
    .with_context(|| format!("failed to create temporary {destination}"))?;
    let mut unlink_guard = TempUnlinkGuard::new(parent, &temp_name)?;
    set_exact_gid(&temp, spec.gid, destination)?;
    set_exact_mode(&temp, spec.temp_mode, destination)?;
    validate_file(
        &temp,
        spec.temp_mode,
        spec.gid,
        &format!("temporary {destination}"),
    )?;
    let split = bytes.len().div_ceil(2);
    temp.write_all(&bytes[..split])
        .with_context(|| format!("failed to write temporary {destination}"))?;
    trigger_crash(crash_failpoint, target, DurableStage::PartialWrite);
    temp.write_all(&bytes[split..])
        .with_context(|| format!("failed to finish temporary {destination}"))?;
    set_exact_mode(&temp, spec.final_mode, destination)?;
    temp.sync_all()
        .with_context(|| format!("failed to fsync temporary {destination}"))?;
    validate_file(
        &temp,
        spec.final_mode,
        spec.gid,
        &format!("temporary {destination}"),
    )?;
    trigger_crash(crash_failpoint, target, DurableStage::FileFsync);
    rename_entry(parent, &temp_name, parent, destination)
        .with_context(|| format!("failed to atomically replace {destination}"))?;
    unlink_guard.disarm();
    trigger_crash(crash_failpoint, target, DurableStage::Rename);
    fsync_file(parent, "durable-state parent directory")?;
    trigger_crash(crash_failpoint, target, DurableStage::ParentFsync);
    Ok(())
}

fn trigger_crash(
    failpoint: Option<CrashFailpoint>,
    target: Option<DurableTarget>,
    stage: DurableStage,
) {
    if let (Some(failpoint), Some(target)) = (failpoint, target) {
        failpoint.trigger(target, stage);
    }
}

struct TempUnlinkGuard {
    parent_fd: RawFd,
    name: CString,
    armed: bool,
}

impl TempUnlinkGuard {
    fn new(parent: &File, name: &str) -> Result<Self> {
        Ok(Self {
            parent_fd: parent.as_raw_fd(),
            name: cstring(name)?,
            armed: true,
        })
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempUnlinkGuard {
    fn drop(&mut self) {
        if self.armed {
            unsafe {
                libc::unlinkat(self.parent_fd, self.name.as_ptr(), 0);
            }
        }
    }
}

#[derive(Clone, Copy)]
enum TempKind {
    Ledger,
    OutputBinding,
    Identity { gid: libc::gid_t },
}

impl TempKind {
    const fn prefix(self) -> &'static str {
        match self {
            Self::Ledger => ".allocator-ledger.json.tmp-",
            Self::OutputBinding => ".allocator-binding.json.tmp-",
            Self::Identity { .. } => ".identity.env.tmp-",
        }
    }
}

fn cleanup_stale_temps(parent: &File, kind: TempKind) -> Result<()> {
    let mut removed = false;
    for name in directory_entry_names(parent)? {
        let Ok(name) = std::str::from_utf8(&name) else {
            continue;
        };
        let Some(uuid) = name.strip_prefix(kind.prefix()) else {
            continue;
        };
        if validate_uuid_v7_text(uuid).is_err() {
            continue;
        }
        let file = openat(
            parent.as_raw_fd(),
            OsStr::new(name),
            libc::O_RDONLY | libc::O_NONBLOCK,
            0,
        )
        .with_context(|| format!("strictly named stale allocator temp {name} is unsafe"))?;
        validate_stale_temp(&file, kind, name)?;
        drop(file);
        unlink_entry(parent, name)
            .with_context(|| format!("failed to remove stale allocator temp {name}"))?;
        removed = true;
    }
    if removed {
        fsync_file(parent, "stale-temp parent directory")?;
    }
    Ok(())
}

fn validate_stale_temp(file: &File, kind: TempKind, name: &str) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect stale allocator temp {name}"))?;
    if !metadata.file_type().is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.nlink() != 1
    {
        bail!("strictly named stale allocator temp {name} has unsafe identity");
    }
    let mode = metadata.mode() & 0o7777;
    let allocator_gid = unsafe { libc::getegid() };
    let valid = match kind {
        TempKind::Ledger | TempKind::OutputBinding => {
            metadata.gid() == allocator_gid
                && (mode == STATE_FILE_MODE || (mode == 0 && metadata.len() == 0))
        }
        TempKind::Identity { gid } => {
            (metadata.gid() == gid
                && (mode == IDENTITY_TEMP_MODE
                    || mode == IDENTITY_FILE_MODE
                    || (mode == 0 && metadata.len() == 0)))
                || (metadata.gid() == allocator_gid
                    && metadata.len() == 0
                    && (mode == 0 || mode == IDENTITY_TEMP_MODE))
        }
    };
    if !valid {
        bail!("strictly named stale allocator temp {name} has unsafe ownership or mode");
    }
    Ok(())
}

fn validate_uuid_v7_text(value: &str) -> Result<()> {
    let uuid = Uuid::parse_str(value).context("temp suffix is not a UUID")?;
    if uuid.get_version() != Some(Version::SortRand)
        || uuid.get_variant() != Variant::RFC4122
        || uuid.hyphenated().to_string() != value
    {
        bail!("temp suffix is not canonical lowercase UUIDv7");
    }
    Ok(())
}

fn directory_entry_names(directory: &File) -> Result<Vec<Vec<u8>>> {
    let duplicate = unsafe {
        libc::fcntl(
            directory.as_raw_fd(),
            libc::F_DUPFD_CLOEXEC,
            3 as libc::c_int,
        )
    };
    if duplicate < 0 {
        return Err(io::Error::last_os_error()).context("failed to duplicate directory descriptor");
    }
    let stream = unsafe { libc::fdopendir(duplicate) };
    if stream.is_null() {
        let error = io::Error::last_os_error();
        unsafe {
            libc::close(duplicate);
        }
        return Err(error).context("failed to open directory stream");
    }
    let mut stream = DirectoryStream(stream);
    let mut names = Vec::new();
    loop {
        unsafe {
            *libc::__errno_location() = 0;
        }
        let entry = unsafe { libc::readdir(stream.0) };
        if entry.is_null() {
            let errno = unsafe { *libc::__errno_location() };
            if errno != 0 {
                return Err(io::Error::from_raw_os_error(errno))
                    .context("failed while reading directory entries");
            }
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if name != b"." && name != b".." {
            names.push(name.to_vec());
        }
    }
    stream.close()?;
    Ok(names)
}

struct DirectoryStream(*mut libc::DIR);

impl DirectoryStream {
    fn close(&mut self) -> Result<()> {
        if self.0.is_null() {
            return Ok(());
        }
        let stream = std::mem::replace(&mut self.0, std::ptr::null_mut());
        if unsafe { libc::closedir(stream) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error()).context("failed to close directory stream")
        }
    }
}

impl Drop for DirectoryStream {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                libc::closedir(self.0);
            }
        }
    }
}

fn open_absolute_trust_root(
    path: &Path,
    expected_mode: u32,
    expected_gid: libc::gid_t,
) -> Result<File> {
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
    let components: Vec<_> = path.components().collect();
    for (index, component) in components.iter().enumerate() {
        match component {
            Component::RootDir if index == 0 => {}
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
            Component::RootDir
            | Component::CurDir
            | Component::ParentDir
            | Component::Prefix(_) => {
                bail!("trusted directory path contains a forbidden component")
            }
        }
    }
    if !saw_normal {
        bail!("filesystem root cannot be used as a trusted allocator directory");
    }
    validate_dir(&current, expected_mode, expected_gid, "trusted directory")?;
    Ok(current)
}

fn open_trusted_descendant(
    trust_root: &File,
    trust_root_path: &Path,
    path: &Path,
    expected_mode: u32,
    expected_gid: libc::gid_t,
) -> Result<File> {
    let relative = path
        .strip_prefix(trust_root_path)
        .context("trusted path is outside the allocator trust root")?;
    if relative.as_os_str().is_empty() {
        bail!("trusted path must be a strict descendant of the allocator trust root");
    }
    let components: Vec<_> = relative.components().collect();
    let mut current = trust_root
        .try_clone()
        .context("failed to pin allocator trust-root descriptor")?;
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(name) = component else {
            bail!("trusted descendant path contains a forbidden component");
        };
        current = openat(
            current.as_raw_fd(),
            name,
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )
        .with_context(|| {
            format!(
                "failed to open trusted descendant component {}",
                name.to_string_lossy()
            )
        })?;
        if index + 1 != components.len() {
            validate_descendant_ancestor(&current, "trusted descendant ancestor")?;
        }
    }
    validate_dir(
        &current,
        expected_mode,
        expected_gid,
        "trusted descendant directory",
    )?;
    Ok(current)
}

fn open_unvalidated_dir(parent: &File, name: &str) -> Result<File> {
    openat(
        parent.as_raw_fd(),
        OsStr::new(name),
        libc::O_RDONLY | libc::O_DIRECTORY,
        0,
    )
}

fn open_or_create_checked_file(parent: &File, name: &str, access: i32, mode: u32) -> Result<File> {
    let gid = unsafe { libc::getegid() };
    match create_new_checked_file(parent, name, access, mode, gid) {
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
            validate_file(&file, mode, gid, name)?;
            Ok(file)
        }
        Err(error) => Err(error),
    }
}

fn create_new_checked_file(
    parent: &File,
    name: &str,
    access: i32,
    mode: u32,
    gid: libc::gid_t,
) -> Result<File> {
    let file = openat(
        parent.as_raw_fd(),
        OsStr::new(name),
        access | libc::O_CREAT | libc::O_EXCL,
        mode,
    )?;
    set_exact_gid(&file, gid, name)?;
    set_exact_mode(&file, mode, name)?;
    validate_file(&file, mode, gid, name)?;
    Ok(file)
}

fn open_optional_checked_file(
    parent: &File,
    name: &str,
    access: i32,
    mode: u32,
    gid: libc::gid_t,
) -> Result<Option<File>> {
    match openat(
        parent.as_raw_fd(),
        OsStr::new(name),
        access | libc::O_NONBLOCK,
        0,
    ) {
        Ok(file) => {
            validate_file(&file, mode, gid, name)?;
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

fn open_optional_unvalidated_file(parent: &File, name: &str) -> Result<Option<File>> {
    match openat(
        parent.as_raw_fd(),
        OsStr::new(name),
        libc::O_RDONLY | libc::O_NONBLOCK,
        0,
    ) {
        Ok(file) => Ok(Some(file)),
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

fn unlink_entry(parent: &File, name: &str) -> Result<()> {
    let name = cstring(name)?;
    if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error()).context("unlinkat failed")
    }
}

fn cstring(value: &str) -> Result<CString> {
    CString::new(value).context("path name contains NUL")
}

fn set_exact_mode(file: &File, mode: u32, kind: &str) -> Result<()> {
    file.set_permissions(Permissions::from_mode(mode))
        .with_context(|| format!("failed to set exact mode on {kind}"))
}

fn set_exact_gid(file: &File, gid: libc::gid_t, kind: &str) -> Result<()> {
    let effective_uid = unsafe { libc::geteuid() };
    if unsafe { libc::fchown(file.as_raw_fd(), effective_uid, gid) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
            .with_context(|| format!("failed to set exact ownership on {kind}"))
    }
}

fn validate_descendant_ancestor(file: &File, kind: &str) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect {kind}"))?;
    if !metadata.file_type().is_dir() {
        bail!("{kind} is not a directory");
    }
    let effective_uid = unsafe { libc::geteuid() };
    if metadata.uid() != effective_uid {
        bail!("{kind} is not owned by the effective supervisor UID");
    }
    if metadata.mode() & 0o022 != 0 {
        bail!("{kind} must not be group- or world-writable");
    }
    Ok(())
}

fn validate_dir(
    file: &File,
    expected_mode: u32,
    expected_gid: libc::gid_t,
    kind: &str,
) -> Result<()> {
    validate_dir_modes(file, &[expected_mode], expected_gid, kind)
}

fn validate_dir_modes(
    file: &File,
    expected_modes: &[u32],
    expected_gid: libc::gid_t,
    kind: &str,
) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect {kind}"))?;
    if !metadata.file_type().is_dir() {
        bail!("{kind} is not a directory");
    }
    validate_owner_gid_and_modes(&metadata, expected_modes, expected_gid, kind)
}

fn validate_file(
    file: &File,
    expected_mode: u32,
    expected_gid: libc::gid_t,
    kind: &str,
) -> Result<()> {
    validate_file_modes(file, &[expected_mode], expected_gid, kind)
}

fn validate_file_modes(
    file: &File,
    expected_modes: &[u32],
    expected_gid: libc::gid_t,
    kind: &str,
) -> Result<()> {
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect {kind}"))?;
    if !metadata.file_type().is_file() {
        bail!("{kind} is not a regular file");
    }
    if metadata.nlink() != 1 {
        bail!("{kind} must have exactly one hard link");
    }
    validate_owner_gid_and_modes(&metadata, expected_modes, expected_gid, kind)
}

fn validate_owner_gid_and_modes(
    metadata: &Metadata,
    expected_modes: &[u32],
    expected_gid: libc::gid_t,
    kind: &str,
) -> Result<()> {
    let effective_uid = unsafe { libc::geteuid() };
    if metadata.uid() != effective_uid {
        bail!("{kind} is not owned by the effective supervisor UID");
    }
    if metadata.gid() != expected_gid {
        bail!("{kind} is not owned by its exact configured GID");
    }
    let observed_mode = metadata.mode() & 0o7777;
    if !expected_modes.contains(&observed_mode) {
        let expected = expected_modes
            .iter()
            .map(|mode| format!("{mode:04o}"))
            .collect::<Vec<_>>()
            .join(" or ");
        bail!("{kind} mode must be exactly {expected}");
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
        os::unix::{
            ffi::OsStrExt,
            fs::{PermissionsExt, symlink},
        },
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
            let role_gids = test_role_gids();
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
                root: root.clone(),
                config: SupervisorAllocatorConfig {
                    personality_agent_id: PersonalityAgentId::parse(PAID).unwrap(),
                    trust_root: root.clone(),
                    state_dir,
                    identity_output_root,
                    role_gids,
                    crash_failpoint: None,
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

        fn assert_role_handoff(&self) {
            for (role, gid) in [
                ("runtime", self.config.role_gids.runtime),
                ("executor", self.config.role_gids.executor),
                ("broker", self.config.role_gids.broker),
            ] {
                let metadata = fs::metadata(self.config.identity_output_root.join(role)).unwrap();
                assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
                assert_eq!(metadata.gid(), gid);
                assert_eq!(metadata.mode() & 0o7777, ROLE_DIR_HANDOFF_MODE);
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            for role in ["runtime", "executor", "broker"] {
                if let Ok(directory) = File::open(self.config.identity_output_root.join(role)) {
                    let _ = set_exact_mode(&directory, 0o700, role);
                }
            }
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn test_role_gids() -> RoleGids {
        if unsafe { libc::geteuid() } == 0 {
            return RoleGids {
                runtime: 61_001,
                executor: 61_002,
                broker: 61_003,
            };
        }
        let real_gid = unsafe { libc::getgid() };
        let effective_gid = unsafe { libc::getegid() };
        let gids: Vec<_> = supplementary_groups()
            .unwrap()
            .into_iter()
            .filter(|gid| *gid != 0 && *gid != real_gid && *gid != effective_gid)
            .take(3)
            .collect();
        assert_eq!(
            gids.len(),
            3,
            "allocator tests require three distinct supplemental groups"
        );
        RoleGids {
            runtime: gids[0],
            executor: gids[1],
            broker: gids[2],
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
        allocate(&fixture.config).unwrap();
        let ledger_path = fixture.config.state_dir.join(LEDGER_FILE);
        let mut ledger: LedgerWire =
            serde_json::from_slice(&fs::read(&ledger_path).unwrap()).unwrap();
        ledger.state = LedgerState::Next {
            generation: MAX_PROCESS_GENERATION,
        };
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
        fixture.assert_role_handoff();
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
        fixture.assert_role_handoff();
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
        fixture.assert_role_handoff();
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
        fixture.assert_role_handoff();
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

        let executor_identity = fixture
            .config
            .identity_output_root
            .join("executor")
            .join("identity.env");
        let fifo = CString::new(executor_identity.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
        assert!(allocate(&fixture.config).is_err());
        fs::remove_file(&executor_identity).unwrap();

        assert_eq!(allocate(&fixture.config).unwrap().generation, 0);
        let hardlink = fixture.root.join("runtime-identity-hardlink");
        fs::hard_link(&runtime_identity, &hardlink).unwrap();
        assert!(allocate(&fixture.config).is_err());
        fs::remove_file(&hardlink).unwrap();

        fs::set_permissions(&runtime_identity, Permissions::from_mode(0o600)).unwrap();
        assert!(allocate(&fixture.config).is_err());
    }

    #[test]
    fn ordinary_atomic_setup_errors_unlink_the_temporary_file() {
        let fixture = Fixture::new("temp-guard");
        let state = File::open(&fixture.config.state_dir).unwrap();
        assert!(
            atomic_replace_bytes(
                &state,
                "guard-test",
                AtomicFileSpec::identity(libc::gid_t::MAX),
                b"content",
                None,
                None,
            )
            .is_err()
        );
        assert!(
            directory_entry_names(&state)
                .unwrap()
                .into_iter()
                .all(|name| !name.starts_with(b".guard-test.tmp-"))
        );
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
