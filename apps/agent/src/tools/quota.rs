//! Resource quota policy and host-boundary enforcement.
//!
//! T27 converges disk/inode/PID/CPU throttle/CPU time/memory max/wall/output
//! limits into typed `ResourceLimit` variants. The boundary layer supports
//! cgroup-v2 when the host delegates controllers, and falls back to rlimits
//! for the low-trust local path.

use std::{
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::process::Command;
use uuid::Uuid;

use crate::tools::{ResourceLimit, ToolError};

const QUOTA_CGROUP_PREFIX: &str = "sumi-exec";
const MAX_EXECUTION_ID_BYTES: usize = 200;

/// A policy describing the resource limits for a single tool execution.
#[derive(Clone)]
pub struct ResourceQuotaPolicy {
    pub wall_time: Duration,
    pub output_bytes: u64,
    pub memory_bytes: Option<u64>,
    pub cpu_time_seconds: Option<u64>,
    /// `cpu.max` quota in microseconds per 100ms period. For example, 10000
    /// limits the cgroup to 10% of a single CPU.
    pub cpu_throttle_us_per_100ms: Option<u64>,
    pub pids_max: Option<u64>,
    pub disk_bytes: Option<u64>,
    pub disk_inodes: Option<u64>,
    /// Optional backend used to enforce disk/inode quotas for `WorkspaceFs`
    /// and to fail-closed when real project quota is unavailable.
    pub disk_quota_backend: Option<Arc<dyn DiskQuotaBackend>>,
}

impl std::fmt::Debug for ResourceQuotaPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceQuotaPolicy")
            .field("wall_time", &self.wall_time)
            .field("output_bytes", &self.output_bytes)
            .field("memory_bytes", &self.memory_bytes)
            .field("cpu_time_seconds", &self.cpu_time_seconds)
            .field("cpu_throttle_us_per_100ms", &self.cpu_throttle_us_per_100ms)
            .field("pids_max", &self.pids_max)
            .field("disk_bytes", &self.disk_bytes)
            .field("disk_inodes", &self.disk_inodes)
            .field("disk_quota_backend", &self.disk_quota_backend.is_some())
            .finish()
    }
}

impl PartialEq for ResourceQuotaPolicy {
    fn eq(&self, other: &Self) -> bool {
        self.wall_time == other.wall_time
            && self.output_bytes == other.output_bytes
            && self.memory_bytes == other.memory_bytes
            && self.cpu_time_seconds == other.cpu_time_seconds
            && self.cpu_throttle_us_per_100ms == other.cpu_throttle_us_per_100ms
            && self.pids_max == other.pids_max
            && self.disk_bytes == other.disk_bytes
            && self.disk_inodes == other.disk_inodes
            && self
                .disk_quota_backend
                .as_ref()
                .map(|arc| Arc::as_ptr(arc) as *const ())
                == other
                    .disk_quota_backend
                    .as_ref()
                    .map(|arc| Arc::as_ptr(arc) as *const ())
    }
}

impl Eq for ResourceQuotaPolicy {}

impl Default for ResourceQuotaPolicy {
    fn default() -> Self {
        Self {
            wall_time: Duration::from_secs(120),
            output_bytes: 10 * 1024 * 1024,
            // Production defaults from docs/agent/workspace.md.
            memory_bytes: Some(512 * 1024 * 1024),
            cpu_time_seconds: Some(120),
            // 1 vCPU cap expressed as 100000us per 100ms period.
            cpu_throttle_us_per_100ms: Some(100_000),
            pids_max: Some(64),
            disk_bytes: None,
            disk_inodes: None,
            disk_quota_backend: None,
        }
    }
}

impl ResourceQuotaPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_wall_time(mut self, seconds: u64) -> Self {
        self.wall_time = Duration::from_secs(seconds);
        self
    }

    pub fn with_output_bytes(mut self, bytes: u64) -> Self {
        self.output_bytes = bytes;
        self
    }

    pub fn with_memory_bytes(mut self, bytes: u64) -> Self {
        self.memory_bytes = Some(bytes);
        self
    }

    pub fn with_cpu_time_seconds(mut self, seconds: u64) -> Self {
        self.cpu_time_seconds = Some(seconds);
        self
    }

    pub fn with_cpu_throttle_percent(mut self, percent: u64) -> Self {
        // 100ms period => percent maps directly to 1000us per 1%.
        self.cpu_throttle_us_per_100ms = Some(percent.saturating_mul(1000));
        self
    }

    pub fn with_pids_max(mut self, max: u64) -> Self {
        self.pids_max = Some(max);
        self
    }

    pub fn with_disk_bytes(mut self, bytes: u64) -> Self {
        self.disk_bytes = Some(bytes);
        self
    }

    pub fn with_disk_inodes(mut self, inodes: u64) -> Self {
        self.disk_inodes = Some(inodes);
        self
    }

    pub fn with_disk_quota_backend(mut self, backend: Arc<dyn DiskQuotaBackend>) -> Self {
        self.disk_quota_backend = Some(backend);
        self
    }

    /// Apply this policy to a `tokio::process::Command`. Returns an
    /// `AppliedQuota` describing the cgroup/rlimit state and any host features
    /// that could not be enforced.
    pub fn apply_to_command(
        &self,
        command: &mut Command,
        execution_id: &str,
    ) -> Result<AppliedQuota, ToolError> {
        if execution_id.is_empty() || execution_id.len() > MAX_EXECUTION_ID_BYTES {
            return Err(ToolError::Protocol(format!(
                "execution_id must contain 1..={MAX_EXECUTION_ID_BYTES} bytes"
            )));
        }

        if (self.disk_bytes.is_some() || self.disk_inodes.is_some())
            && self.disk_quota_backend.is_none()
        {
            // Fail-closed: without a disk/inode backend we cannot enforce
            // the configured workspace limits, so refuse to spawn rather
            // than silently run unbounded.
            if let Some(limit) = self.disk_bytes {
                return Err(ToolError::ResourceLimit(ResourceLimit::DiskBytes {
                    observed: 0,
                    limit,
                }));
            }
            if let Some(limit) = self.disk_inodes {
                return Err(ToolError::ResourceLimit(ResourceLimit::DiskInodes {
                    observed: 0,
                    limit,
                }));
            }
        }

        let boundary = best_available_boundary()?;
        boundary.apply(command, self, execution_id)
    }
}

/// Enforceable result returned by `ResourceQuotaPolicy::apply_to_command`.
#[derive(Debug)]
pub struct AppliedQuota {
    pub skipped_features: Vec<HostFeatureSkip>,
    cgroup: Option<CgroupContext>,
}

impl AppliedQuota {
    pub fn cgroup_path(&self) -> Option<&Path> {
        self.cgroup.as_ref().map(|c| c.path.as_path())
    }

    /// After the child has exited, inspect cgroup evidence and return the most
    /// specific `ResourceLimit` variant that explains the termination. This
    /// lets the low-trust harness produce typed limits for memory/PID/CPU
    /// events even when the kernel killed the process with `SIGKILL`.
    pub fn classify(&self, status: &ExitStatus) -> Option<ResourceLimit> {
        use std::os::unix::process::ExitStatusExt;
        let cgroup = self.cgroup.as_ref()?;
        if status.signal().is_none() {
            // The process exited voluntarily. PID/disk/inode denials may still
            // have happened (the shell may exit non-zero), but the canonical
            // classification for those happens through `pids.events` or shell
            // IO errors and is not tied to a signal.
        }

        if let Some(limit) = cgroup.policy.memory_bytes
            && let Some(events) = read_memory_events(&cgroup.path)
            && (events.oom > 0 || events.oom_kill > 0)
        {
            return Some(ResourceLimit::Memory { limit });
        }

        if let Some(limit_seconds) = cgroup.policy.cpu_time_seconds
            && let Some(stat) = read_cpu_stat(&cgroup.path)
        {
            let limit_us = limit_seconds.saturating_mul(1_000_000);
            if stat.usage_usec >= limit_us {
                return Some(ResourceLimit::CpuTime { limit_seconds });
            }
        }

        if let Some(limit) = cgroup.policy.pids_max
            && let Some(events) = read_pids_events(&cgroup.path)
            && events.max > 0
        {
            return Some(ResourceLimit::Pids { limit });
        }

        if let Some(limit) = cgroup.policy.cpu_throttle_us_per_100ms
            && let Some(stat) = read_cpu_stat(&cgroup.path)
            && stat.nr_throttled > 0
            && status.signal().is_some()
        {
            // Do not confuse CPU-throttle termination with CPU-time
            // termination: if usage has reached a large fraction of the
            // configured CPU-time limit, the kill is a CPU-time kill.
            let cpu_time_exceeded = cgroup.policy.cpu_time_seconds.is_some_and(|limit_seconds| {
                let limit_us = limit_seconds.saturating_mul(1_000_000);
                stat.usage_usec >= limit_us.saturating_mul(8).saturating_div(10)
            });
            if !cpu_time_exceeded {
                return Some(ResourceLimit::CpuThrottle { limit });
            }
        }

        None
    }

    /// Best-effort removal of the per-execution cgroup directory. If processes
    /// remain, `cgroup.kill` is written and the directory is removed once it
    /// becomes empty.
    pub fn cleanup(self) {
        if let Some(cgroup) = self.cgroup {
            let _ = cgroup.destroy();
        }
    }
}

/// Description of a host feature that could not be applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostFeatureSkip {
    pub feature: &'static str,
    pub reason: String,
}

impl HostFeatureSkip {
    pub fn resource_limit(&self) -> ResourceLimit {
        match self.feature {
            "disk_bytes" => ResourceLimit::DiskBytes {
                observed: 0,
                limit: 0,
            },
            "disk_inodes" => ResourceLimit::DiskInodes {
                observed: 0,
                limit: 0,
            },
            "cpu_throttle" => ResourceLimit::CpuThrottle { limit: 0 },
            "pids" => ResourceLimit::Pids { limit: 0 },
            "memory" => ResourceLimit::Memory { limit: 0 },
            "cpu_time" => ResourceLimit::CpuTime { limit_seconds: 0 },
            _ => ResourceLimit::Concurrency,
        }
    }
}

/// Convert a raw breach observation into the typed `ResourceLimit` variant.
pub fn limit_for_breach(kind: QuotaKind, observed: u64, limit: u64) -> ResourceLimit {
    match kind {
        QuotaKind::OutputBytes => ResourceLimit::OutputBytes { observed, limit },
        QuotaKind::InputBytes => ResourceLimit::InputBytes { observed, limit },
        QuotaKind::WallTime => ResourceLimit::WallTime {
            limit_seconds: limit,
        },
        QuotaKind::CpuTime => ResourceLimit::CpuTime {
            limit_seconds: limit,
        },
        QuotaKind::CpuThrottle => ResourceLimit::CpuThrottle { limit },
        QuotaKind::Memory => ResourceLimit::Memory { limit },
        QuotaKind::Pids => ResourceLimit::Pids { limit },
        QuotaKind::DiskBytes => ResourceLimit::DiskBytes { observed, limit },
        QuotaKind::DiskInodes => ResourceLimit::DiskInodes { observed, limit },
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuotaKind {
    OutputBytes,
    InputBytes,
    WallTime,
    CpuTime,
    CpuThrottle,
    Memory,
    Pids,
    DiskBytes,
    DiskInodes,
}

/// Injected backend that enforces workspace disk/inode limits in-process.
///
/// Production uses filesystem/project quota; this in-memory harness is used in
/// tests where the host cannot mount a quota-enabled volume.
pub trait DiskQuotaBackend: Send + Sync {
    /// Return `Ok` if adding `bytes` and `inodes` would stay within limits.
    fn check(&self, bytes: u64, inodes: u64) -> Result<(), ResourceLimit>;
    /// Commit a previously-checked addition.
    fn commit(&self, bytes: u64, inodes: u64);
    /// Rollback a previously-committed addition on error.
    fn rollback(&self, bytes: u64, inodes: u64);
}

/// A simple in-memory disk/inode quota harness for tests.
#[derive(Debug)]
pub struct InMemoryDiskQuota {
    limit_bytes: u64,
    limit_inodes: u64,
    usage: Mutex<DiskUsage>,
}

#[derive(Debug, Default)]
struct DiskUsage {
    bytes: u64,
    inodes: u64,
}

impl InMemoryDiskQuota {
    pub fn new(limit_bytes: u64, limit_inodes: u64) -> Arc<Self> {
        Arc::new(Self {
            limit_bytes,
            limit_inodes,
            usage: Mutex::new(DiskUsage::default()),
        })
    }

    pub fn set_usage(&self, bytes: u64, inodes: u64) {
        let mut usage = self.usage.lock().expect("in-memory disk quota lock");
        usage.bytes = bytes;
        usage.inodes = inodes;
    }
}

impl DiskQuotaBackend for InMemoryDiskQuota {
    fn check(&self, bytes: u64, inodes: u64) -> Result<(), ResourceLimit> {
        let usage = self.usage.lock().expect("in-memory disk quota lock");
        let new_bytes = usage.bytes.saturating_add(bytes);
        let new_inodes = usage.inodes.saturating_add(inodes);

        if bytes > 0 && new_bytes > self.limit_bytes {
            return Err(ResourceLimit::DiskBytes {
                observed: new_bytes,
                limit: self.limit_bytes,
            });
        }
        if inodes > 0 && new_inodes > self.limit_inodes {
            return Err(ResourceLimit::DiskInodes {
                observed: new_inodes,
                limit: self.limit_inodes,
            });
        }
        Ok(())
    }

    fn commit(&self, bytes: u64, inodes: u64) {
        let mut usage = self.usage.lock().expect("in-memory disk quota lock");
        usage.bytes = usage.bytes.saturating_add(bytes);
        usage.inodes = usage.inodes.saturating_add(inodes);
    }

    fn rollback(&self, bytes: u64, inodes: u64) {
        let mut usage = self.usage.lock().expect("in-memory disk quota lock");
        usage.bytes = usage.bytes.saturating_sub(bytes);
        usage.inodes = usage.inodes.saturating_sub(inodes);
    }
}

/// Internal boundary abstraction.
trait QuotaBoundary {
    fn apply(
        &self,
        command: &mut Command,
        policy: &ResourceQuotaPolicy,
        execution_id: &str,
    ) -> Result<AppliedQuota, ToolError>;
}

fn best_available_boundary() -> Result<Box<dyn QuotaBoundary>, ToolError> {
    if let Some(boundary) = CgroupV2Boundary::probe() {
        Ok(Box::new(boundary))
    } else {
        Ok(Box::new(LowTrustBoundary))
    }
}

/// Whether this test host can exercise every cgroup-v2 controller required by
/// the Cloud quota contract. Local/CI hosts without delegated cgroups run the
/// low-trust fallback and must not claim the cgroup release acceptance.
#[cfg(test)]
pub(crate) fn cgroup_v2_release_gate_available() -> bool {
    CgroupV2Boundary::probe().is_some_and(|boundary| {
        ["cpu", "memory", "pids"]
            .iter()
            .all(|controller| boundary.supports(controller))
    })
}

/// Cgroup-v2 boundary. Created only when the current cgroup is writable.
struct CgroupV2Boundary {
    base: PathBuf,
    /// Controllers available for child cgroups created under `base`.
    controllers: Vec<String>,
}

impl CgroupV2Boundary {
    fn probe() -> Option<Self> {
        // If the deployment supervisor prepared a generation-scoped cgroup
        // base, prefer it. Otherwise discover a writable domain ancestor.
        if let Some(base) = env_cgroup_base()
            && let Some(boundary) = Self::probe_candidate(base)
        {
            return Some(boundary);
        }

        let mut candidate = current_cgroup_path()?;
        if !candidate.is_dir() {
            return None;
        }
        loop {
            if let Some(boundary) = Self::probe_candidate(candidate.clone()) {
                return Some(boundary);
            }
            if candidate.as_os_str() == "/sys/fs/cgroup" {
                return None;
            }
            candidate = candidate.parent()?.to_path_buf();
        }
    }

    fn probe_candidate(candidate: PathBuf) -> Option<Self> {
        if !candidate.is_dir() {
            return None;
        }

        let needed = ["cpu", "memory", "pids"];

        // A `domain threaded`, `threaded`, or `domain invalid` ancestor cannot
        // host process cgroups; child cgroups there would get `EOPNOTSUPP`
        // when we try to move the spawned command into them. The root cgroup
        // does not always expose `cgroup.type`, so the probe child below is
        // the authoritative test.
        if let Ok(kind) = read_cgroup_type(&candidate)
            && kind.trim() != "domain"
        {
            return None;
        }

        let available = read_controllers(&candidate).unwrap_or_default();
        if !available.iter().any(|c| needed.contains(&c.as_str())) {
            return None;
        }

        // Creating a directory is not enough: some `domain` parents still
        // produce `domain invalid` children under certain controller/thread
        // modes. Create a probe, verify it can hold processes, and confirm
        // the controller limit files are actually writable.
        let probe = candidate.join(format!(
            "sumi-quota-probe-{}-{}",
            std::process::id(),
            Uuid::now_v7()
        ));
        if std::fs::create_dir(&probe).is_err() {
            return None;
        }

        let probe_kind = read_cgroup_type(&probe)
            .map(|s| s.trim().to_owned())
            .unwrap_or_default();
        // A command is migrated through `cgroup.procs`; `domain threaded`
        // cannot accept that migration.  Do not mistake directory creation
        // for a usable process-cgroup boundary.
        if probe_kind != "domain" {
            let _ = std::fs::remove_dir(&probe);
            return None;
        }

        let mut enabled = read_subtree_control(&candidate).unwrap_or_default();

        // Try to delegate the controllers we need into this ancestor.
        for c in &needed {
            if available.iter().any(|e| e == c)
                && !enabled.iter().any(|e| e == c)
                && write_cgroup_file(candidate.join("cgroup.subtree_control"), format!("+{c}\n"))
                    .is_ok()
            {
                enabled.push(c.to_string());
            }
        }

        // Only claim a controller is usable if the probe cgroup's limit
        // file can actually be written. This prevents silent fallback to
        // wall time when a controller is listed in `cgroup.subtree_control`
        // but not effective for child cgroups.
        let mut usable = Vec::new();
        for c in &needed {
            if !enabled.iter().any(|e| e == c) {
                continue;
            }
            let ok = match *c {
                // memory.swap.max is optional; its absence just means we
                // cannot disable swap, but memory.max may still be effective.
                "memory" => write_cgroup_file(probe.join("memory.max"), "max\n").is_ok(),
                "pids" => write_cgroup_file(probe.join("pids.max"), "max\n").is_ok(),
                "cpu" => write_cgroup_file(probe.join("cpu.max"), "max 100000\n").is_ok(),
                _ => false,
            };
            if ok {
                usable.push(c.to_string());
            }
        }

        let _ = std::fs::remove_dir(&probe);

        Some(Self {
            base: candidate,
            controllers: usable,
        })
    }

    fn supports(&self, controller: &str) -> bool {
        self.controllers.iter().any(|c| c == controller)
    }
}

impl QuotaBoundary for CgroupV2Boundary {
    fn apply(
        &self,
        command: &mut Command,
        policy: &ResourceQuotaPolicy,
        execution_id: &str,
    ) -> Result<AppliedQuota, ToolError> {
        let cgroup_path = self.base.join(format!(
            "{QUOTA_CGROUP_PREFIX}-{execution_id}-{}",
            Uuid::now_v7()
        ));
        if cgroup_path.is_dir() {
            let _ = std::fs::remove_dir(&cgroup_path);
        }
        std::fs::create_dir(&cgroup_path).map_err(|error| {
            ToolError::Protocol(format!(
                "failed to create cgroup {}: {error}",
                cgroup_path.display()
            ))
        })?;

        let mut skips = Vec::new();

        if let Some(memory) = policy.memory_bytes {
            if self.supports("memory") {
                write_cgroup_file(cgroup_path.join("memory.max"), memory.to_string())
                    .map_err(ToolError::Io)?;
                // Disable swap so the hard limit cannot be bypassed by
                // swapping. Ignore errors if swap accounting is unavailable.
                let _ = write_cgroup_file(cgroup_path.join("memory.swap.max"), "0");
            } else {
                skips.push(HostFeatureSkip {
                    feature: "memory",
                    reason: "memory controller not delegated to current cgroup".to_owned(),
                });
            }
        }

        if let Some(pids) = policy.pids_max {
            if self.supports("pids") {
                write_cgroup_file(cgroup_path.join("pids.max"), pids.to_string())
                    .map_err(ToolError::Io)?;
            } else {
                skips.push(HostFeatureSkip {
                    feature: "pids",
                    reason: "pids controller not delegated to current cgroup".to_owned(),
                });
            }
        }

        if let Some(throttle) = policy.cpu_throttle_us_per_100ms {
            if self.supports("cpu") {
                // 100ms period; quota is microseconds per period.
                write_cgroup_file(cgroup_path.join("cpu.max"), format!("{throttle} 100000"))
                    .map_err(ToolError::Io)?;
            } else {
                skips.push(HostFeatureSkip {
                    feature: "cpu_throttle",
                    reason: "cpu controller not delegated to current cgroup".to_owned(),
                });
            }
        }

        let cpu_time = policy.cpu_time_seconds;
        let memory_fallback = policy.memory_bytes.filter(|_| !self.supports("memory"));
        let disk_bytes = policy.disk_bytes;

        // Pre-allocate the NUL-terminated cgroup.procs path before fork so the
        // pre_exec hook can migrate itself using only raw syscalls and stack
        // buffers. The path is intentionally leaked; the child either execs or
        // exits and the kernel reclaims the memory.
        let mut cgroup_procs_bytes = Vec::with_capacity(
            cgroup_path.as_os_str().as_encoded_bytes().len() + b"/cgroup.procs".len() + 1,
        );
        cgroup_procs_bytes.extend_from_slice(cgroup_path.as_os_str().as_encoded_bytes());
        cgroup_procs_bytes.extend_from_slice(b"/cgroup.procs");
        cgroup_procs_bytes.push(0);
        let cgroup_procs_path: &'static [u8] = &*Box::leak(cgroup_procs_bytes.into_boxed_slice());

        unsafe {
            command.pre_exec(move || {
                if let Some(seconds) = cpu_time {
                    set_rlimit(libc::RLIMIT_CPU, seconds)?;
                }
                if let Some(bytes) = memory_fallback {
                    set_rlimit(libc::RLIMIT_AS, bytes)?;
                }
                if let Some(bytes) = disk_bytes {
                    // Per-process max file size. This is a low-trust harness
                    // approximation of a per-command disk-byte limit.
                    set_rlimit(libc::RLIMIT_FSIZE, bytes)?;
                }
                migrate_self_to_cgroup(cgroup_procs_path)
            });
        }

        if policy.disk_inodes.is_some() {
            // Inodes cannot be enforced by rlimit; the injected disk quota
            // backend (or real project quota) is required.
            if policy.disk_quota_backend.is_none() {
                skips.push(HostFeatureSkip {
                    feature: "disk_inodes",
                    reason: "disk inode quota requires project/filesystem-level enforcement or an injected disk quota backend".to_owned(),
                });
            }
        }

        Ok(AppliedQuota {
            skipped_features: skips,
            cgroup: Some(CgroupContext {
                path: cgroup_path,
                policy: policy.clone(),
            }),
        })
    }
}

/// Low-trust fallback boundary that uses `setrlimit` in a `pre_exec` hook.
struct LowTrustBoundary;

impl QuotaBoundary for LowTrustBoundary {
    fn apply(
        &self,
        command: &mut Command,
        policy: &ResourceQuotaPolicy,
        _execution_id: &str,
    ) -> Result<AppliedQuota, ToolError> {
        let cpu_time = policy.cpu_time_seconds;
        let memory = policy.memory_bytes;
        let disk_bytes = policy.disk_bytes;
        let mut skips = Vec::new();

        if policy.pids_max.is_some() {
            skips.push(HostFeatureSkip {
                feature: "pids",
                reason: "pids limit requires cgroup pids controller".to_owned(),
            });
        }
        if policy.cpu_throttle_us_per_100ms.is_some() {
            skips.push(HostFeatureSkip {
                feature: "cpu_throttle",
                reason: "cpu throttle requires cgroup cpu controller".to_owned(),
            });
        }
        if policy.disk_inodes.is_some() {
            skips.push(HostFeatureSkip {
                feature: "disk_inodes",
                reason: "disk inode quota requires project/filesystem-level enforcement or an injected disk quota backend".to_owned(),
            });
        }

        unsafe {
            command.pre_exec(move || {
                if let Some(seconds) = cpu_time {
                    set_rlimit(libc::RLIMIT_CPU, seconds)?;
                }
                if let Some(bytes) = memory {
                    set_rlimit(libc::RLIMIT_AS, bytes)?;
                }
                if let Some(bytes) = disk_bytes {
                    set_rlimit(libc::RLIMIT_FSIZE, bytes)?;
                }
                Ok(())
            });
        }

        Ok(AppliedQuota {
            skipped_features: skips,
            cgroup: None,
        })
    }
}

fn set_rlimit(resource: libc::__rlimit_resource_t, value: u64) -> std::io::Result<()> {
    // Set the hard limit one unit above the soft limit so the kernel delivers
    // the typed signal (SIGXCPU / SIGXFSZ) at the soft limit instead of
    // SIGKILL when the process has not installed a handler.
    let max = value.saturating_add(1);
    let limit = libc::rlimit {
        rlim_cur: value,
        rlim_max: max,
    };
    let rc = unsafe { libc::setrlimit(resource, &limit) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn migrate_self_to_cgroup(path: &'static [u8]) -> std::io::Result<()> {
    debug_assert_eq!(
        path.last(),
        Some(&0),
        "cgroup.procs path must be NUL-terminated"
    );

    // Use raw syscalls in this pre_exec hook; libc wrappers such as open(2)
    // are not async-signal-safe and must not be called between fork and exec
    // in a multithreaded process.
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat,
            libc::AT_FDCWD,
            path.as_ptr() as *const libc::c_char,
            libc::O_WRONLY,
            0,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let fd = fd as libc::c_int;

    let mut payload = [0u8; 32];
    let len = write_pid_to_buffer(&mut payload, std::process::id());
    let mut written = 0;
    while written < len {
        let n = unsafe {
            libc::syscall(
                libc::SYS_write,
                fd,
                payload.as_ptr().add(written) as *const libc::c_void,
                len - written,
            )
        };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            let _ = unsafe { libc::syscall(libc::SYS_close, fd) };
            return Err(err);
        }
        written += n as usize;
    }

    let _ = unsafe { libc::syscall(libc::SYS_close, fd) };
    Ok(())
}

fn write_pid_to_buffer(buf: &mut [u8; 32], pid: u32) -> usize {
    debug_assert!(
        buf.len() >= 12,
        "PID buffer must fit 10 digits and a newline"
    );
    let mut n = pid;
    let mut digits = [0u8; 10];
    let mut count = 0;
    if n == 0 {
        digits[0] = b'0';
        count = 1;
    } else {
        while n > 0 {
            digits[count] = b'0' + (n % 10) as u8;
            count += 1;
            n /= 10;
        }
    }
    let mut pos = 0;
    for i in (0..count).rev() {
        buf[pos] = digits[i];
        pos += 1;
    }
    buf[pos] = b'\n';
    pos + 1
}

fn write_cgroup_file(path: PathBuf, value: impl AsRef<[u8]>) -> std::io::Result<()> {
    let mut file = OpenOptions::new().write(true).open(&path)?;
    file.write_all(value.as_ref())?;
    Ok(())
}

fn current_cgroup_path() -> Option<PathBuf> {
    let contents = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    for line in contents.lines() {
        let mut parts = line.splitn(3, ':');
        let _hierarchy = parts.next()?;
        let controllers = parts.next()?;
        let path = parts.next()?;
        // v2 unified hierarchy has empty controllers field.
        if controllers.is_empty() {
            return Some(PathBuf::from("/sys/fs/cgroup").join(path.trim_start_matches('/')));
        }
    }
    None
}

fn env_cgroup_base() -> Option<PathBuf> {
    std::env::var_os("SUMI_EXECUTOR_CGROUP_BASE").map(PathBuf::from)
}

fn read_controllers(path: &Path) -> Result<Vec<String>, std::io::Error> {
    let contents = std::fs::read_to_string(path.join("cgroup.controllers"))?;
    Ok(contents.split_whitespace().map(str::to_owned).collect())
}

fn read_subtree_control(path: &Path) -> Result<Vec<String>, std::io::Error> {
    let contents = std::fs::read_to_string(path.join("cgroup.subtree_control"))?;
    Ok(contents.split_whitespace().map(str::to_owned).collect())
}

fn read_cgroup_type(path: &Path) -> Result<String, std::io::Error> {
    std::fs::read_to_string(path.join("cgroup.type"))
}

#[derive(Debug, Default)]
struct PidsEvents {
    max: u64,
}

fn read_pids_events(path: &Path) -> Option<PidsEvents> {
    let contents = std::fs::read_to_string(path.join("pids.events")).ok()?;
    let mut events = PidsEvents::default();
    for line in contents.lines() {
        let mut parts = line.split_whitespace();
        if let Some(key) = parts.next()
            && key == "max"
        {
            events.max = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
        }
    }
    Some(events)
}

#[derive(Debug, Default)]
struct MemoryEvents {
    oom: u64,
    oom_kill: u64,
}

fn read_memory_events(path: &Path) -> Option<MemoryEvents> {
    let contents = std::fs::read_to_string(path.join("memory.events")).ok()?;
    let mut events = MemoryEvents::default();
    for line in contents.lines() {
        let mut parts = line.split_whitespace();
        if let Some(key) = parts.next() {
            match key {
                "oom" => events.oom = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0),
                "oom_kill" => {
                    events.oom_kill = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
                }
                _ => {}
            }
        }
    }
    Some(events)
}

#[derive(Debug, Default)]
struct CpuStat {
    usage_usec: u64,
    nr_throttled: u64,
    throttled_usec: u64,
}

fn read_cpu_stat(path: &Path) -> Option<CpuStat> {
    let contents = std::fs::read_to_string(path.join("cpu.stat")).ok()?;
    let mut stat = CpuStat::default();
    for line in contents.lines() {
        let mut parts = line.split_whitespace();
        if let Some(key) = parts.next() {
            let value = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
            match key {
                "usage_usec" => stat.usage_usec = value,
                "nr_throttled" => stat.nr_throttled = value,
                "throttled_usec" => stat.throttled_usec = value,
                _ => {}
            }
        }
    }
    Some(stat)
}

#[derive(Debug)]
struct CgroupContext {
    path: PathBuf,
    policy: ResourceQuotaPolicy,
}

impl CgroupContext {
    fn destroy(&self) -> std::io::Result<()> {
        if !self.path.is_dir() {
            return Ok(());
        }
        let kill_file = self.path.join("cgroup.kill");
        if kill_file.exists() {
            let _ = write_cgroup_file(kill_file, b"1\n");
        }
        // Wait briefly for the cgroup to become empty. We cannot remove a
        // cgroup that still has live processes.
        let procs = self.path.join("cgroup.procs");
        let deadline = std::time::Instant::now() + Duration::from_millis(500);
        while std::time::Instant::now() < deadline {
            if std::fs::read_to_string(&procs)
                .map(|c| c.trim().is_empty())
                .unwrap_or(false)
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        std::fs::remove_dir(&self.path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_preserves_wall_and_output_defaults() {
        let policy = ResourceQuotaPolicy::default();
        assert_eq!(policy.wall_time, Duration::from_secs(120));
        assert_eq!(policy.output_bytes, 10 * 1024 * 1024);
        assert_eq!(policy.memory_bytes, Some(512 * 1024 * 1024));
        assert_eq!(policy.cpu_time_seconds, Some(120));
        assert_eq!(policy.pids_max, Some(64));
    }

    #[test]
    fn builder_chaining_sets_limits() {
        let policy = ResourceQuotaPolicy::new()
            .with_wall_time(30)
            .with_output_bytes(1024)
            .with_memory_bytes(1024 * 1024)
            .with_cpu_time_seconds(10)
            .with_cpu_throttle_percent(50)
            .with_pids_max(64);
        assert_eq!(policy.wall_time, Duration::from_secs(30));
        assert_eq!(policy.output_bytes, 1024);
        assert_eq!(policy.memory_bytes, Some(1024 * 1024));
        assert_eq!(policy.cpu_time_seconds, Some(10));
        assert_eq!(policy.cpu_throttle_us_per_100ms, Some(50_000));
        assert_eq!(policy.pids_max, Some(64));
    }

    #[test]
    fn limit_for_breach_typed_variants() {
        assert_eq!(
            limit_for_breach(QuotaKind::Memory, 0, 1024),
            ResourceLimit::Memory { limit: 1024 }
        );
        assert_eq!(
            limit_for_breach(QuotaKind::CpuTime, 0, 5),
            ResourceLimit::CpuTime { limit_seconds: 5 }
        );
        assert_eq!(
            limit_for_breach(QuotaKind::CpuThrottle, 0, 10000),
            ResourceLimit::CpuThrottle { limit: 10000 }
        );
    }

    #[test]
    fn current_cgroup_path_parses_v2_format() {
        let path = current_cgroup_path();
        // On a cgroup-v2 host this should return a path; on non-Linux or
        // v1-only hosts it may return None.
        if let Some(path) = path {
            assert!(path.starts_with("/sys/fs/cgroup"));
        }
    }

    #[test]
    fn probe_cgroup_detects_environment_support() {
        let boundary = CgroupV2Boundary::probe();
        if boundary.is_none() {
            eprintln!(
                "cgroup-v2 boundary not available in this environment; low-trust rlimit fallback will be used"
            );
        }
    }

    #[test]
    fn in_memory_disk_quota_enforces_bytes_and_inodes() {
        let quota = InMemoryDiskQuota::new(100, 2);
        assert!(quota.check(50, 1).is_ok());
        quota.commit(50, 1);
        assert!(matches!(
            quota.check(60, 0),
            Err(ResourceLimit::DiskBytes { .. })
        ));
        assert!(matches!(
            quota.check(0, 2),
            Err(ResourceLimit::DiskInodes { .. })
        ));
        quota.rollback(50, 1);
        assert!(quota.check(50, 1).is_ok());
    }

    #[test]
    fn policy_fail_closed_without_disk_backend() {
        let policy = ResourceQuotaPolicy::new().with_disk_bytes(1024);
        let mut command = Command::new("true");
        assert!(policy.apply_to_command(&mut command, "exec-1").is_err());
    }

    #[test]
    fn set_rlimit_rejects_invalid_resource() {
        // An out-of-range resource is returned as an OS error; the cgroup
        // pre_exec hook now propagates such errors with `?` instead of `let _ = ...`.
        let invalid = 9_999 as libc::__rlimit_resource_t;
        assert!(set_rlimit(invalid, 1).is_err());
    }

    #[test]
    fn migrate_self_to_cgroup_returns_error_for_missing_path() {
        // The pre_exec cgroup migration uses raw syscalls and must fail closed
        // when the cgroup.procs path is not available instead of aborting.
        let mut bytes = std::fs::canonicalize("/")
            .unwrap_or_else(|_| std::path::PathBuf::from("/"))
            .as_os_str()
            .as_encoded_bytes()
            .to_vec();
        bytes.extend_from_slice(b"/sumi-test-missing-cgroup-XXXXXX/cgroup.procs\0");
        let leaked: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        assert!(migrate_self_to_cgroup(leaked).is_err());
    }

    #[test]
    fn write_pid_to_buffer_formats_with_newline() {
        let mut buf = [0u8; 32];
        let len = write_pid_to_buffer(&mut buf, 12_345);
        assert_eq!(&buf[..len], b"12345\n");
        let len = write_pid_to_buffer(&mut buf, 0);
        assert_eq!(&buf[..len], b"0\n");
        let len = write_pid_to_buffer(&mut buf, 4_294_967_295);
        assert_eq!(&buf[..len], b"4294967295\n");
    }
}
