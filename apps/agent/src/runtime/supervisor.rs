//! T27 deployment-supervisor cgroup integration.
//!
//! The bash supervisor prepares a generation-scoped cgroup base directory and
//! scans sibling directories for stale generations to kill/reap before it
//! spawns a new runtime/executor pair.

use std::{
    fmt::Write as _,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

const CGROUP_BASE_PREFIX: &str = "sumi-agent";
const CGROUP_DELEGATED_PREFIX: &str = "sumi-agent-delegated";
const CGROUP_SUFFIX_GENERATION: &str = "-g";
const SUMI_CGROUP_PARENT_ENV: &str = "SUMI_CGROUP_PARENT";
const REAP_DEADLINE: Duration = Duration::from_secs(2);
/// All command children share this parent.  Each child is separately capped
/// at one vCPU by `ResourceQuotaPolicy`; this parent prevents four admitted
/// children from consuming more than two vCPUs in aggregate.
const EXECUTOR_AGGREGATE_CPU_MAX: &str = "200000 100000";

fn delegated_parent_name(tenant_id: &str, agent_id: &str, conversation_id: &str) -> String {
    let mut hasher = Sha256::new();
    for id in [tenant_id, agent_id, conversation_id] {
        hasher.update((id.len() as u64).to_be_bytes());
        hasher.update(id.as_bytes());
    }
    let digest = hasher.finalize();
    let mut identity_tag = String::with_capacity(24);
    for byte in &digest[..12] {
        write!(&mut identity_tag, "{byte:02x}").expect("writing to String cannot fail");
    }
    format!(
        "{CGROUP_DELEGATED_PREFIX}-{}-{}-{}-{identity_tag}",
        sanitize(tenant_id),
        sanitize(agent_id),
        sanitize(conversation_id),
    )
}

fn cgroup_parent_from_env_or_find(
    tenant_id: &str,
    agent_id: &str,
    conversation_id: &str,
) -> Result<PathBuf> {
    if let Some(parent) = std::env::var_os(SUMI_CGROUP_PARENT_ENV) {
        let parent = PathBuf::from(&parent);
        if !parent.is_absolute() {
            bail!(
                "{} must be an absolute path, got {}",
                SUMI_CGROUP_PARENT_ENV,
                parent.display()
            );
        }
        if !parent.is_dir() {
            bail!(
                "{} does not exist or is not a directory: {}",
                SUMI_CGROUP_PARENT_ENV,
                parent.display()
            );
        }
        let kind = read_cgroup_type(&parent).unwrap_or_default();
        let is_root = parent.as_os_str() == "/sys/fs/cgroup";
        if !is_root && kind != "domain" {
            bail!(
                "{} must be a domain cgroup, got '{kind}' in {}",
                SUMI_CGROUP_PARENT_ENV,
                parent.display()
            );
        }
        return Ok(parent);
    }

    let ancestor = find_domain_cgroup_ancestor()?;
    Ok(ancestor.join(delegated_parent_name(tenant_id, agent_id, conversation_id)))
}

fn prepare_cgroup_parent(
    tenant_id: &str,
    agent_id: &str,
    conversation_id: &str,
) -> Result<PathBuf> {
    let parent = cgroup_parent_from_env_or_find(tenant_id, agent_id, conversation_id)?;

    if !parent.exists() {
        std::fs::create_dir(&parent).with_context(|| {
            format!(
                "failed to create delegated cgroup parent {}",
                parent.display()
            )
        })?;
    }

    for controller in ["cpu", "memory", "pids"] {
        enable_subtree_controller(&parent, controller).with_context(|| {
            format!(
                "required controller {controller} could not be delegated in {}",
                parent.display()
            )
        })?;
    }
    if let Err(error) = enable_subtree_controller(&parent, "io") {
        tracing::warn!(
            path = %parent.display(),
            %error,
            "optional io controller could not be delegated"
        );
    }

    Ok(parent)
}

/// Prepare a per-generation executor cgroup base directory under a
/// generation-scoped delegated parent. The returned path is suitable for
/// `SUMI_EXECUTOR_CGROUP_BASE`.
///
/// The base directory is named
/// `sumi-agent-<tenant>-<agent>-<conversation>-<identity-tag>-g<generation>`.
/// The readable identities are sanitized, while the hash-derived identity tag
/// prevents lossy sanitization collisions. The stable prefix lets
/// `scan_and_kill_stale` discover sibling command cgroups.
pub fn prepare_cgroup_base(
    tenant_id: &str,
    agent_id: &str,
    conversation_id: &str,
    generation: u64,
) -> Result<PathBuf> {
    let parent = prepare_cgroup_parent(tenant_id, agent_id, conversation_id)?;
    let base = parent.join(cgroup_base_name(
        tenant_id,
        agent_id,
        conversation_id,
        generation,
    ));

    if !base.exists() {
        std::fs::create_dir(&base).with_context(|| {
            format!("failed to create cgroup base directory {}", base.display())
        })?;
    }

    // Delegate the controllers the executor may need for command cgroups.
    for controller in ["cpu", "memory", "pids"] {
        enable_subtree_controller(&base, controller).with_context(|| {
            format!(
                "required controller {controller} could not be delegated in {}",
                base.display()
            )
        })?;
    }
    if let Err(error) = enable_subtree_controller(&base, "io") {
        tracing::warn!(
            path = %base.display(),
            %error,
            "optional io controller could not be delegated"
        );
    }

    write_cgroup_value(&base.join("cpu.max"), EXECUTOR_AGGREGATE_CPU_MAX).with_context(|| {
        format!(
            "failed to apply aggregate 2-vCPU executor limit in {}",
            base.display()
        )
    })?;

    Ok(base)
}

fn write_cgroup_value(path: &Path, value: &str) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .open(path)
        .with_context(|| format!("failed to open cgroup control {}", path.display()))?;
    file.write_all(value.as_bytes())
        .with_context(|| format!("failed to write cgroup control {}", path.display()))?;
    Ok(())
}

/// Prepare a per-generation service cgroup base directory under the delegated
/// parent. The returned path is suitable for `SUMI_SERVICE_CGROUP_BASE`.
///
/// This leaf cgroup holds the supervisor, runtime, executor, and artifact-broker
/// processes for the generation. It is distinct from the executor command
/// cgroup base because a cgroup that contains processes cannot have children
/// under cgroup v2.
pub fn prepare_service_cgroup_base(
    tenant_id: &str,
    agent_id: &str,
    conversation_id: &str,
    generation: u64,
) -> Result<PathBuf> {
    let parent = prepare_cgroup_parent(tenant_id, agent_id, conversation_id)?;
    let base = parent.join(cgroup_base_name_with_suffix(
        tenant_id,
        agent_id,
        conversation_id,
        generation,
        Some("services"),
    ));

    if !base.exists() {
        std::fs::create_dir(&base).with_context(|| {
            format!(
                "failed to create service cgroup base directory {}",
                base.display()
            )
        })?;
    }

    Ok(base)
}

/// Find sibling base directories for other generations and kill/reap them.
/// Returns the paths that were removed.
pub fn scan_and_kill_stale(base_dir: &Path, current_generation: u64) -> Result<Vec<PathBuf>> {
    let parent = base_dir.parent().context("cgroup base has no parent")?;
    let name = base_dir
        .file_name()
        .and_then(|n| n.to_str())
        .context("cgroup base has no name")?;
    let prefix = name
        .rsplit_once(CGROUP_SUFFIX_GENERATION)
        .map(|(p, _)| p)
        .context("cgroup base name is missing generation suffix")?;

    let mut removed = Vec::new();
    let mut failures = Vec::new();
    for entry in std::fs::read_dir(parent)
        .with_context(|| format!("failed to read cgroup parent {}", parent.display()))?
    {
        let entry = entry.with_context(|| "failed to read cgroup directory entry")?;
        let path = entry.path();
        let Some(dir_name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !dir_name.starts_with(prefix) || path == base_dir {
            continue;
        }
        let Some(suffix) = dir_name
            .rsplit_once(CGROUP_SUFFIX_GENERATION)
            .map(|(_, s)| s)
        else {
            continue;
        };
        let stale_generation: u64 = suffix.parse().with_context(|| {
            format!("cgroup directory {dir_name} has an invalid generation suffix")
        })?;
        if stale_generation == current_generation {
            continue;
        }
        if stale_generation > current_generation {
            // A stale supervisor must never fence a newer generation. The
            // allocator is monotonic, so this is an ownership violation, not
            // a stale boundary to clean up.
            bail!(
                "refusing to reap newer generation {stale_generation} while starting {current_generation}"
            );
        }

        match kill_and_remove_cgroup(&path) {
            Ok(()) => removed.push(path),
            Err(error) => failures.push(format!("{}: {error:#}", path.display())),
        }
    }

    if !failures.is_empty() {
        bail!(
            "failed to reap {} stale cgroup(s) after scanning all candidates: {}",
            failures.len(),
            failures.join("; ")
        );
    }

    Ok(removed)
}

fn cgroup_base_name(
    tenant_id: &str,
    agent_id: &str,
    conversation_id: &str,
    generation: u64,
) -> String {
    cgroup_base_name_with_suffix(tenant_id, agent_id, conversation_id, generation, None)
}

fn cgroup_base_name_with_suffix(
    tenant_id: &str,
    agent_id: &str,
    conversation_id: &str,
    generation: u64,
    suffix: Option<&str>,
) -> String {
    // Sanitization alone is lossy (`a/b` and `a-b` collide) and would let one
    // conversation's recovery scan reap another's cgroup. Keep a readable
    // prefix while binding the directory name to the exact raw identities.
    let mut hasher = Sha256::new();
    for id in [tenant_id, agent_id, conversation_id] {
        hasher.update((id.len() as u64).to_be_bytes());
        hasher.update(id.as_bytes());
    }
    let digest = hasher.finalize();
    let mut identity_tag = String::with_capacity(24);
    for byte in &digest[..12] {
        write!(&mut identity_tag, "{byte:02x}").expect("writing to String cannot fail");
    }
    let middle = suffix.map(|s| format!("-{s}")).unwrap_or_default();
    format!(
        "{CGROUP_BASE_PREFIX}-{}-{}-{}-{identity_tag}{middle}{CGROUP_SUFFIX_GENERATION}{}",
        sanitize(tenant_id),
        sanitize(agent_id),
        sanitize(conversation_id),
        generation
    )
}

fn sanitize(id: &str) -> String {
    id.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c,
            _ => '-',
        })
        .collect()
}

fn find_domain_cgroup_ancestor() -> Result<PathBuf> {
    let mut candidate = current_cgroup_path()?;
    loop {
        // The root cgroup does not always expose `cgroup.type`; rely on the
        // probe child to prove we can create process cgroups here.
        let is_root = candidate.as_os_str() == "/sys/fs/cgroup";
        let kind = if is_root {
            String::new()
        } else {
            read_cgroup_type(&candidate).unwrap_or_default()
        };

        let can_host_processes = kind.is_empty() || kind == "domain";
        if can_host_processes {
            let probe = candidate.join(format!(
                "sumi-supervisor-probe-{}-{}",
                std::process::id(),
                uuid::Uuid::now_v7()
            ));
            if std::fs::create_dir(&probe).is_ok() {
                // Creating a child is not enough: a `domain threaded` or
                // `domain invalid` parent can create children that refuse
                // process migrations. Only accept if the probe child itself
                // can hold processes.
                let probe_kind = read_cgroup_type(&probe).unwrap_or_default();
                let probe_ok = probe_kind == "domain";
                let _ = std::fs::remove_dir(&probe);
                if probe_ok {
                    return Ok(candidate);
                }
            }
        }

        if is_root {
            bail!("no writable domain cgroup ancestor found");
        }
        candidate = candidate
            .parent()
            .context("reached cgroup root without finding a writable domain ancestor")?
            .to_path_buf();
    }
}

fn current_cgroup_path() -> Result<PathBuf> {
    let contents =
        std::fs::read_to_string("/proc/self/cgroup").context("failed to read /proc/self/cgroup")?;
    for line in contents.lines() {
        let mut parts = line.splitn(3, ':');
        let _ = parts.next();
        let controllers = parts.next().context("malformed /proc/self/cgroup")?;
        let path = parts.next().context("malformed /proc/self/cgroup")?;
        if controllers.is_empty() {
            return Ok(PathBuf::from("/sys/fs/cgroup").join(path.trim_start_matches('/')));
        }
    }
    bail!("cgroup v2 unified hierarchy not found")
}

fn read_cgroup_type(path: &Path) -> Result<String> {
    let kind = std::fs::read_to_string(path.join("cgroup.type"))
        .with_context(|| format!("failed to read cgroup.type for {}", path.display()))?;
    Ok(kind.trim().to_owned())
}

fn enable_subtree_controller(path: &Path, controller: &str) -> Result<()> {
    let available = read_controllers(path)?;
    if !available.iter().any(|c| c == controller) {
        bail!(
            "controller {controller} is not available in {}",
            path.display()
        );
    }
    let mut file = OpenOptions::new()
        .write(true)
        .open(path.join("cgroup.subtree_control"))
        .with_context(|| {
            format!(
                "failed to open cgroup.subtree_control for {}",
                path.display()
            )
        })?;
    file.write_all(format!("+{controller}\n").as_bytes())
        .with_context(|| format!("failed to enable {controller} in {}", path.display()))?;
    Ok(())
}

fn read_controllers(path: &Path) -> Result<Vec<String>> {
    let contents = std::fs::read_to_string(path.join("cgroup.controllers"))
        .with_context(|| format!("failed to read cgroup.controllers for {}", path.display()))?;
    Ok(contents.split_whitespace().map(str::to_owned).collect())
}

fn cgroup_procs_empty(path: &Path) -> bool {
    let procs = path.join("cgroup.procs");
    std::fs::read_to_string(&procs)
        .map(|c| c.trim().is_empty())
        .unwrap_or(false)
}

fn cgroup_events_populated_zero(path: &Path) -> bool {
    let events = path.join("cgroup.events");
    let Ok(contents) = std::fs::read_to_string(&events) else {
        return false;
    };
    contents
        .split_whitespace()
        .collect::<Vec<_>>()
        .windows(2)
        .any(|w| w[0] == "populated" && w[1] == "0")
}

fn wait_cgroup_empty(path: &Path, deadline: Instant) -> Result<()> {
    while Instant::now() < deadline {
        if cgroup_procs_empty(path) && cgroup_events_populated_zero(path) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    bail!(
        "cgroup {} still contains processes after reap deadline",
        path.display()
    )
}

/// Compute the cgroup base path for a generation without creating it.
///
/// This is the same path `prepare_cgroup_base` would create, but it does not
/// probe for writability or enable controllers. It is used by T27 to locate
/// the exact generation cgroup that must be empty before a physical recovery
/// receipt can be persisted.
#[cfg(test)]
pub(crate) fn cgroup_base_for(
    tenant_id: &str,
    agent_id: &str,
    conversation_id: &str,
    generation: u64,
) -> Result<PathBuf> {
    let parent = cgroup_parent_from_env_or_find(tenant_id, agent_id, conversation_id)?;
    let name = cgroup_base_name(tenant_id, agent_id, conversation_id, generation);
    Ok(parent.join(name))
}

/// Kill every process in a stale generation cgroup and wait for both
/// `cgroup.procs` and `cgroup.events` `populated 0` before returning.
///
/// This is the race-free primitive used by both the supervisor recovery scan
/// and T27 physical recovery. It does not enumerate PIDs in `/proc`.
pub(crate) fn kill_and_remove_cgroup(path: &Path) -> Result<()> {
    let kill_file = path.join("cgroup.kill");
    if kill_file.exists() {
        let mut file = OpenOptions::new()
            .write(true)
            .open(&kill_file)
            .with_context(|| format!("failed to open {}", kill_file.display()))?;
        file.write_all(b"1")
            .with_context(|| format!("failed to write to {}", kill_file.display()))?;
        file.sync_all().ok();
    } else {
        bail!(
            "cgroup {} does not expose cgroup.kill; cannot perform race-free kill/reap",
            path.display()
        );
    }

    let deadline = Instant::now() + REAP_DEADLINE;
    wait_cgroup_empty(path, deadline)?;

    remove_cgroup_tree(path)
        .with_context(|| format!("failed to remove cgroup tree {}", path.display()))?;
    Ok(())
}

fn remove_cgroup_tree(path: &Path) -> Result<()> {
    for entry in std::fs::read_dir(path)
        .with_context(|| format!("failed to read cgroup directory {}", path.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read entry in {}", path.display()))?;
        if entry.file_type().is_ok_and(|t| t.is_dir()) {
            remove_cgroup_tree(&entry.path())?;
        }
    }

    if !cgroup_procs_empty(path) || !cgroup_events_populated_zero(path) {
        bail!(
            "cgroup {} still contains processes after reap; refusing to remove",
            path.display()
        );
    }

    std::fs::remove_dir(path)
        .with_context(|| format!("failed to remove cgroup directory {}", path.display()))?;
    Ok(())
}

/// Scan sibling cgroup directories of `cgroup_base` for stale generations and
/// kill/reap them. Returns the paths that were removed.
///
/// Unlike `/proc` enumeration, this uses cgroup ownership: every Sumi service
/// and command process for a generation is expected to live under the
/// generation-scoped base directory. A newer generation sibling is an
/// ownership violation and causes the scan to fail closed.
pub fn scan_and_kill_stale_services(
    cgroup_base: &Path,
    current_generation: u64,
) -> Result<Vec<PathBuf>> {
    scan_and_kill_stale(cgroup_base, current_generation)
}

#[cfg(test)]
mod tests {
    use std::process::Stdio;

    use super::*;

    fn spawn_in_cgroup(base: &Path, command: &str) -> tokio::process::Child {
        let procs = base.join("cgroup.procs");
        let shell = format!("echo $$ > {} && {}", procs.display(), command);
        tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&shell)
            .process_group(0)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn child in cgroup")
    }

    #[test]
    fn sanitize_replaces_special_characters() {
        assert_eq!(sanitize("foo/bar baz"), "foo-bar-baz");
    }

    #[test]
    fn cgroup_base_name_includes_generation_suffix() {
        let name = cgroup_base_name("tenant/1", "agent 2", "conv-3", 7);
        assert!(name.starts_with("sumi-agent-tenant-1-agent-2-conv-3-"));
        assert!(name.ends_with("-g7"));
    }

    #[test]
    fn cgroup_base_name_binds_raw_identity_without_sanitization_collisions() {
        assert_ne!(
            cgroup_base_name("tenant/a", "agent", "conversation", 7),
            cgroup_base_name("tenant-a", "agent", "conversation", 7),
        );
    }

    #[test]
    fn cgroup_base_for_matches_prepare_cgroup_base() {
        let tenant = "t-base-for";
        let agent = "a-base-for";
        let conversation = "c-base-for";

        let Ok(prepared) = prepare_cgroup_base(tenant, agent, conversation, 3) else {
            eprintln!("skipping: writable domain cgroup unavailable in this test environment");
            return;
        };
        let computed = cgroup_base_for(tenant, agent, conversation, 3).expect("compute base");
        assert_eq!(prepared, computed);
        let _ = std::fs::remove_dir(&prepared);
    }

    #[test]
    fn cgroup_events_populated_zero_reports_empty_cgroup() {
        let tenant = "t-populated";
        let agent = "a-populated";
        let conversation = "c-populated";

        let Ok(base) = prepare_cgroup_base(tenant, agent, conversation, 1) else {
            eprintln!("skipping: writable domain cgroup unavailable in this test environment");
            return;
        };
        assert!(
            cgroup_events_populated_zero(&base),
            "empty cgroup must report populated 0"
        );
        let _ = std::fs::remove_dir(&base);
    }

    #[tokio::test]
    async fn kill_and_remove_cgroup_waits_for_populated_zero() {
        let tenant = "t-kill";
        let agent = "a-kill";
        let conversation = "c-kill";

        let Ok(stale_base) = prepare_cgroup_base(tenant, agent, conversation, 6) else {
            eprintln!("skipping: writable domain cgroup unavailable in this test environment");
            return;
        };

        let mut child = spawn_in_cgroup(&stale_base, "exec sleep 60");

        kill_and_remove_cgroup(&stale_base).expect("kill and remove stale cgroup");

        let waited = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
        assert!(waited.is_ok(), "stale child was not reaped");
    }

    #[tokio::test]
    async fn scan_and_kill_stale_services_reaps_stale_generation_cgroup() {
        let tenant = "t-svc";
        let agent = "a-svc";
        let conversation = "c-svc";

        let Ok(current_base) = prepare_cgroup_base(tenant, agent, conversation, 7) else {
            eprintln!("skipping: writable domain cgroup unavailable in this test environment");
            return;
        };
        let Ok(stale_base) = prepare_cgroup_base(tenant, agent, conversation, 6) else {
            let _ = std::fs::remove_dir(&current_base);
            eprintln!("skipping: cannot create a stale generation cgroup in this environment");
            return;
        };

        let mut child = spawn_in_cgroup(&stale_base, "exec sleep 60");

        let removed = scan_and_kill_stale_services(&current_base, 7).expect("scan stale services");
        assert!(removed.iter().any(|p| p == &stale_base));

        let waited = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
        assert!(waited.is_ok(), "stale generation child was not reaped");

        let _ = std::fs::remove_dir(&current_base);
    }

    #[tokio::test]
    async fn scan_and_kill_stale_services_rejects_newer_generation() {
        let tenant = "t-newer";
        let agent = "a-newer";
        let conversation = "c-newer";

        let Ok(current_base) = prepare_cgroup_base(tenant, agent, conversation, 7) else {
            eprintln!("skipping: writable domain cgroup unavailable in this test environment");
            return;
        };
        let Ok(newer_base) = prepare_cgroup_base(tenant, agent, conversation, 8) else {
            let _ = std::fs::remove_dir(&current_base);
            eprintln!("skipping: cannot create a newer generation cgroup in this environment");
            return;
        };

        let mut child = spawn_in_cgroup(&newer_base, "exec sleep 60");

        assert!(
            scan_and_kill_stale_services(&current_base, 7).is_err(),
            "must fail closed when a newer generation is running"
        );

        let _ = child.kill().await;
        let _ = std::fs::remove_dir(&newer_base);
        let _ = std::fs::remove_dir(&current_base);
    }

    #[tokio::test]
    async fn scan_and_kill_stale_services_reaps_multiple_descendants() {
        let tenant = "t-many";
        let agent = "a-many";
        let conversation = "c-many";

        let Ok(current_base) = prepare_cgroup_base(tenant, agent, conversation, 7) else {
            eprintln!("skipping: writable domain cgroup unavailable in this test environment");
            return;
        };
        let Ok(stale_base) = prepare_cgroup_base(tenant, agent, conversation, 6) else {
            let _ = std::fs::remove_dir(&current_base);
            eprintln!("skipping: cannot create a stale generation cgroup in this environment");
            return;
        };

        // Spawn several children that each migrate themselves into the stale
        // cgroup before exec'ing sleep. This avoids shell job-control issues
        // and exercises cgroup.kill for multiple distinct processes.
        let mut children = Vec::new();
        for _ in 0..3 {
            children.push(spawn_in_cgroup(&stale_base, "exec sleep 60"));
        }

        let removed = scan_and_kill_stale_services(&current_base, 7).expect("scan stale services");
        assert!(removed.iter().any(|p| p == &stale_base));

        for mut child in children {
            let waited = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
            assert!(waited.is_ok(), "stale generation descendant was not reaped");
        }

        let _ = std::fs::remove_dir(&current_base);
    }
}
