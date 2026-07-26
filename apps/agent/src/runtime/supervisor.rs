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
    time::Duration,
};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

const CGROUP_BASE_PREFIX: &str = "sumi-agent";
const CGROUP_SUFFIX_GENERATION: &str = "-g";
const REAP_DEADLINE: Duration = Duration::from_secs(2);

/// Prepare a per-generation cgroup base directory under the current process
/// cgroup. The returned path is suitable for `SUMI_EXECUTOR_CGROUP_BASE`.
///
/// The base directory is named `sumi-agent-<tenant>-<agent>-<conversation>-g<generation>`
/// so that stale siblings can be discovered by `scan_and_kill_stale`.
pub fn prepare_cgroup_base(
    tenant_id: &str,
    agent_id: &str,
    conversation_id: &str,
    generation: u64,
) -> Result<PathBuf> {
    let ancestor = find_domain_cgroup_ancestor()?;
    let name = cgroup_base_name(tenant_id, agent_id, conversation_id, generation);
    let base = ancestor.join(name);

    if !base.exists() {
        std::fs::create_dir(&base).with_context(|| {
            format!("failed to create cgroup base directory {}", base.display())
        })?;
    }

    // Delegate the controllers the executor may need for command cgroups.
    for controller in ["cpu", "memory", "pids", "io"] {
        let _ = enable_subtree_controller(&base, controller);
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

        kill_and_remove_cgroup(&path)
            .with_context(|| format!("failed to reap stale cgroup {}", path.display()))?;
        removed.push(path);
    }

    Ok(removed)
}

fn cgroup_base_name(
    tenant_id: &str,
    agent_id: &str,
    conversation_id: &str,
    generation: u64,
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
    format!(
        "{CGROUP_BASE_PREFIX}-{}-{}-{}-{identity_tag}{CGROUP_SUFFIX_GENERATION}{}",
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

fn kill_and_remove_cgroup(path: &Path) -> Result<()> {
    let kill_file = path.join("cgroup.kill");
    if kill_file.exists() {
        let mut file = OpenOptions::new()
            .write(true)
            .open(&kill_file)
            .with_context(|| format!("failed to open {}", kill_file.display()))?;
        file.write_all(b"1")
            .with_context(|| format!("failed to write to {}", kill_file.display()))?;
    }

    let procs = path.join("cgroup.procs");
    let deadline = std::time::Instant::now() + REAP_DEADLINE;
    while std::time::Instant::now() < deadline {
        if std::fs::read_to_string(&procs)
            .map(|c| c.trim().is_empty())
            .unwrap_or(false)
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    std::fs::remove_dir(path)
        .with_context(|| format!("failed to remove cgroup directory {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::process::Stdio;

    use super::*;

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

    #[tokio::test]
    async fn scan_and_kill_stale_reaps_other_generation_cgroups() {
        let tenant = "t-supervisor";
        let agent = "a-supervisor";
        let conversation = "c-supervisor";

        let Ok(current_base) = prepare_cgroup_base(tenant, agent, conversation, 7) else {
            eprintln!("skipping: writable domain cgroup unavailable in this test environment");
            return;
        };
        let Ok(stale_base) = prepare_cgroup_base(tenant, agent, conversation, 6) else {
            let _ = std::fs::remove_dir(&current_base);
            eprintln!("skipping: cannot create a stale generation cgroup in this environment");
            return;
        };

        // Spawn a child that migrates itself into the stale generation cgroup
        // and then sleeps. We use a shell so the migration happens from inside
        // the child (cgroup.procs self-write), which avoids EBUSY for threaded
        // parent cgroups.
        let procs = stale_base.join("cgroup.procs");
        let command = format!("echo $$ > {} && exec sleep 60", procs.display());
        let mut child = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(&command)
            .process_group(0)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn stale child");

        let removed = scan_and_kill_stale(&current_base, 7).expect("scan stale");
        assert!(removed.iter().any(|p| p == &stale_base));

        let waited = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
        assert!(waited.is_ok(), "stale generation child was not reaped");

        let _ = std::fs::remove_dir(&current_base);
    }
}
