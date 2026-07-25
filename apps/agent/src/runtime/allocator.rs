//! T26 persistent monotonic `ProcessGeneration` allocator.
//!
//! This is the only production source of `ProcessGenerationLease` values.  It
//! uses an advisory file lock and an atomic rename so concurrent supervisor
//! invocations cannot issue duplicate generations, and it refuses to wrap or
//! reuse `i64::MAX`.

use std::{
    fs::{self, File, OpenOptions},
    os::fd::AsRawFd,
    path::Path,
};

use anyhow::{Context, Result, anyhow, bail};
use rand::TryRngCore;
use uuid::Uuid;

use super::contracts::{
    GenerationRecoveryFence, ProcessGeneration, ProcessGenerationLease, RpcBootNonce,
};

const GENERATION_FILE_NAME: &str = ".generation";
const GENERATION_TEMP_NAME: &str = ".generation.next";

// Serialize concurrent calls within the same process.  `flock` below protects
// against concurrent supervisor processes, but on Linux `flock` does not block
// threads of the same process, so a process-wide mutex is required too.
static PROCESS_ALLOCATOR_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// A complete allocation issued to the production bootstrap boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenerationAllocation {
    pub lease: ProcessGenerationLease,
    pub nonce: RpcBootNonce,
    pub fence: GenerationRecoveryFence,
}

impl GenerationAllocation {
    pub fn generation(&self) -> ProcessGeneration {
        self.lease.generation()
    }
}

/// Acquire the next `ProcessGeneration` lease from the allocator rooted at
/// `state_dir`.
///
/// The generation file is stored in `state_dir/.generation`.  The domain is
/// `0..=i64::MAX`; `i64::MAX` is a valid generation, but the allocator will
/// refuse to issue a generation beyond it and will not wrap.
pub fn acquire_generation(state_dir: impl AsRef<Path>) -> Result<GenerationAllocation> {
    let _process_guard = PROCESS_ALLOCATOR_MUTEX
        .lock()
        .map_err(|_| anyhow!("process allocator mutex poisoned"))?;

    let state_dir = state_dir.as_ref();
    fs::create_dir_all(state_dir).context("failed to create allocator state directory")?;

    let generation_path = state_dir.join(GENERATION_FILE_NAME);
    let temp_path = state_dir.join(GENERATION_TEMP_NAME);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&generation_path)
        .context("failed to open generation ledger")?;

    // Advisory exclusive lock serializes all allocator invocations for this
    // state directory, including concurrent supervisor processes.
    lock_exclusive(file.as_raw_fd()).context("failed to lock generation ledger")?;

    let current = read_generation(&file)?;
    let next = current
        .checked_add(1)
        .filter(|value| *value <= ProcessGeneration::MAX.as_u64())
        .ok_or_else(|| anyhow!("process generation exhausted at i64::MAX; refuse wrap/reuse"))?;
    let generation = ProcessGeneration::from_wire(current)
        .context("allocator produced an out-of-domain generation")?;

    write_generation(&temp_path, next)?;
    fs::rename(&temp_path, &generation_path)
        .context("failed to commit generation ledger update")?;
    sync_dir(state_dir).context("failed to fsync allocator state directory")?;

    let lease_id = Uuid::now_v7().to_string();
    let fence_id = format!("fence-for-{lease_id}");
    let lease = ProcessGenerationLease::new(generation, &lease_id)
        .context("failed to construct process generation lease")?;
    let fence = GenerationRecoveryFence::new(&lease, &fence_id)
        .context("failed to construct generation recovery fence")?;
    let nonce = fresh_rpc_nonce().context("failed to mint RPC boot nonce")?;

    Ok(GenerationAllocation {
        lease,
        nonce,
        fence,
    })
}

fn read_generation(file: &File) -> Result<u64> {
    use std::io::{BufRead, BufReader};
    let reader = BufReader::new(file);
    let line = reader
        .lines()
        .next()
        .transpose()
        .context("failed to read generation ledger")?
        .unwrap_or_default();
    if line.trim().is_empty() {
        return Ok(0);
    }
    line.trim()
        .parse::<u64>()
        .with_context(|| format!("generation ledger contains non-integer value: {line:?}"))
}

fn write_generation(path: &Path, value: u64) -> Result<()> {
    let mut temp = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .with_context(|| {
            format!(
                "failed to open temporary generation file {}",
                path.display()
            )
        })?;
    use std::io::Write;
    writeln!(&mut temp, "{value}").context("failed to write generation ledger")?;
    temp.sync_all()
        .context("failed to fsync temporary generation file")?;
    Ok(())
}

fn sync_dir(dir: &Path) -> Result<()> {
    let dir = File::open(dir).context("failed to open state directory for fsync")?;
    dir.sync_all().context("failed to fsync state directory")?;
    Ok(())
}

fn lock_exclusive(fd: std::os::fd::RawFd) -> Result<()> {
    let rc = unsafe { libc::flock(fd, libc::LOCK_EX) };
    if rc != 0 {
        bail!("flock(LOCK_EX) failed: {}", std::io::Error::last_os_error());
    }
    Ok(())
}

fn fresh_rpc_nonce() -> Result<RpcBootNonce> {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng
        .try_fill_bytes(&mut bytes)
        .context("operating-system random source failed")?;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    RpcBootNonce::new(hex).context("generated RPC nonce was rejected by contract")
}

/// Prints shell `export` lines for the allocation so a POSIX supervisor can
/// `eval` the output and bind the values into the child environment.
pub fn print_shell_exports(allocation: &GenerationAllocation) {
    println!(
        "export SUMI_RPC_GENERATION={}\nexport SUMI_RPC_NONCE={}\nexport SUMI_PROCESS_GENERATION_LEASE_ID={}\nexport SUMI_GENERATION_RECOVERY_FENCE_ID={}",
        allocation.generation().as_u64(),
        allocation.nonce.as_str(),
        allocation.lease.lease_id(),
        allocation.fence.fence_id(),
    );
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::*;

    fn test_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{}-{}", label, Uuid::now_v7()))
    }

    #[test]
    fn allocator_starts_at_generation_zero() {
        let dir = test_dir("sumi-alloc-zero");
        let alloc = acquire_generation(&dir).unwrap();
        assert_eq!(alloc.generation().as_u64(), 0);
        assert!(!alloc.nonce.as_str().is_empty());
        assert_eq!(alloc.lease.generation(), alloc.generation());
        assert_eq!(alloc.fence.generation(), alloc.generation());
        assert_eq!(alloc.fence.lease_id(), alloc.lease.lease_id());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn allocator_increments_monotonically() {
        let dir = test_dir("sumi-alloc-mono");
        let first = acquire_generation(&dir).unwrap();
        let second = acquire_generation(&dir).unwrap();
        assert_eq!(
            second.generation().as_u64(),
            first.generation().as_u64() + 1
        );
        assert_ne!(first.nonce.as_str(), second.nonce.as_str());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn concurrent_allocations_are_unique_and_monotonic() {
        let dir = Arc::new(test_dir("sumi-alloc-conc"));
        let count = 32;
        let barrier = Arc::new(Barrier::new(count));
        let mut handles = Vec::new();

        for _ in 0..count {
            let dir = dir.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                acquire_generation(&*dir).unwrap()
            }));
        }

        let mut values: Vec<u64> = handles
            .into_iter()
            .map(|h| h.join().unwrap().generation().as_u64())
            .collect();
        values.sort_unstable();
        let unique: std::collections::HashSet<_> = values.iter().copied().collect();
        assert_eq!(unique.len(), values.len());
        assert_eq!(values.first().copied().unwrap(), 0);
        assert_eq!(values.last().copied().unwrap() as usize, count - 1);
        let _ = fs::remove_dir_all(&**dir);
    }

    #[test]
    fn allocator_refuses_to_wrap_past_i64_max() {
        let dir = test_dir("sumi-alloc-max");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(GENERATION_FILE_NAME);
        fs::write(&path, format!("{}\n", ProcessGeneration::MAX.as_u64())).unwrap();
        assert!(acquire_generation(&dir).is_err());
        let _ = fs::remove_dir_all(&dir);
    }
}
