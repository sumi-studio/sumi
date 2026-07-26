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
const GENERATION_LOCK_NAME: &str = ".generation.lock";
const GENERATION_TEMP_NAME: &str = ".generation.next";

// Serialize concurrent calls within the same process.  `flock` below protects
// against concurrent supervisor processes, but on Linux `flock` does not block
// threads of the same process, so a process-wide mutex is required too.
// The lock is held on a dedicated `.generation.lock` file whose inode is never
// renamed or unlinked, so `flock` keeps its lock across the atomic rename of
// the generation ledger.
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

/// Current content of the generation ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GenerationLedger {
    /// The next generation value to issue.
    Next(u64),
    /// `i64::MAX` has already been issued; no further generations are valid.
    Exhausted,
}

/// Acquire the next `ProcessGeneration` lease from the allocator rooted at
/// `state_dir`.
///
/// The generation file is stored in `state_dir/.generation`.  The domain is
/// `0..=i64::MAX`; `i64::MAX` is a valid generation, but the allocator will
/// issue it only once and then persist an `exhausted` sentinel so the next
/// bootstrap fails closed without wrap or reuse.
pub fn acquire_generation(state_dir: impl AsRef<Path>) -> Result<GenerationAllocation> {
    let _process_guard = PROCESS_ALLOCATOR_MUTEX
        .lock()
        .map_err(|_| anyhow!("process allocator mutex poisoned"))?;

    let state_dir = state_dir.as_ref();
    fs::create_dir_all(state_dir).context("failed to create allocator state directory")?;

    let lock_path = state_dir.join(GENERATION_LOCK_NAME);
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .context("failed to open generation lock file")?;

    // Lock a stable inode. The ledger itself is atomically renamed below, so
    // locking that data file would not serialize a later opener.
    lock_exclusive(lock_file.as_raw_fd()).context("failed to lock generation ledger")?;

    let generation_path = state_dir.join(GENERATION_FILE_NAME);
    let temp_path = state_dir.join(GENERATION_TEMP_NAME);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&generation_path)
        .context("failed to open generation ledger")?;

    let ledger = read_generation(&file)?;
    let (generation, next_ledger) = match ledger {
        GenerationLedger::Exhausted => {
            bail!("process generation exhausted at i64::MAX; refuse wrap/reuse");
        }
        GenerationLedger::Next(value) => {
            let generation = ProcessGeneration::from_wire(value)
                .context("allocator produced an out-of-domain generation")?;
            let next_ledger = if value == ProcessGeneration::MAX.as_u64() {
                GenerationLedger::Exhausted
            } else {
                GenerationLedger::Next(value + 1)
            };
            (generation, next_ledger)
        }
    };

    write_ledger(&temp_path, next_ledger)?;
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

fn read_generation(file: &File) -> Result<GenerationLedger> {
    use std::io::{BufRead, BufReader};
    let reader = BufReader::new(file);
    let line = reader
        .lines()
        .next()
        .transpose()
        .context("failed to read generation ledger")?
        .unwrap_or_default();
    let line = line.trim();
    if line.is_empty() {
        return Ok(GenerationLedger::Next(0));
    }
    if line == "exhausted" {
        return Ok(GenerationLedger::Exhausted);
    }
    let value = line
        .parse::<u64>()
        .with_context(|| format!("generation ledger contains non-integer value: {line:?}"))?;
    Ok(GenerationLedger::Next(value))
}

fn write_ledger(path: &Path, ledger: GenerationLedger) -> Result<()> {
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
    match ledger {
        GenerationLedger::Exhausted => writeln!(&mut temp, "exhausted"),
        GenerationLedger::Next(value) => writeln!(&mut temp, "{value}"),
    }
    .context("failed to write generation ledger")?;
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
    fn allocator_issues_i64_max_once_then_refuses_next() {
        let dir = test_dir("sumi-alloc-max");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(GENERATION_FILE_NAME);
        let max = ProcessGeneration::MAX.as_u64();
        // The ledger stores the next generation to issue; seeding it with
        // i64::MAX should issue exactly that valid generation and then latch
        // the ledger as exhausted.
        fs::write(&path, format!("{}\n", max)).unwrap();

        let alloc = acquire_generation(&dir).unwrap();
        assert_eq!(alloc.generation().as_u64(), max);

        assert!(acquire_generation(&dir).is_err());

        let content = fs::read_to_string(&path).unwrap();
        assert!(content.trim() == "exhausted");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn allocator_exhausted_state_survives_restart() {
        let dir = test_dir("sumi-alloc-restart");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(GENERATION_FILE_NAME);
        let max = ProcessGeneration::MAX.as_u64();
        fs::write(&path, format!("{}\n", max)).unwrap();

        let first = acquire_generation(&dir).unwrap();
        assert_eq!(first.generation().as_u64(), max);

        // Simulate a fresh process invocation against the same persistent ledger.
        let second = acquire_generation(&dir);
        assert!(
            second.is_err(),
            "post-MAX allocation must remain refused after restart"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn allocator_refuses_exhausted_seed() {
        let dir = test_dir("sumi-alloc-exhausted");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(GENERATION_FILE_NAME);
        fs::write(&path, "exhausted\n").unwrap();
        assert!(acquire_generation(&dir).is_err());
        let _ = fs::remove_dir_all(&dir);
    }
}
