//! Cross-process allocator test.
//!
//! Spawns multiple `sumi-agent --allocate-generation` processes against the
//! same `SUMI_STATE_DIR` and verifies that each receives a unique, monotonic
//! generation.  This exercises the stable `.generation.lock` file path and the
//! `flock`-protected rename used in `runtime::allocator`.

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Barrier};
use std::thread;

fn find_binary() -> PathBuf {
    for key in ["CARGO_BIN_EXE_sumi_agent", "CARGO_BIN_EXE_sumi-agent"] {
        if let Ok(path) = std::env::var(key) {
            return path.into();
        }
    }

    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    PathBuf::from(manifest)
        .parent()
        .expect("manifest parent")
        .parent()
        .expect("repo root")
        .join("target/debug/sumi-agent")
}

fn parse_generation(stdout: &[u8]) -> u64 {
    let text = String::from_utf8_lossy(stdout);
    text.lines()
        .find_map(|line| line.strip_prefix("export SUMI_RPC_GENERATION="))
        .and_then(|value| value.trim().parse::<u64>().ok())
        .expect("missing SUMI_RPC_GENERATION export")
}

#[test]
fn cross_process_allocator_allocations_are_unique_and_monotonic() {
    let count = 20;
    let bin = find_binary();
    let dir = std::env::temp_dir().join(format!("sumi-alloc-cross-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();

    let barrier = Arc::new(Barrier::new(count));
    let mut handles = Vec::with_capacity(count);

    for _ in 0..count {
        let dir = dir.clone();
        let bin = bin.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            let output = Command::new(&bin)
                .arg("--allocate-generation")
                .env("SUMI_STATE_DIR", &dir)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .expect("spawn allocator process");
            assert!(
                output.status.success(),
                "allocator failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            parse_generation(&output.stdout)
        }));
    }

    let mut values: Vec<u64> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    values.sort_unstable();

    let expected: Vec<u64> = (0..count as u64).collect();
    assert_eq!(
        values, expected,
        "allocator produced duplicate or non-monotonic generations across processes"
    );

    let ledger =
        std::fs::read_to_string(dir.join(".generation")).expect("generation ledger file exists");
    assert_eq!(ledger.trim(), count.to_string());

    let _ = std::fs::remove_dir_all(&dir);
}
