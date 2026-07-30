use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Child, Command, Output, Stdio},
};

use serde_json::Value;
use uuid::{Uuid, Variant, Version};

const PAID: &str = "0198f0f4-9b72-7000-8000-000000000001";

struct Fixture {
    root: PathBuf,
    state: PathBuf,
    output: PathBuf,
}

impl Fixture {
    fn new(label: &str) -> Self {
        let root =
            std::env::temp_dir().join(format!("sumi-cli-allocator-{label}-{}", Uuid::now_v7()));
        let state = root.join("state");
        let output = root.join("output");
        fs::create_dir_all(&state).unwrap();
        fs::create_dir_all(&output).unwrap();
        for role in ["runtime", "executor", "broker"] {
            fs::create_dir(output.join(role)).unwrap();
        }
        for directory in [&root, &state, &output] {
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        for role in ["runtime", "executor", "broker"] {
            fs::set_permissions(output.join(role), fs::Permissions::from_mode(0o700)).unwrap();
        }
        Self {
            root,
            state,
            output,
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_sumi-agent"));
        command
            .env_clear()
            .arg("--supervisor-allocate")
            .env("SUMI_PERSONALITY_AGENT_ID", PAID)
            .env("SUMI_ALLOCATOR_STATE_DIR", &self.state)
            .env("SUMI_IDENTITY_OUTPUT_ROOT", &self.output)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    fn allocate(&self) -> Output {
        self.command().output().unwrap()
    }

    fn identity(&self, role: &str) -> BTreeMap<String, String> {
        parse_identity(&fs::read_to_string(self.output.join(role).join("identity.env")).unwrap())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn parse_identity(value: &str) -> BTreeMap<String, String> {
    value
        .lines()
        .map(|line| {
            let (key, value) = line.split_once('=').expect("key=value identity line");
            assert!(
                !value.is_empty()
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
                "identity values must be safe unquoted atoms"
            );
            (key.to_owned(), value.to_owned())
        })
        .collect()
}

fn successful_generation(output: &Output) -> u64 {
    assert!(
        output.status.success(),
        "allocator failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "success must not log identity data"
    );
    let json: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["status"], "allocated");
    let object = json.as_object().unwrap();
    assert_eq!(object.len(), 2);
    assert!(object.contains_key("generation"));
    assert!(object.contains_key("status"));
    json["generation"].as_u64().unwrap()
}

#[test]
fn concurrent_processes_issue_each_generation_exactly_once() {
    let fixture = Fixture::new("concurrent");
    let mut children: Vec<Child> = (0..16)
        .map(|_| fixture.command().spawn().unwrap())
        .collect();
    let mut generations: Vec<u64> = children
        .drain(..)
        .map(|child| successful_generation(&child.wait_with_output().unwrap()))
        .collect();
    generations.sort_unstable();
    assert_eq!(generations, (0..16).collect::<Vec<_>>());

    let runtime = fixture.identity("runtime");
    let executor = fixture.identity("executor");
    let broker = fixture.identity("broker");
    assert_eq!(
        runtime["SUMI_RPC_GENERATION"],
        generations.last().unwrap().to_string()
    );
    for sidecar in [&executor, &broker] {
        assert_eq!(
            sidecar.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            BTreeSet::from([
                "SUMI_PERSONALITY_AGENT_ID",
                "SUMI_RPC_GENERATION",
                "SUMI_RPC_NONCE",
            ])
        );
        for key in [
            "SUMI_PERSONALITY_AGENT_ID",
            "SUMI_RPC_GENERATION",
            "SUMI_RPC_NONCE",
        ] {
            assert_eq!(sidecar[key], runtime[key]);
        }
    }
}

#[test]
fn restart_rotates_all_secrets_and_never_leaks_runtime_only_fields() {
    let fixture = Fixture::new("restart");
    assert_eq!(successful_generation(&fixture.allocate()), 0);
    let first = fixture.identity("runtime");
    assert_eq!(successful_generation(&fixture.allocate()), 1);
    let second = fixture.identity("runtime");

    assert_ne!(first["SUMI_RPC_NONCE"], second["SUMI_RPC_NONCE"]);
    assert_ne!(
        first["SUMI_PROCESS_GENERATION_LEASE_ID"],
        second["SUMI_PROCESS_GENERATION_LEASE_ID"]
    );
    assert_ne!(
        first["SUMI_GENERATION_RECOVERY_FENCE_ID"],
        second["SUMI_GENERATION_RECOVERY_FENCE_ID"]
    );
    for key in [
        "SUMI_PROCESS_GENERATION_LEASE_ID",
        "SUMI_GENERATION_RECOVERY_FENCE_ID",
    ] {
        let uuid = Uuid::parse_str(&second[key]).unwrap();
        assert_eq!(uuid.get_version(), Some(Version::SortRand));
        assert_eq!(uuid.get_variant(), Variant::RFC4122);
        assert_eq!(uuid.hyphenated().to_string(), second[key]);
    }
    assert_eq!(second["SUMI_RPC_NONCE"].len(), 64);

    assert_eq!(
        second.keys().map(String::as_str).collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "SUMI_GENERATION_RECOVERY_FENCE_ID",
            "SUMI_PERSONALITY_AGENT_ID",
            "SUMI_PROCESS_GENERATION_LEASE_ID",
            "SUMI_RPC_GENERATION",
            "SUMI_RPC_NONCE",
        ])
    );
    for role in ["executor", "broker"] {
        let identity = fixture.identity(role);
        assert!(!identity.keys().any(|key| {
            key.contains("TENANT")
                || key.contains("AGENT") && key != "SUMI_PERSONALITY_AGENT_ID"
                || key.contains("CONVERSATION")
                || key.contains("LEASE")
                || key.contains("FENCE")
        }));
    }
}

#[test]
fn cli_rejects_noncanonical_paid_relative_paths_and_untrusted_output_mode() {
    let fixture = Fixture::new("inputs");
    let bad_paid = fixture
        .command()
        .env("SUMI_PERSONALITY_AGENT_ID", PAID.to_ascii_uppercase())
        .output()
        .unwrap();
    assert!(!bad_paid.status.success());

    let relative = fixture
        .command()
        .env("SUMI_ALLOCATOR_STATE_DIR", "relative")
        .output()
        .unwrap();
    assert!(!relative.status.success());

    fs::set_permissions(&fixture.output, fs::Permissions::from_mode(0o750)).unwrap();
    let permissions = fixture.allocate();
    assert!(!permissions.status.success());
}

#[test]
fn identity_files_are_regular_single_link_owner_only_files() {
    let fixture = Fixture::new("modes");
    successful_generation(&fixture.allocate());
    for role in ["runtime", "executor", "broker"] {
        let path = fixture.output.join(role).join("identity.env");
        let metadata = fs::symlink_metadata(&path).unwrap();
        assert!(metadata.file_type().is_file());
        assert_eq!(metadata.permissions().mode() & 0o7777, 0o400);
        assert_eq!(std::os::unix::fs::MetadataExt::nlink(&metadata), 1);
    }
}
