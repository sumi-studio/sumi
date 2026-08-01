use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    os::{
        fd::AsRawFd,
        unix::{
            fs::{MetadataExt, PermissionsExt, symlink},
            process::{CommandExt, ExitStatusExt},
        },
    },
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
};

use serde_json::Value;
use uuid::{Uuid, Variant, Version};

const PAID: &str = "0198f0f4-9b72-7000-8000-000000000001";

struct Fixture {
    root: PathBuf,
    state: PathBuf,
    output: PathBuf,
    role_gids: [libc::gid_t; 3],
}

impl Fixture {
    fn new(label: &str) -> Self {
        let root =
            std::env::temp_dir().join(format!("sumi-cli-allocator-{label}-{}", Uuid::now_v7()));
        let role_gids = test_role_gids();
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
            role_gids,
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_sumi-agent"));
        command
            .env_clear()
            .arg("--supervisor-allocate")
            .env("SUMI_PERSONALITY_AGENT_ID", PAID)
            .env("SUMI_ALLOCATOR_TRUST_ROOT", &self.root)
            .env("SUMI_ALLOCATOR_STATE_DIR", &self.state)
            .env("SUMI_IDENTITY_OUTPUT_ROOT", &self.output)
            .env("SUMI_RUNTIME_IDENTITY_GID", self.role_gids[0].to_string())
            .env("SUMI_EXECUTOR_IDENTITY_GID", self.role_gids[1].to_string())
            .env("SUMI_BROKER_IDENTITY_GID", self.role_gids[2].to_string())
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

    fn assert_handoff(&self) {
        self.assert_role_mode(0o550);
    }

    fn assert_role_mode(&self, mode: u32) {
        for role in ["runtime", "executor", "broker"] {
            let metadata = fs::metadata(self.output.join(role)).unwrap();
            assert_eq!(metadata.permissions().mode() & 0o7777, mode);
            assert_eq!(metadata.gid(), self.role_gids[role_index(role)]);
        }
    }

    fn directory_inodes(&self) -> BTreeMap<&'static str, (u64, u64)> {
        [
            ("trust_root", self.root.clone()),
            ("state", self.state.clone()),
            ("output", self.output.clone()),
            ("runtime", self.output.join("runtime")),
            ("executor", self.output.join("executor")),
            ("broker", self.output.join("broker")),
        ]
        .into_iter()
        .map(|(name, path)| {
            let metadata = fs::metadata(path).unwrap();
            (name, (metadata.dev(), metadata.ino()))
        })
        .collect()
    }

    fn assert_output_binding_inodes(&self) {
        let binding: Value =
            serde_json::from_slice(&fs::read(self.output.join("allocator-binding.json")).unwrap())
                .unwrap();
        for (name, (device, inode)) in self.directory_inodes() {
            assert_eq!(
                binding["directories"][name]["device"].as_u64(),
                Some(device)
            );
            assert_eq!(binding["directories"][name]["inode"].as_u64(), Some(inode));
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        make_tree_removable(&self.root);
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn role_index(role: &str) -> usize {
    match role {
        "runtime" => 0,
        "executor" => 1,
        "broker" => 2,
        _ => panic!("unknown role"),
    }
}

fn make_tree_removable(path: &Path) {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return;
    };
    if metadata.file_type().is_symlink() {
        return;
    }
    if metadata.is_dir() {
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o700));
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                make_tree_removable(&entry.path());
            }
        }
    } else {
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
    }
}

fn create_fresh_output(path: &Path) {
    fs::create_dir(path).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    for role in ["runtime", "executor", "broker"] {
        let role_path = path.join(role);
        fs::create_dir(&role_path).unwrap();
        fs::set_permissions(role_path, fs::Permissions::from_mode(0o700)).unwrap();
    }
}

fn write_owned_file(path: &Path, gid: libc::gid_t, mode: u32, bytes: &[u8]) {
    fs::write(path, bytes).unwrap();
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    assert_eq!(
        unsafe { libc::fchown(file.as_raw_fd(), libc::geteuid(), gid) },
        0,
        "fchown failed: {}",
        std::io::Error::last_os_error()
    );
    file.set_permissions(fs::Permissions::from_mode(mode))
        .unwrap();
}

fn assert_no_strict_temps(fixture: &Fixture) {
    for (directory, prefix) in [
        (&fixture.state, ".allocator-ledger.json.tmp-"),
        (&fixture.output, ".allocator-binding.json.tmp-"),
    ] {
        assert!(
            fs::read_dir(directory).unwrap().all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(prefix)),
            "strict allocator temp remained in {}",
            directory.display()
        );
    }
    for role in ["runtime", "executor", "broker"] {
        let directory = fixture.output.join(role);
        assert!(
            fs::read_dir(&directory).unwrap().all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".identity.env.tmp-")),
            "strict identity temp remained in {}",
            directory.display()
        );
    }
}

fn entries_with_prefix(directory: &Path, prefix: &str) -> Vec<PathBuf> {
    fs::read_dir(directory)
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with(prefix)
                .then_some(path)
        })
        .collect()
}

fn test_role_gids() -> [libc::gid_t; 3] {
    if unsafe { libc::geteuid() } == 0 {
        return [61_001, 61_002, 61_003];
    }
    let count = unsafe { libc::getgroups(0, std::ptr::null_mut()) };
    assert!(count >= 0);
    let mut groups = vec![0 as libc::gid_t; count as usize];
    if count > 0 {
        assert_eq!(
            unsafe { libc::getgroups(count, groups.as_mut_ptr()) },
            count
        );
    }
    let real = unsafe { libc::getgid() };
    let effective = unsafe { libc::getegid() };
    let groups: Vec<_> = groups
        .into_iter()
        .filter(|gid| *gid != 0 && *gid != real && *gid != effective)
        .collect();
    assert!(
        groups.len() >= 3,
        "allocator integration tests require three supplemental groups"
    );
    [groups[0], groups[1], groups[2]]
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
fn cli_rejects_zero_duplicate_primary_and_malformed_role_gids() {
    let fixture = Fixture::new("gid-inputs");
    for (name, value) in [
        ("SUMI_RUNTIME_IDENTITY_GID", "0".to_owned()),
        (
            "SUMI_RUNTIME_IDENTITY_GID",
            fixture.role_gids[1].to_string(),
        ),
        (
            "SUMI_RUNTIME_IDENTITY_GID",
            unsafe { libc::getegid() }.to_string(),
        ),
        ("SUMI_RUNTIME_IDENTITY_GID", libc::gid_t::MAX.to_string()),
        ("SUMI_RUNTIME_IDENTITY_GID", "010001".to_owned()),
        ("SUMI_RUNTIME_IDENTITY_GID", "+10001".to_owned()),
        ("SUMI_RUNTIME_IDENTITY_GID", "not-a-gid".to_owned()),
    ] {
        let output = fixture.command().env(name, value).output().unwrap();
        assert!(!output.status.success());
    }
}

#[test]
fn identity_files_are_regular_single_link_owner_only_files() {
    let fixture = Fixture::new("modes");
    for role in ["runtime", "executor", "broker"] {
        let metadata = fs::metadata(fixture.output.join(role)).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o7777, 0o700);
        assert_eq!(metadata.gid(), unsafe { libc::getegid() });
    }
    successful_generation(&fixture.allocate());
    for role in ["runtime", "executor", "broker"] {
        let path = fixture.output.join(role).join("identity.env");
        let metadata = fs::symlink_metadata(&path).unwrap();
        assert!(metadata.file_type().is_file());
        assert_eq!(metadata.permissions().mode() & 0o7777, 0o440);
        assert_eq!(metadata.nlink(), 1);
        assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
        assert_eq!(metadata.gid(), fixture.role_gids[role_index(role)]);
        let role_metadata = fs::metadata(fixture.output.join(role)).unwrap();
        assert_eq!(role_metadata.permissions().mode() & 0o7777, 0o550);
        assert_eq!(role_metadata.gid(), fixture.role_gids[role_index(role)]);
    }
}

#[test]
fn role_group_can_read_identity_without_sharing_the_allocator_uid() {
    if unsafe { libc::geteuid() } != 0 {
        return;
    }
    let fixture = Fixture::new("role-reader");
    successful_generation(&fixture.allocate());

    for role in ["runtime", "executor", "broker"] {
        let role_gid = fixture.role_gids[role_index(role)];
        let expected = fs::read(fixture.output.join(role).join("identity.env")).unwrap();
        let mut command = Command::new("/bin/cat");
        command
            .current_dir(fixture.output.join(role))
            .arg("identity.env");
        unsafe {
            command.pre_exec(move || {
                if libc::setgroups(1, &role_gid) != 0
                    || libc::setgid(65_534) != 0
                    || libc::setuid(65_534) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{role} group-only reader failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.stdout == expected,
            "{role} group-only reader returned unexpected bytes"
        );
    }
}

#[test]
fn trust_root_rejects_writable_or_symlinked_ancestors_and_dotdot_aliases() {
    let fixture = Fixture::new("ancestor-trust");
    let nested = fixture.root.join("nested");
    let nested_state = nested.join("state");
    fs::create_dir_all(&nested_state).unwrap();
    fs::set_permissions(&nested, fs::Permissions::from_mode(0o770)).unwrap();
    fs::set_permissions(&nested_state, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(
        !fixture
            .command()
            .env("SUMI_ALLOCATOR_STATE_DIR", &nested_state)
            .output()
            .unwrap()
            .status
            .success()
    );

    fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).unwrap();
    let state_link = fixture.root.join("state-link");
    symlink(&fixture.state, &state_link).unwrap();
    assert!(
        !fixture
            .command()
            .env("SUMI_ALLOCATOR_STATE_DIR", &state_link)
            .output()
            .unwrap()
            .status
            .success()
    );

    let dotdot = nested.join("..").join("state");
    assert!(
        !fixture
            .command()
            .env("SUMI_ALLOCATOR_STATE_DIR", dotdot)
            .output()
            .unwrap()
            .status
            .success()
    );
}

#[test]
fn role_symlink_aliases_are_rejected_without_initializing_the_volume() {
    let fixture = Fixture::new("role-alias");
    let runtime = fixture.output.join("runtime");
    let displaced = fixture.output.join("runtime-displaced");
    fs::rename(&runtime, &displaced).unwrap();
    symlink("executor", &runtime).unwrap();

    assert!(!fixture.allocate().status.success());
    let displaced_metadata = fs::metadata(displaced).unwrap();
    assert_eq!(displaced_metadata.permissions().mode() & 0o7777, 0o700);
    assert_eq!(displaced_metadata.gid(), unsafe { libc::getegid() });
}

#[test]
fn persistent_bindings_reject_each_directory_swap() {
    for role in ["runtime", "executor", "broker"] {
        let fixture = Fixture::new(&format!("{role}-swap"));
        assert_eq!(successful_generation(&fixture.allocate()), 0);
        let role_path = fixture.output.join(role);
        fs::rename(&role_path, fixture.output.join(format!("{role}-old"))).unwrap();
        fs::create_dir(&role_path).unwrap();
        fs::set_permissions(&role_path, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(
            !fixture.allocate().status.success(),
            "{role} volume replacement was accepted"
        );
        assert_eq!(
            fs::metadata(&role_path).unwrap().permissions().mode() & 0o7777,
            0o700,
            "a rejected replacement must not be initialized"
        );
    }

    let fixture = Fixture::new("state-swap");
    assert_eq!(successful_generation(&fixture.allocate()), 0);
    fs::rename(&fixture.state, fixture.root.join("state-old")).unwrap();
    fs::create_dir(&fixture.state).unwrap();
    fs::set_permissions(&fixture.state, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(!fixture.allocate().status.success());

    let fixture = Fixture::new("output-swap");
    assert_eq!(successful_generation(&fixture.allocate()), 0);
    fs::rename(&fixture.output, fixture.root.join("output-old")).unwrap();
    create_fresh_output(&fixture.output);
    assert!(!fixture.allocate().status.success());

    let fixture = Fixture::new("trust-root-swap");
    assert_eq!(successful_generation(&fixture.allocate()), 0);
    let relocated = fixture.root.with_file_name(format!(
        "{}-relocated",
        fixture.root.file_name().unwrap().to_string_lossy()
    ));
    fs::rename(&fixture.root, &relocated).unwrap();
    fs::create_dir(&fixture.root).unwrap();
    fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o700)).unwrap();
    fs::rename(relocated.join("state"), &fixture.state).unwrap();
    fs::rename(relocated.join("output"), &fixture.output).unwrap();
    assert!(!fixture.allocate().status.success());
    fs::remove_dir(relocated).unwrap();
}

#[test]
fn stale_allocator_temps_are_cleaned_but_arbitrary_names_are_untouched() {
    let fixture = Fixture::new("stale-temps");
    assert_eq!(successful_generation(&fixture.allocate()), 0);

    let ledger_temp = fixture.state.join(format!(
        ".allocator-ledger.json.tmp-{}",
        Uuid::now_v7().hyphenated()
    ));
    let binding_temp = fixture.output.join(format!(
        ".allocator-binding.json.tmp-{}",
        Uuid::now_v7().hyphenated()
    ));
    write_owned_file(&ledger_temp, unsafe { libc::getegid() }, 0o600, b"partial");
    write_owned_file(&binding_temp, unsafe { libc::getegid() }, 0o600, b"partial");
    let arbitrary_state = fixture.state.join(".allocator-ledger.json.tmp-not-a-uuid");
    fs::write(&arbitrary_state, b"leave me").unwrap();

    let mut arbitrary_role_paths = Vec::new();
    let mut role_temps = Vec::new();
    for role in ["runtime", "executor", "broker"] {
        let role_dir = fixture.output.join(role);
        fs::set_permissions(&role_dir, fs::Permissions::from_mode(0o750)).unwrap();
        let temp = role_dir.join(format!(".identity.env.tmp-{}", Uuid::now_v7().hyphenated()));
        write_owned_file(
            &temp,
            fixture.role_gids[role_index(role)],
            0o400,
            b"partial",
        );
        role_temps.push(temp);
        let arbitrary = role_dir.join(".identity.env.tmp-not-a-uuid");
        fs::write(&arbitrary, b"leave me").unwrap();
        arbitrary_role_paths.push(arbitrary);
    }

    assert_eq!(successful_generation(&fixture.allocate()), 1);
    assert!(!ledger_temp.exists());
    assert!(!binding_temp.exists());
    assert!(role_temps.iter().all(|path| !path.exists()));
    assert!(arbitrary_state.exists());
    assert!(arbitrary_role_paths.iter().all(|path| path.exists()));
    fixture.assert_handoff();
}

#[test]
fn unsafe_strictly_named_stale_temps_fail_closed() {
    let fixture = Fixture::new("unsafe-stale-temp");
    let symlink_temp = fixture.state.join(format!(
        ".allocator-ledger.json.tmp-{}",
        Uuid::now_v7().hyphenated()
    ));
    symlink("/dev/null", &symlink_temp).unwrap();
    assert!(!fixture.allocate().status.success());
    fs::remove_file(&symlink_temp).unwrap();

    let source = fixture.root.join("linked-temp-source");
    fs::write(&source, b"partial").unwrap();
    fs::set_permissions(&source, fs::Permissions::from_mode(0o600)).unwrap();
    let linked_temp = fixture.state.join(format!(
        ".allocator-ledger.json.tmp-{}",
        Uuid::now_v7().hyphenated()
    ));
    fs::hard_link(&source, &linked_temp).unwrap();
    assert!(!fixture.allocate().status.success());
    fs::remove_file(&linked_temp).unwrap();
    fs::remove_file(&source).unwrap();

    assert_eq!(successful_generation(&fixture.allocate()), 0);
}

#[test]
fn output_binding_is_required_after_ledger_commit_and_validated_on_restart() {
    let fixture = Fixture::new("binding-missing");
    assert_eq!(successful_generation(&fixture.allocate()), 0);
    let binding = fixture.output.join("allocator-binding.json");
    fs::remove_file(&binding).unwrap();
    assert!(!fixture.allocate().status.success());

    let fixture = Fixture::new("binding-corrupt");
    assert_eq!(successful_generation(&fixture.allocate()), 0);
    let binding = fixture.output.join("allocator-binding.json");
    fs::write(&binding, b"{").unwrap();
    fs::set_permissions(&binding, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(!fixture.allocate().status.success());
}

#[test]
fn first_allocation_recovers_when_output_binding_precedes_ledger_commit() {
    let fixture = Fixture::new("binding-before-ledger");
    let killed = fixture
        .command()
        .env("SUMI_ALLOCATOR_TEST_CRASH_AT", "ledger.partial_write")
        .output()
        .unwrap();
    assert_eq!(killed.status.signal(), Some(libc::SIGKILL));
    assert!(fixture.output.join("allocator-binding.json").is_file());
    assert!(!fixture.state.join("allocator-ledger.json").exists());

    assert_eq!(successful_generation(&fixture.allocate()), 0);
    fixture.assert_handoff();
    assert_no_strict_temps(&fixture);
}

#[test]
fn sigkill_at_every_output_binding_stage_recovers_first_allocation() {
    for stage in ["partial_write", "file_fsync", "rename", "parent_fsync"] {
        let failpoint = format!("output_binding.{stage}");
        let fixture = Fixture::new(&format!("output-binding-{stage}"));
        let original_directories = fixture.directory_inodes();

        let killed = fixture
            .command()
            .env("SUMI_ALLOCATOR_TEST_CRASH_AT", &failpoint)
            .output()
            .unwrap();
        assert_eq!(
            killed.status.signal(),
            Some(libc::SIGKILL),
            "{failpoint} did not terminate with SIGKILL: {}",
            String::from_utf8_lossy(&killed.stderr)
        );
        assert_eq!(fixture.directory_inodes(), original_directories);
        fixture.assert_role_mode(0o750);
        assert!(!fixture.state.join("allocator-ledger.json").exists());

        let binding_path = fixture.output.join("allocator-binding.json");
        let temps = entries_with_prefix(&fixture.output, ".allocator-binding.json.tmp-");
        let committed_binding_inode = if matches!(stage, "partial_write" | "file_fsync") {
            assert_eq!(temps.len(), 1, "{failpoint} did not leave one stale temp");
            assert!(!binding_path.exists());
            let metadata = fs::symlink_metadata(&temps[0]).unwrap();
            assert!(metadata.file_type().is_file());
            assert_eq!(metadata.nlink(), 1);
            assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
            assert_eq!(metadata.gid(), unsafe { libc::getegid() });
            assert_eq!(metadata.permissions().mode() & 0o7777, 0o600);
            None
        } else {
            assert!(temps.is_empty(), "{failpoint} left a renamed temp");
            let metadata = fs::symlink_metadata(&binding_path).unwrap();
            assert!(metadata.file_type().is_file());
            assert_eq!(metadata.nlink(), 1);
            assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
            assert_eq!(metadata.gid(), unsafe { libc::getegid() });
            assert_eq!(metadata.permissions().mode() & 0o7777, 0o600);
            fixture.assert_output_binding_inodes();
            Some((metadata.dev(), metadata.ino()))
        };

        assert_eq!(successful_generation(&fixture.allocate()), 0);
        assert_eq!(fixture.directory_inodes(), original_directories);
        for role in ["runtime", "executor", "broker"] {
            assert_eq!(fixture.identity(role)["SUMI_RPC_GENERATION"], "0");
        }
        fixture.assert_handoff();
        fixture.assert_output_binding_inodes();
        assert_no_strict_temps(&fixture);
        if let Some(expected_inode) = committed_binding_inode {
            let metadata = fs::metadata(&binding_path).unwrap();
            assert_eq!((metadata.dev(), metadata.ino()), expected_inode);
        }
    }
}

#[test]
fn sigkill_at_every_ledger_and_role_write_stage_recovers_without_generation_reuse() {
    for target in ["ledger", "runtime", "executor", "broker"] {
        for stage in ["partial_write", "file_fsync", "rename", "parent_fsync"] {
            let failpoint = format!("{target}.{stage}");
            let fixture = Fixture::new(&format!("{target}-{stage}"));
            assert_eq!(successful_generation(&fixture.allocate()), 0);

            let killed = fixture
                .command()
                .env("SUMI_ALLOCATOR_TEST_CRASH_AT", &failpoint)
                .output()
                .unwrap();
            assert_eq!(
                killed.status.signal(),
                Some(libc::SIGKILL),
                "{failpoint} did not terminate with SIGKILL: {}",
                String::from_utf8_lossy(&killed.stderr)
            );

            let expected = if target == "ledger" && matches!(stage, "partial_write" | "file_fsync")
            {
                1
            } else {
                2
            };
            assert_eq!(
                successful_generation(&fixture.allocate()),
                expected,
                "{failpoint} reused or skipped the wrong generation"
            );
            for role in ["runtime", "executor", "broker"] {
                assert_eq!(
                    fixture.identity(role)["SUMI_RPC_GENERATION"],
                    expected.to_string(),
                    "{failpoint} left a mixed role generation"
                );
            }
            fixture.assert_handoff();
            assert_no_strict_temps(&fixture);
        }
    }
}
