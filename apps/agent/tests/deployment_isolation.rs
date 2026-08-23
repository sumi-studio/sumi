use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    io::{Read, Write},
    os::unix::net::{UnixListener, UnixStream},
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, PermissionsExt},
        unix::process::CommandExt,
    },
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    path::PathBuf,
    process::{Command, Output, Stdio},
    sync::{
        Mutex, MutexGuard, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use ed25519_dalek::{Signer, SigningKey};
use serde_json::Value as JsonValue;
use serde_yaml::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

const PAID_A: &str = "0198f0f4-9b72-7000-8000-000000000001";
const PAID_B: &str = "0198f0f4-9b72-7000-8000-000000000002";
const LOCAL_CONTROL_GID: u32 = 10022;
// This is deliberately not /run/sumi.  The deployment supervisor has fixed
// production trust anchors there, so integration tests run a private copy of
// the deployment artifact with the same anchors rooted below a fixture-owned
// directory.  Do not make the production roots configurable by environment.
const TEST_WRAPPING_KEY: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TEST_APPROVAL_DIGEST_KEY: &str =
    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
static HOST_FIXTURE_LOCK: Mutex<()> = Mutex::new(());
/// Fixed base for every fixture-private root.  `TMPDIR` is deliberately not
/// consulted: the private root is substituted verbatim into the supervisor's
/// Bash source, and a caller-controlled directory name could inject quoting or
/// command substitution there.
const FIXTURE_ROOT_BASE: &str = "/tmp";

type HostAnchorIdentity = Option<(u64, u64, u32, u32, u64)>;
type HostAnchorSnapshot = Vec<(&'static str, HostAnchorIdentity)>;

/// The one baseline every fixture in this process compares against.  Taking it
/// per fixture would let the first test that destroys a host anchor become the
/// next test's "before", so a real regression would be recorded as normal.
static HOST_RUN_BASELINE: OnceLock<HostAnchorSnapshot> = OnceLock::new();
static COMPOSE_ANCHOR_BINARY: OnceLock<PathBuf> = OnceLock::new();

fn compose_anchor_binary() -> &'static PathBuf {
    COMPOSE_ANCHOR_BINARY.get_or_init(|| {
        let output_path = std::env::temp_dir().join(format!(
            "sumi-compose-anchor-deployment-test-{}",
            std::process::id()
        ));
        let api_dir = deploy_dir()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("apps/api");
        let output = Command::new("go")
            .current_dir(api_dir)
            .args(["build", "-buildvcs=false", "-o"])
            .arg(&output_path)
            .arg("./cmd/compose-anchor")
            .output()
            .expect("run Go compiler for Compose anchor deployment fixture");
        assert!(
            output.status.success(),
            "build Compose anchor deployment fixture: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output_path
    })
}

fn host_run_baseline() -> &'static HostAnchorSnapshot {
    HOST_RUN_BASELINE.get_or_init(host_run_snapshot)
}

/// Identity of the live host trust anchors this suite must never touch.  The
/// deployment tests used to bind the real `/run` into a container and remove
/// these, which took down the running development stack.
fn host_run_snapshot() -> HostAnchorSnapshot {
    ["/run/sumi", "/run/sumi/local-control"]
        .into_iter()
        .map(|path| {
            let identity = std::fs::symlink_metadata(path).ok().map(|metadata| {
                (
                    metadata.dev(),
                    metadata.ino(),
                    metadata.uid(),
                    metadata.gid(),
                    metadata.nlink(),
                )
            });
            (path, identity)
        })
        .collect()
}

fn deploy_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("deploy/agent")
}

fn read_deploy(name: &str) -> String {
    std::fs::read_to_string(deploy_dir().join(name)).unwrap()
}

fn compose() -> Value {
    serde_yaml::from_str(&read_deploy("compose.yaml")).unwrap()
}

#[test]
fn process_disclosure_hardening_precedes_every_mode_and_supervisor_reads_public_broker_identity() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let main = std::fs::read_to_string(manifest.join("src/main.rs")).unwrap();
    let process_security =
        std::fs::read_to_string(manifest.join("src/runtime/process_security.rs")).unwrap();
    let hardening = main
        .find("disable_dumps_and_core_files()?")
        .expect("process disclosure hardening call");
    let argument_parse = main.find("env::args()").expect("mode argument parse");
    let allocator = main
        .find("--supervisor-allocate")
        .expect("allocator mode dispatch");
    assert!(hardening < argument_parse && argument_parse < allocator);
    assert!(process_security.contains("libc::setrlimit(libc::RLIMIT_CORE"));
    assert!(process_security.contains("libc::prctl(libc::PR_SET_DUMPABLE, 0"));

    let supervisor = read_deploy("supervisor");
    let epoch = supervisor
        .split("epoch_identity() {")
        .nth(1)
        .and_then(|body| body.split("\n}\n").next())
        .expect("supervisor epoch identity function");
    assert!(epoch.contains("identity-output/broker/identity.env"));
    assert!(!epoch.contains("identity-output/runtime/identity.env"));
    assert!(!epoch.contains("SUMI_EXECUTOR_CALL_AUTHORITY_PRIVATE_KEY"));
}

fn service<'a>(compose: &'a Value, name: &str) -> &'a Value {
    compose["services"]
        .get(name)
        .unwrap_or_else(|| panic!("missing Compose service {name}"))
}

fn volume_strings(service: &Value) -> Vec<&str> {
    service["volumes"]
        .as_sequence()
        .unwrap_or_else(|| panic!("service has no volumes"))
        .iter()
        .filter_map(Value::as_str)
        .collect()
}

fn volume_sources(service: &Value) -> BTreeSet<String> {
    service["volumes"]
        .as_sequence()
        .unwrap_or_else(|| panic!("service has no volumes"))
        .iter()
        .map(|mount| {
            mount
                .as_str()
                .map(|mount| mount.split(':').next().unwrap().to_owned())
                .or_else(|| mount["source"].as_str().map(str::to_owned))
                .expect("volume source")
        })
        .collect()
}

fn environment_keys(service: &Value) -> BTreeSet<String> {
    service["environment"]
        .as_mapping()
        .map(|environment| {
            environment
                .keys()
                .map(|key| {
                    key.as_str()
                        .expect("environment key must be text")
                        .to_owned()
                })
                .collect()
        })
        .unwrap_or_default()
}

fn assert_has_mount(service: &Value, expected: &str) {
    let mut parts = expected.split(':');
    let expected_source = parts.next().unwrap();
    let expected_target = parts.next().unwrap();
    let expected_read_only = parts.next() == Some("ro");
    let long_mount = service["volumes"]
        .as_sequence()
        .unwrap_or_else(|| panic!("service has no volumes"))
        .iter()
        .find(|mount| {
            mount["source"].as_str() == Some(expected_source)
                && mount["target"].as_str() == Some(expected_target)
                && mount["read_only"].as_bool().unwrap_or(false) == expected_read_only
        });
    if let Some(mount) = long_mount {
        assert_eq!(
            mount["volume"]["nocopy"].as_bool(),
            Some(true),
            "named volume mount {expected:?} must disable Docker copy-up"
        );
        return;
    }
    assert!(
        volume_strings(service).contains(&expected),
        "missing mount {expected:?}: {:?}",
        volume_strings(service)
    );
}

fn string_set(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn launch_env(command: &mut Command, paid: &str) {
    command
        .env("SUMI_CONFIG_FILE", "/dev/null")
        .env("SUMI_COMPOSE_ANCHOR", compose_anchor_binary())
        .env("SUMI_PERSONALITY_AGENT_ID", paid)
        .env("SUMI_GATEWAY_URL", "wss://gateway.invalid/agent")
        .env("SUMI_LOCAL_CONTROL_BEARER", "control-secret")
        .env("SUMI_LOCAL_CONTROL_BEARER_EXPIRES_AT_UNIX", "1900000000")
        .env("SUMI_AGENT_WRAPPING_KEY", TEST_WRAPPING_KEY)
        .env("SUMI_AGENT_WRAPPING_KEY_ID", "wrapping-key/v1")
        .env("SUMI_APPROVAL_SECRET_DIGEST_KEY", TEST_APPROVAL_DIGEST_KEY)
        .env("SUMI_PROVIDER_API_KEY", "provider-secret")
        .env(
            "SUMI_EXECUTION_REVIEWER_API_KEY",
            "execution-reviewer-secret",
        )
        .env("SUMI_EXECUTION_REVIEWER_MODEL_PRESET", "kimi-k3")
        .env(
            "SUMI_ESCALATION_REVIEWER_API_KEY",
            "escalation-reviewer-secret",
        )
        .env("SUMI_ESCALATION_REVIEWER_MODEL_PRESET", "glm-5.2");
}

/// Set to `1` on a host that genuinely cannot run the deployment fixture (no
/// Docker daemon, no cached base image, a root or role-colliding test uid).
/// Without it an unavailable host is a test failure, because a silent skip
/// makes a run that never exercised the isolation read exactly like a run that
/// did.
const FIXTURE_OPTIONAL_ENV: &str = "SUMI_DEPLOYMENT_FIXTURE_OPTIONAL";

static FIXTURE_RUNS: AtomicUsize = AtomicUsize::new(0);
static FIXTURE_SKIPS: AtomicUsize = AtomicUsize::new(0);

fn fixture_skip_is_opted_in() -> bool {
    std::env::var(FIXTURE_OPTIONAL_ENV).is_ok_and(|value| value == "1")
}

/// Either skip loudly and countably, or fail.  Never return quietly.
fn unavailable_host(reason: &str) {
    assert!(
        fixture_skip_is_opted_in(),
        "deployment fixture host is unavailable: {reason}\nSet {FIXTURE_OPTIONAL_ENV}=1 to \
         accept an unexercised deployment isolation on this host; a silent skip would report \
         a run that never provisioned anything as a passing run"
    );
    let skipped = FIXTURE_SKIPS.fetch_add(1, Ordering::SeqCst) + 1;
    eprintln!("HOST_FIXTURE_SKIPPED (opted in, count={skipped}): {reason}");
}

/// Reports why the Docker host cannot host the fixture, or `None` when it can.
/// A transient probe failure is cached like any other, so it is named in the
/// skip or failure rather than folded into "no Docker here".
fn docker_fixture_host_unavailable() -> Option<&'static str> {
    static UNAVAILABLE: OnceLock<Option<&'static str>> = OnceLock::new();
    *UNAVAILABLE.get_or_init(|| {
        let workdir = deploy_dir().parent().unwrap().parent().unwrap().to_owned();
        if !timeout_available() {
            return Some("the `timeout` utility is required to bound every Docker probe");
        }
        if !bounded_docker_output(&workdir, 30, &["info".into()])
            .status
            .success()
        {
            return Some(
                "`docker info` did not succeed within 30s: no reachable Docker daemon, or a \
                 daemon too slow to provision root-owned fixed trust anchors",
            );
        }
        if !bounded_docker_output(
            &workdir,
            30,
            &[
                "image".into(),
                "inspect".into(),
                "debian:bookworm-slim".into(),
            ],
        )
        .status
        .success()
        {
            return Some("the cached debian:bookworm-slim base image is required");
        }
        None
    })
}

struct HostTrustFixture {
    paid: String,
    project: String,
    lock_path: PathBuf,
    control_socket: PathBuf,
    runtime_secret_root: PathBuf,
    fixture_root: PathBuf,
    supervisor: PathBuf,
    host_run_before: HostAnchorSnapshot,
    control_gid: u32,
    cleaned: bool,
    listener: Option<UnixListener>,
    _guard: MutexGuard<'static, ()>,
}

impl HostTrustFixture {
    fn new() -> Option<Self> {
        let guard = HOST_FIXTURE_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // One process-wide baseline, taken before any fixture can touch anything.
        let host_run_before = host_run_baseline().clone();
        if let Some(reason) = docker_fixture_host_unavailable() {
            unavailable_host(reason);
            return None;
        }
        let server_uid = unsafe { libc::geteuid() };
        let control_gid = unsafe { libc::getegid() };
        if server_uid == 0 {
            unavailable_host(
                "the fixture needs a non-root test uid for the dedicated local-control peer",
            );
            return None;
        }
        if control_gid <= 999
            || control_gid == 65534
            || [10000, 10001, 10002, 10003, 10020, 10021].contains(&control_gid)
        {
            unavailable_host("the fixture needs a non-reserved, non-role primary test gid");
            return None;
        }
        let paid = Uuid::now_v7().to_string();
        let compact = paid.replace('-', "");
        let project = format!("sumi-{compact}");
        // Not `std::env::temp_dir()`: this path is substituted verbatim into the
        // supervisor's Bash source, so a `TMPDIR` carrying a space, a quote, or a
        // `$(...)` would rewrite the script rather than the root.  The base is
        // fixed and the suffix is generated hex.  The private root also carries
        // production's `local-control/<paid>/control.sock` tail, which still has
        // to fit in `sun_path`, so the prefix stays short.
        let unique = Uuid::now_v7().simple().to_string();
        let fixture_root = PathBuf::from(FIXTURE_ROOT_BASE)
            .join(format!("sumi-dep-{}", &unique[unique.len() - 12..]));
        let private_run_root = fixture_root.join("run/sumi");
        // Only the bind source is created here.  The private `/run/sumi` and
        // its lock root must be provisioned root-owned inside the container,
        // exactly like the production anchors the supervisor validates.
        std::fs::create_dir_all(fixture_root.join("run")).unwrap();
        let private_deploy_dir = fixture_root.join("deploy");
        copy_tree(&deploy_dir(), &private_deploy_dir);
        let supervisor = private_deploy_dir.join("supervisor");
        let private_run_root_text = private_run_root.display().to_string();
        assert!(
            private_run_root_text.starts_with('/')
                && private_run_root_text.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-')
                }),
            "the private root is substituted into Bash source and must carry no \
             shell metacharacter: {private_run_root_text}"
        );
        let published_source = std::fs::read_to_string(&supervisor).unwrap();
        // What the rewrite depends on, checked against the published artifact
        // instead of against the rewrite's own output.  These fail the day the
        // supervisor renames a root or stops declaring it as a literal, which is
        // exactly when a fixture would quietly start pointing at the host again.
        for declaration in [
            "readonly SUPERVISOR_LOCK_ROOT=/run/sumi/supervisor-locks",
            "readonly LOCAL_CONTROL_HOST_ROOT=/run/sumi/local-control",
        ] {
            assert!(
                published_source.contains(declaration),
                "published supervisor no longer declares {declaration:?}; the fixture rewrite \
                 cannot be trusted to move this root off the host"
            );
        }
        // A single pass over the published source.  Chained replacements would
        // rewrite the `/run/sumi` tail of an already-substituted private path
        // and produce `{fixture_root}/{fixture_root}/run/sumi/...`.
        let supervisor_source = published_source.replace("/run/sumi", &private_run_root_text);
        assert!(
            host_root_references_are_all_private(&supervisor_source, &fixture_root),
            "the private supervisor still references a host root outside {}",
            fixture_root.display()
        );
        std::fs::write(&supervisor, supervisor_source).unwrap();
        std::fs::set_permissions(&supervisor, std::fs::Permissions::from_mode(0o755)).unwrap();
        let lock_path = private_run_root.join(format!("supervisor-locks/{project}.lock"));
        let control_dir = private_run_root.join(format!("local-control/{compact}"));
        let control_socket = control_dir.join("control.sock");
        // `sun_path` is 108 bytes including the terminator, and the swap test
        // renames the socket to `control.sock.swapped` beside it.
        assert!(
            control_socket.as_os_str().len() + ".swapped".len() < 108,
            "fixture local-control socket path does not fit in sun_path; \
             use a shorter TMPDIR: {}",
            control_socket.display()
        );
        let runtime_secret_root =
            PathBuf::from(FIXTURE_ROOT_BASE).join(format!("sumi-runtime-secrets-{compact}"));
        std::fs::create_dir(&runtime_secret_root).unwrap();
        std::fs::set_permissions(&runtime_secret_root, std::fs::Permissions::from_mode(0o700))
            .unwrap();
        let setup_script = r#"
set -eu
umask 022
mkdir -p /host-run/sumi /host-run/sumi/supervisor-locks
for anchor in /host-run/sumi /host-run/sumi/supervisor-locks; do
  test "$(stat -c %u "$anchor")" = 0
  mode="$(stat -c %a "$anchor")"
  test $((8#$mode & 0022)) = 0
done
install -d -m 0750 -o "$3" -g "$4" /host-run/sumi/local-control
install -d -m 0750 -o "$3" -g "$4" "/host-run/sumi/local-control/$1"
install -m 0600 -o "$3" -g "$4" /dev/null "/host-run/sumi/supervisor-locks/$2.lock"
"#;
        let setup = bounded_docker_output(
            deploy_dir().parent().unwrap().parent().unwrap(),
            30,
            &[
                "run".into(),
                "--rm".into(),
                "--network".into(),
                "none".into(),
                "-v".into(),
                format!("{}:/host-run", fixture_root.join("run").display()),
                "debian:bookworm-slim".into(),
                "bash".into(),
                "-c".into(),
                setup_script.into(),
                "--".into(),
                compact.clone(),
                project.clone(),
                server_uid.to_string(),
                control_gid.to_string(),
            ],
        );
        // Docker and the image were already confirmed above, so a failure here
        // is our own provisioning breaking, not an absent environment.  Skipping
        // it would report a green run for a fixture that never executed.
        if !setup.status.success() {
            purge_private_run_root(&fixture_root);
            let _ = std::fs::remove_dir_all(&runtime_secret_root);
            let _ = std::fs::remove_dir_all(&fixture_root);
            panic!(
                "fixed trust-anchor provisioning failed under a working Docker host; \
                 this is a broken fixture, not an unavailable environment: status={:?}\n\
                 stdout: {}\nstderr: {}",
                setup.status,
                String::from_utf8_lossy(&setup.stdout),
                String::from_utf8_lossy(&setup.stderr)
            );
        }
        assert_eq!(
            host_run_snapshot(),
            host_run_before,
            "deployment fixture changed the live host trust anchors while provisioning"
        );

        let listener = UnixListener::bind(&control_socket).unwrap();
        std::fs::set_permissions(&control_socket, std::fs::Permissions::from_mode(0o660)).unwrap();
        let run = FIXTURE_RUNS.fetch_add(1, Ordering::SeqCst) + 1;
        eprintln!(
            "HOST_FIXTURE_ACTIVE (count={run}): project={project} run_root={private_run_root_text} \
             supervisor={} lock={} control={}",
            supervisor.display(),
            lock_path.display(),
            control_socket.display()
        );

        Some(Self {
            paid,
            project,
            lock_path,
            control_socket,
            runtime_secret_root,
            fixture_root,
            supervisor,
            host_run_before,
            control_gid,
            cleaned: false,
            listener: Some(listener),
            _guard: guard,
        })
    }

    fn apply_launch(&self, command: &mut Command) {
        launch_env(command, &self.paid);
        command
            .env(
                "SUMI_LOCAL_CONTROL_SERVER_UID",
                unsafe { libc::geteuid() }.to_string(),
            )
            .env(
                "SUMI_LOCAL_CONTROL_SOCKET_GID",
                self.control_gid.to_string(),
            );
    }

    fn supervisor_command(&self) -> Command {
        Command::new(&self.supervisor)
    }

    /// The private deployment copy the fixture supervisor resolves its Compose
    /// files against.  Lifecycle assertions must expect these paths, not the
    /// published ones.
    fn deploy_dir(&self) -> PathBuf {
        self.supervisor.parent().unwrap().to_path_buf()
    }

    fn cleanup(&mut self) -> Result<(), String> {
        // Every container below binds `{fixture_root}/run` as its mount source,
        // and Docker recreates a missing bind source as a root-owned directory.
        // A second cleanup (the acceptance tests call it, then `Drop` runs) must
        // therefore not repeat the work it already evidenced.
        if self.cleaned {
            self.assert_host_anchors_intact("post-teardown");
            return Ok(());
        }
        self.cleaned = true;
        self.listener.take();
        let compact = self.paid.replace('-', "");
        let cleanup_script = r#"
set -eu
rm -f "/host-run/sumi/local-control/$1/control.sock"
rm -f "/host-run/sumi/local-control/$1/control.sock.swapped"
rmdir "/host-run/sumi/local-control/$1" 2>/dev/null || true
rm -f "/host-run/sumi/supervisor-locks/$2.lock"
rmdir /host-run/sumi/local-control 2>/dev/null || true
rmdir /host-run/sumi/supervisor-locks 2>/dev/null || true
rmdir /host-run/sumi 2>/dev/null || true
"#;
        let cleanup = try_bounded_docker_output(
            deploy_dir().parent().unwrap().parent().unwrap(),
            30,
            &[
                "run".into(),
                "--rm".into(),
                "--network".into(),
                "none".into(),
                "-v".into(),
                format!("{}:/host-run", self.fixture_root.join("run").display()),
                "debian:bookworm-slim".into(),
                "bash".into(),
                "-c".into(),
                cleanup_script.into(),
                "--".into(),
                compact,
                self.project.clone(),
            ],
        );
        let mut errors = Vec::new();
        match cleanup {
            Ok(cleanup) if cleanup.status.success() => {}
            Ok(cleanup) => errors.push(format!(
                "cannot remove exact host trust fixture {}: {}",
                self.project,
                String::from_utf8_lossy(&cleanup.stderr)
            )),
            Err(error) => errors.push(format!(
                "cannot run exact host trust fixture cleanup {}: {error}",
                self.project
            )),
        }
        if self.lock_path.exists() {
            errors.push(format!(
                "exact host trust lock survived cleanup: {}",
                self.lock_path.display()
            ));
        }
        let control_dir = self.control_socket.parent().unwrap();
        if control_dir.exists() {
            errors.push(format!(
                "exact host trust control directory survived cleanup: {}",
                control_dir.display()
            ));
        }
        make_tree_removable(&self.runtime_secret_root);
        if let Err(error) = std::fs::remove_dir_all(&self.runtime_secret_root)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            errors.push(format!(
                "cannot remove exact runtime secret fixture {}: {error}",
                self.runtime_secret_root.display()
            ));
        }
        purge_private_run_root(&self.fixture_root);
        make_tree_removable(&self.fixture_root);
        if let Err(error) = std::fs::remove_dir_all(&self.fixture_root)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            errors.push(format!(
                "cannot remove private deployment fixture {}: {error}",
                self.fixture_root.display()
            ));
        }
        // The isolation regression itself is never demoted to a returned error.
        // Only two call sites inspected `cleanup()`'s result, so a destroyed host
        // anchor used to leave twelve of thirteen fixture tests green.
        self.assert_host_anchors_intact("teardown");
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("\n"))
        }
    }

    /// Fails whichever test is running, not just the two that inspected
    /// `cleanup()`. Every fixture compares against the same process-wide
    /// baseline, so a fixture that starts after another one destroyed an anchor
    /// fails too instead of adopting the damage as its own "before".
    fn assert_host_anchors_intact(&self, stage: &str) {
        let host_run_after = host_run_snapshot();
        if host_run_after == self.host_run_before {
            eprintln!("HOST_ANCHORS_UNCHANGED ({stage}): {host_run_after:?}");
            return;
        }
        let report = format!(
            "the deployment fixture changed the live host trust anchors at {stage}: \
             baseline={:?} now={host_run_after:?}",
            self.host_run_before
        );
        eprintln!("HOST_ANCHORS_CHANGED ({stage}): {report}");
        // Panicking while already unwinding aborts the process and would bury
        // the original failure. That test is failing either way; the stderr
        // record above is what carries this one.
        assert!(std::thread::panicking(), "{report}");
    }
}

impl Drop for HostTrustFixture {
    fn drop(&mut self) {
        // `cleanup()` is idempotent, so an acceptance test that already called
        // it fallibly gets `Ok` here. Its host-anchor check is a panic rather
        // than a returned error, so it reaches every fixture test through here.
        let _ = self.cleanup();
    }
}

fn launch_runtime_env(command: &mut Command, fixture: &HostTrustFixture) {
    fixture.apply_launch(command);
    command
        .env("SUMI_DEV_ALLOW_APPARMOR_UNCONFINED", "true")
        .env("SUMI_TEST_ALLOW_NONROOT_SECRET_ROOT", "true")
        .env(
            "SUMI_RUNTIME_SECRET_HOST_ROOT",
            &fixture.runtime_secret_root,
        )
        .env_remove("SUMI_LOCAL_CONTROL_HOST_ROOT")
        .env_remove("SUMI_SUPERVISOR_LOCK_DIR");
}

fn launch_owned_acceptance_env(command: &mut Command, fixture: &HostTrustFixture) {
    let credential = |name: &str| format!("deployment-test-{name}-{}", Uuid::now_v7());
    let hex_credential = || {
        let value = Uuid::now_v7().simple().to_string();
        format!("{value}{value}")
    };
    command
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("SUMI_CONFIG_FILE", "/dev/null")
        .env("SUMI_COMPOSE_ANCHOR", compose_anchor_binary())
        .env("SUMI_COMPOSE_TIMEOUT", "10")
        .env("SUMI_DEV_ALLOW_APPARMOR_UNCONFINED", "true")
        .env("SUMI_PERSONALITY_AGENT_ID", &fixture.paid)
        .env("SUMI_GATEWAY_URL", "wss://gateway.invalid/deployment-test")
        .env("SUMI_LOCAL_CONTROL_BEARER", credential("local-control"))
        .env("SUMI_LOCAL_CONTROL_BEARER_EXPIRES_AT_UNIX", "1900000000")
        .env("SUMI_AGENT_WRAPPING_KEY", hex_credential())
        .env("SUMI_AGENT_WRAPPING_KEY_ID", "deployment-test/wrapping")
        .env("SUMI_APPROVAL_SECRET_DIGEST_KEY", hex_credential())
        .env("SUMI_PROVIDER_API_KEY", credential("provider"))
        .env(
            "SUMI_EXECUTION_REVIEWER_API_KEY",
            credential("execution-reviewer"),
        )
        .env("SUMI_EXECUTION_REVIEWER_MODEL_PRESET", "kimi-k3")
        .env(
            "SUMI_ESCALATION_REVIEWER_API_KEY",
            credential("escalation-reviewer"),
        )
        .env("SUMI_ESCALATION_REVIEWER_MODEL_PRESET", "glm-5.2")
        .env(
            "SUMI_LOCAL_CONTROL_SERVER_UID",
            unsafe { libc::geteuid() }.to_string(),
        )
        .env(
            "SUMI_LOCAL_CONTROL_SOCKET_GID",
            fixture.control_gid.to_string(),
        )
        // Direct `docker compose down` parses the full descriptor too, so it
        // must receive this fixture-owned bind source even though it will not
        // start the runtime service.
        .env(
            "SUMI_LOCAL_CONTROL_HOST_DIR",
            fixture.control_socket.parent().unwrap(),
        )
        .env(
            "SUMI_RUNTIME_SECRET_HOST_DIR",
            fixture
                .runtime_secret_root
                .join(fixture.paid.replace('-', ""))
                .join("acceptance-cleanup"),
        );
    preserve_docker_transport(command);
}

fn cleanup_owned_compose_resources(fixture: &HostTrustFixture) -> Result<(), String> {
    // `stop` intentionally keeps project resources for operator status and
    // cleanup. This test owns only its UUID-derived project, so direct Compose
    // cleanup is exact-scoped and must be evidenced even after a test panic.
    let mut cleanup = Command::new("timeout");
    cleanup
        .args(["--preserve-status", "60s", "docker", "compose"])
        .args(["--project-name", &fixture.project, "--file"])
        .arg(deploy_dir().join("compose.yaml"))
        .args([
            "down",
            "--remove-orphans",
            "--volumes",
            "--rmi",
            "local",
            "--timeout",
            "10",
        ]);
    launch_owned_acceptance_env(&mut cleanup, fixture);

    let mut errors = Vec::new();
    match cleanup.output() {
        Err(error) => errors.push(format!(
            "cannot run owned Compose resource cleanup: {error}"
        )),
        Ok(cleanup) if !cleanup.status.success() => errors.push(format!(
            "owned Compose resource cleanup failed: {}",
            String::from_utf8_lossy(&cleanup.stderr)
        )),
        Ok(_) => {}
    }

    let project_label = format!("label=com.docker.compose.project={}", fixture.project);
    for (kind, args) in [
        (
            "containers",
            vec![
                "ps".into(),
                "--all".into(),
                "--quiet".into(),
                "--filter".into(),
                project_label.clone(),
            ],
        ),
        (
            "networks",
            vec![
                "network".into(),
                "ls".into(),
                "--quiet".into(),
                "--filter".into(),
                project_label.clone(),
            ],
        ),
        (
            "volumes",
            vec![
                "volume".into(),
                "ls".into(),
                "--quiet".into(),
                "--filter".into(),
                project_label.clone(),
            ],
        ),
        (
            "local images",
            vec![
                "image".into(),
                "ls".into(),
                "--quiet".into(),
                "--filter".into(),
                format!("reference={}-*", fixture.project),
            ],
        ),
    ] {
        let leftovers =
            try_bounded_docker_output(deploy_dir().parent().unwrap().parent().unwrap(), 20, &args);
        match leftovers {
            Err(error) => errors.push(format!("cannot inspect owned Compose {kind}: {error}")),
            Ok(leftovers) if !leftovers.status.success() => errors.push(format!(
                "cannot inspect owned Compose {kind}: {}",
                String::from_utf8_lossy(&leftovers.stderr)
            )),
            Ok(leftovers) if !leftovers.stdout.is_empty() => errors.push(format!(
                "owned Compose {kind} survived cleanup: {}",
                String::from_utf8_lossy(&leftovers.stdout)
            )),
            Ok(_) => {}
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n"))
    }
}

/// Wait for a marker that a process this test does not `wait()` on writes.
/// Asserting on it the instant the supervisor exits turns host load into a test
/// failure: the signalled grandchild's trap has not necessarily run yet.
fn assert_marker(markers: &std::path::Path, name: &str) {
    let path = markers.join(name);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "marker {name} was never written under {}",
            markers.display()
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_child_exit(
    child: &mut std::process::Child,
    timeout: Duration,
) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn try_bounded_docker_output(
    workdir: &std::path::Path,
    seconds: u64,
    args: &[String],
) -> Result<Output, String> {
    let mut command = Command::new("timeout");
    command
        .arg("--preserve-status")
        .arg(format!("{seconds}s"))
        .arg("docker")
        .args(args)
        .current_dir(workdir)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default());
    preserve_docker_transport(&mut command);
    command
        .output()
        .map_err(|error| format!("run bounded Docker command: {error}"))
}

fn bounded_docker_output(workdir: &std::path::Path, seconds: u64, args: &[String]) -> Output {
    try_bounded_docker_output(workdir, seconds, args).expect("run bounded Docker command")
}

fn preserve_docker_transport(command: &mut Command) {
    for key in [
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "DOCKER_TLS",
        "DOCKER_TLS_VERIFY",
        "DOCKER_CERT_PATH",
        "DOCKER_CONFIG",
        "HOME",
    ] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
}

fn timeout_available() -> bool {
    Command::new("timeout")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

fn finish_opt_in_docker_test(body: std::thread::Result<()>, cleanup: Result<(), String>) {
    match (body, cleanup) {
        (Ok(()), Ok(())) => {}
        (Ok(()), Err(cleanup)) => panic!("owned Docker cleanup evidence failed: {cleanup}"),
        (Err(payload), Ok(())) => resume_unwind(payload),
        (Err(payload), Err(cleanup)) => {
            let original = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic payload");
            panic!(
                "test body failed before cleanup: {original}\nowned Docker cleanup evidence also failed: {cleanup}"
            );
        }
    }
}

struct OwnedExecutorDockerSmoke {
    root: PathBuf,
    image: String,
    container: String,
}

impl OwnedExecutorDockerSmoke {
    fn new() -> Self {
        let unique = Uuid::now_v7().simple().to_string();
        let root = std::env::temp_dir().join(format!("sumi-executor-smoke-{unique}"));
        let image = format!("sumi-executor-smoke-{unique}:latest");
        let container = format!("sumi-executor-smoke-{unique}");
        std::fs::create_dir_all(root.join("workspace")).unwrap();
        std::fs::create_dir_all(root.join("identity")).unwrap();
        std::fs::create_dir_all(root.join("executor")).unwrap();
        Self {
            root,
            image,
            container,
        }
    }

    fn docker(&self, seconds: u64, args: Vec<String>) -> Output {
        bounded_docker_output(
            deploy_dir().parent().unwrap().parent().unwrap(),
            seconds,
            &args,
        )
    }

    fn try_docker(&self, seconds: u64, args: Vec<String>) -> Result<Output, String> {
        try_bounded_docker_output(
            deploy_dir().parent().unwrap().parent().unwrap(),
            seconds,
            &args,
        )
    }

    fn cleanup(&self) -> Result<(), String> {
        let mut errors = Vec::new();
        let containers = self.try_docker(
            20,
            vec![
                "container".into(),
                "ls".into(),
                "--all".into(),
                "--quiet".into(),
                "--filter".into(),
                format!("name=^/{}$", self.container),
            ],
        );
        match containers {
            Err(error) => errors.push(format!(
                "cannot list exact owned container {}: {error}",
                self.container
            )),
            Ok(containers) if !containers.status.success() => errors.push(format!(
                "cannot list exact owned container {}: {}",
                self.container,
                String::from_utf8_lossy(&containers.stderr)
            )),
            Ok(containers) if !containers.stdout.is_empty() => {
                let remove = self.try_docker(
                    20,
                    vec![
                        "container".into(),
                        "rm".into(),
                        "--force".into(),
                        self.container.clone(),
                    ],
                );
                match remove {
                    Err(error) => errors.push(format!(
                        "cannot remove exact owned container {}: {error}",
                        self.container
                    )),
                    Ok(remove) if !remove.status.success() => errors.push(format!(
                        "cannot remove exact owned container {}: {}",
                        self.container,
                        String::from_utf8_lossy(&remove.stderr)
                    )),
                    Ok(_) => {}
                }
            }
            Ok(_) => {}
        }

        if self.root.exists() {
            // Container setup deliberately gives the fixture to the executor
            // uid. Reclaim only this UUID-owned mount before local removal.
            let reclaim = self.try_docker(
                20,
                vec![
                    "run".into(),
                    "--rm".into(),
                    "--network".into(),
                    "none".into(),
                    "-v".into(),
                    format!("{}:/fixture", self.root.display()),
                    "debian:bookworm-slim".into(),
                    "sh".into(),
                    "-ec".into(),
                    format!(
                        "chown -R {}:{} /fixture",
                        unsafe { libc::geteuid() },
                        unsafe { libc::getegid() }
                    ),
                ],
            );
            match reclaim {
                Err(error) => errors.push(format!(
                    "cannot reclaim exact owned fixture {}: {error}",
                    self.root.display()
                )),
                Ok(reclaim) if !reclaim.status.success() => errors.push(format!(
                    "cannot reclaim exact owned fixture {}: {}",
                    self.root.display(),
                    String::from_utf8_lossy(&reclaim.stderr)
                )),
                Ok(_) => {}
            }
            make_tree_removable(&self.root);
            if let Err(error) = std::fs::remove_dir_all(&self.root) {
                errors.push(format!(
                    "cannot remove exact owned fixture {}: {error}",
                    self.root.display()
                ));
            }
        }

        let images = self.try_docker(
            20,
            vec![
                "image".into(),
                "ls".into(),
                "--quiet".into(),
                "--filter".into(),
                format!("reference={}", self.image),
            ],
        );
        match images {
            Err(error) => errors.push(format!(
                "cannot list exact owned image {}: {error}",
                self.image
            )),
            Ok(images) if !images.status.success() => errors.push(format!(
                "cannot list exact owned image {}: {}",
                self.image,
                String::from_utf8_lossy(&images.stderr)
            )),
            Ok(images) if !images.stdout.is_empty() => {
                let remove =
                    self.try_docker(20, vec!["image".into(), "rm".into(), self.image.clone()]);
                match remove {
                    Err(error) => errors.push(format!(
                        "cannot remove exact owned image {}: {error}",
                        self.image
                    )),
                    Ok(remove) if !remove.status.success() => errors.push(format!(
                        "cannot remove exact owned image {}: {}",
                        self.image,
                        String::from_utf8_lossy(&remove.stderr)
                    )),
                    Ok(_) => {}
                }
            }
            Ok(_) => {}
        }

        let container_left = self.try_docker(
            20,
            vec![
                "container".into(),
                "ls".into(),
                "--all".into(),
                "--quiet".into(),
                "--filter".into(),
                format!("name=^/{}$", self.container),
            ],
        );
        match container_left {
            Err(error) => errors.push(format!(
                "cannot inspect exact owned container postcondition: {error}"
            )),
            Ok(container_left)
                if !container_left.status.success() || !container_left.stdout.is_empty() =>
            {
                errors.push(format!(
                    "exact owned container survived cleanup: stdout: {}; stderr: {}",
                    String::from_utf8_lossy(&container_left.stdout),
                    String::from_utf8_lossy(&container_left.stderr)
                ));
            }
            Ok(_) => {}
        }
        let image_left = self.try_docker(
            20,
            vec![
                "image".into(),
                "ls".into(),
                "--quiet".into(),
                "--filter".into(),
                format!("reference={}", self.image),
            ],
        );
        match image_left {
            Err(error) => errors.push(format!(
                "cannot inspect exact owned image postcondition: {error}"
            )),
            Ok(image_left) if !image_left.status.success() || !image_left.stdout.is_empty() => {
                errors.push(format!(
                    "exact owned image survived cleanup: stdout: {}; stderr: {}",
                    String::from_utf8_lossy(&image_left.stdout),
                    String::from_utf8_lossy(&image_left.stderr)
                ));
            }
            Ok(_) => {}
        }
        if self.root.exists() {
            errors.push(format!(
                "exact owned fixture root survived cleanup: {}",
                self.root.display()
            ));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("\n"))
        }
    }
}

impl Drop for OwnedExecutorDockerSmoke {
    fn drop(&mut self) {
        // The test itself calls this fallibly and asserts its postconditions.
        // Drop is only an emergency best-effort fallback for abrupt unwinding.
        let _ = self.cleanup();
    }
}

fn exchange_executor_socket(socket: &std::path::Path, request: JsonValue) -> JsonValue {
    let mut stream = UnixStream::connect(socket).expect("connect owned executor socket");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut request = serde_json::to_vec(&request).unwrap();
    request.push(b'\n');
    stream.write_all(&request).expect("write executor request");
    stream.shutdown(std::net::Shutdown::Write).unwrap();
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read executor response");
    serde_json::from_str(response.trim()).expect("decode executor response")
}

fn signed_executor_authority(
    generation: u64,
    nonce: &str,
    request_id: &str,
    operation: &JsonValue,
    signing_key: &SigningKey,
) -> JsonValue {
    let digest = |domain: &[u8], value: &[u8]| {
        let mut digest = Sha256::new();
        digest.update(domain);
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
        digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    let now = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let claims = serde_json::json!({
        "version": 1,
        "authority_id": Uuid::now_v7().hyphenated().to_string(),
        "audience": "sumi.tool-executor.read.v1",
        "generation": generation,
        "boot_nonce_digest": digest(b"sumi.executor.boot-nonce-digest.v1\0", nonce.as_bytes()),
        "request_id": request_id,
        "execution_id": operation["execution_id"],
        "operation_digest": digest(
            b"sumi.executor.operation-digest.v1\0",
            &serde_json::to_vec(operation).unwrap(),
        ),
        "permit": {
            "grant_digest": "44".repeat(32),
            "bound_evidence_digest": "11".repeat(32),
            "action_digest": "33".repeat(32),
            "authorization_projection_digest": "22".repeat(32),
            "route": "normal",
            "resolved_authority": "agent_own",
        },
        "issued_at_unix_ms": now,
        "expires_at_unix_ms": now + 30_000,
    });
    let key_id = "sumi.executor.call-authority.ed25519.v1";
    let encoded = serde_json::to_vec(&serde_json::json!({
        "key_id": key_id,
        "claims": &claims,
    }))
    .unwrap();
    let mut payload = b"sumi.executor.call-authority.signature.v1\0".to_vec();
    payload.extend_from_slice(&(encoded.len() as u64).to_be_bytes());
    payload.extend_from_slice(&encoded);
    let signature = signing_key.sign(&payload);
    serde_json::json!({
        "key_id": key_id,
        "claims": claims,
        "signature": signature.to_bytes().iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
    })
}

#[test]
fn compose_has_no_global_name_and_supervisor_derives_one_project_per_paid() {
    let source = read_deploy("compose.yaml");
    let parsed = compose();
    assert!(
        parsed.get("name").is_none(),
        "Compose must not fix a global project name"
    );
    assert!(!source.contains("name: sumi-agent"));

    let supervisor = deploy_dir().join("supervisor");
    let project = |paid: &str| {
        let mut command = Command::new(&supervisor);
        command.arg("project-name");
        launch_env(&mut command, paid);
        let output = command.output().unwrap();
        assert!(output.status.success(), "{}", supervisor_failure(&output));
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    };
    let project_a = project(PAID_A);
    let project_b = project(PAID_B);
    assert_eq!(project_a, "sumi-0198f0f49b7270008000000000000001");
    assert_eq!(project_b, "sumi-0198f0f49b7270008000000000000002");
    assert_ne!(project_a, project_b);
    assert!(
        project_a
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    );
}

#[test]
fn allocator_state_and_role_identity_are_not_shared_with_long_lived_services() {
    let compose = compose();
    let allocator = service(&compose, "allocator");
    let runtime = service(&compose, "runtime");
    let executor = service(&compose, "executor");
    let broker = service(&compose, "broker");

    assert_eq!(
        volume_sources(allocator),
        string_set(&[
            "allocator-root",
            "allocator-state",
            "broker-identity",
            "executor-identity",
            "runtime-identity",
        ])
    );
    assert_eq!(allocator["network_mode"].as_str(), Some("none"));
    assert_eq!(allocator["user"].as_str(), Some("0:0"));
    assert_eq!(
        allocator["environment"]["SUMI_ALLOCATOR_TRUST_ROOT"].as_str(),
        Some("/var/lib/sumi-allocator-root")
    );
    assert_eq!(
        allocator["environment"]["SUMI_ALLOCATOR_STATE_DIR"].as_str(),
        Some("/var/lib/sumi-allocator-root/state")
    );
    assert_eq!(
        allocator["environment"]["SUMI_IDENTITY_OUTPUT_ROOT"].as_str(),
        Some("/var/lib/sumi-allocator-root/identity-output")
    );
    assert_eq!(
        allocator["environment"]["SUMI_RUNTIME_IDENTITY_GID"].as_str(),
        Some("10001")
    );
    assert_eq!(
        allocator["environment"]["SUMI_EXECUTOR_IDENTITY_GID"].as_str(),
        Some("10002")
    );
    assert_eq!(
        allocator["environment"]["SUMI_BROKER_IDENTITY_GID"].as_str(),
        Some("10003")
    );
    for long_lived in [runtime, executor, broker] {
        assert!(
            !volume_sources(long_lived).contains("allocator-state")
                && !volume_sources(long_lived).contains("allocator-root"),
            "allocator trust state leaked into a long-lived role"
        );
    }

    assert_has_mount(runtime, "runtime-identity:/run/sumi/identity:ro");
    assert_has_mount(executor, "executor-identity:/run/sumi/identity:ro");
    assert_has_mount(broker, "broker-identity:/run/sumi/identity:ro");
    assert!(!volume_sources(runtime).contains("executor-identity"));
    assert!(!volume_sources(runtime).contains("broker-identity"));
    assert!(!volume_sources(executor).contains("runtime-identity"));
    assert!(!volume_sources(broker).contains("runtime-identity"));

    let entrypoint = read_deploy("container-entrypoint");
    assert!(entrypoint.contains("/usr/local/bin/sumi-agent --supervisor-allocate"));
    assert!(entrypoint.contains("identity_key_allowed"));
    assert!(entrypoint.contains("declare -A seen"));
    assert!(entrypoint.contains("verify_identity_output"));
    assert!(entrypoint.contains("0:${gid}:550"));
    assert!(entrypoint.contains("0:${gid}:440:1"));
    assert!(!entrypoint.contains("lock_identity_output"));
    assert!(!entrypoint.contains("\nsource "));
    assert!(!entrypoint.contains("\neval "));
    let allocator_branch = entrypoint
        .split("  allocator)")
        .nth(1)
        .unwrap()
        .split("  prepare)")
        .next()
        .unwrap();
    assert!(!allocator_branch.contains("chown"));
    assert!(!allocator_branch.contains("chmod"));
    for mount in volume_strings(allocator) {
        let target = mount.split(':').nth(1).unwrap();
        assert!(
            target == "/var/lib/sumi-allocator-root"
                || target.starts_with("/var/lib/sumi-allocator-root/"),
            "allocator mount escaped the pinned trust root: {mount}"
        );
    }
    let dockerfile = read_deploy("Dockerfile");
    assert!(dockerfile.contains("install -d -m 0700 /var/lib/sumi-allocator-root"));
    for target in [
        "/run/sumi/local-control/control.sock",
        "/run/sumi/identity",
        "/run/sumi/executor",
        "/run/sumi/broker",
        "/run/secrets/sumi_local_control_bearer",
        "/run/secrets/sumi_agent_wrapping_key",
        "/run/secrets/sumi_approval_secret_digest_key",
        "/run/secrets/sumi_provider_api_key",
        "/run/secrets/sumi_execution_reviewer_api_key",
        "/run/secrets/sumi_escalation_reviewer_api_key",
    ] {
        assert!(
            dockerfile.contains(target),
            "read-only runtime image omits bind target {target}"
        );
    }
    for role in ["runtime", "executor", "broker"] {
        assert!(dockerfile.contains(&format!(
            "/var/lib/sumi-allocator-root/identity-output/{role}"
        )));
    }
}

#[test]
fn deployed_allocator_cli_durably_advances_two_generations_without_rebinding_outputs() {
    let Some(role_gids) = usable_allocator_role_gids() else {
        unavailable_host(
            "allocator integration requires three usable supplemental groups or chgrp authority",
        );
        return;
    };
    let root = std::env::temp_dir().join(format!("sumi-deploy-allocator-{}", Uuid::now_v7()));
    let state = root.join("state");
    let output = root.join("identity-output");
    std::fs::create_dir_all(&state).unwrap();
    std::fs::create_dir_all(&output).unwrap();
    for role in ["runtime", "executor", "broker"] {
        std::fs::create_dir(output.join(role)).unwrap();
    }
    for directory in [&root, &state, &output] {
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    for role in ["runtime", "executor", "broker"] {
        std::fs::set_permissions(output.join(role), std::fs::Permissions::from_mode(0o700))
            .unwrap();
    }
    if !can_assign_allocator_role_gids(&output, &role_gids) {
        make_tree_removable(&root);
        let _ = std::fs::remove_dir_all(root);
        unavailable_host(
            "allocator integration requires three usable supplemental groups or chgrp authority",
        );
        return;
    }

    let paid = Uuid::now_v7().to_string();
    let allocate = || {
        let output = Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
            .env_clear()
            .arg("--supervisor-allocate")
            .env("SUMI_PERSONALITY_AGENT_ID", &paid)
            .env("SUMI_ALLOCATOR_TRUST_ROOT", &root)
            .env("SUMI_ALLOCATOR_STATE_DIR", &state)
            .env("SUMI_IDENTITY_OUTPUT_ROOT", &output)
            .env("SUMI_RUNTIME_IDENTITY_GID", role_gids[0].to_string())
            .env("SUMI_EXECUTOR_IDENTITY_GID", role_gids[1].to_string())
            .env("SUMI_BROKER_IDENTITY_GID", role_gids[2].to_string())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "allocator CLI failed: {}",
            supervisor_failure(&output)
        );
        serde_json::from_slice::<JsonValue>(&output.stdout).unwrap()
    };

    let first = allocate();
    assert_eq!(first["status"].as_str(), Some("allocated"));
    assert_eq!(first["generation"].as_u64(), Some(0));
    let bound_inodes = allocator_directory_inodes(&root, &state, &output);
    assert_allocator_persistence(&state, &output, &paid, &bound_inodes, 1);
    let first_identities = assert_allocator_identities(&output, &paid, &role_gids, 0);
    assert_no_allocator_temps_or_interrupted_handoff(&state, &output);

    let second = allocate();
    assert_eq!(second["status"].as_str(), Some("allocated"));
    assert_eq!(second["generation"].as_u64(), Some(1));
    assert_eq!(
        allocator_directory_inodes(&root, &state, &output),
        bound_inodes
    );
    assert_allocator_persistence(&state, &output, &paid, &bound_inodes, 2);
    let second_identities = assert_allocator_identities(&output, &paid, &role_gids, 1);
    for role in ["runtime", "executor", "broker"] {
        assert_ne!(first_identities[role], second_identities[role]);
    }
    assert_no_allocator_temps_or_interrupted_handoff(&state, &output);

    make_tree_removable(&root);
    std::fs::remove_dir_all(root).unwrap();
}

fn usable_allocator_role_gids() -> Option<[libc::gid_t; 3]> {
    if unsafe { libc::geteuid() } == 0 {
        return Some([61_001, 61_002, 61_003]);
    }
    let count = unsafe { libc::getgroups(0, std::ptr::null_mut()) };
    if count < 0 {
        return None;
    }
    let mut groups = vec![0 as libc::gid_t; count as usize];
    if count > 0 && unsafe { libc::getgroups(count, groups.as_mut_ptr()) } != count {
        return None;
    }
    let real_gid = unsafe { libc::getgid() };
    let effective_gid = unsafe { libc::getegid() };
    let groups: Vec<_> = groups
        .into_iter()
        .filter(|gid| *gid != 0 && *gid != real_gid && *gid != effective_gid)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    (groups.len() >= 3).then(|| [groups[0], groups[1], groups[2]])
}

fn can_assign_allocator_role_gids(output: &std::path::Path, role_gids: &[libc::gid_t; 3]) -> bool {
    let allocator_gid = unsafe { libc::getegid() };
    ["runtime", "executor", "broker"]
        .into_iter()
        .zip(*role_gids)
        .all(|(role, gid)| {
            let directory = std::fs::OpenOptions::new()
                .read(true)
                .open(output.join(role))
                .ok();
            let Some(directory) = directory else {
                return false;
            };
            (unsafe { libc::fchown(directory.as_raw_fd(), libc::uid_t::MAX, gid) }) == 0
                && (unsafe { libc::fchown(directory.as_raw_fd(), libc::uid_t::MAX, allocator_gid) })
                    == 0
        })
}

fn allocator_directory_inodes(
    root: &std::path::Path,
    state: &std::path::Path,
    output: &std::path::Path,
) -> BTreeMap<&'static str, (u64, u64)> {
    [
        ("trust_root", root.to_path_buf()),
        ("state", state.to_path_buf()),
        ("output", output.to_path_buf()),
        ("runtime", output.join("runtime")),
        ("executor", output.join("executor")),
        ("broker", output.join("broker")),
    ]
    .into_iter()
    .map(|(name, path)| {
        let metadata = std::fs::metadata(path).unwrap();
        (name, (metadata.dev(), metadata.ino()))
    })
    .collect()
}

fn assert_allocator_persistence(
    state: &std::path::Path,
    output: &std::path::Path,
    paid: &str,
    bound_inodes: &BTreeMap<&str, (u64, u64)>,
    next_generation: u64,
) {
    for path in [
        state.join("allocator-ledger.json"),
        output.join("allocator-binding.json"),
    ] {
        let metadata = std::fs::metadata(&path).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o7777, 0o600);
        assert_eq!(metadata.nlink(), 1);
    }
    let ledger: JsonValue =
        serde_json::from_slice(&std::fs::read(state.join("allocator-ledger.json")).unwrap())
            .unwrap();
    let binding: JsonValue =
        serde_json::from_slice(&std::fs::read(output.join("allocator-binding.json")).unwrap())
            .unwrap();
    for document in [&ledger, &binding] {
        assert_eq!(document["version"].as_u64(), Some(1));
        assert_eq!(document["personality_agent_id"].as_str(), Some(paid));
        for (name, (device, inode)) in bound_inodes {
            assert_eq!(
                document["directories"][*name]["device"].as_u64(),
                Some(*device)
            );
            assert_eq!(
                document["directories"][*name]["inode"].as_u64(),
                Some(*inode)
            );
        }
    }
    assert_eq!(ledger["state"]["status"].as_str(), Some("next"));
    assert_eq!(
        ledger["state"]["generation"].as_u64(),
        Some(next_generation)
    );
}

fn assert_allocator_identities(
    output: &std::path::Path,
    paid: &str,
    role_gids: &[libc::gid_t; 3],
    generation: u64,
) -> BTreeMap<&'static str, BTreeMap<String, String>> {
    let mut identities = BTreeMap::new();
    for (index, role) in ["runtime", "executor", "broker"].into_iter().enumerate() {
        let directory = output.join(role);
        let directory_metadata = std::fs::metadata(&directory).unwrap();
        assert_eq!(directory_metadata.permissions().mode() & 0o7777, 0o550);
        assert_eq!(directory_metadata.gid(), role_gids[index]);
        let identity_path = directory.join("identity.env");
        let metadata = std::fs::metadata(&identity_path).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o7777, 0o440);
        assert_eq!(metadata.nlink(), 1);
        assert_eq!(metadata.gid(), role_gids[index]);
        let identity = std::fs::read_to_string(identity_path)
            .unwrap()
            .lines()
            .map(|line| {
                let (key, value) = line.split_once('=').unwrap();
                (key.to_owned(), value.to_owned())
            })
            .collect::<BTreeMap<_, _>>();
        let mut expected = BTreeMap::from([
            ("SUMI_PERSONALITY_AGENT_ID".to_owned(), paid.to_owned()),
            ("SUMI_RPC_GENERATION".to_owned(), generation.to_string()),
        ]);
        if role == "runtime" {
            expected.insert(
                "SUMI_PROCESS_GENERATION_LEASE_ID".to_owned(),
                identity["SUMI_PROCESS_GENERATION_LEASE_ID"].clone(),
            );
            expected.insert(
                "SUMI_GENERATION_RECOVERY_FENCE_ID".to_owned(),
                identity["SUMI_GENERATION_RECOVERY_FENCE_ID"].clone(),
            );
            expected.insert(
                "SUMI_EXECUTOR_CALL_AUTHORITY_PRIVATE_KEY".to_owned(),
                identity["SUMI_EXECUTOR_CALL_AUTHORITY_PRIVATE_KEY"].clone(),
            );
        } else if role == "executor" {
            expected.insert(
                "SUMI_EXECUTOR_CALL_AUTHORITY_PUBLIC_KEY".to_owned(),
                identity["SUMI_EXECUTOR_CALL_AUTHORITY_PUBLIC_KEY"].clone(),
            );
        }
        expected.insert(
            "SUMI_RPC_NONCE".to_owned(),
            identity["SUMI_RPC_NONCE"].clone(),
        );
        assert_eq!(identity, expected, "unexpected {role} identity");
        identities.insert(role, identity);
    }
    let nonce = identities["runtime"]["SUMI_RPC_NONCE"].clone();
    assert_eq!(identities["executor"]["SUMI_RPC_NONCE"], nonce);
    assert_eq!(identities["broker"]["SUMI_RPC_NONCE"], nonce);
    let private_key =
        decode_lower_hex_32(&identities["runtime"]["SUMI_EXECUTOR_CALL_AUTHORITY_PRIVATE_KEY"]);
    let expected_public_key = SigningKey::from_bytes(&private_key)
        .verifying_key()
        .to_bytes();
    let executor_public_key =
        decode_lower_hex_32(&identities["executor"]["SUMI_EXECUTOR_CALL_AUTHORITY_PUBLIC_KEY"]);
    assert_eq!(
        executor_public_key, expected_public_key,
        "runtime private seed and Executor public identity are not one pair"
    );
    assert!(!identities["runtime"].contains_key("SUMI_EXECUTOR_CALL_AUTHORITY_PUBLIC_KEY"));
    assert!(!identities["executor"].contains_key("SUMI_EXECUTOR_CALL_AUTHORITY_PRIVATE_KEY"));
    assert!(!identities["broker"].contains_key("SUMI_EXECUTOR_CALL_AUTHORITY_PRIVATE_KEY"));
    assert!(!identities["broker"].contains_key("SUMI_EXECUTOR_CALL_AUTHORITY_PUBLIC_KEY"));
    identities
}

fn decode_lower_hex_32(encoded: &str) -> [u8; 32] {
    assert_eq!(encoded.len(), 64);
    let mut decoded = [0_u8; 32];
    for (index, pair) in encoded.as_bytes().chunks_exact(2).enumerate() {
        let nibble = |value: u8| match value {
            b'0'..=b'9' => value - b'0',
            b'a'..=b'f' => value - b'a' + 10,
            _ => panic!("identity key is not lowercase hex"),
        };
        decoded[index] = (nibble(pair[0]) << 4) | nibble(pair[1]);
    }
    decoded
}

fn assert_no_allocator_temps_or_interrupted_handoff(
    state: &std::path::Path,
    output: &std::path::Path,
) {
    for (directory, prefix) in [
        (state, ".allocator-ledger.json.tmp-"),
        (output, ".allocator-binding.json.tmp-"),
    ] {
        assert!(std::fs::read_dir(directory).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(prefix)
        }));
    }
    for role in ["runtime", "executor", "broker"] {
        let directory = output.join(role);
        assert_eq!(
            std::fs::metadata(&directory).unwrap().permissions().mode() & 0o7777,
            0o550
        );
        assert!(std::fs::read_dir(directory).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".identity.env.tmp-")
        }));
    }
}

/// Everything the supervisor produced, not just its stderr. A tracked Compose
/// launch that loses a race prints one line of unrelated warning on stderr, so
/// an assertion that shows only stderr reports a failure with no cause.
fn supervisor_failure(output: &Output) -> String {
    format!(
        "status={:?}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// Every `/run/sumi` in the rewritten supervisor must be the tail of a path
/// rooted in this fixture. Unlike counting occurrences of the substitution's own
/// output, this fails on a source that names the host root in a form the
/// single-pass rewrite did not cover.
fn host_root_references_are_all_private(source: &str, fixture_root: &std::path::Path) -> bool {
    let private_prefix = format!("{}/run/sumi", fixture_root.display());
    let mut cursor = 0;
    while let Some(offset) = source[cursor..].find("/run/sumi") {
        let end = cursor + offset + "/run/sumi".len();
        if !source[..end].ends_with(&private_prefix) {
            return false;
        }
        cursor = end;
    }
    true
}

/// Remove the root-owned private trust anchors the fixture provisioned inside a
/// container.  The test uid cannot unlink them, so a leftover tree would stay in
/// the temporary directory forever.  Best effort: callers already report the
/// failure that made this necessary.
fn purge_private_run_root(fixture_root: &std::path::Path) {
    if !fixture_root.join("run/sumi").exists() {
        return;
    }
    let _ = try_bounded_docker_output(
        deploy_dir().parent().unwrap().parent().unwrap(),
        30,
        &[
            "run".into(),
            "--rm".into(),
            "--network".into(),
            "none".into(),
            "-v".into(),
            format!("{}:/host-run", fixture_root.join("run").display()),
            "debian:bookworm-slim".into(),
            "bash".into(),
            "-c".into(),
            "set -eu\nrm -rf /host-run/sumi\n".into(),
        ],
    );
}

/// Copy a published deployment tree into a fixture-owned directory.  The
/// supervisor resolves its Compose files relative to its own location, so the
/// private copy has to carry every nested artifact, not just the top level.
fn copy_tree(source: &std::path::Path, target: &std::path::Path) {
    std::fs::create_dir_all(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let child_target = target.join(entry.file_name());
        let file_type = entry.file_type().unwrap();
        assert!(
            !file_type.is_symlink(),
            "published deployment artifact must not be a symlink: {}",
            entry.path().display()
        );
        if file_type.is_dir() {
            copy_tree(&entry.path(), &child_target);
        } else {
            std::fs::copy(entry.path(), &child_target).unwrap();
        }
    }
}

fn make_tree_removable(path: &std::path::Path) {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return;
    };
    if metadata.file_type().is_symlink() {
        return;
    }
    if metadata.is_dir() {
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700));
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                make_tree_removable(&entry.path());
            }
        }
    } else {
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
}

#[test]
fn data_socket_network_and_credentials_follow_the_role_graph() {
    let compose = compose();
    let runtime = service(&compose, "runtime");
    let executor = service(&compose, "executor");
    let broker = service(&compose, "broker");

    assert_eq!(
        volume_sources(runtime),
        string_set(&[
            "executor-ipc",
            "runtime-identity",
            "state",
            "${SUMI_LOCAL_CONTROL_HOST_DIR:?SUMI_LOCAL_CONTROL_HOST_DIR is required}",
            "${SUMI_LOCAL_CONTROL_HOST_DIR:?SUMI_LOCAL_CONTROL_HOST_DIR is required}/control.sock",
            "${SUMI_RUNTIME_SECRET_HOST_DIR:?SUMI_RUNTIME_SECRET_HOST_DIR is required}/sumi_local_control_bearer",
            "${SUMI_RUNTIME_SECRET_HOST_DIR:?SUMI_RUNTIME_SECRET_HOST_DIR is required}/sumi_agent_wrapping_key",
            "${SUMI_RUNTIME_SECRET_HOST_DIR:?SUMI_RUNTIME_SECRET_HOST_DIR is required}/sumi_approval_secret_digest_key",
            "${SUMI_RUNTIME_SECRET_HOST_DIR:?SUMI_RUNTIME_SECRET_HOST_DIR is required}/sumi_provider_api_key",
            "${SUMI_RUNTIME_SECRET_HOST_DIR:?SUMI_RUNTIME_SECRET_HOST_DIR is required}/sumi_execution_reviewer_api_key",
            "${SUMI_RUNTIME_SECRET_HOST_DIR:?SUMI_RUNTIME_SECRET_HOST_DIR is required}/sumi_escalation_reviewer_api_key",
        ])
    );
    assert_eq!(
        volume_sources(executor),
        string_set(&["executor-identity", "executor-ipc", "workspace",])
    );
    assert_eq!(
        volume_sources(broker),
        string_set(&["artifacts", "broker-identity", "broker-ipc"])
    );
    assert_has_mount(runtime, "executor-ipc:/run/sumi/executor:ro");
    assert_has_mount(executor, "executor-ipc:/run/sumi/executor");
    assert_has_mount(executor, "workspace:/workspace:ro");
    assert_has_mount(broker, "broker-ipc:/run/sumi/broker");
    assert!(!volume_sources(runtime).contains("workspace"));
    assert!(!volume_sources(runtime).contains("broker-ipc"));
    assert!(!volume_sources(runtime).contains("artifacts"));
    assert!(!volume_sources(executor).contains("artifacts"));
    assert!(!volume_sources(executor).contains("state"));
    assert!(!volume_sources(executor).contains("broker-ipc"));
    assert!(!volume_sources(broker).contains("workspace"));
    assert!(!volume_sources(broker).contains("state"));

    let local_control_mount = runtime["volumes"]
        .as_sequence()
        .unwrap()
        .iter()
        .find(|mount| mount["target"].as_str() == Some("/run/sumi/local-control/control.sock"))
        .expect("runtime local-control bind mount");
    assert_eq!(
        local_control_mount["source"].as_str(),
        Some(
            "${SUMI_LOCAL_CONTROL_HOST_DIR:?SUMI_LOCAL_CONTROL_HOST_DIR is required}/control.sock"
        )
    );
    assert_eq!(local_control_mount["read_only"].as_bool(), Some(true));
    assert_eq!(
        local_control_mount["bind"]["create_host_path"].as_bool(),
        Some(false)
    );

    assert_eq!(executor["network_mode"].as_str(), Some("none"));
    assert_eq!(broker["network_mode"].as_str(), Some("none"));
    assert!(runtime.get("network_mode").is_none());
    assert_eq!(
        runtime["networks"]
            .as_mapping()
            .unwrap()
            .keys()
            .filter_map(|key| key.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["default", "control-plane"]),
        "runtime must keep a private netns with provider egress plus the stable API bridge"
    );

    let runtime_env = environment_keys(runtime);
    let executor_env = environment_keys(executor);
    let broker_env = environment_keys(broker);
    assert!(runtime_env.contains("SUMI_LOCAL_CONTROL_UNIX_SOCKET"));
    assert!(runtime_env.contains("SUMI_LOCAL_CONTROL_SERVER_UID"));
    assert!(runtime_env.contains("SUMI_MODEL_ID"));
    assert!(
        runtime["environment"]["SUMI_MODEL_ID"].is_null(),
        "optional model ID must be a host pass-through without a default"
    );
    assert_eq!(
        runtime["environment"]["SUMI_LOCAL_CONTROL_SOCKET_GID"].as_str(),
        Some("${SUMI_LOCAL_CONTROL_SOCKET_GID:?SUMI_LOCAL_CONTROL_SOCKET_GID is required}")
    );
    assert!(
        runtime["group_add"]
            .as_sequence()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .any(|group| {
                group
                    == "${SUMI_LOCAL_CONTROL_SOCKET_GID:?SUMI_LOCAL_CONTROL_SOCKET_GID is required}"
            }),
        "runtime socket group must use the same required supervisor-validated gid"
    );
    assert!(!runtime_env.contains("SUMI_LOCAL_CONTROL_URL"));
    for sensitive in [
        "SUMI_LOCAL_CONTROL_BEARER",
        "SUMI_AGENT_WRAPPING_KEY",
        "SUMI_APPROVAL_SECRET_DIGEST_KEY",
        "SUMI_PROVIDER_API_KEY",
        "SUMI_EXECUTION_REVIEWER_API_KEY",
        "SUMI_ESCALATION_REVIEWER_API_KEY",
    ] {
        assert!(
            !runtime_env.contains(sensitive),
            "{sensitive} must not survive in Docker Config.Env"
        );
        assert!(!executor_env.contains(sensitive));
        assert!(!broker_env.contains(sensitive));
    }
    let expected_secrets = [
        "sumi_local_control_bearer",
        "sumi_agent_wrapping_key",
        "sumi_approval_secret_digest_key",
        "sumi_provider_api_key",
        "sumi_execution_reviewer_api_key",
        "sumi_escalation_reviewer_api_key",
    ];
    assert!(
        compose.get("secrets").is_none(),
        "environment-backed Compose secrets copy into the container after create and cannot be used with read_only"
    );
    assert!(runtime.get("secrets").is_none());
    for source in expected_secrets {
        let target = format!("/run/secrets/{source}");
        let expected_source = format!(
            "${{SUMI_RUNTIME_SECRET_HOST_DIR:?SUMI_RUNTIME_SECRET_HOST_DIR is required}}/{source}"
        );
        let mount = runtime["volumes"]
            .as_sequence()
            .unwrap()
            .iter()
            .find(|mount| mount["target"].as_str() == Some(target.as_str()))
            .unwrap_or_else(|| panic!("missing runtime secret {source}"));
        assert_eq!(mount["source"].as_str(), Some(expected_source.as_str()));
        assert_eq!(mount["read_only"].as_bool(), Some(true));
        assert_eq!(mount["bind"]["create_host_path"].as_bool(), Some(false));
    }
    let supervisor = read_deploy("supervisor");
    assert!(supervisor.contains("/run/sumi/runtime-secrets"));
    assert!(supervisor.contains("materialize_runtime_secrets"));
    assert!(supervisor.contains("remove_runtime_secret_generation"));
    assert!(supervisor.contains("remove_runtime_secret_tree"));
    assert!(supervisor.contains(r#"${secret_owner}:${secret_group}:400:1"#));
    let entrypoint = read_deploy("container-entrypoint");
    assert!(
        entrypoint.matches("SUMI_LOCAL_CONTROL_SERVER_UID").count() >= 2,
        "runtime env scrubber dropped the pinned local-control server uid"
    );
    assert!(
        entrypoint.matches("SUMI_LOCAL_CONTROL_SOCKET_GID").count() >= 2,
        "runtime env scrubber dropped the supervisor-validated local-control socket gid"
    );
    for source in expected_secrets {
        assert!(
            entrypoint.contains(&format!("/run/secrets/{source}")),
            "entrypoint does not use the fixed {source} path"
        );
    }
    assert!(entrypoint.contains("runtime secret must not contain newline or carriage return"));
    assert!(entrypoint.contains("runtime secret byte length changed during parsing"));
    assert!(entrypoint.contains("10001:10001:400:1"));
    assert!(entrypoint.contains("if [[ -v SUMI_MODEL_ID ]]"));
    assert!(entrypoint.contains(r#"runtime_environment+=("SUMI_MODEL_ID=${SUMI_MODEL_ID}")"#));
    assert!(!entrypoint.contains("SUMI_MODEL_ID:-"));
}

#[test]
fn exact_runtime_secret_loader_rejects_invalid_files_without_exposing_values() {
    struct LoaderCase {
        label: &'static str,
        path: PathBuf,
        expected_diagnostic: &'static str,
        secret_fragments: Vec<&'static [u8]>,
        run_as_runtime: bool,
    }

    fn loader_harness() -> String {
        let entrypoint = read_deploy("container-entrypoint");
        let start = entrypoint
            .find("load_runtime_secret() {")
            .expect("exact runtime secret loader");
        let tail = &entrypoint[start..];
        let end = tail
            .find("\n}\n\n# Docker services")
            .expect("runtime secret loader terminator")
            + "\n}\n".len();
        format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
fail() {{
  printf '[sumi-entrypoint] %s\n' "$*" >&2
  exit 64
}}
{}
load_runtime_secret TEST_RUNTIME_SECRET "$1"
printf 'RUNTIME_EXECUTED\n' >&2
exit 99
"#,
            &tail[..end]
        )
    }

    fn chown_runtime(path: &std::path::Path) -> bool {
        use std::{ffi::CString, os::unix::ffi::OsStrExt};

        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        unsafe { libc::chown(path.as_ptr(), 10001, 10001) == 0 }
    }

    fn write_exact_runtime_file(path: &std::path::Path, bytes: &[u8], mode: u32, euid: u32) {
        std::fs::write(path, bytes).unwrap();
        if euid == 0 {
            assert!(chown_runtime(path), "cannot chown exact runtime secret");
        }
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    fn run_loader(harness: &std::path::Path, case: &LoaderCase) -> Output {
        let mut command = Command::new("/bin/bash");
        command
            .arg(harness)
            .arg(&case.path)
            .env_clear()
            .env("PATH", "/usr/bin:/bin");
        if case.run_as_runtime {
            command.gid(10001).uid(10001);
        }
        command.output().unwrap()
    }

    let root = std::env::temp_dir().join(format!("secret-loader-{}", Uuid::now_v7().simple()));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
    let harness = root.join("loader-harness");
    std::fs::write(&harness, loader_harness()).unwrap();
    std::fs::set_permissions(&harness, std::fs::Permissions::from_mode(0o644)).unwrap();

    let mut cases = Vec::new();

    let symlink_target = root.join("symlink-target");
    std::fs::write(&symlink_target, b"symlink-secret-must-not-escape").unwrap();
    let symlink = root.join("symlink-secret");
    std::os::unix::fs::symlink(&symlink_target, &symlink).unwrap();
    cases.push(LoaderCase {
        label: "symlink",
        path: symlink,
        expected_diagnostic: "runtime secret must be a regular non-symlink",
        secret_fragments: vec![b"symlink-secret-must-not-escape"],
        run_as_runtime: false,
    });

    let nonregular = root.join("nonregular-secret");
    std::fs::create_dir(&nonregular).unwrap();
    cases.push(LoaderCase {
        label: "nonregular",
        path: nonregular,
        expected_diagnostic: "runtime secret must be a regular non-symlink",
        secret_fragments: vec![],
        run_as_runtime: false,
    });

    let euid = unsafe { libc::geteuid() };
    if euid != 10001 {
        let wrong_owner = root.join("wrong-owner-secret");
        std::fs::write(&wrong_owner, b"owner-secret-must-not-escape").unwrap();
        std::fs::set_permissions(&wrong_owner, std::fs::Permissions::from_mode(0o400)).unwrap();
        cases.push(LoaderCase {
            label: "owner",
            path: wrong_owner,
            expected_diagnostic: "runtime secret has invalid owner, mode, or link count",
            secret_fragments: vec![b"owner-secret-must-not-escape"],
            run_as_runtime: false,
        });
    } else {
        unavailable_host(
            "owner-mismatch secret case requires chown authority when tests already run as uid 10001",
        );
    }

    let exact_runtime_owner_available = if euid == 10001 {
        true
    } else if euid == 0 {
        let probe = root.join("chown-probe");
        std::fs::write(&probe, b"probe").unwrap();
        let available = chown_runtime(&probe);
        let _ = std::fs::remove_file(probe);
        available
    } else {
        false
    };
    if exact_runtime_owner_available {
        let run_as_runtime = euid == 0;

        let wrong_mode = root.join("wrong-mode-secret");
        write_exact_runtime_file(&wrong_mode, b"mode-secret-must-not-escape", 0o600, euid);
        cases.push(LoaderCase {
            label: "mode",
            path: wrong_mode,
            expected_diagnostic: "runtime secret has invalid owner, mode, or link count",
            secret_fragments: vec![b"mode-secret-must-not-escape"],
            run_as_runtime,
        });

        let linked = root.join("linked-secret");
        write_exact_runtime_file(&linked, b"linked-secret-must-not-escape", 0o400, euid);
        std::fs::hard_link(&linked, root.join("linked-secret-alias")).unwrap();
        cases.push(LoaderCase {
            label: "link-count",
            path: linked,
            expected_diagnostic: "runtime secret has invalid owner, mode, or link count",
            secret_fragments: vec![b"linked-secret-must-not-escape"],
            run_as_runtime,
        });

        let empty = root.join("empty-secret");
        write_exact_runtime_file(&empty, b"", 0o400, euid);
        cases.push(LoaderCase {
            label: "empty",
            path: empty,
            expected_diagnostic: "runtime secret must not be empty",
            secret_fragments: vec![],
            run_as_runtime,
        });

        let nul = root.join("nul-secret");
        write_exact_runtime_file(&nul, b"nul-secret-prefix\0nul-secret-suffix", 0o400, euid);
        cases.push(LoaderCase {
            label: "NUL",
            path: nul,
            expected_diagnostic: "runtime secret byte length changed during parsing",
            secret_fragments: vec![b"nul-secret-prefix", b"nul-secret-suffix"],
            run_as_runtime,
        });

        let carriage_return = root.join("cr-secret");
        write_exact_runtime_file(
            &carriage_return,
            b"cr-secret-prefix\rcr-secret-suffix",
            0o400,
            euid,
        );
        cases.push(LoaderCase {
            label: "CR",
            path: carriage_return,
            expected_diagnostic: "runtime secret must not contain newline or carriage return",
            secret_fragments: vec![b"cr-secret-prefix", b"cr-secret-suffix"],
            run_as_runtime,
        });

        let line_feed = root.join("lf-secret");
        write_exact_runtime_file(
            &line_feed,
            b"lf-secret-prefix\nlf-secret-suffix",
            0o400,
            euid,
        );
        cases.push(LoaderCase {
            label: "LF",
            path: line_feed,
            expected_diagnostic: "runtime secret must not contain newline or carriage return",
            secret_fragments: vec![b"lf-secret-prefix", b"lf-secret-suffix"],
            run_as_runtime,
        });
    } else {
        unavailable_host(
            "mode/link-count/empty/NUL/CR/LF cases require uid 10001 or root chown/setuid authority; symlink/nonregular/owner cases still ran",
        );
    }

    let results = cases
        .iter()
        .map(|case| (case, run_loader(&harness, case)))
        .collect::<Vec<_>>();
    let cleanup = std::fs::remove_dir_all(&root);

    for (case, output) in results {
        assert_eq!(
            output.status.code(),
            Some(64),
            "{} secret reached the runtime continuation: {}",
            case.label,
            String::from_utf8_lossy(&output.stderr)
        );
        let combined = [output.stdout, output.stderr].concat();
        assert!(
            !combined
                .windows(b"RUNTIME_EXECUTED".len())
                .any(|window| window == b"RUNTIME_EXECUTED"),
            "{} secret executed the runtime continuation",
            case.label
        );
        assert!(
            String::from_utf8_lossy(&combined).contains(case.expected_diagnostic),
            "{} secret failed for the wrong reason: {}",
            case.label,
            String::from_utf8_lossy(&combined)
        );
        for fragment in &case.secret_fragments {
            assert!(
                !combined
                    .windows(fragment.len())
                    .any(|window| window == *fragment),
                "{} secret value escaped in diagnostics",
                case.label
            );
        }
    }
    assert!(cleanup.is_ok(), "secret-loader fixture survived cleanup");
}

#[test]
fn identity_loader_enforces_role_minimal_authority_keys_and_exact_hex_without_echoing_them() {
    fn identity_harness() -> String {
        let entrypoint = read_deploy("container-entrypoint");
        let start = entrypoint.find("fail() {").expect("entrypoint helpers");
        let tail = &entrypoint[start..];
        let end = tail
            .find("\nverify_identity_output() {")
            .expect("identity loader terminator");
        format!(
            "#!/usr/bin/env bash\nset -euo pipefail\n{}\nload_identity \"$1\" \"$2\"\nprintf 'IDENTITY_LOADED\\n'\n",
            &tail[..end]
        )
    }

    fn common_identity() -> String {
        format!(
            "SUMI_PERSONALITY_AGENT_ID={PAID_A}\nSUMI_RPC_GENERATION=7\nSUMI_RPC_NONCE=identity-loader-nonce\n"
        )
    }

    fn run(harness: &std::path::Path, role: &str, identity: &std::path::Path) -> Output {
        Command::new("/bin/bash")
            .arg(harness)
            .arg(role)
            .arg(identity)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .output()
            .unwrap()
    }

    let root = std::env::temp_dir().join(format!("identity-loader-{}", Uuid::now_v7().simple()));
    std::fs::create_dir_all(&root).unwrap();
    let harness = root.join("harness");
    std::fs::write(&harness, identity_harness()).unwrap();

    let private_key = "07".repeat(32);
    let public_key = SigningKey::from_bytes(&[7; 32])
        .verifying_key()
        .to_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let valid = [
        (
            "runtime",
            format!(
                "{}SUMI_PROCESS_GENERATION_LEASE_ID=lease\nSUMI_GENERATION_RECOVERY_FENCE_ID=fence\nSUMI_EXECUTOR_CALL_AUTHORITY_PRIVATE_KEY={private_key}\n",
                common_identity()
            ),
        ),
        (
            "executor",
            format!(
                "{}SUMI_EXECUTOR_CALL_AUTHORITY_PUBLIC_KEY={public_key}\n",
                common_identity()
            ),
        ),
        ("broker", common_identity()),
    ];
    for (index, (role, identity)) in valid.iter().enumerate() {
        let path = root.join(format!("valid-{index}.env"));
        std::fs::write(&path, identity).unwrap();
        let output = run(&harness, role, &path);
        assert!(
            output.status.success(),
            "valid {role} identity failed: {}",
            supervisor_failure(&output)
        );
        assert_eq!(output.stdout, b"IDENTITY_LOADED\n");
    }

    let private_sentinel = "de".repeat(32);
    let public_sentinel = "ad".repeat(32);
    let invalid = [
        (
            "runtime-public-cross-role",
            "runtime",
            format!(
                "{}SUMI_PROCESS_GENERATION_LEASE_ID=lease\nSUMI_GENERATION_RECOVERY_FENCE_ID=fence\nSUMI_EXECUTOR_CALL_AUTHORITY_PUBLIC_KEY={public_sentinel}\n",
                common_identity()
            ),
            public_sentinel.clone(),
        ),
        (
            "executor-private-cross-role",
            "executor",
            format!(
                "{}SUMI_EXECUTOR_CALL_AUTHORITY_PRIVATE_KEY={private_sentinel}\n",
                common_identity()
            ),
            private_sentinel.clone(),
        ),
        (
            "broker-private-cross-role",
            "broker",
            format!(
                "{}SUMI_EXECUTOR_CALL_AUTHORITY_PRIVATE_KEY={private_sentinel}\n",
                common_identity()
            ),
            private_sentinel.clone(),
        ),
        (
            "runtime-short-private",
            "runtime",
            format!(
                "{}SUMI_PROCESS_GENERATION_LEASE_ID=lease\nSUMI_GENERATION_RECOVERY_FENCE_ID=fence\nSUMI_EXECUTOR_CALL_AUTHORITY_PRIVATE_KEY={}\n",
                common_identity(),
                "a".repeat(63)
            ),
            "a".repeat(63),
        ),
        (
            "runtime-uppercase-private",
            "runtime",
            format!(
                "{}SUMI_PROCESS_GENERATION_LEASE_ID=lease\nSUMI_GENERATION_RECOVERY_FENCE_ID=fence\nSUMI_EXECUTOR_CALL_AUTHORITY_PRIVATE_KEY={}\n",
                common_identity(),
                "A".repeat(64)
            ),
            "A".repeat(64),
        ),
        (
            "executor-nonhex-public",
            "executor",
            format!(
                "{}SUMI_EXECUTOR_CALL_AUTHORITY_PUBLIC_KEY={}\n",
                common_identity(),
                "g".repeat(64)
            ),
            "g".repeat(64),
        ),
    ];
    for (index, (label, role, identity, secret)) in invalid.iter().enumerate() {
        let path = root.join(format!("invalid-{index}.env"));
        std::fs::write(&path, identity).unwrap();
        let output = run(&harness, role, &path);
        assert_eq!(
            output.status.code(),
            Some(64),
            "{label} unexpectedly loaded"
        );
        let combined = [output.stdout, output.stderr].concat();
        assert!(
            !combined
                .windows(secret.len())
                .any(|window| window == secret.as_bytes())
        );
        assert!(!String::from_utf8_lossy(&combined).contains("IDENTITY_LOADED"));
    }

    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn executor_deployment_is_broker_blind_and_read_only() {
    let compose = compose();
    let executor = service(&compose, "executor");
    let defaults = &compose["x-long-lived-hardening"];

    assert_eq!(executor["user"].as_str(), Some("10002:10002"));
    assert_eq!(executor["network_mode"].as_str(), Some("none"));
    assert_has_mount(executor, "workspace:/workspace:ro");
    assert_has_mount(executor, "executor-identity:/run/sumi/identity:ro");
    assert_has_mount(executor, "executor-ipc:/run/sumi/executor");
    assert!(!volume_sources(executor).contains("broker-ipc"));
    assert!(!volume_sources(executor).contains("artifacts"));

    let depends_on = executor["depends_on"].as_mapping().unwrap();
    assert!(depends_on.contains_key("allocator"));
    assert!(depends_on.contains_key("prepare"));
    assert!(
        !depends_on.contains_key("broker"),
        "the read-only executor must not receive a broker startup dependency"
    );
    assert!(executor.get("group_add").is_none());
    assert!(executor.get("cap_add").is_none());
    assert!(executor.get("security_opt").is_none());
    assert!(executor.get("privileged").is_none());
    assert_eq!(
        defaults["cap_drop"],
        Value::Sequence(vec![Value::String("ALL".to_owned())])
    );
    assert_eq!(
        defaults["security_opt"],
        Value::Sequence(vec![
            Value::String("no-new-privileges:true".to_owned()),
            Value::String("seccomp:./seccomp/sidecar.json".to_owned()),
            Value::String("apparmor:${SUMI_DOCKER_APPARMOR_PROFILE:-docker-default}".to_owned(),),
        ])
    );

    let entrypoint = read_deploy("container-entrypoint");
    let executor_branch = entrypoint
        .split("  executor)")
        .nth(1)
        .and_then(|branch| branch.split("\n\n  broker)").next())
        .expect("executor entrypoint branch");
    assert!(executor_branch.contains("load_identity executor \"${IDENTITY_FILE}\""));
    for retained in [
        "SUMI_PERSONALITY_AGENT_ID=\"${SUMI_PERSONALITY_AGENT_ID}\"",
        "SUMI_RPC_GENERATION=\"${SUMI_RPC_GENERATION}\"",
        "SUMI_RPC_NONCE=\"${SUMI_RPC_NONCE}\"",
        "SUMI_EXECUTOR_CALL_AUTHORITY_PUBLIC_KEY=\"${SUMI_EXECUTOR_CALL_AUTHORITY_PUBLIC_KEY}\"",
        "SUMI_WORKSPACE=/workspace",
        "SUMI_EXECUTOR_SOCKET=/run/sumi/executor/executor.sock",
        "/usr/local/bin/sumi-agent --tool-executor-socket",
    ] {
        assert!(
            executor_branch.contains(retained),
            "executor lost {retained}"
        );
    }
    assert!(!executor_branch.contains("SUMI_ARTIFACT_BROKER_SOCKET"));
    assert!(!executor_branch.contains("--artifact-broker"));
    assert!(!executor_branch.contains("--tool-executor\n"));
}

#[test]
fn every_long_lived_role_is_non_root_read_only_and_restricted() {
    let compose = compose();
    let defaults = &compose["x-long-lived-hardening"];
    let expected_users = BTreeMap::from([
        ("runtime", "10001:10001"),
        ("executor", "10002:10002"),
        ("broker", "10003:10003"),
    ]);
    for (name, user) in expected_users {
        let role = service(&compose, name);
        assert_eq!(role["user"].as_str(), Some(user));
        assert_eq!(defaults["read_only"].as_bool(), Some(true));
        assert_eq!(defaults["stdin_open"].as_bool(), Some(false));
        assert_eq!(defaults["init"].as_bool(), Some(true));
        assert_eq!(defaults["restart"].as_str(), Some("no"));
        assert_eq!(defaults["stop_grace_period"].as_str(), Some("30s"));
        assert_eq!(
            defaults["cap_drop"].as_sequence().unwrap()[0].as_str(),
            Some("ALL")
        );
        let security_source = role
            .get("security_opt")
            .unwrap_or(&defaults["security_opt"]);
        let security = security_source
            .as_sequence()
            .unwrap()
            .iter()
            .map(|item| item.as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(security.contains(&"no-new-privileges:true"));
        assert!(security.contains(&"seccomp:./seccomp/sidecar.json"));
        assert!(security.iter().any(|item| item.starts_with("apparmor:")));
    }

    let entrypoint = read_deploy("container-entrypoint");
    assert!(entrypoint.contains("exec env -i"));
    assert!(entrypoint.contains("close_unlisted_fds"));
    assert!(entrypoint.contains("exec {fd}>&-"));
    assert!(!entrypoint.contains("SUMI_ENFORCE_BROKER_SOCKET_NAMESPACE_ISOLATION"));

    let seccomp: serde_json::Value =
        serde_json::from_str(&read_deploy("seccomp/sidecar.json")).unwrap();
    assert!(seccomp.is_object());
    let syscalls = seccomp["syscalls"].as_array().unwrap();
    let ordinary = syscalls[0]["names"].as_array().unwrap();
    for syscall in ["openat2", "close_range", "prctl", "rt_sigtimedwait"] {
        assert!(ordinary.iter().any(|name| name.as_str() == Some(syscall)));
    }
    for dormant in ["mount", "umount2", "unshare"] {
        assert!(
            !syscalls.iter().any(|rule| {
                rule["names"]
                    .as_array()
                    .is_some_and(|names| names.iter().any(|name| name.as_str() == Some(dormant)))
            }),
            "dormant namespace-masking syscall remained allowed: {dormant}"
        );
    }
    let clone = syscalls
        .iter()
        .find(|rule| rule["names"][0].as_str() == Some("clone"))
        .unwrap();
    assert_eq!(clone["args"][0]["op"].as_str(), Some("SCMP_CMP_MASKED_EQ"));
    assert_eq!(clone["args"][0]["value"].as_u64(), Some(2_114_060_416));
    assert_eq!(clone["args"][0]["valueTwo"].as_u64(), Some(0));
    let clone3 = syscalls
        .iter()
        .find(|rule| rule["names"][0].as_str() == Some("clone3"))
        .unwrap();
    assert_eq!(clone3["action"].as_str(), Some("SCMP_ACT_ERRNO"));
    assert_eq!(clone3["errnoRet"].as_u64(), Some(38));
    assert_eq!(
        service(&compose, "executor")["security_opt"],
        Value::Null,
        "executor should inherit docker-default AppArmor until masking is executable"
    );
    assert!(!deploy_dir().join("apparmor/executor").exists());
}

#[test]
fn exact_image_executor_smoke_is_opt_in_and_owns_every_docker_artifact() {
    if std::env::var_os("SUMI_EXECUTOR_DOCKER_SMOKE").is_none() {
        eprintln!(
            "NOT_RUN: set SUMI_EXECUTOR_DOCKER_SMOKE=1 to build and exercise the exact \
             read-only executor image without providers"
        );
        return;
    }
    if !timeout_available() {
        unavailable_host("GNU timeout is required to bound exact executor smoke");
        return;
    }
    if !bounded_docker_output(
        deploy_dir().parent().unwrap().parent().unwrap(),
        10,
        &["info".into()],
    )
    .status
    .success()
    {
        unavailable_host("Docker daemon cannot run exact executor smoke");
        return;
    }
    let _guard = HOST_FIXTURE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let smoke = OwnedExecutorDockerSmoke::new();
    let body = catch_unwind(AssertUnwindSafe(|| {
        let paid = Uuid::now_v7().to_string();
        let nonce = format!("executor-smoke-{}", Uuid::now_v7());
        let call_authority_key = SigningKey::from_bytes(&[7; 32]);
        let call_authority_public_key = call_authority_key
            .verifying_key()
            .to_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();

        let build = smoke.docker(
            600,
            vec![
                "build".into(),
                "--tag".into(),
                smoke.image.clone(),
                "--file".into(),
                deploy_dir().join("Dockerfile").display().to_string(),
                ".".into(),
            ],
        );
        assert!(
            build.status.success(),
            "exact executor image build failed or exceeded its bound: {}",
            supervisor_failure(&build)
        );

        std::fs::write(
            smoke.root.join("identity/identity.env"),
            format!(
                "SUMI_PERSONALITY_AGENT_ID={paid}\nSUMI_RPC_GENERATION=1\nSUMI_RPC_NONCE={nonce}\nSUMI_EXECUTOR_CALL_AUTHORITY_PUBLIC_KEY={call_authority_public_key}\n"
            ),
        )
        .unwrap();
        std::fs::write(smoke.root.join("workspace/note.txt"), "read-file-content\n").unwrap();
        let fixture_path = format!("{}:/fixture", smoke.root.display());
        let setup = smoke.docker(
        30,
        vec![
            "run".into(),
            "--rm".into(),
            "--network".into(),
            "none".into(),
            "--entrypoint".into(),
            "/bin/sh".into(),
            "--user".into(),
            "0:0".into(),
            "-v".into(),
            fixture_path.clone(),
            smoke.image.clone(),
            "-ec".into(),
            "chown 10002:10002 /fixture/workspace /fixture/workspace/note.txt /fixture/identity /fixture/identity/identity.env; chmod 0700 /fixture/workspace; chmod 0600 /fixture/workspace/note.txt; chmod 0550 /fixture/identity; chmod 0440 /fixture/identity/identity.env; chown 10002:10020 /fixture/executor; chmod 2710 /fixture/executor".into(),
        ],
    );
        assert!(
            setup.status.success(),
            "owned executor fixture setup failed: {}",
            supervisor_failure(&setup)
        );

        let seccomp = format!(
            "seccomp={}",
            deploy_dir().join("seccomp/sidecar.json").display()
        );
        let start = smoke.docker(
            30,
            vec![
                "run".into(),
                "--detach".into(),
                "--name".into(),
                smoke.container.clone(),
                "--init".into(),
                "--network".into(),
                "none".into(),
                "--read-only".into(),
                "--cap-drop".into(),
                "ALL".into(),
                "--security-opt".into(),
                "no-new-privileges:true".into(),
                "--security-opt".into(),
                seccomp,
                "--tmpfs".into(),
                "/tmp:rw,noexec,nosuid,nodev,size=32m".into(),
                "--user".into(),
                "10002:10002".into(),
                "-v".into(),
                format!("{}:/workspace:ro", smoke.root.join("workspace").display()),
                "-v".into(),
                format!(
                    "{}:/run/sumi/identity:ro",
                    smoke.root.join("identity").display()
                ),
                "-v".into(),
                format!(
                    "{}:/run/sumi/executor",
                    smoke.root.join("executor").display()
                ),
                smoke.image.clone(),
                "executor".into(),
            ],
        );
        assert!(
            start.status.success(),
            "exact executor container failed to start: {}",
            supervisor_failure(&start)
        );

        let socket = smoke.root.join("executor/executor.sock");
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut socket_ready = false;
        while Instant::now() < deadline {
            let ready = smoke.docker(
                10,
                vec![
                    "run".into(),
                    "--rm".into(),
                    "--network".into(),
                    "none".into(),
                    "--entrypoint".into(),
                    "/bin/sh".into(),
                    "--user".into(),
                    "0:0".into(),
                    "-v".into(),
                    fixture_path.clone(),
                    smoke.image.clone(),
                    "-ec".into(),
                    "test -S /fixture/executor/executor.sock".into(),
                ],
            );
            if ready.status.success() {
                socket_ready = true;
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        if !socket_ready {
            let logs = smoke.docker(10, vec!["logs".into(), smoke.container.clone()]);
            panic!(
                "executor socket did not become ready in 10s; stdout: {}; stderr: {}",
                String::from_utf8_lossy(&logs.stdout),
                String::from_utf8_lossy(&logs.stderr),
            );
        }

        // The service socket is intentionally runtime-group-only. This fixture is
        // UUID-owned, so changing just its socket directory to the test gid grants
        // the host test client traversal without weakening the deployed Compose
        // contract asserted above.
        let host_gid = unsafe { libc::getegid() };
        let client_access = smoke.docker(
        30,
        vec![
            "run".into(),
            "--rm".into(),
            "--network".into(),
            "none".into(),
            "--entrypoint".into(),
            "/bin/sh".into(),
            "--user".into(),
            "0:0".into(),
            "-v".into(),
            fixture_path.clone(),
            smoke.image.clone(),
            "-ec".into(),
            format!(
                "chgrp {host_gid} /fixture/executor /fixture/executor/executor.sock; chmod 0710 /fixture/executor; chmod 0660 /fixture/executor/executor.sock"
            ),
        ],
    );
        assert!(client_access.status.success());

        let inspect = smoke.docker(30, vec!["inspect".into(), smoke.container.clone()]);
        assert!(inspect.status.success());
        let inspect: JsonValue = serde_json::from_slice(&inspect.stdout).unwrap();
        let container = &inspect[0];
        assert_eq!(
            container["State"]["Running"].as_bool(),
            Some(true),
            "executor container stopped before the read-only mount probe: {container}"
        );
        let environment = container["Config"]["Env"].as_array().unwrap();
        assert!(environment.iter().all(|entry| {
            !entry
                .as_str()
                .is_some_and(|entry| entry.starts_with("SUMI_ARTIFACT_BROKER_SOCKET="))
        }));
        let mounts = container["Mounts"].as_array().unwrap();
        assert!(mounts.iter().any(|mount| {
            mount["Destination"].as_str() == Some("/workspace")
                && mount["RW"].as_bool() == Some(false)
        }));
        assert!(mounts.iter().all(|mount| {
            mount["Destination"].as_str() != Some("/run/sumi/broker")
                && mount["Destination"].as_str() != Some("/var/lib/sumi-artifacts")
        }));

        let health = exchange_executor_socket(
            &socket,
            serde_json::json!({
                "personality_agent_id": paid,
                "generation": 1,
                "nonce": nonce,
                "request_id": "health",
                "operation": {"type": "health", "service_role": "tool_executor"},
            }),
        );
        assert_eq!(health["personality_agent_id"].as_str(), Some(paid.as_str()));
        assert_eq!(health["generation"].as_u64(), Some(1));
        assert_eq!(health["nonce"].as_str(), Some(nonce.as_str()));
        assert_eq!(health["result"]["Ok"]["type"].as_str(), Some("healthy"));
        assert_eq!(
            health["result"]["Ok"]["service_role"].as_str(),
            Some("tool_executor")
        );
        let read_operation = serde_json::json!({
            "type": "read_file", "path": "note.txt", "offset": 0,
            "limit": 1024, "execution_id": "read"
        });
        let read_file = exchange_executor_socket(
            &socket,
            serde_json::json!({
                "personality_agent_id": paid,
                "generation": 1,
                "nonce": nonce,
                "request_id": "read",
                "call_authority": signed_executor_authority(
                    1,
                    &nonce,
                    "read",
                    &read_operation,
                    &call_authority_key,
                ),
                "operation": read_operation,
            }),
        );
        assert_eq!(
            read_file["result"]["Ok"]["result"]["content"].as_str(),
            Some("read-file-content\n"),
            "unexpected read_file response: {read_file}"
        );

        let before_write = smoke.docker(10, vec!["inspect".into(), smoke.container.clone()]);
        assert!(before_write.status.success());
        let before_write: JsonValue = serde_json::from_slice(&before_write.stdout).unwrap();
        assert_eq!(
            before_write[0]["State"]["Running"].as_bool(),
            Some(true),
            "executor container stopped before write denial probe: {}",
            before_write[0]
        );
        let write = smoke.docker(
            10,
            vec![
                "exec".into(),
                "--user".into(),
                "10002:10002".into(),
                "--env".into(),
                "LC_ALL=C".into(),
                smoke.container.clone(),
                "/usr/bin/touch".into(),
                "/workspace/must-not-write".into(),
            ],
        );
        let write_output = format!(
            "stdout: {}; stderr: {}",
            String::from_utf8_lossy(&write.stdout),
            String::from_utf8_lossy(&write.stderr)
        );
        assert!(
            !write.status.success(),
            "executor container wrote through its read-only workspace mount: {write_output}"
        );
        assert!(
            write_output.contains("Read-only file system"),
            "write denial was not the expected read-only-filesystem failure: {write_output}"
        );
        let after_write = smoke.docker(10, vec!["inspect".into(), smoke.container.clone()]);
        assert!(after_write.status.success());
        let after_write: JsonValue = serde_json::from_slice(&after_write.stdout).unwrap();
        assert_eq!(
            after_write[0]["State"]["Running"].as_bool(),
            Some(true),
            "executor container stopped during write denial probe: {}",
            after_write[0]
        );
        let host_write_check = smoke.docker(
            30,
            vec![
                "run".into(),
                "--rm".into(),
                "--network".into(),
                "none".into(),
                "--entrypoint".into(),
                "/bin/sh".into(),
                "--user".into(),
                "0:0".into(),
                "-v".into(),
                fixture_path,
                smoke.image.clone(),
                "-ec".into(),
                "test ! -e /fixture/workspace/must-not-write".into(),
            ],
        );
        assert!(host_write_check.status.success());

        let health_after = exchange_executor_socket(
            &socket,
            serde_json::json!({
                "personality_agent_id": paid,
                "generation": 1,
                "nonce": nonce,
                "request_id": "health-after-write-denial",
                "operation": {"type": "health", "service_role": "tool_executor"},
            }),
        );
        assert_eq!(
            health_after["result"]["Ok"]["type"].as_str(),
            Some("healthy"),
            "executor Health failed after write denial: {health_after}"
        );
        let read_after = exchange_executor_socket(
            &socket,
            serde_json::json!({
                "personality_agent_id": paid,
                "generation": 1,
                "nonce": nonce,
                "request_id": "read-after-write-denial",
                "operation": {
                    "type": "read_file", "path": "note.txt", "offset": 0,
                    "limit": 1024, "execution_id": "read-after-write-denial"
                },
            }),
        );
        assert_eq!(
            read_after["result"]["Ok"]["result"]["content"].as_str(),
            Some("read-file-content\n"),
            "executor read_file failed after write denial: {read_after}"
        );
    }));
    let cleanup = smoke.cleanup();
    finish_opt_in_docker_test(body, cleanup);
}

#[test]
fn deployment_contains_no_legacy_identity_or_file_readiness_contract() {
    let all = [
        "compose.yaml",
        "config.env",
        "container-entrypoint",
        "supervisor",
        "compose.lifecycle.yaml",
    ]
    .into_iter()
    .map(read_deploy)
    .collect::<String>();
    for legacy in [
        "SUMI_TENANT_ID",
        "SUMI_AGENT_ID",
        "SUMI_CONVERSATION_ID",
        "SUMI_AGENT_RUNTIME_STATE_DIR",
        "SUMI_LOCAL_CONTROL_URL",
        "runtime-state:",
        "RuntimeStatePublisher",
        "--check-unix-socket",
    ] {
        assert!(
            !all.contains(legacy),
            "legacy deployment contract survived: {legacy}"
        );
    }
    assert!(all.contains("SUMI_PERSONALITY_AGENT_ID"));
    assert!(all.contains("SUMI_LOCAL_CONTROL_UNIX_SOCKET"));

    let supervisor = read_deploy("supervisor");
    assert!(supervisor.contains("readonly SUPERVISOR_LOCK_ROOT=/run/sumi/supervisor-locks"));
    assert!(supervisor.contains("readonly LOCAL_CONTROL_HOST_ROOT=/run/sumi/local-control"));
    assert!(!supervisor.contains("SUMI_SUPERVISOR_LOCK_DIR"));
    assert!(!supervisor.contains("SUMI_LOCAL_CONTROL_HOST_ROOT"));

    let lifecycle = read_deploy("compose.lifecycle.yaml");
    assert!(!lifecycle.contains("${"));
    for role in ["allocator", "prepare", "runtime", "executor", "broker"] {
        assert!(lifecycle.contains(&format!("  {role}:")));
    }
}

#[test]
fn supervisor_rejects_noncanonical_paid_before_touching_docker() {
    for invalid in [
        "0198F0F4-9B72-7000-8000-000000000001",
        "0198f0f4-9b72-6000-8000-000000000001",
        "0198f0f4-9b72-7000-7000-000000000001",
        "agent-local",
    ] {
        let mut command = Command::new(deploy_dir().join("supervisor"));
        command.arg("project-name");
        launch_env(&mut command, invalid);
        let output = command.output().unwrap();
        assert!(!output.status.success(), "{invalid} was accepted");
        assert!(String::from_utf8_lossy(&output.stderr).contains("canonical lowercase UUIDv7"));
    }
}

#[test]
fn reconcile_keeps_active_epoch_when_prepare_one_shots_remain() {
    let Some(fixture) = HostTrustFixture::new() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("reconcile-orphan-{}", Uuid::now_v7().simple()));
    let bin = root.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let fake_docker = bin.join("docker");
    let script = r#"#!/bin/bash
printf '%s\n' "$*" >> "$SUMI_FAKE_DOCKER_LOG"
case "$*" in
  "compose version")
    exit 0
    ;;
  "ps --all --filter label=com.docker.compose.project="*)
    printf 'aaaaaaaaaaaa\truntime\trunning\n'
    printf 'bbbbbbbbbbbb\texecutor\trunning\n'
    printf 'cccccccccccc\tbroker\trunning\n'
    printf 'dddddddddddd\tallocator\texited\n'
    printf 'eeeeeeeeeeee\tprepare\texited\n'
    exit 0
    ;;
  *"compose.lifecycle.yaml down --remove-orphans"*)
    touch "$SUMI_FAKE_REAPED"
    exit 0
    ;;
  *"compose.lifecycle.yaml ps --all --quiet")
    exit 0
    ;;
  *"compose.prepare.yaml run --rm --no-deps --pull never --entrypoint /bin/bash allocator"*)
    printf 'SUMI_PERSONALITY_AGENT_ID=%s\nSUMI_RPC_GENERATION=7\nSUMI_RPC_NONCE=fixture-nonce\n' "$SUMI_PERSONALITY_AGENT_ID"
    exit 0
    ;;
  *)
    exit 91
    ;;
esac
"#;
    std::fs::write(&fake_docker, script).unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let log = root.join("docker.log");
    let reaped = root.join("reaped");
    let inherited_path = std::env::var("PATH").unwrap_or_default();

    let mut command = fixture.supervisor_command();
    command
        .arg("reconcile")
        .env("PATH", format!("{}:{inherited_path}", bin.display()))
        .env("SUMI_FAKE_DOCKER_LOG", &log)
        .env("SUMI_FAKE_REAPED", &reaped);
    launch_runtime_env(&mut command, &fixture);
    let output = command.output().unwrap();

    assert!(
        output.status.success(),
        "reconcile failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !reaped.exists(),
        "reconcile reaped a healthy active epoch because completed setup roles remained: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(r#""phase":"active","generation":7"#),
        "reconcile did not preserve the active epoch: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let calls = std::fs::read_to_string(&log).unwrap();
    assert!(
        !calls.contains("compose.lifecycle.yaml down --remove-orphans"),
        "active epoch unexpectedly entered destructive reconciliation: {calls}"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn inspect_epoch_returns_recovery_when_any_long_lived_role_is_missing() {
    let Some(fixture) = HostTrustFixture::new() else {
        return;
    };
    let root =
        std::env::temp_dir().join(format!("inspect-missing-role-{}", Uuid::now_v7().simple()));
    let bin = root.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let fake_docker = bin.join("docker");
    let script = r#"#!/bin/bash
case "$*" in
  "compose version")
    ;;
  "ps --all --filter label=com.docker.compose.project="*)
    printf 'aaaaaaaaaaaa\truntime\trunning\n'
    printf 'bbbbbbbbbbbb\texecutor\trunning\n'
    printf 'dddddddddddd\tallocator\texited\n'
    printf 'eeeeeeeeeeee\tprepare\texited\n'
    ;;
  *"compose.prepare.yaml run --rm --no-deps --pull never --entrypoint /bin/bash allocator"*)
    printf 'SUMI_PERSONALITY_AGENT_ID=%s\nSUMI_RPC_GENERATION=7\nSUMI_RPC_NONCE=fixture-nonce\n' "$SUMI_PERSONALITY_AGENT_ID"
    ;;
  *)
    exit 91
    ;;
esac
"#;
    std::fs::write(&fake_docker, script).unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let inherited_path = std::env::var("PATH").unwrap_or_default();

    let mut command = fixture.supervisor_command();
    command
        .arg("inspect-epoch")
        .env("PATH", format!("{}:{inherited_path}", bin.display()));
    launch_runtime_env(&mut command, &fixture);
    let output = command.output().unwrap();

    assert!(
        output.status.success(),
        "inspect failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(r#""phase":"recovery","generation":7"#),
        "a missing long-lived role was reported as active: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn replacement_lifecycle_joins_old_project_before_starting_new_generation() {
    let Some(fixture) = HostTrustFixture::new() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("sumi-deploy-{}", Uuid::now_v7()));
    let bin = root.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let fake_docker = bin.join("docker");
    std::fs::write(
        &fake_docker,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$SUMI_FAKE_DOCKER_LOG\"\ncase \"$*\" in *\"compose.prepare.yaml run --rm --no-deps --pull never --entrypoint /bin/bash allocator\"*) printf 'SUMI_PERSONALITY_AGENT_ID=%s\\nSUMI_RPC_GENERATION=0\\nSUMI_RPC_NONCE=fixture-nonce\\n' \"$SUMI_PERSONALITY_AGENT_ID\" ;; esac\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let log = root.join("docker.log");
    let inherited_path = std::env::var("PATH").unwrap_or_default();

    let mut command = fixture.supervisor_command();
    command
        .arg("prepare")
        .env("PATH", format!("{}:{inherited_path}", bin.display()))
        .env("SUMI_FAKE_DOCKER_LOG", &log);
    launch_runtime_env(&mut command, &fixture);
    let output = command.output().unwrap();
    assert!(output.status.success(), "{}", supervisor_failure(&output));

    let calls = std::fs::read_to_string(&log).unwrap();
    let down = calls
        .find(&format!(
            "compose --project-name {} --file {} down",
            fixture.project,
            fixture
                .deploy_dir()
                .join("compose.lifecycle.yaml")
                .display()
        ))
        .expect("old project must be stopped");
    let up = calls
        .find(&format!(
            "compose --project-name {} --file {} up",
            fixture.project,
            fixture.deploy_dir().join("compose.prepare.yaml").display()
        ))
        .expect("new project must be started");
    assert!(
        down < up,
        "replacement started before old generation joined"
    );
    assert!(!calls.contains("control-secret"));
    assert!(!calls.contains("provider-secret"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn competing_supervisor_invocation_fails_before_lifecycle_mutation() {
    let Some(fixture) = HostTrustFixture::new() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("sl-{}", Uuid::now_v7().simple()));
    let bin = root.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let fake_docker = bin.join("docker");
    let docker_log = root.join("docker.log");
    std::fs::write(
        &fake_docker,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$SUMI_FAKE_DOCKER_LOG\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();

    let inherited_path = std::env::var("PATH").unwrap_or_default();
    let mut command = fixture.supervisor_command();
    command
        .arg("prepare")
        .env("PATH", format!("{}:{inherited_path}", bin.display()))
        .env("SUMI_FAKE_DOCKER_LOG", &docker_log);
    launch_runtime_env(&mut command, &fixture);

    let lock = OpenOptions::new()
        .create(false)
        .truncate(false)
        .write(true)
        .open(&fixture.lock_path)
        .unwrap();
    assert_eq!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );

    let output = command.output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("another supervisor invocation"));
    assert!(
        !docker_log.exists(),
        "losing invocation touched Docker before acquiring the lock"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn lifecycle_actions_work_after_launch_configuration_is_removed() {
    let Some(fixture) = HostTrustFixture::new() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("lc-{}", Uuid::now_v7().simple()));
    let bin = root.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let fake_docker = bin.join("docker");
    std::fs::write(
        &fake_docker,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$SUMI_FAKE_DOCKER_LOG\"\ncase \"$*\" in *\"compose.prepare.yaml run --rm --no-deps --pull never --entrypoint /bin/bash allocator\"*) printf 'SUMI_PERSONALITY_AGENT_ID=%s\\nSUMI_RPC_GENERATION=0\\nSUMI_RPC_NONCE=fixture-nonce\\n' \"$SUMI_PERSONALITY_AGENT_ID\" ;; esac\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let log = root.join("docker.log");
    let inherited_path = std::env::var("PATH").unwrap_or_default();

    let mut up = fixture.supervisor_command();
    up.arg("prepare")
        .env("PATH", format!("{}:{inherited_path}", bin.display()))
        .env("SUMI_FAKE_DOCKER_LOG", &log);
    launch_runtime_env(&mut up, &fixture);
    let output = up.output().unwrap();
    assert!(output.status.success(), "{}", supervisor_failure(&output));

    for (action, expected) in [
        ("stop", "down --remove-orphans"),
        ("status", "ps"),
        ("logs", "logs"),
        ("down", "down --remove-orphans"),
    ] {
        std::fs::write(&log, b"").unwrap();
        let mut command = fixture.supervisor_command();
        command
            .env_clear()
            .arg(action)
            .env("PATH", format!("{}:{inherited_path}", bin.display()))
            .env("SUMI_CONFIG_FILE", "/dev/null")
            .env("SUMI_PERSONALITY_AGENT_ID", &fixture.paid)
            .env("SUMI_TEST_ALLOW_NONROOT_SECRET_ROOT", "true")
            .env(
                "SUMI_RUNTIME_SECRET_HOST_ROOT",
                &fixture.runtime_secret_root,
            )
            .env("SUMI_FAKE_DOCKER_LOG", &log);
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{action} required removed launch configuration: {}",
            supervisor_failure(&output)
        );
        let calls = std::fs::read_to_string(&log).unwrap();
        assert!(
            calls.contains(&format!(
                "--project-name {} --file {} {expected}",
                fixture.project,
                fixture
                    .deploy_dir()
                    .join("compose.lifecycle.yaml")
                    .display()
            )),
            "unexpected {action} calls: {calls}"
        );
        assert!(
            !calls.contains(fixture.deploy_dir().join("compose.yaml").to_str().unwrap()),
            "{action} evaluated the secret-bearing launch descriptor"
        );
    }

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn read_only_supervisor_actions_do_not_require_the_host_mutation_lock() {
    let root = std::env::temp_dir().join(format!("read-only-{}", Uuid::now_v7().simple()));
    let bin = root.join("bin");
    let log = root.join("docker.log");
    std::fs::create_dir_all(&bin).unwrap();
    let fake_docker = bin.join("docker");
    std::fs::write(
        &fake_docker,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$SUMI_FAKE_DOCKER_LOG\"\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let inherited_path = std::env::var("PATH").unwrap_or_default();

    for (action, expected) in [("status", " ps"), ("logs", " logs --tail 1")] {
        std::fs::write(&log, b"").unwrap();
        let mut command = Command::new(deploy_dir().join("supervisor"));
        command
            .env_clear()
            .arg(action)
            .env("PATH", format!("{}:{inherited_path}", bin.display()))
            .env("SUMI_CONFIG_FILE", "/dev/null")
            .env("SUMI_PERSONALITY_AGENT_ID", PAID_A)
            .env("SUMI_FAKE_DOCKER_LOG", &log);
        if action == "logs" {
            command.args(["--tail", "1"]);
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{action} required or created the host mutation lock: {}",
            supervisor_failure(&output)
        );
        let calls = std::fs::read_to_string(&log).unwrap();
        assert!(calls.contains("compose version"));
        assert!(
            calls.contains(&format!(
                "--project-name sumi-{} --file {}{expected}",
                PAID_A.replace('-', ""),
                deploy_dir().join("compose.lifecycle.yaml").display()
            )),
            "unexpected {action} calls: {calls}"
        );
    }

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn followed_logs_do_not_block_an_exclusive_stop() {
    let Some(mut fixture) = HostTrustFixture::new() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("logs-stop-{}", Uuid::now_v7().simple()));
    let bin = root.join("bin");
    let markers = root.join("markers");
    std::fs::create_dir_all(&bin).unwrap();
    std::fs::create_dir_all(&markers).unwrap();
    let fake_docker = bin.join("docker");
    std::fs::write(
        &fake_docker,
        r#"#!/bin/sh
case "$*" in
  "compose version")
    exit 0
    ;;
  *" logs -f")
    touch "$SUMI_FAKE_MARKERS/logs-started"
    while test ! -e "$SUMI_FAKE_MARKERS/release-logs"; do
      sleep 0.01
    done
    exit 0
    ;;
  *" down --remove-orphans "*)
    touch "$SUMI_FAKE_MARKERS/stop-completed"
    exit 0
    ;;
  *)
    exit 97
    ;;
esac
"#,
    )
    .unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let inherited_path = std::env::var("PATH").unwrap_or_default();

    let mut logs = fixture.supervisor_command();
    logs.arg("logs")
        .arg("-f")
        .env("PATH", format!("{}:{inherited_path}", bin.display()))
        .env("SUMI_FAKE_MARKERS", &markers)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    launch_runtime_env(&mut logs, &fixture);
    let mut logs = logs.spawn().unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while !markers.join("logs-started").exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    let logs_started = markers.join("logs-started").exists();

    let mut stop = fixture.supervisor_command();
    stop.arg("stop")
        .env("PATH", format!("{}:{inherited_path}", bin.display()))
        .env("SUMI_FAKE_MARKERS", &markers)
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(root.join("stop.stderr")).unwrap(),
        ));
    launch_runtime_env(&mut stop, &fixture);
    let mut stop = stop.spawn().unwrap();
    let stop_deadline = Instant::now() + Duration::from_secs(5);
    let mut stop_status = None;
    while !markers.join("stop-completed").exists() && Instant::now() < stop_deadline {
        if let Some(status) = stop.try_wait().unwrap() {
            stop_status = Some(status);
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    let stop_completed_before_logs_release = markers.join("stop-completed").exists();

    // Release and join both children before making assertions. This keeps the
    // regression path bounded even if stop blocks behind a mistakenly held
    // logs lock instead of failing its nonblocking acquisition.
    let release_result = std::fs::write(markers.join("release-logs"), b"");
    let logs_status = wait_for_child_exit(&mut logs, Duration::from_secs(5));
    if logs_status.is_none() {
        let _ = logs.kill();
        let _ = logs.wait();
    }
    if stop_status.is_none() {
        stop_status = wait_for_child_exit(&mut stop, Duration::from_secs(5));
    }
    if stop_status.is_none() {
        let _ = stop.kill();
        let _ = stop.wait();
    }
    let stop_stderr = std::fs::read(root.join("stop.stderr")).unwrap_or_default();
    let fixture_cleanup = fixture.cleanup();
    let temp_cleanup = std::fs::remove_dir_all(&root);

    assert!(release_result.is_ok(), "could not release followed logs");
    assert!(
        logs_started,
        "followed logs did not reach the fake Docker stream"
    );
    assert!(
        stop_completed_before_logs_release,
        "stop did not reach Docker while logs remained active: {}",
        String::from_utf8_lossy(&stop_stderr)
    );
    assert_eq!(
        stop_status.and_then(|status| status.code()),
        Some(0),
        "stop did not terminate successfully: {}",
        String::from_utf8_lossy(&stop_stderr)
    );
    assert_eq!(
        logs_status.and_then(|status| status.code()),
        Some(0),
        "followed logs did not terminate after its fake stream was released"
    );
    assert!(
        fixture_cleanup.is_ok(),
        "host trust cleanup failed: {:?}",
        fixture_cleanup.err()
    );
    assert!(
        temp_cleanup.is_ok(),
        "temporary concurrency fixture survived"
    );
}

#[test]
fn failed_up_cleans_every_partial_role_under_lock_and_preserves_status() {
    let Some(fixture) = HostTrustFixture::new() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("pc-{}", Uuid::now_v7().simple()));
    let bin = root.join("bin");
    let markers = root.join("markers");
    std::fs::create_dir_all(&bin).unwrap();
    std::fs::create_dir_all(&markers).unwrap();
    let fake_docker = bin.join("docker");
    let script = r#"#!/bin/bash
printf '%s\n' "$*" >> "$SUMI_FAKE_DOCKER_LOG"
case "$*" in
  "compose version")
    exit 0
    ;;
  *"compose.prepare.yaml config --quiet")
    exit 0
    ;;
  *"compose.lifecycle.yaml down --remove-orphans"*)
    if [[ -f "$SUMI_FAKE_MARKERS/up-attempted" ]]; then
      exec 9<>"$SUMI_EXPECT_LOCK_PATH"
      if flock -n 9; then
        touch "$SUMI_FAKE_MARKERS/cleanup-lock-missing"
      else
        touch "$SUMI_FAKE_MARKERS/cleanup-lock-held"
      fi
      rm -f \
        "$SUMI_FAKE_MARKERS/allocator" \
        "$SUMI_FAKE_MARKERS/prepare" \
        "$SUMI_FAKE_MARKERS/runtime" \
        "$SUMI_FAKE_MARKERS/executor" \
        "$SUMI_FAKE_MARKERS/broker"
      exit 88
    fi
    exit 0
    ;;
  *"compose.lifecycle.yaml ps --all --quiet")
    for role in allocator prepare runtime executor broker; do
      if [[ -f "$SUMI_FAKE_MARKERS/$role" ]]; then
        printf 'fake-container-%s\n' "$role"
      fi
    done
    exit 0
    ;;
  "ps --all --filter label=com.docker.compose.project="*)
    for role in allocator prepare runtime executor broker; do
      if [[ -f "$SUMI_FAKE_MARKERS/$role" ]]; then
        printf 'fake-container-%s\n' "$role"
      fi
    done
    exit 0
    ;;
  *"compose.prepare.yaml up --detach --wait")
    touch "$SUMI_FAKE_MARKERS/up-attempted"
    touch \
      "$SUMI_FAKE_MARKERS/allocator" \
      "$SUMI_FAKE_MARKERS/prepare" \
      "$SUMI_FAKE_MARKERS/runtime" \
      "$SUMI_FAKE_MARKERS/executor" \
      "$SUMI_FAKE_MARKERS/broker"
    exit 37
    ;;
  *)
    exit 93
    ;;
esac
"#;
    std::fs::write(&fake_docker, script).unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let log = root.join("docker.log");
    let inherited_path = std::env::var("PATH").unwrap_or_default();

    let mut command = fixture.supervisor_command();
    command
        .arg("prepare")
        .env("PATH", format!("{}:{inherited_path}", bin.display()))
        .env("SUMI_FAKE_DOCKER_LOG", &log)
        .env("SUMI_FAKE_MARKERS", &markers)
        .env("SUMI_EXPECT_LOCK_PATH", &fixture.lock_path);
    launch_runtime_env(&mut command, &fixture);
    let output = command.output().unwrap();
    assert_eq!(
        output.status.code(),
        Some(37),
        "cleanup replaced the original up failure: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    for role in ["allocator", "prepare", "runtime", "executor", "broker"] {
        assert!(
            !markers.join(role).exists(),
            "partial {role} survived failure cleanup"
        );
    }
    assert_marker(&markers, "cleanup-lock-held");
    assert!(!markers.join("cleanup-lock-missing").exists());
    assert!(
        !String::from_utf8_lossy(&output.stderr).contains("cleanup incomplete"),
        "verified-empty cleanup was reported as incomplete"
    );

    let calls = std::fs::read_to_string(&log).unwrap();
    assert_eq!(
        calls
            .lines()
            .filter(|line| line.contains("compose.lifecycle.yaml down --remove-orphans"))
            .count(),
        2,
        "expected pre-launch join and failure cleanup: {calls}"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn failed_up_retries_cleanup_and_reports_redacted_exhaustion_without_replacing_status() {
    let Some(fixture) = HostTrustFixture::new() else {
        return;
    };
    let inherited_path = std::env::var("PATH").unwrap_or_default();
    let cleanup_sentinel = "cleanup-output-sentinel-not-for-output";
    let provider_sentinel = "cleanup-provider-sentinel-not-for-output";

    for (mode, cleanup_attempts, expect_empty, expect_diagnostic) in
        [("retry", 2, true, false), ("exhaust", 3, false, true)]
    {
        let root = std::env::temp_dir().join(format!("cleanup-{mode}-{}", Uuid::now_v7().simple()));
        let bin = root.join("bin");
        let markers = root.join("markers");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::create_dir_all(&markers).unwrap();
        let fake_docker = bin.join("docker");
        let script = r#"#!/bin/bash
printf '%s\n' "$*" >> "$SUMI_FAKE_DOCKER_LOG"
case "$*" in
  "compose version" | *"compose.prepare.yaml config --quiet")
    exit 0
    ;;
  *"compose.lifecycle.yaml down --remove-orphans"*)
    if [[ ! -f "$SUMI_FAKE_MARKERS/up-attempted" ]]; then
      exit 0
    fi
    exec 9<>"$SUMI_EXPECT_LOCK_PATH"
    if flock -n 9; then
      touch "$SUMI_FAKE_MARKERS/cleanup-lock-missing"
    else
      touch "$SUMI_FAKE_MARKERS/cleanup-lock-held"
    fi
    attempt=0
    if [[ -f "$SUMI_FAKE_MARKERS/cleanup-attempts" ]]; then
      read -r attempt < "$SUMI_FAKE_MARKERS/cleanup-attempts"
    fi
    attempt=$((attempt + 1))
    printf '%s\n' "$attempt" > "$SUMI_FAKE_MARKERS/cleanup-attempts"
    printf '%s %s\n' "$SUMI_CLEANUP_SENTINEL" "$SUMI_PROVIDER_API_KEY"
    printf '%s %s\n' "$SUMI_CLEANUP_SENTINEL" "$SUMI_PROVIDER_API_KEY" >&2
    if [[ "$SUMI_FAKE_CLEANUP_MODE" == retry && "$attempt" -ge 2 ]]; then
      rm -f \
        "$SUMI_FAKE_MARKERS/allocator" \
        "$SUMI_FAKE_MARKERS/prepare" \
        "$SUMI_FAKE_MARKERS/runtime" \
        "$SUMI_FAKE_MARKERS/executor" \
        "$SUMI_FAKE_MARKERS/broker"
    fi
    exit 88
    ;;
  "ps --all --filter label=com.docker.compose.project="*)
    for role in runtime executor broker; do
      if [[ -f "$SUMI_FAKE_MARKERS/$role" ]]; then
        printf 'aaaaaaaaaaaa\t%s\texited\n' "$role"
      fi
    done
    exit 0
    ;;
  *"compose.lifecycle.yaml ps --all --quiet")
    printf '%s %s\n' "$SUMI_CLEANUP_SENTINEL" "$SUMI_PROVIDER_API_KEY" >&2
    for role in allocator prepare runtime executor broker; do
      if [[ -f "$SUMI_FAKE_MARKERS/$role" ]]; then
        printf 'fake-container %s %s\n' "$SUMI_CLEANUP_SENTINEL" "$SUMI_PROVIDER_API_KEY"
        exit 0
      fi
    done
    exit 0
    ;;
  *"compose.prepare.yaml up --detach --wait")
    touch "$SUMI_FAKE_MARKERS/up-attempted"
    touch \
      "$SUMI_FAKE_MARKERS/allocator" \
      "$SUMI_FAKE_MARKERS/prepare" \
      "$SUMI_FAKE_MARKERS/runtime" \
      "$SUMI_FAKE_MARKERS/executor" \
      "$SUMI_FAKE_MARKERS/broker"
    exit 37
    ;;
  *)
    exit 93
    ;;
esac
"#;
        std::fs::write(&fake_docker, script).unwrap();
        std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
        let log = root.join("docker.log");

        let mut command = fixture.supervisor_command();
        command
            .arg("prepare")
            .env("PATH", format!("{}:{inherited_path}", bin.display()))
            .env("SUMI_FAKE_DOCKER_LOG", &log)
            .env("SUMI_FAKE_MARKERS", &markers)
            .env("SUMI_EXPECT_LOCK_PATH", &fixture.lock_path)
            .env("SUMI_FAKE_CLEANUP_MODE", mode)
            .env("SUMI_CLEANUP_SENTINEL", cleanup_sentinel);
        launch_runtime_env(&mut command, &fixture);
        command.env("SUMI_PROVIDER_API_KEY", provider_sentinel);
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(37),
            "{mode} cleanup replaced the launch status: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let combined = [output.stdout, output.stderr.clone()].concat();
        for sentinel in [cleanup_sentinel, provider_sentinel] {
            assert!(
                !combined
                    .windows(sentinel.len())
                    .any(|window| window == sentinel.as_bytes()),
                "{mode} cleanup leaked {sentinel}"
            );
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            stderr.contains(
                "partial-generation cleanup incomplete after 3 attempts (details redacted)"
            ),
            expect_diagnostic,
            "unexpected {mode} cleanup diagnostic: {stderr}"
        );
        assert_marker(&markers, "cleanup-lock-held");
        assert!(!markers.join("cleanup-lock-missing").exists());
        assert_eq!(
            std::fs::read_to_string(markers.join("cleanup-attempts"))
                .unwrap()
                .trim(),
            cleanup_attempts.to_string()
        );
        for role in ["allocator", "prepare", "runtime", "executor", "broker"] {
            assert_eq!(
                !markers.join(role).exists(),
                expect_empty,
                "unexpected {mode} cleanup state for {role}"
            );
        }

        let calls = std::fs::read_to_string(&log).unwrap();
        assert_eq!(
            calls
                .lines()
                .filter(|line| line.contains("compose.lifecycle.yaml down --remove-orphans"))
                .count(),
            cleanup_attempts + 1,
            "unexpected {mode} down attempts: {calls}"
        );
        assert_eq!(
            calls
                .lines()
                .filter(|line| line.contains("ps --all --filter label=com.docker.compose.project="))
                .count(),
            cleanup_attempts,
            "unexpected {mode} long-lived epoch checks: {calls}"
        );
        let _ = std::fs::remove_dir_all(root);
    }
}

#[test]
fn interrupted_up_joins_partial_roles_and_returns_signal_status() {
    let Some(fixture) = HostTrustFixture::new() else {
        return;
    };
    let inherited_path = std::env::var("PATH").unwrap_or_default();

    for (label, signal, expected_status) in
        [("term", libc::SIGTERM, 143), ("int", libc::SIGINT, 130)]
    {
        let root = std::env::temp_dir().join(format!("sig-{label}-{}", Uuid::now_v7().simple()));
        let bin = root.join("bin");
        let markers = root.join("markers");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::create_dir_all(&markers).unwrap();
        let fake_docker = bin.join("docker");
        let script = r#"#!/bin/bash -p
# The supervisor control socket is never Docker/plugin authority.
[[ ! -e /proc/$$/fd/3 ]] || exit 96
# Every idle loop below waits on a backgrounded sleep. Bash defers a trap until
# the running foreground command returns, so `sleep 1` in the foreground would
# make this fake answer SIGTERM up to a second late per level for reasons that
# have nothing to do with the supervisor, and a loaded host would then see the
# supervisor's escalation to SIGKILL as a lost signal contract.
background_pid=
trap '[[ -z "$background_pid" ]] || wait "$background_pid" 2>/dev/null || true; touch "$SUMI_FAKE_MARKERS/compose-child-terminated"; exit 143' TERM
trap '[[ -z "$background_pid" ]] || wait "$background_pid" 2>/dev/null || true; touch "$SUMI_FAKE_MARKERS/compose-child-interrupted"; exit 130' INT
case "$*" in
  "compose version"|*"compose.prepare.yaml config --quiet")
    exit 0
    ;;
  *"compose.lifecycle.yaml down --remove-orphans"*)
    if [[ -f "$SUMI_FAKE_MARKERS/up-attempted" ]]; then
      exec 9<>"$SUMI_EXPECT_LOCK_PATH"
      if flock -n 9; then
        touch "$SUMI_FAKE_MARKERS/cleanup-lock-missing"
      else
        touch "$SUMI_FAKE_MARKERS/cleanup-lock-held"
      fi
      rm -f \
        "$SUMI_FAKE_MARKERS/runtime" \
        "$SUMI_FAKE_MARKERS/executor" \
        "$SUMI_FAKE_MARKERS/broker"
      touch "$SUMI_FAKE_MARKERS/cleanup-complete"
    fi
    exit 0
    ;;
  *"compose.lifecycle.yaml ps --all --quiet")
    for role in runtime executor broker; do
      if [[ -f "$SUMI_FAKE_MARKERS/$role" ]]; then
        printf 'fake-container-%s\n' "$role"
      fi
    done
    exit 0
    ;;
  "ps --all --filter label=com.docker.compose.project="*)
    for role in runtime executor broker; do
      if [[ -f "$SUMI_FAKE_MARKERS/$role" ]]; then
        printf 'fake-container-%s\n' "$role"
      fi
    done
    exit 0
    ;;
  *"compose.prepare.yaml up --detach --wait")
    touch \
      "$SUMI_FAKE_MARKERS/runtime" \
      "$SUMI_FAKE_MARKERS/executor" \
      "$SUMI_FAKE_MARKERS/broker"
    (
      trap 'touch "$SUMI_FAKE_MARKERS/compose-grandchild-terminated"; exit 0' TERM
      trap 'touch "$SUMI_FAKE_MARKERS/compose-grandchild-interrupted"; exit 0' INT
      while true; do sleep 1 & wait $!; done
    ) &
    background_pid=$!
    printf '%s\n' "$background_pid" > "$SUMI_FAKE_MARKERS/compose-grandchild-pid"
    # The test signals the supervisor the moment this marker appears, so it
    # must mean the whole tracked group exists. Announcing the up phase before
    # the fork let a loaded host take the signal in between, and the assertion
    # about the grandchild then failed against a grandchild that was never
    # started -- a race in the fixture, read as a lost signal contract.
    touch "$SUMI_FAKE_MARKERS/up-attempted"
    while true; do sleep 1 & wait $!; done
    ;;
  *)
    exit 94
    ;;
esac
"#;
        std::fs::write(&fake_docker, script).unwrap();
        std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();

        let bash_env = root.join("bash_env");
        std::fs::write(&bash_env, "set -m\n").unwrap();
        let mut command = fixture.supervisor_command();
        command
            .arg("prepare")
            .process_group(0)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .env("PATH", format!("{}:{inherited_path}", bin.display()))
            .env("BASH_ENV", &bash_env)
            .env("SUMI_FAKE_MARKERS", &markers)
            .env("SUMI_EXPECT_LOCK_PATH", &fixture.lock_path);
        launch_runtime_env(&mut command, &fixture);
        let mut child = command.spawn().unwrap();
        for _ in 0..100 {
            if markers.join("up-attempted").exists() {
                break;
            }
            if let Some(status) = child.try_wait().unwrap() {
                panic!("supervisor exited before {label} fixture was ready: {status}");
            }
            thread::sleep(Duration::from_millis(20));
        }
        assert!(
            markers.join("up-attempted").exists(),
            "supervisor never entered the {label} interruptible up phase"
        );
        assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
        let status = match wait_for_child_exit(&mut child, Duration::from_secs(5)) {
            Some(status) => status,
            None => {
                unsafe {
                    libc::kill(-(child.id() as i32), libc::SIGKILL);
                }
                let _ = child.wait();
                panic!("PID-only {label} did not promptly terminate the supervisor");
            }
        };
        assert_eq!(status.code(), Some(expected_status));
        assert_marker(&markers, "compose-child-terminated");
        assert_marker(&markers, "compose-grandchild-terminated");
        assert_marker(&markers, "cleanup-complete");
        assert_marker(&markers, "cleanup-lock-held");
        assert!(!markers.join("cleanup-lock-missing").exists());
        let grandchild_pid = std::fs::read_to_string(markers.join("compose-grandchild-pid"))
            .unwrap()
            .trim()
            .parse::<i32>()
            .unwrap();
        assert_ne!(unsafe { libc::kill(grandchild_pid, 0) }, 0);
        for role in ["runtime", "executor", "broker"] {
            assert!(
                !markers.join(role).exists(),
                "partial {role} survived PID-only {label}"
            );
        }
        let _ = std::fs::remove_dir_all(root);
    }
}

#[test]
fn tracked_launch_fails_closed_when_the_child_is_not_a_session_group_leader() {
    let Some(fixture) = HostTrustFixture::new() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("setsid-{}", Uuid::now_v7().simple()));
    let bin = root.join("bin");
    let markers = root.join("markers");
    std::fs::create_dir_all(&bin).unwrap();
    std::fs::create_dir_all(&markers).unwrap();
    std::fs::write(
        bin.join("setsid"),
        r#"#!/bin/bash
case "$*" in
  *"compose.prepare.yaml up --detach --wait"*)
    touch "$SUMI_FAKE_MARKERS/up-attempted"
    exec "$@"
    ;;
  *)
    exec /usr/bin/setsid "$@"
    ;;
esac
"#,
    )
    .unwrap();
    std::fs::set_permissions(bin.join("setsid"), std::fs::Permissions::from_mode(0o755)).unwrap();
    let fake_docker = bin.join("docker");
    let script = r#"#!/bin/bash
case "$*" in
  "compose version"|*"compose.prepare.yaml config --quiet")
    exit 0
    ;;
  *"compose.lifecycle.yaml down --remove-orphans"*)
    if [[ -f "$SUMI_FAKE_MARKERS/up-attempted" ]]; then
      exec 9<>"$SUMI_EXPECT_LOCK_PATH"
      if flock -n 9; then
        touch "$SUMI_FAKE_MARKERS/cleanup-lock-missing"
      else
        touch "$SUMI_FAKE_MARKERS/cleanup-lock-held"
      fi
      rm -f "$SUMI_FAKE_MARKERS/runtime"
      touch "$SUMI_FAKE_MARKERS/cleanup-complete"
    fi
    exit 0
    ;;
  *"compose.lifecycle.yaml ps --all --quiet")
    [[ ! -f "$SUMI_FAKE_MARKERS/runtime" ]]
    ;;
  "ps --all --filter label=com.docker.compose.project="*)
    [[ ! -f "$SUMI_FAKE_MARKERS/runtime" ]] || printf 'fake-container-runtime\n'
    ;;
  *"compose.prepare.yaml up --detach --wait")
    touch "$SUMI_FAKE_MARKERS/runtime"
    while true; do sleep 1 & wait $!; done
    ;;
  *)
    exit 95
    ;;
esac
"#;
    std::fs::write(&fake_docker, script).unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let inherited_path = std::env::var("PATH").unwrap_or_default();
    let mut command = fixture.supervisor_command();
    command
        .arg("prepare")
        .env("PATH", format!("{}:{inherited_path}", bin.display()))
        .env("SUMI_FAKE_MARKERS", &markers)
        .env("SUMI_EXPECT_LOCK_PATH", &fixture.lock_path);
    launch_runtime_env(&mut command, &fixture);
    let output = command.output().unwrap();
    assert_eq!(output.status.code(), Some(125));
    assert_marker(&markers, "up-attempted");
    assert_marker(&markers, "cleanup-complete");
    assert_marker(&markers, "cleanup-lock-held");
    assert!(!markers.join("cleanup-lock-missing").exists());
    assert!(!markers.join("runtime").exists());
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn validate_error_redacts_combined_compose_output() {
    let Some(fixture) = HostTrustFixture::new() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("ss-{}", Uuid::now_v7().simple()));
    let bin = root.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let fake_docker = bin.join("docker");
    std::fs::write(
        &fake_docker,
        "#!/bin/sh\ncase \"$*\" in\n  \"compose version\") exit 0 ;;\n  *\"config --quiet\")\n    printf '%s\\n' \"$SUMI_LOCAL_CONTROL_BEARER $SUMI_PROVIDER_API_KEY\"\n    printf '%s\\n' \"$SUMI_AGENT_WRAPPING_KEY $SUMI_APPROVAL_SECRET_DIGEST_KEY\" >&2\n    exit 41\n    ;;\n  *) exit 91 ;;\nesac\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let inherited_path = std::env::var("PATH").unwrap_or_default();

    let sentinels = [
        "control-sentinel-not-for-output",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        "provider-sentinel-not-for-output",
    ];
    let mut command = fixture.supervisor_command();
    command
        .arg("validate")
        .env("PATH", format!("{}:{inherited_path}", bin.display()))
        .env("SUMI_LOCAL_CONTROL_BEARER", sentinels[0])
        .env("SUMI_AGENT_WRAPPING_KEY", sentinels[1])
        .env("SUMI_APPROVAL_SECRET_DIGEST_KEY", sentinels[2])
        .env("SUMI_PROVIDER_API_KEY", sentinels[3]);
    launch_runtime_env(&mut command, &fixture);
    command
        .env("SUMI_LOCAL_CONTROL_BEARER", sentinels[0])
        .env("SUMI_AGENT_WRAPPING_KEY", sentinels[1])
        .env("SUMI_APPROVAL_SECRET_DIGEST_KEY", sentinels[2])
        .env("SUMI_PROVIDER_API_KEY", sentinels[3]);
    let output = command.output().unwrap();
    assert!(!output.status.success());
    let combined = [output.stdout, output.stderr].concat();
    assert!(
        String::from_utf8_lossy(&combined).contains("details redacted"),
        "validate should return only a redacted diagnostic"
    );
    for sentinel in sentinels {
        assert!(
            !combined
                .windows(sentinel.len())
                .any(|window| window == sentinel.as_bytes())
        );
    }
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn supervisor_rejects_multiline_secret_before_compose_mutation_without_echoing_it() {
    let root = std::env::temp_dir().join(format!("secret-lines-{}", Uuid::now_v7().simple()));
    let bin = root.join("bin");
    let log = root.join("docker.log");
    std::fs::create_dir_all(&bin).unwrap();
    let fake_docker = bin.join("docker");
    std::fs::write(
        &fake_docker,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$SUMI_FAKE_DOCKER_LOG\"\ncase \"$*\" in \"compose version\") exit 0 ;; *) exit 97 ;; esac\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let inherited_path = std::env::var("PATH").unwrap_or_default();
    let sentinel = "provider-secret-must-not-echo\nsecond-line";
    let mut command = Command::new(deploy_dir().join("supervisor"));
    command
        .arg("validate")
        .env("PATH", format!("{}:{inherited_path}", bin.display()))
        .env("SUMI_FAKE_DOCKER_LOG", &log)
        .env("SUMI_PROVIDER_API_KEY", sentinel)
        .env("SUMI_LOCAL_CONTROL_SERVER_UID", "1000")
        .env(
            "SUMI_LOCAL_CONTROL_SOCKET_GID",
            LOCAL_CONTROL_GID.to_string(),
        );
    launch_env(&mut command, PAID_A);
    command
        .env("PATH", format!("{}:{inherited_path}", bin.display()))
        .env("SUMI_FAKE_DOCKER_LOG", &log)
        .env("SUMI_PROVIDER_API_KEY", sentinel)
        .env("SUMI_LOCAL_CONTROL_SERVER_UID", "1000")
        .env(
            "SUMI_LOCAL_CONTROL_SOCKET_GID",
            LOCAL_CONTROL_GID.to_string(),
        );
    let output = command.output().unwrap();
    assert!(!output.status.success());
    let combined = [output.stdout, output.stderr].concat();
    assert!(
        !combined
            .windows(sentinel.len())
            .any(|window| window == sentinel.as_bytes())
    );
    assert!(
        String::from_utf8_lossy(&combined).contains("must not contain newline or carriage return")
    );
    assert_eq!(
        std::fs::read_to_string(&log).unwrap().trim(),
        "compose version",
        "multiline secret reached Compose validation or lifecycle mutation"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn supervisor_rejects_reserved_or_role_colliding_local_control_gids() {
    let Some(fixture) = HostTrustFixture::new() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("gid-{}", Uuid::now_v7().simple()));
    let bin = root.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let fake_docker = bin.join("docker");
    std::fs::write(
        &fake_docker,
        "#!/bin/sh\ncase \"$*\" in \"compose version\") exit 0 ;; *) exit 97 ;; esac\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let inherited_path = std::env::var("PATH").unwrap_or_default();

    for gid in ["0", "999", "65534", "10001", "10020"] {
        let mut command = fixture.supervisor_command();
        command
            .arg("validate")
            .env("PATH", format!("{}:{inherited_path}", bin.display()))
            .env("SUMI_LOCAL_CONTROL_SOCKET_GID", gid);
        launch_runtime_env(&mut command, &fixture);
        command.env("SUMI_LOCAL_CONTROL_SOCKET_GID", gid);
        let output = command.output().unwrap();
        assert!(!output.status.success(), "gid {gid} was accepted");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("reserved gid") || stderr.contains("collides"),
            "unexpected gid {gid} diagnostic: {stderr}"
        );
    }

    let mut wrong_uid = fixture.supervisor_command();
    wrong_uid
        .arg("validate")
        .env("PATH", format!("{}:{inherited_path}", bin.display()));
    launch_runtime_env(&mut wrong_uid, &fixture);
    wrong_uid.env(
        "SUMI_LOCAL_CONTROL_SERVER_UID",
        (unsafe { libc::geteuid() } + 1).to_string(),
    );
    let output = wrong_uid.output().unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("local-control registry root owner, group, or mode is not trusted")
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn supervisor_requires_explicit_local_control_socket_gid_before_lifecycle_mutation() {
    let Some(fixture) = HostTrustFixture::new() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("missing-gid-{}", Uuid::now_v7().simple()));
    let bin = root.join("bin");
    let log = root.join("docker.log");
    std::fs::create_dir_all(&bin).unwrap();
    let fake_docker = bin.join("docker");
    std::fs::write(
        &fake_docker,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$SUMI_FAKE_DOCKER_LOG\"\ncase \"$*\" in \"compose version\") exit 0 ;; *) exit 97 ;; esac\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let inherited_path = std::env::var("PATH").unwrap_or_default();
    let mut command = fixture.supervisor_command();
    command
        .arg("validate")
        .env("PATH", format!("{}:{inherited_path}", bin.display()))
        .env("SUMI_FAKE_DOCKER_LOG", &log);
    launch_env(&mut command, &fixture.paid);
    command
        .env_remove("SUMI_LOCAL_CONTROL_SOCKET_GID")
        .env(
            "SUMI_LOCAL_CONTROL_SERVER_UID",
            unsafe { libc::geteuid() }.to_string(),
        )
        .env_remove("SUMI_LOCAL_CONTROL_HOST_ROOT")
        .env_remove("SUMI_SUPERVISOR_LOCK_DIR");
    let output = command.output().unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("SUMI_LOCAL_CONTROL_SOCKET_GID is required")
    );
    let calls = std::fs::read_to_string(&log).unwrap();
    assert_eq!(calls.trim(), "compose version");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn local_control_path_swap_is_detected_before_validation_succeeds() {
    let Some(fixture) = HostTrustFixture::new() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("swap-{}", Uuid::now_v7().simple()));
    let bin = root.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let fake_docker = bin.join("docker");
    std::fs::write(
        &fake_docker,
        "#!/bin/sh\ncase \"$*\" in\n  \"compose version\") exit 0 ;;\n  *\"config --quiet\") mv \"$SUMI_FAKE_CONTROL_SOCKET\" \"$SUMI_FAKE_CONTROL_SOCKET.swapped\" ;;\n  *) exit 98 ;;\nesac\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let inherited_path = std::env::var("PATH").unwrap_or_default();
    let mut command = fixture.supervisor_command();
    command
        .arg("validate")
        .env("PATH", format!("{}:{inherited_path}", bin.display()))
        .env("SUMI_FAKE_CONTROL_SOCKET", &fixture.control_socket);
    launch_runtime_env(&mut command, &fixture);
    let output = command.output().unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("local-control socket must pre-exist")
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn prepare_mode_cleans_prior_role_owned_sockets_with_declared_capabilities() {
    if !timeout_available() {
        unavailable_host("GNU timeout is required to bound the prepare capability gate");
        return;
    }
    let deploy = deploy_dir();
    let docker_workdir = deploy.parent().unwrap().parent().unwrap();
    if !bounded_docker_output(docker_workdir, 30, &["info".into()])
        .status
        .success()
    {
        unavailable_host("docker info did not succeed within 30s for the prepare capability gate");
        return;
    }
    if !bounded_docker_output(
        docker_workdir,
        30,
        &[
            "image".into(),
            "inspect".into(),
            "debian:bookworm-slim".into(),
        ],
    )
    .status
    .success()
    {
        unavailable_host(
            "cached debian:bookworm-slim image is unavailable for the prepare capability gate",
        );
        return;
    }

    let root = std::env::temp_dir().join(format!("sumi-prepare-{}", Uuid::now_v7()));
    for directory in ["state", "workspace", "artifacts", "executor", "broker"] {
        std::fs::create_dir_all(root.join(directory)).unwrap();
    }
    // The worktree may be mode 0700, which makes a direct bind of this script
    // non-executable to the non-root container user before the script itself
    // runs. Exercise the exact artifact through an owned, traversable copy.
    let entrypoint = root.join("sumi-entrypoint");
    std::fs::copy(deploy_dir().join("container-entrypoint"), &entrypoint).unwrap();
    std::fs::set_permissions(&entrypoint, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::write(root.join("executor/executor.sock"), b"stale").unwrap();
    std::fs::write(root.join("broker/broker.sock"), b"stale").unwrap();

    let setup = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--network",
            "none",
            "-v",
            &format!("{}:/fixture", root.display()),
            "debian:bookworm-slim",
            "bash",
            "-c",
            "chown 10002:10020 /fixture/executor && chmod 2710 /fixture/executor && \
             chown 10003:10021 /fixture/broker && chmod 2710 /fixture/broker",
        ])
        .output()
        .unwrap();
    assert!(
        setup.status.success(),
        "fixture setup failed: {}",
        supervisor_failure(&setup)
    );

    let script_mount = format!("{}:/usr/local/bin/sumi-entrypoint:ro", entrypoint.display());
    let seccomp = format!(
        "seccomp={}",
        deploy_dir().join("seccomp/sidecar.json").display()
    );
    let mounts = [
        ("state", "/var/lib/sumi"),
        ("workspace", "/workspace"),
        ("artifacts", "/var/lib/sumi-artifacts"),
        ("executor", "/run/sumi/executor"),
        ("broker", "/run/sumi/broker"),
    ];
    let mut command = Command::new("docker");
    command.args([
        "run",
        "--rm",
        "--network",
        "none",
        "--read-only",
        "--cap-drop",
        "ALL",
        "--cap-add",
        "CHOWN",
        "--cap-add",
        "FOWNER",
        "--cap-add",
        "FSETID",
        "--security-opt",
        &seccomp,
        "-v",
        &script_mount,
    ]);
    for (source, target) in mounts {
        command.args(["-v", &format!("{}:{target}", root.join(source).display())]);
    }
    let output = command
        .args([
            "debian:bookworm-slim",
            "/usr/local/bin/sumi-entrypoint",
            "prepare",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "prepare failed under declared caps: {}",
        supervisor_failure(&output)
    );
    assert!(!root.join("executor/executor.sock").exists());
    assert!(!root.join("broker/broker.sock").exists());
    let executor = std::fs::metadata(root.join("executor")).unwrap();
    let broker = std::fs::metadata(root.join("broker")).unwrap();
    assert_eq!(executor.uid(), 10002);
    assert_eq!(executor.gid(), 10020);
    assert_eq!(executor.mode() & 0o7777, 0o2750);
    assert_eq!(broker.uid(), 10003);
    assert_eq!(broker.gid(), 10021);
    assert_eq!(broker.mode() & 0o7777, 0o2710);

    let cleanup = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &format!("{}:/fixture", root.display()),
            "debian:bookworm-slim",
            "bash",
            "-c",
            &format!(
                "chown -R {}:{} /fixture && chmod -R u+rwX /fixture",
                unsafe { libc::geteuid() },
                unsafe { libc::getegid() }
            ),
        ])
        .output()
        .unwrap();
    assert!(cleanup.status.success());
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn docker_compose_config_is_valid_or_cli_unavailable_is_classified() {
    let version = Command::new("docker").args(["compose", "version"]).output();
    let Ok(version) = version else {
        unavailable_host("docker executable is not installed");
        return;
    };
    if !version.status.success() {
        unavailable_host(&format!(
            "Docker Compose v2 is unavailable: {}",
            String::from_utf8_lossy(&version.stderr)
        ));
        return;
    }

    for (paid, project) in [
        (PAID_A, "sumi-0198f0f49b7270008000000000000001"),
        (PAID_B, "sumi-0198f0f49b7270008000000000000002"),
    ] {
        let mut command = Command::new("docker");
        command.args([
            "compose",
            "--project-name",
            project,
            "--file",
            deploy_dir().join("compose.yaml").to_str().unwrap(),
            "config",
            "--format",
            "json",
        ]);
        launch_env(&mut command, paid);
        command
            .env("SUMI_LOCAL_CONTROL_SERVER_UID", "1000")
            .env(
                "SUMI_LOCAL_CONTROL_SOCKET_GID",
                LOCAL_CONTROL_GID.to_string(),
            )
            .env(
                "SUMI_LOCAL_CONTROL_HOST_DIR",
                format!("/run/sumi/local-control/{}", paid.replace('-', "")),
            )
            .env(
                "SUMI_RUNTIME_SECRET_HOST_DIR",
                format!("/run/sumi/runtime-secrets/{}/1", paid.replace('-', "")),
            );
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "docker compose config failed: {}",
            supervisor_failure(&output)
        );
        let rendered: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(rendered["name"].as_str(), Some(project));
        assert_eq!(
            rendered["services"]["runtime"]["user"].as_str(),
            Some("10001:10001")
        );
        assert_eq!(
            rendered["services"]["executor"]["network_mode"].as_str(),
            Some("none")
        );
        assert_eq!(
            rendered["services"]["broker"]["network_mode"].as_str(),
            Some("none")
        );
        for role in ["runtime", "executor", "broker"] {
            assert_eq!(rendered["services"][role]["restart"].as_str(), Some("no"));
        }
        assert_eq!(
            rendered["services"]["allocator"]["environment"]["SUMI_RUNTIME_IDENTITY_GID"].as_str(),
            Some("10001")
        );
        assert_eq!(
            rendered["services"]["runtime"]["environment"]["SUMI_LOCAL_CONTROL_SERVER_UID"]
                .as_str(),
            Some("1000")
        );
        assert_eq!(
            rendered["services"]["runtime"]["environment"]["SUMI_LOCAL_CONTROL_SOCKET_GID"]
                .as_str(),
            Some("10022")
        );
        let rendered_model_id = rendered["services"]["runtime"]["environment"].get("SUMI_MODEL_ID");
        assert!(
            matches!(rendered_model_id, None | Some(JsonValue::Null)),
            "unset optional model ID must remain an omitted or null Compose pass-through"
        );
        for sensitive in [
            "SUMI_LOCAL_CONTROL_BEARER",
            "SUMI_AGENT_WRAPPING_KEY",
            "SUMI_APPROVAL_SECRET_DIGEST_KEY",
            "SUMI_PROVIDER_API_KEY",
            "SUMI_EXECUTION_REVIEWER_API_KEY",
            "SUMI_ESCALATION_REVIEWER_API_KEY",
        ] {
            assert!(
                rendered["services"]["runtime"]["environment"]
                    .get(sensitive)
                    .is_none(),
                "{sensitive} survived rendered Config.Env"
            );
        }
        assert!(
            rendered["services"]["runtime"]["group_add"]
                .as_array()
                .unwrap()
                .iter()
                .any(|group| group.as_str() == Some("10022"))
        );

        let mut exact_model = Command::new("docker");
        exact_model.args([
            "compose",
            "--project-name",
            project,
            "--file",
            deploy_dir().join("compose.yaml").to_str().unwrap(),
            "config",
            "--format",
            "json",
        ]);
        launch_env(&mut exact_model, paid);
        exact_model
            .env("SUMI_LOCAL_CONTROL_SERVER_UID", "1000")
            .env(
                "SUMI_LOCAL_CONTROL_SOCKET_GID",
                LOCAL_CONTROL_GID.to_string(),
            )
            .env(
                "SUMI_LOCAL_CONTROL_HOST_DIR",
                format!("/run/sumi/local-control/{}", paid.replace('-', "")),
            )
            .env(
                "SUMI_RUNTIME_SECRET_HOST_DIR",
                format!("/run/sumi/runtime-secrets/{}/1", paid.replace('-', "")),
            )
            .env("SUMI_MODEL_ID", "gpt-5.6-terra");
        let exact_model = exact_model.output().unwrap();
        assert!(
            exact_model.status.success(),
            "docker compose config with exact model failed: {}",
            supervisor_failure(&exact_model)
        );
        let exact_model: serde_json::Value = serde_json::from_slice(&exact_model.stdout).unwrap();
        assert_eq!(
            exact_model["services"]["runtime"]["environment"]["SUMI_MODEL_ID"].as_str(),
            Some("gpt-5.6-terra")
        );

        let lifecycle = Command::new("docker")
            .args([
                "compose",
                "--project-name",
                project,
                "--file",
                deploy_dir()
                    .join("compose.lifecycle.yaml")
                    .to_str()
                    .unwrap(),
                "config",
                "--quiet",
            ])
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .output()
            .unwrap();
        assert!(
            lifecycle.status.success(),
            "non-secret lifecycle descriptor failed: {}",
            supervisor_failure(&lifecycle)
        );
    }
}

#[test]
fn docker_runtime_acceptance_is_never_silently_treated_as_covered() {
    if !timeout_available() {
        unavailable_host("GNU timeout is required to bound Docker acceptance");
        return;
    }
    let output = bounded_docker_output(
        deploy_dir().parent().unwrap().parent().unwrap(),
        10,
        &[
            "info".into(),
            "--format".into(),
            "{{json .SecurityOptions}}".into(),
        ],
    );
    if !output.status.success() {
        unavailable_host(&format!(
            "docker daemon cannot be used: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
        return;
    }
    if std::env::var_os("SUMI_DEPLOYMENT_DOCKER_ACCEPTANCE").is_none() {
        eprintln!(
            "NOT_RUN: set SUMI_DEPLOYMENT_DOCKER_ACCEPTANCE=1 on the Docker/AppArmor host; \
             mandatory runtime isolation acceptance is not claimed by this test run"
        );
        return;
    }

    let security_options = String::from_utf8(output.stdout).unwrap();
    let Some(mut fixture) = HostTrustFixture::new() else {
        return;
    };
    let body = catch_unwind(AssertUnwindSafe(|| {
        if !security_options.contains("apparmor") {
            unavailable_host(
                "Docker is running without AppArmor; direct UUID-scoped Compose cleanup was exercised, but container mount/network/UID behavior remains an explicit Docker/AppArmor host gate",
            );
            return;
        }

        let expected_project = format!("sumi-{}", fixture.paid.replace('-', ""));
        assert_eq!(fixture.project, expected_project);
        let supervisor = fixture.supervisor.clone();
        let mut project_name = Command::new("timeout");
        project_name
            .args(["--preserve-status", "30s"])
            .arg(&supervisor)
            .arg("project-name");
        launch_owned_acceptance_env(&mut project_name, &fixture);
        let project_name = project_name.output().expect("derive owned test project");
        assert!(project_name.status.success());
        assert_eq!(
            String::from_utf8(project_name.stdout).unwrap().trim(),
            fixture.project,
            "acceptance cleanup is allowed only for the fixture's UUID-derived project"
        );

        let mut prepare = Command::new("timeout");
        prepare
            .args(["--preserve-status", "180s"])
            .arg(&supervisor)
            .arg("prepare");
        launch_owned_acceptance_env(&mut prepare, &fixture);
        let prepared = prepare
            .output()
            .expect("prepare owned Docker/AppArmor acceptance");
        assert!(
            prepared.status.success(),
            "real deployment prepare failed: {}",
            supervisor_failure(&prepared)
        );
        let prepared_epoch: JsonValue =
            serde_json::from_slice(&prepared.stdout).expect("prepared epoch JSON");
        let generation = prepared_epoch["generation"]
            .as_u64()
            .expect("prepared generation");
        let nonce = prepared_epoch["rpc_boot_nonce"]
            .as_str()
            .expect("prepared RPC nonce");

        let mut activate = Command::new("timeout");
        activate
            .args(["--preserve-status", "180s"])
            .arg(&supervisor)
            .arg("activate");
        launch_owned_acceptance_env(&mut activate, &fixture);
        activate
            .env("SUMI_EXPECTED_RPC_GENERATION", generation.to_string())
            .env("SUMI_EXPECTED_RPC_NONCE", nonce);
        let output = activate
            .output()
            .expect("activate owned Docker/AppArmor acceptance");
        let mut inspect = Command::new("timeout");
        inspect
            .args(["--preserve-status", "60s"])
            .arg(&supervisor)
            .arg("inspect-epoch");
        launch_owned_acceptance_env(&mut inspect, &fixture);
        let inspect = inspect
            .output()
            .expect("inspect activated Docker/AppArmor epoch");
        let setup_container_ids = bounded_docker_output(
            deploy_dir().parent().unwrap().parent().unwrap(),
            30,
            &[
                "ps".into(),
                "--all".into(),
                "--quiet".into(),
                "--filter".into(),
                format!("label=com.docker.compose.project={}", fixture.project),
                "--filter".into(),
                "label=com.docker.compose.service=allocator".into(),
            ],
        );
        let runtime_id = bounded_docker_output(
            deploy_dir().parent().unwrap().parent().unwrap(),
            30,
            &[
                "ps".into(),
                "--quiet".into(),
                "--filter".into(),
                format!("label=com.docker.compose.project={}", fixture.project),
                "--filter".into(),
                "label=com.docker.compose.service=runtime".into(),
            ],
        );
        let runtime_id_text = String::from_utf8_lossy(&runtime_id.stdout)
            .trim()
            .to_owned();
        let runtime_inspect = bounded_docker_output(
            deploy_dir().parent().unwrap().parent().unwrap(),
            30,
            &["inspect".into(), runtime_id_text.clone()],
        );
        let secret_metadata = bounded_docker_output(
            deploy_dir().parent().unwrap().parent().unwrap(),
            30,
            &[
                "exec".into(),
                runtime_id_text.clone(),
                "/bin/sh".into(),
                "-ec".into(),
                r#"
for secret in \
  /run/secrets/sumi_local_control_bearer \
  /run/secrets/sumi_agent_wrapping_key \
  /run/secrets/sumi_approval_secret_digest_key \
  /run/secrets/sumi_provider_api_key \
  /run/secrets/sumi_execution_reviewer_api_key \
  /run/secrets/sumi_escalation_reviewer_api_key
do
  test -f "$secret"
  test ! -L "$secret"
  test "$(stat -Lc '%u:%g:%a:%h' -- "$secret")" = 10001:10001:400:1
  test -r "$secret"
  test -s "$secret"
done
"#
                .into(),
            ],
        );
        // Always issue independent lifecycle teardown before asserting launch.
        // The outer cleanup below remains mandatory on every exit path.
        let mut stop = Command::new("timeout");
        stop.args(["--preserve-status", "60s"])
            .arg(&supervisor)
            .arg("stop");
        launch_owned_acceptance_env(&mut stop, &fixture);
        let stop = stop
            .output()
            .expect("stop owned Docker/AppArmor acceptance");
        assert!(
            stop.status.success(),
            "real deployment cleanup failed: {}",
            supervisor_failure(&stop)
        );
        assert!(
            output.status.success(),
            "real deployment failed: {}",
            supervisor_failure(&output)
        );
        assert!(
            inspect.status.success(),
            "inspect after real prepare -> activate failed: {}",
            supervisor_failure(&inspect)
        );
        let inspected_epoch: JsonValue =
            serde_json::from_slice(&inspect.stdout).expect("active epoch JSON");
        assert_eq!(inspected_epoch["phase"].as_str(), Some("active"));
        assert_eq!(inspected_epoch["generation"].as_u64(), Some(generation));
        assert_eq!(inspected_epoch["rpc_boot_nonce"].as_str(), Some(nonce));
        assert!(
            setup_container_ids.status.success()
                && !String::from_utf8_lossy(&setup_container_ids.stdout)
                    .trim()
                    .is_empty(),
            "prepare's completed allocator container did not remain for the active-epoch assertion: {}",
            String::from_utf8_lossy(&setup_container_ids.stderr)
        );
        assert!(
            runtime_id.status.success() && !runtime_id_text.is_empty(),
            "cannot resolve exact runtime container without printing secrets: {}",
            String::from_utf8_lossy(&runtime_id.stderr)
        );
        assert!(
            runtime_inspect.status.success(),
            "cannot inspect exact runtime container: {}",
            supervisor_failure(&runtime_inspect)
        );
        let runtime_inspect: JsonValue =
            serde_json::from_slice(&runtime_inspect.stdout).expect("runtime inspect JSON");
        let runtime_environment = runtime_inspect[0]["Config"]["Env"]
            .as_array()
            .expect("runtime Config.Env");
        for sensitive in [
            "SUMI_LOCAL_CONTROL_BEARER",
            "SUMI_AGENT_WRAPPING_KEY",
            "SUMI_APPROVAL_SECRET_DIGEST_KEY",
            "SUMI_PROVIDER_API_KEY",
            "SUMI_EXECUTION_REVIEWER_API_KEY",
            "SUMI_ESCALATION_REVIEWER_API_KEY",
        ] {
            assert!(
                runtime_environment.iter().all(|entry| {
                    !entry
                        .as_str()
                        .is_some_and(|entry| entry.starts_with(&format!("{sensitive}=")))
                }),
                "{sensitive} survived in runtime Config.Env"
            );
        }
        assert!(
            runtime_environment.iter().all(|entry| {
                !entry.as_str().is_some_and(|entry| {
                    entry.contains("deployment-test-local-control-")
                        || entry.contains("deployment-test-wrapping-")
                        || entry.contains("deployment-test-approval-")
                        || entry.contains("deployment-test-provider-")
                })
            }),
            "a runtime secret value survived under another Config.Env key"
        );
        assert!(
            secret_metadata.status.success(),
            "runtime secret metadata/readability contract failed: {}",
            supervisor_failure(&secret_metadata)
        );
    }));
    let cleanup = cleanup_owned_compose_resources(&fixture).and_then(|()| fixture.cleanup());
    finish_opt_in_docker_test(body, cleanup);
}

#[test]
fn scripts_are_executable() {
    for script in [
        deploy_dir().join("supervisor"),
        deploy_dir().join("container-entrypoint"),
    ] {
        let mode = std::fs::metadata(&script).unwrap().permissions().mode();
        assert_ne!(mode & 0o111, 0, "{} is not executable", script.display());
    }
}

/// A green run is compatible with a fixture that never provisioned anything, so
/// one test makes "the deployment fixture actually ran" a checked property
/// rather than something a reader has to infer from `--nocapture` output.
#[test]
fn deployment_fixture_provisions_private_anchors_or_says_why_it_did_not() {
    let Some(fixture) = HostTrustFixture::new() else {
        // `HostTrustFixture::new` only returns None under the explicit opt-in,
        // and it has already counted and named the skip on stderr.
        assert!(
            fixture_skip_is_opted_in(),
            "the fixture reported unavailable without the opt-in that permits it"
        );
        return;
    };
    let private_run_root = fixture.fixture_root.join("run/sumi");

    // Root-owned anchors the supervisor will validate, created inside the
    // container rather than by the test uid.
    for anchor in [
        &private_run_root,
        &private_run_root.join("supervisor-locks"),
    ] {
        let metadata = std::fs::metadata(anchor)
            .unwrap_or_else(|error| panic!("{} was never provisioned: {error}", anchor.display()));
        assert!(metadata.is_dir(), "{} is not a directory", anchor.display());
        assert_eq!(metadata.uid(), 0, "{} is not root owned", anchor.display());
        assert_eq!(
            metadata.permissions().mode() & 0o022,
            0,
            "{} is group or world writable",
            anchor.display()
        );
    }

    // The per-PAID trust the supervisor requires, owned by the test peer.
    let lock = std::fs::metadata(&fixture.lock_path).expect("per-PAID supervisor lock");
    assert_eq!(lock.uid(), unsafe { libc::geteuid() });
    assert_eq!(lock.permissions().mode() & 0o7777, 0o600);
    let control_dir = std::fs::metadata(fixture.control_socket.parent().unwrap())
        .expect("per-PAID local-control directory");
    assert_eq!(control_dir.uid(), unsafe { libc::geteuid() });
    assert_eq!(control_dir.gid(), fixture.control_gid);
    assert_eq!(control_dir.permissions().mode() & 0o7777, 0o750);

    // The supervisor under test is the private copy, and it is rooted here.
    assert!(
        fixture.supervisor.starts_with(&fixture.fixture_root),
        "the fixture is running the published supervisor: {}",
        fixture.supervisor.display()
    );
    let private_source = std::fs::read_to_string(&fixture.supervisor).unwrap();
    assert!(
        host_root_references_are_all_private(&private_source, &fixture.fixture_root),
        "the supervisor under test still references the host trust root"
    );

    // `unavailable_host()` is the non-tautological check that this fixture was
    // provisioned: without its explicit opt-in it asserts instead of returning.
    // Nothing above touched the live host anchors.
    fixture.assert_host_anchors_intact("fixture self-check");
}
