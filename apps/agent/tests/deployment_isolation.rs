use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    os::unix::net::UnixListener,
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, PermissionsExt},
        unix::process::CommandExt,
    },
    path::PathBuf,
    process::{Command, Stdio},
    sync::{Mutex, MutexGuard, OnceLock},
    thread,
    time::{Duration, Instant},
};

use serde_yaml::Value;
use uuid::Uuid;

const PAID_A: &str = "0198f0f4-9b72-7000-8000-000000000001";
const PAID_B: &str = "0198f0f4-9b72-7000-8000-000000000002";
const LOCAL_CONTROL_GID: u32 = 10022;
const HOST_RUN_ROOT: &str = "/run/sumi";
static HOST_FIXTURE_LOCK: Mutex<()> = Mutex::new(());

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
        .env("SUMI_PERSONALITY_AGENT_ID", paid)
        .env("SUMI_GATEWAY_URL", "wss://gateway.invalid/agent")
        .env("SUMI_LOCAL_CONTROL_BEARER", "control-secret")
        .env("SUMI_LOCAL_CONTROL_BEARER_EXPIRES_AT_UNIX", "1900000000")
        .env("SUMI_AGENT_WRAPPING_KEY", "wrapping-secret")
        .env("SUMI_AGENT_WRAPPING_KEY_ID", "wrapping-key/v1")
        .env("SUMI_APPROVAL_SECRET_DIGEST_KEY", "approval-secret")
        .env("SUMI_PROVIDER_API_KEY", "provider-secret");
}

fn docker_fixture_host_available() -> bool {
    static AVAILABLE: OnceLock<bool> = OnceLock::new();
    *AVAILABLE.get_or_init(|| {
        Command::new("docker")
            .arg("info")
            .output()
            .is_ok_and(|output| output.status.success())
            && Command::new("docker")
                .args(["image", "inspect", "debian:bookworm-slim"])
                .output()
                .is_ok_and(|output| output.status.success())
    })
}

struct HostTrustFixture {
    paid: String,
    project: String,
    lock_path: PathBuf,
    control_socket: PathBuf,
    control_gid: u32,
    listener: Option<UnixListener>,
    _guard: MutexGuard<'static, ()>,
}

impl HostTrustFixture {
    fn new() -> Option<Self> {
        let guard = HOST_FIXTURE_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !docker_fixture_host_available() {
            eprintln!(
                "HOST_UNAVAILABLE: Docker and cached debian:bookworm-slim are required to \
                 provision root-owned fixed supervisor trust anchors"
            );
            return None;
        }
        let server_uid = unsafe { libc::geteuid() };
        let control_gid = unsafe { libc::getegid() };
        if server_uid == 0 {
            eprintln!(
                "HOST_UNAVAILABLE: deployment fixture requires a non-root test uid for the \
                 dedicated local-control peer"
            );
            return None;
        }
        if control_gid <= 999
            || control_gid == 65534
            || [10000, 10001, 10002, 10003, 10020, 10021].contains(&control_gid)
        {
            eprintln!(
                "HOST_UNAVAILABLE: deployment fixture requires a non-reserved, non-role \
                 primary test gid"
            );
            return None;
        }
        let paid = Uuid::now_v7().to_string();
        let compact = paid.replace('-', "");
        let project = format!("sumi-{compact}");
        let lock_path =
            PathBuf::from(HOST_RUN_ROOT).join(format!("supervisor-locks/{project}.lock"));
        let control_dir = PathBuf::from(HOST_RUN_ROOT).join(format!("local-control/{compact}"));
        let control_socket = control_dir.join("control.sock");
        let setup_script = r#"
set -eu
umask 022
mkdir -p /host-run/sumi /host-run/sumi/supervisor-locks /host-run/sumi/local-control
for anchor in /host-run/sumi /host-run/sumi/supervisor-locks /host-run/sumi/local-control; do
  test "$(stat -c %u "$anchor")" = 0
  mode="$(stat -c %a "$anchor")"
  test $((8#$mode & 0022)) = 0
done
install -d -m 0750 -o "$3" -g "$4" "/host-run/sumi/local-control/$1"
install -m 0600 -o "$3" -g "$4" /dev/null "/host-run/sumi/supervisor-locks/$2.lock"
"#;
        let setup = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--network",
                "none",
                "-v",
                "/run:/host-run",
                "debian:bookworm-slim",
                "bash",
                "-c",
                setup_script,
                "--",
                &compact,
                &project,
                &server_uid.to_string(),
                &control_gid.to_string(),
            ])
            .output()
            .unwrap();
        if !setup.status.success() {
            eprintln!(
                "HOST_UNAVAILABLE: fixed trust-anchor provisioning failed: {}",
                String::from_utf8_lossy(&setup.stderr)
            );
            return None;
        }

        let listener = UnixListener::bind(&control_socket).unwrap();
        std::fs::set_permissions(&control_socket, std::fs::Permissions::from_mode(0o660)).unwrap();

        Some(Self {
            paid,
            project,
            lock_path,
            control_socket,
            control_gid,
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
}

impl Drop for HostTrustFixture {
    fn drop(&mut self) {
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
        let _ = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--network",
                "none",
                "-v",
                "/run:/host-run",
                "debian:bookworm-slim",
                "bash",
                "-c",
                cleanup_script,
                "--",
                &compact,
                &self.project,
            ])
            .output();
    }
}

fn launch_runtime_env(command: &mut Command, fixture: &HostTrustFixture) {
    fixture.apply_launch(command);
    command
        .env_remove("SUMI_LOCAL_CONTROL_HOST_ROOT")
        .env_remove("SUMI_SUPERVISOR_LOCK_DIR");
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
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
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
            !volume_sources(long_lived).contains("allocator-state"),
            "allocator state leaked into a long-lived role"
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
            target.starts_with("/var/lib/sumi-allocator-root/"),
            "allocator mount escaped the pinned trust root: {mount}"
        );
    }
    let dockerfile = read_deploy("Dockerfile");
    assert!(dockerfile.contains("install -d -m 0700 /var/lib/sumi-allocator-root"));
    for role in ["runtime", "executor", "broker"] {
        assert!(dockerfile.contains(&format!(
            "/var/lib/sumi-allocator-root/identity-output/{role}"
        )));
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
            "${SUMI_LOCAL_CONTROL_HOST_DIR:?SUMI_LOCAL_CONTROL_HOST_DIR is required}/control.sock",
        ])
    );
    assert_eq!(
        volume_sources(executor),
        string_set(&[
            "broker-ipc",
            "executor-identity",
            "executor-ipc",
            "workspace",
        ])
    );
    assert_eq!(
        volume_sources(broker),
        string_set(&["artifacts", "broker-identity", "broker-ipc"])
    );
    assert_has_mount(runtime, "executor-ipc:/run/sumi/executor:ro");
    assert_has_mount(executor, "executor-ipc:/run/sumi/executor");
    assert_has_mount(executor, "broker-ipc:/run/sumi/broker:ro");
    assert_has_mount(broker, "broker-ipc:/run/sumi/broker");
    assert!(!volume_sources(runtime).contains("broker-ipc"));
    assert!(!volume_sources(runtime).contains("workspace"));
    assert!(!volume_sources(runtime).contains("artifacts"));
    assert!(!volume_sources(executor).contains("artifacts"));
    assert!(!volume_sources(executor).contains("state"));
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
    assert!(
        runtime.get("network_mode").is_none(),
        "runtime must retain the provider/gateway network"
    );

    let runtime_env = environment_keys(runtime);
    let executor_env = environment_keys(executor);
    let broker_env = environment_keys(broker);
    assert!(runtime_env.contains("SUMI_LOCAL_CONTROL_UNIX_SOCKET"));
    assert!(runtime_env.contains("SUMI_LOCAL_CONTROL_SERVER_UID"));
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
    ] {
        assert!(runtime_env.contains(sensitive));
        assert!(!executor_env.contains(sensitive));
        assert!(!broker_env.contains(sensitive));
    }
    let entrypoint = read_deploy("container-entrypoint");
    assert!(
        entrypoint.matches("SUMI_LOCAL_CONTROL_SERVER_UID").count() >= 2,
        "runtime env scrubber dropped the pinned local-control server uid"
    );
    assert!(
        entrypoint.matches("SUMI_LOCAL_CONTROL_SOCKET_GID").count() >= 2,
        "runtime env scrubber dropped the supervisor-validated local-control socket gid"
    );
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
    for syscall in ["openat2", "close_range", "prctl"] {
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
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$SUMI_FAKE_DOCKER_LOG\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let log = root.join("docker.log");
    let inherited_path = std::env::var("PATH").unwrap_or_default();

    let mut command = Command::new(deploy_dir().join("supervisor"));
    command
        .arg("up")
        .env("PATH", format!("{}:{inherited_path}", bin.display()))
        .env("SUMI_FAKE_DOCKER_LOG", &log);
    launch_runtime_env(&mut command, &fixture);
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let calls = std::fs::read_to_string(&log).unwrap();
    let down = calls
        .find(&format!(
            "compose --project-name {} --file {} down",
            fixture.project,
            deploy_dir().join("compose.lifecycle.yaml").display()
        ))
        .expect("old project must be stopped");
    let up = calls
        .find(&format!(
            "compose --project-name {} --file {} up",
            fixture.project,
            deploy_dir().join("compose.yaml").display()
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
    let mut command = Command::new(deploy_dir().join("supervisor"));
    command
        .arg("up")
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
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$SUMI_FAKE_DOCKER_LOG\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let log = root.join("docker.log");
    let inherited_path = std::env::var("PATH").unwrap_or_default();

    let mut up = Command::new(deploy_dir().join("supervisor"));
    up.arg("up")
        .env("PATH", format!("{}:{inherited_path}", bin.display()))
        .env("SUMI_FAKE_DOCKER_LOG", &log);
    launch_runtime_env(&mut up, &fixture);
    let output = up.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    for (action, expected) in [
        ("stop", "down --remove-orphans"),
        ("status", "ps"),
        ("logs", "logs"),
        ("down", "down --remove-orphans"),
    ] {
        std::fs::write(&log, b"").unwrap();
        let mut command = Command::new(deploy_dir().join("supervisor"));
        command
            .env_clear()
            .arg(action)
            .env("PATH", format!("{}:{inherited_path}", bin.display()))
            .env("SUMI_CONFIG_FILE", "/dev/null")
            .env("SUMI_PERSONALITY_AGENT_ID", &fixture.paid)
            .env("SUMI_FAKE_DOCKER_LOG", &log);
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{action} required removed launch configuration: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let calls = std::fs::read_to_string(&log).unwrap();
        assert!(
            calls.contains(&format!(
                "--project-name {} --file {} {expected}",
                fixture.project,
                deploy_dir().join("compose.lifecycle.yaml").display()
            )),
            "unexpected {action} calls: {calls}"
        );
        assert!(
            !calls.contains(deploy_dir().join("compose.yaml").to_str().unwrap()),
            "{action} evaluated the secret-bearing launch descriptor"
        );
    }

    let _ = std::fs::remove_dir_all(root);
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
  *"compose.yaml config --quiet")
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
  *"compose.yaml up --detach --build --wait")
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

    let mut command = Command::new(deploy_dir().join("supervisor"));
    command
        .arg("up")
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
    assert!(markers.join("cleanup-lock-held").exists());
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
  "compose version" | *"compose.yaml config --quiet")
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
  *"compose.yaml up --detach --build --wait")
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

        let mut command = Command::new(deploy_dir().join("supervisor"));
        command
            .arg("up")
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
        assert!(markers.join("cleanup-lock-held").exists());
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
                .filter(|line| line.contains("compose.lifecycle.yaml ps --all --quiet"))
                .count(),
            cleanup_attempts,
            "unexpected {mode} verification attempts: {calls}"
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
background_pid=
trap '[[ -z "$background_pid" ]] || wait "$background_pid" 2>/dev/null || true; touch "$SUMI_FAKE_MARKERS/compose-child-terminated"; exit 143' TERM
trap '[[ -z "$background_pid" ]] || wait "$background_pid" 2>/dev/null || true; touch "$SUMI_FAKE_MARKERS/compose-child-interrupted"; exit 130' INT
case "$*" in
  "compose version"|*"compose.yaml config --quiet")
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
  *"compose.yaml up --detach --build --wait")
    touch "$SUMI_FAKE_MARKERS/up-attempted"
    touch \
      "$SUMI_FAKE_MARKERS/runtime" \
      "$SUMI_FAKE_MARKERS/executor" \
      "$SUMI_FAKE_MARKERS/broker"
    (
      trap 'touch "$SUMI_FAKE_MARKERS/compose-grandchild-terminated"; exit 0' TERM
      trap 'touch "$SUMI_FAKE_MARKERS/compose-grandchild-interrupted"; exit 0' INT
      while true; do sleep 1; done
    ) &
    background_pid=$!
    printf '%s\n' "$background_pid" > "$SUMI_FAKE_MARKERS/compose-grandchild-pid"
    while true; do sleep 1; done
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
        let mut command = Command::new(deploy_dir().join("supervisor"));
        command
            .arg("up")
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
        assert!(markers.join("compose-child-terminated").exists());
        assert!(markers.join("compose-grandchild-terminated").exists());
        assert!(markers.join("cleanup-complete").exists());
        assert!(markers.join("cleanup-lock-held").exists());
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
    std::fs::write(bin.join("setsid"), "#!/bin/sh\nexec \"$@\"\n").unwrap();
    std::fs::set_permissions(bin.join("setsid"), std::fs::Permissions::from_mode(0o755)).unwrap();
    let fake_docker = bin.join("docker");
    let script = r#"#!/bin/bash
case "$*" in
  "compose version"|*"compose.yaml config --quiet")
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
  *"compose.yaml up --detach --build --wait")
    touch "$SUMI_FAKE_MARKERS/up-attempted" "$SUMI_FAKE_MARKERS/runtime"
    while true; do sleep 1; done
    ;;
  *)
    exit 95
    ;;
esac
"#;
    std::fs::write(&fake_docker, script).unwrap();
    std::fs::set_permissions(&fake_docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    let inherited_path = std::env::var("PATH").unwrap_or_default();
    let mut command = Command::new(deploy_dir().join("supervisor"));
    command
        .arg("up")
        .env("PATH", format!("{}:{inherited_path}", bin.display()))
        .env("SUMI_FAKE_MARKERS", &markers)
        .env("SUMI_EXPECT_LOCK_PATH", &fixture.lock_path);
    launch_runtime_env(&mut command, &fixture);
    let output = command.output().unwrap();
    assert_eq!(output.status.code(), Some(125));
    assert!(markers.join("up-attempted").exists());
    assert!(markers.join("cleanup-complete").exists());
    assert!(markers.join("cleanup-lock-held").exists());
    assert!(!markers.join("cleanup-lock-missing").exists());
    assert!(!markers.join("runtime").exists());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("did not become its own live session and process group")
    );
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
        "wrapping-sentinel-not-for-output",
        "approval-sentinel-not-for-output",
        "provider-sentinel-not-for-output",
    ];
    let mut command = Command::new(deploy_dir().join("supervisor"));
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
        let mut command = Command::new(deploy_dir().join("supervisor"));
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

    let mut wrong_uid = Command::new(deploy_dir().join("supervisor"));
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
        String::from_utf8_lossy(&output.stderr).contains("local-control parent uid does not match")
    );
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
    let mut command = Command::new(deploy_dir().join("supervisor"));
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
    if Command::new("docker")
        .arg("info")
        .output()
        .map_or(true, |output| !output.status.success())
    {
        eprintln!("HOST_UNAVAILABLE: docker daemon cannot run prepare capability gate");
        return;
    }
    if Command::new("docker")
        .args(["image", "inspect", "debian:bookworm-slim"])
        .output()
        .map_or(true, |output| !output.status.success())
    {
        eprintln!("HOST_UNAVAILABLE: cached debian:bookworm-slim image is unavailable");
        return;
    }

    let root = std::env::temp_dir().join(format!("sumi-prepare-{}", Uuid::now_v7()));
    for directory in ["state", "workspace", "artifacts", "executor", "broker"] {
        std::fs::create_dir_all(root.join(directory)).unwrap();
    }
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
        String::from_utf8_lossy(&setup.stderr)
    );

    let script_mount = format!(
        "{}:/usr/local/bin/sumi-entrypoint:ro",
        deploy_dir().join("container-entrypoint").display()
    );
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
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!root.join("executor/executor.sock").exists());
    assert!(!root.join("broker/broker.sock").exists());
    let executor = std::fs::metadata(root.join("executor")).unwrap();
    let broker = std::fs::metadata(root.join("broker")).unwrap();
    assert_eq!(executor.uid(), 10002);
    assert_eq!(executor.gid(), 10020);
    assert_eq!(executor.mode() & 0o7777, 0o2710);
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
        eprintln!("HOST_UNAVAILABLE: docker executable is not installed");
        return;
    };
    if !version.status.success() {
        eprintln!(
            "HOST_UNAVAILABLE: Docker Compose v2 is unavailable: {}",
            String::from_utf8_lossy(&version.stderr)
        );
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
            );
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "docker compose config failed: {}",
            String::from_utf8_lossy(&output.stderr)
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
        assert!(
            rendered["services"]["runtime"]["group_add"]
                .as_array()
                .unwrap()
                .iter()
                .any(|group| group.as_str() == Some("10022"))
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
            String::from_utf8_lossy(&lifecycle.stderr)
        );
    }
}

#[test]
fn docker_runtime_acceptance_is_never_silently_treated_as_covered() {
    let output = Command::new("docker")
        .args(["info", "--format", "{{json .SecurityOptions}}"])
        .output();
    let security_options = match output {
        Ok(output) if output.status.success() => String::from_utf8(output.stdout).unwrap(),
        Ok(output) => {
            eprintln!(
                "HOST_UNAVAILABLE: docker daemon cannot be used: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        Err(error) => {
            eprintln!("HOST_UNAVAILABLE: docker info could not run: {error}");
            return;
        }
    };
    if !security_options.contains("apparmor") {
        eprintln!(
            "HOST_UNAVAILABLE: Docker is running without AppArmor; structural and supervisor \
             lifecycle tests ran, but container mount/network/UID behavior remains an explicit \
             Docker/AppArmor host gate"
        );
        return;
    }

    if std::env::var_os("SUMI_DEPLOYMENT_DOCKER_ACCEPTANCE").is_none() {
        eprintln!(
            "NOT_RUN: set SUMI_DEPLOYMENT_DOCKER_ACCEPTANCE=1 on the Docker/AppArmor host; \
             mandatory runtime isolation acceptance is not claimed by this test run"
        );
        return;
    }

    let output = Command::new(deploy_dir().join("supervisor"))
        .arg("up")
        .output()
        .expect("run real Docker/AppArmor deployment acceptance");
    // Always issue the independent lifecycle teardown before asserting the
    // launch result. The supervisor's own EXIT/ERR/signal trap handles partial
    // creation; this final stop also covers a successful launch.
    let stop = Command::new(deploy_dir().join("supervisor"))
        .arg("stop")
        .output()
        .expect("stop real Docker/AppArmor deployment acceptance");
    assert!(
        stop.status.success(),
        "real deployment cleanup failed: {}",
        String::from_utf8_lossy(&stop.stderr)
    );
    assert!(
        output.status.success(),
        "real deployment failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
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
