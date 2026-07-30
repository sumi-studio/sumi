use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    os::unix::net::UnixListener,
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, PermissionsExt},
    },
    path::PathBuf,
    process::Command,
};

use serde_yaml::Value;
use uuid::Uuid;

const PAID_A: &str = "0198f0f4-9b72-7000-8000-000000000001";
const PAID_B: &str = "0198f0f4-9b72-7000-8000-000000000002";

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

fn launch_runtime_env(command: &mut Command, root: &std::path::Path, paid: &str) -> UnixListener {
    launch_env(command, paid);
    let compact = paid.replace('-', "");
    let control_root = root.join("control");
    let control_dir = control_root.join(compact);
    let lock_dir = root.join("locks");
    std::fs::create_dir_all(&control_dir).unwrap();
    std::fs::create_dir_all(&lock_dir).unwrap();
    std::fs::set_permissions(&control_dir, std::fs::Permissions::from_mode(0o750)).unwrap();
    std::fs::set_permissions(&lock_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let socket = control_dir.join("control.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o660)).unwrap();
    command
        .env("SUMI_LOCAL_CONTROL_HOST_ROOT", &control_root)
        .env("SUMI_LOCAL_CONTROL_HOST_DIR", &control_dir)
        .env(
            "SUMI_LOCAL_CONTROL_SOCKET_GID",
            unsafe { libc::getegid() }.to_string(),
        )
        .env("SUMI_SUPERVISOR_LOCK_DIR", lock_dir);
    listener
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
    assert!(!entrypoint.contains("\nsource "));
    assert!(!entrypoint.contains("\neval "));
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
        .find(|mount| mount["target"].as_str() == Some("/run/sumi/local-control"))
        .expect("runtime local-control bind mount");
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
    for syscall in ["openat2", "umount2", "close_range", "prctl"] {
        assert!(ordinary.iter().any(|name| name.as_str() == Some(syscall)));
    }
    let unshare_values = syscalls[1..]
        .iter()
        .filter(|rule| rule["names"][0].as_str() == Some("unshare"))
        .map(|rule| rule["args"][0]["value"].as_u64().unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(unshare_values, BTreeSet::from([268_435_456, 1_073_872_896]));
}

#[test]
fn apparmor_profile_parses_when_host_tooling_is_available() {
    let output = Command::new("apparmor_parser")
        .arg("-Q")
        .arg(deploy_dir().join("apparmor/executor"))
        .output();
    let Ok(output) = output else {
        eprintln!("HOST_UNAVAILABLE: apparmor_parser is not installed");
        return;
    };
    assert!(
        output.status.success(),
        "AppArmor profile parse failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn deployment_contains_no_legacy_identity_or_file_readiness_contract() {
    let all = [
        "compose.yaml",
        "config.env",
        "container-entrypoint",
        "supervisor",
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
    let _control = launch_runtime_env(&mut command, &root, PAID_A);
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let calls = std::fs::read_to_string(&log).unwrap();
    let project = "sumi-0198f0f49b7270008000000000000001";
    let down = calls
        .find(&format!(
            "compose --project-name {project} --file {} down",
            deploy_dir().join("compose.yaml").display()
        ))
        .expect("old project must be stopped");
    let up = calls
        .find(&format!(
            "compose --project-name {project} --file {} up",
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
    let _control = launch_runtime_env(&mut command, &root, PAID_A);

    let lock = root
        .join("locks")
        .join("sumi-0198f0f49b7270008000000000000001.lock");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(lock)
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
fn validate_action_emits_no_secret_or_rendered_compose_output() {
    let root = std::env::temp_dir().join(format!("ss-{}", Uuid::now_v7().simple()));
    let bin = root.join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let fake_docker = bin.join("docker");
    std::fs::write(
        &fake_docker,
        "#!/bin/sh\ncase \"$*\" in\n  \"compose version\") exit 0 ;;\n  *\"config --quiet\") exit 0 ;;\n  *) exit 91 ;;\nesac\n",
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
    let _control = launch_runtime_env(&mut command, &root, PAID_A);
    command
        .env("SUMI_LOCAL_CONTROL_BEARER", sentinels[0])
        .env("SUMI_AGENT_WRAPPING_KEY", sentinels[1])
        .env("SUMI_APPROVAL_SECRET_DIGEST_KEY", sentinels[2])
        .env("SUMI_PROVIDER_API_KEY", sentinels[3]);
    let output = command.output().unwrap();
    assert!(output.status.success());
    let combined = [output.stdout, output.stderr].concat();
    assert!(combined.is_empty(), "validate must be silent on success");
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
        let root = std::env::temp_dir().join(format!("sc-{}", Uuid::now_v7().simple()));
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
        let _control = launch_runtime_env(&mut command, &root, paid);
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
        let _ = std::fs::remove_dir_all(root);
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
    assert!(
        output.status.success(),
        "real deployment failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stop = Command::new(deploy_dir().join("supervisor"))
        .arg("stop")
        .output()
        .expect("stop real Docker/AppArmor deployment acceptance");
    assert!(
        stop.status.success(),
        "real deployment cleanup failed: {}",
        String::from_utf8_lossy(&stop.stderr)
    );
}

#[test]
fn scripts_are_executable() {
    for script in [
        deploy_dir().join("supervisor"),
        deploy_dir().join("container-entrypoint"),
        deploy_dir().join("apparmor/load-profile"),
    ] {
        let mode = std::fs::metadata(&script).unwrap().permissions().mode();
        assert_ne!(mode & 0o111, 0, "{} is not executable", script.display());
    }
}
