use std::{
    collections::{BTreeMap, BTreeSet},
    os::unix::fs::PermissionsExt,
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
        .map(|value| value.as_str().expect("volume must use short syntax"))
        .collect()
}

fn volume_sources(service: &Value) -> BTreeSet<&str> {
    volume_strings(service)
        .into_iter()
        .map(|mount| mount.split(':').next().unwrap())
        .collect()
}

fn environment_keys(service: &Value) -> BTreeSet<&str> {
    service["environment"]
        .as_mapping()
        .map(|environment| {
            environment
                .keys()
                .map(|key| key.as_str().expect("environment key must be text"))
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

fn launch_env(command: &mut Command, paid: &str) {
    command
        .env("SUMI_CONFIG_FILE", "/dev/null")
        .env("SUMI_PERSONALITY_AGENT_ID", paid)
        .env("SUMI_GATEWAY_URL", "wss://gateway.invalid/agent")
        .env("SUMI_LOCAL_CONTROL_URL", "https://control.invalid")
        .env("SUMI_LOCAL_CONTROL_BEARER", "control-secret")
        .env("SUMI_LOCAL_CONTROL_BEARER_EXPIRES_AT_UNIX", "1900000000")
        .env("SUMI_AGENT_WRAPPING_KEY", "wrapping-secret")
        .env("SUMI_AGENT_WRAPPING_KEY_ID", "wrapping-key/v1")
        .env("SUMI_APPROVAL_SECRET_DIGEST_KEY", "approval-secret")
        .env("SUMI_PROVIDER_API_KEY", "provider-secret");
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
        BTreeSet::from([
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
        BTreeSet::from(["executor-ipc", "runtime-identity", "state"])
    );
    assert_eq!(
        volume_sources(executor),
        BTreeSet::from([
            "broker-ipc",
            "executor-identity",
            "executor-ipc",
            "workspace",
        ])
    );
    assert_eq!(
        volume_sources(broker),
        BTreeSet::from(["artifacts", "broker-identity", "broker-ipc"])
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

    assert_eq!(executor["network_mode"].as_str(), Some("none"));
    assert_eq!(broker["network_mode"].as_str(), Some("none"));
    assert!(
        runtime.get("network_mode").is_none(),
        "runtime must retain the provider/gateway network"
    );

    let runtime_env = environment_keys(runtime);
    let executor_env = environment_keys(executor);
    let broker_env = environment_keys(broker);
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
    let source = read_deploy("compose.yaml");
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
    assert!(source.contains("apparmor:sumi-agent-executor"));

    let entrypoint = read_deploy("container-entrypoint");
    assert!(entrypoint.contains("exec env -i"));
    assert!(entrypoint.contains("close_unlisted_fds"));
    assert!(entrypoint.contains("exec {fd}>&-"));
    assert!(entrypoint.contains("--tool-executor-socket"));
    assert!(entrypoint.contains("--artifact-broker"));
    assert!(entrypoint.contains("SUMI_ENFORCE_BROKER_SOCKET_NAMESPACE_ISOLATION=1"));
    assert!(!entrypoint.contains("SUMI_ARTIFACT_BROKER_SOCKET=/run/sumi/broker/broker.sock \\\n      /usr/local/bin/sumi-agent\n"));

    let seccomp: serde_json::Value =
        serde_json::from_str(&read_deploy("seccomp/sidecar.json")).unwrap();
    assert!(seccomp.is_object());
    let seccomp_source = read_deploy("seccomp/sidecar.json");
    for syscall in ["openat2", "umount2", "close_range", "prctl"] {
        assert!(seccomp_source.contains(&format!("\"{syscall}\"")));
    }
    assert!(seccomp_source.contains("\"value\": 268435456"));
    assert!(seccomp_source.contains("\"value\": 1073872896"));

    let apparmor = read_deploy("apparmor/executor");
    assert!(apparmor.contains("userns,"));
    assert!(apparmor.contains("mount options=(rprivate) none -> /"));
    assert!(apparmor.contains("/tmp/.sumi-broker-isolation-*"));
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
    assert!(all.contains("authenticated local control"));
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
    launch_env(&mut command, PAID_A);
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
        ]);
        launch_env(&mut command, paid);
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "docker compose config failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let rendered = String::from_utf8(output.stdout).unwrap();
        assert!(rendered.contains(&format!("name: {project}")));
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

    panic!(
        "DEPENDENCY_UNAVAILABLE: Docker/AppArmor host is ready, but end-to-end acceptance is \
         blocked until --supervisor-allocate and authenticated ExecutorClient::health() are integrated"
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
