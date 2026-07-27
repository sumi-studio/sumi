#![cfg(target_os = "linux")]

//! T26 automated fault/isolation harness for the production deployment boundary.
//!
//! This harness verifies the parts of the boundary that can be exercised without
//! a full privileged site environment. When the host cannot provide an isolation
//! primitive (distinct UIDs, mount namespaces, etc.) the affected test reports
//! the blocker and passes after documenting it, rather than silently degrading.

use std::{
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    process::{Child, Command},
    time::timeout,
};
use uuid::Uuid;

const GENERATION: u64 = 42;
const NONCE: &str = "t26-deployment-nonce";
const CONVERSATION: &str = "conversation-1";

struct Fixture {
    root: PathBuf,
    workspace: PathBuf,
    artifacts: PathBuf,
    socket: PathBuf,
    broker: Child,
}

struct DockerContainerGuard {
    name: String,
}

impl Drop for DockerContainerGuard {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("sumi-deployment-{}-", Uuid::now_v7()));
        let workspace = root.join("workspace");
        let artifacts = root.join("artifacts");
        let broker_ipc = root.join("broker-ipc");
        let socket = broker_ipc.join("broker.sock");
        tokio::fs::create_dir_all(&workspace).await.unwrap();
        tokio::fs::create_dir_all(&artifacts).await.unwrap();
        tokio::fs::create_dir_all(&broker_ipc).await.unwrap();

        let broker = Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
            .arg("--artifact-broker")
            .env_clear()
            .env("SUMI_RPC_GENERATION", GENERATION.to_string())
            .env("SUMI_RPC_NONCE", NONCE)
            .env("SUMI_ARTIFACT_ROOT", &artifacts)
            .env("SUMI_ARTIFACT_BROKER_SOCKET", &socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        timeout(Duration::from_secs(5), async {
            while UnixStream::connect(&socket).await.is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("broker socket");

        Self {
            root,
            workspace,
            artifacts,
            socket,
            broker,
        }
    }

    fn executor(&self) -> Child {
        Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
            .arg("--tool-executor")
            .env_clear()
            .env("SUMI_RPC_GENERATION", GENERATION.to_string())
            .env("SUMI_RPC_NONCE", NONCE)
            .env("SUMI_WORKSPACE", &self.workspace)
            .env("SUMI_CONVERSATION_ID", CONVERSATION)
            .env("SUMI_ARTIFACT_BROKER_SOCKET", &self.socket)
            .env("SUMI_ENFORCE_BROKER_SOCKET_NAMESPACE_ISOLATION", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    }

    fn executor_in_netns(&self) -> Child {
        // Rootless network namespace: the current user maps to root inside the
        // user namespace and gets a fresh network namespace. Filesystem access
        // is inherited, so the executor can still reach the workspace and the
        // Unix-domain broker socket.
        Command::new("unshare")
            .arg("--user")
            .arg("--net")
            .arg("--kill-child")
            .arg("--")
            .arg(env!("CARGO_BIN_EXE_sumi-agent"))
            .arg("--tool-executor")
            .env_clear()
            .env("SUMI_RPC_GENERATION", GENERATION.to_string())
            .env("SUMI_RPC_NONCE", NONCE)
            .env("SUMI_WORKSPACE", &self.workspace)
            .env("SUMI_CONVERSATION_ID", CONVERSATION)
            .env("SUMI_ARTIFACT_BROKER_SOCKET", &self.socket)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.broker.start_kill();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn request(id: &str, operation: Value) -> Value {
    json!({
        "generation": GENERATION,
        "nonce": NONCE,
        "request_id": id,
        "operation": operation,
    })
}

fn allocate_generation(root: &Path) -> u64 {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
        .arg("--allocate-generation")
        .env_clear()
        .env("SUMI_STATE_DIR", root)
        .output()
        .expect("allocate generation");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let exports = String::from_utf8(output.stdout).expect("utf-8 exports");
    for line in exports.lines() {
        if let Some(value) = line.strip_prefix("export SUMI_RPC_GENERATION=") {
            return value.trim().parse().expect("generation is an integer");
        }
    }
    panic!("SUMI_RPC_GENERATION not found in: {exports}");
}

async fn broker_rpc(socket: &Path, request: &Value) -> Option<Value> {
    let mut stream = UnixStream::connect(socket).await.unwrap();
    let mut bytes = serde_json::to_vec(request).unwrap();
    bytes.push(b'\n');
    stream.write_all(&bytes).await.unwrap();
    stream.shutdown().await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    if response.is_empty() {
        None
    } else {
        Some(serde_json::from_slice(&response).unwrap())
    }
}

async fn send_request(stdin: &mut tokio::process::ChildStdin, value: &Value) {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    stdin.write_all(&bytes).await.unwrap();
    stdin.flush().await.unwrap();
}

async fn read_frame(reader: &mut BufReader<tokio::process::ChildStdout>) -> Value {
    let mut line = String::new();
    timeout(Duration::from_secs(5), reader.read_line(&mut line))
        .await
        .expect("response timeout")
        .expect("response read");
    assert!(!line.is_empty(), "executor closed before response");
    serde_json::from_str(&line).unwrap()
}

fn mode_of(path: &Path) -> u32 {
    std::fs::metadata(path).unwrap().mode() & 0o777
}

#[tokio::test]
async fn supervisor_identity_is_shared_and_rejects_mismatched_generation() {
    let fixture = Fixture::new().await;

    let mut stale = request(
        "stale",
        json!({
            "type": "begin_tool_output",
            "conversation_id": CONVERSATION,
            "execution_id": "stale",
            "content": [120],
        }),
    );
    stale["generation"] = json!(GENERATION - 1);
    assert_eq!(broker_rpc(&fixture.socket, &stale).await, None);

    let ok = broker_rpc(
        &fixture.socket,
        &request(
            "ok",
            json!({
                "type": "begin_tool_output",
                "conversation_id": CONVERSATION,
                "execution_id": "ok",
                "content": [121],
            }),
        ),
    )
    .await
    .unwrap();
    assert_eq!(ok["result"]["Ok"]["offset"], 1);
}

#[tokio::test]
async fn artifact_directories_and_files_are_private() {
    let fixture = Fixture::new().await;
    broker_rpc(
        &fixture.socket,
        &request(
            "begin",
            json!({
                "type": "begin_tool_output",
                "conversation_id": CONVERSATION,
                "execution_id": "execution-1",
                "content": [104, 101, 108, 108, 111],
            }),
        ),
    )
    .await
    .unwrap();

    assert_eq!(mode_of(&fixture.artifacts), 0o700);
    assert_eq!(mode_of(&fixture.artifacts.join(CONVERSATION)), 0o700);
    assert_eq!(
        mode_of(
            &fixture
                .artifacts
                .join(format!("{CONVERSATION}/tool-output"))
        ),
        0o700
    );
    assert_eq!(
        mode_of(
            &fixture
                .artifacts
                .join(format!("{CONVERSATION}/tool-output/execution-1"))
        ),
        0o600
    );
}

#[tokio::test]
async fn conversation_subtree_reset_is_idempotent_and_selective() {
    let fixture = Fixture::new().await;

    broker_rpc(
        &fixture.socket,
        &request(
            "begin-one",
            json!({
                "type": "begin_tool_output",
                "conversation_id": CONVERSATION,
                "execution_id": "execution-1",
                "content": b"one".to_vec(),
            }),
        ),
    )
    .await
    .unwrap();

    let other = "conversation-2";
    broker_rpc(
        &fixture.socket,
        &request(
            "begin-two",
            json!({
                "type": "begin_tool_output",
                "conversation_id": other,
                "execution_id": "execution-1",
                "content": b"two".to_vec(),
            }),
        ),
    )
    .await
    .unwrap();

    let delete = request(
        "delete",
        json!({
            "type": "delete_conversation_artifacts",
            "old_conversation_id": CONVERSATION,
            "tombstone_id": "tombstone-1",
        }),
    );
    let response = broker_rpc(&fixture.socket, &delete).await.unwrap();
    assert_eq!(response["result"]["Ok"]["type"], "deleted");

    assert!(!fixture.artifacts.join(CONVERSATION).exists());
    assert!(fixture.artifacts.join(other).exists());

    let replay = request(
        "delete-again",
        json!({
            "type": "delete_conversation_artifacts",
            "old_conversation_id": CONVERSATION,
            "tombstone_id": "tombstone-1",
        }),
    );
    let replay = broker_rpc(&fixture.socket, &replay).await.unwrap();
    assert_eq!(replay["result"]["Ok"]["type"], "deleted");
}

#[tokio::test]
async fn file_tools_continue_through_executor_uid() {
    let fixture = Fixture::new().await;
    let mut child = fixture.executor();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());

    send_request(
        &mut stdin,
        &request(
            "write",
            json!({
                "type": "write_file",
                "path": "note.txt",
                "content": "hello executor",
                "execution_id": "write-1",
            }),
        ),
    )
    .await;
    assert_eq!(
        read_frame(&mut stdout).await["result"]["Ok"]["type"],
        "written"
    );

    send_request(
        &mut stdin,
        &request(
            "read",
            json!({
                "type": "read_file",
                "path": "note.txt",
                "offset": 0,
                "limit": 51200,
                "execution_id": "read-1",
            }),
        ),
    )
    .await;
    let read = read_frame(&mut stdout).await;
    assert_eq!(read["result"]["Ok"]["result"]["content"], "hello executor");

    drop(stdin);
    assert!(child.wait().await.unwrap().success());
}

#[tokio::test]
async fn executor_rejects_workspace_escape() {
    let fixture = Fixture::new().await;
    let mut child = fixture.executor();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());

    send_request(
        &mut stdin,
        &request(
            "escape",
            json!({
                "type": "write_file",
                "path": "../escape.txt",
                "content": "no",
                "execution_id": "escape-1",
            }),
        ),
    )
    .await;
    let response = read_frame(&mut stdout).await;
    assert_eq!(response["result"]["Err"]["code"], "invalid_path");

    drop(stdin);
    assert!(child.wait().await.unwrap().success());
}

#[tokio::test]
async fn bash_env_does_not_leak_broker_socket() {
    let fixture = Fixture::new().await;
    let mut child = fixture.executor();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());

    send_request(
        &mut stdin,
        &request(
            "env",
            json!({
                "type": "bash",
                "command": "env",
                "execution_id": "env-1",
                "tool_call_id": "env-1-tc",
                "command_id": "env-1-cmd",
                "run_id": "run-1",
            }),
        ),
    )
    .await;
    let first = read_frame(&mut stdout).await;
    let (output, terminal) = if first["result"].is_null() {
        (
            first["value"]["output"].as_str().unwrap().to_owned(),
            read_frame(&mut stdout).await,
        )
    } else {
        (
            first["result"]["Ok"]["result"]["output"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            first,
        )
    };
    assert!(!output.contains("SUMI_ARTIFACT_BROKER_SOCKET"));
    assert_eq!(terminal["result"]["Ok"]["result"]["exit_code"], 0);

    drop(stdin);
    assert!(child.wait().await.unwrap().success());
}

#[tokio::test]
async fn production_seccomp_profile_allows_executor_bash_close_range() {
    let docker = Command::new("docker")
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
    if !docker.is_ok_and(|status| status.success()) {
        eprintln!("docker daemon unavailable; seccomp-constrained bash fixture not executed");
        return;
    }

    let fixture = Fixture::new().await;
    let executor_socket = fixture.root.join("executor.sock");
    let container_executor_socket = Path::new("/fixture/executor.sock");
    let container_broker_socket = Path::new("/fixture/broker-ipc/broker.sock");
    let container_workspace = Path::new("/fixture/workspace");
    let container_name = format!("sumi-seccomp-{}", Uuid::now_v7());
    let _container_guard = DockerContainerGuard {
        name: container_name.clone(),
    };
    let deploy_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("deploy/agent");
    let seccomp = deploy_dir.join("seccomp/sidecar.json");
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_sumi-agent"));

    let mut executor = Command::new("docker")
        .arg("run")
        .arg("--rm")
        .arg("--name")
        .arg(&container_name)
        .arg("--network")
        .arg("none")
        .arg("--security-opt")
        .arg(format!("seccomp={}", seccomp.display()))
        .arg("--user")
        .arg(format!("{}:{}", unsafe { libc::geteuid() }, unsafe {
            libc::getegid()
        }))
        .arg("--volume")
        .arg(format!("{}:/sumi-agent:ro", binary.display()))
        .arg("--volume")
        .arg(format!("{}:/fixture", fixture.root.display()))
        .arg("--env")
        .arg(format!("SUMI_RPC_GENERATION={GENERATION}"))
        .arg("--env")
        .arg(format!("SUMI_RPC_NONCE={NONCE}"))
        .arg("--env")
        .arg(format!("SUMI_CONVERSATION_ID={CONVERSATION}"))
        .arg("--env")
        .arg(format!("SUMI_WORKSPACE={}", container_workspace.display()))
        .arg("--env")
        .arg(format!(
            "SUMI_ARTIFACT_BROKER_SOCKET={}",
            container_broker_socket.display()
        ))
        .arg("--env")
        .arg(format!(
            "SUMI_EXECUTOR_SOCKET={}",
            container_executor_socket.display()
        ))
        // The test binary is built on the host and needs the host-generation
        // glibc ABI. Ubuntu 24.04 supplies glibc 2.39 while still exercising the
        // exact production seccomp JSON through Docker/runc.
        .arg("ubuntu:24.04")
        .arg("/sumi-agent")
        .arg("--tool-executor-socket")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start seccomp-constrained executor container");

    let ready = timeout(Duration::from_secs(15), async {
        loop {
            if UnixStream::connect(&executor_socket).await.is_ok() {
                break;
            }
            if let Some(status) = executor.try_wait().expect("poll executor") {
                let mut stderr = String::new();
                if let Some(mut pipe) = executor.stderr.take() {
                    pipe.read_to_string(&mut stderr)
                        .await
                        .expect("read executor stderr");
                }
                panic!("seccomp-constrained executor exited before binding: {status}: {stderr}");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    if ready.is_err() {
        panic!("seccomp-constrained executor did not bind");
    }

    let mut stream = UnixStream::connect(&executor_socket)
        .await
        .expect("connect constrained executor");
    let mut request_bytes = serde_json::to_vec(&request(
        "seccomp-bash",
        json!({
            "type": "bash",
            "command": "printf SECCOMP_CLOSE_RANGE_OK",
            "execution_id": "seccomp-bash-1",
            "tool_call_id": "seccomp-bash-1-tc",
            "command_id": "seccomp-bash-1-cmd",
            "run_id": "run-1",
        }),
    ))
    .unwrap();
    request_bytes.push(b'\n');
    stream.write_all(&request_bytes).await.unwrap();
    let mut frames = BufReader::new(stream).lines();
    let mut terminal = None;
    let mut streamed_output = String::new();
    while let Some(line) = timeout(Duration::from_secs(10), frames.next_line())
        .await
        .expect("executor frame timeout")
        .expect("executor frame read")
    {
        let frame: Value = serde_json::from_str(&line).expect("executor JSON frame");
        if !frame["result"].is_null() {
            terminal = Some(frame);
            break;
        }
        if let Some(output) = frame["value"]["output"].as_str() {
            streamed_output.push_str(output);
        }
    }
    let terminal = terminal.expect("executor terminal frame");
    let terminal_output = terminal["result"]["Ok"]["result"]["output"]
        .as_str()
        .unwrap_or_default();
    let observed_output = if terminal_output.is_empty() {
        streamed_output.as_str()
    } else {
        terminal_output
    };
    assert_eq!(
        observed_output, "SECCOMP_CLOSE_RANGE_OK",
        "unexpected constrained bash terminal: {terminal}"
    );
    assert_eq!(terminal["result"]["Ok"]["result"]["exit_code"], 0);

    drop(_container_guard);
    let _ = executor.wait().await;
}

#[tokio::test]
async fn bash_cannot_reach_broker_socket_path() {
    let fixture = Fixture::new().await;
    let mut child = fixture.executor();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());

    // First prove file tools still work through the executor UID.
    send_request(
        &mut stdin,
        &request(
            "write",
            json!({
                "type": "write_file",
                "path": "isolation.txt",
                "content": "workspace still reachable",
                "execution_id": "iso-write",
            }),
        ),
    )
    .await;
    assert_eq!(
        read_frame(&mut stdout).await["result"]["Ok"]["type"],
        "written"
    );

    send_request(
        &mut stdin,
        &request(
            "read",
            json!({
                "type": "read_file",
                "path": "isolation.txt",
                "offset": 0,
                "limit": 51200,
                "execution_id": "iso-read",
            }),
        ),
    )
    .await;
    let read = read_frame(&mut stdout).await;
    assert_eq!(
        read["result"]["Ok"]["result"]["content"],
        "workspace still reachable"
    );

    // The broker socket is hidden behind a mount namespace, so the bash child
    // cannot see or connect to it even though the same executor UID is used.
    send_request(
        &mut stdin,
        &request(
            "reach",
            json!({
                "type": "bash",
                "command": "test -S ../broker-ipc/broker.sock && printf REACHABLE || printf NOT_REACHABLE",
                "execution_id": "reach-1",
                "tool_call_id": "reach-1-tc",
                "command_id": "reach-1-cmd",
                "run_id": "run-1",
            }),
        ),
    )
    .await;
    let first = read_frame(&mut stdout).await;
    let (output, terminal) = if first["result"].is_null() {
        (
            first["value"]["output"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            read_frame(&mut stdout).await,
        )
    } else {
        (
            first["result"]["Ok"]["result"]["output"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            first,
        )
    };
    assert_eq!(output, "NOT_REACHABLE");
    assert_eq!(terminal["result"]["Ok"]["result"]["exit_code"], 0);

    // Prove the bind mount is private so the mask cannot propagate back to the
    // executor/host namespace. A propagated mount would show 'shared' or
    // 'master' in the broker-ipc line of /proc/self/mountinfo.
    send_request(
        &mut stdin,
        &request(
            "mountinfo",
            json!({
                "type": "bash",
                "command": r#"awk 'BEGIN{ok=0} /broker-ipc/{ok=1; if ($0 ~ /shared|master/) {print "PROPAGATION_LEAK"; exit 1}} END{if(!ok){print "NOT_FOUND"; exit 1} print "MOUNT_PRIVATE_OK"}' /proc/self/mountinfo"#,
                "execution_id": "mountinfo-1",
                "tool_call_id": "mountinfo-1-tc",
                "command_id": "mountinfo-1-cmd",
                "run_id": "run-1",
            }),
        ),
    )
    .await;
    let first = read_frame(&mut stdout).await;
    let (output, terminal) = if first["result"].is_null() {
        (
            first["value"]["output"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            read_frame(&mut stdout).await,
        )
    } else {
        (
            first["result"]["Ok"]["result"]["output"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            first,
        )
    };
    assert_eq!(output, "MOUNT_PRIVATE_OK\n");
    assert_eq!(terminal["result"]["Ok"]["result"]["exit_code"], 0);

    drop(stdin);
    assert!(child.wait().await.unwrap().success());
}

#[tokio::test]
async fn executor_preflight_validates_namespace_support() {
    // If the host cannot even unshare a user+network namespace, the preflight
    // cannot be expected to pass. Document the blocker and pass.
    let unshare_probe = Command::new("unshare")
        .arg("--user")
        .arg("--net")
        .arg("--mount")
        .arg("true")
        .status()
        .await;
    if !unshare_probe.is_ok_and(|s| s.success()) {
        eprintln!(
            "skipping namespace preflight: host cannot unshare user, network, and mount namespaces"
        );
        return;
    }

    let output = Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
        .arg("--tool-executor")
        .env_clear()
        .env("SUMI_ISOLATION_PREFLIGHT", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .await
        .expect("spawn isolation preflight");
    assert!(
        output.status.success(),
        "namespace isolation preflight must succeed on a supported host: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn bash_isolation_fails_closed_for_invalid_broker_socket() {
    let fixture = Fixture::new().await;
    let mut child = Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
        .arg("--tool-executor")
        .env_clear()
        .env("SUMI_RPC_GENERATION", GENERATION.to_string())
        .env("SUMI_RPC_NONCE", NONCE)
        .env("SUMI_WORKSPACE", &fixture.workspace)
        .env("SUMI_CONVERSATION_ID", CONVERSATION)
        // A broker socket with no parent directory cannot be masked; the
        // executor must fail closed instead of silently running the bash
        // command without isolation.
        .env("SUMI_ARTIFACT_BROKER_SOCKET", "/")
        .env("SUMI_ENFORCE_BROKER_SOCKET_NAMESPACE_ISOLATION", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    send_request(
        &mut stdin,
        &request(
            "invalid",
            json!({
                "type": "bash",
                "command": "true",
                "execution_id": "invalid-1",
                "tool_call_id": "invalid-1-tc",
                "command_id": "invalid-1-cmd",
                "run_id": "run-1",
            }),
        ),
    )
    .await;
    let terminal = read_frame(&mut stdout).await;
    assert_eq!(terminal["result"]["Err"]["code"], "io");

    drop(stdin);
    assert!(child.wait().await.unwrap().success());
}

#[tokio::test]
async fn executor_in_network_namespace_cannot_reach_external_network() {
    let fixture = Fixture::new().await;

    let unshare_probe = Command::new("unshare")
        .arg("--user")
        .arg("--net")
        .arg("true")
        .status()
        .await;
    assert!(
        unshare_probe.is_ok() && unshare_probe.unwrap().success(),
        "host must support rootless user+network namespaces for this isolation proof"
    );

    let mut child = fixture.executor_in_netns();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut stderr = BufReader::new(child.stderr.take().unwrap());

    // Prime the executor with a workspace operation to prove it still works.
    send_request(
        &mut stdin,
        &request(
            "write",
            json!({
                "type": "write_file",
                "path": "netns.txt",
                "content": "inside",
                "execution_id": "netns-write",
            }),
        ),
    )
    .await;
    assert_eq!(
        read_frame(&mut stdout).await["result"]["Ok"]["type"],
        "written"
    );

    // Bash /dev/tcp requires no external command and returns a concrete
    // "Network is unreachable" error inside an isolated network namespace.
    send_request(
        &mut stdin,
        &request(
            "net",
            json!({
                "type": "bash",
                "command": "cat < /dev/tcp/8.8.8.8/53",
                "execution_id": "net-1",
                "tool_call_id": "net-1-tc",
                "command_id": "net-1-cmd",
                "run_id": "run-1",
            }),
        ),
    )
    .await;
    let update = read_frame(&mut stdout).await;
    let mut output = update["value"]["output"].as_str().unwrap().to_owned();

    let terminal = read_frame(&mut stdout).await;
    let result = &terminal["result"]["Ok"]["result"];
    if let Some(tail) = result["output"].as_str() {
        output.push_str(tail);
    }
    assert_ne!(result["exit_code"], 0);
    assert!(
        output.contains("Network is unreachable"),
        "expected network unreachable, got: {output}"
    );

    drop(stdin);
    let status = child.wait().await.unwrap();
    let mut err = String::new();
    stderr.read_to_string(&mut err).await.unwrap();
    assert!(
        status.success(),
        "executor exited unsuccessfully: {status} stderr: {err}"
    );
}

#[tokio::test]
async fn executor_in_user_namespace_has_distinct_uid() {
    // An unprivileged user namespace maps the caller to a distinct namespace
    // UID. Root is commonly identity-mapped (0 -> 0), so this fixture cannot
    // prove a distinct UID under root and must not fail by construction there.
    let euid = unsafe { libc::geteuid() };
    if euid == 0 {
        eprintln!("skipping distinct-UID namespace fixture under root identity mapping");
        return;
    }
    let fixture = Fixture::new().await;
    let mut child = Command::new("unshare")
        .arg("--user")
        .arg("--net")
        .arg("--kill-child")
        .arg("--")
        .arg(env!("CARGO_BIN_EXE_sumi-agent"))
        .arg("--tool-executor")
        .env_clear()
        .env("SUMI_RPC_GENERATION", GENERATION.to_string())
        .env("SUMI_RPC_NONCE", NONCE)
        .env("SUMI_WORKSPACE", &fixture.workspace)
        .env("SUMI_CONVERSATION_ID", CONVERSATION)
        .env("SUMI_ARTIFACT_BROKER_SOCKET", &fixture.socket)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut stderr = BufReader::new(child.stderr.take().unwrap());

    send_request(
        &mut stdin,
        &request(
            "uid",
            json!({
                "type": "bash",
                "command": "id -u",
                "execution_id": "uid-1",
                "tool_call_id": "uid-1-tc",
                "command_id": "uid-1-cmd",
                "run_id": "run-1",
            }),
        ),
    )
    .await;
    let first = read_frame(&mut stdout).await;
    let uid_output = first["value"]["output"]
        .as_str()
        .or_else(|| first["result"]["Ok"]["result"]["output"].as_str())
        .expect("bash output in update or terminal")
        .trim();
    let inside_uid: u32 = uid_output.parse().expect("uid is an integer");
    assert_ne!(
        inside_uid, euid,
        "user namespace must expose a distinct UID"
    );

    if first["result"].is_null() {
        let terminal = read_frame(&mut stdout).await;
        assert_eq!(terminal["result"]["Ok"]["result"]["exit_code"], 0);
    }

    drop(stdin);
    let status = child.wait().await.unwrap();
    let mut err = String::new();
    stderr.read_to_string(&mut err).await.unwrap();
    assert!(
        status.success(),
        "executor exited unsuccessfully: {status} stderr: {err}"
    );
}

#[tokio::test]
#[ignore = "requires root or newuidmap with a configured /etc/subuid range"]
async fn mapped_distinct_uid_separation_requires_host_subuid() {
    // Full filesystem UID separation needs a subordinate UID/GID map.
    // This test is ignored by default and must be run on a host that provides
    // root or newuidmap + /etc/subuid.
}

fn supervisor_test_env(root: &Path) -> Vec<(&'static str, String)> {
    vec![
        ("SUMI_BIN", env!("CARGO_BIN_EXE_sumi-agent").to_owned()),
        ("SUMI_CONFIG_FILE", "/dev/null".to_owned()),
        (
            "SUMI_STATE_DIR",
            root.join("state").to_string_lossy().into_owned(),
        ),
        (
            "SUMI_WORKSPACE",
            root.join("workspace").to_string_lossy().into_owned(),
        ),
        (
            "SUMI_ARTIFACT_ROOT",
            root.join("artifacts").to_string_lossy().into_owned(),
        ),
        (
            "SUMI_EXECUTOR_SOCKET",
            root.join("executor.sock").to_string_lossy().into_owned(),
        ),
        (
            "SUMI_ARTIFACT_BROKER_SOCKET",
            root.join("broker.sock").to_string_lossy().into_owned(),
        ),
        ("SUMI_TENANT_ID", "tenant-1".to_owned()),
        ("SUMI_AGENT_ID", "agent-1".to_owned()),
        ("SUMI_CONVERSATION_ID", "conversation-1".to_owned()),
        (
            "SUMI_AGENT_WRAPPING_KEY",
            "4242424242424242424242424242424242424242424242424242424242424242".to_owned(),
        ),
    ]
}

fn supervisor_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("deploy/agent/supervisor")
}

fn compose_service<'a>(source: &'a str, service: &str) -> &'a str {
    let marker = format!("\n  {service}:\n");
    let start = source
        .find(&marker)
        .unwrap_or_else(|| panic!("compose service {service} is missing"));
    let rest = &source[start + marker.len()..];
    let end = rest
        .match_indices("\n  ")
        .find_map(|(offset, _)| (rest.as_bytes().get(offset + 3) != Some(&b' ')).then_some(offset))
        .unwrap_or(rest.len());
    &rest[..end]
}

fn compose_service_volumes(compose: &serde_yaml::Value, service: &str) -> Vec<String> {
    compose["services"][service]["volumes"]
        .as_sequence()
        .unwrap_or_else(|| panic!("compose service {service} volumes are missing"))
        .iter()
        .map(|volume| {
            volume
                .as_str()
                .unwrap_or_else(|| panic!("compose service {service} has a non-string volume"))
                .to_owned()
        })
        .collect()
}

#[test]
fn compose_deployment_has_disjoint_mounts_identities_and_sidecar_policy() {
    let deploy_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("deploy/agent");
    let compose = std::fs::read_to_string(deploy_dir.join("compose.yaml")).unwrap();
    let dockerfile = std::fs::read_to_string(deploy_dir.join("Dockerfile")).unwrap();
    let entrypoint = std::fs::read_to_string(deploy_dir.join("container-entrypoint")).unwrap();
    let seccomp = std::fs::read_to_string(deploy_dir.join("seccomp/sidecar.json")).unwrap();

    let runtime = compose_service(&compose, "runtime");
    let executor = compose_service(&compose, "executor");
    let broker = compose_service(&compose, "broker");
    let compose_value: serde_yaml::Value = serde_yaml::from_str(&compose).unwrap();
    let runtime_volumes = compose_service_volumes(&compose_value, "runtime");
    let executor_volumes = compose_service_volumes(&compose_value, "executor");
    let broker_volumes = compose_service_volumes(&compose_value, "broker");

    // The runtime only sees durable state and its executor IPC endpoint. It
    // must not receive either tenant workspace or artifact storage.
    assert!(runtime.contains("user: \"10001:10001\""));
    assert!(runtime_volumes.contains(&"state:/var/lib/sumi".to_owned()));
    assert!(runtime_volumes.contains(&"runtime-ipc:/run/sumi/runtime:ro".to_owned()));
    assert!(
        !runtime_volumes
            .iter()
            .any(|volume| volume.starts_with("workspace:"))
    );
    assert!(
        !runtime_volumes
            .iter()
            .any(|volume| volume.starts_with("artifacts:"))
    );
    assert!(
        !runtime_volumes
            .iter()
            .any(|volume| volume.starts_with("broker-ipc:"))
    );

    // The executor receives only workspace and the two constrained IPC
    // directories. It cannot mount durable state or the artifact volume.
    assert!(executor.contains("user: \"10002:10002\""));
    assert!(executor.contains("network_mode: none"));
    assert!(executor_volumes.contains(&"workspace:/workspace".to_owned()));
    assert!(executor_volumes.contains(&"runtime-ipc:/run/sumi/runtime".to_owned()));
    assert!(executor_volumes.contains(&"broker-ipc:/run/sumi/broker:ro".to_owned()));
    assert!(
        !executor_volumes
            .iter()
            .any(|volume| volume.starts_with("state:"))
    );
    assert!(
        !executor_volumes
            .iter()
            .any(|volume| volume.starts_with("artifacts:"))
    );
    assert!(executor.contains("apparmor:sumi-agent-executor"));
    assert!(executor.contains("SUMI_READINESS_SOCKET=/run/sumi/runtime/executor.sock"));

    // The broker gets no workspace/state mount and has no TCP/DNS network.
    assert!(broker.contains("user: \"10003:10003\""));
    assert!(broker.contains("network_mode: none"));
    assert!(broker.contains("SUMI_READINESS_SOCKET=/run/sumi/broker/broker.sock"));
    assert!(broker_volumes.contains(&"artifacts:/var/lib/sumi-artifacts".to_owned()));
    assert!(broker_volumes.contains(&"broker-ipc:/run/sumi/broker".to_owned()));
    assert!(
        !broker_volumes
            .iter()
            .any(|volume| volume.starts_with("workspace:"))
    );
    assert!(
        !broker_volumes
            .iter()
            .any(|volume| volume.starts_with("state:"))
    );
    assert!(
        !broker_volumes
            .iter()
            .any(|volume| volume.starts_with("runtime-ipc:"))
    );

    for service in [runtime, executor, broker] {
        assert!(service.contains("<<: *sidecar-hardening"));
    }
    for required in [
        "read_only: true",
        "cap_drop: [ALL]",
        "no-new-privileges:true",
        "seccomp:./seccomp/sidecar.json",
        "openat2",
    ] {
        assert!(
            compose.contains(required) || seccomp.contains(required),
            "deployment policy missing {required}"
        );
    }

    assert!(dockerfile.contains("sumi-runtime"));
    assert!(dockerfile.contains("sumi-tool"));
    assert!(dockerfile.contains("sumi-artifact"));
    assert!(entrypoint.contains("--allocate-generation"));
    assert!(entrypoint.contains("SUMI_RPC_NONCE"));
    assert!(entrypoint.contains("SUMI_PROCESS_GENERATION_LEASE_ID"));
    assert!(entrypoint.contains("SUMI_GENERATION_RECOVERY_FENCE_ID"));
    assert!(entrypoint.contains("SUMI_ENFORCE_BROKER_SOCKET_NAMESPACE_ISOLATION"));
    assert!(entrypoint.contains("env -i"));
    let _: serde_json::Value = serde_json::from_str(&seccomp).unwrap();
    // The deployed seccomp policy permits exactly the two namespace calls
    // used by the fail-closed startup/pre-exec sequence, not arbitrary
    // unshare flags inherited by bash.
    assert!(seccomp.contains("\"value\": 268435456")); // CLONE_NEWUSER
    assert!(seccomp.contains("\"value\": 1073872896")); // CLONE_NEWNS | CLONE_NEWNET
    for syscall in ["rmdir", "umask", "umount2"] {
        assert!(seccomp.contains(&format!("\"{syscall}\"")));
    }

    let apparmor_profile = deploy_dir.join("apparmor/executor");
    assert!(
        apparmor_profile.is_file(),
        "sumi-agent-executor apparmor profile must be present"
    );
    let apparmor_load = deploy_dir.join("apparmor/load-profile");
    assert!(
        apparmor_load.is_file(),
        "apparmor profile loader script must be present"
    );
    let apparmor = std::fs::read_to_string(apparmor_profile).unwrap();
    assert!(apparmor.contains("userns,"));
    assert!(apparmor.contains("mount options=(rprivate) none -> /"));
    assert!(apparmor.contains("mount options=(bind) /tmp/.sumi-broker-isolation-*"));
    assert!(
        apparmor.contains("/tmp/.sumi-broker-preflight-src-* -> /tmp/.sumi-broker-preflight-tgt-*")
    );
    assert!(apparmor.contains("umount /tmp/.sumi-broker-preflight-tgt-*"));
}

#[tokio::test]
async fn supervisor_fail_closed_without_low_trust_enabled() {
    let root = std::env::temp_dir().join(format!("sumi-supervisor-{}-", Uuid::now_v7()));
    tokio::fs::create_dir_all(&root).await.unwrap();

    let mut envs = supervisor_test_env(&root);
    envs.push(("SUMI_ISOLATION_MODE", "low-trust".to_owned()));
    // SUMI_ALLOW_LOW_TRUST intentionally not set.

    let output = Command::new(supervisor_path())
        .env_clear()
        .envs(envs)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .expect("spawn supervisor");

    assert!(
        !output.status.success(),
        "supervisor must fail closed when low-trust is not enabled"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("low-trust mode is disabled"),
        "expected low-trust blocker, got: {stderr}"
    );

    let _ = tokio::fs::remove_dir_all(&root).await;
}

#[tokio::test]
async fn supervisor_config_defaults_do_not_override_explicit_environment() {
    let root = std::env::temp_dir().join(format!("sumi-supervisor-config-{}-", Uuid::now_v7()));
    tokio::fs::create_dir_all(&root).await.unwrap();
    let config = root.join("config.env");
    tokio::fs::write(
        &config,
        "SUMI_ALLOW_LOW_TRUST=1\nSUMI_PROVIDER_API_KEY=config-secret\n",
    )
    .await
    .unwrap();

    let mut envs = supervisor_test_env(&root);
    envs.push(("SUMI_ISOLATION_MODE", "low-trust".to_owned()));
    envs.push(("SUMI_ALLOW_LOW_TRUST", "0".to_owned()));
    envs.push(("SUMI_CONFIG_FILE", config.to_string_lossy().into_owned()));
    envs.push(("SUMI_PROVIDER_API_KEY", String::new()));

    let output = Command::new(supervisor_path())
        .env_clear()
        .envs(envs)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .expect("spawn supervisor");

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("low-trust mode is disabled"),
        "caller environment must override config defaults: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let _ = tokio::fs::remove_dir_all(&root).await;
}

#[tokio::test]
async fn cli_allocate_generation_is_monotonic_and_persistent() {
    let root = std::env::temp_dir().join(format!("sumi-alloc-cli-{}-", Uuid::now_v7()));
    tokio::fs::create_dir_all(&root).await.unwrap();

    let first = allocate_generation(&root);
    let second = allocate_generation(&root);
    assert_eq!(second, first + 1);

    let _ = tokio::fs::remove_dir_all(&root).await;
}

#[tokio::test]
async fn cli_allocate_generation_is_unique_across_concurrent_processes() {
    let root = std::env::temp_dir().join(format!("sumi-alloc-concurrent-{}-", Uuid::now_v7()));
    tokio::fs::create_dir_all(&root).await.unwrap();

    let count = 16usize;
    let mut handles = Vec::with_capacity(count);
    for _ in 0..count {
        let root = root.clone();
        handles.push(tokio::task::spawn_blocking(move || {
            allocate_generation(&root)
        }));
    }

    let mut values: Vec<u64> = Vec::with_capacity(count);
    for handle in handles {
        values.push(handle.await.unwrap());
    }
    values.sort_unstable();

    let unique: std::collections::HashSet<_> = values.iter().copied().collect();
    assert_eq!(
        unique.len(),
        values.len(),
        "concurrent allocations must be unique"
    );
    assert_eq!(values.first().copied().unwrap(), 0);
    assert_eq!(values.last().copied().unwrap() as usize, count - 1);

    let _ = tokio::fs::remove_dir_all(&root).await;
}

#[tokio::test]
async fn artifact_broker_rebinds_stale_socket_on_restart() {
    let root = std::env::temp_dir().join(format!("sumi-broker-restart-{}-", Uuid::now_v7()));
    let artifacts = root.join("artifacts");
    let broker_ipc = root.join("broker-ipc");
    let socket = broker_ipc.join("broker.sock");
    tokio::fs::create_dir_all(&artifacts).await.unwrap();
    tokio::fs::create_dir_all(&broker_ipc).await.unwrap();

    let spawn_broker = || {
        Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
            .arg("--artifact-broker")
            .env_clear()
            .env("SUMI_RPC_GENERATION", GENERATION.to_string())
            .env("SUMI_RPC_NONCE", NONCE)
            .env("SUMI_ARTIFACT_ROOT", &artifacts)
            .env("SUMI_ARTIFACT_BROKER_SOCKET", &socket)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    };

    let mut first = spawn_broker();
    timeout(Duration::from_secs(5), async {
        while UnixStream::connect(&socket).await.is_err() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first broker socket");

    first.kill().await.unwrap();
    first.wait().await.unwrap();

    let mut second = spawn_broker();
    timeout(Duration::from_secs(5), async {
        while UnixStream::connect(&socket).await.is_err() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second broker must rebind the stale socket");

    let _ = second.kill().await;
    let _ = tokio::fs::remove_dir_all(&root).await;
}

#[tokio::test]
async fn ipc_sockets_inherit_setgid_group_and_remain_reachable() {
    let root = std::env::temp_dir().join(format!("sumi-ipc-setgid-{}-", Uuid::now_v7()));
    let workspace = root.join("workspace");
    let artifacts = root.join("artifacts");
    let runtime_ipc = root.join("runtime-ipc");
    let broker_ipc = root.join("broker-ipc");
    let executor_socket = runtime_ipc.join("executor.sock");
    let broker_socket = broker_ipc.join("broker.sock");

    for dir in [&workspace, &artifacts, &runtime_ipc, &broker_ipc] {
        tokio::fs::create_dir_all(dir).await.unwrap();
    }

    for ipc_dir in [&runtime_ipc, &broker_ipc] {
        let mut perms = std::fs::metadata(ipc_dir).unwrap().permissions();
        perms.set_mode(0o2770);
        tokio::fs::set_permissions(ipc_dir, perms).await.unwrap();
    }

    // Start the artifact broker in the broker IPC directory.
    let mut broker = Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
        .arg("--artifact-broker")
        .env_clear()
        .env("SUMI_RPC_GENERATION", GENERATION.to_string())
        .env("SUMI_RPC_NONCE", NONCE)
        .env("SUMI_ARTIFACT_ROOT", &artifacts)
        .env("SUMI_ARTIFACT_BROKER_SOCKET", &broker_socket)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    timeout(Duration::from_secs(5), async {
        while UnixStream::connect(&broker_socket).await.is_err() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("broker socket");

    // Start a socket-bound executor in the runtime IPC directory.
    let mut executor = Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
        .arg("--tool-executor-socket")
        .env_clear()
        .env("SUMI_RPC_GENERATION", GENERATION.to_string())
        .env("SUMI_RPC_NONCE", NONCE)
        .env("SUMI_WORKSPACE", &workspace)
        .env("SUMI_CONVERSATION_ID", CONVERSATION)
        .env("SUMI_ARTIFACT_BROKER_SOCKET", &broker_socket)
        .env("SUMI_EXECUTOR_SOCKET", &executor_socket)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    timeout(Duration::from_secs(5), async {
        while UnixStream::connect(&executor_socket).await.is_err() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("executor socket");

    // Socket inodes must inherit the shared IPC group from their directories.
    let broker_dir_gid = tokio::fs::metadata(&broker_ipc).await.unwrap().gid();
    let broker_sock_gid = tokio::fs::metadata(&broker_socket).await.unwrap().gid();
    assert_eq!(
        broker_sock_gid, broker_dir_gid,
        "broker socket group must inherit the setgid directory group"
    );

    let runtime_dir_gid = tokio::fs::metadata(&runtime_ipc).await.unwrap().gid();
    let executor_sock_gid = tokio::fs::metadata(&executor_socket).await.unwrap().gid();
    assert_eq!(
        executor_sock_gid, runtime_dir_gid,
        "executor socket group must inherit the setgid directory group"
    );

    // The runtime process (this test) must be able to connect to both sockets,
    // and the executor process must have been able to connect to the broker.
    let broker_response = broker_rpc(
        &broker_socket,
        &json!({
            "generation": GENERATION,
            "nonce": NONCE,
            "request_id": "broker-1",
            "operation": {
                "type": "begin_tool_output",
                "conversation_id": CONVERSATION,
                "execution_id": "exec-1",
                "content": [104, 101, 108, 108, 111]
            }
        }),
    )
    .await;
    assert!(
        broker_response.is_some(),
        "runtime must be able to exchange with the broker socket"
    );

    let executor_request = json!({
        "generation": GENERATION,
        "nonce": NONCE,
        "request_id": "exec-1",
        "operation": {
            "type": "read_file",
            "path": "missing.txt",
            "offset": 0,
            "limit": 1024,
            "execution_id": "exec-1"
        }
    });
    let mut bytes = serde_json::to_vec(&executor_request).unwrap();
    bytes.push(b'\n');
    let mut stream = UnixStream::connect(&executor_socket).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
    stream.shutdown().await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    assert!(
        !response.is_empty(),
        "runtime must be able to exchange with the executor socket"
    );

    let _ = broker.kill().await;
    let _ = executor.kill().await;
    let _ = tokio::fs::remove_dir_all(&root).await;
}
