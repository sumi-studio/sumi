#![cfg(target_os = "linux")]

use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    process::{Child, Command},
    time::timeout,
};
use uuid::Uuid;

const GENERATION: u64 = 29;
const NONCE: &str = "executor-manager-boot-a";
const RESTARTED_NONCE: &str = "executor-manager-boot-b";
const PERSONALITY_AGENT_ID: &str = "018f8a9e-65c0-7a5b-8d3c-1f2a3b4c5d6e";
const OTHER_PERSONALITY_AGENT_ID: &str = "018f8a9e-65c0-7a5b-8d3c-1f2a3b4c5d6f";

struct Fixture {
    root: PathBuf,
    workspace: PathBuf,
    broker_socket: PathBuf,
    executor_socket: PathBuf,
    broker: Child,
    executor: Option<Child>,
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("sumi-executor-manager-{}", Uuid::now_v7()));
        let workspace = root.join("workspace");
        let artifacts = root.join("artifacts");
        let broker_socket = root.join("broker.sock");
        let executor_socket = root.join("executor.sock");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&artifacts).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let broker = spawn_broker(&artifacts, &broker_socket, NONCE);
        wait_for_socket(&broker_socket).await;
        Self {
            root,
            workspace,
            broker_socket,
            executor_socket,
            broker,
            executor: None,
        }
    }

    async fn start_executor(&mut self, nonce: &str) {
        assert!(self.executor.is_none());
        self.executor = Some(spawn_executor(
            &self.workspace,
            &self.broker_socket,
            &self.executor_socket,
            nonce,
        ));
        wait_for_socket(&self.executor_socket).await;
    }

    async fn restart_executor(&mut self, nonce: &str) {
        let mut executor = self.executor.take().expect("executor is running");
        executor.kill().await.expect("kill executor");
        executor.wait().await.expect("wait executor");
        self.start_executor(nonce).await;
    }

    async fn restart_broker(&mut self, nonce: &str) {
        self.broker.kill().await.expect("kill broker");
        self.broker.wait().await.expect("wait broker");
        self.broker = spawn_broker(&self.root.join("artifacts"), &self.broker_socket, nonce);
        wait_for_socket(&self.broker_socket).await;
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(executor) = &mut self.executor {
            let _ = executor.start_kill();
        }
        let _ = self.broker.start_kill();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn spawn_broker(artifacts: &Path, socket: &Path, nonce: &str) -> Child {
    Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
        .arg("--artifact-broker")
        .env_clear()
        .env("SUMI_PERSONALITY_AGENT_ID", PERSONALITY_AGENT_ID)
        .env("SUMI_RPC_GENERATION", GENERATION.to_string())
        .env("SUMI_RPC_NONCE", nonce)
        .env("SUMI_ARTIFACT_ROOT", artifacts)
        .env("SUMI_ARTIFACT_BROKER_SOCKET", socket)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn artifact broker")
}

fn spawn_executor(
    workspace: &Path,
    broker_socket: &Path,
    executor_socket: &Path,
    nonce: &str,
) -> Child {
    Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
        .arg("--tool-executor-socket")
        .env_clear()
        .env("SUMI_PERSONALITY_AGENT_ID", PERSONALITY_AGENT_ID)
        .env("SUMI_RPC_GENERATION", GENERATION.to_string())
        .env("SUMI_RPC_NONCE", nonce)
        .env("SUMI_WORKSPACE", workspace)
        .env("SUMI_ARTIFACT_BROKER_SOCKET", broker_socket)
        .env("SUMI_EXECUTOR_SOCKET", executor_socket)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn socket executor")
}

async fn wait_for_socket(socket: &Path) {
    timeout(Duration::from_secs(5), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("socket did not become accepting");
}

fn request(nonce: &str, request_id: &str, operation: Value) -> Value {
    json!({
        "personality_agent_id": PERSONALITY_AGENT_ID,
        "generation": GENERATION,
        "nonce": nonce,
        "request_id": request_id,
        "operation": operation,
    })
}

fn read_file_request(nonce: &str, request_id: &str, execution_id: &str, path: &str) -> Value {
    request(
        nonce,
        request_id,
        json!({
            "type": "read_file",
            "path": path,
            "offset": 0,
            "limit": 1024,
            "execution_id": execution_id,
        }),
    )
}

fn health_request(nonce: &str, request_id: &str) -> Value {
    request(nonce, request_id, json!({"type": "health"}))
}

async fn write_request(stream: &mut UnixStream, value: &Value) {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    stream.write_all(&bytes).await.expect("write request");
}

async fn read_connection(mut stream: UnixStream) -> Vec<Value> {
    stream.shutdown().await.expect("shutdown request half");
    let mut lines = BufReader::new(stream).lines();
    let mut frames = Vec::new();
    timeout(Duration::from_secs(5), async {
        while let Some(line) = lines.next_line().await.expect("read executor frame") {
            frames.push(serde_json::from_str(&line).expect("decode executor frame"));
        }
    })
    .await
    .expect("executor connection did not close");
    frames
}

async fn exchange(socket: &Path, value: &Value) -> Vec<Value> {
    let mut stream = UnixStream::connect(socket).await.expect("connect executor");
    write_request(&mut stream, value).await;
    read_connection(stream).await
}

async fn broker_exchange(socket: &Path, value: &Value) -> Value {
    let frames = exchange(socket, value).await;
    assert_eq!(frames.len(), 1, "broker emitted unexpected frames");
    frames.into_iter().next().unwrap()
}

async fn start_running_bashes(
    fixture: &Fixture,
    label: &str,
    count: usize,
) -> (Vec<UnixStream>, Vec<i32>) {
    let mut streams = Vec::with_capacity(count);
    let mut pids = Vec::with_capacity(count);
    for index in 0..count {
        let pid_name = format!("{label}-{index}.pid");
        let mut stream = UnixStream::connect(&fixture.executor_socket)
            .await
            .expect("connect running bash");
        write_request(
            &mut stream,
            &request(
                NONCE,
                &format!("request-{label}-{index}"),
                json!({
                    "type": "bash",
                    "command": format!("printf '%s' \"$$\" > {pid_name}; sleep 60"),
                    "execution_id": format!("execution-{label}-{index}"),
                }),
            ),
        )
        .await;
        pids.push(wait_for_pid(&fixture.workspace.join(pid_name)).await);
        streams.push(stream);
    }
    (streams, pids)
}

async fn cancel_until_accepted(
    socket: &Path,
    execution_id: &str,
    request_prefix: &str,
) -> Vec<Value> {
    timeout(Duration::from_secs(5), async {
        for attempt in 0u64.. {
            let frames = exchange(
                socket,
                &request(
                    NONCE,
                    &format!("{request_prefix}-{attempt}"),
                    json!({
                        "type": "cancel",
                        "execution_id": execution_id,
                    }),
                ),
            )
            .await;
            if frames.iter().any(|frame| {
                frame["result"]["Ok"]["type"] == Value::String("cancel_accepted".to_owned())
            }) {
                return frames;
            }
            assert!(
                frames
                    .iter()
                    .any(|frame| frame["result"]["Err"]["code"] == "protocol"),
                "unexpected pre-registration cancel response: {frames:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        unreachable!()
    })
    .await
    .expect("execution was never registered for cancellation")
}

fn terminal_content(frames: &[Value]) -> &str {
    frames
        .iter()
        .find(|frame| frame["type"] == "terminal")
        .and_then(|frame| frame["result"]["Ok"]["result"]["content"].as_str())
        .expect("read_file terminal content")
}

async fn wait_for_pid(path: &Path) -> i32 {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(value) = std::fs::read_to_string(path)
                && let Ok(pid) = value.trim().parse::<i32>()
            {
                return pid;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("bash pid was not published")
}

async fn wait_for_process_group_gone(pid: i32) {
    timeout(Duration::from_secs(5), async {
        loop {
            let result = unsafe { libc::kill(-pid, 0) };
            if result != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("bash process group was not reaped");
}

#[tokio::test]
async fn production_manager_shares_registry_and_runs_concurrent_read_file() {
    let mut fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.join("alpha.txt"), "alpha").unwrap();
    std::fs::write(fixture.workspace.join("beta.txt"), "beta").unwrap();
    fixture.start_executor(NONCE).await;

    let alpha = read_file_request(NONCE, "request-alpha", "execution-alpha", "alpha.txt");
    let beta = read_file_request(NONCE, "request-beta", "execution-beta", "beta.txt");
    let (alpha, beta) = tokio::join!(
        exchange(&fixture.executor_socket, &alpha),
        exchange(&fixture.executor_socket, &beta),
    );
    assert_eq!(terminal_content(&alpha), "alpha");
    assert_eq!(terminal_content(&beta), "beta");

    // A second connection cannot reuse an execution or request identity that
    // completed on the first connection.
    let duplicate_execution =
        read_file_request(NONCE, "request-new", "execution-alpha", "alpha.txt");
    assert!(
        exchange(&fixture.executor_socket, &duplicate_execution)
            .await
            .is_empty()
    );
    let duplicate_request = read_file_request(NONCE, "request-beta", "execution-new", "alpha.txt");
    assert!(
        exchange(&fixture.executor_socket, &duplicate_request)
            .await
            .is_empty()
    );

    for stale in [
        {
            let mut value =
                read_file_request("wrong-nonce", "stale-nonce", "stale-nonce", "alpha.txt");
            value["nonce"] = json!("wrong-nonce");
            value
        },
        {
            let mut value =
                read_file_request(NONCE, "stale-generation", "stale-generation", "alpha.txt");
            value["generation"] = json!(GENERATION - 1);
            value
        },
        {
            let mut value = read_file_request(NONCE, "stale-owner", "stale-owner", "alpha.txt");
            value["personality_agent_id"] = json!(OTHER_PERSONALITY_AGENT_ID);
            value
        },
    ] {
        assert!(exchange(&fixture.executor_socket, &stale).await.is_empty());
    }
}

#[tokio::test]
async fn manager_restart_rotates_nonce_and_rebinds_stale_socket() {
    let mut fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.join("source.txt"), "source").unwrap();
    fixture.start_executor(NONCE).await;
    assert_eq!(
        std::fs::metadata(&fixture.executor_socket)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o660
    );
    let first = read_file_request(NONCE, "request-first", "execution-first", "source.txt");
    assert_eq!(
        terminal_content(&exchange(&fixture.executor_socket, &first).await),
        "source"
    );

    let mut competing = spawn_executor(
        &fixture.workspace,
        &fixture.broker_socket,
        &fixture.executor_socket,
        NONCE,
    );
    let status = timeout(Duration::from_secs(5), competing.wait())
        .await
        .expect("competing manager did not fail closed")
        .unwrap();
    assert!(!status.success(), "a second manager stole the live socket");
    let still_owned = read_file_request(
        NONCE,
        "request-still-owned",
        "execution-still-owned",
        "source.txt",
    );
    assert_eq!(
        terminal_content(&exchange(&fixture.executor_socket, &still_owned).await),
        "source"
    );

    fixture.restart_executor(RESTARTED_NONCE).await;
    let stale = read_file_request(NONCE, "request-stale", "execution-stale", "source.txt");
    assert!(exchange(&fixture.executor_socket, &stale).await.is_empty());
    let fresh = read_file_request(
        RESTARTED_NONCE,
        "request-fresh",
        "execution-fresh",
        "source.txt",
    );
    assert_eq!(
        terminal_content(&exchange(&fixture.executor_socket, &fresh).await),
        "source"
    );
}

#[tokio::test]
async fn broker_crash_restart_rebinds_its_stale_socket() {
    let mut fixture = Fixture::new().await;
    let first = broker_exchange(
        &fixture.broker_socket,
        &request(
            NONCE,
            "broker-before-crash",
            json!({
                "type": "begin_tool_output",
                "execution_id": "broker-before-crash",
                "content": [97],
            }),
        ),
    )
    .await;
    assert_eq!(first["result"]["Ok"]["offset"], 1);

    fixture.restart_broker(NONCE).await;
    let restarted = broker_exchange(
        &fixture.broker_socket,
        &request(
            NONCE,
            "broker-after-crash",
            json!({
                "type": "begin_tool_output",
                "execution_id": "broker-after-crash",
                "content": [98],
            }),
        ),
    )
    .await;
    assert_eq!(restarted["result"]["Ok"]["offset"], 1);
}

#[tokio::test]
async fn health_is_authenticated_single_exchange_and_does_not_consume_uniqueness() {
    let mut fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.join("source.txt"), "healthy").unwrap();
    fixture.start_executor(NONCE).await;

    let health = health_request(NONCE, "reused-health-request");
    for _ in 0..16 {
        let frames = exchange(&fixture.executor_socket, &health).await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0]["result"]["Ok"]["type"], "healthy");
    }

    // Health bypasses the manager uniqueness ledger, so its request identity
    // remains available to an ordinary execution.
    let ordinary = read_file_request(
        NONCE,
        "reused-health-request",
        "execution-after-health",
        "source.txt",
    );
    assert_eq!(
        terminal_content(&exchange(&fixture.executor_socket, &ordinary).await),
        "healthy"
    );

    // A socket session is closed after its one primary Health exchange. A
    // buffered second request must not execute.
    let mut stream = UnixStream::connect(&fixture.executor_socket).await.unwrap();
    write_request(
        &mut stream,
        &health_request(NONCE, "health-close-after-one"),
    )
    .await;
    write_request(
        &mut stream,
        &request(
            NONCE,
            "health-buffered-write",
            json!({
                "type": "write_file",
                "path": "must-not-exist.txt",
                "content": "unexpected",
                "execution_id": "health-buffered-write",
            }),
        ),
    )
    .await;
    let frames = read_connection(stream).await;
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0]["result"]["Ok"]["type"], "healthy");
    assert!(!fixture.workspace.join("must-not-exist.txt").exists());

    let mut wrong_identity = health_request("wrong-nonce", "wrong-health");
    wrong_identity["nonce"] = json!("wrong-nonce");
    assert!(
        exchange(&fixture.executor_socket, &wrong_identity)
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn concurrent_stale_socket_starters_leave_one_live_owner() {
    let mut fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.join("source.txt"), "owned").unwrap();
    let stale = std::os::unix::net::UnixListener::bind(&fixture.executor_socket).unwrap();
    drop(stale);

    let mut first = spawn_executor(
        &fixture.workspace,
        &fixture.broker_socket,
        &fixture.executor_socket,
        NONCE,
    );
    let mut second = spawn_executor(
        &fixture.workspace,
        &fixture.broker_socket,
        &fixture.executor_socket,
        NONCE,
    );
    wait_for_socket(&fixture.executor_socket).await;

    let first_lost = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = first.try_wait().unwrap() {
                assert!(!status.success());
                return true;
            }
            if let Some(status) = second.try_wait().unwrap() {
                assert!(!status.success());
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("one concurrent starter did not fail closed");

    if first_lost {
        assert!(second.try_wait().unwrap().is_none());
        fixture.executor = Some(second);
    } else {
        assert!(first.try_wait().unwrap().is_none());
        fixture.executor = Some(first);
    }
    let request = read_file_request(
        NONCE,
        "request-concurrent-owner",
        "execution-concurrent-owner",
        "source.txt",
    );
    assert_eq!(
        terminal_content(&exchange(&fixture.executor_socket, &request).await),
        "owned"
    );
    let lock_path = fixture.root.join("executor.sock.lock");
    let lock_metadata = std::fs::symlink_metadata(lock_path).unwrap();
    assert!(lock_metadata.file_type().is_file());
    assert_eq!(lock_metadata.permissions().mode() & 0o777, 0o600);
}

#[tokio::test]
async fn disconnect_and_explicit_cancel_reap_children_without_poisoning_manager() {
    let mut fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.join("source.txt"), "still-live").unwrap();
    fixture.start_executor(NONCE).await;

    let disconnect_pid_file = fixture.workspace.join("disconnect.pid");
    let mut disconnected = UnixStream::connect(&fixture.executor_socket).await.unwrap();
    let disconnect = request(
        NONCE,
        "request-disconnect",
        json!({
            "type": "bash",
            "command": "printf '%s' \"$$\" > disconnect.pid; sleep 60",
            "execution_id": "execution-disconnect",
        }),
    );
    let mut bytes = serde_json::to_vec(&disconnect).unwrap();
    bytes.push(b'\n');
    disconnected.write_all(&bytes).await.unwrap();
    let disconnect_pid = wait_for_pid(&disconnect_pid_file).await;
    drop(disconnected);
    wait_for_process_group_gone(disconnect_pid).await;

    let cancel_pid_file = fixture.workspace.join("cancel.pid");
    let mut cancelled = UnixStream::connect(&fixture.executor_socket).await.unwrap();
    let start = request(
        NONCE,
        "request-bash",
        json!({
            "type": "bash",
            "command": "printf '%s' \"$$\" > cancel.pid; sleep 60",
            "execution_id": "execution-bash",
        }),
    );
    let mut bytes = serde_json::to_vec(&start).unwrap();
    bytes.push(b'\n');
    cancelled.write_all(&bytes).await.unwrap();
    let cancel_pid = wait_for_pid(&cancel_pid_file).await;
    let cancel = request(
        NONCE,
        "request-cancel",
        json!({
            "type": "cancel",
            "execution_id": "execution-bash",
        }),
    );
    let cancel_frames = exchange(&fixture.executor_socket, &cancel).await;
    cancelled.shutdown().await.unwrap();

    let mut original_frames = Vec::new();
    let mut lines = BufReader::new(cancelled).lines();
    timeout(Duration::from_secs(5), async {
        while let Some(line) = lines.next_line().await.expect("read bash frame") {
            let frame: Value = serde_json::from_str(&line).unwrap();
            if frame["type"] == "terminal" {
                original_frames.push(frame);
            }
        }
    })
    .await
    .expect("cancel settlement timed out");
    wait_for_process_group_gone(cancel_pid).await;
    assert!(cancel_frames.iter().any(|frame| {
        frame["request_id"] == "request-cancel"
            && frame["result"]["Ok"]["type"] == "cancel_accepted"
    }));
    assert!(original_frames.iter().any(|frame| {
        frame["request_id"] == "request-bash"
            && frame["result"]["Ok"]["result"]["cancelled"] == true
    }));

    let read = read_file_request(
        NONCE,
        "request-after-reap",
        "execution-after-reap",
        "source.txt",
    );
    assert_eq!(
        terminal_content(&exchange(&fixture.executor_socket, &read).await),
        "still-live"
    );
}

#[tokio::test]
async fn queued_bash_same_session_cancel_never_spawns_the_command() {
    let mut fixture = Fixture::new().await;
    fixture.start_executor(NONCE).await;
    let (running, pids) = start_running_bashes(&fixture, "same-session-running", 8).await;

    let marker = fixture.workspace.join("same-session-queued.started");
    let mut queued = UnixStream::connect(&fixture.executor_socket).await.unwrap();
    write_request(
        &mut queued,
        &request(
            NONCE,
            "request-same-session-queued",
            json!({
                "type": "bash",
                "command": "touch same-session-queued.started; sleep 60",
                "execution_id": "execution-same-session-queued",
            }),
        ),
    )
    .await;
    write_request(
        &mut queued,
        &request(
            NONCE,
            "request-same-session-cancel",
            json!({
                "type": "cancel",
                "execution_id": "execution-same-session-queued",
            }),
        ),
    )
    .await;
    let frames = read_connection(queued).await;
    assert!(frames.iter().any(|frame| {
        frame["request_id"] == "request-same-session-cancel"
            && frame["result"]["Ok"]["type"] == "cancel_accepted"
    }));
    assert!(frames.iter().any(|frame| {
        frame["request_id"] == "request-same-session-queued"
            && frame["result"]["Ok"]["result"]["cancelled"] == true
    }));
    assert!(
        !marker.exists(),
        "queued command spawned before cancellation"
    );

    drop(running);
    for pid in pids {
        wait_for_process_group_gone(pid).await;
    }
}

#[tokio::test]
async fn queued_bash_cross_connection_cancel_never_spawns_the_command() {
    let mut fixture = Fixture::new().await;
    fixture.start_executor(NONCE).await;
    let (running, pids) = start_running_bashes(&fixture, "cross-session-running", 8).await;

    let marker = fixture.workspace.join("cross-session-queued.started");
    let mut queued = UnixStream::connect(&fixture.executor_socket).await.unwrap();
    write_request(
        &mut queued,
        &request(
            NONCE,
            "request-cross-session-queued",
            json!({
                "type": "bash",
                "command": "touch cross-session-queued.started; sleep 60",
                "execution_id": "execution-cross-session-queued",
            }),
        ),
    )
    .await;
    let cancel = cancel_until_accepted(
        &fixture.executor_socket,
        "execution-cross-session-queued",
        "request-cross-session-cancel",
    )
    .await;
    assert!(
        cancel
            .iter()
            .any(|frame| { frame["result"]["Ok"]["type"] == "cancel_accepted" })
    );
    let frames = read_connection(queued).await;
    assert!(frames.iter().any(|frame| {
        frame["request_id"] == "request-cross-session-queued"
            && frame["result"]["Ok"]["result"]["cancelled"] == true
    }));
    assert!(
        !marker.exists(),
        "queued command spawned before cancellation"
    );

    drop(running);
    for pid in pids {
        wait_for_process_group_gone(pid).await;
    }
}

#[tokio::test]
async fn independent_cancel_bypasses_a_full_ordinary_connection_pool() {
    let mut fixture = Fixture::new().await;
    fixture.start_executor(NONCE).await;
    let (running, pids) = start_running_bashes(&fixture, "pool-running", 8).await;

    let mut queued = Vec::new();
    for index in 0..24 {
        let mut stream = UnixStream::connect(&fixture.executor_socket).await.unwrap();
        write_request(
            &mut stream,
            &request(
                NONCE,
                &format!("request-pool-queued-{index}"),
                json!({
                    "type": "bash",
                    "command": format!(
                        "printf '%s' \"$$\" > pool-queued-{index}.pid; sleep 60"
                    ),
                    "execution_id": format!("execution-pool-queued-{index}"),
                }),
            ),
        )
        .await;
        queued.push(stream);
    }
    // Give all local sessions time to cross the initial-frame tier and occupy
    // the 32-slot ordinary pool (8 admitted operations plus 24 pending).
    tokio::time::sleep(Duration::from_millis(250)).await;
    for index in 0..24 {
        assert!(
            !fixture
                .workspace
                .join(format!("pool-queued-{index}.pid"))
                .exists(),
            "queued operation {index} started while all permits were occupied"
        );
    }

    let cancel = exchange(
        &fixture.executor_socket,
        &request(
            NONCE,
            "request-pool-independent-cancel",
            json!({
                "type": "cancel",
                "execution_id": "execution-pool-running-0",
            }),
        ),
    )
    .await;
    assert!(cancel.iter().any(|frame| {
        frame["request_id"] == "request-pool-independent-cancel"
            && frame["result"]["Ok"]["type"] == "cancel_accepted"
    }));

    drop(queued);
    drop(running);
    for pid in pids {
        wait_for_process_group_gone(pid).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    for index in 0..24 {
        let path = fixture.workspace.join(format!("pool-queued-{index}.pid"));
        if let Ok(pid) = std::fs::read_to_string(path) {
            wait_for_process_group_gone(pid.trim().parse().unwrap()).await;
        }
    }
}

#[tokio::test]
async fn unconsumed_output_does_not_block_cancel_reap_or_later_connections() {
    let mut fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.join("source.txt"), "after-backpressure").unwrap();
    fixture.start_executor(NONCE).await;

    let pid_file = fixture.workspace.join("backpressure.pid");
    let mut stream = UnixStream::connect(&fixture.executor_socket).await.unwrap();
    let start = request(
        NONCE,
        "request-backpressure",
        json!({
            "type": "bash",
            "command": "printf '%s' \"$$\" > backpressure.pid; while :; do printf '%4096s' x; sleep 0.02; done",
            "execution_id": "execution-backpressure",
        }),
    );
    let mut bytes = serde_json::to_vec(&start).unwrap();
    bytes.push(b'\n');
    stream.write_all(&bytes).await.unwrap();
    let pid = wait_for_pid(&pid_file).await;

    // Do not consume the response half. Let progress fill transport buffers,
    // then prove the independent request half still admits cancellation.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let cancel = request(
        NONCE,
        "request-backpressure-cancel",
        json!({
            "type": "cancel",
            "execution_id": "execution-backpressure",
        }),
    );
    let mut bytes = serde_json::to_vec(&cancel).unwrap();
    bytes.push(b'\n');
    timeout(Duration::from_secs(1), stream.write_all(&bytes))
        .await
        .expect("cancel write was blocked by unconsumed output")
        .expect("cancel write failed");
    wait_for_process_group_gone(pid).await;
    drop(stream);

    let read = read_file_request(
        NONCE,
        "request-after-backpressure",
        "execution-after-backpressure",
        "source.txt",
    );
    assert_eq!(
        terminal_content(&exchange(&fixture.executor_socket, &read).await),
        "after-backpressure"
    );
}

#[tokio::test]
async fn idle_initial_connections_expire_before_valid_connection_admission() {
    let mut fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.join("source.txt"), "bounded").unwrap();
    fixture.start_executor(NONCE).await;
    // Let the readiness probe's empty connection release its permit.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut idle = Vec::new();
    for _ in 0..32 {
        idle.push(UnixStream::connect(&fixture.executor_socket).await.unwrap());
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The valid 33rd peer waits in the listener backlog instead of being
    // dropped. The initial-frame tier expires the 32 idle sockets after one
    // second, independently of the 135-second promoted connection lifetime.
    let accepted = read_file_request(
        NONCE,
        "request-valid-after-idle",
        "execution-valid-after-idle",
        "source.txt",
    );
    assert_eq!(
        terminal_content(&exchange(&fixture.executor_socket, &accepted).await),
        "bounded"
    );
    drop(idle);
}

#[tokio::test]
async fn completed_primary_requests_close_even_when_clients_keep_write_halves_open() {
    let mut fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.join("source.txt"), "released").unwrap();
    fixture.start_executor(NONCE).await;

    let mut completed_clients = Vec::new();
    for index in 0..32 {
        let mut stream = UnixStream::connect(&fixture.executor_socket).await.unwrap();
        let request = read_file_request(
            NONCE,
            &format!("request-completed-open-{index}"),
            &format!("execution-completed-open-{index}"),
            "source.txt",
        );
        let mut bytes = serde_json::to_vec(&request).unwrap();
        bytes.push(b'\n');
        stream.write_all(&bytes).await.unwrap();
        // Intentionally retain the client write half after one primary
        // operation. The server must emit the terminal and close its session.
        let mut line = String::new();
        timeout(
            Duration::from_secs(5),
            BufReader::new(&mut stream).read_line(&mut line),
        )
        .await
        .expect("completed connection did not receive a terminal")
        .expect("read terminal");
        let terminal: Value = serde_json::from_str(&line).expect("decode terminal");
        assert_eq!(
            terminal["result"]["Ok"]["result"]["content"],
            Value::String("released".to_owned())
        );
        completed_clients.push(stream);
    }

    let later = read_file_request(
        NONCE,
        "request-after-completed-open",
        "execution-after-completed-open",
        "source.txt",
    );
    assert_eq!(
        terminal_content(&exchange(&fixture.executor_socket, &later).await),
        "released"
    );
    drop(completed_clients);
}

#[tokio::test]
async fn malformed_and_oversized_connections_do_not_poison_listener() {
    let mut fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.join("source.txt"), "source").unwrap();
    fixture.start_executor(NONCE).await;

    for bytes in [b"{not-json}\n".to_vec(), vec![b'x'; 1024 * 1024 + 1]] {
        let mut stream = UnixStream::connect(&fixture.executor_socket).await.unwrap();
        let _ = stream.write_all(&bytes).await;
        let _ = stream.shutdown().await;
        let mut lines = BufReader::new(stream).lines();
        match timeout(Duration::from_secs(5), lines.next_line())
            .await
            .expect("malformed connection did not close")
        {
            Ok(None) | Err(_) => {}
            Ok(Some(line)) => panic!("malformed connection emitted a frame: {line}"),
        }
    }

    let valid = read_file_request(
        NONCE,
        "request-after-malformed",
        "execution-after-malformed",
        "source.txt",
    );
    assert_eq!(
        terminal_content(&exchange(&fixture.executor_socket, &valid).await),
        "source"
    );
}
