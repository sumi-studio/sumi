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
    executor_socket: PathBuf,
    executor: Option<Child>,
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("sumi-critical-executor-{}", Uuid::now_v7()));
        let workspace = root.join("workspace");
        let executor_socket = root.join("executor.sock");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self {
            root,
            workspace,
            executor_socket,
            executor: None,
        }
    }

    async fn start(&mut self, nonce: &str) {
        assert!(self.executor.is_none());
        self.executor = Some(spawn_executor(
            &self.workspace,
            &self.executor_socket,
            nonce,
        ));
        wait_for_socket(&self.executor_socket).await;
    }

    async fn restart(&mut self, nonce: &str) {
        let mut executor = self.executor.take().expect("executor is running");
        executor.kill().await.expect("kill executor");
        executor.wait().await.expect("wait executor");
        self.start(nonce).await;
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(executor) = &mut self.executor {
            let _ = executor.start_kill();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn spawn_executor(workspace: &Path, executor_socket: &Path, nonce: &str) -> Child {
    Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
        .arg("--tool-executor-socket")
        .env_clear()
        .env("SUMI_PERSONALITY_AGENT_ID", PERSONALITY_AGENT_ID)
        .env("SUMI_RPC_GENERATION", GENERATION.to_string())
        .env("SUMI_RPC_NONCE", nonce)
        .env("SUMI_WORKSPACE", workspace)
        .env("SUMI_EXECUTOR_SOCKET", executor_socket)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn critical socket executor")
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
    request(
        nonce,
        request_id,
        json!({
            "type": "health",
            "service_role": "tool_executor",
        }),
    )
}

async fn exchange(socket: &Path, value: &Value) -> Vec<Value> {
    let mut stream = UnixStream::connect(socket).await.expect("connect executor");
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    stream.write_all(&bytes).await.expect("write request");
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

fn terminal_content(frames: &[Value]) -> &str {
    frames
        .iter()
        .find(|frame| frame["type"] == "terminal")
        .and_then(|frame| frame["result"]["Ok"]["result"]["content"].as_str())
        .expect("read_file terminal content")
}

#[tokio::test]
async fn production_manager_runs_concurrent_read_file_and_fences_identity() {
    let mut fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.join("alpha.txt"), "alpha").unwrap();
    std::fs::write(fixture.workspace.join("beta.txt"), "beta").unwrap();
    fixture.start(NONCE).await;

    let alpha = read_file_request(NONCE, "request-alpha", "execution-alpha", "alpha.txt");
    let beta = read_file_request(NONCE, "request-beta", "execution-beta", "beta.txt");
    let (alpha, beta) = tokio::join!(
        exchange(&fixture.executor_socket, &alpha),
        exchange(&fixture.executor_socket, &beta),
    );
    assert_eq!(terminal_content(&alpha), "alpha");
    assert_eq!(terminal_content(&beta), "beta");

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
async fn production_socket_accepts_workspace_list_glob_and_grep() {
    let mut fixture = Fixture::new().await;
    std::fs::create_dir(fixture.workspace.join("nested")).unwrap();
    std::fs::write(fixture.workspace.join("root.txt"), "needle at root\n").unwrap();
    std::fs::write(
        fixture.workspace.join("nested/second.txt"),
        "another needle\n",
    )
    .unwrap();
    fixture.start(NONCE).await;

    let listed = exchange(
        &fixture.executor_socket,
        &request(
            NONCE,
            "request-list",
            json!({"type":"list_dir","path":".","execution_id":"execution-list"}),
        ),
    )
    .await;
    let listed_terminal = listed
        .iter()
        .find(|frame| frame["type"] == "terminal")
        .expect("list_dir terminal");
    assert_eq!(listed_terminal["result"]["Ok"]["type"], "listed");
    let entries = listed_terminal["result"]["Ok"]["entries"]
        .as_array()
        .expect("list_dir entries");
    assert!(entries.iter().any(|entry| entry == "root.txt"));
    assert!(entries.iter().any(|entry| entry == "nested"));

    let globbed = exchange(
        &fixture.executor_socket,
        &request(
            NONCE,
            "request-glob",
            json!({"type":"glob","pattern":"**/*.txt","execution_id":"execution-glob"}),
        ),
    )
    .await;
    let globbed_terminal = globbed
        .iter()
        .find(|frame| frame["type"] == "terminal")
        .expect("glob terminal");
    assert_eq!(globbed_terminal["result"]["Ok"]["type"], "globbed");
    let paths = globbed_terminal["result"]["Ok"]["paths"]
        .as_array()
        .expect("glob paths");
    assert!(paths.iter().any(|path| path == "root.txt"));
    assert!(paths.iter().any(|path| path == "nested/second.txt"));

    let grepped = exchange(
        &fixture.executor_socket,
        &request(
            NONCE,
            "request-grep",
            json!({"type":"grep","path":".","pattern":"needle","execution_id":"execution-grep"}),
        ),
    )
    .await;
    let grepped_terminal = grepped
        .iter()
        .find(|frame| frame["type"] == "terminal")
        .expect("grep terminal");
    assert_eq!(grepped_terminal["result"]["Ok"]["type"], "grepped");
    let matches = grepped_terminal["result"]["Ok"]["matches"]
        .as_array()
        .expect("grep matches");
    assert_eq!(matches.len(), 2);
    assert!(
        matches
            .iter()
            .all(|entry| entry["line"] == "needle at root" || entry["line"] == "another needle")
    );
}

#[tokio::test]
async fn manager_restart_rotates_nonce_and_rebinds_stale_socket() {
    let mut fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.join("source.txt"), "source").unwrap();
    fixture.start(NONCE).await;
    assert_eq!(
        std::fs::metadata(&fixture.executor_socket)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o660
    );

    let mut competing = spawn_executor(&fixture.workspace, &fixture.executor_socket, NONCE);
    let status = timeout(Duration::from_secs(5), competing.wait())
        .await
        .expect("competing manager did not fail closed")
        .unwrap();
    assert!(!status.success(), "a second manager stole the live socket");

    fixture.restart(RESTARTED_NONCE).await;
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
async fn health_is_exact_role_bound_and_side_effect_free() {
    let mut fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.join("source.txt"), "healthy").unwrap();
    fixture.start(NONCE).await;

    let health = health_request(NONCE, "reused-health-request");
    for _ in 0..16 {
        let frames = exchange(&fixture.executor_socket, &health).await;
        assert_eq!(frames.len(), 1);
        assert_eq!(
            frames[0]["result"]["Ok"],
            json!({
                "type": "healthy",
                "service_role": "tool_executor",
            })
        );
    }

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

    let mut wrong_role = health_request(NONCE, "wrong-role");
    wrong_role["operation"]["service_role"] = json!("artifact_broker");
    assert!(
        exchange(&fixture.executor_socket, &wrong_role)
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn production_socket_rejects_mutating_control_and_artifact_operations_without_side_effects() {
    let mut fixture = Fixture::new().await;
    let sentinel = fixture.workspace.join("sentinel.txt");
    std::fs::write(&sentinel, "original").unwrap();
    fixture.start(NONCE).await;

    let operations = [
        json!({
            "type": "bash",
            "command": "printf mutated > sentinel.txt",
            "execution_id": "forbidden-bash",
        }),
        json!({
            "type": "write_file",
            "path": "sentinel.txt",
            "content": "mutated",
            "execution_id": "forbidden-write",
        }),
        json!({
            "type": "edit_file",
            "path": "sentinel.txt",
            "old_string": "original",
            "new_string": "mutated",
            "execution_id": "forbidden-edit",
        }),
        json!({
            "type": "remove_file",
            "path": "sentinel.txt",
            "execution_id": "forbidden-remove",
        }),
        json!({
            "type": "cancel",
            "execution_id": "unknown",
        }),
        json!({
            "type": "read_file",
            "path": concat!(
                "artifact://",
                "018f8a9e-65c0-7a5b-8d3c-1f2a3b4c5d6e/attachments/forbidden"
            ),
            "offset": 0,
            "limit": 1024,
            "execution_id": "forbidden-artifact-read",
        }),
    ];

    for (index, operation) in operations.into_iter().enumerate() {
        let frames = exchange(
            &fixture.executor_socket,
            &request(NONCE, &format!("forbidden-{index}"), operation),
        )
        .await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0]["result"]["Err"]["code"], "protocol");
    }
    assert_eq!(std::fs::read_to_string(sentinel).unwrap(), "original");
}

#[tokio::test]
async fn idle_handshakes_expire_without_dropping_the_next_valid_read() {
    let mut fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.join("source.txt"), "bounded").unwrap();
    fixture.start(NONCE).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut idle = Vec::new();
    for _ in 0..32 {
        idle.push(UnixStream::connect(&fixture.executor_socket).await.unwrap());
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

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
async fn completed_primary_reads_close_and_release_connection_ownership() {
    let mut fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.join("source.txt"), "released").unwrap();
    fixture.start(NONCE).await;

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
async fn malformed_and_oversized_connections_do_not_poison_the_listener() {
    let mut fixture = Fixture::new().await;
    std::fs::write(fixture.workspace.join("source.txt"), "source").unwrap();
    fixture.start(NONCE).await;

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
