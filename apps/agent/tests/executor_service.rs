#![cfg(target_os = "linux")]

use std::{
    fs::OpenOptions,
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    process::{Child, Command},
    sync::mpsc,
    time::timeout,
};
use uuid::Uuid;

const GENERATION: u64 = 19;
const NONCE: &str = "executor-service-test";

struct Fixture {
    root: PathBuf,
    workspace: PathBuf,
    artifacts: PathBuf,
    socket: PathBuf,
    broker: Child,
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!("sumi-executor-{}", Uuid::now_v7()));
        let workspace = root.join("workspace");
        let artifacts = root.join("artifacts");
        let socket = root.join("broker.sock");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&artifacts).unwrap();
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
        self.executor_with_stderr(Stdio::null())
    }

    fn executor_with_stderr(&self, stderr: Stdio) -> Child {
        Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
            .arg("--tool-executor")
            .env_clear()
            .env("SUMI_RPC_GENERATION", GENERATION.to_string())
            .env("SUMI_RPC_NONCE", NONCE)
            .env("SUMI_WORKSPACE", &self.workspace)
            .env("SUMI_CONVERSATION_ID", "conversation-1")
            .env("SUMI_ARTIFACT_BROKER_SOCKET", &self.socket)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr)
            .spawn()
            .unwrap()
    }
}

async fn wait_for_nonempty_file(path: &Path) -> String {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(content) = std::fs::read_to_string(path)
                && !content.trim().is_empty()
            {
                return content;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("nonempty file timeout")
}

fn process_group_exists(process_group: i32) -> bool {
    let result = unsafe { libc::kill(-process_group, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
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

#[tokio::test]
async fn service_mode_dispatch_precedes_runtime_config() {
    let output = Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
        .arg("--tool-executor")
        .env_clear()
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("SUMI_RPC_GENERATION"), "{stderr}");
    assert!(stderr.contains("low-trust-local"), "{stderr}");
    assert!(!stderr.contains("SUMI_CONFIG"), "{stderr}");
    assert!(!stderr.contains("wrapping key"), "{stderr}");
}

#[tokio::test]
async fn idle_or_nonreading_broker_clients_do_not_starve_later_clients() {
    let fixture = Fixture::new().await;
    let idle = UnixStream::connect(&fixture.socket).await.unwrap();
    let mut nonreader = UnixStream::connect(&fixture.socket).await.unwrap();
    let mut bytes = serde_json::to_vec(&request(
        "nonreader",
        json!({
            "type":"begin_tool_output", "conversation_id":"conversation-1",
            "execution_id":"nonreader", "content":[120]
        }),
    ))
    .unwrap();
    bytes.push(b'\n');
    nonreader.write_all(&bytes).await.unwrap();
    nonreader.shutdown().await.unwrap();

    let response = timeout(
        Duration::from_millis(500),
        broker_rpc(
            &fixture.socket,
            &request(
                "later",
                json!({
                    "type":"begin_tool_output", "conversation_id":"conversation-1",
                    "execution_id":"later", "content":[121]
                }),
            ),
        ),
    )
    .await
    .expect("later broker client must not be starved")
    .unwrap();
    assert_eq!(response["result"]["Ok"]["offset"], 1);
    drop(idle);
    drop(nonreader);
}

#[tokio::test]
async fn broker_fences_identity_and_round_trips_begin_append_finish() {
    let fixture = Fixture::new().await;
    let stale = request(
        "stale",
        json!({
            "type": "read_artifact",
            "conversation_id": "conversation-1",
            "handle": "artifact://conversation-1/tool-output/execution-1",
            "offset": 0,
            "limit": 10,
        }),
    );
    let mut stale = stale;
    stale["generation"] = json!(GENERATION - 1);
    assert_eq!(broker_rpc(&fixture.socket, &stale).await, None);

    let begun = broker_rpc(
        &fixture.socket,
        &request(
            "begin",
            json!({
                "type": "begin_tool_output",
                "conversation_id": "conversation-1",
                "execution_id": "execution-1",
                "content": [104, 101, 108, 108, 111],
            }),
        ),
    )
    .await
    .unwrap();
    let handle = begun["result"]["Ok"]["handle"].as_str().unwrap();
    assert_eq!(begun["result"]["Ok"]["offset"], 5);
    let appended = broker_rpc(
        &fixture.socket,
        &request(
            "append",
            json!({
                "type": "append_tool_output",
                "conversation_id": "conversation-1",
                "handle": handle,
                "offset": 5,
                "content": [32, 119, 111, 114, 108, 100],
            }),
        ),
    )
    .await
    .unwrap();
    assert_eq!(appended["result"]["Ok"]["offset"], 11);
    let finished = broker_rpc(
        &fixture.socket,
        &request(
            "finish",
            json!({
                "type": "finish_tool_output",
                "conversation_id": "conversation-1",
                "handle": handle,
            }),
        ),
    )
    .await
    .unwrap();
    assert_eq!(finished["result"]["Ok"]["type"], "finished");
    assert_eq!(
        std::fs::read(
            fixture
                .artifacts
                .join("conversation-1/tool-output/execution-1")
        )
        .unwrap(),
        b"hello world"
    );
}

#[tokio::test]
async fn timed_out_accepted_broker_mutation_finishes_and_replay_is_exact() {
    let fixture = Fixture::new().await;
    let begun = broker_rpc(
        &fixture.socket,
        &request(
            "begin-slow",
            json!({
                "type":"begin_tool_output", "conversation_id":"conversation-1",
                "execution_id":"slow", "content":[97]
            }),
        ),
    )
    .await
    .unwrap();
    let handle = begun["result"]["Ok"]["handle"].as_str().unwrap().to_owned();
    let artifact_path = fixture.artifacts.join("conversation-1/tool-output/slow");
    let locked = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&artifact_path)
        .unwrap();
    assert_eq!(unsafe { libc::flock(locked.as_raw_fd(), libc::LOCK_EX) }, 0);

    let socket = fixture.socket.clone();
    let append = request(
        "append-slow",
        json!({
            "type":"append_tool_output", "conversation_id":"conversation-1",
            "handle":handle, "offset":1, "content":[98,99]
        }),
    );
    let timed_out = tokio::spawn(async move { broker_rpc(&socket, &append).await });
    assert_eq!(
        timeout(Duration::from_secs(4), timed_out)
            .await
            .expect("transport timeout")
            .unwrap(),
        None
    );
    assert_eq!(unsafe { libc::flock(locked.as_raw_fd(), libc::LOCK_UN) }, 0);

    timeout(Duration::from_secs(5), async {
        while std::fs::read(&artifact_path).unwrap() != b"abc" {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("accepted mutation must finish after transport timeout");
    let replay = broker_rpc(
        &fixture.socket,
        &request(
            "append-replay",
            json!({
                "type":"append_tool_output", "conversation_id":"conversation-1",
                "handle":"artifact://conversation-1/tool-output/slow",
                "offset":1, "content":[98,99]
            }),
        ),
    )
    .await
    .unwrap();
    assert_eq!(replay["result"]["Ok"]["offset"], 3);
    assert_eq!(std::fs::read(artifact_path).unwrap(), b"abc");
}

#[tokio::test]
async fn executor_routes_workspace_and_artifact_reads_and_grep() {
    let fixture = Fixture::new().await;
    let begun = broker_rpc(
        &fixture.socket,
        &request(
            "begin-route",
            json!({
                "type": "begin_tool_output",
                "conversation_id": "conversation-1",
                "execution_id": "route-output",
                "content": [97, 108, 112, 104, 97, 10, 110, 101, 101, 100, 108, 101, 10],
            }),
        ),
    )
    .await
    .unwrap();
    let handle = begun["result"]["Ok"]["handle"].as_str().unwrap();

    let mut child = fixture.executor_with_stderr(Stdio::piped());
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    send_request(
        &mut stdin,
        &request(
            "write",
            json!({"type":"write_file","path":"note.txt","content":"workspace","execution_id":"write-1"}),
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
            "read-workspace",
            json!({"type":"read_file","path":"note.txt","offset":0,"limit":51200,"execution_id":"read-1"}),
        ),
    )
    .await;
    let workspace = read_frame(&mut stdout).await;
    assert_eq!(workspace["result"]["Ok"]["result"]["content"], "workspace");
    send_request(
        &mut stdin,
        &request(
            "read-artifact",
            json!({"type":"read_file","path":handle,"offset":0,"limit":51200,"execution_id":"read-2"}),
        ),
    )
    .await;
    let artifact = read_frame(&mut stdout).await;
    assert_eq!(artifact["result"]["Ok"]["response"]["content"][0], 97);
    send_request(
        &mut stdin,
        &request(
            "grep-artifact",
            json!({"type":"grep","path":handle,"pattern":"needle","execution_id":"grep-1"}),
        ),
    )
    .await;
    let grep = read_frame(&mut stdout).await;
    assert_eq!(
        grep["result"]["Ok"]["response"]["matches"][0]["line"],
        "needle"
    );
    send_request(
        &mut stdin,
        &request(
            "reject-artifact-write",
            json!({"type":"write_file","path":handle,"content":"no","execution_id":"write-2"}),
        ),
    )
    .await;
    let rejected = read_frame(&mut stdout).await;
    assert_eq!(rejected["result"]["Err"]["code"], "invalid_path");
    drop(stdin);
    assert!(child.wait().await.unwrap().success());
}

#[tokio::test]
async fn bash_cancel_emits_one_terminal_per_request_and_no_late_update() {
    let fixture = Fixture::new().await;
    let mut child = fixture.executor();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    send_request(
        &mut stdin,
        &request(
            "bash",
            json!({"type":"bash","command":"printf ready; sleep 30; printf late","execution_id":"bash-1"}),
        ),
    )
    .await;
    let first = read_frame(&mut stdout).await;
    assert_eq!(first["type"], "update");
    send_request(
        &mut stdin,
        &request("cancel", json!({"type":"cancel","execution_id":"bash-1"})),
    )
    .await;
    let mut frames = [read_frame(&mut stdout).await, read_frame(&mut stdout).await];
    frames.sort_by_key(|frame| frame["request_id"].as_str().unwrap().to_owned());
    assert_eq!(frames[0]["request_id"], "bash");
    assert_eq!(frames[0]["type"], "terminal");
    assert_eq!(frames[0]["result"]["Ok"]["result"]["cancelled"], true);
    assert_eq!(frames[1]["request_id"], "cancel");
    assert_eq!(frames[1]["type"], "terminal");
    drop(stdin);
    let mut trailing = Vec::new();
    timeout(Duration::from_secs(5), stdout.read_to_end(&mut trailing))
        .await
        .expect("executor stdout must reach EOF after cancellation")
        .unwrap();
    assert!(
        trailing.iter().all(|byte| byte.is_ascii_whitespace()),
        "unexpected trailing executor frames: {}",
        String::from_utf8_lossy(&trailing)
    );
    let status = timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("executor must exit after stdout EOF")
        .unwrap();
    assert!(status.success());
}

#[tokio::test]
async fn slow_progress_consumer_drops_overflow_but_receives_authoritative_terminal() {
    let fixture = Fixture::new().await;
    let mut child = fixture.executor_with_stderr(Stdio::piped());
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    assert_ne!(
        unsafe { libc::fcntl(stdout.as_raw_fd(), libc::F_SETPIPE_SZ, 4_096) },
        -1,
        "shrink executor stdout pipe for deterministic backpressure"
    );
    let mut stdout = BufReader::new(stdout);
    send_request(
        &mut stdin,
        &request(
            "bash-slow-progress",
            json!({
                "type":"bash",
                "command":"for i in $(seq 1 300); do printf '%03d\\n' \"$i\"; sleep 0.001; done",
                "execution_id":"bash-slow-progress",
            }),
        ),
    )
    .await;

    let mut delivered = Vec::new();
    let terminal = timeout(Duration::from_secs(15), async {
        loop {
            let frame = read_frame(&mut stdout).await;
            if frame["type"] == "terminal" {
                break frame;
            }
            if let Some(output) = frame["value"]["output"].as_str() {
                delivered.push(output.to_owned());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("volatile progress must not delay authoritative completion");
    let result = &terminal["result"]["Ok"]["result"];
    assert_eq!(result["cancelled"], false);
    assert_eq!(result["exit_code"], 0);
    let complete = result["output"].as_str().unwrap();
    let mut cursor = 0usize;
    for chunk in delivered {
        let relative = complete[cursor..]
            .find(&chunk)
            .unwrap_or_else(|| panic!("delivered update was out of order: {chunk:?}"));
        cursor += relative + chunk.len();
    }

    drop(stdin);
    assert!(child.wait().await.unwrap().success());
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .await
        .unwrap();
    assert!(
        stderr.contains("dropping volatile executor progress update"),
        "test did not drive either bounded progress queue beyond capacity: {stderr}"
    );
}

async fn run_bash_reap_timeout_case(matching_cancel: bool) -> [Value; 2] {
    let mut fixture = Fixture::new().await;
    fixture.broker.start_kill().unwrap();
    fixture.broker.wait().await.unwrap();
    std::fs::remove_file(&fixture.socket).unwrap();

    let listener = UnixListener::bind(&fixture.socket).unwrap();
    let (accepted_tx, mut accepted_rx) = mpsc::channel(4);
    let hanging_broker = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let accepted_tx = accepted_tx.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                stream.read_to_end(&mut request).await.unwrap();
                accepted_tx.send(()).await.unwrap();
                std::future::pending::<()>().await;
            });
        }
    });

    let mut child = fixture.executor();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    send_request(
        &mut stdin,
        &request(
            "bash-reap-timeout",
            json!({
                "type":"bash",
                "command":"head -c 60000 /dev/zero | tr '\\0' x; sleep 30",
                "execution_id":"bash-reap-timeout",
            }),
        ),
    )
    .await;
    timeout(Duration::from_secs(5), accepted_rx.recv())
        .await
        .expect("bash must reach the hanging broker")
        .expect("hanging broker notification");
    send_request(
        &mut stdin,
        &request(
            "control-reap-timeout",
            json!({
                "type":"cancel",
                "execution_id": if matching_cancel {
                    "bash-reap-timeout"
                } else {
                    "wrong-execution"
                },
            }),
        ),
    )
    .await;
    drop(stdin);

    let first_terminal = loop {
        let frame = read_frame(&mut stdout).await;
        if frame["type"] == "terminal" {
            break frame;
        }
    };
    let second_terminal = read_frame(&mut stdout).await;
    assert_eq!(second_terminal["type"], "terminal");

    let mut trailing = Vec::new();
    timeout(Duration::from_secs(5), stdout.read_to_end(&mut trailing))
        .await
        .expect("executor stdout must close after uncertain reap")
        .unwrap();
    assert!(trailing.iter().all(|byte| byte.is_ascii_whitespace()));
    let status = timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("executor must close the service epoch after uncertain reap")
        .unwrap();
    assert!(!status.success());
    hanging_broker.abort();
    [first_terminal, second_terminal]
}

#[tokio::test]
async fn bash_reap_timeout_emits_bounded_terminals_then_closes_epoch() {
    let cancelled = run_bash_reap_timeout_case(true).await;
    assert_eq!(cancelled[0]["request_id"], "control-reap-timeout");
    assert_eq!(cancelled[0]["result"]["Err"]["code"], "rpc_indeterminate");
    assert_eq!(cancelled[1]["request_id"], "bash-reap-timeout");
    assert_eq!(cancelled[1]["result"]["Err"]["code"], "rpc_indeterminate");

    let fatal = run_bash_reap_timeout_case(false).await;
    assert_eq!(fatal[0]["request_id"], "bash-reap-timeout");
    assert_eq!(fatal[0]["result"]["Err"]["code"], "rpc_indeterminate");
    assert_eq!(fatal[1]["request_id"], "control-reap-timeout");
    assert_eq!(fatal[1]["result"]["Err"]["code"], "protocol");
}

#[tokio::test]
async fn unconsumed_executor_stdout_cannot_block_cancel_and_reap() {
    let fixture = Fixture::new().await;
    let secret_id = "request-secret-must-not-enter-stderr";
    let mut child = fixture.executor_with_stderr(Stdio::piped());
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    send_request(
        &mut stdin,
        &request(
            secret_id,
            json!({"type":"bash","command":"head -c 8388608 /dev/zero | tr '\\0' x; sleep 30","execution_id":"bash-backpressure"}),
        ),
    ).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    send_request(
        &mut stdin,
        &request(
            "cancel-backpressure",
            json!({"type":"cancel","execution_id":"bash-backpressure"}),
        ),
    )
    .await;
    drop(stdin);
    let status = timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("executor must cancel/reap despite stdout backpressure")
        .unwrap();
    assert!(!status.success(), "output failure closes the service epoch");
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .await
        .unwrap();
    assert!(
        stderr.contains("executor output") || stderr.contains("terminal write deadline"),
        "{stderr}"
    );
    assert!(!stderr.contains(secret_id), "{stderr}");
    drop(stdout);
}

#[tokio::test]
async fn active_bash_control_failures_cancel_and_reap_the_process_group() {
    for (case, payload, response_id) in [
        ("decode", b"not-json\n".to_vec(), None),
        ("partial-eof", b"{".to_vec(), None),
        (
            "lifecycle",
            serde_json::to_vec(&request(
                "bash-lifecycle",
                json!({"type":"read_file","path":"missing","offset":0,"limit":1,"execution_id":"other"}),
            ))
            .unwrap()
            .into_iter()
            .chain(std::iter::once(b'\n'))
            .collect(),
            None,
        ),
        (
            "invalid-operation",
            serde_json::to_vec(&request(
                "invalid-operation",
                json!({"type":"read_file","path":"missing","offset":0,"limit":1,"execution_id":"other"}),
            ))
            .unwrap()
            .into_iter()
            .chain(std::iter::once(b'\n'))
            .collect(),
            Some("invalid-operation"),
        ),
        (
            "wrong-target-cancel",
            serde_json::to_vec(&request(
                "wrong-target-cancel",
                json!({"type":"cancel","execution_id":"not-active"}),
            ))
            .unwrap()
            .into_iter()
            .chain(std::iter::once(b'\n'))
            .collect(),
            Some("wrong-target-cancel"),
        ),
    ] {
        let fixture = Fixture::new().await;
        let pid_path = fixture.workspace.join("bash.pid");
        let request_id = if case == "lifecycle" {
            "bash-lifecycle"
        } else {
            "bash-control-failure"
        };
        let mut child = fixture.executor();
        let mut stdin = child.stdin.take().unwrap();
        let mut stdout = child.stdout.take().unwrap();
        send_request(
            &mut stdin,
            &request(
                request_id,
                json!({"type":"bash","command":"echo $$ > bash.pid; sleep 30","execution_id":"bash-control-failure"}),
            ),
        )
        .await;
        let process_group = wait_for_nonempty_file(&pid_path)
            .await
            .trim()
            .parse::<i32>()
            .unwrap();
        stdin.write_all(&payload).await.unwrap();
        drop(stdin);
        let status = timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap_or_else(|_| panic!("{case} failure must cancel and reap"))
            .unwrap();
        assert!(!status.success(), "{case}");
        let mut output = String::new();
        stdout.read_to_string(&mut output).await.unwrap();
        let frames: Vec<Value> = output
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert!(
            frames.iter().all(|frame| frame["type"] == "terminal"),
            "{case}: {frames:?}"
        );
        assert!(
            frames.iter().any(|frame| frame["request_id"] == request_id),
            "{case}: missing active terminal: {frames:?}"
        );
        if let Some(response_id) = response_id {
            let response = frames
                .iter()
                .find(|frame| frame["request_id"] == response_id)
                .unwrap_or_else(|| panic!("{case}: missing control terminal: {frames:?}"));
            assert_eq!(response["result"]["Err"]["code"], "protocol", "{case}");
        }
        timeout(Duration::from_secs(2), async {
            while process_group_exists(process_group) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{case} bash process group must disappear"));
    }
}

#[tokio::test]
async fn queued_cancel_is_settled_before_simultaneous_bash_completion() {
    let fixture = Fixture::new().await;
    let mut child = fixture.executor();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    for iteration in 0..100 {
        let release = fixture.workspace.join(format!("release-{iteration}"));
        let ready = fixture.workspace.join(format!("ready-{iteration}"));
        let execution_id = format!("completion-race-{iteration}");
        send_request(
            &mut stdin,
            &request(
                &format!("bash-{iteration}"),
                json!({
                    "type":"bash",
                    "command":format!("touch ready-{iteration}; while [ ! -e release-{iteration} ]; do :; done"),
                    "execution_id":execution_id,
                }),
            ),
        )
        .await;
        wait_for_nonempty_or_existing_file(&ready).await;
        if iteration % 2 == 0 {
            send_request(
                &mut stdin,
                &request(
                    &format!("cancel-{iteration}"),
                    json!({"type":"cancel","execution_id":execution_id}),
                ),
            )
            .await;
            std::fs::write(&release, b"release").unwrap();
        } else {
            std::fs::write(&release, b"release").unwrap();
            send_request(
                &mut stdin,
                &request(
                    &format!("cancel-{iteration}"),
                    json!({"type":"cancel","execution_id":execution_id}),
                ),
            )
            .await;
        }

        let first = read_frame(&mut stdout).await;
        let second = read_frame(&mut stdout).await;
        let frames = [first, second];
        let cancel = frames
            .iter()
            .find(|frame| frame["request_id"] == format!("cancel-{iteration}"))
            .unwrap_or_else(|| panic!("iteration {iteration}: {frames:?}"));
        let bash = frames
            .iter()
            .find(|frame| frame["request_id"] == format!("bash-{iteration}"))
            .unwrap_or_else(|| panic!("iteration {iteration}: {frames:?}"));
        let physically_cancelled = bash["result"]["Ok"]["result"]["cancelled"] == true;
        assert_eq!(
            cancel["result"]["Ok"]["type"],
            if physically_cancelled {
                "cancel_accepted"
            } else {
                "cancel_too_late"
            },
            "iteration {iteration}: {frames:?}"
        );
    }
    drop(stdin);
    assert!(child.wait().await.unwrap().success());
}

async fn wait_for_nonempty_or_existing_file(path: &Path) {
    timeout(Duration::from_secs(5), async {
        while !path.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("file creation timeout");
}

#[tokio::test]
async fn broker_failure_telemetry_omits_caller_execution_id() {
    let mut fixture = Fixture::new().await;
    fixture.broker.start_kill().unwrap();
    fixture.broker.wait().await.unwrap();
    let secret_execution_id = "execution-secret-must-not-enter-stderr";
    let mut child = fixture.executor_with_stderr(Stdio::piped());
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    send_request(
        &mut stdin,
        &request(
            "broker-failure",
            json!({
                "type":"bash",
                "command":"head -c 60000 /dev/zero | tr '\\0' x",
                "execution_id":secret_execution_id,
            }),
        ),
    )
    .await;
    loop {
        if read_frame(&mut stdout).await["type"] == "terminal" {
            break;
        }
    }
    drop(stdin);
    assert!(child.wait().await.unwrap().success());
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .await
        .unwrap();
    assert!(stderr.contains("artifact publication failed"), "{stderr}");
    assert!(!stderr.contains(secret_execution_id), "{stderr}");
}

#[tokio::test]
async fn bash_archives_large_output_through_the_broker_client() {
    let fixture = Fixture::new().await;
    let mut child = fixture.executor();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    send_request(
        &mut stdin,
        &request(
            "bash-archive",
            json!({"type":"bash","command":"head -c 60000 /dev/zero | tr '\\0' x","execution_id":"bash-archive-1"}),
        ),
    )
    .await;
    let terminal = loop {
        let frame = read_frame(&mut stdout).await;
        if frame["type"] == "terminal" {
            break frame;
        }
    };
    assert_eq!(
        terminal["result"]["Ok"]["result"]["artifact_handle"],
        "artifact://conversation-1/tool-output/bash-archive-1"
    );
    assert_eq!(
        std::fs::metadata(
            fixture
                .artifacts
                .join("conversation-1/tool-output/bash-archive-1")
        )
        .unwrap()
        .len(),
        60_000
    );
    drop(stdin);
    assert!(child.wait().await.unwrap().success());
}

#[tokio::test]
async fn malformed_and_oversized_frames_fail_closed() {
    let fixture = Fixture::new().await;
    for payload in [b"not-json\n".to_vec(), vec![b'x'; 1024 * 1024]] {
        let mut child = fixture.executor();
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(&payload).await.unwrap();
        drop(stdin);
        let status = timeout(Duration::from_secs(5), child.wait())
            .await
            .expect("executor must not hang")
            .unwrap();
        assert!(!status.success());
    }
}
