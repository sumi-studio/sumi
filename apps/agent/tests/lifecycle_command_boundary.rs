use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::{Child, Command};

const WRAPPING_KEY: &str = "4242424242424242424242424242424242424242424242424242424242424242";

fn test_directory() -> PathBuf {
    std::env::temp_dir().join(format!("sumi-lifecycle-{}", uuid::Uuid::now_v7()))
}

fn agent_command(directory: &PathBuf, extra_env: Vec<(&str, String)>) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_sumi-agent"));
    cmd.current_dir(directory)
        .env_remove("SUMI_CONFIG")
        .env_remove("SUMI_ENV_FILE")
        .env("SUMI_KEY_PROVIDER", "env")
        .env("SUMI_AGENT_WRAPPING_KEY", WRAPPING_KEY)
        .env("SUMI_WORKSPACE", directory.join("workspace"))
        .env("SUMI_STATE_DIR", directory.join("state"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd
}

fn envelope(seq: u64, command_id: &str, command: serde_json::Value) -> String {
    serde_json::to_string(&serde_json::json!({
        "seq": seq,
        "command_id": command_id,
        "command": command,
    }))
    .unwrap()
        + "\n"
}

async fn send_commands(child: &mut Child, commands: &[String]) -> (Vec<serde_json::Value>, String) {
    let stdin = child.stdin.as_mut().unwrap();
    for line in commands {
        stdin.write_all(line.as_bytes()).await.unwrap();
    }
    stdin.shutdown().await.unwrap();

    let stdout = child.stdout.take().unwrap();
    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();
    let mut frames = Vec::new();
    while let Ok(Ok(Some(line))) =
        tokio::time::timeout(Duration::from_secs(5), lines.next_line()).await
    {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) {
            frames.push(value);
        }
    }

    let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap();
    let stderr = {
        let mut bytes = Vec::new();
        if let Some(mut stderr) = child.stderr.take() {
            use tokio::io::AsyncReadExt;
            let _ =
                tokio::time::timeout(Duration::from_secs(1), stderr.read_to_end(&mut bytes)).await;
        }
        String::from_utf8_lossy(&bytes).to_string()
    };
    assert!(status.success(), "agent exited with {status:?}: {stderr}");
    (frames, stderr)
}

fn find_ack(frames: &[serde_json::Value], seq: u64) -> Option<&serde_json::Value> {
    frames.iter().find(|f| {
        f.get("frame_type").and_then(|v| v.as_str()) == Some("command_ack")
            && f.get("ack")
                .and_then(|a| a.get("seq"))
                .and_then(|s| s.as_u64())
                == Some(seq)
    })
}

fn find_result<'a>(
    frames: &'a [serde_json::Value],
    command_id: &str,
) -> Option<&'a serde_json::Value> {
    frames.iter().find(|f| {
        f.get("frame_type").and_then(|v| v.as_str()) == Some("event")
            && f.get("envelope")
                .and_then(|e| e.get("event"))
                .and_then(|e| e.get("command"))
                .and_then(|c| c.as_str())
                == Some(command_id)
    })
}

#[tokio::test]
async fn lifecycle_export_and_search_return_applied_acks_and_empty_results() {
    let dir = test_directory();
    tokio::fs::create_dir(&dir).await.unwrap();
    let mut child = agent_command(&dir, Vec::new()).spawn().unwrap();

    let commands = vec![
        envelope(
            1,
            "00000000-0000-4000-8000-000000000001",
            serde_json::json!({"type": "export", "actor_id": "actor-1"}),
        ),
        envelope(
            2,
            "00000000-0000-4000-8000-000000000002",
            serde_json::json!({"type": "search", "actor_id": "actor-1", "query": "secret"}),
        ),
    ];
    let (frames, _stderr) = send_commands(&mut child, &commands).await;

    let ack1 = find_ack(&frames, 1).expect("export ack");
    assert_eq!(
        ack1.get("ack").unwrap().get("status").unwrap().as_str(),
        Some("applied")
    );
    let result1 =
        find_result(&frames, "00000000-0000-4000-8000-000000000001").expect("export event");
    assert_eq!(result1["envelope"]["event"]["data"].as_str().unwrap(), "");

    let ack2 = find_ack(&frames, 2).expect("search ack");
    assert_eq!(
        ack2.get("ack").unwrap().get("status").unwrap().as_str(),
        Some("applied")
    );

    tokio::fs::remove_dir_all(&dir).await.unwrap();
}

#[tokio::test]
async fn conversation_reset_persists_new_scope_and_restarts_cleanly() {
    let dir = test_directory();
    tokio::fs::create_dir(&dir).await.unwrap();

    // First epoch: reset the conversation.
    let mut child = agent_command(&dir, Vec::new()).spawn().unwrap();
    let commands = vec![envelope(
        1,
        "00000000-0000-4000-8000-000000000001",
        serde_json::json!({"type": "conversation_reset", "new_conversation_id": "conv-new-1"}),
    )];
    let (frames, _stderr) = send_commands(&mut child, &commands).await;
    let ack = find_ack(&frames, 1).expect("reset ack");
    assert_eq!(
        ack.get("ack").unwrap().get("status").unwrap().as_str(),
        Some("applied")
    );

    // Second epoch: start with the new conversation id and export.
    let mut child2 = agent_command(
        &dir,
        vec![("SUMI_CONVERSATION_ID", "conv-new-1".to_owned())],
    )
    .spawn()
    .unwrap();
    let commands2 = vec![envelope(
        1,
        "00000000-0000-4000-8000-000000000002",
        serde_json::json!({"type": "export", "actor_id": "actor-1"}),
    )];
    let (frames2, _stderr2) = send_commands(&mut child2, &commands2).await;
    let ack2 = find_ack(&frames2, 1).expect("export ack after reset");
    assert_eq!(
        ack2.get("ack").unwrap().get("status").unwrap().as_str(),
        Some("applied")
    );

    tokio::fs::remove_dir_all(&dir).await.unwrap();
}

#[tokio::test]
async fn conversation_reset_only_removes_target_conversation_artifacts() {
    let dir = test_directory();
    tokio::fs::create_dir(&dir).await.unwrap();

    // Pre-populate two conversation artifact directories.
    let artifact_root = dir.join("workspace").join("artifacts");
    let target = artifact_root.join("conv-1").join("attachments");
    let other = artifact_root.join("conv-2").join("attachments");
    tokio::fs::create_dir_all(&target).await.unwrap();
    tokio::fs::create_dir_all(&other).await.unwrap();
    tokio::fs::write(target.join("target.txt"), "target")
        .await
        .unwrap();
    tokio::fs::write(other.join("other.txt"), "other")
        .await
        .unwrap();

    let mut child = agent_command(&dir, vec![("SUMI_CONVERSATION_ID", "conv-1".to_owned())])
        .spawn()
        .unwrap();
    let commands = vec![envelope(
        1,
        "00000000-0000-4000-8000-000000000001",
        serde_json::json!({"type": "conversation_reset", "new_conversation_id": "conv-new-1"}),
    )];
    let (frames, _stderr) = send_commands(&mut child, &commands).await;
    let ack = find_ack(&frames, 1).expect("reset ack");
    assert_eq!(
        ack.get("ack").unwrap().get("status").unwrap().as_str(),
        Some("applied")
    );

    assert!(
        !artifact_root.join("conv-1").exists(),
        "target conversation artifacts were not removed"
    );
    assert!(
        artifact_root
            .join("conv-2")
            .join("attachments")
            .join("other.txt")
            .exists(),
        "other conversation artifacts were destroyed"
    );

    tokio::fs::remove_dir_all(&dir).await.unwrap();
}

#[tokio::test]
async fn delete_agent_removes_database_and_artifacts() {
    let dir = test_directory();
    tokio::fs::create_dir(&dir).await.unwrap();

    // Create an artifact file that should be removed.
    let artifact_root = dir.join("workspace").join("artifacts");
    tokio::fs::create_dir_all(&artifact_root).await.unwrap();
    tokio::fs::write(artifact_root.join("stale.txt"), "stale")
        .await
        .unwrap();

    let mut child = agent_command(&dir, Vec::new()).spawn().unwrap();
    let commands = vec![envelope(
        1,
        "00000000-0000-4000-8000-000000000001",
        serde_json::json!({"type": "delete_agent"}),
    )];
    let (frames, _stderr) = send_commands(&mut child, &commands).await;
    let ack = find_ack(&frames, 1).expect("delete ack");
    assert_eq!(
        ack.get("ack").unwrap().get("status").unwrap().as_str(),
        Some("applied")
    );

    let db_path = dir.join("state").join("agent.db");
    assert!(!db_path.exists(), "database file was not removed");
    assert!(!artifact_root.exists(), "artifact root was not removed");

    tokio::fs::remove_dir_all(&dir).await.ok();
}

#[tokio::test]
async fn rotate_keys_returns_applied_ack() {
    let dir = test_directory();
    tokio::fs::create_dir(&dir).await.unwrap();
    let mut child = agent_command(&dir, Vec::new()).spawn().unwrap();
    let commands = vec![envelope(
        1,
        "00000000-0000-4000-8000-000000000001",
        serde_json::json!({"type": "rotate_keys"}),
    )];
    let (frames, _stderr) = send_commands(&mut child, &commands).await;
    let ack = find_ack(&frames, 1).expect("rotate ack");
    assert_eq!(
        ack.get("ack").unwrap().get("status").unwrap().as_str(),
        Some("applied")
    );
    tokio::fs::remove_dir_all(&dir).await.unwrap();
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

async fn start_kms_server(state: Arc<KmsState>) -> String {
    let app = Router::new()
        .route("/v1/agents/{agent_id}/current-key", get(current_key))
        .route("/v1/keys/{key_id}/unwrap", post(unwrap_key))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    format!("http://127.0.0.1:{port}")
}

struct KmsState {
    current: Mutex<String>,
    keys: Mutex<HashMap<String, [u8; 32]>>,
    disabled: Mutex<HashSet<String>>,
}

async fn current_key(State(state): State<Arc<KmsState>>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "key_id": state.current.lock().unwrap().clone() }))
}

async fn unwrap_key(
    Path(key_id): Path<String>,
    State(state): State<Arc<KmsState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if state.disabled.lock().unwrap().contains(&key_id) {
        return Err(StatusCode::FORBIDDEN);
    }
    let keys = state.keys.lock().unwrap();
    let bytes = keys.get(&key_id).ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(serde_json::json!({ "plaintext_hex": to_hex(bytes) })))
}

#[tokio::test]
async fn kms_provider_selects_http_client_and_fail_closed_on_revoked_key() {
    let dir = test_directory();
    tokio::fs::create_dir(&dir).await.unwrap();

    let state = Arc::new(KmsState {
        current: Mutex::new("agent-key-v1".to_owned()),
        keys: Mutex::new(HashMap::from([("agent-key-v1".to_owned(), [0x44; 32])])),
        disabled: Mutex::new(HashSet::new()),
    });
    let kms_url = start_kms_server(state.clone()).await;

    // First epoch with a live KMS key: agent starts and exports successfully.
    let mut child = agent_command(
        &dir,
        vec![
            ("SUMI_KEY_PROVIDER", "kms".to_owned()),
            ("SUMI_KMS_URL", kms_url.clone()),
            ("SUMI_KMS_API_TOKEN", "test-token".to_owned()),
            ("SUMI_KMS_ALLOW_HTTP", "true".to_owned()),
            ("SUMI_KMS_AGENT_KEY_ID", "agent-key-v1".to_owned()),
        ],
    )
    .spawn()
    .unwrap();
    let commands = vec![envelope(
        1,
        "00000000-0000-4000-8000-000000000001",
        serde_json::json!({"type": "export", "actor_id": "actor-1"}),
    )];
    let (frames, _stderr) = send_commands(&mut child, &commands).await;
    let ack = find_ack(&frames, 1).expect("kms export ack");
    assert_eq!(
        ack.get("ack").unwrap().get("status").unwrap().as_str(),
        Some("applied")
    );

    // Revoke the key on the KMS side.
    state
        .disabled
        .lock()
        .unwrap()
        .insert("agent-key-v1".to_owned());

    // Second epoch with the revoked key: startup must fail closed before any command.
    let mut child2 = agent_command(
        &dir,
        vec![
            ("SUMI_KEY_PROVIDER", "kms".to_owned()),
            ("SUMI_KMS_URL", kms_url),
            ("SUMI_KMS_API_TOKEN", "test-token".to_owned()),
            ("SUMI_KMS_ALLOW_HTTP", "true".to_owned()),
            ("SUMI_KMS_AGENT_KEY_ID", "agent-key-v1".to_owned()),
        ],
    )
    .spawn()
    .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(5), child2.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !status.success(),
        "agent must fail closed on revoked KMS key"
    );

    tokio::fs::remove_dir_all(&dir).await.ok();
}
