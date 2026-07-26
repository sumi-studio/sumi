use std::{
    io::Write,
    path::Path,
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
};

use sqlx::{Connection, Row, sqlite::SqliteConnectOptions};

static DATABASE_ID: AtomicU64 = AtomicU64::new(1);

fn agent_command(database_path: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sumi-agent"));
    command
        .arg("--low-trust")
        .env_remove("SUMI_CONFIG")
        .env_remove("SUMI_ENV_FILE")
        .env(
            "SUMI_STATE_DIR",
            database_path
                .parent()
                .expect("database path has a state directory"),
        )
        .env(
            "SUMI_AGENT_WRAPPING_KEY",
            "4242424242424242424242424242424242424242424242424242424242424242",
        );
    command
}

fn fresh_database_path() -> std::path::PathBuf {
    std::env::temp_dir()
        .join(format!(
            "sumi-agent-stdio-{}-{}",
            std::process::id(),
            DATABASE_ID.fetch_add(1, Ordering::Relaxed)
        ))
        .join("agent.db")
}

fn run_agent_at(database_path: &Path, input: &[u8]) -> (bool, Vec<serde_json::Value>, String) {
    let mut child = agent_command(database_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start sumi-agent");

    let mut stdin = child.stdin.take().expect("child stdin");
    stdin.write_all(input).expect("write command stream");
    drop(stdin);

    let output = child.wait_with_output().expect("wait for sumi-agent");
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8 JSON lines");
    let frames = stdout
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("valid output frame"))
        .collect::<Vec<_>>();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    (output.status.success(), frames, stderr)
}

fn run_agent(input: &[u8]) -> (bool, Vec<serde_json::Value>, String) {
    let path = fresh_database_path();
    let result = run_agent_at(&path, input);
    std::fs::remove_dir_all(path.parent().expect("database state directory"))
        .expect("remove stdio fixture");
    result
}

fn assert_outer_protocol_violation_closes_epoch(first_frame: &[u8]) {
    let mut input = first_frame.to_vec();
    input.extend_from_slice(
        b"\n{\"seq\":2,\"command_id\":\"00000000-0000-4000-8000-000000000002\",\"command\":{\"type\":\"abort\"}}\n",
    );
    let (success, frames, stderr) = run_agent(&input);

    assert!(
        !success,
        "an invalid outer envelope is a protocol violation; stderr: {stderr}"
    );
    assert_eq!(
        frames.len(),
        1,
        "the later command must not be applied; stderr: {stderr}"
    );
    assert_eq!(frames[0]["frame_type"], "event", "stderr: {stderr}");
    assert_eq!(
        frames[0]["envelope"]["event"]["type"], "error",
        "stderr: {stderr}"
    );
    assert!(
        frames
            .iter()
            .all(|frame| frame["frame_type"] != "command_ack"),
        "no ACK can skip over the invalid outer frame; stderr: {stderr}"
    );
}

#[test]
fn malformed_envelope_closes_the_stdio_epoch_before_the_next_command() {
    assert_outer_protocol_violation_closes_epoch(br#"{"not":"closed""#);
}

#[test]
fn unknown_outer_field_closes_the_stdio_epoch_before_the_next_command() {
    assert_outer_protocol_violation_closes_epoch(
        br#"{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","extra":"rejected","command":{"type":"abort"}}"#,
    );
}

#[test]
fn invalid_command_uuid_closes_the_stdio_epoch_without_a_durable_rejection() {
    assert_outer_protocol_violation_closes_epoch(
        br#"{"seq":1,"command_id":"not-a-uuid","command":{"type":"abort"}}"#,
    );
}

#[test]
fn invalid_control_payloads_are_rejected_without_closing_the_epoch() {
    let (success, frames, stderr) = run_agent(
        br#"{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","command":{"type":"abort","extra":true}}
{"seq":2,"command_id":"00000000-0000-4000-8000-000000000002","command":{"type":"approval_decision","request_id":"request-1","decision":{"totally_unknown":true}}}
{"seq":3,"command_id":"00000000-0000-4000-8000-000000000003","command":{"type":"abort"}}
"#,
    );

    assert!(
        success,
        "typed command rejections keep the epoch readable; stderr: {stderr}"
    );
    assert_eq!(frames.len(), 4, "stderr: {stderr}");
    assert_eq!(frames[0]["ack"]["status"], "rejected", "stderr: {stderr}");
    assert_eq!(
        frames[0]["ack"]["reject_reason"], "schema_violation",
        "stderr: {stderr}"
    );
    assert_eq!(frames[1]["ack"]["status"], "rejected", "stderr: {stderr}");
    assert_eq!(
        frames[1]["ack"]["reject_reason"], "schema_violation",
        "stderr: {stderr}"
    );
    assert_eq!(frames[2]["ack"]["status"], "received", "stderr: {stderr}");
    assert_eq!(frames[3]["ack"]["status"], "applied", "stderr: {stderr}");
}

#[test]
fn live_user_message_then_abort_is_received_and_terminalized_in_one_epoch() {
    let (success, frames, stderr) = run_agent(
        br#"{"seq":1,"command_id":"00000000-0000-4000-8000-000000000051","command":{"type":"user_message","text":"start work","attachments":[]}}
{"seq":2,"command_id":"00000000-0000-4000-8000-000000000052","command":{"type":"abort"}}
"#,
    );

    assert!(
        success,
        "live admission must preserve the reserved Abort window; stderr: {stderr}"
    );
    assert_eq!(frames.len(), 4, "stderr: {stderr}");
    assert_eq!(frames[0]["ack"]["seq"], 1);
    assert_eq!(frames[0]["ack"]["status"], "received");
    assert_eq!(frames[1]["ack"]["seq"], 1);
    assert_eq!(frames[1]["ack"]["status"], "superseded");
    assert_eq!(frames[2]["ack"]["seq"], 2);
    assert_eq!(frames[2]["ack"]["status"], "received");
    assert_eq!(frames[3]["ack"]["seq"], 2);
    assert_eq!(frames[3]["ack"]["status"], "applied");
}

#[test]
fn malformed_command_value_is_durably_rejected_and_replays_the_same_ack() {
    let database_path = fresh_database_path();
    let input = b"{\"seq\":1,\"command_id\":\"00000000-0000-4000-8000-000000000036\",\"command\":{\"type\":\"abort\",}}\n";

    for attempt in 0..2 {
        let (success, frames, stderr) = run_agent_at(&database_path, input);
        assert!(
            success,
            "identity-readable malformed command attempt {attempt} must be terminal; stderr: {stderr}"
        );
        assert_eq!(frames.len(), 1, "stderr: {stderr}");
        assert_eq!(frames[0]["frame_type"], "command_ack");
        assert_eq!(frames[0]["ack"]["seq"], 1);
        assert_eq!(
            frames[0]["ack"]["command_id"],
            "00000000-0000-4000-8000-000000000036"
        );
        assert_eq!(frames[0]["ack"]["status"], "rejected");
        assert_eq!(
            frames[0]["ack"]["reject_reason"], "schema_violation",
            "stderr: {stderr}"
        );
    }

    std::fs::remove_dir_all(database_path.parent().expect("database state directory"))
        .expect("remove malformed command fixture");
}

#[test]
fn duplicate_command_keys_are_rejected_and_changed_raw_replay_fails_authentication() {
    let database_path = fresh_database_path();
    let original = b"{\"seq\":1,\"command_id\":\"00000000-0000-4000-8000-000000000043\",\"command\":{\"type\":\"user_message\",\"text\":\"original\",\"text\":\"stable\",\"attachments\":[]}}\n";

    for attempt in 0..2 {
        let (success, frames, stderr) = run_agent_at(&database_path, original);
        assert!(
            success,
            "duplicate-key command attempt {attempt} must be terminally rejected; stderr: {stderr}"
        );
        assert_eq!(frames.len(), 1, "stderr: {stderr}");
        assert_eq!(frames[0]["ack"]["seq"], 1);
        assert_eq!(
            frames[0]["ack"]["command_id"],
            "00000000-0000-4000-8000-000000000043"
        );
        assert_eq!(frames[0]["ack"]["status"], "rejected");
        assert_eq!(frames[0]["ack"]["reject_reason"], "schema_violation");
    }

    let changed = b"{\"seq\":1,\"command_id\":\"00000000-0000-4000-8000-000000000043\",\"command\":{\"type\":\"user_message\",\"text\":\"changed!\",\"text\":\"stable\",\"attachments\":[]}}\n";
    let (success, frames, stderr) = run_agent_at(&database_path, changed);
    assert!(
        !success,
        "changed first duplicate must not authenticate as the same command; stderr: {stderr}"
    );
    assert!(
        frames.is_empty(),
        "changed duplicate-key replay must not receive an ACK; stderr: {stderr}"
    );
    assert!(stderr.contains("digest mismatch"), "stderr: {stderr}");

    std::fs::remove_dir_all(database_path.parent().expect("database state directory"))
        .expect("remove duplicate-key fixture");
}

#[test]
fn oversized_command_replay_uses_incremental_keyed_digest_without_persisting_body() {
    let database_path = fresh_database_path();
    let command = |fill: char| {
        let mut frame = serde_json::to_vec(&serde_json::json!({
            "seq":1,
            "command_id":"00000000-0000-4000-8000-000000000010",
            "command":{
                "type":"user_message",
                "text":fill.to_string().repeat(1024 * 1024),
                "attachments":[],
            },
        }))
        .expect("serialize oversized command");
        frame.push(b'\n');
        frame
    };
    let original = command('x');
    let (success, frames, stderr) = run_agent_at(&database_path, &original);
    assert!(
        success,
        "oversized receipt must be terminal; stderr: {stderr}"
    );
    assert_eq!(frames.len(), 1, "stderr: {stderr}");
    assert_eq!(frames[0]["ack"]["status"], "rejected");
    assert_eq!(frames[0]["ack"]["reject_reason"], "oversized");
    assert!(
        !stderr.contains(&"x".repeat(256)),
        "initial rejection diagnostics must not contain payload bytes"
    );

    let (success, frames, stderr) = run_agent_at(&database_path, &original);
    assert!(success, "exact replay must succeed; stderr: {stderr}");
    assert_eq!(frames.len(), 1, "stderr: {stderr}");
    assert_eq!(frames[0]["ack"]["status"], "rejected");
    assert!(
        !stderr.contains(&"x".repeat(256)),
        "replay diagnostics must not contain payload bytes"
    );

    let changed = command('y');
    assert_eq!(changed.len(), original.len());
    let (success, frames, stderr) = run_agent_at(&database_path, &changed);
    assert!(
        !success,
        "same identity/size with changed bytes must fail; stderr: {stderr}"
    );
    assert!(
        frames.is_empty(),
        "digest mismatch must not ACK; stderr: {stderr}"
    );
    assert!(
        !stderr.contains(&"y".repeat(256)),
        "diagnostics must not contain rejected payload bytes"
    );

    std::fs::remove_dir_all(database_path.parent().expect("database state directory"))
        .expect("remove oversized fixture");
}

#[test]
fn ack_writer_failure_after_commit_replays_terminal_ack_from_fresh_process() {
    let database_path = fresh_database_path();
    let command = b"{\"seq\":1,\"command_id\":\"00000000-0000-4000-8000-000000000012\",\"command\":{\"type\":\"abort\"}}\n";
    let mut child = agent_command(&database_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start first agent epoch");
    let stdout = child.stdout.take().expect("first epoch stdout pipe");
    drop(stdout);
    let mut stdin = child.stdin.take().expect("first epoch stdin");
    stdin.write_all(command).expect("write Abort");
    drop(stdin);
    let first = child
        .wait_with_output()
        .expect("wait for failed ACK writer epoch");
    assert!(
        !first.status.success(),
        "closed ACK writer must fail after the durable commit"
    );

    let (success, frames, stderr) = run_agent_at(&database_path, command);
    assert!(success, "replay epoch must succeed; stderr: {stderr}");
    assert_eq!(frames.len(), 1, "stderr: {stderr}");
    assert_eq!(
        frames[0]["ack"]["command_id"],
        "00000000-0000-4000-8000-000000000012"
    );
    assert_eq!(frames[0]["ack"]["status"], "applied");

    std::fs::remove_dir_all(database_path.parent().expect("database state directory"))
        .expect("remove ACK replay fixture");
}

#[tokio::test]
async fn t15_recovery_gate_replays_t12_prefix_and_rejects_unseen_sequence_before_insert() {
    let database_path = fresh_database_path();
    let first = b"{\"seq\":1,\"command_id\":\"00000000-0000-4000-8000-000000000041\",\"command\":{\"type\":\"user_message\",\"text\":\"first\",\"attachments\":[]}}\n";
    let (success, frames, stderr) = run_agent_at(&database_path, first);
    assert!(success, "first epoch must stop normally; stderr: {stderr}");
    assert_eq!(frames.len(), 1, "stderr: {stderr}");
    assert_eq!(frames[0]["ack"]["status"], "received");

    let second = b"{\"seq\":1,\"command_id\":\"00000000-0000-4000-8000-000000000041\",\"command\":{\"type\":\"user_message\",\"text\":\"first\",\"attachments\":[]}}
{\"seq\":2,\"command_id\":\"00000000-0000-4000-8000-000000000042\",\"command\":{\"type\":\"user_message\",\"text\":\"second\",\"attachments\":[]}}\n";
    let (success, frames, stderr) = run_agent_at(&database_path, second);
    assert!(
        !success,
        "T15 recovery gate must close an epoch that introduces unseen work before T17 hydration; stderr: {stderr}"
    );
    assert_eq!(frames.len(), 1, "stderr: {stderr}");
    assert_eq!(frames[0]["ack"]["seq"], 1);
    assert_eq!(frames[0]["ack"]["status"], "received");
    assert!(
        stderr.contains("durable suffix recovery is required"),
        "stderr: {stderr}"
    );

    let options = SqliteConnectOptions::new()
        .filename(&database_path)
        .read_only(true);
    let mut connection = sqlx::SqliteConnection::connect_with(&options)
        .await
        .expect("open durable state");
    let rows = sqlx::query(
        "SELECT seq, status, application_kind, run_id, turn_id, run_phase
         FROM inbound_commands ORDER BY seq",
    )
    .fetch_all(&mut connection)
    .await
    .expect("read recovered commands");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("seq"), 1);
    assert_eq!(rows[0].get::<String, _>("status"), "applying");
    assert_eq!(
        rows[0]
            .get::<Option<String>, _>("application_kind")
            .as_deref(),
        Some("idle_run")
    );
    assert!(rows[0].get::<Option<String>, _>("run_id").is_some());
    assert!(rows[0].get::<Option<String>, _>("turn_id").is_some());
    assert_eq!(rows[0].get::<String, _>("run_phase"), "classified");
    let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
        .fetch_one(&mut connection)
        .await
        .expect("count events");
    assert_eq!(
        event_count, 0,
        "T12 prefix application must not emit T17-owned full-suffix run events"
    );
    connection.close().await.expect("close durable state");

    std::fs::remove_dir_all(database_path.parent().expect("database state directory"))
        .expect("remove restart fixture");
}
