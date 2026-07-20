use std::{
    io::Write,
    process::{Command, Stdio},
};

fn run_agent(input: &[u8]) -> (bool, Vec<serde_json::Value>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_sumi-agent"))
        .env_remove("SUMI_CONFIG")
        .env_remove("SUMI_ENV_FILE")
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
    (output.status.success(), frames)
}

fn assert_outer_protocol_violation_closes_epoch(first_frame: &[u8]) {
    let mut input = first_frame.to_vec();
    input.extend_from_slice(
        b"\n{\"seq\":2,\"command_id\":\"command-2\",\"command\":{\"type\":\"abort\"}}\n",
    );
    let (success, frames) = run_agent(&input);

    assert!(
        !success,
        "an invalid outer envelope is a protocol violation"
    );
    assert_eq!(frames.len(), 1, "the later command must not be applied");
    assert_eq!(frames[0]["frame_type"], "event");
    assert_eq!(frames[0]["envelope"]["event"]["type"], "error");
    assert!(
        frames
            .iter()
            .all(|frame| frame["frame_type"] != "command_ack"),
        "no ACK can skip over the invalid outer frame"
    );
}

#[test]
fn malformed_envelope_closes_the_stdio_epoch_before_the_next_command() {
    assert_outer_protocol_violation_closes_epoch(br#"{"not":"closed""#);
}

#[test]
fn unknown_outer_field_closes_the_stdio_epoch_before_the_next_command() {
    assert_outer_protocol_violation_closes_epoch(
        br#"{"seq":1,"command_id":"command-1","extra":"rejected","command":{"type":"abort"}}"#,
    );
}

#[test]
fn invalid_control_payloads_are_rejected_without_closing_the_epoch() {
    let (success, frames) = run_agent(
        br#"{"seq":1,"command_id":"command-1","command":{"type":"abort","extra":true}}
{"seq":2,"command_id":"command-2","command":{"type":"approval_decision","request_id":"request-1","decision":{"totally_unknown":true}}}
{"seq":3,"command_id":"command-3","command":{"type":"abort"}}
"#,
    );

    assert!(success, "typed command rejections keep the epoch readable");
    assert_eq!(frames.len(), 3);
    assert_eq!(frames[0]["ack"]["status"], "rejected");
    assert_eq!(frames[0]["ack"]["reject_reason"], "schema_violation");
    assert_eq!(frames[1]["ack"]["status"], "rejected");
    assert_eq!(frames[1]["ack"]["reject_reason"], "schema_violation");
    assert_eq!(frames[2]["ack"]["status"], "applied");
}
