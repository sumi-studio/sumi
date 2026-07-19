use anyhow::{Context, Result};
use async_trait::async_trait;
use thiserror::Error;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Stdin, Stdout,
};

use super::{
    CommandEnvelope, CommandRejectReason, Gateway, GatewayClosed, InboundCommand, OutboundFrame,
};

const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MAX_USER_COMMAND_BYTES: usize = 1024 * 1024;

#[derive(Debug, Error, PartialEq, Eq)]
#[error("command was {actual} bytes and exceeded the {limit} byte limit")]
struct CommandTooLarge {
    limit: usize,
    actual: usize,
}

#[derive(Debug, Error)]
#[error("invalid command JSON: {0}")]
pub struct InvalidCommand(#[source] serde_json::Error);

pub struct StdioGateway {
    input: BufReader<Stdin>,
    output: BufWriter<Stdout>,
}

impl StdioGateway {
    pub fn new() -> Self {
        Self {
            input: BufReader::new(tokio::io::stdin()),
            output: BufWriter::new(tokio::io::stdout()),
        }
    }
}

impl Default for StdioGateway {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Gateway for StdioGateway {
    async fn next_command(&mut self) -> Result<InboundCommand> {
        read_command(&mut self.input).await
    }

    async fn send(&mut self, frame: OutboundFrame) -> Result<()> {
        let mut line = serde_json::to_vec(&frame).context("failed to encode gateway frame JSON")?;
        line.push(b'\n');
        self.output
            .write_all(&line)
            .await
            .context("failed to write event to stdout")?;
        self.output
            .flush()
            .await
            .context("failed to flush event to stdout")
    }
}

#[derive(serde::Deserialize)]
struct RawCommandEnvelope {
    seq: u64,
    command_id: String,
    command: Option<Box<serde_json::value::RawValue>>,
}

async fn read_command<R>(input: &mut R) -> Result<InboundCommand>
where
    R: AsyncBufRead + Unpin,
{
    let line = read_frame(input).await?;
    let raw: RawCommandEnvelope = serde_json::from_slice(&line).map_err(InvalidCommand)?;
    let Some(raw_command) = raw.command else {
        return Ok(InboundCommand::Invalid {
            seq: raw.seq,
            command_id: raw.command_id,
            reason: CommandRejectReason::SchemaViolation,
        });
    };
    let command_bytes = raw_command.get().len();
    let command_value: serde_json::Value =
        serde_json::from_str(raw_command.get()).map_err(InvalidCommand)?;
    let command_type = command_value
        .get("type")
        .and_then(serde_json::Value::as_str);

    let reason = if command_type == Some("user_message") && command_bytes > MAX_USER_COMMAND_BYTES {
        Some(CommandRejectReason::Oversized {
            actual_bytes: command_bytes as u64,
        })
    } else if command_type == Some("user_message")
        && command_value
            .get("attachments")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|attachments| !attachments.is_empty())
    {
        Some(CommandRejectReason::AttachmentsNotEmpty)
    } else if command_type
        .is_some_and(|kind| !matches!(kind, "user_message" | "abort" | "approval_decision"))
    {
        Some(CommandRejectReason::UnknownCommand)
    } else {
        None
    };

    if let Some(reason) = reason {
        return Ok(InboundCommand::Invalid {
            seq: raw.seq,
            command_id: raw.command_id,
            reason,
        });
    }

    match serde_json::from_value(command_value) {
        Ok(command) => Ok(InboundCommand::Valid(CommandEnvelope {
            seq: raw.seq,
            command_id: raw.command_id,
            command,
        })),
        Err(_) => Ok(InboundCommand::Invalid {
            seq: raw.seq,
            command_id: raw.command_id,
            reason: CommandRejectReason::SchemaViolation,
        }),
    }
}

async fn read_frame<R>(input: &mut R) -> Result<Vec<u8>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::with_capacity(8 * 1024);
    let mut actual_bytes = 0usize;
    let mut terminator_bytes = 0usize;
    let mut previous_byte = None;
    let mut saw_input = false;

    loop {
        let available = input.fill_buf().await.context("failed to read command")?;
        if available.is_empty() {
            if !saw_input {
                return Err(GatewayClosed.into());
            }
            break;
        }
        saw_input = true;

        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        let segment = &available[..consumed];
        actual_bytes = actual_bytes.saturating_add(segment.len());

        let remaining = (MAX_FRAME_BYTES + 2).saturating_sub(line.len());
        line.extend_from_slice(&segment[..segment.len().min(remaining)]);

        if let Some(position) = newline {
            let before_newline = if position > 0 {
                Some(available[position - 1])
            } else {
                previous_byte
            };
            terminator_bytes = 1 + usize::from(before_newline == Some(b'\r'));
        } else {
            previous_byte = segment.last().copied();
        }
        input.consume(consumed);

        if newline.is_some() {
            break;
        }
    }

    let content_bytes = actual_bytes.saturating_sub(terminator_bytes);
    if content_bytes > MAX_FRAME_BYTES {
        return Err(CommandTooLarge {
            limit: MAX_FRAME_BYTES,
            actual: content_bytes,
        }
        .into());
    }
    line.truncate(content_bytes);
    Ok(line)
}

#[cfg(test)]
mod tests {
    use tokio::io::BufReader;

    use super::*;
    use crate::gateway::Command;

    fn envelope(command: serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "seq": 1,
            "command_id": "command-1",
            "command": command,
        }))
        .expect("serialize fixture")
    }

    #[tokio::test]
    async fn reads_a_command_at_eof_without_newline() {
        let bytes = envelope(serde_json::json!({"type": "abort"}));
        let mut input = BufReader::new(bytes.as_slice());
        assert_eq!(
            read_command(&mut input).await.expect("valid command"),
            InboundCommand::Valid(CommandEnvelope {
                seq: 1,
                command_id: "command-1".to_owned(),
                command: Command::Abort,
            })
        );
    }

    #[tokio::test]
    async fn reads_crlf_split_across_reader_buffers() {
        let mut bytes = envelope(serde_json::json!({"type": "abort"}));
        bytes.extend_from_slice(b"\r\n");
        let mut input = BufReader::with_capacity(1, bytes.as_slice());
        assert_eq!(
            read_command(&mut input).await.expect("valid command"),
            InboundCommand::Valid(CommandEnvelope {
                seq: 1,
                command_id: "command-1".to_owned(),
                command: Command::Abort,
            })
        );
    }

    #[tokio::test]
    async fn rejects_an_oversized_user_command_without_losing_its_identity() {
        let command = serde_json::json!({
            "type": "user_message",
            "text": "x".repeat(MAX_USER_COMMAND_BYTES),
            "attachments": [],
        });
        let command_bytes = serde_json::to_vec(&command).expect("serialize command");
        assert!(command_bytes.len() > MAX_USER_COMMAND_BYTES);
        let bytes = envelope(command);
        assert!(bytes.len() < MAX_FRAME_BYTES);
        let mut input = BufReader::new(bytes.as_slice());

        let inbound = read_command(&mut input)
            .await
            .expect("outer envelope remains valid");
        assert_eq!(
            inbound,
            InboundCommand::Invalid {
                seq: 1,
                command_id: "command-1".to_owned(),
                reason: CommandRejectReason::Oversized {
                    actual_bytes: command_bytes.len() as u64,
                },
            }
        );
    }

    #[tokio::test]
    async fn classifies_non_empty_attachments_for_terminal_rejection() {
        let bytes = envelope(serde_json::json!({
            "type": "user_message",
            "text": "inspect this",
            "attachments": [{"name": "secret.txt"}],
        }));
        let mut input = BufReader::new(bytes.as_slice());

        assert_eq!(
            read_command(&mut input)
                .await
                .expect("outer envelope remains valid"),
            InboundCommand::Invalid {
                seq: 1,
                command_id: "command-1".to_owned(),
                reason: CommandRejectReason::AttachmentsNotEmpty,
            }
        );
    }

    #[tokio::test]
    async fn retains_identity_for_unknown_and_missing_commands() {
        let unknown = envelope(serde_json::json!({"type": "future_command"}));
        let mut input = BufReader::new(unknown.as_slice());
        assert_eq!(
            read_command(&mut input)
                .await
                .expect("unknown typed command"),
            InboundCommand::Invalid {
                seq: 1,
                command_id: "command-1".to_owned(),
                reason: CommandRejectReason::UnknownCommand,
            }
        );

        let missing = br#"{"seq":2,"command_id":"command-2"}"#;
        let mut input = BufReader::new(missing.as_slice());
        assert_eq!(
            read_command(&mut input)
                .await
                .expect("missing command body"),
            InboundCommand::Invalid {
                seq: 2,
                command_id: "command-2".to_owned(),
                reason: CommandRejectReason::SchemaViolation,
            }
        );
    }

    #[tokio::test]
    async fn measures_the_raw_command_bytes_before_json_compaction() {
        let whitespace = " ".repeat(MAX_USER_COMMAND_BYTES);
        let input = format!(
            r#"{{"seq":1,"command_id":"command-1","command":{{{whitespace}"type":"user_message","text":"small","attachments":[]}}}}"#
        );
        assert!(input.len() < MAX_FRAME_BYTES);
        let mut input = BufReader::new(input.as_bytes());

        assert!(matches!(
            read_command(&mut input)
                .await
                .expect("outer envelope remains valid"),
            InboundCommand::Invalid {
                seq: 1,
                reason: CommandRejectReason::Oversized { .. },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn rejects_an_oversized_command_line() {
        let mut bytes = vec![b' '; MAX_FRAME_BYTES + 1];
        bytes.push(b'\n');
        let mut input = BufReader::new(bytes.as_slice());
        let error = read_command(&mut input)
            .await
            .expect_err("oversized input must fail");
        assert_eq!(
            error.downcast_ref::<CommandTooLarge>(),
            Some(&CommandTooLarge {
                limit: MAX_FRAME_BYTES,
                actual: MAX_FRAME_BYTES + 1,
            })
        );
    }

    #[tokio::test]
    async fn drains_an_oversized_frame_before_reading_the_next_command() {
        let mut bytes = vec![b' '; MAX_FRAME_BYTES + 100];
        bytes.push(b'\n');
        bytes.extend_from_slice(&envelope(serde_json::json!({"type": "abort"})));
        bytes.push(b'\n');
        let mut input = BufReader::new(bytes.as_slice());

        let error = read_command(&mut input)
            .await
            .expect_err("oversized input must fail");
        assert_eq!(
            error.downcast_ref::<CommandTooLarge>(),
            Some(&CommandTooLarge {
                limit: MAX_FRAME_BYTES,
                actual: MAX_FRAME_BYTES + 100,
            })
        );
        assert_eq!(
            read_command(&mut input)
                .await
                .expect("reader resynchronizes at the next line"),
            InboundCommand::Valid(CommandEnvelope {
                seq: 1,
                command_id: "command-1".to_owned(),
                command: Command::Abort,
            })
        );
    }

    #[tokio::test]
    async fn continues_after_an_oversized_user_command() {
        let mut bytes = envelope(serde_json::json!({
            "type": "user_message",
            "text": "x".repeat(MAX_USER_COMMAND_BYTES),
            "attachments": [],
        }));
        bytes.push(b'\n');
        bytes.extend_from_slice(&envelope(serde_json::json!({"type": "abort"})));
        bytes.push(b'\n');
        let mut input = BufReader::new(bytes.as_slice());

        assert!(matches!(
            read_command(&mut input)
                .await
                .expect("oversized command is a typed rejection"),
            InboundCommand::Invalid {
                reason: CommandRejectReason::Oversized { .. },
                ..
            }
        ));
        assert_eq!(
            read_command(&mut input)
                .await
                .expect("next command remains readable"),
            InboundCommand::Valid(CommandEnvelope {
                seq: 1,
                command_id: "command-1".to_owned(),
                command: Command::Abort,
            })
        );
    }
}
