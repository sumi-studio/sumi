use anyhow::{Context, Result};
use async_trait::async_trait;
use thiserror::Error;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Stdin, Stdout,
};

use super::{Command, Envelope, Gateway, GatewayClosed};

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

#[async_trait]
impl Gateway for StdioGateway {
    async fn next_command(&mut self) -> Result<Command> {
        read_command(&mut self.input).await
    }

    async fn send(&mut self, envelope: Envelope) -> Result<()> {
        let mut line = serde_json::to_vec(&envelope).context("failed to encode event JSON")?;
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

async fn read_command<R>(input: &mut R) -> Result<Command>
where
    R: AsyncBufRead + Unpin,
{
    let line = read_frame(input).await?;
    let command: Command = serde_json::from_slice(&line).map_err(InvalidCommand)?;
    if matches!(command, Command::UserMessage { .. }) && line.len() > MAX_USER_COMMAND_BYTES {
        return Err(CommandTooLarge {
            limit: MAX_USER_COMMAND_BYTES,
            actual: line.len(),
        }
        .into());
    }

    Ok(command)
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

    #[tokio::test]
    async fn reads_a_command_at_eof_without_newline() {
        let mut input = BufReader::new(r#"{"type":"abort"}"#.as_bytes());
        assert_eq!(
            read_command(&mut input).await.expect("valid command"),
            Command::Abort
        );
    }

    #[tokio::test]
    async fn reads_crlf_split_across_reader_buffers() {
        let mut input = BufReader::with_capacity(1, b"{\"type\":\"abort\"}\r\n".as_slice());
        assert_eq!(
            read_command(&mut input).await.expect("valid command"),
            Command::Abort
        );
    }

    #[tokio::test]
    async fn enforces_the_user_command_limit_below_the_transport_limit() {
        let command = serde_json::json!({
            "type": "user_message",
            "text": "x".repeat(MAX_USER_COMMAND_BYTES),
        })
        .to_string();
        assert!(command.len() < MAX_FRAME_BYTES);
        let mut input = BufReader::new(command.as_bytes());

        let error = read_command(&mut input)
            .await
            .expect_err("oversized user command must fail");
        assert_eq!(
            error.downcast_ref::<CommandTooLarge>(),
            Some(&CommandTooLarge {
                limit: MAX_USER_COMMAND_BYTES,
                actual: command.len(),
            })
        );
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
        bytes.extend_from_slice(b"\n{\"type\":\"abort\"}\n");
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
            Command::Abort
        );
    }
}
