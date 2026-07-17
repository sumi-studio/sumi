use anyhow::{Context, Result};
use async_trait::async_trait;
use thiserror::Error;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, Stdin, Stdout,
};

use super::{Command, Envelope, Gateway, GatewayClosed};

const MAX_COMMAND_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Error, PartialEq, Eq)]
#[error("command exceeded {limit} bytes")]
struct CommandTooLarge {
    limit: usize,
}

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
    let mut line = Vec::new();
    let mut bounded = input.take((MAX_COMMAND_BYTES + 2) as u64);
    let bytes_read = bounded
        .read_until(b'\n', &mut line)
        .await
        .context("failed to read command")?;
    if bytes_read == 0 {
        return Err(GatewayClosed.into());
    }

    if line.last() == Some(&b'\n') {
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
    }
    if line.len() > MAX_COMMAND_BYTES {
        return Err(CommandTooLarge {
            limit: MAX_COMMAND_BYTES,
        }
        .into());
    }

    serde_json::from_slice(&line).context("failed to decode command JSON")
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
    async fn rejects_an_oversized_command_line() {
        let mut bytes = vec![b' '; MAX_COMMAND_BYTES + 1];
        bytes.push(b'\n');
        let mut input = BufReader::new(bytes.as_slice());
        let error = read_command(&mut input)
            .await
            .expect_err("oversized input must fail");
        assert_eq!(
            error.downcast_ref::<CommandTooLarge>(),
            Some(&CommandTooLarge {
                limit: MAX_COMMAND_BYTES
            })
        );
    }
}
