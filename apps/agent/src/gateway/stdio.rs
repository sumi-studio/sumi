use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Lines, Stdin, Stdout};

use super::{Command, Envelope, Gateway, GatewayClosed};

pub struct StdioGateway {
    input: Lines<BufReader<Stdin>>,
    output: BufWriter<Stdout>,
}

impl StdioGateway {
    pub fn new() -> Self {
        Self {
            input: BufReader::new(tokio::io::stdin()).lines(),
            output: BufWriter::new(tokio::io::stdout()),
        }
    }
}

#[async_trait]
impl Gateway for StdioGateway {
    async fn next_command(&mut self) -> Result<Command> {
        let line = self
            .input
            .next_line()
            .await
            .context("failed to read command from stdin")?
            .ok_or(GatewayClosed)?;

        serde_json::from_str(&line).context("failed to decode command JSON")
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
