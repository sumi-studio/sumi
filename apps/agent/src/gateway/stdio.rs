use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use serde::{
    Deserialize, Deserializer,
    de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor},
};
use thiserror::Error;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter, Stdin, Stdout,
};
use zeroize::{Zeroize, Zeroizing};

use super::{
    CommandDigestFactory, CommandEnvelope, CommandId, CommandRejectReason, Gateway, GatewayClosed,
    GatewayReader, GatewayWriter, InboundCommand, IncrementalCommandDigest, KeyedCommandDigest,
    OutboundFrame, RejectedCommandPayload, SensitiveCommandPayload,
};

const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MAX_USER_COMMAND_BYTES: usize = 1024 * 1024;
const MAX_ENVELOPE_METADATA_BYTES: usize = 64 * 1024;
const MAX_ENVELOPE_KEY_BYTES: usize = 256;

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
    command_digest_factory: Arc<dyn CommandDigestFactory>,
}

pub struct StdioGatewayReader {
    input: BufReader<Stdin>,
    command_digest_factory: Arc<dyn CommandDigestFactory>,
}

pub struct StdioGatewayWriter {
    output: BufWriter<Stdout>,
}

/// Injected JSON-lines transport used by the T15 loop harness. Production
/// stdin/stdout ownership remains in [`StdioGateway`].
#[allow(
    dead_code,
    reason = "constructed by injected harnesses until T26 bootstrap"
)]
pub(crate) struct InjectedStdioGateway<R, W> {
    input: R,
    output: W,
    command_digest_factory: Arc<dyn CommandDigestFactory>,
}

#[allow(dead_code, reason = "associated injected gateway half")]
pub(crate) struct InjectedStdioGatewayReader<R> {
    input: R,
    command_digest_factory: Arc<dyn CommandDigestFactory>,
}

#[allow(dead_code, reason = "associated injected gateway half")]
pub(crate) struct InjectedStdioGatewayWriter<W> {
    output: W,
}

impl<R, W> InjectedStdioGateway<R, W> {
    #[allow(
        dead_code,
        reason = "constructed by injected harnesses until T26 bootstrap"
    )]
    pub(crate) fn new(
        input: R,
        output: W,
        command_digest_factory: Arc<dyn CommandDigestFactory>,
    ) -> Self {
        Self {
            input,
            output,
            command_digest_factory,
        }
    }
}

impl<R, W> Gateway for InjectedStdioGateway<R, W>
where
    R: AsyncBufRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    type Reader = InjectedStdioGatewayReader<R>;
    type Writer = InjectedStdioGatewayWriter<W>;

    fn split(self) -> (Self::Reader, Self::Writer) {
        (
            InjectedStdioGatewayReader {
                input: self.input,
                command_digest_factory: self.command_digest_factory,
            },
            InjectedStdioGatewayWriter {
                output: self.output,
            },
        )
    }
}

#[async_trait]
impl<R> GatewayReader for InjectedStdioGatewayReader<R>
where
    R: AsyncBufRead + Unpin + Send,
{
    async fn next_command(&mut self) -> Result<InboundCommand> {
        read_command(&mut self.input, self.command_digest_factory.as_ref()).await
    }
}

#[async_trait]
impl<W> GatewayWriter for InjectedStdioGatewayWriter<W>
where
    W: AsyncWrite + Unpin + Send,
{
    async fn send(&mut self, frame: OutboundFrame) -> Result<()> {
        write_frame(&mut self.output, frame).await
    }
}

impl StdioGateway {
    pub(crate) fn new(command_digest_factory: Arc<dyn CommandDigestFactory>) -> Self {
        Self {
            input: BufReader::new(tokio::io::stdin()),
            output: BufWriter::new(tokio::io::stdout()),
            command_digest_factory,
        }
    }
}

impl Gateway for StdioGateway {
    type Reader = StdioGatewayReader;
    type Writer = StdioGatewayWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        (
            StdioGatewayReader {
                input: self.input,
                command_digest_factory: self.command_digest_factory,
            },
            StdioGatewayWriter {
                output: self.output,
            },
        )
    }
}

#[async_trait]
impl GatewayReader for StdioGatewayReader {
    async fn next_command(&mut self) -> Result<InboundCommand> {
        read_command(&mut self.input, self.command_digest_factory.as_ref()).await
    }
}

#[async_trait]
impl GatewayWriter for StdioGatewayWriter {
    async fn send(&mut self, frame: OutboundFrame) -> Result<()> {
        write_frame(&mut self.output, frame).await
    }
}

async fn write_frame<W: AsyncWrite + Unpin>(output: &mut W, frame: OutboundFrame) -> Result<()> {
    let mut line = serde_json::to_vec(&frame).context("failed to encode gateway frame JSON")?;
    line.push(b'\n');
    output
        .write_all(&line)
        .await
        .context("failed to write event to stdout")?;
    output
        .flush()
        .await
        .context("failed to flush event to stdout")
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCommandIdentityEnvelope {
    seq: u64,
    command_id: CommandId,
    #[serde(default)]
    command: CommandFieldPresence,
}

#[derive(Default)]
enum CommandFieldPresence {
    #[default]
    Missing,
    Present,
}

impl<'de> Deserialize<'de> for CommandFieldPresence {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        IgnoredAny::deserialize(deserializer)?;
        Ok(Self::Present)
    }
}

async fn read_command<R>(
    input: &mut R,
    digest_factory: &dyn CommandDigestFactory,
) -> Result<InboundCommand>
where
    R: AsyncBufRead + Unpin,
{
    let mut frame = read_frame(input, digest_factory.start()).await?;
    let raw: RawCommandIdentityEnvelope =
        serde_json::from_slice(&frame.identity).map_err(InvalidCommand)?;
    if matches!(raw.command, CommandFieldPresence::Missing) || !frame.command_found {
        return Ok(InboundCommand::Invalid {
            seq: raw.seq,
            command_id: raw.command_id,
            reason: CommandRejectReason::SchemaViolation,
            raw_command: RejectedCommandPayload::Missing,
            payload_digest: None,
        });
    }
    let command_bytes = frame.command_bytes;
    if command_bytes > MAX_USER_COMMAND_BYTES {
        return Ok(InboundCommand::Invalid {
            seq: raw.seq,
            command_id: raw.command_id,
            reason: CommandRejectReason::Oversized {
                actual_bytes: command_bytes as u64,
            },
            raw_command: RejectedCommandPayload::DiscardedOversized,
            payload_digest: Some(frame.finish_digest()?),
        });
    }

    let raw_command = frame
        .command
        .take()
        .ok_or_else(|| anyhow!("valid-size command payload was not retained"))?;
    let sensitive_payload = SensitiveCommandPayload::new(raw_command.to_vec());
    let command_value = match parse_command_value(&raw_command) {
        Ok(value) => value,
        Err(_) => {
            return Ok(InboundCommand::Invalid {
                seq: raw.seq,
                command_id: raw.command_id,
                reason: CommandRejectReason::SchemaViolation,
                raw_command: RejectedCommandPayload::Present(sensitive_payload),
                payload_digest: None,
            });
        }
    };
    let command_type = command_value
        .get("type")
        .and_then(serde_json::Value::as_str);

    let reason = if command_type == Some("user_message")
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
            raw_command: RejectedCommandPayload::Present(sensitive_payload),
            payload_digest: None,
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
            raw_command: RejectedCommandPayload::Present(sensitive_payload),
            payload_digest: None,
        }),
    }
}

fn parse_command_value(bytes: &[u8]) -> serde_json::Result<serde_json::Value> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = DuplicateCheckedValue::deserialize(&mut deserializer)?.0;
    deserializer.end()?;
    Ok(value)
}

struct DuplicateCheckedValue(serde_json::Value);

impl<'de> Deserialize<'de> for DuplicateCheckedValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(DuplicateCheckedValueVisitor)
    }
}

struct DuplicateCheckedValueVisitor;

impl<'de> Visitor<'de> for DuplicateCheckedValueVisitor {
    type Value = DuplicateCheckedValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E> {
        Ok(DuplicateCheckedValue(value.into()))
    }

    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
        Ok(DuplicateCheckedValue(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
        Ok(DuplicateCheckedValue(value.into()))
    }

    fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .map(DuplicateCheckedValue)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E> {
        Ok(DuplicateCheckedValue(value.into()))
    }

    fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E> {
        Ok(DuplicateCheckedValue(value.into()))
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(DuplicateCheckedValue(serde_json::Value::Null))
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(DuplicateCheckedValue(serde_json::Value::Null))
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or_default());
        while let Some(value) = sequence.next_element::<DuplicateCheckedValue>()? {
            values.push(value.0);
        }
        Ok(DuplicateCheckedValue(serde_json::Value::Array(values)))
    }

    fn visit_map<A>(self, mut object: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = serde_json::Map::with_capacity(object.size_hint().unwrap_or_default());
        while let Some(key) = object.next_key::<String>()? {
            if values.contains_key(&key) {
                return Err(de::Error::custom(format_args!(
                    "duplicate object key {key:?}"
                )));
            }
            let value = object.next_value::<DuplicateCheckedValue>()?;
            values.insert(key, value.0);
        }
        Ok(DuplicateCheckedValue(serde_json::Value::Object(values)))
    }
}

struct ReadCommandFrame {
    identity: Zeroizing<Vec<u8>>,
    command: Option<Zeroizing<Vec<u8>>>,
    command_bytes: usize,
    command_found: bool,
    digest: Option<Box<dyn IncrementalCommandDigest>>,
}

impl ReadCommandFrame {
    fn finish_digest(&mut self) -> Result<KeyedCommandDigest> {
        self.digest
            .take()
            .map(IncrementalCommandDigest::finish)
            .ok_or_else(|| anyhow!("command digest was already consumed"))
    }
}

enum EnvelopeState {
    Start,
    KeyOrEnd,
    Key {
        raw: Zeroizing<Vec<u8>>,
        escaped: bool,
    },
    Colon {
        command: bool,
    },
    ValueStart {
        command: bool,
    },
    Value {
        command: bool,
        tracker: JsonValueTracker,
    },
    CommaOrEnd,
    Done,
    Invalid,
}

enum JsonValueTracker {
    Compound {
        depth: usize,
        in_string: bool,
        escaped: bool,
    },
    String {
        escaped: bool,
    },
    Primitive,
}

enum ValueProgress {
    Continue,
    Complete,
    CompleteBefore,
}

impl JsonValueTracker {
    fn from_first(byte: u8) -> Self {
        match byte {
            b'{' | b'[' => Self::Compound {
                depth: 1,
                in_string: false,
                escaped: false,
            },
            b'"' => Self::String { escaped: false },
            _ => Self::Primitive,
        }
    }

    fn feed(&mut self, byte: u8) -> ValueProgress {
        match self {
            Self::Compound {
                depth,
                in_string,
                escaped,
            } => {
                if *in_string {
                    if *escaped {
                        *escaped = false;
                    } else if byte == b'\\' {
                        *escaped = true;
                    } else if byte == b'"' {
                        *in_string = false;
                    }
                    return ValueProgress::Continue;
                }
                match byte {
                    b'"' => *in_string = true,
                    b'{' | b'[' => *depth = depth.saturating_add(1),
                    b'}' | b']' => {
                        *depth = depth.saturating_sub(1);
                        if *depth == 0 {
                            return ValueProgress::Complete;
                        }
                    }
                    _ => {}
                }
                ValueProgress::Continue
            }
            Self::String { escaped } => {
                if *escaped {
                    *escaped = false;
                } else if byte == b'\\' {
                    *escaped = true;
                } else if byte == b'"' {
                    return ValueProgress::Complete;
                }
                ValueProgress::Continue
            }
            Self::Primitive => {
                if byte.is_ascii_whitespace() || matches!(byte, b',' | b'}') {
                    ValueProgress::CompleteBefore
                } else {
                    ValueProgress::Continue
                }
            }
        }
    }
}

struct EnvelopeScanner {
    state: EnvelopeState,
    identity: Zeroizing<Vec<u8>>,
    command: Option<Zeroizing<Vec<u8>>>,
    command_bytes: usize,
    command_found: bool,
    digest: Box<dyn IncrementalCommandDigest>,
}

impl EnvelopeScanner {
    fn new(digest: Box<dyn IncrementalCommandDigest>) -> Self {
        Self {
            state: EnvelopeState::Start,
            identity: Zeroizing::new(Vec::with_capacity(8 * 1024)),
            command: None,
            command_bytes: 0,
            command_found: false,
            digest,
        }
    }

    fn feed(&mut self, bytes: &[u8]) -> Result<()> {
        for byte in bytes {
            self.feed_byte(*byte)?;
        }
        Ok(())
    }

    fn feed_byte(&mut self, byte: u8) -> Result<()> {
        let mut reprocess = true;
        while reprocess {
            reprocess = false;
            let state = std::mem::replace(&mut self.state, EnvelopeState::Invalid);
            self.state = match state {
                EnvelopeState::Start if byte.is_ascii_whitespace() => {
                    self.push_identity(byte)?;
                    EnvelopeState::Start
                }
                EnvelopeState::Start if byte == b'{' => {
                    self.push_identity(byte)?;
                    EnvelopeState::KeyOrEnd
                }
                EnvelopeState::KeyOrEnd if byte.is_ascii_whitespace() => {
                    self.push_identity(byte)?;
                    EnvelopeState::KeyOrEnd
                }
                EnvelopeState::KeyOrEnd if byte == b'"' => {
                    self.push_identity(byte)?;
                    EnvelopeState::Key {
                        raw: Zeroizing::new(vec![byte]),
                        escaped: false,
                    }
                }
                EnvelopeState::KeyOrEnd if byte == b'}' => {
                    self.push_identity(byte)?;
                    EnvelopeState::Done
                }
                EnvelopeState::Key { mut raw, escaped } => {
                    self.push_identity(byte)?;
                    if raw.len() == MAX_ENVELOPE_KEY_BYTES {
                        self.invalidate()?;
                        EnvelopeState::Invalid
                    } else {
                        raw.push(byte);
                        if escaped {
                            EnvelopeState::Key {
                                raw,
                                escaped: false,
                            }
                        } else if byte == b'\\' {
                            EnvelopeState::Key { raw, escaped: true }
                        } else if byte == b'"' {
                            let key: String = match serde_json::from_slice(&raw) {
                                Ok(key) => key,
                                Err(_) => {
                                    self.invalidate()?;
                                    return Ok(());
                                }
                            };
                            EnvelopeState::Colon {
                                command: key == "command",
                            }
                        } else {
                            EnvelopeState::Key {
                                raw,
                                escaped: false,
                            }
                        }
                    }
                }
                EnvelopeState::Colon { command } if byte.is_ascii_whitespace() => {
                    self.push_identity(byte)?;
                    EnvelopeState::Colon { command }
                }
                EnvelopeState::Colon { command } if byte == b':' => {
                    self.push_identity(byte)?;
                    EnvelopeState::ValueStart { command }
                }
                EnvelopeState::ValueStart { command } if byte.is_ascii_whitespace() => {
                    self.push_identity(byte)?;
                    EnvelopeState::ValueStart { command }
                }
                EnvelopeState::ValueStart { command } => {
                    if command {
                        if self.command_found {
                            self.invalidate()?;
                            EnvelopeState::Invalid
                        } else {
                            self.command_found = true;
                            self.command = Some(Zeroizing::new(Vec::with_capacity(
                                MAX_USER_COMMAND_BYTES.min(8 * 1024),
                            )));
                            self.push_identity(b'{')?;
                            self.push_identity(b'}')?;
                            self.capture_command_byte(byte);
                            EnvelopeState::Value {
                                command: true,
                                tracker: JsonValueTracker::from_first(byte),
                            }
                        }
                    } else {
                        self.push_identity(byte)?;
                        EnvelopeState::Value {
                            command: false,
                            tracker: JsonValueTracker::from_first(byte),
                        }
                    }
                }
                EnvelopeState::Value {
                    command,
                    mut tracker,
                } => match tracker.feed(byte) {
                    ValueProgress::Continue => {
                        if command {
                            self.capture_command_byte(byte);
                        } else {
                            self.push_identity(byte)?;
                        }
                        EnvelopeState::Value { command, tracker }
                    }
                    ValueProgress::Complete => {
                        if command {
                            self.capture_command_byte(byte);
                        } else {
                            self.push_identity(byte)?;
                        }
                        EnvelopeState::CommaOrEnd
                    }
                    ValueProgress::CompleteBefore => {
                        self.state = EnvelopeState::CommaOrEnd;
                        reprocess = true;
                        continue;
                    }
                },
                EnvelopeState::CommaOrEnd if byte.is_ascii_whitespace() => {
                    self.push_identity(byte)?;
                    EnvelopeState::CommaOrEnd
                }
                EnvelopeState::CommaOrEnd if byte == b',' => {
                    self.push_identity(byte)?;
                    EnvelopeState::KeyOrEnd
                }
                EnvelopeState::CommaOrEnd if byte == b'}' => {
                    self.push_identity(byte)?;
                    EnvelopeState::Done
                }
                EnvelopeState::Done if byte.is_ascii_whitespace() => {
                    self.push_identity(byte)?;
                    EnvelopeState::Done
                }
                EnvelopeState::Invalid => EnvelopeState::Invalid,
                _ => {
                    self.invalidate()?;
                    EnvelopeState::Invalid
                }
            };
        }
        Ok(())
    }

    fn capture_command_byte(&mut self, byte: u8) {
        self.command_bytes = self.command_bytes.saturating_add(1);
        self.digest.update(std::slice::from_ref(&byte));
        if self.command_bytes <= MAX_USER_COMMAND_BYTES {
            if let Some(command) = &mut self.command {
                command.push(byte);
            }
        } else if let Some(mut command) = self.command.take() {
            command.zeroize();
        }
    }

    fn push_identity(&mut self, byte: u8) -> Result<()> {
        if self.identity.len() == MAX_ENVELOPE_METADATA_BYTES {
            bail!(
                "command envelope metadata exceeded {} bytes",
                MAX_ENVELOPE_METADATA_BYTES
            );
        }
        self.identity.push(byte);
        Ok(())
    }

    fn invalidate(&mut self) -> Result<()> {
        self.push_identity(b'!')?;
        if let Some(mut command) = self.command.take() {
            command.zeroize();
        }
        Ok(())
    }

    fn finish(mut self) -> Result<ReadCommandFrame> {
        if !matches!(self.state, EnvelopeState::Done) {
            self.invalidate()?;
        }
        Ok(ReadCommandFrame {
            identity: self.identity,
            command: self.command,
            command_bytes: self.command_bytes,
            command_found: self.command_found,
            digest: Some(self.digest),
        })
    }
}

async fn read_frame<R>(
    input: &mut R,
    digest: Box<dyn IncrementalCommandDigest>,
) -> Result<ReadCommandFrame>
where
    R: AsyncBufRead + Unpin,
{
    let mut scanner = EnvelopeScanner::new(digest);
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
        scanner.feed(&segment[..newline.unwrap_or(segment.len())])?;

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
        let minimum_content_bytes =
            actual_bytes.saturating_sub(usize::from(previous_byte == Some(b'\r')));
        if minimum_content_bytes > MAX_FRAME_BYTES {
            return Err(CommandTooLarge {
                limit: MAX_FRAME_BYTES,
                actual: minimum_content_bytes,
            }
            .into());
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
    scanner.finish()
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};
    use tokio::io::{AsyncWriteExt, BufReader};

    use super::*;
    use crate::gateway::Command;

    struct TestDigestFactory;

    impl CommandDigestFactory for TestDigestFactory {
        fn start(&self) -> Box<dyn IncrementalCommandDigest> {
            Box::new(TestDigest(Sha256::new()))
        }
    }

    struct TestDigest(Sha256);

    impl IncrementalCommandDigest for TestDigest {
        fn update(&mut self, bytes: &[u8]) {
            Digest::update(&mut self.0, bytes);
        }

        fn finish(self: Box<Self>) -> KeyedCommandDigest {
            KeyedCommandDigest::new("test-command-key", self.0.finalize().into())
        }
    }

    async fn read_test_command<R>(input: &mut R) -> Result<InboundCommand>
    where
        R: AsyncBufRead + Unpin,
    {
        read_command(input, &TestDigestFactory).await
    }

    fn frame_with_total_bytes(total_bytes: usize) -> Vec<u8> {
        let prefix =
            br#"{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","command":{"type":"user_message","text":""#;
        let suffix = br#"","attachments":[]}}"#;
        assert!(total_bytes >= prefix.len() + suffix.len());
        let mut frame = Vec::with_capacity(total_bytes);
        frame.extend_from_slice(prefix);
        frame.resize(total_bytes - suffix.len(), b'x');
        frame.extend_from_slice(suffix);
        frame
    }

    fn envelope(command: serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "seq": 1,
            "command_id": "00000000-0000-4000-8000-000000000001",
            "command": command,
        }))
        .expect("serialize fixture")
    }

    fn raw_envelope(command: &str) -> Vec<u8> {
        format!(
            r#"{{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","command":{command}}}"#
        )
        .into_bytes()
    }

    #[tokio::test]
    async fn reads_a_command_at_eof_without_newline() {
        let bytes = envelope(serde_json::json!({"type": "abort"}));
        let mut input = BufReader::new(bytes.as_slice());
        assert_eq!(
            read_test_command(&mut input).await.expect("valid command"),
            InboundCommand::Valid(CommandEnvelope {
                seq: 1,
                command_id: CommandId::parse("00000000-0000-4000-8000-000000000001")
                    .expect("canonical test UUID"),
                command: Command::Abort {},
            })
        );
    }

    #[tokio::test]
    async fn reads_crlf_split_across_reader_buffers() {
        let mut bytes = envelope(serde_json::json!({"type": "abort"}));
        bytes.extend_from_slice(b"\r\n");
        let mut input = BufReader::with_capacity(1, bytes.as_slice());
        assert_eq!(
            read_test_command(&mut input).await.expect("valid command"),
            InboundCommand::Valid(CommandEnvelope {
                seq: 1,
                command_id: CommandId::parse("00000000-0000-4000-8000-000000000001")
                    .expect("canonical test UUID"),
                command: Command::Abort {},
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
        let mut input = BufReader::with_capacity(7, bytes.as_slice());

        let inbound = read_test_command(&mut input)
            .await
            .expect("outer envelope remains valid");
        let expected_digest: [u8; 32] = Sha256::digest(&command_bytes).into();
        assert_eq!(
            inbound,
            InboundCommand::Invalid {
                seq: 1,
                command_id: CommandId::parse("00000000-0000-4000-8000-000000000001")
                    .expect("canonical test UUID"),
                reason: CommandRejectReason::Oversized {
                    actual_bytes: command_bytes.len() as u64,
                },
                raw_command: RejectedCommandPayload::DiscardedOversized,
                payload_digest: Some(KeyedCommandDigest::new("test-command-key", expected_digest,)),
            }
        );
    }

    #[tokio::test]
    async fn oversized_command_is_discarded_when_identity_follows_the_payload() {
        let command = serde_json::to_vec(&serde_json::json!({
            "type":"user_message",
            "text":"secret".repeat(MAX_USER_COMMAND_BYTES / 6),
            "attachments":[],
        }))
        .expect("serialize oversized command");
        assert!(command.len() > MAX_USER_COMMAND_BYTES);
        let mut frame = Vec::with_capacity(command.len() + 64);
        frame.extend_from_slice(br#"{"command":"#);
        frame.extend_from_slice(&command);
        frame
            .extend_from_slice(br#","command_id":"00000000-0000-4000-8000-000000000008","seq":7}"#);
        let mut input = BufReader::with_capacity(13, frame.as_slice());

        let inbound = read_test_command(&mut input)
            .await
            .expect("outer identity after payload remains available");
        let InboundCommand::Invalid {
            seq,
            command_id,
            reason: CommandRejectReason::Oversized { actual_bytes },
            raw_command,
            payload_digest,
        } = inbound
        else {
            panic!("oversized command must be a typed rejection");
        };
        assert_eq!(seq, 7);
        assert_eq!(command_id.as_str(), "00000000-0000-4000-8000-000000000008");
        assert_eq!(actual_bytes, command.len() as u64);
        assert!(matches!(
            raw_command,
            RejectedCommandPayload::DiscardedOversized
        ));
        let expected_digest: [u8; 32] = Sha256::digest(&command).into();
        assert_eq!(
            payload_digest.expect("incremental digest").hmac(),
            &expected_digest
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
            read_test_command(&mut input)
                .await
                .expect("outer envelope remains valid"),
            InboundCommand::Invalid {
                seq: 1,
                command_id: CommandId::parse("00000000-0000-4000-8000-000000000001")
                    .expect("canonical test UUID"),
                reason: CommandRejectReason::AttachmentsNotEmpty,
                raw_command: RejectedCommandPayload::Present(SensitiveCommandPayload::new(
                    serde_json::to_vec(&serde_json::json!({
                        "type": "user_message",
                        "text": "inspect this",
                        "attachments": [{"name": "secret.txt"}],
                    }))
                    .expect("serialize raw command"),
                )),
                payload_digest: None,
            }
        );
    }

    #[tokio::test]
    async fn classifies_missing_attachments_as_a_schema_violation() {
        let bytes = envelope(serde_json::json!({
            "type": "user_message",
            "text": "inspect this",
        }));
        let mut input = BufReader::new(bytes.as_slice());

        assert_eq!(
            read_test_command(&mut input)
                .await
                .expect("outer envelope remains valid"),
            InboundCommand::Invalid {
                seq: 1,
                command_id: CommandId::parse("00000000-0000-4000-8000-000000000001")
                    .expect("canonical test UUID"),
                reason: CommandRejectReason::SchemaViolation,
                raw_command: RejectedCommandPayload::Present(SensitiveCommandPayload::new(
                    serde_json::to_vec(&serde_json::json!({
                        "type": "user_message",
                        "text": "inspect this",
                    }))
                    .expect("serialize raw command"),
                )),
                payload_digest: None,
            }
        );
    }

    #[tokio::test]
    async fn rejects_duplicate_command_keys_before_semantic_classification() {
        for command in [
            r#"{"type":"user_message","text":"first","text":"second","attachments":[]}"#,
            r#"{"type":"user_message","text":"first","te\u0078t":"second","attachments":[]}"#,
            r#"{"type":"user_message","text":"first","attachments":[{"name":"first","name":"second"}]}"#,
            r#"{"type":"approval_decision","request_id":"request-1","decision":{"approve_always":{"rule":{"path":"first","path":"second"}}}}"#,
        ] {
            let bytes = raw_envelope(command);
            let mut input = BufReader::with_capacity(3, bytes.as_slice());

            assert_eq!(
                read_test_command(&mut input)
                    .await
                    .expect("duplicate-key command retains its outer identity"),
                InboundCommand::Invalid {
                    seq: 1,
                    command_id: CommandId::parse("00000000-0000-4000-8000-000000000001")
                        .expect("canonical test UUID"),
                    reason: CommandRejectReason::SchemaViolation,
                    raw_command: RejectedCommandPayload::Present(SensitiveCommandPayload::new(
                        command.as_bytes().to_vec(),
                    )),
                    payload_digest: None,
                },
                "command: {command}"
            );
        }
    }

    #[tokio::test]
    async fn preserves_unknown_and_attachment_classification_precedence_for_unique_keys() {
        let cases = [
            (
                r#"{"type":"future_command","attachments":[{"name":"unsupported"}]}"#,
                CommandRejectReason::UnknownCommand,
            ),
            (
                r#"{"type":"user_message","text":7,"attachments":[{"name":"unsupported"}]}"#,
                CommandRejectReason::AttachmentsNotEmpty,
            ),
        ];

        for (command, expected_reason) in cases {
            let bytes = raw_envelope(command);
            let mut input = BufReader::new(bytes.as_slice());
            let InboundCommand::Invalid { reason, .. } = read_test_command(&mut input)
                .await
                .expect("semantic rejection retains its outer identity")
            else {
                panic!("command must be rejected: {command}");
            };
            assert_eq!(reason, expected_reason, "command: {command}");
        }
    }

    #[tokio::test]
    async fn malformed_command_value_with_readable_identity_is_a_schema_violation() {
        let bytes = br#"{"seq":7,"command_id":"00000000-0000-4000-8000-000000000009","command":{"type":"abort",}}"#;
        let mut input = BufReader::with_capacity(3, bytes.as_slice());

        assert_eq!(
            read_test_command(&mut input)
                .await
                .expect("outer identity remains readable"),
            InboundCommand::Invalid {
                seq: 7,
                command_id: CommandId::parse("00000000-0000-4000-8000-000000000009")
                    .expect("canonical test UUID"),
                reason: CommandRejectReason::SchemaViolation,
                raw_command: RejectedCommandPayload::Present(SensitiveCommandPayload::new(
                    br#"{"type":"abort",}"#.to_vec(),
                )),
                payload_digest: None,
            }
        );
    }

    #[tokio::test]
    async fn retains_identity_for_unknown_and_missing_commands() {
        let unknown = envelope(serde_json::json!({"type": "future_command"}));
        let mut input = BufReader::new(unknown.as_slice());
        assert_eq!(
            read_test_command(&mut input)
                .await
                .expect("unknown typed command"),
            InboundCommand::Invalid {
                seq: 1,
                command_id: CommandId::parse("00000000-0000-4000-8000-000000000001")
                    .expect("canonical test UUID"),
                reason: CommandRejectReason::UnknownCommand,
                raw_command: RejectedCommandPayload::Present(SensitiveCommandPayload::new(
                    br#"{"type":"future_command"}"#.to_vec(),
                )),
                payload_digest: None,
            }
        );

        let missing = br#"{"seq":2,"command_id":"00000000-0000-4000-8000-000000000002"}"#;
        let mut input = BufReader::new(missing.as_slice());
        assert_eq!(
            read_test_command(&mut input)
                .await
                .expect("missing command body"),
            InboundCommand::Invalid {
                seq: 2,
                command_id: CommandId::parse("00000000-0000-4000-8000-000000000002")
                    .expect("canonical test UUID"),
                reason: CommandRejectReason::SchemaViolation,
                raw_command: RejectedCommandPayload::Missing,
                payload_digest: None,
            }
        );
    }

    #[tokio::test]
    async fn present_null_is_not_collapsed_into_a_missing_command_field() {
        let bytes =
            br#"{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","command":null}"#;
        let mut input = BufReader::new(bytes.as_slice());
        assert_eq!(
            read_test_command(&mut input)
                .await
                .expect("canonical outer identity remains readable"),
            InboundCommand::Invalid {
                seq: 1,
                command_id: CommandId::parse("00000000-0000-4000-8000-000000000001")
                    .expect("canonical test UUID"),
                reason: CommandRejectReason::SchemaViolation,
                raw_command: RejectedCommandPayload::Present(SensitiveCommandPayload::new(
                    b"null".to_vec(),
                )),
                payload_digest: None,
            }
        );
    }

    #[tokio::test]
    async fn invalid_or_noncanonical_command_uuid_is_a_malformed_outer_envelope() {
        for value in [
            "not-a-uuid",
            "00000000000040008000000000000001",
            "00000000-0000-4000-8000-00000000000A",
        ] {
            let bytes = format!(r#"{{"seq":1,"command_id":"{value}","command":null}}"#);
            let mut input = BufReader::new(bytes.as_bytes());
            let error = read_test_command(&mut input)
                .await
                .expect_err("invalid external command identity must close the epoch");
            assert!(error.downcast_ref::<InvalidCommand>().is_some());
        }
    }

    #[tokio::test]
    async fn measures_the_raw_command_bytes_before_json_compaction() {
        let whitespace = " ".repeat(MAX_USER_COMMAND_BYTES);
        let input = format!(
            r#"{{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","command":{{{whitespace}"type":"user_message","text":"small","attachments":[]}}}}"#
        );
        assert!(input.len() < MAX_FRAME_BYTES);
        let mut input = BufReader::new(input.as_bytes());

        assert!(matches!(
            read_test_command(&mut input)
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
        let mut bytes = frame_with_total_bytes(MAX_FRAME_BYTES + 1);
        bytes.push(b'\n');
        let mut input = BufReader::new(bytes.as_slice());
        let error = read_test_command(&mut input)
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
    async fn accepts_a_max_size_frame_when_crlf_is_split_after_the_grace_byte() {
        let mut bytes = frame_with_total_bytes(MAX_FRAME_BYTES);
        bytes.extend_from_slice(b"\r\n");
        let mut input = BufReader::with_capacity(MAX_FRAME_BYTES + 1, bytes.as_slice());

        let frame = read_frame(&mut input, TestDigestFactory.start())
            .await
            .expect("maximum frame with CRLF is valid");
        assert!(frame.command_bytes > MAX_USER_COMMAND_BYTES);
        assert!(frame.command.is_none());
        assert!(frame.identity.len() < MAX_ENVELOPE_METADATA_BYTES);
    }

    #[tokio::test]
    async fn rejects_an_unterminated_oversized_frame_while_the_writer_is_open() {
        let (mut writer, reader) = tokio::io::duplex(64 * 1024);
        let oversized = frame_with_total_bytes(MAX_FRAME_BYTES + 1);
        let writer_task = tokio::spawn(async move {
            writer
                .write_all(&oversized)
                .await
                .expect("write oversized frame");
            std::future::pending::<()>().await;
        });
        let mut input = BufReader::new(reader);

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            read_test_command(&mut input),
        )
        .await
        .expect("oversized frame must not wait for newline or EOF")
        .expect_err("oversized input must fail");
        writer_task.abort();
        assert_eq!(
            error.downcast_ref::<CommandTooLarge>(),
            Some(&CommandTooLarge {
                limit: MAX_FRAME_BYTES,
                actual: MAX_FRAME_BYTES + 1,
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
            read_test_command(&mut input)
                .await
                .expect("oversized command is a typed rejection"),
            InboundCommand::Invalid {
                reason: CommandRejectReason::Oversized { .. },
                ..
            }
        ));
        assert_eq!(
            read_test_command(&mut input)
                .await
                .expect("next command remains readable"),
            InboundCommand::Valid(CommandEnvelope {
                seq: 1,
                command_id: CommandId::parse("00000000-0000-4000-8000-000000000001")
                    .expect("canonical test UUID"),
                command: Command::Abort {},
            })
        );
    }
}
