use std::{collections::VecDeque, pin::Pin, time::Duration};

use futures_util::{Stream, StreamExt};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

const MAX_ERROR_BODY_CHARS: usize = 4_000;
const MAX_ERROR_BODY_READ_BYTES: usize = MAX_ERROR_BODY_CHARS * 4 + 4;
const MAX_SSE_LINE_BYTES: usize = 1024 * 1024;
const MAX_SSE_EVENT_BYTES: usize = 1024 * 1024;
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);

type ByteStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, String>> + Send>>;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SseError {
    #[error("{status}: {body}")]
    Http { status: u16, body: String },
    #[error("SSE transport error: {0}")]
    Transport(String),
    #[error("SSE stream was idle for {seconds} seconds")]
    IdleTimeout { seconds: u64 },
    #[error("SSE stream cancelled")]
    Cancelled,
    #[error("SSE data was not valid UTF-8")]
    InvalidUtf8,
    #[error("SSE line exceeded {limit} bytes")]
    LineTooLong { limit: usize },
    #[error("SSE event data exceeded {limit} bytes")]
    EventTooLong { limit: usize },
}

pub struct SseStream {
    bytes: ByteStream,
    parser: SseLineParser,
    cancel: CancellationToken,
    idle_timeout: Duration,
}

impl SseStream {
    pub async fn from_response(
        response: reqwest::Response,
        cancel: CancellationToken,
    ) -> Result<Self, SseError> {
        let status = response.status();
        let bytes = response.bytes_stream().map(|chunk| {
            chunk
                .map(|bytes| bytes.to_vec())
                .map_err(|error| error.to_string())
        });
        let bytes: ByteStream = Box::pin(bytes);

        if !status.is_success() {
            let body = read_error_body(bytes, &cancel, IDLE_TIMEOUT).await?;
            return Err(SseError::Http {
                status: status.as_u16(),
                body,
            });
        }

        Ok(Self::new(bytes, cancel, IDLE_TIMEOUT))
    }

    pub async fn next_payload(&mut self) -> Result<Option<String>, SseError> {
        loop {
            if self.cancel.is_cancelled() {
                return Err(SseError::Cancelled);
            }
            if let Some(payload) = self.parser.next_payload() {
                return Ok(Some(payload));
            }
            if self.parser.is_done() {
                return Ok(None);
            }

            match receive_chunk(&mut self.bytes, &self.cancel, self.idle_timeout).await? {
                Some(bytes) => self.parser.push_chunk(&bytes)?,
                None => {
                    self.parser.finish()?;
                    return Ok(self.parser.next_payload());
                }
            }
        }
    }

    fn new(bytes: ByteStream, cancel: CancellationToken, idle_timeout: Duration) -> Self {
        Self {
            bytes,
            parser: SseLineParser::default(),
            cancel,
            idle_timeout,
        }
    }
}

#[derive(Default)]
struct SseLineParser {
    buffer: Vec<u8>,
    event_data: Vec<u8>,
    event_has_data: bool,
    payloads: VecDeque<String>,
    done: bool,
}

impl SseLineParser {
    fn push_chunk(&mut self, chunk: &[u8]) -> Result<(), SseError> {
        if self.done {
            return Ok(());
        }

        for segment in chunk.split_inclusive(|byte| *byte == b'\n') {
            let has_newline = segment.last() == Some(&b'\n');
            let content = if has_newline {
                &segment[..segment.len() - 1]
            } else {
                segment
            };
            if self.buffer.len().saturating_add(content.len()) > MAX_SSE_LINE_BYTES {
                return Err(SseError::LineTooLong {
                    limit: MAX_SSE_LINE_BYTES,
                });
            }
            self.buffer.extend_from_slice(content);

            if has_newline {
                if self.buffer.last() == Some(&b'\r') {
                    self.buffer.pop();
                }
                let line = std::mem::take(&mut self.buffer);
                self.process_line(&line)?;
            }
            if self.done {
                self.buffer.clear();
                self.event_data.clear();
                self.event_has_data = false;
                break;
            }
        }
        Ok(())
    }

    fn process_line(&mut self, line: &[u8]) -> Result<(), SseError> {
        if line.is_empty() {
            return self.dispatch_event();
        }
        let Some(mut data) = line.strip_prefix(b"data:") else {
            return Ok(());
        };
        if data.first() == Some(&b' ') {
            data = &data[1..];
        }
        let separator_len = usize::from(self.event_has_data);
        if self
            .event_data
            .len()
            .saturating_add(separator_len)
            .saturating_add(data.len())
            > MAX_SSE_EVENT_BYTES
        {
            return Err(SseError::EventTooLong {
                limit: MAX_SSE_EVENT_BYTES,
            });
        }
        if self.event_has_data {
            self.event_data.push(b'\n');
        }
        self.event_data.extend_from_slice(data);
        self.event_has_data = true;
        Ok(())
    }

    fn dispatch_event(&mut self) -> Result<(), SseError> {
        if !self.event_has_data {
            return Ok(());
        }
        let data = std::mem::take(&mut self.event_data);
        self.event_has_data = false;
        let payload = String::from_utf8(data).map_err(|_| SseError::InvalidUtf8)?;
        if payload == "[DONE]" {
            self.done = true;
        } else {
            self.payloads.push_back(payload);
        }
        Ok(())
    }

    fn next_payload(&mut self) -> Option<String> {
        self.payloads.pop_front()
    }

    fn is_done(&self) -> bool {
        self.done && self.payloads.is_empty()
    }

    fn finish(&mut self) -> Result<(), SseError> {
        if self.done {
            self.buffer.clear();
            self.event_data.clear();
            self.event_has_data = false;
            return Ok(());
        }
        if !self.buffer.is_empty() {
            if self.buffer.last() == Some(&b'\r') {
                self.buffer.pop();
            }
            let line = std::mem::take(&mut self.buffer);
            self.process_line(&line)?;
        }
        self.dispatch_event()
    }
}

async fn receive_chunk(
    bytes: &mut ByteStream,
    cancel: &CancellationToken,
    idle_timeout: Duration,
) -> Result<Option<Vec<u8>>, SseError> {
    let next_chunk = tokio::time::timeout(idle_timeout, bytes.next());
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(SseError::Cancelled),
        result = next_chunk => match result {
            Ok(Some(Ok(chunk))) => Ok(Some(chunk)),
            Ok(Some(Err(error))) => Err(SseError::Transport(error)),
            Ok(None) => Ok(None),
            Err(_) => Err(SseError::IdleTimeout {
                seconds: idle_timeout.as_secs(),
            }),
        },
    }
}

async fn read_error_body(
    mut bytes: ByteStream,
    cancel: &CancellationToken,
    idle_timeout: Duration,
) -> Result<String, SseError> {
    let mut body = Vec::with_capacity(MAX_ERROR_BODY_READ_BYTES);
    let mut source_truncated = false;

    while let Some(chunk) = receive_chunk(&mut bytes, cancel, idle_timeout).await? {
        let remaining = MAX_ERROR_BODY_READ_BYTES.saturating_sub(body.len());
        if chunk.len() >= remaining {
            body.extend_from_slice(&chunk[..remaining]);
            source_truncated = true;
            break;
        }
        body.extend_from_slice(&chunk);
    }

    let body = String::from_utf8_lossy(&body);
    Ok(truncate_error_body(body.trim(), source_truncated))
}

fn truncate_error_body(body: &str, source_truncated: bool) -> String {
    let count = body.chars().count();
    if count <= MAX_ERROR_BODY_CHARS && !source_truncated {
        return body.to_owned();
    }

    let kept: String = body.chars().take(MAX_ERROR_BODY_CHARS).collect();
    if source_truncated {
        format!("{kept}... [truncated]")
    } else {
        format!(
            "{kept}... [truncated {} chars]",
            count - MAX_ERROR_BODY_CHARS
        )
    }
}

#[cfg(test)]
mod tests {
    use futures_util::stream;

    use super::*;

    fn parser_payloads(
        chunks: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> Result<(Vec<String>, bool), SseError> {
        let mut parser = SseLineParser::default();
        for chunk in chunks {
            parser.push_chunk(chunk.as_ref())?;
        }
        let mut payloads = Vec::new();
        while let Some(payload) = parser.next_payload() {
            payloads.push(payload);
        }
        Ok((payloads, parser.is_done()))
    }

    #[test]
    fn joins_data_lines_until_the_event_boundary() {
        let (payloads, done) =
            parser_payloads([b"data: one\r\ndata: two\n\ndata: separate\n\n".as_slice()])
                .expect("valid SSE");
        assert_eq!(payloads, ["one\ntwo", "separate"]);
        assert!(!done);
    }

    #[test]
    fn ignores_non_data_lines_and_accepts_optional_space() {
        let input = b": comment\nevent: message\ndata:no-space\ndata: with-space\n\n";
        let (payloads, _) = parser_payloads([input.as_slice()]).expect("valid SSE");
        assert_eq!(payloads, ["no-space\nwith-space"]);
    }

    #[test]
    fn parses_lines_split_across_chunks() {
        let chunks = [
            b"da".as_slice(),
            b"ta: {\"text\":".as_slice(),
            b"\"hello\"}\n\ndata: second".as_slice(),
            b"\n\n".as_slice(),
        ];
        let (payloads, _) = parser_payloads(chunks).expect("valid SSE");
        assert_eq!(payloads, [r#"{"text":"hello"}"#, "second"]);
    }

    #[test]
    fn preserves_utf8_split_across_chunks() {
        let encoded = "data: 日本語\n\n".as_bytes();
        let split = encoded
            .windows(2)
            .position(|window| window[0] >= 0x80 && window[1] >= 0x80)
            .expect("multibyte sequence")
            + 1;
        let (payloads, _) =
            parser_payloads([&encoded[..split], &encoded[split..]]).expect("valid UTF-8");
        assert_eq!(payloads, ["日本語"]);
    }

    #[test]
    fn done_terminates_and_ignores_later_data() {
        let input = b"data: before\n\ndata: [DONE]\n\ndata: after\n\n";
        let (payloads, done) = parser_payloads([input.as_slice()]).expect("valid SSE");
        assert_eq!(payloads, ["before"]);
        assert!(done);
    }

    #[test]
    fn eof_dispatches_a_final_line_without_a_newline() {
        let mut clean = SseLineParser::default();
        clean.push_chunk(b"data: complete\n\n").expect("valid SSE");
        assert_eq!(clean.finish(), Ok(()));

        let mut final_line = SseLineParser::default();
        final_line
            .push_chunk(b"data: {\"complete\":true}")
            .expect("buffered final line");
        final_line.finish().expect("valid EOF");
        assert_eq!(
            final_line.next_payload().as_deref(),
            Some("{\"complete\":true}")
        );

        let mut done = SseLineParser::default();
        done.push_chunk(b"data: [DONE]").expect("buffered DONE");
        done.finish().expect("valid EOF");
        assert!(done.is_done());
    }

    #[test]
    fn rejects_invalid_utf8_after_line_is_complete() {
        let error = parser_payloads([b"data: \xff\n\n".as_slice()]).expect_err("invalid UTF-8");
        assert_eq!(error, SseError::InvalidUtf8);
    }

    #[test]
    fn enforces_sse_line_limit_across_chunks() {
        let mut exact = b"data: ".to_vec();
        exact.resize(MAX_SSE_LINE_BYTES, b'a');
        exact.push(b'\n');
        exact.push(b'\n');
        let (payloads, _) = parser_payloads([exact]).expect("line at limit is valid");
        assert_eq!(payloads[0].len(), MAX_SSE_LINE_BYTES - b"data: ".len());

        let first = vec![b'a'; MAX_SSE_LINE_BYTES];
        let error = parser_payloads([first, vec![b'b']]).expect_err("line over limit must fail");
        assert_eq!(
            error,
            SseError::LineTooLong {
                limit: MAX_SSE_LINE_BYTES
            }
        );
    }

    #[test]
    fn enforces_an_event_limit_across_multiple_data_lines() {
        let mut parser = SseLineParser::default();
        let first = format!("data: {}\n", "a".repeat(MAX_SSE_EVENT_BYTES / 2));
        let second = format!("data: {}\n", "b".repeat(MAX_SSE_EVENT_BYTES / 2));
        parser
            .push_chunk(first.as_bytes())
            .expect("first data line");

        assert_eq!(
            parser.push_chunk(second.as_bytes()),
            Err(SseError::EventTooLong {
                limit: MAX_SSE_EVENT_BYTES
            })
        );
    }

    #[test]
    fn dispatches_a_complete_pending_event_at_eof() {
        let mut parser = SseLineParser::default();
        parser
            .push_chunk(b"data: complete\n")
            .expect("complete data line");

        parser.finish().expect("clean EOF");

        assert_eq!(parser.next_payload().as_deref(), Some("complete"));
    }

    #[tokio::test]
    async fn stream_reports_transport_errors_and_cancellation() {
        let transport = stream::iter([Err("connection reset".to_owned())]);
        let mut stream = SseStream::new(
            Box::pin(transport),
            CancellationToken::new(),
            Duration::from_secs(1),
        );
        assert_eq!(
            stream.next_payload().await,
            Err(SseError::Transport("connection reset".to_owned()))
        );

        let cancel = CancellationToken::new();
        cancel.cancel();
        let pending = stream::pending::<Result<Vec<u8>, String>>();
        let mut stream = SseStream::new(Box::pin(pending), cancel, Duration::from_secs(1));
        assert_eq!(stream.next_payload().await, Err(SseError::Cancelled));
    }

    #[tokio::test]
    async fn cancellation_preempts_buffered_payloads_and_done() {
        let bytes = stream::iter([Ok(
            b"data: first\n\ndata: second\n\ndata: [DONE]\n\n".to_vec()
        )]);
        let cancel = CancellationToken::new();
        let mut stream = SseStream::new(Box::pin(bytes), cancel.clone(), Duration::from_secs(1));

        assert_eq!(stream.next_payload().await, Ok(Some("first".to_owned())));
        cancel.cancel();
        assert_eq!(stream.next_payload().await, Err(SseError::Cancelled));
    }

    #[tokio::test]
    async fn stream_reports_idle_timeout() {
        let pending = stream::pending::<Result<Vec<u8>, String>>();
        let mut stream = SseStream::new(
            Box::pin(pending),
            CancellationToken::new(),
            Duration::from_millis(5),
        );
        assert_eq!(
            stream.next_payload().await,
            Err(SseError::IdleTimeout { seconds: 0 })
        );
    }

    #[test]
    fn http_error_body_is_truncated_and_formatted() {
        let body = truncate_error_body(&"あ".repeat(MAX_ERROR_BODY_CHARS + 7), false);
        assert!(body.starts_with(&"あ".repeat(MAX_ERROR_BODY_CHARS)));
        assert!(body.ends_with("... [truncated 7 chars]"));

        let error = SseError::Http { status: 429, body };
        assert!(error.to_string().starts_with("429: "));
    }

    #[tokio::test]
    async fn error_body_reader_stops_at_byte_limit() {
        let chunks = stream::iter([Ok(vec![b'a'; MAX_ERROR_BODY_READ_BYTES + 100])]);
        let body = read_error_body(
            Box::pin(chunks),
            &CancellationToken::new(),
            Duration::from_secs(1),
        )
        .await
        .expect("bounded error body");

        assert_eq!(
            body.strip_suffix("... [truncated]")
                .expect("truncation marker")
                .len(),
            MAX_ERROR_BODY_CHARS
        );
    }
}
