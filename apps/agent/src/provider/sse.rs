use std::{collections::VecDeque, pin::Pin, time::Duration};

use futures_util::{Stream, StreamExt};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

const MAX_ERROR_BODY_CHARS: usize = 4_000;
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
    #[error("SSE stream ended with an incomplete line")]
    UnexpectedEof,
    #[error("SSE data was not valid UTF-8")]
    InvalidUtf8,
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
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response
                .text()
                .await
                .map_err(|error| SseError::Transport(error.to_string()))?;
            return Err(SseError::Http {
                status,
                body: truncate_error_body(body.trim()),
            });
        }

        let bytes = response.bytes_stream().map(|chunk| {
            chunk
                .map(|bytes| bytes.to_vec())
                .map_err(|error| error.to_string())
        });

        Ok(Self::new(Box::pin(bytes), cancel, IDLE_TIMEOUT))
    }

    pub async fn next_payload(&mut self) -> Result<Option<String>, SseError> {
        loop {
            if let Some(payload) = self.parser.next_payload() {
                return Ok(Some(payload));
            }
            if self.parser.is_done() {
                return Ok(None);
            }

            let next_chunk = tokio::time::timeout(self.idle_timeout, self.bytes.next());
            let chunk = tokio::select! {
                _ = self.cancel.cancelled() => return Err(SseError::Cancelled),
                result = next_chunk => match result {
                    Ok(chunk) => chunk,
                    Err(_) => {
                        return Err(SseError::IdleTimeout {
                            seconds: self.idle_timeout.as_secs(),
                        });
                    }
                },
            };

            match chunk {
                Some(Ok(bytes)) => self.parser.push_chunk(&bytes)?,
                Some(Err(error)) => return Err(SseError::Transport(error)),
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
    payloads: VecDeque<String>,
    done: bool,
}

impl SseLineParser {
    fn push_chunk(&mut self, chunk: &[u8]) -> Result<(), SseError> {
        if self.done {
            return Ok(());
        }

        self.buffer.extend_from_slice(chunk);
        while let Some(newline) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let mut line: Vec<u8> = self.buffer.drain(..=newline).collect();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            self.process_line(&line)?;
            if self.done {
                self.buffer.clear();
                break;
            }
        }
        Ok(())
    }

    fn process_line(&mut self, line: &[u8]) -> Result<(), SseError> {
        let Some(mut data) = line.strip_prefix(b"data:") else {
            return Ok(());
        };
        if data.first() == Some(&b' ') {
            data = &data[1..];
        }
        let payload = std::str::from_utf8(data).map_err(|_| SseError::InvalidUtf8)?;
        if payload == "[DONE]" {
            self.done = true;
        } else {
            self.payloads.push_back(payload.to_owned());
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
        if self.done || self.buffer.iter().all(u8::is_ascii_whitespace) {
            self.buffer.clear();
            Ok(())
        } else {
            Err(SseError::UnexpectedEof)
        }
    }
}

fn truncate_error_body(body: &str) -> String {
    let count = body.chars().count();
    if count <= MAX_ERROR_BODY_CHARS {
        return body.to_owned();
    }

    let kept: String = body.chars().take(MAX_ERROR_BODY_CHARS).collect();
    format!(
        "{kept}... [truncated {} chars]",
        count - MAX_ERROR_BODY_CHARS
    )
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
    fn parses_crlf_and_lf_lines() {
        let (payloads, done) =
            parser_payloads([b"data: one\r\ndata: two\n\n".as_slice()]).expect("valid SSE");
        assert_eq!(payloads, ["one", "two"]);
        assert!(!done);
    }

    #[test]
    fn ignores_non_data_lines_and_accepts_optional_space() {
        let input = b": comment\nevent: message\ndata:no-space\ndata: with-space\n";
        let (payloads, _) = parser_payloads([input.as_slice()]).expect("valid SSE");
        assert_eq!(payloads, ["no-space", "with-space"]);
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
        let encoded = "data: 日本語\n".as_bytes();
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
    fn clean_and_incomplete_eof_are_distinct() {
        let mut clean = SseLineParser::default();
        clean.push_chunk(b"data: complete\n\n").expect("valid SSE");
        assert_eq!(clean.finish(), Ok(()));

        let mut incomplete = SseLineParser::default();
        incomplete
            .push_chunk(b"data: {\"partial\":")
            .expect("buffered bytes");
        assert_eq!(incomplete.finish(), Err(SseError::UnexpectedEof));
    }

    #[test]
    fn rejects_invalid_utf8_after_line_is_complete() {
        let error = parser_payloads([b"data: \xff\n".as_slice()]).expect_err("invalid UTF-8");
        assert_eq!(error, SseError::InvalidUtf8);
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
        let body = truncate_error_body(&"あ".repeat(MAX_ERROR_BODY_CHARS + 7));
        assert!(body.starts_with(&"あ".repeat(MAX_ERROR_BODY_CHARS)));
        assert!(body.ends_with("... [truncated 7 chars]"));

        let error = SseError::Http { status: 429, body };
        assert!(error.to_string().starts_with("429: "));
    }
}
