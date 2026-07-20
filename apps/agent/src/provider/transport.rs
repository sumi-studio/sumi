use std::{collections::VecDeque, pin::Pin, time::Duration};

use futures_util::{Stream, StreamExt};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

const MAX_ERROR_BODY_CHARS: usize = 4_000;
const MAX_ERROR_BODY_READ_BYTES: usize = MAX_ERROR_BODY_CHARS * 4 + 4;
const ERROR_BODY_TRUNCATED_MARKER: &str = "... [truncated]";
const ERROR_BODY_DIAGNOSTIC_PREFIX: &str = "[error body read incomplete: ";
const ERROR_BODY_DIAGNOSTIC_SUFFIX: &str = "]";
const MAX_SSE_LINE_BYTES: usize = 1024 * 1024;
const MAX_SSE_EVENT_BYTES: usize = 4 * MAX_SSE_LINE_BYTES;
const MAX_SSE_QUEUED_BYTES: usize = 2 * MAX_SSE_EVENT_BYTES;
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
    #[error("SSE event exceeded {limit} bytes")]
    EventTooLong { limit: usize },
    #[error("queued SSE events exceeded {limit} bytes")]
    EventQueueTooLarge { limit: usize },
    #[error("provider response exceeded {limit} raw wire bytes")]
    ResponseTooLong { limit: usize },
    #[error("SSE stream ended before the current event was terminated")]
    UnexpectedEof,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SseEvent {
    pub event: Option<String>,
    pub data: String,
}

pub struct SseStream {
    bytes: ByteStream,
    parser: SseLineParser,
    cancel: CancellationToken,
    idle_timeout: Duration,
    max_wire_bytes: usize,
    wire_bytes: usize,
}

impl SseStream {
    pub async fn from_response(
        response: reqwest::Response,
        cancel: CancellationToken,
        max_wire_bytes: usize,
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

        Ok(Self::new_with_wire_limit(
            bytes,
            cancel,
            IDLE_TIMEOUT,
            max_wire_bytes,
        ))
    }

    pub async fn next_event(&mut self) -> Result<Option<SseEvent>, SseError> {
        loop {
            if self.cancel.is_cancelled() {
                return Err(SseError::Cancelled);
            }
            if let Some(event) = self.parser.next_event() {
                return Ok(Some(event));
            }

            match receive_chunk(&mut self.bytes, &self.cancel, self.idle_timeout).await? {
                Some(bytes) => {
                    let Some(next) = self.wire_bytes.checked_add(bytes.len()) else {
                        return Err(SseError::ResponseTooLong {
                            limit: self.max_wire_bytes,
                        });
                    };
                    if next > self.max_wire_bytes {
                        return Err(SseError::ResponseTooLong {
                            limit: self.max_wire_bytes,
                        });
                    }
                    self.wire_bytes = next;
                    self.parser.push_chunk(&bytes)?;
                }
                None => {
                    self.parser.finish()?;
                    return Ok(self.parser.next_event());
                }
            }
        }
    }

    #[cfg(test)]
    fn new(bytes: ByteStream, cancel: CancellationToken, idle_timeout: Duration) -> Self {
        Self::new_with_wire_limit(bytes, cancel, idle_timeout, usize::MAX)
    }

    fn new_with_wire_limit(
        bytes: ByteStream,
        cancel: CancellationToken,
        idle_timeout: Duration,
        max_wire_bytes: usize,
    ) -> Self {
        Self {
            bytes,
            parser: SseLineParser::default(),
            cancel,
            idle_timeout,
            max_wire_bytes,
            wire_bytes: 0,
        }
    }
}

#[derive(Default)]
struct SseLineParser {
    line_buffer: Vec<u8>,
    ignore_lf_after_cr: bool,
    saw_first_line: bool,
    event_name: Option<String>,
    data: String,
    has_data: bool,
    events: VecDeque<SseEvent>,
    queued_bytes: usize,
}

impl SseLineParser {
    fn push_chunk(&mut self, chunk: &[u8]) -> Result<(), SseError> {
        for byte in chunk {
            if self.ignore_lf_after_cr {
                self.ignore_lf_after_cr = false;
                if *byte == b'\n' {
                    continue;
                }
            }

            match *byte {
                b'\r' => {
                    self.finish_line()?;
                    self.ignore_lf_after_cr = true;
                }
                b'\n' => self.finish_line()?,
                _ => {
                    if self.line_buffer.len() >= MAX_SSE_LINE_BYTES {
                        return Err(SseError::LineTooLong {
                            limit: MAX_SSE_LINE_BYTES,
                        });
                    }
                    self.line_buffer.push(*byte);
                }
            }
        }
        Ok(())
    }

    fn finish_line(&mut self) -> Result<(), SseError> {
        let mut line = std::mem::take(&mut self.line_buffer);
        if !self.saw_first_line {
            self.saw_first_line = true;
            if line.starts_with(&[0xef, 0xbb, 0xbf]) {
                line.drain(..3);
            }
        }
        self.process_line(&line)
    }

    fn process_line(&mut self, line: &[u8]) -> Result<(), SseError> {
        if line.is_empty() {
            return self.dispatch_event();
        }
        if line.first() == Some(&b':') {
            return Ok(());
        }

        let (field, mut value) = line
            .iter()
            .position(|byte| *byte == b':')
            .map_or((line, &b""[..]), |separator| {
                (&line[..separator], &line[separator + 1..])
            });
        if value.first() == Some(&b' ') {
            value = &value[1..];
        }

        match field {
            b"event" => {
                let event = std::str::from_utf8(value).map_err(|_| SseError::InvalidUtf8)?;
                self.event_name = Some(event.to_owned());
            }
            b"data" => {
                let data = std::str::from_utf8(value).map_err(|_| SseError::InvalidUtf8)?;
                let separator = usize::from(self.has_data);
                if self
                    .data
                    .len()
                    .saturating_add(separator)
                    .saturating_add(data.len())
                    > MAX_SSE_EVENT_BYTES
                {
                    return Err(SseError::EventTooLong {
                        limit: MAX_SSE_EVENT_BYTES,
                    });
                }
                if self.has_data {
                    self.data.push('\n');
                }
                self.data.push_str(data);
                self.has_data = true;
            }
            _ => {}
        }
        Ok(())
    }

    fn dispatch_event(&mut self) -> Result<(), SseError> {
        if !self.has_data {
            self.event_name = None;
            return Ok(());
        }

        let event = SseEvent {
            event: self.event_name.take(),
            data: std::mem::take(&mut self.data),
        };
        self.has_data = false;
        let charge = event_memory_charge(&event);
        if self.queued_bytes.saturating_add(charge) > MAX_SSE_QUEUED_BYTES {
            return Err(SseError::EventQueueTooLarge {
                limit: MAX_SSE_QUEUED_BYTES,
            });
        }
        self.queued_bytes += charge;
        self.events.push_back(event);
        Ok(())
    }

    fn next_event(&mut self) -> Option<SseEvent> {
        let event = self.events.pop_front()?;
        self.queued_bytes = self
            .queued_bytes
            .saturating_sub(event_memory_charge(&event));
        Some(event)
    }

    fn finish(&mut self) -> Result<(), SseError> {
        if self.line_buffer.iter().all(u8::is_ascii_whitespace)
            && !self.has_data
            && self.event_name.is_none()
        {
            self.line_buffer.clear();
            Ok(())
        } else {
            Err(SseError::UnexpectedEof)
        }
    }
}

fn event_memory_charge(event: &SseEvent) -> usize {
    std::mem::size_of::<SseEvent>()
        .saturating_add(event.data.capacity())
        .saturating_add(
            event
                .event
                .as_ref()
                .map_or(0, |event_name| event_name.capacity()),
        )
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
    let mut read_diagnostic = None;

    loop {
        let chunk = match receive_chunk(&mut bytes, cancel, idle_timeout).await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(SseError::Cancelled) => return Err(SseError::Cancelled),
            Err(error) => {
                read_diagnostic = Some(error.to_string());
                break;
            }
        };
        let remaining = MAX_ERROR_BODY_READ_BYTES.saturating_sub(body.len());
        if remaining == 0 {
            if !chunk.is_empty() {
                source_truncated = true;
                break;
            }
            continue;
        }
        if chunk.len() > remaining {
            body.extend_from_slice(&chunk[..remaining]);
            source_truncated = true;
            break;
        }
        body.extend_from_slice(&chunk);
    }

    let body = String::from_utf8_lossy(&body);
    Ok(format_error_body(
        body.trim(),
        source_truncated,
        read_diagnostic.as_deref(),
    ))
}

fn truncate_error_body(body: &str, source_truncated: bool) -> String {
    truncate_error_body_to_limit(body, source_truncated, MAX_ERROR_BODY_READ_BYTES)
}

fn format_error_body(body: &str, source_truncated: bool, read_diagnostic: Option<&str>) -> String {
    let Some(diagnostic) = read_diagnostic else {
        return truncate_error_body(body, source_truncated);
    };

    let separator_len = usize::from(!body.is_empty());
    let minimum_diagnostic_len = ERROR_BODY_DIAGNOSTIC_PREFIX.len()
        + ERROR_BODY_TRUNCATED_MARKER.len()
        + ERROR_BODY_DIAGNOSTIC_SUFFIX.len();
    let body_limit = if body.is_empty() {
        0
    } else {
        MAX_ERROR_BODY_READ_BYTES
            .saturating_sub(separator_len)
            .saturating_sub(minimum_diagnostic_len)
    };
    let body = truncate_error_body_to_limit(body, source_truncated, body_limit);
    let separator = if body.is_empty() { "" } else { " " };
    let diagnostic_limit = MAX_ERROR_BODY_READ_BYTES
        .saturating_sub(body.len())
        .saturating_sub(separator.len());
    let diagnostic = format_error_body_diagnostic(diagnostic, diagnostic_limit);
    let formatted = format!("{body}{separator}{diagnostic}");
    debug_assert!(formatted.len() <= MAX_ERROR_BODY_READ_BYTES);
    formatted
}

fn truncate_error_body_to_limit(body: &str, source_truncated: bool, max_bytes: usize) -> String {
    let count = body.chars().count();
    if count <= MAX_ERROR_BODY_CHARS && !source_truncated && body.len() <= max_bytes {
        return body.to_owned();
    }

    if source_truncated {
        let prefix_limit = max_bytes.saturating_sub(ERROR_BODY_TRUNCATED_MARKER.len());
        let (kept, _) = bounded_char_prefix(body, MAX_ERROR_BODY_CHARS, prefix_limit);
        return format!("{kept}{ERROR_BODY_TRUNCATED_MARKER}");
    }

    let mut kept_chars = count.min(MAX_ERROR_BODY_CHARS);
    loop {
        let suffix = format!("... [truncated {} chars]", count - kept_chars);
        let prefix_limit = max_bytes.saturating_sub(suffix.len());
        let (kept, actual_kept_chars) = bounded_char_prefix(body, kept_chars, prefix_limit);
        if actual_kept_chars == kept_chars {
            return format!("{kept}{suffix}");
        }
        kept_chars = actual_kept_chars;
    }
}

fn format_error_body_diagnostic(diagnostic: &str, max_bytes: usize) -> String {
    let wrapper_len = ERROR_BODY_DIAGNOSTIC_PREFIX.len() + ERROR_BODY_DIAGNOSTIC_SUFFIX.len();
    let content_limit = max_bytes.saturating_sub(wrapper_len);
    if diagnostic.len() <= content_limit {
        return format!("{ERROR_BODY_DIAGNOSTIC_PREFIX}{diagnostic}{ERROR_BODY_DIAGNOSTIC_SUFFIX}");
    }

    let prefix_limit = content_limit.saturating_sub(ERROR_BODY_TRUNCATED_MARKER.len());
    let (kept, _) = bounded_char_prefix(diagnostic, usize::MAX, prefix_limit);
    format!(
        "{ERROR_BODY_DIAGNOSTIC_PREFIX}{kept}{ERROR_BODY_TRUNCATED_MARKER}{ERROR_BODY_DIAGNOSTIC_SUFFIX}"
    )
}

fn bounded_char_prefix(value: &str, max_chars: usize, max_bytes: usize) -> (&str, usize) {
    let mut end = 0_usize;
    let mut chars = 0_usize;
    for character in value.chars() {
        if chars == max_chars || end.saturating_add(character.len_utf8()) > max_bytes {
            break;
        }
        end += character.len_utf8();
        chars += 1;
    }
    (&value[..end], chars)
}

#[cfg(test)]
mod tests {
    use futures_util::stream;

    use super::*;

    fn parser_events(
        chunks: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> Result<Vec<SseEvent>, SseError> {
        let mut parser = SseLineParser::default();
        for chunk in chunks {
            parser.push_chunk(chunk.as_ref())?;
        }
        let mut events = Vec::new();
        while let Some(event) = parser.next_event() {
            events.push(event);
        }
        Ok(events)
    }

    #[test]
    fn joins_multiple_data_lines_at_blank_line() {
        let events = parser_events([b"data: one\r\ndata: two\n\n".as_slice()]).expect("valid SSE");
        assert_eq!(
            events,
            [SseEvent {
                event: None,
                data: "one\ntwo".to_owned(),
            }]
        );
    }

    #[test]
    fn accepts_a_leading_bom_and_all_sse_line_endings() {
        for (input, expected) in [
            (b"\xef\xbb\xbfdata: lf\n\n".as_slice(), "lf"),
            (b"\xef\xbb\xbfdata: crlf\r\n\r\n".as_slice(), "crlf"),
            (b"\xef\xbb\xbfdata: cr\r\r".as_slice(), "cr"),
        ] {
            let events = parser_events([input]).expect("valid SSE line ending");
            assert_eq!(
                events,
                [SseEvent {
                    event: None,
                    data: expected.to_owned(),
                }]
            );
        }
    }

    #[test]
    fn treats_a_crlf_split_across_chunks_as_one_line_ending() {
        let events = parser_events([
            b"event: message\r".as_slice(),
            b"\ndata: split\r".as_slice(),
            b"\n\r".as_slice(),
            b"\n".as_slice(),
        ])
        .expect("valid split CRLF");

        assert_eq!(
            events,
            [SseEvent {
                event: Some("message".to_owned()),
                data: "split".to_owned(),
            }]
        );
    }

    #[test]
    fn preserves_event_name_and_ignores_comments() {
        let input = b": comment\nevent: message\ndata:no-space\ndata: with-space\n\n";
        let events = parser_events([input.as_slice()]).expect("valid SSE");
        assert_eq!(
            events,
            [SseEvent {
                event: Some("message".to_owned()),
                data: "no-space\nwith-space".to_owned(),
            }]
        );
    }

    #[test]
    fn parses_lines_split_across_chunks() {
        let chunks = [
            b"da".as_slice(),
            b"ta: {\"text\":".as_slice(),
            b"\"hello\"}\n\ndata: second".as_slice(),
            b"\n\n".as_slice(),
        ];
        let events = parser_events(chunks).expect("valid SSE");
        assert_eq!(
            events,
            [
                SseEvent {
                    event: None,
                    data: r#"{"text":"hello"}"#.to_owned(),
                },
                SseEvent {
                    event: None,
                    data: "second".to_owned(),
                }
            ]
        );
    }

    #[test]
    fn preserves_utf8_split_across_chunks() {
        let encoded = "data: 日本語\n\n".as_bytes();
        let split = encoded
            .windows(2)
            .position(|window| window[0] >= 0x80 && window[1] >= 0x80)
            .expect("multibyte sequence")
            + 1;
        let events = parser_events([&encoded[..split], &encoded[split..]]).expect("valid UTF-8");
        assert_eq!(events[0].data, "日本語");
    }

    #[test]
    fn done_marker_is_left_to_the_protocol_adapter() {
        let input = b"data: before\n\ndata: [DONE]\n\ndata: after\n\n";
        let events = parser_events([input.as_slice()]).expect("valid SSE");
        assert_eq!(
            events
                .iter()
                .map(|event| event.data.as_str())
                .collect::<Vec<_>>(),
            ["before", "[DONE]", "after"]
        );
    }

    #[test]
    fn eof_requires_a_complete_blank_line_terminated_event() {
        let mut clean = SseLineParser::default();
        clean.push_chunk(b"data: complete\n\n").expect("valid SSE");
        assert_eq!(clean.finish(), Ok(()));

        let mut final_line = SseLineParser::default();
        final_line
            .push_chunk(b"data: {\"complete\":true}")
            .expect("buffered final line");
        assert_eq!(final_line.finish(), Err(SseError::UnexpectedEof));

        let mut done = SseLineParser::default();
        done.push_chunk(b"data: [DONE]").expect("buffered DONE");
        assert_eq!(done.finish(), Err(SseError::UnexpectedEof));
    }

    #[test]
    fn rejects_invalid_utf8_after_line_is_complete() {
        let error = parser_events([b"data: \xff\n".as_slice()]).expect_err("invalid UTF-8");
        assert_eq!(error, SseError::InvalidUtf8);
    }

    #[test]
    fn enforces_sse_line_limit_across_chunks() {
        let mut exact = b"data: ".to_vec();
        exact.resize(MAX_SSE_LINE_BYTES, b'a');
        exact.extend_from_slice(b"\n\n");
        let events = parser_events([exact]).expect("line at limit is valid");
        assert_eq!(events[0].data.len(), MAX_SSE_LINE_BYTES - b"data: ".len());

        let first = vec![b'a'; MAX_SSE_LINE_BYTES];
        let error = parser_events([first, vec![b'b']]).expect_err("line over limit must fail");
        assert_eq!(
            error,
            SseError::LineTooLong {
                limit: MAX_SSE_LINE_BYTES
            }
        );
    }

    #[test]
    fn enforces_sse_event_limit_across_data_lines() {
        let line = format!(
            "data: {}\n",
            "a".repeat(MAX_SSE_LINE_BYTES - b"data: ".len())
        );
        let input = format!("{line}{line}{line}{line}{line}\n");
        let error = parser_events([input.as_bytes()]).expect_err("event over limit must fail");
        assert_eq!(
            error,
            SseError::EventTooLong {
                limit: MAX_SSE_EVENT_BYTES
            }
        );
    }

    #[test]
    fn empty_data_lines_are_charged_to_the_event_limit() {
        let mut parser = SseLineParser {
            data: "\n".repeat(MAX_SSE_EVENT_BYTES),
            has_data: true,
            ..SseLineParser::default()
        };

        assert_eq!(
            parser.process_line(b"data:"),
            Err(SseError::EventTooLong {
                limit: MAX_SSE_EVENT_BYTES
            })
        );
    }

    #[test]
    fn bounds_events_queued_from_one_chunk() {
        let minimum_charge = std::mem::size_of::<SseEvent>();
        let event_count = MAX_SSE_QUEUED_BYTES / minimum_charge + 1;
        let chunk = "data:\n\n".repeat(event_count);
        let mut parser = SseLineParser::default();

        assert_eq!(
            parser.push_chunk(chunk.as_bytes()),
            Err(SseError::EventQueueTooLarge {
                limit: MAX_SSE_QUEUED_BYTES
            })
        );
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
            stream.next_event().await,
            Err(SseError::Transport("connection reset".to_owned()))
        );

        let cancel = CancellationToken::new();
        cancel.cancel();
        let pending = stream::pending::<Result<Vec<u8>, String>>();
        let mut stream = SseStream::new(Box::pin(pending), cancel, Duration::from_secs(1));
        assert_eq!(stream.next_event().await, Err(SseError::Cancelled));
    }

    #[tokio::test]
    async fn raw_wire_budget_counts_sse_framing_at_exact_boundary() {
        let raw = b"data: ok\n\n".to_vec();
        let exact_stream = stream::iter([Ok(raw.clone())]);
        let mut exact = SseStream::new_with_wire_limit(
            Box::pin(exact_stream),
            CancellationToken::new(),
            Duration::from_secs(1),
            raw.len(),
        );
        assert_eq!(
            exact.next_event().await,
            Ok(Some(SseEvent {
                event: None,
                data: "ok".to_owned(),
            }))
        );

        let over_stream = stream::iter([Ok(raw.clone())]);
        let mut over = SseStream::new_with_wire_limit(
            Box::pin(over_stream),
            CancellationToken::new(),
            Duration::from_secs(1),
            raw.len() - 1,
        );
        assert_eq!(
            over.next_event().await,
            Err(SseError::ResponseTooLong {
                limit: raw.len() - 1,
            })
        );
    }

    #[tokio::test]
    async fn cancellation_preempts_buffered_events() {
        let bytes = stream::iter([Ok(
            b"data: first\n\ndata: second\n\ndata: [DONE]\n\n".to_vec()
        )]);
        let cancel = CancellationToken::new();
        let mut stream = SseStream::new(Box::pin(bytes), cancel.clone(), Duration::from_secs(1));

        assert_eq!(
            stream.next_event().await,
            Ok(Some(SseEvent {
                event: None,
                data: "first".to_owned(),
            }))
        );
        cancel.cancel();
        assert_eq!(stream.next_event().await, Err(SseError::Cancelled));
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
            stream.next_event().await,
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

    #[tokio::test]
    async fn exact_error_body_read_limit_reports_known_character_count() {
        let source = "😀".repeat(MAX_ERROR_BODY_CHARS + 1);
        assert_eq!(source.len(), MAX_ERROR_BODY_READ_BYTES);
        let chunks = stream::iter([Ok(source.into_bytes())]);
        let body = read_error_body(
            Box::pin(chunks),
            &CancellationToken::new(),
            Duration::from_secs(1),
        )
        .await
        .expect("bounded error body");

        assert!(body.contains("... [truncated "));
        assert!(body.ends_with(" chars]"));
        assert!(body.len() <= MAX_ERROR_BODY_READ_BYTES);
    }

    #[tokio::test]
    async fn error_body_reader_preserves_partial_and_diagnostic_on_transport_failure() {
        let chunks = stream::iter([Ok(b"partial".to_vec()), Err("connection reset".to_owned())]);
        let body = read_error_body(
            Box::pin(chunks),
            &CancellationToken::new(),
            Duration::from_secs(1),
        )
        .await
        .expect("status remains authoritative after body reset");

        assert!(body.starts_with("partial"));
        assert!(body.contains("error body read incomplete"));
        assert!(body.contains("connection reset"));
    }

    #[tokio::test]
    async fn error_body_and_large_transport_diagnostic_share_one_byte_limit() {
        let partial_body = "🙂".repeat(MAX_ERROR_BODY_CHARS - 1);
        let diagnostic = "transport故障".repeat(MAX_ERROR_BODY_READ_BYTES);
        let read = || {
            stream::iter([
                Ok(partial_body.as_bytes().to_vec()),
                Err(diagnostic.clone()),
            ])
        };
        let first = read_error_body(
            Box::pin(read()),
            &CancellationToken::new(),
            Duration::from_secs(1),
        )
        .await
        .expect("HTTP status remains authoritative");
        let second = read_error_body(
            Box::pin(read()),
            &CancellationToken::new(),
            Duration::from_secs(1),
        )
        .await
        .expect("HTTP status remains authoritative");

        assert_eq!(first, second);
        assert!(first.len() <= MAX_ERROR_BODY_READ_BYTES);
        assert!(MAX_ERROR_BODY_READ_BYTES - first.len() < char::MAX_LEN_UTF8);
        assert!(first.starts_with('🙂'));
        assert!(first.contains(" chars] "));
        assert!(first.contains(ERROR_BODY_DIAGNOSTIC_PREFIX));
        assert!(first.ends_with("... [truncated]]"));

        let error = SseError::Http {
            status: 503,
            body: first,
        };
        assert!(matches!(error, SseError::Http { status: 503, .. }));
    }

    #[tokio::test]
    async fn error_body_reader_preserves_status_on_idle_but_cancel_remains_authoritative() {
        let pending = stream::pending::<Result<Vec<u8>, String>>();
        let body = read_error_body(
            Box::pin(pending),
            &CancellationToken::new(),
            Duration::from_millis(5),
        )
        .await
        .expect("status remains authoritative after body idle timeout");
        assert!(body.contains("error body read incomplete"));
        assert!(body.contains("idle"));

        let cancel = CancellationToken::new();
        cancel.cancel();
        let pending = stream::pending::<Result<Vec<u8>, String>>();
        assert_eq!(
            read_error_body(Box::pin(pending), &cancel, Duration::ZERO).await,
            Err(SseError::Cancelled)
        );
    }
}
