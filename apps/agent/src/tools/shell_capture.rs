//! Bounded, streaming shell-output capture.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

use super::{
    ResourceLimit, ToolError,
    truncate::{DEFAULT_MAX_BYTES, TruncationResult, truncate_tail},
};

pub const ROLLING_BUFFER_BYTES: usize = DEFAULT_MAX_BYTES * 2;
pub const COMMAND_OUTPUT_LIMIT_BYTES: u64 = 10 * 1024 * 1024;
pub const OUTPUT_QUEUE_CAPACITY: usize = 32;
const ARTIFACT_EXCHANGE_TIMEOUT: Duration = Duration::from_secs(5);
const ARTIFACT_AMBIGUOUS_RETRY_LIMIT: usize = 3;

#[async_trait]
pub trait ArtifactAppender: Send + Sync {
    async fn begin_tool_output(
        &self,
        execution_id: &str,
        initial_content: &[u8],
    ) -> Result<String, ToolError>;

    async fn append_tool_output(
        &self,
        handle: &str,
        offset: u64,
        content: &[u8],
    ) -> Result<(), ToolError>;

    async fn finish_tool_output(&self, handle: &str) -> Result<(), ToolError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShellCaptureResult {
    pub output: String,
    pub truncation: TruncationResult,
    pub artifact_handle: Option<String>,
    pub observed_bytes: u64,
}

pub struct ShellCapture<'a> {
    execution_id: &'a str,
    artifact: &'a dyn ArtifactAppender,
    chunks: VecDeque<String>,
    rolling_bytes: usize,
    observed_bytes: u64,
    sanitized_bytes: usize,
    newline_count: usize,
    has_text: bool,
    ends_with_newline: bool,
    pending_utf8: Vec<u8>,
    artifact_handle: Option<String>,
    artifact_initial_content: Option<Vec<u8>>,
    pending_artifact_writes: VecDeque<PendingArtifactWrite>,
    artifact_bytes: u64,
    scheduled_artifact_bytes: u64,
    artifact_disabled: bool,
    artifact_timeout: Duration,
    finished: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PendingArtifactWrite {
    Begin {
        content: Vec<u8>,
        attempted: bool,
    },
    Append {
        offset: u64,
        content: Vec<u8>,
        attempted: bool,
    },
}

pub(super) struct RecordedShellChunk {
    text: String,
}

impl<'a> ShellCapture<'a> {
    pub fn new(execution_id: &'a str, artifact: &'a dyn ArtifactAppender) -> Self {
        Self {
            execution_id,
            artifact,
            chunks: VecDeque::new(),
            rolling_bytes: 0,
            observed_bytes: 0,
            sanitized_bytes: 0,
            newline_count: 0,
            has_text: false,
            ends_with_newline: false,
            pending_utf8: Vec::new(),
            artifact_handle: None,
            artifact_initial_content: None,
            pending_artifact_writes: VecDeque::new(),
            artifact_bytes: 0,
            scheduled_artifact_bytes: 0,
            artifact_disabled: false,
            artifact_timeout: ARTIFACT_EXCHANGE_TIMEOUT,
            finished: false,
        }
    }

    #[cfg(test)]
    fn with_artifact_timeout(mut self, timeout: Duration) -> Self {
        self.artifact_timeout = timeout;
        self
    }

    pub async fn push(&mut self, raw: &[u8]) -> Result<String, ToolError> {
        let recorded = self.record_chunk(raw)?;
        self.archive_recorded(recorded).await
    }

    /// Account and decode a reader-observed chunk without yielding.
    ///
    /// Bash must call this immediately after dequeuing: stop signals may
    /// preempt artifact I/O, but may never discard bytes that crossed the pipe
    /// reader boundary.
    pub(super) fn record_chunk(&mut self, raw: &[u8]) -> Result<RecordedShellChunk, ToolError> {
        let (text, initial_artifact) = self.record_chunk_inner(raw)?;
        if let Err(error) = self.schedule_artifact_text(&text, initial_artifact) {
            self.disable_artifact(&error);
        }
        Ok(RecordedShellChunk { text })
    }

    pub(super) async fn archive_recorded(
        &mut self,
        recorded: RecordedShellChunk,
    ) -> Result<String, ToolError> {
        self.flush_pending_artifact_writes().await?;
        if self.observed_bytes >= COMMAND_OUTPUT_LIMIT_BYTES {
            return Err(ToolError::ResourceLimit(ResourceLimit::OutputBytes {
                observed: self.observed_bytes,
                limit: COMMAND_OUTPUT_LIMIT_BYTES,
            }));
        }
        Ok(recorded.text)
    }

    fn record_chunk_inner(&mut self, raw: &[u8]) -> Result<(String, Option<Vec<u8>>), ToolError> {
        if self.finished {
            return Err(ToolError::Protocol(
                "shell output arrived after capture was finished".to_owned(),
            ));
        }

        self.observed_bytes = self
            .observed_bytes
            .checked_add(u64::try_from(raw.len()).map_err(|_| {
                ToolError::ResourceLimit(ResourceLimit::OutputBytes {
                    observed: u64::MAX,
                    limit: COMMAND_OUTPUT_LIMIT_BYTES,
                })
            })?)
            .ok_or(ToolError::ResourceLimit(ResourceLimit::OutputBytes {
                observed: u64::MAX,
                limit: COMMAND_OUTPUT_LIMIT_BYTES,
            }))?;

        let text = self.decode_chunk(raw, false)?;
        self.record_text(&text);
        let initial_artifact = self.initial_artifact_content(&text);
        self.push_rolling(text.clone());
        Ok((text, initial_artifact))
    }

    pub async fn finish(mut self) -> Result<ShellCaptureResult, ToolError> {
        self.finished = true;
        self.flush_pending_utf8().await?;
        self.flush_pending_artifact_writes().await?;
        self.finish_artifact().await;
        let tail = self.rolling_text();
        let truncation = self.complete_truncation(truncate_tail(&tail, Default::default()));
        let output = truncation.content.clone();
        Ok(ShellCaptureResult {
            output,
            truncation,
            artifact_handle: self.artifact_handle,
            observed_bytes: self.observed_bytes,
        })
    }

    pub async fn finish_after_limit(mut self) -> Result<ShellCaptureResult, ToolError> {
        self.finished = true;
        self.flush_pending_utf8().await?;
        let _ = self.flush_pending_artifact_writes().await;
        if !self.artifact_disabled
            && (self.artifact_bytes != self.scheduled_artifact_bytes
                || !self.pending_artifact_writes.is_empty())
        {
            self.disable_artifact(&ToolError::Rpc(
                "artifact durable prefix did not acknowledge every scheduled byte".to_owned(),
            ));
        }
        self.finish_artifact().await;
        let tail = self.rolling_text();
        let truncation = self.complete_truncation(truncate_tail(&tail, Default::default()));
        Ok(ShellCaptureResult {
            output: truncation.content.clone(),
            truncation,
            artifact_handle: self.artifact_handle,
            observed_bytes: self.observed_bytes,
        })
    }

    pub async fn finish_after_abort(mut self) -> ShellCaptureResult {
        self.finished = true;
        let text = self.decode_chunk(&[], true).unwrap_or_default();
        if !text.is_empty() {
            self.record_text(&text);
            let initial_artifact = self.initial_artifact_content(&text);
            self.push_rolling(text.clone());
            self.archive_text(&text, initial_artifact).await;
        } else {
            let _ = self.flush_pending_artifact_writes().await;
        }
        self.finish_artifact().await;
        let tail = self.rolling_text();
        let truncation = self.complete_truncation(truncate_tail(&tail, Default::default()));
        ShellCaptureResult {
            output: truncation.content.clone(),
            truncation,
            artifact_handle: self.artifact_handle,
            observed_bytes: self.observed_bytes,
        }
    }

    fn push_rolling(&mut self, text: String) {
        self.rolling_bytes = self.rolling_bytes.saturating_add(text.len());
        self.chunks.push_back(text);
        while self.rolling_bytes > ROLLING_BUFFER_BYTES && self.chunks.len() > 1 {
            if let Some(removed) = self.chunks.pop_front() {
                self.rolling_bytes = self.rolling_bytes.saturating_sub(removed.len());
            }
        }
        if self.rolling_bytes > ROLLING_BUFFER_BYTES
            && let Some(only_chunk) = self.chunks.pop_front()
        {
            let retained = tail_at_char_boundary(&only_chunk, ROLLING_BUFFER_BYTES).to_owned();
            self.rolling_bytes = retained.len();
            self.chunks.push_back(retained);
        }
    }

    fn rolling_text(&self) -> String {
        self.chunks.iter().fold(String::new(), |mut output, chunk| {
            output.push_str(chunk);
            output
        })
    }

    fn decode_chunk(&mut self, raw: &[u8], eof: bool) -> Result<String, ToolError> {
        let mut bytes = std::mem::take(&mut self.pending_utf8);
        bytes.extend_from_slice(raw);
        let mut output = String::new();
        let mut remaining = bytes.as_slice();
        while !remaining.is_empty() {
            match std::str::from_utf8(remaining) {
                Ok(text) => {
                    push_sanitized(&mut output, text);
                    remaining = &[];
                }
                Err(error) => {
                    let valid = &remaining[..error.valid_up_to()];
                    let text = std::str::from_utf8(valid).map_err(|_| {
                        ToolError::Protocol("validated UTF-8 prefix is invalid".to_owned())
                    })?;
                    push_sanitized(&mut output, text);
                    remaining = &remaining[error.valid_up_to()..];
                    match error.error_len() {
                        Some(length) => {
                            output.push('\u{fffd}');
                            remaining = &remaining[length..];
                        }
                        None if eof => {
                            output.push('\u{fffd}');
                            remaining = &[];
                        }
                        None => {
                            self.pending_utf8.extend_from_slice(remaining);
                            remaining = &[];
                        }
                    }
                }
            }
        }
        Ok(output)
    }

    fn record_text(&mut self, text: &str) {
        self.sanitized_bytes = self.sanitized_bytes.saturating_add(text.len());
        self.newline_count = self
            .newline_count
            .saturating_add(text.bytes().filter(|byte| *byte == b'\n').count());
        if !text.is_empty() {
            self.has_text = true;
            self.ends_with_newline = text.ends_with('\n');
        }
    }

    fn total_sanitized_lines(&self) -> usize {
        if self.has_text {
            self.newline_count
                .saturating_add(usize::from(!self.ends_with_newline))
        } else {
            0
        }
    }

    fn initial_artifact_content(&self, text: &str) -> Option<Vec<u8>> {
        if !self.artifact_disabled
            && self.scheduled_artifact_bytes == 0
            && (self.sanitized_bytes > DEFAULT_MAX_BYTES
                || self.total_sanitized_lines() > super::truncate::DEFAULT_MAX_LINES)
        {
            let mut prefix = self.rolling_text().into_bytes();
            prefix.extend_from_slice(text.as_bytes());
            Some(prefix)
        } else {
            None
        }
    }

    async fn archive_text(&mut self, text: &str, initial_artifact: Option<Vec<u8>>) {
        if let Err(error) = self.schedule_artifact_text(text, initial_artifact) {
            self.disable_artifact(&error);
        }
        let _ = self.flush_pending_artifact_writes().await;
    }

    fn schedule_artifact_text(
        &mut self,
        text: &str,
        initial_artifact: Option<Vec<u8>>,
    ) -> Result<(), ToolError> {
        if self.artifact_disabled {
            return Ok(());
        }
        if let Some(prefix) = initial_artifact {
            // Artifact thresholding and the rolling window intentionally share
            // the sanitized UTF-8 byte domain. The raw-byte counter is only the
            // execution quota and may be smaller after U+FFFD expansion.
            self.scheduled_artifact_bytes = u64::try_from(prefix.len())
                .map_err(|_| ToolError::Protocol("artifact prefix length overflow".to_owned()))?;
            self.artifact_initial_content = Some(prefix.clone());
            self.pending_artifact_writes
                .push_back(PendingArtifactWrite::Begin {
                    content: prefix,
                    attempted: false,
                });
        } else if self.scheduled_artifact_bytes > 0 && !text.is_empty() {
            let offset = self.scheduled_artifact_bytes;
            self.scheduled_artifact_bytes = self
                .scheduled_artifact_bytes
                .checked_add(u64::try_from(text.len()).map_err(|_| {
                    ToolError::Protocol("artifact append length overflow".to_owned())
                })?)
                .ok_or_else(|| ToolError::Protocol("artifact append length overflow".to_owned()))?;
            self.pending_artifact_writes
                .push_back(PendingArtifactWrite::Append {
                    offset,
                    content: text.as_bytes().to_vec(),
                    attempted: false,
                });
        }
        Ok(())
    }

    async fn flush_pending_artifact_writes(&mut self) -> Result<(), ToolError> {
        if self.artifact_disabled || self.pending_artifact_writes.is_empty() {
            return Ok(());
        }
        for attempt in 0..ARTIFACT_AMBIGUOUS_RETRY_LIMIT {
            let result = tokio::time::timeout(
                self.artifact_timeout,
                self.flush_pending_artifact_writes_inner(),
            )
            .await;
            match result {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(error @ ToolError::ResourceLimit(ResourceLimit::OutputBytes { .. }))) => {
                    self.disable_artifact(&error);
                    return Err(error);
                }
                Ok(Err(_error @ ToolError::RpcIndeterminate(_)))
                    if attempt + 1 < ARTIFACT_AMBIGUOUS_RETRY_LIMIT =>
                {
                    continue;
                }
                Ok(Err(error @ ToolError::RpcIndeterminate(_))) => return Err(error),
                Ok(Err(error)) => {
                    self.disable_artifact(&error);
                    return Ok(());
                }
                Err(_) if attempt + 1 < ARTIFACT_AMBIGUOUS_RETRY_LIMIT => continue,
                Err(_) => {
                    return Err(ToolError::RpcIndeterminate(
                        "artifact RPC exceeded its retry deadline".to_owned(),
                    ));
                }
            }
        }
        unreachable!("bounded artifact retry loop always returns")
    }

    async fn flush_pending_artifact_writes_inner(&mut self) -> Result<(), ToolError> {
        while let Some(write) = self.pending_artifact_writes.front().cloned() {
            match self.pending_artifact_writes.front_mut() {
                Some(PendingArtifactWrite::Begin { attempted, .. })
                | Some(PendingArtifactWrite::Append { attempted, .. }) => *attempted = true,
                None => unreachable!("front was just observed"),
            }
            match write {
                PendingArtifactWrite::Begin { content, .. } => {
                    let handle = self
                        .artifact
                        .begin_tool_output(self.execution_id, &content)
                        .await?;
                    self.artifact_handle = Some(handle);
                    self.artifact_bytes = u64::try_from(content.len()).map_err(|_| {
                        ToolError::Protocol("artifact prefix length overflow".to_owned())
                    })?;
                }
                PendingArtifactWrite::Append {
                    offset,
                    content,
                    attempted,
                } => {
                    if offset != self.artifact_bytes {
                        return Err(ToolError::Protocol(
                            "pending artifact append offset did not match durable offset"
                                .to_owned(),
                        ));
                    }
                    if attempted {
                        let initial_content =
                            self.artifact_initial_content.as_deref().ok_or_else(|| {
                                ToolError::Protocol(
                                    "artifact replay lacked its initial content".to_owned(),
                                )
                            })?;
                        let replayed_handle = self
                            .artifact
                            .begin_tool_output(self.execution_id, initial_content)
                            .await?;
                        if self.artifact_handle.as_deref() != Some(replayed_handle.as_str()) {
                            return Err(ToolError::Protocol(
                                "replayed artifact begin returned a different handle".to_owned(),
                            ));
                        }
                    }
                    let handle = self.artifact_handle.as_deref().ok_or_else(|| {
                        ToolError::Protocol(
                            "artifact append was scheduled before begin acknowledgement".to_owned(),
                        )
                    })?;
                    self.artifact
                        .append_tool_output(handle, offset, &content)
                        .await?;
                    self.artifact_bytes = self
                        .artifact_bytes
                        .checked_add(u64::try_from(content.len()).map_err(|_| {
                            ToolError::Protocol("artifact append length overflow".to_owned())
                        })?)
                        .ok_or_else(|| {
                            ToolError::Protocol("artifact append length overflow".to_owned())
                        })?;
                }
            }
            self.pending_artifact_writes.pop_front();
        }
        Ok(())
    }

    async fn flush_pending_utf8(&mut self) -> Result<(), ToolError> {
        let text = self.decode_chunk(&[], true)?;
        if text.is_empty() {
            return Ok(());
        }
        self.record_text(&text);
        let initial_artifact = self.initial_artifact_content(&text);
        self.push_rolling(text.clone());
        self.archive_text(&text, initial_artifact).await;
        Ok(())
    }

    async fn finish_artifact(&mut self) {
        if self.artifact_disabled {
            return;
        }
        let Some(handle) = self.artifact_handle.clone() else {
            return;
        };
        let result = tokio::time::timeout(
            self.artifact_timeout,
            self.artifact.finish_tool_output(&handle),
        )
        .await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => self.disable_artifact(&error),
            Err(_) => self.disable_artifact(&ToolError::Rpc(
                "artifact finish RPC exceeded its deadline".to_owned(),
            )),
        }
    }

    fn disable_artifact(&mut self, error: &ToolError) {
        if self.artifact_disabled {
            return;
        }
        tracing::warn!(
            %error,
            execution_id = self.execution_id,
            "shell output artifact publication failed; preserving terminal capture without a handle"
        );
        self.artifact_disabled = true;
        self.artifact_handle = None;
        self.artifact_initial_content = None;
        self.pending_artifact_writes.clear();
    }

    fn complete_truncation(&self, mut truncation: TruncationResult) -> TruncationResult {
        let total_lines = self.total_sanitized_lines();
        truncation.total_bytes = self.sanitized_bytes;
        truncation.total_lines = total_lines;
        if self.sanitized_bytes > truncation.output_bytes || total_lines > truncation.output_lines {
            truncation.truncated = true;
            if truncation.truncated_by.is_none() {
                truncation.truncated_by = Some(if self.sanitized_bytes > truncation.max_bytes {
                    super::truncate::TruncatedBy::Bytes
                } else {
                    super::truncate::TruncatedBy::Lines
                });
            }
        }
        truncation
    }
}

pub(super) fn output_limit_if_reached(observed: u64) -> Option<ResourceLimit> {
    (observed >= COMMAND_OUTPUT_LIMIT_BYTES).then_some(ResourceLimit::OutputBytes {
        observed,
        limit: COMMAND_OUTPUT_LIMIT_BYTES,
    })
}

pub(super) async fn copy_bounded_chunks(
    mut reader: impl AsyncRead + Unpin,
    tx: mpsc::Sender<Vec<u8>>,
    observed_bytes: Arc<AtomicU64>,
    output_quota: CancellationToken,
) -> Result<u64, ToolError> {
    let mut buffer = vec![0u8; 8 * 1024];
    loop {
        if output_quota.is_cancelled() {
            return Ok(observed_bytes.load(Ordering::Acquire));
        }
        let permit = match tokio::select! {
            biased;
            _ = output_quota.cancelled() => {
                return Ok(observed_bytes.load(Ordering::Acquire));
            }
            permit = tx.reserve() => permit,
        } {
            Ok(permit) => permit,
            Err(_) => return Ok(observed_bytes.load(Ordering::Acquire)),
        };
        if output_quota.is_cancelled() {
            return Ok(observed_bytes.load(Ordering::Acquire));
        }
        if observed_bytes.load(Ordering::Acquire) >= COMMAND_OUTPUT_LIMIT_BYTES {
            output_quota.cancel();
            return Ok(COMMAND_OUTPUT_LIMIT_BYTES);
        }
        let read = tokio::select! {
            biased;
            _ = output_quota.cancelled() => {
                return Ok(observed_bytes.load(Ordering::Acquire));
            }
            read = reader.read(&mut buffer) => read?,
        };
        if read == 0 {
            return Ok(observed_bytes.load(Ordering::Acquire));
        }
        if output_quota.is_cancelled() {
            return Ok(observed_bytes.load(Ordering::Acquire));
        }

        let read_bytes = u64::try_from(read)
            .map_err(|_| ToolError::Protocol("shell read length overflow".to_owned()))?;
        let mut current = observed_bytes.load(Ordering::Acquire);
        loop {
            if current >= COMMAND_OUTPUT_LIMIT_BYTES {
                output_quota.cancel();
                return Ok(current);
            }
            let accepted = read_bytes.min(COMMAND_OUTPUT_LIMIT_BYTES - current);
            let next = current + accepted;
            match observed_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let accepted = usize::try_from(accepted).map_err(|_| {
                        ToolError::Protocol("accepted shell output length overflow".to_owned())
                    })?;
                    permit.send(buffer[..accepted].to_vec());
                    if accepted < read || next >= COMMAND_OUTPUT_LIMIT_BYTES {
                        output_quota.cancel();
                        return Ok(next);
                    }
                    break;
                }
                Err(observed) => current = observed,
            }
        }
    }
}

fn tail_at_char_boundary(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut start = value.len() - max_bytes;
    while !value.is_char_boundary(start) {
        start += 1;
    }
    &value[start..]
}

pub fn sanitize_binary_output(raw: &[u8]) -> String {
    let mut output = String::new();
    push_sanitized(&mut output, &String::from_utf8_lossy(raw));
    output
}

fn push_sanitized(output: &mut String, text: &str) {
    output.extend(text.chars().filter(|character| {
        (matches!(*character, '\t' | '\n') || !character.is_control())
            && !matches!(*character, '\u{fff9}'..='\u{fffb}')
    }));
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        future::pending,
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
    };

    use tokio::io::{AsyncWriteExt, ReadBuf};

    use super::*;

    struct ReadNotification<R> {
        inner: R,
        entered: Option<tokio::sync::oneshot::Sender<()>>,
    }

    impl<R: AsyncRead + Unpin> AsyncRead for ReadNotification<R> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if let Some(entered) = self.entered.take() {
                let _ = entered.send(());
            }
            Pin::new(&mut self.inner).poll_read(cx, buffer)
        }
    }

    #[derive(Clone, Default)]
    struct MemoryArtifacts {
        state: Arc<Mutex<ArtifactState>>,
    }

    #[derive(Default)]
    struct ArtifactState {
        content: HashMap<String, Vec<u8>>,
        begin_count: usize,
        finish_count: usize,
    }

    #[derive(Default)]
    struct FailingArtifacts {
        begin_count: AtomicU64,
    }

    #[derive(Default)]
    struct CommitThenLoseResponseArtifacts {
        state: Mutex<CommitThenLoseState>,
    }

    #[derive(Default)]
    struct CommitThenLoseState {
        content: Vec<u8>,
        begin_calls: usize,
        append_calls: usize,
    }

    #[async_trait]
    impl ArtifactAppender for CommitThenLoseResponseArtifacts {
        async fn begin_tool_output(
            &self,
            execution_id: &str,
            initial_content: &[u8],
        ) -> Result<String, ToolError> {
            let mut state = self.state.lock().unwrap();
            state.begin_calls += 1;
            if state.content.is_empty() {
                state.content.extend_from_slice(initial_content);
            } else {
                assert!(state.content.starts_with(initial_content));
            }
            if state.begin_calls == 1 {
                return Err(ToolError::RpcIndeterminate(
                    "begin response lost".to_owned(),
                ));
            }
            Ok(format!(
                "artifact://conversation-1/tool-output/{execution_id}"
            ))
        }

        async fn append_tool_output(
            &self,
            _handle: &str,
            offset: u64,
            content: &[u8],
        ) -> Result<(), ToolError> {
            let mut state = self.state.lock().unwrap();
            state.append_calls += 1;
            let offset = usize::try_from(offset).unwrap();
            if state.content.len() == offset {
                state.content.extend_from_slice(content);
            } else {
                assert_eq!(&state.content[offset..], content);
            }
            if state.append_calls == 1 {
                return Err(ToolError::RpcIndeterminate(
                    "append response lost".to_owned(),
                ));
            }
            Ok(())
        }

        async fn finish_tool_output(&self, _handle: &str) -> Result<(), ToolError> {
            Ok(())
        }
    }

    #[async_trait]
    impl ArtifactAppender for FailingArtifacts {
        async fn begin_tool_output(
            &self,
            _execution_id: &str,
            _initial_content: &[u8],
        ) -> Result<String, ToolError> {
            self.begin_count.fetch_add(1, Ordering::Relaxed);
            Err(ToolError::Rpc("injected artifact failure".to_owned()))
        }

        async fn append_tool_output(
            &self,
            _handle: &str,
            _offset: u64,
            _content: &[u8],
        ) -> Result<(), ToolError> {
            unreachable!("begin never succeeds")
        }

        async fn finish_tool_output(&self, _handle: &str) -> Result<(), ToolError> {
            unreachable!("begin never succeeds")
        }
    }

    #[derive(Default)]
    struct StalledArtifacts {
        begin_count: AtomicU64,
    }

    #[derive(Default)]
    struct FinishFailArtifacts;

    struct AggregateLimitArtifacts;

    #[derive(Default)]
    struct PartialAppendLimitArtifacts {
        content: Mutex<Vec<u8>>,
        finish_count: AtomicU64,
    }

    #[async_trait]
    impl ArtifactAppender for PartialAppendLimitArtifacts {
        async fn begin_tool_output(
            &self,
            execution_id: &str,
            initial_content: &[u8],
        ) -> Result<String, ToolError> {
            self.content
                .lock()
                .expect("partial artifact lock")
                .extend_from_slice(initial_content);
            Ok(format!(
                "artifact://conversation/tool-output/{execution_id}"
            ))
        }

        async fn append_tool_output(
            &self,
            _handle: &str,
            _offset: u64,
            _content: &[u8],
        ) -> Result<(), ToolError> {
            Err(ToolError::ResourceLimit(ResourceLimit::OutputBytes {
                observed: 101,
                limit: 100,
            }))
        }

        async fn finish_tool_output(&self, _handle: &str) -> Result<(), ToolError> {
            self.finish_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[async_trait]
    impl ArtifactAppender for AggregateLimitArtifacts {
        async fn begin_tool_output(
            &self,
            _execution_id: &str,
            _initial_content: &[u8],
        ) -> Result<String, ToolError> {
            Err(ToolError::ResourceLimit(ResourceLimit::OutputBytes {
                observed: 101,
                limit: 100,
            }))
        }

        async fn append_tool_output(
            &self,
            _handle: &str,
            _offset: u64,
            _content: &[u8],
        ) -> Result<(), ToolError> {
            unreachable!("begin fails")
        }

        async fn finish_tool_output(&self, _handle: &str) -> Result<(), ToolError> {
            unreachable!("begin fails")
        }
    }

    #[async_trait]
    impl ArtifactAppender for FinishFailArtifacts {
        async fn begin_tool_output(
            &self,
            execution_id: &str,
            _initial_content: &[u8],
        ) -> Result<String, ToolError> {
            Ok(format!(
                "artifact://conversation/tool-output/{execution_id}"
            ))
        }

        async fn append_tool_output(
            &self,
            _handle: &str,
            _offset: u64,
            _content: &[u8],
        ) -> Result<(), ToolError> {
            Ok(())
        }

        async fn finish_tool_output(&self, _handle: &str) -> Result<(), ToolError> {
            Err(ToolError::Rpc("injected finish failure".to_owned()))
        }
    }

    #[async_trait]
    impl ArtifactAppender for StalledArtifacts {
        async fn begin_tool_output(
            &self,
            _execution_id: &str,
            _initial_content: &[u8],
        ) -> Result<String, ToolError> {
            self.begin_count.fetch_add(1, Ordering::Relaxed);
            pending().await
        }

        async fn append_tool_output(
            &self,
            _handle: &str,
            _offset: u64,
            _content: &[u8],
        ) -> Result<(), ToolError> {
            pending().await
        }

        async fn finish_tool_output(&self, _handle: &str) -> Result<(), ToolError> {
            pending().await
        }
    }

    #[async_trait]
    impl ArtifactAppender for MemoryArtifacts {
        async fn begin_tool_output(
            &self,
            execution_id: &str,
            initial_content: &[u8],
        ) -> Result<String, ToolError> {
            let handle = format!("artifact://conversation/tool-output/{execution_id}");
            let mut state = self.state.lock().expect("artifact state lock");
            state.begin_count += 1;
            state
                .content
                .insert(handle.clone(), initial_content.to_vec());
            Ok(handle)
        }

        async fn append_tool_output(
            &self,
            handle: &str,
            offset: u64,
            content: &[u8],
        ) -> Result<(), ToolError> {
            let mut state = self.state.lock().expect("artifact state lock");
            let artifact = state.content.get_mut(handle).expect("known handle");
            assert_eq!(
                u64::try_from(artifact.len()).expect("artifact length"),
                offset
            );
            artifact.extend_from_slice(content);
            Ok(())
        }

        async fn finish_tool_output(&self, _handle: &str) -> Result<(), ToolError> {
            self.state.lock().expect("artifact state lock").finish_count += 1;
            Ok(())
        }
    }

    #[test]
    fn sanitizes_binary_and_carriage_returns() {
        assert_eq!(
            sanitize_binary_output("a\0b\tc\r\nd\u{1}\u{7f}\u{80}\u{9f}e".as_bytes()),
            "ab\tc\nde"
        );
        assert!(sanitize_binary_output(&[0xff]).contains('\u{fffd}'));
    }

    #[tokio::test]
    async fn removes_interlinear_annotation_controls_across_chunks() {
        let raw = "a\u{fff9}b\u{fffa}c\u{fffb}";
        assert_eq!(sanitize_binary_output(raw.as_bytes()), "abc");

        let artifacts = MemoryArtifacts::default();
        let mut capture = ShellCapture::new("interlinear-controls", &artifacts);
        for chunk in raw.as_bytes().chunks(2) {
            capture.push(chunk).await.expect("capture chunk");
        }
        assert_eq!(
            capture.finish().await.expect("finish capture").output,
            "abc"
        );
    }

    #[tokio::test]
    async fn concurrent_readers_claim_only_the_remaining_shared_quota() {
        let observed = Arc::new(AtomicU64::new(COMMAND_OUTPUT_LIMIT_BYTES - 1));
        let quota = CancellationToken::new();
        let (tx, mut rx) = mpsc::channel(2);
        let (mut writer_one, reader_one) = tokio::io::duplex(1);
        let (mut writer_two, reader_two) = tokio::io::duplex(1);
        let (entered_one_tx, entered_one_rx) = tokio::sync::oneshot::channel();
        let (entered_two_tx, entered_two_rx) = tokio::sync::oneshot::channel();

        let first = tokio::spawn(copy_bounded_chunks(
            ReadNotification {
                inner: reader_one,
                entered: Some(entered_one_tx),
            },
            tx.clone(),
            observed.clone(),
            quota.clone(),
        ));
        let second = tokio::spawn(copy_bounded_chunks(
            ReadNotification {
                inner: reader_two,
                entered: Some(entered_two_tx),
            },
            tx.clone(),
            observed.clone(),
            quota.clone(),
        ));
        entered_one_rx.await.expect("first reader entered");
        entered_two_rx.await.expect("second reader entered");

        writer_one.write_all(b"a").await.expect("first byte");
        writer_two.write_all(b"b").await.expect("second byte");
        assert_eq!(
            first.await.expect("first task").expect("first copy"),
            COMMAND_OUTPUT_LIMIT_BYTES
        );
        assert_eq!(
            second.await.expect("second task").expect("second copy"),
            COMMAND_OUTPUT_LIMIT_BYTES
        );
        assert_eq!(observed.load(Ordering::Acquire), COMMAND_OUTPUT_LIMIT_BYTES);
        assert_eq!(rx.recv().await.expect("accepted byte").len(), 1);
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn quota_cancellation_wakes_a_full_queue_reservation() {
        let observed = Arc::new(AtomicU64::new(0));
        let quota = CancellationToken::new();
        let (tx, _rx) = mpsc::channel(1);
        tx.send(vec![b'x']).await.expect("fill queue");
        let task = tokio::spawn(copy_bounded_chunks(
            tokio::io::empty(),
            tx,
            observed,
            quota.clone(),
        ));
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        quota.cancel();
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(100), task)
                .await
                .expect("reserve cancellation timeout")
                .expect("copy task")
                .expect("copy result"),
            0
        );
    }

    #[tokio::test]
    async fn quota_cancellation_wakes_a_blocked_read() {
        let observed = Arc::new(AtomicU64::new(0));
        let quota = CancellationToken::new();
        let (tx, _rx) = mpsc::channel(1);
        let (_writer, reader) = tokio::io::duplex(1);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(copy_bounded_chunks(
            ReadNotification {
                inner: reader,
                entered: Some(entered_tx),
            },
            tx,
            observed,
            quota.clone(),
        ));
        entered_rx.await.expect("reader entered");
        quota.cancel();
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(100), task)
                .await
                .expect("read cancellation timeout")
                .expect("copy task")
                .expect("copy result"),
            0
        );
    }

    #[tokio::test]
    async fn preserves_utf8_split_across_chunks_and_marks_incomplete_eof() {
        let artifacts = MemoryArtifacts::default();
        let mut capture = ShellCapture::new("bash-split-utf8", &artifacts);
        let encoded = "界".as_bytes();
        assert_eq!(capture.push(&encoded[..1]).await.expect("first byte"), "");
        assert_eq!(
            capture.push(&encoded[1..]).await.expect("remaining bytes"),
            "界"
        );
        capture.push(&[0xf0, 0x9f]).await.expect("incomplete EOF");
        let result = capture.finish().await.expect("finish");
        assert_eq!(result.output, "界\u{fffd}");
        assert_eq!(result.truncation.total_bytes, "界\u{fffd}".len());
        assert_eq!(result.truncation.total_lines, 1);
    }

    #[tokio::test]
    async fn empty_eof_reports_zero_lines() {
        let artifacts = MemoryArtifacts::default();
        let capture = ShellCapture::new("empty-eof", &artifacts);
        let result = capture.finish().await.expect("finish");
        assert_eq!(result.output, "");
        assert_eq!(result.truncation.total_lines, 0);
        assert_eq!(result.truncation.output_lines, 0);
    }

    #[tokio::test]
    async fn flushes_complete_prefix_once_then_appends() {
        let artifacts = MemoryArtifacts::default();
        let mut capture = ShellCapture::new("bash-1", &artifacts);
        let first = "界".repeat(17_100);
        capture
            .push(first.as_bytes())
            .await
            .expect("first output chunk");
        capture.push(b"tail").await.expect("second output chunk");
        let result = capture.finish().await.expect("finish capture");
        let handle = result.artifact_handle.expect("full-output handle");
        let state = artifacts.state.lock().expect("artifact state lock");
        assert_eq!(state.begin_count, 1);
        assert_eq!(state.finish_count, 1);
        assert_eq!(
            String::from_utf8(state.content[&handle].clone()).expect("utf8 artifact"),
            format!("{first}tail")
        );
    }

    #[tokio::test]
    async fn aggregate_artifact_limit_stops_capture_instead_of_becoming_best_effort() {
        let artifacts = AggregateLimitArtifacts;
        let mut capture = ShellCapture::new("aggregate-limit", &artifacts);
        let output = vec![b'x'; DEFAULT_MAX_BYTES + 1];
        assert!(matches!(
            capture.push(&output).await,
            Err(ToolError::ResourceLimit(ResourceLimit::OutputBytes {
                observed: 101,
                limit: 100,
            }))
        ));
    }

    #[tokio::test]
    async fn partial_durable_prefix_is_not_advertised_after_append_limit() {
        let artifacts = PartialAppendLimitArtifacts::default();
        let mut capture = ShellCapture::new("partial-limit", &artifacts);
        let prefix = vec![b'x'; DEFAULT_MAX_BYTES + 1];
        capture.push(&prefix).await.expect("durable begin prefix");
        assert!(matches!(
            capture.push(b"unacknowledged-tail").await,
            Err(ToolError::ResourceLimit(ResourceLimit::OutputBytes {
                observed: 101,
                limit: 100,
            }))
        ));

        let result = capture
            .finish_after_limit()
            .await
            .expect("terminal capture survives aggregate limit");
        assert_eq!(result.artifact_handle, None);
        assert!(result.output.ends_with("unacknowledged-tail"));
        assert_eq!(
            artifacts
                .content
                .lock()
                .expect("partial artifact lock")
                .as_slice(),
            prefix
        );
        assert_eq!(artifacts.finish_count.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn rolling_buffer_is_byte_bounded_and_keeps_tail() {
        let artifacts = MemoryArtifacts::default();
        let mut capture = ShellCapture::new("bash-2", &artifacts);
        for index in 0..40 {
            capture
                .push(format!("{index:02}:{}\n", "x".repeat(4_000)).as_bytes())
                .await
                .expect("capture chunk");
        }
        let result = capture.finish().await.expect("finish capture");
        assert!(result.truncation.truncated);
        assert!(result.output.contains("39:"));
        assert!(!result.output.contains("00:"));
        assert!(result.output.len() <= DEFAULT_MAX_BYTES);
        assert_eq!(
            result.truncation.total_bytes,
            (0..40)
                .map(|index| format!("{index:02}:{}\n", "x".repeat(4_000)).len())
                .sum::<usize>()
        );
    }

    #[tokio::test]
    async fn single_multibyte_chunk_cannot_exceed_rolling_hard_cap() {
        let artifacts = MemoryArtifacts::default();
        let mut capture = ShellCapture::new("bash-hard-cap", &artifacts);
        let output = format!("prefix:{}", "界".repeat(50_000));
        capture
            .push(output.as_bytes())
            .await
            .expect("capture oversized chunk");
        assert!(capture.rolling_bytes <= ROLLING_BUFFER_BYTES);
        assert_eq!(
            capture.chunks.iter().map(String::len).sum::<usize>(),
            capture.rolling_bytes
        );
        let result = capture.finish().await.expect("finish capture");
        assert!(result.output.len() <= DEFAULT_MAX_BYTES);
        let handle = result.artifact_handle.expect("full output artifact");
        assert_eq!(
            artifacts.state.lock().expect("artifact state lock").content[&handle],
            output.as_bytes()
        );
    }

    #[tokio::test]
    async fn line_only_truncation_preserves_the_complete_artifact() {
        let artifacts = MemoryArtifacts::default();
        let mut capture = ShellCapture::new("bash-lines", &artifacts);
        let output = "x\n".repeat(super::super::truncate::DEFAULT_MAX_LINES + 1);
        capture
            .push(output.as_bytes())
            .await
            .expect("capture lines");
        let result = capture.finish().await.expect("finish capture");
        assert!(result.truncation.truncated);
        assert_eq!(
            result.truncation.truncated_by,
            Some(super::super::truncate::TruncatedBy::Lines)
        );
        let handle = result.artifact_handle.expect("full output artifact");
        assert_eq!(
            artifacts.state.lock().expect("artifact state lock").content[&handle],
            output.as_bytes()
        );
    }

    #[tokio::test]
    async fn invalid_utf8_expansion_uses_the_sanitized_artifact_threshold() {
        let artifacts = MemoryArtifacts::default();
        let mut capture = ShellCapture::new("bash-invalid-expansion", &artifacts);
        let raw = vec![0xff; DEFAULT_MAX_BYTES / 2 + 1];
        capture.push(&raw).await.expect("capture invalid bytes");
        let result = capture.finish().await.expect("finish capture");
        let expected = "\u{fffd}".repeat(raw.len());
        assert_eq!(result.truncation.total_bytes, expected.len());
        let handle = result
            .artifact_handle
            .expect("expanded full output artifact");
        assert_eq!(
            artifacts.state.lock().expect("artifact state lock").content[&handle],
            expected.as_bytes()
        );
    }

    #[tokio::test]
    async fn output_quota_is_inclusive_at_exact_byte_boundary() {
        for (size, limited) in [
            (COMMAND_OUTPUT_LIMIT_BYTES - 1, false),
            (COMMAND_OUTPUT_LIMIT_BYTES, true),
            (COMMAND_OUTPUT_LIMIT_BYTES + 1, true),
        ] {
            let artifacts = MemoryArtifacts::default();
            let mut capture = ShellCapture::new("bash-boundary", &artifacts);
            let raw = vec![b'x'; usize::try_from(size).expect("fixture size")];
            let error = capture.push(&raw).await.err();
            assert_eq!(error.is_some(), limited, "size={size}");
            if limited {
                assert!(matches!(
                    error,
                    Some(ToolError::ResourceLimit(ResourceLimit::OutputBytes {
                        observed,
                        limit: COMMAND_OUTPUT_LIMIT_BYTES,
                    })) if observed == size
                ));
            }
            let result = if limited {
                capture
                    .finish_after_limit()
                    .await
                    .expect("close limited artifact")
            } else {
                capture.finish().await.expect("close allowed artifact")
            };
            assert_eq!(result.observed_bytes, size);
            assert!(result.output.len() <= DEFAULT_MAX_BYTES);
            assert!(result.artifact_handle.is_some());
            assert_eq!(
                artifacts
                    .state
                    .lock()
                    .expect("artifact state lock")
                    .finish_count,
                1
            );
        }
    }

    #[tokio::test]
    async fn artifact_failure_preserves_typed_output_limit_and_bounded_capture() {
        let artifacts = FailingArtifacts::default();
        let mut capture = ShellCapture::new("bash-artifact-failure-limit", &artifacts);
        let prefix = vec![b'x'; DEFAULT_MAX_BYTES + 1];
        assert_eq!(
            capture.push(&prefix).await.expect("best-effort artifact"),
            String::from_utf8(prefix.clone()).expect("ASCII fixture")
        );
        capture.observed_bytes = COMMAND_OUTPUT_LIMIT_BYTES - 1;

        let error = capture.push(b"z").await.expect_err("inclusive quota");
        assert!(matches!(
            error,
            ToolError::ResourceLimit(ResourceLimit::OutputBytes {
                observed: COMMAND_OUTPUT_LIMIT_BYTES,
                limit: COMMAND_OUTPUT_LIMIT_BYTES,
            })
        ));
        let result = capture
            .finish_after_limit()
            .await
            .expect("artifact failure cannot replace terminal capture");
        assert_eq!(result.observed_bytes, COMMAND_OUTPUT_LIMIT_BYTES);
        assert!(result.output.ends_with('z'));
        assert!(result.output.len() <= DEFAULT_MAX_BYTES);
        assert_eq!(result.artifact_handle, None);
        assert_eq!(artifacts.begin_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn commit_then_close_begin_and_append_replay_to_one_exact_artifact() {
        let artifacts = CommitThenLoseResponseArtifacts::default();
        let prefix = vec![b'x'; DEFAULT_MAX_BYTES + 1];
        let suffix = b"tail";
        let mut capture = ShellCapture::new("commit-loss", &artifacts);
        capture.push(&prefix).await.expect("begin replay converges");
        capture.push(suffix).await.expect("append replay converges");
        let result = capture.finish().await.expect("finish artifact");
        assert!(result.artifact_handle.is_some());
        let state = artifacts.state.lock().unwrap();
        assert_eq!(state.begin_calls, 3);
        assert_eq!(state.append_calls, 2);
        assert_eq!(state.content, [prefix, suffix.to_vec()].concat());
    }

    #[tokio::test]
    async fn stalled_artifact_surfaces_bounded_indeterminate_failure() {
        let artifacts = StalledArtifacts::default();
        let mut capture = ShellCapture::new("bash-artifact-timeout", &artifacts)
            .with_artifact_timeout(Duration::from_millis(10));
        let prefix = vec![b'x'; DEFAULT_MAX_BYTES + 1];
        let error = tokio::time::timeout(Duration::from_millis(100), capture.push(&prefix))
            .await
            .expect("ambiguous retries must be bounded")
            .expect_err("indeterminate artifact cannot be discarded");
        assert!(matches!(error, ToolError::RpcIndeterminate(_)));
        assert_eq!(artifacts.begin_count.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn finish_failure_cannot_replace_a_resource_limit_terminal() {
        let artifacts = FinishFailArtifacts;
        let mut capture = ShellCapture::new("bash-finish-failure-limit", &artifacts);
        capture
            .push(&vec![b'x'; DEFAULT_MAX_BYTES + 1])
            .await
            .expect("start artifact");
        capture.observed_bytes = COMMAND_OUTPUT_LIMIT_BYTES - 1;
        assert!(matches!(
            capture.push(b"z").await,
            Err(ToolError::ResourceLimit(ResourceLimit::OutputBytes {
                observed: COMMAND_OUTPUT_LIMIT_BYTES,
                limit: COMMAND_OUTPUT_LIMIT_BYTES,
            }))
        ));

        let result = capture
            .finish_after_limit()
            .await
            .expect("finish failure is best-effort");
        assert_eq!(result.observed_bytes, COMMAND_OUTPUT_LIMIT_BYTES);
        assert!(result.output.ends_with('z'));
        assert_eq!(result.artifact_handle, None);
    }
}
