//! Bounded, generation-fenced client for the tool executor service.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ed25519_dalek::SigningKey;
use serde_json::Value;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::UnixStream,
    time::{Instant, timeout, timeout_at},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zeroize::Zeroizing;

use super::manager::{
    RPC_CALL_AUTHORITY_CAPACITY_EXHAUSTED_CODE, RPC_CALL_AUTHORITY_REPLAY_CODE,
    RPC_CALL_AUTHORITY_STALE_CODE, RPC_REPLAY_OUTCOME_UNAVAILABLE_CODE,
};
use super::protocol::{RPC_BOOT_UNIQUENESS_EXHAUSTED_CODE, parse_artifact_handle};
use super::{
    ArtifactResponse, Ed25519CallAuthorityIssuer, ExecutorOperation, ExecutorResponse,
    ExecutorRpcRequest, ExecutorServiceRole, MAX_RPC_LINE_BYTES, RpcError, RpcFrame,
    RpcOperationValidation, SignedCallAuthority, call_authority_key_id, decode_rpc_frame,
};
use crate::approval::authority::{
    CommittedEffectReceipt, CommittedExecutionPermit, ExecutorCommittedExecutionPermit,
    MessagingSourceSigningContinuation,
};
use crate::runtime::contracts::{PersonalityAgentId, ProcessGeneration, RpcIdentity};
use crate::tools::{
    ToolError,
    fs::{GrepMatch, MAX_GREP_MATCHES, MAX_GREP_SERIALIZED_BYTES, MAX_SCAN_ENTRIES},
    truncate::{DEFAULT_MAX_LINES, GREP_MAX_LINE_LENGTH, GREP_TRUNCATION_SUFFIX, RetainedOutput},
};

const MAX_EXECUTOR_UPDATES: usize = 65_536;
const GENERATION_ROLLOVER_REQUIRED_MESSAGE: &str = "executor generation rollover required";
const REPLAY_OUTCOME_UNAVAILABLE_MESSAGE: &str = "executor replay outcome is no longer retained";
const CALL_AUTHORITY_REPLAY_MESSAGE: &str = "executor exact-call authority was already consumed";
const CALL_AUTHORITY_CAPACITY_EXHAUSTED_MESSAGE: &str =
    "executor exact-call authority capacity is exhausted";
const CALL_AUTHORITY_STALE_MESSAGE: &str = "executor exact-call authority expired before effect";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutorErrorClassification {
    GenerationRolloverRequired,
    ReplayOutcomeUnavailable,
    CallAuthorityReplay,
    CallAuthorityCapacityExhausted,
    CallAuthorityStale,
}

pub fn classify_executor_error(error: &ToolError) -> Option<ExecutorErrorClassification> {
    match error {
        ToolError::Rpc(message) if message == GENERATION_ROLLOVER_REQUIRED_MESSAGE => {
            Some(ExecutorErrorClassification::GenerationRolloverRequired)
        }
        ToolError::Rpc(message) if message == REPLAY_OUTCOME_UNAVAILABLE_MESSAGE => {
            Some(ExecutorErrorClassification::ReplayOutcomeUnavailable)
        }
        ToolError::Protocol(message) if message == CALL_AUTHORITY_REPLAY_MESSAGE => {
            Some(ExecutorErrorClassification::CallAuthorityReplay)
        }
        ToolError::Rpc(message) if message == CALL_AUTHORITY_CAPACITY_EXHAUSTED_MESSAGE => {
            Some(ExecutorErrorClassification::CallAuthorityCapacityExhausted)
        }
        ToolError::Protocol(message) if message == CALL_AUTHORITY_STALE_MESSAGE => {
            Some(ExecutorErrorClassification::CallAuthorityStale)
        }
        _ => None,
    }
}

#[derive(Clone, Copy)]
struct Deadlines {
    connect: Duration,
    write: Duration,
    frame: Duration,
    overall: Duration,
    cancel: Duration,
    trailing: Duration,
}

impl Default for Deadlines {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(2),
            write: Duration::from_secs(2),
            frame: Duration::from_secs(125),
            overall: Duration::from_secs(130),
            cancel: Duration::from_secs(3),
            trailing: Duration::from_secs(2),
        }
    }
}

/// One Workspace source opened by the executor for a Messaging send: its
/// manifest and the read-only descriptor the executor opened under the
/// Workspace policy. The runtime never opens Workspace paths itself.
pub(crate) struct TransferredSource {
    manifest: super::SourceFileManifest,
    descriptor: std::os::fd::OwnedFd,
}

impl TransferredSource {
    pub(crate) fn manifest(&self) -> &super::SourceFileManifest {
        &self.manifest
    }

    pub(crate) fn into_parts(self) -> (super::SourceFileManifest, std::os::fd::OwnedFd) {
        (self.manifest, self.descriptor)
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        manifest: super::SourceFileManifest,
        descriptor: std::os::fd::OwnedFd,
    ) -> Self {
        Self {
            manifest,
            descriptor,
        }
    }
}

fn validate_transferred_source(
    manifest: &super::SourceFileManifest,
    descriptor: &std::os::fd::OwnedFd,
) -> Result<(), ToolError> {
    use std::os::{fd::AsRawFd, unix::fs::FileExt};

    use sha2::{Digest, Sha256};

    manifest.validate()?;
    let file = std::fs::File::from(descriptor.try_clone()?);
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() != manifest.size_bytes {
        return Err(ToolError::Protocol(
            "executor source descriptor size or type differs from its manifest".to_owned(),
        ));
    }
    let required_seals =
        libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
    // SAFETY: descriptor is an owned live descriptor and F_GET_SEALS has no
    // pointer arguments.
    let seals = unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_GET_SEALS) };
    if seals < 0 || seals & required_seals != required_seals {
        return Err(ToolError::Protocol(
            "executor source descriptor is not an immutable sealed snapshot".to_owned(),
        ));
    }

    let mut digest = Sha256::new();
    let mut offset = 0u64;
    let mut buffer = vec![0u8; 256 * 1024];
    while offset < manifest.size_bytes {
        let remaining = (manifest.size_bytes - offset).min(buffer.len() as u64) as usize;
        let read = file.read_at(&mut buffer[..remaining], offset)?;
        if read == 0 {
            return Err(ToolError::Protocol(
                "executor source descriptor ended before its manifest size".to_owned(),
            ));
        }
        digest.update(&buffer[..read]);
        offset += read as u64;
    }
    let actual = digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if actual != manifest.sha256 {
        return Err(ToolError::Protocol(
            "executor source descriptor digest differs from its manifest".to_owned(),
        ));
    }
    Ok(())
}

/// A single-operation client. Each call gets an isolated Unix service session;
/// a cancellation request, when needed, is sent on that same session.
///
/// Updates are delivered inline and in wire order. As with [`crate::tools::ToolCtx`],
/// the callback must be prompt and nonblocking. The client bounds frame size,
/// update count, frame waits, and the complete exchange. The frozen service can
/// actively cancel Bash; cancellation racing a synchronous non-Bash operation
/// is settled without detaching. A successful synchronous operation followed
/// by `CancelTooLate` is authoritative; every other non-Bash cancel settlement
/// remains indeterminate.
pub struct ExecutorClient {
    socket: PathBuf,
    identity: RpcIdentity,
    call_authority_issuer: Option<Arc<Ed25519CallAuthorityIssuer>>,
    deadlines: Deadlines,
}

impl ExecutorClient {
    pub fn new(socket: impl Into<PathBuf>, identity: RpcIdentity) -> Self {
        Self {
            socket: socket.into(),
            identity,
            call_authority_issuer: None,
            deadlines: Deadlines::default(),
        }
    }

    pub(crate) fn with_call_authority_signing_key(
        mut self,
        signing_key: Zeroizing<[u8; 32]>,
    ) -> Result<Self, ToolError> {
        self.call_authority_issuer = Some(Arc::new(
            Ed25519CallAuthorityIssuer::new(
                call_authority_key_id(),
                SigningKey::from_bytes(&signing_key),
                self.identity.clone(),
            )
            .map_err(ToolError::from)?,
        ));
        Ok(self)
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    pub const fn generation(&self) -> ProcessGeneration {
        self.identity.generation()
    }

    pub(crate) const fn identity(&self) -> &RpcIdentity {
        &self.identity
    }

    pub async fn health(&self) -> Result<(), ToolError> {
        self.health_with_cancellation(CancellationToken::new(), self.deadlines.overall)
            .await
    }

    /// Run one authenticated Health exchange on a fresh Unix connection.
    ///
    /// Health has no execution identity, so cancellation is prompt only before
    /// request emission. After emission the short `overall` bound closes the
    /// connection without manufacturing an invalid empty Cancel operation.
    pub async fn health_with_cancellation(
        &self,
        cancel: CancellationToken,
        overall: Duration,
    ) -> Result<(), ToolError> {
        match self
            .execute_with_overall(
                ExecutorOperation::Health {
                    service_role: ExecutorServiceRole::ToolExecutor,
                },
                None,
                cancel,
                Arc::new(|_| {}),
                overall,
            )
            .await?
        {
            ExecutorResponse::Healthy {
                service_role: ExecutorServiceRole::ToolExecutor,
            } => Ok(()),
            _ => Err(ToolError::Protocol(
                "executor health returned a non-health response".to_owned(),
            )),
        }
    }

    pub async fn execute(
        &self,
        operation: ExecutorOperation,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<ExecutorResponse, ToolError> {
        self.execute_with_overall(operation, None, cancel, on_update, self.deadlines.overall)
            .await
    }

    pub(crate) async fn execute_authorized(
        &self,
        operation: ExecutorOperation,
        permit: CommittedExecutionPermit,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<CommittedEffectReceipt<ExecutorResponse>, ToolError> {
        self.validate_execution_request(&operation, &cancel)?;
        // The adapter has already derived the complete operation from sealed
        // bound arguments. Consume the one-shot permit only after all local
        // validation and cancellation checks, immediately before the future
        // whose first irreversible step signs exact operation claims.
        let effect_start = permit.begin_executor_effect();
        effect_start
            .complete(|permit| {
                self.execute_validated_with_overall(
                    operation,
                    Some(permit),
                    cancel,
                    on_update,
                    self.deadlines.overall,
                )
            })
            .await
    }

    /// Open an exact ordered list of Workspace source files through the
    /// signed executor operation and receive their read-only descriptors on
    /// the same authenticated Unix connection.
    ///
    /// This is the source-transfer leg of the composite Messaging Workspace
    /// send. It signs exactly one operation with the supplied continuation,
    /// reads exactly one terminal frame with `recvmsg` so `SCM_RIGHTS`
    /// descriptors are never dropped, and fails closed on any count, order,
    /// manifest, identity, or truncation mismatch. It produces no effect
    /// receipt of its own.
    pub(crate) async fn execute_source_transfer(
        &self,
        paths: Vec<String>,
        execution_id: String,
        continuation: MessagingSourceSigningContinuation,
        cancel: CancellationToken,
    ) -> Result<Vec<TransferredSource>, ToolError> {
        let operation = ExecutorOperation::OpenSourceFiles {
            paths,
            execution_id,
        };
        self.validate_execution_request(&operation, &cancel)?;
        let request_id = format!("executor-{}", Uuid::now_v7());
        let call_authority = self
            .call_authority_issuer
            .as_ref()
            .ok_or_else(|| {
                ToolError::Protocol(
                    "executor exact-call authority signer is unavailable".to_owned(),
                )
            })?
            .issue(
                request_id.clone(),
                operation.clone(),
                continuation.into_executor_permit(),
            )
            .map_err(ToolError::from)?;
        let encoded = encode_request(
            &self.identity,
            &request_id,
            Some(call_authority),
            operation.clone(),
        )?;
        let exchange = async {
            let mut stream = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(ToolError::Cancelled),
                result = timeout(self.deadlines.connect, UnixStream::connect(&self.socket)) => {
                    result
                        .map_err(|_| ToolError::Rpc("executor connection deadline elapsed".to_owned()))?
                        .map_err(|error| ToolError::Rpc(format!("executor connection failed: {error}")))?
                }
            };
            if cancel.is_cancelled() {
                return Err(ToolError::Cancelled);
            }
            write_with_deadline(&mut stream, &encoded, self.deadlines.write, "request").await?;
            let mut buffer = Vec::with_capacity(16 * 1024);
            let mut chunk = vec![0u8; 64 * 1024];
            let mut descriptors = Vec::new();
            let terminal_line = loop {
                if let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
                    let mut line = buffer.drain(..=newline).collect::<Vec<u8>>();
                    let terminator = line.pop();
                    if terminator != Some(b'\n') || line.iter().any(|byte| *byte == b'\r') {
                        return Err(indeterminate(
                            "executor source transfer terminal has invalid framing",
                        ));
                    }
                    if !buffer.is_empty() {
                        return Err(indeterminate(
                            "executor emitted bytes after the source transfer terminal",
                        ));
                    }
                    break line;
                }
                if buffer.len() > MAX_RPC_LINE_BYTES {
                    return Err(indeterminate(
                        "executor source transfer frame exceeds bounds",
                    ));
                }
                let read = timeout(
                    self.deadlines.frame,
                    super::descriptor_transfer::recv_chunk_with_fds(
                        &stream,
                        &mut chunk,
                        &mut descriptors,
                    ),
                )
                .await
                .map_err(|_| indeterminate("executor response frame deadline elapsed"))?
                .map_err(|error| as_indeterminate(ToolError::Io(error)))?;
                if read == 0 {
                    return Err(indeterminate(
                        "executor closed before the source transfer terminal",
                    ));
                }
                buffer.extend_from_slice(&chunk[..read]);
            };
            let frame = decode_rpc_frame::<ExecutorResponse>(&terminal_line, &self.identity)
                .map_err(as_indeterminate)?;
            let result = match frame {
                RpcFrame::Terminal {
                    request_id: frame_id,
                    result,
                    ..
                } if frame_id == request_id => result,
                RpcFrame::Terminal { .. } => {
                    return Err(indeterminate("executor terminal request_id mismatch"));
                }
                RpcFrame::Update { .. } => {
                    return Err(indeterminate("source transfer must not emit update frames"));
                }
            };
            shutdown_with_deadline(&mut stream, self.deadlines.write).await?;
            let response = result.map_err(|error| map_rpc_error(&operation, error))?;
            validate_response_for_personality_agent(
                &operation,
                &response,
                self.identity.personality_agent_id(),
            )
            .map_err(as_indeterminate)?;
            let ExecutorResponse::SourceFiles { files } = response else {
                return Err(indeterminate(
                    "executor returned a non-source-file response",
                ));
            };
            if files.len() != descriptors.len() {
                return Err(ToolError::Protocol(format!(
                    "executor delivered {} descriptors for {} source files",
                    descriptors.len(),
                    files.len()
                )));
            }
            files
                .into_iter()
                .zip(descriptors)
                .map(|(manifest, descriptor)| {
                    validate_transferred_source(&manifest, &descriptor)?;
                    Ok(TransferredSource {
                        manifest,
                        descriptor,
                    })
                })
                .collect()
        };
        match timeout(self.deadlines.overall, exchange).await {
            Ok(result) => result,
            Err(_) => Err(indeterminate("executor overall exchange deadline elapsed")),
        }
    }

    async fn execute_with_overall(
        &self,
        operation: ExecutorOperation,
        permit: Option<ExecutorCommittedExecutionPermit>,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
        overall: Duration,
    ) -> Result<ExecutorResponse, ToolError> {
        self.validate_execution_request(&operation, &cancel)?;
        self.execute_validated_with_overall(operation, permit, cancel, on_update, overall)
            .await
    }

    fn validate_execution_request(
        &self,
        operation: &ExecutorOperation,
        cancel: &CancellationToken,
    ) -> Result<(), ToolError> {
        operation.validate()?;
        validate_operation_for_personality_agent(operation, self.identity.personality_agent_id())?;
        if matches!(operation, ExecutorOperation::Cancel { .. }) {
            return Err(ToolError::Protocol(
                "ExecutorClient owns cancel request construction".to_owned(),
            ));
        }
        if cancel.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        Ok(())
    }

    async fn execute_validated_with_overall(
        &self,
        operation: ExecutorOperation,
        permit: Option<ExecutorCommittedExecutionPermit>,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
        overall: Duration,
    ) -> Result<ExecutorResponse, ToolError> {
        let request_emitted = Arc::new(AtomicBool::new(false));
        let execution = self.execute_inner(
            operation,
            permit,
            cancel,
            on_update,
            request_emitted.clone(),
        );
        match timeout(overall, execution).await {
            Ok(result) => result,
            Err(_) if request_emitted.load(Ordering::Acquire) => {
                Err(indeterminate("executor overall exchange deadline elapsed"))
            }
            Err(_) => Err(ToolError::Rpc(
                "executor connection deadline elapsed before request emission".to_owned(),
            )),
        }
    }

    async fn execute_inner(
        &self,
        operation: ExecutorOperation,
        permit: Option<ExecutorCommittedExecutionPermit>,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
        request_emitted: Arc<AtomicBool>,
    ) -> Result<ExecutorResponse, ToolError> {
        // The production critical endpoint is a single-frame synchronous
        // exchange. Once an authorized read is emitted it returns the exact
        // primary terminal; it cannot truthfully acknowledge a second-frame
        // Cancel. Cancellation therefore remains prompt only before emission.
        let cancellation_mode = if permit.is_some()
            && super::call_authority::is_production_read_operation(&operation)
        {
            CancellationMode::None
        } else {
            cancellation_mode(&operation)
        };
        let execution_id = operation_execution_id(&operation).to_owned();
        let request_id = format!("executor-{}", Uuid::now_v7());
        let call_authority = match permit {
            Some(permit) => Some(
                self.call_authority_issuer
                    .as_ref()
                    .ok_or_else(|| {
                        ToolError::Protocol(
                            "executor exact-call authority signer is unavailable".to_owned(),
                        )
                    })?
                    .issue(request_id.clone(), operation.clone(), permit)
                    .map_err(ToolError::from)?,
            ),
            None => None,
        };
        let encoded = encode_request(
            &self.identity,
            &request_id,
            call_authority,
            operation.clone(),
        )?;

        let stream = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(ToolError::Cancelled),
            result = timeout(self.deadlines.connect, UnixStream::connect(&self.socket)) => {
                result
                    .map_err(|_| ToolError::Rpc("executor connection deadline elapsed".to_owned()))?
                    .map_err(|error| ToolError::Rpc(format!("executor connection failed: {error}")))?
            }
        };
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);

        if cancel.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        // Once the first write is attempted, a partial JSON line may have
        // reached the service even when write_all reports failure.
        request_emitted.store(true, Ordering::Release);
        write_with_deadline(&mut write, &encoded, self.deadlines.write, "request").await?;

        let mut cancel_request_id = None;
        let mut cancel_terminal = None;
        let mut original_terminal = None;
        let mut update_count = 0usize;
        let mut write_closed = false;
        let mut cancel_deadline = None;

        loop {
            if original_terminal.is_some()
                && (cancel_request_id.is_none() || cancel_terminal.is_some())
                && !write_closed
            {
                shutdown_with_deadline(&mut write, self.deadlines.write).await?;
                write_closed = true;
            }

            if write_closed {
                match timeout(self.deadlines.trailing, read_bounded_line(&mut read)).await {
                    Ok(Ok(None)) => break,
                    Ok(Ok(Some(_))) => {
                        return Err(indeterminate(
                            "executor emitted a trailing or duplicate response frame",
                        ));
                    }
                    Ok(Err(error)) => return Err(as_indeterminate(error)),
                    Err(_) => {
                        return Err(indeterminate(
                            "executor did not close after terminal response",
                        ));
                    }
                }
            }

            let read_deadline =
                cancel_deadline.unwrap_or_else(|| Instant::now() + self.deadlines.frame);
            tokio::select! {
                biased;
                _ = cancel.cancelled(), if cancel_request_id.is_none() && cancellation_mode.sends_cancel() => {
                    let id = format!("executor-cancel-{}", Uuid::now_v7());
                    let cancel_bytes = encode_request(
                        &self.identity,
                        &id,
                        None,
                        ExecutorOperation::Cancel { execution_id: execution_id.clone() },
                    )?;
                    write_with_deadline(
                        &mut write,
                        &cancel_bytes,
                        self.deadlines.write,
                        "cancel request",
                    ).await?;
                    shutdown_with_deadline(&mut write, self.deadlines.write).await?;
                    write_closed = false;
                    cancel_deadline = Some(Instant::now() + self.deadlines.cancel);
                    cancel_request_id = Some(id);
                }
                frame = timeout_at(read_deadline, read_bounded_line(&mut read)) => {
                    let line = match frame {
                        Err(_) => return Err(indeterminate("executor response frame deadline elapsed")),
                        Ok(Err(error)) => return Err(as_indeterminate(error)),
                        Ok(Ok(None)) => return Err(indeterminate("executor closed before all terminal responses")),
                        Ok(Ok(Some(line))) => line,
                    };
                    let frame = decode_rpc_frame::<ExecutorResponse>(&line, &self.identity)
                        .map_err(as_indeterminate)?;
                    match frame {
                        RpcFrame::Update { request_id: frame_id, value, .. } => {
                            if frame_id != request_id || original_terminal.is_some() {
                                return Err(indeterminate("executor update identity or ordering mismatch"));
                            }
                            update_count = update_count.checked_add(1)
                                .ok_or_else(|| indeterminate("executor update count overflow"))?;
                            if update_count > MAX_EXECUTOR_UPDATES {
                                return Err(indeterminate("executor update limit exceeded"));
                            }
                            if catch_unwind(AssertUnwindSafe(|| on_update(value))).is_err() {
                                return Err(indeterminate("executor update callback panicked"));
                            }
                        }
                        RpcFrame::Terminal { request_id: frame_id, result, .. }
                            if frame_id == request_id =>
                        {
                            if original_terminal.is_some() {
                                return Err(indeterminate("executor emitted duplicate operation terminal"));
                            }
                            let response = result.map_err(|error| map_rpc_error(&operation, error));
                            if let Ok(response) = &response {
                                validate_response_for_personality_agent(
                                    &operation,
                                    response,
                                    self.identity.personality_agent_id(),
                                )
                                    .map_err(as_indeterminate)?;
                            }
                            original_terminal = Some(response);
                        }
                        RpcFrame::Terminal { request_id: frame_id, result, .. }
                            if cancel_request_id.as_deref() == Some(frame_id.as_str()) =>
                        {
                            if cancel_terminal.is_some() {
                                return Err(indeterminate("executor emitted duplicate cancel terminal"));
                            }
                            match result {
                                Ok(ExecutorResponse::CancelAccepted {}) => {
                                    cancel_terminal = Some(CancelTerminal::Accepted)
                                }
                                Ok(ExecutorResponse::CancelTooLate {}) => {
                                    cancel_terminal = Some(CancelTerminal::TooLate)
                                }
                                _ => return Err(indeterminate("executor rejected or malformed cancellation")),
                            }
                        }
                        RpcFrame::Terminal { .. } => {
                            return Err(indeterminate("executor terminal request_id mismatch"));
                        }
                    }
                }
            }
        }

        let result = original_terminal
            .ok_or_else(|| indeterminate("executor response lacked operation terminal"))?;
        validate_cancel_settlement(
            cancel_request_id.is_some(),
            cancellation_mode.is_active_bash(),
            cancel_terminal,
            &result,
        )?;
        result
    }

    #[cfg(test)]
    fn with_deadlines(mut self, deadlines: Deadlines) -> Self {
        self.deadlines = deadlines;
        self
    }
}

fn validate_cancel_settlement(
    cancellation_requested: bool,
    cancellable_bash: bool,
    cancel_terminal: Option<CancelTerminal>,
    result: &Result<ExecutorResponse, ToolError>,
) -> Result<(), ToolError> {
    match (
        cancellation_requested,
        cancellable_bash,
        cancel_terminal,
        result,
    ) {
        (false, _, None, _) => {}
        (true, true, Some(CancelTerminal::Accepted), Ok(ExecutorResponse::Bash { result }))
            if result.cancelled => {}
        (true, true, Some(CancelTerminal::TooLate), Ok(ExecutorResponse::Bash { result }))
            if !result.cancelled => {}
        (true, false, Some(CancelTerminal::TooLate), Ok(_)) => {}
        (true, _, Some(CancelTerminal::Accepted), _) => {
            return Err(indeterminate(
                "executor acknowledged cancellation without cancelled=true",
            ));
        }
        (true, _, Some(CancelTerminal::TooLate), _) => {
            return Err(indeterminate(
                "executor reported cancel-too-late without an authoritative completed result",
            ));
        }
        (true, _, None, _) => {
            return Err(indeterminate(
                "executor cancellation lacked a terminal settlement",
            ));
        }
        (false, _, Some(_), _) => {
            return Err(indeterminate(
                "executor emitted an unsolicited cancellation settlement",
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum CancelTerminal {
    Accepted,
    TooLate,
}

#[derive(Clone, Copy)]
enum CancellationMode {
    /// Health and production single-frame reads are cancellable only before
    /// request emission. After emission their primary terminal is truth.
    None,
    /// Synchronous executor operations cannot be actively stopped, but a
    /// post-emission cancellation must be settled against their terminal.
    SettlementOnly,
    /// Bash is the one executor operation the service can actively stop.
    ActiveBash,
}

impl CancellationMode {
    const fn sends_cancel(self) -> bool {
        !matches!(self, Self::None)
    }

    const fn is_active_bash(self) -> bool {
        matches!(self, Self::ActiveBash)
    }
}

fn cancellation_mode(operation: &ExecutorOperation) -> CancellationMode {
    match operation {
        ExecutorOperation::Health { .. } | ExecutorOperation::Cancel { .. } => {
            CancellationMode::None
        }
        ExecutorOperation::Bash { .. } => CancellationMode::ActiveBash,
        ExecutorOperation::ReadFile { .. }
        | ExecutorOperation::WriteFile { .. }
        | ExecutorOperation::EditFile { .. }
        | ExecutorOperation::RemoveFile { .. }
        | ExecutorOperation::ListDir { .. }
        | ExecutorOperation::Glob { .. }
        | ExecutorOperation::Grep { .. }
        | ExecutorOperation::OpenSourceFiles { .. } => CancellationMode::SettlementOnly,
    }
}

fn encode_request(
    identity: &RpcIdentity,
    request_id: &str,
    call_authority: Option<SignedCallAuthority>,
    operation: ExecutorOperation,
) -> Result<Vec<u8>, ToolError> {
    let request = ExecutorRpcRequest {
        personality_agent_id: identity.personality_agent_id().clone(),
        generation: identity.generation().to_wire(),
        nonce: identity.nonce().as_str().to_owned(),
        request_id: request_id.to_owned(),
        call_authority,
        verified_call_authority: None,
        operation,
    };
    let mut encoded = serde_json::to_vec(&request)
        .map_err(|error| ToolError::Protocol(format!("executor request encode failed: {error}")))?;
    if encoded
        .len()
        .checked_add(1)
        .is_none_or(|length| length > MAX_RPC_LINE_BYTES)
    {
        return Err(ToolError::Protocol(
            "executor request exceeds 1MiB".to_owned(),
        ));
    }
    encoded.push(b'\n');
    Ok(encoded)
}

async fn write_with_deadline<W: AsyncWrite + Unpin>(
    write: &mut W,
    bytes: &[u8],
    deadline: Duration,
    kind: &str,
) -> Result<(), ToolError> {
    timeout(deadline, async {
        write.write_all(bytes).await?;
        write.flush().await
    })
    .await
    .map_err(|_| indeterminate(&format!("executor {kind} write deadline elapsed")))?
    .map_err(|error| indeterminate(&format!("executor {kind} write failed: {error}")))
}

async fn shutdown_with_deadline<W: AsyncWrite + Unpin>(
    write: &mut W,
    deadline: Duration,
) -> Result<(), ToolError> {
    timeout(deadline, write.shutdown())
        .await
        .map_err(|_| indeterminate("executor request shutdown deadline elapsed"))?
        .map_err(|error| indeterminate(&format!("executor request shutdown failed: {error}")))
}

async fn read_bounded_line<R: AsyncBufRead + Unpin>(
    read: &mut R,
) -> Result<Option<Vec<u8>>, ToolError> {
    let mut line = Vec::with_capacity(4096);
    loop {
        let buffer = read
            .fill_buf()
            .await
            .map_err(|error| ToolError::Rpc(format!("executor response read failed: {error}")))?;
        if buffer.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Err(ToolError::Protocol(
                    "executor response ended before newline".to_owned(),
                ))
            };
        }
        let separator = buffer.iter().position(|byte| matches!(byte, b'\n' | b'\r'));
        let take = separator.unwrap_or(buffer.len());
        if line.len().saturating_add(take) > MAX_RPC_LINE_BYTES - 1 {
            return Err(ToolError::Protocol(
                "executor response exceeds 1MiB".to_owned(),
            ));
        }
        line.extend_from_slice(&buffer[..take]);
        if let Some(position) = separator {
            let delimiter = buffer[position];
            read.consume(position + 1);
            if delimiter == b'\r' {
                return Err(ToolError::Protocol(
                    "executor response contained carriage return".to_owned(),
                ));
            }
            if line.is_empty() {
                return Err(ToolError::Protocol(
                    "executor emitted an empty response frame".to_owned(),
                ));
            }
            return Ok(Some(line));
        }
        read.consume(take);
    }
}

fn operation_execution_id(operation: &ExecutorOperation) -> &str {
    match operation {
        ExecutorOperation::Health { .. } => "",
        ExecutorOperation::ReadFile { execution_id, .. }
        | ExecutorOperation::WriteFile { execution_id, .. }
        | ExecutorOperation::EditFile { execution_id, .. }
        | ExecutorOperation::RemoveFile { execution_id, .. }
        | ExecutorOperation::ListDir { execution_id, .. }
        | ExecutorOperation::Glob { execution_id, .. }
        | ExecutorOperation::Grep { execution_id, .. }
        | ExecutorOperation::OpenSourceFiles { execution_id, .. }
        | ExecutorOperation::Bash { execution_id, .. }
        | ExecutorOperation::Cancel { execution_id } => execution_id,
    }
}

fn validate_response_for_personality_agent(
    operation: &ExecutorOperation,
    response: &ExecutorResponse,
    personality_agent_id: &PersonalityAgentId,
) -> Result<(), ToolError> {
    validate_operation_for_personality_agent(operation, personality_agent_id)?;
    let valid = match (operation, response) {
        (
            ExecutorOperation::Health {
                service_role: requested,
            },
            ExecutorResponse::Healthy {
                service_role: responding,
            },
        ) => requested == responding && *responding == ExecutorServiceRole::ToolExecutor,
        (
            ExecutorOperation::ReadFile { path, limit, .. },
            ExecutorResponse::ReadFile { result },
        ) => !path.starts_with("artifact://") && read_file_result_within_limit(result, *limit),
        (
            ExecutorOperation::ReadFile { path, limit, .. },
            ExecutorResponse::Artifact {
                response: ArtifactResponse::Read { content, .. },
            },
        ) => path.starts_with("artifact://") && content.len() <= *limit,
        (ExecutorOperation::Grep { path, .. }, ExecutorResponse::Grepped { matches }) => {
            !path.starts_with("artifact://") && workspace_grep_matches_are_bounded(matches)
        }
        (
            ExecutorOperation::Grep { path, .. },
            ExecutorResponse::Artifact {
                response: ArtifactResponse::Grep { matches },
            },
        ) => path.starts_with("artifact://") && artifact_grep_matches_are_bounded(matches),
        (ExecutorOperation::WriteFile { .. }, ExecutorResponse::Written {})
        | (ExecutorOperation::EditFile { .. }, ExecutorResponse::Edited {})
        | (ExecutorOperation::RemoveFile { .. }, ExecutorResponse::Removed {}) => true,
        (ExecutorOperation::ListDir { .. }, ExecutorResponse::Listed { entries }) => {
            entries.len() <= MAX_SCAN_ENTRIES && entries.iter().all(|entry| valid_entry(entry))
        }
        (ExecutorOperation::Glob { .. }, ExecutorResponse::Globbed { paths }) => {
            paths.len() <= MAX_SCAN_ENTRIES && paths.iter().all(|path| valid_relative_path(path))
        }
        (
            ExecutorOperation::OpenSourceFiles { paths, .. },
            ExecutorResponse::SourceFiles { files },
        ) => {
            files.len() == paths.len()
                && files
                    .iter()
                    .zip(paths)
                    .all(|(file, path)| file.validate().is_ok() && &file.path == path)
                && files
                    .iter()
                    .map(|file| file.size_bytes)
                    .try_fold(0u64, |total, size| total.checked_add(size))
                    .is_some_and(|total| total <= super::protocol::MAX_SOURCE_FILES_TOTAL_BYTES)
        }
        (ExecutorOperation::Bash { execution_id, .. }, ExecutorResponse::Bash { result }) => {
            result.is_consistent()
                && result.artifact_handle.as_deref().is_none_or(|handle| {
                    parse_artifact_handle(handle).is_ok_and(|parsed| {
                        parsed.kind == super::protocol::ArtifactKind::ToolOutput
                            && parsed.artifact_id == execution_id
                            && &parsed.personality_agent_id == personality_agent_id
                    })
                })
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(ToolError::Protocol(
            "executor returned a response for a different operation".to_owned(),
        ))
    }
}

fn validate_operation_for_personality_agent(
    operation: &ExecutorOperation,
    personality_agent_id: &PersonalityAgentId,
) -> Result<(), ToolError> {
    let path = match operation {
        ExecutorOperation::ReadFile { path, .. } | ExecutorOperation::Grep { path, .. } => path,
        _ => return Ok(()),
    };
    if !path.starts_with("artifact://") {
        return Ok(());
    }
    let parsed = parse_artifact_handle(path)?;
    if &parsed.personality_agent_id == personality_agent_id {
        Ok(())
    } else {
        Err(ToolError::InvalidPath(
            "artifact belongs to another personality agent".to_owned(),
        ))
    }
}

#[cfg(test)]
fn validate_response(
    operation: &ExecutorOperation,
    response: &ExecutorResponse,
) -> Result<(), ToolError> {
    validate_response_for_personality_agent(
        operation,
        response,
        &PersonalityAgentId::parse("0198f0f4-9b72-7000-8000-000000000001")
            .expect("canonical UUIDv7"),
    )
}

fn read_file_result_within_limit(
    result: &crate::tools::truncate::TruncationResult,
    limit: usize,
) -> bool {
    result.max_lines == DEFAULT_MAX_LINES
        && result.max_bytes == limit
        && result.is_consistent(RetainedOutput::Head)
}

fn valid_entry(entry: &str) -> bool {
    !entry.is_empty()
        && entry.len() < MAX_RPC_LINE_BYTES
        && !entry.contains(['\0', '/'])
        && entry != "."
        && entry != ".."
}

fn valid_relative_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() < MAX_RPC_LINE_BYTES
        && !value.contains('\0')
        && Path::new(value)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn valid_grep_line(line: &str, line_truncated: bool) -> bool {
    let chars = line.chars().count();
    chars <= GREP_MAX_LINE_LENGTH
        && (!line_truncated
            || (chars == GREP_MAX_LINE_LENGTH && line.ends_with(GREP_TRUNCATION_SUFFIX)))
}

fn workspace_grep_matches_are_bounded(matches: &[GrepMatch]) -> bool {
    matches.len() <= MAX_GREP_MATCHES
        && serde_json::to_vec(&ExecutorResponse::Grepped {
            matches: matches.to_vec(),
        })
        .is_ok_and(|encoded| encoded.len() <= MAX_GREP_SERIALIZED_BYTES)
        && matches.iter().all(|item| {
            item.line_number > 0
                && valid_relative_path(&item.path)
                && valid_grep_line(&item.line, item.line_truncated)
        })
}

fn artifact_grep_matches_are_bounded(
    matches: &[crate::tools::executor::ArtifactGrepMatch],
) -> bool {
    matches.len() <= MAX_GREP_MATCHES
        && serde_json::to_vec(&ArtifactResponse::Grep {
            matches: matches.to_vec(),
        })
        .is_ok_and(|encoded| encoded.len() <= MAX_GREP_SERIALIZED_BYTES)
        && matches
            .iter()
            .all(|item| item.line_number > 0 && valid_grep_line(&item.line, item.line_truncated))
}

fn map_rpc_error(operation: &ExecutorOperation, error: RpcError) -> ToolError {
    let mutating = matches!(
        operation,
        ExecutorOperation::WriteFile { .. }
            | ExecutorOperation::EditFile { .. }
            | ExecutorOperation::RemoveFile { .. }
    );
    match (error.code.as_str(), error.resource_limit) {
        (RPC_BOOT_UNIQUENESS_EXHAUSTED_CODE, None) => {
            ToolError::Rpc(GENERATION_ROLLOVER_REQUIRED_MESSAGE.to_owned())
        }
        (RPC_REPLAY_OUTCOME_UNAVAILABLE_CODE, None) => {
            ToolError::Rpc(REPLAY_OUTCOME_UNAVAILABLE_MESSAGE.to_owned())
        }
        (RPC_CALL_AUTHORITY_REPLAY_CODE, None) => {
            ToolError::Protocol(CALL_AUTHORITY_REPLAY_MESSAGE.to_owned())
        }
        (RPC_CALL_AUTHORITY_CAPACITY_EXHAUSTED_CODE, None) => {
            ToolError::Rpc(CALL_AUTHORITY_CAPACITY_EXHAUSTED_MESSAGE.to_owned())
        }
        (RPC_CALL_AUTHORITY_STALE_CODE, None) => {
            ToolError::Protocol(CALL_AUTHORITY_STALE_MESSAGE.to_owned())
        }
        ("resource_limit", Some(limit)) => ToolError::ResourceLimit(limit),
        ("cancelled", None) if !mutating => ToolError::Cancelled,
        ("invalid_arguments", None) => ToolError::InvalidArguments,
        ("invalid_path", None) => ToolError::InvalidPath("executor path rejected".to_owned()),
        ("protocol", None) => ToolError::Protocol("executor rejected request".to_owned()),
        ("rpc_indeterminate", None) => {
            ToolError::RpcIndeterminate("executor reported an indeterminate outcome".to_owned())
        }
        ("io", None) if !mutating => ToolError::Rpc("executor I/O operation failed".to_owned()),
        (_, _) if mutating => ToolError::RpcIndeterminate(
            "executor mutating operation failed after request emission".to_owned(),
        ),
        (_, _) => ToolError::Rpc("executor operation failed".to_owned()),
    }
}

fn as_indeterminate(error: ToolError) -> ToolError {
    match error {
        ToolError::RpcIndeterminate(_) => error,
        _ => ToolError::RpcIndeterminate(error.to_string()),
    }
}

fn indeterminate(message: &str) -> ToolError {
    ToolError::RpcIndeterminate(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAID: &str = "0198f0f4-9b72-7000-8000-000000000001";
    use crate::{
        approval::authority::{CommittedExecutionPermit, ExecutionAuthorityProvenance},
        provider::types::ToolInvocationRoute,
        tools::{
            executor::{
                ArtifactBrokerClient,
                service::{
                    ExecutorTestControls, run_critical_executor_test_service, run_executor_service,
                    run_executor_service_with_cancel_delay,
                },
            },
            fs::WorkspaceFs,
        },
    };
    use serde_json::{Value, json};
    use std::sync::Mutex;
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::UnixListener,
        sync::{Semaphore, oneshot},
        task::JoinHandle,
    };

    fn identity() -> RpcIdentity {
        RpcIdentity::from_wire(PAID, 7, "boot-nonce").unwrap()
    }

    #[test]
    fn outgoing_request_rejects_out_of_domain_generation_before_encoding() {
        let error = RpcIdentity::from_wire(
            PAID,
            crate::runtime::contracts::MAX_PROCESS_GENERATION + 1,
            "boot-nonce",
        )
        .expect_err("invalid generation");
        assert!(error.to_string().contains("generation"));
    }

    fn test_deadlines() -> Deadlines {
        Deadlines {
            connect: Duration::from_millis(100),
            write: Duration::from_millis(100),
            frame: Duration::from_secs(3),
            overall: Duration::from_secs(5),
            cancel: Duration::from_secs(3),
            trailing: Duration::from_millis(100),
        }
    }

    fn temp_root(label: &str) -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("sumi-executor-client-{label}-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn spawn_real_service(root: &Path, connections: usize) -> (PathBuf, JoinHandle<()>) {
        let socket = root.join("executor.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let broker_socket = root.join("unused-broker.sock");
        let task = tokio::spawn(async move {
            let mut sessions = Vec::new();
            for _ in 0..connections {
                let (stream, _) = listener.accept().await.unwrap();
                let workspace = workspace.clone();
                let broker_socket = broker_socket.clone();
                sessions.push(tokio::spawn(async move {
                    let fs = WorkspaceFs::open(&workspace).unwrap();
                    let broker = ArtifactBrokerClient::new(broker_socket, identity());
                    let (read, write) = stream.into_split();
                    run_executor_service(read, write, identity(), workspace, fs, broker)
                        .await
                        .unwrap();
                }));
            }
            for session in sessions {
                session.await.unwrap();
            }
        });
        (socket, task)
    }

    fn spawn_real_service_with_cancel_delay(
        root: &Path,
        cancel_stop_delay: Duration,
    ) -> (PathBuf, oneshot::Receiver<()>, JoinHandle<()>) {
        let socket = root.join("executor.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let broker_socket = root.join("unused-broker.sock");
        let (cancel_ingested, cancel_ingested_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let fs = WorkspaceFs::open(&workspace).unwrap();
            let broker = ArtifactBrokerClient::new(broker_socket, identity());
            let (read, write) = stream.into_split();
            run_executor_service_with_cancel_delay(
                read,
                write,
                identity(),
                workspace,
                fs,
                broker,
                ExecutorTestControls::observe_cancel(cancel_stop_delay, cancel_ingested),
            )
            .await
            .unwrap();
        });
        (socket, cancel_ingested_rx, task)
    }

    fn spawn_real_service_cancel_race(
        root: &Path,
        cancel: CancellationToken,
    ) -> (PathBuf, JoinHandle<()>) {
        let socket = root.join("executor.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let broker_socket = root.join("unused-broker.sock");
        let task = tokio::spawn(async move {
            let (mut client_stream, _) = listener.accept().await.unwrap();
            let (mut proxy_stream, executor_stream) = tokio::io::duplex(2 * MAX_RPC_LINE_BYTES);
            let (read, write) = tokio::io::split(executor_stream);
            let service = tokio::spawn(async move {
                let fs = WorkspaceFs::open(&workspace).unwrap();
                let broker = ArtifactBrokerClient::new(broker_socket, identity());
                run_executor_service(read, write, identity(), workspace, fs, broker)
                    .await
                    .unwrap();
            });

            // Forward exactly the original request frame, then trigger cancel.
            // The real service is now committed to its synchronous operation
            // and cannot observe the following Cancel until it completes.
            let mut request = Vec::new();
            loop {
                let mut byte = [0u8; 1];
                client_stream.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                if byte[0] == b'\n' {
                    break;
                }
            }
            proxy_stream.write_all(&request).await.unwrap();
            cancel.cancel();
            tokio::io::copy_bidirectional(&mut client_stream, &mut proxy_stream)
                .await
                .unwrap();
            service.await.unwrap();
        });
        (socket, task)
    }

    async fn read_request(read: &mut BufReader<tokio::net::unix::OwnedReadHalf>) -> Value {
        let mut line = String::new();
        read.read_line(&mut line).await.unwrap();
        serde_json::from_str(&line).unwrap()
    }

    async fn write_json_line(write: &mut tokio::net::unix::OwnedWriteHalf, value: Value) {
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        write.write_all(&bytes).await.unwrap();
    }

    fn write_operation(execution_id: &str, path: &str, content: &str) -> ExecutorOperation {
        ExecutorOperation::WriteFile {
            path: path.to_owned(),
            content: content.to_owned(),
            execution_id: execution_id.to_owned(),
        }
    }

    #[tokio::test]
    async fn real_service_success_and_ordered_updates() {
        let root = temp_root("success-updates");
        let (socket, service) = spawn_real_service(&root, 2);
        let client = ExecutorClient::new(&socket, identity()).with_deadlines(test_deadlines());
        let response = client
            .execute(
                write_operation("write-1", "written.txt", "content"),
                CancellationToken::new(),
                Arc::new(|_| panic!("write must not update")),
            )
            .await
            .unwrap();
        assert_eq!(response, ExecutorResponse::Written {});
        assert_eq!(
            std::fs::read_to_string(root.join("workspace/written.txt")).unwrap(),
            "content"
        );

        let updates = Arc::new(Mutex::new(Vec::new()));
        let updates_callback = updates.clone();
        let response = client
            .execute(
                ExecutorOperation::Bash {
                    command: "printf first; sleep 0.05; printf second".to_owned(),
                    execution_id: "bash-updates".to_owned(),
                },
                CancellationToken::new(),
                Arc::new(move |value| updates_callback.lock().unwrap().push(value)),
            )
            .await
            .unwrap();
        let ExecutorResponse::Bash { result } = response else {
            panic!("wrong response")
        };
        assert_eq!(result.output, "firstsecond");
        let streamed = updates
            .lock()
            .unwrap()
            .iter()
            .filter_map(|value| value.get("output").and_then(Value::as_str))
            .collect::<String>();
        assert!(
            "firstsecond".starts_with(&streamed),
            "delivered updates were not an ordered prefix: {streamed:?}"
        );
        service.await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn real_service_streams_oversized_normal_output_before_complete_terminal() {
        let root = temp_root("oversized-progress");
        let (socket, service) = spawn_real_service(&root, 1);
        let updates = Arc::new(Mutex::new(Vec::new()));
        let updates_callback = updates.clone();
        let response = ExecutorClient::new(&socket, identity())
            .with_deadlines(test_deadlines())
            .execute(
                ExecutorOperation::Bash {
                    command: "head -c 8192 /dev/zero | tr '\\0' x; sleep 0.05".to_owned(),
                    execution_id: "bash-oversized-progress".to_owned(),
                },
                CancellationToken::new(),
                Arc::new(move |value| updates_callback.lock().unwrap().push(value)),
            )
            .await
            .expect("real service result");
        let ExecutorResponse::Bash { result } = response else {
            panic!("wrong response")
        };
        assert_eq!(result.output, "x".repeat(8_192));
        assert!(result.is_consistent());
        assert!(
            updates.lock().unwrap().iter().any(|value| value["output"]
                .as_str()
                .is_some_and(|text| !text.is_empty())),
            "oversized normal output must deliver progress before the terminal"
        );
        service.await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn real_service_cancellation_waits_for_ack_and_terminal() {
        let root = temp_root("cancel");
        let (socket, service) = spawn_real_service(&root, 1);
        let client = ExecutorClient::new(&socket, identity()).with_deadlines(test_deadlines());
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        let started = Arc::new(Semaphore::new(0));
        let started_update = started.clone();
        tokio::spawn(async move {
            timeout(Duration::from_secs(3), started.acquire())
                .await
                .expect("started output timeout")
                .expect("started output semaphore closed")
                .forget();
            trigger.cancel();
        });
        let response = client
            .execute(
                ExecutorOperation::Bash {
                    command: "printf started; sleep 30".to_owned(),
                    execution_id: "bash-cancel".to_owned(),
                },
                cancel,
                Arc::new(move |value| {
                    if value["output"]
                        .as_str()
                        .is_some_and(|output| output.contains("started"))
                    {
                        started_update.add_permits(1);
                    }
                }),
            )
            .await
            .unwrap();
        let ExecutorResponse::Bash { result } = response else {
            panic!("wrong response")
        };
        assert!(result.cancelled);
        assert!(result.output.contains("started"));
        service.await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn real_service_cancellation_remains_authoritative_at_output_quota() {
        let root = temp_root("cancel-at-output-quota");
        let output_limit = crate::tools::shell_capture::COMMAND_OUTPUT_LIMIT_BYTES;
        let (socket, cancel_ingested, service) =
            spawn_real_service_with_cancel_delay(&root, Duration::from_millis(50));
        let cancel = CancellationToken::new();
        let cancel_at_marker = cancel.clone();
        let marker = root.join("workspace/quota-ready");
        let release = root.join("workspace/quota-release");
        let trigger = tokio::spawn(async move {
            timeout(Duration::from_secs(5), async {
                while !marker.exists() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("quota marker timeout");
            cancel_at_marker.cancel();
            timeout(Duration::from_secs(3), cancel_ingested)
                .await
                .expect("executor cancel-ingestion timeout")
                .expect("executor stopped before ingesting cancel");
            std::fs::write(release, b"release").expect("release final quota bytes");
        });
        let mut deadlines = test_deadlines();
        deadlines.frame = Duration::from_secs(15);
        deadlines.overall = Duration::from_secs(20);
        let response = ExecutorClient::new(&socket, identity())
            .with_deadlines(deadlines)
            .execute(
                ExecutorOperation::Bash {
                    command: "head -c 10477568 /dev/zero | tr '\\0' x; : > quota-ready; while [ ! -e quota-release ]; do sleep 0.001; done; head -c 8192 /dev/zero | tr '\\0' x; while :; do :; done".to_owned(),
                    execution_id: "bash-cancel-at-quota".to_owned(),
                },
                cancel,
                Arc::new(|_| {}),
            )
            .await
            .expect("cancelled quota-race result");
        let ExecutorResponse::Bash { result } = response else {
            panic!("wrong response")
        };
        assert!(result.cancelled);
        assert_eq!(result.resource_limit, None);
        assert!(result.is_consistent());
        assert_eq!(
            result.observed_bytes, output_limit,
            "fixture must reach the output quota concurrently with cancellation"
        );
        trigger.await.unwrap();
        service.await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn concurrent_clients_remain_execution_isolated() {
        let root = temp_root("concurrent");
        let (socket, service) = spawn_real_service(&root, 2);
        let first = ExecutorClient::new(&socket, identity()).with_deadlines(test_deadlines());
        let second = ExecutorClient::new(&socket, identity()).with_deadlines(test_deadlines());
        let (first, second) = tokio::join!(
            first.execute(
                write_operation("execution-a", "a.txt", "alpha"),
                CancellationToken::new(),
                Arc::new(|_| {}),
            ),
            second.execute(
                write_operation("execution-b", "b.txt", "beta"),
                CancellationToken::new(),
                Arc::new(|_| {}),
            ),
        );
        assert_eq!(first.unwrap(), ExecutorResponse::Written {});
        assert_eq!(second.unwrap(), ExecutorResponse::Written {});
        assert_eq!(
            std::fs::read_to_string(root.join("workspace/a.txt")).unwrap(),
            "alpha"
        );
        assert_eq!(
            std::fs::read_to_string(root.join("workspace/b.txt")).unwrap(),
            "beta"
        );
        service.await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn health_authenticates_real_service_and_rejects_untrusted_endpoints() {
        let root = temp_root("health-real");
        let (socket, service) = spawn_real_service(&root, 1);
        ExecutorClient::new(&socket, identity())
            .with_deadlines(test_deadlines())
            .health()
            .await
            .expect("healthy executor");
        service.await.unwrap();
        std::fs::remove_dir_all(root).unwrap();

        for mode in [
            "wrong-paid",
            "wrong-generation",
            "wrong-nonce",
            "wrong-role",
            "malformed",
            "stalled",
            "eof",
            "trailing",
            "duplicate",
            "wrong-request-id",
            "rpc-error",
        ] {
            let root = temp_root(&format!("health-{mode}"));
            let socket = root.join("executor.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let (read, mut write) = stream.into_split();
                let request = read_request(&mut BufReader::new(read)).await;
                match mode {
                    "wrong-paid" | "wrong-generation" | "wrong-nonce" | "wrong-role" => {
                        let personality_agent_id = if mode == "wrong-paid" {
                            "0198f0f4-9b72-7000-8000-000000000002"
                        } else {
                            PAID
                        };
                        let generation = if mode == "wrong-generation" { 8 } else { 7 };
                        let nonce = if mode == "wrong-nonce" {
                            "wrong"
                        } else {
                            "boot-nonce"
                        };
                        let service_role = if mode == "wrong-role" {
                            "artifact_broker"
                        } else {
                            "tool_executor"
                        };
                        write_json_line(
                            &mut write,
                            json!({
                                "type":"terminal",
                                "personality_agent_id":personality_agent_id,
                                "generation":generation,
                                "nonce":nonce,
                                "request_id":request["request_id"],
                                "result":{"Ok":{
                                    "type":"healthy",
                                    "service_role":service_role
                                }}
                            }),
                        )
                        .await;
                    }
                    "malformed" => write.write_all(b"not-json\n").await.unwrap(),
                    "stalled" => tokio::time::sleep(Duration::from_secs(1)).await,
                    "eof" => {}
                    "trailing" => {
                        write_json_line(
                            &mut write,
                            json!({
                                "type":"terminal",
                                "personality_agent_id":PAID,
                                "generation":7,
                                "nonce":"boot-nonce",
                                "request_id":request["request_id"],
                                "result":{"Ok":{
                                    "type":"healthy",
                                    "service_role":"tool_executor"
                                }}
                            }),
                        )
                        .await;
                        write.write_all(b"x").await.unwrap();
                    }
                    "duplicate" => {
                        for _ in 0..2 {
                            write_json_line(
                                &mut write,
                                json!({
                                    "type":"terminal",
                                    "personality_agent_id":PAID,
                                    "generation":7,
                                    "nonce":"boot-nonce",
                                    "request_id":request["request_id"],
                                    "result":{"Ok":{
                                        "type":"healthy",
                                        "service_role":"tool_executor"
                                    }}
                                }),
                            )
                            .await;
                        }
                    }
                    "wrong-request-id" => {
                        write_json_line(
                            &mut write,
                            json!({
                                "type":"terminal",
                                "personality_agent_id":PAID,
                                "generation":7,
                                "nonce":"boot-nonce",
                                "request_id":"wrong-request",
                                "result":{"Ok":{
                                    "type":"healthy",
                                    "service_role":"tool_executor"
                                }}
                            }),
                        )
                        .await;
                    }
                    "rpc-error" => {
                        write_json_line(
                            &mut write,
                            json!({
                                "type":"terminal",
                                "personality_agent_id":PAID,
                                "generation":7,
                                "nonce":"boot-nonce",
                                "request_id":request["request_id"],
                                "result":{"Err":{"code":"protocol"}}
                            }),
                        )
                        .await;
                    }
                    _ => unreachable!(),
                }
            });
            let mut deadlines = test_deadlines();
            deadlines.frame = Duration::from_millis(80);
            deadlines.overall = Duration::from_millis(250);
            let error = ExecutorClient::new(&socket, identity())
                .with_deadlines(deadlines)
                .health()
                .await
                .expect_err("untrusted health endpoint");
            if mode == "rpc-error" {
                assert!(matches!(error, ToolError::Protocol(_)), "{mode}: {error:?}");
            } else {
                assert!(
                    matches!(error, ToolError::RpcIndeterminate(_)),
                    "{mode}: {error:?}"
                );
            }
            server.await.unwrap();
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test]
    async fn health_cancellation_after_emission_never_sends_an_empty_cancel() {
        let root = temp_root("health-cancel-after-emission");
        let socket = root.join("executor.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let cancel = CancellationToken::new();
        let cancel_server = cancel.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut read = BufReader::new(read);
            let request = read_request(&mut read).await;
            assert_eq!(request["operation"]["type"], "health");

            // The request is now on the wire, but the executor has not replied.
            // Health has no execution identity to cancel, so the client must wait
            // for its authenticated terminal rather than emit Cancel { "" }.
            cancel_server.cancel();
            assert!(
                timeout(Duration::from_millis(100), read_request(&mut read))
                    .await
                    .is_err(),
                "health cancellation must not send a follow-up cancel request"
            );
            write_json_line(
                &mut write,
                json!({
                    "type":"terminal", "personality_agent_id":PAID, "generation":7, "nonce":"boot-nonce",
                    "request_id":request["request_id"],
                    "result":{"Ok":{"type":"healthy", "service_role":"tool_executor"}}
                }),
            )
            .await;
        });

        let response = ExecutorClient::new(&socket, identity())
            .with_deadlines(test_deadlines())
            .execute(
                ExecutorOperation::Health {
                    service_role: ExecutorServiceRole::ToolExecutor,
                },
                cancel,
                Arc::new(|_| {}),
            )
            .await
            .expect("post-emission health cancellation must preserve health result");
        assert_eq!(
            response,
            ExecutorResponse::Healthy {
                service_role: ExecutorServiceRole::ToolExecutor,
            }
        );
        server.await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn production_read_post_emission_cancel_preserves_primary_without_cancel_frame() {
        let root = temp_root("prd-cancel");
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("visible.txt"), "visible").unwrap();
        let socket = root.join("executor.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let cancel = CancellationToken::new();
        let cancel_server = cancel.clone();
        let service_identity = identity();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, write) = stream.into_split();
            let mut read = BufReader::new(read);
            let mut first_line = Vec::new();
            let count = read.read_until(b'\n', &mut first_line).await.unwrap();
            assert!(count > 0, "authorized request must reach production socket");
            assert_eq!(first_line.pop(), Some(b'\n'));
            let request: Value = serde_json::from_slice(&first_line).unwrap();
            assert_eq!(request["operation"]["type"], "list_dir");
            assert!(request["call_authority"].is_object());

            // The primary request is now emitted. The production endpoint owns
            // a single synchronous exchange, so cancellation cannot become an
            // unverified second tool operation on this connection.
            cancel_server.cancel();
            let mut second_line = Vec::new();
            assert!(
                timeout(
                    Duration::from_millis(100),
                    read.read_until(b'\n', &mut second_line),
                )
                .await
                .is_err(),
                "authorized production read must not emit a follow-up Cancel frame"
            );
            assert!(second_line.is_empty());

            run_critical_executor_test_service(first_line, write, service_identity, workspace)
                .await
                .unwrap();
        });

        let operation = ExecutorOperation::ListDir {
            path: ".".to_owned(),
            execution_id: "production-read-cancel".to_owned(),
        };
        let response = ExecutorClient::new(&socket, identity())
            .with_call_authority_signing_key(Zeroizing::new([7; 32]))
            .unwrap()
            .with_deadlines(test_deadlines())
            .execute_authorized(
                operation,
                CommittedExecutionPermit::executor_fixture(
                    "grant-production-read-cancel",
                    ToolInvocationRoute::Normal,
                    ExecutionAuthorityProvenance::AgentOwn,
                ),
                cancel,
                Arc::new(|_| {}),
            )
            .await
            .expect("post-emission cancellation must preserve the production primary terminal")
            .into_inner();
        assert_eq!(
            response,
            ExecutorResponse::Listed {
                entries: vec!["visible.txt".to_owned()],
            }
        );
        server.await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    async fn run_source_transfer(
        root: &Path,
        workspace: PathBuf,
        paths: Vec<String>,
    ) -> Result<Vec<TransferredSource>, ToolError> {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let socket = root.join(format!(
            "x{}.sock",
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let listener = UnixListener::bind(&socket).unwrap();
        let service_identity = identity();
        let server = tokio::spawn(async move {
            // Client-side validation failures never connect; give up quietly.
            let Ok(Ok((stream, _))) = timeout(Duration::from_secs(2), listener.accept()).await
            else {
                return;
            };
            let (read, write) = stream.into_split();
            let mut read = BufReader::new(read);
            let mut first_line = Vec::new();
            read.read_until(b'\n', &mut first_line).await.unwrap();
            assert_eq!(first_line.pop(), Some(b'\n'));
            let request: Value = serde_json::from_slice(&first_line).unwrap();
            assert_eq!(request["operation"]["type"], "open_source_files");
            assert!(request["call_authority"].is_object());
            run_critical_executor_test_service(first_line, write, service_identity, workspace)
                .await
                .unwrap();
        });
        let client = ExecutorClient::new(&socket, identity())
            .with_call_authority_signing_key(Zeroizing::new([7; 32]))
            .unwrap()
            .with_deadlines(test_deadlines());
        let result = CommittedExecutionPermit::executor_fixture(
            "grant-source-transfer",
            ToolInvocationRoute::Normal,
            ExecutionAuthorityProvenance::AgentOwn,
        )
        .begin_messaging_workspace_send_effect()
        .complete(|continuation| async move {
            client
                .execute_source_transfer(
                    paths,
                    "source-transfer".to_owned(),
                    continuation,
                    CancellationToken::new(),
                )
                .await
        })
        .await
        .map(|receipt| receipt.into_inner());
        server.await.unwrap();
        result
    }

    #[tokio::test]
    async fn signed_source_transfer_delivers_ordered_descriptors_with_manifests() {
        use std::io::{Read as _, Seek as _, Write as _};

        let root = temp_root("st");
        let workspace = root.join("workspace");
        std::fs::create_dir_all(workspace.join("docs")).unwrap();
        std::fs::write(workspace.join("docs/one.txt"), b"first bytes").unwrap();
        std::fs::write(workspace.join("two.bin"), vec![9u8; 70_000]).unwrap();

        let sources = run_source_transfer(
            &root,
            workspace.clone(),
            vec!["two.bin".to_owned(), "docs/one.txt".to_owned()],
        )
        .await
        .expect("source transfer");
        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0].manifest.path, "two.bin");
        assert_eq!(sources[0].manifest.filename, "two.bin");
        assert_eq!(sources[0].manifest.size_bytes, 70_000);
        assert_eq!(sources[1].manifest.filename, "one.txt");
        assert_eq!(sources[1].manifest.size_bytes, 11);
        std::fs::write(workspace.join("docs/one.txt"), b"changed after transfer").unwrap();
        for source in &sources {
            let mut file = std::fs::File::from(source.descriptor.try_clone().unwrap());
            assert_eq!(
                file.write_all(b"must fail")
                    .expect_err("sealed descriptor must reject writes")
                    .raw_os_error(),
                Some(libc::EPERM)
            );
            let mut content = Vec::new();
            file.seek(std::io::SeekFrom::Start(0)).unwrap();
            file.read_to_end(&mut content).unwrap();
            assert_eq!(content.len() as u64, source.manifest.size_bytes);
            let digest = {
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                hasher.update(&content);
                hasher
                    .finalize()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            };
            assert_eq!(
                digest, source.manifest.sha256,
                "descriptor bytes match the manifest"
            );
        }
        let one = &sources[1];
        let mut file = std::fs::File::from(one.descriptor.try_clone().unwrap());
        let mut content = Vec::new();
        file.seek(std::io::SeekFrom::Start(0)).unwrap();
        file.read_to_end(&mut content).unwrap();
        assert_eq!(content, b"first bytes");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn source_transfer_rejects_symlink_traversal_special_and_oversized_sources() {
        let root = temp_root("st-neg");
        let workspace = root.join("workspace");
        std::fs::create_dir_all(workspace.join("dir")).unwrap();
        std::fs::write(workspace.join("ok.txt"), b"ok").unwrap();
        std::fs::write(root.join("outside.txt"), b"outside").unwrap();
        std::os::unix::fs::symlink(root.join("outside.txt"), workspace.join("escape.txt")).unwrap();
        std::fs::write(workspace.join("empty.txt"), b"").unwrap();
        for (label, paths) in [
            ("symlink", vec!["escape.txt".to_owned()]),
            ("traversal", vec!["../outside.txt".to_owned()]),
            (
                "absolute",
                vec![root.join("outside.txt").to_string_lossy().into_owned()],
            ),
            ("directory", vec!["dir".to_owned()]),
            ("magic link", vec!["/proc/self/exe".to_owned()]),
            ("empty file", vec!["empty.txt".to_owned()]),
            ("duplicate", vec!["ok.txt".to_owned(), "ok.txt".to_owned()]),
            ("too many", (0..11).map(|_| "ok.txt".to_owned()).collect()),
            ("none", Vec::new()),
        ] {
            let result = run_source_transfer(&root, workspace.clone(), paths).await;
            assert!(result.is_err(), "{label} must be refused");
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn short_health_deadline_closes_the_probe_without_sending_cancel() {
        let root = temp_root("health-short-deadline");
        let socket = root.join("executor.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, _write) = stream.into_split();
            let mut read = BufReader::new(read);
            let request = read_request(&mut read).await;
            assert_eq!(request["operation"]["type"], "health");
            let mut trailing = String::new();
            let count = timeout(Duration::from_millis(250), read.read_line(&mut trailing))
                .await
                .expect("short Health deadline must close its Unix connection")
                .expect("read Health connection close");
            assert_eq!(count, 0, "deadline must close without a Cancel frame");
            assert!(trailing.is_empty());
        });

        let error = ExecutorClient::new(&socket, identity())
            .with_deadlines(test_deadlines())
            .health_with_cancellation(CancellationToken::new(), Duration::from_millis(40))
            .await
            .expect_err("stalled Health must obey its short probe deadline");
        assert!(matches!(error, ToolError::RpcIndeterminate(_)));
        server.await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn wrong_identity_and_request_id_fail_closed() {
        for mode in ["identity", "request"] {
            let root = temp_root(mode);
            let socket = root.join("executor.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let (read, mut write) = stream.into_split();
                let request = read_request(&mut BufReader::new(read)).await;
                let nonce = if mode == "identity" {
                    "wrong"
                } else {
                    "boot-nonce"
                };
                let request_id = if mode == "request" {
                    Value::String("wrong-request".to_owned())
                } else {
                    request["request_id"].clone()
                };
                write_json_line(
                    &mut write,
                    json!({
                        "type":"terminal", "personality_agent_id":PAID, "generation":7, "nonce":nonce,
                        "request_id":request_id, "result":{"Ok":{"type":"written"}}
                    }),
                )
                .await;
            });
            let error = ExecutorClient::new(&socket, identity())
                .with_deadlines(test_deadlines())
                .execute(
                    write_operation("wrong-frame", "x", "x"),
                    CancellationToken::new(),
                    Arc::new(|_| {}),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(error, ToolError::RpcIndeterminate(_)),
                "{mode}: {error:?}"
            );
            server.await.unwrap();
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test]
    async fn cross_personality_agent_artifact_routes_fail_before_service_contact() {
        let root = temp_root("xpaid");
        let socket = root.join("executor.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let client = ExecutorClient::new(&socket, identity()).with_deadlines(test_deadlines());

        for operation in [
            ExecutorOperation::ReadFile {
                path: "artifact://malformed".to_owned(),
                offset: 0,
                limit: 4,
                execution_id: "malformed-read".to_owned(),
            },
            ExecutorOperation::ReadFile {
                path: "artifact://0198f0f4-9b72-7000-8000-000000000002/tool-output/read".to_owned(),
                offset: 0,
                limit: 4,
                execution_id: "cross-read".to_owned(),
            },
            ExecutorOperation::Grep {
                path: "artifact://0198f0f4-9b72-7000-8000-000000000002/attachments/input"
                    .to_owned(),
                pattern: "needle".to_owned(),
                execution_id: "cross-grep".to_owned(),
            },
        ] {
            assert!(matches!(
                client
                    .execute(operation, CancellationToken::new(), Arc::new(|_| {}))
                    .await,
                Err(ToolError::InvalidPath(_))
            ));
        }
        assert!(
            timeout(Duration::from_millis(30), listener.accept())
                .await
                .is_err(),
            "invalid artifact route contacted the executor"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn response_validation_enforces_route_inner_variant_and_read_bounds() {
        let workspace_read = ExecutorOperation::ReadFile {
            path: "notes.txt".to_owned(),
            offset: 0,
            limit: 4,
            execution_id: "read-workspace".to_owned(),
        };
        let workspace_result = crate::tools::truncate::truncate_head(
            "abcd",
            crate::tools::truncate::TruncationOptions {
                max_lines: 2_000,
                max_bytes: 4,
            },
        );
        assert!(
            validate_response(
                &workspace_read,
                &ExecutorResponse::ReadFile {
                    result: workspace_result.clone(),
                },
            )
            .is_ok()
        );

        let mut oversized_workspace_result = workspace_result;
        oversized_workspace_result.content.push('e');
        oversized_workspace_result.output_bytes += 1;
        assert!(
            validate_response(
                &workspace_read,
                &ExecutorResponse::ReadFile {
                    result: oversized_workspace_result,
                },
            )
            .is_err()
        );

        let mut contradictory = crate::tools::truncate::truncate_head(
            "abcd",
            crate::tools::truncate::TruncationOptions {
                max_lines: DEFAULT_MAX_LINES,
                max_bytes: 4,
            },
        );
        contradictory.truncated = true;
        contradictory.truncated_by = None;
        assert!(
            validate_response(
                &workspace_read,
                &ExecutorResponse::ReadFile {
                    result: contradictory,
                },
            )
            .is_err()
        );

        let artifact_read = ExecutorOperation::ReadFile {
            path: "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/read".to_owned(),
            offset: 0,
            limit: 4,
            execution_id: "read-artifact".to_owned(),
        };
        assert!(
            validate_response(
                &artifact_read,
                &ExecutorResponse::Artifact {
                    response: ArtifactResponse::Read {
                        content: b"abcd".to_vec(),
                        eof: true,
                    },
                },
            )
            .is_ok()
        );
        assert!(
            validate_response(
                &artifact_read,
                &ExecutorResponse::Artifact {
                    response: ArtifactResponse::Read {
                        content: b"abcde".to_vec(),
                        eof: false,
                    },
                },
            )
            .is_err()
        );
        assert!(
            validate_response(
                &artifact_read,
                &ExecutorResponse::Artifact {
                    response: ArtifactResponse::Begun {
                        handle: "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/read"
                            .to_owned(),
                        offset: 0,
                    },
                },
            )
            .is_err()
        );

        let artifact_grep = ExecutorOperation::Grep {
            path: "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/read".to_owned(),
            pattern: "needle".to_owned(),
            execution_id: "grep-artifact".to_owned(),
        };
        assert!(
            validate_response(
                &artifact_grep,
                &ExecutorResponse::Artifact {
                    response: ArtifactResponse::Grep {
                        matches: Vec::new()
                    },
                },
            )
            .is_ok()
        );
        assert!(
            validate_response(
                &artifact_grep,
                &ExecutorResponse::Artifact {
                    response: ArtifactResponse::Read {
                        content: Vec::new(),
                        eof: true,
                    },
                },
            )
            .is_err()
        );

        for forged_operation in [
            ExecutorOperation::ReadFile {
                path: "artifact://0198f0f4-9b72-7000-8000-000000000002/tool-output/read".to_owned(),
                offset: 0,
                limit: 4,
                execution_id: "forged-read".to_owned(),
            },
            ExecutorOperation::Grep {
                path: "artifact://0198f0f4-9b72-7000-8000-000000000002/tool-output/grep".to_owned(),
                pattern: "needle".to_owned(),
                execution_id: "forged-grep".to_owned(),
            },
        ] {
            let forged_response = match &forged_operation {
                ExecutorOperation::ReadFile { .. } => ExecutorResponse::Artifact {
                    response: ArtifactResponse::Read {
                        content: b"safe".to_vec(),
                        eof: true,
                    },
                },
                ExecutorOperation::Grep { .. } => ExecutorResponse::Artifact {
                    response: ArtifactResponse::Grep {
                        matches: Vec::new(),
                    },
                },
                _ => unreachable!(),
            };
            assert!(
                validate_response_for_personality_agent(
                    &forged_operation,
                    &forged_response,
                    &PAID.parse().unwrap(),
                )
                .is_err(),
                "accepted a bounded cross-personality-agent artifact response"
            );
        }
    }

    #[test]
    fn response_collection_and_grep_limits_are_semantic() {
        let list = ExecutorOperation::ListDir {
            path: ".".to_owned(),
            execution_id: "list-limits".to_owned(),
        };
        let entries = (0..MAX_SCAN_ENTRIES)
            .map(|index| format!("entry-{index}"))
            .collect::<Vec<_>>();
        assert!(
            validate_response(
                &list,
                &ExecutorResponse::Listed {
                    entries: entries.clone(),
                },
            )
            .is_ok()
        );
        let mut too_many_entries = entries;
        too_many_entries.push("overflow".to_owned());
        assert!(
            validate_response(
                &list,
                &ExecutorResponse::Listed {
                    entries: too_many_entries,
                },
            )
            .is_err()
        );

        let grep = ExecutorOperation::Grep {
            path: ".".to_owned(),
            pattern: "x".to_owned(),
            execution_id: "grep-limits".to_owned(),
        };
        let mut matches = Vec::new();
        loop {
            let mut candidate = matches.clone();
            candidate.push(GrepMatch {
                path: "p".to_owned(),
                line_number: 1,
                line: String::new(),
                line_truncated: false,
            });
            if serde_json::to_vec(&ExecutorResponse::Grepped {
                matches: candidate.clone(),
            })
            .unwrap()
            .len()
                > MAX_GREP_SERIALIZED_BYTES
            {
                break;
            }
            matches = candidate;
        }
        let current = serde_json::to_vec(&ExecutorResponse::Grepped {
            matches: matches.clone(),
        })
        .unwrap()
        .len();
        matches
            .last_mut()
            .unwrap()
            .path
            .push_str(&"a".repeat(MAX_GREP_SERIALIZED_BYTES - current));
        assert_eq!(
            serde_json::to_vec(&ExecutorResponse::Grepped {
                matches: matches.clone(),
            })
            .unwrap()
            .len(),
            MAX_GREP_SERIALIZED_BYTES
        );
        assert!(
            validate_response(
                &grep,
                &ExecutorResponse::Grepped {
                    matches: matches.clone(),
                },
            )
            .is_ok()
        );
        matches.last_mut().unwrap().path.push('a');
        assert!(validate_response(&grep, &ExecutorResponse::Grepped { matches }).is_err());

        let artifact_grep = ExecutorOperation::Grep {
            path: "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/grep".to_owned(),
            pattern: "x".to_owned(),
            execution_id: "artifact-grep-lines".to_owned(),
        };
        let exact_line = "x".repeat(GREP_MAX_LINE_LENGTH);
        assert!(
            validate_response(
                &artifact_grep,
                &ExecutorResponse::Artifact {
                    response: ArtifactResponse::Grep {
                        matches: vec![crate::tools::executor::ArtifactGrepMatch {
                            line_number: 1,
                            line: exact_line.clone(),
                            line_truncated: false,
                        }],
                    },
                },
            )
            .is_ok()
        );
        assert!(
            validate_response(
                &artifact_grep,
                &ExecutorResponse::Artifact {
                    response: ArtifactResponse::Grep {
                        matches: vec![crate::tools::executor::ArtifactGrepMatch {
                            line_number: 1,
                            line: format!("{exact_line}x"),
                            line_truncated: false,
                        }],
                    },
                },
            )
            .is_err()
        );
    }

    #[test]
    fn bash_response_limits_and_terminal_metadata_are_semantic() {
        let operation = ExecutorOperation::Bash {
            command: "true".to_owned(),
            execution_id: "bash-bounds".to_owned(),
        };
        let exact = "x".repeat(crate::tools::truncate::DEFAULT_MAX_BYTES);
        let result = crate::tools::bash::BashExecutionResult {
            output: exact.clone(),
            truncation: crate::tools::truncate::truncate_tail(&exact, Default::default()),
            artifact_handle: None,
            observed_bytes: exact.len() as u64,
            exit_code: Some(0),
            cancelled: false,
            resource_limit: None,
        };
        assert!(
            validate_response(
                &operation,
                &ExecutorResponse::Bash {
                    result: result.clone(),
                },
            )
            .is_ok()
        );

        let mut too_visible = result.clone();
        too_visible.output.push('x');
        assert!(
            validate_response(
                &operation,
                &ExecutorResponse::Bash {
                    result: too_visible,
                },
            )
            .is_err()
        );

        let mut unbounded_observed = result.clone();
        unbounded_observed.observed_bytes = u64::MAX;
        assert!(
            validate_response(
                &operation,
                &ExecutorResponse::Bash {
                    result: unbounded_observed,
                },
            )
            .is_err()
        );

        let observed = usize::try_from(result.observed_bytes).expect("fixture observed bytes");
        let mut unbounded_sanitized_bytes = result.clone();
        unbounded_sanitized_bytes.truncation.truncated = true;
        unbounded_sanitized_bytes.truncation.truncated_by =
            Some(crate::tools::truncate::TruncatedBy::Bytes);
        unbounded_sanitized_bytes.truncation.total_bytes = observed * 3 + 1;
        assert!(
            validate_response(
                &operation,
                &ExecutorResponse::Bash {
                    result: unbounded_sanitized_bytes,
                },
            )
            .is_err()
        );

        let mut unbounded_sanitized_lines = result.clone();
        unbounded_sanitized_lines.truncation.truncated = true;
        unbounded_sanitized_lines.truncation.truncated_by =
            Some(crate::tools::truncate::TruncatedBy::Lines);
        unbounded_sanitized_lines.truncation.total_lines = observed + 1;
        assert!(
            validate_response(
                &operation,
                &ExecutorResponse::Bash {
                    result: unbounded_sanitized_lines,
                },
            )
            .is_err()
        );

        let output_limit = crate::tools::shell_capture::COMMAND_OUTPUT_LIMIT_BYTES;
        let mut noncanonical_output_limit = result.clone();
        noncanonical_output_limit.observed_bytes = output_limit;
        noncanonical_output_limit.exit_code = None;
        noncanonical_output_limit.resource_limit = Some(crate::tools::ResourceLimit::OutputBytes {
            observed: output_limit,
            limit: 0,
        });
        assert!(
            validate_response(
                &operation,
                &ExecutorResponse::Bash {
                    result: noncanonical_output_limit,
                },
            )
            .is_err()
        );

        let mut noncanonical_output_observed = result.clone();
        noncanonical_output_observed.observed_bytes = output_limit - 1;
        noncanonical_output_observed.exit_code = None;
        noncanonical_output_observed.resource_limit =
            Some(crate::tools::ResourceLimit::OutputBytes {
                observed: output_limit - 1,
                limit: output_limit,
            });
        assert!(
            validate_response(
                &operation,
                &ExecutorResponse::Bash {
                    result: noncanonical_output_observed,
                },
            )
            .is_err()
        );

        let mut noncanonical_wall_time = result.clone();
        noncanonical_wall_time.observed_bytes = output_limit;
        noncanonical_wall_time.exit_code = None;
        noncanonical_wall_time.resource_limit =
            Some(crate::tools::ResourceLimit::WallTime { limit_seconds: 1 });
        assert!(
            validate_response(
                &operation,
                &ExecutorResponse::Bash {
                    result: noncanonical_wall_time,
                },
            )
            .is_err()
        );

        let mut noncanonical_completion = result.clone();
        noncanonical_completion.observed_bytes = output_limit;
        assert!(
            validate_response(
                &operation,
                &ExecutorResponse::Bash {
                    result: noncanonical_completion,
                },
            )
            .is_err()
        );

        let mut malformed_handle = result.clone();
        malformed_handle.artifact_handle = Some("artifact://bad".to_owned());
        assert!(
            validate_response(
                &operation,
                &ExecutorResponse::Bash {
                    result: malformed_handle,
                },
            )
            .is_err()
        );

        for handle in [
            "artifact://0198f0f4-9b72-7000-8000-000000000002/tool-output/bash-bounds",
            "artifact://0198f0f4-9b72-7000-8000-000000000001/attachments/bash-bounds",
            "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/other-execution",
        ] {
            let mut wrong_claim = result.clone();
            wrong_claim.artifact_handle = Some(handle.to_owned());
            assert!(
                validate_response(
                    &operation,
                    &ExecutorResponse::Bash {
                        result: wrong_claim,
                    },
                )
                .is_err(),
                "accepted wrong artifact claim: {handle}"
            );
        }

        let mut contradictory = result;
        contradictory.cancelled = true;
        contradictory.resource_limit = Some(crate::tools::ResourceLimit::OutputBytes {
            observed: contradictory.observed_bytes,
            limit: contradictory.observed_bytes,
        });
        assert!(
            validate_response(
                &operation,
                &ExecutorResponse::Bash {
                    result: contradictory,
                },
            )
            .is_err()
        );
    }

    #[test]
    fn cancellation_terminal_matches_physical_bash_result() {
        let completed = ExecutorResponse::Bash {
            result: crate::tools::bash::BashExecutionResult {
                output: "done".to_owned(),
                truncation: crate::tools::truncate::truncate_tail("done", Default::default()),
                artifact_handle: None,
                observed_bytes: 4,
                exit_code: Some(0),
                cancelled: false,
                resource_limit: None,
            },
        };
        assert!(
            validate_cancel_settlement(
                true,
                true,
                Some(CancelTerminal::TooLate),
                &Ok(completed.clone()),
            )
            .is_ok()
        );
        assert!(
            validate_cancel_settlement(true, true, Some(CancelTerminal::Accepted), &Ok(completed),)
                .is_err()
        );

        let cancelled = ExecutorResponse::Bash {
            result: crate::tools::bash::BashExecutionResult {
                output: String::new(),
                truncation: crate::tools::truncate::truncate_tail("", Default::default()),
                artifact_handle: None,
                observed_bytes: 0,
                exit_code: None,
                cancelled: true,
                resource_limit: None,
            },
        };
        assert!(
            validate_cancel_settlement(
                true,
                true,
                Some(CancelTerminal::Accepted),
                &Ok(cancelled.clone()),
            )
            .is_ok()
        );
        assert!(
            validate_cancel_settlement(true, true, Some(CancelTerminal::TooLate), &Ok(cancelled),)
                .is_err()
        );
        assert!(
            validate_cancel_settlement(
                true,
                true,
                Some(CancelTerminal::TooLate),
                &Err(ToolError::RpcIndeterminate("reap".to_owned())),
            )
            .is_err()
        );

        let completed_write = Ok(ExecutorResponse::Written {});
        assert!(
            validate_cancel_settlement(
                true,
                false,
                Some(CancelTerminal::TooLate),
                &completed_write,
            )
            .is_ok()
        );
        assert!(
            validate_cancel_settlement(
                true,
                false,
                Some(CancelTerminal::Accepted),
                &completed_write,
            )
            .is_err()
        );
        assert!(
            validate_cancel_settlement(
                true,
                false,
                Some(CancelTerminal::TooLate),
                &Err(ToolError::Rpc("ambiguous read failure".to_owned())),
            )
            .is_err()
        );
    }

    #[test]
    fn mutating_rpc_errors_preserve_unproven_side_effect_ambiguity() {
        let mutation = write_operation("mapping", "note.txt", "content");
        for code in ["io", "rpc", "cancelled", "future_error"] {
            assert!(matches!(
                map_rpc_error(
                    &mutation,
                    RpcError {
                        code: code.to_owned(),
                        resource_limit: None,
                    },
                ),
                ToolError::RpcIndeterminate(_)
            ));
        }
        assert!(matches!(
            map_rpc_error(
                &mutation,
                RpcError {
                    code: "invalid_path".to_owned(),
                    resource_limit: None,
                },
            ),
            ToolError::InvalidPath(_)
        ));
        assert!(matches!(
            map_rpc_error(
                &mutation,
                RpcError {
                    code: "protocol".to_owned(),
                    resource_limit: None,
                },
            ),
            ToolError::Protocol(_)
        ));
    }

    #[test]
    fn executor_control_errors_have_stable_external_classification() {
        let mutation = write_operation("control-mapping", "note.txt", "content");
        let rollover = map_rpc_error(
            &mutation,
            RpcError {
                code: RPC_BOOT_UNIQUENESS_EXHAUSTED_CODE.to_owned(),
                resource_limit: None,
            },
        );
        assert_eq!(
            classify_executor_error(&rollover),
            Some(ExecutorErrorClassification::GenerationRolloverRequired)
        );
        assert!(matches!(rollover, ToolError::Rpc(_)));

        let replay_unavailable = map_rpc_error(
            &mutation,
            RpcError {
                code: RPC_REPLAY_OUTCOME_UNAVAILABLE_CODE.to_owned(),
                resource_limit: None,
            },
        );
        assert_eq!(
            classify_executor_error(&replay_unavailable),
            Some(ExecutorErrorClassification::ReplayOutcomeUnavailable)
        );
        assert!(matches!(replay_unavailable, ToolError::Rpc(_)));

        for (code, expected, rpc) in [
            (
                RPC_CALL_AUTHORITY_REPLAY_CODE,
                ExecutorErrorClassification::CallAuthorityReplay,
                false,
            ),
            (
                RPC_CALL_AUTHORITY_CAPACITY_EXHAUSTED_CODE,
                ExecutorErrorClassification::CallAuthorityCapacityExhausted,
                true,
            ),
            (
                RPC_CALL_AUTHORITY_STALE_CODE,
                ExecutorErrorClassification::CallAuthorityStale,
                false,
            ),
        ] {
            let error = map_rpc_error(
                &ExecutorOperation::ListDir {
                    path: ".".to_owned(),
                    execution_id: format!("classification-{code}"),
                },
                RpcError {
                    code: code.to_owned(),
                    resource_limit: None,
                },
            );
            assert_eq!(classify_executor_error(&error), Some(expected));
            assert_eq!(matches!(error, ToolError::Rpc(_)), rpc);
        }
        assert_eq!(
            classify_executor_error(&ToolError::Rpc("different".to_owned())),
            None
        );
    }

    #[test]
    fn executor_response_unit_variants_reject_unknown_fields() {
        let parsed = serde_json::from_value::<ExecutorResponse>(
            serde_json::json!({"type": "written", "extra": 1}),
        );
        assert!(parsed.is_err(), "unit response accepted an unknown field");
    }

    #[tokio::test]
    async fn oversize_eof_timeout_and_trailing_frames_are_indeterminate() {
        for mode in ["oversize", "eof", "timeout", "trailing"] {
            let root = temp_root(mode);
            let socket = root.join("executor.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let (read, mut write) = stream.into_split();
                let request = read_request(&mut BufReader::new(read)).await;
                match mode {
                    "oversize" => write
                        .write_all(&vec![b'x'; MAX_RPC_LINE_BYTES])
                        .await
                        .unwrap(),
                    "eof" => {}
                    "timeout" => tokio::time::sleep(Duration::from_secs(1)).await,
                    "trailing" => {
                        let terminal = json!({
                            "type":"terminal", "personality_agent_id":PAID, "generation":7, "nonce":"boot-nonce",
                            "request_id":request["request_id"],
                            "result":{"Ok":{"type":"written"}}
                        });
                        write_json_line(&mut write, terminal.clone()).await;
                        write_json_line(&mut write, terminal).await;
                    }
                    _ => unreachable!(),
                }
            });
            let mut deadlines = test_deadlines();
            deadlines.frame = Duration::from_millis(80);
            deadlines.overall = Duration::from_millis(250);
            let error = ExecutorClient::new(&socket, identity())
                .with_deadlines(deadlines)
                .execute(
                    write_operation("bad-reply", "x", "x"),
                    CancellationToken::new(),
                    Arc::new(|_| {}),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(error, ToolError::RpcIndeterminate(_)),
                "{mode}: {error:?}"
            );
            server.await.unwrap();
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test]
    async fn cancellation_without_ack_never_detaches_silently() {
        let root = temp_root("cancel-eof");
        let socket = root.join("executor.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, _write) = stream.into_split();
            let mut bytes = Vec::new();
            BufReader::new(read).read_to_end(&mut bytes).await.unwrap();
            let mut lines = bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty());
            let first: Value = serde_json::from_slice(lines.next().unwrap()).unwrap();
            let second: Value = serde_json::from_slice(lines.next().unwrap()).unwrap();
            assert!(lines.next().is_none());
            assert_eq!(first["operation"]["execution_id"], "cancel-no-ack");
            assert_eq!(second["operation"]["type"], "cancel");
            assert_eq!(second["operation"]["execution_id"], "cancel-no-ack");
        });
        let cancel = CancellationToken::new();
        cancel.cancel();
        // A token cancelled before emission must produce no service contact.
        let pre = ExecutorClient::new(&socket, identity())
            .with_deadlines(test_deadlines())
            .execute(write_operation("pre", "x", "x"), cancel, Arc::new(|_| {}))
            .await;
        assert!(matches!(pre, Err(ToolError::Cancelled)));

        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            trigger.cancel();
        });
        let error = ExecutorClient::new(&socket, identity())
            .with_deadlines(test_deadlines())
            .execute(
                ExecutorOperation::Bash {
                    command: "sleep 30".to_owned(),
                    execution_id: "cancel-no-ack".to_owned(),
                },
                cancel,
                Arc::new(|_| {}),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ToolError::RpcIndeterminate(_)));
        server.await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn real_service_cancel_too_late_preserves_read_and_write_results() {
        for mode in ["read", "write"] {
            let root = temp_root(&format!("non-bash-cancel-{mode}"));
            let workspace = root.join("workspace");
            std::fs::create_dir_all(&workspace).unwrap();
            std::fs::write(workspace.join("source.txt"), "source").unwrap();
            let cancel = CancellationToken::new();
            let (socket, service) = spawn_real_service_cancel_race(&root, cancel.clone());
            let operation = if mode == "read" {
                ExecutorOperation::ReadFile {
                    path: "source.txt".to_owned(),
                    offset: 0,
                    limit: 64,
                    execution_id: "sync-read".to_owned(),
                }
            } else {
                write_operation("sync-write", "written.txt", "written")
            };
            let response = ExecutorClient::new(&socket, identity())
                .with_deadlines(test_deadlines())
                .execute(operation, cancel, Arc::new(|_| {}))
                .await
                .unwrap();
            if mode == "read" {
                assert!(matches!(
                    response,
                    ExecutorResponse::ReadFile { result } if result.content == "source"
                ));
            } else {
                assert_eq!(response, ExecutorResponse::Written {});
                assert_eq!(
                    std::fs::read_to_string(workspace.join("written.txt")).unwrap(),
                    "written"
                );
            }
            service.await.unwrap();
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test]
    async fn cancel_too_late_returns_the_authoritative_completed_bash_result() {
        let root = temp_root("cancel-false");
        let socket = root.join("executor.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut read = BufReader::new(read);
            let operation = read_request(&mut read).await;
            let cancel = read_request(&mut read).await;
            let truncation = crate::tools::truncate::truncate_tail("done", Default::default());
            write_json_line(
                &mut write,
                json!({
                    "type":"terminal", "personality_agent_id":PAID, "generation":7, "nonce":"boot-nonce",
                    "request_id":cancel["request_id"],
                    "result":{"Ok":{"type":"cancel_too_late"}}
                }),
            )
            .await;
            write_json_line(
                &mut write,
                json!({
                    "type":"terminal", "personality_agent_id":PAID, "generation":7, "nonce":"boot-nonce",
                    "request_id":operation["request_id"],
                    "result":{"Ok":{"type":"bash","result":{
                        "output":"done", "truncation":truncation,
                        "artifact_handle":null, "observed_bytes":4,
                        "exit_code":0, "cancelled":false, "resource_limit":null
                    }}}
                }),
            )
            .await;
        });
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            trigger.cancel();
        });
        let response = ExecutorClient::new(&socket, identity())
            .with_deadlines(test_deadlines())
            .execute(
                ExecutorOperation::Bash {
                    command: "sleep 30".to_owned(),
                    execution_id: "cancel-false".to_owned(),
                },
                cancel,
                Arc::new(|_| {}),
            )
            .await
            .unwrap();
        assert!(matches!(
            response,
            ExecutorResponse::Bash { result }
                if !result.cancelled && result.exit_code == Some(0) && result.output == "done"
        ));
        server.await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
