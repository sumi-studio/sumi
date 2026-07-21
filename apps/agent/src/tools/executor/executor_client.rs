//! Bounded, generation-fenced client for the tool executor service.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use serde_json::Value;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::UnixStream,
    time::{Instant, timeout, timeout_at},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ExecutorOperation, ExecutorResponse, MAX_RPC_LINE_BYTES, RpcError, RpcFrame, RpcIdentity,
    RpcOperationValidation, RpcRequest, decode_rpc_frame,
};
use crate::tools::ToolError;

const MAX_EXECUTOR_UPDATES: usize = 65_536;

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

/// A single-operation client. Each call gets an isolated Unix service session;
/// a cancellation request, when needed, is sent on that same session.
///
/// Updates are delivered inline and in wire order. As with [`crate::tools::ToolCtx`],
/// the callback must be prompt and nonblocking. The client bounds frame size,
/// update count, frame waits, and the complete exchange. The frozen service can
/// actively cancel Bash; cancellation racing a synchronous non-Bash operation
/// is settled without detaching but remains indeterminate because side effects
/// may already have occurred.
pub struct ExecutorClient {
    socket: PathBuf,
    identity: RpcIdentity,
    deadlines: Deadlines,
}

impl ExecutorClient {
    pub fn new(socket: impl Into<PathBuf>, identity: RpcIdentity) -> Self {
        Self {
            socket: socket.into(),
            identity,
            deadlines: Deadlines::default(),
        }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    pub async fn execute(
        &self,
        operation: ExecutorOperation,
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> Result<ExecutorResponse, ToolError> {
        operation.validate()?;
        if matches!(operation, ExecutorOperation::Cancel { .. }) {
            return Err(ToolError::Protocol(
                "ExecutorClient owns cancel request construction".to_owned(),
            ));
        }
        if cancel.is_cancelled() {
            return Err(ToolError::Cancelled);
        }

        let request_emitted = Arc::new(AtomicBool::new(false));
        let execution = self.execute_inner(operation, cancel, on_update, request_emitted.clone());
        match timeout(self.deadlines.overall, execution).await {
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
        cancel: CancellationToken,
        on_update: Arc<dyn Fn(Value) + Send + Sync>,
        request_emitted: Arc<AtomicBool>,
    ) -> Result<ExecutorResponse, ToolError> {
        let cancellable_bash = matches!(operation, ExecutorOperation::Bash { .. });
        let execution_id = operation_execution_id(&operation).to_owned();
        let request_id = format!("executor-{}", Uuid::now_v7());
        let encoded = encode_request(&self.identity, &request_id, operation.clone())?;

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
        let mut cancel_terminal_seen = false;
        let mut original_terminal = None;
        let mut update_count = 0usize;
        let mut write_closed = false;
        let mut cancel_deadline = None;

        loop {
            if original_terminal.is_some()
                && (cancel_request_id.is_none() || cancel_terminal_seen)
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
                _ = cancel.cancelled(), if cancel_request_id.is_none() => {
                    let id = format!("executor-cancel-{}", Uuid::now_v7());
                    let cancel_bytes = encode_request(
                        &self.identity,
                        &id,
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
                            let response = result.map_err(map_rpc_error);
                            if let Ok(response) = &response {
                                validate_response(&operation, response).map_err(as_indeterminate)?;
                            }
                            original_terminal = Some(response);
                        }
                        RpcFrame::Terminal { request_id: frame_id, result, .. }
                            if cancel_request_id.as_deref() == Some(frame_id.as_str()) =>
                        {
                            if cancel_terminal_seen {
                                return Err(indeterminate("executor emitted duplicate cancel terminal"));
                            }
                            match result {
                                Ok(ExecutorResponse::CancelAccepted) => cancel_terminal_seen = true,
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
        if cancel_request_id.is_some() && !cancellable_bash {
            return Err(indeterminate(
                "non-bash executor cancellation cannot prove synchronous side effects stopped",
            ));
        }
        result
    }

    #[cfg(test)]
    fn with_deadlines(mut self, deadlines: Deadlines) -> Self {
        self.deadlines = deadlines;
        self
    }
}

fn encode_request(
    identity: &RpcIdentity,
    request_id: &str,
    operation: ExecutorOperation,
) -> Result<Vec<u8>, ToolError> {
    let request = RpcRequest {
        generation: identity.generation,
        nonce: identity.nonce.clone(),
        request_id: request_id.to_owned(),
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
        ExecutorOperation::ReadFile { execution_id, .. }
        | ExecutorOperation::WriteFile { execution_id, .. }
        | ExecutorOperation::EditFile { execution_id, .. }
        | ExecutorOperation::RemoveFile { execution_id, .. }
        | ExecutorOperation::ListDir { execution_id, .. }
        | ExecutorOperation::Glob { execution_id, .. }
        | ExecutorOperation::Grep { execution_id, .. }
        | ExecutorOperation::Bash { execution_id, .. }
        | ExecutorOperation::Cancel { execution_id } => execution_id,
    }
}

fn validate_response(
    operation: &ExecutorOperation,
    response: &ExecutorResponse,
) -> Result<(), ToolError> {
    let routed_response_matches = match operation {
        ExecutorOperation::ReadFile { path, .. } => {
            matches!(response, ExecutorResponse::Artifact { .. }) == path.starts_with("artifact://")
                && matches!(
                    response,
                    ExecutorResponse::Artifact { .. } | ExecutorResponse::ReadFile { .. }
                )
        }
        ExecutorOperation::Grep { path, .. } => {
            matches!(response, ExecutorResponse::Artifact { .. }) == path.starts_with("artifact://")
                && matches!(
                    response,
                    ExecutorResponse::Artifact { .. } | ExecutorResponse::Grepped { .. }
                )
        }
        _ => true,
    };
    let matches = matches!(
        (operation, response),
        (
            ExecutorOperation::ReadFile { .. },
            ExecutorResponse::ReadFile { .. }
        ) | (
            ExecutorOperation::ReadFile { .. },
            ExecutorResponse::Artifact { .. }
        ) | (
            ExecutorOperation::WriteFile { .. },
            ExecutorResponse::Written
        ) | (ExecutorOperation::EditFile { .. }, ExecutorResponse::Edited)
            | (
                ExecutorOperation::RemoveFile { .. },
                ExecutorResponse::Removed
            )
            | (
                ExecutorOperation::ListDir { .. },
                ExecutorResponse::Listed { .. }
            )
            | (
                ExecutorOperation::Glob { .. },
                ExecutorResponse::Globbed { .. }
            )
            | (
                ExecutorOperation::Grep { .. },
                ExecutorResponse::Grepped { .. }
            )
            | (
                ExecutorOperation::Grep { .. },
                ExecutorResponse::Artifact { .. }
            )
            | (
                ExecutorOperation::Bash { .. },
                ExecutorResponse::Bash { .. }
            )
    );
    if matches && routed_response_matches {
        Ok(())
    } else {
        Err(ToolError::Protocol(
            "executor returned a response for a different operation".to_owned(),
        ))
    }
}

fn map_rpc_error(error: RpcError) -> ToolError {
    match (error.code.as_str(), error.resource_limit) {
        ("resource_limit", Some(limit)) => ToolError::ResourceLimit(limit),
        ("cancelled", None) => ToolError::Cancelled,
        ("invalid_arguments", None) => ToolError::InvalidArguments,
        ("invalid_path", None) => ToolError::InvalidPath("executor path rejected".to_owned()),
        ("io", None) => ToolError::Rpc("executor I/O operation failed".to_owned()),
        ("rpc_indeterminate", None) => {
            ToolError::RpcIndeterminate("executor reported an indeterminate outcome".to_owned())
        }
        ("protocol", None) => ToolError::Protocol("executor rejected request".to_owned()),
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
    use crate::tools::{
        executor::{ArtifactBrokerClient, service::run_executor_service},
        fs::WorkspaceFs,
    };
    use serde_json::{Value, json};
    use std::sync::Mutex;
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::UnixListener,
        task::JoinHandle,
    };

    fn identity() -> RpcIdentity {
        RpcIdentity {
            generation: 7,
            nonce: "boot-nonce".to_owned(),
        }
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
                    let broker =
                        ArtifactBrokerClient::new(broker_socket, identity(), "conversation-1");
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
        assert_eq!(response, ExecutorResponse::Written);
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
        assert_eq!(streamed, "firstsecond");
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
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            trigger.cancel();
        });
        let response = client
            .execute(
                ExecutorOperation::Bash {
                    command: "printf started; sleep 30".to_owned(),
                    execution_id: "bash-cancel".to_owned(),
                },
                cancel,
                Arc::new(|_| {}),
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
        assert_eq!(first.unwrap(), ExecutorResponse::Written);
        assert_eq!(second.unwrap(), ExecutorResponse::Written);
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
                        "type":"terminal", "generation":7, "nonce":nonce,
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
                            "type":"terminal", "generation":7, "nonce":"boot-nonce",
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
    async fn non_bash_cancellation_is_indeterminate_even_after_clean_settlement() {
        let root = temp_root("non-bash-cancel");
        let socket = root.join("executor.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut read = BufReader::new(read);
            let operation = read_request(&mut read).await;
            let cancel = read_request(&mut read).await;
            write_json_line(
                &mut write,
                json!({
                    "type":"terminal", "generation":7, "nonce":"boot-nonce",
                    "request_id":operation["request_id"],
                    "result":{"Ok":{"type":"written"}}
                }),
            )
            .await;
            write_json_line(
                &mut write,
                json!({
                    "type":"terminal", "generation":7, "nonce":"boot-nonce",
                    "request_id":cancel["request_id"],
                    "result":{"Ok":{"type":"cancel_accepted"}}
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
        let error = ExecutorClient::new(&socket, identity())
            .with_deadlines(test_deadlines())
            .execute(
                write_operation("sync-side-effect", "x", "x"),
                cancel,
                Arc::new(|_| {}),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ToolError::RpcIndeterminate(message)
                if message.contains("non-bash")));
        server.await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
