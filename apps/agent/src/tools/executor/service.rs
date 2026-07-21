//! Same-binary executor and artifact-broker service modes.

use std::{
    env,
    future::{Future, poll_fn},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use regex::Regex;
use serde::Serialize;
use serde_json::Value;
use tokio::{
    io::unix::AsyncFd,
    io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf},
    net::{UnixListener, UnixStream},
    sync::{Semaphore, mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use super::{
    ArtifactBroker, ArtifactBrokerClient, ArtifactOperation, ArtifactResponse, ExecutorOperation,
    ExecutorResponse, InputRoute, MAX_RPC_LINE_BYTES, RpcError, RpcFrame, RpcIdentity,
    RpcLifecycleTracker, RpcRequest, decode_rpc_line, encode_rpc_frame, resolve_input,
};
use crate::tools::{
    ToolError,
    bash::{BashExecutionResult, LowTrustLocalBash},
    fs::WorkspaceFs,
};

const UPDATE_CHANNEL_CAPACITY: usize = 32;
const BROKER_CONNECTION_CAPACITY: usize = 32;
const BROKER_BLOCKING_WORK_CAPACITY: usize = 8;
const BROKER_EXCHANGE_DEADLINE: Duration = Duration::from_secs(2);
const EXECUTOR_UPDATE_WRITE_DEADLINE: Duration = Duration::from_millis(5);
const EXECUTOR_TERMINAL_WRITE_DEADLINE: Duration = Duration::from_secs(2);
// Service stdout is a nonblocking pipe in production. Keeping each volatile
// update within PIPE_BUF makes a timed-out write all-or-nothing, so dropping
// progress can never leave a partial JSON frame ahead of the terminal.
const MAX_ATOMIC_UPDATE_FRAME_BYTES: usize = 4_096;
const EXECUTOR_REAP_DEADLINE: Duration = Duration::from_secs(2);
type BashUpdateCallback = Arc<dyn Fn(Value) + Send + Sync>;

enum ActiveControl {
    Cancel(String),
    Fatal {
        error: ToolError,
        response: Option<(String, RpcError)>,
    },
}

enum BashExit {
    Completed(Result<BashExecutionResult, ToolError>),
    Cancelled {
        cancel_request_id: String,
        completed: Option<Result<BashExecutionResult, ToolError>>,
    },
    Fatal {
        error: ToolError,
        response: Option<(String, RpcError)>,
        completed: Option<Result<BashExecutionResult, ToolError>>,
    },
}

struct WriterMessage {
    bytes: Vec<u8>,
    acknowledgement: Option<oneshot::Sender<Result<(), String>>>,
}

struct ExecutorWriter {
    updates: mpsc::Sender<WriterMessage>,
    terminals: mpsc::Sender<WriterMessage>,
}

struct NonblockingStdout {
    fd: AsyncFd<OwnedFd>,
}

struct NonblockingStdin {
    fd: AsyncFd<OwnedFd>,
}

impl AsyncRead for NonblockingStdin {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            let mut ready = match self.fd.poll_read_ready(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(ready)) => ready,
            };
            match ready.try_io(|fd| {
                let destination = buffer.initialize_unfilled();
                let read = unsafe {
                    libc::read(
                        fd.get_ref().as_raw_fd(),
                        destination.as_mut_ptr().cast(),
                        destination.len(),
                    )
                };
                if read == -1 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(read as usize)
                }
            }) {
                Ok(Ok(read)) => {
                    buffer.advance(read);
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(error)) if is_interrupted_read_error(&error) => continue,
                Ok(Err(error)) => return Poll::Ready(Err(error)),
                Err(_) => continue,
            }
        }
    }
}

fn is_interrupted_read_error(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::Interrupted
}

impl AsyncWrite for NonblockingStdout {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        loop {
            let mut ready = match self.fd.poll_write_ready(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(ready)) => ready,
            };
            match ready.try_io(|fd| {
                let written = unsafe {
                    libc::write(
                        fd.get_ref().as_raw_fd(),
                        buffer.as_ptr().cast(),
                        buffer.len(),
                    )
                };
                if written == -1 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(written as usize)
                }
            }) {
                Ok(result) => return Poll::Ready(result),
                Err(_) => continue,
            }
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _context: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _context: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl ExecutorWriter {
    fn start<W>(mut write: W) -> (Self, JoinHandle<()>)
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (updates, mut update_receiver) =
            mpsc::channel::<WriterMessage>(UPDATE_CHANNEL_CAPACITY);
        // Progress is volatile. A dedicated terminal slot ensures queued
        // progress can never consume the capacity needed by authoritative
        // completion.
        let (terminals, mut terminal_receiver) = mpsc::channel::<WriterMessage>(1);
        let task = tokio::spawn(async move {
            let mut terminal_started = false;
            loop {
                let message = if terminal_started {
                    terminal_receiver.recv().await
                } else {
                    tokio::select! {
                        biased;
                        message = terminal_receiver.recv() => message,
                        message = update_receiver.recv() => message,
                    }
                };
                let Some(message) = message else {
                    return;
                };
                let terminal = message.acknowledgement.is_some();
                terminal_started |= terminal;
                let deadline = if terminal {
                    EXECUTOR_TERMINAL_WRITE_DEADLINE
                } else {
                    EXECUTOR_UPDATE_WRITE_DEADLINE
                };
                let result = match timeout(deadline, async {
                    write.write_all(&message.bytes).await?;
                    write.flush().await
                })
                .await
                {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(error.to_string()),
                    Err(_) => Err("executor output write deadline elapsed".to_owned()),
                };
                if let Some(acknowledgement) = message.acknowledgement {
                    let _ = acknowledgement.send(result.clone());
                }
                if result.is_err() {
                    if terminal {
                        return;
                    }
                    tracing::warn!("dropping volatile executor progress update: write unavailable");
                }
            }
        });
        (Self { updates, terminals }, task)
    }

    fn try_update<T: Serialize>(&self, frame: &RpcFrame<T>) -> Result<(), ToolError> {
        let bytes = match encode_rpc_frame(frame) {
            Ok(bytes) => bytes,
            Err(error) => {
                tracing::warn!(error = %error, "dropping volatile executor progress update: encode failed");
                return Ok(());
            }
        };
        if bytes.len() > MAX_ATOMIC_UPDATE_FRAME_BYTES {
            tracing::warn!(
                "dropping volatile executor progress update: frame exceeds atomic pipe write"
            );
            return Ok(());
        }
        match self.updates.try_send(WriterMessage {
            bytes,
            acknowledgement: None,
        }) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!("dropping volatile executor progress update: writer queue full");
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!("dropping volatile executor progress update: writer unavailable");
                Ok(())
            }
        }
    }

    async fn terminal(
        &self,
        identity: &RpcIdentity,
        request_id: String,
        result: Result<ExecutorResponse, RpcError>,
    ) -> Result<(), ToolError> {
        let frame = RpcFrame::Terminal {
            generation: identity.generation,
            nonce: identity.nonce.clone(),
            request_id: request_id.clone(),
            result,
        };
        let bytes = match encode_rpc_frame(&frame) {
            Ok(encoded) => encoded,
            Err(ToolError::Protocol(_)) => {
                encode_rpc_frame(&RpcFrame::<ExecutorResponse>::Terminal {
                    generation: identity.generation,
                    nonce: identity.nonce.clone(),
                    request_id,
                    result: Err(bounded_error("response_too_large")),
                })?
            }
            Err(error) => return Err(error),
        };
        let (acknowledgement, received) = oneshot::channel();
        timeout(
            EXECUTOR_TERMINAL_WRITE_DEADLINE,
            self.terminals.send(WriterMessage {
                bytes,
                acknowledgement: Some(acknowledgement),
            }),
        )
        .await
        .map_err(|_| io_error("executor output queue deadline elapsed"))?
        .map_err(|_| io_error("executor output writer unavailable"))?;
        timeout(EXECUTOR_TERMINAL_WRITE_DEADLINE, received)
            .await
            .map_err(|_| io_error("executor terminal write deadline elapsed"))?
            .map_err(|_| io_error("executor output writer stopped"))?
            .map_err(|message| io_error(&message))
    }
}

#[derive(Clone)]
struct BlockingWorkRegistry {
    permits: Arc<Semaphore>,
}

impl BlockingWorkRegistry {
    fn new(capacity: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(capacity)),
        }
    }

    async fn reserve(&self) -> Result<tokio::sync::OwnedSemaphorePermit, ToolError> {
        self.permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| io_error("artifact blocking-work registry closed"))
    }

    async fn execute_reserved(
        broker: Arc<ArtifactBroker>,
        operation: ArtifactOperation,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<ArtifactResponse, ToolError> {
        tokio::task::spawn_blocking(move || broker.execute(operation))
            .await
            .map_err(|_| io_error("artifact blocking worker stopped"))?
    }
}

pub async fn run_tool_executor_mode() -> Result<()> {
    let identity = identity_from_env()?;
    let workspace = required_path("SUMI_WORKSPACE")?;
    let conversation_id = required_text("SUMI_CONVERSATION_ID")?;
    let broker_socket = required_path("SUMI_ARTIFACT_BROKER_SOCKET")?;
    let fs = WorkspaceFs::open(&workspace).context("failed to open executor workspace")?;
    let broker = ArtifactBrokerClient::new(broker_socket, identity.clone(), conversation_id);
    let stdin = nonblocking_stdin().context("failed to take ownership of executor stdin")?;
    let stdout = nonblocking_stdout().context("failed to take ownership of executor stdout")?;
    run_executor_service(stdin, stdout, identity, workspace, fs, broker).await
}

pub async fn run_artifact_broker_mode() -> Result<()> {
    let identity = identity_from_env()?;
    let root = required_path("SUMI_ARTIFACT_ROOT")?;
    let socket = required_path("SUMI_ARTIFACT_BROKER_SOCKET")?;
    let broker = Arc::new(ArtifactBroker::open(&root).context("failed to open artifact root")?);
    let listener = UnixListener::bind(&socket)
        .with_context(|| format!("failed to bind artifact broker socket {}", socket.display()))?;
    let lifecycle = Arc::new(Mutex::new(RpcLifecycleTracker::default()));
    let permits = Arc::new(Semaphore::new(BROKER_CONNECTION_CAPACITY));
    let blocking_work = BlockingWorkRegistry::new(BROKER_BLOCKING_WORK_CAPACITY);
    let identity = Arc::new(identity);
    loop {
        let (stream, _) = listener.accept().await?;
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            tracing::warn!("artifact broker connection capacity reached");
            drop(stream);
            continue;
        };
        let identity = identity.clone();
        let broker = broker.clone();
        let lifecycle = lifecycle.clone();
        let blocking_work = blocking_work.clone();
        tokio::spawn(async move {
            let result = timeout(
                BROKER_EXCHANGE_DEADLINE,
                serve_broker_connection(stream, identity, broker, lifecycle, blocking_work),
            )
            .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => tracing::warn!(%error, "artifact broker rejected connection"),
                Err(_) => tracing::warn!("artifact broker connection deadline elapsed"),
            }
            drop(permit);
        });
    }
}

async fn serve_broker_connection(
    stream: UnixStream,
    identity: Arc<RpcIdentity>,
    broker: Arc<ArtifactBroker>,
    lifecycle: Arc<Mutex<RpcLifecycleTracker>>,
    blocking_work: BlockingWorkRegistry,
) -> Result<(), ToolError> {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let Some(line) = read_frame(&mut read).await? else {
        return Err(ToolError::Protocol(
            "artifact broker connection closed without a request".to_owned(),
        ));
    };
    let request = match decode_artifact_request(&line, &identity)? {
        Ok(request) => request,
        Err((request_id, error)) => {
            {
                let mut lifecycle = lifecycle.lock().map_err(|_| {
                    ToolError::Protocol("artifact lifecycle lock poisoned".to_owned())
                })?;
                lifecycle.begin_request(&request_id)?;
                lifecycle.accept_terminal(&request_id)?;
            }
            write_frame(
                &mut write,
                &RpcFrame::<ArtifactResponse>::Terminal {
                    generation: identity.generation,
                    nonce: identity.nonce.clone(),
                    request_id,
                    result: Err(error),
                },
            )
            .await?;
            write.shutdown().await?;
            return Ok(());
        }
    };
    let blocking_permit = blocking_work.reserve().await?;
    {
        let mut lifecycle = lifecycle
            .lock()
            .map_err(|_| ToolError::Protocol("artifact lifecycle lock poisoned".to_owned()))?;
        lifecycle.begin_request(&request.request_id)?;
    }
    let request_id = request.request_id;
    let (result_tx, result_rx) = oneshot::channel();
    let job_request_id = request_id.clone();
    tokio::spawn(async move {
        let result =
            BlockingWorkRegistry::execute_reserved(broker, request.operation, blocking_permit)
                .await
                .map_err(rpc_error);
        let lifecycle_result = lifecycle
            .lock()
            .map_err(|_| ToolError::Protocol("artifact lifecycle lock poisoned".to_owned()))
            .and_then(|mut lifecycle| lifecycle.accept_terminal(&job_request_id));
        let _ = result_tx.send(lifecycle_result.map(|()| result));
    });
    // Dropping this receiver on the connection deadline does not cancel the
    // accepted blocking job. It retains its operation receipt and releases the
    // registry permit only after mutation and lifecycle finalization finish.
    let result = result_rx
        .await
        .map_err(|_| io_error("artifact blocking job stopped"))??;
    write_frame(
        &mut write,
        &RpcFrame::Terminal {
            generation: identity.generation,
            nonce: identity.nonce.clone(),
            request_id,
            result,
        },
    )
    .await?;
    write.shutdown().await?;
    Ok(())
}

pub(super) async fn run_executor_service<R, W>(
    read: R,
    write: W,
    identity: RpcIdentity,
    workspace: PathBuf,
    fs: WorkspaceFs,
    broker: ArtifactBrokerClient,
) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (writer, writer_task) = ExecutorWriter::start(write);
    let result = run_executor_loop(read, &writer, identity, workspace, fs, broker).await;
    writer_task.abort();
    let _ = timeout(EXECUTOR_TERMINAL_WRITE_DEADLINE, writer_task).await;
    result
}

async fn run_executor_loop<R>(
    read: R,
    writer: &ExecutorWriter,
    identity: RpcIdentity,
    workspace: PathBuf,
    fs: WorkspaceFs,
    broker: ArtifactBrokerClient,
) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut read = BufReader::new(read);
    let mut lifecycle = RpcLifecycleTracker::default();
    loop {
        let Some(line) = read_frame(&mut read).await? else {
            return Ok(());
        };
        let request = match decode_executor_request(&line, &identity)? {
            Ok(request) => request,
            Err((request_id, error)) => {
                lifecycle.begin_request(&request_id)?;
                lifecycle.accept_terminal(&request_id)?;
                writer.terminal(&identity, request_id, Err(error)).await?;
                continue;
            }
        };
        match request.operation {
            ExecutorOperation::Bash {
                command,
                execution_id,
            } => {
                lifecycle.begin_execution(&request.request_id, &execution_id)?;
                run_bash_request(
                    &mut read,
                    writer,
                    &identity,
                    &workspace,
                    &broker,
                    &mut lifecycle,
                    request.request_id,
                    execution_id,
                    command,
                )
                .await?;
            }
            ExecutorOperation::Cancel { execution_id } => {
                lifecycle.begin_request(&request.request_id)?;
                lifecycle.accept_terminal(&request.request_id)?;
                let result = if lifecycle.execution_is_completed(&execution_id) {
                    Ok(ExecutorResponse::CancelTooLate {})
                } else {
                    Err(RpcError {
                        code: "protocol".to_owned(),
                        resource_limit: None,
                    })
                };
                writer
                    .terminal(&identity, request.request_id, result)
                    .await?;
            }
            operation => {
                let execution_id = operation_execution_id(&operation).to_owned();
                lifecycle.begin_execution(&request.request_id, &execution_id)?;
                let result = execute_non_bash(&fs, &broker, operation).await;
                lifecycle.accept_terminal(&request.request_id)?;
                writer
                    .terminal(&identity, request.request_id, result.map_err(rpc_error))
                    .await?;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_bash_request<R>(
    read: &mut R,
    writer: &ExecutorWriter,
    identity: &RpcIdentity,
    workspace: &Path,
    broker: &ArtifactBrokerClient,
    lifecycle: &mut RpcLifecycleTracker,
    request_id: String,
    execution_id: String,
    command: String,
) -> Result<()>
where
    R: AsyncBufRead + Unpin,
{
    let cancel = CancellationToken::new();
    let (on_update, mut updates_rx) = bounded_bash_updates();
    let bash = LowTrustLocalBash::new(workspace.to_path_buf(), broker);
    let execution = bash.execute(&command, &execution_id, cancel.clone(), on_update);
    tokio::pin!(execution);
    // Keep one persistent read future alive across select iterations. Recreating
    // `read_frame` would make a partially consumed Cancel frame disappear when
    // an update wins the select race.
    let (control_tx, mut control_rx) = mpsc::channel(1);
    let control_reader = async {
        loop {
            let next = read_frame(read).await;
            let terminal = !matches!(next, Ok(Some(_)));
            if control_tx.send(next).await.is_err() || terminal {
                break;
            }
        }
    };
    tokio::pin!(control_reader);
    let mut control_reader_done = false;
    let exit = loop {
        tokio::select! {
            biased;
            next = control_rx.recv(), if !control_reader_done || !control_rx.is_empty() => {
                match classify_active_control(next, identity, &execution_id, lifecycle) {
                    ActiveControl::Cancel(cancel_request_id) => break BashExit::Cancelled {
                        cancel_request_id,
                        completed: None,
                    },
                    ActiveControl::Fatal { error, response } => break BashExit::Fatal {
                        error,
                        response,
                        completed: None,
                    },
                }
            }
            _ = &mut control_reader, if !control_reader_done => control_reader_done = true,
            result = &mut execution => {
                match take_queued_control_after_completion(
                    &mut control_rx,
                    control_reader.as_mut(),
                    &mut control_reader_done,
                ).await {
                    Ok(next) => match classify_active_control(
                        Some(next),
                        identity,
                        &execution_id,
                        lifecycle,
                    ) {
                        ActiveControl::Cancel(cancel_request_id) => break BashExit::Cancelled {
                            cancel_request_id,
                            completed: Some(result),
                        },
                        ActiveControl::Fatal { error, response } => break BashExit::Fatal {
                            error,
                            response,
                            completed: Some(result),
                        },
                    },
                    Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                        break BashExit::Completed(result);
                    }
                }
            }
            update = updates_rx.recv() => {
                if let Some(value) = update
                    && let Err(error) = forward_bash_update(
                        writer,
                        identity,
                        lifecycle,
                        &request_id,
                        value,
                    )
                {
                    tracing::warn!("executor output queue unavailable; closing service epoch");
                    break BashExit::Fatal {
                        error,
                        response: None,
                        completed: None,
                    };
                }
            }
        }
    };

    match exit {
        BashExit::Fatal {
            error,
            response,
            completed,
        } => {
            cancel.cancel();
            let result = match completed {
                Some(result) => result,
                None => match timeout(EXECUTOR_REAP_DEADLINE, &mut execution).await {
                    Ok(result) => result,
                    Err(_) => {
                        tracing::warn!("executor cancellation exceeded its service deadline");
                        lifecycle.accept_terminal(&request_id)?;
                        writer
                            .terminal(
                                identity,
                                request_id,
                                Err(rpc_error(ToolError::RpcIndeterminate(
                                    "executor cancellation exceeded its service deadline"
                                        .to_owned(),
                                ))),
                            )
                            .await?;
                        if let Some((response_id, response_error)) = response {
                            writer
                                .terminal(identity, response_id, Err(response_error))
                                .await?;
                        }
                        return Err(error.into());
                    }
                },
            };
            drain_completed_bash_updates(
                writer,
                identity,
                lifecycle,
                &request_id,
                &mut updates_rx,
            )?;
            lifecycle.accept_terminal(&request_id)?;
            writer
                .terminal(
                    identity,
                    request_id,
                    result
                        .map(|result| ExecutorResponse::Bash { result })
                        .map_err(rpc_error),
                )
                .await?;
            if let Some((response_id, response_error)) = response {
                writer
                    .terminal(identity, response_id, Err(response_error))
                    .await?;
            }
            Err(error.into())
        }
        BashExit::Completed(result) => {
            drain_completed_bash_updates(
                writer,
                identity,
                lifecycle,
                &request_id,
                &mut updates_rx,
            )?;
            lifecycle.accept_terminal(&request_id)?;
            writer
                .terminal(
                    identity,
                    request_id,
                    result
                        .map(|result| ExecutorResponse::Bash { result })
                        .map_err(rpc_error),
                )
                .await?;
            Ok(())
        }
        BashExit::Cancelled {
            cancel_request_id,
            completed,
        } => {
            cancel.cancel();
            let result = match completed {
                Some(result) => result,
                None => match timeout(EXECUTOR_REAP_DEADLINE, &mut execution).await {
                    Ok(Ok(result)) => Ok(result),
                    Ok(Err(_reap_error)) => {
                        lifecycle.accept_terminal(&request_id)?;
                        writer
                            .terminal(
                                identity,
                                cancel_request_id,
                                Err(bounded_error("rpc_indeterminate")),
                            )
                            .await?;
                        writer
                            .terminal(
                                identity,
                                request_id,
                                Err(bounded_error("rpc_indeterminate")),
                            )
                            .await?;
                        return Err(anyhow::anyhow!(
                            "executor cancellation failed before cleanup was proven"
                        ));
                    }
                    Err(_) => {
                        tracing::warn!("executor cancellation exceeded its service deadline");
                        lifecycle.accept_terminal(&request_id)?;
                        writer
                            .terminal(
                                identity,
                                cancel_request_id,
                                Err(bounded_error("rpc_indeterminate")),
                            )
                            .await?;
                        writer
                            .terminal(
                                identity,
                                request_id,
                                Err(rpc_error(ToolError::RpcIndeterminate(
                                    "executor cancellation exceeded its service deadline"
                                        .to_owned(),
                                ))),
                            )
                            .await?;
                        return Err(anyhow::anyhow!(
                            "executor cancellation exceeded its service deadline"
                        ));
                    }
                },
            };
            drain_completed_bash_updates(
                writer,
                identity,
                lifecycle,
                &request_id,
                &mut updates_rx,
            )?;
            lifecycle.accept_terminal(&request_id)?;
            let cancel_response = match &result {
                Ok(result) if result.cancelled => Ok(ExecutorResponse::CancelAccepted {}),
                Ok(_) => Ok(ExecutorResponse::CancelTooLate {}),
                Err(_) => Err(bounded_error("rpc_indeterminate")),
            };
            writer
                .terminal(identity, cancel_request_id, cancel_response)
                .await?;
            writer
                .terminal(
                    identity,
                    request_id,
                    result
                        .map(|result| ExecutorResponse::Bash { result })
                        .map_err(rpc_error),
                )
                .await?;
            Ok(())
        }
    }
}

fn bounded_bash_updates() -> (BashUpdateCallback, mpsc::Receiver<Value>) {
    let (updates_tx, updates_rx) = mpsc::channel::<Value>(UPDATE_CHANNEL_CAPACITY);
    let on_update = Arc::new(move |value| match updates_tx.try_send(value) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            tracing::warn!("dropping volatile executor progress update: callback queue full");
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            tracing::warn!("dropping volatile executor progress update: callback queue closed");
        }
    });
    (on_update, updates_rx)
}

fn forward_bash_update(
    writer: &ExecutorWriter,
    identity: &RpcIdentity,
    lifecycle: &RpcLifecycleTracker,
    request_id: &str,
    value: Value,
) -> Result<(), ToolError> {
    lifecycle.accept_update(request_id)?;
    let frame = RpcFrame::<ExecutorResponse>::Update {
        generation: identity.generation,
        nonce: identity.nonce.clone(),
        request_id: request_id.to_owned(),
        value,
    };
    writer.try_update(&frame)
}

fn drain_completed_bash_updates(
    writer: &ExecutorWriter,
    identity: &RpcIdentity,
    lifecycle: &RpcLifecycleTracker,
    request_id: &str,
    updates_rx: &mut mpsc::Receiver<Value>,
) -> Result<(), ToolError> {
    // LowTrustLocalBash resolves only after its pipe reader and every callback
    // invocation complete. No producer can enqueue another value beyond this
    // point, so a nonblocking drain is a stable synchronization boundary, not
    // a racy snapshot.
    while let Ok(value) = updates_rx.try_recv() {
        forward_bash_update(writer, identity, lifecycle, request_id, value)?;
    }
    Ok(())
}

async fn take_queued_control_after_completion<F, T>(
    control_rx: &mut mpsc::Receiver<T>,
    mut control_reader: Pin<&mut F>,
    control_reader_done: &mut bool,
) -> Result<T, mpsc::error::TryRecvError>
where
    F: Future<Output = ()>,
{
    match control_rx.try_recv() {
        Ok(next) => return Ok(next),
        Err(mpsc::error::TryRecvError::Disconnected) => {
            return Err(mpsc::error::TryRecvError::Disconnected);
        }
        Err(mpsc::error::TryRecvError::Empty) => {}
    }

    if !*control_reader_done {
        // The nonblocking stdin reader observes the pipe directly here. This
        // final priority poll closes the gap where completion became ready
        // after the select had already polled control once.
        let poll = poll_fn(|context| Poll::Ready(control_reader.as_mut().poll(context))).await;
        *control_reader_done = poll.is_ready();
    }
    control_rx.try_recv()
}

fn classify_active_control(
    next: Option<Result<Option<Vec<u8>>, ToolError>>,
    identity: &RpcIdentity,
    execution_id: &str,
    lifecycle: &mut RpcLifecycleTracker,
) -> ActiveControl {
    let Some(next) = next else {
        return ActiveControl::Fatal {
            error: ToolError::Protocol(
                "executor control reader stopped without a terminal input state".to_owned(),
            ),
            response: None,
        };
    };
    let line = match next {
        Ok(Some(line)) => line,
        Ok(None) => {
            return ActiveControl::Fatal {
                error: ToolError::Protocol(
                    "executor input closed during active bash execution".to_owned(),
                ),
                response: None,
            };
        }
        Err(error) => {
            return ActiveControl::Fatal {
                error,
                response: None,
            };
        }
    };
    let incoming = match decode_executor_request(&line, identity) {
        Err(error) => {
            return ActiveControl::Fatal {
                error,
                response: None,
            };
        }
        Ok(Ok(request)) => request,
        Ok(Err((request_id, response_error))) => {
            let lifecycle_result = lifecycle
                .begin_request(&request_id)
                .and_then(|()| lifecycle.accept_terminal(&request_id));
            return match lifecycle_result {
                Ok(()) => ActiveControl::Fatal {
                    error: ToolError::Protocol(
                        "invalid control request during active bash execution".to_owned(),
                    ),
                    response: Some((request_id, response_error)),
                },
                Err(error) => ActiveControl::Fatal {
                    error,
                    response: None,
                },
            };
        }
    };
    if let ExecutorOperation::Cancel {
        execution_id: target,
    } = &incoming.operation
        && target == execution_id
    {
        return match lifecycle
            .accept_cancel(&incoming.request_id, target)
            .and_then(|()| lifecycle.accept_terminal(&incoming.request_id))
        {
            Ok(()) => ActiveControl::Cancel(incoming.request_id),
            Err(error) => ActiveControl::Fatal {
                error,
                response: None,
            },
        };
    }

    let request_id = incoming.request_id;
    match lifecycle
        .begin_request(&request_id)
        .and_then(|()| lifecycle.accept_terminal(&request_id))
    {
        Ok(()) => ActiveControl::Fatal {
            error: ToolError::Protocol(
                "only a matching cancel is valid during active bash execution".to_owned(),
            ),
            response: Some((request_id, bounded_error("protocol"))),
        },
        Err(error) => ActiveControl::Fatal {
            error,
            response: None,
        },
    }
}

async fn execute_non_bash(
    fs: &WorkspaceFs,
    broker: &ArtifactBrokerClient,
    operation: ExecutorOperation,
) -> Result<ExecutorResponse, ToolError> {
    match operation {
        ExecutorOperation::ReadFile {
            path,
            offset,
            limit,
            ..
        } => match resolve_input("read_file", &path)? {
            InputRoute::Workspace => Ok(ExecutorResponse::ReadFile {
                result: fs.read_file(path.as_ref(), offset, limit)?,
            }),
            InputRoute::Artifact => Ok(ExecutorResponse::Artifact {
                response: broker.read_artifact(&path, offset, limit).await?,
            }),
        },
        ExecutorOperation::WriteFile { path, content, .. } => {
            resolve_input("write_file", &path)?;
            fs.write_file(path.as_ref(), content.as_bytes())?;
            Ok(ExecutorResponse::Written {})
        }
        ExecutorOperation::EditFile {
            path,
            old_string,
            new_string,
            ..
        } => {
            resolve_input("edit_file", &path)?;
            fs.edit_file(path.as_ref(), &old_string, &new_string)?;
            Ok(ExecutorResponse::Edited {})
        }
        ExecutorOperation::RemoveFile { path, .. } => {
            resolve_input("remove_file", &path)?;
            fs.remove_file(path.as_ref())?;
            Ok(ExecutorResponse::Removed {})
        }
        ExecutorOperation::ListDir { path, .. } => {
            resolve_input("list_dir", &path)?;
            Ok(ExecutorResponse::Listed {
                entries: fs.list_dir(path.as_ref())?,
            })
        }
        ExecutorOperation::Glob { pattern, .. } => {
            resolve_input("glob", &pattern)?;
            Ok(ExecutorResponse::Globbed {
                paths: fs.glob(&pattern)?,
            })
        }
        ExecutorOperation::Grep { path, pattern, .. } => {
            if resolve_input("grep", &path)? == InputRoute::Artifact {
                Ok(ExecutorResponse::Artifact {
                    response: broker.grep_artifact(&path, &pattern).await?,
                })
            } else {
                let pattern = Regex::new(&pattern)
                    .map_err(|_| ToolError::Protocol("invalid grep pattern".to_owned()))?;
                Ok(ExecutorResponse::Grepped {
                    matches: fs.grep(path.as_ref(), &pattern)?,
                })
            }
        }
        ExecutorOperation::Bash { .. } | ExecutorOperation::Cancel { .. } => Err(
            ToolError::Protocol("operation belongs to executor control loop".to_owned()),
        ),
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

fn decode_executor_request(
    line: &[u8],
    identity: &RpcIdentity,
) -> Result<Result<RpcRequest<ExecutorOperation>, (String, RpcError)>, ToolError> {
    match decode_rpc_line::<ExecutorOperation>(line, identity) {
        Ok(request) => Ok(Ok(request)),
        Err(validation_error) => {
            let request = match serde_json::from_slice::<RpcRequest<ExecutorOperation>>(line) {
                Ok(request) => request,
                Err(_) => return Err(validation_error),
            };
            if request.generation != identity.generation
                || request.nonce != identity.nonce
                || request.request_id.is_empty()
                || request.request_id.len() > 128
            {
                return Err(validation_error);
            }
            Ok(Err((request.request_id, rpc_error(validation_error))))
        }
    }
}

fn decode_artifact_request(
    line: &[u8],
    identity: &RpcIdentity,
) -> Result<Result<RpcRequest<ArtifactOperation>, (String, RpcError)>, ToolError> {
    match decode_rpc_line::<ArtifactOperation>(line, identity) {
        Ok(request) => Ok(Ok(request)),
        Err(validation_error) => {
            let request = match serde_json::from_slice::<RpcRequest<ArtifactOperation>>(line) {
                Ok(request) => request,
                Err(_) => return Err(validation_error),
            };
            if request.generation != identity.generation
                || request.nonce != identity.nonce
                || request.request_id.is_empty()
                || request.request_id.len() > 128
            {
                return Err(validation_error);
            }
            Ok(Err((request.request_id, rpc_error(validation_error))))
        }
    }
}

async fn read_frame<R: AsyncBufRead + Unpin>(read: &mut R) -> Result<Option<Vec<u8>>, ToolError> {
    let mut line = Vec::with_capacity(4096);
    loop {
        let buffer = read.fill_buf().await?;
        if buffer.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Err(ToolError::Protocol(
                    "RPC input ended before newline".to_owned(),
                ))
            };
        }
        let separator = buffer.iter().position(|byte| matches!(byte, b'\n' | b'\r'));
        let take = separator.unwrap_or(buffer.len());
        if line.len().saturating_add(take) > MAX_RPC_LINE_BYTES - 1 {
            return Err(ToolError::Protocol("RPC frame exceeds 1MiB".to_owned()));
        }
        line.extend_from_slice(&buffer[..take]);
        if let Some(position) = separator {
            let delimiter = buffer[position];
            read.consume(position + 1);
            if delimiter == b'\r' {
                return Err(ToolError::Protocol(
                    "RPC frame contained carriage return".to_owned(),
                ));
            }
            if line.is_empty() {
                return Err(ToolError::Protocol("empty RPC frame".to_owned()));
            }
            return Ok(Some(line));
        } else {
            read.consume(take);
        }
    }
}

async fn write_frame<W, T>(write: &mut W, frame: &RpcFrame<T>) -> Result<(), ToolError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let encoded = encode_rpc_frame(frame)?;
    write.write_all(&encoded).await?;
    write.flush().await?;
    Ok(())
}

fn rpc_error(error: ToolError) -> RpcError {
    match error {
        ToolError::InvalidArguments => bounded_error("invalid_arguments"),
        ToolError::Cancelled => bounded_error("cancelled"),
        ToolError::ResourceLimit(limit) => RpcError {
            code: "resource_limit".to_owned(),
            resource_limit: Some(limit),
        },
        ToolError::InvalidPath(_) => bounded_error("invalid_path"),
        ToolError::Io(_) => bounded_error("io"),
        ToolError::Rpc(_) => bounded_error("rpc"),
        ToolError::RpcIndeterminate(_) => bounded_error("rpc_indeterminate"),
        ToolError::Protocol(_) => bounded_error("protocol"),
    }
}

fn bounded_error(code: &str) -> RpcError {
    RpcError {
        code: code.to_owned(),
        resource_limit: None,
    }
}

fn io_error(message: &str) -> ToolError {
    ToolError::Io(std::io::Error::other(message.to_owned()))
}

fn nonblocking_stdout() -> std::io::Result<NonblockingStdout> {
    Ok(NonblockingStdout {
        fd: nonblocking_stdio_fd(libc::STDOUT_FILENO)?,
    })
}

fn nonblocking_stdin() -> std::io::Result<NonblockingStdin> {
    Ok(NonblockingStdin {
        fd: nonblocking_stdio_fd(libc::STDIN_FILENO)?,
    })
}

fn nonblocking_stdio_fd(raw_fd: libc::c_int) -> std::io::Result<AsyncFd<OwnedFd>> {
    let duplicated = unsafe { libc::dup(raw_fd) };
    if duplicated == -1 {
        return Err(std::io::Error::last_os_error());
    }
    let fd = unsafe { OwnedFd::from_raw_fd(duplicated) };
    let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
    if flags == -1 {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        return Err(std::io::Error::last_os_error());
    }
    if unsafe { libc::close(raw_fd) } == -1 {
        return Err(std::io::Error::last_os_error());
    }
    AsyncFd::new(fd)
}

fn identity_from_env() -> Result<RpcIdentity> {
    let generation = required_text("SUMI_RPC_GENERATION")?
        .parse::<u64>()
        .context("SUMI_RPC_GENERATION must be an unsigned integer")?;
    let nonce = required_text("SUMI_RPC_NONCE")?;
    if nonce.len() > 128 {
        bail!("SUMI_RPC_NONCE must contain at most 128 bytes");
    }
    Ok(RpcIdentity { generation, nonce })
}

fn required_text(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required for service mode"))?;
    if value.is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(value)
}

fn required_path(name: &str) -> Result<PathBuf> {
    let path = PathBuf::from(required_text(name)?);
    if !path.is_absolute() {
        bail!("{name} must be an absolute path");
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::executor::decode_rpc_frame;

    struct QueueControlOnSecondPoll {
        polls: usize,
        sender: mpsc::Sender<&'static str>,
    }

    impl Future for QueueControlOnSecondPoll {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, _context: &mut TaskContext<'_>) -> Poll<Self::Output> {
            self.polls += 1;
            if self.polls == 2 {
                self.sender.try_send("cancel").unwrap();
            }
            Poll::Pending
        }
    }

    #[test]
    fn update_callback_burst_drops_overflow_without_reordering_delivery() {
        let (callback, mut receiver) = bounded_bash_updates();
        for index in 0..=UPDATE_CHANNEL_CAPACITY {
            callback(serde_json::json!({"index": index}));
        }

        let received = std::iter::from_fn(|| receiver.try_recv().ok()).collect::<Vec<_>>();
        assert_eq!(received.len(), UPDATE_CHANNEL_CAPACITY);
        assert_eq!(received[0]["index"], 0);
        assert_eq!(received[UPDATE_CHANNEL_CAPACITY - 1]["index"], 31);
    }

    #[test]
    fn service_preserves_post_commit_indeterminate_error_code() {
        assert_eq!(
            rpc_error(ToolError::RpcIndeterminate(
                "mutation committed before directory sync failed".to_owned(),
            ))
            .code,
            "rpc_indeterminate"
        );
    }

    #[tokio::test]
    async fn completion_repolls_control_reader_before_settling() {
        let (sender, mut receiver) = mpsc::channel(1);
        let reader = QueueControlOnSecondPoll { polls: 0, sender };
        tokio::pin!(reader);
        let first_poll = poll_fn(|context| Poll::Ready(reader.as_mut().poll(context))).await;
        assert!(first_poll.is_pending());

        let mut reader_done = false;
        let control =
            take_queued_control_after_completion(&mut receiver, reader.as_mut(), &mut reader_done)
                .await;

        assert_eq!(control.unwrap(), "cancel");
        assert!(!reader_done);
    }

    #[tokio::test]
    async fn completed_bash_updates_are_emitted_before_terminal() {
        let identity = RpcIdentity {
            generation: 7,
            nonce: "boot-nonce".to_owned(),
        };
        let (read_side, write_side) = tokio::io::duplex(4096);
        let (writer, writer_task) = ExecutorWriter::start(write_side);
        let mut lifecycle = RpcLifecycleTracker::default();
        lifecycle
            .begin_execution("request-1", "execution-1")
            .expect("begin execution");
        let (updates_tx, mut updates_rx) = mpsc::channel(UPDATE_CHANNEL_CAPACITY);
        updates_tx
            .send(serde_json::json!({"output": "first"}))
            .await
            .expect("queue first update");
        updates_tx
            .send(serde_json::json!({"output": "second"}))
            .await
            .expect("queue second update");
        drop(updates_tx);

        drain_completed_bash_updates(&writer, &identity, &lifecycle, "request-1", &mut updates_rx)
            .expect("drain completed updates");
        lifecycle
            .accept_terminal("request-1")
            .expect("accept terminal");
        writer
            .terminal(
                &identity,
                "request-1".to_owned(),
                Ok(ExecutorResponse::Written {}),
            )
            .await
            .expect("write terminal");

        let mut read = BufReader::new(read_side);
        let mut delivered = Vec::new();
        loop {
            let frame = decode_rpc_frame::<ExecutorResponse>(
                &read_frame(&mut read)
                    .await
                    .expect("read frame")
                    .expect("frame"),
                &identity,
            )
            .expect("decode frame");
            match frame {
                RpcFrame::Update { value, .. } => delivered.push(value["output"].clone()),
                RpcFrame::Terminal { .. } => break,
            }
        }
        assert!(
            delivered == Vec::<Value>::new()
                || delivered == vec![serde_json::json!("first")]
                || delivered == vec![serde_json::json!("first"), serde_json::json!("second")]
        );
        writer_task.abort();
    }

    #[test]
    fn nonblocking_stdin_retries_only_interrupted_reads() {
        assert!(is_interrupted_read_error(
            &std::io::Error::from_raw_os_error(libc::EINTR)
        ));
        assert!(is_interrupted_read_error(&std::io::Error::from(
            std::io::ErrorKind::Interrupted,
        )));
        assert!(!is_interrupted_read_error(&std::io::Error::from(
            std::io::ErrorKind::WouldBlock,
        )));
        assert!(!is_interrupted_read_error(&std::io::Error::from(
            std::io::ErrorKind::BrokenPipe,
        )));
    }

    #[tokio::test]
    async fn blocking_work_registry_has_no_unbounded_waiting_queue() {
        let registry = BlockingWorkRegistry::new(2);
        let first = registry.reserve().await.unwrap();
        let second = registry.reserve().await.unwrap();
        assert!(
            timeout(Duration::from_millis(10), registry.reserve())
                .await
                .is_err()
        );
        drop(first);
        let replacement = timeout(Duration::from_millis(10), registry.reserve())
            .await
            .expect("released blocking-work permit")
            .unwrap();
        drop((second, replacement));
    }
}
