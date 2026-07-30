//! Bounded, generation-fenced client for the artifact broker socket.

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use sha2::Digest;
use tokio::{
    io::{
        AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt,
        BufReader,
    },
    net::UnixStream,
    time::timeout,
};
use uuid::Uuid;

use super::{
    ArtifactOperation, ArtifactResponse, MAX_ATTACHMENT_CHUNK_BYTES, MAX_RPC_LINE_BYTES, RpcError,
    RpcFrame, RpcOperationValidation, RpcRequest, decode_rpc_frame,
};
use crate::runtime::contracts::RpcIdentity;
use crate::tools::{ToolError, shell_capture::ArtifactAppender};

/// Each exchange uses a fresh socket connection and permits exactly one
/// request and one terminal response. This keeps the client bounded without a
/// pending-request map and makes EOF an unambiguous request failure.
pub struct ArtifactBrokerClient {
    socket: PathBuf,
    identity: RpcIdentity,
}

impl ArtifactBrokerClient {
    #[cfg(not(test))]
    const EXCHANGE_DEADLINE: Duration = Duration::from_secs(2);
    #[cfg(test)]
    const EXCHANGE_DEADLINE: Duration = Duration::from_millis(100);
    pub fn new(socket: impl Into<PathBuf>, identity: RpcIdentity) -> Self {
        Self {
            socket: socket.into(),
            identity,
        }
    }

    pub async fn execute(
        &self,
        operation: ArtifactOperation,
    ) -> Result<ArtifactResponse, ToolError> {
        operation.validate()?;
        operation.validate_authenticated_owner(self.identity.personality_agent_id())?;
        let request_emitted = Arc::new(AtomicBool::new(false));
        match timeout(
            Self::EXCHANGE_DEADLINE,
            self.execute_inner(operation, request_emitted.clone()),
        )
        .await
        {
            Ok(result) => result,
            Err(_) if request_emitted.load(Ordering::Acquire) => Err(ToolError::RpcIndeterminate(
                "artifact broker exchange deadline elapsed".to_owned(),
            )),
            Err(_) => Err(ToolError::Rpc(
                "artifact broker connection deadline elapsed before request emission".to_owned(),
            )),
        }
    }

    async fn execute_inner(
        &self,
        operation: ArtifactOperation,
        request_emitted: Arc<AtomicBool>,
    ) -> Result<ArtifactResponse, ToolError> {
        let request_id = format!("broker-{}", Uuid::now_v7());
        let request = RpcRequest {
            personality_agent_id: self.identity.personality_agent_id().clone(),
            generation: self.identity.generation().to_wire(),
            nonce: self.identity.nonce().as_str().to_owned(),
            request_id: request_id.clone(),
            operation,
        };
        let mut encoded = serde_json::to_vec(&request)
            .map_err(|_| ToolError::Protocol("artifact request encode failed".to_owned()))?;
        if encoded
            .len()
            .checked_add(1)
            .is_none_or(|length| length > MAX_RPC_LINE_BYTES)
        {
            return Err(ToolError::Protocol(
                "artifact request exceeds 1MiB".to_owned(),
            ));
        }
        encoded.push(b'\n');

        let stream = UnixStream::connect(&self.socket).await.map_err(|error| {
            ToolError::Rpc(format!("artifact broker connection failed: {error}"))
        })?;
        self.exchange_stream(stream, &encoded, &request_id, request_emitted)
            .await
    }

    async fn exchange_stream<S>(
        &self,
        mut stream: S,
        encoded: &[u8],
        request_id: &str,
        request_emitted: Arc<AtomicBool>,
    ) -> Result<ArtifactResponse, ToolError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        stream
            .write_all(encoded)
            .await
            .map_err(|error| ToolError::Rpc(format!("artifact broker request failed: {error}")))?;
        request_emitted.store(true, Ordering::Release);
        stream.shutdown().await.map_err(|error| {
            // `write_all` included the terminating newline, so the broker may
            // already have accepted and committed the mutation.  A failed
            // half-close cannot prove otherwise and must be replayed with the
            // operation's idempotency key.
            ToolError::RpcIndeterminate(format!("artifact broker request shutdown failed: {error}"))
        })?;

        let mut stream = BufReader::new(stream);
        let line = read_bounded_line(&mut stream)
            .await
            .map_err(indeterminate)?;
        let frame =
            decode_rpc_frame::<ArtifactResponse>(&line, &self.identity).map_err(indeterminate)?;
        let result = match frame {
            RpcFrame::Terminal {
                request_id: response_id,
                result,
                ..
            } if response_id == request_id => result,
            RpcFrame::Terminal { .. } => {
                return Err(ToolError::RpcIndeterminate(
                    "artifact response request_id mismatch".to_owned(),
                ));
            }
            RpcFrame::Update { .. } => {
                return Err(ToolError::RpcIndeterminate(
                    "artifact broker emitted an unexpected update".to_owned(),
                ));
            }
        };
        let mut trailing = [0u8; 1];
        match stream.read(&mut trailing).await {
            Ok(0) => result.map_err(map_rpc_error),
            Ok(_) => Err(ToolError::RpcIndeterminate(
                "artifact broker emitted more than one response frame".to_owned(),
            )),
            Err(error) => Err(ToolError::RpcIndeterminate(format!(
                "artifact broker trailing response read failed: {error}"
            ))),
        }
    }

    pub async fn read_artifact(
        &self,
        handle: &str,
        offset: u64,
        limit: usize,
    ) -> Result<ArtifactResponse, ToolError> {
        let response = self
            .execute(ArtifactOperation::ReadArtifact {
                handle: handle.to_owned(),
                offset,
                limit,
            })
            .await?;
        match &response {
            ArtifactResponse::Read { content, .. } if content.len() <= limit => Ok(response),
            ArtifactResponse::Read { .. } => Err(ToolError::Protocol(
                "artifact read exceeded the requested limit".to_owned(),
            )),
            _ => Err(ToolError::Protocol(
                "artifact read returned the wrong response variant".to_owned(),
            )),
        }
    }

    pub async fn grep_artifact(
        &self,
        handle: &str,
        pattern: &str,
    ) -> Result<ArtifactResponse, ToolError> {
        let response = self
            .execute(ArtifactOperation::GrepArtifact {
                handle: handle.to_owned(),
                pattern: pattern.to_owned(),
            })
            .await?;
        match response {
            ArtifactResponse::Grep { .. } => Ok(response),
            _ => Err(ToolError::Protocol(
                "artifact grep returned the wrong response variant".to_owned(),
            )),
        }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    pub(crate) const fn identity(&self) -> &RpcIdentity {
        &self.identity
    }

    pub async fn put_attachment(
        &self,
        artifact_id: &str,
        content: &str,
    ) -> Result<String, ToolError> {
        let bytes = content.as_bytes();
        let total_bytes = u64::try_from(bytes.len())
            .map_err(|_| ToolError::Protocol("attachment length overflow".to_owned()))?;
        let content_digest = format!("{:x}", sha2::Sha256::digest(bytes));
        let response = self
            .execute(ArtifactOperation::BeginAttachment {
                artifact_id: artifact_id.to_owned(),
                total_bytes,
                content_digest: content_digest.clone(),
            })
            .await?;
        let mut offset = match response {
            ArtifactResponse::AttachmentBegun { offset } if offset <= total_bytes => offset,
            ArtifactResponse::AttachmentBegun { .. } => {
                return Err(ToolError::RpcIndeterminate(
                    "attachment begin returned an invalid offset".to_owned(),
                ));
            }
            _ => {
                return Err(ToolError::Protocol(
                    "attachment begin returned the wrong response variant".to_owned(),
                ));
            }
        };
        while offset < total_bytes {
            let start = usize::try_from(offset)
                .map_err(|_| ToolError::Protocol("attachment offset overflow".to_owned()))?;
            let end = start
                .saturating_add(MAX_ATTACHMENT_CHUNK_BYTES)
                .min(bytes.len());
            let next = u64::try_from(end)
                .map_err(|_| ToolError::Protocol("attachment offset overflow".to_owned()))?;
            let response = self
                .execute(ArtifactOperation::AppendAttachment {
                    artifact_id: artifact_id.to_owned(),
                    total_bytes,
                    content_digest: content_digest.clone(),
                    offset,
                    content: bytes[start..end].to_vec(),
                })
                .await?;
            match response {
                ArtifactResponse::AttachmentAppended {
                    offset: acknowledged,
                } if acknowledged == next => offset = acknowledged,
                ArtifactResponse::AttachmentAppended { .. } => {
                    return Err(ToolError::RpcIndeterminate(
                        "attachment append returned an unexpected offset".to_owned(),
                    ));
                }
                _ => {
                    return Err(ToolError::Protocol(
                        "attachment append returned the wrong response variant".to_owned(),
                    ));
                }
            }
        }
        let response = self
            .execute(ArtifactOperation::FinishAttachment {
                artifact_id: artifact_id.to_owned(),
                total_bytes,
                content_digest,
            })
            .await?;
        match response {
            ArtifactResponse::Put { handle } if handle == self.attachment_handle(artifact_id) => {
                Ok(handle)
            }
            ArtifactResponse::Put { .. } => Err(ToolError::RpcIndeterminate(
                "artifact put returned an unexpected canonical handle".to_owned(),
            )),
            _ => Err(ToolError::Protocol(
                "artifact put returned the wrong response variant".to_owned(),
            )),
        }
    }

    fn attachment_handle(&self, artifact_id: &str) -> String {
        format!(
            "artifact://{}/attachments/{artifact_id}",
            self.identity.personality_agent_id()
        )
    }
}

#[async_trait]
impl ArtifactAppender for ArtifactBrokerClient {
    async fn begin_tool_output(
        &self,
        execution_id: &str,
        initial_content: &[u8],
    ) -> Result<String, ToolError> {
        match self
            .execute(ArtifactOperation::BeginToolOutput {
                execution_id: execution_id.to_owned(),
                content: initial_content.to_vec(),
            })
            .await?
        {
            ArtifactResponse::Begun { handle, offset }
                if offset
                    == u64::try_from(initial_content.len()).map_err(|_| {
                        ToolError::Protocol("artifact initial length overflow".to_owned())
                    })?
                    && handle
                        == format!(
                            "artifact://{}/tool-output/{execution_id}",
                            self.identity.personality_agent_id()
                        ) =>
            {
                Ok(handle)
            }
            ArtifactResponse::Begun { .. } => Err(ToolError::RpcIndeterminate(
                "artifact begin acknowledged the wrong canonical handle or offset".to_owned(),
            )),
            _ => Err(ToolError::RpcIndeterminate(
                "artifact begin returned the wrong response variant".to_owned(),
            )),
        }
    }

    async fn append_tool_output(
        &self,
        handle: &str,
        offset: u64,
        content: &[u8],
    ) -> Result<(), ToolError> {
        match self
            .execute(ArtifactOperation::AppendToolOutput {
                handle: handle.to_owned(),
                offset,
                content: content.to_vec(),
            })
            .await?
        {
            ArtifactResponse::Appended { offset: next }
                if next
                    == offset
                        .checked_add(u64::try_from(content.len()).map_err(|_| {
                            ToolError::Protocol("artifact append length overflow".to_owned())
                        })?)
                        .ok_or_else(|| {
                            ToolError::Protocol("artifact append offset overflow".to_owned())
                        })? =>
            {
                Ok(())
            }
            ArtifactResponse::Appended { .. } => Err(ToolError::RpcIndeterminate(
                "artifact append acknowledged the wrong offset".to_owned(),
            )),
            _ => Err(ToolError::RpcIndeterminate(
                "artifact append returned the wrong response variant".to_owned(),
            )),
        }
    }

    async fn finish_tool_output(&self, handle: &str) -> Result<(), ToolError> {
        match self
            .execute(ArtifactOperation::FinishToolOutput {
                handle: handle.to_owned(),
            })
            .await?
        {
            ArtifactResponse::Finished => Ok(()),
            _ => Err(ToolError::Protocol(
                "artifact finish returned the wrong response variant".to_owned(),
            )),
        }
    }
}

async fn read_bounded_line<R: AsyncBufRead + Unpin>(stream: &mut R) -> Result<Vec<u8>, ToolError> {
    let mut line = Vec::with_capacity(4096);
    loop {
        let buffer = stream.fill_buf().await.map_err(|error| {
            ToolError::Rpc(format!("artifact broker response read failed: {error}"))
        })?;
        if buffer.is_empty() {
            return Err(ToolError::Protocol(
                "artifact broker closed before a terminal response".to_owned(),
            ));
        }
        let separator = buffer.iter().position(|byte| matches!(byte, b'\n' | b'\r'));
        let take = separator.unwrap_or(buffer.len());
        if line.len().saturating_add(take) > MAX_RPC_LINE_BYTES - 1 {
            return Err(ToolError::Protocol(
                "artifact broker response exceeds 1MiB".to_owned(),
            ));
        }
        line.extend_from_slice(&buffer[..take]);
        if let Some(position) = separator {
            let delimiter = buffer[position];
            stream.consume(position + 1);
            if delimiter == b'\r' {
                return Err(ToolError::Protocol(
                    "artifact broker response contained carriage return".to_owned(),
                ));
            }
            if line.is_empty() {
                return Err(ToolError::Protocol(
                    "artifact broker emitted an empty response frame".to_owned(),
                ));
            }
            return Ok(line);
        } else {
            stream.consume(take);
        }
    }
}

fn map_rpc_error(error: RpcError) -> ToolError {
    match (error.code.as_str(), error.resource_limit) {
        ("resource_limit", Some(limit)) => ToolError::ResourceLimit(limit),
        ("cancelled", None) => ToolError::Cancelled,
        ("invalid_path", None) => ToolError::InvalidPath("artifact path rejected".to_owned()),
        ("io", None) => ToolError::Rpc("artifact broker I/O failed".to_owned()),
        ("protocol", None) => ToolError::Protocol("artifact broker rejected request".to_owned()),
        (_, _) => ToolError::Rpc("artifact broker operation failed".to_owned()),
    }
}

fn indeterminate(error: ToolError) -> ToolError {
    ToolError::RpcIndeterminate(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAID: &str = "0198f0f4-9b72-7000-8000-000000000001";
    use crate::tools::{shell_capture::ShellCapture, truncate::DEFAULT_MAX_BYTES};
    use serde_json::json;
    use std::{
        collections::HashMap,
        ffi::CString,
        io,
        os::{
            fd::{AsRawFd, FromRawFd, OwnedFd},
            unix::ffi::OsStrExt,
        },
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
    };
    use tokio::{
        io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf},
        net::UnixListener,
    };

    struct ShutdownFailureStream {
        written: Arc<Mutex<Vec<u8>>>,
    }

    impl AsyncRead for ShutdownFailureStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            _buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for ShutdownFailureStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.written.lock().unwrap().extend_from_slice(buffer);
            Poll::Ready(Ok(buffer.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected shutdown failure",
            )))
        }
    }

    async fn fake_exchange(mode: &'static str) -> ToolError {
        let root = std::env::temp_dir().join(format!("sumi-client-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let socket = root.join("broker.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut read = BufReader::new(read);
            let mut request = String::new();
            read.read_line(&mut request).await.unwrap();
            match mode {
                "partial" => write.write_all(b"{").await.unwrap(),
                "trailing" => {
                    let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                    let response = json!({
                        "type":"terminal", "personality_agent_id":PAID, "generation":1, "nonce":"nonce",
                        "request_id":request["request_id"],
                        "result":{"Ok":{"type":"finished"}}
                    });
                    let mut bytes = serde_json::to_vec(&response).unwrap();
                    bytes.push(b'\n');
                    write.write_all(&bytes).await.unwrap();
                }
                "silent" => {}
                _ => unreachable!(),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        let client =
            ArtifactBrokerClient::new(&socket, RpcIdentity::from_wire(PAID, 1, "nonce").unwrap());
        let error = client
            .execute(ArtifactOperation::FinishToolOutput {
                handle: "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/execution-1"
                    .to_owned(),
            })
            .await
            .unwrap_err();
        server.abort();
        let _ = std::fs::remove_dir_all(root);
        error
    }

    #[tokio::test]
    async fn outgoing_request_rejects_out_of_domain_generation_before_connecting() {
        let error = RpcIdentity::from_wire(
            PAID,
            crate::runtime::contracts::MAX_PROCESS_GENERATION + 1,
            "boot-nonce",
        )
        .expect_err("invalid generation");
        assert!(error.to_string().contains("generation"));
    }

    #[tokio::test]
    async fn cross_personality_agent_handle_is_rejected_before_connecting() {
        let client = ArtifactBrokerClient::new(
            "/path/that/must/not/be-contacted.sock",
            RpcIdentity::from_wire(PAID, 1, "nonce").unwrap(),
        );

        let error = client
            .execute(ArtifactOperation::ReadArtifact {
                handle: "artifact://0198f0f4-9b72-7000-8000-000000000002/tool-output/execution-1"
                    .to_owned(),
                offset: 0,
                limit: 1,
            })
            .await
            .expect_err("cross-personality-agent handle must fail locally");

        assert!(matches!(
            error,
            ToolError::InvalidPath(message)
                if message == "artifact belongs to another personality agent"
        ));
    }

    #[tokio::test]
    async fn silent_partial_and_trailing_open_brokers_are_bounded_and_typed() {
        for mode in ["silent", "partial", "trailing"] {
            assert!(
                matches!(fake_exchange(mode).await, ToolError::RpcIndeterminate(_)),
                "{mode}"
            );
        }
    }

    #[tokio::test]
    async fn complete_request_followed_by_shutdown_failure_is_indeterminate() {
        let client =
            ArtifactBrokerClient::new("/unused", RpcIdentity::from_wire(PAID, 1, "nonce").unwrap());
        let written = Arc::new(Mutex::new(Vec::new()));
        let error = client
            .exchange_stream(
                ShutdownFailureStream {
                    written: written.clone(),
                },
                b"complete-request\n",
                "request-1",
                Arc::new(AtomicBool::new(false)),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ToolError::RpcIndeterminate(_)));
        assert_eq!(&*written.lock().unwrap(), b"complete-request\n");
    }

    #[tokio::test]
    async fn timeout_before_request_emission_is_determinate_and_sends_no_bytes() {
        let root = std::env::temp_dir().join(format!("sumi-saturated-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let socket = root.join("broker.sock");
        let listener = unsafe {
            let raw = libc::socket(
                libc::AF_UNIX,
                libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                0,
            );
            assert_ne!(raw, -1);
            OwnedFd::from_raw_fd(raw)
        };
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        address.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let path = CString::new(socket.as_os_str().as_bytes()).unwrap();
        assert!(path.as_bytes_with_nul().len() <= address.sun_path.len());
        for (destination, source) in address
            .sun_path
            .iter_mut()
            .zip(path.as_bytes_with_nul().iter())
        {
            *destination = *source as libc::c_char;
        }
        let address_length = (std::mem::offset_of!(libc::sockaddr_un, sun_path)
            + path.as_bytes_with_nul().len()) as libc::socklen_t;
        assert_eq!(
            unsafe {
                libc::bind(
                    listener.as_raw_fd(),
                    (&raw const address).cast(),
                    address_length,
                )
            },
            0
        );
        assert_eq!(unsafe { libc::listen(listener.as_raw_fd(), 1) }, 0);

        let mut fillers = Vec::new();
        let mut saturated = false;
        for _ in 0..32 {
            let raw = unsafe {
                libc::socket(
                    libc::AF_UNIX,
                    libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                    0,
                )
            };
            assert_ne!(raw, -1);
            let filler = unsafe { OwnedFd::from_raw_fd(raw) };
            let connected = unsafe {
                libc::connect(
                    filler.as_raw_fd(),
                    (&raw const address).cast(),
                    address_length,
                )
            };
            if connected == 0 {
                fillers.push(filler);
                continue;
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EAGAIN) {
                saturated = true;
                break;
            }
            panic!("unexpected filler connect error: {error}");
        }
        assert!(saturated, "Unix listener backlog must be saturated");

        let client =
            ArtifactBrokerClient::new(&socket, RpcIdentity::from_wire(PAID, 1, "nonce").unwrap());
        let error = client
            .execute(ArtifactOperation::FinishToolOutput {
                handle: "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/no-send"
                    .to_owned(),
            })
            .await
            .unwrap_err();
        assert!(matches!(error, ToolError::Rpc(_)), "{error:?}");

        let mut observed = Vec::new();
        loop {
            let accepted = unsafe {
                libc::accept4(
                    listener.as_raw_fd(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
                )
            };
            if accepted == -1 {
                assert_eq!(
                    io::Error::last_os_error().raw_os_error(),
                    Some(libc::EAGAIN)
                );
                break;
            }
            let accepted = unsafe { OwnedFd::from_raw_fd(accepted) };
            let mut buffer = [0u8; 4096];
            let read = unsafe {
                libc::read(
                    accepted.as_raw_fd(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                )
            };
            if read > 0 {
                observed.extend_from_slice(&buffer[..read as usize]);
            } else if read == -1 {
                assert_eq!(
                    io::Error::last_os_error().raw_os_error(),
                    Some(libc::EAGAIN)
                );
            }
        }
        assert!(
            observed.is_empty(),
            "pre-send timeout emitted request bytes"
        );
        drop((fillers, listener));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn trailing_bytes_after_success_or_error_are_indeterminate() {
        for terminal_result in [
            json!({"Ok":{"type":"finished"}}),
            json!({"Err":{"code":"protocol","resource_limit":null}}),
        ] {
            let root = std::env::temp_dir().join(format!("sumi-trailing-{}", Uuid::now_v7()));
            std::fs::create_dir_all(&root).unwrap();
            let socket = root.join("broker.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let (read, mut write) = stream.into_split();
                let mut read = BufReader::new(read);
                let mut request = String::new();
                read.read_line(&mut request).await.unwrap();
                let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                let response = json!({
                    "type":"terminal", "personality_agent_id":PAID, "generation":1, "nonce":"nonce",
                    "request_id":request["request_id"], "result":terminal_result,
                });
                let mut bytes = serde_json::to_vec(&response).unwrap();
                bytes.extend_from_slice(b"\ntrailing");
                write.write_all(&bytes).await.unwrap();
            });
            let client = ArtifactBrokerClient::new(
                &socket,
                RpcIdentity::from_wire(PAID, 1, "nonce").unwrap(),
            );
            assert!(matches!(
                client
                    .execute(ArtifactOperation::FinishToolOutput {
                        handle:
                            "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/trailing"
                                .to_owned(),
                    })
                    .await,
                Err(ToolError::RpcIndeterminate(_))
            ));
            server.await.unwrap();
            let _ = std::fs::remove_dir_all(root);
        }
    }

    #[tokio::test]
    async fn authenticated_terminal_error_is_determinate_only_after_eof() {
        let root = std::env::temp_dir().join(format!("sumi-error-eof-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let socket = root.join("broker.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut read = BufReader::new(read);
            let mut request = String::new();
            read.read_line(&mut request).await.unwrap();
            let request: serde_json::Value = serde_json::from_str(&request).unwrap();
            let response = json!({
                "type":"terminal", "personality_agent_id":PAID, "generation":1, "nonce":"nonce",
                "request_id":request["request_id"],
                "result":{"Err":{"code":"protocol","resource_limit":null}},
            });
            let mut bytes = serde_json::to_vec(&response).unwrap();
            bytes.push(b'\n');
            write.write_all(&bytes).await.unwrap();
        });
        let client =
            ArtifactBrokerClient::new(&socket, RpcIdentity::from_wire(PAID, 1, "nonce").unwrap());
        assert!(matches!(
            client
                .execute(ArtifactOperation::FinishToolOutput {
                    handle: "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/error"
                        .to_owned(),
                })
                .await,
            Err(ToolError::Protocol(_))
        ));
        server.await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn corrupt_mismatched_and_invalid_receipts_replay_to_one_exact_artifact() {
        let root = std::env::temp_dir().join(format!("sumi-replay-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let socket = root.join("broker.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let mut content = Vec::new();
            let mut begin_attempts = 0usize;
            let mut append_attempts = HashMap::<u64, usize>::new();
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let (read, mut write) = stream.into_split();
                let mut read = BufReader::new(read);
                let mut request = String::new();
                read.read_line(&mut request).await.unwrap();
                let request: serde_json::Value = serde_json::from_str(&request).unwrap();
                let request_id = request["request_id"].as_str().unwrap();
                let operation = &request["operation"];
                let response_result = match operation["type"].as_str().unwrap() {
                    "begin_tool_output" => {
                        begin_attempts += 1;
                        let initial: Vec<u8> =
                            serde_json::from_value(operation["content"].clone()).unwrap();
                        if content.is_empty() {
                            content.extend_from_slice(&initial);
                        } else {
                            assert!(content.starts_with(&initial));
                        }
                        let offset = initial.len() + usize::from(begin_attempts == 1);
                        json!({"Ok":{
                            "type":"begun",
                            "handle":"artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/replay-secret",
                            "offset":offset,
                        }})
                    }
                    "append_tool_output" => {
                        let offset = operation["offset"].as_u64().unwrap();
                        let bytes: Vec<u8> =
                            serde_json::from_value(operation["content"].clone()).unwrap();
                        let offset_usize = usize::try_from(offset).unwrap();
                        if content.len() == offset_usize {
                            content.extend_from_slice(&bytes);
                        } else {
                            assert_eq!(&content[offset_usize..], bytes);
                        }
                        let attempts = append_attempts.entry(offset).or_default();
                        *attempts += 1;
                        if *attempts == 1 && bytes == b"A" {
                            write.write_all(b"not-json\n").await.unwrap();
                            continue;
                        }
                        let response_id = if *attempts == 1 && bytes == b"B" {
                            "mismatched-response-id"
                        } else {
                            request_id
                        };
                        let next = content.len() + usize::from(*attempts == 1 && bytes == b"C");
                        let response = json!({
                            "type":"terminal", "personality_agent_id":PAID, "generation":1, "nonce":"nonce",
                            "request_id":response_id,
                            "result":{"Ok":{"type":"appended","offset":next}},
                        });
                        let mut encoded = serde_json::to_vec(&response).unwrap();
                        encoded.push(b'\n');
                        write.write_all(&encoded).await.unwrap();
                        continue;
                    }
                    "finish_tool_output" => {
                        let response = json!({
                            "type":"terminal", "personality_agent_id":PAID, "generation":1, "nonce":"nonce",
                            "request_id":request_id,
                            "result":{"Ok":{"type":"finished"}},
                        });
                        let mut encoded = serde_json::to_vec(&response).unwrap();
                        encoded.push(b'\n');
                        write.write_all(&encoded).await.unwrap();
                        return content;
                    }
                    other => panic!("unexpected operation: {other}"),
                };
                let response = json!({
                    "type":"terminal", "personality_agent_id":PAID, "generation":1, "nonce":"nonce",
                    "request_id":request_id, "result":response_result,
                });
                let mut encoded = serde_json::to_vec(&response).unwrap();
                encoded.push(b'\n');
                write.write_all(&encoded).await.unwrap();
            }
        });
        let client =
            ArtifactBrokerClient::new(&socket, RpcIdentity::from_wire(PAID, 1, "nonce").unwrap());
        let prefix = vec![b'x'; DEFAULT_MAX_BYTES + 1];
        let mut capture = ShellCapture::new("replay-secret", &client);
        capture.push(&prefix).await.unwrap();
        for suffix in [b"A", b"B", b"C"] {
            capture.push(suffix).await.unwrap();
        }
        let result = capture.finish().await.unwrap();
        assert_eq!(
            result.artifact_handle.as_deref(),
            Some("artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/replay-secret")
        );
        assert_eq!(server.await.unwrap(), [prefix, b"ABC".to_vec()].concat());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn begun_requires_the_exact_canonical_handle() {
        let root = std::env::temp_dir().join(format!("sumi-client-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let socket = root.join("broker.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut read = BufReader::new(read);
            let mut request = String::new();
            read.read_line(&mut request).await.unwrap();
            let request: serde_json::Value = serde_json::from_str(&request).unwrap();
            let response = json!({
                "type":"terminal", "personality_agent_id":PAID, "generation":1, "nonce":"nonce",
                "request_id":request["request_id"],
                "result":{"Ok":{"type":"begun","handle":"artifact://other/tool-output/execution-1","offset":1}}
            });
            let mut bytes = serde_json::to_vec(&response).unwrap();
            bytes.push(b'\n');
            write.write_all(&bytes).await.unwrap();
        });
        let client =
            ArtifactBrokerClient::new(&socket, RpcIdentity::from_wire(PAID, 1, "nonce").unwrap());
        let error = client
            .begin_tool_output("execution-1", b"x")
            .await
            .unwrap_err();
        assert!(
            matches!(error, ToolError::RpcIndeterminate(message) if message.contains("canonical handle"))
        );
        server.await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn put_attachment_chunks_large_content_below_the_rpc_line_limit() {
        let root = std::env::temp_dir().join(format!("sumi-client-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let socket = root.join("broker.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let content = "x".repeat(MAX_RPC_LINE_BYTES + 123);
        let expected = content.as_bytes().to_vec();
        let server = tokio::spawn(async move {
            let mut received: Vec<u8> = Vec::new();
            let mut largest_line = 0usize;
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let (read, mut write) = stream.into_split();
                let mut read = BufReader::new(read);
                let mut line = String::new();
                read.read_line(&mut line).await.unwrap();
                largest_line = largest_line.max(line.len());
                let request: serde_json::Value = serde_json::from_str(&line).unwrap();
                let request_id = request["request_id"].clone();
                let operation = request["operation"]["type"].as_str().unwrap();
                let result = match operation {
                    "begin_attachment" => json!({"Ok":{"type":"attachment_begun","offset":0}}),
                    "append_attachment" => {
                        let offset = request["operation"]["offset"].as_u64().unwrap();
                        let bytes = request["operation"]["content"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|value| value.as_u64().unwrap() as u8)
                            .collect::<Vec<_>>();
                        received.extend(bytes.iter());
                        json!({"Ok":{"type":"attachment_appended","offset":offset + bytes.len() as u64}})
                    }
                    "finish_attachment" => {
                        let response = json!({
                            "type":"terminal", "personality_agent_id":PAID, "generation":1, "nonce":"nonce",
                            "request_id":request_id,
                            "result":{"Ok":{"type":"put","handle":"artifact://0198f0f4-9b72-7000-8000-000000000001/attachments/input-1"}},
                        });
                        let mut encoded = serde_json::to_vec(&response).unwrap();
                        encoded.push(b'\n');
                        write.write_all(&encoded).await.unwrap();
                        return (largest_line, received);
                    }
                    other => panic!("unexpected attachment operation: {other}"),
                };
                let response = json!({
                    "type":"terminal", "personality_agent_id":PAID, "generation":1, "nonce":"nonce",
                    "request_id":request_id, "result":result,
                });
                let mut encoded = serde_json::to_vec(&response).unwrap();
                encoded.push(b'\n');
                write.write_all(&encoded).await.unwrap();
            }
        });
        let client =
            ArtifactBrokerClient::new(&socket, RpcIdentity::from_wire(PAID, 1, "nonce").unwrap());
        assert_eq!(
            client.put_attachment("input-1", &content).await.unwrap(),
            "artifact://0198f0f4-9b72-7000-8000-000000000001/attachments/input-1"
        );
        let (largest_line, received) = server.await.unwrap();
        assert!(largest_line <= MAX_RPC_LINE_BYTES);
        assert_eq!(received, expected);
        let _ = std::fs::remove_dir_all(root);
    }
}
