//! Bounded, generation-fenced client for the artifact broker socket.

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use async_trait::async_trait;
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
    ArtifactOperation, ArtifactResponse, MAX_RPC_LINE_BYTES, RpcError, RpcFrame, RpcIdentity,
    RpcRequest, decode_rpc_frame,
};
use crate::tools::{ToolError, shell_capture::ArtifactAppender};

/// Each exchange uses a fresh socket connection and permits exactly one
/// request and one terminal response. This keeps the client bounded without a
/// pending-request map and makes EOF an unambiguous request failure.
pub struct ArtifactBrokerClient {
    socket: PathBuf,
    identity: RpcIdentity,
    conversation_id: String,
}

impl ArtifactBrokerClient {
    #[cfg(not(test))]
    const EXCHANGE_DEADLINE: Duration = Duration::from_secs(2);
    #[cfg(test)]
    const EXCHANGE_DEADLINE: Duration = Duration::from_millis(100);
    pub fn new(
        socket: impl Into<PathBuf>,
        identity: RpcIdentity,
        conversation_id: impl Into<String>,
    ) -> Self {
        Self {
            socket: socket.into(),
            identity,
            conversation_id: conversation_id.into(),
        }
    }

    pub async fn execute(
        &self,
        operation: ArtifactOperation,
    ) -> Result<ArtifactResponse, ToolError> {
        match timeout(Self::EXCHANGE_DEADLINE, self.execute_inner(operation)).await {
            Ok(result) => result,
            Err(_) => Err(ToolError::RpcIndeterminate(
                "artifact broker exchange deadline elapsed".to_owned(),
            )),
        }
    }

    async fn execute_inner(
        &self,
        operation: ArtifactOperation,
    ) -> Result<ArtifactResponse, ToolError> {
        let request_id = format!("broker-{}", Uuid::now_v7());
        let request = RpcRequest {
            generation: self.identity.generation,
            nonce: self.identity.nonce.clone(),
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
        self.exchange_stream(stream, &encoded, &request_id).await
    }

    async fn exchange_stream<S>(
        &self,
        mut stream: S,
        encoded: &[u8],
        request_id: &str,
    ) -> Result<ArtifactResponse, ToolError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        stream
            .write_all(encoded)
            .await
            .map_err(|error| ToolError::Rpc(format!("artifact broker request failed: {error}")))?;
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
            .map_err(mark_response_loss)?;
        let frame = decode_rpc_frame::<ArtifactResponse>(&line, &self.identity)?;
        let result = match frame {
            RpcFrame::Terminal {
                request_id: response_id,
                result,
                ..
            } if response_id == request_id => result.map_err(map_rpc_error),
            RpcFrame::Terminal { .. } => Err(ToolError::Protocol(
                "artifact response request_id mismatch".to_owned(),
            )),
            RpcFrame::Update { .. } => Err(ToolError::Protocol(
                "artifact broker emitted an unexpected update".to_owned(),
            )),
        }?;
        let mut trailing = [0u8; 1];
        match stream.read(&mut trailing).await {
            Ok(0) => Ok(result),
            Ok(_) => Err(ToolError::Protocol(
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
                conversation_id: self.conversation_id.clone(),
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
                conversation_id: self.conversation_id.clone(),
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
                conversation_id: self.conversation_id.clone(),
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
                            self.conversation_id
                        ) =>
            {
                Ok(handle)
            }
            ArtifactResponse::Begun { .. } => Err(ToolError::Protocol(
                "artifact begin acknowledged the wrong canonical handle or offset".to_owned(),
            )),
            _ => Err(ToolError::Protocol(
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
                conversation_id: self.conversation_id.clone(),
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
            ArtifactResponse::Appended { .. } => Err(ToolError::Protocol(
                "artifact append acknowledged the wrong offset".to_owned(),
            )),
            _ => Err(ToolError::Protocol(
                "artifact append returned the wrong response variant".to_owned(),
            )),
        }
    }

    async fn finish_tool_output(&self, handle: &str) -> Result<(), ToolError> {
        match self
            .execute(ArtifactOperation::FinishToolOutput {
                conversation_id: self.conversation_id.clone(),
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

fn mark_response_loss(error: ToolError) -> ToolError {
    match error {
        ToolError::Rpc(message) | ToolError::Protocol(message)
            if message.contains("closed before a terminal response")
                || message.contains("response read failed") =>
        {
            ToolError::RpcIndeterminate(message)
        }
        error => error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{
        io,
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
                        "type":"terminal", "generation":1, "nonce":"nonce",
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
        let client = ArtifactBrokerClient::new(
            &socket,
            RpcIdentity {
                generation: 1,
                nonce: "nonce".to_owned(),
            },
            "conversation-1",
        );
        let error = client
            .execute(ArtifactOperation::FinishToolOutput {
                conversation_id: "conversation-1".to_owned(),
                handle: "artifact://conversation-1/tool-output/execution-1".to_owned(),
            })
            .await
            .unwrap_err();
        server.abort();
        let _ = std::fs::remove_dir_all(root);
        error
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
        let client = ArtifactBrokerClient::new(
            "/unused",
            RpcIdentity {
                generation: 1,
                nonce: "nonce".to_owned(),
            },
            "conversation-1",
        );
        let written = Arc::new(Mutex::new(Vec::new()));
        let error = client
            .exchange_stream(
                ShutdownFailureStream {
                    written: written.clone(),
                },
                b"complete-request\n",
                "request-1",
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ToolError::RpcIndeterminate(_)));
        assert_eq!(&*written.lock().unwrap(), b"complete-request\n");
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
                "type":"terminal", "generation":1, "nonce":"nonce",
                "request_id":request["request_id"],
                "result":{"Ok":{"type":"begun","handle":"artifact://other/tool-output/execution-1","offset":1}}
            });
            let mut bytes = serde_json::to_vec(&response).unwrap();
            bytes.push(b'\n');
            write.write_all(&bytes).await.unwrap();
        });
        let client = ArtifactBrokerClient::new(
            &socket,
            RpcIdentity {
                generation: 1,
                nonce: "nonce".to_owned(),
            },
            "conversation-1",
        );
        let error = client
            .begin_tool_output("execution-1", b"x")
            .await
            .unwrap_err();
        assert!(
            matches!(error, ToolError::Protocol(message) if message.contains("canonical handle"))
        );
        server.await.unwrap();
        let _ = std::fs::remove_dir_all(root);
    }
}
