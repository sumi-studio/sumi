//! Outbound TLS WebSocket connector for the T24 boundary.
//!
//! `WebSocketConnector` implements `GatewayConnector`. Each connection attempt
//! uses a fresh credential and the handshake is authenticated with a short-lived
//! `Authorization: Bearer <token>` header. After the HTTP upgrade, the
//! `WebSocketGateway` performs the agent/API hello exchange and then splits into
//! a `WsGatewayReader` / `WsGatewayWriter` pair for the epoch.

#![allow(dead_code)]

use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{
    Message, client::IntoClientRequest, http::HeaderValue, protocol::WebSocketConfig,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config, tungstenite};
use zeroize::Zeroizing;

use super::supervisor::{
    AgentHello, ApiHello, ConnectorError, GatewayConnector, GatewayCredential,
};
use super::wire;
use super::{
    Gateway, GatewayClosed, GatewayReader, GatewayWriter, HelloError, InboundCommand,
    MAX_FRAME_BYTES, OutboundFrame,
};

/// Outbound connector for `wss://` (production). `ws://` is only allowed when
/// constructed with [`WebSocketConnector::new_insecure`], which is exposed for
/// test fixtures that spin a local plaintext server.
pub struct WebSocketConnector {
    url: String,
    digest_factory: Arc<dyn crate::gateway::CommandDigestFactory>,
    allow_insecure: bool,
}

impl WebSocketConnector {
    /// Create a production connector that requires `wss://`.
    pub fn new(
        url: impl Into<String>,
        digest_factory: Arc<dyn crate::gateway::CommandDigestFactory>,
    ) -> Self {
        Self {
            url: url.into(),
            digest_factory,
            allow_insecure: false,
        }
    }

    /// Create a test-only connector that permits `ws://`.
    #[cfg(test)]
    pub fn new_insecure(
        url: impl Into<String>,
        digest_factory: Arc<dyn crate::gateway::CommandDigestFactory>,
    ) -> Self {
        Self {
            url: url.into(),
            digest_factory,
            allow_insecure: true,
        }
    }
}

impl std::fmt::Debug for WebSocketConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketConnector")
            .field("url", &self.url)
            .field("allow_insecure", &self.allow_insecure)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl GatewayConnector for WebSocketConnector {
    type Connection = WebSocketGateway;

    async fn connect(
        &mut self,
        credential: GatewayCredential,
    ) -> Result<Self::Connection, ConnectorError> {
        let (scheme, _rest) = self.url.split_once("://").ok_or_else(|| {
            ConnectorError::InvalidConfiguration(anyhow!("websocket url is missing a scheme"))
        })?;
        let scheme_lower = scheme.to_ascii_lowercase();
        match scheme_lower.as_str() {
            "wss" => {}
            "ws" if self.allow_insecure => {}
            "ws" => {
                return Err(ConnectorError::InvalidConfiguration(anyhow!(
                    "refusing to send bearer credential over insecure ws://"
                )));
            }
            _ => {
                return Err(ConnectorError::InvalidConfiguration(anyhow!(
                    "unsupported websocket scheme: {scheme}"
                )));
            }
        }

        let mut request = self.url.as_str().into_client_request().map_err(|e| {
            ConnectorError::InvalidConfiguration(anyhow!("invalid websocket url: {e}"))
        })?;

        let mut auth_value = Zeroizing::new(Vec::with_capacity(7 + credential.token().len()));
        auth_value.extend_from_slice(b"Bearer ");
        auth_value.extend_from_slice(credential.token().as_bytes());
        let header = HeaderValue::from_bytes(&auth_value)
            .map_err(|e| ConnectorError::Other(anyhow!("invalid authorization header: {e}")))?;
        request.headers_mut().insert("Authorization", header);
        drop(auth_value);

        let ws_config = WebSocketConfig {
            max_message_size: Some(MAX_FRAME_BYTES),
            max_frame_size: Some(MAX_FRAME_BYTES),
            ..WebSocketConfig::default()
        };

        match connect_async_with_config(request, Some(ws_config), true).await {
            Ok((ws, _)) => Ok(WebSocketGateway::new(ws, self.digest_factory.clone())),
            Err(tungstenite::Error::Http(response))
                if matches!(response.status().as_u16(), 401 | 403) =>
            {
                Err(ConnectorError::AuthRejected)
            }
            Err(e) => Err(ConnectorError::Other(anyhow!(
                "websocket connect failed: {e}"
            ))),
        }
    }
}

/// A freshly connected WebSocket that has not yet completed the hello exchange.
/// After `authenticate_hello` succeeds, `split` yields the epoch reader/writer.
pub struct WebSocketGateway {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    digest_factory: Arc<dyn crate::gateway::CommandDigestFactory>,
}

impl WebSocketGateway {
    fn new(
        ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
        digest_factory: Arc<dyn crate::gateway::CommandDigestFactory>,
    ) -> Self {
        Self { ws, digest_factory }
    }
}

#[async_trait]
impl Gateway for WebSocketGateway {
    type Reader = WsGatewayReader;
    type Writer = WsGatewayWriter;

    async fn authenticate_hello(
        &mut self,
        hello: AgentHello,
    ) -> std::result::Result<ApiHello, HelloError> {
        let hello_text = serde_json::to_string(&hello).context("serialize agent hello")?;
        self.ws
            .send(Message::Text(hello_text))
            .await
            .context("send agent hello")?;

        let bytes = loop {
            let msg = self
                .ws
                .next()
                .await
                .context("websocket closed before hello response")?
                .context("websocket error")?;

            match msg {
                Message::Text(s) => break s.into_bytes(),
                Message::Binary(b) => break b,
                // A Close before the API has sent its hello is treated as an
                // authentication rejection so the supervisor can refresh the
                // credential and bound the retry loop with max_auth_attempts.
                Message::Close(_) => return Err(HelloError::AuthRejected),
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
            }
        };

        let api_hello: ApiHello = serde_json::from_slice(&bytes).context("parse api hello")?;
        // Generation claim validation is the supervisor's responsibility so it
        // can classify a mismatch as a fatal error instead of a reconnect.
        Ok(api_hello)
    }

    fn split(self) -> (Self::Reader, Self::Writer) {
        let (write, read) = self.ws.split();
        (
            WsGatewayReader {
                read,
                digest_factory: self.digest_factory,
            },
            WsGatewayWriter { write },
        )
    }
}

pub struct WsGatewayReader {
    read: futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    digest_factory: Arc<dyn crate::gateway::CommandDigestFactory>,
}

#[async_trait]
impl GatewayReader for WsGatewayReader {
    async fn next_command(&mut self) -> Result<InboundCommand> {
        loop {
            match self.read.next().await {
                None => return Err(GatewayClosed.into()),
                Some(Err(e)) => return Err(e.into()),
                Some(Ok(Message::Text(s))) => {
                    return decode_command_bytes(s.into_bytes(), self.digest_factory.as_ref())
                        .await;
                }
                Some(Ok(Message::Binary(b))) => {
                    return decode_command_bytes(b, self.digest_factory.as_ref()).await;
                }
                Some(Ok(Message::Close(_))) => return Err(GatewayClosed.into()),
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(Message::Frame(_))) => continue,
            }
        }
    }
}

pub struct WsGatewayWriter {
    write: futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
}

#[async_trait]
impl GatewayWriter for WsGatewayWriter {
    async fn send(&mut self, frame: OutboundFrame) -> Result<()> {
        let wire = wire::to_wire_frame(frame)
            .map_err(|e| anyhow!("frame failed wire contract validation: {e}"))?;
        let text = serde_json::to_string(&wire).context("serialize wire frame")?;
        if text.len() > MAX_FRAME_BYTES {
            bail!(
                "outbound frame exceeds MAX_FRAME_BYTES: {} bytes (limit {})",
                text.len(),
                MAX_FRAME_BYTES
            );
        }
        self.write
            .send(Message::Text(text))
            .await
            .context("send websocket frame")?;
        Ok(())
    }
}

async fn decode_command_bytes(
    bytes: Vec<u8>,
    factory: &dyn crate::gateway::CommandDigestFactory,
) -> Result<InboundCommand> {
    let mut input = BufReader::new(bytes.as_slice());
    let command = crate::gateway::stdio::read_command(&mut input, factory).await?;
    let remaining = input
        .fill_buf()
        .await
        .context("drain websocket command buffer")?;
    if !remaining.is_empty() {
        bail!("websocket message contains trailing bytes");
    }
    Ok(command)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use sha2::{Digest, Sha256};
    use tokio::net::TcpListener;
    use tokio::time::timeout;
    use tokio_tungstenite::{accept_async, accept_hdr_async};

    use super::super::{
        AgentHello, ApiHello, CommandDigestFactory, Envelope, Gateway, GatewayConnector,
        GatewayCredential, GatewayReader, GatewayWriter, HelloError, InboundCommand,
        IncrementalCommandDigest, MAX_FRAME_BYTES, OutboundFrame,
    };
    use super::{WebSocketConnector, decode_command_bytes};
    use crate::gateway::wire::to_wire_frame;
    use crate::runtime::contracts::ProcessGeneration;

    struct TestDigestFactory;

    impl CommandDigestFactory for TestDigestFactory {
        fn start(&self) -> Box<dyn IncrementalCommandDigest> {
            Box::new(TestDigest(Sha256::new()))
        }
    }

    struct TestDigest(Sha256);

    impl IncrementalCommandDigest for TestDigest {
        fn update(&mut self, data: &[u8]) {
            self.0.update(data);
        }

        fn finish(self: Box<Self>) -> crate::gateway::KeyedCommandDigest {
            let hash = self.0.finalize();
            let mut hmac = [0u8; 32];
            hmac.copy_from_slice(&hash[..32.min(hash.len())]);
            crate::gateway::KeyedCommandDigest {
                key_ref: "test".to_owned(),
                hmac,
            }
        }
    }

    #[tokio::test]
    async fn rejects_insecure_ws_without_explicit_flag() {
        let mut connector =
            WebSocketConnector::new("ws://localhost:1234", Arc::new(TestDigestFactory));
        let result = connector.connect(GatewayCredential::new("token")).await;
        let err = result.err().expect("connect must fail");
        assert!(
            matches!(err, super::super::ConnectorError::InvalidConfiguration(ref e) if e.to_string().contains("insecure ws://")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn rejects_case_insensitive_insecure_ws_in_production() {
        for url in [
            "WS://localhost:1234",
            "Ws://localhost:1234",
            "wS://localhost:1234",
        ] {
            let mut connector = WebSocketConnector::new(url, Arc::new(TestDigestFactory));
            let result = connector.connect(GatewayCredential::new("token")).await;
            let err = result.err().expect("connect must fail");
            assert!(
                matches!(err, super::super::ConnectorError::InvalidConfiguration(ref e) if e.to_string().contains("insecure ws://")),
                "for {url}, unexpected error: {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn rejects_missing_or_unsupported_schemes() {
        let cases = [
            ("localhost:1234", "missing"),
            ("http://localhost:1234", "unsupported"),
            ("https://localhost:1234", "unsupported"),
            ("ftp://localhost:1234", "unsupported"),
        ];
        for (url, expected) in cases {
            let mut connector = WebSocketConnector::new(url, Arc::new(TestDigestFactory));
            let result = connector.connect(GatewayCredential::new("token")).await;
            let err = result.err().expect("connect must fail");
            let msg = err.to_string().to_ascii_lowercase();
            assert!(
                matches!(err, super::super::ConnectorError::InvalidConfiguration(_)),
                "for {url}, static URL failure must be fatal configuration: {err:?}"
            );
            assert!(
                msg.contains(expected),
                "for {url}, expected error containing '{expected}', got {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn rejects_malformed_websocket_url_as_fatal_configuration() {
        let mut connector = WebSocketConnector::new("wss://[::1", Arc::new(TestDigestFactory));
        let err = connector
            .connect(GatewayCredential::new("token"))
            .await
            .err()
            .expect("malformed URL must fail before connection attempts");
        assert!(
            matches!(err, super::super::ConnectorError::InvalidConfiguration(_)),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn decode_command_bytes_rejects_multiple_frames_in_one_message() {
        let bytes = b"{\"seq\":1,\"command_id\":\"00000000-0000-4000-8000-000000000001\",\"command\":{\"type\":\"abort\"}}\n{\"seq\":2,\"command_id\":\"00000000-0000-4000-8000-000000000002\",\"command\":{\"type\":\"abort\"}}\n".to_vec();
        let result = decode_command_bytes(bytes, &TestDigestFactory).await;
        assert!(
            result.is_err() && format!("{:?}", result).contains("trailing bytes"),
            "expected trailing bytes rejection, got {result:?}"
        );
    }

    // M5 gate 10: real local mock WebSocket server integration tests.

    fn test_agent_hello() -> AgentHello {
        AgentHello {
            agent_id: "test-agent".to_owned(),
            generation: ProcessGeneration::from_wire(7).unwrap(),
            last_sent_event_seq: 1,
            last_received_command_seq: 0,
            last_applied_command_seq: 1,
        }
    }

    fn test_api_hello() -> ApiHello {
        ApiHello {
            accepted_generation: ProcessGeneration::from_wire(7).unwrap(),
            last_received_event_seq: 1,
            next_command_seq: 2,
        }
    }

    fn test_event_frame() -> OutboundFrame {
        OutboundFrame::Event {
            envelope: Envelope {
                seq: Some(1),
                conversation_id: "conversation-1".to_owned(),
                event: serde_json::json!({"type": "agent_start"}),
            },
        }
    }

    fn test_command_message() -> tokio_tungstenite::tungstenite::Message {
        tokio_tungstenite::tungstenite::Message::Text(
            r#"{"seq":2,"command_id":"00000000-0000-4000-8000-000000000002","command":{"type":"abort"}}"#.to_owned(),
        )
    }

    async fn read_agent_hello<R>(ws: &mut tokio_tungstenite::WebSocketStream<R>)
    where
        R: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let msg = ws.next().await.unwrap().unwrap();
        let text = match msg {
            tokio_tungstenite::tungstenite::Message::Text(s) => s,
            tokio_tungstenite::tungstenite::Message::Binary(b) => String::from_utf8(b).unwrap(),
            other => panic!("unexpected server message: {other:?}"),
        };
        let _: AgentHello = serde_json::from_str(&text).unwrap();
    }

    async fn send_api_hello<R>(ws: &mut tokio_tungstenite::WebSocketStream<R>, api: ApiHello)
    where
        R: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let text = serde_json::to_string(&api).unwrap();
        ws.send(tokio_tungstenite::tungstenite::Message::Text(text))
            .await
            .unwrap();
    }

    fn listener_addr() -> std::net::SocketAddr {
        use std::net::{Ipv4Addr, SocketAddrV4};
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into()
    }

    struct AuthCallback {
        expected: &'static str,
    }

    impl tokio_tungstenite::tungstenite::handshake::server::Callback for AuthCallback {
        fn on_request(
            self,
            request: &tokio_tungstenite::tungstenite::http::Request<()>,
            response: tokio_tungstenite::tungstenite::http::Response<()>,
        ) -> Result<
            tokio_tungstenite::tungstenite::http::Response<()>,
            tokio_tungstenite::tungstenite::http::Response<Option<String>>,
        > {
            let auth = request
                .headers()
                .get("Authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if auth == self.expected {
                Ok(response)
            } else {
                Err(tokio_tungstenite::tungstenite::http::Response::builder()
                    .status(401)
                    .body(Some("Unauthorized".to_owned()))
                    .unwrap())
            }
        }
    }

    #[tokio::test]
    async fn reader_eof_returns_gateway_closed() {
        let listener = TcpListener::bind(listener_addr()).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            read_agent_hello(&mut ws).await;
            send_api_hello(&mut ws, test_api_hello()).await;
            // Server sends a graceful Close, then drops the connection.
            ws.send(tokio_tungstenite::tungstenite::Message::Close(None))
                .await
                .unwrap();
        });

        let mut connector =
            WebSocketConnector::new_insecure(format!("ws://{addr}"), Arc::new(TestDigestFactory));
        let mut gateway = connector
            .connect(GatewayCredential::new("valid"))
            .await
            .unwrap();
        let api_hello = gateway
            .authenticate_hello(test_agent_hello())
            .await
            .unwrap();
        assert_eq!(
            api_hello.accepted_generation,
            test_api_hello().accepted_generation
        );

        let (mut reader, _writer) = gateway.split();
        let result = reader.next_command().await;
        assert!(
            result.is_err(),
            "reader must report EOF/closure: {result:?}"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("gateway input closed")
        );

        let _ = server.await;
    }

    #[tokio::test]
    async fn expired_bearer_token_rejected_with_auth_rejected() {
        let listener = TcpListener::bind(listener_addr()).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let callback = AuthCallback {
                expected: "Bearer valid",
            };
            // Reject non-matching Authorization header with HTTP 401.
            let _ = accept_hdr_async(stream, callback).await;
        });

        let mut connector =
            WebSocketConnector::new_insecure(format!("ws://{addr}"), Arc::new(TestDigestFactory));
        let err = connector
            .connect(GatewayCredential::new("expired"))
            .await
            .err()
            .expect("connect must fail");
        assert!(
            matches!(err, super::super::ConnectorError::AuthRejected),
            "expired token must be rejected as AuthRejected: {err:?}"
        );

        let _ = server.await;
    }

    #[tokio::test]
    async fn hello_response_timeout_is_detectable_by_caller() {
        let listener = TcpListener::bind(listener_addr()).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            // Read the agent hello but never send the API hello.
            read_agent_hello(&mut ws).await;
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let mut connector =
            WebSocketConnector::new_insecure(format!("ws://{addr}"), Arc::new(TestDigestFactory));
        let mut gateway = connector
            .connect(GatewayCredential::new("valid"))
            .await
            .unwrap();
        let result = timeout(
            Duration::from_millis(50),
            gateway.authenticate_hello(test_agent_hello()),
        )
        .await;
        assert!(
            result.is_err(),
            "authenticate_hello must be cancellable/timeoutable when server stalls"
        );

        server.abort();
    }

    #[tokio::test]
    async fn writer_send_delivers_to_server() {
        let listener = TcpListener::bind(listener_addr()).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            read_agent_hello(&mut ws).await;
            send_api_hello(&mut ws, test_api_hello()).await;
            let msg = ws.next().await.unwrap().unwrap();
            let text = match msg {
                tokio_tungstenite::tungstenite::Message::Text(s) => s,
                other => panic!("expected text frame, got {other:?}"),
            };
            let envelope: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(envelope["frame_type"], "event");
            assert_eq!(envelope["envelope"]["seq"], 1);
        });

        let mut connector =
            WebSocketConnector::new_insecure(format!("ws://{addr}"), Arc::new(TestDigestFactory));
        let mut gateway = connector
            .connect(GatewayCredential::new("valid"))
            .await
            .unwrap();
        let _ = gateway
            .authenticate_hello(test_agent_hello())
            .await
            .unwrap();
        let (_reader, mut writer) = gateway.split();
        writer.send(test_event_frame()).await.unwrap();

        let _ = server.await;
    }

    #[tokio::test]
    async fn writer_send_fails_when_server_closes() {
        let listener = TcpListener::bind(listener_addr()).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            read_agent_hello(&mut ws).await;
            send_api_hello(&mut ws, test_api_hello()).await;
            // Send a graceful Close so the client reader observes the EOF.
            tokio::time::sleep(Duration::from_millis(10)).await;
            ws.send(tokio_tungstenite::tungstenite::Message::Close(None))
                .await
                .unwrap();
            // Keep the socket open briefly so the close frame is delivered.
            tokio::time::sleep(Duration::from_millis(50)).await;
        });

        let mut connector =
            WebSocketConnector::new_insecure(format!("ws://{addr}"), Arc::new(TestDigestFactory));
        let mut gateway = connector
            .connect(GatewayCredential::new("valid"))
            .await
            .unwrap();
        let _ = gateway
            .authenticate_hello(test_agent_hello())
            .await
            .unwrap();
        let (mut reader, mut writer) = gateway.split();

        // Reader observes the close first; writer should then refuse to send.
        let read_result = reader.next_command().await;
        assert!(
            read_result.is_err(),
            "reader must observe close: {read_result:?}"
        );

        let result = writer.send(test_event_frame()).await;
        assert!(
            result.is_err(),
            "writer must fail after peer closes: {result:?}"
        );

        let _ = server.await;
    }

    #[tokio::test]
    async fn reconnect_succeeds_after_server_close() {
        let listener = TcpListener::bind(listener_addr()).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            // First connection: handshake, read a sent frame, then close.
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            read_agent_hello(&mut ws).await;
            send_api_hello(&mut ws, test_api_hello()).await;
            let msg = ws.next().await.unwrap().unwrap();
            assert!(matches!(
                msg,
                tokio_tungstenite::tungstenite::Message::Text(_)
            ));
            let _ = ws.close(None).await;

            // Second connection: handshake, send a command (API restart/reconnect).
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            read_agent_hello(&mut ws).await;
            send_api_hello(&mut ws, test_api_hello()).await;
            ws.send(test_command_message()).await.unwrap();
            // Keep connection alive until client reads.
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        let mut connector =
            WebSocketConnector::new_insecure(format!("ws://{addr}"), Arc::new(TestDigestFactory));

        // First epoch.
        let mut gateway = connector
            .connect(GatewayCredential::new("valid"))
            .await
            .unwrap();
        let _ = gateway
            .authenticate_hello(test_agent_hello())
            .await
            .unwrap();
        let (mut reader, mut writer) = gateway.split();
        writer.send(test_event_frame()).await.unwrap();
        let result = reader.next_command().await;
        assert!(
            result.is_err(),
            "first reader must EOF when server closes: {result:?}"
        );

        // Reconnect to the same listener (API restart).
        let mut gateway = connector
            .connect(GatewayCredential::new("valid"))
            .await
            .unwrap();
        let _ = gateway
            .authenticate_hello(test_agent_hello())
            .await
            .unwrap();
        let (mut reader, _writer) = gateway.split();
        let cmd = reader.next_command().await.unwrap();
        assert!(matches!(cmd, InboundCommand::Valid(_)));

        let _ = server.await;
    }

    #[tokio::test]
    async fn authenticate_hello_ignores_ping_and_pong_before_api_hello() {
        let listener = TcpListener::bind(listener_addr()).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            read_agent_hello(&mut ws).await;
            ws.send(tokio_tungstenite::tungstenite::Message::Ping(vec![]))
                .await
                .unwrap();
            ws.send(tokio_tungstenite::tungstenite::Message::Pong(vec![]))
                .await
                .unwrap();
            send_api_hello(&mut ws, test_api_hello()).await;
        });

        let mut connector =
            WebSocketConnector::new_insecure(format!("ws://{addr}"), Arc::new(TestDigestFactory));
        let mut gateway = connector
            .connect(GatewayCredential::new("valid"))
            .await
            .unwrap();
        let api_hello = timeout(
            Duration::from_millis(200),
            gateway.authenticate_hello(test_agent_hello()),
        )
        .await
        .expect("authenticate_hello should not hang on ping/pong")
        .expect("authenticate_hello should succeed after ping/pong");
        assert_eq!(
            api_hello.accepted_generation,
            test_api_hello().accepted_generation
        );

        let _ = server.await;
    }

    #[tokio::test]
    async fn authenticate_hello_rejects_auth_on_close_before_hello() {
        let listener = TcpListener::bind(listener_addr()).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            read_agent_hello(&mut ws).await;
            ws.send(tokio_tungstenite::tungstenite::Message::Close(None))
                .await
                .unwrap();
        });

        let mut connector =
            WebSocketConnector::new_insecure(format!("ws://{addr}"), Arc::new(TestDigestFactory));
        let mut gateway = connector
            .connect(GatewayCredential::new("valid"))
            .await
            .unwrap();
        let result = gateway.authenticate_hello(test_agent_hello()).await;
        assert!(
            matches!(result, Err(HelloError::AuthRejected)),
            "authenticate_hello must classify Close before hello as AuthRejected, got {result:?}"
        );

        let _ = server.await;
    }

    #[tokio::test]
    async fn reader_rejects_message_exceeding_max_frame_bytes() {
        let listener = TcpListener::bind(listener_addr()).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            read_agent_hello(&mut ws).await;
            send_api_hello(&mut ws, test_api_hello()).await;

            // Tell the client the oversized frame is about to be sent, then send
            // it. The send may block once the client stops reading after the
            // capacity error, so the test aborts the server task rather than
            // waiting for the send to complete.
            let _ = ready_tx.send(());
            let oversized = vec![0u8; MAX_FRAME_BYTES + 1];
            let _ = ws
                .send(tokio_tungstenite::tungstenite::Message::Binary(oversized))
                .await;
        });

        let mut connector =
            WebSocketConnector::new_insecure(format!("ws://{addr}"), Arc::new(TestDigestFactory));
        let mut gateway = connector
            .connect(GatewayCredential::new("valid"))
            .await
            .unwrap();
        let _ = gateway
            .authenticate_hello(test_agent_hello())
            .await
            .unwrap();
        let (mut reader, _writer) = gateway.split();

        ready_rx.await.unwrap();
        let result = timeout(Duration::from_secs(2), reader.next_command()).await;
        assert!(
            result.is_ok(),
            "reader must not hang waiting for oversized message"
        );
        let result = result.unwrap();
        assert!(
            result.is_err(),
            "reader must reject oversized websocket message: {result:?}"
        );
        let err = result.unwrap_err().to_string();
        assert!(err.contains("Message too long"), "unexpected error: {err}");

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn connect_sends_expected_authorization_header() {
        let listener = TcpListener::bind(listener_addr()).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let callback = AuthCallback {
                expected: "Bearer test-token",
            };
            // accept_hdr_async checks the Authorization header during the
            // handshake and returns an error if it does not match.
            let _ = accept_hdr_async(stream, callback).await;
        });

        let mut connector =
            WebSocketConnector::new_insecure(format!("ws://{addr}"), Arc::new(TestDigestFactory));
        let result = connector
            .connect(GatewayCredential::new("test-token"))
            .await;
        assert!(
            result.is_ok(),
            "connector must present the expected bearer token"
        );

        let _ = server.await;
    }

    #[test]
    fn gateway_credential_debug_redacts_token() {
        let cred = GatewayCredential::new("super-secret-token");
        let debug = format!("{cred:?}");
        assert!(
            !debug.contains("super-secret-token"),
            "token must not leak in Debug output"
        );
        assert!(
            debug.contains("[REDACTED]"),
            "Debug output must redact token"
        );
    }

    #[tokio::test]
    async fn invalid_authorization_header_rejected_without_exposing_secret() {
        // A control character in the token makes the Authorization header
        // invalid. connect must reject it locally before any network I/O, and
        // the error message must not contain the secret.
        let secret_with_control = "Bearer test-token\nwith-secret";
        let mut connector = WebSocketConnector::new_insecure(
            "ws://localhost:0".to_owned(),
            Arc::new(TestDigestFactory),
        );
        let result = connector
            .connect(GatewayCredential::new(secret_with_control))
            .await;
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("invalid Authorization bytes must be rejected"),
        }
        .to_string();
        assert!(
            err.contains("invalid authorization header"),
            "unexpected error: {err}"
        );
        assert!(
            !err.contains(secret_with_control),
            "error must not echo the secret"
        );
    }

    fn retry_frame_with_error_len(error_message_len: usize) -> OutboundFrame {
        OutboundFrame::Event {
            envelope: Envelope {
                seq: Some(1),
                conversation_id: "c".to_owned(),
                event: serde_json::json!({
                    "type": "retry_scheduled",
                    "attempt": 1,
                    "delay_ms": 0,
                    "retry_at": "1970-01-01T00:00:00+00:00",
                    "error_message": "x".repeat(error_message_len),
                }),
            },
        }
    }

    #[tokio::test]
    async fn writer_accepts_exactly_max_frame_bytes() {
        let empty = retry_frame_with_error_len(0);
        let wire = to_wire_frame(empty).unwrap();
        let base_text = serde_json::to_string(&wire).unwrap();
        let base_len = base_text.len();
        assert!(
            base_len <= MAX_FRAME_BYTES,
            "test fixture fits within limit"
        );

        let payload_len = MAX_FRAME_BYTES - base_len;
        let frame = retry_frame_with_error_len(payload_len);
        let wire = to_wire_frame(frame.clone()).unwrap();
        let text = serde_json::to_string(&wire).unwrap();
        assert_eq!(
            text.len(),
            MAX_FRAME_BYTES,
            "fixture must serialize to exactly MAX_FRAME_BYTES"
        );

        let listener = TcpListener::bind(listener_addr()).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            read_agent_hello(&mut ws).await;
            send_api_hello(&mut ws, test_api_hello()).await;
            let msg = ws.next().await.unwrap().unwrap();
            assert!(matches!(
                msg,
                tokio_tungstenite::tungstenite::Message::Text(_)
            ));
        });

        let mut connector =
            WebSocketConnector::new_insecure(format!("ws://{addr}"), Arc::new(TestDigestFactory));
        let mut gateway = connector
            .connect(GatewayCredential::new("valid"))
            .await
            .unwrap();
        let _ = gateway
            .authenticate_hello(test_agent_hello())
            .await
            .unwrap();
        let (_reader, mut writer) = gateway.split();
        writer.send(frame).await.unwrap();

        let _ = server.await;
    }

    #[tokio::test]
    async fn writer_rejects_oversized_frame() {
        let empty = retry_frame_with_error_len(0);
        let wire = to_wire_frame(empty).unwrap();
        let base_text = serde_json::to_string(&wire).unwrap();
        let base_len = base_text.len();

        let payload_len = MAX_FRAME_BYTES - base_len + 1;
        let oversized = retry_frame_with_error_len(payload_len);

        let listener = TcpListener::bind(listener_addr()).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            read_agent_hello(&mut ws).await;
            send_api_hello(&mut ws, test_api_hello()).await;
            // Give the client time to attempt the oversized send; no frame
            // should actually be delivered because the local check rejects it.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = ws.close(None).await;
        });

        let mut connector =
            WebSocketConnector::new_insecure(format!("ws://{addr}"), Arc::new(TestDigestFactory));
        let mut gateway = connector
            .connect(GatewayCredential::new("valid"))
            .await
            .unwrap();
        let _ = gateway
            .authenticate_hello(test_agent_hello())
            .await
            .unwrap();
        let (_reader, mut writer) = gateway.split();
        let result = writer.send(oversized).await;
        assert!(
            result.is_err(),
            "oversized frame must be rejected before sending: {result:?}"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("outbound frame exceeds MAX_FRAME_BYTES"),
            "oversized rejection must mention the size limit"
        );

        let _ = server.await;
    }

    #[tokio::test]
    async fn wss_connect_attempt_does_not_panic_without_server_tls() {
        // A wss:// connect must be able to construct a rustls ClientConfig
        // (and thus select/install a crypto provider) before the TLS handshake.
        // The local server accepts the TCP connection and immediately closes it,
        // so the handshake fails; the test ensures this failure is an error,
        // not a panic due to a missing CryptoProvider.
        let listener = TcpListener::bind(listener_addr()).await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream);
        });

        let mut connector =
            WebSocketConnector::new(format!("wss://{addr}"), Arc::new(TestDigestFactory));
        let result = connector.connect(GatewayCredential::new("valid")).await;
        assert!(result.is_err(), "expected TLS/handshake error");

        let _ = server.await;
    }
}
