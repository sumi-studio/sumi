//! Outbound TLS WebSocket connector for the T24 boundary.
//!
//! `WebSocketConnector` implements `GatewayConnector`. Each connection attempt
//! uses a fresh credential and the handshake is authenticated with a short-lived
//! `Authorization: Bearer <token>` header. After the HTTP upgrade, the
//! `WebSocketGateway` performs the agent/API hello exchange and then splits into
//! a `WsGatewayReader` / `WsGatewayWriter` pair for the epoch.

#![allow(dead_code)]

use std::sync::{Arc, Once};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest, http::HeaderValue};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite};

use super::supervisor::{
    AgentHello, ApiHello, ConnectorError, GatewayConnector, GatewayCredential,
};
use super::wire;
use super::{Gateway, GatewayClosed, GatewayReader, GatewayWriter, InboundCommand, OutboundFrame};

static RUSTLS_INIT: Once = Once::new();

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
        RUSTLS_INIT.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });

        let (scheme, _rest) = self
            .url
            .split_once("://")
            .ok_or_else(|| ConnectorError::Other(anyhow!("websocket url is missing a scheme")))?;
        let scheme_lower = scheme.to_ascii_lowercase();
        match scheme_lower.as_str() {
            "wss" => {}
            "ws" if self.allow_insecure => {}
            "ws" => {
                return Err(ConnectorError::Other(anyhow!(
                    "refusing to send bearer credential over insecure ws://"
                )));
            }
            _ => {
                return Err(ConnectorError::Other(anyhow!(
                    "unsupported websocket scheme: {scheme}"
                )));
            }
        }

        let mut request = self
            .url
            .as_str()
            .into_client_request()
            .map_err(|e| ConnectorError::Other(anyhow!("invalid websocket url: {e}")))?;

        let auth_value = format!("Bearer {}", credential.token());
        let header = HeaderValue::from_str(&auth_value)
            .map_err(|e| ConnectorError::Other(anyhow!("invalid authorization header: {e}")))?;
        request.headers_mut().insert("Authorization", header);

        match connect_async(request).await {
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

    async fn authenticate_hello(&mut self, hello: AgentHello) -> Result<ApiHello> {
        let hello_text = serde_json::to_string(&hello).context("serialize agent hello")?;
        self.ws
            .send(Message::Text(hello_text))
            .await
            .context("send agent hello")?;

        let msg = self
            .ws
            .next()
            .await
            .context("websocket closed before hello response")?
            .context("websocket error")?;

        let bytes = match msg {
            Message::Text(s) => s.into_bytes(),
            Message::Binary(b) => b,
            Message::Close(_) => return Err(GatewayClosed.into()),
            _ => bail!("unexpected websocket message during hello"),
        };

        let api_hello: ApiHello = serde_json::from_slice(&bytes).context("parse api hello")?;
        if api_hello.accepted_generation != hello.generation {
            bail!(
                "generation claim mismatch: got {}, expected {}",
                api_hello.accepted_generation,
                hello.generation
            );
        }
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

    use sha2::{Digest, Sha256};

    use super::super::{
        CommandDigestFactory, GatewayConnector, GatewayCredential, IncrementalCommandDigest,
    };
    use super::{WebSocketConnector, decode_command_bytes};

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
            matches!(err, super::super::ConnectorError::Other(ref e) if e.to_string().contains("insecure ws://")),
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
                matches!(err, super::super::ConnectorError::Other(ref e) if e.to_string().contains("insecure ws://")),
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
                msg.contains(expected),
                "for {url}, expected error containing '{expected}', got {err:?}"
            );
        }
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
}
