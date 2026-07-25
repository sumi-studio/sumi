//! Outbound TLS WebSocket connector for the T24 boundary.
//!
//! `WebSocketConnector` implements `GatewayConnector`. Each connection attempt
//! uses a fresh credential and the handshake is authenticated with a short-lived
//! `Authorization: Bearer <token>` header. After the HTTP upgrade, the
//! `WebSocketGateway` performs the agent/API hello exchange and then splits into
//! a `WsGatewayReader` / `WsGatewayWriter` pair for the epoch.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest, http::HeaderValue};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite};

use super::supervisor::{
    AgentHello, ApiHello, ConnectorError, GatewayConnector, GatewayCredential,
};
use super::wire;
use super::{Gateway, GatewayClosed, GatewayReader, GatewayWriter, InboundCommand, OutboundFrame};

/// Outbound connector for `wss://` (production) or `ws://` (tests).
pub struct WebSocketConnector {
    url: String,
    digest_factory: Arc<dyn crate::gateway::CommandDigestFactory>,
}

impl WebSocketConnector {
    pub fn new(
        url: impl Into<String>,
        digest_factory: Arc<dyn crate::gateway::CommandDigestFactory>,
    ) -> Self {
        Self {
            url: url.into(),
            digest_factory,
        }
    }
}

impl std::fmt::Debug for WebSocketConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketConnector")
            .field("url", &self.url)
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
            Err(tungstenite::Error::Http(response)) if response.status() == 401 => {
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

        let msg = tokio::time::timeout(Duration::from_secs(30), self.ws.next())
            .await
            .context("hello response timeout")?
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
    crate::gateway::stdio::read_command(&mut input, factory).await
}
