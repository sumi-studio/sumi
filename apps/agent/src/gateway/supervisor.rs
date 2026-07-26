//! ConnectionSupervisor and the gateway lifecycle boundary.
//!
//! This module owns reconnect, re-auth, epoch mapping, bidirectional catch-up,
//! and the `DeliveryEpoch` boundary between the transport and `DeliveryPump`.
//! T17 store integration is represented by compile-safe adapter traits;
//! concrete T17 methods are wired through the `T17StoreAdapter` seam.

#![allow(dead_code)]

use std::fmt;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use futures_util::future::{Either, select};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroize;

use super::wire::MAX_JSON_SAFE_INTEGER;
use crate::runtime::contracts::ProcessGeneration;

use super::{Gateway, GatewayReader, GatewayWriter, InboundCommand, OutboundFrame};

pub mod seams;

// T24-local identity: one `DeliveryEpoch` is minted for each `ConnectionEpoch`
// and invalidated exactly once when the epoch ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ConnectionEpoch(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DeliveryEpoch(u64);

impl DeliveryEpoch {
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl ConnectionEpoch {
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// Short-lived bearer token obtained by `CredentialProvider` for every connect
/// attempt. The token is zeroized on drop and redacted in `Debug`.
#[derive(Clone)]
pub struct GatewayCredential {
    token: String,
}

impl GatewayCredential {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }

    pub fn token(&self) -> &str {
        &self.token
    }
}

impl fmt::Debug for GatewayCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayCredential")
            .field("token", &"[REDACTED]")
            .finish()
    }
}

impl Drop for GatewayCredential {
    fn drop(&mut self) {
        self.token.zeroize();
    }
}

/// Agent → API hello. `generation` is the `ProcessGeneration` bound to the
/// credential claim. All seq values are `u64` and are validated against the
/// durable source before the epoch proceeds.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentHello {
    pub agent_id: String,
    pub generation: u64,
    pub last_sent_event_seq: u64,
    pub last_received_command_seq: u64,
    pub last_applied_command_seq: u64,
}

/// API → Agent hello response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiHello {
    pub accepted_generation: u64,
    pub last_received_event_seq: u64,
    pub next_command_seq: u64,
}

/// Cursors returned by the durable command source.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommandCursors {
    pub received: u64,
    pub applied: u64,
}

/// Cursors returned by the durable event source.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventCursors {
    pub last_sent: u64,
}

/// Latched hydration state for the current `ProcessGeneration`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HydrationState {
    NotReady,
    Ready(HydrationReady),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HydrationReady {
    pub generation: u64,
    pub receipt_identity: String,
}

/// Fresh token for every connect attempt.
#[async_trait]
pub trait CredentialProvider: Send + 'static {
    async fn fresh_credential(&mut self) -> Result<GatewayCredential>;
}

/// Connector returns an established `Gateway` that still needs the hello dance.
#[async_trait]
pub trait GatewayConnector: Send + 'static {
    type Connection: Gateway;
    async fn connect(
        &mut self,
        credential: GatewayCredential,
    ) -> Result<Self::Connection, ConnectorError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    #[error("authentication rejected")]
    AuthRejected,
    #[error("fatal: {0}")]
    Fatal(anyhow::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Narrow durable cursor/source boundary for T24. T17 implements this seam;
/// `T17StoreAdapter` below is a compile-safe placeholder that reports the
/// exact integration contract without duplicating store state.
#[async_trait]
pub trait DurableSource: Clone + Send + Sync + 'static {
    async fn event_cursor(&self) -> Result<EventCursors>;
    async fn events_after(&self, after_seq: u64, limit: usize) -> Result<Vec<OutboundFrame>>;
    async fn command_cursors(&self) -> Result<CommandCursors>;
}

/// `HydrationReady` is a per-generation latched state. T17 will drive the
/// underlying `watch` channel; `T17HydrationLatch` is a compile-safe seam.
#[async_trait]
pub trait HydrationLatch: Clone + Send + Sync + 'static {
    async fn wait_for(&self, generation: u64) -> Result<HydrationReady>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SupervisorConfig {
    pub agent_id: String,
    pub generation: u64,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub send_timeout: Duration,
    pub event_buffer_size: usize,
    pub command_buffer_size: usize,
    pub catch_up_page_size: usize,
    pub max_reconnect_attempts: Option<u32>,
    pub hello_timeout: Duration,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            agent_id: String::new(),
            generation: 0,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
            send_timeout: Duration::from_secs(30),
            event_buffer_size: 64,
            command_buffer_size: 64,
            catch_up_page_size: 64,
            max_reconnect_attempts: None,
            hello_timeout: Duration::from_secs(30),
        }
    }
}

/// Handle to a spawned `ConnectionSupervisor`.
pub struct SupervisorHandle {
    pub commands: mpsc::Receiver<InboundCommand>,
    pub events: mpsc::Sender<(DeliveryEpoch, OutboundFrame)>,
    pub epochs: watch::Receiver<Option<DeliveryEpoch>>,
    task: JoinHandle<Result<()>>,
}

impl SupervisorHandle {
    pub fn abort(&self) {
        self.task.abort();
    }

    pub async fn join(self) -> Result<()> {
        self.task.await?
    }
}

type CurrentWriterSlot =
    Arc<std::sync::Mutex<Option<(DeliveryEpoch, mpsc::Sender<OutboundFrame>)>>>;

/// Owns the connect/authenticate/hello/catch-up/reconnect loop.
pub struct ConnectionSupervisor<C, P, S, L>
where
    C: GatewayConnector,
    P: CredentialProvider,
    S: DurableSource,
    L: HydrationLatch,
{
    connector: C,
    credentials: P,
    source: S,
    latch: L,
    config: SupervisorConfig,
    epoch_counter: Arc<AtomicU64>,
    current_writer: CurrentWriterSlot,
    current_epoch: watch::Sender<Option<DeliveryEpoch>>,
}

impl<C, P, S, L> ConnectionSupervisor<C, P, S, L>
where
    C: GatewayConnector,
    P: CredentialProvider,
    S: DurableSource,
    L: HydrationLatch,
{
    pub fn new(
        connector: C,
        credentials: P,
        source: S,
        latch: L,
        config: SupervisorConfig,
    ) -> Self {
        let (current_epoch, _) = watch::channel(None);
        Self {
            connector,
            credentials,
            source,
            latch,
            config,
            epoch_counter: Arc::new(AtomicU64::new(0)),
            current_writer: Arc::new(std::sync::Mutex::new(None)),
            current_epoch,
        }
    }

    /// Start the supervisor and return channels for commands, events, and epoch
    /// observation. `events` must carry the current `DeliveryEpoch` from
    /// `handle.epochs`.
    pub fn start(self) -> SupervisorHandle {
        let (commands_tx, commands_rx) = mpsc::channel(self.config.command_buffer_size);
        let (events_tx, events_rx) = mpsc::channel(self.config.event_buffer_size);
        let epochs_rx = self.current_epoch.subscribe();
        let task = tokio::spawn(self.run(commands_tx, events_rx));
        SupervisorHandle {
            commands: commands_rx,
            events: events_tx,
            epochs: epochs_rx,
            task,
        }
    }

    pub async fn run(
        mut self,
        commands_tx: mpsc::Sender<InboundCommand>,
        events_rx: mpsc::Receiver<(DeliveryEpoch, OutboundFrame)>,
    ) -> Result<()> {
        let current_writer = self.current_writer.clone();
        let forwarder = tokio::spawn(event_forwarder(events_rx, current_writer));

        let result = self.run_loop(commands_tx).await;

        forwarder.abort();
        let _ = forwarder.await;
        result
    }

    async fn run_loop(&mut self, commands_tx: mpsc::Sender<InboundCommand>) -> Result<()> {
        const DEFAULT_MAX_AUTH_ATTEMPTS: u32 = 3;

        let mut attempt: u32 = 0;
        loop {
            match self.connect_and_run_epoch(commands_tx.clone()).await {
                Ok(()) => continue,
                Err(SupervisorError::Fatal(e)) => return Err(e),
                Err(SupervisorError::AuthRejected) => {
                    attempt = attempt.saturating_add(1);
                    let max = self
                        .config
                        .max_reconnect_attempts
                        .unwrap_or(DEFAULT_MAX_AUTH_ATTEMPTS);
                    if attempt > max {
                        return Err(anyhow!("max auth attempts exceeded"));
                    }
                    Self::backoff_sleep(&self.config, attempt).await?;
                }
                Err(SupervisorError::Reconnect { reason }) => {
                    attempt = attempt.saturating_add(1);
                    if let Some(max) = self.config.max_reconnect_attempts
                        && attempt > max
                    {
                        return Err(anyhow!("max reconnect attempts exceeded: {reason}"));
                    }
                    Self::backoff_sleep(&self.config, attempt).await?;
                }
            }
        }
    }

    async fn connect_and_run_epoch(
        &mut self,
        commands_tx: mpsc::Sender<InboundCommand>,
    ) -> Result<(), SupervisorError> {
        let source = self.source.clone();
        let config = self.config.clone();

        let credential =
            self.credentials
                .fresh_credential()
                .await
                .map_err(|e| SupervisorError::Reconnect {
                    reason: format!("failed to obtain credential: {e}"),
                })?;

        let mut gateway = match self.connector.connect(credential).await {
            Ok(g) => g,
            Err(ConnectorError::AuthRejected) => return Err(SupervisorError::AuthRejected),
            Err(ConnectorError::Fatal(e)) => return Err(SupervisorError::Fatal(e)),
            Err(ConnectorError::Other(e)) => {
                return Err(SupervisorError::Reconnect {
                    reason: format!("connect failed: {e}"),
                });
            }
        };

        let agent_hello = build_agent_hello(&source, &config).await?;
        let api_hello = tokio::time::timeout(
            config.hello_timeout,
            gateway.authenticate_hello(agent_hello.clone()),
        )
        .await
        .map_err(|_| SupervisorError::Reconnect {
            reason: "hello response timeout".to_owned(),
        })?
        .map_err(|e| SupervisorError::Reconnect {
            reason: format!("hello failed: {e}"),
        })?;

        validate_hello(&source, &agent_hello, &api_hello).await?;

        let (connection_epoch, delivery_epoch) = self.next_epoch();
        let (reader, writer) = gateway.split();

        let token = CancellationToken::new();
        let (writer_tx, writer_rx) = mpsc::channel(config.event_buffer_size);

        *self.current_writer.lock().unwrap() = Some((delivery_epoch, writer_tx));
        self.current_epoch.send_replace(Some(delivery_epoch));

        let reader_handle = tokio::spawn(reader_task(
            reader,
            commands_tx,
            self.latch.clone(),
            api_hello.clone(),
            token.child_token(),
        ));

        let writer_handle = tokio::spawn(writer_task(
            writer,
            writer_rx,
            source,
            api_hello,
            config,
            token.child_token(),
        ));

        let result = select(reader_handle, writer_handle).await;
        token.cancel();
        let (first, second) = match result {
            Either::Left((reader_result, writer_handle)) => (reader_result, writer_handle.await),
            Either::Right((writer_result, reader_handle)) => (writer_result, reader_handle.await),
        };

        *self.current_writer.lock().unwrap() = None;
        self.current_epoch.send_replace(None);

        match (&first, &second) {
            (Ok(Err(e)), _) | (_, Ok(Err(e))) => {
                tracing::debug!(
                    connection_epoch = connection_epoch.as_u64(),
                    delivery_epoch = delivery_epoch.as_u64(),
                    error = ?e,
                    "epoch ended with task error"
                );
            }
            (Err(e), _) | (_, Err(e)) => {
                tracing::debug!(
                    connection_epoch = connection_epoch.as_u64(),
                    delivery_epoch = delivery_epoch.as_u64(),
                    error = ?e,
                    "epoch ended with task panic"
                );
            }
            _ => {
                tracing::debug!(
                    connection_epoch = connection_epoch.as_u64(),
                    delivery_epoch = delivery_epoch.as_u64(),
                    "epoch ended cleanly"
                );
            }
        }

        match (first, second) {
            (Err(e), _) => Err(SupervisorError::Fatal(anyhow!("epoch task panicked: {e}"))),
            (_, Err(e)) => Err(SupervisorError::Fatal(anyhow!(
                "epoch sibling task panicked: {e}"
            ))),
            (Ok(Err(e)), _) | (_, Ok(Err(e))) => Err(classify_epoch_error(e)),
            _ => Err(SupervisorError::Reconnect {
                reason: "epoch task ended unexpectedly".to_owned(),
            }),
        }
    }

    fn next_epoch(&self) -> (ConnectionEpoch, DeliveryEpoch) {
        let n = self.epoch_counter.fetch_add(1, Ordering::SeqCst);
        (ConnectionEpoch(n), DeliveryEpoch(n))
    }

    async fn backoff_sleep(config: &SupervisorConfig, attempt: u32) -> Result<()> {
        let base_ms = config.initial_backoff.as_millis() as u64;
        let max_ms = config.max_backoff.as_millis() as u64;
        let shift = attempt.saturating_sub(1).min(31);
        let delay_ms = base_ms
            .saturating_mul(2u64.saturating_pow(shift))
            .min(max_ms);
        let jitter = if delay_ms == 0 {
            0
        } else {
            rand::rng().random_range(1..=delay_ms)
        };
        time::sleep(Duration::from_millis(jitter)).await;
        Ok(())
    }
}

async fn build_agent_hello<S: DurableSource>(
    source: &S,
    config: &SupervisorConfig,
) -> Result<AgentHello, SupervisorError> {
    ProcessGeneration::from_wire(config.generation).map_err(|e| {
        SupervisorError::Fatal(anyhow!("invalid configured ProcessGeneration: {e}"))
    })?;
    let event_cursor = source
        .event_cursor()
        .await
        .map_err(|e| SupervisorError::Reconnect {
            reason: format!("event cursor unavailable: {e}"),
        })?;
    let command_cursor =
        source
            .command_cursors()
            .await
            .map_err(|e| SupervisorError::Reconnect {
                reason: format!("command cursor unavailable: {e}"),
            })?;
    for (name, cursor) in [
        ("last_sent_event_seq", event_cursor.last_sent),
        ("last_received_command_seq", command_cursor.received),
        ("last_applied_command_seq", command_cursor.applied),
    ] {
        if cursor > MAX_JSON_SAFE_INTEGER {
            return Err(SupervisorError::Fatal(anyhow!(
                "{name} exceeds JSON-safe integer range"
            )));
        }
    }
    Ok(AgentHello {
        agent_id: config.agent_id.clone(),
        generation: config.generation,
        last_sent_event_seq: event_cursor.last_sent,
        last_received_command_seq: command_cursor.received,
        last_applied_command_seq: command_cursor.applied,
    })
}

async fn validate_hello<S: DurableSource>(
    source: &S,
    agent: &AgentHello,
    api: &ApiHello,
) -> Result<(), SupervisorError> {
    ProcessGeneration::from_wire(agent.generation)
        .and_then(|_| ProcessGeneration::from_wire(api.accepted_generation))
        .map_err(|e| SupervisorError::Fatal(anyhow!("invalid ProcessGeneration in hello: {e}")))?;
    for (name, cursor) in [
        ("api last_received_event_seq", api.last_received_event_seq),
        ("api next_command_seq", api.next_command_seq),
    ] {
        if cursor > MAX_JSON_SAFE_INTEGER {
            return Err(SupervisorError::Fatal(anyhow!(
                "{name} exceeds JSON-safe integer range"
            )));
        }
    }
    if api.accepted_generation != agent.generation {
        return Err(SupervisorError::Fatal(anyhow!(
            "generation claim mismatch: api={}, agent={}",
            api.accepted_generation,
            agent.generation
        )));
    }
    if api.last_received_event_seq > agent.last_sent_event_seq {
        return Err(SupervisorError::Fatal(anyhow!(
            "event cursor claim mismatch: api received {} but agent only sent {}",
            api.last_received_event_seq,
            agent.last_sent_event_seq
        )));
    }
    let cursor = source
        .event_cursor()
        .await
        .map_err(SupervisorError::Fatal)?;
    if api.last_received_event_seq > cursor.last_sent {
        return Err(SupervisorError::Fatal(anyhow!(
            "API claims event seq {} beyond durable cursor {}",
            api.last_received_event_seq,
            cursor.last_sent
        )));
    }
    if api.next_command_seq > agent.last_received_command_seq.saturating_add(1) {
        return Err(SupervisorError::Fatal(anyhow!(
            "command cursor claim mismatch: next_command_seq {} > received {} + 1",
            api.next_command_seq,
            agent.last_received_command_seq
        )));
    }
    let min_next = agent.last_applied_command_seq.saturating_add(1);
    if api.next_command_seq < min_next {
        return Err(SupervisorError::Fatal(anyhow!(
            "command cursor claim too low: next_command_seq {} < applied {} + 1",
            api.next_command_seq,
            agent.last_applied_command_seq
        )));
    }
    Ok(())
}

#[derive(Debug)]
enum SupervisorError {
    AuthRejected,
    Fatal(anyhow::Error),
    Reconnect { reason: String },
}

fn classify_epoch_error(error: anyhow::Error) -> SupervisorError {
    let reason = format!("{error:#}");
    // These failures demonstrate a violated durable/wire invariant. Reconnects
    // cannot repair them and would only hide a bad state transition.
    if [
        "generation",
        "hydration",
        "command seq gap",
        "cursor",
        "non-monotonic",
    ]
    .iter()
    .any(|marker| reason.contains(marker))
    {
        SupervisorError::Fatal(anyhow!(reason))
    } else {
        SupervisorError::Reconnect { reason }
    }
}

async fn event_forwarder(
    mut event_rx: mpsc::Receiver<(DeliveryEpoch, OutboundFrame)>,
    current_writer: CurrentWriterSlot,
) {
    while let Some((epoch, frame)) = event_rx.recv().await {
        let sender = {
            let guard = current_writer.lock().unwrap();
            guard
                .as_ref()
                .and_then(|(e, s)| if *e == epoch { Some(s.clone()) } else { None })
        };
        if let Some(sender) = sender
            && sender.send(frame).await.is_err()
        {
            // Writer closed; stale frame is dropped. The supervisor will
            // install a new epoch and catch-up from the durable source.
        }
    }
}

async fn writer_task<W, S>(
    mut writer: W,
    mut writer_rx: mpsc::Receiver<OutboundFrame>,
    source: S,
    api_hello: ApiHello,
    config: SupervisorConfig,
    token: CancellationToken,
) -> Result<()>
where
    W: GatewayWriter,
    S: DurableSource,
{
    let mut last_received = api_hello.last_received_event_seq;
    loop {
        let cursor = source.event_cursor().await?;
        if last_received >= cursor.last_sent {
            break;
        }
        let events = source
            .events_after(last_received, config.catch_up_page_size)
            .await?;
        if events.is_empty() {
            bail!("event source returned empty page before cursor");
        }
        for frame in events {
            let seq =
                outbound_frame_event_seq(&frame).context("catch-up frame missing durable seq")?;
            if seq <= last_received {
                bail!("non-monotonic catch-up event: seq {seq} after {last_received}");
            }
            last_received = seq;
            send_with_timeout(&mut writer, frame, config.send_timeout, &token).await?;
        }
    }

    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => return Ok(()),
            frame = writer_rx.recv() => {
                let Some(frame) = frame else { return Ok(()); };
                send_with_timeout(&mut writer, frame, config.send_timeout, &token).await?;
            }
        }
    }
}

async fn send_with_timeout<W>(
    writer: &mut W,
    frame: OutboundFrame,
    timeout: Duration,
    token: &CancellationToken,
) -> Result<()>
where
    W: GatewayWriter,
{
    tokio::select! {
        _ = token.cancelled() => bail!("epoch cancelled"),
        result = time::timeout(timeout, writer.send(frame)) => match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => bail!("gateway send timeout"),
        },
    }
}

async fn reader_task<R, L>(
    mut reader: R,
    mut command_tx: mpsc::Sender<InboundCommand>,
    latch: L,
    api_hello: ApiHello,
    token: CancellationToken,
) -> Result<()>
where
    R: GatewayReader,
    L: HydrationLatch,
{
    const MAX_PENDING_BEFORE_READY: usize = 16;

    let mut ready: Option<HydrationReady> = None;
    let mut pending: Vec<InboundCommand> = Vec::with_capacity(MAX_PENDING_BEFORE_READY);
    let mut next_expected = api_hello.next_command_seq;

    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => return Ok(()),
            result = latch.wait_for(api_hello.accepted_generation), if ready.is_none() => {
                let hydration_ready = result?;
                if hydration_ready.generation != api_hello.accepted_generation {
                    bail!("hydration generation mismatch");
                }
                ready = Some(hydration_ready);
                for cmd in pending.drain(..) {
                    next_expected = send_validated(cmd, next_expected, &mut command_tx).await?;
                }
            }
            cmd = reader.next_command() => {
                let cmd = cmd?;
                if let Some(_ready) = &ready {
                    next_expected = send_validated(cmd, next_expected, &mut command_tx).await?;
                } else {
                    if pending.len() >= MAX_PENDING_BEFORE_READY {
                        bail!("hydration hold buffer full");
                    }
                    pending.push(cmd);
                }
            }
        }
    }
}

async fn send_validated(
    cmd: InboundCommand,
    next_expected: u64,
    command_tx: &mut mpsc::Sender<InboundCommand>,
) -> Result<u64> {
    let seq = inbound_command_seq(&cmd);
    if seq != next_expected {
        bail!("command seq gap: expected {next_expected}, got {seq}");
    }
    if command_tx.send(cmd).await.is_err() {
        bail!("command consumer closed");
    }
    Ok(next_expected + 1)
}

pub(crate) fn inbound_command_seq(cmd: &InboundCommand) -> u64 {
    match cmd {
        InboundCommand::Valid(envelope) => envelope.seq,
        InboundCommand::Invalid { seq, .. } => *seq,
    }
}

pub(crate) fn outbound_frame_event_seq(frame: &OutboundFrame) -> Result<u64> {
    match frame {
        OutboundFrame::Event { envelope } => {
            envelope.seq.context("durable event frame missing seq")
        }
        OutboundFrame::CommandAck { .. } => {
            bail!("command ack frame has no event seq")
        }
    }
}

/// T17 will supply this through a `watch::Receiver<HydrationState>`.
#[derive(Clone)]
pub struct WatchHydrationLatch {
    rx: watch::Receiver<HydrationState>,
}

impl WatchHydrationLatch {
    pub fn new(rx: watch::Receiver<HydrationState>) -> Self {
        Self { rx }
    }
}

#[async_trait]
impl HydrationLatch for WatchHydrationLatch {
    async fn wait_for(&self, generation: u64) -> Result<HydrationReady> {
        let mut rx = self.rx.clone();
        loop {
            let state = rx.borrow().clone();
            match state {
                HydrationState::Ready(ready) if ready.generation == generation => return Ok(ready),
                HydrationState::Ready(ready) => {
                    bail!(
                        "hydration ready for different generation: expected {generation}, got {}",
                        ready.generation
                    )
                }
                HydrationState::NotReady => {
                    rx.changed().await.context("hydration latch dropped")?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use anyhow::{Result, anyhow};
    use tokio::sync::watch;

    use super::*;
    use crate::gateway::stdio::SingleConnectionConnector;
    use crate::gateway::wire::to_wire_frame;
    use crate::gateway::{
        Command, CommandAck, CommandAckStatus, CommandEnvelope, CommandId, CommandRejectReason,
        Envelope, Gateway, GatewayReader, GatewayWriter, InboundCommand, OutboundFrame,
    };

    #[derive(Clone)]
    struct MockDurableSource {
        event_cursor_value: Arc<AtomicU64>,
        event_queue: Arc<std::sync::Mutex<VecDeque<OutboundFrame>>>,
        command_cursor: CommandCursors,
    }

    impl MockDurableSource {
        fn new(command_cursor: CommandCursors) -> Self {
            Self {
                event_cursor_value: Arc::new(AtomicU64::new(0)),
                event_queue: Arc::new(std::sync::Mutex::new(VecDeque::new())),
                command_cursor,
            }
        }

        fn push_event(&self, frame: OutboundFrame) {
            if let Ok(seq) = outbound_frame_event_seq(&frame) {
                self.event_cursor_value.store(seq, Ordering::SeqCst);
            }
            self.event_queue.lock().unwrap().push_back(frame);
        }
    }

    #[async_trait]
    impl DurableSource for MockDurableSource {
        async fn event_cursor(&self) -> Result<EventCursors> {
            Ok(EventCursors {
                last_sent: self.event_cursor_value.load(Ordering::SeqCst),
            })
        }

        async fn events_after(&self, after_seq: u64, limit: usize) -> Result<Vec<OutboundFrame>> {
            let mut guard = self.event_queue.lock().unwrap();
            let mut out = Vec::with_capacity(limit);
            while out.len() < limit {
                match guard.front() {
                    Some(frame) => {
                        let seq = outbound_frame_event_seq(frame)?;
                        if seq > after_seq {
                            out.push(guard.pop_front().unwrap());
                        } else {
                            guard.pop_front();
                        }
                    }
                    None => break,
                }
            }
            Ok(out)
        }

        async fn command_cursors(&self) -> Result<CommandCursors> {
            Ok(self.command_cursor)
        }
    }

    #[derive(Clone)]
    struct StaticHydrationLatch(HydrationReady);

    #[async_trait]
    impl HydrationLatch for StaticHydrationLatch {
        async fn wait_for(&self, generation: u64) -> Result<HydrationReady> {
            if self.0.generation != generation {
                bail!("generation mismatch");
            }
            Ok(self.0.clone())
        }
    }

    #[derive(Clone)]
    struct DynamicHydrationLatch {
        tx: watch::Sender<HydrationState>,
    }

    impl DynamicHydrationLatch {
        fn new() -> (Self, watch::Sender<HydrationState>) {
            let (tx, _rx) = watch::channel(HydrationState::NotReady);
            (Self { tx: tx.clone() }, tx)
        }
    }

    #[async_trait]
    impl HydrationLatch for DynamicHydrationLatch {
        async fn wait_for(&self, generation: u64) -> Result<HydrationReady> {
            WatchHydrationLatch::new(self.tx.subscribe())
                .wait_for(generation)
                .await
        }
    }

    #[derive(Clone)]
    struct CountingCredentialProvider {
        counter: Arc<AtomicU64>,
        prefix: String,
    }

    impl CountingCredentialProvider {
        fn new(prefix: impl Into<String>) -> Self {
            Self {
                counter: Arc::new(AtomicU64::new(0)),
                prefix: prefix.into(),
            }
        }
    }

    #[async_trait]
    impl CredentialProvider for CountingCredentialProvider {
        async fn fresh_credential(&mut self) -> Result<GatewayCredential> {
            let n = self.counter.fetch_add(1, Ordering::SeqCst);
            Ok(GatewayCredential::new(format!("{}-{}", self.prefix, n)))
        }
    }

    struct MockGatewayReader {
        commands: VecDeque<Result<InboundCommand>>,
    }

    struct MockGatewayWriter {
        fail_after: Option<usize>,
        sent: Arc<std::sync::Mutex<Vec<OutboundFrame>>>,
        delay: Option<Duration>,
    }

    #[async_trait]
    impl GatewayReader for MockGatewayReader {
        async fn next_command(&mut self) -> Result<InboundCommand> {
            match self.commands.pop_front() {
                Some(Ok(cmd)) => Ok(cmd),
                Some(Err(e)) => Err(e),
                None => std::future::pending::<Result<InboundCommand>>().await,
            }
        }
    }

    #[async_trait]
    impl GatewayWriter for MockGatewayWriter {
        async fn send(&mut self, frame: OutboundFrame) -> Result<()> {
            if let Some(d) = self.delay {
                tokio::time::sleep(d).await;
            }
            let mut sent = self.sent.lock().unwrap();
            if let Some(n) = self.fail_after
                && sent.len() >= n
            {
                bail!("writer failure");
            }
            sent.push(frame);
            Ok(())
        }
    }

    struct MockGateway {
        reader: MockGatewayReader,
        writer: MockGatewayWriter,
        sent_hellos: Arc<std::sync::Mutex<Vec<AgentHello>>>,
        hello_generation: Option<u64>,
        last_received_event_seq: u64,
        hello_delay: Option<Duration>,
    }

    impl MockGateway {
        fn new(commands: VecDeque<Result<InboundCommand>>) -> Self {
            Self {
                reader: MockGatewayReader { commands },
                writer: MockGatewayWriter {
                    fail_after: None,
                    sent: Arc::new(std::sync::Mutex::new(Vec::new())),
                    delay: None,
                },
                sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
                hello_generation: None,
                last_received_event_seq: 0,
                hello_delay: None,
            }
        }

        fn with_hello_generation(mut self, generation: u64) -> Self {
            self.hello_generation = Some(generation);
            self
        }

        fn with_last_received_event_seq(mut self, seq: u64) -> Self {
            self.last_received_event_seq = seq;
            self
        }

        fn with_hello_delay(mut self, delay: Duration) -> Self {
            self.hello_delay = Some(delay);
            self
        }

        fn sent(&self) -> Arc<std::sync::Mutex<Vec<OutboundFrame>>> {
            self.writer.sent.clone()
        }
    }

    #[async_trait]
    impl Gateway for MockGateway {
        type Reader = MockGatewayReader;
        type Writer = MockGatewayWriter;

        async fn authenticate_hello(&mut self, hello: AgentHello) -> Result<ApiHello> {
            self.sent_hellos.lock().unwrap().push(hello.clone());
            if let Some(delay) = self.hello_delay {
                tokio::time::sleep(delay).await;
            }
            let accepted_generation = self.hello_generation.unwrap_or(hello.generation);
            Ok(ApiHello {
                accepted_generation,
                last_received_event_seq: self.last_received_event_seq,
                next_command_seq: hello.last_applied_command_seq.saturating_add(1),
            })
        }

        fn split(self) -> (Self::Reader, Self::Writer) {
            (self.reader, self.writer)
        }
    }

    struct MockConnector {
        responses: VecDeque<Result<MockGateway, ConnectorError>>,
        sent_hellos: Arc<std::sync::Mutex<Vec<AgentHello>>>,
    }

    impl MockConnector {
        fn new(
            sent_hellos: Arc<std::sync::Mutex<Vec<AgentHello>>>,
            responses: VecDeque<Result<MockGateway, ConnectorError>>,
        ) -> Self {
            Self {
                responses,
                sent_hellos,
            }
        }
    }

    #[async_trait]
    impl GatewayConnector for MockConnector {
        type Connection = MockGateway;

        async fn connect(
            &mut self,
            _credential: GatewayCredential,
        ) -> Result<Self::Connection, ConnectorError> {
            let mut gateway = self
                .responses
                .pop_front()
                .expect("mock connector has a queued response")?;
            // Share the hello tracker so the test can observe all attempts.
            gateway.sent_hellos = self.sent_hellos.clone();
            Ok(gateway)
        }
    }

    fn make_config() -> SupervisorConfig {
        SupervisorConfig {
            agent_id: "test-agent".to_owned(),
            generation: 7,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(5),
            send_timeout: Duration::from_millis(50),
            event_buffer_size: 16,
            command_buffer_size: 16,
            catch_up_page_size: 16,
            max_reconnect_attempts: Some(10),
            hello_timeout: Duration::from_secs(5),
        }
    }

    fn event_frame(seq: u64) -> OutboundFrame {
        OutboundFrame::Event {
            envelope: Envelope {
                seq: Some(seq),
                conversation_id: "conversation-1".to_owned(),
                event: serde_json::json!({"type": "agent_start"}),
            },
        }
    }

    fn valid_command(seq: u64, command_id: &str) -> InboundCommand {
        InboundCommand::Valid(CommandEnvelope {
            seq,
            command_id: CommandId::parse(command_id).unwrap(),
            command: Command::Abort {},
        })
    }

    fn rejected_oversized_command(seq: u64, command_id: &str, actual_bytes: u64) -> InboundCommand {
        InboundCommand::Invalid {
            seq,
            command_id: CommandId::parse(command_id).unwrap(),
            reason: CommandRejectReason::Oversized { actual_bytes },
            raw_command: crate::gateway::RejectedCommandPayload::DiscardedOversized,
            payload_digest: Some(crate::gateway::KeyedCommandDigest::new("test-key", [0; 32])),
        }
    }

    // Tests

    #[tokio::test]
    async fn fresh_credential_per_attempt() {
        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let connector = MockConnector::new(
            sent_hellos.clone(),
            VecDeque::from([
                Ok(MockGateway::new(VecDeque::from([Err(anyhow!(
                    "reader EOF"
                ))]))),
                Ok(MockGateway::new(VecDeque::from([Err(anyhow!(
                    "reader EOF"
                ))]))),
            ]),
        );
        let credentials = CountingCredentialProvider::new("token");
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: 7,
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let handle = supervisor.start();

        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.abort();
        let _ = handle.join().await;

        let hellos = sent_hellos.lock().unwrap();
        assert_eq!(hellos.len(), 2, "two hello attempts with fresh credentials");
    }

    #[tokio::test]
    async fn claim_mismatch_is_fatal() {
        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let connector = MockConnector::new(
            sent_hellos,
            VecDeque::from([Ok(
                MockGateway::new(VecDeque::new()).with_hello_generation(99)
            )]),
        );
        let credentials = CountingCredentialProvider::new("token");
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: 7,
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let handle = supervisor.start();

        let result = handle.join().await;
        assert!(result.is_err());
        assert!(format!("{:#}", result.unwrap_err()).contains("generation claim mismatch"));
    }

    #[tokio::test]
    async fn reader_eof_triggers_reconnect() {
        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let connector = MockConnector::new(
            sent_hellos.clone(),
            VecDeque::from([
                Ok(MockGateway::new(VecDeque::from([Err(anyhow!(
                    "reader EOF"
                ))]))),
                Ok(MockGateway::new(VecDeque::from([Err(anyhow!(
                    "reader EOF"
                ))]))),
            ]),
        );
        let credentials = CountingCredentialProvider::new("token");
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: 7,
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let handle = supervisor.start();

        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.abort();
        let _ = handle.join().await;

        assert!(sent_hellos.lock().unwrap().len() >= 2);
    }

    #[tokio::test]
    async fn writer_failure_closes_epoch_and_reconnects() {
        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));

        let gateway1 = MockGateway {
            reader: MockGatewayReader {
                commands: VecDeque::from([Ok(valid_command(
                    1,
                    "00000000-0000-4000-8000-000000000001",
                ))]),
            },
            writer: MockGatewayWriter {
                fail_after: Some(0),
                sent: sent.clone(),
                delay: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
        };
        let gateway2 = MockGateway {
            reader: MockGatewayReader {
                commands: VecDeque::new(),
            },
            writer: MockGatewayWriter {
                fail_after: None,
                sent: sent.clone(),
                delay: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
        };

        let connector = MockConnector::new(
            sent_hellos.clone(),
            VecDeque::from([Ok(gateway1), Ok(gateway2)]),
        );
        let credentials = CountingCredentialProvider::new("token");
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: 7,
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let mut handle = supervisor.start();

        // Wait for the command to be delivered, then trigger a writer failure by
        // sending a live event to the first epoch.
        let cmd = tokio::time::timeout(Duration::from_millis(200), handle.commands.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cmd_seq(&cmd), 1);

        let epoch1 = (*handle.epochs.borrow()).unwrap();
        handle.events.send((epoch1, event_frame(1))).await.unwrap();

        // After the writer fails, the supervisor reconnects with a new epoch.
        let mut epochs = handle.epochs.clone();
        let _epoch2 = loop {
            tokio::time::timeout(Duration::from_millis(200), epochs.changed())
                .await
                .unwrap()
                .unwrap();
            if let Some(e) = *epochs.borrow()
                && e != epoch1
            {
                break e;
            }
        };

        handle.abort();
        let _ = handle.join().await;

        assert!(sent_hellos.lock().unwrap().len() >= 2);
    }

    #[tokio::test]
    async fn catch_up_sends_durable_events_before_online() {
        let source = MockDurableSource::new(CommandCursors::default());
        source.push_event(event_frame(1));
        source.push_event(event_frame(2));

        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway {
            reader: MockGatewayReader {
                commands: VecDeque::new(),
            },
            writer: MockGatewayWriter {
                fail_after: None,
                sent: sent.clone(),
                delay: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
        };

        let connector = MockConnector::new(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            VecDeque::from([Ok(gateway)]),
        );
        let credentials = CountingCredentialProvider::new("token");
        let latch = StaticHydrationLatch(HydrationReady {
            generation: 7,
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let handle = supervisor.start();

        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.abort();
        let _ = handle.join().await;

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert_eq!(outbound_frame_event_seq(&sent[0]).unwrap(), 1);
        assert_eq!(outbound_frame_event_seq(&sent[1]).unwrap(), 2);
    }

    #[tokio::test]
    async fn epoch_replacement_invalidates_old_delivery_epoch() {
        let source = MockDurableSource::new(CommandCursors::default());

        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));

        let gateway1 = MockGateway {
            reader: MockGatewayReader {
                commands: VecDeque::new(),
            },
            writer: MockGatewayWriter {
                fail_after: Some(0),
                sent: sent.clone(),
                delay: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
        };
        let gateway2 = MockGateway {
            reader: MockGatewayReader {
                commands: VecDeque::new(),
            },
            writer: MockGatewayWriter {
                fail_after: None,
                sent: sent.clone(),
                delay: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
        };

        let connector = MockConnector::new(
            sent_hellos.clone(),
            VecDeque::from([Ok(gateway1), Ok(gateway2)]),
        );
        let credentials = CountingCredentialProvider::new("token");
        let latch = StaticHydrationLatch(HydrationReady {
            generation: 7,
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let mut handle = supervisor.start();

        tokio::time::timeout(Duration::from_millis(100), handle.epochs.changed())
            .await
            .unwrap()
            .unwrap();
        let epoch1 = (*handle.epochs.borrow()).unwrap();

        // Drop a stale-epoch frame; it must not be sent after reconnect.
        handle.events.send((epoch1, event_frame(1))).await.unwrap();

        // Wait for the second epoch to be installed.
        let mut epochs = handle.epochs.clone();
        let epoch2 = loop {
            tokio::time::timeout(Duration::from_millis(200), epochs.changed())
                .await
                .unwrap()
                .unwrap();
            if let Some(e) = *epochs.borrow()
                && e != epoch1
            {
                break e;
            }
        };

        // Send a frame with the new epoch.
        handle.events.send((epoch2, event_frame(2))).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.abort();
        let _ = handle.join().await;

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(outbound_frame_event_seq(&sent[0]).unwrap(), 2);
    }

    #[tokio::test]
    async fn hello_before_ready_holds_commands() {
        let (latch, tx) = DynamicHydrationLatch::new();
        let source = MockDurableSource::new(CommandCursors::default());
        source.push_event(event_frame(1));

        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway {
            reader: MockGatewayReader {
                commands: VecDeque::from([Ok(valid_command(
                    1,
                    "00000000-0000-4000-8000-000000000001",
                ))]),
            },
            writer: MockGatewayWriter {
                fail_after: None,
                sent: sent.clone(),
                delay: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
        };

        let connector = MockConnector::new(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            VecDeque::from([Ok(gateway)]),
        );
        let credentials = CountingCredentialProvider::new("token");

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let mut handle = supervisor.start();

        assert!(
            tokio::time::timeout(Duration::from_millis(50), handle.commands.recv())
                .await
                .is_err(),
            "command must be held until hydration ready"
        );

        tx.send(HydrationState::Ready(HydrationReady {
            generation: 7,
            receipt_identity: "receipt-1".to_owned(),
        }))
        .unwrap();

        let cmd = tokio::time::timeout(Duration::from_millis(200), handle.commands.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cmd_seq(&cmd), 1);

        handle.abort();
        let _ = handle.join().await;
    }

    #[tokio::test]
    async fn oversized_terminal_rejected_is_forwarded_with_digest() {
        let source = MockDurableSource::new(CommandCursors::default());

        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway {
            reader: MockGatewayReader {
                commands: VecDeque::from([Ok(rejected_oversized_command(
                    1,
                    "00000000-0000-4000-8000-000000000001",
                    1_200_000,
                ))]),
            },
            writer: MockGatewayWriter {
                fail_after: None,
                sent: sent.clone(),
                delay: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
        };

        let connector = MockConnector::new(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            VecDeque::from([Ok(gateway)]),
        );
        let credentials = CountingCredentialProvider::new("token");
        let latch = StaticHydrationLatch(HydrationReady {
            generation: 7,
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let mut handle = supervisor.start();

        let cmd = tokio::time::timeout(Duration::from_millis(200), handle.commands.recv())
            .await
            .unwrap()
            .unwrap();
        match cmd {
            InboundCommand::Invalid {
                reason: CommandRejectReason::Oversized { actual_bytes },
                payload_digest: Some(_),
                ..
            } => assert_eq!(actual_bytes, 1_200_000),
            _ => panic!("expected oversized invalid command with digest"),
        }

        // Simulate downstream producing the Rejected ACK.
        let ack_frame = OutboundFrame::CommandAck {
            ack: CommandAck {
                seq: 1,
                command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                status: CommandAckStatus::Rejected,
                reject_reason: Some("oversized".to_owned()),
            },
        };
        to_wire_frame(ack_frame.clone()).expect("ack validates as wire frame");

        let epoch = *handle.epochs.borrow();
        handle
            .events
            .send((epoch.unwrap(), ack_frame))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.abort();
        let _ = handle.join().await;

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert!(matches!(sent[0], OutboundFrame::CommandAck { .. }));
    }

    #[tokio::test]
    async fn send_validated_errors_when_command_consumer_is_closed() {
        let (mut tx, rx) = mpsc::channel::<InboundCommand>(1);
        drop(rx);
        let result = send_validated(
            valid_command(1, "00000000-0000-4000-8000-000000000001"),
            1,
            &mut tx,
        )
        .await;
        assert!(result.is_err());
        assert!(format!("{:#}", result.unwrap_err()).contains("command consumer closed"));
    }

    #[tokio::test]
    async fn auth_rejected_does_not_spin_forever_when_unlimited() {
        let mut config = make_config();
        config.max_reconnect_attempts = None;
        config.hello_timeout = Duration::from_millis(5);

        let credentials = CountingCredentialProvider::new("token");
        let counter = credentials.counter.clone();
        let connector = MockConnector::new(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            VecDeque::from([
                Err(ConnectorError::AuthRejected),
                Err(ConnectorError::AuthRejected),
                Err(ConnectorError::AuthRejected),
                Err(ConnectorError::AuthRejected),
                Err(ConnectorError::AuthRejected),
            ]),
        );
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: 7,
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor = ConnectionSupervisor::new(connector, credentials, source, latch, config);
        let handle = supervisor.start();

        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(
            result.is_ok(),
            "supervisor must stop within bounded time when auth is rejected"
        );
        assert!(result.unwrap().is_err());

        let attempts = counter.load(Ordering::SeqCst);
        assert!(
            (1..=5).contains(&attempts),
            "auth retries must be bounded, got {attempts}"
        );
    }

    #[tokio::test]
    async fn consumed_single_connection_connector_is_fatal() {
        let gateway = MockGateway::new(VecDeque::from([Err(anyhow!("reader EOF"))]));
        let connector = SingleConnectionConnector::new(gateway);
        let credentials = CountingCredentialProvider::new("token");
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: 7,
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let handle = supervisor.start();

        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(
            result.is_ok(),
            "supervisor must stop on a fatal connector error"
        );
        assert!(result.unwrap().is_err());
    }

    #[tokio::test]
    async fn hello_timeout_is_enforced_by_supervisor_config() {
        let mut config = make_config();
        config.hello_timeout = Duration::from_millis(1);
        config.initial_backoff = Duration::from_millis(1);
        config.max_reconnect_attempts = Some(1);

        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let connector = MockConnector::new(
            sent_hellos.clone(),
            VecDeque::from([
                Ok(MockGateway::new(VecDeque::new()).with_hello_delay(Duration::from_secs(60))),
                Ok(MockGateway::new(VecDeque::new()).with_hello_delay(Duration::from_secs(60))),
            ]),
        );
        let credentials = CountingCredentialProvider::new("token");
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: 7,
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor = ConnectionSupervisor::new(connector, credentials, source, latch, config);
        let handle = supervisor.start();

        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_err());

        assert_eq!(
            sent_hellos.lock().unwrap().len(),
            2,
            "hello timeout must reconnect with a fresh connection"
        );
    }

    #[tokio::test]
    async fn backoff_jitter_keeps_nonzero_lower_bound() {
        let mut config = make_config();
        config.initial_backoff = Duration::from_millis(5);
        config.max_backoff = Duration::from_millis(10);

        let start = Instant::now();
        ConnectionSupervisor::<
            MockConnector,
            CountingCredentialProvider,
            MockDurableSource,
            StaticHydrationLatch,
        >::backoff_sleep(&config, 1)
        .await
        .expect("backoff sleep should succeed");
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(1),
            "nonzero delay must have nonzero jitter lower bound, elapsed {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn validate_hello_rejects_next_command_seq_below_last_applied_plus_one() {
        let source = MockDurableSource::new(CommandCursors {
            received: 5,
            applied: 3,
        });

        let agent = AgentHello {
            agent_id: "test-agent".to_owned(),
            generation: 7,
            last_sent_event_seq: 0,
            last_received_command_seq: 5,
            last_applied_command_seq: 3,
        };

        // next_command_seq == last_applied_command_seq is too low.
        let api_too_low = ApiHello {
            accepted_generation: 7,
            last_received_event_seq: 0,
            next_command_seq: 3,
        };
        let err = validate_hello(&source, &agent, &api_too_low).await;
        assert!(err.is_err());
        assert!(
            format!("{:?}", err.unwrap_err()).contains("command cursor claim too low"),
            "api.next_command_seq below last_applied+1 must be fatal"
        );

        // next_command_seq == last_applied_command_seq + 1 is the lower bound.
        let api_valid = ApiHello {
            accepted_generation: 7,
            last_received_event_seq: 0,
            next_command_seq: 4,
        };
        validate_hello(&source, &agent, &api_valid)
            .await
            .expect("next_command_seq at last_applied+1 is valid");
    }

    #[tokio::test]
    async fn established_epoch_failure_preserves_reconnect_backoff() {
        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway::new(VecDeque::from([
            Ok(valid_command(1, "00000000-0000-4000-8000-000000000001")),
            Err(anyhow!("reader EOF")),
        ]));

        let connector = MockConnector::new(
            sent_hellos.clone(),
            VecDeque::from([
                Ok(gateway),
                Err(ConnectorError::AuthRejected),
                Err(ConnectorError::AuthRejected),
            ]),
        );
        let credentials = CountingCredentialProvider::new("token");
        let counter = credentials.counter.clone();
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: 7,
            receipt_identity: "receipt-1".to_owned(),
        });

        let mut config = make_config();
        config.max_reconnect_attempts = Some(1);

        let supervisor = ConnectionSupervisor::new(connector, credentials, source, latch, config);
        let handle = supervisor.start();

        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(
            result.is_ok(),
            "supervisor should stop after exhausting bounded reconnect attempts"
        );
        assert!(result.unwrap().is_err());

        // The reader's transport failure consumes an attempt; the following
        // auth rejection exhausts the configured bound rather than resetting it.
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    fn cmd_seq(cmd: &InboundCommand) -> u64 {
        inbound_command_seq(cmd)
    }
}
