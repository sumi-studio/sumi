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
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinError, JoinHandle};
use tokio::time;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroize;

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
    pub generation: ProcessGeneration,
    pub last_sent_event_seq: u64,
    pub last_received_command_seq: u64,
    pub last_applied_command_seq: u64,
}

/// API → Agent hello response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiHello {
    pub accepted_generation: ProcessGeneration,
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
    pub generation: ProcessGeneration,
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
    async fn wait_for(&self, generation: ProcessGeneration) -> Result<HydrationReady>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SupervisorConfig {
    pub agent_id: String,
    pub generation: ProcessGeneration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub send_timeout: Duration,
    pub event_buffer_size: usize,
    pub command_buffer_size: usize,
    pub catch_up_page_size: usize,
    pub max_reconnect_attempts: Option<u32>,
    pub max_auth_attempts: Option<u32>,
    pub hello_timeout: Duration,
    pub connect_timeout: Duration,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            agent_id: String::new(),
            generation: ProcessGeneration::MIN,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
            send_timeout: Duration::from_secs(30),
            event_buffer_size: 64,
            command_buffer_size: 64,
            catch_up_page_size: 64,
            max_reconnect_attempts: None,
            max_auth_attempts: Some(3),
            hello_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(30),
        }
    }
}

/// Handle to a spawned `ConnectionSupervisor`.
pub struct SupervisorHandle {
    pub commands: mpsc::Receiver<InboundCommand>,
    pub events: mpsc::Sender<(DeliveryEpoch, OutboundFrame)>,
    pub epochs: watch::Receiver<Option<DeliveryEpoch>>,
    cancel: CancellationToken,
    task: JoinHandle<Result<()>>,
}

impl SupervisorHandle {
    pub fn abort(&self) {
        self.cancel.cancel();
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
    cancel: CancellationToken,
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
            cancel: CancellationToken::new(),
        }
    }

    /// Start the supervisor and return channels for commands, events, and epoch
    /// observation. `events` must carry the current `DeliveryEpoch` from
    /// `handle.epochs`.
    pub fn start(self) -> SupervisorHandle {
        let (commands_tx, commands_rx) = mpsc::channel(self.config.command_buffer_size);
        let (events_tx, events_rx) = mpsc::channel(self.config.event_buffer_size);
        let epochs_rx = self.current_epoch.subscribe();
        let cancel = self.cancel.clone();
        let task = tokio::spawn(self.run(commands_tx, events_rx));
        SupervisorHandle {
            commands: commands_rx,
            events: events_tx,
            epochs: epochs_rx,
            cancel,
            task,
        }
    }

    pub async fn run(
        mut self,
        commands_tx: mpsc::Sender<InboundCommand>,
        events_rx: mpsc::Receiver<(DeliveryEpoch, OutboundFrame)>,
    ) -> Result<()> {
        let current_writer = self.current_writer.clone();
        let cancel = self.cancel.clone();
        let forwarder = tokio::spawn(event_forwarder(events_rx, current_writer, cancel));

        let cancel = self.cancel.clone();
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Ok(()),
            result = self.run_loop(commands_tx) => result,
        };

        forwarder.abort();
        let _ = forwarder.await;
        result
    }

    async fn run_loop(&mut self, commands_tx: mpsc::Sender<InboundCommand>) -> Result<()> {
        const DEFAULT_MAX_AUTH_ATTEMPTS: u32 = 3;

        let mut auth_attempt: u32 = 0;
        let mut reconnect_attempt: u32 = 0;

        loop {
            if self.cancel.is_cancelled() {
                return Ok(());
            }

            match self.connect_and_run_epoch(commands_tx.clone()).await {
                Ok(()) => return Ok(()),
                Err(SupervisorError::Fatal(e)) => return Err(e),
                Err(SupervisorError::AuthRejected) => {
                    auth_attempt = auth_attempt.saturating_add(1);
                    reconnect_attempt = 0;
                    let max = self
                        .config
                        .max_auth_attempts
                        .unwrap_or(DEFAULT_MAX_AUTH_ATTEMPTS);
                    if auth_attempt > max {
                        return Err(anyhow!("max auth attempts exceeded"));
                    }
                    tokio::select! {
                        biased;
                        _ = self.cancel.cancelled() => return Ok(()),
                        result = Self::backoff_sleep(&self.config, auth_attempt) => result?,
                    }
                }
                Err(SupervisorError::Reconnect { reason }) => {
                    reconnect_attempt = reconnect_attempt.saturating_add(1);
                    if let Some(max) = self.config.max_reconnect_attempts
                        && reconnect_attempt > max
                    {
                        return Err(anyhow!("max reconnect attempts exceeded: {reason}"));
                    }
                    tokio::select! {
                        biased;
                        _ = self.cancel.cancelled() => return Ok(()),
                        result = Self::backoff_sleep(&self.config, reconnect_attempt) => result?,
                    }
                }
                Err(SupervisorError::EstablishedReconnect { reason }) => {
                    // A healthy epoch ended; reset failure streaks and reconnect.
                    auth_attempt = 0;
                    reconnect_attempt = 0;
                    reconnect_attempt = reconnect_attempt.saturating_add(1);
                    if let Some(max) = self.config.max_reconnect_attempts
                        && reconnect_attempt > max
                    {
                        return Err(anyhow!("max reconnect attempts exceeded: {reason}"));
                    }
                    tokio::select! {
                        biased;
                        _ = self.cancel.cancelled() => return Ok(()),
                        result = Self::backoff_sleep(&self.config, reconnect_attempt) => result?,
                    }
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
        let cancel = self.cancel.clone();

        let credential = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            result = self.credentials.fresh_credential() => result.map_err(|e| SupervisorError::Reconnect {
                reason: format!("failed to obtain credential: {e}"),
            })?,
        };

        let mut gateway = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            result = time::timeout(config.connect_timeout, self.connector.connect(credential)) => match result {
                Ok(Ok(g)) => g,
                Ok(Err(ConnectorError::AuthRejected)) => return Err(SupervisorError::AuthRejected),
                Ok(Err(ConnectorError::Fatal(e))) => return Err(SupervisorError::Fatal(e)),
                Ok(Err(ConnectorError::Other(e))) => return Err(SupervisorError::Reconnect {
                    reason: format!("connect failed: {e}"),
                }),
                Err(_) => return Err(SupervisorError::Reconnect {
                    reason: "connect timeout".to_owned(),
                }),
            },
        };

        let agent_hello = build_agent_hello(&source, &config).await?;
        let api_hello = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            result = time::timeout(config.hello_timeout, gateway.authenticate_hello(agent_hello.clone())) => match result {
                Ok(Ok(h)) => h,
                Ok(Err(e)) => return Err(SupervisorError::Reconnect {
                    reason: format!("hello failed: {e}"),
                }),
                Err(_) => return Err(SupervisorError::Reconnect {
                    reason: "hello response timeout".to_owned(),
                }),
            },
        };

        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            result = validate_hello(&source, &agent_hello, &api_hello) => result?,
        }

        let (connection_epoch, delivery_epoch) = self.next_epoch();
        let (reader, writer) = gateway.split();

        let epoch_token = cancel.child_token();
        let (writer_tx, writer_rx) = mpsc::channel(config.event_buffer_size);

        *self.current_writer.lock().unwrap() = Some((delivery_epoch, writer_tx));
        self.current_epoch.send_replace(Some(delivery_epoch));

        let mut reader_handle = tokio::spawn(reader_task(
            reader,
            commands_tx,
            self.latch.clone(),
            api_hello.clone(),
            epoch_token.child_token(),
        ));

        let mut writer_handle = tokio::spawn(writer_task(
            writer,
            writer_rx,
            source,
            api_hello,
            config,
            epoch_token.child_token(),
        ));

        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                epoch_token.cancel();
                let reader_result = reader_handle.await;
                let writer_result = writer_handle.await;
                *self.current_writer.lock().unwrap() = None;
                self.current_epoch.send_replace(None);
                for result in [&reader_result, &writer_result] {
                    if let Err(join_err) = result {
                        if join_err.is_panic() {
                            return Err(SupervisorError::Fatal(anyhow!(
                                "epoch task panicked: {join_err}"
                            )));
                        }
                        return Err(SupervisorError::Fatal(anyhow!(
                            "epoch task join error: {join_err}"
                        )));
                    }
                }
                return Ok(());
            }
            reader_result = &mut reader_handle => {
                epoch_token.cancel();
                *self.current_writer.lock().unwrap() = None;
                self.current_epoch.send_replace(None);
                Self::inspect_epoch_results(reader_result, writer_handle.await)
            }
            writer_result = &mut writer_handle => {
                epoch_token.cancel();
                *self.current_writer.lock().unwrap() = None;
                self.current_epoch.send_replace(None);
                Self::inspect_epoch_results(reader_handle.await, writer_result)
            }
        };

        if let Err(SupervisorError::EstablishedReconnect { .. }) = &result {
            tracing::debug!(
                connection_epoch = connection_epoch.as_u64(),
                delivery_epoch = delivery_epoch.as_u64(),
                "epoch ended"
            );
        }

        result
    }

    fn inspect_epoch_results(
        reader_result: Result<Result<()>, JoinError>,
        writer_result: Result<Result<()>, JoinError>,
    ) -> Result<(), SupervisorError> {
        for result in [&reader_result, &writer_result] {
            if let Err(join_err) = result {
                if join_err.is_panic() {
                    return Err(SupervisorError::Fatal(anyhow!(
                        "epoch task panicked: {join_err}"
                    )));
                }
                return Err(SupervisorError::Fatal(anyhow!(
                    "epoch task join error: {join_err}"
                )));
            }
        }

        let reader_result = reader_result.unwrap();
        let writer_result = writer_result.unwrap();

        match (reader_result, writer_result) {
            (Ok(()), Ok(())) => Err(SupervisorError::EstablishedReconnect {
                reason: "reader/writer task ended".to_owned(),
            }),
            (Err(e), _) | (_, Err(e)) => Err(SupervisorError::EstablishedReconnect {
                reason: format!("epoch task error: {e}"),
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
    if api.accepted_generation != agent.generation {
        return Err(SupervisorError::Fatal(anyhow!(
            "generation claim mismatch: api={}, agent={}",
            api.accepted_generation,
            agent.generation
        )));
    }

    // Re-fetch cursors so the hello is validated against the durable source
    // at the moment of authentication, not the snapshot used to build AgentHello.
    let event_cursor = source
        .event_cursor()
        .await
        .map_err(SupervisorError::Fatal)?;
    if api.last_received_event_seq > event_cursor.last_sent {
        return Err(SupervisorError::Fatal(anyhow!(
            "API claims event seq {} beyond durable cursor {}",
            api.last_received_event_seq,
            event_cursor.last_sent
        )));
    }

    let command_cursor = source
        .command_cursors()
        .await
        .map_err(SupervisorError::Fatal)?;
    if api.next_command_seq > command_cursor.received.saturating_add(1) {
        return Err(SupervisorError::Fatal(anyhow!(
            "command cursor claim mismatch: next_command_seq {} > received {} + 1",
            api.next_command_seq,
            command_cursor.received
        )));
    }
    Ok(())
}

#[derive(Debug)]
enum SupervisorError {
    AuthRejected,
    Fatal(anyhow::Error),
    Reconnect { reason: String },
    EstablishedReconnect { reason: String },
}

async fn event_forwarder(
    mut event_rx: mpsc::Receiver<(DeliveryEpoch, OutboundFrame)>,
    current_writer: CurrentWriterSlot,
    cancel: CancellationToken,
) {
    loop {
        let frame = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            frame = event_rx.recv() => frame,
        };
        let Some((epoch, frame)) = frame else {
            break;
        };
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
        let cursor = tokio::select! {
            biased;
            _ = token.cancelled() => return Ok(()),
            result = source.event_cursor() => result?,
        };
        if last_received >= cursor.last_sent {
            break;
        }
        let events = tokio::select! {
            biased;
            _ = token.cancelled() => return Ok(()),
            result = source.events_after(last_received, config.catch_up_page_size) => result?,
        };
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
    async fn wait_for(&self, generation: ProcessGeneration) -> Result<HydrationReady> {
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
    use std::sync::Mutex;
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
        async fn wait_for(&self, generation: ProcessGeneration) -> Result<HydrationReady> {
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
        async fn wait_for(&self, generation: ProcessGeneration) -> Result<HydrationReady> {
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
        panic: bool,
    }

    impl MockGatewayReader {
        fn with_panic(mut self) -> Self {
            self.panic = true;
            self
        }
    }

    struct MockGatewayWriter {
        fail_after: Option<usize>,
        sent: Arc<std::sync::Mutex<Vec<OutboundFrame>>>,
        delay: Option<Duration>,
    }

    #[async_trait]
    impl GatewayReader for MockGatewayReader {
        async fn next_command(&mut self) -> Result<InboundCommand> {
            if self.panic {
                panic!("mock reader panic");
            }
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
        hello_generation: Option<ProcessGeneration>,
        last_received_event_seq: u64,
        hello_delay: Option<Duration>,
    }

    impl MockGateway {
        fn new(commands: VecDeque<Result<InboundCommand>>) -> Self {
            Self {
                reader: MockGatewayReader {
                    commands,
                    panic: false,
                },
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
            self.hello_generation = Some(ProcessGeneration::from_wire(generation).unwrap());
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
        connect_delay: Option<Duration>,
    }

    impl MockConnector {
        fn new(
            sent_hellos: Arc<std::sync::Mutex<Vec<AgentHello>>>,
            responses: VecDeque<Result<MockGateway, ConnectorError>>,
        ) -> Self {
            Self {
                responses,
                sent_hellos,
                connect_delay: None,
            }
        }

        fn with_connect_delay(mut self, delay: Duration) -> Self {
            self.connect_delay = Some(delay);
            self
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
            if let Some(delay) = self.connect_delay {
                tokio::time::sleep(delay).await;
            }
            // Share the hello tracker so the test can observe all attempts.
            gateway.sent_hellos = self.sent_hellos.clone();
            Ok(gateway)
        }
    }

    fn make_config() -> SupervisorConfig {
        SupervisorConfig {
            agent_id: "test-agent".to_owned(),
            generation: ProcessGeneration::from_wire(7).unwrap(),
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(5),
            send_timeout: Duration::from_millis(50),
            event_buffer_size: 16,
            command_buffer_size: 16,
            catch_up_page_size: 16,
            max_reconnect_attempts: Some(10),
            max_auth_attempts: Some(3),
            hello_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_millis(50),
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
        let responses = VecDeque::from_iter((0..5).map(|_| {
            Ok(MockGateway::new(VecDeque::from([Err(anyhow!(
                "reader EOF"
            ))])))
        }));
        let connector = MockConnector::new(sent_hellos.clone(), responses);
        let credentials = CountingCredentialProvider::new("token");
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let handle = supervisor.start();

        // Abort as soon as two fresh credentials have been used; this avoids a
        // timing race between the fixed-duration sleep and mock exhaustion.
        tokio::time::timeout(Duration::from_secs(1), async {
            while sent_hellos.lock().unwrap().len() < 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();

        handle.abort();
        assert!(handle.join().await.is_ok());

        let hellos = sent_hellos.lock().unwrap();
        assert!(
            hellos.len() >= 2,
            "each hello attempt must use a fresh credential"
        );
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
            generation: ProcessGeneration::from_wire(7).unwrap(),
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
        let responses = VecDeque::from_iter((0..5).map(|_| {
            Ok(MockGateway::new(VecDeque::from([Err(anyhow!(
                "reader EOF"
            ))])))
        }));
        let connector = MockConnector::new(sent_hellos.clone(), responses);
        let credentials = CountingCredentialProvider::new("token");
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let handle = supervisor.start();

        tokio::time::timeout(Duration::from_secs(1), async {
            while sent_hellos.lock().unwrap().len() < 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();

        handle.abort();
        assert!(handle.join().await.is_ok());

        assert!(sent_hellos.lock().unwrap().len() >= 2);
    }

    #[tokio::test]
    async fn writer_failure_closes_epoch_and_reconnects() {
        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));

        let gateway1 = MockGateway {
            reader: MockGatewayReader {
                panic: false,
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
                panic: false,
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
            generation: ProcessGeneration::from_wire(7).unwrap(),
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
        assert!(handle.join().await.is_ok());

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
                panic: false,
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
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let handle = supervisor.start();

        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.abort();
        assert!(handle.join().await.is_ok());

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
                panic: false,
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
                panic: false,
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
            generation: ProcessGeneration::from_wire(7).unwrap(),
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
        assert!(handle.join().await.is_ok());

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
                panic: false,
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
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "receipt-1".to_owned(),
        }))
        .unwrap();

        let cmd = tokio::time::timeout(Duration::from_millis(200), handle.commands.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cmd_seq(&cmd), 1);

        handle.abort();
        assert!(handle.join().await.is_ok());
    }

    #[tokio::test]
    async fn oversized_terminal_rejected_is_forwarded_with_digest() {
        let source = MockDurableSource::new(CommandCursors::default());

        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway {
            reader: MockGatewayReader {
                panic: false,
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
            generation: ProcessGeneration::from_wire(7).unwrap(),
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
        assert!(handle.join().await.is_ok());

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
            generation: ProcessGeneration::from_wire(7).unwrap(),
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
            generation: ProcessGeneration::from_wire(7).unwrap(),
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
            generation: ProcessGeneration::from_wire(7).unwrap(),
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
    async fn pre_auth_reconnect_failures_accumulate_and_hit_limit() {
        let mut config = make_config();
        config.max_reconnect_attempts = Some(2);
        config.hello_timeout = Duration::from_millis(5);
        config.initial_backoff = Duration::from_millis(1);

        let credentials = CountingCredentialProvider::new("token");
        let counter = credentials.counter.clone();
        let connector = MockConnector::new(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            VecDeque::from([
                Err(ConnectorError::Other(anyhow!("connect refused"))),
                Err(ConnectorError::Other(anyhow!("connect refused"))),
                Err(ConnectorError::Other(anyhow!("connect refused"))),
            ]),
        );
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor = ConnectionSupervisor::new(connector, credentials, source, latch, config);
        let handle = supervisor.start();

        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(result.is_ok(), "supervisor must stop within bounded time");
        assert!(result.unwrap().is_err());

        let attempts = counter.load(Ordering::SeqCst);
        assert_eq!(
            attempts, 3,
            "pre-auth failures must accumulate and stop after limit"
        );
    }

    #[tokio::test]
    async fn established_epoch_resets_reconnect_failure_streak() {
        let mut config = make_config();
        config.max_reconnect_attempts = Some(1);
        config.hello_timeout = Duration::from_millis(5);
        config.initial_backoff = Duration::from_millis(1);
        config.max_backoff = Duration::from_millis(5);

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
                Ok(MockGateway::new(VecDeque::from([Err(anyhow!(
                    "reader EOF"
                ))]))),
                Ok(MockGateway::new(VecDeque::from([Err(anyhow!(
                    "reader EOF"
                ))]))),
                Err(ConnectorError::Fatal(anyhow!("stop"))),
            ]),
        );
        let credentials = CountingCredentialProvider::new("token");
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor = ConnectionSupervisor::new(connector, credentials, source, latch, config);
        let handle = supervisor.start();

        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(result.is_ok(), "supervisor must stop within bounded time");
        assert!(result.unwrap().is_err());

        assert_eq!(
            sent_hellos.lock().unwrap().len(),
            4,
            "healthy epoch reconnects must not exhaust a finite limit"
        );
    }

    #[tokio::test]
    async fn connect_timeout_rejects_black_hole_and_counts_as_reconnect() {
        let mut config = make_config();
        config.connect_timeout = Duration::from_millis(1);
        config.initial_backoff = Duration::from_millis(1);
        config.max_reconnect_attempts = Some(1);

        let credentials = CountingCredentialProvider::new("token");
        let counter = credentials.counter.clone();
        let connector = MockConnector::new(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            VecDeque::from([
                Ok(MockGateway::new(VecDeque::new())),
                Ok(MockGateway::new(VecDeque::new())),
            ]),
        )
        .with_connect_delay(Duration::from_secs(60));

        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor = ConnectionSupervisor::new(connector, credentials, source, latch, config);
        let handle = supervisor.start();

        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(result.is_ok(), "supervisor must stop on connect timeout");
        assert!(result.unwrap().is_err());

        let attempts = counter.load(Ordering::SeqCst);
        assert_eq!(
            attempts, 2,
            "connect timeouts must consume fresh credentials per attempt"
        );
    }

    #[tokio::test]
    async fn auth_rejected_uses_its_own_limit_independent_of_reconnect() {
        let mut config = make_config();
        config.max_reconnect_attempts = None;
        config.max_auth_attempts = Some(1);
        config.initial_backoff = Duration::from_millis(1);

        let credentials = CountingCredentialProvider::new("token");
        let counter = credentials.counter.clone();
        let connector = MockConnector::new(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            VecDeque::from([
                Err(ConnectorError::AuthRejected),
                Err(ConnectorError::AuthRejected),
                Err(ConnectorError::AuthRejected),
            ]),
        );
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor = ConnectionSupervisor::new(connector, credentials, source, latch, config);
        let handle = supervisor.start();

        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(result.is_ok(), "supervisor must stop within bounded time");
        let err = result.unwrap().expect_err("auth limit must fail");
        assert!(format!("{err:#}").contains("max auth attempts"));

        let attempts = counter.load(Ordering::SeqCst);
        assert_eq!(
            attempts, 2,
            "auth limit should stop after max_auth_attempts + 1"
        );
    }

    #[tokio::test]
    async fn reader_panic_is_fatal_and_propagates() {
        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway {
            reader: MockGatewayReader {
                panic: true,
                commands: VecDeque::new(),
            },
            writer: MockGatewayWriter {
                fail_after: None,
                sent: Arc::new(std::sync::Mutex::new(Vec::new())),
                delay: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
        };
        let connector = MockConnector::new(sent_hellos, VecDeque::from([Ok(gateway)]));
        let credentials = CountingCredentialProvider::new("token");
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let handle = supervisor.start();

        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(result.is_ok(), "supervisor must stop on reader panic");
        assert!(result.unwrap().is_err());
    }

    #[tokio::test]
    async fn abort_propagates_to_child_tasks_and_join_returns_ok() {
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway {
            reader: MockGatewayReader {
                panic: false,
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
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let mut handle = supervisor.start();

        // Wait for the epoch to be installed and then abort.
        tokio::time::timeout(Duration::from_millis(200), handle.epochs.changed())
            .await
            .unwrap()
            .unwrap();

        handle.abort();
        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(
            result.is_ok(),
            "abort must stop the supervisor within bounded time"
        );
        assert!(
            result.unwrap().is_ok(),
            "abort must produce a clean shutdown"
        );
    }

    #[tokio::test]
    async fn validate_hello_refetches_command_cursors() {
        // Simulate the durable source advancing between build_agent_hello and validate_hello.
        let cursor_calls = VecDeque::from([
            CommandCursors {
                received: 5,
                applied: 5,
            },
            CommandCursors {
                received: 10,
                applied: 5,
            },
        ]);

        struct CursorsSource {
            event_cursor_value: Arc<AtomicU64>,
            cursors: Mutex<VecDeque<CommandCursors>>,
        }

        impl CursorsSource {
            fn new(cursors: VecDeque<CommandCursors>) -> Self {
                Self {
                    event_cursor_value: Arc::new(AtomicU64::new(0)),
                    cursors: Mutex::new(cursors),
                }
            }
        }

        #[async_trait]
        impl DurableSource for CursorsSource {
            async fn event_cursor(&self) -> Result<EventCursors> {
                Ok(EventCursors {
                    last_sent: self.event_cursor_value.load(Ordering::SeqCst),
                })
            }

            async fn events_after(
                &self,
                _after_seq: u64,
                _limit: usize,
            ) -> Result<Vec<OutboundFrame>> {
                Ok(Vec::new())
            }

            async fn command_cursors(&self) -> Result<CommandCursors> {
                Ok(self.cursors.lock().unwrap().pop_front().unwrap())
            }
        }

        impl Clone for CursorsSource {
            fn clone(&self) -> Self {
                Self {
                    event_cursor_value: self.event_cursor_value.clone(),
                    cursors: Mutex::new(self.cursors.lock().unwrap().clone()),
                }
            }
        }

        let source = CursorsSource::new(cursor_calls);
        let agent = build_agent_hello(&source, &make_config()).await.unwrap();
        assert_eq!(agent.last_received_command_seq, 5);

        let api = ApiHello {
            accepted_generation: ProcessGeneration::from_wire(7).unwrap(),
            last_received_event_seq: 0,
            next_command_seq: 11,
        };

        // If validate_hello used the stale agent snapshot, next_command_seq=11
        // would be > 5 + 1 and fail. With re-fetched cursors (received=10),
        // 11 > 10 + 1 is false, so validation succeeds.
        validate_hello(&source, &agent, &api).await.unwrap();
    }

    #[test]
    fn hello_generation_out_of_range_rejects_on_wire() {
        let oversized = i64::MAX as u64 + 1;
        let api_json = format!(
            r#"{{"accepted_generation":{oversized},"last_received_event_seq":0,"next_command_seq":1}}"#
        );
        assert!(serde_json::from_str::<ApiHello>(&api_json).is_err());

        let agent_json = format!(
            r#"{{"agent_id":"x","generation":{oversized},"last_sent_event_seq":0,"last_received_command_seq":0,"last_applied_command_seq":0}}"#
        );
        assert!(serde_json::from_str::<AgentHello>(&agent_json).is_err());
    }

    fn cmd_seq(cmd: &InboundCommand) -> u64 {
        inbound_command_seq(cmd)
    }
}
