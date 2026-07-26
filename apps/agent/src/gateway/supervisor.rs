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
use tokio::sync::{Notify, mpsc, watch};
use tokio::task::{JoinError, JoinHandle};
use tokio::time;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroize;

use crate::runtime::contracts::ProcessGeneration;

use super::{Gateway, GatewayReader, GatewayWriter, HelloError, InboundCommand, OutboundFrame};

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
#[serde(deny_unknown_fields)]
pub struct AgentHello {
    pub agent_id: String,
    pub generation: ProcessGeneration,
    pub last_sent_event_seq: u64,
    pub last_received_command_seq: u64,
    pub last_applied_command_seq: u64,
}

/// API → Agent hello response.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    #[error("invalid connector configuration: {0}")]
    InvalidConfiguration(anyhow::Error),
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
    /// True once the current epoch has caught up to the durable event cursor.
    pub online: watch::Receiver<bool>,
    cancel: CancellationToken,
    task: Option<JoinHandle<Result<()>>>,
}

impl SupervisorHandle {
    pub fn abort(&self) {
        self.cancel.cancel();
    }

    pub async fn join(mut self) -> Result<()> {
        let task = self
            .task
            .take()
            .context("supervisor task was already consumed")?;
        task.await?
    }
}

impl Drop for SupervisorHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
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
    /// Broadcasts the current epoch's online state. False while catching up,
    /// true once the durable cursor has been reached, false again on epoch end.
    online: Arc<watch::Sender<bool>>,
    cancel: CancellationToken,
    /// Test-only notification fired when `send_validated` is about to block on
    /// a full command channel.
    command_send_blocked_notify: Option<Arc<Notify>>,
    /// Test-only notification fired when `event_forwarder` is about to block on
    /// a full writer channel.
    writer_send_blocked_notify: Option<Arc<Notify>>,
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
        let (online_tx, _) = watch::channel(false);
        Self {
            connector,
            credentials,
            source,
            latch,
            config,
            epoch_counter: Arc::new(AtomicU64::new(0)),
            current_writer: Arc::new(std::sync::Mutex::new(None)),
            current_epoch,
            online: Arc::new(online_tx),
            cancel: CancellationToken::new(),
            command_send_blocked_notify: None,
            writer_send_blocked_notify: None,
        }
    }

    pub(crate) fn with_command_send_blocked_notify(mut self, notify: Arc<Notify>) -> Self {
        self.command_send_blocked_notify = Some(notify);
        self
    }

    pub(crate) fn with_writer_send_blocked_notify(mut self, notify: Arc<Notify>) -> Self {
        self.writer_send_blocked_notify = Some(notify);
        self
    }

    /// Start the supervisor and return channels for commands, events, and epoch
    /// observation. `events` must carry the current `DeliveryEpoch` from
    /// `handle.epochs`.
    pub fn start(self) -> SupervisorHandle {
        let (commands_tx, commands_rx) = mpsc::channel(self.config.command_buffer_size);
        let (events_tx, events_rx) = mpsc::channel(self.config.event_buffer_size);
        let epochs_rx = self.current_epoch.subscribe();
        let online_rx = self.online.subscribe();
        let cancel = self.cancel.clone();
        let task = tokio::spawn(self.run(commands_tx, events_rx));
        SupervisorHandle {
            commands: commands_rx,
            events: events_tx,
            epochs: epochs_rx,
            online: online_rx,
            cancel,
            task: Some(task),
        }
    }

    pub async fn run(
        mut self,
        commands_tx: mpsc::Sender<InboundCommand>,
        events_rx: mpsc::Receiver<(DeliveryEpoch, OutboundFrame)>,
    ) -> Result<()> {
        let current_writer = self.current_writer.clone();
        let cancel = self.cancel.clone();
        let online_rx = self.online.subscribe();
        let writer_send_blocked_notify = self.writer_send_blocked_notify.clone();
        let forwarder = tokio::spawn(event_forwarder(
            events_rx,
            current_writer,
            cancel,
            online_rx,
            writer_send_blocked_notify,
        ));

        // run_loop owns all per-epoch cancellation and cleanup; await it so that
        // connect_and_run_epoch gets a chance to publish Online=false and clear
        // the current writer/epoch before the supervisor task exits.
        let result = self.run_loop(commands_tx).await;

        forwarder.abort();
        if let Err(join_err) = forwarder.await
            && let Ok(panic) = join_err.try_into_panic()
        {
            std::panic::resume_unwind(panic);
        }
        // Normal abort cancellation is intentionally not an error.
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
                    if auth_attempt >= max {
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
                        && reconnect_attempt >= max
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
                    // A successful hello resets the auth streak, but the post-hello
                    // failure is a reconnect like any other; do not reset the streak
                    // merely because the hello succeeded. There is currently no
                    // observable healthy-epoch boundary in this state machine, so a
                    // clean (Ok, Ok) epoch end is also treated as a reconnect.
                    auth_attempt = 0;
                    reconnect_attempt = reconnect_attempt.saturating_add(1);
                    if let Some(max) = self.config.max_reconnect_attempts
                        && reconnect_attempt >= max
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
        // Every new epoch starts offline; writer_task publishes online once catch-up
        // reaches the durable cursor, and cleanup resets it on epoch end.
        let _ = self.online.send(false);

        let source = self.source.clone();
        let config = self.config.clone();
        let cancel = self.cancel.clone();
        let online = self.online.clone();

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
                Ok(Err(ConnectorError::Fatal(e) | ConnectorError::InvalidConfiguration(e))) => return Err(SupervisorError::Fatal(e)),
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
                Ok(Err(HelloError::AuthRejected)) => return Err(SupervisorError::AuthRejected),
                Ok(Err(HelloError::Fatal(e))) => return Err(SupervisorError::Fatal(e)),
                Ok(Err(HelloError::Reconnect(e))) => return Err(SupervisorError::Reconnect {
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

        let command_send_blocked_notify = self.command_send_blocked_notify.clone();
        let mut reader_handle = tokio::spawn(reader_task(
            reader,
            commands_tx,
            self.latch.clone(),
            api_hello.clone(),
            epoch_token.child_token(),
            command_send_blocked_notify,
        ));

        let mut writer_handle = tokio::spawn(writer_task(
            writer,
            writer_rx,
            source,
            api_hello,
            config,
            online,
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
                let _ = self.online.send(false);
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
                    (Ok(()), Ok(())) => Ok(()),
                    (Err(e), _) | (_, Err(e)) => Err(SupervisorError::EstablishedReconnect {
                        reason: format!("epoch task error during cancel: {e}"),
                    }),
                }
            }
            reader_result = &mut reader_handle => {
                epoch_token.cancel();
                *self.current_writer.lock().unwrap() = None;
                self.current_epoch.send_replace(None);
                let _ = self.online.send(false);
                Self::inspect_epoch_results(reader_result, writer_handle.await)
            }
            writer_result = &mut writer_handle => {
                epoch_token.cancel();
                *self.current_writer.lock().unwrap() = None;
                self.current_epoch.send_replace(None);
                let _ = self.online.send(false);
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

    fn backoff_window_ms(config: &SupervisorConfig, attempt: u32) -> (u64, u64) {
        let base_ms = config.initial_backoff.as_millis() as u64;
        let max_ms = config.max_backoff.as_millis() as u64;
        let shift = attempt.saturating_sub(1).min(31);
        let delay_ms = base_ms
            .saturating_mul(2u64.saturating_pow(shift))
            .min(max_ms);
        ((delay_ms.saturating_add(1)) / 2, delay_ms)
    }

    async fn backoff_sleep(config: &SupervisorConfig, attempt: u32) -> Result<()> {
        let (lower_ms, upper_ms) = Self::backoff_window_ms(config, attempt);
        let jitter = if upper_ms == 0 {
            0
        } else {
            rand::rng().random_range(lower_ms..=upper_ms)
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
    // A failed read is transient and ends the current attempt as reconnectable;
    // a successfully read cursor that disproves the peer's claim is fatal.
    let event_cursor = source
        .event_cursor()
        .await
        .map_err(|e| SupervisorError::Reconnect {
            reason: format!("event cursor unavailable for hello validation: {e}"),
        })?;
    if api.last_received_event_seq > event_cursor.last_sent {
        return Err(SupervisorError::Fatal(anyhow!(
            "API claims event seq {} beyond durable cursor {}",
            api.last_received_event_seq,
            event_cursor.last_sent
        )));
    }

    let command_cursor =
        source
            .command_cursors()
            .await
            .map_err(|e| SupervisorError::Reconnect {
                reason: format!("command cursor unavailable for hello validation: {e}"),
            })?;
    let min_next_command_seq = command_cursor.applied.saturating_add(1);
    let max_next_command_seq = command_cursor.received.saturating_add(1);
    if !(min_next_command_seq..=max_next_command_seq).contains(&api.next_command_seq) {
        return Err(SupervisorError::Fatal(anyhow!(
            "command cursor claim outside durable bounds: next_command_seq {} not in {}..={}; applied={}, received={}",
            api.next_command_seq,
            min_next_command_seq,
            max_next_command_seq,
            command_cursor.applied,
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
    online: watch::Receiver<bool>,
    writer_send_blocked_notify: Option<Arc<Notify>>,
) {
    loop {
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            result = event_rx.recv() => result,
        };
        let Some((epoch, frame)) = result else {
            break;
        };

        let sender = {
            let guard = current_writer.lock().unwrap();
            guard.as_ref().and_then(|(current_epoch, sender)| {
                if *current_epoch == epoch {
                    Some(sender.clone())
                } else {
                    None
                }
            })
        };
        let Some(sender) = sender else {
            continue;
        };
        // Volatile/delta Events (no seq) that arrive before the epoch has caught
        // up are stale; drop them. Durable Events (seq present) are held in the
        // writer channel so writer_task can deduplicate them against the durable
        // cursor after Online. CommandAck frames are terminal command feedback
        // and must be delivered even while catch-up is in progress.
        if !*online.borrow()
            && let OutboundFrame::Event { envelope } = &frame
            && envelope.seq.is_none()
        {
            continue;
        }
        if let Some(notify) = writer_send_blocked_notify.as_ref()
            && sender.capacity() == 0
        {
            notify.notify_one();
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            result = sender.send(frame) => {
                if result.is_err() {
                    // Writer closed; stale frame is dropped. The supervisor will
                    // install a new epoch and catch-up from the durable source.
                }
            }
        }
    }
}

async fn writer_task<W, S>(
    mut writer: W,
    mut writer_rx: mpsc::Receiver<OutboundFrame>,
    source: S,
    api_hello: ApiHello,
    config: SupervisorConfig,
    online: Arc<watch::Sender<bool>>,
    token: CancellationToken,
) -> Result<()>
where
    W: GatewayWriter,
    S: DurableSource,
{
    let mut last_received = api_hello.last_received_event_seq;
    let mut cursor = tokio::select! {
        biased;
        _ = token.cancelled() => return Ok(()),
        result = source.event_cursor() => result?,
    };

    while last_received < cursor.last_sent {
        let events = tokio::select! {
            biased;
            _ = token.cancelled() => return Ok(()),
            result = source.events_after(last_received, config.catch_up_page_size) => result?,
        };
        if events.is_empty() {
            // The cursor may have raced ahead of the events_after snapshot.
            // Re-check before treating an empty page as fatal.
            cursor = tokio::select! {
                biased;
                _ = token.cancelled() => return Ok(()),
                result = source.event_cursor() => result?,
            };
            if cursor.last_sent > last_received {
                continue;
            }
            bail!("event source returned empty page before cursor");
        }
        for frame in events {
            let seq =
                outbound_frame_event_seq(&frame).context("catch-up frame missing durable seq")?;
            if seq <= last_received {
                bail!("non-monotonic catch-up event: seq {seq} after {last_received}");
            }
            tokio::select! {
                biased;
                _ = token.cancelled() => return Ok(()),
                result = send_with_timeout(&mut writer, frame, config.send_timeout) => result?,
            }
            last_received = seq;
        }
        // Refresh the cursor each page so commits racing catch-up are included.
        cursor = tokio::select! {
            biased;
            _ = token.cancelled() => return Ok(()),
            result = source.event_cursor() => result?,
        };
    }

    // Final cursor recheck right before publishing Online: catch any durable
    // commits that happened while the last page was being sent.
    loop {
        let fresh_cursor = tokio::select! {
            biased;
            _ = token.cancelled() => return Ok(()),
            result = source.event_cursor() => result?,
        };
        if fresh_cursor.last_sent <= last_received {
            break;
        }
        let events = tokio::select! {
            biased;
            _ = token.cancelled() => return Ok(()),
            result = source.events_after(last_received, config.catch_up_page_size) => result?,
        };
        if events.is_empty() {
            cursor = tokio::select! {
                biased;
                _ = token.cancelled() => return Ok(()),
                result = source.event_cursor() => result?,
            };
            if cursor.last_sent > last_received {
                continue;
            }
            bail!("event source returned empty page before cursor");
        }
        for frame in events {
            let seq =
                outbound_frame_event_seq(&frame).context("catch-up frame missing durable seq")?;
            if seq <= last_received {
                bail!("non-monotonic catch-up event: seq {seq} after {last_received}");
            }
            tokio::select! {
                biased;
                _ = token.cancelled() => return Ok(()),
                result = send_with_timeout(&mut writer, frame, config.send_timeout) => result?,
            }
            last_received = seq;
        }
    }

    // Publish Online only after reaching the durable cursor. From this point on
    // event_forwarder may deliver live frames to this writer.
    let _ = online.send(true);

    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => return Ok(()),
            frame = writer_rx.recv() => {
                let Some(frame) = frame else { return Ok(()); };

                let event_seq = if let OutboundFrame::Event { envelope } = &frame {
                    envelope.seq
                } else {
                    None
                };
                if let Some(seq) = event_seq
                    && seq <= last_received
                {
                    // Already delivered by the durable catch-up; the live
                    // producer raced the Online boundary.
                    continue;
                }

                tokio::select! {
                    biased;
                    _ = token.cancelled() => return Ok(()),
                    result = send_with_timeout(&mut writer, frame, config.send_timeout) => {
                        result?;
                        if let Some(seq) = event_seq {
                            last_received = seq;
                        }
                    }
                }
            }
        }
    }
}

async fn send_with_timeout<W>(writer: &mut W, frame: OutboundFrame, timeout: Duration) -> Result<()>
where
    W: GatewayWriter,
{
    match time::timeout(timeout, writer.send(frame)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => bail!("gateway send timeout"),
    }
}

async fn reader_task<R, L>(
    reader: R,
    mut command_tx: mpsc::Sender<InboundCommand>,
    latch: L,
    api_hello: ApiHello,
    token: CancellationToken,
    command_send_blocked_notify: Option<Arc<Notify>>,
) -> Result<()>
where
    R: GatewayReader + 'static,
    L: HydrationLatch,
{
    const MAX_PENDING_BEFORE_READY: usize = 16;

    // Run command reception in its own task so hydration completion never cancels
    // an in-flight read across transport chunk boundaries.
    let (cmd_tx, mut cmd_rx) = mpsc::channel(MAX_PENDING_BEFORE_READY);
    let cmd_token = token.child_token();
    let command_reader = tokio::spawn(async move {
        let mut reader = reader;
        loop {
            tokio::select! {
                biased;
                _ = cmd_token.cancelled() => break,
                cmd = reader.next_command() => {
                    let is_err = cmd.is_err();
                    let send = cmd_tx.send(cmd);
                    tokio::select! {
                        biased;
                        _ = cmd_token.cancelled() => break,
                        result = send => {
                            if result.is_err() {
                                break;
                            }
                        }
                    }
                    if is_err {
                        break;
                    }
                }
            }
        }
    });

    let mut ready: Option<HydrationReady> = None;
    let mut pending: Vec<InboundCommand> = Vec::with_capacity(MAX_PENDING_BEFORE_READY);
    let mut next_expected = api_hello.next_command_seq;

    let result: Result<()> = 'task: {
        loop {
            tokio::select! {
                biased;
                _ = token.cancelled() => break 'task Ok(()),
                result = latch.wait_for(api_hello.accepted_generation), if ready.is_none() => {
                    let hydration_ready = result?;
                    if hydration_ready.generation != api_hello.accepted_generation {
                        break 'task Err(anyhow!("hydration generation mismatch"));
                    }
                    ready = Some(hydration_ready);
                    for cmd in pending.drain(..) {
                        next_expected = send_validated(cmd, next_expected, &mut command_tx, &token, command_send_blocked_notify.clone()).await?;
                    }
                }
                result = cmd_rx.recv() => {
                    match result {
                        Some(Ok(cmd)) => {
                            if ready.is_some() {
                                next_expected = send_validated(cmd, next_expected, &mut command_tx, &token, command_send_blocked_notify.clone()).await?;
                            } else if pending.len() < MAX_PENDING_BEFORE_READY {
                                pending.push(cmd);
                            } else {
                                break 'task Err(anyhow!("max pending commands before hydration reached"));
                            }
                        }
                        Some(Err(e)) => break 'task Err(e),
                        None => break 'task Err(anyhow!("command reader closed unexpectedly")),
                    }
                }
            }
        }
    };

    command_reader.abort();
    match command_reader.await {
        Ok(()) => result,
        Err(join_err) if join_err.is_panic() => std::panic::resume_unwind(join_err.into_panic()),
        // The command reader was aborted by this task; return the real result
        // instead of manufacturing an error from the join failure.
        Err(_) => result,
    }
}

async fn send_validated(
    cmd: InboundCommand,
    next_expected: u64,
    command_tx: &mut mpsc::Sender<InboundCommand>,
    token: &CancellationToken,
    blocked_notify: Option<Arc<Notify>>,
) -> Result<u64> {
    let seq = inbound_command_seq(&cmd);
    if seq > next_expected {
        bail!("command seq gap: expected {next_expected}, got {seq}");
    }
    if !token.is_cancelled()
        && let Some(notify) = blocked_notify.as_ref()
        && command_tx.capacity() == 0
    {
        notify.notify_one();
    }
    tokio::select! {
        biased;
        _ = token.cancelled() => Ok(next_expected),
        result = command_tx.send(cmd) => {
            if result.is_err() {
                bail!("command consumer closed");
            }
            // seq < next_expected is a legitimate retransmission; the durable consumer
            // (EventWriter) deduplicates by command_id and re-ACKs the same canonical seq.
            if seq == next_expected {
                Ok(next_expected.saturating_add(1))
            } else {
                Ok(next_expected)
            }
        }
    }
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
    observed: Arc<std::sync::Mutex<Option<HydrationReady>>>,
}

impl WatchHydrationLatch {
    pub fn new(rx: watch::Receiver<HydrationState>) -> Self {
        Self {
            rx,
            observed: Arc::new(std::sync::Mutex::new(None)),
        }
    }
}

#[async_trait]
impl HydrationLatch for WatchHydrationLatch {
    async fn wait_for(&self, generation: ProcessGeneration) -> Result<HydrationReady> {
        let mut rx = self.rx.clone();
        loop {
            let state = rx.borrow().clone();
            match state {
                HydrationState::Ready(ready) if ready.generation == generation => {
                    let mut observed = self.observed.lock().unwrap();
                    if let Some(observed_ready) = observed.as_ref() {
                        if observed_ready.generation == generation
                            && observed_ready.receipt_identity != ready.receipt_identity
                        {
                            bail!(
                                "hydration identity changed for generation {generation}: expected {}, got {}",
                                observed_ready.receipt_identity,
                                ready.receipt_identity
                            );
                        }
                        if observed_ready.generation == generation {
                            return Ok(observed_ready.clone());
                        }
                    }
                    *observed = Some(ready.clone());
                    return Ok(ready);
                }
                HydrationState::Ready(ready) => {
                    bail!(
                        "hydration ready for different generation: expected {generation}, got {}",
                        ready.generation
                    )
                }
                HydrationState::NotReady => {
                    drop(state);
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
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use anyhow::{Result, anyhow};
    use sha2::{Digest, Sha256};
    use tokio::sync::watch;

    use super::*;
    use crate::gateway::stdio::SingleConnectionConnector;
    use crate::gateway::wire::to_wire_frame;
    use crate::gateway::{
        Command, CommandAck, CommandAckStatus, CommandEnvelope, CommandId, CommandRejectReason,
        Envelope, Gateway, GatewayReader, GatewayWriter, InboundCommand, OutboundFrame,
    };

    struct TestDigestFactory;

    impl crate::gateway::CommandDigestFactory for TestDigestFactory {
        fn start(&self) -> Box<dyn crate::gateway::IncrementalCommandDigest> {
            Box::new(TestDigest(Sha256::new()))
        }
    }

    struct TestDigest(Sha256);

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    impl crate::gateway::IncrementalCommandDigest for TestDigest {
        fn update(&mut self, data: &[u8]) {
            self.0.update(data);
        }

        fn finish(self: Box<Self>) -> crate::gateway::KeyedCommandDigest {
            let hash = self.0.finalize();
            let mut hmac = [0u8; 32];
            hmac.copy_from_slice(&hash[..32.min(hash.len())]);
            crate::gateway::KeyedCommandDigest::new("test", hmac)
        }
    }

    #[derive(Clone)]
    struct DelayedCatchUpSource {
        events: Arc<std::sync::Mutex<VecDeque<OutboundFrame>>>,
        notify: Arc<tokio::sync::Notify>,
        command_cursor: CommandCursors,
    }

    #[async_trait]
    impl DurableSource for DelayedCatchUpSource {
        async fn event_cursor(&self) -> Result<EventCursors> {
            let events = self.events.lock().unwrap();
            let last_sent = events
                .back()
                .map_or(0, |f| outbound_frame_event_seq(f).unwrap_or(0));
            Ok(EventCursors { last_sent })
        }

        async fn events_after(&self, after_seq: u64, _limit: usize) -> Result<Vec<OutboundFrame>> {
            self.notify.notified().await;
            let events = self.events.lock().unwrap();
            Ok(events
                .iter()
                .filter(|f| outbound_frame_event_seq(f).unwrap() > after_seq)
                .cloned()
                .collect())
        }

        async fn command_cursors(&self) -> Result<CommandCursors> {
            Ok(self.command_cursor)
        }
    }

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
        tokens: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl CountingCredentialProvider {
        fn new(prefix: impl Into<String>) -> Self {
            Self {
                counter: Arc::new(AtomicU64::new(0)),
                prefix: prefix.into(),
                tokens: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl CredentialProvider for CountingCredentialProvider {
        async fn fresh_credential(&mut self) -> Result<GatewayCredential> {
            let n = self.counter.fetch_add(1, Ordering::SeqCst);
            let token = format!("{}-{}", self.prefix, n);
            self.tokens.lock().unwrap().push(token.clone());
            Ok(GatewayCredential::new(token))
        }
    }

    struct MockGatewayReader {
        commands: VecDeque<Result<InboundCommand>>,
        panic: bool,
        on_empty: Option<Arc<tokio::sync::Notify>>,
    }

    impl MockGatewayReader {
        fn with_panic(mut self) -> Self {
            self.panic = true;
            self
        }

        fn notify_on_empty(mut self, notify: Arc<tokio::sync::Notify>) -> Self {
            self.on_empty = Some(notify);
            self
        }
    }

    struct MockGatewayWriter {
        fail_after: Option<usize>,
        sent: Arc<std::sync::Mutex<Vec<OutboundFrame>>>,
        delay: Option<Duration>,
        /// Number of successfully sent frames after which the writer blocks.
        block_after: Option<usize>,
        /// Notified when `block_after` is reached, then waits forever.
        block_notify: Option<Arc<Notify>>,
    }

    impl MockGatewayWriter {
        fn with_block_after(mut self, n: usize, notify: Arc<Notify>) -> Self {
            self.block_after = Some(n);
            self.block_notify = Some(notify);
            self
        }
    }

    #[async_trait]
    impl GatewayReader for MockGatewayReader {
        async fn next_command(&mut self) -> Result<InboundCommand> {
            if self.panic {
                panic!("mock reader panic");
            }
            let result = match self.commands.pop_front() {
                Some(Ok(cmd)) => Ok(cmd),
                Some(Err(e)) => Err(e),
                None => std::future::pending::<Result<InboundCommand>>().await,
            };
            if self.commands.is_empty()
                && let Some(notify) = self.on_empty.as_ref()
            {
                notify.notify_one();
            }
            result
        }
    }

    #[async_trait]
    impl GatewayWriter for MockGatewayWriter {
        async fn send(&mut self, frame: OutboundFrame) -> Result<()> {
            if let Some(d) = self.delay {
                tokio::time::sleep(d).await;
            }
            let (blocked, block_notify) = {
                let mut sent = self.sent.lock().unwrap();
                if let Some(n) = self.fail_after
                    && sent.len() >= n
                {
                    bail!("writer failure");
                }
                sent.push(frame);
                let blocked = self.block_after.map_or(false, |n| sent.len() == n);
                (blocked, self.block_notify.clone())
            };
            if blocked {
                if let Some(notify) = block_notify {
                    notify.notify_one();
                }
                std::future::pending::<()>().await;
            }
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
        hello_error: Option<HelloError>,
    }

    impl MockGateway {
        fn new(commands: VecDeque<Result<InboundCommand>>) -> Self {
            Self {
                reader: MockGatewayReader {
                    commands,
                    panic: false,
                    on_empty: None,
                },
                writer: MockGatewayWriter {
                    fail_after: None,
                    sent: Arc::new(std::sync::Mutex::new(Vec::new())),
                    delay: None,
                    block_after: None,
                    block_notify: None,
                },
                sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
                hello_generation: None,
                last_received_event_seq: 0,
                hello_delay: None,
                hello_error: None,
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

        fn with_hello_error(mut self, err: HelloError) -> Self {
            self.hello_error = Some(err);
            self
        }

        fn with_notify_on_empty(mut self, notify: Arc<tokio::sync::Notify>) -> Self {
            self.reader = self.reader.notify_on_empty(notify);
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

        async fn authenticate_hello(
            &mut self,
            hello: AgentHello,
        ) -> std::result::Result<ApiHello, HelloError> {
            self.sent_hellos.lock().unwrap().push(hello.clone());
            if let Some(delay) = self.hello_delay {
                tokio::time::sleep(delay).await;
            }
            if let Some(err) = self.hello_error.take() {
                return Err(err);
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
            ConnectionSupervisor::new(connector, credentials.clone(), source, latch, make_config());
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

        let tokens = credentials.tokens.lock().unwrap();
        assert!(
            tokens.len() >= 2,
            "each connection attempt must fetch a fresh credential"
        );
        assert_ne!(tokens[0], tokens[1], "successive credentials must differ");
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
                on_empty: None,
                commands: VecDeque::from([Ok(valid_command(
                    1,
                    "00000000-0000-4000-8000-000000000001",
                ))]),
            },
            writer: MockGatewayWriter {
                fail_after: Some(0),
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
        };
        let gateway2 = MockGateway {
            reader: MockGatewayReader {
                panic: false,
                on_empty: None,
                commands: VecDeque::new(),
            },
            writer: MockGatewayWriter {
                fail_after: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
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
                on_empty: None,
                commands: VecDeque::new(),
            },
            writer: MockGatewayWriter {
                fail_after: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
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
                on_empty: None,
                commands: VecDeque::new(),
            },
            writer: MockGatewayWriter {
                fail_after: Some(0),
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
        };
        let gateway2 = MockGateway {
            reader: MockGatewayReader {
                panic: false,
                on_empty: None,
                commands: VecDeque::new(),
            },
            writer: MockGatewayWriter {
                fail_after: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
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
                on_empty: None,
                commands: VecDeque::from([Ok(valid_command(
                    1,
                    "00000000-0000-4000-8000-000000000001",
                ))]),
            },
            writer: MockGatewayWriter {
                fail_after: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
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
                on_empty: None,
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
                block_after: None,
                block_notify: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
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
            &CancellationToken::new(),
            None,
        )
        .await;
        assert!(result.is_err());
        assert!(format!("{:#}", result.unwrap_err()).contains("command consumer closed"));
    }

    #[tokio::test]
    async fn send_validated_saturates_at_maximum_command_sequence() {
        let (mut tx, mut rx) = mpsc::channel::<InboundCommand>(1);
        let next = send_validated(
            valid_command(u64::MAX, "00000000-0000-4000-8000-000000000001"),
            u64::MAX,
            &mut tx,
            &CancellationToken::new(),
            None,
        )
        .await
        .expect("maximum command sequence is accepted");
        assert_eq!(next, u64::MAX);
        assert_eq!(inbound_command_seq(&rx.recv().await.unwrap()), u64::MAX);
    }

    #[tokio::test]
    async fn dropping_supervisor_handle_cancels_and_aborts_task() {
        let (_commands_tx, commands) = mpsc::channel(1);
        let (events, _events_rx) = mpsc::channel(1);
        let (_epochs_tx, epochs) = watch::channel(None);
        let (_online_tx, online) = watch::channel(false);
        let cancel = CancellationToken::new();
        let observed_cancel = cancel.clone();
        let task_dropped = Arc::new(AtomicBool::new(false));
        let drop_flag = DropFlag(task_dropped.clone());
        let task = tokio::spawn(async move {
            let _drop_flag = drop_flag;
            std::future::pending::<Result<()>>().await
        });
        tokio::task::yield_now().await;

        drop(SupervisorHandle {
            commands,
            events,
            epochs,
            online,
            cancel,
            task: Some(task),
        });

        assert!(observed_cancel.is_cancelled());
        tokio::time::timeout(Duration::from_millis(200), async {
            while !task_dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("aborted supervisor task must be dropped");
    }

    #[tokio::test]
    async fn duplicate_command_retransmission_is_accepted() {
        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway::new(VecDeque::from([
            Ok(valid_command(1, "00000000-0000-4000-8000-000000000001")),
            Ok(valid_command(1, "00000000-0000-4000-8000-000000000001")),
            Ok(valid_command(2, "00000000-0000-4000-8000-000000000002")),
        ]));
        let connector = MockConnector::new(sent_hellos.clone(), VecDeque::from([Ok(gateway)]));
        let credentials = CountingCredentialProvider::new("token");
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let mut handle = supervisor.start();

        let first = tokio::time::timeout(Duration::from_millis(200), handle.commands.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cmd_seq(&first), 1);

        let duplicate = tokio::time::timeout(Duration::from_millis(200), handle.commands.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cmd_seq(&duplicate), 1);

        let next = tokio::time::timeout(Duration::from_millis(200), handle.commands.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cmd_seq(&next), 2);

        handle.abort();
        assert!(handle.join().await.is_ok());
    }

    #[tokio::test]
    async fn command_seq_gap_terminates_epoch_and_respects_reconnect_limit() {
        let mut config = make_config();
        config.max_reconnect_attempts = Some(1);

        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway::new(VecDeque::from([
            Ok(valid_command(1, "00000000-0000-4000-8000-000000000001")),
            Ok(valid_command(3, "00000000-0000-4000-8000-000000000003")),
        ]));
        let connector = MockConnector::new(sent_hellos.clone(), VecDeque::from([Ok(gateway)]));
        let credentials = CountingCredentialProvider::new("token");
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor = ConnectionSupervisor::new(connector, credentials, source, latch, config);
        let mut handle = supervisor.start();

        let first = tokio::time::timeout(Duration::from_millis(200), handle.commands.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cmd_seq(&first), 1);

        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(result.is_ok(), "supervisor must stop after gap");
        assert!(result.unwrap().is_err());

        assert_eq!(sent_hellos.lock().unwrap().len(), 1);
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
        assert_eq!(
            attempts, 3,
            "auth retries must be bounded by the default max_auth_attempts"
        );
    }

    #[tokio::test]
    async fn post_hello_auth_rejected_uses_max_auth_attempts() {
        // The HTTP upgrade succeeds, but the server closes during the hello exchange.
        // This must be classified as AuthRejected and bounded by max_auth_attempts,
        // not treated as an unlimited reconnect loop.
        let mut config = make_config();
        config.max_reconnect_attempts = None;
        config.hello_timeout = Duration::from_millis(5);
        config.initial_backoff = Duration::from_millis(1);

        let credentials = CountingCredentialProvider::new("token");
        let counter = credentials.counter.clone();
        let connector = MockConnector::new(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            VecDeque::from([
                Ok(MockGateway::new(VecDeque::new()).with_hello_error(HelloError::AuthRejected)),
                Ok(MockGateway::new(VecDeque::new()).with_hello_error(HelloError::AuthRejected)),
                Ok(MockGateway::new(VecDeque::new()).with_hello_error(HelloError::AuthRejected)),
                Ok(MockGateway::new(VecDeque::new()).with_hello_error(HelloError::AuthRejected)),
                Ok(MockGateway::new(VecDeque::new()).with_hello_error(HelloError::AuthRejected)),
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
            "supervisor must stop within bounded time when post-hello auth is rejected"
        );
        assert!(result.unwrap().is_err());

        let attempts = counter.load(Ordering::SeqCst);
        assert_eq!(
            attempts, 3,
            "post-hello auth rejections must be bounded by max_auth_attempts"
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
        config.max_reconnect_attempts = Some(2);

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
    async fn backoff_jitter_lower_bound_grows_with_attempt() {
        let mut config = make_config();
        config.initial_backoff = Duration::from_millis(20);
        config.max_backoff = Duration::from_millis(80);

        assert_eq!(
            ConnectionSupervisor::<
                MockConnector,
                CountingCredentialProvider,
                MockDurableSource,
                StaticHydrationLatch,
            >::backoff_window_ms(&config, 1),
            (10, 20)
        );
        assert_eq!(
            ConnectionSupervisor::<
                MockConnector,
                CountingCredentialProvider,
                MockDurableSource,
                StaticHydrationLatch,
            >::backoff_window_ms(&config, 3),
            (40, 80)
        );

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
            elapsed >= Duration::from_millis(10),
            "jitter must preserve the attempt's exponential lower bound, elapsed {elapsed:?}"
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
            attempts, 2,
            "pre-auth failures must accumulate and stop at the configured limit"
        );
    }

    #[tokio::test]
    async fn connector_configuration_failure_is_fatal_without_retry() {
        let mut config = make_config();
        config.initial_backoff = Duration::from_millis(1);
        let credentials = CountingCredentialProvider::new("token");
        let counter = credentials.counter.clone();
        let connector = MockConnector::new(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            VecDeque::from([Err(ConnectorError::InvalidConfiguration(anyhow!(
                "missing websocket scheme"
            )))]),
        );
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "receipt-1".to_owned(),
        });

        let handle =
            ConnectionSupervisor::new(connector, credentials, source, latch, config).start();
        let error = tokio::time::timeout(Duration::from_secs(1), handle.join())
            .await
            .expect("fatal configuration must stop promptly")
            .expect_err("invalid connector configuration must be fatal");
        assert!(format!("{error:#}").contains("missing websocket scheme"));
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "fatal connector configuration must not retry"
        );
    }

    #[tokio::test]
    async fn established_reconnect_failures_accumulate_and_hit_limit() {
        let mut config = make_config();
        config.max_reconnect_attempts = Some(2);
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
            2,
            "post-hello reconnect failures must accumulate and stop at the configured limit"
        );
    }

    #[tokio::test]
    async fn connect_timeout_rejects_black_hole_and_counts_as_reconnect() {
        let mut config = make_config();
        config.connect_timeout = Duration::from_millis(1);
        config.initial_backoff = Duration::from_millis(1);
        config.max_reconnect_attempts = Some(2);

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
        assert_eq!(attempts, 1, "auth limit should stop at max_auth_attempts");
    }

    #[tokio::test]
    async fn reader_panic_is_fatal_and_propagates() {
        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway {
            reader: MockGatewayReader {
                panic: true,
                commands: VecDeque::new(),
                on_empty: None,
            },
            writer: MockGatewayWriter {
                fail_after: None,
                sent: Arc::new(std::sync::Mutex::new(Vec::new())),
                delay: None,
                block_after: None,
                block_notify: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
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
                on_empty: None,
                commands: VecDeque::new(),
            },
            writer: MockGatewayWriter {
                fail_after: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
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
            next_command_seq: 6,
        };

        // The first legal resend point is applied + 1, not received + 1.
        // With re-fetched cursors (applied=5, received=10) the expected next
        // sequence is 6; 11 would skip the received-but-unapplied commands.
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

    #[tokio::test]
    async fn validate_hello_cursor_read_errors_are_reconnectable() {
        struct FailingSource {
            fail_event: bool,
            fail_command: bool,
        }
        #[async_trait]
        impl DurableSource for FailingSource {
            async fn event_cursor(&self) -> Result<EventCursors> {
                if self.fail_event {
                    bail!("event cursor transient failure");
                }
                Ok(EventCursors { last_sent: 0 })
            }
            async fn events_after(
                &self,
                _after_seq: u64,
                _limit: usize,
            ) -> Result<Vec<OutboundFrame>> {
                Ok(Vec::new())
            }
            async fn command_cursors(&self) -> Result<CommandCursors> {
                if self.fail_command {
                    bail!("command cursor transient failure");
                }
                Ok(CommandCursors {
                    received: 0,
                    applied: 0,
                })
            }
        }
        impl Clone for FailingSource {
            fn clone(&self) -> Self {
                Self {
                    fail_event: self.fail_event,
                    fail_command: self.fail_command,
                }
            }
        }

        let agent = AgentHello {
            agent_id: "test".to_owned(),
            generation: ProcessGeneration::from_wire(7).unwrap(),
            last_sent_event_seq: 0,
            last_received_command_seq: 0,
            last_applied_command_seq: 0,
        };
        let api = ApiHello {
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 1,
        };

        let result = validate_hello(
            &FailingSource {
                fail_event: true,
                fail_command: false,
            },
            &agent,
            &api,
        )
        .await;
        assert!(
            matches!(result, Err(SupervisorError::Reconnect { .. })),
            "event cursor read error must be reconnectable: {result:?}"
        );

        let result = validate_hello(
            &FailingSource {
                fail_event: false,
                fail_command: true,
            },
            &agent,
            &api,
        )
        .await;
        assert!(
            matches!(result, Err(SupervisorError::Reconnect { .. })),
            "command cursor read error must be reconnectable: {result:?}"
        );
    }

    #[tokio::test]
    async fn validate_hello_command_cursor_requires_applied_lower_bound_and_received_upper_bound() {
        let cursor = CommandCursors {
            received: 10,
            applied: 5,
        };
        let agent = AgentHello {
            agent_id: "test".to_owned(),
            generation: ProcessGeneration::from_wire(7).unwrap(),
            last_sent_event_seq: 0,
            last_received_command_seq: cursor.received,
            last_applied_command_seq: cursor.applied,
        };

        struct StaticSource(CommandCursors);
        #[async_trait]
        impl DurableSource for StaticSource {
            async fn event_cursor(&self) -> Result<EventCursors> {
                Ok(EventCursors { last_sent: 0 })
            }
            async fn events_after(
                &self,
                _after_seq: u64,
                _limit: usize,
            ) -> Result<Vec<OutboundFrame>> {
                Ok(Vec::new())
            }
            async fn command_cursors(&self) -> Result<CommandCursors> {
                Ok(self.0)
            }
        }
        impl Clone for StaticSource {
            fn clone(&self) -> Self {
                Self(self.0)
            }
        }

        // A value before applied+1 would replay an already-applied command.
        let api = ApiHello {
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 5,
        };
        assert!(
            validate_hello(&StaticSource(cursor), &agent, &api)
                .await
                .is_err(),
            "next_command_seq before applied+1 must be fatal"
        );

        // Exactly applied+1 is the normal catch-up boundary and is allowed.
        let api = ApiHello {
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 6,
        };
        validate_hello(&StaticSource(cursor), &agent, &api)
            .await
            .unwrap();

        // Any cursor through received+1 is a valid replay/catch-up boundary.
        let api = ApiHello {
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 7,
        };
        validate_hello(&StaticSource(cursor), &agent, &api)
            .await
            .expect("next_command_seq within the received range must be allowed");

        let api = ApiHello {
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 11,
        };
        validate_hello(&StaticSource(cursor), &agent, &api)
            .await
            .expect("received+1 must be allowed");

        // Ahead of received+1 skips a durable command and is fatal.
        let api = ApiHello {
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 12,
        };
        assert!(
            validate_hello(&StaticSource(cursor), &agent, &api)
                .await
                .is_err(),
            "next_command_seq beyond received+1 must be fatal"
        );
    }

    #[tokio::test]
    async fn stdio_command_survives_hydration_mid_chunk() {
        use std::io;
        use std::pin::Pin;
        use std::task::{Context, Poll, ready};
        use tokio::io::{AsyncBufRead, AsyncRead, BufReader, ReadBuf, sink};
        use tokio::sync::oneshot;

        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum ChunkedState {
            First,
            Waiting,
            Done,
        }

        struct ChunkedBuf {
            first_chunk: Vec<u8>,
            remaining: Vec<u8>,
            second_rx: Option<Pin<Box<oneshot::Receiver<Vec<u8>>>>>,
            latch_tx: Pin<Box<watch::Sender<HydrationState>>>,
            generation: ProcessGeneration,
            state: ChunkedState,
        }

        impl AsyncRead for ChunkedBuf {
            fn poll_read(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                if self.remaining.is_empty() {
                    let _ = ready!(self.as_mut().poll_fill_buf(cx))?;
                }
                let len = self.remaining.len().min(buf.remaining());
                if len > 0 {
                    buf.put_slice(&self.remaining[..len]);
                    self.as_mut().consume(len);
                }
                Poll::Ready(Ok(()))
            }
        }

        impl AsyncBufRead for ChunkedBuf {
            fn poll_fill_buf(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<io::Result<&[u8]>> {
                let this = self.get_mut();
                if !this.remaining.is_empty() {
                    return Poll::Ready(Ok(&this.remaining[..]));
                }
                match this.state {
                    ChunkedState::First => {
                        this.remaining = this.first_chunk.clone();
                        Poll::Ready(Ok(&this.remaining[..]))
                    }
                    ChunkedState::Waiting => {
                        let mut second_rx =
                            this.second_rx.take().expect("second chunk already taken");
                        match second_rx.as_mut().poll(cx) {
                            Poll::Ready(Ok(chunk)) => {
                                this.remaining = chunk;
                                this.state = ChunkedState::Done;
                                Poll::Ready(Ok(&this.remaining[..]))
                            }
                            Poll::Ready(Err(_)) => Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "second chunk channel closed",
                            ))),
                            Poll::Pending => {
                                this.second_rx = Some(second_rx);
                                Poll::Pending
                            }
                        }
                    }
                    ChunkedState::Done => Poll::Ready(Ok(&[])),
                }
            }

            fn consume(self: Pin<&mut Self>, amt: usize) {
                let this = self.get_mut();
                this.remaining.drain(..amt);
                if matches!(this.state, ChunkedState::First) && this.remaining.is_empty() {
                    this.latch_tx
                        .as_ref()
                        .get_ref()
                        .send_replace(HydrationState::Ready(HydrationReady {
                            generation: this.generation,
                            receipt_identity: "chunk-test".to_owned(),
                        }));
                    this.state = ChunkedState::Waiting;
                }
            }
        }

        let generation = ProcessGeneration::from_wire(7).unwrap();
        let (latch_tx, latch_rx) = watch::channel(HydrationState::NotReady);
        let (second_tx, second_rx) = oneshot::channel();

        let first =
            br#"{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","command":{"#.to_vec();
        let second = br#""type":"user_message","text":"hi","attachments":[]}}"#.to_vec();

        let buf = ChunkedBuf {
            first_chunk: first,
            remaining: Vec::new(),
            second_rx: Some(Box::pin(second_rx)),
            latch_tx: Box::pin(latch_tx.clone()),
            generation,
            state: ChunkedState::First,
        };

        let gateway = crate::gateway::InjectedStdioGateway::new(
            BufReader::new(buf),
            sink(),
            Arc::new(TestDigestFactory),
        );
        let connector = SingleConnectionConnector::new(gateway);
        let source = MockDurableSource::new(CommandCursors {
            received: 0,
            applied: 0,
        });
        let mut latch_observer = latch_rx.clone();
        let latch = WatchHydrationLatch::new(latch_rx);
        let mut config = make_config();
        config.generation = generation;
        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token"),
            source,
            latch,
            config,
        );
        let mut handle = supervisor.start();

        // Wait for the first chunk to be consumed and hydration to fire before
        // providing the second chunk. This proves the in-flight read is not
        // cancelled when hydration completes.
        loop {
            if matches!(*latch_observer.borrow(), HydrationState::Ready(_)) {
                break;
            }
            latch_observer
                .changed()
                .await
                .expect("latch sender not dropped");
        }
        second_tx.send(second.to_vec()).unwrap();

        let cmd = tokio::time::timeout(Duration::from_secs(1), handle.commands.recv())
            .await
            .expect("command should arrive")
            .expect("command channel should not close");
        assert_eq!(cmd_seq(&cmd), 1);

        // Ensure exactly one command was delivered.
        assert!(
            handle.commands.try_recv().is_err(),
            "command must be delivered exactly once"
        );
    }

    #[tokio::test]
    async fn online_boundary_drops_events_and_delivers_command_acks() {
        // Use a source that delays catch-up completion until we explicitly signal,
        // so we can send frames while the epoch has a writer but is still not Online.
        let catch_up_notify = Arc::new(tokio::sync::Notify::new());
        let source = DelayedCatchUpSource {
            events: Arc::new(std::sync::Mutex::new(VecDeque::from([
                event_frame(1),
                event_frame(2),
            ]))),
            notify: catch_up_notify.clone(),
            command_cursor: CommandCursors {
                received: 0,
                applied: 0,
            },
        };

        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway {
            reader: MockGatewayReader {
                commands: VecDeque::new(),
                panic: false,
                on_empty: None,
            },
            writer: MockGatewayWriter {
                fail_after: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
        };
        let connector = MockConnector::new(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            VecDeque::from([Ok(gateway)]),
        );
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "r".to_owned(),
        });
        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token"),
            source,
            latch,
            make_config(),
        );
        let handle = supervisor.start();

        // Wait for the epoch to be established (writer installed, still offline).
        let mut epochs = handle.epochs.clone();
        while epochs.borrow().is_none() {
            epochs.changed().await.unwrap();
        }
        let epoch = epochs.borrow().unwrap();

        // A durable Event sent before Online must be dropped, otherwise catch-up
        // would emit the same seq again.
        handle.events.send((epoch, event_frame(2))).await.unwrap();

        // A volatile/delta Event (seq: None) sent before Online must also be dropped.
        let volatile = OutboundFrame::Event {
            envelope: Envelope {
                seq: None,
                conversation_id: "conversation-1".to_owned(),
                event: serde_json::json!({"type": "typing"}),
            },
        };
        handle.events.send((epoch, volatile)).await.unwrap();

        // A CommandAck sent before Online is terminal command feedback and must be
        // delivered once the writer is installed, not held until Online.
        let ack = OutboundFrame::CommandAck {
            ack: CommandAck {
                seq: 1,
                command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                status: CommandAckStatus::Received,
                reject_reason: None,
            },
        };
        handle.events.send((epoch, ack)).await.unwrap();

        // Allow catch-up to finish, then wait for Online.
        catch_up_notify.notify_one();
        let mut online = handle.online.clone();
        while !*online.borrow() {
            online.changed().await.unwrap();
        }

        // A durable frame sent after Online should be delivered.
        handle.events.send((epoch, event_frame(3))).await.unwrap();

        // Give writer_task time to forward the live frame.
        tokio::time::sleep(Duration::from_millis(20)).await;

        {
            let sent_frames = sent.lock().unwrap();
            let seqs: Vec<_> = sent_frames
                .iter()
                .filter_map(|f| outbound_frame_event_seq(f).ok())
                .collect();
            assert_eq!(
                seqs,
                vec![1, 2, 3],
                "catch-up seqs 1 and 2 plus post-online seq 3 should be delivered exactly once"
            );
            assert_eq!(
                sent_frames
                    .iter()
                    .filter(|f| matches!(f, OutboundFrame::CommandAck { .. }))
                    .count(),
                1,
                "pre-online CommandAck must be delivered"
            );
            assert!(
                !sent_frames.iter().any(
                    |f| matches!(f, OutboundFrame::Event { envelope } if envelope.seq.is_none())
                ),
                "volatile pre-online Event must be dropped"
            );
        }

        // Epoch end resets Online.
        handle.abort();
        let _ = handle.join().await;
        assert!(!*online.borrow());
    }

    #[tokio::test]
    async fn writer_task_rechecks_cursor_before_publishing_online() {
        // The durable source commits event 2 while the first page is being sent,
        // then returns an empty page for the updated cursor. The writer must
        // re-check the cursor and include the racing commit before Online.
        struct RacingSource {
            events: Arc<std::sync::Mutex<VecDeque<OutboundFrame>>>,
            first_page_started: Arc<Notify>,
            first_page_release: Arc<Notify>,
            second_page_attempts: AtomicU64,
        }

        impl Clone for RacingSource {
            fn clone(&self) -> Self {
                Self {
                    events: self.events.clone(),
                    first_page_started: self.first_page_started.clone(),
                    first_page_release: self.first_page_release.clone(),
                    second_page_attempts: AtomicU64::new(0),
                }
            }
        }

        #[async_trait]
        impl DurableSource for RacingSource {
            async fn event_cursor(&self) -> Result<EventCursors> {
                let events = self.events.lock().unwrap();
                let last_sent = events
                    .back()
                    .map_or(0, |f| outbound_frame_event_seq(f).unwrap_or(0));
                Ok(EventCursors { last_sent })
            }

            async fn events_after(
                &self,
                after_seq: u64,
                _limit: usize,
            ) -> Result<Vec<OutboundFrame>> {
                if after_seq == 0 {
                    let snapshot: VecDeque<_> = {
                        let events = self.events.lock().unwrap();
                        events
                            .iter()
                            .filter(|f| outbound_frame_event_seq(f).unwrap() > after_seq)
                            .cloned()
                            .collect()
                    };
                    // Signal that the first page fetch has started and is held;
                    // the test will then commit event 2 before releasing.
                    self.first_page_started.notify_one();
                    self.first_page_release.notified().await;
                    return Ok(snapshot.into_iter().collect());
                }

                let attempt = self.second_page_attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt == 1 {
                    // Simulate a stale snapshot: the cursor advanced to 2 but the
                    // page read still sees nothing. The next recheck will retry.
                    return Ok(Vec::new());
                }

                let events = self.events.lock().unwrap();
                Ok(events
                    .iter()
                    .filter(|f| outbound_frame_event_seq(f).unwrap() > after_seq)
                    .cloned()
                    .collect())
            }

            async fn command_cursors(&self) -> Result<CommandCursors> {
                Ok(CommandCursors {
                    received: 0,
                    applied: 0,
                })
            }
        }

        let first_page_started = Arc::new(Notify::new());
        let first_page_release = Arc::new(Notify::new());
        let events = Arc::new(std::sync::Mutex::new(VecDeque::from([event_frame(1)])));
        let source = RacingSource {
            events: events.clone(),
            first_page_started: first_page_started.clone(),
            first_page_release: first_page_release.clone(),
            second_page_attempts: AtomicU64::new(0),
        };

        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway {
            reader: MockGatewayReader {
                commands: VecDeque::new(),
                panic: false,
                on_empty: None,
            },
            writer: MockGatewayWriter {
                fail_after: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
        };
        let connector = MockConnector::new(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            VecDeque::from([Ok(gateway)]),
        );
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "r".to_owned(),
        });
        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token"),
            source,
            latch,
            make_config(),
        );
        let handle = supervisor.start();

        // Wait until catch-up is blocked fetching the first page, then race in
        // the durable commit and release the page.
        tokio::time::timeout(Duration::from_secs(1), first_page_started.notified())
            .await
            .expect("writer must start first catch-up page");
        events.lock().unwrap().push_back(event_frame(2));
        first_page_release.notify_one();

        let mut online = handle.online.clone();
        while !*online.borrow() {
            online.changed().await.unwrap();
        }

        let sent_frames = sent.lock().unwrap();
        let seqs: Vec<_> = sent_frames
            .iter()
            .filter_map(|f| outbound_frame_event_seq(f).ok())
            .collect();
        assert_eq!(
            seqs,
            vec![1, 2],
            "racing durable commit 2 must be included before Online without gaps or duplicates"
        );
    }

    #[tokio::test]
    async fn watch_hydration_latch_rejects_identity_change_for_same_generation() {
        let generation = ProcessGeneration::from_wire(7).unwrap();
        let (tx, rx) = watch::channel(HydrationState::NotReady);
        let latch = WatchHydrationLatch::new(rx);

        tx.send(HydrationState::Ready(HydrationReady {
            generation,
            receipt_identity: "first".to_owned(),
        }))
        .unwrap();
        let first = latch.wait_for(generation).await.unwrap();
        assert_eq!(first.receipt_identity, "first");

        tx.send(HydrationState::Ready(HydrationReady {
            generation,
            receipt_identity: "second".to_owned(),
        }))
        .unwrap();
        let result = latch.wait_for(generation).await;
        assert!(
            result.is_err(),
            "re-latch with a different identity must be rejected"
        );
        let err = format!("{:#}", result.unwrap_err());
        assert!(err.contains("identity changed"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn watch_hydration_latch_accepts_new_generation_with_different_identity() {
        let generation_a = ProcessGeneration::from_wire(7).unwrap();
        let generation_b = ProcessGeneration::from_wire(8).unwrap();
        let (tx, rx) = watch::channel(HydrationState::NotReady);
        let latch = WatchHydrationLatch::new(rx);

        tx.send(HydrationState::Ready(HydrationReady {
            generation: generation_a,
            receipt_identity: "first".to_owned(),
        }))
        .unwrap();
        let first = latch.wait_for(generation_a).await.unwrap();
        assert_eq!(first.receipt_identity, "first");

        // A different identity for a different generation must not be rejected.
        tx.send(HydrationState::Ready(HydrationReady {
            generation: generation_b,
            receipt_identity: "second".to_owned(),
        }))
        .unwrap();
        let second = latch.wait_for(generation_b).await.unwrap();
        assert_eq!(second.receipt_identity, "second");

        // Re-latching the new generation with a changed identity is still rejected.
        tx.send(HydrationState::Ready(HydrationReady {
            generation: generation_b,
            receipt_identity: "third".to_owned(),
        }))
        .unwrap();
        let result = latch.wait_for(generation_b).await;
        assert!(
            result.is_err(),
            "re-latch with a different identity must be rejected"
        );
    }

    fn cmd_seq(cmd: &InboundCommand) -> u64 {
        inbound_command_seq(cmd)
    }

    #[tokio::test]
    async fn hydration_hold_limit_fails_closed() {
        // Latch never becomes ready, so the reader must fail closed once the
        // pre-hydration command ceiling is reached instead of stalling.
        let latch = DynamicHydrationLatch::new().0;
        let commands: VecDeque<_> = (1..=17)
            .map(|seq| {
                Ok(valid_command(
                    seq,
                    &format!("00000000-0000-4000-8000-{:012x}", seq),
                ))
            })
            .collect();
        let gateway = MockGateway::new(commands);
        let connector = MockConnector::new(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            VecDeque::from([Ok(gateway)]),
        );
        let credentials = CountingCredentialProvider::new("token");
        let source = MockDurableSource::new(CommandCursors::default());

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let handle = supervisor.start();

        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(result.is_ok(), "exceeding the pending limit must not hang");
        assert!(
            result.unwrap().is_err(),
            "exceeding the pending limit must fail closed"
        );
    }

    #[tokio::test]
    async fn send_validated_cancels_on_full_command_channel() {
        // command_buffer_size is 1, so the first validated command fills the
        // channel and the second send_validated blocks. Abort must release the
        // blocked send and the pending command must not be delivered.
        let mut config = make_config();
        config.command_buffer_size = 1;

        let blocked = Arc::new(Notify::new());
        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway::new(VecDeque::from([
            Ok(valid_command(1, "00000000-0000-4000-8000-000000000001")),
            Ok(valid_command(2, "00000000-0000-4000-8000-000000000002")),
        ]));
        let connector = MockConnector::new(sent_hellos, VecDeque::from([Ok(gateway)]));
        let credentials = CountingCredentialProvider::new("token");
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "r".to_owned(),
        });

        let supervisor = ConnectionSupervisor::new(connector, credentials, source, latch, config)
            .with_command_send_blocked_notify(blocked.clone());
        let mut handle = supervisor.start();

        // Wait until send_validated is blocked on the full command channel.
        tokio::time::timeout(Duration::from_secs(1), blocked.notified())
            .await
            .expect("send_validated must block on full command channel");

        handle.abort();

        let first = tokio::time::timeout(Duration::from_secs(1), handle.commands.recv())
            .await
            .expect("first command must be delivered");
        assert!(first.is_some(), "first command must be in the channel");
        assert!(
            handle.commands.try_recv().is_err(),
            "blocked second command must not be delivered after cancel"
        );

        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(
            result.is_ok(),
            "abort must complete while send_validated is blocked"
        );
    }

    #[tokio::test]
    async fn event_forwarder_cancels_blocked_writer_send() {
        // Catch-up blocks writer_task so writer_rx is not being consumed.
        // event_forwarder must still cancel its own sender.send when the
        // supervisor is aborted, and the pending frame must not be forwarded.
        let catch_up_notify = Arc::new(Notify::new());
        let source = DelayedCatchUpSource {
            events: Arc::new(std::sync::Mutex::new(VecDeque::from([event_frame(1)]))),
            notify: catch_up_notify,
            command_cursor: CommandCursors {
                received: 0,
                applied: 0,
            },
        };

        let mut config = make_config();
        config.event_buffer_size = 1;

        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway {
            reader: MockGatewayReader {
                commands: VecDeque::new(),
                panic: false,
                on_empty: None,
            },
            writer: MockGatewayWriter {
                fail_after: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
        };
        let connector = MockConnector::new(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            VecDeque::from([Ok(gateway)]),
        );
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "r".to_owned(),
        });
        let blocked = Arc::new(Notify::new());
        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token"),
            source,
            latch,
            config,
        )
        .with_writer_send_blocked_notify(blocked.clone());
        let handle = supervisor.start();

        // Wait for the epoch so there is a writer installed.
        let mut epochs = handle.epochs.clone();
        while epochs.borrow().is_none() {
            epochs.changed().await.unwrap();
        }
        let epoch = epochs.borrow().unwrap();

        // CommandAcks are forwarded even before Online. With event_buffer_size=1,
        // the first fills writer_tx and the second send blocks.
        let ack = |seq| OutboundFrame::CommandAck {
            ack: CommandAck {
                seq,
                command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                status: CommandAckStatus::Received,
                reject_reason: None,
            },
        };
        handle.events.send((epoch, ack(1))).await.unwrap();
        handle.events.send((epoch, ack(2))).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), blocked.notified())
            .await
            .expect("event_forwarder must block on full writer channel");

        handle.abort();
        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(
            result.is_ok(),
            "abort must complete while event_forwarder is blocked on sender.send"
        );

        let sent_frames = sent.lock().unwrap();
        assert!(
            !sent_frames
                .iter()
                .any(|f| matches!(f, OutboundFrame::CommandAck { .. })),
            "blocked CommandAck must not be forwarded to the gateway"
        );
    }

    #[tokio::test]
    async fn event_forwarder_panic_is_propagated() {
        let (tx, rx) = mpsc::channel::<(DeliveryEpoch, OutboundFrame)>(1);
        let current_writer: CurrentWriterSlot = Arc::new(std::sync::Mutex::new(None));

        // Poison the writer mutex so event_forwarder panics when it locks.
        let poison = current_writer.clone();
        std::thread::spawn(move || {
            let _guard = poison.lock().unwrap();
            panic!("poison mutex");
        })
        .join()
        .unwrap_err();

        let (_online_tx, online_rx) = watch::channel(false);
        let cancel = CancellationToken::new();
        let forwarder = tokio::spawn(event_forwarder(rx, current_writer, cancel, online_rx, None));

        // Send a frame so event_forwarder tries to lock and panics.
        tx.send((DeliveryEpoch(1), event_frame(1))).await.unwrap();

        let result = tokio::time::timeout(Duration::from_secs(1), forwarder)
            .await
            .expect("forwarder must finish after panic");
        assert!(result.is_err(), "panic must surface as a JoinError");
        let join_err = result.unwrap_err();
        assert!(join_err.is_panic(), "JoinError must be a panic");
        assert!(
            join_err.try_into_panic().is_ok(),
            "panic payload must be recoverable"
        );
    }

    #[test]
    fn agent_hello_rejects_unknown_fields() {
        let json = r#"{"agent_id":"a","generation":1,"last_sent_event_seq":0,"last_received_command_seq":0,"last_applied_command_seq":0,"extra":1}"#;
        assert!(
            serde_json::from_str::<AgentHello>(json).is_err(),
            "AgentHello must reject unknown fields"
        );
    }

    #[test]
    fn api_hello_rejects_unknown_fields() {
        let json = r#"{"accepted_generation":1,"last_received_event_seq":0,"next_command_seq":1,"extra":1}"#;
        assert!(
            serde_json::from_str::<ApiHello>(json).is_err(),
            "ApiHello must reject unknown fields"
        );
    }

    #[test]
    fn hello_dto_deserialization_still_accepts_known_fields() {
        let agent_json = r#"{"agent_id":"a","generation":1,"last_sent_event_seq":0,"last_received_command_seq":0,"last_applied_command_seq":0}"#;
        assert!(serde_json::from_str::<AgentHello>(agent_json).is_ok());

        let api_json =
            r#"{"accepted_generation":1,"last_received_event_seq":0,"next_command_seq":1}"#;
        assert!(serde_json::from_str::<ApiHello>(api_json).is_ok());
    }

    /// A durable source that replays the same events on every catch-up, so
    /// reconnect tests can observe repeated catch-up without losing the queue.
    #[derive(Clone)]
    struct ReplaySource {
        events: Arc<Mutex<Vec<OutboundFrame>>>,
        command_cursor: CommandCursors,
    }

    impl ReplaySource {
        fn new(events: Vec<OutboundFrame>, command_cursor: CommandCursors) -> Self {
            Self {
                events: Arc::new(Mutex::new(events)),
                command_cursor,
            }
        }
    }

    #[async_trait]
    impl DurableSource for ReplaySource {
        async fn event_cursor(&self) -> Result<EventCursors> {
            let events = self.events.lock().unwrap();
            let last_sent = events
                .last()
                .map_or(0, |f| outbound_frame_event_seq(f).unwrap_or(0));
            Ok(EventCursors { last_sent })
        }

        async fn events_after(&self, after_seq: u64, _limit: usize) -> Result<Vec<OutboundFrame>> {
            let events = self.events.lock().unwrap();
            Ok(events
                .iter()
                .filter(|f| outbound_frame_event_seq(f).unwrap_or(0) > after_seq)
                .cloned()
                .collect())
        }

        async fn command_cursors(&self) -> Result<CommandCursors> {
            Ok(self.command_cursor)
        }
    }

    /// A durable source that blocks a configurable `event_cursor` call so a test
    /// can inject a live durable event between the final cursor read and Online.
    #[derive(Clone)]
    struct FinalCursorRaceSource {
        events: Arc<Mutex<Vec<OutboundFrame>>>,
        command_cursor: CommandCursors,
        cursor_calls: Arc<AtomicU64>,
        block_on_call: u64,
        released: Arc<AtomicBool>,
        notify: Arc<tokio::sync::Notify>,
    }

    impl FinalCursorRaceSource {
        fn new(
            events: Vec<OutboundFrame>,
            command_cursor: CommandCursors,
            block_on_call: u64,
        ) -> Self {
            Self {
                events: Arc::new(Mutex::new(events)),
                command_cursor,
                cursor_calls: Arc::new(AtomicU64::new(0)),
                block_on_call,
                released: Arc::new(AtomicBool::new(false)),
                notify: Arc::new(tokio::sync::Notify::new()),
            }
        }

        fn release_cursor(&self) {
            self.released.store(true, Ordering::SeqCst);
            self.notify.notify_one();
        }

        fn is_blocked(&self) -> bool {
            self.cursor_calls.load(Ordering::SeqCst) > self.block_on_call
                && !self.released.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl DurableSource for FinalCursorRaceSource {
        async fn event_cursor(&self) -> Result<EventCursors> {
            let call = self.cursor_calls.fetch_add(1, Ordering::SeqCst);
            if call == self.block_on_call {
                self.notify.notified().await;
            }
            let events = self.events.lock().unwrap();
            let last_sent = events
                .last()
                .map_or(0, |f| outbound_frame_event_seq(f).unwrap_or(0));
            Ok(EventCursors { last_sent })
        }

        async fn events_after(&self, after_seq: u64, _limit: usize) -> Result<Vec<OutboundFrame>> {
            let events = self.events.lock().unwrap();
            Ok(events
                .iter()
                .filter(|f| outbound_frame_event_seq(f).unwrap_or(0) > after_seq)
                .cloned()
                .collect())
        }

        async fn command_cursors(&self) -> Result<CommandCursors> {
            Ok(self.command_cursor)
        }
    }

    #[tokio::test]
    async fn durable_event_racing_final_cursor_is_not_dropped() {
        // Block the third event_cursor call, which is the final recheck right
        // before Online. Inject a durable event while the writer_task is blocked
        // there; the old implementation would drop it because Online was not yet
        // published, while the new one holds it and deduplicates after Online.
        let source = FinalCursorRaceSource::new(vec![event_frame(1)], CommandCursors::default(), 2);

        let sent_hellos = Arc::new(Mutex::new(Vec::new()));
        let sent = Arc::new(Mutex::new(Vec::new()));
        let mut gateway = MockGateway::new(VecDeque::new());
        gateway.writer.sent = sent.clone();
        gateway.sent_hellos = sent_hellos.clone();
        let connector = MockConnector::new(sent_hellos.clone(), VecDeque::from([Ok(gateway)]));
        let credentials = CountingCredentialProvider::new("token");
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "r".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source.clone(), latch, make_config());
        let handle = supervisor.start();

        let mut epochs = handle.epochs.clone();
        while epochs.borrow().is_none() {
            epochs.changed().await.unwrap();
        }
        let epoch = epochs.borrow().unwrap();

        // Wait until writer_task is blocked on the final cursor recheck.
        tokio::time::timeout(Duration::from_secs(1), async {
            while !source.is_blocked() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();

        // This durable event arrives after catch-up finished seq 1 but before
        // Online is published. The source cursor will still report last_sent=1.
        handle.events.send((epoch, event_frame(2))).await.unwrap();

        // Release the final cursor recheck and let the epoch go Online.
        source.release_cursor();
        let mut online = handle.online.clone();
        while !*online.borrow() {
            online.changed().await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
        handle.abort();
        assert!(handle.join().await.is_ok());

        let seqs: Vec<_> = sent
            .lock()
            .unwrap()
            .iter()
            .filter_map(|f| outbound_frame_event_seq(f).ok())
            .collect();
        assert_eq!(
            seqs,
            vec![1, 2],
            "durable event committed between final cursor read and Online must be delivered"
        );
    }

    #[tokio::test]
    async fn writer_send_timeout_triggers_reconnect() {
        let mut config = make_config();
        config.send_timeout = Duration::from_millis(10);
        config.initial_backoff = Duration::from_millis(1);
        config.max_backoff = Duration::from_millis(5);

        let sent_hellos = Arc::new(Mutex::new(Vec::new()));
        let sent = Arc::new(Mutex::new(Vec::new()));

        // First writer is slow enough that the send timeout fires; second writer
        // is normal and should deliver the catch-up event.
        let mut gateway1 = MockGateway::new(VecDeque::new());
        gateway1.writer.sent = sent.clone();
        gateway1.writer.delay = Some(Duration::from_millis(100));
        gateway1.sent_hellos = sent_hellos.clone();

        let mut gateway2 = MockGateway::new(VecDeque::new());
        gateway2.writer.sent = sent.clone();
        gateway2.sent_hellos = sent_hellos.clone();

        let connector = MockConnector::new(
            sent_hellos.clone(),
            VecDeque::from([Ok(gateway1), Ok(gateway2)]),
        );
        let credentials = CountingCredentialProvider::new("token");
        let source = ReplaySource::new(vec![event_frame(1)], CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "r".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials.clone(), source, latch, config);
        let handle = supervisor.start();

        // Wait for the second epoch to install.
        let mut epochs = handle.epochs.clone();
        let _epoch2 = loop {
            if let Some(e) = *epochs.borrow() {
                break e;
            }
            tokio::time::timeout(Duration::from_secs(1), epochs.changed())
                .await
                .unwrap()
                .unwrap();
        };

        // The second epoch must eventually go Online and deliver the catch-up.
        let mut online = handle.online.clone();
        while !*online.borrow() {
            online.changed().await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
        handle.abort();
        assert!(handle.join().await.is_ok());

        let hellos = sent_hellos.lock().unwrap();
        assert_eq!(
            hellos.len(),
            2,
            "send timeout must trigger a fresh reconnect"
        );

        let tokens = credentials.tokens.lock().unwrap();
        assert!(
            tokens.len() >= 2,
            "each reconnect must fetch a fresh credential"
        );
        assert_ne!(tokens[0], tokens[1]);

        let seqs: Vec<_> = sent
            .lock()
            .unwrap()
            .iter()
            .filter_map(|f| outbound_frame_event_seq(f).ok())
            .collect();
        assert!(
            seqs.contains(&1),
            "catch-up event must be delivered after reconnect"
        );
    }

    #[tokio::test]
    async fn ready_before_hello_releases_immediately() {
        let generation = ProcessGeneration::from_wire(7).unwrap();
        let (tx, rx) = watch::channel(HydrationState::NotReady);
        tx.send_replace(HydrationState::Ready(HydrationReady {
            generation,
            receipt_identity: "ready-before-hello".to_owned(),
        }));
        let latch = WatchHydrationLatch::new(rx);

        let source = MockDurableSource::new(CommandCursors::default());
        let sent_hellos = Arc::new(Mutex::new(Vec::new()));
        let mut gateway = MockGateway::new(VecDeque::from([Ok(valid_command(
            1,
            "00000000-0000-4000-8000-000000000001",
        ))]));
        gateway.sent_hellos = sent_hellos.clone();
        let connector = MockConnector::new(sent_hellos, VecDeque::from([Ok(gateway)]));
        let credentials = CountingCredentialProvider::new("token");

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let mut handle = supervisor.start();

        let cmd = tokio::time::timeout(Duration::from_millis(200), handle.commands.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cmd_seq(&cmd), 1);

        handle.abort();
        assert!(handle.join().await.is_ok());
    }

    #[tokio::test]
    async fn api_restart_reconnects_with_catchup_and_invalidates_old_epoch() {
        let source = ReplaySource::new(vec![event_frame(1)], CommandCursors::default());
        let sent_hellos = Arc::new(Mutex::new(Vec::new()));
        let sent = Arc::new(Mutex::new(Vec::new()));

        let mut gateway1 = MockGateway::new(VecDeque::from([Err(anyhow!("api restart"))]));
        gateway1.writer.sent = sent.clone();
        gateway1.sent_hellos = sent_hellos.clone();

        let mut gateway2 = MockGateway::new(VecDeque::from([Ok(valid_command(
            1,
            "00000000-0000-4000-8000-000000000001",
        ))]));
        gateway2.writer.sent = sent.clone();
        gateway2.sent_hellos = sent_hellos.clone();

        let connector = MockConnector::new(
            sent_hellos.clone(),
            VecDeque::from([Ok(gateway1), Ok(gateway2)]),
        );
        let credentials = CountingCredentialProvider::new("token");
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "r".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials.clone(), source, latch, make_config());
        let mut handle = supervisor.start();

        // The first epoch may end before the test can sample `handle.epochs`, so
        // wait for the second hello and the second active epoch, then derive the
        // first DeliveryEpoch from the deterministic epoch counter.
        let epochs = handle.epochs.clone();
        let epoch2 = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if sent_hellos.lock().unwrap().len() >= 2
                    && let Some(e) = *epochs.borrow()
                {
                    return e;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            epoch2.as_u64() > 0,
            "the second reconnect must mint a new DeliveryEpoch"
        );
        let epoch1 = DeliveryEpoch(epoch2.as_u64() - 1);

        // The new epoch must deliver commands.
        let cmd = tokio::time::timeout(Duration::from_millis(200), handle.commands.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cmd_seq(&cmd), 1);

        // A frame tagged with the old DeliveryEpoch must be dropped.
        handle.events.send((epoch1, event_frame(2))).await.unwrap();
        // A frame tagged with the new DeliveryEpoch must be delivered.
        handle.events.send((epoch2, event_frame(3))).await.unwrap();

        let mut online = handle.online.clone();
        while !*online.borrow() {
            online.changed().await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;

        handle.abort();
        assert!(handle.join().await.is_ok());

        let hellos = sent_hellos.lock().unwrap();
        assert_eq!(
            hellos.len(),
            2,
            "api restart requires re-hello with fresh credential"
        );

        let tokens = credentials.tokens.lock().unwrap();
        assert!(tokens.len() >= 2);
        assert_ne!(
            tokens[0], tokens[1],
            "each restart attempt must use a fresh credential"
        );

        let event_seqs: Vec<_> = sent
            .lock()
            .unwrap()
            .iter()
            .filter_map(|f| outbound_frame_event_seq(f).ok())
            .collect();
        assert!(
            event_seqs.contains(&1),
            "durable catch-up must replay event 1 after restart"
        );
        assert!(
            event_seqs.contains(&3),
            "live event 3 must be delivered on the new epoch"
        );
        assert!(
            !event_seqs.contains(&2),
            "stale DeliveryEpoch frame 2 must be dropped"
        );
    }
}
