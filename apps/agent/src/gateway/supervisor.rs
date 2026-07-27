//! ConnectionSupervisor and the gateway lifecycle boundary.
//!
//! This module owns reconnect, re-auth, epoch mapping, bidirectional catch-up,
//! and the `DeliveryEpoch` boundary between the transport and `DeliveryPump`.
//! T17 store integration is represented by compile-safe adapter traits;
//! concrete T17 methods are wired through the `T17StoreAdapter` seam.

#![allow(dead_code)]

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio::task::{JoinError, JoinHandle};
use tokio::time;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroize;

use crate::runtime::contracts::ProcessGeneration;

use super::{
    Gateway, GatewayClosed, GatewayReader, GatewayWriter, HelloError, InboundCommand,
    OutboundFrame, OversizedFrameError,
};

pub mod seams;

// T24-local identity: one `DeliveryEpoch` is minted for each `ConnectionEpoch`
// and invalidated exactly once when the epoch ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ConnectionEpoch(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DeliveryEpoch(u64);

/// Normal terminal boundary for a local single-connection transport.
/// WebSocket closure continues to use `GatewayClosed` and remains reconnectable.
#[derive(Debug, thiserror::Error)]
#[error("single-connection gateway reached terminal EOF")]
pub(crate) struct TerminalGatewayClosed;

impl DeliveryEpoch {
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    #[cfg(test)]
    pub(crate) fn for_test(label: &str) -> Self {
        let mut value = 0xcbf2_9ce4_8422_2325_u64;
        for byte in label.bytes() {
            value ^= u64::from(byte);
            value = value.wrapping_mul(0x0000_0100_0000_01b3);
        }
        Self(value)
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
    delivery_authorization: DeliveryAuthorization,
}

impl GatewayCredential {
    pub fn new(token: impl Into<String>, delivery_authorization: DeliveryAuthorization) -> Self {
        Self {
            token: token.into(),
            delivery_authorization,
        }
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub const fn delivery_authorization(&self) -> DeliveryAuthorization {
        self.delivery_authorization
    }
}

impl fmt::Debug for GatewayCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayCredential")
            .field("token", &"[REDACTED]")
            .field("delivery_authorization", &self.delivery_authorization)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryAuthorization {
    Raw,
    RedactionOnly,
}

impl Drop for GatewayCredential {
    fn drop(&mut self) {
        self.token.zeroize();
    }
}

/// Agent → API hello. `generation` is the `ProcessGeneration` bound to the
/// credential claim. All seq values are `u64` and are validated against the
/// durable source before the epoch proceeds.
///
/// The `lossless_*` helpers encode `u64`/`ProcessGeneration` as canonical
/// decimal strings on the wire. JSON implementations (including JavaScript)
/// cannot lose precision on a string, so the full `0..=u64::MAX` and
/// `0..=i64::MAX` domains are preserved without narrowing the broader durable
/// state contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHello {
    pub agent_id: String,
    #[serde(with = "lossless_generation")]
    pub generation: ProcessGeneration,
    #[serde(with = "lossless_u64")]
    pub last_sent_event_seq: u64,
    #[serde(with = "lossless_u64")]
    pub last_received_command_seq: u64,
    #[serde(with = "lossless_u64")]
    pub last_applied_command_seq: u64,
}

/// API → Agent hello response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiHello {
    #[serde(with = "lossless_generation")]
    pub accepted_generation: ProcessGeneration,
    #[serde(with = "lossless_u64")]
    pub last_received_event_seq: u64,
    #[serde(with = "lossless_u64")]
    pub next_command_seq: u64,
    pub delivery_authorization: DeliveryAuthorization,
}

/// Parses a canonical decimal string: either exactly `'0'` or a nonzero ASCII
/// digit followed by zero or more ASCII digits, with no sign, leading zeros,
/// fractional/exponent notation, or surrounding whitespace. Rejects empty and
/// overflow.
fn parse_canonical_decimal_u64(s: &str) -> Result<u64, String> {
    if s.is_empty() {
        return Err("empty decimal string".to_string());
    }
    if s == "0" {
        return Ok(0);
    }
    let bytes = s.as_bytes();
    if bytes[0] == b'0' {
        return Err("non-canonical leading zero in decimal string".to_string());
    }
    if !bytes.iter().all(|b| b.is_ascii_digit()) {
        return Err("decimal string contains a non-digit character".to_string());
    }
    s.parse::<u64>()
        .map_err(|_| "decimal string exceeds u64 range".to_string())
}

mod lossless_generation {
    use serde::de::{self, Visitor};
    use serde::{Deserializer, Serializer};

    use crate::runtime::contracts::ProcessGeneration;

    pub fn serialize<S: Serializer>(
        value: &ProcessGeneration,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.as_u64().to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<ProcessGeneration, D::Error> {
        struct GenerationVisitor;

        impl<'de> Visitor<'de> for GenerationVisitor {
            type Value = ProcessGeneration;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str(
                    "a canonical decimal string representing a process generation in 0..=i64::MAX",
                )
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<ProcessGeneration, E> {
                let n = super::parse_canonical_decimal_u64(value).map_err(de::Error::custom)?;
                ProcessGeneration::from_wire(n).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_str(GenerationVisitor)
    }
}

mod lossless_u64 {
    use serde::de::{self, Visitor};
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        struct U64Visitor;

        impl<'de> Visitor<'de> for U64Visitor {
            type Value = u64;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a canonical decimal string representing a u64")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<u64, E> {
                super::parse_canonical_decimal_u64(value).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_str(U64Visitor)
    }
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
    fn bind_delivery_authorization(&self, _authorization: DeliveryAuthorization) -> Result<Self> {
        Ok(self.clone())
    }

    async fn event_cursor(&self) -> Result<EventCursors>;
    async fn events_after(&self, after_seq: u64, limit: usize) -> Result<Vec<OutboundFrame>>;
    async fn command_cursors(&self) -> Result<CommandCursors>;

    async fn install_delivery_epoch(
        &self,
        _epoch: DeliveryEpoch,
        _catch_up_from_seq: u64,
        _sink: EventSender,
        _cancel: CancellationToken,
    ) -> Result<Option<DeliveryEpochRuntime>> {
        Ok(None)
    }

    async fn invalidate_delivery_epoch(&self, _epoch: DeliveryEpoch) -> Result<()> {
        Ok(())
    }

    /// Open volatile delivery only after the writer has completed durable
    /// catch-up and its final cursor recheck. Implementations that own a
    /// DeliveryPump use this as the admission barrier; other sources may treat
    /// the transition as a no-op.
    async fn mark_delivery_online(&self, _epoch: DeliveryEpoch) -> Result<()> {
        Ok(())
    }
}

pub struct DeliveryEpochRuntime {
    failure_rx: mpsc::UnboundedReceiver<String>,
    task: Option<JoinHandle<()>>,
}

enum DeliveryEpochCompletion {
    /// The delivery pump itself reported a recoverable channel failure.
    Reported(String),
    /// The failure channel disappeared without reporting why.
    FailureChannelClosed,
    /// The delivery forwarder terminated before reporting a channel failure.
    /// Keep the join result intact: a `JoinError` carries the distinction
    /// between cancellation and a task panic.
    Task(Result<(), JoinError>),
}

impl DeliveryEpochCompletion {
    fn into_supervisor_error(self) -> Option<SupervisorError> {
        match self {
            Self::Reported(reason) => Some(SupervisorError::EstablishedReconnect {
                reason: format!("delivery epoch failed: {reason}"),
                healthy: false,
            }),
            Self::FailureChannelClosed => Some(SupervisorError::Fatal(anyhow!(
                "delivery epoch failure channel closed without a terminal signal"
            ))),
            Self::Task(Ok(())) => Some(SupervisorError::Fatal(anyhow!(
                "delivery epoch task ended without a terminal signal"
            ))),
            Self::Task(Err(join_err)) if join_err.is_panic() => Some(SupervisorError::Fatal(
                anyhow!("delivery epoch task panicked: {join_err}"),
            )),
            Self::Task(Err(join_err)) if !join_err.is_cancelled() => Some(SupervisorError::Fatal(
                anyhow!("delivery epoch task join error: {join_err}"),
            )),
            // A cancelled task cannot be retried as a delivery-channel failure.
            // It is already a terminal epoch result, so let normal epoch cleanup
            // return without initiating another connection attempt.
            Self::Task(Err(_)) => None,
        }
    }
}

impl DeliveryEpochRuntime {
    pub(crate) fn new(failure_rx: mpsc::UnboundedReceiver<String>, task: JoinHandle<()>) -> Self {
        Self {
            failure_rx,
            task: Some(task),
        }
    }

    async fn failed(&mut self) -> DeliveryEpochCompletion {
        let Some(task) = self.task.as_mut() else {
            return DeliveryEpochCompletion::Task(Ok(()));
        };

        tokio::select! {
            failure = self.failure_rx.recv() => match failure {
                Some(failure) => DeliveryEpochCompletion::Reported(failure),
                None => DeliveryEpochCompletion::FailureChannelClosed,
            },
            result = task => {
                self.task = None;
                match result {
                    // The forwarder reports recoverable delivery failures before
                    // returning. Prefer that queued report over its consequent
                    // clean task completion when both are ready together.
                    Ok(()) => match self.failure_rx.try_recv() {
                        Ok(failure) => DeliveryEpochCompletion::Reported(failure),
                        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                            DeliveryEpochCompletion::Task(Ok(()))
                        }
                    },
                    Err(join_err) => DeliveryEpochCompletion::Task(Err(join_err)),
                }
            }
        }
    }

    async fn join(mut self) -> Result<(), JoinError> {
        match self.task.take() {
            Some(task) => task.await,
            None => Ok(()),
        }
    }
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
    pub event_buffer_size: NonZeroUsize,
    pub command_buffer_size: NonZeroUsize,
    pub catch_up_page_size: NonZeroUsize,
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
            event_buffer_size: NonZeroUsize::new(64).expect("64 is nonzero"),
            command_buffer_size: NonZeroUsize::new(64).expect("64 is nonzero"),
            catch_up_page_size: NonZeroUsize::new(64).expect("64 is nonzero"),
            max_reconnect_attempts: None,
            max_auth_attempts: Some(3),
            hello_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(30),
        }
    }
}

/// Sender returned by `SupervisorHandle::events`.
///
/// Each frame is tagged with the `online` value observed at admission time
/// (just before the frame is enqueued in the supervisor's event channel) so
/// that the `event_forwarder` can drop pre-Online volatile frames using the
/// admission boundary instead of a later, racy watch-channel observation.
#[derive(Clone)]
pub struct EventSender {
    tx: mpsc::Sender<(DeliveryEpoch, bool, OutboundFrame)>,
    online: watch::Receiver<bool>,
}

impl EventSender {
    /// Enqueue `frame` for delivery in `epoch`. The returned future resolves
    /// once the frame has been admitted; it preserves bounded backpressure.
    pub async fn send(
        &self,
        (epoch, frame): (DeliveryEpoch, OutboundFrame),
    ) -> Result<(), mpsc::error::SendError<(DeliveryEpoch, OutboundFrame)>> {
        let permit = match self.tx.reserve().await {
            Ok(p) => p,
            Err(_) => return Err(mpsc::error::SendError((epoch, frame))),
        };
        let online_at_enqueue = *self.online.borrow();
        permit.send((epoch, online_at_enqueue, frame));
        Ok(())
    }
}

/// Handle to a spawned `ConnectionSupervisor`.
pub struct SupervisorHandle {
    pub commands: mpsc::Receiver<InboundCommand>,
    pub events: EventSender,
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
        // Dropping a JoinHandle detaches the task. Keep the cancelled
        // supervisor alive long enough to join both connection halves and
        // invalidate the installed T17 delivery epoch. Aborting this task here
        // would cancel the cleanup future and leave the dead epoch mapped.
        self.task.take();
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
        }
    }

    pub(crate) fn with_command_send_blocked_notify(mut self, notify: Arc<Notify>) -> Self {
        self.command_send_blocked_notify = Some(notify);
        self
    }

    /// Start the supervisor and return channels for commands, events, and epoch
    /// observation. `events` must carry the current `DeliveryEpoch` from
    /// `handle.epochs`.
    pub fn start(self) -> SupervisorHandle {
        let (commands_tx, commands_rx) = mpsc::channel(self.config.command_buffer_size.get());
        let (events_tx, events_rx) = mpsc::channel(self.config.event_buffer_size.get());
        let epochs_rx = self.current_epoch.subscribe();
        let online_rx = self.online.subscribe();
        let cancel = self.cancel.clone();
        let events = EventSender {
            tx: events_tx,
            online: online_rx.clone(),
        };
        let task = tokio::spawn(self.run(commands_tx, events_rx, events.clone()));
        SupervisorHandle {
            commands: commands_rx,
            events,
            epochs: epochs_rx,
            online: online_rx,
            cancel,
            task: Some(task),
        }
    }

    pub async fn run(
        mut self,
        commands_tx: mpsc::Sender<InboundCommand>,
        events_rx: mpsc::Receiver<(DeliveryEpoch, bool, OutboundFrame)>,
        events: EventSender,
    ) -> Result<()> {
        let current_writer = self.current_writer.clone();
        let cancel = self.cancel.clone();
        let forwarder = tokio::spawn(event_forwarder(events_rx, current_writer, cancel));

        // run_loop owns all per-epoch cancellation and cleanup; await it so that
        // connect_and_run_epoch gets a chance to publish Online=false and clear
        // the current writer/epoch before the supervisor task exits.
        let result = self.run_loop(commands_tx, events).await;

        forwarder.abort();
        if let Err(join_err) = forwarder.await
            && let Ok(panic) = join_err.try_into_panic()
        {
            std::panic::resume_unwind(panic);
        }
        // Normal abort cancellation is intentionally not an error.
        result
    }

    async fn run_loop(
        &mut self,
        commands_tx: mpsc::Sender<InboundCommand>,
        events: EventSender,
    ) -> Result<()> {
        const DEFAULT_MAX_AUTH_ATTEMPTS: u32 = 3;

        let mut auth_attempt: u32 = 0;
        let mut reconnect_attempt: u32 = 0;

        loop {
            if self.cancel.is_cancelled() {
                return Ok(());
            }

            match self
                .connect_and_run_epoch(commands_tx.clone(), events.clone())
                .await
            {
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
                        _ = Self::backoff_sleep(&self.config, auth_attempt) => {},
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
                        _ = Self::backoff_sleep(&self.config, reconnect_attempt) => {},
                    }
                }
                Err(SupervisorError::EstablishedReconnect { reason, healthy }) => {
                    // A healthy established epoch (online reached and both half-tasks
                    // ended cleanly) resets the reconnect budget so it bounds
                    // consecutive failure bursts, not lifetime disconnects.
                    auth_attempt = 0;
                    if healthy {
                        reconnect_attempt = 0;
                    } else {
                        reconnect_attempt = reconnect_attempt.saturating_add(1);
                        if let Some(max) = self.config.max_reconnect_attempts
                            && reconnect_attempt >= max
                        {
                            return Err(anyhow!("max reconnect attempts exceeded: {reason}"));
                        }
                    }
                    tokio::select! {
                        biased;
                        _ = self.cancel.cancelled() => return Ok(()),
                        _ = Self::backoff_sleep(&self.config, reconnect_attempt) => {},
                    }
                }
            }
        }
    }

    async fn connect_and_run_epoch(
        &mut self,
        commands_tx: mpsc::Sender<InboundCommand>,
        events: EventSender,
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
        let delivery_authorization = credential.delivery_authorization();

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
        if api_hello.delivery_authorization != delivery_authorization {
            return Err(SupervisorError::Fatal(anyhow!(
                "delivery authorization mismatch: credential={delivery_authorization:?}, api={:?}",
                api_hello.delivery_authorization
            )));
        }
        let source = source
            .bind_delivery_authorization(delivery_authorization)
            .map_err(SupervisorError::Fatal)?;

        let (connection_epoch, delivery_epoch) = self.next_epoch();
        let (reader, writer) = gateway.split();

        let epoch_token = cancel.child_token();
        let (writer_tx, writer_rx) = mpsc::channel(config.event_buffer_size.get());

        *self.current_writer.lock().unwrap() = Some((delivery_epoch, writer_tx));
        self.current_epoch.send_replace(Some(delivery_epoch));

        let (delivery_ready_tx, delivery_ready_rx) = oneshot::channel();
        let writer_source = source.clone();
        let mut writer_handle = tokio::spawn(writer_task(
            writer,
            writer_rx,
            writer_source,
            api_hello.clone(),
            delivery_epoch,
            config.clone(),
            online,
            epoch_token.child_token(),
            delivery_ready_rx,
        ));

        let mut delivery_runtime = match source
            .install_delivery_epoch(
                delivery_epoch,
                api_hello
                    .last_received_event_seq
                    .checked_add(1)
                    .ok_or_else(|| {
                        SupervisorError::Fatal(anyhow::Error::new(
                            DurableReplayInvariantError::new(
                                "API event cursor exhausted before DeliveryPump install",
                            ),
                        ))
                    })?,
                events,
                epoch_token.child_token(),
            )
            .await
        {
            Ok(runtime) => runtime,
            Err(error) => {
                let reason = format!("failed to install T17 delivery epoch: {error:#}");
                let _ = delivery_ready_tx.send(Err(anyhow!(reason.clone())));
                epoch_token.cancel();
                let _ = writer_handle.await;
                *self.current_writer.lock().unwrap() = None;
                self.current_epoch.send_replace(None);
                let _ = self.online.send(false);
                return Err(SupervisorError::EstablishedReconnect {
                    reason,
                    healthy: false,
                });
            }
        };
        let _ = delivery_ready_tx.send(Ok(()));

        let command_send_blocked_notify = self.command_send_blocked_notify.clone();
        let mut reader_handle = tokio::spawn(reader_task(
            reader,
            commands_tx,
            self.latch.clone(),
            api_hello,
            epoch_token.child_token(),
            command_send_blocked_notify,
        ));

        let mut delivery_completion = None;
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                epoch_token.cancel();
                let reader_result = reader_handle.await;
                let writer_result = writer_handle.await;
                *self.current_writer.lock().unwrap() = None;
                self.current_epoch.send_replace(None);
                let _ = self.online.send(false);
                Self::inspect_epoch_results(reader_result, writer_result, || Ok(()))
            }
            reader_result = &mut reader_handle => {
                epoch_token.cancel();
                *self.current_writer.lock().unwrap() = None;
                self.current_epoch.send_replace(None);
                let was_online = {
                    let rx = self.online.subscribe();
                    *rx.borrow()
                };
                let _ = self.online.send(false);
                let writer_result = writer_handle.await;
                let healthy = was_online
                    && reader_result.as_ref().ok().is_some_and(|r| r.is_ok())
                    && writer_result.as_ref().ok().is_some_and(|r| r.is_ok());
                Self::inspect_epoch_results(reader_result, writer_result, || {
                    Err(SupervisorError::EstablishedReconnect {
                        reason: "reader/writer task ended".to_owned(),
                        healthy,
                    })
                })
            }
            writer_result = &mut writer_handle => {
                epoch_token.cancel();
                *self.current_writer.lock().unwrap() = None;
                self.current_epoch.send_replace(None);
                let was_online = {
                    let rx = self.online.subscribe();
                    *rx.borrow()
                };
                let _ = self.online.send(false);
                let reader_result = reader_handle.await;
                let healthy = was_online
                    && reader_result.as_ref().ok().is_some_and(|r| r.is_ok())
                    && writer_result.as_ref().ok().is_some_and(|r| r.is_ok());
                Self::inspect_epoch_results(reader_result, writer_result, || {
                    Err(SupervisorError::EstablishedReconnect {
                        reason: "reader/writer task ended".to_owned(),
                        healthy,
                    })
                })
            }
            completion = wait_delivery_failure(&mut delivery_runtime) => {
                delivery_completion = Some(completion);
                epoch_token.cancel();
                *self.current_writer.lock().unwrap() = None;
                self.current_epoch.send_replace(None);
                let _ = self.online.send(false);
                let reader_result = reader_handle.await;
                let writer_result = writer_handle.await;
                Self::inspect_epoch_results(reader_result, writer_result, || Ok(()))
            }
        };

        let delivery_join_error = if let Some(runtime) = delivery_runtime {
            match runtime.join().await {
                Ok(()) => None,
                Err(join_err) if join_err.is_panic() => Some(SupervisorError::Fatal(anyhow!(
                    "delivery epoch task panicked: {join_err}"
                ))),
                Err(join_err) if !join_err.is_cancelled() => Some(SupervisorError::Fatal(anyhow!(
                    "delivery epoch task join error: {join_err}"
                ))),
                Err(_) => None,
            }
        } else {
            None
        };

        if let Err(error) = source.invalidate_delivery_epoch(delivery_epoch).await {
            return Err(SupervisorError::Fatal(error.context(format!(
                "failed to invalidate T17 delivery epoch {}",
                delivery_epoch.as_u64()
            ))));
        }
        if let Some(error) = delivery_join_error {
            return Err(error);
        }
        if matches!(&result, Err(SupervisorError::Fatal(_))) {
            return result;
        }
        if let Some(completion) = delivery_completion
            && let Some(error) = completion.into_supervisor_error()
        {
            return Err(error);
        }

        if let Err(SupervisorError::EstablishedReconnect { .. }) = &result {
            tracing::debug!(
                connection_epoch = connection_epoch.as_u64(),
                delivery_epoch = delivery_epoch.as_u64(),
                "epoch ended"
            );
        }

        result
    }

    fn inspect_epoch_results<F>(
        reader_result: Result<Result<(), ReaderError>, JoinError>,
        writer_result: Result<Result<()>, JoinError>,
        on_both_ok: F,
    ) -> Result<(), SupervisorError>
    where
        F: FnOnce() -> Result<(), SupervisorError>,
    {
        if let Err(join_err) = &reader_result {
            if join_err.is_panic() {
                return Err(SupervisorError::Fatal(anyhow!(
                    "epoch task panicked: {join_err}"
                )));
            }
            return Err(SupervisorError::Fatal(anyhow!(
                "epoch task join error: {join_err}"
            )));
        }
        if let Err(join_err) = &writer_result {
            if join_err.is_panic() {
                return Err(SupervisorError::Fatal(anyhow!(
                    "epoch task panicked: {join_err}"
                )));
            }
            return Err(SupervisorError::Fatal(anyhow!(
                "epoch task join error: {join_err}"
            )));
        }

        let reader_result = reader_result.unwrap();
        let writer_result = writer_result.unwrap();

        if let Some(e) = first_oversized_error(&writer_result) {
            return Err(SupervisorError::Fatal(anyhow::Error::new(e)));
        }
        if let Some(e) = first_durable_replay_invariant_error(&writer_result) {
            return Err(SupervisorError::Fatal(anyhow::Error::new(e)));
        }

        match (reader_result, writer_result) {
            (Ok(()), Ok(())) => on_both_ok(),
            (Err(ReaderError::Terminal), Ok(())) => Ok(()),
            (Err(ReaderError::Fatal(e)), _) => Err(SupervisorError::Fatal(e)),
            (Err(ReaderError::Reconnect(e)), _) => Err(SupervisorError::EstablishedReconnect {
                reason: format!("epoch task error: {e}"),
                healthy: false,
            }),
            (_, Err(e)) => Err(SupervisorError::EstablishedReconnect {
                reason: format!("epoch task error: {e}"),
                healthy: false,
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

    async fn backoff_sleep(config: &SupervisorConfig, attempt: u32) {
        let (lower_ms, upper_ms) = Self::backoff_window_ms(config, attempt);
        let jitter = if upper_ms == 0 {
            0
        } else {
            rand::rng().random_range(lower_ms..=upper_ms)
        };
        time::sleep(Duration::from_millis(jitter)).await;
    }
}

async fn wait_delivery_failure(
    runtime: &mut Option<DeliveryEpochRuntime>,
) -> DeliveryEpochCompletion {
    match runtime {
        Some(runtime) => runtime.failed().await,
        None => std::future::pending().await,
    }
}

fn first_oversized_error(result: &Result<()>) -> Option<OversizedFrameError> {
    result
        .as_ref()
        .err()
        .and_then(|e| e.downcast_ref::<OversizedFrameError>().copied())
}

fn first_durable_replay_invariant_error(
    result: &Result<()>,
) -> Option<DurableReplayInvariantError> {
    result
        .as_ref()
        .err()
        .and_then(|error| error.downcast_ref::<DurableReplayInvariantError>().cloned())
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
    // The API may not have durably recorded a terminal ACK that the agent
    // already committed. In that case it must restart replay at that locally
    // terminal command so the durable consumer can return the saved
    // Applied/Superseded/Rejected ACK. Older positive cursors remain valid for
    // terminal ACK recovery, but the peer may not skip the first command that
    // was nonterminal in the AgentHello snapshot.
    let max_next_command_seq = agent
        .last_applied_command_seq
        .checked_add(1)
        .ok_or_else(|| {
            SupervisorError::Fatal(anyhow::Error::new(DurableReplayInvariantError::new(
                format!(
                    "command applied cursor overflow: applied={}",
                    agent.last_applied_command_seq
                ),
            )))
        })?;
    if api.next_command_seq == 0 || api.next_command_seq > max_next_command_seq {
        return Err(SupervisorError::Fatal(anyhow!(
            "command cursor claim outside durable bounds: next_command_seq {} not in 1..={}; agent_applied={}, received={}",
            api.next_command_seq,
            max_next_command_seq,
            agent.last_applied_command_seq,
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
    EstablishedReconnect { reason: String, healthy: bool },
}

/// A permanent inconsistency in the local durable replay source.
///
/// Reconnecting cannot repair these failures because every epoch reads the
/// same canonical event log and cursor. Keep this type distinct from transport
/// writer errors, which remain reconnectable.
#[derive(Clone, Debug, thiserror::Error)]
#[error("durable replay invariant violated: {message}")]
struct DurableReplayInvariantError {
    message: String,
}

impl DurableReplayInvariantError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Classifies errors from `reader_task` so the supervisor can decide whether
/// to reconnect or fail closed.
#[derive(Debug, thiserror::Error)]
enum ReaderError {
    #[error("terminal single-connection EOF")]
    Terminal,
    #[error("fatal reader error: {0}")]
    Fatal(#[source] anyhow::Error),
    #[error("reconnectable reader error: {0}")]
    Reconnect(#[source] anyhow::Error),
}

impl From<anyhow::Error> for ReaderError {
    fn from(e: anyhow::Error) -> Self {
        if e.is::<DurableReplayInvariantError>() {
            ReaderError::Fatal(e)
        } else {
            ReaderError::Reconnect(e)
        }
    }
}

async fn event_forwarder(
    mut event_rx: mpsc::Receiver<(DeliveryEpoch, bool, OutboundFrame)>,
    current_writer: CurrentWriterSlot,
    cancel: CancellationToken,
) {
    loop {
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            result = event_rx.recv() => result,
        };
        let Some((epoch, online_at_enqueue, frame)) = result else {
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
        // Volatile/delta Events (no seq) are only live if they were admitted
        // while the epoch was already Online. The boolean was captured at the
        // event's admission/enqueue boundary, so a pre-Online volatile frame
        // cannot become live merely because the forwarder was backpressured
        // until after Online. Durable Events (seq present) are held in the
        // writer channel so writer_task can deduplicate them against the durable
        // cursor after Online. CommandAck frames are terminal command feedback
        // and must be delivered even while catch-up is in progress.
        if !online_at_enqueue
            && let OutboundFrame::Event { envelope } = &frame
            && envelope.seq.is_none()
        {
            continue;
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

fn take_arrival_id(counter: &mut u64) -> u64 {
    let id = *counter;
    *counter = counter.checked_add(1).expect("arrival id overflow");
    id
}

fn classify_frame(
    frame: OutboundFrame,
    online: bool,
    last_received: u64,
    next_arrival_id: &mut u64,
    outbox: &mut VecDeque<(u64, OutboundFrame)>,
    pending_events: &mut BTreeMap<u64, (u64, OutboundFrame)>,
) {
    match frame {
        OutboundFrame::CommandAck { .. } => {
            let id = take_arrival_id(next_arrival_id);
            outbox.push_back((id, frame));
        }
        OutboundFrame::Event { ref envelope } => match envelope.seq {
            None if online => {
                let id = take_arrival_id(next_arrival_id);
                outbox.push_back((id, frame));
            }
            None => {
                // Volatile/delta events before Online are stale; drop them.
            }
            Some(seq) if seq <= last_received => {
                // Already delivered or already superseded by catch-up.
            }
            Some(seq) => {
                let id = take_arrival_id(next_arrival_id);
                pending_events.entry(seq).or_insert_with(|| (id, frame));
            }
        },
    }
}

fn prune_pending_events(
    pending_events: &mut BTreeMap<u64, (u64, OutboundFrame)>,
    last_received: u64,
) {
    if let Some(next) = last_received.checked_add(1) {
        *pending_events = pending_events.split_off(&next);
    } else {
        pending_events.clear();
    }
}

fn internal_buffer_has_capacity(
    writer_rx: &mpsc::Receiver<OutboundFrame>,
    outbox: &VecDeque<(u64, OutboundFrame)>,
    pending_events: &BTreeMap<u64, (u64, OutboundFrame)>,
) -> bool {
    outbox.len().saturating_add(pending_events.len()) < writer_rx.max_capacity()
}

#[allow(clippy::too_many_arguments)]
async fn send_with_interleave<W>(
    writer: &mut W,
    frame: OutboundFrame,
    timeout: Duration,
    token: &CancellationToken,
    writer_rx: &mut mpsc::Receiver<OutboundFrame>,
    next_arrival_id: &mut u64,
    outbox: &mut VecDeque<(u64, OutboundFrame)>,
    pending_events: &mut BTreeMap<u64, (u64, OutboundFrame)>,
    last_received: u64,
    online: bool,
) -> Result<()>
where
    W: GatewayWriter,
{
    let mut send_fut = std::pin::pin!(send_with_timeout(writer, frame, timeout));
    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => return Ok(()),
            result = &mut send_fut => return result,
            frame = writer_rx.recv(),
                if internal_buffer_has_capacity(writer_rx, outbox, pending_events) =>
            {
                let Some(frame) = frame else { return Ok(()); };
                classify_frame(
                    frame,
                    online,
                    last_received,
                    next_arrival_id,
                    outbox,
                    pending_events,
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn drain_next<W>(
    writer: &mut W,
    timeout: Duration,
    token: &CancellationToken,
    writer_rx: &mut mpsc::Receiver<OutboundFrame>,
    next_arrival_id: &mut u64,
    outbox: &mut VecDeque<(u64, OutboundFrame)>,
    pending_events: &mut BTreeMap<u64, (u64, OutboundFrame)>,
    last_received: &mut u64,
    online: bool,
) -> Result<()>
where
    W: GatewayWriter,
{
    loop {
        prune_pending_events(pending_events, *last_received);

        if online {
            // Sendable durable is the smallest seq still ahead of the watermark.
            // Preserve producer order by comparing its arrival id with the earliest
            // ack/volatile in the outbox and sending whichever arrived first.
            if let Some((&seq, &(id, _))) = pending_events.first_key_value() {
                if let Some(&(out_id, _)) = outbox.front()
                    && out_id < id
                {
                    let (_, frame) = outbox.pop_front().expect("front just observed");
                    send_with_interleave(
                        writer,
                        frame,
                        timeout,
                        token,
                        writer_rx,
                        next_arrival_id,
                        outbox,
                        pending_events,
                        *last_received,
                        online,
                    )
                    .await?;
                    continue;
                }
                let expected = last_received
                    .checked_add(1)
                    .context("durable event sequence exhausted")?;
                if seq != expected {
                    bail!("durable live event gap: expected {expected}, got {seq}");
                }
                let (_, frame) = pending_events.remove(&seq).expect("key just observed");
                *last_received = seq;
                send_with_interleave(
                    writer,
                    frame,
                    timeout,
                    token,
                    writer_rx,
                    next_arrival_id,
                    outbox,
                    pending_events,
                    *last_received,
                    online,
                )
                .await?;
                continue;
            }
        }

        if let Some((_, frame)) = outbox.pop_front() {
            send_with_interleave(
                writer,
                frame,
                timeout,
                token,
                writer_rx,
                next_arrival_id,
                outbox,
                pending_events,
                *last_received,
                online,
            )
            .await?;
            continue;
        }

        return Ok(());
    }
}

#[allow(clippy::too_many_arguments)]
async fn await_cursor<S>(
    source: &S,
    token: &CancellationToken,
    writer_rx: &mut mpsc::Receiver<OutboundFrame>,
    next_arrival_id: &mut u64,
    outbox: &mut VecDeque<(u64, OutboundFrame)>,
    pending_events: &mut BTreeMap<u64, (u64, OutboundFrame)>,
    last_received: u64,
    online: bool,
) -> Result<Option<EventCursors>>
where
    S: DurableSource,
{
    let mut fut = std::pin::pin!(source.event_cursor());
    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => return Ok(None),
            result = &mut fut => return result.map(Some),
            frame = writer_rx.recv(),
                if internal_buffer_has_capacity(writer_rx, outbox, pending_events) =>
            {
                let Some(frame) = frame else { return Ok(None); };
                classify_frame(
                    frame,
                    online,
                    last_received,
                    next_arrival_id,
                    outbox,
                    pending_events,
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn await_page<S>(
    source: &S,
    after_seq: u64,
    limit: usize,
    token: &CancellationToken,
    writer_rx: &mut mpsc::Receiver<OutboundFrame>,
    next_arrival_id: &mut u64,
    outbox: &mut VecDeque<(u64, OutboundFrame)>,
    pending_events: &mut BTreeMap<u64, (u64, OutboundFrame)>,
    last_received: u64,
    online: bool,
) -> Result<Option<Vec<OutboundFrame>>>
where
    S: DurableSource,
{
    let mut page = std::pin::pin!(source.events_after(after_seq, limit));
    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => return Ok(None),
            result = &mut page => return result.map(Some),
            frame = writer_rx.recv(),
                if internal_buffer_has_capacity(writer_rx, outbox, pending_events) =>
            {
                let Some(frame) = frame else {
                    return Ok(None);
                };
                classify_frame(
                    frame,
                    online,
                    last_received,
                    next_arrival_id,
                    outbox,
                    pending_events,
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn writer_task<W, S>(
    mut writer: W,
    mut writer_rx: mpsc::Receiver<OutboundFrame>,
    source: S,
    api_hello: ApiHello,
    delivery_epoch: DeliveryEpoch,
    config: SupervisorConfig,
    online: Arc<watch::Sender<bool>>,
    token: CancellationToken,
    delivery_ready: oneshot::Receiver<Result<()>>,
) -> Result<()>
where
    W: GatewayWriter,
    S: DurableSource,
{
    let mut last_received = api_hello.last_received_event_seq;
    let mut outbox: VecDeque<(u64, OutboundFrame)> = VecDeque::new();
    let mut pending_events: BTreeMap<u64, (u64, OutboundFrame)> = BTreeMap::new();
    let mut next_arrival_id: u64 = 0;
    let mut is_online = false;

    let mut cursor = match await_cursor(
        &source,
        &token,
        &mut writer_rx,
        &mut next_arrival_id,
        &mut outbox,
        &mut pending_events,
        last_received,
        is_online,
    )
    .await?
    {
        Some(c) => c,
        None => return Ok(()),
    };

    while last_received < cursor.last_sent {
        let page = match await_page(
            &source,
            last_received,
            config.catch_up_page_size.get(),
            &token,
            &mut writer_rx,
            &mut next_arrival_id,
            &mut outbox,
            &mut pending_events,
            last_received,
            is_online,
        )
        .await?
        {
            Some(p) => p,
            None => return Ok(()),
        };

        if page.is_empty() {
            return Err(DurableReplayInvariantError::new(
                "event source returned empty page before advertised cursor",
            )
            .into());
        }

        for frame in page {
            let seq = outbound_frame_event_seq(&frame).map_err(|_| {
                DurableReplayInvariantError::new("catch-up frame missing durable seq")
            })?;
            let expected = last_received.checked_add(1).ok_or_else(|| {
                DurableReplayInvariantError::new("durable event sequence exhausted during catch-up")
            })?;
            if seq != expected {
                return Err(DurableReplayInvariantError::new(format!(
                    "durable catch-up event gap: expected {expected}, got {seq}"
                ))
                .into());
            }
            send_with_interleave(
                &mut writer,
                frame,
                config.send_timeout,
                &token,
                &mut writer_rx,
                &mut next_arrival_id,
                &mut outbox,
                &mut pending_events,
                last_received,
                is_online,
            )
            .await?;
            last_received = seq;
            drain_next(
                &mut writer,
                config.send_timeout,
                &token,
                &mut writer_rx,
                &mut next_arrival_id,
                &mut outbox,
                &mut pending_events,
                &mut last_received,
                is_online,
            )
            .await?;
        }

        cursor = match await_cursor(
            &source,
            &token,
            &mut writer_rx,
            &mut next_arrival_id,
            &mut outbox,
            &mut pending_events,
            last_received,
            is_online,
        )
        .await?
        {
            Some(c) => c,
            None => return Ok(()),
        };
    }

    tokio::select! {
        biased;
        _ = token.cancelled() => return Ok(()),
        result = delivery_ready => {
            result
                .context("T17 delivery epoch installer dropped")?
                .context("T17 delivery epoch installation failed")?;
        }
    }

    // Final cursor recheck right before publishing Online: catch any durable
    // commits that happened while the last page was being sent.
    loop {
        cursor = match await_cursor(
            &source,
            &token,
            &mut writer_rx,
            &mut next_arrival_id,
            &mut outbox,
            &mut pending_events,
            last_received,
            is_online,
        )
        .await?
        {
            Some(c) => c,
            None => return Ok(()),
        };
        if cursor.last_sent <= last_received {
            break;
        }
        let page = match await_page(
            &source,
            last_received,
            config.catch_up_page_size.get(),
            &token,
            &mut writer_rx,
            &mut next_arrival_id,
            &mut outbox,
            &mut pending_events,
            last_received,
            is_online,
        )
        .await?
        {
            Some(p) => p,
            None => return Ok(()),
        };
        if page.is_empty() {
            return Err(DurableReplayInvariantError::new(
                "event source returned empty page before advertised cursor during final catch-up",
            )
            .into());
        }
        for frame in page {
            let seq = outbound_frame_event_seq(&frame).map_err(|_| {
                DurableReplayInvariantError::new("final catch-up frame missing durable sequence")
            })?;
            let expected = last_received.checked_add(1).ok_or_else(|| {
                DurableReplayInvariantError::new(
                    "durable event sequence exhausted during final catch-up",
                )
            })?;
            if seq != expected {
                return Err(DurableReplayInvariantError::new(format!(
                    "durable catch-up event gap during final catch-up: expected {expected}, got {seq}"
                ))
                .into());
            }
            send_with_interleave(
                &mut writer,
                frame,
                config.send_timeout,
                &token,
                &mut writer_rx,
                &mut next_arrival_id,
                &mut outbox,
                &mut pending_events,
                last_received,
                is_online,
            )
            .await?;
            last_received = seq;
            drain_next(
                &mut writer,
                config.send_timeout,
                &token,
                &mut writer_rx,
                &mut next_arrival_id,
                &mut outbox,
                &mut pending_events,
                &mut last_received,
                is_online,
            )
            .await?;
        }
    }

    // Publish Online only after reaching the durable cursor. From this point on
    // event_forwarder may deliver live frames to this writer.
    source
        .mark_delivery_online(delivery_epoch)
        .await
        .context("failed to open delivery online barrier")?;
    is_online = true;
    let _ = online.send(true);

    // Drain any CommandAcks that arrived during the final recheck and any durable
    // events queued before Online in strict order.
    drain_next(
        &mut writer,
        config.send_timeout,
        &token,
        &mut writer_rx,
        &mut next_arrival_id,
        &mut outbox,
        &mut pending_events,
        &mut last_received,
        is_online,
    )
    .await?;

    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => return Ok(()),
            frame = writer_rx.recv() => {
                let Some(frame) = frame else { return Ok(()); };
                classify_frame(
                    frame,
                    is_online,
                    last_received,
                    &mut next_arrival_id,
                    &mut outbox,
                    &mut pending_events,
                );
                drain_next(
                    &mut writer,
                    config.send_timeout,
                    &token,
                    &mut writer_rx,
                    &mut next_arrival_id,
                    &mut outbox,
                    &mut pending_events,
                    &mut last_received,
                    is_online,
                ).await?;
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
) -> Result<(), ReaderError>
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
    let mut terminal_after_pending = false;

    let result: Result<(), ReaderError> = 'task: {
        loop {
            tokio::select! {
                biased;
                _ = token.cancelled() => break 'task Ok(()),
                result = latch.wait_for(api_hello.accepted_generation), if ready.is_none() => {
                    let hydration_ready = result.map_err(ReaderError::Fatal)?;
                    if hydration_ready.generation != api_hello.accepted_generation {
                        break 'task Err(ReaderError::Fatal(anyhow!("hydration generation mismatch")));
                    }
                    ready = Some(hydration_ready);
                    for cmd in pending.drain(..) {
                        next_expected = send_validated(cmd, next_expected, &mut command_tx, &token, command_send_blocked_notify.clone()).await?;
                    }
                    if terminal_after_pending {
                        break 'task Err(ReaderError::Terminal);
                    }
                }
                result = cmd_rx.recv(),
                    if !terminal_after_pending
                        && (ready.is_some() || pending.len() < MAX_PENDING_BEFORE_READY) =>
                {
                    match result {
                        Some(Ok(cmd)) => {
                            if ready.is_some() {
                                next_expected = send_validated(cmd, next_expected, &mut command_tx, &token, command_send_blocked_notify.clone()).await?;
                            } else {
                                pending.push(cmd);
                            }
                        }
                        Some(Err(e)) if e.is::<TerminalGatewayClosed>() => {
                            if ready.is_some() || pending.is_empty() {
                                break 'task Err(ReaderError::Terminal);
                            }
                            // stdin has closed, but complete commands already read
                            // before EOF remain owned by this epoch. Keep the
                            // hydration gate closed, then flush them before the
                            // single-connection process exits successfully.
                            terminal_after_pending = true;
                        }
                        Some(Err(e)) if e.is::<GatewayClosed>() => break 'task Ok(()),
                        Some(Err(e)) => break 'task Err(ReaderError::Reconnect(e)),
                        None => break 'task Err(ReaderError::Reconnect(anyhow!("command reader closed unexpectedly"))),
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
                next_expected.checked_add(1).ok_or_else(|| {
                    anyhow::Error::new(DurableReplayInvariantError::new(
                        "command sequence exhausted after forwarding u64::MAX",
                    ))
                })
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
    use tokio::sync::{mpsc, watch};

    use super::*;
    use crate::gateway::stdio::{InjectedStdioGateway, SingleConnectionConnector};
    use crate::gateway::wire::to_wire_frame;
    use crate::gateway::{
        Command, CommandAck, CommandAckStatus, CommandEnvelope, CommandId, CommandRejectReason,
        Envelope, Gateway, GatewayClosed, GatewayReader, GatewayWriter, InboundCommand,
        OutboundFrame,
    };
    use crate::runtime::contracts::{GenerationRecoveryFence, ProcessGenerationLease};
    use crate::store::{
        DeliveryChannelBuilder, DeliveryFrame, DeliveryMode, DeliveryPump, HydrationOutcome, Store,
        insert_test_durable_event,
    };

    struct TestDigestFactory;

    impl crate::gateway::CommandDigestFactory for TestDigestFactory {
        fn start(&self) -> Box<dyn crate::gateway::IncrementalCommandDigest> {
            Box::new(TestDigest(Sha256::new()))
        }
    }

    struct TestDigest(Sha256);

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
        delivery_authorization: DeliveryAuthorization,
    }

    impl CountingCredentialProvider {
        fn new(prefix: impl Into<String>) -> Self {
            Self {
                counter: Arc::new(AtomicU64::new(0)),
                prefix: prefix.into(),
                tokens: Arc::new(std::sync::Mutex::new(Vec::new())),
                delivery_authorization: DeliveryAuthorization::Raw,
            }
        }

        fn with_delivery_authorization(
            mut self,
            delivery_authorization: DeliveryAuthorization,
        ) -> Self {
            self.delivery_authorization = delivery_authorization;
            self
        }
    }

    #[async_trait]
    impl CredentialProvider for CountingCredentialProvider {
        async fn fresh_credential(&mut self) -> Result<GatewayCredential> {
            let n = self.counter.fetch_add(1, Ordering::SeqCst);
            let token = format!("{}-{}", self.prefix, n);
            self.tokens.lock().unwrap().push(token.clone());
            Ok(GatewayCredential::new(token, self.delivery_authorization))
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
        /// Notified to release a blocked writer; if None, the writer blocks forever.
        release: Option<Arc<Notify>>,
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
            let (blocked, block_notify, release) = {
                let mut sent = self.sent.lock().unwrap();
                if let Some(n) = self.fail_after
                    && sent.len() >= n
                {
                    bail!("writer failure");
                }
                sent.push(frame);
                let blocked = self.block_after.is_some_and(|n| sent.len() == n);
                (blocked, self.block_notify.clone(), self.release.clone())
            };
            if blocked {
                if let Some(notify) = block_notify {
                    notify.notify_one();
                }
                if let Some(release) = release {
                    release.notified().await;
                } else {
                    std::future::pending::<()>().await;
                }
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
        delivery_authorization: DeliveryAuthorization,
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
                    release: None,
                },
                sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
                hello_generation: None,
                last_received_event_seq: 0,
                hello_delay: None,
                hello_error: None,
                delivery_authorization: DeliveryAuthorization::Raw,
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

        fn with_delivery_authorization(
            mut self,
            delivery_authorization: DeliveryAuthorization,
        ) -> Self {
            self.delivery_authorization = delivery_authorization;
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
                delivery_authorization: self.delivery_authorization,
            })
        }

        fn split(self) -> (Self::Reader, Self::Writer) {
            (self.reader, self.writer)
        }
    }

    struct FixedNextGateway {
        next_command_seq: u64,
        reader: MockGatewayReader,
        writer: MockGatewayWriter,
    }

    impl FixedNextGateway {
        fn new(next_command_seq: u64, commands: VecDeque<Result<InboundCommand>>) -> Self {
            let gateway = MockGateway::new(commands);
            Self {
                next_command_seq,
                reader: gateway.reader,
                writer: gateway.writer,
            }
        }
    }

    #[async_trait]
    impl Gateway for FixedNextGateway {
        type Reader = MockGatewayReader;
        type Writer = MockGatewayWriter;

        async fn authenticate_hello(
            &mut self,
            hello: AgentHello,
        ) -> std::result::Result<ApiHello, HelloError> {
            Ok(ApiHello {
                accepted_generation: hello.generation,
                last_received_event_seq: 0,
                next_command_seq: self.next_command_seq,
                delivery_authorization: DeliveryAuthorization::Raw,
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
            event_buffer_size: NonZeroUsize::new(16).unwrap(),
            command_buffer_size: NonZeroUsize::new(16).unwrap(),
            catch_up_page_size: NonZeroUsize::new(16).unwrap(),
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
    async fn delivery_authorization_mismatch_is_fatal_before_source_binding() {
        #[derive(Clone)]
        struct BindingSource {
            binds: Arc<AtomicU64>,
        }

        #[async_trait]
        impl DurableSource for BindingSource {
            fn bind_delivery_authorization(
                &self,
                _authorization: DeliveryAuthorization,
            ) -> Result<Self> {
                self.binds.fetch_add(1, Ordering::SeqCst);
                Ok(self.clone())
            }

            async fn event_cursor(&self) -> Result<EventCursors> {
                Ok(EventCursors::default())
            }

            async fn events_after(
                &self,
                _after_seq: u64,
                _limit: usize,
            ) -> Result<Vec<OutboundFrame>> {
                panic!("authorization mismatch must fail before durable replay")
            }

            async fn command_cursors(&self) -> Result<CommandCursors> {
                Ok(CommandCursors::default())
            }
        }

        let binds = Arc::new(AtomicU64::new(0));
        let gateway = MockGateway::new(VecDeque::new())
            .with_delivery_authorization(DeliveryAuthorization::RedactionOnly);
        let connector = MockConnector::new(
            Arc::new(std::sync::Mutex::new(Vec::new())),
            VecDeque::from([Ok(gateway)]),
        );
        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token"),
            BindingSource {
                binds: binds.clone(),
            },
            StaticHydrationLatch(HydrationReady {
                generation: ProcessGeneration::from_wire(7).unwrap(),
                receipt_identity: "receipt-1".to_owned(),
            }),
            make_config(),
        );

        let error = supervisor.start().join().await.unwrap_err();
        assert!(
            format!("{error:#}").contains("delivery authorization mismatch"),
            "unexpected error: {error:#}"
        );
        assert_eq!(
            binds.load(Ordering::SeqCst),
            0,
            "mismatched authorization must fail before raw-capable source binding"
        );
    }

    #[tokio::test]
    async fn delivery_epoch_runtime_preserves_task_panic_with_live_failure_sender() {
        let (failure_tx, failure_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async {
            panic!("test delivery forwarder panic");
        });
        let mut runtime = DeliveryEpochRuntime::new(failure_rx, task);

        let completion = tokio::time::timeout(Duration::from_secs(1), runtime.failed())
            .await
            .expect("task termination must wake the delivery failure branch");

        assert!(matches!(
            &completion,
            DeliveryEpochCompletion::Task(Err(join_err)) if join_err.is_panic()
        ));
        assert!(!failure_tx.is_closed());
        assert!(runtime.join().await.is_ok());
    }

    #[tokio::test]
    async fn delivery_task_panic_is_fatal_after_one_epoch_invalidation() {
        #[derive(Clone)]
        struct PanickingDeliverySource {
            installs: Arc<AtomicU64>,
            invalidations: Arc<AtomicU64>,
            failure_tx: Arc<Mutex<Option<mpsc::UnboundedSender<String>>>>,
        }

        #[async_trait]
        impl DurableSource for PanickingDeliverySource {
            async fn event_cursor(&self) -> Result<EventCursors> {
                Ok(EventCursors::default())
            }

            async fn events_after(
                &self,
                _after_seq: u64,
                _limit: usize,
            ) -> Result<Vec<OutboundFrame>> {
                Ok(Vec::new())
            }

            async fn command_cursors(&self) -> Result<CommandCursors> {
                Ok(CommandCursors::default())
            }

            async fn install_delivery_epoch(
                &self,
                _epoch: DeliveryEpoch,
                _catch_up_from_seq: u64,
                _sink: EventSender,
                _cancel: CancellationToken,
            ) -> Result<Option<DeliveryEpochRuntime>> {
                self.installs.fetch_add(1, Ordering::SeqCst);
                let (failure_tx, failure_rx) = mpsc::unbounded_channel();
                *self.failure_tx.lock().unwrap() = Some(failure_tx);
                let task = tokio::spawn(async {
                    panic!("test delivery forwarder panic");
                });
                Ok(Some(DeliveryEpochRuntime::new(failure_rx, task)))
            }

            async fn invalidate_delivery_epoch(&self, _epoch: DeliveryEpoch) -> Result<()> {
                self.invalidations.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        let source = PanickingDeliverySource {
            installs: Arc::new(AtomicU64::new(0)),
            invalidations: Arc::new(AtomicU64::new(0)),
            failure_tx: Arc::new(Mutex::new(None)),
        };
        let installs = source.installs.clone();
        let invalidations = source.invalidations.clone();
        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let credentials = CountingCredentialProvider::new("token");
        let connect_attempts = credentials.counter.clone();
        let supervisor = ConnectionSupervisor::new(
            MockConnector::new(
                sent_hellos.clone(),
                VecDeque::from([Ok(MockGateway::new(VecDeque::new()))]),
            ),
            credentials,
            source,
            StaticHydrationLatch(HydrationReady {
                generation: ProcessGeneration::from_wire(7).unwrap(),
                receipt_identity: "receipt-1".to_owned(),
            }),
            make_config(),
        );

        let error = tokio::time::timeout(Duration::from_secs(1), supervisor.start().join())
            .await
            .expect("delivery task panic must terminate the supervisor")
            .expect_err("delivery task panic must be fatal");

        assert!(format!("{error:#}").contains("delivery epoch task panicked"));
        assert_eq!(installs.load(Ordering::SeqCst), 1);
        assert_eq!(invalidations.load(Ordering::SeqCst), 1);
        assert_eq!(connect_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(sent_hellos.lock().unwrap().len(), 1);
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
                release: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
            delivery_authorization: DeliveryAuthorization::Raw,
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
                release: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
            delivery_authorization: DeliveryAuthorization::Raw,
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
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
            delivery_authorization: DeliveryAuthorization::Raw,
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
                release: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
            delivery_authorization: DeliveryAuthorization::Raw,
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
                release: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
            delivery_authorization: DeliveryAuthorization::Raw,
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
        handle.events.send((epoch1, event_frame(99))).await.unwrap();

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
        handle.events.send((epoch2, event_frame(1))).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.abort();
        assert!(handle.join().await.is_ok());

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(outbound_frame_event_seq(&sent[0]).unwrap(), 1);
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
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
            delivery_authorization: DeliveryAuthorization::Raw,
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
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
            delivery_authorization: DeliveryAuthorization::Raw,
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
    async fn send_validated_forwards_maximum_then_returns_typed_exhaustion() {
        let (mut tx, mut rx) = mpsc::channel::<InboundCommand>(1);
        let error = send_validated(
            valid_command(u64::MAX, "00000000-0000-4000-8000-000000000001"),
            u64::MAX,
            &mut tx,
            &CancellationToken::new(),
            None,
        )
        .await
        .expect_err("maximum command sequence must exhaust the cursor");
        assert!(error.is::<DurableReplayInvariantError>());
        assert_eq!(inbound_command_seq(&rx.recv().await.unwrap()), u64::MAX);
    }

    #[tokio::test]
    async fn event_forwarder_admission_boundary_drops_pre_online_volatile() {
        // Deterministic counterexample: a pre-Online volatile frame is queued
        // behind work that blocks the forwarder. When Online flips before the
        // forwarder unblocks, an admission-boundary rule drops the volatile; a
        // later observation of the `online` watch would have wrongly forwarded it.
        let epoch = DeliveryEpoch(1);
        let (events_tx, events_rx) = mpsc::channel::<(DeliveryEpoch, bool, OutboundFrame)>(1);
        // Pre-fill a capacity-one writer channel so the forwarder blocks on the
        // ack until the consumer is released, simulating backpressure across the
        // Online boundary.
        let (writer_tx, mut writer_rx) = mpsc::channel::<OutboundFrame>(1);
        let blocker = OutboundFrame::CommandAck {
            ack: CommandAck {
                seq: 0,
                command_id: "00000000-0000-4000-8000-000000000000".to_owned(),
                status: CommandAckStatus::Received,
                reject_reason: None,
            },
        };
        writer_tx
            .send(blocker)
            .await
            .expect("writer channel should accept the blocker");
        let current_writer: CurrentWriterSlot =
            Arc::new(std::sync::Mutex::new(Some((epoch, writer_tx))));
        let cancel = CancellationToken::new();

        let forwarder = tokio::spawn(event_forwarder(
            events_rx,
            current_writer.clone(),
            cancel.child_token(),
        ));

        let ack = OutboundFrame::CommandAck {
            ack: CommandAck {
                seq: 1,
                command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                status: CommandAckStatus::Received,
                reject_reason: None,
            },
        };
        let volatile = OutboundFrame::Event {
            envelope: Envelope {
                seq: None,
                conversation_id: "conversation-1".to_owned(),
                event: serde_json::json!({"type": "delta"}),
            },
        };

        // Admit both frames while Offline. The ack blocks behind the pre-filled
        // writer channel; the volatile sits in events_rx behind the ack.
        events_tx
            .send((epoch, false, ack))
            .await
            .expect("ack should be admitted while offline");
        events_tx
            .send((epoch, false, volatile.clone()))
            .await
            .expect("volatile should be queued while offline");

        // Simulate Online flipping while the forwarder is still blocked.
        // With the old watch-channel-based rule the forwarder would read `true`
        // when it finally dequeued the volatile and would forward it live. The
        // admission boundary below must instead drop it.

        // Release the writer by consuming the blocker, then consume the ack. The
        // forwarder unblocks, dequeues the volatile, and must drop it because it
        // was admitted offline.
        let received_blocker = writer_rx.recv().await.expect("blocker must be present");
        assert!(matches!(received_blocker, OutboundFrame::CommandAck { .. }));
        let received_ack = writer_rx.recv().await.expect("ack must be delivered");
        assert!(matches!(received_ack, OutboundFrame::CommandAck { .. }));

        // Give the forwarder a chance to process the volatile, then close the
        // event channel and join the forwarder so the writer_rx closes.
        drop(events_tx);
        tokio::time::timeout(Duration::from_millis(100), forwarder)
            .await
            .expect("forwarder should exit")
            .expect("forwarder should not panic");

        // Drop the current_writer slot so the remaining writer_tx clone is
        // released and writer_rx.recv() returns None when no frame was sent.
        drop(current_writer);
        assert!(
            writer_rx.recv().await.is_none(),
            "pre-Online volatile must be dropped, not delivered after Online"
        );

        // Positive case: the same volatile frame admitted while Online is delivered.
        let (events_tx2, events_rx2) = mpsc::channel::<(DeliveryEpoch, bool, OutboundFrame)>(1);
        let (writer_tx2, mut writer_rx2) = mpsc::channel::<OutboundFrame>(1);
        let current_writer2: CurrentWriterSlot =
            Arc::new(std::sync::Mutex::new(Some((epoch, writer_tx2))));
        let cancel2 = CancellationToken::new();
        let forwarder2 = tokio::spawn(event_forwarder(
            events_rx2,
            current_writer2,
            cancel2.child_token(),
        ));

        events_tx2
            .send((epoch, true, volatile))
            .await
            .expect("volatile admitted while Online should be accepted");
        let received = writer_rx2
            .recv()
            .await
            .expect("post-Online volatile must be delivered");
        assert!(matches!(received, OutboundFrame::Event { .. }));

        drop(events_tx2);
        tokio::time::timeout(Duration::from_millis(100), forwarder2)
            .await
            .expect("forwarder should exit")
            .expect("forwarder should not panic");
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
    async fn stdio_single_connection_eof_is_terminal_success() {
        let gateway = InjectedStdioGateway::new(
            tokio::io::BufReader::new(tokio::io::empty()),
            tokio::io::sink(),
            Arc::new(TestDigestFactory),
        );
        let supervisor = ConnectionSupervisor::new(
            SingleConnectionConnector::new(gateway),
            CountingCredentialProvider::new("token"),
            MockDurableSource::new(CommandCursors::default()),
            StaticHydrationLatch(HydrationReady {
                generation: ProcessGeneration::from_wire(7).unwrap(),
                receipt_identity: "receipt-1".to_owned(),
            }),
            make_config(),
        );

        tokio::time::timeout(Duration::from_secs(1), supervisor.start().join())
            .await
            .expect("stdio EOF must terminate promptly")
            .expect("stdio EOF must be a successful terminal boundary");
    }

    #[tokio::test]
    async fn stdio_eof_waits_for_hydration_and_flushes_commands_already_read() {
        use std::io::Cursor;

        let generation = ProcessGeneration::from_wire(7).unwrap();
        let input = br#"{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","command":{"type":"abort"}}"#
            .to_vec();
        let gateway = InjectedStdioGateway::new(
            tokio::io::BufReader::new(Cursor::new(input)),
            tokio::io::sink(),
            Arc::new(TestDigestFactory),
        );
        let (latch, latch_tx) = DynamicHydrationLatch::new();
        let mut config = make_config();
        config.generation = generation;
        let supervisor = ConnectionSupervisor::new(
            SingleConnectionConnector::new(gateway),
            CountingCredentialProvider::new("token"),
            MockDurableSource::new(CommandCursors::default()),
            latch,
            config,
        );
        let mut handle = supervisor.start();

        assert!(
            tokio::time::timeout(Duration::from_millis(50), handle.commands.recv())
                .await
                .is_err(),
            "a command read before EOF must remain held while hydration is NotReady"
        );
        latch_tx
            .send(HydrationState::Ready(HydrationReady {
                generation,
                receipt_identity: "stdio-eof-ready".to_owned(),
            }))
            .unwrap();

        let command = tokio::time::timeout(Duration::from_secs(1), handle.commands.recv())
            .await
            .expect("held stdio command must flush after hydration")
            .expect("held stdio command must not be lost at EOF");
        assert_eq!(inbound_command_seq(&command), 1);
        tokio::time::timeout(Duration::from_secs(1), handle.join())
            .await
            .expect("stdio supervisor must terminate after flushing held input")
            .expect("terminal stdio EOF after a flush must be successful");
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
        .await;
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
                release: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
            delivery_authorization: DeliveryAuthorization::Raw,
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
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
            delivery_authorization: DeliveryAuthorization::Raw,
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
            delivery_authorization: DeliveryAuthorization::Raw,
        };

        // The first legal resend point is applied + 1, not received + 1.
        // With re-fetched cursors (applied=5, received=10) the expected next
        // sequence is 6; 11 would skip the received-but-unapplied commands.
        validate_hello(&source, &agent, &api).await.unwrap();
    }

    #[tokio::test]
    async fn validate_hello_accepts_applied_cursor_advancement() {
        // If the local applied cursor advances between building AgentHello and
        // validating the peer's claim, the claim must be judged against the
        // original AgentHello snapshot, not the newer applied cursor.
        let cursor_calls = VecDeque::from([
            CommandCursors {
                received: 5,
                applied: 5,
            },
            CommandCursors {
                received: 10,
                applied: 7,
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
                let mut cursors = self.cursors.lock().unwrap();
                if cursors.len() > 1 {
                    Ok(cursors.pop_front().unwrap())
                } else {
                    Ok(*cursors.front().unwrap())
                }
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
        assert_eq!(agent.last_applied_command_seq, 5);

        // The peer responded to the original snapshot: it may send the original
        // applied+1 command, even though the local applied cursor has since
        // moved to 7.
        let api = ApiHello {
            accepted_generation: ProcessGeneration::from_wire(7).unwrap(),
            last_received_event_seq: 0,
            next_command_seq: 6,
            delivery_authorization: DeliveryAuthorization::Raw,
        };
        validate_hello(&source, &agent, &api)
            .await
            .expect("original legal resend point must be accepted");

        // The API may start at an already-applied command when its terminal ACK
        // was lost; the durable consumer will re-ACK it without reapplying it.
        let api = ApiHello {
            accepted_generation: ProcessGeneration::from_wire(7).unwrap(),
            last_received_event_seq: 0,
            next_command_seq: 5,
            delivery_authorization: DeliveryAuthorization::Raw,
        };
        validate_hello(&source, &agent, &api)
            .await
            .expect("locally terminal command must remain replayable");

        let api = ApiHello {
            accepted_generation: ProcessGeneration::from_wire(7).unwrap(),
            last_received_event_seq: 0,
            next_command_seq: 0,
            delivery_authorization: DeliveryAuthorization::Raw,
        };
        assert!(
            validate_hello(&source, &agent, &api).await.is_err(),
            "command sequence zero is not a valid replay boundary"
        );
    }

    #[test]
    fn hello_dto_lossless_decimal_boundaries() {
        // The full u64/i64 domains must round-trip as canonical decimal strings.
        let json_safe = 9_007_199_254_740_991_u64; // 2^53 - 1, the old JS-safe cap
        let over_json_safe = json_safe + 1;
        let i64_max = i64::MAX as u64;
        let over_i64 = i64_max + 1;
        let u64_max = u64::MAX;
        let over_u64 = "18446744073709551616"; // u64::MAX + 1

        // Generation boundaries: 0, JSON-safe max, JSON-safe max + 1, i64::MAX.
        for gen_value in [0, json_safe, over_json_safe, i64_max] {
            let agent = AgentHello {
                agent_id: "a".to_owned(),
                generation: ProcessGeneration::from_wire(gen_value).unwrap(),
                last_sent_event_seq: 0,
                last_received_command_seq: 0,
                last_applied_command_seq: 0,
            };
            let text = serde_json::to_string(&agent).expect("serialize agent hello");
            assert!(
                text.contains(&format!(r#""generation":"{}""#, gen_value)),
                "generation must be a canonical decimal string on the wire: {text}"
            );
            let parsed: AgentHello = serde_json::from_str(&text).expect("deserialize agent hello");
            assert_eq!(parsed.generation, agent.generation);
        }

        // Seq boundaries: 0, JSON-safe max, JSON-safe max + 1, u64::MAX.
        for seq in [0, json_safe, over_json_safe, u64_max] {
            let api = ApiHello {
                accepted_generation: ProcessGeneration::from_wire(0).unwrap(),
                last_received_event_seq: seq,
                next_command_seq: seq,
                delivery_authorization: DeliveryAuthorization::Raw,
            };
            let text = serde_json::to_string(&api).expect("serialize api hello");
            let parsed: ApiHello = serde_json::from_str(&text).expect("deserialize api hello");
            assert_eq!(parsed.last_received_event_seq, seq);
            assert_eq!(parsed.next_command_seq, seq);
        }

        // i64::MAX and u64::MAX together in one hello.
        let agent_i64_max = AgentHello {
            agent_id: "a".to_owned(),
            generation: ProcessGeneration::from_wire(i64_max).unwrap(),
            last_sent_event_seq: u64_max,
            last_received_command_seq: u64_max,
            last_applied_command_seq: u64_max,
        };
        let text = serde_json::to_string(&agent_i64_max).expect("serialize i64::MAX");
        assert!(text.contains(r#""generation":"9223372036854775807""#));
        let parsed: AgentHello = serde_json::from_str(&text).expect("deserialize i64::MAX");
        assert_eq!(parsed, agent_i64_max);

        // Generation beyond i64::MAX is rejected on the wire.
        let agent_json = format!(
            r#"{{"agent_id":"a","generation":"{over_i64}","last_sent_event_seq":"0","last_received_command_seq":"0","last_applied_command_seq":"0"}}"#
        );
        assert!(serde_json::from_str::<AgentHello>(&agent_json).is_err());

        let api_json = format!(
            r#"{{"accepted_generation":"{over_i64}","last_received_event_seq":"0","next_command_seq":"1","delivery_authorization":"raw"}}"#
        );
        assert!(serde_json::from_str::<ApiHello>(&api_json).is_err());

        // u64 overflow is rejected.
        let agent_overflow = format!(
            r#"{{"agent_id":"a","generation":"0","last_sent_event_seq":"{over_u64}","last_received_command_seq":"0","last_applied_command_seq":"0"}}"#
        );
        assert!(serde_json::from_str::<AgentHello>(&agent_overflow).is_err());

        let api_overflow = format!(
            r#"{{"accepted_generation":"0","last_received_event_seq":"{over_u64}","next_command_seq":"1","delivery_authorization":"raw"}}"#
        );
        assert!(serde_json::from_str::<ApiHello>(&api_overflow).is_err());

        // Old numeric encodings are no longer accepted; the wire uses strings.
        let agent_numeric = r#"{"agent_id":"a","generation":1,"last_sent_event_seq":0,"last_received_command_seq":0,"last_applied_command_seq":0}"#;
        assert!(serde_json::from_str::<AgentHello>(agent_numeric).is_err());
        let api_numeric = r#"{"accepted_generation":1,"last_received_event_seq":0,"next_command_seq":1,"delivery_authorization":"raw"}"#;
        assert!(serde_json::from_str::<ApiHello>(api_numeric).is_err());
    }

    #[test]
    fn hello_dto_rejects_malformed_unknown_and_trailing_data() {
        let agent_base = r#"{"agent_id":"a","generation":"1","last_sent_event_seq":"0","last_received_command_seq":"0","last_applied_command_seq":"0"}"#;
        let api_base = r#"{"accepted_generation":"1","last_received_event_seq":"0","next_command_seq":"1","delivery_authorization":"raw"}"#;

        assert!(serde_json::from_str::<AgentHello>(agent_base).is_ok());
        assert!(serde_json::from_str::<ApiHello>(api_base).is_ok());

        // Malformed decimal strings must be rejected fail-closed.
        for malformed in [
            "\"01\"",
            "\"00\"",
            "\"+1\"",
            "\"-1\"",
            "\" 1\"",
            "\"1 \"",
            "\"1.0\"",
            "\"1e0\"",
            "\"0x1\"",
            "\"not-a-generation\"",
            "\"\"",
        ] {
            let agent_bad = agent_base.replace(
                "\"generation\":\"1\"",
                &format!("\"generation\":{malformed}"),
            );
            assert!(
                serde_json::from_str::<AgentHello>(&agent_bad).is_err(),
                "AgentHello must reject malformed generation {malformed}"
            );

            let api_bad = api_base.replace(
                "\"accepted_generation\":\"1\"",
                &format!("\"accepted_generation\":{malformed}"),
            );
            assert!(
                serde_json::from_str::<ApiHello>(&api_bad).is_err(),
                "ApiHello must reject malformed accepted_generation {malformed}"
            );

            let agent_bad_seq = agent_base.replace(
                "\"last_sent_event_seq\":\"0\"",
                &format!("\"last_sent_event_seq\":{malformed}"),
            );
            assert!(
                serde_json::from_str::<AgentHello>(&agent_bad_seq).is_err(),
                "AgentHello must reject malformed seq {malformed}"
            );
        }

        // Unknown fields continue to be rejected.
        let agent_unknown = r#"{"agent_id":"a","generation":"1","last_sent_event_seq":"0","last_received_command_seq":"0","last_applied_command_seq":"0","extra":1}"#;
        assert!(serde_json::from_str::<AgentHello>(agent_unknown).is_err());

        let api_unknown = r#"{"accepted_generation":"1","last_received_event_seq":"0","next_command_seq":"1","delivery_authorization":"raw","extra":1}"#;
        assert!(serde_json::from_str::<ApiHello>(api_unknown).is_err());

        // Trailing data after a valid object must also be rejected.
        let agent_trailing = r#"{"agent_id":"a","generation":"1","last_sent_event_seq":"0","last_received_command_seq":"0","last_applied_command_seq":"0"}{"extra":1}"#;
        assert!(serde_json::from_str::<AgentHello>(agent_trailing).is_err());

        let api_trailing = r#"{"accepted_generation":"1","last_received_event_seq":"0","next_command_seq":"1","delivery_authorization":"raw"}{"extra":1}"#;
        assert!(serde_json::from_str::<ApiHello>(api_trailing).is_err());
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
            delivery_authorization: DeliveryAuthorization::Raw,
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
    async fn validate_hello_command_cursor_cannot_skip_nonterminal_prefix() {
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

        // A locally terminal command remains a valid replay point when the API
        // did not durably record its terminal ACK.
        let api = ApiHello {
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 5,
            delivery_authorization: DeliveryAuthorization::Raw,
        };
        validate_hello(&StaticSource(cursor), &agent, &api)
            .await
            .expect("terminal ACK recovery must allow replay at seq 5");

        // Exactly applied+1 is the normal catch-up boundary and is allowed.
        let api = ApiHello {
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 6,
            delivery_authorization: DeliveryAuthorization::Raw,
        };
        validate_hello(&StaticSource(cursor), &agent, &api)
            .await
            .unwrap();

        // A cursor after applied+1 skips a locally nonterminal command.
        let api = ApiHello {
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 7,
            delivery_authorization: DeliveryAuthorization::Raw,
        };
        assert!(
            validate_hello(&StaticSource(cursor), &agent, &api)
                .await
                .is_err()
        );

        let api = ApiHello {
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 11,
            delivery_authorization: DeliveryAuthorization::Raw,
        };
        assert!(
            validate_hello(&StaticSource(cursor), &agent, &api)
                .await
                .is_err()
        );

        // Ahead of received+1 is also fatal.
        let api = ApiHello {
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 12,
            delivery_authorization: DeliveryAuthorization::Raw,
        };
        assert!(
            validate_hello(&StaticSource(cursor), &agent, &api)
                .await
                .is_err(),
            "next_command_seq beyond received+1 must be fatal"
        );

        let api = ApiHello {
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 0,
            delivery_authorization: DeliveryAuthorization::Raw,
        };
        assert!(
            validate_hello(&StaticSource(cursor), &agent, &api)
                .await
                .is_err(),
            "command seq zero must be fatal"
        );
    }

    #[tokio::test]
    async fn supervisor_rejects_hello_that_skips_nonterminal_commands() {
        let cursor = CommandCursors {
            received: 10,
            applied: 5,
        };
        let supervisor = ConnectionSupervisor::new(
            SingleConnectionConnector::new(FixedNextGateway::new(7, VecDeque::new())),
            CountingCredentialProvider::new("token"),
            MockDurableSource::new(cursor),
            StaticHydrationLatch(HydrationReady {
                generation: ProcessGeneration::from_wire(7).unwrap(),
                receipt_identity: "receipt-1".to_owned(),
            }),
            make_config(),
        );
        let error = tokio::time::timeout(Duration::from_secs(1), supervisor.start().join())
            .await
            .expect("invalid hello must terminate promptly")
            .expect_err("skipping seq 6 must be fatal");
        assert!(
            format!("{error:#}").contains("outside durable bounds"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn lost_terminal_ack_replays_applied_superseded_and_rejected_commands() {
        struct TerminalReplayGateway {
            reader: MockGatewayReader,
            writer: MockGatewayWriter,
            terminal_seq: u64,
        }

        #[async_trait]
        impl Gateway for TerminalReplayGateway {
            type Reader = MockGatewayReader;
            type Writer = MockGatewayWriter;

            async fn authenticate_hello(
                &mut self,
                hello: AgentHello,
            ) -> std::result::Result<ApiHello, HelloError> {
                Ok(ApiHello {
                    accepted_generation: hello.generation,
                    last_received_event_seq: 0,
                    next_command_seq: self.terminal_seq,
                    delivery_authorization: DeliveryAuthorization::Raw,
                })
            }

            fn split(self) -> (Self::Reader, Self::Writer) {
                (self.reader, self.writer)
            }
        }

        let terminal_seq = 5;
        let cases = [
            (
                CommandAckStatus::Applied,
                None,
                valid_command(terminal_seq, "00000000-0000-4000-8000-000000000005"),
            ),
            (
                CommandAckStatus::Superseded,
                None,
                valid_command(terminal_seq, "00000000-0000-4000-8000-000000000006"),
            ),
            (
                CommandAckStatus::Rejected,
                Some("oversized".to_owned()),
                rejected_oversized_command(
                    terminal_seq,
                    "00000000-0000-4000-8000-000000000007",
                    1024 * 1024 + 1,
                ),
            ),
        ];

        for (terminal_status, reject_reason, command) in cases {
            let sent = Arc::new(Mutex::new(Vec::new()));
            let gateway = TerminalReplayGateway {
                reader: MockGatewayReader {
                    commands: VecDeque::from([Ok(command)]),
                    panic: false,
                    on_empty: None,
                },
                writer: MockGatewayWriter {
                    fail_after: None,
                    sent: sent.clone(),
                    delay: None,
                    block_after: None,
                    block_notify: None,
                    release: None,
                },
                terminal_seq,
            };
            let source = MockDurableSource::new(CommandCursors {
                received: terminal_seq,
                applied: terminal_seq,
            });
            let supervisor = ConnectionSupervisor::new(
                SingleConnectionConnector::new(gateway),
                CountingCredentialProvider::new("token"),
                source,
                StaticHydrationLatch(HydrationReady {
                    generation: ProcessGeneration::from_wire(7).unwrap(),
                    receipt_identity: format!("{terminal_status:?}-receipt"),
                }),
                make_config(),
            );
            let mut handle = supervisor.start();

            let replay = tokio::time::timeout(Duration::from_secs(1), handle.commands.recv())
                .await
                .expect("terminal replay must not stall")
                .expect("terminal replay must be forwarded to durable dedupe");
            assert_eq!(
                inbound_command_seq(&replay),
                terminal_seq,
                "{terminal_status:?} replay must preserve its canonical seq"
            );
            let command_id = match replay {
                InboundCommand::Valid(envelope) => envelope.command_id.to_string(),
                InboundCommand::Invalid { command_id, .. } => command_id.to_string(),
            };

            let epoch = loop {
                if let Some(epoch) = *handle.epochs.borrow() {
                    break epoch;
                }
                handle
                    .epochs
                    .changed()
                    .await
                    .expect("epoch watch must stay open");
            };
            handle
                .events
                .send((
                    epoch,
                    OutboundFrame::CommandAck {
                        ack: CommandAck {
                            seq: terminal_seq,
                            command_id,
                            status: terminal_status,
                            reject_reason,
                        },
                    },
                ))
                .await
                .expect("saved terminal ACK must enter the epoch");

            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if sent.lock().unwrap().iter().any(|frame| {
                        matches!(
                            frame,
                            OutboundFrame::CommandAck { ack }
                                if ack.seq == terminal_seq && ack.status == terminal_status
                        )
                    }) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("saved terminal ACK must be delivered after replay");

            handle.abort();
            handle
                .join()
                .await
                .expect("terminal replay epoch must close cleanly");
        }
    }

    #[tokio::test]
    async fn validate_hello_command_applied_cursor_u64_max_fail_closed() {
        // applied = u64::MAX leaves no room for a legal next_command_seq.
        let cursor = CommandCursors {
            received: u64::MAX,
            applied: u64::MAX,
        };
        let agent = AgentHello {
            agent_id: "test".to_owned(),
            generation: ProcessGeneration::from_wire(7).unwrap(),
            last_sent_event_seq: 0,
            last_received_command_seq: cursor.received,
            last_applied_command_seq: cursor.applied,
        };
        let api = ApiHello {
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: u64::MAX,
            delivery_authorization: DeliveryAuthorization::Raw,
        };
        let error = validate_hello(&MockDurableSource::new(cursor), &agent, &api)
            .await
            .expect_err("applied cursor at u64::MAX must fail closed");
        assert!(
            matches!(
                &error,
                SupervisorError::Fatal(error) if error.is::<DurableReplayInvariantError>()
            ),
            "cursor exhaustion must preserve the typed permanent failure: {error:?}"
        );
    }

    #[tokio::test]
    async fn supervisor_forwards_u64_max_once_then_fails_typed_permanent() {
        let command_id = "00000000-0000-4000-8000-000000000001";
        let gateway = FixedNextGateway::new(
            u64::MAX,
            VecDeque::from([Ok(valid_command(u64::MAX, command_id))]),
        );
        let supervisor = ConnectionSupervisor::new(
            SingleConnectionConnector::new(gateway),
            CountingCredentialProvider::new("token"),
            MockDurableSource::new(CommandCursors {
                received: u64::MAX - 1,
                applied: u64::MAX - 1,
            }),
            StaticHydrationLatch(HydrationReady {
                generation: ProcessGeneration::from_wire(7).unwrap(),
                receipt_identity: "receipt-1".to_owned(),
            }),
            make_config(),
        );
        let mut handle = supervisor.start();
        let command = tokio::time::timeout(Duration::from_secs(1), handle.commands.recv())
            .await
            .expect("maximum command must be forwarded")
            .expect("maximum command must be present");
        assert_eq!(inbound_command_seq(&command), u64::MAX);

        let error = tokio::time::timeout(Duration::from_secs(1), handle.join())
            .await
            .expect("cursor exhaustion must terminate promptly")
            .expect_err("cursor exhaustion must be fatal");
        assert!(
            error.is::<DurableReplayInvariantError>(),
            "typed permanent error must survive supervisor join: {error:#}"
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
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
            delivery_authorization: DeliveryAuthorization::Raw,
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
    async fn queued_volatile_does_not_overtake_preceding_durable_event() {
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut writer = MockGatewayWriter {
            fail_after: None,
            sent: sent.clone(),
            delay: None,
            block_after: None,
            block_notify: None,
            release: None,
        };
        let (writer_tx, mut writer_rx) = mpsc::channel(1);
        let token = CancellationToken::new();
        let mut next_arrival_id = 0;
        let mut outbox = VecDeque::new();
        let mut pending_events = BTreeMap::new();
        let mut last_received = 0;

        classify_frame(
            event_frame(1),
            true,
            last_received,
            &mut next_arrival_id,
            &mut outbox,
            &mut pending_events,
        );
        classify_frame(
            OutboundFrame::Event {
                envelope: Envelope {
                    seq: None,
                    conversation_id: "conversation-1".to_owned(),
                    event: serde_json::json!({"type": "message_update"}),
                },
            },
            true,
            last_received,
            &mut next_arrival_id,
            &mut outbox,
            &mut pending_events,
        );

        drain_next(
            &mut writer,
            Duration::from_secs(1),
            &token,
            &mut writer_rx,
            &mut next_arrival_id,
            &mut outbox,
            &mut pending_events,
            &mut last_received,
            true,
        )
        .await
        .expect("drain queued frames");
        drop(writer_tx);

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert!(
            matches!(
                &sent[0],
                OutboundFrame::Event { envelope } if envelope.seq == Some(1)
            ),
            "the preceding durable event must be sent first: {sent:?}"
        );
        assert!(
            matches!(
                &sent[1],
                OutboundFrame::Event { envelope } if envelope.seq.is_none()
            ),
            "the later volatile event must remain second: {sent:?}"
        );
    }

    #[tokio::test]
    async fn live_durable_gap_fails_without_sending_or_advancing() {
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut writer = MockGatewayWriter {
            fail_after: None,
            sent: sent.clone(),
            delay: None,
            block_after: None,
            block_notify: None,
            release: None,
        };
        let (_writer_tx, mut writer_rx) = mpsc::channel(1);
        let token = CancellationToken::new();
        let mut next_arrival_id = 0;
        let mut outbox = VecDeque::new();
        let mut pending_events = BTreeMap::new();
        let mut last_received = 0;

        classify_frame(
            event_frame(2),
            true,
            last_received,
            &mut next_arrival_id,
            &mut outbox,
            &mut pending_events,
        );

        let error = drain_next(
            &mut writer,
            Duration::from_secs(1),
            &token,
            &mut writer_rx,
            &mut next_arrival_id,
            &mut outbox,
            &mut pending_events,
            &mut last_received,
            true,
        )
        .await
        .expect_err("a live durable event must not skip seq 1");

        assert!(format!("{error:#}").contains("expected 1, got 2"));
        assert_eq!(last_received, 0, "gap must not advance the watermark");
        assert!(
            sent.lock().unwrap().is_empty(),
            "out-of-order durable event must not reach the transport"
        );
    }

    #[tokio::test]
    async fn paged_catch_up_delivers_early_command_acks() {
        // Catch-up spans multiple pages. CommandAcks and volatile events sent
        // during catch-up must be interleaved correctly without gaps or
        // duplicates, and durable events that arrive before Online must be held.
        let source = MockDurableSource::new(CommandCursors::default());
        for seq in 1..=5 {
            source.push_event(event_frame(seq));
        }

        let mut config = make_config();
        config.catch_up_page_size = NonZeroUsize::new(2).unwrap();
        config.send_timeout = Duration::from_secs(1);

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
                delay: Some(Duration::from_millis(2)),
                block_after: None,
                block_notify: None,
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
            delivery_authorization: DeliveryAuthorization::Raw,
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
            config,
        );
        let handle = supervisor.start();

        let mut epochs = handle.epochs.clone();
        while epochs.borrow().is_none() {
            epochs.changed().await.unwrap();
        }
        let epoch = epochs.borrow().unwrap();

        // CommandAcks sent during paged catch-up must be delivered immediately.
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
        handle.events.send((epoch, ack(3))).await.unwrap();

        // Volatile pre-online events must be dropped.
        let volatile = OutboundFrame::Event {
            envelope: Envelope {
                seq: None,
                conversation_id: "conversation-1".to_owned(),
                event: serde_json::json!({"type": "typing"}),
            },
        };
        handle.events.send((epoch, volatile)).await.unwrap();

        // A durable event that is already in the durable source must not be
        // double-sent from a pre-online live frame.
        handle.events.send((epoch, event_frame(3))).await.unwrap();

        let mut online = handle.online.clone();
        while !*online.borrow() {
            online.changed().await.unwrap();
        }

        handle.events.send((epoch, event_frame(6))).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        handle.abort();
        assert!(handle.join().await.is_ok());

        let sent_frames = sent.lock().unwrap();
        let event_seqs: Vec<_> = sent_frames
            .iter()
            .filter_map(|f| outbound_frame_event_seq(f).ok())
            .collect();
        assert_eq!(
            event_seqs,
            vec![1, 2, 3, 4, 5, 6],
            "paged catch-up must deliver durable events exactly once and in order"
        );
        assert_eq!(
            sent_frames
                .iter()
                .filter(|f| matches!(f, OutboundFrame::CommandAck { .. }))
                .count(),
            3,
            "all pre-online CommandAcks must be delivered"
        );
        assert!(
            !sent_frames
                .iter()
                .any(|f| matches!(f, OutboundFrame::Event { envelope } if envelope.seq.is_none())),
            "volatile pre-online Event must be dropped"
        );
    }

    #[tokio::test]
    async fn writer_task_rechecks_cursor_before_publishing_online() {
        // The durable source commits event 2 while the first page is being sent,
        // so the writer must re-check the cursor and include the racing commit
        // before Online.
        struct RacingSource {
            events: Arc<std::sync::Mutex<VecDeque<OutboundFrame>>>,
            first_page_started: Arc<Notify>,
            first_page_release: Arc<Notify>,
        }

        impl Clone for RacingSource {
            fn clone(&self) -> Self {
                Self {
                    events: self.events.clone(),
                    first_page_started: self.first_page_started.clone(),
                    first_page_release: self.first_page_release.clone(),
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
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
            delivery_authorization: DeliveryAuthorization::Raw,
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
    async fn hydration_hold_limit_backpressures_then_drains_arbitrary_burst() {
        let generation = ProcessGeneration::from_wire(7).unwrap();
        let (latch, latch_tx) = DynamicHydrationLatch::new();
        let commands: VecDeque<_> = (1..=64)
            .map(|seq| {
                Ok(valid_command(
                    seq,
                    &format!("00000000-0000-4000-8000-{:012x}", seq),
                ))
            })
            .collect();
        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway::new(commands);
        let connector = MockConnector::new(sent_hellos.clone(), VecDeque::from([Ok(gateway)]));
        let credentials = CountingCredentialProvider::new("token");
        let source = MockDurableSource::new(CommandCursors::default());

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let mut handle = supervisor.start();

        assert!(
            tokio::time::timeout(Duration::from_millis(50), handle.commands.recv())
                .await
                .is_err(),
            "pre-hydration commands must remain behind the readiness gate"
        );
        latch_tx
            .send(HydrationState::Ready(HydrationReady {
                generation,
                receipt_identity: "burst-ready".to_owned(),
            }))
            .unwrap();

        for expected in 1..=64 {
            let command = tokio::time::timeout(Duration::from_secs(1), handle.commands.recv())
                .await
                .expect("delayed hydration burst must drain")
                .expect("command channel must remain open");
            assert_eq!(cmd_seq(&command), expected);
        }
        assert!(
            !handle.task.as_ref().expect("supervisor task").is_finished(),
            "a valid replay burst must not terminate the supervisor"
        );
        assert_eq!(
            sent_hellos.lock().unwrap().len(),
            1,
            "backpressure must not manufacture a reconnect"
        );
        handle.abort();
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn hydration_generation_mismatch_is_fatal() {
        // A HydrationLatch that returns a different generation is a contract
        // violation, not a transient transport error. The supervisor must fail
        // closed instead of reconnect-looping.
        let generation = ProcessGeneration::from_wire(7).unwrap();
        let wrong_generation = ProcessGeneration::from_wire(8).unwrap();
        let latch = StaticHydrationLatch(HydrationReady {
            generation: wrong_generation,
            receipt_identity: "receipt".to_owned(),
        });

        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway::new(VecDeque::from([Ok(valid_command(
            1,
            "00000000-0000-4000-8000-000000000001",
        ))]));
        let connector = MockConnector::new(sent_hellos.clone(), VecDeque::from([Ok(gateway)]));
        let source = MockDurableSource::new(CommandCursors::default());
        let mut config = make_config();
        config.generation = generation;

        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token"),
            source,
            latch,
            config,
        );
        let handle = supervisor.start();

        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(result.is_ok(), "must terminate within bounded time");
        let err = result
            .unwrap()
            .expect_err("hydration generation mismatch must fail closed");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("generation mismatch"),
            "expected fatal generation mismatch, got: {msg}"
        );
        assert_eq!(
            sent_hellos.lock().unwrap().len(),
            1,
            "must not reconnect after fatal latch error"
        );
    }

    #[tokio::test]
    async fn hydration_identity_mismatch_is_fatal() {
        // A HydrationLatch that changes receipt identity for the same generation
        // is a contract violation. The supervisor must fail closed.
        let generation = ProcessGeneration::from_wire(7).unwrap();
        let (tx, rx) = watch::channel(HydrationState::NotReady);
        let latch = WatchHydrationLatch::new(rx);

        // Seed the latch with one identity.
        tx.send(HydrationState::Ready(HydrationReady {
            generation,
            receipt_identity: "first".to_owned(),
        }))
        .unwrap();
        let _ = latch.wait_for(generation).await.unwrap();

        // Then publish a different identity for the same generation.
        tx.send(HydrationState::Ready(HydrationReady {
            generation,
            receipt_identity: "second".to_owned(),
        }))
        .unwrap();

        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway::new(VecDeque::from([Ok(valid_command(
            1,
            "00000000-0000-4000-8000-000000000001",
        ))]));
        let connector = MockConnector::new(sent_hellos.clone(), VecDeque::from([Ok(gateway)]));
        let source = MockDurableSource::new(CommandCursors::default());
        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token"),
            source,
            latch.clone(),
            make_config(),
        );
        let handle = supervisor.start();

        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(result.is_ok(), "must terminate within bounded time");
        let err = result
            .unwrap()
            .expect_err("hydration identity mismatch must fail closed");
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("identity changed"),
            "expected fatal identity mismatch, got: {msg}"
        );
        assert_eq!(
            sent_hellos.lock().unwrap().len(),
            1,
            "must not reconnect after fatal latch error"
        );
    }

    #[tokio::test]
    async fn healthy_reconnect_resets_attempt_budget() {
        // A clean gateway close after reaching Online must reset the reconnect
        // attempt budget. max=2 is the smallest configured budget that exposes
        // the off-by-one reset bug: a healthy epoch plus two failed reconnects
        // should consume three gateway responses. max=1 still permits one failed
        // reconnect after a healthy epoch.
        let generation = ProcessGeneration::from_wire(7).unwrap();

        fn healthy_gateway() -> MockGateway {
            MockGateway::new(VecDeque::from([
                Ok(valid_command(1, "00000000-0000-4000-8000-000000000001")),
                Err(GatewayClosed.into()),
            ]))
        }

        fn failing_gateway() -> MockGateway {
            MockGateway::new(VecDeque::from([Err(anyhow!("reader EOF"))]))
        }

        // Budget = 2: healthy epoch + two failed reconnects.
        {
            let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
            let connector = MockConnector::new(
                sent_hellos.clone(),
                VecDeque::from([
                    Ok(healthy_gateway()),
                    Ok(failing_gateway()),
                    Ok(failing_gateway()),
                ]),
            );
            let mut config = make_config();
            config.max_reconnect_attempts = Some(2);
            let source = MockDurableSource::new(CommandCursors::default());
            let supervisor = ConnectionSupervisor::new(
                connector,
                CountingCredentialProvider::new("token"),
                source,
                StaticHydrationLatch(HydrationReady {
                    generation,
                    receipt_identity: "r".to_owned(),
                }),
                config,
            );
            let handle = supervisor.start();

            let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
            assert!(result.is_ok(), "must terminate within bounded time");
            let err = result
                .unwrap()
                .expect_err("max reconnect attempts must be exceeded");
            assert!(
                format!("{:#}", err).contains("max reconnect attempts exceeded"),
                "unexpected error: {err:#}"
            );
            assert_eq!(
                sent_hellos.lock().unwrap().len(),
                3,
                "budget=2 must allow two failed reconnects after a healthy epoch"
            );
        }

        // Budget = 1: healthy epoch + one failed reconnect.
        {
            let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
            let connector = MockConnector::new(
                sent_hellos.clone(),
                VecDeque::from([Ok(healthy_gateway()), Ok(failing_gateway())]),
            );
            let mut config = make_config();
            config.max_reconnect_attempts = Some(1);
            let source = MockDurableSource::new(CommandCursors::default());
            let supervisor = ConnectionSupervisor::new(
                connector,
                CountingCredentialProvider::new("token"),
                source,
                StaticHydrationLatch(HydrationReady {
                    generation,
                    receipt_identity: "r".to_owned(),
                }),
                config,
            );
            let handle = supervisor.start();

            let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
            assert!(result.is_ok(), "must terminate within bounded time");
            let err = result
                .unwrap()
                .expect_err("max reconnect attempts must be exceeded");
            assert!(
                format!("{:#}", err).contains("max reconnect attempts exceeded"),
                "unexpected error: {err:#}"
            );
            assert_eq!(
                sent_hellos.lock().unwrap().len(),
                2,
                "budget=1 must allow one failed reconnect after a healthy epoch"
            );
        }
    }

    #[tokio::test]
    async fn send_validated_cancels_on_full_command_channel() {
        // command_buffer_size is 1, so the first validated command fills the
        // channel and the second send_validated blocks. Abort must release the
        // blocked send and the pending command must not be delivered.
        let mut config = make_config();
        config.command_buffer_size = NonZeroUsize::new(1).unwrap();

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
    async fn bounded_writer_queue_does_not_deadlock_during_catch_up() {
        // Catch-up is held while a burst of CommandAcks and durable events fills
        // the bounded channels. writer_task must consume writer_rx during catch-up
        // so event_forwarder does not deadlock, and all queued frames must be
        // delivered in order once Online is reached.
        let catch_up_notify = Arc::new(Notify::new());
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

        let mut config = make_config();
        config.event_buffer_size = NonZeroUsize::new(2).unwrap();
        config.send_timeout = Duration::from_secs(1);

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
                delay: Some(Duration::from_millis(2)),
                block_after: None,
                block_notify: None,
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            last_received_event_seq: 0,
            hello_delay: None,
            hello_error: None,
            delivery_authorization: DeliveryAuthorization::Raw,
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
            config,
        );
        let handle = supervisor.start();

        let mut epochs = handle.epochs.clone();
        while epochs.borrow().is_none() {
            epochs.changed().await.unwrap();
        }
        let epoch = epochs.borrow().unwrap();

        let ack = |seq| OutboundFrame::CommandAck {
            ack: CommandAck {
                seq,
                command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                status: CommandAckStatus::Received,
                reject_reason: None,
            },
        };

        let mut burst = Vec::new();
        for i in 1..=5 {
            burst.push(ack(i));
            burst.push(event_frame(i + 2));
        }

        let mut burst_send = tokio::spawn(async move {
            for frame in burst {
                handle.events.send((epoch, frame)).await.unwrap();
            }
            handle
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut burst_send)
                .await
                .is_err(),
            "the bounded internal buffer must restore producer backpressure during catch-up"
        );

        // Allow catch-up to finish and reach Online.
        catch_up_notify.notify_one();
        let handle = tokio::time::timeout(Duration::from_secs(1), burst_send)
            .await
            .expect("burst send must resume after catch-up drains the internal buffer")
            .expect("burst sender must not panic");
        let mut online = handle.online.clone();
        while !*online.borrow() {
            online.changed().await.unwrap();
        }

        handle.events.send((epoch, event_frame(8))).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if sent
                    .lock()
                    .unwrap()
                    .iter()
                    .filter_map(|frame| outbound_frame_event_seq(frame).ok())
                    .next_back()
                    == Some(8)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("bounded writer must deliver the admitted suffix before shutdown");

        handle.abort();
        assert!(handle.join().await.is_ok());

        let sent_frames = sent.lock().unwrap();
        let event_seqs: Vec<_> = sent_frames
            .iter()
            .filter_map(|f| outbound_frame_event_seq(f).ok())
            .collect();
        assert_eq!(
            event_seqs,
            vec![1, 2, 3, 4, 5, 6, 7, 8],
            "catch-up, queued pre-online durable events, and post-online event must be in order"
        );
        assert_eq!(
            sent_frames
                .iter()
                .filter(|f| matches!(f, OutboundFrame::CommandAck { .. }))
                .count(),
            5,
            "all pre-online CommandAcks must be delivered"
        );
    }

    #[tokio::test]
    async fn event_forwarder_panic_is_propagated() {
        let (tx, rx) = mpsc::channel::<(DeliveryEpoch, bool, OutboundFrame)>(1);
        let current_writer: CurrentWriterSlot = Arc::new(std::sync::Mutex::new(None));

        // Poison the writer mutex so event_forwarder panics when it locks.
        let poison = current_writer.clone();
        std::thread::spawn(move || {
            let _guard = poison.lock().unwrap();
            panic!("poison mutex");
        })
        .join()
        .unwrap_err();

        let cancel = CancellationToken::new();
        let forwarder = tokio::spawn(event_forwarder(rx, current_writer, cancel));

        // Send a frame so event_forwarder tries to lock and panics.
        tx.send((DeliveryEpoch(1), false, event_frame(1)))
            .await
            .unwrap();

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
        let json = r#"{"agent_id":"a","generation":"1","last_sent_event_seq":"0","last_received_command_seq":"0","last_applied_command_seq":"0","extra":1}"#;
        assert!(
            serde_json::from_str::<AgentHello>(json).is_err(),
            "AgentHello must reject unknown fields"
        );
    }

    #[test]
    fn api_hello_rejects_unknown_fields() {
        let json = r#"{"accepted_generation":"1","last_received_event_seq":"0","next_command_seq":"1","delivery_authorization":"raw","extra":1}"#;
        assert!(
            serde_json::from_str::<ApiHello>(json).is_err(),
            "ApiHello must reject unknown fields"
        );
    }

    #[test]
    fn hello_dto_deserialization_still_accepts_known_fields() {
        let agent_json = r#"{"agent_id":"a","generation":"1","last_sent_event_seq":"0","last_received_command_seq":"0","last_applied_command_seq":"0"}"#;
        assert!(serde_json::from_str::<AgentHello>(agent_json).is_ok());

        let api_json = r#"{"accepted_generation":"1","last_received_event_seq":"0","next_command_seq":"1","delivery_authorization":"raw"}"#;
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

    #[derive(Clone)]
    struct BarrierDeliverySource {
        entered: Arc<Notify>,
        release: Arc<Notify>,
        pump: DeliveryPump,
        epoch: DeliveryEpoch,
    }

    #[async_trait]
    impl DurableSource for BarrierDeliverySource {
        async fn event_cursor(&self) -> Result<EventCursors> {
            Ok(EventCursors { last_sent: 1 })
        }

        async fn events_after(&self, _after_seq: u64, _limit: usize) -> Result<Vec<OutboundFrame>> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(vec![event_frame(1)])
        }

        async fn command_cursors(&self) -> Result<CommandCursors> {
            Ok(CommandCursors::default())
        }

        async fn mark_delivery_online(&self, epoch: DeliveryEpoch) -> Result<()> {
            assert_eq!(epoch, self.epoch);
            self.pump.mark_online(epoch)
        }
    }

    #[tokio::test]
    async fn blocked_catch_up_drops_volatile_until_writer_opens_online_barrier() {
        let (channel, mut delivery_rx) =
            DeliveryChannelBuilder::with_mode(DeliveryMode::Raw).build();
        let (failure_tx, _failure_rx) = mpsc::unbounded_channel();
        let pump = DeliveryPump::new(
            Arc::new(
                Store::session_test_store("blocked-catch-up-pump")
                    .await
                    .unwrap(),
            ),
            channel,
        );
        let epoch = DeliveryEpoch::for_test("blocked-catch-up");
        pump.install_supervised_epoch(epoch, failure_tx);
        let source = BarrierDeliverySource {
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            pump: pump.clone(),
            epoch,
        };

        let sent = Arc::new(Mutex::new(Vec::new()));
        let writer = MockGatewayWriter {
            fail_after: None,
            sent: sent.clone(),
            delay: None,
            block_after: None,
            block_notify: None,
            release: None,
        };
        let (writer_tx, writer_rx) = mpsc::channel(4);
        let (online, mut online_rx) = watch::channel(false);
        let token = CancellationToken::new();
        let api_hello = ApiHello {
            accepted_generation: ProcessGeneration::from_wire(7).unwrap(),
            last_received_event_seq: 0,
            next_command_seq: 1,
            delivery_authorization: DeliveryAuthorization::Raw,
        };
        let (delivery_ready_tx, delivery_ready_rx) = oneshot::channel();
        delivery_ready_tx.send(Ok(())).unwrap();

        let task = tokio::spawn(writer_task(
            writer,
            writer_rx,
            source.clone(),
            api_hello,
            epoch,
            make_config(),
            Arc::new(online),
            token.clone(),
            delivery_ready_rx,
        ));
        tokio::time::timeout(Duration::from_secs(1), source.entered.notified())
            .await
            .expect("writer must be blocked in catch-up");

        pump.on_volatile(crate::agent::AgentEvent::MessageUpdate {
            message_id: "pre-online".to_owned(),
            event: crate::agent::PublicStreamEvent::TextDelta {
                content_index: 0,
                delta: "must-drop".to_owned(),
            },
        })
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), delivery_rx.recv())
                .await
                .is_err()
        );

        source.release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !*online_rx.borrow() {
                online_rx.changed().await.unwrap();
            }
        })
        .await
        .expect("writer must open the online barrier after durable replay");
        pump.on_volatile(crate::agent::AgentEvent::MessageUpdate {
            message_id: "online".to_owned(),
            event: crate::agent::PublicStreamEvent::TextDelta {
                content_index: 0,
                delta: "deliver".to_owned(),
            },
        })
        .await
        .unwrap();
        assert!(matches!(
            delivery_rx.recv().await,
            Some(DeliveryFrame::Volatile {
                event: crate::agent::AgentEvent::MessageUpdate { message_id, .. },
                ..
            }) if message_id == "online"
        ));
        assert!(
            sent.lock()
                .unwrap()
                .iter()
                .any(|frame| outbound_frame_event_seq(frame).is_ok_and(|seq| seq == 1)),
            "durable replay must be sent before online volatile admission"
        );

        token.cancel();
        drop(writer_tx);
        assert!(task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn catch_up_gap_fails_before_online_without_advancing_past_the_gap() {
        let source = ReplaySource::new(
            vec![event_frame(1), event_frame(3)],
            CommandCursors::default(),
        );
        let sent = Arc::new(Mutex::new(Vec::new()));
        let writer = MockGatewayWriter {
            fail_after: None,
            sent: sent.clone(),
            delay: None,
            block_after: None,
            block_notify: None,
            release: None,
        };
        let (writer_tx, writer_rx) = mpsc::channel(4);
        let (online, online_rx) = watch::channel(false);
        let token = CancellationToken::new();
        let api_hello = ApiHello {
            accepted_generation: ProcessGeneration::from_wire(7).unwrap(),
            last_received_event_seq: 0,
            next_command_seq: 1,
            delivery_authorization: DeliveryAuthorization::Raw,
        };
        let (delivery_ready_tx, delivery_ready_rx) = oneshot::channel();
        delivery_ready_tx.send(Ok(())).unwrap();

        let error = writer_task(
            writer,
            writer_rx,
            source,
            api_hello,
            DeliveryEpoch::for_test("writer-task-gap"),
            make_config(),
            Arc::new(online),
            token,
            delivery_ready_rx,
        )
        .await
        .expect_err("catch-up must fail closed on missing seq 2");
        drop(writer_tx);

        assert!(format!("{error:#}").contains("expected 2, got 3"));
        assert!(
            !*online_rx.borrow(),
            "a gapped catch-up must never publish Online"
        );
        let seqs: Vec<_> = sent
            .lock()
            .unwrap()
            .iter()
            .filter_map(|frame| outbound_frame_event_seq(frame).ok())
            .collect();
        assert_eq!(
            seqs,
            vec![1],
            "catch-up must stop at the last contiguous seq"
        );
    }

    #[tokio::test]
    async fn durable_catch_up_gap_is_fatal_without_unlimited_reconnect() {
        let source = ReplaySource::new(
            vec![event_frame(1), event_frame(3)],
            CommandCursors::default(),
        );
        let sent_hellos = Arc::new(Mutex::new(Vec::new()));
        let sent = Arc::new(Mutex::new(Vec::new()));
        let mut gateway = MockGateway::new(VecDeque::new());
        gateway.writer.sent = sent.clone();
        let connector = MockConnector::new(sent_hellos.clone(), VecDeque::from([Ok(gateway)]));
        let credentials = CountingCredentialProvider::new("token");
        let credential_count = credentials.counter.clone();
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "receipt-1".to_owned(),
        });
        let mut config = make_config();
        config.max_reconnect_attempts = None;

        let supervisor = ConnectionSupervisor::new(connector, credentials, source, latch, config);
        let handle = supervisor.start();
        let result = tokio::time::timeout(Duration::from_secs(1), handle.join())
            .await
            .expect("durable replay invariant must terminate the supervisor");
        let error = result.expect_err("durable replay gap must be fatal");

        assert!(
            error.is::<DurableReplayInvariantError>(),
            "supervisor must preserve the typed permanent failure: {error:?}"
        );
        assert_eq!(
            credential_count.load(Ordering::SeqCst),
            1,
            "a permanent durable replay failure must not fetch another credential"
        );
        assert_eq!(
            sent_hellos.lock().unwrap().len(),
            1,
            "a permanent durable replay failure must not start another epoch"
        );
        let seqs: Vec<_> = sent
            .lock()
            .unwrap()
            .iter()
            .filter_map(|frame| outbound_frame_event_seq(frame).ok())
            .collect();
        assert_eq!(seqs, vec![1], "the gap must not advance past seq 1");
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
    async fn merged_t17_store_drives_catchup_hydration_and_epoch_replacement() {
        let store = Arc::new(
            Store::session_test_store("t17-t24-integration")
                .await
                .unwrap(),
        );
        let generation = ProcessGeneration::from_wire(7).unwrap();
        let lease = ProcessGenerationLease::new(generation, "lease-t17-t24").unwrap();
        let fence = GenerationRecoveryFence::new(&lease, "fence-t17-t24").unwrap();
        let receipt = match store.hydrate(&lease, &fence).await.unwrap() {
            HydrationOutcome::Complete(state) => state.receipt,
            HydrationOutcome::RecoveryRequired(_) => {
                panic!("empty test store must hydrate without physical recovery")
            }
        };
        for seq in 1..=9 {
            insert_test_durable_event(&store, seq, &crate::agent::AgentEvent::AgentStart)
                .await
                .unwrap();
        }

        let adapter = seams::T17StoreAdapter::new(store.clone());
        let (hydration_tx, hydration_rx) = watch::channel(None);
        let latch = seams::T17HydrationLatch::new(hydration_rx);

        let sent = Arc::new(Mutex::new(Vec::new()));
        let sent_hellos = Arc::new(Mutex::new(Vec::new()));
        let mut gateway1 =
            MockGateway::new(VecDeque::from([Err(anyhow::Error::new(GatewayClosed))]));
        gateway1.writer.sent = sent.clone();
        gateway1.sent_hellos = sent_hellos.clone();

        let mut gateway2 = MockGateway::new(VecDeque::from([Ok(valid_command(
            1,
            "00000000-0000-4000-8000-000000000001",
        ))]));
        gateway2.writer.sent = sent.clone();
        gateway2.sent_hellos = sent_hellos.clone();

        let connector =
            MockConnector::new(sent_hellos, VecDeque::from([Ok(gateway1), Ok(gateway2)]));
        let mut config = make_config();
        config.initial_backoff = Duration::ZERO;
        config.max_backoff = Duration::ZERO;
        config.catch_up_page_size = NonZeroUsize::new(3).unwrap();
        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token"),
            adapter.clone(),
            latch,
            config,
        );
        let mut handle = supervisor.start();

        let mut epochs = handle.epochs.clone();
        let epoch2 = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(epoch) = *epochs.borrow()
                    && epoch.as_u64() > 0
                {
                    return epoch;
                }
                epochs.changed().await.unwrap();
            }
        })
        .await
        .expect("second Store-backed delivery epoch must install");
        assert_eq!(
            adapter.active_delivery_epoch().await,
            Some(epoch2),
            "old T17 DeliveryPump epoch must be invalidated before replacement"
        );
        assert_eq!(
            adapter.delivery_epoch_lifecycle_counts(),
            (2, 1),
            "replacement must install once after invalidating the old epoch once"
        );

        assert!(
            tokio::time::timeout(Duration::from_millis(50), handle.commands.recv())
                .await
                .is_err(),
            "commands must remain held until the typed T17 receipt is published"
        );
        hydration_tx.send(Some(receipt)).unwrap();
        let command = tokio::time::timeout(Duration::from_secs(1), handle.commands.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(inbound_command_seq(&command), 1);

        let epoch1 = DeliveryEpoch(epoch2.as_u64() - 1);
        handle.events.send((epoch1, event_frame(99))).await.unwrap();
        insert_test_durable_event(&store, 10, &crate::agent::AgentEvent::TurnStart)
            .await
            .unwrap();
        adapter.on_durable_committed(10).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let seqs: Vec<_> = sent
                    .lock()
                    .unwrap()
                    .iter()
                    .filter_map(|frame| outbound_frame_event_seq(frame).ok())
                    .collect();
                if seqs.contains(&9) && seqs.contains(&10) {
                    assert!(
                        !seqs.contains(&99),
                        "late frame from invalidated epoch must be rejected by T24"
                    );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("real Store catch-up and live durable delivery must complete");
        let page_lengths = adapter.replay_page_lengths();
        assert!(
            page_lengths.len() >= 3 && page_lengths.iter().all(|length| *length <= 3),
            "Store replay must be split into catch_up_page_size-bounded pages: {page_lengths:?}"
        );

        handle.abort();
        assert!(handle.join().await.is_ok());
        assert_eq!(
            adapter.active_delivery_epoch().await,
            None,
            "final supervisor shutdown must invalidate the T17 DeliveryPump epoch"
        );
        assert_eq!(
            adapter.delivery_epoch_lifecycle_counts(),
            (2, 2),
            "each Store-backed epoch must be installed and invalidated exactly once"
        );
    }

    #[tokio::test]
    async fn dropping_active_handle_finishes_t17_epoch_cleanup_with_or_without_prior_abort() {
        async fn assert_cleanup(abort_before_drop: bool) {
            let store_name = if abort_before_drop {
                "t17-t24-abort-without-join-cleanup"
            } else {
                "t17-t24-handle-drop-cleanup"
            };
            let store = Arc::new(Store::session_test_store(store_name).await.unwrap());
            let adapter = seams::T17StoreAdapter::new(store);
            let connector = MockConnector::new(
                Arc::new(Mutex::new(Vec::new())),
                VecDeque::from([Ok(MockGateway::new(VecDeque::new()))]),
            );
            let supervisor = ConnectionSupervisor::new(
                connector,
                CountingCredentialProvider::new("token"),
                adapter.clone(),
                StaticHydrationLatch(HydrationReady {
                    generation: ProcessGeneration::from_wire(7).unwrap(),
                    receipt_identity: "handle-drop-ready".to_owned(),
                }),
                make_config(),
            );
            let handle = supervisor.start();

            tokio::time::timeout(Duration::from_secs(1), async {
                while adapter.active_delivery_epoch().await.is_none() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("real T17 delivery epoch must be installed before handle drop");
            assert_eq!(adapter.delivery_epoch_lifecycle_counts(), (1, 0));

            if abort_before_drop {
                handle.abort();
            }
            drop(handle);

            tokio::time::timeout(Duration::from_secs(1), async {
                while adapter.active_delivery_epoch().await.is_some() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("detached supervisor cleanup must invalidate the dropped handle's epoch");
            assert_eq!(
                adapter.delivery_epoch_lifecycle_counts(),
                (1, 1),
                "handle drop must install and invalidate the real T17 epoch exactly once"
            );
        }

        assert_cleanup(false).await;
        assert_cleanup(true).await;
    }

    #[tokio::test]
    async fn redaction_only_store_adapter_replays_and_delivers_projection_without_volatiles() {
        let store = Arc::new(
            Store::session_test_store("t17-t24-redaction-only")
                .await
                .unwrap(),
        );
        let raw_secret = "sk-abcdefghijklmnop";
        let first_projection = insert_test_durable_event(
            &store,
            1,
            &crate::agent::AgentEvent::Error {
                message: format!("first {raw_secret}"),
            },
        )
        .await
        .unwrap();
        let adapter = seams::T17StoreAdapter::new(store.clone());

        let sent = Arc::new(Mutex::new(Vec::new()));
        let mut gateway = MockGateway::new(VecDeque::new())
            .with_delivery_authorization(DeliveryAuthorization::RedactionOnly);
        gateway.writer.sent = sent.clone();
        let connector = MockConnector::new(
            Arc::new(Mutex::new(Vec::new())),
            VecDeque::from([Ok(gateway)]),
        );
        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token")
                .with_delivery_authorization(DeliveryAuthorization::RedactionOnly),
            adapter.clone(),
            StaticHydrationLatch(HydrationReady {
                generation: ProcessGeneration::from_wire(7).unwrap(),
                receipt_identity: "redaction-ready".to_owned(),
            }),
            make_config(),
        );
        let handle = supervisor.start();
        let mut online = handle.online.clone();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !*online.borrow() {
                online.changed().await.unwrap();
            }
        })
        .await
        .expect("projection-only catch-up must reach Online");

        let second_projection = insert_test_durable_event(
            &store,
            2,
            &crate::agent::AgentEvent::Error {
                message: format!("second {raw_secret}"),
            },
        )
        .await
        .unwrap();
        adapter.on_durable_committed(2).await.unwrap();
        adapter
            .on_volatile(crate::agent::AgentEvent::ToolExecutionUpdate {
                tool_call_id: "tool-1".to_owned(),
                partial: serde_json::json!({"stdout": raw_secret}),
            })
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if sent
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|frame| outbound_frame_event_seq(frame).is_ok())
                    .count()
                    == 2
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("projection-only durable frames must be forwarded");

        handle.abort();
        handle.join().await.unwrap();

        let expected = [first_projection, second_projection]
            .map(|projection| serde_json::from_str::<serde_json::Value>(&projection).unwrap());
        let frames = sent.lock().unwrap();
        let delivered: Vec<_> = frames
            .iter()
            .filter_map(|frame| match frame {
                OutboundFrame::Event { envelope } if envelope.seq.is_some() => {
                    Some(envelope.event.clone())
                }
                _ => None,
            })
            .collect();
        assert_eq!(delivered, expected);
        assert!(
            !serde_json::to_string(&delivered)
                .unwrap()
                .contains(raw_secret),
            "projection-only adapter must never expose raw decrypted secret material"
        );
        assert!(
            !frames.iter().any(
                |frame| matches!(frame, OutboundFrame::Event { envelope } if envelope.seq.is_none())
            ),
            "projection-only authorization must suppress every volatile frame"
        );
    }

    #[tokio::test]
    async fn store_forwarder_failure_cancels_epoch_and_reconnects() {
        let store = Arc::new(
            Store::session_test_store("t17-t24-forwarder-failure")
                .await
                .unwrap(),
        );
        let adapter = seams::T17StoreAdapter::new(store.clone());
        let sent_hellos = Arc::new(Mutex::new(Vec::new()));
        let responses = (0..5)
            .map(|_| {
                Ok(MockGateway::new(VecDeque::new())
                    .with_delivery_authorization(DeliveryAuthorization::RedactionOnly))
            })
            .collect();
        let connector = MockConnector::new(sent_hellos.clone(), responses);
        let mut config = make_config();
        config.initial_backoff = Duration::ZERO;
        config.max_backoff = Duration::ZERO;
        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token")
                .with_delivery_authorization(DeliveryAuthorization::RedactionOnly),
            adapter.clone(),
            StaticHydrationLatch(HydrationReady {
                generation: ProcessGeneration::from_wire(7).unwrap(),
                receipt_identity: "redaction-ready".to_owned(),
            }),
            config,
        );
        let handle = supervisor.start();
        let mut online = handle.online.clone();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !*online.borrow() {
                online.changed().await.unwrap();
            }
        })
        .await
        .expect("first delivery epoch must become Online");

        insert_test_durable_event(&store, 1, &crate::agent::AgentEvent::AgentStart)
            .await
            .unwrap();
        sqlx::query("UPDATE agent_events SET envelope = 'not-json' WHERE seq = 1")
            .execute(store.pool())
            .await
            .unwrap();
        adapter.on_durable_committed(1).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while sent_hellos.lock().unwrap().len() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("forwarder projection failure must force a fresh authenticated epoch");
        handle.abort();
        handle.join().await.unwrap();
        assert_eq!(
            adapter.active_delivery_epoch().await,
            None,
            "failed epoch must be invalidated idempotently after pump/forwarder teardown"
        );
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
        handle.events.send((epoch1, event_frame(99))).await.unwrap();
        // The exact next durable frame tagged with the new DeliveryEpoch must be delivered.
        handle.events.send((epoch2, event_frame(2))).await.unwrap();

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
            event_seqs.contains(&2),
            "live event 2 must be delivered on the new epoch"
        );
        assert!(
            !event_seqs.contains(&99),
            "stale DeliveryEpoch frame 99 must be dropped"
        );
    }

    #[derive(Clone)]
    struct OversizedConnector {
        sent_hellos: Arc<Mutex<Vec<AgentHello>>>,
    }

    impl OversizedConnector {
        fn new(sent_hellos: Arc<Mutex<Vec<AgentHello>>>) -> Self {
            Self { sent_hellos }
        }
    }

    #[async_trait]
    impl GatewayConnector for OversizedConnector {
        type Connection = OversizedGateway;

        async fn connect(
            &mut self,
            _credential: GatewayCredential,
        ) -> Result<Self::Connection, ConnectorError> {
            Ok(OversizedGateway {
                reader: MockGatewayReader {
                    commands: VecDeque::new(),
                    panic: false,
                    on_empty: None,
                },
                sent_hellos: self.sent_hellos.clone(),
            })
        }
    }

    struct OversizedGateway {
        reader: MockGatewayReader,
        sent_hellos: Arc<Mutex<Vec<AgentHello>>>,
    }

    #[async_trait]
    impl Gateway for OversizedGateway {
        type Reader = MockGatewayReader;
        type Writer = OversizedWriter;

        async fn authenticate_hello(
            &mut self,
            hello: AgentHello,
        ) -> std::result::Result<ApiHello, HelloError> {
            self.sent_hellos.lock().unwrap().push(hello.clone());
            Ok(ApiHello {
                accepted_generation: hello.generation,
                last_received_event_seq: 0,
                next_command_seq: hello.last_applied_command_seq.saturating_add(1),
                delivery_authorization: DeliveryAuthorization::Raw,
            })
        }

        fn split(self) -> (Self::Reader, Self::Writer) {
            (self.reader, OversizedWriter)
        }
    }

    struct OversizedWriter;

    #[async_trait]
    impl GatewayWriter for OversizedWriter {
        async fn send(&mut self, _frame: OutboundFrame) -> Result<()> {
            Err(OversizedFrameError {
                actual: crate::gateway::MAX_FRAME_BYTES + 1,
                max: crate::gateway::MAX_FRAME_BYTES,
            }
            .into())
        }
    }

    #[tokio::test]
    async fn oversized_catch_up_frame_terminates_supervisor_fatal() {
        // A durable catch-up event that the writer rejects as oversized must
        // terminate the supervisor with the typed permanent error, not loop
        // through reconnect epochs from the same cursor.
        let source = MockDurableSource::new(CommandCursors::default());
        source.push_event(event_frame(1));

        let sent_hellos = Arc::new(Mutex::new(Vec::new()));
        let connector = OversizedConnector::new(sent_hellos.clone());
        let credentials = CountingCredentialProvider::new("token");
        let counter = credentials.counter.clone();

        let mut config = make_config();
        config.max_reconnect_attempts = None;

        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "r".to_owned(),
        });

        let supervisor = ConnectionSupervisor::new(connector, credentials, source, latch, config);
        let handle = supervisor.start();

        let result = tokio::time::timeout(Duration::from_secs(1), handle.join()).await;
        assert!(
            result.is_ok(),
            "supervisor must terminate within bounded time"
        );
        let err = result
            .unwrap()
            .expect_err("oversized catch-up must produce a fatal error");
        assert!(
            err.is::<OversizedFrameError>(),
            "error must be the typed oversized boundary: {err:?}"
        );
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "must not fetch a fresh credential after fatal oversized frame"
        );
        assert_eq!(
            sent_hellos.lock().unwrap().len(),
            1,
            "must not retry through connector epochs"
        );
    }

    #[tokio::test]
    async fn ordinary_writer_failure_during_catch_up_reconnects() {
        // An ordinary send failure (not oversized) must still follow the
        // reconnect path, replay the durable catch-up from the same cursor on
        // a new epoch, and deliver the event.
        let mut config = make_config();
        config.initial_backoff = Duration::from_millis(1);
        config.max_backoff = Duration::from_millis(5);

        let sent_hellos = Arc::new(Mutex::new(Vec::new()));
        let sent = Arc::new(Mutex::new(Vec::new()));

        let mut gateway1 = MockGateway::new(VecDeque::new());
        gateway1.writer.sent = sent.clone();
        gateway1.writer.fail_after = Some(0);
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

        let supervisor = ConnectionSupervisor::new(connector, credentials, source, latch, config);
        let handle = supervisor.start();

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

        let mut online = handle.online.clone();
        while !*online.borrow() {
            online.changed().await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
        handle.abort();
        assert!(handle.join().await.is_ok());

        assert_eq!(
            sent_hellos.lock().unwrap().len(),
            2,
            "ordinary writer failure must trigger a reconnect with a fresh hello"
        );

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
}
