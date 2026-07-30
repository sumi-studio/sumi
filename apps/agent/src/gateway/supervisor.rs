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

use crate::runtime::contracts::{PersonalityAgentId, ProcessGeneration};

use super::{
    CommandAck, Gateway, GatewayClosed, GatewayReader, GatewayWriter, HelloError, InboundCommand,
    OutboundFrame, OversizedFrameError,
};

pub mod post_commit;
pub mod seams;
pub mod session;

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
    personality_agent_id: PersonalityAgentId,
    generation: ProcessGeneration,
    delivery_authorization: DeliveryAuthorization,
}

impl GatewayCredential {
    pub fn new(
        token: impl Into<String>,
        personality_agent_id: PersonalityAgentId,
        generation: ProcessGeneration,
        delivery_authorization: DeliveryAuthorization,
    ) -> Self {
        Self {
            token: token.into(),
            personality_agent_id,
            generation,
            delivery_authorization,
        }
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn personality_agent_id(&self) -> &PersonalityAgentId {
        &self.personality_agent_id
    }

    pub const fn generation(&self) -> ProcessGeneration {
        self.generation
    }

    pub const fn delivery_authorization(&self) -> DeliveryAuthorization {
        self.delivery_authorization
    }
}

impl fmt::Debug for GatewayCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayCredential")
            .field("token", &"[REDACTED]")
            .field("personality_agent_id", &self.personality_agent_id)
            .field("generation", &self.generation)
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
    pub personality_agent_id: PersonalityAgentId,
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
    pub personality_agent_id: PersonalityAgentId,
    #[serde(with = "lossless_generation")]
    pub accepted_generation: ProcessGeneration,
    #[serde(with = "lossless_u64")]
    pub last_received_event_seq: u64,
    #[serde(with = "lossless_u64")]
    pub next_command_seq: u64,
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

    /// Optional composite T26/T17 capability consumed by `SessionGateway`.
    ///
    /// Sources without an EventWriter/post-commit dispatcher/DeliveryPump leave
    /// this absent. A production Session event is fatal if it reaches an
    /// adapter without this capability; ACK-only supervisor users do not
    /// require it.
    fn session_event_sink(&self) -> Option<session::SessionEventSink> {
        None
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
    ///
    /// This operation is transactional at the trait boundary: `Ok(())` means
    /// the epoch is open, while `Err` guarantees that the source has not
    /// admitted any volatile frame for that epoch. An implementation must not
    /// open delivery and then report failure; the supervisor intentionally
    /// keeps its public Online watch false until this method succeeds.
    async fn mark_delivery_online(&self, _epoch: DeliveryEpoch) -> Result<()> {
        Ok(())
    }
}

pub struct DeliveryEpochRuntime {
    failure_rx: mpsc::UnboundedReceiver<DeliveryEpochFailure>,
    task: Option<JoinHandle<()>>,
}

#[derive(Debug)]
pub(crate) enum DeliveryEpochFailure {
    Reconnect(String),
    Fatal(String),
}

enum DeliveryEpochCompletion {
    /// The delivery pump itself reported a recoverable channel failure.
    Reported(DeliveryEpochFailure),
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
            Self::Reported(DeliveryEpochFailure::Reconnect(reason)) => {
                Some(SupervisorError::EstablishedReconnect {
                    reason: format!("delivery epoch failed: {reason}"),
                    healthy: false,
                })
            }
            Self::Reported(DeliveryEpochFailure::Fatal(reason)) => Some(SupervisorError::Fatal(
                anyhow!("delivery epoch failed permanently: {reason}"),
            )),
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
    pub(crate) fn new(
        failure_rx: mpsc::UnboundedReceiver<DeliveryEpochFailure>,
        task: JoinHandle<()>,
    ) -> Self {
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
    pub personality_agent_id: PersonalityAgentId,
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

/// Sender returned by `SupervisorHandle::events`.
///
/// Direct frames are tagged with the public `online` value observed at
/// admission time. Volatile frames emitted by the DeliveryPump instead carry
/// the pump's authoritative epoch-Online admission. The `event_forwarder` can
/// therefore drop pre-Online volatile frames without reclassifying them from a
/// later, racy watch-channel observation.
#[derive(Clone)]
pub struct EventSender {
    tx: mpsc::Sender<(DeliveryEpoch, bool, OutboundFrame)>,
    online: watch::Receiver<bool>,
}

enum DeliveryPumpSendOutcome {
    Sent,
    Cancelled,
    Closed,
}

impl EventSender {
    /// Enqueue `frame` for delivery in `epoch`. The returned future resolves
    /// once the frame has been admitted; it preserves bounded backpressure.
    pub async fn send(
        &self,
        (epoch, frame): (DeliveryEpoch, OutboundFrame),
    ) -> Result<(), mpsc::error::SendError<(DeliveryEpoch, OutboundFrame)>> {
        self.send_with_admission(epoch, frame, None).await
    }

    /// Reliably admit one command ACK into the stable supervisor lane.
    ///
    /// Session owns retry across epoch replacement. This boundary therefore
    /// applies bounded backpressure and reports closure instead of silently
    /// losing an ACK when the lane is full or unavailable.
    pub(super) async fn send_command_ack_if_current(
        &self,
        epoch: DeliveryEpoch,
        ack: CommandAck,
        current_epoch: &watch::Receiver<Option<DeliveryEpoch>>,
    ) -> Result<bool, mpsc::error::SendError<(DeliveryEpoch, OutboundFrame)>> {
        let online_at_enqueue = *self.online.borrow();
        let frame = OutboundFrame::CommandAck { ack };
        let permit = match self.tx.reserve().await {
            Ok(permit) => permit,
            Err(_) => return Err(mpsc::error::SendError((epoch, frame))),
        };
        if *current_epoch.borrow() != Some(epoch) {
            return Ok(false);
        }
        permit.send((epoch, online_at_enqueue, frame));
        Ok(true)
    }

    /// Enqueue a frame emitted by the current DeliveryPump.
    ///
    /// A volatile frame can only leave the pump after `mark_online` has
    /// accepted the matching epoch, so that pump admission is authoritative
    /// even during the short interval before the supervisor publishes its
    /// public Online watch. Durable frames do not use the volatile admission
    /// bit.
    pub(super) async fn send_from_delivery_pump(
        &self,
        (epoch, frame): (DeliveryEpoch, OutboundFrame),
    ) -> Result<(), mpsc::error::SendError<(DeliveryEpoch, OutboundFrame)>> {
        let pump_admitted_online = matches!(
            &frame,
            OutboundFrame::Event { envelope } if envelope.seq.is_none()
        );
        self.send_with_admission(epoch, frame, Some(pump_admitted_online))
            .await
    }

    /// Resolve cancellation before obtaining a bounded-lane permit. Once a
    /// permit is obtained, enqueue and the caller's durable fence completion
    /// are synchronous with no cancellation point between them.
    async fn send_from_delivery_pump_owned(
        &self,
        (epoch, frame): (DeliveryEpoch, OutboundFrame),
        cancel: &CancellationToken,
    ) -> DeliveryPumpSendOutcome {
        let permit = tokio::select! {
            biased;
            _ = cancel.cancelled() => return DeliveryPumpSendOutcome::Cancelled,
            permit = self.tx.reserve() => match permit {
                Ok(permit) => permit,
                Err(_) => return DeliveryPumpSendOutcome::Closed,
            }
        };
        let pump_admitted_online = matches!(
            &frame,
            OutboundFrame::Event { envelope } if envelope.seq.is_none()
        );
        permit.send((epoch, pump_admitted_online, frame));
        DeliveryPumpSendOutcome::Sent
    }

    async fn send_with_admission(
        &self,
        epoch: DeliveryEpoch,
        frame: OutboundFrame,
        admitted_online: Option<bool>,
    ) -> Result<(), mpsc::error::SendError<(DeliveryEpoch, OutboundFrame)>> {
        let permit = match self.tx.reserve().await {
            Ok(p) => p,
            Err(_) => return Err(mpsc::error::SendError((epoch, frame))),
        };
        let online_at_enqueue = admitted_online.unwrap_or_else(|| *self.online.borrow());
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
    session_events: Option<session::SessionEventSink>,
    lifecycle: SupervisorLifecycle,
}

struct SupervisorLifecycle {
    cancel: CancellationToken,
    task: Option<JoinHandle<Result<()>>>,
}

impl SupervisorHandle {
    pub fn abort(&self) {
        self.lifecycle.cancel.cancel();
    }

    pub async fn join(mut self) -> Result<()> {
        let task = self
            .lifecycle
            .task
            .take()
            .context("supervisor task was already consumed")?;
        task.await?
    }
}

impl Drop for SupervisorLifecycle {
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
        let session_events = self.source.session_event_sink();
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
            session_events,
            lifecycle: SupervisorLifecycle {
                cancel,
                task: Some(task),
            },
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
        if credential.personality_agent_id() != &config.personality_agent_id
            || credential.generation() != config.generation
        {
            return Err(SupervisorError::Fatal(anyhow!(
                "gateway credential scope mismatch: expected ({}, {}), got ({}, {})",
                config.personality_agent_id,
                config.generation,
                credential.personality_agent_id(),
                credential.generation(),
            )));
        }
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
        let source = source
            .bind_delivery_authorization(delivery_authorization)
            .map_err(SupervisorError::Fatal)?;

        let (connection_epoch, delivery_epoch) = self.next_epoch();
        let (reader, writer) = gateway.split();

        let epoch_token = cancel.child_token();
        let (writer_tx, writer_rx) = mpsc::channel(config.event_buffer_size.get());

        *self.current_writer.lock().unwrap() = Some((delivery_epoch, writer_tx));

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
                let _ = self.online.send(false);
                self.current_epoch.send_replace(None);

                // `install_delivery_epoch` may have installed the opaque epoch
                // before discovering a later setup error. Always run the
                // matching cleanup once so a failed attempt cannot wedge the
                // next reconnect behind a stale T17 mapping. Preserve the
                // install failure and surface cleanup failure as fatal.
                if let Err(cleanup_error) = source.invalidate_delivery_epoch(delivery_epoch).await {
                    return Err(SupervisorError::Fatal(anyhow!(
                        "{reason}; failed to invalidate T17 delivery epoch {} after install failure: {cleanup_error:#}",
                        delivery_epoch.as_u64()
                    )));
                }
                return Err(SupervisorError::EstablishedReconnect {
                    reason,
                    healthy: false,
                });
            }
        };
        debug_assert!(!*self.online.subscribe().borrow());
        self.current_epoch.send_replace(Some(delivery_epoch));
        let _ = delivery_ready_tx.send(Ok(()));

        let command_send_blocked_notify = self.command_send_blocked_notify.clone();
        let mut reader_handle = tokio::spawn(reader_task(
            reader,
            commands_tx,
            self.latch.clone(),
            api_hello,
            config.personality_agent_id.clone(),
            epoch_token.child_token(),
            command_send_blocked_notify,
        ));

        let mut delivery_completion = None;
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                epoch_token.cancel();
                let _ = self.online.send(false);
                let reader_result = reader_handle.await;
                let writer_result = writer_handle.await;
                *self.current_writer.lock().unwrap() = None;
                self.current_epoch.send_replace(None);
                Self::inspect_epoch_results(reader_result, writer_result, || Ok(()))
            }
            reader_result = &mut reader_handle => {
                epoch_token.cancel();
                *self.current_writer.lock().unwrap() = None;
                let was_online = {
                    let rx = self.online.subscribe();
                    *rx.borrow()
                };
                let _ = self.online.send(false);
                self.current_epoch.send_replace(None);
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
                let was_online = {
                    let rx = self.online.subscribe();
                    *rx.borrow()
                };
                let _ = self.online.send(false);
                self.current_epoch.send_replace(None);
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
                let _ = self.online.send(false);
                self.current_epoch.send_replace(None);
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
        if writer_result
            .as_ref()
            .err()
            .is_some_and(|error| error.is::<seams::DeliveryProjectionError>())
        {
            return Err(SupervisorError::Fatal(
                writer_result.expect_err("projection error checked above"),
            ));
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
        personality_agent_id: config.personality_agent_id.clone(),
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
    if api.personality_agent_id != agent.personality_agent_id {
        return Err(SupervisorError::Fatal(anyhow!(
            "personality-agent claim mismatch: api={}, agent={}",
            api.personality_agent_id,
            agent.personality_agent_id
        )));
    }
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
        // Volatile/delta Events (no seq) are only live if their producer's
        // authoritative admission boundary was already Online: the public
        // watch for direct frames, or the DeliveryPump epoch state for
        // pump-originated frames. A pre-Online volatile cannot become live
        // merely because the forwarder was backpressured until after Online.
        // Durable Events (seq present) are held in the writer channel so
        // writer_task can deduplicate them against the durable cursor after
        // Online. CommandAck frames are terminal command feedback and must be
        // delivered even while catch-up is in progress.
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

        // During catch-up, an ACK that arrived after a live durable notice
        // must remain fenced behind that notice. Catch-up will either send the
        // same sequence or advance `last_received` past it, after which
        // `prune_pending_events` removes the marker and the ACK may proceed.
        // ACKs that arrived before every pending durable notice remain
        // sendable, preserving early receipt feedback without allowing a
        // terminal ACK to overtake its committed event batch.
        let ack_is_fenced = !online
            && matches!(
                (outbox.front(), pending_events.values().map(|(id, _)| *id).min()),
                (Some((out_id, _)), Some(event_id)) if event_id < *out_id
            );
        if !ack_is_fenced && let Some((_, frame)) = outbox.pop_front() {
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

    // Open the DeliveryPump volatile barrier only after reaching the durable
    // cursor. Pump-originated volatile frames carry the pump's authoritative
    // Online admission through EventSender, so the first accepted frame can
    // wait in the bounded writer channel without depending on this public
    // watch. Publish the watch only after the barrier succeeds; direct
    // SupervisorHandle producers therefore cannot observe a false Online.
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
    personality_agent_id: PersonalityAgentId,
    token: CancellationToken,
    command_send_blocked_notify: Option<Arc<Notify>>,
) -> Result<(), ReaderError>
where
    R: GatewayReader + 'static,
    L: HydrationLatch,
{
    const MAX_PENDING_BEFORE_READY: usize = 16;

    // Run command reception in its own task so hydration completion never cancels
    // an in-flight read across transport chunk boundaries. The channel is sized
    // to a single slot so the supervisor can enforce the hold limit strictly:
    // at most MAX_PENDING_BEFORE_READY commands may be buffered in `pending`, and
    // one additional in-flight command is the deterministic fail-closed signal.
    let (cmd_tx, mut cmd_rx) = mpsc::channel(1);
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
                        next_expected = send_validated(cmd, &personality_agent_id, next_expected, &mut command_tx, &token, command_send_blocked_notify.clone()).await?;
                    }
                    if terminal_after_pending {
                        break 'task Err(ReaderError::Terminal);
                    }
                }
                result = cmd_rx.recv(),
                    if !terminal_after_pending
                        && (ready.is_some() || pending.len() <= MAX_PENDING_BEFORE_READY) =>
                {
                    match result {
                        Some(Ok(cmd)) => {
                            if ready.is_some() {
                                next_expected = send_validated(cmd, &personality_agent_id, next_expected, &mut command_tx, &token, command_send_blocked_notify.clone()).await?;
                            } else if pending.len() >= MAX_PENDING_BEFORE_READY {
                                break 'task Err(ReaderError::Fatal(anyhow!(
                                    "hydration hold limit exceeded: {} commands received before Ready",
                                    pending.len()
                                )));
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
    expected_personality_agent_id: &PersonalityAgentId,
    next_expected: u64,
    command_tx: &mut mpsc::Sender<InboundCommand>,
    token: &CancellationToken,
    blocked_notify: Option<Arc<Notify>>,
) -> Result<u64> {
    if cmd.personality_agent_id() != expected_personality_agent_id {
        bail!(
            "command target mismatch: expected {}, got {}",
            expected_personality_agent_id,
            cmd.personality_agent_id()
        );
    }
    if cmd.provenance().personality_agent_id() != expected_personality_agent_id {
        bail!(
            "command provenance target mismatch: expected {}, got {}",
            expected_personality_agent_id,
            cmd.provenance().personality_agent_id()
        );
    }
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
    use tokio::sync::{Notify, mpsc, watch};

    use super::*;
    use crate::agent::{
        AdmittedCommand, AgentEvent, PublicStreamEvent, RunCompletion, RunControl, RunCore,
        RunWorker, Session,
    };
    use crate::gateway::stdio::{InjectedStdioGateway, SingleConnectionConnector};
    use crate::gateway::wire::to_wire_frame;
    use crate::gateway::{
        Command, CommandAck, CommandAckStatus, CommandEnvelope, CommandId, CommandRejectReason,
        Envelope, Gateway, GatewayClosed, GatewayReader, GatewayWriter, InboundCommand,
        OutboundFrame,
    };
    use crate::provider::types::{
        ApiProtocol, ProviderOrigin, PublicAssistantContent, PublicAssistantMessage, PublicMessage,
        StopReason, ToolCall, ToolResultMessage, Usage, UserContent, UserMessage,
        ValidatedToolArguments,
    };
    use crate::runtime::contracts::{
        DirectChatProvenanceV1, GenerationRecoveryFence, PersonalityAgentId, ProcessGenerationLease,
    };
    use crate::store::{
        DeliveryChannelBuilder, DeliveryFrame, DeliveryMode, DeliveryPump, DurableEvent,
        EventBatch, EventWrite, EventWriter, EventWriterQuiescence, HydrationOutcome,
        PostCommitEpochCapability, Store, insert_test_durable_event, user_message_id,
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
            Ok(GatewayCredential::new(
                token,
                crate::gateway::test_personality_agent_id(),
                ProcessGeneration::from_wire(7).unwrap(),
                self.delivery_authorization,
            ))
        }
    }

    #[derive(Clone)]
    struct FixedCredentialProvider(GatewayCredential);

    #[async_trait]
    impl CredentialProvider for FixedCredentialProvider {
        async fn fresh_credential(&mut self) -> Result<GatewayCredential> {
            Ok(self.0.clone())
        }
    }

    #[derive(Clone)]
    struct SequencedAuthorizationProvider {
        authorizations: Arc<Mutex<VecDeque<DeliveryAuthorization>>>,
        counter: Arc<AtomicU64>,
    }

    impl SequencedAuthorizationProvider {
        fn new(authorizations: impl IntoIterator<Item = DeliveryAuthorization>) -> Self {
            Self {
                authorizations: Arc::new(Mutex::new(authorizations.into_iter().collect())),
                counter: Arc::new(AtomicU64::new(0)),
            }
        }
    }

    #[async_trait]
    impl CredentialProvider for SequencedAuthorizationProvider {
        async fn fresh_credential(&mut self) -> Result<GatewayCredential> {
            let authorization = self
                .authorizations
                .lock()
                .unwrap()
                .pop_front()
                .context("test authorization sequence exhausted")?;
            let attempt = self.counter.fetch_add(1, Ordering::SeqCst);
            Ok(GatewayCredential::new(
                format!("sequenced-token-{attempt}"),
                crate::gateway::test_personality_agent_id(),
                ProcessGeneration::from_wire(7).unwrap(),
                authorization,
            ))
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
        /// Record the Nth frame as a completed wire write, then fail the
        /// connection before the peer's durable handler can be assumed.
        fail_after_record: Option<usize>,
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
                if self.fail_after_record == Some(sent.len()) {
                    bail!("writer failed after wire send before peer persistence");
                }
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
        next_command_seq: Option<u64>,
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
                    fail_after_record: None,
                    sent: Arc::new(std::sync::Mutex::new(Vec::new())),
                    delay: None,
                    block_after: None,
                    block_notify: None,
                    release: None,
                },
                sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
                hello_generation: None,
                next_command_seq: None,
                last_received_event_seq: 0,
                hello_delay: None,
                hello_error: None,
            }
        }

        fn with_hello_generation(mut self, generation: u64) -> Self {
            self.hello_generation = Some(ProcessGeneration::from_wire(generation).unwrap());
            self
        }

        fn with_next_command_seq(mut self, next_command_seq: u64) -> Self {
            self.next_command_seq = Some(next_command_seq);
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
                personality_agent_id: hello.personality_agent_id.clone(),
                accepted_generation,
                last_received_event_seq: self.last_received_event_seq,
                next_command_seq: self
                    .next_command_seq
                    .unwrap_or_else(|| hello.last_applied_command_seq.saturating_add(1)),
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
                personality_agent_id: hello.personality_agent_id.clone(),
                accepted_generation: hello.generation,
                last_received_event_seq: 0,
                next_command_seq: self.next_command_seq,
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
        connect_gate: Option<Arc<Notify>>,
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
                connect_gate: None,
            }
        }

        fn with_connect_delay(mut self, delay: Duration) -> Self {
            self.connect_delay = Some(delay);
            self
        }

        fn with_connect_gate(mut self, gate: Arc<Notify>) -> Self {
            self.connect_gate = Some(gate);
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
            if let Some(gate) = self.connect_gate.as_ref() {
                gate.notified().await;
            }
            // Share the hello tracker so the test can observe all attempts.
            gateway.sent_hellos = self.sent_hellos.clone();
            Ok(gateway)
        }
    }

    fn make_config() -> SupervisorConfig {
        SupervisorConfig {
            personality_agent_id: crate::gateway::test_personality_agent_id(),
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

    async fn wait_for_t17_idle(adapter: &seams::T17StoreAdapter) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while adapter.active_delivery_epoch().await.is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("T17 delivery epoch cleanup must finish");
    }

    fn bind_test_post_commit_dispatcher(
        store: Arc<Store>,
        adapter: &seams::T17StoreAdapter,
        start_after_seq: u64,
    ) -> (
        seams::T17StoreAdapter,
        post_commit::OrderedPostCommitDispatcher,
    ) {
        let dispatcher = post_commit::OrderedPostCommitDispatcher::start(
            store,
            adapter.clone(),
            start_after_seq,
            CancellationToken::new(),
        )
        .expect("start one test post-commit dispatcher");
        let bound = adapter
            .bind_post_commit_dispatcher(dispatcher.client())
            .expect("bind the dispatcher proof to Session delivery");
        (bound, dispatcher)
    }

    async fn close_test_post_commit_writer(
        store: &Arc<Store>,
        dispatcher: &post_commit::OrderedPostCommitDispatcher,
    ) -> EventWriterQuiescence {
        EventWriter::new(store.clone())
            .close_post_commit_admission(dispatcher.shutdown_owner())
            .await
            .expect("close the test EventWriter admission boundary")
    }

    fn test_maintenance_batch(kind: &str) -> EventBatch {
        EventBatch {
            writes: vec![EventWrite {
                event: Some(DurableEvent::memory_maintenance(kind).unwrap()),
                projections: Vec::new(),
            }],
            injected_commands: Vec::new(),
        }
    }

    fn event_frame(seq: u64) -> OutboundFrame {
        OutboundFrame::Event {
            envelope: Envelope {
                seq: Some(seq),
                personality_agent_id: crate::gateway::test_personality_agent_id(),
                event: serde_json::json!({"type": "agent_start"}),
            },
        }
    }

    fn valid_command(seq: u64, command_id: &str) -> InboundCommand {
        InboundCommand::Valid(CommandEnvelope {
            personality_agent_id: crate::gateway::test_personality_agent_id(),
            provenance: crate::gateway::test_direct_chat_provenance(),
            seq,
            command_id: CommandId::parse(command_id).unwrap(),
            command: Command::Abort {},
        })
    }

    fn valid_command_for(
        seq: u64,
        command_id: &str,
        personality_agent_id: PersonalityAgentId,
        tenant_id: &str,
        principal_id: &str,
    ) -> InboundCommand {
        InboundCommand::Valid(CommandEnvelope {
            provenance: DirectChatProvenanceV1::new(
                tenant_id,
                personality_agent_id.clone(),
                principal_id,
            )
            .expect("valid direct-chat provenance"),
            personality_agent_id,
            seq,
            command_id: CommandId::parse(command_id).unwrap(),
            command: Command::Abort {},
        })
    }

    fn rejected_oversized_command(seq: u64, command_id: &str, actual_bytes: u64) -> InboundCommand {
        InboundCommand::Invalid {
            seq,
            command_id: CommandId::parse(command_id).unwrap(),
            personality_agent_id: crate::gateway::test_personality_agent_id(),
            provenance: crate::gateway::test_direct_chat_provenance(),
            reason: CommandRejectReason::Oversized { actual_bytes },
            raw_command: crate::gateway::RejectedCommandPayload::DiscardedOversized,
            payload_digest: Some(crate::gateway::KeyedCommandDigest::new("test-key", [0; 32])),
        }
    }

    // Tests

    #[tokio::test]
    async fn credential_scope_mismatch_is_fatal_before_connect() {
        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let connector = MockConnector::new(sent_hellos, VecDeque::new());
        let credentials = FixedCredentialProvider(GatewayCredential::new(
            "wrong-scope-token",
            PersonalityAgentId::parse("018f3f8d-7b2c-7a10-8f9e-123456789abd").unwrap(),
            ProcessGeneration::from_wire(7).unwrap(),
            DeliveryAuthorization::Raw,
        ));
        let source = MockDurableSource::new(CommandCursors::default());
        let latch = StaticHydrationLatch(HydrationReady {
            generation: ProcessGeneration::from_wire(7).unwrap(),
            receipt_identity: "receipt-1".to_owned(),
        });

        let supervisor =
            ConnectionSupervisor::new(connector, credentials, source, latch, make_config());
        let handle = supervisor.start();
        let error = handle
            .join()
            .await
            .expect_err("credential target mismatch must terminate the supervisor");

        assert!(
            error
                .to_string()
                .contains("gateway credential scope mismatch"),
            "unexpected supervisor error: {error:#}"
        );
    }

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
    async fn gateway_credential_delivery_authorization_binds_source() {
        #[derive(Clone)]
        struct BindingSource {
            bound: Arc<std::sync::Mutex<Option<DeliveryAuthorization>>>,
            events: Vec<OutboundFrame>,
        }

        #[async_trait]
        impl DurableSource for BindingSource {
            fn bind_delivery_authorization(
                &self,
                authorization: DeliveryAuthorization,
            ) -> Result<Self> {
                *self.bound.lock().unwrap() = Some(authorization);
                Ok(self.clone())
            }

            async fn event_cursor(&self) -> Result<EventCursors> {
                Ok(EventCursors {
                    last_sent: self.events.len() as u64,
                })
            }

            async fn events_after(
                &self,
                after_seq: u64,
                _limit: usize,
            ) -> Result<Vec<OutboundFrame>> {
                Ok(self
                    .events
                    .iter()
                    .filter(|f| outbound_frame_event_seq(f).unwrap_or(0) > after_seq)
                    .cloned()
                    .collect())
            }

            async fn command_cursors(&self) -> Result<CommandCursors> {
                Ok(CommandCursors::default())
            }
        }

        for authorization in [
            DeliveryAuthorization::Raw,
            DeliveryAuthorization::RedactionOnly,
        ] {
            let bound = Arc::new(std::sync::Mutex::new(None));
            let source = BindingSource {
                bound: bound.clone(),
                events: Vec::new(),
            };
            let gateway = MockGateway::new(VecDeque::new());
            let connector = MockConnector::new(
                Arc::new(std::sync::Mutex::new(Vec::new())),
                VecDeque::from([Ok(gateway)]),
            );
            let supervisor = ConnectionSupervisor::new(
                connector,
                CountingCredentialProvider::new("token").with_delivery_authorization(authorization),
                source,
                StaticHydrationLatch(HydrationReady {
                    generation: ProcessGeneration::from_wire(7).unwrap(),
                    receipt_identity: "credential-auth-binding".to_owned(),
                }),
                make_config(),
            );

            let handle = supervisor.start();
            tokio::time::timeout(Duration::from_secs(1), async {
                while bound.lock().unwrap().is_none() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("bind_delivery_authorization must be called");

            assert_eq!(
                *bound.lock().unwrap(),
                Some(authorization),
                "credential delivery authorization must bind to durable source"
            );

            handle.abort();
            assert!(handle.join().await.is_ok());
        }
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
            failure_tx: Arc<Mutex<Option<mpsc::UnboundedSender<DeliveryEpochFailure>>>>,
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
    async fn install_failure_invalidates_partial_epoch_before_reconnect() {
        #[derive(Clone)]
        struct PartialInstallSource {
            installs: Arc<AtomicU64>,
            invalidations: Arc<AtomicU64>,
            active: Arc<AtomicBool>,
            first_invalidated: Arc<Notify>,
            release_first_invalidation: Arc<Notify>,
        }

        #[async_trait]
        impl DurableSource for PartialInstallSource {
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
                let attempt = self.installs.fetch_add(1, Ordering::SeqCst);
                assert!(
                    !self.active.swap(true, Ordering::SeqCst),
                    "a stale partial mapping would wedge the next install"
                );
                if attempt == 0 {
                    bail!("synthetic partial install failure")
                }
                Ok(None)
            }

            async fn invalidate_delivery_epoch(&self, _epoch: DeliveryEpoch) -> Result<()> {
                let invalidation = self.invalidations.fetch_add(1, Ordering::SeqCst) + 1;
                assert!(
                    self.active.swap(false, Ordering::SeqCst),
                    "each installed epoch must be invalidated exactly once"
                );
                if invalidation == 1 {
                    self.first_invalidated.notify_one();
                    self.release_first_invalidation.notified().await;
                }
                Ok(())
            }
        }

        let source = PartialInstallSource {
            installs: Arc::new(AtomicU64::new(0)),
            invalidations: Arc::new(AtomicU64::new(0)),
            active: Arc::new(AtomicBool::new(false)),
            first_invalidated: Arc::new(Notify::new()),
            release_first_invalidation: Arc::new(Notify::new()),
        };
        let installs = source.installs.clone();
        let invalidations = source.invalidations.clone();
        let active = source.active.clone();
        let first_invalidated = source.first_invalidated.clone();
        let release_first_invalidation = source.release_first_invalidation.clone();
        let sent_hellos = Arc::new(std::sync::Mutex::new(Vec::new()));
        let supervisor = ConnectionSupervisor::new(
            MockConnector::new(
                sent_hellos,
                VecDeque::from([
                    Ok(MockGateway::new(VecDeque::new())),
                    Ok(MockGateway::new(VecDeque::new())),
                ]),
            ),
            CountingCredentialProvider::new("token"),
            source,
            StaticHydrationLatch(HydrationReady {
                generation: ProcessGeneration::from_wire(7).unwrap(),
                receipt_identity: "receipt-1".to_owned(),
            }),
            make_config(),
        );
        let handle = supervisor.start();
        let epochs = handle.epochs.clone();

        tokio::time::timeout(Duration::from_secs(1), first_invalidated.notified())
            .await
            .expect("install failure must invoke invalidation before reconnect");
        assert!(
            epochs.borrow().is_none(),
            "failed epoch must clear the supervisor's current mapping before cleanup returns"
        );
        release_first_invalidation.notify_one();

        tokio::time::timeout(Duration::from_secs(1), async {
            while installs.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("partial install cleanup must permit the next epoch");

        assert_eq!(invalidations.load(Ordering::SeqCst), 1);
        assert!(active.load(Ordering::SeqCst));

        handle.abort();
        handle.join().await.unwrap();
        assert_eq!(invalidations.load(Ordering::SeqCst), 2);
        assert!(!active.load(Ordering::SeqCst));
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
                fail_after_record: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
                release: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            next_command_seq: None,
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
                fail_after_record: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
                release: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            next_command_seq: None,
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
    async fn replacement_epoch_is_never_published_with_stale_online_true() {
        let sent_hellos = Arc::new(Mutex::new(Vec::new()));
        let source = DelayedCatchUpSource {
            events: Arc::new(Mutex::new(VecDeque::from([event_frame(1)]))),
            notify: Arc::new(Notify::new()),
            command_cursor: CommandCursors::default(),
        };

        let mut first_gateway = MockGateway::new(VecDeque::new());
        first_gateway.sent_hellos = sent_hellos.clone();
        first_gateway.last_received_event_seq = 1;
        first_gateway.writer.fail_after = Some(0);
        let mut second_gateway = MockGateway::new(VecDeque::new());
        second_gateway.sent_hellos = sent_hellos;
        second_gateway.last_received_event_seq = 0;
        let connector = MockConnector::new(
            Arc::new(Mutex::new(Vec::new())),
            VecDeque::from([Ok(first_gateway), Ok(second_gateway)]),
        );
        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token"),
            source,
            StaticHydrationLatch(HydrationReady {
                generation: ProcessGeneration::from_wire(7).unwrap(),
                receipt_identity: "epoch-online-order".to_owned(),
            }),
            make_config(),
        );
        let handle = supervisor.start();
        let mut epochs = handle.epochs.clone();
        let mut online = handle.online.clone();

        tokio::time::timeout(Duration::from_secs(1), async {
            while epochs.borrow().is_none() {
                epochs.changed().await.unwrap();
            }
            while !*online.borrow() {
                online.changed().await.unwrap();
            }
        })
        .await
        .expect("first epoch must become Online");
        let first_epoch = epochs.borrow_and_update().expect("first epoch");

        handle
            .events
            .send((first_epoch, event_frame(2)))
            .await
            .expect("live event triggers the first writer failure");
        let replacement = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                epochs.changed().await.unwrap();
                let observed = *epochs.borrow_and_update();
                if observed != Some(first_epoch) {
                    assert!(
                        !*online.borrow(),
                        "epoch transition {first_epoch:?} -> {observed:?} observed stale Online=true"
                    );
                }
                if let Some(epoch) = observed
                    && epoch != first_epoch
                {
                    break epoch;
                }
            }
        })
        .await
        .expect("replacement epoch must be published while catch-up is blocked");
        assert_ne!(replacement, first_epoch);
        assert!(
            !*online.borrow(),
            "replacement catch-up remains blocked and cannot already be Online"
        );

        handle.abort();
        assert!(handle.join().await.is_ok());
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
                fail_after_record: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            next_command_seq: None,
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
                fail_after_record: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
                release: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            next_command_seq: None,
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
                fail_after_record: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
                release: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            next_command_seq: None,
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
                fail_after_record: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            next_command_seq: None,
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
                fail_after_record: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            next_command_seq: None,
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
                personality_agent_id: crate::gateway::test_personality_agent_id(),
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
            &crate::gateway::test_personality_agent_id(),
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
            &crate::gateway::test_personality_agent_id(),
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
    async fn send_validated_rejects_a_different_personality_before_forwarding() {
        let expected = crate::gateway::test_personality_agent_id();
        let different = PersonalityAgentId::parse("018f3f8d-7b2c-7a10-8f9e-123456789abd").unwrap();
        let (mut tx, mut rx) = mpsc::channel::<InboundCommand>(1);

        let error = send_validated(
            valid_command_for(
                1,
                "00000000-0000-4000-8000-000000000001",
                different,
                "tenant-a",
                "human-a",
            ),
            &expected,
            1,
            &mut tx,
            &CancellationToken::new(),
            None,
        )
        .await
        .expect_err("a different personality must be rejected before admission");

        assert!(error.to_string().contains("command target mismatch"));
        assert!(
            rx.try_recv().is_err(),
            "mismatched command must not reach Store"
        );
    }

    #[tokio::test]
    async fn send_validated_keeps_one_runtime_identity_across_tenant_contexts() {
        let personality_agent_id = crate::gateway::test_personality_agent_id();
        let (mut tx, mut rx) = mpsc::channel::<InboundCommand>(2);
        let cancel = CancellationToken::new();

        let next = send_validated(
            valid_command_for(
                1,
                "00000000-0000-4000-8000-000000000001",
                personality_agent_id.clone(),
                "tenant-a",
                "human-a",
            ),
            &personality_agent_id,
            1,
            &mut tx,
            &cancel,
            None,
        )
        .await
        .unwrap();
        let next = send_validated(
            valid_command_for(
                2,
                "00000000-0000-4000-8000-000000000002",
                personality_agent_id.clone(),
                "tenant-b",
                "human-b",
            ),
            &personality_agent_id,
            next,
            &mut tx,
            &cancel,
            None,
        )
        .await
        .unwrap();

        assert_eq!(next, 3);
        let first = rx.recv().await.unwrap();
        let second = rx.recv().await.unwrap();
        assert_eq!(first.personality_agent_id(), second.personality_agent_id());
        assert_ne!(
            first.provenance().tenant_id(),
            second.provenance().tenant_id()
        );
        assert_ne!(
            first.provenance().actor().principal_id(),
            second.provenance().actor().principal_id()
        );
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
                personality_agent_id: crate::gateway::test_personality_agent_id(),
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
                personality_agent_id: crate::gateway::test_personality_agent_id(),
                status: CommandAckStatus::Received,
                reject_reason: None,
            },
        };
        let volatile = OutboundFrame::Event {
            envelope: Envelope {
                seq: None,
                personality_agent_id: crate::gateway::test_personality_agent_id(),
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
                fail_after_record: None,
                sent: Arc::new(std::sync::Mutex::new(Vec::new())),
                delay: None,
                block_after: None,
                block_notify: None,
                release: None,
            },
            sent_hellos: sent_hellos.clone(),
            hello_generation: None,
            next_command_seq: None,
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
                fail_after_record: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            next_command_seq: None,
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
            personality_agent_id: agent.personality_agent_id.clone(),
            accepted_generation: ProcessGeneration::from_wire(7).unwrap(),
            last_received_event_seq: 0,
            next_command_seq: 6,
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
            personality_agent_id: agent.personality_agent_id.clone(),
            accepted_generation: ProcessGeneration::from_wire(7).unwrap(),
            last_received_event_seq: 0,
            next_command_seq: 6,
        };
        validate_hello(&source, &agent, &api)
            .await
            .expect("original legal resend point must be accepted");

        // The API may start at an already-applied command when its terminal ACK
        // was lost; the durable consumer will re-ACK it without reapplying it.
        let api = ApiHello {
            personality_agent_id: agent.personality_agent_id.clone(),
            accepted_generation: ProcessGeneration::from_wire(7).unwrap(),
            last_received_event_seq: 0,
            next_command_seq: 5,
        };
        validate_hello(&source, &agent, &api)
            .await
            .expect("locally terminal command must remain replayable");

        let api = ApiHello {
            personality_agent_id: agent.personality_agent_id.clone(),
            accepted_generation: ProcessGeneration::from_wire(7).unwrap(),
            last_received_event_seq: 0,
            next_command_seq: 0,
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
                personality_agent_id: crate::gateway::test_personality_agent_id(),
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
                personality_agent_id: crate::gateway::test_personality_agent_id(),
                accepted_generation: ProcessGeneration::from_wire(0).unwrap(),
                last_received_event_seq: seq,
                next_command_seq: seq,
            };
            let text = serde_json::to_string(&api).expect("serialize api hello");
            let parsed: ApiHello = serde_json::from_str(&text).expect("deserialize api hello");
            assert_eq!(parsed.last_received_event_seq, seq);
            assert_eq!(parsed.next_command_seq, seq);
        }

        // i64::MAX and u64::MAX together in one hello.
        let agent_i64_max = AgentHello {
            personality_agent_id: crate::gateway::test_personality_agent_id(),
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
            r#"{{"personality_agent_id":"018f3f8d-7b2c-7a10-8f9e-123456789abc","generation":"{over_i64}","last_sent_event_seq":"0","last_received_command_seq":"0","last_applied_command_seq":"0"}}"#
        );
        assert!(serde_json::from_str::<AgentHello>(&agent_json).is_err());

        let api_json = format!(
            r#"{{"personality_agent_id":"018f3f8d-7b2c-7a10-8f9e-123456789abc","accepted_generation":"{over_i64}","last_received_event_seq":"0","next_command_seq":"1"}}"#
        );
        assert!(serde_json::from_str::<ApiHello>(&api_json).is_err());

        // u64 overflow is rejected.
        let agent_overflow = format!(
            r#"{{"personality_agent_id":"018f3f8d-7b2c-7a10-8f9e-123456789abc","generation":"0","last_sent_event_seq":"{over_u64}","last_received_command_seq":"0","last_applied_command_seq":"0"}}"#
        );
        assert!(serde_json::from_str::<AgentHello>(&agent_overflow).is_err());

        let api_overflow = format!(
            r#"{{"personality_agent_id":"018f3f8d-7b2c-7a10-8f9e-123456789abc","accepted_generation":"0","last_received_event_seq":"{over_u64}","next_command_seq":"1"}}"#
        );
        assert!(serde_json::from_str::<ApiHello>(&api_overflow).is_err());

        // Old numeric encodings are no longer accepted; the wire uses strings.
        let agent_numeric = r#"{"personality_agent_id":"018f3f8d-7b2c-7a10-8f9e-123456789abc","generation":1,"last_sent_event_seq":0,"last_received_command_seq":0,"last_applied_command_seq":0}"#;
        assert!(serde_json::from_str::<AgentHello>(agent_numeric).is_err());
        let api_numeric = r#"{"personality_agent_id":"018f3f8d-7b2c-7a10-8f9e-123456789abc","accepted_generation":1,"last_received_event_seq":0,"next_command_seq":1}"#;
        assert!(serde_json::from_str::<ApiHello>(api_numeric).is_err());
    }

    #[test]
    fn hello_dto_rejects_malformed_unknown_and_trailing_data() {
        let agent_base = r#"{"personality_agent_id":"018f3f8d-7b2c-7a10-8f9e-123456789abc","generation":"1","last_sent_event_seq":"0","last_received_command_seq":"0","last_applied_command_seq":"0"}"#;
        let api_base = r#"{"personality_agent_id":"018f3f8d-7b2c-7a10-8f9e-123456789abc","accepted_generation":"1","last_received_event_seq":"0","next_command_seq":"1"}"#;

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
        let agent_unknown = r#"{"personality_agent_id":"018f3f8d-7b2c-7a10-8f9e-123456789abc","generation":"1","last_sent_event_seq":"0","last_received_command_seq":"0","last_applied_command_seq":"0","extra":1}"#;
        assert!(serde_json::from_str::<AgentHello>(agent_unknown).is_err());

        let api_unknown = r#"{"personality_agent_id":"018f3f8d-7b2c-7a10-8f9e-123456789abc","accepted_generation":"1","last_received_event_seq":"0","next_command_seq":"1","extra":1}"#;
        assert!(serde_json::from_str::<ApiHello>(api_unknown).is_err());

        // Trailing data after a valid object must also be rejected.
        let agent_trailing = r#"{"personality_agent_id":"018f3f8d-7b2c-7a10-8f9e-123456789abc","generation":"1","last_sent_event_seq":"0","last_received_command_seq":"0","last_applied_command_seq":"0"}{"extra":1}"#;
        assert!(serde_json::from_str::<AgentHello>(agent_trailing).is_err());

        let api_trailing = r#"{"personality_agent_id":"018f3f8d-7b2c-7a10-8f9e-123456789abc","accepted_generation":"1","last_received_event_seq":"0","next_command_seq":"1"}{"extra":1}"#;
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
            personality_agent_id: crate::gateway::test_personality_agent_id(),
            generation: ProcessGeneration::from_wire(7).unwrap(),
            last_sent_event_seq: 0,
            last_received_command_seq: 0,
            last_applied_command_seq: 0,
        };
        let api = ApiHello {
            personality_agent_id: agent.personality_agent_id.clone(),
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
    async fn validate_hello_rejects_personality_mismatch_before_cursor_state() {
        let agent = AgentHello {
            personality_agent_id: crate::gateway::test_personality_agent_id(),
            generation: ProcessGeneration::from_wire(7).unwrap(),
            last_sent_event_seq: 0,
            last_received_command_seq: 0,
            last_applied_command_seq: 0,
        };
        let api = ApiHello {
            personality_agent_id: PersonalityAgentId::parse("018f3f8d-7b2c-7a10-8f9e-123456789abd")
                .unwrap(),
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 1,
        };

        let result = validate_hello(
            &MockDurableSource::new(CommandCursors::default()),
            &agent,
            &api,
        )
        .await;
        assert!(
            matches!(
                result,
                Err(SupervisorError::Fatal(ref error))
                    if error.to_string().contains("personality-agent claim mismatch")
            ),
            "wrong personality must be fatal before trusting peer cursors: {result:?}"
        );
    }

    #[tokio::test]
    async fn validate_hello_command_cursor_cannot_skip_nonterminal_prefix() {
        let cursor = CommandCursors {
            received: 10,
            applied: 5,
        };
        let agent = AgentHello {
            personality_agent_id: crate::gateway::test_personality_agent_id(),
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
            personality_agent_id: agent.personality_agent_id.clone(),
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 5,
        };
        validate_hello(&StaticSource(cursor), &agent, &api)
            .await
            .expect("terminal ACK recovery must allow replay at seq 5");

        // Exactly applied+1 is the normal catch-up boundary and is allowed.
        let api = ApiHello {
            personality_agent_id: agent.personality_agent_id.clone(),
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 6,
        };
        validate_hello(&StaticSource(cursor), &agent, &api)
            .await
            .unwrap();

        // A cursor after applied+1 skips a locally nonterminal command.
        let api = ApiHello {
            personality_agent_id: agent.personality_agent_id.clone(),
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 7,
        };
        assert!(
            validate_hello(&StaticSource(cursor), &agent, &api)
                .await
                .is_err()
        );

        let api = ApiHello {
            personality_agent_id: agent.personality_agent_id.clone(),
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 11,
        };
        assert!(
            validate_hello(&StaticSource(cursor), &agent, &api)
                .await
                .is_err()
        );

        // Ahead of received+1 is also fatal.
        let api = ApiHello {
            personality_agent_id: agent.personality_agent_id.clone(),
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

        let api = ApiHello {
            personality_agent_id: agent.personality_agent_id.clone(),
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: 0,
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
                    personality_agent_id: hello.personality_agent_id.clone(),
                    accepted_generation: hello.generation,
                    last_received_event_seq: 0,
                    next_command_seq: self.terminal_seq,
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
                    fail_after_record: None,
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
                            personality_agent_id: crate::gateway::test_personality_agent_id(),
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
            personality_agent_id: crate::gateway::test_personality_agent_id(),
            generation: ProcessGeneration::from_wire(7).unwrap(),
            last_sent_event_seq: 0,
            last_received_command_seq: cursor.received,
            last_applied_command_seq: cursor.applied,
        };
        let api = ApiHello {
            personality_agent_id: agent.personality_agent_id.clone(),
            accepted_generation: agent.generation,
            last_received_event_seq: 0,
            next_command_seq: u64::MAX,
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
                fail_after_record: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            next_command_seq: None,
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
                personality_agent_id: crate::gateway::test_personality_agent_id(),
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
                personality_agent_id: crate::gateway::test_personality_agent_id(),
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

    /// A durable source that owns a real DeliveryPump and exposes explicit
    /// barriers at the catch-up and Online boundary so the test can exercise
    /// the exact interleaving between `mark_delivery_online` and the
    /// supervisor's `online` watch.
    #[derive(Clone)]
    struct BoundaryRaceSource {
        store: Arc<Store>,
        pump: Arc<std::sync::Mutex<Option<DeliveryPump>>>,
        catch_up_entered: Arc<Notify>,
        catch_up_release: Arc<Notify>,
        mark_online_entered: Arc<Notify>,
        mark_online_release: Arc<Notify>,
        post_mark_online: Arc<Notify>,
        boundary_release: Arc<Notify>,
    }

    impl BoundaryRaceSource {
        fn new(store: Arc<Store>) -> Self {
            Self {
                store,
                pump: Arc::new(std::sync::Mutex::new(None)),
                catch_up_entered: Arc::new(Notify::new()),
                catch_up_release: Arc::new(Notify::new()),
                mark_online_entered: Arc::new(Notify::new()),
                mark_online_release: Arc::new(Notify::new()),
                post_mark_online: Arc::new(Notify::new()),
                boundary_release: Arc::new(Notify::new()),
            }
        }

        fn pump(&self) -> DeliveryPump {
            self.pump.lock().unwrap().clone().expect("pump installed")
        }
    }

    #[async_trait]
    impl DurableSource for BoundaryRaceSource {
        async fn event_cursor(&self) -> Result<EventCursors> {
            Ok(EventCursors { last_sent: 1 })
        }

        async fn events_after(&self, after_seq: u64, _limit: usize) -> Result<Vec<OutboundFrame>> {
            if after_seq == 0 {
                self.catch_up_entered.notify_one();
                self.catch_up_release.notified().await;
                Ok(vec![event_frame(1)])
            } else {
                Ok(Vec::new())
            }
        }

        async fn command_cursors(&self) -> Result<CommandCursors> {
            Ok(CommandCursors::default())
        }

        async fn install_delivery_epoch(
            &self,
            epoch: DeliveryEpoch,
            _catch_up_from_seq: u64,
            sink: EventSender,
            cancel: CancellationToken,
        ) -> Result<Option<DeliveryEpochRuntime>> {
            let (channel, mut delivery_rx) =
                DeliveryChannelBuilder::with_mode(DeliveryMode::Raw).build();
            let (failure_tx, failure_rx) = mpsc::unbounded_channel();
            let pump = DeliveryPump::new(self.store.clone(), channel);
            pump.install_supervised_epoch(epoch, failure_tx.clone());
            *self.pump.lock().unwrap() = Some(pump);

            let personality_agent_id = crate::gateway::test_personality_agent_id();
            let task = tokio::spawn(async move {
                loop {
                    let frame = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => break,
                        frame = delivery_rx.recv() => match frame {
                            Some(frame) => frame,
                            None => break,
                        }
                    };
                    let (epoch, seq, event) = match frame {
                        DeliveryFrame::Durable { .. } => continue,
                        DeliveryFrame::Volatile { epoch, event } => {
                            let event = match serde_json::to_value(event) {
                                Ok(event) => event,
                                Err(error) => {
                                    let _ = failure_tx.send(DeliveryEpochFailure::Fatal(format!(
                                        "failed to serialize volatile event: {error}"
                                    )));
                                    break;
                                }
                            };
                            (epoch, None, event)
                        }
                    };
                    let outbound = OutboundFrame::Event {
                        envelope: Envelope {
                            seq,
                            personality_agent_id: personality_agent_id.clone(),
                            event,
                        },
                    };
                    let send = sink.send_from_delivery_pump((epoch, outbound));
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => break,
                        result = send => {
                            if result.is_err() {
                                break;
                            }
                        }
                    }
                }
            });
            Ok(Some(DeliveryEpochRuntime::new(failure_rx, task)))
        }

        async fn mark_delivery_online(&self, epoch: DeliveryEpoch) -> Result<()> {
            self.mark_online_entered.notify_one();
            self.mark_online_release.notified().await;
            let pump = self
                .pump
                .lock()
                .unwrap()
                .clone()
                .context("BoundaryRaceSource pump missing")?;
            pump.mark_online(epoch)?;
            self.post_mark_online.notify_one();
            self.boundary_release.notified().await;
            Ok(())
        }

        async fn invalidate_delivery_epoch(&self, epoch: DeliveryEpoch) -> Result<()> {
            if let Some(pump) = self.pump.lock().unwrap().clone() {
                let _ = pump.invalidate_epoch(epoch);
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn first_boundary_volatile_is_forwarded_without_early_public_online() {
        let store = Arc::new(
            Store::session_test_store("boundary-race")
                .await
                .expect("open test store"),
        );
        let source = BoundaryRaceSource::new(store);
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let gateway = MockGateway {
            reader: MockGatewayReader {
                commands: VecDeque::new(),
                panic: false,
                on_empty: None,
            },
            writer: MockGatewayWriter {
                fail_after: None,
                fail_after_record: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            next_command_seq: None,
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
            source.clone(),
            latch,
            make_config(),
        );
        let handle = supervisor.start();

        // Wait for the epoch to be established and catch-up to block.
        let mut epochs = handle.epochs.clone();
        while epochs.borrow().is_none() {
            epochs.changed().await.unwrap();
        }

        tokio::time::timeout(Duration::from_secs(1), source.catch_up_entered.notified())
            .await
            .expect("writer must enter catch-up");

        // A volatile sent while the pump is still CatchingUp must be dropped.
        source
            .pump()
            .on_volatile(AgentEvent::MessageUpdate {
                message_id: "pre-online".to_owned(),
                event: PublicStreamEvent::TextDelta {
                    content_index: 0,
                    delta: "drop".to_owned(),
                },
            })
            .await
            .unwrap();

        // Release catch-up so the writer reaches the Online boundary.
        source.catch_up_release.notify_one();

        // Wait for mark_delivery_online to be entered. The public Online watch
        // must remain false until the DeliveryPump barrier succeeds, so direct
        // SupervisorHandle producers cannot observe a false Online.
        tokio::time::timeout(
            Duration::from_secs(1),
            source.mark_online_entered.notified(),
        )
        .await
        .expect("writer must enter mark_delivery_online");
        assert!(
            !*handle.online.borrow(),
            "online watch must remain false before DeliveryPump mark_online completes"
        );

        // The pump is still CatchingUp here, so this volatile must be dropped.
        source
            .pump()
            .on_volatile(AgentEvent::MessageUpdate {
                message_id: "during-mark-online".to_owned(),
                event: PublicStreamEvent::TextDelta {
                    content_index: 0,
                    delta: "drop".to_owned(),
                },
            })
            .await
            .unwrap();

        // Release mark_online. The pump transitions to Online, but the source
        // holds the return so the test can inject the first accepted volatile
        // before writer_task publishes its public Online watch.
        source.mark_online_release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), source.post_mark_online.notified())
            .await
            .expect("pump must be marked online");
        assert!(
            !*handle.online.borrow(),
            "public Online must not be visible until the pump barrier returns successfully"
        );

        // This is the first volatile accepted by the pump immediately at the
        // catch-up -> Online boundary. Its pump admission is authoritative even
        // though the public watch is intentionally still false.
        source
            .pump()
            .on_volatile(AgentEvent::MessageUpdate {
                message_id: "boundary".to_owned(),
                event: PublicStreamEvent::TextDelta {
                    content_index: 0,
                    delta: "forward".to_owned(),
                },
            })
            .await
            .unwrap();

        // Let writer_task complete the Online transition.
        source.boundary_release.notify_one();

        // Wait for the supervisor to publish Online.
        let mut online = handle.online.clone();
        while !*online.borrow() {
            online.changed().await.unwrap();
        }

        // Wait for the boundary volatile to reach the writer.
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let found = {
                    let sent_frames = sent.lock().unwrap();
                    sent_frames.iter().any(|f| {
                        matches!(
                            f,
                            OutboundFrame::Event { envelope }
                            if envelope.seq.is_none()
                                && envelope.event.get("message_id")
                                    .and_then(|v| v.as_str()) == Some("boundary")
                        )
                    })
                };
                if found {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("boundary volatile must be forwarded");

        {
            let sent_frames = sent.lock().unwrap();
            let seqs: Vec<_> = sent_frames
                .iter()
                .filter_map(|f| outbound_frame_event_seq(f).ok())
                .collect();
            assert_eq!(seqs, vec![1], "catch-up durable must be delivered first");
            let volatiles: Vec<_> = sent_frames
                .iter()
                .filter(
                    |f| matches!(f, OutboundFrame::Event { envelope } if envelope.seq.is_none()),
                )
                .collect();
            assert_eq!(
                volatiles.len(),
                1,
                "exactly one volatile (the boundary volatile) must be delivered"
            );
        }

        handle.abort();
        let _ = handle.join().await;
    }

    #[tokio::test]
    async fn queued_volatile_does_not_overtake_preceding_durable_event() {
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut writer = MockGatewayWriter {
            fail_after: None,
            fail_after_record: None,
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
                    personality_agent_id: crate::gateway::test_personality_agent_id(),
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
    async fn offline_durable_arrival_fences_later_terminal_ack_until_catch_up() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let mut writer = MockGatewayWriter {
            fail_after: None,
            fail_after_record: None,
            sent: sent.clone(),
            delay: None,
            block_after: None,
            block_notify: None,
            release: None,
        };
        let (_writer_tx, mut writer_rx) = mpsc::channel(2);
        let token = CancellationToken::new();
        let mut next_arrival_id = 0;
        let mut outbox = VecDeque::new();
        let mut pending_events = BTreeMap::new();
        let mut last_received = 0;

        classify_frame(
            event_frame(2),
            false,
            last_received,
            &mut next_arrival_id,
            &mut outbox,
            &mut pending_events,
        );
        classify_frame(
            OutboundFrame::CommandAck {
                ack: CommandAck {
                    seq: 1,
                    command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                    personality_agent_id: crate::gateway::test_personality_agent_id(),
                    status: CommandAckStatus::Applied,
                    reject_reason: None,
                },
            },
            false,
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
            false,
        )
        .await
        .expect("offline drain");
        assert!(
            sent.lock().unwrap().is_empty(),
            "terminal ACK must remain behind its earlier durable arrival"
        );

        // Production catch-up has now durably sent through seq 2. Pruning the
        // matching live marker releases the later terminal ACK.
        last_received = 2;
        drain_next(
            &mut writer,
            Duration::from_secs(1),
            &token,
            &mut writer_rx,
            &mut next_arrival_id,
            &mut outbox,
            &mut pending_events,
            &mut last_received,
            false,
        )
        .await
        .expect("post-catch-up drain");
        assert!(matches!(
            sent.lock().unwrap().as_slice(),
            [OutboundFrame::CommandAck { ack }] if ack.status == CommandAckStatus::Applied
        ));
    }

    #[tokio::test]
    async fn live_durable_gap_fails_without_sending_or_advancing() {
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut writer = MockGatewayWriter {
            fail_after: None,
            fail_after_record: None,
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
                fail_after_record: None,
                sent: sent.clone(),
                delay: Some(Duration::from_millis(2)),
                block_after: None,
                block_notify: None,
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            next_command_seq: None,
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
                personality_agent_id: crate::gateway::test_personality_agent_id(),
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
                personality_agent_id: crate::gateway::test_personality_agent_id(),
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
                fail_after_record: None,
                sent: sent.clone(),
                delay: None,
                block_after: None,
                block_notify: None,
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            next_command_seq: None,
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
    async fn hydration_hold_buffers_exact_limit_before_ready_then_drains() {
        let generation = ProcessGeneration::from_wire(7).unwrap();
        let (latch, latch_tx) = DynamicHydrationLatch::new();
        let commands: VecDeque<_> = (1..=16)
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
        let source = MockDurableSource::new(CommandCursors::default());

        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token"),
            source,
            latch,
            make_config(),
        );
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
                receipt_identity: "limit-ready".to_owned(),
            }))
            .unwrap();

        for expected in 1..=16 {
            let command = tokio::time::timeout(Duration::from_secs(1), handle.commands.recv())
                .await
                .expect("exact-limit hydration burst must drain in order")
                .expect("command channel must remain open");
            assert_eq!(cmd_seq(&command), expected);
        }
        assert_eq!(
            sent_hellos.lock().unwrap().len(),
            1,
            "exact-limit hold must not manufacture a reconnect"
        );
        handle.abort();
        handle.join().await.unwrap();
    }

    #[tokio::test]
    async fn hydration_hold_fails_closed_on_limit_plus_one() {
        let (latch, _latch_tx) = DynamicHydrationLatch::new();
        let commands: VecDeque<_> = (1..=17)
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
        let source = MockDurableSource::new(CommandCursors::default());

        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token"),
            source,
            latch,
            make_config(),
        );
        let mut handle = supervisor.start();

        assert!(
            handle.commands.try_recv().is_err(),
            "limit+1 command must not be exposed before Ready"
        );

        let result = tokio::time::timeout(Duration::from_secs(1), handle.join())
            .await
            .expect("supervisor must fail closed within bounded time");
        let err = result.unwrap_err();
        assert!(
            format!("{err:#}").contains("hydration hold limit exceeded"),
            "expected hold-overflow error, got {err:#}"
        );

        assert_eq!(
            sent_hellos.lock().unwrap().len(),
            1,
            "hold overflow must not retry as a new connection"
        );
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
                fail_after_record: None,
                sent: sent.clone(),
                delay: Some(Duration::from_millis(2)),
                block_after: None,
                block_notify: None,
                release: None,
            },
            sent_hellos: Arc::new(std::sync::Mutex::new(Vec::new())),
            hello_generation: None,
            next_command_seq: None,
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
                personality_agent_id: crate::gateway::test_personality_agent_id(),
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
        let json = r#"{"personality_agent_id":"018f3f8d-7b2c-7a10-8f9e-123456789abc","generation":"1","last_sent_event_seq":"0","last_received_command_seq":"0","last_applied_command_seq":"0","extra":1}"#;
        assert!(
            serde_json::from_str::<AgentHello>(json).is_err(),
            "AgentHello must reject unknown fields"
        );
    }

    #[test]
    fn api_hello_rejects_unknown_fields() {
        let json = r#"{"personality_agent_id":"018f3f8d-7b2c-7a10-8f9e-123456789abc","accepted_generation":"1","last_received_event_seq":"0","next_command_seq":"1","extra":1}"#;
        assert!(
            serde_json::from_str::<ApiHello>(json).is_err(),
            "ApiHello must reject unknown fields"
        );
    }

    #[test]
    fn hello_dto_deserialization_still_accepts_known_fields() {
        let agent_json = r#"{"personality_agent_id":"018f3f8d-7b2c-7a10-8f9e-123456789abc","generation":"1","last_sent_event_seq":"0","last_received_command_seq":"0","last_applied_command_seq":"0"}"#;
        assert!(serde_json::from_str::<AgentHello>(agent_json).is_ok());

        let api_json = r#"{"personality_agent_id":"018f3f8d-7b2c-7a10-8f9e-123456789abc","accepted_generation":"1","last_received_event_seq":"0","next_command_seq":"1"}"#;
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
            fail_after_record: None,
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
            personality_agent_id: crate::gateway::test_personality_agent_id(),
            accepted_generation: ProcessGeneration::from_wire(7).unwrap(),
            last_received_event_seq: 0,
            next_command_seq: 1,
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
            fail_after_record: None,
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
            personality_agent_id: crate::gateway::test_personality_agent_id(),
            accepted_generation: ProcessGeneration::from_wire(7).unwrap(),
            last_received_event_seq: 0,
            next_command_seq: 1,
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
        let lease = ProcessGenerationLease::new(
            store.scope().personality_agent_id.clone(),
            generation,
            "lease-t17-t24",
        )
        .unwrap();
        let fence = GenerationRecoveryFence::new(&lease, "fence-t17-t24").unwrap();
        let receipt = match store.hydrate(&lease, &fence).await.unwrap() {
            HydrationOutcome::Complete(state) => state.receipt,
            other => panic!("empty test store must hydrate without recovery: {other:?}"),
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
        let post_commit_epoch = PostCommitEpochCapability::unbound_test(CancellationToken::new());
        adapter
            .admit_ordered_commit(&post_commit_epoch, 10)
            .await
            .unwrap();

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

            wait_for_t17_idle(&adapter).await;
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
        store.publish_test_committed_event_receipt(&[1]).unwrap();
        let base_adapter = seams::T17StoreAdapter::new(store.clone());
        let (adapter, dispatcher) =
            bind_test_post_commit_dispatcher(store.clone(), &base_adapter, 1);

        let sent = Arc::new(Mutex::new(Vec::new()));
        let mut gateway = MockGateway::new(VecDeque::new());
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
        let session_gateway = session::SessionGateway::from(handle);
        let (_session_reader, mut session_writer) = session_gateway.split();
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
        store.publish_test_committed_event_receipt(&[2]).unwrap();
        session_writer
            .send(OutboundFrame::Event {
                envelope: Envelope {
                    seq: Some(2),
                    personality_agent_id: store.scope().personality_agent_id.clone(),
                    // This Session-carried payload is intentionally raw and
                    // different from the committed row. The adapter must
                    // discard it and make T17 re-read seq=2.
                    event: serde_json::json!({
                        "type": "error",
                        "message": format!("must-not-bypass-T17 {raw_secret}")
                    }),
                },
            })
            .await
            .unwrap();
        session_writer
            .send(OutboundFrame::Event {
                envelope: Envelope {
                    seq: None,
                    personality_agent_id: store.scope().personality_agent_id.clone(),
                    event: serde_json::to_value(crate::agent::AgentEvent::ToolExecutionUpdate {
                        tool_call_id: "tool-1".to_owned(),
                        partial: serde_json::json!({"stdout": raw_secret}),
                    })
                    .unwrap(),
                },
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

        drop(session_writer);
        wait_for_t17_idle(&adapter).await;
        // This projection-focused fixture inserts encrypted rows directly and
        // intentionally has no authenticated EventWriter head to quiesce.
        drop(dispatcher);

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
    async fn raw_epoch_backlog_cannot_flush_into_redaction_only_reconnect() {
        let store = Arc::new(
            Store::session_test_store("t17-t24-raw-to-redaction")
                .await
                .unwrap(),
        );
        let base_adapter = seams::T17StoreAdapter::new(store.clone());
        let (adapter, dispatcher) =
            bind_test_post_commit_dispatcher(store.clone(), &base_adapter, 0);
        let raw_secret = "sk-raw-epoch-secret-value";
        let raw_sent = Arc::new(Mutex::new(Vec::new()));
        let redacted_sent = Arc::new(Mutex::new(Vec::new()));
        let raw_writer_blocked = Arc::new(Notify::new());

        let mut raw_gateway = MockGateway::new(VecDeque::new());
        raw_gateway.writer.sent = raw_sent.clone();
        raw_gateway.writer = raw_gateway
            .writer
            .with_block_after(1, raw_writer_blocked.clone());
        let mut redacted_gateway = MockGateway::new(VecDeque::new());
        redacted_gateway.writer.sent = redacted_sent.clone();
        let sent_hellos = Arc::new(Mutex::new(Vec::new()));
        let connector = MockConnector::new(
            sent_hellos,
            VecDeque::from([Ok(raw_gateway), Ok(redacted_gateway)]),
        );
        let mut config = make_config();
        config.initial_backoff = Duration::ZERO;
        config.max_backoff = Duration::ZERO;
        config.send_timeout = Duration::from_millis(25);
        let supervisor = ConnectionSupervisor::new(
            connector,
            SequencedAuthorizationProvider::new([
                DeliveryAuthorization::Raw,
                DeliveryAuthorization::RedactionOnly,
            ]),
            adapter.clone(),
            StaticHydrationLatch(HydrationReady {
                generation: ProcessGeneration::from_wire(7).unwrap(),
                receipt_identity: "raw-to-redaction-ready".to_owned(),
            }),
            config,
        );
        let handle = supervisor.start();
        let mut online = handle.online.clone();
        let mut epochs = handle.epochs.clone();
        let session_gateway = session::SessionGateway::from(handle);
        let (_session_reader, mut session_writer) = session_gateway.split();

        tokio::time::timeout(Duration::from_secs(1), async {
            while !*online.borrow() {
                online.changed().await.unwrap();
            }
        })
        .await
        .expect("raw epoch must become Online");
        let raw_epoch = (*epochs.borrow()).expect("raw delivery epoch installed");

        let projection = insert_test_durable_event(
            &store,
            1,
            &crate::agent::AgentEvent::Error {
                message: format!("durable {raw_secret}"),
            },
        )
        .await
        .unwrap();
        store.publish_test_committed_event_receipt(&[1]).unwrap();
        session_writer
            .send(OutboundFrame::Event {
                envelope: Envelope {
                    seq: Some(1),
                    personality_agent_id: store.scope().personality_agent_id.clone(),
                    event: serde_json::json!({
                        "type": "error",
                        "message": format!("untrusted Session copy {raw_secret}")
                    }),
                },
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), raw_writer_blocked.notified())
            .await
            .expect("raw writer must block with the durable frame in flight");

        session_writer
            .send(OutboundFrame::Event {
                envelope: Envelope {
                    seq: None,
                    personality_agent_id: store.scope().personality_agent_id.clone(),
                    event: serde_json::to_value(crate::agent::AgentEvent::ToolExecutionUpdate {
                        tool_call_id: "raw-backlog-tool".to_owned(),
                        partial: serde_json::json!({"stdout": raw_secret}),
                    })
                    .unwrap(),
                },
            })
            .await
            .expect("raw volatile may enter only the old T17 epoch");

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let replacement = *epochs.borrow();
                if replacement.is_some_and(|epoch| epoch != raw_epoch) && *online.borrow() {
                    break;
                }
                tokio::select! {
                    _ = epochs.changed() => {},
                    _ = online.changed() => {},
                }
            }
        })
        .await
        .expect("writer timeout must reconnect under RedactionOnly authorization");

        tokio::time::timeout(Duration::from_secs(1), async {
            while redacted_sent.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("redaction-only reconnect must catch up the durable event");

        drop(session_writer);
        wait_for_t17_idle(&adapter).await;
        // This projection-focused fixture inserts encrypted rows directly and
        // intentionally has no authenticated EventWriter head to quiesce.
        drop(dispatcher);

        assert!(
            serde_json::to_string(&*raw_sent.lock().unwrap())
                .unwrap()
                .contains(raw_secret),
            "the Raw epoch may receive authorized raw material"
        );
        let expected_projection = serde_json::from_str::<serde_json::Value>(&projection).unwrap();
        let replacement_frames = redacted_sent.lock().unwrap();
        assert_eq!(
            replacement_frames
                .iter()
                .filter_map(|frame| match frame {
                    OutboundFrame::Event { envelope } if envelope.seq == Some(1) => {
                        Some(envelope.event.clone())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![expected_projection],
            "replacement epoch must re-read the projected durable row exactly once"
        );
        assert!(
            !serde_json::to_string(&*replacement_frames)
                .unwrap()
                .contains(raw_secret),
            "old Raw payloads must not cross into RedactionOnly"
        );
        assert!(
            !replacement_frames.iter().any(
                |frame| matches!(frame, OutboundFrame::Event { envelope } if envelope.seq.is_none())
            ),
            "old raw volatile backlog must not flush into RedactionOnly"
        );
    }

    #[tokio::test]
    async fn offline_commit_burst_over_session_capacity_replays_once_after_connect() {
        let store = Arc::new(
            Store::session_test_store("t17-t24-offline-commit-burst")
                .await
                .unwrap(),
        );
        let base_adapter = seams::T17StoreAdapter::new(store.clone());
        let (adapter, dispatcher) =
            bind_test_post_commit_dispatcher(store.clone(), &base_adapter, 0);
        let sent = Arc::new(Mutex::new(Vec::new()));
        let mut gateway = MockGateway::new(VecDeque::new());
        gateway.writer.sent = sent.clone();
        let connect_gate = Arc::new(Notify::new());
        let connector = MockConnector::new(
            Arc::new(Mutex::new(Vec::new())),
            VecDeque::from([Ok(gateway)]),
        )
        .with_connect_gate(connect_gate.clone());
        let mut config = make_config();
        config.connect_timeout = Duration::from_secs(5);
        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token"),
            adapter.clone(),
            StaticHydrationLatch(HydrationReady {
                generation: ProcessGeneration::from_wire(7).unwrap(),
                receipt_identity: "offline-burst-ready".to_owned(),
            }),
            config,
        );
        let handle = supervisor.start();
        let mut online = handle.online.clone();
        let session_gateway = session::SessionGateway::from(handle);
        let (mut session_reader, mut session_writer) = session_gateway.split();

        for seq in 1..=80 {
            insert_test_durable_event(&store, seq, &crate::agent::AgentEvent::AgentStart)
                .await
                .unwrap();
            store.publish_test_committed_event_receipt(&[seq]).unwrap();
            session_writer
                .send(OutboundFrame::Event {
                    envelope: Envelope {
                        seq: Some(seq),
                        personality_agent_id: store.scope().personality_agent_id.clone(),
                        event: serde_json::json!({
                            "type": "error",
                            "message": "untrusted Session payload must be ignored"
                        }),
                    },
                })
                .await
                .expect("offline committed notification must remain nonfatal");
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(20), session_reader.next_command())
                .await
                .is_err(),
            "the stable Session command reader must remain open during offline burst"
        );
        assert!(
            sent.lock().unwrap().is_empty(),
            "no event may bypass T17 while connect is gated"
        );

        connect_gate.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !*online.borrow() {
                online.changed().await.unwrap();
            }
        })
        .await
        .expect("supervisor must catch up the offline burst and become Online");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let durable_count = sent
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|frame| outbound_frame_event_seq(frame).is_ok())
                    .count();
                if durable_count == 80 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all offline committed rows must replay");

        drop(session_writer);
        drop(session_reader);
        wait_for_t17_idle(&adapter).await;
        // This bounded-replay fixture inserts encrypted rows directly and
        // intentionally has no authenticated EventWriter head to quiesce.
        drop(dispatcher);

        let delivered: Vec<_> = sent
            .lock()
            .unwrap()
            .iter()
            .filter_map(|frame| outbound_frame_event_seq(frame).ok())
            .collect();
        assert_eq!(
            delivered,
            (1..=80).collect::<Vec<_>>(),
            "supervisor catch-up must replay each durable seq exactly once"
        );
    }

    #[tokio::test]
    async fn one_post_commit_dispatcher_fences_n_plus_one_and_ack_in_exact_lane_order() {
        const COMMAND_ID: &str = "00000000-0000-4000-8000-000000000001";
        let store = Arc::new(
            Store::session_test_store("t26-exact-post-commit-order")
                .await
                .unwrap(),
        );
        let base_adapter = seams::T17StoreAdapter::new(store.clone())
            .bind_delivery_authorization(DeliveryAuthorization::Raw)
            .unwrap();
        let hook = seams::DurableAdmissionHook::default();
        base_adapter.set_durable_admission_hook(Some(hook.clone()));

        let epoch = DeliveryEpoch::for_test("t26-exact-order");
        let (event_tx, mut event_rx) = mpsc::channel(4);
        let (_online_tx, online) = watch::channel(true);
        let (_epochs_tx, epochs) = watch::channel(Some(epoch));
        let events = EventSender {
            tx: event_tx,
            online: online.clone(),
        };
        let pump_cancel = CancellationToken::new();
        let runtime = base_adapter
            .install_delivery_epoch(epoch, 0, events.clone(), pump_cancel.child_token())
            .await
            .unwrap()
            .expect("T17 adapter installs one delivery epoch");
        let (adapter, dispatcher) =
            bind_test_post_commit_dispatcher(store.clone(), &base_adapter, 0);

        let (_command_tx, commands) = mpsc::channel(1);
        let gateway = session::SessionGateway::from(SupervisorHandle {
            commands,
            events: events.clone(),
            epochs,
            online,
            session_events: adapter.session_event_sink(),
            lifecycle: SupervisorLifecycle {
                cancel: pump_cancel.clone(),
                task: None,
            },
        });
        let (_reader, mut session_writer) = gateway.split();
        let event_writer = EventWriter::new(store.clone());

        assert_eq!(
            event_writer
                .apply(test_maintenance_batch("memory-maintenance"))
                .await
                .unwrap(),
            vec![1]
        );
        tokio::time::timeout(Duration::from_secs(1), hook.reserved.notified())
            .await
            .expect("N must enter the single dispatcher first");

        assert_eq!(
            event_writer
                .apply(test_maintenance_batch("session-output"))
                .await
                .unwrap(),
            vec![2]
        );
        let personality_agent_id = store.scope().personality_agent_id.clone();
        let write = tokio::spawn(async move {
            session_writer
                .send(OutboundFrame::Event {
                    envelope: Envelope {
                        seq: Some(2),
                        personality_agent_id: personality_agent_id.clone(),
                        event: serde_json::json!({
                            "type": "memory_maintenance",
                            "kind": "untrusted-session-copy"
                        }),
                    },
                })
                .await?;
            session_writer
                .send(OutboundFrame::CommandAck {
                    ack: CommandAck {
                        seq: 1,
                        command_id: COMMAND_ID.to_owned(),
                        personality_agent_id,
                        status: CommandAckStatus::Applied,
                        reject_reason: None,
                    },
                })
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !write.is_finished(),
            "Session must await the same N+1 dispatcher proof before its ACK"
        );
        assert!(
            event_rx.try_recv().is_err(),
            "holding N admission must keep N+1 and the ACK out of T24"
        );

        hook.allow_registration.notify_one();
        tokio::time::timeout(Duration::from_secs(1), hook.registered.notified())
            .await
            .expect("N registers its completion fence");
        hook.allow_delivery.notify_one();
        let first = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("N reaches T24")
            .expect("T24 lane remains open");
        assert_eq!(outbound_frame_event_seq(&first.2).unwrap(), 1);

        tokio::time::timeout(Duration::from_secs(1), hook.reserved.notified())
            .await
            .expect("N+1 starts only after N admission completes");
        assert!(
            event_rx.try_recv().is_err(),
            "the terminal ACK cannot overtake held N+1"
        );
        hook.allow_registration.notify_one();
        tokio::time::timeout(Duration::from_secs(1), hook.registered.notified())
            .await
            .expect("N+1 registers its completion fence");
        hook.allow_delivery.notify_one();

        let second = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("N+1 reaches T24")
            .expect("T24 lane remains open");
        let third = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("ACK reaches T24")
            .expect("T24 lane remains open");
        assert_eq!(outbound_frame_event_seq(&second.2).unwrap(), 2);
        assert!(matches!(third.2, OutboundFrame::CommandAck { .. }));
        assert_eq!(first.0, epoch);
        assert_eq!(second.0, epoch);
        assert_eq!(third.0, epoch);
        tokio::time::timeout(Duration::from_secs(1), write)
            .await
            .expect("Session writer completes after ordered admission")
            .expect("Session writer task")
            .expect("durable frame and ACK send");

        base_adapter.set_durable_admission_hook(None);
        let quiescence = close_test_post_commit_writer(&store, &dispatcher).await;
        dispatcher.shutdown(quiescence).await.unwrap();
        pump_cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), runtime.join())
            .await
            .expect("delivery forwarder terminates")
            .expect("delivery forwarder joins");
    }

    #[tokio::test]
    async fn durable_fence_registration_is_atomic_with_epoch_invalidation() {
        const COMMAND_ID: &str = "00000000-0000-4000-8000-000000000001";
        let store = Arc::new(
            Store::session_test_store("t17-t24-atomic-durable-admission")
                .await
                .unwrap(),
        );
        let base_adapter = seams::T17StoreAdapter::new(store.clone())
            .bind_delivery_authorization(DeliveryAuthorization::Raw)
            .unwrap();
        let hook = seams::DurableAdmissionHook::default();
        base_adapter.set_durable_admission_hook(Some(hook.clone()));

        let epoch = DeliveryEpoch::for_test("atomic-admission-old");
        let replacement = DeliveryEpoch::for_test("atomic-admission-replacement");
        let (event_tx, mut event_rx) = mpsc::channel(4);
        let (online_tx, online) = watch::channel(true);
        let (epochs_tx, epochs) = watch::channel(Some(epoch));
        let events = EventSender {
            tx: event_tx,
            online: online.clone(),
        };
        let pump_cancel = CancellationToken::new();
        let runtime = base_adapter
            .install_delivery_epoch(epoch, 0, events.clone(), pump_cancel.child_token())
            .await
            .unwrap()
            .expect("T17 adapter installs a forwarder runtime");
        let (adapter, dispatcher) =
            bind_test_post_commit_dispatcher(store.clone(), &base_adapter, 0);
        let dispatcher_client = dispatcher.client();
        assert_eq!(
            EventWriter::new(store.clone())
                .apply(test_maintenance_batch("atomic-admission"))
                .await
                .unwrap(),
            vec![1]
        );
        let (_command_tx, commands) = mpsc::channel(1);
        let gateway = session::SessionGateway::from(SupervisorHandle {
            commands,
            events: events.clone(),
            epochs,
            online,
            session_events: adapter.session_event_sink(),
            lifecycle: SupervisorLifecycle {
                cancel: pump_cancel.clone(),
                task: None,
            },
        });
        let (_reader, mut writer) = gateway.split();
        let personality_agent_id = store.scope().personality_agent_id.clone();

        tokio::time::timeout(Duration::from_secs(1), hook.reserved.notified())
            .await
            .expect("durable callback must reserve the old epoch");
        let invalidating_adapter = adapter.clone();
        let invalidation =
            tokio::spawn(
                async move { invalidating_adapter.invalidate_delivery_epoch(epoch).await },
            );
        tokio::task::yield_now().await;
        assert!(
            !invalidation.is_finished(),
            "invalidation must not pass between epoch reservation and fence registration"
        );

        hook.allow_registration.notify_one();
        tokio::time::timeout(Duration::from_secs(1), hook.registered.notified())
            .await
            .expect("durable fence must register before the pump slot is released");
        tokio::time::timeout(Duration::from_secs(1), invalidation)
            .await
            .expect("invalidation must finish once the atomic admission section exits")
            .expect("invalidation task")
            .expect("old epoch invalidates");
        assert_eq!(
            adapter.durable_fence_count(),
            0,
            "invalidation must resolve and remove the registered fence"
        );

        online_tx.send_replace(false);
        epochs_tx.send_replace(None);
        hook.allow_delivery.notify_one();
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(1),
                dispatcher_client.admission_for(&personality_agent_id, 1)
            )
            .await
            .expect("old-epoch admission resolves after invalidation")
            .expect("old-epoch loss is reconnectable"),
            session::DurableEventAdmission::Deferred {
                after_epoch: Some(epoch)
            }
        );
        assert!(
            event_rx.try_recv().is_err(),
            "the invalidated pump must not emit stale N"
        );

        assert_eq!(
            EventWriter::new(store.clone())
                .apply(test_maintenance_batch("session-after-epoch-loss"))
                .await
                .unwrap(),
            vec![2]
        );
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(1),
                dispatcher_client.admission_for(&personality_agent_id, 2)
            )
            .await
            .expect("offline N+1 admission resolves")
            .expect("offline admission remains reconnectable"),
            session::DurableEventAdmission::Deferred {
                after_epoch: Some(epoch)
            }
        );
        let write = tokio::spawn(async move {
            writer
                .send(OutboundFrame::Event {
                    envelope: Envelope {
                        seq: Some(2),
                        personality_agent_id: personality_agent_id.clone(),
                        event: serde_json::json!({
                            "type": "memory_maintenance",
                            "kind": "untrusted-session-copy"
                        }),
                    },
                })
                .await?;
            writer
                .send(OutboundFrame::CommandAck {
                    ack: CommandAck {
                        seq: 1,
                        command_id: COMMAND_ID.to_owned(),
                        personality_agent_id,
                        status: CommandAckStatus::Applied,
                        reject_reason: None,
                    },
                })
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !write.is_finished(),
            "the deferred N/N+1 barrier must hold the terminal ACK offline"
        );

        let replacement_runtime = adapter
            .install_delivery_epoch(replacement, 0, events.clone(), pump_cancel.child_token())
            .await
            .unwrap()
            .expect("replacement T17 epoch installs");
        epochs_tx.send_replace(Some(replacement));
        let catch_up = adapter.events_after(0, 16).await.unwrap();
        assert_eq!(
            catch_up
                .iter()
                .map(outbound_frame_event_seq)
                .collect::<Result<Vec<_>>>()
                .unwrap(),
            vec![1, 2]
        );
        for frame in catch_up {
            events.send((replacement, frame)).await.unwrap();
        }
        let replay_n = event_rx.recv().await.unwrap();
        let replay_n_plus_one = event_rx.recv().await.unwrap();
        assert_eq!(outbound_frame_event_seq(&replay_n.2).unwrap(), 1);
        assert_eq!(outbound_frame_event_seq(&replay_n_plus_one.2).unwrap(), 2);
        assert_eq!(replay_n.0, replacement);
        assert_eq!(replay_n_plus_one.0, replacement);
        assert!(
            !write.is_finished() && event_rx.try_recv().is_err(),
            "replacement catch-up must finish before exactly one ACK is admitted"
        );
        adapter.mark_delivery_online(replacement).await.unwrap();
        online_tx.send_replace(true);
        let (ack_epoch, _, frame) = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("replacement Online releases the ACK")
            .expect("stable supervisor event lane remains open");
        assert_eq!(ack_epoch, replacement);
        assert!(matches!(frame, OutboundFrame::CommandAck { .. }));
        tokio::time::timeout(Duration::from_secs(1), write)
            .await
            .expect("Session writer must not wedge on an orphaned fence")
            .expect("Session writer task")
            .expect("durable callback and ACK complete");

        adapter.set_durable_admission_hook(None);
        let quiescence = close_test_post_commit_writer(&store, &dispatcher).await;
        dispatcher.shutdown(quiescence).await.unwrap();
        adapter
            .invalidate_delivery_epoch(replacement)
            .await
            .unwrap();
        pump_cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), runtime.join())
            .await
            .expect("invalidated forwarder must terminate")
            .expect("forwarder task joins");
        tokio::time::timeout(Duration::from_secs(1), replacement_runtime.join())
            .await
            .expect("replacement forwarder must terminate")
            .expect("replacement forwarder joins");
    }

    #[tokio::test]
    async fn emergency_post_commit_dispatcher_drop_resolves_never_drained_t17_admission() {
        let store = Arc::new(
            Store::session_test_store("t26-emergency-fence-cleanup")
                .await
                .unwrap(),
        );
        let adapter = seams::T17StoreAdapter::new(store.clone())
            .bind_delivery_authorization(DeliveryAuthorization::Raw)
            .unwrap();
        let hook = seams::DurableAdmissionHook::default();
        adapter.set_durable_admission_hook(Some(hook.clone()));
        let epoch = DeliveryEpoch::for_test("emergency-fence-cleanup");
        let (event_tx, mut event_rx) = mpsc::channel(1);
        let (_online_tx, online) = watch::channel(true);
        let events = EventSender {
            tx: event_tx,
            online,
        };
        let pump_cancel = CancellationToken::new();
        let runtime = adapter
            .install_delivery_epoch(epoch, 0, events.clone(), pump_cancel.child_token())
            .await
            .unwrap()
            .expect("delivery epoch installs");
        let dispatcher = post_commit::OrderedPostCommitDispatcher::start(
            store.clone(),
            adapter.clone(),
            0,
            CancellationToken::new(),
        )
        .unwrap();

        events
            .send((
                epoch,
                OutboundFrame::Event {
                    envelope: Envelope {
                        seq: None,
                        personality_agent_id: store.scope().personality_agent_id.clone(),
                        event: serde_json::json!({
                            "type": "error",
                            "message": "lane blocker"
                        }),
                    },
                },
            ))
            .await
            .unwrap();
        assert_eq!(
            EventWriter::new(store.clone())
                .apply(test_maintenance_batch("emergency-fence"))
                .await
                .unwrap(),
            vec![1]
        );
        tokio::time::timeout(Duration::from_secs(1), hook.reserved.notified())
            .await
            .expect("dispatcher reserves the delivery epoch");
        hook.allow_registration.notify_one();
        tokio::time::timeout(Duration::from_secs(1), hook.registered.notified())
            .await
            .expect("dispatcher owns a completion fence");
        assert_eq!(adapter.durable_fence_count(), 1);
        hook.allow_delivery.notify_one();
        tokio::time::timeout(Duration::from_secs(1), hook.enqueued.notified())
            .await
            .expect("durable frame reaches T17 while the external lane stays full");

        drop(dispatcher);
        tokio::time::timeout(Duration::from_secs(1), async {
            while adapter.durable_fence_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owner drop force-resolves the T17 fence");
        let blocker = event_rx
            .try_recv()
            .expect("the pre-existing external lane blocker remains queued");
        assert!(matches!(
            blocker.2,
            OutboundFrame::Event {
                envelope: Envelope { seq: None, .. }
            }
        ));
        assert!(
            event_rx.try_recv().is_err(),
            "the cancelled durable frame never reaches the external lane"
        );
        let receiver = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Ok(receiver) = store.claim_post_commit_receiver() {
                    break receiver;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dispatcher drop releases the exclusive receiver claim");
        drop(receiver);
        assert_eq!(
            EventWriter::new(store.clone())
                .apply(test_maintenance_batch("writer-after-emergency"))
                .await
                .unwrap(),
            vec![2],
            "dispatcher cancellation cannot strand EventWriter's gate"
        );

        adapter.set_durable_admission_hook(None);
        adapter.invalidate_delivery_epoch(epoch).await.unwrap();
        pump_cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), runtime.join())
            .await
            .expect("forwarder terminates")
            .expect("forwarder joins");
    }

    #[tokio::test]
    async fn store_projection_corruption_is_typed_fatal_without_reconnect() {
        let store = Arc::new(
            Store::session_test_store("t17-t24-forwarder-failure")
                .await
                .unwrap(),
        );
        let adapter = seams::T17StoreAdapter::new(store.clone());
        let sent_hellos = Arc::new(Mutex::new(Vec::new()));
        let responses = (0..5)
            .map(|_| Ok(MockGateway::new(VecDeque::new())))
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
        let post_commit_epoch = PostCommitEpochCapability::unbound_test(CancellationToken::new());
        let error = adapter
            .admit_ordered_commit(&post_commit_epoch, 1)
            .await
            .expect_err("invalid durable projection must fail synchronously");
        assert!(
            error
                .downcast_ref::<seams::DeliveryProjectionError>()
                .is_some(),
            "projection corruption must preserve its typed permanent boundary: {error:#}"
        );

        let supervisor_error = tokio::time::timeout(Duration::from_secs(1), handle.join())
            .await
            .expect("permanent delivery failure must terminate promptly")
            .expect_err("projection corruption must be fatal");
        assert!(
            format!("{supervisor_error:#}").contains("failed permanently"),
            "unexpected supervisor error: {supervisor_error:#}"
        );
        assert_eq!(
            sent_hellos.lock().unwrap().len(),
            1,
            "permanent projection corruption must not reconnect against the same row"
        );
        assert_eq!(
            adapter.active_delivery_epoch().await,
            None,
            "failed epoch must be invalidated idempotently after pump/forwarder teardown"
        );
    }

    #[tokio::test]
    async fn corrupt_redaction_backlog_is_fatal_before_online_without_reconnect() {
        let store = Arc::new(
            Store::session_test_store("t17-t24-corrupt-redaction-backlog")
                .await
                .unwrap(),
        );
        insert_test_durable_event(&store, 1, &crate::agent::AgentEvent::AgentStart)
            .await
            .unwrap();
        sqlx::query("UPDATE agent_events SET envelope = 'not-json' WHERE seq = 1")
            .execute(store.pool())
            .await
            .unwrap();

        let adapter = seams::T17StoreAdapter::new(store);
        let sent_hellos = Arc::new(Mutex::new(Vec::new()));
        let responses = (0..5)
            .map(|_| Ok(MockGateway::new(VecDeque::new())))
            .collect();
        let mut config = make_config();
        config.initial_backoff = Duration::ZERO;
        config.max_backoff = Duration::ZERO;
        let supervisor = ConnectionSupervisor::new(
            MockConnector::new(sent_hellos.clone(), responses),
            CountingCredentialProvider::new("token")
                .with_delivery_authorization(DeliveryAuthorization::RedactionOnly),
            adapter,
            StaticHydrationLatch(HydrationReady {
                generation: ProcessGeneration::from_wire(7).unwrap(),
                receipt_identity: "corrupt-backlog-ready".to_owned(),
            }),
            config,
        );

        let error = tokio::time::timeout(Duration::from_secs(1), supervisor.start().join())
            .await
            .expect("permanent backlog projection failure must terminate promptly")
            .expect_err("corrupt retained projection cannot be repaired by reconnect");
        assert!(
            format!("{error:#}").contains("redaction-only projection"),
            "typed projection failure must remain visible: {error:#}"
        );
        assert_eq!(
            sent_hellos.lock().unwrap().len(),
            1,
            "the same corrupt retained row must not be retried on a new connection"
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

    #[tokio::test]
    async fn disconnected_terminal_ack_is_recovered_by_command_replay() {
        const COMMAND_ID: &str = "00000000-0000-4000-8000-000000000001";

        let store = Store::session_test_store("terminal-ack-gap-replay")
            .await
            .unwrap();
        let pool = store.pool().clone();
        let adapter = seams::T17StoreAdapter::new(Arc::new(store.clone()));
        let sent_hellos = Arc::new(Mutex::new(Vec::new()));
        let first_sent = Arc::new(Mutex::new(Vec::new()));
        let second_sent = Arc::new(Mutex::new(Vec::new()));

        let mut gateway1 = MockGateway::new(VecDeque::from([Ok(valid_command(1, COMMAND_ID))]));
        gateway1.writer.sent = first_sent.clone();
        gateway1.writer.fail_after_record = Some(2);

        // Production API semantics use its durable ACK log. The first epoch's
        // terminal frame reached the wire but failed before ApplyAck, so the
        // API returns seq 1 despite the agent's second hello reporting local
        // applied=1.
        let mut gateway2 = MockGateway::new(VecDeque::from([Ok(valid_command(1, COMMAND_ID))]))
            .with_next_command_seq(1);
        gateway2.writer.sent = second_sent.clone();

        let connector = MockConnector::new(
            sent_hellos.clone(),
            VecDeque::from([Ok(gateway1), Ok(gateway2)]),
        );
        let config = make_config();
        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token"),
            adapter.clone(),
            StaticHydrationLatch(HydrationReady {
                generation: ProcessGeneration::from_wire(7).unwrap(),
                receipt_identity: "ack-replay".to_owned(),
            }),
            config,
        );
        let handle = supervisor.start();
        let worker_starts = Arc::new(AtomicU64::new(0));
        let worker: Arc<dyn RunWorker> = Arc::new({
            let worker_starts = worker_starts.clone();
            move |core: RunCore,
                  _initial: AdmittedCommand,
                  _controls: mpsc::Receiver<RunControl>,
                  _events: mpsc::Sender<AgentEvent>| {
                worker_starts.fetch_add(1, Ordering::SeqCst);
                async move { RunCompletion::Completed(core) }
            }
        });
        let session = Session::start(
            store,
            session::SessionGateway::from(handle),
            RunCore::fixture_with_unapproved_tools(),
            worker,
            ProcessGeneration::from_wire(7).unwrap(),
        )
        .await
        .unwrap();
        let session_task = tokio::spawn(session.run());

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if second_sent.lock().unwrap().iter().any(|frame| {
                    matches!(
                        frame,
                        OutboundFrame::CommandAck { ack }
                            if ack.seq == 1 && ack.status == CommandAckStatus::Applied
                    )
                }) {
                    break;
                }
                assert!(
                    !session_task.is_finished(),
                    "the continuing Session must survive ACK-gap recovery"
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("stored terminal ACK must be resent on duplicate replay");

        {
            let hellos = sent_hellos.lock().unwrap();
            assert!(hellos.len() >= 2);
            assert_eq!(
                hellos[1].last_applied_command_seq, 1,
                "the API replay decision must override, not falsify, the local applied cursor"
            );
        }
        assert!(matches!(
            first_sent.lock().unwrap().as_slice(),
            [
                OutboundFrame::CommandAck { ack: received },
                OutboundFrame::CommandAck { ack: applied },
            ] if received.status == CommandAckStatus::Received
                && applied.status == CommandAckStatus::Applied
        ));
        assert_eq!(
            worker_starts.load(Ordering::SeqCst),
            0,
            "duplicate recovery must not start provider or tool work"
        );
        let command_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inbound_commands")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            command_rows, 1,
            "duplicate replay must not duplicate durable work"
        );

        session_task.abort();
        assert!(session_task.await.unwrap_err().is_cancelled());
        wait_for_t17_idle(&adapter).await;
    }

    #[tokio::test]
    async fn active_session_run_survives_supervisor_reconnect() {
        const COMMAND_ID: &str = "00000000-0000-4000-8000-000000000001";

        let store = Store::session_test_store("active-session-reconnect")
            .await
            .unwrap();
        let store_arc = Arc::new(store.clone());
        let base_adapter = seams::T17StoreAdapter::new(store_arc.clone());
        let (adapter, dispatcher) =
            bind_test_post_commit_dispatcher(store_arc.clone(), &base_adapter, 0);
        let sent_hellos = Arc::new(Mutex::new(Vec::new()));
        let first_sent = Arc::new(Mutex::new(Vec::new()));
        let second_sent = Arc::new(Mutex::new(Vec::new()));

        let user_command = InboundCommand::Valid(CommandEnvelope {
            personality_agent_id: crate::gateway::test_personality_agent_id(),
            provenance: crate::gateway::test_direct_chat_provenance(),
            seq: 1,
            command_id: CommandId::parse(COMMAND_ID).unwrap(),
            command: Command::UserMessage {
                text: "continue through reconnect".to_owned(),
                attachments: Vec::new(),
            },
        });
        let mut gateway1 = MockGateway::new(VecDeque::from([Ok(user_command)]));
        gateway1.writer.sent = first_sent.clone();
        gateway1.writer.fail_after = Some(1);

        let mut gateway2 = MockGateway::new(VecDeque::new());
        gateway2.writer.sent = second_sent.clone();

        let connector = MockConnector::new(
            sent_hellos.clone(),
            VecDeque::from([Ok(gateway1), Ok(gateway2)]),
        );
        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token"),
            adapter.clone(),
            StaticHydrationLatch(HydrationReady {
                generation: ProcessGeneration::from_wire(7).unwrap(),
                receipt_identity: "active-run-reconnect".to_owned(),
            }),
            make_config(),
        );
        let handle = supervisor.start();
        let mut online = handle.online.clone();
        let mut epochs = handle.epochs.clone();
        let gateway = session::SessionGateway::from(handle);

        let starts = Arc::new(AtomicU64::new(0));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let worker: Arc<dyn RunWorker> = Arc::new({
            let starts = starts.clone();
            let started = started.clone();
            let release = release.clone();
            move |core: RunCore,
                  initial: AdmittedCommand,
                  controls: mpsc::Receiver<RunControl>,
                  events: mpsc::Sender<AgentEvent>| {
                starts.fetch_add(1, Ordering::SeqCst);
                started.notify_one();
                let release = release.clone();
                async move {
                    release.notified().await;
                    let Command::UserMessage { text, .. } = &initial.envelope().command else {
                        panic!("active reconnect fixture requires a user message");
                    };
                    let user = PublicMessage::User(UserMessage {
                        content: vec![UserContent::Text { text: text.clone() }],
                        timestamp: initial.received_at(),
                    });
                    let message_id = user_message_id(
                        &initial.envelope().personality_agent_id,
                        &initial.envelope().command_id,
                    );
                    for event in [
                        AgentEvent::AgentStart,
                        AgentEvent::TurnStart,
                        AgentEvent::MessageStart {
                            message_id: message_id.clone(),
                            message: Box::new(user.clone()),
                        },
                        AgentEvent::MessageEnd {
                            message_id,
                            message: Box::new(user),
                        },
                    ] {
                        events
                            .send(event)
                            .await
                            .expect("continuing Session event lane");
                    }
                    let _ownership = (core, controls);
                    std::future::pending::<RunCompletion>().await
                }
            }
        });
        let session = Session::start(
            store,
            gateway,
            RunCore::fixture_with_unapproved_tools(),
            worker,
            ProcessGeneration::from_wire(7).unwrap(),
        )
        .await
        .unwrap();
        let session_task = tokio::spawn(session.run());

        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("Session must start the run on the first epoch");
        tokio::time::timeout(Duration::from_secs(1), async {
            while !*online.borrow() {
                online.changed().await.unwrap();
            }
        })
        .await
        .expect("first epoch must be Online before the active run triggers failure");
        let first_epoch = (*epochs.borrow()).expect("first delivery epoch installed");

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if sent_hellos.lock().unwrap().len() >= 2
                    && epochs.borrow().is_some_and(|epoch| epoch != first_epoch)
                    && *online.borrow()
                {
                    break;
                }
                tokio::select! {
                    _ = epochs.changed() => {},
                    _ = online.changed() => {},
                    _ = tokio::task::yield_now() => {},
                }
            }
        })
        .await
        .expect("the active run's first durable event must replace the failed T24 epoch");
        assert!(
            !session_task.is_finished(),
            "the stable Session channels must keep the active worker alive across reconnect"
        );
        assert_eq!(
            starts.load(Ordering::SeqCst),
            1,
            "a reconnect must not restart the active worker"
        );
        assert!(
            matches!(
                first_sent.lock().unwrap().as_slice(),
                [OutboundFrame::CommandAck { ack }]
                    if ack.status == CommandAckStatus::Received
            ),
            "the first writer must send Received ACK, then fail on the active run's event"
        );

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if second_sent.lock().unwrap().iter().any(|frame| {
                    matches!(
                        frame,
                        OutboundFrame::Event { envelope }
                            if envelope.event.get("type").and_then(serde_json::Value::as_str)
                                == Some("agent_start")
                    )
                }) {
                    break;
                }
                assert!(
                    !session_task.is_finished(),
                    "the continuing run must not fail while committing through T17"
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replacement epoch must replay the active worker's committed startup");

        session_task.abort();
        let error = session_task
            .await
            .expect_err("test cleanup aborts the otherwise idle Session");
        assert!(error.is_cancelled());
        wait_for_t17_idle(&adapter).await;
        let quiescence = close_test_post_commit_writer(&store_arc, &dispatcher).await;
        dispatcher.shutdown(quiescence).await.unwrap();
    }

    #[tokio::test]
    async fn active_session_survives_over_64_commits_with_blocked_installed_pump() {
        const COMMAND_ID: &str = "00000000-0000-4000-8000-000000000001";
        const TOOL_COUNT: usize = 40;

        let store = Store::session_test_store("active-session-blocked-pump")
            .await
            .unwrap();
        let pool = store.pool().clone();
        let store_arc = Arc::new(store.clone());
        let base_adapter = seams::T17StoreAdapter::new(store_arc.clone());
        let (adapter, dispatcher) =
            bind_test_post_commit_dispatcher(store_arc.clone(), &base_adapter, 0);
        let sent = Arc::new(Mutex::new(Vec::new()));
        let writer_blocked = Arc::new(Notify::new());
        let writer_release = Arc::new(Notify::new());
        let command = InboundCommand::Valid(CommandEnvelope {
            personality_agent_id: crate::gateway::test_personality_agent_id(),
            provenance: crate::gateway::test_direct_chat_provenance(),
            seq: 1,
            command_id: CommandId::parse(COMMAND_ID).unwrap(),
            command: Command::UserMessage {
                text: "keep the run alive under bounded delivery backpressure".to_owned(),
                attachments: Vec::new(),
            },
        });
        let mut gateway = MockGateway::new(VecDeque::from([Ok(command)]));
        gateway.writer.sent = sent.clone();
        gateway.writer = gateway.writer.with_block_after(2, writer_blocked.clone());
        gateway.writer.release = Some(writer_release.clone());

        let connector = MockConnector::new(
            Arc::new(Mutex::new(Vec::new())),
            VecDeque::from([Ok(gateway)]),
        );
        let mut config = make_config();
        config.send_timeout = Duration::from_secs(60);
        let supervisor = ConnectionSupervisor::new(
            connector,
            CountingCredentialProvider::new("token"),
            adapter.clone(),
            StaticHydrationLatch(HydrationReady {
                generation: ProcessGeneration::from_wire(7).unwrap(),
                receipt_identity: "blocked-pump-ready".to_owned(),
            }),
            config,
        );
        let handle = supervisor.start();
        let worker_started = Arc::new(Notify::new());
        let worker: Arc<dyn RunWorker> = Arc::new({
            let worker_started = worker_started.clone();
            move |_core: RunCore,
                  initial: AdmittedCommand,
                  _controls: mpsc::Receiver<RunControl>,
                  events: mpsc::Sender<AgentEvent>| {
                worker_started.notify_one();
                async move {
                    let Command::UserMessage { text, .. } = &initial.envelope().command else {
                        panic!("fixture requires user message");
                    };
                    let user = PublicMessage::User(UserMessage {
                        content: vec![UserContent::Text { text: text.clone() }],
                        timestamp: initial.received_at(),
                    });
                    let message_id = user_message_id(
                        &initial.envelope().personality_agent_id,
                        &initial.envelope().command_id,
                    );
                    for event in [
                        AgentEvent::AgentStart,
                        AgentEvent::TurnStart,
                        AgentEvent::MessageStart {
                            message_id: message_id.clone(),
                            message: Box::new(user.clone()),
                        },
                        AgentEvent::MessageEnd {
                            message_id,
                            message: Box::new(user),
                        },
                    ] {
                        events.send(event).await.expect("initial durable event");
                    }
                    let assistant = PublicMessage::Assistant(PublicAssistantMessage {
                        content: (0..TOOL_COUNT)
                            .map(|index| PublicAssistantContent::ToolCall {
                                tool_call: ToolCall {
                                    id: format!("blocked-pump-tool-{index}"),
                                    name: "blocked-pump-tool".to_owned(),
                                    arguments: serde_json::from_value::<ValidatedToolArguments>(
                                        serde_json::json!({"index": index}),
                                    )
                                    .expect("object-shaped tool arguments"),
                                },
                                wire_item_index: u32::try_from(index).unwrap(),
                            })
                            .collect(),
                        model: "blocked-pump-model".to_owned(),
                        provider: "blocked-pump-provider".to_owned(),
                        origin: ProviderOrigin {
                            provider_instance_id: "blocked-pump-instance".to_owned(),
                            protocol: ApiProtocol::OpenAiResponses,
                            model: "blocked-pump-model".to_owned(),
                        },
                        usage: Usage::default(),
                        stop_reason: StopReason::ToolUse,
                        error_message: None,
                        provider_code: None,
                        interrupted: false,
                        timestamp: chrono::Utc::now(),
                    });
                    for event in [
                        AgentEvent::MessageStart {
                            message_id: "blocked-pump-assistant".to_owned(),
                            message: Box::new(assistant.clone()),
                        },
                        AgentEvent::MessageEnd {
                            message_id: "blocked-pump-assistant".to_owned(),
                            message: Box::new(assistant),
                        },
                    ] {
                        events
                            .send(event)
                            .await
                            .expect("assistant tool prerequisite");
                    }
                    for index in 0..TOOL_COUNT {
                        let tool_call_id = format!("blocked-pump-tool-{index}");
                        let result = ToolResultMessage {
                            tool_call_id: tool_call_id.clone(),
                            tool_name: "blocked-pump-tool".to_owned(),
                            content: vec![UserContent::Text {
                                text: format!("tool result {index}"),
                            }],
                            details: serde_json::json!({"index": index, "ok": true}),
                            is_error: false,
                            timestamp: chrono::Utc::now(),
                        };
                        for event in [
                            AgentEvent::ToolExecutionStart {
                                tool_call_id: tool_call_id.clone(),
                                tool_name: "blocked-pump-tool".to_owned(),
                                args: serde_json::json!({"index": index}),
                            },
                            AgentEvent::ToolExecutionEnd {
                                tool_call_id: tool_call_id.clone(),
                                result: serde_json::to_value(&result).unwrap(),
                                is_error: false,
                            },
                            AgentEvent::MessageStart {
                                message_id: format!("blocked-pump-result-{index}"),
                                message: Box::new(PublicMessage::ToolResult(result.clone())),
                            },
                            AgentEvent::MessageEnd {
                                message_id: format!("blocked-pump-result-{index}"),
                                message: Box::new(PublicMessage::ToolResult(result)),
                            },
                        ] {
                            events.send(event).await.expect("bounded worker event lane");
                        }
                    }
                    std::future::pending::<RunCompletion>().await
                }
            }
        });
        let session = Session::start(
            store,
            session::SessionGateway::from(handle),
            RunCore::fixture_with_unapproved_tools(),
            worker,
            ProcessGeneration::from_wire(7).unwrap(),
        )
        .await
        .unwrap();
        let mut session_task = tokio::spawn(session.run());

        tokio::time::timeout(Duration::from_secs(10), worker_started.notified())
            .await
            .expect("worker must start");
        let blocked =
            tokio::time::timeout(Duration::from_secs(10), writer_blocked.notified()).await;
        if session_task.is_finished() {
            let result = (&mut session_task).await.expect("session join");
            panic!("Session failed before blocked delivery: {result:?}");
        }
        assert!(
            blocked.is_ok(),
            "installed pump must reach the blocked transport; frames={:?}, session_finished={}",
            *sent.lock().unwrap(),
            session_task.is_finished()
        );
        assert!(
            adapter.active_delivery_epoch().await.is_some(),
            "the test must exercise an installed T17 DeliveryPump"
        );

        let expected = i64::try_from(6 + (4 * TOOL_COUNT)).unwrap();
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let committed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                if committed >= expected {
                    break;
                }
                if session_task.is_finished() {
                    let result = (&mut session_task).await.expect("session join");
                    panic!("bounded downstream pressure terminated the active Session: {result:?}");
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("more than 64 durable commits must remain live under blocked delivery");
        assert!(
            !session_task.is_finished(),
            "the one active run must survive while delivery remains backpressured"
        );
        assert_eq!(
            sent.lock().unwrap().len(),
            2,
            "the blocked transport must not be bypassed by Session"
        );

        writer_release.notify_one();
        session_task.abort();
        assert!(session_task.await.unwrap_err().is_cancelled());
        wait_for_t17_idle(&adapter).await;
        let quiescence = close_test_post_commit_writer(&store_arc, &dispatcher).await;
        dispatcher.shutdown(quiescence).await.unwrap();
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
                personality_agent_id: hello.personality_agent_id.clone(),
                accepted_generation: hello.generation,
                last_received_event_seq: 0,
                next_command_seq: hello.last_applied_command_seq.saturating_add(1),
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
