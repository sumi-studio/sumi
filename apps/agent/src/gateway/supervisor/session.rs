//! Adapter from an authenticated `ConnectionSupervisor` to the Session gateway boundary.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::{mpsc, watch};

#[cfg(test)]
use super::SupervisorLifecycle;
use super::{DeliveryEpoch, EventSender, SupervisorHandle, SupervisorRuntime};
use crate::agent::AgentEvent;
use crate::gateway::{
    AgentHello, ApiHello, Gateway, GatewayClosed, GatewayReader, GatewayWriter, HelloError,
    InboundCommand, OutboundFrame,
};
use crate::runtime::contracts::PersonalityAgentId;

#[async_trait]
pub(crate) trait SessionEventDelivery: Send + Sync + 'static {
    async fn on_durable_committed(
        &self,
        personality_agent_id: &PersonalityAgentId,
        seq: u64,
    ) -> Result<DurableEventAdmission>;
    async fn on_volatile(
        &self,
        personality_agent_id: &PersonalityAgentId,
        event: AgentEvent,
    ) -> Result<()>;
}

/// Result of passing one committed durable sequence through T17.
///
/// `Enqueued` proves that the projected frame entered T24's ordered lane in
/// the named epoch. `Deferred` means no live delivery path existed, or that
/// its transport failed; the durable row remains canonical and the next epoch
/// must catch it up before a following command ACK may be admitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DurableEventAdmission {
    Enqueued { epoch: DeliveryEpoch },
    Deferred { after_epoch: Option<DeliveryEpoch> },
}

/// Opaque T26/T17 delivery capability transferred from a durable source to
/// Session.
///
/// Durable sequences await the one post-commit dispatcher's cumulative
/// admission proof; volatile events are offered directly to T17's online
/// delivery gate. Session never receives a `DeliveryEpoch` and cannot send an
/// event frame directly to T24.
#[derive(Clone)]
pub struct SessionEventSink {
    delivery: Arc<dyn SessionEventDelivery>,
}

impl SessionEventSink {
    pub(crate) fn new(delivery: impl SessionEventDelivery) -> Self {
        Self {
            delivery: Arc::new(delivery),
        }
    }

    async fn on_durable_committed(
        &self,
        personality_agent_id: &PersonalityAgentId,
        seq: u64,
    ) -> Result<DurableEventAdmission> {
        self.delivery
            .on_durable_committed(personality_agent_id, seq)
            .await
    }

    async fn on_volatile(
        &self,
        personality_agent_id: &PersonalityAgentId,
        event: AgentEvent,
    ) -> Result<()> {
        self.delivery.on_volatile(personality_agent_id, event).await
    }
}

/// Transfers a running supervisor's stable command channel and delivery
/// capabilities into `Session::start`.
///
/// The supervisor remains the sole owner of connection epochs, delivery epoch
/// creation/invalidation, stale-frame rejection, and the authenticated T17
/// delivery mode. Session events are exhaustively separated from ACKs: durable
/// events await T26's one ordered post-commit proof, volatile events enter
/// T17's Online+Raw gate, and only ACKs may use T24's direct path.
pub struct SessionGateway {
    commands: mpsc::Receiver<InboundCommand>,
    ack_events: EventSender,
    epochs: watch::Receiver<Option<DeliveryEpoch>>,
    online: watch::Receiver<bool>,
    session_events: Option<SessionEventSink>,
    // Production construction has no lifecycle field: bootstrap retains the
    // SupervisorRuntime. Channel-only tests may keep an already-settled
    // lifecycle here; live-task fixtures extract and join it explicitly.
    #[cfg(test)]
    lifecycle: Option<SupervisorLifecycle>,
}

impl SessionGateway {
    /// Transfer only stable gateway channels into Session. Bootstrap retains
    /// the returned runtime owner so supervisor termination is monitored and
    /// every teardown explicitly cancels and joins the task.
    pub(crate) fn from_supervisor(handle: SupervisorHandle) -> (Self, SupervisorRuntime) {
        let SupervisorHandle {
            commands,
            events,
            epochs,
            online,
            session_events,
            lifecycle,
        } = handle;
        (
            Self {
                commands,
                ack_events: events,
                epochs,
                online,
                session_events,
                #[cfg(test)]
                lifecycle: None,
            },
            SupervisorRuntime::new(lifecycle),
        )
    }
}

#[cfg(test)]
impl From<SupervisorHandle> for SessionGateway {
    fn from(handle: SupervisorHandle) -> Self {
        let SupervisorHandle {
            commands,
            events,
            epochs,
            online,
            session_events,
            lifecycle,
        } = handle;
        Self {
            commands,
            ack_events: events,
            epochs,
            online,
            session_events,
            lifecycle: Some(lifecycle),
        }
    }
}

#[derive(Debug, Error)]
pub enum SessionGatewayError {
    #[error("connection supervisor did not provide a T17 Session event sink")]
    MissingEventSink,
    #[error("durable T17 delivery failed for event seq {seq}: {source}")]
    DurableEvent {
        seq: u64,
        #[source]
        source: anyhow::Error,
    },
    #[error("volatile Session event is not a valid AgentEvent: {source}")]
    InvalidVolatileEvent {
        #[source]
        source: serde_json::Error,
    },
    #[error("event kind `{kind}` is durable but Session supplied no sequence")]
    DurableEventWithoutSequence { kind: &'static str },
    #[error("volatile T17 delivery failed: {source}")]
    VolatileEvent {
        #[source]
        source: anyhow::Error,
    },
}

pub struct SessionGatewayReader(mpsc::Receiver<InboundCommand>);

pub struct SessionGatewayWriter {
    ack_events: EventSender,
    epochs: watch::Receiver<Option<DeliveryEpoch>>,
    online: watch::Receiver<bool>,
    session_events: Option<SessionEventSink>,
    ack_barrier: Option<DurableEventAdmission>,
    #[cfg(test)]
    _lifecycle: Option<SupervisorLifecycle>,
}

impl SessionGatewayWriter {
    fn epoch_satisfies_barrier(&self, epoch: DeliveryEpoch) -> bool {
        match self.ack_barrier {
            None => true,
            Some(DurableEventAdmission::Enqueued { epoch: event_epoch }) => {
                epoch == event_epoch || *self.online.borrow()
            }
            Some(DurableEventAdmission::Deferred { after_epoch }) => {
                *self.online.borrow() && after_epoch != Some(epoch)
            }
        }
    }

    async fn wait_for_ack_epoch(&mut self) -> Result<DeliveryEpoch> {
        loop {
            if let Some(epoch) = *self.epochs.borrow()
                && self.epoch_satisfies_barrier(epoch)
            {
                return Ok(epoch);
            }
            tokio::select! {
                result = self.epochs.changed() => {
                    result.map_err(|_| anyhow!("supervisor epoch watch closed before ACK recovery"))?;
                }
                result = self.online.changed() => {
                    result.map_err(|_| anyhow!("supervisor Online watch closed before ACK recovery"))?;
                }
            }
        }
    }

    async fn send_command_ack(
        &mut self,
        ack: crate::gateway::CommandAck,
        clear_barrier: bool,
    ) -> Result<()> {
        loop {
            let epoch = self.wait_for_ack_epoch().await?;
            let admitted = self
                .ack_events
                .send_command_ack_if_current(epoch, ack.clone(), &self.epochs)
                .await
                .map_err(|_| anyhow!("supervisor ACK lane closed before durable ACK recovery"))?;
            if admitted {
                if clear_barrier {
                    self.ack_barrier = None;
                }
                return Ok(());
            }
            // Capacity became available only after epoch replacement. No
            // stale tag was enqueued; retry under the next eligible epoch.
        }
    }
}

#[async_trait]
impl GatewayReader for SessionGatewayReader {
    async fn next_command(&mut self) -> Result<InboundCommand> {
        self.0.recv().await.ok_or_else(|| GatewayClosed.into())
    }
}

#[async_trait]
impl GatewayWriter for SessionGatewayWriter {
    async fn send(&mut self, frame: OutboundFrame) -> Result<()> {
        match frame {
            OutboundFrame::CommandAck { ack } => self.send_command_ack(ack, true).await,
            OutboundFrame::Event { envelope } => {
                let session_events = self
                    .session_events
                    .as_ref()
                    .ok_or(SessionGatewayError::MissingEventSink)?;
                if let Some(seq) = envelope.seq {
                    let epoch_at_commit = *self.epochs.borrow();
                    // Session's one writer awaits T17 admission here, so a
                    // later terminal ACK in the same committed batch cannot
                    // overtake this durable sequence.
                    let admission = session_events
                        .on_durable_committed(&envelope.personality_agent_id, seq)
                        .await
                        .map_err(|source| {
                            anyhow!(SessionGatewayError::DurableEvent { seq, source })
                        })?;
                    self.ack_barrier = Some(match admission {
                        DurableEventAdmission::Deferred { after_epoch: None } => {
                            DurableEventAdmission::Deferred {
                                after_epoch: epoch_at_commit,
                            }
                        }
                        admission => admission,
                    });
                    return Ok(());
                }
                let event =
                    serde_json::from_value::<AgentEvent>(envelope.event).map_err(|source| {
                        anyhow!(SessionGatewayError::InvalidVolatileEvent { source })
                    })?;
                if let Some(kind) = event.durable_kind() {
                    return Err(anyhow!(SessionGatewayError::DurableEventWithoutSequence {
                        kind
                    }));
                }
                session_events
                    .on_volatile(&envelope.personality_agent_id, event)
                    .await
                    .map_err(|source| anyhow!(SessionGatewayError::VolatileEvent { source }))
            }
        }
    }

    async fn send_batch(&mut self, frames: Vec<OutboundFrame>) -> Result<()> {
        let mut ack_after_latest_durable = false;
        for frame in frames {
            match frame {
                OutboundFrame::CommandAck { ack } => {
                    // Keep the current durable admission proof until every
                    // terminal ACK in this committed group has crossed it.
                    self.send_command_ack(ack, false).await?;
                    ack_after_latest_durable = true;
                }
                frame => {
                    if matches!(
                        &frame,
                        OutboundFrame::Event { envelope } if envelope.seq.is_some()
                    ) {
                        ack_after_latest_durable = false;
                    }
                    self.send(frame).await?;
                }
            }
        }
        if ack_after_latest_durable {
            self.ack_barrier = None;
        }
        Ok(())
    }
}

#[async_trait]
impl Gateway for SessionGateway {
    type Reader = SessionGatewayReader;
    type Writer = SessionGatewayWriter;

    async fn authenticate_hello(
        &mut self,
        _hello: AgentHello,
    ) -> std::result::Result<ApiHello, HelloError> {
        Err(HelloError::Fatal(anyhow!(
            "SessionGateway cannot authenticate hello after ConnectionSupervisor handoff"
        )))
    }

    fn split(self) -> (Self::Reader, Self::Writer) {
        (
            SessionGatewayReader(self.commands),
            SessionGatewayWriter {
                ack_events: self.ack_events,
                epochs: self.epochs,
                online: self.online,
                session_events: self.session_events,
                ack_barrier: None,
                #[cfg(test)]
                _lifecycle: self.lifecycle,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::sync::{Notify, mpsc, oneshot};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::gateway::{
        Command, CommandAck, CommandAckStatus, CommandEnvelope, CommandId, Envelope,
    };
    use crate::runtime::contracts::ProcessGeneration;

    type GatewayFixture = (
        SessionGateway,
        mpsc::Sender<InboundCommand>,
        mpsc::Receiver<(DeliveryEpoch, bool, OutboundFrame)>,
        watch::Sender<Option<DeliveryEpoch>>,
        watch::Sender<bool>,
        RecordingDelivery,
    );

    #[derive(Clone, Default)]
    struct RecordingDelivery {
        durable: Arc<Mutex<Vec<(PersonalityAgentId, u64)>>>,
        volatile: Arc<Mutex<Vec<(PersonalityAgentId, AgentEvent)>>>,
    }

    #[async_trait]
    impl SessionEventDelivery for RecordingDelivery {
        async fn on_durable_committed(
            &self,
            personality_agent_id: &PersonalityAgentId,
            seq: u64,
        ) -> Result<DurableEventAdmission> {
            self.durable
                .lock()
                .unwrap()
                .push((personality_agent_id.clone(), seq));
            Ok(DurableEventAdmission::Enqueued {
                epoch: DeliveryEpoch::for_test("session-gateway-flow"),
            })
        }

        async fn on_volatile(
            &self,
            personality_agent_id: &PersonalityAgentId,
            event: AgentEvent,
        ) -> Result<()> {
            self.volatile
                .lock()
                .unwrap()
                .push((personality_agent_id.clone(), event));
            Ok(())
        }
    }

    #[derive(Clone)]
    struct FailingDelivery;

    #[async_trait]
    impl SessionEventDelivery for FailingDelivery {
        async fn on_durable_committed(
            &self,
            _personality_agent_id: &PersonalityAgentId,
            _seq: u64,
        ) -> Result<DurableEventAdmission> {
            anyhow::bail!("store corruption")
        }

        async fn on_volatile(
            &self,
            _personality_agent_id: &PersonalityAgentId,
            _event: AgentEvent,
        ) -> Result<()> {
            anyhow::bail!("authorization corruption")
        }
    }

    #[derive(Clone)]
    struct DeferredDelivery;

    #[async_trait]
    impl SessionEventDelivery for DeferredDelivery {
        async fn on_durable_committed(
            &self,
            _personality_agent_id: &PersonalityAgentId,
            _seq: u64,
        ) -> Result<DurableEventAdmission> {
            Ok(DurableEventAdmission::Deferred { after_epoch: None })
        }

        async fn on_volatile(
            &self,
            _personality_agent_id: &PersonalityAgentId,
            _event: AgentEvent,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct BlockingDelivery {
        epoch: DeliveryEpoch,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[async_trait]
    impl SessionEventDelivery for BlockingDelivery {
        async fn on_durable_committed(
            &self,
            _personality_agent_id: &PersonalityAgentId,
            _seq: u64,
        ) -> Result<DurableEventAdmission> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(DurableEventAdmission::Enqueued { epoch: self.epoch })
        }

        async fn on_volatile(
            &self,
            _personality_agent_id: &PersonalityAgentId,
            _event: AgentEvent,
        ) -> Result<()> {
            Ok(())
        }
    }

    fn command(seq: u64, command_id: &str) -> InboundCommand {
        InboundCommand::Valid(CommandEnvelope {
            personality_agent_id: crate::gateway::test_personality_agent_id(),
            provenance: crate::gateway::test_direct_chat_provenance(),
            seq,
            command_id: CommandId::parse(command_id).expect("canonical command id"),
            command: Command::Abort {},
        })
    }

    fn ack(seq: u64, command_id: &str) -> OutboundFrame {
        OutboundFrame::CommandAck {
            ack: CommandAck {
                seq,
                command_id: command_id.to_owned(),
                personality_agent_id: crate::gateway::test_personality_agent_id(),
                status: CommandAckStatus::Received,
                reject_reason: None,
            },
        }
    }

    fn output(seq: u64) -> OutboundFrame {
        OutboundFrame::Event {
            envelope: Envelope {
                seq: Some(seq),
                personality_agent_id: crate::gateway::test_personality_agent_id(),
                event: serde_json::json!({"type": "turn_start"}),
            },
        }
    }

    fn volatile_output(message: &str) -> OutboundFrame {
        OutboundFrame::Event {
            envelope: Envelope {
                seq: None,
                personality_agent_id: crate::gateway::test_personality_agent_id(),
                event: serde_json::json!({"type": "error", "message": message}),
            },
        }
    }

    fn make_gateway(
        command_capacity: usize,
        event_capacity: usize,
        epoch: Option<DeliveryEpoch>,
    ) -> GatewayFixture {
        let (command_tx, commands) = mpsc::channel(command_capacity);
        let (event_tx, event_rx) = mpsc::channel(event_capacity);
        let (online_tx, online) = watch::channel(true);
        let (epochs_tx, epochs) = watch::channel(epoch);
        let delivery = RecordingDelivery::default();
        let handle = SupervisorHandle {
            commands,
            events: EventSender {
                tx: event_tx,
                online: online.clone(),
            },
            epochs,
            online,
            session_events: Some(SessionEventSink::new(delivery.clone())),
            lifecycle: SupervisorLifecycle {
                cancel: CancellationToken::new(),
                task: None,
            },
        };
        (
            SessionGateway::from(handle),
            command_tx,
            event_rx,
            epochs_tx,
            online_tx,
            delivery,
        )
    }

    #[tokio::test]
    async fn preserves_command_ack_order_and_routes_events_only_through_t17() {
        const FIRST_ID: &str = "00000000-0000-4000-8000-000000000007";
        const SECOND_ID: &str = "00000000-0000-4000-8000-000000000008";
        let epoch = DeliveryEpoch::for_test("session-gateway-flow");
        let (gateway, command_tx, mut event_rx, _epochs_tx, _online_tx, delivery) =
            make_gateway(2, 3, Some(epoch));
        let (mut reader, mut writer) = gateway.split();

        command_tx
            .send(command(7, FIRST_ID))
            .await
            .expect("command receiver open");
        command_tx
            .send(command(8, SECOND_ID))
            .await
            .expect("command receiver open");
        assert_eq!(
            reader.next_command().await.expect("first command"),
            command(7, FIRST_ID)
        );
        assert_eq!(
            reader.next_command().await.expect("second command"),
            command(8, SECOND_ID)
        );

        writer
            .send(ack(7, FIRST_ID))
            .await
            .expect("first ACK admitted");
        writer.send(output(19)).await.expect("event admitted");
        writer
            .send(volatile_output("sk-secret-delta"))
            .await
            .expect("volatile event admitted");
        writer
            .send(ack(8, SECOND_ID))
            .await
            .expect("second ACK admitted");

        let (first_epoch, first_online, first_frame) = event_rx.recv().await.expect("first frame");
        let (second_epoch, second_online, second_frame) =
            event_rx.recv().await.expect("second frame");
        assert_eq!([first_epoch, second_epoch], [epoch; 2]);
        assert!(first_online && second_online);
        assert_eq!(
            [first_frame, second_frame],
            [ack(7, FIRST_ID), ack(8, SECOND_ID)]
        );
        assert_eq!(
            *delivery.durable.lock().unwrap(),
            vec![(crate::gateway::test_personality_agent_id(), 19)]
        );
        assert_eq!(
            *delivery.volatile.lock().unwrap(),
            vec![(
                crate::gateway::test_personality_agent_id(),
                AgentEvent::Error {
                    message: "sk-secret-delta".to_owned()
                }
            )]
        );
        assert!(
            event_rx.try_recv().is_err(),
            "Session event frames must never enter T24's direct event channel"
        );
    }

    #[tokio::test]
    async fn durable_projection_and_enqueue_complete_before_a_later_ack_is_admitted() {
        const COMMAND_ID: &str = "00000000-0000-4000-8000-000000000007";
        let epoch = DeliveryEpoch::for_test("session-gateway-projection-fence");
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (mut gateway, _command_tx, mut event_rx, _epochs_tx, _online_tx, _delivery) =
            make_gateway(1, 1, Some(epoch));
        gateway.session_events = Some(SessionEventSink::new(BlockingDelivery {
            epoch,
            entered: entered.clone(),
            release: release.clone(),
        }));
        let (_reader, mut writer) = gateway.split();

        let write = tokio::spawn(async move {
            writer.send(output(19)).await?;
            writer.send(ack(7, COMMAND_ID)).await
        });
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("durable projection must begin");
        assert!(
            event_rx.try_recv().is_err(),
            "a later ACK must not enter T24 while projection/admission is pending"
        );

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), write)
            .await
            .expect("projection completion must release the writer")
            .expect("writer task")
            .expect("event then ACK succeeds");
        let (seen_epoch, _, seen_frame) = event_rx.recv().await.expect("ACK admitted after fence");
        assert_eq!(seen_epoch, epoch);
        assert_eq!(seen_frame, ack(7, COMMAND_ID));
    }

    #[tokio::test]
    async fn epochless_deferred_event_cannot_release_ack_on_an_already_online_epoch() {
        const COMMAND_ID: &str = "00000000-0000-4000-8000-000000000007";
        let first = DeliveryEpoch::for_test("session-gateway-deferred-current");
        let second = DeliveryEpoch::for_test("session-gateway-deferred-replacement");
        let (mut gateway, _command_tx, mut event_rx, epochs_tx, online_tx, _delivery) =
            make_gateway(1, 1, Some(first));
        gateway.session_events = Some(SessionEventSink::new(DeferredDelivery));
        let (_reader, mut writer) = gateway.split();

        writer
            .send(output(19))
            .await
            .expect("committed row remains canonical while delivery is absent");
        let send = writer.send(ack(7, COMMAND_ID));
        tokio::pin!(send);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut send)
                .await
                .is_err(),
            "Deferred(None) must bind to the current epoch instead of trusting stale Online"
        );
        assert!(event_rx.try_recv().is_err());

        online_tx.send_replace(false);
        epochs_tx.send_replace(None);
        epochs_tx.send_replace(Some(second));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut send)
                .await
                .is_err(),
            "replacement epoch must finish catch-up before the ACK is admitted"
        );
        online_tx.send_replace(true);
        tokio::time::timeout(Duration::from_secs(1), &mut send)
            .await
            .expect("replacement Online releases the fenced ACK")
            .expect("ACK admission succeeds");
        let (seen_epoch, _, seen_frame) = event_rx.recv().await.expect("ACK after replacement");
        assert_eq!(seen_epoch, second);
        assert_eq!(seen_frame, ack(7, COMMAND_ID));
    }

    #[tokio::test]
    async fn every_terminal_ack_in_one_batch_stays_behind_durable_recovery() {
        const FIRST_ID: &str = "00000000-0000-4000-8000-000000000007";
        const SECOND_ID: &str = "00000000-0000-4000-8000-000000000008";
        let first = DeliveryEpoch::for_test("session-gateway-batch-first");
        let second = DeliveryEpoch::for_test("session-gateway-batch-second");
        let (gateway, _command_tx, mut event_rx, epochs_tx, online_tx, _delivery) =
            make_gateway(1, 1, Some(first));
        let (_reader, mut writer) = gateway.split();

        let send = tokio::spawn(async move {
            writer
                .send_batch(vec![output(19), ack(7, FIRST_ID), ack(8, SECOND_ID)])
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while event_rx.len() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first ACK must fill the bounded supervisor lane");

        online_tx.send_replace(false);
        epochs_tx.send_replace(Some(second));
        let (seen_epoch, _, seen_frame) = event_rx.recv().await.expect("first ACK");
        assert_eq!(seen_epoch, first);
        assert_eq!(seen_frame, ack(7, FIRST_ID));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), event_rx.recv())
                .await
                .is_err(),
            "second terminal ACK must wait for replacement catch-up, not use a cleared barrier"
        );
        assert!(
            !send.is_finished(),
            "batch writer must remain fenced until replacement is Online"
        );

        online_tx.send_replace(true);
        let (seen_epoch, _, seen_frame) =
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .expect("replacement Online must release second ACK")
                .expect("supervisor lane remains open");
        assert_eq!(seen_epoch, second);
        assert_eq!(seen_frame, ack(8, SECOND_ID));
        send.await
            .expect("batch writer task")
            .expect("all terminal ACKs admitted");
    }

    #[tokio::test]
    async fn volatile_events_require_valid_nondurable_agent_event_shapes() {
        let (gateway, _command_tx, _event_rx, _epochs_tx, _online_tx, _delivery) =
            make_gateway(1, 1, None);
        let (_reader, mut writer) = gateway.split();

        let malformed = OutboundFrame::Event {
            envelope: Envelope {
                seq: None,
                personality_agent_id: crate::gateway::test_personality_agent_id(),
                event: serde_json::json!({"type": "not_an_agent_event"}),
            },
        };
        let error = writer
            .send(malformed)
            .await
            .expect_err("malformed volatile event must fail closed");
        assert!(matches!(
            error.downcast_ref::<SessionGatewayError>(),
            Some(SessionGatewayError::InvalidVolatileEvent { .. })
        ));

        let error = writer
            .send(OutboundFrame::Event {
                envelope: Envelope {
                    seq: None,
                    personality_agent_id: crate::gateway::test_personality_agent_id(),
                    event: serde_json::json!({"type": "turn_start"}),
                },
            })
            .await
            .expect_err("durable kind without seq must fail closed");
        assert!(matches!(
            error.downcast_ref::<SessionGatewayError>(),
            Some(SessionGatewayError::DurableEventWithoutSequence { kind: "turn_start" })
        ));
    }

    #[tokio::test]
    async fn ack_handoff_observes_epochs_but_session_events_do_not() {
        const COMMAND_ID: &str = "00000000-0000-4000-8000-000000000007";
        let first = DeliveryEpoch::for_test("session-gateway-first");
        let second = DeliveryEpoch::for_test("session-gateway-second");
        let (gateway, _command_tx, mut event_rx, epochs_tx, _online_tx, delivery) =
            make_gateway(1, 2, Some(first));
        let (_reader, mut writer) = gateway.split();

        writer
            .send(ack(7, COMMAND_ID))
            .await
            .expect("first frame admitted");
        epochs_tx.send_replace(Some(second));
        writer
            .send(output(23))
            .await
            .expect("durable notification admitted to T17");
        writer
            .send(ack(8, COMMAND_ID))
            .await
            .expect("replacement ACK admitted");

        let (first_seen, _, _) = event_rx.recv().await.expect("first frame");
        let (second_seen, _, _) = event_rx.recv().await.expect("second frame");
        assert_eq!(first_seen, first);
        assert_eq!(second_seen, second);
        assert_ne!(first_seen, second_seen);
        assert_eq!(
            *delivery.durable.lock().unwrap(),
            vec![(crate::gateway::test_personality_agent_id(), 23)]
        );
    }

    #[tokio::test]
    async fn offline_ack_waits_for_an_epoch_and_is_not_lost() {
        const COMMAND_ID: &str = "00000000-0000-4000-8000-000000000007";
        let epoch = DeliveryEpoch::for_test("session-gateway-reconnect");
        let (gateway, _command_tx, mut event_rx, epochs_tx, _online_tx, _delivery) =
            make_gateway(1, 1, None);
        let (_reader, mut writer) = gateway.split();
        let send = writer.send(ack(7, COMMAND_ID));
        tokio::pin!(send);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut send)
                .await
                .is_err(),
            "offline ACK must remain pending instead of being dropped"
        );
        assert!(event_rx.try_recv().is_err());

        epochs_tx.send_replace(Some(epoch));
        tokio::time::timeout(Duration::from_secs(1), &mut send)
            .await
            .expect("reconnect must release the pending ACK")
            .expect("pending ACK admitted");
        let (seen_epoch, _, seen_frame) =
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .expect("ACK must reach T24")
                .expect("ACK event channel remains open");
        assert_eq!(seen_epoch, epoch);
        assert_eq!(seen_frame, ack(7, COMMAND_ID));
    }

    #[tokio::test]
    async fn full_ack_channel_applies_bounded_backpressure_without_loss() {
        const FIRST_ID: &str = "00000000-0000-4000-8000-000000000007";
        const SECOND_ID: &str = "00000000-0000-4000-8000-000000000008";
        let first = DeliveryEpoch::for_test("session-gateway-blocked-first");
        let second = DeliveryEpoch::for_test("session-gateway-blocked-second");
        let (gateway, _command_tx, mut event_rx, epochs_tx, _online_tx, _delivery) =
            make_gateway(1, 1, Some(first));
        let (_reader, mut writer) = gateway.split();

        writer
            .send(ack(7, FIRST_ID))
            .await
            .expect("first frame fills the bounded channel");
        let second_send = writer.send(ack(8, SECOND_ID));
        tokio::pin!(second_send);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut second_send)
                .await
                .is_err(),
            "a full bounded lane must backpressure rather than lose the ACK"
        );

        epochs_tx.send_replace(Some(second));
        let (first_seen, _, first_frame) = event_rx.recv().await.expect("first frame");
        assert_eq!(first_seen, first);
        assert_eq!(first_frame, ack(7, FIRST_ID));
        tokio::time::timeout(Duration::from_secs(1), &mut second_send)
            .await
            .expect("free capacity must admit the pending ACK")
            .expect("pending ACK remains valid");
        let (replacement_seen, _, replacement_frame) =
            event_rx.recv().await.expect("replacement frame");
        assert_eq!(replacement_seen, second);
        assert_eq!(replacement_frame, ack(8, SECOND_ID));
    }

    #[tokio::test]
    async fn closed_supervisor_channels_report_ack_loss() {
        const COMMAND_ID: &str = "00000000-0000-4000-8000-000000000007";
        let (gateway, _command_tx, _event_rx, epochs_tx, _online_tx, _delivery) =
            make_gateway(1, 1, None);
        drop(epochs_tx);
        let (_reader, mut writer) = gateway.split();
        let error = writer
            .send(ack(7, COMMAND_ID))
            .await
            .expect_err("closed epoch watch must make an undeliverable ACK explicit");
        assert!(error.to_string().contains("epoch watch closed"));

        let epoch = DeliveryEpoch::for_test("session-gateway-closed-events");
        let (gateway, _command_tx, event_rx, _epochs_tx, _online_tx, _delivery) =
            make_gateway(1, 1, Some(epoch));
        drop(event_rx);
        let (_reader, mut writer) = gateway.split();
        let error = writer
            .send(ack(7, COMMAND_ID))
            .await
            .expect_err("closed ACK lane must make an undeliverable ACK explicit");
        assert!(error.to_string().contains("ACK lane closed"));
    }

    #[tokio::test]
    async fn missing_or_failing_t17_event_delivery_is_typed_fatal() {
        let (mut gateway, _command_tx, _event_rx, _epochs_tx, _online_tx, _delivery) =
            make_gateway(1, 1, None);
        gateway.session_events = None;
        let (_reader, mut writer) = gateway.split();
        let error = writer
            .send(output(7))
            .await
            .expect_err("durable event without T17 sink must fail closed");
        assert!(matches!(
            error.downcast_ref::<SessionGatewayError>(),
            Some(SessionGatewayError::MissingEventSink)
        ));

        let (mut gateway, _command_tx, _event_rx, _epochs_tx, _online_tx, _delivery) =
            make_gateway(1, 1, None);
        gateway.session_events = Some(SessionEventSink::new(FailingDelivery));
        let (_reader, mut writer) = gateway.split();
        let error = writer
            .send(output(8))
            .await
            .expect_err("durable Store corruption must remain fatal");
        assert!(matches!(
            error.downcast_ref::<SessionGatewayError>(),
            Some(SessionGatewayError::DurableEvent { seq: 8, .. })
        ));

        let error = writer
            .send(volatile_output("secret"))
            .await
            .expect_err("volatile authorization corruption must remain fatal");
        assert!(matches!(
            error.downcast_ref::<SessionGatewayError>(),
            Some(SessionGatewayError::VolatileEvent { .. })
        ));
    }

    #[tokio::test]
    async fn authenticated_handoff_rejects_a_hidden_second_hello() {
        let (mut gateway, _command_tx, _event_rx, _epochs_tx, _online_tx, _delivery) = make_gateway(
            1,
            1,
            Some(DeliveryEpoch::for_test("session-gateway-no-second-hello")),
        );
        let error = gateway
            .authenticate_hello(AgentHello {
                personality_agent_id: crate::gateway::test_personality_agent_id(),
                generation: ProcessGeneration::MIN,
                last_sent_event_seq: 0,
                last_received_command_seq: 0,
                last_applied_command_seq: 0,
            })
            .await
            .expect_err("supervisor handoff is already authenticated");
        assert!(matches!(error, HelloError::Fatal(_)));
    }

    #[tokio::test]
    async fn closed_command_channel_is_terminal_to_session() {
        let epoch = DeliveryEpoch::for_test("session-gateway-closed-commands");
        let (gateway, command_tx, _event_rx, _epochs_tx, _online_tx, _delivery) =
            make_gateway(1, 1, Some(epoch));
        drop(command_tx);
        let (mut reader, _writer) = gateway.split();

        let error = reader
            .next_command()
            .await
            .expect_err("closed command channel");
        assert!(error.downcast_ref::<GatewayClosed>().is_some());
    }

    #[tokio::test]
    async fn production_split_keeps_supervisor_lifecycle_out_of_session() {
        let (gateway, _command_tx, _event_rx, _epochs_tx, _online_tx, _delivery) =
            make_gateway(1, 1, Some(DeliveryEpoch::for_test("bootstrap-lifecycle")));
        let SessionGateway {
            commands,
            ack_events,
            epochs,
            online,
            session_events,
            lifecycle: _fixture_lifecycle,
        } = gateway;

        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let (finished_tx, finished_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            task_cancel.cancelled().await;
            let _ = finished_tx.send(());
            Ok(())
        });
        let handle = SupervisorHandle {
            commands,
            events: ack_events,
            epochs,
            online,
            session_events,
            lifecycle: SupervisorLifecycle {
                cancel: cancel.clone(),
                task: Some(task),
            },
        };
        let (gateway, mut runtime) = SessionGateway::from_supervisor(handle);

        let (reader, writer) = gateway.split();
        drop(reader);
        drop(writer);
        tokio::task::yield_now().await;
        assert!(
            !cancel.is_cancelled(),
            "Session owns only channels and must not control supervisor lifetime"
        );

        runtime
            .cancel_and_join()
            .await
            .expect("bootstrap-owned supervisor must cancel and join cleanly");
        tokio::time::timeout(Duration::from_secs(1), finished_rx)
            .await
            .expect("joined supervisor must run cancellation cleanup")
            .expect("supervisor completion signal remains open");
    }

    #[tokio::test]
    async fn split_writer_retains_supervisor_lifecycle_until_explicit_join() {
        let (mut gateway, _command_tx, _event_rx, _epochs_tx, _online_tx, _delivery) =
            make_gateway(1, 1, Some(DeliveryEpoch::for_test("session-lifecycle")));
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let (finished_tx, finished_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            task_cancel.cancelled().await;
            let _ = finished_tx.send(());
            Ok(())
        });
        gateway.lifecycle = Some(SupervisorLifecycle {
            cancel: cancel.clone(),
            task: Some(task),
        });

        let (reader, mut writer) = gateway.split();
        drop(reader);
        tokio::task::yield_now().await;
        assert!(
            !cancel.is_cancelled(),
            "reader ownership must not tear down the writer's supervisor"
        );

        let mut lifecycle = writer
            ._lifecycle
            .take()
            .expect("writer retains the supervisor lifecycle");
        drop(writer);
        lifecycle.cancel.cancel();
        lifecycle
            .join()
            .await
            .expect("retained lifecycle joins explicitly");
        tokio::time::timeout(Duration::from_secs(1), cancel.cancelled())
            .await
            .expect("explicit teardown cancels supervisor lifecycle");
        tokio::time::timeout(Duration::from_secs(1), finished_rx)
            .await
            .expect("cancelled supervisor task must finish")
            .expect("supervisor completion signal must remain open");
    }
}
