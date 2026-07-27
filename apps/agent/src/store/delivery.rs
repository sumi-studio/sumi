#![allow(dead_code)]

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use sqlx::Row;
use tokio::{sync::mpsc, time::timeout};
use zeroize::Zeroizing;

use crate::agent::AgentEvent;
use crate::gateway::supervisor::DeliveryEpoch;

use super::{DataKeyPurpose, Store, crypto::decrypt_content};

#[cfg(test)]
use super::PublicProjectionBuilder;

const DELIVERY_SEND_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_CHANNEL_CAPACITY: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeliveryMode {
    Raw,
    RedactionOnly,
}

#[derive(Clone, Debug)]
pub(crate) enum DeliveryFrame {
    Durable {
        seq: u64,
        epoch: DeliveryEpoch,
        raw: Option<AgentEvent>,
        projection: Option<String>,
    },
    Volatile {
        epoch: DeliveryEpoch,
        event: AgentEvent,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct DeliveryChannel {
    sender: mpsc::Sender<DeliveryFrame>,
    mode: DeliveryMode,
}

impl DeliveryChannel {
    pub(crate) fn new(sender: mpsc::Sender<DeliveryFrame>, mode: DeliveryMode) -> Self {
        Self { sender, mode }
    }

    pub(crate) fn mode(&self) -> DeliveryMode {
        self.mode
    }

    async fn send(&self, frame: DeliveryFrame) -> Result<()> {
        timeout(DELIVERY_SEND_TIMEOUT, self.sender.send(frame))
            .await
            .context("delivery send timed out")?
            .context("delivery receiver closed")
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DeliveryChannelBuilder {
    capacity: usize,
    mode: DeliveryMode,
}

impl DeliveryChannelBuilder {
    pub(crate) fn with_mode(mode: DeliveryMode) -> Self {
        Self {
            capacity: DEFAULT_CHANNEL_CAPACITY,
            mode,
        }
    }

    pub(crate) fn capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    pub(crate) fn build(self) -> (DeliveryChannel, mpsc::Receiver<DeliveryFrame>) {
        let (sender, receiver) = mpsc::channel(self.capacity);
        (DeliveryChannel::new(sender, self.mode), receiver)
    }
}

enum PumpState {
    Idle,
    CatchingUp {
        epoch: DeliveryEpoch,
        failure_tx: Option<mpsc::UnboundedSender<String>>,
        pending_durable: usize,
    },
    Online {
        epoch: DeliveryEpoch,
        failure_tx: Option<mpsc::UnboundedSender<String>>,
        pending_durable: usize,
    },
}

#[derive(Clone)]
pub(crate) struct DeliveryPump {
    store: Arc<Store>,
    channel: DeliveryChannel,
    state: Arc<std::sync::Mutex<PumpState>>,
    durable_serial: Arc<tokio::sync::Mutex<()>>,
}

struct PendingDurable<'a> {
    pump: &'a DeliveryPump,
    epoch: DeliveryEpoch,
}

impl Drop for PendingDurable<'_> {
    fn drop(&mut self) {
        let mut state = self.pump.state.lock().unwrap();
        match &mut *state {
            PumpState::CatchingUp {
                epoch,
                pending_durable,
                ..
            } if *epoch == self.epoch => {
                *pending_durable = pending_durable.saturating_sub(1);
            }
            PumpState::Online {
                epoch,
                pending_durable,
                ..
            } if *epoch == self.epoch => {
                *pending_durable = pending_durable.saturating_sub(1);
            }
            PumpState::Idle | PumpState::CatchingUp { .. } | PumpState::Online { .. } => {}
        }
    }
}

impl DeliveryPump {
    pub(crate) fn new(store: Arc<Store>, channel: DeliveryChannel) -> Self {
        Self {
            store,
            channel,
            state: Arc::new(std::sync::Mutex::new(PumpState::Idle)),
            durable_serial: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub(crate) fn epoch(&self) -> Option<DeliveryEpoch> {
        match &*self.state.lock().unwrap() {
            PumpState::Idle => None,
            PumpState::CatchingUp { epoch, .. } | PumpState::Online { epoch, .. } => Some(*epoch),
        }
    }

    pub(crate) fn is_online(&self) -> bool {
        matches!(*self.state.lock().unwrap(), PumpState::Online { .. })
    }

    /// Install the live-delivery epoch. Initial durable replay is owned by the
    /// supervisor writer, which reads bounded pages from `DurableSource`.
    pub(crate) fn install_epoch(&self, epoch: DeliveryEpoch) {
        *self.state.lock().unwrap() = PumpState::Online {
            epoch,
            failure_tx: None,
            pending_durable: 0,
        };
    }

    pub(crate) fn install_supervised_epoch(
        &self,
        epoch: DeliveryEpoch,
        failure_tx: mpsc::UnboundedSender<String>,
    ) {
        *self.state.lock().unwrap() = PumpState::CatchingUp {
            epoch,
            failure_tx: Some(failure_tx),
            pending_durable: 0,
        };
    }

    /// Open volatile admission only after the supervisor has completed its
    /// durable catch-up and final cursor check. Durable callbacks remain
    /// admissible during catch-up so the replay cursor can converge, but all
    /// volatile frames are dropped until this barrier is crossed.
    pub(crate) fn mark_online(&self, epoch: DeliveryEpoch) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        match &mut *state {
            PumpState::CatchingUp {
                epoch: current,
                failure_tx,
                pending_durable,
            } if *current == epoch => {
                *state = PumpState::Online {
                    epoch,
                    failure_tx: failure_tx.clone(),
                    pending_durable: *pending_durable,
                };
                Ok(())
            }
            PumpState::Online { epoch: current, .. } if *current == epoch => Ok(()),
            PumpState::Idle => bail!("delivery epoch {} is not active", epoch.as_u64()),
            PumpState::CatchingUp { epoch: current, .. }
            | PumpState::Online { epoch: current, .. } => bail!(
                "delivery epoch barrier mismatch: expected {}, current {}",
                epoch.as_u64(),
                current.as_u64()
            ),
        }
    }

    pub(crate) fn invalidate_epoch(&self, epoch: DeliveryEpoch) -> bool {
        let mut state = self.state.lock().unwrap();
        if !matches!(
            &*state,
            PumpState::CatchingUp { epoch: current, .. }
                | PumpState::Online { epoch: current, .. }
                if *current == epoch
        ) {
            return false;
        }
        *state = PumpState::Idle;
        true
    }

    pub(crate) async fn on_durable_committed(&self, seq: u64) -> Result<()> {
        // Preserve the EventWriter's post-commit durable FIFO while keeping the
        // shared adapter selection/state lock out of the bounded channel await.
        let admission_epoch = {
            let mut state = self.state.lock().unwrap();
            match &mut *state {
                PumpState::CatchingUp {
                    epoch,
                    pending_durable,
                    ..
                }
                | PumpState::Online {
                    epoch,
                    pending_durable,
                    ..
                } => {
                    *pending_durable = pending_durable.saturating_add(1);
                    *epoch
                }
                PumpState::Idle => return Ok(()),
            }
        };
        let _pending = PendingDurable {
            pump: self,
            epoch: admission_epoch,
        };
        let _serial = self.durable_serial.lock().await;
        let (epoch, failure_tx) = match &*self.state.lock().unwrap() {
            PumpState::Idle => return Ok(()),
            PumpState::CatchingUp {
                epoch, failure_tx, ..
            }
            | PumpState::Online {
                epoch, failure_tx, ..
            } if *epoch == admission_epoch => (*epoch, failure_tx.clone()),
            PumpState::CatchingUp { .. } | PumpState::Online { .. } => return Ok(()),
        };
        if let Err(err) = send_event_range(&self.store, &self.channel, &epoch, seq, seq).await {
            let mut state = self.state.lock().unwrap();
            // A replacement epoch may have been installed while the bounded
            // durable send was waiting. Only the epoch that initiated the send
            // may transition itself to Idle or notify its failure supervisor.
            if matches!(
                &*state,
                PumpState::CatchingUp { epoch: current, .. }
                    | PumpState::Online { epoch: current, .. }
                    if *current == epoch
            ) {
                *state = PumpState::Idle;
                if let Some(failure_tx) = failure_tx {
                    let _ = failure_tx.send(format!("durable delivery failed: {err:#}"));
                }
            }
            return Err(err);
        }
        Ok(())
    }

    pub(crate) async fn on_volatile(&self, event: AgentEvent) -> Result<()> {
        if let Some(kind) = event.durable_kind() {
            bail!("volatile delivery rejected durable event of kind {kind}");
        }
        let (epoch, failure_tx) = match &*self.state.lock().unwrap() {
            PumpState::Online {
                epoch,
                failure_tx,
                pending_durable,
            } if *pending_durable == 0 => (*epoch, failure_tx.clone()),
            PumpState::Online { .. } | PumpState::CatchingUp { .. } => return Ok(()),
            PumpState::Idle => return Ok(()),
        };
        if matches!(self.channel.mode, DeliveryMode::RedactionOnly) {
            return Ok(());
        }
        match self
            .channel
            .sender
            .try_send(DeliveryFrame::Volatile { epoch, event })
        {
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                let mut state = self.state.lock().unwrap();
                if matches!(&*state, PumpState::Online { epoch: current, .. } if *current == epoch)
                {
                    *state = PumpState::Idle;
                    if let Some(failure_tx) = failure_tx {
                        let _ = failure_tx.send("volatile delivery receiver closed".to_owned());
                    }
                }
                bail!("delivery receiver closed")
            }
        }
    }
}

pub(crate) async fn current_event_head_seq(pool: &sqlx::SqlitePool) -> Result<u64> {
    let row = sqlx::query("SELECT COALESCE(MAX(seq), 0) AS head FROM agent_events")
        .fetch_one(pool)
        .await
        .context("failed to read event head")?;
    let head: i64 = row.try_get("head")?;
    u64::try_from(head).context("event head seq is negative")
}

pub(crate) async fn raw_events_after(
    store: &Store,
    after_seq: u64,
    limit: usize,
) -> Result<Vec<(u64, AgentEvent)>> {
    if limit == 0 {
        bail!("delivery event page size must be positive");
    }
    let rows = sqlx::query(
        "SELECT seq, raw_key_ref, raw_ciphertext
         FROM agent_events
         WHERE seq > ?
         ORDER BY seq
         LIMIT ?",
    )
    .bind(i64::try_from(after_seq).context("after_seq exceeds SQLite INTEGER range")?)
    .bind(i64::try_from(limit).context("event page size exceeds SQLite INTEGER range")?)
    .fetch_all(store.pool())
    .await
    .context("failed to fetch durable event page")?;

    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let seq: i64 = row.try_get("seq")?;
        let seq = u64::try_from(seq).context("stored event seq is negative")?;
        let key_ref: String = row.try_get("raw_key_ref")?;
        let ciphertext: Vec<u8> = row.try_get("raw_ciphertext")?;
        let event = decrypt_event(store, seq, &key_ref, &ciphertext)
            .await
            .context("failed to decrypt durable event page row")?;
        events.push((seq, event));
    }
    Ok(events)
}

async fn send_event_range(
    store: &Store,
    channel: &DeliveryChannel,
    epoch: &DeliveryEpoch,
    first_seq: u64,
    last_seq: u64,
) -> Result<()> {
    if first_seq > last_seq {
        return Ok(());
    }
    let rows = sqlx::query(
        "SELECT seq, raw_key_ref, raw_ciphertext, envelope, redaction_version
         FROM agent_events
         WHERE seq >= ? AND seq <= ?
         ORDER BY seq",
    )
    .bind(i64::try_from(first_seq).context("first_seq exceeds SQLite INTEGER range")?)
    .bind(i64::try_from(last_seq).context("last_seq exceeds SQLite INTEGER range")?)
    .fetch_all(store.pool())
    .await
    .context("failed to fetch durable events for delivery")?;

    for row in rows {
        let seq: i64 = row.try_get("seq")?;
        let seq = u64::try_from(seq).context("stored event seq is negative")?;
        let key_ref: String = row.try_get("raw_key_ref")?;
        let ciphertext: Vec<u8> = row.try_get("raw_ciphertext")?;
        let envelope: String = row.try_get("envelope")?;
        let _redaction_version: i64 = row.try_get("redaction_version")?;

        let (raw, projection) = match channel.mode {
            DeliveryMode::RedactionOnly => (None, Some(envelope)),
            DeliveryMode::Raw => {
                let raw = decrypt_event(store, seq, &key_ref, &ciphertext)
                    .await
                    .context("failed to decrypt durable event for raw delivery")?;
                (Some(raw), None)
            }
        };

        channel
            .send(DeliveryFrame::Durable {
                seq,
                epoch: *epoch,
                raw,
                projection,
            })
            .await?;
    }
    Ok(())
}

async fn decrypt_event(
    store: &Store,
    seq: u64,
    key_ref: &str,
    ciphertext: &[u8],
) -> Result<AgentEvent> {
    let key = store.data_key_by_ref(key_ref).await?;
    if key.purpose != DataKeyPurpose::Event {
        bail!("event key {key_ref} has wrong purpose");
    }
    let aad = store
        .scope()
        .row_aad("agent_events", seq.to_string(), DataKeyPurpose::Event);
    let plaintext = Zeroizing::new(
        decrypt_content(&key, ciphertext, &aad)
            .context("failed to decrypt durable event for delivery")?,
    );
    serde_json::from_slice(&plaintext).context("durable event plaintext is not a valid AgentEvent")
}

#[cfg(test)]
pub(crate) async fn insert_test_durable_event(
    store: &Store,
    seq: u64,
    event: &AgentEvent,
) -> Result<String> {
    let key = store.conversation_key(DataKeyPurpose::Event).await?;
    let raw = serde_json::to_vec(event).context("failed to serialize test event")?;
    let aad = store
        .scope()
        .row_aad("agent_events", seq.to_string(), DataKeyPurpose::Event);
    let protected = PublicProjectionBuilder::new(store.redactor(), &key)
        .build_serialized(&raw, &aad)
        .context("failed to protect test durable event")?;

    sqlx::query(
        "INSERT INTO agent_events(
            seq, event_type, internal_metadata, raw_key_ref, raw_ciphertext,
            envelope, redaction_version, created_at
         ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(i64::try_from(seq).unwrap_or(i64::MAX))
    .bind(event.durable_kind().unwrap_or("volatile"))
    .bind("{}")
    .bind(&key.key_ref)
    .bind(&protected.ciphertext)
    .bind(&protected.projection)
    .bind(i64::from(protected.redaction_version))
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(store.pool())
    .await
    .context("failed to insert test durable event")?;
    Ok(protected.projection)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use tokio::time::timeout;

    use super::*;
    use crate::agent::{AgentEvent, PublicStreamEvent};
    use crate::provider::types::{
        ApiProtocol, ProviderOrigin, PublicAssistantContent, PublicAssistantMessage, PublicMessage,
        StopReason, Usage,
    };
    use crate::store::DataKeyPurpose;
    use crate::store::Store;
    use crate::store::crypto::encrypt_content;

    fn assistant_event(message_id: &str, text: &str) -> AgentEvent {
        AgentEvent::MessageEnd {
            message_id: message_id.to_owned(),
            message: Box::new(PublicMessage::Assistant(PublicAssistantMessage {
                content: vec![PublicAssistantContent::Text {
                    text: text.to_owned(),
                    wire_item_index: 0,
                }],
                model: "m".to_owned(),
                provider: "p".to_owned(),
                origin: ProviderOrigin {
                    provider_instance_id: "i".to_owned(),
                    protocol: ApiProtocol::OpenAiChatCompletions,
                    model: "m".to_owned(),
                },
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                provider_code: None,
                interrupted: false,
                timestamp: Utc::now(),
            })),
        }
    }

    async fn store() -> Arc<Store> {
        Arc::new(
            Store::session_test_store("delivery-conversation")
                .await
                .expect("open test store"),
        )
    }

    #[tokio::test]
    async fn event_head_survives_in_memory_pool_connection_replacement() {
        let store = store().await;
        insert_test_durable_event(&store, 1, &AgentEvent::AgentStart)
            .await
            .unwrap();

        // Dropping a cancelled SQLite query can make SQLx discard its worker
        // connection. Close the managed connection explicitly to reproduce that
        // replacement deterministically, without depending on scheduler timing.
        store
            .pool()
            .acquire()
            .await
            .expect("acquire managed in-memory connection")
            .close()
            .await
            .expect("close managed in-memory connection");

        assert_eq!(
            current_event_head_seq(store.pool()).await.unwrap(),
            1,
            "replacement connections must retain the migrated schema and durable rows"
        );
    }

    #[tokio::test]
    async fn raw_connection_receives_decrypted_durable_events() {
        let store = store().await;
        let event = assistant_event("msg-1", "hello");
        insert_test_durable_event(&store, 1, &event).await.unwrap();

        let (channel, mut receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw).build();
        let pump = DeliveryPump::new(store.clone(), channel);
        let epoch = DeliveryEpoch::for_test("epoch-1");
        pump.install_epoch(epoch);
        pump.on_durable_committed(1).await.unwrap();

        let frame = timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        match frame {
            DeliveryFrame::Durable {
                seq: 1,
                raw: Some(AgentEvent::MessageEnd { message_id, .. }),
                projection: None,
                ..
            } => assert_eq!(message_id, "msg-1"),
            other => panic!("unexpected frame {other:?}"),
        }
    }

    #[tokio::test]
    async fn redaction_only_connection_receives_projection_not_raw() {
        let store = store().await;
        let event = assistant_event("msg-1", "use sk-abcdefghijklmnop");
        let projection = insert_test_durable_event(&store, 1, &event).await.unwrap();

        let (channel, mut receiver) =
            DeliveryChannelBuilder::with_mode(DeliveryMode::RedactionOnly).build();
        let pump = DeliveryPump::new(store.clone(), channel);
        pump.install_epoch(DeliveryEpoch::for_test("epoch-1"));
        pump.on_durable_committed(1).await.unwrap();

        let frame = timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        match frame {
            DeliveryFrame::Durable {
                seq: 1,
                raw: None,
                projection: Some(proj),
                ..
            } => {
                assert!(proj.contains("[REDACTED:api_key]"));
                assert!(!proj.contains("sk-abcdefghijklmnop"));
                assert_eq!(proj, projection);
            }
            other => panic!("unexpected frame {other:?}"),
        }
    }

    #[tokio::test]
    async fn install_does_not_replay_backlog_and_offline_volatiles_are_dropped() {
        let store = store().await;
        let event = assistant_event("msg-1", "first");
        insert_test_durable_event(&store, 1, &event).await.unwrap();

        let (channel, mut receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw).build();
        let pump = DeliveryPump::new(store.clone(), channel);

        pump.on_volatile(AgentEvent::MessageUpdate {
            message_id: "msg-1".to_owned(),
            event: PublicStreamEvent::TextDelta {
                content_index: 0,
                delta: "delta".to_owned(),
            },
        })
        .await
        .unwrap();

        assert!(
            timeout(Duration::from_millis(100), receiver.recv())
                .await
                .is_err()
        );

        let event2 = assistant_event("msg-2", "second");
        insert_test_durable_event(&store, 2, &event2).await.unwrap();

        pump.install_epoch(DeliveryEpoch::for_test("epoch-1"));

        assert!(
            timeout(Duration::from_millis(100), receiver.recv())
                .await
                .is_err(),
            "DeliveryPump install must not duplicate supervisor-owned catch-up"
        );

        let event3 = assistant_event("msg-3", "third");
        insert_test_durable_event(&store, 3, &event3).await.unwrap();
        pump.on_durable_committed(3).await.unwrap();

        let frame = timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        match frame {
            DeliveryFrame::Durable { seq: 3, .. } => {}
            other => panic!("unexpected frame {other:?}"),
        }
    }

    #[tokio::test]
    async fn bounded_channel_blocks_and_recover_after_receiver_consumed() {
        let store = store().await;
        let (channel, mut receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw)
            .capacity(1)
            .build();
        let pump = DeliveryPump::new(store.clone(), channel);

        insert_test_durable_event(&store, 1, &assistant_event("msg-1", "a"))
            .await
            .unwrap();
        pump.install_epoch(DeliveryEpoch::for_test("epoch-1"));
        pump.on_durable_committed(1).await.unwrap();

        let event2 = assistant_event("msg-2", "b");
        insert_test_durable_event(&store, 2, &event2).await.unwrap();

        let send = timeout(Duration::from_millis(50), pump.on_durable_committed(2));
        assert!(
            send.await.is_err(),
            "second send should block on full channel"
        );

        receiver.recv().await.unwrap();
        pump.on_durable_committed(2).await.unwrap();
        receiver.recv().await.unwrap();
    }

    #[tokio::test]
    async fn volatile_try_send_is_not_blocked_by_in_flight_durable_send() {
        let store = store().await;
        insert_test_durable_event(&store, 1, &assistant_event("msg-1", "a"))
            .await
            .unwrap();
        insert_test_durable_event(&store, 2, &assistant_event("msg-2", "b"))
            .await
            .unwrap();

        let (channel, mut receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw)
            .capacity(1)
            .build();
        let pump = DeliveryPump::new(store.clone(), channel);
        pump.install_epoch(DeliveryEpoch::for_test("epoch-1"));
        pump.on_durable_committed(1).await.unwrap();

        // The full channel makes seq 2 wait in the bounded durable send. The
        // concurrent volatile path must still snapshot state and use try_send
        // without waiting behind that await; the full channel drops it.
        let durable = {
            let pump = pump.clone();
            tokio::spawn(async move { pump.on_durable_committed(2).await })
        };
        tokio::task::yield_now().await;

        let volatile = tokio::time::timeout(
            Duration::from_millis(100),
            pump.on_volatile(AgentEvent::MessageUpdate {
                message_id: "msg-1".to_owned(),
                event: PublicStreamEvent::TextDelta {
                    content_index: 0,
                    delta: "typing".to_owned(),
                },
            }),
        )
        .await
        .expect("volatile try_send must not wait for durable backpressure");
        volatile.unwrap();

        assert!(matches!(
            receiver.recv().await,
            Some(DeliveryFrame::Durable { seq: 1, .. })
        ));
        durable
            .await
            .expect("durable task must join")
            .expect("durable send must recover after receiver consumes");
        assert!(matches!(
            receiver.recv().await,
            Some(DeliveryFrame::Durable { seq: 2, .. })
        ));
    }

    #[tokio::test]
    async fn supervised_epoch_drops_volatiles_until_online_barrier() {
        let store = store().await;
        let (channel, mut receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw).build();
        let (failure_tx, _failure_rx) = mpsc::unbounded_channel();
        let pump = DeliveryPump::new(store, channel);
        let epoch = DeliveryEpoch::for_test("catching-up");
        pump.install_supervised_epoch(epoch, failure_tx);

        pump.on_volatile(AgentEvent::MessageUpdate {
            message_id: "pre-online".to_owned(),
            event: PublicStreamEvent::TextDelta {
                content_index: 0,
                delta: "must-drop".to_owned(),
            },
        })
        .await
        .unwrap();
        assert!(!pump.is_online());
        assert!(
            timeout(Duration::from_millis(50), receiver.recv())
                .await
                .is_err()
        );

        pump.mark_online(epoch).unwrap();
        pump.on_volatile(AgentEvent::MessageUpdate {
            message_id: "online".to_owned(),
            event: PublicStreamEvent::TextDelta {
                content_index: 0,
                delta: "deliver".to_owned(),
            },
        })
        .await
        .unwrap();
        assert!(matches!(
            receiver.recv().await,
            Some(DeliveryFrame::Volatile {
                event: AgentEvent::MessageUpdate { message_id, .. },
                ..
            }) if message_id == "online"
        ));
    }

    #[tokio::test]
    async fn volatile_drops_promptly_while_earlier_durable_admission_is_pending() {
        let store = store().await;
        insert_test_durable_event(&store, 1, &assistant_event("msg-1", "a"))
            .await
            .unwrap();
        let (channel, mut receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw)
            .capacity(2)
            .build();
        let pump = DeliveryPump::new(store, channel);
        let epoch = DeliveryEpoch::for_test("durable-barrier");
        pump.install_epoch(epoch);

        // Hold the durable serializer after admission. The durable callback has
        // already reserved the ordering barrier, but has not prepared/sent its
        // frame yet; a volatile callback must still return without overtaking it.
        let serial = pump.durable_serial.lock().await;
        let durable = {
            let pump = pump.clone();
            tokio::spawn(async move { pump.on_durable_committed(1).await })
        };
        tokio::task::yield_now().await;

        timeout(
            Duration::from_millis(100),
            pump.on_volatile(AgentEvent::MessageUpdate {
                message_id: "msg-1".to_owned(),
                event: PublicStreamEvent::TextDelta {
                    content_index: 0,
                    delta: "must-drop".to_owned(),
                },
            }),
        )
        .await
        .expect("volatile delivery must not wait on durable preparation")
        .unwrap();
        assert!(
            timeout(Duration::from_millis(50), receiver.recv())
                .await
                .is_err()
        );

        drop(serial);
        durable.await.unwrap().unwrap();
        assert!(matches!(
            receiver.recv().await,
            Some(DeliveryFrame::Durable { seq: 1, .. })
        ));
    }

    #[tokio::test]
    async fn stale_durable_failure_cannot_idle_a_replacement_epoch() {
        let store = store().await;
        insert_test_durable_event(&store, 1, &assistant_event("msg-1", "a"))
            .await
            .unwrap();
        insert_test_durable_event(&store, 2, &assistant_event("msg-2", "b"))
            .await
            .unwrap();

        let (channel, receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw)
            .capacity(1)
            .build();
        let pump = DeliveryPump::new(store.clone(), channel);
        let epoch_1 = DeliveryEpoch::for_test("epoch-1");
        let epoch_2 = DeliveryEpoch::for_test("epoch-2");
        pump.install_epoch(epoch_1);
        pump.on_durable_committed(1).await.unwrap();

        let durable = {
            let pump = pump.clone();
            tokio::spawn(async move { pump.on_durable_committed(2).await })
        };
        tokio::task::yield_now().await;

        assert!(pump.invalidate_epoch(epoch_1));
        pump.install_epoch(epoch_2);
        drop(receiver);

        assert!(durable.await.unwrap().is_err());
        assert_eq!(pump.epoch(), Some(epoch_2));
        assert!(
            pump.is_online(),
            "stale epoch failure must not idle replacement"
        );
    }

    #[tokio::test]
    async fn durable_send_success_keeps_pump_online() {
        let store = store().await;
        insert_test_durable_event(&store, 1, &assistant_event("msg-1", "a"))
            .await
            .unwrap();

        let (channel, mut receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw).build();
        let pump = DeliveryPump::new(store.clone(), channel);
        let epoch = DeliveryEpoch::for_test("epoch-1");
        pump.install_epoch(epoch);
        pump.on_durable_committed(1).await.unwrap();
        receiver.recv().await.unwrap();
        assert!(pump.is_online());

        let event2 = assistant_event("msg-2", "b");
        insert_test_durable_event(&store, 2, &event2).await.unwrap();
        pump.on_durable_committed(2).await.unwrap();

        assert!(pump.is_online());
        assert_eq!(pump.epoch(), Some(epoch));

        let frame = receiver.recv().await.unwrap();
        match frame {
            DeliveryFrame::Durable { seq: 2, .. } => {}
            other => panic!("unexpected frame {other:?}"),
        }
    }

    #[tokio::test]
    async fn durable_send_failure_transitions_to_idle() {
        let store = store().await;
        insert_test_durable_event(&store, 1, &assistant_event("msg-1", "a"))
            .await
            .unwrap();

        let (channel, mut receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw).build();
        let pump = DeliveryPump::new(store.clone(), channel);
        pump.install_epoch(DeliveryEpoch::for_test("epoch-1"));
        pump.on_durable_committed(1).await.unwrap();
        assert!(pump.is_online());
        receiver.recv().await.unwrap();

        let event2 = assistant_event("msg-2", "b");
        insert_test_durable_event(&store, 2, &event2).await.unwrap();

        drop(receiver);
        let err = pump
            .on_durable_committed(2)
            .await
            .expect_err("send on closed channel must fail");
        let message = err.to_string();
        assert!(
            message.contains("delivery receiver closed"),
            "unexpected error: {message}"
        );
        assert!(
            !pump.is_online(),
            "failed send must transition pump to Idle"
        );
        assert!(pump.epoch().is_none(), "failed send must clear epoch");
    }

    #[tokio::test]
    async fn volatile_send_failure_transitions_to_idle() {
        let store = store().await;
        let (channel, receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw).build();
        let pump = DeliveryPump::new(store, channel);
        pump.install_epoch(DeliveryEpoch::for_test("epoch-1"));
        drop(receiver);

        let error = pump
            .on_volatile(AgentEvent::ToolExecutionUpdate {
                tool_call_id: "tool-1".to_owned(),
                partial: serde_json::json!({"stdout": "partial"}),
            })
            .await
            .expect_err("volatile send on a closed delivery channel must fail");
        assert!(
            error.to_string().contains("delivery receiver closed"),
            "unexpected volatile delivery error: {error:#}"
        );
        assert!(
            !pump.is_online(),
            "volatile send failure must terminate the active pump epoch"
        );
        assert!(
            pump.epoch().is_none(),
            "volatile send failure must clear the active epoch"
        );
    }

    #[tokio::test]
    async fn volatile_queue_full_drops_immediately_without_disabling_epoch() {
        let store = store().await;
        let (channel, mut receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw)
            .capacity(1)
            .build();
        let pump = DeliveryPump::new(store, channel);
        let epoch = DeliveryEpoch::for_test("epoch-1");
        pump.install_epoch(epoch);

        let volatile = |suffix: &str| AgentEvent::ToolExecutionUpdate {
            tool_call_id: format!("tool-{suffix}"),
            partial: serde_json::json!({"stdout": suffix}),
        };
        pump.on_volatile(volatile("first")).await.unwrap();
        timeout(
            Duration::from_millis(50),
            pump.on_volatile(volatile("dropped")),
        )
        .await
        .expect("full volatile queue must not block")
        .unwrap();

        assert_eq!(pump.epoch(), Some(epoch));
        assert!(matches!(
            receiver.recv().await,
            Some(DeliveryFrame::Volatile { event: AgentEvent::ToolExecutionUpdate { tool_call_id, .. }, .. })
                if tool_call_id == "tool-first"
        ));
        assert!(
            timeout(Duration::from_millis(50), receiver.recv())
                .await
                .is_err(),
            "the overflow volatile must be dropped"
        );
    }

    #[tokio::test]
    async fn new_epoch_invalidates_old_and_late_frames_from_old_epoch_are_ignored() {
        let store = store().await;
        let (channel, mut receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw).build();
        let pump = DeliveryPump::new(store.clone(), channel);

        insert_test_durable_event(&store, 1, &assistant_event("msg-1", "a"))
            .await
            .unwrap();
        let epoch_1 = DeliveryEpoch::for_test("epoch-1");
        pump.install_epoch(epoch_1);
        pump.on_durable_committed(1).await.unwrap();
        receiver.recv().await.unwrap();

        assert!(pump.invalidate_epoch(epoch_1));
        let event2 = assistant_event("msg-2", "b");
        insert_test_durable_event(&store, 2, &event2).await.unwrap();
        pump.on_durable_committed(2).await.unwrap();

        assert!(
            timeout(Duration::from_millis(100), receiver.recv())
                .await
                .is_err()
        );

        let epoch_2 = DeliveryEpoch::for_test("epoch-2");
        pump.install_epoch(epoch_2);
        pump.on_durable_committed(2).await.unwrap();
        match receiver.recv().await.unwrap() {
            DeliveryFrame::Durable { seq: 2, epoch, .. } => assert_eq!(epoch, epoch_2),
            other => panic!("unexpected frame {other:?}"),
        }
    }

    #[tokio::test]
    async fn volatile_rejected_for_durable_event_and_redaction_only_drops_volatiles() {
        let store = store().await;
        let (channel, mut receiver) =
            DeliveryChannelBuilder::with_mode(DeliveryMode::RedactionOnly).build();
        let pump = DeliveryPump::new(store.clone(), channel);
        pump.install_epoch(DeliveryEpoch::for_test("epoch-1"));

        pump.on_volatile(AgentEvent::MessageUpdate {
            message_id: "msg-1".to_owned(),
            event: PublicStreamEvent::TextDelta {
                content_index: 0,
                delta: "x".to_owned(),
            },
        })
        .await
        .unwrap();

        assert!(
            timeout(Duration::from_millis(100), receiver.recv())
                .await
                .is_err()
        );

        let result = pump.on_volatile(assistant_event("msg-x", "x")).await;
        match result {
            Err(error) => {
                let message = format!("{error:#}");
                assert!(
                    message.contains("of kind message_end"),
                    "error must identify event kind without exposing payload: {message}"
                );
                assert!(
                    !message.contains("sk-"),
                    "error must not leak event contents"
                );
            }
            Ok(_) => panic!("on_volatile must reject a durable event"),
        }
    }

    #[tokio::test]
    async fn queued_frame_retains_attached_epoch_across_invalidation() {
        let store = store().await;
        let (channel, mut receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw).build();
        let pump = DeliveryPump::new(store.clone(), channel);

        let epoch_a = DeliveryEpoch::for_test("epoch-a");
        pump.install_epoch(epoch_a);

        let volatile_a = AgentEvent::MessageUpdate {
            message_id: "msg-a".to_owned(),
            event: PublicStreamEvent::TextDelta {
                content_index: 0,
                delta: "a".to_owned(),
            },
        };
        pump.on_volatile(volatile_a).await.unwrap();

        assert!(pump.invalidate_epoch(epoch_a));

        let durable_b = assistant_event("msg-b", "b");
        insert_test_durable_event(&store, 1, &durable_b)
            .await
            .unwrap();

        let epoch_b = DeliveryEpoch::for_test("epoch-b");
        pump.install_epoch(epoch_b);
        pump.on_durable_committed(1).await.unwrap();

        let volatile_b = AgentEvent::MessageUpdate {
            message_id: "msg-b".to_owned(),
            event: PublicStreamEvent::TextDelta {
                content_index: 0,
                delta: "b".to_owned(),
            },
        };
        pump.on_volatile(volatile_b).await.unwrap();

        // Volatile enqueued under epoch A must still carry A after invalidation.
        let frame = receiver.recv().await.unwrap();
        match frame {
            DeliveryFrame::Volatile { epoch, .. } => {
                assert_eq!(epoch, DeliveryEpoch::for_test("epoch-a"));
            }
            other => panic!("expected volatile frame from epoch A: {other:?}"),
        }

        // Durable catch-up frame enqueued under epoch B must carry B.
        let frame = receiver.recv().await.unwrap();
        match frame {
            DeliveryFrame::Durable { seq: 1, epoch, .. } => {
                assert_eq!(epoch, DeliveryEpoch::for_test("epoch-b"));
            }
            other => panic!("expected durable catch-up frame from epoch B: {other:?}"),
        }

        // New volatile enqueued under epoch B must carry B.
        let frame = receiver.recv().await.unwrap();
        match frame {
            DeliveryFrame::Volatile { epoch, .. } => {
                assert_eq!(epoch, DeliveryEpoch::for_test("epoch-b"));
            }
            other => panic!("expected volatile frame from epoch B: {other:?}"),
        }
    }

    #[tokio::test]
    async fn raw_decrypt_failure_returns_error_and_resets_to_idle() {
        let store = store().await;
        let event = assistant_event("msg-1", "hello");
        insert_test_durable_event(&store, 1, &event).await.unwrap();

        // Corrupt the durable raw ciphertext so decryption fails.
        sqlx::query("UPDATE agent_events SET raw_ciphertext = ? WHERE seq = 1")
            .bind(vec![0u8; 16])
            .execute(store.pool())
            .await
            .unwrap();

        let (channel, _receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw).build();
        let pump = DeliveryPump::new(store.clone(), channel);
        pump.install_epoch(DeliveryEpoch::for_test("epoch-1"));
        let result = pump.on_durable_committed(1).await;
        assert!(result.is_err(), "raw decrypt failure must propagate");
        assert!(
            pump.epoch().is_none(),
            "failed install_epoch must reset pump to Idle"
        );
    }

    #[tokio::test]
    async fn raw_deserialization_failure_does_not_leak_plaintext() {
        let store = store().await;
        let event = assistant_event("msg-1", "hello");
        insert_test_durable_event(&store, 1, &event).await.unwrap();

        let key = store
            .conversation_key(DataKeyPurpose::Event)
            .await
            .expect("active event key");
        let aad = store
            .scope()
            .row_aad("agent_events", "1", DataKeyPurpose::Event);
        let secret = b"this-is-not-valid-json-and-must-not-leak";
        let ciphertext = encrypt_content(&key, secret, &aad).expect("encrypt test plaintext");

        sqlx::query("UPDATE agent_events SET raw_ciphertext = ? WHERE seq = 1")
            .bind(ciphertext)
            .execute(store.pool())
            .await
            .unwrap();

        let (channel, _receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw).build();
        let pump = DeliveryPump::new(store.clone(), channel);
        pump.install_epoch(DeliveryEpoch::for_test("epoch-1"));
        let result = pump.on_durable_committed(1).await;
        let message = result.expect_err("invalid json must fail").to_string();
        assert!(
            !message.contains("this-is-not-valid-json-and-must-not-leak"),
            "decryption failure must not expose plaintext: {message}"
        );
    }
}
