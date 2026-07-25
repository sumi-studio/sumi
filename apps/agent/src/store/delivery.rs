#![allow(dead_code)]

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use sqlx::Row;
use tokio::{sync::mpsc, time::timeout};

use crate::agent::AgentEvent;

use super::{DataKeyPurpose, Store, crypto::decrypt_content};

#[cfg(test)]
use super::PublicProjectionBuilder;

const MAX_EPOCH_BYTES: usize = 128;
const DELIVERY_SEND_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_CHANNEL_CAPACITY: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DeliveryEpoch(pub(crate) String);

impl DeliveryEpoch {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_EPOCH_BYTES {
            bail!("DeliveryEpoch must be 1..={MAX_EPOCH_BYTES} bytes");
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeliveryMode {
    Raw,
    RedactionOnly,
}

#[derive(Clone, Debug)]
pub(crate) enum DeliveryFrame {
    Durable {
        seq: u64,
        raw: Option<AgentEvent>,
        projection: Option<String>,
    },
    Volatile(AgentEvent),
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
    CatchingUp { epoch: DeliveryEpoch },
    Online { epoch: DeliveryEpoch },
}

pub(crate) struct DeliveryPump {
    store: Arc<Store>,
    channel: DeliveryChannel,
    state: PumpState,
}

impl DeliveryPump {
    pub(crate) fn new(store: Arc<Store>, channel: DeliveryChannel) -> Self {
        Self {
            store,
            channel,
            state: PumpState::Idle,
        }
    }

    pub(crate) fn epoch(&self) -> Option<&DeliveryEpoch> {
        match &self.state {
            PumpState::Idle => None,
            PumpState::CatchingUp { epoch } | PumpState::Online { epoch } => Some(epoch),
        }
    }

    pub(crate) fn is_online(&self) -> bool {
        matches!(self.state, PumpState::Online { .. })
    }

    pub(crate) async fn install_epoch(
        &mut self,
        epoch: DeliveryEpoch,
        catch_up_from_seq: u64,
    ) -> Result<()> {
        if catch_up_from_seq == 0 {
            bail!("catch-up must start from a positive next-seq");
        }
        self.state = PumpState::CatchingUp {
            epoch: epoch.clone(),
        };

        let head_seq = current_event_head_seq(self.store.pool()).await?;
        if head_seq >= catch_up_from_seq {
            send_event_range(&self.store, &self.channel, catch_up_from_seq, head_seq).await?;
        }

        self.state = PumpState::Online { epoch };
        Ok(())
    }

    pub(crate) fn invalidate_epoch(&mut self) {
        self.state = PumpState::Idle;
    }

    pub(crate) async fn on_durable_committed(&mut self, seq: u64) -> Result<()> {
        match &self.state {
            PumpState::Idle => Ok(()),
            PumpState::CatchingUp { .. } => Ok(()),
            PumpState::Online { .. } => {
                send_event_range(&self.store, &self.channel, seq, seq).await
            }
        }
    }

    pub(crate) async fn on_volatile(&mut self, event: AgentEvent) -> Result<()> {
        if event.durable_kind().is_some() {
            bail!("volatile delivery rejected durable event {event:?}");
        }
        if !self.is_online() {
            return Ok(());
        }
        if matches!(self.channel.mode, DeliveryMode::RedactionOnly) {
            return Ok(());
        }
        self.channel.send(DeliveryFrame::Volatile(event)).await
    }
}

async fn current_event_head_seq(pool: &sqlx::SqlitePool) -> Result<u64> {
    let row = sqlx::query("SELECT COALESCE(MAX(seq), 0) AS head FROM agent_events")
        .fetch_one(pool)
        .await
        .context("failed to read event head")?;
    let head: i64 = row.try_get("head")?;
    u64::try_from(head).context("event head seq is negative")
}

async fn send_event_range(
    store: &Store,
    channel: &DeliveryChannel,
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
    .bind(i64::try_from(first_seq).unwrap_or(i64::MAX))
    .bind(i64::try_from(last_seq).unwrap_or(i64::MAX))
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
                let raw = decrypt_event(store, seq, &key_ref, &ciphertext).await.ok();
                if let Some(event) = raw {
                    (Some(event), None)
                } else {
                    (None, Some(envelope))
                }
            }
        };

        channel
            .send(DeliveryFrame::Durable {
                seq,
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
    let plaintext = decrypt_content(&key, ciphertext, &aad)
        .context("failed to decrypt durable event for delivery")?;
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
    use crate::store::Store;

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
    async fn raw_connection_receives_decrypted_durable_events() {
        let store = store().await;
        let event = assistant_event("msg-1", "hello");
        insert_test_durable_event(&store, 1, &event).await.unwrap();

        let (channel, mut receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw).build();
        let mut pump = DeliveryPump::new(store.clone(), channel);
        pump.install_epoch(DeliveryEpoch::new("epoch-1").unwrap(), 1)
            .await
            .unwrap();

        let frame = timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        match frame {
            DeliveryFrame::Durable {
                seq: 1,
                raw: Some(AgentEvent::MessageEnd { message_id, .. }),
                projection: None,
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
        let mut pump = DeliveryPump::new(store.clone(), channel);
        pump.install_epoch(DeliveryEpoch::new("epoch-1").unwrap(), 1)
            .await
            .unwrap();

        let frame = timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        match frame {
            DeliveryFrame::Durable {
                seq: 1,
                raw: None,
                projection: Some(proj),
            } => {
                assert!(proj.contains("[REDACTED:api_key]"));
                assert!(!proj.contains("sk-abcdefghijklmnop"));
                assert_eq!(proj, projection);
            }
            other => panic!("unexpected frame {other:?}"),
        }
    }

    #[tokio::test]
    async fn volatiles_dropped_while_offline_and_catch_up_discards_deltas() {
        let store = store().await;
        let event = assistant_event("msg-1", "first");
        insert_test_durable_event(&store, 1, &event).await.unwrap();

        let (channel, mut receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw).build();
        let mut pump = DeliveryPump::new(store.clone(), channel);

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

        pump.install_epoch(DeliveryEpoch::new("epoch-1").unwrap(), 1)
            .await
            .unwrap();

        let mut seqs = Vec::new();
        while let Ok(Some(frame)) = timeout(Duration::from_millis(200), receiver.recv()).await {
            match frame {
                DeliveryFrame::Durable { seq, .. } => seqs.push(seq),
                DeliveryFrame::Volatile(_) => panic!("volatile must not appear in catch-up"),
            }
        }
        assert_eq!(seqs, vec![1, 2]);

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
        let mut pump = DeliveryPump::new(store.clone(), channel);

        insert_test_durable_event(&store, 1, &assistant_event("msg-1", "a"))
            .await
            .unwrap();
        pump.install_epoch(DeliveryEpoch::new("epoch-1").unwrap(), 1)
            .await
            .unwrap();

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
    async fn new_epoch_invalidates_old_and_late_frames_from_old_epoch_are_ignored() {
        let store = store().await;
        let (channel, mut receiver) = DeliveryChannelBuilder::with_mode(DeliveryMode::Raw).build();
        let mut pump = DeliveryPump::new(store.clone(), channel);

        insert_test_durable_event(&store, 1, &assistant_event("msg-1", "a"))
            .await
            .unwrap();
        pump.install_epoch(DeliveryEpoch::new("epoch-1").unwrap(), 1)
            .await
            .unwrap();
        receiver.recv().await.unwrap();

        pump.invalidate_epoch();
        let event2 = assistant_event("msg-2", "b");
        insert_test_durable_event(&store, 2, &event2).await.unwrap();
        pump.on_durable_committed(2).await.unwrap();

        assert!(
            timeout(Duration::from_millis(100), receiver.recv())
                .await
                .is_err()
        );

        pump.install_epoch(DeliveryEpoch::new("epoch-2").unwrap(), 1)
            .await
            .unwrap();
        let mut count = 0;
        while let Ok(Some(_)) = timeout(Duration::from_millis(200), receiver.recv()).await {
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn volatile_rejected_for_durable_event_and_redaction_only_drops_volatiles() {
        let store = store().await;
        let (channel, mut receiver) =
            DeliveryChannelBuilder::with_mode(DeliveryMode::RedactionOnly).build();
        let mut pump = DeliveryPump::new(store.clone(), channel);
        pump.install_epoch(DeliveryEpoch::new("epoch-1").unwrap(), 1)
            .await
            .unwrap();

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

        let error = pump.on_volatile(assistant_event("msg-x", "x")).await;
        assert!(error.is_err());
    }
}
