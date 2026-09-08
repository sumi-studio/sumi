//! Optional, individual-scoped access to persisted experience, including L0
//! whose active context representation has been compacted or dropped.

use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use serde::Serialize;
use sqlx::{QueryBuilder, Row, Sqlite, Transaction};

use crate::provider::types::PublicMessage;

use super::{Store, transcript::decrypt_transcript_row};

pub(crate) const MAX_RECALL_MESSAGES: usize = 20;
// Bound the decrypted working set between whole messages. The model-facing
// tool separately pages the text inside a single oversized original message.
const RECALL_PAGE_BYTES: usize = 256 * 1024;

#[derive(Debug, Default)]
pub(crate) struct RecallRequest {
    pub query: Option<String>,
    pub message_id: Option<String>,
    pub batch_id: Option<String>,
    pub from_seq: Option<u64>,
    pub after_seq: Option<u64>,
    pub limit: usize,
}

impl RecallRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if !(1..=MAX_RECALL_MESSAGES).contains(&self.limit)
            || [self.from_seq, self.after_seq]
                .into_iter()
                .flatten()
                .any(|seq| seq > i64::MAX as u64)
        {
            bail!("invalid private recall page bounds");
        }
        for id in [&self.message_id, &self.batch_id].into_iter().flatten() {
            if id.is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
                bail!("invalid private recall source identity");
            }
        }
        let locators = usize::from(self.message_id.is_some())
            + usize::from(self.batch_id.is_some())
            + usize::from(self.from_seq.is_some());
        if locators > 1 || (self.message_id.is_some() && self.after_seq.is_some()) {
            bail!("private recall requires one source locator");
        }
        if let Some(query) = &self.query {
            if query.is_empty()
                || query.len() > 1024
                || query.chars().any(char::is_control)
                || locators != 0
            {
                bail!("invalid private recall search query");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct RecallSource {
    pub message_id: String,
    pub seq: u64,
    pub batch_id: Option<String>,
    pub stored_at: String,
}

#[derive(Debug)]
pub(crate) struct RecalledMessage {
    pub source: RecallSource,
    pub message: PublicMessage,
    pub search_text: String,
}

#[derive(Debug)]
pub(crate) struct RecallPage {
    pub messages: Vec<RecalledMessage>,
    pub next_after_seq: Option<u64>,
}

// Resolve only the requested ancestry. Metadata lives in the private SQLite
// store; originals are still individually authenticated when read below.
async fn original_batches(
    transaction: &mut Transaction<'_, Sqlite>,
    requested: &str,
) -> Result<Vec<String>> {
    let mut pending = vec![(requested.to_owned(), None, false)];
    let mut visiting = HashSet::new();
    let mut completed = HashSet::new();
    let mut originals = Vec::new();
    while let Some((id, expected_layer, exiting)) = pending.pop() {
        if exiting {
            visiting.remove(&id);
            completed.insert(id);
            continue;
        }
        if visiting.contains(&id) {
            bail!("cyclic private recall batch lineage");
        }
        let row = sqlx::query("SELECT layer, batch_seq FROM memory_batches WHERE id = ?")
            .bind(&id)
            .fetch_optional(&mut **transaction)
            .await?
            .context("missing private recall source batch")?;
        let layer: i64 = row.try_get("layer")?;
        if expected_layer.is_some_and(|expected| expected != layer) {
            bail!("invalid private recall source layer");
        }
        if completed.contains(&id) {
            continue;
        }
        if layer == 0 {
            originals.push(id.clone());
            completed.insert(id);
            continue;
        }
        let kinds: &[&str] = match layer {
            1 => &["compact_l0"],
            2 => &["compact_l1", "consolidate_l2"],
            _ => bail!("invalid private recall batch layer"),
        };
        let mut producers = Vec::new();
        for kind in kinds {
            if let Some(row) = sqlx::query(
                "SELECT kind, source_ids FROM memory_jobs
                 WHERE kind = ? AND batch_seq = ? AND status = 'applied'",
            )
            .bind(kind)
            .bind(row.try_get::<i64, _>("batch_seq")?)
            .fetch_optional(&mut **transaction)
            .await?
            {
                producers.push(row);
            }
        }
        if producers.len() != 1 {
            bail!("missing or ambiguous private recall batch producer");
        }
        let producer = &producers[0];
        let sources: Vec<String> =
            serde_json::from_str(&producer.try_get::<String, _>("source_ids")?)
                .context("invalid private recall source identities")?;
        let mut unique = HashSet::new();
        if sources.is_empty()
            || sources
                .iter()
                .any(|source| uuid::Uuid::parse_str(source).is_err() || !unique.insert(source))
        {
            bail!("invalid private recall source identities");
        }
        let source_layer = match producer.try_get::<&str, _>("kind")? {
            "compact_l0" => 0,
            "compact_l1" => 1,
            "consolidate_l2" => 2,
            _ => unreachable!(),
        };
        visiting.insert(id.clone());
        pending.push((id, expected_layer, true));
        for source in sources.into_iter().rev() {
            pending.push((source, Some(source_layer), false));
        }
    }
    Ok(originals)
}

impl Store {
    /// Scope is the Store's authenticated individual, never a tool argument.
    /// This reads only transcripts, not shared workspace records or opaque
    /// provider continuation state. A missing search match is not proof that
    /// the unredacted transcript lacks the query.
    pub(crate) async fn recall_messages(&self, request: &RecallRequest) -> Result<RecallPage> {
        request.validate()?;
        let mut transaction = self.pool.begin().await?;
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT m.id, m.seq, length(m.raw_ciphertext) AS raw_bytes
             FROM messages m WHERE 1=1",
        );
        if let Some(id) = &request.message_id {
            query.push(" AND m.id = ").push_bind(id);
        }
        if let Some(id) = &request.batch_id {
            let sources = original_batches(&mut transaction, id).await?;
            query
                .push(" AND m.id IN (SELECT message_id FROM memory_batch_messages WHERE batch_id IN (SELECT value FROM json_each(")
                .push_bind(serde_json::to_string(&sources)?)
                .push(")))");
        }
        if let Some(seq) = request.from_seq {
            query.push(" AND m.seq >= ").push_bind(seq as i64);
        }
        if let Some(seq) = request.after_seq {
            query.push(" AND m.seq > ").push_bind(seq as i64);
        }
        if let Some(text) = &request.query {
            if text.chars().count() < 3 {
                let escaped = text
                    .replace('\\', "\\\\")
                    .replace('%', "\\%")
                    .replace('_', "\\_");
                query
                    .push(" AND m.search_text LIKE ")
                    .push_bind(format!("%{escaped}%"))
                    .push(" ESCAPE '\\'");
            } else {
                query
                    .push(
                        " AND m.rowid IN (SELECT rowid FROM messages_fts WHERE messages_fts MATCH ",
                    )
                    .push_bind(format!("\"{}\"", text.replace('"', "\"\"")))
                    .push(")");
            }
        }
        query
            .push(" ORDER BY m.seq LIMIT ")
            .push_bind((request.limit + 1) as i64);
        let rows = query
            .build()
            .fetch_all(&mut *transaction)
            .await
            .context("failed to locate private experience")?;
        let mut page = RecallPage {
            messages: Vec::new(),
            next_after_seq: None,
        };
        let mut bytes = 0_usize;
        for row in rows {
            let raw_bytes = usize::try_from(row.try_get::<i64, _>("raw_bytes")?)?;
            if page.messages.len() == request.limit
                || (request.query.is_none()
                    && !page.messages.is_empty()
                    && bytes.saturating_add(raw_bytes) > RECALL_PAGE_BYTES)
            {
                page.next_after_seq = page.messages.last().map(|message| message.source.seq);
                break;
            }
            let id: String = row.try_get("id")?;
            let row = sqlx::query(
                "SELECT m.*, b.batch_id FROM messages m
                 LEFT JOIN memory_batch_messages b ON b.message_id = m.id WHERE m.id = ?",
            )
            .bind(&id)
            .fetch_one(&mut *transaction)
            .await?;
            let key = self
                .data_key_by_ref_in_transaction(
                    &mut transaction,
                    &row.try_get::<String, _>("raw_key_ref")?,
                )
                .await?;
            let message = decrypt_transcript_row(&row, &key, &self.scope, &self.redactor)?;
            bytes = bytes.saturating_add(raw_bytes);
            page.messages.push(RecalledMessage {
                source: RecallSource {
                    message_id: id,
                    seq: u64::try_from(row.try_get::<i64, _>("seq")?)?,
                    batch_id: row.try_get("batch_id")?,
                    stored_at: row.try_get("created_at")?,
                },
                message,
                search_text: row.try_get("search_text")?,
            });
        }
        transaction.commit().await?;
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::store::{
        AgentScope, DataKeyPurpose, DurableEvent, EventBatch, EventWrite, EventWriter,
        MemoryBatchMutation, MemoryBatchState, MemoryTransition, Projection,
        transcript::TranscriptRecord,
    };

    const INDIVIDUAL: &str = "0198f0f4-9b72-7000-8000-000000000001";
    const OTHER: &str = "0198f0f4-9b72-7000-8000-000000000002";

    async fn store() -> Store {
        Store::session_test_store(INDIVIDUAL).await.unwrap()
    }

    fn original(text: &str) -> PublicMessage {
        serde_json::from_value(json!({
            "role":"tool_result", "tool_call_id":"observation-call-42", "tool_name":"messaging",
            "content":[{"type":"text", "text":text},
                {"type":"image", "data":"aW1hZ2UtYnl0ZXM=", "mime_type":"image/png"}],
            "details":{"message_id":"shared-original-17", "sender_id":"person-8"},
            "is_error":false, "timestamp":"2026-09-01T02:03:04.123456789Z"
        }))
        .unwrap()
    }

    async fn insert(store: &Store, id: &str, seq: u64, message: &PublicMessage) {
        let key = store.private_key(DataKeyPurpose::Transcript).await.unwrap();
        TranscriptRecord::encrypt(message, id, seq, &key, store.scope(), &store.redactor)
            .unwrap()
            .insert(store.pool())
            .await
            .unwrap();
    }

    async fn transition(store: &Store, transition: MemoryTransition) {
        EventWriter::new(Arc::new(store.clone()))
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(DurableEvent::memory_maintenance("recall_fixture").unwrap()),
                    projections: vec![Projection::MemoryTransition(transition)],
                }],
                injected_commands: vec![],
            })
            .await
            .unwrap();
    }

    async fn admit_user_experience(store: &Store, text: &str) -> (String, PublicMessage) {
        use crate::gateway::{Command, CommandEnvelope, CommandId, InboundCommand};
        use crate::provider::types::{UserContent, UserMessage};
        use crate::runtime::contracts::IncomingProvenance;
        use crate::store::{ApplicationKind, InjectedCommand, RunPhase};

        let command_id = Uuid::now_v7().to_string();
        let run_id = format!("recall-run-{command_id}");
        let turn_id = format!("recall-turn-{command_id}");
        let provenance = IncomingProvenance::new(
            "recall-tenant",
            store.scope().personality_agent_id.clone(),
            "human-1",
        )
        .unwrap();
        let writer = EventWriter::new(Arc::new(store.clone()));
        writer
            .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                seq: 1,
                command_id: CommandId::parse(&command_id).unwrap(),
                personality_agent_id: store.scope().personality_agent_id.clone(),
                provenance: provenance.clone(),
                command: Command::UserMessage {
                    text: text.to_owned(),
                    attachments: vec![],
                },
            }))
            .await
            .unwrap();
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: command_id.clone(),
                        application_kind: ApplicationKind::IdleRun,
                        run_id: run_id.clone(),
                        turn_id: turn_id.clone(),
                    }],
                }],
                injected_commands: vec![],
            })
            .await
            .unwrap();
        let received_at: String =
            sqlx::query_scalar("SELECT received_at FROM inbound_commands WHERE command_id = ?")
                .bind(&command_id)
                .fetch_one(store.pool())
                .await
                .unwrap();
        let message = PublicMessage::User(UserMessage {
            incoming_source: None,
            incoming_timing: writer
                .timing_for_command(&command_id)
                .await
                .expect("fixture timing")
                .1,
            content: vec![UserContent::Text {
                text: text.to_owned(),
            }],
            timestamp: chrono::DateTime::parse_from_rfc3339(&received_at)
                .unwrap()
                .with_timezone(&chrono::Utc),
        });
        let message_id =
            crate::store::user_message_id(&store.scope().personality_agent_id, command_id.as_str());
        let mut writes = [
            (
                DurableEvent::agent_start(&run_id).unwrap(),
                RunPhase::Classified,
                RunPhase::RunStarted,
            ),
            (
                DurableEvent::turn_start(&run_id, &turn_id).unwrap(),
                RunPhase::RunStarted,
                RunPhase::TurnStarted,
            ),
            (
                DurableEvent::message("message_start", &message_id, &message).unwrap(),
                RunPhase::TurnStarted,
                RunPhase::UserStarted,
            ),
        ]
        .into_iter()
        .map(|(event, expected, next)| EventWrite {
            event: Some(event),
            projections: vec![Projection::RunPhase {
                command_id: command_id.clone(),
                run_id: run_id.clone(),
                expected,
                next,
            }],
        })
        .collect::<Vec<_>>();
        writes.push(EventWrite {
            event: Some(DurableEvent::message("message_end", &message_id, &message).unwrap()),
            projections: vec![
                Projection::MessageEnd {
                    message_id: message_id.clone(),
                    role: "user",
                    message: message.clone(),
                    append_to_l0: true,
                    provider_context: vec![],
                    eviction_footprint_tokens: 0,
                },
                Projection::RunPhase {
                    command_id: command_id.clone(),
                    run_id,
                    expected: RunPhase::UserStarted,
                    next: RunPhase::UserCommitted,
                },
            ],
        });
        writer
            .apply(EventBatch {
                writes,
                injected_commands: vec![InjectedCommand::new(
                    1,
                    CommandId::parse(&command_id).unwrap(),
                    provenance,
                )],
            })
            .await
            .unwrap();
        (message_id, message)
    }

    async fn lineage_batch(store: &Store, layer: i64, seq: i64) -> String {
        EventWriter::new(Arc::new(store.clone()))
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(DurableEvent::memory_maintenance("lineage_fixture").unwrap()),
                    projections: vec![],
                }],
                injected_commands: vec![],
            })
            .await
            .unwrap();
        let id = Uuid::now_v7().to_string();
        // Lineage is trusted SQLite metadata, not a synthetic signed producer.
        sqlx::query(
            "INSERT INTO memory_batches
             (id, layer, ord, batch_seq, state, est_tokens, updated_at,
              projection_event_seq, projection_digest)
             VALUES (?, ?, ?, ?, 'dropped', 0, '2026-09-08T00:00:00Z',
                     (SELECT MAX(seq) FROM agent_events), zeroblob(32))",
        )
        .bind(&id)
        .bind(layer)
        .bind(seq)
        .bind(seq)
        .execute(store.pool())
        .await
        .unwrap();
        id
    }

    async fn lineage_job(store: &Store, kind: &str, seq: i64, sources: &[&str]) {
        sqlx::query(
            "INSERT INTO memory_jobs
             (id, kind, batch_seq, source_ids, source_versions, status,
              created_at, updated_at, projection_event_seq, projection_digest)
             VALUES (?, ?, ?, ?, '[]', 'applied', '2026-09-08T00:00:00Z',
                     '2026-09-08T00:00:00Z', (SELECT MAX(seq) FROM agent_events), zeroblob(32))",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(kind)
        .bind(seq)
        .bind(serde_json::to_string(sources).unwrap())
        .execute(store.pool())
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn upper_lineage_pages_actual_originals_without_including_gaps() {
        let store = store().await;
        let first = lineage_batch(&store, 0, 100).await;
        let second = lineage_batch(&store, 0, 101).await;
        for (seq, batch) in [(1, Some(&first)), (2, None), (3, Some(&second))] {
            let id = format!("lineage-message-{seq}");
            insert(&store, &id, seq, &original(&id)).await;
            if let Some(batch) = batch {
                sqlx::query(
                    "INSERT INTO memory_batch_messages(batch_id, message_id, ord) VALUES (?, ?, 0)",
                )
                .bind(batch)
                .bind(&id)
                .execute(store.pool())
                .await
                .unwrap();
            }
        }
        let l1a = lineage_batch(&store, 1, 100).await;
        let l1b = lineage_batch(&store, 1, 101).await;
        lineage_job(&store, "compact_l0", 100, &[&first]).await;
        lineage_job(&store, "compact_l0", 101, &[&second]).await;
        let l2 = lineage_batch(&store, 2, 200).await;
        lineage_job(&store, "compact_l1", 200, &[&l1a, &l1b]).await;
        let deep = lineage_batch(&store, 2, 201).await;
        lineage_job(&store, "consolidate_l2", 201, &[&l2]).await;
        // A malformed unrelated graph must not affect this read.
        let unrelated = lineage_batch(&store, 2, 202).await;
        lineage_job(&store, "consolidate_l2", 202, &[&unrelated]).await;
        let first_page = store
            .recall_messages(&RecallRequest {
                batch_id: Some(deep.clone()),
                limit: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            first_page.messages[0].message,
            original("lineage-message-1")
        );
        assert_eq!(
            first_page.messages[0].source.batch_id.as_deref(),
            Some(first.as_str())
        );
        assert_eq!(first_page.next_after_seq, Some(1));
        let next = store
            .recall_messages(&RecallRequest {
                batch_id: Some(deep),
                after_seq: first_page.next_after_seq,
                limit: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(next.messages[0].message, original("lineage-message-3"));
        assert_eq!(next.next_after_seq, None);
    }

    #[tokio::test]
    async fn invalid_requested_lineage_is_reported_instead_of_partial_history() {
        let store = store().await;
        let missing = lineage_batch(&store, 1, 301).await;
        let empty = lineage_batch(&store, 1, 302).await;
        lineage_job(&store, "compact_l0", 302, &[]).await;
        let absent_source = lineage_batch(&store, 1, 303).await;
        lineage_job(&store, "compact_l0", 303, &[&Uuid::now_v7().to_string()]).await;
        let invalid_source = lineage_batch(&store, 1, 304).await;
        lineage_job(&store, "compact_l0", 304, &["not-an-id"]).await;
        let ambiguous = lineage_batch(&store, 2, 305).await;
        lineage_job(&store, "compact_l1", 305, &[&missing]).await;
        lineage_job(&store, "consolidate_l2", 305, &[&ambiguous]).await;
        let cycle_a = lineage_batch(&store, 2, 306).await;
        let cycle_b = lineage_batch(&store, 2, 307).await;
        lineage_job(&store, "consolidate_l2", 306, &[&cycle_b]).await;
        lineage_job(&store, "consolidate_l2", 307, &[&cycle_a]).await;
        for (batch, error) in [
            (missing, "producer"),
            (empty, "identities"),
            (absent_source, "source batch"),
            (invalid_source, "identities"),
            (ambiguous, "ambiguous"),
            (cycle_a, "cyclic"),
        ] {
            let result = store
                .recall_messages(&RecallRequest {
                    batch_id: Some(batch),
                    limit: 5,
                    ..Default::default()
                })
                .await
                .unwrap_err();
            assert!(result.to_string().contains(error), "{result:#}");
        }
    }

    #[tokio::test]
    async fn original_experience_remains_readable_after_leaving_active_l0() {
        let store = store().await;
        let (message_id, message) = admit_user_experience(
            &store,
            "『今回は結論だけでなく、迷った理由も聞きたい』\nそのままの言葉。",
        )
        .await;
        let row = sqlx::query(
            "SELECT b.id, b.version, m.seq FROM memory_batches b
             JOIN memory_batch_messages membership ON membership.batch_id = b.id
             JOIN messages m ON m.id = membership.message_id WHERE m.id = ?",
        )
        .bind(&message_id)
        .fetch_one(store.pool())
        .await
        .unwrap();
        let batch: Uuid = row.try_get::<String, _>("id").unwrap().parse().unwrap();
        let version = u64::try_from(row.try_get::<i64, _>("version").unwrap()).unwrap();
        let message_seq = u64::try_from(row.try_get::<i64, _>("seq").unwrap()).unwrap();

        let before = store
            .recall_messages(&RecallRequest {
                batch_id: Some(batch.to_string()),
                limit: 5,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(before.messages.len(), 1);
        assert_eq!(before.messages[0].message, message);

        transition(
            &store,
            MemoryTransition {
                batch_mutations: vec![MemoryBatchMutation {
                    batch_id: batch,
                    expected_version: version,
                    new_state: MemoryBatchState::Dropped,
                    summary: None,
                    est_tokens: 0,
                    footprint_delta: 0,
                }],
                ..Default::default()
            },
        )
        .await;
        let retained_sources: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM memory_batch_messages")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(retained_sources, 1);
        let reread_batch = store
            .recall_messages(&RecallRequest {
                batch_id: Some(batch.to_string()),
                limit: 5,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(reread_batch.messages[0].message, message);
        assert_eq!(
            reread_batch.messages[0].source.batch_id,
            Some(batch.to_string())
        );
        let after = store
            .recall_messages(&RecallRequest {
                message_id: Some(message_id.clone()),
                limit: 5,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(after.messages[0].message, message);
        assert_eq!(after.messages[0].source.seq, message_seq);

        let search = store
            .recall_messages(&RecallRequest {
                query: Some("迷った理由".to_owned()),
                limit: 5,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(search.messages[0].source.message_id, message_id);
    }

    #[tokio::test]
    async fn recall_authenticates_individual_scope_even_when_given_a_foreign_pool() {
        let own = store().await;
        let other = Store::session_test_store(OTHER).await.unwrap();
        insert(&own, "same-id", 1, &original("my observation")).await;
        insert(
            &other,
            "same-id",
            1,
            &original("another individual's observation"),
        )
        .await;
        let request = RecallRequest {
            message_id: Some("same-id".to_owned()),
            limit: 1,
            ..Default::default()
        };
        assert_eq!(
            own.recall_messages(&request).await.unwrap().messages[0].message,
            original("my observation")
        );
        let mut mismatched = own.clone();
        mismatched.scope = AgentScope::new(OTHER.parse().unwrap());
        assert!(
            mismatched
                .recall_messages(&request)
                .await
                .unwrap_err()
                .to_string()
                .contains("another personality")
        );
        let search = RecallRequest {
            query: Some("observation".to_owned()),
            limit: 5,
            ..Default::default()
        };
        assert!(mismatched.recall_messages(&search).await.is_err());
    }

    #[tokio::test]
    async fn redacted_literal_search_pages_without_skipping_and_reads_raw_fields() {
        let store = store().await;
        for seq in 1..=3 {
            insert(
                &store,
                &format!("message-{seq}"),
                seq,
                &original("過去の言葉は100%そのまま。sk-abcdefghijklmnop"),
            )
            .await;
        }
        for query in ["過去", "過去の言葉", "%", "100%"] {
            let first = store
                .recall_messages(&RecallRequest {
                    query: Some(query.to_owned()),
                    limit: 2,
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(first.messages.len(), 2);
            assert_eq!(first.next_after_seq, Some(2));
            let next = store
                .recall_messages(&RecallRequest {
                    query: Some(query.to_owned()),
                    after_seq: first.next_after_seq,
                    limit: 2,
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(next.messages[0].source.seq, 3);
            assert_eq!(next.next_after_seq, None);
        }
        for omitted in ["sk-abcdefghijklmnop", "aW1hZ2UtYnl0ZXM=", "person-8"] {
            assert!(
                store
                    .recall_messages(&RecallRequest {
                        query: Some(omitted.to_owned()),
                        limit: 5,
                        ..Default::default()
                    })
                    .await
                    .unwrap()
                    .messages
                    .is_empty()
            );
        }
        let read = store
            .recall_messages(&RecallRequest {
                from_seq: Some(3),
                limit: 5,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            read.messages[0].message,
            original("過去の言葉は100%そのまま。sk-abcdefghijklmnop")
        );
    }

    #[tokio::test]
    async fn whole_message_pages_do_not_cut_large_words_or_images() {
        let store = store().await;
        let large = original(&"あ".repeat(RECALL_PAGE_BYTES));
        insert(&store, "large", 1, &large).await;
        insert(&store, "next", 2, &original("next observation")).await;
        let page = store
            .recall_messages(&RecallRequest {
                limit: 5,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.messages.len(), 1);
        assert_eq!(page.messages[0].message, large);
        assert_eq!(page.next_after_seq, Some(1));
        let next = store
            .recall_messages(&RecallRequest {
                after_seq: page.next_after_seq,
                limit: 5,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(next.messages[0].source.message_id, "next");
    }

    #[tokio::test]
    async fn oversized_original_can_be_reread_in_bounded_fragments_without_losing_details_or_images()
     {
        use crate::provider::types::{UserContent, ValidatedToolArguments};
        use crate::tools::memory::ConversationHistoryTool;
        use crate::tools::{Tool, ToolCtx, ToolError, WorkspacePaths};
        use tokio_util::sync::CancellationToken;

        let store = store().await;
        // This text alone is 768 KiB in UTF-8, before the independently large
        // tool details. limit=1 must still yield a usable bounded response.
        let mut message = original(&"あ".repeat(256 * 1024));
        if let PublicMessage::ToolResult(tool_result) = &mut message {
            tool_result.details["long_details"] = json!({
                "observations":["詳細🙂\n".repeat(16 * 1024), "last detail"],
            });
        }
        insert(&store, "large-original", 1, &message).await;
        insert(
            &store,
            "later-original",
            2,
            &original("the later observation"),
        )
        .await;
        let tool = ConversationHistoryTool::new(store.clone());
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        let mut request = json!({"operation":"read","from_seq":1,"limit":1});
        let mut reconstructed = String::new();
        let mut image = None;
        let mut page_count = 0;
        let mut next_offset = 0;
        loop {
            let args: ValidatedToolArguments = serde_json::from_value(request).unwrap();
            let output = Tool::execute(
                &tool,
                ToolCtx {
                    flow_id: "private-recall",
                    call_id: "large-message-page",
                    args: &args,
                    cancel: CancellationToken::new(),
                    on_update: Arc::new(|_| {}),
                    workspace: &workspace,
                },
            )
            .await
            .unwrap();
            let UserContent::Text { text } = &output.content[0] else {
                panic!("metadata text");
            };
            assert!(
                text.len() < 128 * 1024,
                "one returned text page was {} bytes",
                text.len()
            );
            let item = &output.details["messages"][0];
            assert_eq!(item["source"]["message_id"], "large-original");
            assert_eq!(item["content_complete"], false);
            assert!(
                item.get("message").is_none(),
                "partial read must not also include full original"
            );
            assert_eq!(item["message_json_range"]["start_char"], next_offset);
            let fragment = item["message_json_fragment"].as_str().unwrap();
            assert!(fragment.chars().count() <= 16 * 1024);
            reconstructed.push_str(fragment);
            next_offset += fragment.chars().count();
            assert_eq!(item["message_json_range"]["end_char"], next_offset);
            if page_count == 0 {
                assert_eq!(output.content.len(), 2);
                image = Some(output.content[1].clone());
                assert_eq!(item["images_delivered"], true);
            } else {
                assert_eq!(
                    output.content.len(),
                    1,
                    "native image must not be repeated with every suffix"
                );
                assert_eq!(item["images_delivered"], false);
                assert_eq!(item["image_count"], 1);
            }
            page_count += 1;
            assert!(page_count < 100, "continuation must advance");
            if output.details["next_read"].is_null() {
                assert_eq!(item["message_json_range"]["ends_message"], true);
                assert_eq!(item["message_json_range"]["total_chars"], next_offset);
                assert_eq!(item["resume_after_seq"], 1);
                break;
            }
            assert_eq!(item["message_json_range"]["ends_message"], false);
            request = output.details["next_read"].clone();
            assert_eq!(request["message_id"], "large-original");
            assert_eq!(request["content_offset"], next_offset);
        }
        assert!(page_count > 1);
        let mut decoded: serde_json::Value = serde_json::from_str(&reconstructed).unwrap();
        assert_eq!(decoded["content"][1]["image_index"], 0);
        decoded["content"][1] = serde_json::to_value(image.unwrap()).unwrap();
        assert_eq!(decoded, serde_json::to_value(&message).unwrap());
        assert_eq!(
            store
                .recall_messages(&RecallRequest {
                    message_id: Some("large-original".to_owned()),
                    limit: 1,
                    ..Default::default()
                })
                .await
                .unwrap()
                .messages[0]
                .message,
            message
        );

        let invalid: ValidatedToolArguments = serde_json::from_value(json!({
            "operation":"read","message_id":"large-original","content_offset":next_offset + 1,
        }))
        .unwrap();
        assert!(matches!(
            Tool::execute(
                &tool,
                ToolCtx {
                    flow_id: "private-recall",
                    call_id: "invalid-range",
                    args: &invalid,
                    cancel: CancellationToken::new(),
                    on_update: Arc::new(|_| {}),
                    workspace: &workspace,
                }
            )
            .await
            .unwrap_err(),
            ToolError::InvalidArguments
        ));

        let later: ValidatedToolArguments = serde_json::from_value(json!({
            "operation":"read","from_seq":1,"after_seq":1,"limit":1,
        }))
        .unwrap();
        let output = Tool::execute(
            &tool,
            ToolCtx {
                flow_id: "private-recall",
                call_id: "resume-outer-page",
                args: &later,
                cancel: CancellationToken::new(),
                on_update: Arc::new(|_| {}),
                workspace: &workspace,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            output.details["messages"][0]["source"]["message_id"],
            "later-original"
        );
        assert_eq!(output.details["messages"][0]["content_complete"], true);
    }

    #[tokio::test]
    async fn unauthenticated_projection_is_never_returned_as_experience() {
        let store = store().await;
        insert(&store, "message", 1, &original("original words")).await;
        sqlx::query("UPDATE messages SET search_text = 'forged observation' WHERE id = 'message'")
            .execute(store.pool())
            .await
            .unwrap();
        assert!(
            store
                .recall_messages(&RecallRequest {
                    query: Some("forged".to_owned()),
                    limit: 5,
                    ..Default::default()
                })
                .await
                .is_err()
        );
    }
}
