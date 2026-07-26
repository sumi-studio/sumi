//! Durable memory layer state: batches, membership, compaction jobs, and
//! apply cursors.  This module owns the `memory_*` table shapes and the
//! transactional CAS primitives used by the memory maintainer.  It deliberately
//! does not contain compaction algorithms or L0/L1/L2 policy logic (T19-T21).

#![allow(dead_code)]

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use chrono::Utc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MemoryLayer {
    L0 = 0,
    L1 = 1,
    L2 = 2,
}

impl MemoryLayer {
    fn as_i64(self) -> i64 {
        self as i64
    }

    pub(crate) fn from_i64(value: i64) -> Option<Self> {
        match value {
            0 => Some(Self::L0),
            1 => Some(Self::L1),
            2 => Some(Self::L2),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MemoryBatchState {
    Open,
    Sealed,
    Compacting,
    CompactFailed,
    Compacted,
    Promoted,
    Dropped,
}

impl MemoryBatchState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Sealed => "sealed",
            Self::Compacting => "compacting",
            Self::CompactFailed => "compact_failed",
            Self::Compacted => "compacted",
            Self::Promoted => "promoted",
            Self::Dropped => "dropped",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "open" => Some(Self::Open),
            "sealed" => Some(Self::Sealed),
            "compacting" => Some(Self::Compacting),
            "compact_failed" => Some(Self::CompactFailed),
            "compacted" => Some(Self::Compacted),
            "promoted" => Some(Self::Promoted),
            "dropped" => Some(Self::Dropped),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MemoryJobKind {
    CompactL0,
    CompactL1,
    ConsolidateL2,
}

impl MemoryJobKind {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::CompactL0 => "compact_l0",
            Self::CompactL1 => "compact_l1",
            Self::ConsolidateL2 => "consolidate_l2",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "compact_l0" => Some(Self::CompactL0),
            "compact_l1" => Some(Self::CompactL1),
            "consolidate_l2" => Some(Self::ConsolidateL2),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MemoryJobStatus {
    Pending,
    Running,
    Completed,
    Applied,
    Failed,
}

impl MemoryJobStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Applied => "applied",
            Self::Failed => "failed",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "applied" => Some(Self::Applied),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MemoryBatchSummary {
    pub key_ref: String,
    pub ciphertext: Vec<u8>,
    pub projection: String,
    pub redaction_version: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct MemoryBatchRecord {
    pub id: String,
    pub layer: MemoryLayer,
    pub ord: i64,
    pub batch_seq: i64,
    pub version: i64,
    pub state: MemoryBatchState,
    pub est_tokens: i64,
    pub eviction_footprint_tokens: i64,
    pub summary: Option<MemoryBatchSummary>,
    pub updated_at: String,
}

impl MemoryBatchRecord {
    pub(crate) fn new(
        id: impl Into<String>,
        layer: MemoryLayer,
        ord: i64,
        batch_seq: i64,
        state: MemoryBatchState,
        est_tokens: i64,
        eviction_footprint_tokens: i64,
    ) -> Self {
        Self {
            id: id.into(),
            layer,
            ord,
            batch_seq,
            version: 0,
            state,
            est_tokens,
            eviction_footprint_tokens,
            summary: None,
            updated_at: Utc::now().to_rfc3339(),
        }
    }

    pub(crate) async fn insert<'e, E>(&self, executor: E) -> Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let (key_ref, ciphertext, projection, redaction_version) = match &self.summary {
            Some(s) => (
                Some(&s.key_ref),
                Some(&s.ciphertext),
                Some(&s.projection),
                Some(i64::from(s.redaction_version)),
            ),
            None => (None, None, None, None),
        };

        sqlx::query(
            "INSERT INTO memory_batches(
                id, layer, ord, batch_seq, version, state, est_tokens,
                eviction_footprint_tokens, summary_key_ref, summary_ciphertext,
                summary_projection, summary_redaction_version, updated_at
             ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&self.id)
        .bind(self.layer.as_i64())
        .bind(self.ord)
        .bind(self.batch_seq)
        .bind(self.version)
        .bind(self.state.as_str())
        .bind(self.est_tokens)
        .bind(self.eviction_footprint_tokens)
        .bind(key_ref)
        .bind(ciphertext)
        .bind(projection)
        .bind(redaction_version)
        .bind(&self.updated_at)
        .execute(executor)
        .await
        .context("failed to insert memory batch")?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MemoryBatchMessageRecord {
    pub batch_id: String,
    pub message_id: String,
    pub ord: i64,
}

impl MemoryBatchMessageRecord {
    pub(crate) async fn insert<'e, E>(&self, executor: E) -> Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        sqlx::query(
            "INSERT INTO memory_batch_messages(batch_id, message_id, ord)
             VALUES(?, ?, ?)",
        )
        .bind(&self.batch_id)
        .bind(&self.message_id)
        .bind(self.ord)
        .execute(executor)
        .await
        .context("failed to insert memory batch message")?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MemoryJobResult {
    pub key_ref: String,
    pub ciphertext: Vec<u8>,
    pub projection: String,
    pub redaction_version: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct MemoryJobRecord {
    pub id: String,
    pub kind: MemoryJobKind,
    pub batch_seq: i64,
    pub source_ids: Vec<String>,
    pub source_versions: BTreeMap<String, i64>,
    pub status: MemoryJobStatus,
    pub attempts: i64,
    pub result: Option<MemoryJobResult>,
    pub created_at: String,
    pub updated_at: String,
}

impl MemoryJobRecord {
    pub(crate) fn new(
        id: impl Into<String>,
        kind: MemoryJobKind,
        batch_seq: i64,
        source_ids: Vec<String>,
        source_versions: BTreeMap<String, i64>,
    ) -> Self {
        let now = Utc::now().to_rfc3339();
        Self {
            id: id.into(),
            kind,
            batch_seq,
            source_ids,
            source_versions,
            status: MemoryJobStatus::Pending,
            attempts: 0,
            result: None,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    pub(crate) async fn insert<'e, E>(&self, executor: E) -> Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let (key_ref, ciphertext, projection, redaction_version) = match &self.result {
            Some(r) => (
                Some(&r.key_ref),
                Some(&r.ciphertext),
                Some(&r.projection),
                Some(i64::from(r.redaction_version)),
            ),
            None => (None, None, None, None),
        };

        let source_ids_json =
            serde_json::to_string(&self.source_ids).context("failed to serialize source_ids")?;
        let source_versions_json = serde_json::to_string(&self.source_versions)
            .context("failed to serialize source_versions")?;

        sqlx::query(
            "INSERT INTO memory_jobs(
                id, kind, batch_seq, source_ids, source_versions, status, attempts,
                result_key_ref, result_ciphertext, result_projection,
                result_redaction_version, created_at, updated_at
             ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&self.id)
        .bind(self.kind.as_str())
        .bind(self.batch_seq)
        .bind(&source_ids_json)
        .bind(&source_versions_json)
        .bind(self.status.as_str())
        .bind(self.attempts)
        .bind(key_ref)
        .bind(ciphertext)
        .bind(projection)
        .bind(redaction_version)
        .bind(&self.created_at)
        .bind(&self.updated_at)
        .execute(executor)
        .await
        .context("failed to insert memory job")?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MemoryApplyCursorRecord {
    pub kind: String,
    pub next_batch_seq: i64,
}

impl MemoryApplyCursorRecord {
    pub(crate) async fn insert<'e, E>(&self, executor: E) -> Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        sqlx::query(
            "INSERT INTO memory_apply_cursors(kind, next_batch_seq)
             VALUES(?, ?)
             ON CONFLICT(kind) DO UPDATE SET
                next_batch_seq = excluded.next_batch_seq
             WHERE excluded.next_batch_seq >= memory_apply_cursors.next_batch_seq",
        )
        .bind(&self.kind)
        .bind(self.next_batch_seq)
        .execute(executor)
        .await
        .context("failed to upsert memory apply cursor")?;
        Ok(())
    }

    /// CAS advance: succeeds only when `expected` matches the stored value.
    pub(crate) async fn advance<'e, E>(&self, executor: E, expected: i64) -> Result<bool>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        let updated = sqlx::query(
            "UPDATE memory_apply_cursors
             SET next_batch_seq = ?
             WHERE kind = ? AND next_batch_seq = ? AND ? >= next_batch_seq",
        )
        .bind(self.next_batch_seq)
        .bind(&self.kind)
        .bind(expected)
        .bind(self.next_batch_seq)
        .execute(executor)
        .await
        .context("failed to advance memory apply cursor")?;
        Ok(updated.rows_affected() == 1)
    }
}

/// CAS primitive: update a batch state and version only when the expected
/// version matches.  Returns `true` when the row was updated.
pub(crate) async fn update_batch_state_version<'e, E>(
    executor: E,
    batch_id: &str,
    expected_version: i64,
    new_state: MemoryBatchState,
    new_est_tokens: i64,
) -> Result<bool>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let updated = sqlx::query(
        "UPDATE memory_batches
         SET state = ?, version = version + 1, est_tokens = ?, updated_at = ?
         WHERE id = ? AND version = ?",
    )
    .bind(new_state.as_str())
    .bind(new_est_tokens)
    .bind(Utc::now().to_rfc3339())
    .bind(batch_id)
    .bind(expected_version)
    .execute(executor)
    .await
    .context("failed to CAS-update memory batch")?;
    Ok(updated.rows_affected() == 1)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use sqlx::Row;

    use super::*;
    use crate::store::{DataKeyPurpose, Store};

    async fn store() -> Store {
        Store::session_test_store("conversation-1")
            .await
            .expect("open test store")
    }

    #[tokio::test]
    async fn migration_enforces_memory_batch_constraints() {
        let store = store().await;

        let invalid_layer = sqlx::query(
            "INSERT INTO memory_batches(
                id, layer, ord, batch_seq, version, state, est_tokens,
                eviction_footprint_tokens, updated_at
             ) VALUES('batch-1', 5, 0, 0, 0, 'open', 0, 0, 'now')",
        )
        .execute(store.pool())
        .await;
        assert!(invalid_layer.is_err());

        let invalid_state = sqlx::query(
            "INSERT INTO memory_batches(
                id, layer, ord, batch_seq, version, state, est_tokens,
                eviction_footprint_tokens, updated_at
             ) VALUES('batch-2', 0, 0, 0, 0, 'unknown', 0, 0, 'now')",
        )
        .execute(store.pool())
        .await;
        assert!(invalid_state.is_err());

        let negative_tokens = sqlx::query(
            "INSERT INTO memory_batches(
                id, layer, ord, batch_seq, version, state, est_tokens,
                eviction_footprint_tokens, updated_at
             ) VALUES('batch-3', 0, 0, 0, 0, 'open', -1, 0, 'now')",
        )
        .execute(store.pool())
        .await;
        assert!(negative_tokens.is_err());

        let partial_summary = sqlx::query(
            "INSERT INTO memory_batches(
                id, layer, ord, batch_seq, version, state, est_tokens,
                eviction_footprint_tokens, summary_key_ref, summary_ciphertext,
                summary_projection, summary_redaction_version, updated_at
             ) VALUES('batch-4', 0, 0, 0, 0, 'open', 0, 0, 'key', NULL, 'proj', 1, 'now')",
        )
        .execute(store.pool())
        .await;
        assert!(partial_summary.is_err());
    }

    #[tokio::test]
    async fn memory_batch_record_round_trip_and_unique_layer_batch_seq() {
        let store = store().await;
        let batch = MemoryBatchRecord::new(
            "batch-1",
            MemoryLayer::L0,
            0,
            1,
            MemoryBatchState::Open,
            100,
            20,
        );
        batch.insert(store.pool()).await.expect("insert batch");

        let row = sqlx::query("SELECT * FROM memory_batches WHERE id = ?")
            .bind("batch-1")
            .fetch_one(store.pool())
            .await
            .expect("fetch batch");
        assert_eq!(row.get::<i64, _>("layer"), 0);
        assert_eq!(row.get::<i64, _>("batch_seq"), 1);
        assert_eq!(row.get::<String, _>("state"), "open");
        assert_eq!(row.get::<i64, _>("est_tokens"), 100);
        assert_eq!(row.get::<i64, _>("eviction_footprint_tokens"), 20);

        let duplicate = MemoryBatchRecord::new(
            "batch-2",
            MemoryLayer::L0,
            1,
            1,
            MemoryBatchState::Sealed,
            0,
            0,
        );
        assert!(duplicate.insert(store.pool()).await.is_err());
    }

    #[tokio::test]
    async fn memory_batch_message_enforces_membership_and_orphan_rejection() {
        let store = store().await;

        let key = store
            .conversation_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint transcript key");
        sqlx::query(
            "INSERT INTO messages(
                id, seq, role, raw_key_ref, raw_ciphertext, payload, search_text,
                redaction_version, interrupted, created_at
             ) VALUES('message-1', 1, 'user', ?, X'00', '{}', '', 1, 0, 'now')",
        )
        .bind(&key.key_ref)
        .execute(store.pool())
        .await
        .expect("seed message");

        let batch = MemoryBatchRecord::new(
            "batch-1",
            MemoryLayer::L0,
            0,
            1,
            MemoryBatchState::Open,
            0,
            0,
        );
        batch.insert(store.pool()).await.unwrap();

        let orphan = MemoryBatchMessageRecord {
            batch_id: "missing-batch".to_owned(),
            message_id: "message-1".to_owned(),
            ord: 0,
        };
        assert!(orphan.insert(store.pool()).await.is_err());

        let member = MemoryBatchMessageRecord {
            batch_id: "batch-1".to_owned(),
            message_id: "message-1".to_owned(),
            ord: 0,
        };
        member.insert(store.pool()).await.unwrap();

        let duplicate_message = MemoryBatchMessageRecord {
            batch_id: "batch-1".to_owned(),
            message_id: "message-1".to_owned(),
            ord: 1,
        };
        assert!(duplicate_message.insert(store.pool()).await.is_err());
    }

    #[tokio::test]
    async fn memory_job_unique_kind_batch_seq_and_status_check() {
        let store = store().await;
        let job = MemoryJobRecord::new(
            "job-1",
            MemoryJobKind::CompactL0,
            1,
            vec!["batch-1".to_owned()],
            BTreeMap::new(),
        );
        job.insert(store.pool()).await.unwrap();

        let duplicate = MemoryJobRecord::new(
            "job-2",
            MemoryJobKind::CompactL0,
            1,
            vec!["batch-2".to_owned()],
            BTreeMap::new(),
        );
        assert!(duplicate.insert(store.pool()).await.is_err());

        let invalid_status = sqlx::query(
            "INSERT INTO memory_jobs(
                id, kind, batch_seq, source_ids, source_versions, status, attempts,
                created_at, updated_at
             ) VALUES('job-bad', 'compact_l0', 2, '[]', '{}', 'unknown', 0, 'now', 'now')",
        )
        .execute(store.pool())
        .await;
        assert!(invalid_status.is_err());
    }

    #[tokio::test]
    async fn memory_apply_cursor_cas_advances_monotonically() {
        let store = store().await;
        let cursor = MemoryApplyCursorRecord {
            kind: "l1".to_owned(),
            next_batch_seq: 0,
        };
        cursor.insert(store.pool()).await.unwrap();

        let advanced = MemoryApplyCursorRecord {
            kind: "l1".to_owned(),
            next_batch_seq: 1,
        };
        assert!(advanced.advance(store.pool(), 0).await.unwrap());
        assert!(!advanced.advance(store.pool(), 0).await.unwrap());

        let current: i64 =
            sqlx::query_scalar("SELECT next_batch_seq FROM memory_apply_cursors WHERE kind = ?")
                .bind("l1")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(current, 1);
    }

    #[tokio::test]
    async fn batch_state_version_cas_rejects_stale_expected_version() {
        let store = store().await;
        let batch = MemoryBatchRecord::new(
            "batch-1",
            MemoryLayer::L0,
            0,
            1,
            MemoryBatchState::Open,
            0,
            0,
        );
        batch.insert(store.pool()).await.unwrap();

        assert!(
            update_batch_state_version(store.pool(), "batch-1", 0, MemoryBatchState::Sealed, 0,)
                .await
                .unwrap()
        );
        assert!(
            !update_batch_state_version(
                store.pool(),
                "batch-1",
                0,
                MemoryBatchState::Compacting,
                0,
            )
            .await
            .unwrap()
        );

        let row = sqlx::query("SELECT state, version FROM memory_batches WHERE id = ?")
            .bind("batch-1")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(row.get::<String, _>("state"), "sealed");
        assert_eq!(row.get::<i64, _>("version"), 1);
    }

    #[tokio::test]
    async fn memory_summary_requires_valid_data_key_foreign_key() {
        let store = store().await;
        let summary_key = store
            .conversation_key(DataKeyPurpose::MemorySummary)
            .await
            .expect("mint memory summary key");

        let batch = MemoryBatchRecord {
            id: "batch-1".to_owned(),
            layer: MemoryLayer::L2,
            ord: 0,
            batch_seq: 1,
            version: 0,
            state: MemoryBatchState::Compacted,
            est_tokens: 0,
            eviction_footprint_tokens: 0,
            summary: Some(MemoryBatchSummary {
                key_ref: summary_key.key_ref.clone(),
                ciphertext: vec![1, 2, 3],
                projection: "{}".to_owned(),
                redaction_version: 1,
            }),
            updated_at: Utc::now().to_rfc3339(),
        };
        batch.insert(store.pool()).await.unwrap();

        let invalid = MemoryBatchRecord {
            id: "batch-2".to_owned(),
            layer: MemoryLayer::L2,
            ord: 1,
            batch_seq: 2,
            version: 0,
            state: MemoryBatchState::Compacted,
            est_tokens: 0,
            eviction_footprint_tokens: 0,
            summary: Some(MemoryBatchSummary {
                key_ref: "missing-key".to_owned(),
                ciphertext: vec![1, 2, 3],
                projection: "{}".to_owned(),
                redaction_version: 1,
            }),
            updated_at: Utc::now().to_rfc3339(),
        };
        assert!(invalid.insert(store.pool()).await.is_err());
    }

    #[tokio::test]
    async fn memory_apply_cursor_insert_does_not_regress() {
        let store = store().await;

        let forward = MemoryApplyCursorRecord {
            kind: "layer-0".to_owned(),
            next_batch_seq: 10,
        };
        forward.insert(store.pool()).await.expect("insert cursor");

        let backward = MemoryApplyCursorRecord {
            kind: "layer-0".to_owned(),
            next_batch_seq: 5,
        };
        backward
            .insert(store.pool())
            .await
            .expect("no-op regress insert");

        let stored: i64 =
            sqlx::query_scalar("SELECT next_batch_seq FROM memory_apply_cursors WHERE kind = ?")
                .bind("layer-0")
                .fetch_one(store.pool())
                .await
                .expect("fetch cursor");
        assert_eq!(stored, 10);
    }
}
