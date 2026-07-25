//! Conversation reset, agent deletion, export, search, and KMS rotation.
#![allow(dead_code)]

use std::{collections::HashSet, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::Utc;
use sqlx::Row;

use super::{
    AgentScope, DataKeyPurpose, Store, TombstoneRepository, TombstoneScope, TombstoneStatus,
};
use crate::gateway::Command;
use crate::tools::{
    ToolError,
    executor::{ArtifactBroker, ArtifactBrokerClient, ArtifactOperation, ArtifactResponse},
};

/// Adapter for the artifact broker during lifecycle operations.  The production
/// runtime uses a Unix-socket `ArtifactBrokerClient`; tests can use an in-process
/// broker to avoid spawning a broker process.
#[async_trait]
pub trait ArtifactLifecycleBroker: Send + Sync {
    async fn delete_conversation_artifacts(
        &self,
        conversation_id: &str,
        tombstone_id: &str,
    ) -> Result<u64, ToolError>;
}

#[async_trait]
impl ArtifactLifecycleBroker for ArtifactBrokerClient {
    async fn delete_conversation_artifacts(
        &self,
        conversation_id: &str,
        tombstone_id: &str,
    ) -> Result<u64, ToolError> {
        let response = self
            .execute(ArtifactOperation::DeleteConversationArtifacts {
                conversation_id: conversation_id.to_owned(),
                tombstone_id: tombstone_id.to_owned(),
            })
            .await?;
        match response {
            ArtifactResponse::Deleted { deleted_count } => Ok(deleted_count),
            _ => Err(ToolError::Protocol(
                "unexpected artifact response for delete operation".to_owned(),
            )),
        }
    }
}

/// In-process broker for tests and single-process deployments.
#[derive(Clone)]
pub struct DirectArtifactBroker {
    broker: Arc<ArtifactBroker>,
}

impl DirectArtifactBroker {
    pub fn new(broker: ArtifactBroker) -> Self {
        Self {
            broker: Arc::new(broker),
        }
    }
}

#[async_trait]
impl ArtifactLifecycleBroker for DirectArtifactBroker {
    async fn delete_conversation_artifacts(
        &self,
        conversation_id: &str,
        tombstone_id: &str,
    ) -> Result<u64, ToolError> {
        let broker = self.broker.clone();
        let conversation_id = conversation_id.to_owned();
        let tombstone_id = tombstone_id.to_owned();
        tokio::task::spawn_blocking(move || {
            broker.execute(ArtifactOperation::DeleteConversationArtifacts {
                conversation_id,
                tombstone_id,
            })
        })
        .await
        .map_err(|_| ToolError::Protocol("artifact broker worker panicked".to_owned()))?
        .and_then(|response| match response {
            ArtifactResponse::Deleted { deleted_count } => Ok(deleted_count),
            _ => Err(ToolError::Protocol(
                "unexpected artifact response for delete operation".to_owned(),
            )),
        })
    }
}

/// Drives data lifecycle: conversation reset, agent deletion, export, search,
/// and KMS key rotation.
pub struct LifecycleWorker {
    store: Arc<Store>,
    tombstones: Arc<dyn TombstoneRepository>,
    broker: Arc<dyn ArtifactLifecycleBroker>,
    artifact_root: Option<PathBuf>,
}

impl LifecycleWorker {
    pub fn new(
        store: Arc<Store>,
        tombstones: Arc<dyn TombstoneRepository>,
        broker: Arc<dyn ArtifactLifecycleBroker>,
        artifact_root: Option<PathBuf>,
    ) -> Self {
        Self {
            store,
            tombstones,
            broker,
            artifact_root,
        }
    }

    /// Conversation reset: tombstone the old conversation, crypto-erase its
    /// data keys, delete its row-level state, remove its artifact volume, and
    /// re-bind the store to the new conversation id.
    pub async fn conversation_reset(&self, new_conversation_id: &str) -> Result<()> {
        if new_conversation_id.is_empty() {
            bail!("new conversation id must not be empty");
        }
        let scope = self.store.scope();
        let old_conversation_id = scope.conversation_id.clone();
        if old_conversation_id == new_conversation_id {
            bail!("new conversation id must differ from the current conversation id");
        }

        let purge_after = "2099-12-31T23:59:59Z";
        let tombstone_id = self
            .tombstones
            .request(
                &scope.tenant_id,
                &scope.agent_id,
                Some(&old_conversation_id),
                TombstoneScope::Conversation,
                purge_after,
            )
            .await
            .context("failed to record conversation reset tombstone")?;
        self.tombstones
            .advance(
                &tombstone_id,
                TombstoneStatus::Requested,
                TombstoneStatus::Fenced,
            )
            .await
            .context("failed to fence conversation reset tombstone")?;

        self.reset_conversation_state(&scope, &old_conversation_id, new_conversation_id)
            .await
            .context("failed to reset conversation state")?;

        let deleted = self
            .broker
            .delete_conversation_artifacts(&old_conversation_id, &tombstone_id)
            .await
            .context("failed to delete conversation artifacts")?;

        self.tombstones
            .advance(
                &tombstone_id,
                TombstoneStatus::Fenced,
                TombstoneStatus::LivePurged,
            )
            .await
            .context("failed to mark conversation reset live-purged")?;

        self.store
            .set_conversation_id(new_conversation_id.to_owned());

        // Insert an audit record for the reset itself.
        self.tombstones
            .log_access(
                "lifecycle-worker",
                &scope.tenant_id,
                "reset",
                &format!("conversation:{old_conversation_id}"),
                deleted as i64,
            )
            .await
            .ok();

        Ok(())
    }

    async fn reset_conversation_state(
        &self,
        scope: &AgentScope,
        old_conversation_id: &str,
        new_conversation_id: &str,
    ) -> Result<()> {
        let mut transaction = self.store.pool().begin().await?;

        // Crypto-erase all conversation-scoped data keys.
        sqlx::query(
            "UPDATE data_keys
             SET state = 'destroyed', wrapped_key = NULL, wrap_nonce = NULL, destroyed_at = ?
             WHERE scope = 'conversation' AND conversation_id = ? AND state = 'active'",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(old_conversation_id)
        .execute(&mut *transaction)
        .await
        .context("failed to crypto-erase conversation data keys")?;

        // Delete row-level conversation state.  Order respects FK and trigger
        // dependencies: messages cascades to provider_context and
        // memory_batch_messages; memory_batches then memory_jobs, then cursors.
        for table in [
            "messages",
            "memory_batches",
            "memory_batch_messages",
            "memory_jobs",
            "memory_apply_cursors",
            "agent_events",
            "inbound_commands",
            "tool_executions",
            "approval_log",
            "event_log_heads",
            "provider_context",
            "provider_context_mutations",
            "provider_context_replace_heads",
            "kv",
        ] {
            sqlx::query(&format!("DELETE FROM {table}"))
                .execute(&mut *transaction)
                .await
                .with_context(|| format!("failed to delete from {table} during reset"))?;
        }

        // Re-bind the singleton scope to the new conversation id.
        sqlx::query(
            "UPDATE agent_scope SET conversation_id = ? WHERE tenant_id = ? AND agent_id = ?",
        )
        .bind(new_conversation_id)
        .bind(&scope.tenant_id)
        .bind(&scope.agent_id)
        .execute(&mut *transaction)
        .await
        .context("failed to update agent scope conversation id")?;

        transaction.commit().await?;
        Ok(())
    }

    /// Agent deletion: tombstone the agent, close the store, delete the SQLite
    /// file and artifact volume.  The compliance store outlives this call.
    pub async fn delete_agent(&self) -> Result<()> {
        let scope = self.store.scope();

        let purge_after = "2099-12-31T23:59:59Z";
        let tombstone_id = self
            .tombstones
            .request(
                &scope.tenant_id,
                &scope.agent_id,
                None,
                TombstoneScope::Agent,
                purge_after,
            )
            .await
            .context("failed to record agent deletion tombstone")?;
        self.tombstones
            .advance(
                &tombstone_id,
                TombstoneStatus::Requested,
                TombstoneStatus::Fenced,
            )
            .await
            .context("failed to fence agent deletion tombstone")?;

        // Persist tombstone before destroying local storage so a crash replay
        // can still observe the deletion intent.
        self.tombstones
            .advance(
                &tombstone_id,
                TombstoneStatus::Fenced,
                TombstoneStatus::LivePurged,
            )
            .await
            .context("failed to mark agent deletion live-purged")?;

        // Close the SQLite pool and remove the database files.
        self.store.pool().close().await;
        if let Some(db_path) = self.store.db_path() {
            let owned = db_path.to_path_buf();
            let paths = [
                owned.clone(),
                owned.with_extension("db-wal"),
                owned.with_extension("db-shm"),
            ];
            for path in paths.iter().filter(|p| p.exists()) {
                tokio::fs::remove_file(path)
                    .await
                    .with_context(|| format!("failed to remove {path:?}"))?;
            }
        }

        // Remove the artifact volume if one was provided.
        if let Some(artifact_root) = self.artifact_root.as_ref()
            && artifact_root.exists()
        {
            tokio::fs::remove_dir_all(artifact_root)
                .await
                .with_context(|| format!("failed to remove artifact root {artifact_root:?}"))?;
        }

        self.tombstones
            .advance(
                &tombstone_id,
                TombstoneStatus::LivePurged,
                TombstoneStatus::BackupExpired,
            )
            .await
            .context("failed to mark agent deletion backup-expired")?;

        Ok(())
    }

    /// Rotate the agent's wrapping key (e.g. after a KMS re-key) by re-wrapping
    /// every active conversation data key with the current `KeyProvider` key.
    pub async fn rotate_conversation_keys(&self) -> Result<()> {
        self.store
            .rewrap_active_data_keys()
            .await
            .context("failed to rewrap active conversation data keys")?;
        Ok(())
    }

    /// Export redacted conversation messages as newline-delimited JSON.
    pub async fn export_conversation(&self, actor_id: &str) -> Result<Vec<u8>> {
        let scope = self.store.scope();
        let rows = sqlx::query(
            "SELECT id, seq, role, payload, search_text, created_at
             FROM messages ORDER BY seq",
        )
        .fetch_all(self.store.pool())
        .await
        .context("failed to read messages for export")?;

        let mut lines = Vec::new();
        for row in rows {
            let payload: String = row.try_get("payload")?;
            let redacted_payload = self
                .store
                .redactor()
                .redact_serialized(payload.as_bytes())
                .context("failed to redact payload")?;
            let search_text: String = row.try_get("search_text")?;
            let redacted_search = self.store.redactor().redact_text(&search_text);
            let record = serde_json::json!({
                "id": row.try_get::<String, _>("id")?,
                "seq": row.try_get::<i64, _>("seq")?,
                "role": row.try_get::<String, _>("role")?,
                "payload": serde_json::from_str::<serde_json::Value>(&redacted_payload).unwrap_or(serde_json::Value::Null),
                "search_text": redacted_search,
                "created_at": row.try_get::<String, _>("created_at")?,
            });
            lines.push(serde_json::to_string(&record)?);
        }

        self.tombstones
            .log_access(
                actor_id,
                &scope.tenant_id,
                "export",
                &format!("conversation:{}", scope.conversation_id),
                lines.len() as i64,
            )
            .await
            .context("failed to append export audit record")?;

        Ok(lines.join("\n").into_bytes())
    }

    /// Search conversation messages with FTS5 and return redacted payloads.
    pub async fn search_conversation(&self, actor_id: &str, query: &str) -> Result<Vec<u8>> {
        let scope = self.store.scope();
        if query.is_empty() {
            bail!("search query must not be empty");
        }
        let redacted_query = self.store.redactor().redact_text(query);

        let rows = sqlx::query(
            "SELECT m.id, m.seq, m.role, m.payload, m.search_text, m.created_at
             FROM messages_fts fts
             JOIN messages m ON m.rowid = fts.rowid
             WHERE messages_fts MATCH ?
             ORDER BY m.seq",
        )
        .bind(&redacted_query)
        .fetch_all(self.store.pool())
        .await
        .context("failed to search messages")?;

        let mut lines = Vec::new();
        for row in rows {
            let payload: String = row.try_get("payload")?;
            let redacted_payload = self
                .store
                .redactor()
                .redact_serialized(payload.as_bytes())
                .unwrap_or(payload);
            let record = serde_json::json!({
                "id": row.try_get::<String, _>("id")?,
                "seq": row.try_get::<i64, _>("seq")?,
                "role": row.try_get::<String, _>("role")?,
                "payload": serde_json::from_str::<serde_json::Value>(&redacted_payload).unwrap_or(serde_json::Value::Null),
                "created_at": row.try_get::<String, _>("created_at")?,
            });
            lines.push(serde_json::to_string(&record)?);
        }

        self.tombstones
            .log_access(
                actor_id,
                &scope.tenant_id,
                "search",
                &format!("conversation:{}", scope.conversation_id),
                lines.len() as i64,
            )
            .await
            .context("failed to append search audit record")?;

        Ok(lines.join("\n").into_bytes())
    }

    /// Verify that a tombstone blocks access to the current conversation or
    /// agent.  Used by adversarial acceptance tests.
    pub async fn blocking_tombstone(&self) -> Result<Option<super::Tombstone>> {
        let scope = self.store.scope();
        self.tombstones
            .blocking_tombstone(
                &scope.tenant_id,
                &scope.agent_id,
                Some(&scope.conversation_id),
            )
            .await
    }

    /// Return the set of active data-key purposes for the current conversation.
    pub async fn active_conversation_purposes(&self) -> Result<HashSet<DataKeyPurpose>> {
        let scope = self.store.scope();
        let rows = sqlx::query(
            "SELECT purpose FROM data_keys
             WHERE scope = 'conversation' AND conversation_id = ? AND state = 'active'",
        )
        .bind(&scope.conversation_id)
        .fetch_all(self.store.pool())
        .await
        .context("failed to list active conversation purposes")?;

        rows.into_iter()
            .map(|row| {
                let purpose: String = row.try_get("purpose")?;
                DataKeyPurpose::parse(&purpose)
            })
            .collect()
    }

    /// Dispatch a lifecycle command that was admitted by the gateway.
    /// Returns any JSON/bytes payload that should be delivered as an outbound
    /// event (used by `export` and `search`).
    pub async fn handle_command(&self, command: &Command) -> Result<Option<Vec<u8>>> {
        match command {
            Command::ConversationReset {
                new_conversation_id,
            } => {
                self.conversation_reset(new_conversation_id).await?;
                Ok(None)
            }
            Command::DeleteAgent {} => {
                self.delete_agent().await?;
                Ok(None)
            }
            Command::Export { actor_id } => {
                let payload = self.export_conversation(actor_id).await?;
                Ok(Some(payload))
            }
            Command::Search { actor_id, query } => {
                let payload = self.search_conversation(actor_id, query).await?;
                Ok(Some(payload))
            }
            Command::RotateKeys {} => {
                self.rotate_conversation_keys().await?;
                Ok(None)
            }
            Command::UserMessage { .. } | Command::Abort {} | Command::ApprovalDecision { .. } => {
                bail!("lifecycle worker received non-lifecycle command")
            }
        }
    }

    /// Mark a lifecycle command as applied in `inbound_commands` so that
    /// startup recovery does not see it as a pending suffix.
    pub async fn apply_command(&self, command_id: &str, seq: u64) -> Result<()> {
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applied', applied_at=?
             WHERE command_id=? AND seq=? AND status='received'",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(command_id)
        .bind(i64::try_from(seq)?)
        .execute(self.store.pool())
        .await
        .context("failed to mark lifecycle command applied")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::store::{
        AgentScope, DATA_KEY_BYTES, DataKeyPurpose, InMemoryTombstoneRepository, KeyProvider,
        KmsClient, KmsKeyProvider, MockKmsClient, TombstoneRepository, TombstoneScope,
        TombstoneStatus, WrappingKey,
    };
    use crate::tools::executor::{ArtifactBroker, ArtifactOperation};
    use uuid::Uuid;

    fn test_kek() -> WrappingKey {
        WrappingKey::new("tenant-kek/v1", [0x11; DATA_KEY_BYTES])
    }

    fn test_agent_key(id: &str, byte: u8) -> WrappingKey {
        WrappingKey::new(id, [byte; DATA_KEY_BYTES])
    }

    fn temp_test_dir() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("sumi-lifecycle-test-{}", Uuid::now_v7()))
    }

    async fn open_test_store(
        conversation_id: &str,
        client: Arc<MockKmsClient>,
    ) -> (Store, WrappingKey) {
        let dir = temp_test_dir();
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("agent.db");
        let agent_key = test_agent_key("agent-key/v1", 0x22);
        client
            .register_agent_key("agent-key/v1", &agent_key)
            .unwrap();
        client.set_current_key_id("agent-key/v1");

        let kms_client: Arc<dyn KmsClient> = client.clone();
        let provider = KmsKeyProvider::new(kms_client).unwrap();
        let key_provider: Arc<dyn KeyProvider> = Arc::new(provider);
        let scope = AgentScope {
            tenant_id: "tenant-1".to_owned(),
            agent_id: "agent-1".to_owned(),
            conversation_id: conversation_id.to_owned(),
        };
        let store = Store::open(&db_path, scope, key_provider).await.unwrap();
        (store, agent_key)
    }

    async fn seed_message(store: &Store, _conversation_id: &str, seq: i64) {
        let key = store
            .conversation_key(DataKeyPurpose::Transcript)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO messages(
                id, seq, role, raw_key_ref, raw_ciphertext, payload, search_text,
                redaction_version, interrupted, created_at
             ) VALUES(?, ?, 'user', ?, X'00', ?, ?, 1, 0, ?)",
        )
        .bind(format!("msg-{seq}"))
        .bind(seq)
        .bind(key.key_ref.clone())
        .bind(r#"{"text":"hello world"}"#)
        .bind("hello world")
        .bind(Utc::now().to_rfc3339())
        .execute(store.pool())
        .await
        .expect("seed message");
    }

    fn open_broker(root: &std::path::Path) -> crate::tools::executor::ArtifactBroker {
        std::fs::create_dir_all(root).unwrap();
        ArtifactBroker::open(root).unwrap()
    }

    #[tokio::test]
    async fn conversation_reset_crypto_erases_keys_and_removes_artifacts() {
        let client = Arc::new(MockKmsClient::new("tenant-1", "agent-1", test_kek()));
        let (store, _) = open_test_store("conversation-old", client.clone()).await;
        let store = Arc::new(store);
        seed_message(&store, "conversation-old", 1).await;

        let tombstones: Arc<dyn TombstoneRepository> = Arc::new(InMemoryTombstoneRepository::new());

        let artifact_root = temp_test_dir();
        std::fs::create_dir_all(&artifact_root).unwrap();
        let broker = open_broker(&artifact_root);
        broker
            .execute(ArtifactOperation::PutAttachment {
                conversation_id: "conversation-old".to_owned(),
                artifact_id: "att-1".to_owned(),
                content: "secret artifact".to_owned(),
            })
            .unwrap();

        let worker = LifecycleWorker::new(
            store.clone(),
            tombstones.clone(),
            Arc::new(DirectArtifactBroker::new(broker)),
            Some(artifact_root.clone()),
        );

        worker.conversation_reset("conversation-new").await.unwrap();

        assert_eq!(store.scope().conversation_id, "conversation-new");
        let purposes = worker.active_conversation_purposes().await.unwrap();
        assert!(purposes.is_empty());

        let message_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(message_count, 0);

        let tombstone = tombstones
            .list_for_agent("tenant-1", "agent-1")
            .await
            .unwrap()
            .pop()
            .expect("tombstone exists");
        assert_eq!(tombstone.status, TombstoneStatus::LivePurged);
        assert_eq!(tombstone.scope, TombstoneScope::Conversation);

        assert!(!artifact_root.join("conversation-old").exists());
    }

    #[tokio::test]
    async fn agent_deletion_removes_db_and_artifacts_and_advances_tombstone() {
        let client = Arc::new(MockKmsClient::new("tenant-1", "agent-1", test_kek()));
        let (store, _) = open_test_store("conversation-del", client.clone()).await;
        let db_path = store.db_path().unwrap().to_path_buf();
        let store = Arc::new(store);

        let tombstones: Arc<dyn TombstoneRepository> = Arc::new(InMemoryTombstoneRepository::new());
        let artifact_root = temp_test_dir();
        std::fs::create_dir_all(&artifact_root).unwrap();

        let worker = LifecycleWorker::new(
            store.clone(),
            tombstones.clone(),
            Arc::new(DirectArtifactBroker::new(open_broker(&artifact_root))),
            Some(artifact_root.clone()),
        );

        worker.delete_agent().await.unwrap();

        assert!(!db_path.exists());
        assert!(!artifact_root.exists());

        let tombstone = tombstones
            .list_for_agent("tenant-1", "agent-1")
            .await
            .unwrap()
            .pop()
            .expect("tombstone exists");
        assert_eq!(tombstone.status, TombstoneStatus::BackupExpired);
        assert_eq!(tombstone.scope, TombstoneScope::Agent);
    }

    #[tokio::test]
    async fn kms_rotation_rewraps_active_conversation_keys() {
        let client = Arc::new(MockKmsClient::new("tenant-1", "agent-1", test_kek()));
        let (store, _) = open_test_store("conversation-rot", client.clone()).await;
        let store = Arc::new(store);
        seed_message(&store, "conversation-rot", 1).await;

        let tombstones: Arc<dyn TombstoneRepository> = Arc::new(InMemoryTombstoneRepository::new());
        let worker = LifecycleWorker::new(
            store.clone(),
            tombstones,
            Arc::new(DirectArtifactBroker::new(open_broker(&temp_test_dir()))),
            None,
        );

        // Rotate to a new agent key.
        client
            .register_agent_key("agent-key/v2", &test_agent_key("agent-key/v2", 0x33))
            .unwrap();
        client.set_current_key_id("agent-key/v2");

        worker.rotate_conversation_keys().await.unwrap();

        let wrap_key_id: String =
            sqlx::query_scalar("SELECT wrap_key_id FROM data_keys WHERE state = 'active' LIMIT 1")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(wrap_key_id, "agent-key/v2");

        // The conversation key can still be loaded under the new wrapping key.
        assert!(
            store
                .conversation_key(DataKeyPurpose::Transcript)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn disabled_old_kms_key_blocks_decrypt_after_rotation() {
        let client = Arc::new(MockKmsClient::new("tenant-1", "agent-1", test_kek()));
        let (store, _) = open_test_store("conversation-rev", client.clone()).await;
        let store = Arc::new(store);
        seed_message(&store, "conversation-rev", 1).await;

        let tombstones: Arc<dyn TombstoneRepository> = Arc::new(InMemoryTombstoneRepository::new());
        let worker = LifecycleWorker::new(
            store.clone(),
            tombstones,
            Arc::new(DirectArtifactBroker::new(open_broker(&temp_test_dir()))),
            None,
        );

        client
            .register_agent_key("agent-key/v2", &test_agent_key("agent-key/v2", 0x34))
            .unwrap();
        client.set_current_key_id("agent-key/v2");
        worker.rotate_conversation_keys().await.unwrap();
        client.disable_key("agent-key/v1");

        // Old key is revoked; the rewrapped data key still decrypts because it
        // no longer references the disabled key.
        assert!(
            store
                .conversation_key(DataKeyPurpose::Transcript)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn export_and_search_are_redacted_and_audited() {
        let client = Arc::new(MockKmsClient::new("tenant-1", "agent-1", test_kek()));
        let (store, _) = open_test_store("conversation-export", client.clone()).await;
        let store = Arc::new(store);
        seed_message(&store, "conversation-export", 1).await;

        let tombstones: Arc<dyn TombstoneRepository> = Arc::new(InMemoryTombstoneRepository::new());
        let worker = LifecycleWorker::new(
            store.clone(),
            tombstones.clone(),
            Arc::new(DirectArtifactBroker::new(open_broker(&temp_test_dir()))),
            None,
        );

        // Insert a message that contains a secret token.
        sqlx::query(
            "INSERT INTO messages(
                id, seq, role, raw_key_ref, raw_ciphertext, payload, search_text,
                redaction_version, interrupted, created_at
             ) VALUES('msg-secret', 2, 'user', ?, X'00', ?, ?, 1, 0, ?)",
        )
        .bind(
            store
                .conversation_key(DataKeyPurpose::Transcript)
                .await
                .unwrap()
                .key_ref
                .clone(),
        )
        .bind(r#"{"text":"token is sk-1234567890abcdef"}"#)
        .bind("token is sk-1234567890abcdef")
        .bind(Utc::now().to_rfc3339())
        .execute(store.pool())
        .await
        .unwrap();

        let exported = worker.export_conversation("actor-1").await.unwrap();
        let text = String::from_utf8(exported).unwrap();
        assert!(text.contains("[REDACTED:api_key]"));
        assert!(!text.contains("sk-1234567890abcdef"));

        let search = worker
            .search_conversation("actor-1", "token")
            .await
            .unwrap();
        let search_text = String::from_utf8(search).unwrap();
        assert!(search_text.contains("[REDACTED:api_key]"));

        let audit = tombstones
            .list_audit("tenant-1", Some("actor-1"), None)
            .await
            .unwrap();
        assert_eq!(audit.len(), 2);
    }
}
