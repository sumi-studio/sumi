//! Conversation reset, agent deletion, export, search, and KMS rotation.
#![allow(dead_code)]

use std::{collections::HashSet, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::Utc;
use sqlx::Row;

use super::{
    AgentScope, DataKeyPurpose, Store, Tombstone, TombstoneRepository, TombstoneScope,
    TombstoneStatus,
};
use crate::gateway::Command;
use crate::runtime::contracts::GenerationRecoveryFence;
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
}

#[derive(Debug)]
pub struct LifecycleCommandResult {
    pub payload: Option<Vec<u8>>,
    pub restart_required: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResetFailpoint {
    None,
    AfterCryptoErase,
    AfterFenced,
    AfterArtifactDelete,
    AfterDatabaseReset,
}

impl LifecycleWorker {
    pub fn new(
        store: Arc<Store>,
        tombstones: Arc<dyn TombstoneRepository>,
        broker: Arc<dyn ArtifactLifecycleBroker>,
        _artifact_root: Option<PathBuf>,
    ) -> Self {
        Self {
            store,
            tombstones,
            broker,
        }
    }

    /// Request or replay a reset. The external tombstone is the durable command
    /// receipt; a new request blocks until a supervisor persists an exact typed
    /// generation fence.
    pub async fn conversation_reset(
        &self,
        command_id: &str,
        command_seq: u64,
        new_conversation_id: &str,
    ) -> Result<bool> {
        let scope = self.store.scope();
        let command_seq = i64::try_from(command_seq)?;
        if let Some(existing) = self
            .tombstones
            .find_by_command(&scope.tenant_id, &scope.agent_id, command_id)
            .await?
        {
            self.validate_reset_receipt(&existing, command_id, command_seq, new_conversation_id)?;
            if matches!(
                existing.status,
                TombstoneStatus::LivePurged | TombstoneStatus::BackupExpired
            ) {
                return Ok(false);
            }
            return self
                .resume_conversation_reset_with_failpoint(&existing.id, ResetFailpoint::None)
                .await;
        }

        let tombstone = self
            .tombstones
            .request_conversation_reset(
                &scope.tenant_id,
                &scope.agent_id,
                &scope.conversation_id,
                new_conversation_id,
                command_id,
                command_seq,
                "2099-12-31T23:59:59Z",
            )
            .await
            .context("failed to record conversation reset tombstone")?;
        self.resume_conversation_reset_with_failpoint(&tombstone.id, ResetFailpoint::None)
            .await
    }

    fn validate_reset_receipt(
        &self,
        tombstone: &Tombstone,
        command_id: &str,
        command_seq: i64,
        new_conversation_id: &str,
    ) -> Result<()> {
        if tombstone.scope != TombstoneScope::Conversation
            || tombstone.command_id.as_deref() != Some(command_id)
            || tombstone.command_seq != Some(command_seq)
            || tombstone.replacement_conversation_id.as_deref() != Some(new_conversation_id)
        {
            bail!("conversation reset command conflicts with durable tombstone receipt");
        }
        Ok(())
    }

    /// Called only after the deployment supervisor has stopped/fenced the old
    /// process generation. Recording the proof is idempotent and deliberately
    /// does not advance the lifecycle status.
    pub async fn record_generation_fence(
        &self,
        tombstone_id: &str,
        fence: &GenerationRecoveryFence,
    ) -> Result<Tombstone> {
        self.tombstones
            .record_generation_fence(
                tombstone_id,
                fence.generation().as_i64(),
                fence.lease_id(),
                fence.fence_id(),
            )
            .await
            .context("failed to persist reset generation fence")
    }

    /// Resume pending reset stages before startup exposes keys, commands,
    /// transcript, search, export, provider calls, or tools.
    pub async fn resume_pending_resets(&self) -> Result<bool> {
        let scope = self.store.scope();
        let tombstones = self
            .tombstones
            .list_for_agent(&scope.tenant_id, &scope.agent_id)
            .await?;
        let mut restart_required = false;
        for tombstone in tombstones {
            if tombstone.scope != TombstoneScope::Conversation
                || matches!(
                    tombstone.status,
                    TombstoneStatus::LivePurged | TombstoneStatus::BackupExpired
                )
            {
                continue;
            }
            if tombstone.conversation_id.as_deref() == Some(scope.conversation_id.as_str())
                || tombstone.replacement_conversation_id.as_deref()
                    == Some(scope.conversation_id.as_str())
            {
                restart_required |= self
                    .resume_conversation_reset_with_failpoint(&tombstone.id, ResetFailpoint::None)
                    .await?;
            }
        }
        Ok(restart_required)
    }

    async fn resume_conversation_reset_with_failpoint(
        &self,
        tombstone_id: &str,
        failpoint: ResetFailpoint,
    ) -> Result<bool> {
        let mut tombstone = self.tombstones.get(tombstone_id).await?;
        let scope = self.store.scope();
        if tombstone.tenant_id != scope.tenant_id
            || tombstone.agent_id != scope.agent_id
            || tombstone.scope != TombstoneScope::Conversation
        {
            bail!("conversation reset tombstone identity does not match the agent store");
        }
        let old_conversation_id = tombstone
            .conversation_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("reset tombstone is missing old conversation"))?;
        let new_conversation_id =
            tombstone
                .replacement_conversation_id
                .clone()
                .ok_or_else(|| {
                    anyhow::anyhow!("reset tombstone is missing replacement conversation")
                })?;

        if tombstone.status == TombstoneStatus::Requested {
            if tombstone.fenced_generation.is_none()
                || tombstone.generation_lease_id.is_none()
                || tombstone.generation_fence_id.is_none()
            {
                bail!(
                    "conversation reset tombstone {} blocks access until the deployment \
                     supervisor persists a generation fence",
                    tombstone.id
                );
            }
            self.crypto_erase_conversation(&old_conversation_id).await?;
            Self::trip_failpoint(failpoint, ResetFailpoint::AfterCryptoErase)?;
            self.tombstones
                .advance(
                    &tombstone.id,
                    TombstoneStatus::Requested,
                    TombstoneStatus::Fenced,
                )
                .await?;
            tombstone = self.tombstones.get(&tombstone.id).await?;
        }

        if tombstone.status == TombstoneStatus::Fenced {
            Self::trip_failpoint(failpoint, ResetFailpoint::AfterFenced)?;
            let deleted = self
                .broker
                .delete_conversation_artifacts(&old_conversation_id, &tombstone.id)
                .await
                .context("failed to delete conversation artifacts")?;
            Self::trip_failpoint(failpoint, ResetFailpoint::AfterArtifactDelete)?;
            self.reset_conversation_state(&scope, &new_conversation_id)
                .await?;
            self.store.set_conversation_id(new_conversation_id.clone());
            Self::trip_failpoint(failpoint, ResetFailpoint::AfterDatabaseReset)?;
            self.tombstones
                .advance(
                    &tombstone.id,
                    TombstoneStatus::Fenced,
                    TombstoneStatus::LivePurged,
                )
                .await?;
            self.tombstones
                .log_access(
                    "lifecycle-worker",
                    &scope.tenant_id,
                    "reset",
                    &format!("conversation:{old_conversation_id}"),
                    deleted as i64,
                )
                .await
                .context("failed to persist reset audit receipt")?;
            return Ok(true);
        }
        Ok(false)
    }

    fn trip_failpoint(actual: ResetFailpoint, expected: ResetFailpoint) -> Result<()> {
        if actual == expected {
            bail!("deterministic lifecycle failpoint: {expected:?}");
        }
        Ok(())
    }

    async fn crypto_erase_conversation(&self, old_conversation_id: &str) -> Result<()> {
        sqlx::query(
            "UPDATE data_keys
             SET state = 'destroyed', wrapped_key = NULL, wrap_nonce = NULL, destroyed_at = ?
             WHERE scope = 'conversation' AND conversation_id = ? AND state = 'active'",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(old_conversation_id)
        .execute(self.store.pool())
        .await
        .context("failed to crypto-erase conversation data keys")?;
        Ok(())
    }

    async fn reset_conversation_state(
        &self,
        scope: &AgentScope,
        new_conversation_id: &str,
    ) -> Result<()> {
        let mut transaction = self.store.pool().begin().await?;
        for table in [
            "physical_recovery_receipt_intents",
            "physical_recovery_receipt_applications",
            "memory_batch_messages",
            "provider_context",
            "provider_context_mutations",
            "provider_context_replace_heads",
            "memory_jobs",
            "memory_apply_cursors",
            "memory_batches",
            "approval_log",
            "event_log_heads",
            "messages",
            "agent_events",
            "tool_executions",
            "inbound_commands",
            "kv",
        ] {
            sqlx::query(&format!("DELETE FROM {table}"))
                .execute(&mut *transaction)
                .await
                .with_context(|| format!("failed to delete from {table} during reset"))?;
        }

        let updated = sqlx::query(
            "UPDATE agent_scope SET conversation_id = ? WHERE tenant_id = ? AND agent_id = ?",
        )
        .bind(new_conversation_id)
        .bind(&scope.tenant_id)
        .bind(&scope.agent_id)
        .execute(&mut *transaction)
        .await?;
        if updated.rows_affected() != 1 {
            bail!("conversation reset scope update did not affect exactly one agent");
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Agent deletion is a supervisor-owned operation. The runtime must not
    /// mark an inbound command applied and then self-delete its database or
    /// artifact root: a crash in between makes the durable command unretryable
    /// and direct filesystem removal crosses the broker/supervisor boundary.
    ///
    /// The control plane creates the durable agent tombstone and its deployment
    /// supervisor fences the runtime, destroys agent/workspace keys, and removes
    /// the agent DB and volumes. This local runtime fails closed until that
    /// boundary is available.
    pub async fn delete_agent(&self) -> Result<()> {
        bail!(
            "DeleteAgent must be executed by the control-plane deployment supervisor; \
             runtime-side deletion is disabled"
        )
    }

    /// Rotate the agent's wrapping key (e.g. after a KMS re-key) by re-wrapping
    /// every active conversation data key with the current `KeyProvider` key.
    pub async fn rotate_conversation_keys(&self) -> Result<()> {
        self.ensure_access_allowed("key rotation").await?;
        self.store
            .rewrap_active_data_keys()
            .await
            .context("failed to rewrap active conversation data keys")?;
        Ok(())
    }

    /// Export redacted conversation messages as newline-delimited JSON.
    pub async fn export_conversation(&self, actor_id: &str) -> Result<Vec<u8>> {
        self.ensure_access_allowed("export").await?;
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

    /// Escape a redacted or user-supplied query so it is treated as a literal
    /// sequence of tokens by FTS5. FTS5 metacharacters (`:`, `[`, `"`, `*`, etc.)
    /// inside the redaction replacement would otherwise produce syntax errors or
    /// unintended column/filter semantics.
    fn fts5_literal_query(redacted: &str) -> Result<String> {
        let sanitized: String = redacted
            .chars()
            .map(|c| {
                if c.is_whitespace() || c.is_ascii_control() {
                    ' '
                } else {
                    c
                }
            })
            .collect();
        let tokens: Vec<String> = sanitized
            .split_whitespace()
            .map(|token| {
                let escaped = token.replace('"', "\"\"");
                format!("\"{escaped}\"")
            })
            .collect();
        if tokens.is_empty() {
            bail!("search query contains no searchable tokens after redaction");
        }
        Ok(tokens.join(" "))
    }

    /// Search conversation messages with FTS5 and return redacted payloads.
    pub async fn search_conversation(&self, actor_id: &str, query: &str) -> Result<Vec<u8>> {
        self.ensure_access_allowed("search").await?;
        let scope = self.store.scope();
        if query.is_empty() {
            bail!("search query must not be empty");
        }
        let redacted_query = self.store.redactor().redact_text(query);
        let literal_query = Self::fts5_literal_query(&redacted_query)?;

        let rows = sqlx::query(
            "SELECT m.id, m.seq, m.role, m.payload, m.search_text, m.created_at
             FROM messages_fts fts
             JOIN messages m ON m.rowid = fts.rowid
             WHERE messages_fts MATCH ?
             ORDER BY m.seq",
        )
        .bind(&literal_query)
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

    pub async fn ensure_access_allowed(&self, operation: &str) -> Result<()> {
        if let Some(tombstone) = self.blocking_tombstone().await? {
            bail!(
                "{operation} is blocked by deletion tombstone {} in status {}",
                tombstone.id,
                tombstone.status.as_str()
            );
        }
        Ok(())
    }

    /// Return the set of active data-key purposes for the current conversation.
    pub async fn active_conversation_purposes(&self) -> Result<HashSet<DataKeyPurpose>> {
        self.ensure_access_allowed("conversation key access")
            .await?;
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
    pub async fn handle_command(
        &self,
        command_id: &str,
        command_seq: u64,
        command: &Command,
    ) -> Result<LifecycleCommandResult> {
        match command {
            Command::ConversationReset {
                new_conversation_id,
            } => {
                let restart_required = self
                    .conversation_reset(command_id, command_seq, new_conversation_id)
                    .await?;
                Ok(LifecycleCommandResult {
                    payload: None,
                    restart_required,
                })
            }
            Command::DeleteAgent {} => {
                self.delete_agent().await?;
                unreachable!("delete_agent always fails closed")
            }
            Command::Export { actor_id } => {
                let payload = self.export_conversation(actor_id).await?;
                Ok(LifecycleCommandResult {
                    payload: Some(payload),
                    restart_required: false,
                })
            }
            Command::Search { actor_id, query } => {
                let payload = self.search_conversation(actor_id, query).await?;
                Ok(LifecycleCommandResult {
                    payload: Some(payload),
                    restart_required: false,
                })
            }
            Command::RotateKeys {} => {
                self.rotate_conversation_keys().await?;
                Ok(LifecycleCommandResult {
                    payload: None,
                    restart_required: false,
                })
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
    use crate::runtime::contracts::{ProcessGeneration, ProcessGenerationLease};
    use crate::store::crypto::{DataKeyScope, KeyWrapAad, WRAP_ALGORITHM, wrap_data_key};
    use crate::store::{
        AgentScope, DATA_KEY_BYTES, DataKeyMaterial, DataKeyPurpose, InMemoryTombstoneRepository,
        KeyProvider, KmsClient, KmsKeyProvider, MockKmsClient, SqliteTombstoneRepository,
        TombstoneRepository, TombstoneScope, TombstoneStatus, WrappingKey,
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
        open_test_store_for("tenant-1", "agent-1", conversation_id, client).await
    }

    async fn open_test_store_for(
        tenant_id: &str,
        agent_id: &str,
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
            tenant_id: tenant_id.to_owned(),
            agent_id: agent_id.to_owned(),
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

    async fn seed_workspace_key(store: &Store, wrapping_key: &WrappingKey) {
        let key_ref = "workspace-key";
        let data_key =
            DataKeyMaterial::from_bytes(key_ref, DataKeyPurpose::Workspace, [0x42; DATA_KEY_BYTES]);
        let aad = KeyWrapAad {
            key_ref: key_ref.to_owned(),
            scope: DataKeyScope::Agent,
            purpose: DataKeyPurpose::Workspace,
            conversation_id: None,
            wrap_key_id: wrapping_key.key_id().to_owned(),
        };
        let (wrap_nonce, wrapped_key) = wrap_data_key(&data_key, wrapping_key, &aad).unwrap();
        sqlx::query(
            "INSERT INTO data_keys(
                key_ref, scope, purpose, conversation_id, algorithm, wrap_key_id,
                wrap_nonce, wrapped_key, state, created_at, destroyed_at
             ) VALUES(?, 'agent', 'workspace', NULL, ?, ?, ?, ?, 'active', ?, NULL)",
        )
        .bind(key_ref)
        .bind(WRAP_ALGORITHM)
        .bind(wrapping_key.key_id())
        .bind(wrap_nonce.as_slice())
        .bind(wrapped_key)
        .bind(Utc::now().to_rfc3339())
        .execute(store.pool())
        .await
        .expect("seed workspace key");
    }

    async fn seed_physical_recovery_receipt(store: &Store, suffix_seq: i64, label: &str) {
        let event_key = store.conversation_key(DataKeyPurpose::Event).await.unwrap();
        sqlx::query(
            "INSERT INTO agent_events(
                seq, event_type, internal_metadata, raw_key_ref, raw_ciphertext,
                envelope, redaction_version, created_at
             ) VALUES(?, 'tool_execution_end', ?, ?, X'00', ?, 1, ?)",
        )
        .bind(suffix_seq)
        .bind(format!(
            r#"{{"receipt_id":"receipt-{label}","tool_call_id":"tool-{label}"}}"#
        ))
        .bind(&event_key.key_ref)
        .bind(format!(
            r#"{{"type":"tool_execution_end","label":"{label}"}}"#
        ))
        .bind(Utc::now().to_rfc3339())
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO tool_executions(
                tool_call_id, command_id, run_id, executor_generation, state,
                idempotency_key, started_at, finished_at, error_code
             ) VALUES(?, ?, ?, 7, 'indeterminate', ?, ?, ?, 'indeterminate')",
        )
        .bind(format!("tool-{label}"))
        .bind(format!("command-{label}"))
        .bind(format!("run-{label}"))
        .bind(format!("idempotency-{label}"))
        .bind(Utc::now().to_rfc3339())
        .bind(Utc::now().to_rfc3339())
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO physical_recovery_receipt_applications(
                receipt_id, receipt_digest, lease_id, fence_id, generation,
                intent_count, logical_suffix_first_seq, logical_suffix_last_seq, applied_at
             ) VALUES(?, ?, ?, ?, 7, 1, ?, ?, ?)",
        )
        .bind(format!("receipt-{label}"))
        .bind(format!("digest-{label}"))
        .bind(format!("lease-{label}"))
        .bind(format!("fence-{label}"))
        .bind(suffix_seq)
        .bind(suffix_seq)
        .bind(Utc::now().to_rfc3339())
        .execute(store.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO physical_recovery_receipt_intents(
                receipt_id, tool_call_id, command_id, run_id, executor_generation,
                indeterminate_terminal_seq
             ) VALUES(?, ?, ?, ?, 7, ?)",
        )
        .bind(format!("receipt-{label}"))
        .bind(format!("tool-{label}"))
        .bind(format!("command-{label}"))
        .bind(format!("run-{label}"))
        .bind(suffix_seq)
        .execute(store.pool())
        .await
        .unwrap();
    }

    fn open_broker(root: &std::path::Path) -> crate::tools::executor::ArtifactBroker {
        std::fs::create_dir_all(root).unwrap();
        ArtifactBroker::open(root).unwrap()
    }

    fn test_generation_fence() -> GenerationRecoveryFence {
        let lease =
            ProcessGenerationLease::new(ProcessGeneration::from_wire(7).unwrap(), "reset-lease-7")
                .unwrap();
        GenerationRecoveryFence::new(&lease, "reset-fence-7").unwrap()
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

        let command_id = "00000000-0000-4000-8000-000000000001";
        let error = worker
            .conversation_reset(command_id, 1, "conversation-new")
            .await
            .expect_err("reset must wait for the supervisor fence");
        assert!(error.to_string().contains("generation fence"));
        assert!(worker.ensure_access_allowed("startup").await.is_err());
        assert!(worker.export_conversation("actor").await.is_err());
        assert!(worker.search_conversation("actor", "hello").await.is_err());
        assert!(
            worker.resume_pending_resets().await.is_err(),
            "startup recovery must fail closed before a generation fence"
        );
        let tombstone = tombstones
            .find_by_command("tenant-1", "agent-1", command_id)
            .await
            .unwrap()
            .unwrap();
        worker
            .record_generation_fence(&tombstone.id, &test_generation_fence())
            .await
            .unwrap();
        assert!(
            worker
                .conversation_reset(command_id, 1, "conversation-new")
                .await
                .unwrap(),
            "the fenced generation must exit after live purge"
        );

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
    async fn reset_failpoints_resume_one_tombstone_stage_by_stage() {
        for failpoint in [
            ResetFailpoint::AfterCryptoErase,
            ResetFailpoint::AfterFenced,
            ResetFailpoint::AfterArtifactDelete,
            ResetFailpoint::AfterDatabaseReset,
        ] {
            let old = format!("conversation-old-{failpoint:?}");
            let new = format!("conversation-new-{failpoint:?}");
            let command_id = format!("command-{failpoint:?}");
            let client = Arc::new(MockKmsClient::new("tenant-1", "agent-1", test_kek()));
            let (store, _) = open_test_store(&old, client).await;
            let store = Arc::new(store);
            seed_message(&store, &old, 1).await;
            let compliance_root = temp_test_dir();
            std::fs::create_dir_all(&compliance_root).unwrap();
            let compliance_path = compliance_root.join("control-plane.db");
            let tombstones: Arc<dyn TombstoneRepository> = Arc::new(
                SqliteTombstoneRepository::open(&compliance_path)
                    .await
                    .unwrap(),
            );
            let artifact_root = temp_test_dir();
            let broker = open_broker(&artifact_root);
            broker
                .execute(ArtifactOperation::PutAttachment {
                    conversation_id: old.clone(),
                    artifact_id: "restart-proof".to_owned(),
                    content: "must be erased".to_owned(),
                })
                .unwrap();
            let broker: Arc<dyn ArtifactLifecycleBroker> =
                Arc::new(DirectArtifactBroker::new(broker));
            let worker =
                LifecycleWorker::new(store.clone(), tombstones.clone(), broker.clone(), None);

            worker
                .conversation_reset(&command_id, 1, &new)
                .await
                .expect_err("unfenced request must stop");
            let tombstone = tombstones
                .find_by_command("tenant-1", "agent-1", &command_id)
                .await
                .unwrap()
                .unwrap();
            worker
                .record_generation_fence(&tombstone.id, &test_generation_fence())
                .await
                .unwrap();
            worker
                .resume_conversation_reset_with_failpoint(&tombstone.id, failpoint)
                .await
                .expect_err("deterministic crash boundary must fire");

            // Reopen the external compliance repository and reconstruct the
            // worker as a process-restart counterexample.
            drop(worker);
            drop(tombstones);
            let tombstones: Arc<dyn TombstoneRepository> = Arc::new(
                SqliteTombstoneRepository::open(&compliance_path)
                    .await
                    .unwrap(),
            );
            let restarted =
                LifecycleWorker::new(store.clone(), tombstones.clone(), broker.clone(), None);
            assert!(
                restarted
                    .resume_conversation_reset_with_failpoint(&tombstone.id, ResetFailpoint::None,)
                    .await
                    .unwrap()
            );
            assert_eq!(
                tombstones.get(&tombstone.id).await.unwrap().status,
                TombstoneStatus::LivePurged
            );
            assert_eq!(
                tombstones
                    .list_for_agent("tenant-1", "agent-1")
                    .await
                    .unwrap()
                    .len(),
                1,
                "restart minted an extra tombstone at {failpoint:?}"
            );
            assert_eq!(store.scope().conversation_id, new);
            assert!(!artifact_root.join(old).exists());
        }
    }

    #[tokio::test]
    async fn reset_physical_receipt_is_fk_safe_and_isolated_from_second_agent() {
        let target_client = Arc::new(MockKmsClient::new("tenant-1", "agent-1", test_kek()));
        let second_client = Arc::new(MockKmsClient::new("tenant-2", "agent-2", test_kek()));
        let (target, _) =
            open_test_store_for("tenant-1", "agent-1", "conversation-target", target_client).await;
        let (second, _) =
            open_test_store_for("tenant-2", "agent-2", "conversation-second", second_client).await;
        let target = Arc::new(target);
        let second = Arc::new(second);
        seed_physical_recovery_receipt(&target, 1, "target").await;
        seed_physical_recovery_receipt(&second, 1, "second").await;
        let command_key = target
            .conversation_key(DataKeyPurpose::Command)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO inbound_commands(
                seq, command_id, command_kind, payload_ciphertext, payload_key_ref,
                payload_hmac, status, run_phase, received_at
             ) VALUES(7, 'reset-with-physical-receipt', 'lifecycle', X'00', ?,
                      zeroblob(32), 'received', 'received', ?)",
        )
        .bind(&command_key.key_ref)
        .bind(Utc::now().to_rfc3339())
        .execute(target.pool())
        .await
        .unwrap();

        let artifact_root = temp_test_dir();
        let broker = open_broker(&artifact_root);
        for conversation_id in ["conversation-target", "conversation-second"] {
            broker
                .execute(ArtifactOperation::PutAttachment {
                    conversation_id: conversation_id.to_owned(),
                    artifact_id: "receipt-proof".to_owned(),
                    content: conversation_id.to_owned(),
                })
                .unwrap();
        }
        let tombstones: Arc<dyn TombstoneRepository> = Arc::new(InMemoryTombstoneRepository::new());
        let worker = LifecycleWorker::new(
            target.clone(),
            tombstones.clone(),
            Arc::new(DirectArtifactBroker::new(broker)),
            None,
        );
        let command_id = "reset-with-physical-receipt";
        worker
            .conversation_reset(command_id, 7, "conversation-target-new")
            .await
            .expect_err("unfenced reset must stop");
        let tombstone = tombstones
            .find_by_command("tenant-1", "agent-1", command_id)
            .await
            .unwrap()
            .unwrap();
        worker
            .record_generation_fence(&tombstone.id, &test_generation_fence())
            .await
            .unwrap();
        worker
            .conversation_reset(command_id, 7, "conversation-target-new")
            .await
            .unwrap();
        let command_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inbound_commands")
            .fetch_one(target.pool())
            .await
            .unwrap();
        assert_eq!(command_rows, 0, "old encrypted command row survived reset");
        assert!(
            !worker
                .conversation_reset(command_id, 7, "conversation-target-new")
                .await
                .unwrap(),
            "durable tombstone receipt replay minted or re-applied reset"
        );

        for table in [
            "physical_recovery_receipt_intents",
            "physical_recovery_receipt_applications",
            "agent_events",
            "tool_executions",
        ] {
            let target_count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(target.pool())
                .await
                .unwrap();
            let second_count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(second.pool())
                .await
                .unwrap();
            assert_eq!(target_count, 0, "target {table} survived reset");
            assert_eq!(second_count, 1, "second-agent {table} was modified");
        }
        assert!(!artifact_root.join("conversation-target").exists());
        assert!(
            artifact_root
                .join("conversation-second")
                .join("attachments")
                .join("receipt-proof")
                .exists(),
            "second-agent artifact subtree was modified"
        );
        assert!(
            tombstones
                .list_for_agent("tenant-2", "agent-2")
                .await
                .unwrap()
                .is_empty(),
            "target reset crossed the control-plane agent identity"
        );
    }

    #[tokio::test]
    async fn agent_deletion_fails_closed_without_supervisor_and_preserves_local_state() {
        let client = Arc::new(MockKmsClient::new("tenant-1", "agent-1", test_kek()));
        let (store, _) = open_test_store("conversation-del", client.clone()).await;
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

        let error = worker
            .delete_agent()
            .await
            .expect_err("runtime deletion must fail closed");
        assert!(error.to_string().contains("deployment supervisor"));

        assert!(artifact_root.exists());

        let scope_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_scope")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(scope_count, 1, "runtime database remains open");

        let tombstones = tombstones
            .list_for_agent("tenant-1", "agent-1")
            .await
            .unwrap();
        assert!(
            tombstones.is_empty(),
            "runtime cannot mint a control-plane tombstone"
        );
    }

    #[tokio::test]
    async fn kms_rotation_rewraps_active_conversation_keys() {
        let client = Arc::new(MockKmsClient::new("tenant-1", "agent-1", test_kek()));
        let (store, agent_key) = open_test_store("conversation-rot", client.clone()).await;
        let store = Arc::new(store);
        seed_message(&store, "conversation-rot", 1).await;
        seed_workspace_key(&store, &agent_key).await;

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

        let stale_key_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM data_keys
             WHERE state = 'active' AND wrap_key_id != 'agent-key/v2'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(
            stale_key_count, 0,
            "all active keys, including workspace, rewrapped"
        );

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

    #[tokio::test]
    async fn search_redacts_secret_query_and_escapes_fts_metacharacters() {
        let client = Arc::new(MockKmsClient::new("tenant-1", "agent-1", test_kek()));
        let (store, _) = open_test_store("conversation-fts", client.clone()).await;
        let store = Arc::new(store);

        // Insert a message whose payload contains the secret but whose
        // search_text has already been redacted, as the real transcript path
        // does before indexing.
        let key = store
            .conversation_key(DataKeyPurpose::Transcript)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO messages(
                id, seq, role, raw_key_ref, raw_ciphertext, payload, search_text,
                redaction_version, interrupted, created_at
             ) VALUES('msg-secret', 1, 'user', ?, X'00', ?, ?, 1, 0, ?)",
        )
        .bind(key.key_ref.clone())
        .bind(r#"{"text":"token is sk-1234567890abcdef"}"#)
        .bind("token is [REDACTED:api_key]")
        .bind(Utc::now().to_rfc3339())
        .execute(store.pool())
        .await
        .unwrap();

        let tombstones: Arc<dyn TombstoneRepository> = Arc::new(InMemoryTombstoneRepository::new());
        let worker = LifecycleWorker::new(
            store.clone(),
            tombstones,
            Arc::new(DirectArtifactBroker::new(open_broker(&temp_test_dir()))),
            None,
        );

        // Searching the raw secret must redact to the placeholder and not error.
        let search = worker
            .search_conversation("actor-1", "sk-1234567890abcdef")
            .await
            .unwrap();
        let search_text = String::from_utf8(search).unwrap();
        assert!(search_text.contains("[REDACTED:api_key]"));
        assert!(!search_text.contains("sk-1234567890abcdef"));

        // FTS5 metacharacters in the user query must not cause syntax errors.
        for metachar_query in ["[", "]", ":", "\"", "*", "[REDACTED:api_key]"] {
            worker
                .search_conversation("actor-1", metachar_query)
                .await
                .unwrap_or_else(|error| {
                    panic!("FTS5 query {metachar_query:?} produced an error: {error}")
                });
        }
    }
}
