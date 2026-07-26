//! Durable conversation, event, and memory storage.

mod crypto;
mod delivery;
mod event_log;
mod event_writer;
mod memory_state;
mod physical_recovery;
mod provider_context;
mod recovery;
mod redactor;
mod sizer;
mod transcript;

use std::{collections::HashMap, path::Path, sync::Arc};

#[cfg(test)]
use std::str::FromStr;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use sha2::{Digest, Sha256};
use sqlx::{
    Row, Sqlite, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::provider::types::{
    ContextMessage, Message, ProviderContextItem, ProviderContextPayload, PublicMessage,
};
use crate::runtime::contracts::{
    GenerationRecoveryFence, ProcessGeneration, ProcessGenerationLease,
};

use self::crypto::{
    ConversationCommandDigestFactory, DataKeyScope, KeyWrapAad, WRAP_ALGORITHM, unwrap_data_key,
    wrap_data_key,
};
#[allow(unused_imports)]
pub(crate) use self::physical_recovery::{
    ApplyReceiptOutcome, HydrationReceiptIdentity, PhysicalRecoveryApplier, PhysicalRecoveryIntent,
    PhysicalRecoveryIntentRequest, PhysicalRecoveryReceipt,
};
pub(crate) use self::provider_context::{ProviderContextEvictionEstimate, ProviderContextKind};
pub(crate) use self::transcript::{message_interrupted, public_message_role};
#[cfg(test)]
pub(crate) use crypto::{DATA_KEY_BYTES, WrappingKey};
pub(crate) use crypto::{
    DataKeyMaterial, DataKeyPurpose, EnvironmentKeyProvider, KeyProvider, RowAad,
    command_payload_digest, decrypt_content, encrypt_content, verify_command_payload_digest,
};
#[allow(
    unused_imports,
    reason = "T12 freezes projection types consumed by T15 without duplicating EventWriter"
)]
pub(crate) use event_writer::{
    ApplicationKind, ApprovalMutation, DurableEvent, EventBatch, EventWrite, EventWriter,
    InboundAdmission, InboundReceipt, InboundReceiptOrigin, InjectedCommand,
    MemoryApplyCursorAdvance, MemoryBatchMutation, MemoryJobMutation, MemoryJobUpdate,
    MemoryTransition, Projection, RecoveryRequired, RunPhase, ToolExecutionMutation,
    USER_MESSAGE_ID_NAMESPACE, user_message_id,
};
#[allow(
    unused_imports,
    reason = "T17 exposes the hydrated memory state boundary consumed by T19-T21"
)]
pub(crate) use memory_state::{
    MemoryApplyCursorRecord, MemoryBatchMessageRecord, MemoryBatchRecord, MemoryBatchState,
    MemoryBatchSummary, MemoryJobKind, MemoryJobRecord, MemoryJobResult, MemoryJobStatus,
    MemoryLayer,
};
#[allow(
    unused_imports,
    reason = "T12 exposes the recovery plan boundary consumed by T15"
)]
pub(crate) use recovery::{HydratedRunState, HydrationOutcome, RecoveryStep, SuffixRecovery};
pub(crate) use redactor::{PublicProjectionBuilder, Redactor};
#[allow(
    unused_imports,
    reason = "T12 freezes the full injection sizing boundary consumed by the T15 run loop"
)]
pub(crate) use sizer::{
    BatchBounds, CommandSizeInput, DURABLE_ROW_OVERHEAD_BYTES, EventBatchSizer,
    InjectionApplication, InjectionBatchSizeInput, InjectionCommandSizeInput,
};
#[cfg(test)]
pub(crate) use transcript::TranscriptRecord;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AgentScope {
    pub tenant_id: String,
    pub agent_id: String,
    pub conversation_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "T11 freezes the per-anchor key boundary before provider-context persistence is wired"
)]
pub(crate) struct ProviderContextKeyAnchor {
    pub conversation_id: String,
    pub anchor_id: String,
}

impl AgentScope {
    fn validate(&self) -> Result<()> {
        if self.tenant_id.is_empty() || self.agent_id.is_empty() || self.conversation_id.is_empty()
        {
            bail!("agent scope identifiers must not be empty");
        }
        Ok(())
    }

    pub(crate) fn row_aad(
        &self,
        table: impl Into<String>,
        row_id: impl Into<String>,
        purpose: DataKeyPurpose,
    ) -> RowAad {
        RowAad {
            tenant_id: self.tenant_id.clone(),
            agent_id: self.agent_id.clone(),
            conversation_id: self.conversation_id.clone(),
            table: table.into(),
            row_id: row_id.into(),
            purpose: purpose.as_str().to_owned(),
            schema_version: 1,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Store {
    pool: SqlitePool,
    scope: AgentScope,
    key_provider: Arc<dyn KeyProvider>,
    redactor: Redactor,
    event_writer_state: Arc<Mutex<event_writer::WriterState>>,
}

impl Store {
    pub(crate) async fn open(
        path: &Path,
        scope: AgentScope,
        key_provider: Arc<dyn KeyProvider>,
    ) -> Result<Self> {
        scope.validate()?;
        prepare_state_path(path).await?;
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .with_context(|| format!("failed to open SQLite database {}", path.display()))?;
        let store = Self::finish_open(pool, scope, key_provider).await?;
        secure_sqlite_files(path).await?;
        Ok(store)
    }

    #[cfg(test)]
    async fn in_memory(scope: AgentScope, key_provider: Arc<dyn KeyProvider>) -> Result<Self> {
        let options = SqliteConnectOptions::from_str("sqlite::memory:")?
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Memory);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        Self::finish_open(pool, scope, key_provider).await
    }

    #[cfg(test)]
    pub(crate) async fn session_test_store(conversation_id: &str) -> Result<Self> {
        #[derive(Clone)]
        struct SessionTestKeyProvider(WrappingKey);

        #[async_trait::async_trait]
        impl KeyProvider for SessionTestKeyProvider {
            async fn current_key(&self) -> Result<WrappingKey> {
                Ok(self.0.clone())
            }

            async fn key_by_id(&self, key_id: &str) -> Result<WrappingKey> {
                if key_id != self.0.key_id() {
                    bail!("unknown session test wrapping key {key_id}");
                }
                Ok(self.0.clone())
            }
        }

        Self::in_memory(
            AgentScope {
                tenant_id: "session-test-tenant".to_owned(),
                agent_id: "session-test-agent".to_owned(),
                conversation_id: conversation_id.to_owned(),
            },
            Arc::new(SessionTestKeyProvider(WrappingKey::new(
                "session-test-key/v1",
                [0x5a; 32],
            ))),
        )
        .await
    }

    async fn finish_open(
        pool: SqlitePool,
        scope: AgentScope,
        key_provider: Arc<dyn KeyProvider>,
    ) -> Result<Self> {
        MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply agent store migrations")?;

        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO agent_scope(singleton, tenant_id, agent_id, conversation_id, created_at)
             VALUES(1, ?, ?, ?, ?)
             ON CONFLICT(singleton) DO NOTHING",
        )
        .bind(&scope.tenant_id)
        .bind(&scope.agent_id)
        .bind(&scope.conversation_id)
        .bind(now)
        .execute(&pool)
        .await
        .context("failed to initialize agent scope")?;

        let store = Arc::new(Self {
            pool,
            scope,
            key_provider,
            redactor: Redactor::v1(),
            event_writer_state: Arc::new(Mutex::new(event_writer::WriterState::default())),
        });
        store.validate_startup().await?;
        event_writer::EventWriter::new(store.clone())
            .recover_provider_context_mutations()
            .await
            .context("failed to recover prepared provider-context mutations")?;
        Arc::try_unwrap(store).map_err(|_| anyhow!("recovery must not retain Store references"))
    }

    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Authenticated T17 cold-boot hydration boundary.
    ///
    /// Validates the injected `ProcessGenerationLease`/`GenerationRecoveryFence`,
    /// authenticates the Store scope and data keys, decrypts and validates
    /// persisted transcript anchors, provider context, and Store-owned
    /// memory/command/phase state, and returns either physical recovery intents
    /// (boot remains fail-closed until T27 injects a receipt) or a complete
    /// `HydratedRunState` with a stable `HydrationReceiptIdentity`.
    #[allow(dead_code, reason = "T26 injects the production hydration lease/fence")]
    pub(crate) async fn hydrate(
        &self,
        lease: &ProcessGenerationLease,
        fence: &GenerationRecoveryFence,
    ) -> Result<HydrationOutcome> {
        self.validate_startup().await?;

        lease
            .validate_exact(fence.generation(), fence.lease_id())
            .map_err(|error| anyhow!("invalid recovery lease/fence binding: {error}"))?;
        fence
            .validate_exact(lease, fence.fence_id())
            .map_err(|error| anyhow!("invalid recovery fence binding: {error}"))?;

        let intents = self.hydrate_running_intents().await?;
        if !intents.is_empty() {
            return Ok(HydrationOutcome::RecoveryRequired(intents));
        }

        let messages = self.hydrate_messages().await?;
        let provider_context = self.hydrate_provider_context(&messages).await?;
        let (memory_batches, memory_batch_messages, memory_jobs, memory_apply_cursors) =
            self.hydrate_memory_state().await?;

        let recovery_steps = SuffixRecovery::plan_full_suffix(self).await?;

        let receipt = HydrationReceiptIdentity {
            lease_id: lease.lease_id().to_owned(),
            generation: lease.generation(),
            fence_id: fence.fence_id().to_owned(),
            intent_count: 0,
        };

        Ok(HydrationOutcome::Complete(HydratedRunState {
            scope: self.scope.clone(),
            lease: lease.clone(),
            fence: fence.clone(),
            receipt,
            messages,
            provider_context,
            memory_batches,
            memory_batch_messages,
            memory_jobs,
            memory_apply_cursors,
            recovery_steps,
        }))
    }

    /// Hydrate only the T17 physical-recovery boundary.  T17 validates the
    /// injected lease/fence and returns immutable running-tool attestations;
    /// T27 owns the physical kill/reap and proof persistence.  A clean state
    /// returns a stable receipt identity, while any running execution keeps
    /// hydration not-ready until a matching receipt is applied through
    /// EventWriter.
    #[allow(dead_code, reason = "T26 injects the production hydration lease/fence")]
    pub(crate) async fn hydrate_recovery_intents(
        &self,
        lease: &ProcessGenerationLease,
        fence: &GenerationRecoveryFence,
    ) -> Result<(
        Vec<PhysicalRecoveryIntentRequest>,
        Option<HydrationReceiptIdentity>,
    )> {
        match self.hydrate(lease, fence).await? {
            HydrationOutcome::RecoveryRequired(intents) => Ok((intents, None)),
            HydrationOutcome::Complete(state) => Ok((Vec::new(), Some(state.receipt))),
        }
    }

    async fn hydrate_running_intents(&self) -> Result<Vec<PhysicalRecoveryIntentRequest>> {
        let rows = sqlx::query(
            "SELECT tool_call_id, command_id, run_id, executor_generation
             FROM tool_executions WHERE state = 'running' ORDER BY tool_call_id",
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to hydrate running tool executions")?;
        let mut intents = Vec::with_capacity(rows.len());
        for row in rows {
            let tool_call_id: String = row.try_get("tool_call_id")?;
            let command_id: String = row.try_get("command_id")?;
            let run_id: String = row.try_get("run_id")?;
            if tool_call_id.is_empty() || command_id.is_empty() || run_id.is_empty() {
                bail!("running tool execution identity must not be empty");
            }
            let generation = ProcessGeneration::from_sqlite(row.try_get("executor_generation")?)
                .map_err(|error| anyhow!("invalid persisted executor generation: {error}"))?;
            intents.push(PhysicalRecoveryIntentRequest {
                tool_call_id,
                command_id,
                run_id,
                executor_generation: generation,
            });
        }
        Ok(intents)
    }

    async fn hydrate_messages(&self) -> Result<Vec<ContextMessage>> {
        let mut key_cache: HashMap<String, Arc<DataKeyMaterial>> = HashMap::new();

        let rows = sqlx::query(
            "SELECT id, seq, role, raw_key_ref, raw_ciphertext, redaction_version, interrupted
             FROM messages ORDER BY seq",
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to hydrate transcript messages")?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("id")?;
            let seq: i64 = row.try_get("seq")?;
            let seq =
                u64::try_from(seq).with_context(|| format!("message {id} seq out of u64 range"))?;
            let stored_role: String = row.try_get("role")?;
            let key_ref: String = row.try_get("raw_key_ref")?;
            let redaction_version: i64 = row.try_get("redaction_version")?;
            if redaction_version != i64::from(self.redactor.version()) {
                bail!("message {id} uses an unsupported redaction version");
            }
            let interrupted: i64 = row.try_get("interrupted")?;
            let interrupted = interrupted != 0;

            let key = self.load_hydration_key(&mut key_cache, &key_ref).await?;
            let ciphertext: Vec<u8> = row.try_get("raw_ciphertext")?;
            let aad = self
                .scope
                .row_aad("messages", &id, DataKeyPurpose::Transcript);
            let plaintext = decrypt_content(&key, &ciphertext, &aad)
                .with_context(|| format!("failed to decrypt transcript message {id}"))?;
            let public: PublicMessage = serde_json::from_slice(&plaintext)
                .with_context(|| format!("transcript message {id} is not a valid PublicMessage"))?;

            if public_message_role(&public) != stored_role {
                bail!("message {id} role does not match decrypted public message");
            }
            if message_interrupted(&public) != interrupted {
                bail!("message {id} interrupted flag does not match decrypted public message");
            }

            messages.push(ContextMessage::Persisted {
                id: id.clone(),
                seq,
                message: Message::from(public),
            });
        }
        Ok(messages)
    }

    async fn hydrate_provider_context(
        &self,
        messages: &[ContextMessage],
    ) -> Result<Vec<ProviderContextItem>> {
        let mut key_cache: HashMap<String, Arc<DataKeyMaterial>> = HashMap::new();

        let rows = sqlx::query(
            "SELECT id, message_id, message_seq, wire_item_index, item_ordinal,
                    idempotency_key, kind, coverage_through_seq, context_fingerprint,
                    key_ref, ciphertext
             FROM provider_context
             ORDER BY message_seq NULLS LAST, wire_item_index NULLS LAST, item_ordinal",
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to hydrate provider context")?;

        let mut provider_context = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("id")?;
            let stored_message_id: Option<String> = row.try_get("message_id")?;
            let stored_message_seq: Option<i64> = row.try_get("message_seq")?;
            let stored_wire_item_index: Option<i64> = row.try_get("wire_item_index")?;
            let stored_item_ordinal: i64 = row.try_get("item_ordinal")?;
            let stored_idempotency_key: String = row.try_get("idempotency_key")?;
            let stored_kind: String = row.try_get("kind")?;
            let stored_coverage_seq: Option<i64> = row.try_get("coverage_through_seq")?;
            let stored_fingerprint: Option<String> = row.try_get("context_fingerprint")?;
            let key_ref: String = row.try_get("key_ref")?;

            let key = self.load_hydration_key(&mut key_cache, &key_ref).await?;
            let ciphertext: Vec<u8> = row.try_get("ciphertext")?;
            let aad = self
                .scope
                .row_aad("provider_context", &id, DataKeyPurpose::ProviderContext);
            let plaintext = decrypt_content(&key, &ciphertext, &aad)
                .with_context(|| format!("failed to decrypt provider-context record {id}"))?;
            let item: ProviderContextItem =
                serde_json::from_slice(&plaintext).with_context(|| {
                    format!("provider-context record {id} is not a valid ProviderContextItem")
                })?;

            if item.origin_message.as_ref().map(|a| a.message_id.as_str())
                != stored_message_id.as_deref()
            {
                bail!("provider-context record {id} message_id does not match decrypted anchor");
            }
            let stored_message_seq_u64 = stored_message_seq
                .map(|v| {
                    u64::try_from(v).with_context(|| {
                        format!("provider-context record {id} message_seq out of u64 range")
                    })
                })
                .transpose()?;
            if item.origin_message.as_ref().map(|a| a.message_seq) != stored_message_seq_u64 {
                bail!("provider-context record {id} message_seq does not match decrypted anchor");
            }
            let stored_wire_u32 = stored_wire_item_index
                .map(|v| {
                    u32::try_from(v).with_context(|| {
                        format!("provider-context record {id} wire_item_index out of u32 range")
                    })
                })
                .transpose()?;
            if item.wire_item_index != stored_wire_u32 {
                bail!("provider-context record {id} wire_item_index does not match decrypted item");
            }
            let stored_ordinal_u32 = u32::try_from(stored_item_ordinal).with_context(|| {
                format!("provider-context record {id} item_ordinal out of u32 range")
            })?;
            if item.ordinal != stored_ordinal_u32 {
                bail!("provider-context record {id} item_ordinal does not match decrypted item");
            }
            if ProviderContextKind::from_payload(&item.payload).as_str() != stored_kind {
                bail!("provider-context record {id} kind does not match decrypted payload");
            }

            match &item.payload {
                ProviderContextPayload::OpenAiCompactedWindow { coverage, .. }
                | ProviderContextPayload::AnthropicCompaction { coverage, .. } => {
                    let expected_seq = stored_coverage_seq
                        .map(|v| {
                            u64::try_from(v).with_context(|| {
                                format!(
                                    "provider-context record {id} coverage seq out of u64 range"
                                )
                            })
                        })
                        .transpose()?;
                    if Some(coverage.through_message_seq) != expected_seq {
                        bail!(
                            "provider-context record {id} coverage_through_seq does not match decrypted payload"
                        );
                    }
                    if stored_fingerprint.as_deref() != Some(coverage.context_fingerprint.as_str())
                    {
                        bail!(
                            "provider-context record {id} context_fingerprint does not match decrypted payload"
                        );
                    }
                }
                ProviderContextPayload::EncryptedReasoning { .. } => {
                    if stored_coverage_seq.is_some() || stored_fingerprint.is_some() {
                        bail!(
                            "provider-context record {id} reasoning payload must not carry coverage metadata"
                        );
                    }
                    let anchor = item.origin_message.as_ref().ok_or_else(|| {
                        anyhow!(
                            "provider-context record {id} encrypted reasoning is missing an anchor"
                        )
                    })?;
                    let expected_idempotency_key =
                        self::provider_context::provider_context_idempotency_key(
                            &anchor.message_id,
                            &item,
                        );
                    if stored_idempotency_key != expected_idempotency_key {
                        bail!(
                            "provider-context record {id} idempotency key does not match decrypted reasoning item"
                        );
                    }
                }
            }

            if let Some(anchor) = &item.origin_message {
                let anchor_found = messages.iter().any(|message| match message {
                    ContextMessage::Persisted { id, seq, message } => {
                        id == &anchor.message_id
                            && *seq == anchor.message_seq
                            && matches!(message, Message::Assistant(_))
                    }
                    ContextMessage::Synthetic { .. } => false,
                });
                if !anchor_found {
                    bail!(
                        "provider-context record {id} anchor {}:{} does not resolve to a persisted assistant message",
                        anchor.message_id,
                        anchor.message_seq
                    );
                }
            }

            provider_context.push(item);
        }
        Ok(provider_context)
    }

    async fn hydrate_memory_state(
        &self,
    ) -> Result<(
        Vec<MemoryBatchRecord>,
        Vec<MemoryBatchMessageRecord>,
        Vec<MemoryJobRecord>,
        Vec<MemoryApplyCursorRecord>,
    )> {
        let batch_rows = sqlx::query(
            "SELECT id, layer, ord, batch_seq, version, state, est_tokens,
                    eviction_footprint_tokens, summary_key_ref, summary_ciphertext,
                    summary_projection, summary_redaction_version, updated_at
             FROM memory_batches ORDER BY layer, ord",
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to hydrate memory batches")?;

        let mut memory_batches = Vec::with_capacity(batch_rows.len());
        for row in batch_rows {
            let summary = match (
                row.try_get::<Option<String>, _>("summary_key_ref")?,
                row.try_get::<Option<Vec<u8>>, _>("summary_ciphertext")?,
                row.try_get::<Option<String>, _>("summary_projection")?,
                row.try_get::<Option<i64>, _>("summary_redaction_version")?,
            ) {
                (Some(key_ref), Some(ciphertext), Some(projection), Some(version)) => {
                    Some(MemoryBatchSummary {
                        key_ref,
                        ciphertext,
                        projection,
                        redaction_version: u32::try_from(version)
                            .with_context(|| "memory batch redaction version out of u32 range")?,
                    })
                }
                (None, None, None, None) => None,
                _ => bail!("memory batch summary fields are inconsistent"),
            };

            let layer = row.try_get::<i64, _>("layer")?;
            let layer = MemoryLayer::from_i64(layer)
                .ok_or_else(|| anyhow!("memory batch has unknown layer {layer}"))?;
            let state: String = row.try_get("state")?;
            let state = MemoryBatchState::from_str(&state)
                .ok_or_else(|| anyhow!("memory batch has unknown state {state}"))?;

            memory_batches.push(MemoryBatchRecord {
                id: row.try_get("id")?,
                layer,
                ord: row.try_get("ord")?,
                batch_seq: row.try_get("batch_seq")?,
                version: row.try_get("version")?,
                state,
                est_tokens: row.try_get("est_tokens")?,
                eviction_footprint_tokens: row.try_get("eviction_footprint_tokens")?,
                summary,
                updated_at: row.try_get("updated_at")?,
            });
        }

        let batch_message_rows = sqlx::query(
            "SELECT batch_id, message_id, ord FROM memory_batch_messages ORDER BY batch_id, ord",
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to hydrate memory batch messages")?;
        let mut memory_batch_messages = Vec::with_capacity(batch_message_rows.len());
        for row in batch_message_rows {
            memory_batch_messages.push(MemoryBatchMessageRecord {
                batch_id: row.try_get("batch_id")?,
                message_id: row.try_get("message_id")?,
                ord: row.try_get("ord")?,
            });
        }

        let job_rows = sqlx::query(
            "SELECT id, kind, batch_seq, source_ids, source_versions, status, lease_until, attempts,
                    result_key_ref, result_ciphertext, result_projection, result_redaction_version,
                    created_at, updated_at
             FROM memory_jobs ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to hydrate memory jobs")?;
        let mut memory_jobs = Vec::with_capacity(job_rows.len());
        for row in job_rows {
            let result = match (
                row.try_get::<Option<String>, _>("result_key_ref")?,
                row.try_get::<Option<Vec<u8>>, _>("result_ciphertext")?,
                row.try_get::<Option<String>, _>("result_projection")?,
                row.try_get::<Option<i64>, _>("result_redaction_version")?,
            ) {
                (Some(key_ref), Some(ciphertext), Some(projection), Some(version)) => {
                    Some(MemoryJobResult {
                        key_ref,
                        ciphertext,
                        projection,
                        redaction_version: u32::try_from(version).with_context(
                            || "memory job result redaction version out of u32 range",
                        )?,
                    })
                }
                (None, None, None, None) => None,
                _ => bail!("memory job result fields are inconsistent"),
            };

            let kind: String = row.try_get("kind")?;
            let kind = MemoryJobKind::from_str(&kind)
                .ok_or_else(|| anyhow!("memory job has unknown kind {kind}"))?;
            let status: String = row.try_get("status")?;
            let status = MemoryJobStatus::from_str(&status)
                .ok_or_else(|| anyhow!("memory job has unknown status {status}"))?;
            let source_ids: String = row.try_get("source_ids")?;
            let source_ids: Vec<String> = serde_json::from_str(&source_ids)
                .context("memory job source_ids is not valid JSON")?;
            let source_versions: String = row.try_get("source_versions")?;
            let source_versions: std::collections::BTreeMap<String, i64> =
                serde_json::from_str(&source_versions)
                    .context("memory job source_versions is not valid JSON")?;

            memory_jobs.push(MemoryJobRecord {
                id: row.try_get("id")?,
                kind,
                batch_seq: row.try_get("batch_seq")?,
                source_ids,
                source_versions,
                status,
                lease_until: row.try_get("lease_until")?,
                attempts: row.try_get("attempts")?,
                result,
                created_at: row.try_get("created_at")?,
                updated_at: row.try_get("updated_at")?,
            });
        }

        let cursor_rows =
            sqlx::query("SELECT kind, next_batch_seq FROM memory_apply_cursors ORDER BY kind")
                .fetch_all(&self.pool)
                .await
                .context("failed to hydrate memory apply cursors")?;
        let mut memory_apply_cursors = Vec::with_capacity(cursor_rows.len());
        for row in cursor_rows {
            memory_apply_cursors.push(MemoryApplyCursorRecord {
                kind: row.try_get("kind")?,
                next_batch_seq: row.try_get("next_batch_seq")?,
            });
        }

        Ok((
            memory_batches,
            memory_batch_messages,
            memory_jobs,
            memory_apply_cursors,
        ))
    }

    async fn load_hydration_key(
        &self,
        cache: &mut HashMap<String, Arc<DataKeyMaterial>>,
        key_ref: &str,
    ) -> Result<Arc<DataKeyMaterial>> {
        if let Some(key) = cache.get(key_ref) {
            return Ok(key.clone());
        }
        let key = self
            .data_key_by_ref(key_ref)
            .await
            .with_context(|| format!("failed to load hydration data key {key_ref}"))?;
        let key = Arc::new(key);
        cache.insert(key_ref.to_owned(), key.clone());
        Ok(key)
    }

    pub(crate) fn scope(&self) -> &AgentScope {
        &self.scope
    }

    pub(crate) fn redactor(&self) -> &Redactor {
        &self.redactor
    }

    fn event_writer_state(&self) -> Arc<Mutex<event_writer::WriterState>> {
        self.event_writer_state.clone()
    }

    async fn validate_startup(&self) -> Result<()> {
        let quick_check: String = sqlx::query_scalar("PRAGMA quick_check")
            .fetch_one(&self.pool)
            .await
            .context("failed to run SQLite quick_check")?;
        if quick_check != "ok" {
            bail!("SQLite quick_check failed: {quick_check}");
        }

        let foreign_key_violation = sqlx::query("PRAGMA foreign_key_check")
            .fetch_optional(&self.pool)
            .await
            .context("failed to run SQLite foreign_key_check")?;
        if foreign_key_violation.is_some() {
            bail!("SQLite foreign_key_check found a violation");
        }

        let rows = sqlx::query(
            "SELECT tenant_id, agent_id, conversation_id FROM agent_scope ORDER BY singleton",
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to read agent scope")?;
        if rows.len() != 1 {
            bail!("agent_scope must contain exactly one row");
        }
        let row = &rows[0];
        let stored = AgentScope {
            tenant_id: row.try_get("tenant_id")?,
            agent_id: row.try_get("agent_id")?,
            conversation_id: row.try_get("conversation_id")?,
        };
        if stored != self.scope {
            bail!(
                "database scope does not match authenticated runtime scope: expected {:?}, found {:?}",
                self.scope,
                stored
            );
        }

        // Public projections are readable without decrypting their raw source,
        // so an unsupported rule version must stop startup before any command
        // admission. These bounded existence probes avoid replaying projections
        // whose exact event parity is authenticated separately by EventWriter.
        for (table, column, label) in [
            ("messages", "redaction_version", "message"),
            ("approval_log", "redaction_version", "approval"),
            ("agent_events", "redaction_version", "event"),
            (
                "memory_batches",
                "summary_redaction_version",
                "memory batch",
            ),
            ("memory_jobs", "result_redaction_version", "memory job"),
        ] {
            let sql = format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE {column} <> ? LIMIT 1)");
            let unsupported: i64 = sqlx::query_scalar(&sql)
                .bind(i64::from(self.redactor.version()))
                .fetch_one(&self.pool)
                .await
                .with_context(|| format!("failed to validate {label} redaction versions"))?;
            if unsupported != 0 {
                bail!("persisted {label} projection uses an unsupported redaction version");
            }
        }

        let active_keys = sqlx::query(
            "SELECT key_ref, scope, purpose, conversation_id, algorithm,
                    wrap_key_id, wrap_nonce, wrapped_key
             FROM data_keys WHERE state = 'active' ORDER BY key_ref",
        )
        .fetch_all(&self.pool)
        .await
        .context("failed to validate active data keys")?;
        for row in active_keys {
            let purpose = DataKeyPurpose::parse(row.try_get("purpose")?)?;
            let key_ref: String = row.try_get("key_ref")?;
            let algorithm: String = row.try_get("algorithm")?;
            if algorithm != WRAP_ALGORITHM {
                bail!("active data key {key_ref} has unsupported algorithm {algorithm}");
            }
            let key_scope = match row.try_get::<String, _>("scope")?.as_str() {
                "conversation" => DataKeyScope::Conversation,
                "agent" => DataKeyScope::Agent,
                value => bail!("active data key {key_ref} has unknown scope {value}"),
            };
            let conversation_id: Option<String> = row.try_get("conversation_id")?;
            match key_scope {
                DataKeyScope::Conversation
                    if conversation_id.as_deref() != Some(self.scope.conversation_id.as_str()) =>
                {
                    bail!("active conversation key {key_ref} is bound to the wrong conversation");
                }
                DataKeyScope::Agent if conversation_id.is_some() => {
                    bail!("active agent key {key_ref} unexpectedly has a conversation");
                }
                DataKeyScope::Agent if purpose != DataKeyPurpose::Workspace => {
                    bail!(
                        "active agent key {key_ref} has purpose {purpose} but agent scope only permits workspace",
                        purpose = purpose.as_str()
                    );
                }
                _ => {}
            }
            let wrap_key_id: String = row.try_get("wrap_key_id")?;
            let wrapping_key = self
                .key_provider
                .key_by_id(&wrap_key_id)
                .await
                .with_context(|| format!("failed to obtain wrapping key {wrap_key_id}"))?;
            let aad = KeyWrapAad {
                key_ref: key_ref.clone(),
                scope: key_scope,
                purpose,
                conversation_id,
                wrap_key_id,
            };
            unwrap_data_key(
                key_ref,
                purpose,
                row.try_get::<Vec<u8>, _>("wrapped_key")?.as_slice(),
                row.try_get::<Vec<u8>, _>("wrap_nonce")?.as_slice(),
                &wrapping_key,
                &aad,
            )
            .context("active data-key row failed authenticated startup validation")?;
        }
        Ok(())
    }

    pub(crate) async fn conversation_key(
        &self,
        purpose: DataKeyPurpose,
    ) -> Result<DataKeyMaterial> {
        if purpose == DataKeyPurpose::Workspace {
            bail!("workspace keys are agent-scoped");
        }
        if purpose == DataKeyPurpose::ProviderContext {
            bail!("provider-context keys require a caller-stable authenticated anchor");
        }
        if let Some(key) = self.load_active_conversation_key(purpose).await? {
            return Ok(key);
        }

        let wrapping_key = self.key_provider.current_key().await?;
        let key_ref = format!("{}-{}", purpose.as_str(), Uuid::now_v7());
        let data_key = DataKeyMaterial::generate(&key_ref, purpose)?;
        let aad = KeyWrapAad {
            key_ref: key_ref.clone(),
            scope: DataKeyScope::Conversation,
            purpose,
            conversation_id: Some(self.scope.conversation_id.clone()),
            wrap_key_id: wrapping_key.key_id().to_owned(),
        };
        let (wrap_nonce, wrapped_key) = wrap_data_key(&data_key, &wrapping_key, &aad)?;
        let result = sqlx::query(
            "INSERT INTO data_keys(
                key_ref, scope, purpose, conversation_id, algorithm, wrap_key_id,
                wrap_nonce, wrapped_key, state, created_at, destroyed_at
             ) VALUES(?, 'conversation', ?, ?, ?, ?, ?, ?, 'active', ?, NULL)",
        )
        .bind(&key_ref)
        .bind(purpose.as_str())
        .bind(&self.scope.conversation_id)
        .bind(WRAP_ALGORITHM)
        .bind(wrapping_key.key_id())
        .bind(wrap_nonce.as_slice())
        .bind(wrapped_key)
        .bind(Utc::now().to_rfc3339())
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(data_key),
            Err(error) if is_unique_violation(&error) => self
                .load_active_conversation_key(purpose)
                .await?
                .ok_or_else(|| anyhow!("active data key disappeared after creation race")),
            Err(error) => Err(error).context("failed to persist wrapped conversation data key"),
        }
    }

    pub(crate) async fn command_digest_factory(
        &self,
    ) -> Result<Arc<dyn crate::gateway::CommandDigestFactory>> {
        let key = self.conversation_key(DataKeyPurpose::Command).await?;
        Ok(Arc::new(ConversationCommandDigestFactory::new(&key)?))
    }

    async fn load_active_conversation_key(
        &self,
        purpose: DataKeyPurpose,
    ) -> Result<Option<DataKeyMaterial>> {
        if purpose == DataKeyPurpose::ProviderContext {
            bail!("provider-context keys are not conversation-shared");
        }
        let row = sqlx::query(
            "SELECT key_ref, wrap_key_id, wrap_nonce, wrapped_key
             FROM data_keys
             WHERE scope = 'conversation' AND conversation_id = ? AND purpose = ?
               AND state = 'active'",
        )
        .bind(&self.scope.conversation_id)
        .bind(purpose.as_str())
        .fetch_optional(&self.pool)
        .await
        .context("failed to load conversation data key")?;
        let Some(row) = row else {
            return Ok(None);
        };
        let key_ref: String = row.try_get("key_ref")?;
        let wrap_key_id: String = row.try_get("wrap_key_id")?;
        let wrapping_key = self.key_provider.key_by_id(&wrap_key_id).await?;
        let aad = KeyWrapAad {
            key_ref: key_ref.clone(),
            scope: DataKeyScope::Conversation,
            purpose,
            conversation_id: Some(self.scope.conversation_id.clone()),
            wrap_key_id,
        };
        unwrap_data_key(
            key_ref,
            purpose,
            row.try_get::<Vec<u8>, _>("wrapped_key")?.as_slice(),
            row.try_get::<Vec<u8>, _>("wrap_nonce")?.as_slice(),
            &wrapping_key,
            &aad,
        )
        .map(Some)
    }

    #[allow(
        dead_code,
        reason = "T11 freezes the per-anchor key boundary before provider-context persistence is wired"
    )]
    pub(crate) async fn provider_context_key(
        &self,
        anchor: &ProviderContextKeyAnchor,
    ) -> Result<DataKeyMaterial> {
        if anchor.conversation_id != self.scope.conversation_id {
            bail!("provider-context anchor belongs to a different authenticated conversation");
        }
        if anchor.anchor_id.is_empty() {
            bail!("provider-context anchor identity must not be empty");
        }

        let key_ref = provider_context_key_ref(&self.scope, &anchor.anchor_id);
        let existing_state: Option<String> =
            sqlx::query_scalar("SELECT state FROM data_keys WHERE key_ref = ?")
                .bind(&key_ref)
                .fetch_optional(&self.pool)
                .await
                .context("failed to inspect provider-context anchor key")?;
        if let Some(state) = existing_state {
            if state != "active" {
                bail!("provider-context anchor key has been crypto-erased");
            }
            let key = self.data_key_by_ref(&key_ref).await?;
            if key.purpose != DataKeyPurpose::ProviderContext {
                bail!("provider-context anchor resolved to a key with the wrong purpose");
            }
            return Ok(key);
        }

        let wrapping_key = self.key_provider.current_key().await?;
        let purpose = DataKeyPurpose::ProviderContext;
        let data_key = DataKeyMaterial::generate(&key_ref, purpose)?;
        let aad = KeyWrapAad {
            key_ref: key_ref.clone(),
            scope: DataKeyScope::Conversation,
            purpose,
            conversation_id: Some(self.scope.conversation_id.clone()),
            wrap_key_id: wrapping_key.key_id().to_owned(),
        };
        let (wrap_nonce, wrapped_key) = wrap_data_key(&data_key, &wrapping_key, &aad)?;
        let result = sqlx::query(
            "INSERT INTO data_keys(
                key_ref, scope, purpose, conversation_id, algorithm, wrap_key_id,
                wrap_nonce, wrapped_key, state, created_at, destroyed_at
             ) VALUES(?, 'conversation', 'provider_context', ?, ?, ?, ?, ?, 'active', ?, NULL)",
        )
        .bind(&key_ref)
        .bind(&self.scope.conversation_id)
        .bind(WRAP_ALGORITHM)
        .bind(wrapping_key.key_id())
        .bind(wrap_nonce.as_slice())
        .bind(wrapped_key)
        .bind(Utc::now().to_rfc3339())
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(data_key),
            Err(error) if is_unique_violation(&error) => {
                let key = self
                    .data_key_by_ref(&key_ref)
                    .await
                    .context("provider-context anchor key is not active after creation race")?;
                if key.purpose != DataKeyPurpose::ProviderContext {
                    bail!("provider-context anchor resolved to a key with the wrong purpose");
                }
                Ok(key)
            }
            Err(error) => Err(error).context("failed to persist provider-context anchor key"),
        }
    }

    pub(crate) async fn data_key_by_ref(&self, key_ref: &str) -> Result<DataKeyMaterial> {
        let row = sqlx::query(
            "SELECT purpose, conversation_id, wrap_key_id, wrap_nonce, wrapped_key
             FROM data_keys
             WHERE key_ref = ? AND scope = 'conversation' AND state = 'active'",
        )
        .bind(key_ref)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load data key by reference")?
        .ok_or_else(|| anyhow!("active data key {key_ref} is unavailable"))?;
        let purpose = DataKeyPurpose::parse(row.try_get("purpose")?)?;
        let conversation_id: Option<String> = row.try_get("conversation_id")?;
        if conversation_id.as_deref() != Some(self.scope.conversation_id.as_str()) {
            bail!("data key {key_ref} belongs to a different conversation");
        }
        let wrap_key_id: String = row.try_get("wrap_key_id")?;
        let wrapping_key = self.key_provider.key_by_id(&wrap_key_id).await?;
        let aad = KeyWrapAad {
            key_ref: key_ref.to_owned(),
            scope: DataKeyScope::Conversation,
            purpose,
            conversation_id,
            wrap_key_id,
        };
        unwrap_data_key(
            key_ref,
            purpose,
            row.try_get::<Vec<u8>, _>("wrapped_key")?.as_slice(),
            row.try_get::<Vec<u8>, _>("wrap_nonce")?.as_slice(),
            &wrapping_key,
            &aad,
        )
    }

    pub(crate) async fn data_key_by_ref_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        key_ref: &str,
    ) -> Result<DataKeyMaterial> {
        let row = sqlx::query(
            "SELECT purpose, conversation_id, wrap_key_id, wrap_nonce, wrapped_key
             FROM data_keys
             WHERE key_ref = ? AND scope = 'conversation' AND state = 'active'",
        )
        .bind(key_ref)
        .fetch_optional(&mut **transaction)
        .await
        .context("failed to load data key by reference in EventBatch")?
        .ok_or_else(|| anyhow!("active data key {key_ref} is unavailable"))?;
        let purpose = DataKeyPurpose::parse(row.try_get("purpose")?)?;
        let conversation_id: Option<String> = row.try_get("conversation_id")?;
        if conversation_id.as_deref() != Some(self.scope.conversation_id.as_str()) {
            bail!("data key {key_ref} belongs to a different conversation");
        }
        let wrap_key_id: String = row.try_get("wrap_key_id")?;
        let wrapping_key = self.key_provider.key_by_id(&wrap_key_id).await?;
        let aad = KeyWrapAad {
            key_ref: key_ref.to_owned(),
            scope: DataKeyScope::Conversation,
            purpose,
            conversation_id,
            wrap_key_id,
        };
        unwrap_data_key(
            key_ref,
            purpose,
            row.try_get::<Vec<u8>, _>("wrapped_key")?.as_slice(),
            row.try_get::<Vec<u8>, _>("wrap_nonce")?.as_slice(),
            &wrapping_key,
            &aad,
        )
    }

    /// Transactionally destroys one conversation-owned data key by its durable
    /// reference. This is the narrow product boundary used by conversation
    /// reset and provider-context anchor eviction.
    #[allow(
        dead_code,
        reason = "T11 product crypto-erase boundary is wired to lifecycle callers in M3"
    )]
    pub(crate) async fn destroy_conversation_key_ref(&self, key_ref: &str) -> Result<()> {
        if key_ref.is_empty() {
            bail!("crypto-erase key_ref must not be empty");
        }
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT scope, purpose, conversation_id, state, wrapped_key, wrap_nonce, destroyed_at
             FROM data_keys WHERE key_ref = ?",
        )
        .bind(key_ref)
        .fetch_optional(&mut *transaction)
        .await
        .context("failed to load crypto-erase target")?
        .ok_or_else(|| anyhow!("crypto-erase key_ref {key_ref} does not exist"))?;

        let scope: String = row.try_get("scope")?;
        let purpose = DataKeyPurpose::parse(row.try_get("purpose")?)?;
        let conversation_id: Option<String> = row.try_get("conversation_id")?;
        if scope != DataKeyScope::Conversation.as_str()
            || conversation_id.as_deref() != Some(self.scope.conversation_id.as_str())
            || purpose == DataKeyPurpose::Workspace
        {
            bail!("crypto-erase key_ref {key_ref} is outside the active conversation scope");
        }

        let state: String = row.try_get("state")?;
        match state.as_str() {
            "active" => {
                if row.try_get::<Option<Vec<u8>>, _>("wrapped_key")?.is_none()
                    || row.try_get::<Option<Vec<u8>>, _>("wrap_nonce")?.is_none()
                    || row.try_get::<Option<String>, _>("destroyed_at")?.is_some()
                {
                    bail!("active key_ref {key_ref} has incomplete wrapped key material");
                }
                let result = sqlx::query(
                    "UPDATE data_keys
                     SET state = 'destroyed', wrapped_key = NULL, wrap_nonce = NULL,
                         destroyed_at = ?
                     WHERE key_ref = ? AND scope = 'conversation'
                       AND conversation_id = ? AND state = 'active'",
                )
                .bind(Utc::now().to_rfc3339())
                .bind(key_ref)
                .bind(&self.scope.conversation_id)
                .execute(&mut *transaction)
                .await?;
                if result.rows_affected() != 1 {
                    bail!("crypto-erase CAS failed for key_ref {key_ref}");
                }
            }
            "destroyed"
                if row.try_get::<Option<Vec<u8>>, _>("wrapped_key")?.is_none()
                    && row.try_get::<Option<Vec<u8>>, _>("wrap_nonce")?.is_none()
                    && row.try_get::<Option<String>, _>("destroyed_at")?.is_some() => {}
            "destroyed" => {
                bail!("destroyed key_ref {key_ref} retains wrapped key material");
            }
            value => bail!("crypto-erase key_ref {key_ref} has invalid state {value}"),
        }
        transaction.commit().await?;
        Ok(())
    }
}

#[allow(
    dead_code,
    reason = "used by the T11 per-anchor key boundary before its production caller exists"
)]
fn provider_context_key_ref(scope: &AgentScope, anchor_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"sumi-provider-context-anchor/v1");
    for field in [
        scope.tenant_id.as_bytes(),
        scope.agent_id.as_bytes(),
        scope.conversation_id.as_bytes(),
        anchor_id.as_bytes(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    format!("provider_context-{:x}", digest.finalize())
}

#[cfg(unix)]
async fn prepare_state_path(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("SQLite database path has no state directory"))?;
    tokio::fs::create_dir_all(parent)
        .await
        .with_context(|| format!("failed to create state directory {}", parent.display()))?;
    secure_path(parent, 0o700, true).await?;

    let database_path = path.to_owned();
    let open_path = database_path.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let metadata = std::fs::symlink_metadata(&open_path).ok();
        if metadata.as_ref().is_some_and(|metadata| {
            metadata.file_type().is_symlink() || !metadata.file_type().is_file()
        }) {
            bail!(
                "SQLite database path {} must be a regular file",
                open_path.display()
            );
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&open_path)
            .with_context(|| format!("failed to securely create {}", open_path.display()))?;
        let metadata = file.metadata()?;
        validate_owned(&open_path, &metadata)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        Ok(())
    })
    .await
    .context("secure SQLite file preparation task failed")??;
    secure_sqlite_files(&database_path).await?;
    Ok(())
}

#[cfg(not(unix))]
async fn prepare_state_path(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create state directory {}", parent.display()))?;
    }
    Ok(())
}

#[cfg(unix)]
async fn secure_sqlite_files(path: &Path) -> Result<()> {
    for candidate in [
        path.to_owned(),
        path.with_file_name(format!(
            "{}-wal",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("")
        )),
        path.with_file_name(format!(
            "{}-shm",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("")
        )),
    ] {
        if tokio::fs::try_exists(&candidate).await? {
            secure_path(&candidate, 0o600, false).await?;
        }
    }
    Ok(())
}

#[cfg(not(unix))]
async fn secure_sqlite_files(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
async fn secure_path(path: &Path, mode: u32, directory: bool) -> Result<()> {
    let metadata = tokio::fs::symlink_metadata(path).await?;
    let valid_type = if directory {
        metadata.file_type().is_dir()
    } else {
        metadata.file_type().is_file()
    };
    if metadata.file_type().is_symlink() || !valid_type {
        bail!(
            "state path {} has an unsafe filesystem type",
            path.display()
        );
    }
    validate_owned(path, &metadata)?;
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).await?;
    let secured = tokio::fs::symlink_metadata(path).await?;
    if secured.mode() & 0o777 != mode {
        bail!(
            "state path {} permissions are {:o}, expected {:o}",
            path.display(),
            secured.mode() & 0o777,
            mode
        );
    }
    Ok(())
}

#[cfg(unix)]
fn validate_owned(path: &Path, metadata: &std::fs::Metadata) -> Result<()> {
    // SAFETY: geteuid has no preconditions and does not retain pointers.
    let effective_uid: libc::uid_t = unsafe { libc::geteuid() };
    if metadata.uid() != effective_uid {
        bail!(
            "state path {} is owned by uid {}, runtime uid is {}",
            path.display(),
            metadata.uid(),
            effective_uid
        );
    }
    Ok(())
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|error| error.code())
        .is_some_and(|code| code == "2067" || code == "1555")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::crypto::{DATA_KEY_BYTES, WrappingKey, decrypt_content, encrypt_content};

    struct TestKeyProvider {
        key: WrappingKey,
    }

    #[async_trait::async_trait]
    impl KeyProvider for TestKeyProvider {
        async fn current_key(&self) -> Result<WrappingKey> {
            Ok(self.key.clone())
        }

        async fn key_by_id(&self, key_id: &str) -> Result<WrappingKey> {
            if key_id != self.key.key_id() {
                bail!("unknown test key");
            }
            Ok(self.key.clone())
        }
    }

    fn scope() -> AgentScope {
        AgentScope {
            tenant_id: "tenant-1".to_owned(),
            agent_id: "agent-1".to_owned(),
            conversation_id: "conversation-1".to_owned(),
        }
    }

    fn provider() -> Arc<dyn KeyProvider> {
        Arc::new(TestKeyProvider {
            key: WrappingKey::new("test-wrap-v1", [0x42; DATA_KEY_BYTES]),
        })
    }

    async fn store() -> Store {
        Store::in_memory(scope(), provider())
            .await
            .expect("open in-memory store")
    }

    fn assert_not_null_violation(error: &sqlx::Error) {
        let database_error = error
            .as_database_error()
            .expect("constraint violation must be a database error");
        assert_eq!(
            database_error.kind(),
            sqlx::error::ErrorKind::NotNullViolation
        );
    }

    #[tokio::test]
    async fn migration_rejects_null_text_primary_key_identities() {
        let store = store().await;
        let transcript_key = store
            .conversation_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint transcript key");
        let event_key = store
            .conversation_key(DataKeyPurpose::Event)
            .await
            .expect("mint event key");

        let data_key_error = sqlx::query(
            "INSERT INTO data_keys(
                key_ref, scope, purpose, conversation_id, algorithm, wrap_key_id,
                wrap_nonce, wrapped_key, state, created_at, destroyed_at
             ) VALUES(NULL, 'conversation', 'artifact', 'conversation-1', ?, 'wrap',
                X'00', X'00', 'active', 'now', NULL)",
        )
        .bind(WRAP_ALGORITHM)
        .execute(store.pool())
        .await
        .expect_err("data_keys.key_ref NULL must be rejected");
        assert_not_null_violation(&data_key_error);

        let message_error = sqlx::query(
            "INSERT INTO messages(
                id, seq, role, raw_key_ref, raw_ciphertext, payload, search_text,
                redaction_version, interrupted, created_at
             ) VALUES(NULL, 1, 'user', ?, X'00', '{}', '', 1, 0, 'now')",
        )
        .bind(&transcript_key.key_ref)
        .execute(store.pool())
        .await
        .expect_err("messages.id NULL must be rejected");
        assert_not_null_violation(&message_error);

        let head_error = sqlx::query(
            "INSERT INTO event_log_heads(
                conversation_id, last_seq, event_count, chain_digest, key_ref,
                head_hmac, updated_at
             ) VALUES(NULL, 1, 1, zeroblob(32), ?, zeroblob(32), 'now')",
        )
        .bind(&event_key.key_ref)
        .execute(store.pool())
        .await
        .expect_err("event_log_heads.conversation_id NULL must be rejected");
        assert_not_null_violation(&head_error);

        let tool_execution_error = sqlx::query(
            "INSERT INTO tool_executions(
                tool_call_id, command_id, run_id, executor_generation, state,
                idempotency_key, started_at, finished_at, error_code
             ) VALUES(NULL, 'command-null', 'run-null', 0, 'prepared',
                'idempotency-null', NULL, NULL, NULL)",
        )
        .execute(store.pool())
        .await
        .expect_err("tool_executions.tool_call_id NULL must be rejected");
        assert_not_null_violation(&tool_execution_error);

        let approval_error = sqlx::query(
            "INSERT INTO approval_log(
                id, tool_call_id, run_id, turn_id, state, request_projection,
                redaction_version, created_at, decided_at
             ) VALUES(NULL, 'approval-tool-null', 'run-null', 'turn-null',
                'pending', '{}', 1, 'now', NULL)",
        )
        .execute(store.pool())
        .await
        .expect_err("approval_log.id NULL must be rejected");
        assert_not_null_violation(&approval_error);

        store
            .conversation_key(DataKeyPurpose::Artifact)
            .await
            .expect("valid data_keys identity still inserts");
        sqlx::query(
            "INSERT INTO messages(
                id, seq, role, raw_key_ref, raw_ciphertext, payload, search_text,
                redaction_version, interrupted, created_at
             ) VALUES('message-valid', 1, 'user', ?, X'00', '{}', '', 1, 0, 'now')",
        )
        .bind(&transcript_key.key_ref)
        .execute(store.pool())
        .await
        .expect("valid messages identity still inserts");
        sqlx::query(
            "INSERT INTO event_log_heads(
                conversation_id, last_seq, event_count, chain_digest, key_ref,
                head_hmac, updated_at
             ) VALUES('conversation-1', 1, 1, zeroblob(32), ?, zeroblob(32), 'now')",
        )
        .bind(&event_key.key_ref)
        .execute(store.pool())
        .await
        .expect("valid event_log_heads identity still inserts");
        sqlx::query(
            "INSERT INTO tool_executions(
                tool_call_id, command_id, run_id, executor_generation, state,
                idempotency_key, started_at, finished_at, error_code
             ) VALUES('tool-valid', 'command-valid', 'run-valid', 0, 'prepared',
                'idempotency-valid', NULL, NULL, NULL)",
        )
        .execute(store.pool())
        .await
        .expect("valid tool_executions identity still inserts");
        sqlx::query(
            "INSERT INTO approval_log(
                id, tool_call_id, run_id, turn_id, state, request_projection,
                redaction_version, created_at, decided_at
             ) VALUES('approval-valid', 'approval-tool-valid', 'run-valid',
                'turn-valid', 'pending', '{}', 1, 'now', NULL)",
        )
        .execute(store.pool())
        .await
        .expect("valid approval_log identity still inserts");

        let quick_check: String = sqlx::query_scalar("PRAGMA quick_check")
            .fetch_one(store.pool())
            .await
            .expect("quick_check after valid fixtures");
        assert_eq!(quick_check, "ok");
        assert!(
            sqlx::query("PRAGMA foreign_key_check")
                .fetch_optional(store.pool())
                .await
                .expect("foreign_key_check after valid fixtures")
                .is_none()
        );
    }

    #[tokio::test]
    async fn migration_rejects_invalid_data_key_check_fixtures() {
        let store = store().await;
        let mut invalid = vec![
            ("unknown", "transcript", Some("conversation-1")),
            ("conversation", "unknown", Some("conversation-1")),
            ("conversation", "workspace", Some("conversation-1")),
            ("conversation", "transcript", None),
            ("agent", "workspace", Some("conversation-1")),
        ];
        for purpose in [
            "transcript",
            "event",
            "memory_summary",
            "provider_context",
            "command",
            "mutation",
            "artifact",
        ] {
            invalid.push(("agent", purpose, None));
        }
        for (scope, purpose, conversation_id) in invalid {
            let result = sqlx::query(
                "INSERT INTO data_keys(
                    key_ref, scope, purpose, conversation_id, algorithm, wrap_key_id,
                    wrap_nonce, wrapped_key, state, created_at, destroyed_at
                 ) VALUES(?, ?, ?, ?, ?, 'wrap', X'00', X'00', 'active', 'now', NULL)",
            )
            .bind(format!("{scope}-{purpose}-{conversation_id:?}"))
            .bind(scope)
            .bind(purpose)
            .bind(conversation_id)
            .bind(WRAP_ALGORITHM)
            .execute(store.pool())
            .await;
            assert!(result.is_err(), "fixture must violate CHECK constraints");
        }
    }

    #[tokio::test]
    async fn migration_accepts_only_complete_active_and_destroyed_key_states() {
        let store = store().await;
        let invalid_states = [
            ("active", None, Some(vec![0]), None),
            ("active", Some(vec![0]), None, None),
            ("active", Some(vec![0]), Some(vec![0]), Some("destroyed")),
            ("destroyed", Some(vec![0]), Some(vec![0]), Some("destroyed")),
            ("destroyed", None, None, None),
            ("future", Some(vec![0]), Some(vec![0]), None),
        ];
        for (index, (state, wrap_nonce, wrapped_key, destroyed_at)) in
            invalid_states.into_iter().enumerate()
        {
            let result = sqlx::query(
                "INSERT INTO data_keys(
                    key_ref, scope, purpose, conversation_id, algorithm, wrap_key_id,
                    wrap_nonce, wrapped_key, state, created_at, destroyed_at
                 ) VALUES(?, 'conversation', 'artifact', 'conversation-1', ?, 'wrap',
                    ?, ?, ?, 'now', ?)",
            )
            .bind(format!("invalid-state-{index}"))
            .bind(WRAP_ALGORITHM)
            .bind(wrap_nonce)
            .bind(wrapped_key)
            .bind(state)
            .bind(destroyed_at)
            .execute(store.pool())
            .await;
            assert!(
                result.is_err(),
                "partial or unknown key state {state} must be rejected"
            );
        }

        let key = store
            .conversation_key(DataKeyPurpose::Command)
            .await
            .expect("mint command key");
        let duplicate = store
            .conversation_key(DataKeyPurpose::Command)
            .await
            .expect("reuse active command key");
        assert_eq!(key.key_ref, duplicate.key_ref);

        store
            .destroy_conversation_key_ref(&key.key_ref)
            .await
            .expect("destroy command key");
        let destroyed = sqlx::query(
            "SELECT state, wrapped_key, wrap_nonce, destroyed_at FROM data_keys WHERE key_ref = ?",
        )
        .bind(&key.key_ref)
        .fetch_one(store.pool())
        .await
        .expect("read destroyed key");
        assert_eq!(destroyed.get::<String, _>("state"), "destroyed");
        assert!(destroyed.get::<Option<Vec<u8>>, _>("wrapped_key").is_none());
        assert!(destroyed.get::<Option<Vec<u8>>, _>("wrap_nonce").is_none());
        assert!(destroyed.get::<Option<String>, _>("destroyed_at").is_some());
    }

    #[tokio::test]
    async fn key_ref_crypto_erase_is_scoped_transactional_and_idempotent() {
        let store = store().await;
        let transcript = store
            .conversation_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint conversation transcript key");
        let provider_anchor = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: "conversation-1".to_owned(),
                anchor_id: "message-1:7".to_owned(),
            })
            .await
            .expect("mint provider-context anchor key");

        for key_ref in [&transcript.key_ref, &provider_anchor.key_ref] {
            store
                .destroy_conversation_key_ref(key_ref)
                .await
                .expect("destroy conversation key");
            store
                .destroy_conversation_key_ref(key_ref)
                .await
                .expect("repeated destroy is an idempotent no-op");
            let row = sqlx::query(
                "SELECT state, wrapped_key, wrap_nonce, destroyed_at
                 FROM data_keys WHERE key_ref = ?",
            )
            .bind(key_ref)
            .fetch_one(store.pool())
            .await
            .expect("read destroyed key");
            assert_eq!(row.get::<String, _>("state"), "destroyed");
            assert!(row.get::<Option<Vec<u8>>, _>("wrapped_key").is_none());
            assert!(row.get::<Option<Vec<u8>>, _>("wrap_nonce").is_none());
            assert!(row.get::<Option<String>, _>("destroyed_at").is_some());
        }

        sqlx::query(
            "INSERT INTO data_keys(
                key_ref, scope, purpose, conversation_id, algorithm, wrap_key_id,
                wrap_nonce, wrapped_key, state, created_at, destroyed_at
             ) VALUES('workspace-key', 'agent', 'workspace', NULL, ?, 'wrap',
                X'00', X'00', 'active', 'now', NULL)",
        )
        .bind(WRAP_ALGORITHM)
        .execute(store.pool())
        .await
        .expect("insert agent-scoped fixture");
        let error = store
            .destroy_conversation_key_ref("workspace-key")
            .await
            .expect_err("conversation erase must reject an agent key");
        assert!(
            error
                .to_string()
                .contains("outside the active conversation")
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM data_keys WHERE key_ref='workspace-key'",
            )
            .fetch_one(store.pool())
            .await
            .expect("workspace key remains active"),
            "active"
        );
    }

    #[tokio::test]
    async fn provider_context_keys_are_stable_per_anchor_and_independently_erasable() {
        let store = store().await;
        let first_anchor = ProviderContextKeyAnchor {
            conversation_id: "conversation-1".to_owned(),
            anchor_id: "message-1:7".to_owned(),
        };
        let second_anchor = ProviderContextKeyAnchor {
            conversation_id: "conversation-1".to_owned(),
            anchor_id: "message-2:8".to_owned(),
        };
        let first = store
            .provider_context_key(&first_anchor)
            .await
            .expect("mint first anchor key");
        let first_retry = store
            .provider_context_key(&first_anchor)
            .await
            .expect("same anchor retry");
        let second = store
            .provider_context_key(&second_anchor)
            .await
            .expect("mint second anchor key");
        assert_eq!(first.key_ref, first_retry.key_ref);
        assert_ne!(first.key_ref, second.key_ref);

        let second_aad = store.scope().row_aad(
            "provider_context",
            "context-row-2",
            DataKeyPurpose::ProviderContext,
        );
        let second_ciphertext =
            encrypt_content(&second, b"second-anchor", &second_aad).expect("encrypt second anchor");
        store
            .destroy_conversation_key_ref(&first.key_ref)
            .await
            .expect("erase first anchor only");
        assert!(
            store
                .provider_context_key(&first_anchor)
                .await
                .expect_err("an erased anchor identity cannot silently mint a replacement")
                .to_string()
                .contains("crypto-erased")
        );
        let second_retry = store
            .provider_context_key(&second_anchor)
            .await
            .expect("second anchor remains active");
        assert_eq!(
            decrypt_content(&second_retry, &second_ciphertext, &second_aad)
                .expect("decrypt unaffected second anchor"),
            b"second-anchor"
        );
    }

    #[tokio::test]
    async fn provider_context_key_api_rejects_shared_empty_and_cross_conversation_use() {
        let store = store().await;
        assert!(
            store
                .conversation_key(DataKeyPurpose::ProviderContext)
                .await
                .expect_err("purpose-level shared lookup is forbidden")
                .to_string()
                .contains("caller-stable authenticated anchor")
        );
        assert!(
            store
                .provider_context_key(&ProviderContextKeyAnchor {
                    conversation_id: "conversation-1".to_owned(),
                    anchor_id: String::new(),
                })
                .await
                .expect_err("empty anchor")
                .to_string()
                .contains("must not be empty")
        );
        assert!(
            store
                .provider_context_key(&ProviderContextKeyAnchor {
                    conversation_id: "conversation-2".to_owned(),
                    anchor_id: "message-1:7".to_owned(),
                })
                .await
                .expect_err("cross-conversation anchor")
                .to_string()
                .contains("different authenticated conversation")
        );
    }

    #[tokio::test]
    async fn conversation_key_rejects_workspace_purpose() {
        let store = store().await;
        assert!(
            store
                .conversation_key(DataKeyPurpose::Workspace)
                .await
                .expect_err("workspace keys must be agent-scoped")
                .to_string()
                .contains("workspace keys are agent-scoped")
        );
    }

    #[tokio::test]
    async fn file_store_treats_uri_reserved_characters_as_literal_filename_bytes() {
        let root = std::env::temp_dir().join(format!("sumi-store-uri-{}", Uuid::now_v7()));
        let path = root.join("agent ?# %.db");
        std::fs::create_dir_all(&root).expect("create URI filename fixture");

        let store = Store::open(&path, scope(), provider())
            .await
            .expect("open literal SQLite filename");
        assert!(path.is_file());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_scope")
                .fetch_one(store.pool())
                .await
                .expect("read literal-path store"),
            1
        );
        store.pool().close().await;
        std::fs::remove_dir_all(root).expect("remove URI filename fixture");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_store_enforces_private_permissions_on_create_and_reopen() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!("sumi-store-mode-{}", Uuid::now_v7()));
        let state = root.join("state");
        let path = state.join("agent.db");
        std::fs::create_dir_all(&state).expect("create umask-style directory");
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o755))
            .expect("install permissive directory fixture");

        let store = Store::open(&path, scope(), provider())
            .await
            .expect("open file store");
        sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
            .execute(store.pool())
            .await
            .expect("touch WAL state");
        assert_eq!(
            std::fs::metadata(&state)
                .expect("state metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(&path)
                .expect("database metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        for suffix in ["-wal", "-shm"] {
            let sidecar = state.join(format!("agent.db{suffix}"));
            if sidecar.exists() {
                assert_eq!(
                    std::fs::metadata(sidecar)
                        .expect("sidecar metadata")
                        .permissions()
                        .mode()
                        & 0o777,
                    0o600
                );
            }
        }
        store.pool().close().await;

        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o755))
            .expect("loosen directory for reopen fixture");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("loosen database for reopen fixture");
        let reopened = Store::open(&path, scope(), provider())
            .await
            .expect("reopen file store");
        assert_eq!(
            std::fs::metadata(&state)
                .expect("reopened state metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(&path)
                .expect("reopened database metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        for suffix in ["-wal", "-shm"] {
            let sidecar = state.join(format!("agent.db{suffix}"));
            if sidecar.exists() {
                assert_eq!(
                    std::fs::metadata(sidecar)
                        .expect("reopened sidecar metadata")
                        .permissions()
                        .mode()
                        & 0o777,
                    0o600
                );
            }
        }
        reopened.pool().close().await;
        std::fs::remove_dir_all(root).expect("remove mode fixture");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_store_rejects_symlink_database_path() {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!("sumi-store-link-{}", Uuid::now_v7()));
        let state = root.join("state");
        std::fs::create_dir_all(&state).expect("create state directory");
        let target = root.join("target.db");
        std::fs::write(&target, []).expect("create target");
        let path = state.join("agent.db");
        symlink(&target, &path).expect("create symlink fixture");
        let error = match Store::open(&path, scope(), provider()).await {
            Ok(_) => panic!("symlink database path must fail closed"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("regular file"));
        std::fs::remove_dir_all(root).expect("remove symlink fixture");
    }

    #[tokio::test]
    async fn migration_accepts_every_valid_data_key_scope_and_purpose_pair() {
        let store = store().await;
        for purpose in [
            "transcript",
            "event",
            "memory_summary",
            "provider_context",
            "command",
            "mutation",
            "artifact",
        ] {
            sqlx::query(
                "INSERT INTO data_keys(
                    key_ref, scope, purpose, conversation_id, algorithm, wrap_key_id,
                    wrap_nonce, wrapped_key, state, created_at, destroyed_at
                 ) VALUES(?, 'conversation', ?, 'conversation-1', ?, 'wrap',
                    X'00', X'00', 'active', 'now', NULL)",
            )
            .bind(format!("valid-conversation-{purpose}"))
            .bind(purpose)
            .bind(WRAP_ALGORITHM)
            .execute(store.pool())
            .await
            .unwrap_or_else(|error| panic!("valid conversation purpose {purpose}: {error}"));
        }
        sqlx::query(
            "INSERT INTO data_keys(
                key_ref, scope, purpose, conversation_id, algorithm, wrap_key_id,
                wrap_nonce, wrapped_key, state, created_at, destroyed_at
             ) VALUES('valid-agent-workspace', 'agent', 'workspace', NULL, ?, 'wrap',
                X'00', X'00', 'active', 'now', NULL)",
        )
        .bind(WRAP_ALGORITHM)
        .execute(store.pool())
        .await
        .expect("agent workspace is the valid agent-scoped purpose");
    }

    #[tokio::test]
    async fn startup_rejects_authenticated_scope_mismatch() {
        let store = store().await;
        let pool = store.pool.clone();
        drop(store);
        let wrong_scope = AgentScope {
            conversation_id: "conversation-2".to_owned(),
            ..scope()
        };
        let error = match Store::finish_open(pool, wrong_scope, provider()).await {
            Ok(_) => panic!("scope mismatch must fail closed"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("database scope does not match"));
    }

    #[tokio::test]
    async fn startup_rejects_tampered_wrapped_key() {
        let store = store().await;
        store
            .conversation_key(DataKeyPurpose::Event)
            .await
            .expect("mint event key");
        sqlx::query("UPDATE data_keys SET wrapped_key = zeroblob(length(wrapped_key))")
            .execute(store.pool())
            .await
            .expect("tamper fixture");
        let pool = store.pool.clone();
        drop(store);

        let error = match Store::finish_open(pool, scope(), provider()).await {
            Ok(_) => panic!("tampered key must fail startup validation"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("authenticated startup validation")
        );
    }

    #[tokio::test]
    async fn startup_rejects_unknown_active_key_algorithm() {
        let store = store().await;
        store
            .conversation_key(DataKeyPurpose::Event)
            .await
            .expect("mint event key");
        sqlx::query("UPDATE data_keys SET algorithm = 'future/v9'")
            .execute(store.pool())
            .await
            .expect("install unsupported algorithm fixture");
        let pool = store.pool.clone();
        drop(store);

        let error = match Store::finish_open(pool, scope(), provider()).await {
            Ok(_) => panic!("unknown active key algorithm must fail startup"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("unsupported algorithm"));
    }

    #[tokio::test]
    async fn startup_rejects_unsupported_message_redaction_version() {
        let store = store().await;
        let key = store
            .conversation_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint transcript key");
        sqlx::query(
            "INSERT INTO messages(
                id, seq, role, raw_key_ref, raw_ciphertext, payload, search_text,
                redaction_version, interrupted, created_at
             ) VALUES('message-version-2', 1, 'user', ?, X'00', '{}', '', 2, 0, 'now')",
        )
        .bind(&key.key_ref)
        .execute(store.pool())
        .await
        .expect("seed isolated message projection version tamper");
        let pool = store.pool.clone();
        drop(store);

        let error = match Store::finish_open(pool, scope(), provider()).await {
            Ok(_) => panic!("message projection version 2 must fail startup"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("message projection uses an unsupported redaction version")
        );
    }

    #[tokio::test]
    async fn startup_rejects_unsupported_approval_redaction_version() {
        let store = store().await;
        sqlx::query(
            "INSERT INTO approval_log(
                id, tool_call_id, run_id, turn_id, state, request_projection,
                redaction_version, created_at, decided_at
             ) VALUES('approval-version-2', 'tool-version-2', 'run-version-2',
                      'turn-version-2', 'pending', '{}', 2, 'now', NULL)",
        )
        .execute(store.pool())
        .await
        .expect("seed isolated approval projection version tamper");
        let pool = store.pool.clone();
        drop(store);

        let error = match Store::finish_open(pool, scope(), provider()).await {
            Ok(_) => panic!("approval projection version 2 must fail startup"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("approval projection uses an unsupported redaction version")
        );
    }
}
