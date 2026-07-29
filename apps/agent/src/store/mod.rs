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

use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::Arc,
};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::{
    Row, Sqlite, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use tokio::sync::Mutex;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::memory::{
    HydratedMemoryBatch, HydratedMemoryCursor, HydratedMemoryJob, HydratedMemoryMembership,
    HydratedMemoryRuntime, HydratedMemorySummary,
    estimate::{
        EVICTION_ESTIMATOR_VERSION_REPLAY_PROBE_V1, EVICTION_ESTIMATOR_VERSION_SERIALIZED_BYTES,
        EvictionFootprint, ProviderContextItemWithFootprint, TokenCalibration,
        eviction_footprint_for_payload,
    },
};
use crate::provider::model::ModelSpec;
use crate::provider::types::{
    ApiProtocol, ContextMessage, Message, ProviderContextItem, ProviderContextPayload,
    PublicMessage, StopReason, validate_native_suffix_for_hydration,
};
use crate::runtime::contracts::{
    GenerationRecoveryFence, PersonalityAgentId, ProcessGeneration, ProcessGenerationLease,
};

use self::crypto::{
    DataKeyScope, KeyWrapAad, PersonalityAgentCommandDigestFactory, WRAP_ALGORITHM,
    unwrap_data_key, wrap_data_key,
};
#[cfg(test)]
pub(crate) use self::delivery::insert_test_durable_event;
pub(crate) use self::delivery::{
    DeliveryChannelBuilder, DeliveryFrame, DeliveryMode, DeliveryPump, DeliveryTransportError,
    DurableDeliveryOutcome, current_event_head_seq, raw_events_after,
};
#[cfg(test)]
pub(crate) use self::event_writer::seed_provider_context_owner_event_evidence;
#[allow(unused_imports)]
pub(crate) use self::physical_recovery::{
    ApplyReceiptOutcome, HydrationReceiptIdentity, PhysicalRecoveryApplier, PhysicalRecoveryIntent,
    PhysicalRecoveryIntentRequest, PhysicalRecoveryReceipt,
};
#[cfg(test)]
pub(crate) use self::provider_context::{
    EncryptedProviderContextRecord, provider_context_record_id,
};
pub(crate) use self::provider_context::{ProviderContextKind, provider_context_idempotency_key};
#[cfg(test)]
pub(crate) use self::provider_context::{
    ProviderContextMutationApplier, ProviderContextMutationBuilder,
};
pub(crate) use self::transcript::{message_interrupted, public_message_role};
#[cfg(test)]
pub(crate) use crypto::{DATA_KEY_BYTES, WrappingKey};
#[allow(unused_imports)]
pub(crate) use crypto::{
    DataKeyMaterial, DataKeyPurpose, EnvironmentKeyProvider, KeyProvider, RowAad,
    command_payload_digest, decrypt_content, encrypt_content, verify_command_payload_digest,
};
#[allow(
    unused_imports,
    reason = "T12 freezes projection types consumed by T15 without duplicating EventWriter"
)]
pub(crate) use event_writer::{
    ApplicationKind, ApprovalMutation, ApprovalRuleMutation, BootstrapRecoveryGuard, DurableEvent,
    ErrorContextDisposition, EventBatch, EventWrite, EventWriter, InboundAdmission, InboundReceipt,
    InboundReceiptOrigin, InjectedCommand, MemoryApplyCursorAdvance, MemoryBatchMutation,
    MemoryJobMutation, MemoryJobUpdate, MemoryTransition, Projection, RecoveryBatchWriter,
    RecoveryRequired, RunPhase, ToolExecutionMutation, user_message_id,
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
pub(crate) use recovery::{
    HydratedRunState, HydrationOutcome, PendingApprovalRecovery, PendingErrorContextRecovery,
    RecoveryStep, ResumeDirective, SuffixRecovery,
};
pub(crate) use redactor::{PublicProjectionBuilder, Redactor, search_text_from_projection};
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

/// Number of rows fetched per page during cold-boot hydration.  Pages are
/// processed and dropped before the next page is requested, so decrypted
/// plaintext and `SqliteRow` buffers are not retained for the whole history.
const HYDRATION_PAGE_SIZE: i64 = 64;
const HYDRATION_MAX_ROWS: u64 = 100_000;
const HYDRATION_MAX_ENCODED_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
struct HydrationBudget {
    max_rows: u64,
    max_encoded_bytes: u64,
}

impl Default for HydrationBudget {
    fn default() -> Self {
        Self {
            max_rows: HYDRATION_MAX_ROWS,
            max_encoded_bytes: HYDRATION_MAX_ENCODED_BYTES,
        }
    }
}

impl HydrationBudget {
    fn validate(self, rows: i64, encoded_bytes: i64) -> Result<()> {
        let rows = u64::try_from(rows).context("hydration row count is negative")?;
        let encoded_bytes =
            u64::try_from(encoded_bytes).context("hydration encoded-byte count is negative")?;
        if rows > self.max_rows {
            bail!(
                "hydration snapshot has {rows} rows, exceeding the {}-row budget",
                self.max_rows
            );
        }
        if encoded_bytes > self.max_encoded_bytes {
            bail!(
                "hydration snapshot has {encoded_bytes} encoded bytes, exceeding the {}-byte budget",
                self.max_encoded_bytes
            );
        }
        Ok(())
    }
}

/// Canonical plaintext encrypted in `memory_batches` summaries and
/// `memory_jobs` results. Store is the sole decryption/redaction verifier and
/// immediately converts this DTO to a ciphertext-free runtime value.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MemorySummaryPayload {
    summary: String,
    est_tokens: u64,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

fn pending_error_context_recovery(
    messages: &[ContextMessage],
    provider_context: &[ProviderContextItemWithFootprint],
) -> Result<Option<PendingErrorContextRecovery>> {
    let error_messages: BTreeMap<(&str, u64), bool> = messages
        .iter()
        .filter_map(|message| match message {
            ContextMessage::Persisted { id, seq, message } => Some((
                (id.as_str(), *seq),
                matches!(
                    message,
                    Message::Assistant(assistant)
                        if assistant.stop_reason == StopReason::Error
                ),
            )),
            ContextMessage::Synthetic { .. } => None,
        })
        .collect();
    let mut pending = BTreeMap::<(String, u64), usize>::new();
    for item in provider_context {
        let anchor = &item.item.retention_owner;
        if error_messages
            .get(&(anchor.message_id.as_str(), anchor.message_seq))
            .copied()
            == Some(true)
        {
            *pending
                .entry((anchor.message_id.clone(), anchor.message_seq))
                .or_default() += 1;
        }
    }
    if pending.len() > 1 {
        bail!("cold hydration found multiple undisposed Error provider-context retention units");
    }
    let Some(((message_id, message_seq), item_count)) = pending.into_iter().next() else {
        return Ok(None);
    };
    Ok(Some(PendingErrorContextRecovery {
        message_id,
        message_seq,
        item_count: u32::try_from(item_count)
            .context("Error provider-context item count exceeds u32")?,
    }))
}

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

mod sqlite_uuid {
    use std::{
        ffi::c_int,
        panic::{AssertUnwindSafe, catch_unwind},
        slice, str,
    };

    use libsqlite3_sys as ffi;
    use sqlx::{Error, Result, sqlite::SqliteConnection};

    use crate::runtime::contracts::PersonalityAgentId;
    const FUNCTION_NAME: &[u8] = b"sumi_is_canonical_uuid_v7\0";

    pub(super) async fn register(connection: &mut SqliteConnection) -> Result<()> {
        let mut handle = connection.lock_handle().await?;
        let result = unsafe {
            // The locked SQLx handle excludes concurrent SQLite access for the
            // duration of registration. The callback owns no application data
            // and SQLite retains only the static function name and function
            // pointer.
            ffi::sqlite3_create_function_v2(
                handle.as_raw_handle().as_ptr().cast(),
                FUNCTION_NAME.as_ptr().cast(),
                1,
                ffi::SQLITE_UTF8 | ffi::SQLITE_DETERMINISTIC | ffi::SQLITE_INNOCUOUS,
                std::ptr::null_mut(),
                Some(is_canonical_uuid_v7),
                None,
                None,
                None,
            )
        };
        if result != ffi::SQLITE_OK {
            return Err(Error::Protocol(format!(
                "failed to register canonical UUIDv7 SQLite scalar: code {result}"
            )));
        }
        Ok(())
    }

    unsafe extern "C" fn is_canonical_uuid_v7(
        context: *mut ffi::sqlite3_context,
        argument_count: c_int,
        arguments: *mut *mut ffi::sqlite3_value,
    ) {
        let valid = catch_unwind(AssertUnwindSafe(|| {
            if argument_count != 1 || arguments.is_null() {
                return None;
            }
            let value = unsafe { *arguments };
            if value.is_null() || unsafe { ffi::sqlite3_value_type(value) } != ffi::SQLITE_TEXT {
                return Some(false);
            }
            let length = unsafe { ffi::sqlite3_value_bytes(value) };
            if length < 0 {
                return None;
            }
            let text = unsafe { ffi::sqlite3_value_text(value) };
            if text.is_null() {
                return None;
            }
            let bytes = unsafe { slice::from_raw_parts(text, length as usize) };
            Some(
                str::from_utf8(bytes)
                    .ok()
                    .and_then(|raw| raw.parse::<PersonalityAgentId>().ok())
                    .is_some(),
            )
        }));
        match valid {
            Ok(Some(valid)) => unsafe { ffi::sqlite3_result_int(context, c_int::from(valid)) },
            Ok(None) | Err(_) => unsafe {
                ffi::sqlite3_result_error_code(context, ffi::SQLITE_CONSTRAINT_FUNCTION)
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AgentScope {
    pub personality_agent_id: PersonalityAgentId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "T11 freezes the per-anchor key boundary before provider-context persistence is wired"
)]
pub(crate) struct ProviderContextKeyAnchor {
    pub personality_agent_id: PersonalityAgentId,
    pub anchor_id: String,
}

#[allow(
    dead_code,
    reason = "artifact broker wiring consumes the per-artifact key boundary"
)]
pub(crate) struct ArtifactKeyAnchor {
    pub personality_agent_id: PersonalityAgentId,
    pub artifact_handle: String,
}

impl AgentScope {
    pub(crate) const fn new(personality_agent_id: PersonalityAgentId) -> Self {
        Self {
            personality_agent_id,
        }
    }

    pub(crate) const fn personality_agent_id(&self) -> &PersonalityAgentId {
        &self.personality_agent_id
    }

    pub(crate) fn row_aad(
        &self,
        table: impl Into<String>,
        row_id: impl Into<String>,
        purpose: DataKeyPurpose,
    ) -> RowAad {
        RowAad {
            personality_agent_id: self.personality_agent_id.to_string(),
            table: table.into(),
            row_id: row_id.into(),
            purpose: purpose.as_str().to_owned(),
            schema_version: 2,
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
    #[cfg(test)]
    _in_memory_anchor: Option<Arc<Mutex<sqlx::SqliteConnection>>>,
}

pub(in crate::store) struct AuthenticatedProviderContextRow {
    pub(in crate::store) id: String,
    pub(in crate::store) item: ProviderContextItem,
    pub(in crate::store) key_ref: String,
    pub(in crate::store) footprint: EvictionFootprint,
}

impl Store {
    pub(crate) async fn open(
        path: &Path,
        scope: AgentScope,
        key_provider: Arc<dyn KeyProvider>,
    ) -> Result<Self> {
        prepare_state_path(path).await?;
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .after_connect(|connection, _| {
                Box::pin(async move { sqlite_uuid::register(connection).await })
            })
            .connect_with(options)
            .await
            .with_context(|| format!("failed to open SQLite database {}", path.display()))?;
        let store = Self::finish_open(pool, scope, key_provider).await?;
        secure_sqlite_files(path).await?;
        Ok(store)
    }

    #[cfg(test)]
    async fn in_memory(scope: AgentScope, key_provider: Arc<dyn KeyProvider>) -> Result<Self> {
        // SQLx closes a SQLite worker connection when an in-flight query is
        // cancelled. A single-connection anonymous in-memory pool then opens a
        // fresh, empty database, losing the migrated schema. Keep one connection
        // permanently outside the managed pool so cancellation-driven connection
        // replacement observes the same named shared-memory database.
        let database_name = format!("sumi-test-{}", Uuid::now_v7());
        let options = SqliteConnectOptions::new()
            .filename(format!("file:{database_name}?mode=memory&cache=shared"))
            .in_memory(true)
            .shared_cache(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Memory);
        let mut anchor = <sqlx::SqliteConnection as sqlx::Connection>::connect_with(&options)
            .await
            .context("failed to open in-memory database anchor")?;
        sqlite_uuid::register(&mut anchor)
            .await
            .context("failed to register SQLite identity validator on test anchor")?;
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .after_connect(|connection, _| {
                Box::pin(async move { sqlite_uuid::register(connection).await })
            })
            .connect_with(options)
            .await?;
        let mut store = Self::finish_open(pool, scope, key_provider).await?;
        store._in_memory_anchor = Some(Arc::new(Mutex::new(anchor)));
        Ok(store)
    }

    #[cfg(test)]
    pub(crate) async fn session_test_store(personality_agent_id: &str) -> Result<Self> {
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
            AgentScope::new(personality_agent_id.parse()?),
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
            "INSERT INTO agent_scope(singleton, personality_agent_id, created_at)
             VALUES(1, ?, ?)
             ON CONFLICT(singleton) DO NOTHING",
        )
        .bind(scope.personality_agent_id.as_str())
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
            #[cfg(test)]
            _in_memory_anchor: None,
        });
        // Scope and every pre-existing active key must authenticate before
        // initialization is allowed to mint the Mutation key or commit the
        // provider-context projection head. Otherwise a wrong-scope open of an
        // uninitialized database could permanently bind genesis to the wrong
        // runtime identity before startup eventually rejected that identity.
        store.validate_startup().await?;
        provider_context::initialize_provider_context_projection_head(&store).await?;
        // Also cover the newly minted projection key and the rest of startup
        // invariants after initialization.
        store.validate_startup().await?;
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

        let writer = EventWriter::new(Arc::new(self.clone()));
        let mut recovery = writer.begin_bootstrap_recovery(lease, fence).await?;

        // Inspect physical recovery first under the same writer fence used for
        // boot repairs. No EventWriter or pool call is made while this
        // transaction remains open, which preserves max_connections(1).
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to begin hydration snapshot transaction")?;

        event_writer::authenticate_event_log_snapshot(self, &mut transaction).await?;
        provider_context::verify_provider_context_projection_set(self, &mut transaction).await?;
        let intents = self.hydrate_running_intents(&mut transaction).await?;
        if !intents.is_empty() {
            transaction
                .rollback()
                .await
                .context("failed to roll back hydration transaction")?;
            return Ok(HydrationOutcome::PhysicalRecoveryRequired(intents));
        }
        self.preflight_hydration_budget(&mut transaction, HydrationBudget::default())
            .await?;
        transaction
            .commit()
            .await
            .context("failed to commit initial hydration inspection")?;

        // Prepared provider context and recoverable memory state are repaired
        // through the already-held EventWriter gate. Each operation owns its
        // own short transaction; none tries to reacquire the gate.
        recovery
            .recover_provider_context_mutations()
            .await
            .context("failed to recover prepared provider-context mutations")?;
        crate::memory::compactor::recover_boot_memory_jobs(&mut recovery)
            .await
            .context("failed to recover durable memory maintenance state")?;

        // Re-authenticate and materialize a fresh post-repair snapshot.
        let mut transaction = self
            .pool
            .begin()
            .await
            .context("failed to begin post-recovery hydration snapshot")?;
        event_writer::authenticate_event_log_snapshot(self, &mut transaction).await?;
        provider_context::verify_provider_context_projection_set(self, &mut transaction).await?;
        let post_recovery_intents = self.hydrate_running_intents(&mut transaction).await?;
        if !post_recovery_intents.is_empty() {
            transaction.rollback().await?;
            return Ok(HydrationOutcome::PhysicalRecoveryRequired(
                post_recovery_intents,
            ));
        }
        self.preflight_hydration_budget(&mut transaction, HydrationBudget::default())
            .await?;
        let messages = self
            .hydrate_messages(&mut transaction)
            .await
            .context("failed to hydrate authenticated transcript rows")?;
        let provider_context = self
            .hydrate_provider_context(&messages, &mut transaction)
            .await?;
        let pending_error_context = pending_error_context_recovery(&messages, &provider_context)?;
        let memory = self
            .hydrate_memory_runtime(&messages, &provider_context, &mut transaction)
            .await?;
        crate::memory::ThreeLayerMemory::from_hydrated(memory.clone())
            .context("hydrated memory graph is structurally invalid")?;

        transaction
            .commit()
            .await
            .context("failed to commit hydration snapshot transaction")?;
        let mut recovery_steps = SuffixRecovery::plan_full_suffix(self).await?;
        if let Some(pending_error_context) = pending_error_context {
            let mut matching_steps = recovery_steps.iter_mut().filter(|step| {
                matches!(step, RecoveryStep::ResumeAssistantFromDurableEvents { .. })
            });
            let step = matching_steps.next().ok_or_else(|| {
                anyhow!("undisposed Error provider context has no assistant logical-recovery owner")
            })?;
            if matching_steps.next().is_some() {
                bail!(
                    "undisposed Error provider context has multiple assistant logical-recovery owners"
                );
            }
            let RecoveryStep::ResumeAssistantFromDurableEvents {
                pending_error_context: slot,
                ..
            } = step
            else {
                unreachable!("matching recovery step variant was checked")
            };
            *slot = Some(pending_error_context);
        }

        let receipt = HydrationReceiptIdentity {
            lease_id: lease.lease_id().to_owned(),
            generation: lease.generation(),
            fence_id: fence.fence_id().to_owned(),
            intent_count: 0,
        };

        if !recovery_steps.is_empty() {
            return Ok(HydrationOutcome::LogicalRecoveryRequired {
                steps: recovery_steps,
            });
        }

        Ok(HydrationOutcome::Complete(HydratedRunState {
            scope: self.scope.clone(),
            lease: lease.clone(),
            fence: fence.clone(),
            receipt,
            messages,
            provider_context,
            memory,
            resume: ResumeDirective::AdmitCommands,
        }))
    }

    async fn hydrate_running_intents(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<Vec<PhysicalRecoveryIntentRequest>> {
        let mut intents = Vec::new();
        let mut offset = 0_i64;
        loop {
            let rows = sqlx::query(
                "SELECT tool_call_id, command_id, run_id, executor_generation
                 FROM tool_executions WHERE state = 'running' ORDER BY tool_call_id
                 LIMIT ? OFFSET ?",
            )
            .bind(HYDRATION_PAGE_SIZE)
            .bind(offset)
            .fetch_all(&mut **transaction)
            .await
            .context("failed to hydrate running tool executions")?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                let tool_call_id: String = row.try_get("tool_call_id")?;
                let command_id: String = row.try_get("command_id")?;
                let run_id: String = row.try_get("run_id")?;
                if tool_call_id.is_empty() || command_id.is_empty() || run_id.is_empty() {
                    bail!("running tool execution identity must not be empty");
                }
                let generation = ProcessGeneration::from_sqlite(
                    row.try_get("executor_generation")?,
                )
                .map_err(|error| anyhow!("invalid persisted executor generation: {error}"))?;
                event_writer::authenticate_running_tool_intent(
                    self,
                    transaction,
                    &tool_call_id,
                    &command_id,
                    &run_id,
                    generation,
                )
                .await?;
                intents.push(PhysicalRecoveryIntentRequest {
                    tool_call_id,
                    command_id,
                    run_id,
                    executor_generation: generation,
                });
            }
            offset += HYDRATION_PAGE_SIZE;
        }
        Ok(intents)
    }

    async fn preflight_hydration_budget(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        budget: HydrationBudget,
    ) -> Result<()> {
        // Bound the total rows and encoded bytes that the typed hydration
        // snapshot can materialize before decrypting any transcript, provider,
        // or memory payload. Fixed-width integers are represented by a small
        // per-row allowance; all variable-width columns are counted as their
        // UTF-8/BLOB byte lengths.
        let row = sqlx::query(
            "SELECT
                COALESCE(SUM(row_count), 0) AS row_count,
                COALESCE(SUM(encoded_bytes), 0) AS encoded_bytes
             FROM (
                SELECT COUNT(*) AS row_count,
                       COALESCE(SUM(
                         64 +
                         length(CAST(id AS BLOB)) +
                         length(CAST(role AS BLOB)) +
                         length(CAST(raw_key_ref AS BLOB)) +
                         length(raw_ciphertext) +
                         length(CAST(payload AS BLOB)) +
                         length(CAST(search_text AS BLOB))
                       ), 0) AS encoded_bytes
                FROM messages
                UNION ALL
                SELECT COUNT(*),
                       COALESCE(SUM(
                         96 +
                         length(CAST(id AS BLOB)) +
                         COALESCE(length(CAST(message_id AS BLOB)), 0) +
                         length(CAST(idempotency_key AS BLOB)) +
                         length(CAST(kind AS BLOB)) +
                         COALESCE(length(CAST(context_fingerprint AS BLOB)), 0) +
                         length(CAST(provider_instance_id AS BLOB)) +
                         length(CAST(protocol AS BLOB)) +
                         length(CAST(model AS BLOB)) +
                         length(CAST(key_ref AS BLOB)) +
                         length(ciphertext)
                       ), 0)
                FROM provider_context
                UNION ALL
                SELECT COUNT(*),
                       COALESCE(SUM(
                         64 +
                         length(CAST(mutation_id AS BLOB)) +
                         length(CAST(state AS BLOB)) +
                         length(CAST(intent_key_ref AS BLOB)) +
                         length(intent_ciphertext) +
                         length(CAST(hmac_key_id AS BLOB)) +
                         length(intent_hmac) +
                         length(CAST(prepared_at AS BLOB)) +
                         COALESCE(length(CAST(finished_at AS BLOB)), 0) +
                         COALESCE(length(CAST(terminal_reason AS BLOB)), 0)
                       ), 0)
                FROM provider_context_mutations
                UNION ALL
                SELECT COUNT(*),
                       COALESCE(SUM(
                         32 +
                         length(CAST(scope_key AS BLOB)) +
                         length(CAST(latest_insert_id AS BLOB)) +
                         length(CAST(updated_at AS BLOB))
                       ), 0)
                FROM provider_context_replace_heads
                UNION ALL
                SELECT COUNT(*),
                       COALESCE(SUM(
                         48 +
                         length(CAST(state AS BLOB)) +
                         COALESCE(length(set_digest), 0) +
                         COALESCE(length(CAST(key_ref AS BLOB)), 0) +
                         COALESCE(length(head_hmac), 0)
                       ), 0)
                FROM provider_context_projection_head
                UNION ALL
                SELECT COUNT(*),
                       COALESCE(SUM(
                         96 +
                         length(CAST(id AS BLOB)) +
                         length(CAST(state AS BLOB)) +
                         COALESCE(length(CAST(summary_key_ref AS BLOB)), 0) +
                         COALESCE(length(summary_ciphertext), 0) +
                         COALESCE(length(CAST(summary_projection AS BLOB)), 0)
                       ), 0)
                FROM memory_batches
                UNION ALL
                SELECT COUNT(*),
                       COALESCE(SUM(
                         16 +
                         length(CAST(batch_id AS BLOB)) +
                         length(CAST(message_id AS BLOB))
                       ), 0)
                FROM memory_batch_messages
                UNION ALL
                SELECT COUNT(*),
                       COALESCE(SUM(
                         96 +
                         length(CAST(id AS BLOB)) +
                         length(CAST(kind AS BLOB)) +
                         length(CAST(source_ids AS BLOB)) +
                         length(CAST(source_versions AS BLOB)) +
                         length(CAST(status AS BLOB)) +
                         COALESCE(length(CAST(lease_until AS BLOB)), 0) +
                         COALESCE(length(CAST(result_key_ref AS BLOB)), 0) +
                         COALESCE(length(result_ciphertext), 0) +
                         COALESCE(length(CAST(result_projection AS BLOB)), 0)
                       ), 0)
                FROM memory_jobs
                UNION ALL
                SELECT COUNT(*),
                       COALESCE(SUM(16 + length(CAST(kind AS BLOB))), 0)
                FROM memory_apply_cursors
                UNION ALL
                SELECT COUNT(*),
                       COALESCE(SUM(40 + length(ratio_bits)), 0)
                FROM memory_calibration
             )",
        )
        .fetch_one(&mut **transaction)
        .await
        .context("failed to preflight hydration snapshot bounds")?;
        budget.validate(row.try_get("row_count")?, row.try_get("encoded_bytes")?)
    }

    async fn hydrate_messages(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<Vec<ContextMessage>> {
        let mut key_cache: HashMap<String, Arc<DataKeyMaterial>> = HashMap::new();

        let mut messages = Vec::new();
        let mut offset = 0_i64;
        loop {
            let rows = sqlx::query(
                "SELECT id, seq, role, raw_key_ref, raw_ciphertext, payload, search_text,
                        redaction_version, interrupted
                 FROM messages ORDER BY seq LIMIT ? OFFSET ?",
            )
            .bind(HYDRATION_PAGE_SIZE)
            .bind(offset)
            .fetch_all(&mut **transaction)
            .await
            .context("failed to hydrate transcript messages")?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                let id: String = row.try_get("id")?;
                let seq: i64 = row.try_get("seq")?;
                let seq = u64::try_from(seq)
                    .with_context(|| format!("message {id} seq out of u64 range"))?;
                let stored_role: String = row.try_get("role")?;
                let key_ref: String = row.try_get("raw_key_ref")?;
                let redaction_version: i64 = row.try_get("redaction_version")?;
                if redaction_version != i64::from(self.redactor.version()) {
                    bail!("message {id} uses an unsupported redaction version");
                }
                let interrupted: i64 = row.try_get("interrupted")?;
                let interrupted = interrupted != 0;
                let stored_payload: String = row.try_get("payload")?;
                let stored_search_text: String = row.try_get("search_text")?;

                let key = self
                    .load_hydration_key(
                        &mut key_cache,
                        transaction,
                        &key_ref,
                        DataKeyPurpose::Transcript,
                    )
                    .await?;
                let ciphertext: Vec<u8> = row.try_get("raw_ciphertext")?;
                let aad = self
                    .scope
                    .row_aad("messages", &id, DataKeyPurpose::Transcript);
                let plaintext = Zeroizing::new(
                    decrypt_content(&key, &ciphertext, &aad)
                        .with_context(|| format!("failed to decrypt transcript message {id}"))?,
                );
                let public: PublicMessage =
                    serde_json::from_slice(&plaintext).with_context(|| {
                        format!("transcript message {id} is not a valid PublicMessage")
                    })?;

                if public_message_role(&public) != stored_role {
                    bail!("message {id} role does not match decrypted public message");
                }
                if message_interrupted(&public) != interrupted {
                    bail!("message {id} interrupted flag does not match decrypted public message");
                }

                let derived_payload = self
                    .redactor
                    .redact_serialized(&plaintext)
                    .with_context(|| format!("failed to re-derive payload for message {id}"))?;
                if derived_payload != stored_payload {
                    bail!(
                        "message {id} stored payload does not match re-derived redacted projection"
                    );
                }

                let derived_search_text = search_text_from_projection(&derived_payload)
                    .with_context(|| format!("failed to re-derive search text for message {id}"))?;
                if derived_search_text != stored_search_text {
                    bail!("message {id} stored search_text does not match re-derived search text");
                }

                messages.push(ContextMessage::Persisted {
                    id: id.clone(),
                    seq,
                    message: Message::from(public),
                });
            }
            offset += HYDRATION_PAGE_SIZE;
        }
        Ok(messages)
    }

    pub(crate) async fn hydrate_provider_context(
        &self,
        messages: &[ContextMessage],
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<Vec<ProviderContextItemWithFootprint>> {
        Ok(self
            .hydrate_authenticated_provider_context(messages, transaction)
            .await?
            .into_iter()
            .map(|row| ProviderContextItemWithFootprint::new(row.item, row.footprint))
            .collect())
    }

    pub(in crate::store) async fn hydrate_authenticated_provider_context(
        &self,
        messages: &[ContextMessage],
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<Vec<AuthenticatedProviderContextRow>> {
        let mut key_cache: HashMap<String, Arc<DataKeyMaterial>> = HashMap::new();

        let mut provider_context = Vec::new();
        let mut offset = 0_i64;

        // Persisted messages indexed by seq for anchor and provider-origin lookups.
        let seq_to_message: BTreeMap<u64, &ContextMessage> = messages
            .iter()
            .filter_map(|message| match message {
                ContextMessage::Persisted { seq, .. } => Some((*seq, message)),
                ContextMessage::Synthetic { .. } => None,
            })
            .collect();

        loop {
            let rows = sqlx::query(
                "SELECT id, message_id, message_seq, wire_item_index, item_ordinal,
                    idempotency_key, kind, coverage_through_seq, context_fingerprint,
                    provider_instance_id, protocol, model, key_ref, ciphertext,
                    eviction_tokens, eviction_estimator_version, created_at
             FROM provider_context
             ORDER BY COALESCE(message_seq, coverage_through_seq),
                      wire_item_index NULLS FIRST,
                      item_ordinal,
                      id
             LIMIT ? OFFSET ?",
            )
            .bind(HYDRATION_PAGE_SIZE)
            .bind(offset)
            .fetch_all(&mut **transaction)
            .await
            .context("failed to hydrate provider context")?;
            if rows.is_empty() {
                break;
            }

            for row in rows {
                let id: String = row.try_get("id")?;
                let stored_message_id: Option<String> = row.try_get("message_id")?;
                let stored_message_seq: Option<i64> = row.try_get("message_seq")?;
                let stored_wire_item_index: Option<i64> = row.try_get("wire_item_index")?;
                let stored_item_ordinal: i64 = row.try_get("item_ordinal")?;
                let stored_coverage_seq: Option<i64> = row.try_get("coverage_through_seq")?;
                let stored_idempotency_key: String = row.try_get("idempotency_key")?;
                let stored_kind: String = row.try_get("kind")?;
                let stored_fingerprint: Option<String> = row.try_get("context_fingerprint")?;
                let stored_provider_instance_id: String = row.try_get("provider_instance_id")?;
                let stored_protocol: String = row.try_get("protocol")?;
                let stored_model: String = row.try_get("model")?;
                let key_ref: String = row.try_get("key_ref")?;
                let stored_eviction_tokens: i64 = row.try_get("eviction_tokens")?;
                let stored_eviction_estimator_version: i64 =
                    row.try_get("eviction_estimator_version")?;
                let stored_created_at: String = row.try_get("created_at")?;
                self::provider_context::validate_canonical_created_at(&stored_created_at)
                    .with_context(|| {
                        format!("provider-context record {id} has invalid created_at metadata")
                    })?;

                let key = self
                    .load_hydration_key(
                        &mut key_cache,
                        transaction,
                        &key_ref,
                        DataKeyPurpose::ProviderContext,
                    )
                    .await?;
                let ciphertext: Vec<u8> = row.try_get("ciphertext")?;
                let aad =
                    self.scope
                        .row_aad("provider_context", &id, DataKeyPurpose::ProviderContext);
                let plaintext =
                    Zeroizing::new(decrypt_content(&key, &ciphertext, &aad).with_context(
                        || format!("failed to decrypt provider-context record {id}"),
                    )?);
                let item: ProviderContextItem =
                    serde_json::from_slice(&plaintext).with_context(|| {
                        format!("provider-context record {id} is not a valid ProviderContextItem")
                    })?;
                if item.retention_owner.message_id.is_empty() {
                    bail!("provider-context record {id} has an empty retention owner");
                }
                let expected_record_id = self::provider_context::provider_context_record_id(&item);
                if id != expected_record_id {
                    bail!(
                        "provider-context record {id} row id does not match authenticated retention owner"
                    );
                }
                let expected_key_ref = self::provider_context::provider_context_owner_key_ref(
                    &self.scope,
                    &item.retention_owner,
                );
                if key_ref != expected_key_ref {
                    bail!(
                        "provider-context record {id} key_ref does not match authenticated retention owner"
                    );
                }
                let owner_message = seq_to_message
                    .get(&item.retention_owner.message_seq)
                    .ok_or_else(|| {
                        anyhow!(
                            "provider-context record {id} retention owner {}:{} does not resolve to a persisted message",
                            item.retention_owner.message_id,
                            item.retention_owner.message_seq
                        )
                    })?;
                let (owner_id, owner_assistant) = match owner_message {
                    ContextMessage::Persisted {
                        id,
                        message: Message::Assistant(assistant),
                        ..
                    } => (id, assistant),
                    _ => {
                        bail!(
                            "provider-context record {id} retention owner {}:{} does not resolve to a persisted assistant MessageEnd",
                            item.retention_owner.message_id,
                            item.retention_owner.message_seq
                        );
                    }
                };
                if owner_id != &item.retention_owner.message_id {
                    bail!(
                        "provider-context record {id} retention owner {}:{} resolves to a different message id",
                        item.retention_owner.message_id,
                        item.retention_owner.message_seq
                    );
                }
                if owner_assistant.origin != item.provider_origin {
                    bail!(
                        "provider-context record {id} provider_origin does not match its retention-owner assistant origin"
                    );
                }

                if item.origin_message.as_ref().map(|a| a.message_id.as_str())
                    != stored_message_id.as_deref()
                {
                    bail!(
                        "provider-context record {id} message_id does not match decrypted anchor"
                    );
                }
                let stored_message_seq_u64 = stored_message_seq
                    .map(|v| {
                        u64::try_from(v).with_context(|| {
                            format!("provider-context record {id} message_seq out of u64 range")
                        })
                    })
                    .transpose()?;
                if item.origin_message.as_ref().map(|a| a.message_seq) != stored_message_seq_u64 {
                    bail!(
                        "provider-context record {id} message_seq does not match decrypted anchor"
                    );
                }
                let stored_wire_u32 = stored_wire_item_index
                    .map(|v| {
                        u32::try_from(v).with_context(|| {
                            format!("provider-context record {id} wire_item_index out of u32 range")
                        })
                    })
                    .transpose()?;
                if item.wire_item_index != stored_wire_u32 {
                    bail!(
                        "provider-context record {id} wire_item_index does not match decrypted item"
                    );
                }
                let stored_ordinal_u32 = u32::try_from(stored_item_ordinal).with_context(|| {
                    format!("provider-context record {id} item_ordinal out of u32 range")
                })?;
                if item.ordinal != stored_ordinal_u32 {
                    bail!(
                        "provider-context record {id} item_ordinal does not match decrypted item"
                    );
                }
                if ProviderContextKind::from_payload(&item.payload).as_str() != stored_kind {
                    bail!("provider-context record {id} kind does not match decrypted payload");
                }

                if stored_provider_instance_id.is_empty()
                    || stored_protocol.is_empty()
                    || stored_model.is_empty()
                {
                    bail!("provider-context record {id} has an empty provider origin field");
                }

                if stored_provider_instance_id != item.provider_origin.provider_instance_id
                    || stored_protocol != item.provider_origin.protocol.as_str()
                    || stored_model != item.provider_origin.model
                {
                    bail!(
                        "provider-context record {id} stored provider origin does not match authenticated plaintext origin"
                    );
                }

                let expected_protocol = match &item.payload {
                    ProviderContextPayload::OpenAiCompactedWindow { .. } => {
                        ApiProtocol::OpenAiResponses.as_str()
                    }
                    ProviderContextPayload::AnthropicCompaction { .. } => {
                        ApiProtocol::AnthropicMessages.as_str()
                    }
                    ProviderContextPayload::EncryptedReasoning { protocol, .. } => {
                        protocol.as_str()
                    }
                };
                if stored_protocol != expected_protocol {
                    bail!("provider-context record {id} protocol does not match decrypted payload");
                }

                match &item.payload {
                    ProviderContextPayload::OpenAiCompactedWindow { .. }
                    | ProviderContextPayload::AnthropicCompaction { .. } => {
                        if item.origin_message.is_some() {
                            bail!(
                                "provider-context record {id} native compaction must not have an origin message"
                            );
                        }
                        if item.wire_item_index.is_some() {
                            bail!(
                                "provider-context record {id} native compaction must not have a wire_item_index"
                            );
                        }
                    }
                    ProviderContextPayload::EncryptedReasoning { .. } => {
                        if item.origin_message.as_ref() != Some(&item.retention_owner) {
                            bail!(
                                "provider-context record {id} encrypted reasoning origin message must match its retention owner"
                            );
                        }
                        if item.wire_item_index.is_none() {
                            bail!(
                                "provider-context record {id} encrypted reasoning must have a wire_item_index"
                            );
                        }
                    }
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
                        if stored_fingerprint.as_deref()
                            != Some(coverage.context_fingerprint.as_str())
                        {
                            bail!(
                                "provider-context record {id} context_fingerprint does not match decrypted payload"
                            );
                        }

                        let expected_idempotency_key = provider_context_idempotency_key(&item);
                        if stored_idempotency_key != expected_idempotency_key {
                            bail!(
                                "provider-context record {id} idempotency key does not match authenticated native item"
                            );
                        }

                        validate_native_suffix_for_hydration(messages, coverage.through_message_seq)
                        .map_err(|message| {
                            anyhow!("provider-context record {id} failed native suffix validation: {message}")
                        })?;
                    }
                    ProviderContextPayload::EncryptedReasoning { .. } => {
                        if stored_coverage_seq.is_some() || stored_fingerprint.is_some() {
                            bail!(
                                "provider-context record {id} reasoning payload must not carry coverage metadata"
                            );
                        }
                        let expected_idempotency_key =
                            self::provider_context::provider_context_idempotency_key(&item);
                        if stored_idempotency_key != expected_idempotency_key {
                            bail!(
                                "provider-context record {id} idempotency key does not match decrypted reasoning item"
                            );
                        }
                    }
                }

                let stored_eviction_version = u32::try_from(stored_eviction_estimator_version)
                    .with_context(|| {
                        format!(
                            "provider-context record {id} eviction_estimator_version out of u32 range"
                        )
                    })?;
                let stored_eviction_tokens_u64 = u64::try_from(stored_eviction_tokens)
                    .with_context(|| {
                        format!("provider-context record {id} eviction_tokens out of u64 range")
                    })?;

                // Old T17 estimator v1 and ReplayProbeV1 must not share version 1 with
                // different formulas. Version 1 is the legacy serialized-bytes estimator
                // and its saved token value is authoritative; version 2 is the
                // provider-owned ReplayProbeV1 and is recomputed on hydration so
                // tampering is detected.
                let footprint = if stored_eviction_version
                    == EVICTION_ESTIMATOR_VERSION_SERIALIZED_BYTES
                {
                    EvictionFootprint::from_saved(
                        EVICTION_ESTIMATOR_VERSION_SERIALIZED_BYTES,
                        0,
                        stored_eviction_tokens_u64,
                    )
                    .with_context(|| {
                        format!(
                            "provider-context record {id} has an invalid saved eviction footprint"
                        )
                    })?
                } else if stored_eviction_version == EVICTION_ESTIMATOR_VERSION_REPLAY_PROBE_V1 {
                    let spec = ModelSpec::from_origin(&item.provider_origin).ok_or_else(|| {
                        anyhow!(
                            "provider-context record {id} has no known model spec for its origin"
                        )
                    })?;
                    let expected_eviction = eviction_footprint_for_payload(&spec, &item.payload)?;
                    if stored_eviction_tokens_u64 != expected_eviction.eviction_tokens() {
                        bail!(
                            "provider-context record {id} eviction_tokens do not match the decrypted payload"
                        );
                    }
                    expected_eviction
                } else {
                    bail!(
                        "provider-context record {id} uses unsupported eviction estimator version {stored_eviction_version}"
                    );
                };

                provider_context.push(AuthenticatedProviderContextRow {
                    id,
                    item,
                    key_ref: expected_key_ref,
                    footprint,
                });
            }
            offset += HYDRATION_PAGE_SIZE;
        }
        crate::provider::types::validate_provider_context_ordinal_refs(
            provider_context.iter().map(|row| &row.item),
        )
        .map_err(|message| anyhow!("hydrated provider-context ordering is invalid: {message}"))?;
        Ok(provider_context)
    }

    /// Project memory rows into typed, ciphertext-free runtime input under the
    /// same authenticated snapshot as transcript and provider context.
    async fn hydrate_memory_runtime(
        &self,
        messages: &[ContextMessage],
        provider_context: &[ProviderContextItemWithFootprint],
        transaction: &mut Transaction<'_, Sqlite>,
    ) -> Result<HydratedMemoryRuntime> {
        let mut persisted_messages = HashMap::with_capacity(messages.len());
        for message in messages {
            let ContextMessage::Persisted { id, .. } = message else {
                bail!("hydrated transcript contains a synthetic message");
            };
            if persisted_messages.insert(id.as_str(), message).is_some() {
                bail!("hydrated transcript contains duplicate message id {id}");
            }
        }

        let calibration = match sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT ratio_bits FROM memory_calibration WHERE singleton = 1",
        )
        .fetch_optional(&mut **transaction)
        .await
        .context("failed to hydrate token calibration")?
        {
            Some(bits) => {
                let bits: [u8; 8] = bits
                    .try_into()
                    .map_err(|_| anyhow!("persisted calibration ratio_bits is not 8 bytes"))?;
                TokenCalibration::new(f64::from_bits(u64::from_be_bytes(bits)))
                    .with_context(|| "persisted calibration ratio is not positive and finite")?
            }
            None => TokenCalibration::default(),
        };

        // `hydrate_provider_context` has authenticated the payload, its exact
        // transcript anchor, and the canonical provider footprint. Aggregate
        // those verified values so runtime reconstruction can prove every
        // live L0 batch total without reading redacted projections.
        let mut anchored_footprints = HashMap::new();
        for context in provider_context {
            let Some(anchor) = context.item.origin_message.as_ref() else {
                continue;
            };
            let owner = persisted_messages
                .get(anchor.message_id.as_str())
                .copied()
                .ok_or_else(|| {
                    anyhow!(
                        "provider-context footprint references unknown transcript message {}",
                        anchor.message_id
                    )
                })?;
            // Error assistants are durable lifetime owners for retained native
            // context, but are deliberately excluded from L0. Keep their
            // provider rows available for disposition without projecting a
            // footprint onto a membership that must not exist.
            if matches!(
                owner,
                ContextMessage::Persisted {
                    message: Message::Assistant(assistant),
                    ..
                } if assistant.stop_reason == StopReason::Error
            ) {
                continue;
            }
            let total = anchored_footprints
                .entry(anchor.message_id.clone())
                .or_insert(0u64);
            *total = total
                .checked_add(context.footprint.eviction_tokens())
                .ok_or_else(|| {
                    anyhow!(
                        "provider-context footprint overflow for transcript message {}",
                        anchor.message_id
                    )
                })?;
        }

        let mut key_cache: HashMap<String, Arc<DataKeyMaterial>> = HashMap::new();
        let mut batches = Vec::new();
        let mut offset = 0_i64;
        loop {
            let rows = sqlx::query(
                "SELECT id, layer, ord, batch_seq, version, state, est_tokens,
                    eviction_footprint_tokens, summary_key_ref, summary_ciphertext,
                    summary_projection, summary_redaction_version
                 FROM memory_batches ORDER BY layer, ord LIMIT ? OFFSET ?",
            )
            .bind(HYDRATION_PAGE_SIZE)
            .bind(offset)
            .fetch_all(&mut **transaction)
            .await
            .context("failed to hydrate memory batches")?;
            if rows.is_empty() {
                break;
            }

            for row in rows {
                let id_text: String = row.try_get("id")?;
                let id = Uuid::parse_str(&id_text)
                    .with_context(|| format!("memory batch id {id_text} is not a UUID"))?;
                let layer_value: i64 = row.try_get("layer")?;
                let layer = MemoryLayer::from_i64(layer_value).ok_or_else(|| {
                    anyhow!("memory batch {id_text} has unknown layer {layer_value}")
                })?;
                let state_text: String = row.try_get("state")?;
                let state = MemoryBatchState::from_str(&state_text).ok_or_else(|| {
                    anyhow!("memory batch {id_text} has unknown state {state_text}")
                })?;
                let summary = match (
                    row.try_get::<Option<String>, _>("summary_key_ref")?,
                    row.try_get::<Option<Vec<u8>>, _>("summary_ciphertext")?,
                    row.try_get::<Option<String>, _>("summary_projection")?,
                    row.try_get::<Option<i64>, _>("summary_redaction_version")?,
                ) {
                    (Some(key_ref), Some(ciphertext), Some(projection), Some(version)) => Some(
                        self.hydrate_memory_summary(
                            &mut key_cache,
                            transaction,
                            &key_ref,
                            &ciphertext,
                            &projection,
                            version,
                            "memory_batches",
                            &id_text,
                        )
                        .await?,
                    ),
                    (None, None, None, None) => None,
                    _ => bail!("memory batch {id_text} summary fields are inconsistent"),
                };

                let ord = u64::try_from(row.try_get::<i64, _>("ord")?)
                    .with_context(|| format!("memory batch {id_text} ord out of u64 range"))?;
                let batch_seq =
                    u64::try_from(row.try_get::<i64, _>("batch_seq")?).with_context(|| {
                        format!("memory batch {id_text} batch_seq out of u64 range")
                    })?;
                let version = u64::try_from(row.try_get::<i64, _>("version")?)
                    .with_context(|| format!("memory batch {id_text} version out of u64 range"))?;
                let est_tokens =
                    u64::try_from(row.try_get::<i64, _>("est_tokens")?).with_context(|| {
                        format!("memory batch {id_text} est_tokens out of u64 range")
                    })?;
                let eviction_footprint_tokens =
                    u64::try_from(row.try_get::<i64, _>("eviction_footprint_tokens")?)
                        .with_context(|| {
                            format!(
                                "memory batch {id_text} eviction_footprint_tokens out of u64 range"
                            )
                        })?;

                batches.push(HydratedMemoryBatch::new(
                    id,
                    layer,
                    ord,
                    batch_seq,
                    version,
                    state,
                    est_tokens,
                    eviction_footprint_tokens,
                    summary,
                ));
            }
            offset += HYDRATION_PAGE_SIZE;
        }

        let mut memberships = Vec::new();
        offset = 0;
        loop {
            let rows = sqlx::query(
                "SELECT batch_id, message_id, ord
                 FROM memory_batch_messages
                 ORDER BY batch_id, ord LIMIT ? OFFSET ?",
            )
            .bind(HYDRATION_PAGE_SIZE)
            .bind(offset)
            .fetch_all(&mut **transaction)
            .await
            .context("failed to hydrate memory batch messages")?;
            if rows.is_empty() {
                break;
            }

            for row in rows {
                let batch_id_text: String = row.try_get("batch_id")?;
                let batch_id = Uuid::parse_str(&batch_id_text).with_context(|| {
                    format!("memory membership batch id {batch_id_text} is not a UUID")
                })?;
                let message_id: String = row.try_get("message_id")?;
                let message = persisted_messages
                    .get(message_id.as_str())
                    .copied()
                    .cloned()
                    .ok_or_else(|| {
                        anyhow!(
                            "memory membership for batch {batch_id} references unknown persisted message {message_id}"
                        )
                    })?;
                let ord = u64::try_from(row.try_get::<i64, _>("ord")?).with_context(|| {
                    format!(
                        "memory membership for batch {batch_id} message {message_id} ord out of u64 range"
                    )
                })?;
                memberships.push(HydratedMemoryMembership::new(batch_id, ord, message));
            }
            offset += HYDRATION_PAGE_SIZE;
        }

        let mut jobs = Vec::new();
        offset = 0;
        loop {
            let rows = sqlx::query(
                "SELECT id, kind, batch_seq, source_ids, source_versions, status,
                    lease_until, attempts, result_key_ref, result_ciphertext,
                    result_projection, result_redaction_version
                 FROM memory_jobs ORDER BY id LIMIT ? OFFSET ?",
            )
            .bind(HYDRATION_PAGE_SIZE)
            .bind(offset)
            .fetch_all(&mut **transaction)
            .await
            .context("failed to hydrate memory jobs")?;
            if rows.is_empty() {
                break;
            }

            for row in rows {
                let id_text: String = row.try_get("id")?;
                let id = Uuid::parse_str(&id_text)
                    .with_context(|| format!("memory job id {id_text} is not a UUID"))?;
                let kind_text: String = row.try_get("kind")?;
                let kind = MemoryJobKind::from_str(&kind_text)
                    .ok_or_else(|| anyhow!("memory job {id_text} has unknown kind {kind_text}"))?;
                let status_text: String = row.try_get("status")?;
                let status = MemoryJobStatus::from_str(&status_text).ok_or_else(|| {
                    anyhow!("memory job {id_text} has unknown status {status_text}")
                })?;
                let batch_seq = u64::try_from(row.try_get::<i64, _>("batch_seq")?)
                    .with_context(|| format!("memory job {id_text} batch_seq out of u64 range"))?;
                let _attempts = u64::try_from(row.try_get::<i64, _>("attempts")?)
                    .with_context(|| format!("memory job {id_text} attempts out of u64 range"))?;
                let lease_until: Option<String> = row.try_get("lease_until")?;
                match (status, lease_until.as_deref()) {
                    (MemoryJobStatus::Running, Some(lease_until)) => {
                        DateTime::parse_from_rfc3339(lease_until).with_context(|| {
                            format!("running memory job {id_text} has an invalid lease timestamp")
                        })?;
                    }
                    (MemoryJobStatus::Running, None) => {
                        bail!("running memory job {id_text} is missing its lease");
                    }
                    (_, Some(_)) => {
                        bail!("non-running memory job {id_text} retains a lease");
                    }
                    (_, None) => {}
                }

                let source_ids_json: String = row.try_get("source_ids")?;
                let source_id_texts: Vec<String> = serde_json::from_str(&source_ids_json)
                    .with_context(|| {
                        format!("memory job {id_text} source_ids is not valid JSON")
                    })?;
                let source_ids = source_id_texts
                    .into_iter()
                    .map(|source_id| {
                        Uuid::parse_str(&source_id).with_context(|| {
                            format!(
                                "memory job {id_text} source batch id {source_id} is not a UUID"
                            )
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                let source_versions_json: String = row.try_get("source_versions")?;
                let source_version_texts: BTreeMap<String, i64> =
                    serde_json::from_str(&source_versions_json).with_context(|| {
                        format!("memory job {id_text} source_versions is not valid JSON")
                    })?;
                let mut source_versions = BTreeMap::new();
                for (batch_id_text, version) in source_version_texts {
                    let batch_id = Uuid::parse_str(&batch_id_text).with_context(|| {
                        format!(
                            "memory job {id_text} source-version batch id {batch_id_text} is not a UUID"
                        )
                    })?;
                    let version = u64::try_from(version).with_context(|| {
                        format!(
                            "memory job {id_text} source version for {batch_id_text} is out of u64 range"
                        )
                    })?;
                    if source_versions.insert(batch_id, version).is_some() {
                        bail!(
                            "memory job {id_text} has duplicate normalized source-version batch id {batch_id}"
                        );
                    }
                }

                let result = match (
                    row.try_get::<Option<String>, _>("result_key_ref")?,
                    row.try_get::<Option<Vec<u8>>, _>("result_ciphertext")?,
                    row.try_get::<Option<String>, _>("result_projection")?,
                    row.try_get::<Option<i64>, _>("result_redaction_version")?,
                ) {
                    (Some(key_ref), Some(ciphertext), Some(projection), Some(version)) => Some(
                        self.hydrate_memory_summary(
                            &mut key_cache,
                            transaction,
                            &key_ref,
                            &ciphertext,
                            &projection,
                            version,
                            "memory_jobs",
                            &id_text,
                        )
                        .await?,
                    ),
                    (None, None, None, None) => None,
                    _ => bail!("memory job {id_text} result fields are inconsistent"),
                };

                jobs.push(HydratedMemoryJob::new(
                    id,
                    kind,
                    batch_seq,
                    source_ids,
                    source_versions,
                    status,
                    result,
                ));
            }
            offset += HYDRATION_PAGE_SIZE;
        }

        let mut cursors = Vec::new();
        offset = 0;
        loop {
            let rows = sqlx::query(
                "SELECT kind, next_batch_seq
                 FROM memory_apply_cursors ORDER BY kind LIMIT ? OFFSET ?",
            )
            .bind(HYDRATION_PAGE_SIZE)
            .bind(offset)
            .fetch_all(&mut **transaction)
            .await
            .context("failed to hydrate memory apply cursors")?;
            if rows.is_empty() {
                break;
            }

            for row in rows {
                let kind_text: String = row.try_get("kind")?;
                let kind = MemoryJobKind::from_str(&kind_text)
                    .ok_or_else(|| anyhow!("memory apply cursor has unknown kind {kind_text}"))?;
                let next_batch_seq = u64::try_from(row.try_get::<i64, _>("next_batch_seq")?)
                    .with_context(|| {
                        format!("memory apply cursor {kind_text} next_batch_seq out of u64 range")
                    })?;
                cursors.push(HydratedMemoryCursor::new(kind, next_batch_seq));
            }
            offset += HYDRATION_PAGE_SIZE;
        }

        Ok(
            HydratedMemoryRuntime::new(batches, memberships, jobs, cursors, anchored_footprints)
                .with_calibration(calibration),
        )
    }

    async fn load_hydration_key(
        &self,
        cache: &mut HashMap<String, Arc<DataKeyMaterial>>,
        transaction: &mut Transaction<'_, Sqlite>,
        key_ref: &str,
        expected_purpose: DataKeyPurpose,
    ) -> Result<Arc<DataKeyMaterial>> {
        if let Some(key) = cache.get(key_ref) {
            if key.purpose != expected_purpose {
                bail!(
                    "hydration data key {key_ref} has purpose {}, expected {}",
                    key.purpose.as_str(),
                    expected_purpose.as_str()
                );
            }
            return Ok(key.clone());
        }
        let key = self
            .data_key_by_ref_in_transaction(transaction, key_ref)
            .await
            .with_context(|| format!("failed to load hydration data key {key_ref}"))?;
        if key.purpose != expected_purpose {
            bail!(
                "hydration data key {key_ref} has purpose {}, expected {}",
                key.purpose.as_str(),
                expected_purpose.as_str()
            );
        }
        let key = Arc::new(key);
        cache.insert(key_ref.to_owned(), key.clone());
        Ok(key)
    }

    #[allow(clippy::too_many_arguments)]
    async fn hydrate_memory_summary(
        &self,
        key_cache: &mut HashMap<String, Arc<DataKeyMaterial>>,
        transaction: &mut Transaction<'_, Sqlite>,
        key_ref: &str,
        ciphertext: &[u8],
        projection: &str,
        stored_redaction_version: i64,
        table: &str,
        row_id: &str,
    ) -> Result<HydratedMemorySummary> {
        let redaction_version = u32::try_from(stored_redaction_version).with_context(|| {
            format!("{table} projection for {row_id} has redaction version out of u32 range")
        })?;
        if redaction_version != self.redactor.version() {
            bail!(
                "{table} projection for {row_id} uses unsupported redaction version {redaction_version}"
            );
        }
        let key = self
            .load_hydration_key(
                key_cache,
                transaction,
                key_ref,
                DataKeyPurpose::MemorySummary,
            )
            .await
            .with_context(|| format!("failed to load data key for {table} {row_id}"))?;
        let aad = self
            .scope
            .row_aad(table, row_id, DataKeyPurpose::MemorySummary);
        let plaintext = Zeroizing::new(
            decrypt_content(&key, ciphertext, &aad)
                .with_context(|| format!("failed to decrypt {table} projection for {row_id}"))?,
        );
        let derived = self
            .redactor
            .redact_serialized(&plaintext)
            .with_context(|| format!("failed to redact {table} plaintext for {row_id}"))?;
        if derived != projection {
            bail!("{table} projection for {row_id} does not match re-derived redacted plaintext");
        }
        let payload: MemorySummaryPayload =
            serde_json::from_slice(&plaintext).with_context(|| {
                format!("{table} projection for {row_id} is not a valid MemorySummaryPayload")
            })?;
        HydratedMemorySummary::new(
            payload.summary,
            payload.est_tokens,
            payload.from,
            payload.to,
        )
        .with_context(|| format!("{table} projection for {row_id} has an invalid summary payload"))
    }

    pub(crate) fn scope(&self) -> &AgentScope {
        &self.scope
    }

    pub(crate) fn redactor(&self) -> &Redactor {
        &self.redactor
    }

    /// Load persisted approval rules in durable creation order.
    ///
    /// Each stored `pattern` is deserialized as an `ApprovalRule`; fail-closed
    /// on malformed JSON or a column/pattern mismatch. The returned rules are
    /// not validated here; callers must feed them through `Policy::from_rules`
    /// (or `try_with_rule`) before trusting them.
    #[allow(dead_code)] // Production bootstrap (T26) owns construction of the broker.
    pub(crate) async fn load_approval_rules(
        &self,
    ) -> Result<Vec<crate::approval::policy::ApprovalRule>> {
        let rows: Vec<(String, String, String)> =
            sqlx::query_as("SELECT id, tool, pattern FROM approval_rules ORDER BY created_at, id")
                .fetch_all(&self.pool)
                .await
                .context("failed to load approval rules")?;

        let mut rules = Vec::with_capacity(rows.len());
        for (id, tool, pattern) in rows {
            let rule: crate::approval::policy::ApprovalRule = serde_json::from_str(&pattern)
                .with_context(|| format!("approval rule {id} has malformed pattern"))?;
            if rule.id != id || rule.tool != tool {
                bail!("approval rule {id} stored columns do not match pattern contents");
            }
            rules.push(rule);
        }
        Ok(rules)
    }

    /// Load and verify the control-plane-signed D6 materialized policy cache.
    #[allow(dead_code)] // T26 consumes this control-plane cache load seam.
    pub(crate) async fn load_approval_policy(
        &self,
        workspace_root: impl Into<std::path::PathBuf>,
        trust: &crate::approval::policy::ApprovalPolicyTrustStore,
        authority_tenant_id: &str,
        minimum_version: u64,
        now: chrono::DateTime<Utc>,
    ) -> Result<crate::approval::policy::LoadedApprovalPolicy> {
        use crate::approval::policy::{
            ApprovalPolicyBundle, ApprovalPolicyCacheStatus, LoadedApprovalPolicy, Policy,
            SignedApprovalPolicyBundle,
        };

        let workspace_root = workspace_root.into();
        let row = sqlx::query(
            "SELECT tenant_id, personality_agent_id, version, issued_at, expires_at, key_id,
                    payload_json, signature
             FROM approval_policy_cache WHERE singleton = 1",
        )
        .fetch_optional(&self.pool)
        .await
        .context("failed to load approval policy cache")?;
        let Some(row) = row else {
            return Ok(LoadedApprovalPolicy {
                policy: Policy::unavailable_authority(workspace_root),
                status: ApprovalPolicyCacheStatus::Missing,
            });
        };

        let unavailable = |reason: String| LoadedApprovalPolicy {
            policy: Policy::unavailable_authority(workspace_root.clone()),
            status: ApprovalPolicyCacheStatus::Unavailable { reason },
        };
        let payload_json: String = row.try_get("payload_json")?;
        let payload: ApprovalPolicyBundle = match serde_json::from_str(&payload_json) {
            Ok(payload) => payload,
            Err(error) => return Ok(unavailable(format!("malformed cached payload: {error}"))),
        };
        let stored_version: i64 = row.try_get("version")?;
        let denormalized_matches = stored_version >= 0
            && u64::try_from(stored_version).ok() == Some(payload.version)
            && row.try_get::<String, _>("tenant_id")? == payload.tenant_id
            && row.try_get::<String, _>("personality_agent_id")?
                == payload.personality_agent_id.as_str()
            && row.try_get::<String, _>("issued_at")? == payload.issued_at.to_rfc3339()
            && row.try_get::<String, _>("expires_at")? == payload.expires_at.to_rfc3339();
        if !denormalized_matches {
            return Ok(unavailable(
                "cached approval policy metadata does not match its signed payload".to_owned(),
            ));
        }
        let signed = SignedApprovalPolicyBundle {
            key_id: row.try_get("key_id")?,
            payload,
            signature: row.try_get("signature")?,
        };
        if let Err(error) = trust.verify(
            &signed,
            authority_tenant_id,
            self.scope.personality_agent_id(),
            minimum_version,
            now,
        ) {
            return Ok(unavailable(error.to_string()));
        }
        let policy = Policy::from_verified_bundle(workspace_root, &signed.payload)
            .context("verified approval policy failed deterministic validation")?;
        Ok(LoadedApprovalPolicy {
            policy,
            status: ApprovalPolicyCacheStatus::Verified {
                version: signed.payload.version,
                expires_at: signed.payload.expires_at,
            },
        })
    }

    #[allow(dead_code)] // T26 consumes this control-plane cache installation seam.
    pub(crate) async fn install_approval_policy_bundle(
        &self,
        signed: &crate::approval::policy::SignedApprovalPolicyBundle,
        trust: &crate::approval::policy::ApprovalPolicyTrustStore,
        authority_tenant_id: &str,
        now: chrono::DateTime<Utc>,
    ) -> Result<()> {
        trust.verify(
            signed,
            authority_tenant_id,
            self.scope.personality_agent_id(),
            0,
            now,
        )?;
        let version = i64::try_from(signed.payload.version)
            .context("approval policy version exceeds SQLite INTEGER")?;
        let payload_json =
            serde_json::to_string(&signed.payload).context("serialize approval policy cache")?;
        let mut transaction = self.pool.begin().await?;
        let existing =
            sqlx::query("SELECT version, key_id, payload_json, signature FROM approval_policy_cache WHERE singleton=1")
                .fetch_optional(&mut *transaction)
                .await?;
        if let Some(existing) = existing {
            let existing_version: i64 = existing.try_get("version")?;
            if existing_version > version {
                bail!("approval policy bundle version rollback is forbidden");
            }
            if existing_version == version {
                let exact = existing.try_get::<String, _>("key_id")? == signed.key_id
                    && existing.try_get::<String, _>("payload_json")? == payload_json
                    && existing.try_get::<Vec<u8>, _>("signature")? == signed.signature;
                if exact {
                    transaction.rollback().await?;
                    return Ok(());
                }
                bail!("approval policy bundle version conflicts with cached material");
            }
        }
        sqlx::query(
            "INSERT INTO approval_policy_cache(
                singleton, tenant_id, personality_agent_id, version, issued_at, expires_at,
                key_id, payload_json, signature, installed_at
             ) VALUES(1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(singleton) DO UPDATE SET
                tenant_id=excluded.tenant_id,
                personality_agent_id=excluded.personality_agent_id,
                version=excluded.version,
                issued_at=excluded.issued_at,
                expires_at=excluded.expires_at,
                key_id=excluded.key_id,
                payload_json=excluded.payload_json,
                signature=excluded.signature,
                installed_at=excluded.installed_at",
        )
        .bind(&signed.payload.tenant_id)
        .bind(signed.payload.personality_agent_id.as_str())
        .bind(version)
        .bind(signed.payload.issued_at.to_rfc3339())
        .bind(signed.payload.expires_at.to_rfc3339())
        .bind(&signed.key_id)
        .bind(payload_json)
        .bind(&signed.signature)
        .bind(now.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
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

        let rows = sqlx::query("SELECT personality_agent_id FROM agent_scope ORDER BY singleton")
            .fetch_all(&self.pool)
            .await
            .context("failed to read agent scope")?;
        if rows.len() != 1 {
            bail!("agent_scope must contain exactly one row");
        }
        let row = &rows[0];
        let stored = AgentScope::new(
            row.try_get::<String, _>("personality_agent_id")?
                .parse()
                .context("stored personality agent id is invalid")?,
        );
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
            "SELECT key_ref, scope, purpose, personality_agent_id, retention_unit, algorithm,
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
                "personality_agent" => DataKeyScope::PersonalityAgent,
                value => bail!("active data key {key_ref} has unknown scope {value}"),
            };
            let personality_agent_id: String = row.try_get("personality_agent_id")?;
            let retention_unit: String = row.try_get("retention_unit")?;
            validate_retention_unit(purpose, &retention_unit)?;
            if personality_agent_id != self.scope.personality_agent_id.as_str() {
                bail!("active private key {key_ref} is bound to another personality agent");
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
                personality_agent_id,
                retention_unit,
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

    pub(crate) async fn private_key(&self, purpose: DataKeyPurpose) -> Result<DataKeyMaterial> {
        if matches!(
            purpose,
            DataKeyPurpose::ProviderContext
                | DataKeyPurpose::MemorySummary
                | DataKeyPurpose::Artifact
        ) {
            bail!(
                "provider-context, memory-summary, and artifact keys require caller-stable retention anchors"
            );
        }
        if let Some(key) = self.load_active_private_key(purpose).await? {
            return Ok(key);
        }

        let wrapping_key = self.key_provider.current_key().await?;
        let key_ref = format!("{}-{}", purpose.as_str(), Uuid::now_v7());
        let data_key = DataKeyMaterial::generate(&key_ref, purpose)?;
        let aad = KeyWrapAad {
            key_ref: key_ref.clone(),
            scope: DataKeyScope::PersonalityAgent,
            purpose,
            personality_agent_id: self.scope.personality_agent_id.to_string(),
            retention_unit: "agent".to_owned(),
            wrap_key_id: wrapping_key.key_id().to_owned(),
        };
        let (wrap_nonce, wrapped_key) = wrap_data_key(&data_key, &wrapping_key, &aad)?;
        let result = sqlx::query(
            "INSERT INTO data_keys(
                key_ref, scope, purpose, personality_agent_id, retention_unit, algorithm, wrap_key_id,
                wrap_nonce, wrapped_key, state, created_at, destroyed_at
             ) VALUES(?, 'personality_agent', ?, ?, 'agent', ?, ?, ?, ?, 'active', ?, NULL)",
        )
        .bind(&key_ref)
        .bind(purpose.as_str())
        .bind(self.scope.personality_agent_id.as_str())
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
                .load_active_private_key(purpose)
                .await?
                .ok_or_else(|| anyhow!("active data key disappeared after creation race")),
            Err(error) => Err(error).context("failed to persist wrapped private data key"),
        }
    }

    pub(crate) async fn command_digest_factory(
        &self,
    ) -> Result<Arc<dyn crate::gateway::CommandDigestFactory>> {
        let key = self.private_key(DataKeyPurpose::Command).await?;
        Ok(Arc::new(PersonalityAgentCommandDigestFactory::new(&key)?))
    }

    async fn load_active_private_key(
        &self,
        purpose: DataKeyPurpose,
    ) -> Result<Option<DataKeyMaterial>> {
        if matches!(
            purpose,
            DataKeyPurpose::ProviderContext
                | DataKeyPurpose::MemorySummary
                | DataKeyPurpose::Artifact
        ) {
            bail!(
                "provider-context, memory-summary, and artifact keys require authenticated retention anchors"
            );
        }
        let row = sqlx::query(
            "SELECT key_ref, retention_unit, wrap_key_id, wrap_nonce, wrapped_key
             FROM data_keys
             WHERE scope = 'personality_agent' AND personality_agent_id = ? AND purpose = ?
               AND retention_unit = 'agent' AND state = 'active'",
        )
        .bind(self.scope.personality_agent_id.as_str())
        .bind(purpose.as_str())
        .fetch_optional(&self.pool)
        .await
        .context("failed to load private data key")?;
        let Some(row) = row else {
            return Ok(None);
        };
        let key_ref: String = row.try_get("key_ref")?;
        let wrap_key_id: String = row.try_get("wrap_key_id")?;
        let wrapping_key = self.key_provider.key_by_id(&wrap_key_id).await?;
        let aad = KeyWrapAad {
            key_ref: key_ref.clone(),
            scope: DataKeyScope::PersonalityAgent,
            purpose,
            personality_agent_id: self.scope.personality_agent_id.to_string(),
            retention_unit: row.try_get("retention_unit")?,
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
        if anchor.personality_agent_id != self.scope.personality_agent_id {
            bail!("provider-context anchor belongs to a different personality agent");
        }
        if anchor.anchor_id.is_empty() || anchor.anchor_id.len() > 1024 {
            bail!("provider-context anchor identity must be 1..=1024 bytes");
        }

        self.anchored_private_key(
            DataKeyPurpose::ProviderContext,
            "provider_context",
            &anchor.anchor_id,
        )
        .await
    }

    #[allow(
        dead_code,
        reason = "artifact broker wiring consumes this key boundary"
    )]
    pub(crate) async fn artifact_key(&self, anchor: &ArtifactKeyAnchor) -> Result<DataKeyMaterial> {
        if anchor.personality_agent_id != self.scope.personality_agent_id {
            bail!("artifact anchor belongs to a different personality agent");
        }
        let required_prefix = format!("artifact://{}/", self.scope.personality_agent_id);
        if anchor.artifact_handle.len() > 2048
            || !anchor.artifact_handle.starts_with(&required_prefix)
            || anchor.artifact_handle.len() == required_prefix.len()
        {
            bail!("artifact handle must be a non-empty PAID-scoped artifact URI");
        }
        self.anchored_private_key(
            DataKeyPurpose::Artifact,
            "artifact",
            &anchor.artifact_handle,
        )
        .await
    }

    pub(crate) async fn memory_summary_key(
        &self,
        unit_kind: &'static str,
        unit_id: &str,
    ) -> Result<DataKeyMaterial> {
        if !matches!(unit_kind, "batch" | "job") {
            bail!("memory-summary retention kind must be batch or job");
        }
        if unit_id.is_empty() || unit_id.len() > 1024 {
            bail!("memory-summary retention identity must be 1..=1024 bytes");
        }
        self.anchored_private_key(
            DataKeyPurpose::MemorySummary,
            "memory_summary",
            &format!("{unit_kind}:{unit_id}"),
        )
        .await
    }

    async fn anchored_private_key(
        &self,
        purpose: DataKeyPurpose,
        retention_kind: &str,
        anchor_id: &str,
    ) -> Result<DataKeyMaterial> {
        let retention_unit = anchored_retention_unit(retention_kind, &self.scope, anchor_id);
        let key_ref = format!(
            "{}-{}",
            purpose.as_str(),
            retention_unit
                .split_once(':')
                .expect("anchored retention unit has a kind separator")
                .1
        );
        let existing_state: Option<String> =
            sqlx::query_scalar("SELECT state FROM data_keys WHERE key_ref = ?")
                .bind(&key_ref)
                .fetch_optional(&self.pool)
                .await
                .context("failed to inspect provider-context anchor key")?;
        if let Some(state) = existing_state {
            if state != "active" {
                bail!("{retention_kind} retention key has been crypto-erased");
            }
            let key = self.data_key_by_ref(&key_ref).await?;
            if key.purpose != purpose {
                bail!("{retention_kind} anchor resolved to a key with the wrong purpose");
            }
            return Ok(key);
        }

        let wrapping_key = self.key_provider.current_key().await?;
        let data_key = DataKeyMaterial::generate(&key_ref, purpose)?;
        let aad = KeyWrapAad {
            key_ref: key_ref.clone(),
            scope: DataKeyScope::PersonalityAgent,
            purpose,
            personality_agent_id: self.scope.personality_agent_id.to_string(),
            retention_unit: retention_unit.clone(),
            wrap_key_id: wrapping_key.key_id().to_owned(),
        };
        let (wrap_nonce, wrapped_key) = wrap_data_key(&data_key, &wrapping_key, &aad)?;
        let result = sqlx::query(
            "INSERT INTO data_keys(
                key_ref, scope, purpose, personality_agent_id, retention_unit, algorithm, wrap_key_id,
                wrap_nonce, wrapped_key, state, created_at, destroyed_at
             ) VALUES(?, 'personality_agent', ?, ?, ?, ?, ?, ?, ?, 'active', ?, NULL)",
        )
        .bind(&key_ref)
        .bind(purpose.as_str())
        .bind(self.scope.personality_agent_id.as_str())
        .bind(&retention_unit)
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
                let key = self.data_key_by_ref(&key_ref).await.with_context(|| {
                    format!("{retention_kind} anchor key is not active after creation race")
                })?;
                if key.purpose != purpose {
                    bail!("{retention_kind} anchor resolved to a key with the wrong purpose");
                }
                Ok(key)
            }
            Err(error) => {
                Err(error).with_context(|| format!("failed to persist {retention_kind} anchor key"))
            }
        }
    }

    pub(crate) async fn data_key_by_ref(&self, key_ref: &str) -> Result<DataKeyMaterial> {
        let row = sqlx::query(
            "SELECT purpose, personality_agent_id, retention_unit, wrap_key_id, wrap_nonce, wrapped_key
             FROM data_keys
             WHERE key_ref = ? AND scope = 'personality_agent' AND state = 'active'",
        )
        .bind(key_ref)
        .fetch_optional(&self.pool)
        .await
        .context("failed to load data key by reference")?
        .ok_or_else(|| anyhow!("active data key {key_ref} is unavailable"))?;
        let purpose = DataKeyPurpose::parse(row.try_get("purpose")?)?;
        let personality_agent_id: String = row.try_get("personality_agent_id")?;
        let retention_unit: String = row.try_get("retention_unit")?;
        validate_retention_unit(purpose, &retention_unit)?;
        if personality_agent_id != self.scope.personality_agent_id.as_str() {
            bail!("data key {key_ref} belongs to another personality agent");
        }
        let wrap_key_id: String = row.try_get("wrap_key_id")?;
        let wrapping_key = self.key_provider.key_by_id(&wrap_key_id).await?;
        let aad = KeyWrapAad {
            key_ref: key_ref.to_owned(),
            scope: DataKeyScope::PersonalityAgent,
            purpose,
            personality_agent_id,
            retention_unit,
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
            "SELECT purpose, personality_agent_id, retention_unit, wrap_key_id, wrap_nonce, wrapped_key
             FROM data_keys
             WHERE key_ref = ? AND scope = 'personality_agent' AND state = 'active'",
        )
        .bind(key_ref)
        .fetch_optional(&mut **transaction)
        .await
        .context("failed to load data key by reference in EventBatch")?
        .ok_or_else(|| anyhow!("active data key {key_ref} is unavailable"))?;
        let purpose = DataKeyPurpose::parse(row.try_get("purpose")?)?;
        let personality_agent_id: String = row.try_get("personality_agent_id")?;
        let retention_unit: String = row.try_get("retention_unit")?;
        validate_retention_unit(purpose, &retention_unit)?;
        if personality_agent_id != self.scope.personality_agent_id.as_str() {
            bail!("data key {key_ref} belongs to another personality agent");
        }
        let wrap_key_id: String = row.try_get("wrap_key_id")?;
        let wrapping_key = self.key_provider.key_by_id(&wrap_key_id).await?;
        let aad = KeyWrapAad {
            key_ref: key_ref.to_owned(),
            scope: DataKeyScope::PersonalityAgent,
            purpose,
            personality_agent_id,
            retention_unit,
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

    /// Destroys one PAID-owned derived data key inside an existing transaction.
    /// The caller is responsible for committing the transaction.
    pub(crate) async fn destroy_private_key_ref_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        key_ref: &str,
    ) -> Result<()> {
        if key_ref.is_empty() {
            bail!("crypto-erase key_ref must not be empty");
        }
        let row = sqlx::query(
            "SELECT scope, purpose, personality_agent_id, retention_unit,
                    state, wrapped_key, wrap_nonce, destroyed_at
             FROM data_keys WHERE key_ref = ?",
        )
        .bind(key_ref)
        .fetch_optional(&mut **transaction)
        .await
        .context("failed to load crypto-erase target")?
        .ok_or_else(|| anyhow!("crypto-erase key_ref {key_ref} does not exist"))?;

        let scope: String = row.try_get("scope")?;
        let purpose = DataKeyPurpose::parse(row.try_get("purpose")?)?;
        let personality_agent_id: String = row.try_get("personality_agent_id")?;
        validate_retention_unit(purpose, row.try_get("retention_unit")?)?;
        if scope != DataKeyScope::PersonalityAgent.as_str()
            || personality_agent_id != self.scope.personality_agent_id.as_str()
            || purpose == DataKeyPurpose::Workspace
        {
            bail!("crypto-erase key_ref {key_ref} is outside the active personality agent scope");
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
                     WHERE key_ref = ? AND scope = 'personality_agent'
                       AND personality_agent_id = ? AND state = 'active'",
                )
                .bind(Utc::now().to_rfc3339())
                .bind(key_ref)
                .bind(self.scope.personality_agent_id.as_str())
                .execute(&mut **transaction)
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
        Ok(())
    }

    /// Transactionally destroys one PAID-owned derived key by durable reference.
    /// This does not erase the canonical life log or reset the personality.
    #[allow(
        dead_code,
        reason = "T11 product crypto-erase boundary is wired to lifecycle callers in M3"
    )]
    pub(crate) async fn destroy_private_key_ref(&self, key_ref: &str) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        self.destroy_private_key_ref_in_transaction(&mut transaction, key_ref)
            .await?;
        transaction.commit().await?;
        Ok(())
    }
}

#[allow(
    dead_code,
    reason = "used by the T11 per-anchor key boundary before its production caller exists"
)]
fn anchored_retention_unit(kind: &str, scope: &AgentScope, anchor_id: &str) -> String {
    debug_assert!(matches!(
        kind,
        "provider_context" | "memory_summary" | "artifact"
    ));
    let mut digest = Sha256::new();
    digest.update(b"sumi-retention-unit/v1");
    for field in [
        kind.as_bytes(),
        scope.personality_agent_id.as_str().as_bytes(),
        anchor_id.as_bytes(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    format!("{kind}:{:x}", digest.finalize())
}

fn validate_retention_unit(purpose: DataKeyPurpose, retention_unit: &str) -> Result<()> {
    let valid_hash_unit = |prefix: &str| {
        retention_unit.strip_prefix(prefix).is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .as_bytes()
                    .iter()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
        })
    };
    let valid = match purpose {
        DataKeyPurpose::ProviderContext => valid_hash_unit("provider_context:"),
        DataKeyPurpose::MemorySummary => valid_hash_unit("memory_summary:"),
        DataKeyPurpose::Artifact => valid_hash_unit("artifact:"),
        _ => retention_unit == "agent",
    };
    if !valid {
        bail!(
            "data-key retention unit {retention_unit:?} is invalid for purpose {}",
            purpose.as_str()
        );
    }
    Ok(())
}

fn provider_context_key_ref(scope: &AgentScope, anchor_id: &str) -> String {
    let retention_unit = anchored_retention_unit("provider_context", scope, anchor_id);
    format!(
        "provider_context-{}",
        retention_unit
            .split_once(':')
            .expect("provider-context retention unit has a separator")
            .1
    )
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
    use crate::gateway::{Command, CommandEnvelope, CommandId, InboundCommand};
    use crate::memory::context_assembler::ContextAssembler;
    use crate::memory::estimate::eviction_footprint_for_payload;
    use crate::provider::model::ModelSpec;
    use crate::provider::types::{
        ApiProtocol, AssistantMessage, ContextMessage, Message, ProviderContextAnchor,
        ProviderContextItem, ProviderContextPayload, PublicAssistantMessage, PublicMessage,
        StopReason, Usage, UserContent, UserMessage,
    };
    use crate::runtime::contracts::{
        DirectChatProvenanceV1, GenerationRecoveryFence, ProcessGeneration, ProcessGenerationLease,
    };
    use crate::store::crypto::{DATA_KEY_BYTES, WrappingKey, decrypt_content, encrypt_content};
    use crate::store::transcript::TranscriptRecord;
    use chrono::{Duration as ChronoDuration, Utc};
    use ed25519_dalek::{Signer, SigningKey};
    use serde_json::json;
    use tokio_util::sync::CancellationToken;

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
            personality_agent_id: "0198f0f4-9b72-7000-8000-000000000001".parse().unwrap(),
        }
    }

    fn direct_chat_provenance() -> DirectChatProvenanceV1 {
        DirectChatProvenanceV1::new("tenant-1", scope().personality_agent_id, "human-1")
            .expect("valid direct-chat provenance")
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

    fn approval_signer() -> SigningKey {
        SigningKey::from_bytes(&[0x71; 32])
    }

    fn approval_trust() -> crate::approval::ApprovalPolicyTrustStore {
        crate::approval::ApprovalPolicyTrustStore::new([(
            "control-plane-v1".to_owned(),
            approval_signer().verifying_key(),
        )])
        .expect("fixture approval trust")
    }

    fn signed_approval_bundle(
        version: u64,
        rules: Vec<crate::approval::ApprovalRule>,
        issued_at: chrono::DateTime<Utc>,
        expires_at: chrono::DateTime<Utc>,
    ) -> crate::approval::SignedApprovalPolicyBundle {
        let payload = crate::approval::ApprovalPolicyBundle {
            schema_version: crate::approval::policy::APPROVAL_POLICY_BUNDLE_SCHEMA_VERSION,
            tenant_id: "tenant-1".to_owned(),
            personality_agent_id: scope().personality_agent_id,
            version,
            issued_at,
            expires_at,
            rules,
        };
        let signature = approval_signer()
            .sign(&payload.signing_bytes().expect("signing bytes"))
            .to_bytes()
            .to_vec();
        crate::approval::SignedApprovalPolicyBundle {
            key_id: "control-plane-v1".to_owned(),
            payload,
            signature,
        }
    }

    #[tokio::test]
    async fn legacy_contract_database_is_rejected_without_compatibility_migration() {
        let root = std::env::temp_dir().join(format!("sumi-store-legacy-{}", Uuid::now_v7()));
        let database = root.join("conversation.sqlite");
        std::fs::create_dir_all(&root).expect("create legacy fixture directory");
        let options = SqliteConnectOptions::new()
            .filename(&database)
            .create_if_missing(true)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("open legacy database");
        sqlx::query(
            "CREATE TABLE agent_scope(
                singleton INTEGER PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                personality_agent_id TEXT NOT NULL,
                created_at TEXT NOT NULL
             )",
        )
        .execute(&pool)
        .await
        .expect("create legacy scope schema");
        sqlx::query(
            "CREATE TABLE data_keys(
                key_ref TEXT PRIMARY KEY,
                scope TEXT NOT NULL,
                purpose TEXT NOT NULL,
                personality_agent_id TEXT,
                algorithm TEXT NOT NULL,
                wrap_key_id TEXT NOT NULL,
                wrap_nonce BLOB,
                wrapped_key BLOB,
                state TEXT NOT NULL,
                created_at TEXT NOT NULL,
                destroyed_at TEXT
             )",
        )
        .execute(&pool)
        .await
        .expect("create legacy credential schema");
        sqlx::query(
            "INSERT INTO agent_scope(
                singleton, tenant_id, agent_id, personality_agent_id, created_at
             ) VALUES(1, 'tenant-1', 'agent-1', 'conversation-1', 'now')",
        )
        .execute(&pool)
        .await
        .expect("insert legacy scope identity");
        sqlx::query(
            "INSERT INTO data_keys(
                key_ref, scope, purpose, personality_agent_id, algorithm, wrap_key_id,
                wrap_nonce, wrapped_key, state, created_at, destroyed_at
             ) VALUES(
                'legacy-transcript-key', 'conversation', 'transcript',
                'conversation-1', 'xchacha20-poly1305/v2', 'test-wrap-v1',
                zeroblob(24), zeroblob(48), 'active', 'now', NULL
             )",
        )
        .execute(&pool)
        .await
        .expect("insert legacy credential fixture");
        pool.close().await;

        let error = Store::open(&database, scope(), provider())
            .await
            .expect_err("legacy schema and credentials must fail closed");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("migration") || rendered.contains("already exists"),
            "unexpected legacy rejection: {rendered}"
        );

        let options = SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .expect("reopen rejected legacy fixture");
        let legacy_identity: String =
            sqlx::query_scalar("SELECT personality_agent_id FROM agent_scope WHERE singleton=1")
                .fetch_one(&pool)
                .await
                .expect("legacy identity remains intact");
        assert_eq!(legacy_identity, "conversation-1");
        let legacy_algorithm: String = sqlx::query_scalar(
            "SELECT algorithm FROM data_keys WHERE key_ref='legacy-transcript-key'",
        )
        .fetch_one(&pool)
        .await
        .expect("legacy credential remains intact");
        assert_eq!(legacy_algorithm, "xchacha20-poly1305/v2");
        pool.close().await;

        std::fs::remove_dir_all(root).expect("remove legacy fixture directory");
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
            .private_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint transcript key");
        let event_key = store
            .private_key(DataKeyPurpose::Event)
            .await
            .expect("mint event key");

        let data_key_error = sqlx::query(
            "INSERT INTO data_keys(
                key_ref, scope, purpose, personality_agent_id, algorithm, wrap_key_id,
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
                personality_agent_id, last_seq, event_count, chain_digest, key_ref,
                head_hmac, updated_at
             ) VALUES(NULL, 1, 1, zeroblob(32), ?, zeroblob(32), 'now')",
        )
        .bind(&event_key.key_ref)
        .execute(store.pool())
        .await
        .expect_err("event_log_heads.personality_agent_id NULL must be rejected");
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
            .artifact_key(&ArtifactKeyAnchor {
                personality_agent_id: scope().personality_agent_id,
                artifact_handle: format!(
                    "artifact://{}/null-identity-test",
                    scope().personality_agent_id
                ),
            })
            .await
            .expect("valid anchored artifact key still inserts");
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
                personality_agent_id, last_seq, event_count, chain_digest, key_ref,
                head_hmac, updated_at
             ) VALUES(?, 1, 1, zeroblob(32), ?, zeroblob(32), 'now')",
        )
        .bind(scope().personality_agent_id.as_str())
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
            (
                "unknown",
                "transcript",
                Some("0198f0f4-9b72-7000-8000-000000000001"),
            ),
            (
                "0198f0f4-9b72-7000-8000-000000000001",
                "unknown",
                Some("0198f0f4-9b72-7000-8000-000000000001"),
            ),
            (
                "0198f0f4-9b72-7000-8000-000000000001",
                "workspace",
                Some("0198f0f4-9b72-7000-8000-000000000001"),
            ),
            ("0198f0f4-9b72-7000-8000-000000000001", "transcript", None),
            (
                "agent",
                "workspace",
                Some("0198f0f4-9b72-7000-8000-000000000001"),
            ),
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
        for (scope, purpose, personality_agent_id) in invalid {
            let result = sqlx::query(
                "INSERT INTO data_keys(
                    key_ref, scope, purpose, personality_agent_id, algorithm, wrap_key_id,
                    wrap_nonce, wrapped_key, state, created_at, destroyed_at
                 ) VALUES(?, ?, ?, ?, ?, 'wrap', X'00', X'00', 'active', 'now', NULL)",
            )
            .bind(format!("{scope}-{purpose}-{personality_agent_id:?}"))
            .bind(scope)
            .bind(purpose)
            .bind(personality_agent_id)
            .bind(WRAP_ALGORITHM)
            .execute(store.pool())
            .await;
            assert!(result.is_err(), "fixture must violate CHECK constraints");
        }
    }

    #[tokio::test]
    async fn migration_rejects_lookalike_retention_unit_prefixes() {
        let store = store().await;
        for (index, (purpose, retention_unit)) in [
            (
                "provider_context",
                format!("providerXcontext:{}", "0".repeat(64)),
            ),
            (
                "memory_summary",
                format!("memoryXsummary:{}", "0".repeat(64)),
            ),
            ("artifact", format!("artifactX{}", "0".repeat(64))),
        ]
        .into_iter()
        .enumerate()
        {
            let result = sqlx::query(
                "INSERT INTO data_keys(
                    key_ref, scope, purpose, personality_agent_id, retention_unit,
                    algorithm, wrap_key_id, wrap_nonce, wrapped_key, state,
                    created_at, destroyed_at
                 ) VALUES(
                    ?, 'personality_agent', ?, ?, ?, ?, 'wrap', X'00', X'00',
                    'active', 'now', NULL
                 )",
            )
            .bind(format!("lookalike-retention-{index}"))
            .bind(purpose)
            .bind(scope().personality_agent_id.as_str())
            .bind(retention_unit)
            .bind(WRAP_ALGORITHM)
            .execute(store.pool())
            .await;
            assert!(
                result.is_err(),
                "lookalike {purpose} retention prefix must be rejected"
            );
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
                    key_ref, scope, purpose, personality_agent_id, retention_unit,
                    algorithm, wrap_key_id, wrap_nonce, wrapped_key, state,
                    created_at, destroyed_at
                 ) VALUES(
                    ?, 'personality_agent', 'artifact', ?,
                    'artifact:0000000000000000000000000000000000000000000000000000000000000000',
                    ?, 'wrap', ?, ?, ?, 'now', ?
                 )",
            )
            .bind(format!("invalid-state-{index}"))
            .bind(scope().personality_agent_id.as_str())
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
            .private_key(DataKeyPurpose::Command)
            .await
            .expect("mint command key");
        let duplicate = store
            .private_key(DataKeyPurpose::Command)
            .await
            .expect("reuse active command key");
        assert_eq!(key.key_ref, duplicate.key_ref);

        store
            .destroy_private_key_ref(&key.key_ref)
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
            .private_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint conversation transcript key");
        let provider_anchor = store
            .provider_context_key(&ProviderContextKeyAnchor {
                personality_agent_id: "0198f0f4-9b72-7000-8000-000000000001".parse().unwrap(),
                anchor_id: "message-1:7".to_owned(),
            })
            .await
            .expect("mint provider-context anchor key");

        for key_ref in [&transcript.key_ref, &provider_anchor.key_ref] {
            store
                .destroy_private_key_ref(key_ref)
                .await
                .expect("destroy conversation key");
            store
                .destroy_private_key_ref(key_ref)
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

        let workspace = store
            .private_key(DataKeyPurpose::Workspace)
            .await
            .expect("mint PAID-owned workspace key");
        let error = store
            .destroy_private_key_ref(&workspace.key_ref)
            .await
            .expect_err("private crypto erase must reject the workspace key");
        assert!(
            error
                .to_string()
                .contains("outside the active personality agent scope")
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT state FROM data_keys WHERE key_ref=?",)
                .bind(&workspace.key_ref)
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
            personality_agent_id: "0198f0f4-9b72-7000-8000-000000000001".parse().unwrap(),
            anchor_id: "message-1:7".to_owned(),
        };
        let second_anchor = ProviderContextKeyAnchor {
            personality_agent_id: "0198f0f4-9b72-7000-8000-000000000001".parse().unwrap(),
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
        let retention_units: Vec<String> = sqlx::query_scalar(
            "SELECT retention_unit FROM data_keys
             WHERE purpose='provider_context' ORDER BY retention_unit",
        )
        .fetch_all(store.pool())
        .await
        .expect("load provider-context retention units");
        assert_eq!(retention_units.len(), 2);
        assert!(retention_units.iter().all(|unit| {
            unit.starts_with("provider_context:") && unit.len() == "provider_context:".len() + 64
        }));
        assert_ne!(retention_units[0], retention_units[1]);

        let second_aad = store.scope().row_aad(
            "provider_context",
            "context-row-2",
            DataKeyPurpose::ProviderContext,
        );
        let second_ciphertext =
            encrypt_content(&second, b"second-anchor", &second_aad).expect("encrypt second anchor");
        store
            .destroy_private_key_ref(&first.key_ref)
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
    async fn artifact_keys_are_stable_per_handle_and_not_shared_at_paid_grain() {
        let store = store().await;
        assert!(
            store
                .private_key(DataKeyPurpose::Artifact)
                .await
                .expect_err("artifact keys require a handle retention anchor")
                .to_string()
                .contains("retention anchors")
        );
        assert!(
            store
                .artifact_key(&ArtifactKeyAnchor {
                    personality_agent_id: scope().personality_agent_id,
                    artifact_handle: "artifact://0198f0f4-9b72-7000-8000-000000000002/wrong"
                        .to_owned(),
                })
                .await
                .expect_err("handle PAID must match the authenticated Store")
                .to_string()
                .contains("PAID-scoped artifact URI")
        );
        assert!(
            store
                .artifact_key(&ArtifactKeyAnchor {
                    personality_agent_id: "0198f0f4-9b72-7000-8000-000000000002".parse().unwrap(),
                    artifact_handle: format!(
                        "artifact://{}/wrong-typed-anchor",
                        scope().personality_agent_id
                    ),
                })
                .await
                .expect_err("typed anchor PAID must match the authenticated Store")
                .to_string()
                .contains("different personality agent")
        );
        let first_anchor = ArtifactKeyAnchor {
            personality_agent_id: scope().personality_agent_id,
            artifact_handle: format!("artifact://{}/attachments/a", scope().personality_agent_id),
        };
        let second_anchor = ArtifactKeyAnchor {
            personality_agent_id: scope().personality_agent_id,
            artifact_handle: format!("artifact://{}/tool-output/b", scope().personality_agent_id),
        };
        let first = store.artifact_key(&first_anchor).await.unwrap();
        let first_replay = store.artifact_key(&first_anchor).await.unwrap();
        let second = store.artifact_key(&second_anchor).await.unwrap();
        assert_eq!(first.key_ref, first_replay.key_ref);
        assert_ne!(first.key_ref, second.key_ref);
        let units: Vec<String> = sqlx::query_scalar(
            "SELECT retention_unit FROM data_keys
             WHERE purpose='artifact' ORDER BY retention_unit",
        )
        .fetch_all(store.pool())
        .await
        .unwrap();
        assert_eq!(units.len(), 2);
        assert!(
            units.iter().all(|unit| {
                unit.starts_with("artifact:") && unit.len() == "artifact:".len() + 64
            })
        );
    }

    #[tokio::test]
    async fn memory_summary_keys_are_stable_per_batch_or_job_and_independently_erasable() {
        let store = store().await;
        let batch = store.memory_summary_key("batch", "batch-1").await.unwrap();
        let batch_replay = store.memory_summary_key("batch", "batch-1").await.unwrap();
        let other_batch = store.memory_summary_key("batch", "batch-2").await.unwrap();
        let job = store.memory_summary_key("job", "batch-1").await.unwrap();
        assert_eq!(batch.key_ref, batch_replay.key_ref);
        assert_ne!(batch.key_ref, other_batch.key_ref);
        assert_ne!(
            batch.key_ref, job.key_ref,
            "batch and job domains must remain distinct for the same caller ID"
        );

        let job_aad =
            store
                .scope()
                .row_aad("memory_jobs", "batch-1", DataKeyPurpose::MemorySummary);
        let job_ciphertext =
            encrypt_content(&job, b"job-summary", &job_aad).expect("encrypt job summary");
        store
            .destroy_private_key_ref(&batch.key_ref)
            .await
            .expect("erase only the batch summary key");
        assert!(
            store
                .memory_summary_key("batch", "batch-1")
                .await
                .expect_err("erased batch key cannot be reminted")
                .to_string()
                .contains("crypto-erased")
        );
        let job_replay = store.memory_summary_key("job", "batch-1").await.unwrap();
        assert_eq!(
            decrypt_content(&job_replay, &job_ciphertext, &job_aad).unwrap(),
            b"job-summary"
        );

        let units: Vec<String> = sqlx::query_scalar(
            "SELECT retention_unit FROM data_keys
             WHERE purpose='memory_summary' ORDER BY retention_unit",
        )
        .fetch_all(store.pool())
        .await
        .unwrap();
        assert_eq!(units.len(), 3);
        assert!(units.iter().all(|unit| {
            unit.starts_with("memory_summary:") && unit.len() == "memory_summary:".len() + 64
        }));
    }

    #[tokio::test]
    async fn provider_context_key_api_rejects_shared_empty_and_cross_paid_use() {
        let store = store().await;
        assert!(
            store
                .private_key(DataKeyPurpose::ProviderContext)
                .await
                .expect_err("purpose-level shared lookup is forbidden")
                .to_string()
                .contains("caller-stable authenticated anchor")
        );
        assert!(
            store
                .provider_context_key(&ProviderContextKeyAnchor {
                    personality_agent_id: "0198f0f4-9b72-7000-8000-000000000001".parse().unwrap(),
                    anchor_id: String::new(),
                })
                .await
                .expect_err("empty anchor")
                .to_string()
                .contains("1..=1024 bytes")
        );
        assert!(
            store
                .provider_context_key(&ProviderContextKeyAnchor {
                    personality_agent_id: "0198f0f4-9b72-7000-8000-000000000002".parse().unwrap(),
                    anchor_id: "message-1:7".to_owned(),
                })
                .await
                .expect_err("cross-PAID anchor")
                .to_string()
                .contains("different personality agent")
        );
    }

    #[tokio::test]
    async fn private_key_rejects_workspace_purpose() {
        let store = store().await;
        assert!(
            store
                .private_key(DataKeyPurpose::Workspace)
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
    async fn data_key_apis_persist_only_paid_scoped_retention_units() {
        let store = store().await;
        for purpose in [
            DataKeyPurpose::Transcript,
            DataKeyPurpose::Event,
            DataKeyPurpose::Command,
            DataKeyPurpose::Mutation,
            DataKeyPurpose::Workspace,
        ] {
            store
                .private_key(purpose)
                .await
                .unwrap_or_else(|error| panic!("mint shared {} key: {error}", purpose.as_str()));
        }
        store
            .memory_summary_key("batch", "retention-test-batch")
            .await
            .expect("mint batch summary key");
        store
            .provider_context_key(&ProviderContextKeyAnchor {
                personality_agent_id: scope().personality_agent_id,
                anchor_id: "retention-test-context".to_owned(),
            })
            .await
            .expect("mint provider-context key");
        store
            .artifact_key(&ArtifactKeyAnchor {
                personality_agent_id: scope().personality_agent_id,
                artifact_handle: format!(
                    "artifact://{}/retention-test",
                    scope().personality_agent_id
                ),
            })
            .await
            .expect("mint artifact key");

        let rows = sqlx::query(
            "SELECT purpose, personality_agent_id, retention_unit
             FROM data_keys WHERE state='active'",
        )
        .fetch_all(store.pool())
        .await
        .expect("load persisted data-key identities");
        assert!(!rows.is_empty());
        for row in rows {
            let purpose = DataKeyPurpose::parse(row.get("purpose")).expect("known purpose");
            assert_eq!(
                row.get::<String, _>("personality_agent_id"),
                scope().personality_agent_id.as_str()
            );
            validate_retention_unit(purpose, row.get("retention_unit"))
                .expect("purpose-specific retention unit");
        }
    }

    #[tokio::test]
    async fn startup_rejects_authenticated_scope_mismatch() {
        let store = store().await;
        let pool = store.pool.clone();
        drop(store);
        let wrong_scope = AgentScope {
            personality_agent_id: "0198f0f4-9b72-7000-8000-000000000002".parse().unwrap(),
            ..scope()
        };
        let error = match Store::finish_open(pool, wrong_scope, provider()).await {
            Ok(_) => panic!("scope mismatch must fail closed"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("database scope does not match"));
    }

    #[tokio::test]
    async fn wrong_scope_open_cannot_poison_uninitialized_provider_projection_genesis() {
        let store = store().await;
        sqlx::query(
            "UPDATE provider_context_projection_head
             SET state = 'uninitialized', revision = 0, record_count = 0,
                 set_digest = NULL, key_ref = NULL, head_hmac = NULL
             WHERE singleton = 1",
        )
        .execute(store.pool())
        .await
        .expect("simulate crash before provider projection genesis");
        let pool = store.pool.clone();
        drop(store);

        let wrong_scope = AgentScope {
            personality_agent_id: "0198f0f4-9b72-7000-8000-000000000002".parse().unwrap(),
            ..scope()
        };
        let error = match Store::finish_open(pool.clone(), wrong_scope, provider()).await {
            Ok(_) => panic!("wrong scope must fail before projection initialization"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("database scope does not match"));
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM provider_context_projection_head WHERE singleton = 1",
            )
            .fetch_one(&pool)
            .await
            .expect("projection marker remains readable"),
            "uninitialized"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM data_keys
                 WHERE purpose = 'mutation'
                   AND personality_agent_id = '0198f0f4-9b72-7000-8000-000000000002'",
            )
            .fetch_one(&pool)
            .await
            .expect("count wrong-scope mutation keys"),
            0
        );

        let reopened = Store::finish_open(pool, scope(), provider())
            .await
            .expect("correct scope initializes projection genesis after rejected wrong open");
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM provider_context_projection_head WHERE singleton = 1",
            )
            .fetch_one(reopened.pool())
            .await
            .expect("load initialized projection state"),
            "active"
        );
        let mut transaction = reopened
            .pool()
            .begin()
            .await
            .expect("begin projection verify");
        provider_context::verify_provider_context_projection_set(&reopened, &mut transaction)
            .await
            .expect("correct-scope projection genesis authenticates");
    }

    #[tokio::test]
    async fn startup_rejects_tampered_wrapped_key() {
        let store = store().await;
        store
            .private_key(DataKeyPurpose::Event)
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
            .private_key(DataKeyPurpose::Event)
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
            .private_key(DataKeyPurpose::Transcript)
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

    #[tokio::test]
    async fn hydration_key_lookup_rejects_wrong_purpose() {
        let store = store().await;
        let transcript_key = store
            .private_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint transcript key");

        let mut transaction = store.pool().begin().await.expect("begin test transaction");
        let mut cache = HashMap::new();
        let error = store
            .load_hydration_key(
                &mut cache,
                &mut transaction,
                &transcript_key.key_ref,
                DataKeyPurpose::ProviderContext,
            )
            .await
            .expect_err("provider-context purpose lookup for a transcript key must fail");
        let message = format!("{error:#}");
        assert!(
            message.contains("has purpose") && message.contains("expected provider_context"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn provider_context_fk_prevents_message_delete_cascade() {
        let store = store().await;
        let key = store
            .private_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint transcript key for provider_context row");

        sqlx::query(
            "INSERT INTO messages(
                id, seq, role, raw_key_ref, raw_ciphertext, payload, search_text,
                redaction_version, interrupted, created_at
             ) VALUES('msg-fk', 1, 'user', ?, X'00', '{}', '', 1, 0, 'now')",
        )
        .bind(&key.key_ref)
        .execute(store.pool())
        .await
        .expect("seed message");

        sqlx::query(
            "INSERT INTO provider_context(
                id, message_id, message_seq, wire_item_index, item_ordinal,
                idempotency_key, provider_instance_id, protocol, model, kind,
                coverage_through_seq, context_fingerprint, key_ref, ciphertext,
                eviction_tokens, eviction_estimator_version, created_at
             ) VALUES('pc-fk', 'msg-fk', 1, NULL, 0, 'idem', 'inst', 'protocol',
                     'model', 'kind', NULL, NULL, ?, X'00', 0, 1, 'now')",
        )
        .bind(&key.key_ref)
        .execute(store.pool())
        .await
        .expect("seed provider_context row");

        let error = sqlx::query("DELETE FROM messages WHERE id = 'msg-fk'")
            .execute(store.pool())
            .await
            .expect_err("deleting a referenced message must fail with a foreign key constraint");
        let message = format!("{error:#}");
        assert!(
            message.contains("FOREIGN KEY") || message.contains("787"),
            "{message}"
        );
    }

    fn test_lease(raw: u64) -> ProcessGenerationLease {
        ProcessGenerationLease::new(
            ProcessGeneration::from_wire(raw).expect("valid generation"),
            "test-lease",
        )
        .expect("valid lease")
    }

    fn test_fence(lease: &ProcessGenerationLease) -> GenerationRecoveryFence {
        GenerationRecoveryFence::new(lease, "test-fence").expect("valid fence")
    }

    fn responses_spec() -> ModelSpec {
        ModelSpec::preset("openai-responses").expect("preset")
    }

    fn responses_reasoning_payload(value: &str) -> ProviderContextPayload {
        ProviderContextPayload::EncryptedReasoning {
            protocol: ApiProtocol::OpenAiResponses,
            item: serde_json::json!({
                "type": "reasoning",
                "id": "rs-test",
                "encrypted_content": value,
                "summary": [],
            }),
        }
    }

    async fn seed_persisted_assistant(store: &Store, message_id: &str, seq: u64, spec: &ModelSpec) {
        let transcript_key = store
            .private_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint transcript key");
        let public = PublicMessage::Assistant(PublicAssistantMessage {
            content: vec![],
            model: spec.id.clone(),
            provider: spec.provider.clone(),
            origin: spec.origin(),
            usage: Usage::default(),
            stop_reason: StopReason::Stop,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: Utc::now(),
        });
        let plaintext = serde_json::to_vec(&public).expect("serialize public message");
        let aad = store
            .scope
            .row_aad("messages", message_id, DataKeyPurpose::Transcript);
        let ciphertext = encrypt_content(&transcript_key, &plaintext, &aad)
            .expect("encrypt assistant transcript");
        let projection = store
            .redactor()
            .redact_serialized(&plaintext)
            .expect("redact assistant transcript");
        let search_text =
            super::redactor::search_text_from_projection(&projection).expect("search text");
        sqlx::query(
            "INSERT INTO messages(
                id, seq, role, raw_key_ref, raw_ciphertext, payload, search_text,
                redaction_version, interrupted, created_at
             ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'now')",
        )
        .bind(message_id)
        .bind(i64::try_from(seq).expect("seq in i64"))
        .bind(public_message_role(&public))
        .bind(&transcript_key.key_ref)
        .bind(ciphertext)
        .bind(projection)
        .bind(search_text)
        .bind(i64::from(store.redactor.version()))
        .bind(if message_interrupted(&public) { 1 } else { 0 })
        .execute(store.pool())
        .await
        .expect("insert assistant message");
    }

    async fn insert_provider_context_record(
        store: &Store,
        message_id: &str,
        message_seq: u64,
        saved_tokens: u64,
    ) {
        let spec = responses_spec();
        let retention_owner = ProviderContextAnchor {
            message_id: message_id.to_owned(),
            message_seq,
        };
        let item = ProviderContextItem {
            retention_owner: retention_owner.clone(),
            origin_message: Some(retention_owner),
            wire_item_index: Some(0),
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: responses_reasoning_payload("opaque"),
        };
        let footprint =
            eviction_footprint_for_payload(&spec, &item.payload).expect("canonical footprint");
        let anchor_id = format!("{message_id}:{message_seq}");
        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                personality_agent_id: store.scope().personality_agent_id.clone(),
                anchor_id,
            })
            .await
            .expect("mint provider context key");
        let record = EncryptedProviderContextRecord::encrypt(
            &item,
            &spec.origin().provider_instance_id,
            spec.protocol,
            &spec.id,
            footprint,
            &key,
            &store.scope,
        )
        .expect("encrypt provider context record");
        let id = record.id().to_owned();
        record
            .insert_committed(store)
            .await
            .expect("insert provider context");
        let mut transaction = store.pool().begin().await.expect("begin fixture update");
        let checkpoint =
            provider_context::verify_provider_context_projection_set(store, &mut transaction)
                .await
                .expect("authenticate fixture provider-context set");
        sqlx::query(
            "UPDATE provider_context
             SET eviction_tokens = ?, eviction_estimator_version = ?
             WHERE id = ?",
        )
        .bind(i64::try_from(saved_tokens).expect("saved tokens fit SQLite"))
        .bind(i64::from(EVICTION_ESTIMATOR_VERSION_SERIALIZED_BYTES))
        .bind(id)
        .execute(&mut *transaction)
        .await
        .expect("downgrade fixture to legacy saved footprint");
        provider_context::commit_provider_context_projection_set(
            store,
            &mut transaction,
            &checkpoint,
        )
        .await
        .expect("commit fixture provider-context set");
        transaction.commit().await.expect("commit fixture update");
    }

    #[tokio::test]
    async fn hydration_preserves_saved_eviction_footprint_for_t21_accounting() {
        let store = store().await;
        let message_id = "saved-footprint-msg";
        let saved_tokens = 100_000u64;
        let spec = responses_spec();

        seed_persisted_assistant(&store, message_id, 1, &spec).await;
        insert_provider_context_record(&store, message_id, 1, saved_tokens).await;

        // This fixture isolates provider-context accounting. Its synthetic
        // transcript row intentionally has no durable MessageEnd lifecycle, so
        // exercise the provider hydration boundary directly instead of
        // weakening full Store hydration's exact event/projection parity.
        let mut transaction = store.pool().begin().await.expect("begin hydration");
        let messages = store
            .hydrate_messages(&mut transaction)
            .await
            .expect("hydrate fixture transcript");
        let provider_context = store
            .hydrate_provider_context(&messages, &mut transaction)
            .await
            .expect("hydrate provider context");
        transaction
            .commit()
            .await
            .expect("commit hydration snapshot");

        assert_eq!(provider_context.len(), 1);
        let hydrated = &provider_context[0];
        assert_eq!(hydrated.footprint.eviction_tokens(), saved_tokens);

        // The saved footprint must differ from what the current estimator would
        // recompute for the same payload, proving we are not silently
        // recomputing on cold boot.
        let recomputed = eviction_footprint_for_payload(&spec, &hydrated.item.payload)
            .expect("recompute footprint")
            .eviction_tokens();
        assert_ne!(recomputed, saved_tokens);

        // T21 overflow accounting must use the saved value.  A heavy saved
        // footprint anchored to the assistant forces the assistant to drop,
        // leaving the later user message.
        let assembler = ContextAssembler::from_prompt_with_spec(
            crate::provider::types::PromptContext {
                system_prompt: "System.".to_owned(),
                memory_blocks: vec![],
                messages: vec![],
                provider_context: vec![],
                tools: vec![],
                replay_provenance: None,
            },
            spec.clone(),
        )
        .expect("valid prompt");
        assembler.set_provider_context(provider_context);

        let assistant = ContextMessage::Persisted {
            id: message_id.to_owned(),
            seq: 1,
            message: Message::Assistant(AssistantMessage {
                content: vec![],
                model: spec.id.clone(),
                provider: spec.provider.clone(),
                origin: spec.origin(),
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                provider_code: None,
                interrupted: false,
                timestamp: Utc::now(),
            }),
        };
        let user = ContextMessage::Persisted {
            id: "user-2".to_owned(),
            seq: 2,
            message: Message::User(UserMessage {
                content: vec![UserContent::Text {
                    text: "ack".to_owned(),
                }],
                timestamp: Utc::now(),
            }),
        };
        let recovered = assembler
            .recover_overflow(&[assistant, user])
            .expect("recover overflow");
        assert_eq!(recovered.len(), 1);
        assert!(
            matches!(&recovered[0], ContextMessage::Persisted { id, .. } if id == "user-2"),
            "saved footprint must drive overflow and drop the anchored assistant"
        );
    }

    #[tokio::test]
    async fn hydration_rejects_out_of_range_eviction_estimator_version() {
        let store = store().await;
        let message_id = "range-version-msg";
        let spec = responses_spec();

        seed_persisted_assistant(&store, message_id, 1, &spec).await;
        insert_provider_context_record(&store, message_id, 1, 0).await;
        let mut transaction = store.pool().begin().await.expect("begin fixture update");
        let checkpoint =
            provider_context::verify_provider_context_projection_set(&store, &mut transaction)
                .await
                .expect("authenticate fixture provider-context set");
        sqlx::query(
            "UPDATE provider_context SET eviction_estimator_version = ? WHERE message_id = ?",
        )
        .bind(1_i64 << 40)
        .bind(message_id)
        .execute(&mut *transaction)
        .await
        .expect("tamper estimator version");
        provider_context::commit_provider_context_projection_set(
            &store,
            &mut transaction,
            &checkpoint,
        )
        .await
        .expect("commit validly authenticated out-of-range fixture");
        transaction.commit().await.expect("commit fixture update");

        let mut transaction = store.pool().begin().await.expect("begin hydration");
        let messages = store
            .hydrate_messages(&mut transaction)
            .await
            .expect("hydrate fixture transcript");
        let error = store
            .hydrate_provider_context(&messages, &mut transaction)
            .await
            .expect_err("hydration must fail closed for out-of-range version");
        assert!(
            error
                .to_string()
                .contains("eviction_estimator_version out of u32 range")
        );
    }

    fn test_memory_payload() -> serde_json::Value {
        json!({
            "summary": "Nothing of secret value here.",
            "est_tokens": 42,
            "from": "2024-01-01T00:00:00+00:00",
            "to": "2024-01-02T00:00:00+00:00",
        })
    }

    fn encrypt_memory_projection(
        store: &Store,
        key: &DataKeyMaterial,
        table: &str,
        row_id: &str,
        payload: &serde_json::Value,
    ) -> (Vec<u8>, String, u32) {
        let raw = serde_json::to_vec(payload).expect("serialize memory payload");
        let aad = store
            .scope()
            .row_aad(table, row_id, DataKeyPurpose::MemorySummary);
        let ciphertext = encrypt_content(key, &raw, &aad).expect("encrypt memory payload");
        let projection = store
            .redactor()
            .redact_serialized(&raw)
            .expect("redact memory payload");
        (ciphertext, projection, store.redactor().version())
    }

    async fn authenticate_memory_summary(
        store: &Store,
        key_ref: &str,
        ciphertext: &[u8],
        projection: &str,
        redaction_version: i64,
        table: &str,
        row_id: &str,
    ) -> Result<HydratedMemorySummary> {
        let mut transaction = store.pool().begin().await.expect("begin test transaction");
        store
            .hydrate_memory_summary(
                &mut HashMap::new(),
                &mut transaction,
                key_ref,
                ciphertext,
                projection,
                redaction_version,
                table,
                row_id,
            )
            .await
    }

    fn fixture_compact_result(summary: &str, est_tokens: u64) -> crate::memory::CompactResult {
        let now = Utc::now();
        crate::memory::CompactResult {
            summary: crate::memory::DecryptedMemorySummary::new(summary.to_owned()),
            est_tokens,
            time_range: (now, now),
        }
    }

    async fn insert_authenticated_summary_batch(
        store: &Store,
        id: &str,
        result: crate::memory::CompactResult,
    ) {
        EventWriter::new(std::sync::Arc::new(store.clone()))
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::memory_maintenance("fixture_compacted_l1")
                            .expect("fixture memory-maintenance event"),
                    ),
                    projections: vec![Projection::MemoryTransition(MemoryTransition {
                        batch_inserts: vec![MemoryBatchRecord::new(
                            id,
                            MemoryLayer::L1,
                            0,
                            0,
                            MemoryBatchState::Compacting,
                            0,
                            0,
                        )],
                        ..Default::default()
                    })],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("insert summaryless authenticated memory batch");
        apply_memory_transition_fixture(
            store,
            "fixture_attach_memory_summary",
            MemoryTransition {
                batch_mutations: vec![MemoryBatchMutation {
                    batch_id: Uuid::parse_str(id).expect("fixture batch UUID"),
                    expected_version: 0,
                    new_state: MemoryBatchState::Compacted,
                    est_tokens: result.est_tokens,
                    summary: Some(result),
                    footprint_delta: 0,
                    delete_membership: false,
                }],
                ..Default::default()
            },
        )
        .await;
    }

    async fn apply_memory_transition_fixture(
        store: &Store,
        kind: &str,
        transition: MemoryTransition,
    ) {
        EventWriter::new(std::sync::Arc::new(store.clone()))
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::memory_maintenance(kind)
                            .expect("fixture memory-maintenance event"),
                    ),
                    projections: vec![Projection::MemoryTransition(transition)],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("apply authenticated memory transition fixture");
    }

    async fn apply_compact_l1_fixture(
        store: &Store,
        summary_text: &str,
        est_tokens: u64,
        expected_batch_seq: u64,
        initialize_cursor: bool,
    ) {
        let source_id = Uuid::now_v7();
        let target_id = Uuid::now_v7();
        let source_key = source_id.to_string();
        let target_key = target_id.to_string();
        let job_id = Uuid::now_v7().to_string();
        let source = MemoryBatchRecord::new(
            source_key.clone(),
            MemoryLayer::L1,
            0,
            0,
            MemoryBatchState::Compacting,
            0,
            0,
        );
        apply_memory_transition_fixture(
            store,
            "fixture_compact_l1_source",
            MemoryTransition {
                batch_inserts: vec![source],
                ..Default::default()
            },
        )
        .await;
        let source_result = fixture_compact_result("Nothing of secret value here.", 42);
        apply_memory_transition_fixture(
            store,
            "fixture_compact_l1_source_summary",
            MemoryTransition {
                batch_mutations: vec![MemoryBatchMutation {
                    batch_id: source_id,
                    expected_version: 0,
                    new_state: MemoryBatchState::Compacted,
                    summary: Some(source_result),
                    est_tokens: 42,
                    footprint_delta: 0,
                    delete_membership: false,
                }],
                ..Default::default()
            },
        )
        .await;
        apply_memory_transition_fixture(
            store,
            "fixture_reopen_compact_l1_source",
            MemoryTransition {
                batch_mutations: vec![MemoryBatchMutation {
                    batch_id: source_id,
                    expected_version: 1,
                    new_state: MemoryBatchState::Compacting,
                    summary: None,
                    est_tokens: 42,
                    footprint_delta: 0,
                    delete_membership: false,
                }],
                ..Default::default()
            },
        )
        .await;
        let target = MemoryBatchRecord::new(
            target_key.clone(),
            MemoryLayer::L2,
            0,
            0,
            MemoryBatchState::Compacting,
            0,
            0,
        );
        apply_memory_transition_fixture(
            store,
            "fixture_compact_l1_graph",
            MemoryTransition {
                batch_inserts: vec![target],
                job_inserts: vec![MemoryJobRecord::new(
                    job_id.clone(),
                    MemoryJobKind::CompactL1,
                    0,
                    vec![source_key.clone()],
                    BTreeMap::from([(source_key, 2), (target_key, 0)]),
                )],
                cursor_advance: initialize_cursor.then_some(MemoryApplyCursorAdvance {
                    kind: MemoryJobKind::CompactL1.as_str().to_owned(),
                    expected: expected_batch_seq,
                    next: expected_batch_seq,
                    initialize: true,
                }),
                ..Default::default()
            },
        )
        .await;
        let batch_seq: i64 = sqlx::query_scalar("SELECT batch_seq FROM memory_jobs WHERE id = ?")
            .bind(&job_id)
            .fetch_one(store.pool())
            .await
            .expect("load inserted CompactL1 sequence");
        assert_eq!(
            u64::try_from(batch_seq).expect("fixture sequence is non-negative"),
            expected_batch_seq
        );

        let lease_until = format!("2099-01-01T00:00:{expected_batch_seq:02}Z");
        apply_memory_transition_fixture(
            store,
            "fixture_compact_l1_claim",
            MemoryTransition {
                expected_source_versions: BTreeMap::from([(source_id, 2), (target_id, 0)]),
                job_mutations: vec![MemoryJobMutation::Claim {
                    job_id: job_id.clone(),
                    lease_until: lease_until.clone(),
                }],
                ..Default::default()
            },
        )
        .await;
        apply_memory_transition_fixture(
            store,
            "fixture_compact_l1_start",
            MemoryTransition {
                expected_source_versions: BTreeMap::from([(source_id, 2), (target_id, 0)]),
                job_mutations: vec![MemoryJobMutation::Start {
                    job_id: job_id.clone(),
                    expected_attempt: 0,
                    lease_witness: Some(lease_until.clone()),
                    lease_until: lease_until.clone(),
                }],
                ..Default::default()
            },
        )
        .await;
        let now = Utc::now();
        let result = crate::memory::CompactResult {
            summary: crate::memory::DecryptedMemorySummary::new(summary_text.to_owned()),
            est_tokens,
            time_range: (now, now),
        };
        apply_memory_transition_fixture(
            store,
            "fixture_compact_l1_complete",
            MemoryTransition {
                expected_source_versions: BTreeMap::from([(source_id, 2), (target_id, 0)]),
                batch_mutations: vec![
                    MemoryBatchMutation {
                        batch_id: source_id,
                        expected_version: 2,
                        new_state: MemoryBatchState::Compacted,
                        summary: None,
                        est_tokens: 42,
                        footprint_delta: 0,
                        delete_membership: false,
                    },
                    MemoryBatchMutation {
                        batch_id: target_id,
                        expected_version: 0,
                        new_state: MemoryBatchState::Compacted,
                        summary: Some(result.clone()),
                        est_tokens,
                        footprint_delta: 0,
                        delete_membership: false,
                    },
                ],
                job_mutations: vec![MemoryJobMutation::Complete {
                    job_id: job_id.clone(),
                    expected_attempt: 1,
                    lease_witness: Some(lease_until),
                    result,
                }],
                ..Default::default()
            },
        )
        .await;
        apply_memory_transition_fixture(
            store,
            "fixture_compact_l1_apply",
            MemoryTransition {
                expected_source_versions: BTreeMap::from([(source_id, 3), (target_id, 1)]),
                batch_mutations: vec![
                    MemoryBatchMutation {
                        batch_id: source_id,
                        expected_version: 3,
                        new_state: MemoryBatchState::Dropped,
                        summary: None,
                        est_tokens: 42,
                        footprint_delta: 0,
                        delete_membership: false,
                    },
                    MemoryBatchMutation {
                        batch_id: target_id,
                        expected_version: 1,
                        new_state: MemoryBatchState::Promoted,
                        summary: None,
                        est_tokens,
                        footprint_delta: 0,
                        delete_membership: false,
                    },
                ],
                job_mutations: vec![MemoryJobMutation::Apply {
                    job_id,
                    expected_attempt: 1,
                    lease_witness: None,
                }],
                cursor_advance: Some(MemoryApplyCursorAdvance {
                    kind: MemoryJobKind::CompactL1.as_str().to_owned(),
                    expected: expected_batch_seq,
                    next: expected_batch_seq + 1,
                    initialize: false,
                }),
                ..Default::default()
            },
        )
        .await;
    }

    #[tokio::test]
    async fn memory_batch_summary_hydrates_successfully() {
        let store = store().await;
        let key = store
            .memory_summary_key("batch", "batch-ok")
            .await
            .expect("mint memory summary key");
        let payload = test_memory_payload();
        let (ciphertext, projection, version) =
            encrypt_memory_projection(&store, &key, "memory_batches", "batch-ok", &payload);
        let summary = authenticate_memory_summary(
            &store,
            &key.key_ref,
            &ciphertext,
            &projection,
            i64::from(version),
            "memory_batches",
            "batch-ok",
        )
        .await
        .expect("authenticate memory batch summary");
        assert_eq!(summary.test_plaintext(), "Nothing of secret value here.");
    }

    #[tokio::test]
    async fn memory_batch_summary_rejects_tampered_ciphertext() {
        let store = store().await;
        let key = store
            .memory_summary_key("batch", "batch-tamper")
            .await
            .expect("mint memory summary key");
        let payload = test_memory_payload();
        let (mut ciphertext, projection, version) =
            encrypt_memory_projection(&store, &key, "memory_batches", "batch-tamper", &payload);
        ciphertext[0] ^= 0xff;
        let error = authenticate_memory_summary(
            &store,
            &key.key_ref,
            &ciphertext,
            &projection,
            i64::from(version),
            "memory_batches",
            "batch-tamper",
        )
        .await
        .err()
        .expect("tampered ciphertext must fail hydration");
        let message = format!("{error:#}");
        assert!(
            message.contains("failed to decrypt memory_batches projection"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn memory_batch_summary_rejects_wrong_key_ref() {
        let store = store().await;
        let memory_key = store
            .memory_summary_key("batch", "batch-wrong-key")
            .await
            .expect("mint memory summary key");
        let transcript_key = store
            .private_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint transcript key");
        let payload = test_memory_payload();
        let (ciphertext, projection, version) = encrypt_memory_projection(
            &store,
            &memory_key,
            "memory_batches",
            "batch-wrong-key",
            &payload,
        );
        let error = authenticate_memory_summary(
            &store,
            &transcript_key.key_ref,
            &ciphertext,
            &projection,
            i64::from(version),
            "memory_batches",
            "batch-wrong-key",
        )
        .await
        .err()
        .expect("wrong key reference must fail hydration");
        let message = format!("{error:#}");
        assert!(
            message.contains("has purpose transcript")
                && message.contains("expected memory_summary"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn memory_batch_summary_rejects_mismatched_projection() {
        let store = store().await;
        let key = store
            .memory_summary_key("batch", "batch-bad-projection")
            .await
            .expect("mint memory summary key");
        let payload = test_memory_payload();
        let (ciphertext, mut projection, version) = encrypt_memory_projection(
            &store,
            &key,
            "memory_batches",
            "batch-bad-projection",
            &payload,
        );
        projection.push_str(" tampered");
        let error = authenticate_memory_summary(
            &store,
            &key.key_ref,
            &ciphertext,
            &projection,
            i64::from(version),
            "memory_batches",
            "batch-bad-projection",
        )
        .await
        .err()
        .expect("mismatched projection must fail hydration");
        let message = format!("{error:#}");
        assert!(
            message.contains("does not match re-derived redacted plaintext"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn memory_batch_summary_rejects_stale_redaction_version() {
        let store = store().await;
        let key = store
            .memory_summary_key("batch", "batch-stale")
            .await
            .expect("mint memory summary key");
        let payload = test_memory_payload();
        let (ciphertext, projection, _version) =
            encrypt_memory_projection(&store, &key, "memory_batches", "batch-stale", &payload);
        let error = authenticate_memory_summary(
            &store,
            &key.key_ref,
            &ciphertext,
            &projection,
            2,
            "memory_batches",
            "batch-stale",
        )
        .await
        .err()
        .expect("stale redaction version must fail hydration");
        let message = format!("{error:#}");
        assert!(
            message.contains("unsupported redaction version"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn memory_job_result_hydrates_successfully() {
        let store = store().await;
        let key = store
            .memory_summary_key("job", "job-ok")
            .await
            .expect("mint memory summary key");
        let payload = test_memory_payload();
        let (ciphertext, projection, version) =
            encrypt_memory_projection(&store, &key, "memory_jobs", "job-ok", &payload);
        let summary = authenticate_memory_summary(
            &store,
            &key.key_ref,
            &ciphertext,
            &projection,
            i64::from(version),
            "memory_jobs",
            "job-ok",
        )
        .await
        .expect("authenticate memory job result");
        assert_eq!(summary.test_plaintext(), "Nothing of secret value here.");
    }

    #[tokio::test]
    async fn memory_job_result_rejects_tampered_ciphertext() {
        let store = store().await;
        let key = store
            .memory_summary_key("job", "job-tamper")
            .await
            .expect("mint memory summary key");
        let payload = test_memory_payload();
        let (mut ciphertext, projection, version) =
            encrypt_memory_projection(&store, &key, "memory_jobs", "job-tamper", &payload);
        ciphertext[0] ^= 0xff;
        let error = authenticate_memory_summary(
            &store,
            &key.key_ref,
            &ciphertext,
            &projection,
            i64::from(version),
            "memory_jobs",
            "job-tamper",
        )
        .await
        .err()
        .expect("tampered ciphertext must fail hydration");
        let message = format!("{error:#}");
        assert!(
            message.contains("failed to decrypt memory_jobs projection"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn memory_job_result_rejects_wrong_key_ref() {
        let store = store().await;
        let memory_key = store
            .memory_summary_key("job", "job-wrong-key")
            .await
            .expect("mint memory summary key");
        let transcript_key = store
            .private_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint transcript key");
        let payload = test_memory_payload();
        let (ciphertext, projection, version) = encrypt_memory_projection(
            &store,
            &memory_key,
            "memory_jobs",
            "job-wrong-key",
            &payload,
        );
        let error = authenticate_memory_summary(
            &store,
            &transcript_key.key_ref,
            &ciphertext,
            &projection,
            i64::from(version),
            "memory_jobs",
            "job-wrong-key",
        )
        .await
        .err()
        .expect("wrong key reference must fail hydration");
        let message = format!("{error:#}");
        assert!(
            message.contains("has purpose transcript")
                && message.contains("expected memory_summary"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn memory_job_result_rejects_mismatched_projection() {
        let store = store().await;
        let key = store
            .memory_summary_key("job", "job-bad-projection")
            .await
            .expect("mint memory summary key");
        let payload = test_memory_payload();
        let (ciphertext, mut projection, version) =
            encrypt_memory_projection(&store, &key, "memory_jobs", "job-bad-projection", &payload);
        projection.push_str(" tampered");
        let error = authenticate_memory_summary(
            &store,
            &key.key_ref,
            &ciphertext,
            &projection,
            i64::from(version),
            "memory_jobs",
            "job-bad-projection",
        )
        .await
        .err()
        .expect("mismatched projection must fail hydration");
        let message = format!("{error:#}");
        assert!(
            message.contains("does not match re-derived redacted plaintext"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn memory_job_result_rejects_stale_redaction_version() {
        let store = store().await;
        let key = store
            .memory_summary_key("job", "job-stale")
            .await
            .expect("mint memory summary key");
        let payload = test_memory_payload();
        let (ciphertext, projection, _version) =
            encrypt_memory_projection(&store, &key, "memory_jobs", "job-stale", &payload);
        let error = authenticate_memory_summary(
            &store,
            &key.key_ref,
            &ciphertext,
            &projection,
            2,
            "memory_jobs",
            "job-stale",
        )
        .await
        .err()
        .expect("stale redaction version must fail hydration");
        let message = format!("{error:#}");
        assert!(
            message.contains("unsupported redaction version"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_structurally_invalid_authentic_memory_graph() {
        let store = store().await;
        let batch_id = Uuid::now_v7().to_string();
        insert_authenticated_summary_batch(
            &store,
            &batch_id,
            fixture_compact_result("Nothing of secret value here.", 42),
        )
        .await;

        let error = store
            .hydrate(&test_lease(1), &test_fence(&test_lease(1)))
            .await
            .expect_err("authenticated summary without a compaction owner must fail closed");
        let message = format!("{error:#}");
        assert!(
            message.contains("hydrated memory graph is structurally invalid")
                && message.contains("has no matching compaction job"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn store_hydration_round_trips_applied_memory_into_runtime_layers() {
        let store = store().await;
        let source_id = Uuid::now_v7();
        let target_id = Uuid::now_v7();
        let job_id = Uuid::now_v7();
        let source_key = source_id.to_string();
        let target_key = target_id.to_string();
        let job_key = job_id.to_string();
        let initial_source_versions =
            BTreeMap::from([(source_key.clone(), 0_i64), (target_key.clone(), 0_i64)]);

        apply_memory_transition_fixture(
            &store,
            "fixture_compaction_graph",
            MemoryTransition {
                batch_inserts: vec![
                    MemoryBatchRecord::new(
                        source_key.clone(),
                        MemoryLayer::L0,
                        0,
                        0,
                        MemoryBatchState::Compacting,
                        0,
                        0,
                    ),
                    MemoryBatchRecord::new(
                        target_key.clone(),
                        MemoryLayer::L1,
                        0,
                        0,
                        MemoryBatchState::Compacting,
                        0,
                        0,
                    ),
                ],
                job_inserts: vec![MemoryJobRecord::new(
                    job_key.clone(),
                    MemoryJobKind::CompactL0,
                    0,
                    vec![source_key.clone()],
                    initial_source_versions,
                )],
                cursor_advance: Some(MemoryApplyCursorAdvance {
                    kind: MemoryJobKind::CompactL0.as_str().to_owned(),
                    expected: 0,
                    next: 1,
                    initialize: true,
                }),
                ..Default::default()
            },
        )
        .await;

        let lease_until = "2099-01-01T00:00:00Z".to_owned();
        apply_memory_transition_fixture(
            &store,
            "fixture_compaction_claim",
            MemoryTransition {
                expected_source_versions: BTreeMap::from([(source_id, 0), (target_id, 0)]),
                job_mutations: vec![MemoryJobMutation::Claim {
                    job_id: job_key.clone(),
                    lease_until: lease_until.clone(),
                }],
                ..Default::default()
            },
        )
        .await;
        apply_memory_transition_fixture(
            &store,
            "fixture_compaction_start",
            MemoryTransition {
                expected_source_versions: BTreeMap::from([(source_id, 0), (target_id, 0)]),
                job_mutations: vec![MemoryJobMutation::Start {
                    job_id: job_key.clone(),
                    expected_attempt: 0,
                    lease_witness: Some(lease_until.clone()),
                    lease_until: lease_until.clone(),
                }],
                ..Default::default()
            },
        )
        .await;

        let now = Utc::now();
        let result = crate::memory::CompactResult {
            summary: crate::memory::DecryptedMemorySummary::new(
                "Nothing of secret value here.".to_owned(),
            ),
            est_tokens: 42,
            time_range: (now, now),
        };
        apply_memory_transition_fixture(
            &store,
            "fixture_compaction_complete",
            MemoryTransition {
                expected_source_versions: BTreeMap::from([(source_id, 0), (target_id, 0)]),
                batch_mutations: vec![
                    MemoryBatchMutation {
                        batch_id: source_id,
                        expected_version: 0,
                        new_state: MemoryBatchState::Compacted,
                        summary: None,
                        est_tokens: 0,
                        footprint_delta: 0,
                        delete_membership: false,
                    },
                    MemoryBatchMutation {
                        batch_id: target_id,
                        expected_version: 0,
                        new_state: MemoryBatchState::Compacted,
                        summary: Some(result.clone()),
                        est_tokens: result.est_tokens,
                        footprint_delta: 0,
                        delete_membership: false,
                    },
                ],
                job_mutations: vec![MemoryJobMutation::Complete {
                    job_id: job_key.clone(),
                    expected_attempt: 1,
                    lease_witness: Some(lease_until),
                    result,
                }],
                ..Default::default()
            },
        )
        .await;
        apply_memory_transition_fixture(
            &store,
            "fixture_compaction_apply",
            MemoryTransition {
                expected_source_versions: BTreeMap::from([(source_id, 1), (target_id, 1)]),
                batch_mutations: vec![
                    MemoryBatchMutation {
                        batch_id: source_id,
                        expected_version: 1,
                        new_state: MemoryBatchState::Dropped,
                        summary: None,
                        est_tokens: 0,
                        footprint_delta: 0,
                        delete_membership: true,
                    },
                    MemoryBatchMutation {
                        batch_id: target_id,
                        expected_version: 1,
                        new_state: MemoryBatchState::Promoted,
                        summary: None,
                        est_tokens: 42,
                        footprint_delta: 0,
                        delete_membership: false,
                    },
                ],
                job_mutations: vec![MemoryJobMutation::Apply {
                    job_id: job_key,
                    expected_attempt: 1,
                    lease_witness: None,
                }],
                cursor_advance: Some(MemoryApplyCursorAdvance {
                    kind: MemoryJobKind::CompactL0.as_str().to_owned(),
                    expected: 1,
                    next: 2,
                    initialize: false,
                }),
                ..Default::default()
            },
        )
        .await;

        let lease = test_lease(1);
        let fence = test_fence(&lease);
        let HydrationOutcome::Complete(state) = store
            .hydrate(&lease, &fence)
            .await
            .expect("hydrate typed memory handoff")
        else {
            panic!("applied memory must not require physical recovery");
        };
        let memory = crate::memory::ThreeLayerMemory::from_hydrated(state.memory)
            .expect("typed Store handoff reconstructs exact runtime layers");
        assert!(memory.l0().is_empty());
        assert_eq!(memory.l1().len(), 1);
        assert_eq!(memory.l1()[0].source_batch, source_id);
        assert_eq!(
            memory.l1()[0].summary.expose(),
            "Nothing of secret value here."
        );
        assert_eq!(memory.l2().summary.expose(), "");
    }

    #[tokio::test]
    async fn store_hydration_restores_completed_compact_l0_result_to_speculative_shelf() {
        let store = store().await;
        let user = insert_user_message(&store, 1, "remember this before apply").await;
        complete_user_message_fixture(&store, &user).await;
        let source = sqlx::query(
            "SELECT id, est_tokens FROM memory_batches
             WHERE layer = 0 AND state = 'open'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load canonical MessageEnd L0 source");
        let source_key: String = source.get("id");
        let source_id = Uuid::parse_str(&source_key).expect("canonical L0 batch UUID");
        let source_est_tokens = u64::try_from(source.get::<i64, _>("est_tokens"))
            .expect("fixture source estimate is non-negative");
        let target_id = Uuid::now_v7();
        let target_key = target_id.to_string();
        let job_id = Uuid::now_v7().to_string();

        apply_memory_transition_fixture(
            &store,
            "fixture_completed_shelf_graph",
            MemoryTransition {
                batch_inserts: vec![MemoryBatchRecord::new(
                    target_key.clone(),
                    MemoryLayer::L1,
                    0,
                    0,
                    MemoryBatchState::Compacting,
                    0,
                    0,
                )],
                batch_mutations: vec![MemoryBatchMutation {
                    batch_id: source_id,
                    expected_version: 0,
                    new_state: MemoryBatchState::Compacting,
                    summary: None,
                    est_tokens: source_est_tokens,
                    footprint_delta: 0,
                    delete_membership: false,
                }],
                job_inserts: vec![MemoryJobRecord::new(
                    job_id.clone(),
                    MemoryJobKind::CompactL0,
                    0,
                    vec![source_key.clone()],
                    BTreeMap::from([(source_key, 1), (target_key, 0)]),
                )],
                ..Default::default()
            },
        )
        .await;

        let lease_until = "2099-01-01T00:00:00Z".to_owned();
        for (kind, mutation) in [
            (
                "fixture_completed_shelf_claim",
                MemoryJobMutation::Claim {
                    job_id: job_id.clone(),
                    lease_until: lease_until.clone(),
                },
            ),
            (
                "fixture_completed_shelf_start",
                MemoryJobMutation::Start {
                    job_id: job_id.clone(),
                    expected_attempt: 0,
                    lease_witness: Some(lease_until.clone()),
                    lease_until: lease_until.clone(),
                },
            ),
        ] {
            apply_memory_transition_fixture(
                &store,
                kind,
                MemoryTransition {
                    expected_source_versions: BTreeMap::from([(source_id, 1), (target_id, 0)]),
                    job_mutations: vec![mutation],
                    ..Default::default()
                },
            )
            .await;
        }

        let result = fixture_compact_result("completed L1 shelf", 7);
        apply_memory_transition_fixture(
            &store,
            "fixture_completed_shelf_complete",
            MemoryTransition {
                expected_source_versions: BTreeMap::from([(source_id, 1), (target_id, 0)]),
                batch_mutations: vec![
                    MemoryBatchMutation {
                        batch_id: source_id,
                        expected_version: 1,
                        new_state: MemoryBatchState::Compacted,
                        summary: None,
                        est_tokens: source_est_tokens,
                        footprint_delta: 0,
                        delete_membership: false,
                    },
                    MemoryBatchMutation {
                        batch_id: target_id,
                        expected_version: 0,
                        new_state: MemoryBatchState::Compacted,
                        summary: Some(result.clone()),
                        est_tokens: result.est_tokens,
                        footprint_delta: 0,
                        delete_membership: false,
                    },
                ],
                job_mutations: vec![MemoryJobMutation::Complete {
                    job_id: job_id.clone(),
                    expected_attempt: 1,
                    lease_witness: Some(lease_until),
                    result,
                }],
                ..Default::default()
            },
        )
        .await;

        let lease = test_lease(1);
        let fence = test_fence(&lease);
        let HydrationOutcome::Complete(state) = store
            .hydrate(&lease, &fence)
            .await
            .expect("hydrate completed speculative shelf")
        else {
            panic!("completed CompactL0 shelf must hydrate without physical recovery");
        };
        let memory = crate::memory::ThreeLayerMemory::from_hydrated(state.memory)
            .expect("completed shelf graph reconstructs");
        let shelf = memory
            .shelf()
            .get(&source_id)
            .expect("completed CompactL0 result is restored to its source shelf");
        assert_eq!(shelf.summary.expose(), "completed L1 shelf");
        assert_eq!(shelf.est_tokens, 7);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT status FROM memory_jobs WHERE id = ?")
                .bind(job_id)
                .fetch_one(store.pool())
                .await
                .expect("load completed shelf job"),
            "completed"
        );
    }

    #[tokio::test]
    async fn store_hydration_folds_multiple_authenticated_l2_rows_in_order() {
        let store = store().await;
        apply_compact_l1_fixture(&store, "first durable L2", 3, 1, true).await;
        apply_compact_l1_fixture(&store, "second durable L2", 5, 2, false).await;

        let lease = test_lease(1);
        let fence = test_fence(&lease);
        let HydrationOutcome::Complete(state) = store
            .hydrate(&lease, &fence)
            .await
            .expect("hydrate repeated authenticated L2 applies")
        else {
            panic!("fully applied L2 graph must hydrate completely");
        };
        let memory = crate::memory::ThreeLayerMemory::from_hydrated(state.memory)
            .expect("multi-L2 Store handoff reconstructs");
        assert!(memory.l0().is_empty());
        assert!(memory.l1().is_empty());
        assert_eq!(
            memory.l2().summary.expose(),
            "first durable L2\n\nsecond durable L2"
        );
        assert_eq!(memory.l2().est_tokens, 8);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM memory_batches
                 WHERE layer = 2 AND state = 'promoted'",
            )
            .fetch_one(store.pool())
            .await
            .expect("count promoted L2 rows"),
            2
        );
    }

    #[tokio::test]
    async fn hydrate_repairs_expired_memory_job_before_returning_complete_state() {
        let store = store().await;
        let message_text = "recover me";
        let user = insert_user_message(&store, 1, message_text).await;
        complete_user_message_fixture(&store, &user).await;
        let source = sqlx::query(
            "SELECT id, est_tokens FROM memory_batches
             WHERE layer = 0 AND state = 'open'",
        )
        .fetch_one(store.pool())
        .await
        .expect("load canonical MessageEnd L0 source");
        let source_key: String = source.get("id");
        let source_id = Uuid::parse_str(&source_key).expect("canonical L0 batch UUID");
        let est_tokens: i64 = source.get("est_tokens");
        let target_id = Uuid::now_v7();
        let job_id = Uuid::now_v7().to_string();
        let target_key = target_id.to_string();

        apply_memory_transition_fixture(
            &store,
            "fixture_expired_job_graph",
            MemoryTransition {
                batch_inserts: vec![MemoryBatchRecord::new(
                    target_key.clone(),
                    MemoryLayer::L1,
                    0,
                    0,
                    MemoryBatchState::Compacting,
                    0,
                    0,
                )],
                batch_mutations: vec![MemoryBatchMutation {
                    batch_id: source_id,
                    expected_version: 0,
                    new_state: MemoryBatchState::Compacting,
                    summary: None,
                    est_tokens: u64::try_from(est_tokens)
                        .expect("fixture source estimate is non-negative"),
                    footprint_delta: 0,
                    delete_membership: false,
                }],
                job_inserts: vec![MemoryJobRecord::new(
                    job_id.clone(),
                    MemoryJobKind::CompactL0,
                    0,
                    vec![source_key.clone()],
                    BTreeMap::from([(source_key, 1), (target_key, 0)]),
                )],
                ..Default::default()
            },
        )
        .await;
        let expired_lease = "2000-01-01T00:00:00Z".to_owned();
        apply_memory_transition_fixture(
            &store,
            "fixture_expired_job_claim",
            MemoryTransition {
                expected_source_versions: BTreeMap::from([(source_id, 1), (target_id, 0)]),
                job_mutations: vec![MemoryJobMutation::Claim {
                    job_id: job_id.clone(),
                    lease_until: expired_lease.clone(),
                }],
                ..Default::default()
            },
        )
        .await;
        apply_memory_transition_fixture(
            &store,
            "fixture_expired_job_start",
            MemoryTransition {
                expected_source_versions: BTreeMap::from([(source_id, 1), (target_id, 0)]),
                job_mutations: vec![MemoryJobMutation::Start {
                    job_id: job_id.clone(),
                    expected_attempt: 0,
                    lease_witness: Some(expired_lease.clone()),
                    lease_until: expired_lease,
                }],
                ..Default::default()
            },
        )
        .await;

        let lease = test_lease(1);
        let fence = test_fence(&lease);
        let HydrationOutcome::Complete(state) = store
            .hydrate(&lease, &fence)
            .await
            .expect("hydration repairs recoverable memory state")
        else {
            panic!("expired memory maintenance must reach a complete fixed point");
        };
        let row = sqlx::query("SELECT status, attempts, lease_until FROM memory_jobs WHERE id = ?")
            .bind(&job_id)
            .fetch_one(store.pool())
            .await
            .expect("load recovered memory job");
        assert_eq!(row.get::<String, _>("status"), "pending");
        assert_eq!(row.get::<i64, _>("attempts"), 1);
        assert!(row.get::<Option<String>, _>("lease_until").is_none());
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM agent_events
                 WHERE event_type = 'memory_maintenance'
                   AND json_extract(envelope, '$.kind') = 'compact_expired_lease_recovered'",
            )
            .fetch_one(store.pool())
            .await
            .expect("count durable recovery event"),
            1
        );

        let memory = crate::memory::ThreeLayerMemory::from_hydrated(state.memory)
            .expect("repaired memory graph reconstructs");
        assert_eq!(memory.l0().len(), 1);
        assert_eq!(memory.l0()[0].state, crate::memory::BatchState::Compacting);
    }

    #[tokio::test]
    async fn hydrate_loads_transcript_and_memory_in_one_snapshot() {
        let store = store().await;
        let user = insert_user_message(&store, 1, "hello snapshot").await;
        complete_user_message_fixture(&store, &user).await;

        let outcome = store
            .hydrate(&test_lease(1), &test_fence(&test_lease(1)))
            .await
            .expect("hydrate must return a complete state from one snapshot");
        let HydrationOutcome::Complete(state) = outcome else {
            panic!("expected complete hydration outcome");
        };

        assert_eq!(state.messages.len(), 2);
        match &state.messages[0] {
            ContextMessage::Persisted { id, .. } => assert_eq!(id, &user.message_id),
            _ => panic!("expected persisted message"),
        }
        assert!(!state.memory.is_empty());
        assert!(state.provider_context.is_empty());
        assert_eq!(state.resume, ResumeDirective::AdmitCommands);
    }

    #[tokio::test]
    async fn hydrate_with_pending_logical_suffix_exposes_no_ready_receipt() {
        let store = store().await;
        let command_id = CommandId::parse(&Uuid::now_v7().hyphenated().to_string())
            .expect("canonical command UUID");
        EventWriter::new(Arc::new(store.clone()))
            .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                seq: 1,
                command_id: command_id.clone(),
                personality_agent_id: scope().personality_agent_id,
                provenance: direct_chat_provenance(),
                command: Command::UserMessage {
                    text: "pending logical suffix".to_owned(),
                    attachments: Vec::new(),
                },
            }))
            .await
            .expect("persist real pending command");

        let lease = test_lease(1);
        let fence = test_fence(&lease);
        match store
            .hydrate(&lease, &fence)
            .await
            .expect("hydrate pending logical suffix")
        {
            HydrationOutcome::LogicalRecoveryRequired { steps } => {
                assert!(matches!(
                    steps.as_slice(),
                    [RecoveryStep::Reclassify { command_id: recovered_id }]
                        if recovered_id == command_id.as_str()
                ));
            }
            HydrationOutcome::Complete(_) => {
                panic!("a pending logical suffix must not expose a ready receipt")
            }
            HydrationOutcome::PhysicalRecoveryRequired(intents) => {
                panic!("fixture has no running execution: {intents:?}")
            }
        }
    }

    #[tokio::test]
    async fn hydrate_messages_pages_across_multiple_pages() {
        let store = store().await;
        const MESSAGE_COUNT: usize = 70;
        for i in 0..MESSAGE_COUNT {
            insert_raw_user_message(
                &store,
                &format!("msg-page-{i}"),
                (i + 1) as u64,
                &format!("hello page {i}"),
            )
            .await;
        }

        let messages = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store
                .hydrate_messages(&mut transaction)
                .await
                .expect("hydrate messages across pages")
        };

        assert_eq!(messages.len(), MESSAGE_COUNT);
        for (i, message) in messages.iter().enumerate() {
            let expected_seq = (i + 1) as u64;
            match message {
                ContextMessage::Persisted { id, seq, .. } => {
                    assert_eq!(*seq, expected_seq);
                    assert_eq!(id, &format!("msg-page-{i}"));
                }
                _ => panic!("expected persisted message"),
            }
        }
    }

    #[tokio::test]
    async fn hydrate_rejects_tampered_memory_batch() {
        let store = store().await;
        let batch_id = Uuid::now_v7().to_string();
        insert_authenticated_summary_batch(
            &store,
            &batch_id,
            fixture_compact_result("Nothing of secret value here.", 42),
        )
        .await;
        let mut ciphertext: Vec<u8> =
            sqlx::query_scalar("SELECT summary_ciphertext FROM memory_batches WHERE id = ?")
                .bind(&batch_id)
                .fetch_one(store.pool())
                .await
                .expect("load authenticated memory ciphertext");
        ciphertext[0] ^= 0xff;
        sqlx::query("UPDATE memory_batches SET summary_ciphertext = ? WHERE id = ?")
            .bind(ciphertext)
            .bind(&batch_id)
            .execute(store.pool())
            .await
            .expect("tamper memory ciphertext");

        let error = store
            .hydrate(&test_lease(1), &test_fence(&test_lease(1)))
            .await
            .expect_err("tampered memory batch must fail hydrate");
        let message = format!("{error:#}");
        assert!(
            message.contains("authenticated memory projection digest mismatch"),
            "{message}"
        );
    }

    struct CanonicalUserFixture {
        seq: u64,
        command_id: String,
        run_id: String,
        turn_id: String,
        message_id: String,
    }

    async fn insert_user_message(store: &Store, seq: u64, text: &str) -> CanonicalUserFixture {
        let command_id = Uuid::now_v7().hyphenated().to_string();
        let command_id = CommandId::parse(&command_id).expect("canonical command UUID");
        let writer = EventWriter::new(Arc::new(store.clone()));
        writer
            .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                seq,
                command_id: command_id.clone(),
                personality_agent_id: scope().personality_agent_id,
                provenance: direct_chat_provenance(),
                command: Command::UserMessage {
                    text: text.to_owned(),
                    attachments: Vec::new(),
                },
            }))
            .await
            .expect("persist fixture command");
        let run_id = format!("run-{}", command_id.as_str());
        let turn_id = format!("turn-{}", command_id.as_str());
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: command_id.as_str().to_owned(),
                        application_kind: ApplicationKind::IdleRun,
                        run_id: run_id.clone(),
                        turn_id: turn_id.clone(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("classify fixture command");
        let received_at: String =
            sqlx::query_scalar("SELECT received_at FROM inbound_commands WHERE command_id = ?")
                .bind(command_id.as_str())
                .fetch_one(store.pool())
                .await
                .expect("load canonical fixture receipt time");
        let timestamp = DateTime::parse_from_rfc3339(&received_at)
            .expect("fixture receipt time is RFC3339")
            .with_timezone(&Utc);
        let message_id = user_message_id(store.scope().personality_agent_id(), &command_id);
        let message = PublicMessage::User(UserMessage {
            content: vec![UserContent::Text {
                text: text.to_owned(),
            }],
            timestamp,
        });
        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type": "agent_start",
                                "run_id": run_id.clone(),
                            }))
                            .expect("fixture AgentStart"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: command_id.as_str().to_owned(),
                            run_id: run_id.clone(),
                            expected: RunPhase::Classified,
                            next: RunPhase::RunStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::new(&json!({
                                "type": "turn_start",
                                "run_id": run_id.clone(),
                                "turn_id": turn_id.clone(),
                            }))
                            .expect("fixture TurnStart"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: command_id.as_str().to_owned(),
                            run_id: run_id.clone(),
                            expected: RunPhase::RunStarted,
                            next: RunPhase::TurnStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_start", &message_id, &message)
                                .expect("fixture MessageStart"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: command_id.as_str().to_owned(),
                            run_id: run_id.clone(),
                            expected: RunPhase::TurnStarted,
                            next: RunPhase::UserStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_end", &message_id, &message)
                                .expect("fixture MessageEnd"),
                        ),
                        projections: vec![
                            Projection::MessageEnd {
                                message_id: message_id.clone(),
                                role: "user",
                                message,
                                append_to_l0: true,
                                provider_context: Vec::new(),
                                eviction_footprint_tokens: 0,
                            },
                            Projection::RunPhase {
                                command_id: command_id.as_str().to_owned(),
                                run_id: run_id.clone(),
                                expected: RunPhase::UserStarted,
                                next: RunPhase::UserCommitted,
                            },
                        ],
                    },
                ],
                injected_commands: vec![InjectedCommand::new(
                    seq,
                    command_id.clone(),
                    direct_chat_provenance(),
                )],
            })
            .await
            .expect("commit canonical fixture MessageEnd");
        CanonicalUserFixture {
            seq,
            command_id: command_id.as_str().to_owned(),
            run_id,
            turn_id,
            message_id,
        }
    }

    async fn complete_user_message_fixture(store: &Store, user: &CanonicalUserFixture) {
        let writer = EventWriter::new(Arc::new(store.clone()));
        let message_id = Uuid::now_v7().hyphenated().to_string();
        let message = PublicMessage::Assistant(PublicAssistantMessage {
            content: Vec::new(),
            model: "fixture-model".to_owned(),
            provider: "fixture-provider".to_owned(),
            origin: crate::provider::types::ProviderOrigin {
                provider_instance_id: "fixture-provider".to_owned(),
                protocol: ApiProtocol::OpenAiResponses,
                model: "fixture-model".to_owned(),
            },
            usage: Usage::default(),
            stop_reason: StopReason::Stop,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: Utc::now(),
        });
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message_in_turn(
                            "message_start",
                            &message_id,
                            &message,
                            Some(user.run_id.clone()),
                            Some(user.turn_id.clone()),
                        )
                        .expect("fixture assistant MessageStart"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: user.command_id.clone(),
                        run_id: user.run_id.clone(),
                        expected: RunPhase::UserCommitted,
                        next: RunPhase::AssistantStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("open fixture assistant");
        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_end",
                                &message_id,
                                &message,
                                Some(user.run_id.clone()),
                                Some(user.turn_id.clone()),
                            )
                            .expect("fixture assistant MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id,
                            role: "assistant",
                            message: message.clone(),
                            append_to_l0: true,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::turn_end(
                                user.run_id.clone(),
                                user.turn_id.clone(),
                                message,
                                Vec::new(),
                            )
                            .expect("fixture TurnEnd"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::agent_end(user.run_id.clone()).expect("fixture AgentEnd"),
                        ),
                        projections: vec![Projection::CommandApplied {
                            command_id: user.command_id.clone(),
                            command_seq: user.seq,
                            run_id: Some(user.run_id.clone()),
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("close fixture run");
    }

    async fn insert_excluded_assistant_message(
        store: &Store,
        user: &CanonicalUserFixture,
    ) -> String {
        let writer = EventWriter::new(Arc::new(store.clone()));
        let message_id = Uuid::now_v7().hyphenated().to_string();
        let message = PublicMessage::Assistant(PublicAssistantMessage {
            content: Vec::new(),
            model: "fixture-model".to_owned(),
            provider: "fixture-provider".to_owned(),
            origin: crate::provider::types::ProviderOrigin {
                provider_instance_id: "fixture-provider".to_owned(),
                protocol: ApiProtocol::OpenAiResponses,
                model: "fixture-model".to_owned(),
            },
            usage: Usage::default(),
            stop_reason: StopReason::Error,
            error_message: Some("retryable fixture".to_owned()),
            provider_code: None,
            interrupted: false,
            timestamp: Utc::now(),
        });
        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_start",
                                &message_id,
                                &message,
                                Some(user.run_id.clone()),
                                Some(user.turn_id.clone()),
                            )
                            .expect("excluded fixture MessageStart"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: user.command_id.clone(),
                            run_id: user.run_id.clone(),
                            expected: RunPhase::UserCommitted,
                            next: RunPhase::AssistantStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_end",
                                &message_id,
                                &message,
                                Some(user.run_id.clone()),
                                Some(user.turn_id.clone()),
                            )
                            .expect("excluded fixture MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: message_id.clone(),
                            role: "assistant",
                            message,
                            append_to_l0: false,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("commit excluded assistant MessageEnd");
        message_id
    }

    async fn insert_raw_user_message(
        store: &Store,
        id: &str,
        seq: u64,
        text: &str,
    ) -> TranscriptRecord {
        let key = store
            .private_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint transcript key");
        let message = PublicMessage::User(UserMessage {
            content: vec![UserContent::Text {
                text: text.to_owned(),
            }],
            timestamp: Utc::now(),
        });
        let record =
            TranscriptRecord::encrypt(&message, id, seq, &key, &store.scope, &store.redactor)
                .expect("encrypt transcript record");
        record
            .insert(store.pool())
            .await
            .expect("insert transcript record");
        record
    }

    #[tokio::test]
    async fn hydrate_rejects_missing_historical_message_end_projection() {
        let store = store().await;
        let user = insert_user_message(&store, 1, "retained user").await;
        let assistant_id = insert_excluded_assistant_message(&store, &user).await;

        let deleted = sqlx::query("DELETE FROM messages WHERE id = ?")
            .bind(&assistant_id)
            .execute(store.pool())
            .await
            .expect("delete excluded transcript fixture");
        assert_eq!(deleted.rows_affected(), 1);

        let error = store
            .hydrate(&test_lease(1), &test_fence(&test_lease(1)))
            .await
            .expect_err("authenticated MessageEnd deletion must fail closed");
        let message = format!("{error:#}");
        assert!(
            message.contains("requires exactly one transcript row, found 0")
                && message.contains(&assistant_id),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_transcript_row_without_message_end() {
        let store = store().await;
        insert_user_message(&store, 1, "authenticated user").await;
        insert_raw_user_message(&store, "extra-transcript", 999, "not in event log").await;

        let error = store
            .hydrate(&test_lease(1), &test_fence(&test_lease(1)))
            .await
            .expect_err("extra transcript row must fail exact MessageEnd hydration");
        let message = format!("{error:#}");
        assert!(
            message.contains("transcript row count")
                && message.contains("authenticated MessageEnd count"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_validly_reencrypted_message_that_differs_from_message_end() {
        let store = store().await;
        let user = insert_user_message(&store, 1, "authenticated content").await;
        let message_seq: i64 = sqlx::query_scalar("SELECT seq FROM messages WHERE id = ?")
            .bind(&user.message_id)
            .fetch_one(store.pool())
            .await
            .expect("load canonical message sequence");
        let message_seq = u64::try_from(message_seq).expect("message sequence is non-negative");
        let replacement = PublicMessage::User(UserMessage {
            content: vec![UserContent::Text {
                text: "different but valid content".to_owned(),
            }],
            timestamp: Utc::now(),
        });
        let key = store
            .private_key(DataKeyPurpose::Transcript)
            .await
            .expect("reuse transcript key");
        let raw = serde_json::to_vec(&replacement).expect("serialize replacement message");
        let aad = store
            .scope()
            .row_aad("messages", &user.message_id, DataKeyPurpose::Transcript);
        let ciphertext = encrypt_content(&key, &raw, &aad).expect("encrypt replacement message");
        let payload = store
            .redactor()
            .redact_serialized(&raw)
            .expect("redact replacement message");
        let search_text =
            search_text_from_projection(&payload).expect("derive replacement search text");
        let updated = sqlx::query(
            "UPDATE messages
             SET seq = ?, role = 'user', raw_key_ref = ?, raw_ciphertext = ?,
                 payload = ?, search_text = ?, redaction_version = 1,
                 interrupted = 0
             WHERE id = ?",
        )
        .bind(i64::try_from(message_seq).expect("message sequence fits SQLite"))
        .bind(&key.key_ref)
        .bind(ciphertext)
        .bind(payload)
        .bind(search_text)
        .bind(&user.message_id)
        .execute(store.pool())
        .await
        .expect("replace transcript with a validly encrypted different message");
        assert_eq!(updated.rows_affected(), 1);

        let error = store
            .hydrate(&test_lease(1), &test_fence(&test_lease(1)))
            .await
            .expect_err("valid row-local authentication must not override MessageEnd truth");
        let message = format!("{error:#}");
        assert!(
            message.contains("content disagrees with authenticated MessageEnd"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_validly_reencrypted_message_id_that_differs_from_message_end() {
        let store = store().await;
        let user = insert_user_message(&store, 1, "authenticated user").await;
        let assistant_id = insert_excluded_assistant_message(&store, &user).await;
        let replacement_id = Uuid::now_v7().to_string();
        let row = sqlx::query("SELECT raw_key_ref, raw_ciphertext FROM messages WHERE id = ?")
            .bind(&assistant_id)
            .fetch_one(store.pool())
            .await
            .expect("load excluded authenticated assistant");
        let key_ref: String = row.get("raw_key_ref");
        let key = store
            .data_key_by_ref(&key_ref)
            .await
            .expect("load transcript key");
        let old_aad = store
            .scope()
            .row_aad("messages", &assistant_id, DataKeyPurpose::Transcript);
        let raw = decrypt_content(&key, &row.get::<Vec<u8>, _>("raw_ciphertext"), &old_aad)
            .expect("decrypt canonical assistant");
        let new_aad =
            store
                .scope()
                .row_aad("messages", &replacement_id, DataKeyPurpose::Transcript);
        let ciphertext =
            encrypt_content(&key, &raw, &new_aad).expect("reencrypt under replacement identity");
        sqlx::query("UPDATE messages SET id = ?, raw_ciphertext = ? WHERE id = ?")
            .bind(&replacement_id)
            .bind(ciphertext)
            .bind(&assistant_id)
            .execute(store.pool())
            .await
            .expect("replace row identity with valid local authentication");

        let error = store
            .hydrate(&test_lease(1), &test_fence(&test_lease(1)))
            .await
            .expect_err("row-local id authentication must not override MessageEnd identity");
        let message = format!("{error:#}");
        assert!(
            message.contains("disagrees with authenticated MessageEnd id")
                && message.contains(&assistant_id)
                && message.contains(&replacement_id),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_interrupted_projection_that_differs_from_message_end() {
        let store = store().await;
        let user = insert_user_message(&store, 1, "authenticated user").await;
        let assistant_id = insert_excluded_assistant_message(&store, &user).await;
        sqlx::query("UPDATE messages SET interrupted = 1 WHERE id = ?")
            .bind(&assistant_id)
            .execute(store.pool())
            .await
            .expect("tamper interrupted projection");

        let error = store
            .hydrate(&test_lease(1), &test_fence(&test_lease(1)))
            .await
            .expect_err("interrupted projection must match authenticated MessageEnd");
        let message = format!("{error:#}");
        assert!(
            message.contains("interrupted flag does not match authenticated raw message"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_tampered_message_payload() {
        let store = store().await;
        insert_raw_user_message(&store, "msg-tamper-payload", 1, "hello world").await;

        sqlx::query("UPDATE messages SET payload = ? WHERE id = ?")
            .bind("tampered-payload")
            .bind("msg-tamper-payload")
            .execute(store.pool())
            .await
            .expect("tamper payload");

        let error = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store.hydrate_messages(&mut transaction).await
        }
        .expect_err("tampered payload must fail hydration");
        let message = format!("{error:#}");
        assert!(message.contains("stored payload"), "{message}");
    }

    #[tokio::test]
    async fn hydrate_rejects_tampered_message_search_text() {
        let store = store().await;
        insert_raw_user_message(&store, "msg-tamper-search", 2, "hello world").await;

        sqlx::query("UPDATE messages SET search_text = ? WHERE id = ?")
            .bind("tampered-search")
            .bind("msg-tamper-search")
            .execute(store.pool())
            .await
            .expect("tamper search text");

        let error = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store.hydrate_messages(&mut transaction).await
        }
        .expect_err("tampered search text must fail hydration");
        let message = format!("{error:#}");
        assert!(message.contains("stored search_text"), "{message}");
    }

    #[tokio::test]
    async fn migration_0006_creates_approval_rules_and_expands_tool_error_codes() {
        let store = store().await;

        sqlx::raw_sql(
            "INSERT INTO approval_rules(id, tool, pattern, created_at)
             VALUES('rule-1', 'bash', '{\"effect\":\"allow\"}', '2026-07-26T00:00:00Z')",
        )
        .execute(store.pool())
        .await
        .expect("insert approval rule on fresh schema");

        for error_code in [
            "user_steer_cancelled",
            "approval_denied",
            "approval_cancelled",
        ] {
            sqlx::query(
                "INSERT INTO tool_executions(
                    tool_call_id, command_id, run_id, executor_generation, state,
                    idempotency_key, started_at, finished_at, error_code
                 ) VALUES(?1, 'cmd-1', 'run-1', 0, 'not_started', ?2, NULL, '2026-07-26T00:00:00Z', ?3)",
            )
            .bind(format!("tool-{error_code}"))
            .bind(format!("idem-{error_code}"))
            .bind(error_code)
            .execute(store.pool())
            .await
            .unwrap_or_else(|e| panic!("{error_code} not_started must be accepted: {e}"));
        }

        sqlx::query(
            "INSERT INTO tool_executions(
                tool_call_id, command_id, run_id, executor_generation, state,
                idempotency_key, started_at, finished_at, error_code
             ) VALUES('tool-cancel-denied', 'cmd-1', 'run-1', 0, 'cancelled',
                      'idem-cancel-denied', '2026-07-26T00:00:00Z', '2026-07-26T00:00:00Z', 'approval_denied')",
        )
        .execute(store.pool())
        .await
        .expect("cancelled with approval_denied must be accepted");

        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM approval_rules")
                .fetch_one(store.pool())
                .await
                .expect("count approval_rules"),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM tool_executions WHERE state='not_started'"
            )
            .fetch_one(store.pool())
            .await
            .expect("count not_started tools"),
            3
        );

        let quick_check: String = sqlx::query_scalar("PRAGMA quick_check")
            .fetch_one(store.pool())
            .await
            .expect("quick_check");
        assert_eq!(quick_check, "ok");
        assert!(
            sqlx::query("PRAGMA foreign_key_check")
                .fetch_optional(store.pool())
                .await
                .expect("foreign_key_check")
                .is_none()
        );
    }

    #[tokio::test]
    async fn signed_approval_policy_survives_reopen_and_local_proposal_is_not_authority() {
        use crate::approval::action::{Permission, SecretAwareActionProjector, SecretDigestKey};
        use crate::approval::policy::{ApprovalRule, RuleEffect};
        use crate::approval::{ApprovalBroker, broker::ApprovalOutcome};
        use crate::provider::types::{ToolCall, ValidatedToolArguments};

        let rule = ApprovalRule {
            id: "rule-git-status".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["git".to_owned(), "status".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };

        let dir = std::env::temp_dir().join(format!("sumi-approval-restart-{}", Uuid::now_v7()));
        let path = dir.join("agent.db");

        let store = Store::open(&path, scope(), provider())
            .await
            .expect("open file-backed store");
        let now = Utc::now();
        let bundle = signed_approval_bundle(
            7,
            vec![rule.clone()],
            now - ChronoDuration::minutes(1),
            now + ChronoDuration::hours(1),
        );
        store
            .install_approval_policy_bundle(&bundle, &approval_trust(), "tenant-1", now)
            .await
            .expect("install signed policy");
        let pattern = serde_json::to_string(&rule).expect("serialize rule");
        sqlx::query(
            "INSERT INTO approval_rules(id, tool, pattern, created_at)
             VALUES(?, ?, ?, ?)",
        )
        .bind(&rule.id)
        .bind(&rule.tool)
        .bind(&pattern)
        .bind(Utc::now().to_rfc3339())
        .execute(store.pool())
        .await
        .expect("persist approval rule");
        store.pool().close().await;
        drop(store);

        let store = Store::open(&path, scope(), provider())
            .await
            .expect("reopen file-backed store");
        let loaded = store
            .load_approval_policy("/workspace", &approval_trust(), "tenant-1", 7, now)
            .await
            .expect("load persisted rules into policy");
        assert!(matches!(
            loaded.status,
            crate::approval::ApprovalPolicyCacheStatus::Verified { version: 7, .. }
        ));
        let projector = SecretAwareActionProjector::new(Redactor::v1(), SecretDigestKey::fixture());
        let broker = ApprovalBroker::headless(loaded.policy, projector);

        let arguments = serde_json::from_value::<ValidatedToolArguments>(
            serde_json::json!({"command": "git status"}),
        )
        .expect("validated bash arguments");
        let tool_call = ToolCall {
            id: "call-1".to_owned(),
            name: "bash".to_owned(),
            arguments,
        };
        let outcome = broker
            .start_request(
                &tool_call,
                &[],
                "run-1",
                "turn-1",
                "v1",
                CancellationToken::new(),
            )
            .await
            .expect("start request");
        assert!(
            matches!(outcome, ApprovalOutcome::Allowed { .. }),
            "loaded RuleEffect::Allow rule must allow matching bash command"
        );

        // A locally inserted proposal not covered by the signed bundle cannot
        // widen restart-time authority.
        let proposal = ApprovalRule {
            id: "proposal-npm-test".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["npm".to_owned(), "test".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        sqlx::query("INSERT INTO approval_rules(id, tool, pattern, created_at) VALUES(?, ?, ?, ?)")
            .bind(&proposal.id)
            .bind(&proposal.tool)
            .bind(serde_json::to_string(&proposal).unwrap())
            .bind(now.to_rfc3339())
            .execute(store.pool())
            .await
            .expect("insert uncovered local proposal");
        let reloaded = store
            .load_approval_policy("/workspace", &approval_trust(), "tenant-1", 7, now)
            .await
            .expect("reload signed policy");
        assert!(!reloaded.policy.rules().contains(&proposal));

        store.pool().close().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn approval_policy_cache_fails_closed_for_missing_expired_tampered_scope_and_stale() {
        use crate::approval::action::{CanonicalAction, Permission};
        use crate::approval::policy::{ApprovalRule, PolicyDecision, RuleEffect};
        use crate::provider::types::ValidatedToolArguments;

        fn assert_unavailable_asks(policy: &crate::approval::Policy) {
            let read_args: ValidatedToolArguments =
                serde_json::from_value(json!({"path":"notes.txt"})).unwrap();
            let read = CanonicalAction::from_tool_call(
                std::path::PathBuf::from("/workspace"),
                "read_file",
                &read_args,
            )
            .unwrap();
            assert!(
                matches!(policy.evaluate(&read), PolicyDecision::NeedsApproval { .. }),
                "unavailable authority must not retain the default workspace Allow"
            );

            let write_args: ValidatedToolArguments =
                serde_json::from_value(json!({"path":"/etc/passwd","content":"x"})).unwrap();
            let escaping_write = CanonicalAction::from_tool_call(
                std::path::PathBuf::from("/workspace"),
                "write_file",
                &write_args,
            )
            .unwrap();
            assert!(
                policy.evaluate(&escaping_write).is_forbidden(),
                "intrinsic workspace hard denies must survive unavailable authority"
            );
        }

        let store = store().await;
        let now = Utc::now();
        let rule = ApprovalRule {
            id: "rule-git-status".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["git".to_owned(), "status".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };

        let missing = store
            .load_approval_policy("/workspace", &approval_trust(), "tenant-1", 1, now)
            .await
            .expect("missing cache loads Ask policy");
        assert_eq!(
            missing.status,
            crate::approval::ApprovalPolicyCacheStatus::Missing
        );
        assert!(missing.policy.rules().is_empty());
        assert_unavailable_asks(&missing.policy);

        let expired = signed_approval_bundle(
            1,
            vec![rule.clone()],
            now - ChronoDuration::hours(2),
            now - ChronoDuration::hours(1),
        );
        assert!(
            store
                .install_approval_policy_bundle(&expired, &approval_trust(), "tenant-1", now)
                .await
                .is_err()
        );

        let wrong_scope_payload = crate::approval::ApprovalPolicyBundle {
            tenant_id: "tenant-2".to_owned(),
            ..signed_approval_bundle(
                1,
                vec![rule.clone()],
                now - ChronoDuration::minutes(1),
                now + ChronoDuration::hours(1),
            )
            .payload
        };
        let wrong_scope = crate::approval::SignedApprovalPolicyBundle {
            key_id: "control-plane-v1".to_owned(),
            signature: approval_signer()
                .sign(&wrong_scope_payload.signing_bytes().unwrap())
                .to_bytes()
                .to_vec(),
            payload: wrong_scope_payload,
        };
        assert!(
            store
                .install_approval_policy_bundle(&wrong_scope, &approval_trust(), "tenant-1", now)
                .await
                .is_err()
        );

        let valid = signed_approval_bundle(
            2,
            vec![rule],
            now - ChronoDuration::minutes(1),
            now + ChronoDuration::hours(1),
        );
        store
            .install_approval_policy_bundle(&valid, &approval_trust(), "tenant-1", now)
            .await
            .expect("install valid bundle");
        let expired_after_install = store
            .load_approval_policy(
                "/workspace",
                &approval_trust(),
                "tenant-1",
                2,
                now + ChronoDuration::hours(2),
            )
            .await
            .expect("expired cache loads Ask policy");
        assert!(matches!(
            expired_after_install.status,
            crate::approval::ApprovalPolicyCacheStatus::Unavailable { ref reason }
                if reason.contains("expired")
        ));
        assert!(expired_after_install.policy.rules().is_empty());
        assert_unavailable_asks(&expired_after_install.policy);

        let stale = store
            .load_approval_policy("/workspace", &approval_trust(), "tenant-1", 3, now)
            .await
            .expect("stale cache loads Ask policy");
        assert!(matches!(
            stale.status,
            crate::approval::ApprovalPolicyCacheStatus::Unavailable { ref reason }
                if reason.contains("stale")
        ));
        assert!(stale.policy.rules().is_empty());
        assert_unavailable_asks(&stale.policy);

        sqlx::query("UPDATE approval_policy_cache SET signature=zeroblob(64) WHERE singleton=1")
            .execute(store.pool())
            .await
            .expect("tamper cached signature");
        let tampered = store
            .load_approval_policy("/workspace", &approval_trust(), "tenant-1", 2, now)
            .await
            .expect("tampered cache loads Ask policy");
        assert!(matches!(
            tampered.status,
            crate::approval::ApprovalPolicyCacheStatus::Unavailable { ref reason }
                if reason.contains("signature")
        ));
        assert!(tampered.policy.rules().is_empty());
        assert_unavailable_asks(&tampered.policy);
    }

    #[tokio::test]
    async fn approval_policy_cache_replacement_is_monotonic_and_replay_idempotent() {
        let store = store().await;
        let now = Utc::now();
        let v1 = signed_approval_bundle(
            1,
            Vec::new(),
            now - ChronoDuration::minutes(1),
            now + ChronoDuration::hours(1),
        );
        let v2 = signed_approval_bundle(
            2,
            Vec::new(),
            now - ChronoDuration::minutes(1),
            now + ChronoDuration::hours(2),
        );
        store
            .install_approval_policy_bundle(&v1, &approval_trust(), "tenant-1", now)
            .await
            .expect("install v1");
        store
            .install_approval_policy_bundle(&v1, &approval_trust(), "tenant-1", now)
            .await
            .expect("exact v1 replay");
        store
            .install_approval_policy_bundle(&v2, &approval_trust(), "tenant-1", now)
            .await
            .expect("replace with v2");
        assert!(
            store
                .install_approval_policy_bundle(&v1, &approval_trust(), "tenant-1", now)
                .await
                .is_err(),
            "version rollback must fail"
        );
        let loaded = store
            .load_approval_policy("/workspace", &approval_trust(), "tenant-1", 2, now)
            .await
            .expect("load v2");
        assert!(matches!(
            loaded.status,
            crate::approval::ApprovalPolicyCacheStatus::Verified { version: 2, .. }
        ));
    }

    #[tokio::test]
    async fn load_approval_rules_rejects_malformed_and_invalid_stored_rules() {
        use crate::approval::action::Permission;
        use crate::approval::policy::{ApprovalRule, RuleEffect, RuleValidationError};

        // Malformed JSON must fail closed.
        let store_malformed = store().await;
        sqlx::query(
            "INSERT INTO approval_rules(id, tool, pattern, created_at)
             VALUES('rule-bad', 'bash', 'not-json', '2026-07-26T00:00:00Z')",
        )
        .execute(store_malformed.pool())
        .await
        .expect("insert malformed fixture rule");

        let error = match store_malformed.load_approval_rules().await {
            Err(error) => error,
            Ok(_) => panic!("malformed stored rule must fail closed"),
        };
        assert!(
            error.to_string().contains("malformed pattern"),
            "error must name the bad rule: {error}"
        );

        // A rule whose stored columns disagree with its pattern must fail closed.
        let store_mismatch = store().await;
        let mismatched = ApprovalRule {
            id: "rule-mismatch".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["git".to_owned(), "status".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        sqlx::query(
            "INSERT INTO approval_rules(id, tool, pattern, created_at)
             VALUES('rule-mismatch', 'edit_file', ?, '2026-07-26T00:00:01Z')",
        )
        .bind(serde_json::to_string(&mismatched).expect("serialize rule"))
        .execute(store_mismatch.pool())
        .await
        .expect("insert column mismatch rule");

        let error = match store_mismatch.load_approval_rules().await {
            Err(error) => error,
            Ok(_) => panic!("column/pattern mismatch must fail closed"),
        };
        assert!(
            error
                .to_string()
                .contains("stored columns do not match pattern"),
            "error must name the mismatch: {error}"
        );

        // A once-valid rule that is now too broad for current policy must fail closed.
        let store_broad = store().await;
        let broad = ApprovalRule {
            id: "rule-broad".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["bash".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        sqlx::query(
            "INSERT INTO approval_rules(id, tool, pattern, created_at)
             VALUES('rule-broad', 'bash', ?, '2026-07-26T00:00:02Z')",
        )
        .bind(serde_json::to_string(&broad).expect("serialize rule"))
        .execute(store_broad.pool())
        .await
        .expect("insert broad fixture rule");

        let stored = store_broad
            .load_approval_rules()
            .await
            .expect("deserialize broad proposal");
        let error = crate::approval::Policy::from_rules("/workspace", stored)
            .expect_err("broad stored rule must fail deterministic validation");
        assert!(
            error == RuleValidationError::BroadPrefix,
            "broad stored rule must fail with BroadPrefix, got {error}"
        );
    }

    #[tokio::test]
    async fn migration_0006_upgrades_from_0001_and_0002_without_data_loss() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open upgrade test pool");

        let one = MIGRATOR
            .migrations
            .iter()
            .find(|m| m.version == 1)
            .expect("migration 0001");
        let two = MIGRATOR
            .migrations
            .iter()
            .find(|m| m.version == 2)
            .expect("migration 0002");

        sqlx::raw_sql(one.sql.as_ref())
            .execute(&pool)
            .await
            .expect("apply 0001 manually");
        sqlx::raw_sql(two.sql.as_ref())
            .execute(&pool)
            .await
            .expect("apply 0002 manually");

        // Seed a 0002-era skipped tool and a cancelled tool that must survive the 0003 rebuild.
        sqlx::query(
            "INSERT INTO tool_executions(
                tool_call_id, command_id, run_id, executor_generation, state,
                idempotency_key, started_at, finished_at, error_code
             ) VALUES('tool-legacy-steer', 'cmd-1', 'run-1', 0, 'not_started',
                      'idem-legacy-steer', NULL, '2026-07-26T00:00:00Z', 'user_steer_cancelled')",
        )
        .execute(&pool)
        .await
        .expect("seed 0002-era user_steer_cancelled row");
        sqlx::query(
            "INSERT INTO tool_executions(
                tool_call_id, command_id, run_id, executor_generation, state,
                idempotency_key, started_at, finished_at, error_code
             ) VALUES('tool-legacy-cancel', 'cmd-1', 'run-1', 0, 'cancelled',
                      'idem-legacy-cancel', '2026-07-26T00:00:00Z', '2026-07-26T00:00:00Z', 'cancelled')",
        )
        .execute(&pool)
        .await
        .expect("seed 0002-era cancelled row");

        sqlx::raw_sql(
            "CREATE TABLE _sqlx_migrations (
                version BIGINT PRIMARY KEY,
                description TEXT NOT NULL,
                installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                success BOOLEAN NOT NULL,
                checksum BLOB NOT NULL,
                execution_time BIGINT NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .expect("create migrations tracking table");

        for migration in [one, two] {
            sqlx::query(
                "INSERT INTO _sqlx_migrations(
                    version, description, success, checksum, execution_time
                 ) VALUES(?1, ?2, TRUE, ?3, 0)",
            )
            .bind(migration.version)
            .bind(migration.description.as_ref())
            .bind(&*migration.checksum)
            .execute(&pool)
            .await
            .expect("record applied migration");
        }

        MIGRATOR
            .run(&pool)
            .await
            .expect("apply migrations 0003 through 0009");

        let applied: Vec<i64> = sqlx::query_scalar(
            "SELECT version FROM _sqlx_migrations WHERE success = TRUE ORDER BY version",
        )
        .fetch_all(&pool)
        .await
        .expect("list applied migrations");
        assert_eq!(applied, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);

        let table_sql: String = sqlx::query_scalar(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='approval_rules'",
        )
        .fetch_one(&pool)
        .await
        .expect("approval_rules table exists");
        assert!(table_sql.contains("id TEXT NOT NULL PRIMARY KEY"));
        assert!(table_sql.contains("tool TEXT NOT NULL"));
        assert!(table_sql.contains("pattern TEXT NOT NULL"));
        assert!(table_sql.contains("created_at TEXT NOT NULL"));
        let policy_cache_sql: String = sqlx::query_scalar(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='approval_policy_cache'",
        )
        .fetch_one(&pool)
        .await
        .expect("approval_policy_cache table exists");
        assert!(policy_cache_sql.contains("signature BLOB NOT NULL"));

        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tool_executions")
                .fetch_one(&pool)
                .await
                .expect("count legacy tool rows"),
            2
        );

        for error_code in ["approval_denied", "approval_cancelled"] {
            sqlx::query(
                "INSERT INTO tool_executions(
                    tool_call_id, command_id, run_id, executor_generation, state,
                    idempotency_key, started_at, finished_at, error_code
                 ) VALUES(?1, 'cmd-1', 'run-1', 0, 'not_started', ?2, NULL, '2026-07-26T00:00:00Z', ?3)",
            )
            .bind(format!("tool-upgrade-{error_code}"))
            .bind(format!("idem-upgrade-{error_code}"))
            .bind(error_code)
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("{error_code} not_started must be accepted after upgrade: {e}"));
        }

        let quick_check: String = sqlx::query_scalar("PRAGMA quick_check")
            .fetch_one(&pool)
            .await
            .expect("quick_check");
        assert_eq!(quick_check, "ok");
        assert!(
            sqlx::query("PRAGMA foreign_key_check")
                .fetch_optional(&pool)
                .await
                .expect("foreign_key_check")
                .is_none()
        );
    }

    async fn migration_pool_through(version: i64) -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open migration fixture pool");
        for migration in MIGRATOR
            .migrations
            .iter()
            .filter(|migration| migration.version <= version)
        {
            sqlx::raw_sql(migration.sql.as_ref())
                .execute(&pool)
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "apply migration {} ({}): {error}",
                        migration.version, migration.description
                    )
                });
        }
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&pool)
            .await
            .expect("enable foreign keys for migration fixture");
        pool
    }

    #[tokio::test]
    async fn migration_0009_rejects_legacy_memory_state_and_calibration() {
        let nine = MIGRATOR
            .migrations
            .iter()
            .find(|migration| migration.version == 9)
            .expect("migration 0009");

        for fixture in ["memory_batch", "calibration"] {
            let pool = migration_pool_through(8).await;
            match fixture {
                "memory_batch" => {
                    sqlx::query(
                        "INSERT INTO memory_batches(
                            id, layer, ord, batch_seq, version, state, est_tokens,
                            eviction_footprint_tokens, updated_at
                         ) VALUES('legacy-batch', 0, 1, 1, 0, 'open', 1, 0, 'now')",
                    )
                    .execute(&pool)
                    .await
                    .expect("seed legacy memory batch");
                }
                "calibration" => {
                    sqlx::query("INSERT INTO kv(key, value) VALUES('calib.ratio', '1.25')")
                        .execute(&pool)
                        .await
                        .expect("seed legacy calibration");
                }
                _ => unreachable!(),
            }

            let error = sqlx::raw_sql(nine.sql.as_ref())
                .execute(&pool)
                .await
                .expect_err("0009 must reject unauthenticated legacy memory state");
            assert!(
                format!("{error:#}").contains("CHECK constraint failed"),
                "{fixture} upgrade failed for an unexpected reason: {error:#}"
            );
            pool.close().await;
        }

        let current = migration_pool_through(9).await;
        let error = sqlx::query("INSERT INTO kv(key, value) VALUES('calib.ratio', '1.5')")
            .execute(&current)
            .await
            .expect_err("legacy calibration key must remain reserved after 0009");
        assert!(
            format!("{error:#}").contains("calib.ratio is reserved"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn migration_0009_memory_projection_event_foreign_keys_are_deferred() {
        let store = store().await;
        let event_key = store
            .private_key(DataKeyPurpose::Event)
            .await
            .expect("mint event key");

        let mut transaction = store.pool().begin().await.expect("begin transaction");
        sqlx::query(
            "INSERT INTO memory_batches(
                id, layer, ord, batch_seq, version, state, est_tokens,
                eviction_footprint_tokens, projection_event_seq, projection_digest, updated_at
             ) VALUES(
                'deferred-positive', 0, 1, 1, 0, 'open', 0, 0,
                500, zeroblob(32), 'now'
             )",
        )
        .execute(&mut *transaction)
        .await
        .expect("memory row may precede its event inside one transaction");
        sqlx::query(
            "INSERT INTO agent_events(
                seq, event_type, internal_metadata, raw_key_ref, raw_ciphertext,
                envelope, redaction_version, created_at
             ) VALUES(500, 'memory_maintenance', '{}', ?, X'00', '{}', 1, 'now')",
        )
        .bind(&event_key.key_ref)
        .execute(&mut *transaction)
        .await
        .expect("insert deferred parent event");
        transaction
            .commit()
            .await
            .expect("deferred event reference resolves at commit");

        let mut transaction = store.pool().begin().await.expect("begin transaction");
        sqlx::query(
            "INSERT INTO memory_batches(
                id, layer, ord, batch_seq, version, state, est_tokens,
                eviction_footprint_tokens, projection_event_seq, projection_digest, updated_at
             ) VALUES(
                'deferred-negative', 0, 2, 2, 0, 'open', 0, 0,
                501, zeroblob(32), 'now'
             )",
        )
        .execute(&mut *transaction)
        .await
        .expect("deferred missing parent is checked at commit");
        let error = transaction
            .commit()
            .await
            .expect_err("missing projection event must reject commit");
        assert!(
            format!("{error:#}").contains("FOREIGN KEY constraint failed"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn migration_0008_preserves_failed_jobs_and_adds_discarded_status() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open migration 0008 test pool");

        for migration in MIGRATOR
            .migrations
            .iter()
            .filter(|migration| migration.version < 8)
        {
            sqlx::raw_sql(migration.sql.as_ref())
                .execute(&pool)
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "apply migration {} before 0008 fixture: {error}",
                        migration.version
                    )
                });
        }

        sqlx::query(
            "INSERT INTO memory_jobs(
                id, kind, batch_seq, source_ids, source_versions, status,
                lease_until, attempts, created_at, updated_at
             ) VALUES(
                'failed-before-0008', 'compact_l0', 1, '[\"source\"]',
                '{\"source\":1}', 'failed', NULL, 3, 'now', 'now'
             )",
        )
        .execute(&pool)
        .await
        .expect("seed pre-0008 failed job");

        let migration = MIGRATOR
            .migrations
            .iter()
            .find(|migration| migration.version == 8)
            .expect("migration 0008");
        sqlx::raw_sql(migration.sql.as_ref())
            .execute(&pool)
            .await
            .expect("apply migration 0008");

        let preserved: (String, i64) =
            sqlx::query_as("SELECT status, attempts FROM memory_jobs WHERE id = ?")
                .bind("failed-before-0008")
                .fetch_one(&pool)
                .await
                .expect("read preserved failed job");
        assert_eq!(preserved, ("failed".to_owned(), 3));

        sqlx::query(
            "INSERT INTO memory_jobs(
                id, kind, batch_seq, source_ids, source_versions, status,
                lease_until, attempts, created_at, updated_at
             ) VALUES(
                'discarded-after-0008', 'compact_l0', 2, '[\"source\"]',
                '{}', 'discarded', NULL, 1, 'now', 'now'
             )",
        )
        .execute(&pool)
        .await
        .expect("0008 accepts discarded job");

        let error = sqlx::query(
            "INSERT INTO memory_jobs(
                id, kind, batch_seq, source_ids, source_versions, status,
                lease_until, attempts, created_at, updated_at
             ) VALUES(
                'invalid-after-0008', 'compact_l0', 3, '[\"source\"]',
                '{}', 'terminal', NULL, 1, 'now', 'now'
             )",
        )
        .execute(&pool)
        .await
        .expect_err("0008 must still reject unknown job statuses");
        assert!(
            error.to_string().contains("CHECK constraint failed"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn migration_0006_preserves_nonempty_physical_recovery_attestations() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open migration boundary pool");
        sqlx::raw_sql(
            "PRAGMA foreign_keys=ON;
             CREATE TABLE agent_events(seq INTEGER PRIMARY KEY);
             CREATE TABLE physical_recovery_receipt_applications(
               receipt_id TEXT PRIMARY KEY
             );
             CREATE TABLE tool_executions(
               tool_call_id TEXT NOT NULL PRIMARY KEY,
               command_id TEXT NOT NULL,
               run_id TEXT NOT NULL,
               executor_generation INTEGER NOT NULL,
               state TEXT NOT NULL,
               idempotency_key TEXT NOT NULL UNIQUE,
               started_at TEXT,
               finished_at TEXT,
               error_code TEXT
             );
             CREATE UNIQUE INDEX tool_executions_attestation
             ON tool_executions(tool_call_id, command_id, run_id, executor_generation);
             CREATE TABLE physical_recovery_receipt_intents(
               receipt_id TEXT NOT NULL,
               tool_call_id TEXT NOT NULL,
               command_id TEXT NOT NULL,
               run_id TEXT NOT NULL,
               executor_generation INTEGER NOT NULL,
               indeterminate_terminal_seq INTEGER NOT NULL,
               PRIMARY KEY(receipt_id, tool_call_id),
               UNIQUE(tool_call_id),
               UNIQUE(indeterminate_terminal_seq),
               FOREIGN KEY(receipt_id)
                 REFERENCES physical_recovery_receipt_applications(receipt_id),
               FOREIGN KEY(tool_call_id, command_id, run_id, executor_generation)
                 REFERENCES tool_executions(
                   tool_call_id, command_id, run_id, executor_generation
                 ),
               FOREIGN KEY(indeterminate_terminal_seq) REFERENCES agent_events(seq)
             );
             INSERT INTO agent_events(seq) VALUES(7);
             INSERT INTO physical_recovery_receipt_applications(receipt_id)
             VALUES('receipt-1');
             INSERT INTO tool_executions(
               tool_call_id, command_id, run_id, executor_generation, state,
               idempotency_key, started_at, finished_at, error_code
             ) VALUES(
               'tool-1', 'command-1', 'run-1', 3, 'running',
               'idem-1', '2026-07-27T00:00:00Z', NULL, NULL
             );
             INSERT INTO physical_recovery_receipt_intents(
               receipt_id, tool_call_id, command_id, run_id, executor_generation,
               indeterminate_terminal_seq
             ) VALUES('receipt-1', 'tool-1', 'command-1', 'run-1', 3, 7);",
        )
        .execute(&pool)
        .await
        .expect("seed enforced nonempty attestation graph");

        let six = MIGRATOR
            .migrations
            .iter()
            .find(|migration| migration.version == 6)
            .expect("migration 0006");
        let mut transaction = pool.begin().await.expect("begin sqlx-style migration");
        sqlx::raw_sql(six.sql.as_ref())
            .execute(&mut *transaction)
            .await
            .expect("migration preserves children with foreign keys enabled");
        transaction.commit().await.expect("commit migration");

        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM physical_recovery_receipt_intents
                 WHERE receipt_id='receipt-1' AND tool_call_id='tool-1'"
            )
            .fetch_one(&pool)
            .await
            .expect("count preserved attestation"),
            1
        );
        assert!(
            sqlx::query("PRAGMA foreign_key_check")
                .fetch_optional(&pool)
                .await
                .expect("foreign_key_check")
                .is_none()
        );
    }

    #[tokio::test]
    async fn load_approval_rules_is_deterministic_when_created_at_ties() {
        use crate::approval::action::Permission;
        use crate::approval::policy::{ApprovalRule, RuleEffect};

        let store = store().await;
        let ts = "2026-07-26T00:00:00Z";
        let rule_b = ApprovalRule {
            id: "rule-b".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["b".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        let rule_a = ApprovalRule {
            id: "rule-a".to_owned(),
            tool: "bash".to_owned(),
            literal_prefix: vec!["a".to_owned()],
            effect: RuleEffect::Allow,
            workspace_only: true,
            allowed_permissions: vec![Permission::Exec],
            allowed_network_domains: vec![],
        };
        for rule in [&rule_b, &rule_a] {
            sqlx::query(
                "INSERT INTO approval_rules(id, tool, pattern, created_at) VALUES(?, ?, ?, ?)",
            )
            .bind(&rule.id)
            .bind(&rule.tool)
            .bind(serde_json::to_string(rule).expect("serialize rule"))
            .bind(ts)
            .execute(store.pool())
            .await
            .expect("insert rule");
        }

        let loaded = store.load_approval_rules().await.expect("load rules");
        let ids: Vec<&str> = loaded.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec!["rule-a", "rule-b"]);
    }
}
