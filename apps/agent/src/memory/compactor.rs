//! A memory reasoning fork of the actual parent request.
//!
//! The edit target limits what may be replaced, never what Sumi can read. The
//! parent system, tools, transcript, images, provider state and request settings
//! are cloned intact. Only a trailing memory directive is added. This module
//! has no tool executor, alternate compact model or batch-only request path.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{Row, sqlite::SqliteRow};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::prompts::CompactPrompt;
use crate::provider::types::{
    AssistantContent, ContextMessage, Message, ParentContextSnapshot, PromptContext, ProviderEvent,
    ProviderEventStream, ProviderOutput, StopReason, UserContent, UserMessage,
};
use crate::provider::{ModelSpec, RequestOptions};
use crate::store::{
    BootstrapRecoveryGuard, DurableEvent, EventBatch, EventWrite, EventWriter, MemoryBatchMutation,
    MemoryBatchRecord, MemoryBatchState, MemoryJobKind, MemoryJobMutation, MemoryJobRecord,
    MemoryJobStatus, MemoryJobUpdate, MemoryLayer, MemoryTransition, Projection,
    RecoveryBatchWriter, Store,
};

use super::{BatchId, CompactResult, DecryptedMemorySummary};

const LEASE_DURATION: Duration = Duration::from_secs(300);
const KEEP_UNCHANGED: &str = "KEEP_UNCHANGED";

#[derive(Debug, thiserror::Error)]
pub(crate) enum CompactError {
    #[error("memory fork was cancelled")]
    Cancelled,
    #[error("memory fork input is invalid: {0}")]
    InvalidInput(String),
    #[error("memory fork did not return a complete replacement")]
    IncompleteResponse,
    #[error("memory fork requested a tool; no tool was executed")]
    ToolRequested,
    #[error("memory fork token estimate failed: {0}")]
    Estimate(String),
}

/// A location in the unchanged parent transcript. Indices count only
/// `PromptContext.messages`, start at one, and do not count memory blocks.
#[derive(Clone, Debug, Serialize)]
struct TargetMessage {
    parent_message_index: usize,
    message_id: String,
    seq: u64,
    role: &'static str,
    timestamp: DateTime<Utc>,
    original: Message,
}

#[derive(Clone, Serialize)]
struct CompactionTarget {
    batch_id: BatchId,
    source_version: u64,
    messages: Vec<TargetMessage>,
}

/// Only an actual parent snapshot can construct the reasoning input. Durable
/// target records provide identity and version metadata, not replacement text
/// or a separate, reduced view of the conversation.
#[derive(Clone)]
pub(crate) struct CompactionInput {
    parent: ParentContextSnapshot,
    target: CompactionTarget,
}

impl CompactionInput {
    fn from_parent(
        parent: ParentContextSnapshot,
        batch_id: BatchId,
        source_version: u64,
        messages: &[(String, u64)],
    ) -> Result<Self, CompactError> {
        if messages.is_empty() {
            return Err(CompactError::InvalidInput("empty edit target".into()));
        }
        let mut target_messages = Vec::with_capacity(messages.len());
        let mut previous_index = None;
        for (message_id, seq) in messages {
            let Some((index, message)) =
                parent
                    .prompt()
                    .messages
                    .iter()
                    .enumerate()
                    .find_map(|(index, context)| match context {
                        ContextMessage::Persisted {
                            id,
                            seq: actual_seq,
                            message,
                        } if id == message_id && actual_seq == seq => Some((index, message)),
                        _ => None,
                    })
            else {
                return Err(CompactError::InvalidInput(
                    "edit target is not wholly present in the actual parent context".into(),
                ));
            };
            if previous_index.is_some_and(|previous| previous >= index) {
                return Err(CompactError::InvalidInput(
                    "edit target is out of order".into(),
                ));
            }
            previous_index = Some(index);
            let (role, timestamp) = match message {
                Message::User(message) => ("user", message.timestamp),
                Message::Assistant(message) => ("assistant", message.timestamp),
                Message::ToolResult(message) => ("tool_result", message.timestamp),
            };
            target_messages.push(TargetMessage {
                parent_message_index: index + 1,
                message_id: message_id.clone(),
                seq: *seq,
                role,
                timestamp,
                original: message.clone(),
            });
        }
        Ok(Self {
            parent,
            target: CompactionTarget {
                batch_id,
                source_version,
                messages: target_messages,
            },
        })
    }

    fn fork_prompt(&self) -> Result<PromptContext, CompactError> {
        // Canonical indices alone are not visible after provider adapters merge
        // or split messages. Supply an exact target copy after the full parent
        // prefix, with images as native blocks, so the edit boundary is explicit.
        let mut target = serde_json::to_value(&self.target)
            .map_err(|error| CompactError::InvalidInput(error.to_string()))?;
        let mut images = Vec::new();
        for message in target["messages"]
            .as_array_mut()
            .expect("serialized target messages")
        {
            if let Some(content) = message["original"]["content"].as_array_mut() {
                for block in content {
                    if block["type"].as_str() == Some("image") {
                        let image: UserContent = serde_json::from_value(block.clone())
                            .map_err(|error| CompactError::InvalidInput(error.to_string()))?;
                        images.push(image);
                        *block = serde_json::json!({
                            "type": "image",
                            "target_image_index": images.len(),
                            "mime_type": block["mime_type"],
                        });
                    }
                }
            }
        }
        let target = serde_json::to_string(&serde_json::json!({"compact_target": target}))
            .map_err(|error| CompactError::InvalidInput(error.to_string()))?;
        let mut content = vec![UserContent::Text {
            text: format!("{}\n\n{}", CompactPrompt::L0ToL1.as_str(), target),
        }];
        for (index, image) in images.into_iter().enumerate() {
            content.push(UserContent::Text {
                text: format!("compact_target target_image_index={}", index + 1),
            });
            content.push(image);
        }
        self.parent
            .fork_with_directive(UserMessage {
                incoming_source: None,
                incoming_timing: None,
                content,
                timestamp: Utc::now(),
            })
            .map_err(CompactError::InvalidInput)
    }

    fn time_range(&self) -> (DateTime<Utc>, DateTime<Utc>) {
        let timestamps = self.target.messages.iter().map(|message| message.timestamp);
        (
            timestamps.clone().min().expect("nonempty target"),
            timestamps.max().expect("nonempty target"),
        )
    }
}

/// Injection seam for tests; production always uses the normal conversation
/// provider stream. Execution permission is not encoded by changing tool
/// definitions or the parent's provider options.
trait CompactProvider: Send + Sync {
    fn start(
        &self,
        spec: ModelSpec,
        context: PromptContext,
        options: RequestOptions,
        cancel: CancellationToken,
    ) -> ProviderEventStream;
}

struct ParentProvider;

impl CompactProvider for ParentProvider {
    fn start(
        &self,
        spec: ModelSpec,
        context: PromptContext,
        options: RequestOptions,
        cancel: CancellationToken,
    ) -> ProviderEventStream {
        crate::provider::stream(spec, context, options, cancel)
    }
}

/// None means Sumi chose to retain the original representation.
async fn compact(
    input: &CompactionInput,
    cancel: CancellationToken,
    provider: &dyn CompactProvider,
) -> Result<Option<CompactResult>, CompactError> {
    if cancel.is_cancelled() {
        return Err(CompactError::Cancelled);
    }
    let context = input.fork_prompt()?;
    // Dropping a provider stream cancels its own token. A child ensures a
    // failed or completed memory fork cannot cancel the parent run.
    let stream_cancel = cancel.child_token();
    let mut events = provider.start(
        input.parent.spec().clone(),
        context,
        input.parent.options().clone(),
        stream_cancel,
    );
    loop {
        let event = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(CompactError::Cancelled),
            event = events.recv() => event,
        };
        match event {
            Some(ProviderEvent::Done {
                reason: StopReason::Stop,
                output,
            }) => {
                return result_from_output(output, input);
            }
            Some(
                ProviderEvent::ToolCallStart { .. }
                | ProviderEvent::ToolCallEnd { .. }
                | ProviderEvent::ToolCallRejected { .. },
            ) => {
                return Err(CompactError::ToolRequested);
            }
            Some(ProviderEvent::Done { .. } | ProviderEvent::Error { .. }) | None => {
                return Err(CompactError::IncompleteResponse);
            }
            Some(_) => {}
        }
    }
}

fn result_from_output(
    output: ProviderOutput,
    input: &CompactionInput,
) -> Result<Option<CompactResult>, CompactError> {
    let message = output.message;
    if message.stop_reason != StopReason::Stop
        || message.interrupted
        || message.error_message.is_some()
    {
        return Err(CompactError::IncompleteResponse);
    }
    let mut text = String::new();
    for content in message.content {
        match content {
            AssistantContent::Text { text: part, .. } => text.push_str(&part),
            AssistantContent::Thinking { .. } => {}
            AssistantContent::ToolCall { .. } | AssistantContent::RejectedToolCall { .. } => {
                return Err(CompactError::ToolRequested);
            }
        }
    }
    if text.trim().is_empty() {
        return Err(CompactError::IncompleteResponse);
    }
    if text.trim() == KEEP_UNCHANGED {
        return Ok(None);
    }
    let est_tokens = crate::memory::estimate::estimate_text_tokens(&text)
        .map_err(|error| CompactError::Estimate(error.to_string()))?;
    Ok(Some(CompactResult {
        summary: DecryptedMemorySummary::new(text),
        est_tokens,
        time_range: input.time_range(),
    }))
}

/// Run at most one queued L0 edit from a fresh parent snapshot. The caller owns
/// the task lifetime and supplies a new actual snapshot for the next attempt.
/// Missing target context never falls back to a batch-only reconstruction.
pub(crate) async fn compact_next_l0(
    store: Arc<Store>,
    parent: ParentContextSnapshot,
    cancel: CancellationToken,
) -> Result<bool> {
    compact_next_l0_with_provider(store, parent, cancel, &ParentProvider).await
}

async fn compact_next_l0_with_provider(
    store: Arc<Store>,
    parent: ParentContextSnapshot,
    cancel: CancellationToken,
    provider: &dyn CompactProvider,
) -> Result<bool> {
    if cancel.is_cancelled() {
        return Ok(false);
    }
    recover_expired_running_jobs(&store).await?;
    let Some((mut job, input)) = claim_next_pending_job(&store, &parent).await? else {
        return Ok(false);
    };
    if cancel.is_cancelled() {
        release_claimed_job(&store, &job).await?;
        return Ok(false);
    }
    start_attempt(&store, &mut job).await?;
    match compact(&input, cancel, provider).await {
        Ok(Some(result)) => match complete_job(&store, &job, &result).await {
            Ok(ready) => Ok(ready),
            Err(error) => {
                let _ = release_claimed_job(&store, &job).await;
                Err(error.into())
            }
        },
        Ok(None) => {
            retain_original_job(&store, &job).await?;
            Ok(false)
        }
        Err(CompactError::Cancelled) => {
            release_claimed_job(&store, &job).await?;
            Ok(false)
        }
        Err(error) => {
            release_claimed_job(&store, &job).await?;
            Err(error.into())
        }
    }
}

async fn input_for_job(
    store: &Store,
    job: &Job,
    parent: ParentContextSnapshot,
) -> Result<Option<CompactionInput>> {
    if job.kind != MemoryJobKind::CompactL0 || job.source_ids.len() != 1 {
        bail!("only a single L0 batch can be a memory edit target");
    }
    let source_id = &job.source_ids[0];
    let rows = sqlx::query(
        "SELECT m.id, m.seq FROM messages m
         JOIN memory_batch_messages mbm ON m.id = mbm.message_id
         WHERE mbm.batch_id = ? ORDER BY mbm.ord ASC",
    )
    .bind(source_id)
    .fetch_all(store.pool())
    .await?;
    let messages: Vec<(String, u64)> = rows
        .into_iter()
        .map(|row| {
            Ok((
                row.try_get("id")?,
                u64::try_from(row.try_get::<i64, _>("seq")?)?,
            ))
        })
        .collect::<Result<_>>()?;
    let expected = job_source_versions(job)?;
    if current_batch_versions(store, &expected).await? != expected {
        return Ok(None);
    }
    let batch_id = Uuid::parse_str(source_id)?;
    let version = *expected
        .get(&batch_id)
        .ok_or_else(|| anyhow!("source version missing"))?;
    match CompactionInput::from_parent(parent, batch_id, version, &messages) {
        Ok(input) => Ok(Some(input)),
        Err(CompactError::InvalidInput(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, thiserror::Error)]
enum WorkerError {
    #[error("store operation failed: {0}")]
    Store(#[from] anyhow::Error),
}

impl From<sqlx::Error> for WorkerError {
    fn from(error: sqlx::Error) -> Self {
        Self::Store(error.into())
    }
}

struct Job {
    id: String,
    kind: MemoryJobKind,
    batch_seq: i64,
    source_ids: Vec<String>,
    source_versions: HashMap<String, i64>,
    status: MemoryJobStatus,
    attempts: i64,
    lease_until: Option<String>,
}

struct BatchRow {
    id: String,
    version: i64,
    state: MemoryBatchState,
}

fn parse_job_kind(value: &str) -> Result<MemoryJobKind> {
    match value {
        "compact_l0" => Ok(MemoryJobKind::CompactL0),
        "compact_l1" => Ok(MemoryJobKind::CompactL1),
        "consolidate_l2" => Ok(MemoryJobKind::ConsolidateL2),
        _ => bail!("unknown memory job kind: {value}"),
    }
}

fn parse_job_status(value: &str) -> Result<MemoryJobStatus> {
    match value {
        "pending" => Ok(MemoryJobStatus::Pending),
        "running" => Ok(MemoryJobStatus::Running),
        "completed" => Ok(MemoryJobStatus::Completed),
        "applied" => Ok(MemoryJobStatus::Applied),
        "discarded" => Ok(MemoryJobStatus::Discarded),
        "failed" => Ok(MemoryJobStatus::Failed),
        "unchanged" => Ok(MemoryJobStatus::Unchanged),
        _ => bail!("unknown memory job status: {value}"),
    }
}

fn parse_batch_state(value: &str) -> Result<MemoryBatchState> {
    match value {
        "open" => Ok(MemoryBatchState::Open),
        "sealed" => Ok(MemoryBatchState::Sealed),
        "compacting" => Ok(MemoryBatchState::Compacting),
        "compact_failed" => Ok(MemoryBatchState::CompactFailed),
        "compacted" => Ok(MemoryBatchState::Compacted),
        "promoted" => Ok(MemoryBatchState::Promoted),
        "dropped" => Ok(MemoryBatchState::Dropped),
        _ => bail!("unknown memory batch state: {value}"),
    }
}

fn parse_job(row: &SqliteRow) -> Result<Job> {
    let source_ids: Vec<String> =
        serde_json::from_str(row.try_get::<String, _>("source_ids")?.as_str())
            .context("deserialize source_ids")?;
    let source_versions: HashMap<String, i64> =
        serde_json::from_str(row.try_get::<String, _>("source_versions")?.as_str())
            .context("deserialize source_versions")?;

    Ok(Job {
        id: row.try_get("id")?,
        kind: parse_job_kind(row.try_get::<String, _>("kind")?.as_str())?,
        batch_seq: row.try_get("batch_seq")?,
        source_ids,
        source_versions,
        status: parse_job_status(row.try_get::<String, _>("status")?.as_str())?,
        attempts: row.try_get("attempts")?,
        lease_until: row.try_get::<Option<String>, _>("lease_until")?,
    })
}

fn parse_batch_row(row: &SqliteRow) -> Result<BatchRow> {
    Ok(BatchRow {
        id: row.try_get("id")?,

        version: row.try_get("version")?,
        state: parse_batch_state(row.try_get::<String, _>("state")?.as_str())?,
    })
}

fn target_layer_for_kind(kind: MemoryJobKind) -> MemoryLayer {
    match kind {
        MemoryJobKind::CompactL0 => MemoryLayer::L1,
        MemoryJobKind::CompactL1 | MemoryJobKind::ConsolidateL2 => MemoryLayer::L2,
    }
}

async fn load_target_batch(
    store: &Store,
    kind: MemoryJobKind,
    batch_seq: i64,
) -> Result<Option<BatchRow>> {
    let layer = target_layer_for_kind(kind).as_i64();
    let row = sqlx::query(
        "SELECT id, layer, batch_seq, version, state
         FROM memory_batches
         WHERE layer = ? AND batch_seq = ?",
    )
    .bind(layer)
    .bind(batch_seq)
    .fetch_optional(store.pool())
    .await?;
    row.map(|r| parse_batch_row(&r)).transpose()
}

fn job_source_versions(job: &Job) -> Result<BTreeMap<BatchId, u64>> {
    job.source_versions
        .iter()
        .map(|(id, version)| {
            let batch_id = BatchId::parse_str(id)
                .with_context(|| format!("invalid source batch id {id} in job"))?;
            let version = u64::try_from(*version)
                .with_context(|| format!("source version for {id} out of range"))?;
            Ok((batch_id, version))
        })
        .collect()
}

async fn current_batch_versions(
    store: &Store,
    expected: &BTreeMap<BatchId, u64>,
) -> Result<BTreeMap<BatchId, u64>> {
    let mut current = BTreeMap::new();
    for batch_id in expected.keys() {
        let row = sqlx::query("SELECT version FROM memory_batches WHERE id = ?")
            .bind(batch_id.to_string())
            .fetch_optional(store.pool())
            .await
            .with_context(|| format!("failed to load batch version for {batch_id}"))?
            .ok_or_else(|| anyhow!("batch {batch_id} does not exist"))?;
        let version: i64 = row.try_get("version")?;
        current.insert(
            *batch_id,
            u64::try_from(version)
                .with_context(|| format!("batch {batch_id} version out of range"))?,
        );
    }
    Ok(current)
}

async fn claim_next_pending_job(
    store: &Store,
    parent: &ParentContextSnapshot,
) -> Result<Option<(Job, CompactionInput)>> {
    let rows = sqlx::query(
        "SELECT id, kind, batch_seq, source_ids, source_versions, status, attempts,
                lease_until, created_at, updated_at
         FROM memory_jobs
         WHERE status = 'pending' AND kind = 'compact_l0'
         ORDER BY batch_seq ASC, created_at ASC",
    )
    .fetch_all(store.pool())
    .await?;
    let mut candidate = None;
    for row in rows {
        let job = parse_job(&row)?;
        if let Some(input) = input_for_job(store, &job, parent.clone()).await? {
            candidate = Some((job, input));
            break;
        }
    }
    let Some((job, input)) = candidate else {
        return Ok(None);
    };
    let lease_until = (Utc::now() + LEASE_DURATION).to_rfc3339();

    let update = MemoryJobUpdate {
        expected_source_versions: BTreeMap::new(),
        job_mutations: vec![MemoryJobMutation::Claim {
            job_id: job.id.clone(),
            lease_until,
        }],
    };
    let batch = EventBatch {
        writes: vec![EventWrite {
            event: Some(DurableEvent::memory_maintenance("compact_claimed")?),
            projections: vec![Projection::MemoryJobUpdate(update)],
        }],
        injected_commands: Vec::new(),
    };

    if let Err(error) = EventWriter::new(Arc::new(store.clone())).apply(batch).await {
        tracing::debug!("claim CAS lost for {}: {error}", job.id);
        return Ok(None);
    }

    let row = sqlx::query(
        "SELECT id, kind, batch_seq, source_ids, source_versions, status, attempts,
                lease_until, created_at, updated_at
         FROM memory_jobs
         WHERE id = ?",
    )
    .bind(&job.id)
    .fetch_one(store.pool())
    .await?;

    Ok(Some((parse_job(&row)?, input)))
}

async fn release_or_reset_job(store: &Store, job: &Job) -> Result<()> {
    let update = MemoryJobUpdate {
        expected_source_versions: BTreeMap::new(),
        job_mutations: vec![MemoryJobMutation::Release {
            job_id: job.id.clone(),
            expected_attempt: job.attempts,
            lease_witness: job.lease_until.clone(),
        }],
    };
    EventWriter::new(Arc::new(store.clone()))
        .apply(EventBatch {
            writes: vec![EventWrite {
                event: Some(DurableEvent::memory_maintenance("compact_released")?),
                projections: vec![Projection::MemoryJobUpdate(update)],
            }],
            injected_commands: Vec::new(),
        })
        .await
        .context("release/reset job to pending")?;
    Ok(())
}

async fn release_claimed_job(store: &Store, job: &Job) -> Result<()> {
    release_or_reset_job(store, job).await
}

/// Persist the fact that this leased job is about to consume one provider
/// attempt.  The lease (claim) itself does not count; only a durable
/// `start_attempt` does.  This keeps crash-recovery from giving free retries
/// after real provider failures while also not consuming budget for a crash
/// that happens before the provider call is started.
async fn start_attempt(store: &Store, job: &mut Job) -> Result<()> {
    // Refresh the lease at the start of each provider attempt. A single
    // attempt can approach the header+body idle timeout budget, so the lease
    // must cover the remaining attempts without relying on the claim time.
    let lease_until = (Utc::now() + LEASE_DURATION).to_rfc3339();
    let update = MemoryJobUpdate {
        expected_source_versions: BTreeMap::new(),
        job_mutations: vec![MemoryJobMutation::Start {
            job_id: job.id.clone(),
            expected_attempt: job.attempts,
            lease_witness: job.lease_until.clone(),
            lease_until: lease_until.clone(),
        }],
    };
    EventWriter::new(Arc::new(store.clone()))
        .apply(EventBatch {
            writes: vec![EventWrite {
                event: Some(DurableEvent::memory_maintenance("compact_started")?),
                projections: vec![Projection::MemoryJobUpdate(update)],
            }],
            injected_commands: Vec::new(),
        })
        .await
        .context("start job attempt")?;
    job.attempts = job
        .attempts
        .checked_add(1)
        .ok_or_else(|| anyhow!("attempts overflow for job {}", job.id))?;
    job.lease_until = Some(lease_until);
    Ok(())
}

/// Restore only sources still owned by this snapshot. A failed speculative
/// summary must not hide the earlier summary or revive a superseded source.
async fn restore_abandoned_sources(
    store: &Store,
    job: &Job,
    owned_state: MemoryBatchState,
    expected_versions: &mut BTreeMap<BatchId, u64>,
    mutations: &mut Vec<MemoryBatchMutation>,
) -> Result<()> {
    for source_id in &job.source_ids {
        let row = sqlx::query("SELECT version, state FROM memory_batches WHERE id = ?")
            .bind(source_id)
            .fetch_optional(store.pool())
            .await?
            .ok_or_else(|| anyhow!("source batch {source_id} missing for supersede"))?;
        let version: i64 = row.try_get("version")?;
        let state: String = row.try_get("state")?;
        let batch_uuid = BatchId::parse_str(source_id)?;
        let checked_version = u64::try_from(version)?;
        expected_versions.insert(batch_uuid, checked_version);
        if state == owned_state.as_str()
            && job.source_versions.get(source_id).copied() == Some(version)
        {
            mutations.push(MemoryBatchMutation {
                batch_id: batch_uuid,
                expected_version: checked_version,
                new_state: if job.kind == MemoryJobKind::CompactL0 {
                    MemoryBatchState::CompactFailed
                } else {
                    MemoryBatchState::Promoted
                },
                // A state-only transition preserves the existing encrypted
                // summary, exact estimate, membership and provider footprint.
                summary: None,
                est_tokens: 0,
                footprint_delta: 0,
            });
        }
    }
    Ok(())
}

/// Retire this job's speculative target atomically with restoring any source
/// still owned by its snapshot. Later generations remain untouched.
async fn supersede_stale_job(
    store: &Store,
    job: &Job,
    target: &BatchRow,
) -> Result<(), WorkerError> {
    let target_uuid = BatchId::parse_str(&target.id).context("invalid abandoned target id")?;
    let target_version =
        u64::try_from(target.version).context("invalid abandoned target version")?;
    let mut expected_source_versions = BTreeMap::from([(target_uuid, target_version)]);
    let mut batch_mutations = Vec::new();
    if target.state == MemoryBatchState::Compacting
        && job.source_versions.get(&target.id).copied() == Some(target.version)
    {
        batch_mutations.push(MemoryBatchMutation {
            batch_id: target_uuid,
            expected_version: target_version,
            new_state: MemoryBatchState::Dropped,
            summary: None,
            est_tokens: 0,
            footprint_delta: 0,
        });
    }
    restore_abandoned_sources(
        store,
        job,
        MemoryBatchState::Compacting,
        &mut expected_source_versions,
        &mut batch_mutations,
    )
    .await?;
    let transition = MemoryTransition {
        expected_source_versions,
        batch_mutations,
        job_mutations: vec![MemoryJobMutation::Fail {
            job_id: job.id.clone(),
            expected_attempt: job.attempts,
            lease_witness: job.lease_until.clone(),
        }],
        ..Default::default()
    };
    EventWriter::new(Arc::new(store.clone()))
        .apply(EventBatch {
            writes: vec![EventWrite {
                event: Some(DurableEvent::memory_maintenance("compact_superseded")?),
                projections: vec![Projection::MemoryTransition(transition)],
            }],
            injected_commands: Vec::new(),
        })
        .await
        .context("supersede stale compaction job")?;
    Ok(())
}

async fn complete_job(
    store: &Store,
    job: &Job,
    result: &CompactResult,
) -> Result<bool, WorkerError> {
    let target = load_target_batch(store, job.kind, job.batch_seq)
        .await?
        .ok_or_else(|| anyhow!("target batch missing for job {}", job.id))?;

    if target.state != MemoryBatchState::Compacting {
        return Err(WorkerError::Store(anyhow!(
            "target batch {} is not compacting",
            target.id
        )));
    }

    let expected_source_versions = job_source_versions(job)?;
    let target_uuid = BatchId::parse_str(&target.id)
        .with_context(|| format!("target batch id {} is not a UUID", target.id))?;

    // Pre-verify source versions. If a concurrent worker has already advanced
    // the source batches, this job is stale: supersede it by failing the job
    // and closing the target, then let the scheduler move on.
    let current_versions = current_batch_versions(store, &expected_source_versions).await?;
    if current_versions != expected_source_versions {
        supersede_stale_job(store, job, &target).await?;
        return Ok(false);
    }

    let mut batch_mutations = Vec::with_capacity(job.source_ids.len() + 1);
    for source_id in &job.source_ids {
        let row = sqlx::query("SELECT version, state FROM memory_batches WHERE id = ?")
            .bind(source_id)
            .fetch_one(store.pool())
            .await
            .with_context(|| format!("load completion source batch {source_id}"))?;
        let version: i64 = row.try_get("version")?;
        let state: String = row.try_get("state")?;
        let expected =
            job.source_versions.get(source_id).copied().ok_or_else(|| {
                anyhow!("source version missing for {source_id} in job {}", job.id)
            })?;
        if version != expected || state != MemoryBatchState::Compacting.as_str() {
            supersede_stale_job(store, job, &target).await?;
            return Ok(false);
        }
        let source_uuid = BatchId::parse_str(source_id)
            .with_context(|| format!("source batch id {source_id} is not a UUID"))?;
        batch_mutations.push(MemoryBatchMutation {
            batch_id: source_uuid,
            expected_version: u64::try_from(version)
                .with_context(|| format!("source batch {source_id} version out of range"))?,
            new_state: MemoryBatchState::Compacted,
            summary: None,
            est_tokens: 0,
            footprint_delta: 0,
        });
    }
    batch_mutations.push(MemoryBatchMutation {
        batch_id: target_uuid,
        expected_version: target.version as u64,
        new_state: MemoryBatchState::Compacted,
        summary: Some(result.clone()),
        est_tokens: result.est_tokens,
        footprint_delta: 0,
    });

    let transition = MemoryTransition {
        expected_source_versions,
        batch_mutations,
        job_mutations: vec![MemoryJobMutation::Complete {
            job_id: job.id.clone(),
            expected_attempt: job.attempts,
            lease_witness: job.lease_until.clone(),
            result: result.clone(),
        }],
        cursor_advance: None,
        ..Default::default()
    };

    EventWriter::new(Arc::new(store.clone()))
        .apply(EventBatch {
            writes: vec![EventWrite {
                event: Some(DurableEvent::memory_maintenance("compact_completed")?),
                projections: vec![Projection::MemoryTransition(transition)],
            }],
            injected_commands: Vec::new(),
        })
        .await
        .context("complete compaction job")?;
    Ok(true)
}

/// Record a successful decision to retain the target, without manufacturing a
/// failed summary or scheduling the same no-op on every later parent turn.
async fn retain_original_job(store: &Store, job: &Job) -> Result<()> {
    let expected_source_versions = job_source_versions(job)?;
    if current_batch_versions(store, &expected_source_versions).await? != expected_source_versions {
        release_claimed_job(store, job).await?;
        return Ok(());
    }
    let target = load_target_batch(store, job.kind, job.batch_seq)
        .await?
        .ok_or_else(|| anyhow!("memory edit target is missing"))?;
    let mut mutations = Vec::with_capacity(job.source_ids.len() + 1);
    for source_id in &job.source_ids {
        let batch_id = Uuid::parse_str(source_id)?;
        let row = sqlx::query("SELECT est_tokens FROM memory_batches WHERE id = ?")
            .bind(source_id)
            .fetch_one(store.pool())
            .await?;
        mutations.push(MemoryBatchMutation {
            batch_id,
            expected_version: expected_source_versions[&batch_id],
            new_state: MemoryBatchState::Sealed,
            summary: None,
            est_tokens: u64::try_from(row.try_get::<i64, _>("est_tokens")?)?,
            footprint_delta: 0,
        });
    }
    let target_id = Uuid::parse_str(&target.id)?;
    mutations.push(MemoryBatchMutation {
        batch_id: target_id,
        expected_version: expected_source_versions[&target_id],
        new_state: MemoryBatchState::Dropped,
        summary: None,
        est_tokens: 0,
        footprint_delta: 0,
    });
    EventWriter::new(Arc::new(store.clone()))
        .apply(EventBatch {
            writes: vec![EventWrite {
                event: Some(DurableEvent::memory_maintenance("compact_unchanged")?),
                projections: vec![Projection::MemoryTransition(MemoryTransition {
                    expected_source_versions,
                    batch_mutations: mutations,
                    job_mutations: vec![MemoryJobMutation::RetainOriginal {
                        job_id: job.id.clone(),
                        expected_attempt: job.attempts,
                        lease_witness: job.lease_until.clone(),
                    }],
                    ..Default::default()
                })],
            }],
            injected_commands: Vec::new(),
        })
        .await?;
    Ok(())
}

async fn recover_expired_running_jobs_with<W: RecoveryBatchWriter>(writer: &mut W) -> Result<()> {
    let store = writer.recovery_store().clone();
    let now = Utc::now().to_rfc3339();
    let rows = sqlx::query(
        "SELECT id, attempts, lease_until
         FROM memory_jobs
         WHERE status = 'running' AND (lease_until IS NULL OR lease_until < ?)
         ORDER BY batch_seq, id",
    )
    .bind(&now)
    .fetch_all(store.pool())
    .await
    .context("list expired running memory jobs")?;
    for row in rows {
        let job_id: String = row.try_get("id")?;
        let attempts: i64 = row.try_get("attempts")?;
        if attempts < 0 {
            bail!("expired memory job {job_id} attempts out of range");
        }
        let lease_witness: Option<String> = row.try_get("lease_until")?;
        writer
            .apply_recovery_batch(EventBatch {
                writes: vec![EventWrite {
                    event: Some(DurableEvent::memory_maintenance(
                        "compact_expired_lease_recovered",
                    )?),
                    projections: vec![Projection::MemoryJobUpdate(MemoryJobUpdate {
                        expected_source_versions: BTreeMap::new(),
                        job_mutations: vec![MemoryJobMutation::Release {
                            job_id: job_id.clone(),
                            expected_attempt: attempts,
                            lease_witness,
                        }],
                    })],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .with_context(|| format!("recover expired running memory job {job_id}"))?;
    }
    Ok(())
}

async fn recover_expired_running_jobs(store: &Store) -> Result<()> {
    let mut writer = EventWriter::new(Arc::new(store.clone()));
    recover_expired_running_jobs_with(&mut writer).await
}

async fn recover_compacting_batches_with<W: RecoveryBatchWriter>(writer: &mut W) -> Result<()> {
    let store = writer.recovery_store().clone();
    let rows = sqlx::query(
        "SELECT id, batch_seq, version FROM memory_batches
         WHERE state = 'compacting' AND layer = 0",
    )
    .fetch_all(store.pool())
    .await?;

    for row in rows {
        let source_id: String = row.try_get("id")?;
        let version: i64 = row.try_get("version")?;
        let kind = MemoryJobKind::CompactL0;
        let target_layer = MemoryLayer::L1;

        // Skip if a job already references this source batch.
        let referenced: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM memory_jobs
             WHERE EXISTS (
                 SELECT 1 FROM json_each(source_ids) WHERE json_each.value = ?
             ) LIMIT 1",
        )
        .bind(&source_id)
        .fetch_optional(store.pool())
        .await
        .context("check existing memory job for source batch")?;
        if referenced.is_some() {
            continue;
        }

        // Recover the source through the EventWriter single-writer transaction
        // so the target batch_seq/ord allocation cannot race with concurrent L0
        // seal preparation.
        let source_uuid = BatchId::parse_str(&source_id)
            .with_context(|| format!("source batch id {source_id} is not a UUID"))?;
        let mut expected_source_versions = BTreeMap::new();
        expected_source_versions.insert(source_uuid, version as u64);
        let mut expected_source_states = BTreeMap::new();
        expected_source_states.insert(source_uuid, MemoryBatchState::Compacting);

        let target_id = Uuid::now_v7().to_string();
        let target_record = MemoryBatchRecord::new(
            &target_id,
            target_layer,
            0,
            0,
            MemoryBatchState::Compacting,
            0,
            0,
        );

        let mut source_versions = BTreeMap::new();
        source_versions.insert(source_id.clone(), version);
        source_versions.insert(target_id.clone(), 0);
        let job_record = MemoryJobRecord::new(
            Uuid::now_v7().to_string(),
            kind,
            0,
            vec![source_id.clone()],
            source_versions,
        );

        let transition = MemoryTransition {
            expected_source_versions,
            expected_source_states,
            batch_mutations: Vec::new(),
            job_mutations: Vec::new(),
            batch_inserts: vec![target_record],
            job_inserts: vec![job_record],
            membership_inserts: Vec::new(),
            cursor_advance: None,
        };

        writer
            .apply_recovery_batch(EventBatch {
                writes: vec![EventWrite {
                    event: Some(DurableEvent::memory_maintenance("compact_recovered")?),
                    projections: vec![Projection::MemoryTransition(transition)],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .context("recover compacting batch")?;

        tracing::debug!("reinserted compacting L0 batch {source_id} as {kind:?} job");
    }

    Ok(())
}

pub(crate) async fn recover_boot_memory_jobs(
    writer: &mut BootstrapRecoveryGuard<'_>,
) -> Result<()> {
    recover_expired_running_jobs_with(writer).await?;
    recover_compacting_batches_with(writer).await
}

/// Prepare results asynchronously, but retain the original provider prefix until
/// live L0 strictly exceeds its budget. Apply only enough oldest ready chunks to
/// return within the budget; unavailable older chunks do not block later ones.
/// Apply completed L0 edits independently. Each transaction checks only its
/// captured source versions, so an unchanged or unavailable older batch cannot
/// prevent another completed edit from becoming usable. Summary chronology is
/// retained by the target batch sequence and result time range.
pub(crate) async fn apply_ready_memory(store: Arc<Store>) -> Result<usize> {
    if live_l0_tokens(&store).await? <= super::L0_LIMIT {
        return Ok(0);
    }
    let rows = sqlx::query(
        "SELECT id, kind, batch_seq, source_ids, source_versions, status, attempts,
                lease_until, created_at, updated_at
         FROM memory_jobs WHERE kind = 'compact_l0' AND status = 'completed'
         ORDER BY batch_seq ASC",
    )
    .fetch_all(store.pool())
    .await?;
    let mut applied = 0;
    for row in rows {
        if live_l0_tokens(&store).await? <= super::L0_LIMIT {
            break;
        }
        let job = parse_job(&row)?;
        if apply_completed_job(store.clone(), &job).await? {
            applied += 1;
        }
    }
    Ok(applied)
}

/// Read accounting in one snapshot. Summary shelf rows are L1 and do not count;
/// compacted L0 remains live until its source-CAS promotion commits. The owning
/// Session is idle, so no conversation writer can race these decisions.
async fn live_l0_tokens(store: &Store) -> Result<u64> {
    let mut tx = store.pool().begin().await?;
    let row = sqlx::query(
        "SELECT COALESCE(SUM(est_tokens), 0) AS public_tokens,
                COALESCE(SUM(eviction_footprint_tokens), 0) AS footprint
         FROM memory_batches WHERE layer = 0 AND state != 'dropped'",
    )
    .fetch_one(&mut *tx)
    .await?;
    let public = u64::try_from(row.try_get::<i64, _>("public_tokens")?)?;
    let footprint = u64::try_from(row.try_get::<i64, _>("footprint")?)?;
    let bits = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT ratio_bits FROM memory_calibration WHERE singleton = 1",
    )
    .fetch_optional(&mut *tx)
    .await?;
    let calibration = match bits {
        Some(bits) => {
            let bits: [u8; 8] = bits
                .try_into()
                .map_err(|_| anyhow!("invalid calibration bytes"))?;
            super::estimate::TokenCalibration::new(f64::from_bits(u64::from_be_bytes(bits)))?
        }
        None => super::estimate::TokenCalibration::default(),
    };
    tx.commit().await?;
    Ok(calibration.effective_tokens(public, footprint)?)
}

async fn discard_stale_completed_job(
    store: Arc<Store>,
    job: &Job,
    target: Option<(&str, i64, &str)>,
) -> Result<bool> {
    let mut expected_source_versions = BTreeMap::new();
    let mut batch_mutations = Vec::new();

    if let Some((target_id, target_version, target_state)) = target
        && target_state == MemoryBatchState::Compacted.as_str()
        && job.source_versions.get(target_id).copied() == Some(target_version)
    {
        let target_uuid = BatchId::parse_str(target_id)
            .with_context(|| format!("target batch id {target_id} is not a UUID"))?;
        expected_source_versions.insert(
            target_uuid,
            u64::try_from(target_version)
                .with_context(|| format!("target batch {target_id} version out of range"))?,
        );
        batch_mutations.push(MemoryBatchMutation {
            batch_id: target_uuid,
            expected_version: target_version as u64,
            new_state: MemoryBatchState::Dropped,
            summary: None,
            est_tokens: 0,
            footprint_delta: 0,
        });
    }

    if job.kind != MemoryJobKind::CompactL0 {
        restore_abandoned_sources(
            &store,
            job,
            MemoryBatchState::Compacted,
            &mut expected_source_versions,
            &mut batch_mutations,
        )
        .await?;
    }
    let transition = MemoryTransition {
        expected_source_versions,
        batch_mutations,
        job_mutations: vec![MemoryJobMutation::Discard {
            job_id: job.id.clone(),
            expected_attempt: job.attempts,
            lease_witness: job.lease_until.clone(),
        }],
        ..Default::default()
    };
    EventWriter::new(store)
        .apply(EventBatch {
            writes: vec![EventWrite {
                event: Some(DurableEvent::memory_maintenance("compact_stale_discarded")?),
                projections: vec![Projection::MemoryTransition(transition)],
            }],
            injected_commands: Vec::new(),
        })
        .await
        .context("discard stale completed compaction job")?;
    Ok(false)
}

async fn apply_completed_job(store: Arc<Store>, job: &Job) -> Result<bool> {
    // Load the target batch and all source batches for the transition.
    let target_layer = target_layer_for_kind(job.kind).as_i64();
    let target_row = sqlx::query(
        "SELECT id, layer, version, state, est_tokens, eviction_footprint_tokens
         FROM memory_batches
         WHERE layer = ? AND batch_seq = ?",
    )
    .bind(target_layer)
    .bind(job.batch_seq)
    .fetch_optional(store.pool())
    .await
    .context("load apply target batch")?;
    let Some(target_row) = target_row else {
        bail!(
            "completed memory job {} target is missing; no authenticated GC/tombstone permits discard",
            job.id
        );
    };
    let target_id: String = target_row.try_get("id")?;
    let target_version: i64 = target_row.try_get("version")?;
    let target_state: String = target_row.try_get("state")?;
    let target_est_tokens: i64 = target_row.try_get("est_tokens")?;
    let target_expected = job
        .source_versions
        .get(&target_id)
        .copied()
        .ok_or_else(|| anyhow!("target version missing for {target_id} in job {}", job.id))?;
    if target_version != target_expected || target_state != MemoryBatchState::Compacted.as_str() {
        return discard_stale_completed_job(
            store,
            job,
            Some((&target_id, target_version, &target_state)),
        )
        .await;
    }

    let mut batch_mutations = Vec::with_capacity(job.source_ids.len() + 1);
    let mut expected_source_versions = BTreeMap::new();

    for source_id in &job.source_ids {
        let row = sqlx::query(
            "SELECT id, layer, version, state
             FROM memory_batches
             WHERE id = ?",
        )
        .bind(source_id)
        .fetch_optional(store.pool())
        .await
        .with_context(|| format!("load apply source batch {source_id}"))?;
        let Some(row) = row else {
            bail!(
                "completed memory job {} source batch {source_id} is missing; no authenticated GC/tombstone permits discard",
                job.id
            );
        };
        let version: i64 = row.try_get("version")?;
        let state: String = row.try_get("state")?;

        let expected =
            job.source_versions.get(source_id).copied().ok_or_else(|| {
                anyhow!("source version missing for {source_id} in job {}", job.id)
            })?;
        if version != expected || state != MemoryBatchState::Compacted.as_str() {
            return discard_stale_completed_job(
                store,
                job,
                Some((&target_id, target_version, &target_state)),
            )
            .await;
        }

        let batch_uuid = BatchId::parse_str(source_id)
            .with_context(|| format!("source batch id {source_id} is not a UUID"))?;
        expected_source_versions.insert(batch_uuid, version as u64);

        batch_mutations.push(MemoryBatchMutation {
            batch_id: batch_uuid,
            expected_version: version as u64,
            new_state: MemoryBatchState::Dropped,
            summary: None,
            est_tokens: 0,
            footprint_delta: 0,
        });
    }

    let target_uuid = BatchId::parse_str(&target_id)
        .with_context(|| format!("target batch id {target_id} is not a UUID"))?;
    expected_source_versions.insert(target_uuid, target_version as u64);

    batch_mutations.push(MemoryBatchMutation {
        batch_id: target_uuid,
        expected_version: target_version as u64,
        new_state: MemoryBatchState::Promoted,
        summary: None,
        est_tokens: u64::try_from(target_est_tokens)
            .with_context(|| format!("target batch {target_id} est_tokens out of range"))?,
        footprint_delta: 0,
    });

    let transition = MemoryTransition {
        expected_source_versions,
        batch_mutations,
        job_mutations: vec![MemoryJobMutation::Apply {
            job_id: job.id.clone(),
            expected_attempt: job.attempts,
            lease_witness: job.lease_until.clone(),
        }],
        ..Default::default()
    };

    let batch = EventBatch {
        writes: vec![EventWrite {
            event: Some(DurableEvent::memory_maintenance("compact_applied")?),
            projections: vec![Projection::MemoryTransition(transition)],
        }],
        injected_commands: Vec::new(),
    };

    EventWriter::new(store.clone()).apply(batch).await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::{Command, CommandEnvelope, CommandId, InboundCommand};
    use crate::memory::{ThreeLayerMemory, estimate::eviction_footprint_for_payload};
    use crate::provider::types::{
        ApiProtocol, AssistantMessage, ProviderContextAnchor, ProviderContextFragment,
        ProviderContextItem, ProviderContextPayload, PublicAssistantMessage, PublicMessage,
        ToolCall, ToolDefinition, ToolInvocationRoute, ToolResultMessage, Usage,
    };
    use crate::runtime::contracts::{
        GenerationRecoveryFence, IncomingProvenance, ProcessGeneration, ProcessGenerationLease,
    };
    use crate::store::{
        ApplicationKind, DataKeyPurpose, HydrationOutcome, InjectedCommand,
        MemoryBatchMessageRecord, RunPhase, TranscriptRecord,
    };
    use chrono::TimeZone;
    use serde_json::json;
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::{Notify, mpsc};

    const PERSONALITY_AGENT_ID: &str = "0198f0f4-9b72-7000-8000-000000000001";

    fn timestamp() -> DateTime<Utc> {
        Utc.timestamp_nanos(1_700_000_000_123_456_789)
    }

    fn chat_model() -> ModelSpec {
        ModelSpec::preset("kimi-k3").expect("test model")
    }

    fn user(text: &str) -> PublicMessage {
        PublicMessage::User(UserMessage {
            incoming_source: None,
            incoming_timing: None,
            content: vec![UserContent::Text { text: text.into() }],
            timestamp: timestamp(),
        })
    }

    fn assistant(text: &str, reason: StopReason) -> AssistantMessage {
        let spec = chat_model();
        AssistantMessage {
            content: vec![AssistantContent::Text {
                text: text.into(),
                wire_item_index: 0,
            }],
            model: spec.id.clone(),
            provider: spec.provider.clone(),
            origin: spec.origin(),
            usage: Usage::default(),
            stop_reason: reason,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: timestamp(),
        }
    }

    fn public_assistant(text: &str) -> PublicMessage {
        let message = assistant(text, StopReason::Stop);
        PublicMessage::Assistant(PublicAssistantMessage {
            content: vec![crate::provider::types::PublicAssistantContent::Text {
                text: text.into(),
                wire_item_index: 0,
            }],
            model: message.model,
            provider: message.provider,
            origin: message.origin,
            usage: message.usage,
            stop_reason: message.stop_reason,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: message.timestamp,
        })
    }

    fn snapshot(messages: Vec<ContextMessage>) -> ParentContextSnapshot {
        let prompt = PromptContext::new(
            "Sumi's continuing context".into(),
            vec![],
            messages,
            vec![],
            vec![],
        );
        ParentContextSnapshot::capture(&prompt, &chat_model(), &RequestOptions::default())
    }

    fn persisted(id: &str, seq: u64, message: PublicMessage) -> ContextMessage {
        ContextMessage::Persisted {
            id: id.into(),
            seq,
            message: message.into(),
        }
    }

    fn input() -> CompactionInput {
        CompactionInput::from_parent(
            snapshot(vec![persisted("target", 1, user("first experience"))]),
            Uuid::now_v7(),
            4,
            &[("target".into(), 1)],
        )
        .expect("target in parent")
    }

    #[derive(Default)]
    struct FakeProvider {
        text: String,
        fail: bool,
        tool: bool,
        calls: AtomicUsize,
        observed: Mutex<Option<(ModelSpec, PromptContext, RequestOptions)>>,
        started: Notify,
        finish: Option<Arc<Notify>>,
    }

    impl CompactProvider for FakeProvider {
        fn start(
            &self,
            spec: ModelSpec,
            context: PromptContext,
            options: RequestOptions,
            cancel: CancellationToken,
        ) -> ProviderEventStream {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.observed.lock().expect("observed") = Some((spec.clone(), context, options));
            self.started.notify_one();
            let (tx, rx) = mpsc::channel(8);
            let text = self.text.clone();
            let fail = self.fail;
            let tool = self.tool;
            let finish = self.finish.clone();
            let output_spec = spec.clone();
            let task = tokio::spawn(async move {
                if let Some(finish) = finish {
                    finish.notified().await;
                }
                tx.send(ProviderEvent::Start).await.expect("start");
                if tool {
                    let _ = tx
                        .send(ProviderEvent::ToolCallStart { content_index: 0 })
                        .await;
                    return;
                }
                if fail {
                    return;
                }
                tx.send(ProviderEvent::TextStart { content_index: 0 })
                    .await
                    .expect("text start");
                tx.send(ProviderEvent::TextDelta {
                    content_index: 0,
                    delta: text.clone(),
                })
                .await
                .expect("text delta");
                tx.send(ProviderEvent::TextEnd {
                    content_index: 0,
                    content: text.clone(),
                })
                .await
                .expect("text end");
                tx.send(ProviderEvent::Done {
                    reason: StopReason::Stop,
                    output: ProviderOutput {
                        message: AssistantMessage {
                            model: output_spec.id.clone(),
                            provider: output_spec.provider.clone(),
                            origin: output_spec.origin(),
                            ..assistant(&text, StopReason::Stop)
                        },
                        provider_context: vec![],
                    },
                })
                .await
                .expect("done");
            });
            ProviderEventStream::new(rx, cancel, spec.provider.clone(), spec.origin())
                .own_producer(task)
        }
    }

    #[tokio::test]
    async fn fork_keeps_full_parent_and_settings_and_identifies_exact_target() {
        let spec = ModelSpec::preset("openai-responses").expect("Responses model");
        let mut prompt = input().parent.prompt().clone();
        let image = UserContent::Image {
            data: "pixel data".into(),
            mime_type: "image/png".into(),
        };
        if let ContextMessage::Persisted {
            message: Message::User(user),
            ..
        } = &mut prompt.messages[0]
        {
            user.content.push(image.clone());
        }
        let call = ToolCall {
            id: "call-exact".into(),
            name: "read_file".into(),
            route: ToolInvocationRoute::Elevated,
            arguments: serde_json::from_value(json!({"path":"/observed/file"})).expect("arguments"),
        };
        let mut observed = AssistantMessage {
            model: spec.id.clone(),
            provider: spec.provider.clone(),
            origin: spec.origin(),
            ..assistant("I read it", StopReason::ToolUse)
        };
        observed.content.insert(
            0,
            AssistantContent::Thinking {
                thinking: "earlier reasoning retained".into(),
                signature_field: "signature".into(),
                wire_item_index: 0,
            },
        );
        observed.content.push(AssistantContent::ToolCall {
            tool_call: call,
            wire_item_index: 2,
        });
        prompt.messages.push(ContextMessage::Persisted {
            id: "call-message".into(),
            seq: 2,
            message: Message::Assistant(observed),
        });
        prompt.messages.push(ContextMessage::Persisted {
            id: "result-message".into(),
            seq: 3,
            message: Message::ToolResult(ToolResultMessage {
                tool_call_id: "call-exact".into(),
                tool_name: "read_file".into(),
                content: vec![UserContent::Text {
                    text: "actual words".into(),
                }],
                details: json!({"observed":null}),
                is_error: false,
                timestamp: timestamp(),
            }),
        });
        prompt.messages.push(persisted(
            "later-correction",
            4,
            user("It meant something different."),
        ));
        prompt.tools.push(ToolDefinition {
            name: "read_file".into(),
            description: "Read original".into(),
            parameters: json!({"type":"object"}),
        });
        prompt.provider_context.push(ProviderContextItem {
            retention_owner: ProviderContextAnchor {
                message_id: "call-message".into(),
                message_seq: 2,
            },
            origin_message: None,
            wire_item_index: Some(1),
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::OpenAiResponses,
                item: json!({"type":"reasoning","id":"rs-parent","encrypted_content":"same-parent-state","summary":[]}),
            },
        });
        let options = RequestOptions {
            max_tokens: Some(5000),
            temperature: Some(0.3),
            reasoning_effort: Some("high".into()),
            tool_choice: Some(json!("auto")),
            ..Default::default()
        };
        let parent = ParentContextSnapshot::capture(&prompt, &spec, &options);
        let input = CompactionInput::from_parent(
            parent,
            Uuid::now_v7(),
            7,
            &[
                ("target".into(), 1),
                ("call-message".into(), 2),
                ("result-message".into(), 3),
            ],
        )
        .expect("input");
        let provider = FakeProvider {
            text: "Retained meaning".into(),
            ..Default::default()
        };
        let cancel = CancellationToken::new();
        let result = compact(&input, cancel.clone(), &provider)
            .await
            .expect("complete")
            .expect("replacement");
        assert!(
            !cancel.is_cancelled(),
            "fork drop must not cancel its owner"
        );
        assert_eq!(result.time_range, (timestamp(), timestamp()));
        let observed = provider.observed.lock().unwrap();
        let (actual_spec, fork, actual_options) = observed.as_ref().unwrap();
        assert_eq!(actual_spec, &spec);
        assert_eq!(actual_options, &options);
        assert_eq!(fork.system_prompt, prompt.system_prompt);
        assert_eq!(fork.tools, prompt.tools);
        assert_eq!(fork.provider_context, prompt.provider_context);
        assert_eq!(
            &fork.messages[..prompt.messages.len()],
            prompt.messages.as_slice()
        );
        let ContextMessage::Synthetic {
            message: Message::User(directive),
        } = fork.messages.last().unwrap()
        else {
            panic!("directive")
        };
        assert!(directive.content.contains(&image));
        let UserContent::Text { text } = &directive.content[0] else {
            panic!("target text")
        };
        assert!(text.contains("call-exact"));
        assert!(text.contains("elevated"));
        assert!(text.contains("2023-11-14T22:13:20.123456789Z"));
        assert!(text.contains("actual words"));
        assert!(
            !text.contains("same-parent-state"),
            "opaque state stays only in parent replay positions"
        );
    }

    #[test]
    fn target_must_exist_wholly_and_in_order_in_parent() {
        let parent = input().parent;
        assert!(
            CompactionInput::from_parent(
                parent.clone(),
                Uuid::now_v7(),
                0,
                &[("missing".into(), 1)]
            )
            .is_err()
        );
        assert!(
            CompactionInput::from_parent(
                parent,
                Uuid::now_v7(),
                0,
                &[("target".into(), 1), ("target".into(), 1)]
            )
            .is_err()
        );
    }

    #[test]
    fn incomplete_output_and_tool_calls_never_replace_memory() {
        for reason in [
            StopReason::Length,
            StopReason::Error,
            StopReason::ToolUse,
            StopReason::Aborted,
        ] {
            assert!(
                result_from_output(
                    ProviderOutput {
                        message: assistant("partial", reason),
                        provider_context: vec![]
                    },
                    &input()
                )
                .is_err()
            );
        }
        assert!(
            result_from_output(
                ProviderOutput {
                    message: assistant(" ", StopReason::Stop),
                    provider_context: vec![]
                },
                &input()
            )
            .is_err()
        );
        let long = "Distinct experience retained. ".repeat(1500);
        assert_eq!(
            result_from_output(
                ProviderOutput {
                    message: assistant(&long, StopReason::Stop),
                    provider_context: vec![]
                },
                &input()
            )
            .unwrap()
            .unwrap()
            .summary
            .expose(),
            long
        );
        assert!(
            result_from_output(
                ProviderOutput {
                    message: assistant(" KEEP_UNCHANGED\n", StopReason::Stop),
                    provider_context: vec![]
                },
                &input()
            )
            .unwrap()
            .is_none()
        );
    }

    #[tokio::test]
    async fn tool_attempt_is_stopped_at_provider_boundary() {
        let provider = FakeProvider {
            tool: true,
            ..Default::default()
        };
        assert!(matches!(
            compact(&input(), CancellationToken::new(), &provider).await,
            Err(CompactError::ToolRequested)
        ));
    }

    async fn test_store() -> Arc<Store> {
        Arc::new(
            Store::session_test_store(PERSONALITY_AGENT_ID)
                .await
                .expect("open test store"),
        )
    }

    async fn insert_memory_fixture(store: &Store, kind: &str, transition: MemoryTransition) {
        EventWriter::new(Arc::new(store.clone()))
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
            .expect("insert authenticated memory fixture");
    }

    async fn insert_l0_batch(store: &Store, messages: &[PublicMessage]) -> (String, String) {
        insert_l0_batch_with_seq_and_event_evidence(store, 1, messages, false).await
    }

    async fn insert_l0_batch_with_seq(
        store: &Store,
        batch_seq: i64,
        messages: &[PublicMessage],
    ) -> (String, String) {
        insert_l0_batch_with_seq_and_event_evidence(store, batch_seq, messages, false).await
    }

    async fn insert_authenticated_l0_batch_with_seq(
        store: &Store,
        batch_seq: i64,
        messages: &[PublicMessage],
    ) -> (String, String) {
        insert_l0_batch_with_seq_and_event_evidence(store, batch_seq, messages, true).await
    }

    async fn insert_l0_batch_with_seq_and_event_evidence(
        store: &Store,
        batch_seq: i64,
        messages: &[PublicMessage],
        authenticate_messages: bool,
    ) -> (String, String) {
        // This compactor fixture deliberately seeds transcript rows at
        // synthetic sequences. Provider-context erasure fixtures additionally
        // seed exact MessageEnd evidence before the first subsequent
        // EventWriter transaction; ordinary compactor fixtures keep the
        // lighter raw-transcript setup.
        EventWriter::new(Arc::new(store.clone()))
            .initialize_recovery_checkpoint()
            .await
            .expect("initialize fixture EventWriter checkpoint");
        let key = store
            .private_key(DataKeyPurpose::Transcript)
            .await
            .expect("transcript key");
        let redactor = store.redactor();
        let scope = store.scope();

        let source_id = Uuid::now_v7().to_string();
        let batch = MemoryBatchRecord::new(
            &source_id,
            MemoryLayer::L0,
            0,
            batch_seq,
            MemoryBatchState::Compacting,
            100,
            0,
        );
        let target_id = Uuid::now_v7().to_string();
        let target = MemoryBatchRecord::new(
            &target_id,
            MemoryLayer::L1,
            0,
            batch_seq,
            MemoryBatchState::Compacting,
            0,
            0,
        );
        let mut membership_inserts = Vec::with_capacity(messages.len());
        let mut event_owners = Vec::with_capacity(messages.len());
        for (seq, message) in messages.iter().enumerate() {
            let message_id = format!("{source_id}-msg-{seq}");
            let message_seq = (batch_seq as u64)
                .saturating_mul(100)
                .saturating_add(seq as u64);
            let record =
                TranscriptRecord::encrypt(message, &message_id, message_seq, &key, scope, redactor)
                    .expect("encrypt message");
            record.insert(store.pool()).await.expect("insert message");
            membership_inserts.push(MemoryBatchMessageRecord {
                batch_id: source_id.clone(),
                message_id: record.id().to_owned(),
                ord: i64::try_from(seq + 1).expect("fixture membership ordinal"),
            });
            if authenticate_messages {
                assert!(
                    matches!(message, PublicMessage::Assistant(_)),
                    "authenticated provider-context fixture owners must be assistants"
                );
                event_owners.push((message_id, message_seq));
            }
        }
        if authenticate_messages {
            let owner_refs = event_owners
                .iter()
                .map(|(message_id, message_seq)| (message_id.as_str(), *message_seq))
                .collect::<Vec<_>>();
            crate::store::seed_provider_context_owner_event_evidence(store, &owner_refs)
                .await
                .expect("seed authenticated provider-context owner events");
        }

        insert_memory_fixture(
            store,
            "fixture_l0_batches",
            MemoryTransition {
                batch_inserts: vec![batch, target],
                membership_inserts,
                ..Default::default()
            },
        )
        .await;

        (source_id, target_id)
    }

    async fn insert_compact_l0_job(store: &Store, job_id: &str, source_id: &str, batch_seq: i64) {
        let target_id: String =
            sqlx::query_scalar("SELECT id FROM memory_batches WHERE layer = 1 AND batch_seq = ?")
                .bind(batch_seq)
                .fetch_one(store.pool())
                .await
                .expect("load compact-l0 target");
        let source_versions = BTreeMap::from([(source_id.to_owned(), 0), (target_id, 0)]);
        let job = MemoryJobRecord::new(
            job_id,
            MemoryJobKind::CompactL0,
            batch_seq,
            vec![source_id.to_owned()],
            source_versions,
        );
        insert_memory_fixture(
            store,
            "fixture_compact_l0_job",
            MemoryTransition {
                job_inserts: vec![job],
                ..Default::default()
            },
        )
        .await;
    }

    fn provenance(store: &Store) -> IncomingProvenance {
        IncomingProvenance::new(
            "tenant-1",
            store.scope().personality_agent_id.clone(),
            "human-1",
        )
        .expect("provenance")
    }
    async fn seed_completed_authenticated_turn(
        store: &Arc<Store>,
        spec: &ModelSpec,
        user_text: &str,
        assistant_message_id: &str,
        assistant: PublicMessage,
        provider_context: Vec<ProviderContextFragment>,
    ) {
        let eviction_footprint_tokens = provider_context
            .iter()
            .try_fold(0_u64, |total, fragment| {
                let footprint = eviction_footprint_for_payload(spec, &fragment.payload)?;
                total
                    .checked_add(footprint.eviction_tokens())
                    .ok_or(crate::memory::estimate::EstimateError::ArithmeticOverflow)
            })
            .expect("fixture provider-context footprint");
        let command_uuid = Uuid::now_v7().to_string();
        let command_id = command_uuid.as_str();
        let command_seq =
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq), 0) + 1 FROM inbound_commands")
                .fetch_one(store.pool())
                .await
                .expect("next command sequence") as u64;
        let run_id = format!("run-{command_id}");
        let turn_id = format!("turn-{command_id}");
        let envelope = CommandEnvelope {
            seq: command_seq,
            command_id: CommandId::parse(command_id).expect("fixture command id"),
            personality_agent_id: store.scope().personality_agent_id.clone(),
            provenance: provenance(store),
            command: Command::UserMessage {
                text: user_text.to_owned(),
                attachments: Vec::new(),
            },
        };
        let writer = EventWriter::new(store.clone());
        writer
            .persist_inbound(&InboundCommand::Valid(envelope))
            .await
            .expect("persist fixture command");
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: command_id.to_owned(),
                        application_kind: ApplicationKind::IdleRun,
                        run_id: run_id.clone(),
                        turn_id: turn_id.clone(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("classify fixture command");

        let user_id =
            crate::store::user_message_id(&store.scope().personality_agent_id, command_id);
        let received_at: String =
            sqlx::query_scalar("SELECT received_at FROM inbound_commands WHERE command_id = ?")
                .bind(command_id)
                .fetch_one(store.pool())
                .await
                .expect("fixture command received_at");
        let timing_json: Option<String> = sqlx::query_scalar(
            "SELECT incoming_timing_json FROM inbound_commands WHERE command_id = ?",
        )
        .bind(command_id)
        .fetch_one(store.pool())
        .await
        .expect("fixture timing");
        let user = PublicMessage::User(UserMessage {
            incoming_source: None,
            incoming_timing: timing_json
                .map(|json| serde_json::from_str(&json).expect("fixture timing json")),
            content: vec![UserContent::Text {
                text: user_text.to_owned(),
            }],
            timestamp: DateTime::parse_from_rfc3339(&received_at)
                .expect("fixture received_at")
                .with_timezone(&Utc),
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
                            command_id: command_id.to_owned(),
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
                            command_id: command_id.to_owned(),
                            run_id: run_id.clone(),
                            expected: RunPhase::RunStarted,
                            next: RunPhase::TurnStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_start", &user_id, &user)
                                .expect("fixture user MessageStart"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: command_id.to_owned(),
                            run_id: run_id.clone(),
                            expected: RunPhase::TurnStarted,
                            next: RunPhase::UserStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_end", &user_id, &user)
                                .expect("fixture user MessageEnd"),
                        ),
                        projections: vec![
                            Projection::MessageEnd {
                                message_id: user_id,
                                role: "user",
                                message: user,
                                append_to_l0: true,
                                provider_context: Vec::new(),
                                eviction_footprint_tokens: 0,
                            },
                            Projection::RunPhase {
                                command_id: command_id.to_owned(),
                                run_id: run_id.clone(),
                                expected: RunPhase::UserStarted,
                                next: RunPhase::UserCommitted,
                            },
                        ],
                    },
                ],
                injected_commands: vec![InjectedCommand::new(
                    command_seq,
                    CommandId::parse(command_id).expect("fixture command id"),
                    provenance(store),
                )],
            })
            .await
            .expect("commit fixture user injection");

        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message_in_turn(
                            "message_start",
                            assistant_message_id,
                            &assistant,
                            Some(run_id.clone()),
                            Some(turn_id.clone()),
                        )
                        .expect("fixture assistant MessageStart"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: command_id.to_owned(),
                        run_id: run_id.clone(),
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
                                assistant_message_id,
                                &assistant,
                                Some(run_id.clone()),
                                Some(turn_id.clone()),
                            )
                            .expect("fixture assistant MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: assistant_message_id.to_owned(),
                            role: "assistant",
                            message: assistant.clone(),
                            append_to_l0: true,
                            provider_context,
                            eviction_footprint_tokens,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::turn_end(run_id.clone(), turn_id, assistant, Vec::new())
                                .expect("fixture TurnEnd"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::agent_end(run_id.clone()).expect("fixture AgentEnd"),
                        ),
                        projections: vec![Projection::CommandApplied {
                            command_id: command_id.to_owned(),
                            command_seq,
                            run_id: Some(run_id),
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("commit fixture assistant terminal");
    }

    #[tokio::test]
    async fn abandoned_higher_compaction_restores_source_across_hydration() {
        for completed in [false, true] {
            let store = test_store().await;
            let parent = real_parent_with_queued_target(&store).await;
            let expected = "The original experience remains available.";
            let provider = FakeProvider {
                text: expected.into(),
                ..Default::default()
            };
            assert!(
                compact_next_l0_with_provider(
                    store.clone(),
                    parent,
                    CancellationToken::new(),
                    &provider
                )
                .await
                .unwrap()
            );
            let row = sqlx::query(
                "SELECT * FROM memory_jobs WHERE kind = 'compact_l0' AND status = 'completed'",
            )
            .fetch_one(store.pool())
            .await
            .unwrap();
            let original_job = parse_job(&row).unwrap();
            assert!(
                apply_completed_job(store.clone(), &original_job)
                    .await
                    .unwrap()
            );
            let row = sqlx::query("SELECT id, version, est_tokens, summary_ciphertext FROM memory_batches WHERE layer = 1 AND state = 'promoted'")
                .fetch_one(store.pool()).await.unwrap();
            let source: String = row.get("id");
            let source_version: i64 = row.get("version");
            let source_estimate: i64 = row.get("est_tokens");
            let source_ciphertext: Vec<u8> = row.get("summary_ciphertext");
            let target = Uuid::now_v7().to_string();
            let job_id = Uuid::now_v7().to_string();
            insert_memory_fixture(
                &store,
                "fixture_higher_compaction",
                MemoryTransition {
                    expected_source_versions: BTreeMap::from([(
                        Uuid::parse_str(&source).unwrap(),
                        source_version as u64,
                    )]),
                    batch_mutations: vec![MemoryBatchMutation {
                        batch_id: Uuid::parse_str(&source).unwrap(),
                        expected_version: source_version as u64,
                        new_state: MemoryBatchState::Compacting,
                        summary: None,
                        est_tokens: 0,
                        footprint_delta: 0,
                    }],
                    batch_inserts: vec![MemoryBatchRecord::new(
                        &target,
                        MemoryLayer::L2,
                        1,
                        1,
                        MemoryBatchState::Compacting,
                        0,
                        0,
                    )],
                    job_inserts: vec![MemoryJobRecord::new(
                        &job_id,
                        MemoryJobKind::CompactL1,
                        1,
                        vec![source.clone()],
                        BTreeMap::from([(source.clone(), source_version + 1), (target.clone(), 0)]),
                    )],
                    ..Default::default()
                },
            )
            .await;
            insert_memory_fixture(
                &store,
                "fixture_claim_higher",
                MemoryTransition {
                    job_mutations: vec![MemoryJobMutation::Claim {
                        job_id: job_id.clone(),
                        lease_until: (Utc::now() + LEASE_DURATION).to_rfc3339(),
                    }],
                    ..Default::default()
                },
            )
            .await;
            let row = sqlx::query("SELECT * FROM memory_jobs WHERE id = ?")
                .bind(&job_id)
                .fetch_one(store.pool())
                .await
                .unwrap();
            let job = parse_job(&row).unwrap();
            // A source at the same state but a later version belongs to a
            // successor. The failure path must not restore that source.
            let mut obsolete = parse_job(&row).unwrap();
            *obsolete.source_versions.get_mut(&source).unwrap() -= 1;
            let mut guards = BTreeMap::new();
            let mut mutations = Vec::new();
            restore_abandoned_sources(
                &store,
                &obsolete,
                MemoryBatchState::Compacting,
                &mut guards,
                &mut mutations,
            )
            .await
            .unwrap();
            assert!(
                mutations.is_empty(),
                "stale version must not regain ownership"
            );
            if completed {
                let candidate = CompactResult {
                    summary: DecryptedMemorySummary::new("Abandoned output".into()),
                    est_tokens: 5,
                    time_range: (timestamp(), timestamp()),
                };
                assert!(complete_job(&store, &job, &candidate).await.unwrap());
                let row = sqlx::query("SELECT * FROM memory_jobs WHERE id = ?")
                    .bind(&job_id)
                    .fetch_one(store.pool())
                    .await
                    .unwrap();
                let job = parse_job(&row).unwrap();
                let target_row = load_target_batch(&store, job.kind, job.batch_seq)
                    .await
                    .unwrap()
                    .unwrap();
                discard_stale_completed_job(
                    store.clone(),
                    &job,
                    Some((&target, target_row.version, target_row.state.as_str())),
                )
                .await
                .unwrap();
            } else {
                let target_row = load_target_batch(&store, job.kind, job.batch_seq)
                    .await
                    .unwrap()
                    .unwrap();
                supersede_stale_job(&store, &job, &target_row)
                    .await
                    .unwrap();
            }
            let row = sqlx::query(
                "SELECT state, est_tokens, summary_ciphertext FROM memory_batches WHERE id = ?",
            )
            .bind(&source)
            .fetch_one(store.pool())
            .await
            .unwrap();
            assert_eq!(row.get::<String, _>("state"), "promoted");
            assert_eq!(row.get::<i64, _>("est_tokens"), source_estimate);
            assert_eq!(
                row.get::<Vec<u8>, _>("summary_ciphertext"),
                source_ciphertext
            );
            assert_eq!(
                sqlx::query_scalar::<_, String>("SELECT state FROM memory_batches WHERE id = ?")
                    .bind(&target)
                    .fetch_one(store.pool())
                    .await
                    .unwrap(),
                "dropped"
            );
            let memory = ThreeLayerMemory::from_hydrated(hydrate(&store).await.memory).unwrap();
            assert_eq!(memory.l1().len(), 1);
            assert_eq!(memory.l1()[0].summary.expose(), expected);
            assert_eq!(memory.l1()[0].est_tokens, source_estimate as u64);
            assert!(memory.l2().summary.expose().is_empty());
        }
    }

    #[tokio::test]
    async fn failure_releases_job_and_fresh_parent_can_retry() {
        let store = test_store().await;
        let original = user("original observation");
        let (source, _) = insert_l0_batch(&store, std::slice::from_ref(&original)).await;
        let job_id = Uuid::now_v7().to_string();
        insert_compact_l0_job(&store, &job_id, &source, 1).await;
        let parent = snapshot(vec![persisted(&format!("{source}-msg-0"), 100, original)]);
        let failing = FakeProvider {
            fail: true,
            ..Default::default()
        };
        assert!(
            compact_next_l0_with_provider(
                store.clone(),
                parent.clone(),
                CancellationToken::new(),
                &failing
            )
            .await
            .is_err()
        );
        let row = sqlx::query("SELECT status, attempts, lease_until FROM memory_jobs WHERE id = ?")
            .bind(&job_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(row.get::<String, _>("status"), "pending");
        assert_eq!(row.get::<i64, _>("attempts"), 1);
        assert!(row.get::<Option<String>, _>("lease_until").is_none());
        let retry = FakeProvider {
            text: "Observation preserved with less repetition".into(),
            ..Default::default()
        };
        assert!(
            compact_next_l0_with_provider(store, parent, CancellationToken::new(), &retry)
                .await
                .unwrap()
        );
        assert_eq!(retry.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unchanged_retains_original_and_does_not_block_later_target() {
        let store = test_store().await;
        let first = user("These exact words should stay.");
        let second = user("repetitive second observation");
        let (first_source, _) = insert_l0_batch(&store, std::slice::from_ref(&first)).await;
        let (second_source, _) =
            insert_l0_batch_with_seq(&store, 2, std::slice::from_ref(&second)).await;
        let first_job = Uuid::now_v7().to_string();
        let second_job = Uuid::now_v7().to_string();
        insert_compact_l0_job(&store, &first_job, &first_source, 1).await;
        insert_compact_l0_job(&store, &second_job, &second_source, 2).await;
        let parent = snapshot(vec![
            persisted(&format!("{first_source}-msg-0"), 100, first),
            persisted(&format!("{second_source}-msg-0"), 200, second),
        ]);
        let unchanged = FakeProvider {
            text: KEEP_UNCHANGED.into(),
            ..Default::default()
        };
        assert!(
            !compact_next_l0_with_provider(
                store.clone(),
                parent.clone(),
                CancellationToken::new(),
                &unchanged
            )
            .await
            .unwrap()
        );
        let summary = FakeProvider {
            text: "The later observation retained.".into(),
            ..Default::default()
        };
        assert!(
            compact_next_l0_with_provider(
                store.clone(),
                parent.clone(),
                CancellationToken::new(),
                &summary
            )
            .await
            .unwrap()
        );
        insert_pressure_batch(&store, 3, super::super::L0_LIMIT).await;
        assert_eq!(apply_ready_memory(store.clone()).await.unwrap(), 1);
        let first_state: String =
            sqlx::query_scalar("SELECT state FROM memory_batches WHERE id = ?")
                .bind(&first_source)
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(first_state, "sealed");
        let statuses: Vec<String> =
            sqlx::query_scalar("SELECT status FROM memory_jobs ORDER BY batch_seq")
                .fetch_all(store.pool())
                .await
                .unwrap();
        assert_eq!(statuses, vec!["unchanged", "applied"]);
        let retained: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM memory_batch_messages WHERE batch_id = ?")
                .bind(&first_source)
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(retained, 1);
        assert!(
            !compact_next_l0_with_provider(store, parent, CancellationToken::new(), &summary)
                .await
                .unwrap()
        );
        assert_eq!(
            summary.calls.load(Ordering::SeqCst),
            1,
            "unchanged does not requeue"
        );
    }

    #[tokio::test]
    async fn unavailable_older_target_does_not_force_a_partial_parent_or_block_an_eligible_target()
    {
        let store = test_store().await;
        let (old_source, _) =
            insert_l0_batch(&store, &[user("old source absent from parent")]).await;
        let current = user("current source present");
        let (source, _) = insert_l0_batch_with_seq(&store, 2, std::slice::from_ref(&current)).await;
        insert_compact_l0_job(&store, &Uuid::now_v7().to_string(), &old_source, 1).await;
        insert_compact_l0_job(&store, &Uuid::now_v7().to_string(), &source, 2).await;
        let parent = snapshot(vec![persisted(&format!("{source}-msg-0"), 200, current)]);
        let provider = FakeProvider {
            text: "current meaning preserved".into(),
            ..Default::default()
        };
        assert!(
            compact_next_l0_with_provider(
                store.clone(),
                parent,
                CancellationToken::new(),
                &provider
            )
            .await
            .unwrap()
        );
        let old_attempts: i64 =
            sqlx::query_scalar("SELECT attempts FROM memory_jobs WHERE batch_seq = 1")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(old_attempts, 0);
        insert_pressure_batch(&store, 3, super::super::L0_LIMIT).await;
        assert_eq!(apply_ready_memory(store).await.unwrap(), 1);
    }

    async fn insert_pressure_batch(store: &Store, seq: i64, tokens: u64) {
        insert_memory_fixture(
            store,
            "test_pressure",
            MemoryTransition {
                batch_inserts: vec![MemoryBatchRecord::new(
                    Uuid::now_v7().to_string(),
                    MemoryLayer::L0,
                    0,
                    seq,
                    MemoryBatchState::Sealed,
                    i64::try_from(tokens).unwrap(),
                    0,
                )],
                ..Default::default()
            },
        )
        .await;
    }

    async fn hydrate(store: &Store) -> crate::store::HydratedRunState {
        let lease = ProcessGenerationLease::new(
            store.scope().personality_agent_id.clone(),
            ProcessGeneration::from_wire(41).expect("generation"),
            "memory-test-lease",
        )
        .expect("lease");
        let fence = GenerationRecoveryFence::new(&lease, "memory-test-fence").expect("fence");
        match store
            .hydrate(&lease, &fence)
            .await
            .expect("hydrate authenticated memory")
        {
            HydrationOutcome::Complete(hydrated) => hydrated,
            other => panic!("completed runs must hydrate: {other:?}"),
        }
    }

    async fn real_parent_with_queued_target(store: &Arc<Store>) -> ParentContextSnapshot {
        let spec = chat_model();
        let repeated =
            "Observed path: /workspace/source. The value was unchanged on this read. ".repeat(1800);
        seed_completed_authenticated_turn(
            store,
            &spec,
            "Read and observe the original.",
            "assistant-original",
            public_assistant(&repeated),
            vec![],
        )
        .await;
        seed_completed_authenticated_turn(
            store,
            &spec,
            "Now continue with me.",
            "assistant-current",
            public_assistant("I am here."),
            vec![],
        )
        .await;
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM memory_jobs WHERE status = 'pending' AND kind = 'compact_l0'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert!(pending >= 1, "real user boundary seals old L0");
        snapshot(hydrate(store).await.messages)
    }

    #[tokio::test]
    async fn applying_slow_fork_keeps_new_real_user_correction_after_hydration() {
        let store = test_store().await;
        let parent = real_parent_with_queued_target(&store).await;
        let finish = Arc::new(Notify::new());
        let provider = Arc::new(FakeProvider {
            text: "I observed /workspace/source repeatedly with unchanged values.".into(),
            finish: Some(finish.clone()),
            ..Default::default()
        });
        let task_store = store.clone();
        let task_provider = provider.clone();
        let task = tokio::spawn(async move {
            compact_next_l0_with_provider(
                task_store,
                parent,
                CancellationToken::new(),
                task_provider.as_ref(),
            )
            .await
        });
        provider.started.notified().await;
        let correction = "Correction: the value changed afterward; keep that new fact.";
        seed_completed_authenticated_turn(
            &store,
            &chat_model(),
            correction,
            "assistant-correction",
            public_assistant("I understand the new observation."),
            vec![],
        )
        .await;
        seed_completed_authenticated_turn(
            &store,
            &chat_model(),
            "Continue observing.",
            "assistant-pressure",
            public_assistant(&"additional detail ".repeat(5000)),
            vec![],
        )
        .await;
        finish.notify_one();
        assert!(task.await.expect("fork task").expect("durable completion"));
        assert_eq!(apply_ready_memory(store.clone()).await.unwrap(), 1);
        let restored = hydrate(&store).await;
        assert!(restored.messages.iter().any(|message| matches!(message,
            ContextMessage::Persisted { message: Message::User(user), .. }
                if user.content.iter().any(|part| matches!(part, UserContent::Text { text } if text == correction))
        )), "new durable correction survives targeted application");
        let memory =
            ThreeLayerMemory::from_hydrated(restored.memory).expect("independent result hydrates");
        assert!(memory.l0().iter().flat_map(|batch| &batch.messages).any(|message| matches!(message,
            ContextMessage::Persisted { message: Message::User(user), .. }
                if user.content.iter().any(|part| matches!(part, UserContent::Text { text } if text == correction))
        )));
        assert!(
            memory
                .l1()
                .iter()
                .any(|entry| entry.summary.expose().contains("/workspace/source"))
        );
    }

    #[tokio::test]
    async fn ready_results_wait_at_exact_limit_and_apply_only_enough_oldest_chunks() {
        let store = test_store().await;
        let first = user("first observation");
        let second = user("second observation");
        let (a, _) = insert_l0_batch(&store, std::slice::from_ref(&first)).await;
        let (b, _) = insert_l0_batch_with_seq(&store, 2, std::slice::from_ref(&second)).await;
        insert_compact_l0_job(&store, &Uuid::now_v7().to_string(), &a, 1).await;
        insert_compact_l0_job(&store, &Uuid::now_v7().to_string(), &b, 2).await;
        let parent = snapshot(vec![
            persisted(&format!("{a}-msg-0"), 100, first),
            persisted(&format!("{b}-msg-0"), 200, second),
        ]);
        let provider = FakeProvider {
            text: "observation retained".into(),
            ..Default::default()
        };
        for _ in 0..2 {
            assert!(
                compact_next_l0_with_provider(
                    store.clone(),
                    parent.clone(),
                    CancellationToken::new(),
                    &provider
                )
                .await
                .unwrap()
            );
        }
        assert_eq!(apply_ready_memory(store.clone()).await.unwrap(), 0);
        insert_pressure_batch(&store, 3, super::super::L0_LIMIT - 200).await;
        assert_eq!(live_l0_tokens(&store).await.unwrap(), 40_000);
        assert_eq!(apply_ready_memory(store.clone()).await.unwrap(), 0);
        // No new fork runs or signals readiness when later input crosses the limit.
        insert_pressure_batch(&store, 4, 1).await;
        assert_eq!(apply_ready_memory(store.clone()).await.unwrap(), 1);
        let statuses: Vec<String> =
            sqlx::query_scalar("SELECT status FROM memory_jobs ORDER BY batch_seq")
                .fetch_all(store.pool())
                .await
                .unwrap();
        assert_eq!(statuses, ["applied", "completed"]);
        assert_eq!(live_l0_tokens(&store).await.unwrap(), 39_901);
        assert_eq!(apply_ready_memory(store.clone()).await.unwrap(), 0);
        insert_pressure_batch(&store, 5, 100).await;
        assert_eq!(apply_ready_memory(store.clone()).await.unwrap(), 1);
        let statuses: Vec<String> =
            sqlx::query_scalar("SELECT status FROM memory_jobs ORDER BY batch_seq")
                .fetch_all(store.pool())
                .await
                .unwrap();
        assert_eq!(statuses, ["applied", "applied"]);
        assert_eq!(live_l0_tokens(&store).await.unwrap(), 39_901);
    }

    #[tokio::test]
    async fn completed_shelf_preserves_original_prefix_across_hydration_until_later_input() {
        let store = test_store().await;
        let parent = real_parent_with_queued_target(&store).await;
        let before = hydrate(&store).await;
        assert!(live_l0_tokens(&store).await.unwrap() < super::super::L0_LIMIT);
        let provider = FakeProvider {
            text: "I observed the source repeatedly.".into(),
            ..Default::default()
        };
        assert!(
            compact_next_l0_with_provider(
                store.clone(),
                parent,
                CancellationToken::new(),
                &provider
            )
            .await
            .unwrap()
        );
        assert_eq!(apply_ready_memory(store.clone()).await.unwrap(), 0);
        let restored = hydrate(&store).await;
        assert_eq!(
            restored.messages, before.messages,
            "original native message identities and contents survive preparation/restart"
        );
        assert_eq!(restored.provider_context, before.provider_context);
        let memory = ThreeLayerMemory::from_hydrated(restored.memory).unwrap();
        assert!(
            memory.l1().is_empty(),
            "ready L1 remains off the parent prefix"
        );
        assert!(!memory.shelf.is_empty());
        let correction = "Correction: the source changed afterward.";
        seed_completed_authenticated_turn(
            &store,
            &chat_model(),
            correction,
            "later-pressure",
            public_assistant(&"new observed detail ".repeat(5000)),
            vec![],
        )
        .await;
        assert!(live_l0_tokens(&store).await.unwrap() > super::super::L0_LIMIT);
        assert_eq!(apply_ready_memory(store.clone()).await.unwrap(), 1);
        let after = hydrate(&store).await;
        assert!(after.messages.iter().any(|message| matches!(message,
            ContextMessage::Persisted { message: Message::User(user), .. }
                if user.content.iter().any(|part| matches!(part, UserContent::Text { text } if text == correction))
        )));
        assert_eq!(
            ThreeLayerMemory::from_hydrated(after.memory)
                .unwrap()
                .l1()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn driver_rearms_remaining_shelf_without_a_new_fork_completion() {
        use crate::agent::{InjectedRunDriver, RunCore, RunDriver};
        use crate::tools::{ToolRegistryBuilder, WorkspacePaths};
        let store = test_store().await;
        real_parent_with_queued_target(&store).await;
        let detail = "Another distinct observed source and unchanged values. ".repeat(2400);
        seed_completed_authenticated_turn(
            &store,
            &chat_model(),
            "Observe another source.",
            "second-source",
            public_assistant(&detail),
            vec![],
        )
        .await;
        seed_completed_authenticated_turn(
            &store,
            &chat_model(),
            "Continue.",
            "second-closed",
            public_assistant("Here."),
            vec![],
        )
        .await;
        let parent = snapshot(hydrate(&store).await.messages);
        let provider = FakeProvider {
            text: "Observation retained.".into(),
            ..Default::default()
        };
        for _ in 0..2 {
            assert!(
                compact_next_l0_with_provider(
                    store.clone(),
                    parent.clone(),
                    CancellationToken::new(),
                    &provider
                )
                .await
                .unwrap()
            );
        }
        let restored = hydrate(&store).await;
        let generation = ProcessGeneration::from_wire(41).unwrap();
        let lease = ProcessGenerationLease::new(
            store.scope().personality_agent_id.clone(),
            generation,
            "memory-test-lease",
        )
        .unwrap();
        let fence = GenerationRecoveryFence::new(&lease, "memory-test-fence").unwrap();
        let registry = ToolRegistryBuilder::default().build();
        let prompt = PromptContext::new(
            "fixture".into(),
            vec![],
            vec![],
            vec![],
            registry.definitions(),
        );
        let driver = InjectedRunDriver::with_stream_starter(
            chat_model(),
            RequestOptions::default(),
            Some(prompt),
            Some(registry),
            Some(WorkspacePaths::new("/workspace").unwrap()),
            Some(generation),
            Arc::new(|_, _, _, _, _| panic!("idle maintenance must not call the provider")),
        )
        .unwrap()
        .with_hydrated_memory(store.clone(), &lease, &fence, &restored)
        .unwrap();
        let mut core = RunCore::new();
        driver.memory_maintenance_ready().await; // boot notification
        assert!(
            driver
                .apply_idle_memory_maintenance(&mut core)
                .await
                .unwrap()
        );
        assert!(live_l0_tokens(&store).await.unwrap() <= super::super::L0_LIMIT);
        tokio::time::timeout(Duration::from_secs(1), driver.memory_maintenance_ready())
            .await
            .expect("successful partial application rearms the remaining shelf");
        assert!(
            !driver
                .apply_idle_memory_maintenance(&mut core)
                .await
                .unwrap()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), driver.memory_maintenance_ready())
                .await
                .is_err(),
            "waiting below the limit must not self-notify or busy-loop"
        );
        seed_completed_authenticated_turn(
            &store,
            &chat_model(),
            "A later correction.",
            "third-source",
            public_assistant(&detail),
            vec![],
        )
        .await;
        assert!(
            driver
                .apply_idle_memory_maintenance(&mut core)
                .await
                .unwrap()
        );
        let statuses: Vec<String> =
            sqlx::query_scalar("SELECT status FROM memory_jobs ORDER BY batch_seq")
                .fetch_all(store.pool())
                .await
                .unwrap();
        assert_eq!(
            statuses
                .iter()
                .filter(|status| status.as_str() == "applied")
                .count(),
            2
        );
        assert_eq!(
            provider.calls.load(Ordering::SeqCst),
            2,
            "later replacement did not need another fork"
        );
    }

    #[tokio::test]
    async fn unchanged_is_successful_durable_memory_after_restart() {
        let store = test_store().await;
        let parent = real_parent_with_queued_target(&store).await;
        let before = parent.prompt().messages.clone();
        let provider = FakeProvider {
            text: KEEP_UNCHANGED.into(),
            ..Default::default()
        };
        assert!(
            !compact_next_l0_with_provider(
                store.clone(),
                parent,
                CancellationToken::new(),
                &provider
            )
            .await
            .unwrap()
        );
        assert_eq!(apply_ready_memory(store.clone()).await.unwrap(), 0);
        let restored = hydrate(&store).await;
        let memory =
            ThreeLayerMemory::from_hydrated(restored.memory).expect("unchanged decision hydrates");
        let active = memory
            .l0()
            .iter()
            .flat_map(|batch| &batch.messages)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(active, before, "the original representations remain active");
        assert!(memory.l1().is_empty());
    }
}
