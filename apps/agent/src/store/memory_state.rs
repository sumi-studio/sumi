//! Durable memory layer state: batches, membership, compaction jobs, and
//! apply cursors.  This module owns the `memory_*` table shapes and the
//! transactional CAS primitives used by the memory maintainer.  It deliberately
//! does not contain compaction algorithms or L0/L1/L2 policy logic (T19-T21).

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, Transaction};
use uuid::Uuid;

use super::AgentScope;

const MEMORY_BATCH_DIGEST_DOMAIN: &[u8] = b"sumi-memory-batch/v1";
const MEMORY_JOB_DIGEST_DOMAIN: &[u8] = b"sumi-memory-job/v1";
const MEMORY_CURSOR_DIGEST_DOMAIN: &[u8] = b"sumi-memory-cursor/v1";
const MEMORY_CALIBRATION_DIGEST_DOMAIN: &[u8] = b"sumi-memory-calibration-row/v1";
const MEMORY_MEMBERSHIP_DIGEST_DOMAIN: &[u8] = b"sumi-memory-membership/v1";
pub(crate) const MEMORY_PROJECTION_DIGEST_BYTES: usize = 32;
pub(crate) const MEMORY_CALIBRATION_ID: &str = "prompt_token_ratio";

fn digest_field(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

fn digest_text(hash: &mut Sha256, value: &str) {
    digest_field(hash, value.as_bytes());
}

fn digest_i64(hash: &mut Sha256, value: i64) {
    digest_field(hash, &value.to_be_bytes());
}

fn digest_u64(hash: &mut Sha256, value: u64) {
    digest_field(hash, &value.to_be_bytes());
}

fn digest_option_marker(hash: &mut Sha256, present: bool) {
    digest_field(hash, &[u8::from(present)]);
}

fn digest_scope(hash: &mut Sha256, scope: &AgentScope) {
    digest_text(hash, scope.personality_agent_id.as_str());
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryProjectionEntity {
    Batch,
    Job,
    Cursor,
    Calibration,
}

impl MemoryProjectionEntity {
    fn table(self) -> &'static str {
        match self {
            Self::Batch => "memory_batches",
            Self::Job => "memory_jobs",
            Self::Cursor => "memory_apply_cursors",
            Self::Calibration => "memory_calibration",
        }
    }

    fn identity_column(self) -> &'static str {
        match self {
            Self::Batch | Self::Job => "id",
            Self::Cursor => "kind",
            Self::Calibration => "singleton",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct MemoryProjectionRef {
    pub(crate) event_seq: u64,
    pub(crate) digest: [u8; MEMORY_PROJECTION_DIGEST_BYTES],
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(deny_unknown_fields)]
pub(crate) struct MemoryProjectionKey {
    pub(crate) entity: MemoryProjectionEntity,
    pub(crate) id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct MemoryProjectionChange {
    pub(crate) entity: MemoryProjectionEntity,
    pub(crate) id: String,
    pub(crate) previous: Option<MemoryProjectionRef>,
    pub(crate) current_digest: [u8; MEMORY_PROJECTION_DIGEST_BYTES],
}

impl MemoryProjectionChange {
    pub(crate) fn key(&self) -> MemoryProjectionKey {
        MemoryProjectionKey {
            entity: self.entity,
            id: self.id.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct MemoryProjectionDeltaV1 {
    pub(crate) changes: Vec<MemoryProjectionChange>,
}

impl MemoryProjectionDeltaV1 {
    pub(crate) fn new(mut changes: Vec<MemoryProjectionChange>) -> Result<Self> {
        changes.sort_by(|left, right| {
            left.entity
                .cmp(&right.entity)
                .then_with(|| left.id.as_bytes().cmp(right.id.as_bytes()))
        });
        if changes.windows(2).any(|pair| {
            pair[0].entity == pair[1].entity && pair[0].id.as_bytes() == pair[1].id.as_bytes()
        }) {
            bail!("memory projection delta contains a duplicate entity identity");
        }
        if changes.is_empty() {
            bail!("memory projection delta must contain at least one change");
        }
        Ok(Self { changes })
    }
}

pub(crate) fn memory_membership_seed(
    scope: &AgentScope,
    batch_id: &str,
) -> [u8; MEMORY_PROJECTION_DIGEST_BYTES] {
    let mut hash = Sha256::new();
    digest_field(&mut hash, MEMORY_MEMBERSHIP_DIGEST_DOMAIN);
    digest_scope(&mut hash, scope);
    digest_text(&mut hash, batch_id);
    hash.finalize().into()
}

pub(crate) fn extend_memory_membership_digest(
    previous: &[u8; MEMORY_PROJECTION_DIGEST_BYTES],
    ord: u64,
    message_id: &str,
) -> [u8; MEMORY_PROJECTION_DIGEST_BYTES] {
    let mut hash = Sha256::new();
    digest_field(&mut hash, MEMORY_MEMBERSHIP_DIGEST_DOMAIN);
    digest_field(&mut hash, previous);
    digest_u64(&mut hash, ord);
    digest_text(&mut hash, message_id);
    hash.finalize().into()
}

pub(crate) async fn recompute_memory_membership_digest(
    scope: &AgentScope,
    transaction: &mut Transaction<'_, Sqlite>,
    batch_id: &str,
) -> Result<(u64, [u8; MEMORY_PROJECTION_DIGEST_BYTES])> {
    let rows = sqlx::query(
        "SELECT ord, message_id FROM memory_batch_messages
         WHERE batch_id = ? ORDER BY ord",
    )
    .bind(batch_id)
    .fetch_all(&mut **transaction)
    .await
    .context("failed to recompute memory membership digest")?;
    let mut count = 0_u64;
    let mut digest = memory_membership_seed(scope, batch_id);
    for row in rows {
        let ord = u64::try_from(row.try_get::<i64, _>("ord")?)
            .context("memory membership ordinal out of range")?;
        let expected = count
            .checked_add(1)
            .ok_or_else(|| anyhow!("memory membership count overflow"))?;
        if ord != expected {
            bail!(
                "memory batch {batch_id} membership ordinal {ord} is not contiguous at {expected}"
            );
        }
        let message_id: String = row.try_get("message_id")?;
        digest = extend_memory_membership_digest(&digest, ord, &message_id);
        count = expected;
    }
    Ok((count, digest))
}

async fn compute_memory_batch_digest(
    scope: &AgentScope,
    transaction: &mut Transaction<'_, Sqlite>,
    id: &str,
) -> Result<[u8; MEMORY_PROJECTION_DIGEST_BYTES]> {
    let row = sqlx::query(
        "SELECT id, layer, ord, batch_seq, version, state, est_tokens,
                eviction_footprint_tokens, membership_count, membership_digest,
                summary_key_ref, summary_ciphertext, summary_projection,
                summary_redaction_version, updated_at
         FROM memory_batches WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| anyhow!("memory batch {id} disappeared before commitment"))?;
    let mut hash = Sha256::new();
    digest_field(&mut hash, MEMORY_BATCH_DIGEST_DOMAIN);
    digest_scope(&mut hash, scope);
    digest_text(&mut hash, &row.try_get::<String, _>("id")?);
    for field in ["layer", "ord", "batch_seq", "version"] {
        digest_i64(&mut hash, row.try_get(field)?);
    }
    digest_text(&mut hash, &row.try_get::<String, _>("state")?);
    for field in [
        "est_tokens",
        "eviction_footprint_tokens",
        "membership_count",
    ] {
        digest_i64(&mut hash, row.try_get(field)?);
    }
    let membership_digest: Vec<u8> = row.try_get("membership_digest")?;
    if membership_digest.len() != MEMORY_PROJECTION_DIGEST_BYTES {
        bail!("memory batch {id} has invalid membership digest length");
    }
    digest_field(&mut hash, &membership_digest);
    let summary_key_ref: Option<String> = row.try_get("summary_key_ref")?;
    let summary_ciphertext: Option<Vec<u8>> = row.try_get("summary_ciphertext")?;
    let summary_projection: Option<String> = row.try_get("summary_projection")?;
    let summary_redaction_version: Option<i64> = row.try_get("summary_redaction_version")?;
    let summary_present = summary_key_ref.is_some();
    if summary_present != summary_ciphertext.is_some()
        || summary_present != summary_projection.is_some()
        || summary_present != summary_redaction_version.is_some()
    {
        bail!("memory batch {id} has an incomplete summary tuple");
    }
    digest_option_marker(&mut hash, summary_present);
    if let (Some(key_ref), Some(ciphertext), Some(projection), Some(version)) = (
        summary_key_ref,
        summary_ciphertext,
        summary_projection,
        summary_redaction_version,
    ) {
        digest_text(&mut hash, &key_ref);
        digest_field(&mut hash, &ciphertext);
        digest_text(&mut hash, &projection);
        digest_i64(&mut hash, version);
    }
    digest_text(&mut hash, &row.try_get::<String, _>("updated_at")?);
    Ok(hash.finalize().into())
}

async fn compute_memory_job_digest(
    scope: &AgentScope,
    transaction: &mut Transaction<'_, Sqlite>,
    id: &str,
) -> Result<[u8; MEMORY_PROJECTION_DIGEST_BYTES]> {
    let row = sqlx::query(
        "SELECT id, kind, batch_seq, source_ids, source_versions, status,
                lease_until, attempts, result_key_ref, result_ciphertext,
                result_projection, result_redaction_version, created_at, updated_at
         FROM memory_jobs WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| anyhow!("memory job {id} disappeared before commitment"))?;
    let mut hash = Sha256::new();
    digest_field(&mut hash, MEMORY_JOB_DIGEST_DOMAIN);
    digest_scope(&mut hash, scope);
    digest_text(&mut hash, &row.try_get::<String, _>("id")?);
    digest_text(&mut hash, &row.try_get::<String, _>("kind")?);
    digest_i64(&mut hash, row.try_get("batch_seq")?);

    let source_ids_json: String = row.try_get("source_ids")?;
    let source_ids: Vec<String> = serde_json::from_str(&source_ids_json)
        .context("memory job source_ids is not canonical JSON")?;
    digest_u64(
        &mut hash,
        u64::try_from(source_ids.len()).context("memory job source count overflow")?,
    );
    for source_id in source_ids {
        Uuid::parse_str(&source_id)
            .with_context(|| format!("memory job source id {source_id} is not a UUID"))?;
        digest_text(&mut hash, &source_id);
    }

    let source_versions_json: String = row.try_get("source_versions")?;
    let source_versions: BTreeMap<String, i64> = serde_json::from_str(&source_versions_json)
        .context("memory job source_versions is not canonical JSON")?;
    let mut sorted_versions = source_versions
        .into_iter()
        .map(|(batch_id, version)| {
            let uuid = Uuid::parse_str(&batch_id).with_context(|| {
                format!("memory job source-version id {batch_id} is not a UUID")
            })?;
            Ok((uuid, batch_id, version))
        })
        .collect::<Result<Vec<_>>>()?;
    sorted_versions.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    digest_u64(
        &mut hash,
        u64::try_from(sorted_versions.len()).context("memory job version count overflow")?,
    );
    for (_, batch_id, version) in sorted_versions {
        digest_text(&mut hash, &batch_id);
        digest_i64(&mut hash, version);
    }

    digest_text(&mut hash, &row.try_get::<String, _>("status")?);
    let lease_until: Option<String> = row.try_get("lease_until")?;
    digest_option_marker(&mut hash, lease_until.is_some());
    if let Some(lease_until) = lease_until {
        digest_text(&mut hash, &lease_until);
    }
    digest_i64(&mut hash, row.try_get("attempts")?);

    let result_key_ref: Option<String> = row.try_get("result_key_ref")?;
    let result_ciphertext: Option<Vec<u8>> = row.try_get("result_ciphertext")?;
    let result_projection: Option<String> = row.try_get("result_projection")?;
    let result_redaction_version: Option<i64> = row.try_get("result_redaction_version")?;
    let result_present = result_key_ref.is_some();
    if result_present != result_ciphertext.is_some()
        || result_present != result_projection.is_some()
        || result_present != result_redaction_version.is_some()
    {
        bail!("memory job {id} has an incomplete result tuple");
    }
    digest_option_marker(&mut hash, result_present);
    if let (Some(key_ref), Some(ciphertext), Some(projection), Some(version)) = (
        result_key_ref,
        result_ciphertext,
        result_projection,
        result_redaction_version,
    ) {
        digest_text(&mut hash, &key_ref);
        digest_field(&mut hash, &ciphertext);
        digest_text(&mut hash, &projection);
        digest_i64(&mut hash, version);
    }
    digest_text(&mut hash, &row.try_get::<String, _>("created_at")?);
    digest_text(&mut hash, &row.try_get::<String, _>("updated_at")?);
    Ok(hash.finalize().into())
}

async fn compute_memory_cursor_digest(
    scope: &AgentScope,
    transaction: &mut Transaction<'_, Sqlite>,
    kind: &str,
) -> Result<[u8; MEMORY_PROJECTION_DIGEST_BYTES]> {
    let next_batch_seq: i64 =
        sqlx::query_scalar("SELECT next_batch_seq FROM memory_apply_cursors WHERE kind = ?")
            .bind(kind)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or_else(|| anyhow!("memory cursor {kind} disappeared before commitment"))?;
    let mut hash = Sha256::new();
    digest_field(&mut hash, MEMORY_CURSOR_DIGEST_DOMAIN);
    digest_scope(&mut hash, scope);
    digest_text(&mut hash, kind);
    digest_i64(&mut hash, next_batch_seq);
    Ok(hash.finalize().into())
}

async fn compute_memory_calibration_digest(
    scope: &AgentScope,
    transaction: &mut Transaction<'_, Sqlite>,
    id: &str,
) -> Result<[u8; MEMORY_PROJECTION_DIGEST_BYTES]> {
    if id != MEMORY_CALIBRATION_ID {
        bail!("unknown memory calibration identity {id}");
    }
    let ratio_bits: Vec<u8> =
        sqlx::query_scalar("SELECT ratio_bits FROM memory_calibration WHERE singleton = 1")
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or_else(|| anyhow!("memory calibration disappeared before commitment"))?;
    if ratio_bits.len() != 8 {
        bail!("memory calibration ratio_bits has invalid length");
    }
    let mut hash = Sha256::new();
    digest_field(&mut hash, MEMORY_CALIBRATION_DIGEST_DOMAIN);
    digest_scope(&mut hash, scope);
    digest_text(&mut hash, "calibration");
    digest_i64(&mut hash, 1);
    digest_field(&mut hash, &ratio_bits);
    Ok(hash.finalize().into())
}

pub(crate) async fn compute_memory_projection_digest(
    scope: &AgentScope,
    transaction: &mut Transaction<'_, Sqlite>,
    key: &MemoryProjectionKey,
) -> Result<[u8; MEMORY_PROJECTION_DIGEST_BYTES]> {
    match key.entity {
        MemoryProjectionEntity::Batch => {
            compute_memory_batch_digest(scope, transaction, &key.id).await
        }
        MemoryProjectionEntity::Job => compute_memory_job_digest(scope, transaction, &key.id).await,
        MemoryProjectionEntity::Cursor => {
            compute_memory_cursor_digest(scope, transaction, &key.id).await
        }
        MemoryProjectionEntity::Calibration => {
            compute_memory_calibration_digest(scope, transaction, &key.id).await
        }
    }
}

pub(crate) async fn capture_memory_projection_ref(
    scope: &AgentScope,
    transaction: &mut Transaction<'_, Sqlite>,
    key: MemoryProjectionKey,
) -> Result<(MemoryProjectionKey, Option<MemoryProjectionRef>)> {
    let row = if key.entity == MemoryProjectionEntity::Calibration {
        if key.id != MEMORY_CALIBRATION_ID {
            bail!("unknown memory calibration identity {}", key.id);
        }
        sqlx::query(
            "SELECT projection_event_seq, projection_digest
             FROM memory_calibration WHERE singleton = 1",
        )
        .fetch_optional(&mut **transaction)
        .await?
    } else {
        let sql = format!(
            "SELECT projection_event_seq, projection_digest FROM {} WHERE {} = ?",
            key.entity.table(),
            key.entity.identity_column()
        );
        sqlx::query(&sql)
            .bind(&key.id)
            .fetch_optional(&mut **transaction)
            .await?
    };
    let Some(row) = row else {
        return Ok((key, None));
    };
    let event_seq: Option<i64> = row.try_get("projection_event_seq")?;
    let digest: Option<Vec<u8>> = row.try_get("projection_digest")?;
    let reference = match (event_seq, digest) {
        (Some(event_seq), Some(digest)) => {
            let reference = MemoryProjectionRef {
                event_seq: u64::try_from(event_seq)
                    .context("memory projection event sequence out of range")?,
                digest: digest
                    .try_into()
                    .map_err(|_| anyhow!("memory projection digest has invalid length"))?,
            };
            if key.entity == MemoryProjectionEntity::Batch {
                let (actual_count, actual_digest) =
                    recompute_memory_membership_digest(scope, transaction, &key.id).await?;
                let row = sqlx::query(
                    "SELECT membership_count, membership_digest
                     FROM memory_batches WHERE id = ?",
                )
                .bind(&key.id)
                .fetch_one(&mut **transaction)
                .await?;
                let stored_count = u64::try_from(row.try_get::<i64, _>("membership_count")?)
                    .context("stored memory membership count out of range")?;
                let stored_digest: Vec<u8> = row.try_get("membership_digest")?;
                if stored_count != actual_count || stored_digest.as_slice() != actual_digest {
                    bail!("memory batch {} membership commitment mismatch", key.id);
                }
            }
            let actual_digest = compute_memory_projection_digest(scope, transaction, &key).await?;
            if actual_digest != reference.digest {
                bail!(
                    "authenticated memory projection digest mismatch for {:?} {}",
                    key.entity,
                    key.id
                );
            }
            reference
        }
        (None, None) => bail!(
            "existing {} {} has no authenticated memory projection commitment",
            key.entity.table(),
            key.id
        ),
        _ => bail!(
            "existing {} {} has an incomplete memory projection commitment",
            key.entity.table(),
            key.id
        ),
    };
    Ok((key, Some(reference)))
}

pub(crate) async fn commit_memory_projection(
    scope: &AgentScope,
    transaction: &mut Transaction<'_, Sqlite>,
    event_seq: u64,
    captured: (MemoryProjectionKey, Option<MemoryProjectionRef>),
) -> Result<MemoryProjectionChange> {
    let (key, previous) = captured;
    let current_digest = compute_memory_projection_digest(scope, transaction, &key).await?;
    let sql = match (key.entity, previous.as_ref()) {
        (MemoryProjectionEntity::Calibration, Some(_)) => "UPDATE memory_calibration
             SET projection_event_seq = ?, projection_digest = ?
             WHERE singleton = 1 AND projection_event_seq = ? AND projection_digest = ?"
            .to_owned(),
        (MemoryProjectionEntity::Calibration, None) => "UPDATE memory_calibration
             SET projection_event_seq = ?, projection_digest = ?
             WHERE singleton = 1 AND projection_event_seq = ?
               AND projection_digest = zeroblob(32)"
            .to_owned(),
        (_, Some(_)) => format!(
            "UPDATE {} SET projection_event_seq = ?, projection_digest = ?
             WHERE {} = ? AND projection_event_seq = ? AND projection_digest = ?",
            key.entity.table(),
            key.entity.identity_column()
        ),
        (_, None) => format!(
            "UPDATE {} SET projection_event_seq = ?, projection_digest = ?
             WHERE {} = ? AND projection_event_seq = ?
               AND projection_digest = zeroblob(32)",
            key.entity.table(),
            key.entity.identity_column()
        ),
    };
    let mut query = sqlx::query(&sql)
        .bind(i64::try_from(event_seq).context("memory projection event sequence out of range")?)
        .bind(current_digest.as_slice());
    if key.entity != MemoryProjectionEntity::Calibration {
        query = query.bind(&key.id);
    }
    if let Some(previous) = &previous {
        query = query
            .bind(
                i64::try_from(previous.event_seq)
                    .context("previous memory projection event sequence out of range")?,
            )
            .bind(previous.digest.as_slice());
    } else {
        query = query.bind(
            i64::try_from(event_seq).context("memory projection event sequence out of range")?,
        );
    }
    let result = query.execute(&mut **transaction).await?;
    if result.rows_affected() != 1 {
        bail!(
            "memory projection previous-reference CAS failed for {:?} {}",
            key.entity,
            key.id
        );
    }
    Ok(MemoryProjectionChange {
        entity: key.entity,
        id: key.id,
        previous,
        current_digest,
    })
}

pub(crate) async fn load_verified_memory_projection_set(
    scope: &AgentScope,
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<BTreeMap<MemoryProjectionKey, MemoryProjectionRef>> {
    let mut keys = BTreeSet::new();
    for (entity, table, identity) in [
        (MemoryProjectionEntity::Batch, "memory_batches", "id"),
        (MemoryProjectionEntity::Job, "memory_jobs", "id"),
        (
            MemoryProjectionEntity::Cursor,
            "memory_apply_cursors",
            "kind",
        ),
    ] {
        let sql = format!("SELECT {identity} AS id FROM {table} ORDER BY {identity}");
        for id in sqlx::query_scalar::<_, String>(&sql)
            .fetch_all(&mut **transaction)
            .await?
        {
            keys.insert(MemoryProjectionKey { entity, id });
        }
    }
    let calibration_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM memory_calibration WHERE singleton = 1")
            .fetch_one(&mut **transaction)
            .await?;
    if calibration_count == 1 {
        keys.insert(MemoryProjectionKey {
            entity: MemoryProjectionEntity::Calibration,
            id: MEMORY_CALIBRATION_ID.to_owned(),
        });
    } else if calibration_count != 0 {
        bail!("memory calibration singleton cardinality is invalid");
    }

    let mut verified = BTreeMap::new();
    for key in keys {
        if key.entity == MemoryProjectionEntity::Batch {
            let (actual_count, actual_membership_digest) =
                recompute_memory_membership_digest(scope, transaction, &key.id).await?;
            let row = sqlx::query(
                "SELECT membership_count, membership_digest
                 FROM memory_batches WHERE id = ?",
            )
            .bind(&key.id)
            .fetch_one(&mut **transaction)
            .await?;
            let stored_count = u64::try_from(row.try_get::<i64, _>("membership_count")?)
                .context("stored memory membership count out of range")?;
            let stored_digest: Vec<u8> = row.try_get("membership_digest")?;
            if stored_count != actual_count
                || stored_digest.as_slice() != actual_membership_digest.as_slice()
            {
                bail!(
                    "memory batch {} membership chain does not match its committed projection",
                    key.id
                );
            }
        }
        let (_, reference) = capture_memory_projection_ref(scope, transaction, key.clone()).await?;
        let reference = reference.ok_or_else(|| {
            anyhow!(
                "{} {} disappeared from authenticated memory projection set",
                key.entity.table(),
                key.id
            )
        })?;
        verified.insert(key, reference);
    }
    Ok(verified)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MemoryLayer {
    L0 = 0,
    L1 = 1,
    L2 = 2,
}

impl MemoryLayer {
    pub(crate) fn as_i64(self) -> i64 {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    Discarded,
    Failed,
}

impl MemoryJobStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Applied => "applied",
            Self::Discarded => "discarded",
            Self::Failed => "failed",
        }
    }

    pub(crate) fn from_str(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "applied" => Some(Self::Applied),
            "discarded" => Some(Self::Discarded),
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

    pub(crate) async fn insert_staged(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        event_seq: u64,
    ) -> Result<()> {
        let (key_ref, ciphertext, projection, redaction_version) = match &self.summary {
            Some(summary) => (
                Some(&summary.key_ref),
                Some(&summary.ciphertext),
                Some(&summary.projection),
                Some(i64::from(summary.redaction_version)),
            ),
            None => (None, None, None, None),
        };
        sqlx::query(
            "INSERT INTO memory_batches(
                id, layer, ord, batch_seq, version, state, est_tokens,
                eviction_footprint_tokens, summary_key_ref, summary_ciphertext,
                summary_projection, summary_redaction_version,
                projection_event_seq, projection_digest, updated_at
             ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        .bind(i64::try_from(event_seq).context("memory projection event sequence out of range")?)
        .bind([0_u8; MEMORY_PROJECTION_DIGEST_BYTES].as_slice())
        .bind(&self.updated_at)
        .execute(&mut **transaction)
        .await
        .context("failed to stage memory batch insert")?;
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
    pub lease_until: Option<String>,
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
            lease_until: None,
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
                id, kind, batch_seq, source_ids, source_versions, status, lease_until, attempts,
                result_key_ref, result_ciphertext, result_projection,
                result_redaction_version, created_at, updated_at
             ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&self.id)
        .bind(self.kind.as_str())
        .bind(self.batch_seq)
        .bind(&source_ids_json)
        .bind(&source_versions_json)
        .bind(self.status.as_str())
        .bind(self.lease_until.as_ref())
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

    pub(crate) async fn insert_staged(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        event_seq: u64,
    ) -> Result<()> {
        let (key_ref, ciphertext, projection, redaction_version) = match &self.result {
            Some(result) => (
                Some(&result.key_ref),
                Some(&result.ciphertext),
                Some(&result.projection),
                Some(i64::from(result.redaction_version)),
            ),
            None => (None, None, None, None),
        };
        let source_ids_json =
            serde_json::to_string(&self.source_ids).context("failed to serialize source_ids")?;
        let source_versions_json = serde_json::to_string(&self.source_versions)
            .context("failed to serialize source_versions")?;
        sqlx::query(
            "INSERT INTO memory_jobs(
                id, kind, batch_seq, source_ids, source_versions, status, lease_until, attempts,
                result_key_ref, result_ciphertext, result_projection,
                result_redaction_version, projection_event_seq, projection_digest,
                created_at, updated_at
             ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&self.id)
        .bind(self.kind.as_str())
        .bind(self.batch_seq)
        .bind(&source_ids_json)
        .bind(&source_versions_json)
        .bind(self.status.as_str())
        .bind(self.lease_until.as_ref())
        .bind(self.attempts)
        .bind(key_ref)
        .bind(ciphertext)
        .bind(projection)
        .bind(redaction_version)
        .bind(i64::try_from(event_seq).context("memory projection event sequence out of range")?)
        .bind([0_u8; MEMORY_PROJECTION_DIGEST_BYTES].as_slice())
        .bind(&self.created_at)
        .bind(&self.updated_at)
        .execute(&mut **transaction)
        .await
        .context("failed to stage memory job insert")?;
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
    use std::{collections::BTreeMap, sync::Arc};

    use super::*;
    use crate::store::{
        DataKeyPurpose, DurableEvent, EventBatch, EventWrite, EventWriter,
        MemoryApplyCursorAdvance, MemoryTransition, Projection, Store,
    };
    use uuid::Uuid;

    async fn store() -> Store {
        Store::session_test_store("0198f0f4-9b72-7000-8000-000000000001")
            .await
            .expect("open test store")
    }

    async fn apply_transition(store: &Store, kind: &str, transition: MemoryTransition) -> u64 {
        let seqs = EventWriter::new(Arc::new(store.clone()))
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::memory_maintenance(kind)
                            .expect("memory-maintenance test event"),
                    ),
                    projections: vec![Projection::MemoryTransition(transition)],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("commit authenticated memory transition");
        assert_eq!(seqs.len(), 1);
        seqs[0]
    }

    async fn seed_batch(
        store: &Store,
        layer: MemoryLayer,
        state: MemoryBatchState,
    ) -> (String, i64, i64, u64) {
        let id = Uuid::now_v7().to_string();
        let event_seq = apply_transition(
            store,
            "fixture_seed_batch",
            MemoryTransition {
                batch_inserts: vec![MemoryBatchRecord::new(id.clone(), layer, 0, 0, state, 0, 0)],
                ..Default::default()
            },
        )
        .await;
        let row = sqlx::query("SELECT ord, batch_seq FROM memory_batches WHERE id = ?")
            .bind(&id)
            .fetch_one(store.pool())
            .await
            .expect("load seeded memory batch");
        (
            id,
            row.try_get("ord").expect("seeded batch ord"),
            row.try_get("batch_seq").expect("seeded batch sequence"),
            event_seq,
        )
    }

    async fn seed_job(store: &Store) -> (String, String, String, i64, u64) {
        let source_id = Uuid::now_v7().to_string();
        let target_id = Uuid::now_v7().to_string();
        let job_id = Uuid::now_v7().to_string();
        let source = MemoryBatchRecord::new(
            source_id.clone(),
            MemoryLayer::L0,
            0,
            0,
            MemoryBatchState::Compacting,
            0,
            0,
        );
        let target = MemoryBatchRecord::new(
            target_id.clone(),
            MemoryLayer::L1,
            0,
            0,
            MemoryBatchState::Compacting,
            0,
            0,
        );
        let job = MemoryJobRecord::new(
            job_id.clone(),
            MemoryJobKind::CompactL0,
            0,
            vec![source_id.clone()],
            BTreeMap::from([(source_id.clone(), 0), (target_id.clone(), 0)]),
        );
        let event_seq = apply_transition(
            store,
            "fixture_seed_job",
            MemoryTransition {
                batch_inserts: vec![source, target],
                job_inserts: vec![job],
                ..Default::default()
            },
        )
        .await;
        let batch_seq: i64 = sqlx::query_scalar("SELECT batch_seq FROM memory_jobs WHERE id = ?")
            .bind(&job_id)
            .fetch_one(store.pool())
            .await
            .expect("load seeded memory job sequence");
        (source_id, target_id, job_id, batch_seq, event_seq)
    }

    async fn seed_cursor(store: &Store, kind: &str, next_batch_seq: u64) -> u64 {
        apply_transition(
            store,
            "fixture_seed_cursor",
            MemoryTransition {
                cursor_advance: Some(MemoryApplyCursorAdvance {
                    kind: kind.to_owned(),
                    expected: next_batch_seq,
                    next: next_batch_seq,
                    initialize: true,
                }),
                ..Default::default()
            },
        )
        .await
    }

    fn assert_sql_error_contains<T>(result: std::result::Result<T, sqlx::Error>, expected: &str) {
        let error = match result {
            Ok(_) => panic!("statement must violate its intended constraint"),
            Err(error) => error,
        };
        let rendered = error.to_string();
        assert!(
            rendered.contains(expected),
            "expected SQLite error containing {expected:?}, got {rendered}"
        );
    }

    #[tokio::test]
    async fn migration_enforces_memory_batch_constraints() {
        let store = store().await;
        let (_, _, _, event_seq) =
            seed_batch(&store, MemoryLayer::L0, MemoryBatchState::Open).await;
        let event_seq = i64::try_from(event_seq).expect("test event sequence fits i64");
        let summary_key = store
            .memory_summary_key("batch", "migration-constraint-fixture")
            .await
            .expect("mint summary key");

        let invalid_layer = sqlx::query(
            "INSERT INTO memory_batches(
                id, layer, ord, batch_seq, version, state, est_tokens,
                eviction_footprint_tokens, projection_event_seq, projection_digest, updated_at
             ) VALUES('batch-invalid-layer', 5, 101, 101, 0, 'open', 0, 0, ?, zeroblob(32), 'now')",
        )
        .bind(event_seq)
        .execute(store.pool())
        .await;
        assert_sql_error_contains(invalid_layer, "layer IN");

        let invalid_state = sqlx::query(
            "INSERT INTO memory_batches(
                id, layer, ord, batch_seq, version, state, est_tokens,
                eviction_footprint_tokens, projection_event_seq, projection_digest, updated_at
             ) VALUES('batch-invalid-state', 0, 102, 102, 0, 'unknown', 0, 0, ?, zeroblob(32), 'now')",
        )
        .bind(event_seq)
        .execute(store.pool())
        .await;
        assert_sql_error_contains(invalid_state, "state IN");

        let negative_tokens = sqlx::query(
            "INSERT INTO memory_batches(
                id, layer, ord, batch_seq, version, state, est_tokens,
                eviction_footprint_tokens, projection_event_seq, projection_digest, updated_at
             ) VALUES('batch-negative-tokens', 0, 103, 103, 0, 'open', -1, 0, ?, zeroblob(32), 'now')",
        )
        .bind(event_seq)
        .execute(store.pool())
        .await;
        assert_sql_error_contains(negative_tokens, "est_tokens >= 0");

        let partial_summary = sqlx::query(
            "INSERT INTO memory_batches(
                id, layer, ord, batch_seq, version, state, est_tokens,
                eviction_footprint_tokens, summary_key_ref, summary_ciphertext,
                summary_projection, summary_redaction_version, projection_event_seq,
                projection_digest, updated_at
             ) VALUES(
                'batch-partial-summary', 1, 104, 104, 0, 'compacting', 0, 0,
                ?, NULL, 'proj', 1, ?, zeroblob(32), 'now'
             )",
        )
        .bind(&summary_key.key_ref)
        .bind(event_seq)
        .execute(store.pool())
        .await;
        assert_sql_error_contains(partial_summary, "summary_key_ref IS NULL");
    }

    #[tokio::test]
    async fn memory_batch_record_round_trip_and_unique_layer_batch_seq() {
        let store = store().await;
        let (batch_id, ord, batch_seq, event_seq) =
            seed_batch(&store, MemoryLayer::L0, MemoryBatchState::Open).await;
        let row = sqlx::query(
            "SELECT layer, ord, batch_seq, version, state, projection_event_seq,
                    length(projection_digest) AS digest_len
             FROM memory_batches WHERE id = ?",
        )
        .bind(&batch_id)
        .fetch_one(store.pool())
        .await
        .expect("round-trip authenticated memory batch");
        assert_eq!(row.try_get::<i64, _>("layer").unwrap(), 0);
        assert_eq!(row.try_get::<i64, _>("ord").unwrap(), ord);
        assert_eq!(row.try_get::<i64, _>("batch_seq").unwrap(), batch_seq);
        assert_eq!(row.try_get::<i64, _>("version").unwrap(), 0);
        assert_eq!(row.try_get::<String, _>("state").unwrap(), "open");
        assert_eq!(
            row.try_get::<i64, _>("projection_event_seq").unwrap(),
            i64::try_from(event_seq).unwrap()
        );
        assert_eq!(row.try_get::<i64, _>("digest_len").unwrap(), 32);

        let duplicate = sqlx::query(
            "INSERT INTO memory_batches(
                id, layer, ord, batch_seq, version, state, est_tokens,
                eviction_footprint_tokens, projection_event_seq, projection_digest, updated_at
             ) VALUES(?, 0, ?, ?, 0, 'open', 0, 0, ?, zeroblob(32), 'now')",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(ord.checked_add(1).expect("test ord"))
        .bind(batch_seq)
        .bind(i64::try_from(event_seq).unwrap())
        .execute(store.pool())
        .await;
        assert_sql_error_contains(duplicate, "memory_batches.layer, memory_batches.batch_seq");
    }

    #[tokio::test]
    async fn memory_batch_message_enforces_membership_and_orphan_rejection() {
        let store = store().await;

        let key = store
            .private_key(DataKeyPurpose::Transcript)
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

        let orphan = MemoryBatchMessageRecord {
            batch_id: "missing-batch".to_owned(),
            message_id: "message-1".to_owned(),
            ord: 0,
        };
        let orphan = orphan.insert(store.pool()).await;
        let error = orphan.expect_err("orphan membership must fail");
        assert!(format!("{error:#}").contains("FOREIGN KEY"), "{error:#}");

        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory_batch_messages")
                .fetch_one(store.pool())
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn memory_job_unique_kind_batch_seq_and_status_check() {
        let store = store().await;
        let (source_id, target_id, job_id, batch_seq, event_seq) = seed_job(&store).await;
        let row = sqlx::query(
            "SELECT kind, batch_seq, source_ids, source_versions, status,
                    projection_event_seq, length(projection_digest) AS digest_len
             FROM memory_jobs WHERE id = ?",
        )
        .bind(&job_id)
        .fetch_one(store.pool())
        .await
        .expect("round-trip authenticated memory job");
        assert_eq!(row.try_get::<String, _>("kind").unwrap(), "compact_l0");
        assert_eq!(row.try_get::<i64, _>("batch_seq").unwrap(), batch_seq);
        assert_eq!(row.try_get::<String, _>("status").unwrap(), "pending");
        assert_eq!(
            serde_json::from_str::<Vec<String>>(&row.try_get::<String, _>("source_ids").unwrap())
                .unwrap(),
            vec![source_id.clone()]
        );
        assert_eq!(
            serde_json::from_str::<BTreeMap<String, i64>>(
                &row.try_get::<String, _>("source_versions").unwrap()
            )
            .unwrap(),
            BTreeMap::from([(source_id.clone(), 0), (target_id, 0)])
        );
        assert_eq!(
            row.try_get::<i64, _>("projection_event_seq").unwrap(),
            i64::try_from(event_seq).unwrap()
        );
        assert_eq!(row.try_get::<i64, _>("digest_len").unwrap(), 32);

        let duplicate = sqlx::query(
            "INSERT INTO memory_jobs(
                id, kind, batch_seq, source_ids, source_versions, status, lease_until, attempts,
                projection_event_seq, projection_digest, created_at, updated_at
             ) VALUES(?, 'compact_l0', ?, ?, ?, 'pending', NULL, 0, ?, zeroblob(32), 'now', 'now')",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(batch_seq)
        .bind(serde_json::to_string(&vec![source_id.clone()]).unwrap())
        .bind(serde_json::to_string(&BTreeMap::from([(source_id, 0)])).unwrap())
        .bind(i64::try_from(event_seq).unwrap())
        .execute(store.pool())
        .await;
        assert_sql_error_contains(duplicate, "memory_jobs.kind, memory_jobs.batch_seq");

        let invalid_status = sqlx::query(
            "INSERT INTO memory_jobs(
                id, kind, batch_seq, source_ids, source_versions, status, attempts,
                projection_event_seq, projection_digest, created_at, updated_at
             ) VALUES(
                ?, 'compact_l1', ?, '[\"source\"]', '{\"source\":0}', 'unknown', 0,
                ?, zeroblob(32), 'now', 'now'
             )",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(batch_seq.checked_add(100).expect("test job sequence"))
        .bind(i64::try_from(event_seq).unwrap())
        .execute(store.pool())
        .await;
        assert_sql_error_contains(invalid_status, "status IN");
    }

    #[tokio::test]
    async fn memory_apply_cursor_cas_advances_monotonically() {
        let store = store().await;
        let kind = MemoryJobKind::CompactL0.as_str();
        seed_cursor(&store, kind, 0).await;
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT next_batch_seq FROM memory_apply_cursors WHERE kind = ?"
            )
            .bind(kind)
            .fetch_one(store.pool())
            .await
            .expect("load authenticated cursor"),
            0
        );

        let mut transaction = store.pool().begin().await.expect("begin cursor CAS test");
        assert!(
            MemoryApplyCursorRecord {
                kind: kind.to_owned(),
                next_batch_seq: 2,
            }
            .advance(&mut *transaction, 0)
            .await
            .expect("advance cursor")
        );
        assert!(
            !MemoryApplyCursorRecord {
                kind: kind.to_owned(),
                next_batch_seq: 3,
            }
            .advance(&mut *transaction, 0)
            .await
            .expect("reject stale expected cursor")
        );
        assert!(
            !MemoryApplyCursorRecord {
                kind: kind.to_owned(),
                next_batch_seq: 1,
            }
            .advance(&mut *transaction, 2)
            .await
            .expect("reject cursor regression")
        );
        transaction
            .rollback()
            .await
            .expect("rollback unauthenticated primitive test writes");
    }

    #[tokio::test]
    async fn batch_state_version_cas_rejects_stale_expected_version() {
        let store = store().await;
        let (batch_id, _, _, _) = seed_batch(&store, MemoryLayer::L0, MemoryBatchState::Open).await;
        let mut transaction = store.pool().begin().await.expect("begin batch CAS test");
        assert!(
            !update_batch_state_version(
                &mut *transaction,
                &batch_id,
                1,
                MemoryBatchState::Compacting,
                0,
            )
            .await
            .unwrap()
        );
        assert!(
            update_batch_state_version(
                &mut *transaction,
                &batch_id,
                0,
                MemoryBatchState::Compacting,
                0,
            )
            .await
            .expect("matching batch CAS")
        );
        assert!(
            !update_batch_state_version(
                &mut *transaction,
                &batch_id,
                0,
                MemoryBatchState::CompactFailed,
                0,
            )
            .await
            .expect("stale batch CAS after version increment")
        );
        transaction
            .rollback()
            .await
            .expect("rollback unauthenticated primitive test writes");
    }

    #[tokio::test]
    async fn memory_summary_requires_valid_data_key_foreign_key() {
        let store = store().await;
        let (batch_id, _, _, _) =
            seed_batch(&store, MemoryLayer::L1, MemoryBatchState::Compacting).await;
        let summary_key = store
            .memory_summary_key("batch", &batch_id)
            .await
            .expect("mint memory summary key");

        let mut transaction = store
            .pool()
            .begin()
            .await
            .expect("begin valid summary FK test");
        sqlx::query(
            "UPDATE memory_batches
             SET summary_key_ref = ?, summary_ciphertext = X'010203',
                 summary_projection = '{}', summary_redaction_version = 1
             WHERE id = ?",
        )
        .bind(&summary_key.key_ref)
        .bind(&batch_id)
        .execute(&mut *transaction)
        .await
        .expect("valid memory-summary key satisfies FK");
        transaction
            .rollback()
            .await
            .expect("rollback unauthenticated FK fixture update");

        let invalid = sqlx::query(
            "UPDATE memory_batches
             SET summary_key_ref = 'missing-key', summary_ciphertext = X'010203',
                 summary_projection = '{}', summary_redaction_version = 1
             WHERE id = ?",
        )
        .bind(&batch_id)
        .execute(store.pool())
        .await;
        assert_sql_error_contains(invalid, "FOREIGN KEY");
    }
}
