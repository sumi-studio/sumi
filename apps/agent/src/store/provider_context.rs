//! Encrypted provider-context records and durable provider-context mutations.
//!
//! Provider context (opaque reasoning / native compaction windows) is stored in
//! its own per-anchor data key, separate from the public transcript.  This
//! module owns the encryption envelope, the canonical `Replace`/`Invalidate`
//! mutation intent, the HKDF-derived HMAC binding, and the transactional
//! apply/CAS primitives used by `ProviderContextMutationRecovery`.

#![allow(dead_code)]

use std::collections::BTreeSet;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::Row;
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, Zeroizing};

use crate::memory::estimate::{
    EVICTION_ESTIMATOR_VERSION_REPLAY_PROBE_V1, EVICTION_ESTIMATOR_VERSION_SERIALIZED_BYTES,
    EvictionFootprint, eviction_footprint_for_payload, legacy_serialized_bytes_eviction_footprint,
};
use crate::provider::model::ModelSpec;
use crate::provider::types::{
    ApiProtocol, ProviderContextItem, ProviderContextPayload, ProviderOrigin,
};

use super::crypto::{RowAad, decrypt_content, encrypt_content};
use super::event_writer::require_single_cas;
use super::memory_state::MemoryLayer;
use super::{AgentScope, DataKeyMaterial, DataKeyPurpose, Store};

fn sqlite_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{field} exceeds SQLite INTEGER range"))
}

const INTENT_HMAC_INFO: &[u8] = b"provider-context-mutation-intent/v1";
const INTENT_HMAC_KEY_ID: &str = "mutation-intent-hmac/v1";
const PLAINTEXT_HMAC_DOMAIN: &[u8] = b"sumi-provider-context-plaintext/v1";
const INTENT_HMAC_DOMAIN: &[u8] = b"sumi-provider-context-mutation-intent/v1";
const PROJECTION_HMAC_INFO: &[u8] = b"provider-context-projection-head/v1";
const PROJECTION_STATE_DIGEST_DOMAIN: &[u8] = b"sumi-provider-context-durable-state/v1";
const PROJECTION_HEAD_HMAC_DOMAIN: &[u8] = b"sumi-provider-context-projection-head/v1";
const PROJECTION_SCHEMA_VERSION: i64 = 1;
const PROJECTION_PAGE_SIZE: i64 = 256;
const SCOPE_KEY_DOMAIN: &[u8] = b"sumi-provider-context-scope/v1";
const PREPARED_KEY_MATERIAL_PROOF_DOMAIN: &[u8] = b"sumi-event-batch-prepared-key-material/v1";
const PREPARED_KEY_MATERIAL_PROOF: &[u8] = b"active-key-material";

/// HKDF-Extract/Expand with HMAC-SHA256, keyed by the durable mutation data key
/// and conversation-scoped salt.  This key is used for both the plaintext HMAC
/// and the canonical semantic-intent HMAC.
pub(crate) fn hkdf_intent_hmac_key(data_key: &DataKeyMaterial, conversation_id: &str) -> [u8; 32] {
    hkdf_hmac_key(data_key, conversation_id, INTENT_HMAC_INFO)
}

fn hkdf_projection_hmac_key(data_key: &DataKeyMaterial, conversation_id: &str) -> [u8; 32] {
    hkdf_hmac_key(data_key, conversation_id, PROJECTION_HMAC_INFO)
}

fn hkdf_hmac_key(data_key: &DataKeyMaterial, conversation_id: &str, info: &[u8]) -> [u8; 32] {
    let mut prk_mac = <Hmac<Sha256> as Mac>::new_from_slice(conversation_id.as_bytes())
        .expect("HMAC accepts any salt length");
    prk_mac.update(data_key.bytes());
    let prk = prk_mac.finalize().into_bytes();

    let mut t_mac =
        <Hmac<Sha256> as Mac>::new_from_slice(&prk).expect("HMAC output is a valid HMAC key");
    t_mac.update(info);
    t_mac.update(&[1]);
    let t = t_mac.finalize().into_bytes();
    t.into()
}

fn hmac_sha256(key: &[u8], domain: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(domain);
    mac.update(&(payload.len() as u64).to_be_bytes());
    mac.update(payload);
    mac.finalize().into_bytes().to_vec()
}

#[derive(Clone, Debug)]
pub(super) struct ProviderContextProjectionCheckpoint {
    revision: i64,
    record_count: i64,
    set_digest: [u8; 32],
    key_ref: String,
    head_hmac: Vec<u8>,
}

fn projection_head_hmac(
    store: &Store,
    projection_key: &[u8],
    revision: i64,
    record_count: i64,
    set_digest: &[u8; 32],
    key_ref: &str,
) -> Vec<u8> {
    let mut writer = CanonicalWriter::with_domain(PROJECTION_HEAD_HMAC_DOMAIN);
    writer.field(PROJECTION_SCHEMA_VERSION.to_string().as_bytes());
    writer.field(store.scope().tenant_id.as_bytes());
    writer.field(store.scope().agent_id.as_bytes());
    writer.field(store.scope().conversation_id.as_bytes());
    writer.field(revision.to_string().as_bytes());
    writer.field(record_count.to_string().as_bytes());
    writer.field(set_digest);
    writer.field(key_ref.as_bytes());
    hmac_sha256(
        projection_key,
        PROJECTION_HEAD_HMAC_DOMAIN,
        &writer.finish(),
    )
}

fn digest_field(hasher: &mut Sha256, bytes: &[u8]) {
    Digest::update(hasher, (bytes.len() as u64).to_be_bytes());
    Digest::update(hasher, bytes);
}

fn digest_optional_field(hasher: &mut Sha256, bytes: Option<&[u8]>) {
    match bytes {
        None => Digest::update(hasher, [0]),
        Some(bytes) => {
            Digest::update(hasher, [1]);
            digest_field(hasher, bytes);
        }
    }
}

fn digest_i64(hasher: &mut Sha256, value: i64) {
    digest_field(hasher, &value.to_be_bytes());
}

fn digest_optional_i64(hasher: &mut Sha256, value: Option<i64>) {
    match value {
        None => Digest::update(hasher, [0]),
        Some(value) => {
            Digest::update(hasher, [1]);
            digest_i64(hasher, value);
        }
    }
}

async fn preflight_provider_context_projection_bounds(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<()> {
    preflight_provider_context_projection_bounds_with_limits(
        transaction,
        super::HYDRATION_MAX_ROWS,
        super::HYDRATION_MAX_ENCODED_BYTES,
    )
    .await
}

async fn preflight_provider_context_projection_bounds_with_limits(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    max_rows: u64,
    max_encoded_bytes: u64,
) -> Result<()> {
    let row = sqlx::query(
        "SELECT
            COALESCE(SUM(row_count), 0) AS row_count,
            COALESCE(SUM(encoded_bytes), 0) AS encoded_bytes
         FROM (
            SELECT COUNT(*) AS row_count,
                   COALESCE(SUM(
                     96 +
                     length(CAST(id AS BLOB)) +
                     COALESCE(length(CAST(message_id AS BLOB)), 0) +
                     length(CAST(idempotency_key AS BLOB)) +
                     length(CAST(provider_instance_id AS BLOB)) +
                     length(CAST(protocol AS BLOB)) +
                     length(CAST(model AS BLOB)) +
                     length(CAST(kind AS BLOB)) +
                     COALESCE(length(CAST(context_fingerprint AS BLOB)), 0) +
                     length(CAST(key_ref AS BLOB)) +
                     length(ciphertext) +
                     length(CAST(created_at AS BLOB))
                   ), 0) AS encoded_bytes
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
         )",
    )
    .fetch_one(&mut **transaction)
    .await
    .context("failed to preflight provider-context durable-state bounds")?;
    let row_count = u64::try_from(row.try_get::<i64, _>("row_count")?)
        .context("provider-context durable-state row count is negative")?;
    let encoded_bytes = u64::try_from(row.try_get::<i64, _>("encoded_bytes")?)
        .context("provider-context durable-state byte count is negative")?;
    if row_count > max_rows {
        bail!("provider-context durable state has {row_count} rows, limit is {max_rows}");
    }
    if encoded_bytes > max_encoded_bytes {
        bail!(
            "provider-context durable state has {encoded_bytes} encoded bytes, limit is {max_encoded_bytes}"
        );
    }
    Ok(())
}

async fn provider_context_set_digest(
    store: &Store,
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(i64, [u8; 32])> {
    let mut hasher = Sha256::new();
    Digest::update(&mut hasher, PROJECTION_STATE_DIGEST_DOMAIN);
    digest_field(
        &mut hasher,
        PROJECTION_SCHEMA_VERSION.to_string().as_bytes(),
    );
    digest_field(&mut hasher, store.scope().tenant_id.as_bytes());
    digest_field(&mut hasher, store.scope().agent_id.as_bytes());
    digest_field(&mut hasher, store.scope().conversation_id.as_bytes());

    digest_field(&mut hasher, b"provider_context");
    let mut after_id: Option<String> = None;
    let mut record_count = 0_i64;
    loop {
        let rows = sqlx::query(
            "SELECT id, message_id, message_seq, wire_item_index, item_ordinal,
                    idempotency_key, provider_instance_id, protocol, model, kind,
                    coverage_through_seq, context_fingerprint, key_ref, ciphertext,
                    eviction_tokens, eviction_estimator_version, created_at
             FROM provider_context
             WHERE ? IS NULL OR id > ?
             ORDER BY id
             LIMIT ?",
        )
        .bind(after_id.as_deref())
        .bind(after_id.as_deref())
        .bind(PROJECTION_PAGE_SIZE)
        .fetch_all(&mut **transaction)
        .await
        .context("failed to page provider-context projection set")?;
        if rows.is_empty() {
            break;
        }

        for row in rows {
            let id: String = row.try_get("id")?;
            digest_field(&mut hasher, id.as_bytes());

            let message_id: Option<String> = row.try_get("message_id")?;
            digest_optional_field(&mut hasher, message_id.as_deref().map(str::as_bytes));
            digest_optional_i64(&mut hasher, row.try_get("message_seq")?);
            digest_optional_i64(&mut hasher, row.try_get("wire_item_index")?);
            digest_i64(&mut hasher, row.try_get("item_ordinal")?);

            for field in [
                "idempotency_key",
                "provider_instance_id",
                "protocol",
                "model",
                "kind",
            ] {
                let value: String = row.try_get(field)?;
                digest_field(&mut hasher, value.as_bytes());
            }
            digest_optional_i64(&mut hasher, row.try_get("coverage_through_seq")?);
            let fingerprint: Option<String> = row.try_get("context_fingerprint")?;
            digest_optional_field(&mut hasher, fingerprint.as_deref().map(str::as_bytes));

            let key_ref: String = row.try_get("key_ref")?;
            digest_field(&mut hasher, key_ref.as_bytes());
            let ciphertext: Vec<u8> = row.try_get("ciphertext")?;
            digest_field(&mut hasher, &ciphertext);
            digest_i64(&mut hasher, row.try_get("eviction_tokens")?);
            digest_i64(&mut hasher, row.try_get("eviction_estimator_version")?);
            let created_at: String = row.try_get("created_at")?;
            digest_field(&mut hasher, created_at.as_bytes());

            record_count = record_count
                .checked_add(1)
                .ok_or_else(|| anyhow!("provider-context record count overflow"))?;
            after_id = Some(id);
        }
    }

    digest_field(&mut hasher, b"provider_context_mutations");
    let mut after_mutation_id: Option<String> = None;
    loop {
        let rows = sqlx::query(
            "SELECT mutation_id, state, intent_key_ref, intent_ciphertext,
                    hmac_key_id, intent_hmac, prepared_at, finished_at,
                    terminal_reason
             FROM provider_context_mutations
             WHERE ? IS NULL OR mutation_id > ?
             ORDER BY mutation_id
             LIMIT ?",
        )
        .bind(after_mutation_id.as_deref())
        .bind(after_mutation_id.as_deref())
        .bind(PROJECTION_PAGE_SIZE)
        .fetch_all(&mut **transaction)
        .await
        .context("failed to page provider-context mutation state")?;
        if rows.is_empty() {
            break;
        }

        for row in rows {
            let mutation_id: String = row.try_get("mutation_id")?;
            digest_field(&mut hasher, mutation_id.as_bytes());
            for field in ["state", "intent_key_ref"] {
                let value: String = row.try_get(field)?;
                digest_field(&mut hasher, value.as_bytes());
            }
            let intent_ciphertext: Vec<u8> = row.try_get("intent_ciphertext")?;
            digest_field(&mut hasher, &intent_ciphertext);
            let hmac_key_id: String = row.try_get("hmac_key_id")?;
            digest_field(&mut hasher, hmac_key_id.as_bytes());
            let intent_hmac: Vec<u8> = row.try_get("intent_hmac")?;
            digest_field(&mut hasher, &intent_hmac);
            let prepared_at: String = row.try_get("prepared_at")?;
            digest_field(&mut hasher, prepared_at.as_bytes());
            let finished_at: Option<String> = row.try_get("finished_at")?;
            digest_optional_field(&mut hasher, finished_at.as_deref().map(str::as_bytes));
            let terminal_reason: Option<String> = row.try_get("terminal_reason")?;
            digest_optional_field(&mut hasher, terminal_reason.as_deref().map(str::as_bytes));

            record_count = record_count
                .checked_add(1)
                .ok_or_else(|| anyhow!("provider-context durable-state count overflow"))?;
            after_mutation_id = Some(mutation_id);
        }
    }

    digest_field(&mut hasher, b"provider_context_replace_heads");
    let mut after_scope_key: Option<String> = None;
    loop {
        let rows = sqlx::query(
            "SELECT scope_key, max_config_generation, max_window_ordinal,
                    latest_insert_id, updated_at
             FROM provider_context_replace_heads
             WHERE ? IS NULL OR scope_key > ?
             ORDER BY scope_key
             LIMIT ?",
        )
        .bind(after_scope_key.as_deref())
        .bind(after_scope_key.as_deref())
        .bind(PROJECTION_PAGE_SIZE)
        .fetch_all(&mut **transaction)
        .await
        .context("failed to page provider-context replace-head state")?;
        if rows.is_empty() {
            break;
        }

        for row in rows {
            let scope_key: String = row.try_get("scope_key")?;
            digest_field(&mut hasher, scope_key.as_bytes());
            digest_i64(&mut hasher, row.try_get("max_config_generation")?);
            digest_i64(&mut hasher, row.try_get("max_window_ordinal")?);
            let latest_insert_id: String = row.try_get("latest_insert_id")?;
            digest_field(&mut hasher, latest_insert_id.as_bytes());
            let updated_at: String = row.try_get("updated_at")?;
            digest_field(&mut hasher, updated_at.as_bytes());

            record_count = record_count
                .checked_add(1)
                .ok_or_else(|| anyhow!("provider-context durable-state count overflow"))?;
            after_scope_key = Some(scope_key);
        }
    }

    Digest::update(&mut hasher, record_count.to_be_bytes());
    Ok((record_count, hasher.finalize().into()))
}

async fn load_authenticated_projection_head(
    store: &Store,
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<ProviderContextProjectionCheckpoint> {
    let row = sqlx::query(
        "SELECT schema_version, state, revision, record_count, set_digest,
                key_ref, head_hmac
         FROM provider_context_projection_head
         WHERE singleton = 1",
    )
    .fetch_optional(&mut **transaction)
    .await
    .context("failed to load provider-context projection head")?
    .ok_or_else(|| anyhow!("provider-context projection head is missing"))?;

    let schema_version: i64 = row.try_get("schema_version")?;
    if schema_version != PROJECTION_SCHEMA_VERSION {
        bail!("provider-context projection head uses unsupported schema version {schema_version}");
    }
    let state: String = row.try_get("state")?;
    if state != "active" {
        bail!("provider-context projection head is not initialized");
    }
    let revision: i64 = row.try_get("revision")?;
    let record_count: i64 = row.try_get("record_count")?;
    if revision < 0 || record_count < 0 {
        bail!("provider-context projection head has a negative counter");
    }
    let set_digest: [u8; 32] = row
        .try_get::<Vec<u8>, _>("set_digest")?
        .try_into()
        .map_err(|_| anyhow!("provider-context projection digest has invalid length"))?;
    let key_ref: String = row.try_get("key_ref")?;
    let head_hmac: Vec<u8> = row.try_get("head_hmac")?;
    if head_hmac.len() != 32 {
        bail!("provider-context projection head HMAC has invalid length");
    }

    let key = store
        .data_key_by_ref_in_transaction(transaction, &key_ref)
        .await
        .context("failed to load provider-context projection key")?;
    if key.purpose != DataKeyPurpose::Mutation {
        bail!("provider-context projection head references a non-mutation key");
    }
    let projection_key = hkdf_projection_hmac_key(&key, &store.scope().conversation_id);
    let expected = projection_head_hmac(
        store,
        &projection_key,
        revision,
        record_count,
        &set_digest,
        &key_ref,
    );
    if expected.as_slice().ct_eq(&head_hmac).unwrap_u8() != 1 {
        bail!("provider-context projection head HMAC mismatch");
    }

    Ok(ProviderContextProjectionCheckpoint {
        revision,
        record_count,
        set_digest,
        key_ref,
        head_hmac,
    })
}

pub(super) async fn verify_provider_context_projection_set(
    store: &Store,
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<ProviderContextProjectionCheckpoint> {
    preflight_provider_context_projection_bounds(transaction).await?;
    let checkpoint = load_authenticated_projection_head(store, transaction).await?;
    let (record_count, set_digest) = provider_context_set_digest(store, transaction).await?;
    if record_count != checkpoint.record_count
        || set_digest.ct_eq(&checkpoint.set_digest).unwrap_u8() != 1
    {
        bail!("provider-context durable state does not exactly match its authenticated commitment");
    }
    Ok(checkpoint)
}

pub(super) async fn commit_provider_context_projection_set(
    store: &Store,
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    previous: &ProviderContextProjectionCheckpoint,
) -> Result<()> {
    let current = load_authenticated_projection_head(store, transaction).await?;
    if current.revision != previous.revision
        || current.record_count != previous.record_count
        || current.set_digest != previous.set_digest
        || current.key_ref != previous.key_ref
        || current.head_hmac.ct_eq(&previous.head_hmac).unwrap_u8() != 1
    {
        bail!("provider-context projection head changed after verification");
    }

    let (record_count, set_digest) = provider_context_set_digest(store, transaction).await?;
    if record_count == previous.record_count && set_digest == previous.set_digest {
        bail!("provider-context projection commit did not change durable state");
    }
    let revision = previous
        .revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("provider-context projection revision overflow"))?;
    let key = store
        .data_key_by_ref_in_transaction(transaction, &previous.key_ref)
        .await
        .context("failed to reload provider-context projection key")?;
    if key.purpose != DataKeyPurpose::Mutation {
        bail!("provider-context projection head references a non-mutation key");
    }
    let projection_key = hkdf_projection_hmac_key(&key, &store.scope().conversation_id);
    let head_hmac = projection_head_hmac(
        store,
        &projection_key,
        revision,
        record_count,
        &set_digest,
        &previous.key_ref,
    );

    let result = sqlx::query(
        "UPDATE provider_context_projection_head
         SET revision = ?, record_count = ?, set_digest = ?, head_hmac = ?
         WHERE singleton = 1
           AND state = 'active'
           AND schema_version = ?
           AND revision = ?
           AND record_count = ?
           AND set_digest = ?
           AND key_ref = ?
           AND head_hmac = ?",
    )
    .bind(revision)
    .bind(record_count)
    .bind(set_digest.as_slice())
    .bind(&head_hmac)
    .bind(PROJECTION_SCHEMA_VERSION)
    .bind(previous.revision)
    .bind(previous.record_count)
    .bind(previous.set_digest.as_slice())
    .bind(&previous.key_ref)
    .bind(&previous.head_hmac)
    .execute(&mut **transaction)
    .await
    .context("failed to commit provider-context projection head")?;
    require_single_cas(result.rows_affected(), "provider-context projection head")
}

pub(super) async fn initialize_provider_context_projection_head(store: &Store) -> Result<()> {
    let state: Option<String> = sqlx::query_scalar(
        "SELECT state FROM provider_context_projection_head WHERE singleton = 1",
    )
    .fetch_optional(store.pool())
    .await
    .context("failed to inspect provider-context projection marker")?;
    match state.as_deref() {
        Some("active") => return Ok(()),
        Some("uninitialized") => {}
        Some(other) => bail!("provider-context projection marker has unknown state {other}"),
        None => bail!("provider-context projection marker is missing"),
    }

    let legacy_rows: i64 = sqlx::query_scalar(
        "SELECT
            (SELECT COUNT(*) FROM provider_context) +
            (SELECT COUNT(*) FROM provider_context_mutations) +
            (SELECT COUNT(*) FROM provider_context_replace_heads)",
    )
    .fetch_one(store.pool())
    .await
    .context("failed to inspect pre-commitment provider-context state")?;
    if legacy_rows != 0 {
        bail!(
            "cannot initialize provider-context projection head from non-empty unauthenticated state"
        );
    }

    let key = store
        .conversation_key(DataKeyPurpose::Mutation)
        .await
        .context("failed to initialize provider-context projection key")?;
    let mut transaction = store.pool().begin().await?;
    let current_state: String = sqlx::query_scalar(
        "SELECT state FROM provider_context_projection_head WHERE singleton = 1",
    )
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or_else(|| anyhow!("provider-context projection marker disappeared"))?;
    if current_state == "active" {
        transaction.commit().await?;
        return Ok(());
    }
    if current_state != "uninitialized" {
        bail!("provider-context projection marker has unknown state {current_state}");
    }
    let legacy_rows: i64 = sqlx::query_scalar(
        "SELECT
            (SELECT COUNT(*) FROM provider_context) +
            (SELECT COUNT(*) FROM provider_context_mutations) +
            (SELECT COUNT(*) FROM provider_context_replace_heads)",
    )
    .fetch_one(&mut *transaction)
    .await?;
    if legacy_rows != 0 {
        bail!(
            "cannot initialize provider-context projection head from non-empty unauthenticated state"
        );
    }

    let (record_count, set_digest) = provider_context_set_digest(store, &mut transaction).await?;
    if record_count != 0 {
        bail!("provider-context projection genesis is not empty");
    }
    let projection_key = hkdf_projection_hmac_key(&key, &store.scope().conversation_id);
    let head_hmac = projection_head_hmac(
        store,
        &projection_key,
        0,
        record_count,
        &set_digest,
        &key.key_ref,
    );
    let result = sqlx::query(
        "UPDATE provider_context_projection_head
         SET state = 'active', record_count = ?, set_digest = ?,
             key_ref = ?, head_hmac = ?
         WHERE singleton = 1
           AND state = 'uninitialized'
           AND schema_version = ?
           AND revision = 0
           AND record_count = 0
           AND set_digest IS NULL
           AND key_ref IS NULL
           AND head_hmac IS NULL",
    )
    .bind(record_count)
    .bind(set_digest.as_slice())
    .bind(&key.key_ref)
    .bind(head_hmac)
    .bind(PROJECTION_SCHEMA_VERSION)
    .execute(&mut *transaction)
    .await
    .context("failed to initialize provider-context projection head")?;
    require_single_cas(
        result.rows_affected(),
        "provider-context projection genesis",
    )?;
    transaction.commit().await?;
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderContextKind {
    EncryptedReasoning,
    OpenAiCompactedWindow,
    AnthropicCompaction,
}

impl ProviderContextKind {
    pub(crate) fn from_payload(payload: &ProviderContextPayload) -> Self {
        match payload {
            ProviderContextPayload::OpenAiCompactedWindow { .. } => Self::OpenAiCompactedWindow,
            ProviderContextPayload::AnthropicCompaction { .. } => Self::AnthropicCompaction,
            ProviderContextPayload::EncryptedReasoning { .. } => Self::EncryptedReasoning,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::EncryptedReasoning => "encrypted_reasoning",
            Self::OpenAiCompactedWindow => "open_ai_compacted_window",
            Self::AnthropicCompaction => "anthropic_compaction",
        }
    }
}

/// Extracts the request/message identity from a native provider-context record id.
///
/// Native compaction rows use the canonical id form
/// `{request_id}:{message_seq}:{wire_label}:{ordinal}` where `message_seq` and
/// `ordinal` are decimal and `wire_label` is `"_"` for unanchored native windows.
/// Because `request_id` may itself contain `':'` separators, this parses from the
/// fixed trailing fields rather than splitting naively from the start.
pub(crate) fn native_request_id_from_record_id(id: &str) -> Option<String> {
    let mut parts = id.rsplitn(4, ':');
    let ordinal = parts.next()?;
    if ordinal.parse::<u32>().is_err() {
        return None;
    }
    let wire_label = parts.next()?;
    if wire_label != "_" {
        return None;
    }
    let message_seq = parts.next()?;
    if message_seq.parse::<u64>().is_err() {
        return None;
    }
    let request_id = parts.next()?;
    if request_id.is_empty() {
        return None;
    }
    Some(request_id.to_owned())
}

/// Canonical idempotency key for provider-context records.
///
/// Regular reasoning items use `message_id:wire_item_index:ordinal:kind`;
/// native/dedicated compaction windows use `request_id:coverage_seq:fingerprint`.
/// The key is stored in `provider_context.idempotency_key` and used for
/// uniqueness and mutation-intent HMACs, while the row `id` remains a distinct
/// stable record identifier.
pub(crate) fn provider_context_idempotency_key(
    request_id: &str,
    item: &ProviderContextItem,
) -> String {
    match &item.payload {
        ProviderContextPayload::EncryptedReasoning { .. } => {
            let wire_label = item
                .wire_item_index
                .map_or_else(|| "_".to_owned(), |index| index.to_string());
            format!(
                "{}:{}:{}:{}",
                request_id,
                wire_label,
                item.ordinal,
                ProviderContextKind::from_payload(&item.payload).as_str()
            )
        }
        ProviderContextPayload::OpenAiCompactedWindow { coverage, .. }
        | ProviderContextPayload::AnthropicCompaction { coverage, .. } => {
            format!(
                "{}:{}:{}",
                request_id, coverage.through_message_seq, coverage.context_fingerprint
            )
        }
    }
}

/// A durable `provider_context` row.  Plaintext is not retained after
/// construction; the record exposes only the encrypted ciphertext and the
/// metadata required for ordering, eviction accounting, and mutation intent.
#[derive(Clone)]
pub(crate) struct EncryptedProviderContextRecord {
    id: String,
    message_id: Option<String>,
    message_seq: Option<u64>,
    wire_item_index: Option<u32>,
    item_ordinal: u32,
    idempotency_key: String,
    provider_instance_id: String,
    protocol: ApiProtocol,
    model: String,
    kind: ProviderContextKind,
    coverage_through_seq: Option<u64>,
    context_fingerprint: Option<String>,
    key_ref: String,
    ciphertext: Vec<u8>,
    eviction_tokens: u64,
    eviction_estimator_version: u32,
    created_at: String,
}

impl EncryptedProviderContextRecord {
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    pub(crate) fn message_id(&self) -> Option<&str> {
        self.message_id.as_deref()
    }

    pub(crate) fn message_seq(&self) -> Option<u64> {
        self.message_seq
    }

    pub(crate) fn kind(&self) -> ProviderContextKind {
        self.kind
    }

    pub(crate) fn coverage_through_seq(&self) -> Option<u64> {
        self.coverage_through_seq
    }

    pub(crate) fn context_fingerprint(&self) -> Option<&str> {
        self.context_fingerprint.as_deref()
    }

    pub(crate) fn eviction_tokens(&self) -> u64 {
        self.eviction_tokens
    }

    pub(crate) fn provider_instance_id(&self) -> &str {
        &self.provider_instance_id
    }

    pub(crate) fn protocol(&self) -> ApiProtocol {
        self.protocol
    }

    pub(crate) fn model(&self) -> &str {
        &self.model
    }

    pub(crate) fn key_ref(&self) -> &str {
        &self.key_ref
    }

    pub(crate) fn ciphertext(&self) -> &[u8] {
        &self.ciphertext
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn encrypt(
        item: &ProviderContextItem,
        provider_instance_id: impl Into<String>,
        protocol: ApiProtocol,
        model: impl Into<String>,
        id: impl Into<String>,
        idempotency_key: impl Into<String>,
        eviction_footprint: EvictionFootprint,
        data_key: &DataKeyMaterial,
        scope: &AgentScope,
    ) -> Result<Self> {
        if data_key.purpose != DataKeyPurpose::ProviderContext {
            bail!("provider-context records require a provider_context data key");
        }

        let id = id.into();
        let provider_instance_id = provider_instance_id.into();
        let model = model.into();
        let expected_origin = ProviderOrigin {
            provider_instance_id: provider_instance_id.clone(),
            protocol,
            model: model.clone(),
        };
        if item.provider_origin != expected_origin {
            bail!("provider-context item origin does not match the encryption origin arguments");
        }

        let expected_footprint = match eviction_footprint.estimator_version() {
            EVICTION_ESTIMATOR_VERSION_SERIALIZED_BYTES => {
                // Re-encrypting rows written before ReplayProbeV1 must retain
                // their authenticated legacy accounting rather than silently
                // recalculate it with the current estimator.
                legacy_serialized_bytes_eviction_footprint(&item.payload)
                    .context("failed to compute legacy provider-context eviction footprint")?
            }
            EVICTION_ESTIMATOR_VERSION_REPLAY_PROBE_V1 => {
                let spec = ModelSpec::from_origin(&item.provider_origin).ok_or_else(|| {
                    anyhow!("provider-context origin has no canonical model specification")
                })?;
                eviction_footprint_for_payload(&spec, &item.payload)
                    .context("failed to compute canonical provider-context eviction footprint")?
            }
            version => {
                bail!("unsupported provider-context eviction estimator version {version}");
            }
        };
        if eviction_footprint != expected_footprint {
            bail!(
                "provider-context eviction footprint does not match the canonical payload footprint"
            );
        }

        let aad = scope.row_aad("provider_context", &id, DataKeyPurpose::ProviderContext);
        let plaintext = Zeroizing::new(
            serde_json::to_vec(item).context("failed to serialize provider-context plaintext")?,
        );
        let ciphertext = encrypt_content(data_key, &plaintext, &aad)?;

        let (coverage_through_seq, context_fingerprint) = match &item.payload {
            ProviderContextPayload::OpenAiCompactedWindow { coverage, .. }
            | ProviderContextPayload::AnthropicCompaction { coverage, .. } => (
                Some(coverage.through_message_seq),
                Some(coverage.context_fingerprint.clone()),
            ),
            _ => (None, None),
        };

        let message_id = item.origin_message.as_ref().map(|a| a.message_id.clone());
        let message_seq = item.origin_message.as_ref().map(|a| a.message_seq);

        Ok(Self {
            id,
            message_id,
            message_seq,
            wire_item_index: item.wire_item_index,
            item_ordinal: item.ordinal,
            idempotency_key: idempotency_key.into(),
            provider_instance_id,
            protocol,
            model,
            kind: ProviderContextKind::from_payload(&item.payload),
            coverage_through_seq,
            context_fingerprint,
            key_ref: data_key.key_ref.clone(),
            ciphertext,
            eviction_tokens: eviction_footprint.eviction_tokens(),
            eviction_estimator_version: eviction_footprint.estimator_version(),
            created_at: Utc::now().to_rfc3339(),
        })
    }

    pub(crate) async fn insert<'e, E>(&self, executor: E) -> Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
    {
        sqlx::query(
            "INSERT INTO provider_context(
                id, message_id, message_seq, wire_item_index, item_ordinal,
                idempotency_key, provider_instance_id, protocol, model, kind,
                coverage_through_seq, context_fingerprint, key_ref, ciphertext,
                eviction_tokens, eviction_estimator_version, created_at
             ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&self.id)
        .bind(self.message_id.as_ref())
        .bind(
            self.message_seq
                .map(|v| sqlite_i64(v, "provider_context.message_seq"))
                .transpose()?,
        )
        .bind(self.wire_item_index.map(i64::from))
        .bind(i64::from(self.item_ordinal))
        .bind(&self.idempotency_key)
        .bind(&self.provider_instance_id)
        .bind(self.protocol.as_str())
        .bind(&self.model)
        .bind(self.kind.as_str())
        .bind(
            self.coverage_through_seq
                .map(|v| sqlite_i64(v, "provider_context.coverage_through_seq"))
                .transpose()?,
        )
        .bind(self.context_fingerprint.as_ref())
        .bind(&self.key_ref)
        .bind(&self.ciphertext)
        .bind(sqlite_i64(
            self.eviction_tokens,
            "provider_context.eviction_tokens",
        )?)
        .bind(i64::from(self.eviction_estimator_version))
        .bind(&self.created_at)
        .execute(executor)
        .await
        .context("failed to insert provider-context record")?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn insert_committed(&self, store: &Store) -> Result<()> {
        let mut transaction = store.pool().begin().await?;
        let checkpoint = verify_provider_context_projection_set(store, &mut transaction).await?;
        self.insert(&mut *transaction).await?;
        commit_provider_context_projection_set(store, &mut transaction, &checkpoint).await?;
        transaction.commit().await?;
        Ok(())
    }
}

impl ApiProtocol {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::OpenAiChatCompletions => "open_ai_chat_completions",
            Self::OpenAiResponses => "open_ai_responses",
            Self::AnthropicMessages => "anthropic_messages",
        }
    }
}

/// Encrypted payload stored in `provider_context_mutations.intent_ciphertext`.
/// The fields are grouped into a *semantic* subset (covered by `intent_hmac`)
/// and a *non-semantic* subset (`key_ref`, `ciphertext`, `created_at`) that is
/// excluded from the HMAC so that key rotation and re-encryption do not
/// invalidate the prepared intent.
#[derive(Clone, Serialize, Deserialize)]
struct FullIntent {
    variant: String,
    mutation_id: String,
    expected_latest_id: Option<String>,
    invalidate_ids: Vec<String>,
    provider_context_id: String,
    message_id: Option<String>,
    message_seq: Option<u64>,
    wire_item_index: Option<u32>,
    item_ordinal: u32,
    idempotency_key: String,
    provider_instance_id: String,
    protocol: String,
    model: String,
    kind: String,
    coverage_through_seq: Option<u64>,
    context_fingerprint: Option<String>,
    eviction_tokens: u64,
    eviction_estimator_version: u32,
    config_generation: u64,
    window_ordinal: u64,
    plaintext_hmac: Vec<u8>,
    // Non-semantic fields excluded from intent_hmac.
    key_ref: String,
    ciphertext: Vec<u8>,
    created_at: String,
}

impl FullIntent {
    fn is_replace(&self) -> bool {
        self.variant == "replace"
    }
}

/// Canonical, length-delimited byte serialization for the semantic subset of a
/// `FullIntent`.  The output is the exact payload fed to the HMAC.
fn intent_bytes(full: &FullIntent, include_witness: bool) -> Vec<u8> {
    let mut writer = CanonicalWriter::with_domain(INTENT_HMAC_DOMAIN);
    writer.field(full.variant.as_bytes());
    writer.field(full.mutation_id.as_bytes());
    writer.optional_field(
        include_witness
            .then_some(full.expected_latest_id.as_deref().map(str::as_bytes))
            .flatten(),
    );
    writer.field(&canonical_id_list(&full.invalidate_ids));
    writer.field(full.provider_context_id.as_bytes());
    writer.optional_field(full.message_id.as_deref().map(str::as_bytes));
    writer.field(&opt_u64_bytes(full.message_seq));
    writer.field(&opt_u32_bytes(full.wire_item_index));
    writer.field(full.item_ordinal.to_string().as_bytes());
    writer.field(full.idempotency_key.as_bytes());
    writer.field(full.provider_instance_id.as_bytes());
    writer.field(full.protocol.as_bytes());
    writer.field(full.model.as_bytes());
    writer.field(full.kind.as_bytes());
    writer.field(&opt_u64_bytes(full.coverage_through_seq));
    writer.optional_field(full.context_fingerprint.as_deref().map(str::as_bytes));
    writer.field(full.eviction_tokens.to_string().as_bytes());
    writer.field(full.eviction_estimator_version.to_string().as_bytes());
    writer.field(full.config_generation.to_string().as_bytes());
    writer.field(full.window_ordinal.to_string().as_bytes());
    writer.field(&full.plaintext_hmac);
    writer.finish()
}

fn semantic_intent_bytes(full: &FullIntent) -> Vec<u8> {
    intent_bytes(full, true)
}

/// Semantic subset excluding `expected_latest_id`, used to detect a CAS retry
/// where only the expected-latest witness changed.
fn stable_intent_bytes(full: &FullIntent) -> Vec<u8> {
    intent_bytes(full, false)
}

/// Returns whether the authenticated `expected_latest_id` witness is consistent
/// with the current replace head. The absent-head and absent-witness cases
/// match each other; an existing head requires its exact latest insert id.
fn expected_latest_matches_head(full: &FullIntent, head: Option<&(i64, i64, String)>) -> bool {
    match head {
        None => full.expected_latest_id.is_none(),
        Some((_, _, head_id)) => full.expected_latest_id.as_deref() == Some(head_id),
    }
}

struct CanonicalWriter(Vec<u8>);

impl CanonicalWriter {
    fn with_domain(domain: &[u8]) -> Self {
        Self(domain.to_vec())
    }

    fn field(&mut self, bytes: &[u8]) {
        self.0.extend(&(bytes.len() as u64).to_be_bytes());
        self.0.extend(bytes);
    }

    /// Writes an optional field with an explicit one-byte presence marker.
    /// `None` is encoded as `0`; `Some` is encoded as `1` followed by the
    /// length-delimited value, so `None`/`Some("")`/`Some(0)` are all distinct.
    fn optional_field(&mut self, bytes: Option<&[u8]>) {
        match bytes {
            None => self.0.push(0),
            Some(bytes) => {
                self.0.push(1);
                self.field(bytes);
            }
        }
    }

    fn finish(self) -> Vec<u8> {
        self.0
    }
}

fn canonical_id_list(ids: &[String]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(&(ids.len() as u64).to_be_bytes());
    for id in ids {
        bytes.extend(&(id.len() as u64).to_be_bytes());
        bytes.extend(id.as_bytes());
    }
    bytes
}

fn opt_u64_bytes(opt: Option<u64>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(2);
    match opt {
        None => bytes.push(0),
        Some(value) => {
            bytes.push(1);
            bytes.extend(value.to_string().into_bytes());
        }
    }
    bytes
}

fn opt_u32_bytes(opt: Option<u32>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(2);
    match opt {
        None => bytes.push(0),
        Some(value) => {
            bytes.push(1);
            bytes.extend(value.to_string().into_bytes());
        }
    }
    bytes
}

/// A prepared `Replace`/`Invalidate` intent ready to be persisted and applied.
#[derive(Debug)]
pub(crate) struct PreparedProviderContextMutation {
    mutation_id: String,
    intent_key_ref: String,
    intent_ciphertext: Vec<u8>,
    hmac_key_id: String,
    intent_hmac: Vec<u8>,
}

impl PreparedProviderContextMutation {
    pub(crate) fn intent_hmac(&self) -> &[u8] {
        &self.intent_hmac
    }
}

pub(crate) struct ProviderContextMutationBuilder {
    mutation_key: DataKeyMaterial,
    scope: AgentScope,
    mutation_id: String,
}

impl ProviderContextMutationBuilder {
    pub(crate) fn new(
        mutation_key: DataKeyMaterial,
        scope: AgentScope,
        mutation_id: impl Into<String>,
    ) -> Self {
        Self {
            mutation_key,
            scope,
            mutation_id: mutation_id.into(),
        }
    }

    pub(crate) fn build_invalidate(
        self,
        expected_latest_id: Option<String>,
        invalidate_ids: Vec<String>,
    ) -> Result<PreparedProviderContextMutation> {
        if invalidate_ids.is_empty() {
            bail!("Invalidate intent requires a non-empty target set");
        }
        let sorted = unique_sorted(invalidate_ids)?;
        self.build_full(
            "invalidate",
            expected_latest_id,
            sorted,
            None,
            0,
            0,
            Vec::new(),
        )
    }

    pub(crate) fn build_replace(
        self,
        expected_latest_id: Option<String>,
        invalidate_ids: Vec<String>,
        insert: &EncryptedProviderContextRecord,
        plaintext: &ProviderContextItem,
        config_generation: u64,
        window_ordinal: u64,
    ) -> Result<PreparedProviderContextMutation> {
        let sorted = unique_sorted(invalidate_ids)?;
        let plaintext_bytes = Zeroizing::new(
            serde_json::to_vec(plaintext)
                .context("failed to serialize provider-context plaintext for intent")?,
        );
        let intent_key = hkdf_intent_hmac_key(&self.mutation_key, &self.scope.conversation_id);
        let plaintext_hmac = hmac_sha256(&intent_key, PLAINTEXT_HMAC_DOMAIN, &plaintext_bytes);

        self.build_full(
            "replace",
            expected_latest_id,
            sorted,
            Some(insert),
            config_generation,
            window_ordinal,
            plaintext_hmac,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build_full(
        self,
        variant: &'static str,
        expected_latest_id: Option<String>,
        invalidate_ids: Vec<String>,
        insert: Option<&EncryptedProviderContextRecord>,
        config_generation: u64,
        window_ordinal: u64,
        plaintext_hmac: Vec<u8>,
    ) -> Result<PreparedProviderContextMutation> {
        if self.mutation_id.is_empty() {
            bail!("provider-context mutation_id must not be empty");
        }

        let full = FullIntent {
            variant: variant.to_owned(),
            mutation_id: self.mutation_id.clone(),
            expected_latest_id,
            invalidate_ids,
            provider_context_id: insert.map(|r| r.id.clone()).unwrap_or_default(),
            message_id: insert.and_then(|r| r.message_id.clone()),
            message_seq: insert.and_then(|r| r.message_seq),
            wire_item_index: insert.and_then(|r| r.wire_item_index),
            item_ordinal: insert.map(|r| r.item_ordinal).unwrap_or(1),
            idempotency_key: insert
                .map(|r| r.idempotency_key.clone())
                .unwrap_or_default(),
            provider_instance_id: insert
                .map(|r| r.provider_instance_id.clone())
                .unwrap_or_default(),
            protocol: insert
                .map(|r| r.protocol.as_str().to_owned())
                .unwrap_or_default(),
            model: insert.map(|r| r.model.clone()).unwrap_or_default(),
            kind: insert
                .map(|r| r.kind.as_str().to_owned())
                .unwrap_or_default(),
            coverage_through_seq: insert.and_then(|r| r.coverage_through_seq),
            context_fingerprint: insert.and_then(|r| r.context_fingerprint.clone()),
            eviction_tokens: insert.map(|r| r.eviction_tokens).unwrap_or(0),
            eviction_estimator_version: insert.map(|r| r.eviction_estimator_version).unwrap_or(1),
            config_generation,
            window_ordinal,
            plaintext_hmac,
            key_ref: insert.map(|r| r.key_ref.clone()).unwrap_or_default(),
            ciphertext: insert.map(|r| r.ciphertext.clone()).unwrap_or_default(),
            created_at: insert.map(|r| r.created_at.clone()).unwrap_or_default(),
        };

        let intent_key = hkdf_intent_hmac_key(&self.mutation_key, &self.scope.conversation_id);
        let semantic = semantic_intent_bytes(&full);
        let intent_hmac = hmac_sha256(&intent_key, INTENT_HMAC_DOMAIN, &semantic);

        let aad = self.scope.row_aad(
            "provider_context_mutations",
            &self.mutation_id,
            DataKeyPurpose::Mutation,
        );
        let full_json = Zeroizing::new(
            serde_json::to_vec(&full).context("failed to serialize full mutation intent")?,
        );
        let intent_ciphertext = encrypt_content(&self.mutation_key, &full_json, &aad)?;

        Ok(PreparedProviderContextMutation {
            mutation_id: self.mutation_id,
            intent_key_ref: self.mutation_key.key_ref.clone(),
            intent_ciphertext,
            hmac_key_id: INTENT_HMAC_KEY_ID.to_owned(),
            intent_hmac,
        })
    }
}

fn unique_sorted(ids: Vec<String>) -> Result<Vec<String>> {
    let set: BTreeSet<_> = ids.iter().cloned().collect();
    if set.len() != ids.len() {
        bail!("provider-context mutation invalidate_ids must be unique");
    }
    Ok(set.into_iter().collect())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ApplyOutcome {
    Applied,
    AlreadySatisfied,
    Superseded { reason: String },
}

#[derive(Debug, Default)]
struct InvalidatedIds {
    key_refs: BTreeSet<String>,
    deleted_ids: BTreeSet<String>,
}

/// EventWriter projection handle for a prepared `Replace`/`Invalidate`.
/// Construction is private to the provider-context builder; EventWriter loads
/// and revalidates the stored intent inside its transaction.
#[derive(Clone)]
pub(crate) struct ProviderContextMutation {
    pub(crate) mutation_id: String,
}

/// Size and prepared-key proof bundle computed outside the EventWriter
/// transaction for projection byte accounting and `revalidate_prepared_key_refs`.
pub(in crate::store) struct ProviderContextProjectionSize {
    pub(crate) size: usize,
    pub(crate) intent_key_ref: String,
    pub(crate) intent_key_proof: Vec<u8>,
    pub(crate) insert_key_ref: Option<String>,
    pub(crate) insert_key_proof: Option<Vec<u8>>,
}

/// Transactional owner for `provider_context_mutations` prepare/apply.
pub(crate) struct ProviderContextMutationApplier<'a> {
    store: &'a Store,
    #[cfg(test)]
    zero_before_delete_checks: AtomicUsize,
}

impl<'a> ProviderContextMutationApplier<'a> {
    pub(crate) fn new(store: &'a Store) -> Self {
        Self {
            store,
            #[cfg(test)]
            zero_before_delete_checks: AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    fn zero_before_delete_checks(&self) -> usize {
        self.zero_before_delete_checks.load(Ordering::Relaxed)
    }

    fn decrypt_full_intent(
        &self,
        mutation_key: &DataKeyMaterial,
        ciphertext: &[u8],
        aad: &RowAad,
        intent_key: &[u8],
        expected_hmac: &[u8],
        label: &str,
    ) -> Result<FullIntent> {
        let mut full_json = Zeroizing::new(
            decrypt_content(mutation_key, ciphertext, aad)
                .with_context(|| format!("failed to decrypt {label} mutation intent"))?,
        );
        let full: FullIntent = serde_json::from_slice(&full_json)
            .with_context(|| format!("{label} mutation intent is invalid"))?;
        let semantic = semantic_intent_bytes(&full);
        let recomputed = hmac_sha256(intent_key, INTENT_HMAC_DOMAIN, &semantic);
        if recomputed.as_slice().ct_eq(expected_hmac).unwrap_u8() != 1 {
            bail!("{label} provider-context mutation intent HMAC mismatch");
        }
        full_json.zeroize();
        Ok(full)
    }

    /// Remove durable replay copies for provider-context rows erased by a
    /// memory transition. Prepared replacements for an erased row are first
    /// terminalized so recovery cannot recreate the deleted plaintext.
    ///
    /// Returns provider-context key refs still required by other prepared
    /// replacement intents. Callers must preserve those keys even when no
    /// active provider-context row currently references them.
    pub(in crate::store) async fn scrub_erased_provider_context_intents(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        erased_ids: &BTreeSet<String>,
    ) -> Result<BTreeSet<String>> {
        if erased_ids.is_empty() {
            return Ok(BTreeSet::new());
        }

        let rows = sqlx::query(
            "SELECT mutation_id, state, intent_key_ref, intent_ciphertext, intent_hmac
             FROM provider_context_mutations
             WHERE intent_key_ref IN (
                 SELECT key_ref FROM data_keys
                 WHERE scope = 'conversation' AND conversation_id = ? AND state = 'active'
             )
             ORDER BY mutation_id",
        )
        .bind(&self.store.scope().conversation_id)
        .fetch_all(&mut **transaction)
        .await
        .context("failed to load provider-context intents for crypto-erasure")?;

        let mut protected_key_refs = BTreeSet::new();
        for row in rows {
            let mutation_id: String = row.try_get("mutation_id")?;
            let state: String = row.try_get("state")?;
            let intent_key_ref: String = row.try_get("intent_key_ref")?;
            let intent_ciphertext: Vec<u8> = row.try_get("intent_ciphertext")?;
            let stored_hmac: Vec<u8> = row.try_get("intent_hmac")?;
            let mutation_key = self
                .store
                .data_key_by_ref_in_transaction(transaction, &intent_key_ref)
                .await
                .with_context(|| {
                    format!("failed to load mutation key while scrubbing {mutation_id}")
                })?;
            let aad = self.store.scope().row_aad(
                "provider_context_mutations",
                &mutation_id,
                DataKeyPurpose::Mutation,
            );
            let intent_key =
                hkdf_intent_hmac_key(&mutation_key, &self.store.scope().conversation_id);
            let mut full = self.decrypt_full_intent(
                &mutation_key,
                &intent_ciphertext,
                &aad,
                &intent_key,
                &stored_hmac,
                "stored",
            )?;

            if !full.is_replace() {
                continue;
            }
            if !erased_ids.contains(&full.provider_context_id) {
                if state == "prepared" && !full.key_ref.is_empty() {
                    protected_key_refs.insert(full.key_ref);
                }
                continue;
            }

            if state == "prepared" {
                let result = sqlx::query(
                    "UPDATE provider_context_mutations
                     SET state = 'applied', finished_at = ?, terminal_reason = 'already_satisfied'
                     WHERE mutation_id = ? AND state = 'prepared'",
                )
                .bind(Utc::now().to_rfc3339())
                .bind(&mutation_id)
                .execute(&mut **transaction)
                .await
                .context("failed to terminalize erased provider-context intent")?;
                require_single_cas(
                    result.rows_affected(),
                    "ProviderContextMutationEraseTerminalize",
                )?;
            }

            // The encrypted insert is non-semantic and may be removed without
            // changing intent_hmac. Overwrite the old envelope first so SQLite
            // does not retain a replayable ciphertext in the live page.
            sqlx::query(
                "UPDATE provider_context_mutations
                 SET intent_ciphertext = zeroblob(length(intent_ciphertext))
                 WHERE mutation_id = ?",
            )
            .bind(&mutation_id)
            .execute(&mut **transaction)
            .await
            .context("failed to zero erased provider-context mutation intent")?;

            full.key_ref.clear();
            full.ciphertext.zeroize();
            full.ciphertext.clear();
            let mut full_json = Zeroizing::new(
                serde_json::to_vec(&full)
                    .context("failed to serialize scrubbed provider-context mutation intent")?,
            );
            let scrubbed_ciphertext = encrypt_content(&mutation_key, &full_json, &aad)?;
            full_json.zeroize();
            let result = sqlx::query(
                "UPDATE provider_context_mutations
                 SET intent_ciphertext = ?
                 WHERE mutation_id = ?",
            )
            .bind(scrubbed_ciphertext)
            .bind(&mutation_id)
            .execute(&mut **transaction)
            .await
            .context("failed to persist scrubbed provider-context mutation intent")?;
            require_single_cas(result.rows_affected(), "ProviderContextMutationEraseScrub")?;
        }

        Ok(protected_key_refs)
    }

    pub(crate) async fn prepare(&self, prepared: &PreparedProviderContextMutation) -> Result<()> {
        if prepared.mutation_id.is_empty() {
            bail!("mutation_id must not be empty");
        }

        let mut transaction = self.store.pool().begin().await?;
        let projection_checkpoint =
            verify_provider_context_projection_set(self.store, &mut transaction).await?;

        #[allow(clippy::type_complexity)]
        let existing: Option<(String, String, Vec<u8>, String, Vec<u8>, Vec<u8>)> = sqlx::query_as(
            "SELECT state, intent_key_ref, intent_hmac, hmac_key_id, intent_ciphertext, prepared_at
             FROM provider_context_mutations WHERE mutation_id = ?",
        )
        .bind(&prepared.mutation_id)
        .fetch_optional(&mut *transaction)
        .await
        .context("failed to load existing mutation row")?;

        let mutation_key = self
            .store
            .data_key_by_ref_in_transaction(&mut transaction, &prepared.intent_key_ref)
            .await
            .context("failed to load mutation key for prepare")?;
        let aad = self.store.scope().row_aad(
            "provider_context_mutations",
            &prepared.mutation_id,
            DataKeyPurpose::Mutation,
        );
        let intent_key = hkdf_intent_hmac_key(&mutation_key, &self.store.scope().conversation_id);

        if let Some((state, key_ref, hmac, hmac_key_id, ciphertext, _prepared_at)) = existing {
            if state != "prepared" {
                bail!(
                    "provider-context mutation {} is already terminal",
                    prepared.mutation_id
                );
            }
            if key_ref != prepared.intent_key_ref || hmac_key_id != prepared.hmac_key_id {
                bail!("conflicting provider-context mutation intent already exists");
            }
            if hmac == prepared.intent_hmac {
                transaction.commit().await?;
                return Ok(());
            }

            // HMAC differs: this may be a CAS retry where only the expected-latest
            // witness changed.  Verify the existing intent, decrypt both, and compare
            // the stable semantic subset (which excludes expected_latest_id).
            let old_full = self.decrypt_full_intent(
                &mutation_key,
                &ciphertext,
                &aad,
                &intent_key,
                &hmac,
                "existing",
            )?;
            let new_full = self.decrypt_full_intent(
                &mutation_key,
                &prepared.intent_ciphertext,
                &aad,
                &intent_key,
                &prepared.intent_hmac,
                "new",
            )?;

            if stable_intent_bytes(&old_full) != stable_intent_bytes(&new_full) {
                bail!("conflicting provider-context mutation intent already exists");
            }

            if !self
                .is_intent_latest_candidate(&mut transaction, &new_full, &intent_key)
                .await?
            {
                bail!(
                    "CAS update rejected: provider-context intent is no longer the latest candidate"
                );
            }

            let result = sqlx::query(
                "UPDATE provider_context_mutations
                 SET intent_ciphertext = ?, intent_hmac = ?, prepared_at = ?
                 WHERE mutation_id = ? AND state = 'prepared'",
            )
            .bind(&prepared.intent_ciphertext)
            .bind(&prepared.intent_hmac)
            .bind(Utc::now().to_rfc3339())
            .bind(&prepared.mutation_id)
            .execute(&mut *transaction)
            .await
            .context("failed to CAS-update provider-context mutation intent")?;
            require_single_cas(
                result.rows_affected(),
                "ProviderContextMutationPrepareRefresh",
            )?;

            commit_provider_context_projection_set(
                self.store,
                &mut transaction,
                &projection_checkpoint,
            )
            .await?;
            transaction.commit().await?;
            return Ok(());
        }

        let new_full = self.decrypt_full_intent(
            &mutation_key,
            &prepared.intent_ciphertext,
            &aad,
            &intent_key,
            &prepared.intent_hmac,
            "new",
        )?;
        let head = self
            .load_replace_head(&mut transaction, &new_full, &intent_key)
            .await?;
        if !expected_latest_matches_head(&new_full, head.as_ref()) {
            bail!(
                "provider-context mutation intent expected_latest_id does not match the current head"
            );
        }

        sqlx::query(
            "INSERT INTO provider_context_mutations(
                mutation_id, state, intent_key_ref, intent_ciphertext, hmac_key_id,
                intent_hmac, prepared_at, finished_at, terminal_reason
             ) VALUES(?, 'prepared', ?, ?, ?, ?, ?, NULL, NULL)",
        )
        .bind(&prepared.mutation_id)
        .bind(&prepared.intent_key_ref)
        .bind(&prepared.intent_ciphertext)
        .bind(&prepared.hmac_key_id)
        .bind(&prepared.intent_hmac)
        .bind(Utc::now().to_rfc3339())
        .execute(&mut *transaction)
        .await
        .context("failed to prepare provider-context mutation")?;

        commit_provider_context_projection_set(
            self.store,
            &mut transaction,
            &projection_checkpoint,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Recompute the projection byte bound and the prepared-key proofs for a
    /// pending provider-context mutation.  The intent is decrypted and its HMAC
    /// verified so that EventWriter can account for its exact durable cost and
    /// bind the key material into the EventBatch revalidation check.
    pub(in crate::store) async fn verify_and_size(
        &self,
        mutation_id: &str,
    ) -> Result<ProviderContextProjectionSize> {
        let mut transaction = self.store.pool().begin().await?;
        let (full, mutation_key, _intent_key) = self
            .load_and_verify_full_intent(&mut transaction, mutation_id)
            .await?;

        let size = full
            .invalidate_ids
            .iter()
            .map(String::len)
            .sum::<usize>()
            .saturating_add(full.provider_context_id.len())
            .saturating_add(full.ciphertext.len())
            .saturating_add(512);

        let (insert_key_ref, insert_key_proof) = if full.is_replace() && !full.key_ref.is_empty() {
            let provider_context_key = self
                .store
                .data_key_by_ref_in_transaction(&mut transaction, &full.key_ref)
                .await?;
            if provider_context_key.purpose != DataKeyPurpose::ProviderContext {
                bail!(
                    "provider-context insert key {full_key_ref} has wrong purpose",
                    full_key_ref = full.key_ref
                );
            }
            let proof = super::crypto::keyed_proof(
                &provider_context_key,
                PREPARED_KEY_MATERIAL_PROOF_DOMAIN,
                PREPARED_KEY_MATERIAL_PROOF,
            );
            (Some(full.key_ref.clone()), Some(proof))
        } else {
            (None, None)
        };

        transaction.commit().await?;
        Ok(ProviderContextProjectionSize {
            size,
            intent_key_ref: mutation_key.key_ref.clone(),
            intent_key_proof: super::crypto::keyed_proof(
                &mutation_key,
                PREPARED_KEY_MATERIAL_PROOF_DOMAIN,
                PREPARED_KEY_MATERIAL_PROOF,
            ),
            insert_key_ref,
            insert_key_proof,
        })
    }

    /// Apply one prepared provider-context mutation inside an EventWriter
    /// transaction.  The intent and, for Replace, the encrypted plaintext HMAC
    /// are revalidated before any durable writes.
    pub(in crate::store) async fn apply_in_transaction(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        mutation_id: &str,
    ) -> Result<ApplyOutcome> {
        let projection_checkpoint =
            verify_provider_context_projection_set(self.store, transaction).await?;
        let (full, _mutation_key, intent_key) = self
            .load_and_verify_full_intent(transaction, mutation_id)
            .await?;

        if full.is_replace() {
            if full.key_ref.is_empty() || full.ciphertext.is_empty() {
                bail!("Replace provider-context mutation is missing encrypted insert");
            }
            let provider_context_key = self
                .store
                .data_key_by_ref_in_transaction(transaction, &full.key_ref)
                .await?;
            if provider_context_key.purpose != DataKeyPurpose::ProviderContext {
                bail!("provider-context insert key has wrong purpose");
            }
            let aad = self.store.scope().row_aad(
                "provider_context",
                &full.provider_context_id,
                DataKeyPurpose::ProviderContext,
            );
            let plaintext = Zeroizing::new(
                decrypt_content(&provider_context_key, &full.ciphertext, &aad).context(
                    "failed to decrypt provider-context insert for plaintext HMAC check",
                )?,
            );
            let expected = hmac_sha256(&intent_key, PLAINTEXT_HMAC_DOMAIN, &plaintext);
            if expected.as_slice().ct_eq(&full.plaintext_hmac).unwrap_u8() != 1 {
                bail!("Replace provider-context mutation plaintext HMAC mismatch");
            }
        }

        self.apply_full_intent(
            transaction,
            &full,
            mutation_id,
            &intent_key,
            &projection_checkpoint,
        )
        .await
    }

    async fn load_and_verify_full_intent(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        mutation_id: &str,
    ) -> Result<(FullIntent, DataKeyMaterial, [u8; 32])> {
        let row = sqlx::query(
            "SELECT state, intent_key_ref, intent_ciphertext, intent_hmac
             FROM provider_context_mutations WHERE mutation_id = ?",
        )
        .bind(mutation_id)
        .fetch_optional(&mut **transaction)
        .await
        .context("failed to load mutation for apply")?;

        let Some(row) = row else {
            bail!("mutation {mutation_id} not found");
        };

        let state: String = row.try_get("state")?;
        if state != "prepared" {
            bail!("mutation {mutation_id} is not in prepared state");
        }

        let intent_key_ref: String = row.try_get("intent_key_ref")?;
        let intent_ciphertext: Vec<u8> = row.try_get("intent_ciphertext")?;
        let stored_hmac: Vec<u8> = row.try_get("intent_hmac")?;

        let mutation_key = self
            .store
            .data_key_by_ref_in_transaction(transaction, &intent_key_ref)
            .await?;
        let aad = self.store.scope().row_aad(
            "provider_context_mutations",
            mutation_id,
            DataKeyPurpose::Mutation,
        );
        let mut full_json =
            Zeroizing::new(decrypt_content(&mutation_key, &intent_ciphertext, &aad)?);
        let full: FullIntent = serde_json::from_slice(&full_json)
            .context("failed to deserialize full mutation intent")?;

        let semantic = semantic_intent_bytes(&full);
        let intent_key = hkdf_intent_hmac_key(&mutation_key, &self.store.scope().conversation_id);
        let recomputed = hmac_sha256(&intent_key, INTENT_HMAC_DOMAIN, &semantic);
        if recomputed.as_slice().ct_eq(&stored_hmac).unwrap_u8() != 1 {
            bail!("provider-context mutation intent HMAC mismatch");
        }
        full_json.zeroize();

        Ok((full, mutation_key, intent_key))
    }

    async fn apply_full_intent(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        full: &FullIntent,
        mutation_id: &str,
        intent_key: &[u8],
        projection_checkpoint: &ProviderContextProjectionCheckpoint,
    ) -> Result<ApplyOutcome> {
        if full.is_replace() {
            let scope_key = replace_scope_key(
                &full.provider_instance_id,
                &full.protocol,
                &full.model,
                &full.kind,
                &self.store.scope().conversation_id,
                intent_key,
            );

            let head: Option<(i64, i64, String)> = sqlx::query_as(
                "SELECT max_config_generation, max_window_ordinal, latest_insert_id
                 FROM provider_context_replace_heads WHERE scope_key = ?",
            )
            .bind(&scope_key)
            .fetch_optional(&mut **transaction)
            .await?;

            if !expected_latest_matches_head(full, head.as_ref()) {
                self.finish_mutation(
                    transaction,
                    mutation_id,
                    "superseded",
                    Some("newer_replace"),
                )
                .await?;
                commit_provider_context_projection_set(
                    self.store,
                    transaction,
                    projection_checkpoint,
                )
                .await?;
                return Ok(ApplyOutcome::Superseded {
                    reason: "newer_replace".to_owned(),
                });
            }

            if let Some(expected) = &full.expected_latest_id
                && expected != &full.provider_context_id
                && !full.invalidate_ids.contains(expected)
            {
                bail!("Replace expected_latest_id must be included in invalidate_ids");
            }

            let candidate_gen = sqlite_i64(full.config_generation, "config_generation")?;
            let candidate_ord = sqlite_i64(full.window_ordinal, "window_ordinal")?;

            if let Some((head_gen, head_ord, head_id)) = head {
                if (candidate_gen, candidate_ord) < (head_gen, head_ord) {
                    self.finish_mutation(
                        transaction,
                        mutation_id,
                        "superseded",
                        Some("newer_replace"),
                    )
                    .await?;
                    commit_provider_context_projection_set(
                        self.store,
                        transaction,
                        projection_checkpoint,
                    )
                    .await?;
                    return Ok(ApplyOutcome::Superseded {
                        reason: "newer_replace".to_owned(),
                    });
                }
                if (candidate_gen, candidate_ord) == (head_gen, head_ord)
                    && head_id == full.provider_context_id
                {
                    self.finish_mutation(
                        transaction,
                        mutation_id,
                        "applied",
                        Some("already_satisfied"),
                    )
                    .await?;
                    commit_provider_context_projection_set(
                        self.store,
                        transaction,
                        projection_checkpoint,
                    )
                    .await?;
                    return Ok(ApplyOutcome::AlreadySatisfied);
                }
                if (candidate_gen, candidate_ord) == (head_gen, head_ord)
                    && head_id != full.provider_context_id
                {
                    self.finish_mutation(
                        transaction,
                        mutation_id,
                        "superseded",
                        Some("newer_replace"),
                    )
                    .await?;
                    commit_provider_context_projection_set(
                        self.store,
                        transaction,
                        projection_checkpoint,
                    )
                    .await?;
                    return Ok(ApplyOutcome::Superseded {
                        reason: "newer_replace".to_owned(),
                    });
                }
            }

            let invalidated = self
                .invalidate_ids(transaction, &full.invalidate_ids)
                .await?;

            let tokens = sqlite_i64(full.eviction_tokens, "provider_context.eviction_tokens")?;

            sqlx::query(
                "INSERT INTO provider_context(
                    id, message_id, message_seq, wire_item_index, item_ordinal,
                    idempotency_key, provider_instance_id, protocol, model, kind,
                    coverage_through_seq, context_fingerprint, key_ref, ciphertext,
                    eviction_tokens, eviction_estimator_version, created_at
                 ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&full.provider_context_id)
            .bind(full.message_id.as_ref())
            .bind(
                full.message_seq
                    .map(|v| sqlite_i64(v, "provider_context.message_seq"))
                    .transpose()?,
            )
            .bind(full.wire_item_index.map(i64::from))
            .bind(i64::from(full.item_ordinal))
            .bind(&full.idempotency_key)
            .bind(&full.provider_instance_id)
            .bind(&full.protocol)
            .bind(&full.model)
            .bind(&full.kind)
            .bind(
                full.coverage_through_seq
                    .map(|v| sqlite_i64(v, "provider_context.coverage_through_seq"))
                    .transpose()?,
            )
            .bind(full.context_fingerprint.as_ref())
            .bind(&full.key_ref)
            .bind(&full.ciphertext)
            .bind(tokens)
            .bind(i64::from(full.eviction_estimator_version))
            .bind(&full.created_at)
            .execute(&mut **transaction)
            .await?;

            // The new row's eviction footprint must be charged to the owning
            // extant L0 batch, mirroring the MessageEnd path. A prepared
            // Replace may apply after that batch has sealed or compacted.
            self.increment_batch_footprint(transaction, full.message_id.as_deref(), tokens)
                .await?;

            sqlx::query(
                "INSERT INTO provider_context_replace_heads(
                    scope_key, max_config_generation, max_window_ordinal, latest_insert_id, updated_at
                 ) VALUES(?, ?, ?, ?, ?)
                 ON CONFLICT(scope_key) DO UPDATE SET
                    max_config_generation = excluded.max_config_generation,
                    max_window_ordinal = excluded.max_window_ordinal,
                    latest_insert_id = excluded.latest_insert_id,
                    updated_at = excluded.updated_at",
            )
            .bind(&scope_key)
            .bind(candidate_gen)
            .bind(candidate_ord)
            .bind(&full.provider_context_id)
            .bind(Utc::now().to_rfc3339())
            .execute(&mut **transaction)
            .await?;

            self.finish_mutation(transaction, mutation_id, "applied", None)
                .await?;
            commit_provider_context_projection_set(self.store, transaction, projection_checkpoint)
                .await?;
            self.destroy_unreferenced_provider_context_keys(transaction, invalidated.key_refs)
                .await?;
            Ok(ApplyOutcome::Applied)
        } else {
            if full.invalidate_ids.is_empty() {
                bail!("Invalidate intent requires a non-empty target set");
            }
            let invalidated = self
                .invalidate_ids(transaction, &full.invalidate_ids)
                .await?;
            if invalidated.deleted_ids.is_empty() {
                self.finish_mutation(
                    transaction,
                    mutation_id,
                    "applied",
                    Some("already_satisfied"),
                )
                .await?;
                commit_provider_context_projection_set(
                    self.store,
                    transaction,
                    projection_checkpoint,
                )
                .await?;
                Ok(ApplyOutcome::AlreadySatisfied)
            } else {
                self.finish_mutation(transaction, mutation_id, "applied", None)
                    .await?;
                commit_provider_context_projection_set(
                    self.store,
                    transaction,
                    projection_checkpoint,
                )
                .await?;
                self.destroy_unreferenced_provider_context_keys(transaction, invalidated.key_refs)
                    .await?;
                Ok(ApplyOutcome::Applied)
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn apply(&self, mutation_id: &str) -> Result<ApplyOutcome> {
        use super::event_writer::{EventBatch, EventWrite, EventWriter};
        EventWriter::new(std::sync::Arc::new(self.store.clone()))
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![super::event_writer::Projection::ProviderContextMutation(
                        ProviderContextMutation {
                            mutation_id: mutation_id.to_owned(),
                        },
                    )],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .context("EventWriter provider-context apply failed")?;
        Self::outcome_from_row(self.store.pool(), mutation_id).await
    }

    #[cfg(test)]
    pub(crate) async fn recover(&self) -> Result<()> {
        use super::event_writer::{EventBatch, EventWrite, EventWriter};
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT mutation_id FROM provider_context_mutations
             WHERE state = 'prepared'
             ORDER BY prepared_at, mutation_id",
        )
        .fetch_all(self.store.pool())
        .await
        .context("failed to list prepared provider-context mutations")?;

        for (mutation_id,) in rows {
            EventWriter::new(std::sync::Arc::new(self.store.clone()))
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: None,
                        projections: vec![
                            super::event_writer::Projection::ProviderContextMutation(
                                ProviderContextMutation {
                                    mutation_id: mutation_id.clone(),
                                },
                            ),
                        ],
                    }],
                    injected_commands: Vec::new(),
                })
                .await
                .with_context(|| {
                    format!("failed to recover provider-context mutation {mutation_id}")
                })?;
        }
        Ok(())
    }

    #[cfg(test)]
    async fn outcome_from_row(pool: &sqlx::SqlitePool, mutation_id: &str) -> Result<ApplyOutcome> {
        let row = sqlx::query(
            "SELECT state, terminal_reason FROM provider_context_mutations WHERE mutation_id = ?",
        )
        .bind(mutation_id)
        .fetch_optional(pool)
        .await
        .context("failed to load mutation outcome")?;
        match row {
            Some(row) => {
                let state: String = row.try_get("state")?;
                let reason: Option<String> = row.try_get("terminal_reason")?;
                match (state.as_str(), reason.as_deref()) {
                    ("applied", Some("already_satisfied")) => Ok(ApplyOutcome::AlreadySatisfied),
                    ("superseded", Some("newer_replace")) => Ok(ApplyOutcome::Superseded {
                        reason: "newer_replace".to_owned(),
                    }),
                    ("applied", _) => Ok(ApplyOutcome::Applied),
                    _ => bail!("unexpected mutation state {state} with reason {reason:?}"),
                }
            }
            None => bail!("mutation {mutation_id} not found after apply"),
        }
    }

    #[cfg(test)]
    pub(crate) async fn inspect_stored_insert(
        &self,
        mutation_id: &str,
    ) -> Result<(String, Option<String>, String, usize)> {
        let mut transaction = self.store.pool().begin().await?;
        let row = sqlx::query(
            "SELECT state, terminal_reason, intent_key_ref, intent_ciphertext, intent_hmac
             FROM provider_context_mutations WHERE mutation_id = ?",
        )
        .bind(mutation_id)
        .fetch_one(&mut *transaction)
        .await?;
        let state: String = row.try_get("state")?;
        let terminal_reason: Option<String> = row.try_get("terminal_reason")?;
        let intent_key_ref: String = row.try_get("intent_key_ref")?;
        let intent_ciphertext: Vec<u8> = row.try_get("intent_ciphertext")?;
        let stored_hmac: Vec<u8> = row.try_get("intent_hmac")?;
        let mutation_key = self
            .store
            .data_key_by_ref_in_transaction(&mut transaction, &intent_key_ref)
            .await?;
        let aad = self.store.scope().row_aad(
            "provider_context_mutations",
            mutation_id,
            DataKeyPurpose::Mutation,
        );
        let intent_key = hkdf_intent_hmac_key(&mutation_key, &self.store.scope().conversation_id);
        let full = self.decrypt_full_intent(
            &mutation_key,
            &intent_ciphertext,
            &aad,
            &intent_key,
            &stored_hmac,
            "inspected",
        )?;
        transaction.commit().await?;
        Ok((state, terminal_reason, full.key_ref, full.ciphertext.len()))
    }

    fn replace_scope_key(&self, full: &FullIntent, intent_key: &[u8]) -> String {
        replace_scope_key(
            &full.provider_instance_id,
            &full.protocol,
            &full.model,
            &full.kind,
            &self.store.scope().conversation_id,
            intent_key,
        )
    }

    async fn load_replace_head(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        full: &FullIntent,
        intent_key: &[u8],
    ) -> Result<Option<(i64, i64, String)>> {
        if !full.is_replace() {
            return Ok(None);
        }
        let scope_key = self.replace_scope_key(full, intent_key);
        let head: Option<(i64, i64, String)> = sqlx::query_as(
            "SELECT max_config_generation, max_window_ordinal, latest_insert_id
             FROM provider_context_replace_heads WHERE scope_key = ?",
        )
        .bind(&scope_key)
        .fetch_optional(&mut **transaction)
        .await?;
        Ok(head)
    }

    async fn is_intent_latest_candidate(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        full: &FullIntent,
        intent_key: &[u8],
    ) -> Result<bool> {
        let head = self
            .load_replace_head(transaction, full, intent_key)
            .await?;
        if !expected_latest_matches_head(full, head.as_ref()) {
            return Ok(false);
        }
        if !full.is_replace() {
            return Ok(true);
        }

        let candidate_gen = sqlite_i64(full.config_generation, "config_generation")?;
        let candidate_ord = sqlite_i64(full.window_ordinal, "window_ordinal")?;

        match head {
            None => Ok(true),
            Some((head_gen, head_ord, head_id)) => Ok((candidate_gen, candidate_ord)
                > (head_gen, head_ord)
                || ((candidate_gen, candidate_ord) == (head_gen, head_ord)
                    && full.provider_context_id == head_id)),
        }
    }

    async fn invalidate_ids(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        ids: &[String],
    ) -> Result<InvalidatedIds> {
        let mut result = InvalidatedIds::default();
        for id in ids {
            let row = sqlx::query(
                "SELECT message_id, eviction_tokens, key_ref
                 FROM provider_context
                 WHERE id = ?",
            )
            .bind(id)
            .fetch_optional(&mut **transaction)
            .await?;

            let Some(row) = row else {
                // The target is already gone; converge rather than fail.
                continue;
            };

            let message_id: Option<String> = row.try_get("message_id")?;
            let tokens: i64 = row.try_get("eviction_tokens")?;
            let key_ref: String = row.try_get("key_ref")?;

            // The row exists, so fail closed if its data key belongs to another conversation.
            let key_scope: Option<(String, String)> =
                sqlx::query_as("SELECT scope, conversation_id FROM data_keys WHERE key_ref = ?")
                    .bind(&key_ref)
                    .fetch_optional(&mut **transaction)
                    .await?;
            match key_scope {
                Some((scope, conversation_id))
                    if scope == "conversation"
                        && conversation_id == self.store.scope().conversation_id => {}
                _ => {
                    bail!(
                        "provider-context row {id} is outside the active conversation scope or does not exist"
                    );
                }
            }

            // Best-effort overwrite the encrypted payload before deleting the
            // row: SQLite free pages may retain bytes. Destroying the
            // unreferenced data key below is the actual crypto-erasure
            // guarantee.
            sqlx::query(
                "UPDATE provider_context
                 SET ciphertext = zeroblob(length(ciphertext))
                 WHERE id = ?",
            )
            .bind(id)
            .execute(&mut **transaction)
            .await
            .context("failed to crypto-erase provider-context row before delete")?;

            #[cfg(test)]
            self.assert_zeroed_before_delete(transaction, id).await?;

            if let Some(message_id) = message_id {
                self.decrement_batch_footprint(transaction, &message_id, tokens)
                    .await?;
            }

            sqlx::query("DELETE FROM provider_context WHERE id = ?")
                .bind(id)
                .execute(&mut **transaction)
                .await?;
            result.key_refs.insert(key_ref);
            result.deleted_ids.insert(id.clone());
        }
        Ok(result)
    }

    #[cfg(test)]
    async fn assert_zeroed_before_delete(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        id: &str,
    ) -> Result<()> {
        let ciphertext: Vec<u8> =
            sqlx::query_scalar("SELECT ciphertext FROM provider_context WHERE id = ?")
                .bind(id)
                .fetch_optional(&mut **transaction)
                .await?
                .ok_or_else(|| anyhow!("provider-context row {id} disappeared before delete"))?;
        assert!(
            ciphertext.iter().all(|&b| b == 0),
            "provider-context ciphertext for {id} was not zeroed before delete"
        );
        self.zero_before_delete_checks
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn destroy_unreferenced_provider_context_keys(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        key_refs: BTreeSet<String>,
    ) -> Result<()> {
        for key_ref in key_refs {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE key_ref = ?")
                    .bind(&key_ref)
                    .fetch_one(&mut **transaction)
                    .await?;
            if count == 0 {
                self.store
                    .destroy_conversation_key_ref_in_transaction(transaction, &key_ref)
                    .await
                    .with_context(|| {
                        format!("failed to crypto-erase provider-context data key {key_ref}")
                    })?;
            }
        }
        Ok(())
    }

    async fn decrement_batch_footprint(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        message_id: &str,
        tokens: i64,
    ) -> Result<()> {
        if tokens <= 0 {
            return Ok(());
        }
        let batch_id: Option<String> =
            sqlx::query_scalar("SELECT batch_id FROM memory_batch_messages WHERE message_id = ?")
                .bind(message_id)
                .fetch_optional(&mut **transaction)
                .await?;

        let Some(batch_id) = batch_id else {
            return Ok(());
        };
        let row = sqlx::query(
            "UPDATE memory_batches
             SET eviction_footprint_tokens = eviction_footprint_tokens - ?
             WHERE id = ? AND eviction_footprint_tokens >= ?
             RETURNING eviction_footprint_tokens",
        )
        .bind(tokens)
        .bind(&batch_id)
        .bind(tokens)
        .fetch_optional(&mut **transaction)
        .await?;
        if row.is_none() {
            bail!("batch {batch_id} footprint underflow or missing when subtracting {tokens}");
        }
        Ok(())
    }

    async fn increment_batch_footprint(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        message_id: Option<&str>,
        tokens: i64,
    ) -> Result<()> {
        if tokens <= 0 {
            return Ok(());
        }
        let Some(message_id) = message_id else {
            bail!(
                "provider-context row with non-zero eviction_tokens is missing anchor message_id"
            );
        };
        let row = sqlx::query(
            "SELECT mb.id, mb.eviction_footprint_tokens, mb.state
             FROM memory_batches mb
             JOIN memory_batch_messages mbm ON mbm.batch_id = mb.id
             WHERE mbm.message_id = ? AND mb.layer = ?",
        )
        .bind(message_id)
        .bind(MemoryLayer::L0.as_i64())
        .fetch_optional(&mut **transaction)
        .await
        .context("failed to locate owning L0 batch for provider-context insert")?;

        let Some(row) = row else {
            bail!("L0 batch not found for provider-context anchor message {message_id}");
        };
        let batch_id: String = row.try_get("id")?;
        let current: i64 = row.try_get("eviction_footprint_tokens")?;
        let state: String = row.try_get("state")?;
        match state.as_str() {
            "open" | "sealed" | "compacting" | "compact_failed" | "compacted" => {}
            "promoted" | "dropped" => {
                bail!(
                    "provider-context anchor message {message_id} belongs to terminal L0 batch {batch_id} in state {state}"
                );
            }
            _ => bail!("L0 batch {batch_id} has unknown state {state}"),
        }
        let new = current
            .checked_add(tokens)
            .ok_or_else(|| anyhow!("memory batch {batch_id} eviction_footprint_tokens overflow"))?;
        let result = sqlx::query(
            "UPDATE memory_batches
             SET eviction_footprint_tokens = ?
             WHERE id = ?",
        )
        .bind(new)
        .bind(&batch_id)
        .execute(&mut **transaction)
        .await?;
        if result.rows_affected() != 1 {
            bail!("failed to increment eviction footprint for batch {batch_id}");
        }
        Ok(())
    }

    async fn finish_mutation(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        mutation_id: &str,
        state: &str,
        terminal_reason: Option<&str>,
    ) -> Result<()> {
        let result = sqlx::query(
            "UPDATE provider_context_mutations
             SET state = ?, finished_at = ?, terminal_reason = ?
             WHERE mutation_id = ?",
        )
        .bind(state)
        .bind(Utc::now().to_rfc3339())
        .bind(terminal_reason)
        .bind(mutation_id)
        .execute(&mut **transaction)
        .await?;
        require_single_cas(result.rows_affected(), "ProviderContextMutationFinish")?;
        Ok(())
    }
}

fn replace_scope_key(
    provider_instance_id: &str,
    protocol: &str,
    model: &str,
    kind: &str,
    conversation_id: &str,
    intent_key: &[u8],
) -> String {
    let mut writer = CanonicalWriter::with_domain(SCOPE_KEY_DOMAIN);
    writer.field(conversation_id.as_bytes());
    writer.field(provider_instance_id.as_bytes());
    writer.field(protocol.as_bytes());
    writer.field(model.as_bytes());
    writer.field(kind.as_bytes());
    let digest = hmac_sha256(
        intent_key,
        b"sumi-provider-context-scope-digest/v1",
        &writer.finish(),
    );
    encode_hex(&digest)
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize]);
        out.push(HEX[(byte & 0x0f) as usize]);
    }
    String::from_utf8(out).expect("hex digits are valid UTF-8")
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;
    use sqlx::Row;

    use super::*;
    use crate::memory::estimate::{
        EvictionFootprint, ProviderContextItemWithFootprint, eviction_footprint_for_payload,
        legacy_serialized_bytes_eviction_footprint, native_canonical_window_footprint,
    };
    use crate::provider::model::ModelSpec;
    use crate::provider::types::{
        ApiProtocol, AssistantContent, AssistantMessage, ContextMessage, Message,
        NativeCompactionCoverage, ProviderContextAnchor, ProviderContextItem,
        ProviderContextPayload, ProviderOrigin, StopReason, Usage,
    };
    use crate::store::{
        DataKeyPurpose, DurableEvent, EventBatch, EventWrite, EventWriter,
        MemoryBatchMessageRecord, MemoryBatchRecord, MemoryBatchState, MemoryTransition,
        Projection, ProviderContextKeyAnchor, Store,
    };

    fn dummy_footprint() -> EvictionFootprint {
        // Native compaction windows and many mutation/invalidation tests do not
        // need a real reasoning footprint; the canonical zero footprint is enough.
        native_canonical_window_footprint()
    }

    async fn store() -> Store {
        Store::session_test_store("conversation-1")
            .await
            .expect("open test store")
    }

    #[tokio::test]
    async fn projection_verifier_preflights_mutation_and_replace_head_bytes_before_paging() {
        let store = store().await;
        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        sqlx::query(
            "INSERT INTO provider_context_mutations(
                mutation_id, state, intent_key_ref, intent_ciphertext,
                hmac_key_id, intent_hmac, prepared_at, finished_at, terminal_reason
             ) VALUES(
                'oversized-intent', 'prepared', ?, zeroblob(2048),
                'intent-hmac', zeroblob(32), 'now', NULL, NULL
             )",
        )
        .bind(&mutation_key.key_ref)
        .execute(store.pool())
        .await
        .expect("insert oversized mutation fixture");
        let mut transaction = store
            .pool()
            .begin()
            .await
            .expect("begin verifier preflight");
        let error =
            preflight_provider_context_projection_bounds_with_limits(&mut transaction, 100, 1024)
                .await
                .expect_err("mutation ciphertext must be included in verifier preflight");
        assert!(error.to_string().contains("encoded bytes"), "{error:#}");
        transaction.rollback().await.expect("rollback preflight");

        sqlx::query("DELETE FROM provider_context_mutations")
            .execute(store.pool())
            .await
            .expect("remove mutation fixture");
        sqlx::query(
            "INSERT INTO provider_context_replace_heads(
                scope_key, max_config_generation, max_window_ordinal,
                latest_insert_id, updated_at
             ) VALUES('oversized-head', 1, 1, ?, 'now')",
        )
        .bind("x".repeat(2048))
        .execute(store.pool())
        .await
        .expect("insert oversized replace-head fixture");
        let mut transaction = store
            .pool()
            .begin()
            .await
            .expect("begin verifier preflight");
        let error =
            preflight_provider_context_projection_bounds_with_limits(&mut transaction, 100, 1024)
                .await
                .expect_err("replace-head text must be included in verifier preflight");
        assert!(error.to_string().contains("encoded bytes"), "{error:#}");
    }

    async fn seed_message(store: &Store, id: &str, seq: u64) -> anyhow::Result<()> {
        // These provider-context unit fixtures exercise row-local provider
        // semantics rather than the transcript/event projection contract.
        // Freeze the empty EventWriter checkpoint before the deliberate direct
        // transcript insert so later provider mutation writes do not treat the
        // fixture as authenticated lifecycle history.
        EventWriter::new(std::sync::Arc::new(store.clone()))
            .initialize_recovery_checkpoint()
            .await?;
        let key = store
            .conversation_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint transcript key");
        sqlx::query(
            "INSERT INTO messages(
                id, seq, role, raw_key_ref, raw_ciphertext, payload, search_text,
                redaction_version, interrupted, created_at
             ) VALUES(?, ?, 'user', ?, X'00', '{}', '', 1, 0, 'now')",
        )
        .bind(id)
        .bind(sqlite_i64(seq, "messages.seq")?)
        .bind(&key.key_ref)
        .execute(store.pool())
        .await?;
        Ok(())
    }

    async fn seed_message_in_open_l0_batch(
        store: &Store,
        message_id: &str,
        seq: u64,
        footprint_tokens: i64,
    ) -> anyhow::Result<String> {
        seed_message(store, message_id, seq).await?;
        let batch_id = uuid::Uuid::now_v7().to_string();
        EventWriter::new(std::sync::Arc::new(store.clone()))
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(DurableEvent::memory_maintenance(
                        "fixture_provider_context_batch",
                    )?),
                    projections: vec![Projection::MemoryTransition(MemoryTransition {
                        batch_inserts: vec![MemoryBatchRecord::new(
                            batch_id.clone(),
                            MemoryLayer::L0,
                            0,
                            0,
                            MemoryBatchState::Open,
                            0,
                            footprint_tokens,
                        )],
                        membership_inserts: vec![MemoryBatchMessageRecord {
                            batch_id: batch_id.clone(),
                            message_id: message_id.to_owned(),
                            ord: 1,
                        }],
                        ..Default::default()
                    })],
                }],
                injected_commands: Vec::new(),
            })
            .await?;
        Ok(batch_id)
    }

    async fn seed_non_message_event(store: &Store, seq: u64) -> anyhow::Result<()> {
        EventWriter::new(std::sync::Arc::new(store.clone()))
            .initialize_recovery_checkpoint()
            .await?;
        let key = store
            .conversation_key(DataKeyPurpose::Event)
            .await
            .expect("mint event key");
        sqlx::query(
            "INSERT INTO agent_events(
                seq, event_type, internal_metadata, raw_key_ref, raw_ciphertext,
                envelope, redaction_version, created_at
             ) VALUES(?, 'test_non_message', '{}', ?, X'00', '{}', 1, 'now')",
        )
        .bind(sqlite_i64(seq, "agent_events.seq")?)
        .bind(&key.key_ref)
        .execute(store.pool())
        .await?;
        Ok(())
    }

    fn reasoning_origin() -> ProviderOrigin {
        ProviderOrigin {
            provider_instance_id: "provider-instance-1".to_owned(),
            protocol: ApiProtocol::OpenAiResponses,
            model: "model-1".to_owned(),
        }
    }

    fn valid_reasoning_item() -> serde_json::Value {
        json!({
            "type": "reasoning",
            "id": "rs-test",
            "encrypted_content": "opaque-reasoning",
            "summary": [],
        })
    }

    fn reasoning_footprint(item: &ProviderContextItem) -> EvictionFootprint {
        let spec = ModelSpec::from_origin(&item.provider_origin)
            .expect("test origin must resolve to a ModelSpec");
        eviction_footprint_for_payload(&spec, &item.payload)
            .expect("test reasoning payload must be footprintable")
    }

    fn reasoning_item(message_id: impl Into<String>, message_seq: u64) -> ProviderContextItem {
        let origin = reasoning_origin();
        ProviderContextItem {
            origin_message: Some(ProviderContextAnchor {
                message_id: message_id.into(),
                message_seq,
            }),
            wire_item_index: Some(0),
            ordinal: 0,
            provider_origin: origin.clone(),
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: origin.protocol,
                item: valid_reasoning_item(),
            },
        }
    }

    async fn reasoning_record(
        store: &Store,
        message_id: &str,
        message_seq: u64,
        id: &str,
    ) -> EncryptedProviderContextRecord {
        reasoning_record_with(store, message_id, message_seq, id, 0, 0).await
    }

    #[tokio::test]
    async fn encrypt_rejects_noncanonical_eviction_footprint_before_persistence() {
        let store = store().await;
        let item = reasoning_item("message-1", 7);
        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "message-1:7".to_owned(),
            })
            .await
            .expect("mint reasoning anchor key");
        let canonical = reasoning_footprint(&item);
        let mismatched = EvictionFootprint::from_saved(
            canonical.estimator_version(),
            canonical.replay_wire_bytes(),
            canonical.eviction_tokens() + 1,
        )
        .expect("construct mismatched footprint");
        let origin = reasoning_origin();

        let result = EncryptedProviderContextRecord::encrypt(
            &item,
            &origin.provider_instance_id,
            origin.protocol,
            &origin.model,
            "pc-mismatched-footprint",
            provider_context_idempotency_key("message-1", &item),
            mismatched,
            &key,
            store.scope(),
        );
        let error = match result {
            Ok(_) => panic!("encryption must reject a noncanonical footprint"),
            Err(error) => error,
        };
        assert!(format!("{error:#}").contains("does not match the canonical payload footprint"));

        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_context")
            .fetch_one(store.pool())
            .await
            .expect("count provider-context rows");
        assert_eq!(rows, 0, "rejected records must not be persisted");
    }

    fn reasoning_item_with(
        message_id: impl Into<String>,
        message_seq: u64,
        wire_item_index: u32,
        ordinal: u32,
    ) -> ProviderContextItem {
        let origin = reasoning_origin();
        ProviderContextItem {
            origin_message: Some(ProviderContextAnchor {
                message_id: message_id.into(),
                message_seq,
            }),
            wire_item_index: Some(wire_item_index),
            ordinal,
            provider_origin: origin.clone(),
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: origin.protocol,
                item: valid_reasoning_item(),
            },
        }
    }

    fn reasoning_item_with_content(
        message_id: impl Into<String>,
        message_seq: u64,
        wire_item_index: u32,
        ordinal: u32,
        content: &str,
    ) -> ProviderContextItem {
        let origin = reasoning_origin();
        ProviderContextItem {
            origin_message: Some(ProviderContextAnchor {
                message_id: message_id.into(),
                message_seq,
            }),
            wire_item_index: Some(wire_item_index),
            ordinal,
            provider_origin: origin.clone(),
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: origin.protocol,
                item: json!({
                    "type": "reasoning",
                    "id": "rs-test",
                    "encrypted_content": content,
                    "summary": [],
                }),
            },
        }
    }

    async fn reasoning_record_with_content(
        store: &Store,
        message_id: &str,
        message_seq: u64,
        id: &str,
        wire_item_index: u32,
        ordinal: u32,
        content: &str,
    ) -> EncryptedProviderContextRecord {
        let anchor = ProviderContextKeyAnchor {
            conversation_id: store.scope().conversation_id.clone(),
            anchor_id: format!("{message_id}:{message_seq}"),
        };
        let key = store
            .provider_context_key(&anchor)
            .await
            .expect("mint reasoning anchor key");
        let item =
            reasoning_item_with_content(message_id, message_seq, wire_item_index, ordinal, content);
        let origin = reasoning_origin();
        EncryptedProviderContextRecord::encrypt(
            &item,
            &origin.provider_instance_id,
            origin.protocol,
            &origin.model,
            id,
            provider_context_idempotency_key(message_id, &item),
            reasoning_footprint(&item),
            &key,
            store.scope(),
        )
        .expect("encrypt reasoning record")
    }

    async fn reasoning_record_with(
        store: &Store,
        message_id: &str,
        message_seq: u64,
        id: &str,
        wire_item_index: u32,
        ordinal: u32,
    ) -> EncryptedProviderContextRecord {
        let anchor = ProviderContextKeyAnchor {
            conversation_id: store.scope().conversation_id.clone(),
            anchor_id: format!("{message_id}:{message_seq}"),
        };
        let key = store
            .provider_context_key(&anchor)
            .await
            .expect("mint reasoning anchor key");
        let item = reasoning_item_with(message_id, message_seq, wire_item_index, ordinal);
        let origin = reasoning_origin();
        EncryptedProviderContextRecord::encrypt(
            &item,
            &origin.provider_instance_id,
            origin.protocol,
            &origin.model,
            id,
            provider_context_idempotency_key(message_id, &item),
            reasoning_footprint(&item),
            &key,
            store.scope(),
        )
        .expect("encrypt reasoning record")
    }

    fn assistant_message(origin: ProviderOrigin) -> Message {
        Message::Assistant(AssistantMessage {
            content: vec![AssistantContent::Text {
                text: "assistant".to_owned(),
                wire_item_index: 0,
            }],
            model: origin.model.clone(),
            provider: origin.provider_instance_id.clone(),
            origin,
            usage: Usage::default(),
            stop_reason: StopReason::Stop,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: Utc::now(),
        })
    }

    async fn data_key_state(store: &Store, key_ref: &str) -> Option<String> {
        sqlx::query_scalar::<_, String>("SELECT state FROM data_keys WHERE key_ref = ?")
            .bind(key_ref)
            .fetch_optional(store.pool())
            .await
            .expect("read data key state")
    }

    #[tokio::test]
    async fn migration_rejects_null_orphan_wrong_anchor_and_duplicate_ordinal() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        // message_id non-null but message_seq null violates the paired-null CHECK.
        let null_seq = sqlx::query(
            "INSERT INTO provider_context(
                id, message_id, message_seq, item_ordinal, idempotency_key,
                provider_instance_id, protocol, model, kind, key_ref, ciphertext,
                eviction_tokens, eviction_estimator_version, created_at
             ) VALUES('pc-null', 'message-1', NULL, 1, 'key', 'inst', 'p', 'm', 'k', 'kref', X'00', 0, 1, 'now')",
        )
        .execute(store.pool())
        .await;
        assert!(null_seq.is_err());

        // message_id null with non-zero eviction_tokens violates reasoning-anchor CHECK.
        let orphan_reasoning = sqlx::query(
            "INSERT INTO provider_context(
                id, message_id, message_seq, item_ordinal, idempotency_key,
                provider_instance_id, protocol, model, kind, key_ref, ciphertext,
                eviction_tokens, eviction_estimator_version, created_at
             ) VALUES('pc-orphan-evict', NULL, NULL, 1, 'key2', 'inst', 'p', 'm', 'k', 'kref', X'00', 5, 1, 'now')",
        )
        .execute(store.pool())
        .await;
        assert!(orphan_reasoning.is_err());

        // Nonexistent message anchor fails the composite FK.
        let orphan_fk = sqlx::query(
            "INSERT INTO provider_context(
                id, message_id, message_seq, item_ordinal, idempotency_key,
                provider_instance_id, protocol, model, kind, key_ref, ciphertext,
                eviction_tokens, eviction_estimator_version, created_at
             ) VALUES('pc-orphan-fk', 'missing', 7, 1, 'key3', 'inst', 'p', 'm', 'k', 'kref', X'00', 0, 1, 'now')",
        )
        .execute(store.pool())
        .await;
        assert!(orphan_fk.is_err());

        // Wrong seq for an existing message fails the composite FK.
        let wrong_anchor = sqlx::query(
            "INSERT INTO provider_context(
                id, message_id, message_seq, item_ordinal, idempotency_key,
                provider_instance_id, protocol, model, kind, key_ref, ciphertext,
                eviction_tokens, eviction_estimator_version, created_at
             ) VALUES('pc-wrong', 'message-1', 99, 1, 'key4', 'inst', 'p', 'm', 'k', 'kref', X'00', 0, 1, 'now')",
        )
        .execute(store.pool())
        .await;
        assert!(wrong_anchor.is_err());

        // Insert a valid reasoning row.
        let record = reasoning_record(&store, "message-1", 7, "pc-1").await;
        record.insert_committed(&store).await.unwrap();

        // Duplicate (message_id, wire_item_index, item_ordinal) must fail.
        let record2 = reasoning_record(&store, "message-1", 7, "pc-2").await;
        let result = record2.insert_committed(&store).await;
        assert!(result.is_err(), "duplicate ordinal must be rejected");
    }

    #[tokio::test]
    async fn durable_state_commitment_detects_deletion_of_lone_native_row() {
        let store = store().await;
        seed_message(&store, "coverage-message", 1).await.unwrap();
        let id = insert_native_compaction(&store, "lone-native", &native_compaction_item(false, 1))
            .await;

        let mut transaction = store.pool().begin().await.unwrap();
        verify_provider_context_projection_set(&store, &mut transaction)
            .await
            .expect("committed lone native row must verify");
        transaction.commit().await.unwrap();

        sqlx::query("DELETE FROM provider_context WHERE id = ?")
            .bind(&id)
            .execute(store.pool())
            .await
            .unwrap();

        let mut transaction = store.pool().begin().await.unwrap();
        let error = verify_provider_context_projection_set(&store, &mut transaction)
            .await
            .expect_err("deleting the only row must not collapse to an authenticated empty set");
        assert!(
            format!("{error:#}").contains("authenticated commitment"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn durable_state_commitment_detects_deletion_of_lone_prepared_mutation() {
        let store = store().await;
        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let applier = ProviderContextMutationApplier::new(&store);
        let prepared = ProviderContextMutationBuilder::new(
            mutation_key,
            store.scope().clone(),
            "prepared-only".to_owned(),
        )
        .build_invalidate(None, vec!["not-present".to_owned()])
        .expect("build invalidate");
        applier.prepare(&prepared).await.expect("prepare mutation");

        sqlx::query("DELETE FROM provider_context_mutations WHERE mutation_id = ?")
            .bind("prepared-only")
            .execute(store.pool())
            .await
            .unwrap();

        let mut transaction = store.pool().begin().await.unwrap();
        let error = verify_provider_context_projection_set(&store, &mut transaction)
            .await
            .expect_err("deleting a prepared replay intent must fail closed");
        assert!(
            format!("{error:#}").contains("authenticated commitment"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn durable_state_commitment_detects_replace_head_deletion() {
        let store = store().await;
        seed_message_in_open_l0_batch(&store, "message-1", 7, 1_000_000)
            .await
            .unwrap();
        let record = reasoning_record(&store, "message-1", 7, "replace-head-row").await;
        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let applier = ProviderContextMutationApplier::new(&store);
        let prepared = ProviderContextMutationBuilder::new(
            mutation_key,
            store.scope().clone(),
            "replace-head-mutation".to_owned(),
        )
        .build_replace(
            None,
            Vec::new(),
            &record,
            &reasoning_item("message-1", 7),
            1,
            1,
        )
        .expect("build replace");
        applier.prepare(&prepared).await.expect("prepare replace");
        assert_eq!(
            applier.apply("replace-head-mutation").await.unwrap(),
            ApplyOutcome::Applied
        );

        let mut transaction = store.pool().begin().await.unwrap();
        verify_provider_context_projection_set(&store, &mut transaction)
            .await
            .expect("applied Replace must leave committed durable state");
        transaction.commit().await.unwrap();

        let deleted = sqlx::query("DELETE FROM provider_context_replace_heads")
            .execute(store.pool())
            .await
            .unwrap();
        assert_eq!(deleted.rows_affected(), 1);

        let mut transaction = store.pool().begin().await.unwrap();
        let error = verify_provider_context_projection_set(&store, &mut transaction)
            .await
            .expect_err("deleting a Replace CAS head must fail closed");
        assert!(
            format!("{error:#}").contains("authenticated commitment"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn durable_state_commitment_rejects_head_hmac_tamper() {
        let store = store().await;
        sqlx::query(
            "UPDATE provider_context_projection_head
             SET head_hmac = zeroblob(length(head_hmac))",
        )
        .execute(store.pool())
        .await
        .unwrap();

        let mut transaction = store.pool().begin().await.unwrap();
        let error = verify_provider_context_projection_set(&store, &mut transaction)
            .await
            .expect_err("projection-head HMAC tamper must fail closed");
        assert!(
            format!("{error:#}").contains("projection head HMAC mismatch"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn mutation_prepare_is_idempotent_and_rejects_conflicting_hmac() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();
        let record = reasoning_record(&store, "message-1", 7, "pc-1").await;

        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let applier = ProviderContextMutationApplier::new(&store);
        let scope = store.scope().clone();

        let prepared =
            ProviderContextMutationBuilder::new(mutation_key, scope, "mutation-1".to_owned())
                .build_replace(None, vec![], &record, &reasoning_item("message-1", 7), 1, 1)
                .expect("build replace intent");

        applier.prepare(&prepared).await.expect("first prepare");
        applier
            .prepare(&prepared)
            .await
            .expect("idempotent prepare");

        // Re-preparing with the same mutation_id and same intent must keep the row.
        let hmac: Vec<u8> = sqlx::query_scalar(
            "SELECT intent_hmac FROM provider_context_mutations WHERE mutation_id = ?",
        )
        .bind("mutation-1")
        .fetch_one(store.pool())
        .await
        .expect("fetch prepared intent");
        assert_eq!(hmac, prepared.intent_hmac());

        // A different plaintext under the same mutation_id is a conflicting intent.
        let mut different_item = reasoning_item("message-1", 7);
        different_item.ordinal = 2;
        let different_origin = reasoning_origin();
        let different_record = EncryptedProviderContextRecord::encrypt(
            &different_item,
            &different_origin.provider_instance_id,
            different_origin.protocol,
            &different_origin.model,
            "pc-different",
            provider_context_idempotency_key("message-1", &different_item),
            reasoning_footprint(&different_item),
            &store
                .provider_context_key(&ProviderContextKeyAnchor {
                    conversation_id: store.scope().conversation_id.clone(),
                    anchor_id: "message-1:7".to_owned(),
                })
                .await
                .expect("mint different anchor key"),
            store.scope(),
        )
        .expect("encrypt different reasoning");

        let mutation_key2 = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("reuse mutation key");
        let conflicting = ProviderContextMutationBuilder::new(
            mutation_key2,
            store.scope().clone(),
            "mutation-1".to_owned(),
        )
        .build_replace(None, vec![], &different_record, &different_item, 1, 1)
        .expect("build conflicting intent");

        let error = applier
            .prepare(&conflicting)
            .await
            .expect_err("conflicting HMAC must be rejected");
        assert!(error.to_string().contains("conflicting"));
    }

    #[tokio::test]
    async fn replace_head_is_monotonic_and_idempotent() {
        let store = store().await;
        seed_message_in_open_l0_batch(&store, "message-1", 7, 1_000_000)
            .await
            .unwrap();

        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let applier = ProviderContextMutationApplier::new(&store);
        let scope = store.scope().clone();

        // First Replace: config=1, window=1.
        let a = reasoning_record(&store, "message-1", 7, "pc-a").await;
        let intent_a = ProviderContextMutationBuilder::new(
            mutation_key,
            scope.clone(),
            "replace-a".to_owned(),
        )
        .build_replace(None, vec![], &a, &reasoning_item("message-1", 7), 1, 1)
        .expect("build replace-a");
        applier.prepare(&intent_a).await.unwrap();
        assert_eq!(
            applier.apply("replace-a").await.unwrap(),
            ApplyOutcome::Applied
        );

        // Older Replace is superseded.
        let b = reasoning_record(&store, "message-1", 7, "pc-b").await;
        let mutation_key_b = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("reuse mutation key");
        let intent_b = ProviderContextMutationBuilder::new(
            mutation_key_b,
            scope.clone(),
            "replace-b".to_owned(),
        )
        .build_replace(
            Some("pc-a".to_owned()),
            vec!["pc-a".to_owned()],
            &b,
            &reasoning_item("message-1", 7),
            0,
            0,
        )
        .expect("build replace-b");
        applier.prepare(&intent_b).await.unwrap();
        let outcome_b = applier.apply("replace-b").await.unwrap();
        assert!(
            matches!(outcome_b, ApplyOutcome::Superseded { reason } if reason == "newer_replace")
        );

        // Equal (gen, ord) with the same insert id is already satisfied.
        let mutation_key_a2 = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("reuse mutation key");
        let intent_a2 = ProviderContextMutationBuilder::new(
            mutation_key_a2,
            scope.clone(),
            "replace-a-again".to_owned(),
        )
        .build_replace(
            Some("pc-a".to_owned()),
            vec![],
            &a,
            &reasoning_item("message-1", 7),
            1,
            1,
        )
        .expect("build replace-a-again");
        applier.prepare(&intent_a2).await.unwrap();
        assert_eq!(
            applier.apply("replace-a-again").await.unwrap(),
            ApplyOutcome::AlreadySatisfied
        );

        // Equal (gen, ord) with a different insert id is superseded.
        let mutation_key_c = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("reuse mutation key");
        let intent_c = ProviderContextMutationBuilder::new(
            mutation_key_c,
            scope.clone(),
            "replace-c".to_owned(),
        )
        .build_replace(
            Some("pc-a".to_owned()),
            vec!["pc-a".to_owned()],
            &b,
            &reasoning_item("message-1", 7),
            1,
            1,
        )
        .expect("build replace-c");
        applier.prepare(&intent_c).await.unwrap();
        let outcome_c = applier.apply("replace-c").await.unwrap();
        assert!(
            matches!(outcome_c, ApplyOutcome::Superseded { reason } if reason == "newer_replace")
        );

        // Strictly greater Replace advances the head and deletes the prior row.
        let e = reasoning_record(&store, "message-1", 7, "pc-e").await;
        let mutation_key_e = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("reuse mutation key");
        let intent_e =
            ProviderContextMutationBuilder::new(mutation_key_e, scope, "replace-e".to_owned())
                .build_replace(
                    Some("pc-a".to_owned()),
                    vec!["pc-a".to_owned()],
                    &e,
                    &reasoning_item("message-1", 7),
                    2,
                    2,
                )
                .expect("build replace-e");
        applier.prepare(&intent_e).await.unwrap();
        assert_eq!(
            applier.apply("replace-e").await.unwrap(),
            ApplyOutcome::Applied
        );

        let remaining: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_context WHERE id IN ('pc-a', 'pc-e')",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(remaining, 1);

        let head = sqlx::query(
            "SELECT max_config_generation, max_window_ordinal, latest_insert_id
             FROM provider_context_replace_heads",
        )
        .fetch_one(store.pool())
        .await
        .expect("read replace head");
        assert_eq!(head.get::<i64, _>("max_config_generation"), 2);
        assert_eq!(head.get::<i64, _>("max_window_ordinal"), 2);
        assert_eq!(head.get::<String, _>("latest_insert_id"), "pc-e");
    }

    #[tokio::test]
    async fn invalidate_deletes_targets_and_marks_mutation_applied() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let record = reasoning_record(&store, "message-1", 7, "pc-1").await;
        record.insert_committed(&store).await.unwrap();

        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let applier = ProviderContextMutationApplier::new(&store);
        let intent = ProviderContextMutationBuilder::new(
            mutation_key,
            store.scope().clone(),
            "invalidate-1".to_owned(),
        )
        .build_invalidate(None, vec!["pc-1".to_owned()])
        .expect("build invalidate intent");

        applier.prepare(&intent).await.unwrap();
        assert_eq!(
            applier.apply("invalidate-1").await.unwrap(),
            ApplyOutcome::Applied
        );

        let remaining: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
                .bind("pc-1")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(remaining, 0);

        let state: String = sqlx::query_scalar(
            "SELECT state FROM provider_context_mutations WHERE mutation_id = ?",
        )
        .bind("invalidate-1")
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(state, "applied");
    }

    #[tokio::test]
    async fn replace_prepare_enforces_expected_latest_id_and_allows_cas_update() {
        let store = store().await;
        seed_message_in_open_l0_batch(&store, "message-1", 7, 1_000_000)
            .await
            .unwrap();

        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let applier = ProviderContextMutationApplier::new(&store);
        let scope = store.scope().clone();

        // First Replace creates head pc-a with (gen=1, ord=1).
        let a = reasoning_record(&store, "message-1", 7, "pc-a").await;
        let intent_a = ProviderContextMutationBuilder::new(
            mutation_key,
            scope.clone(),
            "replace-a".to_owned(),
        )
        .build_replace(None, vec![], &a, &reasoning_item("message-1", 7), 1, 1)
        .expect("build replace-a");
        applier.prepare(&intent_a).await.unwrap();
        assert_eq!(
            applier.apply("replace-a").await.unwrap(),
            ApplyOutcome::Applied
        );

        // A newer Replace with a stale expected_latest_id is rejected at prepare.
        let b = reasoning_record(&store, "message-1", 7, "pc-b").await;
        let mutation_key_b = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("reuse mutation key");
        let stale = ProviderContextMutationBuilder::new(
            mutation_key_b,
            scope.clone(),
            "replace-b".to_owned(),
        )
        .build_replace(
            Some("stale-id".to_owned()),
            vec!["pc-a".to_owned()],
            &b,
            &reasoning_item("message-1", 7),
            2,
            2,
        )
        .expect("build stale replace-b");
        applier
            .prepare(&stale)
            .await
            .expect_err("stale expected_latest_id must be rejected");

        // CAS update: re-prepare a pending intent after correcting only expected_latest_id.
        let mutation_key_d = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("reuse mutation key");
        let pending = ProviderContextMutationBuilder::new(
            mutation_key_d,
            scope.clone(),
            "cas-pending".to_owned(),
        )
        .build_replace(
            Some("pc-a".to_owned()),
            vec![],
            &a,
            &reasoning_item("message-1", 7),
            1,
            1,
        )
        .expect("build cas pending without expected");
        let pending_key_ref = pending.intent_key_ref.clone();
        applier.prepare(&pending).await.unwrap();

        let reused_key = store
            .data_key_by_ref(&pending_key_ref)
            .await
            .expect("reload mutation key for CAS update");
        let corrected = ProviderContextMutationBuilder::new(
            reused_key,
            scope.clone(),
            "cas-pending".to_owned(),
        )
        .build_replace(
            Some("pc-a".to_owned()),
            vec![],
            &a,
            &reasoning_item("message-1", 7),
            1,
            1,
        )
        .expect("build cas pending with corrected expected");
        applier.prepare(&corrected).await.unwrap();

        let stored_hmac: Vec<u8> = sqlx::query_scalar(
            "SELECT intent_hmac FROM provider_context_mutations WHERE mutation_id = ?",
        )
        .bind("cas-pending")
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(stored_hmac, corrected.intent_hmac());

        assert_eq!(
            applier.apply("cas-pending").await.unwrap(),
            ApplyOutcome::AlreadySatisfied
        );

        // The same newer Replace with the correct expected head succeeds.
        let mutation_key_c = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("reuse mutation key");
        let correct =
            ProviderContextMutationBuilder::new(mutation_key_c, scope, "replace-b".to_owned())
                .build_replace(
                    Some("pc-a".to_owned()),
                    vec!["pc-a".to_owned()],
                    &b,
                    &reasoning_item("message-1", 7),
                    2,
                    2,
                )
                .expect("build correct replace-b");
        applier.prepare(&correct).await.unwrap();
        assert_eq!(
            applier.apply("replace-b").await.unwrap(),
            ApplyOutcome::Applied
        );
    }

    #[tokio::test]
    async fn recover_applies_prepared_provider_context_mutations() {
        let store = store().await;
        seed_message_in_open_l0_batch(&store, "message-1", 7, 1_000_000)
            .await
            .unwrap();

        let record = reasoning_record(&store, "message-1", 7, "pc-1").await;
        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let applier = ProviderContextMutationApplier::new(&store);
        let intent = ProviderContextMutationBuilder::new(
            mutation_key,
            store.scope().clone(),
            "recover-1".to_owned(),
        )
        .build_replace(None, vec![], &record, &reasoning_item("message-1", 7), 1, 1)
        .expect("build replace intent");

        applier.prepare(&intent).await.unwrap();
        applier.recover().await.expect("recover prepared mutations");

        let state: String = sqlx::query_scalar(
            "SELECT state FROM provider_context_mutations WHERE mutation_id = ?",
        )
        .bind("recover-1")
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(state, "applied");

        let remaining: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
                .bind("pc-1")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(remaining, 1);
    }

    #[tokio::test]
    async fn reasoning_idempotency_key_is_message_wire_ordinal_kind() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let a = reasoning_record(&store, "message-1", 7, "pc-a").await;
        a.insert_committed(&store).await.unwrap();

        let b = reasoning_record(&store, "message-1", 7, "pc-b").await;
        let error = b
            .insert_committed(&store)
            .await
            .expect_err("same canonical reasoning idempotency key must collide");
        let message = format!("{error:#}");
        assert!(
            message.contains("idempotency_key") || message.contains("UNIQUE"),
            "{message}"
        );
    }

    fn openai_responses_origin() -> ProviderOrigin {
        ProviderOrigin {
            provider_instance_id: "provider-instance-1".to_owned(),
            protocol: ApiProtocol::OpenAiResponses,
            model: "model-1".to_owned(),
        }
    }

    fn openai_responses_origin_with_model(model: &str) -> ProviderOrigin {
        ProviderOrigin {
            provider_instance_id: "provider-instance-1".to_owned(),
            protocol: ApiProtocol::OpenAiResponses,
            model: model.to_owned(),
        }
    }

    fn native_compaction_item(anthropic: bool, coverage: u64) -> ProviderContextItem {
        let provider_origin = ProviderOrigin {
            provider_instance_id: "provider-instance-1".to_owned(),
            protocol: if anthropic {
                ApiProtocol::AnthropicMessages
            } else {
                ApiProtocol::OpenAiResponses
            },
            model: "model-1".to_owned(),
        };
        let coverage = NativeCompactionCoverage {
            through_message_seq: coverage,
            context_fingerprint: "fp-1".to_owned(),
        };
        ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin,
            payload: if anthropic {
                ProviderContextPayload::AnthropicCompaction {
                    block: json!({"type": "compaction", "content": "summary"}),
                    coverage,
                }
            } else {
                ProviderContextPayload::OpenAiCompactedWindow {
                    items: vec![json!({
                        "type": "compaction",
                        "id": "native-cmp",
                        "encrypted_content": "opaque",
                    })],
                    coverage,
                }
            },
        }
    }

    async fn insert_native_compaction(
        store: &Store,
        request_id: &str,
        item: &ProviderContextItem,
    ) -> String {
        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "native:0".to_owned(),
            })
            .await
            .expect("mint native provider-context key");
        let id = format!("{request_id}:4:_:{}", item.ordinal);
        EncryptedProviderContextRecord::encrypt(
            item,
            &item.provider_origin.provider_instance_id,
            item.provider_origin.protocol,
            &item.provider_origin.model,
            &id,
            provider_context_idempotency_key(request_id, item),
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt native compaction")
        .insert_committed(store)
        .await
        .expect("insert native compaction");
        id
    }

    #[tokio::test]
    async fn compaction_idempotency_key_is_request_coverage_fingerprint() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "message-1:7".to_owned(),
            })
            .await
            .unwrap();

        let mut base = ProviderContextItem {
            origin_message: Some(ProviderContextAnchor {
                message_id: "message-1".to_owned(),
                message_seq: 7,
            }),
            wire_item_index: None,
            ordinal: 1,
            provider_origin: openai_responses_origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({
                    "type": "compaction",
                    "id": "cmp-a",
                    "encrypted_content": "opaque-a",
                })],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 7,
                    context_fingerprint: "fp-a".to_owned(),
                },
            },
        };

        let a = EncryptedProviderContextRecord::encrypt(
            &base,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-1",
            "pc-a",
            provider_context_idempotency_key("message-1", &base),
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt compaction a");
        a.insert_committed(&store).await.unwrap();

        // Same request/coverage/fingerprint with a different ordinal still collides
        // on the canonical idempotency key, even though the (message_id, NULL, ordinal)
        // tuple differs.
        base.ordinal = 2;
        let b = EncryptedProviderContextRecord::encrypt(
            &base,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-1",
            "pc-b",
            provider_context_idempotency_key("message-1", &base),
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt compaction b");
        let error = b
            .insert_committed(&store)
            .await
            .expect_err("same canonical compaction idempotency key must collide");
        let message = format!("{error:#}");
        assert!(
            message.contains("idempotency_key") || message.contains("UNIQUE"),
            "{message}"
        );

        // A different fingerprint produces a different canonical key and succeeds.
        base.ordinal = 2;
        base.payload = ProviderContextPayload::OpenAiCompactedWindow {
            items: vec![json!({
                "type": "compaction",
                "id": "cmp-c",
                "encrypted_content": "opaque-c",
            })],
            coverage: NativeCompactionCoverage {
                through_message_seq: 7,
                context_fingerprint: "fp-b".to_owned(),
            },
        };
        // keep the plaintext origin in sync with the payload kind
        base.provider_origin = openai_responses_origin();
        let c = EncryptedProviderContextRecord::encrypt(
            &base,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-1",
            "pc-c",
            provider_context_idempotency_key("message-1", &base),
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt compaction c");
        c.insert_committed(&store)
            .await
            .expect("different fingerprint must not collide");
    }

    #[tokio::test]
    async fn invalidation_crypto_erases_data_key_when_unreferenced() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();
        let record = reasoning_record(&store, "message-1", 7, "pc-1").await;
        let key_ref = record.key_ref.clone();
        record.insert_committed(&store).await.unwrap();

        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let applier = ProviderContextMutationApplier::new(&store);
        let prepared = ProviderContextMutationBuilder::new(
            mutation_key,
            store.scope().clone(),
            "mutation-1".to_owned(),
        )
        .build_invalidate(None, vec!["pc-1".to_owned()])
        .expect("build invalidate");

        applier.prepare(&prepared).await.unwrap();
        applier.apply("mutation-1").await.unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
            .bind("pc-1")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "provider_context row must be deleted");

        let state = data_key_state(&store, &key_ref).await.expect("key exists");
        assert_eq!(
            state, "destroyed",
            "unreferenced provider-context key must be crypto-erased"
        );
    }

    #[tokio::test]
    async fn replacement_preserves_data_key_when_same_anchor_is_reinserted() {
        let store = store().await;
        seed_message_in_open_l0_batch(&store, "message-1", 7, 1_000_000)
            .await
            .unwrap();
        let old_record = reasoning_record(&store, "message-1", 7, "pc-1").await;
        let key_ref = old_record.key_ref.clone();
        old_record.insert_committed(&store).await.unwrap();

        let new_record = reasoning_record_with(&store, "message-1", 7, "pc-2", 0, 1).await;

        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let applier = ProviderContextMutationApplier::new(&store);
        let prepared = ProviderContextMutationBuilder::new(
            mutation_key,
            store.scope().clone(),
            "mutation-1".to_owned(),
        )
        .build_replace(
            None,
            vec!["pc-1".to_owned()],
            &new_record,
            &reasoning_item_with("message-1", 7, 0, 1),
            1,
            1,
        )
        .expect("build replace");

        applier.prepare(&prepared).await.unwrap();
        applier.apply("mutation-1").await.unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
            .bind("pc-1")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "old provider_context row must be deleted");

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
            .bind("pc-2")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(count, 1, "new provider_context row must be inserted");

        let state = data_key_state(&store, &key_ref).await.expect("key exists");
        assert_eq!(
            state, "active",
            "shared anchor key must stay active while replacement row references it"
        );
    }

    #[tokio::test]
    async fn shared_data_key_survives_until_last_reference_is_invalidated() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let a = reasoning_record_with(&store, "message-1", 7, "pc-a", 0, 1).await;
        let key_ref = a.key_ref.clone();
        a.insert_committed(&store).await.unwrap();

        let b = reasoning_record_with(&store, "message-1", 7, "pc-b", 1, 2).await;
        b.insert_committed(&store).await.unwrap();

        let mutation_key_a = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key a");
        let mutation_key_b = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key b");
        let applier = ProviderContextMutationApplier::new(&store);

        let invalidate_a = ProviderContextMutationBuilder::new(
            mutation_key_a,
            store.scope().clone(),
            "mutation-a".to_owned(),
        )
        .build_invalidate(None, vec!["pc-a".to_owned()])
        .expect("build invalidate a");
        applier.prepare(&invalidate_a).await.unwrap();
        applier.apply("mutation-a").await.unwrap();

        let state = data_key_state(&store, &key_ref).await.expect("key exists");
        assert_eq!(
            state, "active",
            "shared key must stay active while pc-b references it"
        );

        let invalidate_b = ProviderContextMutationBuilder::new(
            mutation_key_b,
            store.scope().clone(),
            "mutation-b".to_owned(),
        )
        .build_invalidate(None, vec!["pc-b".to_owned()])
        .expect("build invalidate b");
        applier.prepare(&invalidate_b).await.unwrap();
        applier.apply("mutation-b").await.unwrap();

        let state = data_key_state(&store, &key_ref).await.expect("key exists");
        assert_eq!(
            state, "destroyed",
            "shared key must be destroyed after last reference"
        );
    }

    #[tokio::test]
    async fn invalidation_rejects_cross_conversation_provider_context() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();
        seed_message(&store, "message-2", 9).await.unwrap();

        let cross_record = reasoning_record(&store, "message-1", 7, "pc-cross").await;
        let cross_key_ref = cross_record.key_ref.clone();
        cross_record.insert_committed(&store).await.unwrap();

        // Tamper with the data_keys row so it appears to belong to another conversation,
        // simulating a cross-conversation row referenced by this conversation's store.
        sqlx::query(
            "UPDATE data_keys SET conversation_id = 'other-conversation' WHERE key_ref = ?",
        )
        .bind(&cross_key_ref)
        .execute(store.pool())
        .await
        .expect("tamper fixture");

        let replacement = reasoning_record(&store, "message-2", 9, "pc-replacement").await;

        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let applier = ProviderContextMutationApplier::new(&store);
        let prepared = ProviderContextMutationBuilder::new(
            mutation_key,
            store.scope().clone(),
            "mutation-1".to_owned(),
        )
        .build_replace(
            None,
            vec!["pc-cross".to_owned()],
            &replacement,
            &reasoning_item("message-2", 9),
            1,
            1,
        )
        .expect("build replace");

        applier.prepare(&prepared).await.unwrap();
        let error = applier
            .apply("mutation-1")
            .await
            .expect_err("cross-conversation id must fail closed");
        let message = format!("{error:#}");
        assert!(
            message.contains("outside the active conversation scope"),
            "{message}"
        );

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
            .bind("pc-cross")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(count, 1, "cross-conversation row must not be deleted");

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
            .bind("pc-replacement")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(
            count, 0,
            "replacement must not be inserted when invalidation fails closed"
        );
    }

    #[tokio::test]
    async fn finish_mutation_requires_exactly_one_row() {
        let store = store().await;
        let applier = ProviderContextMutationApplier::new(&store);
        let mut transaction = store.pool().begin().await.expect("begin transaction");

        let error = applier
            .finish_mutation(&mut transaction, "missing-mutation", "applied", None)
            .await
            .expect_err("finishing a missing mutation must fail CAS");
        let message = format!("{error:#}");
        assert!(
            message.contains("ProviderContextMutationFinish CAS expected one row, updated 0"),
            "{message}"
        );
        transaction.rollback().await.ok();
    }

    #[tokio::test]
    async fn hydrate_validates_provider_origin_against_plaintext() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let record = reasoning_record(&store, "message-1", 7, "pc-1").await;
        record.insert_committed(&store).await.unwrap();

        let messages = vec![ContextMessage::Persisted {
            id: "message-1".to_owned(),
            seq: 7,
            message: assistant_message(reasoning_origin()),
        }];
        let hydrated = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store
                .hydrate_provider_context(&messages, &mut transaction)
                .await
        }
        .expect("hydration should succeed with matching origin");
        assert_eq!(hydrated.len(), 1);
        assert_eq!(
            hydrated[0]
                .item
                .origin_message
                .as_ref()
                .unwrap()
                .message_seq,
            7
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_provider_context_ordinal_gap_after_row_loss() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();
        reasoning_record_with(&store, "message-1", 7, "pc-0", 0, 0)
            .await
            .insert_committed(&store)
            .await
            .unwrap();
        reasoning_record_with(&store, "message-1", 7, "pc-1", 0, 1)
            .await
            .insert_committed(&store)
            .await
            .unwrap();
        sqlx::query("DELETE FROM provider_context WHERE id = 'pc-0'")
            .execute(store.pool())
            .await
            .unwrap();

        let messages = vec![ContextMessage::Persisted {
            id: "message-1".to_owned(),
            seq: 7,
            message: assistant_message(reasoning_origin()),
        }];
        let mut transaction = store.pool().begin().await.unwrap();
        let error = store
            .hydrate_provider_context(&messages, &mut transaction)
            .await
            .expect_err("missing ordinal zero must fail canonical hydration");
        assert!(
            error
                .to_string()
                .contains("must be unique and contiguous from zero"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_stored_provider_origin_mismatch() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let record = reasoning_record(&store, "message-1", 7, "pc-1").await;
        record.insert_committed(&store).await.unwrap();

        // Tamper with the stored provider-origin metadata. The authenticated plaintext
        // still carries the real origin, so hydration must detect the mismatch.
        sqlx::query("UPDATE provider_context SET provider_instance_id = 'tampered' WHERE id = ?")
            .bind("pc-1")
            .execute(store.pool())
            .await
            .expect("tamper stored provider instance id");

        let messages = vec![ContextMessage::Persisted {
            id: "message-1".to_owned(),
            seq: 7,
            message: assistant_message(reasoning_origin()),
        }];
        let error = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store
                .hydrate_provider_context(&messages, &mut transaction)
                .await
        }
        .expect_err("hydration must reject origin mismatch");
        let message = format!("{error:#}");
        assert!(
            message
                .contains("stored provider origin does not match authenticated plaintext origin"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_uses_canonical_order_by_coverage_and_id() {
        let store = store().await;
        seed_message(&store, "message-1", 1).await.unwrap();
        seed_message(&store, "message-2", 2).await.unwrap();

        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "message-1:7".to_owned(),
            })
            .await
            .unwrap();

        let mut item = ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: openai_responses_origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({
                    "type": "compaction",
                    "id": "cmp-a",
                    "encrypted_content": "opaque-a",
                })],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 1,
                    context_fingerprint: "fp-a".to_owned(),
                },
            },
        };
        // Native row ids are canonical `{request_id}:{message_seq}:{wire_label}:{ordinal}`;
        // the request identity may contain ':' separators, so it is parsed from the
        // fixed trailing fields during hydration.
        let later = EncryptedProviderContextRecord::encrypt(
            &item,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-1",
            "request-a:1:_:0",
            provider_context_idempotency_key("request-a", &item),
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt later compaction");
        later.insert_committed(&store).await.unwrap();

        // A different model keeps this a distinct native-compaction scope so the
        // active-native-window unique index is respected while still testing sort order.
        item.ordinal = 0;
        item.provider_origin = openai_responses_origin_with_model("model-2");
        item.payload = ProviderContextPayload::OpenAiCompactedWindow {
            items: vec![json!({
                "type": "compaction",
                "id": "cmp-b",
                "encrypted_content": "opaque-b",
            })],
            coverage: NativeCompactionCoverage {
                through_message_seq: 2,
                context_fingerprint: "fp-b".to_owned(),
            },
        };
        let earlier_reasoning = EncryptedProviderContextRecord::encrypt(
            &item,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-2",
            "request-b:1:_:0",
            provider_context_idempotency_key("request-b", &item),
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt earlier compaction");
        earlier_reasoning.insert_committed(&store).await.unwrap();

        let origin = openai_responses_origin();
        let messages = vec![
            ContextMessage::Persisted {
                id: "message-1".to_owned(),
                seq: 1,
                message: assistant_message(origin.clone()),
            },
            ContextMessage::Persisted {
                id: "message-2".to_owned(),
                seq: 2,
                message: assistant_message(origin),
            },
        ];
        let hydrated = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store
                .hydrate_provider_context(&messages, &mut transaction)
                .await
        }
        .expect("hydration should succeed");
        assert_eq!(hydrated.len(), 2);
        // Native compaction is unanchored; sort order is by coverage seq.
        let coverage_seq = |item: &ProviderContextItemWithFootprint| match &item.item.payload {
            ProviderContextPayload::OpenAiCompactedWindow { coverage, .. } => {
                coverage.through_message_seq
            }
            _ => panic!("expected OpenAI compacted window"),
        };
        assert_eq!(coverage_seq(&hydrated[0]), 1);
        assert_eq!(
            coverage_seq(&hydrated[1]),
            2,
            "higher coverage seq must sort after lower coverage seq"
        );
    }

    #[tokio::test]
    async fn hydrate_orders_native_compaction_before_anchored_reasoning_by_coverage() {
        let store = store().await;
        seed_message(&store, "message-1", 1).await.unwrap();
        seed_message(&store, "message-2", 2).await.unwrap();

        // Native compaction covering seq 1; suffix begins at seq 2.
        let compaction_key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "native:0".to_owned(),
            })
            .await
            .unwrap();

        let compaction_item = ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: openai_responses_origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({
                    "type": "compaction",
                    "id": "compaction-1",
                    "encrypted_content": "opaque",
                })],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 1,
                    context_fingerprint: "fp-1".to_owned(),
                },
            },
        };
        let compaction = EncryptedProviderContextRecord::encrypt(
            &compaction_item,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-1",
            "request-1:2:_:0",
            provider_context_idempotency_key("request-1", &compaction_item),
            dummy_footprint(),
            &compaction_key,
            store.scope(),
        )
        .expect("encrypt compaction");
        compaction.insert_committed(&store).await.unwrap();

        // Anchored reasoning at seq 2.
        let reasoning = reasoning_record_with(&store, "message-2", 2, "pc-reasoning", 0, 0).await;
        reasoning.insert_committed(&store).await.unwrap();

        let messages = vec![
            ContextMessage::Persisted {
                id: "message-1".to_owned(),
                seq: 1,
                message: assistant_message(reasoning_origin()),
            },
            ContextMessage::Persisted {
                id: "message-2".to_owned(),
                seq: 2,
                message: assistant_message(reasoning_origin()),
            },
        ];

        let hydrated = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store
                .hydrate_provider_context(&messages, &mut transaction)
                .await
        }
        .expect("hydration should succeed");

        assert_eq!(hydrated.len(), 2);
        assert!(
            matches!(
                &hydrated[0].item.payload,
                ProviderContextPayload::OpenAiCompactedWindow { .. }
            ),
            "native compaction with lower coverage seq must sort before anchored reasoning"
        );
        assert!(
            matches!(
                &hydrated[1].item.payload,
                ProviderContextPayload::EncryptedReasoning { .. }
            ),
            "anchored reasoning at higher message seq must sort after native compaction"
        );
    }

    #[tokio::test]
    async fn invalidate_rejects_uncommitted_deletion_of_all_targets() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let record = reasoning_record(&store, "message-1", 7, "pc-1").await;
        record.insert_committed(&store).await.unwrap();

        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let applier = ProviderContextMutationApplier::new(&store);
        let intent = ProviderContextMutationBuilder::new(
            mutation_key,
            store.scope().clone(),
            "invalidate-all-gone".to_owned(),
        )
        .build_invalidate(None, vec!["pc-1".to_owned()])
        .expect("build invalidate intent");

        applier.prepare(&intent).await.unwrap();

        // Removing an authenticated target outside the mutation transaction is
        // corruption, not an idempotent replay witness.
        sqlx::query("DELETE FROM provider_context WHERE id = ?")
            .bind("pc-1")
            .execute(store.pool())
            .await
            .unwrap();
        let error = applier
            .apply("invalidate-all-gone")
            .await
            .expect_err("uncommitted target deletion must fail closed");
        assert!(
            format!("{error:#}").contains("authenticated commitment"),
            "{error:#}"
        );

        let remaining: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
                .bind("pc-1")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(remaining, 0);

        let state: String = sqlx::query_scalar(
            "SELECT state FROM provider_context_mutations WHERE mutation_id = ?",
        )
        .bind("invalidate-all-gone")
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(
            state, "prepared",
            "failed apply must not terminalize intent"
        );
    }

    #[tokio::test]
    async fn invalidate_rejects_uncommitted_deletion_of_some_targets() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();
        seed_message(&store, "message-2", 9).await.unwrap();

        let record1 = reasoning_record_with(&store, "message-1", 7, "pc-1", 0, 1).await;
        let record2 = reasoning_record_with(&store, "message-2", 9, "pc-2", 0, 1).await;
        record1.insert_committed(&store).await.unwrap();
        record2.insert_committed(&store).await.unwrap();

        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let applier = ProviderContextMutationApplier::new(&store);
        let intent = ProviderContextMutationBuilder::new(
            mutation_key,
            store.scope().clone(),
            "invalidate-partial".to_owned(),
        )
        .build_invalidate(None, vec!["pc-1".to_owned(), "pc-2".to_owned()])
        .expect("build invalidate intent");

        applier.prepare(&intent).await.unwrap();

        sqlx::query("DELETE FROM provider_context WHERE id = ?")
            .bind("pc-1")
            .execute(store.pool())
            .await
            .unwrap();
        let error = applier
            .apply("invalidate-partial")
            .await
            .expect_err("partial uncommitted deletion must fail closed");
        assert!(
            format!("{error:#}").contains("authenticated commitment"),
            "{error:#}"
        );

        let remaining: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM provider_context WHERE id IN ('pc-1', 'pc-2')",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(
            remaining, 1,
            "failed apply must preserve the still-authenticated target"
        );

        let state: String = sqlx::query_scalar(
            "SELECT state FROM provider_context_mutations WHERE mutation_id = ?",
        )
        .bind("invalidate-partial")
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(
            state, "prepared",
            "failed apply must not terminalize intent"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_native_compaction_with_origin_message() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "native:0".to_owned(),
            })
            .await
            .unwrap();

        let item = ProviderContextItem {
            origin_message: Some(ProviderContextAnchor {
                message_id: "message-1".to_owned(),
                message_seq: 7,
            }),
            wire_item_index: None,
            ordinal: 1,
            provider_origin: openai_responses_origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({
                    "type": "compaction",
                    "id": "cmp-origin",
                    "encrypted_content": "opaque-origin",
                })],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 7,
                    context_fingerprint: "fp-7".to_owned(),
                },
            },
        };
        let record = EncryptedProviderContextRecord::encrypt(
            &item,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-1",
            "pc-tamper-origin",
            provider_context_idempotency_key("request-1", &item),
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt compaction with origin");

        record.insert_committed(&store).await.unwrap();

        let error = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store.hydrate_provider_context(&[], &mut transaction).await
        }
        .expect_err("hydration must reject native compaction with an origin message");
        let message = format!("{error:#}");
        assert!(
            message.contains("native compaction must not have an origin message"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_native_compaction_with_wire_item_index() {
        let store = store().await;

        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "native:0".to_owned(),
            })
            .await
            .unwrap();

        let item = ProviderContextItem {
            origin_message: None,
            wire_item_index: Some(0),
            ordinal: 1,
            provider_origin: openai_responses_origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({
                    "type": "compaction",
                    "id": "cmp-wire",
                    "encrypted_content": "opaque-wire",
                })],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 7,
                    context_fingerprint: "fp-7".to_owned(),
                },
            },
        };
        let record = EncryptedProviderContextRecord::encrypt(
            &item,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-1",
            "pc-tamper-wire",
            provider_context_idempotency_key("request-1", &item),
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt compaction with wire item index");

        record.insert_committed(&store).await.unwrap();

        let error = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store.hydrate_provider_context(&[], &mut transaction).await
        }
        .expect_err("hydration must reject native compaction with a wire_item_index");
        let message = format!("{error:#}");
        assert!(
            message.contains("native compaction must not have a wire_item_index"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_encrypted_reasoning_without_wire_item_index() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "message-1:7".to_owned(),
            })
            .await
            .unwrap();

        let mut item = reasoning_item("message-1", 7);
        item.wire_item_index = None;
        let origin = reasoning_origin();
        let record = EncryptedProviderContextRecord::encrypt(
            &item,
            &origin.provider_instance_id,
            origin.protocol,
            &origin.model,
            "pc-tamper-reasoning-wire",
            provider_context_idempotency_key("message-1", &item),
            reasoning_footprint(&item),
            &key,
            store.scope(),
        )
        .expect("encrypt reasoning without wire item index");

        record.insert_committed(&store).await.unwrap();

        let messages = vec![ContextMessage::Persisted {
            id: "message-1".to_owned(),
            seq: 7,
            message: assistant_message(reasoning_origin()),
        }];
        let error = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store
                .hydrate_provider_context(&messages, &mut transaction)
                .await
        }
        .expect_err("hydration must reject encrypted reasoning without a wire_item_index");
        let message = format!("{error:#}");
        assert!(
            message.contains("encrypted reasoning must have a wire_item_index"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_encrypted_reasoning_without_origin_message() {
        let store = store().await;

        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "tamper:0".to_owned(),
            })
            .await
            .unwrap();

        let mut item = reasoning_item("message-1", 7);
        item.origin_message = None;
        let origin = reasoning_origin();
        let record = EncryptedProviderContextRecord::encrypt(
            &item,
            &origin.provider_instance_id,
            origin.protocol,
            &origin.model,
            "pc-tamper-reasoning-origin",
            provider_context_idempotency_key("message-1", &item),
            reasoning_footprint(&item),
            &key,
            store.scope(),
        )
        .expect("encrypt reasoning without origin");

        // Direct insert with message_id/message_seq NULL and eviction_tokens=0 to satisfy the
        // schema CHECK while preserving the plaintext tamper.
        sqlx::query(
            "INSERT INTO provider_context(
                id, message_id, message_seq, wire_item_index, item_ordinal,
                idempotency_key, provider_instance_id, protocol, model, kind,
                coverage_through_seq, context_fingerprint, key_ref, ciphertext,
                eviction_tokens, eviction_estimator_version, created_at
             ) VALUES(?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, 0, 1, 'now')",
        )
        .bind(record.id())
        .bind(0i64)
        .bind(0i64)
        .bind(record.idempotency_key())
        .bind(record.provider_instance_id())
        .bind(record.protocol().as_str())
        .bind(record.model())
        .bind(record.kind().as_str())
        .bind(record.key_ref())
        .bind(record.ciphertext())
        .execute(store.pool())
        .await
        .unwrap();

        let messages = vec![ContextMessage::Persisted {
            id: "message-1".to_owned(),
            seq: 7,
            message: assistant_message(reasoning_origin()),
        }];
        let error = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store
                .hydrate_provider_context(&messages, &mut transaction)
                .await
        }
        .expect_err("hydration must reject encrypted reasoning without an origin message");
        let message = format!("{error:#}");
        assert!(
            message.contains("encrypted reasoning must have an origin message"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_eviction_token_mismatch() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let record = reasoning_record(&store, "message-1", 7, "pc-1").await;
        record.insert_committed(&store).await.unwrap();

        sqlx::query("UPDATE provider_context SET eviction_tokens = ? WHERE id = ?")
            .bind(999i64)
            .bind("pc-1")
            .execute(store.pool())
            .await
            .unwrap();

        let messages = vec![ContextMessage::Persisted {
            id: "message-1".to_owned(),
            seq: 7,
            message: assistant_message(reasoning_origin()),
        }];
        let error = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store
                .hydrate_provider_context(&messages, &mut transaction)
                .await
        }
        .expect_err("hydration must reject mismatched eviction tokens");
        let message = format!("{error:#}");
        assert!(
            message.contains("eviction_tokens do not match the decrypted payload"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_accepts_legacy_v1_anthropic_reasoning_with_canonical_ordering() {
        let store = store().await;
        seed_message(&store, "message-legacy", 7).await.unwrap();

        let origin = ProviderOrigin {
            provider_instance_id: "legacy-provider-instance".to_owned(),
            protocol: ApiProtocol::AnthropicMessages,
            model: "anthropic".to_owned(),
        };
        let item = ProviderContextItem {
            origin_message: Some(ProviderContextAnchor {
                message_id: "message-legacy".to_owned(),
                message_seq: 7,
            }),
            wire_item_index: Some(0),
            ordinal: 0,
            provider_origin: origin.clone(),
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::AnthropicMessages,
                item: json!({
                    "type": "thinking_signature",
                    "signature": "quote:\" backslash:\\ newline:\n 日本語 YWJjZA==",
                }),
            },
        };
        let footprint = legacy_serialized_bytes_eviction_footprint(&item.payload)
            .expect("current-main V1 footprint");
        assert_eq!(footprint.eviction_tokens(), 24);
        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "message-legacy:7".to_owned(),
            })
            .await
            .expect("provider-context key");
        EncryptedProviderContextRecord::encrypt(
            &item,
            &origin.provider_instance_id,
            origin.protocol,
            &origin.model,
            "pc-legacy-v1",
            provider_context_idempotency_key("message-legacy", &item),
            footprint,
            &key,
            store.scope(),
        )
        .expect("encrypt legacy record")
        .insert_committed(&store)
        .await
        .expect("insert legacy record");

        let messages = vec![ContextMessage::Persisted {
            id: "message-legacy".to_owned(),
            seq: 7,
            message: assistant_message(origin),
        }];
        let hydrated = {
            let mut transaction = store.pool().begin().await.expect("begin hydration");
            store
                .hydrate_provider_context(&messages, &mut transaction)
                .await
                .expect("legacy V1 record remains hydratable")
        };
        assert_eq!(hydrated.len(), 1);
        assert_eq!(hydrated[0].item, item);
    }

    #[tokio::test]
    async fn hydrate_rejects_unsupported_eviction_estimator_version() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let record = reasoning_record(&store, "message-1", 7, "pc-1").await;
        record.insert_committed(&store).await.unwrap();

        sqlx::query("UPDATE provider_context SET eviction_estimator_version = ? WHERE id = ?")
            .bind(99i64)
            .bind("pc-1")
            .execute(store.pool())
            .await
            .unwrap();

        let messages = vec![ContextMessage::Persisted {
            id: "message-1".to_owned(),
            seq: 7,
            message: assistant_message(reasoning_origin()),
        }];
        let error = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store
                .hydrate_provider_context(&messages, &mut transaction)
                .await
        }
        .expect_err("hydration must reject unsupported eviction estimator version");
        let message = format!("{error:#}");
        assert!(
            message.contains("uses unsupported eviction estimator version"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_reasoning_with_mismatched_assistant_origin() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let record = reasoning_record(&store, "message-1", 7, "pc-1").await;
        record.insert_committed(&store).await.unwrap();

        // The reasoning record was bound to reasoning_origin(); supply an assistant
        // message whose origin differs so P1-1 authentication is violated.
        let mut wrong_origin = reasoning_origin();
        wrong_origin.model = "different-model".to_owned();
        let messages = vec![ContextMessage::Persisted {
            id: "message-1".to_owned(),
            seq: 7,
            message: assistant_message(wrong_origin),
        }];

        let error = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store
                .hydrate_provider_context(&messages, &mut transaction)
                .await
        }
        .expect_err("hydration must reject reasoning with mismatched assistant origin");
        let message = format!("{error:#}");
        assert!(
            message.contains("provider_origin does not match the anchored assistant origin"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_anchor_error_uses_provider_context_record_id() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let record = reasoning_record(&store, "message-1", 7, "pc-1").await;
        record.insert_committed(&store).await.unwrap();

        // The persisted message at seq 7 has a different id than the anchor claims.
        // Before the id-shadow fix the outer provider-context record id would be
        // overwritten by the inner message id in the match arm, causing the error
        // to name the wrong record.
        let messages = vec![ContextMessage::Persisted {
            id: "message-2".to_owned(),
            seq: 7,
            message: assistant_message(reasoning_origin()),
        }];
        let error = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store
                .hydrate_provider_context(&messages, &mut transaction)
                .await
        }
        .expect_err("hydration must reject an anchor that resolves to a different message id");
        let message = format!("{error:#}");
        assert!(
            message.contains("pc-1"),
            "error should name the provider-context record id: {message}"
        );
        assert!(
            !message.contains("message-2"),
            "error must not leak the mismatched persisted message id as the record id: {message}"
        );
        assert!(
            message.contains("resolves to a different message id"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn build_replace_zeroizes_plaintext_on_empty_mutation_id() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let record = reasoning_record(&store, "message-1", 7, "pc-1").await;
        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let plaintext = reasoning_item("message-1", 7);

        // plaintext_bytes is serialized and then build_full fails on the empty
        // mutation_id.  Wrapping it in Zeroizing from creation ensures the buffer
        // is cleared even on this error path.
        let error = ProviderContextMutationBuilder::new(mutation_key, store.scope().clone(), "")
            .build_replace(None, vec!["pc-0".to_owned()], &record, &plaintext, 0, 0)
            .expect_err("empty mutation_id must fail");
        let message = format!("{error:#}");
        assert!(
            message.contains("mutation_id must not be empty"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn active_native_window_is_unique_per_origin_scope() {
        let store = store().await;
        seed_message(&store, "message-1", 1).await.unwrap();

        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "native:0".to_owned(),
            })
            .await
            .unwrap();

        let item = ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 1,
            provider_origin: openai_responses_origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({
                    "type": "compaction",
                    "id": "cmp-first",
                    "encrypted_content": "opaque-first",
                })],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 1,
                    context_fingerprint: "fp-1".to_owned(),
                },
            },
        };
        let first = EncryptedProviderContextRecord::encrypt(
            &item,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-1",
            "pc-first",
            provider_context_idempotency_key("request-1", &item),
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt first window");
        first.insert_committed(&store).await.unwrap();

        let mut second_item = item.clone();
        second_item.payload = ProviderContextPayload::OpenAiCompactedWindow {
            items: vec![json!({
                "type": "compaction",
                "id": "cmp-second",
                "encrypted_content": "opaque-second",
            })],
            coverage: NativeCompactionCoverage {
                through_message_seq: 1,
                context_fingerprint: "fp-2".to_owned(),
            },
        };
        let second = EncryptedProviderContextRecord::encrypt(
            &second_item,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-1",
            "pc-second",
            provider_context_idempotency_key("request-2", &second_item),
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt second window");
        let error = second
            .insert_committed(&store)
            .await
            .expect_err("second active native window for the same origin scope must fail");
        let message = format!("{error:#}");
        assert!(
            message.contains("UNIQUE")
                || message.contains("idx_provider_context_active_native_window"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn replace_head_survives_row_deletion_and_supersedes_older_prepared_replace() {
        let store = store().await;
        seed_message_in_open_l0_batch(&store, "message-1", 7, 1_000_000)
            .await
            .unwrap();
        seed_message_in_open_l0_batch(&store, "message-2", 8, 1_000_000)
            .await
            .unwrap();

        let applier = ProviderContextMutationApplier::new(&store);
        let scope = store.scope().clone();

        // Prepare A first, then apply newer B. Recovery must not resurrect A
        // after B's active row is later invalidated.
        let a = reasoning_record(&store, "message-1", 7, "pc-a").await;
        let intent_a = ProviderContextMutationBuilder::new(
            store
                .conversation_key(DataKeyPurpose::Mutation)
                .await
                .expect("mint mutation key"),
            scope.clone(),
            "replace-a".to_owned(),
        )
        .build_replace(None, vec![], &a, &reasoning_item("message-1", 7), 1, 10)
        .expect("build replace-a");
        applier.prepare(&intent_a).await.unwrap();

        let b = reasoning_record(&store, "message-2", 8, "pc-b").await;
        let intent_b = ProviderContextMutationBuilder::new(
            store
                .conversation_key(DataKeyPurpose::Mutation)
                .await
                .expect("mint mutation key"),
            scope.clone(),
            "replace-b".to_owned(),
        )
        .build_replace(None, vec![], &b, &reasoning_item("message-2", 8), 1, 11)
        .expect("build replace-b");
        applier.prepare(&intent_b).await.unwrap();
        assert_eq!(
            applier.apply("replace-b").await.unwrap(),
            ApplyOutcome::Applied
        );

        let invalidate = ProviderContextMutationBuilder::new(
            store
                .conversation_key(DataKeyPurpose::Mutation)
                .await
                .expect("mint mutation key"),
            scope,
            "invalidate-b".to_owned(),
        )
        .build_invalidate(None, vec!["pc-b".to_owned()])
        .expect("build invalidation");
        applier.prepare(&invalidate).await.unwrap();
        assert_eq!(
            applier.apply("invalidate-b").await.unwrap(),
            ApplyOutcome::Applied
        );

        let head: i64 =
            sqlx::query_scalar("SELECT max_window_ordinal FROM provider_context_replace_heads")
                .fetch_one(store.pool())
                .await
                .expect("head survives row deletion");
        assert_eq!(head, 11);

        applier.recover().await.expect("recover prepared A");
        let terminal_reason: Option<String> = sqlx::query_scalar(
            "SELECT terminal_reason FROM provider_context_mutations WHERE mutation_id = 'replace-a'",
        )
        .fetch_one(store.pool())
        .await
        .expect("read recovered A outcome");
        assert_eq!(terminal_reason.as_deref(), Some("newer_replace"));
        let rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = 'pc-a'")
                .fetch_one(store.pool())
                .await
                .expect("count resurrected A rows");
        assert_eq!(rows, 0, "older A must not be reinserted after B is deleted");
    }

    #[tokio::test]
    async fn replace_requires_expected_latest_id_in_invalidate_ids() {
        let store = store().await;
        seed_message_in_open_l0_batch(&store, "message-1", 7, 1_000_000)
            .await
            .unwrap();

        let applier = ProviderContextMutationApplier::new(&store);
        let scope = store.scope().clone();

        let a = reasoning_record(&store, "message-1", 7, "pc-a").await;
        let intent_a = ProviderContextMutationBuilder::new(
            store
                .conversation_key(DataKeyPurpose::Mutation)
                .await
                .expect("mint mutation key"),
            scope.clone(),
            "replace-a".to_owned(),
        )
        .build_replace(None, vec![], &a, &reasoning_item("message-1", 7), 1, 1)
        .expect("build replace-a");
        applier.prepare(&intent_a).await.unwrap();
        assert_eq!(
            applier.apply("replace-a").await.unwrap(),
            ApplyOutcome::Applied
        );

        let b = reasoning_record(&store, "message-1", 7, "pc-b").await;
        let intent_b = ProviderContextMutationBuilder::new(
            store
                .conversation_key(DataKeyPurpose::Mutation)
                .await
                .expect("mint mutation key"),
            scope,
            "replace-b".to_owned(),
        )
        .build_replace(
            Some("pc-a".to_owned()),
            vec![], // missing the expected witness in invalidate_ids
            &b,
            &reasoning_item("message-1", 7),
            2,
            2,
        )
        .expect("build replace-b");
        applier.prepare(&intent_b).await.unwrap();
        let error = applier
            .apply("replace-b")
            .await
            .expect_err("replace with expected_latest_id missing from invalidate_ids must fail");
        let message = format!("{error:#}");
        assert!(
            message.contains("expected_latest_id must be included in invalidate_ids"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_native_compaction_accepts_real_global_event_gaps_for_both_protocols() {
        for anthropic in [false, true] {
            let store = store().await;
            for seq in [1, 2, 3, 5] {
                seed_non_message_event(&store, seq).await.unwrap();
            }
            seed_message(&store, "message-4", 4).await.unwrap();
            seed_message(&store, "message-6", 6).await.unwrap();

            let item = native_compaction_item(anthropic, 4);
            insert_native_compaction(&store, "request:with:colons", &item).await;
            let messages = vec![
                ContextMessage::Persisted {
                    id: "message-4".to_owned(),
                    seq: 4,
                    message: assistant_message(item.provider_origin.clone()),
                },
                ContextMessage::Persisted {
                    id: "message-6".to_owned(),
                    seq: 6,
                    message: assistant_message(item.provider_origin.clone()),
                },
            ];

            let hydrated = {
                let mut transaction = store.pool().begin().await.expect("begin test transaction");
                store
                    .hydrate_provider_context(&messages, &mut transaction)
                    .await
            }
            .expect("global event gaps must not invalidate native compaction hydration");
            assert_eq!(
                hydrated.iter().map(|h| h.item.clone()).collect::<Vec<_>>(),
                vec![item]
            );
        }
    }

    #[tokio::test]
    async fn hydrate_native_compaction_rejects_tampered_idempotency_for_both_protocols() {
        for anthropic in [false, true] {
            let store = store().await;
            seed_message(&store, "message-4", 4).await.unwrap();
            seed_message(&store, "message-6", 6).await.unwrap();
            let item = native_compaction_item(anthropic, 4);
            let id = insert_native_compaction(&store, "request:with:colons", &item).await;
            sqlx::query("UPDATE provider_context SET idempotency_key = 'tampered' WHERE id = ?")
                .bind(&id)
                .execute(store.pool())
                .await
                .expect("tamper stored idempotency key");

            let messages = vec![
                ContextMessage::Persisted {
                    id: "message-4".to_owned(),
                    seq: 4,
                    message: assistant_message(item.provider_origin.clone()),
                },
                ContextMessage::Persisted {
                    id: "message-6".to_owned(),
                    seq: 6,
                    message: assistant_message(item.provider_origin.clone()),
                },
            ];
            let error = {
                let mut transaction = store.pool().begin().await.expect("begin test transaction");
                store
                    .hydrate_provider_context(&messages, &mut transaction)
                    .await
            }
            .expect_err("tampered native idempotency key must fail hydration");
            assert!(
                format!("{error:#}")
                    .contains("idempotency key does not match authenticated native item"),
                "{error:#}"
            );
        }
    }

    #[tokio::test]
    async fn hydrate_native_compaction_rejects_reordered_messages() {
        let store = store().await;
        seed_message(&store, "message-4", 4).await.unwrap();
        seed_message(&store, "message-6", 6).await.unwrap();
        let item = native_compaction_item(false, 4);
        insert_native_compaction(&store, "request-1", &item).await;
        let messages = vec![
            ContextMessage::Persisted {
                id: "message-6".to_owned(),
                seq: 6,
                message: assistant_message(item.provider_origin.clone()),
            },
            ContextMessage::Persisted {
                id: "message-4".to_owned(),
                seq: 4,
                message: assistant_message(item.provider_origin.clone()),
            },
        ];
        let error = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store
                .hydrate_provider_context(&messages, &mut transaction)
                .await
        }
        .expect_err("reordered persisted messages must fail hydration");
        assert!(format!("{error:#}").contains("reordered"), "{error:#}");
    }

    #[tokio::test]
    async fn hydrate_rejects_native_compaction_coverage_out_of_range() {
        let store = store().await;
        seed_message(&store, "message-1", 1).await.unwrap();
        seed_message(&store, "message-3", 3).await.unwrap();

        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "native:0".to_owned(),
            })
            .await
            .unwrap();

        // Persisted messages at seq 1 and 3 (gaps are legal), but coverage claims seq 5.
        let item = ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 1,
            provider_origin: openai_responses_origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({
                    "type": "compaction",
                    "id": "cmp-out-of-range",
                    "encrypted_content": "opaque",
                })],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 5,
                    context_fingerprint: "fp-1".to_owned(),
                },
            },
        };
        let compaction = EncryptedProviderContextRecord::encrypt(
            &item,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-1",
            "request-1:1:_:1",
            provider_context_idempotency_key("request-1", &item),
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt compaction");
        compaction.insert_committed(&store).await.unwrap();

        let origin = openai_responses_origin();
        let messages = vec![
            ContextMessage::Persisted {
                id: "message-1".to_owned(),
                seq: 1,
                message: assistant_message(origin.clone()),
            },
            ContextMessage::Persisted {
                id: "message-3".to_owned(),
                seq: 3,
                message: assistant_message(origin),
            },
        ];

        let error = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store
                .hydrate_provider_context(&messages, &mut transaction)
                .await
        }
        .expect_err("hydration must reject out-of-range native compaction coverage");
        let message = format!("{error:#}");
        assert!(
            message.contains("coverage does not identify a persisted message"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_provider_context_gap_duplicate_and_tamper() {
        // Gap: a native compaction claims coverage through a missing message seq.
        let store1 = store().await;
        seed_message(&store1, "message-1", 1).await.unwrap();
        seed_message(&store1, "message-3", 3).await.unwrap();
        let gap_item = native_compaction_item(false, 2);
        insert_native_compaction(&store1, "gap", &gap_item).await;
        let origin = reasoning_origin();
        let messages = vec![
            ContextMessage::Persisted {
                id: "message-1".to_owned(),
                seq: 1,
                message: assistant_message(origin.clone()),
            },
            ContextMessage::Persisted {
                id: "message-3".to_owned(),
                seq: 3,
                message: assistant_message(origin),
            },
        ];
        let error = {
            let mut tx = store1.pool().begin().await.expect("begin test transaction");
            store1.hydrate_provider_context(&messages, &mut tx).await
        };
        assert!(
            error.is_err(),
            "gap in native compaction coverage must fail closed"
        );
        assert!(
            error.unwrap_err().to_string().contains("coverage"),
            "error must describe coverage gap"
        );

        // Duplicate: the idempotency key must be unique across provider-context records.
        let store2 = store().await;
        seed_message(&store2, "message-1", 1).await.unwrap();
        let first = reasoning_record(&store2, "message-1", 1, "dup-1").await;
        first.insert_committed(&store2).await.unwrap();
        let second = reasoning_record(&store2, "message-1", 1, "dup-2").await;
        assert!(
            second.insert_committed(&store2).await.is_err(),
            "duplicate idempotency key must fail closed"
        );

        // Tamper: changing the stored kind after insert must be caught on hydration.
        sqlx::query("UPDATE provider_context SET kind = 'open_ai_compacted_window' WHERE id = ?")
            .bind("dup-1")
            .execute(store2.pool())
            .await
            .expect("tamper stored provider-context kind");
        let messages = vec![ContextMessage::Persisted {
            id: "message-1".to_owned(),
            seq: 1,
            message: assistant_message(reasoning_origin()),
        }];
        let error = {
            let mut tx = store2.pool().begin().await.expect("begin test transaction");
            store2.hydrate_provider_context(&messages, &mut tx).await
        };
        assert!(
            error.is_err(),
            "tampered provider-context kind must fail closed"
        );
        assert!(
            error.unwrap_err().to_string().contains("kind"),
            "error must describe kind mismatch"
        );
    }
    async fn invalidate_zeroes_ciphertext_before_delete_and_preserves_shared_key() {
        let store = store().await;
        let batch_id = seed_message_in_open_l0_batch(&store, "message-1", 7, 1_000_000)
            .await
            .unwrap();

        // Two reasoning records for the same anchor share a data key.
        let a = reasoning_record_with(&store, "message-1", 7, "pc-a", 0, 0).await;
        let b = reasoning_record_with(&store, "message-1", 7, "pc-b", 0, 1).await;
        a.insert_committed(&store).await.unwrap();
        b.insert_committed(&store).await.unwrap();

        // Replace invalidates pc-a and inserts pc-c. All three share the anchor key.
        let c = reasoning_record_with(&store, "message-1", 7, "pc-c", 0, 2).await;
        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let applier = ProviderContextMutationApplier::new(&store);
        let prepared = ProviderContextMutationBuilder::new(
            mutation_key,
            store.scope().clone(),
            "mutation-1".to_owned(),
        )
        .build_replace(
            None,
            vec!["pc-a".to_owned()],
            &c,
            &reasoning_item_with("message-1", 7, 0, 2),
            1,
            1,
        )
        .expect("build replace");
        applier.prepare(&prepared).await.unwrap();
        // A prepared Replace can be applied after its L0 batch has compacted;
        // accounting must still use the extant membership.
        sqlx::query("UPDATE memory_batches SET state = 'compacted' WHERE id = ?")
            .bind(&batch_id)
            .execute(store.pool())
            .await
            .unwrap();
        let mut transaction = store.pool().begin().await.unwrap();
        assert_eq!(
            applier
                .apply_in_transaction(&mut transaction, "mutation-1")
                .await
                .unwrap(),
            ApplyOutcome::Applied
        );
        transaction.commit().await.unwrap();

        // The invalidated row is gone.
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
            .bind("pc-a")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "invalidated provider-context row must be deleted");
        assert_eq!(
            applier.zero_before_delete_checks(),
            1,
            "the zero-before-delete test seam must observe exactly one invalidation"
        );

        // The remaining rows are intact.
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id IN (?, ?)")
                .bind("pc-b")
                .bind("pc-c")
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(count, 2, "remaining rows must survive");

        // The shared anchor key stays active because pc-b and pc-c still use it.
        let state = data_key_state(&store, &a.key_ref)
            .await
            .expect("key exists");
        assert_eq!(state, "active", "shared anchor key must stay active");

        // The internal test seam in `invalidate_ids` asserted the row's
        // ciphertext was overwritten with zeros before the DELETE executed.
    }

    #[tokio::test]
    async fn footprint_increment_rejects_terminal_l0_membership() {
        let store = store().await;
        let batch_id = seed_message_in_open_l0_batch(&store, "message-1", 7, 41)
            .await
            .unwrap();
        sqlx::query("UPDATE memory_batches SET state = 'dropped' WHERE id = ?")
            .bind(&batch_id)
            .execute(store.pool())
            .await
            .unwrap();

        let applier = ProviderContextMutationApplier::new(&store);
        let mut transaction = store.pool().begin().await.unwrap();
        let error = applier
            .increment_batch_footprint(&mut transaction, Some("message-1"), 1)
            .await
            .expect_err("terminal L0 membership must fail closed");
        assert!(
            format!("{error:#}")
                .contains(&format!("terminal L0 batch {batch_id} in state dropped")),
            "{error:#}"
        );
        transaction.rollback().await.unwrap();

        let footprint: i64 =
            sqlx::query_scalar("SELECT eviction_footprint_tokens FROM memory_batches WHERE id = ?")
                .bind(&batch_id)
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(footprint, 41);
    }

    #[tokio::test]
    async fn replace_accounts_eviction_footprint_and_is_idempotent() {
        let store = store().await;

        // Build two reasoning items with measurably different footprints by
        // varying the opaque encrypted content length.
        let old_item = reasoning_item_with_content("message-1", 7, 0, 0, "short");
        let new_item = reasoning_item_with_content(
            "message-1",
            7,
            0,
            1,
            "this-is-a-much-longer-opaque-reasoning-payload",
        );
        let old_footprint = reasoning_footprint(&old_item).eviction_tokens();
        let new_footprint = reasoning_footprint(&new_item).eviction_tokens();
        assert!(
            new_footprint > old_footprint,
            "regression requires different footprints"
        );

        // Seed an open L0 batch whose footprint already includes the old record.
        seed_message_in_open_l0_batch(
            &store,
            "message-1",
            7,
            i64::try_from(old_footprint).unwrap(),
        )
        .await
        .unwrap();

        let old_record = reasoning_record_from_item(&store, "pc-old", &old_item).await;
        old_record.insert_committed(&store).await.unwrap();

        let new_record = reasoning_record_from_item(&store, "pc-new", &new_item).await;

        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let applier = ProviderContextMutationApplier::new(&store);

        let prepared = ProviderContextMutationBuilder::new(
            mutation_key,
            store.scope().clone(),
            "replace-1".to_owned(),
        )
        .build_replace(
            None,
            vec!["pc-old".to_owned()],
            &new_record,
            &new_item,
            1,
            1,
        )
        .expect("build replace-1");
        applier.prepare(&prepared).await.unwrap();
        assert_eq!(
            applier.apply("replace-1").await.unwrap(),
            ApplyOutcome::Applied
        );

        let footprint_after_apply: i64 = sqlx::query_scalar(
            "SELECT eviction_footprint_tokens
             FROM memory_batches
             WHERE layer = ? AND state = 'open'",
        )
        .bind(MemoryLayer::L0.as_i64())
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(
            footprint_after_apply,
            i64::try_from(new_footprint).unwrap(),
            "batch footprint must reflect old-subtract/new-add exactly"
        );

        // A duplicate replace intent with the same (gen, ord, id) is already
        // satisfied and must not re-add the footprint.
        let mutation_key_2 = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint second mutation key");
        let prepared_2 = ProviderContextMutationBuilder::new(
            mutation_key_2,
            store.scope().clone(),
            "replace-2".to_owned(),
        )
        .build_replace(
            Some("pc-new".to_owned()),
            vec!["pc-old".to_owned()],
            &new_record,
            &new_item,
            1,
            1,
        )
        .expect("build replace-2");
        applier.prepare(&prepared_2).await.unwrap();
        assert_eq!(
            applier.apply("replace-2").await.unwrap(),
            ApplyOutcome::AlreadySatisfied
        );

        let footprint_after_retry: i64 = sqlx::query_scalar(
            "SELECT eviction_footprint_tokens
             FROM memory_batches
             WHERE layer = ? AND state = 'open'",
        )
        .bind(MemoryLayer::L0.as_i64())
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(
            footprint_after_retry,
            i64::try_from(new_footprint).unwrap(),
            "duplicate replace must not change batch footprint"
        );
    }

    async fn reasoning_record_from_item(
        store: &Store,
        id: &str,
        item: &ProviderContextItem,
    ) -> EncryptedProviderContextRecord {
        let origin_message = item
            .origin_message
            .as_ref()
            .expect("reasoning item must have an anchor");
        let anchor = ProviderContextKeyAnchor {
            conversation_id: store.scope().conversation_id.clone(),
            anchor_id: format!(
                "{}:{}",
                origin_message.message_id, origin_message.message_seq
            ),
        };
        let key = store
            .provider_context_key(&anchor)
            .await
            .expect("mint reasoning anchor key");
        EncryptedProviderContextRecord::encrypt(
            item,
            &item.provider_origin.provider_instance_id,
            item.provider_origin.protocol,
            &item.provider_origin.model,
            id,
            provider_context_idempotency_key(&origin_message.message_id, item),
            reasoning_footprint(item),
            &key,
            store.scope(),
        )
        .expect("encrypt reasoning record")
    }
}
