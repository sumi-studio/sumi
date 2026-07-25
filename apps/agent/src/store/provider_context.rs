//! Encrypted provider-context records and durable provider-context mutations.
//!
//! Provider context (opaque reasoning / native compaction windows) is stored in
//! its own per-anchor data key, separate from the public transcript.  This
//! module owns the encryption envelope, the canonical `Replace`/`Invalidate`
//! mutation intent, the HKDF-derived HMAC binding, and the transactional
//! apply/CAS primitives used by `ProviderContextMutationRecovery`.

#![allow(dead_code)]

use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use sqlx::Row;
use zeroize::Zeroize;

use crate::provider::types::{ApiProtocol, ProviderContextItem, ProviderContextPayload};

use super::crypto::{decrypt_content, encrypt_content};
use super::{AgentScope, DataKeyMaterial, DataKeyPurpose, Store};

const INTENT_HMAC_INFO: &[u8] = b"provider-context-mutation-intent/v1";
const INTENT_HMAC_KEY_ID: &str = "mutation-intent-hmac/v1";
const PLAINTEXT_HMAC_DOMAIN: &[u8] = b"sumi-provider-context-plaintext/v1";
const INTENT_HMAC_DOMAIN: &[u8] = b"sumi-provider-context-mutation-intent/v1";
const SCOPE_KEY_DOMAIN: &[u8] = b"sumi-provider-context-scope/v1";

/// HKDF-Extract/Expand with HMAC-SHA256, keyed by the durable mutation data key
/// and conversation-scoped salt.  This key is used for both the plaintext HMAC
/// and the canonical semantic-intent HMAC.
pub(crate) fn hkdf_intent_hmac_key(data_key: &DataKeyMaterial, conversation_id: &str) -> [u8; 32] {
    let mut prk_mac = <Hmac<Sha256> as Mac>::new_from_slice(conversation_id.as_bytes())
        .expect("HMAC accepts any salt length");
    prk_mac.update(data_key.bytes());
    let prk = prk_mac.finalize().into_bytes();

    let mut t_mac =
        <Hmac<Sha256> as Mac>::new_from_slice(&prk).expect("HMAC output is a valid HMAC key");
    t_mac.update(INTENT_HMAC_INFO);
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderContextKind {
    EncryptedReasoning,
    OpenAiCompactedWindow,
    AnthropicCompaction,
}

impl ProviderContextKind {
    fn from_payload(payload: &ProviderContextPayload) -> Self {
        match payload {
            ProviderContextPayload::OpenAiCompactedWindow { .. } => Self::OpenAiCompactedWindow,
            ProviderContextPayload::AnthropicCompaction { .. } => Self::AnthropicCompaction,
            ProviderContextPayload::EncryptedReasoning { .. } => Self::EncryptedReasoning,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::EncryptedReasoning => "encrypted_reasoning",
            Self::OpenAiCompactedWindow => "open_ai_compacted_window",
            Self::AnthropicCompaction => "anthropic_compaction",
        }
    }
}

/// Versioned eviction footprint for opaque provider-context payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ProviderContextEvictionEstimate {
    pub tokens: u64,
    pub version: u32,
}

impl ProviderContextEvictionEstimate {
    pub(crate) const V1: u32 = 1;

    /// Returns the V1 estimate: opaque `EncryptedReasoning` payloads pay
    /// `ceil(serialized_bytes / 4)` re-send tokens; native compaction windows
    /// carry zero because they replace rather than append to the context.
    pub(crate) fn v1(item: &ProviderContextItem) -> Self {
        let tokens = match &item.payload {
            ProviderContextPayload::EncryptedReasoning { item, .. } => {
                let bytes = serde_json::to_vec(item).unwrap_or_default();
                (bytes.len() as u64).div_ceil(4)
            }
            _ => 0,
        };
        Self {
            tokens,
            version: Self::V1,
        }
    }
}

/// A durable `provider_context` row.  Plaintext is not retained after
/// construction; the record exposes only the encrypted ciphertext and the
/// metadata required for ordering, eviction accounting, and mutation intent.
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
        data_key: &DataKeyMaterial,
        scope: &AgentScope,
    ) -> Result<Self> {
        if data_key.purpose != DataKeyPurpose::ProviderContext {
            bail!("provider-context records require a provider_context data key");
        }

        let id = id.into();
        let aad = scope.row_aad("provider_context", &id, DataKeyPurpose::ProviderContext);
        let mut plaintext =
            serde_json::to_vec(item).context("failed to serialize provider-context plaintext")?;
        let ciphertext = encrypt_content(data_key, &plaintext, &aad)?;
        plaintext.zeroize();

        let (coverage_through_seq, context_fingerprint) = match &item.payload {
            ProviderContextPayload::OpenAiCompactedWindow { coverage, .. }
            | ProviderContextPayload::AnthropicCompaction { coverage, .. } => (
                Some(coverage.through_message_seq),
                Some(coverage.context_fingerprint.clone()),
            ),
            _ => (None, None),
        };

        let estimate = ProviderContextEvictionEstimate::v1(item);
        let message_id = item.origin_message.as_ref().map(|a| a.message_id.clone());
        let message_seq = item.origin_message.as_ref().map(|a| a.message_seq);

        Ok(Self {
            id,
            message_id,
            message_seq,
            wire_item_index: item.wire_item_index,
            item_ordinal: item.ordinal,
            idempotency_key: idempotency_key.into(),
            provider_instance_id: provider_instance_id.into(),
            protocol,
            model: model.into(),
            kind: ProviderContextKind::from_payload(&item.payload),
            coverage_through_seq,
            context_fingerprint,
            key_ref: data_key.key_ref.clone(),
            ciphertext,
            eviction_tokens: estimate.tokens,
            eviction_estimator_version: estimate.version,
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
                .map(|v| i64::try_from(v).unwrap_or(i64::MAX)),
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
                .map(|v| i64::try_from(v).unwrap_or(i64::MAX)),
        )
        .bind(self.context_fingerprint.as_ref())
        .bind(&self.key_ref)
        .bind(&self.ciphertext)
        .bind(i64::try_from(self.eviction_tokens).unwrap_or(i64::MAX))
        .bind(i64::from(self.eviction_estimator_version))
        .bind(&self.created_at)
        .execute(executor)
        .await
        .context("failed to insert provider-context record")?;
        Ok(())
    }
}

impl ApiProtocol {
    fn as_str(self) -> &'static str {
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
#[derive(Serialize, Deserialize)]
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
fn semantic_intent_bytes(full: &FullIntent) -> Vec<u8> {
    let mut writer = CanonicalWriter::with_domain(INTENT_HMAC_DOMAIN);
    writer.field(full.variant.as_bytes());
    writer.field(full.mutation_id.as_bytes());
    writer.field(full.expected_latest_id.as_deref().unwrap_or("").as_bytes());
    writer.field(&canonical_id_list(&full.invalidate_ids));
    writer.field(full.provider_context_id.as_bytes());
    writer.field(full.message_id.as_deref().unwrap_or("").as_bytes());
    writer.field(&opt_u64_bytes(full.message_seq));
    writer.field(&opt_u32_bytes(full.wire_item_index));
    writer.field(full.item_ordinal.to_string().as_bytes());
    writer.field(full.idempotency_key.as_bytes());
    writer.field(full.provider_instance_id.as_bytes());
    writer.field(full.protocol.as_bytes());
    writer.field(full.model.as_bytes());
    writer.field(full.kind.as_bytes());
    writer.field(&opt_u64_bytes(full.coverage_through_seq));
    writer.field(full.context_fingerprint.as_deref().unwrap_or("").as_bytes());
    writer.field(full.eviction_tokens.to_string().as_bytes());
    writer.field(full.eviction_estimator_version.to_string().as_bytes());
    writer.field(full.config_generation.to_string().as_bytes());
    writer.field(full.window_ordinal.to_string().as_bytes());
    writer.field(&full.plaintext_hmac);
    writer.finish()
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
    opt.map(|v| v.to_string()).unwrap_or_default().into_bytes()
}

fn opt_u32_bytes(opt: Option<u32>) -> Vec<u8> {
    opt.map(|v| v.to_string()).unwrap_or_default().into_bytes()
}

/// A prepared `Replace`/`Invalidate` intent ready to be persisted and applied.
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
        let mut plaintext_bytes = serde_json::to_vec(plaintext)
            .context("failed to serialize provider-context plaintext for intent")?;
        let intent_key = hkdf_intent_hmac_key(&self.mutation_key, &self.scope.conversation_id);
        let plaintext_hmac = hmac_sha256(&intent_key, PLAINTEXT_HMAC_DOMAIN, &plaintext_bytes);
        plaintext_bytes.zeroize();

        self.build_full(
            "replace",
            expected_latest_id,
            sorted,
            Some(insert),
            Some(plaintext),
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
        _plaintext: Option<&ProviderContextItem>,
        config_generation: u64,
        window_ordinal: u64,
        plaintext_hmac: Vec<u8>,
    ) -> Result<PreparedProviderContextMutation> {
        if self.mutation_id.is_empty() {
            bail!("provider-context mutation_id must not be empty");
        }

        let record = insert.map(|r| {
            (
                r.id.clone(),
                r.message_id.clone(),
                r.message_seq,
                r.wire_item_index,
                r.item_ordinal,
                r.idempotency_key.clone(),
                r.provider_instance_id.clone(),
                r.protocol.as_str().to_owned(),
                r.model.clone(),
                r.kind.as_str().to_owned(),
                r.coverage_through_seq,
                r.context_fingerprint.clone(),
                r.eviction_tokens,
                r.eviction_estimator_version,
                r.key_ref.clone(),
                r.ciphertext.clone(),
                r.created_at.clone(),
            )
        });

        let full = FullIntent {
            variant: variant.to_owned(),
            mutation_id: self.mutation_id.clone(),
            expected_latest_id,
            invalidate_ids,
            provider_context_id: record.as_ref().map(|r| r.0.clone()).unwrap_or_default(),
            message_id: record.as_ref().and_then(|r| r.1.clone()),
            message_seq: record.as_ref().and_then(|r| r.2),
            wire_item_index: record.as_ref().and_then(|r| r.3),
            item_ordinal: record.as_ref().map(|r| r.4).unwrap_or(1),
            idempotency_key: record.as_ref().map(|r| r.5.clone()).unwrap_or_default(),
            provider_instance_id: record.as_ref().map(|r| r.6.clone()).unwrap_or_default(),
            protocol: record.as_ref().map(|r| r.7.clone()).unwrap_or_default(),
            model: record.as_ref().map(|r| r.8.clone()).unwrap_or_default(),
            kind: record.as_ref().map(|r| r.9.clone()).unwrap_or_default(),
            coverage_through_seq: record.as_ref().and_then(|r| r.10),
            context_fingerprint: record.as_ref().and_then(|r| r.11.clone()),
            eviction_tokens: record.as_ref().map(|r| r.12).unwrap_or(0),
            eviction_estimator_version: record.as_ref().map(|r| r.13).unwrap_or(1),
            config_generation,
            window_ordinal,
            plaintext_hmac,
            key_ref: record.as_ref().map(|r| r.14.clone()).unwrap_or_default(),
            ciphertext: record.as_ref().map(|r| r.15.clone()).unwrap_or_default(),
            created_at: record.as_ref().map(|r| r.16.clone()).unwrap_or_default(),
        };

        let intent_key = hkdf_intent_hmac_key(&self.mutation_key, &self.scope.conversation_id);
        let semantic = semantic_intent_bytes(&full);
        let intent_hmac = hmac_sha256(&intent_key, INTENT_HMAC_DOMAIN, &semantic);

        let aad = self.scope.row_aad(
            "provider_context_mutations",
            &self.mutation_id,
            DataKeyPurpose::Mutation,
        );
        let mut full_json =
            serde_json::to_vec(&full).context("failed to serialize full mutation intent")?;
        let intent_ciphertext = encrypt_content(&self.mutation_key, &full_json, &aad)?;
        full_json.zeroize();

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

/// Transactional owner for `provider_context_mutations` prepare/apply.
pub(crate) struct ProviderContextMutationApplier<'a> {
    store: &'a Store,
}

impl<'a> ProviderContextMutationApplier<'a> {
    pub(crate) fn new(store: &'a Store) -> Self {
        Self { store }
    }

    pub(crate) async fn prepare(&self, prepared: &PreparedProviderContextMutation) -> Result<()> {
        if prepared.mutation_id.is_empty() {
            bail!("mutation_id must not be empty");
        }

        let existing: Option<(String, Vec<u8>)> = sqlx::query_as(
            "SELECT intent_key_ref, intent_hmac FROM provider_context_mutations WHERE mutation_id = ?",
        )
        .bind(&prepared.mutation_id)
        .fetch_optional(self.store.pool())
        .await
        .context("failed to load existing mutation row")?;

        if let Some((key_ref, hmac)) = existing {
            if key_ref == prepared.intent_key_ref && hmac == prepared.intent_hmac {
                return Ok(());
            }
            bail!("conflicting provider-context mutation intent already exists");
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
        .execute(self.store.pool())
        .await
        .context("failed to prepare provider-context mutation")?;
        Ok(())
    }

    pub(crate) async fn apply(&self, mutation_id: &str) -> Result<ApplyOutcome> {
        let mut transaction = self.store.pool().begin().await?;

        let row = sqlx::query(
            "SELECT state, intent_key_ref, intent_ciphertext, intent_hmac
             FROM provider_context_mutations WHERE mutation_id = ?",
        )
        .bind(mutation_id)
        .fetch_optional(&mut *transaction)
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
            .data_key_by_ref_in_transaction(&mut transaction, &intent_key_ref)
            .await?;
        let aad = self.store.scope().row_aad(
            "provider_context_mutations",
            mutation_id,
            DataKeyPurpose::Mutation,
        );
        let mut full_json = decrypt_content(&mutation_key, &intent_ciphertext, &aad)?;
        let full: FullIntent = serde_json::from_slice(&full_json)
            .context("failed to deserialize full mutation intent")?;
        full_json.zeroize();

        let semantic = semantic_intent_bytes(&full);
        let intent_key = hkdf_intent_hmac_key(&mutation_key, &self.store.scope().conversation_id);
        let recomputed = hmac_sha256(&intent_key, INTENT_HMAC_DOMAIN, &semantic);
        if recomputed != stored_hmac {
            bail!("provider-context mutation intent HMAC mismatch");
        }

        let outcome = self
            .apply_intent(&mut transaction, &full, mutation_id, &intent_key)
            .await?;

        transaction.commit().await?;
        Ok(outcome)
    }

    async fn apply_intent(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        full: &FullIntent,
        mutation_id: &str,
        intent_key: &[u8],
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

            let candidate_gen = i64::try_from(full.config_generation).unwrap_or(i64::MAX);
            let candidate_ord = i64::try_from(full.window_ordinal).unwrap_or(i64::MAX);

            if let Some((head_gen, head_ord, head_id)) = head {
                if (candidate_gen, candidate_ord) < (head_gen, head_ord) {
                    return self
                        .finish_mutation(
                            transaction,
                            mutation_id,
                            "superseded",
                            Some("newer_replace"),
                        )
                        .await
                        .map(|_| ApplyOutcome::Superseded {
                            reason: "newer_replace".to_owned(),
                        });
                }
                if (candidate_gen, candidate_ord) == (head_gen, head_ord)
                    && head_id == full.provider_context_id
                {
                    return self
                        .finish_mutation(
                            transaction,
                            mutation_id,
                            "applied",
                            Some("already_satisfied"),
                        )
                        .await
                        .map(|_| ApplyOutcome::AlreadySatisfied);
                }
                if (candidate_gen, candidate_ord) == (head_gen, head_ord)
                    && head_id != full.provider_context_id
                {
                    return self
                        .finish_mutation(
                            transaction,
                            mutation_id,
                            "superseded",
                            Some("newer_replace"),
                        )
                        .await
                        .map(|_| ApplyOutcome::Superseded {
                            reason: "newer_replace".to_owned(),
                        });
                }
            }

            self.invalidate_ids(transaction, &full.invalidate_ids)
                .await?;

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
                    .map(|v| i64::try_from(v).unwrap_or(i64::MAX)),
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
                    .map(|v| i64::try_from(v).unwrap_or(i64::MAX)),
            )
            .bind(full.context_fingerprint.as_ref())
            .bind(&full.key_ref)
            .bind(&full.ciphertext)
            .bind(i64::try_from(full.eviction_tokens).unwrap_or(i64::MAX))
            .bind(i64::from(full.eviction_estimator_version))
            .bind(&full.created_at)
            .execute(&mut **transaction)
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
            Ok(ApplyOutcome::Applied)
        } else {
            if full.invalidate_ids.is_empty() {
                bail!("Invalidate intent requires a non-empty target set");
            }
            self.invalidate_ids(transaction, &full.invalidate_ids)
                .await?;
            self.finish_mutation(transaction, mutation_id, "applied", None)
                .await?;
            Ok(ApplyOutcome::Applied)
        }
    }

    async fn invalidate_ids(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        ids: &[String],
    ) -> Result<()> {
        for id in ids {
            let row = sqlx::query(
                "SELECT message_id, eviction_tokens FROM provider_context WHERE id = ?",
            )
            .bind(id)
            .fetch_optional(&mut **transaction)
            .await?;

            if let Some(row) = row {
                let message_id: Option<String> = row.try_get("message_id")?;
                let tokens: i64 = row.try_get("eviction_tokens")?;

                if let Some(message_id) = message_id {
                    self.decrement_batch_footprint(transaction, &message_id, tokens)
                        .await?;
                }

                sqlx::query("DELETE FROM provider_context WHERE id = ?")
                    .bind(id)
                    .execute(&mut **transaction)
                    .await?;
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

        if let Some(batch_id) = batch_id {
            let updated = sqlx::query(
                "UPDATE memory_batches
                 SET eviction_footprint_tokens = eviction_footprint_tokens - ?
                 WHERE id = ? AND eviction_footprint_tokens >= ?",
            )
            .bind(tokens)
            .bind(&batch_id)
            .bind(tokens)
            .execute(&mut **transaction)
            .await?;
            if updated.rows_affected() != 1 {
                bail!("batch footprint underflow for {batch_id}");
            }
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
        sqlx::query(
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
    use serde_json::json;
    use sqlx::Row;

    use super::*;
    use crate::provider::types::ProviderContextAnchor;
    use crate::store::{DataKeyPurpose, ProviderContextKeyAnchor, Store};

    async fn store() -> Store {
        Store::session_test_store("conversation-1")
            .await
            .expect("open test store")
    }

    async fn seed_message(store: &Store, id: &str, seq: u64) -> anyhow::Result<()> {
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
        .bind(i64::try_from(seq).unwrap_or(i64::MAX))
        .bind(&key.key_ref)
        .execute(store.pool())
        .await?;
        Ok(())
    }

    fn reasoning_item(message_id: impl Into<String>, message_seq: u64) -> ProviderContextItem {
        ProviderContextItem {
            origin_message: Some(ProviderContextAnchor {
                message_id: message_id.into(),
                message_seq,
            }),
            wire_item_index: Some(0),
            ordinal: 1,
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::OpenAiChatCompletions,
                item: json!({"text": "opaque reasoning"}),
            },
        }
    }

    async fn reasoning_record(
        store: &Store,
        message_id: &str,
        message_seq: u64,
        id: &str,
    ) -> EncryptedProviderContextRecord {
        let anchor = ProviderContextKeyAnchor {
            conversation_id: store.scope().conversation_id.clone(),
            anchor_id: format!("{message_id}:{message_seq}"),
        };
        let key = store
            .provider_context_key(&anchor)
            .await
            .expect("mint reasoning anchor key");
        let item = reasoning_item(message_id, message_seq);
        EncryptedProviderContextRecord::encrypt(
            &item,
            "provider-instance-1",
            ApiProtocol::OpenAiChatCompletions,
            "model-1",
            id,
            format!("{id}:0:1:encrypted_reasoning"),
            &key,
            &store.scope(),
        )
        .expect("encrypt reasoning record")
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
        record.insert(store.pool()).await.unwrap();

        // Duplicate (message_id, wire_item_index, item_ordinal) must fail.
        let record2 = reasoning_record(&store, "message-1", 7, "pc-2").await;
        let result = record2.insert(store.pool()).await;
        assert!(result.is_err(), "duplicate ordinal must be rejected");
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
        let different_record = EncryptedProviderContextRecord::encrypt(
            &different_item,
            "provider-instance-1",
            ApiProtocol::OpenAiChatCompletions,
            "model-1",
            "pc-different",
            "pc-different:0:2:encrypted_reasoning",
            &store
                .provider_context_key(&ProviderContextKeyAnchor {
                    conversation_id: store.scope().conversation_id.clone(),
                    anchor_id: "message-1:7".to_owned(),
                })
                .await
                .expect("mint different anchor key"),
            &store.scope(),
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
        seed_message(&store, "message-1", 7).await.unwrap();

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
            None,
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
        .build_replace(None, vec![], &a, &reasoning_item("message-1", 7), 1, 1)
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
        .build_replace(None, vec![], &b, &reasoning_item("message-1", 7), 1, 1)
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
                    None,
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
        record.insert(store.pool()).await.unwrap();

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
}
