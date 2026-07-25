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
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, Zeroizing};

use crate::provider::types::{ApiProtocol, ProviderContextItem, ProviderContextPayload};

use super::crypto::{RowAad, decrypt_content, encrypt_content};
use super::{AgentScope, DataKeyMaterial, DataKeyPurpose, Store};

fn sqlite_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{field} exceeds SQLite INTEGER range"))
}

const INTENT_HMAC_INFO: &[u8] = b"provider-context-mutation-intent/v1";
const INTENT_HMAC_KEY_ID: &str = "mutation-intent-hmac/v1";
const PLAINTEXT_HMAC_DOMAIN: &[u8] = b"sumi-provider-context-plaintext/v1";
const INTENT_HMAC_DOMAIN: &[u8] = b"sumi-provider-context-mutation-intent/v1";
const SCOPE_KEY_DOMAIN: &[u8] = b"sumi-provider-context-scope/v1";
const PREPARED_KEY_MATERIAL_PROOF_DOMAIN: &[u8] = b"sumi-event-batch-prepared-key-material/v1";
const PREPARED_KEY_MATERIAL_PROOF: &[u8] = b"active-key-material";

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

    /// Returns the V1 estimate for a payload: opaque `EncryptedReasoning`
    /// payloads pay `ceil(serialized_bytes / 4)` re-send tokens; native
    /// compaction windows carry zero because they replace rather than append
    /// to the context.
    pub(crate) fn from_payload(payload: &ProviderContextPayload) -> Self {
        let tokens = match payload {
            ProviderContextPayload::EncryptedReasoning { item, .. } => {
                let mut bytes = serde_json::to_vec(item).unwrap_or_default();
                let tokens = (bytes.len() as u64).div_ceil(4);
                bytes.zeroize();
                tokens
            }
            _ => 0,
        };
        Self {
            tokens,
            version: Self::V1,
        }
    }

    /// Returns the V1 estimate for a full provider-context item.
    pub(crate) fn v1(item: &ProviderContextItem) -> Self {
        Self::from_payload(&item.payload)
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
/// with the current replace head.  An absent witness is allowed; a present
/// witness must match the head's latest insert id.
fn expected_latest_matches_head(full: &FullIntent, head: Option<&(i64, i64, String)>) -> bool {
    match head {
        None => full.expected_latest_id.is_none(),
        Some((_, _, head_id)) => full
            .expected_latest_id
            .as_ref()
            .is_none_or(|expected| expected == head_id),
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
}

impl<'a> ProviderContextMutationApplier<'a> {
    pub(crate) fn new(store: &'a Store) -> Self {
        Self { store }
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

    pub(crate) async fn prepare(&self, prepared: &PreparedProviderContextMutation) -> Result<()> {
        if prepared.mutation_id.is_empty() {
            bail!("mutation_id must not be empty");
        }

        let mut transaction = self.store.pool().begin().await?;

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

            sqlx::query(
                "UPDATE provider_context_mutations
                 SET intent_ciphertext = ?, intent_hmac = ?, prepared_at = ?
                 WHERE mutation_id = ?",
            )
            .bind(&prepared.intent_ciphertext)
            .bind(&prepared.intent_hmac)
            .bind(Utc::now().to_rfc3339())
            .bind(&prepared.mutation_id)
            .execute(&mut *transaction)
            .await
            .context("failed to CAS-update provider-context mutation intent")?;

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

        self.apply_full_intent(transaction, &full, mutation_id, &intent_key)
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

            let candidate_gen = sqlite_i64(full.config_generation, "config_generation")?;
            let candidate_ord = sqlite_i64(full.window_ordinal, "window_ordinal")?;

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
            .bind(sqlite_i64(
                full.eviction_tokens,
                "provider_context.eviction_tokens",
            )?)
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
        .bind(sqlite_i64(seq, "messages.seq")?)
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
            store.scope(),
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

    #[tokio::test]
    async fn replace_prepare_enforces_expected_latest_id_and_allows_cas_update() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

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
        .build_replace(None, vec![], &a, &reasoning_item("message-1", 7), 1, 1)
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
        seed_message(&store, "message-1", 7).await.unwrap();

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
}
