//! Encrypted provider-context records and durable provider-context mutations.
//!
//! Provider context (opaque reasoning / native compaction windows) is stored in
//! its own per-anchor data key, separate from the public transcript.  This
//! module owns the encryption envelope, the canonical `Replace`/`Invalidate`
//! mutation intent, the HKDF-derived HMAC binding, and the transactional
//! apply/CAS primitives used by `ProviderContextMutationRecovery`.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use sqlx::Row;
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, Zeroizing};

use crate::memory::estimate::{
    EVICTION_ESTIMATOR_VERSION_REPLAY_PROBE_V1, EVICTION_ESTIMATOR_VERSION_SERIALIZED_BYTES,
    EvictionFootprint, eviction_footprint_for_payload, legacy_serialized_bytes_eviction_footprint,
};
use crate::provider::model::ModelSpec;
use crate::provider::types::{
    ApiProtocol, ContextMessage, Message, ProviderContextAnchor, ProviderContextItem,
    ProviderContextPayload, ProviderOrigin, validate_native_suffix_for_hydration,
};

use super::crypto::{RowAad, decrypt_content, encrypt_content};
use super::event_writer::{
    ProviderContextOwnerEventEvidence, authenticate_provider_context_owner_events,
    require_single_cas,
};
use super::memory_state::MemoryLayer;
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

/// Canonical durable row identity for one provider-context item.
///
/// The exact owning MessageEnd is encrypted inside `ProviderContextItem` and
/// repeated in the row identity bound by AEAD AAD. Hydration requires both
/// representations to agree, so lifecycle ownership never depends on the
/// nullable semantic `origin_message` columns.
pub(crate) fn provider_context_record_id(item: &ProviderContextItem) -> String {
    let wire_label = item
        .wire_item_index
        .map_or_else(|| "_".to_owned(), |index| index.to_string());
    format!(
        "{}:{}:{wire_label}:{}",
        item.retention_owner.message_id, item.retention_owner.message_seq, item.ordinal
    )
}

/// Canonical idempotency key for provider-context records.
///
/// Regular reasoning items use
/// `owner_message_id:wire_item_index:ordinal:kind`; native/dedicated compaction
/// windows use `owner_message_id:coverage_seq:fingerprint`.
/// The key is stored in `provider_context.idempotency_key` and used for
/// uniqueness and mutation-intent HMACs, while the row `id` remains a distinct
/// stable record identifier.
pub(crate) fn provider_context_idempotency_key(item: &ProviderContextItem) -> String {
    let owner_message_id = &item.retention_owner.message_id;
    match &item.payload {
        ProviderContextPayload::EncryptedReasoning { .. } => {
            let wire_label = item
                .wire_item_index
                .map_or_else(|| "_".to_owned(), |index| index.to_string());
            format!(
                "{}:{}:{}:{}",
                owner_message_id,
                wire_label,
                item.ordinal,
                ProviderContextKind::from_payload(&item.payload).as_str()
            )
        }
        ProviderContextPayload::OpenAiCompactedWindow { coverage, .. }
        | ProviderContextPayload::AnthropicCompaction { coverage, .. } => {
            format!(
                "{}:{}:{}",
                owner_message_id, coverage.through_message_seq, coverage.context_fingerprint
            )
        }
    }
}

pub(crate) fn provider_context_owner_key_ref(
    scope: &AgentScope,
    owner: &ProviderContextAnchor,
) -> String {
    super::provider_context_key_ref(
        scope,
        &format!("{}:{}", owner.message_id, owner.message_seq),
    )
}

fn validate_provider_context_item_semantics(item: &ProviderContextItem) -> Result<()> {
    if item.retention_owner.message_id.is_empty() {
        bail!("provider-context retention owner message_id must not be empty");
    }
    if item.provider_origin.provider_instance_id.is_empty() || item.provider_origin.model.is_empty()
    {
        bail!("provider-context item has an empty provider origin field");
    }

    let payload_protocol = match &item.payload {
        ProviderContextPayload::OpenAiCompactedWindow { .. } => ApiProtocol::OpenAiResponses,
        ProviderContextPayload::AnthropicCompaction { .. } => ApiProtocol::AnthropicMessages,
        ProviderContextPayload::EncryptedReasoning { protocol, .. } => *protocol,
    };
    if item.provider_origin.protocol != payload_protocol {
        bail!("provider-context item protocol does not match its payload");
    }

    match &item.payload {
        ProviderContextPayload::OpenAiCompactedWindow { .. }
        | ProviderContextPayload::AnthropicCompaction { .. } => {
            if item.origin_message.is_some() {
                bail!("native provider context must not have an origin message");
            }
            if item.wire_item_index.is_some() {
                bail!("native provider context must not have a wire_item_index");
            }
        }
        ProviderContextPayload::EncryptedReasoning { .. } => {
            if item.origin_message.as_ref() != Some(&item.retention_owner) {
                bail!(
                    "encrypted reasoning origin message must match its provider-context retention owner"
                );
            }
            if item.wire_item_index.is_none() {
                bail!("encrypted reasoning must have a wire_item_index");
            }
        }
    }
    Ok(())
}

fn canonical_eviction_footprint(
    item: &ProviderContextItem,
    estimator_version: u32,
) -> Result<EvictionFootprint> {
    match estimator_version {
        EVICTION_ESTIMATOR_VERSION_SERIALIZED_BYTES => {
            legacy_serialized_bytes_eviction_footprint(&item.payload)
                .context("failed to compute legacy provider-context eviction footprint")
        }
        EVICTION_ESTIMATOR_VERSION_REPLAY_PROBE_V1 => {
            let spec = ModelSpec::from_origin(&item.provider_origin).ok_or_else(|| {
                anyhow!("provider-context origin has no canonical model specification")
            })?;
            eviction_footprint_for_payload(&spec, &item.payload)
                .context("failed to compute canonical provider-context eviction footprint")
        }
        version => bail!("unsupported provider-context eviction estimator version {version}"),
    }
}

#[derive(Debug)]
struct CanonicalProviderContextMetadata {
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
    eviction_tokens: u64,
    eviction_estimator_version: u32,
}

fn canonical_provider_context_metadata(
    scope: &AgentScope,
    item: &ProviderContextItem,
    eviction_estimator_version: u32,
) -> Result<CanonicalProviderContextMetadata> {
    validate_provider_context_item_semantics(item)?;
    let footprint = canonical_eviction_footprint(item, eviction_estimator_version)?;
    let (coverage_through_seq, context_fingerprint) = match &item.payload {
        ProviderContextPayload::OpenAiCompactedWindow { coverage, .. }
        | ProviderContextPayload::AnthropicCompaction { coverage, .. } => (
            Some(coverage.through_message_seq),
            Some(coverage.context_fingerprint.clone()),
        ),
        ProviderContextPayload::EncryptedReasoning { .. } => (None, None),
    };

    Ok(CanonicalProviderContextMetadata {
        id: provider_context_record_id(item),
        message_id: item
            .origin_message
            .as_ref()
            .map(|anchor| anchor.message_id.clone()),
        message_seq: item
            .origin_message
            .as_ref()
            .map(|anchor| anchor.message_seq),
        wire_item_index: item.wire_item_index,
        item_ordinal: item.ordinal,
        idempotency_key: provider_context_idempotency_key(item),
        provider_instance_id: item.provider_origin.provider_instance_id.clone(),
        protocol: item.provider_origin.protocol,
        model: item.provider_origin.model.clone(),
        kind: ProviderContextKind::from_payload(&item.payload),
        coverage_through_seq,
        context_fingerprint,
        key_ref: provider_context_owner_key_ref(scope, &item.retention_owner),
        eviction_tokens: footprint.eviction_tokens(),
        eviction_estimator_version: footprint.estimator_version(),
    })
}

pub(super) fn validate_canonical_created_at(created_at: &str) -> Result<()> {
    let parsed = DateTime::parse_from_rfc3339(created_at)
        .context("provider-context created_at is not RFC 3339")?;
    if parsed.with_timezone(&Utc).to_rfc3339() != created_at {
        bail!("provider-context created_at is not canonical UTC RFC 3339");
    }
    Ok(())
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
        eviction_footprint: EvictionFootprint,
        data_key: &DataKeyMaterial,
        scope: &AgentScope,
    ) -> Result<Self> {
        if data_key.purpose != DataKeyPurpose::ProviderContext {
            bail!("provider-context records require a provider_context data key");
        }

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

        let expected_footprint =
            canonical_eviction_footprint(item, eviction_footprint.estimator_version())?;
        if eviction_footprint != expected_footprint {
            bail!(
                "provider-context eviction footprint does not match the canonical payload footprint"
            );
        }
        let metadata = canonical_provider_context_metadata(
            scope,
            item,
            eviction_footprint.estimator_version(),
        )?;
        if data_key.key_ref != metadata.key_ref {
            bail!("provider-context data key does not match the deterministic retention-owner key");
        }

        let aad = scope.row_aad(
            "provider_context",
            &metadata.id,
            DataKeyPurpose::ProviderContext,
        );
        let plaintext = Zeroizing::new(
            serde_json::to_vec(item).context("failed to serialize provider-context plaintext")?,
        );
        let ciphertext = encrypt_content(data_key, &plaintext, &aad)?;

        Ok(Self {
            id: metadata.id,
            message_id: metadata.message_id,
            message_seq: metadata.message_seq,
            wire_item_index: metadata.wire_item_index,
            item_ordinal: metadata.item_ordinal,
            idempotency_key: metadata.idempotency_key,
            provider_instance_id: metadata.provider_instance_id,
            protocol: metadata.protocol,
            model: metadata.model,
            kind: metadata.kind,
            coverage_through_seq: metadata.coverage_through_seq,
            context_fingerprint: metadata.context_fingerprint,
            key_ref: metadata.key_ref,
            ciphertext,
            eviction_tokens: metadata.eviction_tokens,
            eviction_estimator_version: metadata.eviction_estimator_version,
            created_at: Utc::now().to_rfc3339(),
        })
    }

    fn validate_against_plaintext(
        &self,
        scope: &AgentScope,
        plaintext: &ProviderContextItem,
    ) -> Result<()> {
        let expected =
            canonical_provider_context_metadata(scope, plaintext, self.eviction_estimator_version)?;
        if self.id != expected.id {
            bail!("Replace provider-context row id is not canonical for its plaintext owner");
        }
        if self.message_id != expected.message_id || self.message_seq != expected.message_seq {
            bail!("Replace provider-context semantic origin metadata does not match plaintext");
        }
        if self.wire_item_index != expected.wire_item_index
            || self.item_ordinal != expected.item_ordinal
        {
            bail!("Replace provider-context wire placement does not match plaintext");
        }
        if self.idempotency_key != expected.idempotency_key {
            bail!("Replace provider-context idempotency key is not canonical");
        }
        if self.provider_instance_id != expected.provider_instance_id
            || self.protocol != expected.protocol
            || self.model != expected.model
        {
            bail!("Replace provider-context provider origin metadata does not match plaintext");
        }
        if self.kind != expected.kind
            || self.coverage_through_seq != expected.coverage_through_seq
            || self.context_fingerprint != expected.context_fingerprint
        {
            bail!("Replace provider-context payload metadata does not match plaintext");
        }
        if self.key_ref != expected.key_ref {
            bail!("Replace provider-context key is not bound to its retention owner");
        }
        if self.eviction_tokens != expected.eviction_tokens
            || self.eviction_estimator_version != expected.eviction_estimator_version
        {
            bail!("Replace provider-context eviction metadata does not match plaintext");
        }
        if self.ciphertext.is_empty() {
            bail!("Replace provider-context ciphertext must not be empty");
        }
        validate_canonical_created_at(&self.created_at)?;
        Ok(())
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

    fn validate_identity_and_variant(&self, mutation_id: &str) -> Result<()> {
        if self.mutation_id != mutation_id {
            bail!("provider-context mutation plaintext identity does not match its durable row");
        }
        if !matches!(self.variant.as_str(), "replace" | "invalidate") {
            bail!(
                "provider-context mutation {} has unsupported variant {}",
                self.mutation_id,
                self.variant
            );
        }
        Ok(())
    }

    fn validate_replace_insert(
        &self,
        scope: &AgentScope,
        plaintext: &ProviderContextItem,
    ) -> Result<()> {
        if !self.is_replace() {
            bail!("provider-context insert validation requires a Replace intent");
        }
        let expected =
            canonical_provider_context_metadata(scope, plaintext, self.eviction_estimator_version)?;
        if self.provider_context_id != expected.id {
            bail!("Replace provider-context row id is not canonical for its plaintext owner");
        }
        if self.message_id != expected.message_id || self.message_seq != expected.message_seq {
            bail!("Replace provider-context semantic origin metadata does not match plaintext");
        }
        if self.wire_item_index != expected.wire_item_index
            || self.item_ordinal != expected.item_ordinal
        {
            bail!("Replace provider-context wire placement does not match plaintext");
        }
        if self.idempotency_key != expected.idempotency_key {
            bail!("Replace provider-context idempotency key is not canonical");
        }
        if self.provider_instance_id != expected.provider_instance_id
            || self.protocol != expected.protocol.as_str()
            || self.model != expected.model
        {
            bail!("Replace provider-context provider origin metadata does not match plaintext");
        }
        if self.kind != expected.kind.as_str()
            || self.coverage_through_seq != expected.coverage_through_seq
            || self.context_fingerprint != expected.context_fingerprint
        {
            bail!("Replace provider-context payload metadata does not match plaintext");
        }
        if self.key_ref != expected.key_ref {
            bail!("Replace provider-context key is not bound to its retention owner");
        }
        if self.eviction_tokens != expected.eviction_tokens
            || self.eviction_estimator_version != expected.eviction_estimator_version
        {
            bail!("Replace provider-context eviction metadata does not match plaintext");
        }
        if self.ciphertext.is_empty() {
            bail!("Replace provider-context ciphertext must not be empty");
        }
        validate_canonical_created_at(&self.created_at)?;
        Ok(())
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
#[derive(Clone, Debug)]
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

    pub(in crate::store) fn mutation_id(&self) -> &str {
        &self.mutation_id
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
        insert
            .validate_against_plaintext(&self.scope, plaintext)
            .context("Replace provider-context insert is not canonical")?;
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
    key_refs: BTreeSet<AuthenticatedProviderContextKeyRef>,
    deleted_ids: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct AuthenticatedProviderContextKeyRef(String);

#[derive(Debug)]
struct AuthenticatedInvalidationTarget {
    id: String,
    message_id: Option<String>,
    eviction_tokens: u64,
    key_ref: AuthenticatedProviderContextKeyRef,
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
        let mut transaction = self.store.pool().begin().await?;
        self.prepare_in_transaction(&mut transaction, prepared)
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Persist one prepared intent inside an existing EventWriter transaction.
    ///
    /// Destructive provider-context application remains a separate, common
    /// eventless projection. This entry point exists so an attempt disposition
    /// and the durable prepare record cannot be split by a crash.
    pub(in crate::store) async fn prepare_in_transaction(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        prepared: &PreparedProviderContextMutation,
    ) -> Result<()> {
        if prepared.mutation_id.is_empty() {
            bail!("mutation_id must not be empty");
        }

        #[allow(clippy::type_complexity)]
        let existing: Option<(String, String, Vec<u8>, String, Vec<u8>, Vec<u8>)> = sqlx::query_as(
            "SELECT state, intent_key_ref, intent_hmac, hmac_key_id, intent_ciphertext, prepared_at
             FROM provider_context_mutations WHERE mutation_id = ?",
        )
        .bind(&prepared.mutation_id)
        .fetch_optional(&mut **transaction)
        .await
        .context("failed to load existing mutation row")?;

        let mutation_key = self
            .store
            .data_key_by_ref_in_transaction(transaction, &prepared.intent_key_ref)
            .await
            .context("failed to load mutation key for prepare")?;
        if mutation_key.purpose != DataKeyPurpose::Mutation {
            bail!("provider-context mutation prepare key has wrong purpose");
        }
        if prepared.hmac_key_id != INTENT_HMAC_KEY_ID {
            bail!("provider-context mutation prepare has unsupported HMAC key id");
        }
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
                .is_intent_latest_candidate(transaction, &new_full, &intent_key)
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
            .execute(&mut **transaction)
            .await
            .context("failed to CAS-update provider-context mutation intent")?;

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
            .load_replace_head(transaction, &new_full, &intent_key)
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
        .execute(&mut **transaction)
        .await
        .context("failed to prepare provider-context mutation")?;

        Ok(())
    }

    /// Authenticate an in-memory private-builder result before EventWriter
    /// enters its SQLite transaction. The proof binds the mutation key into
    /// EventWriter's prepared-key revalidation, while the same ciphertext/HMAC
    /// is authenticated again by `prepare_in_transaction`.
    pub(in crate::store) async fn verify_prepared_invalidate_and_size(
        &self,
        prepared: &PreparedProviderContextMutation,
    ) -> Result<ProviderContextProjectionSize> {
        if prepared.mutation_id.is_empty() {
            bail!("mutation_id must not be empty");
        }
        if prepared.hmac_key_id != INTENT_HMAC_KEY_ID {
            bail!("provider-context mutation prepare has unsupported HMAC key id");
        }

        let mut transaction = self.store.pool().begin().await?;
        let mutation_key = self
            .store
            .data_key_by_ref_in_transaction(&mut transaction, &prepared.intent_key_ref)
            .await
            .context("failed to load mutation key for prepared projection")?;
        if mutation_key.purpose != DataKeyPurpose::Mutation {
            bail!("provider-context mutation prepare key has wrong purpose");
        }
        let aad = self.store.scope().row_aad(
            "provider_context_mutations",
            &prepared.mutation_id,
            DataKeyPurpose::Mutation,
        );
        let intent_key = hkdf_intent_hmac_key(&mutation_key, &self.store.scope().conversation_id);
        let full = self.decrypt_full_intent(
            &mutation_key,
            &prepared.intent_ciphertext,
            &aad,
            &intent_key,
            &prepared.intent_hmac,
            "prepared",
        )?;
        if full.mutation_id != prepared.mutation_id {
            bail!("prepared provider-context mutation identity mismatch");
        }
        if full.variant != "invalidate"
            || full.invalidate_ids.is_empty()
            || !full.provider_context_id.is_empty()
            || !full.key_ref.is_empty()
            || !full.ciphertext.is_empty()
        {
            bail!("attempt disposition requires an Invalidate provider-context intent");
        }

        let size = full
            .invalidate_ids
            .iter()
            .map(String::len)
            .sum::<usize>()
            .saturating_add(512);
        transaction.commit().await?;
        Ok(ProviderContextProjectionSize {
            size,
            intent_key_ref: mutation_key.key_ref.clone(),
            intent_key_proof: super::crypto::keyed_proof(
                &mutation_key,
                PREPARED_KEY_MATERIAL_PROOF_DOMAIN,
                PREPARED_KEY_MATERIAL_PROOF,
            ),
            insert_key_ref: None,
            insert_key_proof: None,
        })
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

    async fn authenticate_replace_retention_owner(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        item: &ProviderContextItem,
    ) -> Result<()> {
        let messages = self
            .store
            .hydrate_messages(transaction)
            .await
            .context("failed to authenticate transcript for provider-context Replace")?;
        let owner = messages.iter().find(|message| {
            matches!(
                message,
                ContextMessage::Persisted { id, seq, .. }
                    if id == &item.retention_owner.message_id
                        && *seq == item.retention_owner.message_seq
            )
        });
        let Some(ContextMessage::Persisted {
            message: Message::Assistant(assistant),
            ..
        }) = owner
        else {
            bail!(
                "Replace provider-context retention owner {}:{} is not an authenticated persisted assistant",
                item.retention_owner.message_id,
                item.retention_owner.message_seq
            );
        };
        if assistant.origin != item.provider_origin {
            bail!(
                "Replace provider-context provider origin does not match its authenticated retention owner"
            );
        }

        if let ProviderContextPayload::OpenAiCompactedWindow { coverage, .. }
        | ProviderContextPayload::AnthropicCompaction { coverage, .. } = &item.payload
        {
            validate_native_suffix_for_hydration(&messages, coverage.through_message_seq).map_err(
                |message| anyhow!("Replace provider-context native coverage is invalid: {message}"),
            )?;
        }
        Ok(())
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
            let item: ProviderContextItem = serde_json::from_slice(&plaintext)
                .context("Replace provider-context plaintext is not a ProviderContextItem")?;
            full.validate_replace_insert(self.store.scope(), &item)
                .context("Replace provider-context insert metadata is not canonical")?;
            self.authenticate_replace_retention_owner(transaction, &item)
                .await?;
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
            "SELECT state, intent_key_ref, intent_ciphertext, hmac_key_id, intent_hmac
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
        let hmac_key_id: String = row.try_get("hmac_key_id")?;
        let stored_hmac: Vec<u8> = row.try_get("intent_hmac")?;
        if hmac_key_id != INTENT_HMAC_KEY_ID {
            bail!("provider-context mutation has unsupported HMAC key id");
        }

        let mutation_key = self
            .store
            .data_key_by_ref_in_transaction(transaction, &intent_key_ref)
            .await?;
        if mutation_key.purpose != DataKeyPurpose::Mutation {
            bail!("provider-context mutation intent key has wrong purpose");
        }
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
        full.validate_identity_and_variant(mutation_id)?;
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

            self.destroy_unreferenced_provider_context_keys(transaction, invalidated.key_refs)
                .await?;

            self.finish_mutation(transaction, mutation_id, "applied", None)
                .await?;
            Ok(ApplyOutcome::Applied)
        } else {
            if full.invalidate_ids.is_empty() {
                bail!("Invalidate intent requires a non-empty target set");
            }
            let invalidated = self
                .invalidate_ids(transaction, &full.invalidate_ids)
                .await?;
            self.destroy_unreferenced_provider_context_keys(transaction, invalidated.key_refs)
                .await?;
            if invalidated.deleted_ids.is_empty() {
                self.finish_mutation(
                    transaction,
                    mutation_id,
                    "applied",
                    Some("already_satisfied"),
                )
                .await?;
                Ok(ApplyOutcome::AlreadySatisfied)
            } else {
                self.finish_mutation(transaction, mutation_id, "applied", None)
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
        let targets = self
            .authenticate_invalidation_targets(transaction, ids)
            .await?;
        let mut result = InvalidatedIds::default();
        for target in targets {
            // Best-effort overwrite the encrypted payload before deleting the
            // row: SQLite free pages may retain bytes. Destroying the
            // unreferenced data key below is the actual crypto-erasure
            // guarantee.
            let zeroed = sqlx::query(
                "UPDATE provider_context
                 SET ciphertext = zeroblob(length(ciphertext))
                 WHERE id = ? AND key_ref = ?",
            )
            .bind(&target.id)
            .bind(&target.key_ref.0)
            .execute(&mut **transaction)
            .await
            .context("failed to crypto-erase provider-context row before delete")?;
            require_single_cas(
                zeroed.rows_affected(),
                "authenticated provider-context ciphertext erase",
            )?;

            #[cfg(test)]
            self.assert_zeroed_before_delete(transaction, &target.id)
                .await?;

            if let Some(message_id) = target.message_id {
                let tokens =
                    sqlite_i64(target.eviction_tokens, "provider_context.eviction_tokens")?;
                self.decrement_batch_footprint(transaction, &message_id, tokens)
                    .await?;
            }

            let deleted = sqlx::query("DELETE FROM provider_context WHERE id = ? AND key_ref = ?")
                .bind(&target.id)
                .bind(&target.key_ref.0)
                .execute(&mut **transaction)
                .await?;
            require_single_cas(
                deleted.rows_affected(),
                "authenticated provider-context row delete",
            )?;
            result.key_refs.insert(target.key_ref);
            result.deleted_ids.insert(target.id);
        }
        Ok(result)
    }

    async fn authenticate_invalidation_targets(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        ids: &[String],
    ) -> Result<Vec<AuthenticatedInvalidationTarget>> {
        let target_ids = unique_sorted(ids.to_vec())?;
        if target_ids.is_empty() {
            return Ok(Vec::new());
        }

        let messages = self
            .store
            .hydrate_messages(transaction)
            .await
            .context("failed to authenticate transcript for provider-context invalidation")?;
        let rows = self
            .store
            .hydrate_authenticated_provider_context(&messages, transaction)
            .await
            .context("failed to authenticate live provider-context invalidation targets")?;
        let mut rows_by_id: BTreeMap<_, _> =
            rows.into_iter().map(|row| (row.id.clone(), row)).collect();
        let mut targets = Vec::new();
        let mut owner_evidence = Vec::new();
        for id in target_ids {
            let Some(row) = rows_by_id.remove(&id) else {
                // Absence is accepted only after the prepared mutation intent
                // and its key material have authenticated in this transaction.
                // SQLite commits the destructive phase atomically, so this is
                // idempotent convergence on a prior durable state, never
                // recovery from a partially applied current transaction.
                continue;
            };
            let owner_message = messages.iter().find_map(|message| match message {
                ContextMessage::Persisted { id, seq, message }
                    if id == &row.item.retention_owner.message_id
                        && *seq == row.item.retention_owner.message_seq =>
                {
                    Some(message.clone())
                }
                ContextMessage::Persisted { .. } | ContextMessage::Synthetic { .. } => None,
            });
            let owner_message = owner_message.ok_or_else(|| {
                anyhow!(
                    "provider-context target {} has no authenticated transcript owner {}:{}",
                    row.id,
                    row.item.retention_owner.message_id,
                    row.item.retention_owner.message_seq
                )
            })?;
            owner_evidence.push(ProviderContextOwnerEventEvidence {
                anchor: row.item.retention_owner.clone(),
                message: owner_message,
            });
            targets.push(AuthenticatedInvalidationTarget {
                id: row.id,
                message_id: row
                    .item
                    .origin_message
                    .as_ref()
                    .map(|anchor| anchor.message_id.clone()),
                eviction_tokens: row.eviction_tokens,
                key_ref: AuthenticatedProviderContextKeyRef(row.key_ref),
            });
        }

        authenticate_provider_context_owner_events(self.store, transaction, &owner_evidence)
            .await
            .context(
                "failed to authenticate MessageEnd evidence for provider-context invalidation",
            )?;
        Ok(targets)
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
        key_refs: BTreeSet<AuthenticatedProviderContextKeyRef>,
    ) -> Result<()> {
        for key_ref in key_refs {
            let key = self
                .store
                .data_key_by_ref_in_transaction(transaction, &key_ref.0)
                .await
                .with_context(|| {
                    format!(
                        "failed to reload authenticated provider-context data key {}",
                        key_ref.0
                    )
                })?;
            if key.purpose != DataKeyPurpose::ProviderContext {
                bail!(
                    "authenticated provider-context erasure target {} changed purpose",
                    key_ref.0
                );
            }
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE key_ref = ?")
                    .bind(&key_ref.0)
                    .fetch_one(&mut **transaction)
                    .await?;
            if count == 0 {
                self.store
                    .destroy_conversation_key_ref_in_transaction(transaction, &key_ref.0)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to crypto-erase provider-context data key {}",
                            key_ref.0
                        )
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
        EvictionFootprint, eviction_footprint_for_payload,
        legacy_serialized_bytes_eviction_footprint, native_canonical_window_footprint,
    };
    use crate::provider::model::ModelSpec;
    use crate::provider::types::{
        ApiProtocol, AssistantContent, AssistantMessage, ContextMessage, Message,
        NativeCompactionCoverage, ProviderContextAnchor, ProviderContextItem,
        ProviderContextPayload, ProviderOrigin, PublicAssistantContent, PublicAssistantMessage,
        PublicMessage, StopReason, Usage,
    };
    use crate::store::{DataKeyPurpose, ProviderContextKeyAnchor, Store};

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

    async fn seed_message(store: &Store, id: &str, seq: u64) -> anyhow::Result<()> {
        let key = store
            .conversation_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint transcript key");
        let origin = reasoning_origin();
        let message = PublicMessage::Assistant(PublicAssistantMessage {
            content: vec![PublicAssistantContent::Text {
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
        });
        let raw = Zeroizing::new(serde_json::to_vec(&message)?);
        let aad = store
            .scope()
            .row_aad("messages", id, DataKeyPurpose::Transcript);
        let ciphertext = encrypt_content(&key, &raw, &aad)?;
        let payload = store.redactor().redact_serialized(&raw)?;
        let search_text = super::super::search_text_from_projection(&payload)?;
        sqlx::query(
            "INSERT INTO messages(
                id, seq, role, raw_key_ref, raw_ciphertext, payload, search_text,
                redaction_version, interrupted, created_at
             ) VALUES(?, ?, 'assistant', ?, ?, ?, ?, ?, 0, 'now')",
        )
        .bind(id)
        .bind(sqlite_i64(seq, "messages.seq")?)
        .bind(&key.key_ref)
        .bind(ciphertext)
        .bind(payload)
        .bind(search_text)
        .bind(i64::from(store.redactor().version()))
        .execute(store.pool())
        .await?;
        Ok(())
    }

    async fn seed_owner_event_evidence(
        store: &Store,
        owners: &[(&str, u64)],
    ) -> anyhow::Result<()> {
        super::super::event_writer::seed_provider_context_owner_event_evidence(store, owners).await
    }

    async fn seed_message_in_open_l0_batch(
        store: &Store,
        message_id: &str,
        seq: u64,
        footprint_tokens: i64,
    ) -> anyhow::Result<String> {
        seed_message(store, message_id, seq).await?;
        let batch_id = format!("l0-batch-{message_id}");
        let batch_seq: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(batch_seq), 0) + 1 FROM memory_batches WHERE layer = ?",
        )
        .bind(MemoryLayer::L0.as_i64())
        .fetch_one(store.pool())
        .await?;
        sqlx::query(
            "INSERT INTO memory_batches(
                id, layer, ord, batch_seq, version, state, est_tokens,
                eviction_footprint_tokens, updated_at
             ) VALUES(?, ?, 1, ?, 0, 'open', 0, ?, 'now')",
        )
        .bind(&batch_id)
        .bind(MemoryLayer::L0.as_i64())
        .bind(batch_seq)
        .bind(footprint_tokens)
        .execute(store.pool())
        .await?;
        sqlx::query("INSERT INTO memory_batch_messages(batch_id, message_id, ord) VALUES(?, ?, 1)")
            .bind(&batch_id)
            .bind(message_id)
            .execute(store.pool())
            .await?;
        Ok(batch_id)
    }

    async fn seed_non_message_event(store: &Store, seq: u64) -> anyhow::Result<()> {
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
        let retention_owner = ProviderContextAnchor {
            message_id: message_id.into(),
            message_seq,
        };
        ProviderContextItem {
            retention_owner: retention_owner.clone(),
            origin_message: Some(retention_owner),
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
    ) -> EncryptedProviderContextRecord {
        reasoning_record_with(store, message_id, message_seq, 0, 0).await
    }

    async fn canonical_reasoning_record_with(
        store: &Store,
        message_id: &str,
        message_seq: u64,
        wire_item_index: u32,
        ordinal: u32,
    ) -> EncryptedProviderContextRecord {
        reasoning_record_with(store, message_id, message_seq, wire_item_index, ordinal).await
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
        let retention_owner = ProviderContextAnchor {
            message_id: message_id.into(),
            message_seq,
        };
        ProviderContextItem {
            retention_owner: retention_owner.clone(),
            origin_message: Some(retention_owner),
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
        let retention_owner = ProviderContextAnchor {
            message_id: message_id.into(),
            message_seq,
        };
        ProviderContextItem {
            retention_owner: retention_owner.clone(),
            origin_message: Some(retention_owner),
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
            reasoning_footprint(&item),
            &key,
            store.scope(),
        )
        .expect("encrypt reasoning record")
    }

    fn reencrypt_test_record(
        store: &Store,
        record: &mut EncryptedProviderContextRecord,
        item: &ProviderContextItem,
        key: &DataKeyMaterial,
    ) {
        let plaintext = Zeroizing::new(
            serde_json::to_vec(item).expect("serialize deliberately corrupted provider context"),
        );
        let aad = store.scope().row_aad(
            "provider_context",
            &record.id,
            DataKeyPurpose::ProviderContext,
        );
        record.ciphertext =
            encrypt_content(key, &plaintext, &aad).expect("encrypt deliberately corrupted record");
    }

    fn build_unchecked_replace_for_test(
        builder: ProviderContextMutationBuilder,
        insert: &EncryptedProviderContextRecord,
        plaintext: &ProviderContextItem,
    ) -> PreparedProviderContextMutation {
        let plaintext_bytes = Zeroizing::new(
            serde_json::to_vec(plaintext).expect("serialize unchecked Replace plaintext"),
        );
        let intent_key =
            hkdf_intent_hmac_key(&builder.mutation_key, &builder.scope.conversation_id);
        let plaintext_hmac = hmac_sha256(&intent_key, PLAINTEXT_HMAC_DOMAIN, &plaintext_bytes);
        builder
            .build_full(
                "replace",
                None,
                Vec::new(),
                Some(insert),
                1,
                1,
                plaintext_hmac,
            )
            .expect("build deliberately unchecked Replace intent")
    }

    #[derive(Clone, Copy)]
    enum PreparedInsertTamper {
        RowId,
        IdempotencyKey,
        SameConversationWrongOwnerKey,
    }

    async fn assert_prepared_insert_tamper_rolls_back(
        tamper: PreparedInsertTamper,
        mutation_id: &str,
        expected_error: &str,
    ) {
        let store = store().await;
        let original_footprint = 1_000_000_i64;
        seed_message_in_open_l0_batch(&store, "message-1", 7, original_footprint)
            .await
            .unwrap();

        let item = reasoning_item("message-1", 7);
        let mut insert = reasoning_record(&store, "message-1", 7).await;
        let canonical_key = store
            .data_key_by_ref(insert.key_ref())
            .await
            .expect("reload canonical owner key");

        match tamper {
            PreparedInsertTamper::RowId => {
                insert.id = "noncanonical-provider-context-id".to_owned();
                reencrypt_test_record(&store, &mut insert, &item, &canonical_key);
            }
            PreparedInsertTamper::IdempotencyKey => {
                insert.idempotency_key = "noncanonical-idempotency-key".to_owned();
            }
            PreparedInsertTamper::SameConversationWrongOwnerKey => {
                seed_message(&store, "different-owner", 8)
                    .await
                    .expect("seed a different authenticated owner in the same conversation");
                let wrong_owner_key = store
                    .provider_context_key(&ProviderContextKeyAnchor {
                        conversation_id: store.scope().conversation_id.clone(),
                        anchor_id: "different-owner:8".to_owned(),
                    })
                    .await
                    .expect("mint same-conversation wrong-owner key");
                insert.key_ref = wrong_owner_key.key_ref.clone();
                reencrypt_test_record(&store, &mut insert, &item, &wrong_owner_key);
            }
        }

        // This bypasses the hardened caller-side builder to model an intent
        // prepared by an older or compromised caller. Apply must independently
        // re-authenticate it inside EventWriter's transaction.
        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let prepared = build_unchecked_replace_for_test(
            ProviderContextMutationBuilder::new(mutation_key, store.scope().clone(), mutation_id),
            &insert,
            &item,
        );
        let applier = ProviderContextMutationApplier::new(&store);
        applier
            .prepare(&prepared)
            .await
            .expect("persist hostile fixture");
        applier
            .verify_and_size(mutation_id)
            .await
            .expect("hostile fixture reaches EventWriter apply validation");

        let error = applier
            .apply(mutation_id)
            .await
            .expect_err("noncanonical prepared Replace must fail closed");
        let message = format!("{error:#}");
        assert!(message.contains(expected_error), "{message}");

        let mutation: (String, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT state, terminal_reason, finished_at
             FROM provider_context_mutations WHERE mutation_id = ?",
        )
        .bind(mutation_id)
        .fetch_one(store.pool())
        .await
        .expect("read rejected mutation state");
        assert_eq!(mutation, ("prepared".to_owned(), None, None));

        let provider_context_rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_context")
                .fetch_one(store.pool())
                .await
                .expect("count provider-context rows after rejected apply");
        assert_eq!(
            provider_context_rows, 0,
            "rejected Replace must not insert a provider-context row"
        );
        let replace_heads: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_context_replace_heads")
                .fetch_one(store.pool())
                .await
                .expect("count replace heads after rejected apply");
        assert_eq!(
            replace_heads, 0,
            "rejected Replace must not transition the monotonic head"
        );
        let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(store.pool())
            .await
            .expect("count durable events after rejected eventless apply");
        assert_eq!(
            events, 0,
            "rejected provider-context projection must remain eventless"
        );
        let footprint: i64 = sqlx::query_scalar(
            "SELECT eviction_footprint_tokens FROM memory_batches WHERE layer = ?",
        )
        .bind(MemoryLayer::L0.as_i64())
        .fetch_one(store.pool())
        .await
        .expect("read L0 footprint after rejected apply");
        assert_eq!(
            footprint, original_footprint,
            "EventWriter transaction must roll back footprint accounting"
        );
    }

    #[tokio::test]
    async fn encrypt_and_build_replace_reject_noncanonical_owner_identity() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();
        seed_message(&store, "different-owner", 8).await.unwrap();
        let item = reasoning_item("message-1", 7);
        let origin = reasoning_origin();
        let wrong_owner_key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "different-owner:8".to_owned(),
            })
            .await
            .expect("mint same-conversation wrong-owner key");

        let encryption_error = match EncryptedProviderContextRecord::encrypt(
            &item,
            &origin.provider_instance_id,
            origin.protocol,
            &origin.model,
            reasoning_footprint(&item),
            &wrong_owner_key,
            store.scope(),
        ) {
            Ok(_) => panic!("encryption must reject a non-owner deterministic key"),
            Err(error) => error,
        };
        assert!(
            format!("{encryption_error:#}")
                .contains("does not match the deterministic retention-owner key")
        );

        let canonical = reasoning_record(&store, "message-1", 7).await;

        let mut bad_id = canonical.clone();
        bad_id.id = "noncanonical-provider-context-id".to_owned();
        let error = ProviderContextMutationBuilder::new(
            store
                .conversation_key(DataKeyPurpose::Mutation)
                .await
                .expect("mint mutation key"),
            store.scope().clone(),
            "bad-id",
        )
        .build_replace(None, Vec::new(), &bad_id, &item, 1, 1)
        .expect_err("builder must reject a noncanonical row id");
        assert!(format!("{error:#}").contains("row id is not canonical"));

        let mut bad_idempotency = canonical.clone();
        bad_idempotency.idempotency_key = "noncanonical-idempotency-key".to_owned();
        let error = ProviderContextMutationBuilder::new(
            store
                .conversation_key(DataKeyPurpose::Mutation)
                .await
                .expect("reuse mutation key"),
            store.scope().clone(),
            "bad-idempotency",
        )
        .build_replace(None, Vec::new(), &bad_idempotency, &item, 1, 1)
        .expect_err("builder must reject a noncanonical idempotency key");
        assert!(format!("{error:#}").contains("idempotency key is not canonical"));

        let mut bad_key = canonical;
        bad_key.key_ref = wrong_owner_key.key_ref.clone();
        let error = ProviderContextMutationBuilder::new(
            store
                .conversation_key(DataKeyPurpose::Mutation)
                .await
                .expect("reuse mutation key"),
            store.scope().clone(),
            "bad-key",
        )
        .build_replace(None, Vec::new(), &bad_key, &item, 1, 1)
        .expect_err("builder must reject a non-owner deterministic key");
        assert!(
            format!("{error:#}").contains("key is not bound to its retention owner"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn prepared_replace_rejects_noncanonical_row_id_without_committing() {
        assert_prepared_insert_tamper_rolls_back(
            PreparedInsertTamper::RowId,
            "prepared-bad-row-id",
            "row id is not canonical",
        )
        .await;
    }

    #[tokio::test]
    async fn prepared_replace_rejects_noncanonical_idempotency_without_committing() {
        assert_prepared_insert_tamper_rolls_back(
            PreparedInsertTamper::IdempotencyKey,
            "prepared-bad-idempotency",
            "idempotency key is not canonical",
        )
        .await;
    }

    #[tokio::test]
    async fn prepared_replace_rejects_same_conversation_wrong_owner_key_without_committing() {
        assert_prepared_insert_tamper_rolls_back(
            PreparedInsertTamper::SameConversationWrongOwnerKey,
            "prepared-wrong-owner-key",
            "key is not bound to its retention owner",
        )
        .await;
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

    #[derive(Debug, PartialEq, Eq)]
    struct DestructiveStateSnapshot {
        provider_context: Vec<String>,
        data_keys: Vec<String>,
        messages: Vec<String>,
        mutations: Vec<String>,
        replace_heads: Vec<String>,
        events: Vec<String>,
        event_heads: Vec<String>,
        memory_batches: Vec<String>,
        memory_membership: Vec<String>,
    }

    async fn snapshot_rows(store: &Store, sql: &str) -> Vec<String> {
        sqlx::query_scalar(sql)
            .fetch_all(store.pool())
            .await
            .expect("snapshot durable rows")
    }

    async fn destructive_state_snapshot(store: &Store) -> DestructiveStateSnapshot {
        DestructiveStateSnapshot {
            provider_context: snapshot_rows(
                store,
                "SELECT json_array(
                    id, message_id, message_seq, wire_item_index, item_ordinal,
                    idempotency_key, provider_instance_id, protocol, model, kind,
                    coverage_through_seq, context_fingerprint, key_ref, hex(ciphertext),
                    eviction_tokens, eviction_estimator_version, created_at
                 ) FROM provider_context ORDER BY id",
            )
            .await,
            data_keys: snapshot_rows(
                store,
                "SELECT json_array(
                    key_ref, scope, purpose, conversation_id, algorithm, wrap_key_id,
                    hex(wrap_nonce), hex(wrapped_key), state, created_at, destroyed_at
                 ) FROM data_keys ORDER BY key_ref",
            )
            .await,
            messages: snapshot_rows(
                store,
                "SELECT json_array(
                    id, seq, role, raw_key_ref, hex(raw_ciphertext), payload, search_text,
                    redaction_version, interrupted, created_at
                 ) FROM messages ORDER BY seq",
            )
            .await,
            mutations: snapshot_rows(
                store,
                "SELECT json_array(
                    mutation_id, state, intent_key_ref, hex(intent_ciphertext),
                    hmac_key_id, hex(intent_hmac), prepared_at, finished_at, terminal_reason
                 ) FROM provider_context_mutations ORDER BY mutation_id",
            )
            .await,
            replace_heads: snapshot_rows(
                store,
                "SELECT json_array(
                    scope_key, max_config_generation, max_window_ordinal,
                    latest_insert_id, updated_at
                 ) FROM provider_context_replace_heads ORDER BY scope_key",
            )
            .await,
            events: snapshot_rows(
                store,
                "SELECT json_array(
                    seq, event_type, internal_metadata, raw_key_ref, hex(raw_ciphertext),
                    envelope, redaction_version, created_at
                 ) FROM agent_events ORDER BY seq",
            )
            .await,
            event_heads: snapshot_rows(
                store,
                "SELECT json_array(
                    conversation_id, last_seq, event_count, hex(chain_digest),
                    key_ref, hex(head_hmac), updated_at
                 ) FROM event_log_heads ORDER BY conversation_id",
            )
            .await,
            memory_batches: snapshot_rows(
                store,
                "SELECT json_array(
                    id, layer, ord, batch_seq, version, state, est_tokens,
                    eviction_footprint_tokens, summary_key_ref, hex(summary_ciphertext),
                    summary_projection, summary_redaction_version, updated_at
                 ) FROM memory_batches ORDER BY layer, ord, id",
            )
            .await,
            memory_membership: snapshot_rows(
                store,
                "SELECT json_array(batch_id, message_id, ord)
                 FROM memory_batch_messages ORDER BY batch_id, ord",
            )
            .await,
        }
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
        let record = canonical_reasoning_record_with(&store, "message-1", 7, 0, 0).await;
        record.insert(store.pool()).await.unwrap();

        // Duplicate (message_id, wire_item_index, item_ordinal) must fail.
        let record2 = reasoning_record(&store, "message-1", 7).await;
        let result = record2.insert(store.pool()).await;
        assert!(result.is_err(), "duplicate ordinal must be rejected");
    }

    #[tokio::test]
    async fn mutation_prepare_is_idempotent_and_rejects_conflicting_hmac() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();
        let record = reasoning_record(&store, "message-1", 7).await;

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
        seed_owner_event_evidence(&store, &[("message-1", 7)])
            .await
            .unwrap();

        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let applier = ProviderContextMutationApplier::new(&store);
        let scope = store.scope().clone();

        // First Replace: config=1, window=1.
        let a_item = reasoning_item("message-1", 7);
        let a = reasoning_record(&store, "message-1", 7).await;
        let a_id = a.id().to_owned();
        let intent_a = ProviderContextMutationBuilder::new(
            mutation_key,
            scope.clone(),
            "replace-a".to_owned(),
        )
        .build_replace(None, vec![], &a, &a_item, 1, 1)
        .expect("build replace-a");
        applier.prepare(&intent_a).await.unwrap();
        assert_eq!(
            applier.apply("replace-a").await.unwrap(),
            ApplyOutcome::Applied
        );

        // Older Replace is superseded.
        let b_item = reasoning_item_with("message-1", 7, 0, 1);
        let b = reasoning_record_with(&store, "message-1", 7, 0, 1).await;
        let mutation_key_b = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("reuse mutation key");
        let intent_b = ProviderContextMutationBuilder::new(
            mutation_key_b,
            scope.clone(),
            "replace-b".to_owned(),
        )
        .build_replace(Some(a_id.clone()), vec![a_id.clone()], &b, &b_item, 0, 0)
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
        .build_replace(Some(a_id.clone()), vec![], &a, &a_item, 1, 1)
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
        .build_replace(Some(a_id.clone()), vec![a_id.clone()], &b, &b_item, 1, 1)
        .expect("build replace-c");
        applier.prepare(&intent_c).await.unwrap();
        let outcome_c = applier.apply("replace-c").await.unwrap();
        assert!(
            matches!(outcome_c, ApplyOutcome::Superseded { reason } if reason == "newer_replace")
        );

        // Strictly greater Replace advances the head and deletes the prior row.
        let e_item = reasoning_item_with("message-1", 7, 0, 2);
        let e = reasoning_record_with(&store, "message-1", 7, 0, 2).await;
        let e_id = e.id().to_owned();
        let mutation_key_e = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("reuse mutation key");
        let intent_e =
            ProviderContextMutationBuilder::new(mutation_key_e, scope, "replace-e".to_owned())
                .build_replace(Some(a_id), vec![a.id().to_owned()], &e, &e_item, 2, 2)
                .expect("build replace-e");
        applier.prepare(&intent_e).await.unwrap();
        assert_eq!(
            applier.apply("replace-e").await.unwrap(),
            ApplyOutcome::Applied
        );

        let remaining: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id IN (?, ?)")
                .bind(a.id())
                .bind(&e_id)
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
        assert_eq!(head.get::<String, _>("latest_insert_id"), e_id);
    }

    #[tokio::test]
    async fn invalidate_deletes_targets_and_marks_mutation_applied() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();
        seed_owner_event_evidence(&store, &[("message-1", 7)])
            .await
            .unwrap();

        let record = reasoning_record(&store, "message-1", 7).await;
        let record_id = record.id().to_owned();
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
        .build_invalidate(None, vec![record_id.clone()])
        .expect("build invalidate intent");

        applier.prepare(&intent).await.unwrap();
        assert_eq!(
            applier.apply("invalidate-1").await.unwrap(),
            ApplyOutcome::Applied
        );

        let remaining: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
                .bind(record_id)
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
        seed_owner_event_evidence(&store, &[("message-1", 7)])
            .await
            .unwrap();

        let mutation_key = store
            .conversation_key(DataKeyPurpose::Mutation)
            .await
            .expect("mint mutation key");
        let applier = ProviderContextMutationApplier::new(&store);
        let scope = store.scope().clone();

        // First Replace creates head pc-a with (gen=1, ord=1).
        let a_item = reasoning_item("message-1", 7);
        let a = reasoning_record(&store, "message-1", 7).await;
        let a_id = a.id().to_owned();
        let intent_a = ProviderContextMutationBuilder::new(
            mutation_key,
            scope.clone(),
            "replace-a".to_owned(),
        )
        .build_replace(None, vec![], &a, &a_item, 1, 1)
        .expect("build replace-a");
        applier.prepare(&intent_a).await.unwrap();
        assert_eq!(
            applier.apply("replace-a").await.unwrap(),
            ApplyOutcome::Applied
        );

        // A newer Replace with a stale expected_latest_id is rejected at prepare.
        let b_item = reasoning_item_with("message-1", 7, 0, 1);
        let b = reasoning_record_with(&store, "message-1", 7, 0, 1).await;
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
            vec![a_id.clone()],
            &b,
            &b_item,
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
        .build_replace(Some(a_id.clone()), vec![], &a, &a_item, 1, 1)
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
        .build_replace(Some(a_id.clone()), vec![], &a, &a_item, 1, 1)
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
                .build_replace(Some(a_id.clone()), vec![a_id], &b, &b_item, 2, 2)
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

        let record = reasoning_record(&store, "message-1", 7).await;
        let record_id = record.id().to_owned();
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
                .bind(record_id)
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(remaining, 1);
    }

    #[tokio::test]
    async fn reasoning_idempotency_key_is_message_wire_ordinal_kind() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let a = reasoning_record(&store, "message-1", 7).await;
        a.insert(store.pool()).await.unwrap();

        let b = reasoning_record(&store, "message-1", 7).await;
        let error = b
            .insert(store.pool())
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

    fn native_compaction_item(
        anthropic: bool,
        owner_message_id: &str,
        owner_message_seq: u64,
        coverage: u64,
    ) -> ProviderContextItem {
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
            retention_owner: ProviderContextAnchor {
                message_id: owner_message_id.to_owned(),
                message_seq: owner_message_seq,
            },
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

    async fn insert_native_compaction(store: &Store, item: &ProviderContextItem) -> String {
        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: format!(
                    "{}:{}",
                    item.retention_owner.message_id, item.retention_owner.message_seq
                ),
            })
            .await
            .expect("mint native provider-context key");
        let id = provider_context_record_id(item);
        EncryptedProviderContextRecord::encrypt(
            item,
            &item.provider_origin.provider_instance_id,
            item.provider_origin.protocol,
            &item.provider_origin.model,
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt native compaction")
        .insert(store.pool())
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
            retention_owner: ProviderContextAnchor {
                message_id: "message-1".to_owned(),
                message_seq: 7,
            },
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
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt compaction a");
        a.insert(store.pool()).await.unwrap();

        // Same request/coverage/fingerprint with a different ordinal still collides
        // on the canonical idempotency key, even though the (message_id, NULL, ordinal)
        // tuple differs.
        base.ordinal = 1;
        let b = EncryptedProviderContextRecord::encrypt(
            &base,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-1",
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt compaction b");
        let error = b
            .insert(store.pool())
            .await
            .expect_err("same canonical compaction idempotency key must collide");
        let message = format!("{error:#}");
        assert!(
            message.contains("idempotency_key") || message.contains("UNIQUE"),
            "{message}"
        );

        // Remove the active window before checking that a different fingerprint
        // produces a distinct canonical idempotency key. Active native windows
        // are independently unique per provider-origin scope.
        sqlx::query("DELETE FROM provider_context WHERE id = ?")
            .bind(a.id())
            .execute(store.pool())
            .await
            .expect("remove first active native window");
        base.ordinal = 1;
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
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt compaction c");
        c.insert(store.pool())
            .await
            .expect("different fingerprint must not collide");
    }

    #[tokio::test]
    async fn invalidation_crypto_erases_data_key_when_unreferenced() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();
        seed_owner_event_evidence(&store, &[("message-1", 7)])
            .await
            .unwrap();
        let record = reasoning_record(&store, "message-1", 7).await;
        let record_id = record.id().to_owned();
        let key_ref = record.key_ref.clone();
        record.insert(store.pool()).await.unwrap();

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
        .build_invalidate(None, vec![record_id.clone()])
        .expect("build invalidate");

        applier.prepare(&prepared).await.unwrap();
        applier.apply("mutation-1").await.unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
            .bind(record_id)
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
        seed_owner_event_evidence(&store, &[("message-1", 7)])
            .await
            .unwrap();
        let old_record = reasoning_record(&store, "message-1", 7).await;
        let old_id = old_record.id().to_owned();
        let key_ref = old_record.key_ref.clone();
        old_record.insert(store.pool()).await.unwrap();

        let new_record = reasoning_record_with(&store, "message-1", 7, 0, 1).await;
        let new_id = new_record.id().to_owned();

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
            vec![old_id.clone()],
            &new_record,
            &reasoning_item_with("message-1", 7, 0, 1),
            1,
            1,
        )
        .expect("build replace");

        applier.prepare(&prepared).await.unwrap();
        applier.apply("mutation-1").await.unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
            .bind(old_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "old provider_context row must be deleted");

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
            .bind(new_id)
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
        seed_owner_event_evidence(&store, &[("message-1", 7)])
            .await
            .unwrap();

        let a = reasoning_record_with(&store, "message-1", 7, 0, 0).await;
        let a_id = a.id().to_owned();
        let key_ref = a.key_ref.clone();
        a.insert(store.pool()).await.unwrap();

        let b = reasoning_record_with(&store, "message-1", 7, 1, 0).await;
        let b_id = b.id().to_owned();
        b.insert(store.pool()).await.unwrap();

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
        .build_invalidate(None, vec![a_id])
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
        .build_invalidate(None, vec![b_id])
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

        let cross_record = reasoning_record(&store, "message-1", 7).await;
        let cross_id = cross_record.id().to_owned();
        let cross_key_ref = cross_record.key_ref.clone();
        cross_record.insert(store.pool()).await.unwrap();

        // Tamper with the data_keys row so it appears to belong to another conversation,
        // simulating a cross-conversation row referenced by this conversation's store.
        sqlx::query(
            "UPDATE data_keys SET conversation_id = 'other-conversation' WHERE key_ref = ?",
        )
        .bind(&cross_key_ref)
        .execute(store.pool())
        .await
        .expect("tamper fixture");

        let replacement = reasoning_record(&store, "message-2", 9).await;

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
            vec![cross_id.clone()],
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
            message.contains("belongs to a different conversation"),
            "{message}"
        );

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
            .bind(&cross_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(count, 1, "cross-conversation row must not be deleted");

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
            .bind(replacement.id())
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(
            count, 0,
            "replacement must not be inserted when invalidation fails closed"
        );
    }

    #[tokio::test]
    async fn invalidate_rejects_other_provider_context_key_before_any_destructive_drift() {
        let store = store().await;
        seed_message_in_open_l0_batch(&store, "message-1", 7, 1_000_000)
            .await
            .unwrap();
        seed_message_in_open_l0_batch(&store, "message-2", 9, 1_000_000)
            .await
            .unwrap();
        seed_owner_event_evidence(&store, &[("message-1", 7), ("message-2", 9)])
            .await
            .unwrap();

        let target = reasoning_record(&store, "message-1", 7).await;
        let target_id = target.id().to_owned();
        let target_key_ref = target.key_ref.clone();
        target.insert(store.pool()).await.unwrap();
        let other = reasoning_record(&store, "message-2", 9).await;
        let other_key_ref = other.key_ref.clone();
        other.insert(store.pool()).await.unwrap();
        assert_ne!(target_key_ref, other_key_ref);

        let applier = ProviderContextMutationApplier::new(&store);
        let prepared = ProviderContextMutationBuilder::new(
            store
                .conversation_key(DataKeyPurpose::Mutation)
                .await
                .expect("mint mutation key"),
            store.scope().clone(),
            "hostile-provider-key-swap".to_owned(),
        )
        .build_invalidate(None, vec![target_id.clone()])
        .expect("build hostile invalidation");
        applier.prepare(&prepared).await.unwrap();

        sqlx::query("UPDATE provider_context SET key_ref = ? WHERE id = ?")
            .bind(&other_key_ref)
            .bind(&target_id)
            .execute(store.pool())
            .await
            .expect("swap target to another live provider-context key");
        let before = destructive_state_snapshot(&store).await;

        let error = applier
            .apply("hostile-provider-key-swap")
            .await
            .expect_err("wrong live provider-context key must fail authentication");
        assert!(
            format!("{error:#}").contains("failed to decrypt provider-context record"),
            "{error:#}"
        );
        assert_eq!(
            destructive_state_snapshot(&store).await,
            before,
            "rejected key swap must preserve both keys/material, rows, heads, events, mutation state, and L0 state"
        );
    }

    #[tokio::test]
    async fn invalidate_rejects_non_provider_context_key_purposes_before_destruction() {
        for purpose in [
            DataKeyPurpose::Event,
            DataKeyPurpose::Transcript,
            DataKeyPurpose::Mutation,
        ] {
            let store = store().await;
            seed_message_in_open_l0_batch(&store, "message-1", 7, 1_000_000)
                .await
                .unwrap();
            seed_owner_event_evidence(&store, &[("message-1", 7)])
                .await
                .unwrap();
            let target = reasoning_record(&store, "message-1", 7).await;
            let target_id = target.id().to_owned();
            target.insert(store.pool()).await.unwrap();

            let mutation_key = store
                .conversation_key(DataKeyPurpose::Mutation)
                .await
                .expect("mint mutation key");
            let wrong_key_ref = if purpose == DataKeyPurpose::Mutation {
                mutation_key.key_ref.clone()
            } else {
                store
                    .conversation_key(purpose)
                    .await
                    .expect("load hostile wrong-purpose key")
                    .key_ref
                    .clone()
            };
            let mutation_id = format!("hostile-{}-key-swap", purpose.as_str());
            let applier = ProviderContextMutationApplier::new(&store);
            let prepared = ProviderContextMutationBuilder::new(
                mutation_key,
                store.scope().clone(),
                mutation_id.clone(),
            )
            .build_invalidate(None, vec![target_id.clone()])
            .expect("build hostile invalidation");
            applier.prepare(&prepared).await.unwrap();

            sqlx::query("UPDATE provider_context SET key_ref = ? WHERE id = ?")
                .bind(&wrong_key_ref)
                .bind(&target_id)
                .execute(store.pool())
                .await
                .expect("swap target to active wrong-purpose key");
            let before = destructive_state_snapshot(&store).await;

            let error = applier
                .apply(&mutation_id)
                .await
                .expect_err("wrong-purpose erasure target must fail closed");
            let message = format!("{error:#}");
            assert!(
                message.contains(&format!("has purpose {}", purpose.as_str()))
                    && message.contains("expected provider_context"),
                "{message}"
            );
            assert_eq!(
                destructive_state_snapshot(&store).await,
                before,
                "{} key swap changed durable state",
                purpose.as_str()
            );
        }
    }

    #[tokio::test]
    async fn replace_rejects_tampered_target_metadata_or_footprint_before_destructive_writes() {
        for tamper in ["metadata", "footprint"] {
            let store = store().await;
            seed_message_in_open_l0_batch(&store, "message-1", 7, 1_000_000)
                .await
                .unwrap();
            seed_owner_event_evidence(&store, &[("message-1", 7)])
                .await
                .unwrap();
            let old = reasoning_record_with(&store, "message-1", 7, 0, 0).await;
            let old_id = old.id().to_owned();
            let tampered_tokens = old
                .eviction_tokens
                .checked_add(1)
                .expect("test footprint increment");
            old.insert(store.pool()).await.unwrap();
            let replacement = reasoning_record_with(&store, "message-1", 7, 0, 1).await;
            let mutation_id = format!("hostile-replace-{tamper}");

            let applier = ProviderContextMutationApplier::new(&store);
            let prepared = ProviderContextMutationBuilder::new(
                store
                    .conversation_key(DataKeyPurpose::Mutation)
                    .await
                    .expect("mint mutation key"),
                store.scope().clone(),
                mutation_id.clone(),
            )
            .build_replace(
                None,
                vec![old_id.clone()],
                &replacement,
                &reasoning_item_with("message-1", 7, 0, 1),
                1,
                1,
            )
            .expect("build hostile Replace");
            applier.prepare(&prepared).await.unwrap();

            match tamper {
                "metadata" => {
                    sqlx::query(
                        "UPDATE provider_context
                         SET provider_instance_id = 'tampered-provider'
                         WHERE id = ?",
                    )
                    .bind(&old_id)
                    .execute(store.pool())
                    .await
                    .expect("tamper target provider metadata");
                }
                "footprint" => {
                    sqlx::query("UPDATE provider_context SET eviction_tokens = ? WHERE id = ?")
                        .bind(
                            sqlite_i64(tampered_tokens, "hostile provider-context eviction tokens")
                                .expect("hostile footprint fits SQLite"),
                        )
                        .bind(&old_id)
                        .execute(store.pool())
                        .await
                        .expect("tamper target footprint");
                }
                _ => unreachable!(),
            }
            let before = destructive_state_snapshot(&store).await;

            let error = applier
                .apply(&mutation_id)
                .await
                .expect_err("tampered Replace invalidation target must fail closed");
            let message = format!("{error:#}");
            let expected = if tamper == "metadata" {
                "stored provider origin does not match authenticated plaintext origin"
            } else {
                "eviction_tokens do not match the decrypted payload"
            };
            assert!(message.contains(expected), "{message}");
            assert_eq!(
                destructive_state_snapshot(&store).await,
                before,
                "rejected Replace {tamper} tamper must not insert, move its head, erase a key, or mutate L0"
            );
        }
    }

    #[tokio::test]
    async fn invalidate_rejects_cross_row_ciphertext_id_binding_before_destruction() {
        let store = store().await;
        seed_message_in_open_l0_batch(&store, "message-1", 7, 1_000_000)
            .await
            .unwrap();
        seed_owner_event_evidence(&store, &[("message-1", 7)])
            .await
            .unwrap();
        let target = reasoning_record_with(&store, "message-1", 7, 0, 0).await;
        let target_id = target.id().to_owned();
        target.insert(store.pool()).await.unwrap();
        let other = reasoning_record_with(&store, "message-1", 7, 0, 1).await;
        let other_ciphertext = other.ciphertext.clone();
        other.insert(store.pool()).await.unwrap();

        let applier = ProviderContextMutationApplier::new(&store);
        let prepared = ProviderContextMutationBuilder::new(
            store
                .conversation_key(DataKeyPurpose::Mutation)
                .await
                .expect("mint mutation key"),
            store.scope().clone(),
            "hostile-ciphertext-id-binding".to_owned(),
        )
        .build_invalidate(None, vec![target_id.clone()])
        .expect("build hostile invalidation");
        applier.prepare(&prepared).await.unwrap();

        sqlx::query("UPDATE provider_context SET ciphertext = ? WHERE id = ?")
            .bind(other_ciphertext)
            .bind(&target_id)
            .execute(store.pool())
            .await
            .expect("copy ciphertext from another authenticated row id");
        let before = destructive_state_snapshot(&store).await;

        let error = applier
            .apply("hostile-ciphertext-id-binding")
            .await
            .expect_err("cross-row ciphertext must fail row-id AAD authentication");
        assert!(
            format!("{error:#}").contains("failed to decrypt provider-context record"),
            "{error:#}"
        );
        assert_eq!(
            destructive_state_snapshot(&store).await,
            before,
            "ciphertext/AAD rejection must precede every destructive mutation"
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

        let record = canonical_reasoning_record_with(&store, "message-1", 7, 0, 0).await;
        record.insert(store.pool()).await.unwrap();

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
        assert_eq!(hydrated[0].origin_message.as_ref().unwrap().message_seq, 7);
    }

    #[tokio::test]
    async fn hydrate_rejects_authenticated_retention_owner_row_id_mismatch() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();
        seed_message(&store, "message-2", 8).await.unwrap();

        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "message-2:8".to_owned(),
            })
            .await
            .unwrap();
        let item = ProviderContextItem {
            retention_owner: ProviderContextAnchor {
                message_id: "message-2".to_owned(),
                message_seq: 8,
            },
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: openai_responses_origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({
                    "id": "cmp-owner-mismatch",
                    "type": "compaction",
                    "encrypted_content": "opaque",
                })],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 7,
                    context_fingerprint: "fp-owner-mismatch".to_owned(),
                },
            },
        };
        let mut row_identity = item.clone();
        row_identity.retention_owner = ProviderContextAnchor {
            message_id: "message-1".to_owned(),
            message_seq: 7,
        };
        let mut record = EncryptedProviderContextRecord::encrypt(
            &item,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-1",
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt canonical owner fixture");
        record.id = provider_context_record_id(&row_identity);
        let plaintext = serde_json::to_vec(&item).unwrap();
        let aad = store.scope().row_aad(
            "provider_context",
            &record.id,
            DataKeyPurpose::ProviderContext,
        );
        record.ciphertext = encrypt_content(&key, &plaintext, &aad).unwrap();
        record.insert(store.pool()).await.unwrap();

        let messages = vec![
            ContextMessage::Persisted {
                id: "message-1".to_owned(),
                seq: 7,
                message: assistant_message(openai_responses_origin()),
            },
            ContextMessage::Persisted {
                id: "message-2".to_owned(),
                seq: 8,
                message: assistant_message(openai_responses_origin()),
            },
        ];
        let error = {
            let mut transaction = store.pool().begin().await.unwrap();
            store
                .hydrate_provider_context(&messages, &mut transaction)
                .await
        }
        .expect_err("authenticated owner and AAD row id mismatch must fail closed");
        assert!(
            format!("{error:#}").contains("row id does not match authenticated retention owner"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_retention_owner_key_binding_mismatch() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let wrong_key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "different-message:9".to_owned(),
            })
            .await
            .unwrap();
        let item = ProviderContextItem {
            retention_owner: ProviderContextAnchor {
                message_id: "message-1".to_owned(),
                message_seq: 7,
            },
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: openai_responses_origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({
                    "id": "cmp-key-mismatch",
                    "type": "compaction",
                    "encrypted_content": "opaque",
                })],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 7,
                    context_fingerprint: "fp-key-mismatch".to_owned(),
                },
            },
        };
        let correct_key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "message-1:7".to_owned(),
            })
            .await
            .unwrap();
        let mut record = EncryptedProviderContextRecord::encrypt(
            &item,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-1",
            dummy_footprint(),
            &correct_key,
            store.scope(),
        )
        .expect("encrypt canonical key fixture");
        record.key_ref = wrong_key.key_ref.clone();
        let plaintext = serde_json::to_vec(&item).unwrap();
        let aad = store.scope().row_aad(
            "provider_context",
            &record.id,
            DataKeyPurpose::ProviderContext,
        );
        record.ciphertext = encrypt_content(&wrong_key, &plaintext, &aad).unwrap();
        record.insert(store.pool()).await.unwrap();

        let messages = vec![ContextMessage::Persisted {
            id: "message-1".to_owned(),
            seq: 7,
            message: assistant_message(openai_responses_origin()),
        }];
        let error = {
            let mut transaction = store.pool().begin().await.unwrap();
            store
                .hydrate_provider_context(&messages, &mut transaction)
                .await
        }
        .expect_err("retention owner and key-ref mismatch must fail closed");
        assert!(
            format!("{error:#}").contains("key_ref does not match authenticated retention owner"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_legacy_provider_context_without_retention_owner() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();
        let item = reasoning_item("message-1", 7);
        let record_id = provider_context_record_id(&item);
        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "message-1:7".to_owned(),
            })
            .await
            .unwrap();
        let record = EncryptedProviderContextRecord::encrypt(
            &item,
            &item.provider_origin.provider_instance_id,
            item.provider_origin.protocol,
            &item.provider_origin.model,
            reasoning_footprint(&item),
            &key,
            store.scope(),
        )
        .unwrap();
        record.insert(store.pool()).await.unwrap();

        let mut legacy_value = serde_json::to_value(&item).unwrap();
        legacy_value
            .as_object_mut()
            .expect("provider context serializes as an object")
            .remove("retention_owner");
        let legacy_plaintext = serde_json::to_vec(&legacy_value).unwrap();
        let aad = store.scope().row_aad(
            "provider_context",
            &record_id,
            DataKeyPurpose::ProviderContext,
        );
        let legacy_ciphertext = encrypt_content(&key, &legacy_plaintext, &aad).unwrap();
        sqlx::query("UPDATE provider_context SET ciphertext = ? WHERE id = ?")
            .bind(legacy_ciphertext)
            .bind(&record_id)
            .execute(store.pool())
            .await
            .unwrap();

        let messages = vec![ContextMessage::Persisted {
            id: "message-1".to_owned(),
            seq: 7,
            message: assistant_message(reasoning_origin()),
        }];
        let error = {
            let mut transaction = store.pool().begin().await.unwrap();
            store
                .hydrate_provider_context(&messages, &mut transaction)
                .await
        }
        .expect_err("legacy plaintext without a durable owner must not be inferred");
        let message = format!("{error:#}");
        assert!(
            message.contains("not a valid ProviderContextItem"),
            "{message}"
        );
        assert!(
            message.contains("missing field `retention_owner`"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_provider_context_ordinal_gap_after_row_loss() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();
        let first = canonical_reasoning_record_with(&store, "message-1", 7, 0, 0).await;
        let first_id = first.id().to_owned();
        first.insert(store.pool()).await.unwrap();
        canonical_reasoning_record_with(&store, "message-1", 7, 0, 1)
            .await
            .insert(store.pool())
            .await
            .unwrap();
        sqlx::query("DELETE FROM provider_context WHERE id = ?")
            .bind(first_id)
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

        let record = canonical_reasoning_record_with(&store, "message-1", 7, 0, 0).await;
        let record_id = record.id().to_owned();
        record.insert(store.pool()).await.unwrap();

        // Tamper with the stored provider-origin metadata. The authenticated plaintext
        // still carries the real origin, so hydration must detect the mismatch.
        sqlx::query("UPDATE provider_context SET provider_instance_id = 'tampered' WHERE id = ?")
            .bind(record_id)
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
                anchor_id: "message-1:1".to_owned(),
            })
            .await
            .unwrap();

        let mut item = ProviderContextItem {
            retention_owner: ProviderContextAnchor {
                message_id: "message-1".to_owned(),
                message_seq: 1,
            },
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
        let later = EncryptedProviderContextRecord::encrypt(
            &item,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-1",
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt later compaction");
        later.insert(store.pool()).await.unwrap();

        // A different model keeps this a distinct native-compaction scope so the
        // active-native-window unique index is respected while still testing sort order.
        item.retention_owner = ProviderContextAnchor {
            message_id: "message-2".to_owned(),
            message_seq: 2,
        };
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
        let second_key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "message-2:2".to_owned(),
            })
            .await
            .unwrap();
        let earlier_reasoning = EncryptedProviderContextRecord::encrypt(
            &item,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-2",
            dummy_footprint(),
            &second_key,
            store.scope(),
        )
        .expect("encrypt earlier compaction");
        earlier_reasoning.insert(store.pool()).await.unwrap();

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
                message: assistant_message(openai_responses_origin_with_model("model-2")),
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
        let coverage_seq = |item: &ProviderContextItem| match &item.payload {
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
                anchor_id: "message-2:2".to_owned(),
            })
            .await
            .unwrap();

        let compaction_item = ProviderContextItem {
            retention_owner: ProviderContextAnchor {
                message_id: "message-2".to_owned(),
                message_seq: 2,
            },
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
            dummy_footprint(),
            &compaction_key,
            store.scope(),
        )
        .expect("encrypt compaction");
        compaction.insert(store.pool()).await.unwrap();

        // Anchored reasoning at seq 2.
        let reasoning = canonical_reasoning_record_with(&store, "message-2", 2, 0, 0).await;
        reasoning.insert(store.pool()).await.unwrap();

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
                &hydrated[0].payload,
                ProviderContextPayload::OpenAiCompactedWindow { .. }
            ),
            "native compaction with lower coverage seq must sort before anchored reasoning"
        );
        assert!(
            matches!(
                &hydrated[1].payload,
                ProviderContextPayload::EncryptedReasoning { .. }
            ),
            "anchored reasoning at higher message seq must sort after native compaction"
        );
    }

    #[tokio::test]
    async fn invalidate_converges_when_all_targets_already_deleted() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let record = reasoning_record(&store, "message-1", 7).await;
        let record_id = record.id().to_owned();
        record.insert(store.pool()).await.unwrap();

        // Simulate a previous successful apply (or external deletion) by removing the target.
        sqlx::query("DELETE FROM provider_context WHERE id = ?")
            .bind(&record_id)
            .execute(store.pool())
            .await
            .unwrap();

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
        .build_invalidate(None, vec![record_id.clone()])
        .expect("build invalidate intent");

        applier.prepare(&intent).await.unwrap();
        assert_eq!(
            applier.apply("invalidate-all-gone").await.unwrap(),
            ApplyOutcome::AlreadySatisfied
        );

        let remaining: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
                .bind(record_id)
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(remaining, 0);

        let reason: Option<String> = sqlx::query_scalar(
            "SELECT terminal_reason FROM provider_context_mutations WHERE mutation_id = ?",
        )
        .bind("invalidate-all-gone")
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(reason.as_deref(), Some("already_satisfied"));
    }

    #[tokio::test]
    async fn invalidate_converges_when_some_targets_already_deleted() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();
        seed_message(&store, "message-2", 9).await.unwrap();
        seed_owner_event_evidence(&store, &[("message-1", 7), ("message-2", 9)])
            .await
            .unwrap();

        let record1 = reasoning_record_with(&store, "message-1", 7, 0, 0).await;
        let record2 = reasoning_record_with(&store, "message-2", 9, 0, 0).await;
        let record1_id = record1.id().to_owned();
        let record2_id = record2.id().to_owned();
        record1.insert(store.pool()).await.unwrap();
        record2.insert(store.pool()).await.unwrap();

        // One target disappears before the intent is applied.
        sqlx::query("DELETE FROM provider_context WHERE id = ?")
            .bind(&record1_id)
            .execute(store.pool())
            .await
            .unwrap();

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
        .build_invalidate(None, vec![record1_id.clone(), record2_id.clone()])
        .expect("build invalidate intent");

        applier.prepare(&intent).await.unwrap();
        assert_eq!(
            applier.apply("invalidate-partial").await.unwrap(),
            ApplyOutcome::Applied
        );

        let remaining: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id IN (?, ?)")
                .bind(record1_id)
                .bind(record2_id)
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(remaining, 0);

        let reason: Option<String> = sqlx::query_scalar(
            "SELECT terminal_reason FROM provider_context_mutations WHERE mutation_id = ?",
        )
        .bind("invalidate-partial")
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(reason, None);
    }

    #[tokio::test]
    async fn hydrate_rejects_native_compaction_with_origin_message() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "message-1:7".to_owned(),
            })
            .await
            .unwrap();

        let mut item = ProviderContextItem {
            retention_owner: ProviderContextAnchor {
                message_id: "message-1".to_owned(),
                message_seq: 7,
            },
            origin_message: None,
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
        let mut record = EncryptedProviderContextRecord::encrypt(
            &item,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-1",
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt canonical native compaction");
        item.origin_message = Some(item.retention_owner.clone());
        record.message_id = Some(item.retention_owner.message_id.clone());
        record.message_seq = Some(item.retention_owner.message_seq);
        reencrypt_test_record(&store, &mut record, &item, &key);

        record.insert(store.pool()).await.unwrap();

        let error = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store
                .hydrate_provider_context(
                    &[ContextMessage::Persisted {
                        id: "message-1".to_owned(),
                        seq: 7,
                        message: assistant_message(openai_responses_origin()),
                    }],
                    &mut transaction,
                )
                .await
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
        seed_message(&store, "message-1", 7).await.unwrap();

        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "message-1:7".to_owned(),
            })
            .await
            .unwrap();

        let mut item = ProviderContextItem {
            retention_owner: ProviderContextAnchor {
                message_id: "message-1".to_owned(),
                message_seq: 7,
            },
            origin_message: None,
            wire_item_index: None,
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
        let mut record = EncryptedProviderContextRecord::encrypt(
            &item,
            "provider-instance-1",
            ApiProtocol::OpenAiResponses,
            "model-1",
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt canonical native compaction");
        item.wire_item_index = Some(0);
        record.id = provider_context_record_id(&item);
        record.wire_item_index = item.wire_item_index;
        reencrypt_test_record(&store, &mut record, &item, &key);

        record.insert(store.pool()).await.unwrap();

        let error = {
            let mut transaction = store.pool().begin().await.expect("begin test transaction");
            store
                .hydrate_provider_context(
                    &[ContextMessage::Persisted {
                        id: "message-1".to_owned(),
                        seq: 7,
                        message: assistant_message(openai_responses_origin()),
                    }],
                    &mut transaction,
                )
                .await
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
        let origin = reasoning_origin();
        let mut record = EncryptedProviderContextRecord::encrypt(
            &item,
            &origin.provider_instance_id,
            origin.protocol,
            &origin.model,
            reasoning_footprint(&item),
            &key,
            store.scope(),
        )
        .expect("encrypt canonical reasoning");
        item.wire_item_index = None;
        record.id = provider_context_record_id(&item);
        record.wire_item_index = None;
        reencrypt_test_record(&store, &mut record, &item, &key);

        record.insert(store.pool()).await.unwrap();

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
        seed_message(&store, "message-1", 7).await.unwrap();

        let key = store
            .provider_context_key(&ProviderContextKeyAnchor {
                conversation_id: store.scope().conversation_id.clone(),
                anchor_id: "message-1:7".to_owned(),
            })
            .await
            .unwrap();

        let mut item = reasoning_item("message-1", 7);
        let origin = reasoning_origin();
        let mut record = EncryptedProviderContextRecord::encrypt(
            &item,
            &origin.provider_instance_id,
            origin.protocol,
            &origin.model,
            reasoning_footprint(&item),
            &key,
            store.scope(),
        )
        .expect("encrypt canonical reasoning");
        item.origin_message = None;
        record.message_id = None;
        record.message_seq = None;
        reencrypt_test_record(&store, &mut record, &item, &key);

        // Direct insert with message_id/message_seq NULL and eviction_tokens=0 to satisfy the
        // schema CHECK while preserving the plaintext tamper.
        sqlx::query(
            "INSERT INTO provider_context(
                id, message_id, message_seq, wire_item_index, item_ordinal,
                idempotency_key, provider_instance_id, protocol, model, kind,
                coverage_through_seq, context_fingerprint, key_ref, ciphertext,
                eviction_tokens, eviction_estimator_version, created_at
             ) VALUES(?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, 0, 1, ?)",
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
        .bind(Utc::now().to_rfc3339())
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
            message.contains("encrypted reasoning origin message must match its retention owner"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_rejects_eviction_token_mismatch() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let record = canonical_reasoning_record_with(&store, "message-1", 7, 0, 0).await;
        let record_id = record.id().to_owned();
        record.insert(store.pool()).await.unwrap();

        sqlx::query("UPDATE provider_context SET eviction_tokens = ? WHERE id = ?")
            .bind(999i64)
            .bind(record_id)
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
            retention_owner: ProviderContextAnchor {
                message_id: "message-legacy".to_owned(),
                message_seq: 7,
            },
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
            footprint,
            &key,
            store.scope(),
        )
        .expect("encrypt legacy record")
        .insert(store.pool())
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
        assert_eq!(hydrated, vec![item]);
    }

    #[tokio::test]
    async fn hydrate_rejects_unsupported_eviction_estimator_version() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let record = canonical_reasoning_record_with(&store, "message-1", 7, 0, 0).await;
        let record_id = record.id().to_owned();
        record.insert(store.pool()).await.unwrap();

        sqlx::query("UPDATE provider_context SET eviction_estimator_version = ? WHERE id = ?")
            .bind(99i64)
            .bind(record_id)
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

        let record = canonical_reasoning_record_with(&store, "message-1", 7, 0, 0).await;
        record.insert(store.pool()).await.unwrap();

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
            message.contains("provider_origin does not match its retention-owner assistant origin"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn hydrate_anchor_error_uses_provider_context_record_id() {
        let store = store().await;
        seed_message(&store, "message-1", 7).await.unwrap();

        let record = canonical_reasoning_record_with(&store, "message-1", 7, 0, 0).await;
        let record_id = record.id().to_owned();
        record.insert(store.pool()).await.unwrap();

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
            message.contains(&record_id),
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

        let record = reasoning_record(&store, "message-1", 7).await;
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
                anchor_id: "message-1:1".to_owned(),
            })
            .await
            .unwrap();

        let item = ProviderContextItem {
            retention_owner: ProviderContextAnchor {
                message_id: "message-1".to_owned(),
                message_seq: 1,
            },
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
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt first window");
        first.insert(store.pool()).await.unwrap();

        let mut second_item = item.clone();
        second_item.ordinal = 2;
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
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt second window");
        let error = second
            .insert(store.pool())
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
        seed_owner_event_evidence(&store, &[("message-1", 7), ("message-2", 8)])
            .await
            .unwrap();

        let applier = ProviderContextMutationApplier::new(&store);
        let scope = store.scope().clone();

        // Prepare A first, then apply newer B. Recovery must not resurrect A
        // after B's active row is later invalidated.
        let a = reasoning_record(&store, "message-1", 7).await;
        let a_id = a.id().to_owned();
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

        let b = reasoning_record(&store, "message-2", 8).await;
        let b_id = b.id().to_owned();
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
        .build_invalidate(None, vec![b_id])
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
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM provider_context WHERE id = ?")
            .bind(a_id)
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

        let a = reasoning_record(&store, "message-1", 7).await;
        let a_id = a.id().to_owned();
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

        let b_item = reasoning_item_with("message-1", 7, 0, 1);
        let b = reasoning_record_with(&store, "message-1", 7, 0, 1).await;
        let intent_b = ProviderContextMutationBuilder::new(
            store
                .conversation_key(DataKeyPurpose::Mutation)
                .await
                .expect("mint mutation key"),
            scope,
            "replace-b".to_owned(),
        )
        .build_replace(
            Some(a_id),
            vec![], // missing the expected witness in invalidate_ids
            &b,
            &b_item,
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
            seed_message(&store, "request:with:colons", 4)
                .await
                .unwrap();
            seed_message(&store, "message-6", 6).await.unwrap();

            let item = native_compaction_item(anthropic, "request:with:colons", 4, 4);
            insert_native_compaction(&store, &item).await;
            let messages = vec![
                ContextMessage::Persisted {
                    id: "request:with:colons".to_owned(),
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
            assert_eq!(hydrated, vec![item]);
        }
    }

    #[tokio::test]
    async fn hydrate_native_compaction_rejects_tampered_idempotency_for_both_protocols() {
        for anthropic in [false, true] {
            let store = store().await;
            seed_message(&store, "request:with:colons", 4)
                .await
                .unwrap();
            seed_message(&store, "message-6", 6).await.unwrap();
            let item = native_compaction_item(anthropic, "request:with:colons", 4, 4);
            let id = insert_native_compaction(&store, &item).await;
            sqlx::query("UPDATE provider_context SET idempotency_key = 'tampered' WHERE id = ?")
                .bind(&id)
                .execute(store.pool())
                .await
                .expect("tamper stored idempotency key");

            let messages = vec![
                ContextMessage::Persisted {
                    id: "request:with:colons".to_owned(),
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
        seed_message(&store, "request-1", 4).await.unwrap();
        seed_message(&store, "message-6", 6).await.unwrap();
        let item = native_compaction_item(false, "request-1", 4, 4);
        insert_native_compaction(&store, &item).await;
        let messages = vec![
            ContextMessage::Persisted {
                id: "message-6".to_owned(),
                seq: 6,
                message: assistant_message(item.provider_origin.clone()),
            },
            ContextMessage::Persisted {
                id: "request-1".to_owned(),
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
                anchor_id: "message-3:3".to_owned(),
            })
            .await
            .unwrap();

        // Persisted messages at seq 1 and 3 (gaps are legal), but coverage claims seq 5.
        let item = ProviderContextItem {
            retention_owner: ProviderContextAnchor {
                message_id: "message-3".to_owned(),
                message_seq: 3,
            },
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
            dummy_footprint(),
            &key,
            store.scope(),
        )
        .expect("encrypt compaction");
        compaction.insert(store.pool()).await.unwrap();

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
    async fn invalidate_zeroes_ciphertext_before_delete_and_preserves_shared_key() {
        let store = store().await;
        let batch_id = seed_message_in_open_l0_batch(&store, "message-1", 7, 1_000_000)
            .await
            .unwrap();
        seed_owner_event_evidence(&store, &[("message-1", 7)])
            .await
            .unwrap();

        // Two reasoning records for the same anchor share a data key.
        let a = reasoning_record_with(&store, "message-1", 7, 0, 0).await;
        let a_id = a.id().to_owned();
        let b = reasoning_record_with(&store, "message-1", 7, 0, 1).await;
        let b_id = b.id().to_owned();
        a.insert(store.pool()).await.unwrap();
        b.insert(store.pool()).await.unwrap();

        // Replace invalidates pc-a and inserts pc-c. All three share the anchor key.
        let c = reasoning_record_with(&store, "message-1", 7, 0, 2).await;
        let c_id = c.id().to_owned();
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
            vec![a_id.clone()],
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
            .bind(&a_id)
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
                .bind(b_id)
                .bind(c_id)
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
        seed_owner_event_evidence(&store, &[("message-1", 7)])
            .await
            .unwrap();

        let old_record = reasoning_record_from_item(&store, &old_item).await;
        let old_id = old_record.id().to_owned();
        old_record.insert(store.pool()).await.unwrap();

        let new_record = reasoning_record_from_item(&store, &new_item).await;
        let new_id = new_record.id().to_owned();

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
        .build_replace(None, vec![old_id.clone()], &new_record, &new_item, 1, 1)
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
        .build_replace(Some(new_id), vec![old_id], &new_record, &new_item, 1, 1)
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
            reasoning_footprint(item),
            &key,
            store.scope(),
        )
        .expect("encrypt reasoning record")
    }
}
