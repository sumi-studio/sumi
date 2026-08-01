//! Conversation memory state, batch boundaries, and token accounting.

#![allow(dead_code)] // T20/T21 consume these foundations.

pub mod batch;
pub mod compactor;
pub mod context_assembler;
pub mod estimate;
pub mod overflow;
#[allow(dead_code)]
pub mod transform;

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::provider::types::{ContextMessage, PublicMessage};
use crate::store::{MemoryBatchState, MemoryJobKind, MemoryJobStatus, MemoryLayer};

use self::estimate::{TokenCalibration, estimate_public_messages};

/// Minimum public estimate before an open L0 batch is sealed at a user-turn
/// boundary.
pub const L0_BATCH_MIN: u64 = 5_000;
/// Maximum effective estimate tolerated by an open batch.  This is the
/// forced-seal fallback threshold, not a general seal boundary.
pub const L0_FORCED_SEAL_LIMIT: u64 = L0_BATCH_MIN * 2;
pub const L0_LIMIT: u64 = 40_000;
pub const L0_DROP_TO: u64 = 30_000;
pub const L1_LIMIT: u64 = 15_000;
pub const L1_DROP_TO: u64 = 11_000;
pub const L2_LIMIT: u64 = 10_000;

pub type BatchId = Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchState {
    Open,
    Sealed,
    Compacting,
    CompactFailed,
    Compacted,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct L0Batch {
    pub id: BatchId,
    pub batch_seq: u64,
    /// Durable transcript content with persisted identities.  Opaque provider
    /// context is accounted for by `eviction_footprint_tokens`, never stored
    /// in this vector.  Public projections are produced on demand for
    /// compaction and estimation.
    pub messages: Vec<ContextMessage>,
    pub est_tokens: u64,
    pub eviction_footprint_tokens: u64,
    pub state: BatchState,
}

impl L0Batch {
    pub fn new(
        messages: Vec<ContextMessage>,
        batch_seq: u64,
        eviction_footprint_tokens: u64,
        est_tokens: u64,
    ) -> Self {
        Self {
            id: Uuid::now_v7(),
            batch_seq,
            messages,
            est_tokens,
            eviction_footprint_tokens,
            state: BatchState::Open,
        }
    }

    pub fn public_messages(&self) -> Vec<PublicMessage> {
        self.messages
            .iter()
            .map(crate::memory::overflow::context_message_to_public)
            .collect()
    }
}

/// Runtime-only plaintext summary.
///
/// The field is private and this type deliberately implements neither
/// `Serialize` nor `Debug`, so routine persistence and structured logging
/// cannot expose its plaintext. `Zeroizing` clears the allocation on drop.
pub struct DecryptedMemorySummary(Zeroizing<String>);

impl DecryptedMemorySummary {
    pub(crate) fn new(summary: String) -> Self {
        Self(Zeroizing::new(summary))
    }

    pub fn expose(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn clone_zeroized(&self) -> Zeroizing<String> {
        self.0.clone()
    }
}

pub struct L1Entry {
    pub source_batch: BatchId,
    pub summary: DecryptedMemorySummary,
    pub est_tokens: u64,
    pub time_range: (DateTime<Utc>, DateTime<Utc>),
}

pub struct ConsolidatedMemory {
    pub summary: DecryptedMemorySummary,
    pub est_tokens: u64,
}

pub struct CompactResult {
    pub summary: DecryptedMemorySummary,
    pub est_tokens: u64,
    pub time_range: (DateTime<Utc>, DateTime<Utc>),
}

impl Clone for CompactResult {
    fn clone(&self) -> Self {
        Self {
            summary: DecryptedMemorySummary::new(self.summary.expose().to_owned()),
            est_tokens: self.est_tokens,
            time_range: self.time_range,
        }
    }
}

/// Authenticated, ciphertext-free memory summary produced by Store hydration.
///
/// This is intentionally distinct from durable Store records: it contains no
/// key reference, ciphertext, or redacted projection. Plaintext remains behind
/// [`DecryptedMemorySummary`]'s explicit access boundary.
pub(crate) struct HydratedMemorySummary {
    summary: DecryptedMemorySummary,
    est_tokens: u64,
    time_range: (DateTime<Utc>, DateTime<Utc>),
}

impl Clone for HydratedMemorySummary {
    fn clone(&self) -> Self {
        Self {
            summary: DecryptedMemorySummary::new(self.summary.expose().to_owned()),
            est_tokens: self.est_tokens,
            time_range: self.time_range,
        }
    }
}

impl HydratedMemorySummary {
    pub(crate) fn new(
        summary: String,
        est_tokens: u64,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Self> {
        if from > to {
            bail!("memory summary time range is inverted");
        }
        Ok(Self {
            summary: DecryptedMemorySummary::new(summary),
            est_tokens,
            time_range: (from, to),
        })
    }

    fn semantically_matches(&self, other: &Self) -> bool {
        self.est_tokens == other.est_tokens
            && self.time_range == other.time_range
            && self.summary.expose() == other.summary.expose()
    }

    #[cfg(test)]
    pub(crate) fn test_plaintext(&self) -> &str {
        self.summary.expose()
    }

    fn into_compact_result(self) -> CompactResult {
        CompactResult {
            summary: self.summary,
            est_tokens: self.est_tokens,
            time_range: self.time_range,
        }
    }

    fn into_l1_entry(self, source_batch: BatchId) -> L1Entry {
        L1Entry {
            source_batch,
            summary: self.summary,
            est_tokens: self.est_tokens,
            time_range: self.time_range,
        }
    }

    fn into_consolidated(self) -> ConsolidatedMemory {
        ConsolidatedMemory {
            summary: self.summary,
            est_tokens: self.est_tokens,
        }
    }
}

/// One authenticated durable batch stripped of Store-only crypto/redaction
/// metadata. It remains intermediate until the complete graph is validated.
#[derive(Clone)]
pub(crate) struct HydratedMemoryBatch {
    id: BatchId,
    layer: MemoryLayer,
    ord: u64,
    batch_seq: u64,
    version: u64,
    state: MemoryBatchState,
    est_tokens: u64,
    eviction_footprint_tokens: u64,
    summary: Option<HydratedMemorySummary>,
}

impl HydratedMemoryBatch {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: BatchId,
        layer: MemoryLayer,
        ord: u64,
        batch_seq: u64,
        version: u64,
        state: MemoryBatchState,
        est_tokens: u64,
        eviction_footprint_tokens: u64,
        summary: Option<HydratedMemorySummary>,
    ) -> Self {
        Self {
            id,
            layer,
            ord,
            batch_seq,
            version,
            state,
            est_tokens,
            eviction_footprint_tokens,
            summary,
        }
    }
}

/// An L0 membership joined by Store hydration to the exact authenticated
/// persisted transcript value. Synthetic messages are never admitted here.
#[derive(Clone)]
pub(crate) struct HydratedMemoryMembership {
    batch_id: BatchId,
    ord: u64,
    message: ContextMessage,
}

impl HydratedMemoryMembership {
    pub(crate) fn new(batch_id: BatchId, ord: u64, message: ContextMessage) -> Self {
        Self {
            batch_id,
            ord,
            message,
        }
    }
}

/// A durable memory job after Store has parsed IDs, numeric values, and any
/// encrypted result into runtime-only plaintext.
#[derive(Clone)]
pub(crate) struct HydratedMemoryJob {
    id: Uuid,
    kind: MemoryJobKind,
    batch_seq: u64,
    source_ids: Vec<BatchId>,
    source_versions: BTreeMap<BatchId, u64>,
    status: MemoryJobStatus,
    result: Option<HydratedMemorySummary>,
}

impl HydratedMemoryJob {
    pub(crate) fn new(
        id: Uuid,
        kind: MemoryJobKind,
        batch_seq: u64,
        source_ids: Vec<BatchId>,
        source_versions: BTreeMap<BatchId, u64>,
        status: MemoryJobStatus,
        result: Option<HydratedMemorySummary>,
    ) -> Self {
        Self {
            id,
            kind,
            batch_seq,
            source_ids,
            source_versions,
            status,
            result,
        }
    }
}

/// Typed durable FIFO cursor. Store rejects unknown kinds before construction.
#[derive(Clone, Copy)]
pub(crate) struct HydratedMemoryCursor {
    kind: MemoryJobKind,
    next_batch_seq: u64,
}

impl HydratedMemoryCursor {
    pub(crate) fn new(kind: MemoryJobKind, next_batch_seq: u64) -> Self {
        Self {
            kind,
            next_batch_seq,
        }
    }
}

/// Authenticated Store-to-runtime memory handoff.
///
/// Store verifies encryption, AAD, redaction, and transcript anchors. This
/// module owns structural validation and the only conversion into live
/// `ThreeLayerMemory`.
#[derive(Clone)]
pub(crate) struct HydratedMemoryRuntime {
    batches: Vec<HydratedMemoryBatch>,
    memberships: Vec<HydratedMemoryMembership>,
    jobs: Vec<HydratedMemoryJob>,
    cursors: Vec<HydratedMemoryCursor>,
    anchored_footprints: HashMap<String, u64>,
    calibration: TokenCalibration,
}

impl HydratedMemoryRuntime {
    pub(crate) fn new(
        batches: Vec<HydratedMemoryBatch>,
        memberships: Vec<HydratedMemoryMembership>,
        jobs: Vec<HydratedMemoryJob>,
        cursors: Vec<HydratedMemoryCursor>,
        anchored_footprints: HashMap<String, u64>,
    ) -> Self {
        Self {
            batches,
            memberships,
            jobs,
            cursors,
            anchored_footprints,
            calibration: TokenCalibration::default(),
        }
    }

    pub(crate) fn with_calibration(mut self, calibration: TokenCalibration) -> Self {
        self.calibration = calibration;
        self
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.batches.is_empty()
            && self.memberships.is_empty()
            && self.jobs.is_empty()
            && self.cursors.is_empty()
            && self.anchored_footprints.is_empty()
    }
}

impl fmt::Debug for HydratedMemoryRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HydratedMemoryRuntime")
            .field("batch_count", &self.batches.len())
            .field("membership_count", &self.memberships.len())
            .field("job_count", &self.jobs.len())
            .field("cursor_count", &self.cursors.len())
            .field("anchored_footprint_count", &self.anchored_footprints.len())
            .field("calibration_ratio", &self.calibration.ratio())
            .finish()
    }
}

/// In-memory representation of the three memory layers and the speculative
/// compaction shelf.  Durable persistence and promotion are intentionally
/// owned by later tasks.
pub struct ThreeLayerMemory {
    l2: ConsolidatedMemory,
    l1: VecDeque<L1Entry>,
    l0: VecDeque<L0Batch>,
    shelf: HashMap<BatchId, CompactResult>,
    calib: TokenCalibration,
    pending_apply: bool,
    next_l0_batch_seq: u64,
}

impl ThreeLayerMemory {
    pub fn new(l2: ConsolidatedMemory, calib: TokenCalibration) -> Self {
        Self {
            l2,
            l1: VecDeque::new(),
            l0: VecDeque::new(),
            shelf: HashMap::new(),
            calib,
            pending_apply: false,
            next_l0_batch_seq: 0,
        }
    }

    /// Convert Store's authenticated, ciphertext-free memory handoff into the
    /// exact live three-layer representation.
    ///
    /// Cold boot must never turn a malformed durable graph into empty memory.
    /// This boundary therefore validates batch identity/order, L0 membership,
    /// provider-context ownership, compaction source/target witnesses, job
    /// state, summaries, and FIFO cursors before exposing runtime state.
    pub(crate) fn from_hydrated(hydrated: HydratedMemoryRuntime) -> Result<Self> {
        let HydratedMemoryRuntime {
            batches,
            memberships,
            jobs,
            cursors,
            anchored_footprints,
            calibration,
        } = hydrated;

        let mut batches_by_id = HashMap::with_capacity(batches.len());
        let mut batches_by_layer_seq = HashMap::with_capacity(batches.len());
        for batch in batches {
            let id = batch.id;
            if batches_by_id.insert(id, batch).is_some() {
                bail!("hydrated memory contains duplicate batch UUID {id}");
            }
            let batch = batches_by_id
                .get(&id)
                .expect("batch was inserted into hydration map");
            let key = (layer_rank(batch.layer), batch.batch_seq);
            if batches_by_layer_seq.insert(key, id).is_some() {
                bail!(
                    "hydrated memory contains duplicate {:?} batch sequence {}",
                    batch.layer,
                    batch.batch_seq
                );
            }
            validate_batch_shape(batch)?;
        }
        validate_batch_order(&batches_by_id)?;

        let mut memberships_by_batch: HashMap<BatchId, Vec<HydratedMemoryMembership>> =
            HashMap::new();
        let mut member_message_ids = HashSet::new();
        for membership in memberships {
            let batch = batches_by_id.get(&membership.batch_id).ok_or_else(|| {
                anyhow!(
                    "hydrated memory membership references unknown batch {}",
                    membership.batch_id
                )
            })?;
            if batch.layer != MemoryLayer::L0 {
                bail!(
                    "hydrated memory membership for batch {} targets non-L0 layer",
                    membership.batch_id
                );
            }
            let ContextMessage::Persisted { id, .. } = &membership.message else {
                bail!(
                    "hydrated memory membership for batch {} contains a synthetic message",
                    membership.batch_id
                );
            };
            if matches!(
                &membership.message,
                ContextMessage::Persisted {
                    message: crate::provider::types::Message::Assistant(assistant),
                    ..
                } if assistant.stop_reason == crate::provider::types::StopReason::Error
            ) {
                bail!("Error assistant {id} must not belong to an L0 memory batch");
            }
            if !member_message_ids.insert(id.clone()) {
                bail!("hydrated memory message {id} belongs to more than one L0 batch");
            }
            memberships_by_batch
                .entry(membership.batch_id)
                .or_default()
                .push(membership);
        }
        for members in memberships_by_batch.values_mut() {
            members.sort_by_key(|membership| membership.ord);
            for (index, member) in members.iter().enumerate() {
                let expected = u64::try_from(index)
                    .map_err(|_| anyhow!("hydrated memory membership ordinal overflow"))?
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("hydrated memory membership ordinal overflow"))?;
                if member.ord != expected {
                    bail!(
                        "hydrated memory membership for batch {} has non-contiguous ordinal {} (expected {expected})",
                        member.batch_id,
                        member.ord
                    );
                }
            }
        }

        validate_jobs(&batches_by_id, &batches_by_layer_seq, &jobs)?;
        validate_batch_job_relationships(&batches_by_id, &batches_by_layer_seq, &jobs)?;
        validate_apply_cursors(&jobs, &cursors)?;

        let mut l0_ids = ordered_batch_ids(&batches_by_id, MemoryLayer::L0);
        let mut previous_message_seq = None;
        let mut open_l0_count = 0usize;
        let mut live_l0_message_ids = HashSet::new();
        for (position, batch_id) in l0_ids.iter().enumerate() {
            let batch = batches_by_id
                .get(batch_id)
                .expect("ordered batch id must resolve");
            let members = memberships_by_batch.get(batch_id);
            match batch.state {
                MemoryBatchState::Dropped => {
                    if members.is_some_and(|members| !members.is_empty()) {
                        bail!("dropped L0 batch {batch_id} retains message membership");
                    }
                    continue;
                }
                MemoryBatchState::Open
                | MemoryBatchState::Compacting
                | MemoryBatchState::CompactFailed
                | MemoryBatchState::Compacted => {}
                MemoryBatchState::Sealed => {
                    unreachable!("durable sealed batches were rejected before reconstruction")
                }
                MemoryBatchState::Promoted => {
                    bail!("L0 batch {batch_id} cannot be in promoted state");
                }
            }

            let members = members.ok_or_else(|| {
                anyhow!("live L0 batch {batch_id} is missing durable message membership")
            })?;
            if members.is_empty() {
                bail!("live L0 batch {batch_id} has empty durable message membership");
            }

            let mut public_messages = Vec::with_capacity(members.len());
            let mut expected_footprint = 0u64;
            for member in members {
                let ContextMessage::Persisted { id, seq, .. } = &member.message else {
                    unreachable!("synthetic members were rejected above");
                };
                if let Some(previous) = previous_message_seq
                    && *seq <= previous
                {
                    bail!(
                        "L0 message sequence is not strictly increasing at message {id}: {seq} after {previous}"
                    );
                }
                previous_message_seq = Some(*seq);
                live_l0_message_ids.insert(id.clone());
                expected_footprint = expected_footprint
                    .checked_add(anchored_footprints.get(id).copied().unwrap_or(0))
                    .ok_or_else(|| anyhow!("L0 footprint overflow for batch {batch_id}"))?;
                public_messages.push(crate::memory::overflow::context_message_to_public(
                    &member.message,
                ));
            }

            let expected_estimate = estimate_public_messages(&public_messages)
                .with_context(|| format!("failed to re-estimate hydrated L0 batch {batch_id}"))?;
            if batch.est_tokens != expected_estimate {
                bail!(
                    "hydrated L0 batch {batch_id} estimate {} does not match exact membership estimate {expected_estimate}",
                    batch.est_tokens
                );
            }
            if batch.eviction_footprint_tokens != expected_footprint {
                bail!(
                    "hydrated L0 batch {batch_id} footprint {} does not match anchored provider-context total {expected_footprint}",
                    batch.eviction_footprint_tokens
                );
            }
            if batch.state == MemoryBatchState::Open {
                open_l0_count = open_l0_count
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("open L0 batch count overflow"))?;
                if position + 1 != l0_ids.len() {
                    bail!("open L0 batch {batch_id} is not the newest L0 batch");
                }
            }
            validate_l0_job_state(batch, &jobs)?;
        }
        if open_l0_count > 1 {
            bail!("hydrated memory contains more than one open L0 batch");
        }
        for anchor in anchored_footprints.keys() {
            if !live_l0_message_ids.contains(anchor) {
                bail!("provider-context anchor {anchor} does not belong to a live L0 membership");
            }
        }

        // Completed CompactL0 jobs are the only durable results represented
        // by ThreeLayerMemory's speculative shelf.
        let mut shelf_by_source = HashMap::new();
        for job in jobs.iter().filter(|job| {
            job.kind == MemoryJobKind::CompactL0 && job.status == MemoryJobStatus::Completed
        }) {
            let source = *job
                .source_ids
                .first()
                .expect("validated CompactL0 job has one source");
            let result = job
                .result
                .as_ref()
                .ok_or_else(|| anyhow!("completed CompactL0 job {} has no result", job.id))?;
            if shelf_by_source.insert(source, result).is_some() {
                bail!("multiple completed L0 shelf results target source batch {source}");
            }
        }

        let visible_l1 = visible_source_batches(&jobs, MemoryJobKind::CompactL1);
        let visible_l2 = visible_source_batches(&jobs, MemoryJobKind::ConsolidateL2);

        // `L1Entry::source_batch` is the original L0 source identity, not the
        // durable L1 target UUID.
        let mut l1_sources = HashMap::new();
        for job in jobs.iter().filter(|job| {
            job.kind == MemoryJobKind::CompactL0 && job.status == MemoryJobStatus::Applied
        }) {
            let target = *batches_by_layer_seq
                .get(&(layer_rank(MemoryLayer::L1), job.batch_seq))
                .expect("validated applied CompactL0 target must resolve");
            let source = *job
                .source_ids
                .first()
                .expect("validated CompactL0 job has one source");
            if l1_sources.insert(target, source).is_some() {
                bail!("multiple applied CompactL0 jobs promote L1 batch {target}");
            }
        }

        let l1_ids = ordered_batch_ids(&batches_by_id, MemoryLayer::L1)
            .into_iter()
            .filter(|batch_id| {
                let batch = batches_by_id
                    .get(batch_id)
                    .expect("ordered batch id must resolve");
                batch.state == MemoryBatchState::Promoted
                    || (visible_l1.contains(batch_id)
                        && is_visible_summary_source_state(batch.state))
            })
            .collect::<Vec<_>>();
        let l2_ids = ordered_batch_ids(&batches_by_id, MemoryLayer::L2)
            .into_iter()
            .filter(|batch_id| {
                let batch = batches_by_id
                    .get(batch_id)
                    .expect("ordered batch id must resolve");
                batch.state == MemoryBatchState::Promoted
                    || (visible_l2.contains(batch_id)
                        && is_visible_summary_source_state(batch.state))
            })
            .collect::<Vec<_>>();
        let next_l0_batch_seq = batches_by_id
            .values()
            .filter(|batch| batch.layer == MemoryLayer::L0)
            .map(|batch| batch.batch_seq)
            .max()
            .map(|sequence| {
                sequence
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("hydrated L0 batch sequence overflow"))
            })
            .transpose()?
            .unwrap_or(0);

        let mut l0 = VecDeque::new();
        let mut l0_estimate = 0u64;
        let mut l0_footprint = 0u64;
        for batch_id in l0_ids.drain(..) {
            let batch = batches_by_id
                .remove(&batch_id)
                .expect("validated L0 batch must remain present");
            if batch.state == MemoryBatchState::Dropped {
                continue;
            }
            let messages = memberships_by_batch
                .remove(&batch_id)
                .expect("validated live L0 batch must have membership")
                .into_iter()
                .map(|member| member.message)
                .collect();
            l0_estimate = l0_estimate
                .checked_add(batch.est_tokens)
                .ok_or_else(|| anyhow!("hydrated L0 estimate overflow"))?;
            l0_footprint = l0_footprint
                .checked_add(batch.eviction_footprint_tokens)
                .ok_or_else(|| anyhow!("hydrated L0 footprint overflow"))?;
            l0.push_back(L0Batch {
                id: batch.id,
                batch_seq: batch.batch_seq,
                messages,
                est_tokens: batch.est_tokens,
                eviction_footprint_tokens: batch.eviction_footprint_tokens,
                state: l0_runtime_state(batch.state)?,
            });
        }

        let mut l1 = VecDeque::new();
        for batch_id in l1_ids {
            let batch = batches_by_id
                .remove(&batch_id)
                .expect("validated visible L1 batch must remain present");
            let summary = batch.summary.ok_or_else(|| {
                anyhow!("visible L1 batch {batch_id} is missing an authenticated summary")
            })?;
            let source_batch = l1_sources.remove(&batch.id).ok_or_else(|| {
                anyhow!("visible L1 batch {batch_id} has no applied CompactL0 source identity")
            })?;
            l1.push_back(summary.into_l1_entry(source_batch));
        }

        // Repeated CompactL1 applies append independently authenticated L2
        // rows. Until a ConsolidateL2 job is applied, all visible rows are
        // live memory. Fold them into the runtime's single L2 block in
        // durable ordinal order without dropping an older summary.
        let l2 = if l2_ids.is_empty() {
            ConsolidatedMemory {
                summary: DecryptedMemorySummary::new(String::new()),
                est_tokens: 0,
            }
        } else {
            let mut combined = Zeroizing::new(String::new());
            let mut total_est_tokens = 0_u64;
            for batch_id in l2_ids {
                let batch = batches_by_id
                    .remove(&batch_id)
                    .expect("validated visible L2 batch must remain present");
                let summary = batch.summary.ok_or_else(|| {
                    anyhow!("visible L2 batch {batch_id} is missing an authenticated summary")
                })?;
                if !combined.is_empty() {
                    combined.push_str("\n\n");
                }
                combined.push_str(summary.summary.expose());
                total_est_tokens = total_est_tokens
                    .checked_add(summary.est_tokens)
                    .ok_or_else(|| anyhow!("hydrated visible L2 estimate overflow"))?;
            }
            ConsolidatedMemory {
                summary: DecryptedMemorySummary(combined),
                est_tokens: total_est_tokens,
            }
        };

        let mut shelf = HashMap::new();
        for job in jobs {
            if job.kind != MemoryJobKind::CompactL0 || job.status != MemoryJobStatus::Completed {
                continue;
            }
            let source = job.source_ids[0];
            let result = job
                .result
                .expect("validated completed CompactL0 job must have a result")
                .into_compact_result();
            if shelf.insert(source, result).is_some() {
                bail!("multiple completed L0 shelf results target source batch {source}");
            }
        }

        let pending_apply = calibration
            .effective_tokens(l0_estimate, l0_footprint)
            .context("failed to derive hydrated L0 pending-apply state")?
            > L0_LIMIT;

        Ok(Self {
            l2,
            l1,
            l0,
            shelf,
            calib: calibration,
            pending_apply,
            next_l0_batch_seq,
        })
    }

    pub fn l2(&self) -> &ConsolidatedMemory {
        &self.l2
    }

    pub fn l1(&self) -> &VecDeque<L1Entry> {
        &self.l1
    }

    pub fn l0(&self) -> &VecDeque<L0Batch> {
        &self.l0
    }

    pub(crate) fn next_l0_batch_seq(&self) -> u64 {
        self.next_l0_batch_seq
    }

    pub(crate) fn l0_mut(&mut self) -> &mut VecDeque<L0Batch> {
        &mut self.l0
    }

    pub fn shelf(&self) -> &HashMap<BatchId, CompactResult> {
        &self.shelf
    }

    pub fn calibration(&self) -> TokenCalibration {
        self.calib
    }

    pub fn pending_apply(&self) -> bool {
        self.pending_apply
    }

    pub fn set_pending_apply(&mut self, pending: bool) {
        self.pending_apply = pending;
    }

    pub fn push_l0(&mut self, batch: L0Batch) {
        self.next_l0_batch_seq = self.next_l0_batch_seq.max(
            batch
                .batch_seq
                .checked_add(1)
                .expect("L0 batch sequence overflow"),
        );
        self.l0.push_back(batch);
    }

    /// Allocate a monotonic sequence independent from the current queue
    /// length, which can decrease during promotion.
    pub fn allocate_l0_batch_seq(&mut self) -> u64 {
        let sequence = self.next_l0_batch_seq;
        self.next_l0_batch_seq = self
            .next_l0_batch_seq
            .checked_add(1)
            .expect("L0 batch sequence overflow");
        sequence
    }

    pub fn store_compact_result(&mut self, batch_id: BatchId, result: CompactResult) {
        self.shelf.insert(batch_id, result);
    }

    /// Promote an L0 batch to L1 using a shelf summary.  The batch must not be
    /// open.  Provider-context footprint is cleared in the same logical
    /// transaction because L1 stores summaries, not the original messages.
    pub fn promote_l0_to_l1(&mut self, batch_id: BatchId) -> anyhow::Result<()> {
        let position = self
            .l0
            .iter()
            .position(|batch| batch.id == batch_id)
            .ok_or_else(|| anyhow::anyhow!("batch {batch_id} not found in L0"))?;
        let batch = self.l0.remove(position).unwrap();
        if batch.state == BatchState::Open {
            self.l0.insert(position, batch);
            anyhow::bail!("cannot promote an open L0 batch");
        }
        let result = match self.shelf.remove(&batch_id) {
            Some(result) => result,
            None => {
                // Removal is the first half of the logical promotion
                // transaction. Restore it before reporting a missing shelf
                // result so no transcript batch is lost.
                self.l0.insert(position, batch);
                anyhow::bail!("no compact result for batch {batch_id}");
            }
        };
        let time_range = result.time_range;
        self.l1.push_back(L1Entry {
            source_batch: batch_id,
            summary: result.summary,
            est_tokens: result.est_tokens,
            time_range,
        });
        Ok(())
    }

    /// Replace L1 entries with a compacted L2 summary.  Used when L1 overflows.
    pub fn compact_l1_to_l2(&mut self, result: CompactResult) {
        self.l1.clear();
        self.l2 = ConsolidatedMemory {
            summary: result.summary,
            est_tokens: result.est_tokens,
        };
    }

    /// Replace the L2 summary with a consolidated summary.  Used when L2
    /// itself grows beyond its limit.
    pub fn consolidate_l2(&mut self, result: CompactResult) {
        self.l2 = ConsolidatedMemory {
            summary: result.summary,
            est_tokens: result.est_tokens,
        };
    }

    pub fn l0_totals(&self) -> anyhow::Result<(u64, u64)> {
        self.l0
            .iter()
            .try_fold((0u64, 0u64), |(est, footprint), batch| {
                let new_est = est
                    .checked_add(batch.est_tokens)
                    .ok_or_else(|| anyhow::anyhow!("L0 estimate overflow"))?;
                let new_footprint = footprint
                    .checked_add(batch.eviction_footprint_tokens)
                    .ok_or_else(|| anyhow::anyhow!("L0 eviction footprint overflow"))?;
                Ok((new_est, new_footprint))
            })
    }

    pub fn l1_total(&self) -> anyhow::Result<u64> {
        self.l1.iter().try_fold(0u64, |total, entry| {
            total
                .checked_add(entry.est_tokens)
                .ok_or_else(|| anyhow::anyhow!("L1 estimate overflow"))
        })
    }

    pub fn effective_l0(&self) -> anyhow::Result<u64> {
        let (est, footprint) = self.l0_totals()?;
        Ok(self.calib.effective_tokens(est, footprint)?)
    }
}

fn layer_rank(layer: MemoryLayer) -> u8 {
    match layer {
        MemoryLayer::L0 => 0,
        MemoryLayer::L1 => 1,
        MemoryLayer::L2 => 2,
    }
}

fn job_kind_rank(kind: MemoryJobKind) -> u8 {
    match kind {
        MemoryJobKind::CompactL0 => 0,
        MemoryJobKind::CompactL1 => 1,
        MemoryJobKind::ConsolidateL2 => 2,
    }
}

fn target_layer(kind: MemoryJobKind) -> MemoryLayer {
    match kind {
        MemoryJobKind::CompactL0 => MemoryLayer::L1,
        MemoryJobKind::CompactL1 | MemoryJobKind::ConsolidateL2 => MemoryLayer::L2,
    }
}

fn source_layer(kind: MemoryJobKind) -> MemoryLayer {
    match kind {
        MemoryJobKind::CompactL0 => MemoryLayer::L0,
        MemoryJobKind::CompactL1 => MemoryLayer::L1,
        MemoryJobKind::ConsolidateL2 => MemoryLayer::L2,
    }
}

fn validate_batch_shape(batch: &HydratedMemoryBatch) -> Result<()> {
    if batch.ord == 0 {
        bail!("memory batch {} has zero ordinal", batch.id);
    }
    if batch.batch_seq == 0 {
        bail!("memory batch {} has zero sequence", batch.id);
    }
    if batch.state == MemoryBatchState::Sealed {
        bail!(
            "memory batch {} is durably sealed; sealing must atomically reserve a compaction job",
            batch.id
        );
    }
    if let Some(summary) = &batch.summary
        && summary.est_tokens != batch.est_tokens
    {
        bail!(
            "memory batch {} summary estimate {} does not match durable batch estimate {}",
            batch.id,
            summary.est_tokens,
            batch.est_tokens
        );
    }
    match batch.layer {
        MemoryLayer::L0 => {
            if batch.summary.is_some() {
                bail!("L0 batch {} must not carry a memory summary", batch.id);
            }
            if batch.state == MemoryBatchState::Promoted {
                bail!("L0 batch {} cannot be promoted", batch.id);
            }
        }
        MemoryLayer::L1 | MemoryLayer::L2 => {
            if batch.eviction_footprint_tokens != 0 {
                bail!(
                    "{:?} batch {} has a non-zero L0 eviction footprint",
                    batch.layer,
                    batch.id
                );
            }
            if batch.state == MemoryBatchState::Open {
                bail!(
                    "{:?} batch {} cannot be in open state",
                    batch.layer,
                    batch.id
                );
            }
            if matches!(
                batch.state,
                MemoryBatchState::Compacted
                    | MemoryBatchState::Promoted
                    | MemoryBatchState::Dropped
            ) && batch.summary.is_none()
            {
                bail!(
                    "{:?} batch {} in {} state is missing its summary",
                    batch.layer,
                    batch.id,
                    batch.state.as_str()
                );
            }
        }
    }
    Ok(())
}

fn validate_batch_order(batches: &HashMap<BatchId, HydratedMemoryBatch>) -> Result<()> {
    for layer in [MemoryLayer::L0, MemoryLayer::L1, MemoryLayer::L2] {
        let mut rows = batches
            .values()
            .filter(|batch| batch.layer == layer)
            .collect::<Vec<_>>();
        rows.sort_by_key(|batch| batch.ord);

        for (index, batch) in rows.iter().enumerate() {
            let expected = u64::try_from(index)
                .map_err(|_| anyhow!("hydrated {:?} batch ordinal overflow", layer))?
                .checked_add(1)
                .ok_or_else(|| anyhow!("hydrated {:?} batch ordinal overflow", layer))?;
            if batch.ord != expected {
                bail!(
                    "hydrated {:?} batch {} has ordinal {} (expected {expected})",
                    layer,
                    batch.id,
                    batch.ord
                );
            }
            if batch.batch_seq != expected {
                bail!(
                    "hydrated {:?} batch {} has sequence {} (expected {expected})",
                    layer,
                    batch.id,
                    batch.batch_seq
                );
            }
        }
    }
    Ok(())
}

fn ordered_batch_ids(
    batches: &HashMap<BatchId, HydratedMemoryBatch>,
    layer: MemoryLayer,
) -> Vec<BatchId> {
    let mut ids = batches
        .values()
        .filter(|batch| batch.layer == layer)
        .map(|batch| batch.id)
        .collect::<Vec<_>>();
    ids.sort_by_key(|id| {
        let batch = batches
            .get(id)
            .expect("batch id collected from this map must resolve");
        (batch.ord, batch.batch_seq)
    });
    ids
}

fn validate_jobs(
    batches: &HashMap<BatchId, HydratedMemoryBatch>,
    batches_by_layer_seq: &HashMap<(u8, u64), BatchId>,
    jobs: &[HydratedMemoryJob],
) -> Result<()> {
    let mut job_ids = HashSet::new();
    let mut target_jobs = HashSet::new();
    let mut kind_sequences = HashSet::new();
    let mut source_owner = HashMap::new();
    for job in jobs
        .iter()
        .filter(|job| job.status != MemoryJobStatus::Discarded)
    {
        for source_id in &job.source_ids {
            if source_owner.insert(*source_id, job).is_some() {
                bail!(
                    "hydrated memory batch {source_id} is owned as a source by more than one non-discarded job"
                );
            }
        }
    }

    for job in jobs {
        if !job_ids.insert(job.id) {
            bail!("hydrated memory contains duplicate job UUID {}", job.id);
        }
        if job.batch_seq == 0 {
            bail!("hydrated memory job {} has zero sequence", job.id);
        }
        if !kind_sequences.insert((job_kind_rank(job.kind), job.batch_seq)) {
            bail!(
                "hydrated memory contains duplicate {} job sequence {}",
                job.kind.as_str(),
                job.batch_seq
            );
        }
        if job.source_ids.is_empty() {
            bail!("hydrated memory job {} has no source batches", job.id);
        }
        if job.kind == MemoryJobKind::CompactL0 && job.source_ids.len() != 1 {
            bail!(
                "hydrated CompactL0 job {} must have exactly one source batch",
                job.id
            );
        }

        let mut unique_sources = HashSet::new();
        for source_id in &job.source_ids {
            if !unique_sources.insert(*source_id) {
                bail!(
                    "hydrated memory job {} repeats source batch {source_id}",
                    job.id
                );
            }
        }

        let target_id = *batches_by_layer_seq
            .get(&(layer_rank(target_layer(job.kind)), job.batch_seq))
            .ok_or_else(|| {
                anyhow!(
                    "hydrated memory job {} has no {:?} target at batch sequence {}",
                    job.id,
                    target_layer(job.kind),
                    job.batch_seq
                )
            })?;
        if !target_jobs.insert(target_id) {
            bail!("hydrated memory has more than one job targeting batch {target_id}");
        }
        if unique_sources.contains(&target_id) {
            bail!(
                "hydrated memory job {} uses target batch {target_id} as a source",
                job.id
            );
        }
        let target = batches
            .get(&target_id)
            .expect("target id resolved from batch map");

        for source_id in &job.source_ids {
            let source = batches.get(source_id).ok_or_else(|| {
                anyhow!(
                    "hydrated memory job {} references missing source batch {source_id}",
                    job.id
                )
            })?;
            let expected_layer = source_layer(job.kind);
            if source.layer != expected_layer {
                bail!(
                    "hydrated memory job {} source {source_id} is {:?}, expected {:?}",
                    job.id,
                    source.layer,
                    expected_layer
                );
            }
        }

        let mut expected_versions = job.source_ids.iter().copied().collect::<HashSet<_>>();
        expected_versions.insert(target_id);
        let version_ids = job.source_versions.keys().copied().collect::<HashSet<_>>();
        if version_ids != expected_versions {
            bail!(
                "hydrated memory job {} source-version witnesses do not exactly cover its source and target batches",
                job.id
            );
        }

        let (source_state, target_state, requires_result) = match job.status {
            MemoryJobStatus::Pending | MemoryJobStatus::Running => (
                MemoryBatchState::Compacting,
                MemoryBatchState::Compacting,
                false,
            ),
            MemoryJobStatus::Completed => (
                MemoryBatchState::Compacted,
                MemoryBatchState::Compacted,
                true,
            ),
            MemoryJobStatus::Applied => {
                (MemoryBatchState::Dropped, MemoryBatchState::Promoted, true)
            }
            MemoryJobStatus::Failed => (
                MemoryBatchState::CompactFailed,
                MemoryBatchState::CompactFailed,
                false,
            ),
            MemoryJobStatus::Discarded => (
                MemoryBatchState::CompactFailed,
                MemoryBatchState::CompactFailed,
                true,
            ),
        };

        if requires_result {
            let result = job.result.as_ref().ok_or_else(|| {
                anyhow!(
                    "hydrated memory job {} is missing its completed result",
                    job.id
                )
            })?;
            let target_summary = target.summary.as_ref().ok_or_else(|| {
                anyhow!(
                    "hydrated memory job {} target {target_id} is missing its completed summary",
                    job.id
                )
            })?;
            if !result.semantically_matches(target_summary) {
                bail!(
                    "hydrated memory job {} result does not match target batch {target_id} summary",
                    job.id
                );
            }
        } else if job.result.is_some() {
            bail!(
                "hydrated memory {} job {} retains a result without completion",
                job.status.as_str(),
                job.id
            );
        }

        // Discard preserves the exact original graph and authenticated result,
        // but its version witnesses intentionally describe the rejected
        // snapshot rather than the current one.
        if job.status == MemoryJobStatus::Discarded {
            continue;
        }

        for source_id in &job.source_ids {
            let source = batches
                .get(source_id)
                .expect("job source was validated above");
            if job.source_versions.get(source_id) != Some(&source.version) {
                bail!(
                    "hydrated memory job {} source witness for batch {source_id} does not match current version",
                    job.id
                );
            }
            // A failed stale job can witness a source already advanced by a
            // newer owner. Other statuses own the exact source state.
            if job.status != MemoryJobStatus::Failed && source.state != source_state {
                bail!(
                    "hydrated memory job {} source {source_id} is {}, expected {} for {}",
                    job.id,
                    source.state.as_str(),
                    source_state.as_str(),
                    job.status.as_str()
                );
            }
        }

        if let Some(successor) = source_owner.get(&target_id).copied() {
            if job.status != MemoryJobStatus::Applied {
                bail!(
                    "hydrated {} job {} target {target_id} is reused before the predecessor was applied",
                    job.status.as_str(),
                    job.id
                );
            }
            let predecessor_version = job
                .source_versions
                .get(&target_id)
                .copied()
                .expect("exact target witness was validated above");
            let successor_version = successor
                .source_versions
                .get(&target_id)
                .copied()
                .expect("successor exact source witness was validated above");
            if successor_version <= predecessor_version {
                bail!(
                    "hydrated job {} does not advance target {target_id} beyond predecessor job {}",
                    successor.id,
                    job.id
                );
            }
        } else {
            if job.source_versions.get(&target_id) != Some(&target.version) {
                bail!(
                    "hydrated memory job {} target witness for batch {target_id} does not match current version",
                    job.id
                );
            }
            if target.state != target_state {
                bail!(
                    "hydrated memory job {} target {target_id} is {}, expected {} for {}",
                    job.id,
                    target.state.as_str(),
                    target_state.as_str(),
                    job.status.as_str()
                );
            }
        }
    }
    Ok(())
}

fn validate_batch_job_relationships(
    batches: &HashMap<BatchId, HydratedMemoryBatch>,
    batches_by_layer_seq: &HashMap<(u8, u64), BatchId>,
    jobs: &[HydratedMemoryJob],
) -> Result<()> {
    let target_for = |job: &HydratedMemoryJob| {
        batches_by_layer_seq
            .get(&(layer_rank(target_layer(job.kind)), job.batch_seq))
            .copied()
    };
    let is_target = |batch_id: BatchId, kind: MemoryJobKind, status: MemoryJobStatus| {
        jobs.iter().any(|job| {
            job.kind == kind
                && job.status == status
                && target_for(job).is_some_and(|target| target == batch_id)
        })
    };
    let is_source = |batch_id: BatchId, kind: MemoryJobKind, status: MemoryJobStatus| {
        jobs.iter().any(|job| {
            job.kind == kind && job.status == status && job.source_ids.contains(&batch_id)
        })
    };

    // L1/L2 sources compact from authenticated plaintext summaries, never a
    // redacted projection. Discarded jobs contribute no live source state.
    for job in jobs
        .iter()
        .filter(|job| job.status != MemoryJobStatus::Discarded)
    {
        if source_layer(job.kind) == MemoryLayer::L0 {
            continue;
        }
        for source_id in &job.source_ids {
            let source = batches
                .get(source_id)
                .expect("non-discarded job source was validated");
            if source.summary.is_none() {
                bail!(
                    "hydrated {} job {} source {source_id} is missing its authenticated summary",
                    job.kind.as_str(),
                    job.id
                );
            }
        }
    }

    for batch in batches.values() {
        match (batch.layer, batch.state) {
            (MemoryLayer::L0, MemoryBatchState::Dropped)
                if !is_source(batch.id, MemoryJobKind::CompactL0, MemoryJobStatus::Applied) =>
            {
                bail!(
                    "dropped L0 batch {} has no applied CompactL0 source job",
                    batch.id
                );
            }
            (MemoryLayer::L1, MemoryBatchState::Promoted)
                if !is_target(batch.id, MemoryJobKind::CompactL0, MemoryJobStatus::Applied) =>
            {
                bail!(
                    "promoted L1 batch {} has no applied CompactL0 target job",
                    batch.id
                );
            }
            (MemoryLayer::L2, MemoryBatchState::Promoted)
                if !is_target(batch.id, MemoryJobKind::CompactL1, MemoryJobStatus::Applied)
                    && !is_target(
                        batch.id,
                        MemoryJobKind::ConsolidateL2,
                        MemoryJobStatus::Applied,
                    ) =>
            {
                bail!(
                    "promoted L2 batch {} has no applied compaction target job",
                    batch.id
                );
            }
            (MemoryLayer::L1, MemoryBatchState::Dropped)
                if !is_source(batch.id, MemoryJobKind::CompactL1, MemoryJobStatus::Applied) =>
            {
                bail!(
                    "dropped L1 batch {} has no applied CompactL1 source job",
                    batch.id
                );
            }
            (MemoryLayer::L2, MemoryBatchState::Dropped)
                if !is_source(
                    batch.id,
                    MemoryJobKind::ConsolidateL2,
                    MemoryJobStatus::Applied,
                ) =>
            {
                bail!(
                    "dropped L2 batch {} has no applied ConsolidateL2 source job",
                    batch.id
                );
            }
            (MemoryLayer::L1 | MemoryLayer::L2, MemoryBatchState::Compacting)
            | (MemoryLayer::L1 | MemoryLayer::L2, MemoryBatchState::CompactFailed)
            | (MemoryLayer::L1 | MemoryLayer::L2, MemoryBatchState::Compacted) => {
                validate_summary_batch_state_owner(batch, &is_target, &is_source)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_summary_batch_state_owner(
    batch: &HydratedMemoryBatch,
    is_target: &impl Fn(BatchId, MemoryJobKind, MemoryJobStatus) -> bool,
    is_source: &impl Fn(BatchId, MemoryJobKind, MemoryJobStatus) -> bool,
) -> Result<()> {
    let statuses = match batch.state {
        MemoryBatchState::Compacting => &[MemoryJobStatus::Pending, MemoryJobStatus::Running][..],
        MemoryBatchState::CompactFailed => {
            &[MemoryJobStatus::Failed, MemoryJobStatus::Discarded][..]
        }
        MemoryBatchState::Compacted => &[MemoryJobStatus::Completed][..],
        _ => unreachable!("only transitional summary states call this validator"),
    };
    let target_kinds: &[MemoryJobKind] = match batch.layer {
        MemoryLayer::L1 => &[MemoryJobKind::CompactL0],
        MemoryLayer::L2 => &[MemoryJobKind::CompactL1, MemoryJobKind::ConsolidateL2],
        MemoryLayer::L0 => unreachable!("L0 is not a summary batch"),
    };
    let source_kind = match batch.layer {
        MemoryLayer::L1 => MemoryJobKind::CompactL1,
        MemoryLayer::L2 => MemoryJobKind::ConsolidateL2,
        MemoryLayer::L0 => unreachable!("L0 is not a summary batch"),
    };

    let target_owned = target_kinds.iter().copied().any(|kind| {
        statuses
            .iter()
            .copied()
            .any(|status| is_target(batch.id, kind, status))
    });
    let source_owned = statuses
        .iter()
        .copied()
        .any(|status| is_source(batch.id, source_kind, status));
    if !target_owned && !source_owned {
        bail!(
            "{} {:?} batch {} has no matching compaction job",
            batch.state.as_str(),
            batch.layer,
            batch.id
        );
    }
    Ok(())
}

fn validate_apply_cursors(
    jobs: &[HydratedMemoryJob],
    cursors: &[HydratedMemoryCursor],
) -> Result<()> {
    let mut seen = Vec::new();
    for cursor in cursors {
        if seen.contains(&cursor.kind) {
            bail!(
                "hydrated memory contains duplicate {} apply cursor",
                cursor.kind.as_str()
            );
        }
        seen.push(cursor.kind);
        let mut kind_jobs = jobs
            .iter()
            .filter(|job| job.kind == cursor.kind)
            .collect::<Vec<_>>();
        if kind_jobs.is_empty() {
            bail!(
                "hydrated memory has {} apply cursor without any jobs",
                cursor.kind.as_str()
            );
        }
        kind_jobs.sort_by_key(|job| job.batch_seq);
        let first = kind_jobs
            .first()
            .expect("non-empty kind jobs has first")
            .batch_seq;
        let last = kind_jobs
            .last()
            .expect("non-empty kind jobs has last")
            .batch_seq;
        let after_last = last
            .checked_add(1)
            .ok_or_else(|| anyhow!("hydrated {} cursor overflow", cursor.kind.as_str()))?;
        if cursor.next_batch_seq < first || cursor.next_batch_seq > after_last {
            bail!(
                "hydrated {} cursor {} is outside durable job range {first}..={after_last}",
                cursor.kind.as_str(),
                cursor.next_batch_seq
            );
        }
        for job in kind_jobs {
            if job.batch_seq < cursor.next_batch_seq
                && !matches!(
                    job.status,
                    MemoryJobStatus::Applied | MemoryJobStatus::Discarded
                )
            {
                bail!(
                    "hydrated {} cursor skips {} job {} at sequence {}",
                    cursor.kind.as_str(),
                    job.status.as_str(),
                    job.id,
                    job.batch_seq
                );
            }
        }
    }

    for job in jobs.iter().filter(|job| {
        matches!(
            job.status,
            MemoryJobStatus::Applied | MemoryJobStatus::Discarded
        )
    }) {
        let cursor = cursors
            .iter()
            .find(|cursor| cursor.kind == job.kind)
            .ok_or_else(|| {
                anyhow!(
                    "{} {} job {} has no durable apply cursor",
                    job.status.as_str(),
                    job.kind.as_str(),
                    job.id
                )
            })?;
        if cursor.next_batch_seq <= job.batch_seq {
            bail!(
                "{} apply cursor {} has not advanced past {} job {} at sequence {}",
                job.kind.as_str(),
                cursor.next_batch_seq,
                job.status.as_str(),
                job.id,
                job.batch_seq
            );
        }
    }
    Ok(())
}

fn validate_l0_job_state(batch: &HydratedMemoryBatch, jobs: &[HydratedMemoryJob]) -> Result<()> {
    let matching = |statuses: &[MemoryJobStatus]| {
        jobs.iter().any(|job| {
            job.kind == MemoryJobKind::CompactL0
                && statuses.contains(&job.status)
                && job.source_ids.contains(&batch.id)
        })
    };
    let expected = match batch.state {
        MemoryBatchState::Compacting => {
            Some(&[MemoryJobStatus::Pending, MemoryJobStatus::Running][..])
        }
        MemoryBatchState::CompactFailed => Some(&[MemoryJobStatus::Failed][..]),
        MemoryBatchState::Compacted => Some(&[MemoryJobStatus::Completed][..]),
        MemoryBatchState::Open => None,
        MemoryBatchState::Sealed => {
            unreachable!("durable sealed batches were rejected before L0 validation")
        }
        MemoryBatchState::Dropped => Some(&[MemoryJobStatus::Applied][..]),
        MemoryBatchState::Promoted => None,
    };
    if let Some(statuses) = expected
        && !matching(statuses)
    {
        bail!(
            "hydrated L0 batch {} is {} without a matching CompactL0 job",
            batch.id,
            batch.state.as_str()
        );
    }
    Ok(())
}

fn visible_source_batches(jobs: &[HydratedMemoryJob], kind: MemoryJobKind) -> HashSet<BatchId> {
    jobs.iter()
        .filter(|job| {
            job.kind == kind
                && matches!(
                    job.status,
                    MemoryJobStatus::Pending
                        | MemoryJobStatus::Running
                        | MemoryJobStatus::Completed
                        | MemoryJobStatus::Failed
                )
        })
        .flat_map(|job| job.source_ids.iter().copied())
        .collect()
}

fn is_visible_summary_source_state(state: MemoryBatchState) -> bool {
    matches!(
        state,
        MemoryBatchState::Compacting
            | MemoryBatchState::CompactFailed
            | MemoryBatchState::Compacted
            | MemoryBatchState::Promoted
    )
}

fn l0_runtime_state(state: MemoryBatchState) -> Result<BatchState> {
    match state {
        MemoryBatchState::Open => Ok(BatchState::Open),
        MemoryBatchState::Compacting => Ok(BatchState::Compacting),
        MemoryBatchState::CompactFailed => Ok(BatchState::CompactFailed),
        MemoryBatchState::Compacted => Ok(BatchState::Compacted),
        MemoryBatchState::Sealed => {
            bail!("durable sealed L0 batch cannot enter runtime")
        }
        MemoryBatchState::Promoted | MemoryBatchState::Dropped => {
            bail!("non-live L0 state {} cannot enter runtime", state.as_str())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::types::{
        ApiProtocol, AssistantMessage, Message, ProviderOrigin, StopReason, Usage, UserContent,
        UserMessage,
    };

    fn persisted_user(id: &str, seq: u64, text: &str) -> ContextMessage {
        ContextMessage::Persisted {
            id: id.to_owned(),
            seq,
            message: Message::User(UserMessage {
                content: vec![UserContent::Text {
                    text: text.to_owned(),
                }],
                timestamp: Utc::now(),
            }),
        }
    }

    fn persisted_error(id: &str, seq: u64) -> ContextMessage {
        ContextMessage::Persisted {
            id: id.to_owned(),
            seq,
            message: Message::Assistant(AssistantMessage {
                content: Vec::new(),
                model: "test-model".to_owned(),
                provider: "test-provider".to_owned(),
                origin: ProviderOrigin {
                    provider_instance_id: "test-provider-instance".to_owned(),
                    protocol: ApiProtocol::OpenAiResponses,
                    model: "test-model".to_owned(),
                },
                usage: Usage::default(),
                stop_reason: StopReason::Error,
                error_message: Some("retryable failure".to_owned()),
                provider_code: None,
                interrupted: false,
                timestamp: Utc::now(),
            }),
        }
    }

    fn message_estimate(message: &ContextMessage) -> u64 {
        estimate_public_messages(&[crate::memory::overflow::context_message_to_public(message)])
            .expect("estimate test message")
    }

    fn hydrated_summary(text: &str, est_tokens: u64) -> HydratedMemorySummary {
        HydratedMemorySummary::new(
            text.to_owned(),
            est_tokens,
            "2024-01-01T00:00:00Z".parse().expect("test timestamp"),
            "2024-01-02T00:00:00Z".parse().expect("test timestamp"),
        )
        .expect("valid test summary")
    }

    #[test]
    fn reconstructs_authenticated_open_l0_with_exact_anchor_footprint() {
        let batch_id = Uuid::now_v7();
        let message = persisted_user("live-message", 1, "remember this");
        let estimate = message_estimate(&message);
        let hydrated = HydratedMemoryRuntime::new(
            vec![HydratedMemoryBatch::new(
                batch_id,
                MemoryLayer::L0,
                1,
                1,
                0,
                MemoryBatchState::Open,
                estimate,
                7,
                None,
            )],
            vec![HydratedMemoryMembership::new(batch_id, 1, message)],
            vec![],
            vec![],
            HashMap::from([("live-message".to_owned(), 7)]),
        );

        let memory = ThreeLayerMemory::from_hydrated(hydrated).expect("valid handoff reconstructs");
        assert_eq!(memory.l0().len(), 1);
        assert_eq!(memory.l0()[0].id, batch_id);
        assert_eq!(memory.l0()[0].eviction_footprint_tokens, 7);
        assert_eq!(memory.next_l0_batch_seq(), 2);
    }

    #[test]
    fn rejects_every_durable_sealed_batch() {
        let batch_id = Uuid::now_v7();
        let message = persisted_user("sealed-message", 1, "must be atomic");
        let estimate = message_estimate(&message);
        let hydrated = HydratedMemoryRuntime::new(
            vec![HydratedMemoryBatch::new(
                batch_id,
                MemoryLayer::L0,
                1,
                1,
                1,
                MemoryBatchState::Sealed,
                estimate,
                0,
                None,
            )],
            vec![HydratedMemoryMembership::new(batch_id, 1, message)],
            vec![],
            vec![],
            HashMap::new(),
        );

        let error = ThreeLayerMemory::from_hydrated(hydrated)
            .err()
            .expect("a durable sealed snapshot is not producer-reachable");
        assert!(error.to_string().contains("durably sealed"), "{error:#}");
    }

    #[test]
    fn rejects_provider_context_anchor_outside_live_l0() {
        let batch_id = Uuid::now_v7();
        let message = persisted_user("live-message", 1, "live");
        let estimate = message_estimate(&message);
        let hydrated = HydratedMemoryRuntime::new(
            vec![HydratedMemoryBatch::new(
                batch_id,
                MemoryLayer::L0,
                1,
                1,
                0,
                MemoryBatchState::Open,
                estimate,
                0,
                None,
            )],
            vec![HydratedMemoryMembership::new(batch_id, 1, message)],
            vec![],
            vec![],
            HashMap::from([("persisted-but-not-live".to_owned(), 11)]),
        );

        let error = ThreeLayerMemory::from_hydrated(hydrated)
            .err()
            .expect("provider context cannot survive outside live L0");
        assert!(
            error.to_string().contains("does not belong to a live L0"),
            "{error:#}"
        );
    }

    #[test]
    fn rejects_error_assistant_membership_even_with_zero_footprint() {
        let batch_id = Uuid::now_v7();
        let message = persisted_error("error-assistant", 1);
        let estimate = message_estimate(&message);
        let hydrated = HydratedMemoryRuntime::new(
            vec![HydratedMemoryBatch::new(
                batch_id,
                MemoryLayer::L0,
                1,
                1,
                0,
                MemoryBatchState::Open,
                estimate,
                0,
                None,
            )],
            vec![HydratedMemoryMembership::new(batch_id, 1, message)],
            vec![],
            vec![],
            HashMap::new(),
        );

        let error = ThreeLayerMemory::from_hydrated(hydrated)
            .err()
            .expect("Error assistant membership must fail closed");
        assert!(
            error
                .to_string()
                .contains("Error assistant error-assistant must not belong to an L0"),
            "{error:#}"
        );
    }

    #[test]
    fn failed_job_holds_fifo_cursor() {
        let failed = HydratedMemoryJob::new(
            Uuid::now_v7(),
            MemoryJobKind::CompactL0,
            1,
            vec![Uuid::now_v7()],
            BTreeMap::new(),
            MemoryJobStatus::Failed,
            None,
        );
        let error = validate_apply_cursors(
            &[failed],
            &[HydratedMemoryCursor::new(MemoryJobKind::CompactL0, 2)],
        )
        .expect_err("failed work must never be passed by the FIFO cursor");
        assert!(error.to_string().contains("skips failed job"), "{error:#}");
    }

    #[test]
    fn only_applied_and_discarded_jobs_may_be_behind_cursor() {
        let applied = HydratedMemoryJob::new(
            Uuid::now_v7(),
            MemoryJobKind::CompactL0,
            1,
            vec![Uuid::now_v7()],
            BTreeMap::new(),
            MemoryJobStatus::Applied,
            Some(hydrated_summary("applied", 1)),
        );
        let discarded = HydratedMemoryJob::new(
            Uuid::now_v7(),
            MemoryJobKind::CompactL0,
            2,
            vec![Uuid::now_v7()],
            BTreeMap::new(),
            MemoryJobStatus::Discarded,
            Some(hydrated_summary("discarded", 1)),
        );
        validate_apply_cursors(
            &[applied, discarded],
            &[HydratedMemoryCursor::new(MemoryJobKind::CompactL0, 3)],
        )
        .expect("applied and discarded terminal holes may be passed");
    }

    #[test]
    fn non_discarded_job_requires_exact_source_and_target_witnesses() {
        let source_id = Uuid::now_v7();
        let target_id = Uuid::now_v7();
        let source = HydratedMemoryBatch::new(
            source_id,
            MemoryLayer::L0,
            1,
            1,
            4,
            MemoryBatchState::CompactFailed,
            1,
            0,
            None,
        );
        let target = HydratedMemoryBatch::new(
            target_id,
            MemoryLayer::L1,
            1,
            1,
            3,
            MemoryBatchState::CompactFailed,
            0,
            0,
            None,
        );
        let batches = HashMap::from([(source_id, source), (target_id, target)]);
        let by_layer_seq = HashMap::from([((layer_rank(MemoryLayer::L1), 1), target_id)]);
        let failed = HydratedMemoryJob::new(
            Uuid::now_v7(),
            MemoryJobKind::CompactL0,
            1,
            vec![source_id],
            BTreeMap::from([(target_id, 3)]),
            MemoryJobStatus::Failed,
            None,
        );

        let error = validate_jobs(&batches, &by_layer_seq, &[failed])
            .expect_err("failed is not discard and must retain exact witnesses");
        assert!(
            error
                .to_string()
                .contains("exactly cover its source and target"),
            "{error:#}"
        );
    }

    #[test]
    fn discarded_job_rejects_missing_original_graph() {
        let source_id = Uuid::now_v7();
        let obsolete_target_id = Uuid::now_v7();
        let discarded = HydratedMemoryJob::new(
            Uuid::now_v7(),
            MemoryJobKind::CompactL0,
            1,
            vec![source_id],
            BTreeMap::from([(source_id, 4), (obsolete_target_id, 7)]),
            MemoryJobStatus::Discarded,
            Some(hydrated_summary("obsolete", 1)),
        );
        let hydrated = HydratedMemoryRuntime::new(
            vec![],
            vec![],
            vec![discarded],
            vec![HydratedMemoryCursor::new(MemoryJobKind::CompactL0, 2)],
            HashMap::new(),
        );

        let error = match ThreeLayerMemory::from_hydrated(hydrated) {
            Ok(_) => panic!("discarded work must retain its exact authenticated graph"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("has no L1 target"), "{error:#}");
    }

    #[test]
    fn discarded_job_may_retain_versions_from_the_rejected_graph() {
        let source_id = Uuid::now_v7();
        let target_id = Uuid::now_v7();
        let batches = HashMap::from([
            (
                source_id,
                HydratedMemoryBatch::new(
                    source_id,
                    MemoryLayer::L0,
                    1,
                    1,
                    5,
                    MemoryBatchState::Open,
                    1,
                    0,
                    None,
                ),
            ),
            (
                target_id,
                HydratedMemoryBatch::new(
                    target_id,
                    MemoryLayer::L1,
                    1,
                    1,
                    8,
                    MemoryBatchState::CompactFailed,
                    0,
                    0,
                    Some(hydrated_summary("obsolete", 1)),
                ),
            ),
        ]);
        let by_layer_seq = HashMap::from([((layer_rank(MemoryLayer::L1), 1), target_id)]);
        let discarded = HydratedMemoryJob::new(
            Uuid::now_v7(),
            MemoryJobKind::CompactL0,
            1,
            vec![source_id],
            BTreeMap::from([(source_id, 4), (target_id, 7)]),
            MemoryJobStatus::Discarded,
            Some(hydrated_summary("obsolete", 1)),
        );

        validate_jobs(&batches, &by_layer_seq, &[discarded])
            .expect("discarded witnesses describe the rejected graph, not current versions");
    }

    #[test]
    fn decrypted_summary_has_an_explicit_plaintext_access_boundary() {
        let summary = DecryptedMemorySummary::new("runtime plaintext".to_owned());
        assert_eq!(summary.expose(), "runtime plaintext");
    }

    #[test]
    fn memory_limits_match_the_canonical_defaults() {
        assert_eq!(L0_BATCH_MIN, 5_000);
        assert_eq!(L0_FORCED_SEAL_LIMIT, 10_000);
        assert_eq!(L0_LIMIT, 40_000);
        assert_eq!(L0_DROP_TO, 30_000);
        assert_eq!(L1_LIMIT, 15_000);
        assert_eq!(L1_DROP_TO, 11_000);
        assert_eq!(L2_LIMIT, 10_000);
    }

    #[test]
    fn missing_shelf_result_does_not_remove_l0_batch() {
        let mut memory = ThreeLayerMemory::new(
            ConsolidatedMemory {
                summary: DecryptedMemorySummary::new(String::new()),
                est_tokens: 0,
            },
            TokenCalibration::default(),
        );
        let batch = L0Batch::new(Vec::new(), memory.allocate_l0_batch_seq(), 7, 11);
        let id = batch.id;
        memory.push_l0(batch);
        assert!(memory.promote_l0_to_l1(id).is_err());
        assert_eq!(memory.l0().len(), 1);
        assert_eq!(memory.l0().front().unwrap().id, id);
        assert_eq!(memory.l0().front().unwrap().eviction_footprint_tokens, 7);
    }

    #[test]
    fn l0_sequence_never_reuses_a_promoted_or_removed_queue_position() {
        let mut memory = ThreeLayerMemory::new(
            ConsolidatedMemory {
                summary: DecryptedMemorySummary::new(String::new()),
                est_tokens: 0,
            },
            TokenCalibration::default(),
        );
        let first = memory.allocate_l0_batch_seq();
        memory.push_l0(L0Batch::new(Vec::new(), first, 0, 0));
        memory.l0_mut().pop_front();
        assert_eq!(memory.allocate_l0_batch_seq(), first + 1);
    }
}
