//! Conversation memory state, batch boundaries, and token accounting.

#![allow(dead_code)] // T20/T21 consume these foundations.

pub mod batch;
pub mod compactor;
pub mod context_assembler;
pub mod estimate;
pub mod overflow;
#[allow(dead_code)]
pub mod transform;

use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::provider::types::{ContextMessage, PublicMessage};

use self::estimate::TokenCalibration;

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

    pub fn l2(&self) -> &ConsolidatedMemory {
        &self.l2
    }

    pub fn l1(&self) -> &VecDeque<L1Entry> {
        &self.l1
    }

    pub fn l0(&self) -> &VecDeque<L0Batch> {
        &self.l0
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

#[cfg(test)]
mod tests {
    use super::*;

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
