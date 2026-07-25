//! Conversation memory state, batch boundaries, and token accounting.

#![allow(dead_code)] // T20/T21 consume these foundations.

pub mod batch;
pub mod compactor;
pub mod estimate;
#[allow(dead_code)]
pub mod transform;

use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::provider::types::PublicMessage;

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
    /// Public transcript content only.  Opaque provider context is accounted
    /// for by `eviction_footprint_tokens`, never stored in this vector.
    pub messages: Vec<PublicMessage>,
    pub est_tokens: u64,
    pub eviction_footprint_tokens: u64,
    pub state: BatchState,
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

    pub fn shelf(&self) -> &HashMap<BatchId, CompactResult> {
        &self.shelf
    }

    pub fn calibration(&self) -> TokenCalibration {
        self.calib
    }

    pub fn pending_apply(&self) -> bool {
        self.pending_apply
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
}
