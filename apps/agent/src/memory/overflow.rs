//! Memory overflow policy and promotion helpers for the three-layer memory.
//!
//! This module owns the capacity-driven rules: effective L0 calculation,
//! FIFO+hysteresis promotion, open-batch protection, and the runtime message
//! list recovery used before a provider retry.

use std::collections::{HashSet, VecDeque};

use anyhow::{Result, bail};

use crate::memory::estimate::{
    EstimateError, ProviderContextItemWithFootprint, TokenCalibration, estimate_public_message,
};
#[cfg(test)]
use crate::memory::{BatchId, ConsolidatedMemory, DecryptedMemorySummary, L0Batch};
use crate::memory::{
    BatchState, CompactResult, L0_DROP_TO, L0_LIMIT, L1_LIMIT, L2_LIMIT, ThreeLayerMemory,
};
use crate::provider::types::{
    AssistantContent, AssistantMessage, ContextMessage, Message, PublicAssistantContent,
    PublicAssistantMessage, PublicMessage, ToolResultMessage, UserMessage,
};

/// Hard-trigger multiplier for the first user call: 1.2x L0_LIMIT.
pub const L0_HARD_TRIGGER_NUM: u64 = 12;
pub const L0_HARD_TRIGGER_DEN: u64 = 10;

/// Runtime user text truncation limit for the L0 send view.
pub const USER_ATTACHMENT_TRUNCATION_BYTES: usize = 50 * 1024;

/// How the `ContextAssembler` should treat provider context.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AssemblyMode {
    /// Sumi's native three-layer assembly: L2/L1 summaries as memory blocks and
    /// encrypted reasoning as opaque provider context.
    SumiThreeLayer,
    /// Delegate context shaping to a provider-native compaction window when it
    /// exactly matches the destination fingerprint.
    ProviderNative,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OverflowReport {
    pub l0_promoted: usize,
    pub l1_compacted: bool,
    pub l2_consolidated: bool,
    pub l0_effective: u64,
}

/// Capacity-only overflow logic.  No elapsed-time factors are used.
pub struct Overflow {
    calib: TokenCalibration,
    mode: AssemblyMode,
}

impl Overflow {
    pub fn new(calib: TokenCalibration, mode: AssemblyMode) -> Self {
        Self { calib, mode }
    }

    pub fn mode(&self) -> AssemblyMode {
        self.mode
    }

    pub fn calibration(&self) -> TokenCalibration {
        self.calib
    }

    /// Hard limit used for the first provider request in a turn.
    pub fn l0_hard_limit() -> u64 {
        L0_LIMIT * L0_HARD_TRIGGER_NUM / L0_HARD_TRIGGER_DEN
    }

    /// `effective_l0 = ceil((sum(est) + sum(eviction_footprint)) * ratio)`.
    /// The calibration is applied exactly once to the summed raw values.
    pub fn effective_l0(
        total_est: u64,
        total_eviction_footprint: u64,
        calib: TokenCalibration,
    ) -> Result<u64> {
        Ok(calib
            .effective_tokens(total_est, total_eviction_footprint)
            .map_err(OverflowError::from)?)
    }

    /// Whether overflow handling must run before a provider call.
    ///
    /// * First user call: only if we exceed the 1.2x hard limit, preserving
    ///   the first-call TTFT budget.
    /// * Retries/continuations: apply as soon as we exceed the ordinary L0
    ///   limit.
    pub fn should_apply_l0(effective_l0: u64, is_first_user_call: bool) -> bool {
        if effective_l0 > Self::l0_hard_limit() {
            return true;
        }
        if is_first_user_call {
            return false;
        }
        effective_l0 > L0_LIMIT
    }

    /// Drop the oldest runtime messages until the calibrated total is at most
    /// `L0_DROP_TO` tokens.  The most recent user message is always preserved
    /// (recovery must not drop the active user command).
    ///
    /// This overload ignores provider-context eviction footprint; use
    /// [`Self::recover_context_with_provider_context`] when footprint is known.
    pub fn recover_context(
        &self,
        messages: Vec<ContextMessage>,
        is_first_user_call: bool,
    ) -> Result<Vec<ContextMessage>> {
        self.recover_context_with_provider_context(messages, is_first_user_call, &[])
    }

    /// Same as [`Self::recover_context`], but includes the saved eviction
    /// footprint of anchored `provider_context` items when deciding how much
    /// must drop.  Each entry carries its authoritative `EvictionFootprint`;
    /// the overflow logic does not recompute it.
    pub(crate) fn recover_context_with_provider_context(
        &self,
        messages: Vec<ContextMessage>,
        is_first_user_call: bool,
        provider_context: &[ProviderContextItemWithFootprint],
    ) -> Result<Vec<ContextMessage>> {
        if messages.is_empty() {
            return Ok(messages);
        }

        let footprints = message_footprints(provider_context, &messages)?;

        let mut estimates = VecDeque::with_capacity(messages.len());
        let mut footprints_q = VecDeque::with_capacity(messages.len());
        let mut total_est: u64 = 0;
        let mut total_footprint: u64 = 0;
        for message in &messages {
            let est = estimate_context_message(message)?;
            let footprint = *footprints
                .get(message_id_or_synthetic(message))
                .unwrap_or(&0);
            total_est = total_est
                .checked_add(est)
                .ok_or(OverflowError::ArithmeticOverflow)?;
            total_footprint = total_footprint
                .checked_add(footprint)
                .ok_or(OverflowError::ArithmeticOverflow)?;
            estimates.push_back(est);
            footprints_q.push_back(footprint);
        }

        let effective = Self::effective_l0(total_est, total_footprint, self.calib)?;
        let action_threshold = if is_first_user_call {
            Self::l0_hard_limit()
        } else {
            L0_LIMIT
        };
        if effective <= action_threshold {
            return Ok(messages);
        }

        // Drop only safe transcript units. An assistant tool-call and every
        // contiguous result for that call form one unit, so eviction can never
        // leave an orphan result or a request without its result.
        let mut units = replay_units(messages);
        let mut last_user_unit = units.iter().rposition(|unit| unit.iter().any(is_user));
        let mut unit_costs = VecDeque::with_capacity(units.len());
        for unit in &units {
            let mut cost = (0u64, 0u64);
            for _message in unit {
                let est = estimates
                    .pop_front()
                    .ok_or(OverflowError::ArithmeticOverflow)?;
                let footprint = footprints_q
                    .pop_front()
                    .ok_or(OverflowError::ArithmeticOverflow)?;
                cost.0 = cost
                    .0
                    .checked_add(est)
                    .ok_or(OverflowError::ArithmeticOverflow)?;
                cost.1 = cost
                    .1
                    .checked_add(footprint)
                    .ok_or(OverflowError::ArithmeticOverflow)?;
            }
            unit_costs.push_back(cost);
        }

        while Self::effective_l0(total_est, total_footprint, self.calib)? > L0_DROP_TO {
            let can_drop_front = match last_user_unit {
                Some(index) => index > 0,
                None => !units.is_empty(),
            };
            if !can_drop_front {
                break;
            }
            let (dropped_est, dropped_footprint) = unit_costs
                .pop_front()
                .ok_or(OverflowError::ArithmeticOverflow)?;
            total_est = total_est.saturating_sub(dropped_est);
            total_footprint = total_footprint.saturating_sub(dropped_footprint);
            units.pop_front();
            if let Some(index) = last_user_unit.as_mut() {
                // `*index > 0` was checked above; remaining units are now
                // before the preserved latest user by one fewer position.
                *index -= 1;
            }
        }

        Ok(units.into_iter().flatten().collect())
    }

    /// Apply L0 overflow by FIFO-promoting the oldest non-open L0 batches that
    /// already have a compacted summary on the shelf.  The open (most recent)
    /// batch is never dropped.
    ///
    /// Hysteresis: promotion stops once the calibrated L0 total is at most
    /// `L0_DROP_TO`.
    pub fn apply_l0(&self, memory: &mut ThreeLayerMemory) -> Result<OverflowReport> {
        let mut report = OverflowReport::default();
        loop {
            let effective = memory.effective_l0()?;
            report.l0_effective = effective;
            if effective <= L0_DROP_TO {
                break;
            }
            let front = match memory.l0().front() {
                Some(batch) => batch.clone(),
                None => break,
            };
            if front.state == BatchState::Open {
                break;
            }
            if !memory.shelf().contains_key(&front.id) {
                break;
            }
            memory.promote_l0_to_l1(front.id)?;
            report.l0_promoted += 1;
        }
        Ok(report)
    }

    /// If L1 has overflowed, replace the whole L1 queue with the supplied L2
    /// compact result.  Callers are responsible for producing a compact summary
    /// that covers the oldest L1 entries.
    pub fn apply_l1(
        &self,
        memory: &mut ThreeLayerMemory,
        compact: Option<CompactResult>,
    ) -> Result<OverflowReport> {
        let mut report = OverflowReport::default();
        if memory.l1_total()? > L1_LIMIT {
            if let Some(result) = compact {
                memory.compact_l1_to_l2(result);
                report.l1_compacted = true;
            } else {
                bail!("L1 overflow requires a compact result");
            }
        }
        Ok(report)
    }

    /// If the single L2 summary exceeds its limit, replace it with a freshly
    /// consolidated summary.
    pub fn apply_l2(
        &self,
        memory: &mut ThreeLayerMemory,
        compact: Option<CompactResult>,
    ) -> Result<OverflowReport> {
        let mut report = OverflowReport::default();
        if memory.l2().est_tokens > L2_LIMIT {
            if let Some(result) = compact {
                memory.consolidate_l2(result);
                report.l2_consolidated = true;
            } else {
                bail!("L2 overflow requires a compact result");
            }
        }
        Ok(report)
    }
}

/// Partition the persisted runtime view at replay-safe boundaries. A tool
/// call batch remains inseparable from its following results; every other
/// message is independently evictable.
fn replay_units(messages: Vec<ContextMessage>) -> VecDeque<Vec<ContextMessage>> {
    let mut units = VecDeque::new();
    let mut active = Vec::new();
    let mut pending_calls = HashSet::new();

    for message in messages {
        match match &message {
            ContextMessage::Persisted { message, .. } | ContextMessage::Synthetic { message } => {
                message
            }
        } {
            Message::Assistant(assistant) => {
                if !active.is_empty() {
                    units.push_back(std::mem::take(&mut active));
                    pending_calls.clear();
                }
                pending_calls.extend(assistant.content.iter().filter_map(
                    |content| match content {
                        AssistantContent::ToolCall { tool_call, .. } => Some(tool_call.id.clone()),
                        _ => None,
                    },
                ));
                active.push(message);
                if pending_calls.is_empty() {
                    units.push_back(std::mem::take(&mut active));
                }
            }
            Message::ToolResult(result) if pending_calls.contains(&result.tool_call_id) => {
                active.push(message);
            }
            Message::ToolResult(_) | Message::User(_) => {
                if !active.is_empty() {
                    units.push_back(std::mem::take(&mut active));
                    pending_calls.clear();
                }
                units.push_back(vec![message]);
            }
        }
    }
    if !active.is_empty() {
        units.push_back(active);
    }
    units
}

fn estimate_context_message(message: &ContextMessage) -> Result<u64> {
    let public = context_message_to_public(message);
    estimate_public_message(&public)
        .map_err(OverflowError::from)
        .map_err(Into::into)
}

fn is_user(message: &ContextMessage) -> bool {
    let inner = match message {
        ContextMessage::Persisted { message, .. } | ContextMessage::Synthetic { message } => {
            message
        }
    };
    matches!(inner, Message::User(_))
}

fn message_id_or_synthetic(message: &ContextMessage) -> &str {
    match message {
        ContextMessage::Persisted { id, .. } => id.as_str(),
        ContextMessage::Synthetic { .. } => "",
    }
}

fn message_footprints<'a>(
    provider_context: &'a [ProviderContextItemWithFootprint],
    messages: &'a [ContextMessage],
) -> Result<std::collections::HashMap<&'a str, u64>> {
    let mut map = std::collections::HashMap::new();
    for entry in provider_context {
        let anchor = match &entry.item.origin_message {
            Some(anchor) => anchor,
            None => continue,
        };
        if entry.footprint.estimator_version() == 0 {
            return Err(EstimateError::InvalidEstimatorVersion.into());
        }
        let tokens = entry.footprint.eviction_tokens();
        let map_entry = map.entry(anchor.message_id.as_str()).or_insert(0u64);
        *map_entry = map_entry
            .checked_add(tokens)
            .ok_or(OverflowError::ArithmeticOverflow)?;
    }
    // Messages without provider context still need an entry so the deque lookup
    // falls back to zero instead of missing.
    for message in messages {
        let id = message_id_or_synthetic(message);
        map.entry(id).or_insert(0);
    }
    Ok(map)
}

pub(crate) fn context_message_to_public(message: &ContextMessage) -> PublicMessage {
    match message {
        ContextMessage::Persisted { message, .. } | ContextMessage::Synthetic { message } => {
            message_to_public(message)
        }
    }
}

fn message_to_public(message: &Message) -> PublicMessage {
    match message {
        Message::User(UserMessage { content, timestamp }) => PublicMessage::User(UserMessage {
            content: content.clone(),
            timestamp: *timestamp,
        }),
        Message::ToolResult(ToolResultMessage {
            tool_call_id,
            tool_name,
            content,
            details,
            is_error,
            timestamp,
        }) => PublicMessage::ToolResult(ToolResultMessage {
            tool_call_id: tool_call_id.clone(),
            tool_name: tool_name.clone(),
            content: content.clone(),
            details: details.clone(),
            is_error: *is_error,
            timestamp: *timestamp,
        }),
        Message::Assistant(AssistantMessage {
            content,
            model,
            provider,
            origin,
            usage,
            stop_reason,
            error_message,
            provider_code,
            interrupted,
            timestamp,
        }) => PublicMessage::Assistant(PublicAssistantMessage {
            content: content.iter().map(assistant_content_to_public).collect(),
            model: model.clone(),
            provider: provider.clone(),
            origin: origin.clone(),
            usage: usage.clone(),
            stop_reason: *stop_reason,
            error_message: error_message.clone(),
            provider_code: provider_code.clone(),
            interrupted: *interrupted,
            timestamp: *timestamp,
        }),
    }
}

fn assistant_content_to_public(content: &AssistantContent) -> PublicAssistantContent {
    match content {
        AssistantContent::Text {
            text,
            wire_item_index,
        } => PublicAssistantContent::Text {
            text: text.clone(),
            wire_item_index: *wire_item_index,
        },
        AssistantContent::Thinking {
            thinking,
            signature_field,
            wire_item_index,
        } => PublicAssistantContent::Thinking {
            thinking: thinking.clone(),
            signature_field: signature_field.clone(),
            wire_item_index: *wire_item_index,
        },
        AssistantContent::ToolCall {
            tool_call,
            wire_item_index,
        } => PublicAssistantContent::ToolCall {
            tool_call: tool_call.clone(),
            wire_item_index: *wire_item_index,
        },
        AssistantContent::RejectedToolCall {
            rejected,
            wire_item_index,
        } => PublicAssistantContent::RejectedToolCall {
            rejected: rejected.clone(),
            wire_item_index: *wire_item_index,
        },
    }
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum OverflowError {
    #[error("token estimate overflowed u64")]
    ArithmeticOverflow,
    #[error(transparent)]
    Estimate(#[from] EstimateError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::types::{AssistantContent, AssistantMessage, UserContent, UserMessage};
    use chrono::Utc;

    fn user(text: &str) -> ContextMessage {
        ContextMessage::Synthetic {
            message: Message::User(UserMessage {
                content: vec![UserContent::Text {
                    text: text.to_owned(),
                }],
                timestamp: Utc::now(),
            }),
        }
    }

    fn user_of_len(len: usize) -> ContextMessage {
        user(&"x".repeat(len))
    }

    fn assistant(text: &str) -> ContextMessage {
        ContextMessage::Synthetic {
            message: Message::Assistant(AssistantMessage {
                content: vec![AssistantContent::Text {
                    text: text.to_owned(),
                    wire_item_index: 0,
                }],
                model: "m".to_owned(),
                provider: "p".to_owned(),
                origin: crate::provider::types::ProviderOrigin {
                    provider_instance_id: "pi".to_owned(),
                    protocol: crate::provider::types::ApiProtocol::OpenAiChatCompletions,
                    model: "m".to_owned(),
                },
                usage: crate::provider::types::Usage::default(),
                stop_reason: crate::provider::types::StopReason::Stop,
                error_message: None,
                provider_code: None,
                interrupted: false,
                timestamp: Utc::now(),
            }),
        }
    }

    #[test]
    fn effective_l0_applies_calibration_once() {
        let calib = TokenCalibration::new(1.5).unwrap();
        assert_eq!(
            Overflow::effective_l0(10_000, 2_000, calib).unwrap(),
            18_000
        );
    }

    #[test]
    fn should_apply_respects_first_call_hard_limit() {
        let hard = Overflow::l0_hard_limit();
        assert!(Overflow::should_apply_l0(hard + 1, true));
        assert!(!Overflow::should_apply_l0(hard, true));
        assert!(Overflow::should_apply_l0(hard, false)); // hard == L0_LIMIT*1.2 > L0_LIMIT
    }

    #[test]
    fn recover_context_drops_oldest_and_preserves_last_user() {
        let calib = TokenCalibration::new(1.0).unwrap();
        let overflow = Overflow::new(calib, AssemblyMode::SumiThreeLayer);

        let messages = vec![user_of_len(200_000), assistant("ack"), user("second")];
        let recovered = overflow.recover_context(messages, false).unwrap();
        assert_eq!(recovered.len(), 2);
        assert!(is_user(&recovered[recovered.len() - 1]));
    }

    #[test]
    fn recover_context_keeps_first_call_under_hard_limit() {
        let calib = TokenCalibration::new(1.0).unwrap();
        let overflow = Overflow::new(calib, AssemblyMode::SumiThreeLayer);

        let messages = vec![user("hello")];
        let recovered = overflow.recover_context(messages, true).unwrap();
        assert_eq!(recovered.len(), 1);
    }

    #[test]
    fn recover_context_does_not_drop_only_user() {
        let calib = TokenCalibration::new(1.0).unwrap();
        let overflow = Overflow::new(calib, AssemblyMode::SumiThreeLayer);

        let long = "x".repeat(200_000);
        let messages = vec![user(&long)];
        let recovered = overflow.recover_context(messages, false).unwrap();
        assert_eq!(recovered.len(), 1);
    }

    #[test]
    fn recover_context_drops_userless_overflow_fail_closed() {
        let overflow = Overflow::new(
            TokenCalibration::new(1.0).unwrap(),
            AssemblyMode::SumiThreeLayer,
        );
        let recovered = overflow
            .recover_context(vec![assistant(&"x".repeat(200_000))], false)
            .unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn l0_promotion_never_moves_open_batch() {
        let calib = TokenCalibration::new(1.0).unwrap();
        let overflow = Overflow::new(calib, AssemblyMode::SumiThreeLayer);
        let mut memory = ThreeLayerMemory::new(
            ConsolidatedMemory {
                summary: DecryptedMemorySummary::new("summary".to_owned()),
                est_tokens: 1_000,
            },
            calib,
        );

        let open_batch = L0Batch {
            id: BatchId::now_v7(),
            batch_seq: 0,
            messages: vec![],
            est_tokens: 50_000,
            eviction_footprint_tokens: 0,
            state: BatchState::Open,
        };
        memory.push_l0(open_batch);

        let report = overflow.apply_l0(&mut memory).unwrap();
        assert_eq!(report.l0_promoted, 0);
        assert_eq!(memory.l0().len(), 1);
    }
}
