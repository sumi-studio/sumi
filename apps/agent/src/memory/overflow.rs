//! Memory overflow policy and promotion helpers for the three-layer memory.
//!
//! This module owns the capacity-driven rules: effective L0 calculation,
//! FIFO+hysteresis promotion, open-batch protection, and the runtime message
//! list recovery used before a provider retry.

use std::collections::VecDeque;

use anyhow::{Result, bail};

use crate::memory::estimate::{EstimateError, TokenCalibration, estimate_public_message};
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

    /// Drop the oldest runtime messages until the public estimate is at most
    /// `L0_DROP_TO` calibrated tokens.  The most recent user message is always
    /// preserved (recovery must not drop the active user command).
    pub fn recover_context(
        &self,
        messages: Vec<ContextMessage>,
        is_first_user_call: bool,
    ) -> Result<Vec<ContextMessage>> {
        if messages.is_empty() {
            return Ok(messages);
        }

        let mut estimates = VecDeque::with_capacity(messages.len());
        let mut total_est: u64 = 0;
        for message in &messages {
            let est = estimate_context_message(message)?;
            total_est = total_est
                .checked_add(est)
                .ok_or(OverflowError::ArithmeticOverflow)?;
            estimates.push_back(est);
        }

        let effective = Self::effective_l0(total_est, 0, self.calib)?;
        let action_threshold = if is_first_user_call {
            Self::l0_hard_limit()
        } else {
            L0_LIMIT
        };
        if effective <= action_threshold {
            return Ok(messages);
        }

        let mut messages: VecDeque<_> = messages.into();
        let mut last_user_index = messages.iter().rposition(is_user).unwrap_or(0);

        while Self::effective_l0(total_est, 0, self.calib)? > L0_DROP_TO
            && messages.len() > 1
            && last_user_index > 0
        {
            let dropped = estimates
                .pop_front()
                .ok_or(OverflowError::ArithmeticOverflow)?;
            total_est = total_est.saturating_sub(dropped);
            messages.pop_front();
            last_user_index -= 1;
        }

        Ok(messages.into_iter().collect())
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
