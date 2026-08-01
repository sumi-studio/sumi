use std::collections::{BTreeMap, HashMap, HashSet, hash_map::Entry};

use chrono::{DateTime, Utc};
use jsonschema::Validator;
use serde::Serialize;
use serde_json::{Value, json};
use thiserror::Error;

use super::{
    partial_json::parse_streaming,
    types::{
        AssistantContent, AssistantMessage, ProviderEvent, ProviderOrigin, RejectedToolCall,
        StopReason, ToolArgsPreview, ToolArgumentError, ToolCall, ToolDefinition,
        ToolResultMessage, Usage, UserContent, ValidatedToolArguments,
    },
};

pub const MAX_TOOL_ARGUMENT_BYTES: usize = 4 * 1024 * 1024;
pub const RESPONSE_BYTES_PER_OUTPUT_TOKEN: u64 = 64;
pub const RESPONSE_FIXED_OVERHEAD_BYTES: u64 = 1024 * 1024;
pub const RESPONSE_WIRE_EXPANSION: u64 = 6;
pub const RESPONSE_EVENTS_PER_OUTPUT_TOKEN: u64 = 8;
pub const RESPONSE_FIXED_EVENTS: u64 = 256;
pub const RESPONSE_PREVIEW_WORK_MULTIPLIER: u64 = 8;
pub const RESPONSE_TOOL_TOKENS_FLOOR: u64 = 8;
pub const RESPONSE_FIXED_TOOL_CALLS: u64 = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResponseBudget {
    pub max_content_bytes: usize,
    pub max_wire_bytes: usize,
    pub max_events: usize,
    pub max_preview_work_bytes: usize,
    pub max_tool_calls: usize,
}

impl ResponseBudget {
    pub fn for_output_tokens(output_tokens: u64) -> Option<Self> {
        if output_tokens == 0 {
            return None;
        }
        let max_content_bytes = output_tokens
            .checked_mul(RESPONSE_BYTES_PER_OUTPUT_TOKEN)?
            .checked_add(RESPONSE_FIXED_OVERHEAD_BYTES)?;
        let max_wire_bytes = max_content_bytes
            .checked_mul(RESPONSE_WIRE_EXPANSION)?
            .checked_add(RESPONSE_FIXED_OVERHEAD_BYTES)?;
        let max_events = output_tokens
            .checked_mul(RESPONSE_EVENTS_PER_OUTPUT_TOKEN)?
            .checked_add(RESPONSE_FIXED_EVENTS)?;
        let max_preview_work_bytes =
            max_content_bytes.checked_mul(RESPONSE_PREVIEW_WORK_MULTIPLIER)?;
        let max_tool_calls = output_tokens
            .checked_div(RESPONSE_TOOL_TOKENS_FLOOR)?
            .checked_add(RESPONSE_FIXED_TOOL_CALLS)?;
        Some(Self {
            max_content_bytes: usize::try_from(max_content_bytes).ok()?,
            max_wire_bytes: usize::try_from(max_wire_bytes).ok()?,
            max_events: usize::try_from(max_events).ok()?,
            max_preview_work_bytes: usize::try_from(max_preview_work_bytes).ok()?,
            max_tool_calls: usize::try_from(max_tool_calls).ok()?,
        })
    }
}

impl Default for ResponseBudget {
    fn default() -> Self {
        Self::for_output_tokens(16_384).expect("default response budget fits usize")
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AssemblerError {
    #[error("provider event stream emitted Start more than once")]
    DuplicateStart,
    #[error("provider event arrived before Start")]
    MissingStart,
    #[error("content index {0} is already in use")]
    DuplicateContentIndex(usize),
    #[error("content index {0} has no matching start event")]
    MissingContentStart(usize),
    #[error("content index {0} received an event for a different content family")]
    ContentFamilyMismatch(usize),
    #[error("content index {0} end content does not match accumulated deltas")]
    ContentMismatch(usize),
    #[error("content index {0} cannot be represented as a wire item index")]
    WireItemIndexOverflow(usize),
    #[error("provider event stream emitted more than one terminal event")]
    TerminalAlreadyEmitted,
    #[error("terminal reason does not match the assembled message")]
    TerminalReasonMismatch,
    #[error("terminal message content does not match the normalized event sequence")]
    TerminalContentMismatch,
    #[error("terminal message model does not match its provider origin")]
    TerminalOriginMismatch,
    #[error("successful terminal event arrived with unfinished content")]
    UnfinishedContent,
    #[error("terminal event variant does not match its stop reason")]
    TerminalVariantMismatch,
    #[error("rejected tool call and synthetic result do not form a canonical safe pair")]
    InvalidRejectedToolPair,
    #[error("response content exceeded the {limit}-byte cumulative budget")]
    ResponseContentBudgetExceeded { limit: usize },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ScratchBlock {
    Text(String),
    Thinking {
        content: String,
        signature_field: String,
    },
    Tool(ToolScratch),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ToolScratch {
    Raw(String),
    Overflow,
}

/// Reconstructs durable assistant content exclusively from normalized
/// `ProviderEvent`s. Protocol-specific chunk, usage, and finish-reason parsing
/// belongs to adapters.
#[derive(Debug, Default)]
pub struct MessageAssembler {
    started: bool,
    terminal: bool,
    scratch: HashMap<usize, ScratchBlock>,
    completed: BTreeMap<usize, AssistantContent>,
    synthetic_results: BTreeMap<usize, ToolResultMessage>,
    budget: ResponseBudget,
    content_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct TerminalMetadata {
    pub provider: String,
    pub origin: ProviderOrigin,
    pub usage: Usage,
    pub stop_reason: StopReason,
    pub error_message: Option<String>,
    pub provider_code: Option<String>,
    pub interrupted: bool,
    pub timestamp: DateTime<Utc>,
}

impl MessageAssembler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_budget(budget: ResponseBudget) -> Self {
        Self {
            budget,
            ..Self::default()
        }
    }

    /// Apply one normalized event. A terminal event returns the message
    /// reconstructed from the preceding events after checking it against the
    /// terminal payload.
    pub fn apply(
        &mut self,
        event: &ProviderEvent,
    ) -> Result<Option<AssistantMessage>, AssemblerError> {
        if self.terminal {
            return Err(AssemblerError::TerminalAlreadyEmitted);
        }

        match event {
            ProviderEvent::Start => {
                if self.started {
                    return Err(AssemblerError::DuplicateStart);
                }
                self.started = true;
                Ok(None)
            }
            ProviderEvent::Done { reason, output } => {
                self.ensure_started()?;
                if !matches!(
                    reason,
                    StopReason::Stop | StopReason::Length | StopReason::ToolUse
                ) {
                    return Err(AssemblerError::TerminalVariantMismatch);
                }
                if !self.scratch.is_empty() {
                    return Err(AssemblerError::UnfinishedContent);
                }
                let assembled = self.completed_content();
                if output.message.stop_reason != *reason {
                    return Err(AssemblerError::TerminalReasonMismatch);
                }
                if !terminal_origin_is_valid(&output.message) {
                    return Err(AssemblerError::TerminalOriginMismatch);
                }
                if output.message.interrupted {
                    return Err(AssemblerError::TerminalVariantMismatch);
                }
                if output.message.content != assembled {
                    return Err(AssemblerError::TerminalContentMismatch);
                }
                self.terminal = true;
                Ok(Some(output.message.clone()))
            }
            ProviderEvent::Error { reason, output } => {
                self.ensure_started()?;
                if !matches!(reason, StopReason::Error | StopReason::Aborted) {
                    return Err(AssemblerError::TerminalVariantMismatch);
                }
                if output.message.stop_reason != *reason {
                    return Err(AssemblerError::TerminalReasonMismatch);
                }
                if !terminal_origin_is_valid(&output.message) {
                    return Err(AssemblerError::TerminalOriginMismatch);
                }
                if (*reason == StopReason::Aborted) != output.message.interrupted {
                    return Err(AssemblerError::TerminalVariantMismatch);
                }
                self.accept_authoritative_error_content(*reason, &output.message.content)?;
                self.terminal = true;
                self.scratch.clear();
                Ok(Some(output.message.clone()))
            }
            _ => {
                self.ensure_started()?;
                self.apply_non_terminal(event)?;
                Ok(None)
            }
        }
    }

    /// Build the terminal message at the provider side after the adapter has
    /// emitted all normalized end events.
    pub fn finish(
        &mut self,
        metadata: TerminalMetadata,
    ) -> Result<AssistantMessage, AssemblerError> {
        let message = self.prepare_finish(metadata)?;
        if message.stop_reason == StopReason::Aborted {
            self.commit_prepared_error_terminal(message.stop_reason, &message.content)?;
        } else {
            self.commit_prepared_terminal();
        }
        Ok(message)
    }

    pub(crate) fn prepare_finish(
        &self,
        metadata: TerminalMetadata,
    ) -> Result<AssistantMessage, AssemblerError> {
        self.ensure_started()?;
        if self.terminal {
            return Err(AssemblerError::TerminalAlreadyEmitted);
        }
        if metadata.provider.is_empty()
            || metadata.origin.provider_instance_id.is_empty()
            || metadata.origin.model.is_empty()
        {
            return Err(AssemblerError::TerminalOriginMismatch);
        }
        match metadata.stop_reason {
            StopReason::Stop | StopReason::Length | StopReason::ToolUse => {
                if metadata.interrupted {
                    return Err(AssemblerError::TerminalVariantMismatch);
                }
                if !self.scratch.is_empty() {
                    return Err(AssemblerError::UnfinishedContent);
                }
            }
            StopReason::Error => {
                if metadata.interrupted {
                    return Err(AssemblerError::TerminalVariantMismatch);
                }
            }
            StopReason::Aborted => {
                if !metadata.interrupted {
                    return Err(AssemblerError::TerminalVariantMismatch);
                }
            }
        }
        let content = if metadata.stop_reason == StopReason::Aborted {
            self.authoritative_abort_content()?
        } else {
            self.completed_content()
        };
        Ok(AssistantMessage {
            content,
            model: metadata.origin.model.clone(),
            provider: metadata.provider,
            origin: metadata.origin,
            usage: metadata.usage,
            stop_reason: metadata.stop_reason,
            error_message: metadata.error_message,
            provider_code: metadata.provider_code,
            interrupted: metadata.interrupted,
            timestamp: metadata.timestamp,
        })
    }

    pub(crate) fn commit_prepared_terminal(&mut self) {
        debug_assert!(self.started);
        debug_assert!(!self.terminal);
        self.terminal = true;
        self.scratch.clear();
    }

    pub(crate) fn commit_prepared_error_terminal(
        &mut self,
        reason: StopReason,
        content: &[AssistantContent],
    ) -> Result<(), AssemblerError> {
        self.accept_authoritative_error_content(reason, content)?;
        self.commit_prepared_terminal();
        Ok(())
    }

    pub fn completed_content(&self) -> Vec<AssistantContent> {
        self.completed.values().cloned().collect()
    }

    pub fn synthetic_results(&self) -> Vec<ToolResultMessage> {
        self.synthetic_results.values().cloned().collect()
    }

    pub(crate) fn authoritative_error_content(
        &self,
    ) -> Result<Vec<AssistantContent>, AssemblerError> {
        let mut content = self.completed.clone();
        for (content_index, scratch) in &self.scratch {
            let wire_item_index = wire_item_index(*content_index)?;
            let block = match scratch {
                ScratchBlock::Text(text) => AssistantContent::Text {
                    text: text.clone(),
                    wire_item_index,
                },
                ScratchBlock::Thinking {
                    content,
                    signature_field,
                } => AssistantContent::Thinking {
                    thinking: content.clone(),
                    signature_field: signature_field.clone(),
                    wire_item_index,
                },
                ScratchBlock::Tool(_) => continue,
            };
            content.insert(*content_index, block);
        }
        Ok(content.into_values().collect())
    }

    pub(crate) fn authoritative_abort_content(
        &self,
    ) -> Result<Vec<AssistantContent>, AssemblerError> {
        let mut content = BTreeMap::new();
        for (content_index, block) in &self.completed {
            if matches!(
                block,
                AssistantContent::Text { .. } | AssistantContent::Thinking { .. }
            ) {
                content.insert(*content_index, block.clone());
            }
        }
        for (content_index, scratch) in &self.scratch {
            if let ScratchBlock::Text(text) = scratch {
                content.insert(
                    *content_index,
                    AssistantContent::Text {
                        text: text.clone(),
                        wire_item_index: wire_item_index(*content_index)?,
                    },
                );
            }
        }
        Ok(content.into_values().collect())
    }

    fn apply_non_terminal(&mut self, event: &ProviderEvent) -> Result<(), AssemblerError> {
        match event {
            ProviderEvent::TextStart { content_index } => {
                self.start_block(*content_index, ScratchBlock::Text(String::new()))
            }
            ProviderEvent::TextDelta {
                content_index,
                delta,
            } => {
                match self.scratch.get(content_index) {
                    Some(ScratchBlock::Text(_)) => Ok(()),
                    Some(_) => Err(AssemblerError::ContentFamilyMismatch(*content_index)),
                    None => Err(AssemblerError::MissingContentStart(*content_index)),
                }?;
                let next_content_bytes = self.checked_content_bytes(delta.len())?;
                let Some(ScratchBlock::Text(content)) = self.scratch.get_mut(content_index) else {
                    unreachable!("text scratch was validated above");
                };
                content.push_str(delta);
                self.content_bytes = next_content_bytes;
                Ok(())
            }
            ProviderEvent::TextEnd {
                content_index,
                content,
            } => {
                match self.scratch.get(content_index) {
                    Some(ScratchBlock::Text(accumulated)) if accumulated == content => {}
                    Some(ScratchBlock::Text(_)) => {
                        return Err(AssemblerError::ContentMismatch(*content_index));
                    }
                    Some(_) => {
                        return Err(AssemblerError::ContentFamilyMismatch(*content_index));
                    }
                    None => return Err(AssemblerError::MissingContentStart(*content_index)),
                };
                let wire_item_index = wire_item_index(*content_index)?;
                self.scratch.remove(content_index);
                self.completed.insert(
                    *content_index,
                    AssistantContent::Text {
                        text: content.clone(),
                        wire_item_index,
                    },
                );
                Ok(())
            }
            ProviderEvent::ThinkingStart {
                content_index,
                signature_field,
            } => {
                self.ensure_block_index_available(*content_index)?;
                let next_content_bytes = self.checked_content_bytes(signature_field.len())?;
                self.start_block(
                    *content_index,
                    ScratchBlock::Thinking {
                        content: String::new(),
                        signature_field: signature_field.clone(),
                    },
                )?;
                self.content_bytes = next_content_bytes;
                Ok(())
            }
            ProviderEvent::ThinkingDelta {
                content_index,
                delta,
            } => {
                match self.scratch.get(content_index) {
                    Some(ScratchBlock::Thinking { .. }) => Ok(()),
                    Some(_) => Err(AssemblerError::ContentFamilyMismatch(*content_index)),
                    None => Err(AssemblerError::MissingContentStart(*content_index)),
                }?;
                let next_content_bytes = self.checked_content_bytes(delta.len())?;
                let Some(ScratchBlock::Thinking { content, .. }) =
                    self.scratch.get_mut(content_index)
                else {
                    unreachable!("thinking scratch was validated above");
                };
                content.push_str(delta);
                self.content_bytes = next_content_bytes;
                Ok(())
            }
            ProviderEvent::ThinkingEnd {
                content_index,
                content,
            } => {
                let signature_field = match self.scratch.get(content_index) {
                    Some(ScratchBlock::Thinking {
                        content: accumulated,
                        signature_field,
                    }) if accumulated == content => signature_field.clone(),
                    Some(ScratchBlock::Thinking { .. }) => {
                        return Err(AssemblerError::ContentMismatch(*content_index));
                    }
                    Some(_) => {
                        return Err(AssemblerError::ContentFamilyMismatch(*content_index));
                    }
                    None => return Err(AssemblerError::MissingContentStart(*content_index)),
                };
                let wire_item_index = wire_item_index(*content_index)?;
                self.scratch.remove(content_index);
                self.completed.insert(
                    *content_index,
                    AssistantContent::Thinking {
                        thinking: content.clone(),
                        signature_field,
                        wire_item_index,
                    },
                );
                Ok(())
            }
            ProviderEvent::ToolCallStart { content_index } => self.start_block(
                *content_index,
                ScratchBlock::Tool(ToolScratch::Raw(String::new())),
            ),
            ProviderEvent::ToolCallDelta {
                content_index,
                delta,
            } => match self.scratch.get_mut(content_index) {
                Some(ScratchBlock::Tool(tool)) => {
                    if let ToolScratch::Raw(raw) = tool {
                        if raw
                            .len()
                            .checked_add(delta.len())
                            .is_none_or(|next| next > MAX_TOOL_ARGUMENT_BYTES)
                        {
                            // Replacing the state drops the allocation now.
                            // Later deltas remain observable events but retain
                            // no raw bytes in this assembler/shadow.
                            *tool = ToolScratch::Overflow;
                        } else {
                            raw.push_str(delta);
                        }
                    }
                    Ok(())
                }
                Some(_) => Err(AssemblerError::ContentFamilyMismatch(*content_index)),
                None => Err(AssemblerError::MissingContentStart(*content_index)),
            },
            ProviderEvent::ToolCallPreview { content_index, .. } => {
                match self.scratch.get(content_index) {
                    Some(ScratchBlock::Tool(_)) => Ok(()),
                    Some(_) => Err(AssemblerError::ContentFamilyMismatch(*content_index)),
                    None => Err(AssemblerError::MissingContentStart(*content_index)),
                }
            }
            ProviderEvent::ToolCallEnd {
                content_index,
                tool_call,
            } => {
                match self.scratch.get(content_index) {
                    Some(ScratchBlock::Tool(ToolScratch::Raw(_))) => {}
                    Some(ScratchBlock::Tool(ToolScratch::Overflow)) => {
                        return Err(AssemblerError::TerminalContentMismatch);
                    }
                    Some(_) => {
                        return Err(AssemblerError::ContentFamilyMismatch(*content_index));
                    }
                    None => return Err(AssemblerError::MissingContentStart(*content_index)),
                }
                let durable_bytes = tool_call
                    .id
                    .len()
                    .checked_add(tool_call.name.len())
                    .and_then(|bytes| {
                        serde_json::to_vec(tool_call.arguments.as_object())
                            .ok()
                            .and_then(|arguments| bytes.checked_add(arguments.len()))
                    })
                    .ok_or(AssemblerError::ResponseContentBudgetExceeded {
                        limit: self.budget.max_content_bytes,
                    })?;
                let wire_item_index = wire_item_index(*content_index)?;
                let next_content_bytes = self.checked_content_bytes(durable_bytes)?;
                self.scratch.remove(content_index);
                self.completed.insert(
                    *content_index,
                    AssistantContent::ToolCall {
                        tool_call: tool_call.clone(),
                        wire_item_index,
                    },
                );
                self.content_bytes = next_content_bytes;
                Ok(())
            }
            ProviderEvent::ToolCallRejected {
                content_index,
                rejected,
                synthetic_result,
            } => {
                validate_rejected_tool_pair(rejected, synthetic_result)?;
                match self.scratch.get(content_index) {
                    Some(ScratchBlock::Tool(ToolScratch::Overflow))
                        if rejected.error == ToolArgumentError::TooLarge => {}
                    Some(ScratchBlock::Tool(ToolScratch::Raw(_)))
                        if rejected.error != ToolArgumentError::TooLarge => {}
                    Some(ScratchBlock::Tool(_)) => {
                        return Err(AssemblerError::TerminalContentMismatch);
                    }
                    Some(_) => {
                        return Err(AssemblerError::ContentFamilyMismatch(*content_index));
                    }
                    None => return Err(AssemblerError::MissingContentStart(*content_index)),
                }
                let durable_bytes = rejected.id.len().checked_add(rejected.name.len()).ok_or(
                    AssemblerError::ResponseContentBudgetExceeded {
                        limit: self.budget.max_content_bytes,
                    },
                )?;
                let wire_item_index = wire_item_index(*content_index)?;
                let next_content_bytes = self.checked_content_bytes(durable_bytes)?;
                self.scratch.remove(content_index);
                self.completed.insert(
                    *content_index,
                    AssistantContent::RejectedToolCall {
                        rejected: rejected.clone(),
                        wire_item_index,
                    },
                );
                self.synthetic_results
                    .insert(*content_index, synthetic_result.clone());
                self.content_bytes = next_content_bytes;
                Ok(())
            }
            // Summaries are display-only and intentionally use a separate
            // correlation namespace from durable assistant content.
            ProviderEvent::ReasoningSummaryStart { .. }
            | ProviderEvent::ReasoningSummaryDelta { .. }
            | ProviderEvent::ReasoningSummaryEnd { .. } => Ok(()),
            ProviderEvent::Start | ProviderEvent::Done { .. } | ProviderEvent::Error { .. } => {
                unreachable!("handled by apply")
            }
        }
    }

    fn ensure_started(&self) -> Result<(), AssemblerError> {
        if self.started {
            Ok(())
        } else {
            Err(AssemblerError::MissingStart)
        }
    }

    fn start_block(
        &mut self,
        content_index: usize,
        block: ScratchBlock,
    ) -> Result<(), AssemblerError> {
        self.ensure_block_index_available(content_index)?;
        match self.scratch.entry(content_index) {
            Entry::Vacant(entry) => {
                entry.insert(block);
                Ok(())
            }
            Entry::Occupied(_) => Err(AssemblerError::DuplicateContentIndex(content_index)),
        }
    }

    fn ensure_block_index_available(&self, content_index: usize) -> Result<(), AssemblerError> {
        if self.completed.contains_key(&content_index) || self.scratch.contains_key(&content_index)
        {
            Err(AssemblerError::DuplicateContentIndex(content_index))
        } else {
            Ok(())
        }
    }

    fn checked_content_bytes(&self, additional: usize) -> Result<usize, AssemblerError> {
        let Some(next) = self.content_bytes.checked_add(additional) else {
            return Err(AssemblerError::ResponseContentBudgetExceeded {
                limit: self.budget.max_content_bytes,
            });
        };
        if next > self.budget.max_content_bytes {
            return Err(AssemblerError::ResponseContentBudgetExceeded {
                limit: self.budget.max_content_bytes,
            });
        }
        Ok(next)
    }

    fn accept_authoritative_error_content(
        &mut self,
        reason: StopReason,
        content: &[AssistantContent],
    ) -> Result<(), AssemblerError> {
        let aborted = reason == StopReason::Aborted;
        let mut authoritative = BTreeMap::new();
        for block in content {
            if aborted
                && matches!(
                    block,
                    AssistantContent::ToolCall { .. } | AssistantContent::RejectedToolCall { .. }
                )
            {
                return Err(AssemblerError::TerminalContentMismatch);
            }
            let content_index = assistant_content_index(block);
            if authoritative.insert(content_index, block.clone()).is_some() {
                return Err(AssemblerError::TerminalContentMismatch);
            }
        }

        for (content_index, completed) in &self.completed {
            if aborted
                && matches!(
                    completed,
                    AssistantContent::ToolCall { .. } | AssistantContent::RejectedToolCall { .. }
                )
            {
                if authoritative.contains_key(content_index) {
                    return Err(AssemblerError::TerminalContentMismatch);
                }
                continue;
            }
            if authoritative.get(content_index) != Some(completed) {
                return Err(AssemblerError::TerminalContentMismatch);
            }
        }
        for (content_index, scratch) in &self.scratch {
            let Some(block) = authoritative.get(content_index) else {
                // Error terminals are authoritative about durability. Open
                // thinking may be omitted when its producer did not verify or
                // retain it; retained partial text must still be closed and
                // reconciled explicitly.
                if matches!(scratch, ScratchBlock::Tool(_))
                    || matches!(scratch, ScratchBlock::Thinking { .. })
                {
                    continue;
                }
                return Err(AssemblerError::TerminalContentMismatch);
            };
            if !scratch_is_prefix_of(scratch, block) {
                return Err(AssemblerError::TerminalContentMismatch);
            }
        }

        let durable_bytes = content.iter().try_fold(0_usize, |total, block| {
            total.checked_add(assistant_content_bytes(block)?).ok_or(
                AssemblerError::ResponseContentBudgetExceeded {
                    limit: self.budget.max_content_bytes,
                },
            )
        })?;
        if durable_bytes > self.budget.max_content_bytes {
            return Err(AssemblerError::ResponseContentBudgetExceeded {
                limit: self.budget.max_content_bytes,
            });
        }
        self.completed = authoritative;
        self.synthetic_results.retain(|content_index, _| {
            matches!(
                self.completed.get(content_index),
                Some(AssistantContent::RejectedToolCall { .. })
            )
        });
        self.content_bytes = durable_bytes;
        Ok(())
    }
}

fn assistant_content_index(content: &AssistantContent) -> usize {
    let wire_item_index = match content {
        AssistantContent::Text {
            wire_item_index, ..
        }
        | AssistantContent::Thinking {
            wire_item_index, ..
        }
        | AssistantContent::ToolCall {
            wire_item_index, ..
        }
        | AssistantContent::RejectedToolCall {
            wire_item_index, ..
        } => *wire_item_index,
    };
    wire_item_index as usize
}

fn terminal_origin_is_valid(message: &AssistantMessage) -> bool {
    !message.provider.is_empty()
        && !message.model.is_empty()
        && !message.origin.provider_instance_id.is_empty()
        && message.model == message.origin.model
}

fn assistant_content_bytes(content: &AssistantContent) -> Result<usize, AssemblerError> {
    match content {
        AssistantContent::Text { text, .. } => Ok(text.len()),
        AssistantContent::Thinking {
            thinking,
            signature_field,
            ..
        } => thinking
            .len()
            .checked_add(signature_field.len())
            .ok_or(AssemblerError::ResponseContentBudgetExceeded { limit: usize::MAX }),
        AssistantContent::ToolCall { tool_call, .. } => tool_call
            .id
            .len()
            .checked_add(tool_call.name.len())
            .and_then(|bytes| {
                serde_json::to_vec(tool_call.arguments.as_object())
                    .ok()
                    .and_then(|arguments| bytes.checked_add(arguments.len()))
            })
            .ok_or(AssemblerError::ResponseContentBudgetExceeded { limit: usize::MAX }),
        AssistantContent::RejectedToolCall { rejected, .. } => rejected
            .id
            .len()
            .checked_add(rejected.name.len())
            .ok_or(AssemblerError::ResponseContentBudgetExceeded { limit: usize::MAX }),
    }
}

fn scratch_is_prefix_of(scratch: &ScratchBlock, content: &AssistantContent) -> bool {
    match (scratch, content) {
        (ScratchBlock::Text(prefix), AssistantContent::Text { text, .. }) => {
            text.starts_with(prefix)
        }
        (
            ScratchBlock::Thinking {
                content: prefix,
                signature_field,
            },
            AssistantContent::Thinking {
                thinking,
                signature_field: authoritative_field,
                ..
            },
        ) => authoritative_field == signature_field && thinking.starts_with(prefix),
        (
            ScratchBlock::Tool(ToolScratch::Raw(raw)),
            AssistantContent::ToolCall { tool_call, .. },
        ) => serde_json::from_str::<Value>(raw)
            .ok()
            .is_none_or(|value| value == Value::Object(tool_call.arguments.as_object().clone())),
        (
            ScratchBlock::Tool(ToolScratch::Raw(_)),
            AssistantContent::RejectedToolCall { rejected, .. },
        ) => rejected.error != ToolArgumentError::TooLarge,
        (
            ScratchBlock::Tool(ToolScratch::Overflow),
            AssistantContent::RejectedToolCall { rejected, .. },
        ) => rejected.error == ToolArgumentError::TooLarge,
        _ => false,
    }
}

fn wire_item_index(content_index: usize) -> Result<u32, AssemblerError> {
    u32::try_from(content_index).map_err(|_| AssemblerError::WireItemIndexOverflow(content_index))
}

#[derive(Debug, Error)]
pub enum FrozenSchemaError {
    #[error("duplicate tool schema for {0}")]
    DuplicateTool(String),
    #[error("tool schema for {0} contains a non-local $ref")]
    ExternalReference(String),
    #[error("invalid tool schema for {tool}: {message}")]
    InvalidSchema { tool: String, message: String },
}

#[derive(Clone, Debug, Default)]
pub struct FrozenToolSchemaRegistry {
    validators: HashMap<String, FrozenToolSchema>,
}

#[derive(Clone, Debug)]
struct FrozenToolSchema {
    validator: Validator,
    property_names: HashSet<String>,
}

impl FrozenToolSchemaRegistry {
    pub fn compile(tools: &[ToolDefinition]) -> Result<Self, FrozenSchemaError> {
        let mut validators = HashMap::with_capacity(tools.len());
        for tool in tools {
            if validators.contains_key(&tool.name) {
                return Err(FrozenSchemaError::DuplicateTool(tool.name.clone()));
            }
            if contains_external_ref(&tool.parameters) {
                return Err(FrozenSchemaError::ExternalReference(tool.name.clone()));
            }
            let validator = jsonschema::validator_for(&tool.parameters).map_err(|error| {
                FrozenSchemaError::InvalidSchema {
                    tool: tool.name.clone(),
                    message: error.to_string(),
                }
            })?;
            let mut property_names = HashSet::new();
            collect_property_names(&tool.parameters, &mut property_names);
            validators.insert(
                tool.name.clone(),
                FrozenToolSchema {
                    validator,
                    property_names,
                },
            );
        }
        Ok(Self { validators })
    }

    fn validator(&self, tool_name: &str) -> Option<&FrozenToolSchema> {
        self.validators.get(tool_name)
    }

    pub(crate) fn contains(&self, tool_name: &str) -> bool {
        self.validators.contains_key(tool_name)
    }
}

fn collect_property_names(value: &Value, output: &mut HashSet<String>) {
    match value {
        Value::Object(object) => {
            if let Some(properties) = object.get("properties").and_then(Value::as_object) {
                output.extend(properties.keys().cloned());
            }
            for value in object.values() {
                collect_property_names(value, output);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_property_names(value, output);
            }
        }
        _ => {}
    }
}

fn contains_external_ref(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, value)| {
            (key == "$ref"
                && value
                    .as_str()
                    .is_some_and(|reference| !reference.starts_with('#')))
                || contains_external_ref(value)
        }),
        Value::Array(values) => values.iter().any(contains_external_ref),
        _ => false,
    }
}

#[derive(Clone, Debug, Default)]
pub struct ToolArgumentAccumulator {
    raw: String,
    too_large: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ToolArgumentOutcome {
    Validated(ToolCall),
    Rejected {
        rejected: RejectedToolCall,
        synthetic_result: ToolResultMessage,
    },
}

#[derive(Clone, Debug)]
struct RejectionDetail {
    error: ToolArgumentError,
    instance_path: String,
    constraint: String,
}

const REJECTION_DIAGNOSTIC_TEXT: &str =
    "Tool arguments were rejected. Regenerate the tool call with complete, schema-valid arguments.";

// This is intentionally a narrow diagnostic vocabulary, not a mirror of every
// JSON Schema draft keyword. Validator labels outside this set are collapsed to
// `schema` so provider-controlled schema text cannot widen the rejection wire
// contract.
const SAFE_SCHEMA_CONSTRAINTS: &[&str] = &[
    "known_tool_schema",
    "schema",
    "type",
    "enum",
    "const",
    "required",
    "properties",
    "additionalProperties",
    "unevaluatedProperties",
    "dependentRequired",
    "propertyNames",
    "minProperties",
    "maxProperties",
    "items",
    "prefixItems",
    "contains",
    "minContains",
    "maxContains",
    "minItems",
    "maxItems",
    "uniqueItems",
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
    "minLength",
    "maxLength",
    "pattern",
    "format",
    "allOf",
    "anyOf",
    "oneOf",
    "not",
    "if",
    "then",
    "else",
];

impl ToolArgumentAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn append(&mut self, delta: &str) -> ToolArgsPreview {
        if self.too_large {
            return ToolArgsPreview::new(json!({}));
        }
        if self.raw.len().saturating_add(delta.len()) > MAX_TOOL_ARGUMENT_BYTES {
            self.raw = String::new();
            self.too_large = true;
            return ToolArgsPreview::new(json!({}));
        }
        self.raw.push_str(delta);
        parse_streaming(&self.raw)
    }

    pub fn raw_len(&self) -> usize {
        self.raw.len()
    }

    pub fn is_prefix_of(&self, complete: &str) -> bool {
        !self.too_large && complete.starts_with(&self.raw)
    }

    pub fn matches_raw(&self, complete: &str) -> bool {
        !self.too_large && self.raw == complete
    }

    pub fn finish(
        self,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        registry: &FrozenToolSchemaRegistry,
        timestamp: DateTime<Utc>,
    ) -> ToolArgumentOutcome {
        let call_id = call_id.into();
        let tool_name = tool_name.into();
        if self.too_large {
            return rejected_outcome(
                call_id,
                tool_name,
                RejectionDetail {
                    error: ToolArgumentError::TooLarge,
                    instance_path: String::new(),
                    constraint: "max_argument_bytes".to_owned(),
                },
                timestamp,
            );
        }
        let parsed = match serde_json::from_str::<Value>(&self.raw) {
            Ok(value) => value,
            Err(_) => {
                return rejected_outcome(
                    call_id,
                    tool_name,
                    RejectionDetail {
                        error: ToolArgumentError::InvalidJson,
                        instance_path: String::new(),
                        constraint: "json_syntax".to_owned(),
                    },
                    timestamp,
                );
            }
        };
        let Value::Object(arguments) = parsed else {
            return rejected_outcome(
                call_id,
                tool_name,
                RejectionDetail {
                    error: ToolArgumentError::NonObject,
                    instance_path: String::new(),
                    constraint: "object".to_owned(),
                },
                timestamp,
            );
        };
        let Some(schema) = registry.validator(&tool_name) else {
            return rejected_outcome(
                call_id,
                tool_name,
                RejectionDetail {
                    error: ToolArgumentError::SchemaViolation,
                    instance_path: String::new(),
                    constraint: "known_tool_schema".to_owned(),
                },
                timestamp,
            );
        };
        let instance = Value::Object(arguments.clone());
        if let Some(error) = schema.validator.iter_errors(&instance).next() {
            let schema_path = error.schema_path().to_string();
            return rejected_outcome(
                call_id,
                tool_name,
                RejectionDetail {
                    error: ToolArgumentError::SchemaViolation,
                    instance_path: safe_instance_path(
                        &error.instance_path().to_string(),
                        &schema.property_names,
                    ),
                    constraint: pointer_tail(&schema_path),
                },
                timestamp,
            );
        }

        ToolArgumentOutcome::Validated(ToolCall {
            id: call_id,
            name: tool_name,
            arguments: ValidatedToolArguments::from_schema_validated(arguments),
        })
    }

    pub fn reject_incomplete(
        self,
        call_id: impl Into<String>,
        tool_name: impl Into<String>,
        timestamp: DateTime<Utc>,
    ) -> ToolArgumentOutcome {
        let detail = if self.too_large {
            RejectionDetail {
                error: ToolArgumentError::TooLarge,
                instance_path: String::new(),
                constraint: "max_argument_bytes".to_owned(),
            }
        } else {
            RejectionDetail {
                error: ToolArgumentError::IncompleteResponse,
                instance_path: String::new(),
                constraint: "complete_response".to_owned(),
            }
        };
        rejected_outcome(call_id.into(), tool_name.into(), detail, timestamp)
    }
}

fn rejected_outcome(
    call_id: String,
    tool_name: String,
    detail: RejectionDetail,
    timestamp: DateTime<Utc>,
) -> ToolArgumentOutcome {
    let constraint = canonical_rejection_constraint(detail.error, &detail.constraint);
    let rejected = RejectedToolCall {
        id: call_id.clone(),
        name: tool_name.clone(),
        error: detail.error,
    };
    let synthetic_result = ToolResultMessage {
        tool_call_id: call_id,
        tool_name,
        content: vec![UserContent::Text {
            text: REJECTION_DIAGNOSTIC_TEXT.to_owned(),
        }],
        details: json!({
            "category": rejection_category(detail.error),
            "instance_path": detail.instance_path,
            "constraint": constraint,
        }),
        is_error: true,
        timestamp,
    };
    ToolArgumentOutcome::Rejected {
        rejected,
        synthetic_result,
    }
}

fn validate_rejected_tool_pair(
    rejected: &RejectedToolCall,
    synthetic_result: &ToolResultMessage,
) -> Result<(), AssemblerError> {
    let valid_content = matches!(
        synthetic_result.content.as_slice(),
        [UserContent::Text { text }] if text == REJECTION_DIAGNOSTIC_TEXT
    );
    let valid_details = synthetic_result.details.as_object().is_some_and(|details| {
        let instance_path = details.get("instance_path").and_then(Value::as_str);
        let constraint = details.get("constraint").and_then(Value::as_str);
        details.len() == 3
            && details
                .get("category")
                .and_then(Value::as_str)
                .is_some_and(|category| category == rejection_category(rejected.error))
            && instance_path.is_some_and(|path| {
                if rejected.error == ToolArgumentError::SchemaViolation {
                    is_safe_instance_path(path)
                } else {
                    path.is_empty()
                }
            })
            && constraint
                .is_some_and(|constraint| is_canonical_constraint(rejected.error, constraint))
    });
    if synthetic_result.tool_call_id == rejected.id
        && synthetic_result.tool_name == rejected.name
        && synthetic_result.is_error
        && valid_content
        && valid_details
    {
        Ok(())
    } else {
        Err(AssemblerError::InvalidRejectedToolPair)
    }
}

fn is_safe_instance_path(path: &str) -> bool {
    path.len() <= 1024
        && (path.is_empty()
            || (path.starts_with('/')
                && path.split('/').skip(1).all(|segment| {
                    !segment.is_empty() && segment.len() <= 128 && is_safe_path_segment(segment)
                })))
}

fn is_safe_path_segment(segment: &str) -> bool {
    segment == "*"
        || segment.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.' | '~')
        })
}

fn is_canonical_constraint(error: ToolArgumentError, constraint: &str) -> bool {
    constraint == canonical_rejection_constraint(error, constraint)
}

fn canonical_rejection_constraint(
    error: ToolArgumentError,
    validator_constraint: &str,
) -> &'static str {
    match error {
        ToolArgumentError::InvalidJson => "json_syntax",
        ToolArgumentError::NonObject => "object",
        ToolArgumentError::IncompleteResponse => "complete_response",
        ToolArgumentError::TooLarge => "max_argument_bytes",
        ToolArgumentError::SchemaViolation => SAFE_SCHEMA_CONSTRAINTS
            .iter()
            .copied()
            .find(|constraint| *constraint == validator_constraint)
            .unwrap_or("schema"),
    }
}

fn rejection_category(error: ToolArgumentError) -> &'static str {
    match error {
        ToolArgumentError::InvalidJson => "invalid_json",
        ToolArgumentError::NonObject => "non_object",
        ToolArgumentError::SchemaViolation => "schema_violation",
        ToolArgumentError::IncompleteResponse => "incomplete_response",
        ToolArgumentError::TooLarge => "too_large",
    }
}

fn safe_instance_path(path: &str, property_names: &HashSet<String>) -> String {
    let sanitized = path
        .split('/')
        .map(|segment| {
            if segment.is_empty()
                || segment.parse::<usize>().is_ok()
                || (property_names.contains(&unescape_pointer(segment))
                    && is_safe_path_segment(segment)
                    && segment.len() <= 128)
            {
                segment.to_owned()
            } else {
                "*".to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("/");
    if is_safe_instance_path(&sanitized) {
        sanitized
    } else {
        "/*".to_owned()
    }
}

fn unescape_pointer(segment: &str) -> String {
    segment.replace("~1", "/").replace("~0", "~")
}

fn pointer_tail(pointer: &str) -> String {
    pointer
        .rsplit('/')
        .find(|part| !part.is_empty())
        .unwrap_or("schema")
        .replace("~1", "/")
        .replace("~0", "~")
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use super::*;
    use crate::provider::types::{
        ApiProtocol, ProviderContextFragment, ProviderOutput, ToolArgumentError,
    };

    fn timestamp() -> DateTime<Utc> {
        Utc.timestamp_millis_opt(1_700_000_000_000)
            .single()
            .expect("timestamp")
    }

    fn origin() -> ProviderOrigin {
        ProviderOrigin {
            provider_instance_id: "moonshot:https://api.moonshot.ai/v1".to_owned(),
            protocol: ApiProtocol::OpenAiChatCompletions,
            model: "kimi-k3".to_owned(),
        }
    }

    fn metadata(stop_reason: StopReason) -> TerminalMetadata {
        TerminalMetadata {
            provider: "moonshot".to_owned(),
            origin: origin(),
            usage: Usage::default(),
            stop_reason,
            error_message: None,
            provider_code: Some("stop".to_owned()),
            interrupted: false,
            timestamp: timestamp(),
        }
    }

    fn schema_registry() -> FrozenToolSchemaRegistry {
        FrozenToolSchemaRegistry::compile(&[ToolDefinition {
            name: "read_file".to_owned(),
            description: "Read a file".to_owned(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "minLength": 1},
                    "limit": {"type": "integer", "minimum": 1}
                },
                "required": ["path"],
                "additionalProperties": false
            }),
        }])
        .expect("schema")
    }

    fn rejected_tool_event(
        content_index: usize,
        id: &str,
        error: ToolArgumentError,
    ) -> ProviderEvent {
        ProviderEvent::ToolCallRejected {
            content_index,
            rejected: RejectedToolCall {
                id: id.to_owned(),
                name: "read_file".to_owned(),
                error,
            },
            synthetic_result: ToolResultMessage {
                tool_call_id: id.to_owned(),
                tool_name: "read_file".to_owned(),
                content: vec![UserContent::Text {
                    text: REJECTION_DIAGNOSTIC_TEXT.to_owned(),
                }],
                details: json!({
                    "category": rejection_category(error),
                    "instance_path": "",
                    "constraint": match error {
                        ToolArgumentError::InvalidJson => "json_syntax",
                        ToolArgumentError::NonObject => "object",
                        ToolArgumentError::SchemaViolation => "required",
                        ToolArgumentError::IncompleteResponse => "complete_response",
                        ToolArgumentError::TooLarge => "max_argument_bytes",
                    },
                }),
                is_error: true,
                timestamp: timestamp(),
            },
        }
    }

    #[test]
    fn response_budget_is_checked_and_represents_kimi_k3_maximum() {
        let budget = ResponseBudget::for_output_tokens(1_048_576)
            .expect("Kimi K3 maximum output budget must fit");
        assert_eq!(budget.max_content_bytes, 68_157_440);
        assert_eq!(budget.max_wire_bytes, 409_993_216);
        assert_eq!(budget.max_events, 8_388_864);
        assert_eq!(budget.max_preview_work_bytes, 545_259_520);
        assert_eq!(budget.max_tool_calls, 131_088);
        assert!(ResponseBudget::for_output_tokens(0).is_none());
        assert!(ResponseBudget::for_output_tokens(u64::MAX).is_none());
    }

    #[test]
    fn assembler_enforces_content_budget_at_the_delta_boundary() {
        let budget = ResponseBudget {
            max_content_bytes: 5,
            ..ResponseBudget::default()
        };
        let mut assembler = MessageAssembler::with_budget(budget);
        assert_eq!(assembler.apply(&ProviderEvent::Start), Ok(None));
        assert_eq!(
            assembler.apply(&ProviderEvent::TextStart { content_index: 0 }),
            Ok(None)
        );
        assert_eq!(
            assembler.apply(&ProviderEvent::TextDelta {
                content_index: 0,
                delta: "hello".to_owned(),
            }),
            Ok(None)
        );
        assert_eq!(
            assembler.apply(&ProviderEvent::TextDelta {
                content_index: 0,
                delta: "!".to_owned(),
            }),
            Err(AssemblerError::ResponseContentBudgetExceeded { limit: 5 })
        );
    }

    #[test]
    fn tool_terminal_budget_counts_only_durable_identity_and_final_arguments() {
        let registry = schema_registry();
        let mut accumulator = ToolArgumentAccumulator::new();
        accumulator.append(r#"{"path":"x"}"#);
        let ToolArgumentOutcome::Validated(tool_call) =
            accumulator.finish("c", "read_file", &registry, timestamp())
        else {
            panic!("validated tool call")
        };
        let durable_bytes = tool_call.id.len()
            + tool_call.name.len()
            + serde_json::to_vec(tool_call.arguments.as_object())
                .expect("arguments serialize")
                .len();

        let mut exact = MessageAssembler::with_budget(ResponseBudget {
            max_content_bytes: durable_bytes,
            ..ResponseBudget::default()
        });
        exact.apply(&ProviderEvent::Start).expect("start");
        exact
            .apply(&ProviderEvent::ToolCallStart { content_index: 0 })
            .expect("tool start");
        exact
            .apply(&ProviderEvent::ToolCallDelta {
                content_index: 0,
                delta: r#"{"path":"x"}"#.to_owned(),
            })
            .expect("raw arguments are not durable-byte charged");
        exact
            .apply(&ProviderEvent::ToolCallEnd {
                content_index: 0,
                tool_call: tool_call.clone(),
            })
            .expect("exact durable boundary");

        let mut one_short = MessageAssembler::with_budget(ResponseBudget {
            max_content_bytes: durable_bytes - 1,
            ..ResponseBudget::default()
        });
        one_short.apply(&ProviderEvent::Start).expect("start");
        one_short
            .apply(&ProviderEvent::ToolCallStart { content_index: 0 })
            .expect("tool start");
        one_short
            .apply(&ProviderEvent::ToolCallDelta {
                content_index: 0,
                delta: r#"{"path":"x"}"#.to_owned(),
            })
            .expect("raw arguments excluded");
        assert_eq!(
            one_short.apply(&ProviderEvent::ToolCallEnd {
                content_index: 0,
                tool_call,
            }),
            Err(AssemblerError::ResponseContentBudgetExceeded {
                limit: durable_bytes - 1
            })
        );

        let mut rejected = MessageAssembler::with_budget(ResponseBudget {
            max_content_bytes: 0,
            ..ResponseBudget::default()
        });
        rejected.apply(&ProviderEvent::Start).expect("start");
        rejected
            .apply(&ProviderEvent::ToolCallStart { content_index: 0 })
            .expect("tool start");
        rejected
            .apply(&ProviderEvent::ToolCallDelta {
                content_index: 0,
                delta: "discarded raw arguments".to_owned(),
            })
            .expect("rejected raw is never durable");
        rejected
            .apply(&ProviderEvent::ToolCallRejected {
                content_index: 0,
                rejected: RejectedToolCall {
                    id: String::new(),
                    name: String::new(),
                    error: ToolArgumentError::InvalidJson,
                },
                synthetic_result: ToolResultMessage {
                    tool_call_id: String::new(),
                    tool_name: String::new(),
                    content: vec![UserContent::Text {
                        text: REJECTION_DIAGNOSTIC_TEXT.to_owned(),
                    }],
                    details: json!({
                        "category": "invalid_json",
                        "instance_path": "",
                        "constraint": "json_syntax",
                    }),
                    is_error: true,
                    timestamp: timestamp(),
                },
            })
            .expect("zero-byte durable rejection fits zero budget");
    }

    #[test]
    fn rejected_tool_pair_validation_is_exact_and_transactional() {
        let valid = rejected_tool_event(0, "call-1", ToolArgumentError::InvalidJson);
        let mut accepted = MessageAssembler::new();
        for event in [
            ProviderEvent::Start,
            ProviderEvent::ToolCallStart { content_index: 0 },
        ] {
            accepted.apply(&event).expect("prefix");
        }
        accepted.apply(&valid).expect("canonical pair");
        assert_eq!(accepted.synthetic_results().len(), 1);

        let mut invalid = Vec::new();
        for mutation in [
            "id",
            "name",
            "is_error",
            "content",
            "details",
            "category",
            "instance_path",
            "constraint",
        ] {
            let mut event = valid.clone();
            let ProviderEvent::ToolCallRejected {
                synthetic_result, ..
            } = &mut event
            else {
                unreachable!("rejection helper")
            };
            match mutation {
                "id" => synthetic_result.tool_call_id = "other-call".to_owned(),
                "name" => synthetic_result.tool_name = "other_tool".to_owned(),
                "is_error" => synthetic_result.is_error = false,
                "content" => {
                    synthetic_result.content = vec![UserContent::Text {
                        text: "raw arguments: {\"secret\":\"leak\"}".to_owned(),
                    }]
                }
                "details" => synthetic_result.details["raw_arguments"] = json!({"secret": "leak"}),
                "category" => synthetic_result.details["category"] = json!("schema_violation"),
                "instance_path" => {
                    synthetic_result.details["instance_path"] = json!("/{\"secret\":\"leak\"}")
                }
                "constraint" => {
                    synthetic_result.details["constraint"] = json!("raw={\"secret\":\"leak\"}")
                }
                _ => unreachable!(),
            }
            invalid.push((mutation, event));
        }

        for (mutation, event) in invalid {
            let mut assembler = MessageAssembler::new();
            assembler.apply(&ProviderEvent::Start).expect("start");
            assembler
                .apply(&ProviderEvent::ToolCallStart { content_index: 0 })
                .expect("tool start");
            assembler
                .apply(&ProviderEvent::ToolCallDelta {
                    content_index: 0,
                    delta: "untrusted raw".to_owned(),
                })
                .expect("tool delta");
            let scratch = assembler.scratch.clone();
            let completed = assembler.completed.clone();
            let synthetic_results = assembler.synthetic_results.clone();
            let content_bytes = assembler.content_bytes;

            assert_eq!(
                assembler.apply(&event),
                Err(AssemblerError::InvalidRejectedToolPair),
                "{mutation}"
            );
            assert_eq!(assembler.scratch, scratch, "{mutation}");
            assert_eq!(assembler.completed, completed, "{mutation}");
            assert_eq!(assembler.synthetic_results, synthetic_results, "{mutation}");
            assert_eq!(assembler.content_bytes, content_bytes, "{mutation}");
        }
    }

    #[test]
    fn provider_error_retains_completed_tools_and_safe_rejection_results() {
        let mut assembler = MessageAssembler::new();
        for event in [
            ProviderEvent::Start,
            ProviderEvent::ToolCallStart { content_index: 0 },
            rejected_tool_event(0, "call-rejected", ToolArgumentError::InvalidJson),
        ] {
            assembler.apply(&event).expect("provider error prefix");
        }
        let message = assembler
            .finish(TerminalMetadata {
                error_message: Some("provider failed after rejection".to_owned()),
                provider_code: Some("provider_error".to_owned()),
                ..metadata(StopReason::Error)
            })
            .expect("provider Error");
        assert!(matches!(
            message.content.as_slice(),
            [AssistantContent::RejectedToolCall { .. }]
        ));
        assert_eq!(assembler.synthetic_results().len(), 1);
    }

    #[test]
    fn aborted_snapshot_keeps_open_text_but_only_completed_thinking() {
        let mut assembler = MessageAssembler::new();
        for event in [
            ProviderEvent::Start,
            ProviderEvent::ThinkingStart {
                content_index: 0,
                signature_field: "reasoning_content".to_owned(),
            },
            ProviderEvent::ThinkingDelta {
                content_index: 0,
                delta: "unsigned partial".to_owned(),
            },
            ProviderEvent::TextStart { content_index: 1 },
            ProviderEvent::TextDelta {
                content_index: 1,
                delta: "visible partial".to_owned(),
            },
        ] {
            assembler.apply(&event).expect("prefix");
        }
        assert_eq!(
            assembler.authoritative_abort_content().expect("snapshot"),
            vec![AssistantContent::Text {
                text: "visible partial".to_owned(),
                wire_item_index: 1,
            }]
        );
    }

    #[test]
    fn every_assembler_tool_scratch_has_an_independent_four_mib_bound() {
        let first = "x".repeat(MAX_TOOL_ARGUMENT_BYTES / 2);
        let second = "y".repeat(MAX_TOOL_ARGUMENT_BYTES - first.len());
        let mut exact = MessageAssembler::new();
        exact.apply(&ProviderEvent::Start).expect("start");
        exact
            .apply(&ProviderEvent::ToolCallStart { content_index: 0 })
            .expect("tool start");
        for delta in [first, second] {
            exact
                .apply(&ProviderEvent::ToolCallDelta {
                    content_index: 0,
                    delta,
                })
                .expect("exact-bound delta");
        }
        assert!(matches!(
            exact.scratch.get(&0),
            Some(ScratchBlock::Tool(ToolScratch::Raw(raw)))
                if raw.len() == MAX_TOOL_ARGUMENT_BYTES
        ));

        exact
            .apply(&ProviderEvent::ToolCallDelta {
                content_index: 0,
                delta: "overflow".to_owned(),
            })
            .expect("first over-bound delta enters overflow state");
        assert_eq!(
            exact.scratch.get(&0),
            Some(&ScratchBlock::Tool(ToolScratch::Overflow))
        );
        exact
            .apply(&ProviderEvent::ToolCallDelta {
                content_index: 0,
                delta: "later-secret-must-not-be-retained".to_owned(),
            })
            .expect("later overflow delta");
        assert_eq!(
            exact.scratch.get(&0),
            Some(&ScratchBlock::Tool(ToolScratch::Overflow))
        );

        exact
            .apply(&ProviderEvent::ToolCallStart { content_index: 1 })
            .expect("independent tool start");
        exact
            .apply(&ProviderEvent::ToolCallDelta {
                content_index: 1,
                delta: "{}".to_owned(),
            })
            .expect("independent tool remains bounded raw");
        assert_eq!(
            exact.scratch.get(&1),
            Some(&ScratchBlock::Tool(ToolScratch::Raw("{}".to_owned())))
        );
    }

    #[test]
    fn producer_and_consumer_shadows_close_overflow_only_as_too_large_rejection() {
        let events = [
            ProviderEvent::Start,
            ProviderEvent::ToolCallStart { content_index: 4 },
            ProviderEvent::ToolCallDelta {
                content_index: 4,
                delta: "x".repeat(MAX_TOOL_ARGUMENT_BYTES),
            },
            ProviderEvent::ToolCallDelta {
                content_index: 4,
                delta: "!".to_owned(),
            },
            ProviderEvent::ToolCallDelta {
                content_index: 4,
                delta: "ignored-later-delta".to_owned(),
            },
            rejected_tool_event(4, "call-overflow", ToolArgumentError::TooLarge),
        ];
        let mut producer = MessageAssembler::new();
        let mut consumer_shadow = MessageAssembler::new();
        for event in &events {
            producer.apply(event).expect("producer event");
            consumer_shadow.apply(event).expect("consumer shadow event");
        }
        assert_eq!(
            producer.completed_content(),
            consumer_shadow.completed_content()
        );
        assert!(matches!(
            producer.completed_content().as_slice(),
            [AssistantContent::RejectedToolCall { rejected, .. }]
                if rejected.error == ToolArgumentError::TooLarge
        ));

        let mut invalid = MessageAssembler::new();
        for event in &events[..5] {
            invalid.apply(event).expect("overflow prefix");
        }
        assert_eq!(
            invalid.apply(&rejected_tool_event(
                4,
                "call-overflow",
                ToolArgumentError::InvalidJson,
            )),
            Err(AssemblerError::TerminalContentMismatch)
        );
        assert_eq!(
            invalid.scratch.get(&4),
            Some(&ScratchBlock::Tool(ToolScratch::Overflow))
        );
    }

    #[test]
    fn normalized_events_reconstruct_sparse_content_and_thinking_signature() {
        let mut assembler = MessageAssembler::new();
        for event in [
            ProviderEvent::Start,
            ProviderEvent::ThinkingStart {
                content_index: 1,
                signature_field: "reasoning_content".to_owned(),
            },
            ProviderEvent::ThinkingDelta {
                content_index: 1,
                delta: "consider".to_owned(),
            },
            ProviderEvent::ThinkingEnd {
                content_index: 1,
                content: "consider".to_owned(),
            },
            ProviderEvent::ReasoningSummaryStart { content_index: 1 },
            ProviderEvent::ReasoningSummaryDelta {
                content_index: 1,
                delta: "safe summary".to_owned(),
            },
            ProviderEvent::ReasoningSummaryEnd {
                content_index: 1,
                content: "safe summary".to_owned(),
            },
            ProviderEvent::TextStart { content_index: 3 },
            ProviderEvent::TextDelta {
                content_index: 3,
                delta: "hello".to_owned(),
            },
            ProviderEvent::TextEnd {
                content_index: 3,
                content: "hello".to_owned(),
            },
        ] {
            assert_eq!(assembler.apply(&event), Ok(None));
        }

        let message = assembler
            .finish(metadata(StopReason::Stop))
            .expect("terminal message");
        assert_eq!(
            message.content,
            vec![
                AssistantContent::Thinking {
                    thinking: "consider".to_owned(),
                    signature_field: "reasoning_content".to_owned(),
                    wire_item_index: 1,
                },
                AssistantContent::Text {
                    text: "hello".to_owned(),
                    wire_item_index: 3,
                },
            ]
        );
    }

    #[test]
    fn terminal_consumer_reconstructs_and_checks_payload() {
        let sequence = [
            ProviderEvent::Start,
            ProviderEvent::TextStart { content_index: 0 },
            ProviderEvent::TextDelta {
                content_index: 0,
                delta: "hello".to_owned(),
            },
            ProviderEvent::TextEnd {
                content_index: 0,
                content: "hello".to_owned(),
            },
        ];
        let mut producer = MessageAssembler::new();
        for event in &sequence {
            producer.apply(event).expect("producer event");
        }
        let message = producer
            .finish(metadata(StopReason::Stop))
            .expect("message");
        let terminal = ProviderEvent::Done {
            reason: StopReason::Stop,
            output: ProviderOutput {
                message: message.clone(),
                provider_context: Vec::<ProviderContextFragment>::new(),
            },
        };

        let mut consumer = MessageAssembler::new();
        for event in &sequence {
            consumer.apply(event).expect("consumer event");
        }
        assert_eq!(consumer.apply(&terminal).expect("terminal"), Some(message));
        assert_eq!(
            consumer.apply(&terminal),
            Err(AssemblerError::TerminalAlreadyEmitted)
        );
    }

    #[test]
    fn priority_error_snapshot_reconciles_received_partial_prefix() {
        let sequence = [
            ProviderEvent::Start,
            ProviderEvent::TextStart { content_index: 0 },
            ProviderEvent::TextDelta {
                content_index: 0,
                delta: "partial".to_owned(),
            },
            ProviderEvent::TextEnd {
                content_index: 0,
                content: "partial".to_owned(),
            },
        ];
        let mut producer = MessageAssembler::new();
        for event in &sequence {
            producer.apply(event).expect("producer event");
        }
        let message = producer
            .finish(TerminalMetadata {
                error_message: Some("transport failed".to_owned()),
                provider_code: Some("transport_error".to_owned()),
                ..metadata(StopReason::Error)
            })
            .expect("producer snapshot");
        let terminal = ProviderEvent::Error {
            reason: StopReason::Error,
            output: ProviderOutput {
                message: message.clone(),
                provider_context: Vec::new(),
            },
        };

        let mut consumer = MessageAssembler::new();
        for event in &sequence[..3] {
            consumer.apply(event).expect("consumer prefix");
        }
        assert_eq!(
            consumer.apply(&terminal).expect("authoritative terminal"),
            Some(message)
        );
    }

    #[test]
    fn error_terminal_may_drop_unfinished_thinking_after_verified_content() {
        let verified = AssistantContent::Text {
            text: "verified".to_owned(),
            wire_item_index: 0,
        };
        let terminal = ProviderEvent::Error {
            reason: StopReason::Error,
            output: ProviderOutput {
                message: AssistantMessage {
                    content: vec![verified.clone()],
                    model: origin().model,
                    provider: "moonshot".to_owned(),
                    origin: origin(),
                    usage: Usage::default(),
                    stop_reason: StopReason::Error,
                    error_message: Some("provider failed".to_owned()),
                    provider_code: Some("provider_error".to_owned()),
                    interrupted: false,
                    timestamp: timestamp(),
                },
                provider_context: Vec::new(),
            },
        };
        let mut consumer = MessageAssembler::new();
        for event in [
            ProviderEvent::Start,
            ProviderEvent::TextStart { content_index: 0 },
            ProviderEvent::TextDelta {
                content_index: 0,
                delta: "verified".to_owned(),
            },
            ProviderEvent::TextEnd {
                content_index: 0,
                content: "verified".to_owned(),
            },
            ProviderEvent::ThinkingStart {
                content_index: 1,
                signature_field: "signature".to_owned(),
            },
            ProviderEvent::ThinkingDelta {
                content_index: 1,
                delta: "unsigned".to_owned(),
            },
        ] {
            consumer.apply(&event).expect("trusted prefix");
        }

        let message = consumer
            .apply(&terminal)
            .expect("authoritative error may omit unfinished thinking")
            .expect("terminal message");
        assert_eq!(message.content, vec![verified]);
    }

    #[test]
    fn priority_error_snapshot_rejects_conflicts_and_budget_overflow() {
        let mut producer = MessageAssembler::new();
        producer.apply(&ProviderEvent::Start).expect("start");
        producer
            .apply(&ProviderEvent::TextStart { content_index: 0 })
            .expect("text start");
        producer
            .apply(&ProviderEvent::TextDelta {
                content_index: 0,
                delta: "authoritative".to_owned(),
            })
            .expect("text");
        producer
            .apply(&ProviderEvent::TextEnd {
                content_index: 0,
                content: "authoritative".to_owned(),
            })
            .expect("text end");
        let message = producer
            .finish(metadata(StopReason::Error))
            .expect("message");
        let terminal = ProviderEvent::Error {
            reason: StopReason::Error,
            output: ProviderOutput {
                message,
                provider_context: Vec::new(),
            },
        };

        let mut conflict = MessageAssembler::new();
        conflict.apply(&ProviderEvent::Start).expect("start");
        conflict
            .apply(&ProviderEvent::TextStart { content_index: 0 })
            .expect("text start");
        conflict
            .apply(&ProviderEvent::TextDelta {
                content_index: 0,
                delta: "different".to_owned(),
            })
            .expect("text");
        assert_eq!(
            conflict.apply(&terminal),
            Err(AssemblerError::TerminalContentMismatch)
        );

        let mut bounded = MessageAssembler::with_budget(ResponseBudget {
            max_content_bytes: "authoritative".len() - 1,
            ..ResponseBudget::default()
        });
        bounded.apply(&ProviderEvent::Start).expect("start");
        assert_eq!(
            bounded.apply(&terminal),
            Err(AssemblerError::ResponseContentBudgetExceeded {
                limit: "authoritative".len() - 1
            })
        );
    }

    #[test]
    fn done_still_requires_the_complete_ordered_event_sequence() {
        let mut producer = MessageAssembler::new();
        for event in [
            ProviderEvent::Start,
            ProviderEvent::TextStart { content_index: 0 },
            ProviderEvent::TextDelta {
                content_index: 0,
                delta: "complete".to_owned(),
            },
            ProviderEvent::TextEnd {
                content_index: 0,
                content: "complete".to_owned(),
            },
        ] {
            producer.apply(&event).expect("producer event");
        }
        let message = producer
            .finish(metadata(StopReason::Stop))
            .expect("producer message");
        let mut consumer = MessageAssembler::new();
        consumer.apply(&ProviderEvent::Start).expect("start");
        consumer
            .apply(&ProviderEvent::TextStart { content_index: 0 })
            .expect("text start");
        consumer
            .apply(&ProviderEvent::TextDelta {
                content_index: 0,
                delta: "complete".to_owned(),
            })
            .expect("text delta");
        assert_eq!(
            consumer.apply(&ProviderEvent::Done {
                reason: StopReason::Stop,
                output: ProviderOutput {
                    message,
                    provider_context: Vec::new(),
                },
            }),
            Err(AssemblerError::UnfinishedContent)
        );
    }

    #[test]
    fn terminal_event_variant_must_match_stop_reason() {
        let mut assembler = MessageAssembler::new();
        assembler.apply(&ProviderEvent::Start).expect("start");
        let message = assembler
            .finish(metadata(StopReason::Stop))
            .expect("message");
        let mut consumer = MessageAssembler::new();
        consumer.apply(&ProviderEvent::Start).expect("start");
        assert_eq!(
            consumer.apply(&ProviderEvent::Error {
                reason: StopReason::Stop,
                output: ProviderOutput {
                    message,
                    provider_context: vec![],
                },
            }),
            Err(AssemblerError::TerminalVariantMismatch)
        );
    }

    #[test]
    fn error_terminal_discards_unfinished_scratch() {
        let mut assembler = MessageAssembler::new();
        assembler.apply(&ProviderEvent::Start).expect("start");
        assembler
            .apply(&ProviderEvent::TextStart { content_index: 0 })
            .expect("text start");
        assembler
            .apply(&ProviderEvent::TextDelta {
                content_index: 0,
                delta: "partial secret".to_owned(),
            })
            .expect("text delta");
        let message = assembler
            .finish(TerminalMetadata {
                error_message: Some("transport failed".to_owned()),
                provider_code: Some("network_error".to_owned()),
                ..metadata(StopReason::Error)
            })
            .expect("error message");
        assert!(message.content.is_empty());
    }

    #[test]
    fn successful_terminal_rejects_unfinished_scratch() {
        let mut assembler = MessageAssembler::new();
        assembler.apply(&ProviderEvent::Start).expect("start");
        assembler
            .apply(&ProviderEvent::TextStart { content_index: 0 })
            .expect("text start");
        assert_eq!(
            assembler.finish(metadata(StopReason::Stop)),
            Err(AssemblerError::UnfinishedContent)
        );
    }

    #[test]
    fn content_family_and_end_content_must_match() {
        let mut assembler = MessageAssembler::new();
        assembler.apply(&ProviderEvent::Start).expect("start");
        assembler
            .apply(&ProviderEvent::TextStart { content_index: 0 })
            .expect("text start");
        assert_eq!(
            assembler.apply(&ProviderEvent::ThinkingDelta {
                content_index: 0,
                delta: "wrong family".to_owned(),
            }),
            Err(AssemblerError::ContentFamilyMismatch(0))
        );
        assert_eq!(
            assembler.apply(&ProviderEvent::TextEnd {
                content_index: 0,
                content: "not accumulated".to_owned(),
            }),
            Err(AssemblerError::ContentMismatch(0))
        );
    }

    #[test]
    fn rejected_deltas_and_ends_leave_partial_state_and_budget_unchanged() {
        let mut assembler = MessageAssembler::with_budget(ResponseBudget {
            max_content_bytes: 5,
            ..ResponseBudget::default()
        });
        assembler.apply(&ProviderEvent::Start).expect("start");
        assembler
            .apply(&ProviderEvent::TextStart { content_index: 0 })
            .expect("text start");

        assert_eq!(
            assembler.apply(&ProviderEvent::ThinkingDelta {
                content_index: 0,
                delta: "wrong".to_owned(),
            }),
            Err(AssemblerError::ContentFamilyMismatch(0))
        );
        assert_eq!(assembler.content_bytes, 0);
        assert_eq!(
            assembler.scratch.get(&0),
            Some(&ScratchBlock::Text(String::new()))
        );

        assembler
            .apply(&ProviderEvent::TextDelta {
                content_index: 0,
                delta: "hello".to_owned(),
            })
            .expect("accepted partial");
        assert_eq!(
            assembler.apply(&ProviderEvent::TextDelta {
                content_index: 0,
                delta: "!".to_owned(),
            }),
            Err(AssemblerError::ResponseContentBudgetExceeded { limit: 5 })
        );
        assert_eq!(assembler.content_bytes, 5);
        assert_eq!(
            assembler.scratch.get(&0),
            Some(&ScratchBlock::Text("hello".to_owned()))
        );

        assert_eq!(
            assembler.apply(&ProviderEvent::ThinkingEnd {
                content_index: 0,
                content: "hello".to_owned(),
            }),
            Err(AssemblerError::ContentFamilyMismatch(0))
        );
        assert_eq!(
            assembler.apply(&ProviderEvent::TextEnd {
                content_index: 0,
                content: "mismatch".to_owned(),
            }),
            Err(AssemblerError::ContentMismatch(0))
        );
        assert_eq!(assembler.content_bytes, 5);
        assert_eq!(
            assembler.scratch.get(&0),
            Some(&ScratchBlock::Text("hello".to_owned()))
        );
        assembler
            .apply(&ProviderEvent::TextEnd {
                content_index: 0,
                content: "hello".to_owned(),
            })
            .expect("matching end still succeeds");
    }

    #[test]
    fn rejected_start_and_wire_index_conversion_are_transactional() {
        let mut duplicate = MessageAssembler::with_budget(ResponseBudget {
            max_content_bytes: 5,
            ..ResponseBudget::default()
        });
        duplicate.apply(&ProviderEvent::Start).expect("start");
        duplicate
            .apply(&ProviderEvent::ThinkingStart {
                content_index: 0,
                signature_field: "sig".to_owned(),
            })
            .expect("thinking start");
        assert_eq!(
            duplicate.apply(&ProviderEvent::ThinkingStart {
                content_index: 0,
                signature_field: "xx".to_owned(),
            }),
            Err(AssemblerError::DuplicateContentIndex(0))
        );
        assert_eq!(duplicate.content_bytes, 3);
        duplicate
            .apply(&ProviderEvent::ThinkingDelta {
                content_index: 0,
                delta: "hi".to_owned(),
            })
            .expect("duplicate start did not consume budget");

        let content_index = usize::try_from(u64::from(u32::MAX) + 1)
            .expect("wire overflow index is representable on supported targets");
        let mut overflow = MessageAssembler::new();
        overflow.apply(&ProviderEvent::Start).expect("start");
        overflow
            .apply(&ProviderEvent::TextStart { content_index })
            .expect("text start");
        overflow
            .apply(&ProviderEvent::TextDelta {
                content_index,
                delta: "partial".to_owned(),
            })
            .expect("partial");
        assert_eq!(
            overflow.apply(&ProviderEvent::TextEnd {
                content_index,
                content: "partial".to_owned(),
            }),
            Err(AssemblerError::WireItemIndexOverflow(content_index))
        );
        assert_eq!(overflow.content_bytes, "partial".len());
        assert_eq!(
            overflow.scratch.get(&content_index),
            Some(&ScratchBlock::Text("partial".to_owned()))
        );
        assert!(overflow.completed.is_empty());
    }

    #[test]
    fn preview_repair_never_becomes_validated_arguments() {
        let registry = schema_registry();
        let mut accumulator = ToolArgumentAccumulator::new();
        let preview = accumulator.append(r#"{"path":"notes"#);
        assert_eq!(preview, json!({"path": "notes"}));
        let outcome = accumulator.finish("call-1", "read_file", &registry, timestamp());
        assert_rejected(outcome, ToolArgumentError::InvalidJson);
    }

    #[test]
    fn strict_validation_rejects_non_object_and_schema_violations() {
        let registry = schema_registry();
        for (raw, expected) in [
            (r#"["notes.txt"]"#, ToolArgumentError::NonObject),
            (r#"{"path":""}"#, ToolArgumentError::SchemaViolation),
            (
                r#"{"path":"notes.txt","extra":true}"#,
                ToolArgumentError::SchemaViolation,
            ),
            (
                r#"{"path":"notes.txt","limit":0}"#,
                ToolArgumentError::SchemaViolation,
            ),
        ] {
            let mut accumulator = ToolArgumentAccumulator::new();
            accumulator.append(raw);
            assert_rejected(
                accumulator.finish("call-1", "read_file", &registry, timestamp()),
                expected,
            );
        }
    }

    #[test]
    fn schema_rejection_preserves_known_constraint_and_normalizes_unknown_constraint() {
        let registry = schema_registry();
        let mut known = ToolArgumentAccumulator::new();
        known.append(r#"{"path":""}"#);
        let known = known.finish("call-known", "read_file", &registry, timestamp());
        let ToolArgumentOutcome::Rejected {
            rejected,
            synthetic_result,
        } = known
        else {
            panic!("known schema violation must be rejected")
        };
        assert_eq!(synthetic_result.details["instance_path"], json!("/path"));
        assert_eq!(synthetic_result.details["constraint"], json!("minLength"));

        let event = ProviderEvent::ToolCallRejected {
            content_index: 0,
            rejected,
            synthetic_result,
        };
        let mut assembler = MessageAssembler::new();
        assembler.apply(&ProviderEvent::Start).expect("start");
        assembler
            .apply(&ProviderEvent::ToolCallStart { content_index: 0 })
            .expect("tool start");
        assembler.apply(&event).expect("known canonical constraint");

        let raw_keyword = "unknown-".repeat(512);
        let unknown = rejected_outcome(
            "call-unknown".to_owned(),
            "read_file".to_owned(),
            RejectionDetail {
                error: ToolArgumentError::SchemaViolation,
                instance_path: "/path".to_owned(),
                constraint: raw_keyword.clone(),
            },
            timestamp(),
        );
        let ToolArgumentOutcome::Rejected {
            synthetic_result, ..
        } = unknown
        else {
            panic!("unknown schema constraint must be rejected")
        };
        assert_eq!(synthetic_result.details["constraint"], json!("schema"));
        assert!(
            !serde_json::to_string(&synthetic_result)
                .expect("serialize synthetic result")
                .contains(&raw_keyword)
        );
    }

    #[test]
    fn only_schema_valid_object_produces_validated_arguments() {
        let registry = schema_registry();
        let mut accumulator = ToolArgumentAccumulator::new();
        accumulator.append(r#"{"path":"notes.txt","limit":2}"#);
        let ToolArgumentOutcome::Validated(tool_call) =
            accumulator.finish("call-1", "read_file", &registry, timestamp())
        else {
            panic!("validated")
        };
        assert_eq!(
            tool_call.arguments.as_object(),
            json!({"path":"notes.txt","limit":2})
                .as_object()
                .expect("object")
        );
    }

    #[test]
    fn unknown_tools_and_length_stops_fail_closed_without_raw_arguments() {
        let registry = schema_registry();
        let raw_secret = r#"{"password":"do-not-echo"}"#;

        let mut unknown = ToolArgumentAccumulator::new();
        unknown.append(raw_secret);
        let outcome = unknown.finish("call-x", "unknown", &registry, timestamp());
        assert_rejected_without_raw(outcome, ToolArgumentError::SchemaViolation, raw_secret);

        let mut incomplete = ToolArgumentAccumulator::new();
        incomplete.append(r#"{"path":"valid.txt"}"#);
        let outcome = incomplete.reject_incomplete("call-y", "read_file", timestamp());
        assert_rejected_without_raw(outcome, ToolArgumentError::IncompleteResponse, "valid.txt");
    }

    #[test]
    fn compiled_schema_is_frozen_and_external_references_are_rejected() {
        let mut tools = vec![ToolDefinition {
            name: "read_file".to_owned(),
            description: String::new(),
            parameters: json!({
                "type":"object",
                "properties":{"path":{"type":"string"}},
                "required":["path"]
            }),
        }];
        let registry = FrozenToolSchemaRegistry::compile(&tools).expect("schema");
        tools[0].parameters["required"] = json!([]);

        let mut accumulator = ToolArgumentAccumulator::new();
        accumulator.append("{}");
        assert_rejected(
            accumulator.finish("call-1", "read_file", &registry, timestamp()),
            ToolArgumentError::SchemaViolation,
        );

        let external = [ToolDefinition {
            name: "remote".to_owned(),
            description: String::new(),
            parameters: json!({"$ref":"https://example.invalid/schema.json"}),
        }];
        assert!(matches!(
            FrozenToolSchemaRegistry::compile(&external),
            Err(FrozenSchemaError::ExternalReference(tool)) if tool == "remote"
        ));
    }

    #[test]
    fn argument_accumulation_is_bounded_and_rejection_does_not_echo_tail() {
        let registry = schema_registry();
        let mut accumulator = ToolArgumentAccumulator::new();
        let chunk = "x".repeat(MAX_TOOL_ARGUMENT_BYTES);
        accumulator.append(&chunk);
        let preview = accumulator.append("do-not-echo");
        assert_eq!(preview, json!({}));
        assert_eq!(accumulator.raw.capacity(), 0);
        assert_rejected_without_raw(
            accumulator.finish("call-1", "read_file", &registry, timestamp()),
            ToolArgumentError::TooLarge,
            "do-not-echo",
        );
    }

    #[test]
    fn length_stop_does_not_downgrade_too_large_rejection() {
        let mut accumulator = ToolArgumentAccumulator::new();
        accumulator.append(&"x".repeat(MAX_TOOL_ARGUMENT_BYTES + 1));
        assert_rejected(
            accumulator.reject_incomplete("call-1", "read_file", timestamp()),
            ToolArgumentError::TooLarge,
        );
    }

    #[test]
    fn unknown_instance_path_segments_are_redacted() {
        let allowed = HashSet::from(["known".to_owned()]);
        assert_eq!(
            safe_instance_path("/known/0/secret-key", &allowed),
            "/known/0/*"
        );
    }

    #[test]
    fn overlong_numeric_instance_path_collapses_to_safe_rejection() {
        let raw_path = format!("/{}", vec!["0"; 513].join("/"));
        let safe_path = safe_instance_path(&raw_path, &HashSet::new());
        assert_eq!(safe_path, "/*");
        assert!(is_safe_instance_path(&safe_path));

        let ToolArgumentOutcome::Rejected {
            rejected,
            synthetic_result,
        } = rejected_outcome(
            "call-deep".to_owned(),
            "nested_tool".to_owned(),
            RejectionDetail {
                error: ToolArgumentError::SchemaViolation,
                instance_path: safe_path,
                constraint: "type".to_owned(),
            },
            timestamp(),
        )
        else {
            panic!("schema violation must reject the tool call")
        };
        let event = ProviderEvent::ToolCallRejected {
            content_index: 0,
            rejected,
            synthetic_result,
        };
        let mut assembler = MessageAssembler::new();
        assembler.apply(&ProviderEvent::Start).expect("start");
        assembler
            .apply(&ProviderEvent::ToolCallStart { content_index: 0 })
            .expect("tool start");
        assembler
            .apply(&event)
            .expect("bounded rejection must remain a tool rejection");
        let synthetic_results = assembler.synthetic_results();
        assert_eq!(synthetic_results.len(), 1);
        assert!(synthetic_results[0].is_error);
        assert_eq!(synthetic_results[0].details["instance_path"], json!("/*"));
        let message = assembler
            .finish(metadata(StopReason::ToolUse))
            .expect("schema rejection must close as ToolUse, not provider Error");
        assert_eq!(message.stop_reason, StopReason::ToolUse);
        assert!(matches!(
            message.content.as_slice(),
            [AssistantContent::RejectedToolCall { rejected, .. }]
                if rejected.error == ToolArgumentError::SchemaViolation
        ));
        assert!(
            !serde_json::to_string(&event)
                .expect("serialize rejection")
                .contains(&raw_path)
        );
    }

    fn assert_rejected(outcome: ToolArgumentOutcome, expected: ToolArgumentError) {
        let ToolArgumentOutcome::Rejected {
            rejected,
            synthetic_result,
        } = outcome
        else {
            panic!("rejected")
        };
        assert_eq!(rejected.error, expected);
        assert!(synthetic_result.is_error);
    }

    fn assert_rejected_without_raw(
        outcome: ToolArgumentOutcome,
        expected: ToolArgumentError,
        raw_fragment: &str,
    ) {
        let serialized = serde_json::to_string(&outcome).expect("serialize outcome");
        assert_rejected(outcome, expected);
        assert!(!serialized.contains(raw_fragment));
    }
}
