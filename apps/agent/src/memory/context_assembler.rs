//! Assemble a `PromptContext` from runtime state, applying overflow fallback,
//! replay normalization (transform), 50KB user attachment truncation, and the
//! provider-native vs Sumi three-layer mode decision.

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::{Context as _, Result};
use sha2::Digest;

use crate::memory::estimate::{
    EstimateError, ProviderContextItemWithFootprint, TokenCalibration, estimate_public_messages,
    estimate_text_tokens, eviction_footprint_for_payload, sum_saved_footprints,
};
use crate::memory::overflow::{
    AssemblyMode, Overflow, USER_ATTACHMENT_TRUNCATION_BYTES, context_message_to_public,
};
use crate::memory::transform;
use crate::memory::{BatchState, L0Batch, ThreeLayerMemory};
#[cfg(test)]
use crate::provider::types::Usage;
use crate::provider::{
    ModelSpec,
    context_fingerprint::compute_context_fingerprint,
    types::{
        ApiProtocol, AssistantContent, AssistantMessage, ContextMessage, MemoryBlock, MemoryLayer,
        Message, PromptContext, ProviderContextAnchor, ProviderContextFragment,
        ProviderContextItem, ProviderContextPayload, ProviderOrigin, ToolDefinition, UserContent,
    },
};
use crate::tools::executor::ArtifactBrokerClient;

pub struct ContextAssembler {
    spec: ModelSpec,
    system_prompt: String,
    tools: Vec<ToolDefinition>,
    provider_context: Mutex<Vec<ProviderContextItemWithFootprint>>,
    memory_blocks: Mutex<Vec<MemoryBlock>>,
    broker: Option<ArtifactBrokerClient>,
    calib: Mutex<TokenCalibration>,
    mode: AssemblyMode,
    // The text itself may be arbitrarily large. Cache only its fixed-size
    // digest alongside the deterministic artifact identity so equal content
    // from different messages cannot reuse the wrong handle.
    attachment_handles: Mutex<HashMap<String, ([u8; 32], String)>>,
    three_layer_memory: Mutex<Option<ThreeLayerMemory>>,
}

/// The exact estimate used to construct one provider attempt. Keeping it in
/// the assembly result prevents a concurrent attempt from overwriting a
/// shared "last estimate" before terminal usage is recorded.
pub struct AssembledPrompt {
    pub prompt: PromptContext,
    pub uncalibrated_prompt_estimate: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderCallTrigger {
    FirstAfterUser,
    Continuation,
}

impl ContextAssembler {
    pub fn from_prompt_with_spec(
        prompt: PromptContext,
        spec: ModelSpec,
    ) -> Result<Self, EstimateError> {
        let provider_context: Vec<_> = prompt
            .provider_context
            .into_iter()
            .map(|item| {
                let footprint = eviction_footprint_for_payload(&spec, &item.payload)?;
                Ok(ProviderContextItemWithFootprint::new(item, footprint))
            })
            .collect::<Result<Vec<_>, EstimateError>>()?;

        Ok(Self {
            system_prompt: prompt.system_prompt,
            tools: prompt.tools,
            memory_blocks: Mutex::new(prompt.memory_blocks),
            provider_context: Mutex::new(provider_context),
            broker: None,
            calib: Mutex::new(TokenCalibration::default()),
            mode: AssemblyMode::SumiThreeLayer,
            attachment_handles: Mutex::new(HashMap::new()),
            spec,
            three_layer_memory: Mutex::new(None),
        })
    }

    pub fn with_broker(mut self, broker: ArtifactBrokerClient) -> Self {
        self.broker = Some(broker);
        self
    }

    pub fn set_broker(&mut self, broker: ArtifactBrokerClient) {
        self.broker = Some(broker);
    }

    pub fn with_calibration(mut self, calib: TokenCalibration) -> Self {
        *self.calib.get_mut().expect("calibration lock") = calib;
        self
    }

    pub fn with_mode(mut self, mode: AssemblyMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_three_layer_memory(mut self, memory: ThreeLayerMemory) -> Self {
        *self.calib.get_mut().expect("calibration lock") = memory.calibration();
        *self.three_layer_memory.get_mut().expect("memory lock") = Some(memory);
        self
    }

    pub fn spec(&self) -> &ModelSpec {
        &self.spec
    }

    pub fn calibration(&self) -> TokenCalibration {
        *self.calib.lock().expect("calibration lock")
    }

    /// Assemble a send-ready `PromptContext` for the configured destination.
    ///
    /// `attempt` preserves the direct-call compatibility surface. Production
    /// runner calls use `assemble_for_call_with_estimate` so a later user
    /// injection is not mistaken for a continuation merely because its global
    /// attempt ordinal is nonzero.
    pub async fn assemble(
        &self,
        context: &[ContextMessage],
        attempt: usize,
    ) -> Result<PromptContext> {
        Ok(self.assemble_with_estimate(context, attempt).await?.prompt)
    }

    /// Assemble a prompt and return the estimate belonging to that exact
    /// attempt. Callers that record terminal usage must retain this value.
    pub async fn assemble_with_estimate(
        &self,
        context: &[ContextMessage],
        attempt: usize,
    ) -> Result<AssembledPrompt> {
        let trigger = if attempt == 0 {
            ProviderCallTrigger::FirstAfterUser
        } else {
            ProviderCallTrigger::Continuation
        };
        self.assemble_for_call_with_estimate(context, trigger).await
    }

    pub(crate) async fn assemble_for_call_with_estimate(
        &self,
        context: &[ContextMessage],
        trigger: ProviderCallTrigger,
    ) -> Result<AssembledPrompt> {
        let overflow = Overflow::new(self.calibration(), self.mode);
        let is_first_user_call = trigger == ProviderCallTrigger::FirstAfterUser;

        let provider_context = self
            .provider_context
            .lock()
            .expect("provider context lock")
            .clone();
        let mut messages = overflow.recover_context_with_provider_context(
            context.to_vec(),
            is_first_user_call,
            &provider_context,
        )?;
        messages = transform::transform(&messages, &self.spec.origin());

        self.apply_user_attachment_truncation(&mut messages).await?;

        let (memory_blocks, selected_context, messages) =
            self.assemble_provider_view(messages, &self.spec.origin())?;

        let estimate =
            self.compute_uncalibrated_estimate(&memory_blocks, &messages, &selected_context)?;
        let selected_items: Vec<ProviderContextItem> =
            selected_context.into_iter().map(|it| it.item).collect();
        Ok(AssembledPrompt {
            prompt: PromptContext {
                system_prompt: self.system_prompt.clone(),
                memory_blocks,
                messages,
                provider_context: selected_items,
                tools: self.tools.clone(),
            },
            uncalibrated_prompt_estimate: estimate,
        })
    }

    /// Produce a smaller runtime context for an overflow retry.  This is
    /// intentionally not the first user call path, so it applies the ordinary
    /// L0 limit immediately.  When a `ThreeLayerMemory` is configured the
    /// caller is responsible for ensuring it already reflects the active
    /// context; promotion is used if shelf summaries are available.
    pub fn recover_overflow(
        &self,
        active_context: &[ContextMessage],
    ) -> Result<Vec<ContextMessage>> {
        let overflow = Overflow::new(self.calibration(), self.mode);
        let provider_context = self
            .provider_context
            .lock()
            .expect("provider context lock")
            .clone();

        // Promotion is a durable MemoryTransition: it removes batch membership,
        // crypto-erases provider context, and advances the apply cursor in the
        // same EventWriter transaction.  Do not synthesize a new open batch from
        // `active_context` here.  That context already spans sealed batches; a
        // local replacement would overlap those batches and replay them twice.
        //
        // The Session-owned maintainer is the only caller allowed to apply
        // completed shelves while Idle. At this API-preflight fallback we retain
        // only the bounded, replay-safe send-view recovery. It neither changes
        // durable membership nor tries to imitate promotion in process memory.
        overflow.recover_context_with_provider_context(
            active_context.to_vec(),
            false,
            &provider_context,
        )
    }

    /// Install the exact calibration value returned by a committed
    /// MessageEnd receipt. Runtime code must never independently replay the
    /// EMA because a crash between durable commit and local mutation would
    /// otherwise produce a different ratio after restart.
    pub(crate) fn install_committed_calibration(&self, ratio_bits: [u8; 8]) -> Result<()> {
        let ratio = f64::from_bits(u64::from_be_bytes(ratio_bits));
        let calibration =
            TokenCalibration::new(ratio).context("committed calibration ratio is invalid")?;
        *self.calib.lock().expect("calibration lock") = calibration;
        Ok(())
    }

    /// Refresh per-turn state from a terminal assistant turn.
    ///
    /// `message` is the authoritative assistant transcript as returned by the
    /// provider, `message_id` and `message_seq` are its durable identity, and
    /// `fragments` are the opaque provider-context pieces produced for this
    /// turn.  Reasoning fragments are anchored to the assistant message and
    /// converted to `ProviderContextItem`s with deterministic ordinals.
    pub fn apply_terminal(
        &self,
        message_id: &str,
        message_seq: u64,
        message: &AssistantMessage,
        fragments: &[ProviderContextFragment],
    ) -> Result<()> {
        let destination = self.spec.origin();
        let mut provider_context = self.provider_context.lock().expect("provider context lock");
        let new_items = fragments_to_items(
            &destination,
            &ProviderContextAnchor {
                message_id: message_id.to_owned(),
                message_seq,
            },
            &self.spec,
            fragments,
        )?;
        provider_context.extend(new_items);

        if let Some(memory) = self
            .three_layer_memory
            .lock()
            .expect("memory lock")
            .as_mut()
        {
            let context_message = ContextMessage::Persisted {
                id: message_id.to_owned(),
                seq: message_seq,
                message: Message::Assistant(message.clone()),
            };
            append_to_open_l0_batch(memory, context_message)?;
        }

        Ok(())
    }

    /// Replace the runtime `provider_context` wholesale.  Used when a caller
    /// (e.g. T17 hydration) supplies a freshly loaded set with its saved
    /// `EvictionFootprint` per item; the saved values are authoritative and are
    /// not recomputed.
    pub(crate) fn set_provider_context(
        &self,
        provider_context: Vec<ProviderContextItemWithFootprint>,
    ) {
        *self.provider_context.lock().expect("provider context lock") = provider_context;
    }

    /// Replace the runtime `memory_blocks` wholesale.  Used when a caller
    /// supplies freshly loaded L1/L2 projections.
    pub fn set_memory_blocks(&self, memory_blocks: Vec<MemoryBlock>) {
        *self.memory_blocks.lock().expect("memory blocks lock") = memory_blocks;
    }

    fn three_layer_memory_is_present(&self) -> bool {
        self.three_layer_memory
            .lock()
            .expect("memory lock")
            .is_some()
    }

    async fn apply_user_attachment_truncation(
        &self,
        messages: &mut [ContextMessage],
    ) -> Result<()> {
        for message in messages.iter_mut() {
            let id_prefix = attachment_id_prefix(&*message);
            let user_message = match message {
                ContextMessage::Persisted {
                    message: Message::User(user_message),
                    ..
                }
                | ContextMessage::Synthetic {
                    message: Message::User(user_message),
                } => user_message,
                _ => continue,
            };

            for (content_index, content) in user_message.content.iter_mut().enumerate() {
                let UserContent::Text { text } = content else {
                    continue;
                };
                if text.len() <= USER_ATTACHMENT_TRUNCATION_BYTES {
                    continue;
                }

                let full_text = std::mem::take(text);
                let artifact_id = format!("{id_prefix}-{content_index}");
                let content_digest: [u8; 32] = sha2::Sha256::digest(full_text.as_bytes()).into();

                let cached = {
                    let handles = self
                        .attachment_handles
                        .lock()
                        .expect("attachment handle lock");
                    handles.get(&artifact_id).cloned()
                };
                let handle = match cached {
                    Some((cached_digest, cached_handle)) if cached_digest == content_digest => {
                        cached_handle
                    }
                    Some(_) => {
                        return Err(anyhow::anyhow!(
                            "attachment identity {artifact_id} was reused with different content"
                        ));
                    }
                    None => {
                        let broker = self.broker.as_ref().ok_or_else(|| {
                            anyhow::anyhow!("oversized user input requires an attachment broker")
                        })?;
                        let handle = broker
                            .put_attachment(&artifact_id, &full_text)
                            .await
                            .with_context(|| format!("put_attachment failed for {artifact_id}"))?;
                        let mut handles = self
                            .attachment_handles
                            .lock()
                            .expect("attachment handle lock");
                        handles.insert(artifact_id, (content_digest, handle.clone()));
                        handle
                    }
                };

                *text = truncate_with_attachment_handle(&full_text, &handle);
            }
        }
        Ok(())
    }

    fn assemble_provider_view(
        &self,
        messages: Vec<ContextMessage>,
        destination: &ProviderOrigin,
    ) -> Result<(
        Vec<MemoryBlock>,
        Vec<ProviderContextItemWithFootprint>,
        Vec<ContextMessage>,
    )> {
        let provider_context = self
            .provider_context
            .lock()
            .expect("provider context lock")
            .clone();

        if self.mode == AssemblyMode::ProviderNative {
            let fingerprint = self.destination_fingerprint()?;
            if let Some(item) = find_native_window(&provider_context, destination, &fingerprint) {
                let through = native_coverage_through(item);
                let suffix: Vec<ContextMessage> = messages
                    .iter()
                    .filter(|message| match message {
                        ContextMessage::Persisted { seq, .. } => *seq > through,
                        ContextMessage::Synthetic { .. } => true,
                    })
                    .cloned()
                    .collect();

                let mut result_context = vec![item.clone()];
                let suffix_map: HashMap<&str, &ContextMessage> = suffix
                    .iter()
                    .filter_map(|message| match message {
                        ContextMessage::Persisted { id, .. } => Some((id.as_str(), message)),
                        ContextMessage::Synthetic { .. } => None,
                    })
                    .collect();
                let mut suffix_reasoning: Vec<_> = provider_context
                    .iter()
                    .filter(|it| {
                        matches_sumi_reasoning_with_anchor_check(
                            it,
                            destination,
                            &suffix_map,
                            through,
                        )
                    })
                    .collect();
                suffix_reasoning.sort_by_key(|entry| provider_context_order_key(entry));
                result_context.extend(suffix_reasoning.into_iter().cloned());

                return Ok((Vec::new(), result_context, suffix));
            }
        }

        let message_map: HashMap<&str, &ContextMessage> = messages
            .iter()
            .filter_map(|message| match message {
                ContextMessage::Persisted { id, .. } => Some((id.as_str(), message)),
                ContextMessage::Synthetic { .. } => None,
            })
            .collect();

        let mut selected_provider_context: Vec<_> = provider_context
            .iter()
            .filter(|it| matches_sumi_reasoning(it, destination, &message_map))
            .cloned()
            .collect();
        selected_provider_context.sort_by_key(provider_context_order_key);

        let memory_blocks = self.current_memory_blocks();
        Ok((memory_blocks, selected_provider_context, messages))
    }

    fn destination_fingerprint(&self) -> Result<String> {
        compute_context_fingerprint(&self.spec, &self.system_prompt, &self.tools)
            .map_err(Into::into)
    }

    fn current_memory_blocks(&self) -> Vec<MemoryBlock> {
        if let Some(memory) = self
            .three_layer_memory
            .lock()
            .expect("memory lock")
            .as_ref()
        {
            memory_blocks_from_three_layer(memory)
        } else {
            self.memory_blocks
                .lock()
                .expect("memory blocks lock")
                .clone()
        }
    }

    fn compute_uncalibrated_estimate(
        &self,
        memory_blocks: &[MemoryBlock],
        messages: &[ContextMessage],
        provider_context: &[ProviderContextItemWithFootprint],
    ) -> Result<u64> {
        let mut total = estimate_text_tokens(&self.system_prompt)?;

        let public: Vec<_> = messages.iter().map(context_message_to_public).collect();
        total = total
            .checked_add(estimate_public_messages(&public)?)
            .ok_or(EstimateError::ArithmeticOverflow)?;

        total = total
            .checked_add(estimate_tool_definitions(&self.tools)?)
            .ok_or(EstimateError::ArithmeticOverflow)?;

        for block in memory_blocks {
            total = total
                .checked_add(estimate_text_tokens(&block.text)?)
                .ok_or(EstimateError::ArithmeticOverflow)?;
        }

        total = total
            .checked_add(sum_saved_footprints(
                provider_context.iter().map(|it| it.footprint),
            )?)
            .ok_or(EstimateError::ArithmeticOverflow)?;

        Ok(total)
    }
}

fn public_estimate_for_context(messages: &[ContextMessage]) -> Result<u64> {
    let public: Vec<_> = messages.iter().map(context_message_to_public).collect();
    estimate_public_messages(&public).map_err(Into::into)
}

fn provider_context_footprint_for_messages(
    messages: &[ContextMessage],
    provider_context: &[ProviderContextItemWithFootprint],
) -> Result<u64> {
    let ids: std::collections::HashSet<&str> = messages
        .iter()
        .filter_map(|m| match m {
            ContextMessage::Persisted { id, .. } => Some(id.as_str()),
            ContextMessage::Synthetic { .. } => None,
        })
        .collect();
    let selected: Vec<_> = provider_context
        .iter()
        .filter(|it| {
            it.item
                .origin_message
                .as_ref()
                .is_some_and(|anchor| ids.contains(anchor.message_id.as_str()))
        })
        .map(|it| it.footprint)
        .collect();
    Ok(sum_saved_footprints(selected)?)
}

fn estimate_tool_definitions(tools: &[ToolDefinition]) -> Result<u64> {
    let mut total = 0u64;
    for tool in tools {
        total = total
            .checked_add(estimate_text_tokens(&tool.name)?)
            .ok_or(EstimateError::ArithmeticOverflow)?;
        total = total
            .checked_add(estimate_text_tokens(&tool.description)?)
            .ok_or(EstimateError::ArithmeticOverflow)?;
        let params = serde_json::to_string(&tool.parameters)
            .map_err(|e| EstimateError::SerializerFailure(e.to_string()))?;
        total = total
            .checked_add(estimate_text_tokens(&params)?)
            .ok_or(EstimateError::ArithmeticOverflow)?;
    }
    Ok(total)
}

fn append_to_open_l0_batch(memory: &mut ThreeLayerMemory, message: ContextMessage) -> Result<()> {
    // The simplest incremental policy: append to the open batch, or start one.
    if let Some(batch) = memory.l0_mut().back_mut()
        && batch.state == BatchState::Open
    {
        batch.messages.push(message);
        batch.est_tokens = public_estimate_for_context(&batch.messages)?;
        return Ok(());
    }
    let seq = memory.allocate_l0_batch_seq();
    let est = public_estimate_for_context(std::slice::from_ref(&message))?;
    memory.push_l0(L0Batch::new(vec![message], seq, 0, est));
    Ok(())
}

fn memory_blocks_from_three_layer(memory: &ThreeLayerMemory) -> Vec<MemoryBlock> {
    let mut blocks = Vec::new();
    let l2_summary = memory.l2().summary.expose();
    if !l2_summary.is_empty() {
        blocks.push(MemoryBlock {
            layer: MemoryLayer::L2,
            text: l2_summary.to_owned(),
            time_range: None,
        });
    }
    for entry in memory.l1() {
        blocks.push(MemoryBlock {
            layer: MemoryLayer::L1,
            text: entry.summary.expose().to_owned(),
            time_range: Some(entry.time_range),
        });
    }
    blocks
}

fn truncate_with_attachment_handle(full_text: &str, handle: &str) -> String {
    let mut split = USER_ATTACHMENT_TRUNCATION_BYTES;
    while split > 0 && !full_text.is_char_boundary(split) {
        split -= 1;
    }
    let prefix = &full_text[..split];
    let total_kb = full_text.len().div_ceil(1024);
    format!("{prefix}[全文 {total_kb}KB: {handle}]")
}

fn attachment_id_prefix(message: &ContextMessage) -> String {
    use sha2::{Digest, Sha256};

    match message {
        ContextMessage::Persisted { id, .. } => id.clone(),
        ContextMessage::Synthetic { message } => {
            let canonical = serde_json::to_vec(message)
                .expect("ContextMessage::Synthetic message must serialize canonically");
            let digest: [u8; 32] = Sha256::digest(canonical).into();
            format!("synthetic-{}", hex_digest(&digest))
        }
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn find_native_window<'a>(
    provider_context: &'a [ProviderContextItemWithFootprint],
    destination: &ProviderOrigin,
    fingerprint: &str,
) -> Option<&'a ProviderContextItemWithFootprint> {
    provider_context
        .iter()
        .rev()
        .find(|entry| match &entry.item.payload {
            ProviderContextPayload::OpenAiCompactedWindow { coverage, .. } => {
                matches_openai_protocol(destination.protocol)
                    && coverage.context_fingerprint == fingerprint
            }
            ProviderContextPayload::AnthropicCompaction { coverage, .. } => {
                destination.protocol == ApiProtocol::AnthropicMessages
                    && coverage.context_fingerprint == fingerprint
            }
            _ => false,
        })
}

fn native_coverage_through(entry: &ProviderContextItemWithFootprint) -> u64 {
    match &entry.item.payload {
        ProviderContextPayload::OpenAiCompactedWindow { coverage, .. }
        | ProviderContextPayload::AnthropicCompaction { coverage, .. } => {
            coverage.through_message_seq
        }
        _ => 0,
    }
}

fn matches_openai_protocol(protocol: ApiProtocol) -> bool {
    matches!(
        protocol,
        ApiProtocol::OpenAiChatCompletions | ApiProtocol::OpenAiResponses
    )
}

fn matches_sumi_reasoning(
    entry: &ProviderContextItemWithFootprint,
    destination: &ProviderOrigin,
    message_map: &HashMap<&str, &ContextMessage>,
) -> bool {
    // Sumi three-layer mode includes every reasoning item anchored to a
    // persisted assistant in the active send view; the canonical seq starts at
    // 1, so 0 means "no lower bound".
    matches_sumi_reasoning_with_anchor_check(entry, destination, message_map, 0)
}

fn matches_sumi_reasoning_with_anchor_check(
    entry: &ProviderContextItemWithFootprint,
    destination: &ProviderOrigin,
    message_map_or_suffix: &HashMap<&str, &ContextMessage>,
    through_message_seq: u64,
) -> bool {
    let item = &entry.item;
    let protocol = match &item.payload {
        ProviderContextPayload::EncryptedReasoning { protocol, .. } => *protocol,
        _ => return false,
    };
    if protocol != destination.protocol {
        return false;
    }
    let anchor = match &item.origin_message {
        Some(anchor) => anchor,
        None => return false,
    };
    if anchor.message_seq <= through_message_seq {
        return false;
    }
    let message = match message_map_or_suffix.get(anchor.message_id.as_str()) {
        Some(message) => *message,
        None => return false,
    };
    let assistant = match message {
        ContextMessage::Persisted {
            message: Message::Assistant(assistant),
            ..
        }
        | ContextMessage::Synthetic {
            message: Message::Assistant(assistant),
        } => assistant,
        _ => return false,
    };
    if assistant.origin != *destination {
        return false;
    }
    if assistant.model != destination.model {
        return false;
    }
    let wire = match item.wire_item_index {
        Some(wire) => wire,
        None => return false,
    };
    assistant.content.iter().any(|content| match content {
        AssistantContent::Thinking {
            wire_item_index, ..
        } => *wire_item_index == wire,
        _ => false,
    })
}

fn provider_context_order_key(entry: &ProviderContextItemWithFootprint) -> (u64, Option<u32>, u32) {
    let seq = entry
        .item
        .origin_message
        .as_ref()
        .map(|a| a.message_seq)
        .unwrap_or(0);
    (seq, entry.item.wire_item_index, entry.item.ordinal)
}

fn fragments_to_items(
    destination: &ProviderOrigin,
    anchor: &ProviderContextAnchor,
    spec: &ModelSpec,
    fragments: &[ProviderContextFragment],
) -> Result<Vec<ProviderContextItemWithFootprint>, EstimateError> {
    use std::collections::BTreeMap;

    let mut by_wire: BTreeMap<Option<u32>, Vec<(usize, &ProviderContextFragment)>> =
        BTreeMap::new();
    for (idx, fragment) in fragments.iter().enumerate() {
        by_wire
            .entry(fragment.wire_item_index)
            .or_default()
            .push((idx, fragment));
    }

    let mut items = Vec::with_capacity(fragments.len());
    for (wire, group) in by_wire {
        let mut group = group;
        group.sort_by_key(|(idx, _)| *idx);
        for (ordinal, (_, fragment)) in group.into_iter().enumerate() {
            let item = ProviderContextItem {
                origin_message: if wire.is_some() {
                    Some(anchor.clone())
                } else {
                    None
                },
                wire_item_index: wire,
                ordinal: ordinal as u32,
                provider_origin: destination.clone(),
                payload: fragment.payload.clone(),
            };
            // Native compaction windows and reasoning fragments from other
            // protocols do not belong in the per-destination runtime context.
            if matches!(
                item.payload,
                ProviderContextPayload::OpenAiCompactedWindow { .. }
                    | ProviderContextPayload::AnthropicCompaction { .. }
            ) {
                continue;
            }
            let protocol = match &item.payload {
                ProviderContextPayload::EncryptedReasoning { protocol, .. } => *protocol,
                _ => continue,
            };
            if protocol != destination.protocol {
                continue;
            }
            let footprint = eviction_footprint_for_payload(spec, &item.payload)?;
            items.push(ProviderContextItemWithFootprint::new(item, footprint));
        }
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::ConsolidatedMemory;
    use crate::memory::estimate::EvictionFootprint;
    use crate::provider::types::{StopReason, UserMessage};
    use chrono::Utc;

    fn model_spec() -> ModelSpec {
        ModelSpec::preset("kimi-k3").expect("preset")
    }

    fn responses_spec() -> ModelSpec {
        ModelSpec::preset("openai-responses").expect("preset")
    }

    fn responses_reasoning_payload(value: &str) -> ProviderContextPayload {
        ProviderContextPayload::EncryptedReasoning {
            protocol: ApiProtocol::OpenAiResponses,
            item: serde_json::json!({
                "type": "reasoning",
                "id": "rs_context_assembler",
                "encrypted_content": value,
                "summary": [],
            }),
        }
    }

    fn simple_prompt() -> PromptContext {
        PromptContext {
            system_prompt: "System.".to_owned(),
            memory_blocks: vec![],
            messages: vec![],
            provider_context: vec![],
            tools: vec![],
        }
    }

    fn assembler() -> ContextAssembler {
        ContextAssembler::from_prompt_with_spec(simple_prompt(), model_spec())
            .expect("valid prompt")
    }

    fn user(text: &str, seq: u64) -> ContextMessage {
        ContextMessage::Persisted {
            id: format!("msg-{seq}"),
            seq,
            message: Message::User(UserMessage {
                content: vec![UserContent::Text {
                    text: text.to_owned(),
                }],
                timestamp: Utc::now(),
            }),
        }
    }

    fn assistant_with_thinking(seq: u64, text: &str, wire: u32) -> ContextMessage {
        assistant_with_thinking_for(&model_spec(), seq, text, wire)
    }

    fn assistant_with_thinking_for(
        spec: &ModelSpec,
        seq: u64,
        text: &str,
        wire: u32,
    ) -> ContextMessage {
        ContextMessage::Persisted {
            id: format!("assistant-{seq}"),
            seq,
            message: Message::Assistant(AssistantMessage {
                content: vec![
                    AssistantContent::Text {
                        text: "public".to_owned(),
                        wire_item_index: wire.saturating_sub(1),
                    },
                    AssistantContent::Thinking {
                        thinking: text.to_owned(),
                        signature_field: "reasoning_content".to_owned(),
                        wire_item_index: wire,
                    },
                ],
                model: spec.id.clone(),
                provider: spec.provider.clone(),
                origin: spec.origin(),
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                provider_code: None,
                interrupted: false,
                timestamp: Utc::now(),
            }),
        }
    }

    #[test]
    fn attachment_truncation_prefix_keeps_char_boundary() {
        let text = "α".repeat(100_000); // two-byte chars
        let truncated = truncate_with_attachment_handle(&text, "artifact://x");
        assert!(truncated.is_char_boundary(USER_ATTACHMENT_TRUNCATION_BYTES));
        assert!(truncated.contains("artifact://x"));
    }

    #[test]
    fn synthetic_attachment_identity_is_content_addressed() {
        let first = ContextMessage::Synthetic {
            message: Message::User(UserMessage {
                content: vec![UserContent::Text {
                    text: "first".to_owned(),
                }],
                timestamp: Utc::now(),
            }),
        };
        let second = ContextMessage::Synthetic {
            message: Message::User(UserMessage {
                content: vec![UserContent::Text {
                    text: "second".to_owned(),
                }],
                timestamp: Utc::now(),
            }),
        };
        assert_ne!(attachment_id_prefix(&first), attachment_id_prefix(&second));
    }

    #[tokio::test]
    async fn attachment_cache_binds_handle_to_artifact_id_and_content_digest() {
        let full_text = "x".repeat(USER_ATTACHMENT_TRUNCATION_BYTES + 1);
        let digest: [u8; 32] = sha2::Sha256::digest(full_text.as_bytes()).into();
        let handle = "artifact://conversation/attachments/msg-1-0".to_owned();
        let assembler = assembler();
        assembler
            .attachment_handles
            .lock()
            .expect("attachment handle lock")
            .insert("msg-1-0".to_owned(), (digest, handle.clone()));

        let mut replay = vec![user(&full_text, 1)];
        assembler
            .apply_user_attachment_truncation(&mut replay)
            .await
            .expect("same identity and content reuse the cached handle");
        assert!(matches!(
            &replay[0],
            ContextMessage::Persisted {
                message: Message::User(UserMessage { content, .. }),
                ..
            } if matches!(&content[0], UserContent::Text { text } if text.contains(&handle))
        ));

        let mut equal_content_different_message = vec![user(&full_text, 2)];
        let missing_broker = assembler
            .apply_user_attachment_truncation(&mut equal_content_different_message)
            .await
            .expect_err("equal content under a different artifact ID must not reuse the handle");
        assert!(
            missing_broker
                .to_string()
                .contains("requires an attachment broker")
        );

        let mut conflicting_replay =
            vec![user(&"y".repeat(USER_ATTACHMENT_TRUNCATION_BYTES + 1), 1)];
        let conflict = assembler
            .apply_user_attachment_truncation(&mut conflicting_replay)
            .await
            .expect_err("one artifact ID must never alias different content");
        assert!(
            conflict
                .to_string()
                .contains("attachment identity msg-1-0 was reused with different content")
        );
    }

    #[test]
    fn destination_fingerprint_is_stable_and_model_sensitive() {
        let assembler = assembler();
        let f1 = assembler.destination_fingerprint().expect("fingerprint");
        let mut other = model_spec();
        other.id = "other".to_owned();
        let assembler2 =
            ContextAssembler::from_prompt_with_spec(simple_prompt(), other).expect("valid prompt");
        let f2 = assembler2.destination_fingerprint().expect("fingerprint");
        assert_ne!(f1, f2);
    }

    #[test]
    fn destination_fingerprint_rejects_protocol_compat_mismatch() {
        let mut spec = model_spec();
        spec.compat = crate::provider::model::ProtocolCompat::Responses(
            crate::provider::model::ResponsesCompat {
                supports_store: false,
                supports_encrypted_reasoning: false,
                supports_native_compact: false,
                supports_streaming: true,
            },
        );
        let assembler =
            ContextAssembler::from_prompt_with_spec(simple_prompt(), spec).expect("valid prompt");
        assert!(assembler.destination_fingerprint().is_err());
    }

    #[tokio::test]
    async fn calibration_installs_exact_committed_ratio_bits() {
        let assembler = assembler();
        assembler
            .install_committed_calibration(1.3_f64.to_bits().to_be_bytes())
            .unwrap();
        assert_eq!(assembler.calibration().ratio().to_bits(), 1.3_f64.to_bits());
    }

    #[tokio::test]
    async fn provider_native_mode_matches_adapter_fingerprint() {
        let mut spec = model_spec();
        spec.protocol = ApiProtocol::OpenAiResponses;
        spec.compat = crate::provider::model::ProtocolCompat::Responses(
            crate::provider::model::ResponsesCompat {
                supports_store: false,
                supports_encrypted_reasoning: false,
                supports_native_compact: true,
                supports_streaming: true,
            },
        );
        let assembler = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec)
            .expect("valid prompt")
            .with_mode(AssemblyMode::ProviderNative);
        let _fingerprint = assembler.destination_fingerprint().expect("fingerprint");
        let prompt = assembler
            .assemble(&[user("hello", 1)], 1)
            .await
            .expect("assemble");
        assert!(!prompt.messages.is_empty());
        assert_eq!(prompt.memory_blocks, Vec::new());
    }

    #[tokio::test]
    async fn provider_native_window_keeps_suffix_reasoning() {
        let spec = responses_spec();
        let coverage = crate::provider::types::NativeCompactionCoverage {
            through_message_seq: 2,
            context_fingerprint: ContextAssembler::from_prompt_with_spec(
                simple_prompt(),
                spec.clone(),
            )
            .expect("valid prompt")
            .destination_fingerprint()
            .expect("fingerprint"),
        };
        let native = ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![serde_json::json!({
                    "type": "message",
                    "role": "assistant",
                    "content": [],
                })],
                coverage,
            },
        };
        let reasoning = ProviderContextItem {
            origin_message: Some(ProviderContextAnchor {
                message_id: "assistant-3".to_owned(),
                message_seq: 3,
            }),
            wire_item_index: Some(1),
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: responses_reasoning_payload("opaque"),
        };
        let prompt = PromptContext {
            system_prompt: "System.".to_owned(),
            memory_blocks: vec![],
            messages: vec![user("first", 1), user("second", 2), user("third", 3)],
            provider_context: vec![native, reasoning],
            tools: vec![],
        };
        let assembler = ContextAssembler::from_prompt_with_spec(prompt, spec.clone())
            .expect("valid prompt")
            .with_mode(AssemblyMode::ProviderNative);
        let result = assembler
            .assemble(
                &[
                    user("first", 1),
                    user("second", 2),
                    user("third", 3),
                    assistant_with_thinking_for(&spec, 3, "private", 1),
                ],
                1,
            )
            .await
            .expect("assemble");
        assert_eq!(result.provider_context.len(), 2);
        assert!(matches!(
            &result.provider_context[0].payload,
            ProviderContextPayload::OpenAiCompactedWindow { .. }
        ));
        assert!(matches!(
            &result.provider_context[1].payload,
            ProviderContextPayload::EncryptedReasoning { .. }
        ));
        assert_eq!(result.messages.len(), 2);
        assert!(
            result
                .messages
                .iter()
                .any(|m| matches!(m, ContextMessage::Persisted { seq: 3, .. }))
        );
    }

    #[tokio::test]
    async fn sumi_reasoning_requires_matching_thinking_block() {
        let spec = responses_spec();
        let mut prompt = simple_prompt();
        prompt.provider_context = vec![ProviderContextItem {
            origin_message: Some(ProviderContextAnchor {
                message_id: "assistant-1".to_owned(),
                message_seq: 1,
            }),
            wire_item_index: Some(5),
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: responses_reasoning_payload("opaque"),
        }];
        let assembler =
            ContextAssembler::from_prompt_with_spec(prompt, spec.clone()).expect("valid prompt");
        let result = assembler
            .assemble(&[assistant_with_thinking_for(&spec, 1, "private", 5)], 1)
            .await
            .expect("assemble");
        assert_eq!(result.provider_context.len(), 1);

        let mut prompt2 = simple_prompt();
        prompt2.provider_context = vec![ProviderContextItem {
            origin_message: Some(ProviderContextAnchor {
                message_id: "assistant-1".to_owned(),
                message_seq: 1,
            }),
            wire_item_index: Some(99),
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: responses_reasoning_payload("opaque"),
        }];
        let assembler2 = ContextAssembler::from_prompt_with_spec(prompt2, responses_spec())
            .expect("valid prompt");
        let result2 = assembler2
            .assemble(
                &[assistant_with_thinking_for(
                    &responses_spec(),
                    1,
                    "private",
                    5,
                )],
                1,
            )
            .await
            .expect("assemble");
        assert!(result2.provider_context.is_empty());
    }

    #[tokio::test]
    async fn provider_context_is_sorted_canonically() {
        let spec = responses_spec();
        let mut prompt = simple_prompt();
        prompt.provider_context = vec![
            ProviderContextItem {
                origin_message: Some(ProviderContextAnchor {
                    message_id: "assistant-2".to_owned(),
                    message_seq: 2,
                }),
                wire_item_index: Some(0),
                ordinal: 0,
                provider_origin: spec.origin(),
                payload: responses_reasoning_payload("second"),
            },
            ProviderContextItem {
                origin_message: Some(ProviderContextAnchor {
                    message_id: "assistant-1".to_owned(),
                    message_seq: 1,
                }),
                wire_item_index: Some(0),
                ordinal: 0,
                provider_origin: spec.origin(),
                payload: responses_reasoning_payload("first"),
            },
        ];
        let assembler =
            ContextAssembler::from_prompt_with_spec(prompt, spec.clone()).expect("valid prompt");
        let assistant2 = |seq| ContextMessage::Persisted {
            id: format!("assistant-{seq}"),
            seq,
            message: Message::Assistant(AssistantMessage {
                content: vec![AssistantContent::Thinking {
                    thinking: "x".to_owned(),
                    signature_field: "s".to_owned(),
                    wire_item_index: 0,
                }],
                model: spec.id.clone(),
                provider: spec.provider.clone(),
                origin: spec.origin(),
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                provider_code: None,
                interrupted: false,
                timestamp: Utc::now(),
            }),
        };
        let result = assembler
            .assemble(&[assistant2(1), assistant2(2)], 1)
            .await
            .expect("assemble");
        assert_eq!(
            result.provider_context[0]
                .origin_message
                .as_ref()
                .unwrap()
                .message_seq,
            1
        );
        assert_eq!(
            result.provider_context[1]
                .origin_message
                .as_ref()
                .unwrap()
                .message_seq,
            2
        );
    }

    #[test]
    fn recover_overflow_accounts_for_eviction_footprint() {
        let spec = responses_spec();
        let mut prompt = simple_prompt();
        let long_text = "x".repeat(200_000);
        // Provider context anchored to the first user message adds a heavy footprint.
        prompt.provider_context = vec![ProviderContextItem {
            origin_message: Some(ProviderContextAnchor {
                message_id: "msg-1".to_owned(),
                message_seq: 1,
            }),
            wire_item_index: Some(0),
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: responses_reasoning_payload(&"x".repeat(200_000)),
        }];
        let assembler =
            ContextAssembler::from_prompt_with_spec(prompt, spec).expect("valid prompt");
        let recovered = assembler
            .recover_overflow(&[user(&long_text, 1), user("ack", 2), user("second", 3)])
            .expect("recover");
        // The footprint should force dropping the first (oversized) user message.
        assert!(
            recovered
                .iter()
                .all(|m| !matches!(m, ContextMessage::Persisted { seq: 1, .. }))
        );
    }

    #[test]
    fn overflow_recovery_never_rebuilds_l0_from_overlapping_runtime_history() {
        let mut memory = ThreeLayerMemory::new(
            ConsolidatedMemory {
                summary: crate::memory::DecryptedMemorySummary::new(String::new()),
                est_tokens: 0,
            },
            TokenCalibration::default(),
        );
        let first = user(&"a".repeat(100_000), 1);
        let second = user("b", 2);
        let newest = user("c", 3);
        let mut sealed = L0Batch::new(
            vec![first.clone()],
            memory.allocate_l0_batch_seq(),
            0,
            25_000,
        );
        sealed.state = BatchState::Compacted;
        let sealed_id = sealed.id;
        memory.push_l0(sealed);
        let open = L0Batch::new(vec![second.clone()], memory.allocate_l0_batch_seq(), 0, 1);
        let open_id = open.id;
        memory.push_l0(open);

        let assembler = ContextAssembler::from_prompt_with_spec(simple_prompt(), model_spec())
            .expect("valid prompt")
            .with_three_layer_memory(memory);
        let recovered = assembler
            .recover_overflow(&[first, second, newest.clone()])
            .expect("bounded fallback recovery");

        assert!(
            recovered.contains(&newest),
            "active user must survive fallback"
        );
        let memory = assembler.three_layer_memory.lock().expect("memory lock");
        let l0 = memory.as_ref().expect("configured memory").l0();
        assert_eq!(l0.len(), 2, "fallback must not append a full-history batch");
        assert_eq!(l0[0].id, sealed_id);
        assert_eq!(l0[1].id, open_id);
    }

    #[test]
    fn apply_terminal_anchors_reasoning_fragments() {
        let spec = responses_spec();
        let assembler = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec.clone())
            .expect("valid prompt");
        let assistant = AssistantMessage {
            content: vec![],
            model: spec.id.clone(),
            provider: spec.provider.clone(),
            origin: spec.origin(),
            usage: Usage::default(),
            stop_reason: StopReason::Stop,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: Utc::now(),
        };
        let fragments = vec![
            ProviderContextFragment {
                wire_item_index: Some(0),
                payload: responses_reasoning_payload("reasoning-0"),
            },
            ProviderContextFragment {
                wire_item_index: Some(0),
                payload: responses_reasoning_payload("reasoning-1"),
            },
        ];
        assembler
            .apply_terminal("m1", 1, &assistant, &fragments)
            .expect("apply");
        let ctx = assembler.provider_context.lock().expect("lock");
        assert_eq!(ctx.len(), 2);
        assert_eq!(ctx[0].item.ordinal, 0);
        assert_eq!(ctx[1].item.ordinal, 1);
        assert_eq!(ctx[0].item.wire_item_index, Some(0));
        assert_eq!(
            ctx[0].item.origin_message.as_ref().unwrap().message_id,
            "m1"
        );
    }

    #[tokio::test]
    async fn memory_blocks_derive_from_three_layer_memory() {
        let spec = model_spec();
        let memory = ThreeLayerMemory::new(
            ConsolidatedMemory {
                summary: crate::memory::DecryptedMemorySummary::new("L2 summary".to_owned()),
                est_tokens: 10,
            },
            TokenCalibration::default(),
        );
        let assembler = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec)
            .expect("valid prompt")
            .with_three_layer_memory(memory);
        let result = assembler.assemble(&[], 1).await.expect("assemble");
        assert_eq!(result.memory_blocks.len(), 1);
        assert_eq!(result.memory_blocks[0].layer, MemoryLayer::L2);
        assert_eq!(result.memory_blocks[0].text, "L2 summary");
    }

    #[tokio::test]
    async fn hydrated_provider_context_influences_assembled_turn_context() {
        let spec = responses_spec();
        let item = ProviderContextItem {
            origin_message: Some(ProviderContextAnchor {
                message_id: "assistant-2".to_owned(),
                message_seq: 2,
            }),
            wire_item_index: Some(1),
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: responses_reasoning_payload("hydrated-opaque"),
        };
        // A saved legacy footprint that is much larger than the freshly
        // recomputed value would be; assembly must use the hydrated saved value.
        let saved_footprint =
            EvictionFootprint::from_saved(1, 0, 1_000_000).expect("valid saved footprint");
        let prompt = PromptContext {
            system_prompt: "System.".to_owned(),
            memory_blocks: vec![],
            messages: vec![],
            provider_context: vec![item.clone()],
            tools: vec![],
        };
        let assembler =
            ContextAssembler::from_prompt_with_spec(prompt, spec.clone()).expect("valid prompt");
        assembler.set_provider_context(vec![ProviderContextItemWithFootprint::new(
            item,
            saved_footprint,
        )]);

        // Include a preceding user message so overflow cannot drop the only
        // message and the anchored assistant survives for provider-context selection.
        let result = assembler
            .assemble_with_estimate(
                &[
                    user("latest", 1),
                    assistant_with_thinking_for(&spec, 2, "private", 1),
                ],
                1,
            )
            .await
            .expect("assemble");

        assert_eq!(result.prompt.provider_context.len(), 1);
        assert_eq!(result.prompt.provider_context[0].wire_item_index, Some(1));
        assert!(
            result.uncalibrated_prompt_estimate > 1_000_000,
            "saved hydrated footprint must influence the assembled estimate"
        );
    }

    #[test]
    fn provider_context_footprint_drives_overflow_replay() {
        let spec = responses_spec();
        let long = "x".repeat(50_000);
        let item = ProviderContextItem {
            origin_message: Some(ProviderContextAnchor {
                message_id: "msg-1".to_owned(),
                message_seq: 1,
            }),
            wire_item_index: Some(0),
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: responses_reasoning_payload(&long),
        };
        let saved_footprint =
            EvictionFootprint::from_saved(1, 0, 500_000).expect("valid saved footprint");
        let prompt = PromptContext {
            system_prompt: "System.".to_owned(),
            memory_blocks: vec![],
            messages: vec![],
            provider_context: vec![item.clone()],
            tools: vec![],
        };
        let assembler =
            ContextAssembler::from_prompt_with_spec(prompt, spec).expect("valid prompt");
        assembler.set_provider_context(vec![ProviderContextItemWithFootprint::new(
            item,
            saved_footprint,
        )]);

        let recovered = assembler
            .recover_overflow(&[user(&long, 1), user("second", 2), user("third", 3)])
            .expect("recover");

        assert!(
            recovered
                .iter()
                .all(|m| !matches!(m, ContextMessage::Persisted { seq: 1, .. })),
            "heavy provider-context footprint must let overflow drop the anchored message"
        );
        assert!(
            recovered
                .iter()
                .any(|m| matches!(m, ContextMessage::Persisted { seq: 3, .. })),
            "latest user message must survive replay"
        );
    }
}
