//! Assemble a `PromptContext` from runtime state, applying overflow fallback,
//! replay normalization (transform), 50KB user attachment truncation, and the
//! provider-native vs Sumi three-layer mode decision.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use anyhow::{Context as _, Result};
use sha2::Digest;

use crate::memory::ThreeLayerMemory;
use crate::memory::estimate::{
    EstimateError, ProviderContextItemWithFootprint, TokenCalibration, estimate_public_messages,
    estimate_text_tokens, eviction_footprint_for_payload, sum_saved_footprints,
};
use crate::memory::overflow::{
    AssemblyMode, Overflow, USER_ATTACHMENT_TRUNCATION_BYTES, context_message_to_public,
};
use crate::memory::transform;
#[cfg(test)]
use crate::provider::types::Usage;
use crate::provider::{
    ModelSpec, ProtocolCompat,
    context_fingerprint::compute_context_fingerprint,
    types::{
        ApiProtocol, AssistantContent, AssistantMessage, ContextMessage, MemoryBlock, MemoryLayer,
        Message, PromptContext, ProviderContextAnchor, ProviderContextFragment,
        ProviderContextItem, ProviderContextPayload, ProviderOrigin, ToolDefinition, UserContent,
        VerifiedReplayProvenance,
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
    hydrated_three_layer: Mutex<Option<HydratedThreeLayer>>,
}

struct HydratedThreeLayer {
    memory: ThreeLayerMemory,
    transcript_through_seq: u64,
}

struct BoundProviderContext {
    items: Vec<ProviderContextItemWithFootprint>,
    native_window: Option<BoundNativeWindow>,
}

struct BoundNativeWindow {
    item: ProviderContextItemWithFootprint,
    through_message_seq: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReplayProvenance {
    kind: ReplayProvenanceKind,
    seal: [u8; 32],
}

#[derive(Clone, Debug, PartialEq)]
enum ReplayProvenanceKind {
    SumiNormalized {
        provider_origin: ProviderOrigin,
        canonical_through_seq: Option<u64>,
    },
    ProviderNativeExact {
        provider_origin: ProviderOrigin,
        native_coverage_through_seq: u64,
        canonical_suffix_through_seq: Option<u64>,
    },
}

impl ReplayProvenance {
    pub(crate) fn verify(
        &self,
        send_view_digest: [u8; 32],
    ) -> Result<VerifiedReplayProvenance, String> {
        if self.seal != replay_binding_seal(&self.kind, send_view_digest)? {
            return Err("bound replay send view changed after assembly".into());
        }
        Ok(match &self.kind {
            ReplayProvenanceKind::SumiNormalized {
                provider_origin,
                canonical_through_seq,
            } => VerifiedReplayProvenance::SumiNormalized {
                provider_origin: provider_origin.clone(),
                canonical_through_seq: *canonical_through_seq,
            },
            ReplayProvenanceKind::ProviderNativeExact {
                provider_origin,
                native_coverage_through_seq,
                canonical_suffix_through_seq,
            } => VerifiedReplayProvenance::ProviderNativeExact {
                provider_origin: provider_origin.clone(),
                native_coverage_through_seq: *native_coverage_through_seq,
                canonical_suffix_through_seq: *canonical_suffix_through_seq,
            },
        })
    }
}

fn replay_binding_seal(
    kind: &ReplayProvenanceKind,
    send_view_digest: [u8; 32],
) -> Result<[u8; 32], String> {
    let mut seal = sha2::Sha256::new();
    seal.update(b"sumi.prompt-replay-provenance-seal.v2\0");
    match kind {
        ReplayProvenanceKind::SumiNormalized {
            provider_origin,
            canonical_through_seq,
        } => {
            update_seal_frame(&mut seal, b"sumi_normalized")?;
            update_seal_origin(&mut seal, provider_origin)?;
            update_seal_optional_seq(&mut seal, *canonical_through_seq)?;
        }
        ReplayProvenanceKind::ProviderNativeExact {
            provider_origin,
            native_coverage_through_seq,
            canonical_suffix_through_seq,
        } => {
            update_seal_frame(&mut seal, b"provider_native_exact")?;
            update_seal_origin(&mut seal, provider_origin)?;
            update_seal_frame(&mut seal, &native_coverage_through_seq.to_be_bytes())?;
            update_seal_optional_seq(&mut seal, *canonical_suffix_through_seq)?;
        }
    }
    update_seal_frame(&mut seal, &send_view_digest)?;
    Ok(seal.finalize().into())
}

fn update_seal_origin(
    seal: &mut sha2::Sha256,
    provider_origin: &ProviderOrigin,
) -> Result<(), String> {
    update_seal_frame(seal, provider_origin.provider_instance_id.as_bytes())?;
    let protocol: &[u8] = match provider_origin.protocol {
        ApiProtocol::OpenAiChatCompletions => b"open_ai_chat_completions",
        ApiProtocol::OpenAiResponses => b"open_ai_responses",
        ApiProtocol::AnthropicMessages => b"anthropic_messages",
    };
    update_seal_frame(seal, protocol)?;
    update_seal_frame(seal, provider_origin.model.as_bytes())
}

fn update_seal_frame(seal: &mut sha2::Sha256, value: &[u8]) -> Result<(), String> {
    seal.update(
        u64::try_from(value.len())
            .map_err(|_| "replay provenance frame length exceeds u64".to_owned())?
            .to_be_bytes(),
    );
    seal.update(value);
    Ok(())
}

fn update_seal_optional_seq(seal: &mut sha2::Sha256, value: Option<u64>) -> Result<(), String> {
    let Some(value) = value else {
        return update_seal_frame(seal, &[0]);
    };
    let mut frame = [0_u8; 9];
    frame[0] = 1;
    frame[1..].copy_from_slice(&value.to_be_bytes());
    update_seal_frame(seal, &frame)
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
            hydrated_three_layer: Mutex::new(None),
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

    #[cfg(test)]
    fn with_three_layer_memory(
        mut self,
        memory: ThreeLayerMemory,
        transcript_through_seq: u64,
    ) -> Self {
        *self.calib.get_mut().expect("calibration lock") = memory.calibration();
        *self.hydrated_three_layer.get_mut().expect("memory lock") = Some(HydratedThreeLayer {
            memory,
            transcript_through_seq,
        });
        self
    }

    /// Install one authenticated Store snapshot as the fallback send-view
    /// source.
    ///
    /// The caller transcript remains the canonical life log. The snapshot
    /// binds the exact live L0 membership to the greatest transcript sequence
    /// observed during the same hydration transaction; later persisted
    /// messages are overlaid until the next idle refresh. Provider-native mode
    /// uses this exact view only when no authenticated native window matches
    /// the destination.
    pub(crate) fn install_hydrated_memory(
        &self,
        memory: ThreeLayerMemory,
        hydrated_messages: &[ContextMessage],
        provider_context: Vec<ProviderContextItemWithFootprint>,
    ) -> Result<()> {
        let transcript_through_seq = hydrated_transcript_cutoff(hydrated_messages)?;
        for message in memory.l0().iter().flat_map(|batch| &batch.messages) {
            let ContextMessage::Persisted { seq, .. } = message else {
                anyhow::bail!("hydrated L0 contains a synthetic message");
            };
            if *seq > transcript_through_seq {
                anyhow::bail!(
                    "hydrated L0 message sequence {seq} exceeds transcript cutoff {transcript_through_seq}"
                );
            }
        }

        let calibration = memory.calibration();
        *self.calib.lock().expect("calibration lock") = calibration;
        *self.provider_context.lock().expect("provider context lock") = provider_context;
        *self.hydrated_three_layer.lock().expect("memory lock") = Some(HydratedThreeLayer {
            memory,
            transcript_through_seq,
        });
        Ok(())
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
        let destination = self.spec.origin();

        let provider_context = self.bind_provider_context(&destination)?;
        let active_view =
            self.send_source_messages(context, provider_context.native_window.as_ref())?;
        let mut messages = overflow.recover_context_with_provider_context(
            active_view,
            is_first_user_call,
            &provider_context.items,
        )?;
        let canonical_through_seq = normalized_replay_through(&messages)?;
        messages = transform::transform(&messages, &destination);

        self.apply_user_attachment_truncation(&mut messages).await?;

        let (memory_blocks, selected_context, messages) =
            self.assemble_provider_view(messages, &destination, &provider_context);

        let estimate =
            self.compute_uncalibrated_estimate(&memory_blocks, &messages, &selected_context)?;
        let selected_items: Vec<ProviderContextItem> =
            selected_context.into_iter().map(|it| it.item).collect();
        let mut prompt = PromptContext::new(
            self.system_prompt.clone(),
            memory_blocks,
            messages,
            selected_items,
            self.tools.clone(),
        );
        if let Some(native_window) = &provider_context.native_window {
            bind_provider_native_exact_replay(
                &mut prompt,
                destination,
                native_window.through_message_seq,
                canonical_through_seq,
            )?;
        } else {
            bind_sumi_normalized_replay(&mut prompt, destination, canonical_through_seq)?;
        }
        Ok(AssembledPrompt {
            prompt,
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
        let provider_context = self.bind_provider_context(&self.spec.origin())?;

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
        let active_view =
            self.send_source_messages(active_context, provider_context.native_window.as_ref())?;
        overflow.recover_context_with_provider_context(active_view, false, &provider_context.items)
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
        _message: &AssistantMessage,
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

    fn active_send_messages(&self, life_log: &[ContextMessage]) -> Result<Vec<ContextMessage>> {
        let hydrated = self.hydrated_three_layer.lock().expect("memory lock");
        let Some(hydrated) = hydrated.as_ref() else {
            return Ok(life_log.to_vec());
        };

        let mut active = Vec::new();
        let mut active_ids = std::collections::HashSet::new();
        for message in hydrated
            .memory
            .l0()
            .iter()
            .flat_map(|batch| batch.messages.iter())
        {
            let ContextMessage::Persisted { id, .. } = message else {
                anyhow::bail!("three-layer L0 contains a synthetic message");
            };
            if !active_ids.insert(id.clone()) {
                anyhow::bail!("three-layer L0 contains duplicate message id {id}");
            }
            active.push(message.clone());
        }

        for message in life_log {
            match message {
                ContextMessage::Persisted { id, seq, .. }
                    if *seq > hydrated.transcript_through_seq =>
                {
                    if active_ids.insert(id.clone()) {
                        active.push(message.clone());
                    }
                }
                ContextMessage::Synthetic { .. } => active.push(message.clone()),
                ContextMessage::Persisted { .. } => {}
            }
        }
        Ok(active)
    }

    fn send_source_messages(
        &self,
        life_log: &[ContextMessage],
        native_window: Option<&BoundNativeWindow>,
    ) -> Result<Vec<ContextMessage>> {
        if let Some(native_window) = native_window {
            return select_native_suffix(life_log, native_window.through_message_seq);
        }
        self.active_send_messages(life_log)
    }

    fn bind_provider_context(&self, destination: &ProviderOrigin) -> Result<BoundProviderContext> {
        let items = self
            .provider_context
            .lock()
            .expect("provider context lock")
            .clone();
        let native_window = if self.mode == AssemblyMode::ProviderNative
            && supports_native_compaction(&self.spec)
        {
            let fingerprint = self.destination_fingerprint()?;
            find_native_window(&items, destination, &fingerprint).map(|item| BoundNativeWindow {
                through_message_seq: native_coverage_through(item),
                item: item.clone(),
            })
        } else {
            None
        };
        Ok(BoundProviderContext {
            items,
            native_window,
        })
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
        provider_context: &BoundProviderContext,
    ) -> (
        Vec<MemoryBlock>,
        Vec<ProviderContextItemWithFootprint>,
        Vec<ContextMessage>,
    ) {
        if let Some(native_window) = &provider_context.native_window {
            let suffix_map: HashMap<&str, &ContextMessage> = messages
                .iter()
                .filter_map(|message| match message {
                    ContextMessage::Persisted { id, .. } => Some((id.as_str(), message)),
                    ContextMessage::Synthetic { .. } => None,
                })
                .collect();
            let mut suffix_reasoning: Vec<_> = provider_context
                .items
                .iter()
                .filter(|it| {
                    matches_sumi_reasoning_with_anchor_check(
                        it,
                        destination,
                        &suffix_map,
                        native_window.through_message_seq,
                    )
                })
                .collect();
            suffix_reasoning.sort_by_key(|entry| provider_context_order_key(entry));
            let mut result_context = vec![native_window.item.clone()];
            result_context.extend(suffix_reasoning.into_iter().cloned());

            return (Vec::new(), result_context, messages);
        }

        let message_map: HashMap<&str, &ContextMessage> = messages
            .iter()
            .filter_map(|message| match message {
                ContextMessage::Persisted { id, .. } => Some((id.as_str(), message)),
                ContextMessage::Synthetic { .. } => None,
            })
            .collect();

        let mut selected_provider_context: Vec<_> = provider_context
            .items
            .iter()
            .filter(|it| matches_sumi_reasoning(it, destination, &message_map))
            .cloned()
            .collect();
        selected_provider_context.sort_by_key(provider_context_order_key);

        let memory_blocks = self.current_memory_blocks();
        (memory_blocks, selected_provider_context, messages)
    }

    fn destination_fingerprint(&self) -> Result<String> {
        compute_context_fingerprint(&self.spec, &self.system_prompt, &self.tools)
            .map_err(Into::into)
    }

    fn current_memory_blocks(&self) -> Vec<MemoryBlock> {
        if let Some(hydrated) = self
            .hydrated_three_layer
            .lock()
            .expect("memory lock")
            .as_ref()
        {
            memory_blocks_from_three_layer(&hydrated.memory)
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

fn hydrated_transcript_cutoff(messages: &[ContextMessage]) -> Result<u64> {
    let mut previous = 0_u64;
    for message in messages {
        let ContextMessage::Persisted { seq, .. } = message else {
            anyhow::bail!("hydrated transcript contains a synthetic message");
        };
        if *seq <= previous {
            anyhow::bail!(
                "hydrated transcript sequence is not strictly increasing: {seq} after {previous}"
            );
        }
        previous = *seq;
    }
    Ok(previous)
}

fn normalized_replay_through(messages: &[ContextMessage]) -> Result<Option<u64>> {
    let mut previous = None;
    for message in messages {
        let ContextMessage::Persisted { seq, .. } = message else {
            continue;
        };
        if *seq == 0 || previous.is_some_and(|value| value >= *seq) {
            anyhow::bail!(
                "normalized replay persistence is not positive and strictly increasing: {seq} after {previous:?}"
            );
        }
        previous = Some(*seq);
    }
    Ok(previous)
}

fn bind_sumi_normalized_replay(
    prompt: &mut PromptContext,
    provider_origin: ProviderOrigin,
    canonical_through_seq: Option<u64>,
) -> Result<()> {
    ensure_unbound_replay(prompt)?;
    let normalized_through = normalized_replay_through(&prompt.messages)?;
    if normalized_through > canonical_through_seq {
        anyhow::bail!("normalized Sumi replay exceeds its authenticated canonical watermark");
    }
    let send_view_digest = prompt
        .replay_send_view_digest()
        .map_err(anyhow::Error::msg)?;
    let kind = ReplayProvenanceKind::SumiNormalized {
        provider_origin,
        canonical_through_seq,
    };
    let seal = replay_binding_seal(&kind, send_view_digest).map_err(anyhow::Error::msg)?;
    prompt.replay_provenance = Some(ReplayProvenance { kind, seal });
    Ok(())
}

fn bind_provider_native_exact_replay(
    prompt: &mut PromptContext,
    provider_origin: ProviderOrigin,
    native_coverage_through_seq: u64,
    canonical_suffix_through_seq: Option<u64>,
) -> Result<()> {
    ensure_unbound_replay(prompt)?;
    if native_coverage_through_seq == 0 {
        anyhow::bail!("native replay coverage must be greater than zero");
    }
    if canonical_suffix_through_seq.is_some_and(|through| through <= native_coverage_through_seq) {
        anyhow::bail!("native canonical suffix watermark must be greater than native coverage");
    }
    let normalized_through = normalized_replay_through(&prompt.messages)?;
    if normalized_through.is_some_and(|through| through <= native_coverage_through_seq) {
        anyhow::bail!("normalized native replay contains covered persisted history");
    }
    if normalized_through > canonical_suffix_through_seq {
        anyhow::bail!(
            "normalized native replay exceeds its authenticated canonical suffix watermark"
        );
    }

    let matching_native = prompt
        .provider_context
        .iter()
        .filter(|item| {
            if item.provider_origin != provider_origin {
                return false;
            }
            match &item.payload {
                ProviderContextPayload::OpenAiCompactedWindow { coverage, .. }
                    if provider_origin.protocol == ApiProtocol::OpenAiResponses =>
                {
                    coverage.through_message_seq == native_coverage_through_seq
                }
                ProviderContextPayload::AnthropicCompaction { coverage, .. }
                    if provider_origin.protocol == ApiProtocol::AnthropicMessages =>
                {
                    coverage.through_message_seq == native_coverage_through_seq
                }
                ProviderContextPayload::OpenAiCompactedWindow { .. }
                | ProviderContextPayload::AnthropicCompaction { .. }
                | ProviderContextPayload::EncryptedReasoning { .. } => false,
            }
        })
        .count();
    let native_count = prompt
        .provider_context
        .iter()
        .filter(|item| {
            matches!(
                item.payload,
                ProviderContextPayload::OpenAiCompactedWindow { .. }
                    | ProviderContextPayload::AnthropicCompaction { .. }
            )
        })
        .count();
    if matching_native != 1 || native_count != 1 {
        anyhow::bail!("native replay provenance requires exactly one matching bound native window");
    }

    let send_view_digest = prompt
        .replay_send_view_digest()
        .map_err(anyhow::Error::msg)?;
    let kind = ReplayProvenanceKind::ProviderNativeExact {
        provider_origin,
        native_coverage_through_seq,
        canonical_suffix_through_seq,
    };
    let seal = replay_binding_seal(&kind, send_view_digest).map_err(anyhow::Error::msg)?;
    prompt.replay_provenance = Some(ReplayProvenance { kind, seal });
    Ok(())
}

fn ensure_unbound_replay(prompt: &PromptContext) -> Result<()> {
    if prompt.replay_provenance.is_some() {
        anyhow::bail!("prompt replay provenance is already bound");
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn bind_sumi_replay_for_test(
    prompt: &mut PromptContext,
    canonical_through_seq: Option<u64>,
) -> Result<()> {
    let mut origins = prompt
        .provider_context
        .iter()
        .map(|item| item.provider_origin.clone());
    let provider_origin = origins
        .next()
        .ok_or_else(|| anyhow::anyhow!("test replay origin is absent"))?;
    if origins.any(|origin| origin != provider_origin) {
        anyhow::bail!("test replay origin is ambiguous");
    }
    bind_sumi_normalized_replay(prompt, provider_origin, canonical_through_seq)
}

#[cfg(test)]
pub(crate) fn bind_sumi_replay_for_origin_test(
    prompt: &mut PromptContext,
    provider_origin: ProviderOrigin,
    canonical_through_seq: Option<u64>,
) -> Result<()> {
    bind_sumi_normalized_replay(prompt, provider_origin, canonical_through_seq)
}

#[cfg(test)]
pub(crate) fn bind_native_replay_for_test(
    prompt: &mut PromptContext,
    provider_origin: ProviderOrigin,
    native_coverage_through_seq: u64,
    canonical_suffix_through_seq: Option<u64>,
) -> Result<()> {
    bind_provider_native_exact_replay(
        prompt,
        provider_origin,
        native_coverage_through_seq,
        canonical_suffix_through_seq,
    )
}

fn select_native_suffix(
    life_log: &[ContextMessage],
    through_message_seq: u64,
) -> Result<Vec<ContextMessage>> {
    let mut coverage_index = None;
    let mut covered_tool_call_ids = HashSet::new();
    for (index, message) in life_log.iter().enumerate() {
        let ContextMessage::Persisted { seq, message, .. } = message else {
            continue;
        };
        if *seq == through_message_seq && coverage_index.replace(index).is_some() {
            anyhow::bail!("native coverage sequence {through_message_seq} appears more than once");
        }
        if *seq <= through_message_seq
            && let Message::Assistant(assistant) = message
        {
            for content in &assistant.content {
                match content {
                    AssistantContent::ToolCall { tool_call, .. } => {
                        covered_tool_call_ids.insert(tool_call.id.clone());
                    }
                    AssistantContent::RejectedToolCall { rejected, .. } => {
                        covered_tool_call_ids.insert(rejected.id.clone());
                    }
                    AssistantContent::Text { .. } | AssistantContent::Thinking { .. } => {}
                }
            }
        }
    }
    let coverage_index = coverage_index.ok_or_else(|| {
        anyhow::anyhow!(
            "native coverage sequence {through_message_seq} is absent from the canonical life log"
        )
    })?;
    let first_persisted_suffix = life_log.iter().enumerate().find_map(|(index, message)| {
        matches!(
            message,
            ContextMessage::Persisted { seq, .. } if *seq > through_message_seq
        )
        .then_some(index)
    });

    let mut suffix = Vec::new();
    for (index, message) in life_log.iter().enumerate() {
        match message {
            ContextMessage::Persisted { seq, .. } if *seq > through_message_seq => {
                suffix.push(message.clone());
            }
            ContextMessage::Persisted { .. } => {}
            ContextMessage::Synthetic { message: synthetic } => {
                if index <= coverage_index
                    || transform::is_generated_replay_artifact(message)
                    || matches!(
                        synthetic,
                        Message::ToolResult(result)
                            if covered_tool_call_ids.contains(&result.tool_call_id)
                    )
                {
                    continue;
                }
                if first_persisted_suffix.is_some_and(|first| index > first) {
                    anyhow::bail!(
                        "native suffix contains an unsequenced synthetic input after persisted history"
                    );
                }
                suffix.push(message.clone());
            }
        }
    }
    Ok(suffix)
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
    let mut matching = provider_context.iter().filter(|entry| {
        if entry.item.provider_origin != *destination
            || entry.item.origin_message.is_some()
            || entry.item.wire_item_index.is_some()
            || entry.item.ordinal != 0
        {
            return false;
        }
        match &entry.item.payload {
            ProviderContextPayload::OpenAiCompactedWindow { coverage, .. } => {
                matches_openai_protocol(destination.protocol)
                    && coverage.context_fingerprint == fingerprint
            }
            ProviderContextPayload::AnthropicCompaction { coverage, .. } => {
                destination.protocol == ApiProtocol::AnthropicMessages
                    && coverage.context_fingerprint == fingerprint
            }
            _ => false,
        }
    });
    let item = matching.next()?;
    matching.next().is_none().then_some(item)
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

fn supports_native_compaction(spec: &ModelSpec) -> bool {
    match &spec.compat {
        ProtocolCompat::Responses(compat) => compat.supports_native_compact,
        ProtocolCompat::Anthropic(compat) => compat.supports_native_compact,
        ProtocolCompat::Chat(_) => false,
    }
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
                retention_owner: anchor.clone(),
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
    use std::sync::Arc;

    use super::*;
    use crate::memory::estimate::EvictionFootprint;
    use crate::memory::{BatchState, ConsolidatedMemory, L0Batch};
    use crate::provider::{
        RequestOptions,
        types::{
            RejectedToolCall, StopReason, ToolArgumentError, ToolResultMessage, UserMessage,
            ValidatedToolArguments,
        },
    };
    use chrono::Utc;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixListener,
    };

    fn model_spec() -> ModelSpec {
        ModelSpec::preset("kimi-k3").expect("preset")
    }

    fn responses_spec() -> ModelSpec {
        ModelSpec::preset("openai-responses").expect("preset")
    }

    fn anthropic_spec() -> ModelSpec {
        ModelSpec::preset("anthropic").expect("preset")
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

    fn provider_context_owner(message_id: &str, message_seq: u64) -> ProviderContextAnchor {
        ProviderContextAnchor {
            message_id: message_id.to_owned(),
            message_seq,
        }
    }

    fn simple_prompt() -> PromptContext {
        PromptContext {
            system_prompt: "System.".to_owned(),
            memory_blocks: vec![],
            messages: vec![],
            provider_context: vec![],
            tools: vec![],
            replay_provenance: None,
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

    fn rejected_assistant(spec: &ModelSpec, seq: u64, id: &str) -> ContextMessage {
        ContextMessage::Persisted {
            id: format!("assistant-{seq}"),
            seq,
            message: Message::Assistant(AssistantMessage {
                content: vec![AssistantContent::RejectedToolCall {
                    rejected: RejectedToolCall {
                        id: id.to_owned(),
                        name: "fixture".to_owned(),
                        error: ToolArgumentError::SchemaViolation,
                    },
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
        }
    }

    fn tool_result(seq: u64, call_id: &str) -> ContextMessage {
        ContextMessage::Persisted {
            id: format!("tool-result-{seq}"),
            seq,
            message: Message::ToolResult(ToolResultMessage {
                tool_call_id: call_id.to_owned(),
                tool_name: "fixture".to_owned(),
                content: vec![UserContent::Text {
                    text: "fixture result".to_owned(),
                }],
                details: serde_json::Value::Null,
                is_error: false,
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

    #[tokio::test]
    async fn oversized_context_assembly_round_trips_through_real_artifact_broker() {
        use crate::{
            runtime::contracts::RpcIdentity,
            tools::executor::{
                ArtifactBroker, ArtifactBrokerClient, ArtifactOperation, RpcFrame, RpcRequest,
                decode_rpc_line, encode_rpc_frame,
            },
        };

        const PAID: &str = "0198f0f4-9b72-7000-8000-000000000001";
        let fixture_root =
            std::env::temp_dir().join(format!("sumi-context-real-broker-{}", uuid::Uuid::now_v7()));
        let artifact_root = fixture_root.join("artifacts");
        std::fs::create_dir_all(&artifact_root).expect("create broker fixture root");
        let socket = fixture_root.join("broker.sock");
        let listener = UnixListener::bind(&socket).expect("bind broker fixture socket");
        let identity = RpcIdentity::from_wire(PAID, 7, "context-broker").expect("fixture identity");
        let broker = Arc::new(ArtifactBroker::open(&artifact_root).expect("open real broker"));
        let server_identity = identity.clone();
        let server = tokio::spawn(async move {
            // Begin, one <=64KiB append, and Finish are separate authenticated
            // client exchanges for this >50KiB attachment.
            for _ in 0..3 {
                let (stream, _) = listener.accept().await.expect("accept broker request");
                let (read, mut write) = stream.into_split();
                let mut read = BufReader::new(read);
                let mut line = Vec::new();
                read.read_until(b'\n', &mut line)
                    .await
                    .expect("read broker request");
                assert_eq!(line.pop(), Some(b'\n'));
                let request: RpcRequest<ArtifactOperation> =
                    decode_rpc_line(&line, &server_identity).expect("decode broker request");
                let response = broker
                    .execute(server_identity.personality_agent_id(), request.operation)
                    .expect("real broker operation");
                let encoded = encode_rpc_frame(&RpcFrame::Terminal {
                    personality_agent_id: server_identity.personality_agent_id().clone(),
                    generation: server_identity.generation().as_u64(),
                    nonce: server_identity.nonce().as_str().to_owned(),
                    request_id: request.request_id,
                    result: Ok(response),
                })
                .expect("encode broker response");
                write
                    .write_all(&encoded)
                    .await
                    .expect("write broker response");
                write.shutdown().await.expect("close broker response");
            }
        });

        let oversized = "x".repeat(USER_ATTACHMENT_TRUNCATION_BYTES + 1);
        let assembler = assembler().with_broker(ArtifactBrokerClient::new(&socket, identity));
        let assembled = assembler
            .assemble_for_call_with_estimate(
                &[user(&oversized, 1)],
                ProviderCallTrigger::FirstAfterUser,
            )
            .await
            .expect("assemble through real broker");
        let attached_text = assembled
            .prompt
            .messages
            .iter()
            .find_map(|message| match message {
                ContextMessage::Persisted {
                    message: Message::User(user),
                    ..
                } => user.content.iter().find_map(|content| match content {
                    UserContent::Text { text } => Some(text),
                    _ => None,
                }),
                _ => None,
            })
            .expect("assembled user attachment");
        let expected_handle = format!("artifact://{PAID}/attachments/msg-1-0");
        assert!(attached_text.contains(&expected_handle));
        assert_ne!(attached_text, &oversized);
        assert!(attached_text.starts_with(&oversized[..USER_ATTACHMENT_TRUNCATION_BYTES]));

        server.await.expect("real broker fixture join");
        assert_eq!(
            std::fs::read_to_string(artifact_root.join(PAID).join("attachments/msg-1-0"))
                .expect("read persisted attachment"),
            oversized
        );
        std::fs::remove_dir_all(&fixture_root).expect("remove real broker fixture");
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
            retention_owner: provider_context_owner("native-owner-2", 2),
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
            retention_owner: provider_context_owner("assistant-4", 4),
            origin_message: Some(ProviderContextAnchor {
                message_id: "assistant-4".to_owned(),
                message_seq: 4,
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
            replay_provenance: None,
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
                    assistant_with_thinking_for(&spec, 4, "private", 1),
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
                .any(|m| matches!(m, ContextMessage::Persisted { seq: 4, .. }))
        );
    }

    #[tokio::test]
    async fn native_window_cuts_covered_tool_call_before_transform_and_responses_serialization() {
        let spec = responses_spec();
        let covered_call_id = "covered-call";
        let covered = ContextMessage::Persisted {
            id: "assistant-1".to_owned(),
            seq: 1,
            message: Message::Assistant(AssistantMessage {
                content: vec![AssistantContent::ToolCall {
                    tool_call: crate::provider::types::ToolCall {
                        id: covered_call_id.to_owned(),
                        name: "fixture".to_owned(),
                        arguments: serde_json::from_value::<ValidatedToolArguments>(
                            serde_json::json!({}),
                        )
                        .expect("object tool arguments"),
                    },
                    wire_item_index: 0,
                }],
                model: spec.id.clone(),
                provider: spec.provider.clone(),
                origin: spec.origin(),
                usage: Usage::default(),
                stop_reason: StopReason::ToolUse,
                error_message: None,
                provider_code: None,
                interrupted: false,
                timestamp: Utc::now(),
            }),
        };
        let uncovered = user("after native coverage", 2);
        let fingerprint = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec.clone())
            .expect("fingerprint assembler")
            .destination_fingerprint()
            .expect("fingerprint");
        let native = ProviderContextItem {
            retention_owner: provider_context_owner("assistant-1", 1),
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![serde_json::json!({
                    "id": "fc-covered",
                    "type": "function_call",
                    "call_id": covered_call_id,
                    "name": "fixture",
                    "arguments": "{}",
                })],
                coverage: crate::provider::types::NativeCompactionCoverage {
                    through_message_seq: 1,
                    context_fingerprint: fingerprint,
                },
            },
        };
        let prompt = PromptContext {
            system_prompt: "System.".to_owned(),
            memory_blocks: Vec::new(),
            messages: Vec::new(),
            provider_context: vec![native.clone()],
            tools: Vec::new(),
            replay_provenance: None,
        };
        let assembler = ContextAssembler::from_prompt_with_spec(prompt, spec.clone())
            .expect("native assembler")
            .with_mode(AssemblyMode::ProviderNative);

        let assembled = assembler
            .assemble(&[covered, uncovered.clone()], 1)
            .await
            .expect("assemble exact native suffix");
        assert_eq!(assembled.provider_context, vec![native]);
        assert_eq!(assembled.messages, vec![uncovered]);
        let assembled_json = serde_json::to_string(&assembled).expect("serialize assembled prompt");
        assert!(!assembled_json.contains(transform::MISSING_TOOL_RESULT_TEXT));
        assert!(!assembled_json.contains("missing_tool_result"));

        let request = crate::provider::adapters::responses::build_request(
            &spec,
            &assembled,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("serialize native window before uncovered suffix");
        let input = request["input"].as_array().expect("Responses input array");
        assert_eq!(input.len(), 2);
        assert_eq!(input[0]["type"], "function_call");
        assert_eq!(input[0]["call_id"], covered_call_id);
        assert_eq!(input[1]["role"], "user");
        assert_eq!(input[1]["content"][0]["text"], "after native coverage");
        let request_json = request.to_string();
        assert!(!request_json.contains("function_call_output"));
        assert!(!request_json.contains(transform::MISSING_TOOL_RESULT_TEXT));
        assert!(!request_json.contains("missing_tool_result"));
    }

    #[tokio::test]
    async fn responses_native_window_precedes_normalized_rejection_suffix() {
        let spec = responses_spec();
        let life_log = vec![
            user("covered", 1),
            user("before rejected call", 2),
            rejected_assistant(&spec, 3, "rejected-call"),
            user("after rejected call", 4),
        ];
        let fingerprint = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec.clone())
            .expect("fingerprint assembler")
            .destination_fingerprint()
            .expect("fingerprint");
        let native = ProviderContextItem {
            retention_owner: provider_context_owner("native-owner-1", 1),
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![serde_json::json!({
                    "id": "cmp-rejection",
                    "type": "compaction",
                    "encrypted_content": "opaque-rejection",
                })],
                coverage: crate::provider::types::NativeCompactionCoverage {
                    through_message_seq: 1,
                    context_fingerprint: fingerprint,
                },
            },
        };
        let assembler = ContextAssembler::from_prompt_with_spec(
            PromptContext {
                provider_context: vec![native.clone()],
                ..simple_prompt()
            },
            spec.clone(),
        )
        .expect("native assembler")
        .with_mode(AssemblyMode::ProviderNative);

        let assembled = assembler
            .assemble(&life_log, 1)
            .await
            .expect("assemble normalized rejection suffix");
        assert_eq!(assembled.provider_context, vec![native]);
        assert!(matches!(
            assembled.messages.as_slice(),
            [
                ContextMessage::Persisted { seq: 2, .. },
                ContextMessage::Synthetic {
                    message: Message::User(_),
                },
                ContextMessage::Persisted { seq: 4, .. },
            ]
        ));

        let request = crate::provider::adapters::responses::build_request(
            &spec,
            &assembled,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("retain native window across normalized rejection suffix");
        let input = request["input"].as_array().expect("Responses input array");
        assert_eq!(input.len(), 4);
        assert_eq!(input[0]["id"], "cmp-rejection");
        assert_eq!(input[1]["content"][0]["text"], "before rejected call");
        assert!(
            input[2]["content"][0]["text"]
                .as_str()
                .is_some_and(|text| text.contains("引数検証に失敗"))
        );
        assert_eq!(input[3]["content"][0]["text"], "after rejected call");
    }

    #[tokio::test]
    async fn anthropic_native_collapse_carries_full_canonical_watermark_to_next_turn() {
        let spec = anthropic_spec();
        let mut life_log = vec![
            user("covered", 1),
            user("before rejected call", 2),
            rejected_assistant(&spec, 3, "rejected-call"),
            tool_result(4, "rejected-call"),
        ];
        let fingerprint = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec.clone())
            .expect("fingerprint assembler")
            .destination_fingerprint()
            .expect("fingerprint");
        let native = ProviderContextItem {
            retention_owner: provider_context_owner("native-owner-1", 1),
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: ProviderContextPayload::AnthropicCompaction {
                block: serde_json::json!({
                    "type": "compaction",
                    "content": "opaque-rejection",
                }),
                coverage: crate::provider::types::NativeCompactionCoverage {
                    through_message_seq: 1,
                    context_fingerprint: fingerprint,
                },
            },
        };
        let assembler = ContextAssembler::from_prompt_with_spec(
            PromptContext {
                provider_context: vec![native.clone()],
                ..simple_prompt()
            },
            spec.clone(),
        )
        .expect("native assembler")
        .with_mode(AssemblyMode::ProviderNative);

        let assembled = assembler
            .assemble(&life_log, 1)
            .await
            .expect("assemble normalized rejection suffix");
        assert_eq!(assembled.provider_context, vec![native.clone()]);
        assert!(matches!(
            assembled.messages.as_slice(),
            [
                ContextMessage::Persisted { seq: 2, .. },
                ContextMessage::Synthetic {
                    message: Message::User(_),
                },
            ]
        ));
        assert!(matches!(
            assembled
                .verified_replay_provenance()
                .expect("verify assembler replay binding"),
            Some(VerifiedReplayProvenance::ProviderNativeExact {
                native_coverage_through_seq: 1,
                canonical_suffix_through_seq: Some(4),
                ..
            })
        ));
        let returned_coverage =
            crate::provider::adapters::anthropic::request_coverage(&spec, &assembled, true)
                .expect("derive authenticated compaction coverage")
                .expect("persisted canonical coverage");
        assert_eq!(returned_coverage.through_message_seq, 4);

        let request = crate::provider::adapters::anthropic::build_request(
            &spec,
            &assembled,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("retain native block across normalized rejection suffix");
        let messages = request["messages"]
            .as_array()
            .expect("Anthropic messages array");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["role"], "assistant");
        assert_eq!(messages[0]["content"][0]["type"], "compaction");
        assert_eq!(messages[0]["content"][0]["content"], "opaque-rejection");
        assert_eq!(messages[1]["role"], "user");
        let serialized = serde_json::to_string(messages).expect("serialize Anthropic messages");
        let before = serialized
            .find("before rejected call")
            .expect("before marker");
        let rejection = serialized.find("引数検証に失敗").expect("rejection marker");
        assert!(before < rejection);
        assert!(!serialized.contains("fixture result"));
        assert!(request.get("context_management").is_some());

        life_log.push(user("next turn", 5));
        let next_native = ProviderContextItem {
            retention_owner: provider_context_owner("native-owner-4", 4),
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: ProviderContextPayload::AnthropicCompaction {
                block: serde_json::json!({
                    "type": "compaction",
                    "content": "opaque-through-4",
                }),
                coverage: returned_coverage,
            },
        };
        let next_assembler = ContextAssembler::from_prompt_with_spec(
            PromptContext {
                provider_context: vec![next_native],
                ..simple_prompt()
            },
            spec.clone(),
        )
        .expect("next-turn native assembler")
        .with_mode(AssemblyMode::ProviderNative);
        let next = next_assembler
            .assemble(&life_log, 1)
            .await
            .expect("assemble only the uncovered next turn");
        assert!(matches!(
            next.messages.as_slice(),
            [ContextMessage::Persisted { seq: 5, .. }]
        ));
        assert!(
            next.messages
                .iter()
                .all(|message| !transform::is_generated_replay_artifact(message))
        );
        let next_request = crate::provider::adapters::anthropic::build_request(
            &spec,
            &next,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("serialize next-turn native suffix");
        let next_serialized = next_request.to_string();
        assert!(next_serialized.contains("next turn"));
        assert!(!next_serialized.contains("引数検証に失敗"));
    }

    #[tokio::test]
    async fn anthropic_sumi_fallback_authorizes_rejection_and_orphan_diagnostics() {
        let spec = anthropic_spec();
        let life_log = vec![
            user("before rejected call", 1),
            rejected_assistant(&spec, 2, "rejected-call"),
            tool_result(3, "rejected-call"),
            user("before orphan result", 4),
            tool_result(5, "orphan-call"),
        ];
        let assembler = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec.clone())
            .expect("Sumi fallback assembler")
            .with_mode(AssemblyMode::ProviderNative);
        let assembled = assembler
            .assemble(&life_log, 1)
            .await
            .expect("assemble normalized Sumi fallback");
        assert!(assembled.provider_context.is_empty());
        let diagnostics = assembled
            .messages
            .iter()
            .filter(|message| transform::is_generated_replay_artifact(message))
            .count();
        assert_eq!(diagnostics, 2);
        assert!(matches!(
            assembled
                .verified_replay_provenance()
                .expect("verify assembler replay binding"),
            Some(VerifiedReplayProvenance::SumiNormalized {
                canonical_through_seq: Some(5),
                ..
            })
        ));
        let coverage =
            crate::provider::adapters::anthropic::request_coverage(&spec, &assembled, true)
                .expect("derive authenticated fallback coverage")
                .expect("canonical persisted coverage");
        assert_eq!(coverage.through_message_seq, 5);

        let request = crate::provider::adapters::anthropic::build_request(
            &spec,
            &assembled,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("serialize normalized fallback without native validation");
        let serialized = request.to_string();
        assert!(serialized.contains("引数検証に失敗"));
        assert!(serialized.contains("対応するツール呼び出しがない"));
        assert!(request.get("context_management").is_some());
    }

    #[tokio::test]
    async fn responses_native_synthetic_only_suffix_is_exact_and_authenticated() {
        let spec = responses_spec();
        let life_log = vec![
            user("covered", 1),
            rejected_assistant(&spec, 2, "rejected-call"),
            tool_result(3, "rejected-call"),
        ];
        let fingerprint = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec.clone())
            .expect("fingerprint assembler")
            .destination_fingerprint()
            .expect("fingerprint");
        let native = ProviderContextItem {
            retention_owner: provider_context_owner("native-owner-1", 1),
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![serde_json::json!({
                    "id": "cmp-synthetic-only",
                    "type": "compaction",
                    "encrypted_content": "opaque-synthetic-only",
                })],
                coverage: crate::provider::types::NativeCompactionCoverage {
                    through_message_seq: 1,
                    context_fingerprint: fingerprint,
                },
            },
        };
        let assembler = ContextAssembler::from_prompt_with_spec(
            PromptContext {
                provider_context: vec![native],
                ..simple_prompt()
            },
            spec.clone(),
        )
        .expect("native assembler")
        .with_mode(AssemblyMode::ProviderNative);
        let assembled = assembler
            .assemble(&life_log, 1)
            .await
            .expect("assemble synthetic-only native suffix");
        assert!(matches!(
            assembled.messages.as_slice(),
            [ContextMessage::Synthetic {
                message: Message::User(_),
            }]
        ));
        assert!(transform::is_generated_replay_artifact(
            &assembled.messages[0]
        ));
        assert!(matches!(
            assembled
                .verified_replay_provenance()
                .expect("verify assembler replay binding"),
            Some(VerifiedReplayProvenance::ProviderNativeExact {
                native_coverage_through_seq: 1,
                canonical_suffix_through_seq: Some(3),
                ..
            })
        ));

        let request = crate::provider::adapters::responses::build_request(
            &spec,
            &assembled,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("serialize exact synthetic-only suffix");
        let input = request["input"].as_array().expect("Responses input");
        assert_eq!(input.len(), 2);
        assert_eq!(input[0]["id"], "cmp-synthetic-only");
        assert!(
            input[1]["content"][0]["text"]
                .as_str()
                .is_some_and(|text| text.contains("引数検証に失敗"))
        );
    }

    #[test]
    fn replay_binding_survives_clone_is_stripped_by_serde_and_rejects_mutation() {
        let provider_origin = responses_spec().origin();
        let mut prompt = simple_prompt();
        prompt.messages = vec![user("bound", 1)];
        let unbound_send_view =
            serde_json::to_vec(&prompt).expect("serialize unbound provider send view");
        bind_sumi_normalized_replay(&mut prompt, provider_origin.clone(), Some(1))
            .expect("bind Sumi replay");
        assert_eq!(
            serde_json::to_vec(&prompt).expect("serialize bound provider send view"),
            unbound_send_view,
            "private replay authority must not perturb provider/overflow send-view digests"
        );

        let cloned = prompt.clone();
        assert!(matches!(
            cloned
                .verified_replay_provenance()
                .expect("clone retains valid binding"),
            Some(VerifiedReplayProvenance::SumiNormalized {
                ref provider_origin,
                canonical_through_seq: Some(1),
            }) if provider_origin == &responses_spec().origin()
        ));
        let mut different_destination = provider_origin.clone();
        different_destination.model.push_str("-different");
        assert_eq!(
            cloned
                .verified_replay_provenance_for(&different_destination)
                .expect_err("bound destination must remain exact"),
            "bound replay destination does not match the selected provider_origin"
        );

        let encoded = serde_json::to_value(&prompt).expect("serialize prompt");
        assert!(encoded.get("replay_provenance").is_none());
        let decoded: PromptContext = serde_json::from_value(encoded).expect("deserialize prompt");
        assert_eq!(
            decoded
                .verified_replay_provenance()
                .expect("deserialized prompt is unbound"),
            None
        );

        let mut mutated = cloned;
        mutated.system_prompt.push_str(" changed");
        assert_eq!(
            mutated
                .verified_replay_provenance()
                .expect_err("mutation must invalidate replay authority"),
            "bound replay send view changed after assembly"
        );

        let mut origin_mutated = prompt.clone();
        let ReplayProvenanceKind::SumiNormalized {
            provider_origin: mutated_origin,
            ..
        } = &mut origin_mutated
            .replay_provenance
            .as_mut()
            .expect("bound marker")
            .kind
        else {
            panic!("Sumi marker");
        };
        mutated_origin.model.push_str("-changed");
        assert_eq!(
            origin_mutated
                .verified_replay_provenance()
                .expect_err("destination mutation must invalidate its seal"),
            "bound replay send view changed after assembly"
        );

        let mut marker_mutated = prompt.clone();
        marker_mutated
            .replay_provenance
            .as_mut()
            .expect("bound marker")
            .kind = ReplayProvenanceKind::SumiNormalized {
            provider_origin: provider_origin.clone(),
            canonical_through_seq: Some(2),
        };
        assert_eq!(
            marker_mutated
                .verified_replay_provenance()
                .expect_err("marker mutation must invalidate its seal"),
            "bound replay send view changed after assembly"
        );

        let constructed = PromptContext::new(
            "System.".to_owned(),
            Vec::new(),
            vec![user("constructed", 1)],
            Vec::new(),
            Vec::new(),
        );
        assert_eq!(
            constructed
                .verified_replay_provenance()
                .expect("public construction remains unbound"),
            None
        );
    }

    #[test]
    fn legacy_test_binder_requires_one_distinct_provider_origin() {
        let item = |provider_origin: ProviderOrigin, ordinal| {
            let anchor = provider_context_owner("assistant-1", 1);
            ProviderContextItem {
                retention_owner: anchor.clone(),
                origin_message: Some(anchor),
                wire_item_index: Some(0),
                ordinal,
                provider_origin,
                payload: ProviderContextPayload::EncryptedReasoning {
                    protocol: ApiProtocol::OpenAiResponses,
                    item: serde_json::json!({"type":"reasoning","ordinal":ordinal}),
                },
            }
        };
        assert_eq!(
            bind_sumi_replay_for_test(&mut simple_prompt(), None)
                .expect_err("missing origin must fail")
                .to_string(),
            "test replay origin is absent"
        );

        let origin = responses_spec().origin();
        let mut unique = PromptContext {
            provider_context: vec![item(origin.clone(), 0), item(origin.clone(), 1)],
            ..simple_prompt()
        };
        bind_sumi_replay_for_test(&mut unique, None)
            .expect("repeated identical origin is uniquely derivable");

        let mut other = origin.clone();
        other.model.push_str("-other");
        let mut ambiguous = PromptContext {
            provider_context: vec![item(origin, 0), item(other, 1)],
            ..simple_prompt()
        };
        assert_eq!(
            bind_sumi_replay_for_test(&mut ambiguous, None)
                .expect_err("multiple distinct origins must fail")
                .to_string(),
            "test replay origin is ambiguous"
        );
    }

    #[test]
    fn replay_binding_validates_empty_and_synthetic_only_watermark_bounds() {
        let spec = responses_spec();
        let fingerprint = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec.clone())
            .expect("fingerprint assembler")
            .destination_fingerprint()
            .expect("fingerprint");
        let native = || ProviderContextItem {
            retention_owner: provider_context_owner("native-owner-1", 1),
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: spec.origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![serde_json::json!({
                    "id": "cmp-watermark",
                    "type": "compaction",
                    "encrypted_content": "opaque-watermark",
                })],
                coverage: crate::provider::types::NativeCompactionCoverage {
                    through_message_seq: 1,
                    context_fingerprint: fingerprint.clone(),
                },
            },
        };
        let synthetic = ContextMessage::Synthetic {
            message: Message::User(UserMessage {
                content: vec![UserContent::Text {
                    text: "authorized diagnostic".to_owned(),
                }],
                timestamp: Utc::now(),
            }),
        };

        let mut no_progress = PromptContext {
            provider_context: vec![native()],
            messages: vec![synthetic.clone()],
            ..simple_prompt()
        };
        bind_provider_native_exact_replay(&mut no_progress, spec.origin(), 1, None)
            .expect("synthetic-only suffix may represent no canonical progress");

        let mut invalid = PromptContext {
            provider_context: vec![native()],
            messages: vec![synthetic.clone()],
            ..simple_prompt()
        };
        assert!(
            bind_provider_native_exact_replay(&mut invalid, spec.origin(), 1, Some(1)).is_err()
        );

        let mut collapsed = PromptContext {
            provider_context: vec![native()],
            messages: vec![synthetic],
            ..simple_prompt()
        };
        bind_provider_native_exact_replay(&mut collapsed, spec.origin(), 1, Some(3))
            .expect("synthetic-only transformed suffix carries canonical progress");

        let mut empty_collapsed = PromptContext {
            provider_context: vec![native()],
            messages: Vec::new(),
            ..simple_prompt()
        };
        bind_provider_native_exact_replay(&mut empty_collapsed, spec.origin(), 1, Some(2))
            .expect("fully collapsed suffix carries canonical progress");

        let mut invalid_sumi = simple_prompt();
        invalid_sumi.messages = vec![user("persisted", 1)];
        assert!(bind_sumi_normalized_replay(&mut invalid_sumi, spec.origin(), None).is_err());
    }

    #[tokio::test]
    async fn hydrated_provider_native_uses_canonical_window_suffix_and_exact_sumi_fallback() {
        let spec = responses_spec();
        let promoted_prefix = user("covered prefix", 1);
        let promoted_after_window = user("promoted after native coverage", 2);
        let exact_l0 = user("exact live L0", 3);
        let life_log = vec![
            promoted_prefix,
            promoted_after_window.clone(),
            exact_l0.clone(),
        ];
        let memory = || {
            let mut memory = ThreeLayerMemory::new(
                ConsolidatedMemory {
                    summary: crate::memory::DecryptedMemorySummary::new(
                        "sumi fallback summary".to_owned(),
                    ),
                    est_tokens: 6,
                },
                TokenCalibration::default(),
            );
            memory.push_l0(L0Batch::new(vec![exact_l0.clone()], 1, 0, 4));
            memory
        };

        let fingerprint = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec.clone())
            .expect("fingerprint assembler")
            .destination_fingerprint()
            .expect("fingerprint");
        let native = ProviderContextItem {
            retention_owner: provider_context_owner("native-owner-1", 1),
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
                coverage: crate::provider::types::NativeCompactionCoverage {
                    through_message_seq: 1,
                    context_fingerprint: fingerprint,
                },
            },
        };
        let native_footprint =
            eviction_footprint_for_payload(&spec, &native.payload).expect("native footprint");
        let native_entry = ProviderContextItemWithFootprint::new(native.clone(), native_footprint);

        let native_assembler =
            ContextAssembler::from_prompt_with_spec(simple_prompt(), spec.clone())
                .expect("native assembler")
                .with_mode(AssemblyMode::ProviderNative);
        native_assembler
            .install_hydrated_memory(memory(), &life_log, vec![native_entry])
            .expect("install authenticated native-mode memory");
        let native_prompt = native_assembler
            .assemble(&life_log, 1)
            .await
            .expect("assemble native window");
        assert_eq!(
            native_prompt
                .messages
                .iter()
                .filter_map(|message| match message {
                    ContextMessage::Persisted { seq, .. } => Some(*seq),
                    ContextMessage::Synthetic { .. } => None,
                })
                .collect::<Vec<_>>(),
            [2, 3],
            "native coverage suffix must come from the canonical life log"
        );
        assert!(native_prompt.memory_blocks.is_empty());
        assert_eq!(native_prompt.provider_context, vec![native]);

        let fallback_assembler = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec)
            .expect("fallback assembler")
            .with_mode(AssemblyMode::ProviderNative);
        fallback_assembler
            .install_hydrated_memory(memory(), &life_log, Vec::new())
            .expect("install authenticated fallback memory");
        let fallback_prompt = fallback_assembler
            .assemble(&life_log, 1)
            .await
            .expect("assemble Sumi fallback");
        assert_eq!(fallback_prompt.messages, vec![exact_l0]);
        assert_eq!(fallback_prompt.memory_blocks.len(), 1);
        assert_eq!(
            fallback_prompt.memory_blocks[0].text,
            "sumi fallback summary"
        );
    }

    #[tokio::test]
    async fn sumi_reasoning_requires_matching_thinking_block() {
        let spec = responses_spec();
        let mut prompt = simple_prompt();
        prompt.provider_context = vec![ProviderContextItem {
            retention_owner: provider_context_owner("assistant-1", 1),
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
            retention_owner: provider_context_owner("assistant-1", 1),
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
                retention_owner: provider_context_owner("assistant-2", 2),
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
                retention_owner: provider_context_owner("assistant-1", 1),
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
            retention_owner: provider_context_owner("msg-1", 1),
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
            .with_three_layer_memory(memory, 2);
        let recovered = assembler
            .recover_overflow(&[first, second, newest.clone()])
            .expect("bounded fallback recovery");

        assert!(
            recovered.contains(&newest),
            "active user must survive fallback"
        );
        let memory = assembler.hydrated_three_layer.lock().expect("memory lock");
        let l0 = memory.as_ref().expect("configured memory").memory.l0();
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
            .with_three_layer_memory(memory, 0);
        let result = assembler.assemble(&[], 1).await.expect("assemble");
        assert_eq!(result.memory_blocks.len(), 1);
        assert_eq!(result.memory_blocks[0].layer, MemoryLayer::L2);
        assert_eq!(result.memory_blocks[0].text, "L2 summary");
    }

    #[tokio::test]
    async fn hydrated_send_view_excludes_promoted_history_and_keeps_live_role_suffix() {
        let promoted = user("promoted old history", 1);
        let live_l0 = user("exact hydrated L0", 2);
        let post_hydration_user = user("new user", 3);
        let spec = model_spec();
        let post_hydration_tool_call = ContextMessage::Persisted {
            id: "assistant-4".to_owned(),
            seq: 4,
            message: Message::Assistant(AssistantMessage {
                content: vec![AssistantContent::ToolCall {
                    tool_call: crate::provider::types::ToolCall {
                        id: "call-4".to_owned(),
                        name: "fixture".to_owned(),
                        arguments: serde_json::from_value(serde_json::json!({}))
                            .expect("object tool arguments"),
                    },
                    wire_item_index: 0,
                }],
                model: spec.id.clone(),
                provider: spec.provider.clone(),
                origin: spec.origin(),
                usage: Usage::default(),
                stop_reason: StopReason::ToolUse,
                error_message: None,
                provider_code: None,
                interrupted: false,
                timestamp: Utc::now(),
            }),
        };
        let post_hydration_tool = ContextMessage::Persisted {
            id: "tool-4".to_owned(),
            seq: 5,
            message: Message::ToolResult(ToolResultMessage {
                tool_call_id: "call-4".to_owned(),
                tool_name: "fixture".to_owned(),
                content: vec![UserContent::Text {
                    text: "new tool".to_owned(),
                }],
                details: serde_json::json!({}),
                is_error: false,
                timestamp: Utc::now(),
            }),
        };
        let post_hydration_assistant = assistant_with_thinking(6, "new private", 1);

        let mut memory = ThreeLayerMemory::new(
            ConsolidatedMemory {
                summary: crate::memory::DecryptedMemorySummary::new(
                    "summary of promoted old history".to_owned(),
                ),
                est_tokens: 8,
            },
            TokenCalibration::default(),
        );
        memory.push_l0(L0Batch::new(vec![live_l0.clone()], 1, 0, 5));
        let assembler = ContextAssembler::from_prompt_with_spec(simple_prompt(), model_spec())
            .expect("valid prompt")
            .with_three_layer_memory(memory, 2);

        let result = assembler
            .assemble(
                &[
                    promoted,
                    live_l0,
                    post_hydration_user,
                    post_hydration_tool_call,
                    post_hydration_tool,
                    post_hydration_assistant,
                ],
                1,
            )
            .await
            .expect("assemble exact hydrated view");
        let seqs = result
            .messages
            .iter()
            .filter_map(|message| match message {
                ContextMessage::Persisted { seq, .. } => Some(*seq),
                ContextMessage::Synthetic { .. } => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(seqs, [2, 3, 4, 5, 6]);
        assert_eq!(result.memory_blocks.len(), 1);
        assert_eq!(
            result.memory_blocks[0].text,
            "summary of promoted old history"
        );
    }

    #[tokio::test]
    async fn hydrated_provider_context_influences_assembled_turn_context() {
        let spec = responses_spec();
        let item = ProviderContextItem {
            retention_owner: provider_context_owner("assistant-2", 2),
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
            replay_provenance: None,
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
            retention_owner: provider_context_owner("msg-1", 1),
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
            replay_provenance: None,
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
