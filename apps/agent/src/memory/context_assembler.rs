//! Assemble a `PromptContext` from runtime state, applying replay normalization,
//! 50KB user attachment truncation, and the provider-native vs Sumi three-layer
//! mode decision. Sumi assembly retains active experience until durable memory
//! replacement; explicit overflow recovery is a separate operation.

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
        UserMessage, VerifiedReplayProvenance, VisibleMemoryFragment,
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
    // Armed only after the provider rejects an actual request for capacity.
    // This changes the working view, never durable L0 membership.
    recovery_budget: Mutex<Option<u64>>,
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

impl PromptContext {
    /// Extend a validated send view without normalizing or rebuilding its
    /// prefix. A synthetic user directive cannot change the persisted history
    /// covered by either form of replay provenance.
    pub(crate) fn with_appended_user_directive(
        &self,
        directive: UserMessage,
    ) -> Result<Self, String> {
        self.verified_replay_provenance()?;
        let mut fork = self.clone();
        fork.messages.push(ContextMessage::Synthetic {
            message: Message::User(directive),
        });
        if let Some(provenance) = &self.replay_provenance {
            fork.replay_provenance = Some(ReplayProvenance {
                kind: provenance.kind.clone(),
                seal: replay_binding_seal(&provenance.kind, fork.replay_send_view_digest()?)?,
            });
        }
        Ok(fork)
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
    pub(crate) visible_memory: Vec<VisibleMemoryFragment>,
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
            recovery_budget: Mutex::new(None),
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
        self.install_hydrated_memory_at(memory, transcript_through_seq, provider_context)
    }

    pub(crate) fn install_hydrated_memory_at(
        &self,
        memory: ThreeLayerMemory,
        transcript_through_seq: u64,
        provider_context: Vec<ProviderContextItemWithFootprint>,
    ) -> Result<()> {
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
        let mut hydrated = self.hydrated_three_layer.lock().expect("memory lock");
        // This budget belongs to the membership rejected by the provider.
        // A committed replacement invalidates it; ordinary appends, sealing
        // and metadata refreshes do not. If the new view still exceeds the
        // provider's capacity, the existing bounded recovery computes a new one.
        let replaced_l0 = hydrated.as_ref().is_some_and(|previous| {
            memory.l1().iter().any(|entry| {
                previous.memory.l0().iter().any(|batch| {
                    batch.id == entry.source_batch
                        && !memory.l0().iter().any(|live| live.id == batch.id)
                })
            })
        });
        *hydrated = Some(HydratedThreeLayer {
            memory,
            transcript_through_seq,
        });
        if replaced_l0 {
            *self.recovery_budget.lock().expect("recovery budget lock") = None;
        }
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
        let recovery_budget = *self.recovery_budget.lock().expect("recovery budget lock");
        let mut messages = match (self.mode, recovery_budget) {
            (_, Some(budget)) => {
                overflow.recover_context_to_budget(active_view, budget, &provider_context.items)?
            }
            // Active membership already accounts for durable memory replacements.
            // Trimming here would erase the raw target from both the parent and
            // its memory fork before either could decide what to retain.
            (AssemblyMode::SumiThreeLayer, None) => active_view,
            (AssemblyMode::ProviderNative, None) => overflow
                .recover_context_with_provider_context(
                    active_view,
                    is_first_user_call,
                    &provider_context.items,
                )?,
        };
        let canonical_through_seq = normalized_replay_through(&messages)?;
        self.apply_user_attachment_truncation(&mut messages).await?;
        let candidates = if provider_context.native_window.is_none() {
            let (with_memory, descriptors) = self.insert_memory_fragments(messages);
            messages = with_memory;
            if self.mode != AssemblyMode::SumiThreeLayer || recovery_budget.is_some() {
                // Native fallback and explicit recovery can hide retained experience
                // between visible summaries.
                // Defer upper replacement until durable memory releases that view;
                // visible adjacency alone cannot prove original continuity.
                Vec::new()
            } else {
                descriptors
            }
        } else {
            Vec::new()
        };
        messages = transform::transform(&messages, &destination);

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
        let visible_memory = candidates
            .into_iter()
            .filter_map(|mut descriptor| {
                let rendered = descriptor.render_message();
                let mut matching = prompt
                    .messages
                    .iter()
                    .enumerate()
                    .filter(|(_, message)| **message == rendered);
                let (index, _) = matching.next()?;
                if matching.next().is_some() {
                    // Ambiguous identical synthetic positions cannot authorize an edit.
                    return None;
                }
                descriptor.message_index = index;
                Some(descriptor)
            })
            .collect();
        Ok(AssembledPrompt {
            prompt,
            uncalibrated_prompt_estimate: estimate,
            visible_memory,
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
        self.recover_overflow_with_output_reserve(active_context, self.spec.default_output_tokens)
    }

    pub(crate) fn recover_overflow_with_output_reserve(
        &self,
        active_context: &[ContextMessage],
        output_reserve: u64,
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
        let (blocks, fixed_provider_context, _) =
            self.assemble_provider_view(Vec::new(), &self.spec.origin(), &provider_context);
        let fragments = if provider_context.native_window.is_none() {
            self.insert_memory_fragments(Vec::new()).0
        } else {
            Vec::new()
        };
        let overhead = self.calibration().effective_tokens(
            self.compute_uncalibrated_estimate(&blocks, &fragments, &fixed_provider_context)?,
            0,
        )?;
        let public = active_view
            .iter()
            .map(context_message_to_public)
            .collect::<Vec<_>>();
        let current = self.calibration().effective_tokens(
            estimate_public_messages(&public)?,
            provider_context_footprint_for_messages(&active_view, &provider_context.items)?,
        )?;
        // Leave headroom for request framing and calibration error, and shrink
        // relative to the rejected view even if the provider's true limit is
        // lower than its configured model window.
        let model_budget = self
            .spec
            .context_window
            .saturating_sub(output_reserve)
            .saturating_sub(overhead)
            .saturating_mul(3)
            / 4;
        let mut recovery_budget = self.recovery_budget.lock().expect("recovery budget lock");
        let previous = recovery_budget.unwrap_or(current);
        let budget = model_budget.min(previous.min(current).saturating_mul(3) / 4);
        *recovery_budget = Some(budget);
        drop(recovery_budget);
        overflow.recover_context_to_budget(active_view, budget, &provider_context.items)
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

    /// Place both summary layers at their original transcript position. L2 is
    /// not a global prefix: an older retained L0 may precede a later L2 fragment.
    /// Collect identity from this same memory snapshot before replay repair.
    fn insert_memory_fragments(
        &self,
        messages: Vec<ContextMessage>,
    ) -> (Vec<ContextMessage>, Vec<VisibleMemoryFragment>) {
        let hydrated = self.hydrated_three_layer.lock().expect("memory lock");
        let Some(hydrated) = hydrated.as_ref() else {
            return (messages, Vec::new());
        };
        let after_history = messages
            .iter()
            .rposition(|message| {
                matches!(message,
                    ContextMessage::Persisted { seq, .. } if *seq <= hydrated.transcript_through_seq
                )
            })
            .map_or(0, |index| index + 1);
        let position_for = |span: Option<crate::memory::OriginalSequenceSpan>,
                            legacy_l0_seq: Option<u64>| {
            let before_seq = if let Some(span) = span {
                span.to_seq.checked_add(1)
            } else {
                legacy_l0_seq.and_then(|source_seq| {
                    hydrated
                        .memory
                        .l0()
                        .iter()
                        .filter(|batch| batch.batch_seq > source_seq)
                        .flat_map(|batch| batch.messages.iter())
                        .filter_map(|message| match message {
                            ContextMessage::Persisted { seq, .. } => Some(*seq),
                            _ => None,
                        })
                        .min()
                })
            };
            before_seq
                .and_then(|boundary| {
                    messages.iter().position(|message|
                matches!(message, ContextMessage::Persisted { seq, .. } if *seq >= boundary)
            )
                })
                .unwrap_or(after_history)
        };
        let mut l1_entries: Vec<_> = hydrated.memory.l1().iter().collect();
        l1_entries.sort_by_key(|entry| entry.source_batch_seq);
        let mut fragments: Vec<_> = l1_entries
            .into_iter()
            .map(|entry| {
                let descriptor = l1_descriptor(entry);
                (
                    position_for(entry.original_seq_span, Some(entry.source_batch_seq)),
                    descriptor,
                )
            })
            .chain(hydrated.memory.l2().iter().map(|entry| {
                let descriptor = VisibleMemoryFragment {
                    message_index: 0,
                    adjacency_group: entry.batch_id,
                    layer: MemoryLayer::L2,
                    batch_id: entry.batch_id,
                    version: entry.version,
                    source_ids: entry.source_batches.clone(),
                    original_seq_span: entry.original_seq_span,
                    time_range: entry.time_range,
                    text: entry.summary.expose().to_owned(),
                };
                (position_for(entry.original_seq_span, None), descriptor)
            }))
            .collect();
        fragments.sort_by_key(|(position, descriptor)| {
            (
                *position,
                descriptor
                    .original_seq_span
                    .map_or(u64::MAX, |span| span.from_seq),
            )
        });
        let mut descriptors = Vec::with_capacity(fragments.len());
        let mut fragments = fragments.into_iter().peekable();
        let mut result = Vec::with_capacity(messages.len() + fragments.len());
        let mut group = None;
        for (index, message) in messages.into_iter().enumerate() {
            while fragments
                .peek()
                .is_some_and(|(position, _)| *position <= index)
            {
                let (_, mut descriptor) = fragments.next().expect("peeked fragment");
                descriptor.adjacency_group = *group.get_or_insert(descriptor.batch_id);
                result.push(descriptor.render_message());
                descriptors.push(descriptor);
            }
            result.push(message);
            group = None;
        }
        for (_, mut descriptor) in fragments {
            descriptor.adjacency_group = *group.get_or_insert(descriptor.batch_id);
            result.push(descriptor.render_message());
            descriptors.push(descriptor);
        }
        (result, descriptors)
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

fn memory_blocks_from_three_layer(_memory: &ThreeLayerMemory) -> Vec<MemoryBlock> {
    // Both layers live in chronological messages; duplicating L2 in a global
    // prefix would change its relationship to retained earlier experience.
    Vec::new()
}

fn l1_descriptor(entry: &crate::memory::L1Entry) -> VisibleMemoryFragment {
    VisibleMemoryFragment {
        message_index: 0,
        adjacency_group: entry.batch_id,
        layer: MemoryLayer::L1,
        batch_id: entry.batch_id,
        version: entry.version,
        source_ids: vec![entry.source_batch],
        original_seq_span: entry.original_seq_span,
        time_range: entry.time_range,
        text: entry.summary.expose().to_owned(),
    }
}

#[cfg(test)]
fn l1_fragment(entry: &crate::memory::L1Entry) -> ContextMessage {
    l1_descriptor(entry).render_message()
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
    if protocol != destination.protocol || item.provider_origin != *destination {
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
            seq,
            message: Message::Assistant(assistant),
            ..
        } if *seq == anchor.message_seq => assistant,
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
    // Responses reasoning is its own opaque wire item, not a public Thinking
    // block. The adapter validates its payload and wire-slot uniqueness.
    if protocol == ApiProtocol::OpenAiResponses {
        return true;
    }
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
    use crate::memory::{BatchState, L0Batch};
    use crate::provider::{
        RequestOptions,
        types::{
            ParentContextSnapshot, RejectedToolCall, StopReason, ToolArgumentError, ToolCall,
            ToolInvocationRoute, ToolResultMessage, UserMessage, ValidatedToolArguments,
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
                incoming_source: None,
                incoming_timing: None,
                content: vec![UserContent::Text {
                    text: text.to_owned(),
                }],
                timestamp: Utc::now(),
            }),
        }
    }

    fn l2_fixture(
        text: &str,
        est_tokens: u64,
        span: Option<crate::memory::OriginalSequenceSpan>,
    ) -> std::collections::VecDeque<crate::memory::L2Entry> {
        let now = Utc::now();
        std::collections::VecDeque::from([crate::memory::L2Entry {
            batch_id: uuid::Uuid::now_v7(),
            version: 1,
            source_batches: vec![uuid::Uuid::now_v7()],
            summary: crate::memory::DecryptedMemorySummary::new(text.to_owned()),
            est_tokens,
            time_range: (now, now),
            original_seq_span: span,
        }])
    }

    #[tokio::test]
    async fn provider_overflow_uses_a_recoverable_working_view_and_keeps_later_input() {
        let mut spec = model_spec();
        spec.context_window = 4_096;
        spec.default_output_tokens = 512;
        let assembler =
            ContextAssembler::from_prompt_with_spec(simple_prompt(), spec).expect("assembler");
        let old = user(&"original experience ".repeat(1_500), 1);
        let correction = user(
            "Correction: the updated explanation supersedes my first claim.",
            2,
        );
        let mut canonical = vec![old.clone(), correction.clone()];
        let first = assembler
            .assemble(&canonical, 0)
            .await
            .expect("full first request");
        assert!(
            first.messages.contains(&old),
            "no projection before actual overflow"
        );

        let preview = assembler
            .recover_overflow(&canonical)
            .expect("overflow projection");
        assert!(!preview.contains(&old));
        assert!(preview.contains(&correction));
        let recovered = assembler
            .assemble(&canonical, 1)
            .await
            .expect("retry request");
        assert_eq!(recovered.messages, preview);
        let ContextMessage::Synthetic {
            message: Message::User(notice),
        } = &preview[0]
        else {
            panic!("capacity notice is distinct from original history");
        };
        let UserContent::Text { text } = &notice.content[0] else {
            panic!("text notice");
        };
        assert!(text.contains("sequence range 1..=1"));
        assert!(text.contains("not been summarized or deleted"));
        assert!(text.contains("\"operation\":\"read\",\"from_seq\":1"));
        assert!(
            canonical.contains(&old),
            "canonical experience remains intact"
        );

        let later = user("Here is the next thing we need to discuss.", 3);
        canonical.push(later.clone());
        let next = assembler
            .assemble(&canonical, 0)
            .await
            .expect("next user remains usable");
        assert!(next.messages.contains(&correction));
        assert!(next.messages.contains(&later));
        assert!(!next.messages.contains(&old));
        assert!(
            next.replay_provenance.is_some(),
            "recovery is a valid provider send view"
        );
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
                incoming_source: None,
                incoming_timing: None,
                content: vec![UserContent::Text {
                    text: "first".to_owned(),
                }],
                timestamp: Utc::now(),
            }),
        };
        let second = ContextMessage::Synthetic {
            message: Message::User(UserMessage {
                incoming_source: None,
                incoming_timing: None,
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
                dialect: crate::provider::model::ResponsesDialect::Standard,
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
                dialect: crate::provider::model::ResponsesDialect::Standard,
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
                        route: crate::provider::types::ToolInvocationRoute::Normal,
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
    fn leading_recovery_notice_does_not_move_trailing_memory_before_older_raw() {
        let mut memory = ThreeLayerMemory::new(
            std::collections::VecDeque::new(),
            TokenCalibration::default(),
        );
        let a = user("Older A retained", 10);
        let b = user("Newer B original", 20);
        let mut batch_a = L0Batch::new(vec![a.clone()], 1, 0, 4);
        batch_a.state = BatchState::Sealed;
        memory.push_l0(batch_a);
        let mut batch_b = L0Batch::new(vec![b], 2, 0, 4);
        batch_b.state = BatchState::Sealed;
        let id = batch_b.id;
        memory.push_l0(batch_b);
        let time = Utc::now();
        memory.store_compact_result(
            id,
            crate::memory::CompactResult {
                original_seq_span: None,
                summary: crate::memory::DecryptedMemorySummary::new("B organized".into()),
                est_tokens: 3,
                time_range: (time, time),
            },
        );
        memory.promote_l0_to_l1(id).unwrap();
        let assembler = assembler().with_three_layer_memory(memory, 20);
        let synthetic = |text: &str| ContextMessage::Synthetic {
            message: Message::User(UserMessage {
                incoming_source: None,
                incoming_timing: None,
                timestamp: time,
                content: vec![UserContent::Text { text: text.into() }],
            }),
        };
        let notice = synthetic("Earlier raw history was omitted for capacity.");
        let tail = synthetic("Current internal instruction.");
        let placed = assembler
            .insert_memory_fragments(vec![notice.clone(), a.clone(), tail.clone()])
            .0;
        assert_eq!(placed.len(), 4);
        assert_eq!(placed[0], notice);
        assert_eq!(placed[1], a);
        assert!(
            matches!(&placed[2], ContextMessage::Synthetic { message: Message::User(fragment) }
            if matches!(&fragment.content[0], UserContent::Text { text } if text.ends_with("B organized")))
        );
        assert_eq!(placed[3], tail);
    }

    #[tokio::test]
    async fn durable_replacement_releases_recovery_budget_but_routine_hydration_does_not() {
        for output_reserve in [0, 2_000] {
            let raw = vec![
                user(&"Large original observation. ".repeat(1_000), 10),
                user("Earlier detail that should become available again.", 20),
                user("Latest request.", 30),
            ];
            let mut batches: Vec<_> = raw
                .iter()
                .enumerate()
                .map(|(index, message)| {
                    let mut batch = L0Batch::new(vec![message.clone()], index as u64 + 1, 0, 1_000);
                    batch.state = BatchState::Sealed;
                    batch
                })
                .collect();
            let source_id = batches[0].id;
            let make_memory = |batches: &[L0Batch]| {
                let mut memory = ThreeLayerMemory::new(
                    std::collections::VecDeque::new(),
                    TokenCalibration::default(),
                );
                for batch in batches {
                    memory.push_l0(batch.clone());
                }
                memory
            };
            let mut spec = model_spec();
            spec.context_window = 2_000;
            let assembler = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec).unwrap();
            assembler
                .install_hydrated_memory(make_memory(&batches), &raw, Vec::new())
                .unwrap();
            assembler
                .recover_overflow_with_output_reserve(&raw, output_reserve)
                .unwrap();
            let budget = *assembler.recovery_budget.lock().unwrap();
            assert!(budget.is_some());
            if output_reserve == 2_000 {
                assert_eq!(budget, Some(0));
            }
            let restricted = assembler.assemble(&raw, 1).await.unwrap();
            assert!(!restricted.messages.contains(&raw[0]));
            assert!(restricted.messages.contains(&raw[2]));

            // Rehydrating the same snapshot, then sealing/appending a batch,
            // must not silently undo recovery before any actual replacement.
            assembler
                .install_hydrated_memory(make_memory(&batches), &raw, Vec::new())
                .unwrap();
            assert_eq!(*assembler.recovery_budget.lock().unwrap(), budget);
            batches[0].state = BatchState::Compacted;
            let mut extended = raw.clone();
            extended.push(user("New ordinary input.", 40));
            batches.push(L0Batch::new(vec![extended[3].clone()], 4, 0, 10));
            assembler
                .install_hydrated_memory(make_memory(&batches), &extended, Vec::new())
                .unwrap();
            assert_eq!(*assembler.recovery_budget.lock().unwrap(), budget);
            if output_reserve == 2_000 {
                assert!(
                    !assembler
                        .assemble(&extended, 1)
                        .await
                        .unwrap()
                        .messages
                        .contains(&raw[1])
                );
            }

            let mut replaced = make_memory(&batches);
            let time = Utc::now();
            replaced.store_compact_result(
                source_id,
                crate::memory::CompactResult {
                    original_seq_span: None,
                    summary: crate::memory::DecryptedMemorySummary::new(
                        "Observation organized.".into(),
                    ),
                    est_tokens: 5,
                    time_range: (time, time),
                },
            );
            replaced.promote_l0_to_l1(source_id).unwrap();
            assembler
                .install_hydrated_memory(replaced, &extended, Vec::new())
                .unwrap();
            assert_eq!(*assembler.recovery_budget.lock().unwrap(), None);
            let restored = assembler.assemble(&extended, 1).await.unwrap();
            assert!(restored.messages.contains(&raw[1]));
            assert!(restored.messages.contains(&raw[2]));
            assert!(restored.messages.contains(&extended[3]));
            assert!(!restored.messages.contains(&raw[0]));
            assert!(restored.messages.iter().any(|message| matches!(message,
                ContextMessage::Synthetic { message: Message::User(fragment) }
                    if matches!(&fragment.content[0], UserContent::Text { text } if text.ends_with("Observation organized.")))));
        }
    }

    #[tokio::test]
    async fn overflow_recovery_reserves_space_for_positioned_l1() {
        let mut memory = ThreeLayerMemory::new(
            std::collections::VecDeque::new(),
            TokenCalibration::default(),
        );
        let raw = vec![
            user(&"old detail ".repeat(800), 10),
            user("B source", 20),
            user(&"recent detail ".repeat(800), 30),
            user("Continue.", 40),
        ];
        let mut ids = Vec::new();
        for (index, message) in raw.iter().enumerate() {
            let mut batch = L0Batch::new(vec![message.clone()], index as u64 + 1, 0, 10);
            batch.state = BatchState::Sealed;
            ids.push(batch.id);
            memory.push_l0(batch);
        }
        let time = Utc::now();
        memory.store_compact_result(
            ids[1],
            crate::memory::CompactResult {
                original_seq_span: None,
                summary: crate::memory::DecryptedMemorySummary::new("b".repeat(12_000)),
                est_tokens: 3000,
                time_range: (time, time),
            },
        );
        memory.promote_l0_to_l1(ids[1]).unwrap();
        let mut spec = model_spec();
        spec.context_window = 5_000;
        let assembler = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec)
            .unwrap()
            .with_three_layer_memory(memory, 40);
        assembler
            .recover_overflow_with_output_reserve(&raw, 500)
            .unwrap();
        let assembled = assembler.assemble_with_estimate(&raw, 1).await.unwrap();
        assert!(
            assembled.uncalibrated_prompt_estimate <= 4_500,
            "retained L1 must consume the overflow recovery budget: {}",
            assembled.uncalibrated_prompt_estimate
        );
        assert!(assembled.prompt.messages.contains(raw.last().unwrap()));
        assert!(
            assembled.visible_memory.is_empty(),
            "overflow recovery must not expose upper targets across hidden experience"
        );
        assert!(assembled.prompt.messages.iter().any(|message| matches!(message,
            ContextMessage::Synthetic { message: Message::User(fragment) }
                if matches!(&fragment.content[0], UserContent::Text { text } if text.ends_with(&"b".repeat(12_000))))));
    }

    #[tokio::test]
    async fn hydration_uses_original_batch_order_not_summary_allocation_order() {
        use crate::memory::{
            HydratedMemoryBatch, HydratedMemoryJob, HydratedMemoryMembership,
            HydratedMemoryRuntime, HydratedMemorySummary,
        };
        use crate::store::{
            MemoryBatchState, MemoryJobKind, MemoryJobStatus, MemoryLayer as StoredLayer,
        };
        let a = uuid::Uuid::now_v7();
        let b = uuid::Uuid::now_v7();
        let c = uuid::Uuid::now_v7();
        let summary_id = uuid::Uuid::now_v7();
        let unchanged_target = uuid::Uuid::now_v7();
        let originals = vec![
            user("A retained", 10),
            user("B original", 20),
            user("C retained", 30),
        ];
        let time = Utc::now();
        let summary = HydratedMemorySummary::new("B organized".into(), 3, time, time).unwrap();
        let hydrated = HydratedMemoryRuntime::new(
            vec![
                HydratedMemoryBatch::new(
                    a,
                    StoredLayer::L0,
                    1,
                    1,
                    2,
                    MemoryBatchState::Sealed,
                    3,
                    0,
                    None,
                ),
                HydratedMemoryBatch::new(
                    b,
                    StoredLayer::L0,
                    2,
                    2,
                    3,
                    MemoryBatchState::Dropped,
                    0,
                    0,
                    None,
                ),
                HydratedMemoryBatch::new(
                    c,
                    StoredLayer::L0,
                    3,
                    3,
                    1,
                    MemoryBatchState::Open,
                    3,
                    0,
                    None,
                ),
                HydratedMemoryBatch::new(
                    summary_id,
                    StoredLayer::L1,
                    1,
                    1,
                    2,
                    MemoryBatchState::Promoted,
                    3,
                    0,
                    Some(summary.clone()),
                ),
                HydratedMemoryBatch::new(
                    unchanged_target,
                    StoredLayer::L1,
                    2,
                    2,
                    1,
                    MemoryBatchState::Dropped,
                    0,
                    0,
                    None,
                ),
            ],
            vec![
                HydratedMemoryMembership::new(a, 1, originals[0].clone()),
                HydratedMemoryMembership::new(b, 1, originals[1].clone()),
                HydratedMemoryMembership::new(c, 1, originals[2].clone()),
            ],
            vec![
                HydratedMemoryJob::new(
                    uuid::Uuid::now_v7(),
                    MemoryJobKind::CompactL0,
                    1,
                    vec![b],
                    std::collections::BTreeMap::from([(b, 3), (summary_id, 2)]),
                    MemoryJobStatus::Applied,
                    Some(summary),
                ),
                HydratedMemoryJob::new(
                    uuid::Uuid::now_v7(),
                    MemoryJobKind::CompactL0,
                    2,
                    vec![a],
                    std::collections::BTreeMap::from([(a, 2), (unchanged_target, 1)]),
                    MemoryJobStatus::Unchanged,
                    None,
                ),
            ],
            vec![],
            HashMap::new(),
        );
        let memory = ThreeLayerMemory::from_hydrated(hydrated).unwrap();
        assert_eq!(memory.l1()[0].source_batch_seq, 2);
        let assembler = assembler().with_three_layer_memory(memory, 30);
        let prompt = assembler.assemble(&originals, 0).await.unwrap();
        assert_eq!(prompt.messages[0], originals[0]);
        assert_eq!(prompt.messages[2], originals[2]);
        assert!(
            matches!(&prompt.messages[1], ContextMessage::Synthetic { message: Message::User(fragment) }
            if matches!(&fragment.content[0], UserContent::Text { text } if text.ends_with("B organized")))
        );
    }

    #[tokio::test]
    async fn replacements_keep_source_order_and_leave_prepared_memory_off_context() {
        let mut memory = ThreeLayerMemory::new(
            std::collections::VecDeque::new(),
            TokenCalibration::default(),
        );
        let mut original = vec![user("A raw", 10), user("B raw", 20), user("C raw", 30)];
        // Wall clocks can run backward. Source order must not depend on them.
        let time = Utc::now();
        for (index, message) in original.iter_mut().enumerate() {
            let ContextMessage::Persisted {
                message: Message::User(user),
                ..
            } = message
            else {
                unreachable!()
            };
            user.timestamp = time - chrono::Duration::days(index as i64);
        }
        let mut ids = Vec::new();
        for (index, message) in original.iter().enumerate() {
            let mut batch = L0Batch::new(vec![message.clone()], index as u64 + 1, 0, 10);
            batch.state = BatchState::Sealed;
            ids.push(batch.id);
            memory.push_l0(batch);
        }
        let summary_b = "B organized detail. ".repeat(3000);
        memory.store_compact_result(
            ids[1],
            crate::memory::CompactResult {
                original_seq_span: None,
                summary: crate::memory::DecryptedMemorySummary::new(summary_b.clone()),
                est_tokens: 15_000,
                time_range: (
                    time - chrono::Duration::days(1),
                    time - chrono::Duration::days(1),
                ),
            },
        );
        let assembler = assembler().with_three_layer_memory(memory, 30);
        let prepared = assembler.assemble(&original, 0).await.unwrap();
        assert_eq!(prepared.messages, original);
        assert!(prepared.memory_blocks.is_empty());
        {
            let mut hydrated = assembler.hydrated_three_layer.lock().unwrap();
            hydrated
                .as_mut()
                .unwrap()
                .memory
                .promote_l0_to_l1(ids[1])
                .unwrap();
        }
        let placed = assembler.assemble(&original, 0).await.unwrap();
        assert_eq!(placed.messages.len(), 3);
        assert_eq!(placed.messages[0], original[0]);
        assert_eq!(placed.messages[2], original[2]);
        let ContextMessage::Synthetic {
            message: Message::User(fragment),
        } = &placed.messages[1]
        else {
            panic!("positioned L1");
        };
        assert!(fragment.incoming_timing.is_none());
        let UserContent::Text { text } = &fragment.content[0] else {
            panic!("summary text");
        };
        assert!(
            text.ends_with(&summary_b),
            "memory is not truncated into an uploaded attachment"
        );
        assert!(placed.memory_blocks.is_empty(), "no duplicated prefix L1");
        {
            let mut hydrated = assembler.hydrated_three_layer.lock().unwrap();
            let memory = &mut hydrated.as_mut().unwrap().memory;
            memory.store_compact_result(
                ids[0],
                crate::memory::CompactResult {
                    original_seq_span: None,
                    summary: crate::memory::DecryptedMemorySummary::new("A organized".into()),
                    est_tokens: 3,
                    time_range: (time, time),
                },
            );
            memory.promote_l0_to_l1(ids[0]).unwrap();
        }
        let placed = assembler.assemble(&original, 0).await.unwrap();
        let text = |message: &ContextMessage| match message {
            ContextMessage::Synthetic {
                message: Message::User(user),
            } => match &user.content[0] {
                UserContent::Text { text } => text.clone(),
                _ => panic!("text"),
            },
            _ => panic!("synthetic memory"),
        };
        assert!(text(&placed.messages[0]).ends_with("A organized"));
        assert!(text(&placed.messages[1]).ends_with(&summary_b));
        assert_eq!(placed.messages[2], original[2]);
        assert_eq!(
            assembler.assemble(&original, 0).await.unwrap(),
            placed,
            "reassembling does not refresh or reorder memory"
        );
    }

    #[tokio::test]
    async fn visible_upper_fragments_keep_chronological_gaps_and_exact_distinct_identity() {
        use crate::memory::{L1Entry, OriginalSequenceSpan};
        let raw = [
            user("earlier retained experience", 1),
            user("retained middle gap", 3),
            user("later retained experience", 5),
            user("latest correction", 7),
        ];
        let mut memory = ThreeLayerMemory::new(
            l2_fixture(
                "Later integrated memory",
                7,
                Some(OriginalSequenceSpan {
                    from_seq: 6,
                    to_seq: 6,
                }),
            ),
            TokenCalibration::default(),
        );
        for (index, message) in raw.iter().enumerate() {
            memory.push_l0(L0Batch::new(vec![message.clone()], index as u64 + 1, 0, 1));
        }
        let now = Utc::now();
        let first_id = uuid::Uuid::now_v7();
        let second_id = uuid::Uuid::now_v7();
        for (batch_id, seq) in [(first_id, 2), (second_id, 4)] {
            memory.l1.push_back(L1Entry {
                batch_id,
                version: 3,
                source_batch: uuid::Uuid::now_v7(),
                source_batch_seq: seq,
                summary: crate::memory::DecryptedMemorySummary::new(
                    "Identical summary text".into(),
                ),
                est_tokens: 5,
                time_range: (now, now),
                original_seq_span: Some(OriginalSequenceSpan {
                    from_seq: seq,
                    to_seq: seq,
                }),
            });
        }
        let spec = model_spec();
        let assembler = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec.clone())
            .unwrap()
            .with_three_layer_memory(memory, 7);
        let assembled = assembler.assemble_with_estimate(&raw, 0).await.unwrap();
        assert!(
            assembled.prompt.memory_blocks.is_empty(),
            "L2 must not move to a global prefix"
        );
        assert_eq!(assembled.prompt.messages.len(), 7);
        for (index, message) in raw.iter().enumerate() {
            assert_eq!(&assembled.prompt.messages[index * 2], message);
        }
        assert_eq!(
            assembled
                .visible_memory
                .iter()
                .map(|fragment| fragment.message_index)
                .collect::<Vec<_>>(),
            vec![1, 3, 5]
        );
        assert_eq!(assembled.visible_memory[0].batch_id, first_id);
        assert_eq!(assembled.visible_memory[1].batch_id, second_id);
        assert_eq!(
            assembled.visible_memory[0].text,
            assembled.visible_memory[1].text
        );
        assert_eq!(assembled.visible_memory[2].layer, MemoryLayer::L2);
        let options = RequestOptions::default();
        let snapshot = ParentContextSnapshot::capture_with_memory(
            &assembled.prompt,
            &spec,
            &options,
            &assembled.visible_memory,
        )
        .unwrap();
        assert_eq!(snapshot.prompt(), &assembled.prompt);
        assert_eq!(snapshot.visible_memory(), assembled.visible_memory);
        let mut wrong_identity = assembled.visible_memory.clone();
        wrong_identity[0].batch_id = second_id;
        assert!(
            ParentContextSnapshot::capture_with_memory(
                &assembled.prompt,
                &spec,
                &options,
                &wrong_identity
            )
            .is_err()
        );
        let mut wrong_position = assembled.visible_memory.clone();
        wrong_position[0].message_index = 2;
        assert!(
            ParentContextSnapshot::capture_with_memory(
                &assembled.prompt,
                &spec,
                &options,
                &wrong_position
            )
            .is_err()
        );
        let mut altered_prompt = assembled.prompt.clone();
        altered_prompt.messages.swap(1, 3);
        assert!(
            ParentContextSnapshot::capture_with_memory(
                &altered_prompt,
                &spec,
                &options,
                &assembled.visible_memory
            )
            .is_err()
        );
        assert!(
            ParentContextSnapshot::capture(&assembled.prompt, &spec, &options)
                .visible_memory()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn normalized_away_retained_thinking_separates_upper_adjacency_groups() {
        use crate::memory::{L1Entry, OriginalSequenceSpan};
        let mut interrupted = assistant_with_thinking(2, "retained private experience", 1);
        let ContextMessage::Persisted {
            message: Message::Assistant(assistant),
            ..
        } = &mut interrupted
        else {
            unreachable!()
        };
        assistant
            .content
            .retain(|content| matches!(content, AssistantContent::Thinking { .. }));
        assistant.interrupted = true;
        assistant.stop_reason = StopReason::Aborted;
        let mut spec = model_spec();
        spec.id = "another-model".into();
        let mut memory = ThreeLayerMemory::new(
            std::collections::VecDeque::new(),
            TokenCalibration::default(),
        );
        memory.push_l0(L0Batch::new(vec![interrupted.clone()], 2, 0, 10));
        let now = Utc::now();
        for seq in [1, 3, 4] {
            memory.l1.push_back(L1Entry {
                batch_id: uuid::Uuid::now_v7(),
                version: 1,
                source_batch: uuid::Uuid::now_v7(),
                source_batch_seq: seq,
                summary: crate::memory::DecryptedMemorySummary::new(format!("Memory {seq}")),
                est_tokens: 5,
                time_range: (now, now),
                original_seq_span: Some(OriginalSequenceSpan {
                    from_seq: seq,
                    to_seq: seq,
                }),
            });
        }
        let assembler = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec)
            .unwrap()
            .with_three_layer_memory(memory, 4);
        let raw = [interrupted];
        let (before, descriptors) = assembler.insert_memory_fragments(raw.to_vec());
        assert_eq!(before.len(), 4);
        assert_eq!(descriptors.len(), 3);
        assert_eq!(before[1], raw[0]);
        let assembled = assembler.assemble_with_estimate(&raw, 0).await.unwrap();
        assert_eq!(assembled.prompt.messages.len(), 3);
        assert!(assembled.prompt.messages.iter().all(|message| matches!(
            message,
            ContextMessage::Synthetic {
                message: Message::User(_)
            }
        )));
        let visible = &assembled.visible_memory;
        assert_eq!(visible.len(), 3);
        assert_eq!(
            visible
                .iter()
                .map(|fragment| fragment.message_index)
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_ne!(
            visible[0].adjacency_group, visible[1].adjacency_group,
            "normalization must not erase a retained gap in target continuity"
        );
        assert_eq!(
            visible[1].adjacency_group, visible[2].adjacency_group,
            "uninterrupted summaries remain eligible together"
        );
        assert_eq!(visible[0].adjacency_group, visible[0].batch_id);
        assert_eq!(visible[1].adjacency_group, visible[1].batch_id);
    }

    #[tokio::test]
    async fn unknown_original_span_stays_visible_without_inventing_a_target_range() {
        let memory = ThreeLayerMemory::new(
            l2_fixture("Retained older memory", 5, None),
            TokenCalibration::default(),
        );
        let assembler = ContextAssembler::from_prompt_with_spec(simple_prompt(), model_spec())
            .unwrap()
            .with_three_layer_memory(memory, 0);
        let assembled = assembler.assemble_with_estimate(&[], 0).await.unwrap();
        assert_eq!(assembled.visible_memory.len(), 1);
        assert!(assembled.visible_memory[0].original_seq_span.is_none());
        assert!(
            serde_json::to_string(&assembled.prompt.messages[0])
                .unwrap()
                .contains("Original transcript position is not recorded")
        );
    }

    #[test]
    fn l1_fragment_exposes_actual_summary_batch_and_recorded_time_range() {
        let source_batch = uuid::Uuid::now_v7();
        let batch_id = uuid::Uuid::now_v7();
        let from = chrono::DateTime::parse_from_rfc3339("2026-09-07T10:00:00.123456789Z")
            .expect("start timestamp")
            .with_timezone(&Utc);
        let through = from + chrono::Duration::minutes(5);
        let mut memory = ThreeLayerMemory::new(
            std::collections::VecDeque::new(),
            TokenCalibration::default(),
        );
        memory.l1.push_back(crate::memory::L1Entry {
            batch_id,
            version: 1,
            original_seq_span: None,
            source_batch,
            source_batch_seq: 2,
            summary: crate::memory::DecryptedMemorySummary::new(
                "Original fragment meaning.".into(),
            ),
            est_tokens: 5,
            time_range: (from, through),
        });
        assert!(memory_blocks_from_three_layer(&memory).is_empty());
        let ContextMessage::Synthetic {
            message: Message::User(fragment),
        } = l1_fragment(&memory.l1()[0])
        else {
            panic!("memory fragment");
        };
        assert!(fragment.incoming_timing.is_none());
        let UserContent::Text { text } = &fragment.content[0] else {
            panic!("text fragment");
        };
        assert!(text.contains(&from.to_rfc3339()));
        assert!(text.contains(&through.to_rfc3339()));
        let read_args = text
            .split("conversation_history(")
            .nth(1)
            .expect("actionable source read")
            .split(").")
            .next()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(read_args).expect("source read arguments"),
            serde_json::json!({"operation": "read", "batch_id": batch_id, "limit": 5}),
        );
        assert!(text.contains("next_after_seq as after_seq"));
        assert!(text.ends_with("Original fragment meaning."));
    }

    #[test]
    fn parent_snapshot_fork_preserves_full_context_and_rendered_prefix() {
        use crate::provider::adapters::{anthropic, chat_completions, responses};
        use serde_json::{Value, json};

        fn remove_cache_control(value: &mut Value) {
            match value {
                Value::Object(object) => {
                    object.remove("cache_control");
                    for value in object.values_mut() {
                        remove_cache_control(value);
                    }
                }
                Value::Array(array) => array.iter_mut().for_each(remove_cache_control),
                _ => {}
            }
        }

        for mut spec in [model_spec(), responses_spec(), anthropic_spec()] {
            let timestamp = chrono::DateTime::parse_from_rfc3339("2026-09-07T23:40:12.123456789Z")
                .expect("precise timestamp")
                .with_timezone(&Utc);
            let mut prompt = simple_prompt();
            prompt.system_prompt =
                "The actual parent instructions, including current permissions.".into();
            prompt.memory_blocks.push(MemoryBlock {
                layer: MemoryLayer::L1,
                text: "An earlier interpretation, since corrected by the latest message.".into(),
                time_range: Some((timestamp, timestamp)),
            });
            prompt.tools.push(ToolDefinition {
                name: "fixture".into(),
                description: "Read a precise source.".into(),
                parameters: json!({
                    "type": "object",
                    "properties": {"path": {"$ref": "#/$defs/path"}},
                    "required": ["path"],
                    "$defs": {"path": {"type": "string"}}
                }),
            });
            let mut observation = user("Original observation", 1);
            if let ContextMessage::Persisted {
                message: Message::User(user),
                ..
            } = &mut observation
            {
                user.timestamp = timestamp;
                user.content.push(UserContent::Image {
                    data: "cGFyZW50LXVzZXItaW1hZ2U=".into(),
                    mime_type: "image/png".into(),
                });
            }
            let mut assistant = assistant_with_thinking_for(&spec, 2, "parent reasoning", 1);
            if let ContextMessage::Persisted {
                message: Message::Assistant(assistant),
                ..
            } = &mut assistant
            {
                assistant.timestamp = timestamp;
                assistant.stop_reason = StopReason::ToolUse;
                assistant.content.push(AssistantContent::ToolCall {
                    tool_call: ToolCall {
                        id: "call-with-stable-id".into(),
                        name: "fixture".into(),
                        route: ToolInvocationRoute::Elevated,
                        arguments: serde_json::from_value(json!({"path": "/workspace/notes"}))
                            .expect("tool arguments"),
                    },
                    wire_item_index: 2,
                });
            }
            let mut result = tool_result(3, "call-with-stable-id");
            if let ContextMessage::Persisted {
                message: Message::ToolResult(result),
                ..
            } = &mut result
            {
                result.timestamp = timestamp;
                result.details = json!({"line": 17, "complete": false, "observed": null});
                result.content.push(UserContent::Image {
                    data: "cGFyZW50LXRvb2wtaW1hZ2U=".into(),
                    mime_type: "image/png".into(),
                });
            }
            prompt.messages = vec![
                observation,
                assistant,
                result,
                user("Latest correction outside target", 4),
            ];
            let opaque_payload = match spec.protocol {
                ApiProtocol::OpenAiChatCompletions => None,
                ApiProtocol::OpenAiResponses => {
                    Some(responses_reasoning_payload("opaque-parent-state"))
                }
                ApiProtocol::AnthropicMessages => {
                    Some(ProviderContextPayload::EncryptedReasoning {
                        protocol: ApiProtocol::AnthropicMessages,
                        item: json!({"type": "thinking_signature", "signature": "parent-signature"}),
                    })
                }
            };
            if let Some(payload) = opaque_payload {
                prompt.provider_context.push(ProviderContextItem {
                    retention_owner: provider_context_owner("assistant-2", 2),
                    origin_message: Some(provider_context_owner("assistant-2", 2)),
                    // Responses opaque items occupy their own output slot;
                    // Anthropic signatures bind the existing thinking block.
                    wire_item_index: Some(match spec.protocol {
                        ApiProtocol::OpenAiResponses => 3,
                        ApiProtocol::AnthropicMessages => 1,
                        ApiProtocol::OpenAiChatCompletions => unreachable!("no opaque Chat item"),
                    }),
                    ordinal: 0,
                    provider_origin: spec.origin(),
                    payload,
                });
            }
            bind_sumi_normalized_replay(&mut prompt, spec.origin(), Some(4)).expect("bind parent");
            let mut options = RequestOptions {
                max_tokens: Some(8192),
                reasoning_effort: match spec.protocol {
                    ApiProtocol::OpenAiChatCompletions => Some("max".into()),
                    ApiProtocol::OpenAiResponses => Some("high".into()),
                    ApiProtocol::AnthropicMessages => None,
                },
                ..RequestOptions::default()
            };
            let original_prompt = prompt.clone();
            let original_spec = spec.clone();
            let original_options = options.clone();
            let snapshot = ParentContextSnapshot::capture(&prompt, &spec, &options);
            prompt.messages.clear();
            spec.account_scope.push_str("-changed");
            options.max_tokens = Some(4096);
            assert_eq!(snapshot.prompt(), &original_prompt);
            assert_eq!(snapshot.spec(), &original_spec);
            assert_eq!(snapshot.options(), &original_options);

            let directive = UserMessage {
                incoming_source: None,
                incoming_timing: None,
                content: vec![UserContent::Text {
                    text: "Reorganize messages 1–3 using the whole context.".into(),
                }],
                timestamp,
            };
            let fork = snapshot
                .fork_with_directive(directive.clone())
                .expect("fork exact parent");
            let mut expected = original_prompt.clone();
            expected.messages.push(ContextMessage::Synthetic {
                message: Message::User(directive),
            });
            assert_eq!(
                serde_json::to_value(&fork).unwrap(),
                serde_json::to_value(&expected).unwrap()
            );
            assert_eq!(
                fork.verified_replay_provenance().unwrap(),
                original_prompt.verified_replay_provenance().unwrap()
            );
            assert_eq!(
                snapshot.prompt(),
                &original_prompt,
                "fork must not change its parent"
            );

            let render = |context: &PromptContext| match original_spec.protocol {
                ApiProtocol::OpenAiChatCompletions => {
                    chat_completions::build_request(&original_spec, context, &original_options)
                        .expect("render Chat")
                }
                ApiProtocol::OpenAiResponses => {
                    responses::build_request(&original_spec, context, &original_options)
                        .expect("render Responses")
                }
                ApiProtocol::AnthropicMessages => {
                    anthropic::build_request(&original_spec, context, &original_options)
                        .expect("render Anthropic")
                }
            };
            let mut parent_request = render(snapshot.prompt());
            let mut fork_request = render(&fork);
            let field = if original_spec.protocol == ApiProtocol::OpenAiResponses {
                "input"
            } else {
                "messages"
            };
            let mut parent_messages = parent_request
                .as_object_mut()
                .unwrap()
                .remove(field)
                .unwrap();
            let mut fork_messages = fork_request.as_object_mut().unwrap().remove(field).unwrap();
            assert_eq!(
                fork_request, parent_request,
                "system, tools and request settings must remain exact"
            );
            if original_spec.protocol == ApiProtocol::AnthropicMessages {
                // Anthropic merges adjacent users and moves its cache breakpoint
                // to the appended block. Compare model-visible content, not that
                // transport hint; this does not assert a measured cache hit.
                remove_cache_control(&mut parent_messages);
                remove_cache_control(&mut fork_messages);
                let parent = parent_messages.as_array().unwrap();
                let fork = fork_messages.as_array().unwrap();
                let last = parent.len() - 1;
                assert_eq!(fork.len(), parent.len());
                assert_eq!(&fork[..last], &parent[..last]);
                assert_eq!(fork[last]["role"], parent[last]["role"]);
                let parent_content = parent[last]["content"].as_array().unwrap();
                let fork_content = fork[last]["content"].as_array().unwrap();
                assert_eq!(&fork_content[..parent_content.len()], parent_content);
                assert_eq!(fork_content.len(), parent_content.len() + 1);
            } else {
                let parent = parent_messages.as_array().unwrap();
                let fork = fork_messages.as_array().unwrap();
                assert_eq!(&fork[..parent.len()], parent);
                assert_eq!(fork.len(), parent.len() + 1);
            }
        }
    }

    #[test]
    fn parent_snapshot_fork_preserves_native_continuation_and_coverage() {
        for spec in [responses_spec(), anthropic_spec()] {
            let mut prompt = simple_prompt();
            let coverage = crate::provider::types::NativeCompactionCoverage {
                through_message_seq: 3,
                context_fingerprint: "parent-fingerprint".into(),
            };
            let payload = match spec.protocol {
                ApiProtocol::OpenAiResponses => ProviderContextPayload::OpenAiCompactedWindow {
                    items: vec![
                        serde_json::json!({"type": "compaction", "encrypted_content": "parent-native-state"}),
                    ],
                    coverage,
                },
                ApiProtocol::AnthropicMessages => ProviderContextPayload::AnthropicCompaction {
                    block: serde_json::json!({"type": "compaction", "content": "parent-native-state"}),
                    coverage,
                },
                _ => unreachable!(),
            };
            prompt.provider_context.push(ProviderContextItem {
                retention_owner: provider_context_owner("native-owner-3", 3),
                origin_message: None,
                wire_item_index: None,
                ordinal: 0,
                provider_origin: spec.origin(),
                payload,
            });
            prompt
                .messages
                .push(user("Correction after native window", 4));
            bind_provider_native_exact_replay(&mut prompt, spec.origin(), 3, Some(4))
                .expect("bind native parent");
            let snapshot = ParentContextSnapshot::capture(
                &prompt,
                &spec,
                &RequestOptions {
                    native_compaction: true,
                    ..RequestOptions::default()
                },
            );
            let fork = snapshot
                .fork_with_directive(UserMessage {
                    incoming_source: None,
                    incoming_timing: None,
                    content: vec![UserContent::Text {
                        text: "Organize the target with this same context.".into(),
                    }],
                    timestamp: Utc::now(),
                })
                .expect("fork native parent");
            assert_eq!(fork.provider_context, prompt.provider_context);
            assert_eq!(fork.messages[..prompt.messages.len()], prompt.messages);
            assert_eq!(
                fork.verified_replay_provenance_for(&spec.origin()).unwrap(),
                prompt
                    .verified_replay_provenance_for(&spec.origin())
                    .unwrap()
            );
            assert!(snapshot.options().native_compaction);
        }
    }

    #[tokio::test]
    async fn normal_sumi_assembly_retains_uncompacted_target_and_latest_experience() {
        let old = (1..=5)
            .map(|seq| user(&"x".repeat(48_000), seq))
            .collect::<Vec<_>>();
        let mut life_log = old.clone();
        life_log.push(user("Latest correction after the hydrated snapshot", 6));
        let mut memory = ThreeLayerMemory::new(
            std::collections::VecDeque::new(),
            TokenCalibration::default(),
        );
        let mut target = L0Batch::new(old, 1, 0, 60_000);
        target.state = BatchState::Sealed;
        memory.push_l0(target);
        for assembler in [assembler(), assembler().with_three_layer_memory(memory, 5)] {
            for trigger in [
                ProviderCallTrigger::FirstAfterUser,
                ProviderCallTrigger::Continuation,
            ] {
                let assembled = assembler
                    .assemble_for_call_with_estimate(&life_log, trigger)
                    .await
                    .expect("assemble active context");
                assert!(assembled.uncalibrated_prompt_estimate > crate::memory::L0_LIMIT);
                assert_eq!(
                    assembled.prompt.messages, life_log,
                    "a target may only be replaced by applied memory, not silently dropped before the fork"
                );
            }
        }
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
                incoming_source: None,
                incoming_timing: None,
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
                l2_fixture(
                    "sumi fallback summary",
                    6,
                    Some(crate::memory::OriginalSequenceSpan {
                        from_seq: 1,
                        to_seq: 1,
                    }),
                ),
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
        assert!(
            native_assembler
                .assemble_with_estimate(&life_log, 1)
                .await
                .unwrap()
                .visible_memory
                .is_empty(),
            "native-hidden summaries never become memory-fork targets"
        );

        let fallback_assembler = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec)
            .expect("fallback assembler")
            .with_mode(AssemblyMode::ProviderNative);
        fallback_assembler
            .install_hydrated_memory(memory(), &life_log, Vec::new())
            .expect("install authenticated fallback memory");
        let fallback = fallback_assembler
            .assemble_with_estimate(&life_log, 1)
            .await
            .expect("assemble Sumi fallback");
        assert!(
            fallback.visible_memory.is_empty(),
            "native mode without a checkpoint must defer upper targets too"
        );
        let fallback_prompt = fallback.prompt;
        assert_eq!(fallback_prompt.messages.last(), Some(&exact_l0));
        assert!(fallback_prompt.memory_blocks.is_empty());
        assert!(
            serde_json::to_string(&fallback_prompt.messages[0])
                .unwrap()
                .contains("sumi fallback summary")
        );
    }

    #[tokio::test]
    async fn anthropic_reasoning_requires_matching_thinking_block() {
        let spec = anthropic_spec();
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
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::AnthropicMessages,
                item: serde_json::json!({"type":"thinking_signature", "signature":"opaque"}),
            },
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
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::AnthropicMessages,
                item: serde_json::json!({"type":"thinking_signature", "signature":"opaque"}),
            },
        }];
        let assembler2 = ContextAssembler::from_prompt_with_spec(prompt2, anthropic_spec())
            .expect("valid prompt");
        let result2 = assembler2
            .assemble(
                &[assistant_with_thinking_for(
                    &anthropic_spec(),
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
            std::collections::VecDeque::new(),
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
            l2_fixture(
                "L2 summary",
                10,
                Some(crate::memory::OriginalSequenceSpan {
                    from_seq: 1,
                    to_seq: 1,
                }),
            ),
            TokenCalibration::default(),
        );
        let assembler = ContextAssembler::from_prompt_with_spec(simple_prompt(), spec)
            .expect("valid prompt")
            .with_three_layer_memory(memory, 0);
        let result = assembler.assemble(&[], 1).await.expect("assemble");
        assert!(result.memory_blocks.is_empty());
        assert_eq!(result.messages.len(), 1);
        assert!(
            serde_json::to_string(&result.messages[0])
                .unwrap()
                .contains("L2 summary")
        );
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
                        route: crate::provider::types::ToolInvocationRoute::Normal,
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
            l2_fixture(
                "summary of promoted old history",
                8,
                Some(crate::memory::OriginalSequenceSpan {
                    from_seq: 1,
                    to_seq: 1,
                }),
            ),
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
        assert!(result.memory_blocks.is_empty());
        assert!(
            serde_json::to_string(&result.messages[0])
                .unwrap()
                .contains("summary of promoted old history")
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
