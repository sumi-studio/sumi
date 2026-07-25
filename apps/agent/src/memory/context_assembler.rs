//! Assemble a `PromptContext` from runtime state, applying overflow fallback,
//! replay normalization (transform), 50KB user attachment truncation, and the
//! provider-native vs Sumi three-layer mode decision.

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

use crate::memory::estimate::TokenCalibration;
use crate::memory::overflow::{AssemblyMode, Overflow, USER_ATTACHMENT_TRUNCATION_BYTES};
use crate::memory::transform;
use crate::provider::types::{
    ApiProtocol, ContextMessage, MemoryBlock, Message, PromptContext, ProviderContextItem,
    ProviderContextPayload, ProviderOrigin, ToolDefinition, UserContent,
};
use crate::tools::executor::ArtifactBrokerClient;

pub struct ContextAssembler {
    system_prompt: String,
    memory_blocks: Vec<MemoryBlock>,
    tools: Vec<ToolDefinition>,
    provider_context: Vec<ProviderContextItem>,
    broker: Option<ArtifactBrokerClient>,
    calib: TokenCalibration,
    mode: AssemblyMode,
    attachment_handles: Mutex<HashMap<String, String>>,
}

impl ContextAssembler {
    pub fn from_prompt(prompt: PromptContext) -> Self {
        Self {
            system_prompt: prompt.system_prompt,
            memory_blocks: prompt.memory_blocks,
            tools: prompt.tools,
            provider_context: prompt.provider_context,
            broker: None,
            calib: TokenCalibration::default(),
            mode: AssemblyMode::SumiThreeLayer,
            attachment_handles: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_broker(mut self, broker: ArtifactBrokerClient) -> Self {
        self.broker = Some(broker);
        self
    }

    pub fn with_calibration(mut self, calib: TokenCalibration) -> Self {
        self.calib = calib;
        self
    }

    pub fn with_mode(mut self, mode: AssemblyMode) -> Self {
        self.mode = mode;
        self
    }

    /// Assemble a send-ready `PromptContext` for `destination`.
    ///
    /// `attempt` mirrors `RunDriver::start_provider_for_command`; the first
    /// attempt (`attempt == 0`) is the first user call of the turn and is
    /// shielded from ordinary overflow fallback to protect TTFT.
    pub async fn assemble(
        &self,
        context: &[ContextMessage],
        destination: &ProviderOrigin,
        attempt: usize,
    ) -> Result<PromptContext> {
        let overflow = Overflow::new(self.calib, self.mode);
        let is_first_user_call = attempt == 0;

        let mut messages = overflow.recover_context(context.to_vec(), is_first_user_call)?;
        messages = transform::transform(&messages, destination);

        self.apply_user_attachment_truncation(&mut messages).await?;

        let (memory_blocks, provider_context, messages) =
            self.assemble_provider_view(messages, destination)?;

        Ok(PromptContext {
            system_prompt: self.system_prompt.clone(),
            memory_blocks,
            messages,
            provider_context,
            tools: self.tools.clone(),
        })
    }

    /// Produce a smaller runtime context for an overflow retry.  This is
    /// intentionally not the first user call path, so it applies the ordinary
    /// L0 limit immediately.
    pub fn recover_overflow(
        &self,
        active_context: &[ContextMessage],
    ) -> Result<Vec<ContextMessage>> {
        let overflow = Overflow::new(self.calib, self.mode);
        overflow.recover_context(active_context.to_vec(), false)
    }

    async fn apply_user_attachment_truncation(
        &self,
        messages: &mut [ContextMessage],
    ) -> Result<()> {
        for (message_index, message) in messages.iter_mut().enumerate() {
            let id_prefix = attachment_id_prefix(&*message, message_index);
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

                let handle = {
                    let handles = self
                        .attachment_handles
                        .lock()
                        .expect("attachment handle lock");
                    handles.get(&full_text).cloned()
                };
                let handle = match handle {
                    Some(cached) => cached,
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
                        handles.insert(full_text.clone(), handle.clone());
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
        Vec<ProviderContextItem>,
        Vec<ContextMessage>,
    )> {
        if self.mode == AssemblyMode::ProviderNative {
            let fingerprint =
                destination_fingerprint(destination, &self.system_prompt, &self.tools);
            if let Some(item) =
                find_native_window(&self.provider_context, destination, &fingerprint)
            {
                let through = native_coverage_through(item);
                let suffix: Vec<ContextMessage> = messages
                    .into_iter()
                    .filter(|message| match message {
                        ContextMessage::Persisted { seq, .. } => *seq > through,
                        ContextMessage::Synthetic { .. } => true,
                    })
                    .collect();
                return Ok((Vec::new(), vec![item.clone()], suffix));
            }
        }

        let message_map: HashMap<&str, &ContextMessage> = messages
            .iter()
            .filter_map(|message| match message {
                ContextMessage::Persisted { id, .. } => Some((id.as_str(), message)),
                ContextMessage::Synthetic { .. } => None,
            })
            .collect();

        let mut provider_context: Vec<_> = self
            .provider_context
            .iter()
            .filter(|item| matches_sumi_reasoning(item, destination, &message_map))
            .cloned()
            .collect();
        provider_context.sort_by_key(|item| item.ordinal);

        Ok((self.memory_blocks.clone(), provider_context, messages))
    }
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

fn attachment_id_prefix(message: &ContextMessage, message_index: usize) -> String {
    match message {
        ContextMessage::Persisted { id, .. } => id.clone(),
        ContextMessage::Synthetic { .. } => {
            let digest: [u8; 32] = Sha256::digest(format!("{message_index}").as_bytes()).into();
            format!("synthetic-{}", hex_digest(&digest))
        }
    }
}

fn destination_fingerprint(
    destination: &ProviderOrigin,
    system_prompt: &str,
    tools: &[ToolDefinition],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"sumi-provider-native-context-fingerprint/v1");
    hasher.update(destination.provider_instance_id.as_bytes());
    hasher.update(protocol_str(destination.protocol).as_bytes());
    hasher.update(destination.model.as_bytes());
    hasher.update(system_prompt.as_bytes());

    let mut tool_keys: Vec<&ToolDefinition> = tools.iter().collect();
    tool_keys.sort_by(|a, b| a.name.cmp(&b.name));
    for tool in tool_keys {
        hasher.update(tool.name.as_bytes());
        if let Ok(encoded) = serde_json::to_string(&tool.parameters) {
            hasher.update(encoded.as_bytes());
        }
    }

    hex_digest(hasher.finalize().as_slice())
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn find_native_window<'a>(
    provider_context: &'a [ProviderContextItem],
    destination: &ProviderOrigin,
    fingerprint: &str,
) -> Option<&'a ProviderContextItem> {
    provider_context
        .iter()
        .rev()
        .find(|item| match &item.payload {
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

fn native_coverage_through(item: &ProviderContextItem) -> u64 {
    match &item.payload {
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

fn protocol_str(protocol: ApiProtocol) -> &'static str {
    match protocol {
        ApiProtocol::OpenAiChatCompletions => "open_ai_chat_completions",
        ApiProtocol::OpenAiResponses => "open_ai_responses",
        ApiProtocol::AnthropicMessages => "anthropic_messages",
    }
}

fn matches_sumi_reasoning(
    item: &ProviderContextItem,
    destination: &ProviderOrigin,
    message_map: &HashMap<&str, &ContextMessage>,
) -> bool {
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
    let message = match message_map.get(anchor.message_id.as_str()) {
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
    assistant.origin == *destination
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::types::{
        ApiProtocol, ContextMessage, Message, PromptContext, ProviderOrigin, UserContent,
        UserMessage,
    };
    use chrono::Utc;

    fn simple_prompt() -> PromptContext {
        PromptContext {
            system_prompt: "System.".to_owned(),
            memory_blocks: vec![],
            messages: vec![],
            provider_context: vec![],
            tools: vec![],
        }
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

    #[test]
    fn attachment_truncation_prefix_keeps_char_boundary() {
        let text = "α".repeat(100_000); // two-byte chars
        let truncated = truncate_with_attachment_handle(&text, "artifact://x");
        assert!(truncated.is_char_boundary(USER_ATTACHMENT_TRUNCATION_BYTES));
        assert!(truncated.contains("artifact://x"));
    }

    #[test]
    fn destination_fingerprint_is_stable_and_model_sensitive() {
        let origin = ProviderOrigin {
            provider_instance_id: "pi".to_owned(),
            protocol: ApiProtocol::OpenAiChatCompletions,
            model: "m1".to_owned(),
        };
        let f1 = destination_fingerprint(&origin, "System.", &[]);
        let mut origin2 = origin.clone();
        origin2.model = "m2".to_owned();
        let f2 = destination_fingerprint(&origin2, "System.", &[]);
        assert_ne!(f1, f2);
    }
}
