use serde_json::Value;

use crate::provider::types::{ApiProtocol, ProviderOrigin};

pub(crate) const DEFAULT_OUTPUT_TOKENS: u64 = 16_384;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaxTokensField {
    MaxTokens,
    MaxCompletionTokens,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThinkingFormat {
    Off,
    Deepseek,
    Zai,
    OpenAiEffort,
    /// Gateway dialect has not been proven by a live fixture. Do not send a
    /// provider-specific thinking control object.
    ProviderDefault,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatCompat {
    pub max_tokens_field: MaxTokensField,
    pub supports_usage_in_streaming: bool,
    pub thinking_format: ThinkingFormat,
    pub requires_reasoning_content_on_assistant: bool,
    pub zai_tool_stream: bool,
    pub supports_strict_mode: bool,
    pub supports_required_tool_choice: bool,
    pub supports_store: bool,
    pub supports_developer_role: bool,
    pub allows_sampling_parameters: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponsesCompat {
    pub supports_store: bool,
    pub supports_encrypted_reasoning: bool,
    pub supports_native_compact: bool,
    pub supports_streaming: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnthropicCompat {
    pub beta_headers: Vec<String>,
    pub supports_prompt_cache: bool,
    pub supports_fine_grained_tool_streaming: bool,
    pub supports_native_compact: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolCompat {
    Chat(ChatCompat),
    Responses(ResponsesCompat),
    Anthropic(AnthropicCompat),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelSpec {
    pub id: String,
    pub provider: String,
    pub base_url: String,
    pub account_scope: String,
    pub api_key_env: String,
    pub context_window: u64,
    pub max_output_tokens: u64,
    pub default_output_tokens: u64,
    pub reasoning: bool,
    pub supports_images: bool,
    pub protocol: ApiProtocol,
    pub compat: ProtocolCompat,
}

impl ModelSpec {
    pub fn preset(name: &str) -> Option<Self> {
        if name == "anthropic" {
            return Some(Self {
                id: "claude-sonnet-4-6".to_owned(),
                provider: "anthropic".to_owned(),
                base_url: "https://api.anthropic.com/v1".to_owned(),
                account_scope: "default".to_owned(),
                api_key_env: "ANTHROPIC_API_KEY".to_owned(),
                context_window: 200_000,
                max_output_tokens: 64_000,
                default_output_tokens: DEFAULT_OUTPUT_TOKENS,
                reasoning: true,
                supports_images: true,
                protocol: ApiProtocol::AnthropicMessages,
                compat: ProtocolCompat::Anthropic(AnthropicCompat {
                    beta_headers: vec![
                        "compact-2026-01-12".to_owned(),
                        "interleaved-thinking-2025-05-14".to_owned(),
                    ],
                    supports_prompt_cache: true,
                    supports_fine_grained_tool_streaming: true,
                    supports_native_compact: true,
                }),
            });
        }
        if name == "openai-responses" {
            return Some(Self {
                id: "gpt-5.6".to_owned(),
                provider: "openai".to_owned(),
                base_url: "https://api.openai.com/v1".to_owned(),
                account_scope: "default".to_owned(),
                api_key_env: "OPENAI_API_KEY".to_owned(),
                context_window: 1_000_000,
                max_output_tokens: 128_000,
                default_output_tokens: DEFAULT_OUTPUT_TOKENS,
                reasoning: true,
                supports_images: true,
                protocol: ApiProtocol::OpenAiResponses,
                compat: ProtocolCompat::Responses(ResponsesCompat {
                    supports_store: true,
                    supports_encrypted_reasoning: true,
                    supports_native_compact: true,
                    supports_streaming: true,
                }),
            });
        }
        let (
            id,
            provider,
            base_url,
            api_key_env,
            context_window,
            max_output_tokens,
            supports_images,
            compat,
        ) = match name {
            "kimi-k3" => (
                "kimi-k3",
                "moonshot",
                "https://api.moonshot.ai/v1",
                "MOONSHOT_API_KEY",
                1_048_576,
                1_048_576,
                true,
                ChatCompat {
                    max_tokens_field: MaxTokensField::MaxCompletionTokens,
                    supports_usage_in_streaming: true,
                    thinking_format: ThinkingFormat::OpenAiEffort,
                    requires_reasoning_content_on_assistant: true,
                    zai_tool_stream: false,
                    supports_strict_mode: true,
                    supports_required_tool_choice: true,
                    supports_store: false,
                    supports_developer_role: false,
                    allows_sampling_parameters: false,
                },
            ),
            "glm-5.2" => (
                "glm-5.2",
                "zai",
                "https://api.z.ai/api/paas/v4",
                "ZAI_API_KEY",
                1_000_000,
                131_072,
                false,
                ChatCompat {
                    max_tokens_field: MaxTokensField::MaxTokens,
                    supports_usage_in_streaming: true,
                    thinking_format: ThinkingFormat::Zai,
                    requires_reasoning_content_on_assistant: false,
                    zai_tool_stream: true,
                    supports_strict_mode: false,
                    supports_required_tool_choice: true,
                    supports_store: false,
                    supports_developer_role: false,
                    allows_sampling_parameters: true,
                },
            ),
            "umans" | "umans-kimi-k2.7" => (
                "umans-kimi-k2.7",
                "umans",
                "https://api.code.umans.ai/v1",
                "UMANS_API_KEY",
                262_144,
                32_768,
                false,
                ChatCompat {
                    max_tokens_field: MaxTokensField::MaxTokens,
                    supports_usage_in_streaming: true,
                    thinking_format: ThinkingFormat::ProviderDefault,
                    requires_reasoning_content_on_assistant: false,
                    zai_tool_stream: false,
                    supports_strict_mode: false,
                    supports_required_tool_choice: true,
                    supports_store: false,
                    supports_developer_role: false,
                    allows_sampling_parameters: true,
                },
            ),
            "opencode-go" | "opencode-zen-go" => (
                "kimi-k2.7-code",
                "opencode-go",
                "https://opencode.ai/zen/go/v1",
                "OPENCODE_GO_API_KEY",
                262_144,
                32_768,
                false,
                ChatCompat {
                    max_tokens_field: MaxTokensField::MaxTokens,
                    supports_usage_in_streaming: true,
                    thinking_format: ThinkingFormat::ProviderDefault,
                    requires_reasoning_content_on_assistant: false,
                    zai_tool_stream: false,
                    supports_strict_mode: false,
                    supports_required_tool_choice: false,
                    supports_store: false,
                    supports_developer_role: false,
                    allows_sampling_parameters: true,
                },
            ),
            _ => return None,
        };

        Some(Self {
            id: id.to_owned(),
            provider: provider.to_owned(),
            base_url: base_url.to_owned(),
            account_scope: "default".to_owned(),
            api_key_env: api_key_env.to_owned(),
            context_window,
            max_output_tokens,
            default_output_tokens: DEFAULT_OUTPUT_TOKENS.min(max_output_tokens),
            reasoning: true,
            supports_images,
            protocol: ApiProtocol::OpenAiChatCompletions,
            compat: ProtocolCompat::Chat(compat),
        })
    }

    pub fn endpoint(&self) -> String {
        let suffix = match self.protocol {
            ApiProtocol::OpenAiChatCompletions => "chat/completions",
            ApiProtocol::OpenAiResponses => "responses",
            ApiProtocol::AnthropicMessages => "messages",
        };
        format!("{}/{suffix}", normalize_base_url(&self.base_url))
    }

    pub fn compact_endpoint(&self) -> String {
        format!("{}/responses/compact", normalize_base_url(&self.base_url))
    }

    pub fn origin(&self) -> ProviderOrigin {
        ProviderOrigin {
            provider_instance_id: self.provider_instance_id(),
            protocol: self.protocol,
            model: self.id.clone(),
        }
    }

    /// Resolve a canonical `ModelSpec` for a persisted provider origin. The
    /// exact model id is tried first; if it is not a built-in preset we fall
    /// back to a canonical preset for the protocol so that replay-probe
    /// footprinting stays consistent with the actual wire shape.
    pub(crate) fn from_origin(origin: &ProviderOrigin) -> Option<Self> {
        Self::preset(&origin.model).or_else(|| match origin.protocol {
            ApiProtocol::OpenAiResponses => Self::preset("openai-responses"),
            ApiProtocol::AnthropicMessages => Self::preset("anthropic"),
            ApiProtocol::OpenAiChatCompletions => Self::preset("kimi-k3"),
        })
    }

    pub fn provider_instance_id(&self) -> String {
        let endpoint = provider_instance_endpoint(&self.base_url);
        let protocol = protocol_tag(self.protocol);
        format!(
            "v1|{}|{}|{}|{}",
            identity_part(&self.provider),
            identity_part(&endpoint),
            identity_part(&self.account_scope),
            identity_part(protocol)
        )
    }

    pub fn set_model_id(&mut self, id: impl Into<String>) {
        self.id = id.into();
    }

    pub fn chat_compat(&self) -> Option<&ChatCompat> {
        match &self.compat {
            ProtocolCompat::Chat(compat) => Some(compat),
            ProtocolCompat::Responses(_) | ProtocolCompat::Anthropic(_) => None,
        }
    }

    pub fn responses_compat(&self) -> Option<&ResponsesCompat> {
        match &self.compat {
            ProtocolCompat::Responses(compat) => Some(compat),
            ProtocolCompat::Chat(_) | ProtocolCompat::Anthropic(_) => None,
        }
    }

    pub fn anthropic_compat(&self) -> Option<&AnthropicCompat> {
        match &self.compat {
            ProtocolCompat::Anthropic(compat) => Some(compat),
            ProtocolCompat::Chat(_) | ProtocolCompat::Responses(_) => None,
        }
    }
}

fn normalize_base_url(base_url: &str) -> String {
    base_url.trim().trim_end_matches('/').to_owned()
}

fn provider_instance_endpoint(base_url: &str) -> String {
    let normalized = normalize_base_url(base_url);
    let Ok(mut url) = reqwest::Url::parse(&normalized) else {
        return "invalid-url".to_owned();
    };
    url.set_query(None);
    url.set_fragment(None);
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.to_string().trim_end_matches('/').to_owned()
}

fn identity_part(value: &str) -> String {
    format!("{}:{value}", value.len())
}

const fn protocol_tag(protocol: ApiProtocol) -> &'static str {
    match protocol {
        ApiProtocol::OpenAiChatCompletions => "open_ai_chat_completions",
        ApiProtocol::OpenAiResponses => "open_ai_responses",
        ApiProtocol::AnthropicMessages => "anthropic_messages",
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RequestOptions {
    pub max_tokens: Option<u64>,
    pub temperature: Option<f64>,
    pub tool_choice: Option<Value>,
    pub reasoning_effort: Option<String>,
    /// Opt in to provider-native compaction. The canonical default remains
    /// `sumi_three_layer`; T17 will wire the durable conversation mode into
    /// this request option.
    pub native_compaction: bool,
}
