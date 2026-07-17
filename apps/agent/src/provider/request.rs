use serde_json::{Map, Value, json};

use super::types::{
    AssistantContent, Message, PromptContext, ToolDefinition, ToolResultMessage, UserContent,
};

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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Compat {
    pub max_tokens_field: MaxTokensField,
    pub supports_usage_in_streaming: bool,
    pub thinking_format: ThinkingFormat,
    pub requires_reasoning_content_on_assistant: bool,
    pub zai_tool_stream: bool,
    pub supports_strict_mode: bool,
    pub supports_store: bool,
    pub supports_developer_role: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelSpec {
    pub id: String,
    pub provider: String,
    pub base_url: String,
    pub api_key_env: String,
    pub context_window: u64,
    pub max_tokens: u64,
    pub reasoning: bool,
    pub supports_images: bool,
    pub compat: Compat,
}

impl ModelSpec {
    pub fn preset(name: &str) -> Option<Self> {
        let (id, provider, base_url, api_key_env, context_window, max_tokens, compat) = match name {
            "kimi-k3" => (
                "kimi-k3",
                "moonshot",
                "https://api.moonshot.ai/v1",
                "MOONSHOT_API_KEY",
                1_048_576,
                131_072,
                Compat {
                    max_tokens_field: MaxTokensField::MaxTokens,
                    supports_usage_in_streaming: true,
                    thinking_format: ThinkingFormat::Deepseek,
                    requires_reasoning_content_on_assistant: true,
                    zai_tool_stream: false,
                    supports_strict_mode: false,
                    supports_store: false,
                    supports_developer_role: false,
                },
            ),
            "glm-5.2" => (
                "glm-5.2",
                "zai",
                "https://api.z.ai/api/paas/v4",
                "ZAI_API_KEY",
                1_048_576,
                131_072,
                Compat {
                    max_tokens_field: MaxTokensField::MaxTokens,
                    supports_usage_in_streaming: true,
                    thinking_format: ThinkingFormat::Zai,
                    requires_reasoning_content_on_assistant: false,
                    zai_tool_stream: true,
                    supports_strict_mode: false,
                    supports_store: false,
                    supports_developer_role: false,
                },
            ),
            "umans" | "umans-kimi-k2.7" => (
                "umans-kimi-k2.7",
                "umans",
                "https://api.code.umans.ai/v1",
                "UMANS_API_KEY",
                262_144,
                32_768,
                Compat {
                    max_tokens_field: MaxTokensField::MaxTokens,
                    supports_usage_in_streaming: true,
                    thinking_format: ThinkingFormat::Deepseek,
                    requires_reasoning_content_on_assistant: true,
                    zai_tool_stream: false,
                    supports_strict_mode: false,
                    supports_store: false,
                    supports_developer_role: false,
                },
            ),
            "opencode-go" => (
                "kimi-k2.7-code",
                "opencode-go",
                "https://opencode.ai/zen/go/v1",
                "OPENCODE_GO_API_KEY",
                262_144,
                32_768,
                Compat {
                    max_tokens_field: MaxTokensField::MaxTokens,
                    supports_usage_in_streaming: true,
                    thinking_format: ThinkingFormat::Deepseek,
                    requires_reasoning_content_on_assistant: true,
                    zai_tool_stream: false,
                    supports_strict_mode: false,
                    supports_store: false,
                    supports_developer_role: false,
                },
            ),
            _ => return None,
        };

        Some(Self {
            id: id.to_owned(),
            provider: provider.to_owned(),
            base_url: base_url.to_owned(),
            api_key_env: api_key_env.to_owned(),
            context_window,
            // This is the default per-request output budget, not the model's
            // advertised maximum output capability.
            max_tokens,
            reasoning: true,
            supports_images: matches!(name, "kimi-k3" | "opencode-go"),
            compat,
        })
    }

    /// Override the model id, re-resolving model-dependent capabilities when
    /// the id matches a known preset. Multi-model endpoints (OpenCode Go
    /// serves Kimi K2.7 Code and GLM-5.2 behind one URL) would otherwise keep
    /// the previous model's thinking format, image support, and token limits.
    /// Endpoint-level fields (base_url, api_key_env, provider) are preserved.
    pub fn override_id(&mut self, id: &str) {
        if self.id == id {
            return;
        }
        if let Some(model) = Self::preset(id) {
            self.context_window = model.context_window;
            self.max_tokens = model.max_tokens;
            self.reasoning = model.reasoning;
            self.supports_images = model.supports_images;
            self.compat.thinking_format = model.compat.thinking_format;
            self.compat.requires_reasoning_content_on_assistant =
                model.compat.requires_reasoning_content_on_assistant;
            self.compat.zai_tool_stream = model.compat.zai_tool_stream;
        }
        id.clone_into(&mut self.id);
    }

    pub fn endpoint(&self) -> String {
        format!("{}/chat/completions", self.base_url.trim_end_matches('/'))
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RequestOptions {
    pub max_tokens: Option<u64>,
    pub temperature: Option<f64>,
    pub tool_choice: Option<Value>,
    pub reasoning_effort: Option<String>,
}

pub fn build_request(spec: &ModelSpec, context: &PromptContext, options: &RequestOptions) -> Value {
    let mut request = Map::new();
    request.insert("model".to_owned(), json!(spec.id));
    request.insert(
        "messages".to_owned(),
        json!(convert_messages(spec, context)),
    );
    request.insert("stream".to_owned(), json!(true));

    if spec.compat.supports_usage_in_streaming {
        request.insert("stream_options".to_owned(), json!({"include_usage": true}));
    }
    if spec.compat.supports_store {
        request.insert("store".to_owned(), json!(false));
    }

    let max_tokens = options.max_tokens.unwrap_or(spec.max_tokens);
    let max_tokens_key = match spec.compat.max_tokens_field {
        MaxTokensField::MaxTokens => "max_tokens",
        MaxTokensField::MaxCompletionTokens => "max_completion_tokens",
    };
    request.insert(max_tokens_key.to_owned(), json!(max_tokens));

    if let Some(temperature) = options.temperature {
        request.insert("temperature".to_owned(), json!(temperature));
    }
    if let Some(tool_choice) = &options.tool_choice {
        request.insert("tool_choice".to_owned(), tool_choice.clone());
    }

    if context.tools.is_empty() {
        if has_tool_history(&context.messages) {
            request.insert("tools".to_owned(), json!([]));
        }
    } else {
        request.insert(
            "tools".to_owned(),
            Value::Array(
                context
                    .tools
                    .iter()
                    .map(|tool| convert_tool(tool, &spec.compat))
                    .collect(),
            ),
        );
        if spec.compat.zai_tool_stream {
            request.insert("tool_stream".to_owned(), json!(true));
        }
    }

    if spec.reasoning {
        match spec.compat.thinking_format {
            ThinkingFormat::Off => {}
            ThinkingFormat::Deepseek => {
                request.insert("thinking".to_owned(), json!({"type": "enabled"}));
            }
            ThinkingFormat::Zai => {
                request.insert(
                    "thinking".to_owned(),
                    json!({
                        "type": "enabled",
                        "clear_thinking": false,
                    }),
                );
                if let Some(effort) = &options.reasoning_effort {
                    request.insert("reasoning_effort".to_owned(), json!(effort));
                }
            }
        }
    }

    Value::Object(request)
}

fn convert_messages(spec: &ModelSpec, context: &PromptContext) -> Vec<Value> {
    let mut output = Vec::new();
    if !context.system_prompt.is_empty() {
        let role = if spec.reasoning && spec.compat.supports_developer_role {
            "developer"
        } else {
            "system"
        };
        output.push(json!({"role": role, "content": context.system_prompt}));
    }

    let mut index = 0;
    while index < context.messages.len() {
        match &context.messages[index] {
            Message::User(message) => {
                let content = convert_user_content(&message.content, spec.supports_images);
                if !content.is_empty() {
                    output.push(json!({"role": "user", "content": content}));
                }
            }
            Message::Assistant(message) => {
                let mut assistant = Map::new();
                assistant.insert("role".to_owned(), json!("assistant"));
                let mut text = String::new();
                let mut thinking = Vec::new();
                let mut tool_calls = Vec::new();

                for block in &message.content {
                    match block {
                        AssistantContent::Text { text: part } => text.push_str(part),
                        AssistantContent::Thinking {
                            thinking: part,
                            signature_field,
                        } if message.model == spec.id => {
                            thinking.push((signature_field.as_str(), part.as_str()));
                        }
                        AssistantContent::Thinking { thinking: part, .. } => {
                            if !text.is_empty() {
                                text.push_str("\n\n");
                            }
                            text.push_str(part);
                        }
                        AssistantContent::ToolCall(call) => {
                            tool_calls.push(json!({
                                "id": normalize_tool_call_id(&call.id),
                                "type": "function",
                                "function": {
                                    "name": call.name,
                                    "arguments": call.arguments.to_string(),
                                },
                            }));
                        }
                    }
                }

                if !text.is_empty() {
                    assistant.insert("content".to_owned(), json!(text));
                }
                if let Some((field, _)) = thinking.first() {
                    let joined = thinking
                        .iter()
                        .map(|(_, value)| *value)
                        .collect::<Vec<_>>()
                        .join("\n");
                    assistant.insert((*field).to_owned(), json!(joined));
                }
                if !tool_calls.is_empty() {
                    assistant.insert("tool_calls".to_owned(), Value::Array(tool_calls));
                }
                if spec.compat.requires_reasoning_content_on_assistant
                    && spec.reasoning
                    && !assistant.contains_key("reasoning_content")
                {
                    assistant.insert("reasoning_content".to_owned(), json!(""));
                }
                if assistant.contains_key("content") || assistant.contains_key("tool_calls") {
                    output.push(Value::Object(assistant));
                }
            }
            Message::ToolResult(_) => {
                let mut images = Vec::new();
                while index < context.messages.len() {
                    let Message::ToolResult(tool_result) = &context.messages[index] else {
                        break;
                    };
                    output.push(convert_tool_result(tool_result, spec.supports_images));
                    if spec.supports_images {
                        images.extend(tool_result.content.iter().filter_map(|content| {
                            let UserContent::Image { data, mime_type } = content else {
                                return None;
                            };
                            Some(json!({
                                "type": "image_url",
                                "image_url": {"url": format!("data:{mime_type};base64,{data}")},
                            }))
                        }));
                    }
                    index += 1;
                }
                index = index.saturating_sub(1);
                if !images.is_empty() {
                    let mut content = vec![
                        json!({"type": "text", "text": "Attached image(s) from tool result:"}),
                    ];
                    content.extend(images);
                    output.push(json!({"role": "user", "content": content}));
                }
            }
        }
        index += 1;
    }
    output
}

const IMAGE_OMITTED_NOTE: &str = "(image omitted: model does not support image input)";

fn convert_user_content(content: &[UserContent], supports_images: bool) -> Vec<Value> {
    content
        .iter()
        .map(|item| match item {
            UserContent::Text { text } => json!({"type": "text", "text": text}),
            UserContent::Image { data, mime_type } if supports_images => json!({
                "type": "image_url",
                "image_url": {"url": format!("data:{mime_type};base64,{data}")},
            }),
            UserContent::Image { .. } => json!({
                "type": "text",
                "text": IMAGE_OMITTED_NOTE,
            }),
        })
        .collect()
}

fn convert_tool_result(message: &ToolResultMessage, supports_images: bool) -> Value {
    let text = message
        .content
        .iter()
        .filter_map(|item| match item {
            UserContent::Text { text } => Some(text.as_str()),
            UserContent::Image { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    let has_images = message
        .content
        .iter()
        .any(|item| matches!(item, UserContent::Image { .. }));
    // Images are re-sent in a follow-up user message only when the model
    // supports them; the tool message itself must not point at attachments
    // that will never be sent.
    let content = if has_images && !supports_images {
        if text.is_empty() {
            IMAGE_OMITTED_NOTE.to_owned()
        } else {
            format!("{text}\n{IMAGE_OMITTED_NOTE}")
        }
    } else if !text.is_empty() {
        text
    } else if has_images {
        "(see attached image)".to_owned()
    } else {
        "(no tool output)".to_owned()
    };
    json!({
        "role": "tool",
        "content": content,
        "tool_call_id": normalize_tool_call_id(&message.tool_call_id),
    })
}

fn convert_tool(tool: &ToolDefinition, compat: &Compat) -> Value {
    let mut function = Map::new();
    function.insert("name".to_owned(), json!(tool.name));
    function.insert("description".to_owned(), json!(tool.description));
    function.insert("parameters".to_owned(), tool.parameters.clone());
    if compat.supports_strict_mode {
        function.insert("strict".to_owned(), json!(true));
    }
    json!({"type": "function", "function": function})
}

fn normalize_tool_call_id(id: &str) -> String {
    const MAX_ID_LEN: usize = 40;
    const HASH_LEN: usize = 16;

    if !id.is_empty()
        && id.len() <= MAX_ID_LEN
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return id.to_owned();
    }

    let hash = id
        .as_bytes()
        .iter()
        .fold(0xcbf29ce484222325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        });
    let suffix = format!("{hash:016x}");
    let prefix_len = MAX_ID_LEN - 1 - HASH_LEN;
    let mut prefix: String = id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
                character
            } else {
                '_'
            }
        })
        .take(prefix_len)
        .collect();
    if prefix.is_empty() {
        prefix.push_str("call");
    }
    format!("{prefix}_{suffix}")
}

fn has_tool_history(messages: &[Message]) -> bool {
    messages.iter().any(|message| match message {
        Message::ToolResult(_) => true,
        Message::Assistant(message) => message
            .content
            .iter()
            .any(|content| matches!(content, AssistantContent::ToolCall(_))),
        Message::User(_) => false,
    })
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;

    use super::*;
    use crate::provider::types::{
        AssistantMessage, StopReason, ToolCall, ToolResultMessage, Usage, UserMessage,
    };

    fn context() -> PromptContext {
        PromptContext {
            system_prompt: "Stay grounded.".to_owned(),
            messages: vec![
                Message::User(UserMessage {
                    content: vec![UserContent::Text {
                        text: "read it".to_owned(),
                    }],
                    timestamp: Utc::now(),
                }),
                Message::Assistant(AssistantMessage {
                    content: vec![
                        AssistantContent::Thinking {
                            thinking: "I should read.".to_owned(),
                            signature_field: "reasoning_content".to_owned(),
                        },
                        AssistantContent::ToolCall(ToolCall {
                            id: "call|with+noise".to_owned(),
                            name: "read_file".to_owned(),
                            arguments: json!({"path":"a.txt"}),
                        }),
                    ],
                    model: "kimi-k3".to_owned(),
                    provider: "moonshot".to_owned(),
                    usage: Usage::default(),
                    stop_reason: StopReason::ToolUse,
                    error_message: None,
                    interrupted: false,
                    timestamp: Utc::now(),
                }),
                Message::ToolResult(ToolResultMessage {
                    tool_call_id: "call|with+noise".to_owned(),
                    tool_name: "read_file".to_owned(),
                    content: vec![UserContent::Text {
                        text: "hello".to_owned(),
                    }],
                    details: json!({}),
                    is_error: false,
                    timestamp: Utc::now(),
                }),
            ],
            tools: vec![ToolDefinition {
                name: "read_file".to_owned(),
                description: "Read a file".to_owned(),
                parameters: json!({"type":"object"}),
            }],
        }
    }

    #[test]
    fn messages_and_tools_are_independent_top_level_fields() {
        let request = build_request(
            &ModelSpec::preset("kimi-k3").expect("preset"),
            &context(),
            &RequestOptions {
                reasoning_effort: Some("high".to_owned()),
                ..RequestOptions::default()
            },
        );

        assert_eq!(request["messages"][0]["role"], "system");
        assert_eq!(request["tools"][0]["type"], "function");
        assert!(
            request["messages"]
                .as_array()
                .expect("messages")
                .iter()
                .all(|message| message.get("type") != Some(&Value::String("function".to_owned())))
        );
        assert_eq!(request["messages"][1]["content"][0]["text"], "read it");
        let tool_call_id = request["messages"][2]["tool_calls"][0]["id"]
            .as_str()
            .expect("tool call id");
        assert_eq!(
            request["messages"][3]["tool_call_id"].as_str(),
            Some(tool_call_id)
        );
        assert_ne!(tool_call_id, "call");
        assert!(tool_call_id.len() <= 40);
        assert_eq!(
            request["messages"][2]["reasoning_content"],
            "I should read."
        );
    }

    #[test]
    fn glm_uses_its_compat_fields() {
        let request = build_request(
            &ModelSpec::preset("glm-5.2").expect("preset"),
            &PromptContext {
                tools: vec![],
                messages: vec![],
                system_prompt: String::new(),
            },
            &RequestOptions {
                reasoning_effort: Some("high".to_owned()),
                ..RequestOptions::default()
            },
        );

        assert_eq!(request["max_tokens"], 131_072);
        assert!(request.get("max_completion_tokens").is_none());
        assert_eq!(request["thinking"]["type"], "enabled");
        assert_eq!(request["thinking"]["clear_thinking"], false);
        assert_eq!(request["reasoning_effort"], "high");
    }

    #[test]
    fn kimi_preserved_thinking_is_enabled_by_default() {
        let request = build_request(
            &ModelSpec::preset("kimi-k3").expect("preset"),
            &PromptContext {
                tools: vec![],
                messages: vec![],
                system_prompt: String::new(),
            },
            &RequestOptions::default(),
        );

        assert_eq!(request["thinking"]["type"], "enabled");
        assert_eq!(request["max_tokens"], 131_072);
    }

    #[test]
    fn cross_model_thinking_becomes_plain_content() {
        let mut context = context();
        let Message::Assistant(message) = &mut context.messages[1] else {
            panic!("assistant");
        };
        message.model = "another-model".to_owned();

        let request = build_request(
            &ModelSpec::preset("kimi-k3").expect("preset"),
            &context,
            &RequestOptions::default(),
        );

        assert_eq!(request["messages"][2]["content"], "I should read.");
        assert_eq!(request["messages"][2]["reasoning_content"], "");
    }

    fn set_tool_result_content(context: &mut PromptContext, content: Vec<UserContent>) {
        let Message::ToolResult(result) = &mut context.messages[2] else {
            panic!("tool result");
        };
        result.content = content;
    }

    fn image() -> UserContent {
        UserContent::Image {
            data: "AAA".to_owned(),
            mime_type: "image/png".to_owned(),
        }
    }

    #[test]
    fn image_only_tool_result_becomes_omission_note_without_image_support() {
        let mut context = context();
        set_tool_result_content(&mut context, vec![image()]);
        let mut spec = ModelSpec::preset("kimi-k3").expect("preset");
        spec.supports_images = false;

        let request = build_request(&spec, &context, &RequestOptions::default());

        assert_eq!(request["messages"][3]["content"], IMAGE_OMITTED_NOTE);
        let request_text = request.to_string();
        assert!(!request_text.contains("image_url"));
        assert!(!request_text.contains("see attached image"));
    }

    #[test]
    fn mixed_tool_result_keeps_text_and_notes_image_omission() {
        let mut context = context();
        set_tool_result_content(
            &mut context,
            vec![
                UserContent::Text {
                    text: "rendered ok".to_owned(),
                },
                image(),
            ],
        );
        let mut spec = ModelSpec::preset("kimi-k3").expect("preset");
        spec.supports_images = false;

        let request = build_request(&spec, &context, &RequestOptions::default());

        assert_eq!(
            request["messages"][3]["content"],
            format!("rendered ok\n{IMAGE_OMITTED_NOTE}")
        );
    }

    #[test]
    fn image_only_tool_result_is_attached_in_follow_up_when_supported() {
        let mut context = context();
        set_tool_result_content(&mut context, vec![image()]);
        let spec = ModelSpec::preset("kimi-k3").expect("preset");
        assert!(spec.supports_images);

        let request = build_request(&spec, &context, &RequestOptions::default());

        assert_eq!(request["messages"][3]["content"], "(see attached image)");
        assert_eq!(request["messages"][4]["role"], "user");
        assert_eq!(request["messages"][4]["content"][1]["type"], "image_url");
        assert_eq!(
            request["messages"][4]["content"][1]["image_url"]["url"],
            "data:image/png;base64,AAA"
        );
    }

    #[test]
    fn normalized_tool_call_ids_are_stable_and_collision_resistant() {
        let first = normalize_tool_call_id("call|a");
        let second = normalize_tool_call_id("call|b");
        let long_a = normalize_tool_call_id(&format!("{}a", "x".repeat(40)));
        let long_b = normalize_tool_call_id(&format!("{}b", "x".repeat(40)));

        assert_eq!(first, normalize_tool_call_id("call|a"));
        assert_ne!(first, second);
        assert_ne!(long_a, long_b);
        for id in [first, second, long_a, long_b] {
            assert!(id.len() <= 40);
            assert!(
                id.bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            );
        }
    }
}
