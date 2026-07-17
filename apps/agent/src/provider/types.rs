use std::{
    pin::Pin,
    task::{Context, Poll},
};

use chrono::{DateTime, Utc};
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum Message {
    User(UserMessage),
    Assistant(AssistantMessage),
    ToolResult(ToolResultMessage),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UserMessage {
    pub content: Vec<UserContent>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UserContent {
    Text { text: String },
    Image { data: String, mime_type: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssistantMessage {
    pub content: Vec<AssistantContent>,
    pub model: String,
    pub provider: String,
    pub usage: Usage,
    pub stop_reason: StopReason,
    pub error_message: Option<String>,
    pub interrupted: bool,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolResultMessage {
    pub tool_call_id: String,
    pub tool_name: String,
    pub content: Vec<UserContent>,
    pub details: Value,
    pub is_error: bool,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AssistantContent {
    Text {
        text: String,
    },
    Thinking {
        thinking: String,
        signature_field: String,
    },
    ToolCall(ToolCall),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    Stop,
    Length,
    ToolUse,
    Error,
    Aborted,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub reasoning: u64,
    pub total_tokens: u64,
}

impl Usage {
    pub fn from_raw(raw: &RawUsage) -> Self {
        let cache_read = raw
            .prompt_tokens_details
            .as_ref()
            .and_then(|details| details.cached_tokens)
            .or(raw.prompt_cache_hit_tokens)
            .unwrap_or_default();
        let cache_write = raw
            .prompt_tokens_details
            .as_ref()
            .and_then(|details| details.cache_write_tokens)
            .unwrap_or_default();
        let input = raw
            .prompt_tokens
            .unwrap_or_default()
            .saturating_sub(cache_read)
            .saturating_sub(cache_write);
        let output = raw.completion_tokens.unwrap_or_default();
        let reasoning = raw
            .completion_tokens_details
            .as_ref()
            .and_then(|details| details.reasoning_tokens)
            .unwrap_or_default();

        Self {
            input,
            output,
            cache_read,
            cache_write,
            reasoning,
            total_tokens: input + output + cache_read + cache_write,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawUsage {
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub prompt_cache_hit_tokens: Option<u64>,
    pub prompt_tokens_details: Option<PromptTokensDetails>,
    pub completion_tokens_details: Option<CompletionTokensDetails>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptTokensDetails {
    pub cached_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionTokensDetails {
    pub reasoning_tokens: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderEvent {
    Start,
    TextStart {
        content_index: usize,
    },
    TextDelta {
        content_index: usize,
        delta: String,
    },
    TextEnd {
        content_index: usize,
        content: String,
    },
    ThinkingStart {
        content_index: usize,
    },
    ThinkingDelta {
        content_index: usize,
        delta: String,
    },
    ThinkingEnd {
        content_index: usize,
        content: String,
    },
    ToolCallStart {
        content_index: usize,
    },
    ToolCallDelta {
        content_index: usize,
        delta: String,
    },
    ToolCallEnd {
        content_index: usize,
        tool_call: ToolCall,
    },
    Done {
        reason: StopReason,
        message: AssistantMessage,
    },
    Error {
        reason: StopReason,
        error: AssistantMessage,
    },
}

pub struct ProviderEventStream {
    rx: mpsc::Receiver<ProviderEvent>,
}

impl ProviderEventStream {
    pub fn new(rx: mpsc::Receiver<ProviderEvent>) -> Self {
        Self { rx }
    }

    pub async fn recv(&mut self) -> Option<ProviderEvent> {
        self.rx.recv().await
    }
}

impl Stream for ProviderEventStream {
    type Item = ProviderEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PromptContext {
    pub system_prompt: String,
    pub messages: Vec<Message>,
    pub tools: Vec<ToolDefinition>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use super::*;

    fn timestamp() -> DateTime<Utc> {
        Utc.timestamp_millis_opt(1_700_000_000_000)
            .single()
            .expect("valid timestamp")
    }

    fn tool_call() -> ToolCall {
        ToolCall {
            id: "call-1".to_owned(),
            name: "read_file".to_owned(),
            arguments: json!({"path": "notes.txt"}),
        }
    }

    fn assistant_message() -> AssistantMessage {
        AssistantMessage {
            content: vec![
                AssistantContent::Thinking {
                    thinking: "I should inspect the file.".to_owned(),
                    signature_field: "reasoning_content".to_owned(),
                },
                AssistantContent::Text {
                    text: "I'll inspect it.".to_owned(),
                },
                AssistantContent::ToolCall(tool_call()),
            ],
            model: "kimi-k3".to_owned(),
            provider: "moonshot".to_owned(),
            usage: Usage {
                input: 90,
                output: 12,
                cache_read: 10,
                cache_write: 0,
                reasoning: 4,
                total_tokens: 112,
            },
            stop_reason: StopReason::ToolUse,
            error_message: None,
            interrupted: false,
            timestamp: timestamp(),
        }
    }

    fn assert_round_trip<T>(value: &T)
    where
        T: Serialize + for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug,
    {
        let json = serde_json::to_value(value).expect("serialize");
        let decoded = serde_json::from_value(json.clone()).expect("deserialize");
        assert_eq!(value, &decoded);
        assert_eq!(
            json,
            serde_json::to_value(decoded).expect("serialize again")
        );
    }

    #[test]
    fn message_types_round_trip_with_stable_tags() {
        let user = Message::User(UserMessage {
            content: vec![
                UserContent::Text {
                    text: "hello".to_owned(),
                },
                UserContent::Image {
                    data: "aGVsbG8=".to_owned(),
                    mime_type: "image/png".to_owned(),
                },
            ],
            timestamp: timestamp(),
        });
        let assistant = Message::Assistant(assistant_message());
        let tool_result = Message::ToolResult(ToolResultMessage {
            tool_call_id: "call-1".to_owned(),
            tool_name: "read_file".to_owned(),
            content: vec![UserContent::Text {
                text: "contents".to_owned(),
            }],
            details: json!({"bytes": 8}),
            is_error: false,
            timestamp: timestamp(),
        });

        for message in [&user, &assistant, &tool_result] {
            assert_round_trip(message);
        }

        assert_eq!(
            serde_json::to_value(&user).expect("serialize")["role"],
            "user"
        );
        assert_eq!(
            serde_json::to_value(&assistant).expect("serialize")["role"],
            "assistant"
        );
        assert_eq!(
            serde_json::to_value(&tool_result).expect("serialize")["role"],
            "tool_result"
        );
    }

    #[test]
    fn assistant_content_and_stop_reason_tags_are_stable() {
        let content = [
            AssistantContent::Text {
                text: "hello".to_owned(),
            },
            AssistantContent::Thinking {
                thinking: "hmm".to_owned(),
                signature_field: "reasoning".to_owned(),
            },
            AssistantContent::ToolCall(tool_call()),
        ];

        for item in &content {
            assert_round_trip(item);
        }

        assert_eq!(
            serde_json::to_value(&content[0]).expect("serialize")["type"],
            "text"
        );
        assert_eq!(
            serde_json::to_value(&content[1]).expect("serialize")["type"],
            "thinking"
        );
        assert_eq!(
            serde_json::to_value(&content[2]).expect("serialize")["type"],
            "tool_call"
        );
        assert_eq!(
            serde_json::to_value(StopReason::ToolUse).expect("serialize"),
            json!("tool_use")
        );
    }

    #[test]
    fn provider_events_round_trip_with_stable_tags() {
        let events = vec![
            ProviderEvent::Start,
            ProviderEvent::TextStart { content_index: 0 },
            ProviderEvent::TextDelta {
                content_index: 0,
                delta: "hel".to_owned(),
            },
            ProviderEvent::TextEnd {
                content_index: 0,
                content: "hello".to_owned(),
            },
            ProviderEvent::ThinkingStart { content_index: 1 },
            ProviderEvent::ThinkingDelta {
                content_index: 1,
                delta: "hmm".to_owned(),
            },
            ProviderEvent::ThinkingEnd {
                content_index: 1,
                content: "hmm".to_owned(),
            },
            ProviderEvent::ToolCallStart { content_index: 2 },
            ProviderEvent::ToolCallDelta {
                content_index: 2,
                delta: r#"{"path":"#.to_owned(),
            },
            ProviderEvent::ToolCallEnd {
                content_index: 2,
                tool_call: tool_call(),
            },
            ProviderEvent::Done {
                reason: StopReason::ToolUse,
                message: assistant_message(),
            },
            ProviderEvent::Error {
                reason: StopReason::Error,
                error: AssistantMessage {
                    stop_reason: StopReason::Error,
                    error_message: Some("provider failed".to_owned()),
                    ..assistant_message()
                },
            },
        ];

        for event in &events {
            assert_round_trip(event);
        }

        assert_eq!(
            serde_json::to_value(&events[2]).expect("serialize")["type"],
            "text_delta"
        );
        assert_eq!(
            serde_json::to_value(&events[9]).expect("serialize")["type"],
            "tool_call_end"
        );
    }

    #[test]
    fn prompt_context_and_tool_definition_round_trip() {
        let context = PromptContext {
            system_prompt: "Be useful.".to_owned(),
            messages: vec![Message::Assistant(assistant_message())],
            tools: vec![ToolDefinition {
                name: "read_file".to_owned(),
                description: "Read a workspace file.".to_owned(),
                parameters: json!({
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"]
                }),
            }],
        };

        assert_round_trip(&context);
        assert_round_trip(&context.tools[0]);
    }

    #[test]
    fn usage_from_raw_separates_cache_and_reasoning_tokens() {
        let usage = Usage::from_raw(&RawUsage {
            prompt_tokens: Some(120),
            completion_tokens: Some(30),
            prompt_cache_hit_tokens: Some(15),
            prompt_tokens_details: Some(PromptTokensDetails {
                cached_tokens: Some(20),
                cache_write_tokens: Some(10),
            }),
            completion_tokens_details: Some(CompletionTokensDetails {
                reasoning_tokens: Some(12),
            }),
        });

        assert_eq!(
            usage,
            Usage {
                input: 90,
                output: 30,
                cache_read: 20,
                cache_write: 10,
                reasoning: 12,
                total_tokens: 150,
            }
        );
    }

    #[test]
    fn usage_from_raw_saturates_invalid_provider_counts() {
        let usage = Usage::from_raw(&RawUsage {
            prompt_tokens: Some(5),
            prompt_tokens_details: Some(PromptTokensDetails {
                cached_tokens: Some(7),
                cache_write_tokens: Some(3),
            }),
            ..RawUsage::default()
        });

        assert_eq!(usage.input, 0);
        assert_eq!(usage.total_tokens, 10);
    }
}
