use std::{
    pin::Pin,
    task::{Context, Poll},
};

use chrono::{DateTime, Utc};
use futures_util::Stream;
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::{Map, Value};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum Message {
    User(UserMessage),
    Assistant(AssistantMessage),
    ToolResult(ToolResultMessage),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum PublicMessage {
    User(UserMessage),
    Assistant(PublicAssistantMessage),
    ToolResult(ToolResultMessage),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryLayer {
    L1,
    L2,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MemoryBlock {
    pub layer: MemoryLayer,
    pub text: String,
    pub time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiProtocol {
    OpenAiChatCompletions,
    OpenAiResponses,
    AnthropicMessages,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProviderContextItem {
    pub origin_message: Option<ProviderContextAnchor>,
    pub wire_item_index: Option<u32>,
    pub ordinal: u32,
    pub payload: ProviderContextPayload,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderContextAnchor {
    pub message_id: String,
    pub message_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderContextPayload {
    OpenAiCompactedWindow {
        items: Vec<Value>,
        coverage: NativeCompactionCoverage,
    },
    AnthropicCompaction {
        block: Value,
        coverage: NativeCompactionCoverage,
    },
    EncryptedReasoning {
        protocol: ApiProtocol,
        item: Value,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeCompactionCoverage {
    pub through_message_seq: u64,
    pub context_fingerprint: String,
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
    pub provider_code: Option<String>,
    pub interrupted: bool,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PublicAssistantMessage {
    pub content: Vec<PublicAssistantContent>,
    pub model: String,
    pub provider: String,
    pub usage: Usage,
    pub stop_reason: StopReason,
    pub error_message: Option<String>,
    pub provider_code: Option<String>,
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
        wire_item_index: u32,
    },
    Thinking {
        thinking: String,
        signature_field: String,
        wire_item_index: u32,
    },
    ToolCall {
        tool_call: ToolCall,
        wire_item_index: u32,
    },
    RejectedToolCall {
        rejected: RejectedToolCall,
        wire_item_index: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PublicAssistantContent {
    Text {
        text: String,
        wire_item_index: u32,
    },
    Thinking {
        thinking: String,
        signature_field: String,
        wire_item_index: u32,
    },
    ToolCall {
        tool_call: ToolCall,
        wire_item_index: u32,
    },
    RejectedToolCall {
        rejected: RejectedToolCall,
        wire_item_index: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: ValidatedToolArguments,
}

/// Live construction is reserved for the schema-validating assembler.
/// Deserialization only restores object-shaped transcript data and does not
/// grant permission to execute a replayed tool call.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ValidatedToolArguments(Map<String, Value>);

impl ValidatedToolArguments {
    pub fn as_object(&self) -> &Map<String, Value> {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ValidatedToolArguments {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Value::deserialize(deserializer)? {
            Value::Object(arguments) => Ok(Self(arguments)),
            _ => Err(de::Error::custom(
                "validated tool arguments must be a JSON object",
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolArgsPreview(Value);

impl ToolArgsPreview {
    pub(crate) fn new(value: Value) -> Self {
        Self(value)
    }

    pub fn as_value(&self) -> &Value {
        &self.0
    }
}

impl PartialEq<Value> for ToolArgsPreview {
    fn eq(&self, other: &Value) -> bool {
        self.0 == *other
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectedToolCall {
    pub id: String,
    pub name: String,
    pub error: ToolArgumentError,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolArgumentError {
    InvalidJson,
    NonObject,
    SchemaViolation,
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
            total_tokens: input
                .saturating_add(output)
                .saturating_add(cache_read)
                .saturating_add(cache_write),
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

#[derive(Clone, Debug, PartialEq)]
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
    ToolCallPreview {
        content_index: usize,
        preview: ToolArgsPreview,
    },
    ToolCallEnd {
        content_index: usize,
        tool_call: ToolCall,
    },
    ToolCallRejected {
        content_index: usize,
        rejected: RejectedToolCall,
    },
    ReasoningSummaryStart {
        content_index: usize,
    },
    ReasoningSummaryDelta {
        content_index: usize,
        delta: String,
    },
    ReasoningSummaryEnd {
        content_index: usize,
        content: String,
    },
    Done {
        reason: StopReason,
        output: ProviderOutput,
    },
    Error {
        reason: StopReason,
        output: ProviderOutput,
    },
}

impl ProviderEvent {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Done { .. } | Self::Error { .. })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProviderOutput {
    pub message: AssistantMessage,
    pub provider_context: Vec<ProviderContextFragment>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProviderContextFragment {
    pub wire_item_index: Option<u32>,
    pub payload: ProviderContextPayload,
}

pub struct ProviderEventStream {
    rx: Option<mpsc::Receiver<ProviderEvent>>,
    cancel: CancellationToken,
    model: String,
    provider: String,
    terminal_emitted: bool,
}

impl ProviderEventStream {
    pub fn new(
        rx: mpsc::Receiver<ProviderEvent>,
        cancel: CancellationToken,
        model: impl Into<String>,
        provider: impl Into<String>,
    ) -> Self {
        Self {
            rx: Some(rx),
            cancel,
            model: model.into(),
            provider: provider.into(),
            terminal_emitted: false,
        }
    }

    pub async fn recv(&mut self) -> Option<ProviderEvent> {
        if self.terminal_emitted {
            return None;
        }

        let event = match self.rx.as_mut() {
            Some(rx) => rx.recv().await,
            None => return None,
        };
        Some(match event {
            Some(event) => self.accept_event(event),
            None => self.synthesize_terminal(),
        })
    }

    fn accept_event(&mut self, event: ProviderEvent) -> ProviderEvent {
        if event.is_terminal() {
            self.fuse();
        }
        event
    }

    fn synthesize_terminal(&mut self) -> ProviderEvent {
        let cancelled = self.cancel.is_cancelled();
        let reason = if cancelled {
            StopReason::Aborted
        } else {
            StopReason::Error
        };
        let error_message = if cancelled {
            "provider stream cancelled"
        } else {
            "provider stream ended without a terminal event"
        };
        let provider_code = if cancelled {
            "cancelled"
        } else {
            "stream_ended_without_terminal_event"
        };

        let event = ProviderEvent::Error {
            reason,
            output: ProviderOutput {
                message: AssistantMessage {
                    content: Vec::new(),
                    model: self.model.clone(),
                    provider: self.provider.clone(),
                    usage: Usage::default(),
                    stop_reason: reason,
                    error_message: Some(error_message.to_owned()),
                    provider_code: Some(provider_code.to_owned()),
                    interrupted: cancelled,
                    timestamp: Utc::now(),
                },
                provider_context: Vec::new(),
            },
        };
        self.fuse();
        event
    }

    fn fuse(&mut self) {
        self.terminal_emitted = true;
        if let Some(mut rx) = self.rx.take() {
            const AUDIT_LIMIT: usize = 32;
            let (ignored, more_queued) = audit_queued_events(&mut rx, AUDIT_LIMIT);
            if ignored > 0 {
                tracing::warn!(
                    ignored,
                    audit_limit = AUDIT_LIMIT,
                    more_queued,
                    "discarded provider events queued after terminal event"
                );
            }
        }
    }
}

fn audit_queued_events(
    rx: &mut mpsc::Receiver<ProviderEvent>,
    audit_limit: usize,
) -> (usize, bool) {
    let mut ignored = 0;
    while ignored < audit_limit && rx.try_recv().is_ok() {
        ignored += 1;
    }
    let more_queued = ignored == audit_limit && rx.try_recv().is_ok();
    (ignored, more_queued)
}

impl Stream for ProviderEventStream {
    type Item = ProviderEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminal_emitted {
            return Poll::Ready(None);
        }

        let polled = match self.rx.as_mut() {
            Some(rx) => rx.poll_recv(cx),
            None => return Poll::Ready(None),
        };
        match polled {
            Poll::Ready(Some(event)) => Poll::Ready(Some(self.accept_event(event))),
            Poll::Ready(None) => Poll::Ready(Some(self.synthesize_terminal())),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PromptContext {
    pub system_prompt: String,
    pub memory_blocks: Vec<MemoryBlock>,
    pub messages: Vec<ContextMessage>,
    pub provider_context: Vec<ProviderContextItem>,
    pub tools: Vec<ToolDefinition>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum ContextMessage {
    Persisted {
        id: String,
        seq: u64,
        message: Message,
    },
    Synthetic {
        message: Message,
    },
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
            arguments: ValidatedToolArguments(
                json!({"path": "notes.txt"})
                    .as_object()
                    .expect("object")
                    .clone(),
            ),
        }
    }

    fn assistant_message() -> AssistantMessage {
        AssistantMessage {
            content: vec![
                AssistantContent::Thinking {
                    thinking: "I should inspect the file.".to_owned(),
                    signature_field: "reasoning_content".to_owned(),
                    wire_item_index: 0,
                },
                AssistantContent::Text {
                    text: "I'll inspect it.".to_owned(),
                    wire_item_index: 1,
                },
                AssistantContent::ToolCall {
                    tool_call: tool_call(),
                    wire_item_index: 2,
                },
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
            provider_code: Some("tool_calls".to_owned()),
            interrupted: false,
            timestamp: timestamp(),
        }
    }

    fn provider_output() -> ProviderOutput {
        ProviderOutput {
            message: assistant_message(),
            provider_context: Vec::new(),
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
                wire_item_index: 0,
            },
            AssistantContent::Thinking {
                thinking: "hmm".to_owned(),
                signature_field: "reasoning".to_owned(),
                wire_item_index: 1,
            },
            AssistantContent::ToolCall {
                tool_call: tool_call(),
                wire_item_index: 2,
            },
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
    fn prompt_context_and_tool_definition_round_trip() {
        let context = PromptContext {
            system_prompt: "Be useful.".to_owned(),
            memory_blocks: vec![MemoryBlock {
                layer: MemoryLayer::L1,
                text: "The user prefers concise replies.".to_owned(),
                time_range: None,
            }],
            messages: vec![ContextMessage::Persisted {
                id: "message-1".to_owned(),
                seq: 1,
                message: Message::Assistant(assistant_message()),
            }],
            provider_context: Vec::new(),
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
    fn validated_tool_arguments_reject_non_objects_on_replay() {
        assert!(serde_json::from_value::<ValidatedToolArguments>(json!({"path": "ok"})).is_ok());
        for invalid in [
            json!(["not", "an", "object"]),
            json!("command"),
            json!(null),
        ] {
            let error = serde_json::from_value::<ValidatedToolArguments>(invalid)
                .expect_err("non-object arguments must fail");
            assert!(
                error
                    .to_string()
                    .contains("validated tool arguments must be a JSON object")
            );
        }
    }

    #[tokio::test]
    async fn provider_stream_fuses_after_terminal_event() {
        use futures_util::StreamExt;

        let (tx, rx) = mpsc::channel(4);
        tx.send(ProviderEvent::Done {
            reason: StopReason::ToolUse,
            output: provider_output(),
        })
        .await
        .expect("terminal event");
        tx.send(ProviderEvent::TextDelta {
            content_index: 0,
            delta: "late".to_owned(),
        })
        .await
        .expect("queued invalid event");

        let mut stream =
            ProviderEventStream::new(rx, CancellationToken::new(), "kimi-k3", "moonshot");
        assert!(matches!(
            stream.next().await,
            Some(ProviderEvent::Done { .. })
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), stream.next())
                .await
                .expect("fused stream returns immediately")
                .is_none()
        );
    }

    #[test]
    fn terminal_queue_audit_reports_when_more_events_remain() {
        let (tx, mut rx) = mpsc::channel(34);
        for content_index in 0..34 {
            tx.try_send(ProviderEvent::TextDelta {
                content_index,
                delta: "late".to_owned(),
            })
            .expect("queue test event");
        }

        assert_eq!(audit_queued_events(&mut rx, 32), (32, true));
    }

    #[tokio::test]
    async fn provider_stream_synthesizes_one_terminal_event_on_eof() {
        let (tx, rx) = mpsc::channel(1);
        drop(tx);
        let mut stream =
            ProviderEventStream::new(rx, CancellationToken::new(), "kimi-k3", "moonshot");

        let event = stream.recv().await.expect("synthetic terminal event");
        match event {
            ProviderEvent::Error { reason, output } => {
                assert_eq!(reason, StopReason::Error);
                assert_eq!(
                    output.message.error_message.as_deref(),
                    Some("provider stream ended without a terminal event")
                );
            }
            other => panic!("unexpected event: {other:?}"),
        }
        assert!(stream.recv().await.is_none());
    }

    #[tokio::test]
    async fn provider_stream_classifies_cancelled_eof_as_aborted() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let (tx, rx) = mpsc::channel(1);
        drop(tx);
        let mut stream = ProviderEventStream::new(rx, cancel, "kimi-k3", "moonshot");

        assert!(matches!(
            stream.recv().await,
            Some(ProviderEvent::Error {
                reason: StopReason::Aborted,
                ..
            })
        ));
        assert!(stream.recv().await.is_none());
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

    #[test]
    fn usage_from_raw_saturates_total_at_u64_max() {
        let usage = Usage::from_raw(&RawUsage {
            prompt_tokens: Some(u64::MAX),
            completion_tokens: Some(1),
            ..RawUsage::default()
        });

        assert_eq!(usage.input, u64::MAX);
        assert_eq!(usage.total_tokens, u64::MAX);
    }
}
