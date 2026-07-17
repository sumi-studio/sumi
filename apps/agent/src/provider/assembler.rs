use std::collections::HashMap;

use chrono::Utc;
use serde_json::{Map, Value, json};

use super::{
    partial_json::parse_streaming,
    types::{
        AssistantContent, AssistantMessage, ProviderEvent, RawUsage, StopReason, ToolCall, Usage,
    },
};

pub struct MessageAssembler {
    model: String,
    provider: String,
    content: Vec<AssistantContent>,
    text_index: Option<usize>,
    thinking_index: Option<usize>,
    tool_by_stream_index: HashMap<u64, usize>,
    tool_by_id: HashMap<String, usize>,
    partial_args: HashMap<usize, String>,
    usage: Usage,
    finish_reason: Option<StopReason>,
    finish_error: Option<String>,
    started: bool,
}

impl MessageAssembler {
    pub fn new(model: impl Into<String>, provider: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            provider: provider.into(),
            content: Vec::new(),
            text_index: None,
            thinking_index: None,
            tool_by_stream_index: HashMap::new(),
            tool_by_id: HashMap::new(),
            partial_args: HashMap::new(),
            usage: Usage::default(),
            finish_reason: None,
            finish_error: None,
            started: false,
        }
    }

    pub fn push_chunk(&mut self, chunk: &Value) -> Vec<ProviderEvent> {
        let mut events = Vec::new();
        if !self.started {
            self.started = true;
            events.push(ProviderEvent::Start);
        }

        if let Some(model) = chunk.get("model").and_then(Value::as_str)
            && !model.is_empty()
            && model != self.model
        {
            tracing::debug!(
                requested_model = %self.model,
                response_model = model,
                "provider returned a different model label"
            );
        }

        let choice = chunk
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first());
        let usage = chunk
            .get("usage")
            .or_else(|| choice.and_then(|choice| choice.get("usage")));
        if let Some(raw) = usage.and_then(parse_usage) {
            self.usage = Usage::from_raw(&raw);
        }

        let Some(choice) = choice else {
            return events;
        };
        if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
            let (stop_reason, error) = map_finish_reason(reason);
            self.finish_reason = Some(stop_reason);
            self.finish_error = error;
        }

        let Some(delta) = choice.get("delta").and_then(Value::as_object) else {
            return events;
        };

        if let Some(text) = delta.get("content").and_then(Value::as_str)
            && !text.is_empty()
        {
            let (index, created) = self.ensure_text();
            if created {
                events.push(ProviderEvent::TextStart {
                    content_index: index,
                });
            }
            let AssistantContent::Text { text: output } = &mut self.content[index] else {
                return events;
            };
            output.push_str(text);
            events.push(ProviderEvent::TextDelta {
                content_index: index,
                delta: text.to_owned(),
            });
        }

        if let Some((field, thinking)) = first_reasoning(delta) {
            let signature = if self.provider == "opencode-go" && field == "reasoning" {
                "reasoning_content"
            } else {
                field
            };
            let (index, created) = self.ensure_thinking(signature);
            if created {
                events.push(ProviderEvent::ThinkingStart {
                    content_index: index,
                });
            }
            let AssistantContent::Thinking {
                thinking: output, ..
            } = &mut self.content[index]
            else {
                return events;
            };
            output.push_str(thinking);
            events.push(ProviderEvent::ThinkingDelta {
                content_index: index,
                delta: thinking.to_owned(),
            });
        }

        if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
            for tool_delta in tool_calls {
                let (index, created) = self.ensure_tool(tool_delta);
                if created {
                    events.push(ProviderEvent::ToolCallStart {
                        content_index: index,
                    });
                }

                let id = tool_delta.get("id").and_then(Value::as_str);
                let function = tool_delta.get("function").and_then(Value::as_object);
                let name = function
                    .and_then(|function| function.get("name"))
                    .and_then(Value::as_str);
                let arguments = function
                    .and_then(|function| function.get("arguments"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();

                let AssistantContent::ToolCall(call) = &mut self.content[index] else {
                    continue;
                };
                if call.id.is_empty()
                    && let Some(id) = id
                {
                    call.id = id.to_owned();
                    self.tool_by_id.insert(id.to_owned(), index);
                }
                if call.name.is_empty()
                    && let Some(name) = name
                {
                    call.name = name.to_owned();
                }
                // 引数の確定パースは finish() の一回だけ。delta ごとに
                // 蓄積全体を parse_streaming すると O(n²) になる。
                let partial = self.partial_args.entry(index).or_default();
                if !arguments.is_empty() {
                    partial.push_str(arguments);
                }
                events.push(ProviderEvent::ToolCallDelta {
                    content_index: index,
                    delta: arguments.to_owned(),
                });
            }
        }

        events
    }

    pub fn finish(mut self, cancelled: bool) -> Vec<ProviderEvent> {
        let mut events = Vec::new();
        if !self.started {
            events.push(ProviderEvent::Start);
        }

        for (index, block) in self.content.iter_mut().enumerate() {
            match block {
                AssistantContent::Text { text } => events.push(ProviderEvent::TextEnd {
                    content_index: index,
                    content: text.clone(),
                }),
                AssistantContent::Thinking { thinking, .. } => {
                    events.push(ProviderEvent::ThinkingEnd {
                        content_index: index,
                        content: thinking.clone(),
                    });
                }
                AssistantContent::ToolCall(call) => {
                    // Best-effort salvage, same chain as pi. Truncated
                    // arguments are handled by the Length-stop bulk failure
                    // in the agent loop, not rejected here.
                    if let Some(partial) = self.partial_args.get(&index) {
                        call.arguments = object_or_empty(parse_streaming(partial));
                    }
                    events.push(ProviderEvent::ToolCallEnd {
                        content_index: index,
                        tool_call: call.clone(),
                    });
                }
            }
        }

        let (reason, error_message) = if cancelled {
            (StopReason::Aborted, Some("Request was aborted".to_owned()))
        } else if let Some(reason) = self.finish_reason {
            (reason, self.finish_error)
        } else {
            (
                StopReason::Error,
                Some("Stream ended without finish_reason".to_owned()),
            )
        };
        let message = AssistantMessage {
            content: self.content,
            model: self.model,
            provider: self.provider,
            usage: self.usage,
            stop_reason: reason,
            error_message,
            interrupted: false,
            timestamp: Utc::now(),
        };

        if matches!(reason, StopReason::Error | StopReason::Aborted) {
            events.push(ProviderEvent::Error {
                reason,
                error: message,
            });
        } else {
            events.push(ProviderEvent::Done { reason, message });
        }
        events
    }

    pub fn fail(self, message: impl Into<String>, cancelled: bool) -> Vec<ProviderEvent> {
        let mut events = Vec::new();
        if !self.started {
            events.push(ProviderEvent::Start);
        }
        let reason = if cancelled {
            StopReason::Aborted
        } else {
            StopReason::Error
        };
        events.push(ProviderEvent::Error {
            reason,
            error: AssistantMessage {
                content: self.content,
                model: self.model,
                provider: self.provider,
                usage: self.usage,
                stop_reason: reason,
                error_message: Some(message.into()),
                interrupted: false,
                timestamp: Utc::now(),
            },
        });
        events
    }

    fn ensure_text(&mut self) -> (usize, bool) {
        if let Some(index) = self.text_index {
            return (index, false);
        }
        let index = self.content.len();
        self.content.push(AssistantContent::Text {
            text: String::new(),
        });
        self.text_index = Some(index);
        (index, true)
    }

    fn ensure_thinking(&mut self, signature_field: &str) -> (usize, bool) {
        if let Some(index) = self.thinking_index {
            return (index, false);
        }
        let index = self.content.len();
        self.content.push(AssistantContent::Thinking {
            thinking: String::new(),
            signature_field: signature_field.to_owned(),
        });
        self.thinking_index = Some(index);
        (index, true)
    }

    fn ensure_tool(&mut self, delta: &Value) -> (usize, bool) {
        let stream_index = delta.get("index").and_then(Value::as_u64);
        let id = delta.get("id").and_then(Value::as_str);
        if let Some(index) = stream_index
            .and_then(|key| self.tool_by_stream_index.get(&key))
            .or_else(|| id.and_then(|key| self.tool_by_id.get(key)))
            .copied()
        {
            if let Some(stream_index) = stream_index {
                self.tool_by_stream_index.insert(stream_index, index);
            }
            if let Some(id) = id {
                self.tool_by_id.insert(id.to_owned(), index);
            }
            return (index, false);
        }

        let index = self.content.len();
        self.content.push(AssistantContent::ToolCall(ToolCall {
            id: id.unwrap_or_default().to_owned(),
            name: String::new(),
            arguments: json!({}),
        }));
        if let Some(stream_index) = stream_index {
            self.tool_by_stream_index.insert(stream_index, index);
        }
        if let Some(id) = id {
            self.tool_by_id.insert(id.to_owned(), index);
        }
        (index, true)
    }
}

fn first_reasoning(delta: &Map<String, Value>) -> Option<(&str, &str)> {
    ["reasoning_content", "reasoning", "reasoning_text"]
        .into_iter()
        .find_map(|field| {
            delta
                .get(field)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(|value| (field, value))
        })
}

fn parse_usage(value: &Value) -> Option<RawUsage> {
    serde_json::from_value(value.clone()).ok()
}

fn object_or_empty(value: Value) -> Value {
    if value.is_object() { value } else { json!({}) }
}

fn map_finish_reason(reason: &str) -> (StopReason, Option<String>) {
    match reason {
        "stop" | "end" => (StopReason::Stop, None),
        "length" => (StopReason::Length, None),
        "tool_calls" | "function_call" => (StopReason::ToolUse, None),
        other => (
            StopReason::Error,
            Some(format!("Provider finish_reason: {other}")),
        ),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn assembles_text_reasoning_tools_usage_and_finish_reason() {
        let mut assembler = MessageAssembler::new("kimi-k3", "moonshot");
        let first = assembler.push_chunk(&json!({
            "model": "kimi-k3",
            "choices": [{
                "delta": {
                    "reasoning_content": "think",
                    "content": "hello",
                    "tool_calls": [{
                        "index": 0,
                        "id": "call-1",
                        "function": {"name": "read_file", "arguments": "{\"path\":"}
                    }]
                }
            }]
        }));
        let second = assembler.push_chunk(&json!({
            "choices": [{
                "delta": {
                    "tool_calls": [{
                        "index": 0,
                        "function": {"arguments": "\"a.txt\"}"}
                    }]
                },
                "finish_reason": "tool_calls",
                "usage": {
                    "prompt_tokens": 20,
                    "completion_tokens": 5,
                    "prompt_tokens_details": {"cached_tokens": 4}
                }
            }]
        }));
        let finished = assembler.finish(false);

        assert!(matches!(first[0], ProviderEvent::Start));
        assert!(second.iter().any(|event| matches!(
            event,
            ProviderEvent::ToolCallDelta { delta, .. } if delta == "\"a.txt\"}"
        )));
        let ProviderEvent::Done { reason, message } = finished.last().expect("terminal") else {
            panic!("done");
        };
        assert_eq!(*reason, StopReason::ToolUse);
        assert_eq!(message.usage.input, 16);
        assert_eq!(message.usage.cache_read, 4);
        let AssistantContent::ToolCall(call) = &message.content[2] else {
            panic!("tool call");
        };
        assert_eq!(call.arguments, json!({"path":"a.txt"}));
    }

    #[test]
    fn missing_finish_reason_is_an_error() {
        let mut assembler = MessageAssembler::new("model", "provider");
        assembler.push_chunk(&json!({"choices":[{"delta":{"content":"partial"}}]}));

        let events = assembler.finish(false);

        let ProviderEvent::Error { error, .. } = events.last().expect("terminal") else {
            panic!("error");
        };
        assert_eq!(
            error.error_message.as_deref(),
            Some("Stream ended without finish_reason")
        );
    }

    #[test]
    fn tool_blocks_are_resolved_by_index_then_id() {
        let mut assembler = MessageAssembler::new("model", "provider");
        assembler.push_chunk(&json!({
            "choices":[{"delta":{"tool_calls":[{
                "index": 2,
                "function":{"name":"bash","arguments":"{\"command\":"}
            }]}}]
        }));
        assembler.push_chunk(&json!({
            "choices":[{"delta":{"tool_calls":[{
                "index": 2,
                "id":"call-2",
                "function":{"arguments":"\"pwd\"}"}
            }]},"finish_reason":"tool_calls"}]
        }));

        let events = assembler.finish(false);
        let ProviderEvent::Done { message, .. } = events.last().expect("terminal") else {
            panic!("done");
        };
        assert_eq!(message.content.len(), 1);
        let AssistantContent::ToolCall(call) = &message.content[0] else {
            panic!("tool");
        };
        assert_eq!(call.id, "call-2");
        assert_eq!(call.arguments, json!({"command":"pwd"}));
    }

    #[test]
    fn response_model_label_does_not_change_the_requested_model_identity() {
        let mut assembler = MessageAssembler::new("kimi-k3", "moonshot");
        assembler.push_chunk(&json!({
            "model": "kimi-k3-202607",
            "choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]
        }));

        let events = assembler.finish(false);

        let ProviderEvent::Done { message, .. } = events.last().expect("terminal") else {
            panic!("done");
        };
        assert_eq!(message.model, "kimi-k3");
    }

    #[test]
    fn transport_failure_does_not_close_partial_blocks() {
        let mut assembler = MessageAssembler::new("model", "provider");
        assembler.push_chunk(&json!({
            "choices":[{"delta":{
                "content":"partial",
                "tool_calls":[{
                    "index":0,
                    "id":"call-1",
                    "function":{"name":"read_file","arguments":"{\"path\":"}
                }]
            }}]
        }));

        let events = assembler.fail("connection lost", false);

        assert!(!events.iter().any(|event| matches!(
            event,
            ProviderEvent::TextEnd { .. } | ProviderEvent::ToolCallEnd { .. }
        )));
        assert!(matches!(
            events.last(),
            Some(ProviderEvent::Error {
                reason: StopReason::Error,
                ..
            })
        ));
    }

    #[test]
    fn tool_arguments_with_raw_control_characters_are_repaired_on_finish() {
        let mut assembler = MessageAssembler::new("model", "provider");
        assembler.push_chunk(&json!({
            "choices":[{"delta":{"tool_calls":[{
                "index":0,
                "id":"call-1",
                "function":{"name":"write_file","arguments":"{\"text\":\"a\nb\"}"}
            }]},"finish_reason":"tool_calls"}]
        }));

        let events = assembler.finish(false);

        let ProviderEvent::Done { message, .. } = events.last().expect("terminal") else {
            panic!("done");
        };
        let AssistantContent::ToolCall(call) = &message.content[0] else {
            panic!("tool");
        };
        assert_eq!(call.arguments, json!({"text":"a\nb"}));
    }

    #[test]
    fn truncated_tool_arguments_are_salvaged_best_effort_on_finish() {
        // 切断の検出・拒否は Length 停止時のツール一括失敗 (agent ループ側,
        // 計画書#19) が受け持つ。assembler は pi と同じくサルベージで確定する。
        let mut assembler = MessageAssembler::new("model", "provider");
        assembler.push_chunk(&json!({
            "choices":[{"delta":{"tool_calls":[{
                "index":0,
                "id":"call-1",
                "function":{
                    "name":"read_file",
                    "arguments":"{\"path\":\"/etc/passw"
                }
            }]},"finish_reason":"length"}]
        }));

        let events = assembler.finish(false);

        let ProviderEvent::Done { reason, message } = events.last().expect("terminal") else {
            panic!("done");
        };
        assert_eq!(*reason, StopReason::Length);
        let AssistantContent::ToolCall(call) = &message.content[0] else {
            panic!("tool");
        };
        assert_eq!(call.arguments, json!({"path":"/etc/passw"}));
    }

    #[test]
    fn non_object_or_missing_tool_arguments_finalize_as_empty_object() {
        for function in [
            json!({"name":"no_args","arguments":"[]"}),
            json!({"name":"no_args"}),
            json!({"name":"no_args","arguments":""}),
        ] {
            let mut assembler = MessageAssembler::new("model", "provider");
            assembler.push_chunk(&json!({
                "choices":[{"delta":{"tool_calls":[{
                    "index":0,
                    "id":"call-1",
                    "function": function
                }]},"finish_reason":"tool_calls"}]
            }));

            let events = assembler.finish(false);

            let ProviderEvent::Done { message, .. } = events.last().expect("terminal") else {
                panic!("done");
            };
            let AssistantContent::ToolCall(call) = &message.content[0] else {
                panic!("tool");
            };
            assert_eq!(call.arguments, json!({}));
        }
    }
}
