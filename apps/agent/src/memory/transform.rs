//! Protocol-neutral replay normalization over the L0 send view.

use std::collections::{HashMap, HashSet};

use sha2::{Digest, Sha256};

use crate::provider::types::{
    ApiProtocol, AssistantContent, AssistantMessage, ContextMessage, Message, ProviderOrigin,
    RejectedToolCall, StopReason, ToolCall, ToolResultMessage, UserContent, UserMessage,
};

/// Marker appended to an interrupted assistant message so the model can tell the
/// previous response was cut off by the user. This text is injected at replay
/// time and is never persisted.
pub const INTERRUPTION_MARKER: &str = "[この応答はユーザーの割り込みにより中断された]";

fn protocol_requires_bounded_tool_ids(protocol: ApiProtocol) -> bool {
    matches!(
        protocol,
        ApiProtocol::OpenAiChatCompletions | ApiProtocol::OpenAiResponses
    )
}

#[derive(Clone)]
struct PendingTool {
    call: ToolCall,
    timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone)]
struct PendingRejection {
    rejected: RejectedToolCall,
    timestamp: chrono::DateTime<chrono::Utc>,
}

/// Build a provider send view without mutating persisted identities or transcript data.
pub fn transform(messages: &[ContextMessage], destination: &ProviderOrigin) -> Vec<ContextMessage> {
    let normalized = normalize_messages(messages, destination);
    let mut result = Vec::with_capacity(normalized.len());
    let mut pending_tools = Vec::<PendingTool>::new();
    let mut seen_tool_results = HashSet::<String>::new();
    let mut pending_rejections = Vec::<PendingRejection>::new();
    let mut rejected_ids = HashSet::<String>::new();
    let mut seen_call_ids = HashSet::<String>::new();
    let mut accepted_call_ids = HashSet::<String>::new();

    for context in normalized {
        match context_message(&context) {
            Message::Assistant(message) => {
                flush_pending_tools(&mut result, &mut pending_tools, &seen_tool_results);
                seen_tool_results.clear();
                flush_rejections(&mut result, &mut pending_rejections);
                rejected_ids.clear();
                seen_call_ids.clear();
                accepted_call_ids.clear();

                if should_skip_assistant(message) {
                    continue;
                }

                let mut assistant = message.clone();
                let mut retained = Vec::with_capacity(assistant.content.len());
                let mut has_sendable_content = false;
                for content in assistant.content {
                    match content {
                        AssistantContent::ToolCall { ref tool_call, .. } => {
                            if seen_call_ids.insert(tool_call.id.clone()) {
                                accepted_call_ids.insert(tool_call.id.clone());
                                has_sendable_content = true;
                                pending_tools.push(PendingTool {
                                    call: tool_call.clone(),
                                    timestamp: assistant.timestamp,
                                });
                                retained.push(content);
                            } else {
                                pending_rejections.push(PendingRejection {
                                    rejected: RejectedToolCall {
                                        id: tool_call.id.clone(),
                                        name: tool_call.name.clone(),
                                        error:
                                            crate::provider::types::ToolArgumentError::InvalidJson,
                                    },
                                    timestamp: assistant.timestamp,
                                });
                            }
                        }
                        AssistantContent::RejectedToolCall { rejected, .. } => {
                            rejected_ids.insert(rejected.id.clone());
                            pending_rejections.push(PendingRejection {
                                rejected,
                                timestamp: assistant.timestamp,
                            });
                        }
                        AssistantContent::Text { ref text, .. } => {
                            has_sendable_content |= !text.is_empty();
                            retained.push(content);
                        }
                        AssistantContent::Thinking { ref thinking, .. } => {
                            has_sendable_content |= !thinking.is_empty();
                            retained.push(content);
                        }
                    }
                }
                if has_sendable_content {
                    assistant.content = retained;
                    if assistant.interrupted {
                        append_interruption_marker(&mut assistant.content);
                    }
                    result.push(with_message(context, Message::Assistant(assistant)));
                }
            }
            Message::ToolResult(tool_result) => {
                if rejected_ids.contains(&tool_result.tool_call_id) && tool_result.is_error {
                    continue;
                }
                let is_new_result = seen_tool_results.insert(tool_result.tool_call_id.clone());
                if accepted_call_ids.contains(&tool_result.tool_call_id) && !is_new_result {
                    continue;
                }
                result.push(context);
            }
            Message::User(_) => {
                flush_pending_tools(&mut result, &mut pending_tools, &seen_tool_results);
                seen_tool_results.clear();
                flush_rejections(&mut result, &mut pending_rejections);
                rejected_ids.clear();
                seen_call_ids.clear();
                accepted_call_ids.clear();
                result.push(context);
            }
        }
    }

    flush_pending_tools(&mut result, &mut pending_tools, &seen_tool_results);
    flush_rejections(&mut result, &mut pending_rejections);
    result
}

fn normalize_messages(
    messages: &[ContextMessage],
    destination: &ProviderOrigin,
) -> Vec<ContextMessage> {
    let mut id_map = ToolIdMap::default();
    messages
        .iter()
        .cloned()
        .map(|context| {
            let message = match context_message(&context) {
                Message::User(message) => {
                    id_map = ToolIdMap::default();
                    Message::User(message.clone())
                }
                Message::ToolResult(message) => {
                    let mut message = message.clone();
                    if let Some(normalized) =
                        id_map.original_to_normalized.get(&message.tool_call_id)
                    {
                        message.tool_call_id = normalized.clone();
                    }
                    Message::ToolResult(message)
                }
                Message::Assistant(message) => {
                    // A mapping belongs to this assistant flow and its
                    // following tool results only.
                    id_map = ToolIdMap::default();
                    let mut message = message.clone();
                    let keep_thinking = may_replay_thinking(&context, &message, destination);
                    let normalize_tool_ids =
                        protocol_requires_bounded_tool_ids(destination.protocol)
                            && !(same_origin(&context, destination)
                                && message.model == destination.model);
                    message.content = message
                        .content
                        .into_iter()
                        .filter_map(|content| match content {
                            AssistantContent::Thinking { .. } if !keep_thinking => None,
                            AssistantContent::ToolCall {
                                mut tool_call,
                                wire_item_index,
                            } => {
                                if normalize_tool_ids {
                                    tool_call.id = mapped_tool_id(&tool_call.id, &mut id_map);
                                }
                                Some(AssistantContent::ToolCall {
                                    tool_call,
                                    wire_item_index,
                                })
                            }
                            AssistantContent::RejectedToolCall {
                                mut rejected,
                                wire_item_index,
                            } => {
                                if normalize_tool_ids {
                                    rejected.id = mapped_tool_id(&rejected.id, &mut id_map);
                                }
                                Some(AssistantContent::RejectedToolCall {
                                    rejected,
                                    wire_item_index,
                                })
                            }
                            other => Some(other),
                        })
                        .collect();
                    Message::Assistant(message)
                }
            };
            with_message(context, message)
        })
        .collect()
}

fn same_origin(context: &ContextMessage, destination: &ProviderOrigin) -> bool {
    let ContextMessage::Persisted { message, .. } = context else {
        return false;
    };
    let Message::Assistant(message) = message else {
        return false;
    };
    message.origin == *destination
}

fn may_replay_thinking(
    context: &ContextMessage,
    message: &AssistantMessage,
    destination: &ProviderOrigin,
) -> bool {
    same_origin(context, destination) && message.model == destination.model
}

#[derive(Default)]
struct ToolIdMap {
    original_to_normalized: HashMap<String, String>,
    normalized_to_original: HashMap<String, String>,
}

fn mapped_tool_id(id: &str, map: &mut ToolIdMap) -> String {
    if let Some(normalized) = map.original_to_normalized.get(id) {
        return normalized.clone();
    }
    let preferred = if id.len() <= 40
        && !id.is_empty()
        && id
            .chars()
            .all(|character| character.is_alphanumeric() || matches!(character, '_' | '-'))
    {
        id.to_owned()
    } else {
        normalized_tool_id(id, 0)
    };
    let mut normalized = preferred;
    let mut attempt = 1u32;
    while map
        .normalized_to_original
        .get(&normalized)
        .is_some_and(|original| original != id)
    {
        normalized = normalized_tool_id(id, attempt);
        attempt = attempt.saturating_add(1);
    }
    map.original_to_normalized
        .insert(id.to_owned(), normalized.clone());
    map.normalized_to_original
        .insert(normalized.clone(), id.to_owned());
    normalized
}

fn normalized_tool_id(id: &str, attempt: u32) -> String {
    const MAX_ID_BYTES: usize = 40;
    const DIGEST_BYTES: usize = 5;
    const SUFFIX_BYTES: usize = 1 + DIGEST_BYTES * 2;

    let prefix = id.split('|').next().unwrap_or(id);
    let readable_candidate = prefix
        .chars()
        .map(|character| {
            if character.is_alphanumeric() || matches!(character, '_' | '-') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    let readable = utf8_prefix(&readable_candidate, MAX_ID_BYTES - SUFFIX_BYTES);
    let readable = if readable.is_empty() {
        "call".to_owned()
    } else {
        readable.to_owned()
    };
    let mut digest = Sha256::new();
    digest.update(id.as_bytes());
    digest.update(attempt.to_be_bytes());
    let digest = digest.finalize();
    let suffix = digest[..5]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{readable}-{suffix}")
}

fn utf8_prefix(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn should_skip_assistant(message: &AssistantMessage) -> bool {
    !message.interrupted && matches!(message.stop_reason, StopReason::Error | StopReason::Aborted)
}

fn flush_pending_tools(
    result: &mut Vec<ContextMessage>,
    pending: &mut Vec<PendingTool>,
    seen_results: &HashSet<String>,
) {
    for pending in pending.drain(..) {
        if seen_results.contains(&pending.call.id) {
            continue;
        }
        result.push(ContextMessage::Synthetic {
            message: Message::ToolResult(ToolResultMessage {
                tool_call_id: pending.call.id,
                tool_name: pending.call.name,
                content: vec![UserContent::Text {
                    text: "No result provided".to_owned(),
                }],
                details: serde_json::json!({"code": "missing_tool_result"}),
                is_error: true,
                timestamp: pending.timestamp,
            }),
        });
    }
}

fn flush_rejections(result: &mut Vec<ContextMessage>, pending: &mut Vec<PendingRejection>) {
    for pending in pending.drain(..) {
        result.push(ContextMessage::Synthetic {
            message: Message::User(UserMessage {
                content: vec![UserContent::Text {
                    text: format!(
                        "ツール `{}` の引数検証に失敗したため実行されませんでした。ツール呼び出しを正しい引数で再生成してください。",
                        pending.rejected.name
                    ),
                }],
                timestamp: pending.timestamp,
            }),
        });
    }
}

fn context_message(context: &ContextMessage) -> &Message {
    match context {
        ContextMessage::Persisted { message, .. } | ContextMessage::Synthetic { message } => {
            message
        }
    }
}

fn append_interruption_marker(content: &mut Vec<AssistantContent>) {
    let next_wire_item_index = content
        .iter()
        .map(|item| match item {
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
        })
        .max()
        .map(|index| index.saturating_add(1))
        .unwrap_or(0);
    content.push(AssistantContent::Text {
        text: INTERRUPTION_MARKER.to_owned(),
        wire_item_index: next_wire_item_index,
    });
}

fn with_message(context: ContextMessage, message: Message) -> ContextMessage {
    match context {
        ContextMessage::Persisted { id, seq, .. } => ContextMessage::Persisted { id, seq, message },
        ContextMessage::Synthetic { .. } => ContextMessage::Synthetic { message },
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use serde_json::{Value, json};

    use super::*;
    use crate::provider::types::{ToolArgumentError, Usage, UserMessage, ValidatedToolArguments};

    fn timestamp() -> chrono::DateTime<Utc> {
        Utc.timestamp_millis_opt(1_700_000_000_000)
            .single()
            .expect("valid timestamp")
    }

    fn args(value: Value) -> ValidatedToolArguments {
        serde_json::from_value(value).expect("object arguments")
    }

    fn tool_call(id: &str, name: &str) -> AssistantContent {
        AssistantContent::ToolCall {
            tool_call: ToolCall {
                id: id.to_owned(),
                name: name.to_owned(),
                arguments: args(json!({"value": 1})),
            },
            wire_item_index: 0,
        }
    }

    fn rejected(id: &str, name: &str) -> AssistantContent {
        AssistantContent::RejectedToolCall {
            rejected: RejectedToolCall {
                id: id.to_owned(),
                name: name.to_owned(),
                error: ToolArgumentError::SchemaViolation,
            },
            wire_item_index: 0,
        }
    }

    fn assistant(content: Vec<AssistantContent>, reason: StopReason, interrupted: bool) -> Message {
        Message::Assistant(AssistantMessage {
            content,
            model: "model-a".to_owned(),
            provider: "provider-a".to_owned(),
            origin: ProviderOrigin {
                provider_instance_id: "provider-instance-a".to_owned(),
                protocol: ApiProtocol::OpenAiChatCompletions,
                model: "model-a".to_owned(),
            },
            usage: Usage::default(),
            stop_reason: reason,
            error_message: None,
            provider_code: None,
            interrupted,
            timestamp: timestamp(),
        })
    }

    fn user(text: &str) -> Message {
        Message::User(UserMessage {
            content: vec![UserContent::Text {
                text: text.to_owned(),
            }],
            timestamp: timestamp(),
        })
    }

    fn result(id: &str, error: bool) -> Message {
        Message::ToolResult(ToolResultMessage {
            tool_call_id: id.to_owned(),
            tool_name: "tool".to_owned(),
            content: vec![UserContent::Text {
                text: "result".to_owned(),
            }],
            details: json!({}),
            is_error: error,
            timestamp: timestamp(),
        })
    }

    fn persisted(id: &str, seq: u64, message: Message) -> ContextMessage {
        ContextMessage::Persisted {
            id: id.to_owned(),
            seq,
            message,
        }
    }

    fn target() -> ProviderOrigin {
        ProviderOrigin {
            provider_instance_id: "provider-instance-a".to_owned(),
            protocol: ApiProtocol::OpenAiChatCompletions,
            model: "model-a".to_owned(),
        }
    }

    fn cross_origin_target() -> ProviderOrigin {
        let mut destination = target();
        destination.provider_instance_id = "provider-instance-b".to_owned();
        destination
    }

    fn message(context: &ContextMessage) -> &Message {
        context_message(context)
    }

    #[test]
    fn preserves_persisted_anchor_identity() {
        let output = transform(&[persisted("m1", 7, user("hello"))], &target());
        assert!(matches!(
            &output[0],
            ContextMessage::Persisted { id, seq: 7, .. } if id == "m1"
        ));
    }

    #[test]
    fn complete_tool_pair_is_unchanged() {
        let input = vec![
            persisted(
                "a",
                1,
                assistant(vec![tool_call("id", "tool")], StopReason::ToolUse, false),
            ),
            persisted("r", 2, result("id", false)),
        ];
        assert_eq!(transform(&input, &target()), input);
    }

    #[test]
    fn inserts_missing_result_at_conversation_end() {
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(vec![tool_call("id", "tool")], StopReason::ToolUse, false),
            )],
            &cross_origin_target(),
        );
        assert_eq!(output.len(), 2);
        assert!(matches!(
            &output[1],
            ContextMessage::Synthetic { message: Message::ToolResult(result) }
                if result.is_error && result.tool_call_id == "id"
        ));
    }

    #[test]
    fn inserts_missing_result_before_user_boundary() {
        let output = transform(
            &[
                persisted(
                    "a",
                    1,
                    assistant(vec![tool_call("id", "tool")], StopReason::ToolUse, false),
                ),
                persisted("u", 2, user("steer")),
            ],
            &cross_origin_target(),
        );
        assert_eq!(output.len(), 3);
        assert!(matches!(message(&output[1]), Message::ToolResult(_)));
        assert!(matches!(message(&output[2]), Message::User(_)));
    }

    #[test]
    fn inserts_only_missing_member_of_multiple_calls() {
        let output = transform(
            &[
                persisted(
                    "a",
                    1,
                    assistant(
                        vec![tool_call("one", "tool"), tool_call("two", "tool")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("r", 2, result("one", false)),
            ],
            &target(),
        );
        let results = output
            .iter()
            .filter_map(|context| match message(context) {
                Message::ToolResult(result) => Some(result),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(results.len(), 2);
        assert!(
            results
                .iter()
                .any(|result| result.tool_call_id == "two" && result.is_error)
        );
    }

    #[test]
    fn flushes_previous_orphans_before_next_assistant() {
        let output = transform(
            &[
                persisted(
                    "a1",
                    1,
                    assistant(vec![tool_call("id", "tool")], StopReason::ToolUse, false),
                ),
                persisted(
                    "a2",
                    2,
                    assistant(
                        vec![AssistantContent::Text {
                            text: "next".to_owned(),
                            wire_item_index: 0,
                        }],
                        StopReason::Stop,
                        false,
                    ),
                ),
            ],
            &target(),
        );
        assert!(matches!(message(&output[1]), Message::ToolResult(_)));
        assert!(matches!(message(&output[2]), Message::Assistant(_)));
    }

    #[test]
    fn skips_error_assistant() {
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(
                    vec![AssistantContent::Text {
                        text: "partial".to_owned(),
                        wire_item_index: 0,
                    }],
                    StopReason::Error,
                    false,
                ),
            )],
            &target(),
        );
        assert!(output.is_empty());
    }

    #[test]
    fn skips_aborted_assistant() {
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(Vec::new(), StopReason::Aborted, false),
            )],
            &target(),
        );
        assert!(output.is_empty());
    }

    #[test]
    fn keeps_interrupted_assistant_even_when_aborted_and_appends_marker() {
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(
                    vec![AssistantContent::Text {
                        text: "partial".to_owned(),
                        wire_item_index: 0,
                    }],
                    StopReason::Aborted,
                    true,
                ),
            )],
            &target(),
        );
        assert_eq!(output.len(), 1);
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("expected assistant");
        };
        assert_eq!(assistant.content.len(), 2);
        assert!(assistant.content.iter().any(|content| matches!(
            content,
            AssistantContent::Text { text, .. } if text == "partial"
        )));
        assert!(assistant.content.iter().any(|content| matches!(
            content,
            AssistantContent::Text { text, .. } if text == INTERRUPTION_MARKER
        )));
    }

    #[test]
    fn empty_interrupted_assistant_is_not_replayed() {
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(Vec::new(), StopReason::Aborted, true),
            )],
            &target(),
        );
        assert!(output.is_empty());
    }

    #[test]
    fn empty_text_interrupted_assistant_is_not_replayed() {
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(
                    vec![AssistantContent::Text {
                        text: String::new(),
                        wire_item_index: 0,
                    }],
                    StopReason::Aborted,
                    true,
                ),
            )],
            &target(),
        );
        assert!(output.is_empty());
    }

    #[test]
    fn non_interrupted_assistant_does_not_emit_interruption_marker() {
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(
                    vec![AssistantContent::Text {
                        text: "complete".to_owned(),
                        wire_item_index: 0,
                    }],
                    StopReason::Stop,
                    false,
                ),
            )],
            &target(),
        );
        assert_eq!(output.len(), 1);
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("expected assistant");
        };
        assert_eq!(assistant.content.len(), 1);
        assert!(!assistant
            .content
            .iter()
            .any(|content| matches!(content, AssistantContent::Text { text, .. } if text == INTERRUPTION_MARKER)));
    }

    #[test]
    fn interrupted_marker_is_appended_after_retained_content() {
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(
                    vec![
                        AssistantContent::Text {
                            text: "before".to_owned(),
                            wire_item_index: 0,
                        },
                        AssistantContent::Thinking {
                            thinking: "private".to_owned(),
                            signature_field: "reasoning_content".to_owned(),
                            wire_item_index: 1,
                        },
                    ],
                    StopReason::Aborted,
                    true,
                ),
            )],
            &target(),
        );
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("expected assistant");
        };
        assert_eq!(assistant.content.len(), 3);
        assert!(matches!(
            &assistant.content[2],
            AssistantContent::Text { text, .. } if text == INTERRUPTION_MARKER
        ));
    }

    #[test]
    fn preserves_empty_text_blocks_around_nonempty_content_in_wire_order() {
        let content = vec![
            AssistantContent::Text {
                text: String::new(),
                wire_item_index: 0,
            },
            AssistantContent::Text {
                text: "before".to_owned(),
                wire_item_index: 1,
            },
            AssistantContent::Text {
                text: String::new(),
                wire_item_index: 2,
            },
            AssistantContent::Text {
                text: "after".to_owned(),
                wire_item_index: 3,
            },
            AssistantContent::Text {
                text: String::new(),
                wire_item_index: 4,
            },
        ];
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(content.clone(), StopReason::Stop, false),
            )],
            &target(),
        );
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("expected assistant");
        };
        assert_eq!(assistant.content, content);
    }

    #[test]
    fn preserves_same_origin_empty_thinking_when_text_keeps_assistant() {
        let content = vec![
            AssistantContent::Thinking {
                thinking: String::new(),
                signature_field: "reasoning_content".to_owned(),
                wire_item_index: 0,
            },
            AssistantContent::Text {
                text: "answer".to_owned(),
                wire_item_index: 1,
            },
        ];
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(content.clone(), StopReason::Stop, false),
            )],
            &target(),
        );
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("expected assistant");
        };
        assert_eq!(assistant.content, content);
    }

    #[test]
    fn cross_model_thinking_only_interrupted_assistant_is_not_replayed() {
        let mut target = target();
        target.model = "model-b".to_owned();
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(
                    vec![AssistantContent::Thinking {
                        thinking: "private".to_owned(),
                        signature_field: "reasoning_content".to_owned(),
                        wire_item_index: 0,
                    }],
                    StopReason::Aborted,
                    true,
                ),
            )],
            &target,
        );
        assert!(output.is_empty());
    }

    #[test]
    fn preserves_persisted_assistant_anchor_identity_when_replayed() {
        let output = transform(
            &[persisted(
                "assistant-anchor",
                42,
                assistant(
                    vec![AssistantContent::Text {
                        text: "answer".to_owned(),
                        wire_item_index: 0,
                    }],
                    StopReason::Stop,
                    false,
                ),
            )],
            &target(),
        );
        assert!(matches!(
            &output[0],
            ContextMessage::Persisted { id, seq: 42, .. } if id == "assistant-anchor"
        ));
    }

    #[test]
    fn preserves_thinking_for_exact_target() {
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(
                    vec![AssistantContent::Thinking {
                        thinking: "private".to_owned(),
                        signature_field: "reasoning_content".to_owned(),
                        wire_item_index: 0,
                    }],
                    StopReason::Stop,
                    false,
                ),
            )],
            &target(),
        );
        assert!(matches!(
            message(&output[0]),
            Message::Assistant(AssistantMessage { content, .. })
                if matches!(content[0], AssistantContent::Thinking { .. })
        ));
    }

    #[test]
    fn empty_thinking_only_assistant_is_not_replayed() {
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(
                    vec![AssistantContent::Thinking {
                        thinking: String::new(),
                        signature_field: "reasoning_content".to_owned(),
                        wire_item_index: 0,
                    }],
                    StopReason::Stop,
                    false,
                ),
            )],
            &target(),
        );
        assert!(output.is_empty());
    }

    #[test]
    fn drops_thinking_for_cross_model() {
        let mut target = target();
        target.model = "model-b".to_owned();
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(
                    vec![
                        AssistantContent::Thinking {
                            thinking: "marker".to_owned(),
                            signature_field: "reasoning_content".to_owned(),
                            wire_item_index: 0,
                        },
                        AssistantContent::Text {
                            text: "answer".to_owned(),
                            wire_item_index: 1,
                        },
                    ],
                    StopReason::Stop,
                    false,
                ),
            )],
            &target,
        );
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("assistant");
        };
        assert_eq!(assistant.content.len(), 1);
        assert!(matches!(
            assistant.content[0],
            AssistantContent::Text { .. }
        ));
    }

    #[test]
    fn drops_thinking_for_missing_conflicting_or_mismatched_origin() {
        let mut other_instance = target();
        other_instance.provider_instance_id = "provider-instance-b".to_owned();
        let mut other_protocol = target();
        other_protocol.protocol = ApiProtocol::OpenAiResponses;
        let mut other_model = target();
        other_model.model = "model-b".to_owned();

        for destination in [other_instance, other_protocol, other_model] {
            let output = transform(
                &[persisted(
                    "a",
                    1,
                    assistant(
                        vec![AssistantContent::Thinking {
                            thinking: "marker".to_owned(),
                            signature_field: "reasoning_content".to_owned(),
                            wire_item_index: 0,
                        }],
                        StopReason::Stop,
                        false,
                    ),
                )],
                &destination,
            );
            assert!(output.is_empty());
        }

        let output = transform(
            &[ContextMessage::Synthetic {
                message: assistant(
                    vec![AssistantContent::Thinking {
                        thinking: "synthetic".to_owned(),
                        signature_field: "reasoning_content".to_owned(),
                        wire_item_index: 0,
                    }],
                    StopReason::Stop,
                    false,
                ),
            }],
            &target(),
        );
        assert!(output.is_empty());
    }

    #[test]
    fn applies_thinking_provenance_per_persisted_anchor() {
        let thinking = |text: &str| {
            assistant(
                vec![AssistantContent::Thinking {
                    thinking: text.to_owned(),
                    signature_field: "reasoning_content".to_owned(),
                    wire_item_index: 0,
                }],
                StopReason::Stop,
                false,
            )
        };
        let mut mismatch = thinking("drop-mismatch");
        if let Message::Assistant(message) = &mut mismatch {
            message.origin.provider_instance_id = "provider-instance-b".to_owned();
        }
        let mut missing = thinking("drop-missing");
        if let Message::Assistant(message) = &mut missing {
            message.origin.protocol = ApiProtocol::OpenAiResponses;
        }
        let output = transform(
            &[
                persisted("exact", 1, thinking("keep")),
                persisted("mismatch", 2, mismatch),
                persisted("missing", 3, missing),
                ContextMessage::Synthetic {
                    message: thinking("drop-synthetic"),
                },
            ],
            &target(),
        );

        assert_eq!(output.len(), 1);
        let encoded = serde_json::to_string(&output).expect("serialize replay view");
        assert!(encoded.contains("keep"));
        assert!(!encoded.contains("drop-mismatch"));
        assert!(!encoded.contains("drop-missing"));
        assert!(!encoded.contains("drop-synthetic"));
        assert!(encoded.contains("reasoning_content"));
    }

    #[test]
    fn rejected_pair_becomes_one_user_diagnostic_without_arguments() {
        let output = transform(
            &[
                persisted(
                    "a",
                    1,
                    assistant(
                        vec![rejected("bad", "write_file")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("r", 2, result("bad", true)),
            ],
            &target(),
        );
        assert_eq!(output.len(), 1);
        assert!(matches!(
            &output[0],
            ContextMessage::Synthetic {
                message: Message::User(_)
            }
        ));
        let encoded = serde_json::to_string(&output).expect("serialize transformed history");
        assert!(!encoded.contains("\"value\":1"));
        assert!(!encoded.contains("tool_result"));
    }

    #[test]
    fn rejected_call_without_result_still_becomes_diagnostic() {
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(vec![rejected("bad", "bash")], StopReason::ToolUse, false),
            )],
            &target(),
        );
        assert_eq!(output.len(), 1);
        assert!(matches!(message(&output[0]), Message::User(_)));
    }

    #[test]
    fn rejected_id_suppresses_all_error_results_but_keeps_non_error_and_unrelated() {
        let output = transform(
            &[
                persisted(
                    "a",
                    1,
                    assistant(
                        vec![rejected("bad", "write_file")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("bad-error-1", 2, result("bad", true)),
                persisted("bad-error-2", 3, result("bad", true)),
                persisted("bad-ok", 4, result("bad", false)),
                persisted("other-error", 5, result("other", true)),
            ],
            &target(),
        );
        let results = output
            .iter()
            .filter_map(|context| match message(context) {
                Message::ToolResult(result) => Some(result),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(results.len(), 2);
        assert!(
            results
                .iter()
                .any(|result| result.tool_call_id == "bad" && !result.is_error)
        );
        assert!(
            results
                .iter()
                .any(|result| result.tool_call_id == "other" && result.is_error)
        );
        assert!(output.iter().any(|context| matches!(
            context,
            ContextMessage::Synthetic {
                message: Message::User(_)
            }
        )));
    }

    #[test]
    fn rejected_id_suppression_does_not_leak_into_a_later_turn() {
        let output = transform(
            &[
                persisted(
                    "a1",
                    1,
                    assistant(vec![rejected("reused", "bash")], StopReason::ToolUse, false),
                ),
                persisted("r1", 2, result("reused", true)),
                persisted("u", 3, user("next")),
                persisted(
                    "a2",
                    4,
                    assistant(
                        vec![tool_call("reused", "tool")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("r2", 5, result("reused", true)),
            ],
            &target(),
        );
        assert_eq!(
            output
                .iter()
                .filter(|context| matches!(message(context), Message::ToolResult(_)))
                .count(),
            1
        );
        assert!(output.iter().any(|context| {
            matches!(
                context,
                ContextMessage::Persisted { id, .. } if id == "r2"
            )
        }));
    }

    #[test]
    fn rejected_content_does_not_remove_valid_text() {
        let output = transform(
            &[
                persisted(
                    "a",
                    1,
                    assistant(
                        vec![
                            AssistantContent::Text {
                                text: "before".to_owned(),
                                wire_item_index: 0,
                            },
                            rejected("bad", "tool"),
                        ],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("r", 2, result("bad", true)),
            ],
            &target(),
        );
        assert_eq!(output.len(), 2);
        assert!(matches!(message(&output[0]), Message::Assistant(_)));
        assert!(matches!(message(&output[1]), Message::User(_)));
    }

    #[test]
    fn normalizes_long_pipe_separated_ids_and_matching_result() {
        let original = format!("call+bad|{}", "x".repeat(100));
        let output = transform(
            &[
                persisted(
                    "a",
                    1,
                    assistant(
                        vec![tool_call(&original, "tool")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("r", 2, result(&original, false)),
            ],
            &cross_origin_target(),
        );
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("assistant");
        };
        let AssistantContent::ToolCall { tool_call, .. } = &assistant.content[0] else {
            panic!("tool call");
        };
        let Message::ToolResult(result) = message(&output[1]) else {
            panic!("tool result");
        };
        assert!(tool_call.id.starts_with("call_bad-"));
        assert_eq!(result.tool_call_id, tool_call.id);
        assert!(tool_call.id.len() <= 40);
    }

    #[test]
    fn normalizes_bounded_ids_for_chat_and_responses_destinations() {
        let original = format!("call+bad|{}", "x".repeat(100));
        for protocol in [
            ApiProtocol::OpenAiChatCompletions,
            ApiProtocol::OpenAiResponses,
        ] {
            let mut destination = cross_origin_target();
            destination.protocol = protocol;
            let output = transform(
                &[
                    persisted(
                        "a",
                        1,
                        assistant(
                            vec![tool_call(&original, "tool")],
                            StopReason::ToolUse,
                            false,
                        ),
                    ),
                    persisted("r", 2, result(&original, false)),
                ],
                &destination,
            );
            let Message::Assistant(assistant) = message(&output[0]) else {
                panic!("assistant");
            };
            let AssistantContent::ToolCall { tool_call, .. } = &assistant.content[0] else {
                panic!("tool call");
            };
            let Message::ToolResult(result) = message(&output[1]) else {
                panic!("tool result");
            };
            assert!(tool_call.id.len() <= 40);
            assert_eq!(result.tool_call_id, tool_call.id);
        }
    }

    #[test]
    fn normalizes_long_multibyte_id_at_utf8_boundary_and_pairs_result() {
        let original = format!("呼出し{}|opaque", "界".repeat(40));
        let output = transform(
            &[
                persisted(
                    "a",
                    1,
                    assistant(
                        vec![tool_call(&original, "tool")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("r", 2, result(&original, false)),
            ],
            &cross_origin_target(),
        );
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("assistant");
        };
        let AssistantContent::ToolCall { tool_call, .. } = &assistant.content[0] else {
            panic!("tool call");
        };
        let Message::ToolResult(result) = message(&output[1]) else {
            panic!("tool result");
        };

        assert!(tool_call.id.len() <= 40);
        assert_eq!(tool_call.id.len(), 38);
        assert_eq!(&tool_call.id[..27], "呼出し界界界界界界");
        assert_eq!(result.tool_call_id, tool_call.id);
    }

    #[test]
    fn same_origin_replay_preserves_noisy_tool_id_bytes() {
        let original = format!("call+bad|{}", "界".repeat(40));
        let output = transform(
            &[
                persisted(
                    "a",
                    1,
                    assistant(
                        vec![tool_call(&original, "tool")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("r", 2, result(&original, false)),
            ],
            &target(),
        );
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("assistant");
        };
        let AssistantContent::ToolCall { tool_call, .. } = &assistant.content[0] else {
            panic!("tool call");
        };
        let Message::ToolResult(result) = message(&output[1]) else {
            panic!("tool result");
        };
        assert_eq!(tool_call.id.as_bytes(), original.as_bytes());
        assert_eq!(result.tool_call_id.as_bytes(), original.as_bytes());
    }

    #[test]
    fn cross_origin_unbounded_destination_preserves_tool_id_bytes() {
        let original = format!("call+bad|{}", "x".repeat(100));
        let mut target = target();
        target.protocol = ApiProtocol::AnthropicMessages;
        let output = transform(
            &[
                persisted(
                    "a",
                    1,
                    assistant(
                        vec![tool_call(&original, "tool")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("r", 2, result(&original, false)),
            ],
            &target,
        );
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("assistant");
        };
        let AssistantContent::ToolCall { tool_call, .. } = &assistant.content[0] else {
            panic!("tool call");
        };
        let Message::ToolResult(result) = message(&output[1]) else {
            panic!("tool result");
        };
        assert_eq!(tool_call.id, original);
        assert_eq!(result.tool_call_id, original);
    }

    #[test]
    fn long_ids_with_the_same_readable_prefix_do_not_collide() {
        let first = format!("same-prefix|{}", "a".repeat(100));
        let second = format!("same-prefix|{}", "b".repeat(100));
        let input = vec![
            persisted(
                "a",
                1,
                assistant(
                    vec![tool_call(&first, "tool"), tool_call(&second, "tool")],
                    StopReason::ToolUse,
                    false,
                ),
            ),
            persisted("r1", 2, result(&first, false)),
            persisted("r2", 3, result(&second, false)),
        ];
        let output = transform(&input, &cross_origin_target());
        assert_eq!(
            output,
            transform(&input, &cross_origin_target()),
            "collision resolution must be stable across replay"
        );
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("assistant");
        };
        let ids = assistant
            .content
            .iter()
            .filter_map(|content| match content {
                AssistantContent::ToolCall { tool_call, .. } => Some(&tool_call.id),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);
        assert!(ids.iter().all(|id| id.len() <= 40));
    }

    #[test]
    fn valid_id_cannot_collide_with_an_earlier_normalized_id() {
        let long = format!("same-prefix|{}", "a".repeat(100));
        let colliding_valid = normalized_tool_id(&long, 0);
        let output = transform(
            &[
                persisted(
                    "a",
                    1,
                    assistant(
                        vec![
                            tool_call(&long, "first"),
                            tool_call(&colliding_valid, "second"),
                        ],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("r1", 2, result(&long, false)),
                persisted("r2", 3, result(&colliding_valid, false)),
            ],
            &cross_origin_target(),
        );
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("assistant");
        };
        let ids = assistant
            .content
            .iter()
            .filter_map(|content| match content {
                AssistantContent::ToolCall { tool_call, .. } => Some(tool_call.id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);
        assert_eq!(ids[0], colliding_valid);
        let results = output
            .iter()
            .filter_map(|context| match message(context) {
                Message::ToolResult(result) => Some(result.tool_call_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(results, ids);
    }

    #[test]
    fn duplicate_tool_call_id_is_not_replayed_as_two_executable_calls() {
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(
                    vec![tool_call("duplicate", "one"), tool_call("duplicate", "two")],
                    StopReason::ToolUse,
                    false,
                ),
            )],
            &cross_origin_target(),
        );
        let executable_calls = output
            .iter()
            .filter_map(|context| match message(context) {
                Message::Assistant(assistant) => Some(
                    assistant
                        .content
                        .iter()
                        .filter(|content| matches!(content, AssistantContent::ToolCall { .. }))
                        .count(),
                ),
                _ => None,
            })
            .sum::<usize>();
        assert_eq!(executable_calls, 1);
        assert!(output.iter().any(|context| matches!(
            context,
            ContextMessage::Synthetic {
                message: Message::User(_)
            }
        )));
    }

    #[test]
    fn duplicate_tool_call_with_two_results_keeps_one_result_and_diagnostic() {
        let output = transform(
            &[
                persisted(
                    "a",
                    1,
                    assistant(
                        vec![tool_call("duplicate", "one"), tool_call("duplicate", "two")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("r1", 2, result("duplicate", false)),
                persisted("r2", 3, result("duplicate", false)),
            ],
            &target(),
        );
        assert_eq!(
            output
                .iter()
                .filter(|context| matches!(message(context), Message::ToolResult(_)))
                .count(),
            1
        );
        assert!(output.iter().any(|context| matches!(
            context,
            ContextMessage::Synthetic {
                message: Message::User(_)
            }
        )));
    }

    #[test]
    fn duplicate_tool_call_id_may_be_reused_in_a_later_assistant_flow() {
        let output = transform(
            &[
                persisted(
                    "a1",
                    1,
                    assistant(
                        vec![tool_call("duplicate", "one")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("r1", 2, result("duplicate", false)),
                persisted(
                    "a2",
                    3,
                    assistant(
                        vec![tool_call("duplicate", "two")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("r2", 4, result("duplicate", false)),
            ],
            &target(),
        );
        let executable_calls = output
            .iter()
            .filter_map(|context| match message(context) {
                Message::Assistant(assistant) => Some(
                    assistant
                        .content
                        .iter()
                        .filter(|content| matches!(content, AssistantContent::ToolCall { .. }))
                        .count(),
                ),
                _ => None,
            })
            .sum::<usize>();
        assert_eq!(executable_calls, 2);
        assert!(!output.iter().any(|context| matches!(
            context,
            ContextMessage::Synthetic {
                message: Message::User(_)
            }
        )));
    }

    #[test]
    fn id_mapping_is_flow_local_and_later_valid_id_is_preserved() {
        let first = format!("first|{}", "a".repeat(80));
        let second = normalized_tool_id(&first, 0);
        let output = transform(
            &[
                persisted(
                    "a1",
                    1,
                    assistant(vec![tool_call(&first, "one")], StopReason::ToolUse, false),
                ),
                persisted("r1", 2, result(&first, false)),
                persisted(
                    "a2",
                    3,
                    assistant(vec![tool_call(&second, "two")], StopReason::ToolUse, false),
                ),
                persisted("r2", 4, result(&second, false)),
            ],
            &cross_origin_target(),
        );
        let ids = output
            .iter()
            .filter_map(|context| match message(context) {
                Message::Assistant(assistant) => assistant.content.iter().find_map(|content| {
                    if let AssistantContent::ToolCall { tool_call, .. } = content {
                        Some(tool_call.id.clone())
                    } else {
                        None
                    }
                }),
                _ => None,
            })
            .collect::<Vec<_>>();
        let result_ids = output
            .iter()
            .filter_map(|context| match message(context) {
                Message::ToolResult(result) if !result.is_error => {
                    Some(result.tool_call_id.clone())
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), 2);
        assert_eq!(result_ids, ids);
        assert_eq!(ids[0], second);
        assert_eq!(ids[1], second);
        assert!(ids.iter().all(|id| id.len() <= 40));
    }

    #[test]
    fn tool_call_id_may_be_reused_in_a_later_turn() {
        let output = transform(
            &[
                persisted(
                    "a1",
                    1,
                    assistant(
                        vec![tool_call("reused", "first")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("r1", 2, result("reused", false)),
                persisted("u", 3, user("next turn")),
                persisted(
                    "a2",
                    4,
                    assistant(
                        vec![tool_call("reused", "second")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("r2", 5, result("reused", false)),
            ],
            &target(),
        );
        assert_eq!(
            output
                .iter()
                .filter_map(|context| match message(context) {
                    Message::Assistant(assistant) => Some(
                        assistant
                            .content
                            .iter()
                            .filter(|content| {
                                matches!(content, AssistantContent::ToolCall { .. })
                            })
                            .count(),
                    ),
                    _ => None,
                })
                .sum::<usize>(),
            2
        );
        assert_eq!(
            output
                .iter()
                .filter(|context| matches!(message(context), Message::ToolResult(_)))
                .count(),
            2
        );
        assert!(!output.iter().any(|context| matches!(
            context,
            ContextMessage::Synthetic {
                message: Message::User(_)
            }
        )));
    }

    #[test]
    fn append_only_history_keeps_the_previous_send_view_as_a_prefix() {
        let base = vec![persisted(
            "a",
            1,
            assistant(
                vec![tool_call("stable-id", "tool")],
                StopReason::ToolUse,
                false,
            ),
        )];
        let previous = transform(&base, &target());
        assert!(matches!(message(&previous[1]), Message::ToolResult(_)));

        let mut appended = base;
        appended.push(persisted("u", 2, user("continue")));
        let next = transform(&appended, &target());
        assert_eq!(&next[..previous.len()], previous.as_slice());
        assert!(matches!(message(&next[2]), Message::User(_)));
    }

    #[test]
    fn transform_is_idempotent_for_a_normalized_send_view() {
        let first = transform(
            &[persisted(
                "a",
                1,
                assistant(
                    vec![tool_call("call+bad|provider-suffix", "tool")],
                    StopReason::ToolUse,
                    false,
                ),
            )],
            &target(),
        );
        assert_eq!(transform(&first, &target()), first);
    }

    #[test]
    fn natural_marker_text_does_not_prevent_a_new_interruption_marker() {
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(
                    vec![
                        AssistantContent::Text {
                            text: INTERRUPTION_MARKER.to_owned(),
                            wire_item_index: 0,
                        },
                        AssistantContent::Text {
                            text: "partial".to_owned(),
                            wire_item_index: 1,
                        },
                    ],
                    StopReason::Aborted,
                    true,
                ),
            )],
            &target(),
        );
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("expected assistant");
        };
        assert_eq!(assistant.content.len(), 3);
        assert!(matches!(
            &assistant.content[0],
            AssistantContent::Text { text, .. } if text == INTERRUPTION_MARKER
        ));
        assert!(matches!(
            &assistant.content[1],
            AssistantContent::Text { text, .. } if text == "partial"
        ));
        assert!(matches!(
            &assistant.content[2],
            AssistantContent::Text { text, .. } if text == INTERRUPTION_MARKER
        ));
    }
}
