//! Protocol-neutral replay normalization over the L0 send view.

use std::collections::{HashMap, HashSet};

use sha2::{Digest, Sha256};

use crate::provider::types::{
    ApiProtocol, AssistantContent, AssistantMessage, ContextMessage, Message, ProviderContextItem,
    ProviderOrigin, RejectedToolCall, StopReason, ToolCall, ToolResultMessage, UserContent,
    UserMessage,
};

/// Marker appended to an interrupted assistant message so the model can tell the
/// previous response was cut off by the user. This text is injected at replay
/// time and is never persisted.
pub const INTERRUPTION_MARKER: &str = "[この応答はユーザーの割り込みにより中断された]";
pub(crate) const MISSING_TOOL_RESULT_TEXT: &str = "No result provided";
const ORPHAN_TOOL_RESULT_NOTICE: &str = "対応するツール呼び出しがないツール結果は再送から除外されました。必要ならツール呼び出しを再生成してください。";
const REJECTED_TOOL_NOTICE_PREFIX: &str = "ツール `";
const REJECTED_TOOL_NOTICE_SUFFIX: &str = "の引数検証に失敗したため実行されませんでした。ツール呼び出しを正しい引数で再生成してください。";

#[derive(Clone, Copy)]
enum ToolIdConstraint {
    OpenAiCompatible,
    Anthropic,
}

impl ToolIdConstraint {
    fn max_bytes(self) -> usize {
        match self {
            Self::OpenAiCompatible => 40,
            Self::Anthropic => 64,
        }
    }

    fn accepts(self, id: &str) -> bool {
        if id.is_empty() || id.len() > self.max_bytes() {
            return false;
        }
        match self {
            Self::OpenAiCompatible => id
                .chars()
                .all(|character| character.is_alphanumeric() || matches!(character, '_' | '-')),
            Self::Anthropic => id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')),
        }
    }

    fn accepts_readable_char(self, character: char) -> bool {
        match self {
            Self::OpenAiCompatible => character.is_alphanumeric() || matches!(character, '_' | '-'),
            Self::Anthropic => character.is_ascii_alphanumeric() || matches!(character, '_' | '-'),
        }
    }
}

#[derive(Clone)]
struct PendingTool {
    raw_id: String,
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
    let mut result = Vec::with_capacity(messages.len());
    let mut pending_tools = Vec::<PendingTool>::new();
    let mut seen_tool_results = HashSet::<String>::new();
    let mut pending_rejections = Vec::<PendingRejection>::new();
    let mut consumed_call_ids = HashSet::<String>::new();
    let mut seen_call_ids = HashSet::<String>::new();
    let mut accepted_call_ids = HashMap::<String, String>::new();
    let mut pending_orphan_result = None;

    for context in messages.iter().cloned() {
        match context_message(&context) {
            Message::Assistant(message) => {
                flush_pending_tools(&mut result, &mut pending_tools, &seen_tool_results);
                seen_tool_results.clear();
                flush_rejections(&mut result, &mut pending_rejections);
                flush_orphan_result(&mut result, &mut pending_orphan_result);
                consumed_call_ids.clear();
                seen_call_ids.clear();
                accepted_call_ids.clear();

                if should_skip_assistant(message) {
                    for content in &message.content {
                        match content {
                            AssistantContent::ToolCall { tool_call, .. } => {
                                consumed_call_ids.insert(tool_call.id.clone());
                            }
                            AssistantContent::RejectedToolCall { rejected, .. } => {
                                consumed_call_ids.insert(rejected.id.clone());
                            }
                            AssistantContent::Text { .. } | AssistantContent::Thinking { .. } => {}
                        }
                    }
                    continue;
                }

                let mut assistant = message.clone();
                let mut id_map = ToolIdMap::default();
                let keep_thinking = may_replay_thinking(&context, &assistant, destination);
                let exact_origin =
                    same_origin(&context, destination) && assistant.model == destination.model;
                let tool_id_constraint = match destination.protocol {
                    ApiProtocol::OpenAiChatCompletions if !exact_origin => {
                        Some(ToolIdConstraint::OpenAiCompatible)
                    }
                    ApiProtocol::AnthropicMessages => Some(ToolIdConstraint::Anthropic),
                    _ => None,
                };
                let mut retained = Vec::with_capacity(assistant.content.len());
                let mut has_sendable_content = false;
                for content in assistant.content {
                    match content {
                        AssistantContent::ToolCall {
                            mut tool_call,
                            wire_item_index,
                        } => {
                            let raw_id = tool_call.id.clone();
                            if seen_call_ids.insert(raw_id.clone()) {
                                if let Some(constraint) = tool_id_constraint {
                                    tool_call.id = mapped_tool_id(&raw_id, &mut id_map, constraint);
                                }
                                accepted_call_ids.insert(raw_id.clone(), tool_call.id.clone());
                                has_sendable_content = true;
                                pending_tools.push(PendingTool {
                                    call: tool_call.clone(),
                                    raw_id,
                                    timestamp: assistant.timestamp,
                                });
                                retained.push(AssistantContent::ToolCall {
                                    tool_call,
                                    wire_item_index,
                                });
                            } else {
                                pending_rejections.push(PendingRejection {
                                    rejected: RejectedToolCall {
                                        id: raw_id,
                                        name: tool_call.name.clone(),
                                        error:
                                            crate::provider::types::ToolArgumentError::InvalidJson,
                                    },
                                    timestamp: assistant.timestamp,
                                });
                            }
                        }
                        AssistantContent::RejectedToolCall { rejected, .. } => {
                            if !accepted_call_ids.contains_key(&rejected.id) {
                                consumed_call_ids.insert(rejected.id.clone());
                            }
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
                            if keep_thinking {
                                has_sendable_content |= !thinking.is_empty();
                                retained.push(content);
                            }
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
                let raw_id = tool_result.tool_call_id.clone();
                if let Some(wire_id) = accepted_call_ids.get(&raw_id) {
                    if seen_tool_results.insert(raw_id) {
                        let mut tool_result = tool_result.clone();
                        tool_result.tool_call_id = wire_id.clone();
                        result.push(with_message(context, Message::ToolResult(tool_result)));
                    }
                    continue;
                }
                if consumed_call_ids.contains(&raw_id) {
                    continue;
                }
                pending_orphan_result.get_or_insert(tool_result.timestamp);
            }
            Message::User(_) => {
                flush_pending_tools(&mut result, &mut pending_tools, &seen_tool_results);
                seen_tool_results.clear();
                flush_rejections(&mut result, &mut pending_rejections);
                flush_orphan_result(&mut result, &mut pending_orphan_result);
                consumed_call_ids.clear();
                seen_call_ids.clear();
                accepted_call_ids.clear();
                result.push(context);
            }
        }
    }

    flush_pending_tools(&mut result, &mut pending_tools, &seen_tool_results);
    flush_rejections(&mut result, &mut pending_rejections);
    flush_orphan_result(&mut result, &mut pending_orphan_result);
    result
}

/// Restrict durable provider context to the final provider send view.
///
/// Hydration authenticates and retains provider-context rows independently of
/// L0 replay. Context is sendable only while its exact durable retention owner
/// `(message_id, message_seq)` survives transcript normalization. Native
/// compaction remains semantically unanchored, but its authenticated owner
/// still prevents context retained by an Error MessageEnd from leaking into a
/// later provider request.
pub fn provider_context_for_send_view<T>(
    messages: &[ContextMessage],
    provider_context: &[T],
) -> Vec<T>
where
    T: AsRef<ProviderContextItem> + Clone,
{
    let anchors = messages
        .iter()
        .filter_map(|message| match message {
            ContextMessage::Persisted { id, seq, .. } => Some((id.as_str(), *seq)),
            ContextMessage::Synthetic { .. } => None,
        })
        .collect::<HashSet<_>>();

    provider_context
        .iter()
        .filter(|item| {
            let item = item.as_ref();
            anchors.contains(&(
                item.retention_owner.message_id.as_str(),
                item.retention_owner.message_seq,
            ))
        })
        .cloned()
        .collect()
}

pub(crate) fn is_generated_replay_artifact(context: &ContextMessage) -> bool {
    let ContextMessage::Synthetic { message } = context else {
        return false;
    };
    match message {
        Message::ToolResult(result) => {
            result
                .details
                .get("code")
                .and_then(serde_json::Value::as_str)
                == Some("missing_tool_result")
                && matches!(
                    result.content.as_slice(),
                    [UserContent::Text { text }] if text == MISSING_TOOL_RESULT_TEXT
                )
        }
        Message::User(user) => matches!(
            user.content.as_slice(),
            [UserContent::Text { text }]
                if text == ORPHAN_TOOL_RESULT_NOTICE
                    || (text.starts_with(REJECTED_TOOL_NOTICE_PREFIX)
                        && text.ends_with(REJECTED_TOOL_NOTICE_SUFFIX))
        ),
        Message::Assistant(_) => false,
    }
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

fn mapped_tool_id(id: &str, map: &mut ToolIdMap, constraint: ToolIdConstraint) -> String {
    if let Some(normalized) = map.original_to_normalized.get(id) {
        return normalized.clone();
    }
    let preferred = if constraint.accepts(id) {
        id.to_owned()
    } else {
        normalized_tool_id(id, 0, constraint)
    };
    let mut normalized = preferred;
    let mut attempt = 1u32;
    while map
        .normalized_to_original
        .get(&normalized)
        .is_some_and(|original| original != id)
    {
        normalized = normalized_tool_id(id, attempt, constraint);
        attempt = attempt.saturating_add(1);
    }
    map.original_to_normalized
        .insert(id.to_owned(), normalized.clone());
    map.normalized_to_original
        .insert(normalized.clone(), id.to_owned());
    normalized
}

fn normalized_tool_id(id: &str, attempt: u32, constraint: ToolIdConstraint) -> String {
    const DIGEST_BYTES: usize = 5;
    const SUFFIX_BYTES: usize = 1 + DIGEST_BYTES * 2;

    let prefix = id.split('|').next().unwrap_or(id);
    let readable_candidate = prefix
        .chars()
        .map(|character| {
            if constraint.accepts_readable_char(character) {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    let readable = utf8_prefix(&readable_candidate, constraint.max_bytes() - SUFFIX_BYTES);
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
        if seen_results.contains(&pending.raw_id) {
            continue;
        }
        result.push(ContextMessage::Synthetic {
            message: Message::ToolResult(ToolResultMessage {
                tool_call_id: pending.call.id,
                tool_name: pending.call.name,
                content: vec![UserContent::Text {
                    text: MISSING_TOOL_RESULT_TEXT.to_owned(),
                }],
                details: serde_json::json!({"code": "missing_tool_result"}),
                is_error: true,
                timestamp: pending.timestamp,
            }),
        });
    }
}

fn flush_orphan_result(
    result: &mut Vec<ContextMessage>,
    pending_timestamp: &mut Option<chrono::DateTime<chrono::Utc>>,
) {
    let Some(timestamp) = pending_timestamp.take() else {
        return;
    };
    result.push(ContextMessage::Synthetic {
        message: Message::User(UserMessage {
            content: vec![UserContent::Text {
                text: ORPHAN_TOOL_RESULT_NOTICE.to_owned(),
            }],
            timestamp,
        }),
    });
}

fn flush_rejections(result: &mut Vec<ContextMessage>, pending: &mut Vec<PendingRejection>) {
    for pending in pending.drain(..) {
        result.push(ContextMessage::Synthetic {
            message: Message::User(UserMessage {
                content: vec![UserContent::Text {
                    text: format!(
                        "{REJECTED_TOOL_NOTICE_PREFIX}{}` {REJECTED_TOOL_NOTICE_SUFFIX}",
                        pending.rejected.name,
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
    use crate::provider::{
        ModelSpec, RequestOptions,
        adapters::{anthropic, chat_completions, responses},
        types::{
            NativeCompactionCoverage, PromptContext, ProviderContextAnchor, ProviderContextPayload,
            ToolArgumentError, Usage, UserMessage, ValidatedToolArguments,
        },
    };

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
                route: crate::provider::types::ToolInvocationRoute::Normal,
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

    fn prompt_context(messages: Vec<ContextMessage>) -> PromptContext {
        PromptContext {
            system_prompt: "system".to_owned(),
            memory_blocks: Vec::new(),
            messages,
            provider_context: Vec::new(),
            tools: Vec::new(),
            replay_provenance: None,
        }
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
    fn provider_send_view_keeps_exact_surviving_anchors_and_native_context() {
        let send_messages = vec![persisted(
            "kept",
            7,
            assistant(Vec::new(), StopReason::Stop, false),
        )];
        let anchored = |message_id: &str, message_seq: u64| ProviderContextItem {
            retention_owner: ProviderContextAnchor {
                message_id: message_id.to_owned(),
                message_seq,
            },
            origin_message: Some(ProviderContextAnchor {
                message_id: message_id.to_owned(),
                message_seq,
            }),
            wire_item_index: Some(0),
            ordinal: 0,
            provider_origin: target(),
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::OpenAiResponses,
                item: json!({"type": "reasoning", "encrypted_content": message_id}),
            },
        };
        let native = ProviderContextItem {
            retention_owner: ProviderContextAnchor {
                message_id: "kept".to_owned(),
                message_seq: 7,
            },
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: target(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({"type": "compaction", "id": "cmp-1"})],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 1,
                    context_fingerprint: "fixture".to_owned(),
                },
            },
        };
        let removed_native = ProviderContextItem {
            retention_owner: ProviderContextAnchor {
                message_id: "error-owner".to_owned(),
                message_seq: 6,
            },
            ..native.clone()
        };

        let result = provider_context_for_send_view(
            &send_messages,
            &[
                anchored("kept", 7),
                anchored("kept", 8),
                anchored("removed", 7),
                native.clone(),
                removed_native,
            ],
        );

        assert_eq!(result, vec![anchored("kept", 7), native]);
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
    fn skipped_terminal_assistant_consumes_its_later_result_without_synthesis() {
        for reason in [StopReason::Error, StopReason::Aborted] {
            let output = transform(
                &[
                    persisted(
                        "a",
                        1,
                        assistant(vec![tool_call("skipped", "tool")], reason, false),
                    ),
                    persisted("r", 2, result("skipped", false)),
                ],
                &target(),
            );
            assert!(output.is_empty());
        }
    }

    #[test]
    fn orphan_only_and_unknown_after_a_completed_flow_become_safe_user_diagnostics() {
        let orphan_only = transform(
            &[persisted("orphan", 1, result("unknown", false))],
            &target(),
        );
        assert_eq!(orphan_only.len(), 1);
        assert!(matches!(
            &orphan_only[0],
            ContextMessage::Synthetic {
                message: Message::User(_)
            }
        ));

        let after_flow = transform(
            &[
                persisted(
                    "a",
                    1,
                    assistant(vec![tool_call("known", "tool")], StopReason::ToolUse, false),
                ),
                persisted("known-result", 2, result("known", false)),
                persisted("orphan", 3, result("unknown", false)),
            ],
            &target(),
        );
        assert_eq!(after_flow.len(), 3);
        assert!(matches!(message(&after_flow[0]), Message::Assistant(_)));
        assert!(matches!(message(&after_flow[1]), Message::ToolResult(_)));
        assert!(matches!(message(&after_flow[2]), Message::User(_)));
    }

    #[test]
    fn raw_result_equal_to_another_calls_normalized_id_does_not_pair() {
        let raw_call_id = format!("same-prefix|{}", "a".repeat(100));
        let normalized = normalized_tool_id(&raw_call_id, 0, ToolIdConstraint::OpenAiCompatible);
        let output = transform(
            &[
                persisted(
                    "a",
                    1,
                    assistant(
                        vec![tool_call(&raw_call_id, "tool")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("collision", 2, result(&normalized, false)),
            ],
            &cross_origin_target(),
        );

        assert_eq!(output.len(), 3);
        assert!(matches!(
            &output[1],
            ContextMessage::Synthetic {
                message: Message::ToolResult(result)
            } if result.tool_call_id == normalized
                && result.is_error
                && result.details == json!({"code": "missing_tool_result"})
        ));
        assert!(matches!(message(&output[2]), Message::User(_)));
        assert!(!output.iter().any(|context| matches!(
            context,
            ContextMessage::Persisted { id, .. } if id == "collision"
        )));
    }

    #[test]
    fn repaired_orphan_only_history_is_valid_for_all_provider_builders() {
        for preset in ["glm-5.2", "openai-responses", "anthropic"] {
            let spec = ModelSpec::preset(preset).expect("built-in provider preset");
            let replay = transform(
                &[persisted("orphan", 1, result("unknown", false))],
                &spec.origin(),
            );
            let context = prompt_context(replay);
            let options = RequestOptions::default();
            match spec.protocol {
                ApiProtocol::OpenAiChatCompletions => {
                    let request = chat_completions::build_request(&spec, &context, &options)
                        .expect("Chat builder accepts repaired history");
                    assert!(
                        request["messages"]
                            .as_array()
                            .expect("messages")
                            .iter()
                            .all(|message| message["role"] != "tool")
                    );
                }
                ApiProtocol::OpenAiResponses => {
                    let request = responses::build_request(&spec, &context, &options)
                        .expect("Responses builder accepts repaired history");
                    assert!(
                        request["input"]
                            .as_array()
                            .expect("input")
                            .iter()
                            .all(|item| item["type"] != "function_call_output")
                    );
                }
                ApiProtocol::AnthropicMessages => {
                    let request = anthropic::build_request(&spec, &context, &options)
                        .expect("Anthropic builder accepts repaired history");
                    assert!(!request["messages"].as_array().expect("messages").is_empty());
                }
            }
        }
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
    fn rejected_only_id_consumes_error_and_non_error_results() {
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
    }

    #[test]
    fn retained_call_owns_first_cross_kind_result_even_when_error() {
        let output = transform(
            &[
                persisted(
                    "a",
                    1,
                    assistant(
                        vec![tool_call("shared", "run"), rejected("shared", "run")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("first-error", 2, result("shared", true)),
                persisted("extra-ok", 3, result("shared", false)),
                persisted("extra-error", 4, result("shared", true)),
            ],
            &target(),
        );

        assert_eq!(output.len(), 3);
        assert!(matches!(message(&output[0]), Message::Assistant(_)));
        assert!(matches!(
            &output[1],
            ContextMessage::Persisted {
                id,
                message: Message::ToolResult(result),
                ..
            } if id == "first-error" && result.is_error && result.tool_call_id == "shared"
        ));
        assert!(matches!(message(&output[2]), Message::User(_)));
    }

    #[test]
    fn retained_call_owns_first_cross_kind_non_error_result_when_rejection_comes_first() {
        let output = transform(
            &[
                persisted(
                    "a",
                    1,
                    assistant(
                        vec![rejected("shared", "run"), tool_call("shared", "run")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("first-ok", 2, result("shared", false)),
                persisted("extra-error", 3, result("shared", true)),
            ],
            &target(),
        );

        assert_eq!(output.len(), 3);
        assert!(matches!(message(&output[0]), Message::Assistant(_)));
        assert!(matches!(
            &output[1],
            ContextMessage::Persisted {
                id,
                message: Message::ToolResult(result),
                ..
            } if id == "first-ok" && !result.is_error && result.tool_call_id == "shared"
        ));
        assert!(matches!(message(&output[2]), Message::User(_)));
    }

    #[test]
    fn retained_cross_kind_call_without_result_gets_normal_missing_result() {
        let output = transform(
            &[persisted(
                "a",
                1,
                assistant(
                    vec![tool_call("shared", "run"), rejected("shared", "run")],
                    StopReason::ToolUse,
                    false,
                ),
            )],
            &target(),
        );

        assert_eq!(output.len(), 3);
        assert!(matches!(message(&output[0]), Message::Assistant(_)));
        assert!(matches!(
            &output[1],
            ContextMessage::Synthetic {
                message: Message::ToolResult(result)
            } if result.tool_call_id == "shared"
                && result.is_error
                && result.details == json!({"code": "missing_tool_result"})
        ));
        assert!(matches!(message(&output[2]), Message::User(_)));
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
    fn normalizes_bounded_ids_for_cross_origin_chat_destination() {
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
        assert!(tool_call.id.len() <= 40);
        assert_eq!(result.tool_call_id, tool_call.id);
    }

    #[test]
    fn preserves_cross_origin_responses_tool_ids_without_a_wire_constraint() {
        let original = format!("call+bad|{}界", "x".repeat(100));
        let mut destination = cross_origin_target();
        destination.protocol = ApiProtocol::OpenAiResponses;
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
        assert_eq!(tool_call.id.as_bytes(), original.as_bytes());
        assert_eq!(result.tool_call_id.as_bytes(), original.as_bytes());
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
    fn cross_origin_anthropic_normalizes_invalid_id_and_pairs_result() {
        let original = format!("call+bad|{}", "x".repeat(100));
        let mut target = target();
        target.protocol = ApiProtocol::AnthropicMessages;
        let input = vec![
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
        ];
        let output = transform(&input, &target);
        assert_eq!(output, transform(&input, &target));
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("assistant");
        };
        let AssistantContent::ToolCall { tool_call, .. } = &assistant.content[0] else {
            panic!("tool call");
        };
        let Message::ToolResult(result) = message(&output[1]) else {
            panic!("tool result");
        };
        assert_ne!(tool_call.id, original);
        assert!(ToolIdConstraint::Anthropic.accepts(&tool_call.id));
        assert_eq!(result.tool_call_id, tool_call.id);
    }

    #[test]
    fn anthropic_preserves_64_byte_boundary_and_normalizes_65_bytes() {
        let boundary = "a".repeat(64);
        let overlong = "b".repeat(65);
        let mut destination = target();
        destination.protocol = ApiProtocol::AnthropicMessages;
        let output = transform(
            &[
                persisted(
                    "a",
                    1,
                    assistant(
                        vec![
                            tool_call(&boundary, "boundary"),
                            tool_call(&overlong, "overlong"),
                        ],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("r1", 2, result(&boundary, false)),
                persisted("r2", 3, result(&overlong, false)),
            ],
            &destination,
        );
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("assistant");
        };
        let call_ids = assistant
            .content
            .iter()
            .filter_map(|content| match content {
                AssistantContent::ToolCall { tool_call, .. } => Some(tool_call.id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let result_ids = output[1..]
            .iter()
            .filter_map(|context| match message(context) {
                Message::ToolResult(result) => Some(result.tool_call_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(call_ids[0], boundary);
        assert_ne!(call_ids[1], overlong);
        assert!(
            call_ids
                .iter()
                .all(|id| ToolIdConstraint::Anthropic.accepts(id))
        );
        assert_eq!(result_ids, call_ids);
    }

    #[test]
    fn anthropic_empty_noisy_and_unicode_ids_are_stable_safe_and_paired() {
        let mut destination = target();
        destination.protocol = ApiProtocol::AnthropicMessages;
        for original in ["", "call+bad|opaque", "呼び出し識別子"] {
            let input = vec![
                persisted(
                    "a",
                    1,
                    assistant(
                        vec![tool_call(original, "tool")],
                        StopReason::ToolUse,
                        false,
                    ),
                ),
                persisted("r", 2, result(original, false)),
            ];
            let output = transform(&input, &destination);
            assert_eq!(output, transform(&input, &destination));
            let Message::Assistant(assistant) = message(&output[0]) else {
                panic!("assistant");
            };
            let AssistantContent::ToolCall { tool_call, .. } = &assistant.content[0] else {
                panic!("tool call");
            };
            let Message::ToolResult(result) = message(&output[1]) else {
                panic!("tool result");
            };
            assert!(ToolIdConstraint::Anthropic.accepts(&tool_call.id));
            assert_eq!(result.tool_call_id, tool_call.id);
        }
    }

    #[test]
    fn same_origin_anthropic_preserves_valid_id_but_repairs_builder_invalid_id() {
        let valid = "v".repeat(64);
        let invalid = "same+origin";
        let mut destination = target();
        destination.protocol = ApiProtocol::AnthropicMessages;
        let mut source = assistant(
            vec![tool_call(&valid, "valid"), tool_call(invalid, "invalid")],
            StopReason::ToolUse,
            false,
        );
        let Message::Assistant(assistant) = &mut source else {
            panic!("assistant");
        };
        assistant.origin = destination.clone();
        let output = transform(
            &[
                persisted("a", 1, source),
                persisted("r1", 2, result(&valid, false)),
                persisted("r2", 3, result(invalid, false)),
            ],
            &destination,
        );
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("assistant");
        };
        let call_ids = assistant
            .content
            .iter()
            .filter_map(|content| match content {
                AssistantContent::ToolCall { tool_call, .. } => Some(tool_call.id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(call_ids[0], valid);
        assert_ne!(call_ids[1], invalid);
        assert!(
            call_ids
                .iter()
                .all(|id| ToolIdConstraint::Anthropic.accepts(id))
        );
        assert_eq!(
            output[1..]
                .iter()
                .filter_map(|context| match message(context) {
                    Message::ToolResult(result) => Some(result.tool_call_id.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            call_ids
        );
    }

    #[test]
    fn anthropic_collision_resolution_is_deterministic_and_pairs_each_result() {
        let invalid = format!("same+prefix|{}", "x".repeat(100));
        let colliding_valid = normalized_tool_id(&invalid, 0, ToolIdConstraint::Anthropic);
        let mut destination = target();
        destination.protocol = ApiProtocol::AnthropicMessages;
        let input = vec![
            persisted(
                "a",
                1,
                assistant(
                    vec![
                        tool_call(&invalid, "first"),
                        tool_call(&colliding_valid, "second"),
                    ],
                    StopReason::ToolUse,
                    false,
                ),
            ),
            persisted("r1", 2, result(&invalid, false)),
            persisted("r2", 3, result(&colliding_valid, false)),
        ];
        let output = transform(&input, &destination);
        assert_eq!(output, transform(&input, &destination));
        let Message::Assistant(assistant) = message(&output[0]) else {
            panic!("assistant");
        };
        let call_ids = assistant
            .content
            .iter()
            .filter_map(|content| match content {
                AssistantContent::ToolCall { tool_call, .. } => Some(tool_call.id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(call_ids.len(), 2);
        assert_eq!(call_ids[0], colliding_valid);
        assert_ne!(call_ids[0], call_ids[1]);
        assert!(
            call_ids
                .iter()
                .all(|id| ToolIdConstraint::Anthropic.accepts(id))
        );
        assert_eq!(
            output[1..]
                .iter()
                .filter_map(|context| match message(context) {
                    Message::ToolResult(result) => Some(result.tool_call_id.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            call_ids
        );
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
        let colliding_valid = normalized_tool_id(&long, 0, ToolIdConstraint::OpenAiCompatible);
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
        let second = normalized_tool_id(&first, 0, ToolIdConstraint::OpenAiCompatible);
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
    fn anthropic_id_mapping_is_reset_for_a_later_assistant_flow() {
        let first = format!("first+{}", "a".repeat(80));
        let second = normalized_tool_id(&first, 0, ToolIdConstraint::Anthropic);
        let mut destination = target();
        destination.protocol = ApiProtocol::AnthropicMessages;
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
            &destination,
        );
        let call_ids = output
            .iter()
            .filter_map(|context| match message(context) {
                Message::Assistant(assistant) => assistant.content.iter().find_map(|content| {
                    if let AssistantContent::ToolCall { tool_call, .. } = content {
                        Some(tool_call.id.as_str())
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
                Message::ToolResult(result) => Some(result.tool_call_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(call_ids, vec![second.as_str(), second.as_str()]);
        assert_eq!(result_ids, call_ids);
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
