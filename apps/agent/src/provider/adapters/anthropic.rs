use std::collections::{BTreeMap, HashSet};

use chrono::Utc;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::provider::{
    assembler::{
        FrozenToolSchemaRegistry, ResponseBudget, ToolArgumentAccumulator, ToolArgumentOutcome,
    },
    model::{AnthropicCompat, ModelSpec, ProtocolCompat, RequestOptions},
    types::{
        ApiProtocol, AssistantContent, ContextMessage, MemoryLayer, Message,
        NativeCompactionCoverage, PromptContext, ProviderContextAnchor, ProviderContextFragment,
        ProviderContextItem, ProviderContextPayload, ProviderEvent, StopReason, ToolDefinition,
        Usage, UserContent,
    },
};

const ANTHROPIC_VERSION: &str = "2023-06-01";
const REDACTED_PLACEHOLDER: &str = "[Reasoning redacted]";

#[derive(Debug, Error)]
pub enum AnthropicAdapterError {
    #[error("model protocol/compat variant is not Anthropic Messages")]
    UnsupportedProtocol,
    #[error("max_tokens must be within 1..={max}, got {requested}")]
    InvalidMaxTokens { requested: u64, max: u64 },
    #[error("temperature must be finite, got {0}")]
    InvalidTemperature(f64),
    #[error("invalid Anthropic request context: {0}")]
    InvalidContext(String),
    #[error("invalid Anthropic stream event: {0}")]
    InvalidEvent(String),
    #[error("provider returned an error: {message}")]
    Provider {
        code: Option<String>,
        message: String,
    },
    #[error("provider response exceeded {resource} budget ({limit})")]
    ResponseLimitExceeded {
        resource: &'static str,
        limit: usize,
    },
    #[error("Anthropic stream ended before message_stop")]
    MissingTerminal,
    #[error("native compaction failed: {code}")]
    NativeCompactionFailed { code: String },
}

#[derive(Debug)]
pub struct AnthropicTerminal {
    pub events: Vec<ProviderEvent>,
    pub reason: StopReason,
    pub usage: Usage,
    pub error_message: Option<String>,
    pub provider_code: Option<String>,
    pub provider_context: Vec<ProviderContextFragment>,
}

#[derive(Debug, Default)]
pub struct AnthropicPush {
    pub events: Vec<ProviderEvent>,
    pub terminal: Option<AnthropicTerminal>,
}

pub fn requested_output_tokens(
    spec: &ModelSpec,
    options: &RequestOptions,
) -> Result<u64, AnthropicAdapterError> {
    ensure_anthropic_spec(spec)?;
    let requested = options.max_tokens.unwrap_or(spec.default_output_tokens);
    if requested == 0 || requested > spec.max_output_tokens {
        return Err(AnthropicAdapterError::InvalidMaxTokens {
            requested,
            max: spec.max_output_tokens,
        });
    }
    Ok(requested)
}

pub fn build_request(
    spec: &ModelSpec,
    context: &PromptContext,
    options: &RequestOptions,
) -> Result<Value, AnthropicAdapterError> {
    let compat = ensure_anthropic_spec(spec)?;
    if let Some(temperature) = options.temperature
        && !temperature.is_finite()
    {
        return Err(AnthropicAdapterError::InvalidTemperature(temperature));
    }
    let mut request = Map::new();
    request.insert("model".into(), json!(spec.id));
    request.insert(
        "system".into(),
        if compat.supports_prompt_cache {
            json!([{
                "type":"text",
                "text":context.system_prompt,
                "cache_control":{"type":"ephemeral"},
            }])
        } else {
            json!(context.system_prompt)
        },
    );
    request.insert("stream".into(), json!(true));
    let max_tokens = requested_output_tokens(spec, options)?;
    if spec.reasoning && max_tokens <= 1024 {
        return Err(AnthropicAdapterError::InvalidContext(
            "thinking-enabled max_tokens must leave room beyond the 1024-token thinking budget"
                .into(),
        ));
    }
    request.insert("max_tokens".into(), json!(max_tokens));
    let messages = convert_messages(spec, context, options.native_compaction)?;
    if messages.is_empty() {
        return Err(AnthropicAdapterError::InvalidContext(
            "Anthropic request requires at least one conversation turn".into(),
        ));
    }
    request.insert("messages".into(), Value::Array(messages));
    let has_request_coverage = match request_coverage(spec, context, options.native_compaction) {
        Ok(coverage) => coverage.is_some(),
        Err(error) => {
            tracing::warn!(reason = %error, "omitted unanchored Anthropic native compaction request");
            false
        }
    };
    if has_request_coverage {
        request.insert(
            "context_management".into(),
            json!({"edits":[{"type":"compact_20260112"}]}),
        );
    }

    if !context.tools.is_empty() {
        request.insert(
            "tools".into(),
            Value::Array(
                context
                    .tools
                    .iter()
                    .map(|tool| convert_tool(tool, compat))
                    .collect(),
            ),
        );
    }
    if spec.reasoning {
        request.insert(
            "thinking".into(),
            json!({"type":"enabled","budget_tokens":1024}),
        );
    } else if let Some(temperature) = options.temperature {
        request.insert("temperature".into(), json!(temperature));
    }
    if let Some(choice) = &options.tool_choice {
        request.insert(
            "tool_choice".into(),
            normalize_tool_choice(choice, spec.reasoning)?,
        );
    }
    Ok(Value::Object(request))
}

fn ensure_anthropic_spec(spec: &ModelSpec) -> Result<&AnthropicCompat, AnthropicAdapterError> {
    match (&spec.protocol, &spec.compat) {
        (ApiProtocol::AnthropicMessages, ProtocolCompat::Anthropic(compat)) => Ok(compat),
        _ => Err(AnthropicAdapterError::UnsupportedProtocol),
    }
}

fn normalize_tool_choice(choice: &Value, thinking: bool) -> Result<Value, AnthropicAdapterError> {
    let (kind, object) = match choice {
        Value::String(value) => (value.as_str(), None),
        Value::Object(object) => (
            object.get("type").and_then(Value::as_str).ok_or_else(|| {
                AnthropicAdapterError::InvalidContext("tool_choice.type must be a string".into())
            })?,
            Some(object),
        ),
        _ => {
            return Err(AnthropicAdapterError::InvalidContext(
                "tool_choice must be a string or object".into(),
            ));
        }
    };
    if !matches!(kind, "auto" | "none" | "any" | "tool") {
        return Err(AnthropicAdapterError::InvalidContext(format!(
            "unsupported tool_choice type {kind}"
        )));
    }
    if thinking && !matches!(kind, "auto" | "none") {
        return Err(AnthropicAdapterError::InvalidContext(
            "thinking-enabled Anthropic requests only allow tool_choice auto or none".into(),
        ));
    }
    if let Some(object) = object {
        let allowed = match kind {
            "auto" | "any" => &["type", "disable_parallel_tool_use"][..],
            "tool" => &["type", "name", "disable_parallel_tool_use"][..],
            "none" => &["type"][..],
            _ => unreachable!("kind validated above"),
        };
        if object.keys().any(|key| !allowed.contains(&key.as_str())) {
            return Err(AnthropicAdapterError::InvalidContext(
                "tool_choice contains fields outside its official shape".into(),
            ));
        }
        if let Some(value) = object.get("disable_parallel_tool_use")
            && !value.is_boolean()
        {
            return Err(AnthropicAdapterError::InvalidContext(
                "tool_choice.disable_parallel_tool_use must be a boolean".into(),
            ));
        }
        if kind == "tool"
            && object
                .get("name")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
        {
            return Err(AnthropicAdapterError::InvalidContext(
                "tool_choice type=tool requires a non-empty name".into(),
            ));
        }
    } else if kind == "tool" {
        return Err(AnthropicAdapterError::InvalidContext(
            "tool_choice type=tool requires an object with a non-empty name".into(),
        ));
    }
    Ok(match choice {
        Value::String(_) => json!({"type":kind}),
        _ => choice.clone(),
    })
}

fn convert_tool(tool: &ToolDefinition, compat: &AnthropicCompat) -> Value {
    let mut value = json!({
        "name": tool.name,
        "description": tool.description,
        "input_schema": tool.parameters,
    });
    if compat.supports_fine_grained_tool_streaming {
        value
            .as_object_mut()
            .expect("tool object")
            .insert("eager_input_streaming".into(), json!(true));
    }
    value
}

fn convert_messages(
    spec: &ModelSpec,
    context: &PromptContext,
    native_compaction: bool,
) -> Result<Vec<Value>, AnthropicAdapterError> {
    let compat = ensure_anthropic_spec(spec)?;
    let mut messages = Vec::<Value>::new();
    for memory in &context.memory_blocks {
        let layer = match memory.layer {
            MemoryLayer::L1 => "l1",
            MemoryLayer::L2 => "l2",
        };
        push_turn(
            &mut messages,
            "user",
            vec![json!({
                "type":"text",
                "text":format!("<memory layer=\"{layer}\">{}</memory>", escape_memory(&memory.text)),
            })],
        );
    }

    let has_foreign_native = context.provider_context.iter().any(|item| {
        matches!(
            item.payload,
            ProviderContextPayload::OpenAiCompactedWindow { .. }
        )
    });
    let mut native = if native_compaction && compat.supports_native_compact && !has_foreign_native {
        context
            .provider_context
            .iter()
            .filter_map(|item| match &item.payload {
                ProviderContextPayload::AnthropicCompaction { block, coverage } => {
                    Some((item, block, coverage))
                }
                _ => None,
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    if let Err(error) = validate_native_replay(spec, context, &native) {
        tracing::warn!(reason = %error, "discarded stale Anthropic native context");
        native.clear();
    }
    let (coverage_seq, mut native_block) = if let Some((_, block, coverage)) = native.first() {
        if !context.memory_blocks.is_empty() {
            return Err(AnthropicAdapterError::InvalidContext(
                "native compaction cannot coexist with memory blocks".into(),
            ));
        }
        validate_compaction_block(block)?;
        if coverage.context_fingerprint != context_fingerprint(spec, context)? {
            return Err(AnthropicAdapterError::InvalidContext(
                "native compaction context fingerprint mismatch".into(),
            ));
        }
        (Some(coverage.through_message_seq), Some((*block).clone()))
    } else {
        (None, None)
    };

    let mut opaque_by_anchor = BTreeMap::<(String, u64), Vec<&ProviderContextItem>>::new();
    for item in &context.provider_context {
        match &item.payload {
            ProviderContextPayload::EncryptedReasoning { .. } => {
                let anchor = item.origin_message.as_ref().ok_or_else(|| {
                    AnthropicAdapterError::InvalidContext(
                        "Anthropic reasoning is missing an origin anchor".into(),
                    )
                })?;
                if coverage_seq.is_some_and(|coverage| anchor.message_seq <= coverage) {
                    continue;
                }
                opaque_by_anchor
                    .entry((anchor.message_id.clone(), anchor.message_seq))
                    .or_default()
                    .push(item);
            }
            // A native window from another protocol is stale provider state;
            // omit it and rebuild from durable public context.
            ProviderContextPayload::OpenAiCompactedWindow { .. } => {}
            ProviderContextPayload::AnthropicCompaction { .. } => {}
        }
    }

    let mut pending_tool_ids = HashSet::<String>::new();
    let mut persisted_started = false;
    for context_message in &context.messages {
        let (anchor, message) = match context_message {
            ContextMessage::Persisted { id, seq, message } => {
                if !persisted_started {
                    persisted_started = true;
                    if let Some(block) = native_block.take() {
                        push_turn(&mut messages, "assistant", vec![block]);
                    }
                }
                if coverage_seq.is_some_and(|coverage| *seq <= coverage) {
                    continue;
                }
                (
                    Some(ProviderContextAnchor {
                        message_id: id.clone(),
                        message_seq: *seq,
                    }),
                    message,
                )
            }
            ContextMessage::Synthetic { message } => {
                if coverage_seq.is_some() && persisted_started {
                    return Err(AnthropicAdapterError::InvalidContext(
                        "native compaction suffix requires persisted message sequence numbers"
                            .into(),
                    ));
                }
                (None, message)
            }
        };
        match message {
            Message::User(user) => {
                if !pending_tool_ids.is_empty() {
                    return Err(AnthropicAdapterError::InvalidContext(
                        "user turn interrupted an unresolved tool_use/tool_result pair".into(),
                    ));
                }
                let blocks = user
                    .content
                    .iter()
                    .map(|content| anthropic_user_content(content, spec.supports_images))
                    .collect();
                push_turn(&mut messages, "user", blocks);
            }
            Message::ToolResult(result) => {
                if !pending_tool_ids.remove(&result.tool_call_id) {
                    return Err(AnthropicAdapterError::InvalidContext(
                        "tool_result has no matching unresolved tool_use".into(),
                    ));
                }
                push_turn(
                    &mut messages,
                    "user",
                    vec![json!({
                    "type":"tool_result",
                    "tool_use_id":result.tool_call_id,
                    "content":result.content.iter()
                        .map(|content| anthropic_user_content(content, spec.supports_images))
                        .collect::<Vec<_>>(),
                    "is_error":result.is_error,
                    })],
                );
            }
            Message::Assistant(assistant) => {
                if !pending_tool_ids.is_empty() {
                    return Err(AnthropicAdapterError::InvalidContext(
                        "assistant turn followed unresolved tool_use blocks".into(),
                    ));
                }
                let same_origin = assistant.origin == spec.origin();
                let mut opaque = BTreeMap::<(u32, u32), Value>::new();
                if let Some(anchor) = &anchor
                    && let Some(items) =
                        opaque_by_anchor.remove(&(anchor.message_id.clone(), anchor.message_seq))
                {
                    if same_origin && !spec.reasoning {
                        return Err(AnthropicAdapterError::InvalidContext(
                            "Anthropic thinking mode changed inside a tool loop".into(),
                        ));
                    }
                    // T25 invalidation boundary: switching endpoint/account invalidates
                    // opaque continuation state and raw Thinking, but not public text.
                    for item in items.into_iter().filter(|_| same_origin) {
                        let wire = item.wire_item_index.ok_or_else(|| {
                            AnthropicAdapterError::InvalidContext(
                                "Anthropic reasoning is missing wire_item_index".into(),
                            )
                        })?;
                        let ProviderContextPayload::EncryptedReasoning {
                            protocol,
                            item: payload,
                        } = &item.payload
                        else {
                            unreachable!("filtered above")
                        };
                        if *protocol != ApiProtocol::AnthropicMessages {
                            return Err(AnthropicAdapterError::InvalidContext(
                                "same-origin Anthropic reasoning protocol mismatch".into(),
                            ));
                        }
                        validate_reasoning_item(payload)?;
                        if opaque
                            .insert((wire, item.ordinal), payload.clone())
                            .is_some()
                        {
                            return Err(AnthropicAdapterError::InvalidContext(
                                "duplicate Anthropic reasoning placement".into(),
                            ));
                        }
                    }
                }
                let mut public = BTreeMap::<u32, &AssistantContent>::new();
                for content in &assistant.content {
                    let wire = wire_index(content);
                    if public.insert(wire, content).is_some() {
                        return Err(AnthropicAdapterError::InvalidContext(
                            "duplicate assistant wire_item_index".into(),
                        ));
                    }
                }
                let mut blocks = Vec::new();
                let mut opaque_iter = opaque.into_iter().peekable();
                for (wire, content) in public {
                    if opaque_iter
                        .peek()
                        .is_some_and(|((opaque_wire, _), _)| *opaque_wire < wire)
                    {
                        return Err(AnthropicAdapterError::InvalidContext(
                            "saved thinking placement has no transcript block".into(),
                        ));
                    }
                    match content {
                        AssistantContent::Text { text, .. } => {
                            blocks.push(json!({"type":"text","text":text}));
                        }
                        AssistantContent::Thinking { thinking, .. } if same_origin => {
                            let mut matching = Vec::new();
                            while opaque_iter
                                .peek()
                                .is_some_and(|((opaque_wire, _), _)| *opaque_wire == wire)
                            {
                                matching.push(
                                    opaque_iter
                                        .next()
                                        .expect("peeked opaque item")
                                        .1,
                                );
                            }
                            if matching.len() != 1 {
                                return Err(AnthropicAdapterError::InvalidContext(
                                    "thinking continuation requires exactly one saved signature or redacted block".into(),
                                ));
                            }
                            let saved = matching.pop().expect("one item");
                            match saved.get("type").and_then(Value::as_str) {
                                Some("thinking_signature") => blocks.push(json!({
                                    "type":"thinking",
                                    "thinking":thinking,
                                    "signature":saved.get("signature").and_then(Value::as_str)
                                        .expect("validated signature"),
                                })),
                                Some("redacted_thinking") if thinking == REDACTED_PLACEHOLDER => {
                                    blocks.push(saved);
                                }
                                _ => {
                                    return Err(AnthropicAdapterError::InvalidContext(
                                        "saved thinking kind does not match transcript block".into(),
                                    ));
                                }
                            }
                        }
                        AssistantContent::Thinking { .. } => {}
                        AssistantContent::ToolCall { tool_call, .. } => {
                            validate_tool_use_id(&tool_call.id)?;
                            if !pending_tool_ids.insert(tool_call.id.clone()) {
                                return Err(AnthropicAdapterError::InvalidContext(
                                    "duplicate unresolved tool_use id".into(),
                                ));
                            }
                            blocks.push(json!({
                                "type":"tool_use",
                                "id":tool_call.id,
                                "name":tool_call.name,
                                "input":tool_call.arguments.as_object(),
                            }));
                        }
                        AssistantContent::RejectedToolCall { .. } => blocks.push(json!({
                            "type":"text",
                            "text":"A previous tool call was rejected because its arguments were invalid. Regenerate it.",
                        })),
                    }
                }
                if opaque_iter.next().is_some() {
                    return Err(AnthropicAdapterError::InvalidContext(
                        "saved thinking placement has no transcript block".into(),
                    ));
                }
                if !blocks.is_empty() {
                    push_turn(&mut messages, "assistant", blocks);
                }
            }
        }
    }
    if native_block.is_some() {
        return Err(AnthropicAdapterError::InvalidContext(
            "native compaction requires a persisted message suffix".into(),
        ));
    }
    if !opaque_by_anchor.is_empty() {
        return Err(AnthropicAdapterError::InvalidContext(
            "Anthropic reasoning anchor is absent from the replay transcript".into(),
        ));
    }
    if !pending_tool_ids.is_empty() {
        return Err(AnthropicAdapterError::InvalidContext(
            "assistant tool_use is missing a following tool_result".into(),
        ));
    }
    for pair in messages.windows(2) {
        if pair[0].get("role") == pair[1].get("role") {
            return Err(AnthropicAdapterError::InvalidContext(
                "Anthropic messages must alternate user and assistant turns".into(),
            ));
        }
    }
    if native.is_empty()
        && messages
            .first()
            .and_then(|message| message.get("role"))
            .and_then(Value::as_str)
            .is_some_and(|role| role != "user")
    {
        return Err(AnthropicAdapterError::InvalidContext(
            "Anthropic conversation must begin with a user turn".into(),
        ));
    }
    if ensure_anthropic_spec(spec)?.supports_prompt_cache
        && let Some(last_user) = messages
            .iter_mut()
            .rev()
            .find(|message| message.get("role").and_then(Value::as_str) == Some("user"))
        && let Some(last_block) = last_user
            .get_mut("content")
            .and_then(Value::as_array_mut)
            .and_then(|content| content.last_mut())
        && let Some(object) = last_block.as_object_mut()
    {
        object.insert("cache_control".into(), json!({"type":"ephemeral"}));
    }
    Ok(messages)
}

fn validate_native_replay(
    spec: &ModelSpec,
    context: &PromptContext,
    native: &[(&ProviderContextItem, &Value, &NativeCompactionCoverage)],
) -> Result<(), AnthropicAdapterError> {
    let Some((item, block, coverage)) = native.first().copied() else {
        return Ok(());
    };
    if native.len() != 1 {
        return Err(AnthropicAdapterError::InvalidContext(
            "multiple Anthropic native compaction blocks".into(),
        ));
    }
    if item.origin_message.is_some() || item.wire_item_index.is_some() {
        return Err(AnthropicAdapterError::InvalidContext(
            "Anthropic native compaction has reasoning placement metadata".into(),
        ));
    }
    if coverage.through_message_seq == 0 {
        return Err(AnthropicAdapterError::InvalidContext(
            "Anthropic native coverage must be greater than zero".into(),
        ));
    }
    if !context.memory_blocks.is_empty() {
        return Err(AnthropicAdapterError::InvalidContext(
            "native compaction cannot coexist with memory blocks".into(),
        ));
    }
    validate_compaction_block(block)?;
    if coverage.context_fingerprint != context_fingerprint(spec, context)? {
        return Err(AnthropicAdapterError::InvalidContext(
            "native compaction context fingerprint mismatch".into(),
        ));
    }
    validate_native_suffix(&context.messages, coverage.through_message_seq)
}

fn validate_native_suffix(
    messages: &[ContextMessage],
    coverage: u64,
) -> Result<(), AnthropicAdapterError> {
    let mut persisted_started = false;
    let mut previous = None;
    let mut suffix_started = false;
    for message in messages {
        let ContextMessage::Persisted { seq, .. } = message else {
            if persisted_started {
                return Err(AnthropicAdapterError::InvalidContext(
                    "native compaction suffix contains synthetic content after persisted history"
                        .into(),
                ));
            }
            continue;
        };
        persisted_started = true;
        if *seq == 0 || previous.is_some_and(|value: u64| value.checked_add(1) != Some(*seq)) {
            return Err(AnthropicAdapterError::InvalidContext(
                "persisted native replay sequence is gapped, duplicated, or reordered".into(),
            ));
        }
        if *seq > coverage {
            if !suffix_started
                && *seq
                    != coverage.checked_add(1).ok_or_else(|| {
                        AnthropicAdapterError::InvalidContext("native coverage overflow".into())
                    })?
            {
                return Err(AnthropicAdapterError::InvalidContext(
                    "native suffix must begin exactly at coverage + 1".into(),
                ));
            }
            suffix_started = true;
        } else if suffix_started {
            return Err(AnthropicAdapterError::InvalidContext(
                "covered history appears after the native suffix".into(),
            ));
        }
        previous = Some(*seq);
    }
    let max_seq = previous.ok_or_else(|| {
        AnthropicAdapterError::InvalidContext(
            "native compaction requires persisted replay history".into(),
        )
    })?;
    if coverage > max_seq {
        return Err(AnthropicAdapterError::InvalidContext(
            "native compaction coverage exceeds the latest persisted message sequence".into(),
        ));
    }
    Ok(())
}

fn anthropic_user_content(content: &UserContent, supports_images: bool) -> Value {
    match content {
        UserContent::Text { text } => json!({"type":"text","text":text}),
        UserContent::Image { data, mime_type } if supports_images => json!({
            "type":"image",
            "source":{"type":"base64","media_type":mime_type,"data":data},
        }),
        UserContent::Image { .. } => {
            json!({"type":"text","text":"(image omitted: model does not support image input)"})
        }
    }
}

fn push_turn(messages: &mut Vec<Value>, role: &str, mut blocks: Vec<Value>) {
    if blocks.is_empty() {
        return;
    }
    if (role == "user"
        || (role == "assistant"
            && messages.last().is_some_and(|last| {
                last.get("content")
                    .and_then(Value::as_array)
                    .is_some_and(|content| {
                        content.len() == 1
                            && content[0].get("type").and_then(Value::as_str) == Some("compaction")
                    })
            })))
        && let Some(last) = messages.last_mut()
        && last.get("role").and_then(Value::as_str) == Some(role)
    {
        last.get_mut("content")
            .and_then(Value::as_array_mut)
            .expect("constructed content array")
            .append(&mut blocks);
        return;
    }
    messages.push(json!({"role":role,"content":blocks}));
}

fn wire_index(content: &AssistantContent) -> u32 {
    match content {
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
    }
}

fn validate_reasoning_item(value: &Value) -> Result<(), AnthropicAdapterError> {
    let object = value.as_object().ok_or_else(|| {
        AnthropicAdapterError::InvalidContext("reasoning payload must be an object".into())
    })?;
    let (allowed, field) = match object.get("type").and_then(Value::as_str) {
        Some("thinking_signature") => (&["type", "signature"][..], "signature"),
        Some("redacted_thinking") => (&["type", "data"][..], "data"),
        _ => {
            return Err(AnthropicAdapterError::InvalidContext(
                "invalid Anthropic reasoning payload type".into(),
            ));
        }
    };
    ensure_only_fields(object, allowed, "Anthropic reasoning payload")
        .map_err(AnthropicAdapterError::InvalidContext)?;
    if object
        .get(field)
        .and_then(Value::as_str)
        .is_none_or(str::is_empty)
    {
        return Err(AnthropicAdapterError::InvalidContext(format!(
            "Anthropic reasoning payload {field} must be a non-empty string"
        )));
    }
    Ok(())
}

fn ensure_only_fields(
    object: &Map<String, Value>,
    allowed: &[&str],
    label: &str,
) -> Result<(), String> {
    if let Some(field) = object
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        return Err(format!("{label} contains unsupported property {field}"));
    }
    Ok(())
}

fn validate_compaction_block(value: &Value) -> Result<(), AnthropicAdapterError> {
    if value.get("type").and_then(Value::as_str) != Some("compaction") {
        return Err(AnthropicAdapterError::InvalidContext(
            "Anthropic native block must have type compaction".into(),
        ));
    }
    if value.get("content").and_then(Value::as_str).is_none() {
        return Err(AnthropicAdapterError::InvalidContext(
            "Anthropic native compaction content must be a string".into(),
        ));
    }
    Ok(())
}

fn validate_tool_use_id(id: &str) -> Result<(), AnthropicAdapterError> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(AnthropicAdapterError::InvalidContext(
            "tool_use id must match [A-Za-z0-9_-]{1,64}".into(),
        ));
    }
    Ok(())
}

fn validate_inbound_tool_use_id(id: &str) -> Result<(), AnthropicAdapterError> {
    validate_tool_use_id(id).map_err(|_| {
        AnthropicAdapterError::InvalidEvent("tool_use id must match [A-Za-z0-9_-]{1,64}".into())
    })
}

fn escape_memory(text: &str) -> String {
    text.replace("</memory", "&lt;/memory")
}

pub fn context_fingerprint(
    spec: &ModelSpec,
    context: &PromptContext,
) -> Result<String, AnthropicAdapterError> {
    let compat = ensure_anthropic_spec(spec)?;
    let tools = serde_json::to_vec(&context.tools)
        .map_err(|error| AnthropicAdapterError::InvalidContext(error.to_string()))?;
    let mut hasher = Sha256::new();
    for bytes in [
        spec.provider_instance_id().as_bytes(),
        b"anthropic_messages",
        spec.id.as_bytes(),
        context.system_prompt.as_bytes(),
        tools.as_slice(),
        compat.beta_headers.join("\0").as_bytes(),
        ANTHROPIC_VERSION.as_bytes(),
    ] {
        hasher.update(bytes.len().to_be_bytes());
        hasher.update(bytes);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[derive(Debug)]
enum OpenBlock {
    Text {
        index: u32,
        content: String,
    },
    Thinking {
        index: u32,
        content: String,
        signature: Option<String>,
    },
    Redacted {
        index: u32,
        data: String,
    },
    Tool {
        index: u32,
        id: String,
        name: String,
        accumulator: ToolArgumentAccumulator,
    },
    Compaction {
        index: u32,
        block: Value,
    },
}

#[derive(Debug)]
struct ClosedTool {
    id: String,
    name: String,
    accumulator: ToolArgumentAccumulator,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    MessageStart,
    Blocks,
    MessageDeltas,
    Terminal,
}

#[derive(Debug)]
pub struct AnthropicReceiveState {
    schemas: FrozenToolSchemaRegistry,
    budget: ResponseBudget,
    phase: Phase,
    open: Option<OpenBlock>,
    closed_tools: BTreeMap<u32, ClosedTool>,
    next_index: u32,
    usage: Usage,
    reason: Option<StopReason>,
    reason_wire: Option<String>,
    response_id: Option<String>,
    response_model: Option<String>,
    expected_model: String,
    provider_context: Vec<ProviderContextFragment>,
    content_bytes: usize,
    event_count: usize,
    preview_work_bytes: usize,
    saw_tool: bool,
    seen_tool_ids: HashSet<String>,
    coverage: Option<NativeCompactionCoverage>,
}

impl AnthropicReceiveState {
    pub fn with_budget(
        schemas: FrozenToolSchemaRegistry,
        budget: ResponseBudget,
        coverage: Option<NativeCompactionCoverage>,
        expected_model: impl Into<String>,
    ) -> Self {
        Self {
            schemas,
            budget,
            phase: Phase::MessageStart,
            open: None,
            closed_tools: BTreeMap::new(),
            next_index: 0,
            usage: Usage::default(),
            reason: None,
            reason_wire: None,
            response_id: None,
            response_model: None,
            expected_model: expected_model.into(),
            provider_context: Vec::new(),
            content_bytes: 0,
            event_count: 0,
            preview_work_bytes: 0,
            saw_tool: false,
            seen_tool_ids: HashSet::new(),
            coverage,
        }
    }

    pub fn usage(&self) -> &Usage {
        &self.usage
    }

    pub fn verified_reasoning_context(&self) -> Vec<ProviderContextFragment> {
        self.provider_context
            .iter()
            .filter(|fragment| {
                matches!(
                    fragment.payload,
                    ProviderContextPayload::EncryptedReasoning {
                        protocol: ApiProtocol::AnthropicMessages,
                        ..
                    }
                )
            })
            .cloned()
            .collect()
    }

    pub fn push_named(
        &mut self,
        event_name: Option<&str>,
        payload: &str,
    ) -> Result<AnthropicPush, AnthropicAdapterError> {
        let value: Value = serde_json::from_str(payload)
            .map_err(|error| AnthropicAdapterError::InvalidEvent(error.to_string()))?;
        let object = value.as_object().ok_or_else(|| {
            AnthropicAdapterError::InvalidEvent("event payload must be an object".into())
        })?;
        let kind = required_str(object, "type")?;
        if let Some(name) = event_name
            && name != kind
        {
            return Err(AnthropicAdapterError::InvalidEvent(format!(
                "SSE event name {name} does not match payload type {kind}"
            )));
        }
        if self.phase == Phase::Terminal {
            return Err(AnthropicAdapterError::InvalidEvent(
                "event arrived after terminal".into(),
            ));
        }
        if kind == "ping" {
            return Ok(AnthropicPush::default());
        }
        if kind == "error" {
            return self.provider_error(object);
        }
        match kind {
            "message_start" => self.message_start(object),
            "content_block_start" => self.block_start(object),
            "content_block_delta" => self.block_delta(object),
            "content_block_stop" => self.block_stop(object),
            "message_delta" => self.message_delta(object),
            "message_stop" => self.message_stop(),
            other => {
                tracing::debug!(event_type = other, "ignored unknown Anthropic event");
                Ok(AnthropicPush::default())
            }
        }
    }

    pub fn finish_eof(&self) -> Result<(), AnthropicAdapterError> {
        if self.phase == Phase::Terminal {
            Ok(())
        } else {
            Err(AnthropicAdapterError::MissingTerminal)
        }
    }

    pub fn fail(&mut self) -> Vec<ProviderEvent> {
        let Some(block) = self.open.take() else {
            return Vec::new();
        };
        match block {
            OpenBlock::Text { index, content } => vec![ProviderEvent::TextEnd {
                content_index: index as usize,
                content,
            }],
            // Only block_stop-verified signed thinking/redacted blocks survive.
            OpenBlock::Thinking { .. }
            | OpenBlock::Redacted { .. }
            | OpenBlock::Tool { .. }
            | OpenBlock::Compaction { .. } => Vec::new(),
        }
    }

    fn message_start(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<AnthropicPush, AnthropicAdapterError> {
        if self.phase != Phase::MessageStart {
            return Err(order_error("message_start"));
        }
        let message = object
            .get("message")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                AnthropicAdapterError::InvalidEvent("message must be an object".into())
            })?;
        let response_id = required_str(message, "id")?.to_owned();
        let model = required_str(message, "model")?;
        if model != self.expected_model {
            return Err(AnthropicAdapterError::InvalidEvent(
                "response model does not match requested model".into(),
            ));
        }
        let response_model = model.to_owned();
        if required_str(message, "role")? != "assistant" {
            return Err(AnthropicAdapterError::InvalidEvent(
                "message role must be assistant".into(),
            ));
        }
        if message
            .get("content")
            .and_then(Value::as_array)
            .is_none_or(|content| !content.is_empty())
        {
            return Err(AnthropicAdapterError::InvalidEvent(
                "message_start content must be an empty array".into(),
            ));
        }
        let usage = parse_usage(message.get("usage"))?;
        self.charge(response_id.len() + response_model.len(), 1, 0)?;
        self.response_id = Some(response_id);
        self.response_model = Some(response_model);
        self.usage = usage;
        self.phase = Phase::Blocks;
        Ok(AnthropicPush::default())
    }

    fn block_start(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<AnthropicPush, AnthropicAdapterError> {
        if self.phase != Phase::Blocks || self.open.is_some() {
            return Err(order_error("content_block_start"));
        }
        let index = event_index(object)?;
        if index != self.next_index {
            return Err(AnthropicAdapterError::InvalidEvent(
                "content block index is missing, duplicated, or reordered".into(),
            ));
        }
        let block = object
            .get("content_block")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                AnthropicAdapterError::InvalidEvent("content_block must be an object".into())
            })?;
        let kind = required_str(block, "type")?;
        let (open, events, is_tool, content_charge) = match kind {
            "text" => {
                if !string_field(block, "text")?.is_empty() {
                    return Err(AnthropicAdapterError::InvalidEvent(
                        "text block must start empty".into(),
                    ));
                }
                (
                    OpenBlock::Text {
                        index,
                        content: String::new(),
                    },
                    vec![ProviderEvent::TextStart {
                        content_index: index as usize,
                    }],
                    false,
                    0,
                )
            }
            "thinking" => {
                ensure_only_fields(block, &["type", "thinking", "signature"], "thinking block")
                    .map_err(AnthropicAdapterError::InvalidEvent)?;
                if !string_field(block, "thinking")?.is_empty() {
                    return Err(AnthropicAdapterError::InvalidEvent(
                        "thinking block must start empty".into(),
                    ));
                }
                let signature = match block.get("signature") {
                    None => None,
                    Some(Value::String(signature)) if signature.is_empty() => None,
                    Some(Value::String(signature)) => Some(signature.to_owned()),
                    Some(_) => {
                        return Err(AnthropicAdapterError::InvalidEvent(
                            "thinking block signature must be a string when present".into(),
                        ));
                    }
                };
                let content_charge = signature.as_ref().map_or(0, String::len);
                (
                    OpenBlock::Thinking {
                        index,
                        content: String::new(),
                        signature,
                    },
                    vec![ProviderEvent::ThinkingStart {
                        content_index: index as usize,
                        signature_field: "signature".into(),
                    }],
                    false,
                    content_charge,
                )
            }
            "redacted_thinking" => {
                ensure_only_fields(block, &["type", "data"], "redacted_thinking block")
                    .map_err(AnthropicAdapterError::InvalidEvent)?;
                let data = required_str(block, "data")?.to_owned();
                let content_charge = data.len();
                (
                    OpenBlock::Redacted { index, data },
                    vec![
                        ProviderEvent::ThinkingStart {
                            content_index: index as usize,
                            signature_field: "redacted_thinking.data".into(),
                        },
                        ProviderEvent::ThinkingDelta {
                            content_index: index as usize,
                            delta: REDACTED_PLACEHOLDER.into(),
                        },
                    ],
                    false,
                    content_charge,
                )
            }
            "tool_use" => {
                let input = block
                    .get("input")
                    .and_then(Value::as_object)
                    .ok_or_else(|| {
                        AnthropicAdapterError::InvalidEvent(
                            "tool_use.input must be an object".into(),
                        )
                    })?;
                if !input.is_empty() {
                    return Err(AnthropicAdapterError::InvalidEvent(
                        "streamed tool_use must start with empty input".into(),
                    ));
                }
                let id = required_str(block, "id")?.to_owned();
                validate_inbound_tool_use_id(&id)?;
                if self.seen_tool_ids.contains(&id) {
                    return Err(AnthropicAdapterError::InvalidEvent(
                        "duplicate tool_use id in response stream".into(),
                    ));
                }
                let name = required_str(block, "name")?.to_owned();
                let content_charge = id.len().checked_add(name.len()).ok_or(
                    AnthropicAdapterError::ResponseLimitExceeded {
                        resource: "content_bytes",
                        limit: self.budget.max_content_bytes,
                    },
                )?;
                (
                    OpenBlock::Tool {
                        index,
                        id,
                        name,
                        accumulator: ToolArgumentAccumulator::new(),
                    },
                    vec![ProviderEvent::ToolCallStart {
                        content_index: index as usize,
                    }],
                    true,
                    content_charge,
                )
            }
            "compaction" => {
                if self.coverage.is_none() {
                    return Err(AnthropicAdapterError::InvalidEvent(
                        "compaction block arrived without request coverage".into(),
                    ));
                }
                if self.next_index != 0 {
                    return Err(AnthropicAdapterError::InvalidEvent(
                        "streamed compaction block must be index 0 and before all other blocks"
                            .into(),
                    ));
                }
                match block.get("content") {
                    Some(Value::Null) => {
                        return Err(AnthropicAdapterError::NativeCompactionFailed {
                            code: "compaction_content_null".into(),
                        });
                    }
                    Some(value) if value.as_str().is_some_and(|text| text.is_empty()) => {}
                    Some(_) => {
                        return Err(AnthropicAdapterError::InvalidEvent(
                            "streamed compaction block must start with empty content".into(),
                        ));
                    }
                    None => {
                        return Err(AnthropicAdapterError::InvalidEvent(
                            "streamed compaction block is missing content".into(),
                        ));
                    }
                }
                (
                    OpenBlock::Compaction {
                        index,
                        block: Value::Object(block.clone()),
                    },
                    Vec::new(),
                    false,
                    0,
                )
            }
            other => {
                return Err(AnthropicAdapterError::InvalidEvent(format!(
                    "unsupported content block type {other}"
                )));
            }
        };
        self.charge(content_charge, events.len(), 0)?;
        if let OpenBlock::Tool { id, .. } = &open {
            self.seen_tool_ids.insert(id.clone());
        }
        self.saw_tool |= is_tool;
        self.open = Some(open);
        Ok(AnthropicPush {
            events,
            terminal: None,
        })
    }

    fn block_delta(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<AnthropicPush, AnthropicAdapterError> {
        if self.phase != Phase::Blocks {
            return Err(order_error("content_block_delta"));
        }
        let index = event_index(object)?;
        let delta = object
            .get("delta")
            .and_then(Value::as_object)
            .ok_or_else(|| AnthropicAdapterError::InvalidEvent("delta must be an object".into()))?;
        let kind = required_str(delta, "type")?;
        match kind {
            "thinking_delta" => {
                ensure_only_fields(
                    delta,
                    &["type", "thinking", "estimated_tokens"],
                    "thinking_delta",
                )
                .map_err(AnthropicAdapterError::InvalidEvent)?;
                if let Some(estimated) = delta.get("estimated_tokens")
                    && !estimated.is_null()
                    && estimated.as_u64().is_none()
                {
                    return Err(AnthropicAdapterError::InvalidEvent(
                        "thinking_delta estimated_tokens must be null or a non-negative integer"
                            .into(),
                    ));
                }
            }
            "signature_delta" => {
                ensure_only_fields(delta, &["type", "signature"], "signature_delta")
                    .map_err(AnthropicAdapterError::InvalidEvent)?;
            }
            _ => {}
        }
        let (value, preview_charge, event_count) = match (&self.open, kind) {
            (Some(OpenBlock::Text { index: open, .. }), "text_delta") if *open == index => {
                (string_field(delta, "text")?.to_owned(), 0, 1)
            }
            (
                Some(OpenBlock::Thinking {
                    index: open,
                    signature: None,
                    ..
                }),
                "thinking_delta",
            ) if *open == index => (string_field(delta, "thinking")?.to_owned(), 0, 1),
            (Some(OpenBlock::Thinking { index: open, .. }), "signature_delta")
                if *open == index =>
            {
                let signature = required_str(delta, "signature")?;
                if signature.is_empty() {
                    return Err(AnthropicAdapterError::InvalidEvent(
                        "signature_delta signature must be non-empty".into(),
                    ));
                }
                if let Some(OpenBlock::Thinking {
                    signature: Some(existing),
                    ..
                }) = &self.open
                    && existing != signature
                {
                    return Err(AnthropicAdapterError::InvalidEvent(
                        "thinking signature changed during content block".into(),
                    ));
                }
                (signature.to_owned(), 0, 0)
            }
            (Some(OpenBlock::Compaction { index: open, block }), "compaction_delta")
                if *open == index =>
            {
                if block.get("content").and_then(Value::as_str) != Some("") {
                    return Err(AnthropicAdapterError::InvalidEvent(
                        "compaction block must start with empty content".into(),
                    ));
                }
                (string_field(delta, "content")?.to_owned(), 0, 0)
            }
            (
                Some(OpenBlock::Tool {
                    index: open,
                    accumulator,
                    ..
                }),
                "input_json_delta",
            ) if *open == index => {
                let value = string_field(delta, "partial_json")?.to_owned();
                let preview_work = accumulator.raw_len().checked_add(value.len()).ok_or(
                    AnthropicAdapterError::ResponseLimitExceeded {
                        resource: "preview_parse_work",
                        limit: self.budget.max_preview_work_bytes,
                    },
                )?;
                (value, preview_work, 2)
            }
            _ => {
                return Err(AnthropicAdapterError::InvalidEvent(
                    "delta type/index does not match the open content block".into(),
                ));
            }
        };
        self.charge(value.len(), event_count, preview_charge)?;
        let events = match (&mut self.open, kind) {
            (Some(OpenBlock::Text { content, .. }), "text_delta") => {
                content.push_str(&value);
                vec![ProviderEvent::TextDelta {
                    content_index: index as usize,
                    delta: value,
                }]
            }
            (Some(OpenBlock::Thinking { content, .. }), "thinking_delta") => {
                content.push_str(&value);
                vec![ProviderEvent::ThinkingDelta {
                    content_index: index as usize,
                    delta: value,
                }]
            }
            (Some(OpenBlock::Thinking { signature, .. }), "signature_delta") => {
                if signature.is_none() {
                    *signature = Some(value);
                }
                Vec::new()
            }
            (Some(OpenBlock::Compaction { block, .. }), "compaction_delta") => {
                block
                    .as_object_mut()
                    .expect("validated compaction object")
                    .insert("content".into(), json!(value));
                Vec::new()
            }
            (Some(OpenBlock::Tool { accumulator, .. }), "input_json_delta") => {
                let preview = accumulator.append(&value);
                vec![
                    ProviderEvent::ToolCallDelta {
                        content_index: index as usize,
                        delta: value,
                    },
                    ProviderEvent::ToolCallPreview {
                        content_index: index as usize,
                        preview,
                    },
                ]
            }
            _ => unreachable!("open block validated before charge commit"),
        };
        Ok(AnthropicPush {
            events,
            terminal: None,
        })
    }

    fn block_stop(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<AnthropicPush, AnthropicAdapterError> {
        if self.phase != Phase::Blocks {
            return Err(order_error("content_block_stop"));
        }
        let index = event_index(object)?;
        let event_count = match self.open.as_ref() {
            Some(OpenBlock::Text { index: open, .. }) if *open == index => 1,
            Some(OpenBlock::Thinking {
                index: open,
                signature,
                ..
            }) if *open == index => {
                if signature.as_deref().is_none_or(str::is_empty) {
                    return Err(AnthropicAdapterError::InvalidEvent(
                        "thinking block ended without a signature".into(),
                    ));
                }
                1
            }
            Some(OpenBlock::Redacted { index: open, .. }) if *open == index => 1,
            Some(OpenBlock::Tool { index: open, .. }) if *open == index => {
                if self.closed_tools.contains_key(&index) {
                    return Err(AnthropicAdapterError::InvalidEvent(
                        "duplicate closed tool block".into(),
                    ));
                }
                0
            }
            Some(OpenBlock::Compaction { index: open, block }) if *open == index => {
                validate_compaction_block(block)?;
                0
            }
            Some(_) => {
                return Err(AnthropicAdapterError::InvalidEvent(
                    "content block stop index does not match open block".into(),
                ));
            }
            None => {
                return Err(AnthropicAdapterError::InvalidEvent(
                    "no open content block".into(),
                ));
            }
        };
        let next_index = self.next_index.checked_add(1).ok_or_else(|| {
            AnthropicAdapterError::InvalidEvent("content block index exceeds u32".into())
        })?;
        self.charge(0, event_count, 0)?;
        let block = self
            .open
            .take()
            .expect("open block validated before charge");
        let mut events = Vec::new();
        match block {
            OpenBlock::Text {
                index: open,
                content,
            } if open == index => events.push(ProviderEvent::TextEnd {
                content_index: index as usize,
                content,
            }),
            OpenBlock::Thinking {
                index: open,
                content,
                signature,
            } if open == index => {
                events.push(ProviderEvent::ThinkingEnd {
                    content_index: index as usize,
                    content,
                });
                self.provider_context.push(ProviderContextFragment {
                    wire_item_index: Some(index),
                    payload: ProviderContextPayload::EncryptedReasoning {
                        protocol: ApiProtocol::AnthropicMessages,
                        item: json!({"type":"thinking_signature","signature":signature.expect("validated signature")}),
                    },
                });
            }
            OpenBlock::Redacted { index: open, data } if open == index => {
                events.push(ProviderEvent::ThinkingEnd {
                    content_index: index as usize,
                    content: REDACTED_PLACEHOLDER.into(),
                });
                self.provider_context.push(ProviderContextFragment {
                    wire_item_index: Some(index),
                    payload: ProviderContextPayload::EncryptedReasoning {
                        protocol: ApiProtocol::AnthropicMessages,
                        item: json!({"type":"redacted_thinking","data":data}),
                    },
                });
            }
            OpenBlock::Tool {
                index: open,
                id,
                name,
                accumulator,
            } if open == index => {
                self.closed_tools.insert(
                    index,
                    ClosedTool {
                        id,
                        name,
                        accumulator,
                    },
                );
            }
            OpenBlock::Compaction { index: open, block } if open == index => {
                let coverage = self.coverage.clone().expect("validated at block start");
                self.provider_context.push(ProviderContextFragment {
                    wire_item_index: None,
                    payload: ProviderContextPayload::AnthropicCompaction { block, coverage },
                });
            }
            _ => unreachable!("open block kind/index validated before charge"),
        }
        self.next_index = next_index;
        Ok(AnthropicPush {
            events,
            terminal: None,
        })
    }

    fn message_delta(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<AnthropicPush, AnthropicAdapterError> {
        if !matches!(self.phase, Phase::Blocks | Phase::MessageDeltas) || self.open.is_some() {
            return Err(order_error("message_delta"));
        }
        // Usage is accounting sideband rather than executable response state.
        // Parse it before stop validation and keep it if later validation or
        // budget charging fails.
        let usage = object
            .get("usage")
            .map(|usage| merge_usage(&self.usage, usage))
            .transpose()?
            .unwrap_or_else(|| self.usage.clone());
        let result = (|| -> Result<AnthropicPush, AnthropicAdapterError> {
            let delta = object
                .get("delta")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    AnthropicAdapterError::InvalidEvent("delta must be an object".into())
                })?;
            let (reason_wire, reason) = match delta.get("stop_reason") {
                None | Some(Value::Null) => (None, None),
                Some(Value::String(reason)) if !reason.is_empty() => (
                    Some(reason.as_str()),
                    Some(match reason.as_str() {
                        "end_turn" | "stop_sequence" | "compaction" => StopReason::Stop,
                        "pause_turn" => {
                            return Err(AnthropicAdapterError::InvalidEvent(
                                "unsupported stop reason pause_turn: server-tool continuation is outside Sumi scope".into(),
                            ));
                        }
                        "max_tokens" => StopReason::Length,
                        "model_context_window_exceeded" => StopReason::Error,
                        "tool_use" if self.saw_tool => StopReason::ToolUse,
                        "tool_use" => {
                            return Err(AnthropicAdapterError::InvalidEvent(
                                "tool_use stop reason without tool block".into(),
                            ));
                        }
                        "refusal" | "sensitive" => StopReason::Error,
                        other => {
                            return Err(AnthropicAdapterError::InvalidEvent(format!(
                                "unsupported stop reason {other}"
                            )));
                        }
                    }),
                ),
                _ => {
                    return Err(AnthropicAdapterError::InvalidEvent(
                        "stop_reason must be a non-empty string or null".into(),
                    ));
                }
            };
            if let (Some(previous), Some(next)) = (self.reason_wire.as_deref(), reason_wire)
                && previous != next
            {
                return Err(AnthropicAdapterError::InvalidEvent(
                    "stop reason changed across message_delta events".into(),
                ));
            }
            self.charge(0, 1, 0)?;
            self.reason = self.reason.or(reason);
            if self.reason_wire.is_none() {
                self.reason_wire = reason_wire.map(str::to_owned);
            }
            self.usage = usage.clone();
            self.phase = Phase::MessageDeltas;
            Ok(AnthropicPush::default())
        })();
        if result.is_err() {
            self.usage = usage;
        }
        result
    }

    fn message_stop(&mut self) -> Result<AnthropicPush, AnthropicAdapterError> {
        if self.phase != Phase::MessageDeltas {
            return Err(order_error("message_stop"));
        }
        let reason = self
            .reason
            .ok_or_else(|| AnthropicAdapterError::InvalidEvent("missing stop reason".into()))?;
        if self.reason_wire.as_deref() == Some("compaction") {
            let fragments = self
                .provider_context
                .iter()
                .filter(|fragment| {
                    matches!(
                        fragment.payload,
                        ProviderContextPayload::AnthropicCompaction { .. }
                    )
                })
                .collect::<Vec<_>>();
            let coverage = self.coverage.as_ref().ok_or_else(|| {
                AnthropicAdapterError::InvalidEvent(
                    "compaction stop reason arrived without request coverage".into(),
                )
            })?;
            let [fragment] = fragments.as_slice() else {
                return Err(AnthropicAdapterError::InvalidEvent(
                    "compaction stop reason requires exactly one native compaction fragment".into(),
                ));
            };
            let ProviderContextPayload::AnthropicCompaction {
                block,
                coverage: fragment_coverage,
            } = &fragment.payload
            else {
                unreachable!("filtered native compaction fragment")
            };
            validate_compaction_block(block)?;
            if fragment.wire_item_index.is_some() || fragment_coverage != coverage {
                return Err(AnthropicAdapterError::InvalidEvent(
                    "native compaction fragment does not preserve request coverage".into(),
                ));
            }
        }
        self.charge(0, self.closed_tools.len(), 0)?;
        self.phase = Phase::Terminal;
        let mut events = Vec::with_capacity(self.closed_tools.len());
        for (index, tool) in std::mem::take(&mut self.closed_tools) {
            let outcome = if reason == StopReason::Length {
                tool.accumulator
                    .reject_incomplete(tool.id, tool.name, Utc::now())
            } else {
                tool.accumulator
                    .finish(tool.id, tool.name, &self.schemas, Utc::now())
            };
            events.push(match outcome {
                ToolArgumentOutcome::Validated(tool_call) => ProviderEvent::ToolCallEnd {
                    content_index: index as usize,
                    tool_call,
                },
                ToolArgumentOutcome::Rejected {
                    rejected,
                    synthetic_result,
                } => ProviderEvent::ToolCallRejected {
                    content_index: index as usize,
                    rejected,
                    synthetic_result,
                },
            });
        }
        let provider_context = std::mem::take(&mut self.provider_context);
        let provider_code = self
            .reason_wire
            .as_ref()
            .and_then(|code| (reason == StopReason::Error).then(|| code.clone()));
        let error_message = match provider_code.as_deref() {
            Some("refusal") => Some("Anthropic refused the response".into()),
            Some("sensitive") => Some("Anthropic marked the response as sensitive".into()),
            _ => None,
        };
        Ok(AnthropicPush {
            events: Vec::new(),
            terminal: Some(AnthropicTerminal {
                events,
                reason,
                usage: self.usage.clone(),
                error_message,
                provider_code,
                provider_context,
            }),
        })
    }

    fn provider_error(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<AnthropicPush, AnthropicAdapterError> {
        let error = object
            .get("error")
            .and_then(Value::as_object)
            .ok_or_else(|| AnthropicAdapterError::InvalidEvent("error must be an object".into()))?;
        let message = required_str(error, "message")?.to_owned();
        let code = error
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("provider_error")
            .to_owned();
        let reason = StopReason::Error;
        let events = self.fail();
        self.phase = Phase::Terminal;
        Ok(AnthropicPush {
            events: Vec::new(),
            terminal: Some(AnthropicTerminal {
                events,
                reason,
                usage: self.usage.clone(),
                error_message: Some(message),
                provider_code: Some(code),
                provider_context: std::mem::take(&mut self.provider_context),
            }),
        })
    }

    fn charge(
        &mut self,
        content: usize,
        events: usize,
        preview: usize,
    ) -> Result<(), AnthropicAdapterError> {
        let content_bytes = checked_counter(
            self.content_bytes,
            content,
            self.budget.max_content_bytes,
            "content_bytes",
        )?;
        let event_count = checked_counter(
            self.event_count,
            events,
            self.budget.max_events,
            "event_count",
        )?;
        let preview_work_bytes = checked_counter(
            self.preview_work_bytes,
            preview,
            self.budget.max_preview_work_bytes,
            "preview_parse_work",
        )?;
        self.content_bytes = content_bytes;
        self.event_count = event_count;
        self.preview_work_bytes = preview_work_bytes;
        Ok(())
    }
}

fn checked_counter(
    current: usize,
    additional: usize,
    limit: usize,
    resource: &'static str,
) -> Result<usize, AnthropicAdapterError> {
    let value = current
        .checked_add(additional)
        .ok_or(AnthropicAdapterError::ResponseLimitExceeded { resource, limit })?;
    if value > limit {
        return Err(AnthropicAdapterError::ResponseLimitExceeded { resource, limit });
    }
    Ok(value)
}

fn parse_usage(value: Option<&Value>) -> Result<Usage, AnthropicAdapterError> {
    let Some(value) = value else {
        return Ok(Usage::default());
    };
    merge_usage(&Usage::default(), value)
}

fn merge_usage(current: &Usage, value: &Value) -> Result<Usage, AnthropicAdapterError> {
    let object = value
        .as_object()
        .ok_or_else(|| AnthropicAdapterError::InvalidEvent("usage must be an object".into()))?;
    let input = optional_u64(object, "input_tokens")?.unwrap_or(current.input);
    let output = optional_u64(object, "output_tokens")?.unwrap_or(current.output);
    let cache_read = optional_u64(object, "cache_read_input_tokens")?.unwrap_or(current.cache_read);
    let cache_write =
        optional_u64(object, "cache_creation_input_tokens")?.unwrap_or(current.cache_write);
    let reasoning = match object.get("output_tokens_details") {
        None => current.reasoning,
        Some(value) => {
            let details = value.as_object().ok_or_else(|| {
                AnthropicAdapterError::InvalidEvent(
                    "output_tokens_details must be an object".into(),
                )
            })?;
            optional_u64(details, "thinking_tokens")?.unwrap_or(current.reasoning)
        }
    };
    for (field, previous, next) in [
        ("input_tokens", current.input, input),
        ("output_tokens", current.output, output),
        ("cache_read_input_tokens", current.cache_read, cache_read),
        (
            "cache_creation_input_tokens",
            current.cache_write,
            cache_write,
        ),
        ("thinking_tokens", current.reasoning, reasoning),
    ] {
        if next < previous {
            return Err(AnthropicAdapterError::InvalidEvent(format!(
                "{field} decreased across cumulative usage updates"
            )));
        }
    }
    if reasoning > output {
        return Err(AnthropicAdapterError::InvalidEvent(
            "thinking_tokens exceeds output_tokens".into(),
        ));
    }
    let total_tokens = input
        .checked_add(cache_read)
        .and_then(|total| total.checked_add(cache_write))
        .and_then(|total| total.checked_add(output))
        .ok_or_else(|| {
            AnthropicAdapterError::InvalidEvent("usage token total exceeds u64".into())
        })?;
    Ok(Usage {
        input,
        output,
        cache_read,
        cache_write,
        reasoning,
        total_tokens,
    })
}

fn optional_u64(
    object: &Map<String, Value>,
    field: &str,
) -> Result<Option<u64>, AnthropicAdapterError> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or_else(|| {
            AnthropicAdapterError::InvalidEvent(format!("{field} must be an unsigned integer"))
        }),
    }
}

fn required_str<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, AnthropicAdapterError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            AnthropicAdapterError::InvalidEvent(format!("{field} must be a non-empty string"))
        })
}

fn string_field<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, AnthropicAdapterError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| AnthropicAdapterError::InvalidEvent(format!("{field} must be a string")))
}

fn event_index(object: &Map<String, Value>) -> Result<u32, AnthropicAdapterError> {
    let index = object.get("index").and_then(Value::as_u64).ok_or_else(|| {
        AnthropicAdapterError::InvalidEvent("index must be an unsigned integer".into())
    })?;
    u32::try_from(index)
        .map_err(|_| AnthropicAdapterError::InvalidEvent("index exceeds u32".into()))
}

fn order_error(event: &str) -> AnthropicAdapterError {
    AnthropicAdapterError::InvalidEvent(format!("{event} arrived out of order"))
}

pub fn request_coverage(
    spec: &ModelSpec,
    context: &PromptContext,
    native_compaction: bool,
) -> Result<Option<NativeCompactionCoverage>, AnthropicAdapterError> {
    if !ensure_anthropic_spec(spec)?.supports_native_compact || !native_compaction {
        return Ok(None);
    }
    let mut previous: Option<u64> = None;
    let mut persisted_started = false;
    for message in &context.messages {
        let ContextMessage::Persisted { seq, .. } = message else {
            if persisted_started {
                return Err(AnthropicAdapterError::InvalidContext(
                    "native compaction requires persisted messages to form a trailing suffix"
                        .into(),
                ));
            }
            continue;
        };
        persisted_started = true;
        if *seq == 0 {
            return Err(AnthropicAdapterError::InvalidContext(
                "persisted message sequence must be greater than zero".into(),
            ));
        }
        if let Some(previous) = previous
            && previous.checked_add(1) != Some(*seq)
        {
            return Err(AnthropicAdapterError::InvalidContext(
                "persisted message sequence is duplicated, nonmonotonic, or gapped".into(),
            ));
        }
        previous = Some(*seq);
    }
    Ok(
        previous.map(|through_message_seq| NativeCompactionCoverage {
            through_message_seq,
            context_fingerprint: context_fingerprint(spec, context)
                .expect("spec was validated above"),
        }),
    )
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::provider::types::{AssistantMessage, ToolResultMessage, UserMessage};

    fn spec() -> ModelSpec {
        ModelSpec::preset("anthropic").expect("preset")
    }

    fn context(messages: Vec<ContextMessage>) -> PromptContext {
        PromptContext {
            system_prompt: "constitution".into(),
            memory_blocks: Vec::new(),
            messages,
            provider_context: Vec::new(),
            tools: vec![ToolDefinition {
                name: "read_file".into(),
                description: "read".into(),
                parameters: json!({
                    "type":"object",
                    "properties":{"path":{"type":"string"}},
                    "required":["path"],
                    "additionalProperties":false,
                }),
            }],
        }
    }

    fn synthetic(message: Message) -> ContextMessage {
        ContextMessage::Synthetic { message }
    }

    fn persisted(seq: u64) -> ContextMessage {
        ContextMessage::Persisted {
            id: format!("message-{seq}"),
            seq,
            message: Message::User(UserMessage {
                content: vec![UserContent::Text {
                    text: format!("message {seq}"),
                }],
                timestamp: timestamp(),
            }),
        }
    }

    fn timestamp() -> chrono::DateTime<Utc> {
        Utc.timestamp_millis_opt(1_700_000_000_000)
            .single()
            .expect("timestamp")
    }

    #[test]
    fn request_uses_top_level_system_merges_users_and_pairs_tools() {
        let assistant = AssistantMessage {
            content: vec![AssistantContent::ToolCall {
                tool_call: crate::provider::types::ToolCall {
                    id: "toolu_1".into(),
                    name: "read_file".into(),
                    arguments:
                        crate::provider::types::ValidatedToolArguments::from_schema_validated(
                            json!({"path":"a"}).as_object().expect("object").clone(),
                        ),
                },
                wire_item_index: 0,
            }],
            model: spec().id.clone(),
            provider: spec().provider.clone(),
            origin: spec().origin(),
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: timestamp(),
        };
        let value = build_request(
            &spec(),
            &context(vec![
                synthetic(Message::User(UserMessage {
                    content: vec![UserContent::Text { text: "a".into() }],
                    timestamp: timestamp(),
                })),
                synthetic(Message::User(UserMessage {
                    content: vec![UserContent::Text { text: "b".into() }],
                    timestamp: timestamp(),
                })),
                synthetic(Message::Assistant(assistant)),
                synthetic(Message::ToolResult(ToolResultMessage {
                    tool_call_id: "toolu_1".into(),
                    tool_name: "read_file".into(),
                    content: vec![UserContent::Text { text: "ok".into() }],
                    details: Value::Null,
                    is_error: false,
                    timestamp: timestamp(),
                })),
            ]),
            &RequestOptions::default(),
        )
        .expect("request");
        assert_eq!(value["system"][0]["text"], "constitution");
        assert_eq!(value["system"][0]["cache_control"]["type"], "ephemeral");
        assert_eq!(value["messages"].as_array().expect("messages").len(), 3);
        assert_eq!(value["messages"][0]["content"].as_array().unwrap().len(), 2);
        assert_eq!(value["messages"][1]["content"][0]["type"], "tool_use");
        assert_eq!(value["messages"][2]["content"][0]["type"], "tool_result");
    }

    #[test]
    fn native_compaction_is_opt_in_and_default_requests_omit_native_context() {
        let spec = spec();
        let mut context = context(vec![persisted(1)]);
        let coverage = NativeCompactionCoverage {
            through_message_seq: 1,
            context_fingerprint: context_fingerprint(&spec, &context).expect("fingerprint"),
        };
        context.provider_context.push(ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            payload: ProviderContextPayload::AnthropicCompaction {
                block: json!({"type":"compaction","content":"NATIVE_MARKER"}),
                coverage,
            },
        });

        let default_request = build_request(&spec, &context, &RequestOptions::default())
            .expect("canonical three-layer request");
        assert!(default_request.get("context_management").is_none());
        assert!(!default_request.to_string().contains("NATIVE_MARKER"));

        let native_request = build_request(
            &spec,
            &context,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("opt-in native request");
        assert_eq!(
            native_request["context_management"],
            json!({"edits":[{"type":"compact_20260112"}]})
        );
        assert!(native_request.to_string().contains("NATIVE_MARKER"));

        let mut unsupported = spec.clone();
        if let ProtocolCompat::Anthropic(compat) = &mut unsupported.compat {
            compat.supports_native_compact = false;
        }
        let unsupported_request = build_request(
            &unsupported,
            &context,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("capability loss falls back");
        assert!(unsupported_request.get("context_management").is_none());
        assert!(!unsupported_request.to_string().contains("NATIVE_MARKER"));
    }

    #[test]
    fn native_compaction_request_requires_persisted_coverage() {
        let request = build_request(
            &spec(),
            &context(vec![synthetic(Message::User(UserMessage {
                content: vec![UserContent::Text {
                    text: "first turn".into(),
                }],
                timestamp: timestamp(),
            }))]),
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("synthetic-only request remains valid");
        assert!(request.get("context_management").is_none());

        let request = build_request(
            &spec(),
            &context(vec![persisted(1)]),
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("persisted request has anchored coverage");
        assert_eq!(
            request["context_management"],
            json!({"edits":[{"type":"compact_20260112"}]})
        );
    }

    #[test]
    fn foreign_native_context_forces_three_layer_fallback() {
        let spec = spec();
        let mut context = context(vec![persisted(1)]);
        context.provider_context.push(ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({
                    "id":"cmp",
                    "type":"compaction",
                    "encrypted_content":"FOREIGN_NATIVE"
                })],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 1,
                    context_fingerprint: "foreign".into(),
                },
            },
        });
        let request = build_request(
            &spec,
            &context,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("foreign native state falls back");
        assert!(!request.to_string().contains("FOREIGN_NATIVE"));
        assert!(request.to_string().contains("message 1"));
    }

    #[test]
    fn non_finite_temperature_is_rejected_before_anthropic_json_construction() {
        let context = context(vec![synthetic(Message::User(UserMessage {
            content: vec![UserContent::Text {
                text: "hello".into(),
            }],
            timestamp: timestamp(),
        }))]);
        for temperature in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let error = build_request(
                &spec(),
                &context,
                &RequestOptions {
                    temperature: Some(temperature),
                    ..RequestOptions::default()
                },
            )
            .expect_err("non-finite temperature");
            assert!(
                matches!(error, AnthropicAdapterError::InvalidTemperature(value)
                if value.to_bits() == temperature.to_bits())
            );
        }
        let finite = build_request(
            &spec(),
            &context,
            &RequestOptions {
                temperature: Some(0.7),
                ..RequestOptions::default()
            },
        )
        .expect("finite Anthropic temperature");
        assert!(finite.get("temperature").is_none());
    }

    #[test]
    fn tool_result_images_follow_model_image_capability() {
        for supports_images in [false, true] {
            let mut model = spec();
            model.supports_images = supports_images;
            let assistant = AssistantMessage {
                content: vec![AssistantContent::ToolCall {
                    tool_call: crate::provider::types::ToolCall {
                        id: "toolu_image".into(),
                        name: "read_file".into(),
                        arguments:
                            crate::provider::types::ValidatedToolArguments::from_schema_validated(
                                json!({"path":"image.png"})
                                    .as_object()
                                    .expect("object")
                                    .clone(),
                            ),
                    },
                    wire_item_index: 0,
                }],
                model: model.id.clone(),
                provider: model.provider.clone(),
                origin: model.origin(),
                usage: Usage::default(),
                stop_reason: StopReason::ToolUse,
                error_message: None,
                provider_code: None,
                interrupted: false,
                timestamp: timestamp(),
            };
            let request = build_request(
                &model,
                &context(vec![
                    synthetic(Message::User(UserMessage {
                        content: vec![UserContent::Text {
                            text: "read image".into(),
                        }],
                        timestamp: timestamp(),
                    })),
                    synthetic(Message::Assistant(assistant)),
                    synthetic(Message::ToolResult(ToolResultMessage {
                        tool_call_id: "toolu_image".into(),
                        tool_name: "read_file".into(),
                        content: vec![UserContent::Image {
                            data: "base64-image-payload".into(),
                            mime_type: "image/png".into(),
                        }],
                        details: Value::Null,
                        is_error: false,
                        timestamp: timestamp(),
                    })),
                ]),
                &RequestOptions::default(),
            )
            .expect("request");
            let block = &request["messages"][2]["content"][0]["content"][0];
            if supports_images {
                assert_eq!(block["type"], "image");
                assert_eq!(block["source"]["type"], "base64");
                assert_eq!(block["source"]["media_type"], "image/png");
                assert_eq!(block["source"]["data"], "base64-image-payload");
            } else {
                assert_eq!(block["type"], "text");
                assert_eq!(
                    block["text"],
                    "(image omitted: model does not support image input)"
                );
                assert!(!request.to_string().contains("base64-image-payload"));
            }
        }
    }

    #[test]
    fn thinking_rejects_forced_tool_choice() {
        for choice in [json!("any"), json!({"type":"tool","name":"read_file"})] {
            let prompt = synthetic(Message::User(UserMessage {
                content: vec![UserContent::Text {
                    text: "use a tool".into(),
                }],
                timestamp: timestamp(),
            }));
            let error = build_request(
                &spec(),
                &context(vec![prompt]),
                &RequestOptions {
                    tool_choice: Some(choice),
                    ..RequestOptions::default()
                },
            )
            .expect_err("forced choice rejected");
            assert!(error.to_string().contains("only allow"));
        }
    }

    #[test]
    fn tool_choice_requires_the_official_local_shape() {
        assert_eq!(
            normalize_tool_choice(
                &json!({"type":"tool","name":"read_file","disable_parallel_tool_use":true}),
                false,
            )
            .unwrap(),
            json!({"type":"tool","name":"read_file","disable_parallel_tool_use":true})
        );
        for malformed in [
            json!("tool"),
            json!({"type":"tool"}),
            json!({"type":"tool","name":""}),
            json!({"type":"tool","name":7}),
            json!({"type":"tool","name":"read_file","disable_parallel_tool_use":"yes"}),
            json!({"type":"none","name":"read_file"}),
            json!({"type":"auto","future":true}),
        ] {
            assert!(
                normalize_tool_choice(&malformed, false).is_err(),
                "{malformed}"
            );
        }
    }

    #[test]
    fn fixture_normalizes_thinking_tool_usage_and_order() {
        let schemas =
            FrozenToolSchemaRegistry::compile(&context(Vec::new()).tools).expect("schemas");
        let mut state = AnthropicReceiveState::with_budget(
            schemas,
            ResponseBudget::for_output_tokens(1024).expect("budget"),
            None,
            spec().id,
        );
        let fixture = include_str!("../../../tests/fixtures/anthropic_messages_official.sse");
        let mut events = Vec::new();
        let mut terminal = None;
        for frame in fixture
            .split("\n\n")
            .filter(|frame| frame.lines().any(|line| line.starts_with("data: ")))
        {
            let mut name = None;
            let mut data = None;
            for line in frame.lines() {
                if let Some(value) = line.strip_prefix("event: ") {
                    name = Some(value);
                } else if let Some(value) = line.strip_prefix("data: ") {
                    data = Some(value);
                }
            }
            let pushed = state
                .push_named(name, data.expect("data"))
                .expect("valid fixture");
            events.extend(pushed.events);
            terminal = terminal.or(pushed.terminal);
        }
        let terminal = terminal.expect("terminal");
        events.extend(terminal.events.clone());
        assert_eq!(terminal.reason, StopReason::ToolUse);
        assert_eq!(terminal.usage.input, 10);
        assert_eq!(terminal.usage.cache_read, 3);
        assert_eq!(terminal.usage.cache_write, 4);
        assert_eq!(terminal.usage.output, 8);
        assert_eq!(terminal.usage.total_tokens, 25);
        assert!(matches!(
            events.as_slice(),
            [
                ProviderEvent::ThinkingStart {
                    content_index: 0,
                    signature_field,
                },
                ProviderEvent::ThinkingDelta {
                    content_index: 0,
                    delta: thinking,
                },
                ProviderEvent::ThinkingEnd {
                    content_index: 0,
                    content: thinking_end,
                },
                ProviderEvent::ThinkingStart {
                    content_index: 1,
                    signature_field: redacted_field,
                },
                ProviderEvent::ThinkingDelta {
                    content_index: 1,
                    delta: redacted,
                },
                ProviderEvent::ThinkingEnd {
                    content_index: 1,
                    content: redacted_end,
                },
                ProviderEvent::ToolCallStart { content_index: 2 },
                ProviderEvent::ToolCallDelta {
                    content_index: 2,
                    delta: first_delta,
                },
                ProviderEvent::ToolCallPreview {
                    content_index: 2,
                    ..
                },
                ProviderEvent::ToolCallDelta {
                    content_index: 2,
                    delta: second_delta,
                },
                ProviderEvent::ToolCallPreview {
                    content_index: 2,
                    preview,
                },
                ProviderEvent::ToolCallEnd {
                    content_index: 2,
                    tool_call,
                },
            ] if signature_field == "signature"
                && thinking == "I should read."
                && thinking_end == "I should read."
                && redacted_field == "redacted_thinking.data"
                && redacted == REDACTED_PLACEHOLDER
                && redacted_end == REDACTED_PLACEHOLDER
                && first_delta == "{\"path\":"
                && second_delta == "\"notes.txt\"}"
                && preview == &json!({"path":"notes.txt"})
                && tool_call.id == "toolu_01"
                && tool_call.name == "read_file"
                && tool_call.arguments.as_object() == json!({"path":"notes.txt"}).as_object().unwrap()
        ));
        assert_eq!(
            terminal.provider_context,
            vec![
                ProviderContextFragment {
                    wire_item_index: Some(0),
                    payload: ProviderContextPayload::EncryptedReasoning {
                        protocol: ApiProtocol::AnthropicMessages,
                        item: json!({"type":"thinking_signature","signature":"sig_opaque"}),
                    },
                },
                ProviderContextFragment {
                    wire_item_index: Some(1),
                    payload: ProviderContextPayload::EncryptedReasoning {
                        protocol: ApiProtocol::AnthropicMessages,
                        item: json!({"type":"redacted_thinking","data":"redacted_opaque"}),
                    },
                },
            ]
        );
    }

    #[test]
    fn missing_or_reordered_signature_fails_closed() {
        let schemas = FrozenToolSchemaRegistry::compile(&[]).expect("schemas");
        let budget = ResponseBudget::for_output_tokens(1024).expect("budget");
        let mut missing =
            AnthropicReceiveState::with_budget(schemas.clone(), budget, None, "claude");
        missing
            .push_named(
                Some("message_start"),
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
            )
            .unwrap();
        missing
            .push_named(
                Some("content_block_start"),
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}"#,
            )
            .unwrap();
        assert!(
            missing
                .push_named(
                    Some("content_block_stop"),
                    r#"{"type":"content_block_stop","index":0}"#,
                )
                .is_err()
        );

        let mut reordered = AnthropicReceiveState::with_budget(schemas, budget, None, "claude");
        reordered
            .push_named(
                Some("message_start"),
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
            )
            .unwrap();
        assert!(
            reordered
                .push_named(
                    Some("content_block_start"),
                    r#"{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}"#,
                )
                .is_err()
        );
    }

    #[test]
    fn cancellation_discards_unsigned_thinking_and_tool() {
        let schemas =
            FrozenToolSchemaRegistry::compile(&context(Vec::new()).tools).expect("schemas");
        let budget = ResponseBudget::for_output_tokens(1024).expect("budget");
        for start in [
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}"#,
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t","name":"read_file","input":{}}}"#,
        ] {
            let mut state =
                AnthropicReceiveState::with_budget(schemas.clone(), budget, None, "claude");
            state
                .push_named(
                    Some("message_start"),
                    r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
                )
                .unwrap();
            state
                .push_named(Some("content_block_start"), start)
                .unwrap();
            assert!(state.fail().is_empty());
        }
    }

    #[test]
    fn continuation_merges_exact_signature_before_tool_use() {
        let spec = spec();
        let anchor = ProviderContextAnchor {
            message_id: "assistant-1".into(),
            message_seq: 2,
        };
        let assistant = AssistantMessage {
            content: vec![
                AssistantContent::Thinking {
                    thinking: "private body".into(),
                    signature_field: "signature".into(),
                    wire_item_index: 0,
                },
                AssistantContent::ToolCall {
                    tool_call: crate::provider::types::ToolCall {
                        id: "toolu_1".into(),
                        name: "read_file".into(),
                        arguments:
                            crate::provider::types::ValidatedToolArguments::from_schema_validated(
                                json!({"path":"a"}).as_object().unwrap().clone(),
                            ),
                    },
                    wire_item_index: 1,
                },
            ],
            model: spec.id.clone(),
            provider: spec.provider.clone(),
            origin: spec.origin(),
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: timestamp(),
        };
        let mut context = context(vec![
            ContextMessage::Persisted {
                id: "user-1".into(),
                seq: 1,
                message: Message::User(UserMessage {
                    content: vec![UserContent::Text {
                        text: "use the tool".into(),
                    }],
                    timestamp: timestamp(),
                }),
            },
            ContextMessage::Persisted {
                id: anchor.message_id.clone(),
                seq: anchor.message_seq,
                message: Message::Assistant(assistant),
            },
            ContextMessage::Persisted {
                id: "result-1".into(),
                seq: 3,
                message: Message::ToolResult(ToolResultMessage {
                    tool_call_id: "toolu_1".into(),
                    tool_name: "read_file".into(),
                    content: vec![UserContent::Text { text: "ok".into() }],
                    details: Value::Null,
                    is_error: false,
                    timestamp: timestamp(),
                }),
            },
        ]);
        context.provider_context.push(ProviderContextItem {
            origin_message: Some(anchor),
            wire_item_index: Some(0),
            ordinal: 0,
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::AnthropicMessages,
                item: json!({"type":"thinking_signature","signature":"opaque-sig"}),
            },
        });
        let request =
            build_request(&spec, &context, &RequestOptions::default()).expect("continuation");
        let blocks = request["messages"][1]["content"].as_array().unwrap();
        assert_eq!(blocks[0]["type"], "thinking");
        assert_eq!(blocks[0]["thinking"], "private body");
        assert_eq!(blocks[0]["signature"], "opaque-sig");
        assert_eq!(blocks[1]["type"], "tool_use");

        let mut changed_mode = spec.clone();
        changed_mode.reasoning = false;
        assert!(
            build_request(&changed_mode, &context, &RequestOptions::default())
                .expect_err("thinking mode change rejected")
                .to_string()
                .contains("mode changed")
        );

        context.provider_context.clear();
        assert!(build_request(&spec, &context, &RequestOptions::default()).is_err());
    }

    #[test]
    fn cross_origin_opaque_and_raw_thinking_are_omitted_from_request() {
        let mut source = spec();
        source.base_url = "https://source.example/v1".into();
        source.account_scope = "source-account".into();
        let mut target = spec();
        target.base_url = "https://target.example/v1".into();
        target.account_scope = "target-account".into();
        let anchor = ProviderContextAnchor {
            message_id: "assistant-2".into(),
            message_seq: 2,
        };
        let assistant = AssistantMessage {
            content: vec![
                AssistantContent::Thinking {
                    thinking: "RAW_THINKING_MARKER".into(),
                    signature_field: "signature".into(),
                    wire_item_index: 0,
                },
                AssistantContent::Text {
                    text: "PUBLIC_TEXT_MARKER".into(),
                    wire_item_index: 1,
                },
            ],
            model: source.id.clone(),
            provider: source.provider.clone(),
            origin: source.origin(),
            usage: Usage::default(),
            stop_reason: StopReason::Stop,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: timestamp(),
        };
        let mut context = context(vec![
            ContextMessage::Persisted {
                id: "user-1".into(),
                seq: 1,
                message: Message::User(UserMessage {
                    content: vec![UserContent::Text {
                        text: "public user".into(),
                    }],
                    timestamp: timestamp(),
                }),
            },
            ContextMessage::Persisted {
                id: anchor.message_id.clone(),
                seq: anchor.message_seq,
                message: Message::Assistant(assistant),
            },
        ]);
        context.provider_context.push(ProviderContextItem {
            origin_message: Some(anchor),
            wire_item_index: Some(0),
            ordinal: 0,
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::AnthropicMessages,
                item: json!({"type":"thinking_signature","signature":"OPAQUE_MARKER"}),
            },
        });
        let request = build_request(&target, &context, &RequestOptions::default()).unwrap();
        let wire = request.to_string();
        assert!(wire.contains("PUBLIC_TEXT_MARKER"));
        assert!(!wire.contains("RAW_THINKING_MARKER"));
        assert!(!wire.contains("OPAQUE_MARKER"));

        if let ContextMessage::Persisted {
            message: Message::Assistant(assistant),
            ..
        } = &mut context.messages[1]
        {
            assistant.origin = target.origin();
            assistant.model.clone_from(&target.id);
            assistant.provider.clone_from(&target.provider);
        } else {
            unreachable!();
        }
        context.provider_context[0].payload = ProviderContextPayload::EncryptedReasoning {
            protocol: ApiProtocol::OpenAiResponses,
            item: json!({"malformed":"SAME_ORIGIN_MARKER"}),
        };
        assert!(build_request(&target, &context, &RequestOptions::default()).is_err());
    }

    #[test]
    fn native_compaction_uses_coverage_suffix_and_terminal_fragment() {
        let spec = spec();
        let mut context = context(vec![
            synthetic(Message::User(UserMessage {
                content: vec![UserContent::Text {
                    text: "leading-synthetic".into(),
                }],
                timestamp: timestamp(),
            })),
            ContextMessage::Persisted {
                id: "old".into(),
                seq: 1,
                message: Message::User(UserMessage {
                    content: vec![UserContent::Text {
                        text: "old-prefix-marker".into(),
                    }],
                    timestamp: timestamp(),
                }),
            },
            ContextMessage::Persisted {
                id: "compacted-response".into(),
                seq: 2,
                message: Message::Assistant(AssistantMessage {
                    content: vec![AssistantContent::Text {
                        text: "assistant-suffix-marker".into(),
                        wire_item_index: 1,
                    }],
                    model: spec.id.clone(),
                    provider: spec.provider.clone(),
                    origin: spec.origin(),
                    usage: Usage::default(),
                    stop_reason: StopReason::Stop,
                    error_message: None,
                    provider_code: None,
                    interrupted: false,
                    timestamp: timestamp(),
                }),
            },
            ContextMessage::Persisted {
                id: "new".into(),
                seq: 3,
                message: Message::User(UserMessage {
                    content: vec![UserContent::Text {
                        text: "suffix-marker".into(),
                    }],
                    timestamp: timestamp(),
                }),
            },
        ]);
        let coverage = NativeCompactionCoverage {
            through_message_seq: 1,
            context_fingerprint: context_fingerprint(&spec, &context).unwrap(),
        };
        context.provider_context.push(ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            payload: ProviderContextPayload::AnthropicCompaction {
                block: json!({"type":"compaction","content":"opaque-compact"}),
                coverage: coverage.clone(),
            },
        });
        let request = build_request(
            &spec,
            &context,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("native request");
        let serialized = serde_json::to_string(&request).unwrap();
        assert!(!serialized.contains("old-prefix-marker"));
        assert!(serialized.contains("suffix-marker"));
        assert!(serialized.contains("assistant-suffix-marker"));
        assert!(serialized.contains("opaque-compact"));
        assert_eq!(
            request["context_management"],
            json!({"edits":[{"type":"compact_20260112"}]})
        );
        assert_eq!(request["messages"][0]["role"], "user");
        assert_eq!(
            request["messages"][0]["content"],
            json!([{"type":"text","text":"leading-synthetic"}])
        );
        assert_eq!(request["messages"][1]["role"], "assistant");
        assert_eq!(
            request["messages"][1]["content"],
            json!([
                {"type":"compaction","content":"opaque-compact"},
                {"type":"text","text":"assistant-suffix-marker"},
            ])
        );
        assert_eq!(request["messages"][2]["role"], "user");

        let schemas = FrozenToolSchemaRegistry::compile(&[]).unwrap();
        let mut receive = AnthropicReceiveState::with_budget(
            schemas,
            ResponseBudget::for_output_tokens(1024).unwrap(),
            Some(coverage.clone()),
            spec.id.clone(),
        );
        receive
            .push_named(
                Some("message_start"),
                &format!(
                    r#"{{"type":"message_start","message":{{"id":"m","model":"{}","role":"assistant","content":[],"usage":{{}}}}}}"#,
                    spec.id
                ),
            )
            .unwrap();
        receive
            .push_named(
                Some("content_block_start"),
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"compaction","content":""}}"#,
            )
            .unwrap();
        receive
            .push_named(
                Some("content_block_delta"),
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"compaction_delta","content":"opaque-new"}}"#,
            )
            .unwrap();
        receive
            .push_named(
                Some("content_block_stop"),
                r#"{"type":"content_block_stop","index":0}"#,
            )
            .unwrap();
        receive
            .push_named(
                Some("message_delta"),
                r#"{"type":"message_delta","delta":{"stop_reason":"compaction"},"usage":{"output_tokens":1}}"#,
            )
            .unwrap();
        let terminal = receive
            .push_named(Some("message_stop"), r#"{"type":"message_stop"}"#)
            .unwrap()
            .terminal
            .unwrap();
        assert_eq!(
            terminal.provider_context[0].payload,
            ProviderContextPayload::AnthropicCompaction {
                block: json!({"type":"compaction","content":"opaque-new"}),
                coverage,
            }
        );
        assert_eq!(terminal.provider_context[0].wire_item_index, None);
    }

    #[test]
    fn compaction_stop_reason_requires_one_valid_fragment_with_exact_coverage() {
        let coverage = NativeCompactionCoverage {
            through_message_seq: 1,
            context_fingerprint: "fingerprint".into(),
        };
        let fragment =
            |block: Value, fragment_coverage: NativeCompactionCoverage| ProviderContextFragment {
                wire_item_index: None,
                payload: ProviderContextPayload::AnthropicCompaction {
                    block,
                    coverage: fragment_coverage,
                },
            };
        for provider_context in [
            Vec::new(),
            vec![
                fragment(
                    json!({"type":"compaction","content":"one"}),
                    coverage.clone(),
                ),
                fragment(
                    json!({"type":"compaction","content":"two"}),
                    coverage.clone(),
                ),
            ],
            vec![fragment(
                json!({"type":"compaction","content":null}),
                coverage.clone(),
            )],
            vec![fragment(
                json!({"type":"compaction","content":"opaque"}),
                NativeCompactionCoverage {
                    through_message_seq: 2,
                    context_fingerprint: "other".into(),
                },
            )],
        ] {
            let mut state = AnthropicReceiveState::with_budget(
                FrozenToolSchemaRegistry::compile(&[]).unwrap(),
                ResponseBudget::default(),
                Some(coverage.clone()),
                "claude",
            );
            state.phase = Phase::MessageDeltas;
            state.reason = Some(StopReason::Stop);
            state.reason_wire = Some("compaction".into());
            state.provider_context = provider_context;
            assert!(state.message_stop().is_err());
            assert_eq!(state.phase, Phase::MessageDeltas);
        }

        let mut valid = AnthropicReceiveState::with_budget(
            FrozenToolSchemaRegistry::compile(&[]).unwrap(),
            ResponseBudget::default(),
            Some(coverage.clone()),
            "claude",
        );
        valid.phase = Phase::MessageDeltas;
        valid.reason = Some(StopReason::Stop);
        valid.reason_wire = Some("compaction".into());
        valid.provider_context = vec![fragment(
            json!({"type":"compaction","content":"opaque"}),
            coverage,
        )];
        assert!(valid.message_stop().unwrap().terminal.is_some());
    }

    #[test]
    fn malformed_native_compaction_falls_back_and_valid_opaque_fields_are_preserved() {
        let spec = spec();
        let mut context = context(vec![persisted(1)]);
        let coverage = NativeCompactionCoverage {
            through_message_seq: 1,
            context_fingerprint: context_fingerprint(&spec, &context).unwrap(),
        };
        let native = |block| ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            payload: ProviderContextPayload::AnthropicCompaction {
                block,
                coverage: coverage.clone(),
            },
        };

        for malformed in [
            json!({"type":"compaction"}),
            json!({"type":"compaction","content":null}),
            json!({"type":"compaction","content":7}),
            json!({"type":"compaction","content":{}}),
        ] {
            context.provider_context = vec![native(malformed)];
            let request = build_request(
                &spec,
                &context,
                &RequestOptions {
                    native_compaction: true,
                    ..RequestOptions::default()
                },
            )
            .expect("malformed persisted native state falls back");
            assert!(request.get("context_management").is_some());
            assert!(!request.to_string().contains("opaque"));
        }

        let opaque = json!({
            "type":"compaction",
            "content":"opaque",
            "future_provider_field":{"nested":[1,2,3]}
        });
        context.provider_context = vec![native(opaque.clone())];
        let request = build_request(
            &spec,
            &context,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("unknown compaction fields remain opaque");
        assert_eq!(request["messages"][0]["content"][0], opaque);
    }

    #[test]
    fn invalid_native_suffix_placement_falls_back_to_durable_messages() {
        let spec = spec();
        let mut context = context(vec![
            persisted(1),
            synthetic(Message::User(UserMessage {
                content: vec![UserContent::Text {
                    text: "late".into(),
                }],
                timestamp: timestamp(),
            })),
        ]);
        let coverage = NativeCompactionCoverage {
            through_message_seq: 1,
            context_fingerprint: context_fingerprint(&spec, &context).unwrap(),
        };
        context.provider_context.push(ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            payload: ProviderContextPayload::AnthropicCompaction {
                block: json!({"type":"compaction","content":"opaque"}),
                coverage,
            },
        });
        let request = build_request(
            &spec,
            &context,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("invalid native placement falls back");
        let serialized = request.to_string();
        assert!(!serialized.contains("opaque"));
        assert!(serialized.contains("late"));
    }

    #[test]
    fn native_fingerprint_and_suffix_gap_fall_back_without_sending_stale_block() {
        let spec = spec();
        let mut durable = context(vec![persisted(1), persisted(2)]);
        durable.provider_context.push(ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            payload: ProviderContextPayload::AnthropicCompaction {
                block: json!({"type":"compaction","content":"STALE"}),
                coverage: NativeCompactionCoverage {
                    through_message_seq: 1,
                    context_fingerprint: "wrong".into(),
                },
            },
        });
        let request = build_request(
            &spec,
            &durable,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("fingerprint fallback");
        assert!(!request.to_string().contains("STALE"));

        let mut gap = context(vec![persisted(3)]);
        gap.provider_context.push(ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            payload: ProviderContextPayload::AnthropicCompaction {
                block: json!({"type":"compaction","content":"GAP"}),
                coverage: NativeCompactionCoverage {
                    through_message_seq: 1,
                    context_fingerprint: context_fingerprint(&spec, &gap).unwrap(),
                },
            },
        });
        let request = build_request(
            &spec,
            &gap,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("suffix gap fallback");
        assert!(!request.to_string().contains("GAP"));
    }

    #[test]
    fn stream_error_and_eof_are_explicit_terminals() {
        let schemas = FrozenToolSchemaRegistry::compile(&[]).unwrap();
        let budget = ResponseBudget::for_output_tokens(1024).unwrap();
        let mut errored =
            AnthropicReceiveState::with_budget(schemas.clone(), budget, None, "claude");
        let terminal = errored
            .push_named(
                Some("error"),
                r#"{"type":"error","error":{"type":"overloaded_error","message":"busy"}}"#,
            )
            .unwrap()
            .terminal
            .unwrap();
        assert_eq!(terminal.reason, StopReason::Error);
        assert_eq!(terminal.provider_code.as_deref(), Some("overloaded_error"));

        let mut overflow = AnthropicReceiveState::with_budget(
            FrozenToolSchemaRegistry::compile(&[]).unwrap(),
            budget,
            None,
            "claude",
        );
        let terminal = overflow
            .push_named(
                Some("error"),
                r#"{"type":"error","error":{"type":"model_context_window_exceeded","message":"prompt is too long"}}"#,
            )
            .unwrap()
            .terminal
            .unwrap();
        assert_eq!(terminal.reason, StopReason::Error);
        assert_eq!(
            terminal.provider_code.as_deref(),
            Some("model_context_window_exceeded")
        );
        assert_eq!(
            terminal.error_message.as_deref(),
            Some("prompt is too long")
        );

        let incomplete = AnthropicReceiveState::with_budget(schemas, budget, None, "claude");
        assert!(matches!(
            incomplete.finish_eof(),
            Err(AnthropicAdapterError::MissingTerminal)
        ));
    }

    #[test]
    fn provider_error_closes_trusted_text_and_preserves_only_verified_context() {
        let mut state = AnthropicReceiveState::with_budget(
            FrozenToolSchemaRegistry::compile(&[]).unwrap(),
            ResponseBudget::default(),
            None,
            "claude",
        );
        for (name, payload) in [
            (
                "message_start",
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
            ),
            (
                "content_block_start",
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}"#,
            ),
            (
                "content_block_delta",
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"verified"}}"#,
            ),
            (
                "content_block_delta",
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig"}}"#,
            ),
            (
                "content_block_stop",
                r#"{"type":"content_block_stop","index":0}"#,
            ),
            (
                "content_block_start",
                r#"{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}"#,
            ),
            (
                "content_block_delta",
                r#"{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"trusted prefix"}}"#,
            ),
        ] {
            state.push_named(Some(name), payload).expect("setup event");
        }
        let terminal = state
            .push_named(
                Some("error"),
                r#"{"type":"error","error":{"type":"overloaded_error","message":"busy"}}"#,
            )
            .unwrap()
            .terminal
            .unwrap();
        assert!(matches!(
            terminal.events.as_slice(),
            [ProviderEvent::TextEnd { content_index: 1, content }] if content == "trusted prefix"
        ));
        assert_eq!(terminal.provider_context.len(), 1);
        assert!(matches!(
            terminal.provider_context[0].payload,
            ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::AnthropicMessages,
                ..
            }
        ));
    }

    #[test]
    fn provider_error_drops_each_unverified_open_block_kind() {
        let blocks = [
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}"#,
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"redacted_thinking","data":"opaque"}}"#,
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"read","input":{}}}"#,
        ];
        for block in blocks {
            let mut state = AnthropicReceiveState::with_budget(
                FrozenToolSchemaRegistry::compile(&[]).unwrap(),
                ResponseBudget::default(),
                None,
                "claude",
            );
            state
                .push_named(
                    Some("message_start"),
                    r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
                )
                .unwrap();
            state
                .push_named(Some("content_block_start"), block)
                .unwrap();
            let terminal = state
                .push_named(
                    Some("error"),
                    r#"{"type":"error","error":{"type":"server_error","message":"failed"}}"#,
                )
                .unwrap()
                .terminal
                .unwrap();
            assert!(terminal.events.is_empty(), "{block}");
            assert!(terminal.provider_context.is_empty(), "{block}");
        }

        let coverage = NativeCompactionCoverage {
            through_message_seq: 1,
            context_fingerprint: "fingerprint".into(),
        };
        let mut state = AnthropicReceiveState::with_budget(
            FrozenToolSchemaRegistry::compile(&[]).unwrap(),
            ResponseBudget::default(),
            Some(coverage),
            "claude",
        );
        state
            .push_named(
                Some("message_start"),
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
            )
            .unwrap();
        state
            .push_named(
                Some("content_block_start"),
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"compaction","content":""}}"#,
            )
            .unwrap();
        let terminal = state
            .push_named(
                Some("error"),
                r#"{"type":"error","error":{"type":"server_error","message":"failed"}}"#,
            )
            .unwrap()
            .terminal
            .unwrap();
        assert!(terminal.events.is_empty());
        assert!(terminal.provider_context.is_empty());
    }

    #[test]
    fn cancellation_context_contains_only_block_stop_verified_reasoning() {
        let schemas = FrozenToolSchemaRegistry::compile(&[]).unwrap();
        let budget = ResponseBudget::for_output_tokens(1024).unwrap();
        let mut state = AnthropicReceiveState::with_budget(schemas, budget, None, "claude");
        for (name, payload) in [
            (
                "message_start",
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
            ),
            (
                "content_block_start",
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}"#,
            ),
            (
                "content_block_delta",
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"complete"}}"#,
            ),
            (
                "content_block_delta",
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"verified"}}"#,
            ),
            (
                "content_block_stop",
                r#"{"type":"content_block_stop","index":0}"#,
            ),
            (
                "content_block_start",
                r#"{"type":"content_block_start","index":1,"content_block":{"type":"thinking","thinking":""}}"#,
            ),
            (
                "content_block_delta",
                r#"{"type":"content_block_delta","index":1,"delta":{"type":"thinking_delta","thinking":"partial"}}"#,
            ),
        ] {
            state.push_named(Some(name), payload).unwrap();
        }
        let saved = state.verified_reasoning_context();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].wire_item_index, Some(0));
        assert!(state.fail().is_empty());
    }

    #[test]
    fn signature_is_single_and_seals_thinking_transactionally() {
        let mut state = delta_test_state(
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}"#,
            None,
        );
        state
            .push_named(
                Some("content_block_delta"),
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"fixed"}}"#,
            )
            .unwrap();
        state
            .push_named(
                Some("content_block_delta"),
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"verified"}}"#,
            )
            .unwrap();
        let before = (
            format!("{:?}", state.open),
            state.content_bytes,
            state.event_count,
            state.preview_work_bytes,
        );
        for invalid in [
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"modified"}}"#,
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"replacement"}}"#,
        ] {
            assert!(
                state
                    .push_named(Some("content_block_delta"), invalid)
                    .is_err()
            );
            assert_eq!(
                before,
                (
                    format!("{:?}", state.open),
                    state.content_bytes,
                    state.event_count,
                    state.preview_work_bytes,
                )
            );
        }
        state
            .push_named(
                Some("content_block_stop"),
                r#"{"type":"content_block_stop","index":0}"#,
            )
            .unwrap();
        assert_eq!(state.verified_reasoning_context().len(), 1);
    }

    #[test]
    fn inbound_tool_use_ids_are_valid_and_unique_at_block_start() {
        let valid = r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_valid-1","name":"read_file","input":{}}}"#;
        let mut state = delta_test_state(valid, None);
        state
            .push_named(
                Some("content_block_stop"),
                r#"{"type":"content_block_stop","index":0}"#,
            )
            .unwrap();
        let before = (
            state.next_index,
            state.content_bytes,
            state.event_count,
            state.seen_tool_ids.clone(),
        );
        let duplicate = r#"{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_valid-1","name":"read_file","input":{}}}"#;
        assert!(
            state
                .push_named(Some("content_block_start"), duplicate)
                .is_err()
        );
        assert_eq!(
            before,
            (
                state.next_index,
                state.content_bytes,
                state.event_count,
                state.seen_tool_ids.clone(),
            )
        );

        for id in ["bad id", &"x".repeat(65)] {
            let block = json!({
                "type":"content_block_start",
                "index":0,
                "content_block":{"type":"tool_use","id":id,"name":"read_file","input":{}}
            });
            let mut malformed = AnthropicReceiveState::with_budget(
                FrozenToolSchemaRegistry::compile(&context(Vec::new()).tools).unwrap(),
                ResponseBudget::default(),
                None,
                "claude",
            );
            malformed
                .push_named(
                    Some("message_start"),
                    r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
                )
                .unwrap();
            let before = (malformed.content_bytes, malformed.event_count);
            assert!(
                malformed
                    .push_named(Some("content_block_start"), &block.to_string())
                    .is_err()
            );
            assert_eq!(before, (malformed.content_bytes, malformed.event_count));
            assert!(malformed.open.is_none());
            assert!(malformed.seen_tool_ids.is_empty());
        }
    }

    #[test]
    fn native_coverage_requires_contiguous_trailing_persisted_suffix() {
        let spec = spec();
        let valid = context(vec![
            synthetic(Message::User(UserMessage {
                content: vec![],
                timestamp: timestamp(),
            })),
            persisted(7),
            persisted(8),
        ]);
        assert_eq!(
            request_coverage(&spec, &valid, true)
                .unwrap()
                .expect("coverage")
                .through_message_seq,
            8
        );

        for messages in [
            vec![persisted(7), persisted(9)],
            vec![persisted(7), persisted(7)],
            vec![persisted(0)],
            vec![persisted(8), persisted(7)],
            vec![
                persisted(1),
                synthetic(Message::User(UserMessage {
                    content: vec![],
                    timestamp: timestamp(),
                })),
            ],
        ] {
            assert!(matches!(
                request_coverage(&spec, &context(messages), true),
                Err(AnthropicAdapterError::InvalidContext(_))
            ));
        }
    }

    #[test]
    fn invalid_tool_json_is_rejected_not_executable() {
        let schemas =
            FrozenToolSchemaRegistry::compile(&context(Vec::new()).tools).expect("schemas");
        let mut state = AnthropicReceiveState::with_budget(
            schemas,
            ResponseBudget::for_output_tokens(1024).unwrap(),
            None,
            "claude",
        );
        state
            .push_named(
                Some("message_start"),
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
            )
            .unwrap();
        state
            .push_named(
                Some("content_block_start"),
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t","name":"read_file","input":{}}}"#,
            )
            .unwrap();
        state
            .push_named(
                Some("content_block_delta"),
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"path\":"}}"#,
            )
            .unwrap();
        let events = state
            .push_named(
                Some("content_block_stop"),
                r#"{"type":"content_block_stop","index":0}"#,
            )
            .unwrap()
            .events;
        assert!(events.is_empty());
        state
            .push_named(
                Some("message_delta"),
                r#"{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{}}"#,
            )
            .unwrap();
        let events = state
            .push_named(Some("message_stop"), r#"{"type":"message_stop"}"#)
            .unwrap()
            .terminal
            .expect("terminal")
            .events;
        assert!(matches!(
            events.as_slice(),
            [ProviderEvent::ToolCallRejected { .. }]
        ));
    }

    #[test]
    fn max_tokens_rejects_closed_valid_tool_without_executable_end() {
        let schemas =
            FrozenToolSchemaRegistry::compile(&context(Vec::new()).tools).expect("schemas");
        let mut state = AnthropicReceiveState::with_budget(
            schemas,
            ResponseBudget::for_output_tokens(1024).unwrap(),
            None,
            "claude",
        );
        for (name, payload) in [
            (
                "message_start",
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
            ),
            (
                "content_block_start",
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t","name":"read_file","input":{}}}"#,
            ),
            (
                "content_block_delta",
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"path\":\"notes.txt\"}"}}"#,
            ),
        ] {
            state.push_named(Some(name), payload).unwrap();
        }
        let stopped = state
            .push_named(
                Some("content_block_stop"),
                r#"{"type":"content_block_stop","index":0}"#,
            )
            .unwrap();
        assert!(stopped.events.is_empty());
        state
            .push_named(
                Some("message_delta"),
                r#"{"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":4}}"#,
            )
            .unwrap();
        let terminal = state
            .push_named(Some("message_stop"), r#"{"type":"message_stop"}"#)
            .unwrap()
            .terminal
            .expect("terminal");
        assert_eq!(terminal.reason, StopReason::Length);
        assert!(matches!(
            terminal.events.as_slice(),
            [ProviderEvent::ToolCallRejected { rejected, .. }]
                if rejected.error == crate::provider::types::ToolArgumentError::IncompleteResponse
        ));
        assert!(
            !terminal
                .events
                .iter()
                .any(|event| matches!(event, ProviderEvent::ToolCallEnd { .. }))
        );
    }

    fn delta_test_state(
        block: &str,
        coverage: Option<NativeCompactionCoverage>,
    ) -> AnthropicReceiveState {
        let mut state = AnthropicReceiveState::with_budget(
            FrozenToolSchemaRegistry::compile(&context(Vec::new()).tools).unwrap(),
            ResponseBudget::default(),
            coverage,
            "claude",
        );
        state
            .push_named(
                Some("message_start"),
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
            )
            .unwrap();
        state
            .push_named(Some("content_block_start"), block)
            .unwrap();
        state
    }

    #[test]
    fn delta_budget_preflight_is_atomic_at_exact_boundaries() {
        let coverage = NativeCompactionCoverage {
            through_message_seq: 1,
            context_fingerprint: "fingerprint".into(),
        };
        for (block, delta, content_add, event_add, coverage) in [
            (
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"abc"}}"#,
                3,
                1,
                None,
            ),
            (
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}"#,
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"abc"}}"#,
                3,
                1,
                None,
            ),
            (
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}"#,
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"abc"}}"#,
                3,
                0,
                None,
            ),
            (
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"compaction","content":""}}"#,
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"compaction_delta","content":"abc"}}"#,
                3,
                0,
                Some(coverage.clone()),
            ),
        ] {
            let mut state = delta_test_state(block, coverage);
            let before_open = format!("{:?}", state.open);
            let before_counters = (
                state.content_bytes,
                state.event_count,
                state.preview_work_bytes,
            );
            state.budget.max_content_bytes = state.content_bytes + content_add - 1;
            assert!(
                state
                    .push_named(Some("content_block_delta"), delta)
                    .is_err()
            );
            assert_eq!(format!("{:?}", state.open), before_open);
            assert_eq!(
                (
                    state.content_bytes,
                    state.event_count,
                    state.preview_work_bytes,
                ),
                before_counters
            );
            state.budget.max_content_bytes = state.content_bytes + content_add;
            state.budget.max_events = state.event_count + event_add;
            state
                .push_named(Some("content_block_delta"), delta)
                .expect("exact byte/event boundary");
            assert_eq!(state.content_bytes, before_counters.0 + content_add);
            assert_eq!(state.event_count, before_counters.1 + event_add);
        }

        let tool_block = r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t","name":"read_file","input":{}}}"#;
        let tool_delta = r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"path\":\"a\"}"}}"#;
        let mut tool = delta_test_state(tool_block, None);
        let before_open = format!("{:?}", tool.open);
        let before_counters = (
            tool.content_bytes,
            tool.event_count,
            tool.preview_work_bytes,
        );
        tool.budget.max_content_bytes = tool.content_bytes + 12;
        tool.budget.max_events = tool.event_count + 2;
        tool.budget.max_preview_work_bytes = 11;
        assert!(
            tool.push_named(Some("content_block_delta"), tool_delta)
                .is_err()
        );
        assert_eq!(format!("{:?}", tool.open), before_open);
        assert_eq!(
            (
                tool.content_bytes,
                tool.event_count,
                tool.preview_work_bytes,
            ),
            before_counters
        );
        tool.budget.max_preview_work_bytes = 12;
        tool.push_named(Some("content_block_delta"), tool_delta)
            .expect("exact tool preview boundary");

        let mut late_counter = delta_test_state(
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
            None,
        );
        late_counter.budget.max_content_bytes = late_counter.content_bytes + 1;
        late_counter.budget.max_events = late_counter.event_count;
        let before = (
            late_counter.content_bytes,
            late_counter.event_count,
            format!("{:?}", late_counter.open),
        );
        assert!(
            late_counter
                .push_named(
                    Some("content_block_delta"),
                    r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x"}}"#,
                )
                .is_err()
        );
        assert_eq!(
            (
                late_counter.content_bytes,
                late_counter.event_count,
                format!("{:?}", late_counter.open),
            ),
            before
        );
    }

    #[test]
    fn failed_delta_terminal_keeps_only_last_trusted_prefix() {
        let mut state = delta_test_state(
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
            None,
        );
        state.budget.max_content_bytes = state.content_bytes + 2;
        state
            .push_named(
                Some("content_block_delta"),
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}"#,
            )
            .unwrap();
        assert!(
            state
                .push_named(
                    Some("content_block_delta"),
                    r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x"}}"#,
                )
                .is_err()
        );
        assert!(matches!(
            state.fail().as_slice(),
            [ProviderEvent::TextEnd { content, .. }] if content == "ok"
        ));
    }

    #[test]
    fn repeated_message_deltas_accumulate_usage_and_require_consistent_reason() {
        let mut state = AnthropicReceiveState::with_budget(
            FrozenToolSchemaRegistry::compile(&[]).unwrap(),
            ResponseBudget::default(),
            None,
            "claude",
        );
        state
            .push_named(
                Some("message_start"),
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{"input_tokens":3,"output_tokens":0}}}"#,
            )
            .unwrap();
        state
            .push_named(
                Some("message_delta"),
                r#"{"type":"message_delta","delta":{"stop_reason":null},"usage":{"output_tokens":2}}"#,
            )
            .expect("usage-only delta");
        state
            .push_named(
                Some("message_delta"),
                r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}"#,
            )
            .expect("terminal-reason delta");
        state
            .push_named(
                Some("message_delta"),
                r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":8}}"#,
            )
            .expect("same normalized stop reason");
        let terminal = state
            .push_named(Some("message_stop"), r#"{"type":"message_stop"}"#)
            .unwrap()
            .terminal
            .unwrap();
        assert_eq!(terminal.reason, StopReason::Stop);
        assert_eq!(terminal.usage.input, 3);
        assert_eq!(terminal.usage.output, 8);

        let mut inconsistent = AnthropicReceiveState::with_budget(
            FrozenToolSchemaRegistry::compile(&[]).unwrap(),
            ResponseBudget::default(),
            None,
            "claude",
        );
        inconsistent
            .push_named(
                Some("message_start"),
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
            )
            .unwrap();
        inconsistent
            .push_named(
                Some("message_delta"),
                r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}"#,
            )
            .unwrap();
        assert!(
            inconsistent
                .push_named(
                    Some("message_delta"),
                    r#"{"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":3}}"#,
                )
                .is_err()
        );
        assert_eq!(inconsistent.usage.output, 3);
        assert_eq!(inconsistent.reason, Some(StopReason::Stop));
    }

    #[test]
    fn message_delta_retains_valid_usage_when_charge_fails_without_semantic_commit() {
        let mut state = AnthropicReceiveState::with_budget(
            FrozenToolSchemaRegistry::compile(&[]).unwrap(),
            ResponseBudget::default(),
            None,
            "claude",
        );
        state
            .push_named(
                Some("message_start"),
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{"input_tokens":3,"output_tokens":0}}}"#,
            )
            .unwrap();
        state.budget.max_events = state.event_count;
        assert!(matches!(
            state.push_named(
                Some("message_delta"),
                r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":4}}"#,
            ),
            Err(AnthropicAdapterError::ResponseLimitExceeded {
                resource: "event_count",
                ..
            })
        ));
        assert_eq!(state.usage.input, 3);
        assert_eq!(state.usage.output, 4);
        assert_eq!(state.usage.total_tokens, 7);
        assert_eq!(state.reason, None);
        assert_eq!(state.reason_wire, None);
        assert_eq!(state.phase, Phase::Blocks);
    }

    #[test]
    fn usage_optional_details_are_typed_bounded_and_checked() {
        let valid = merge_usage(
            &Usage::default(),
            &json!({
                "input_tokens":10,
                "cache_read_input_tokens":3,
                "cache_creation_input_tokens":4,
                "output_tokens":8,
                "output_tokens_details":{"thinking_tokens":5}
            }),
        )
        .unwrap();
        assert_eq!(valid.reasoning, 5);
        assert_eq!(valid.total_tokens, 25);
        assert!(
            merge_usage(
                &Usage::default(),
                &json!({"input_tokens":1,"output_tokens":1})
            )
            .is_ok()
        );
        for malformed in [
            json!({"output_tokens":1,"output_tokens_details":null}),
            json!({"output_tokens":1,"output_tokens_details":[]}),
            json!({"output_tokens":1,"output_tokens_details":{"thinking_tokens":"1"}}),
            json!({"output_tokens":1,"output_tokens_details":{"thinking_tokens":2}}),
            json!({"input_tokens":u64::MAX,"output_tokens":1}),
        ] {
            assert!(
                merge_usage(&Usage::default(), &malformed).is_err(),
                "{malformed}"
            );
        }
    }

    #[test]
    fn sensitive_stop_reason_remains_distinct_from_refusal() {
        let mut state = AnthropicReceiveState::with_budget(
            FrozenToolSchemaRegistry::compile(&[]).unwrap(),
            ResponseBudget::default(),
            None,
            "claude",
        );
        state
            .push_named(
                Some("message_start"),
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
            )
            .unwrap();
        state
            .push_named(
                Some("message_delta"),
                r#"{"type":"message_delta","delta":{"stop_reason":"sensitive"},"usage":{"output_tokens":0}}"#,
            )
            .unwrap();
        let terminal = state
            .push_named(Some("message_stop"), r#"{"type":"message_stop"}"#)
            .unwrap()
            .terminal
            .unwrap();
        assert_eq!(terminal.provider_code.as_deref(), Some("sensitive"));
        assert_eq!(
            terminal.error_message.as_deref(),
            Some("Anthropic marked the response as sensitive")
        );
    }

    #[test]
    fn context_window_stop_reason_is_error_with_exact_provider_code() {
        let mut state = AnthropicReceiveState::with_budget(
            FrozenToolSchemaRegistry::compile(&[]).unwrap(),
            ResponseBudget::default(),
            None,
            "claude",
        );
        state
            .push_named(
                Some("message_start"),
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
            )
            .unwrap();
        state
            .push_named(
                Some("message_delta"),
                r#"{"type":"message_delta","delta":{"stop_reason":"model_context_window_exceeded"},"usage":{"output_tokens":0}}"#,
            )
            .unwrap();
        let terminal = state
            .push_named(Some("message_stop"), r#"{"type":"message_stop"}"#)
            .unwrap()
            .terminal
            .unwrap();
        assert_eq!(terminal.reason, StopReason::Error);
        assert_eq!(
            terminal.provider_code.as_deref(),
            Some("model_context_window_exceeded")
        );
        assert_eq!(terminal.error_message, None);
    }

    #[test]
    fn message_stop_requires_a_reason_and_unknown_top_level_events_are_ignored() {
        let mut state = AnthropicReceiveState::with_budget(
            FrozenToolSchemaRegistry::compile(&[]).unwrap(),
            ResponseBudget::default(),
            None,
            "claude",
        );
        state
            .push_named(
                Some("message_start"),
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
            )
            .unwrap();
        assert!(
            state
                .push_named(
                    Some("future_event"),
                    r#"{"type":"future_event","payload":{"type":"future_variant"}}"#,
                )
                .expect("unknown top-level event")
                .events
                .is_empty()
        );
        state
            .push_named(
                Some("message_delta"),
                r#"{"type":"message_delta","delta":{"stop_reason":null},"usage":{"output_tokens":1}}"#,
            )
            .unwrap();
        assert!(
            state
                .push_named(Some("message_stop"), r#"{"type":"message_stop"}"#)
                .is_err()
        );
    }

    #[test]
    fn reasoning_payload_variants_reject_unknown_and_cross_variant_fields() {
        for valid in [
            json!({"type":"thinking_signature","signature":"opaque"}),
            json!({"type":"redacted_thinking","data":"opaque"}),
        ] {
            validate_reasoning_item(&valid).expect("documented reasoning payload");
        }
        for invalid in [
            json!({"type":"thinking_signature","signature":"opaque","data":"cross"}),
            json!({"type":"redacted_thinking","data":"opaque","signature":"cross"}),
            json!({"type":"thinking_signature","signature":"opaque","future":true}),
            json!({"type":"redacted_thinking","data":"opaque","future":true}),
        ] {
            assert!(validate_reasoning_item(&invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn streamed_reasoning_variants_reject_unknown_fields_without_advancing_state() {
        let mut state = AnthropicReceiveState::with_budget(
            FrozenToolSchemaRegistry::compile(&[]).unwrap(),
            ResponseBudget::default(),
            None,
            "claude",
        );
        state
            .push_named(
                Some("message_start"),
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
            )
            .unwrap();
        assert!(
            state
                .push_named(
                    Some("content_block_start"),
                    r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","future":true}}"#,
                )
                .is_err()
        );
        state
            .push_named(
                Some("content_block_start"),
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}"#,
            )
            .expect("valid retry at unchanged block index");
        assert!(
            state
                .push_named(
                    Some("content_block_delta"),
                    r#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"x","future":true}}"#,
                )
                .is_err()
        );
        state
            .push_named(
                Some("content_block_delta"),
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"x","estimated_tokens":null}}"#,
            )
            .expect("documented nullable beta estimate is accepted");
    }

    #[test]
    fn thinking_start_signature_reconciles_without_losing_opaque_value() {
        for (start_signature, delta_signature) in [
            (r#""signature":"""#, Some("opaque")),
            (r#""signature":"opaque""#, None),
            (r#""signature":"opaque""#, Some("opaque")),
        ] {
            let mut state = AnthropicReceiveState::with_budget(
                FrozenToolSchemaRegistry::compile(&[]).unwrap(),
                ResponseBudget::default(),
                None,
                "claude",
            );
            state
                .push_named(
                    Some("message_start"),
                    r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
                )
                .unwrap();
            let start = format!(
                r#"{{"type":"content_block_start","index":0,"content_block":{{"type":"thinking","thinking":"",{start_signature}}}}}"#
            );
            state
                .push_named(Some("content_block_start"), &start)
                .expect("documented start signature shape");
            if let Some(signature) = delta_signature {
                let delta = format!(
                    r#"{{"type":"content_block_delta","index":0,"delta":{{"type":"signature_delta","signature":"{signature}"}}}}"#
                );
                state
                    .push_named(Some("content_block_delta"), &delta)
                    .expect("matching signature delta");
            }
            state
                .push_named(
                    Some("content_block_stop"),
                    r#"{"type":"content_block_stop","index":0}"#,
                )
                .unwrap();
            assert_eq!(
                state.verified_reasoning_context()[0].payload,
                ProviderContextPayload::EncryptedReasoning {
                    protocol: ApiProtocol::AnthropicMessages,
                    item: json!({"type":"thinking_signature","signature":"opaque"}),
                }
            );
        }

        let mut conflict = delta_test_state(
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":"opaque"}}"#,
            None,
        );
        let before = format!("{:?}", conflict.open);
        assert!(conflict
            .push_named(
                Some("content_block_delta"),
                r#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"replacement"}}"#,
            )
            .is_err());
        assert_eq!(format!("{:?}", conflict.open), before);
    }

    #[test]
    fn pause_turn_and_server_fallback_are_explicitly_unsupported() {
        let mut paused = AnthropicReceiveState::with_budget(
            FrozenToolSchemaRegistry::compile(&[]).unwrap(),
            ResponseBudget::default(),
            None,
            "claude",
        );
        paused
            .push_named(
                Some("message_start"),
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
            )
            .unwrap();
        let error = paused
            .push_named(
                Some("message_delta"),
                r#"{"type":"message_delta","delta":{"stop_reason":"pause_turn"},"usage":{"output_tokens":1}}"#,
            )
            .expect_err("pause_turn is outside Sumi's server-tool scope");
        assert!(
            error
                .to_string()
                .contains("unsupported stop reason pause_turn")
        );
        assert_eq!(paused.usage.output, 1);

        let mut fallback = AnthropicReceiveState::with_budget(
            FrozenToolSchemaRegistry::compile(&[]).unwrap(),
            ResponseBudget::default(),
            None,
            "claude",
        );
        fallback
            .push_named(
                Some("message_start"),
                r#"{"type":"message_start","message":{"id":"m","model":"claude","role":"assistant","content":[],"usage":{}}}"#,
            )
            .unwrap();
        let error = fallback
            .push_named(
                Some("content_block_start"),
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"fallback","from":{"model":"requested"},"to":{"model":"fallback"}}}"#,
            )
            .expect_err("server fallback remains unsupported");
        assert!(
            error
                .to_string()
                .contains("unsupported content block type fallback")
        );
    }

    // T17 release blocker: encrypted provider_context durable round-trip must
    // prove exact origin/anchor/ordinal restoration after restart.
    // T25 release blocker: live Anthropic two-turn + tool capture is not
    // substituted by this synthetic fixture.
}
