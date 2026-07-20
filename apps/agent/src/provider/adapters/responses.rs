use std::collections::{BTreeMap, HashMap};

use chrono::Utc;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::provider::{
    assembler::{
        FrozenToolSchemaRegistry, ResponseBudget, ToolArgumentAccumulator, ToolArgumentOutcome,
    },
    model::{ModelSpec, ProtocolCompat, RequestOptions, ResponsesCompat},
    types::{
        ApiProtocol, AssistantContent, ContextMessage, MemoryLayer, Message,
        NativeCompactionCoverage, PromptContext, ProviderContextAnchor, ProviderContextFragment,
        ProviderContextItem, ProviderContextPayload, ProviderEvent, StopReason, ToolDefinition,
        Usage, UserContent,
    },
};

#[derive(Debug, Error)]
pub enum ResponsesAdapterError {
    #[error("model protocol/compat variant is not OpenAI Responses")]
    UnsupportedProtocol,
    #[error("max_output_tokens must be within 1..={max}, got {requested}")]
    InvalidMaxTokens { requested: u64, max: u64 },
    #[error("invalid Responses request context: {0}")]
    InvalidContext(String),
    #[error("invalid Responses stream event: {0}")]
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
    #[error("Responses stream ended before a terminal response event")]
    MissingTerminal,
    #[error("invalid compact response: {0}")]
    InvalidCompactResponse(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct NativeCompactionResult {
    items: Vec<Value>,
    coverage: NativeCompactionCoverage,
    usage: Usage,
}

impl NativeCompactionResult {
    pub fn items(&self) -> &[Value] {
        &self.items
    }

    pub fn coverage(&self) -> &NativeCompactionCoverage {
        &self.coverage
    }

    pub fn usage(&self) -> &Usage {
        &self.usage
    }
}

#[derive(Debug)]
pub struct ResponsesTerminal {
    pub events: Vec<ProviderEvent>,
    pub reason: StopReason,
    pub usage: Usage,
    pub error_message: Option<String>,
    pub provider_code: Option<String>,
    pub provider_context: Vec<ProviderContextFragment>,
}

#[derive(Debug, Default)]
pub struct ResponsesPush {
    pub events: Vec<ProviderEvent>,
    pub terminal: Option<ResponsesTerminal>,
}

pub(crate) fn validate_event_name(
    event_name: Option<&str>,
    payload: &str,
) -> Result<(), ResponsesAdapterError> {
    let Some(event_name) = event_name else {
        return Ok(());
    };
    let value: Value = serde_json::from_str(payload)
        .map_err(|error| ResponsesAdapterError::InvalidEvent(error.to_string()))?;
    let payload_type = value
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| ResponsesAdapterError::InvalidEvent("type must be a string".into()))?;
    if event_name != payload_type {
        return Err(ResponsesAdapterError::InvalidEvent(format!(
            "SSE event name {event_name} does not match payload type {payload_type}"
        )));
    }
    Ok(())
}

pub fn requested_output_tokens(
    spec: &ModelSpec,
    options: &RequestOptions,
) -> Result<u64, ResponsesAdapterError> {
    ensure_responses_spec(spec)?;
    let requested = options.max_tokens.unwrap_or(spec.default_output_tokens);
    if requested == 0 || requested > spec.max_output_tokens {
        return Err(ResponsesAdapterError::InvalidMaxTokens {
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
) -> Result<Value, ResponsesAdapterError> {
    let compat = ensure_responses_spec(spec)?;
    if !compat.supports_streaming {
        return Err(ResponsesAdapterError::UnsupportedProtocol);
    }
    let mut request = Map::new();
    request.insert("model".to_owned(), json!(spec.id));
    request.insert("instructions".to_owned(), json!(context.system_prompt));
    request.insert(
        "input".to_owned(),
        Value::Array(convert_input(spec, context)?),
    );
    request.insert("stream".to_owned(), json!(true));
    request.insert(
        "max_output_tokens".to_owned(),
        json!(requested_output_tokens(spec, options)?),
    );
    if compat.supports_store {
        request.insert("store".to_owned(), json!(false));
    }
    if let Some(temperature) = options.temperature {
        request.insert("temperature".to_owned(), json!(temperature));
    }
    if let Some(tool_choice) = &options.tool_choice {
        request.insert("tool_choice".to_owned(), tool_choice.clone());
    }
    if !context.tools.is_empty() {
        request.insert(
            "tools".to_owned(),
            Value::Array(context.tools.iter().map(convert_tool).collect()),
        );
    }
    if spec.reasoning {
        let mut reasoning = Map::new();
        if let Some(effort) = &options.reasoning_effort {
            reasoning.insert("effort".to_owned(), json!(effort));
        }
        reasoning.insert("summary".to_owned(), json!("auto"));
        request.insert("reasoning".to_owned(), Value::Object(reasoning));
        if compat.supports_encrypted_reasoning {
            request.insert("include".to_owned(), json!(["reasoning.encrypted_content"]));
        }
    }
    Ok(Value::Object(request))
}

pub fn build_compact_request(
    spec: &ModelSpec,
    context: &PromptContext,
) -> Result<Value, ResponsesAdapterError> {
    let compat = ensure_responses_spec(spec)?;
    if !compat.supports_native_compact {
        return Err(ResponsesAdapterError::UnsupportedProtocol);
    }
    let mut request = Map::new();
    request.insert("model".into(), json!(spec.id));
    request.insert("instructions".into(), json!(context.system_prompt));
    request.insert("input".into(), Value::Array(convert_input(spec, context)?));
    if compat.supports_store {
        request.insert("store".into(), json!(false));
    }
    Ok(Value::Object(request))
}

pub(in crate::provider) fn derive_compaction_coverage(
    spec: &ModelSpec,
    context: &PromptContext,
) -> Result<NativeCompactionCoverage, ResponsesAdapterError> {
    let compat = ensure_responses_spec(spec)?;
    if !compat.supports_native_compact {
        return Err(ResponsesAdapterError::UnsupportedProtocol);
    }
    let mut previous: Option<u64> = None;
    let mut persisted_started = false;
    for message in &context.messages {
        let ContextMessage::Persisted { seq, .. } = message else {
            if persisted_started {
                return Err(ResponsesAdapterError::InvalidContext(
                    "native compaction requires persisted messages to form a trailing suffix"
                        .into(),
                ));
            }
            continue;
        };
        persisted_started = true;
        if *seq == 0 {
            return Err(ResponsesAdapterError::InvalidContext(
                "persisted message sequence must be greater than zero".into(),
            ));
        }
        if let Some(previous) = previous
            && previous.checked_add(1) != Some(*seq)
        {
            return Err(ResponsesAdapterError::InvalidContext(
                "persisted message sequence is duplicated, nonmonotonic, or gapped".into(),
            ));
        }
        previous = Some(*seq);
    }
    let through_message_seq = previous.ok_or_else(|| {
        ResponsesAdapterError::InvalidContext(
            "native compaction requires at least one persisted message".into(),
        )
    })?;
    let context_fingerprint = context_fingerprint(spec, context)?;
    Ok(NativeCompactionCoverage {
        through_message_seq,
        context_fingerprint,
    })
}

fn context_fingerprint(
    spec: &ModelSpec,
    context: &PromptContext,
) -> Result<String, ResponsesAdapterError> {
    ensure_responses_spec(spec)?;
    let tools = serde_json::to_vec(&context.tools)
        .map_err(|error| ResponsesAdapterError::InvalidContext(error.to_string()))?;
    let mut hasher = Sha256::new();
    for bytes in [
        spec.provider_instance_id().as_bytes(),
        b"open_ai_responses",
        spec.id.as_bytes(),
        context.system_prompt.as_bytes(),
        tools.as_slice(),
        b"", // Responses currently has no request beta header.
    ] {
        hasher.update(bytes.len().to_be_bytes());
        hasher.update(bytes);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

pub(in crate::provider) fn parse_compact_response(
    value: Value,
    coverage: NativeCompactionCoverage,
) -> Result<NativeCompactionResult, ResponsesAdapterError> {
    let object = value.as_object().ok_or_else(|| {
        ResponsesAdapterError::InvalidCompactResponse("root is not an object".into())
    })?;
    if object.get("object").and_then(Value::as_str) != Some("response.compaction") {
        return Err(ResponsesAdapterError::InvalidCompactResponse(
            "object must be response.compaction".into(),
        ));
    }
    let output = object
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ResponsesAdapterError::InvalidCompactResponse("output must be an array".into())
        })?;
    let mut items = Vec::with_capacity(output.len());
    for item in output {
        validate_canonical_item(item).map_err(ResponsesAdapterError::InvalidCompactResponse)?;
        items.push(item.clone());
    }
    if items.is_empty() {
        return Err(ResponsesAdapterError::InvalidCompactResponse(
            "output must not be empty".into(),
        ));
    }
    let usage = object
        .get("usage")
        .map(parse_usage)
        .transpose()?
        .unwrap_or_default();
    Ok(NativeCompactionResult {
        items,
        coverage,
        usage,
    })
}

fn ensure_responses_spec(spec: &ModelSpec) -> Result<&ResponsesCompat, ResponsesAdapterError> {
    match (&spec.protocol, &spec.compat) {
        (ApiProtocol::OpenAiResponses, ProtocolCompat::Responses(compat)) => Ok(compat),
        _ => Err(ResponsesAdapterError::UnsupportedProtocol),
    }
}

fn convert_tool(tool: &ToolDefinition) -> Value {
    json!({
        "type": "function",
        "name": tool.name,
        "description": tool.description,
        "parameters": tool.parameters,
        "strict": true,
    })
}

fn convert_input(
    spec: &ModelSpec,
    context: &PromptContext,
) -> Result<Vec<Value>, ResponsesAdapterError> {
    let mut output = Vec::new();
    let compacted = context
        .provider_context
        .iter()
        .filter_map(|item| match &item.payload {
            ProviderContextPayload::OpenAiCompactedWindow { items, coverage } => {
                Some((items, coverage))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if compacted.len() > 1 {
        return Err(ResponsesAdapterError::InvalidContext(
            "multiple OpenAI native compacted windows".into(),
        ));
    }
    let (coverage_seq, mut native_items) = if let Some((items, coverage)) = compacted.first() {
        if !context.memory_blocks.is_empty() {
            return Err(ResponsesAdapterError::InvalidContext(
                "native compacted window cannot coexist with memory blocks".into(),
            ));
        }
        if coverage.context_fingerprint != context_fingerprint(spec, context)? {
            return Err(ResponsesAdapterError::InvalidContext(
                "native compacted window context fingerprint mismatch".into(),
            ));
        }
        if items.is_empty() {
            return Err(ResponsesAdapterError::InvalidContext(
                "native compacted window must not be empty".into(),
            ));
        }
        let mut validated = Vec::with_capacity(items.len());
        for item in *items {
            validate_canonical_item(item).map_err(|error| {
                ResponsesAdapterError::InvalidContext(format!(
                    "invalid native compacted window item: {error}"
                ))
            })?;
            validated.push(item.clone());
        }
        (Some(coverage.through_message_seq), Some(validated))
    } else {
        (None, None)
    };
    for memory in &context.memory_blocks {
        let layer = match memory.layer {
            MemoryLayer::L1 => "l1",
            MemoryLayer::L2 => "l2",
        };
        output.push(json!({
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": format!(
                    "<memory layer=\"{layer}\">{}</memory>",
                    escape_memory_text(&memory.text)
                ),
            }],
        }));
    }

    let mut context_by_anchor: BTreeMap<(String, u64), Vec<&ProviderContextItem>> = BTreeMap::new();
    for item in &context.provider_context {
        match &item.payload {
            ProviderContextPayload::OpenAiCompactedWindow { .. } => {}
            ProviderContextPayload::AnthropicCompaction { .. } => {
                return Err(ResponsesAdapterError::InvalidContext(
                    "Anthropic provider context cannot be sent to Responses".into(),
                ));
            }
            ProviderContextPayload::EncryptedReasoning { .. } => {
                let anchor = item.origin_message.as_ref().ok_or_else(|| {
                    ResponsesAdapterError::InvalidContext(
                        "encrypted reasoning is missing an origin anchor".into(),
                    )
                })?;
                if coverage_seq.is_some_and(|coverage| anchor.message_seq <= coverage) {
                    continue;
                }
                context_by_anchor
                    .entry((anchor.message_id.clone(), anchor.message_seq))
                    .or_default()
                    .push(item);
            }
        }
    }

    let mut previous_suffix_seq = None;
    let mut suffix_started = false;
    let mut persisted_started = false;
    for message in &context.messages {
        let (anchor, message) = match message {
            ContextMessage::Persisted { id, seq, message } => {
                if !persisted_started {
                    persisted_started = true;
                    if let Some(items) = native_items.take() {
                        output.extend(items);
                    }
                }
                if coverage_seq.is_some_and(|coverage| *seq <= coverage) {
                    if suffix_started {
                        return Err(ResponsesAdapterError::InvalidContext(
                            "native compacted window covered history appears after its suffix"
                                .into(),
                        ));
                    }
                    continue;
                }
                if coverage_seq.is_some()
                    && previous_suffix_seq.is_some_and(|previous| *seq <= previous)
                {
                    return Err(ResponsesAdapterError::InvalidContext(
                        "native compacted window suffix sequence is duplicated or reordered".into(),
                    ));
                }
                suffix_started = coverage_seq.is_some();
                previous_suffix_seq = Some(*seq);
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
                    return Err(ResponsesAdapterError::InvalidContext(
                        "native compacted window suffix requires persisted message sequence numbers"
                            .into(),
                    ));
                }
                (None, message)
            }
        };
        match message {
            Message::User(user) => {
                let content = user
                    .content
                    .iter()
                    .map(|content| match content {
                        UserContent::Text { text } => json!({"type":"input_text","text":text}),
                        UserContent::Image { data, mime_type } if spec.supports_images => json!({
                            "type":"input_image",
                            "detail":"auto",
                            "image_url":format!("data:{mime_type};base64,{data}"),
                        }),
                        UserContent::Image { .. } => {
                            json!({"type":"input_text","text":"(image omitted: model does not support image input)"})
                        }
                    })
                    .collect::<Vec<_>>();
                if !content.is_empty() {
                    output.push(json!({"type":"message","role":"user","content":content}));
                }
            }
            Message::Assistant(assistant) => {
                let same_origin = assistant.origin == spec.origin();
                let mut opaque = BTreeMap::<(u32, u32), Value>::new();
                if let Some(anchor) = &anchor
                    && let Some(items) =
                        context_by_anchor.remove(&(anchor.message_id.clone(), anchor.message_seq))
                {
                    // T25 invalidation boundary: opaque continuation state is useful only
                    // at the exact provider origin. Cross-origin state is omitted from the
                    // send view alongside raw Thinking, while public transcript content
                    // remains replayable.
                    for item in items.into_iter().filter(|_| same_origin) {
                        let wire = item.wire_item_index.ok_or_else(|| {
                            ResponsesAdapterError::InvalidContext(
                                "encrypted reasoning is missing wire_item_index".into(),
                            )
                        })?;
                        let ProviderContextPayload::EncryptedReasoning {
                            protocol,
                            item: payload,
                        } = &item.payload
                        else {
                            unreachable!("provider context filtered above")
                        };
                        if *protocol != ApiProtocol::OpenAiResponses {
                            return Err(ResponsesAdapterError::InvalidContext(
                                "same-origin encrypted reasoning protocol mismatch".into(),
                            ));
                        }
                        validate_canonical_item(payload).map_err(|error| {
                            ResponsesAdapterError::InvalidContext(format!(
                                "invalid encrypted reasoning item: {error}"
                            ))
                        })?;
                        if payload.get("type").and_then(Value::as_str) != Some("reasoning")
                            || payload
                                .get("encrypted_content")
                                .and_then(Value::as_str)
                                .is_none_or(str::is_empty)
                        {
                            return Err(ResponsesAdapterError::InvalidContext(
                                "encrypted reasoning payload must be a reasoning item with encrypted_content"
                                    .into(),
                            ));
                        }
                        if opaque
                            .insert((wire, item.ordinal), payload.clone())
                            .is_some()
                        {
                            return Err(ResponsesAdapterError::InvalidContext(
                                "duplicate encrypted reasoning placement".into(),
                            ));
                        }
                    }
                }

                let mut public = assistant.content.iter().collect::<Vec<_>>();
                public.sort_by_key(|content| match content {
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
                });
                let mut opaque_iter = opaque.into_iter().peekable();
                for content in public {
                    let wire = match content {
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
                    };
                    while opaque_iter
                        .peek()
                        .is_some_and(|((index, _), _)| *index <= wire)
                    {
                        let (_, item) = opaque_iter.next().expect("peeked item");
                        output.push(item);
                    }
                    match content {
                        AssistantContent::Text {
                            text,
                            wire_item_index,
                        } => output.push(json!({
                            "type":"message",
                            "id":format!("msg_sumi_{}_{}", anchor.as_ref().map_or(0, |a| a.message_seq), wire_item_index),
                            "status":"completed",
                            "role":"assistant",
                            "content":[{"type":"output_text","text":text,"annotations":[]}],
                        })),
                        AssistantContent::ToolCall { tool_call, .. } => output.push(json!({
                            "type":"function_call",
                            "call_id":tool_call.id,
                            "name":tool_call.name,
                            "arguments":Value::Object(tool_call.arguments.as_object().clone()).to_string(),
                        })),
                        AssistantContent::Thinking { .. } => {}
                        AssistantContent::RejectedToolCall { .. } => {
                            return Err(ResponsesAdapterError::InvalidContext(
                                "rejected tool calls must be normalized before Responses conversion".into(),
                            ));
                        }
                    }
                }
                output.extend(opaque_iter.map(|(_, item)| item));
            }
            Message::ToolResult(result) => {
                let text = result
                    .content
                    .iter()
                    .filter_map(|content| match content {
                        UserContent::Text { text } => Some(text.as_str()),
                        UserContent::Image { .. } => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                let has_images = result
                    .content
                    .iter()
                    .any(|content| matches!(content, UserContent::Image { .. }));
                let output_text = if !text.is_empty() {
                    text
                } else if has_images {
                    "(see attached image)".to_owned()
                } else {
                    "(no tool output)".to_owned()
                };
                output.push(json!({
                    "type":"function_call_output",
                    "call_id":result.tool_call_id,
                    "output":output_text,
                }));
            }
        }
    }
    if native_items.is_some() {
        return Err(ResponsesAdapterError::InvalidContext(
            "native compacted window requires a persisted message suffix".into(),
        ));
    }
    if !context_by_anchor.is_empty() {
        return Err(ResponsesAdapterError::InvalidContext(
            "provider context anchor was not found in L0".into(),
        ));
    }
    Ok(output)
}

fn escape_memory_text(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[derive(Debug)]
enum OutputSlot {
    Text {
        id: String,
        content: String,
        next_content_index: u32,
        active_part: Option<TextPart>,
    },
    Tool {
        id: String,
        call_id: String,
        name: String,
        accumulator: ToolArgumentAccumulator,
        done: bool,
    },
    Reasoning {
        id: String,
        summary_slot: usize,
        summary: String,
        started: bool,
        next_summary_index: u32,
        active_part: Option<SummaryPart>,
    },
}

#[derive(Debug)]
struct TextPart {
    index: u32,
    kind: String,
    start_len: usize,
    done: bool,
}

#[derive(Debug)]
struct SummaryPart {
    index: u32,
    start_len: usize,
    done: bool,
}

#[derive(Debug)]
pub struct ResponsesReceiveState {
    schemas: FrozenToolSchemaRegistry,
    slots: BTreeMap<u32, OutputSlot>,
    output_identities: BTreeMap<u32, (String, String)>,
    completed_items: BTreeMap<u32, Value>,
    next_output_index: u32,
    next_summary_slot: usize,
    next_sequence_number: u64,
    reasoning_fragments: Vec<(String, ProviderContextFragment)>,
    usage: Usage,
    budget: ResponseBudget,
    content_bytes: usize,
    event_count: usize,
    preview_work_bytes: usize,
    tool_count: usize,
    response_id: Option<String>,
    response_model: Option<String>,
    saw_tool: bool,
    terminal: bool,
}

impl ResponsesReceiveState {
    pub fn with_budget(schemas: FrozenToolSchemaRegistry, budget: ResponseBudget) -> Self {
        Self {
            schemas,
            slots: BTreeMap::new(),
            output_identities: BTreeMap::new(),
            completed_items: BTreeMap::new(),
            next_output_index: 0,
            next_summary_slot: 0,
            next_sequence_number: 0,
            reasoning_fragments: Vec::new(),
            usage: Usage::default(),
            budget,
            content_bytes: 0,
            event_count: 0,
            preview_work_bytes: 0,
            tool_count: 0,
            response_id: None,
            response_model: None,
            saw_tool: false,
            terminal: false,
        }
    }

    pub fn usage(&self) -> &Usage {
        &self.usage
    }

    pub fn provider_context(&self) -> Vec<ProviderContextFragment> {
        self.reasoning_fragments
            .iter()
            .map(|(_, fragment)| fragment.clone())
            .collect()
    }

    pub fn push_json(&mut self, payload: &str) -> Result<ResponsesPush, ResponsesAdapterError> {
        let value: Value = serde_json::from_str(payload)
            .map_err(|error| ResponsesAdapterError::InvalidEvent(error.to_string()))?;
        let object = value
            .as_object()
            .ok_or_else(|| ResponsesAdapterError::InvalidEvent("event is not an object".into()))?;
        let event_type = required_str(object, "type")?;
        if self.terminal {
            return Err(ResponsesAdapterError::InvalidEvent(
                "event arrived after terminal response".into(),
            ));
        }
        let sequence_number = object
            .get("sequence_number")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                ResponsesAdapterError::InvalidEvent(
                    "sequence_number must be a non-negative integer".into(),
                )
            })?;
        if sequence_number != self.next_sequence_number {
            return Err(ResponsesAdapterError::InvalidEvent(
                "sequence_number is missing, duplicated, or reordered".into(),
            ));
        }
        let next_sequence_number = self.next_sequence_number.checked_add(1).ok_or_else(|| {
            ResponsesAdapterError::InvalidEvent("sequence_number exceeds u64".into())
        })?;

        let result = match event_type {
            "response.created" | "response.in_progress" | "response.queued" => {
                self.observe_response_identity(object)?;
                Ok(ResponsesPush::default())
            }
            "response.output_item.added" => self.output_item_added(object),
            "response.output_text.delta" | "response.refusal.delta" => self.text_delta(object),
            "response.output_text.done" | "response.refusal.done" => self.text_done(object),
            "response.content_part.added" => self.content_part_added(object),
            "response.content_part.done" => self.content_part_done(object),
            "response.function_call_arguments.delta" => self.tool_delta(object),
            "response.function_call_arguments.done" => self.tool_arguments_done(object),
            "response.reasoning_summary_text.delta" => self.summary_delta(object),
            "response.reasoning_summary_part.added" => self.summary_part_added(object),
            "response.reasoning_summary_text.done" => self.summary_text_done(object),
            "response.reasoning_summary_part.done" => self.summary_part_done(object),
            "response.output_item.done" => self.output_item_done(object),
            "response.completed" | "response.incomplete" => self.response_terminal(object),
            "response.failed" => self.response_failed(object),
            "error" => Err(ResponsesAdapterError::Provider {
                code: object
                    .get("code")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                message: object
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("provider returned an unknown stream error")
                    .to_owned(),
            }),
            unknown => {
                tracing::debug!(event_type = unknown, "ignored unknown Responses event");
                Ok(ResponsesPush::default())
            }
        };
        if result.is_ok() {
            self.next_sequence_number = next_sequence_number;
        }
        result
    }

    pub fn finish_eof(&self) -> Result<(), ResponsesAdapterError> {
        if self.terminal {
            Ok(())
        } else {
            Err(ResponsesAdapterError::MissingTerminal)
        }
    }

    pub fn fail(&mut self) -> Vec<ProviderEvent> {
        let mut events = Vec::new();
        for (index, slot) in std::mem::take(&mut self.slots) {
            match slot {
                OutputSlot::Text { content, .. } => events.push(ProviderEvent::TextEnd {
                    content_index: index as usize,
                    content,
                }),
                OutputSlot::Reasoning {
                    summary_slot,
                    summary,
                    started: true,
                    ..
                } => events.push(ProviderEvent::ReasoningSummaryEnd {
                    content_index: summary_slot,
                    content: summary,
                }),
                OutputSlot::Reasoning { .. } => {}
                OutputSlot::Tool {
                    call_id,
                    name,
                    accumulator,
                    ..
                } => match accumulator.reject_incomplete(call_id, name, Utc::now()) {
                    ToolArgumentOutcome::Validated(_) => {
                        unreachable!("reject_incomplete never validates")
                    }
                    ToolArgumentOutcome::Rejected {
                        rejected,
                        synthetic_result,
                    } => events.push(ProviderEvent::ToolCallRejected {
                        content_index: index as usize,
                        rejected,
                        synthetic_result,
                    }),
                },
            }
        }
        events
    }

    fn output_item_added(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ResponsesPush, ResponsesAdapterError> {
        let index = output_index(object)?;
        if index != self.next_output_index {
            return Err(ResponsesAdapterError::InvalidEvent(
                "response output index is missing, duplicated, or reordered".into(),
            ));
        }
        let item = object
            .get("item")
            .and_then(Value::as_object)
            .ok_or_else(|| ResponsesAdapterError::InvalidEvent("item must be an object".into()))?;
        let item_type = required_str(item, "type")?;
        let mut events = Vec::new();
        let mut event_add = 0;
        let mut is_tool = false;
        let mut next_summary_slot = None;
        let slot = match item_type {
            "message" => {
                ensure_assistant_message_item(item)?;
                event_add = 1;
                events.push(ProviderEvent::TextStart {
                    content_index: index as usize,
                });
                OutputSlot::Text {
                    id: required_str(item, "id")?.to_owned(),
                    content: String::new(),
                    next_content_index: 0,
                    active_part: None,
                }
            }
            "function_call" => {
                event_add = 1;
                is_tool = true;
                events.push(ProviderEvent::ToolCallStart {
                    content_index: index as usize,
                });
                let accumulator = ToolArgumentAccumulator::new();
                let arguments = string_field(item, "arguments")?;
                if !arguments.is_empty() {
                    return Err(ResponsesAdapterError::InvalidEvent(
                        "function_call added item must begin with empty arguments".into(),
                    ));
                }
                OutputSlot::Tool {
                    id: required_str(item, "id")?.to_owned(),
                    call_id: required_str(item, "call_id")?.to_owned(),
                    name: required_str(item, "name")?.to_owned(),
                    accumulator,
                    done: false,
                }
            }
            "reasoning" => {
                if !reasoning_summary_text(item)?.is_empty() {
                    return Err(ResponsesAdapterError::InvalidEvent(
                        "reasoning added item must begin with an empty summary".into(),
                    ));
                }
                let summary_slot = self.next_summary_slot;
                next_summary_slot = Some(summary_slot.checked_add(1).ok_or_else(|| {
                    ResponsesAdapterError::InvalidEvent("summary slot exceeds usize".into())
                })?);
                OutputSlot::Reasoning {
                    id: required_str(item, "id")?.to_owned(),
                    summary_slot,
                    summary: String::new(),
                    started: false,
                    next_summary_index: 0,
                    active_part: None,
                }
            }
            other => {
                return Err(ResponsesAdapterError::InvalidEvent(format!(
                    "unsupported known output item variant {other}"
                )));
            }
        };
        let tool_count = checked_counter(
            self.tool_count,
            usize::from(is_tool),
            self.budget.max_tool_calls,
            "tool_calls",
        )?;
        self.commit_charges(item_identity_bytes(item)?, event_add, 0)?;
        if let Some(next_summary_slot) = next_summary_slot {
            self.next_summary_slot = next_summary_slot;
        }
        self.tool_count = tool_count;
        self.saw_tool |= is_tool;
        self.output_identities.insert(
            index,
            (required_str(item, "id")?.to_owned(), item_type.to_owned()),
        );
        self.slots.insert(index, slot);
        self.next_output_index = self.next_output_index.checked_add(1).ok_or_else(|| {
            ResponsesAdapterError::InvalidEvent("output index exceeds u32".into())
        })?;
        Ok(ResponsesPush {
            events,
            terminal: None,
        })
    }

    fn text_delta(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ResponsesPush, ResponsesAdapterError> {
        let index = output_index(object)?;
        let item_id = required_str(object, "item_id")?;
        let content_index = nested_index(object, "content_index")?;
        let delta = required_str(object, "delta")?;
        if !matches!(
            self.slots.get(&index),
            Some(OutputSlot::Text {
                id,
                active_part: Some(part),
                ..
            }) if id == item_id && part.index == content_index && !part.done
        ) {
            return Err(ResponsesAdapterError::InvalidEvent(
                "text delta identity/index does not match an active content part".into(),
            ));
        }
        self.commit_charges(delta.len(), 1, 0)?;
        let Some(OutputSlot::Text { content, .. }) = self.slots.get_mut(&index) else {
            unreachable!("text slot validated above")
        };
        content.push_str(delta);
        Ok(ResponsesPush {
            events: vec![ProviderEvent::TextDelta {
                content_index: index as usize,
                delta: delta.to_owned(),
            }],
            terminal: None,
        })
    }

    fn content_part_added(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ResponsesPush, ResponsesAdapterError> {
        let index = output_index(object)?;
        let item_id = required_str(object, "item_id")?;
        let content_index = nested_index(object, "content_index")?;
        let part = object
            .get("part")
            .and_then(Value::as_object)
            .ok_or_else(|| ResponsesAdapterError::InvalidEvent("part must be an object".into()))?;
        let kind = required_str(part, "type")?;
        let empty = match kind {
            "output_text" => string_field(part, "text")?.is_empty(),
            "refusal" => string_field(part, "refusal")?.is_empty(),
            other => {
                return Err(ResponsesAdapterError::InvalidEvent(format!(
                    "unsupported known content part variant {other}"
                )));
            }
        };
        let Some(OutputSlot::Text {
            id,
            content,
            next_content_index,
            active_part,
            ..
        }) = self.slots.get_mut(&index)
        else {
            return Err(ResponsesAdapterError::InvalidEvent(
                "content part has no matching message item".into(),
            ));
        };
        if id != item_id || *next_content_index != content_index || active_part.is_some() || !empty
        {
            return Err(ResponsesAdapterError::InvalidEvent(
                "content part identity/index is missing, duplicated, or reordered".into(),
            ));
        }
        *active_part = Some(TextPart {
            index: content_index,
            kind: kind.to_owned(),
            start_len: content.len(),
            done: false,
        });
        Ok(ResponsesPush::default())
    }

    fn text_done(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ResponsesPush, ResponsesAdapterError> {
        let index = output_index(object)?;
        let item_id = required_str(object, "item_id")?;
        let content_index = nested_index(object, "content_index")?;
        let event_type = required_str(object, "type")?;
        let final_text = if event_type == "response.refusal.done" {
            string_field(object, "refusal").or_else(|_| string_field(object, "text"))?
        } else {
            string_field(object, "text")?
        };
        let Some(OutputSlot::Text {
            id,
            content,
            active_part: Some(part),
            ..
        }) = self.slots.get_mut(&index)
        else {
            return Err(ResponsesAdapterError::InvalidEvent(
                "text done has no active content part".into(),
            ));
        };
        let expected_kind = if event_type == "response.refusal.done" {
            "refusal"
        } else {
            "output_text"
        };
        if id != item_id
            || part.index != content_index
            || part.kind != expected_kind
            || part.done
            || content.get(part.start_len..) != Some(final_text)
        {
            return Err(ResponsesAdapterError::InvalidEvent(
                "text done does not match streamed content identity/order".into(),
            ));
        }
        part.done = true;
        Ok(ResponsesPush::default())
    }

    fn content_part_done(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ResponsesPush, ResponsesAdapterError> {
        let index = output_index(object)?;
        let item_id = required_str(object, "item_id")?;
        let content_index = nested_index(object, "content_index")?;
        let part = object
            .get("part")
            .and_then(Value::as_object)
            .ok_or_else(|| ResponsesAdapterError::InvalidEvent("part must be an object".into()))?;
        let kind = required_str(part, "type")?;
        let final_text = match kind {
            "output_text" => string_field(part, "text")?,
            "refusal" => string_field(part, "refusal")?,
            other => {
                return Err(ResponsesAdapterError::InvalidEvent(format!(
                    "unsupported known content part variant {other}"
                )));
            }
        };
        let Some(OutputSlot::Text {
            id,
            content,
            next_content_index,
            active_part,
        }) = self.slots.get_mut(&index)
        else {
            return Err(ResponsesAdapterError::InvalidEvent(
                "content part done has no matching message item".into(),
            ));
        };
        if id != item_id
            || !matches!(active_part, Some(active) if active.index == content_index
                && active.kind == kind
                && active.done
                && content.get(active.start_len..) == Some(final_text))
        {
            return Err(ResponsesAdapterError::InvalidEvent(
                "content part done does not match streamed content identity/order".into(),
            ));
        }
        *active_part = None;
        *next_content_index = next_content_index.checked_add(1).ok_or_else(|| {
            ResponsesAdapterError::InvalidEvent("content index exceeds u32".into())
        })?;
        Ok(ResponsesPush::default())
    }

    fn tool_delta(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ResponsesPush, ResponsesAdapterError> {
        let index = output_index(object)?;
        self.validate_item_id(index, object, "function_call")?;
        let delta = required_str(object, "delta")?;
        let current_len = match self.slots.get(&index) {
            Some(OutputSlot::Tool { accumulator, .. }) => accumulator.raw_len(),
            _ => {
                return Err(ResponsesAdapterError::InvalidEvent(
                    "tool delta has no matching function_call item".into(),
                ));
            }
        };
        let next_len = current_len.checked_add(delta.len()).ok_or(
            ResponsesAdapterError::ResponseLimitExceeded {
                resource: "preview_parse_work",
                limit: self.budget.max_preview_work_bytes,
            },
        )?;
        self.commit_charges(delta.len(), 2, next_len)?;
        let Some(OutputSlot::Tool { accumulator, .. }) = self.slots.get_mut(&index) else {
            unreachable!("tool slot validated above")
        };
        let preview = accumulator.append(delta);
        Ok(ResponsesPush {
            events: vec![
                ProviderEvent::ToolCallDelta {
                    content_index: index as usize,
                    delta: delta.to_owned(),
                },
                ProviderEvent::ToolCallPreview {
                    content_index: index as usize,
                    preview,
                },
            ],
            terminal: None,
        })
    }

    fn tool_arguments_done(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ResponsesPush, ResponsesAdapterError> {
        let index = output_index(object)?;
        self.validate_item_id(index, object, "function_call")?;
        let arguments = string_field(object, "arguments")?;
        let (current, prefix_matches) = match self.slots.get(&index) {
            Some(OutputSlot::Tool { accumulator, .. }) => {
                (accumulator.raw_len(), accumulator.is_prefix_of(arguments))
            }
            _ => {
                return Err(ResponsesAdapterError::InvalidEvent(
                    "arguments.done has no matching function_call item".into(),
                ));
            }
        };
        if !prefix_matches {
            return Err(ResponsesAdapterError::InvalidEvent(
                "arguments.done does not extend streamed arguments".into(),
            ));
        }
        let suffix = &arguments[current..];
        if suffix.is_empty() {
            return Ok(ResponsesPush::default());
        }
        let mut synthetic = object.clone();
        synthetic.insert("delta".to_owned(), json!(suffix));
        self.tool_delta(&synthetic)
    }

    fn summary_delta(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ResponsesPush, ResponsesAdapterError> {
        let index = output_index(object)?;
        self.validate_item_id(index, object, "reasoning")?;
        let summary_index = nested_index(object, "summary_index")?;
        let delta = required_str(object, "delta")?;
        let start = match self.slots.get(&index) {
            Some(OutputSlot::Reasoning {
                started,
                active_part: Some(part),
                ..
            }) if part.index == summary_index && !part.done => !started,
            _ => {
                return Err(ResponsesAdapterError::InvalidEvent(
                    "reasoning summary delta identity/index does not match an active summary part"
                        .into(),
                ));
            }
        };
        self.commit_charges(delta.len(), 1 + usize::from(start), 0)?;
        let Some(OutputSlot::Reasoning {
            summary,
            started,
            summary_slot,
            ..
        }) = self.slots.get_mut(&index)
        else {
            unreachable!("reasoning slot validated above")
        };
        *started = true;
        summary.push_str(delta);
        let mut events = Vec::with_capacity(2);
        if start {
            events.push(ProviderEvent::ReasoningSummaryStart {
                content_index: *summary_slot,
            });
        }
        events.push(ProviderEvent::ReasoningSummaryDelta {
            content_index: *summary_slot,
            delta: delta.to_owned(),
        });
        Ok(ResponsesPush {
            events,
            terminal: None,
        })
    }

    fn summary_part_added(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ResponsesPush, ResponsesAdapterError> {
        validate_reasoning_summary_event(object)?;
        let index = output_index(object)?;
        self.validate_item_id(index, object, "reasoning")?;
        let summary_index = nested_index(object, "summary_index")?;
        let (next_summary_index, has_active, needs_separator, started) =
            match self.slots.get(&index) {
                Some(OutputSlot::Reasoning {
                    next_summary_index,
                    active_part,
                    summary,
                    started,
                    ..
                }) => (
                    *next_summary_index,
                    active_part.is_some(),
                    !summary.is_empty(),
                    *started,
                ),
                _ => {
                    return Err(ResponsesAdapterError::InvalidEvent(
                        "reasoning summary part has no matching reasoning item".into(),
                    ));
                }
            };
        if next_summary_index != summary_index || has_active {
            return Err(ResponsesAdapterError::InvalidEvent(
                "summary index is missing, duplicated, or reordered".into(),
            ));
        }
        let event_add = usize::from(needs_separator) + usize::from(needs_separator && !started);
        self.commit_charges(2 * usize::from(needs_separator), event_add, 0)?;
        let Some(OutputSlot::Reasoning {
            active_part,
            summary,
            started,
            summary_slot,
            ..
        }) = self.slots.get_mut(&index)
        else {
            unreachable!("reasoning slot validated above")
        };
        *active_part = Some(SummaryPart {
            index: summary_index,
            start_len: summary.len() + 2 * usize::from(needs_separator),
            done: false,
        });
        let mut events = Vec::new();
        if needs_separator {
            if !*started {
                *started = true;
                events.push(ProviderEvent::ReasoningSummaryStart {
                    content_index: *summary_slot,
                });
            }
            summary.push_str("\n\n");
            events.push(ProviderEvent::ReasoningSummaryDelta {
                content_index: *summary_slot,
                delta: "\n\n".into(),
            });
        }
        Ok(ResponsesPush {
            events,
            terminal: None,
        })
    }

    fn summary_text_done(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ResponsesPush, ResponsesAdapterError> {
        validate_reasoning_summary_event(object)?;
        let index = output_index(object)?;
        self.validate_item_id(index, object, "reasoning")?;
        let summary_index = nested_index(object, "summary_index")?;
        let text = string_field(object, "text")?;
        let Some(OutputSlot::Reasoning {
            summary,
            active_part: Some(part),
            ..
        }) = self.slots.get_mut(&index)
        else {
            return Err(ResponsesAdapterError::InvalidEvent(
                "summary text done has no active summary part".into(),
            ));
        };
        if part.index != summary_index || part.done || summary.get(part.start_len..) != Some(text) {
            return Err(ResponsesAdapterError::InvalidEvent(
                "summary text done does not match streamed summary identity/order".into(),
            ));
        }
        part.done = true;
        Ok(ResponsesPush::default())
    }

    fn summary_part_done(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ResponsesPush, ResponsesAdapterError> {
        validate_reasoning_summary_event(object)?;
        let index = output_index(object)?;
        self.validate_item_id(index, object, "reasoning")?;
        let summary_index = nested_index(object, "summary_index")?;
        let part_text = object
            .get("part")
            .and_then(Value::as_object)
            .map(|part| string_field(part, "text"))
            .transpose()?
            .unwrap_or("");
        let Some(OutputSlot::Reasoning {
            summary,
            next_summary_index,
            active_part,
            ..
        }) = self.slots.get_mut(&index)
        else {
            return Err(ResponsesAdapterError::InvalidEvent(
                "summary part done has no matching reasoning item".into(),
            ));
        };
        if !matches!(active_part, Some(part) if part.index == summary_index
            && part.done
            && summary.get(part.start_len..) == Some(part_text))
        {
            return Err(ResponsesAdapterError::InvalidEvent(
                "summary part done identity/index is missing or reordered".into(),
            ));
        }
        *active_part = None;
        *next_summary_index = next_summary_index.checked_add(1).ok_or_else(|| {
            ResponsesAdapterError::InvalidEvent("summary index exceeds u32".into())
        })?;
        Ok(ResponsesPush::default())
    }

    fn output_item_done(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ResponsesPush, ResponsesAdapterError> {
        let index = output_index(object)?;
        let item = object
            .get("item")
            .and_then(Value::as_object)
            .ok_or_else(|| ResponsesAdapterError::InvalidEvent("item must be an object".into()))?;
        let item_type = required_str(item, "type")?;
        match item_type {
            "message" => {
                let final_text = final_message_text(item)?;
                let Some(OutputSlot::Text {
                    id,
                    content,
                    active_part,
                    ..
                }) = self.slots.get(&index)
                else {
                    return Err(ResponsesAdapterError::InvalidEvent(
                        "message done has no matching item".into(),
                    ));
                };
                if required_str(item, "id")? != id
                    || final_text != *content
                    || active_part.is_some()
                {
                    return Err(ResponsesAdapterError::InvalidEvent(
                        "message done does not match streamed item".into(),
                    ));
                }
                self.reserve_events(1)?;
                self.completed_items
                    .insert(index, Value::Object(item.clone()));
                self.slots.remove(&index);
                Ok(ResponsesPush {
                    events: vec![ProviderEvent::TextEnd {
                        content_index: index as usize,
                        content: final_text,
                    }],
                    terminal: None,
                })
            }
            "function_call" => {
                let Some(OutputSlot::Tool {
                    id,
                    call_id,
                    name,
                    accumulator,
                    done,
                }) = self.slots.get(&index)
                else {
                    return Err(ResponsesAdapterError::InvalidEvent(
                        "function_call done has no matching item".into(),
                    ));
                };
                if required_str(item, "id")? != id
                    || required_str(item, "call_id")? != call_id
                    || required_str(item, "name")? != name
                    || !accumulator.matches_raw(string_field(item, "arguments")?)
                    || *done
                {
                    return Err(ResponsesAdapterError::InvalidEvent(
                        "function_call done does not match streamed item".into(),
                    ));
                }
                let Some(OutputSlot::Tool { done, .. }) = self.slots.get_mut(&index) else {
                    unreachable!("tool slot validated above")
                };
                *done = true;
                self.completed_items
                    .insert(index, Value::Object(item.clone()));
                Ok(ResponsesPush::default())
            }
            "reasoning" => {
                let final_summary = reasoning_summary_text(item)?;
                let Some(OutputSlot::Reasoning {
                    id,
                    summary_slot,
                    summary,
                    started,
                    active_part,
                    ..
                }) = self.slots.get(&index)
                else {
                    return Err(ResponsesAdapterError::InvalidEvent(
                        "reasoning done has no matching item".into(),
                    ));
                };
                if required_str(item, "id")? != id
                    || (!final_summary.is_empty() && final_summary != *summary)
                    || active_part.is_some()
                {
                    return Err(ResponsesAdapterError::InvalidEvent(
                        "reasoning done does not match streamed summary".into(),
                    ));
                }
                let summary = if final_summary.is_empty() {
                    summary.clone()
                } else {
                    final_summary
                };
                let started = *started;
                let summary_slot = *summary_slot;
                self.reserve_events(usize::from(started))?;
                self.completed_items
                    .insert(index, Value::Object(item.clone()));
                self.slots.remove(&index);
                if item
                    .get("encrypted_content")
                    .and_then(Value::as_str)
                    .is_some_and(|content| !content.is_empty())
                {
                    self.reasoning_fragments.push((
                        required_str(item, "id")?.to_owned(),
                        ProviderContextFragment {
                            wire_item_index: Some(index),
                            payload: ProviderContextPayload::EncryptedReasoning {
                                protocol: ApiProtocol::OpenAiResponses,
                                item: Value::Object(item.clone()),
                            },
                        },
                    ));
                }
                Ok(ResponsesPush {
                    events: if started {
                        vec![ProviderEvent::ReasoningSummaryEnd {
                            content_index: summary_slot,
                            content: summary,
                        }]
                    } else {
                        Vec::new()
                    },
                    terminal: None,
                })
            }
            other => Err(ResponsesAdapterError::InvalidEvent(format!(
                "unsupported known output item variant {other}"
            ))),
        }
    }

    fn response_terminal(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ResponsesPush, ResponsesAdapterError> {
        self.observe_response_identity(object)?;
        if self
            .slots
            .values()
            .any(|slot| !matches!(slot, OutputSlot::Tool { done: true, .. }))
        {
            return Err(ResponsesAdapterError::InvalidEvent(
                "terminal response arrived with unfinished output items".into(),
            ));
        }
        let response = object
            .get("response")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                ResponsesAdapterError::InvalidEvent("response must be an object".into())
            })?;
        self.usage = response
            .get("usage")
            .map(parse_usage)
            .transpose()?
            .unwrap_or_default();
        let status = required_str(response, "status")?;
        self.validate_terminal_output(response)?;
        let reason = match status {
            "completed" if self.saw_tool => StopReason::ToolUse,
            "completed" => StopReason::Stop,
            "incomplete" => StopReason::Length,
            other => {
                return Err(ResponsesAdapterError::InvalidEvent(format!(
                    "terminal event has unsupported status {other}"
                )));
            }
        };
        backfill_reasoning_fragments(response, &mut self.reasoning_fragments)?;
        let mut events = Vec::new();
        self.reserve_events(self.slots.len().saturating_add(1))?;
        let tool_slots = std::mem::take(&mut self.slots);
        for (index, slot) in tool_slots {
            let OutputSlot::Tool {
                call_id,
                name,
                accumulator,
                done: true,
                ..
            } = slot
            else {
                unreachable!("unfinished slots rejected above")
            };
            let outcome = if reason == StopReason::Length {
                accumulator.reject_incomplete(call_id, name, Utc::now())
            } else {
                accumulator.finish(call_id, name, &self.schemas, Utc::now())
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
        self.terminal = true;
        Ok(ResponsesPush {
            events: Vec::new(),
            terminal: Some(ResponsesTerminal {
                events,
                reason,
                usage: self.usage.clone(),
                error_message: None,
                provider_code: if reason == StopReason::Length {
                    response
                        .get("incomplete_details")
                        .and_then(Value::as_object)
                        .and_then(|details| details.get("reason"))
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                } else {
                    None
                },
                provider_context: self.provider_context(),
            }),
        })
    }

    fn validate_item_id(
        &self,
        index: u32,
        object: &Map<String, Value>,
        expected_type: &str,
    ) -> Result<(), ResponsesAdapterError> {
        let item_id = required_str(object, "item_id")?;
        if !matches!(
            self.output_identities.get(&index),
            Some((id, kind)) if id == item_id && kind == expected_type
        ) {
            return Err(ResponsesAdapterError::InvalidEvent(
                "typed event item_id/type does not match output slot".into(),
            ));
        }
        Ok(())
    }

    fn validate_terminal_output(
        &self,
        response: &Map<String, Value>,
    ) -> Result<(), ResponsesAdapterError> {
        let output = response
            .get("output")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                ResponsesAdapterError::InvalidEvent("response.output must be an array".into())
            })?;
        if output.len() != self.completed_items.len()
            || self.completed_items.len() != self.output_identities.len()
        {
            return Err(ResponsesAdapterError::InvalidEvent(
                "terminal response output is missing or reordered".into(),
            ));
        }
        for (index, item) in output.iter().enumerate() {
            let item = item.as_object().ok_or_else(|| {
                ResponsesAdapterError::InvalidEvent("terminal output item must be an object".into())
            })?;
            let index = u32::try_from(index).map_err(|_| {
                ResponsesAdapterError::InvalidEvent("terminal output index exceeds u32".into())
            })?;
            let Some((id, kind)) = self.output_identities.get(&index) else {
                return Err(ResponsesAdapterError::InvalidEvent(
                    "terminal response output index is missing".into(),
                ));
            };
            if required_str(item, "id")? != id || required_str(item, "type")? != kind {
                return Err(ResponsesAdapterError::InvalidEvent(
                    "terminal response output identity/order does not match stream".into(),
                ));
            }
            let completed = self.completed_items.get(&index).ok_or_else(|| {
                ResponsesAdapterError::InvalidEvent(
                    "terminal response output item was not completed".into(),
                )
            })?;
            if !terminal_item_matches_completed(completed, item)? {
                return Err(ResponsesAdapterError::InvalidEvent(
                    "terminal response output item mutated after item.done".into(),
                ));
            }
        }
        Ok(())
    }

    fn response_failed(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<ResponsesPush, ResponsesAdapterError> {
        self.observe_response_identity(object)?;
        let response = object
            .get("response")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                ResponsesAdapterError::InvalidEvent("response must be an object".into())
            })?;
        self.usage = response
            .get("usage")
            .map(parse_usage)
            .transpose()?
            .unwrap_or_default();
        let error = response.get("error").and_then(Value::as_object);
        let code = error
            .and_then(|error| error.get("code"))
            .and_then(Value::as_str)
            .unwrap_or("provider_error")
            .to_owned();
        let message = error
            .and_then(|error| error.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("provider response failed")
            .to_owned();
        self.reserve_events(self.slots.len().saturating_add(1))?;
        let events = self.fail();
        self.terminal = true;
        Ok(ResponsesPush {
            events: Vec::new(),
            terminal: Some(ResponsesTerminal {
                events,
                reason: StopReason::Error,
                usage: self.usage.clone(),
                error_message: Some(message),
                provider_code: Some(code),
                provider_context: self.provider_context(),
            }),
        })
    }

    fn observe_response_identity(
        &mut self,
        object: &Map<String, Value>,
    ) -> Result<(), ResponsesAdapterError> {
        let response = object
            .get("response")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                ResponsesAdapterError::InvalidEvent("response must be an object".into())
            })?;
        let id = required_str(response, "id")?;
        let model = required_str(response, "model")?;
        if self.response_id.as_deref().is_some_and(|known| known != id)
            || self
                .response_model
                .as_deref()
                .is_some_and(|known| known != model)
        {
            return Err(ResponsesAdapterError::InvalidEvent(
                "response identity changed during stream".into(),
            ));
        }
        if self.response_id.is_none() {
            self.commit_charges(id.len().saturating_add(model.len()), 0, 0)?;
            self.response_id = Some(id.to_owned());
            self.response_model = Some(model.to_owned());
        }
        Ok(())
    }

    fn commit_charges(
        &mut self,
        content: usize,
        events: usize,
        preview: usize,
    ) -> Result<(), ResponsesAdapterError> {
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

    fn reserve_events(&mut self, additional: usize) -> Result<(), ResponsesAdapterError> {
        self.event_count = checked_counter(
            self.event_count,
            additional,
            self.budget.max_events,
            "event_count",
        )?;
        Ok(())
    }
}

fn checked_counter(
    current: usize,
    additional: usize,
    limit: usize,
    resource: &'static str,
) -> Result<usize, ResponsesAdapterError> {
    let next = current
        .checked_add(additional)
        .ok_or(ResponsesAdapterError::ResponseLimitExceeded { resource, limit })?;
    if next > limit {
        return Err(ResponsesAdapterError::ResponseLimitExceeded { resource, limit });
    }
    Ok(next)
}

fn required_str<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, ResponsesAdapterError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ResponsesAdapterError::InvalidEvent(format!("{field} must be a non-empty string"))
        })
}

fn string_field<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, ResponsesAdapterError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| ResponsesAdapterError::InvalidEvent(format!("{field} must be a string")))
}

fn output_index(object: &Map<String, Value>) -> Result<u32, ResponsesAdapterError> {
    let value = object
        .get("output_index")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            ResponsesAdapterError::InvalidEvent("output_index must be an integer".into())
        })?;
    u32::try_from(value)
        .map_err(|_| ResponsesAdapterError::InvalidEvent("output_index exceeds u32".into()))
}

fn nested_index(object: &Map<String, Value>, field: &str) -> Result<u32, ResponsesAdapterError> {
    let value = object.get(field).and_then(Value::as_u64).ok_or_else(|| {
        ResponsesAdapterError::InvalidEvent(format!("{field} must be an integer"))
    })?;
    u32::try_from(value)
        .map_err(|_| ResponsesAdapterError::InvalidEvent(format!("{field} exceeds u32")))
}

fn terminal_item_matches_completed(
    completed: &Value,
    terminal: &Map<String, Value>,
) -> Result<bool, ResponsesAdapterError> {
    if completed == &Value::Object(terminal.clone()) {
        return Ok(true);
    }
    let completed = completed.as_object().ok_or_else(|| {
        ResponsesAdapterError::InvalidEvent("completed output item must be an object".into())
    })?;
    if required_str(completed, "type")? != "reasoning"
        || required_str(terminal, "type")? != "reasoning"
    {
        return Ok(false);
    }
    let terminal_encrypted = terminal
        .get("encrypted_content")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty());
    let completed_encrypted = completed
        .get("encrypted_content")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty());
    if terminal_encrypted.is_none() || completed_encrypted.is_some() {
        return Ok(false);
    }
    let mut terminal_without_backfill = terminal.clone();
    terminal_without_backfill.remove("encrypted_content");
    let mut completed_without_empty = completed.clone();
    if completed_without_empty
        .get("encrypted_content")
        .is_some_and(|value| value.is_null() || value.as_str() == Some(""))
    {
        completed_without_empty.remove("encrypted_content");
    }
    Ok(terminal_without_backfill == completed_without_empty)
}

fn ensure_assistant_message_item(item: &Map<String, Value>) -> Result<(), ResponsesAdapterError> {
    if required_str(item, "role")? != "assistant" {
        return Err(ResponsesAdapterError::InvalidEvent(
            "output message role must be assistant".into(),
        ));
    }
    let content = item
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ResponsesAdapterError::InvalidEvent("message content must be an array".into())
        })?;
    if !content.is_empty() {
        return Err(ResponsesAdapterError::InvalidEvent(
            "message added item must begin with empty content".into(),
        ));
    }
    Ok(())
}

fn final_message_text(item: &Map<String, Value>) -> Result<String, ResponsesAdapterError> {
    if required_str(item, "role")? != "assistant" {
        return Err(ResponsesAdapterError::InvalidEvent(
            "output message role must be assistant".into(),
        ));
    }
    let content = item
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ResponsesAdapterError::InvalidEvent("message content must be an array".into())
        })?;
    let mut text = String::new();
    for part in content {
        let part = part.as_object().ok_or_else(|| {
            ResponsesAdapterError::InvalidEvent("content part must be an object".into())
        })?;
        match required_str(part, "type")? {
            "output_text" => text.push_str(string_field(part, "text")?),
            "refusal" => text.push_str(string_field(part, "refusal")?),
            other => {
                return Err(ResponsesAdapterError::InvalidEvent(format!(
                    "unsupported known message content variant {other}"
                )));
            }
        }
    }
    Ok(text)
}

fn reasoning_summary_text(item: &Map<String, Value>) -> Result<String, ResponsesAdapterError> {
    let summary = item
        .get("summary")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ResponsesAdapterError::InvalidEvent("reasoning summary must be an array".into())
        })?;
    let mut parts = Vec::with_capacity(summary.len());
    for part in summary {
        let part = part.as_object().ok_or_else(|| {
            ResponsesAdapterError::InvalidEvent("summary part must be an object".into())
        })?;
        if required_str(part, "type")? != "summary_text" {
            return Err(ResponsesAdapterError::InvalidEvent(
                "unsupported known reasoning summary variant".into(),
            ));
        }
        parts.push(string_field(part, "text")?);
    }
    Ok(parts.join("\n\n"))
}

fn validate_reasoning_summary_event(
    object: &Map<String, Value>,
) -> Result<(), ResponsesAdapterError> {
    if let Some(part) = object.get("part") {
        let part = part.as_object().ok_or_else(|| {
            ResponsesAdapterError::InvalidEvent("reasoning summary part must be an object".into())
        })?;
        if required_str(part, "type")? != "summary_text" {
            return Err(ResponsesAdapterError::InvalidEvent(
                "unsupported known reasoning summary part variant".into(),
            ));
        }
        let _ = string_field(part, "text")?;
    } else if let Some(text) = object.get("text")
        && !text.is_string()
    {
        return Err(ResponsesAdapterError::InvalidEvent(
            "reasoning summary text must be a string".into(),
        ));
    }
    Ok(())
}

fn parse_usage(value: &Value) -> Result<Usage, ResponsesAdapterError> {
    let usage = value
        .as_object()
        .ok_or_else(|| ResponsesAdapterError::InvalidEvent("usage must be an object".into()))?;
    let input_total = usage
        .get("input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let output = usage
        .get("output_tokens")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let cache_read = usage
        .get("input_tokens_details")
        .and_then(Value::as_object)
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let cache_write = usage
        .get("input_tokens_details")
        .and_then(Value::as_object)
        .and_then(|details| details.get("cache_write_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let reasoning = usage
        .get("output_tokens_details")
        .and_then(Value::as_object)
        .and_then(|details| details.get("reasoning_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or_default();
    Ok(Usage {
        input: input_total
            .saturating_sub(cache_read)
            .saturating_sub(cache_write),
        output,
        cache_read,
        cache_write,
        reasoning,
        total_tokens: usage
            .get("total_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| input_total.saturating_add(output)),
    })
}

fn item_identity_bytes(item: &Map<String, Value>) -> Result<usize, ResponsesAdapterError> {
    let mut total = required_str(item, "id")?.len();
    for field in ["call_id", "name"] {
        if let Some(value) = item.get(field).and_then(Value::as_str) {
            total = total.checked_add(value.len()).ok_or(
                ResponsesAdapterError::ResponseLimitExceeded {
                    resource: "content_bytes",
                    limit: usize::MAX,
                },
            )?;
        }
    }
    Ok(total)
}

fn backfill_reasoning_fragments(
    response: &Map<String, Value>,
    fragments: &mut Vec<(String, ProviderContextFragment)>,
) -> Result<(), ResponsesAdapterError> {
    let Some(output) = response.get("output").and_then(Value::as_array) else {
        return Ok(());
    };
    let mut encrypted = HashMap::new();
    for (index, item) in output.iter().enumerate() {
        let Some(item) = item.as_object() else {
            return Err(ResponsesAdapterError::InvalidEvent(
                "terminal output item must be an object".into(),
            ));
        };
        let item_type = required_str(item, "type")?;
        if !matches!(item_type, "message" | "function_call" | "reasoning") {
            return Err(ResponsesAdapterError::InvalidEvent(format!(
                "unsupported known output item variant {item_type}"
            )));
        }
        if item_type == "reasoning"
            && item
                .get("encrypted_content")
                .and_then(Value::as_str)
                .is_some_and(|content| !content.is_empty())
        {
            encrypted.insert(
                required_str(item, "id")?.to_owned(),
                (
                    u32::try_from(index).map_err(|_| {
                        ResponsesAdapterError::InvalidEvent(
                            "terminal output index exceeds u32".into(),
                        )
                    })?,
                    item.clone(),
                ),
            );
        }
    }
    for (id, fragment) in fragments.iter_mut() {
        let Some((_, item)) = encrypted.remove(id) else {
            continue;
        };
        fragment.payload = ProviderContextPayload::EncryptedReasoning {
            protocol: ApiProtocol::OpenAiResponses,
            item: Value::Object(item),
        };
    }
    for (id, (index, item)) in encrypted {
        fragments.push((
            id,
            ProviderContextFragment {
                wire_item_index: Some(index),
                payload: ProviderContextPayload::EncryptedReasoning {
                    protocol: ApiProtocol::OpenAiResponses,
                    item: Value::Object(item),
                },
            },
        ));
    }
    fragments.sort_by_key(|(_, fragment)| fragment.wire_item_index);
    Ok(())
}

fn validate_canonical_item(item: &Value) -> Result<(), String> {
    let object = item
        .as_object()
        .ok_or_else(|| "output item must be an object".to_owned())?;
    let item_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| "output item type must be a string".to_owned())?;
    match item_type {
        "message" => {
            let role = object
                .get("role")
                .and_then(Value::as_str)
                .ok_or_else(|| "message role must be a string".to_owned())?;
            if !matches!(role, "user" | "assistant" | "system" | "developer") {
                return Err("unsupported compacted message role".into());
            }
            let content = object
                .get("content")
                .and_then(Value::as_array)
                .ok_or_else(|| "message content must be an array".to_owned())?;
            for part in content {
                let part = part
                    .as_object()
                    .ok_or_else(|| "message content part must be an object".to_owned())?;
                let part_type = part
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "message content part type must be a string".to_owned())?;
                match part_type {
                    "input_text" | "output_text" => {
                        if !part.get("text").is_some_and(Value::is_string) {
                            return Err(format!("{part_type} text must be a string"));
                        }
                    }
                    "refusal" => {
                        if !part.get("refusal").is_some_and(Value::is_string) {
                            return Err("refusal content must be a string".into());
                        }
                    }
                    "input_image" => {
                        if !part.get("image_url").is_some_and(Value::is_string) {
                            return Err("input_image image_url must be a string".into());
                        }
                    }
                    other => {
                        return Err(format!(
                            "unsupported compacted message content variant {other}"
                        ));
                    }
                }
            }
        }
        "function_call" => {
            for field in ["call_id", "name", "arguments"] {
                if !object.get(field).is_some_and(Value::is_string) {
                    return Err(format!("function_call {field} must be a string"));
                }
            }
        }
        "function_call_output" => {
            if !object.get("call_id").is_some_and(Value::is_string) {
                return Err("function_call_output call_id must be a string".into());
            }
            let output = object
                .get("output")
                .ok_or_else(|| "function_call_output output is required".to_owned())?;
            if let Some(parts) = output.as_array() {
                for part in parts {
                    validate_input_content_part(part, "function_call_output")?;
                }
            } else if !output.is_string() {
                return Err("function_call_output output must be a string or content array".into());
            }
        }
        "reasoning" => {
            if !object.get("id").is_some_and(Value::is_string)
                || !object.get("summary").is_some_and(Value::is_array)
            {
                return Err("invalid reasoning item".into());
            }
            for part in object["summary"]
                .as_array()
                .expect("checked reasoning summary array")
            {
                let part = part
                    .as_object()
                    .ok_or_else(|| "reasoning summary part must be an object".to_owned())?;
                if part.get("type").and_then(Value::as_str) != Some("summary_text")
                    || !part.get("text").is_some_and(Value::is_string)
                {
                    return Err(
                        "reasoning summary part must be summary_text with string text".into(),
                    );
                }
            }
            if let Some(content) = object.get("content") {
                let content = content
                    .as_array()
                    .ok_or_else(|| "reasoning content must be an array".to_owned())?;
                for part in content {
                    let part = part
                        .as_object()
                        .ok_or_else(|| "reasoning content part must be an object".to_owned())?;
                    if part.get("type").and_then(Value::as_str) != Some("reasoning_text")
                        || !part.get("text").is_some_and(Value::is_string)
                    {
                        return Err(
                            "reasoning content part must be reasoning_text with string text".into(),
                        );
                    }
                }
            }
        }
        "compaction" => {
            if !object.get("id").is_some_and(Value::is_string)
                || object
                    .get("encrypted_content")
                    .and_then(Value::as_str)
                    .is_none_or(|content| content.is_empty())
            {
                return Err("compaction encrypted_content must be non-empty".into());
            }
        }
        other => return Err(format!("unsupported compact output item variant {other}")),
    }
    Ok(())
}

fn validate_input_content_part(part: &Value, parent: &str) -> Result<(), String> {
    let part = part
        .as_object()
        .ok_or_else(|| format!("{parent} content part must be an object"))?;
    match part.get("type").and_then(Value::as_str) {
        Some("input_text") if part.get("text").is_some_and(Value::is_string) => Ok(()),
        Some("input_image") if part.get("image_url").is_some_and(Value::is_string) => Ok(()),
        Some("input_text") => Err(format!("{parent} input_text text must be a string")),
        Some("input_image") => Err(format!("{parent} input_image image_url must be a string")),
        Some(other) => Err(format!("{parent} unsupported content variant {other}")),
        None => Err(format!("{parent} content part type must be a string")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::types::{
        AssistantMessage, ToolArgsPreview, ToolCall, ToolDefinition, UserMessage,
        ValidatedToolArguments,
    };

    fn spec() -> ModelSpec {
        ModelSpec::preset("openai-responses").expect("Responses preset")
    }

    fn schemas() -> FrozenToolSchemaRegistry {
        FrozenToolSchemaRegistry::compile(&[ToolDefinition {
            name: "weather".into(),
            description: "Weather".into(),
            parameters: json!({
                "type":"object",
                "properties":{"city":{"type":"string"}},
                "required":["city"],
                "additionalProperties":false
            }),
        }])
        .expect("schema")
    }

    fn persisted_user(seq: u64) -> ContextMessage {
        ContextMessage::Persisted {
            id: format!("user-{seq}"),
            seq,
            message: Message::User(UserMessage {
                content: vec![UserContent::Text {
                    text: format!("message-{seq}"),
                }],
                timestamp: Utc::now(),
            }),
        }
    }

    fn fixture_values() -> Vec<Value> {
        include_str!("../../../tests/fixtures/openai_responses_official.sse")
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .map(|data| serde_json::from_str(data).expect("fixture JSON"))
            .collect()
    }

    #[test]
    fn request_uses_instructions_three_layer_items_and_store_false() {
        let context = PromptContext {
            system_prompt: "constitution".into(),
            memory_blocks: vec![crate::provider::types::MemoryBlock {
                layer: MemoryLayer::L2,
                text: "old </memory> attack".into(),
                time_range: None,
            }],
            messages: vec![ContextMessage::Synthetic {
                message: Message::User(UserMessage {
                    content: vec![UserContent::Text {
                        text: "hello".into(),
                    }],
                    timestamp: Utc::now(),
                }),
            }],
            provider_context: vec![],
            tools: vec![],
        };
        let body = build_request(&spec(), &context, &RequestOptions::default()).expect("request");
        assert_eq!(body["instructions"], "constitution");
        assert_eq!(body["store"], false);
        assert_eq!(body["input"][0]["role"], "user");
        assert_eq!(
            body["input"][0]["content"][0]["text"],
            "<memory layer=\"l2\">old &lt;/memory&gt; attack</memory>"
        );
        assert_eq!(body["input"][1]["type"], "message");
        assert!(body.get("previous_response_id").is_none());
    }

    #[test]
    fn official_sse_fixture_normalizes_all_supported_events() {
        // Adapted from the official Responses streaming API example. Durable encrypted
        // round-trip and live two-turn/tool evidence remain release-blocking until
        // T17/T25; this fixture does not claim either gate.
        let fixture = include_str!("../../../tests/fixtures/openai_responses_official.sse");
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        let mut events = Vec::new();
        let mut terminal = None;
        for line in fixture.lines() {
            let Some(data) = line.strip_prefix("data: ") else {
                continue;
            };
            let pushed = state.push_json(data).expect("official event");
            events.extend(pushed.events);
            if let Some(value) = pushed.terminal {
                events.extend(value.events.clone());
                terminal = Some(value);
            }
        }
        state.finish_eof().expect("terminal");
        assert_eq!(
            events,
            vec![
                ProviderEvent::TextStart { content_index: 0 },
                ProviderEvent::TextDelta {
                    content_index: 0,
                    delta: "Weather: ".into(),
                },
                ProviderEvent::TextEnd {
                    content_index: 0,
                    content: "Weather: ".into(),
                },
                ProviderEvent::ReasoningSummaryStart { content_index: 0 },
                ProviderEvent::ReasoningSummaryDelta {
                    content_index: 0,
                    delta: "Checking.".into(),
                },
                ProviderEvent::ReasoningSummaryEnd {
                    content_index: 0,
                    content: "Checking.".into(),
                },
                ProviderEvent::ToolCallStart { content_index: 2 },
                ProviderEvent::ToolCallDelta {
                    content_index: 2,
                    delta: r#"{"city":"Tokyo"}"#.into(),
                },
                ProviderEvent::ToolCallPreview {
                    content_index: 2,
                    preview: ToolArgsPreview::new(json!({"city":"Tokyo"})),
                },
                ProviderEvent::ToolCallEnd {
                    content_index: 2,
                    tool_call: ToolCall {
                        id: "call_fixture".into(),
                        name: "weather".into(),
                        arguments: ValidatedToolArguments::from_schema_validated(
                            json!({"city":"Tokyo"}).as_object().unwrap().clone(),
                        ),
                    },
                },
            ]
        );
        let terminal = terminal.expect("terminal");
        assert_eq!(terminal.reason, StopReason::ToolUse);
        assert_eq!(terminal.usage.output, 12);
        assert_eq!(terminal.usage.reasoning, 4);
        assert_eq!(terminal.provider_context.len(), 1);
        assert_eq!(
            terminal.provider_context[0].payload,
            ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::OpenAiResponses,
                item: json!({
                    "id":"rs_fixture",
                    "type":"reasoning",
                    "summary":[{"type":"summary_text","text":"Checking."}],
                    "encrypted_content":"opaque-reasoning"
                }),
            }
        );
    }

    #[test]
    fn sequence_numbers_cover_unknown_events_without_ordering_ambiguity() {
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        state
            .push_json(
                r#"{"type":"response.created","sequence_number":0,"response":{"id":"r","model":"gpt-5.6","status":"in_progress","output":[]}}"#,
            )
            .unwrap();
        assert!(
            state
                .push_json(r#"{"type":"response.future.delta","sequence_number":2}"#)
                .is_err()
        );
        state
            .push_json(r#"{"type":"response.future.delta","sequence_number":1}"#)
            .expect("unknown event consumes its documented global sequence slot");
        assert!(
            state
                .push_json(r#"{"type":"response.future.delta"}"#)
                .is_err()
        );
        state
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"m","type":"message","role":"assistant","content":[]}}"#,
            )
            .expect("failed sequence attempts do not consume state");
    }

    #[test]
    fn terminal_output_rejects_message_reasoning_and_tool_mutation() {
        for mutation in ["message", "reasoning", "tool"] {
            let mut values = fixture_values();
            let terminal = values.last_mut().unwrap()["response"]["output"]
                .as_array_mut()
                .unwrap();
            match mutation {
                "message" => terminal[0]["content"][0]["text"] = json!("mutated"),
                "reasoning" => terminal[1]["summary"][0]["text"] = json!("mutated"),
                "tool" => terminal[2]["name"] = json!("mutated"),
                _ => unreachable!(),
            }
            let mut state =
                ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
            for value in &values[..values.len() - 1] {
                state.push_json(&value.to_string()).unwrap();
            }
            assert!(
                state
                    .push_json(&values.last().unwrap().to_string())
                    .expect_err("terminal mutation must fail")
                    .to_string()
                    .contains("mutated after item.done"),
                "{mutation}"
            );
        }
    }

    #[test]
    fn terminal_may_only_backfill_reasoning_encrypted_content() {
        let mut values = fixture_values();
        values[12]["item"]
            .as_object_mut()
            .unwrap()
            .remove("encrypted_content");
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        let mut terminal = None;
        for value in values {
            terminal = state
                .push_json(&value.to_string())
                .expect("documented encrypted_content backfill")
                .terminal
                .or(terminal);
        }
        let terminal = terminal.expect("terminal");
        assert_eq!(terminal.provider_context.len(), 1);
        assert_eq!(
            terminal.provider_context[0].payload.clone(),
            ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::OpenAiResponses,
                item: json!({
                    "id":"rs_fixture",
                    "type":"reasoning",
                    "summary":[{"type":"summary_text","text":"Checking."}],
                    "encrypted_content":"opaque-reasoning",
                }),
            }
        );
    }

    #[test]
    fn compaction_coverage_is_internal_canonical_and_requires_contiguous_persistence() {
        let spec = spec();
        let mut context = PromptContext {
            system_prompt: "system".into(),
            memory_blocks: vec![],
            messages: vec![persisted_user(7), persisted_user(8)],
            provider_context: vec![],
            tools: vec![],
        };
        let coverage = derive_compaction_coverage(&spec, &context).expect("coverage");
        assert_eq!(coverage.through_message_seq, 8);
        assert_eq!(coverage.context_fingerprint.len(), 64);
        context.messages.insert(
            0,
            ContextMessage::Synthetic {
                message: Message::User(UserMessage {
                    content: vec![],
                    timestamp: Utc::now(),
                }),
            },
        );
        assert_eq!(
            derive_compaction_coverage(&spec, &context)
                .expect("leading synthetic prefix with persisted suffix")
                .through_message_seq,
            8
        );
        context.messages.remove(0);

        let mut changed_origin = spec.clone();
        changed_origin.account_scope = "other".into();
        assert_ne!(
            coverage.context_fingerprint,
            derive_compaction_coverage(&changed_origin, &context)
                .unwrap()
                .context_fingerprint
        );
        context.system_prompt = "changed".into();
        assert_ne!(
            coverage.context_fingerprint,
            derive_compaction_coverage(&spec, &context)
                .unwrap()
                .context_fingerprint
        );

        for messages in [
            vec![],
            vec![ContextMessage::Synthetic {
                message: Message::User(UserMessage {
                    content: vec![],
                    timestamp: Utc::now(),
                }),
            }],
            vec![persisted_user(7), persisted_user(7)],
            vec![persisted_user(8), persisted_user(7)],
            vec![persisted_user(7), persisted_user(9)],
            vec![
                persisted_user(7),
                ContextMessage::Synthetic {
                    message: Message::User(UserMessage {
                        content: vec![],
                        timestamp: Utc::now(),
                    }),
                },
            ],
        ] {
            context.messages = messages;
            assert!(derive_compaction_coverage(&spec, &context).is_err());
        }
    }

    #[test]
    fn compact_request_disables_provider_storage_when_supported() {
        let body = build_compact_request(
            &spec(),
            &PromptContext {
                system_prompt: "system".into(),
                memory_blocks: vec![],
                messages: vec![persisted_user(1)],
                provider_context: vec![],
                tools: vec![],
            },
        )
        .expect("compact request");
        assert_eq!(body["store"], false);
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
            message_id: "assistant-1".into(),
            message_seq: 1,
        };
        let assistant = AssistantMessage {
            content: vec![
                AssistantContent::Thinking {
                    thinking: "RAW_THINKING_MARKER".into(),
                    signature_field: "encrypted_content".into(),
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
            timestamp: Utc::now(),
        };
        let context = PromptContext {
            system_prompt: "system".into(),
            memory_blocks: vec![],
            messages: vec![ContextMessage::Persisted {
                id: anchor.message_id.clone(),
                seq: anchor.message_seq,
                message: Message::Assistant(assistant),
            }],
            provider_context: vec![ProviderContextItem {
                origin_message: Some(anchor),
                wire_item_index: Some(0),
                ordinal: 0,
                payload: ProviderContextPayload::EncryptedReasoning {
                    protocol: ApiProtocol::OpenAiResponses,
                    item: json!({
                        "id":"reasoning-marker",
                        "type":"reasoning",
                        "summary":[],
                        "encrypted_content":"OPAQUE_MARKER",
                    }),
                },
            }],
            tools: vec![],
        };
        let request = build_request(&target, &context, &RequestOptions::default()).unwrap();
        let wire = request.to_string();
        assert!(wire.contains("PUBLIC_TEXT_MARKER"));
        assert!(!wire.contains("RAW_THINKING_MARKER"));
        assert!(!wire.contains("OPAQUE_MARKER"));

        let mut same_origin = context;
        if let Message::Assistant(assistant) = match &mut same_origin.messages[0] {
            ContextMessage::Persisted { message, .. } => message,
            ContextMessage::Synthetic { .. } => unreachable!(),
        } {
            assistant.origin = target.origin();
            assistant.model.clone_from(&target.id);
            assistant.provider.clone_from(&target.provider);
        }
        same_origin.provider_context[0].payload = ProviderContextPayload::EncryptedReasoning {
            protocol: ApiProtocol::AnthropicMessages,
            item: json!({"malformed":"SAME_ORIGIN_MARKER"}),
        };
        assert!(build_request(&target, &same_origin, &RequestOptions::default()).is_err());
    }

    #[test]
    fn typed_event_identity_and_nested_index_reordering_fail_closed() {
        let mut wrong_text_id =
            ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        wrong_text_id
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"m","type":"message","role":"assistant","content":[]}}"#,
            )
            .unwrap();
        assert!(
            wrong_text_id
                .push_json(
                    r#"{"type":"response.content_part.added","sequence_number":1,"item_id":"other","output_index":0,"content_index":0,"part":{"type":"output_text","text":""}}"#,
                )
                .is_err()
        );

        let mut reordered_summary =
            ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        reordered_summary
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"r","type":"reasoning","summary":[]}}"#,
            )
            .unwrap();
        assert!(
            reordered_summary
                .push_json(
                    r#"{"type":"response.reasoning_summary_part.added","sequence_number":1,"item_id":"r","output_index":0,"summary_index":1,"part":{"type":"summary_text","text":""}}"#,
                )
                .is_err()
        );

        let mut mixed_tool =
            ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        mixed_tool
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"fc","type":"function_call","call_id":"call","name":"weather","arguments":""}}"#,
            )
            .unwrap();
        assert!(
            mixed_tool
                .push_json(
                    r#"{"type":"response.function_call_arguments.delta","sequence_number":1,"item_id":"r","output_index":0,"delta":"{}"}"#,
                )
                .is_err()
        );

        let mut missing_output =
            ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        assert!(
            missing_output
                .push_json(
                    r#"{"type":"response.output_item.added","sequence_number":0,"output_index":1,"item":{"id":"m","type":"message","role":"assistant","content":[]}}"#,
                )
                .is_err()
        );
    }

    #[test]
    fn known_item_unknown_variant_fails_but_unknown_event_is_ignored() {
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        assert!(
            state
                .push_json(
                    r#"{"type":"response.future.delta","sequence_number":0,"delta":"ignored"}"#
                )
                .expect("unknown event")
                .events
                .is_empty()
        );
        let error = state
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":{"id":"x","type":"future_item"}}"#,
            )
            .expect_err("known item variant must fail");
        assert!(
            error
                .to_string()
                .contains("unsupported known output item variant")
        );
    }

    #[test]
    fn protocol_compat_mismatch_and_budget_failure_are_fail_closed_transactionally() {
        let mut mismatched = spec();
        mismatched.protocol = ApiProtocol::OpenAiChatCompletions;
        assert!(matches!(
            build_request(
                &mismatched,
                &PromptContext {
                    system_prompt: String::new(),
                    memory_blocks: vec![],
                    messages: vec![],
                    provider_context: vec![],
                    tools: vec![],
                },
                &RequestOptions::default()
            ),
            Err(ResponsesAdapterError::UnsupportedProtocol)
        ));

        let budget = ResponseBudget {
            max_content_bytes: 3,
            max_wire_bytes: 1024,
            max_events: 1,
            max_preview_work_bytes: 1024,
            max_tool_calls: 1,
        };
        let mut state = ResponsesReceiveState::with_budget(schemas(), budget);
        assert!(matches!(
            state.push_json(
                r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"long","type":"message","role":"assistant","content":[]}}"#
            ),
            Err(ResponsesAdapterError::ResponseLimitExceeded { .. })
        ));
        let pushed = state
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"m","type":"message","role":"assistant","content":[]}}"#,
            )
            .expect("failed budget preflight must not consume slot or event counters");
        assert!(matches!(
            pushed.events.as_slice(),
            [ProviderEvent::TextStart { content_index: 0 }]
        ));
    }

    #[test]
    fn incomplete_response_rejects_even_strictly_valid_tool_arguments() {
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        for payload in [
            r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"fc","type":"function_call","call_id":"call","name":"weather","arguments":""}}"#,
            r#"{"type":"response.function_call_arguments.delta","sequence_number":1,"item_id":"fc","output_index":0,"delta":"{\"city\":\"Tokyo\"}"}"#,
            r#"{"type":"response.output_item.done","sequence_number":2,"output_index":0,"item":{"id":"fc","type":"function_call","call_id":"call","name":"weather","arguments":"{\"city\":\"Tokyo\"}"}}"#,
        ] {
            state.push_json(payload).expect("tool event");
        }
        let terminal = state
            .push_json(
                r#"{"type":"response.incomplete","sequence_number":3,"response":{"id":"resp_incomplete","model":"gpt-5.6","status":"incomplete","output":[{"id":"fc","type":"function_call","call_id":"call","name":"weather","arguments":"{\"city\":\"Tokyo\"}"}],"incomplete_details":{"reason":"max_output_tokens"},"usage":{"input_tokens":1,"output_tokens":1}}}"#,
            )
            .expect("incomplete terminal")
            .terminal
            .expect("terminal");
        assert_eq!(terminal.reason, StopReason::Length);
        assert!(matches!(
            terminal.events.as_slice(),
            [ProviderEvent::ToolCallRejected { rejected, .. }]
                if rejected.error == crate::provider::types::ToolArgumentError::IncompleteResponse
        ));
    }

    #[test]
    fn compact_preserves_ordered_output_without_pruning() {
        let items = vec![
            json!({"id":"m1","type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}),
            json!({"id":"fc1","type":"function_call","call_id":"call1","name":"weather","arguments":"{\"city\":\"Tokyo\"}"}),
            json!({"type":"function_call_output","call_id":"call1","output":"sunny"}),
            json!({"id":"cmp1","type":"compaction","encrypted_content":"opaque"}),
        ];
        let coverage = NativeCompactionCoverage {
            through_message_seq: 42,
            context_fingerprint: "fp".into(),
        };
        let result = parse_compact_response(
            json!({
                "object":"response.compaction",
                "output":items,
                "usage":{"input_tokens":10,"output_tokens":3,"total_tokens":13}
            }),
            coverage.clone(),
        )
        .expect("compact response");
        assert_eq!(result.items, items);
        assert_eq!(result.coverage, coverage);
        assert_eq!(result.usage.total_tokens, 13);
    }

    #[test]
    fn native_compacted_window_replays_exact_order_then_only_uncovered_suffix() {
        let spec = spec();
        let window = vec![
            json!({"id":"m-old","type":"message","role":"user","content":[{"type":"input_text","text":"old"}]}),
            json!({"id":"cmp","type":"compaction","encrypted_content":"opaque"}),
            json!({"type":"function_call_output","call_id":"old-call","output":[{"type":"input_text","text":"done"}]}),
        ];
        let mut context = PromptContext {
            system_prompt: "system".into(),
            memory_blocks: vec![],
            messages: vec![
                ContextMessage::Synthetic {
                    message: Message::User(UserMessage {
                        content: vec![UserContent::Text {
                            text: "leading-synthetic".into(),
                        }],
                        timestamp: Utc::now(),
                    }),
                },
                persisted_user(7),
                persisted_user(8),
                persisted_user(9),
                persisted_user(10),
            ],
            provider_context: vec![],
            tools: vec![],
        };
        let fingerprint = context_fingerprint(&spec, &context).unwrap();
        context.provider_context.push(ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: window.clone(),
                coverage: NativeCompactionCoverage {
                    through_message_seq: 8,
                    context_fingerprint: fingerprint,
                },
            },
        });

        let input = convert_input(&spec, &context).expect("valid native replay");
        assert_eq!(input[0]["content"][0]["text"], "leading-synthetic");
        assert_eq!(&input[1..=window.len()], window.as_slice());
        assert_eq!(input.len(), window.len() + 3);
        assert_eq!(input[window.len() + 1]["content"][0]["text"], "message-9");
        assert_eq!(input[window.len() + 2]["content"][0]["text"], "message-10");

        let request = build_request(&spec, &context, &RequestOptions::default())
            .expect("request reuses canonical replay ordering");
        assert_eq!(request["input"], Value::Array(input));
    }

    #[test]
    fn native_compacted_window_rejects_duplicates_coexistence_and_bad_coverage() {
        let spec = spec();
        let mut context = PromptContext {
            system_prompt: "system".into(),
            memory_blocks: vec![],
            messages: vec![persisted_user(8), persisted_user(9)],
            provider_context: vec![],
            tools: vec![],
        };
        let native = ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({"id":"cmp","type":"compaction","encrypted_content":"opaque"})],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 8,
                    context_fingerprint: context_fingerprint(&spec, &context).unwrap(),
                },
            },
        };
        context.provider_context = vec![native.clone(), native.clone()];
        assert!(convert_input(&spec, &context).is_err());

        context.provider_context = vec![native.clone()];
        context
            .memory_blocks
            .push(crate::provider::types::MemoryBlock {
                layer: MemoryLayer::L1,
                text: "memory".into(),
                time_range: None,
            });
        assert!(convert_input(&spec, &context).is_err());
        context.memory_blocks.clear();

        if let ProviderContextPayload::OpenAiCompactedWindow { coverage, .. } =
            &mut context.provider_context[0].payload
        {
            coverage.context_fingerprint = "wrong".into();
        }
        assert!(convert_input(&spec, &context).is_err());

        context.provider_context = vec![native];
        context.messages = vec![persisted_user(9), persisted_user(9)];
        assert!(convert_input(&spec, &context).is_err());
        context.messages = vec![persisted_user(9), persisted_user(8)];
        assert!(convert_input(&spec, &context).is_err());
        context.messages = vec![
            persisted_user(9),
            ContextMessage::Synthetic {
                message: Message::User(UserMessage {
                    content: vec![],
                    timestamp: Utc::now(),
                }),
            },
        ];
        assert!(convert_input(&spec, &context).is_err());
    }

    #[test]
    fn canonical_replay_recursively_validates_reasoning_and_tool_output_unions() {
        for invalid in [
            json!({"id":"r","type":"reasoning","summary":[{"type":"future","text":"x"}]}),
            json!({"id":"r","type":"reasoning","summary":[{"type":"summary_text","text":1}]}),
            json!({"id":"r","type":"reasoning","summary":[],"content":[{"type":"future","text":"x"}]}),
            json!({"id":"r","type":"reasoning","summary":[],"content":[{"type":"reasoning_text","text":1}]}),
            json!({"type":"function_call_output","call_id":"c","output":[{"type":"future","text":"x"}]}),
            json!({"type":"function_call_output","call_id":"c","output":[{"type":"input_text","text":1}]}),
        ] {
            assert!(validate_canonical_item(&invalid).is_err(), "{invalid}");
        }
        assert!(
            validate_canonical_item(&json!({
                "id":"r",
                "type":"reasoning",
                "summary":[{"type":"summary_text","text":"safe"}],
                "content":[{"type":"reasoning_text","text":"reason"}],
            }))
            .is_ok()
        );
    }

    #[test]
    fn reasoning_summary_slots_are_independent_from_wire_output_indexes() {
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        let payloads = [
            r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"m","type":"message","role":"assistant","content":[]}}"#,
            r#"{"type":"response.output_item.done","sequence_number":1,"output_index":0,"item":{"id":"m","type":"message","role":"assistant","content":[]}}"#,
            r#"{"type":"response.output_item.added","sequence_number":2,"output_index":1,"item":{"id":"r1","type":"reasoning","summary":[]}}"#,
            r#"{"type":"response.reasoning_summary_part.added","sequence_number":3,"item_id":"r1","output_index":1,"summary_index":0,"part":{"type":"summary_text","text":""}}"#,
            r#"{"type":"response.reasoning_summary_text.delta","sequence_number":4,"item_id":"r1","output_index":1,"summary_index":0,"delta":"one"}"#,
            r#"{"type":"response.reasoning_summary_text.done","sequence_number":5,"item_id":"r1","output_index":1,"summary_index":0,"text":"one"}"#,
            r#"{"type":"response.reasoning_summary_part.done","sequence_number":6,"item_id":"r1","output_index":1,"summary_index":0,"part":{"type":"summary_text","text":"one"}}"#,
            r#"{"type":"response.output_item.done","sequence_number":7,"output_index":1,"item":{"id":"r1","type":"reasoning","summary":[{"type":"summary_text","text":"one"}]}}"#,
            r#"{"type":"response.output_item.added","sequence_number":8,"output_index":2,"item":{"id":"r2","type":"reasoning","summary":[]}}"#,
            r#"{"type":"response.reasoning_summary_part.added","sequence_number":9,"item_id":"r2","output_index":2,"summary_index":0,"part":{"type":"summary_text","text":""}}"#,
            r#"{"type":"response.reasoning_summary_text.delta","sequence_number":10,"item_id":"r2","output_index":2,"summary_index":0,"delta":"two"}"#,
        ];
        let events = payloads
            .iter()
            .flat_map(|payload| state.push_json(payload).unwrap().events)
            .filter_map(|event| match event {
                ProviderEvent::ReasoningSummaryStart { content_index } => Some(content_index),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(events, vec![0, 1]);
    }
}
