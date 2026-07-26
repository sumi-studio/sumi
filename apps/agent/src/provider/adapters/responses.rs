use std::collections::{BTreeMap, HashMap, HashSet};

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
        ApiProtocol, AssistantContent, AssistantMessage, ContextMessage, MemoryLayer, Message,
        NativeCompactionCoverage, PromptContext, ProviderContextAnchor, ProviderContextFragment,
        ProviderContextItem, ProviderContextPayload, ProviderEvent, StopReason, ToolDefinition,
        Usage, UserContent, UserMessage,
    },
};

#[derive(Debug, Error)]
pub enum ResponsesAdapterError {
    #[error("model protocol/compat variant is not OpenAI Responses")]
    UnsupportedProtocol,
    #[error("max_output_tokens must be within 1..={max}, got {requested}")]
    InvalidMaxTokens { requested: u64, max: u64 },
    #[error("temperature must be finite, got {0}")]
    InvalidTemperature(f64),
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
    if let Some(temperature) = options.temperature
        && !temperature.is_finite()
    {
        return Err(ResponsesAdapterError::InvalidTemperature(temperature));
    }
    let mut request = Map::new();
    request.insert("model".to_owned(), json!(spec.id));
    request.insert("instructions".to_owned(), json!(context.system_prompt));
    request.insert(
        "input".to_owned(),
        Value::Array(convert_input(spec, context, options.native_compaction)?),
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

pub(in crate::provider) fn build_replay_probe_request(
    fragment: Option<&Value>,
) -> Result<Value, ResponsesAdapterError> {
    build_replay_probe_request_with_usage(fragment, Usage::default())
}

fn build_replay_probe_request_with_usage(
    fragment: Option<&Value>,
    usage: Usage,
) -> Result<Value, ResponsesAdapterError> {
    let spec = ModelSpec::preset("openai-responses")
        .expect("the V1 Responses replay probe preset is built in");
    let anchor = ProviderContextAnchor {
        message_id: "replay-probe-v1-assistant".into(),
        message_seq: 1,
    };
    let origin = spec.origin();
    let mut provider_context = vec![ProviderContextItem {
        origin_message: Some(anchor.clone()),
        wire_item_index: Some(0),
        ordinal: 0,
        provider_origin: origin.clone(),
        payload: ProviderContextPayload::EncryptedReasoning {
            protocol: ApiProtocol::OpenAiResponses,
            item: json!({
                "type":"reasoning",
                "id":"rs_replay_probe_v1_sentinel",
                "encrypted_content":"replay-probe-v1-sentinel",
                "summary":[],
            }),
        },
    }];
    if let Some(fragment) = fragment {
        provider_context.push(ProviderContextItem {
            origin_message: Some(anchor),
            wire_item_index: Some(1),
            ordinal: 0,
            provider_origin: origin,
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::OpenAiResponses,
                item: fragment.clone(),
            },
        });
    }
    let context = PromptContext {
        system_prompt: "replay-probe-v1".into(),
        memory_blocks: Vec::new(),
        messages: vec![
            ContextMessage::Persisted {
                id: "replay-probe-v1-assistant".into(),
                seq: 1,
                message: Message::Assistant(AssistantMessage {
                    content: Vec::new(),
                    model: spec.id.clone(),
                    provider: spec.provider.clone(),
                    origin: spec.origin(),
                    usage,
                    stop_reason: StopReason::Stop,
                    error_message: None,
                    provider_code: None,
                    interrupted: false,
                    timestamp: Utc::now(),
                }),
            },
            ContextMessage::Persisted {
                id: "replay-probe-v1-user".into(),
                seq: 2,
                message: Message::User(UserMessage {
                    content: vec![UserContent::Text {
                        text: "replay-probe-v1-user".into(),
                    }],
                    timestamp: Utc::now(),
                }),
            },
        ],
        provider_context,
        tools: Vec::new(),
    };
    build_request(&spec, &context, &RequestOptions::default())
}

#[cfg(test)]
pub(in crate::provider) fn build_replay_probe_request_for_usage_test(
    fragment: Option<&Value>,
    usage: Usage,
) -> Result<Value, ResponsesAdapterError> {
    build_replay_probe_request_with_usage(fragment, usage)
}

pub(in crate::provider) fn validate_replay_native_items(
    items: &[Value],
) -> Result<(), ResponsesAdapterError> {
    if items.is_empty() {
        return Err(ResponsesAdapterError::InvalidContext(
            "native compacted window must not be empty".into(),
        ));
    }
    items.iter().try_for_each(|item| {
        validate_canonical_item(item).map_err(ResponsesAdapterError::InvalidContext)
    })
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
    request.insert(
        "input".into(),
        Value::Array(convert_input(spec, context, true)?),
    );
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
    let through_message_seq =
        crate::provider::types::validate_native_suffix(&context.messages, None)
            .map_err(ResponsesAdapterError::InvalidContext)?
            .ok_or_else(|| {
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
    // OpenAI Responses attempts strict generation by default. Explicitly
    // disable it when the tool schema does not satisfy OpenAI's strict subset;
    // this keeps the provider's fallback behavior explicit in the request.
    let strict = is_openai_strict_safe(&tool.parameters);
    json!({
        "type": "function",
        "name": tool.name,
        "description": tool.description,
        "parameters": tool.parameters,
        "strict": strict,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpenAiSchemaType {
    Object,
    Array,
    String,
    Integer,
    Number,
    Boolean,
    Null,
}

#[derive(Default)]
struct OpenAiSchemaValidationState<'a> {
    definitions: Option<&'a Map<String, Value>>,
    active_refs: HashSet<String>,
    validated_refs: HashSet<String>,
    property_count: usize,
    enum_values: usize,
    aggregate_string_length: usize,
}

/// Proves that a tool schema can be sent with `strict: true` to OpenAI
/// Responses. OpenAI's strict function schemas require every object to opt
/// out of additional properties and every declared property to be required.
/// It also enforces the documented 10-level object, 1000-enum, 120,000-string,
/// and 15,000-long-string-enum limits. The remaining checks intentionally
/// retain the conservative schema subset used by the Chat adapter; a false
/// result only opts into best-effort calls.
fn is_openai_strict_safe(schema: &Value) -> bool {
    let Some(root) = schema.as_object() else {
        return false;
    };
    let definitions = match root.get("$defs") {
        Some(value) => {
            let Some(definitions) = value.as_object() else {
                return false;
            };
            Some(definitions)
        }
        None => None,
    };
    if definitions.is_some_and(|definitions| {
        definitions
            .keys()
            .any(|name| name.is_empty() || name.contains('/') || name.contains('~'))
    }) {
        return false;
    }
    let mut state = OpenAiSchemaValidationState {
        definitions,
        ..Default::default()
    };
    if definitions.is_some_and(|definitions| {
        definitions
            .keys()
            .any(|name| !add_openai_string_budget(&mut state, name))
    }) {
        return false;
    }
    if !validate_openai_schema(schema, true, 1, 0, true, &mut state) {
        return false;
    }
    state.property_count <= 2_048
        && definitions.is_none_or(|definitions| {
            definitions.iter().all(|(name, definition)| {
                if state.validated_refs.contains(name) {
                    true
                } else {
                    let valid = validate_openai_schema(definition, false, 2, 0, true, &mut state);
                    if valid {
                        state.validated_refs.insert(name.clone());
                    }
                    valid
                }
            })
        })
}

fn validate_openai_schema(
    schema: &Value,
    root: bool,
    depth: usize,
    object_nesting: usize,
    count_limits: bool,
    state: &mut OpenAiSchemaValidationState<'_>,
) -> bool {
    if depth > 30 {
        return false;
    }
    let Some(object) = schema.as_object() else {
        return false;
    };
    if object.contains_key("default") {
        return false;
    }
    if object
        .get("description")
        .is_some_and(|value| !value.is_string())
        || object.get("title").is_some_and(|value| !value.is_string())
        || object.get("$id").is_some_and(|value| !value.is_string())
        || (!root && (object.contains_key("$defs") || object.contains_key("$id")))
    {
        return false;
    }

    if let Some(reference) = object.get("$ref") {
        if root
            || object
                .keys()
                .any(|key| !matches!(key.as_str(), "$ref" | "description" | "title"))
        {
            return false;
        }
        let Some(name) = reference
            .as_str()
            .and_then(|reference| reference.strip_prefix("#/$defs/"))
            .filter(|name| !name.is_empty() && !name.contains('/') && !name.contains('~'))
        else {
            return false;
        };
        let Some(definitions) = state.definitions else {
            return false;
        };
        if !definitions.contains_key(name) {
            return false;
        }
        if state.validated_refs.contains(name) {
            return validate_openai_schema(
                definitions.get(name).expect("checked definition presence"),
                false,
                depth + 1,
                object_nesting,
                false,
                state,
            );
        }
        if !state.active_refs.insert(name.to_owned()) {
            return true;
        }
        let valid = validate_openai_schema(
            definitions.get(name).expect("checked definition presence"),
            false,
            depth + 1,
            object_nesting,
            count_limits,
            state,
        );
        state.active_refs.remove(name);
        if valid {
            state.validated_refs.insert(name.to_owned());
        }
        return valid;
    }

    if let Some(any_of) = object.get("anyOf") {
        if root
            || object
                .keys()
                .any(|key| !matches!(key.as_str(), "anyOf" | "description" | "title"))
        {
            return false;
        }
        let Some(branches) = any_of.as_array() else {
            return false;
        };
        return !branches.is_empty()
            && branches.iter().all(|branch| {
                validate_openai_schema(
                    branch,
                    false,
                    depth + 1,
                    object_nesting,
                    count_limits,
                    state,
                )
            });
    }

    let Some((types, primary_type)) = openai_schema_types(object.get("type")) else {
        return false;
    };
    if root && (primary_type != OpenAiSchemaType::Object || types.len() != 1) {
        return false;
    }
    if object
        .keys()
        .any(|key| !openai_keyword_allowed(key, primary_type, root))
    {
        return false;
    }

    match primary_type {
        OpenAiSchemaType::Object => {
            let object_nesting = object_nesting.saturating_add(1);
            if object_nesting > 10 {
                return false;
            }
            if object.get("additionalProperties") != Some(&Value::Bool(false)) {
                return false;
            }
            let properties = match object.get("properties") {
                Some(value) => {
                    let Some(properties) = value.as_object() else {
                        return false;
                    };
                    Some(properties)
                }
                None => None,
            };
            if properties.is_some_and(|properties| {
                properties.keys().any(|name| {
                    name.contains('/')
                        || matches!(
                            name.as_str(),
                            "$defs" | "$ref" | "anyOf" | "required" | "additionalProperties"
                        )
                })
            }) {
                return false;
            }
            if count_limits
                && properties.is_some_and(|properties| {
                    properties
                        .keys()
                        .any(|name| !add_openai_string_budget(state, name))
                })
            {
                return false;
            }
            let property_count = properties.map_or(0, Map::len);
            if count_limits {
                state.property_count = state.property_count.saturating_add(property_count);
                if state.property_count > 2_048 {
                    return false;
                }
            }
            if properties.is_some_and(|properties| {
                properties.values().any(|property| {
                    !validate_openai_schema(
                        property,
                        false,
                        depth + 1,
                        object_nesting,
                        count_limits,
                        state,
                    )
                })
            }) {
                return false;
            }

            let Some(required) = object.get("required").and_then(Value::as_array) else {
                return false;
            };
            let mut seen = HashSet::new();
            if required.len() != property_count
                || required.iter().any(|value| {
                    !value.as_str().is_some_and(|name| {
                        properties.is_some_and(|properties| properties.contains_key(name))
                            && seen.insert(name.to_owned())
                    })
                })
            {
                return false;
            }
        }
        OpenAiSchemaType::Array => {
            if let Some(items) = object.get("items")
                && !validate_openai_schema(
                    items,
                    false,
                    depth + 1,
                    object_nesting,
                    count_limits,
                    state,
                )
            {
                return false;
            }
            if !valid_openai_u64_bounds(object, "minItems", "maxItems") {
                return false;
            }
        }
        OpenAiSchemaType::String => {
            if !valid_openai_u64_bounds(object, "minLength", "maxLength") {
                return false;
            }
        }
        OpenAiSchemaType::Integer | OpenAiSchemaType::Number => {
            if !valid_openai_number_bounds(object, "minimum", "maximum") {
                return false;
            }
        }
        OpenAiSchemaType::Boolean | OpenAiSchemaType::Null => {}
    }

    if count_limits
        && object
            .get("enum")
            .is_some_and(|values| !record_openai_enum(values, &types, primary_type, state))
    {
        return false;
    }
    if count_limits
        && object.get("const").is_some_and(|value| {
            !openai_value_matches(value, &types)
                || value
                    .as_str()
                    .is_some_and(|value| !add_openai_string_budget(state, value))
        })
    {
        return false;
    }
    true
}

fn openai_schema_types(value: Option<&Value>) -> Option<(Vec<OpenAiSchemaType>, OpenAiSchemaType)> {
    let raw = value?;
    let names = match raw {
        Value::String(name) => vec![name.as_str()],
        Value::Array(names) if !names.is_empty() => names
            .iter()
            .map(Value::as_str)
            .collect::<Option<Vec<_>>>()?,
        _ => return None,
    };
    let mut types = Vec::with_capacity(names.len());
    for name in names {
        let kind = match name {
            "object" => OpenAiSchemaType::Object,
            "array" => OpenAiSchemaType::Array,
            "string" => OpenAiSchemaType::String,
            "integer" => OpenAiSchemaType::Integer,
            "number" => OpenAiSchemaType::Number,
            "boolean" => OpenAiSchemaType::Boolean,
            "null" => OpenAiSchemaType::Null,
            _ => return None,
        };
        if types.contains(&kind) {
            return None;
        }
        types.push(kind);
    }
    let non_null = types
        .iter()
        .copied()
        .filter(|kind| *kind != OpenAiSchemaType::Null)
        .collect::<Vec<_>>();
    let primary = match non_null.as_slice() {
        [primary] if types.len() <= 2 => *primary,
        [] if types.as_slice() == [OpenAiSchemaType::Null] => OpenAiSchemaType::Null,
        _ => return None,
    };
    Some((types, primary))
}

fn openai_keyword_allowed(key: &str, kind: OpenAiSchemaType, root: bool) -> bool {
    if matches!(key, "type" | "description" | "title" | "enum" | "const")
        || (root && matches!(key, "$defs" | "$id"))
    {
        return true;
    }
    match kind {
        OpenAiSchemaType::Object => {
            matches!(key, "properties" | "required" | "additionalProperties")
        }
        OpenAiSchemaType::Array => matches!(key, "items" | "minItems" | "maxItems"),
        OpenAiSchemaType::String => matches!(key, "minLength" | "maxLength"),
        OpenAiSchemaType::Integer | OpenAiSchemaType::Number => {
            matches!(key, "minimum" | "maximum")
        }
        OpenAiSchemaType::Boolean | OpenAiSchemaType::Null => false,
    }
}

fn valid_openai_u64_bounds(object: &Map<String, Value>, minimum: &str, maximum: &str) -> bool {
    let min = object.get(minimum).map(Value::as_u64);
    let max = object.get(maximum).map(Value::as_u64);
    min.flatten()
        .zip(max.flatten())
        .is_none_or(|(min, max)| min <= max)
        && min.is_none_or(|value| value.is_some())
        && max.is_none_or(|value| value.is_some())
}

fn valid_openai_number_bounds(object: &Map<String, Value>, minimum: &str, maximum: &str) -> bool {
    let min = object
        .get(minimum)
        .map(|value| value.as_f64().filter(|value| value.is_finite()));
    let max = object
        .get(maximum)
        .map(|value| value.as_f64().filter(|value| value.is_finite()));
    min.flatten()
        .zip(max.flatten())
        .is_none_or(|(min, max)| min <= max)
        && min.is_none_or(|value| value.is_some())
        && max.is_none_or(|value| value.is_some())
}

fn add_openai_string_budget(state: &mut OpenAiSchemaValidationState<'_>, value: &str) -> bool {
    state.aggregate_string_length = state
        .aggregate_string_length
        .saturating_add(value.chars().count());
    state.aggregate_string_length <= 120_000
}

fn record_openai_enum(
    values: &Value,
    types: &[OpenAiSchemaType],
    primary_type: OpenAiSchemaType,
    state: &mut OpenAiSchemaValidationState<'_>,
) -> bool {
    let Some(values) = values.as_array() else {
        return false;
    };
    if values.is_empty() {
        return false;
    }
    state.enum_values = state.enum_values.saturating_add(values.len());
    if state.enum_values > 1_000
        || (primary_type == OpenAiSchemaType::String
            && values.len() > 250
            && values
                .iter()
                .filter_map(Value::as_str)
                .map(|value| value.chars().count())
                .sum::<usize>()
                > 15_000)
    {
        return false;
    }
    values.iter().all(|value| {
        openai_value_matches(value, types)
            && value
                .as_str()
                .is_none_or(|value| add_openai_string_budget(state, value))
    })
}

fn openai_value_matches(value: &Value, types: &[OpenAiSchemaType]) -> bool {
    types.iter().any(|kind| match kind {
        OpenAiSchemaType::Object => value.is_object(),
        OpenAiSchemaType::Array => value.is_array(),
        OpenAiSchemaType::String => value.is_string(),
        OpenAiSchemaType::Integer => value
            .as_number()
            .is_some_and(|number| number.is_i64() || number.is_u64()),
        OpenAiSchemaType::Number => value.is_number(),
        OpenAiSchemaType::Boolean => value.is_boolean(),
        OpenAiSchemaType::Null => value.is_null(),
    })
}

fn convert_input(
    spec: &ModelSpec,
    context: &PromptContext,
    native_compaction: bool,
) -> Result<Vec<Value>, ResponsesAdapterError> {
    let compat = ensure_responses_spec(spec)?;
    let replay_encrypted_reasoning = spec.reasoning && compat.supports_encrypted_reasoning;
    let mut output = Vec::new();
    let has_foreign_native = context.provider_context.iter().any(|item| {
        matches!(
            item.payload,
            ProviderContextPayload::AnthropicCompaction { .. }
        )
    });
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
    let native = if native_compaction && compat.supports_native_compact && !has_foreign_native {
        match prepare_native_window(spec, context, &compacted) {
            Ok(native) => native,
            Err(error) => {
                tracing::warn!(reason = %error, "discarded stale Responses native context");
                None
            }
        }
    } else {
        None
    };
    let (coverage_seq, mut native_items) = native
        .map(|(coverage, items)| (Some(coverage), Some(items)))
        .unwrap_or((None, None));
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
            // Native context from another protocol is stale by definition. It is
            // discarded while the durable public transcript is rebuilt below.
            ProviderContextPayload::AnthropicCompaction { .. } => {}
            ProviderContextPayload::EncryptedReasoning { .. } => {
                if !replay_encrypted_reasoning {
                    // Encrypted reasoning is only replayable when both the model
                    // request asks for reasoning and the compat capability is on.
                    // Otherwise fall back to the durable public transcript view.
                    continue;
                }
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

                let mut public = BTreeMap::<u32, &AssistantContent>::new();
                for content in &assistant.content {
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
                    if public.insert(wire, content).is_some()
                        || opaque.keys().any(|(opaque_wire, _)| *opaque_wire == wire)
                    {
                        return Err(ResponsesAdapterError::InvalidContext(
                            "duplicate or ambiguous Responses wire_item_index".into(),
                        ));
                    }
                }
                let mut opaque_iter = opaque.into_iter().peekable();
                for (wire, content) in public {
                    while opaque_iter
                        .peek()
                        .is_some_and(|((index, _), _)| *index < wire)
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
                            "content":[{"type":"output_text","text":text,"annotations":[],"logprobs":[]}],
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
                for (_, item) in opaque_iter {
                    output.push(item);
                }
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

fn prepare_native_window(
    spec: &ModelSpec,
    context: &PromptContext,
    compacted: &[(&Vec<Value>, &NativeCompactionCoverage)],
) -> Result<Option<(u64, Vec<Value>)>, String> {
    let Some((items, coverage)) = compacted.first().copied() else {
        return Ok(None);
    };
    if compacted.len() != 1 {
        return Err("multiple OpenAI native compacted windows".into());
    }
    let native_item = context
        .provider_context
        .iter()
        .find(|item| {
            matches!(
                item.payload,
                ProviderContextPayload::OpenAiCompactedWindow { .. }
            )
        })
        .expect("compacted entry came from provider_context");
    if native_item.origin_message.is_some() || native_item.wire_item_index.is_some() {
        return Err("native compacted window has reasoning placement metadata".into());
    }
    if coverage.through_message_seq == 0 {
        return Err("native compacted window coverage must be greater than zero".into());
    }
    if !context.memory_blocks.is_empty() {
        return Err("native compacted window cannot coexist with memory blocks".into());
    }
    if coverage.context_fingerprint
        != context_fingerprint(spec, context).map_err(|error| error.to_string())?
    {
        return Err("native compacted window context fingerprint mismatch".into());
    }
    if items.is_empty() {
        return Err("native compacted window must not be empty".into());
    }
    let mut validated = Vec::with_capacity(items.len());
    for item in items {
        validate_canonical_item(item)
            .map_err(|error| format!("invalid native compacted window item: {error}"))?;
        validated.push(item.clone());
    }
    let _ = crate::provider::types::validate_native_suffix(
        &context.messages,
        Some(coverage.through_message_seq),
    )
    .map_err(|error| error.to_string())?;
    Ok(Some((coverage.through_message_seq, validated)))
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
        id: Option<String>,
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
    output_identities: BTreeMap<u32, (Option<String>, String)>,
    completed_items: BTreeMap<u32, Value>,
    next_output_index: u32,
    next_summary_slot: usize,
    next_sequence_number: u64,
    output_item_ids: HashSet<String>,
    function_call_ids: HashSet<String>,
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

#[derive(Clone, Debug)]
struct ResponsesStateSnapshot {
    response_id: Option<String>,
    response_model: Option<String>,
    usage: Usage,
    content_bytes: usize,
    event_count: usize,
    preview_work_bytes: usize,
    reasoning_fragments: Vec<(String, ProviderContextFragment)>,
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
            output_item_ids: HashSet::new(),
            function_call_ids: HashSet::new(),
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

    fn snapshot(&self) -> ResponsesStateSnapshot {
        ResponsesStateSnapshot {
            response_id: self.response_id.clone(),
            response_model: self.response_model.clone(),
            usage: self.usage.clone(),
            content_bytes: self.content_bytes,
            event_count: self.event_count,
            preview_work_bytes: self.preview_work_bytes,
            reasoning_fragments: self.reasoning_fragments.clone(),
        }
    }

    fn restore(&mut self, snapshot: ResponsesStateSnapshot) {
        self.response_id = snapshot.response_id;
        self.response_model = snapshot.response_model;
        self.usage = snapshot.usage;
        self.content_bytes = snapshot.content_bytes;
        self.event_count = snapshot.event_count;
        self.preview_work_bytes = snapshot.preview_work_bytes;
        self.reasoning_fragments = snapshot.reasoning_fragments;
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
        let id = match item_type {
            "function_call" => optional_non_null_string(item, "id")?,
            _ => Some(required_str(item, "id")?),
        };
        if id.is_some_and(|id| self.output_item_ids.contains(id)) {
            return Err(ResponsesAdapterError::InvalidEvent(format!(
                "duplicate output item id {}",
                id.expect("checked present")
            )));
        }
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
                    id: id.expect("message id required").to_owned(),
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
                let call_id = required_str(item, "call_id")?;
                if self.function_call_ids.contains(call_id) {
                    return Err(ResponsesAdapterError::InvalidEvent(
                        "duplicate function_call call_id".into(),
                    ));
                }
                OutputSlot::Tool {
                    id: id.map(str::to_owned),
                    call_id: call_id.to_owned(),
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
                validate_reasoning_content(item).map_err(ResponsesAdapterError::InvalidEvent)?;
                let _ = optional_encrypted_content(item)?;
                let summary_slot = self.next_summary_slot;
                next_summary_slot = Some(summary_slot.checked_add(1).ok_or_else(|| {
                    ResponsesAdapterError::InvalidEvent("summary slot exceeds usize".into())
                })?);
                OutputSlot::Reasoning {
                    id: id.expect("reasoning id required").to_owned(),
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
        if let Some(id) = id {
            self.output_item_ids.insert(id.to_owned());
        }
        if let OutputSlot::Tool { call_id, .. } = &slot {
            self.function_call_ids.insert(call_id.clone());
        }
        self.tool_count = tool_count;
        self.saw_tool |= is_tool;
        self.output_identities
            .insert(index, (id.map(str::to_owned), item_type.to_owned()));
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
        validate_reasoning_summary_part_event(object)?;
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
        validate_reasoning_summary_text_event(object)?;
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
        validate_reasoning_summary_part_event(object)?;
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
                if optional_non_null_string(item, "id")? != id.as_deref()
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
                validate_reasoning_content(item).map_err(ResponsesAdapterError::InvalidEvent)?;
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
                let encrypted = optional_encrypted_content(item)?;
                self.commit_charges(encrypted.map_or(0, str::len), usize::from(started), 0)?;
                self.completed_items
                    .insert(index, Value::Object(item.clone()));
                self.slots.remove(&index);
                if encrypted.is_some() {
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
        let response = object
            .get("response")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                ResponsesAdapterError::InvalidEvent("response must be an object".into())
            })?;
        // Usage is an independently received accounting sideband. Once its
        // schema and invariants validate, retain it even if the semantic
        // terminal payload fails validation below.
        let usage = parse_terminal_usage(response.get("usage"), &self.usage)?;
        let snapshot = self.snapshot();
        let result = (|| -> Result<ResponsesPush, ResponsesAdapterError> {
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
            let status = required_str(response, "status")?;
            self.validate_terminal_output(response)?;
            let incomplete_reason = response
                .get("incomplete_details")
                .and_then(Value::as_object)
                .and_then(|details| details.get("reason"))
                .and_then(Value::as_str);
            let reason = match status {
                "completed" if self.saw_tool => StopReason::ToolUse,
                "completed" => StopReason::Stop,
                "incomplete" => match incomplete_reason {
                    Some("max_output_tokens" | "max_tokens") => StopReason::Length,
                    Some("content_filter") => StopReason::Error,
                    Some(other) => {
                        return Err(ResponsesAdapterError::InvalidEvent(format!(
                            "unsupported incomplete response reason {other}"
                        )));
                    }
                    None => {
                        return Err(ResponsesAdapterError::InvalidEvent(
                            "incomplete response is missing incomplete_details.reason".into(),
                        ));
                    }
                },
                other => {
                    return Err(ResponsesAdapterError::InvalidEvent(format!(
                        "terminal event has unsupported status {other}"
                    )));
                }
            };
            let (error_message, provider_code) = if reason == StopReason::Error {
                let details = response
                    .get("incomplete_details")
                    .and_then(Value::as_object);
                let error = response.get("error").and_then(Value::as_object);
                (
                    details
                        .and_then(|details| details.get("message"))
                        .and_then(Value::as_str)
                        .or_else(|| {
                            error
                                .and_then(|error| error.get("message"))
                                .and_then(Value::as_str)
                        })
                        .unwrap_or("Responses response was filtered by content policy")
                        .to_owned(),
                    details
                        .and_then(|details| details.get("code"))
                        .and_then(Value::as_str)
                        .or_else(|| {
                            error
                                .and_then(|error| error.get("code"))
                                .and_then(Value::as_str)
                        })
                        .unwrap_or("content_filter")
                        .to_owned(),
                )
            } else {
                (String::new(), String::new())
            };
            let (provider_context, opaque_bytes) =
                backfilled_reasoning_fragments(response, &self.reasoning_fragments)?;
            let mut events = Vec::new();
            self.commit_charges(opaque_bytes, self.slots.len().saturating_add(1), 0)?;
            self.reasoning_fragments = provider_context;
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
            self.usage = usage.clone();
            Ok(ResponsesPush {
                events: Vec::new(),
                terminal: Some(ResponsesTerminal {
                    events,
                    reason,
                    usage: self.usage.clone(),
                    error_message: (reason == StopReason::Error).then_some(error_message),
                    provider_code: if reason == StopReason::Error {
                        Some(provider_code)
                    } else if reason == StopReason::Length {
                        incomplete_reason.map(str::to_owned)
                    } else {
                        None
                    },
                    provider_context: self.provider_context(),
                }),
            })
        })();
        if result.is_err() {
            self.restore(snapshot);
            self.usage = usage.clone();
        }
        result
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
            Some((id, kind)) if id.as_deref().is_none_or(|id| id == item_id)
                && kind == expected_type
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
        if self.completed_items.len() != self.output_identities.len() {
            return Err(ResponsesAdapterError::InvalidEvent(
                "terminal response output is missing or reordered".into(),
            ));
        }
        // The ChatGPT Codex Responses endpoint sends each canonical item through
        // output_item.done, then deliberately omits the repeated terminal copy.
        // An empty terminal output is therefore complete only when every
        // observed identity already has a validated item.done record.
        if output.is_empty() {
            return Ok(());
        }
        if output.len() != self.completed_items.len() {
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
            if optional_non_null_string(item, "id")? != id.as_deref()
                || required_str(item, "type")? != kind
            {
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
        let response = object
            .get("response")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                ResponsesAdapterError::InvalidEvent("response must be an object".into())
            })?;
        let usage = parse_terminal_usage(response.get("usage"), &self.usage)?;
        let snapshot = self.snapshot();
        let result = (|| -> Result<ResponsesPush, ResponsesAdapterError> {
            self.observe_response_identity(object)?;
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
            self.usage = usage.clone();
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
        })();
        if result.is_err() {
            self.restore(snapshot);
            self.usage = usage;
        }
        result
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
        let model = response
            .get("model")
            .map(|model| {
                model
                    .as_str()
                    .filter(|model| !model.is_empty())
                    .ok_or_else(|| {
                        ResponsesAdapterError::InvalidEvent(
                            "response.model must be a non-empty string when present".into(),
                        )
                    })
            })
            .transpose()?;
        if self.response_id.as_deref().is_some_and(|known| known != id) {
            return Err(ResponsesAdapterError::InvalidEvent(
                "response identity changed during stream".into(),
            ));
        }
        if let Some(model) = model
            && self
                .response_model
                .as_deref()
                .is_some_and(|known| known != model)
        {
            return Err(ResponsesAdapterError::InvalidEvent(
                "response identity changed during stream".into(),
            ));
        }
        let new_id = self.response_id.is_none();
        let new_model = self.response_model.is_none().then_some(model).flatten();
        let identity_charge = (if new_id { id.len() } else { 0 })
            .checked_add(new_model.map_or(0, str::len))
            .ok_or(ResponsesAdapterError::ResponseLimitExceeded {
                resource: "content_bytes",
                limit: self.budget.max_content_bytes,
            })?;
        self.commit_charges(identity_charge, 0, 0)?;
        if new_id {
            self.response_id = Some(id.to_owned());
        }
        if let Some(model) = new_model {
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

fn optional_non_null_string<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<Option<&'a str>, ResponsesAdapterError> {
    match object.get(field) {
        None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(ResponsesAdapterError::InvalidEvent(format!(
            "{field} must be a string when present"
        ))),
    }
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
    let terminal_encrypted = optional_encrypted_content(terminal)?;
    let completed_encrypted = optional_encrypted_content(completed)?;
    if terminal_encrypted.is_none() || completed_encrypted.is_some() {
        return Ok(false);
    }
    let mut terminal_without_backfill = terminal.clone();
    terminal_without_backfill.remove("encrypted_content");
    Ok(terminal_without_backfill == *completed)
}

fn optional_encrypted_content(
    object: &Map<String, Value>,
) -> Result<Option<&str>, ResponsesAdapterError> {
    match object.get("encrypted_content") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.is_empty() => Ok(Some(value)),
        Some(_) => Err(ResponsesAdapterError::InvalidEvent(
            "encrypted_content must be a non-empty string when present".into(),
        )),
    }
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

fn validate_reasoning_summary_part_event(
    object: &Map<String, Value>,
) -> Result<(), ResponsesAdapterError> {
    let part = object
        .get("part")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            ResponsesAdapterError::InvalidEvent("reasoning summary part must be an object".into())
        })?;
    if required_str(part, "type")? != "summary_text" {
        return Err(ResponsesAdapterError::InvalidEvent(
            "unsupported known reasoning summary part variant".into(),
        ));
    }
    let _ = string_field(part, "text")?;
    Ok(())
}

fn validate_reasoning_summary_text_event(
    object: &Map<String, Value>,
) -> Result<(), ResponsesAdapterError> {
    if !object.get("text").is_some_and(Value::is_string) {
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
    let input_total = required_u64(usage, "input_tokens")?;
    let output = required_u64(usage, "output_tokens")?;
    let total_tokens = required_u64(usage, "total_tokens")?;
    let cache_read = optional_usage_detail(usage, "input_tokens_details", "cached_tokens")?;
    let cache_write = 0;
    let reasoning = optional_usage_detail(usage, "output_tokens_details", "reasoning_tokens")?;
    let uncached_input = input_total.checked_sub(cache_read).ok_or_else(|| {
        ResponsesAdapterError::InvalidEvent("cached tokens exceed input_tokens".into())
    })?;
    if reasoning > output {
        return Err(ResponsesAdapterError::InvalidEvent(
            "reasoning_tokens exceeds output_tokens".into(),
        ));
    }
    let expected_total = input_total.checked_add(output).ok_or_else(|| {
        ResponsesAdapterError::InvalidEvent("usage token total exceeds u64".into())
    })?;
    if total_tokens != expected_total {
        return Err(ResponsesAdapterError::InvalidEvent(
            "total_tokens must equal input_tokens + output_tokens".into(),
        ));
    }
    Ok(Usage {
        input: uncached_input,
        output,
        cache_read,
        cache_write,
        reasoning,
        total_tokens,
    })
}

fn parse_terminal_usage(
    value: Option<&Value>,
    current: &Usage,
) -> Result<Usage, ResponsesAdapterError> {
    match value {
        None | Some(Value::Null) => Ok(current.clone()),
        Some(value) => parse_usage(value),
    }
}

fn optional_usage_detail(
    usage: &Map<String, Value>,
    details_field: &str,
    token_field: &str,
) -> Result<u64, ResponsesAdapterError> {
    let Some(details) = usage.get(details_field) else {
        return Ok(0);
    };
    let details = details.as_object().ok_or_else(|| {
        ResponsesAdapterError::InvalidEvent(format!("{details_field} must be an object"))
    })?;
    match details.get(token_field) {
        None => Ok(0),
        Some(value) => value.as_u64().ok_or_else(|| {
            ResponsesAdapterError::InvalidEvent(format!(
                "{details_field}.{token_field} must be a non-negative integer"
            ))
        }),
    }
}

fn required_u64(object: &Map<String, Value>, field: &str) -> Result<u64, ResponsesAdapterError> {
    object.get(field).and_then(Value::as_u64).ok_or_else(|| {
        ResponsesAdapterError::InvalidEvent(format!(
            "{field} must be a present non-negative integer"
        ))
    })
}

fn item_identity_bytes(item: &Map<String, Value>) -> Result<usize, ResponsesAdapterError> {
    let mut total = item.get("id").and_then(Value::as_str).map_or(0, str::len);
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

fn backfilled_reasoning_fragments(
    response: &Map<String, Value>,
    fragments: &[(String, ProviderContextFragment)],
) -> Result<(Vec<(String, ProviderContextFragment)>, usize), ResponsesAdapterError> {
    let output = response
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ResponsesAdapterError::InvalidEvent("response.output must be an array".into())
        })?;
    if output.is_empty() {
        return Ok((fragments.to_vec(), 0));
    }
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
        let encrypted_content = if item_type == "reasoning" {
            optional_encrypted_content(item)?
        } else {
            None
        };
        if let Some(encrypted_content) = encrypted_content
            && encrypted
                .insert(
                    required_str(item, "id")?.to_owned(),
                    (
                        u32::try_from(index).map_err(|_| {
                            ResponsesAdapterError::InvalidEvent(
                                "terminal output index exceeds u32".into(),
                            )
                        })?,
                        item.clone(),
                        encrypted_content.len(),
                    ),
                )
                .is_some()
        {
            return Err(ResponsesAdapterError::InvalidEvent(
                "duplicate encrypted reasoning id in terminal output".into(),
            ));
        }
    }
    let mut result = fragments.to_vec();
    let mut seen = HashSet::new();
    for (id, fragment) in &mut result {
        if !seen.insert(id.clone()) {
            return Err(ResponsesAdapterError::InvalidEvent(
                "duplicate retained reasoning fragment id".into(),
            ));
        }
        let Some((index, item, _)) = encrypted.remove(id) else {
            return Err(ResponsesAdapterError::InvalidEvent(
                "retained encrypted reasoning is orphaned from terminal output".into(),
            ));
        };
        if fragment.wire_item_index != Some(index) {
            return Err(ResponsesAdapterError::InvalidEvent(
                "retained encrypted reasoning placement changed at terminal".into(),
            ));
        }
        fragment.payload = ProviderContextPayload::EncryptedReasoning {
            protocol: ApiProtocol::OpenAiResponses,
            item: Value::Object(item),
        };
    }
    let mut additional_bytes = 0usize;
    for (id, (index, item, encrypted_len)) in encrypted {
        additional_bytes = additional_bytes.checked_add(encrypted_len).ok_or(
            ResponsesAdapterError::ResponseLimitExceeded {
                resource: "content_bytes",
                limit: usize::MAX,
            },
        )?;
        result.push((
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
    result.sort_by_key(|(_, fragment)| fragment.wire_item_index);
    // Multiple persisted opaque fragments may share a wire_item_index as long
    // as their ordinals differ; the request builder enforces the (wire, ordinal)
    // pair, so the terminal backfill does not duplicate that check here.
    Ok((result, additional_bytes))
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
            ensure_only_fields(
                object,
                &["id", "type", "role", "content", "status", "phase"],
                "message",
            )?;
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
                validate_message_content_part(part)?;
            }
            validate_optional_status(object, "message", false)?;
            if let Some(phase) = object.get("phase")
                && !phase.is_null()
                && !matches!(phase.as_str(), Some("commentary" | "final_answer"))
            {
                return Err("message phase is invalid".into());
            }
        }
        "function_call" => {
            ensure_only_fields(
                object,
                &[
                    "arguments",
                    "call_id",
                    "name",
                    "type",
                    "id",
                    "caller",
                    "namespace",
                    "status",
                ],
                "function_call",
            )?;
            if let Some(id) = object.get("id")
                && !id.is_string()
            {
                return Err("function_call id must be a string when present".into());
            }
            for field in ["call_id", "name"] {
                if object
                    .get(field)
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                {
                    return Err(format!("function_call {field} must be a non-empty string"));
                }
            }
            if !object.get("arguments").is_some_and(Value::is_string) {
                return Err("function_call arguments must be a string".into());
            }
            if let Some(namespace) = object.get("namespace")
                && !namespace.is_string()
            {
                return Err("function_call namespace must be a string when present".into());
            }
            validate_optional_status(object, "function_call", false)?;
            validate_optional_caller(object.get("caller"), "function_call", true)?;
        }
        "function_call_output" => {
            ensure_only_fields(
                object,
                &["call_id", "output", "type", "id", "caller", "status"],
                "function_call_output",
            )?;
            if object
                .get("call_id")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            {
                return Err("function_call_output call_id must be a non-empty string".into());
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
            if let Some(id) = object.get("id")
                && !id.is_null()
                && !id.is_string()
            {
                return Err("function_call_output id must be null or a string".into());
            }
            validate_optional_status(object, "function_call_output", true)?;
            validate_optional_caller(object.get("caller"), "function_call_output", true)?;
        }
        "reasoning" => {
            ensure_only_fields(
                object,
                &[
                    "id",
                    "summary",
                    "type",
                    "content",
                    "encrypted_content",
                    "status",
                ],
                "reasoning",
            )?;
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
                ensure_only_fields(part, &["text", "type"], "reasoning summary part")?;
                if part.get("type").and_then(Value::as_str) != Some("summary_text")
                    || !part.get("text").is_some_and(Value::is_string)
                {
                    return Err(
                        "reasoning summary part must be summary_text with string text".into(),
                    );
                }
            }
            validate_reasoning_content(object)?;
            if let Some(encrypted) = object.get("encrypted_content") {
                match encrypted {
                    Value::Null => {}
                    Value::String(encrypted) if !encrypted.is_empty() => {}
                    _ => {
                        return Err(
                            "reasoning encrypted_content must be null or a non-empty string".into(),
                        );
                    }
                }
            }
            validate_optional_status(object, "reasoning", false)?;
        }
        "compaction" => {
            ensure_only_fields(
                object,
                &["encrypted_content", "type", "id", "created_by"],
                "compaction",
            )?;
            if let Some(id) = object.get("id")
                && !id.is_null()
                && !id.is_string()
            {
                return Err("compaction id must be null or a string when present".into());
            }
            if object
                .get("encrypted_content")
                .and_then(Value::as_str)
                .is_none_or(|content| content.is_empty())
            {
                return Err("compaction encrypted_content must be non-empty".into());
            }
            if let Some(created_by) = object.get("created_by")
                && !created_by.is_string()
            {
                return Err("compaction created_by must be a string when present".into());
            }
        }
        other => return Err(format!("unsupported compact output item variant {other}")),
    }
    Ok(())
}

fn validate_message_content_part(part: &Value) -> Result<(), String> {
    let part = part
        .as_object()
        .ok_or_else(|| "message content part must be an object".to_owned())?;
    let part_type = part
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| "message content part type must be a string".to_owned())?;
    match part_type {
        "input_text" => {
            ensure_only_fields(
                part,
                &["type", "text", "prompt_cache_breakpoint"],
                "input_text",
            )?;
            require_string(part, "text", "input_text")?;
            validate_prompt_cache_breakpoint(part.get("prompt_cache_breakpoint"), "input_text")
        }
        "output_text" => {
            ensure_only_fields(
                part,
                &["type", "text", "annotations", "logprobs"],
                "output_text",
            )?;
            require_string(part, "text", "output_text")?;
            validate_annotations(part.get("annotations"))?;
            validate_logprobs(part.get("logprobs"))
        }
        "text" | "summary_text" | "reasoning_text" => {
            ensure_only_fields(part, &["type", "text"], part_type)?;
            require_string(part, "text", part_type)
        }
        "refusal" => {
            ensure_only_fields(part, &["type", "refusal"], "refusal")?;
            require_string(part, "refusal", "refusal")
        }
        "input_image" => validate_input_image(part, "message"),
        "computer_screenshot" => validate_computer_screenshot(part),
        "input_file" => validate_input_file(part, "message"),
        other => Err(format!(
            "unsupported compacted message content variant {other}"
        )),
    }
}

fn require_string(object: &Map<String, Value>, field: &str, parent: &str) -> Result<(), String> {
    if object.get(field).is_some_and(Value::is_string) {
        Ok(())
    } else {
        Err(format!("{parent} {field} must be a string"))
    }
}

fn validate_nullable_string(
    object: &Map<String, Value>,
    field: &str,
    parent: &str,
) -> Result<(), String> {
    if let Some(value) = object.get(field)
        && !value.is_null()
        && !value.is_string()
    {
        return Err(format!("{parent} {field} must be null or a string"));
    }
    Ok(())
}

fn validate_input_image(part: &Map<String, Value>, parent: &str) -> Result<(), String> {
    let label = format!("{parent} input_image");
    ensure_only_fields(
        part,
        &[
            "type",
            "image_url",
            "file_id",
            "detail",
            "prompt_cache_breakpoint",
        ],
        &label,
    )?;
    validate_nullable_string(part, "image_url", &label)?;
    validate_nullable_string(part, "file_id", &label)?;
    if !matches!(
        part.get("detail").and_then(Value::as_str),
        Some("auto" | "low" | "high" | "original")
    ) {
        return Err(format!(
            "{label} detail must be auto, low, high, or original"
        ));
    }
    validate_prompt_cache_breakpoint(part.get("prompt_cache_breakpoint"), &label)
}

fn validate_computer_screenshot(part: &Map<String, Value>) -> Result<(), String> {
    ensure_only_fields(
        part,
        &[
            "type",
            "image_url",
            "file_id",
            "detail",
            "prompt_cache_breakpoint",
        ],
        "computer_screenshot",
    )?;
    for field in ["image_url", "file_id"] {
        if !part.contains_key(field) {
            return Err(format!("computer_screenshot {field} is required"));
        }
        validate_nullable_string(part, field, "computer_screenshot")?;
    }
    if !matches!(
        part.get("detail").and_then(Value::as_str),
        Some("auto" | "low" | "high" | "original")
    ) {
        return Err("computer_screenshot detail must be auto, low, high, or original".into());
    }
    validate_prompt_cache_breakpoint(part.get("prompt_cache_breakpoint"), "computer_screenshot")
}

fn validate_annotations(value: Option<&Value>) -> Result<(), String> {
    let annotations = value
        .and_then(Value::as_array)
        .ok_or_else(|| "output_text annotations must be an array".to_owned())?;
    for annotation in annotations {
        let annotation = annotation
            .as_object()
            .ok_or_else(|| "output_text annotation must be an object".to_owned())?;
        match annotation.get("type").and_then(Value::as_str) {
            Some("file_citation") => {
                ensure_only_fields(
                    annotation,
                    &["type", "file_id", "index", "filename"],
                    "file_citation",
                )?;
                require_string(annotation, "file_id", "file_citation")?;
                require_u64(annotation, "index", "file_citation")?;
                require_string(annotation, "filename", "file_citation")?;
            }
            Some("url_citation") => {
                ensure_only_fields(
                    annotation,
                    &["type", "url", "start_index", "end_index", "title"],
                    "url_citation",
                )?;
                require_string(annotation, "url", "url_citation")?;
                require_u64(annotation, "start_index", "url_citation")?;
                require_u64(annotation, "end_index", "url_citation")?;
                require_string(annotation, "title", "url_citation")?;
            }
            Some("container_file_citation") => {
                ensure_only_fields(
                    annotation,
                    &[
                        "type",
                        "container_id",
                        "file_id",
                        "start_index",
                        "end_index",
                        "filename",
                    ],
                    "container_file_citation",
                )?;
                for field in ["container_id", "file_id", "filename"] {
                    require_string(annotation, field, "container_file_citation")?;
                }
                for field in ["start_index", "end_index"] {
                    require_u64(annotation, field, "container_file_citation")?;
                }
            }
            Some("file_path") => {
                ensure_only_fields(annotation, &["type", "file_id", "index"], "file_path")?;
                require_string(annotation, "file_id", "file_path")?;
                require_u64(annotation, "index", "file_path")?;
            }
            Some(other) => return Err(format!("unsupported output_text annotation {other}")),
            None => return Err("output_text annotation type must be a string".into()),
        }
    }
    Ok(())
}

fn validate_logprobs(value: Option<&Value>) -> Result<(), String> {
    let logprobs = value
        .and_then(Value::as_array)
        .ok_or_else(|| "output_text logprobs must be an array".to_owned())?;
    for logprob in logprobs {
        validate_logprob(logprob, false)?;
    }
    Ok(())
}

fn validate_logprob(value: &Value, top: bool) -> Result<(), String> {
    let label = if top { "top_logprob" } else { "logprob" };
    let value = value
        .as_object()
        .ok_or_else(|| format!("{label} must be an object"))?;
    let allowed = if top {
        &["token", "logprob", "bytes"][..]
    } else {
        &["token", "logprob", "bytes", "top_logprobs"][..]
    };
    ensure_only_fields(value, allowed, label)?;
    require_string(value, "token", label)?;
    if !value.get("logprob").is_some_and(Value::is_number) {
        return Err(format!("{label} logprob must be a number"));
    }
    let bytes = value
        .get("bytes")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{label} bytes must be an array"))?;
    if bytes.iter().any(|byte| byte.as_i64().is_none()) {
        return Err(format!("{label} bytes must contain integers"));
    }
    if !top {
        let top_logprobs = value
            .get("top_logprobs")
            .and_then(Value::as_array)
            .ok_or_else(|| "logprob top_logprobs must be an array".to_owned())?;
        for top_logprob in top_logprobs {
            validate_logprob(top_logprob, true)?;
        }
    }
    Ok(())
}

fn require_u64(object: &Map<String, Value>, field: &str, parent: &str) -> Result<(), String> {
    if object.get(field).and_then(Value::as_u64).is_some() {
        Ok(())
    } else {
        Err(format!("{parent} {field} must be a non-negative integer"))
    }
}

fn validate_reasoning_content(object: &Map<String, Value>) -> Result<(), String> {
    let Some(content) = object.get("content") else {
        return Ok(());
    };
    let content = content
        .as_array()
        .ok_or_else(|| "reasoning content must be an array".to_owned())?;
    for part in content {
        let part = part
            .as_object()
            .ok_or_else(|| "reasoning content part must be an object".to_owned())?;
        ensure_only_fields(part, &["text", "type"], "reasoning content part")?;
        if part.get("type").and_then(Value::as_str) != Some("reasoning_text")
            || !part.get("text").is_some_and(Value::is_string)
        {
            return Err("reasoning content part must be reasoning_text with string text".into());
        }
    }
    Ok(())
}

fn validate_input_content_part(part: &Value, parent: &str) -> Result<(), String> {
    let part = part
        .as_object()
        .ok_or_else(|| format!("{parent} content part must be an object"))?;
    match part.get("type").and_then(Value::as_str) {
        Some("input_text") => {
            ensure_only_fields(
                part,
                &["type", "text", "prompt_cache_breakpoint"],
                &format!("{parent} input_text"),
            )?;
            require_string(part, "text", &format!("{parent} input_text"))?;
            validate_prompt_cache_breakpoint(part.get("prompt_cache_breakpoint"), parent)
        }
        Some("input_image") => validate_input_image(part, parent),
        Some("input_file") => validate_input_file(part, parent),
        Some(other) => Err(format!("{parent} unsupported content variant {other}")),
        None => Err(format!("{parent} content part type must be a string")),
    }
}

fn validate_input_file(part: &Map<String, Value>, parent: &str) -> Result<(), String> {
    ensure_only_fields(
        part,
        &[
            "type",
            "detail",
            "file_data",
            "file_id",
            "file_url",
            "filename",
            "prompt_cache_breakpoint",
        ],
        &format!("{parent} input_file"),
    )?;
    validate_nullable_string(part, "file_id", &format!("{parent} input_file"))?;
    for field in ["file_data", "file_url", "filename"] {
        if let Some(value) = part.get(field)
            && !value.is_string()
        {
            return Err(format!("{parent} input_file {field} must be a string"));
        }
    }
    if let Some(detail) = part.get("detail")
        && !matches!(detail.as_str(), Some("auto" | "low" | "high"))
    {
        return Err(format!(
            "{parent} input_file detail must be auto, low, or high"
        ));
    }
    validate_prompt_cache_breakpoint(part.get("prompt_cache_breakpoint"), parent)
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

fn validate_optional_status(
    object: &Map<String, Value>,
    label: &str,
    nullable: bool,
) -> Result<(), String> {
    if let Some(status) = object.get("status")
        && !(nullable && status.is_null())
        && !matches!(
            status.as_str(),
            Some("in_progress" | "completed" | "incomplete")
        )
    {
        return Err(format!("{label} status is invalid"));
    }
    Ok(())
}

fn validate_optional_caller(
    value: Option<&Value>,
    label: &str,
    nullable: bool,
) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    if nullable && value.is_null() {
        return Ok(());
    }
    let caller = value
        .as_object()
        .ok_or_else(|| format!("{label} caller must be an object"))?;
    match caller.get("type").and_then(Value::as_str) {
        Some("direct") => ensure_only_fields(caller, &["type"], "direct caller"),
        Some("program") => {
            ensure_only_fields(caller, &["caller_id", "type"], "program caller")?;
            if caller
                .get("caller_id")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            {
                return Err("program caller caller_id must be a non-empty string".into());
            }
            Ok(())
        }
        _ => Err(format!("{label} caller type is invalid")),
    }
}

fn validate_prompt_cache_breakpoint(value: Option<&Value>, parent: &str) -> Result<(), String> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_null() {
        return Ok(());
    }
    let object = value
        .as_object()
        .ok_or_else(|| format!("{parent} prompt_cache_breakpoint must be null or an object"))?;
    ensure_only_fields(object, &["mode"], "prompt_cache_breakpoint")?;
    if object.get("mode").and_then(Value::as_str) != Some("explicit") {
        return Err(format!(
            "{parent} prompt_cache_breakpoint mode must be explicit"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::types::{
        AssistantContent, AssistantMessage, ContextMessage, MemoryLayer, Message,
        NativeCompactionCoverage, ProviderContextAnchor, ProviderContextItem,
        ProviderContextPayload, StopReason, ToolArgsPreview, ToolCall, ToolDefinition, Usage,
        UserContent, UserMessage, ValidatedToolArguments,
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
    fn request_tool_strict_mode_requires_openai_strict_schema() {
        let mut context = PromptContext {
            system_prompt: "constitution".into(),
            memory_blocks: vec![],
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
        context.tools = vec![
            ToolDefinition {
                name: "safe".into(),
                description: "safe schema".into(),
                parameters: json!({
                    "type":"object",
                    "properties":{"city":{"type":"string"}},
                    "required":["city"],
                    "additionalProperties":false
                }),
            },
            ToolDefinition {
                name: "unsafe".into(),
                description: "schema outside MFJS strict subset".into(),
                parameters: json!({
                    "type":"object",
                    "properties":{"city":{"type":"string","pattern":".*"}},
                    "required":["city"],
                    "additionalProperties":false
                }),
            },
            ToolDefinition {
                name: "missing_required".into(),
                description: "not strict because a property is optional".into(),
                parameters: json!({
                    "type":"object",
                    "properties":{"city":{"type":"string"}},
                    "additionalProperties":false
                }),
            },
            ToolDefinition {
                name: "missing_additional_properties".into(),
                description: "not strict because extra keys are allowed".into(),
                parameters: json!({
                    "type":"object",
                    "properties":{"city":{"type":"string"}},
                    "required":["city"]
                }),
            },
            ToolDefinition {
                name: "nested".into(),
                description: "nested strict objects".into(),
                parameters: json!({
                    "type":"object",
                    "properties":{
                        "address":{
                            "type":"object",
                            "properties":{"city":{"type":"string"}},
                            "required":["city"],
                            "additionalProperties":false
                        }
                    },
                    "required":["address"],
                    "additionalProperties":false
                }),
            },
            ToolDefinition {
                name: "nested_missing_additional_properties".into(),
                description: "nested object must also disallow extra keys".into(),
                parameters: json!({
                    "type":"object",
                    "properties":{
                        "address":{
                            "type":"object",
                            "properties":{"city":{"type":"string"}},
                            "required":["city"]
                        }
                    },
                    "required":["address"],
                    "additionalProperties":false
                }),
            },
        ];
        let body = build_request(&spec(), &context, &RequestOptions::default()).expect("request");
        assert_eq!(body["tools"][0]["strict"], true);
        assert_eq!(body["tools"][1]["strict"], false);
        assert_eq!(body["tools"][2]["strict"], false);
        assert_eq!(body["tools"][3]["strict"], false);
        assert_eq!(body["tools"][4]["strict"], true);
        assert_eq!(body["tools"][5]["strict"], false);
    }

    #[test]
    fn openai_strict_schema_enforces_official_limits_and_recursive_requirements() {
        fn nested_objects(levels: usize) -> Value {
            let mut schema = json!({"type":"string"});
            for _ in 0..levels {
                schema = json!({
                    "type":"object",
                    "properties":{"next":schema},
                    "required":["next"],
                    "additionalProperties":false
                });
            }
            schema
        }

        fn property_schema(name: String, value: Value) -> Value {
            let mut properties = Map::new();
            properties.insert(name.clone(), value);
            json!({
                "type":"object",
                "properties":properties,
                "required":[name],
                "additionalProperties":false
            })
        }

        fn enum_schema(count: usize, value_length: usize) -> Value {
            property_schema(
                "value".into(),
                json!({
                    "type":"string",
                    "enum":(0..count)
                        .map(|index| json!(format!("{}{}", "x".repeat(value_length), index)))
                        .collect::<Vec<_>>()
                }),
            )
        }

        assert!(is_openai_strict_safe(&nested_objects(10)));
        assert!(!is_openai_strict_safe(&nested_objects(11)));

        assert!(is_openai_strict_safe(&enum_schema(1_000, 1)));
        assert!(!is_openai_strict_safe(&enum_schema(1_001, 1)));
        assert!(is_openai_strict_safe(&enum_schema(250, 60)));
        assert!(!is_openai_strict_safe(&enum_schema(251, 60)));

        let property_name = "p".repeat(120_000);
        assert!(is_openai_strict_safe(&property_schema(
            property_name.clone(),
            json!({"type":"string"}),
        )));
        assert!(!is_openai_strict_safe(&property_schema(
            format!("{property_name}x"),
            json!({"type":"string"}),
        )));

        let enum_value = "e".repeat(119_995);
        assert!(is_openai_strict_safe(&property_schema(
            "value".into(),
            json!({"type":"string","enum":[enum_value.clone()]}),
        )));
        assert!(!is_openai_strict_safe(&property_schema(
            "value".into(),
            json!({"type":"string","enum":[format!("{enum_value}x")]}),
        )));

        let const_value = "c".repeat(119_995);
        assert!(is_openai_strict_safe(&property_schema(
            "value".into(),
            json!({"type":"string","const":const_value.clone()}),
        )));
        assert!(!is_openai_strict_safe(&property_schema(
            "value".into(),
            json!({"type":"string","const":format!("{const_value}x")}),
        )));

        let definition_name = "d".repeat(119_996);
        let mut definitions = Map::new();
        definitions.insert(definition_name.clone(), json!({"type":"string"}));
        let mut definition_schema = property_schema(
            "node".into(),
            json!({"$ref":format!("#/$defs/{definition_name}")}),
        );
        definition_schema["$defs"] = Value::Object(definitions);
        assert!(is_openai_strict_safe(&definition_schema));
        let mut oversized_definitions = Map::new();
        let oversized_definition_name = format!("{definition_name}x");
        oversized_definitions.insert(oversized_definition_name.clone(), json!({"type":"string"}));
        definition_schema["$defs"] = Value::Object(oversized_definitions);
        definition_schema["properties"]["node"]["$ref"] =
            json!(format!("#/$defs/{oversized_definition_name}"));
        assert!(!is_openai_strict_safe(&definition_schema));

        assert!(!is_openai_strict_safe(&property_schema(
            "nested".into(),
            json!({
                "type":"object",
                "properties":{"value":{"type":"string","default":"x"}},
                "required":["value"],
                "additionalProperties":false
            }),
        )));

        let valid_any_of = property_schema(
            "value".into(),
            json!({"anyOf":[{"type":"string"},{"type":"null"}]}),
        );
        assert!(is_openai_strict_safe(&valid_any_of));
        let invalid_any_of = property_schema(
            "value".into(),
            json!({
                "anyOf":[{
                    "type":"object",
                    "properties":{"city":{"type":"string"}},
                    "additionalProperties":false
                }]
            }),
        );
        assert!(!is_openai_strict_safe(&invalid_any_of));
    }

    #[test]
    fn non_finite_temperature_is_rejected_before_responses_json_construction() {
        for temperature in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let error = build_request(
                &spec(),
                &PromptContext {
                    system_prompt: "constitution".into(),
                    memory_blocks: vec![],
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
                },
                &RequestOptions {
                    temperature: Some(temperature),
                    ..RequestOptions::default()
                },
            )
            .expect_err("non-finite temperature");
            assert!(
                matches!(error, ResponsesAdapterError::InvalidTemperature(value)
                if value.to_bits() == temperature.to_bits())
            );
        }
        let finite = build_request(
            &spec(),
            &PromptContext {
                system_prompt: "constitution".into(),
                memory_blocks: vec![],
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
            },
            &RequestOptions {
                temperature: Some(0.7),
                ..RequestOptions::default()
            },
        )
        .expect("finite Responses temperature");
        assert_eq!(finite["temperature"], json!(0.7));
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
    fn terminal_may_omit_repeated_output_after_all_items_are_done() {
        let mut values = fixture_values();
        values.last_mut().unwrap()["response"]["output"] = json!([]);
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        let mut terminal = None;
        for value in values {
            terminal = state
                .push_json(&value.to_string())
                .expect("Codex terminal omission is valid after item.done")
                .terminal
                .or(terminal);
        }
        let terminal = terminal.expect("terminal");
        assert_eq!(terminal.reason, StopReason::ToolUse);
        assert_eq!(terminal.provider_context.len(), 1);
    }

    #[test]
    fn empty_terminal_output_requires_every_observed_item_to_finish() {
        let mut values = fixture_values();
        values[16]["type"] = json!("response.future.event");
        values.last_mut().unwrap()["response"]["output"] = json!([]);
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        for value in &values[..values.len() - 1] {
            state
                .push_json(&value.to_string())
                .expect("unknown event preserves its sequence slot");
        }
        assert!(
            state
                .push_json(&values.last().unwrap().to_string())
                .expect_err("an empty terminal output cannot hide an unfinished item")
                .to_string()
                .contains("unfinished output items")
        );
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
    fn compaction_coverage_is_internal_canonical_and_requires_strictly_increasing_persistence() {
        let spec = spec();
        let mut context = PromptContext {
            system_prompt: "system".into(),
            memory_blocks: vec![],
            messages: vec![persisted_user(5), persisted_user(8), persisted_user(12)],
            provider_context: vec![],
            tools: vec![],
        };
        let coverage = derive_compaction_coverage(&spec, &context).expect("coverage");
        assert_eq!(coverage.through_message_seq, 12);
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
            12
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
            vec![persisted_user(0)],
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
                provider_origin: ProviderContextItem::test_origin(),
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
                r#"{"type":"response.incomplete","sequence_number":3,"response":{"id":"resp_incomplete","model":"gpt-5.6","status":"incomplete","output":[{"id":"fc","type":"function_call","call_id":"call","name":"weather","arguments":"{\"city\":\"Tokyo\"}"}],"incomplete_details":{"reason":"max_output_tokens"},"usage":{"input_tokens":1,"input_tokens_details":{"cached_tokens":0},"output_tokens":1,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":2}}}"#,
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
    fn incomplete_content_filter_is_error_and_never_length_rejection() {
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
                r#"{"type":"response.incomplete","sequence_number":3,"response":{"id":"resp_filter","model":"gpt-5.6","status":"incomplete","output":[{"id":"fc","type":"function_call","call_id":"call","name":"weather","arguments":"{\"city\":\"Tokyo\"}"}],"incomplete_details":{"reason":"content_filter","message":"blocked by policy"},"usage":{"input_tokens":1,"input_tokens_details":{"cached_tokens":0},"output_tokens":1,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":2}}}"#,
            )
            .expect("content-filter terminal")
            .terminal
            .expect("terminal");
        assert_eq!(terminal.reason, StopReason::Error);
        assert_eq!(terminal.provider_code.as_deref(), Some("content_filter"));
        assert_eq!(terminal.error_message.as_deref(), Some("blocked by policy"));
        assert!(matches!(
            terminal.events.as_slice(),
            [ProviderEvent::ToolCallEnd { .. }]
        ));
    }

    #[test]
    fn incomplete_unknown_or_missing_reason_fails_closed() {
        for details in [r#""reason":"provider_specific""#, r#""#] {
            let mut state =
                ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
            let incomplete_details = if details.is_empty() {
                String::new()
            } else {
                format!(",\"incomplete_details\":{{{details}}}")
            };
            let payload = format!(
                r#"{{"type":"response.incomplete","sequence_number":0,"response":{{"id":"resp_bad","model":"gpt-5.6","status":"incomplete","output":[]{} }}}}"#,
                incomplete_details
            );
            let error = state
                .push_json(&payload)
                .expect_err("invalid incomplete reason");
            assert!(error.to_string().contains("incomplete"));
        }
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
                "usage":{
                    "input_tokens":10,
                    "input_tokens_details":{"cached_tokens":0},
                    "output_tokens":3,
                    "output_tokens_details":{"reasoning_tokens":0},
                    "total_tokens":13
                }
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
                persisted_user(1),
                persisted_user(2),
                persisted_user(3),
                persisted_user(4),
                persisted_user(5),
                persisted_user(6),
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
            provider_origin: ProviderContextItem::test_origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: window.clone(),
                coverage: NativeCompactionCoverage {
                    through_message_seq: 8,
                    context_fingerprint: fingerprint,
                },
            },
        });

        let input = convert_input(&spec, &context, true).expect("valid native replay");
        assert_eq!(input[0]["content"][0]["text"], "leading-synthetic");
        assert_eq!(&input[1..=window.len()], window.as_slice());
        assert_eq!(input.len(), window.len() + 3);
        assert_eq!(input[window.len() + 1]["content"][0]["text"], "message-9");
        assert_eq!(input[window.len() + 2]["content"][0]["text"], "message-10");

        let default_request = build_request(&spec, &context, &RequestOptions::default())
            .expect("default three-layer request");
        assert!(!default_request.to_string().contains("opaque"));
        assert!(default_request.to_string().contains("message-7"));

        let request = build_request(
            &spec,
            &context,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("request reuses canonical replay ordering");
        assert_eq!(request["input"], Value::Array(input));
    }

    #[test]
    fn native_compacted_window_replays_gapped_persisted_suffix() {
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
                persisted_user(1),
                persisted_user(3),
                persisted_user(5),
                persisted_user(7),
                persisted_user(9),
            ],
            provider_context: vec![],
            tools: vec![],
        };
        let fingerprint = context_fingerprint(&spec, &context).unwrap();
        context.provider_context.push(ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: ProviderContextItem::test_origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: window.clone(),
                coverage: NativeCompactionCoverage {
                    through_message_seq: 5,
                    context_fingerprint: fingerprint,
                },
            },
        });

        let input = convert_input(&spec, &context, true)
            .expect("valid native replay with global event gaps");
        assert_eq!(input[..window.len()], window);
        assert_eq!(input.len(), window.len() + 2);
        assert_eq!(input[window.len()]["content"][0]["text"], "message-7");
        assert_eq!(input[window.len() + 1]["content"][0]["text"], "message-9");

        let request = build_request(
            &spec,
            &context,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("request reuses gapped canonical replay ordering");
        assert_eq!(request["input"], Value::Array(input));
    }

    #[test]
    fn stale_native_context_falls_back_to_durable_three_layer_view() {
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
            provider_origin: ProviderContextItem::test_origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({"id":"cmp","type":"compaction","encrypted_content":"opaque"})],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 8,
                    context_fingerprint: context_fingerprint(&spec, &context).unwrap(),
                },
            },
        };
        context.provider_context = vec![native.clone(), native.clone()];
        let fallback = convert_input(&spec, &context, true).expect("duplicate fallback");
        assert!(!Value::Array(fallback).to_string().contains("opaque"));

        context.provider_context = vec![native.clone()];
        context
            .memory_blocks
            .push(crate::provider::types::MemoryBlock {
                layer: MemoryLayer::L1,
                text: "memory".into(),
                time_range: None,
            });
        let fallback = convert_input(&spec, &context, true).expect("coexistence fallback");
        assert!(Value::Array(fallback).to_string().contains("memory"));
        context.memory_blocks.clear();

        if let ProviderContextPayload::OpenAiCompactedWindow { coverage, .. } =
            &mut context.provider_context[0].payload
        {
            coverage.context_fingerprint = "wrong".into();
        }
        let fallback = convert_input(&spec, &context, true).expect("fingerprint fallback");
        assert!(!Value::Array(fallback).to_string().contains("opaque"));

        context.provider_context = vec![native];
        context.messages = vec![persisted_user(10)];
        let fallback = convert_input(&spec, &context, true).expect("suffix gap fallback");
        let fallback = Value::Array(fallback).to_string();
        assert!(fallback.contains("message-10"));
        assert!(!fallback.contains("opaque"));
        context.messages = vec![
            persisted_user(9),
            ContextMessage::Synthetic {
                message: Message::User(UserMessage {
                    content: vec![],
                    timestamp: Utc::now(),
                }),
            },
        ];
        let fallback = convert_input(&spec, &context, true).expect("placement fallback");
        assert!(!Value::Array(fallback).to_string().contains("opaque"));
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
            json!({"id":"r","type":"reasoning","summary":[],"encrypted_content":""}),
            json!({"id":"r","type":"reasoning","summary":[],"encrypted_content":7}),
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
        assert!(
            validate_canonical_item(&json!({
                "id":"r",
                "type":"reasoning",
                "summary":[],
                "encrypted_content":null,
            }))
            .is_ok()
        );
    }

    #[test]
    fn native_replay_requires_both_opt_in_and_compat_capability() {
        let mut spec = spec();
        let mut context = PromptContext {
            system_prompt: "system".into(),
            memory_blocks: vec![],
            messages: vec![persisted_user(1), persisted_user(2)],
            provider_context: vec![],
            tools: vec![],
        };
        let coverage = NativeCompactionCoverage {
            through_message_seq: 1,
            context_fingerprint: context_fingerprint(&spec, &context).unwrap(),
        };
        context.provider_context.push(ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: ProviderContextItem::test_origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({"id":"cmp","type":"compaction","encrypted_content":"NATIVE"})],
                coverage,
            },
        });
        if let ProtocolCompat::Responses(compat) = &mut spec.compat {
            compat.supports_native_compact = false;
        }
        let request = build_request(
            &spec,
            &context,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("capability loss falls back");
        let request = request.to_string();
        assert!(!request.contains("NATIVE"));
        assert!(request.contains("message-1"));
        assert!(request.contains("message-2"));
    }

    #[test]
    fn foreign_native_context_forces_three_layer_fallback() {
        let spec = spec();
        let mut context = PromptContext {
            system_prompt: "system".into(),
            memory_blocks: vec![],
            messages: vec![persisted_user(1)],
            provider_context: vec![],
            tools: vec![],
        };
        context.provider_context.push(ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: ProviderContextItem::test_origin(),
            payload: ProviderContextPayload::AnthropicCompaction {
                block: json!({"type":"compaction","content":"FOREIGN_NATIVE"}),
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
        assert!(request.to_string().contains("message-1"));
    }

    #[test]
    fn usage_schema_and_accounting_invariants_fail_closed() {
        let valid = json!({
            "input_tokens":10,
            "input_tokens_details":{"cached_tokens":3},
            "output_tokens":4,
            "output_tokens_details":{"reasoning_tokens":1},
            "total_tokens":14
        });
        assert_eq!(
            parse_usage(&valid).unwrap(),
            Usage {
                input: 7,
                output: 4,
                cache_read: 3,
                cache_write: 0,
                reasoning: 1,
                total_tokens: 14,
            }
        );
        assert_eq!(
            parse_usage(&json!({
                "input_tokens":2,
                "output_tokens":3,
                "total_tokens":5
            }))
            .unwrap(),
            Usage {
                input: 2,
                output: 3,
                cache_read: 0,
                cache_write: 0,
                reasoning: 0,
                total_tokens: 5,
            }
        );
        for invalid in [
            json!({}),
            json!({"input_tokens":"10","input_tokens_details":{"cached_tokens":0},"output_tokens":0,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":10}),
            json!({"input_tokens":10,"input_tokens_details":null,"output_tokens":0,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":10}),
            json!({"input_tokens":1,"input_tokens_details":{"cached_tokens":2},"output_tokens":0,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":1}),
            json!({"input_tokens":1,"input_tokens_details":{"cached_tokens":0},"output_tokens":1,"output_tokens_details":{"reasoning_tokens":2},"total_tokens":2}),
            json!({"input_tokens":1,"input_tokens_details":{"cached_tokens":0},"output_tokens":1,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":3}),
        ] {
            assert!(parse_usage(&invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn encrypted_reasoning_ids_types_and_budget_are_transactional() {
        for malformed in ["\"\"", "7"] {
            let mut state =
                ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
            state.push_json(
                r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"r","type":"reasoning","summary":[]}}"#,
            ).unwrap();
            let done = format!(
                r#"{{"type":"response.output_item.done","sequence_number":1,"output_index":0,"item":{{"id":"r","type":"reasoning","summary":[],"encrypted_content":{malformed}}}}}"#
            );
            assert!(state.push_json(&done).is_err());
            assert!(state.provider_context().is_empty());
        }

        let mut nullable = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        nullable.push_json(
            r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"r","type":"reasoning","summary":[]}}"#,
        ).unwrap();
        nullable.push_json(
            r#"{"type":"response.output_item.done","sequence_number":1,"output_index":0,"item":{"id":"r","type":"reasoning","summary":[],"encrypted_content":null}}"#,
        ).expect("official nullable encrypted_content is equivalent to absence");
        assert!(nullable.provider_context().is_empty());

        let mut duplicate =
            ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        duplicate.push_json(
            r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"same","type":"reasoning","summary":[]}}"#,
        ).unwrap();
        assert!(duplicate.push_json(
            r#"{"type":"response.output_item.added","sequence_number":1,"output_index":1,"item":{"id":"same","type":"reasoning","summary":[]}}"#,
        ).is_err());

        let budget = ResponseBudget {
            max_content_bytes: 1,
            max_wire_bytes: usize::MAX,
            max_events: usize::MAX,
            max_preview_work_bytes: usize::MAX,
            max_tool_calls: usize::MAX,
        };
        let mut state = ResponsesReceiveState::with_budget(schemas(), budget);
        state.push_json(
            r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"r","type":"reasoning","summary":[]}}"#,
        ).unwrap();
        assert!(matches!(
            state.push_json(
                r#"{"type":"response.output_item.done","sequence_number":1,"output_index":0,"item":{"id":"r","type":"reasoning","summary":[],"encrypted_content":"x"}}"#,
            ),
            Err(ResponsesAdapterError::ResponseLimitExceeded { resource: "content_bytes", .. })
        ));
        assert!(state.provider_context().is_empty());
        state.push_json(
            r#"{"type":"response.output_item.done","sequence_number":1,"output_index":0,"item":{"id":"r","type":"reasoning","summary":[]}}"#,
        ).expect("failed opaque charge did not mutate semantic state");
        let terminal = r#"{"type":"response.completed","sequence_number":2,"response":{"id":"resp","model":"gpt-5.6","status":"completed","output":[{"id":"r","type":"reasoning","summary":[],"encrypted_content":"x"}],"usage":{"input_tokens":0,"input_tokens_details":{"cached_tokens":0},"output_tokens":0,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":0}}}"#;
        assert!(matches!(
            state.push_json(terminal),
            Err(ResponsesAdapterError::ResponseLimitExceeded {
                resource: "content_bytes",
                ..
            })
        ));
        assert!(state.provider_context().is_empty());
    }

    #[test]
    fn replay_rejects_duplicate_public_indexes_and_orders_trailing_opaque_items() {
        let spec = spec();
        let assistant = |content| {
            Message::Assistant(crate::provider::types::AssistantMessage {
                content,
                model: spec.id.clone(),
                provider: spec.provider.clone(),
                origin: spec.origin(),
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                provider_code: None,
                interrupted: false,
                timestamp: Utc::now(),
            })
        };
        let mut context = PromptContext {
            system_prompt: "system".into(),
            memory_blocks: vec![],
            messages: vec![ContextMessage::Persisted {
                id: "assistant".into(),
                seq: 1,
                message: assistant(vec![
                    AssistantContent::Text {
                        text: "a".into(),
                        wire_item_index: 0,
                    },
                    AssistantContent::Text {
                        text: "b".into(),
                        wire_item_index: 0,
                    },
                ]),
            }],
            provider_context: vec![],
            tools: vec![],
        };
        assert!(build_request(&spec, &context, &RequestOptions::default()).is_err());

        context.messages[0] = ContextMessage::Persisted {
            id: "assistant".into(),
            seq: 1,
            message: assistant(vec![AssistantContent::Text {
                text: "a".into(),
                wire_item_index: 0,
            }]),
        };
        context.provider_context.push(ProviderContextItem {
            origin_message: Some(ProviderContextAnchor {
                message_id: "assistant".into(),
                message_seq: 1,
            }),
            wire_item_index: Some(1),
            ordinal: 1,
            provider_origin: ProviderContextItem::test_origin(),
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::OpenAiResponses,
                item: json!({
                    "id":"reasoning-b",
                    "type":"reasoning",
                    "summary":[],
                    "encrypted_content":"opaque-b"
                }),
            },
        });
        context.provider_context.push(ProviderContextItem {
            origin_message: Some(ProviderContextAnchor {
                message_id: "assistant".into(),
                message_seq: 1,
            }),
            wire_item_index: Some(1),
            ordinal: 0,
            provider_origin: ProviderContextItem::test_origin(),
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::OpenAiResponses,
                item: json!({
                    "id":"reasoning-a",
                    "type":"reasoning",
                    "summary":[],
                    "encrypted_content":"opaque-a"
                }),
            },
        });
        let request = build_request(&spec, &context, &RequestOptions::default())
            .expect("trailing opaque reasoning has an explicit stable placement");
        let input = request["input"].as_array().unwrap();
        assert_eq!(input[1]["id"], "reasoning-a");
        assert_eq!(input[2]["id"], "reasoning-b");

        context.provider_context[1].ordinal = 1;
        assert!(matches!(
            build_request(&spec, &context, &RequestOptions::default()),
            Err(ResponsesAdapterError::InvalidContext(message))
                if message.contains("duplicate encrypted reasoning placement")
        ));

        context.provider_context[1].ordinal = 0;
        context.provider_context[1].wire_item_index = None;
        assert!(matches!(
            build_request(&spec, &context, &RequestOptions::default()),
            Err(ResponsesAdapterError::InvalidContext(message))
                if message.contains("missing wire_item_index")
        ));
        context.provider_context[1].wire_item_index = Some(1);
        context.provider_context[1].origin_message = None;
        assert!(matches!(
            build_request(&spec, &context, &RequestOptions::default()),
            Err(ResponsesAdapterError::InvalidContext(message))
                if message.contains("missing an origin anchor")
        ));
    }

    #[test]
    fn terminal_null_usage_preserves_observed_usage_until_validation_succeeds() {
        let observed = Usage {
            input: 7,
            output: 3,
            cache_read: 2,
            cache_write: 0,
            reasoning: 1,
            total_tokens: 12,
        };
        let mut failed = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        failed.usage = observed.clone();
        let terminal = failed
            .push_json(
                r#"{"type":"response.failed","sequence_number":0,"response":{"id":"resp","model":"gpt-5.6","status":"failed","output":[],"error":{"code":"server_error","message":"failed"},"usage":null}}"#,
            )
            .unwrap()
            .terminal
            .unwrap();
        assert_eq!(terminal.usage, observed);

        let mut malformed =
            ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        malformed.usage = observed.clone();
        assert!(malformed
            .push_json(
                r#"{"type":"response.incomplete","sequence_number":0,"response":{"id":"resp","model":"gpt-5.6","status":"incomplete","output":[],"incomplete_details":{},"usage":null}}"#,
            )
            .is_err());
        assert_eq!(malformed.usage, observed);
    }

    #[test]
    fn incomplete_terminal_retains_valid_usage_across_reason_validation_failures() {
        let received = Usage {
            input: 7,
            output: 3,
            cache_read: 2,
            cache_write: 0,
            reasoning: 1,
            total_tokens: 12,
        };
        let usage = r#""usage":{"input_tokens":9,"input_tokens_details":{"cached_tokens":2},"output_tokens":3,"output_tokens_details":{"reasoning_tokens":1},"total_tokens":12}"#;
        for details in [
            r#""incomplete_details":{}"#,
            r#""incomplete_details":{"reason":"future_reason"}"#,
        ] {
            let mut state =
                ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
            let payload = format!(
                r#"{{"type":"response.incomplete","sequence_number":0,"response":{{"id":"resp","model":"gpt-5.6","status":"incomplete","output":[],{details},{usage}}}}}"#
            );
            assert!(state.push_json(&payload).is_err());
            assert_eq!(state.usage, received);
            assert!(!state.terminal);
            assert!(state.response_id.is_none());
            assert!(state.provider_context().is_empty());
            assert_eq!(state.next_sequence_number, 0);
        }
    }

    #[test]
    fn failed_terminal_retains_valid_usage_when_later_reservation_fails() {
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        state.budget.max_events = 0;
        let payload = r#"{"type":"response.failed","sequence_number":0,"response":{"id":"resp","model":"gpt-5.6","status":"failed","output":[],"error":{"code":"server_error","message":"failed"},"usage":{"input_tokens":4,"input_tokens_details":{"cached_tokens":1},"output_tokens":2,"output_tokens_details":{"reasoning_tokens":1},"total_tokens":6}}}"#;
        assert!(matches!(
            state.push_json(payload),
            Err(ResponsesAdapterError::ResponseLimitExceeded {
                resource: "event_count",
                ..
            })
        ));
        assert_eq!(
            state.usage,
            Usage {
                input: 3,
                output: 2,
                cache_read: 1,
                cache_write: 0,
                reasoning: 1,
                total_tokens: 6,
            }
        );
        assert!(state.response_id.is_none());
        assert!(state.response_model.is_none());
        assert!(!state.terminal);
        assert!(state.provider_context().is_empty());
        assert_eq!(state.next_sequence_number, 0);
    }

    #[test]
    fn parallel_function_calls_reject_duplicate_pairing_identities_transactionally() {
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        state
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"fc-a","type":"function_call","call_id":"call-a","name":"weather","arguments":""}}"#,
            )
            .unwrap();
        assert!(state
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":1,"output_index":1,"item":{"id":"fc-b","type":"function_call","call_id":"call-a","name":"weather","arguments":""}}"#,
            )
            .is_err());
        state
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":1,"output_index":1,"item":{"id":"fc-b","type":"function_call","call_id":"call-b","name":"weather","arguments":""}}"#,
            )
            .expect("rejected duplicate did not consume sequence, slot, or identity");

        let mut duplicate_item =
            ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        duplicate_item
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"fc","type":"function_call","call_id":"call-a","name":"weather","arguments":""}}"#,
            )
            .unwrap();
        assert!(duplicate_item
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":1,"output_index":1,"item":{"id":"fc","type":"function_call","call_id":"call-b","name":"weather","arguments":""}}"#,
            )
            .is_err());
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

    #[test]
    fn terminal_validation_failure_rolls_back_identity_counters_and_slots() {
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        for payload in [
            r#"{"type":"response.created","sequence_number":0,"response":{"id":"resp","model":"gpt-5.6","status":"in_progress","output":[]}}"#,
            r#"{"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":{"id":"fc","type":"function_call","call_id":"call","name":"weather","arguments":""}}"#,
            r#"{"type":"response.function_call_arguments.delta","sequence_number":2,"item_id":"fc","output_index":0,"delta":"{\"city\":\"Tokyo\"}"}"#,
            r#"{"type":"response.output_item.done","sequence_number":3,"output_index":0,"item":{"id":"fc","type":"function_call","call_id":"call","name":"weather","arguments":"{\"city\":\"Tokyo\"}"}}"#,
        ] {
            state.push_json(payload).expect("setup");
        }
        let snapshot = (
            state.response_id.clone(),
            state.response_model.clone(),
            state.content_bytes,
            state.event_count,
            state.preview_work_bytes,
            state.slots.len(),
        );
        let bad = r#"{"type":"response.completed","sequence_number":4,"response":{"id":"other","model":"gpt-5.6","status":"completed","output":[{"id":"fc","type":"function_call","call_id":"call","name":"weather","arguments":"{\"city\":\"Tokyo\"}"}],"usage":{"input_tokens":1,"input_tokens_details":{"cached_tokens":0},"output_tokens":1,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":2}}}"#;
        assert!(state.push_json(bad).is_err());
        assert!(!state.terminal);
        assert_eq!(
            (
                state.response_id.clone(),
                state.response_model.clone(),
                state.content_bytes,
                state.event_count,
                state.preview_work_bytes,
                state.slots.len()
            ),
            snapshot
        );
        assert_eq!(state.usage.input, 1);
        assert_eq!(state.usage.output, 1);
        assert_eq!(state.usage.total_tokens, 2);
        let good = r#"{"type":"response.completed","sequence_number":4,"response":{"id":"resp","model":"gpt-5.6","status":"completed","output":[{"id":"fc","type":"function_call","call_id":"call","name":"weather","arguments":"{\"city\":\"Tokyo\"}"}],"usage":{"input_tokens":1,"input_tokens_details":{"cached_tokens":0},"output_tokens":1,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":2}}}"#;
        let terminal = state
            .push_json(good)
            .expect("retry under same sequence number")
            .terminal
            .expect("terminal");
        assert_eq!(terminal.reason, StopReason::ToolUse);
        assert!(state.terminal);
    }

    #[test]
    fn response_failed_preserves_state_on_usage_validation_error_and_retries() {
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        state
            .push_json(
                r#"{"type":"response.created","sequence_number":0,"response":{"id":"resp","model":"gpt-5.6","status":"in_progress","output":[]}}"#,
            )
            .unwrap();
        let snapshot = (
            state.response_id.clone(),
            state.response_model.clone(),
            state.content_bytes,
            state.event_count,
            state.preview_work_bytes,
        );
        let bad = r#"{"type":"response.failed","sequence_number":1,"response":{"id":"resp","model":"gpt-5.6","status":"failed","output":[],"error":{"code":"server_error","message":"failed"},"usage":{"input_tokens":"bad"}}}"#;
        assert!(state.push_json(bad).is_err());
        assert!(!state.terminal);
        assert_eq!(
            (
                state.response_id.clone(),
                state.response_model.clone(),
                state.content_bytes,
                state.event_count,
                state.preview_work_bytes
            ),
            snapshot
        );
        let good = r#"{"type":"response.failed","sequence_number":1,"response":{"id":"resp","model":"gpt-5.6","status":"failed","output":[],"error":{"code":"server_error","message":"failed"}}}"#;
        let terminal = state
            .push_json(good)
            .expect("retry under same sequence number")
            .terminal
            .expect("terminal");
        assert_eq!(terminal.reason, StopReason::Error);
        assert_eq!(terminal.provider_code.as_deref(), Some("server_error"));
        assert!(state.terminal);
    }

    #[test]
    fn encrypted_reasoning_is_omitted_when_reasoning_or_capability_disabled() {
        let spec = spec();
        let anchor = ProviderContextAnchor {
            message_id: "assistant-1".into(),
            message_seq: 1,
        };
        let context = || PromptContext {
            system_prompt: "system".into(),
            memory_blocks: vec![],
            messages: vec![ContextMessage::Persisted {
                id: anchor.message_id.clone(),
                seq: anchor.message_seq,
                message: Message::Assistant(AssistantMessage {
                    content: vec![AssistantContent::Text {
                        text: "public".into(),
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
                    timestamp: Utc::now(),
                }),
            }],
            provider_context: vec![ProviderContextItem {
                origin_message: Some(anchor.clone()),
                wire_item_index: Some(0),
                ordinal: 0,
                provider_origin: ProviderContextItem::test_origin(),
                payload: ProviderContextPayload::EncryptedReasoning {
                    protocol: ApiProtocol::OpenAiResponses,
                    item: json!({
                        "id": "er",
                        "type": "reasoning",
                        "summary": [],
                        "encrypted_content": "OPAQUE"
                    }),
                },
            }],
            tools: vec![],
        };

        let enabled =
            build_request(&spec, &context(), &RequestOptions::default()).expect("enabled");
        assert!(enabled.to_string().contains("OPAQUE"));

        let mut no_reasoning = spec.clone();
        no_reasoning.reasoning = false;
        let request = build_request(&no_reasoning, &context(), &RequestOptions::default())
            .expect("reasoning disabled");
        assert!(!request.to_string().contains("OPAQUE"));
        assert!(request.to_string().contains("public"));

        let mut no_capability = spec.clone();
        if let ProtocolCompat::Responses(compat) = &mut no_capability.compat {
            compat.supports_encrypted_reasoning = false;
        }
        let request = build_request(&no_capability, &context(), &RequestOptions::default())
            .expect("capability disabled");
        assert!(!request.to_string().contains("OPAQUE"));
        assert!(request.to_string().contains("public"));
    }

    #[test]
    fn native_coverage_greater_than_max_seq_falls_back_to_durable_messages() {
        let spec = spec();
        let mut context = PromptContext {
            system_prompt: "system".into(),
            memory_blocks: vec![],
            messages: vec![persisted_user(7), persisted_user(8)],
            provider_context: vec![],
            tools: vec![],
        };
        let native = ProviderContextItem {
            origin_message: None,
            wire_item_index: None,
            ordinal: 0,
            provider_origin: ProviderContextItem::test_origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({
                    "id": "cmp",
                    "type": "compaction",
                    "encrypted_content": "STALE"
                })],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 9,
                    context_fingerprint: context_fingerprint(&spec, &context).unwrap(),
                },
            },
        };
        context.provider_context.push(native);
        let request = build_request(
            &spec,
            &context,
            &RequestOptions {
                native_compaction: true,
                ..RequestOptions::default()
            },
        )
        .expect("coverage too high fallback");
        assert!(!request.to_string().contains("STALE"));
        assert!(request.to_string().contains("message-7"));
        assert!(request.to_string().contains("message-8"));
    }

    #[test]
    fn duplicate_output_item_ids_are_rejected_across_variants_transactionally() {
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        state
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"same","type":"message","role":"assistant","content":[]}}"#,
            )
            .unwrap();
        let before = (
            state.output_item_ids.len(),
            state.output_identities.len(),
            state.next_output_index,
        );
        let err = state
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":1,"output_index":1,"item":{"id":"same","type":"reasoning","summary":[]}}"#,
            )
            .expect_err("duplicate id across variants");
        assert!(err.to_string().contains("duplicate output item id"));
        assert_eq!(
            (
                state.output_item_ids.len(),
                state.output_identities.len(),
                state.next_output_index
            ),
            before
        );
        state
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":1,"output_index":1,"item":{"id":"different","type":"reasoning","summary":[]}}"#,
            )
            .expect("retry under same sequence number");
    }

    #[test]
    fn duplicate_message_output_item_id_is_rejected() {
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        state
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"m","type":"message","role":"assistant","content":[]}}"#,
            )
            .unwrap();
        assert!(state
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":1,"output_index":1,"item":{"id":"m","type":"message","role":"assistant","content":[]}}"#,
            )
            .is_err());
    }

    #[test]
    fn reasoning_summary_part_shapes_are_transactional_and_retryable() {
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        state
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"r","type":"reasoning","summary":[]}}"#,
            )
            .unwrap();

        let before_added = (
            state.next_sequence_number,
            state.next_summary_slot,
            state.content_bytes,
            state.event_count,
        );
        assert!(state
            .push_json(
                r#"{"type":"response.reasoning_summary_part.added","sequence_number":1,"item_id":"r","output_index":0,"summary_index":0}"#,
            )
            .is_err());
        assert_eq!(
            (
                state.next_sequence_number,
                state.next_summary_slot,
                state.content_bytes,
                state.event_count,
            ),
            before_added
        );
        state
            .push_json(
                r#"{"type":"response.reasoning_summary_part.added","sequence_number":1,"item_id":"r","output_index":0,"summary_index":0,"part":{"type":"summary_text","text":""}}"#,
            )
            .expect("valid retry at the same sequence");
        state
            .push_json(
                r#"{"type":"response.reasoning_summary_text.delta","sequence_number":2,"item_id":"r","output_index":0,"summary_index":0,"delta":"checked"}"#,
            )
            .unwrap();
        state
            .push_json(
                r#"{"type":"response.reasoning_summary_text.done","sequence_number":3,"item_id":"r","output_index":0,"summary_index":0,"text":"checked"}"#,
            )
            .unwrap();

        let before_done = (
            state.next_sequence_number,
            state.content_bytes,
            state.event_count,
        );
        assert!(state
            .push_json(
                r#"{"type":"response.reasoning_summary_part.done","sequence_number":4,"item_id":"r","output_index":0,"summary_index":0,"text":"checked"}"#,
            )
            .is_err());
        assert_eq!(
            (
                state.next_sequence_number,
                state.content_bytes,
                state.event_count,
            ),
            before_done
        );
        state
            .push_json(
                r#"{"type":"response.reasoning_summary_part.done","sequence_number":4,"item_id":"r","output_index":0,"summary_index":0,"part":{"type":"summary_text","text":"checked"}}"#,
            )
            .expect("valid done retry at the same sequence");
    }

    #[test]
    fn reasoning_item_content_is_validated_before_stream_state_or_context_mutation() {
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        assert!(state
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"r","type":"reasoning","summary":[],"content":{"type":"reasoning_text","text":"x"}}}"#,
            )
            .is_err());
        assert_eq!(state.next_sequence_number, 0);
        assert_eq!(state.next_output_index, 0);
        assert!(state.slots.is_empty());
        state
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"r","type":"reasoning","summary":[],"content":[{"type":"reasoning_text","text":"x"}]}}"#,
            )
            .expect("valid added retry at the same sequence");

        let before_done = (
            state.next_sequence_number,
            state.content_bytes,
            state.event_count,
            state.completed_items.len(),
            state.reasoning_fragments.len(),
        );
        assert!(state
            .push_json(
                r#"{"type":"response.output_item.done","sequence_number":1,"output_index":0,"item":{"id":"r","type":"reasoning","summary":[],"content":[{"type":"future_reasoning","text":"x"}],"encrypted_content":"opaque"}}"#,
            )
            .is_err());
        assert_eq!(
            (
                state.next_sequence_number,
                state.content_bytes,
                state.event_count,
                state.completed_items.len(),
                state.reasoning_fragments.len(),
            ),
            before_done
        );
        state
            .push_json(
                r#"{"type":"response.output_item.done","sequence_number":1,"output_index":0,"item":{"id":"r","type":"reasoning","summary":[],"content":[{"type":"reasoning_text","text":"x"}],"encrypted_content":"opaque"}}"#,
            )
            .expect("valid done retry at the same sequence");
        assert_eq!(state.completed_items.len(), 1);
        assert_eq!(state.reasoning_fragments.len(), 1);
    }

    #[test]
    fn canonical_official_optional_ids_and_tool_output_shapes() {
        validate_canonical_item(&json!({
            "type":"compaction",
            "encrypted_content":"opaque"
        }))
        .expect("compaction id is optional");
        validate_canonical_item(&json!({
            "type":"function_call",
            "call_id":"call",
            "name":"tool",
            "arguments":""
        }))
        .expect("function_call id is optional and empty arguments are valid");
        validate_canonical_item(&json!({
            "type":"function_call_output",
            "call_id":"call",
            "output":[{"type":"input_file"}]
        }))
        .expect("input_file fields are optional in the official shape");
        validate_canonical_item(&json!({
            "type":"function_call_output",
            "call_id":"call",
            "output":[{
                "type":"input_file",
                "detail":"high",
                "file_id":null,
                "file_url":"https://example.test/file.pdf",
                "prompt_cache_breakpoint":{"mode":"explicit"}
            }]
        }))
        .expect("all documented input_file fields and nullable file_id are valid");
        validate_canonical_item(&json!({
            "type":"compaction",
            "id":null,
            "encrypted_content":"opaque"
        }))
        .expect("compaction Optional id accepts null");

        for item in [
            json!({"type":"compaction","id":7,"encrypted_content":"opaque"}),
            json!({"type":"function_call","id":null,"call_id":"call","name":"tool","arguments":""}),
            json!({"type":"function_call","id":7,"call_id":"call","name":"tool","arguments":""}),
        ] {
            assert!(validate_canonical_item(&item).is_err(), "{item}");
        }

        validate_canonical_item(&json!({
            "type":"function_call_output",
            "call_id":"call",
            "output":[{"type":"input_file","file_id":"file-123"}]
        }))
        .expect("input_file with file_id is valid");
        validate_canonical_item(&json!({
            "type":"function_call_output",
            "call_id":"call",
            "output":[{"type":"input_file","file_data":"ZGF0YQ==","filename":"a.txt"}]
        }))
        .expect("input_file with file_data is valid");
        for item in [
            json!({"type":"function_call_output","call_id":"","output":"x"}),
            json!({"type":"function_call_output","call_id":"call","output":[{"type":"input_file","file_id":7}]}),
            json!({"type":"function_call_output","call_id":"call","output":[{"type":"input_file","file_id":"file-123","unexpected":"x"}]}),
            json!({"type":"function_call_output","call_id":"call","output":[{"type":"input_file","detail":null}]}),
            json!({"type":"function_call_output","call_id":"call","output":[{"type":"input_file","detail":"original"}]}),
            json!({"type":"function_call_output","call_id":"call","output":[{"type":"input_file","prompt_cache_breakpoint":{"mode":"future"}}]}),
        ] {
            assert!(validate_canonical_item(&item).is_err(), "{item}");
        }

        for item in [
            json!({"type":"function_call","id":"fc","call_id":"","name":"tool","arguments":""}),
            json!({"type":"function_call","id":"fc","call_id":"call","name":"","arguments":""}),
            json!({"type":"function_call","id":"fc","call_id":"call","name":"tool","arguments":null}),
        ] {
            assert!(validate_canonical_item(&item).is_err(), "{item}");
        }
    }

    #[test]
    fn canonical_variants_reject_unknown_fields_recursively() {
        for item in [
            json!({"id":"r","type":"reasoning","summary":[],"future":true}),
            json!({"id":"r","type":"reasoning","summary":[{"type":"summary_text","text":"x","future":true}]}),
            json!({"id":"r","type":"reasoning","summary":[],"content":[{"type":"reasoning_text","text":"x","future":true}]}),
            json!({"type":"function_call","call_id":"c","name":"f","arguments":"{}","future":true}),
            json!({"type":"function_call_output","call_id":"c","output":"x","future":true}),
            json!({"type":"compaction","encrypted_content":"x","future":true}),
        ] {
            assert!(validate_canonical_item(&item).is_err(), "{item}");
        }
    }

    #[test]
    fn streamed_function_call_without_optional_item_id_uses_output_index_identity() {
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        for event in [
            r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"type":"function_call","call_id":"call","name":"weather","arguments":""}}"#,
            r#"{"type":"response.function_call_arguments.delta","sequence_number":1,"item_id":"provider-local","output_index":0,"delta":"{\"city\":\"Tokyo\"}"}"#,
            r#"{"type":"response.output_item.done","sequence_number":2,"output_index":0,"item":{"type":"function_call","call_id":"call","name":"weather","arguments":"{\"city\":\"Tokyo\"}"}}"#,
            r#"{"type":"response.completed","sequence_number":3,"response":{"id":"resp","model":"gpt-5.6","status":"completed","output":[{"type":"function_call","call_id":"call","name":"weather","arguments":"{\"city\":\"Tokyo\"}"}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}"#,
        ] {
            state.push_json(event).expect("valid id-less function call");
        }
    }

    #[test]
    fn queued_response_establishes_model_only_when_supplied_later() {
        let mut state = ResponsesReceiveState::with_budget(schemas(), ResponseBudget::default());
        state
            .push_json(
                r#"{"type":"response.queued","sequence_number":0,"response":{"id":"resp","status":"queued","created_at":1,"updated_at":1}}"#,
            )
            .expect("official queued response omits model");
        assert_eq!(state.response_id.as_deref(), Some("resp"));
        assert!(state.response_model.is_none());
        state
            .push_json(
                r#"{"type":"response.in_progress","sequence_number":1,"response":{"id":"resp","model":"gpt-5.6","status":"in_progress","created_at":1}}"#,
            )
            .expect("later response event establishes model");
        assert_eq!(state.response_model.as_deref(), Some("gpt-5.6"));
        assert!(state
            .push_json(
                r#"{"type":"response.in_progress","sequence_number":2,"response":{"id":"resp","model":"other","status":"in_progress","created_at":1}}"#,
            )
            .is_err());
        assert_eq!(state.response_model.as_deref(), Some("gpt-5.6"));
        assert_eq!(state.next_sequence_number, 2);
    }

    #[test]
    fn canonical_compaction_preserves_official_created_by_shape() {
        let item = json!({
            "type":"compaction",
            "id":"cmp_123",
            "encrypted_content":"opaque",
            "created_by":"system"
        });
        validate_canonical_item(&item).expect("optional non-null created_by is accepted");
        let result = parse_compact_response(
            json!({"object":"response.compaction","output":[item]}),
            NativeCompactionCoverage {
                through_message_seq: 1,
                context_fingerprint: "fingerprint".into(),
            },
        )
        .expect("official compaction response");
        assert_eq!(result.items[0]["created_by"], "system");
        for invalid in [
            json!({"type":"compaction","encrypted_content":"opaque","created_by":null}),
            json!({"type":"compaction","encrypted_content":"opaque","created_by":7}),
        ] {
            assert!(validate_canonical_item(&invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn canonical_message_content_accepts_official_image_annotations_and_logprobs() {
        validate_canonical_item(&json!({
            "id":"msg",
            "type":"message",
            "role":"assistant",
            "status":"completed",
            "content":[
                {
                    "type":"input_image",
                    "detail":"original",
                    "file_id":"file_123",
                    "image_url":null,
                    "prompt_cache_breakpoint":{"mode":"explicit"}
                },
                {
                    "type":"output_text",
                    "text":"answer",
                    "annotations":[
                        {"type":"file_citation","file_id":"file_123","index":0,"filename":"a.txt"},
                        {"type":"url_citation","url":"https://example.test","start_index":0,"end_index":6,"title":"source"},
                        {"type":"container_file_citation","container_id":"ctr","file_id":"file_456","start_index":0,"end_index":6,"filename":"b.txt"},
                        {"type":"file_path","file_id":"file_789","index":1}
                    ],
                    "logprobs":[{
                        "token":"answer",
                        "logprob":-0.1,
                        "bytes":[97],
                        "top_logprobs":[{"token":"answer","logprob":-0.1,"bytes":[97]}]
                    }]
                }
            ]
        }))
        .expect("documented nested content variants are accepted");

        validate_canonical_item(&json!({
            "type":"function_call_output",
            "call_id":"call",
            "output":[{"type":"input_image","detail":"auto","file_id":"file_123"}]
        }))
        .expect("file-backed input_image does not require image_url");
        for image in [
            json!({"type":"input_image","detail":"auto"}),
            json!({"type":"input_image","detail":"high","file_id":"file_123","image_url":"https://example.test/image.png"}),
        ] {
            validate_canonical_item(&json!({
                "type":"function_call_output",
                "call_id":"call",
                "output":[image]
            }))
            .expect("official schema does not impose an exactly-one source constraint");
        }

        for invalid in [
            json!({"type":"message","role":"user","content":[{"type":"input_image","file_id":"file_123"}]}),
            json!({"type":"message","role":"user","content":[{"type":"input_image","detail":"auto","file_id":"file_123","future":true}]}),
            json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"x","annotations":[],"logprobs":[],"future":true}]}),
            json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"x","annotations":[{"type":"file_path","file_id":"f","index":0,"future":true}],"logprobs":[]}]}),
            json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"x","annotations":[],"logprobs":[{"token":"x","logprob":0,"bytes":[],"top_logprobs":[],"future":true}]}]}),
        ] {
            assert!(validate_canonical_item(&invalid).is_err(), "{invalid}");
        }
    }
}
