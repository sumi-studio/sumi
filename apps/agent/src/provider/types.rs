use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use chrono::{DateTime, Utc};
use futures_util::{Stream, task::AtomicWaker};
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::{Map, Value};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::assembler::{MessageAssembler, ResponseBudget};

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

impl From<PublicMessage> for Message {
    fn from(message: PublicMessage) -> Self {
        match message {
            PublicMessage::User(message) => Message::User(message),
            PublicMessage::ToolResult(message) => Message::ToolResult(message),
            PublicMessage::Assistant(assistant) => Message::Assistant(AssistantMessage {
                content: assistant
                    .content
                    .into_iter()
                    .map(|content| match content {
                        PublicAssistantContent::Text {
                            text,
                            wire_item_index,
                        } => AssistantContent::Text {
                            text,
                            wire_item_index,
                        },
                        PublicAssistantContent::Thinking {
                            thinking,
                            signature_field,
                            wire_item_index,
                        } => AssistantContent::Thinking {
                            thinking,
                            signature_field,
                            wire_item_index,
                        },
                        PublicAssistantContent::ToolCall {
                            tool_call,
                            wire_item_index,
                        } => AssistantContent::ToolCall {
                            tool_call,
                            wire_item_index,
                        },
                        PublicAssistantContent::RejectedToolCall {
                            rejected,
                            wire_item_index,
                        } => AssistantContent::RejectedToolCall {
                            rejected,
                            wire_item_index,
                        },
                    })
                    .collect(),
                model: assistant.model,
                provider: assistant.provider,
                origin: assistant.origin,
                usage: assistant.usage,
                stop_reason: assistant.stop_reason,
                error_message: assistant.error_message,
                provider_code: assistant.provider_code,
                interrupted: assistant.interrupted,
                timestamp: assistant.timestamp,
            }),
        }
    }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiProtocol {
    OpenAiChatCompletions,
    OpenAiResponses,
    AnthropicMessages,
}

/// Non-secret identity of the provider boundary that produced an assistant
/// message. Plaintext reasoning may only be replayed to this exact origin.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderOrigin {
    pub provider_instance_id: String,
    pub protocol: ApiProtocol,
    pub model: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProviderContextItem {
    /// Exact durable MessageEnd that owns this retention unit. This is
    /// deliberately separate from `origin_message`: native compaction has no
    /// semantic transcript origin, but still has one authenticated lifecycle
    /// owner for recovery and erasure.
    pub retention_owner: ProviderContextAnchor,
    pub origin_message: Option<ProviderContextAnchor>,
    pub wire_item_index: Option<u32>,
    /// Tie-breaker within the same `wire_item_index`, zero-based and assigned
    /// deterministically by the consumer that assembles the context list.
    pub ordinal: u32,
    pub provider_origin: ProviderOrigin,
    pub payload: ProviderContextPayload,
}

/// Validates the stable ordering metadata shared by durable hydration and
/// provider replay. Every authenticated anchor/wire group starts at ordinal
/// zero and has no duplicate or missing ordinal. Native windows form an
/// unanchored group scoped by their exact provider origin and payload kind.
pub fn validate_provider_context_ordinals(items: &[ProviderContextItem]) -> Result<(), String> {
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum Group {
        Anchored {
            message_id: String,
            message_seq: u64,
            wire_item_index: u32,
        },
        Native {
            retention_owner_message_id: String,
            retention_owner_message_seq: u64,
            provider_instance_id: String,
            protocol: ApiProtocol,
            model: String,
            kind: u8,
        },
    }

    let mut groups = BTreeMap::<Group, Vec<u32>>::new();
    for item in items {
        if item.retention_owner.message_id.is_empty() {
            return Err("provider context retention owner message_id is empty".to_owned());
        }
        let group = match &item.payload {
            ProviderContextPayload::EncryptedReasoning { .. } => {
                let anchor = item
                    .origin_message
                    .as_ref()
                    .ok_or_else(|| "encrypted reasoning is missing an origin message".to_owned())?;
                if anchor != &item.retention_owner {
                    return Err(
                        "encrypted reasoning origin message does not match its retention owner"
                            .to_owned(),
                    );
                }
                let wire_item_index = item
                    .wire_item_index
                    .ok_or_else(|| "encrypted reasoning is missing a wire_item_index".to_owned())?;
                Group::Anchored {
                    message_id: anchor.message_id.clone(),
                    message_seq: anchor.message_seq,
                    wire_item_index,
                }
            }
            ProviderContextPayload::OpenAiCompactedWindow { .. }
            | ProviderContextPayload::AnthropicCompaction { .. } => {
                if item.origin_message.is_some() || item.wire_item_index.is_some() {
                    return Err(
                        "native provider context must be unanchored and have no wire_item_index"
                            .to_owned(),
                    );
                }
                Group::Native {
                    retention_owner_message_id: item.retention_owner.message_id.clone(),
                    retention_owner_message_seq: item.retention_owner.message_seq,
                    provider_instance_id: item.provider_origin.provider_instance_id.clone(),
                    protocol: item.provider_origin.protocol,
                    model: item.provider_origin.model.clone(),
                    kind: match &item.payload {
                        ProviderContextPayload::OpenAiCompactedWindow { .. } => 1,
                        ProviderContextPayload::AnthropicCompaction { .. } => 2,
                        ProviderContextPayload::EncryptedReasoning { .. } => unreachable!(),
                    },
                }
            }
        };
        groups.entry(group).or_default().push(item.ordinal);
    }

    for (group, mut ordinals) in groups {
        ordinals.sort_unstable();
        for (expected, actual) in ordinals.into_iter().enumerate() {
            let expected = u32::try_from(expected).map_err(|_| {
                format!("provider context ordinal count overflows u32 for {group:?}")
            })?;
            if actual != expected {
                return Err(format!(
                    "provider context ordinals for {group:?} must be unique and contiguous from zero; expected {expected}, found {actual}"
                ));
            }
        }
    }
    Ok(())
}

/// Binds terminal fragments to the exact durable MessageEnd receipt anchor.
/// Ordinals are assigned independently within each wire slot.
pub fn bind_provider_context_fragments(
    fragments: Vec<ProviderContextFragment>,
    anchor: ProviderContextAnchor,
    provider_origin: ProviderOrigin,
) -> Result<Vec<ProviderContextItem>, String> {
    let mut next_ordinal = BTreeMap::<Option<u32>, u32>::new();
    let mut items = Vec::with_capacity(fragments.len());
    for fragment in fragments {
        let ordinal = next_ordinal.entry(fragment.wire_item_index).or_insert(0);
        let item = ProviderContextItem {
            retention_owner: anchor.clone(),
            origin_message: match &fragment.payload {
                ProviderContextPayload::EncryptedReasoning { .. } => Some(anchor.clone()),
                ProviderContextPayload::OpenAiCompactedWindow { .. }
                | ProviderContextPayload::AnthropicCompaction { .. } => None,
            },
            wire_item_index: fragment.wire_item_index,
            ordinal: *ordinal,
            provider_origin: provider_origin.clone(),
            payload: fragment.payload,
        };
        *ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| "provider context ordinal overflows u32".to_owned())?;
        items.push(item);
    }
    validate_provider_context_ordinals(&items)?;
    Ok(items)
}

#[cfg(test)]
impl ProviderContextItem {
    /// Test-only origin fixture for provider-context item construction.
    /// Production callers must supply the real provider origin that produced
    /// the assistant turn or compaction window.
    pub fn test_origin() -> ProviderOrigin {
        ProviderOrigin {
            provider_instance_id: "test-provider-instance".to_owned(),
            protocol: ApiProtocol::OpenAiResponses,
            model: "test-model".to_owned(),
        }
    }
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
    pub origin: ProviderOrigin,
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
    pub origin: ProviderOrigin,
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
    pub(super) fn from_schema_validated(arguments: Map<String, Value>) -> Self {
        Self(arguments)
    }

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
    IncompleteResponse,
    TooLarge,
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

#[derive(Clone, Debug, PartialEq, Serialize)]
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
        signature_field: String,
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
        synthetic_result: ToolResultMessage,
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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ProviderOutput {
    pub message: AssistantMessage,
    pub provider_context: Vec<ProviderContextFragment>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ProviderContextFragment {
    pub wire_item_index: Option<u32>,
    pub payload: ProviderContextPayload,
}

pub(crate) struct SuccessTerminalCommit {
    committed: AtomicBool,
    waker: AtomicWaker,
}

impl SuccessTerminalCommit {
    pub(crate) fn new() -> Self {
        Self {
            committed: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        }
    }

    pub(crate) fn commit(&self) {
        self.committed.store(true, Ordering::Release);
        self.waker.wake();
    }

    pub(crate) fn is_committed(&self) -> bool {
        self.committed.load(Ordering::Acquire)
    }

    fn register(&self, waker: &std::task::Waker) {
        self.waker.register(waker);
    }
}

pub struct ProviderEventStream {
    rx: Option<mpsc::Receiver<ProviderEvent>>,
    priority_terminal_rx: Option<mpsc::Receiver<ProviderEvent>>,
    ordered_prefix_drain_rx: Option<mpsc::Receiver<()>>,
    cancel: CancellationToken,
    provider: String,
    origin: ProviderOrigin,
    shadow: MessageAssembler,
    success_terminal_committed: Arc<SuccessTerminalCommit>,
    producer_task: Option<tokio::task::JoinHandle<()>>,
    // Only an explicitly marked terminal waits for its already-enqueued normal
    // prefix. Responses partial tool-call rejections carry their synthetic
    // result only in that prefix; ordinary failure terminals retain priority.
    pending_priority_terminal: Option<ProviderEvent>,
    start_emitted: bool,
    terminal_emitted: bool,
}

impl ProviderEventStream {
    pub fn new(
        rx: mpsc::Receiver<ProviderEvent>,
        cancel: CancellationToken,
        provider: impl Into<String>,
        origin: ProviderOrigin,
    ) -> Self {
        Self {
            rx: Some(rx),
            priority_terminal_rx: None,
            ordered_prefix_drain_rx: None,
            cancel,
            provider: provider.into(),
            origin,
            shadow: MessageAssembler::new(),
            success_terminal_committed: Arc::new(SuccessTerminalCommit::new()),
            producer_task: None,
            pending_priority_terminal: None,
            start_emitted: false,
            terminal_emitted: false,
        }
    }

    pub(crate) fn with_priority_terminal(
        rx: mpsc::Receiver<ProviderEvent>,
        priority_terminal_rx: mpsc::Receiver<ProviderEvent>,
        cancel: CancellationToken,
        provider: impl Into<String>,
        origin: ProviderOrigin,
        budget: ResponseBudget,
        success_terminal_committed: Arc<SuccessTerminalCommit>,
    ) -> Self {
        Self {
            rx: Some(rx),
            priority_terminal_rx: Some(priority_terminal_rx),
            ordered_prefix_drain_rx: None,
            cancel,
            provider: provider.into(),
            origin,
            shadow: MessageAssembler::with_budget(budget),
            success_terminal_committed,
            producer_task: None,
            pending_priority_terminal: None,
            start_emitted: false,
            terminal_emitted: false,
        }
    }

    /// Transfers the adapter producer into the stream ownership boundary.
    /// Dropping or fusing the stream aborts the child even if it fails to
    /// observe cooperative cancellation.
    pub(crate) fn own_producer(mut self, producer_task: tokio::task::JoinHandle<()>) -> Self {
        self.producer_task = Some(producer_task);
        self
    }

    /// Opts in only explicitly marked priority terminals to draining their
    /// already-queued normal prefix before consumer terminal validation.
    pub(crate) fn with_ordered_prefix_drain(mut self, rx: mpsc::Receiver<()>) -> Self {
        self.ordered_prefix_drain_rx = Some(rx);
        self
    }

    pub async fn recv(&mut self) -> Option<ProviderEvent> {
        futures_util::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
    }

    fn accept_event(&mut self, event: ProviderEvent) -> ProviderEvent {
        if event.is_terminal() {
            return self.accept_terminal(event);
        }
        if let Err(error) = self.shadow.apply(&event) {
            tracing::warn!(%error, "provider stream shadow rejected normalized event");
            let terminal = self.error_terminal(
                StopReason::Error,
                "provider stream emitted an invalid normalized event",
                "invalid_provider_event",
                false,
            );
            self.fuse();
            return terminal;
        }
        event
    }

    fn accept_terminal(&mut self, event: ProviderEvent) -> ProviderEvent {
        let accepted = if self.terminal_matches_expected(&event) {
            match self.shadow.apply(&event) {
                Ok(Some(_)) => event,
                Ok(None) => {
                    tracing::warn!("provider stream terminal did not finalize shadow assembler");
                    self.invalid_terminal()
                }
                Err(error) => {
                    tracing::warn!(%error, "provider stream shadow rejected terminal event");
                    self.invalid_terminal()
                }
            }
        } else {
            tracing::warn!("provider stream rejected terminal from unexpected provider origin");
            self.invalid_terminal()
        };
        self.fuse();
        accepted
    }

    fn terminal_matches_expected(&self, event: &ProviderEvent) -> bool {
        let message = match event {
            ProviderEvent::Done { output, .. } | ProviderEvent::Error { output, .. } => {
                &output.message
            }
            _ => return false,
        };
        message.provider == self.provider
            && message.model == self.origin.model
            && message.origin == self.origin
    }

    fn invalid_terminal(&mut self) -> ProviderEvent {
        self.error_terminal(
            StopReason::Error,
            "provider stream emitted an invalid terminal event",
            "invalid_provider_terminal",
            false,
        )
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

        let event = self.error_terminal(reason, error_message, provider_code, cancelled);
        self.fuse();
        event
    }

    fn error_terminal(
        &mut self,
        reason: StopReason,
        error_message: &str,
        provider_code: &str,
        interrupted: bool,
    ) -> ProviderEvent {
        let content = match if reason == StopReason::Aborted {
            self.shadow.authoritative_abort_content()
        } else {
            self.shadow.authoritative_error_content()
        } {
            Ok(content) => content,
            Err(error) => {
                tracing::warn!(%error, "provider stream shadow could not snapshot open content");
                if reason == StopReason::Aborted {
                    Vec::new()
                } else {
                    self.shadow.completed_content()
                }
            }
        };
        let event = ProviderEvent::Error {
            reason,
            output: ProviderOutput {
                message: AssistantMessage {
                    content,
                    model: self.origin.model.clone(),
                    provider: self.provider.clone(),
                    origin: self.origin.clone(),
                    usage: Usage::default(),
                    stop_reason: reason,
                    error_message: Some(error_message.to_owned()),
                    provider_code: Some(provider_code.to_owned()),
                    interrupted,
                    timestamp: Utc::now(),
                },
                provider_context: Vec::new(),
            },
        };
        if let Err(error) = self.shadow.apply(&event) {
            tracing::warn!(%error, "provider stream shadow rejected synthesized terminal event");
        }
        event
    }

    fn fuse(&mut self) {
        self.terminal_emitted = true;
        if let Some(task) = self.producer_task.take() {
            task.abort();
        }
        self.priority_terminal_rx.take();
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

impl Drop for ProviderEventStream {
    fn drop(&mut self) {
        // The stream is the consumer-side ownership boundary for the producer.
        // Dropping it must not detach an HTTP/SSE task until its transport
        // timeout; adapters observe this same token and close their producer.
        self.cancel.cancel();
        if let Some(task) = self.producer_task.take() {
            task.abort();
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
        if !self.start_emitted {
            self.start_emitted = true;
            if let Err(error) = self.shadow.apply(&ProviderEvent::Start) {
                tracing::warn!(%error, "provider stream shadow rejected synthetic Start");
            }
            return Poll::Ready(Some(ProviderEvent::Start));
        }

        let mut success_committed = self.success_terminal_committed.is_committed();
        if !success_committed {
            self.success_terminal_committed.register(cx.waker());
            success_committed = self.success_terminal_committed.is_committed();
        }
        if !success_committed {
            if let Some(terminal) = self.pending_priority_terminal.take() {
                if self.cancel.is_cancelled() {
                    return Poll::Ready(Some(self.accept_event(terminal)));
                }
                if let Some(rx) = self.rx.as_mut() {
                    match rx.poll_recv(cx) {
                        Poll::Ready(Some(ProviderEvent::Start)) => {
                            tracing::warn!("discarded duplicate producer Start event");
                            self.pending_priority_terminal = Some(terminal);
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                        Poll::Ready(Some(event)) => {
                            self.pending_priority_terminal = Some(terminal);
                            return Poll::Ready(Some(self.accept_event(event)));
                        }
                        Poll::Ready(None) | Poll::Pending => {}
                    }
                }
                return Poll::Ready(Some(self.accept_event(terminal)));
            }
            if let Some(priority_rx) = self.priority_terminal_rx.as_mut() {
                match priority_rx.poll_recv(cx) {
                    Poll::Ready(Some(event)) => {
                        debug_assert!(event.is_terminal());
                        if self.cancel.is_cancelled() {
                            return Poll::Ready(Some(self.accept_event(event)));
                        }
                        let drain_prefix = self
                            .ordered_prefix_drain_rx
                            .as_mut()
                            .is_some_and(|rx| matches!(rx.poll_recv(cx), Poll::Ready(Some(()))));
                        if drain_prefix && let Some(rx) = self.rx.as_mut() {
                            match rx.poll_recv(cx) {
                                Poll::Ready(Some(ProviderEvent::Start)) => {
                                    tracing::warn!("discarded duplicate producer Start event");
                                    self.pending_priority_terminal = Some(event);
                                    cx.waker().wake_by_ref();
                                    return Poll::Pending;
                                }
                                Poll::Ready(Some(normal)) => {
                                    self.pending_priority_terminal = Some(event);
                                    return Poll::Ready(Some(self.accept_event(normal)));
                                }
                                Poll::Ready(None) | Poll::Pending => {}
                            }
                        }
                        return Poll::Ready(Some(self.accept_event(event)));
                    }
                    Poll::Ready(None) => self.priority_terminal_rx = None,
                    Poll::Pending => {}
                }
            }

            if self.cancel.is_cancelled() {
                // Before a successful terminal commit, cancellation abandons
                // the normal backlog and waits only for the authoritative
                // priority terminal. A committed success instead drains the
                // ordered lane below even when cancellation becomes visible.
                return if self.priority_terminal_rx.is_some() {
                    Poll::Pending
                } else {
                    Poll::Ready(Some(self.synthesize_terminal()))
                };
            }
        }

        if self.rx.is_none() {
            return if !success_committed && self.priority_terminal_rx.is_some() {
                Poll::Pending
            } else {
                Poll::Ready(Some(self.synthesize_terminal()))
            };
        }
        let polled = self
            .rx
            .as_mut()
            .expect("receiver checked above")
            .poll_recv(cx);
        match polled {
            Poll::Ready(Some(ProviderEvent::Start)) => {
                tracing::warn!("discarded duplicate producer Start event");
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(Some(event)) => Poll::Ready(Some(self.accept_event(event))),
            Poll::Ready(None) => {
                self.rx = None;
                if !success_committed && self.priority_terminal_rx.is_some() {
                    Poll::Pending
                } else {
                    Poll::Ready(Some(self.synthesize_terminal()))
                }
            }
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

/// Validate native compaction invariants for `messages`.
///
/// Persisted message `seq` values are the global `agent_events.seq` assigned to
/// each `MessageEnd`, so non-message durable events create gaps. This validator
/// enforces:
///
/// 1. Persisted messages are strictly increasing with positive `seq`.
/// 2. Synthetic messages may appear only before the first persisted message.
/// 3. If `coverage` is `Some(seq)`, that `seq` must identify one of the persisted
///    messages — it is the global event sequence of the last transcript message
///    replaced by the window.
///
/// Returns the maximum persisted `seq` when at least one persisted message
/// exists and validation succeeds. Returns `None` only when `coverage` is
/// `None` and there are no persisted messages, so callers that only need
/// coverage can treat that as "no persisted history".
pub fn validate_native_suffix(
    messages: &[ContextMessage],
    coverage: Option<u64>,
) -> Result<Option<u64>, String> {
    let mut previous: Option<u64> = None;
    let mut persisted_started = false;
    let mut coverage_seen = false;
    for message in messages {
        let ContextMessage::Persisted { seq, .. } = message else {
            if persisted_started {
                return Err(
                    "native suffix contains synthetic content after persisted history".into(),
                );
            }
            continue;
        };
        persisted_started = true;
        if *seq == 0 {
            return Err("persisted native replay sequence must be greater than zero".into());
        }
        if previous.is_some_and(|value: u64| value >= *seq) {
            return Err(
                "persisted native replay sequence is duplicated, reordered, or not strictly increasing".into(),
            );
        }
        if coverage.is_some_and(|value| value == *seq) {
            coverage_seen = true;
        }
        previous = Some(*seq);
    }
    if coverage.is_some() && !coverage_seen {
        return Err("native compaction coverage does not identify a persisted message".into());
    }
    Ok(previous)
}

/// Hydration alias for [`validate_native_suffix`].
///
/// This is a convenience wrapper that retains the old `coverage: u64` signature
/// used by T17 hydration; it returns the maximum persisted `seq` on success.
pub fn validate_native_suffix_for_hydration(
    messages: &[ContextMessage],
    coverage: u64,
) -> Result<u64, String> {
    validate_native_suffix(messages, Some(coverage))?
        .ok_or_else(|| "native compaction requires persisted replay history".into())
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use super::*;
    use crate::provider::assembler::MessageAssembler;

    fn timestamp() -> DateTime<Utc> {
        Utc.timestamp_millis_opt(1_700_000_000_000)
            .single()
            .expect("valid timestamp")
    }

    fn tool_call() -> ToolCall {
        ToolCall {
            id: "call-1".to_owned(),
            name: "read_file".to_owned(),
            arguments: ValidatedToolArguments::from_schema_validated(
                json!({"path": "notes.txt"})
                    .as_object()
                    .expect("object")
                    .clone(),
            ),
        }
    }

    fn origin() -> ProviderOrigin {
        ProviderOrigin {
            provider_instance_id: "moonshot:https://api.moonshot.ai/v1".to_owned(),
            protocol: ApiProtocol::OpenAiChatCompletions,
            model: "kimi-k3".to_owned(),
        }
    }

    #[test]
    fn dropping_stream_cancels_its_producer_token() {
        let cancel = CancellationToken::new();
        let (_tx, rx) = mpsc::channel(1);
        let stream = ProviderEventStream::new(rx, cancel.clone(), "fixture", origin());
        assert!(!cancel.is_cancelled());
        drop(stream);
        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    async fn dropping_stream_aborts_a_held_owned_producer() {
        struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for DropSignal {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::channel(1);
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _drop_signal = DropSignal(Some(dropped_tx));
            let _tx = tx;
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.expect("producer started");
        let stream = ProviderEventStream::new(rx, cancel, "fixture", origin()).own_producer(task);
        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
            .await
            .expect("owned producer remained detached")
            .expect("drop signal sender");
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
            origin: origin(),
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

    fn empty_terminal_output(
        provider: &str,
        origin: ProviderOrigin,
        reason: StopReason,
    ) -> ProviderOutput {
        ProviderOutput {
            message: AssistantMessage {
                content: Vec::new(),
                model: origin.model.clone(),
                provider: provider.to_owned(),
                origin,
                usage: Usage::default(),
                stop_reason: reason,
                error_message: None,
                provider_code: Some("stop".to_owned()),
                interrupted: reason == StopReason::Aborted,
                timestamp: timestamp(),
            },
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
            reason: StopReason::Stop,
            output: empty_terminal_output("moonshot", origin(), StopReason::Stop),
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
            ProviderEventStream::new(rx, CancellationToken::new(), "moonshot", origin());
        assert!(matches!(stream.next().await, Some(ProviderEvent::Start)));
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

    #[tokio::test]
    async fn exact_expected_terminal_origin_is_accepted_and_fused() {
        let (tx, rx) = mpsc::channel(1);
        tx.send(ProviderEvent::Done {
            reason: StopReason::Stop,
            output: empty_terminal_output("moonshot", origin(), StopReason::Stop),
        })
        .await
        .expect("terminal");
        let mut stream =
            ProviderEventStream::new(rx, CancellationToken::new(), "moonshot", origin());
        let mut consumer = MessageAssembler::new();
        let start = stream.recv().await.expect("Start");
        consumer.apply(&start).expect("consumer Start");
        let terminal = stream.recv().await.expect("terminal");
        assert!(matches!(terminal, ProviderEvent::Done { .. }));
        assert!(
            consumer
                .apply(&terminal)
                .expect("trusted terminal")
                .is_some()
        );
        assert!(stream.recv().await.is_none());
    }

    #[tokio::test]
    async fn malformed_nonterminal_is_replaced_by_one_sanitized_terminal() {
        let (tx, rx) = mpsc::channel(3);
        tx.send(ProviderEvent::TextDelta {
            content_index: 0,
            delta: "must-not-cross".to_owned(),
        })
        .await
        .expect("malformed event");
        let mut leaked = empty_terminal_output("moonshot", origin(), StopReason::Stop);
        leaked.message.content = vec![AssistantContent::Text {
            text: "queued leak".to_owned(),
            wire_item_index: 0,
        }];
        leaked.provider_context = vec![ProviderContextFragment {
            wire_item_index: Some(0),
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::OpenAiChatCompletions,
                item: json!({"secret": "queued"}),
            },
        }];
        tx.send(ProviderEvent::Done {
            reason: StopReason::Stop,
            output: leaked,
        })
        .await
        .expect("queued terminal");

        let mut stream =
            ProviderEventStream::new(rx, CancellationToken::new(), "moonshot", origin());
        let mut consumer = MessageAssembler::new();
        consumer
            .apply(&stream.recv().await.expect("Start"))
            .expect("consumer Start");
        let terminal = stream.recv().await.expect("sanitized terminal");
        let ProviderEvent::Error { reason, output } = &terminal else {
            panic!("malformed event must become Error")
        };
        assert_eq!(*reason, StopReason::Error);
        assert_eq!(
            output.message.provider_code.as_deref(),
            Some("invalid_provider_event")
        );
        assert!(output.message.content.is_empty());
        assert!(output.provider_context.is_empty());
        assert_eq!(output.message.provider, "moonshot");
        assert_eq!(output.message.origin, origin());
        assert!(
            consumer
                .apply(&terminal)
                .expect("downstream accepts sanitized terminal")
                .is_some()
        );
        assert!(stream.recv().await.is_none(), "firewall terminal must fuse");
    }

    #[tokio::test]
    async fn family_and_budget_violations_keep_only_last_trusted_shadow_snapshot() {
        for malformed in [
            ProviderEvent::ThinkingDelta {
                content_index: 0,
                delta: "wrong-family".to_owned(),
            },
            ProviderEvent::TextDelta {
                content_index: 0,
                delta: "over".to_owned(),
            },
        ] {
            let (tx, rx) = mpsc::channel(4);
            tx.send(ProviderEvent::TextStart { content_index: 0 })
                .await
                .expect("text start");
            tx.send(ProviderEvent::TextDelta {
                content_index: 0,
                delta: "ok".to_owned(),
            })
            .await
            .expect("trusted delta");
            tx.send(malformed.clone()).await.expect("malformed event");
            let (_priority_tx, priority_rx) = mpsc::channel(1);
            let mut stream = ProviderEventStream::with_priority_terminal(
                rx,
                priority_rx,
                CancellationToken::new(),
                "moonshot",
                origin(),
                ResponseBudget {
                    max_content_bytes: 2,
                    ..ResponseBudget::default()
                },
                Arc::new(SuccessTerminalCommit::new()),
            );
            let mut consumer = MessageAssembler::with_budget(ResponseBudget {
                max_content_bytes: 2,
                ..ResponseBudget::default()
            });
            for _ in 0..3 {
                let event = stream.recv().await.expect("trusted prefix");
                consumer.apply(&event).expect("consumer trusted prefix");
            }
            let terminal = stream.recv().await.expect("firewall terminal");
            let ProviderEvent::Error { output, .. } = &terminal else {
                panic!("violation must become Error")
            };
            assert_eq!(
                output.message.content,
                vec![AssistantContent::Text {
                    text: "ok".to_owned(),
                    wire_item_index: 0,
                }]
            );
            assert!(output.provider_context.is_empty());
            assert!(
                consumer
                    .apply(&terminal)
                    .expect("consumer terminal")
                    .is_some()
            );
            assert!(stream.recv().await.is_none());
        }
    }

    #[tokio::test]
    async fn terminal_expected_metadata_mismatches_fail_closed_without_leaking_payload() {
        let expected = origin();
        let cases = [
            (
                "moonshot",
                ProviderOrigin {
                    provider_instance_id: "moonshot:https://other.example/v1".to_owned(),
                    ..expected.clone()
                },
            ),
            (
                "moonshot",
                ProviderOrigin {
                    protocol: ApiProtocol::OpenAiResponses,
                    ..expected.clone()
                },
            ),
            ("other-provider", expected.clone()),
            (
                "moonshot",
                ProviderOrigin {
                    model: "other-model".to_owned(),
                    ..expected.clone()
                },
            ),
        ];
        for (provider, untrusted_origin) in cases {
            let (tx, rx) = mpsc::channel(1);
            let mut output = empty_terminal_output(provider, untrusted_origin, StopReason::Stop);
            output.message.content = vec![AssistantContent::Text {
                text: "untrusted terminal content".to_owned(),
                wire_item_index: 0,
            }];
            output.provider_context = vec![ProviderContextFragment {
                wire_item_index: Some(0),
                payload: ProviderContextPayload::EncryptedReasoning {
                    protocol: ApiProtocol::OpenAiChatCompletions,
                    item: json!({"secret": "untrusted"}),
                },
            }];
            tx.send(ProviderEvent::Done {
                reason: StopReason::Stop,
                output,
            })
            .await
            .expect("untrusted terminal");

            let mut stream = ProviderEventStream::new(
                rx,
                CancellationToken::new(),
                "moonshot",
                expected.clone(),
            );
            let mut consumer = MessageAssembler::new();
            consumer
                .apply(&stream.recv().await.expect("Start"))
                .expect("consumer Start");
            let terminal = stream.recv().await.expect("sanitized terminal");
            let ProviderEvent::Error { reason, output } = &terminal else {
                panic!("mismatch must become Error")
            };
            assert_eq!(*reason, StopReason::Error);
            assert_eq!(output.message.provider, "moonshot");
            assert_eq!(output.message.origin, expected);
            assert!(output.message.content.is_empty());
            assert!(output.provider_context.is_empty());
            assert_eq!(
                output.message.provider_code.as_deref(),
                Some("invalid_provider_terminal")
            );
            assert!(
                consumer
                    .apply(&terminal)
                    .expect("sanitized terminal is assembler-valid")
                    .is_some()
            );
            assert!(stream.recv().await.is_none(), "mismatch must fuse");
        }
    }

    #[tokio::test]
    async fn committed_done_wins_over_later_cancellation_before_consumer_receive() {
        let (tx, rx) = mpsc::channel(1);
        let (_priority_tx, priority_rx) = mpsc::channel(1);
        let committed = Arc::new(SuccessTerminalCommit::new());
        tx.send(ProviderEvent::Done {
            reason: StopReason::Stop,
            output: empty_terminal_output("moonshot", origin(), StopReason::Stop),
        })
        .await
        .expect("queued Done");
        committed.commit();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut stream = ProviderEventStream::with_priority_terminal(
            rx,
            priority_rx,
            cancel,
            "moonshot",
            origin(),
            ResponseBudget::default(),
            committed,
        );
        let mut consumer = MessageAssembler::new();
        let start = stream.recv().await.expect("Start");
        consumer.apply(&start).expect("consumer Start");
        let terminal = stream.recv().await.expect("Done");
        assert!(matches!(terminal, ProviderEvent::Done { .. }));
        assert!(
            consumer
                .apply(&terminal)
                .expect("consumer accepts Done")
                .is_some()
        );
        assert!(stream.recv().await.is_none());
    }

    #[tokio::test]
    async fn precommit_cancellation_uses_one_well_formed_priority_aborted_terminal() {
        let (tx, rx) = mpsc::channel(1);
        tx.send(ProviderEvent::TextStart { content_index: 0 })
            .await
            .expect("normal backlog");
        let (priority_tx, priority_rx) = mpsc::channel(1);
        priority_tx
            .send(ProviderEvent::Error {
                reason: StopReason::Aborted,
                output: empty_terminal_output("moonshot", origin(), StopReason::Aborted),
            })
            .await
            .expect("priority Aborted");
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut stream = ProviderEventStream::with_priority_terminal(
            rx,
            priority_rx,
            cancel,
            "moonshot",
            origin(),
            ResponseBudget::default(),
            Arc::new(SuccessTerminalCommit::new()),
        );
        let mut consumer = MessageAssembler::new();
        let start = stream.recv().await.expect("Start");
        consumer.apply(&start).expect("consumer Start");
        let terminal = stream.recv().await.expect("Aborted");
        assert!(matches!(
            terminal,
            ProviderEvent::Error {
                reason: StopReason::Aborted,
                ..
            }
        ));
        assert!(
            consumer
                .apply(&terminal)
                .expect("consumer accepts Aborted")
                .is_some()
        );
        assert!(stream.recv().await.is_none());
    }

    #[tokio::test]
    async fn priority_provider_error_drops_observed_unsigned_thinking_without_synthesis() {
        let expected_origin = ProviderOrigin {
            provider_instance_id: "anthropic:https://api.anthropic.com".to_owned(),
            protocol: ApiProtocol::AnthropicMessages,
            model: "claude-sonnet-4-20250514".to_owned(),
        };
        let verified = AssistantContent::Text {
            text: "verified prior text".to_owned(),
            wire_item_index: 0,
        };
        let expected_output = ProviderOutput {
            message: AssistantMessage {
                content: vec![verified],
                model: expected_origin.model.clone(),
                provider: "anthropic".to_owned(),
                origin: expected_origin.clone(),
                usage: Usage {
                    input: 21,
                    output: 8,
                    cache_read: 5,
                    cache_write: 3,
                    reasoning: 4,
                    total_tokens: 41,
                },
                stop_reason: StopReason::Error,
                error_message: Some("upstream overloaded".to_owned()),
                provider_code: Some("overloaded_error".to_owned()),
                interrupted: false,
                timestamp: timestamp(),
            },
            provider_context: vec![ProviderContextFragment {
                wire_item_index: Some(0),
                payload: ProviderContextPayload::EncryptedReasoning {
                    protocol: ApiProtocol::AnthropicMessages,
                    item: json!({"type": "redacted_thinking", "data": "verified-context"}),
                },
            }],
        };
        let (tx, rx) = mpsc::channel(5);
        for event in [
            ProviderEvent::TextStart { content_index: 0 },
            ProviderEvent::TextDelta {
                content_index: 0,
                delta: "verified prior text".to_owned(),
            },
            ProviderEvent::TextEnd {
                content_index: 0,
                content: "verified prior text".to_owned(),
            },
            ProviderEvent::ThinkingStart {
                content_index: 1,
                signature_field: "signature".to_owned(),
            },
            ProviderEvent::ThinkingDelta {
                content_index: 1,
                delta: "unsigned prefix".to_owned(),
            },
        ] {
            tx.send(event).await.expect("normal event");
        }
        let (priority_tx, priority_rx) = mpsc::channel(1);
        let mut stream = ProviderEventStream::with_priority_terminal(
            rx,
            priority_rx,
            CancellationToken::new(),
            "anthropic",
            expected_origin,
            ResponseBudget::default(),
            Arc::new(SuccessTerminalCommit::new()),
        );
        let mut consumer = MessageAssembler::new();
        for _ in 0..=5 {
            let event = stream.recv().await.expect("observed prefix");
            consumer.apply(&event).expect("consumer accepts prefix");
        }

        let expected_terminal = ProviderEvent::Error {
            reason: StopReason::Error,
            output: expected_output.clone(),
        };
        priority_tx
            .send(expected_terminal.clone())
            .await
            .expect("provider error");
        let terminal = stream.recv().await.expect("provider terminal");
        assert_eq!(terminal, expected_terminal);
        let message = consumer
            .apply(&terminal)
            .expect("consumer reconciles provider terminal")
            .expect("terminal message");
        assert_eq!(message, expected_output.message);
        assert!(stream.recv().await.is_none());
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

    #[test]
    fn terminal_queue_audit_reports_no_more_at_or_below_limit() {
        for queued in [32, 10] {
            let (tx, mut rx) = mpsc::channel(queued);
            for content_index in 0..queued {
                tx.try_send(ProviderEvent::TextDelta {
                    content_index,
                    delta: "late".to_owned(),
                })
                .expect("queue test event");
            }

            assert_eq!(audit_queued_events(&mut rx, 32), (queued, false));
        }
    }

    #[tokio::test]
    async fn provider_stream_synthesizes_one_terminal_event_on_eof() {
        let (tx, rx) = mpsc::channel(1);
        drop(tx);
        let mut stream =
            ProviderEventStream::new(rx, CancellationToken::new(), "moonshot", origin());

        assert!(matches!(stream.recv().await, Some(ProviderEvent::Start)));
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
    async fn synthesized_eof_replays_completed_content_without_terminal_mismatch() {
        let (tx, rx) = mpsc::channel(4);
        for event in [
            ProviderEvent::TextStart { content_index: 0 },
            ProviderEvent::TextDelta {
                content_index: 0,
                delta: "accepted".to_owned(),
            },
            ProviderEvent::TextEnd {
                content_index: 0,
                content: "accepted".to_owned(),
            },
        ] {
            tx.send(event).await.expect("event");
        }
        drop(tx);

        let mut stream =
            ProviderEventStream::new(rx, CancellationToken::new(), "moonshot", origin());
        let mut consumer = MessageAssembler::new();
        let mut terminal = None;
        while let Some(event) = stream.recv().await {
            if let Some(message) = consumer.apply(&event).expect("consumer replay") {
                terminal = Some(message);
            }
        }

        assert_eq!(
            terminal.expect("terminal").content,
            vec![AssistantContent::Text {
                text: "accepted".to_owned(),
                wire_item_index: 0,
            }]
        );
    }

    #[tokio::test]
    async fn synthesized_eof_replays_open_content_without_terminal_mismatch() {
        let (tx, rx) = mpsc::channel(2);
        tx.send(ProviderEvent::TextStart { content_index: 3 })
            .await
            .expect("start");
        tx.send(ProviderEvent::TextDelta {
            content_index: 3,
            delta: "partial".to_owned(),
        })
        .await
        .expect("delta");
        drop(tx);

        let mut stream =
            ProviderEventStream::new(rx, CancellationToken::new(), "moonshot", origin());
        let mut consumer = MessageAssembler::new();
        let mut terminal = None;
        while let Some(event) = stream.recv().await {
            if let Some(message) = consumer.apply(&event).expect("consumer replay") {
                terminal = Some(message);
            }
        }

        assert_eq!(
            terminal.expect("terminal").content,
            vec![AssistantContent::Text {
                text: "partial".to_owned(),
                wire_item_index: 3,
            }]
        );
    }

    #[tokio::test]
    async fn provider_stream_classifies_cancelled_eof_as_aborted() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let (tx, rx) = mpsc::channel(1);
        drop(tx);
        let mut stream = ProviderEventStream::new(rx, cancel, "moonshot", origin());

        assert!(matches!(stream.recv().await, Some(ProviderEvent::Start)));
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

    #[test]
    fn validate_native_suffix_accepts_gapped_positive_sequences() {
        let messages = vec![
            ContextMessage::Persisted {
                id: "m-4".to_owned(),
                seq: 4,
                message: Message::Assistant(assistant_message()),
            },
            ContextMessage::Persisted {
                id: "m-6".to_owned(),
                seq: 6,
                message: Message::Assistant(assistant_message()),
            },
        ];
        assert_eq!(
            validate_native_suffix(&messages, Some(4))
                .expect("gapped coverage must identify a persisted message"),
            Some(6)
        );
        assert_eq!(
            validate_native_suffix(&messages, Some(6))
                .expect("coverage may be the last persisted message"),
            Some(6)
        );
        assert_eq!(
            validate_native_suffix(&messages, None)
                .expect("gapped messages without coverage should return max seq"),
            Some(6)
        );
    }

    #[test]
    fn validate_native_suffix_accepts_leading_synthetic_then_positive_seq() {
        let messages = vec![
            ContextMessage::Synthetic {
                message: Message::Assistant(assistant_message()),
            },
            ContextMessage::Persisted {
                id: "m-2".to_owned(),
                seq: 2,
                message: Message::Assistant(assistant_message()),
            },
            ContextMessage::Persisted {
                id: "m-5".to_owned(),
                seq: 5,
                message: Message::Assistant(assistant_message()),
            },
        ];
        validate_native_suffix(&messages, Some(5))
            .expect("leading synthetic + gapped persisted must pass");
    }

    #[test]
    fn validate_native_suffix_returns_none_when_no_persisted_messages_and_no_coverage() {
        assert_eq!(validate_native_suffix(&[], None), Ok(None));
    }

    #[test]
    fn validate_native_suffix_rejects_invalid_order_and_missing_coverage() {
        let ordered = vec![
            ContextMessage::Persisted {
                id: "m-4".to_owned(),
                seq: 4,
                message: Message::Assistant(assistant_message()),
            },
            ContextMessage::Persisted {
                id: "m-6".to_owned(),
                seq: 6,
                message: Message::Assistant(assistant_message()),
            },
        ];
        let error = validate_native_suffix(&ordered, Some(5))
            .expect_err("coverage must identify a persisted message");
        assert!(
            error.contains("does not identify a persisted message"),
            "{error}"
        );

        let reordered = vec![ordered[1].clone(), ordered[0].clone()];
        let error = validate_native_suffix(&reordered, Some(4))
            .expect_err("persisted messages must remain ordered");
        assert!(error.contains("reordered"), "{error}");

        let duplicate = vec![
            ordered[0].clone(),
            ContextMessage::Persisted {
                id: "m-4b".to_owned(),
                seq: 4,
                message: Message::Assistant(assistant_message()),
            },
        ];
        assert!(validate_native_suffix(&duplicate, Some(4)).is_err());

        let seq_zero = vec![ContextMessage::Persisted {
            id: "m-0".to_owned(),
            seq: 0,
            message: Message::Assistant(assistant_message()),
        }];
        assert!(validate_native_suffix(&seq_zero, Some(0)).is_err());

        let synthetic_after_persisted = vec![
            ordered[0].clone(),
            ContextMessage::Synthetic {
                message: Message::Assistant(assistant_message()),
            },
        ];
        assert!(validate_native_suffix(&synthetic_after_persisted, Some(4)).is_err());
    }

    #[test]
    fn provider_context_ordinals_are_zero_based_unique_and_contiguous_per_wire() {
        let anchor = ProviderContextAnchor {
            message_id: "assistant-1".to_owned(),
            message_seq: 7,
        };
        let item = |wire_item_index, ordinal| ProviderContextItem {
            retention_owner: anchor.clone(),
            origin_message: Some(anchor.clone()),
            wire_item_index: Some(wire_item_index),
            ordinal,
            provider_origin: origin(),
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::OpenAiResponses,
                item: json!({
                    "id": format!("reasoning-{wire_item_index}-{ordinal}"),
                    "type": "reasoning",
                    "summary": [],
                    "encrypted_content": "opaque",
                }),
            },
        };

        validate_provider_context_ordinals(&[item(0, 0), item(0, 1), item(2, 0)])
            .expect("each wire group is independently contiguous");
        for invalid in [
            vec![item(0, 1)],
            vec![item(0, 0), item(0, 0)],
            vec![item(0, 0), item(0, 2)],
        ] {
            assert!(
                validate_provider_context_ordinals(&invalid).is_err(),
                "{invalid:?}"
            );
        }

        let native = ProviderContextItem {
            retention_owner: anchor,
            origin_message: None,
            wire_item_index: None,
            ordinal: 1,
            provider_origin: origin(),
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({"type":"compaction","encrypted_content":"opaque"})],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 7,
                    context_fingerprint: "fingerprint".to_owned(),
                },
            },
        };
        assert!(validate_provider_context_ordinals(&[native]).is_err());
    }

    #[test]
    fn native_binding_keeps_semantic_origin_none_and_authenticates_retention_owner() {
        let owner = ProviderContextAnchor {
            message_id: "assistant-error".to_owned(),
            message_seq: 11,
        };
        let items = bind_provider_context_fragments(
            vec![ProviderContextFragment {
                wire_item_index: None,
                payload: ProviderContextPayload::OpenAiCompactedWindow {
                    items: vec![json!({
                        "id": "cmp-error",
                        "type": "compaction",
                        "encrypted_content": "opaque",
                    })],
                    coverage: NativeCompactionCoverage {
                        through_message_seq: 9,
                        context_fingerprint: "fp-error".to_owned(),
                    },
                },
            }],
            owner.clone(),
            origin(),
        )
        .expect("bind native Error context");

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].retention_owner, owner);
        assert_eq!(items[0].origin_message, None);
    }
}
