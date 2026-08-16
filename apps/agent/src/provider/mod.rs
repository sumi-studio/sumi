//! Provider transport, protocol adapters, and normalized event assembly.

pub mod adapters;
pub mod assembler;
pub(crate) mod canonical_request;
pub(crate) mod context_fingerprint;
pub mod model;
pub mod overflow;
pub mod partial_json;
// Provider prerequisite consumed by T19 once its memory estimator lands.
#[allow(dead_code)]
pub(crate) mod replay_probe;
pub mod retry;
pub mod transport;
pub mod types;

use std::{
    env,
    future::Future,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use adapters::anthropic::{
    AnthropicAdapterError, AnthropicReceiveState, AnthropicTerminal,
    build_request as build_anthropic_request, request_coverage as anthropic_request_coverage,
    requested_output_tokens as anthropic_requested_output_tokens,
};
use adapters::chat_completions::{
    ChatAdapterError, ChatReceiveState, ChatTerminal, build_request,
    requested_output_tokens as chat_requested_output_tokens,
};
pub use adapters::responses::NativeCompactionResult;
use adapters::responses::{
    ResponsesAdapterError, ResponsesReceiveState, ResponsesTerminal, build_compact_request,
    build_request as build_responses_request,
    derive_compaction_coverage as responses_compaction_coverage, parse_compact_response,
    requested_output_tokens as responses_requested_output_tokens, validate_event_name,
};
use assembler::{FrozenToolSchemaRegistry, MessageAssembler, ResponseBudget, TerminalMetadata};
use canonical_request::CanonicalRequestBody;
use chrono::Utc;
use futures_util::StreamExt;
pub use model::{
    AnthropicCompat, ChatCompat, ModelSpec, ProtocolCompat, RequestOptions, ResponsesCompat,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use transport::{SseError, SseStream};
use types::{
    ApiProtocol, AssistantMessage, PromptContext, ProviderEvent, ProviderEventStream,
    ProviderOutput, StopReason, SuccessTerminalCommit, Usage,
};

const EVENT_CHANNEL_CAPACITY: usize = 64;
const TIMING_OBSERVATION_CAPACITY: usize = 2;
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(120);
pub(crate) const RESPONSE_BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
pub(crate) const MAX_PROVIDER_ERROR_BODY_BYTES: usize = 16_000;

enum RequestWait<T, E> {
    Response {
        response: Result<T, E>,
        request_sent_at: Instant,
    },
    Cancelled,
    TimedOut,
}

#[derive(Debug, thiserror::Error)]
pub enum NativeCompactionError {
    #[error("native compaction was cancelled")]
    Cancelled,
    #[error("native compaction request is invalid: {0}")]
    InvalidRequest(String),
    #[error("native compaction transport failed: {0}")]
    Transport(String),
    #[error("native compaction response headers timed out")]
    HeaderTimeout,
    #[error("native compaction response body was idle for 120 seconds")]
    BodyIdleTimeout,
    #[error("{status}: {body}")]
    Http { status: u16, body: String },
    #[error("native compaction response exceeded {limit} bytes")]
    ResponseLimitExceeded { limit: usize },
    #[error("native compaction response is invalid: {0}")]
    InvalidResponse(String),
}

struct ProducerChannels {
    normal: mpsc::Sender<ProviderEvent>,
    priority_terminal: mpsc::Sender<ProviderEvent>,
    ordered_prefix_drain: Option<mpsc::Sender<()>>,
    success_terminal_committed: Arc<SuccessTerminalCommit>,
    timing: Option<ProviderTimingObserver>,
}

/// Monotonic provider timing observations consumed by the agent run driver.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderTimingObservation {
    RequestSent(Instant),
    FirstPublicDelta(Instant),
}

pub(crate) struct ProviderTimingObserver {
    sender: mpsc::Sender<ProviderTimingObservation>,
}

#[allow(dead_code)] // T15 consumes this receiver from ProductionRunDriver.
pub(crate) struct ProviderTimingObservations {
    receiver: mpsc::Receiver<ProviderTimingObservation>,
}

#[allow(dead_code)] // T15 consumes this receiver from ProductionRunDriver.
impl ProviderTimingObservations {
    pub(crate) async fn recv(&mut self) -> Option<ProviderTimingObservation> {
        self.receiver.recv().await
    }
}

/// Creates the fixed-size observation lane. The producer never awaits this lane.
#[allow(dead_code)] // T15 connects this seam to ProductionRunDriver.
pub(crate) fn timing_observation_channel() -> (ProviderTimingObserver, ProviderTimingObservations) {
    let (sender, receiver) = mpsc::channel(TIMING_OBSERVATION_CAPACITY);
    (
        ProviderTimingObserver { sender },
        ProviderTimingObservations { receiver },
    )
}

impl ProviderTimingObserver {
    pub(crate) fn observe(&self, observation: ProviderTimingObservation) {
        let _ = self.sender.try_send(observation);
    }
}

async fn await_request<F, T, E, O>(
    request: F,
    cancel: &CancellationToken,
    timeout: Duration,
    on_first_poll: O,
) -> RequestWait<T, E>
where
    F: Future<Output = Result<T, E>>,
    O: FnOnce(Instant),
{
    tokio::select! {
        biased;
        _ = cancel.cancelled() => RequestWait::Cancelled,
        result = async move {
            let request_sent_at = Instant::now();
            on_first_poll(request_sent_at);
            (request.await, request_sent_at)
        } => RequestWait::Response {
            response: result.0,
            request_sent_at: result.1,
        },
        _ = tokio::time::sleep(timeout) => RequestWait::TimedOut,
    }
}

struct TtftObservation {
    request_sent_at: Instant,
    first_public_delta_sent: bool,
    observer: Option<ProviderTimingObserver>,
}

impl TtftObservation {
    fn new(request_sent_at: Instant, observer: Option<ProviderTimingObserver>) -> Self {
        Self {
            request_sent_at,
            first_public_delta_sent: false,
            observer,
        }
    }

    fn observe_request_sent(observer: &Option<ProviderTimingObserver>, at: Instant) {
        tracing::info!(phase = "request_sent", "provider request sent");
        if let Some(observer) = observer {
            observer.observe(ProviderTimingObservation::RequestSent(at));
        }
    }

    fn observe_emit(&mut self, is_public_delta: bool, result: &EmitResult) {
        if is_public_delta && !self.first_public_delta_sent && matches!(result, EmitResult::Sent) {
            self.first_public_delta_sent = true;
            let observed_at = Instant::now();
            if let Some(observer) = &self.observer {
                observer.observe(ProviderTimingObservation::FirstPublicDelta(observed_at));
            }
            tracing::info!(
                phase = "request_sent_to_first_public_delta",
                elapsed_ms = observed_at.duration_since(self.request_sent_at).as_millis() as u64,
                "provider first public delta"
            );
        }
    }
}

pub(crate) fn http_client() -> Result<&'static reqwest::Client, String> {
    static CLIENT: OnceLock<Result<reqwest::Client, String>> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .build()
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|error| format!("failed to build HTTP client: {error}"))
}

pub fn stream(
    spec: ModelSpec,
    context: PromptContext,
    options: RequestOptions,
    cancel: CancellationToken,
) -> ProviderEventStream {
    let api_key = env::var(&spec.api_key_env).ok();
    stream_with_api_key(spec, context, options, cancel, api_key)
}

/// Starts a provider stream with a non-blocking, bounded timing observer.
#[allow(dead_code)] // T15 connects this seam to ProductionRunDriver.
pub(crate) fn stream_observed(
    spec: ModelSpec,
    context: PromptContext,
    options: RequestOptions,
    cancel: CancellationToken,
    observer: ProviderTimingObserver,
) -> ProviderEventStream {
    let api_key = env::var(&spec.api_key_env).ok();
    stream_with_api_key_observed(spec, context, options, cancel, api_key, Some(observer))
}

pub async fn compact_native(
    spec: ModelSpec,
    context: PromptContext,
    cancel: CancellationToken,
) -> Result<NativeCompactionResult, NativeCompactionError> {
    responses_compaction_coverage(&spec, &context)
        .map_err(|error| NativeCompactionError::InvalidRequest(error.to_string()))?;
    let api_key = env::var(&spec.api_key_env)
        .ok()
        .filter(|key| !key.is_empty())
        .ok_or_else(|| {
            NativeCompactionError::InvalidRequest(format!(
                "missing API key environment variable {}",
                spec.api_key_env
            ))
        })?;
    compact_native_with_api_key(spec, context, cancel, api_key).await
}

async fn compact_native_with_api_key(
    spec: ModelSpec,
    context: PromptContext,
    cancel: CancellationToken,
    api_key: String,
) -> Result<NativeCompactionResult, NativeCompactionError> {
    let coverage = responses_compaction_coverage(&spec, &context)
        .map_err(|error| NativeCompactionError::InvalidRequest(error.to_string()))?;
    let body = build_compact_request(&spec, &context)
        .map_err(|error| NativeCompactionError::InvalidRequest(error.to_string()))?;
    let client = http_client().map_err(NativeCompactionError::Transport)?;
    let body = CanonicalRequestBody::serialize(&body)
        .map_err(|error| NativeCompactionError::InvalidRequest(error.to_string()))?;
    let request = body
        .apply(client.post(spec.compact_endpoint()).bearer_auth(api_key))
        .send();
    let response = match await_request(request, &cancel, RESPONSE_HEADER_TIMEOUT, |_| {}).await {
        RequestWait::Cancelled => return Err(NativeCompactionError::Cancelled),
        RequestWait::TimedOut => return Err(NativeCompactionError::HeaderTimeout),
        RequestWait::Response {
            response: Err(error),
            ..
        } => {
            return Err(NativeCompactionError::Transport(error.to_string()));
        }
        RequestWait::Response {
            response: Ok(response),
            ..
        } => response,
    };
    let status = response.status();
    let max_bytes = ResponseBudget::for_output_tokens(spec.max_output_tokens)
        .ok_or_else(|| {
            NativeCompactionError::InvalidRequest(
                "max output budget cannot be represented on this platform".into(),
            )
        })?
        .max_wire_bytes;
    let success = status.is_success();
    let body_limit = if success {
        max_bytes
    } else {
        MAX_PROVIDER_ERROR_BODY_BYTES
    };
    let bytes = retain_compact_http_status(
        status,
        collect_bounded_body(response, body_limit, !success, &cancel).await,
    )?;
    if !status.is_success() {
        let body = String::from_utf8_lossy(&bytes)
            .chars()
            .take(4_000)
            .collect();
        return Err(NativeCompactionError::Http {
            status: status.as_u16(),
            body,
        });
    }
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| NativeCompactionError::InvalidResponse(error.to_string()))?;
    parse_compact_response(value, coverage)
        .map_err(|error| NativeCompactionError::InvalidResponse(error.to_string()))
}

fn retain_compact_http_status(
    status: reqwest::StatusCode,
    body: Result<Vec<u8>, NativeCompactionError>,
) -> Result<Vec<u8>, NativeCompactionError> {
    match body {
        Err(NativeCompactionError::Cancelled) => Err(NativeCompactionError::Cancelled),
        Err(error) if !status.is_success() => Err(NativeCompactionError::Http {
            status: status.as_u16(),
            body: format!("failed to read response body: {error}"),
        }),
        result => result,
    }
}

async fn collect_bounded_body(
    response: reqwest::Response,
    limit: usize,
    truncate: bool,
    cancel: &CancellationToken,
) -> Result<Vec<u8>, NativeCompactionError> {
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    loop {
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(NativeCompactionError::Cancelled),
            chunk = stream.next() => chunk,
            _ = tokio::time::sleep(RESPONSE_BODY_IDLE_TIMEOUT) => {
                return Err(NativeCompactionError::BodyIdleTimeout);
            }
        };
        let Some(chunk) = next else {
            return Ok(body);
        };
        let chunk = chunk.map_err(|error| NativeCompactionError::Transport(error.to_string()))?;
        let next_len = body
            .len()
            .checked_add(chunk.len())
            .ok_or(NativeCompactionError::ResponseLimitExceeded { limit })?;
        if next_len > limit {
            if truncate {
                body.extend_from_slice(&chunk[..limit.saturating_sub(body.len())]);
                return Ok(body);
            }
            return Err(NativeCompactionError::ResponseLimitExceeded { limit });
        }
        body.extend_from_slice(&chunk);
    }
}

fn stream_with_api_key(
    spec: ModelSpec,
    context: PromptContext,
    options: RequestOptions,
    cancel: CancellationToken,
    api_key: Option<String>,
) -> ProviderEventStream {
    stream_with_api_key_observed(spec, context, options, cancel, api_key, None)
}

pub(crate) fn stream_with_api_key_observed(
    spec: ModelSpec,
    context: PromptContext,
    options: RequestOptions,
    cancel: CancellationToken,
    api_key: Option<String>,
    observer: Option<ProviderTimingObserver>,
) -> ProviderEventStream {
    match spec.protocol {
        ApiProtocol::OpenAiChatCompletions => {
            stream_chat_with_api_key(spec, context, options, cancel, api_key, observer)
        }
        ApiProtocol::OpenAiResponses => {
            stream_responses_with_api_key(spec, context, options, cancel, api_key, observer)
        }
        ApiProtocol::AnthropicMessages => {
            stream_anthropic_with_api_key(spec, context, options, cancel, api_key, observer)
        }
    }
}

fn stream_chat_with_api_key(
    spec: ModelSpec,
    context: PromptContext,
    options: RequestOptions,
    cancel: CancellationToken,
    api_key: Option<String>,
    observer: Option<ProviderTimingObserver>,
) -> ProviderEventStream {
    let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    let (priority_terminal_tx, priority_terminal_rx) = mpsc::channel(1);
    let origin = spec.origin();
    let provider = spec.provider.clone();
    let stream_budget = chat_requested_output_tokens(&spec, &options)
        .ok()
        .and_then(ResponseBudget::for_output_tokens)
        .unwrap_or_default();
    let success_terminal_committed = Arc::new(SuccessTerminalCommit::new());
    let stream_cancel = cancel.clone();
    let producer_terminal_committed = success_terminal_committed.clone();
    let span = tracing::info_span!(
        "provider_request",
        provider = %spec.provider,
        model = %spec.id,
        protocol = "open_ai_chat_completions"
    );
    let producer_task = tokio::spawn(
        async move {
            run_chat_stream(
                spec,
                context,
                options,
                stream_cancel,
                api_key,
                ProducerChannels {
                    normal: tx,
                    priority_terminal: priority_terminal_tx,
                    ordered_prefix_drain: None,
                    success_terminal_committed: producer_terminal_committed,
                    timing: observer,
                },
            )
            .await;
        }
        .instrument(span),
    );
    ProviderEventStream::with_priority_terminal(
        rx,
        priority_terminal_rx,
        cancel,
        provider,
        origin,
        stream_budget,
        success_terminal_committed,
    )
    .own_producer(producer_task)
}

fn stream_responses_with_api_key(
    spec: ModelSpec,
    context: PromptContext,
    options: RequestOptions,
    cancel: CancellationToken,
    api_key: Option<String>,
    observer: Option<ProviderTimingObserver>,
) -> ProviderEventStream {
    let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    let (priority_terminal_tx, priority_terminal_rx) = mpsc::channel(1);
    let (ordered_prefix_drain_tx, ordered_prefix_drain_rx) = mpsc::channel(1);
    let origin = spec.origin();
    let provider = spec.provider.clone();
    let stream_budget = responses_requested_output_tokens(&spec, &options)
        .ok()
        .and_then(ResponseBudget::for_output_tokens)
        .unwrap_or_default();
    let success_terminal_committed = Arc::new(SuccessTerminalCommit::new());
    let producer_terminal_committed = success_terminal_committed.clone();
    let stream_cancel = cancel.clone();
    let span = tracing::info_span!(
        "provider_request",
        provider = %spec.provider,
        model = %spec.id,
        protocol = "open_ai_responses"
    );
    let producer_task = tokio::spawn(
        async move {
            run_responses_stream(
                spec,
                context,
                options,
                stream_cancel,
                api_key,
                ProducerChannels {
                    normal: tx,
                    priority_terminal: priority_terminal_tx,
                    ordered_prefix_drain: Some(ordered_prefix_drain_tx),
                    success_terminal_committed: producer_terminal_committed,
                    timing: observer,
                },
            )
            .await;
        }
        .instrument(span),
    );
    ProviderEventStream::with_priority_terminal(
        rx,
        priority_terminal_rx,
        cancel,
        provider,
        origin,
        stream_budget,
        success_terminal_committed,
    )
    .with_ordered_prefix_drain(ordered_prefix_drain_rx)
    .own_producer(producer_task)
}

fn stream_anthropic_with_api_key(
    spec: ModelSpec,
    context: PromptContext,
    options: RequestOptions,
    cancel: CancellationToken,
    api_key: Option<String>,
    observer: Option<ProviderTimingObserver>,
) -> ProviderEventStream {
    let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    let (priority_terminal_tx, priority_terminal_rx) = mpsc::channel(1);
    let origin = spec.origin();
    let provider = spec.provider.clone();
    let stream_budget = anthropic_requested_output_tokens(&spec, &options)
        .ok()
        .and_then(ResponseBudget::for_output_tokens)
        .unwrap_or_default();
    let success_terminal_committed = Arc::new(SuccessTerminalCommit::new());
    let producer_terminal_committed = success_terminal_committed.clone();
    let stream_cancel = cancel.clone();
    let span = tracing::info_span!(
        "provider_request",
        provider = %spec.provider,
        model = %spec.id,
        protocol = "anthropic_messages"
    );
    let producer_task = tokio::spawn(
        async move {
            run_anthropic_stream(
                spec,
                context,
                options,
                stream_cancel,
                api_key,
                ProducerChannels {
                    normal: tx,
                    priority_terminal: priority_terminal_tx,
                    ordered_prefix_drain: None,
                    success_terminal_committed: producer_terminal_committed,
                    timing: observer,
                },
            )
            .await;
        }
        .instrument(span),
    );
    ProviderEventStream::with_priority_terminal(
        rx,
        priority_terminal_rx,
        cancel,
        provider,
        origin,
        stream_budget,
        success_terminal_committed,
    )
    .own_producer(producer_task)
}

async fn run_anthropic_stream(
    spec: ModelSpec,
    context: PromptContext,
    options: RequestOptions,
    cancel: CancellationToken,
    api_key: Option<String>,
    channels: ProducerChannels,
) {
    let ProducerChannels {
        normal: tx,
        priority_terminal: priority_terminal_tx,
        ordered_prefix_drain: _,
        success_terminal_committed,
        timing,
    } = channels;
    let mut assembler = MessageAssembler::new();
    let _ = assembler.apply(&ProviderEvent::Start);
    let output_tokens = match anthropic_requested_output_tokens(&spec, &options) {
        Ok(tokens) => tokens,
        Err(error) => {
            finish_anthropic_error(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                Usage::default(),
                error,
                cancel.is_cancelled(),
                Vec::new(),
            )
            .await;
            return;
        }
    };
    let Some(budget) = ResponseBudget::for_output_tokens(output_tokens) else {
        finish_failure(
            &priority_terminal_tx,
            &mut assembler,
            &spec,
            Usage::default(),
            "requested output budget cannot be represented on this platform".into(),
            "invalid_provider_request",
            cancel.is_cancelled(),
        )
        .await;
        return;
    };
    assembler = MessageAssembler::with_budget(budget);
    let _ = assembler.apply(&ProviderEvent::Start);
    let schemas = match FrozenToolSchemaRegistry::compile(&context.tools) {
        Ok(schemas) => schemas,
        Err(error) => {
            finish_failure(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                Usage::default(),
                error.to_string(),
                "invalid_tool_schema",
                cancel.is_cancelled(),
            )
            .await;
            return;
        }
    };
    let coverage = match anthropic_request_coverage(&spec, &context, options.native_compaction) {
        Ok(coverage) => coverage,
        Err(error) => {
            finish_anthropic_error(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                Usage::default(),
                error,
                cancel.is_cancelled(),
                Vec::new(),
            )
            .await;
            return;
        }
    };
    let mut receive =
        AnthropicReceiveState::with_budget(schemas, budget, coverage, spec.id.clone());
    let Some(api_key) = api_key.filter(|key| !key.is_empty()) else {
        finish_failure(
            &priority_terminal_tx,
            &mut assembler,
            &spec,
            Usage::default(),
            format!("missing API key environment variable {}", spec.api_key_env),
            "missing_api_key",
            cancel.is_cancelled(),
        )
        .await;
        return;
    };
    let body = match build_anthropic_request(&spec, &context, &options) {
        Ok(body) => body,
        Err(error) => {
            finish_anthropic_error(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                receive.usage().clone(),
                error,
                cancel.is_cancelled(),
                Vec::new(),
            )
            .await;
            return;
        }
    };
    let client = match http_client() {
        Ok(client) => client,
        Err(error) => {
            finish_failure(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                receive.usage().clone(),
                error,
                "http_client_initialization_failed",
                cancel.is_cancelled(),
            )
            .await;
            return;
        }
    };
    let mut request = client
        .post(spec.endpoint())
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01");
    if let Some(compat) = spec.anthropic_compat()
        && !compat.beta_headers.is_empty()
    {
        request = request.header("anthropic-beta", compat.beta_headers.join(","));
    }
    let body = match CanonicalRequestBody::serialize(&body) {
        Ok(body) => body,
        Err(error) => {
            finish_anthropic_error(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                receive.usage().clone(),
                AnthropicAdapterError::InvalidContext(error.to_string()),
                cancel.is_cancelled(),
                Vec::new(),
            )
            .await;
            return;
        }
    };
    let (response, request_sent_at) = match await_request(
        body.apply(request).send(),
        &cancel,
        RESPONSE_HEADER_TIMEOUT,
        |at| TtftObservation::observe_request_sent(&timing, at),
    )
    .await
    {
        RequestWait::Cancelled => {
            finish_failure_with_context(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                receive.usage().clone(),
                "provider request cancelled".into(),
                "cancelled",
                true,
                receive.verified_reasoning_context(),
            )
            .await;
            return;
        }
        RequestWait::TimedOut => {
            finish_failure_with_context(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                receive.usage().clone(),
                format!(
                    "provider response headers timed out after {} seconds",
                    RESPONSE_HEADER_TIMEOUT.as_secs()
                ),
                "response_header_timeout",
                cancel.is_cancelled(),
                receive.verified_reasoning_context(),
            )
            .await;
            return;
        }
        RequestWait::Response {
            response: Ok(response),
            request_sent_at,
        } => (response, request_sent_at),
        RequestWait::Response {
            response: Err(error),
            ..
        } => {
            if cancel.is_cancelled() {
                finish_failure_with_context(
                    &priority_terminal_tx,
                    &mut assembler,
                    &spec,
                    receive.usage().clone(),
                    error.to_string(),
                    "request_error",
                    true,
                    receive.verified_reasoning_context(),
                )
                .await;
            } else {
                finish_failure(
                    &priority_terminal_tx,
                    &mut assembler,
                    &spec,
                    receive.usage().clone(),
                    error.to_string(),
                    "request_error",
                    cancel.is_cancelled(),
                )
                .await;
            }
            return;
        }
    };
    let mut transport =
        match SseStream::from_response(response, cancel.clone(), budget.max_wire_bytes).await {
            Ok(transport) => transport,
            Err(error) => {
                let cancelled = matches!(error, SseError::Cancelled) || cancel.is_cancelled();
                if cancelled {
                    finish_failure_with_context(
                        &priority_terminal_tx,
                        &mut assembler,
                        &spec,
                        receive.usage().clone(),
                        error.to_string(),
                        &transport_error_code(&error),
                        true,
                        receive.verified_reasoning_context(),
                    )
                    .await;
                } else {
                    finish_failure(
                        &priority_terminal_tx,
                        &mut assembler,
                        &spec,
                        receive.usage().clone(),
                        error.to_string(),
                        &transport_error_code(&error),
                        false,
                    )
                    .await;
                }
                return;
            }
        };

    let mut ttft = TtftObservation::new(request_sent_at, timing);
    loop {
        match transport.next_event().await {
            Ok(Some(event)) => {
                let pushed = match receive.push_named(event.event.as_deref(), &event.data) {
                    Ok(pushed) => pushed,
                    Err(error) => {
                        close_anthropic_partial(&mut receive, &mut assembler);
                        finish_anthropic_error(
                            &priority_terminal_tx,
                            &mut assembler,
                            &spec,
                            receive.usage().clone(),
                            error,
                            cancel.is_cancelled(),
                            receive.verified_reasoning_context(),
                        )
                        .await;
                        return;
                    }
                };
                for normalized in pushed.events {
                    let is_public_delta = matches!(
                        &normalized,
                        ProviderEvent::TextDelta { .. } | ProviderEvent::ThinkingDelta { .. }
                    );
                    let emit_result = emit(&tx, &mut assembler, normalized, &cancel).await;
                    ttft.observe_emit(is_public_delta, &emit_result);
                    match emit_result {
                        EmitResult::Sent => {}
                        EmitResult::Closed => return,
                        EmitResult::Cancelled => {
                            close_anthropic_partial(&mut receive, &mut assembler);
                            finish_failure_with_context(
                                &priority_terminal_tx,
                                &mut assembler,
                                &spec,
                                receive.usage().clone(),
                                "provider request cancelled".into(),
                                "cancelled",
                                true,
                                receive.verified_reasoning_context(),
                            )
                            .await;
                            return;
                        }
                        EmitResult::ContractViolation(error) => {
                            finish_failure_with_context(
                                &priority_terminal_tx,
                                &mut assembler,
                                &spec,
                                receive.usage().clone(),
                                error,
                                "normalized_event_contract_violation",
                                cancel.is_cancelled(),
                                receive.verified_reasoning_context(),
                            )
                            .await;
                            return;
                        }
                    }
                }
                if let Some(terminal) = pushed.terminal {
                    finish_anthropic_terminal(
                        &tx,
                        &priority_terminal_tx,
                        &mut assembler,
                        &spec,
                        terminal,
                        &cancel,
                        &success_terminal_committed,
                    )
                    .await;
                    return;
                }
            }
            Ok(None) => {
                let error = receive
                    .finish_eof()
                    .expect_err("loop returns immediately after message_stop");
                close_anthropic_partial(&mut receive, &mut assembler);
                if cancel.is_cancelled() {
                    finish_failure_with_context(
                        &priority_terminal_tx,
                        &mut assembler,
                        &spec,
                        receive.usage().clone(),
                        error.to_string(),
                        "stream_ended_without_terminal_event",
                        true,
                        receive.verified_reasoning_context(),
                    )
                    .await;
                } else {
                    finish_anthropic_error(
                        &priority_terminal_tx,
                        &mut assembler,
                        &spec,
                        receive.usage().clone(),
                        error,
                        false,
                        receive.verified_reasoning_context(),
                    )
                    .await;
                }
                return;
            }
            Err(error) => {
                close_anthropic_partial(&mut receive, &mut assembler);
                let cancelled = matches!(error, SseError::Cancelled) || cancel.is_cancelled();
                if cancelled {
                    finish_failure_with_context(
                        &priority_terminal_tx,
                        &mut assembler,
                        &spec,
                        receive.usage().clone(),
                        error.to_string(),
                        &transport_error_code(&error),
                        true,
                        receive.verified_reasoning_context(),
                    )
                    .await;
                } else {
                    finish_failure_with_context(
                        &priority_terminal_tx,
                        &mut assembler,
                        &spec,
                        receive.usage().clone(),
                        error.to_string(),
                        &transport_error_code(&error),
                        false,
                        receive.verified_reasoning_context(),
                    )
                    .await;
                }
                return;
            }
        }
    }
}

async fn run_responses_stream(
    spec: ModelSpec,
    context: PromptContext,
    options: RequestOptions,
    cancel: CancellationToken,
    api_key: Option<String>,
    channels: ProducerChannels,
) {
    let ProducerChannels {
        normal: tx,
        priority_terminal: priority_terminal_tx,
        ordered_prefix_drain,
        success_terminal_committed,
        timing,
    } = channels;
    let mut assembler = MessageAssembler::new();
    let _ = assembler.apply(&ProviderEvent::Start);
    let output_tokens = match responses_requested_output_tokens(&spec, &options) {
        Ok(tokens) => tokens,
        Err(error) => {
            finish_responses_error(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                Usage::default(),
                error,
                cancel.is_cancelled(),
                Vec::new(),
            )
            .await;
            return;
        }
    };
    let Some(budget) = ResponseBudget::for_output_tokens(output_tokens) else {
        finish_failure(
            &priority_terminal_tx,
            &mut assembler,
            &spec,
            Usage::default(),
            "requested output budget cannot be represented on this platform".to_owned(),
            "invalid_provider_request",
            cancel.is_cancelled(),
        )
        .await;
        return;
    };
    assembler = MessageAssembler::with_budget(budget);
    let _ = assembler.apply(&ProviderEvent::Start);
    let schemas = match FrozenToolSchemaRegistry::compile(&context.tools) {
        Ok(schemas) => schemas,
        Err(error) => {
            finish_failure(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                Usage::default(),
                error.to_string(),
                "invalid_tool_schema",
                cancel.is_cancelled(),
            )
            .await;
            return;
        }
    };
    let mut receive = ResponsesReceiveState::with_budget(schemas, budget);
    let Some(api_key) = api_key.filter(|key| !key.is_empty()) else {
        finish_failure(
            &priority_terminal_tx,
            &mut assembler,
            &spec,
            Usage::default(),
            format!("missing API key environment variable {}", spec.api_key_env),
            "missing_api_key",
            cancel.is_cancelled(),
        )
        .await;
        return;
    };
    let body = match build_responses_request(&spec, &context, &options) {
        Ok(body) => body,
        Err(error) => {
            finish_responses_error(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                receive.usage().clone(),
                error,
                cancel.is_cancelled(),
                receive.provider_context(),
            )
            .await;
            return;
        }
    };
    let client = match http_client() {
        Ok(client) => client,
        Err(error) => {
            finish_failure(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                receive.usage().clone(),
                error,
                "http_client_initialization_failed",
                cancel.is_cancelled(),
            )
            .await;
            return;
        }
    };
    let body = match CanonicalRequestBody::serialize(&body) {
        Ok(body) => body,
        Err(error) => {
            finish_responses_error(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                receive.usage().clone(),
                ResponsesAdapterError::InvalidContext(error.to_string()),
                cancel.is_cancelled(),
                receive.provider_context(),
            )
            .await;
            return;
        }
    };
    let request = body
        .apply(client.post(spec.endpoint()).bearer_auth(api_key))
        .send();
    let (response, request_sent_at) =
        match await_request(request, &cancel, RESPONSE_HEADER_TIMEOUT, |at| {
            TtftObservation::observe_request_sent(&timing, at)
        })
        .await
        {
            RequestWait::Cancelled => {
                finish_failure_with_context(
                    &priority_terminal_tx,
                    &mut assembler,
                    &spec,
                    receive.usage().clone(),
                    "provider request cancelled".to_owned(),
                    "cancelled",
                    true,
                    receive.provider_context(),
                )
                .await;
                return;
            }
            RequestWait::TimedOut => {
                finish_failure_with_context(
                    &priority_terminal_tx,
                    &mut assembler,
                    &spec,
                    receive.usage().clone(),
                    format!(
                        "provider response headers timed out after {} seconds",
                        RESPONSE_HEADER_TIMEOUT.as_secs()
                    ),
                    "response_header_timeout",
                    cancel.is_cancelled(),
                    receive.provider_context(),
                )
                .await;
                return;
            }
            RequestWait::Response {
                response: Ok(response),
                request_sent_at,
            } => (response, request_sent_at),
            RequestWait::Response {
                response: Err(error),
                ..
            } => {
                finish_failure_with_context(
                    &priority_terminal_tx,
                    &mut assembler,
                    &spec,
                    receive.usage().clone(),
                    error.to_string(),
                    "request_error",
                    cancel.is_cancelled(),
                    receive.provider_context(),
                )
                .await;
                return;
            }
        };
    let mut transport =
        match SseStream::from_response(response, cancel.clone(), budget.max_wire_bytes).await {
            Ok(transport) => transport,
            Err(error) => {
                finish_failure_with_context(
                    &priority_terminal_tx,
                    &mut assembler,
                    &spec,
                    receive.usage().clone(),
                    error.to_string(),
                    &transport_error_code(&error),
                    matches!(error, SseError::Cancelled) || cancel.is_cancelled(),
                    receive.provider_context(),
                )
                .await;
                return;
            }
        };

    let mut ttft = TtftObservation::new(request_sent_at, timing);
    loop {
        match transport.next_event().await {
            Ok(Some(event)) => {
                if let Err(error) = validate_event_name(event.event.as_deref(), &event.data) {
                    mark_responses_partial_rejection_prefix(
                        &ordered_prefix_drain,
                        close_responses_partial(&tx, &mut receive, &mut assembler, &cancel).await,
                    );
                    finish_responses_error(
                        &priority_terminal_tx,
                        &mut assembler,
                        &spec,
                        receive.usage().clone(),
                        error,
                        cancel.is_cancelled(),
                        receive.provider_context(),
                    )
                    .await;
                    return;
                }
                let pushed = match receive.push_json(&event.data) {
                    Ok(pushed) => pushed,
                    Err(error) => {
                        mark_responses_partial_rejection_prefix(
                            &ordered_prefix_drain,
                            close_responses_partial(&tx, &mut receive, &mut assembler, &cancel)
                                .await,
                        );
                        finish_responses_error(
                            &priority_terminal_tx,
                            &mut assembler,
                            &spec,
                            receive.usage().clone(),
                            error,
                            cancel.is_cancelled(),
                            receive.provider_context(),
                        )
                        .await;
                        return;
                    }
                };
                for normalized in pushed.events {
                    let is_public_delta = matches!(
                        &normalized,
                        ProviderEvent::TextDelta { .. } | ProviderEvent::ThinkingDelta { .. }
                    );
                    let emit_result = emit(&tx, &mut assembler, normalized, &cancel).await;
                    ttft.observe_emit(is_public_delta, &emit_result);
                    match emit_result {
                        EmitResult::Sent => {}
                        EmitResult::Closed => return,
                        EmitResult::Cancelled => {
                            mark_responses_partial_rejection_prefix(
                                &ordered_prefix_drain,
                                close_responses_partial(&tx, &mut receive, &mut assembler, &cancel)
                                    .await,
                            );
                            finish_failure_with_context(
                                &priority_terminal_tx,
                                &mut assembler,
                                &spec,
                                receive.usage().clone(),
                                "provider request cancelled".to_owned(),
                                "cancelled",
                                true,
                                receive.provider_context(),
                            )
                            .await;
                            return;
                        }
                        EmitResult::ContractViolation(error) => {
                            finish_failure_with_context(
                                &priority_terminal_tx,
                                &mut assembler,
                                &spec,
                                receive.usage().clone(),
                                error,
                                "normalized_event_contract_violation",
                                cancel.is_cancelled(),
                                receive.provider_context(),
                            )
                            .await;
                            return;
                        }
                    }
                }
                if let Some(terminal) = pushed.terminal {
                    finish_responses_terminal(
                        &tx,
                        &priority_terminal_tx,
                        &mut assembler,
                        &spec,
                        terminal,
                        &cancel,
                        &success_terminal_committed,
                    )
                    .await;
                    return;
                }
            }
            Ok(None) => {
                let error = receive
                    .finish_eof()
                    .expect_err("loop returns immediately after a terminal response");
                mark_responses_partial_rejection_prefix(
                    &ordered_prefix_drain,
                    close_responses_partial(&tx, &mut receive, &mut assembler, &cancel).await,
                );
                finish_responses_error(
                    &priority_terminal_tx,
                    &mut assembler,
                    &spec,
                    receive.usage().clone(),
                    error,
                    cancel.is_cancelled(),
                    receive.provider_context(),
                )
                .await;
                return;
            }
            Err(error) => {
                mark_responses_partial_rejection_prefix(
                    &ordered_prefix_drain,
                    close_responses_partial(&tx, &mut receive, &mut assembler, &cancel).await,
                );
                finish_failure_with_context(
                    &priority_terminal_tx,
                    &mut assembler,
                    &spec,
                    receive.usage().clone(),
                    error.to_string(),
                    &transport_error_code(&error),
                    matches!(error, SseError::Cancelled) || cancel.is_cancelled(),
                    receive.provider_context(),
                )
                .await;
                return;
            }
        }
    }
}

async fn run_chat_stream(
    spec: ModelSpec,
    context: PromptContext,
    options: RequestOptions,
    cancel: CancellationToken,
    api_key: Option<String>,
    channels: ProducerChannels,
) {
    let ProducerChannels {
        normal: tx,
        priority_terminal: priority_terminal_tx,
        ordered_prefix_drain: _,
        success_terminal_committed,
        timing,
    } = channels;
    let output_tokens = match chat_requested_output_tokens(&spec, &options) {
        Ok(output_tokens) => output_tokens,
        Err(error) => {
            let mut assembler = MessageAssembler::new();
            let _ = assembler.apply(&ProviderEvent::Start);
            let (message, code) = adapter_error(&error);
            finish_failure(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                Usage::default(),
                message,
                &code,
                cancel.is_cancelled(),
            )
            .await;
            return;
        }
    };
    let Some(budget) = ResponseBudget::for_output_tokens(output_tokens) else {
        let mut assembler = MessageAssembler::new();
        let _ = assembler.apply(&ProviderEvent::Start);
        finish_failure(
            &priority_terminal_tx,
            &mut assembler,
            &spec,
            Usage::default(),
            "requested output budget cannot be represented on this platform".to_owned(),
            "invalid_provider_request",
            cancel.is_cancelled(),
        )
        .await;
        return;
    };
    let mut assembler = MessageAssembler::with_budget(budget);
    assembler
        .apply(&ProviderEvent::Start)
        .expect("fresh producer assembler accepts Start");

    let schemas = match FrozenToolSchemaRegistry::compile(&context.tools) {
        Ok(schemas) => schemas,
        Err(error) => {
            finish_failure(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                Usage::default(),
                error.to_string(),
                "invalid_tool_schema",
                cancel.is_cancelled(),
            )
            .await;
            return;
        }
    };
    let mut receive = ChatReceiveState::with_budget(schemas, budget);

    let Some(api_key) = api_key.filter(|key| !key.is_empty()) else {
        finish_failure(
            &priority_terminal_tx,
            &mut assembler,
            &spec,
            Usage::default(),
            format!("missing API key environment variable {}", spec.api_key_env),
            "missing_api_key",
            cancel.is_cancelled(),
        )
        .await;
        return;
    };
    let client = match http_client() {
        Ok(client) => client,
        Err(error) => {
            finish_failure(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                Usage::default(),
                error,
                "http_client_initialization_failed",
                cancel.is_cancelled(),
            )
            .await;
            return;
        }
    };
    let body = match build_request(&spec, &context, &options) {
        Ok(body) => body,
        Err(error) => {
            finish_failure(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                Usage::default(),
                error.to_string(),
                "invalid_provider_request",
                cancel.is_cancelled(),
            )
            .await;
            return;
        }
    };

    let body = match CanonicalRequestBody::serialize(&body) {
        Ok(body) => body,
        Err(error) => {
            finish_failure(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                receive.usage().clone(),
                error.to_string(),
                "request_serialization_failed",
                cancel.is_cancelled(),
            )
            .await;
            return;
        }
    };
    let request = body
        .apply(client.post(spec.endpoint()).bearer_auth(api_key))
        .send();
    let (response, request_sent_at) =
        match await_request(request, &cancel, RESPONSE_HEADER_TIMEOUT, |at| {
            TtftObservation::observe_request_sent(&timing, at)
        })
        .await
        {
            RequestWait::Cancelled => {
                finish_failure(
                    &priority_terminal_tx,
                    &mut assembler,
                    &spec,
                    receive.usage().clone(),
                    "provider request cancelled".to_owned(),
                    "cancelled",
                    true,
                )
                .await;
                return;
            }
            RequestWait::TimedOut => {
                finish_failure(
                    &priority_terminal_tx,
                    &mut assembler,
                    &spec,
                    receive.usage().clone(),
                    format!(
                        "provider response headers timed out after {} seconds",
                        RESPONSE_HEADER_TIMEOUT.as_secs()
                    ),
                    "response_header_timeout",
                    cancel.is_cancelled(),
                )
                .await;
                return;
            }
            RequestWait::Response {
                response,
                request_sent_at,
            } => (response, request_sent_at),
        };
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            finish_failure(
                &priority_terminal_tx,
                &mut assembler,
                &spec,
                receive.usage().clone(),
                error.to_string(),
                "request_error",
                cancel.is_cancelled(),
            )
            .await;
            return;
        }
    };

    let mut transport =
        match SseStream::from_response(response, cancel.clone(), budget.max_wire_bytes).await {
            Ok(transport) => transport,
            Err(error) => {
                let cancelled = matches!(error, SseError::Cancelled) || cancel.is_cancelled();
                let code = transport_error_code(&error);
                finish_failure(
                    &priority_terminal_tx,
                    &mut assembler,
                    &spec,
                    receive.usage().clone(),
                    error.to_string(),
                    &code,
                    cancelled,
                )
                .await;
                return;
            }
        };

    let mut ttft = TtftObservation::new(request_sent_at, timing);
    loop {
        match transport.next_event().await {
            Ok(Some(event)) if event.data == "[DONE]" => {
                finish_chat(
                    &tx,
                    &priority_terminal_tx,
                    &mut assembler,
                    &spec,
                    &mut receive,
                    &cancel,
                    &success_terminal_committed,
                )
                .await;
                return;
            }
            Ok(Some(event)) => {
                let events = match receive.push_json(&event.data) {
                    Ok(events) => events,
                    Err(error) => {
                        close_partial(&mut receive, &mut assembler);
                        let (message, code) = adapter_error(&error);
                        finish_failure(
                            &priority_terminal_tx,
                            &mut assembler,
                            &spec,
                            receive.usage().clone(),
                            message,
                            &code,
                            cancel.is_cancelled(),
                        )
                        .await;
                        return;
                    }
                };
                let mut events = events.into_iter();
                while let Some(event) = events.next() {
                    let is_public_delta = matches!(
                        &event,
                        ProviderEvent::TextDelta { .. } | ProviderEvent::ThinkingDelta { .. }
                    );
                    let emit_result = emit(&tx, &mut assembler, event, &cancel).await;
                    ttft.observe_emit(is_public_delta, &emit_result);
                    match emit_result {
                        EmitResult::Sent => {}
                        EmitResult::Closed => return,
                        EmitResult::Cancelled => {
                            // The adapter normalized the whole chunk
                            // transactionally. Apply the unsent suffix locally
                            // so the priority terminal snapshot remains
                            // authoritative even though the normal backlog is
                            // intentionally abandoned.
                            for pending in events {
                                if let Err(error) = assembler.apply(&pending) {
                                    tracing::error!(
                                        %error,
                                        "failed to assemble cancelled chunk suffix"
                                    );
                                    break;
                                }
                            }
                            close_partial(&mut receive, &mut assembler);
                            finish_failure(
                                &priority_terminal_tx,
                                &mut assembler,
                                &spec,
                                receive.usage().clone(),
                                "provider request cancelled".to_owned(),
                                "cancelled",
                                true,
                            )
                            .await;
                            return;
                        }
                        EmitResult::ContractViolation(error) => {
                            close_partial(&mut receive, &mut assembler);
                            finish_failure(
                                &priority_terminal_tx,
                                &mut assembler,
                                &spec,
                                receive.usage().clone(),
                                error,
                                "normalized_event_contract_violation",
                                cancel.is_cancelled(),
                            )
                            .await;
                            return;
                        }
                    }
                }
            }
            Ok(None) => {
                let usage = receive.usage().clone();
                // A body that ends by HTTP framing without [DONE]. Endpoints
                // flagged `infer_finish_reason_at_done` (opencode.ai zen/go with
                // gpt-5.6-luna, observed 2026-08-17) also omit the sentinel, so
                // the same inference applies; a transport error still arrives as
                // Err below and is never inferred as complete.
                let infer = spec
                    .chat_compat()
                    .is_some_and(|compat| compat.infer_finish_reason_at_done);
                let finished = if infer {
                    receive.finish_after_done_sentinel(Utc::now())
                } else {
                    receive.finish(Utc::now())
                };
                match finished {
                    Ok(terminal) => {
                        finish_terminal(
                            &tx,
                            &priority_terminal_tx,
                            &mut assembler,
                            &spec,
                            terminal,
                            &cancel,
                            &success_terminal_committed,
                        )
                        .await;
                    }
                    Err(ChatAdapterError::MissingFinishReason) => {
                        close_partial(&mut receive, &mut assembler);
                        finish_failure(
                            &priority_terminal_tx,
                            &mut assembler,
                            &spec,
                            usage,
                            "provider stream ended without a terminal event".to_owned(),
                            "stream_ended_without_terminal_event",
                            cancel.is_cancelled(),
                        )
                        .await;
                    }
                    Err(error) => {
                        close_partial(&mut receive, &mut assembler);
                        let (message, code) = adapter_error(&error);
                        finish_failure(
                            &priority_terminal_tx,
                            &mut assembler,
                            &spec,
                            usage,
                            message,
                            &code,
                            cancel.is_cancelled(),
                        )
                        .await;
                    }
                }
                return;
            }
            Err(error) => {
                let cancelled = matches!(error, SseError::Cancelled) || cancel.is_cancelled();
                close_partial(&mut receive, &mut assembler);
                let code = transport_error_code(&error);
                finish_failure(
                    &priority_terminal_tx,
                    &mut assembler,
                    &spec,
                    receive.usage().clone(),
                    error.to_string(),
                    &code,
                    cancelled,
                )
                .await;
                return;
            }
        }
    }
}

async fn finish_chat(
    tx: &mpsc::Sender<ProviderEvent>,
    priority_terminal_tx: &mpsc::Sender<ProviderEvent>,
    assembler: &mut MessageAssembler,
    spec: &ModelSpec,
    receive: &mut ChatReceiveState,
    cancel: &CancellationToken,
    success_terminal_committed: &SuccessTerminalCommit,
) {
    let usage = receive.usage().clone();
    let infer_at_done = spec
        .chat_compat()
        .is_some_and(|compat| compat.infer_finish_reason_at_done);
    let finished = if infer_at_done {
        receive.finish_after_done_sentinel(Utc::now())
    } else {
        receive.finish(Utc::now())
    };
    match finished {
        Ok(terminal) => {
            finish_terminal(
                tx,
                priority_terminal_tx,
                assembler,
                spec,
                terminal,
                cancel,
                success_terminal_committed,
            )
            .await
        }
        Err(error) => {
            close_partial(receive, assembler);
            let (message, code) = adapter_error(&error);
            finish_failure(
                priority_terminal_tx,
                assembler,
                spec,
                usage,
                message,
                &code,
                cancel.is_cancelled(),
            )
            .await;
        }
    }
}

fn close_partial(receive: &mut ChatReceiveState, assembler: &mut MessageAssembler) {
    for event in receive.fail() {
        if let Err(error) = assembler.apply(&event) {
            tracing::error!(%error, "failed to close partial provider response");
            break;
        }
    }
}

async fn close_responses_partial(
    tx: &mpsc::Sender<ProviderEvent>,
    receive: &mut ResponsesReceiveState,
    assembler: &mut MessageAssembler,
    cancel: &CancellationToken,
) -> bool {
    let mut rejection_sent = false;
    for event in receive.fail() {
        // Reserve the ordered-lane slot before mutating the authoritative
        // producer snapshot. A cancellation must still be able to abandon an
        // unread normal backlog; in that case an unsent synthetic rejection
        // cannot be allowed into the abort terminal.
        let permit = tokio::select! {
            biased;
            _ = cancel.cancelled() => return false,
            permit = tx.reserve() => match permit {
                Ok(permit) => permit,
                Err(_) => return false,
            }
        };
        if let Err(error) = assembler.apply(&event) {
            drop(permit);
            tracing::error!(%error, "failed to close partial Responses output");
            break;
        }
        let is_rejection = matches!(event, ProviderEvent::ToolCallRejected { .. });
        permit.send(event);
        rejection_sent |= is_rejection;
    }
    rejection_sent
}

fn mark_responses_partial_rejection_prefix(
    ordered_prefix_drain: &Option<mpsc::Sender<()>>,
    rejection_sent: bool,
) {
    if !rejection_sent {
        return;
    }
    let Some(ordered_prefix_drain) = ordered_prefix_drain else {
        tracing::error!("Responses partial rejection has no ordered-prefix drain channel");
        return;
    };
    if ordered_prefix_drain.try_send(()).is_err() {
        tracing::error!("failed to mark Responses partial rejection ordered-prefix drain");
    }
}

fn close_anthropic_partial(receive: &mut AnthropicReceiveState, assembler: &mut MessageAssembler) {
    for event in receive.fail() {
        if let Err(error) = assembler.apply(&event) {
            tracing::error!(%error, "failed to close partial Anthropic output");
            break;
        }
    }
}

async fn finish_anthropic_error(
    priority_terminal_tx: &mpsc::Sender<ProviderEvent>,
    assembler: &mut MessageAssembler,
    spec: &ModelSpec,
    usage: Usage,
    error: AnthropicAdapterError,
    cancelled: bool,
    provider_context: Vec<types::ProviderContextFragment>,
) {
    let (message, code) = anthropic_adapter_error(&error);
    finish_failure_with_context(
        priority_terminal_tx,
        assembler,
        spec,
        usage,
        message,
        &code,
        cancelled,
        provider_context,
    )
    .await;
}

async fn finish_anthropic_terminal(
    tx: &mpsc::Sender<ProviderEvent>,
    priority_terminal_tx: &mpsc::Sender<ProviderEvent>,
    assembler: &mut MessageAssembler,
    spec: &ModelSpec,
    terminal: AnthropicTerminal,
    cancel: &CancellationToken,
    success_terminal_committed: &SuccessTerminalCommit,
) {
    let cancel_context = terminal
        .provider_context
        .iter()
        .filter(|fragment| {
            matches!(
                &fragment.payload,
                types::ProviderContextPayload::EncryptedReasoning {
                    protocol: ApiProtocol::AnthropicMessages,
                    ..
                }
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    for event in terminal.events {
        match emit(tx, assembler, event, cancel).await {
            EmitResult::Sent => {}
            EmitResult::Closed => return,
            EmitResult::Cancelled => {
                finish_failure_with_context(
                    priority_terminal_tx,
                    assembler,
                    spec,
                    terminal.usage,
                    "provider request cancelled".into(),
                    "cancelled",
                    true,
                    cancel_context.clone(),
                )
                .await;
                return;
            }
            EmitResult::ContractViolation(error) => {
                finish_failure_with_context(
                    priority_terminal_tx,
                    assembler,
                    spec,
                    terminal.usage,
                    error,
                    "normalized_event_contract_violation",
                    cancel.is_cancelled(),
                    terminal.provider_context.clone(),
                )
                .await;
                return;
            }
        }
    }
    if terminal.reason == StopReason::Error {
        finish_failure_with_context(
            priority_terminal_tx,
            assembler,
            spec,
            terminal.usage,
            terminal
                .error_message
                .unwrap_or_else(|| "provider returned an error terminal".into()),
            terminal
                .provider_code
                .as_deref()
                .unwrap_or("provider_error"),
            cancel.is_cancelled(),
            terminal.provider_context,
        )
        .await;
        return;
    }
    let permit = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            finish_failure_with_context(
                priority_terminal_tx,
                assembler,
                spec,
                terminal.usage,
                "provider request cancelled".into(),
                "cancelled",
                true,
                cancel_context,
            )
            .await;
            return;
        }
        permit = tx.reserve() => match permit {
            Ok(permit) => permit,
            Err(_) => return,
        }
    };
    let metadata = TerminalMetadata {
        provider: spec.provider.clone(),
        origin: spec.origin(),
        usage: terminal.usage.clone(),
        stop_reason: terminal.reason,
        error_message: terminal.error_message,
        provider_code: terminal.provider_code,
        interrupted: false,
        timestamp: Utc::now(),
    };
    let message = match assembler.prepare_finish(metadata) {
        Ok(message) => message,
        Err(error) => {
            drop(permit);
            finish_failure_with_context(
                priority_terminal_tx,
                assembler,
                spec,
                terminal.usage,
                error.to_string(),
                "normalized_event_contract_violation",
                cancel.is_cancelled(),
                terminal.provider_context,
            )
            .await;
            return;
        }
    };
    success_terminal_committed.commit();
    assembler.commit_prepared_terminal();
    permit.send(ProviderEvent::Done {
        reason: terminal.reason,
        output: ProviderOutput {
            message,
            provider_context: terminal.provider_context,
        },
    });
}

async fn finish_responses_error(
    priority_terminal_tx: &mpsc::Sender<ProviderEvent>,
    assembler: &mut MessageAssembler,
    spec: &ModelSpec,
    usage: Usage,
    error: ResponsesAdapterError,
    cancelled: bool,
    provider_context: Vec<types::ProviderContextFragment>,
) {
    let (message, code) = responses_adapter_error(&error);
    finish_failure_with_context(
        priority_terminal_tx,
        assembler,
        spec,
        usage,
        message,
        &code,
        cancelled,
        provider_context,
    )
    .await;
}

async fn finish_responses_terminal(
    tx: &mpsc::Sender<ProviderEvent>,
    priority_terminal_tx: &mpsc::Sender<ProviderEvent>,
    assembler: &mut MessageAssembler,
    spec: &ModelSpec,
    terminal: ResponsesTerminal,
    cancel: &CancellationToken,
    success_terminal_committed: &SuccessTerminalCommit,
) {
    if let Some(observed_model) = terminal.response_model.as_deref()
        && observed_model != spec.id
    {
        tracing::debug!(
            observed_model,
            canonical_model = spec.id,
            "provider reported a different model string; using canonical spec model"
        );
    }
    for event in terminal.events {
        match emit(tx, assembler, event, cancel).await {
            EmitResult::Sent => {}
            EmitResult::Closed => return,
            EmitResult::Cancelled => {
                finish_failure_with_context(
                    priority_terminal_tx,
                    assembler,
                    spec,
                    terminal.usage,
                    "provider request cancelled".to_owned(),
                    "cancelled",
                    true,
                    terminal.provider_context.clone(),
                )
                .await;
                return;
            }
            EmitResult::ContractViolation(error) => {
                finish_failure_with_context(
                    priority_terminal_tx,
                    assembler,
                    spec,
                    terminal.usage,
                    error,
                    "normalized_event_contract_violation",
                    cancel.is_cancelled(),
                    terminal.provider_context.clone(),
                )
                .await;
                return;
            }
        }
    }
    if terminal.reason == StopReason::Error {
        finish_failure_with_context(
            priority_terminal_tx,
            assembler,
            spec,
            terminal.usage,
            terminal
                .error_message
                .unwrap_or_else(|| "provider returned an error terminal".to_owned()),
            terminal
                .provider_code
                .as_deref()
                .unwrap_or("provider_error"),
            cancel.is_cancelled(),
            terminal.provider_context,
        )
        .await;
        return;
    }
    let permit = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            finish_failure_with_context(
                priority_terminal_tx,
                assembler,
                spec,
                terminal.usage,
                "provider request cancelled".to_owned(),
                "cancelled",
                true,
                terminal.provider_context.clone(),
            )
            .await;
            return;
        }
        permit = tx.reserve() => match permit {
            Ok(permit) => permit,
            Err(_) => return,
        }
    };
    let metadata = TerminalMetadata {
        provider: spec.provider.clone(),
        origin: spec.origin(),
        usage: terminal.usage.clone(),
        stop_reason: terminal.reason,
        error_message: terminal.error_message,
        provider_code: terminal.provider_code,
        interrupted: false,
        timestamp: Utc::now(),
    };
    let message = match assembler.prepare_finish(metadata) {
        Ok(message) => message,
        Err(error) => {
            drop(permit);
            finish_failure_with_context(
                priority_terminal_tx,
                assembler,
                spec,
                terminal.usage,
                error.to_string(),
                "normalized_event_contract_violation",
                cancel.is_cancelled(),
                terminal.provider_context,
            )
            .await;
            return;
        }
    };
    success_terminal_committed.commit();
    assembler.commit_prepared_terminal();
    permit.send(ProviderEvent::Done {
        reason: terminal.reason,
        output: ProviderOutput {
            message,
            provider_context: terminal.provider_context,
        },
    });
}

async fn finish_terminal(
    tx: &mpsc::Sender<ProviderEvent>,
    priority_terminal_tx: &mpsc::Sender<ProviderEvent>,
    assembler: &mut MessageAssembler,
    spec: &ModelSpec,
    terminal: ChatTerminal,
    cancel: &CancellationToken,
    success_terminal_committed: &SuccessTerminalCommit,
) {
    if matches!(
        terminal.stop_reason,
        StopReason::Error | StopReason::Aborted
    ) {
        for event in &terminal.events {
            if let Err(error) = assembler.apply(event) {
                finish_failure(
                    priority_terminal_tx,
                    assembler,
                    spec,
                    terminal.usage.clone(),
                    error.to_string(),
                    "normalized_event_contract_violation",
                    cancel.is_cancelled(),
                )
                .await;
                return;
            }
        }
        let error_message = terminal
            .error_message
            .unwrap_or_else(|| "provider returned an error terminal".to_owned());
        let provider_code = terminal
            .provider_code
            .unwrap_or_else(|| "provider_error".to_owned());
        finish_failure(
            priority_terminal_tx,
            assembler,
            spec,
            terminal.usage,
            error_message,
            &provider_code,
            cancel.is_cancelled() || terminal.stop_reason == StopReason::Aborted,
        )
        .await;
        return;
    }
    let mut terminal_events = terminal.events.into_iter();
    while let Some(event) = terminal_events.next() {
        let permit = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                apply_abort_snapshot_closers(
                    assembler,
                    std::iter::once(&event).chain(terminal_events.as_slice()),
                );
                finish_failure(
                    priority_terminal_tx,
                    assembler,
                    spec,
                    terminal.usage.clone(),
                    "provider request cancelled".to_owned(),
                    "cancelled",
                    true,
                )
                .await;
                return;
            }
            permit = tx.reserve() => match permit {
                Ok(permit) => permit,
                Err(_) => return,
            }
        };
        if let Err(error) = assembler.apply(&event) {
            drop(permit);
            finish_failure(
                priority_terminal_tx,
                assembler,
                spec,
                terminal.usage.clone(),
                error.to_string(),
                "normalized_event_contract_violation",
                cancel.is_cancelled(),
            )
            .await;
            return;
        }
        permit.send(event);
    }

    let permit = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            finish_failure(
                priority_terminal_tx,
                assembler,
                spec,
                terminal.usage,
                "provider request cancelled".to_owned(),
                "cancelled",
                true,
            )
            .await;
            return;
        }
        permit = tx.reserve() => match permit {
            Ok(permit) => permit,
            Err(_) => return,
        }
    };
    let metadata = TerminalMetadata {
        provider: spec.provider.clone(),
        origin: spec.origin(),
        usage: terminal.usage.clone(),
        stop_reason: terminal.stop_reason,
        error_message: terminal.error_message,
        provider_code: terminal.provider_code,
        interrupted: false,
        timestamp: Utc::now(),
    };
    let message = match assembler.prepare_finish(metadata) {
        Ok(message) => message,
        Err(error) => {
            drop(permit);
            finish_failure(
                priority_terminal_tx,
                assembler,
                spec,
                terminal.usage,
                error.to_string(),
                "normalized_event_contract_violation",
                cancel.is_cancelled(),
            )
            .await;
            return;
        }
    };

    // From this release-store onward the reserved slot is guaranteed to
    // receive this Done without another await. The stream therefore treats a
    // later cancellation as subordinate to the already committed success.
    success_terminal_committed.commit();
    assembler.commit_prepared_terminal();
    permit.send(ProviderEvent::Done {
        reason: terminal.stop_reason,
        output: ProviderOutput {
            message,
            provider_context: Vec::new(),
        },
    });
}

fn apply_abort_snapshot_closers<'a>(
    assembler: &mut MessageAssembler,
    events: impl IntoIterator<Item = &'a ProviderEvent>,
) {
    for event in events {
        if !matches!(
            event,
            ProviderEvent::TextEnd { .. } | ProviderEvent::ThinkingEnd { .. }
        ) {
            continue;
        }
        if let Err(error) = assembler.apply(event) {
            tracing::error!(%error, "failed to apply provider-approved abort snapshot closer");
        }
    }
}

async fn finish_failure(
    priority_terminal_tx: &mpsc::Sender<ProviderEvent>,
    assembler: &mut MessageAssembler,
    spec: &ModelSpec,
    usage: Usage,
    error_message: String,
    provider_code: &str,
    cancelled: bool,
) {
    finish_failure_with_context(
        priority_terminal_tx,
        assembler,
        spec,
        usage,
        error_message,
        provider_code,
        cancelled,
        Vec::new(),
    )
    .await;
}

#[allow(
    clippy::too_many_arguments,
    reason = "terminal normalization keeps the existing failure contract plus opaque context"
)]
async fn finish_failure_with_context(
    priority_terminal_tx: &mpsc::Sender<ProviderEvent>,
    assembler: &mut MessageAssembler,
    spec: &ModelSpec,
    usage: Usage,
    error_message: String,
    provider_code: &str,
    cancelled: bool,
    provider_context: Vec<types::ProviderContextFragment>,
) {
    let Ok(permit) = priority_terminal_tx.try_reserve() else {
        return;
    };
    let requested_reason = if cancelled {
        StopReason::Aborted
    } else {
        StopReason::Error
    };
    let metadata = TerminalMetadata {
        provider: spec.provider.clone(),
        origin: spec.origin(),
        usage: usage.clone(),
        stop_reason: requested_reason,
        error_message: Some(error_message),
        provider_code: Some(provider_code.to_owned()),
        interrupted: cancelled,
        timestamp: Utc::now(),
    };
    let message = match assembler.prepare_finish(metadata) {
        Ok(message) => {
            if requested_reason == StopReason::Aborted {
                if let Err(error) =
                    assembler.commit_prepared_error_terminal(requested_reason, &message.content)
                {
                    tracing::error!(%error, "failed to commit normalized provider failure");
                    assembler.commit_prepared_terminal();
                }
            } else {
                assembler.commit_prepared_terminal();
            }
            message
        }
        Err(error) => {
            tracing::error!(%error, "failed to finalize normalized provider events");
            AssistantMessage {
                content: if requested_reason == StopReason::Aborted {
                    assembler.authoritative_abort_content().unwrap_or_default()
                } else {
                    assembler
                        .authoritative_error_content()
                        .unwrap_or_else(|_| assembler.completed_content())
                },
                model: spec.id.clone(),
                provider: spec.provider.clone(),
                origin: spec.origin(),
                usage,
                stop_reason: StopReason::Error,
                error_message: Some("provider event assembly failed".to_owned()),
                provider_code: Some("normalized_event_contract_violation".to_owned()),
                interrupted: false,
                timestamp: Utc::now(),
            }
        }
    };
    let reason = message.stop_reason;
    permit.send(ProviderEvent::Error {
        reason,
        output: ProviderOutput {
            message,
            provider_context,
        },
    });
}

enum EmitResult {
    Sent,
    Cancelled,
    Closed,
    ContractViolation(String),
}

enum SendResult {
    Sent,
    Cancelled,
    Closed,
}

async fn emit(
    tx: &mpsc::Sender<ProviderEvent>,
    assembler: &mut MessageAssembler,
    event: ProviderEvent,
    cancel: &CancellationToken,
) -> EmitResult {
    if let Err(error) = assembler.apply(&event) {
        tracing::error!(%error, "normalized provider event violated assembler contract");
        return EmitResult::ContractViolation(error.to_string());
    }
    match send_ordered(tx, event, cancel).await {
        SendResult::Sent => EmitResult::Sent,
        SendResult::Cancelled => EmitResult::Cancelled,
        SendResult::Closed => EmitResult::Closed,
    }
}

async fn send_ordered(
    tx: &mpsc::Sender<ProviderEvent>,
    event: ProviderEvent,
    cancel: &CancellationToken,
) -> SendResult {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => SendResult::Cancelled,
        result = tx.send(event) => {
            if result.is_ok() {
                SendResult::Sent
            } else {
                SendResult::Closed
            }
        }
    }
}

fn transport_error_code(error: &SseError) -> String {
    match error {
        SseError::Http { status, .. } => format!("http_{status}"),
        SseError::Transport(_) => "transport_error".to_owned(),
        SseError::IdleTimeout { .. } => "idle_timeout".to_owned(),
        SseError::Cancelled => "cancelled".to_owned(),
        SseError::InvalidUtf8 => "invalid_sse_utf8".to_owned(),
        SseError::LineTooLong { .. } => "sse_line_too_long".to_owned(),
        SseError::EventTooLong { .. } => "sse_event_too_long".to_owned(),
        SseError::EventQueueTooLarge { .. } => "sse_event_queue_too_large".to_owned(),
        SseError::ResponseTooLong { .. } => "response_limit_exceeded".to_owned(),
        SseError::UnexpectedEof => "unexpected_sse_eof".to_owned(),
    }
}

fn adapter_error(error: &ChatAdapterError) -> (String, String) {
    match error {
        ChatAdapterError::Provider { code, message } => (
            message.clone(),
            code.clone().unwrap_or_else(|| "provider_error".to_owned()),
        ),
        ChatAdapterError::InvalidChunk(_) => (error.to_string(), "invalid_sse_json".to_owned()),
        ChatAdapterError::MultipleChoices(_)
        | ChatAdapterError::MissingToolDeltaIdentity
        | ChatAdapterError::ConflictingToolIdentity
        | ChatAdapterError::ConflictingToolFormats
        | ChatAdapterError::LegacyFunctionCallUnsupported
        | ChatAdapterError::MissingToolCall
        | ChatAdapterError::IncompleteToolIdentity
        | ChatAdapterError::AmbiguousToolName { .. }
        | ChatAdapterError::UnexpectedToolCall
        | ChatAdapterError::EventsAfterFinishReason => {
            (error.to_string(), "invalid_provider_stream".to_owned())
        }
        ChatAdapterError::ResponseLimitExceeded { .. } => {
            (error.to_string(), "response_limit_exceeded".to_owned())
        }
        ChatAdapterError::MissingFinishReason => (
            error.to_string(),
            "stream_ended_without_finish_reason".to_owned(),
        ),
        ChatAdapterError::UnsupportedProtocol
        | ChatAdapterError::InvalidContext(_)
        | ChatAdapterError::InvalidMaxTokens { .. }
        | ChatAdapterError::InvalidTemperature(_)
        | ChatAdapterError::ReasoningRequired
        | ChatAdapterError::InvalidReasoningEffort(_)
        | ChatAdapterError::RequiredToolChoiceUnsupported
        | ChatAdapterError::StructuredOutputUnsupported => {
            (error.to_string(), "invalid_provider_request".to_owned())
        }
    }
}

fn responses_adapter_error(error: &ResponsesAdapterError) -> (String, String) {
    match error {
        ResponsesAdapterError::Provider { code, message } => (
            message.clone(),
            code.clone().unwrap_or_else(|| "provider_error".to_owned()),
        ),
        ResponsesAdapterError::InvalidEvent(_) => {
            (error.to_string(), "invalid_provider_stream".to_owned())
        }
        ResponsesAdapterError::ResponseLimitExceeded { .. } => {
            (error.to_string(), "response_limit_exceeded".to_owned())
        }
        ResponsesAdapterError::MissingTerminal => (
            error.to_string(),
            "stream_ended_without_terminal_event".to_owned(),
        ),
        ResponsesAdapterError::UnsupportedProtocol
        | ResponsesAdapterError::InvalidMaxTokens { .. }
        | ResponsesAdapterError::InvalidTemperature(_)
        | ResponsesAdapterError::InvalidContext(_) => {
            (error.to_string(), "invalid_provider_request".to_owned())
        }
        ResponsesAdapterError::InvalidCompactResponse(_) => {
            (error.to_string(), "invalid_compact_response".to_owned())
        }
    }
}

fn anthropic_adapter_error(error: &AnthropicAdapterError) -> (String, String) {
    match error {
        AnthropicAdapterError::Provider { code, message } => (
            message.clone(),
            code.clone().unwrap_or_else(|| "provider_error".into()),
        ),
        AnthropicAdapterError::InvalidEvent(_) => {
            (error.to_string(), "invalid_provider_stream".into())
        }
        AnthropicAdapterError::ResponseLimitExceeded { .. } => {
            (error.to_string(), "response_limit_exceeded".into())
        }
        AnthropicAdapterError::MissingTerminal => (
            error.to_string(),
            "stream_ended_without_terminal_event".into(),
        ),
        AnthropicAdapterError::NativeCompactionFailed { code } => (error.to_string(), code.clone()),
        AnthropicAdapterError::UnsupportedProtocol
        | AnthropicAdapterError::InvalidMaxTokens { .. }
        | AnthropicAdapterError::InvalidTemperature(_)
        | AnthropicAdapterError::InvalidContext(_) => {
            (error.to_string(), "invalid_provider_request".into())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::Cell,
        convert::Infallible,
        env, fs, io,
        os::unix::fs::PermissionsExt,
        path::{Path, PathBuf},
        process::{Command, Output},
    };

    use axum::{
        Router,
        body::Body,
        http::{Response, StatusCode},
        routing::post,
    };
    use futures_util::{StreamExt, stream};

    use super::*;
    use crate::provider::types::{
        AssistantContent, ContextMessage, Message, RejectedToolCall, ToolArgumentError, ToolCall,
        ToolDefinition, ToolResultMessage, UserContent, UserMessage, ValidatedToolArguments,
    };

    #[test]
    fn chat_context_validation_is_classified_as_invalid_provider_request() {
        let (message, code) = adapter_error(&ChatAdapterError::InvalidContext("fixture".into()));
        assert!(message.contains("fixture"));
        assert_eq!(code, "invalid_provider_request");
    }

    struct CaptureCommandFixture {
        root: PathBuf,
        state: PathBuf,
        bin: PathBuf,
    }

    impl CaptureCommandFixture {
        fn new() -> Self {
            let base = env::temp_dir().join(format!("sumi-capture-test-{}", uuid::Uuid::now_v7()));
            let root = base.join("repo");
            let state = base.join("state");
            let bin = base.join("bin");
            fs::create_dir_all(&root).expect("create fake repository");
            fs::create_dir_all(&state).expect("create fake command state");
            fs::create_dir_all(&bin).expect("create fake command directory");

            write_executable(
                &bin.join("git"),
                r#"#!/bin/sh
set -eu
{
  printf '%s\n' git
  for argument in "$@"; do
    printf '%s\n' "$argument"
  done
} >>"$FAKE_STATE/git.log"
case ${1-} in
  rev-parse)
    [ "$#" -eq 2 ]
    [ "$2" = "--show-toplevel" ]
    printf '%s\n' "$FAKE_REPO_ROOT"
    ;;
  -C)
    [ "$#" -eq 7 ]
    [ "$2" = "$FAKE_REPO_ROOT" ]
    [ "$3" = "check-ignore" ]
    [ "$4" = "--quiet" ]
    [ "$5" = "--no-index" ]
    [ "$6" = "--" ]
    ;;
  *)
    exit 64
    ;;
esac
"#,
            );
            write_executable(
                &bin.join("mktemp"),
                r#"#!/bin/sh
set -eu
[ "$#" -eq 1 ]
printf '%s\n' "$1" >>"$FAKE_STATE/mktemp.log"
case "$1" in
  /tmp/sumi-opencode-curl.XXXXXX)
    path="$FAKE_STATE/curl.config"
    ;;
  "$FAKE_REPO_ROOT"/target/provider-captures/opencode-go/opencode-kimi-k2-7-code.XXXXXX.tmp)
    path="$FAKE_REPO_ROOT/target/provider-captures/opencode-go/opencode-kimi-k2-7-code.fixture.tmp"
    ;;
  *)
    exit 64
    ;;
esac
: >"$path"
printf '%s\n' "$path"
"#,
            );
            write_executable(
                &bin.join("curl"),
                r#"#!/bin/sh
set -eu
{
  printf '%s\n' curl
  for argument in "$@"; do
    printf '%s\n' "$argument"
  done
} >"$FAKE_STATE/curl.log"
[ "${1-}" = "--disable" ]
config=
output=
previous=
for argument in "$@"; do
  case "$previous" in
    --config) config=$argument ;;
    --output) output=$argument ;;
  esac
  previous=$argument
done
[ "$config" = "$FAKE_STATE/curl.config" ]
[ -f "$config" ]
[ -n "$output" ]
printf '%s\n' 'data: {"fixture":true}' >"$output"
if [ "$FAKE_CURL_MODE" = "failure" ]; then
  printf '%s\n' 'fake curl failure' >&2
  exit 22
fi
"#,
            );

            Self { root, state, bin }
        }

        fn run(&self, api_key: &str, curl_mode: &str) -> Output {
            let path = format!("{}:/usr/bin:/bin", self.bin.display());
            Command::new("/bin/sh")
                .arg("-c")
                .arg(opencode_capture_script())
                .current_dir(&self.root)
                .env("PATH", path)
                .env("FAKE_REPO_ROOT", &self.root)
                .env("FAKE_STATE", &self.state)
                .env("FAKE_CURL_MODE", curl_mode)
                .env("OPENCODE_GO_API_KEY", api_key)
                .output()
                .expect("execute documented capture command")
        }

        fn diagnostics(output: &Output) -> String {
            format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
        }

        fn capture_files(&self) -> Vec<PathBuf> {
            files_below(&self.root.join("target/provider-captures"))
        }

        fn assert_exact_curl_command(&self) {
            let arguments = fs::read_to_string(self.state.join("curl.log"))
                .expect("fake curl invocation log")
                .lines()
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>();
            assert_eq!(
                arguments,
                vec![
                    "curl".to_owned(),
                    "--disable".to_owned(),
                    "--config".to_owned(),
                    self.state.join("curl.config").display().to_string(),
                    "--silent".to_owned(),
                    "--show-error".to_owned(),
                    "--no-buffer".to_owned(),
                    "--fail-with-body".to_owned(),
                    "--max-time".to_owned(),
                    "60".to_owned(),
                    "--output".to_owned(),
                    self.root
                        .join("target/provider-captures/opencode-go/opencode-kimi-k2-7-code.fixture.tmp")
                        .display()
                        .to_string(),
                    "https://opencode.ai/zen/go/v1/chat/completions".to_owned(),
                    "--data-binary".to_owned(),
                    concat!(
                        "{\"max_tokens\":64,\"messages\":[{\"content\":[{\"text\":",
                        "\"Reply with exactly fixture-ok\",\"type\":\"text\"}],\"role\":",
                        "\"user\"}],\"model\":\"kimi-k2.7-code\",\"stream\":true,",
                        "\"stream_options\":{\"include_usage\":true}}"
                    )
                    .to_owned(),
                ]
            );
        }
    }

    impl Drop for CaptureCommandFixture {
        fn drop(&mut self) {
            if let Some(base) = self.root.parent().map(Path::to_path_buf) {
                let _ = fs::remove_dir_all(base);
            }
        }
    }

    fn write_executable(path: &Path, contents: &str) {
        fs::write(path, contents).expect("write fake command");
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .expect("mark fake command executable");
    }

    fn opencode_capture_script() -> &'static str {
        let readme = include_str!("../../tests/fixtures/README.md");
        let (_, capture_section) = readme
            .split_once("The OpenCode capture script below is retained for future qualification.")
            .expect("README OpenCode capture section");
        let (_, fenced) = capture_section
            .split_once("```sh\n")
            .expect("README capture shell fence");
        fenced
            .split_once("\n```")
            .expect("README capture shell fence end")
            .0
    }

    fn files_below(path: &Path) -> Vec<PathBuf> {
        let Ok(entries) = fs::read_dir(path) else {
            return Vec::new();
        };
        let mut files = Vec::new();
        for entry in entries {
            let path = entry.expect("capture directory entry").path();
            if path.is_dir() {
                files.extend(files_below(&path));
            } else {
                files.push(path);
            }
        }
        files.sort();
        files
    }

    #[test]
    fn documented_opencode_capture_exact_command_preserves_success_and_curl_failure() {
        let api_key = "sk-current.OpenCode_key~9";
        let success = CaptureCommandFixture::new();
        let output = success.run(api_key, "success");
        assert!(
            output.status.success(),
            "documented capture failed: {}",
            CaptureCommandFixture::diagnostics(&output)
        );
        success.assert_exact_curl_command();
        let files = success.capture_files();
        assert_eq!(
            files.len(),
            1,
            "success must publish exactly one raw capture"
        );
        assert!(
            files[0]
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".raw.sse"))
        );
        assert!(!success.state.join("curl.config").exists());
        assert!(!CaptureCommandFixture::diagnostics(&output).contains(api_key));
        assert!(
            !fs::read_to_string(success.state.join("curl.log"))
                .expect("curl log")
                .contains(api_key)
        );

        let failure = CaptureCommandFixture::new();
        let output = failure.run(api_key, "failure");
        assert!(!output.status.success(), "curl failure must fail closed");
        failure.assert_exact_curl_command();
        assert!(
            failure.capture_files().is_empty(),
            "curl failure must not publish raw or final captures"
        );
        assert!(!failure.state.join("curl.config").exists());
        assert!(!CaptureCommandFixture::diagnostics(&output).contains(api_key));
        assert!(
            !fs::read_to_string(failure.state.join("curl.log"))
                .expect("curl log")
                .contains(api_key)
        );
    }

    #[test]
    fn documented_opencode_capture_rejects_unsafe_keys_before_any_external_command() {
        for api_key in [
            "",
            "sk-safe\nheader = X-Evil: injected",
            "sk-quote\"injected",
            "sk-backslash\\injected",
            "sk-carriage\rreturn",
            "sk with space",
            "sk-tab\tinjected",
            "sk-non-ascii-\u{00e9}",
        ] {
            let fixture = CaptureCommandFixture::new();
            let output = fixture.run(api_key, "success");
            assert!(
                !output.status.success(),
                "unsafe API key unexpectedly passed validation: {api_key:?}"
            );
            assert!(
                fixture.capture_files().is_empty(),
                "unsafe API key created a raw or final capture: {api_key:?}"
            );
            assert!(
                !fixture.state.join("curl.config").exists(),
                "unsafe API key created a curl config: {api_key:?}"
            );
            for log in ["git.log", "mktemp.log", "curl.log"] {
                assert!(
                    !fixture.state.join(log).exists(),
                    "unsafe API key reached external command recorded in {log}: {api_key:?}"
                );
            }
            if !api_key.is_empty() {
                assert!(
                    !CaptureCommandFixture::diagnostics(&output).contains(api_key),
                    "unsafe API key leaked to command output"
                );
            }
        }
    }

    async fn serve_fixture(
        status: StatusCode,
        body: &'static str,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/chat/completions",
            post(move || async move {
                Response::builder()
                    .status(status)
                    .header("content-type", "text/event-stream")
                    .body(Body::from(body))
                    .expect("response")
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });
        (format!("http://{address}"), task)
    }

    async fn serve_delayed_headers() -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/chat/completions",
            post(|| async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Response::new(Body::from(""))
            }),
        );
        serve_router(app).await
    }

    async fn serve_stalled_body(
        prefix: Option<&'static str>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/chat/completions",
            post(move || async move {
                let prefix = stream::iter(
                    prefix
                        .into_iter()
                        .map(|value| Ok::<String, Infallible>(value.to_owned())),
                );
                let stalled = stream::pending::<Result<String, Infallible>>();
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(Body::from_stream(prefix.chain(stalled)))
                    .expect("response")
            }),
        );
        serve_router(app).await
    }

    async fn serve_stalled_owned_body(prefix: String) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/chat/completions",
            post(move || {
                let prefix = prefix.clone();
                async move {
                    let prefix = stream::iter([Ok::<String, Infallible>(prefix)]);
                    let stalled = stream::pending::<Result<String, Infallible>>();
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "text/event-stream")
                        .body(Body::from_stream(prefix.chain(stalled)))
                        .expect("response")
                }
            }),
        );
        serve_router(app).await
    }

    async fn serve_reset_error_body(
        status: StatusCode,
        prefix: &'static str,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/chat/completions",
            post(move || async move {
                let prefix =
                    stream::once(async move { Ok::<String, io::Error>(prefix.to_owned()) });
                let reset = stream::once(async {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Err::<String, io::Error>(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "fixture reset error body",
                    ))
                });
                Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .body(Body::from_stream(prefix.chain(reset)))
                    .expect("response")
            }),
        );
        serve_router(app).await
    }

    async fn serve_stalled_error_body(status: StatusCode) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/chat/completions",
            post(move || async move {
                let stalled = stream::pending::<Result<String, Infallible>>();
                Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .body(Body::from_stream(stalled))
                    .expect("response")
            }),
        );
        serve_router(app).await
    }

    async fn serve_router(app: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });
        (format!("http://{address}"), task)
    }

    fn empty_context() -> PromptContext {
        PromptContext {
            system_prompt: "test".to_owned(),
            memory_blocks: vec![],
            messages: vec![],
            provider_context: vec![],
            tools: vec![],
            replay_provenance: None,
        }
    }

    fn persisted_context(seq: u64) -> PromptContext {
        PromptContext {
            messages: vec![ContextMessage::Persisted {
                id: format!("message-{seq}"),
                seq,
                message: Message::User(UserMessage {
                    content: vec![UserContent::Text {
                        text: "compact this".into(),
                    }],
                    timestamp: Utc::now(),
                }),
            }],
            ..empty_context()
        }
    }

    async fn replay(preset: &str, body: &'static str) -> Vec<ProviderEvent> {
        replay_with_context(preset, body, empty_context()).await
    }

    async fn replay_with_context(
        preset: &str,
        body: &'static str,
        context: PromptContext,
    ) -> Vec<ProviderEvent> {
        let (base_url, server) = serve_fixture(StatusCode::OK, body).await;
        let mut spec = ModelSpec::preset(preset).expect("preset");
        spec.base_url = base_url;
        let mut events = stream_with_api_key(
            spec,
            context,
            RequestOptions::default(),
            CancellationToken::new(),
            Some("test-key".to_owned()),
        );
        let mut received = Vec::new();
        while let Some(event) = events.recv().await {
            received.push(event);
        }
        server.abort();
        received
    }

    async fn replay_observed(
        preset: &str,
        route: &'static str,
        body: &'static str,
        context: PromptContext,
    ) -> (Vec<ProviderEvent>, Vec<ProviderTimingObservation>) {
        let app = Router::new().route(
            route,
            post(move || async move {
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(Body::from(body))
                    .expect("response")
            }),
        );
        let (base_url, server) = serve_router(app).await;
        let mut spec = ModelSpec::preset(preset).expect("preset");
        spec.base_url = base_url;
        let (observer, mut observations) = timing_observation_channel();
        let mut stream = stream_with_api_key_observed(
            spec,
            context,
            RequestOptions::default(),
            CancellationToken::new(),
            Some("test-key".to_owned()),
            Some(observer),
        );
        let mut events = Vec::new();
        while let Some(event) = stream.recv().await {
            events.push(event);
        }
        let mut timing = Vec::new();
        while let Some(observation) = observations.recv().await {
            timing.push(observation);
        }
        server.abort();
        (events, timing)
    }

    #[tokio::test]
    async fn timing_observations_are_ordered_once_for_all_protocols() {
        for (preset, route, fixture, expected_delta) in [
            (
                "kimi-k3",
                "/chat/completions",
                include_str!("../../tests/fixtures/kimi_text.sse"),
                "text",
            ),
            (
                "openai-responses",
                "/responses",
                include_str!("../../tests/fixtures/openai_responses_official.sse"),
                "text",
            ),
            (
                "anthropic",
                "/messages",
                include_str!("../../tests/fixtures/anthropic_messages_official.sse"),
                "thinking",
            ),
        ] {
            let context = if preset == "anthropic" {
                persisted_context(1)
            } else {
                empty_context()
            };
            let (events, observations) = replay_observed(preset, route, fixture, context).await;
            assert!(
                events.iter().any(|event| matches!(
                    (expected_delta, event),
                    ("text", ProviderEvent::TextDelta { .. })
                        | ("thinking", ProviderEvent::ThinkingDelta { .. })
                )),
                "{preset} fixture must exercise {expected_delta}"
            );
            assert_eq!(observations.len(), 2, "{preset}");
            let ProviderTimingObservation::RequestSent(request_sent) = observations[0] else {
                panic!("{preset}: first observation was not request_sent")
            };
            let ProviderTimingObservation::FirstPublicDelta(first_delta) = observations[1] else {
                panic!("{preset}: second observation was not first_public_delta")
            };
            assert!(first_delta >= request_sent, "{preset}");
        }
    }

    #[tokio::test]
    async fn tool_only_stream_has_no_first_public_delta_observation() {
        let context = PromptContext {
            tools: vec![ToolDefinition {
                name: "read_file".to_owned(),
                description: String::new(),
                parameters: serde_json::json!({
                    "type":"object",
                    "properties":{"path":{"type":"string"}},
                    "required":["path"]
                }),
            }],
            ..empty_context()
        };
        let (_, observations) = replay_observed(
            "kimi-k3",
            "/chat/completions",
            include_str!("../../tests/fixtures/kimi_toolcall.sse"),
            context,
        )
        .await;
        assert!(matches!(
            observations.as_slice(),
            [ProviderTimingObservation::RequestSent(_)]
        ));
    }

    #[tokio::test]
    async fn preflight_failure_and_precancel_emit_no_timing_observations() {
        let (observer, mut observations) = timing_observation_channel();
        let mut invalid = stream_with_api_key_observed(
            ModelSpec::preset("anthropic").expect("preset"),
            empty_context(),
            RequestOptions::default(),
            CancellationToken::new(),
            Some("test-key".to_owned()),
            Some(observer),
        );
        while invalid.recv().await.is_some() {}
        assert!(observations.recv().await.is_none());

        let cancel = CancellationToken::new();
        cancel.cancel();
        let (observer, mut observations) = timing_observation_channel();
        let mut cancelled = stream_with_api_key_observed(
            ModelSpec::preset("kimi-k3").expect("preset"),
            empty_context(),
            RequestOptions::default(),
            cancel,
            Some("test-key".to_owned()),
            Some(observer),
        );
        while cancelled.recv().await.is_some() {}
        assert!(observations.recv().await.is_none());
    }

    #[tokio::test]
    async fn observed_and_unobserved_streams_emit_the_same_events() {
        let fixture = include_str!("../../tests/fixtures/kimi_text.sse");
        let expected = replay("kimi-k3", fixture).await;
        let (actual, _) =
            replay_observed("kimi-k3", "/chat/completions", fixture, empty_context()).await;
        assert_eq!(
            normalized_event_snapshot(&actual),
            normalized_event_snapshot(&expected)
        );
    }

    #[tokio::test]
    async fn closed_or_full_timing_observer_does_not_affect_provider_stream() {
        for (observer, _keep_full_receiver_alive) in {
            let (closed_sender, closed_receiver) = mpsc::channel(1);
            drop(closed_receiver);

            let (full_sender, full_receiver) = mpsc::channel(1);
            full_sender
                .try_send(ProviderTimingObservation::RequestSent(Instant::now()))
                .expect("fill observer lane");
            [
                (
                    ProviderTimingObserver {
                        sender: closed_sender,
                    },
                    None,
                ),
                (
                    ProviderTimingObserver {
                        sender: full_sender,
                    },
                    Some(full_receiver),
                ),
            ]
        } {
            let fixture = include_str!("../../tests/fixtures/kimi_text.sse");
            let app = Router::new().route(
                "/chat/completions",
                post(move || async move {
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "text/event-stream")
                        .body(Body::from(fixture))
                        .expect("response")
                }),
            );
            let (base_url, server) = serve_router(app).await;
            let mut spec = ModelSpec::preset("kimi-k3").expect("preset");
            spec.base_url = base_url;
            let mut stream = stream_with_api_key_observed(
                spec,
                empty_context(),
                RequestOptions::default(),
                CancellationToken::new(),
                Some("test-key".to_owned()),
                Some(observer),
            );
            let terminal = tokio::time::timeout(Duration::from_secs(1), async {
                let mut terminal = None;
                while let Some(event) = stream.recv().await {
                    if matches!(
                        event,
                        ProviderEvent::Done { .. } | ProviderEvent::Error { .. }
                    ) {
                        terminal = Some(event);
                    }
                }
                terminal
            })
            .await
            .expect("timing observer must not block provider")
            .expect("terminal event");
            assert!(matches!(terminal, ProviderEvent::Done { .. }));
            server.abort();
        }
    }

    fn normalized_event_snapshot(events: &[ProviderEvent]) -> serde_json::Value {
        fn normalize(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Object(object) => {
                    for (key, value) in object {
                        if key == "timestamp" {
                            *value = serde_json::json!("<timestamp>");
                        } else if key == "provider_instance_id" {
                            *value = serde_json::json!("<provider_instance_id>");
                        } else {
                            normalize(value);
                        }
                    }
                }
                serde_json::Value::Array(values) => {
                    for value in values {
                        normalize(value);
                    }
                }
                _ => {}
            }
        }
        let mut snapshot = serde_json::to_value(events).expect("serialize provider events");
        normalize(&mut snapshot);
        snapshot
    }

    fn reconstruct_terminal(events: &[ProviderEvent]) -> AssistantMessage {
        let mut assembler = MessageAssembler::new();
        let mut terminal = None;
        for event in events {
            if let Some(message) = assembler.apply(event).expect("consumer event sequence") {
                terminal = Some(message);
            }
        }
        terminal.expect("terminal message")
    }

    fn event_types(events: &[ProviderEvent]) -> Vec<&str> {
        events
            .iter()
            .map(|event| match event {
                ProviderEvent::Start => "start",
                ProviderEvent::TextStart { .. } => "text_start",
                ProviderEvent::TextDelta { .. } => "text_delta",
                ProviderEvent::TextEnd { .. } => "text_end",
                ProviderEvent::ThinkingStart { .. } => "thinking_start",
                ProviderEvent::ThinkingDelta { .. } => "thinking_delta",
                ProviderEvent::ThinkingEnd { .. } => "thinking_end",
                ProviderEvent::ToolCallStart { .. } => "tool_call_start",
                ProviderEvent::ToolCallDelta { .. } => "tool_call_delta",
                ProviderEvent::ToolCallPreview { .. } => "tool_call_preview",
                ProviderEvent::ToolCallEnd { .. } => "tool_call_end",
                ProviderEvent::ToolCallRejected { .. } => "tool_call_rejected",
                ProviderEvent::ReasoningSummaryStart { .. } => "reasoning_summary_start",
                ProviderEvent::ReasoningSummaryDelta { .. } => "reasoning_summary_delta",
                ProviderEvent::ReasoningSummaryEnd { .. } => "reasoning_summary_end",
                ProviderEvent::Done { .. } => "done",
                ProviderEvent::Error { .. } => "error",
            })
            .collect()
    }

    fn complete_snapshot_digest(events: &[ProviderEvent]) -> String {
        use sha2::{Digest, Sha256};

        let bytes =
            serde_json::to_vec(&normalized_event_snapshot(events)).expect("snapshot serialize");
        format!("{:x}", Sha256::digest(bytes))
    }

    #[tokio::test]
    async fn fixture_replays_match_complete_normalized_event_snapshots() {
        let tool_context = PromptContext {
            tools: vec![ToolDefinition {
                name: "read_file".to_owned(),
                description: String::new(),
                parameters: serde_json::json!({
                    "type":"object",
                    "properties":{"path":{"type":"string"}},
                    "required":["path"]
                }),
            }],
            ..empty_context()
        };
        let glm_context = PromptContext {
            tools: vec![ToolDefinition {
                name: "bash".to_owned(),
                description: String::new(),
                parameters: serde_json::json!({
                    "type":"object",
                    "properties":{"command":{"type":"string"}},
                    "required":["command"],
                    "additionalProperties":false
                }),
            }],
            ..empty_context()
        };
        let mut actual = serde_json::Map::new();
        for (name, events) in [
            (
                "kimi_text",
                replay(
                    "kimi-k3",
                    include_str!("../../tests/fixtures/kimi_text.sse"),
                )
                .await,
            ),
            (
                "kimi_reasoning",
                replay(
                    "kimi-k3",
                    include_str!("../../tests/fixtures/kimi_reasoning.sse"),
                )
                .await,
            ),
            (
                "kimi_toolcall",
                replay_with_context(
                    "kimi-k3",
                    include_str!("../../tests/fixtures/kimi_toolcall.sse"),
                    tool_context,
                )
                .await,
            ),
            (
                "glm_tool_stream",
                replay_with_context(
                    "glm-5.2",
                    include_str!("../../tests/fixtures/glm_tool_stream.sse"),
                    glm_context,
                )
                .await,
            ),
        ] {
            actual.insert(name.to_owned(), normalized_event_snapshot(&events));
        }
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/snapshots/provider_fixture_events.json"
        ))
        .expect("provider event snapshot JSON");
        assert_eq!(serde_json::Value::Object(actual), expected);
    }

    #[tokio::test]
    async fn draft_2020_12_unevaluated_items_rejection_stays_tool_use_and_redacted() {
        const RAW_ARGUMENT_SECRET: &str = "raw-argument-secret";
        const SCHEMA_TEXT_SECRET: &str = "schema-text-secret";
        const INVALID_TOOL_CALL: &str = concat!(
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,",
            "\"id\":\"call-array\",\"function\":{\"name\":\"array_tool\",",
            "\"arguments\":\"{\\\"route\\\":\\\"normal\\\",\\\"input\\\":{\\\"items\\\":[\\\"ok\\\",\\\"raw-argument-secret\\\"]}}\"}}]},",
            "\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n"
        );
        let context = PromptContext {
            tools: vec![ToolDefinition {
                name: "array_tool".to_owned(),
                description: String::new(),
                parameters: serde_json::json!({
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "description": SCHEMA_TEXT_SECRET,
                    "type": "object",
                    "properties": {
                        "items": {
                            "type": "array",
                            "prefixItems": [{"type": "string"}],
                            "unevaluatedItems": false
                        }
                    },
                    "required": ["items"]
                }),
            }],
            ..empty_context()
        };

        let events = replay_with_context("kimi-k3", INVALID_TOOL_CALL, context).await;
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ProviderEvent::Error { .. }))
        );
        let rejection = events
            .iter()
            .find_map(|event| match event {
                ProviderEvent::ToolCallRejected {
                    rejected,
                    synthetic_result,
                    ..
                } => Some((rejected, synthetic_result)),
                _ => None,
            })
            .expect("schema-invalid arguments must emit ToolCallRejected");
        assert_eq!(rejection.0.error, ToolArgumentError::SchemaViolation);
        assert_eq!(
            rejection.1.details["instance_path"],
            serde_json::json!("/items")
        );
        assert_eq!(
            rejection.1.details["constraint"],
            serde_json::json!("schema")
        );
        let serialized_result =
            serde_json::to_string(rejection.1).expect("serialize safe synthetic result");
        for sensitive in [RAW_ARGUMENT_SECRET, SCHEMA_TEXT_SECRET, "unevaluatedItems"] {
            assert!(
                !serialized_result.contains(sensitive),
                "synthetic result leaked {sensitive}"
            );
        }

        let terminal = reconstruct_terminal(&events);
        assert_eq!(terminal.stop_reason, StopReason::ToolUse);
        assert_eq!(terminal.provider_code.as_deref(), Some("tool_calls"));
        assert!(matches!(
            terminal.content.as_slice(),
            [AssistantContent::RejectedToolCall { rejected, .. }]
                if rejected.id == "call-array"
                    && rejected.name == "array_tool"
                    && rejected.error == ToolArgumentError::SchemaViolation
        ));
        let serialized_terminal =
            serde_json::to_string(&terminal).expect("serialize terminal assistant message");
        for sensitive in [RAW_ARGUMENT_SECRET, SCHEMA_TEXT_SECRET, "unevaluatedItems"] {
            assert!(
                !serialized_terminal.contains(sensitive),
                "terminal message leaked {sensitive}"
            );
        }
        assert!(matches!(
            events.last(),
            Some(ProviderEvent::Done {
                reason: StopReason::ToolUse,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn opencode_live_capture_preserves_post_done_trailer_and_matches_snapshot() {
        let raw = include_str!("../../tests/fixtures/opencode_kimi_k2_7_code_text.sse");
        assert_eq!(
            raw.lines().filter(|line| line.starts_with("data:")).count(),
            23
        );
        let done = raw.find("data: [DONE]").expect("DONE marker");
        let post_done = raw[done..]
            .find("data: {\"choices\":[],\"cost\":\"0\"}")
            .expect("post-DONE cost trailer");
        let events = replay("opencode-go", raw).await;
        assert!(post_done > 0);
        let ProviderEvent::Done { output, .. } = events.last().expect("terminal") else {
            panic!("OpenCode capture must finish with Done")
        };
        assert_eq!(output.message.usage.input, 13);
        assert_eq!(output.message.usage.output, 19);
        assert_eq!(output.message.usage.reasoning, 14);
        assert_eq!(output.message.usage.total_tokens, 32);
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/snapshots/opencode_kimi_k2_7_code_text.events.json"
        ))
        .expect("OpenCode event snapshot");
        assert_eq!(normalized_event_snapshot(&events), expected);
    }

    #[tokio::test]
    async fn provider_specific_finish_reasons_match_complete_error_snapshots() {
        let mut actual = serde_json::Map::new();
        for (name, fixture) in [
            (
                "sensitive",
                include_str!("../../tests/fixtures/glm_sensitive.sse"),
            ),
            (
                "network_error",
                include_str!("../../tests/fixtures/glm_network_error.sse"),
            ),
            (
                "model_context_window_exceeded",
                include_str!("../../tests/fixtures/glm_context_window.sse"),
            ),
        ] {
            let events = replay("glm-5.2", fixture).await;
            actual.insert(name.to_owned(), normalized_event_snapshot(&events));
        }
        let expected: serde_json::Value = serde_json::from_str(include_str!(
            "../../tests/snapshots/glm_provider_finish_reasons.events.json"
        ))
        .expect("provider finish-reason snapshots");
        assert_eq!(serde_json::Value::Object(actual), expected);
    }

    #[test]
    #[ignore = "controlled-host performance smoke; run explicitly with --ignored"]
    fn controlled_host_adapter_normalization_p95_smoke_is_under_30ms() {
        let payload = r#"{"choices":[{"delta":{"content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":2}}"#;
        let registry = FrozenToolSchemaRegistry::compile(&[]).expect("registry");
        let mut samples = Vec::with_capacity(100);
        for _ in 0..100 {
            let started = std::time::Instant::now();
            let mut receive = ChatReceiveState::new(registry.clone());
            receive.push_json(payload).expect("chunk");
            receive.finish(Utc::now()).expect("terminal");
            samples.push(started.elapsed());
        }
        samples.sort_unstable();
        let p95 = samples[94];
        assert!(
            p95 < Duration::from_millis(30),
            "adapter normalization p95 was {p95:?}"
        );
    }

    #[tokio::test]
    async fn response_header_wait_is_bounded_and_precancel_does_not_record_request_sent() {
        let cancel = CancellationToken::new();
        let first_poll_count = Cell::new(0);
        assert!(matches!(
            await_request(
                std::future::pending::<Result<(), ()>>(),
                &cancel,
                Duration::from_millis(1),
                |_| first_poll_count.set(first_poll_count.get() + 1),
            )
            .await,
            RequestWait::TimedOut
        ));
        assert_eq!(first_poll_count.get(), 1);

        cancel.cancel();
        assert!(matches!(
            await_request(
                std::future::ready(Ok::<_, ()>(())),
                &cancel,
                Duration::ZERO,
                |_| first_poll_count.set(first_poll_count.get() + 1),
            )
            .await,
            RequestWait::Cancelled
        ));
        assert_eq!(
            first_poll_count.get(),
            1,
            "pre-cancelled biased select must not record request_sent"
        );
    }

    #[tokio::test]
    async fn normalized_but_unsent_public_delta_does_not_record_ttft() {
        let registry = FrozenToolSchemaRegistry::compile(&[]).expect("registry");
        let mut receive = ChatReceiveState::new(registry);
        let events = receive
            .push_json(r#"{"choices":[{"delta":{"content":"hello"}}]}"#)
            .expect("normalized chunk");
        let delta = events
            .into_iter()
            .find(|event| matches!(event, ProviderEvent::TextDelta { .. }))
            .expect("normalized text delta");

        let (tx, _rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut assembler = MessageAssembler::new();
        assembler.apply(&ProviderEvent::Start).expect("Start");
        assembler
            .apply(&ProviderEvent::TextStart { content_index: 0 })
            .expect("TextStart");
        let result = emit(&tx, &mut assembler, delta, &cancel).await;
        assert!(matches!(result, EmitResult::Cancelled));

        let mut ttft = TtftObservation::new(Instant::now(), None);
        ttft.observe_emit(true, &result);
        assert!(!ttft.first_public_delta_sent);

        let (tx, mut rx) = mpsc::channel(1);
        let mut assembler = MessageAssembler::new();
        assembler.apply(&ProviderEvent::Start).expect("Start");
        assembler
            .apply(&ProviderEvent::TextStart { content_index: 0 })
            .expect("TextStart");
        let result = emit(
            &tx,
            &mut assembler,
            ProviderEvent::TextDelta {
                content_index: 0,
                delta: "sent".to_owned(),
            },
            &CancellationToken::new(),
        )
        .await;
        assert!(matches!(result, EmitResult::Sent));
        ttft.observe_emit(true, &result);
        assert!(ttft.first_public_delta_sent);
        assert!(matches!(
            rx.recv().await,
            Some(ProviderEvent::TextDelta { delta, .. }) if delta == "sent"
        ));
    }

    #[tokio::test]
    async fn response_header_timeout_aborts_when_already_cancelled_preserving_partial() {
        let spec = ModelSpec::preset("kimi-k3").expect("preset");
        let (priority_tx, mut priority_rx) = mpsc::channel(1);
        let mut assembler = MessageAssembler::new();
        assembler.apply(&ProviderEvent::Start).expect("Start");
        assembler
            .apply(&ProviderEvent::TextStart { content_index: 0 })
            .expect("text start");
        assembler
            .apply(&ProviderEvent::TextDelta {
                content_index: 0,
                delta: "partial".to_owned(),
            })
            .expect("text delta");

        let cancel = CancellationToken::new();
        cancel.cancel();

        finish_failure(
            &priority_tx,
            &mut assembler,
            &spec,
            Usage {
                input: 5,
                output: 2,
                total_tokens: 7,
                ..Default::default()
            },
            format!(
                "provider response headers timed out after {} seconds",
                RESPONSE_HEADER_TIMEOUT.as_secs()
            ),
            "response_header_timeout",
            cancel.is_cancelled(),
        )
        .await;

        let event = priority_rx.recv().await.expect("priority terminal");
        let ProviderEvent::Error { reason, output } = event else {
            panic!("expected Error terminal, got {event:?}")
        };
        assert_eq!(reason, StopReason::Aborted);
        assert_eq!(output.message.stop_reason, StopReason::Aborted);
        assert!(output.message.interrupted);
        assert_eq!(
            output.message.provider_code.as_deref(),
            Some("response_header_timeout")
        );
        assert!(!retry::is_retryable(&output.message));
        assert!(matches!(
            output.message.content.as_slice(),
            [types::AssistantContent::Text { text, .. }] if text == "partial"
        ));
    }

    #[tokio::test]
    async fn adapter_push_json_error_aborts_when_already_cancelled_preserving_partial() {
        let spec = ModelSpec::preset("kimi-k3").expect("preset");
        let (priority_tx, mut priority_rx) = mpsc::channel(1);
        let schemas = FrozenToolSchemaRegistry::compile(&[]).expect("registry");
        let mut receive = ChatReceiveState::with_budget(schemas, ResponseBudget::default());
        let mut assembler = MessageAssembler::new();
        assembler.apply(&ProviderEvent::Start).expect("Start");

        let events = receive
            .push_json(r#"{"choices":[{"delta":{"content":"partial"}}]}"#)
            .expect("valid partial chunk");
        for event in events {
            assembler.apply(&event).expect("assemble partial");
        }

        let cancel = CancellationToken::new();
        cancel.cancel();

        let error = receive
            .push_json("this is not valid json")
            .expect_err("invalid chunk must fail parsing");
        close_partial(&mut receive, &mut assembler);
        let (message, code) = adapter_error(&error);
        finish_failure(
            &priority_tx,
            &mut assembler,
            &spec,
            receive.usage().clone(),
            message,
            &code,
            cancel.is_cancelled(),
        )
        .await;

        let event = priority_rx.recv().await.expect("priority terminal");
        let ProviderEvent::Error { reason, output } = event else {
            panic!("expected Error terminal, got {event:?}")
        };
        assert_eq!(reason, StopReason::Aborted);
        assert_eq!(output.message.stop_reason, StopReason::Aborted);
        assert!(output.message.interrupted);
        assert_eq!(
            output.message.provider_code.as_deref(),
            Some("invalid_sse_json")
        );
        assert!(!retry::is_retryable(&output.message));
        assert!(matches!(
            output.message.content.as_slice(),
            [types::AssistantContent::Text { text, .. }] if text == "partial"
        ));
    }

    #[tokio::test]
    async fn observed_response_header_wait_records_only_an_actually_polled_request() {
        let cancel = CancellationToken::new();
        let first_poll_count = Cell::new(0);
        assert!(matches!(
            await_request(
                std::future::pending::<Result<(), ()>>(),
                &cancel,
                Duration::from_millis(1),
                |_| first_poll_count.set(first_poll_count.get() + 1),
            )
            .await,
            RequestWait::TimedOut
        ));
        assert_eq!(first_poll_count.get(), 1);

        cancel.cancel();
        assert!(matches!(
            await_request(
                std::future::ready(Ok::<_, ()>(())),
                &cancel,
                Duration::ZERO,
                |_| first_poll_count.set(first_poll_count.get() + 1),
            )
            .await,
            RequestWait::Cancelled
        ));
        assert_eq!(first_poll_count.get(), 1);
    }

    #[tokio::test]
    async fn all_protocols_pre_cancel_before_request_poll_converge_to_aborted() {
        for preset in ["kimi-k3", "openai-responses", "anthropic"] {
            let spec = ModelSpec::preset(preset).expect("preset");
            let cancel = CancellationToken::new();
            cancel.cancel();
            let mut events = stream_with_api_key(
                spec,
                PromptContext {
                    messages: vec![ContextMessage::Synthetic {
                        message: Message::User(UserMessage {
                            content: vec![UserContent::Text {
                                text: "pre-cancel".into(),
                            }],
                            timestamp: Utc::now(),
                        }),
                    }],
                    ..empty_context()
                },
                RequestOptions::default(),
                cancel,
                Some("test-key".into()),
            );
            assert!(matches!(events.recv().await, Some(ProviderEvent::Start)));
            let terminal = events.recv().await.expect("terminal");
            assert!(
                matches!(
                    terminal,
                ProviderEvent::Error {
                    reason: StopReason::Aborted,
                    ref output,
                } if output.message.interrupted
                ),
                "{preset}: {terminal:?}"
            );
            assert!(events.recv().await.is_none(), "{preset} stream must fuse");
        }
    }

    async fn assert_preflight_cancelled(
        preset: &str,
        options: RequestOptions,
        api_key: Option<String>,
        expected_code: &str,
    ) {
        let mut spec = ModelSpec::preset(preset).expect("preset");
        spec.base_url = "http://127.0.0.1:9".to_owned();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut events = stream_with_api_key(spec, empty_context(), options, cancel, api_key);
        assert!(matches!(events.recv().await, Some(ProviderEvent::Start)));
        let terminal = events.recv().await.expect("terminal");
        assert!(
            matches!(
                terminal,
                ProviderEvent::Error {
                    reason: StopReason::Aborted,
                    ref output,
                } if output.message.interrupted
                    && output.message.provider_code.as_deref() == Some(expected_code)
            ),
            "{preset}: {terminal:?}"
        );
        assert!(events.recv().await.is_none(), "{preset} stream must fuse");
    }

    #[tokio::test]
    async fn all_protocols_pre_cancelled_missing_key_preserves_failure_code_and_aborts() {
        for preset in ["kimi-k3", "openai-responses", "anthropic"] {
            assert_preflight_cancelled(preset, RequestOptions::default(), None, "missing_api_key")
                .await;
        }
    }

    #[tokio::test]
    async fn all_protocols_pre_cancelled_invalid_max_preserves_failure_code_and_aborts() {
        for preset in ["kimi-k3", "openai-responses", "anthropic"] {
            assert_preflight_cancelled(
                preset,
                RequestOptions {
                    max_tokens: Some(u64::MAX),
                    ..RequestOptions::default()
                },
                Some("test-key".to_owned()),
                "invalid_provider_request",
            )
            .await;
        }
    }

    #[tokio::test]
    async fn all_protocol_terminal_errors_observed_after_cancel_converge_to_aborted() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let success = SuccessTerminalCommit::new();

        let chat = ModelSpec::preset("kimi-k3").unwrap();
        let (tx, _rx) = mpsc::channel(1);
        let (priority, mut terminal_rx) = mpsc::channel(1);
        let mut assembler = MessageAssembler::new();
        assembler.apply(&ProviderEvent::Start).unwrap();
        finish_terminal(
            &tx,
            &priority,
            &mut assembler,
            &chat,
            ChatTerminal {
                events: vec![],
                usage: Usage::default(),
                stop_reason: StopReason::Error,
                error_message: Some("late error".into()),
                provider_code: Some("late_error".into()),
            },
            &cancel,
            &success,
        )
        .await;
        assert!(matches!(
            terminal_rx.recv().await,
            Some(ProviderEvent::Error {
                reason: StopReason::Aborted,
                ..
            })
        ));

        let responses = ModelSpec::preset("openai-responses").unwrap();
        let (priority, mut terminal_rx) = mpsc::channel(1);
        let mut assembler = MessageAssembler::new();
        assembler.apply(&ProviderEvent::Start).unwrap();
        finish_responses_terminal(
            &tx,
            &priority,
            &mut assembler,
            &responses,
            ResponsesTerminal {
                events: vec![],
                reason: StopReason::Error,
                usage: Usage::default(),
                error_message: Some("late error".into()),
                provider_code: Some("late_error".into()),
                response_model: None,
                provider_context: vec![],
            },
            &cancel,
            &success,
        )
        .await;
        assert!(matches!(
            terminal_rx.recv().await,
            Some(ProviderEvent::Error {
                reason: StopReason::Aborted,
                ..
            })
        ));

        let anthropic = ModelSpec::preset("anthropic").unwrap();
        let (priority, mut terminal_rx) = mpsc::channel(1);
        let mut assembler = MessageAssembler::new();
        assembler.apply(&ProviderEvent::Start).unwrap();
        finish_anthropic_terminal(
            &tx,
            &priority,
            &mut assembler,
            &anthropic,
            AnthropicTerminal {
                events: vec![],
                reason: StopReason::Error,
                usage: Usage::default(),
                error_message: Some("late error".into()),
                provider_code: Some("late_error".into()),
                provider_context: vec![],
            },
            &cancel,
            &success,
        )
        .await;
        assert!(matches!(
            terminal_rx.recv().await,
            Some(ProviderEvent::Error {
                reason: StopReason::Aborted,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn chat_terminal_prepare_finish_contract_error_uses_failure_fallback() {
        let mut spec = ModelSpec::preset("kimi-k3").expect("preset");
        spec.id.clear();
        let (tx, _rx) = mpsc::channel(1);
        let (priority_tx, mut priority_rx) = mpsc::channel(1);
        let mut assembler = MessageAssembler::new();
        assembler.apply(&ProviderEvent::Start).expect("Start");
        let cancel = CancellationToken::new();
        let committed = SuccessTerminalCommit::new();

        finish_terminal(
            &tx,
            &priority_tx,
            &mut assembler,
            &spec,
            ChatTerminal {
                events: Vec::new(),
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                provider_code: Some("stop".to_owned()),
            },
            &cancel,
            &committed,
        )
        .await;

        let ProviderEvent::Error { reason, output } =
            priority_rx.recv().await.expect("priority terminal")
        else {
            panic!("expected Error terminal")
        };
        assert_eq!(reason, StopReason::Error);
        assert_eq!(output.message.stop_reason, StopReason::Error);
        assert!(!output.message.interrupted);
        assert_eq!(
            output.message.provider_code.as_deref(),
            Some("normalized_event_contract_violation")
        );
        assert!(!committed.is_committed());
    }

    #[tokio::test]
    async fn responses_terminal_uses_canonical_origin_despite_observed_model() {
        let spec = ModelSpec::preset("openai-responses").expect("preset");
        let cancel = CancellationToken::new();

        // Matching observed model -> success and origin model is the canonical spec value.
        let (tx, mut rx) = mpsc::channel(1);
        let (priority_tx, _priority_rx) = mpsc::channel(1);
        let mut assembler = MessageAssembler::new();
        assembler.apply(&ProviderEvent::Start).expect("Start");
        let committed = SuccessTerminalCommit::new();
        finish_responses_terminal(
            &tx,
            &priority_tx,
            &mut assembler,
            &spec,
            ResponsesTerminal {
                events: vec![],
                reason: StopReason::Stop,
                usage: Usage::default(),
                error_message: None,
                provider_code: Some("stop".to_owned()),
                response_model: Some(spec.id.clone()),
                provider_context: vec![],
            },
            &cancel,
            &committed,
        )
        .await;
        let ProviderEvent::Done { output, .. } = rx.recv().await.expect("terminal") else {
            panic!("expected Done terminal")
        };
        assert_eq!(output.message.model, spec.id);
        assert_eq!(output.message.origin.model, spec.id);
        assert_eq!(output.message.origin, spec.origin());
        assert!(committed.is_committed());

        // Mismatched observed model -> still a normal success terminal, with canonical
        // origin/model preserved and terminal events still delivered on the normal lane.
        let (tx2, mut rx2) = mpsc::channel(10);
        let (priority_tx2, mut priority_rx2) = mpsc::channel(1);
        let mut assembler2 = MessageAssembler::new();
        assembler2.apply(&ProviderEvent::Start).expect("Start");
        let committed2 = SuccessTerminalCommit::new();
        finish_responses_terminal(
            &tx2,
            &priority_tx2,
            &mut assembler2,
            &spec,
            ResponsesTerminal {
                events: vec![
                    ProviderEvent::TextStart { content_index: 0 },
                    ProviderEvent::TextDelta {
                        content_index: 0,
                        delta: "hello".to_owned(),
                    },
                    ProviderEvent::TextEnd {
                        content_index: 0,
                        content: "hello".to_owned(),
                    },
                ],
                reason: StopReason::Stop,
                usage: Usage::default(),
                error_message: None,
                provider_code: Some("stop".to_owned()),
                response_model: Some("other-model".to_owned()),
                provider_context: vec![],
            },
            &cancel,
            &committed2,
        )
        .await;
        let mut output = None;
        let mut _normal_lane_events = 0;
        while let Some(event) = rx2.recv().await {
            _normal_lane_events += 1;
            if let ProviderEvent::Done {
                output: done_output,
                ..
            } = event
            {
                output = Some(done_output);
                break;
            }
        }
        let output = output.expect("expected Done terminal");
        assert_eq!(output.message.model, spec.id);
        assert_eq!(output.message.origin.model, spec.id);
        assert_eq!(output.message.origin, spec.origin());
        assert_eq!(output.message.content.len(), 1);
        assert!(committed2.is_committed());
        assert!(priority_rx2.try_recv().is_err());

        drop(tx2);
        let mut remaining_events = 0;
        while rx2.recv().await.is_some() {
            remaining_events += 1;
        }
        assert_eq!(
            remaining_events, 0,
            "all terminal events were already delivered on the normal lane"
        );
    }

    #[tokio::test]
    async fn fixture_stream_reconstructs_text_and_usage() {
        let received = replay(
            "kimi-k3",
            include_str!("../../tests/fixtures/kimi_text.sse"),
        )
        .await;
        assert!(matches!(received.first(), Some(ProviderEvent::Start)));
        assert!(received.iter().any(
            |event| matches!(event, ProviderEvent::TextDelta { delta, .. } if delta == "hello")
        ));
        let ProviderEvent::Done { output, .. } = received.last().expect("terminal") else {
            panic!("done")
        };
        assert_eq!(output.message.usage.total_tokens, 7);
        assert_eq!(output.message.content.len(), 1);
    }

    #[tokio::test]
    async fn fixture_tool_call_is_strictly_validated() {
        let (base_url, server) = serve_fixture(
            StatusCode::OK,
            include_str!("../../tests/fixtures/kimi_toolcall.sse"),
        )
        .await;
        let mut spec = ModelSpec::preset("kimi-k3").expect("preset");
        spec.base_url = base_url;
        let context = PromptContext {
            tools: vec![ToolDefinition {
                name: "read_file".to_owned(),
                description: String::new(),
                parameters: serde_json::json!({
                    "type":"object",
                    "properties":{"path":{"type":"string"}},
                    "required":["path"]
                }),
            }],
            ..empty_context()
        };
        let mut stream = stream_with_api_key(
            spec,
            context,
            RequestOptions::default(),
            CancellationToken::new(),
            Some("test-key".to_owned()),
        );
        let mut events = Vec::new();
        while let Some(event) = stream.recv().await {
            events.push(event);
        }
        server.abort();
        assert!(events.iter().any(|event| matches!(
            event,
            ProviderEvent::ToolCallEnd { tool_call, .. }
                if tool_call.arguments.as_object().get("path")
                    == Some(&serde_json::json!("a.txt"))
        )));
    }

    #[tokio::test]
    async fn fixture_reasoning_preserves_signature_and_wire_order() {
        let received = replay(
            "kimi-k3",
            include_str!("../../tests/fixtures/kimi_reasoning.sse"),
        )
        .await;
        assert!(received.iter().any(|event| matches!(
            event,
            ProviderEvent::ThinkingStart {
                signature_field,
                ..
            } if signature_field == "reasoning_content"
        )));
        let ProviderEvent::Done { output, .. } = received.last().expect("terminal") else {
            panic!("done")
        };
        assert!(matches!(
            output.message.content.as_slice(),
            [
                types::AssistantContent::Thinking {
                    wire_item_index: 0,
                    ..
                },
                types::AssistantContent::Text {
                    wire_item_index: 1,
                    ..
                }
            ]
        ));
    }

    #[tokio::test]
    async fn glm_fixture_normalizes_reasoning_and_tool_stream() {
        let (base_url, server) = serve_fixture(
            StatusCode::OK,
            include_str!("../../tests/fixtures/glm_tool_stream.sse"),
        )
        .await;
        let mut spec = ModelSpec::preset("glm-5.2").expect("preset");
        spec.base_url = base_url;
        let context = PromptContext {
            tools: vec![ToolDefinition {
                name: "bash".to_owned(),
                description: String::new(),
                parameters: serde_json::json!({
                    "type":"object",
                    "properties":{"command":{"type":"string"}},
                    "required":["command"],
                    "additionalProperties":false
                }),
            }],
            ..empty_context()
        };
        let mut stream = stream_with_api_key(
            spec,
            context,
            RequestOptions::default(),
            CancellationToken::new(),
            Some("test-key".to_owned()),
        );
        let mut events = Vec::new();
        while let Some(event) = stream.recv().await {
            events.push(event);
        }
        server.abort();
        assert!(events.iter().any(|event| matches!(
            event,
            ProviderEvent::ThinkingStart {
                signature_field,
                ..
            } if signature_field == "reasoning"
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            ProviderEvent::ToolCallEnd { tool_call, .. }
                if tool_call.name == "bash"
                    && tool_call.arguments.as_object().get("command")
                        == Some(&serde_json::json!("pwd"))
        )));
        assert!(matches!(
            events.last(),
            Some(ProviderEvent::Done {
                reason: StopReason::ToolUse,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn invalid_tool_arguments_emit_rejection_without_executable_call() {
        const INVALID_TOOL_STREAM: &str = concat!(
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,",
            "\"id\":\"call-invalid\",\"function\":{\"name\":\"read_file\",",
            "\"arguments\":\"{\\\"path\\\":\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n",
            ": fixture eof\n"
        );
        let (base_url, server) = serve_fixture(StatusCode::OK, INVALID_TOOL_STREAM).await;
        let mut spec = ModelSpec::preset("kimi-k3").expect("preset");
        spec.base_url = base_url;
        let context = PromptContext {
            tools: vec![ToolDefinition {
                name: "read_file".to_owned(),
                description: String::new(),
                parameters: serde_json::json!({
                    "type":"object",
                    "properties":{"path":{"type":"string"}},
                    "required":["path"]
                }),
            }],
            ..empty_context()
        };
        let mut stream = stream_with_api_key(
            spec,
            context,
            RequestOptions::default(),
            CancellationToken::new(),
            Some("test-key".to_owned()),
        );
        let mut events = Vec::new();
        while let Some(event) = stream.recv().await {
            events.push(event);
        }
        server.abort();
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ProviderEvent::ToolCallEnd { .. }))
        );
        assert!(events.iter().any(|event| matches!(
            event,
            ProviderEvent::ToolCallRejected {
                rejected,
                synthetic_result,
                ..
            } if rejected.error == types::ToolArgumentError::InvalidJson
                && synthetic_result.is_error
        )));
        assert_eq!(
            event_types(&events),
            [
                "start",
                "tool_call_start",
                "tool_call_delta",
                "tool_call_preview",
                "tool_call_rejected",
                "done"
            ]
        );
        assert_eq!(
            reconstruct_terminal(&events),
            match events.last().expect("terminal") {
                ProviderEvent::Done { output, .. } => output.message.clone(),
                other => panic!("unexpected terminal: {other:?}"),
            }
        );
        assert_eq!(
            complete_snapshot_digest(&events),
            "69a1e7c2a9312f5c121694947b8058475bb5a06a13651605d15b21187a79c3a0"
        );
    }

    #[tokio::test]
    async fn http_error_and_transport_eof_are_error_events() {
        let (base_url, server) = serve_fixture(
            StatusCode::TOO_MANY_REQUESTS,
            include_str!("../../tests/fixtures/http_429.json"),
        )
        .await;
        let mut spec = ModelSpec::preset("kimi-k3").expect("preset");
        spec.base_url = base_url;
        let mut stream = stream_with_api_key(
            spec,
            empty_context(),
            RequestOptions::default(),
            CancellationToken::new(),
            Some("test-key".to_owned()),
        );
        let mut http_events = Vec::new();
        while let Some(event) = stream.recv().await {
            http_events.push(event);
        }
        server.abort();
        assert!(matches!(
            http_events.last(),
            Some(ProviderEvent::Error { output, .. })
                if output.message.provider_code.as_deref() == Some("http_429")
        ));
        assert_eq!(event_types(&http_events), ["start", "error"]);
        assert_eq!(
            reconstruct_terminal(&http_events).provider_code.as_deref(),
            Some("http_429")
        );
        assert_eq!(
            complete_snapshot_digest(&http_events),
            "f7a1a99b17c346a2a1ee1b33954096ac1abb371ae7f800e690f14ceadff790e4"
        );

        let events = replay(
            "kimi-k3",
            include_str!("../../tests/fixtures/transport_error.sse"),
        )
        .await;
        let Some(ProviderEvent::Error { output, .. }) = events.last() else {
            panic!("transport fixture must close with Error")
        };
        assert_eq!(
            output.message.provider_code.as_deref(),
            Some("unexpected_sse_eof")
        );
        assert!(retry::is_retryable(&output.message));
        assert_eq!(event_types(&events), ["start", "error"]);
        assert!(matches!(
            reconstruct_terminal(&events).content.as_slice(),
            [types::AssistantContent::Text { text, .. }] if text == "partial"
        ));
        assert_eq!(
            complete_snapshot_digest(&events),
            "8d9fb930bc77a119c5274b7df8838db6e3fcd9cf422cfc82881a25c622a70592"
        );
    }

    #[tokio::test]
    async fn opencode_console_upstream_400_survives_transport_and_is_provider_scoped() {
        const BODY: &str = concat!(
            "{\"error\":{",
            "\"message\":\"Error from provider (Console Go): Upstream request failed\",",
            "\"type\":\"invalid_request_error\",",
            "\"param\":null,",
            "\"code\":\"invalid_request_error\"}}"
        );

        for (preset, expected_retryable) in [("opencode-go", true), ("kimi-k3", false)] {
            let (base_url, server) = serve_fixture(StatusCode::BAD_REQUEST, BODY).await;
            let mut spec = ModelSpec::preset(preset).expect("preset");
            spec.base_url = base_url;
            let mut stream = stream_with_api_key(
                spec,
                empty_context(),
                RequestOptions::default(),
                CancellationToken::new(),
                Some("test-key".to_owned()),
            );
            let mut events = Vec::new();
            while let Some(event) = stream.recv().await {
                events.push(event);
            }
            server.abort();

            assert_eq!(event_types(&events), ["start", "error"]);
            let message = reconstruct_terminal(&events);
            assert_eq!(message.stop_reason, StopReason::Error);
            assert_eq!(message.provider_code.as_deref(), Some("http_400"));
            let expected_error = format!("400: {BODY}");
            assert_eq!(
                message.error_message.as_deref(),
                Some(expected_error.as_str())
            );
            assert_eq!(
                retry::is_retryable(&message),
                expected_retryable,
                "{preset}"
            );
        }
    }

    #[tokio::test]
    async fn http_status_survives_error_body_reset_for_overflow_and_nonretryable_4xx() {
        for (status, expected_code) in [
            (StatusCode::PAYLOAD_TOO_LARGE, "http_413"),
            (StatusCode::BAD_REQUEST, "http_400"),
        ] {
            let (base_url, server) = serve_reset_error_body(status, r#"{"error":"partial"#).await;
            let mut spec = ModelSpec::preset("kimi-k3").expect("preset");
            spec.base_url = base_url;
            let mut stream = stream_with_api_key(
                spec,
                empty_context(),
                RequestOptions::default(),
                CancellationToken::new(),
                Some("test-key".to_owned()),
            );
            let mut events = Vec::new();
            while let Some(event) = stream.recv().await {
                events.push(event);
            }
            server.abort();

            assert_eq!(event_types(&events), ["start", "error"]);
            let message = reconstruct_terminal(&events);
            assert_eq!(message.stop_reason, StopReason::Error);
            assert_eq!(message.provider_code.as_deref(), Some(expected_code));
            assert!(
                message
                    .error_message
                    .as_deref()
                    .is_some_and(|error| error.contains(r#"{"error":"partial"#)),
                "bounded partial error body was not retained: {:?}",
                message.error_message
            );
            assert!(!retry::is_retryable(&message));

            if status == StatusCode::PAYLOAD_TOO_LARGE {
                assert_eq!(
                    overflow::classify_context_overflow(&message, None),
                    Some(overflow::OverflowClassification::ImmediateRecovery(
                        overflow::OverflowSource::ProviderCode,
                    ))
                );
            } else {
                assert_eq!(overflow::classify_context_overflow(&message, None), None);
            }
        }
    }

    #[test]
    fn compact_non_success_status_survives_every_bounded_body_failure() {
        for error in [
            NativeCompactionError::BodyIdleTimeout,
            NativeCompactionError::Transport("connection reset".into()),
            NativeCompactionError::ResponseLimitExceeded { limit: 16_000 },
        ] {
            let retained = retain_compact_http_status(StatusCode::BAD_GATEWAY, Err(error))
                .expect_err("non-success body failure must retain HTTP status");
            assert!(matches!(
                retained,
                NativeCompactionError::Http {
                    status: 502,
                    body
                } if body.starts_with("failed to read response body:")
            ));
        }

        assert!(matches!(
            retain_compact_http_status(
                StatusCode::BAD_GATEWAY,
                Err(NativeCompactionError::Cancelled),
            ),
            Err(NativeCompactionError::Cancelled)
        ));
        assert!(matches!(
            retain_compact_http_status(StatusCode::OK, Err(NativeCompactionError::BodyIdleTimeout),),
            Err(NativeCompactionError::BodyIdleTimeout)
        ));
    }

    #[tokio::test]
    async fn explicit_cancellation_preempts_non_success_error_body_read() {
        let (base_url, server) = serve_stalled_error_body(StatusCode::PAYLOAD_TOO_LARGE).await;
        let cancel = CancellationToken::new();
        let mut spec = ModelSpec::preset("kimi-k3").expect("preset");
        spec.base_url = base_url;
        let mut events = stream_with_api_key(
            spec,
            empty_context(),
            RequestOptions::default(),
            cancel.clone(),
            Some("test-key".to_owned()),
        );
        assert!(matches!(events.recv().await, Some(ProviderEvent::Start)));
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancel.cancel();
        let terminal = assert_aborted_within_one_second(&mut events).await;
        let ProviderEvent::Error { output, .. } = terminal else {
            panic!("explicit cancellation must close with Error(Aborted)")
        };
        assert_eq!(output.message.stop_reason, StopReason::Aborted);
        assert_eq!(output.message.provider_code.as_deref(), Some("cancelled"));
        server.abort();
    }

    #[tokio::test]
    async fn no_identity_parallel_tool_continuation_closes_with_error_without_executable_call() {
        const BODY: &str = concat!(
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[",
            "{\"index\":0,\"id\":\"call-a\",\"function\":{\"name\":\"read_file\",\"arguments\":\"{\\\"path\\\":\"}},",
            "{\"index\":1,\"id\":\"call-b\",\"function\":{\"name\":\"read_file\",\"arguments\":\"{\\\"path\\\":\"}}",
            "]}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[",
            "{\"function\":{\"arguments\":\"\\\"b.txt\\\"}\"}}",
            "]}}]}\n\n",
            "data: [DONE]\n\n"
        );
        let context = PromptContext {
            tools: vec![ToolDefinition {
                name: "read_file".to_owned(),
                description: String::new(),
                parameters: serde_json::json!({
                    "type":"object",
                    "properties":{"path":{"type":"string"}},
                    "required":["path"]
                }),
            }],
            ..empty_context()
        };
        let events = replay_with_context("kimi-k3", BODY, context).await;
        let Some(ProviderEvent::Error { output, .. }) = events.last() else {
            panic!("missing tool delta identity must close with Error")
        };
        assert_eq!(
            output.message.provider_code.as_deref(),
            Some("invalid_provider_stream")
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, ProviderEvent::ToolCallEnd { .. }))
        );
        assert!(
            !output
                .message
                .content
                .iter()
                .any(|content| matches!(content, types::AssistantContent::ToolCall { .. }))
        );
        assert_eq!(event_types(&events), ["start", "error"]);
        assert_eq!(reconstruct_terminal(&events), output.message);
    }

    #[tokio::test]
    async fn raw_failure_streams_preserve_complete_terminal_columns() {
        const MISSING_FINISH: &str = concat!(
            "data: {\"id\":\"missing-finish\",\"model\":\"kimi-k3\",",
            "\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n",
            "data: [DONE]\n\n"
        );
        let missing = replay("kimi-k3", MISSING_FINISH).await;
        assert_eq!(event_types(&missing), ["start", "error"]);
        let missing_message = reconstruct_terminal(&missing);
        assert_eq!(
            missing_message.provider_code.as_deref(),
            Some("stream_ended_without_finish_reason")
        );
        assert!(matches!(
            missing_message.content.as_slice(),
            [types::AssistantContent::Text { text, .. }] if text == "partial"
        ));
        assert_eq!(
            complete_snapshot_digest(&missing),
            "614bbddf59981e79fc39ba6750f6edeff062dcc58fcc850cb2a3130b5754e3a4"
        );

        const PROVIDER_ERROR_WITH_USAGE: &str = concat!(
            "data: {\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3},\"choices\":[],",
            "\"error\":{\"code\":\"network_error\",\"message\":\"failed\"}}\n\n"
        );
        let provider_error = replay("kimi-k3", PROVIDER_ERROR_WITH_USAGE).await;
        assert_eq!(event_types(&provider_error), ["start", "error"]);
        let provider_message = reconstruct_terminal(&provider_error);
        assert_eq!(
            provider_message.provider_code.as_deref(),
            Some("network_error")
        );
        assert_eq!(provider_message.usage.input, 7);
        assert_eq!(provider_message.usage.output, 3);
        assert_eq!(
            complete_snapshot_digest(&provider_error),
            "a0a57c76a2a22c1bd25d0749a6f5fc2a0a475dadd61939c9234b030d668b2f07"
        );

        const FINISH_ERROR_WITH_USAGE: &str = concat!(
            "data: {\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":5},",
            "\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n"
        );
        let finish_error = replay("kimi-k3", FINISH_ERROR_WITH_USAGE).await;
        assert_eq!(event_types(&finish_error), ["start", "error"]);
        let finish_message = reconstruct_terminal(&finish_error);
        assert_eq!(
            finish_message.provider_code.as_deref(),
            Some("invalid_provider_stream")
        );
        assert_eq!(finish_message.usage.input, 11);
        assert_eq!(finish_message.usage.output, 5);
        assert_eq!(
            complete_snapshot_digest(&finish_error),
            "ffe431a6a6c61b48a446906bee71675f6591667a389e6b36d61fe57a0adea0ba"
        );
    }

    #[tokio::test]
    async fn cancellation_while_waiting_for_headers_closes_within_one_second() {
        let (base_url, server) = serve_delayed_headers().await;
        let cancel = CancellationToken::new();
        let mut spec = ModelSpec::preset("kimi-k3").expect("preset");
        spec.base_url = base_url;
        let mut events = stream_with_api_key(
            spec,
            empty_context(),
            RequestOptions::default(),
            cancel.clone(),
            Some("test-key".to_owned()),
        );
        assert!(matches!(events.recv().await, Some(ProviderEvent::Start)));
        cancel.cancel();
        assert_aborted_within_one_second(&mut events).await;
        server.abort();
    }

    #[tokio::test]
    async fn cancellation_after_headers_closes_within_one_second() {
        let (base_url, server) = serve_stalled_body(None).await;
        let cancel = CancellationToken::new();
        let mut spec = ModelSpec::preset("kimi-k3").expect("preset");
        spec.base_url = base_url;
        let mut events = stream_with_api_key(
            spec,
            empty_context(),
            RequestOptions::default(),
            cancel.clone(),
            Some("test-key".to_owned()),
        );
        assert!(matches!(events.recv().await, Some(ProviderEvent::Start)));
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancel.cancel();
        assert_aborted_within_one_second(&mut events).await;
        server.abort();
    }

    #[tokio::test]
    async fn cancellation_after_partial_delta_preserves_text_and_fuses() {
        let prefix = "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n";
        let (base_url, server) = serve_stalled_body(Some(prefix)).await;
        let cancel = CancellationToken::new();
        let mut spec = ModelSpec::preset("kimi-k3").expect("preset");
        spec.base_url = base_url;
        let mut events = stream_with_api_key(
            spec,
            empty_context(),
            RequestOptions::default(),
            cancel.clone(),
            Some("test-key".to_owned()),
        );
        let mut consumer = MessageAssembler::new();
        loop {
            let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
                .await
                .expect("partial delta timeout")
                .expect("event");
            consumer.apply(&event).expect("consumer prefix");
            if matches!(event, ProviderEvent::TextDelta { .. }) {
                break;
            }
        }
        cancel.cancel();
        let terminal = assert_aborted_within_one_second(&mut events).await;
        let reconstructed = consumer
            .apply(&terminal)
            .expect("priority snapshot reconciles")
            .expect("terminal message");
        let ProviderEvent::Error { output, .. } = terminal else {
            panic!("aborted error")
        };
        assert_eq!(reconstructed, output.message);
        assert!(matches!(
            output.message.content.as_slice(),
            [types::AssistantContent::Text { text, .. }] if text == "partial"
        ));
        server.abort();
    }

    #[tokio::test]
    async fn cancellation_while_done_waits_for_full_ordered_lane_emits_valid_aborted_once() {
        let spec = ModelSpec::preset("kimi-k3").expect("preset");
        let (tx, rx) = mpsc::channel(1);
        tx.send(ProviderEvent::ReasoningSummaryStart { content_index: 0 })
            .await
            .expect("fill normal lane");
        let (priority_tx, priority_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let committed = Arc::new(SuccessTerminalCommit::new());
        let producer_cancel = cancel.clone();
        let producer_committed = committed.clone();
        let producer_spec = spec.clone();
        let producer = tokio::spawn(async move {
            let mut assembler = MessageAssembler::new();
            assembler.apply(&ProviderEvent::Start).expect("Start");
            finish_terminal(
                &tx,
                &priority_tx,
                &mut assembler,
                &producer_spec,
                ChatTerminal {
                    events: Vec::new(),
                    usage: Usage::default(),
                    stop_reason: StopReason::Stop,
                    error_message: None,
                    provider_code: Some("stop".to_owned()),
                },
                &producer_cancel,
                producer_committed.as_ref(),
            )
            .await;
            assembler
        });

        tokio::task::yield_now().await;
        cancel.cancel();
        let mut producer_assembler = producer.await.expect("producer");
        assert!(
            !committed.is_committed(),
            "Done must not commit without an ordered-lane permit"
        );

        let mut stream = ProviderEventStream::with_priority_terminal(
            rx,
            priority_rx,
            cancel,
            spec.provider.clone(),
            spec.origin(),
            ResponseBudget::default(),
            committed,
        );
        let mut consumer = MessageAssembler::new();
        let start = stream.recv().await.expect("Start");
        consumer.apply(&start).expect("consumer Start");
        let terminal = stream.recv().await.expect("Aborted");
        let ProviderEvent::Error {
            reason: StopReason::Aborted,
            output,
        } = &terminal
        else {
            panic!("pre-commit cancellation must emit Aborted")
        };
        assert_eq!(output.message.stop_reason, StopReason::Aborted);
        assert!(output.message.interrupted);
        assert_eq!(
            consumer
                .apply(&terminal)
                .expect("consumer accepts terminal"),
            Some(output.message.clone())
        );
        assert!(stream.recv().await.is_none(), "terminal must fuse");
        assert_eq!(
            producer_assembler.apply(&terminal),
            Err(assembler::AssemblerError::TerminalAlreadyEmitted),
            "producer committed exactly one well-formed failure terminal"
        );
    }

    #[tokio::test]
    async fn cancellation_before_terminal_event_permit_does_not_commit_unsent_rejection() {
        let spec = ModelSpec::preset("kimi-k3").expect("preset");
        let (tx, rx) = mpsc::channel(1);
        tx.send(ProviderEvent::ToolCallStart { content_index: 0 })
            .await
            .expect("fill ordered lane");
        let (priority_tx, priority_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let committed = Arc::new(SuccessTerminalCommit::new());
        let producer_cancel = cancel.clone();
        let producer_committed = committed.clone();
        let producer_spec = spec.clone();
        let producer = tokio::spawn(async move {
            let mut assembler = MessageAssembler::new();
            assembler.apply(&ProviderEvent::Start).expect("Start");
            assembler
                .apply(&ProviderEvent::ToolCallStart { content_index: 0 })
                .expect("tool start");
            finish_terminal(
                &tx,
                &priority_tx,
                &mut assembler,
                &producer_spec,
                ChatTerminal {
                    events: vec![rejected_tool_event(0)],
                    usage: Usage::default(),
                    stop_reason: StopReason::ToolUse,
                    error_message: None,
                    provider_code: Some("tool_calls".to_owned()),
                },
                &producer_cancel,
                producer_committed.as_ref(),
            )
            .await;
            assembler
        });

        tokio::task::yield_now().await;
        cancel.cancel();
        let mut producer_assembler = producer.await.expect("producer");
        assert!(!committed.is_committed());

        let mut stream = ProviderEventStream::with_priority_terminal(
            rx,
            priority_rx,
            cancel,
            spec.provider.clone(),
            spec.origin(),
            ResponseBudget::default(),
            committed,
        );
        let start = stream.recv().await.expect("Start");
        assert!(matches!(start, ProviderEvent::Start));
        let terminal = stream.recv().await.expect("Aborted");
        let ProviderEvent::Error {
            reason: StopReason::Aborted,
            output,
        } = &terminal
        else {
            panic!("pre-permit cancellation must emit Aborted")
        };
        assert!(
            output.message.content.is_empty(),
            "unsent rejected tool content must not enter the authoritative snapshot"
        );
        assert!(!matches!(terminal, ProviderEvent::ToolCallRejected { .. }));
        assert!(stream.recv().await.is_none(), "terminal must fuse");
        assert_eq!(
            producer_assembler.apply(&terminal),
            Err(assembler::AssemblerError::TerminalAlreadyEmitted),
            "producer accepted exactly one terminal"
        );
    }

    #[tokio::test]
    async fn saturated_lane_cancellation_preserves_kimi_reasoning_closed_at_terminal() {
        let spec = ModelSpec::preset("kimi-k3").expect("preset");
        let (tx, rx) = mpsc::channel(5);
        let partial_events = [
            ProviderEvent::TextStart { content_index: 0 },
            ProviderEvent::TextDelta {
                content_index: 0,
                delta: "visible".to_owned(),
            },
            ProviderEvent::ThinkingStart {
                content_index: 1,
                signature_field: "reasoning_content".to_owned(),
            },
            ProviderEvent::ThinkingDelta {
                content_index: 1,
                delta: "first ".to_owned(),
            },
            ProviderEvent::ThinkingDelta {
                content_index: 1,
                delta: "second".to_owned(),
            },
        ];
        for event in &partial_events {
            tx.send(event.clone()).await.expect("fill ordered lane");
        }
        let (priority_tx, priority_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let committed = Arc::new(SuccessTerminalCommit::new());
        let producer_cancel = cancel.clone();
        let producer_committed = committed.clone();
        let producer_spec = spec.clone();
        let producer = tokio::spawn(async move {
            let mut assembler = MessageAssembler::new();
            assembler.apply(&ProviderEvent::Start).expect("Start");
            for event in &partial_events {
                assembler.apply(event).expect("producer partial event");
            }
            finish_terminal(
                &tx,
                &priority_tx,
                &mut assembler,
                &producer_spec,
                ChatTerminal {
                    events: vec![
                        ProviderEvent::TextEnd {
                            content_index: 0,
                            content: "visible".to_owned(),
                        },
                        ProviderEvent::ThinkingEnd {
                            content_index: 1,
                            content: "first second".to_owned(),
                        },
                    ],
                    usage: Usage::default(),
                    stop_reason: StopReason::Stop,
                    error_message: None,
                    provider_code: Some("stop".to_owned()),
                },
                &producer_cancel,
                producer_committed.as_ref(),
            )
            .await;
        });

        tokio::task::yield_now().await;
        cancel.cancel();
        producer.await.expect("producer");
        assert!(!committed.is_committed());

        let mut stream = ProviderEventStream::with_priority_terminal(
            rx,
            priority_rx,
            cancel,
            spec.provider.clone(),
            spec.origin(),
            ResponseBudget::default(),
            committed,
        );
        assert!(matches!(stream.recv().await, Some(ProviderEvent::Start)));
        let terminal = stream.recv().await.expect("priority Aborted");
        let ProviderEvent::Error {
            reason: StopReason::Aborted,
            output,
        } = terminal
        else {
            panic!("pre-permit cancellation must emit Aborted")
        };
        assert!(matches!(
            output.message.content.as_slice(),
            [
                types::AssistantContent::Text { text, .. },
                types::AssistantContent::Thinking {
                    thinking,
                    signature_field,
                    ..
                }
            ] if text == "visible"
                && thinking == "first second"
                && signature_field == "reasoning_content"
        ));
        assert!(stream.recv().await.is_none(), "terminal must fuse");
    }

    #[tokio::test]
    async fn terminal_rejection_is_committed_once_after_permit_before_done() {
        let spec = ModelSpec::preset("kimi-k3").expect("preset");
        let (tx, mut rx) = mpsc::channel(1);
        let tool_start = ProviderEvent::ToolCallStart { content_index: 0 };
        tx.send(tool_start.clone())
            .await
            .expect("fill ordered lane");
        let (priority_tx, _priority_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let committed = Arc::new(SuccessTerminalCommit::new());
        let producer_committed = committed.clone();
        let producer_spec = spec.clone();
        let rejection = rejected_tool_event(0);
        let producer = tokio::spawn(async move {
            let mut assembler = MessageAssembler::new();
            assembler.apply(&ProviderEvent::Start).expect("Start");
            assembler.apply(&tool_start).expect("tool start");
            finish_terminal(
                &tx,
                &priority_tx,
                &mut assembler,
                &producer_spec,
                ChatTerminal {
                    events: vec![rejection],
                    usage: Usage::default(),
                    stop_reason: StopReason::ToolUse,
                    error_message: None,
                    provider_code: Some("tool_calls".to_owned()),
                },
                &cancel,
                producer_committed.as_ref(),
            )
            .await;
        });

        let mut consumer = MessageAssembler::new();
        consumer.apply(&ProviderEvent::Start).expect("Start");
        let start = rx.recv().await.expect("tool start");
        consumer.apply(&start).expect("consumer tool start");
        let rejection = rx.recv().await.expect("tool rejection");
        let ProviderEvent::ToolCallRejected {
            synthetic_result, ..
        } = &rejection
        else {
            panic!("rejection must precede Done")
        };
        assert_eq!(synthetic_result.tool_call_id, "call-rejected");
        assert!(synthetic_result.is_error);
        consumer.apply(&rejection).expect("consumer tool rejection");
        let done = rx.recv().await.expect("Done");
        let ProviderEvent::Done { output, .. } = &done else {
            panic!("Done must follow rejection")
        };
        assert_eq!(
            consumer.apply(&done).expect("consumer Done"),
            Some(output.message.clone())
        );
        producer.await.expect("producer");
        assert!(committed.is_committed());
        assert!(rx.try_recv().is_err(), "synthetic result is delivered once");
    }

    #[tokio::test]
    async fn cancellation_discards_unread_terminal_tool_and_rejection_events() {
        assert_abort_reconciles_terminal_tool_events(false).await;
    }

    #[tokio::test]
    async fn cancellation_reconciles_already_consumed_terminal_tool_and_rejection_events() {
        assert_abort_reconciles_terminal_tool_events(true).await;
    }

    async fn assert_abort_reconciles_terminal_tool_events(consume_terminal_event: bool) {
        for terminal_event in [validated_tool_event(2), rejected_tool_event(2)] {
            let spec = ModelSpec::preset("kimi-k3").expect("preset");
            let (tx, rx) = mpsc::channel(1);
            let observer_tx = tx.clone();
            let (priority_tx, priority_rx) = mpsc::channel(1);
            let cancel = CancellationToken::new();
            let committed = Arc::new(SuccessTerminalCommit::new());
            let producer_spec = spec.clone();
            let producer_cancel = cancel.clone();
            let producer_committed = committed.clone();
            let prefix = durable_text_thinking_tool_prefix();
            let producer_prefix = prefix.clone();
            let prefix_ready = Arc::new(tokio::sync::Notify::new());
            let producer_ready = prefix_ready.clone();
            let producer = tokio::spawn(async move {
                let mut assembler = MessageAssembler::new();
                assembler.apply(&ProviderEvent::Start).expect("Start");
                for event in producer_prefix {
                    assembler.apply(&event).expect("producer prefix");
                }
                producer_ready.notified().await;
                finish_terminal(
                    &tx,
                    &priority_tx,
                    &mut assembler,
                    &producer_spec,
                    ChatTerminal {
                        events: vec![terminal_event],
                        usage: Usage::default(),
                        stop_reason: StopReason::ToolUse,
                        error_message: None,
                        provider_code: Some("tool_calls".to_owned()),
                    },
                    &producer_cancel,
                    producer_committed.as_ref(),
                )
                .await;
                assembler
            });
            let mut stream = ProviderEventStream::with_priority_terminal(
                rx,
                priority_rx,
                cancel.clone(),
                spec.provider.clone(),
                spec.origin(),
                ResponseBudget::default(),
                committed.clone(),
            );
            let mut consumer = MessageAssembler::new();
            consumer
                .apply(&stream.recv().await.expect("Start"))
                .expect("consumer Start");
            for event in prefix {
                observer_tx
                    .send(event.clone())
                    .await
                    .expect("ordered prefix");
                let received = stream.recv().await.expect("ordered prefix");
                assert_eq!(received, event);
                consumer.apply(&received).expect("consumer prefix");
            }
            prefix_ready.notify_one();
            if consume_terminal_event {
                let event = stream.recv().await.expect("ordered terminal tool event");
                consumer.apply(&event).expect("consumer tool event");
            } else {
                tokio::time::timeout(Duration::from_secs(1), async {
                    while observer_tx.capacity() != 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("terminal event admitted to full ordered lane");
            }
            cancel.cancel();
            let terminal = stream.recv().await.expect("priority Aborted");
            let producer_assembler = producer.await.expect("producer");
            let ProviderEvent::Error {
                reason: StopReason::Aborted,
                output,
            } = &terminal
            else {
                panic!("pre-success cancellation must abort")
            };
            assert_eq!(output.message.content, retained_text_and_thinking());
            assert!(
                consumer
                    .apply(&terminal)
                    .expect("abort reconciliation")
                    .is_some()
            );
            assert_eq!(consumer.completed_content(), retained_text_and_thinking());
            assert!(consumer.synthetic_results().is_empty());
            assert!(producer_assembler.synthetic_results().is_empty());
            assert!(!committed.is_committed());
            assert!(stream.recv().await.is_none());
        }
    }

    #[tokio::test]
    async fn terminal_text_and_tool_events_keep_order_before_done() {
        let spec = ModelSpec::preset("kimi-k3").expect("preset");
        let (tx, mut rx) = mpsc::channel(3);
        let (priority_tx, _priority_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let committed = SuccessTerminalCommit::new();
        let text_start = ProviderEvent::TextStart { content_index: 0 };
        let text_delta = ProviderEvent::TextDelta {
            content_index: 0,
            delta: "answer".to_owned(),
        };
        let tool_start = ProviderEvent::ToolCallStart { content_index: 1 };
        let tool_delta = ProviderEvent::ToolCallDelta {
            content_index: 1,
            delta: r#"{"path":"a.txt"}"#.to_owned(),
        };
        let mut arguments = serde_json::Map::new();
        arguments.insert("path".to_owned(), serde_json::json!("a.txt"));
        let mut assembler = MessageAssembler::new();
        for event in [
            ProviderEvent::Start,
            text_start.clone(),
            text_delta.clone(),
            tool_start.clone(),
            tool_delta.clone(),
        ] {
            assembler.apply(&event).expect("producer prefix");
        }

        finish_terminal(
            &tx,
            &priority_tx,
            &mut assembler,
            &spec,
            ChatTerminal {
                events: vec![
                    ProviderEvent::TextEnd {
                        content_index: 0,
                        content: "answer".to_owned(),
                    },
                    ProviderEvent::ToolCallEnd {
                        content_index: 1,
                        tool_call: ToolCall {
                            id: "call-valid".to_owned(),
                            name: "read_file".to_owned(),
                            route: crate::provider::types::ToolInvocationRoute::Normal,
                            arguments: ValidatedToolArguments::from_schema_validated(arguments),
                        },
                    },
                ],
                usage: Usage::default(),
                stop_reason: StopReason::ToolUse,
                error_message: None,
                provider_code: Some("tool_calls".to_owned()),
            },
            &cancel,
            &committed,
        )
        .await;

        let events = [
            rx.recv().await.expect("text end"),
            rx.recv().await.expect("tool end"),
            rx.recv().await.expect("Done"),
        ];
        assert_eq!(event_types(&events), ["text_end", "tool_call_end", "done"]);
        let mut consumer = MessageAssembler::new();
        for event in [
            ProviderEvent::Start,
            text_start,
            text_delta,
            tool_start,
            tool_delta,
        ] {
            consumer.apply(&event).expect("consumer prefix");
        }
        for event in &events {
            consumer.apply(event).expect("consumer terminal sequence");
        }
        assert!(committed.is_committed());
    }

    fn rejected_tool_event(content_index: usize) -> ProviderEvent {
        ProviderEvent::ToolCallRejected {
            content_index,
            rejected: RejectedToolCall {
                id: "call-rejected".to_owned(),
                name: "read_file".to_owned(),
                error: ToolArgumentError::InvalidJson,
            },
            synthetic_result: ToolResultMessage {
                tool_call_id: "call-rejected".to_owned(),
                tool_name: "read_file".to_owned(),
                content: vec![UserContent::Text {
                    text: "Tool arguments were rejected. Regenerate the tool call with complete, schema-valid arguments."
                        .to_owned(),
                }],
                details: serde_json::json!({
                    "category": "invalid_json",
                    "instance_path": "",
                    "constraint": "json_syntax"
                }),
                is_error: true,
                timestamp: Utc::now(),
            },
        }
    }

    fn validated_tool_event(content_index: usize) -> ProviderEvent {
        ProviderEvent::ToolCallEnd {
            content_index,
            tool_call: ToolCall {
                id: "call-valid".to_owned(),
                name: "read_file".to_owned(),
                route: crate::provider::types::ToolInvocationRoute::Normal,
                arguments: ValidatedToolArguments::from_schema_validated(
                    serde_json::json!({"path": "a.txt"})
                        .as_object()
                        .expect("object")
                        .clone(),
                ),
            },
        }
    }

    fn durable_text_thinking_tool_prefix() -> Vec<ProviderEvent> {
        vec![
            ProviderEvent::TextStart { content_index: 0 },
            ProviderEvent::TextDelta {
                content_index: 0,
                delta: "kept text".to_owned(),
            },
            ProviderEvent::TextEnd {
                content_index: 0,
                content: "kept text".to_owned(),
            },
            ProviderEvent::ThinkingStart {
                content_index: 1,
                signature_field: "reasoning_content".to_owned(),
            },
            ProviderEvent::ThinkingDelta {
                content_index: 1,
                delta: "kept thinking".to_owned(),
            },
            ProviderEvent::ThinkingEnd {
                content_index: 1,
                content: "kept thinking".to_owned(),
            },
            ProviderEvent::ToolCallStart { content_index: 2 },
        ]
    }

    fn retained_text_and_thinking() -> Vec<types::AssistantContent> {
        vec![
            types::AssistantContent::Text {
                text: "kept text".to_owned(),
                wire_item_index: 0,
            },
            types::AssistantContent::Thinking {
                thinking: "kept thinking".to_owned(),
                signature_field: "reasoning_content".to_owned(),
                wire_item_index: 1,
            },
        ]
    }

    #[tokio::test]
    async fn saturated_queue_cancellation_preempts_backlog_with_partial_and_usage() {
        let mut prefix =
            "data: {\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":4},\"choices\":[]}\n\n"
                .to_owned();
        for _ in 0..100 {
            prefix.push_str(
                "data: {\"choices\":[{\"delta\":{\"content\":\"x\"},\"finish_reason\":null}]}\n\n",
            );
        }
        let (base_url, server) = serve_stalled_owned_body(prefix).await;
        let cancel = CancellationToken::new();
        let mut spec = ModelSpec::preset("kimi-k3").expect("preset");
        spec.base_url = base_url;
        let mut events = stream_with_api_key(
            spec,
            empty_context(),
            RequestOptions::default(),
            cancel.clone(),
            Some("test-key".to_owned()),
        );
        let start = events.recv().await.expect("Start");
        assert!(matches!(start, ProviderEvent::Start));
        let mut consumer = MessageAssembler::new();
        consumer.apply(&start).expect("consumer Start");

        tokio::time::sleep(Duration::from_millis(50)).await;
        let started = std::time::Instant::now();
        cancel.cancel();
        let terminal = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .expect("saturated cancellation exceeded one second")
            .expect("priority terminal");
        assert!(started.elapsed() < Duration::from_secs(1));
        let ProviderEvent::Error {
            reason: StopReason::Aborted,
            ref output,
        } = terminal
        else {
            panic!("expected aborted terminal")
        };
        assert_eq!(output.message.usage.input, 9);
        assert_eq!(output.message.usage.output, 4);
        assert!(matches!(
            output.message.content.as_slice(),
            [types::AssistantContent::Text { text, .. }] if !text.is_empty()
        ));
        assert_eq!(
            consumer
                .apply(&terminal)
                .expect("consumer accepts authoritative backlog snapshot"),
            Some(output.message.clone())
        );
        assert!(
            events.recv().await.is_none(),
            "no delta may follow terminal"
        );
        server.abort();
    }

    #[tokio::test]
    async fn initial_priority_error_is_start_prefixed_and_consumer_reconstructable() {
        let spec = ModelSpec::preset("opencode-go").expect("preset");
        let mut events = stream_with_api_key(
            spec,
            empty_context(),
            RequestOptions::default(),
            CancellationToken::new(),
            None,
        );
        let mut consumer = MessageAssembler::new();
        let start = events.recv().await.expect("Start");
        assert!(matches!(start, ProviderEvent::Start));
        consumer.apply(&start).expect("consumer Start");
        let terminal = events.recv().await.expect("priority Error");
        let ProviderEvent::Error { output, .. } = &terminal else {
            panic!("expected initial Error")
        };
        assert_eq!(
            output.message.provider_code.as_deref(),
            Some("missing_api_key")
        );
        assert_eq!(
            consumer.apply(&terminal).expect("consumer terminal"),
            Some(output.message.clone())
        );
        assert!(events.recv().await.is_none());
    }

    #[tokio::test]
    async fn usage_trailer_survives_provider_error_and_finish_validation_failure() {
        const PROVIDER_ERROR: &str = concat!(
            "data: {\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3},\"choices\":[]}\n\n",
            "data: {\"error\":{\"code\":\"network_error\",\"message\":\"failed\"},\"choices\":[]}\n\n"
        );
        let provider_events = replay("kimi-k3", PROVIDER_ERROR).await;
        let Some(ProviderEvent::Error { output, .. }) = provider_events.last() else {
            panic!("provider error terminal")
        };
        assert_eq!(output.message.usage.input, 7);
        assert_eq!(output.message.usage.output, 3);

        const INVALID_FINISH: &str = concat!(
            "data: {\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":5},",
            "\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n"
        );
        let finish_events = replay("kimi-k3", INVALID_FINISH).await;
        let Some(ProviderEvent::Error { output, .. }) = finish_events.last() else {
            panic!("finish validation error terminal")
        };
        assert_eq!(output.message.usage.input, 11);
        assert_eq!(output.message.usage.output, 5);
    }

    #[tokio::test]
    #[ignore = "post-deadline provider-qualification debt; OpenCode Go is confirmed unavailable"]
    async fn live_opencode_go_two_turn_tool_reasoning_gate() {
        run_live_chat_tool_roundtrip("opencode-go").await;
    }

    #[tokio::test]
    #[ignore = "release-blocking missing direct-provider evidence; Moonshot proof not completed or substituted"]
    async fn live_kimi_k3_direct_two_turn_tool_reasoning_gate() {
        run_live_chat_tool_roundtrip("kimi-k3").await;
    }

    #[tokio::test]
    #[ignore = "release-blocking missing direct-provider evidence; Z.ai proof not completed or substituted"]
    async fn live_glm_5_2_direct_two_turn_tool_reasoning_gate() {
        run_live_chat_tool_roundtrip("glm-5.2").await;
    }

    #[tokio::test]
    #[ignore = "release-blocking missing direct-provider evidence; Umans proof not completed or substituted"]
    async fn live_umans_direct_two_turn_tool_reasoning_gate() {
        run_live_chat_tool_roundtrip("umans").await;
    }

    #[tokio::test]
    #[ignore = "post-deadline provider-qualification debt; OpenCode Go is confirmed unavailable"]
    async fn live_opencode_go_provider_release_gate() {
        if env::var("SUMI_LIVE_TEST").as_deref() != Ok("1") {
            return;
        }
        run_live_chat_tool_roundtrip("opencode-go").await;
    }

    /// T25 release gate: OpenAI Responses through the local development-only
    /// Codex OAuth bridge. `SUMI_LIVE_TEST=1` selects this non-ignored gate.
    /// Missing or empty `SUMI_CODEX_RESPONSES_BASE_URL` or
    /// `SUMI_CODEX_RESPONSES_PROXY_SECRET` fails before any network call.
    #[tokio::test]
    async fn live_codex_responses_provider_release_gate() {
        // `provider` is also compiled into the doctest-only library target,
        // which has no agent runtime. The identically named binary-target test
        // below is the sole release gate and owns the canonical Session.
        if !crate::canonical_live_responses_harness_available() {
            if env::var("SUMI_LIVE_TEST").as_deref() == Ok("1") {
                eprintln!(
                    "skipping duplicate doctest-library target; the sumi-agent binary target owns the live Responses release gate"
                );
            }
            return;
        }
        if env::var("SUMI_LIVE_TEST").as_deref() != Ok("1") {
            return;
        }
        run_live_codex_responses_bridge().await;
    }

    fn run_codex_responses_release_dispatcher(
        base_url: Option<&str>,
        proxy_secret: Option<&str>,
    ) -> Output {
        let mut command = Command::new(env::current_exe().expect("current test executable"));
        command
            .args([
                "--exact",
                "provider::tests::live_codex_responses_provider_release_gate",
                "--nocapture",
            ])
            .env("SUMI_LIVE_TEST", "1")
            .env_remove("SUMI_ENV_FILE")
            .env_remove("SUMI_CODEX_RESPONSES_BASE_URL")
            .env_remove("SUMI_CODEX_RESPONSES_PROXY_SECRET")
            .env_remove("SUMI_CODEX_RESPONSES_MODEL")
            .env_remove("OPENAI_API_KEY")
            .env_remove("OPENCODE_GO_API_KEY")
            .env_remove("MOONSHOT_API_KEY")
            .env_remove("ZAI_API_KEY")
            .env_remove("UMANS_API_KEY");
        if let Some(base_url) = base_url {
            command.env("SUMI_CODEX_RESPONSES_BASE_URL", base_url);
        }
        if let Some(proxy_secret) = proxy_secret {
            command.env("SUMI_CODEX_RESPONSES_PROXY_SECRET", proxy_secret);
        }
        command
            .output()
            .expect("run isolated live release dispatcher")
    }

    fn assert_codex_responses_config_failure(
        base_url: Option<&str>,
        proxy_secret: Option<&str>,
        expected: &str,
    ) {
        let output = run_codex_responses_release_dispatcher(base_url, proxy_secret);
        assert!(
            !output.status.success(),
            "invalid live release dispatcher configuration must not report green"
        );
        let diagnostics = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            diagnostics.contains(expected),
            "unexpected dispatcher failure: {diagnostics}"
        );
    }

    #[test]
    fn live_codex_responses_release_opt_in_without_bridge_url_fails_before_network() {
        if !crate::canonical_live_responses_harness_available() {
            return;
        }
        assert_codex_responses_config_failure(
            None,
            None,
            "live Codex Responses release gate requires SUMI_CODEX_RESPONSES_BASE_URL",
        );
    }

    #[test]
    fn live_codex_responses_release_opt_in_with_empty_bridge_url_fails_before_network() {
        if !crate::canonical_live_responses_harness_available() {
            return;
        }
        assert_codex_responses_config_failure(
            Some(""),
            Some("unused-test-secret"),
            "live Codex Responses release gate requires non-empty SUMI_CODEX_RESPONSES_BASE_URL",
        );
    }

    #[test]
    fn live_codex_responses_release_opt_in_without_proxy_secret_fails_before_network() {
        if !crate::canonical_live_responses_harness_available() {
            return;
        }
        assert_codex_responses_config_failure(
            Some("http://127.0.0.1:1"),
            None,
            "live Codex Responses release gate requires SUMI_CODEX_RESPONSES_PROXY_SECRET",
        );
    }

    #[test]
    fn live_codex_responses_release_opt_in_with_empty_proxy_secret_fails_before_network() {
        if !crate::canonical_live_responses_harness_available() {
            return;
        }
        assert_codex_responses_config_failure(
            Some("http://127.0.0.1:1"),
            Some(""),
            "live Codex Responses release gate requires non-empty SUMI_CODEX_RESPONSES_PROXY_SECRET",
        );
    }

    #[test]
    fn codex_responses_proxy_self_test_passes() {
        let proxy = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../scripts/dev/codex-responses-proxy.py");
        let output = Command::new("python3")
            .arg(&proxy)
            .arg("--self-test")
            .output()
            .expect("spawn proxy self-test");
        let diagnostics = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.status.success(),
            "Codex Responses proxy self-test failed:\n{diagnostics}"
        );
    }

    async fn run_live_chat_tool_roundtrip(preset: &str) {
        if let Some(path) = env::var_os("SUMI_ENV_FILE") {
            dotenvy::from_path(path).expect("load SUMI_ENV_FILE for live test");
        }

        let spec = ModelSpec::preset(preset).unwrap_or_else(|| panic!("unknown preset {preset}"));
        let api_key = env::var(&spec.api_key_env)
            .unwrap_or_else(|_| panic!("{preset} live gate requires {}", spec.api_key_env));
        assert!(
            !api_key.trim().is_empty(),
            "{preset} live gate requires non-empty {}",
            spec.api_key_env
        );

        let tool = ToolDefinition {
            name: "echo_value".to_owned(),
            description: "Return the supplied value unchanged.".to_owned(),
            parameters: serde_json::json!({
                "type":"object",
                "properties":{"value":{"type":"string"}},
                "required":["value"],
                "additionalProperties":false
            }),
        };
        let user = types::UserMessage {
            content: vec![types::UserContent::Text {
                text: "Call echo_value once with value live-smoke-ok.".to_owned(),
            }],
            timestamp: Utc::now(),
        };
        let first_context = PromptContext {
            system_prompt: "Use the requested tool exactly once.".to_owned(),
            memory_blocks: vec![],
            messages: vec![types::ContextMessage::Synthetic {
                message: types::Message::User(user.clone()),
            }],
            provider_context: vec![],
            tools: vec![tool.clone()],
            replay_provenance: None,
        };
        let first = run_live_request(
            spec.clone(),
            first_context,
            RequestOptions {
                max_tokens: Some(4_096),
                ..RequestOptions::default()
            },
            api_key.clone(),
        )
        .await;
        assert_eq!(first.stop_reason, StopReason::ToolUse, "{preset}");
        let calls: Vec<_> = first
            .content
            .iter()
            .filter_map(|content| match content {
                types::AssistantContent::ToolCall { tool_call, .. } => Some(tool_call),
                _ => None,
            })
            .collect();
        assert_eq!(calls.len(), 1, "{preset}");
        assert_eq!(
            calls[0].arguments.as_object().get("value"),
            Some(&serde_json::json!("live-smoke-ok")),
            "{preset}"
        );
        assert!(
            first
                .content
                .iter()
                .any(|content| matches!(content, types::AssistantContent::Thinking { .. })),
            "{preset} live gate did not expose replayable reasoning"
        );

        let second_context = PromptContext {
            system_prompt: "Use the requested tool exactly once.".to_owned(),
            memory_blocks: vec![],
            messages: vec![
                types::ContextMessage::Synthetic {
                    message: types::Message::User(user),
                },
                types::ContextMessage::Synthetic {
                    message: types::Message::Assistant(first.clone()),
                },
                types::ContextMessage::Synthetic {
                    message: types::Message::ToolResult(types::ToolResultMessage {
                        tool_call_id: calls[0].id.clone(),
                        tool_name: calls[0].name.clone(),
                        content: vec![types::UserContent::Text {
                            text: "live-smoke-ok".to_owned(),
                        }],
                        details: serde_json::json!({}),
                        is_error: false,
                        timestamp: Utc::now(),
                    }),
                },
            ],
            provider_context: vec![],
            tools: vec![tool],
            replay_provenance: None,
        };
        let second = run_live_request(
            spec,
            second_context,
            RequestOptions {
                max_tokens: Some(4_096),
                ..RequestOptions::default()
            },
            api_key,
        )
        .await;
        assert!(
            matches!(second.stop_reason, StopReason::Stop | StopReason::Length),
            "{preset}: {:?}",
            second.error_message
        );
        assert!(
            second.content.iter().any(|content| matches!(
                content,
                types::AssistantContent::Text { text, .. } if !text.is_empty()
            )),
            "{preset}"
        );
    }

    async fn run_live_codex_responses_bridge() {
        if let Some(path) = env::var_os("SUMI_ENV_FILE") {
            dotenvy::from_path(path).expect("load SUMI_ENV_FILE for live test");
        }

        let base_url = env::var("SUMI_CODEX_RESPONSES_BASE_URL").unwrap_or_else(|_| {
            panic!("live Codex Responses release gate requires SUMI_CODEX_RESPONSES_BASE_URL")
        });
        assert!(
            !base_url.trim().is_empty(),
            "live Codex Responses release gate requires non-empty SUMI_CODEX_RESPONSES_BASE_URL"
        );
        let proxy_secret = env::var("SUMI_CODEX_RESPONSES_PROXY_SECRET").unwrap_or_else(|_| {
            panic!("live Codex Responses release gate requires SUMI_CODEX_RESPONSES_PROXY_SECRET")
        });
        assert!(
            !proxy_secret.trim().is_empty(),
            "live Codex Responses release gate requires non-empty SUMI_CODEX_RESPONSES_PROXY_SECRET"
        );

        let ten_turn_stress = match env::var("SUMI_LIVE_TEST_TURNS") {
            Err(env::VarError::NotPresent) => false,
            Ok(value) if value == "10" => true,
            Ok(value) => panic!(
                "SUMI_LIVE_TEST_TURNS must be unset (the two-turn release gate) or exactly 10, got {value:?}"
            ),
            Err(env::VarError::NotUnicode(_)) => {
                panic!("SUMI_LIVE_TEST_TURNS must be unset or UTF-8 value exactly 10")
            }
        };
        let mut spec = ModelSpec::preset("openai-responses").expect("Responses preset");
        spec.id = match env::var("SUMI_CODEX_RESPONSES_MODEL") {
            Ok(model)
                if !model.trim().is_empty() && (!ten_turn_stress || model == "gpt-5.6-terra") =>
            {
                model
            }
            Ok(model) if ten_turn_stress => panic!(
                "SUMI_LIVE_TEST_TURNS=10 requires SUMI_CODEX_RESPONSES_MODEL=gpt-5.6-terra, got {model:?}"
            ),
            Ok(_) => panic!(
                "live Codex Responses release gate requires a non-empty SUMI_CODEX_RESPONSES_MODEL when set"
            ),
            Err(_) if ten_turn_stress => "gpt-5.6-terra".to_owned(),
            // The release gate must exercise encrypted reasoning provider context.
            // gpt-5.6-luna is the cost-optimized tier and adaptively emits zero
            // reasoning tokens for simple tool-use turns, so the canonical first
            // turn produces no provider context. gpt-5.6-sol is the frontier
            // reasoning model and reliably emits reasoning items here.
            Err(_) => "gpt-5.6-sol".to_owned(),
        };
        spec.base_url = base_url;
        spec.api_key_env = "SUMI_CODEX_RESPONSES_PROXY_SECRET".to_owned();
        run_live_responses_tool_roundtrip(spec, proxy_secret).await;
    }

    async fn run_live_responses_tool_roundtrip(spec: ModelSpec, api_key: String) {
        crate::run_canonical_live_responses_roundtrip(spec, api_key).await;
    }
    async fn run_live_request(
        spec: ModelSpec,
        context: PromptContext,
        options: RequestOptions,
        api_key: String,
    ) -> AssistantMessage {
        run_live_output(spec, context, options, api_key)
            .await
            .message
    }

    async fn run_live_output(
        spec: ModelSpec,
        context: PromptContext,
        options: RequestOptions,
        api_key: String,
    ) -> types::ProviderOutput {
        let mut events = stream_with_api_key(
            spec,
            context,
            options,
            CancellationToken::new(),
            Some(api_key),
        );
        tokio::time::timeout(Duration::from_secs(180), async {
            while let Some(event) = events.recv().await {
                match event {
                    ProviderEvent::Done { output, .. } => return output,
                    ProviderEvent::Error { output, .. } => {
                        panic!(
                            "live provider error {}: {}",
                            output.message.provider_code.as_deref().unwrap_or("unknown"),
                            output.message.error_message.as_deref().unwrap_or("unknown")
                        )
                    }
                    _ => {}
                }
            }
            panic!("live provider stream closed without terminal event")
        })
        .await
        .expect("live provider request timed out")
    }

    #[tokio::test]
    async fn responses_dispatch_normalizes_fixture_and_preserves_context() {
        let fixture = include_str!("../../tests/fixtures/openai_responses_official.sse");
        let app = Router::new().route(
            "/responses",
            post(move || async move {
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(Body::from(fixture))
                    .expect("response")
            }),
        );
        let (base_url, server) = serve_router(app).await;
        let mut spec = ModelSpec::preset("openai-responses").expect("Responses preset");
        spec.base_url = base_url;
        let context = PromptContext {
            system_prompt: "constitution".into(),
            memory_blocks: vec![],
            messages: vec![],
            provider_context: vec![],
            tools: vec![ToolDefinition {
                name: "weather".into(),
                description: "Weather".into(),
                parameters: serde_json::json!({
                    "type":"object",
                    "properties":{"city":{"type":"string"}},
                    "required":["city"],
                    "additionalProperties":false
                }),
            }],
            replay_provenance: None,
        };
        let mut stream = stream_with_api_key(
            spec,
            context,
            RequestOptions::default(),
            CancellationToken::new(),
            Some("test-key".into()),
        );
        let mut events = Vec::new();
        while let Some(event) = stream.recv().await {
            events.push(event);
        }
        server.abort();
        assert!(matches!(events.first(), Some(ProviderEvent::Start)));
        let terminal = events.last().expect("terminal");
        let ProviderEvent::Done {
            reason: StopReason::ToolUse,
            output,
        } = terminal
        else {
            panic!("unexpected terminal: {terminal:?}");
        };
        assert_eq!(output.provider_context.len(), 1);
        assert_eq!(output.message.usage.cache_read, 5);
        assert!(output.message.content.iter().any(
            |content| matches!(content, AssistantContent::ToolCall { tool_call, .. } if tool_call.name == "weather")
        ));
    }

    const RESPONSES_PARTIAL_TOOL_PREFIX: &str = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp-partial\",\"model\":\"gpt-5.6\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
        "event: response.output_item.added\n",
        "data: {\"type\":\"response.output_item.added\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"fc-partial\",\"type\":\"function_call\",\"call_id\":\"call-partial\",\"name\":\"weather\",\"arguments\":\"\"}}\n\n"
    );

    fn partial_responses_tool_context() -> PromptContext {
        PromptContext {
            tools: vec![ToolDefinition {
                name: "weather".to_owned(),
                description: "Weather".to_owned(),
                parameters: serde_json::json!({
                    "type": "object",
                    "properties": {"city": {"type": "string"}},
                    "required": ["city"],
                    "additionalProperties": false
                }),
            }],
            ..empty_context()
        }
    }

    fn assert_responses_partial_rejection_before_failure(
        events: &[ProviderEvent],
        expected_provider_code: &str,
    ) {
        assert_eq!(
            event_types(events),
            ["start", "tool_call_start", "tool_call_rejected", "error"],
            "the consumer must receive the synthetic result before terminal validation"
        );
        let [
            ProviderEvent::Start,
            ProviderEvent::ToolCallStart { content_index: 0 },
            ProviderEvent::ToolCallRejected {
                content_index: 0,
                rejected,
                synthetic_result,
            },
            ProviderEvent::Error {
                reason: StopReason::Error,
                output,
            },
        ] = events
        else {
            panic!("unexpected partial Responses failure sequence: {events:?}");
        };
        assert_eq!(rejected.id, "call-partial");
        assert_eq!(rejected.name, "weather");
        assert_eq!(synthetic_result.tool_call_id, rejected.id);
        assert_eq!(synthetic_result.tool_name, rejected.name);
        assert!(synthetic_result.is_error);
        assert_eq!(output.message.stop_reason, StopReason::Error);
        assert!(!output.message.interrupted);
        assert_eq!(
            output.message.provider_code.as_deref(),
            Some(expected_provider_code)
        );
        assert!(matches!(
            output.message.content.as_slice(),
            [AssistantContent::RejectedToolCall { rejected: terminal_rejected, .. }]
                if terminal_rejected == rejected
        ));
        assert_eq!(
            reconstruct_terminal(events),
            output.message.clone(),
            "the consumer assembler must accept the emitted rejection/result before the authoritative terminal"
        );
    }

    enum PartialResponsesFailure {
        MalformedInput,
        Eof,
        Transport,
    }

    async fn partial_responses_failure_events(
        failure: PartialResponsesFailure,
    ) -> Vec<ProviderEvent> {
        let spec = ModelSpec::preset("openai-responses").expect("Responses preset");
        let schemas = FrozenToolSchemaRegistry::compile(&partial_responses_tool_context().tools)
            .expect("tool schema");
        let mut receive = ResponsesReceiveState::with_budget(schemas, ResponseBudget::default());
        let mut assembler = MessageAssembler::new();
        assembler
            .apply(&ProviderEvent::Start)
            .expect("producer Start");
        let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let (priority_tx, priority_rx) = mpsc::channel(1);
        let (ordered_prefix_drain_tx, ordered_prefix_drain_rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let committed = Arc::new(SuccessTerminalCommit::new());

        let pushed = receive
            .push_json(
                r#"{"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"id":"fc-partial","type":"function_call","call_id":"call-partial","name":"weather","arguments":""}}"#,
            )
            .expect("partial function call");
        assert_eq!(pushed.events.len(), 1);
        assert!(matches!(
            emit(
                &tx,
                &mut assembler,
                pushed.events.into_iter().next().expect("tool start"),
                &cancel
            )
            .await,
            EmitResult::Sent
        ));
        mark_responses_partial_rejection_prefix(
            &Some(ordered_prefix_drain_tx),
            close_responses_partial(&tx, &mut receive, &mut assembler, &cancel).await,
        );

        match failure {
            PartialResponsesFailure::MalformedInput => {
                let error = receive
                    .push_json("fixture malformed input")
                    .expect_err("malformed input must fail Responses normalization");
                finish_responses_error(
                    &priority_tx,
                    &mut assembler,
                    &spec,
                    receive.usage().clone(),
                    error,
                    false,
                    receive.provider_context(),
                )
                .await;
            }
            PartialResponsesFailure::Eof => {
                let error = receive
                    .finish_eof()
                    .expect_err("unfinished Responses output must reject EOF");
                finish_responses_error(
                    &priority_tx,
                    &mut assembler,
                    &spec,
                    receive.usage().clone(),
                    error,
                    false,
                    receive.provider_context(),
                )
                .await;
            }
            PartialResponsesFailure::Transport => {
                let error = SseError::Transport("fixture transport failure".to_owned());
                finish_failure_with_context(
                    &priority_tx,
                    &mut assembler,
                    &spec,
                    receive.usage().clone(),
                    error.to_string(),
                    &transport_error_code(&error),
                    false,
                    receive.provider_context(),
                )
                .await;
            }
        }

        let mut stream = ProviderEventStream::with_priority_terminal(
            rx,
            priority_rx,
            cancel,
            spec.provider.clone(),
            spec.origin(),
            ResponseBudget::default(),
            committed,
        )
        .with_ordered_prefix_drain(ordered_prefix_drain_rx);
        let mut events = Vec::new();
        while let Some(event) = stream.recv().await {
            events.push(event);
        }
        events
    }

    #[tokio::test]
    async fn responses_partial_tool_rejection_reaches_consumer_before_failure_terminal() {
        assert_responses_partial_rejection_before_failure(
            &partial_responses_failure_events(PartialResponsesFailure::MalformedInput).await,
            "invalid_provider_stream",
        );
        assert_responses_partial_rejection_before_failure(
            &partial_responses_failure_events(PartialResponsesFailure::Eof).await,
            "stream_ended_without_terminal_event",
        );
        assert_responses_partial_rejection_before_failure(
            &partial_responses_failure_events(PartialResponsesFailure::Transport).await,
            "transport_error",
        );
    }

    #[tokio::test]
    async fn ordinary_priority_error_does_not_drain_normal_backlog() {
        let spec = ModelSpec::preset("kimi-k3").expect("preset");
        let (tx, rx) = mpsc::channel(1);
        tx.send(ProviderEvent::ToolCallStart { content_index: 0 })
            .await
            .expect("queue ordinary normal event");
        let (priority_tx, priority_rx) = mpsc::channel(1);
        let mut assembler = MessageAssembler::new();
        assembler
            .apply(&ProviderEvent::Start)
            .expect("producer Start");
        finish_failure(
            &priority_tx,
            &mut assembler,
            &spec,
            Usage::default(),
            "fixture ordinary failure".to_owned(),
            "ordinary_failure",
            false,
        )
        .await;

        let mut stream = ProviderEventStream::with_priority_terminal(
            rx,
            priority_rx,
            CancellationToken::new(),
            spec.provider.clone(),
            spec.origin(),
            ResponseBudget::default(),
            Arc::new(SuccessTerminalCommit::new()),
        );
        let mut events = Vec::new();
        while let Some(event) = stream.recv().await {
            events.push(event);
        }
        assert_eq!(event_types(&events), ["start", "error"]);
        assert!(matches!(
            events.last(),
            Some(ProviderEvent::Error { output, .. })
                if output.message.provider_code.as_deref() == Some("ordinary_failure")
        ));
    }

    #[tokio::test]
    async fn responses_partial_tool_cancellation_keeps_priority_over_unsent_rejection() {
        let app = Router::new().route(
            "/responses",
            post(|| async {
                let prefix = stream::once(async {
                    Ok::<String, Infallible>(RESPONSES_PARTIAL_TOOL_PREFIX.to_owned())
                });
                let stalled = stream::pending::<Result<String, Infallible>>();
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(Body::from_stream(prefix.chain(stalled)))
                    .expect("response")
            }),
        );
        let (base_url, server) = serve_router(app).await;
        let mut spec = ModelSpec::preset("openai-responses").expect("Responses preset");
        spec.base_url = base_url;
        let cancel = CancellationToken::new();
        let mut stream = stream_with_api_key(
            spec,
            partial_responses_tool_context(),
            RequestOptions::default(),
            cancel.clone(),
            Some("test-key".to_owned()),
        );
        assert!(matches!(stream.recv().await, Some(ProviderEvent::Start)));
        assert!(matches!(
            stream.recv().await,
            Some(ProviderEvent::ToolCallStart { content_index: 0 })
        ));
        cancel.cancel();
        let terminal = tokio::time::timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("Responses cancellation must retain priority")
            .expect("priority terminal");
        let ProviderEvent::Error {
            reason: StopReason::Aborted,
            output,
        } = terminal
        else {
            panic!("cancellation must emit an Aborted terminal");
        };
        assert!(output.message.interrupted);
        assert!(output.message.content.is_empty());
        assert!(
            stream.recv().await.is_none(),
            "terminal must fuse the stream"
        );
        server.abort();
    }

    #[tokio::test]
    async fn responses_cancel_and_eof_terminals_preserve_only_item_done_reasoning() {
        let fixture = include_str!("../../tests/fixtures/openai_responses_official.sse");
        let prefix = fixture
            .lines()
            .take_while(|line| !line.contains("sequence_number\":13"))
            .collect::<Vec<_>>()
            .join("\n");
        for cancel_after_prefix in [false, true] {
            let body = prefix.clone();
            let app = Router::new().route(
                "/responses",
                post(move || {
                    let body = body.clone();
                    async move {
                        let prefix = stream::iter([Ok::<String, Infallible>(body)]);
                        let response_body = if cancel_after_prefix {
                            Body::from_stream(
                                prefix.chain(stream::pending::<Result<String, Infallible>>()),
                            )
                        } else {
                            Body::from_stream(
                                prefix.chain(stream::empty::<Result<String, Infallible>>()),
                            )
                        };
                        Response::builder()
                            .status(StatusCode::OK)
                            .header("content-type", "text/event-stream")
                            .body(response_body)
                            .expect("response")
                    }
                }),
            );
            let (base_url, server) = serve_router(app).await;
            let mut spec = ModelSpec::preset("openai-responses").unwrap();
            spec.base_url = base_url;
            let cancel = CancellationToken::new();
            let mut events = stream_with_api_key(
                spec,
                empty_context(),
                RequestOptions::default(),
                cancel.clone(),
                Some("test-key".into()),
            );
            let mut terminal = None;
            while let Some(event) = events.recv().await {
                if cancel_after_prefix && matches!(event, ProviderEvent::ReasoningSummaryEnd { .. })
                {
                    cancel.cancel();
                }
                if matches!(
                    event,
                    ProviderEvent::Done { .. } | ProviderEvent::Error { .. }
                ) {
                    terminal = Some(event);
                    break;
                }
            }
            server.abort();
            let ProviderEvent::Error { reason, output } = terminal.expect("error terminal") else {
                panic!("unexpected terminal")
            };
            assert_eq!(
                reason,
                if cancel_after_prefix {
                    StopReason::Aborted
                } else {
                    StopReason::Error
                }
            );
            assert_eq!(output.provider_context.len(), 1);
            assert!(matches!(
                &output.provider_context[0].payload,
                types::ProviderContextPayload::EncryptedReasoning { item, .. }
                    if item["id"] == "rs_fixture"
                        && item["encrypted_content"] == "opaque-reasoning"
            ));
        }
    }

    #[tokio::test]
    async fn compact_native_rejects_unprovable_coverage_before_credentials() {
        let spec = ModelSpec::preset("openai-responses").expect("Responses preset");
        let error = compact_native(spec, empty_context(), CancellationToken::new())
            .await
            .expect_err("empty history cannot prove persisted compaction coverage");
        assert!(matches!(
            error,
            NativeCompactionError::InvalidRequest(ref message)
                if message.contains("persisted")
        ));
    }

    #[tokio::test]
    async fn anthropic_failure_terminal_preserves_verified_reasoning_context() {
        let spec = ModelSpec::preset("anthropic").expect("Anthropic preset");
        let mut assembler = MessageAssembler::new();
        assembler.apply(&ProviderEvent::Start).unwrap();
        let verified = types::ProviderContextFragment {
            wire_item_index: Some(0),
            payload: types::ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::AnthropicMessages,
                item: serde_json::json!({
                    "type":"thinking_signature",
                    "signature":"verified",
                }),
            },
        };
        let (tx, mut rx) = mpsc::channel(1);
        finish_anthropic_error(
            &tx,
            &mut assembler,
            &spec,
            Usage::default(),
            AnthropicAdapterError::MissingTerminal,
            false,
            vec![verified.clone()],
        )
        .await;
        let ProviderEvent::Error { output, .. } = rx.recv().await.expect("terminal") else {
            panic!("expected error terminal")
        };
        assert_eq!(output.provider_context, vec![verified]);
    }

    #[tokio::test]
    async fn compact_native_preserves_order_and_has_typed_cancel_and_http_errors() {
        let compact_body = serde_json::json!({
            "id":"resp_compact",
            "object":"response.compaction",
            "output":[
                {"id":"m1","type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]},
                {"id":"cmp1","type":"compaction","encrypted_content":"opaque"}
            ],
            "usage":{
                "input_tokens":8,
                "input_tokens_details":{"cached_tokens":0},
                "output_tokens":2,
                "output_tokens_details":{"reasoning_tokens":0},
                "total_tokens":10
            }
        })
        .to_string();
        let app = Router::new().route(
            "/responses/compact",
            post(move || {
                let compact_body = compact_body.clone();
                async move {
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json")
                        .body(Body::from(compact_body))
                        .expect("response")
                }
            }),
        );
        let (base_url, server) = serve_router(app).await;
        let mut spec = ModelSpec::preset("openai-responses").expect("Responses preset");
        spec.base_url = base_url;
        let mut context = persisted_context(9);
        context.messages.insert(
            0,
            ContextMessage::Synthetic {
                message: Message::User(UserMessage {
                    content: vec![UserContent::Text {
                        text: "leading synthetic compact input".into(),
                    }],
                    timestamp: Utc::now(),
                }),
            },
        );
        let coverage =
            responses_compaction_coverage(&spec, &context).expect("derived test coverage");
        let result = compact_native_with_api_key(
            spec.clone(),
            context,
            CancellationToken::new(),
            "test-key".into(),
        )
        .await
        .expect("compact");
        assert_eq!(result.coverage(), &coverage);
        assert_eq!(result.items()[0]["type"], "message");
        assert_eq!(result.items()[1]["type"], "compaction");
        server.abort();

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let cancel_context = persisted_context(10);
        let error = compact_native_with_api_key(spec, cancel_context, cancelled, "test-key".into())
            .await
            .expect_err("cancelled");
        assert!(matches!(error, NativeCompactionError::Cancelled));

        let app = Router::new().route(
            "/responses/compact",
            post(|| async {
                Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from("bad compact request"))
                    .expect("response")
            }),
        );
        let (base_url, server) = serve_router(app).await;
        let mut spec = ModelSpec::preset("openai-responses").expect("Responses preset");
        spec.base_url = base_url;
        let http_context = persisted_context(11);
        let error = compact_native_with_api_key(
            spec,
            http_context,
            CancellationToken::new(),
            "test-key".into(),
        )
        .await
        .expect_err("HTTP error");
        server.abort();
        assert!(matches!(
            error,
            NativeCompactionError::Http {
                status: 400,
                ref body
            } if body == "bad compact request"
        ));
    }

    async fn assert_aborted_within_one_second(events: &mut ProviderEventStream) -> ProviderEvent {
        let started = std::time::Instant::now();
        let mut terminal = None;
        tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(event) = events.recv().await {
                if matches!(
                    event,
                    ProviderEvent::Done { .. } | ProviderEvent::Error { .. }
                ) {
                    assert!(terminal.is_none(), "terminal event emitted twice");
                    terminal = Some(event);
                }
            }
        })
        .await
        .expect("provider cancellation exceeded one second");
        assert!(started.elapsed() < Duration::from_secs(1));
        let terminal = terminal.expect("terminal event");
        assert!(matches!(
            terminal,
            ProviderEvent::Error {
                reason: StopReason::Aborted,
                ..
            }
        ));
        terminal
    }
}
