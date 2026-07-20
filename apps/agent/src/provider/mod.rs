//! Provider transport, protocol adapters, and normalized event assembly.

pub mod adapters;
pub mod assembler;
pub mod overflow;
pub mod partial_json;
pub mod retry;
pub mod transport;
pub mod types;

use std::{
    env,
    future::Future,
    sync::{Arc, OnceLock},
    time::Duration,
};

use adapters::chat_completions::{
    ChatAdapterError, ChatReceiveState, ChatTerminal, ModelSpec, RequestOptions, build_request,
    requested_output_tokens,
};
use assembler::{FrozenToolSchemaRegistry, MessageAssembler, ResponseBudget, TerminalMetadata};
use chrono::Utc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use transport::{SseError, SseStream};
use types::{
    AssistantMessage, PromptContext, ProviderEvent, ProviderEventStream, ProviderOutput,
    StopReason, SuccessTerminalCommit, Usage,
};

const EVENT_CHANNEL_CAPACITY: usize = 64;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(120);

enum RequestWait<T, E> {
    Response(Result<T, E>),
    Cancelled,
    TimedOut,
}

struct ProducerChannels {
    normal: mpsc::Sender<ProviderEvent>,
    priority_terminal: mpsc::Sender<ProviderEvent>,
    success_terminal_committed: Arc<SuccessTerminalCommit>,
}

async fn await_request<F, T, E>(
    request: F,
    cancel: &CancellationToken,
    timeout: Duration,
) -> RequestWait<T, E>
where
    F: Future<Output = Result<T, E>>,
{
    tokio::select! {
        biased;
        _ = cancel.cancelled() => RequestWait::Cancelled,
        response = request => RequestWait::Response(response),
        _ = tokio::time::sleep(timeout) => RequestWait::TimedOut,
    }
}

fn http_client() -> Result<&'static reqwest::Client, String> {
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

fn stream_with_api_key(
    spec: ModelSpec,
    context: PromptContext,
    options: RequestOptions,
    cancel: CancellationToken,
    api_key: Option<String>,
) -> ProviderEventStream {
    let (tx, rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    let (priority_terminal_tx, priority_terminal_rx) = mpsc::channel(1);
    let origin = spec.origin();
    let provider = spec.provider.clone();
    let stream_budget = requested_output_tokens(&spec, &options)
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
    tokio::spawn(
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
                    success_terminal_committed: producer_terminal_committed,
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
        success_terminal_committed,
    } = channels;
    let output_tokens = match requested_output_tokens(&spec, &options) {
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
                false,
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
            false,
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
                false,
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
            false,
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
                false,
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
                false,
            )
            .await;
            return;
        }
    };

    let request_started = std::time::Instant::now();
    tracing::info!(phase = "request_sent", "provider request sent");
    let request = client
        .post(spec.endpoint())
        .bearer_auth(api_key)
        .json(&body)
        .send();
    let response = match await_request(request, &cancel, RESPONSE_HEADER_TIMEOUT).await {
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
                false,
            )
            .await;
            return;
        }
        RequestWait::Response(response) => match response {
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
        },
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

    let mut saw_public_delta = false;
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
                            false,
                        )
                        .await;
                        return;
                    }
                };
                if !saw_public_delta
                    && events.iter().any(|event| {
                        matches!(
                            event,
                            ProviderEvent::TextDelta { .. } | ProviderEvent::ThinkingDelta { .. }
                        )
                    })
                {
                    saw_public_delta = true;
                    tracing::info!(
                        phase = "request_sent_to_first_public_delta",
                        elapsed_ms = request_started.elapsed().as_millis() as u64,
                        "provider first public delta"
                    );
                }
                let mut events = events.into_iter();
                while let Some(event) = events.next() {
                    match emit(&tx, &mut assembler, event, &cancel).await {
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
                                false,
                            )
                            .await;
                            return;
                        }
                    }
                }
            }
            Ok(None) => {
                let usage = receive.usage().clone();
                match receive.finish(Utc::now()) {
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
    match receive.finish(Utc::now()) {
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
                    false,
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
            terminal.stop_reason == StopReason::Aborted,
        )
        .await;
        return;
    }
    for event in terminal.events {
        let permit = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
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
                false,
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
                false,
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

async fn finish_failure(
    priority_terminal_tx: &mpsc::Sender<ProviderEvent>,
    assembler: &mut MessageAssembler,
    spec: &ModelSpec,
    usage: Usage,
    error_message: String,
    provider_code: &str,
    cancelled: bool,
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
            provider_context: Vec::new(),
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
        | ChatAdapterError::InvalidMaxTokens { .. }
        | ChatAdapterError::ReasoningRequired
        | ChatAdapterError::InvalidReasoningEffort(_) => {
            (error.to_string(), "invalid_provider_request".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, env};

    use axum::{
        Router,
        body::Body,
        http::{Response, StatusCode},
        routing::post,
    };
    use futures_util::{StreamExt, stream};

    use super::*;
    use crate::provider::types::{
        AssistantContent, RejectedToolCall, ToolArgumentError, ToolCall, ToolDefinition,
        ToolResultMessage, UserContent, ValidatedToolArguments,
    };

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
            "\"arguments\":\"{\\\"items\\\":[\\\"ok\\\",\\\"raw-argument-secret\\\"]}\"}}]},",
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
            36
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
        assert_eq!(output.message.usage.output, 32);
        assert_eq!(output.message.usage.reasoning, 27);
        assert_eq!(output.message.usage.total_tokens, 45);
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
    fn adapter_normalization_p95_smoke_is_under_30ms() {
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
    async fn response_header_wait_is_bounded_and_cancel_first() {
        let cancel = CancellationToken::new();
        assert!(matches!(
            await_request(
                std::future::pending::<Result<(), ()>>(),
                &cancel,
                Duration::from_millis(1),
            )
            .await,
            RequestWait::TimedOut
        ));

        cancel.cancel();
        assert!(matches!(
            await_request(std::future::ready(Ok::<_, ()>(())), &cancel, Duration::ZERO,).await,
            RequestWait::Cancelled
        ));
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
    async fn live_chat_tool_roundtrip_when_opted_in() {
        if env::var("SUMI_LIVE_TEST").as_deref() != Ok("1") {
            return;
        }
        if let Some(path) = env::var_os("SUMI_ENV_FILE") {
            dotenvy::from_path(path).expect("load SUMI_ENV_FILE for live test");
        }

        let selected = env::var("SUMI_LIVE_PRESETS")
            .unwrap_or_else(|_| "kimi-k3,glm-5.2,umans,opencode-go".to_owned());
        for preset in selected
            .split(',')
            .map(str::trim)
            .filter(|preset| !preset.is_empty())
        {
            let spec =
                ModelSpec::preset(preset).unwrap_or_else(|| panic!("unknown preset {preset}"));
            let Ok(api_key) = env::var(&spec.api_key_env) else {
                continue;
            };
            if api_key.is_empty() {
                continue;
            }

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
            };
            let first = run_live_request(
                spec.clone(),
                first_context,
                RequestOptions {
                    max_tokens: Some(4_096),
                    tool_choice: Some(serde_json::json!("required")),
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
            if preset == "kimi-k3" {
                assert!(
                    first
                        .content
                        .iter()
                        .any(|content| matches!(content, types::AssistantContent::Thinking { .. })),
                    "Kimi live fixture did not expose replayable reasoning"
                );
            }

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
    }

    async fn run_live_request(
        spec: ModelSpec,
        context: PromptContext,
        options: RequestOptions,
        api_key: String,
    ) -> AssistantMessage {
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
                    ProviderEvent::Done { output, .. } => return output.message,
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
