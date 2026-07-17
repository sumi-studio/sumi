//! OpenAI-compatible model provider streaming.

pub mod assembler;
pub mod overflow;
pub mod partial_json;
pub mod request;
pub mod retry;
pub mod sse;
pub mod types;

use std::{env, sync::OnceLock, time::Duration};

use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use self::{
    assembler::MessageAssembler,
    request::{ModelSpec, RequestOptions, build_request},
    sse::{SseError, SseStream},
    types::{PromptContext, ProviderEvent, ProviderEventStream},
};

const EVENT_CHANNEL_CAPACITY: usize = 64;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// プロセス共有のHTTPクライアント。リクエストごとに生成するとコネクション
/// プールが使い捨てになり、`Client::new()` はTLS初期化失敗時にpanicして
/// §3.2 の「必ず終端イベントで返す」契約を破る。ここではResultを保持して
/// 呼び出し側が Error イベントに変換できるようにする。
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
    tokio::spawn(async move {
        let mut assembler = MessageAssembler::new(&spec.id, &spec.provider);
        let Some(api_key) = api_key.filter(|key| !key.is_empty()) else {
            send_all(
                &tx,
                assembler.fail(
                    format!("missing API key environment variable {}", spec.api_key_env),
                    false,
                ),
            )
            .await;
            return;
        };

        let client = match http_client() {
            Ok(client) => client,
            Err(error) => {
                send_all(&tx, assembler.fail(error, false)).await;
                return;
            }
        };

        let body = build_request(&spec, &context, &options);
        let request_sent = std::time::Instant::now();
        tracing::debug!(model = %spec.id, "provider request sent");
        let request = client
            .post(spec.endpoint())
            .bearer_auth(api_key)
            .json(&body)
            .send();
        let response = tokio::select! {
            _ = cancel.cancelled() => {
                send_all(&tx, assembler.fail("Request was aborted", true)).await;
                return;
            }
            response = request => match response {
                Ok(response) => response,
                Err(error) => {
                    send_all(&tx, assembler.fail(error.to_string(), cancel.is_cancelled())).await;
                    return;
                }
            }
        };

        let mut sse = match SseStream::from_response(response, cancel.clone()).await {
            Ok(sse) => sse,
            Err(error) => {
                let cancelled = matches!(error, SseError::Cancelled) || cancel.is_cancelled();
                send_all(&tx, assembler.fail(error.to_string(), cancelled)).await;
                return;
            }
        };

        let mut saw_delta = false;
        loop {
            match sse.next_payload().await {
                Ok(Some(payload)) => {
                    let chunk: Value = match serde_json::from_str(&payload) {
                        Ok(chunk) => chunk,
                        Err(error) => {
                            send_all(
                                &tx,
                                assembler.fail(format!("invalid SSE JSON: {error}"), false),
                            )
                            .await;
                            return;
                        }
                    };
                    let events = assembler.push_chunk(&chunk);
                    if !saw_delta
                        && events.iter().any(|event| {
                            matches!(
                                event,
                                ProviderEvent::TextDelta { .. }
                                    | ProviderEvent::ThinkingDelta { .. }
                                    | ProviderEvent::ToolCallDelta { .. }
                            )
                        })
                    {
                        saw_delta = true;
                        tracing::debug!(
                            model = %spec.id,
                            ttft_ms = request_sent.elapsed().as_millis() as u64,
                            "provider first delta"
                        );
                    }
                    if !send_all(&tx, events).await {
                        return;
                    }
                }
                Ok(None) => {
                    send_all(&tx, assembler.finish(cancel.is_cancelled())).await;
                    return;
                }
                Err(error) => {
                    let cancelled = matches!(error, SseError::Cancelled) || cancel.is_cancelled();
                    send_all(&tx, assembler.fail(error.to_string(), cancelled)).await;
                    return;
                }
            }
        }
    });
    ProviderEventStream::new(rx)
}

async fn send_all(tx: &mpsc::Sender<ProviderEvent>, events: Vec<ProviderEvent>) -> bool {
    for event in events {
        if tx.send(event).await.is_err() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        body::Body,
        http::{Response, StatusCode},
        routing::post,
    };
    use chrono::Utc;

    use super::*;
    use crate::provider::types::{Message, StopReason, UserContent, UserMessage};

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

    async fn replay(preset: &str, body: &'static str) -> Vec<ProviderEvent> {
        let (base_url, server) = serve_fixture(StatusCode::OK, body).await;
        let mut spec = ModelSpec::preset(preset).expect("preset");
        spec.base_url = base_url;
        let mut events = stream_with_api_key(
            spec,
            PromptContext {
                system_prompt: "test".to_owned(),
                messages: vec![],
                tools: vec![],
            },
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

    #[tokio::test]
    async fn fixture_stream_closes_with_done() {
        let received = replay(
            "kimi-k3",
            include_str!("../../tests/fixtures/kimi_text.sse"),
        )
        .await;

        assert!(received.iter().any(
            |event| matches!(event, ProviderEvent::TextDelta { delta, .. } if delta == "hello")
        ));
        let ProviderEvent::Done { message, .. } = received.last().expect("terminal") else {
            panic!("done");
        };
        assert_eq!(message.usage.total_tokens, 7);
    }

    #[tokio::test]
    async fn provider_fixtures_cover_tools_reasoning_and_truncation() {
        let tool_events = replay(
            "kimi-k3",
            include_str!("../../tests/fixtures/kimi_toolcall.sse"),
        )
        .await;
        assert!(tool_events.iter().any(|event| matches!(
            event,
            ProviderEvent::ToolCallEnd { tool_call, .. }
                if tool_call.arguments == serde_json::json!({"path":"a.txt"})
        )));
        assert!(matches!(
            tool_events.last(),
            Some(ProviderEvent::Done {
                reason: StopReason::ToolUse,
                ..
            })
        ));

        let reasoning_events = replay(
            "kimi-k3",
            include_str!("../../tests/fixtures/kimi_reasoning.sse"),
        )
        .await;
        assert!(reasoning_events.iter().any(
            |event| matches!(event, ProviderEvent::ThinkingDelta { delta, .. } if delta == "consider")
        ));

        let glm_events = replay(
            "glm-5.2",
            include_str!("../../tests/fixtures/glm_tool_stream.sse"),
        )
        .await;
        assert!(glm_events.iter().any(
            |event| matches!(event, ProviderEvent::ThinkingDelta { delta, .. } if delta == "think")
        ));

        let truncated = replay(
            "kimi-k3",
            include_str!("../../tests/fixtures/transport_error.sse"),
        )
        .await;
        assert!(matches!(
            truncated.last(),
            Some(ProviderEvent::Error {
                reason: StopReason::Error,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn http_429_fixture_becomes_an_error_event() {
        let (base_url, server) = serve_fixture(
            StatusCode::TOO_MANY_REQUESTS,
            include_str!("../../tests/fixtures/http_429.json"),
        )
        .await;
        let mut spec = ModelSpec::preset("kimi-k3").expect("preset");
        spec.base_url = base_url;
        let mut events = stream_with_api_key(
            spec,
            PromptContext {
                system_prompt: "test".to_owned(),
                messages: vec![],
                tools: vec![],
            },
            RequestOptions::default(),
            CancellationToken::new(),
            Some("test-key".to_owned()),
        );
        let mut received = Vec::new();
        while let Some(event) = events.recv().await {
            received.push(event);
        }
        server.abort();

        let ProviderEvent::Error { error, .. } = received.last().expect("terminal") else {
            panic!("error");
        };
        assert!(
            error.error_message.as_deref().is_some_and(|message| {
                message.contains("429") && message.contains("rate limit")
            })
        );
    }

    #[tokio::test]
    async fn missing_key_is_reported_as_an_event() {
        let spec = ModelSpec::preset("kimi-k3").expect("preset");
        let mut events = stream_with_api_key(
            spec,
            PromptContext {
                system_prompt: String::new(),
                messages: vec![],
                tools: vec![],
            },
            RequestOptions::default(),
            CancellationToken::new(),
            None,
        );

        assert!(matches!(events.recv().await, Some(ProviderEvent::Start)));
        let ProviderEvent::Error { error, .. } = events.recv().await.expect("error event") else {
            panic!("error");
        };
        assert!(
            error
                .error_message
                .expect("message")
                .contains("missing API key")
        );
    }

    #[test]
    fn all_fixtures_are_valid_chunk_sequences() {
        for fixture in [
            include_str!("../../tests/fixtures/kimi_text.sse"),
            include_str!("../../tests/fixtures/kimi_toolcall.sse"),
            include_str!("../../tests/fixtures/kimi_reasoning.sse"),
            include_str!("../../tests/fixtures/glm_tool_stream.sse"),
            include_str!("../../tests/fixtures/transport_error.sse"),
        ] {
            for line in fixture
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
            {
                if line != "[DONE]" {
                    serde_json::from_str::<Value>(line).expect("fixture chunk JSON");
                }
            }
        }
        serde_json::from_str::<Value>(include_str!("../../tests/fixtures/http_429.json"))
            .expect("429 fixture JSON");
    }

    /// プロセス環境を優先しつつ、SUMI_ENV_FILE の env ファイルからも値を引く。
    /// ユニットテストバイナリはマルチスレッドで走るため、`load_env_file()`
    /// (= set_var) を呼ぶと env を読む他テストとデータレースになる
    /// (edition 2024 で `set_var` が unsafe になった理由)。ここでは
    /// プロセス環境を変更せずにファイルを直接パースする。
    fn live_env(name: &str) -> Option<String> {
        if let Ok(value) = std::env::var(name) {
            return Some(value);
        }
        let path = std::env::var_os("SUMI_ENV_FILE")?;
        dotenvy::from_path_iter(std::path::Path::new(&path))
            .ok()?
            .filter_map(Result::ok)
            .find(|(key, _)| key == name)
            .map(|(_, value)| value)
    }

    #[tokio::test]
    async fn live_smoke_is_opt_in() {
        if live_env("SUMI_LIVE_TEST").as_deref() != Some("1") {
            return;
        }
        let preset = live_env("SUMI_LIVE_PRESET").unwrap_or_else(|| "opencode-go".to_owned());
        let spec = ModelSpec::preset(&preset).expect("SUMI_LIVE_PRESET must name a preset");
        let api_key = live_env(&spec.api_key_env);
        let mut events = stream_with_api_key(
            spec,
            PromptContext {
                system_prompt: "Reply briefly.".to_owned(),
                messages: vec![Message::User(UserMessage {
                    content: vec![UserContent::Text {
                        text: "Reply with OK.".to_owned(),
                    }],
                    timestamp: Utc::now(),
                })],
                tools: vec![],
            },
            RequestOptions {
                max_tokens: Some(64),
                ..RequestOptions::default()
            },
            CancellationToken::new(),
            api_key,
        );
        let mut terminal = None;
        while let Some(event) = events.recv().await {
            if matches!(
                event,
                ProviderEvent::Done { .. } | ProviderEvent::Error { .. }
            ) {
                terminal = Some(event);
            }
        }
        assert!(matches!(terminal, Some(ProviderEvent::Done { .. })));
    }
}
