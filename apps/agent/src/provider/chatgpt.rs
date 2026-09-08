//! Account-scoped ChatGPT credentials and native Responses transport.
//! OAuth and refresh-token storage belong to the API, never the model adapter.

use std::{
    fmt,
    sync::{Arc, OnceLock},
    time::Instant,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::header::HeaderValue;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use super::{
    RESPONSE_HEADER_TIMEOUT, RequestWait, await_request,
    canonical_request::CanonicalRequestBody,
    model::{ModelSpec, ProtocolCompat, ProviderBackend, RequestOptions, ResponsesDialect},
    session_request,
};

pub const LITE_HEADER: &str = "x-openai-internal-codex-responses-lite";

pub struct ChatGptAccess {
    pub connection_id: String,
    pub account_id: String,
    pub access_token: Zeroizing<String>,
    pub expires_at: DateTime<Utc>,
}

impl fmt::Debug for ChatGptAccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChatGptAccess")
            .field("connection_id", &self.connection_id)
            .field("account_id", &self.account_id)
            .field("access_token", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChatGptAuthError {
    #[error("ChatGPT connection is not configured")]
    Disconnected,
    #[error("ChatGPT credential service is unavailable")]
    Unavailable,
    #[error("ChatGPT connection does not match the configured account")]
    IdentityMismatch,
    #[error("ChatGPT access credential is invalid or expired")]
    InvalidCredential,
    #[error("ChatGPT backend endpoint is invalid")]
    InvalidEndpoint,
}

#[async_trait]
pub trait ChatGptCredentialResolver: Send + Sync {
    /// The API conditionally refreshes only if this rejected token is still current.
    async fn resolve(
        &self,
        connection_id: &str,
        rejected_access_token: Option<&str>,
    ) -> Result<ChatGptAccess, ChatGptAuthError>;
}

#[derive(Clone)]
pub struct ChatGptCredentialSource {
    connection_id: String,
    resolver: Arc<dyn ChatGptCredentialResolver>,
}

impl ChatGptCredentialSource {
    pub fn new(connection_id: String, resolver: Arc<dyn ChatGptCredentialResolver>) -> Self {
        Self {
            connection_id,
            resolver,
        }
    }

    async fn resolve(
        &self,
        account_id: &str,
        rejected: Option<&str>,
    ) -> Result<ChatGptAccess, ChatGptAuthError> {
        if self.connection_id.trim().is_empty() || account_id.trim().is_empty() {
            return Err(ChatGptAuthError::Disconnected);
        }
        let access = self.resolver.resolve(&self.connection_id, rejected).await?;
        if access.connection_id != self.connection_id || access.account_id != account_id {
            return Err(ChatGptAuthError::IdentityMismatch);
        }
        if access.access_token.is_empty()
            || access.expires_at <= Utc::now()
            || HeaderValue::from_str(&access.access_token).is_err()
            || HeaderValue::from_str(&access.account_id).is_err()
        {
            return Err(ChatGptAuthError::InvalidCredential);
        }
        Ok(access)
    }
}

impl fmt::Debug for ChatGptCredentialSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChatGptCredentialSource")
            .field("connection_id", &self.connection_id)
            .finish_non_exhaustive()
    }
}

// Model configuration equality describes the selected connection, not resolver
// implementation identity. Every resolved credential is independently bound above.
impl PartialEq for ChatGptCredentialSource {
    fn eq(&self, other: &Self) -> bool {
        self.connection_id == other.connection_id
    }
}
impl Eq for ChatGptCredentialSource {}

#[derive(Debug, thiserror::Error)]
pub(super) enum RequestError {
    #[error(transparent)]
    Authentication(#[from] ChatGptAuthError),
    #[error("provider request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("missing API key environment variable {0}")]
    MissingApiKey(String),
}

impl RequestError {
    pub(super) fn code(&self) -> &'static str {
        match self {
            Self::Authentication(_) => "chatgpt_authentication_failed",
            Self::MissingApiKey(_) => "missing_api_key",
            Self::Transport(_) => "request_error",
        }
    }
}

fn validate_endpoint(spec: &ModelSpec) -> Result<(), ChatGptAuthError> {
    let url = reqwest::Url::parse(&spec.base_url).map_err(|_| ChatGptAuthError::InvalidEndpoint)?;
    let is_native = url.scheme() == "https"
        && url.host_str() == Some("chatgpt.com")
        && url.path().trim_end_matches('/') == "/backend-api/codex"
        && url.port().is_none();
    #[cfg(test)]
    let is_native = is_native
        || (url.scheme() == "http" && matches!(url.host_str(), Some("127.0.0.1" | "localhost")));
    if !is_native
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ChatGptAuthError::InvalidEndpoint);
    }
    Ok(())
}

// Native credentials and full Sumi context must never follow a Location header,
// even to another path on the same host. Keep the generic API client's policy intact.
fn native_http_client() -> Result<&'static reqwest::Client, ChatGptAuthError> {
    static CLIENT: OnceLock<Result<reqwest::Client, reqwest::Error>> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .user_agent(concat!("sumi-agent/", env!("CARGO_PKG_VERSION")))
                .connect_timeout(super::CONNECT_TIMEOUT)
                .redirect(reqwest::redirect::Policy::none())
                .build()
        })
        .as_ref()
        .map_err(|_| ChatGptAuthError::Unavailable)
}

/// Retry only a 401 response header, before any response body/output is read.
/// Transport errors, response-stream failures and a second 401 are never replayed.
pub(super) async fn send(
    client: &reqwest::Client,
    spec: &ModelSpec,
    body: &CanonicalRequestBody,
    options: &RequestOptions,
    cancel: &CancellationToken,
    api_key: Option<&str>,
    compact: bool,
    mut on_sent: impl FnMut(Instant),
) -> RequestWait<reqwest::Response, RequestError> {
    let client = if spec.backend == ProviderBackend::ChatGpt {
        match native_http_client() {
            Ok(client) => client,
            Err(error) => {
                return RequestWait::Response {
                    response: Err(error.into()),
                    request_sent_at: Instant::now(),
                };
            }
        }
    } else {
        client
    };
    let mut rejected: Option<Zeroizing<String>> = None;
    for attempt in 0..2 {
        let mut request = client.post(if compact {
            spec.compact_endpoint()
        } else {
            spec.endpoint()
        });
        let access = if spec.backend == ProviderBackend::ChatGpt {
            let resolve = async {
                validate_endpoint(spec)?;
                spec.chatgpt_credentials
                    .as_ref()
                    .ok_or(ChatGptAuthError::Disconnected)?
                    .resolve(&spec.account_scope, rejected.as_deref().map(String::as_str))
                    .await
            };
            let result = tokio::select! {
                biased;
                _ = cancel.cancelled() => return RequestWait::Cancelled,
                value = tokio::time::timeout(RESPONSE_HEADER_TIMEOUT, resolve) => value,
            };
            let access = match result {
                Err(_) => return RequestWait::TimedOut,
                Ok(Err(error)) => {
                    return RequestWait::Response {
                        response: Err(error.into()),
                        request_sent_at: Instant::now(),
                    };
                }
                Ok(Ok(access)) => access,
            };
            request = request
                .bearer_auth(access.access_token.as_str())
                .header("ChatGPT-Account-ID", &access.account_id);
            Some(access)
        } else {
            let Some(key) = api_key.filter(|key| !key.is_empty()) else {
                return RequestWait::Response {
                    response: Err(RequestError::MissingApiKey(spec.api_key_env.clone())),
                    request_sent_at: Instant::now(),
                };
            };
            request = request.bearer_auth(key);
            None
        };
        if matches!(&spec.compat, ProtocolCompat::Responses(c) if c.dialect == ResponsesDialect::CodexLite)
        {
            request = request.header(LITE_HEADER, "true");
        }
        let request = body
            .clone()
            .apply(session_request(request, spec, options))
            .send();
        match await_request(request, cancel, RESPONSE_HEADER_TIMEOUT, &mut on_sent).await {
            RequestWait::Cancelled => return RequestWait::Cancelled,
            RequestWait::TimedOut => return RequestWait::TimedOut,
            RequestWait::Response {
                response: Err(error),
                request_sent_at,
            } => {
                return RequestWait::Response {
                    response: Err(error.into()),
                    request_sent_at,
                };
            }
            RequestWait::Response {
                response: Ok(response),
                request_sent_at,
            } => {
                if response.status() == reqwest::StatusCode::UNAUTHORIZED
                    && attempt == 0
                    && let Some(access) = access
                {
                    rejected = Some(access.access_token);
                    continue;
                }
                if response.status() == reqwest::StatusCode::UNAUTHORIZED
                    && spec.backend == ProviderBackend::ChatGpt
                {
                    return RequestWait::Response {
                        response: Err(ChatGptAuthError::InvalidCredential.into()),
                        request_sent_at,
                    };
                }
                return RequestWait::Response {
                    response: Ok(response),
                    request_sent_at,
                };
            }
        }
    }
    unreachable!("second response returns without retry")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{
        adapters::responses::{build_compact_request, build_request},
        types::{PromptContext, ToolDefinition},
    };
    use axum::{
        Router,
        body::Body,
        extract::State,
        http::{HeaderMap, StatusCode},
        response::Response,
        routing::post,
    };
    use serde_json::{Value, json};
    use std::sync::Mutex;

    struct Resolver {
        requests: Mutex<Vec<Option<String>>>,
        account: &'static str,
    }
    #[async_trait]
    impl ChatGptCredentialResolver for Resolver {
        async fn resolve(
            &self,
            connection_id: &str,
            rejected: Option<&str>,
        ) -> Result<ChatGptAccess, ChatGptAuthError> {
            self.requests
                .lock()
                .unwrap()
                .push(rejected.map(str::to_owned));
            Ok(ChatGptAccess {
                connection_id: connection_id.into(),
                account_id: self.account.into(),
                access_token: Zeroizing::new(
                    if rejected.is_some() {
                        "rotated-secret"
                    } else {
                        "first-secret"
                    }
                    .into(),
                ),
                expires_at: Utc::now() + chrono::Duration::hours(1),
            })
        }
    }
    fn fixture_context() -> PromptContext {
        PromptContext {
            system_prompt: "Sumi's own constitution. Preserve my context.".into(),
            memory_blocks: vec![],
            messages: vec![],
            provider_context: vec![],
            replay_provenance: None,
            tools: vec![ToolDefinition {
                name: "read_file".into(),
                description: "Read a Sumi note".into(),
                parameters: json!({"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":false}),
            }],
        }
    }
    fn fixture_spec(resolver: Arc<Resolver>) -> ModelSpec {
        let mut spec = ModelSpec::preset("chatgpt-responses").unwrap();
        spec.account_scope = "account-fixture".into();
        spec.chatgpt_credentials = Some(ChatGptCredentialSource::new(
            "connection-fixture".into(),
            resolver,
        ));
        spec
    }

    #[tokio::test]
    async fn chatgpt_refresh_retries_only_401_before_output_with_identical_lite_body() {
        type Requests = Arc<Mutex<Vec<(HeaderMap, Value)>>>;
        async fn handler(
            State(requests): State<Requests>,
            headers: HeaderMap,
            axum::Json(body): axum::Json<Value>,
        ) -> Response {
            let mut requests = requests.lock().unwrap();
            requests.push((headers, body));
            Response::builder()
                .status(if requests.len() == 1 {
                    StatusCode::UNAUTHORIZED
                } else {
                    StatusCode::OK
                })
                .body(Body::from("fixture"))
                .unwrap()
        }
        let requests: Requests = Default::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(
            axum::serve(
                listener,
                Router::new()
                    .route("/responses", post(handler))
                    .with_state(requests.clone()),
            )
            .into_future(),
        );
        let resolver = Arc::new(Resolver {
            requests: Mutex::new(vec![]),
            account: "account-fixture",
        });
        let mut spec = fixture_spec(resolver.clone());
        spec.base_url = base_url;
        let options = RequestOptions {
            session_id: Some("pa-owned-session".into()),
            reasoning_effort: Some("medium".into()),
            ..Default::default()
        };
        let request = build_request(&spec, &fixture_context(), &options).unwrap();
        let canonical = CanonicalRequestBody::serialize(&request).unwrap();
        let mut sent = 0;
        let response = send(
            &reqwest::Client::new(),
            &spec,
            &canonical,
            &options,
            &CancellationToken::new(),
            None,
            false,
            |_| sent += 1,
        )
        .await;
        server.abort();
        assert!(
            matches!(response, RequestWait::Response {response:Ok(ref r),..} if r.status()==StatusCode::OK)
        );
        assert_eq!(sent, 2);
        assert_eq!(
            *resolver.requests.lock().unwrap(),
            vec![None, Some("first-secret".into())]
        );
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].1, requests[1].1);
        assert_eq!(requests[0].0["authorization"], "Bearer first-secret");
        assert_eq!(requests[1].0["authorization"], "Bearer rotated-secret");
        for (headers, body) in requests.iter() {
            assert_eq!(headers["chatgpt-account-id"], "account-fixture");
            assert_eq!(headers[LITE_HEADER], "true");
            assert_eq!(body["input"][0]["type"], "additional_tools");
            assert_eq!(body["input"][0]["tools"][0]["name"], "functions");
            assert_eq!(
                body["input"][0]["tools"][0]["tools"][0]["name"],
                "read_file"
            );
            assert_eq!(
                body["input"][1]["content"][0]["text"],
                fixture_context().system_prompt
            );
            assert_eq!(body["reasoning"]["context"], "all_turns");
            assert_eq!(body["reasoning"]["effort"], "medium");
            assert_eq!(body["prompt_cache_key"], "pa-owned-session");
            assert_eq!(body["parallel_tool_calls"], false);
            assert!(
                body.get("tools").is_none()
                    && body.get("instructions").is_none()
                    && body.get("max_output_tokens").is_none()
            );
        }
    }

    #[tokio::test]
    async fn chatgpt_identity_mismatch_and_untrusted_endpoint_fail_without_exposing_tokens() {
        let resolver = Arc::new(Resolver {
            requests: Mutex::new(vec![]),
            account: "other-account",
        });
        let spec = fixture_spec(resolver.clone());
        let body = CanonicalRequestBody::serialize(&json!({})).unwrap();
        let result = send(
            &reqwest::Client::new(),
            &spec,
            &body,
            &RequestOptions::default(),
            &CancellationToken::new(),
            None,
            false,
            |_| panic!("must not send"),
        )
        .await;
        let RequestWait::Response {
            response: Err(error),
            ..
        } = result
        else {
            panic!("expected authentication failure")
        };
        assert!(matches!(
            error,
            RequestError::Authentication(ChatGptAuthError::IdentityMismatch)
        ));
        assert!(!format!("{error:?}").contains("first-secret"));
        let mut spec = spec;
        spec.base_url = "https://unrelated.invalid/backend-api/codex".into();
        let result = send(
            &reqwest::Client::new(),
            &spec,
            &body,
            &RequestOptions::default(),
            &CancellationToken::new(),
            None,
            false,
            |_| panic!("must not send"),
        )
        .await;
        assert!(matches!(
            result,
            RequestWait::Response {
                response: Err(RequestError::Authentication(
                    ChatGptAuthError::InvalidEndpoint
                )),
                ..
            }
        ));
        assert_eq!(resolver.requests.lock().unwrap().len(), 1);
    }

    #[test]
    fn chatgpt_lite_compact_and_parent_share_the_same_prefix_identity() {
        let spec = fixture_spec(Arc::new(Resolver {
            requests: Mutex::new(vec![]),
            account: "account-fixture",
        }));
        let context = fixture_context();
        let parent = build_request(
            &spec,
            &context,
            &RequestOptions {
                session_id: Some("same-pa".into()),
                ..Default::default()
            },
        )
        .unwrap();
        let compact = build_compact_request(&spec, &context).unwrap();
        assert_eq!(parent["input"], compact["input"]);
        assert_eq!(compact["reasoning"]["context"], "all_turns");
        assert_eq!(compact["parallel_tool_calls"], false);
        assert!(compact.get("stream").is_none());
        assert!(
            build_request(
                &spec,
                &context,
                &RequestOptions {
                    temperature: Some(0.7),
                    ..Default::default()
                }
            )
            .is_err()
        );
    }
    #[tokio::test]
    async fn chatgpt_second_401_stops_without_reading_reflected_credentials() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut spec = fixture_spec(Arc::new(Resolver {
            requests: Mutex::new(vec![]),
            account: "account-fixture",
        }));
        spec.base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/responses",
                    post(|| async { (StatusCode::UNAUTHORIZED, "reflected rotated-secret") }),
                ),
            )
            .await
            .unwrap();
        });
        let mut sent = 0;
        let result = send(
            &reqwest::Client::new(),
            &spec,
            &CanonicalRequestBody::serialize(&json!({})).unwrap(),
            &RequestOptions::default(),
            &CancellationToken::new(),
            None,
            false,
            |_| sent += 1,
        )
        .await;
        server.abort();
        assert_eq!(sent, 2);
        let RequestWait::Response {
            response: Err(error),
            ..
        } = result
        else {
            panic!("second rejection must stop")
        };
        assert!(matches!(
            error,
            RequestError::Authentication(ChatGptAuthError::InvalidCredential)
        ));
        assert!(!format!("{error:?}").contains("rotated-secret"));
    }
    #[tokio::test]
    async fn chatgpt_redirect_never_forwards_context_or_account_to_second_destination() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let hits = Arc::new(AtomicUsize::new(0));
        let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let location = format!("http://{}/capture", target.local_addr().unwrap());
        let target_hits = hits.clone();
        let target_server = tokio::spawn(async move {
            axum::serve(
                target,
                Router::new().route(
                    "/capture",
                    post(move || {
                        let hits = target_hits.clone();
                        async move {
                            hits.fetch_add(1, Ordering::SeqCst);
                            "must not reach"
                        }
                    }),
                ),
            )
            .await
            .unwrap();
        });
        for status in [
            StatusCode::TEMPORARY_REDIRECT,
            StatusCode::PERMANENT_REDIRECT,
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut spec = fixture_spec(Arc::new(Resolver {
                requests: Mutex::new(vec![]),
                account: "account-fixture",
            }));
            spec.base_url = format!("http://{}", listener.local_addr().unwrap());
            let location = location.clone();
            let server = tokio::spawn(async move {
                axum::serve(
                    listener,
                    Router::new().route(
                        "/responses",
                        post(move || {
                            let location = location.clone();
                            async move {
                                Response::builder()
                                    .status(status)
                                    .header("location", location)
                                    .body(Body::empty())
                                    .unwrap()
                            }
                        }),
                    ),
                )
                .await
                .unwrap();
            });
            let mut sent = 0;
            let result = send(
                &reqwest::Client::new(),
                &spec,
                &CanonicalRequestBody::serialize(&json!({"input":"owned Sumi context"})).unwrap(),
                &RequestOptions::default(),
                &CancellationToken::new(),
                None,
                false,
                |_| sent += 1,
            )
            .await;
            let RequestWait::Response {
                response: Ok(response),
                ..
            } = result
            else {
                panic!("redirect response expected")
            };
            assert_eq!(response.status(), status);
            assert_eq!(sent, 1);
            assert_eq!(hits.load(Ordering::SeqCst), 0);
            server.abort();
        }
        target_server.abort();
    }
}
