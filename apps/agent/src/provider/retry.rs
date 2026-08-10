//! 呼び出し順の契約: エージェントループ (T15) では必ず
//! `overflow::classify_context_overflow` を先に判定してから `is_retryable` を
//! 呼ぶこと。`\b500\b` 等の数字パターンは "maximum context length is 500
//! tokens" のような本文中の数字にも一致するため、overflow を先に除外
//! しないとコンテキスト溢れを無限リトライしうる。

use std::{sync::OnceLock, time::Duration};

use regex::RegexSet;
use tokio_util::sync::CancellationToken;

use super::types::{AssistantMessage, StopReason};

pub const MAX_RETRIES: usize = 3;

pub fn is_retryable(message: &AssistantMessage) -> bool {
    if message.stop_reason != StopReason::Error {
        return false;
    }
    match message.provider_code.as_deref() {
        Some(code) if retryable_machine_code(code) && !transient_status_code(code) => return true,
        Some("model_context_window_exceeded" | "sensitive" | "content_filter" | "cancelled") => {
            return false;
        }
        Some(code) if transient_status_code(code) => {
            return !message
                .error_message
                .as_deref()
                .is_some_and(|error| non_retryable_patterns().is_match(error));
        }
        _ => {}
    }
    if let Some(retryable) = classify_opencode_go_console_400(message) {
        return retryable;
    }
    let Some(error) = message.error_message.as_deref() else {
        return false;
    };
    if non_retryable_patterns().is_match(error) {
        return false;
    }
    retryable_patterns().is_match(error)
}

/// OpenCode Go's Console proxy can surface a selected upstream's transient
/// failure as an HTTP 400. Console intentionally does not retry generic 400s,
/// so keep this exception pinned to the exact observed envelope instead of
/// teaching the generic classifier that bad requests are transient.
fn classify_opencode_go_console_400(message: &AssistantMessage) -> Option<bool> {
    if message.provider != "opencode-go"
        || message.origin.protocol != super::types::ApiProtocol::OpenAiChatCompletions
        || message.provider_code.as_deref() != Some("http_400")
    {
        return None;
    }

    let Some(body) = message
        .error_message
        .as_deref()
        .and_then(|error| error.strip_prefix("400: "))
    else {
        return Some(false);
    };
    let Ok(envelope) = serde_json::from_str::<serde_json::Value>(body) else {
        return Some(false);
    };
    let Some(error) = envelope.get("error").and_then(serde_json::Value::as_object) else {
        return Some(false);
    };

    Some(
        error.get("message").and_then(serde_json::Value::as_str)
            == Some("Error from provider (Console Go): Upstream request failed")
            && error.get("type").and_then(serde_json::Value::as_str)
                == Some("invalid_request_error")
            && error.get("code").and_then(serde_json::Value::as_str)
                == Some("invalid_request_error")
            && error.get("param").is_some_and(serde_json::Value::is_null),
    )
}

pub(crate) fn retryable_machine_code(code: &str) -> bool {
    matches!(
        code,
        "network_error"
            | "request_error"
            | "transport_error"
            | "overloaded_error"
            | "server_error"
            | "unexpected_sse_eof"
            | "idle_timeout"
            | "response_header_timeout"
    ) || transient_status_code(code)
}

fn transient_status_code(code: &str) -> bool {
    matches!(
        code,
        "429"
            | "500"
            | "502"
            | "503"
            | "504"
            | "524"
            | "http_429"
            | "http_500"
            | "http_502"
            | "http_503"
            | "http_504"
            | "http_524"
    )
}

pub fn retry_delay(attempt: usize) -> Option<Duration> {
    (attempt < MAX_RETRIES).then(|| Duration::from_secs(2_u64.pow(attempt as u32 + 1)))
}

pub async fn sleep_or_cancel(delay: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

fn non_retryable_patterns() -> &'static RegexSet {
    static PATTERNS: OnceLock<RegexSet> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        compile_patterns([
            r"(?i)GoUsageLimitError",
            r"(?i)FreeUsageLimitError",
            r"(?i)Monthly usage limit reached",
            r"(?i)available balance",
            r"(?i)insufficient_quota",
            r"(?i)out of budget",
            r"(?i)quota exceeded",
            r"(?i)billing",
        ])
    })
}

fn retryable_patterns() -> &'static RegexSet {
    static PATTERNS: OnceLock<RegexSet> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        compile_patterns([
            r"(?i)overloaded",
            r"(?i)rate.?limit",
            r"(?i)too many requests",
            r"\b429\b",
            r"\b500\b",
            r"\b502\b",
            r"\b503\b",
            r"\b504\b",
            r"\b524\b",
            r"(?i)service.?unavailable",
            r"(?i)server.?error",
            r"(?i)internal.?error",
            // OpenRouter transient upstream failure (#2264).
            r"(?i)provider.?returned.?error",
            // Raw fetch/proxy failures (#733) and connection drops (#3317).
            r"(?i)network.?error",
            r"(?i)connection.?(error|refused|lost)",
            r"(?i)other side closed",
            r"(?i)fetch failed",
            r"(?i)upstream.?connect",
            r"(?i)reset before headers",
            r"(?i)socket (hang up|connection was closed)",
            r"(?i)timed? out",
            r"(?i)timeout",
            r"(?i)terminated",
            r"(?i)websocket.?(closed|error)",
            // Premature provider stream endings (#4433, #3594).
            r"(?i)ended without",
            r"(?i)stream ended before message_stop",
            r"(?i)http2 request did not get a response",
            // Provider-requested delay exceeded the inner cap (#1123).
            r"(?i)retry delay",
            // Explicit mid-stream retry guidance (#6019).
            r"(?i)(you can|please) retry your request",
            r"(?i)try your request again",
            r"(?i)ResourceExhausted",
        ])
    })
}

fn compile_patterns<const N: usize>(patterns: [&str; N]) -> RegexSet {
    RegexSet::new(patterns).unwrap_or_else(|error| {
        tracing::error!(%error, "invalid built-in retry pattern");
        RegexSet::empty()
    })
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::provider::types::{ApiProtocol, ProviderOrigin, Usage};

    fn error_with_code(text: &str, code: Option<&str>) -> AssistantMessage {
        AssistantMessage {
            content: vec![],
            model: "model".to_owned(),
            provider: "provider".to_owned(),
            origin: ProviderOrigin {
                provider_instance_id: "instance".to_owned(),
                protocol: ApiProtocol::OpenAiChatCompletions,
                model: "model".to_owned(),
            },
            usage: Usage::default(),
            stop_reason: StopReason::Error,
            error_message: Some(text.to_owned()),
            provider_code: code.map(str::to_owned),
            interrupted: false,
            timestamp: Utc::now(),
        }
    }

    fn error(text: &str) -> AssistantMessage {
        error_with_code(text, None)
    }

    #[test]
    fn classifies_provider_errors() {
        let retryable = [
            "provider overloaded",
            "rate limit",
            "Too many requests",
            "HTTP 429",
            "HTTP 500",
            "HTTP 502",
            "HTTP 503",
            "HTTP 504",
            "HTTP 524",
            "service unavailable",
            "internal error",
            "Provider returned error",
            "network error",
            "connection refused",
            "connection lost",
            "other side closed",
            "fetch failed",
            "upstream connect error",
            "reset before headers",
            "socket hang up",
            "timed out",
            "timeout",
            "websocket closed",
            "stream ended without finish_reason",
            "please retry your request",
            "ResourceExhausted",
        ];
        for text in retryable {
            assert!(is_retryable(&error(text)), "{text}");
        }

        let terminal = [
            "insufficient_quota",
            "out of budget",
            "quota exceeded",
            "billing disabled",
            "Monthly usage limit reached",
            "GoUsageLimitError 429",
            "invalid api key",
            "bad request",
        ];
        for text in terminal {
            assert!(!is_retryable(&error(text)), "{text}");
        }
    }

    #[test]
    fn backoff_is_bounded() {
        assert_eq!(retry_delay(0), Some(Duration::from_secs(2)));
        assert_eq!(retry_delay(1), Some(Duration::from_secs(4)));
        assert_eq!(retry_delay(2), Some(Duration::from_secs(8)));
        assert_eq!(retry_delay(3), None);
    }

    #[test]
    fn provider_code_precedes_conflicting_display_text() {
        assert!(is_retryable(&error_with_code(
            "billing quota exceeded",
            Some("network_error")
        )));
        assert!(is_retryable(&error_with_code(
            "maximum context length is 131072 tokens",
            Some("unexpected_sse_eof")
        )));
        for code in [
            "model_context_window_exceeded",
            "sensitive",
            "content_filter",
            "cancelled",
        ] {
            assert!(
                !is_retryable(&error_with_code("HTTP 503, retry", Some(code))),
                "{code}"
            );
        }
        for code in [
            "429",
            "503",
            "http_429",
            "http_503",
            "idle_timeout",
            "unexpected_sse_eof",
        ] {
            assert!(is_retryable(&error_with_code("", Some(code))), "{code}");
        }
        assert!(!is_retryable(&error_with_code(
            "insufficient_quota",
            Some("http_429")
        )));
    }

    #[test]
    fn native_provider_transient_codes_are_authoritative() {
        for (code, terse_display) in [("overloaded_error", "busy"), ("server_error", "failed")] {
            assert!(
                is_retryable(&error_with_code(terse_display, Some(code))),
                "{code}/{terse_display}"
            );
            assert!(
                is_retryable(&error_with_code("billing quota exceeded", Some(code))),
                "{code} must take precedence over display text"
            );

            let mut non_error = error_with_code(terse_display, Some(code));
            non_error.stop_reason = StopReason::Stop;
            assert!(
                !is_retryable(&non_error),
                "{code} must not override a non-error terminal"
            );
        }

        for (code, terse_display) in [("overloaded", "busy"), ("server_error_detail", "failed")] {
            assert!(
                !is_retryable(&error_with_code(terse_display, Some(code))),
                "near-match code {code} must not become retryable"
            );
        }
    }

    #[test]
    fn retries_only_the_exact_opencode_go_console_upstream_400() {
        const EXACT: &str = concat!(
            "400: {\"error\":{",
            "\"message\":\"Error from provider (Console Go): Upstream request failed\",",
            "\"type\":\"invalid_request_error\",",
            "\"param\":null,",
            "\"code\":\"invalid_request_error\"}}"
        );
        let mut message = error_with_code(EXACT, Some("http_400"));
        message.provider = "opencode-go".to_owned();
        assert!(is_retryable(&message));

        message.error_message = Some(
            concat!(
                "400: {\n  \"error\": {",
                "\n    \"code\": \"invalid_request_error\",",
                "\n    \"param\": null,",
                "\n    \"message\": \"Error from provider (Console Go): Upstream request failed\",",
                "\n    \"type\": \"invalid_request_error\"\n  }\n}"
            )
            .to_owned(),
        );
        assert!(
            is_retryable(&message),
            "JSON whitespace and key order are not semantic"
        );

        let exact = message.clone();
        let mut near_misses = Vec::new();

        let mut wrong_provider = exact.clone();
        wrong_provider.provider = "kimi".to_owned();
        near_misses.push(("provider", wrong_provider));

        let mut wrong_protocol = exact.clone();
        wrong_protocol.origin.protocol = ApiProtocol::OpenAiResponses;
        near_misses.push(("protocol", wrong_protocol));

        let mut wrong_status = exact.clone();
        wrong_status.provider_code = Some("http_401".to_owned());
        near_misses.push(("status", wrong_status));

        let mut raw_json_without_transport_status = exact.clone();
        raw_json_without_transport_status.error_message = raw_json_without_transport_status
            .error_message
            .as_deref()
            .and_then(|error| error.strip_prefix("400: "))
            .map(str::to_owned);
        near_misses.push(("transport prefix", raw_json_without_transport_status));

        for (field, replacement) in [
            (
                "message",
                "Error from provider (Console Go): Upstream request failed.",
            ),
            ("type", "server_error"),
            ("code", "server_error"),
        ] {
            let mut changed = exact.clone();
            let mut body: serde_json::Value = serde_json::from_str(
                changed
                    .error_message
                    .as_deref()
                    .expect("error")
                    .strip_prefix("400: ")
                    .expect("transport prefix"),
            )
            .expect("fixture JSON");
            body["error"][field] = serde_json::Value::String(replacement.to_owned());
            changed.error_message = Some(format!("400: {body}"));
            near_misses.push((field, changed));
        }

        let mut missing_param = exact.clone();
        let mut body: serde_json::Value = serde_json::from_str(
            missing_param
                .error_message
                .as_deref()
                .expect("error")
                .strip_prefix("400: ")
                .expect("transport prefix"),
        )
        .expect("fixture JSON");
        body["error"]
            .as_object_mut()
            .expect("error object")
            .remove("param");
        missing_param.error_message = Some(format!("400: {body}"));
        near_misses.push(("param", missing_param));

        let mut non_error = exact;
        non_error.stop_reason = StopReason::Stop;
        near_misses.push(("stop reason", non_error));

        for (difference, message) in near_misses {
            assert!(
                !is_retryable(&message),
                "near-match {difference} must remain terminal"
            );
        }
    }

    #[tokio::test]
    async fn cancellation_interrupts_sleep() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(!sleep_or_cancel(Duration::ZERO, &cancel).await);
        assert!(!sleep_or_cancel(Duration::from_secs(60), &cancel).await);
    }
}
