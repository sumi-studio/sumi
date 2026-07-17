//! 呼び出し順の契約: エージェントループ (T11) では必ず
//! `overflow::is_context_overflow` を先に判定してから `is_retryable` を
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
    let Some(error) = message.error_message.as_deref() else {
        return false;
    };
    if non_retryable_patterns().is_match(error) {
        return false;
    }
    retryable_patterns().is_match(error)
}

pub fn retry_delay(attempt: usize) -> Option<Duration> {
    (attempt < MAX_RETRIES).then(|| Duration::from_secs(2_u64.pow(attempt as u32 + 1)))
}

pub async fn sleep_or_cancel(delay: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(delay) => true,
        _ = cancel.cancelled() => false,
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
            r"(?i)provider.?returned.?error",
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
            r"(?i)ended without",
            r"(?i)stream ended before message_stop",
            r"(?i)http2 request did not get a response",
            r"(?i)retry delay",
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
    use crate::provider::types::{AssistantContent, Usage};

    fn error(text: &str) -> AssistantMessage {
        AssistantMessage {
            content: Vec::<AssistantContent>::new(),
            model: "model".to_owned(),
            provider: "provider".to_owned(),
            usage: Usage::default(),
            stop_reason: StopReason::Error,
            error_message: Some(text.to_owned()),
            interrupted: false,
            timestamp: Utc::now(),
        }
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

    #[tokio::test]
    async fn cancellation_interrupts_sleep() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(!sleep_or_cancel(Duration::from_secs(60), &cancel).await);
    }
}
