use std::sync::OnceLock;

use regex::RegexSet;

use super::types::{AssistantMessage, StopReason};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OverflowSource {
    ProviderCode,
    ErrorPattern,
    LengthUsage,
    StopUsage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OverflowClassification {
    ImmediateRecovery(OverflowSource),
    DeferredApply(OverflowSource),
}

pub fn classify_context_overflow(
    message: &AssistantMessage,
    context_window: Option<u64>,
) -> Option<OverflowClassification> {
    match message.provider_code.as_deref() {
        Some(
            "model_context_window_exceeded"
            | "context_length_exceeded"
            | "request_too_large"
            | "413"
            | "http_413",
        ) => {
            return Some(OverflowClassification::ImmediateRecovery(
                OverflowSource::ProviderCode,
            ));
        }
        // These machine-readable codes are authoritative even when the
        // display message contains broad fallback words such as "tokens".
        Some(code)
            if message.stop_reason == StopReason::Error
                && authoritative_non_overflow_code(code) =>
        {
            return None;
        }
        _ => {}
    }

    if message.stop_reason == StopReason::Error
        && let Some(error) = message.error_message.as_deref()
        && !non_overflow_patterns().is_match(error)
        && overflow_patterns().is_match(error)
    {
        return Some(OverflowClassification::ImmediateRecovery(
            OverflowSource::ErrorPattern,
        ));
    }

    let context_window = context_window?;
    let input = message
        .usage
        .input
        .saturating_add(message.usage.cache_read)
        .saturating_add(message.usage.cache_write);
    if message.stop_reason == StopReason::Length
        && message.usage.output == 0
        && input >= ninety_nine_percent_ceiling(context_window)
    {
        return Some(OverflowClassification::ImmediateRecovery(
            OverflowSource::LengthUsage,
        ));
    }
    if message.stop_reason == StopReason::Stop && input > context_window {
        return Some(OverflowClassification::DeferredApply(
            OverflowSource::StopUsage,
        ));
    }
    None
}

fn authoritative_non_overflow_code(code: &str) -> bool {
    matches!(
        code,
        "network_error"
            | "request_error"
            | "transport_error"
            | "idle_timeout"
            | "response_header_timeout"
            | "sensitive"
            | "content_filter"
            | "cancelled"
            | "invalid_provider_stream"
            | "429"
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

fn ninety_nine_percent_ceiling(value: u64) -> u64 {
    let scaled = u128::from(value) * 99;
    scaled.div_ceil(100) as u64
}

fn non_overflow_patterns() -> &'static RegexSet {
    static PATTERNS: OnceLock<RegexSet> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        compile_patterns([
            r"(?i)^(Throttling error|Service unavailable):",
            r"(?i)rate limit",
            r"(?i)too many requests",
        ])
    })
}

fn overflow_patterns() -> &'static RegexSet {
    static PATTERNS: OnceLock<RegexSet> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        compile_patterns([
            r"(?i)prompt is too long",
            r"(?i)request_too_large",
            r"(?i)input is too long for requested model",
            r"(?i)exceeds the context window",
            r"(?i)exceeds (the )?(model'?s )?maximum context length",
            r"(?i)input token count.*exceeds the maximum",
            r"(?i)maximum prompt length is \d+",
            r"(?i)reduce the length of the messages",
            r"(?i)maximum context length is \d+ tokens",
            r"(?i)exceeds (the )?maximum allowed input length",
            r"(?i)is longer than the model'?s context length",
            r"(?i)exceeds the limit of \d+",
            r"(?i)exceeds the available context size",
            r"(?i)greater than the context length",
            r"(?i)context window exceeds limit",
            r"(?i)exceeded model token limit",
            r"(?i)too large for model with \d+ maximum context length",
            r"(?i)prompt has [\d,]+ tokens?.*configured context size",
            r"(?i)model_context_window_exceeded",
            r"(?i)prompt too long; exceeded (max )?context length",
            r"(?i)context[_ ]length[_ ]exceeded",
            r"(?i)too many tokens",
            r"(?i)token limit exceeded",
            r"(?i)^4(00|13)\s*(status code)?\s*\(no body\)",
        ])
    })
}

fn compile_patterns<const N: usize>(patterns: [&str; N]) -> RegexSet {
    RegexSet::new(patterns).unwrap_or_else(|error| {
        tracing::error!(%error, "invalid built-in overflow pattern");
        RegexSet::empty()
    })
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::provider::types::{ApiProtocol, ProviderOrigin, Usage};

    fn message(
        reason: StopReason,
        code: Option<&str>,
        error: Option<&str>,
        usage: Usage,
    ) -> AssistantMessage {
        AssistantMessage {
            content: vec![],
            model: "model".to_owned(),
            provider: "provider".to_owned(),
            origin: ProviderOrigin {
                provider_instance_id: "instance".to_owned(),
                protocol: ApiProtocol::OpenAiChatCompletions,
                model: "model".to_owned(),
            },
            usage,
            stop_reason: reason,
            error_message: error.map(str::to_owned),
            provider_code: code.map(str::to_owned),
            interrupted: false,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn all_migrated_error_patterns_select_immediate_recovery() {
        let cases = [
            "prompt is too long: 200001 tokens",
            "request_too_large",
            "input is too long for requested model",
            "input exceeds the context window",
            "exceeds the model's maximum context length of 131072 tokens",
            "input token count 10 exceeds the maximum",
            "maximum prompt length is 131072",
            "Please reduce the length of the messages",
            "maximum context length is 131072 tokens",
            "exceeds the maximum allowed input length",
            "is longer than the model's context length",
            "exceeds the limit of 1000",
            "exceeds the available context size",
            "greater than the context length",
            "context window exceeds limit",
            "Your request exceeded model token limit",
            "too large for model with 100 maximum context length",
            "Prompt has 200,000 tokens, but the configured context size is 100,000 tokens",
            "model_context_window_exceeded",
            "prompt too long; exceeded max context length",
            "context_length_exceeded",
            "too many tokens",
            "token limit exceeded",
            "413 status code (no body)",
        ];
        for error in cases {
            assert_eq!(
                classify_context_overflow(
                    &message(StopReason::Error, None, Some(error), Usage::default()),
                    None,
                ),
                Some(OverflowClassification::ImmediateRecovery(
                    OverflowSource::ErrorPattern
                )),
                "{error}"
            );
        }
    }

    #[test]
    fn provider_code_precedes_display_message_patterns() {
        assert_eq!(
            classify_context_overflow(
                &message(
                    StopReason::Error,
                    Some("model_context_window_exceeded"),
                    Some("rate limit"),
                    Usage::default(),
                ),
                None,
            ),
            Some(OverflowClassification::ImmediateRecovery(
                OverflowSource::ProviderCode
            ))
        );
        for code in [
            "network_error",
            "sensitive",
            "content_filter",
            "http_429",
            "invalid_provider_stream",
        ] {
            assert_eq!(
                classify_context_overflow(
                    &message(
                        StopReason::Error,
                        Some(code),
                        Some("too many tokens"),
                        Usage::default(),
                    ),
                    None,
                ),
                None,
                "{code}"
            );
        }
        assert_eq!(
            classify_context_overflow(
                &message(
                    StopReason::Error,
                    Some("invalid_request_error"),
                    Some("maximum context length is 131072 tokens"),
                    Usage::default(),
                ),
                None,
            ),
            Some(OverflowClassification::ImmediateRecovery(
                OverflowSource::ErrorPattern
            ))
        );
        for code in [
            "context_length_exceeded",
            "request_too_large",
            "413",
            "http_413",
        ] {
            assert_eq!(
                classify_context_overflow(
                    &message(StopReason::Error, Some(code), None, Usage::default()),
                    None,
                ),
                Some(OverflowClassification::ImmediateRecovery(
                    OverflowSource::ProviderCode
                )),
                "{code}"
            );
        }
    }

    #[test]
    fn non_overflow_exclusions_precede_broad_patterns() {
        for error in [
            "rate limit: too many tokens",
            "too many requests: token limit exceeded",
            "Service unavailable: too many tokens",
            "Throttling error: too many tokens",
        ] {
            assert_eq!(
                classify_context_overflow(
                    &message(StopReason::Error, None, Some(error), Usage::default()),
                    None,
                ),
                None,
                "{error}"
            );
        }
    }

    #[test]
    fn usage_classification_includes_cache_write_and_recovery_timing() {
        let stop_usage = Usage {
            input: 80,
            cache_read: 10,
            cache_write: 11,
            ..Usage::default()
        };
        assert_eq!(
            classify_context_overflow(
                &message(StopReason::Stop, None, None, stop_usage),
                Some(100),
            ),
            Some(OverflowClassification::DeferredApply(
                OverflowSource::StopUsage
            ))
        );

        let length_usage = Usage {
            input: 80,
            cache_read: 10,
            cache_write: 9,
            output: 0,
            ..Usage::default()
        };
        assert_eq!(
            classify_context_overflow(
                &message(StopReason::Length, None, None, length_usage),
                Some(100),
            ),
            Some(OverflowClassification::ImmediateRecovery(
                OverflowSource::LengthUsage
            ))
        );
    }

    #[test]
    fn length_threshold_uses_ceiling_not_floor() {
        assert_eq!(ninety_nine_percent_ceiling(101), 100);
        let usage = Usage {
            input: 99,
            ..Usage::default()
        };
        assert_eq!(
            classify_context_overflow(&message(StopReason::Length, None, None, usage), Some(101),),
            None
        );
    }
}
