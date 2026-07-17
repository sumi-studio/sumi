use std::sync::OnceLock;

use regex::RegexSet;

use super::types::{AssistantMessage, StopReason};

pub fn is_context_overflow(message: &AssistantMessage, context_window: Option<u64>) -> bool {
    if message.stop_reason == StopReason::Error
        && let Some(error) = message.error_message.as_deref()
        && !non_overflow_patterns().is_match(error)
        && overflow_patterns().is_match(error)
    {
        return true;
    }

    let Some(context_window) = context_window else {
        return false;
    };
    let input = message.usage.input.saturating_add(message.usage.cache_read);
    (message.stop_reason == StopReason::Stop && input > context_window)
        || (message.stop_reason == StopReason::Length
            && message.usage.output == 0
            && input >= context_window.saturating_mul(99) / 100)
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
    use crate::provider::types::{AssistantContent, Usage};

    fn message(reason: StopReason, error: Option<&str>, usage: Usage) -> AssistantMessage {
        AssistantMessage {
            content: Vec::<AssistantContent>::new(),
            model: "model".to_owned(),
            provider: "provider".to_owned(),
            usage,
            stop_reason: reason,
            error_message: error.map(str::to_owned),
            interrupted: false,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn detects_error_patterns_without_confusing_throttling() {
        for error in [
            "Your request exceeded model token limit",
            "input exceeds the context window",
            "maximum context length is 131072 tokens",
            "context_length_exceeded",
            "too many tokens",
            "token limit exceeded",
            "Prompt has 200,000 tokens, but the configured context size is 100,000 tokens",
        ] {
            assert!(
                is_context_overflow(
                    &message(StopReason::Error, Some(error), Usage::default()),
                    None
                ),
                "{error}"
            );
        }
        for error in [
            "rate limit: too many tokens",
            "too many requests: token limit exceeded",
            "Service unavailable: too many tokens",
        ] {
            assert!(
                !is_context_overflow(
                    &message(StopReason::Error, Some(error), Usage::default()),
                    None
                ),
                "{error}"
            );
        }
    }

    #[test]
    fn detects_silent_and_length_overflow_from_usage() {
        let usage = Usage {
            input: 101,
            ..Usage::default()
        };
        assert!(is_context_overflow(
            &message(StopReason::Stop, None, usage),
            Some(100)
        ));
        let usage = Usage {
            input: 99,
            output: 0,
            ..Usage::default()
        };
        assert!(is_context_overflow(
            &message(StopReason::Length, None, usage),
            Some(100)
        ));
    }
}
