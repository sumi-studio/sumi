use chrono::{DateTime, Utc};
use serde_json::Value;

use super::{ModelSpec, RequestOptions, adapters, types::*};

fn timestamp() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-09-08T09:00:00.125+09:00")
        .unwrap()
        .with_timezone(&Utc)
}

fn payload_blocks(preset: &str, user: UserMessage) -> Vec<Value> {
    let spec = ModelSpec::preset(preset).unwrap();
    let context = PromptContext::new(
        String::new(),
        vec![],
        vec![ContextMessage::Synthetic {
            message: Message::User(user),
        }],
        vec![],
        vec![],
    );
    let options = RequestOptions::default();
    let (request, field) = match spec.protocol {
        ApiProtocol::OpenAiChatCompletions => (
            adapters::chat_completions::build_request(&spec, &context, &options).unwrap(),
            "messages",
        ),
        ApiProtocol::OpenAiResponses => (
            adapters::responses::build_request(&spec, &context, &options).unwrap(),
            "input",
        ),
        ApiProtocol::AnthropicMessages => (
            adapters::anthropic::build_request(&spec, &context, &options).unwrap(),
            "messages",
        ),
    };
    request[field]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "user")
        .unwrap()["content"]
        .as_array()
        .unwrap()
        .clone()
}

#[test]
fn all_provider_payloads_preserve_raw_content_after_utc_receipt_metadata() {
    let received_at = timestamp();
    let expected = "[Received 2026-09-08 00:00:00 UTC; approximately 1 second since the previous incoming message]";
    let user = UserMessage {
        incoming_timing: Some(IncomingEventTiming {
            previous_receipt: Some(IncomingEventReceipt {
                received_at: received_at - chrono::Duration::milliseconds(1250),
                command_seq: 17,
            }),
        }),
        content: vec![
            UserContent::Text {
                text: "Tomorrow at 9 am.\nDo not change this text.".into(),
            },
            UserContent::Image {
                data: "aW1hZ2U=".into(),
                mime_type: "image/png".into(),
            },
        ],
        timestamp: received_at,
    };
    for preset in ["kimi-k3", "openai-responses", "anthropic"] {
        let blocks = payload_blocks(preset, user.clone());
        assert_eq!(blocks[0]["text"], expected, "{preset}");
        let mut without_timing = user.clone();
        without_timing.incoming_timing = None;
        assert_eq!(blocks[1..], payload_blocks(preset, without_timing));
    }
    let restored: UserMessage =
        serde_json::from_value(serde_json::to_value(&user).unwrap()).unwrap();
    assert_eq!(restored, user);
    assert_eq!(restored.incoming_timing_text().as_deref(), Some(expected));
}

#[test]
fn first_receipt_has_no_invented_interval_and_synthetic_user_has_no_prefix() {
    let mut user = UserMessage {
        incoming_timing: Some(IncomingEventTiming {
            previous_receipt: None,
        }),
        content: vec![UserContent::Text {
            text: "Directive".into(),
        }],
        timestamp: timestamp(),
    };
    for preset in ["kimi-k3", "openai-responses", "anthropic"] {
        let blocks = payload_blocks(preset, user.clone());
        assert_eq!(blocks[0]["text"], "[Received 2026-09-08 00:00:00 UTC]");
    }
    user.incoming_timing = None;
    for preset in ["kimi-k3", "openai-responses", "anthropic"] {
        let blocks = payload_blocks(preset, user.clone());
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["text"], "Directive");
    }
    assert!(
        serde_json::to_value(user)
            .unwrap()
            .get("incoming_timing")
            .is_none()
    );
}

#[test]
fn receipt_intervals_are_readable_without_hiding_clock_regressions() {
    for (delta, expected) in [
        (
            chrono::Duration::milliseconds(16_926),
            "approximately 17 seconds since the previous incoming message",
        ),
        (
            chrono::Duration::seconds(125),
            "2 minutes 5 seconds since the previous incoming message",
        ),
        (
            chrono::Duration::seconds(7_380),
            "2 hours 3 minutes since the previous incoming message",
        ),
        (
            chrono::Duration::seconds(7_384),
            "approximately 2 hours 3 minutes since the previous incoming message",
        ),
        (
            chrono::Duration::hours(51),
            "2 days 3 hours since the previous incoming message",
        ),
        (
            chrono::Duration::nanoseconds(1),
            "less than a second since the previous incoming message",
        ),
        (
            chrono::Duration::nanoseconds(-1),
            "receipt clock is less than a second earlier than the previous receipt",
        ),
        (
            chrono::Duration::milliseconds(-1_600),
            "receipt clock is approximately 2 seconds earlier than the previous receipt",
        ),
        (
            chrono::Duration::zero(),
            "same receipt time as the previous incoming message",
        ),
    ] {
        let user = UserMessage {
            incoming_timing: Some(IncomingEventTiming {
                previous_receipt: Some(IncomingEventReceipt {
                    received_at: timestamp() - delta,
                    command_seq: 18,
                }),
            }),
            content: vec![],
            timestamp: timestamp(),
        };
        let rendered = user.incoming_timing_text().unwrap();
        assert_eq!(
            rendered,
            format!("[Received 2026-09-08 00:00:00 UTC; {expected}]")
        );
        assert!(!rendered.contains("command_seq") && !rendered.contains("delta_ms"));
    }
}
