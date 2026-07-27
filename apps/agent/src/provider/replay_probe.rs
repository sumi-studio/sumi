use serde_json::Value;
use thiserror::Error;
use zeroize::Zeroizing;

use super::{
    adapters::{anthropic, responses},
    canonical_request::CanonicalRequestBody,
    model::{ModelSpec, ProtocolCompat},
    types::{ApiProtocol, NativeCompactionCoverage, ProviderContextPayload},
};

pub(crate) const REPLAY_PROBE_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
enum ReplayProbeKind {
    ResponsesEncryptedReasoning,
    AnthropicThinkingSignature,
    AnthropicRedactedThinking,
    ResponsesNativeCanonicalWindow,
    AnthropicNativeCanonicalWindow,
}

/// Provider-owned V1 replay measurement. Its private fields prevent callers
/// from supplying request shapes, sentinel positions, or pre-serialized body
/// pairs.
#[derive(Clone, Debug)]
pub(crate) struct ReplayProbeV1 {
    kind: ReplayProbeKind,
    fragment: Option<Value>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReplayProbeResult {
    SerializedDelta { replay_wire_bytes: u64 },
    NativeCanonicalWindow,
}

#[derive(Debug, Error)]
pub(crate) enum ReplayProbeError {
    #[error("ReplayProbeV1 protocol/model-family/payload mismatch: {0}")]
    Validation(String),
    #[error("ReplayProbeV1 canonical request serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("ReplayProbeV1 body with fragment was shorter than body without fragment")]
    NegativeDelta,
    #[error("ReplayProbeV1 byte length cannot be represented as u64")]
    LengthOverflow,
}

impl ReplayProbeV1 {
    pub(crate) fn new(
        spec: &ModelSpec,
        payload: &ProviderContextPayload,
    ) -> Result<Self, ReplayProbeError> {
        match payload {
            ProviderContextPayload::EncryptedReasoning { protocol, item } => {
                if *protocol != spec.protocol {
                    return Err(ReplayProbeError::Validation(
                        "encrypted reasoning protocol does not match target model".into(),
                    ));
                }
                match (&spec.protocol, &spec.compat) {
                    (ApiProtocol::OpenAiResponses, ProtocolCompat::Responses(compat))
                        if spec.reasoning && compat.supports_encrypted_reasoning =>
                    {
                        // The adapter's production conversion validates the item below.
                        Ok(Self {
                            kind: ReplayProbeKind::ResponsesEncryptedReasoning,
                            fragment: Some(item.clone()),
                        })
                    }
                    (ApiProtocol::AnthropicMessages, ProtocolCompat::Anthropic(_))
                        if spec.reasoning =>
                    {
                        let kind = match item.get("type").and_then(Value::as_str) {
                            Some("thinking_signature") => {
                                ReplayProbeKind::AnthropicThinkingSignature
                            }
                            Some("redacted_thinking") => {
                                ReplayProbeKind::AnthropicRedactedThinking
                            }
                            _ => {
                                return Err(ReplayProbeError::Validation(
                                    "unsupported Anthropic reasoning kind".into(),
                                ));
                            }
                        };
                        Ok(Self {
                            kind,
                            fragment: Some(item.clone()),
                        })
                    }
                    (ApiProtocol::OpenAiChatCompletions, ProtocolCompat::Chat(_)) => Err(
                        ReplayProbeError::Validation(
                            "Chat plaintext Thinking is public transcript content, not an opaque replay fragment"
                                .into(),
                        ),
                    ),
                    _ => Err(ReplayProbeError::Validation(
                        "target model does not support this opaque reasoning family".into(),
                    )),
                }
            }
            ProviderContextPayload::OpenAiCompactedWindow { items, coverage } => {
                ensure_coverage(coverage)?;
                match (&spec.protocol, &spec.compat) {
                    (ApiProtocol::OpenAiResponses, ProtocolCompat::Responses(compat))
                        if compat.supports_native_compact =>
                    {
                        responses::validate_replay_native_items(items)
                            .map_err(|error| ReplayProbeError::Validation(error.to_string()))?;
                        Ok(Self {
                            kind: ReplayProbeKind::ResponsesNativeCanonicalWindow,
                            fragment: None,
                        })
                    }
                    _ => Err(ReplayProbeError::Validation(
                        "OpenAI compacted window requires a native-capable Responses model".into(),
                    )),
                }
            }
            ProviderContextPayload::AnthropicCompaction { block, coverage } => {
                ensure_coverage(coverage)?;
                match (&spec.protocol, &spec.compat) {
                    (ApiProtocol::AnthropicMessages, ProtocolCompat::Anthropic(compat))
                        if compat.supports_native_compact =>
                    {
                        anthropic::validate_replay_native_block(block)
                            .map_err(|error| ReplayProbeError::Validation(error.to_string()))?;
                        Ok(Self {
                            kind: ReplayProbeKind::AnthropicNativeCanonicalWindow,
                            fragment: None,
                        })
                    }
                    _ => Err(ReplayProbeError::Validation(
                        "Anthropic compaction requires a native-capable Anthropic model".into(),
                    )),
                }
            }
        }
    }

    pub(crate) fn measure(&self) -> Result<ReplayProbeResult, ReplayProbeError> {
        match self.kind {
            ReplayProbeKind::ResponsesNativeCanonicalWindow
            | ReplayProbeKind::AnthropicNativeCanonicalWindow => {
                Ok(ReplayProbeResult::NativeCanonicalWindow)
            }
            ReplayProbeKind::ResponsesEncryptedReasoning => measure_bodies(
                &responses::build_replay_probe_request(None)
                    .map_err(|error| ReplayProbeError::Validation(error.to_string()))?,
                &responses::build_replay_probe_request(self.fragment.as_ref())
                    .map_err(|error| ReplayProbeError::Validation(error.to_string()))?,
            ),
            ReplayProbeKind::AnthropicThinkingSignature
            | ReplayProbeKind::AnthropicRedactedThinking => measure_bodies(
                &anthropic::build_replay_probe_request(None)
                    .map_err(|error| ReplayProbeError::Validation(error.to_string()))?,
                &anthropic::build_replay_probe_request(self.fragment.as_ref())
                    .map_err(|error| ReplayProbeError::Validation(error.to_string()))?,
            ),
        }
    }
}

fn ensure_coverage(coverage: &NativeCompactionCoverage) -> Result<(), ReplayProbeError> {
    if coverage.through_message_seq == 0 || coverage.context_fingerprint.is_empty() {
        return Err(ReplayProbeError::Validation(
            "native canonical window coverage is incomplete".into(),
        ));
    }
    Ok(())
}

fn measure_bodies<T: serde::Serialize, U: serde::Serialize>(
    without: &T,
    with: &U,
) -> Result<ReplayProbeResult, ReplayProbeError> {
    let _ = CanonicalRequestBody::serialize(&serde_json::Value::Null)
        .map(|body| body.len())
        .unwrap_or(0);
    let without = Zeroizing::new(serde_json::to_vec(without)?);
    let with = Zeroizing::new(serde_json::to_vec(with)?);
    checked_delta(without.len(), with.len())
}

fn checked_delta(
    without_len: usize,
    with_len: usize,
) -> Result<ReplayProbeResult, ReplayProbeError> {
    let delta = with_len
        .checked_sub(without_len)
        .ok_or(ReplayProbeError::NegativeDelta)?;
    let replay_wire_bytes = u64::try_from(delta).map_err(|_| ReplayProbeError::LengthOverflow)?;
    Ok(ReplayProbeResult::SerializedDelta { replay_wire_bytes })
}

#[cfg(test)]
mod tests {
    use serde::ser::{Error as _, Serializer};
    use serde_json::json;
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::provider::canonical_request::CanonicalRequestBody;

    struct SerializationFailure;

    impl serde::Serialize for SerializationFailure {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            Err(S::Error::custom("intentional test failure"))
        }
    }

    fn coverage() -> NativeCompactionCoverage {
        NativeCompactionCoverage {
            through_message_seq: 1,
            context_fingerprint: "replay-probe-v1".into(),
        }
    }

    #[test]
    fn special_characters_are_measured_after_canonical_json_escaping() {
        let spec = ModelSpec::preset("openai-responses").expect("preset");
        for encrypted_content in [
            "quote:\" backslash:\\ newline:\n",
            "日本語と絵文字🦀",
            "YWJjZGVmZ2hpamtsbW5vcA==",
        ] {
            let payload = ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::OpenAiResponses,
                item: json!({
                    "type":"reasoning",
                    "id":"rs_replay_probe_v1_fragment",
                    "encrypted_content":encrypted_content,
                    "summary":[],
                }),
            };
            assert!(matches!(
                ReplayProbeV1::new(&spec, &payload)
                    .and_then(|probe| probe.measure()),
                Ok(ReplayProbeResult::SerializedDelta { replay_wire_bytes })
                    if replay_wire_bytes > 0
            ));
        }

        let anthropic = ModelSpec::preset("anthropic").expect("preset");
        for item in [
            json!({
                "type":"thinking_signature",
                "signature":"quote:\" backslash:\\ newline:\n 日本語 YWJjZA==",
            }),
            json!({
                "type":"redacted_thinking",
                "data":"quote:\" backslash:\\ newline:\n 日本語 YWJjZA==",
            }),
        ] {
            let payload = ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::AnthropicMessages,
                item,
            };
            assert!(matches!(
                ReplayProbeV1::new(&anthropic, &payload)
                    .and_then(|probe| probe.measure()),
                Ok(ReplayProbeResult::SerializedDelta { replay_wire_bytes })
                    if replay_wire_bytes > 0
            ));
        }
    }

    #[test]
    fn rejects_chat_mismatch_and_malformed_reasoning() {
        let chat = ModelSpec::preset("kimi-k3").expect("preset");
        let responses = ModelSpec::preset("openai-responses").expect("preset");
        let response_payload = ProviderContextPayload::EncryptedReasoning {
            protocol: ApiProtocol::OpenAiResponses,
            item: json!({"type":"reasoning","encrypted_content":"opaque"}),
        };
        assert!(ReplayProbeV1::new(&chat, &response_payload).is_err());
        let chat_plaintext_misclassified_as_opaque = ProviderContextPayload::EncryptedReasoning {
            protocol: ApiProtocol::OpenAiChatCompletions,
            item: json!({"type":"reasoning_content","content":"public"}),
        };
        assert!(ReplayProbeV1::new(&chat, &chat_plaintext_misclassified_as_opaque).is_err());
        let malformed = ProviderContextPayload::EncryptedReasoning {
            protocol: ApiProtocol::OpenAiResponses,
            item: json!({"type":"reasoning","encrypted_content":""}),
        };
        assert!(
            ReplayProbeV1::new(&responses, &malformed)
                .and_then(|probe| probe.measure())
                .is_err()
        );

        let anthropic = ModelSpec::preset("anthropic").expect("preset");
        let malformed = ProviderContextPayload::EncryptedReasoning {
            protocol: ApiProtocol::AnthropicMessages,
            item: json!({"type":"thinking_signature","signature":""}),
        };
        assert!(
            ReplayProbeV1::new(&anthropic, &malformed)
                .and_then(|probe| probe.measure())
                .is_err()
        );
    }

    #[test]
    fn native_windows_return_the_typed_zero_case() {
        let responses = ProviderContextPayload::OpenAiCompactedWindow {
            items: vec![json!({"type":"message","role":"assistant","content":[]})],
            coverage: coverage(),
        };
        let anthropic = ProviderContextPayload::AnthropicCompaction {
            block: json!({"type":"compaction","content":"opaque"}),
            coverage: coverage(),
        };
        for (spec, payload) in [
            (
                ModelSpec::preset("openai-responses").expect("preset"),
                responses,
            ),
            (ModelSpec::preset("anthropic").expect("preset"), anthropic),
        ] {
            assert_eq!(
                ReplayProbeV1::new(&spec, &payload)
                    .and_then(|probe| probe.measure())
                    .expect("native probe"),
                ReplayProbeResult::NativeCanonicalWindow
            );
        }
    }

    #[test]
    fn replay_probe_request_bytes_are_usage_independent() {
        let usage = super::super::types::Usage {
            input: 1,
            output: 2,
            cache_read: 3,
            cache_write: 4,
            reasoning: 5,
            total_tokens: 15,
        };
        for (baseline, with_usage) in [
            (
                responses::build_replay_probe_request(None).expect("Responses baseline"),
                responses::build_replay_probe_request_for_usage_test(None, usage.clone())
                    .expect("Responses usage"),
            ),
            (
                anthropic::build_replay_probe_request(None).expect("Anthropic baseline"),
                anthropic::build_replay_probe_request_for_usage_test(None, usage)
                    .expect("Anthropic usage"),
            ),
        ] {
            assert_eq!(
                CanonicalRequestBody::serialize(&baseline)
                    .expect("baseline bytes")
                    .as_bytes(),
                CanonicalRequestBody::serialize(&with_usage)
                    .expect("usage bytes")
                    .as_bytes()
            );
        }
    }

    #[test]
    fn internal_seams_fail_closed_for_negative_delta_and_serializer_error() {
        assert!(matches!(
            checked_delta(2, 1),
            Err(ReplayProbeError::NegativeDelta)
        ));
        assert!(matches!(
            measure_bodies(&SerializationFailure, &json!({})),
            Err(ReplayProbeError::Serialization(_))
        ));
    }

    fn sha256(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn serialized_golden_case(
        protocol: &str,
        model_family: &str,
        kind: &str,
        fragment_slot: &str,
        without: Value,
        with: Value,
    ) -> Value {
        let without = CanonicalRequestBody::serialize(&without).expect("without body");
        let with = CanonicalRequestBody::serialize(&with).expect("with body");
        let delta = with
            .len()
            .checked_sub(without.len())
            .expect("positive delta") as u64;
        json!({
            "protocol":protocol,
            "model_family":model_family,
            "kind":kind,
            "sentinel_position":"immediately_before_fragment",
            "fragment_slot":fragment_slot,
            "without_body_sha256":sha256(without.as_bytes()),
            "with_body_sha256":sha256(with.as_bytes()),
            "without_body_bytes":without.len(),
            "with_body_bytes":with.len(),
            "replay_wire_bytes":delta,
            "expected_t19_token_ceiling":delta.div_ceil(4),
        })
    }

    fn golden_contract() -> Value {
        let responses_item = json!({
            "type":"reasoning",
            "id":"rs_replay_probe_v1_fragment",
            "encrypted_content":"quote:\" backslash:\\ newline:\n 日本語 YWJjZA==",
            "summary":[],
        });
        let signature = json!({
            "type":"thinking_signature",
            "signature":"quote:\" backslash:\\ newline:\n 日本語 YWJjZA==",
        });
        let redacted = json!({
            "type":"redacted_thinking",
            "data":"quote:\" backslash:\\ newline:\n 日本語 YWJjZA==",
        });
        let cases = vec![
            serialized_golden_case(
                "open_ai_responses",
                "responses_encrypted_reasoning",
                "encrypted_reasoning",
                "input[1]",
                responses::build_replay_probe_request(None).expect("without"),
                responses::build_replay_probe_request(Some(&responses_item)).expect("with"),
            ),
            serialized_golden_case(
                "anthropic_messages",
                "anthropic_thinking",
                "thinking_signature",
                "messages[1].content[1]",
                anthropic::build_replay_probe_request(None).expect("without"),
                anthropic::build_replay_probe_request(Some(&signature)).expect("with"),
            ),
            serialized_golden_case(
                "anthropic_messages",
                "anthropic_thinking",
                "redacted_thinking",
                "messages[1].content[1]",
                anthropic::build_replay_probe_request(None).expect("without"),
                anthropic::build_replay_probe_request(Some(&redacted)).expect("with"),
            ),
            json!({
                "protocol":"open_ai_responses",
                "model_family":"responses_native_compaction",
                "kind":"open_ai_compacted_window",
                "sentinel_position":null,
                "fragment_slot":"native_canonical_window",
                "replay_wire_bytes":null,
                "expected_t19_token_ceiling":0,
                "result":"native_canonical_window",
            }),
            json!({
                "protocol":"anthropic_messages",
                "model_family":"anthropic_native_compaction",
                "kind":"anthropic_compaction",
                "sentinel_position":null,
                "fragment_slot":"native_canonical_window",
                "replay_wire_bytes":null,
                "expected_t19_token_ceiling":0,
                "result":"native_canonical_window",
            }),
        ];
        json!({
            "contract":"ReplayProbeV1",
            "eviction_estimator_version":REPLAY_PROBE_VERSION,
            "cases":cases,
        })
    }

    #[test]
    fn replay_probe_v1_matches_versioned_contract_golden() {
        let expected: Value =
            serde_json::from_str(include_str!("../../tests/snapshots/replay_probe_v1.json"))
                .expect("ReplayProbeV1 golden JSON");
        assert_eq!(golden_contract(), expected);

        for (spec, payload, expected_bytes) in [
            (
                ModelSpec::preset("openai-responses").expect("preset"),
                ProviderContextPayload::EncryptedReasoning {
                    protocol: ApiProtocol::OpenAiResponses,
                    item: json!({
                        "type":"reasoning",
                        "id":"rs_replay_probe_v1_fragment",
                        "encrypted_content":"quote:\" backslash:\\ newline:\n 日本語 YWJjZA==",
                        "summary":[],
                    }),
                },
                143,
            ),
            (
                ModelSpec::preset("anthropic").expect("preset"),
                ProviderContextPayload::EncryptedReasoning {
                    protocol: ApiProtocol::AnthropicMessages,
                    item: json!({
                        "type":"thinking_signature",
                        "signature":"quote:\" backslash:\\ newline:\n 日本語 YWJjZA==",
                    }),
                },
                124,
            ),
            (
                ModelSpec::preset("anthropic").expect("preset"),
                ProviderContextPayload::EncryptedReasoning {
                    protocol: ApiProtocol::AnthropicMessages,
                    item: json!({
                        "type":"redacted_thinking",
                        "data":"quote:\" backslash:\\ newline:\n 日本語 YWJjZA==",
                    }),
                },
                90,
            ),
        ] {
            assert_eq!(
                ReplayProbeV1::new(&spec, &payload)
                    .and_then(|probe| probe.measure())
                    .expect("golden probe"),
                ReplayProbeResult::SerializedDelta {
                    replay_wire_bytes: expected_bytes,
                }
            );
        }
    }
}
