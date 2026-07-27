//! Public-message estimation and versioned replay-footprint accounting.

use std::borrow::Borrow;

use crate::provider::{
    model::ModelSpec,
    replay_probe::{ReplayProbeResult, ReplayProbeV1},
    types::{ProviderContextPayload, PublicAssistantContent, PublicMessage, Usage, UserContent},
};
use thiserror::Error;
use zeroize::Zeroizing;

/// Legacy estimator that used the serialized plaintext byte length divided
/// by four with ceiling. This predates the provider-specific `ReplayProbeV1`
/// contract and is only accepted for backward-compatible hydration.
pub const EVICTION_ESTIMATOR_VERSION_SERIALIZED_BYTES: u32 = 1;

/// The estimator version for the `ReplayProbeV1` wire-byte accounting
/// contract. It is intentionally distinct from `SERIALIZED_BYTES` so the two
/// formulas cannot be silently reinterpreted.
pub const EVICTION_ESTIMATOR_VERSION_REPLAY_PROBE_V1: u32 =
    crate::provider::replay_probe::REPLAY_PROBE_EVICTION_ESTIMATOR_VERSION;

const NO_TOOL_OUTPUT_PLACEHOLDER: &str = "(no tool output)";
const TOOL_RESULT_IMAGE_PLACEHOLDER: &str = "(see attached image)";

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TokenCalibration {
    ratio: f64,
}

impl Default for TokenCalibration {
    fn default() -> Self {
        Self { ratio: 1.0 }
    }
}

impl TokenCalibration {
    pub fn new(ratio: f64) -> Result<Self, EstimateError> {
        validate_positive_finite(ratio)?;
        Ok(Self { ratio })
    }

    pub fn ratio(self) -> f64 {
        self.ratio
    }

    /// Update the EMA from an uncalibrated estimate and one measured prompt.
    /// Callers must pass the raw estimate; calibrated values are never fed
    /// back into this function.
    pub fn update_ema(
        &mut self,
        observed_prompt_tokens: u64,
        uncalibrated_prompt_estimate: u64,
        alpha: f64,
    ) -> Result<(), EstimateError> {
        if uncalibrated_prompt_estimate == 0 {
            return Err(EstimateError::ZeroEstimatedTokens);
        }
        if !(alpha.is_finite() && 0.0 < alpha && alpha <= 1.0) {
            return Err(EstimateError::InvalidEmaAlpha);
        }
        let observed_ratio = observed_prompt_tokens as f64 / uncalibrated_prompt_estimate as f64;
        validate_positive_finite(observed_ratio)?;
        let updated = alpha.mul_add(observed_ratio, (1.0 - alpha) * self.ratio);
        validate_positive_finite(updated)?;
        self.ratio = updated;
        Ok(())
    }

    /// Apply calibration exactly once to the saved public estimate plus saved
    /// footprints.  Individual persisted footprint values are intentionally
    /// never calibrated or recomputed.
    pub fn effective_tokens(
        self,
        public_estimate: u64,
        eviction_footprint: u64,
    ) -> Result<u64, EstimateError> {
        let raw = public_estimate
            .checked_add(eviction_footprint)
            .ok_or(EstimateError::ArithmeticOverflow)?;
        if self.ratio == 1.0 || raw == 0 {
            return Ok(raw);
        }
        let effective = raw as f64 * self.ratio;
        // A conversion at or above 2^64 cannot be represented as u64.  The
        // conservative rejection also handles f64's coarse precision near
        // u64::MAX without silently wrapping.
        if !effective.is_finite() || effective >= u64::MAX as f64 {
            return Err(EstimateError::ArithmeticOverflow);
        }
        Ok(effective.ceil() as u64)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EvictionFootprint {
    estimator_version: u32,
    replay_wire_bytes: u64,
    eviction_tokens: u64,
}

impl EvictionFootprint {
    pub fn estimator_version(self) -> u32 {
        self.estimator_version
    }

    pub fn replay_wire_bytes(self) -> u64 {
        self.replay_wire_bytes
    }

    pub fn eviction_tokens(self) -> u64 {
        self.eviction_tokens
    }

    /// Restore a previously persisted value without recalculating it.  This
    /// is crate-only: callers cannot manufacture a footprint from arbitrary
    /// serialized bytes through the memory API.
    pub(crate) fn from_saved(
        estimator_version: u32,
        replay_wire_bytes: u64,
        eviction_tokens: u64,
    ) -> Result<Self, EstimateError> {
        if estimator_version == 0 {
            return Err(EstimateError::InvalidEstimatorVersion);
        }
        Ok(Self {
            estimator_version,
            replay_wire_bytes,
            eviction_tokens,
        })
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum EstimateError {
    #[error("token estimate overflowed u64")]
    ArithmeticOverflow,
    #[error("calibration ratio must be finite and positive")]
    InvalidCalibrationRatio,
    #[error("EMA alpha must be finite and in (0, 1]")]
    InvalidEmaAlpha,
    #[error("cannot calibrate against a zero token estimate")]
    ZeroEstimatedTokens,
    #[error("eviction estimator version must be at least 1")]
    InvalidEstimatorVersion,
    #[error("public message serialization failed: {0}")]
    SerializerFailure(String),
    #[error("ReplayProbeV1 failed closed: {0}")]
    ReplayProbeFailure(String),
}

pub fn observed_prompt_tokens(usage: &Usage) -> Result<u64, EstimateError> {
    usage
        .input
        .checked_add(usage.cache_read)
        .and_then(|tokens| tokens.checked_add(usage.cache_write))
        .ok_or(EstimateError::ArithmeticOverflow)
}

/// ASCII/4 + non-ASCII/1.5, represented with denominator 12 and rounded up
/// once after checked integer arithmetic.
pub fn estimate_text_tokens(text: &str) -> Result<u64, EstimateError> {
    let (mut ascii, mut non_ascii) = (0_u64, 0_u64);
    for character in text.chars() {
        let counter = if character.is_ascii() {
            &mut ascii
        } else {
            &mut non_ascii
        };
        *counter = counter
            .checked_add(1)
            .ok_or(EstimateError::ArithmeticOverflow)?;
    }
    let numerator = ascii
        .checked_mul(3)
        .and_then(|ascii_part| {
            non_ascii
                .checked_mul(8)
                .and_then(|part| ascii_part.checked_add(part))
        })
        .ok_or(EstimateError::ArithmeticOverflow)?;
    Ok(numerator.div_ceil(12))
}

/// Estimate all public transcript fields that contribute textual prompt
/// content.  Opaque provider context is intentionally absent from this API.
pub fn estimate_public_message(message: &PublicMessage) -> Result<u64, EstimateError> {
    match message {
        PublicMessage::User(message) => message.content.iter().try_fold(0, add_user_content),
        PublicMessage::Assistant(message) => {
            message.content.iter().try_fold(0_u64, |total, content| {
                let estimate = match content {
                    PublicAssistantContent::Text { text, .. }
                    | PublicAssistantContent::Thinking { thinking: text, .. } => {
                        estimate_text_tokens(text)
                    }
                    PublicAssistantContent::ToolCall { tool_call, .. } => {
                        let arguments = serde_json::to_string(tool_call.arguments.as_object())
                            .map_err(|error| EstimateError::SerializerFailure(error.to_string()))?;
                        checked_sum([
                            estimate_text_tokens(&tool_call.id)?,
                            estimate_text_tokens(&tool_call.name)?,
                            estimate_text_tokens(&arguments)?,
                        ])
                    }
                    PublicAssistantContent::RejectedToolCall { rejected, .. } => checked_sum([
                        estimate_text_tokens(&rejected.id)?,
                        estimate_text_tokens(&rejected.name)?,
                    ]),
                }?;
                total
                    .checked_add(estimate)
                    .ok_or(EstimateError::ArithmeticOverflow)
            })
        }
        PublicMessage::ToolResult(message) => checked_sum([
            estimate_tool_result_content(&message.content)?,
            estimate_text_tokens(&message.tool_call_id)?,
        ]),
    }
}

pub fn estimate_public_messages<I, M>(messages: I) -> Result<u64, EstimateError>
where
    I: IntoIterator<Item = M>,
    M: Borrow<PublicMessage>,
{
    messages.into_iter().try_fold(0_u64, |total, message| {
        let current = estimate_public_message(message.borrow())?;
        total
            .checked_add(current)
            .ok_or(EstimateError::ArithmeticOverflow)
    })
}

fn add_user_content(total: u64, content: &UserContent) -> Result<u64, EstimateError> {
    let current = match content {
        UserContent::Text { text } => estimate_text_tokens(text)?,
        UserContent::Image { data, mime_type } => checked_sum([
            estimate_text_tokens(data)?,
            estimate_text_tokens(mime_type)?,
        ])?,
    };
    total
        .checked_add(current)
        .ok_or(EstimateError::ArithmeticOverflow)
}

fn estimate_tool_result_content(content: &[UserContent]) -> Result<u64, EstimateError> {
    let text = content
        .iter()
        .filter_map(|item| match item {
            UserContent::Text { text } => Some(text.as_str()),
            UserContent::Image { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    let images = content.iter().try_fold(0_u64, |total, item| match item {
        UserContent::Text { .. } => Ok(total),
        UserContent::Image { .. } => add_user_content(total, item),
    })?;
    let has_images = content
        .iter()
        .any(|item| matches!(item, UserContent::Image { .. }));
    let placeholder = if !text.is_empty() {
        0
    } else if has_images {
        estimate_text_tokens(TOOL_RESULT_IMAGE_PLACEHOLDER)?
    } else {
        estimate_text_tokens(NO_TOOL_OUTPUT_PLACEHOLDER)?
    };
    checked_sum([estimate_text_tokens(&text)?, images, placeholder])
}

fn checked_sum(values: impl IntoIterator<Item = u64>) -> Result<u64, EstimateError> {
    values.into_iter().try_fold(0_u64, |total, value| {
        total
            .checked_add(value)
            .ok_or(EstimateError::ArithmeticOverflow)
    })
}

/// Consume the provider-owned probe and its typed native-window zero result.
/// Probe construction, payload validation, request shape, and serialization
/// remain provider responsibilities.
pub(crate) fn eviction_footprint_v1(
    probe: &ReplayProbeV1,
) -> Result<EvictionFootprint, EstimateError> {
    match probe
        .measure()
        .map_err(|error| EstimateError::ReplayProbeFailure(error.to_string()))?
    {
        ReplayProbeResult::SerializedDelta { replay_wire_bytes } => Ok(EvictionFootprint {
            estimator_version: EVICTION_ESTIMATOR_VERSION_REPLAY_PROBE_V1,
            replay_wire_bytes,
            eviction_tokens: replay_wire_bytes.div_ceil(4),
        }),
        ReplayProbeResult::NativeCanonicalWindow => Ok(native_canonical_window_footprint()),
    }
}

pub(crate) fn native_canonical_window_footprint() -> EvictionFootprint {
    EvictionFootprint {
        estimator_version: EVICTION_ESTIMATOR_VERSION_REPLAY_PROBE_V1,
        replay_wire_bytes: 0,
        eviction_tokens: 0,
    }
}

/// Compute the exact legacy serialized-payload/4 eviction footprint used by
/// current main before the `ReplayProbeV1` estimator. Legacy encrypted
/// reasoning measured only its opaque JSON item, not the surrounding
/// `ProviderContextItem`; native windows were a typed zero.
pub(crate) fn legacy_serialized_bytes_eviction_footprint(
    payload: &ProviderContextPayload,
) -> Result<EvictionFootprint, EstimateError> {
    match payload {
        ProviderContextPayload::EncryptedReasoning { item, .. } => {
            let serialized = Zeroizing::new(
                serde_json::to_vec(item)
                    .map_err(|error| EstimateError::SerializerFailure(error.to_string()))?,
            );
            let replay_wire_bytes =
                u64::try_from(serialized.len()).map_err(|_| EstimateError::ArithmeticOverflow)?;
            Ok(EvictionFootprint {
                estimator_version: EVICTION_ESTIMATOR_VERSION_SERIALIZED_BYTES,
                replay_wire_bytes,
                eviction_tokens: replay_wire_bytes.div_ceil(4),
            })
        }
        ProviderContextPayload::OpenAiCompactedWindow { .. }
        | ProviderContextPayload::AnthropicCompaction { .. } => Ok(EvictionFootprint {
            estimator_version: EVICTION_ESTIMATOR_VERSION_SERIALIZED_BYTES,
            replay_wire_bytes: 0,
            eviction_tokens: 0,
        }),
    }
}

/// Compute the V1 eviction footprint for a provider-context payload using the
/// authoritative `ReplayProbeV1` and the canonical request serializer for the
/// given `ModelSpec`. This is the only durable footprinting entry point for
/// opaque provider context; callers must not silently recompute with a
/// different estimator.
pub(crate) fn eviction_footprint_for_payload(
    spec: &ModelSpec,
    payload: &ProviderContextPayload,
) -> Result<EvictionFootprint, EstimateError> {
    let probe = ReplayProbeV1::new(spec, payload)
        .map_err(|error| EstimateError::ReplayProbeFailure(error.to_string()))?;
    eviction_footprint_v1(&probe)
}

/// Saved values from mixed estimator versions are additive and are never
/// recalculated with the current probe or calibration.
pub(crate) fn sum_saved_footprints(
    footprints: impl IntoIterator<Item = EvictionFootprint>,
) -> Result<u64, EstimateError> {
    footprints.into_iter().try_fold(0_u64, |sum, footprint| {
        if footprint.estimator_version == 0 {
            return Err(EstimateError::InvalidEstimatorVersion);
        }
        sum.checked_add(footprint.eviction_tokens)
            .ok_or(EstimateError::ArithmeticOverflow)
    })
}

fn validate_positive_finite(value: f64) -> Result<(), EstimateError> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(EstimateError::InvalidCalibrationRatio)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::{Value, json};

    use super::*;
    use crate::provider::{
        ModelSpec,
        replay_probe::ReplayProbeV1,
        types::{ApiProtocol, ProviderContextPayload, ToolResultMessage},
    };

    fn public_tool_result(
        tool_call_id: &str,
        tool_name: &str,
        content: Vec<UserContent>,
        details: Value,
    ) -> PublicMessage {
        PublicMessage::ToolResult(ToolResultMessage {
            tool_call_id: tool_call_id.to_owned(),
            tool_name: tool_name.to_owned(),
            content,
            details,
            is_error: false,
            timestamp: chrono::Utc::now(),
        })
    }

    #[test]
    fn multilingual_estimates_have_explicit_rounding() {
        for (text, expected) in [
            ("", 0),
            ("abcd", 1),
            ("abcde", 2),
            ("日", 1),
            ("日本語", 2),
            ("abcd日本語", 3),
            ("😀😀😀", 2),
            ("a日", 1),
        ] {
            assert_eq!(estimate_text_tokens(text), Ok(expected), "{text:?}");
        }
    }

    #[test]
    fn observed_usage_and_ema_do_not_double_apply_ratio() {
        let usage = Usage {
            input: 70,
            cache_read: 20,
            cache_write: 10,
            ..Usage::default()
        };
        assert_eq!(observed_prompt_tokens(&usage), Ok(100));
        let mut calibration = TokenCalibration::new(2.0).expect("valid ratio");
        calibration
            .update_ema(100, 100, 0.5)
            .expect("valid observation");
        assert_eq!(calibration.ratio(), 1.5);
        assert_eq!(calibration.effective_tokens(60, 40), Ok(150));
    }

    #[test]
    fn overflow_comparison_rounds_up_after_one_ratio_application() {
        let calibration = TokenCalibration::new(1.01).expect("valid ratio");
        assert_eq!(calibration.effective_tokens(9, 1), Ok(11));
        assert_eq!(
            calibration.effective_tokens(u64::MAX, 1),
            Err(EstimateError::ArithmeticOverflow)
        );
    }

    #[test]
    fn mixed_versions_sum_saved_values_and_native_window_is_zero() {
        let v1 = EvictionFootprint::from_saved(1, 5, 2).expect("v1");
        let future = EvictionFootprint::from_saved(2, 7, 3).expect("future");
        assert_eq!(
            sum_saved_footprints([v1, future, native_canonical_window_footprint()]),
            Ok(5)
        );
    }

    #[test]
    fn legacy_v1_reproduces_current_main_payload_only_accounting() {
        let signature = ProviderContextPayload::EncryptedReasoning {
            protocol: ApiProtocol::AnthropicMessages,
            item: json!({
                "type": "thinking_signature",
                "signature": "quote:\" backslash:\\ newline:\n 日本語 YWJjZA==",
            }),
        };
        let legacy =
            legacy_serialized_bytes_eviction_footprint(&signature).expect("legacy footprint");
        assert_eq!(
            legacy.estimator_version(),
            EVICTION_ESTIMATOR_VERSION_SERIALIZED_BYTES
        );
        assert_eq!(legacy.replay_wire_bytes(), 95);
        assert_eq!(legacy.eviction_tokens(), 24);

        let replay = eviction_footprint_for_payload(
            &ModelSpec::preset("anthropic").expect("preset"),
            &signature,
        )
        .expect("replay-probe footprint");
        assert_eq!(
            replay.estimator_version(),
            EVICTION_ESTIMATOR_VERSION_REPLAY_PROBE_V1
        );
        assert_eq!(replay.replay_wire_bytes(), 124);
        assert_eq!(replay.eviction_tokens(), 31);

        let native = legacy_serialized_bytes_eviction_footprint(
            &ProviderContextPayload::AnthropicCompaction {
                block: json!({"type": "compaction", "content": "summary"}),
                coverage: crate::provider::types::NativeCompactionCoverage {
                    through_message_seq: 1,
                    context_fingerprint: "legacy-native".to_owned(),
                },
            },
        )
        .expect("legacy native zero");
        assert_eq!(native.replay_wire_bytes(), 0);
        assert_eq!(native.eviction_tokens(), 0);
    }

    #[test]
    fn malformed_or_overflowing_inputs_fail_closed() {
        assert_eq!(
            EvictionFootprint::from_saved(0, 0, 0),
            Err(EstimateError::InvalidEstimatorVersion)
        );
        assert_eq!(
            TokenCalibration::new(f64::NAN),
            Err(EstimateError::InvalidCalibrationRatio)
        );
        let mut calibration = TokenCalibration::default();
        assert_eq!(
            calibration.update_ema(10, 0, 0.5),
            Err(EstimateError::ZeroEstimatedTokens)
        );
        assert_eq!(
            calibration.update_ema(10, 10, 0.0),
            Err(EstimateError::InvalidEmaAlpha)
        );
        let usage = Usage {
            input: u64::MAX,
            cache_read: 1,
            ..Usage::default()
        };
        assert_eq!(
            observed_prompt_tokens(&usage),
            Err(EstimateError::ArithmeticOverflow)
        );
        let huge = EvictionFootprint::from_saved(1, 0, u64::MAX).expect("stored");
        let one = EvictionFootprint::from_saved(9, 0, 1).expect("stored");
        assert_eq!(
            sum_saved_footprints([huge, one]),
            Err(EstimateError::ArithmeticOverflow)
        );
    }

    #[test]
    fn replay_probe_estimator_consumes_the_versioned_contract_golden() {
        let contract: Value =
            serde_json::from_str(include_str!("../../tests/snapshots/replay_probe_v1.json"))
                .expect("golden JSON");
        assert_eq!(contract["contract"], "ReplayProbeV1");
        assert_eq!(
            contract["eviction_estimator_version"],
            EVICTION_ESTIMATOR_VERSION_REPLAY_PROBE_V1
        );
        let supported_kinds = HashSet::from([
            "encrypted_reasoning",
            "thinking_signature",
            "redacted_thinking",
            "open_ai_compacted_window",
            "anthropic_compaction",
        ]);
        let cases = contract["cases"].as_array().expect("golden cases array");
        let mut seen_kinds = HashSet::new();
        for case in cases {
            let kind = case["kind"].as_str().expect("case kind");
            assert!(
                supported_kinds.contains(kind),
                "unknown ReplayProbeV1 golden case: {kind}"
            );
            assert!(seen_kinds.insert(kind), "duplicate golden case: {kind}");
            let protocol = case["protocol"].as_str().expect("case protocol");
            let model_family = case["model_family"].as_str().expect("case model family");
            let (spec, payload, native) = match kind {
                "encrypted_reasoning" => (
                    ModelSpec::preset("openai-responses").expect("Responses preset"),
                    ProviderContextPayload::EncryptedReasoning {
                        protocol: ApiProtocol::OpenAiResponses,
                        item: json!({
                            "type":"reasoning",
                            "id":"rs_replay_probe_v1_fragment",
                            "encrypted_content":"quote:\" backslash:\\ newline:\n 日本語 YWJjZA==",
                            "summary":[],
                        }),
                    },
                    false,
                ),
                "thinking_signature" => (
                    ModelSpec::preset("anthropic").expect("Anthropic preset"),
                    ProviderContextPayload::EncryptedReasoning {
                        protocol: ApiProtocol::AnthropicMessages,
                        item: json!({
                            "type":"thinking_signature",
                            "signature":"quote:\" backslash:\\ newline:\n 日本語 YWJjZA==",
                        }),
                    },
                    false,
                ),
                "redacted_thinking" => (
                    ModelSpec::preset("anthropic").expect("Anthropic preset"),
                    ProviderContextPayload::EncryptedReasoning {
                        protocol: ApiProtocol::AnthropicMessages,
                        item: json!({
                            "type":"redacted_thinking",
                            "data":"quote:\" backslash:\\ newline:\n 日本語 YWJjZA==",
                        }),
                    },
                    false,
                ),
                "open_ai_compacted_window" => (
                    ModelSpec::preset("openai-responses").expect("Responses preset"),
                    ProviderContextPayload::OpenAiCompactedWindow {
                        items: vec![json!({
                            "type":"message",
                            "role":"assistant",
                            "content":[],
                        })],
                        coverage: crate::provider::types::NativeCompactionCoverage {
                            through_message_seq: 1,
                            context_fingerprint: "golden".into(),
                        },
                    },
                    true,
                ),
                "anthropic_compaction" => (
                    ModelSpec::preset("anthropic").expect("Anthropic preset"),
                    ProviderContextPayload::AnthropicCompaction {
                        block: json!({"type":"compaction","content":"golden"}),
                        coverage: crate::provider::types::NativeCompactionCoverage {
                            through_message_seq: 1,
                            context_fingerprint: "golden".into(),
                        },
                    },
                    true,
                ),
                _ => unreachable!("supported kinds checked above"),
            };
            let expected_model_family = match kind {
                "encrypted_reasoning" => "responses_encrypted_reasoning",
                "thinking_signature" | "redacted_thinking" => "anthropic_thinking",
                "open_ai_compacted_window" => "responses_native_compaction",
                "anthropic_compaction" => "anthropic_native_compaction",
                _ => unreachable!("supported kinds checked above"),
            };
            assert_eq!(model_family, expected_model_family);
            assert_eq!(
                protocol,
                match &payload {
                    ProviderContextPayload::EncryptedReasoning { protocol, .. } => {
                        match protocol {
                            ApiProtocol::OpenAiResponses => "open_ai_responses",
                            ApiProtocol::AnthropicMessages => "anthropic_messages",
                            ApiProtocol::OpenAiChatCompletions => "open_ai_chat_completions",
                        }
                    }
                    ProviderContextPayload::OpenAiCompactedWindow { .. } => "open_ai_responses",
                    ProviderContextPayload::AnthropicCompaction { .. } => "anthropic_messages",
                }
            );
            let probe = ReplayProbeV1::new(&spec, &payload).expect("validated probe");
            let footprint = eviction_footprint_v1(&probe).expect("estimator");
            let expected_tokens = case["expected_t19_token_ceiling"]
                .as_u64()
                .expect("expected token ceiling");
            if native {
                assert_eq!(case["result"], "native_canonical_window");
                assert!(case["replay_wire_bytes"].is_null());
                assert!(case.get("without_body_bytes").is_none());
                assert!(case.get("with_body_bytes").is_none());
                assert_eq!(expected_tokens, 0);
                assert_eq!(
                    probe.measure().expect("native measure"),
                    ReplayProbeResult::NativeCanonicalWindow
                );
                assert_eq!(footprint.replay_wire_bytes(), 0);
            } else {
                assert!(case.get("result").is_none());
                let expected_bytes = case["replay_wire_bytes"].as_u64().expect("wire bytes");
                let without = case["without_body_bytes"].as_u64().expect("without bytes");
                let with = case["with_body_bytes"].as_u64().expect("with bytes");
                assert_eq!(with.checked_sub(without), Some(expected_bytes));
                assert_eq!(expected_tokens, expected_bytes.div_ceil(4));
                assert_eq!(
                    probe.measure().expect("serialized measure"),
                    ReplayProbeResult::SerializedDelta {
                        replay_wire_bytes: expected_bytes,
                    }
                );
                assert_eq!(footprint.replay_wire_bytes(), expected_bytes);
                assert_eq!(footprint.eviction_tokens(), expected_tokens);
            }
        }
        assert_eq!(seen_kinds, supported_kinds, "missing required golden case");
    }

    #[test]
    fn native_probe_result_is_a_typed_zero_footprint() {
        let payload = ProviderContextPayload::OpenAiCompactedWindow {
            items: vec![json!({"type":"message","role":"assistant","content":[]})],
            coverage: crate::provider::types::NativeCompactionCoverage {
                through_message_seq: 1,
                context_fingerprint: "golden".into(),
            },
        };
        let spec = ModelSpec::preset("openai-responses").expect("preset");
        let probe = ReplayProbeV1::new(&spec, &payload).expect("validated probe");
        assert_eq!(
            eviction_footprint_v1(&probe).expect("zero"),
            native_canonical_window_footprint()
        );
    }

    #[test]
    fn public_estimator_counts_chat_plaintext_assistant_thinking() {
        let public = PublicMessage::Assistant(crate::provider::types::PublicAssistantMessage {
            content: vec![crate::provider::types::PublicAssistantContent::Thinking {
                thinking: "abc日本語".into(),
                signature_field: "opaque-signature-is-not-counted-separately".into(),
                wire_item_index: 0,
            }],
            model: "kimi-k3".into(),
            provider: "moonshot".into(),
            origin: crate::provider::types::ProviderOrigin {
                provider_instance_id: "moonshot".into(),
                protocol: ApiProtocol::OpenAiChatCompletions,
                model: "kimi-k3".into(),
            },
            usage: Usage::default(),
            stop_reason: crate::provider::types::StopReason::Stop,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: chrono::Utc::now(),
        });
        assert_eq!(estimate_public_message(&public), Ok(3));
        assert_eq!(estimate_public_messages([&public]), Ok(3));
    }

    #[test]
    fn tool_result_estimate_ignores_ui_only_name_and_details() {
        let content = vec![UserContent::Text {
            text: "visible output".into(),
        }];
        let baseline = public_tool_result("call-1", "read", content.clone(), json!({}));
        let huge_ui_only = public_tool_result(
            "call-1",
            &"n".repeat(100_000),
            content,
            json!({"ui_only": "d".repeat(100_000)}),
        );
        assert_eq!(
            estimate_public_message(&baseline),
            estimate_public_message(&huge_ui_only)
        );
    }

    #[test]
    fn tool_result_estimate_tracks_visible_content_and_call_id() {
        let baseline = public_tool_result(
            "id",
            "ignored",
            vec![UserContent::Text {
                text: "abcd".into(),
            }],
            json!({}),
        );
        let longer_content = public_tool_result(
            "id",
            "ignored",
            vec![UserContent::Text {
                text: "abcdefghijkl".into(),
            }],
            json!({}),
        );
        let longer_id = public_tool_result(
            "identifier-longer-than-id",
            "ignored",
            vec![UserContent::Text {
                text: "abcd".into(),
            }],
            json!({}),
        );
        assert!(
            estimate_public_message(&longer_content).expect("content estimate")
                > estimate_public_message(&baseline).expect("baseline estimate")
        );
        assert!(
            estimate_public_message(&longer_id).expect("ID estimate")
                > estimate_public_message(&baseline).expect("baseline estimate")
        );
    }

    #[test]
    fn tool_result_estimate_counts_protocol_placeholders() {
        let id_tokens = estimate_text_tokens("id").expect("ID estimate");
        let empty = public_tool_result("id", "ignored", vec![], json!({}));
        assert_eq!(
            estimate_public_message(&empty),
            checked_sum([
                id_tokens,
                estimate_text_tokens(NO_TOOL_OUTPUT_PLACEHOLDER).expect("placeholder estimate")
            ])
        );

        let image = UserContent::Image {
            data: "base64".into(),
            mime_type: "image/png".into(),
        };
        let image_content = add_user_content(0, &image).expect("image estimate");
        let image_only = public_tool_result("id", "ignored", vec![image], json!({}));
        assert_eq!(
            estimate_public_message(&image_only),
            checked_sum([
                id_tokens,
                image_content,
                estimate_text_tokens(TOOL_RESULT_IMAGE_PLACEHOLDER)
                    .expect("image placeholder estimate")
            ])
        );
    }
}
