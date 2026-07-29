//! Stable, canonical context fingerprint shared between adapters and the
//! runtime `ContextAssembler`.
//!
//! A fingerprint must change exactly when the provider-visible prompt content
//! changes: provider instance identity, protocol, model, system prompt, and
//! tool definitions.  Protocol-specific inputs (Anthropic beta headers and
//! API version) are included for protocols that define them.

use sha2::{Digest, Sha256};

use crate::provider::model::{ModelSpec, ProtocolCompat};
use crate::provider::types::{ApiProtocol, ToolDefinition};

pub const ANTHROPIC_VERSION: &str = "2023-06-01";

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum ContextFingerprintError {
    #[error("protocol does not support context fingerprinting: {0:?}")]
    UnsupportedProtocol(ApiProtocol),
    #[error("model protocol {protocol:?} is incompatible with {compat}")]
    ProtocolCompatMismatch {
        protocol: ApiProtocol,
        compat: &'static str,
    },
    #[error("tool definition serialization failed: {0}")]
    ToolsSerialize(#[from] serde_json::Error),
}

/// Compute the canonical context fingerprint for a destination.
///
/// This is the exact algorithm used by the OpenAI Responses and Anthropic
/// adapters so that runtime native-window lookup matches adapter-generated
/// windows.
pub fn compute_context_fingerprint(
    spec: &ModelSpec,
    system_prompt: &str,
    tools: &[ToolDefinition],
) -> Result<String, ContextFingerprintError> {
    let tools = serde_json::to_vec(tools)?;
    let (protocol, beta, version) = protocol_extras(spec)?;

    let mut hasher = Sha256::new();
    for bytes in [
        spec.provider_instance_id().as_bytes(),
        protocol.as_bytes(),
        spec.id.as_bytes(),
        system_prompt.as_bytes(),
        tools.as_slice(),
        beta.as_bytes(),
        version.as_bytes(),
    ] {
        hasher.update(bytes.len().to_be_bytes());
        hasher.update(bytes);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn protocol_extras(
    spec: &ModelSpec,
) -> Result<(&'static str, String, &'static str), ContextFingerprintError> {
    match (&spec.protocol, &spec.compat) {
        (ApiProtocol::OpenAiChatCompletions, ProtocolCompat::Chat(_)) => {
            Ok(("open_ai_chat_completions", String::new(), ""))
        }
        (ApiProtocol::OpenAiResponses, ProtocolCompat::Responses(_)) => {
            Ok(("open_ai_responses", String::new(), ""))
        }
        (ApiProtocol::AnthropicMessages, ProtocolCompat::Anthropic(compat)) => Ok((
            "anthropic_messages",
            compat.beta_headers.join("\0"),
            ANTHROPIC_VERSION,
        )),
        (protocol, ProtocolCompat::Chat(_)) => {
            Err(ContextFingerprintError::ProtocolCompatMismatch {
                protocol: *protocol,
                compat: "ChatCompat",
            })
        }
        (protocol, ProtocolCompat::Responses(_)) => {
            Err(ContextFingerprintError::ProtocolCompatMismatch {
                protocol: *protocol,
                compat: "ResponsesCompat",
            })
        }
        (protocol, ProtocolCompat::Anthropic(_)) => {
            Err(ContextFingerprintError::ProtocolCompatMismatch {
                protocol: *protocol,
                compat: "AnthropicCompat",
            })
        }
    }
}
