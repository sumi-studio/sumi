use anyhow::{Context, Result, bail};
use regex::Regex;
use serde::Serialize;
use serde_json::{Map, Value};
use zeroize::Zeroize;

use crate::provider::types::{PublicAssistantContent, PublicMessage, UserContent};

use super::crypto::{DataKeyMaterial, RowAad, encrypt_content};

pub const REDACTION_VERSION: u32 = 1;

#[derive(Debug)]
struct RedactionRule {
    pattern: Regex,
    replacement: &'static str,
}

#[derive(Debug)]
pub struct Redactor {
    version: u32,
    rules: Vec<RedactionRule>,
}

impl Redactor {
    pub fn v1() -> Self {
        Self {
            version: REDACTION_VERSION,
            rules: vec![
                RedactionRule {
                    pattern: Regex::new(r"sk-[A-Za-z0-9_-]{12,}")
                        .expect("static API key pattern is valid"),
                    replacement: "[REDACTED:api_key]",
                },
                RedactionRule {
                    pattern: Regex::new(r"(?i)\bBearer[ \t]+[A-Za-z0-9._~+/=-]{8,}")
                        .expect("static bearer token pattern is valid"),
                    replacement: "Bearer [REDACTED:bearer_token]",
                },
                RedactionRule {
                    pattern: Regex::new(
                        r"(?i)(X-Amz-Signature|X-Goog-Signature|signature)=([A-Za-z0-9%._~-]{8,})",
                    )
                    .expect("static signed URL pattern is valid"),
                    replacement: "$1=[REDACTED:signature]",
                },
                RedactionRule {
                    pattern: Regex::new(
                        r#"(?i)\b(api[_-]?key|access[_-]?token|secret)[ \t]*[:=][ \t]*["']?([A-Za-z0-9._~+/=-]{8,})"#,
                    )
                    .expect("static named secret pattern is valid"),
                    replacement: "$1=[REDACTED:secret]",
                },
            ],
        }
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn redact_text(&self, input: &str) -> String {
        self.rules.iter().fold(input.to_owned(), |text, rule| {
            rule.pattern
                .replace_all(&text, rule.replacement)
                .into_owned()
        })
    }

    pub fn redact_value(&self, input: &Value) -> Result<Value> {
        match input {
            Value::String(text) => Ok(Value::String(self.redact_text(text))),
            Value::Array(values) => values
                .iter()
                .map(|value| self.redact_value(value))
                .collect::<Result<Vec<_>>>()
                .map(Value::Array),
            Value::Object(object) => {
                let mut redacted = Map::with_capacity(object.len());
                for (key, value) in object {
                    let structured_secret = structured_secret_placeholder(key);
                    let key = self.redact_text(key);
                    let value = structured_secret.map_or_else(
                        || self.redact_value(value),
                        |placeholder| match value {
                            Value::String(text) => {
                                let redacted = self.redact_text(text);
                                Ok(Value::String(if redacted == *text {
                                    placeholder.to_owned()
                                } else {
                                    redacted
                                }))
                            }
                            _ => Ok(Value::String(placeholder.to_owned())),
                        },
                    )?;
                    if redacted.insert(key, value).is_some() {
                        bail!("JSON object keys collide after secret redaction");
                    }
                }
                Ok(Value::Object(redacted))
            }
            scalar => Ok(scalar.clone()),
        }
    }

    pub fn redact_serialized(&self, input: &[u8]) -> Result<String> {
        let value: Value =
            serde_json::from_slice(input).context("raw public value is not valid JSON")?;
        serde_json::to_string(&self.redact_value(&value)?)
            .context("failed to serialize redacted public projection")
    }
}

fn structured_secret_placeholder(key: &str) -> Option<&'static str> {
    let normalized = key
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|character| character.to_ascii_lowercase())
        .collect::<String>();
    match normalized.as_str() {
        "apikey" | "accesstoken" | "secret" | "authorization" | "proxyauthorization" => {
            Some("[REDACTED:secret]")
        }
        "signature" | "xamzsignature" | "xgoogsignature" => Some("[REDACTED:signature]"),
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProtectedProjection {
    pub ciphertext: Vec<u8>,
    pub projection: String,
    pub redaction_version: u32,
}

pub(crate) struct PublicProjectionBuilder<'a> {
    redactor: &'a Redactor,
    data_key: &'a DataKeyMaterial,
}

impl<'a> PublicProjectionBuilder<'a> {
    pub(crate) fn new(redactor: &'a Redactor, data_key: &'a DataKeyMaterial) -> Self {
        Self { redactor, data_key }
    }

    pub(crate) fn build<T>(&self, value: &T, aad: &RowAad) -> Result<ProtectedProjection>
    where
        T: Serialize,
    {
        let mut raw = serde_json::to_vec(value).context("failed to serialize raw public value")?;
        let protected = self.build_serialized(&raw, aad);
        raw.zeroize();
        protected
    }

    pub(crate) fn build_serialized(&self, raw: &[u8], aad: &RowAad) -> Result<ProtectedProjection> {
        let ciphertext = encrypt_content(self.data_key, raw, aad)?;
        let projection = self.redactor.redact_serialized(raw)?;
        Ok(ProtectedProjection {
            ciphertext,
            projection,
            redaction_version: self.redactor.version(),
        })
    }
}

pub(crate) fn search_text_from_projection(projection: &str) -> Result<String> {
    let message: PublicMessage = serde_json::from_str(projection)
        .context("redacted projection is not a valid PublicMessage")?;
    let mut parts = Vec::new();
    match message {
        PublicMessage::User(message) => collect_visible_content(&message.content, &mut parts),
        PublicMessage::Assistant(message) => {
            for content in message.content {
                if let PublicAssistantContent::Text { text, .. } = content {
                    parts.push(text);
                }
            }
        }
        PublicMessage::ToolResult(message) => {
            collect_visible_content(&message.content, &mut parts);
        }
    }
    Ok(parts.join("\n"))
}

fn collect_visible_content(content: &[UserContent], output: &mut Vec<String>) {
    for content in content {
        if let UserContent::Text { text } = content {
            output.push(text.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        provider::types::{
            PublicAssistantMessage, RejectedToolCall, StopReason, ToolArgumentError, ToolCall,
            ToolResultMessage, Usage, UserMessage, ValidatedToolArguments,
        },
        store::crypto::{DATA_KEY_BYTES, DataKeyPurpose},
    };

    fn timestamp() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-07-20T01:02:03Z")
            .expect("valid fixture timestamp")
            .with_timezone(&chrono::Utc)
    }

    fn projected_search(message: &PublicMessage) -> String {
        let raw = serde_json::to_vec(message).expect("serialize PublicMessage");
        let projection = Redactor::v1()
            .redact_serialized(&raw)
            .expect("redact PublicMessage");
        search_text_from_projection(&projection).expect("extract typed search text")
    }

    #[test]
    fn redacts_secrets_in_nested_message_event_and_error_fields() {
        let redactor = Redactor::v1();
        let value = json!({
            "message": {
                "text": "use sk-abcdefghijklmnop",
                "tool_args": {"authorization": "Bearer abcdefghijklmnop"},
                "details": ["api_key=supersecretvalue"],
            },
            "error_message": "https://example.test/?X-Amz-Signature=abcdef1234567890",
        });
        let projection = redactor.redact_value(&value).expect("redact value");
        let encoded = serde_json::to_string(&projection).expect("serialize projection");

        assert!(!encoded.contains("sk-abcdefghijklmnop"));
        assert!(!encoded.contains("abcdefghijklmnop"));
        assert!(!encoded.contains("supersecretvalue"));
        assert!(!encoded.contains("abcdef1234567890"));
        assert!(encoded.contains("[REDACTED:api_key]"));
        assert!(encoded.contains("[REDACTED:bearer_token]"));
        assert!(encoded.contains("[REDACTED:secret]"));
        assert!(encoded.contains("[REDACTED:signature]"));
    }

    #[test]
    fn structured_named_secret_fields_redact_values_across_case_and_separator_variants() {
        let redactor = Redactor::v1();
        let value = json!({
            "api_key": "plain-api-value",
            "API-Key": "plain-api-dash-value",
            "apiKey": "plain-api-camel-value",
            "access_token": "plain-access-value",
            "Access-Token": "plain-access-dash-value",
            "secret": {"nested": "plain-nested-value"},
            "Authorization": "Basic plain-authorization-value",
            "proxy_authorization": "plain-proxy-value",
            "X-Amz-Signature": "plain-signature-value",
            "ordinary": "plain-visible-value"
        });

        let projection = redactor.redact_value(&value).expect("redact value");
        let encoded = serde_json::to_string(&projection).expect("serialize projection");

        for secret in [
            "plain-api-value",
            "plain-api-dash-value",
            "plain-api-camel-value",
            "plain-access-value",
            "plain-access-dash-value",
            "plain-nested-value",
            "plain-authorization-value",
            "plain-proxy-value",
            "plain-signature-value",
        ] {
            assert!(
                !encoded.contains(secret),
                "structured field leaked {secret}"
            );
        }
        assert_eq!(projection["ordinary"], "plain-visible-value");
        assert_eq!(projection["api_key"], "[REDACTED:secret]");
        assert_eq!(projection["Authorization"], "[REDACTED:secret]");
        assert_eq!(projection["X-Amz-Signature"], "[REDACTED:signature]");
    }

    #[test]
    fn builder_keeps_raw_only_in_authenticated_ciphertext() {
        let key = DataKeyMaterial::from_bytes(
            "transcript-key",
            DataKeyPurpose::Transcript,
            [5; DATA_KEY_BYTES],
        );
        let aad = RowAad {
            tenant_id: "tenant".to_owned(),
            agent_id: "agent".to_owned(),
            conversation_id: "conversation".to_owned(),
            table: "messages".to_owned(),
            row_id: "message-1".to_owned(),
            purpose: "transcript".to_owned(),
            schema_version: 1,
        };
        let protected = PublicProjectionBuilder::new(&Redactor::v1(), &key)
            .build(&json!({"text": "token sk-abcdefghijklmnop"}), &aad)
            .expect("protect value");

        assert_eq!(protected.redaction_version, REDACTION_VERSION);
        assert!(!protected.projection.contains("sk-abcdefghijklmnop"));
        assert!(
            !protected
                .ciphertext
                .windows(b"sk-abcdefghijklmnop".len())
                .any(|window| window == b"sk-abcdefghijklmnop")
        );
    }

    #[test]
    fn user_search_indexes_only_visible_text_content() {
        let message = PublicMessage::User(UserMessage {
            content: vec![
                UserContent::Text {
                    text: "ordinary user text".to_owned(),
                },
                UserContent::Image {
                    data: "private-base64-image-data".to_owned(),
                    mime_type: "image/private-fixture".to_owned(),
                },
            ],
            timestamp: timestamp(),
        });

        assert_eq!(projected_search(&message), "ordinary user text");
    }

    #[test]
    fn assistant_search_excludes_thinking_signature_and_message_metadata() {
        let message = PublicMessage::Assistant(PublicAssistantMessage {
            content: vec![
                PublicAssistantContent::Thinking {
                    thinking: "private chain".to_owned(),
                    signature_field: "private signature field".to_owned(),
                    wire_item_index: 0,
                },
                PublicAssistantContent::ToolCall {
                    tool_call: ToolCall {
                        id: "private-tool-call-id".to_owned(),
                        name: "private-tool-name".to_owned(),
                        arguments: serde_json::from_value::<ValidatedToolArguments>(json!({
                            "path": "/workspace/private-tool-argument.txt"
                        }))
                        .expect("validated tool argument fixture"),
                    },
                    wire_item_index: 1,
                },
                PublicAssistantContent::RejectedToolCall {
                    rejected: RejectedToolCall {
                        id: "private-rejected-tool-call-id".to_owned(),
                        name: "private-rejected-tool-name".to_owned(),
                        error: ToolArgumentError::SchemaViolation,
                    },
                    wire_item_index: 2,
                },
                PublicAssistantContent::Text {
                    text: "visible answer".to_owned(),
                    wire_item_index: 3,
                },
            ],
            model: "private-model-metadata".to_owned(),
            provider: "private-provider-metadata".to_owned(),
            usage: Usage::default(),
            stop_reason: StopReason::Stop,
            error_message: Some("private error metadata".to_owned()),
            provider_code: Some("private provider code".to_owned()),
            interrupted: false,
            timestamp: timestamp(),
        });

        assert_eq!(projected_search(&message), "visible answer");
    }

    #[test]
    fn image_only_message_has_no_search_text() {
        let message = PublicMessage::User(UserMessage {
            content: vec![UserContent::Image {
                data: "private-base64-image-data".to_owned(),
                mime_type: "image/private-fixture".to_owned(),
            }],
            timestamp: timestamp(),
        });

        assert!(projected_search(&message).is_empty());
    }

    #[test]
    fn tool_result_search_indexes_only_visible_text_content() {
        let message = PublicMessage::ToolResult(ToolResultMessage {
            tool_call_id: "private-tool-call-id".to_owned(),
            tool_name: "private-tool-name".to_owned(),
            content: vec![
                UserContent::Text {
                    text: "visible tool output".to_owned(),
                },
                UserContent::Image {
                    data: "private-tool-image-base64".to_owned(),
                    mime_type: "image/private-tool-fixture".to_owned(),
                },
            ],
            details: json!({
                "internal_metadata": "private tool metadata",
                "path": "/workspace/private.txt"
            }),
            is_error: false,
            timestamp: timestamp(),
        });

        assert_eq!(projected_search(&message), "visible tool output");
    }

    #[test]
    fn search_text_is_derived_from_the_redacted_typed_projection() {
        let secret = "sk-abcdefghijklmnop";
        let message = PublicMessage::User(UserMessage {
            content: vec![UserContent::Text {
                text: format!("visible {secret}"),
            }],
            timestamp: timestamp(),
        });
        let raw = serde_json::to_vec(&message).expect("serialize secret fixture");
        let projection = Redactor::v1()
            .redact_serialized(&raw)
            .expect("redact secret fixture");
        let search_text =
            search_text_from_projection(&projection).expect("extract redacted search text");

        assert!(!projection.contains(secret));
        assert!(!search_text.contains(secret));
        assert_eq!(search_text, "visible [REDACTED:api_key]");
    }

    #[test]
    fn redacts_secret_patterns_in_every_json_object_key_position() {
        let redactor = Redactor::v1();
        let value = json!({
            "args api_key=supersecretvalue": {
                "details sk-abcdefghijklmnop": {
                    "event Bearer abcdefghijklmnop": {
                        "message X-Amz-Signature=abcdef1234567890": "safe"
                    }
                }
            }
        });
        let encoded =
            serde_json::to_string(&redactor.redact_value(&value).expect("redact nested keys"))
                .expect("serialize projection");

        for secret in [
            "supersecretvalue",
            "sk-abcdefghijklmnop",
            "abcdefghijklmnop",
            "abcdef1234567890",
        ] {
            assert!(!encoded.contains(secret));
        }
        assert!(encoded.contains("[REDACTED:secret]"));
        assert!(encoded.contains("[REDACTED:api_key]"));
        assert!(encoded.contains("[REDACTED:bearer_token]"));
        assert!(encoded.contains("[REDACTED:signature]"));
    }

    #[test]
    fn redacted_object_key_collision_fails_closed_without_leaking_keys() {
        let redactor = Redactor::v1();
        let value = json!({
            "sk-abcdefghijklmnop": 1,
            "sk-ponmlkjihgfedcba": 2
        });
        let error = redactor
            .redact_value(&value)
            .expect_err("two secret keys redact to the same supported placeholder");

        assert_eq!(
            error.to_string(),
            "JSON object keys collide after secret redaction"
        );
        assert!(!error.to_string().contains("abcdefghijklmnop"));
        assert!(!error.to_string().contains("ponmlkjihgfedcba"));
    }
}
