use anyhow::{Context, Result, bail};
use regex::Regex;
use serde::Serialize;
use serde_json::{Map, Value};
use zeroize::Zeroize;

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
                    let key = self.redact_text(key);
                    let value = self.redact_value(value)?;
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
    let value: Value =
        serde_json::from_str(projection).context("redacted projection is not valid JSON")?;
    let mut parts = Vec::new();
    collect_search_text(&value, None, &mut parts);
    Ok(parts.join("\n"))
}

fn collect_search_text(value: &Value, field: Option<&str>, output: &mut Vec<String>) {
    if matches!(field, Some("thinking" | "signature_field")) {
        return;
    }
    match value {
        Value::String(text) => output.push(text.clone()),
        Value::Array(values) => {
            for value in values {
                collect_search_text(value, field, output);
            }
        }
        Value::Object(object) => {
            for (key, value) in object {
                collect_search_text(value, Some(key), output);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::store::crypto::{DATA_KEY_BYTES, DataKeyPurpose};

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
    fn thinking_is_excluded_from_search_projection() {
        let search_text = search_text_from_projection(
            r#"{"content":[{"type":"thinking","thinking":"private chain"},{"type":"text","text":"visible answer"}]}"#,
        )
        .expect("extract search text");
        assert!(!search_text.contains("private chain"));
        assert!(search_text.contains("visible answer"));
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
