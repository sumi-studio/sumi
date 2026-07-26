use anyhow::{Context, Result, bail};
use regex::Regex;
use serde::Serialize;
use serde_json::{Map, Value};
use zeroize::Zeroize;

use crate::provider::types::{PublicAssistantContent, PublicMessage, UserContent};

use super::crypto::{DataKeyMaterial, RowAad, encrypt_content};

pub const REDACTION_VERSION: u32 = 1;

#[derive(Clone, Debug)]
struct RedactionRule {
    pattern: Regex,
    replacement: &'static str,
}

#[derive(Clone, Debug)]
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
                        r#"(?i)\b(api[_-]?key|access[_-]?token|client[_-]?secret|refresh[_-]?token|consumer[_-]?secret|api[_-]?secret|secret[_-]?key|secret)[ \t]*[:=][ \t]*["']?([A-Za-z0-9._~+/=-]{8,})"#,
                    )
                    .expect("static named secret pattern is valid"),
                    replacement: "$1=[REDACTED:secret]",
                },
                RedactionRule {
                    pattern: Regex::new(r"(?i)Basic[ \t]+[A-Za-z0-9+/=]{8,}")
                        .expect("static basic auth pattern is valid"),
                    replacement: "Basic [REDACTED:basic_credentials]",
                },
                RedactionRule {
                    pattern: Regex::new(r"(?i)https?://[^\s@/]+@[^\s]+")
                        .expect("static URL credential pattern is valid"),
                    replacement: "[REDACTED:url_with_credentials]",
                },
                RedactionRule {
                    pattern: Regex::new(r"(?i)([?&])([^=&\s]*(?:token|secret|api[_-]?key|access[_-]?token|password|passwd|pwd|credential)[^=&\s]*)=([^&\s]{8,})")
                        .expect("static query secret pattern is valid"),
                    replacement: "${1}${2}=[REDACTED:secret]",
                },
                RedactionRule {
                    pattern: Regex::new(r"AKIA[0-9A-Z]{16}")
                        .expect("static AWS access key pattern is valid"),
                    replacement: "[REDACTED:aws_access_key_id]",
                },
                RedactionRule {
                    pattern: Regex::new(r"gh[pousr]_[A-Za-z0-9_]{36,}")
                        .expect("static GitHub token pattern is valid"),
                    replacement: "[REDACTED:github_token]",
                },
                RedactionRule {
                    pattern: Regex::new(r"xox[baprs]-[0-9a-zA-Z-]{10,}")
                        .expect("static Slack token pattern is valid"),
                    replacement: "[REDACTED:slack_token]",
                },
                RedactionRule {
                    pattern: Regex::new(r"eyJ[A-Za-z0-9_-]*\.eyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]*")
                        .expect("static JWT pattern is valid"),
                    replacement: "[REDACTED:jwt]",
                },
                RedactionRule {
                    pattern: Regex::new(
                        r"(?is)-----BEGIN\s+[A-Z0-9 ]*PRIVATE KEY-----.*?-----END\s+[A-Z0-9 ]*PRIVATE KEY-----",
                    )
                    .expect("static PEM private key pattern is valid"),
                    replacement: "[REDACTED:private_key]",
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
        "apikey" | "apitoken" | "xapikey" | "accesstoken" | "authtoken" | "secret"
        | "secretkey" | "apisecret" | "clientsecret" | "consumersecret" | "refreshtoken"
        | "authorization" | "proxyauthorization" | "password" | "passwd" | "pwd" | "privatekey"
        | "credential" | "credentials" | "sessiontoken" | "awssessiontoken"
        | "awssecretaccesskey" | "cookie" | "setcookie" => Some("[REDACTED:secret]"),
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
            ApiProtocol, NativeCompactionCoverage, ProviderContextAnchor, ProviderContextItem,
            ProviderContextPayload, ProviderOrigin, PublicAssistantMessage, RejectedToolCall,
            StopReason, ToolArgumentError, ToolCall, ToolResultMessage, Usage, UserMessage,
            ValidatedToolArguments,
        },
        store::crypto::{DATA_KEY_BYTES, DataKeyPurpose},
    };

    fn timestamp() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-07-20T01:02:03Z")
            .expect("valid fixture timestamp")
            .with_timezone(&chrono::Utc)
    }

    fn provider_origin() -> ProviderOrigin {
        ProviderOrigin {
            provider_instance_id: "private-provider-instance".to_owned(),
            protocol: ApiProtocol::OpenAiChatCompletions,
            model: "private-model-metadata".to_owned(),
        }
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
            origin: provider_origin(),
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

    #[test]
    fn additional_secret_patterns_redact_common_leaks() {
        let redactor = Redactor::v1();
        let aws = "AKIAIOSFODNN7EXAMPLE";
        let gh = "ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
        // Build synthetic fixture from fragments so secret scanners cannot match
        // the literal slack token below.
        let slack = format!(
            "xoxb-{}-{}-{}",
            "1111111111111", "2222222222222", "abcdefghijklmnopqrstuvwx"
        );
        let jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIn0.abc";
        let url = "https://user:p4ssw0rd@example.test/path";
        let query = "https://example.test/?token=secrettokencode&password=hiddenpass";
        let basic = "Basic c29tZTpzZWNyZXQ=";
        let pem = "-----BEGIN OPENSSH PRIVATE KEY-----\nabc\n-----END OPENSSH PRIVATE KEY-----";

        let text = format!("{aws} {gh} {slack} {jwt} {url} {query} {basic} {pem}");
        let redacted = redactor.redact_text(&text);

        assert!(!redacted.contains(aws));
        assert!(!redacted.contains(gh));
        assert!(!redacted.contains(slack.as_str()));
        assert!(!redacted.contains(jwt));
        assert!(!redacted.contains("p4ssw0rd"));
        assert!(!redacted.contains("secrettokencode"));
        assert!(!redacted.contains("hiddenpass"));
        assert!(!redacted.contains("c29tZTpzZWNyZXQ="));
        assert!(!redacted.contains("OPENSSH"));
        assert!(redacted.contains("[REDACTED:aws_access_key_id]"));
        assert!(redacted.contains("[REDACTED:github_token]"));
        assert!(redacted.contains("[REDACTED:slack_token]"));
        assert!(redacted.contains("[REDACTED:jwt]"));
        assert!(redacted.contains("[REDACTED:url_with_credentials]"));
        assert!(redacted.contains("[REDACTED:basic_credentials]"));
        assert!(redacted.contains("[REDACTED:private_key]"));
        assert!(redacted.contains("[REDACTED:secret]"));
        assert!(
            redacted.contains("?token=[REDACTED:secret]"),
            "query-start secret delimiter must be preserved, got: {redacted}"
        );
        assert!(
            redacted.contains("&password=[REDACTED:secret]"),
            "query-continuation secret delimiter must be preserved, got: {redacted}"
        );
    }

    #[test]
    fn structured_secret_keys_cover_password_and_private_key_fields() {
        let redactor = Redactor::v1();
        let value = json!({
            "password": "plain-password",
            "secretKey": "plain-secret-key",
            "privateKey": "plain-private-key",
            "sessionToken": "plain-session-token",
            "ordinary": "visible"
        });
        let encoded = serde_json::to_string(&redactor.redact_value(&value).unwrap()).unwrap();

        for secret in [
            "plain-password",
            "plain-secret-key",
            "plain-private-key",
            "plain-session-token",
        ] {
            assert!(
                !encoded.contains(secret),
                "structured field leaked {secret}"
            );
        }
        assert!(encoded.contains("visible"));
        assert!(encoded.contains("[REDACTED:secret]"));
    }

    #[test]
    fn oauth_structured_keys_redact_in_json() {
        let redactor = Redactor::v1();
        let secrets = [
            ("client_secret", "plain-client-secret-value"),
            ("refresh_token", "plain-refresh-token-value"),
            ("consumer_secret", "plain-consumer-secret-value"),
            ("api_secret", "plain-api-secret-value"),
        ];
        let mut object = Map::new();
        for (key, value) in secrets {
            object.insert(key.to_owned(), Value::String(value.to_owned()));
        }
        object.insert(
            "client_id".to_owned(),
            Value::String("public-id".to_owned()),
        );

        let encoded =
            serde_json::to_string(&redactor.redact_value(&Value::Object(object)).unwrap()).unwrap();
        for (_, secret) in secrets {
            assert!(
                !encoded.contains(secret),
                "structured OAuth field leaked {secret}"
            );
        }
        assert!(
            encoded.contains("public-id"),
            "non-secret client_id must remain visible"
        );
        assert!(encoded.contains("[REDACTED:secret]"));
    }

    #[test]
    fn oauth_free_text_keys_redact_without_leaks() {
        let redactor = Redactor::v1();
        let client_secret = "plain-client-secret-value";
        let refresh_token = "plain-refresh-token-value";
        let consumer_secret = "plain-consumer-secret-value";
        let api_secret = "plain-api-secret-value";
        let text = format!(
            "client_secret: \"{client_secret}\" refresh_token={refresh_token}\n\
             consumer_secret: '{consumer_secret}' and api_secret:{api_secret}"
        );
        let redacted = redactor.redact_text(&text);

        for secret in [client_secret, refresh_token, consumer_secret, api_secret] {
            assert!(
                !redacted.contains(secret),
                "free-text OAuth value leaked {secret}"
            );
        }
        for key in [
            "client_secret",
            "refresh_token",
            "consumer_secret",
            "api_secret",
        ] {
            assert!(
                redacted.contains(&format!("{key}=[REDACTED:secret]")),
                "OAuth key {key} must remain visible to identify the redacted field"
            );
        }
    }

    #[test]
    fn url_query_oauth_secret_delimiters_preserved() {
        let redactor = Redactor::v1();
        let client_secret_value = "plaincs1";
        let refresh_token_value = "plainrt1";
        let url = format!(
            "https://example.test/?client_secret={client_secret_value}&refresh_token={refresh_token_value}&x=1"
        );
        let redacted = redactor.redact_text(&url);
        assert!(!redacted.contains(client_secret_value));
        assert!(!redacted.contains(refresh_token_value));
        assert!(
            redacted.contains("?client_secret=[REDACTED:secret]"),
            "query-start delimiter must be preserved: {redacted}"
        );
        assert!(
            redacted.contains("&refresh_token=[REDACTED:secret]"),
            "query-continuation delimiter must be preserved: {redacted}"
        );
        assert!(
            redacted.contains("&x=1"),
            "non-secret query parameter must remain"
        );
    }

    #[test]
    fn provider_context_payload_cannot_be_reinterpreted_as_public_message() {
        let item = ProviderContextItem {
            origin_message: Some(ProviderContextAnchor {
                message_id: "msg-1".to_owned(),
                message_seq: 1,
            }),
            wire_item_index: Some(0),
            ordinal: 1,
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![json!({"secret": "plain-secret"})],
                coverage: NativeCompactionCoverage {
                    through_message_seq: 1,
                    context_fingerprint: "fp".to_owned(),
                },
            },
        };
        let raw = serde_json::to_vec(&item).expect("serialize provider context");
        assert!(
            serde_json::from_slice::<PublicMessage>(&raw).is_err(),
            "opaque provider context must not deserialize as a public message"
        );
    }
}
