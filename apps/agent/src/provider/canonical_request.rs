use reqwest::{RequestBuilder, header::CONTENT_TYPE};
use serde::Serialize;

pub(crate) const JSON_CONTENT_TYPE: &str = "application/json";

/// The one serialization boundary for provider JSON request bodies.
///
/// Keeping the serialized bytes in an owned type makes dry-run accounting use
/// the same uncompressed UTF-8 bytes that reqwest sends on the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CanonicalRequestBody {
    bytes: Vec<u8>,
}

impl CanonicalRequestBody {
    pub(crate) fn serialize<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> {
        Ok(Self {
            bytes: serde_json::to_vec(value)?,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    #[cfg(test)]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn apply(self, request: RequestBuilder) -> RequestBuilder {
        request
            .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
            .body(self.bytes)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn apply_sends_the_exact_canonical_uncompressed_json_bytes() {
        let body = CanonicalRequestBody::serialize(&json!({
            "escaped":"quote:\" backslash:\\ newline:\n 日本語",
        }))
        .expect("serialize");
        let expected = body.as_bytes().to_vec();
        let request = body
            .apply(reqwest::Client::new().post("https://example.invalid/provider"))
            .build()
            .expect("request");

        assert_eq!(
            request
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some(JSON_CONTENT_TYPE)
        );
        assert_eq!(
            request.body().and_then(reqwest::Body::as_bytes),
            Some(expected.as_slice())
        );
        assert!(
            request
                .headers()
                .get(reqwest::header::CONTENT_ENCODING)
                .is_none()
        );
    }
}
