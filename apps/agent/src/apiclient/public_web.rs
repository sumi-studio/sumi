//! Bounded public-page access through Sumi's authenticated control plane.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub(crate) const MAX_URL_BYTES: usize = 8192;
pub(crate) const MAX_TEXT_BYTES: usize = 128 * 1024;
// Shared with Go publicweb: worst-case JSON escaping plus bounded metadata.
pub(crate) const MAX_RESPONSE_BYTES: usize =
    6 * (MAX_TEXT_BYTES + 32 * 1024 + 2 * MAX_URL_BYTES + 1024) + 16 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublicWebRequest {
    pub url: String,
}

impl PublicWebRequest {
    pub fn validate(&self) -> bool {
        if self.url.len() > MAX_URL_BYTES
            || self.url.bytes().any(|c| c <= 0x20 || c == 0x7f)
            || self.url.contains('\\')
            || !self
                .url
                .get(..8)
                .is_some_and(|s| s.eq_ignore_ascii_case("https://"))
        {
            return false;
        }
        let authority = self.url[8..].split(['/', '?', '#']).next().unwrap_or("");
        if authority.is_empty() || !authority.is_ascii() || authority.contains(['@', '%']) {
            return false;
        }
        let host = if let Some(bracketed) = authority.strip_prefix('[') {
            let Some((host, suffix)) = bracketed.split_once(']') else {
                return false;
            };
            if !matches!(suffix, "" | ":443") || host.parse::<std::net::Ipv6Addr>().is_err() {
                return false;
            }
            host
        } else {
            let host = if let Some((host, port)) = authority.split_once(':') {
                if port != "443" {
                    return false;
                }
                host
            } else {
                authority
            };
            if let Ok(ip) = host.parse::<std::net::Ipv4Addr>() {
                if ip.to_string() != host {
                    return false;
                }
            } else {
                let dns = host.strip_suffix('.').unwrap_or(host);
                let labels: Vec<_> = dns.split('.').collect();
                if dns.len() > 253
                    || labels.len() < 2
                    || labels
                        .last()
                        .is_none_or(|label| label.bytes().all(|c| c.is_ascii_digit()))
                    || labels.iter().any(|label| {
                        label.is_empty()
                            || label.len() > 63
                            || label.starts_with('-')
                            || label.ends_with('-')
                            || !label
                                .bytes()
                                .all(|c| c.is_ascii_alphanumeric() || c == b'-')
                    })
                {
                    return false;
                }
            }
            host
        };
        if host.is_empty() {
            return false;
        }
        let bytes = self.url.as_bytes();
        for (i, byte) in bytes.iter().enumerate() {
            if *byte == b'%'
                && !(bytes.get(i + 1).is_some_and(u8::is_ascii_hexdigit)
                    && bytes.get(i + 2).is_some_and(u8::is_ascii_hexdigit))
            {
                return false;
            }
        }
        true
    }

    pub fn network_url(&self) -> &str {
        self.url.split('#').next().unwrap_or(&self.url)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublicWebLink {
    pub id: usize,
    pub url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublicWebPage {
    pub links: Vec<PublicWebLink>,
    pub links_truncated: bool,
    pub requested_url: String,
    pub fetched_url: String,
    pub fetched_at: DateTime<Utc>,
    pub status_code: u16,
    pub media_type: String,
    pub title: Option<String>,
    pub text: String,
    pub body_bytes: u64,
    pub body_sha256: String,
    pub text_truncated: bool,
}

impl PublicWebPage {
    pub fn matches(&self, request: &PublicWebRequest) -> bool {
        self.links.len() <= 100
            && self.links.iter().map(|link| link.url.len()).sum::<usize>() <= 32 * 1024
            && self.links.iter().enumerate().all(|(index, link)| {
                link.id == index + 1
                    && PublicWebRequest {
                        url: link.url.clone(),
                    }
                    .validate()
            })
            && self.requested_url == request.url
            && self.fetched_url == request.network_url()
            && (200..300).contains(&self.status_code)
            && matches!(self.media_type.as_str(), "text/html" | "text/plain")
            && !self.text.trim().is_empty()
            && self.text.len() <= MAX_TEXT_BYTES
            && self.title.as_ref().is_none_or(|title| title.len() <= 1024)
            && self.body_bytes <= 1024 * 1024
            && self.body_sha256.len() == 64
            && self
                .body_sha256
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PublicWebErrorCode {
    InvalidUrl,
    DestinationNotPublic,
    DnsFailed,
    RedirectRequiresNewRequest,
    Timeout,
    TlsFailed,
    HttpStatus,
    AccessChallenge,
    ResponseTooLarge,
    UnsupportedContent,
    NoReadableText,
    Busy,
    Cancelled,
    RuntimeEpochChanged,
    FetchFailed,
    InvalidRequest,
    Unauthorized,
    Unavailable,
    InvalidResponse,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PublicWebContentReason {
    ContentEncoding,
    MediaType,
    Charset,
    InvalidUtf8,
    HtmlParse,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublicWebError {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<PublicWebContentReason>,
    pub error: PublicWebErrorCode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redirect_url: Option<String>,
}

impl PublicWebError {
    pub fn new(error: PublicWebErrorCode) -> Self {
        Self {
            reason: None,
            error,
            requested_url: None,
            status_code: None,
            redirect_url: None,
        }
    }

    pub fn matches(&self, request: &PublicWebRequest) -> bool {
        (self.reason.is_none() || self.error == PublicWebErrorCode::UnsupportedContent)
            && self
                .requested_url
                .as_ref()
                .is_none_or(|url| url == &request.url)
            && self
                .status_code
                .is_none_or(|status| (100..600).contains(&status))
            && self
                .redirect_url
                .as_ref()
                .is_none_or(|url| url.len() <= MAX_URL_BYTES)
    }
}

#[async_trait]
pub(crate) trait PublicWebApi: Send + Sync + 'static {
    async fn read(&self, request: &PublicWebRequest) -> Result<PublicWebPage, PublicWebError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_web_contract_matches_go_url_boundaries_and_results() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../api/internal/publicweb/testdata/contract.json"
        ))
        .unwrap();
        assert_eq!(
            fixture["max_response_bytes"].as_u64().unwrap(),
            MAX_RESPONSE_BYTES as u64
        );
        for case in fixture["url_cases"].as_array().unwrap() {
            let request = PublicWebRequest {
                url: case["url"].as_str().unwrap().into(),
            };
            assert_eq!(
                request.validate(),
                case["valid"].as_bool().unwrap(),
                "{}",
                request.url
            );
            if request.validate() {
                assert_eq!(request.network_url(), case["fetched_url"].as_str().unwrap());
            }
        }
        let page: PublicWebPage = serde_json::from_value(fixture["success"].clone()).unwrap();
        let request = PublicWebRequest {
            url: page.requested_url.clone(),
        };
        assert!(page.matches(&request));
        assert_eq!(serde_json::to_value(page).unwrap(), fixture["success"]);
        let linked: PublicWebPage =
            serde_json::from_value(fixture["linked_success"].clone()).unwrap();
        assert!(linked.matches(&request));
        assert_eq!(
            serde_json::to_value(&linked).unwrap(),
            fixture["linked_success"]
        );
        let mut invalid = linked.clone();
        invalid.links[0].id = 2;
        assert!(!invalid.matches(&request));
        invalid.links[0].id = 1;
        invalid.links[0].url = "javascript:alert(1)".into();
        assert!(!invalid.matches(&request));
        for key in ["content_failure", "challenge_failure"] {
            let error: PublicWebError = serde_json::from_value(fixture[key].clone()).unwrap();
            assert!(error.matches(&request));
            assert_eq!(serde_json::to_value(error).unwrap(), fixture[key]);
        }
        let error: PublicWebError = serde_json::from_value(fixture["failure"].clone()).unwrap();
        assert!(error.matches(&request));
        assert_eq!(serde_json::to_value(error).unwrap(), fixture["failure"]);
    }
}
