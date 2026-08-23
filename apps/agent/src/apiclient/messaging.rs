//! PersonalityAgent-facing adapter for the shared Workspace messaging domain.
//!
//! The authenticated transport derives the acting PersonalityAgent from its
//! generation-fenced local-control credential.  None of these requests carry
//! a Human session or a caller-supplied actor identity.

use std::os::fd::OwnedFd;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use zeroize::Zeroizing;

use super::apps::AppInstallationResolver;
use crate::tools::executor::TransferredSource;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MessagingApiFailureClass {
    Terminal,
    Indeterminate,
}

#[derive(Debug, thiserror::Error)]
#[error("{operation}: {detail}")]
pub(crate) struct MessagingApiFailure {
    class: MessagingApiFailureClass,
    operation: &'static str,
    detail: String,
}

impl MessagingApiFailure {
    pub(crate) fn terminal(operation: &'static str, detail: impl Into<String>) -> Self {
        Self {
            class: MessagingApiFailureClass::Terminal,
            operation,
            detail: detail.into(),
        }
    }

    pub(crate) fn indeterminate(operation: &'static str, detail: impl Into<String>) -> Self {
        Self {
            class: MessagingApiFailureClass::Indeterminate,
            operation,
            detail: detail.into(),
        }
    }

    pub(crate) const fn class(&self) -> MessagingApiFailureClass {
        self.class
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ExactMessagingScope {
    pub workspace_id: String,
    pub installation_id: String,
    /// Canonical positive signed-int64 decimal wire value.
    pub authority_epoch: String,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenMessagingPlaceRequest<'a> {
    pub place_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u16>,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WriteMessagingMessageRequest<'a> {
    pub place_id: &'a str,
    pub content: &'a str,
    pub urgency: &'a str,
    pub reply_to: Option<&'a str>,
    pub client_nonce: &'a str,
    pub attachments: &'a [String],
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct MessagingWriteReceipt {
    pub client_nonce: String,
    pub message_id: String,
    pub seq: u64,
    pub created: bool,
}

/// One immutable, sealed Workspace source descriptor obtained through the
/// signed executor operation. The local application sees only its display
/// metadata and bytes; the private Workspace path never crosses this API.
pub(crate) struct UploadMessagingAttachmentRequest {
    place_id: String,
    client_nonce: String,
    filename: String,
    size_bytes: u64,
    sha256: String,
    declared_mime: Option<String>,
    descriptor: OwnedFd,
}

impl UploadMessagingAttachmentRequest {
    /// Preserve source provenance: only a descriptor returned by the signed
    /// executor transfer can become an attachment upload request.
    pub(crate) fn from_executor_source(
        place_id: String,
        client_nonce: String,
        filename: String,
        declared_mime: Option<String>,
        source: TransferredSource,
    ) -> Self {
        let (manifest, descriptor) = source.into_parts();
        Self {
            place_id,
            client_nonce,
            filename,
            size_bytes: manifest.size_bytes,
            sha256: manifest.sha256,
            declared_mime,
            descriptor,
        }
    }

    pub(crate) fn as_parts(&self) -> (&str, &str, &str, u64, &str, Option<&str>, &OwnedFd) {
        (
            &self.place_id,
            &self.client_nonce,
            &self.filename,
            self.size_bytes,
            &self.sha256,
            self.declared_mime.as_deref(),
            &self.descriptor,
        )
    }

    pub(crate) fn into_parts(
        self,
    ) -> (String, String, String, u64, String, Option<String>, OwnedFd) {
        (
            self.place_id,
            self.client_nonce,
            self.filename,
            self.size_bytes,
            self.sha256,
            self.declared_mime,
            self.descriptor,
        )
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct MessagingAttachmentMetadata {
    pub attachment_id: String,
    pub filename: String,
    pub mime: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub position: u8,
    /// The sender asked the receiving side to keep this covered until the
    /// reader opens it. It is carried, never inferred: the agent has to know
    /// what a human's screen is hiding before it says anything about the file.
    pub spoiler: bool,
    /// The sender's description of the content, for whoever cannot or should
    /// not see it yet. Empty when the sender wrote none.
    pub alt: String,
}

/// Metadata proven by the authorized byte response. Position belongs to the
/// message snapshot, not the download headers, so this type deliberately
/// cannot manufacture one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OpenMessagingAttachmentMetadata {
    pub attachment_id: String,
    pub filename: String,
    pub mime: String,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UploadMessagingAttachmentResponse {
    pub attachment: MessagingAttachmentMetadata,
    pub created: bool,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenMessagingAttachmentRequest<'a> {
    pub place_id: &'a str,
    pub message_id: &'a str,
    pub attachment_id: &'a str,
}

pub(crate) struct OpenMessagingAttachmentResponse {
    pub attachment: OpenMessagingAttachmentMetadata,
    pub bytes: Zeroizing<Vec<u8>>,
}

/// Match the Go attachment transport's display-name canonicalization. The
/// source path itself remains sealed and exact elsewhere; only this non-path
/// label crosses into Messaging metadata.
pub(crate) fn canonical_attachment_filename(source: &str) -> String {
    let slashed = source.replace('\\', "/");
    let trimmed = slashed.trim();
    // Go path.Base removes trailing separators before choosing the last
    // element (except that all-slash input becomes "/"). Mirror that exact
    // behavior so the server never persists a renamed receipt after upload.
    let without_trailing = trimmed.trim_end_matches('/');
    let base = if without_trailing.is_empty() && trimmed.contains('/') {
        "/"
    } else {
        without_trailing.rsplit('/').next().unwrap_or_default()
    };
    let mut name = base
        .chars()
        .filter(|character| !forbidden_attachment_display_character(*character))
        .collect::<String>();
    name = name.trim().to_owned();
    if name.is_empty() || matches!(name.as_str(), "." | ".." | "/") {
        return "file".to_owned();
    }
    while name.len() > 255 {
        name.pop();
    }
    if name.is_empty() {
        "file".to_owned()
    } else {
        name
    }
}

/// Keep agent metadata aligned with the API/web display-text gate: C0/C1
/// controls (including NEL), Unicode line/paragraph separators, bidi controls,
/// and zero-width format controls never enter sender-controlled display text.
pub(crate) fn forbidden_attachment_display_character(character: char) -> bool {
    character.is_control()
        || matches!(
            character,
            '\u{180e}'
                | '\u{200b}'..='\u{200f}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202a}'..='\u{202e}'
                | '\u{2060}'
                | '\u{2066}'..='\u{2069}'
                | '\u{feff}'
        )
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReactMessagingReactionRequest<'a> {
    pub place_id: &'a str,
    pub message_id: &'a str,
    pub emoji: &'a str,
    pub client_nonce: &'a str,
}

/// Declaring one's own attention state.  There is no field for whose status it
/// is: the transport's credential decides, the same way the human UI can only
/// set the signed-in person's status.
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SetMessagingStatusRequest<'a> {
    pub status: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<&'a str>,
    /// Relative, so the server's clock fixes the instant.  None holds the
    /// status until it is replaced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_in_minutes: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateMessagingReplyLaterRequest<'a> {
    pub place_id: &'a str,
    pub message_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<&'a str>,
    /// Relative for the same reason as the status expiry above.  None takes
    /// the server's default reminder delay.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remind_in_minutes: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResolveMessagingReplyLaterRequest<'a> {
    pub marker_id: &'a str,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadMessagingThroughRequest<'a> {
    pub place_id: &'a str,
    pub seq: u64,
}

/// Call presence is readable state only. There is intentionally no matching
/// request that could obtain a room token or join media.
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetMessagingCallStateRequest<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub place_id: Option<&'a str>,
}

#[async_trait]
pub(crate) trait MessagingApi: AppInstallationResolver + Send + Sync + 'static {
    async fn overview(&self, scope: &ExactMessagingScope) -> Result<Value>;

    async fn open(
        &self,
        scope: &ExactMessagingScope,
        request: OpenMessagingPlaceRequest<'_>,
    ) -> Result<Value>;

    async fn write(
        &self,
        scope: &ExactMessagingScope,
        request: WriteMessagingMessageRequest<'_>,
    ) -> Result<MessagingWriteReceipt>;

    async fn upload_attachment(
        &self,
        scope: &ExactMessagingScope,
        request: UploadMessagingAttachmentRequest,
    ) -> Result<UploadMessagingAttachmentResponse>;

    async fn open_attachment(
        &self,
        scope: &ExactMessagingScope,
        request: OpenMessagingAttachmentRequest<'_>,
    ) -> Result<OpenMessagingAttachmentResponse>;

    async fn react(
        &self,
        scope: &ExactMessagingScope,
        request: ReactMessagingReactionRequest<'_>,
    ) -> Result<Value>;

    async fn set_status(
        &self,
        scope: &ExactMessagingScope,
        request: SetMessagingStatusRequest<'_>,
    ) -> Result<Value>;

    async fn reply_later(
        &self,
        scope: &ExactMessagingScope,
        request: CreateMessagingReplyLaterRequest<'_>,
    ) -> Result<Value>;

    async fn resolve_reply_later(
        &self,
        scope: &ExactMessagingScope,
        request: ResolveMessagingReplyLaterRequest<'_>,
    ) -> Result<Value>;

    async fn read_through(
        &self,
        scope: &ExactMessagingScope,
        request: ReadMessagingThroughRequest<'_>,
    ) -> Result<Value>;

    async fn call_state(
        &self,
        scope: &ExactMessagingScope,
        request: GetMessagingCallStateRequest<'_>,
    ) -> Result<Value>;
}

#[cfg(test)]
mod tests {
    use super::{canonical_attachment_filename, forbidden_attachment_display_character};

    #[test]
    fn attachment_filename_canonicalization_matches_the_go_wire_contract() {
        for (source, expected) in [
            (" report.txt ", "report.txt"),
            ("a\\b.txt", "b.txt"),
            ("foo/", "foo"),
            ("a/b/..", "file"),
            ("///", "file"),
            ("\u{0001}hello\u{007f}.txt", "hello.txt"),
            ("before\u{0085}after.txt", "beforeafter.txt"),
            ("before\u{2028}after\u{2029}end.txt", "beforeafterend.txt"),
            ("before\u{202e}after\u{200b}end.txt", "beforeafterend.txt"),
            ("\u{2003}wide\u{2003}", "wide"),
            ("", "file"),
            (".", "file"),
            ("..", "file"),
        ] {
            assert_eq!(
                canonical_attachment_filename(source),
                expected,
                "{source:?}"
            );
        }

        let multibyte = "é".repeat(128);
        let bounded = canonical_attachment_filename(&multibyte);
        assert_eq!(bounded.len(), 254);
        assert_eq!(bounded, "é".repeat(127));
    }

    #[test]
    fn attachment_display_character_set_covers_controls_bidi_and_zero_width() {
        for character in ['\u{0085}', '\u{2028}', '\u{2029}', '\u{202e}', '\u{200b}'] {
            assert!(
                forbidden_attachment_display_character(character),
                "{character:?}"
            );
        }
        assert!(!forbidden_attachment_display_character('名'));
    }
}
