//! PersonalityAgent-facing adapter for the shared Workspace messaging domain.
//!
//! The authenticated transport derives the acting PersonalityAgent from its
//! generation-fenced local-control credential.  None of these requests carry
//! a Human session or a caller-supplied actor identity.

use anyhow::Result;
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;

use super::apps::AppInstallationResolver;

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
    ) -> Result<Value>;

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
}
