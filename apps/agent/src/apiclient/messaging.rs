//! PersonalityAgent-facing adapter for the shared Workspace messaging domain.
//!
//! The authenticated transport derives the acting PersonalityAgent from its
//! generation-fenced local-control credential.  None of these requests carry
//! a Human session or a caller-supplied actor identity.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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

/// One participant on the wire, in the exact shape overview reports members
/// in — so naming somebody is copying what was already shown, not composing a
/// new identity.  Humans and PersonalityAgents share the one shape.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MessagingParticipant {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub human_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub personality_agent_id: Option<String>,
}

/// Opening a direct conversation.  One other participant is the single dm with
/// them; several are a group dm.  The acting agent is never listed: the
/// transport's credential decides who is starting the conversation.
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StartMessagingDMRequest<'a> {
    pub participants: &'a [MessagingParticipant],
}

#[async_trait]
pub(crate) trait MessagingApi: Send + Sync + 'static {
    async fn overview(&self) -> Result<Value>;

    async fn open(&self, request: OpenMessagingPlaceRequest<'_>) -> Result<Value>;

    async fn write(&self, request: WriteMessagingMessageRequest<'_>) -> Result<Value>;

    async fn react(&self, request: ReactMessagingReactionRequest<'_>) -> Result<Value>;

    async fn set_status(&self, request: SetMessagingStatusRequest<'_>) -> Result<Value>;

    async fn reply_later(&self, request: CreateMessagingReplyLaterRequest<'_>) -> Result<Value>;

    async fn resolve_reply_later(
        &self,
        request: ResolveMessagingReplyLaterRequest<'_>,
    ) -> Result<Value>;

    async fn read_through(&self, request: ReadMessagingThroughRequest<'_>) -> Result<Value>;

    async fn start_dm(&self, request: StartMessagingDMRequest<'_>) -> Result<Value>;
}
