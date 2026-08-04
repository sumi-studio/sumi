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

/// Reading or changing one's own名乗り.  There is no field for whose profile
/// it is: the transport's credential decides, exactly as the human settings
/// screen can only edit the signed-in person.  Omitted fields are left alone,
/// so naming one thing about oneself never discards the rest.
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SetMessagingProfileRequest<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tagline: Option<&'a str>,
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

/// Opening a channel in the workspace.  An absent workspace means "the one I
/// am in", which is the only case the MVP has.
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateMessagingChannelRequest<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<&'a str>,
    pub name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voice: Option<bool>,
}

/// Editing a channel's mutable identity.  An absent field is left alone, so
/// renaming never silently clears the topic.
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateMessagingChannelRequest<'a> {
    pub place_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<&'a str>,
}

/// Copying a channel's shape into a new, empty one.  An absent name takes the
/// server's derived default, the same one the human menu gets.
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DuplicateMessagingChannelRequest<'a> {
    pub place_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<&'a str>,
}

/// Searching the messages one can already see.  Visibility is the server's to
/// decide, exactly as it is for the human search box: the query never widens
/// what this person may read.
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SearchMessagingRequest<'a> {
    pub query: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub place_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u16>,
}

/// One place-scoped override of one's own default notification level.
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MessagingNotificationPlace<'a> {
    pub place_id: &'a str,
    pub level: &'a str,
}

/// Reading or changing one's own notification setting.  Every field is
/// optional, and a request with none of them is a read: naming one preference
/// must not silently discard the rest, the way a full replacement would.
/// Whose setting it is comes from the transport's credential — nobody
/// configures anyone else's attention.
#[derive(Debug, Default, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MessagingNotificationSettingsRequest<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defaults_level: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub per_place: Option<Vec<MessagingNotificationPlace<'a>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keywords: Option<Vec<&'a str>>,
}

/// Taking in one's own AttentionCandidates.  `consume_through` acknowledges
/// everything up to that candidate_seq before the remaining ones are listed:
/// one says what one has taken in, then asks what is left.
#[derive(Debug, Default, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PollMessagingAttentionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consume_through: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u16>,
}

/// Reading who is currently in a call (ADR 0012).  No place means every place
/// the agent can see; naming one narrows the answer to it.  There is
/// deliberately no request for *joining* a call — the ADR records that as an
/// open design question rather than a missing endpoint.
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GetMessagingCallStateRequest<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub place_id: Option<&'a str>,
}

/// The side conversations under one place.  Reading them is the same act a
/// human performs by opening the thread list of a channel.
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ListMessagingThreadsRequest<'a> {
    pub place_id: &'a str,
}

/// Opening a thread under the place currently in view.  `parent_message_id`
/// names the message the thread grows from; None starts one from nothing said.
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateMessagingThreadRequest<'a> {
    pub place_id: &'a str,
    pub name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_message_id: Option<&'a str>,
}

/// Asking a question of the room.  It rides the ordinary send, so the poll and
/// the message that carries it commit as one event.
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateMessagingPollRequest<'a> {
    pub place_id: &'a str,
    pub question: &'a str,
    pub options: &'a [String],
    pub allow_multi: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<&'a str>,
    pub client_nonce: &'a str,
    /// Relative for the same reason the status expiry above is: the server's
    /// clock fixes the deadline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closes_in_minutes: Option<u32>,
}

/// Answering one.  The whole choice is restated; an empty list withdraws it.
#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VoteMessagingPollRequest<'a> {
    pub place_id: &'a str,
    pub message_id: &'a str,
    pub option_ids: &'a [String],
}

#[async_trait]
pub(crate) trait MessagingApi: Send + Sync + 'static {
    async fn overview(&self) -> Result<Value>;

    async fn open(&self, request: OpenMessagingPlaceRequest<'_>) -> Result<Value>;

    async fn write(&self, request: WriteMessagingMessageRequest<'_>) -> Result<Value>;

    async fn react(&self, request: ReactMessagingReactionRequest<'_>) -> Result<Value>;

    async fn set_status(&self, request: SetMessagingStatusRequest<'_>) -> Result<Value>;

    async fn profile(&self, request: SetMessagingProfileRequest<'_>) -> Result<Value>;

    async fn reply_later(&self, request: CreateMessagingReplyLaterRequest<'_>) -> Result<Value>;

    async fn resolve_reply_later(
        &self,
        request: ResolveMessagingReplyLaterRequest<'_>,
    ) -> Result<Value>;

    async fn read_through(&self, request: ReadMessagingThroughRequest<'_>) -> Result<Value>;

    async fn start_dm(&self, request: StartMessagingDMRequest<'_>) -> Result<Value>;

    async fn create_channel(&self, request: CreateMessagingChannelRequest<'_>) -> Result<Value>;

    async fn update_channel(&self, request: UpdateMessagingChannelRequest<'_>) -> Result<Value>;

    async fn duplicate_channel(
        &self,
        request: DuplicateMessagingChannelRequest<'_>,
    ) -> Result<Value>;

    async fn search(&self, request: SearchMessagingRequest<'_>) -> Result<Value>;

    async fn notification_settings(
        &self,
        request: MessagingNotificationSettingsRequest<'_>,
    ) -> Result<Value>;

    async fn attention(&self, request: PollMessagingAttentionRequest) -> Result<Value>;

    async fn call_state(&self, request: GetMessagingCallStateRequest<'_>) -> Result<Value>;

    async fn threads(&self, request: ListMessagingThreadsRequest<'_>) -> Result<Value>;

    async fn create_thread(&self, request: CreateMessagingThreadRequest<'_>) -> Result<Value>;

    async fn create_poll(&self, request: CreateMessagingPollRequest<'_>) -> Result<Value>;

    async fn vote_poll(&self, request: VoteMessagingPollRequest<'_>) -> Result<Value>;
}
