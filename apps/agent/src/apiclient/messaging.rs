//! PersonalityAgent-facing adapter for the shared Workspace messaging domain.
//!
//! The authenticated transport derives the acting PersonalityAgent from its
//! generation-fenced local-control credential.  None of these requests carry
//! a Human session or a caller-supplied actor identity.

use anyhow::Result;
use async_trait::async_trait;
use serde::Serialize;
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

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadMessagingThroughRequest<'a> {
    pub place_id: &'a str,
    pub seq: u64,
}

#[async_trait]
pub(crate) trait MessagingApi: Send + Sync + 'static {
    async fn overview(&self) -> Result<Value>;

    async fn open(&self, request: OpenMessagingPlaceRequest<'_>) -> Result<Value>;

    async fn write(&self, request: WriteMessagingMessageRequest<'_>) -> Result<Value>;

    async fn react(&self, request: ReactMessagingReactionRequest<'_>) -> Result<Value>;

    async fn read_through(&self, request: ReadMessagingThroughRequest<'_>) -> Result<Value>;
}
