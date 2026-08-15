//! PersonalityAgent-facing adapter for its own Sumi Workspace memberships.
//!
//! The authenticated local-control transport fixes the acting
//! PersonalityAgent. Requests in this module deliberately have no actor,
//! PAID, "current Workspace", installation, or enablement field.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorkspaceSummary {
    pub workspace_id: String,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorkspaceListPage {
    pub workspaces: Vec<WorkspaceSummary>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorkspaceInvitationSummary {
    pub invitation_id: String,
    pub workspace_id: String,
    pub workspace_name: String,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorkspaceInvitationListPage {
    pub invitations: Vec<WorkspaceInvitationSummary>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorkspaceMembershipTenure {
    pub workspace_member_id: String,
    pub workspace_id: String,
    pub display_name: String,
    pub owner: bool,
    pub role_ids: Vec<String>,
    pub joined_at: DateTime<Utc>,
    pub left_at: Option<DateTime<Utc>>,
}

/// Sanitized local-control failure classes. None of these variants contains
/// request or response bodies or transport credentials.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum WorkspaceApiError {
    #[error("the Workspace request was rejected as invalid")]
    InvalidRequest,
    #[error("the Workspace request was not authenticated")]
    Unauthenticated,
    #[error("the Workspace request was not authorized")]
    Forbidden,
    #[error("the requested Workspace resource was unavailable")]
    NotFound,
    #[error("the Workspace request conflicted with current state")]
    Conflict,
    #[error("the Workspace service is unavailable")]
    ServiceUnavailable,
    #[error("the Workspace local-control transport failed")]
    Transport,
    #[error("the Workspace local-control response violated its protocol")]
    Protocol,
}

pub(crate) type WorkspaceApiResult<T> = Result<T, WorkspaceApiError>;

#[async_trait]
pub(crate) trait WorkspaceApi: Send + Sync + 'static {
    /// Return one bounded page of active Sumi Workspace memberships for the
    /// authenticated actor. The cursor is opaque ordering state, not actor or
    /// Workspace authority. An empty page is valid.
    async fn list_memberships(&self, cursor: Option<&str>)
    -> WorkspaceApiResult<WorkspaceListPage>;
}

#[async_trait]
pub(crate) trait WorkspaceInvitationApi: Send + Sync + 'static {
    /// Return one bounded page of still-acceptable targeted invitations for
    /// the exact PersonalityAgent authenticated by local-control.
    async fn list_invitations(
        &self,
        cursor: Option<&str>,
    ) -> WorkspaceApiResult<WorkspaceInvitationListPage>;

    /// Accept one exact invitation. Actor and Workspace scope are never caller
    /// inputs; the server resolves both from the bearer and invitation ledger.
    async fn accept_invitation(
        &self,
        invitation_id: &str,
    ) -> WorkspaceApiResult<WorkspaceMembershipTenure>;
}
