//! PersonalityAgent-facing adapter for its own Sumi Workspace memberships.
//!
//! The authenticated local-control transport fixes the acting
//! PersonalityAgent. Requests in this module deliberately have no actor,
//! PAID, "current Workspace", installation, or enablement field.

use async_trait::async_trait;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorkspaceSummary {
    pub workspace_id: String,
    pub name: String,
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
    /// Return every active Sumi Workspace membership for the authenticated
    /// actor. An empty list is valid; it never creates or selects a Workspace.
    async fn list_memberships(&self) -> WorkspaceApiResult<Vec<WorkspaceSummary>>;
}
