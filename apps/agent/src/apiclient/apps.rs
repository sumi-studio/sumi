//! Authenticated app-lifecycle resolution for bound app tool invocations.
//!
//! The model never addresses an installation. It selects an explicit owner
//! scope, and a trusted app adapter supplies its own app id while the live
//! PA-bound local-control credential supplies the actor identity.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResolveEnabledWorkspaceAppRequest<'a> {
    pub workspace_id: &'a str,
    pub app_id: &'a str,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResolvedAppInstallation {
    pub workspace_id: String,
    pub installation_id: String,
    /// Canonical positive signed-int64 decimal wire value.
    pub authority_epoch: String,
}

/// Redacted failure taxonomy for resolving one exact app installation.
///
/// Domain rejections are safe for an app adapter to turn into a user-actionable
/// precondition. Authentication, availability and protocol failures are kept
/// separate so infrastructure faults never masquerade as "install this app".
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum AppInstallationResolutionError {
    #[error("app installation resolution was forbidden")]
    Forbidden,
    #[error("app installation owner was not found")]
    NotFound,
    #[error("app installation was not found")]
    InstallationNotFound,
    #[error("app installation is disabled")]
    AppDisabled,
    #[error("app installation resolver authentication is unavailable")]
    AuthenticationUnavailable,
    #[error("app installation resolver service is unavailable")]
    ServiceUnavailable,
    #[error("app installation resolver transport is unavailable")]
    TransportUnavailable,
    #[error("app installation resolver violated its protocol")]
    Protocol,
}

pub(crate) type AppInstallationResolutionResult<T> =
    std::result::Result<T, AppInstallationResolutionError>;

#[async_trait]
pub(crate) trait AppInstallationResolver: Send + Sync + 'static {
    async fn resolve_enabled_workspace_app(
        &self,
        request: ResolveEnabledWorkspaceAppRequest<'_>,
    ) -> AppInstallationResolutionResult<ResolvedAppInstallation>;
}
