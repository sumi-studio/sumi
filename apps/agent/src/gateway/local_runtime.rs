//! Explicit local control-plane clients for one authenticated runtime epoch.
//!
//! The host control plane owns the local HMAC signing key and authoritative
//! runtime registry. The normal Rust runtime holds only an agent/process-scoped
//! control credential and receives opaque short-lived Gateway credentials.
//! Production uses a least-privilege Unix socket; literal loopback HTTP remains
//! an explicit developer fixture. The mount-provisioned server UID and socket
//! GID are checked against both the trusted parent and socket ownership. This
//! local boundary does not replace workload identity or the central cross-VM
//! issuer/registry tracked by issue #80.

use std::collections::BTreeSet;
use std::fmt;
use std::net::IpAddr;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, watch};
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};

use super::local_control::{
    ISSUE_CREDENTIAL_PATH, LocalCredentialIssueRequest, LocalCredentialIssueResponse,
    LocalRuntimePublicationReason, LocalRuntimePublicationState, LocalRuntimeStateAck,
    LocalRuntimeStatePublication, PUBLISH_RUNTIME_STATE_PATH,
};
use super::supervisor::seams::T17HydrationLatch;
use super::supervisor::{
    CredentialProvider, DeliveryAuthorization, GatewayCredential, HydrationLatch, HydrationReady,
};
use crate::apiclient::messaging::{
    CreateMessagingReplyLaterRequest, MessagingApi, OpenMessagingPlaceRequest,
    ReactMessagingReactionRequest, ReadMessagingThroughRequest, ResolveMessagingReplyLaterRequest,
    SetMessagingStatusRequest, WriteMessagingMessageRequest,
};
use crate::runtime::authority::RuntimeEpochAuthority;
use crate::runtime::contracts::{ProcessGeneration, RpcIdentity};
use crate::store::HydrationReceiptIdentity;

pub(crate) const LOCAL_AGENT_AUDIENCE: &str = "sumi:agent:events";
const MAX_LOCAL_CONTROL_RESPONSE_BYTES: usize = 64 * 1024;
// A messaging screen may contain up to fifty full messages plus its member
// list. Keep that cohesive response bounded independently without widening the
// credential and runtime-state control-plane boundary above.
const MAX_MESSAGING_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MAX_LOCAL_CONTROL_CREDENTIAL_BYTES: usize = 8 * 1024;
const MAX_LOCAL_GATEWAY_CREDENTIAL_BYTES: usize = 8 * 1024;
const MAX_LOCAL_GATEWAY_CREDENTIAL_TTL: Duration = Duration::from_secs(5 * 60);
const DEFAULT_LOCAL_CONTROL_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_LOCAL_CONTROL_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_UNIX_SOCKET_PATH_BYTES: usize = 100;
const TRUSTED_UNIX_SOCKET_MODE: u32 = 0o660;
const TRUSTED_UNIX_PARENT_MODE: u32 = 0o750;

/// Short-lived credential accepted only by the local-control transport.
///
/// This is not the Gateway bearer token and is not a signing key.  It is bound
/// to the exact PAID/generation/boot nonce before the HTTP client can use it.
pub(crate) struct LocalControlCredential {
    token: Zeroizing<String>,
    identity: RpcIdentity,
    expires_at: SystemTime,
}

impl LocalControlCredential {
    pub(crate) fn new(
        token: impl Into<String>,
        identity: RpcIdentity,
        expires_at: SystemTime,
    ) -> Result<Self> {
        let token = token.into();
        if token.is_empty() || token.len() > MAX_LOCAL_CONTROL_CREDENTIAL_BYTES {
            bail!(
                "local control credential must contain 1..={MAX_LOCAL_CONTROL_CREDENTIAL_BYTES} bytes"
            );
        }
        reqwest::header::HeaderValue::from_bytes(token.as_bytes())
            .context("local control credential is not a valid HTTP bearer value")?;
        Ok(Self {
            token: Zeroizing::new(token),
            identity,
            expires_at,
        })
    }

    fn validate_at(&self, authority: &RuntimeEpochAuthority, now: SystemTime) -> Result<()> {
        authority
            .validate_rpc_identity(&self.identity)
            .context("local control credential runtime identity mismatch")?;
        if now >= self.expires_at {
            bail!("local control credential expired");
        }
        Ok(())
    }
}

impl fmt::Debug for LocalControlCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalControlCredential")
            .field("token", &"[REDACTED]")
            .field("personality_agent_id", self.identity.personality_agent_id())
            .field("generation", &self.identity.generation())
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// Injectable local-control seam. Unit tests use an in-memory fake; production
/// bootstrap uses `LocalControlHttpClient::new_unix`.
///
/// The error boundary is transport-independent so the evolving Unix client
/// can preserve the same contract: local validation and explicit peer
/// rejections are terminal, while send/response/ACK loss is indeterminate and
/// must retain the exact CAS publication for reconciliation.
#[derive(Debug, thiserror::Error)]
pub(crate) enum LocalPublicationError {
    #[error("local runtime-state publication failed terminal validation or was rejected: {source}")]
    Terminal {
        #[source]
        source: anyhow::Error,
    },
    #[error("local runtime-state publication outcome is indeterminate: {source}")]
    Indeterminate {
        #[source]
        source: anyhow::Error,
    },
}

impl LocalPublicationError {
    pub(crate) fn terminal(source: anyhow::Error) -> Self {
        Self::Terminal { source }
    }

    pub(crate) fn indeterminate(source: anyhow::Error) -> Self {
        Self::Indeterminate { source }
    }

    pub(crate) const fn is_indeterminate(&self) -> bool {
        matches!(self, Self::Indeterminate { .. })
    }
}

pub(crate) type LocalPublicationResult<T> = std::result::Result<T, LocalPublicationError>;

#[async_trait]
pub(crate) trait LocalControlPlane: Send + Sync + 'static {
    async fn issue_gateway_credential(
        &self,
        request: LocalCredentialIssueRequest,
    ) -> Result<LocalCredentialIssueResponse>;

    async fn publish_runtime_state(
        &self,
        publication: LocalRuntimeStatePublication,
    ) -> LocalPublicationResult<LocalRuntimeStateAck>;
}

/// Authenticated HTTP client for the Go-owned local-control plane.
#[derive(Clone)]
pub(crate) struct LocalControlHttpClient {
    authority: RuntimeEpochAuthority,
    base_url: reqwest::Url,
    credential: Arc<LocalControlCredential>,
    transport: LocalControlTransport,
}

#[derive(Clone)]
enum LocalControlTransport {
    Unix(TrustedUnixEndpoint),
    Loopback(reqwest::Client),
}

impl fmt::Debug for LocalControlTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unix(_) => formatter.write_str("Unix"),
            Self::Loopback(_) => formatter.write_str("Loopback"),
        }
    }
}

#[derive(Clone)]
struct TrustedUnixEndpoint {
    path: PathBuf,
    expected_server_uid: u32,
    expected_socket_gid: u32,
    identity: UnixEndpointIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct UnixEndpointIdentity {
    parent_dev: u64,
    parent_ino: u64,
    parent_nlink: u64,
    parent_uid: u32,
    parent_gid: u32,
    parent_mode: u32,
    parent_ctime: i64,
    parent_ctime_nsec: i64,
    socket_dev: u64,
    socket_ino: u64,
    socket_nlink: u64,
    socket_uid: u32,
    socket_gid: u32,
    socket_mode: u32,
    socket_ctime: i64,
    socket_ctime_nsec: i64,
}

impl fmt::Debug for LocalControlHttpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalControlHttpClient")
            .field(
                "personality_agent_id",
                self.authority.personality_agent_id(),
            )
            .field("generation", &self.authority.generation())
            .field("transport", &self.transport)
            .field("credential", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl LocalControlHttpClient {
    pub(crate) fn new_unix(
        socket_path: impl AsRef<Path>,
        expected_server_uid: u32,
        expected_socket_gid: u32,
        authority: RuntimeEpochAuthority,
        credential: LocalControlCredential,
    ) -> Result<Self> {
        credential.validate_at(&authority, SystemTime::now())?;
        let endpoint = validate_unix_socket_path(
            socket_path.as_ref(),
            expected_server_uid,
            expected_socket_gid,
        )?;
        let base_url = reqwest::Url::parse("http://local-control.invalid/")
            .context("construct local control endpoint")?;
        Ok(Self {
            authority,
            base_url,
            credential: Arc::new(credential),
            transport: LocalControlTransport::Unix(endpoint),
        })
    }

    pub(crate) fn new_loopback(
        base_url: impl AsRef<str>,
        authority: RuntimeEpochAuthority,
        credential: LocalControlCredential,
    ) -> Result<Self> {
        credential.validate_at(&authority, SystemTime::now())?;
        let base_url = validate_loopback_base_url(base_url.as_ref())?;
        let http = reqwest::Client::builder()
            .connect_timeout(DEFAULT_LOCAL_CONTROL_CONNECT_TIMEOUT)
            .timeout(DEFAULT_LOCAL_CONTROL_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .context("build local control HTTP client")?;
        Ok(Self {
            authority,
            base_url,
            credential: Arc::new(credential),
            transport: LocalControlTransport::Loopback(http),
        })
    }

    async fn post_json<Request, Response>(&self, path: &str, body: &Request) -> Result<Response>
    where
        Request: Serialize + Sync,
        Response: for<'de> Deserialize<'de>,
    {
        self.post_json_bounded(path, body, MAX_LOCAL_CONTROL_RESPONSE_BYTES)
            .await
    }

    async fn post_json_bounded<Request, Response>(
        &self,
        path: &str,
        body: &Request,
        max_response_bytes: usize,
    ) -> Result<Response>
    where
        Request: Serialize + Sync,
        Response: for<'de> Deserialize<'de>,
    {
        self.credential
            .validate_at(&self.authority, SystemTime::now())?;
        let (http, unix_endpoint) = match &self.transport {
            LocalControlTransport::Unix(endpoint) => {
                (build_unix_http_client(&endpoint.path)?, Some(endpoint))
            }
            LocalControlTransport::Loopback(http) => (http.clone(), None),
        };
        let url = self
            .base_url
            .join(path)
            .context("join local control endpoint URL")?;
        let mut request = http
            .post(url)
            .bearer_auth(self.credential.token.as_str())
            .json(body);
        if unix_endpoint.is_some() {
            request = request.header(reqwest::header::CONNECTION, "close");
        }
        let request = request.build().context("build local control request")?;
        if let Some(endpoint) = unix_endpoint {
            // reqwest's sealed Unix connector accepts only a path and does not
            // expose the connected stream for SO_PEERCRED validation. Recheck
            // the original identity at the last synchronous point before
            // execute(); the 0750 parent remains mutable only by its trusted
            // owner during the remaining connect syscall window.
            endpoint.revalidate()?;
        }
        let response = http
            .execute(request)
            .await
            .context("local control request failed")?;
        if !response.status().is_success() {
            bail!(
                "local control request was rejected with status {}",
                response.status()
            );
        }
        if response
            .content_length()
            .is_some_and(|length| length > max_response_bytes as u64)
        {
            bail!("local control response exceeds bounded size");
        }
        let mut body = Zeroizing::new(Vec::new());
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("read local control response")?;
            if body.len().saturating_add(chunk.len()) > max_response_bytes {
                bail!("local control response exceeds bounded size");
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(body.as_slice()).context("decode strict local control response")
    }

    async fn post_runtime_state(
        &self,
        publication: &LocalRuntimeStatePublication,
    ) -> LocalPublicationResult<LocalRuntimeStateAck> {
        self.credential
            .validate_at(&self.authority, SystemTime::now())
            .map_err(LocalPublicationError::terminal)?;
        let url = self
            .base_url
            .join(PUBLISH_RUNTIME_STATE_PATH)
            .context("join local control endpoint URL")
            .map_err(LocalPublicationError::terminal)?;
        let (http, unix_endpoint) = match &self.transport {
            LocalControlTransport::Unix(endpoint) => (
                build_unix_http_client(&endpoint.path).map_err(LocalPublicationError::terminal)?,
                Some(endpoint),
            ),
            LocalControlTransport::Loopback(http) => (http.clone(), None),
        };
        let mut request = http
            .post(url)
            .bearer_auth(self.credential.token.as_str())
            .json(publication);
        if unix_endpoint.is_some() {
            request = request.header(reqwest::header::CONNECTION, "close");
        }
        let request = request
            .build()
            .context("build local control publication request")
            .map_err(LocalPublicationError::terminal)?;
        if let Some(endpoint) = unix_endpoint {
            endpoint
                .revalidate()
                .map_err(LocalPublicationError::terminal)?;
        }
        let response = http
            .execute(request)
            .await
            .context("local control publication request failed")
            .map_err(LocalPublicationError::indeterminate)?;
        if let Some(error) = publication_http_status_error(response.status()) {
            return Err(error);
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_LOCAL_CONTROL_RESPONSE_BYTES as u64)
        {
            return Err(LocalPublicationError::indeterminate(anyhow::anyhow!(
                "local control publication response exceeds bounded size"
            )));
        }
        let mut body = Zeroizing::new(Vec::new());
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk
                .context("read local control publication response")
                .map_err(LocalPublicationError::indeterminate)?;
            if body.len().saturating_add(chunk.len()) > MAX_LOCAL_CONTROL_RESPONSE_BYTES {
                return Err(LocalPublicationError::indeterminate(anyhow::anyhow!(
                    "local control publication response exceeds bounded size"
                )));
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(body.as_slice())
            .context("decode strict local control publication response")
            .map_err(LocalPublicationError::indeterminate)
    }
}

fn publication_http_status_error(status: reqwest::StatusCode) -> Option<LocalPublicationError> {
    if status.is_success() {
        return None;
    }
    if status.is_client_error() || status.is_redirection() {
        return Some(LocalPublicationError::terminal(anyhow::anyhow!(
            "local control publication was rejected with status {status}"
        )));
    }
    Some(LocalPublicationError::indeterminate(anyhow::anyhow!(
        "local control publication acknowledgement failed with status {status}"
    )))
}

#[async_trait]
impl LocalControlPlane for LocalControlHttpClient {
    async fn issue_gateway_credential(
        &self,
        request: LocalCredentialIssueRequest,
    ) -> Result<LocalCredentialIssueResponse> {
        validate_wire_epoch(
            &self.authority,
            &request.personality_agent_id,
            request.generation,
            &request.rpc_boot_nonce,
            "local credential issue request",
        )?;
        if request.audience != LOCAL_AGENT_AUDIENCE {
            bail!("local credential issue request audience mismatch");
        }
        self.post_json(ISSUE_CREDENTIAL_PATH, &request).await
    }

    async fn publish_runtime_state(
        &self,
        publication: LocalRuntimeStatePublication,
    ) -> LocalPublicationResult<LocalRuntimeStateAck> {
        validate_wire_epoch(
            &self.authority,
            &publication.personality_agent_id,
            publication.generation,
            &publication.rpc_boot_nonce,
            "local runtime-state publication",
        )
        .map_err(LocalPublicationError::terminal)?;
        validate_publication_payload(
            publication.state,
            publication.hydration_receipt_identity.as_deref(),
            publication.reason,
        )
        .map_err(LocalPublicationError::terminal)?;
        self.post_runtime_state(&publication).await
    }
}

#[async_trait]
impl MessagingApi for LocalControlHttpClient {
    async fn overview(&self) -> Result<serde_json::Value> {
        self.post_json_bounded(
            "/local-control/v1/messaging:overview",
            &serde_json::json!({}),
            MAX_MESSAGING_RESPONSE_BYTES,
        )
        .await
    }

    async fn open(&self, request: OpenMessagingPlaceRequest<'_>) -> Result<serde_json::Value> {
        self.post_json_bounded(
            "/local-control/v1/messaging:open",
            &request,
            MAX_MESSAGING_RESPONSE_BYTES,
        )
        .await
    }

    async fn write(&self, request: WriteMessagingMessageRequest<'_>) -> Result<serde_json::Value> {
        self.post_json("/local-control/v1/messaging:write", &request)
            .await
    }

    async fn react(&self, request: ReactMessagingReactionRequest<'_>) -> Result<serde_json::Value> {
        // The response echoes the full message (content up to 64 KiB plus its
        // reaction state), so it shares the messaging screen bound rather than
        // the tighter control-plane bound.
        self.post_json_bounded(
            "/local-control/v1/messaging:react",
            &request,
            MAX_MESSAGING_RESPONSE_BYTES,
        )
        .await
    }

    async fn set_status(
        &self,
        request: SetMessagingStatusRequest<'_>,
    ) -> Result<serde_json::Value> {
        self.post_json("/local-control/v1/messaging:status", &request)
            .await
    }

    async fn reply_later(
        &self,
        request: CreateMessagingReplyLaterRequest<'_>,
    ) -> Result<serde_json::Value> {
        self.post_json("/local-control/v1/messaging:reply-later", &request)
            .await
    }

    async fn resolve_reply_later(
        &self,
        request: ResolveMessagingReplyLaterRequest<'_>,
    ) -> Result<serde_json::Value> {
        self.post_json("/local-control/v1/messaging:reply-later-resolve", &request)
            .await
    }

    async fn read_through(
        &self,
        request: ReadMessagingThroughRequest<'_>,
    ) -> Result<serde_json::Value> {
        self.post_json("/local-control/v1/messaging:read-through", &request)
            .await
    }
}

fn validate_wire_epoch(
    authority: &RuntimeEpochAuthority,
    personality_agent_id: &str,
    generation: u64,
    rpc_boot_nonce: &str,
    operation: &'static str,
) -> Result<()> {
    if personality_agent_id != authority.personality_agent_id().as_str()
        || generation != authority.generation().as_u64()
        || rpc_boot_nonce != authority.nonce().as_str()
    {
        bail!("{operation} runtime epoch mismatch");
    }
    Ok(())
}

fn validate_loopback_base_url(value: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(value).context("parse local control URL")?;
    if url.scheme() != "http" {
        bail!("local control URL must use http:// on a loopback interface");
    }
    let host = url
        .host_str()
        .and_then(|host| host.parse::<IpAddr>().ok())
        .context("local control URL host must be a literal IP address")?;
    if !host.is_loopback() {
        bail!("local control URL host must be loopback");
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        bail!("local control base URL must not contain credentials, path, query, or fragment");
    }
    Ok(url)
}

fn build_unix_http_client(path: &Path) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .unix_socket(path)
        .connect_timeout(DEFAULT_LOCAL_CONTROL_CONNECT_TIMEOUT)
        .timeout(DEFAULT_LOCAL_CONTROL_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .pool_max_idle_per_host(0)
        .http1_only()
        .build()
        .context("build one-request Unix local control HTTP client")
}

fn validate_unix_socket_path(
    value: &Path,
    expected_server_uid: u32,
    expected_socket_gid: u32,
) -> Result<TrustedUnixEndpoint> {
    if !value.is_absolute() {
        bail!("local control Unix socket path must be absolute");
    }
    if value.as_os_str().as_bytes().len() > MAX_UNIX_SOCKET_PATH_BYTES {
        bail!("local control Unix socket path exceeds bounded length");
    }
    if value
        .components()
        .any(|component| !matches!(component, Component::RootDir | Component::Normal(_)))
    {
        bail!("local control Unix socket path must be lexically clean");
    }
    let identity = inspect_unix_socket_identity(value, expected_server_uid, expected_socket_gid)?;
    Ok(TrustedUnixEndpoint {
        path: value.to_path_buf(),
        expected_server_uid,
        expected_socket_gid,
        identity,
    })
}

impl TrustedUnixEndpoint {
    fn revalidate(&self) -> Result<()> {
        let current = inspect_unix_socket_identity(
            &self.path,
            self.expected_server_uid,
            self.expected_socket_gid,
        )?;
        if current != self.identity {
            bail!("local control Unix socket identity changed after client construction");
        }
        Ok(())
    }
}

fn inspect_unix_socket_identity(
    value: &Path,
    expected_server_uid: u32,
    expected_socket_gid: u32,
) -> Result<UnixEndpointIdentity> {
    let parent = value
        .parent()
        .context("local control Unix socket must have a parent")?;
    let canonical_parent =
        std::fs::canonicalize(parent).context("resolve local control Unix socket parent")?;
    if canonical_parent != parent {
        bail!("local control Unix socket parent path must not contain symlinks");
    }
    let parent_metadata =
        std::fs::symlink_metadata(parent).context("inspect local control Unix socket parent")?;
    if !parent_metadata.file_type().is_dir() || parent_metadata.file_type().is_symlink() {
        bail!("local control Unix socket parent must be a real directory");
    }
    if parent_metadata.mode() & 0o777 != TRUSTED_UNIX_PARENT_MODE {
        bail!("local control Unix socket parent mode must be 0750");
    }

    let socket_metadata =
        std::fs::symlink_metadata(value).context("inspect local control Unix socket")?;
    if !socket_metadata.file_type().is_socket() || socket_metadata.file_type().is_symlink() {
        bail!("local control Unix socket target must be a real socket");
    }
    if socket_metadata.mode() & 0o777 != TRUSTED_UNIX_SOCKET_MODE {
        bail!("local control Unix socket mode must be 0660");
    }
    if socket_metadata.nlink() != 1 {
        bail!("local control Unix socket link count must be exactly one");
    }
    if parent_metadata.uid() != socket_metadata.uid()
        || parent_metadata.gid() != socket_metadata.gid()
    {
        bail!("local control Unix socket parent and socket ownership must match");
    }
    if parent_metadata.uid() != expected_server_uid {
        bail!("local control Unix socket owner does not match SUMI_LOCAL_CONTROL_SERVER_UID");
    }
    if parent_metadata.gid() != expected_socket_gid {
        bail!("local control Unix socket group does not match SUMI_LOCAL_CONTROL_SOCKET_GID");
    }
    let euid = unsafe { libc::geteuid() };
    if euid != socket_metadata.uid() && !process_has_group(socket_metadata.gid())? {
        bail!("local control Unix socket group is not assigned to this runtime");
    }
    Ok(UnixEndpointIdentity {
        parent_dev: parent_metadata.dev(),
        parent_ino: parent_metadata.ino(),
        parent_nlink: parent_metadata.nlink(),
        parent_uid: parent_metadata.uid(),
        parent_gid: parent_metadata.gid(),
        parent_mode: parent_metadata.mode(),
        parent_ctime: parent_metadata.ctime(),
        parent_ctime_nsec: parent_metadata.ctime_nsec(),
        socket_dev: socket_metadata.dev(),
        socket_ino: socket_metadata.ino(),
        socket_nlink: socket_metadata.nlink(),
        socket_uid: socket_metadata.uid(),
        socket_gid: socket_metadata.gid(),
        socket_mode: socket_metadata.mode(),
        socket_ctime: socket_metadata.ctime(),
        socket_ctime_nsec: socket_metadata.ctime_nsec(),
    })
}

fn process_has_group(gid: u32) -> Result<bool> {
    if unsafe { libc::getegid() } == gid {
        return Ok(true);
    }
    let count = unsafe { libc::getgroups(0, std::ptr::null_mut()) };
    if count < 0 {
        return Err(std::io::Error::last_os_error()).context("read runtime supplementary groups");
    }
    let mut groups = vec![0; usize::try_from(count).context("invalid supplementary group count")?];
    if count > 0 {
        let read = unsafe { libc::getgroups(count, groups.as_mut_ptr()) };
        if read != count {
            return Err(std::io::Error::last_os_error())
                .context("read runtime supplementary groups");
        }
    }
    Ok(groups.contains(&gid))
}

/// Fixed-scope T24 credential provider backed by the Go local-control fixture.
pub(crate) struct LocalCredentialProvider {
    authority: RuntimeEpochAuthority,
    audience: String,
    delivery_authorization: DeliveryAuthorization,
    control: Arc<dyn LocalControlPlane>,
}

impl LocalCredentialProvider {
    pub(crate) fn new(
        authority: RuntimeEpochAuthority,
        delivery_authorization: DeliveryAuthorization,
        control: Arc<dyn LocalControlPlane>,
    ) -> Self {
        Self {
            authority,
            audience: LOCAL_AGENT_AUDIENCE.to_owned(),
            delivery_authorization,
            control,
        }
    }
}

impl fmt::Debug for LocalCredentialProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalCredentialProvider")
            .field(
                "personality_agent_id",
                self.authority.personality_agent_id(),
            )
            .field("generation", &self.authority.generation())
            .field("audience", &self.audience)
            .field("delivery_authorization", &self.delivery_authorization)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CredentialProvider for LocalCredentialProvider {
    async fn fresh_credential(&mut self) -> Result<GatewayCredential> {
        let request_id = Uuid::now_v7().hyphenated().to_string();
        let request = LocalCredentialIssueRequest {
            request_id: request_id.clone(),
            personality_agent_id: self.authority.personality_agent_id().as_str().to_owned(),
            generation: self.authority.generation().as_u64(),
            rpc_boot_nonce: self.authority.nonce().as_str().to_owned(),
            audience: self.audience.clone(),
        };
        let mut response = self.control.issue_gateway_credential(request).await?;
        validate_credential_response(
            &self.authority,
            &request_id,
            &self.audience,
            self.delivery_authorization,
            &response,
            SystemTime::now(),
        )?;
        let mut grant = LocalCredentialGrant {
            token: Some(std::mem::take(&mut response.token)),
            identity: self.authority.rpc_identity().clone(),
            expires_at: system_time_from_unix(response.expires_at_unix)?,
            delivery_authorization: response.delivery_authorization,
        };
        grant.consume_at(&self.authority, SystemTime::now())
    }
}

fn validate_credential_response(
    authority: &RuntimeEpochAuthority,
    request_id: &str,
    audience: &str,
    delivery_authorization: DeliveryAuthorization,
    response: &LocalCredentialIssueResponse,
    now: SystemTime,
) -> Result<()> {
    if response.request_id != request_id
        || response.personality_agent_id != authority.personality_agent_id().as_str()
        || response.generation != authority.generation().as_u64()
        || response.rpc_boot_nonce != authority.nonce().as_str()
        || response.audience != audience
        || response.delivery_authorization != delivery_authorization
    {
        bail!("local Gateway credential response scope mismatch");
    }
    if response.token.is_empty() || response.token.len() > MAX_LOCAL_GATEWAY_CREDENTIAL_BYTES {
        bail!(
            "local Gateway credential must contain 1..={MAX_LOCAL_GATEWAY_CREDENTIAL_BYTES} bytes"
        );
    }
    reqwest::header::HeaderValue::from_bytes(response.token.as_bytes())
        .context("local Gateway credential is not a valid HTTP bearer value")?;
    let expires_at = system_time_from_unix(response.expires_at_unix)?;
    if expires_at <= now {
        bail!("local Gateway credential response is already expired");
    }
    if expires_at
        .duration_since(now)
        .context("local Gateway credential expiry precedes issuance")?
        > MAX_LOCAL_GATEWAY_CREDENTIAL_TTL
    {
        bail!("local Gateway credential response exceeds maximum local TTL");
    }
    Ok(())
}

struct LocalCredentialGrant {
    token: Option<String>,
    identity: RpcIdentity,
    expires_at: SystemTime,
    delivery_authorization: DeliveryAuthorization,
}

impl LocalCredentialGrant {
    fn consume_at(
        &mut self,
        authority: &RuntimeEpochAuthority,
        now: SystemTime,
    ) -> Result<GatewayCredential> {
        authority
            .validate_rpc_identity(&self.identity)
            .context("local Gateway credential grant runtime identity mismatch")?;
        if now >= self.expires_at {
            bail!("local Gateway credential grant expired before consumption");
        }
        let token = self
            .token
            .take()
            .context("local Gateway credential grant was already consumed")?;
        Ok(GatewayCredential::new(
            token,
            self.identity.personality_agent_id().clone(),
            self.identity.generation(),
            self.delivery_authorization,
        ))
    }
}

impl fmt::Debug for LocalCredentialGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalCredentialGrant")
            .field("token", &"[REDACTED]")
            .field("personality_agent_id", self.identity.personality_agent_id())
            .field("generation", &self.identity.generation())
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl Drop for LocalCredentialGrant {
    fn drop(&mut self) {
        if let Some(token) = self.token.as_mut() {
            token.zeroize();
        }
    }
}

/// Runtime dependencies that must exist before the first browser vertical can
/// release command delivery and publish Ready.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum LocalRuntimeComponent {
    EventDispatcher,
    GatewayConnector,
    Session,
    Provider,
    ApprovalBroker,
    ToolRegistry,
    Executor,
}

pub(crate) const FIRST_BROWSER_VERTICAL_COMPONENTS: [LocalRuntimeComponent; 7] = [
    LocalRuntimeComponent::EventDispatcher,
    LocalRuntimeComponent::GatewayConnector,
    LocalRuntimeComponent::Session,
    LocalRuntimeComponent::Provider,
    LocalRuntimeComponent::ApprovalBroker,
    LocalRuntimeComponent::ToolRegistry,
    LocalRuntimeComponent::Executor,
];

#[derive(Clone)]
pub(crate) struct LocalReadyController {
    authority: RuntimeEpochAuthority,
    required: BTreeSet<LocalRuntimeComponent>,
    ready: watch::Sender<BTreeSet<LocalRuntimeComponent>>,
}

/// T24 latch combining exact T17 hydration with explicit component readiness
/// for the same PAID/generation/boot nonce.
#[derive(Clone)]
pub(crate) struct LocalReadyLatch {
    authority: RuntimeEpochAuthority,
    hydration: T17HydrationLatch,
    typed_hydration: watch::Receiver<Option<HydrationReceiptIdentity>>,
    required: BTreeSet<LocalRuntimeComponent>,
    ready: watch::Receiver<BTreeSet<LocalRuntimeComponent>>,
}

/// Proof minted only after exact hydration and every required component are
/// Ready for one runtime epoch.
#[derive(Clone, Debug)]
pub(crate) struct LocalReadyProof {
    authority: RuntimeEpochAuthority,
    receipt: HydrationReceiptIdentity,
    ready: HydrationReady,
}

impl LocalReadyProof {
    pub(crate) const fn hydration_ready(&self) -> &HydrationReady {
        &self.ready
    }
}

pub(crate) fn first_browser_vertical_ready_gate(
    authority: RuntimeEpochAuthority,
    hydration: watch::Receiver<Option<HydrationReceiptIdentity>>,
) -> (LocalReadyController, LocalReadyLatch) {
    local_ready_gate(authority, hydration, FIRST_BROWSER_VERTICAL_COMPONENTS)
        .expect("the closed first-browser component set is nonempty")
}

pub(crate) fn local_ready_gate(
    authority: RuntimeEpochAuthority,
    hydration: watch::Receiver<Option<HydrationReceiptIdentity>>,
    required: impl IntoIterator<Item = LocalRuntimeComponent>,
) -> Result<(LocalReadyController, LocalReadyLatch)> {
    let required: BTreeSet<_> = required.into_iter().collect();
    if required.is_empty() {
        bail!("local Ready gate must require at least one runtime component");
    }
    let (ready_tx, ready_rx) = watch::channel(BTreeSet::new());
    let typed_hydration = hydration.clone();
    let hydration = T17HydrationLatch::new(hydration, authority.clone());
    Ok((
        LocalReadyController {
            authority: authority.clone(),
            required: required.clone(),
            ready: ready_tx,
        },
        LocalReadyLatch {
            authority,
            hydration,
            typed_hydration,
            required,
            ready: ready_rx,
        },
    ))
}

impl LocalReadyController {
    pub(crate) fn mark_ready(
        &self,
        identity: &RpcIdentity,
        component: LocalRuntimeComponent,
    ) -> Result<()> {
        self.authority
            .validate_rpc_identity(identity)
            .context("local runtime component identity mismatch")?;
        if !self.required.contains(&component) {
            bail!("local runtime component {component:?} is not required by this Ready gate");
        }
        self.ready.send_modify(|ready| {
            ready.insert(component);
        });
        Ok(())
    }
}

#[async_trait]
impl HydrationLatch for LocalReadyLatch {
    async fn wait_for(&self, generation: ProcessGeneration) -> Result<HydrationReady> {
        Ok(self.wait_for_proof(generation).await?.ready)
    }
}

impl LocalReadyLatch {
    pub(crate) async fn wait_for_proof(
        &self,
        generation: ProcessGeneration,
    ) -> Result<LocalReadyProof> {
        self.authority
            .validate_generation(generation)
            .context("local Ready wait requested the wrong runtime generation")?;
        let initial_receipt = self.hydration.wait_for(generation).await?;
        let mut ready = self.ready.clone();
        loop {
            if self.required.is_subset(&ready.borrow()) {
                break;
            }
            ready
                .changed()
                .await
                .context("local runtime component channel closed before Ready")?;
        }
        let final_receipt = self.hydration.wait_for(generation).await?;
        if final_receipt != initial_receipt {
            bail!("local hydration receipt changed while runtime components became Ready");
        }
        let receipt = self
            .typed_hydration
            .borrow()
            .clone()
            .context("typed hydration receipt disappeared before local Ready")?;
        validate_exact_hydration_receipt(&self.authority, &receipt)?;
        if receipt.stable_id() != final_receipt.receipt_identity {
            bail!("typed hydration receipt identity changed before local Ready");
        }
        Ok(LocalReadyProof {
            authority: self.authority.clone(),
            receipt,
            ready: final_receipt,
        })
    }
}

#[async_trait]
pub(crate) trait LocalReadyPublisher: Send + Sync + 'static {
    async fn publish_not_ready(&self) -> LocalPublicationResult<()>;
    async fn publish_ready(&self, proof: &LocalReadyProof) -> LocalPublicationResult<()>;
    async fn publish_shutdown_not_ready(&self) -> LocalPublicationResult<()>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PublishedPhase {
    New,
    NotReady,
    Ready(String),
    ShutdownNotReady,
}

#[derive(Clone, Debug)]
struct PublicationMachine {
    phase: PublishedPhase,
    revision: Option<u64>,
    pending: Option<LocalRuntimeStatePublication>,
}

/// Local/CI Ready publisher backed by the same authenticated Go control plane
/// that issues Gateway credentials.
pub(crate) struct LocalControlReadyPublisher {
    authority: RuntimeEpochAuthority,
    control: Arc<dyn LocalControlPlane>,
    machine: Mutex<PublicationMachine>,
}

impl LocalControlReadyPublisher {
    pub(crate) fn new(
        authority: RuntimeEpochAuthority,
        control: Arc<dyn LocalControlPlane>,
    ) -> Self {
        Self {
            authority,
            control,
            machine: Mutex::new(PublicationMachine {
                phase: PublishedPhase::New,
                revision: None,
                pending: None,
            }),
        }
    }

    async fn publish(
        &self,
        state: LocalRuntimePublicationState,
        receipt_identity: Option<String>,
        reason: LocalRuntimePublicationReason,
    ) -> LocalPublicationResult<()> {
        let mut machine = self.machine.lock().await;
        let desired = (state, receipt_identity.as_deref(), reason);
        let request = match machine.pending.as_ref() {
            Some(pending)
                if (
                    pending.state,
                    pending.hydration_receipt_identity.as_deref(),
                    pending.reason,
                ) == desired =>
            {
                pending.clone()
            }
            Some(_) => {
                return Err(LocalPublicationError::terminal(anyhow::anyhow!(
                    "a different local runtime-state transition is already pending"
                )));
            }
            None => {
                if !publication_transition_required(
                    &machine.phase,
                    state,
                    receipt_identity.as_deref(),
                    reason,
                )
                .map_err(LocalPublicationError::terminal)?
                {
                    return Ok(());
                }
                let request = LocalRuntimeStatePublication {
                    publication_id: Uuid::now_v7().hyphenated().to_string(),
                    personality_agent_id: self.authority.personality_agent_id().as_str().to_owned(),
                    generation: self.authority.generation().as_u64(),
                    rpc_boot_nonce: self.authority.nonce().as_str().to_owned(),
                    expected_revision: machine.revision,
                    state,
                    hydration_receipt_identity: receipt_identity,
                    reason,
                };
                machine.pending = Some(request.clone());
                request
            }
        };

        let ack = match self.control.publish_runtime_state(request.clone()).await {
            Ok(ack) => ack,
            Err(error) => {
                if !error.is_indeterminate() {
                    // A terminal preflight/auth/validation/rejection outcome
                    // is known not to require same-CAS reconciliation. Only
                    // ambiguous transport/ACK outcomes retain the request.
                    machine.pending = None;
                }
                return Err(error);
            }
        };
        validate_publication_ack(&self.authority, &request, &ack)
            .map_err(LocalPublicationError::indeterminate)?;
        machine.revision = Some(ack.revision);
        machine.pending = None;
        machine.phase = match (state, reason) {
            (LocalRuntimePublicationState::NotReady, LocalRuntimePublicationReason::Shutdown) => {
                PublishedPhase::ShutdownNotReady
            }
            (LocalRuntimePublicationState::NotReady, _) => PublishedPhase::NotReady,
            (LocalRuntimePublicationState::Ready, _) => PublishedPhase::Ready(
                request
                    .hydration_receipt_identity
                    .expect("Ready request was validated with a receipt"),
            ),
        };
        Ok(())
    }
}

#[async_trait]
impl LocalReadyPublisher for LocalControlReadyPublisher {
    async fn publish_not_ready(&self) -> LocalPublicationResult<()> {
        self.publish(
            LocalRuntimePublicationState::NotReady,
            None,
            LocalRuntimePublicationReason::Startup,
        )
        .await
    }

    async fn publish_ready(&self, proof: &LocalReadyProof) -> LocalPublicationResult<()> {
        if proof.authority != self.authority {
            return Err(LocalPublicationError::terminal(anyhow::anyhow!(
                "local Ready proof belongs to a different runtime epoch"
            )));
        }
        validate_exact_hydration_receipt(&self.authority, &proof.receipt)
            .map_err(LocalPublicationError::terminal)?;
        let receipt_identity = proof.receipt.stable_id();
        if proof.ready.generation != self.authority.generation()
            || proof.ready.receipt_identity != receipt_identity
        {
            return Err(LocalPublicationError::terminal(anyhow::anyhow!(
                "local Ready proof hydration identity mismatch"
            )));
        }
        self.publish(
            LocalRuntimePublicationState::Ready,
            Some(receipt_identity),
            LocalRuntimePublicationReason::Hydrated,
        )
        .await
    }

    async fn publish_shutdown_not_ready(&self) -> LocalPublicationResult<()> {
        self.publish(
            LocalRuntimePublicationState::NotReady,
            None,
            LocalRuntimePublicationReason::Shutdown,
        )
        .await
    }
}

fn publication_transition_required(
    phase: &PublishedPhase,
    state: LocalRuntimePublicationState,
    receipt_identity: Option<&str>,
    reason: LocalRuntimePublicationReason,
) -> Result<bool> {
    validate_publication_payload(state, receipt_identity, reason)?;
    match (state, receipt_identity, reason) {
        (LocalRuntimePublicationState::NotReady, None, LocalRuntimePublicationReason::Startup) => {
            match phase {
                PublishedPhase::New => Ok(true),
                PublishedPhase::NotReady => Ok(false),
                PublishedPhase::Ready(_) => {
                    bail!("Ready may only return to NotReady through the shutdown transition")
                }
                PublishedPhase::ShutdownNotReady => {
                    bail!("startup NotReady cannot be republished after shutdown")
                }
            }
        }
        (
            LocalRuntimePublicationState::Ready,
            Some(receipt),
            LocalRuntimePublicationReason::Hydrated,
        ) => match phase {
            PublishedPhase::NotReady => Ok(true),
            PublishedPhase::Ready(previous) if previous == receipt => Ok(false),
            PublishedPhase::Ready(_) => {
                bail!("local Ready receipt conflicts with the already published receipt")
            }
            PublishedPhase::New => bail!("local Ready requires an acknowledged NotReady first"),
            PublishedPhase::ShutdownNotReady => {
                bail!("local Ready cannot be republished after shutdown")
            }
        },
        (LocalRuntimePublicationState::NotReady, None, LocalRuntimePublicationReason::Shutdown) => {
            match phase {
                PublishedPhase::Ready(_) => Ok(true),
                PublishedPhase::ShutdownNotReady
                | PublishedPhase::New
                | PublishedPhase::NotReady => Ok(false),
            }
        }
        _ => unreachable!("publication payload was validated above"),
    }
}

fn validate_publication_payload(
    state: LocalRuntimePublicationState,
    receipt_identity: Option<&str>,
    reason: LocalRuntimePublicationReason,
) -> Result<()> {
    match (state, receipt_identity, reason) {
        (
            LocalRuntimePublicationState::NotReady,
            None,
            LocalRuntimePublicationReason::Startup | LocalRuntimePublicationReason::Shutdown,
        ) => Ok(()),
        (
            LocalRuntimePublicationState::Ready,
            Some(receipt),
            LocalRuntimePublicationReason::Hydrated,
        ) if receipt.len() == 64
            && receipt
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) =>
        {
            Ok(())
        }
        _ => bail!("invalid local runtime-state publication payload"),
    }
}

fn validate_publication_ack(
    authority: &RuntimeEpochAuthority,
    request: &LocalRuntimeStatePublication,
    ack: &LocalRuntimeStateAck,
) -> Result<()> {
    if ack.publication_id != request.publication_id
        || ack.personality_agent_id != authority.personality_agent_id().as_str()
        || ack.generation != authority.generation().as_u64()
        || ack.rpc_boot_nonce != authority.nonce().as_str()
        || ack.state != request.state
        || ack.hydration_receipt_identity != request.hydration_receipt_identity
    {
        bail!("local runtime-state acknowledgement scope or payload mismatch");
    }
    match request.expected_revision {
        Some(previous) => {
            let expected_revision = previous
                .checked_add(1)
                .context("local runtime-state CAS revision exhausted")?;
            if ack.revision != expected_revision {
                bail!(
                    "local runtime-state acknowledgement revision is not the exact next CAS revision"
                );
            }
        }
        None => {
            if request.state != LocalRuntimePublicationState::NotReady
                || request.hydration_receipt_identity.is_some()
                || request.reason != LocalRuntimePublicationReason::Startup
            {
                bail!(
                    "null local runtime-state CAS revision is only valid for startup NotReady without a receipt"
                );
            }
            if ack.revision == 0 {
                bail!("local runtime-state startup acknowledgement revision must be nonzero");
            }
        }
    }
    Ok(())
}

fn validate_exact_hydration_receipt(
    authority: &RuntimeEpochAuthority,
    receipt: &HydrationReceiptIdentity,
) -> Result<()> {
    if receipt.personality_agent_id != *authority.personality_agent_id()
        || receipt.generation != authority.generation()
        || receipt.lease_id != authority.lease().lease_id()
        || receipt.fence_id != authority.fence().fence_id()
        || receipt.intent_count != 0
    {
        bail!(
            "local Ready receipt does not match the exact PAID, lease, generation, fence, and clean intent set"
        );
    }
    Ok(())
}

fn system_time_from_unix(seconds: i64) -> Result<SystemTime> {
    let seconds = u64::try_from(seconds).context("local control timestamp must be nonnegative")?;
    UNIX_EPOCH
        .checked_add(Duration::from_secs(seconds))
        .context("local control timestamp overflow")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::sync::Mutex as StdMutex;

    use axum::Json;
    use axum::Router;
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::runtime::contracts::{
        GenerationRecoveryFence, PersonalityAgentId, ProcessGenerationLease, RpcBootNonce,
    };

    const PAID: &str = "0198f0f4-9b72-7000-8000-000000000001";
    const OTHER_PAID: &str = "0198f0f4-9b72-7000-8000-000000000002";

    struct TestSocketDir(PathBuf);

    impl TestSocketDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("su-{}", Uuid::now_v7().simple()));
            std::fs::create_dir(&path).unwrap();
            std::fs::set_permissions(
                &path,
                std::fs::Permissions::from_mode(TRUSTED_UNIX_PARENT_MODE),
            )
            .unwrap();
            Self(path)
        }

        fn socket(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TestSocketDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn authority_with(
        personality_agent_id: &str,
        generation: u64,
        nonce: &str,
    ) -> RuntimeEpochAuthority {
        let personality_agent_id = PersonalityAgentId::parse(personality_agent_id).unwrap();
        let generation = ProcessGeneration::from_wire(generation).unwrap();
        let rpc_identity = RpcIdentity::new(
            personality_agent_id.clone(),
            generation,
            RpcBootNonce::new(nonce).unwrap(),
        );
        let lease =
            ProcessGenerationLease::new(personality_agent_id, generation, "lease-a").unwrap();
        let fence = GenerationRecoveryFence::new(&lease, "fence-a").unwrap();
        RuntimeEpochAuthority::new(rpc_identity, lease, fence).unwrap()
    }

    fn authority() -> RuntimeEpochAuthority {
        authority_with(PAID, 7, "boot-a")
    }

    fn current_euid() -> u32 {
        unsafe { libc::geteuid() }
    }

    fn current_egid() -> u32 {
        unsafe { libc::getegid() }
    }

    fn receipt(authority: &RuntimeEpochAuthority) -> HydrationReceiptIdentity {
        HydrationReceiptIdentity {
            personality_agent_id: authority.personality_agent_id().clone(),
            lease_id: authority.lease().lease_id().to_owned(),
            generation: authority.generation(),
            fence_id: authority.fence().fence_id().to_owned(),
            intent_count: 0,
        }
    }

    async fn ready_proof(authority: &RuntimeEpochAuthority) -> LocalReadyProof {
        let (_hydration_tx, hydration_rx) = watch::channel(Some(receipt(authority)));
        let (controller, latch) = local_ready_gate(
            authority.clone(),
            hydration_rx,
            [LocalRuntimeComponent::Session],
        )
        .unwrap();
        controller
            .mark_ready(authority.rpc_identity(), LocalRuntimeComponent::Session)
            .unwrap();
        latch.wait_for_proof(authority.generation()).await.unwrap()
    }

    #[derive(Default)]
    struct FakeState {
        credential_count: u64,
        credential_requests: Vec<LocalCredentialIssueRequest>,
        credential_response_token: Option<String>,
        publications: Vec<LocalRuntimeStatePublication>,
        publication_attempts: Vec<LocalRuntimeStatePublication>,
        publication_acks: BTreeMap<String, LocalRuntimeStateAck>,
        revision: u64,
        receipt: Option<String>,
        accept_rollover_startup: bool,
        drop_next_publication_ack: bool,
        credential_response_nonce: Option<String>,
        credential_response_expiry: Option<i64>,
    }

    struct FakeControlPlane {
        expected: RuntimeEpochAuthority,
        state: StdMutex<FakeState>,
    }

    impl FakeControlPlane {
        fn new(expected: RuntimeEpochAuthority) -> Self {
            Self {
                expected,
                state: StdMutex::new(FakeState::default()),
            }
        }
    }

    #[async_trait]
    impl LocalControlPlane for FakeControlPlane {
        async fn issue_gateway_credential(
            &self,
            request: LocalCredentialIssueRequest,
        ) -> Result<LocalCredentialIssueResponse> {
            if request.personality_agent_id != self.expected.personality_agent_id().as_str()
                || request.generation != self.expected.generation().as_u64()
                || request.rpc_boot_nonce != self.expected.nonce().as_str()
            {
                bail!("fake local control rejected stale runtime credential request");
            }
            let mut state = self.state.lock().unwrap();
            state.credential_count += 1;
            state.credential_requests.push(request.clone());
            let expires_at_unix = state
                .credential_response_expiry
                .unwrap_or_else(|| unix_now() + 30);
            Ok(LocalCredentialIssueResponse {
                request_id: request.request_id,
                personality_agent_id: request.personality_agent_id,
                generation: request.generation,
                rpc_boot_nonce: state
                    .credential_response_nonce
                    .clone()
                    .unwrap_or(request.rpc_boot_nonce),
                audience: request.audience,
                expires_at_unix,
                delivery_authorization: DeliveryAuthorization::Raw,
                token: state
                    .credential_response_token
                    .clone()
                    .unwrap_or_else(|| format!("opaque-gateway-token-{}", state.credential_count)),
            })
        }

        async fn publish_runtime_state(
            &self,
            publication: LocalRuntimeStatePublication,
        ) -> LocalPublicationResult<LocalRuntimeStateAck> {
            if publication.personality_agent_id != self.expected.personality_agent_id().as_str()
                || publication.generation != self.expected.generation().as_u64()
                || publication.rpc_boot_nonce != self.expected.nonce().as_str()
            {
                return Err(LocalPublicationError::terminal(anyhow::anyhow!(
                    "fake local registry rejected stale runtime epoch"
                )));
            }
            let mut state = self.state.lock().unwrap();
            state.publication_attempts.push(publication.clone());
            if let Some(ack) = state
                .publication_acks
                .get(&publication.publication_id)
                .cloned()
            {
                if !state
                    .publications
                    .iter()
                    .any(|committed| committed == &publication)
                {
                    return Err(LocalPublicationError::terminal(anyhow::anyhow!(
                        "fake local registry rejected duplicate-different publication"
                    )));
                }
                return Ok(ack);
            }
            let is_rollover_startup = state.accept_rollover_startup
                && state.revision > 0
                && publication.expected_revision.is_none()
                && publication.state == LocalRuntimePublicationState::NotReady
                && publication.hydration_receipt_identity.is_none()
                && publication.reason == LocalRuntimePublicationReason::Startup;
            if !is_rollover_startup
                && publication.expected_revision != (state.revision > 0).then_some(state.revision)
            {
                return Err(LocalPublicationError::terminal(anyhow::anyhow!(
                    "fake local registry rejected stale CAS revision"
                )));
            }
            state.accept_rollover_startup = false;
            match publication.state {
                LocalRuntimePublicationState::NotReady => {
                    state.receipt = None;
                }
                LocalRuntimePublicationState::Ready => {
                    let receipt = publication
                        .hydration_receipt_identity
                        .clone()
                        .context("fake Ready requires receipt")
                        .map_err(LocalPublicationError::terminal)?;
                    if state
                        .receipt
                        .as_ref()
                        .is_some_and(|current| current != &receipt)
                    {
                        return Err(LocalPublicationError::terminal(anyhow::anyhow!(
                            "fake local registry rejected duplicate-different Ready"
                        )));
                    }
                    state.receipt = Some(receipt);
                }
            }
            state.revision += 1;
            let ack = LocalRuntimeStateAck {
                publication_id: publication.publication_id.clone(),
                personality_agent_id: publication.personality_agent_id.clone(),
                generation: publication.generation,
                rpc_boot_nonce: publication.rpc_boot_nonce.clone(),
                revision: state.revision,
                state: publication.state,
                hydration_receipt_identity: publication.hydration_receipt_identity.clone(),
            };
            state.publications.push(publication.clone());
            state
                .publication_acks
                .insert(publication.publication_id, ack.clone());
            if state.drop_next_publication_ack {
                state.drop_next_publication_ack = false;
                return Err(LocalPublicationError::indeterminate(anyhow::anyhow!(
                    "simulated response loss after registry commit"
                )));
            }
            Ok(ack)
        }
    }

    fn unix_now() -> i64 {
        i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn every_reconnect_requests_and_consumes_a_distinct_fresh_grant() {
        let expected = authority();
        let control = Arc::new(FakeControlPlane::new(expected.clone()));
        let mut provider = LocalCredentialProvider::new(
            expected.clone(),
            DeliveryAuthorization::Raw,
            control.clone(),
        );
        let first = provider.fresh_credential().await.unwrap();
        let second = provider.fresh_credential().await.unwrap();
        assert_ne!(first.token(), second.token());
        assert_eq!(
            first.personality_agent_id(),
            expected.personality_agent_id()
        );
        assert_eq!(second.generation(), expected.generation());
        let state = control.state.lock().unwrap();
        assert_eq!(state.credential_requests.len(), 2);
        assert_ne!(
            state.credential_requests[0].request_id,
            state.credential_requests[1].request_id
        );
    }

    #[tokio::test]
    async fn reconnect_accepts_fresh_grants_with_identical_token_bytes() {
        let expected = authority();
        let control = Arc::new(FakeControlPlane::new(expected.clone()));
        control.state.lock().unwrap().credential_response_token =
            Some("issuer-reused-token".to_owned());
        let mut provider =
            LocalCredentialProvider::new(expected, DeliveryAuthorization::Raw, control.clone());
        let first = provider.fresh_credential().await.unwrap();
        let second = provider.fresh_credential().await.unwrap();
        assert_eq!(first.token(), "issuer-reused-token");
        assert_eq!(second.token(), "issuer-reused-token");

        let state = control.state.lock().unwrap();
        assert_eq!(state.credential_requests.len(), 2);
        assert_ne!(
            state.credential_requests[0].request_id,
            state.credential_requests[1].request_id
        );
    }

    #[tokio::test]
    async fn credential_request_and_response_bind_paid_generation_and_nonce() {
        for candidate in [
            authority_with(OTHER_PAID, 7, "boot-a"),
            authority_with(PAID, 8, "boot-a"),
            authority_with(PAID, 7, "boot-b"),
        ] {
            let expected = authority();
            let control = Arc::new(FakeControlPlane::new(expected));
            let mut provider =
                LocalCredentialProvider::new(candidate, DeliveryAuthorization::Raw, control);
            assert!(provider.fresh_credential().await.is_err());
        }

        let expected = authority();
        let control = Arc::new(FakeControlPlane::new(expected.clone()));
        control.state.lock().unwrap().credential_response_nonce = Some("boot-b".to_owned());
        let mut provider =
            LocalCredentialProvider::new(expected, DeliveryAuthorization::Raw, control);
        let error = provider
            .fresh_credential()
            .await
            .expect_err("cross-nonce response must fail closed");
        assert!(error.to_string().contains("scope mismatch"));
    }

    #[tokio::test]
    async fn expired_credential_response_is_rejected() {
        let expected = authority();
        let control = Arc::new(FakeControlPlane::new(expected.clone()));
        control.state.lock().unwrap().credential_response_expiry = Some(unix_now());
        let mut provider =
            LocalCredentialProvider::new(expected, DeliveryAuthorization::Raw, control);
        assert!(
            provider
                .fresh_credential()
                .await
                .unwrap_err()
                .to_string()
                .contains("expired")
        );
    }

    #[test]
    fn credential_grant_is_one_use_and_expires_at_the_boundary() {
        let expected = authority();
        let now = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let mut grant = LocalCredentialGrant {
            token: Some("opaque".to_owned()),
            identity: expected.rpc_identity().clone(),
            expires_at: now + Duration::from_secs(30),
            delivery_authorization: DeliveryAuthorization::Raw,
        };
        grant
            .consume_at(&expected, now + Duration::from_secs(1))
            .unwrap();
        assert!(
            grant
                .consume_at(&expected, now + Duration::from_secs(2))
                .unwrap_err()
                .to_string()
                .contains("already consumed")
        );
        let mut expired = LocalCredentialGrant {
            token: Some("opaque".to_owned()),
            identity: expected.rpc_identity().clone(),
            expires_at: now + Duration::from_secs(30),
            delivery_authorization: DeliveryAuthorization::Raw,
        };
        assert!(
            expired
                .consume_at(&expected, now + Duration::from_secs(30))
                .unwrap_err()
                .to_string()
                .contains("expired")
        );
    }

    #[tokio::test]
    async fn local_ready_waits_for_exact_hydration_and_every_required_component() {
        let expected = authority();
        let (hydration_tx, hydration_rx) = watch::channel(None);
        let required = [
            LocalRuntimeComponent::GatewayConnector,
            LocalRuntimeComponent::Session,
        ];
        let (controller, latch) =
            local_ready_gate(expected.clone(), hydration_rx, required).unwrap();
        let generation = expected.generation();
        let wait = tokio::spawn(async move { latch.wait_for_proof(generation).await });

        controller
            .mark_ready(
                expected.rpc_identity(),
                LocalRuntimeComponent::GatewayConnector,
            )
            .unwrap();
        tokio::task::yield_now().await;
        assert!(!wait.is_finished());
        hydration_tx.send_replace(Some(receipt(&expected)));
        tokio::task::yield_now().await;
        assert!(!wait.is_finished());
        controller
            .mark_ready(expected.rpc_identity(), LocalRuntimeComponent::Session)
            .unwrap();
        let proof = wait.await.unwrap().unwrap();
        assert_eq!(
            proof.hydration_ready().receipt_identity,
            receipt(&expected).stable_id()
        );
        assert_eq!(proof.authority, expected);
    }

    #[test]
    fn first_browser_gate_requires_the_closed_component_set() {
        let expected = authority();
        let (_tx, rx) = watch::channel(None);
        let (controller, _latch) = first_browser_vertical_ready_gate(expected.clone(), rx);
        for component in FIRST_BROWSER_VERTICAL_COMPONENTS {
            controller
                .mark_ready(expected.rpc_identity(), component)
                .unwrap();
        }
    }

    #[test]
    fn component_marks_bind_paid_generation_and_nonce() {
        let expected = authority();
        let (_tx, rx) = watch::channel(None);
        let (controller, _latch) =
            local_ready_gate(expected, rx, [LocalRuntimeComponent::Session]).unwrap();
        for candidate in [
            authority_with(OTHER_PAID, 7, "boot-a"),
            authority_with(PAID, 8, "boot-a"),
            authority_with(PAID, 7, "boot-b"),
        ] {
            assert!(
                controller
                    .mark_ready(candidate.rpc_identity(), LocalRuntimeComponent::Session)
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn publisher_orders_not_ready_exact_ready_and_shutdown_not_ready() {
        let expected = authority();
        let control = Arc::new(FakeControlPlane::new(expected.clone()));
        let publisher = LocalControlReadyPublisher::new(expected.clone(), control.clone());
        let exact = ready_proof(&expected).await;

        publisher.publish_not_ready().await.unwrap();
        publisher.publish_ready(&exact).await.unwrap();
        publisher.publish_ready(&exact).await.unwrap();

        let mut conflicting = exact.clone();
        conflicting.receipt.fence_id = "different-fence".to_owned();
        assert!(publisher.publish_ready(&conflicting).await.is_err());

        publisher.publish_shutdown_not_ready().await.unwrap();
        let state = control.state.lock().unwrap();
        assert_eq!(state.publications.len(), 3);
        assert_eq!(
            state
                .publications
                .iter()
                .map(|publication| publication.state)
                .collect::<Vec<_>>(),
            vec![
                LocalRuntimePublicationState::NotReady,
                LocalRuntimePublicationState::Ready,
                LocalRuntimePublicationState::NotReady,
            ]
        );
        assert_eq!(
            state.publications[2].reason,
            LocalRuntimePublicationReason::Shutdown
        );
        assert!(state.receipt.is_none());
    }

    #[tokio::test]
    async fn publisher_rollover_seeds_ready_cas_from_authoritative_startup_ack() {
        let expected = authority();
        let control = Arc::new(FakeControlPlane::new(expected.clone()));
        {
            let mut state = control.state.lock().unwrap();
            state.revision = 2;
            state.receipt = Some("old-generation-receipt".to_owned());
            state.accept_rollover_startup = true;
            state.drop_next_publication_ack = true;
        }
        let publisher = LocalControlReadyPublisher::new(expected.clone(), control.clone());
        let exact = ready_proof(&expected).await;

        let error = publisher
            .publish_not_ready()
            .await
            .expect_err("lost rollover startup ACK is indeterminate");
        assert!(error.is_indeterminate());
        publisher
            .publish_not_ready()
            .await
            .expect("rollover startup reconciles its authoritative revision");
        publisher
            .publish_ready(&exact)
            .await
            .expect("Ready advances from the rollover startup revision");

        let state = control.state.lock().unwrap();
        assert_eq!(state.publication_attempts.len(), 3);
        assert_eq!(
            state.publication_attempts[0], state.publication_attempts[1],
            "indeterminate startup must retry the identical publication"
        );
        assert_eq!(state.publication_attempts[0].expected_revision, None);
        assert_eq!(
            state.publication_attempts[2].expected_revision,
            Some(3),
            "Ready must seed its CAS from the authoritative startup ACK"
        );
        assert_eq!(state.revision, 4);
    }

    #[tokio::test]
    async fn publisher_rejects_a_ready_proof_from_another_boot_epoch() {
        let expected = authority();
        let control = Arc::new(FakeControlPlane::new(expected.clone()));
        let publisher = LocalControlReadyPublisher::new(expected, control);
        publisher.publish_not_ready().await.unwrap();

        let other_boot = authority_with(PAID, 7, "boot-b");
        let other_proof = ready_proof(&other_boot).await;
        let error = publisher
            .publish_ready(&other_proof)
            .await
            .expect_err("same PAID/generation with another boot nonce must fail closed");
        assert!(!error.is_indeterminate());
        assert!(error.to_string().contains("different runtime epoch"));
    }

    #[tokio::test]
    async fn publisher_retries_the_same_id_after_ack_loss_and_rejects_stale_epoch() {
        let expected = authority();
        let control = Arc::new(FakeControlPlane::new(expected.clone()));
        control.state.lock().unwrap().drop_next_publication_ack = true;
        let publisher = LocalControlReadyPublisher::new(expected.clone(), control.clone());
        let error = publisher
            .publish_not_ready()
            .await
            .expect_err("lost ACK leaves the publication indeterminate");
        assert!(error.is_indeterminate());
        assert!(
            publisher
                .publish_shutdown_not_ready()
                .await
                .unwrap_err()
                .to_string()
                .contains("different local runtime-state transition")
        );
        publisher.publish_not_ready().await.unwrap();
        {
            let state = control.state.lock().unwrap();
            assert_eq!(state.publications.len(), 1);
            assert_eq!(state.publication_attempts.len(), 2);
            assert_eq!(
                state.publication_attempts[0].publication_id,
                state.publication_attempts[1].publication_id
            );
            assert_eq!(state.publication_acks.len(), 1);
        }

        let stale = authority_with(PAID, 8, "boot-stale");
        let stale_publisher = LocalControlReadyPublisher::new(stale, control);
        assert!(stale_publisher.publish_not_ready().await.is_err());
    }

    #[tokio::test]
    async fn signal_after_ready_commit_ack_loss_reconciles_same_cas_before_shutdown() {
        let expected = authority();
        let control = Arc::new(FakeControlPlane::new(expected.clone()));
        let publisher = LocalControlReadyPublisher::new(expected.clone(), control.clone());
        let proof = ready_proof(&expected).await;
        let shutdown = CancellationToken::new();

        publisher.publish_not_ready().await.unwrap();
        control.state.lock().unwrap().drop_next_publication_ack = true;
        let error = publisher
            .publish_ready(&proof)
            .await
            .expect_err("committed Ready ACK is lost");
        assert!(error.is_indeterminate());
        assert!(error.to_string().contains("response loss"));
        assert!(
            control.state.lock().unwrap().receipt.is_some(),
            "the registry committed Ready even though its ACK was lost"
        );
        shutdown.cancel();
        assert!(shutdown.is_cancelled(), "test signal is now observed");
        publisher
            .publish_ready(&proof)
            .await
            .expect("retry must reconcile the retained Ready publication");
        publisher.publish_shutdown_not_ready().await.unwrap();

        let state = control.state.lock().unwrap();
        assert_eq!(state.publications.len(), 3);
        assert_eq!(state.publication_attempts.len(), 4);
        let first_ready = &state.publication_attempts[1];
        let retried_ready = &state.publication_attempts[2];
        assert_eq!(first_ready, retried_ready);
        assert_eq!(first_ready.expected_revision, Some(1));
        assert_eq!(retried_ready.expected_revision, Some(1));
        assert_eq!(
            state.publication_attempts[3].expected_revision,
            Some(2),
            "shutdown must advance from the reconciled Ready revision"
        );
        assert_eq!(
            state.publication_attempts[3].reason,
            LocalRuntimePublicationReason::Shutdown
        );
    }

    #[test]
    fn publication_ack_requires_exact_scope_payload_and_next_cas_revision() {
        let expected = authority();
        let request = LocalRuntimeStatePublication {
            publication_id: "0198f0f4-9b72-7000-8000-000000000020".to_owned(),
            personality_agent_id: PAID.to_owned(),
            generation: 7,
            rpc_boot_nonce: "boot-a".to_owned(),
            expected_revision: Some(4),
            state: LocalRuntimePublicationState::Ready,
            hydration_receipt_identity: Some(receipt(&expected).stable_id()),
            reason: LocalRuntimePublicationReason::Hydrated,
        };
        let ack_for =
            |request: &LocalRuntimeStatePublication, revision: u64| LocalRuntimeStateAck {
                publication_id: request.publication_id.clone(),
                personality_agent_id: request.personality_agent_id.clone(),
                generation: request.generation,
                rpc_boot_nonce: request.rpc_boot_nonce.clone(),
                revision,
                state: request.state,
                hydration_receipt_identity: request.hydration_receipt_identity.clone(),
            };
        let exact = ack_for(&request, 5);
        validate_publication_ack(&expected, &request, &exact).unwrap();

        let mut mismatches = Vec::new();
        let mut mismatch = exact.clone();
        mismatch.publication_id = "different-publication".to_owned();
        mismatches.push(mismatch);
        let mut mismatch = exact.clone();
        mismatch.personality_agent_id = OTHER_PAID.to_owned();
        mismatches.push(mismatch);
        let mut mismatch = exact.clone();
        mismatch.generation = 8;
        mismatches.push(mismatch);
        let mut mismatch = exact.clone();
        mismatch.rpc_boot_nonce = "boot-b".to_owned();
        mismatches.push(mismatch);
        let mut mismatch = exact.clone();
        mismatch.state = LocalRuntimePublicationState::NotReady;
        mismatches.push(mismatch);
        let mut mismatch = exact.clone();
        mismatch.hydration_receipt_identity = None;
        mismatches.push(mismatch);
        let mut mismatch = exact;
        mismatch.revision = 6;
        mismatches.push(mismatch);
        for mismatch in mismatches {
            assert!(validate_publication_ack(&expected, &request, &mismatch).is_err());
        }

        let mut invalid_epoch_start = request.clone();
        invalid_epoch_start.expected_revision = None;
        assert!(
            validate_publication_ack(
                &expected,
                &invalid_epoch_start,
                &ack_for(&invalid_epoch_start, 1)
            )
            .is_err()
        );

        let startup_request = LocalRuntimeStatePublication {
            publication_id: "0198f0f4-9b72-7000-8000-000000000021".to_owned(),
            personality_agent_id: PAID.to_owned(),
            generation: 7,
            rpc_boot_nonce: "boot-a".to_owned(),
            expected_revision: None,
            state: LocalRuntimePublicationState::NotReady,
            hydration_receipt_identity: None,
            reason: LocalRuntimePublicationReason::Startup,
        };
        let mut startup_ack = ack_for(&startup_request, 3);
        validate_publication_ack(&expected, &startup_request, &startup_ack).unwrap();
        startup_ack.revision = 1;
        validate_publication_ack(&expected, &startup_request, &startup_ack).unwrap();
        startup_ack.revision = 0;
        assert!(validate_publication_ack(&expected, &startup_request, &startup_ack).is_err());

        let mut shutdown_epoch_start = startup_request.clone();
        shutdown_epoch_start.reason = LocalRuntimePublicationReason::Shutdown;
        assert!(
            validate_publication_ack(
                &expected,
                &shutdown_epoch_start,
                &ack_for(&shutdown_epoch_start, 4)
            )
            .is_err()
        );

        let mut receipt_epoch_start = startup_request.clone();
        receipt_epoch_start.hydration_receipt_identity = Some(receipt(&expected).stable_id());
        assert!(
            validate_publication_ack(
                &expected,
                &receipt_epoch_start,
                &ack_for(&receipt_epoch_start, 4)
            )
            .is_err()
        );

        let mut exhausted = request;
        exhausted.expected_revision = Some(u64::MAX);
        assert!(
            validate_publication_ack(&expected, &exhausted, &ack_for(&exhausted, 0))
                .unwrap_err()
                .to_string()
                .contains("exhausted")
        );
    }

    #[tokio::test]
    async fn credential_secrets_are_redacted_from_debug_and_errors() {
        let expected = authority();
        let control_credential = LocalControlCredential::new(
            "control-secret-sentinel",
            expected.rpc_identity().clone(),
            SystemTime::now() + Duration::from_secs(30),
        )
        .unwrap();
        assert!(!format!("{control_credential:?}").contains("control-secret-sentinel"));

        let response = LocalCredentialIssueResponse {
            request_id: "request-a".to_owned(),
            personality_agent_id: PAID.to_owned(),
            generation: 7,
            rpc_boot_nonce: "boot-b".to_owned(),
            audience: LOCAL_AGENT_AUDIENCE.to_owned(),
            expires_at_unix: unix_now() + 30,
            delivery_authorization: DeliveryAuthorization::Raw,
            token: "gateway-secret-sentinel".to_owned(),
        };
        assert!(!format!("{response:?}").contains("gateway-secret-sentinel"));

        let control = Arc::new(FakeControlPlane::new(expected.clone()));
        {
            let mut state = control.state.lock().unwrap();
            state.credential_response_nonce = Some("boot-b".to_owned());
            state.credential_response_token = Some("error-secret-sentinel".to_owned());
        }
        let mut provider =
            LocalCredentialProvider::new(expected, DeliveryAuthorization::Raw, control);
        let error = provider.fresh_credential().await.unwrap_err();
        assert!(!format!("{error:#}").contains("error-secret-sentinel"));
    }

    #[test]
    fn loopback_client_rejects_non_loopback_and_expired_control_credentials() {
        let expected = authority();
        let credential = LocalControlCredential::new(
            "control-secret",
            expected.rpc_identity().clone(),
            SystemTime::now() + Duration::from_secs(30),
        )
        .unwrap();
        assert!(
            LocalControlHttpClient::new_loopback(
                "http://192.0.2.1:8080",
                expected.clone(),
                credential,
            )
            .is_err()
        );

        let expired = LocalControlCredential::new(
            "control-secret",
            expected.rpc_identity().clone(),
            SystemTime::now(),
        )
        .unwrap();
        assert!(
            LocalControlHttpClient::new_loopback("http://127.0.0.1:8080", expected, expired)
                .is_err()
        );
    }

    #[tokio::test]
    async fn loopback_client_rejects_cross_epoch_payloads_before_http() {
        let expected = authority();
        let credential = LocalControlCredential::new(
            "control-secret",
            expected.rpc_identity().clone(),
            SystemTime::now() + Duration::from_secs(30),
        )
        .unwrap();
        let client = LocalControlHttpClient::new_loopback(
            "http://127.0.0.1:9",
            expected.clone(),
            credential,
        )
        .unwrap();

        let credential_request = LocalCredentialIssueRequest {
            request_id: "request-a".to_owned(),
            personality_agent_id: PAID.to_owned(),
            generation: 7,
            rpc_boot_nonce: "boot-b".to_owned(),
            audience: LOCAL_AGENT_AUDIENCE.to_owned(),
        };
        assert!(
            client
                .issue_gateway_credential(credential_request)
                .await
                .unwrap_err()
                .to_string()
                .contains("runtime epoch mismatch")
        );

        let wrong_audience = LocalCredentialIssueRequest {
            request_id: "request-b".to_owned(),
            personality_agent_id: PAID.to_owned(),
            generation: expected.generation().as_u64(),
            rpc_boot_nonce: expected.nonce().as_str().to_owned(),
            audience: "sumi:different-service".to_owned(),
        };
        assert!(
            client
                .issue_gateway_credential(wrong_audience)
                .await
                .unwrap_err()
                .to_string()
                .contains("audience mismatch")
        );

        let publication = LocalRuntimeStatePublication {
            publication_id: "publication-a".to_owned(),
            personality_agent_id: OTHER_PAID.to_owned(),
            generation: expected.generation().as_u64(),
            rpc_boot_nonce: expected.nonce().as_str().to_owned(),
            expected_revision: None,
            state: LocalRuntimePublicationState::NotReady,
            hydration_receipt_identity: None,
            reason: LocalRuntimePublicationReason::Startup,
        };
        let error = client
            .publish_runtime_state(publication)
            .await
            .expect_err("cross-epoch publication must fail before transport");
        assert!(!error.is_indeterminate());
        assert!(error.to_string().contains("runtime epoch mismatch"));
    }

    async fn read_unix_http_request(
        stream: &mut tokio::net::UnixStream,
    ) -> (String, String, Vec<u8>) {
        let mut request = Vec::new();
        let (header_end, content_length) = loop {
            assert!(request.len() <= MAX_LOCAL_CONTROL_RESPONSE_BYTES);
            let mut chunk = [0_u8; 4096];
            let read = stream.read(&mut chunk).await.unwrap();
            assert!(read > 0, "HTTP client closed before sending a request");
            request.extend_from_slice(&chunk[..read]);
            let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n") else {
                continue;
            };
            let headers = std::str::from_utf8(&request[..header_end]).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            break (header_end + 4, content_length);
        };
        while request.len() < header_end + content_length {
            let mut chunk = [0_u8; 4096];
            let read = stream.read(&mut chunk).await.unwrap();
            assert!(read > 0);
            request.extend_from_slice(&chunk[..read]);
        }
        let headers = std::str::from_utf8(&request[..header_end]).unwrap();
        let request_line = headers.lines().next().unwrap().to_owned();
        let authorization = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("authorization")
                    .then(|| value.trim().to_owned())
            })
            .unwrap();
        (
            request_line,
            authorization,
            request[header_end..header_end + content_length].to_vec(),
        )
    }

    async fn write_unix_http_json(stream: &mut tokio::net::UnixStream, value: &impl Serialize) {
        let body = serde_json::to_vec(value).unwrap();
        let headers = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(headers.as_bytes()).await.unwrap();
        stream.write_all(&body).await.unwrap();
        stream.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unix_client_round_trip_sends_exact_bearer_and_epoch_bodies() {
        let directory = TestSocketDir::new();
        let socket_path = directory.socket("control.sock");
        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        std::fs::set_permissions(
            &socket_path,
            std::fs::Permissions::from_mode(TRUSTED_UNIX_SOCKET_MODE),
        )
        .unwrap();

        let server = tokio::spawn(async move {
            let (mut issue_stream, _) = listener.accept().await.unwrap();
            let (request_line, authorization, body) =
                read_unix_http_request(&mut issue_stream).await;
            assert_eq!(
                request_line,
                format!("POST /{ISSUE_CREDENTIAL_PATH} HTTP/1.1")
            );
            assert_eq!(authorization, "Bearer control-secret");
            let issue: LocalCredentialIssueRequest = serde_json::from_slice(&body).unwrap();
            assert_eq!(issue.personality_agent_id, PAID);
            assert_eq!(issue.generation, 7);
            assert_eq!(issue.rpc_boot_nonce, "boot-a");
            assert_eq!(issue.audience, LOCAL_AGENT_AUDIENCE);
            let issue_response = LocalCredentialIssueResponse {
                request_id: issue.request_id,
                personality_agent_id: issue.personality_agent_id,
                generation: issue.generation,
                rpc_boot_nonce: issue.rpc_boot_nonce,
                audience: issue.audience,
                expires_at_unix: unix_now() + 30,
                delivery_authorization: DeliveryAuthorization::Raw,
                token: "fixture-issued-token".to_owned(),
            };
            write_unix_http_json(&mut issue_stream, &issue_response).await;

            let (mut publish_stream, _) = listener.accept().await.unwrap();
            let (request_line, authorization, body) =
                read_unix_http_request(&mut publish_stream).await;
            assert_eq!(
                request_line,
                format!("POST /{PUBLISH_RUNTIME_STATE_PATH} HTTP/1.1")
            );
            assert_eq!(authorization, "Bearer control-secret");
            let publication: LocalRuntimeStatePublication = serde_json::from_slice(&body).unwrap();
            assert_eq!(publication.personality_agent_id, PAID);
            assert_eq!(publication.generation, 7);
            assert_eq!(publication.rpc_boot_nonce, "boot-a");
            assert_eq!(publication.state, LocalRuntimePublicationState::NotReady);
            assert_eq!(publication.reason, LocalRuntimePublicationReason::Startup);
            let ack = LocalRuntimeStateAck {
                publication_id: publication.publication_id,
                personality_agent_id: publication.personality_agent_id,
                generation: publication.generation,
                rpc_boot_nonce: publication.rpc_boot_nonce,
                revision: 1,
                state: publication.state,
                hydration_receipt_identity: publication.hydration_receipt_identity,
            };
            write_unix_http_json(&mut publish_stream, &ack).await;
        });

        let expected = authority();
        let credential = LocalControlCredential::new(
            "control-secret",
            expected.rpc_identity().clone(),
            SystemTime::now() + Duration::from_secs(30),
        )
        .unwrap();
        let control = Arc::new(
            LocalControlHttpClient::new_unix(
                &socket_path,
                current_euid(),
                current_egid(),
                expected.clone(),
                credential,
            )
            .unwrap(),
        );
        let mut provider = LocalCredentialProvider::new(
            expected.clone(),
            DeliveryAuthorization::Raw,
            control.clone(),
        );
        assert_eq!(
            provider.fresh_credential().await.unwrap().token(),
            "fixture-issued-token"
        );
        let publisher = LocalControlReadyPublisher::new(expected, control);
        publisher.publish_not_ready().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn unix_client_rejects_socket_replacement_before_sending_bearer_or_body() {
        let directory = TestSocketDir::new();
        let socket_path = directory.socket("control.sock");
        let original = tokio::net::UnixListener::bind(&socket_path).unwrap();
        std::fs::set_permissions(
            &socket_path,
            std::fs::Permissions::from_mode(TRUSTED_UNIX_SOCKET_MODE),
        )
        .unwrap();

        let expected = authority();
        let credential = LocalControlCredential::new(
            "replacement-test-secret",
            expected.rpc_identity().clone(),
            SystemTime::now() + Duration::from_secs(30),
        )
        .unwrap();
        let client = LocalControlHttpClient::new_unix(
            &socket_path,
            current_euid(),
            current_egid(),
            expected.clone(),
            credential,
        )
        .unwrap();

        drop(original);
        let old_inode = directory.socket("old-inode.sock");
        std::fs::hard_link(&socket_path, &old_inode).unwrap();
        std::fs::remove_file(&socket_path).unwrap();
        let replacement = tokio::net::UnixListener::bind(&socket_path).unwrap();
        std::fs::set_permissions(
            &socket_path,
            std::fs::Permissions::from_mode(TRUSTED_UNIX_SOCKET_MODE),
        )
        .unwrap();
        let replacement_observation = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(250), replacement.accept())
                .await
                .is_ok()
        });

        let request = LocalCredentialIssueRequest {
            request_id: "replacement-request".to_owned(),
            personality_agent_id: PAID.to_owned(),
            generation: expected.generation().as_u64(),
            rpc_boot_nonce: expected.nonce().as_str().to_owned(),
            audience: LOCAL_AGENT_AUDIENCE.to_owned(),
        };
        let error = client.issue_gateway_credential(request).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("identity changed after client construction")
        );
        assert!(!format!("{error:#}").contains("replacement-test-secret"));
        assert!(
            !replacement_observation.await.unwrap(),
            "replacement socket received a connection carrying local-control authority"
        );
    }

    #[test]
    fn unix_client_rejects_wrong_mode_symlink_and_hardlinked_socket() {
        let expected = authority();
        let credential = || {
            LocalControlCredential::new(
                "control-secret",
                expected.rpc_identity().clone(),
                SystemTime::now() + Duration::from_secs(30),
            )
            .unwrap()
        };

        let wrong_uid_dir = TestSocketDir::new();
        let wrong_uid_path = wrong_uid_dir.socket("wrong-uid.sock");
        let _wrong_uid_listener = std::os::unix::net::UnixListener::bind(&wrong_uid_path).unwrap();
        std::fs::set_permissions(
            &wrong_uid_path,
            std::fs::Permissions::from_mode(TRUSTED_UNIX_SOCKET_MODE),
        )
        .unwrap();
        let wrong_expected_uid = if current_euid() == u32::MAX {
            current_euid() - 1
        } else {
            current_euid() + 1
        };
        let error = LocalControlHttpClient::new_unix(
            &wrong_uid_path,
            wrong_expected_uid,
            current_egid(),
            expected.clone(),
            credential(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("SUMI_LOCAL_CONTROL_SERVER_UID"));

        let wrong_gid_dir = TestSocketDir::new();
        let wrong_gid_path = wrong_gid_dir.socket("wrong-gid.sock");
        let _wrong_gid_listener = std::os::unix::net::UnixListener::bind(&wrong_gid_path).unwrap();
        std::fs::set_permissions(
            &wrong_gid_path,
            std::fs::Permissions::from_mode(TRUSTED_UNIX_SOCKET_MODE),
        )
        .unwrap();
        let wrong_expected_gid = if current_egid() == u32::MAX {
            current_egid() - 1
        } else {
            current_egid() + 1
        };
        let error = LocalControlHttpClient::new_unix(
            &wrong_gid_path,
            current_euid(),
            wrong_expected_gid,
            expected.clone(),
            credential(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("SUMI_LOCAL_CONTROL_SOCKET_GID"));

        let wrong_mode_dir = TestSocketDir::new();
        let wrong_mode_path = wrong_mode_dir.socket("wrong-mode.sock");
        let _wrong_mode_listener =
            std::os::unix::net::UnixListener::bind(&wrong_mode_path).unwrap();
        std::fs::set_permissions(&wrong_mode_path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let error = LocalControlHttpClient::new_unix(
            &wrong_mode_path,
            current_euid(),
            current_egid(),
            expected.clone(),
            credential(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("mode must be 0660"));

        let symlink_dir = TestSocketDir::new();
        let real_path = symlink_dir.socket("real.sock");
        let _real_listener = std::os::unix::net::UnixListener::bind(&real_path).unwrap();
        std::fs::set_permissions(
            &real_path,
            std::fs::Permissions::from_mode(TRUSTED_UNIX_SOCKET_MODE),
        )
        .unwrap();
        let linked_path = symlink_dir.socket("linked.sock");
        symlink(&real_path, &linked_path).unwrap();
        let error = LocalControlHttpClient::new_unix(
            &linked_path,
            current_euid(),
            current_egid(),
            expected.clone(),
            credential(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("real socket"));

        let hardlink_dir = TestSocketDir::new();
        let hardlink_path = hardlink_dir.socket("hardlink.sock");
        let _hardlink_listener = std::os::unix::net::UnixListener::bind(&hardlink_path).unwrap();
        std::fs::set_permissions(
            &hardlink_path,
            std::fs::Permissions::from_mode(TRUSTED_UNIX_SOCKET_MODE),
        )
        .unwrap();
        std::fs::hard_link(&hardlink_path, hardlink_dir.socket("second-link.sock")).unwrap();
        let error = LocalControlHttpClient::new_unix(
            &hardlink_path,
            current_euid(),
            current_egid(),
            expected.clone(),
            credential(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("link count"));
    }

    #[derive(Clone)]
    struct HttpFixtureState {
        expected_authorization: String,
        publications: Arc<StdMutex<Vec<LocalRuntimeStatePublication>>>,
    }

    async fn bounded_json_fixture(State(payload_bytes): State<usize>) -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "messages": [{"content": "x".repeat(payload_bytes)}]
        }))
    }

    #[derive(Clone, Default)]
    struct CompactWriteFixtureState {
        request_body: Arc<StdMutex<Option<Vec<u8>>>>,
    }

    async fn compact_write_fixture(
        State(state): State<CompactWriteFixtureState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> std::result::Result<(StatusCode, Json<serde_json::Value>), StatusCode> {
        if headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            != Some("Bearer control-secret")
        {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let request: serde_json::Value =
            serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
        let client_nonce = request
            .get("client_nonce")
            .and_then(serde_json::Value::as_str)
            .ok_or(StatusCode::BAD_REQUEST)?;
        *state.request_body.lock().unwrap() = Some(body.to_vec());
        Ok((
            StatusCode::CREATED,
            Json(serde_json::json!({
                "client_nonce": client_nonce,
                "message_id": "0198f0f4-9b72-7000-8000-000000000099",
                "seq": 7,
                "created": true
            })),
        ))
    }

    #[tokio::test]
    async fn messaging_write_observes_a_compact_receipt_for_max_escaped_content() {
        let state = CompactWriteFixtureState::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route(
                "/local-control/v1/messaging:write",
                post(compact_write_fixture),
            )
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let expected = authority();
        let credential = LocalControlCredential::new(
            "control-secret",
            expected.rpc_identity().clone(),
            SystemTime::now() + Duration::from_secs(30),
        )
        .unwrap();
        let client = LocalControlHttpClient::new_loopback(
            format!("http://{address}/"),
            expected,
            credential,
        )
        .unwrap();
        let content = "\u{1}".repeat(64 * 1024);

        let receipt = client
            .write(WriteMessagingMessageRequest {
                place_id: "01900000-0000-7000-8000-000000000002",
                content: &content,
                urgency: "normal",
                reply_to: None,
                client_nonce: "nonce-max-escaped",
            })
            .await
            .expect("a legal maximum write must be observed as success");

        assert_eq!(receipt["client_nonce"], "nonce-max-escaped");
        assert_eq!(receipt["seq"], 7);
        assert_eq!(receipt["created"], true);
        assert!(receipt.get("message").is_none());
        let raw = state.request_body.lock().unwrap().take().unwrap();
        assert!(raw.len() > 2 * 64 * 1024);
        let request: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(
            request["content"].as_str().unwrap().as_bytes().len(),
            64 * 1024
        );
        server.abort();
    }

    async fn response_limit_fixture(
        payload_bytes: usize,
    ) -> (LocalControlHttpClient, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route(
                "/local-control/v1/messaging:open",
                post(bounded_json_fixture),
            )
            .route("/default", post(bounded_json_fixture))
            .with_state(payload_bytes);
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let expected = authority();
        let credential = LocalControlCredential::new(
            "control-secret",
            expected.rpc_identity().clone(),
            SystemTime::now() + Duration::from_secs(30),
        )
        .unwrap();
        let client = LocalControlHttpClient::new_loopback(
            format!("http://{address}/"),
            expected,
            credential,
        )
        .unwrap();
        (client, server)
    }

    #[tokio::test]
    async fn messaging_open_has_a_dedicated_response_bound_without_widening_control_responses() {
        let payload_bytes = MAX_LOCAL_CONTROL_RESPONSE_BYTES + 1024;
        let (client, server) = response_limit_fixture(payload_bytes).await;
        let response = client
            .open(OpenMessagingPlaceRequest {
                place_id: "01900000-0000-7000-8000-000000000002",
                before_seq: None,
                limit: Some(20),
            })
            .await
            .expect("messaging screen larger than 64 KiB remains readable");
        assert_eq!(
            response["messages"][0]["content"].as_str().unwrap().len(),
            payload_bytes
        );

        let error = client
            .post_json::<_, serde_json::Value>("/default", &serde_json::json!({}))
            .await
            .expect_err("non-messaging local control responses remain capped at 64 KiB");
        assert!(error.to_string().contains("exceeds bounded size"));
        server.abort();
    }

    #[tokio::test]
    async fn messaging_open_rejects_a_response_above_its_dedicated_bound() {
        let (client, server) = response_limit_fixture(MAX_MESSAGING_RESPONSE_BYTES + 1).await;
        let error = client
            .open(OpenMessagingPlaceRequest {
                place_id: "01900000-0000-7000-8000-000000000002",
                before_seq: None,
                limit: Some(50),
            })
            .await
            .expect_err("messaging responses must remain bounded");
        assert!(error.to_string().contains("exceeds bounded size"));
        server.abort();
    }

    async fn issue_http_fixture(
        State(state): State<HttpFixtureState>,
        headers: HeaderMap,
        Json(request): Json<LocalCredentialIssueRequest>,
    ) -> std::result::Result<Json<LocalCredentialIssueResponse>, StatusCode> {
        if headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            != Some(state.expected_authorization.as_str())
        {
            return Err(StatusCode::UNAUTHORIZED);
        }
        Ok(Json(LocalCredentialIssueResponse {
            request_id: request.request_id,
            personality_agent_id: request.personality_agent_id,
            generation: request.generation,
            rpc_boot_nonce: request.rpc_boot_nonce,
            audience: request.audience,
            expires_at_unix: unix_now() + 30,
            delivery_authorization: DeliveryAuthorization::Raw,
            token: "fixture-issued-token".to_owned(),
        }))
    }

    async fn publish_http_fixture(
        State(state): State<HttpFixtureState>,
        headers: HeaderMap,
        Json(publication): Json<LocalRuntimeStatePublication>,
    ) -> std::result::Result<Json<LocalRuntimeStateAck>, StatusCode> {
        if headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            != Some(state.expected_authorization.as_str())
        {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let revision = publication
            .expected_revision
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(StatusCode::CONFLICT)?;
        let ack = LocalRuntimeStateAck {
            publication_id: publication.publication_id.clone(),
            personality_agent_id: publication.personality_agent_id.clone(),
            generation: publication.generation,
            rpc_boot_nonce: publication.rpc_boot_nonce.clone(),
            revision,
            state: publication.state,
            hydration_receipt_identity: publication.hydration_receipt_identity.clone(),
        };
        state.publications.lock().unwrap().push(publication);
        Ok(Json(ack))
    }

    #[tokio::test]
    async fn loopback_publication_auth_rejection_is_terminal() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let fixture_state = HttpFixtureState {
            publications: Arc::new(StdMutex::new(Vec::new())),
            expected_authorization: "Bearer a-different-control-secret".to_owned(),
        };
        let app = Router::new()
            .route(
                &format!("/{PUBLISH_RUNTIME_STATE_PATH}"),
                post(publish_http_fixture),
            )
            .with_state(fixture_state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let expected = authority();
        let credential = LocalControlCredential::new(
            "control-secret",
            expected.rpc_identity().clone(),
            SystemTime::now() + Duration::from_secs(30),
        )
        .unwrap();
        let client = LocalControlHttpClient::new_loopback(
            format!("http://{address}/"),
            expected.clone(),
            credential,
        )
        .unwrap();
        let publication = LocalRuntimeStatePublication {
            publication_id: Uuid::now_v7().hyphenated().to_string(),
            personality_agent_id: expected.personality_agent_id().as_str().to_owned(),
            generation: expected.generation().as_u64(),
            rpc_boot_nonce: expected.nonce().as_str().to_owned(),
            expected_revision: None,
            state: LocalRuntimePublicationState::NotReady,
            hydration_receipt_identity: None,
            reason: LocalRuntimePublicationReason::Startup,
        };

        let error = client
            .publish_runtime_state(publication)
            .await
            .expect_err("HTTP auth rejection must be terminal");
        assert!(!error.is_indeterminate());
        assert!(error.to_string().contains("401"));
        assert!(fixture_state.publications.lock().unwrap().is_empty());
        server.abort();
    }

    #[test]
    fn publication_http_status_distinguishes_rejection_from_ack_indeterminacy() {
        let rejection = publication_http_status_error(reqwest::StatusCode::UNAUTHORIZED)
            .expect("401 is a publication rejection");
        assert!(!rejection.is_indeterminate());

        let server_failure =
            publication_http_status_error(reqwest::StatusCode::INTERNAL_SERVER_ERROR)
                .expect("500 leaves publication acknowledgement indeterminate");
        assert!(server_failure.is_indeterminate());

        assert!(publication_http_status_error(reqwest::StatusCode::OK).is_none());
    }

    #[tokio::test]
    async fn loopback_publication_transport_failure_is_indeterminate() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let expected = authority();
        let credential = LocalControlCredential::new(
            "control-secret",
            expected.rpc_identity().clone(),
            SystemTime::now() + Duration::from_secs(30),
        )
        .unwrap();
        let client = LocalControlHttpClient::new_loopback(
            format!("http://{address}/"),
            expected.clone(),
            credential,
        )
        .unwrap();
        let publication = LocalRuntimeStatePublication {
            publication_id: Uuid::now_v7().hyphenated().to_string(),
            personality_agent_id: expected.personality_agent_id().as_str().to_owned(),
            generation: expected.generation().as_u64(),
            rpc_boot_nonce: expected.nonce().as_str().to_owned(),
            expected_revision: None,
            state: LocalRuntimePublicationState::NotReady,
            hydration_receipt_identity: None,
            reason: LocalRuntimePublicationReason::Startup,
        };

        let error = client
            .publish_runtime_state(publication)
            .await
            .expect_err("transport loss cannot determine commit state");
        assert!(error.is_indeterminate());
        assert!(error.to_string().contains("request failed"));
    }

    #[tokio::test]
    async fn concrete_loopback_client_issues_credentials_and_publishes_ready_without_a_signing_key()
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route(
                &format!("/{ISSUE_CREDENTIAL_PATH}"),
                post(issue_http_fixture),
            )
            .route(
                &format!("/{PUBLISH_RUNTIME_STATE_PATH}"),
                post(publish_http_fixture),
            );
        let fixture_state = HttpFixtureState {
            publications: Arc::new(StdMutex::new(Vec::new())),
            expected_authorization: "Bearer control-secret".to_owned(),
        };
        let app = app.with_state(fixture_state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let expected = authority();
        let credential = LocalControlCredential::new(
            "control-secret",
            expected.rpc_identity().clone(),
            SystemTime::now() + Duration::from_secs(30),
        )
        .unwrap();
        let control = Arc::new(
            LocalControlHttpClient::new_loopback(
                format!("http://{address}"),
                expected.clone(),
                credential,
            )
            .unwrap(),
        );
        let mut provider = LocalCredentialProvider::new(
            expected.clone(),
            DeliveryAuthorization::Raw,
            control.clone(),
        );
        assert_eq!(
            provider.fresh_credential().await.unwrap().token(),
            "fixture-issued-token"
        );

        let publisher = LocalControlReadyPublisher::new(expected.clone(), control);
        let proof = ready_proof(&expected).await;
        publisher.publish_not_ready().await.unwrap();
        publisher.publish_ready(&proof).await.unwrap();
        publisher.publish_shutdown_not_ready().await.unwrap();
        assert_eq!(
            fixture_state
                .publications
                .lock()
                .unwrap()
                .iter()
                .map(|publication| publication.state)
                .collect::<Vec<_>>(),
            vec![
                LocalRuntimePublicationState::NotReady,
                LocalRuntimePublicationState::Ready,
                LocalRuntimePublicationState::NotReady,
            ]
        );
        server.abort();
    }
}
