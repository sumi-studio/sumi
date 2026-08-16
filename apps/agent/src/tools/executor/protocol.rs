use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::super::{ResourceLimit, ToolError};
use super::{ArtifactResponse, SignedCallAuthority, VerifiedCallAuthority};
use crate::runtime::contracts::{PersonalityAgentId, RpcIdentity};
use crate::tools::{bash::BashExecutionResult, fs::GrepMatch, truncate::TruncationResult};

pub const MAX_RPC_LINE_BYTES: usize = 1024 * 1024;
pub const MAX_RPC_READ_BYTES: usize = 50 * 1024;
/// Kept well below the JSON-line envelope after `Vec<u8>` serialization.
pub const MAX_ATTACHMENT_CHUNK_BYTES: usize = 64 * 1024;
const MAX_RPC_ID_BYTES: usize = 128;
const MAX_RPC_ERROR_CODE_BYTES: usize = 128;
const RPC_ACTIVE_REQUEST_CAPACITY: usize = 4_096;
const RPC_ORDINARY_ACTIVE_REQUEST_CAPACITY: usize = RPC_ACTIVE_REQUEST_CAPACITY - 1;
// Exact boot-scoped replay fencing cannot silently evict arbitrary caller
// identities. This fixed digest budget supports roughly 500k completed
// execution request/execution pairs per generation; exhaustion is explicit so
// the supervisor can roll the generation before any later side effect.
const RPC_BOOT_UNIQUENESS_CAPACITY: usize = 1_000_000;
// Once ordinary admission exhausts the main budget, every request that was
// already active must still be able to consume its one cancellation identity.
const RPC_CANCEL_UNIQUENESS_RESERVE: usize = RPC_ACTIVE_REQUEST_CAPACITY;
pub(super) const RPC_BOOT_UNIQUENESS_EXHAUSTED_CODE: &str = "rpc_boot_uniqueness_exhausted";
type RpcIdDigest = [u8; 32];

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorServiceRole {
    ToolExecutor,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RpcRequest<T> {
    pub personality_agent_id: PersonalityAgentId,
    pub generation: u64,
    pub nonce: String,
    pub request_id: String,
    pub operation: T,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutorRpcRequest {
    pub personality_agent_id: PersonalityAgentId,
    pub generation: u64,
    pub nonce: String,
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub call_authority: Option<SignedCallAuthority>,
    #[serde(skip)]
    pub verified_call_authority: Option<VerifiedCallAuthority>,
    pub operation: ExecutorOperation,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RpcFrame<T> {
    Update {
        personality_agent_id: PersonalityAgentId,
        generation: u64,
        nonce: String,
        request_id: String,
        value: Value,
    },
    Terminal {
        personality_agent_id: PersonalityAgentId,
        generation: u64,
        nonce: String,
        request_id: String,
        result: Result<T, RpcError>,
    },
}

impl<T> RpcFrame<T> {
    fn identity_fields(&self) -> (&PersonalityAgentId, u64, &str) {
        match self {
            Self::Update {
                personality_agent_id,
                generation,
                nonce,
                ..
            }
            | Self::Terminal {
                personality_agent_id,
                generation,
                nonce,
                ..
            } => (personality_agent_id, *generation, nonce),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RpcError {
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub resource_limit: Option<ResourceLimit>,
}

impl RpcError {
    fn validate(&self) -> Result<(), ToolError> {
        validate_bounded_text(&self.code, "error code", MAX_RPC_ERROR_CODE_BYTES)?;
        if self.code == "resource_limit" && self.resource_limit.is_none() {
            return Err(ToolError::Protocol(
                "resource_limit RPC error is missing typed ResourceLimit detail".to_owned(),
            ));
        }
        if self.code != "resource_limit" && self.resource_limit.is_some() {
            return Err(ToolError::Protocol(format!(
                "RPC error code {} cannot carry a resource_limit detail",
                self.code
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutorOperation {
    Health {
        service_role: ExecutorServiceRole,
    },
    ReadFile {
        path: String,
        offset: u64,
        limit: usize,
        execution_id: String,
    },
    WriteFile {
        path: String,
        content: String,
        execution_id: String,
    },
    EditFile {
        path: String,
        old_string: String,
        new_string: String,
        execution_id: String,
    },
    RemoveFile {
        path: String,
        execution_id: String,
    },
    ListDir {
        path: String,
        execution_id: String,
    },
    Glob {
        pattern: String,
        execution_id: String,
    },
    Grep {
        path: String,
        pattern: String,
        execution_id: String,
    },
    Bash {
        command: String,
        execution_id: String,
    },
    /// Open an exact ordered list of Workspace regular files for a Messaging
    /// attachment send. The terminal frame carries one manifest per path and
    /// the read-only descriptors ride on that same frame as SCM_RIGHTS
    /// ancillary data, in the same order.
    OpenSourceFiles {
        paths: Vec<String>,
        execution_id: String,
    },
    Cancel {
        execution_id: String,
    },
}

/// Bounds for one OpenSourceFiles operation. They mirror the Messaging
/// attachment limits so a source that can never be sent is refused at the
/// executor instead of after transfer.
pub const MAX_SOURCE_FILES_PER_OPERATION: usize = 10;
pub const MAX_SOURCE_FILE_BYTES: u64 = 20 * 1024 * 1024;
pub const MAX_SOURCE_FILES_TOTAL_BYTES: u64 =
    MAX_SOURCE_FILES_PER_OPERATION as u64 * MAX_SOURCE_FILE_BYTES;
const MAX_SOURCE_PATH_BYTES: usize = 4 * 1024;

/// One opened Workspace source. `path` echoes the requested path; `filename`
/// is its final component; `sha256` is the hex digest of the exact bytes the
/// executor read while holding the descriptor open.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceFileManifest {
    pub path: String,
    pub filename: String,
    pub size_bytes: u64,
    pub sha256: String,
}

impl SourceFileManifest {
    pub fn validate(&self) -> Result<(), ToolError> {
        validate_bounded_text(&self.path, "source path", MAX_SOURCE_PATH_BYTES)?;
        validate_bounded_text(&self.filename, "source filename", 255)?;
        if self.filename.contains('/') || self.filename == "." || self.filename == ".." {
            return Err(ToolError::Protocol(
                "source filename must be a single path component".to_owned(),
            ));
        }
        if self.size_bytes == 0 || self.size_bytes > MAX_SOURCE_FILE_BYTES {
            return Err(ToolError::Protocol(
                "source file size is outside the attachment bounds".to_owned(),
            ));
        }
        if self.sha256.len() != 64 || !self.sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(ToolError::Protocol(
                "source digest must be lowercase hex sha256".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Validate the ordered path list of one OpenSourceFiles operation.
pub fn validate_source_paths(paths: &[String]) -> Result<(), ToolError> {
    if paths.is_empty() || paths.len() > MAX_SOURCE_FILES_PER_OPERATION {
        return Err(ToolError::InvalidArguments);
    }
    let mut seen = HashSet::with_capacity(paths.len());
    for path in paths {
        validate_bounded_text(path, "source path", MAX_SOURCE_PATH_BYTES)?;
        validate_workspace_input(path, "path")?;
        if path.starts_with('/') || path.contains('\0') {
            return Err(ToolError::InvalidPath(
                "source path must be workspace-relative".to_owned(),
            ));
        }
        if !seen.insert(path.as_str()) {
            return Err(ToolError::InvalidArguments);
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutorResponse {
    Healthy { service_role: ExecutorServiceRole },
    ReadFile { result: TruncationResult },
    SourceFiles { files: Vec<SourceFileManifest> },
    Written {},
    Edited {},
    Removed {},
    Listed { entries: Vec<String> },
    Globbed { paths: Vec<String> },
    Grepped { matches: Vec<GrepMatch> },
    Artifact { response: ArtifactResponse },
    Bash { result: BashExecutionResult },
    CancelAccepted {},
    CancelTooLate {},
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactOperation {
    BeginToolOutput {
        execution_id: String,
        content: Vec<u8>,
    },
    AppendToolOutput {
        handle: String,
        offset: u64,
        content: Vec<u8>,
    },
    FinishToolOutput {
        handle: String,
    },
    ReadArtifact {
        handle: String,
        offset: u64,
        limit: usize,
    },
    GrepArtifact {
        handle: String,
        pattern: String,
    },
    PutAttachment {
        artifact_id: String,
        content: String,
    },
    BeginAttachment {
        artifact_id: String,
        total_bytes: u64,
        content_digest: String,
    },
    AppendAttachment {
        artifact_id: String,
        total_bytes: u64,
        content_digest: String,
        offset: u64,
        content: Vec<u8>,
    },
    FinishAttachment {
        artifact_id: String,
        total_bytes: u64,
        content_digest: String,
    },
}

pub trait RpcOperationValidation {
    fn validate(&self) -> Result<(), ToolError>;
}

impl RpcOperationValidation for ExecutorOperation {
    fn validate(&self) -> Result<(), ToolError> {
        match self {
            Self::Health { .. } => Ok(()),
            Self::ReadFile {
                path,
                limit,
                execution_id,
                ..
            } => {
                validate_rpc_read_limit(*limit)?;
                validate_routable_input(path)?;
                validate_executor_execution_id(execution_id)
            }
            Self::WriteFile {
                path, execution_id, ..
            }
            | Self::EditFile {
                path, execution_id, ..
            }
            | Self::RemoveFile { path, execution_id }
            | Self::ListDir { path, execution_id } => {
                validate_workspace_input(path, "path")?;
                validate_executor_execution_id(execution_id)
            }
            Self::Grep {
                path, execution_id, ..
            } => {
                validate_routable_input(path)?;
                validate_executor_execution_id(execution_id)
            }
            Self::Glob {
                pattern,
                execution_id,
            } => {
                validate_workspace_input(pattern, "pattern")?;
                validate_executor_execution_id(execution_id)
            }
            Self::OpenSourceFiles {
                paths,
                execution_id,
            } => {
                validate_source_paths(paths)?;
                validate_executor_execution_id(execution_id)
            }
            Self::Bash { execution_id, .. } | Self::Cancel { execution_id } => {
                validate_executor_execution_id(execution_id)
            }
        }
    }
}

impl RpcOperationValidation for ArtifactOperation {
    fn validate(&self) -> Result<(), ToolError> {
        match self {
            Self::BeginToolOutput { execution_id, .. } => {
                validate_artifact_handle_component(execution_id)?;
                validate_rpc_id(execution_id, "execution_id")
            }
            Self::AppendToolOutput { handle, .. } | Self::FinishToolOutput { handle } => {
                let parsed = parse_artifact_handle(handle)?;
                if parsed.kind != ArtifactKind::ToolOutput {
                    return Err(ToolError::InvalidPath(
                        "tool-output mutation requires a tool-output artifact handle".to_owned(),
                    ));
                }
                Ok(())
            }
            Self::ReadArtifact { handle, limit, .. } => {
                parse_artifact_handle(handle)?;
                validate_rpc_read_limit(*limit)
            }
            Self::GrepArtifact { handle, pattern } => {
                parse_artifact_handle(handle)?;
                if pattern.is_empty() {
                    return Err(ToolError::Protocol(
                        "RPC grep pattern must be non-empty".to_owned(),
                    ));
                }
                Ok(())
            }
            Self::PutAttachment { artifact_id, .. } => {
                validate_artifact_handle_component(artifact_id)?;
                Ok(())
            }
            Self::BeginAttachment {
                artifact_id,
                content_digest,
                ..
            }
            | Self::FinishAttachment {
                artifact_id,
                content_digest,
                ..
            } => {
                validate_artifact_handle_component(artifact_id)?;
                validate_attachment_digest(content_digest)
            }
            Self::AppendAttachment {
                artifact_id,
                content_digest,
                content,
                ..
            } => {
                validate_artifact_handle_component(artifact_id)?;
                validate_attachment_digest(content_digest)?;
                if content.is_empty() || content.len() > MAX_ATTACHMENT_CHUNK_BYTES {
                    return Err(ToolError::Protocol(format!(
                        "attachment chunk must contain 1..={MAX_ATTACHMENT_CHUNK_BYTES} bytes"
                    )));
                }
                Ok(())
            }
        }
    }
}

impl ArtifactOperation {
    pub(super) fn validate_authenticated_owner(
        &self,
        personality_agent_id: &PersonalityAgentId,
    ) -> Result<(), ToolError> {
        let handle = match self {
            Self::AppendToolOutput { handle, .. }
            | Self::FinishToolOutput { handle }
            | Self::ReadArtifact { handle, .. }
            | Self::GrepArtifact { handle, .. } => handle,
            Self::BeginToolOutput { .. }
            | Self::PutAttachment { .. }
            | Self::BeginAttachment { .. }
            | Self::AppendAttachment { .. }
            | Self::FinishAttachment { .. } => return Ok(()),
        };
        parse_artifact_handle_for_personality_agent(handle, personality_agent_id)?;
        Ok(())
    }
}

pub fn decode_rpc_line<T: DeserializeOwned + RpcOperationValidation>(
    line: &[u8],
    identity: &RpcIdentity,
) -> Result<RpcRequest<T>, ToolError> {
    if framed_rpc_len(line.len()).is_none() {
        return Err(ToolError::Protocol("RPC line exceeds 1MiB".to_owned()));
    }
    if line.iter().any(|byte| *byte == b'\n' || *byte == b'\r') {
        return Err(ToolError::Protocol(
            "RPC decoder expects exactly one unframed JSON line".to_owned(),
        ));
    }
    let request = serde_json::from_slice::<RpcRequest<T>>(line)
        .map_err(|error| ToolError::Protocol(format!("invalid RPC JSON: {error}")))?;
    identity.validate_wire(
        request.personality_agent_id.as_str(),
        request.generation,
        &request.nonce,
    )?;
    validate_rpc_id(&request.request_id, "request_id")?;
    request.operation.validate()?;
    Ok(request)
}

pub fn decode_executor_rpc_line(
    line: &[u8],
    identity: &RpcIdentity,
) -> Result<ExecutorRpcRequest, ToolError> {
    if framed_rpc_len(line.len()).is_none() {
        return Err(ToolError::Protocol("RPC line exceeds 1MiB".to_owned()));
    }
    if line.iter().any(|byte| *byte == b'\n' || *byte == b'\r') {
        return Err(ToolError::Protocol(
            "RPC decoder expects exactly one unframed JSON line".to_owned(),
        ));
    }
    let request = serde_json::from_slice::<ExecutorRpcRequest>(line)
        .map_err(|error| ToolError::Protocol(format!("invalid RPC JSON: {error}")))?;
    identity.validate_wire(
        request.personality_agent_id.as_str(),
        request.generation,
        &request.nonce,
    )?;
    validate_rpc_id(&request.request_id, "request_id")?;
    request.operation.validate()?;
    Ok(request)
}

pub fn encode_rpc_frame<T: Serialize>(frame: &RpcFrame<T>) -> Result<Vec<u8>, ToolError> {
    let (personality_agent_id, generation, nonce) = frame.identity_fields();
    RpcIdentity::from_wire(personality_agent_id.as_str(), generation, nonce)?;
    match frame {
        RpcFrame::Update { request_id, .. } => validate_rpc_id(request_id, "request_id")?,
        RpcFrame::Terminal {
            request_id, result, ..
        } => {
            validate_rpc_id(request_id, "request_id")?;
            if let Err(error) = result {
                error.validate()?;
            }
        }
    }

    let mut encoded = serde_json::to_vec(frame)
        .map_err(|error| ToolError::Protocol(format!("RPC response encode failed: {error}")))?;
    if framed_rpc_len(encoded.len()).is_none() {
        return Err(ToolError::Protocol("RPC response exceeds 1MiB".to_owned()));
    }
    encoded.push(b'\n');
    Ok(encoded)
}

pub fn decode_rpc_frame<T: DeserializeOwned>(
    line: &[u8],
    identity: &RpcIdentity,
) -> Result<RpcFrame<T>, ToolError> {
    if framed_rpc_len(line.len()).is_none() {
        return Err(ToolError::Protocol("RPC line exceeds 1MiB".to_owned()));
    }
    if line.iter().any(|byte| *byte == b'\n' || *byte == b'\r') {
        return Err(ToolError::Protocol(
            "RPC decoder expects exactly one unframed JSON line".to_owned(),
        ));
    }
    let frame = serde_json::from_slice::<RpcFrame<T>>(line)
        .map_err(|error| ToolError::Protocol(format!("invalid RPC JSON: {error}")))?;
    let (personality_agent_id, generation, nonce) = frame.identity_fields();
    identity.validate_wire(personality_agent_id.as_str(), generation, nonce)?;
    match &frame {
        RpcFrame::Update { request_id, .. } => validate_rpc_id(request_id, "request_id")?,
        RpcFrame::Terminal {
            request_id, result, ..
        } => {
            validate_rpc_id(request_id, "request_id")?;
            if let Err(error) = result {
                error.validate()?;
            }
        }
    }
    Ok(frame)
}

pub struct RpcLifecycleTracker {
    active_requests: HashSet<String>,
    active_cancel_requests: HashSet<String>,
    completed_requests: HashSet<RpcIdDigest>,
    executions: HashMap<String, String>,
    completed_executions: HashSet<RpcIdDigest>,
    cancelled_executions: HashSet<String>,
    boot_uniqueness_capacity: usize,
    cancel_uniqueness_reserve: usize,
}

impl Default for RpcLifecycleTracker {
    fn default() -> Self {
        Self::with_boot_uniqueness_budget(
            RPC_BOOT_UNIQUENESS_CAPACITY,
            RPC_CANCEL_UNIQUENESS_RESERVE,
        )
    }
}

impl RpcLifecycleTracker {
    fn with_boot_uniqueness_budget(capacity: usize, cancel_reserve: usize) -> Self {
        assert!(capacity > 0);
        Self {
            active_requests: HashSet::new(),
            active_cancel_requests: HashSet::new(),
            completed_requests: HashSet::new(),
            executions: HashMap::new(),
            completed_executions: HashSet::new(),
            cancelled_executions: HashSet::new(),
            boot_uniqueness_capacity: capacity,
            cancel_uniqueness_reserve: cancel_reserve,
        }
    }

    #[cfg(test)]
    pub(super) fn with_test_boot_uniqueness_budget(capacity: usize, cancel_reserve: usize) -> Self {
        Self::with_boot_uniqueness_budget(capacity, cancel_reserve)
    }

    pub fn begin_request(&mut self, request_id: &str) -> Result<(), ToolError> {
        validate_rpc_id(request_id, "request_id")?;
        let completed_id = rpc_id_digest(request_id);
        if self.active_requests.contains(request_id)
            || self.completed_requests.contains(&completed_id)
        {
            return Err(ToolError::Protocol(
                "RPC request_id must be unique".to_owned(),
            ));
        }
        self.ensure_ordinary_active_capacity()?;
        self.ensure_boot_uniqueness_capacity(1, false)?;
        self.active_requests.insert(request_id.to_owned());
        Ok(())
    }

    pub fn begin_execution(
        &mut self,
        request_id: &str,
        execution_id: &str,
    ) -> Result<(), ToolError> {
        validate_executor_execution_id(execution_id)?;
        if self.executions.contains_key(execution_id)
            || self
                .completed_executions
                .contains(&rpc_id_digest(execution_id))
        {
            return Err(ToolError::Protocol(
                "RPC execution_id must be unique".to_owned(),
            ));
        }
        validate_rpc_id(request_id, "request_id")?;
        let completed_request_id = rpc_id_digest(request_id);
        if self.active_requests.contains(request_id)
            || self.completed_requests.contains(&completed_request_id)
        {
            return Err(ToolError::Protocol(
                "RPC request_id must be unique".to_owned(),
            ));
        }
        self.ensure_ordinary_active_capacity()?;
        self.ensure_boot_uniqueness_capacity(2, false)?;
        self.active_requests.insert(request_id.to_owned());
        self.executions
            .insert(execution_id.to_owned(), request_id.to_owned());
        Ok(())
    }

    pub fn accept_cancel(&mut self, request_id: &str, execution_id: &str) -> Result<(), ToolError> {
        validate_rpc_id(request_id, "request_id")?;
        validate_executor_execution_id(execution_id)?;
        let completed_id = rpc_id_digest(request_id);
        if self.active_requests.contains(request_id)
            || self.completed_requests.contains(&completed_id)
        {
            return Err(ToolError::Protocol(
                "RPC request_id must be unique".to_owned(),
            ));
        }
        let execution_request = self.executions.get(execution_id).ok_or_else(|| {
            if self
                .completed_executions
                .contains(&rpc_id_digest(execution_id))
            {
                ToolError::Protocol("RPC cancel targeted a terminal execution".to_owned())
            } else {
                ToolError::Protocol("RPC cancel targeted an unknown execution".to_owned())
            }
        })?;
        if !self.active_requests.contains(execution_request) {
            return Err(ToolError::Protocol(
                "RPC cancel targeted a terminal execution".to_owned(),
            ));
        }
        if self.cancelled_executions.contains(execution_id) {
            return Err(ToolError::Protocol(
                "RPC execution received more than one cancel request".to_owned(),
            ));
        }
        self.ensure_cancel_active_capacity()?;
        self.ensure_boot_uniqueness_capacity(1, true)?;
        self.cancelled_executions.insert(execution_id.to_owned());
        self.active_requests.insert(request_id.to_owned());
        self.active_cancel_requests.insert(request_id.to_owned());
        Ok(())
    }

    pub fn execution_is_completed(&self, execution_id: &str) -> bool {
        self.completed_executions
            .contains(&rpc_id_digest(execution_id))
    }

    pub fn accept_update(&self, request_id: &str) -> Result<(), ToolError> {
        if self.active_requests.contains(request_id) {
            if self.active_cancel_requests.contains(request_id) {
                Err(ToolError::Protocol(
                    "RPC cancel request cannot emit updates".to_owned(),
                ))
            } else {
                Ok(())
            }
        } else if self.completed_requests.contains(&rpc_id_digest(request_id)) {
            Err(ToolError::Protocol(
                "RPC update arrived after terminal response".to_owned(),
            ))
        } else {
            Err(ToolError::Protocol(
                "RPC update referenced an unknown request".to_owned(),
            ))
        }
    }

    pub fn accept_terminal(&mut self, request_id: &str) -> Result<(), ToolError> {
        if !self.active_requests.remove(request_id) {
            return Err(
                if self.completed_requests.contains(&rpc_id_digest(request_id)) {
                    ToolError::Protocol(
                        "RPC request emitted more than one terminal response".to_owned(),
                    )
                } else {
                    ToolError::Protocol("RPC terminal referenced an unknown request".to_owned())
                },
            );
        }
        self.active_cancel_requests.remove(request_id);
        let completed_executions = self
            .executions
            .iter()
            .filter_map(|(execution_id, owner)| {
                (owner == request_id).then_some(execution_id.clone())
            })
            .collect::<Vec<_>>();
        for execution_id in completed_executions {
            self.executions.remove(&execution_id);
            self.cancelled_executions.remove(&execution_id);
            self.completed_executions
                .insert(rpc_id_digest(&execution_id));
        }
        self.completed_requests.insert(rpc_id_digest(request_id));
        Ok(())
    }

    fn ensure_ordinary_active_capacity(&self) -> Result<(), ToolError> {
        if self.active_requests.len() >= RPC_ORDINARY_ACTIVE_REQUEST_CAPACITY {
            return Err(ToolError::Protocol(format!(
                "RPC ordinary active request limit of {RPC_ORDINARY_ACTIVE_REQUEST_CAPACITY} was reached"
            )));
        }
        Ok(())
    }

    fn ensure_cancel_active_capacity(&self) -> Result<(), ToolError> {
        if self.active_requests.len() >= RPC_ACTIVE_REQUEST_CAPACITY {
            return Err(ToolError::Protocol(format!(
                "RPC active request limit of {RPC_ACTIVE_REQUEST_CAPACITY} was reached"
            )));
        }
        Ok(())
    }

    fn ensure_boot_uniqueness_capacity(
        &self,
        additional: usize,
        cancel: bool,
    ) -> Result<(), ToolError> {
        let limit = if cancel {
            self.boot_uniqueness_capacity
                .checked_add(self.cancel_uniqueness_reserve)
        } else {
            Some(self.boot_uniqueness_capacity)
        }
        .ok_or_else(boot_uniqueness_exhausted)?;
        if self
            .tracked_identity_count()
            .checked_add(additional)
            .is_none_or(|next| next > limit)
        {
            return Err(boot_uniqueness_exhausted());
        }
        Ok(())
    }

    pub(super) fn tracked_identity_count(&self) -> usize {
        self.active_requests.len()
            + self.completed_requests.len()
            + self.executions.len()
            + self.completed_executions.len()
    }
}

fn boot_uniqueness_exhausted() -> ToolError {
    ToolError::Protocol(RPC_BOOT_UNIQUENESS_EXHAUSTED_CODE.to_owned())
}

fn framed_rpc_len(unframed_len: usize) -> Option<usize> {
    unframed_len
        .checked_add(1)
        .filter(|framed_len| *framed_len <= MAX_RPC_LINE_BYTES)
}

fn validate_rpc_id(value: &str, name: &str) -> Result<(), ToolError> {
    if value.is_empty() || value.len() > MAX_RPC_ID_BYTES {
        return Err(ToolError::Protocol(format!(
            "RPC {name} must contain 1..={MAX_RPC_ID_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_bounded_text(value: &str, name: &str, max_bytes: usize) -> Result<(), ToolError> {
    if value.is_empty() || value.len() > max_bytes {
        return Err(ToolError::Protocol(format!(
            "RPC {name} must contain 1..={max_bytes} bytes"
        )));
    }
    Ok(())
}

fn validate_attachment_digest(value: &str) -> Result<(), ToolError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ToolError::Protocol(
            "attachment content_digest must be a SHA-256 hex digest".to_owned(),
        ));
    }
    Ok(())
}

fn validate_rpc_read_limit(limit: usize) -> Result<(), ToolError> {
    if limit > MAX_RPC_READ_BYTES {
        return Err(ToolError::Protocol(format!(
            "RPC read limit must be <= {MAX_RPC_READ_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_routable_input(value: &str) -> Result<(), ToolError> {
    if value.starts_with("artifact://") {
        parse_artifact_handle(value)?;
    }
    Ok(())
}

fn validate_workspace_input(value: &str, field: &str) -> Result<(), ToolError> {
    if value.starts_with("artifact://") {
        return Err(ToolError::InvalidPath(format!(
            "executor workspace {field} cannot be an artifact handle"
        )));
    }
    Ok(())
}

fn validate_executor_execution_id(execution_id: &str) -> Result<(), ToolError> {
    validate_artifact_handle_component(execution_id)?;
    validate_rpc_id(execution_id, "execution_id")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ArtifactKind {
    Attachments,
    ToolOutput,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ParsedArtifactHandle<'a> {
    pub personality_agent_id: PersonalityAgentId,
    pub kind: ArtifactKind,
    pub artifact_id: &'a str,
}

pub(super) fn parse_artifact_handle(handle: &str) -> Result<ParsedArtifactHandle<'_>, ToolError> {
    let suffix = handle
        .strip_prefix("artifact://")
        .ok_or_else(|| ToolError::InvalidPath("invalid artifact handle scheme".to_owned()))?;
    let mut components = suffix.split('/');
    let personality_agent_id = PersonalityAgentId::parse(components.next().unwrap_or_default())
        .map_err(|error| ToolError::InvalidPath(error.to_string()))?;
    let kind = match components.next().unwrap_or_default() {
        "attachments" => ArtifactKind::Attachments,
        "tool-output" => ArtifactKind::ToolOutput,
        _ => {
            return Err(ToolError::InvalidPath(
                "invalid artifact handle kind".to_owned(),
            ));
        }
    };
    let artifact_id = components.next().unwrap_or_default();
    if components.next().is_some() {
        return Err(ToolError::InvalidPath(
            "artifact handle has extra path components".to_owned(),
        ));
    }
    validate_artifact_handle_component(artifact_id)?;
    Ok(ParsedArtifactHandle {
        personality_agent_id,
        kind,
        artifact_id,
    })
}

fn parse_artifact_handle_for_personality_agent<'a>(
    handle: &'a str,
    expected_personality_agent_id: &PersonalityAgentId,
) -> Result<ParsedArtifactHandle<'a>, ToolError> {
    let parsed = parse_artifact_handle(handle)?;
    if &parsed.personality_agent_id != expected_personality_agent_id {
        return Err(ToolError::InvalidPath(
            "artifact belongs to another personality agent".to_owned(),
        ));
    }
    Ok(parsed)
}

fn validate_artifact_handle_component(value: &str) -> Result<(), ToolError> {
    if value.is_empty()
        || value.len() > 200
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        || value == "."
        || value == ".."
    {
        return Err(ToolError::InvalidPath(
            "invalid artifact handle component".to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn rpc_id_digest(value: &str) -> RpcIdDigest {
    Sha256::digest(value.as_bytes()).into()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputRoute {
    Workspace,
    Artifact,
}

pub fn resolve_input(tool_name: &str, input: &str) -> Result<InputRoute, ToolError> {
    if !input.starts_with("artifact://") {
        return Ok(InputRoute::Workspace);
    }
    match tool_name {
        "read_file" | "grep" => {
            parse_artifact_handle(input)?;
            Ok(InputRoute::Artifact)
        }
        _ => Err(ToolError::InvalidPath(format!(
            "{tool_name} does not accept artifact handles"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::runtime::contracts::{MAX_OPAQUE_ID_BYTES, MAX_PROCESS_GENERATION};

    const PAID: &str = "0198f0f4-9b72-7000-8000-000000000001";

    fn identity() -> RpcIdentity {
        RpcIdentity::from_wire(PAID, 7, "boot-nonce").unwrap()
    }

    #[test]
    fn health_requires_the_exact_tool_executor_role_without_legacy_shape() {
        let request = |operation| {
            json!({
                "personality_agent_id": PAID,
                "generation": 7,
                "nonce": "boot-nonce",
                "request_id": "health-request",
                "operation": operation,
            })
        };
        let exact = serde_json::to_vec(&request(json!({
            "type": "health",
            "service_role": "tool_executor",
        })))
        .unwrap();
        assert_eq!(
            decode_rpc_line::<ExecutorOperation>(&exact, &identity())
                .unwrap()
                .operation,
            ExecutorOperation::Health {
                service_role: ExecutorServiceRole::ToolExecutor,
            }
        );
        for rejected in [
            json!({"type": "health"}),
            json!({"type": "health", "service_role": "artifact_broker"}),
            json!({
                "type": "health",
                "service_role": "tool_executor",
                "legacy": true,
            }),
        ] {
            let line = serde_json::to_vec(&request(rejected)).unwrap();
            assert!(decode_rpc_line::<ExecutorOperation>(&line, &identity()).is_err());
        }
    }

    #[test]
    fn process_generation_domain_is_shared_by_identity_and_wire_paths() {
        for generation in [0, MAX_PROCESS_GENERATION] {
            let identity = RpcIdentity::from_wire(PAID, generation, "boot-nonce")
                .expect("valid identity generation");

            let request = serde_json::to_vec(&RpcRequest {
                personality_agent_id: identity.personality_agent_id().clone(),
                generation,
                nonce: identity.nonce().as_str().to_owned(),
                request_id: "request-1".to_owned(),
                operation: ExecutorOperation::Cancel {
                    execution_id: "execution-1".to_owned(),
                },
            })
            .expect("encode request fixture");
            decode_rpc_line::<ExecutorOperation>(&request, &identity)
                .expect("valid request generation");

            encode_rpc_frame(&RpcFrame::<Value>::Update {
                personality_agent_id: identity.personality_agent_id().clone(),
                generation,
                nonce: identity.nonce().as_str().to_owned(),
                request_id: "request-1".to_owned(),
                value: json!({}),
            })
            .expect("valid response generation");
        }

        let out_of_domain = MAX_PROCESS_GENERATION + 1;
        assert!(RpcIdentity::from_wire(PAID, out_of_domain, "boot-nonce").is_err());
        assert!(
            encode_rpc_frame(&RpcFrame::<Value>::Update {
                personality_agent_id: PAID.parse().unwrap(),
                generation: out_of_domain,
                nonce: "boot-nonce".to_owned(),
                request_id: "request-1".to_owned(),
                value: json!({}),
            })
            .is_err()
        );

        let valid_identity =
            RpcIdentity::from_wire(PAID, MAX_PROCESS_GENERATION, "boot-nonce").unwrap();
        let invalid_request = serde_json::to_vec(&RpcRequest {
            personality_agent_id: PAID.parse().unwrap(),
            generation: out_of_domain,
            nonce: "boot-nonce".to_owned(),
            request_id: "request-1".to_owned(),
            operation: ExecutorOperation::Cancel {
                execution_id: "execution-1".to_owned(),
            },
        })
        .expect("encode invalid request fixture");
        assert!(decode_rpc_line::<ExecutorOperation>(&invalid_request, &valid_identity).is_err());

        let invalid_frame = serde_json::to_vec(&RpcFrame::<Value>::Update {
            personality_agent_id: PAID.parse().unwrap(),
            generation: out_of_domain,
            nonce: "boot-nonce".to_owned(),
            request_id: "request-1".to_owned(),
            value: json!({}),
        })
        .expect("encode invalid response fixture");
        assert!(decode_rpc_frame::<Value>(&invalid_frame, &valid_identity).is_err());
    }

    fn request_line(request_id: &str, execution_id: &str) -> Vec<u8> {
        serde_json::to_vec(&RpcRequest {
            personality_agent_id: PAID.parse().unwrap(),
            generation: 7,
            nonce: "boot-nonce".to_owned(),
            request_id: request_id.to_owned(),
            operation: ExecutorOperation::Cancel {
                execution_id: execution_id.to_owned(),
            },
        })
        .expect("encode request")
    }

    fn update_frame(request_id: impl Into<String>) -> RpcFrame<Value> {
        RpcFrame::Update {
            personality_agent_id: PAID.parse().unwrap(),
            generation: 7,
            nonce: "boot-nonce".to_owned(),
            request_id: request_id.into(),
            value: json!({"chunk": "x"}),
        }
    }

    fn terminal_frame(result: Result<Value, RpcError>) -> RpcFrame<Value> {
        RpcFrame::Terminal {
            personality_agent_id: PAID.parse().unwrap(),
            generation: 7,
            nonce: "boot-nonce".to_owned(),
            request_id: "request-1".to_owned(),
            result,
        }
    }

    #[test]
    fn requests_are_bounded_generation_fenced_and_strict() {
        let encoded = request_line("request-1", "execution-1");
        assert!(decode_rpc_line::<ExecutorOperation>(&encoded, &identity()).is_ok());
        assert!(
            decode_rpc_line::<ExecutorOperation>(&[b'x'; MAX_RPC_LINE_BYTES + 1], &identity())
                .is_err()
        );
        assert!(decode_rpc_line::<ExecutorOperation>(b"{}\n", &identity()).is_err());

        let wrong = RpcIdentity::from_wire(PAID, 8, "boot-nonce").unwrap();
        assert!(decode_rpc_line::<ExecutorOperation>(&encoded, &wrong).is_err());
        let wrong_paid =
            RpcIdentity::from_wire("0198f0f4-9b72-7000-8000-000000000002", 7, "boot-nonce")
                .unwrap();
        assert!(
            decode_rpc_line::<ExecutorOperation>(&encoded, &wrong_paid).is_err(),
            "same generation and nonce must not cross a PAID boundary"
        );
        assert!(decode_rpc_line::<ExecutorOperation>(
            br#"{"generation":7,"nonce":"boot-nonce","request_id":"request-1","operation":{"type":"cancel","execution_id":"execution-1"},"extra":true}"#,
            &identity(),
        ).is_err());
    }

    #[test]
    fn jsonl_size_limit_includes_the_terminal_newline() {
        let mut request = RpcRequest {
            personality_agent_id: PAID.parse().unwrap(),
            generation: 7,
            nonce: "boot-nonce".to_owned(),
            request_id: "request-1".to_owned(),
            operation: ExecutorOperation::WriteFile {
                path: "notes.txt".to_owned(),
                content: String::new(),
                execution_id: "execution-1".to_owned(),
            },
        };
        let base_len = serde_json::to_vec(&request).unwrap().len();
        {
            let ExecutorOperation::WriteFile { content, .. } = &mut request.operation else {
                unreachable!()
            };
            *content = "x".repeat(MAX_RPC_LINE_BYTES - 1 - base_len);
        }
        let at_limit = serde_json::to_vec(&request).unwrap();
        assert_eq!(at_limit.len() + 1, MAX_RPC_LINE_BYTES);
        assert!(decode_rpc_line::<ExecutorOperation>(&at_limit, &identity()).is_ok());

        let ExecutorOperation::WriteFile { content, .. } = &mut request.operation else {
            unreachable!()
        };
        content.push('x');
        let over_limit = serde_json::to_vec(&request).unwrap();
        assert_eq!(over_limit.len(), MAX_RPC_LINE_BYTES);
        assert!(matches!(
            decode_rpc_line::<ExecutorOperation>(&over_limit, &identity()),
            Err(ToolError::Protocol(message)) if message == "RPC line exceeds 1MiB"
        ));

        let mut frame = update_frame("request-1");
        let RpcFrame::Update { value, .. } = &mut frame else {
            unreachable!()
        };
        *value = Value::String(String::new());
        let empty_len = serde_json::to_vec(&frame).unwrap().len();
        let RpcFrame::Update { value, .. } = &mut frame else {
            unreachable!()
        };
        *value = Value::String("x".repeat(MAX_RPC_LINE_BYTES - 1 - empty_len));
        let encoded = encode_rpc_frame(&frame).expect("exact framed limit");
        assert_eq!(encoded.len(), MAX_RPC_LINE_BYTES);
        assert_eq!(encoded.last(), Some(&b'\n'));

        let RpcFrame::Update { value, .. } = &mut frame else {
            unreachable!()
        };
        let Value::String(text) = value else {
            unreachable!()
        };
        text.push('x');
        assert!(matches!(
            encode_rpc_frame(&frame),
            Err(ToolError::Protocol(message)) if message == "RPC response exceeds 1MiB"
        ));
    }

    #[test]
    fn nonces_are_nonempty_bounded_and_checked_on_both_directions() {
        for nonce in ["".to_owned(), "n".repeat(MAX_OPAQUE_ID_BYTES + 1)] {
            let request = serde_json::to_vec(&RpcRequest {
                personality_agent_id: PAID.parse().unwrap(),
                generation: 7,
                nonce: nonce.clone(),
                request_id: "request-1".to_owned(),
                operation: ExecutorOperation::Cancel {
                    execution_id: "execution-1".to_owned(),
                },
            })
            .expect("encode malformed request fixture");
            assert!(decode_rpc_line::<ExecutorOperation>(&request, &identity()).is_err());

            assert!(RpcIdentity::from_wire(PAID, 7, nonce.clone()).is_err());

            let mut frame = update_frame("request-1");
            let RpcFrame::Update {
                nonce: frame_nonce, ..
            } = &mut frame
            else {
                unreachable!()
            };
            *frame_nonce = nonce;
            assert!(matches!(
                encode_rpc_frame(&frame),
                Err(ToolError::Protocol(message))
                    if message == "RPC nonce must contain 1..=128 bytes"
            ));
            let raw_frame = serde_json::to_vec(&frame).expect("encode malformed response fixture");
            assert!(matches!(
                decode_rpc_frame::<Value>(&raw_frame, &identity()),
                Err(ToolError::Protocol(message))
                    if message == "RPC nonce must contain 1..=128 bytes"
            ));
        }
    }

    #[test]
    fn request_and_execution_ids_are_bounded() {
        for (request_id, execution_id) in [
            ("", "execution-1"),
            ("request-1", ""),
            (&"r".repeat(MAX_RPC_ID_BYTES + 1), "execution-1"),
            ("request-1", &"e".repeat(MAX_RPC_ID_BYTES + 1)),
        ] {
            let encoded = request_line(request_id, execution_id);
            assert!(decode_rpc_line::<ExecutorOperation>(&encoded, &identity()).is_err());
        }
    }

    #[test]
    fn executor_execution_ids_are_canonical_artifact_components() {
        let operations = |execution_id: &str| {
            vec![
                ExecutorOperation::ReadFile {
                    path: "notes.txt".to_owned(),
                    offset: 0,
                    limit: MAX_RPC_READ_BYTES,
                    execution_id: execution_id.to_owned(),
                },
                ExecutorOperation::WriteFile {
                    path: "notes.txt".to_owned(),
                    content: "artifact://content|+会話".to_owned(),
                    execution_id: execution_id.to_owned(),
                },
                ExecutorOperation::EditFile {
                    path: "notes.txt".to_owned(),
                    old_string: "artifact://old|+".to_owned(),
                    new_string: "artifact://new|+".to_owned(),
                    execution_id: execution_id.to_owned(),
                },
                ExecutorOperation::RemoveFile {
                    path: "notes.txt".to_owned(),
                    execution_id: execution_id.to_owned(),
                },
                ExecutorOperation::ListDir {
                    path: "workspace".to_owned(),
                    execution_id: execution_id.to_owned(),
                },
                ExecutorOperation::Glob {
                    pattern: "**/*.txt".to_owned(),
                    execution_id: execution_id.to_owned(),
                },
                ExecutorOperation::Grep {
                    path: "workspace".to_owned(),
                    pattern: "artifact://search|+会話".to_owned(),
                    execution_id: execution_id.to_owned(),
                },
                ExecutorOperation::Bash {
                    command: "artifact://command | + echo 会話".to_owned(),
                    execution_id: execution_id.to_owned(),
                },
                ExecutorOperation::Cancel {
                    execution_id: execution_id.to_owned(),
                },
            ]
        };

        for execution_id in ["550e8400-e29b-41d4-a716-446655440000", "execution-_.1"] {
            for operation in operations(execution_id) {
                let encoded = serde_json::to_vec(&RpcRequest {
                    personality_agent_id: PAID.parse().unwrap(),
                    generation: 7,
                    nonce: "boot-nonce".to_owned(),
                    request_id: "request-1".to_owned(),
                    operation,
                })
                .unwrap();
                assert!(decode_rpc_line::<ExecutorOperation>(&encoded, &identity()).is_ok());
            }
        }

        for execution_id in ["with/slash", ".", "..", "会話", "pipe|id", "plus+id"] {
            for operation in operations(execution_id) {
                let encoded = serde_json::to_vec(&RpcRequest {
                    personality_agent_id: PAID.parse().unwrap(),
                    generation: 7,
                    nonce: "boot-nonce".to_owned(),
                    request_id: "request-1".to_owned(),
                    operation,
                })
                .unwrap();
                assert!(matches!(
                    decode_rpc_line::<ExecutorOperation>(&encoded, &identity()),
                    Err(ToolError::InvalidPath(message))
                        if message == "invalid artifact handle component"
                ));
            }
        }
    }

    #[test]
    fn response_ids_are_validated_symmetrically_before_serialization() {
        for request_id in ["".to_owned(), "r".repeat(MAX_RPC_ID_BYTES + 1)] {
            let update = update_frame(request_id.clone());
            let mut terminal = terminal_frame(Ok(json!({"done": true})));
            let RpcFrame::Terminal {
                request_id: terminal_id,
                ..
            } = &mut terminal
            else {
                unreachable!()
            };
            *terminal_id = request_id;
            for frame in [&update, &terminal] {
                assert!(matches!(
                    encode_rpc_frame(frame),
                    Err(ToolError::Protocol(message))
                        if message == "RPC request_id must contain 1..=128 bytes"
                ));
                let raw_frame = serde_json::to_vec(frame).expect("encode malformed ID fixture");
                assert!(matches!(
                    decode_rpc_frame::<Value>(&raw_frame, &identity()),
                    Err(ToolError::Protocol(message))
                        if message == "RPC request_id must contain 1..=128 bytes"
                ));
            }
        }
    }

    #[test]
    fn response_roundtrip_has_exact_identity_and_rejects_stale_peers() {
        let frame = update_frame("request-1");
        let encoded = encode_rpc_frame(&frame).expect("encode response");
        assert_eq!(
            serde_json::from_slice::<Value>(&encoded).expect("decode wire fixture"),
            json!({
                "type": "update",
                "personality_agent_id": PAID,
                "generation": 7,
                "nonce": "boot-nonce",
                "request_id": "request-1",
                "value": {"chunk": "x"},
            }),
        );
        let line = &encoded[..encoded.len() - 1];
        assert_eq!(decode_rpc_frame::<Value>(line, &identity()).unwrap(), frame);

        for stale in [
            RpcIdentity::from_wire("0198f0f4-9b72-7000-8000-000000000002", 7, "boot-nonce")
                .unwrap(),
            RpcIdentity::from_wire(PAID, 8, "boot-nonce").unwrap(),
            RpcIdentity::from_wire(PAID, 7, "stale-nonce").unwrap(),
        ] {
            assert!(matches!(
                decode_rpc_frame::<Value>(line, &stale),
                Err(ToolError::Protocol(message))
                    if message == "RPC personality agent, generation, or boot nonce mismatch"
            ));
        }
    }

    #[test]
    fn terminal_rpc_errors_require_a_matching_typed_resource_limit() {
        for error in [
            RpcError {
                code: "resource_limit".to_owned(),
                resource_limit: None,
            },
            RpcError {
                code: "cancelled".to_owned(),
                resource_limit: Some(ResourceLimit::Concurrency),
            },
        ] {
            assert!(encode_rpc_frame(&terminal_frame(Err(error))).is_err());
        }

        let encoded = encode_rpc_frame(&terminal_frame(Err(RpcError {
            code: "resource_limit".to_owned(),
            resource_limit: Some(ResourceLimit::OutputBytes {
                observed: 11,
                limit: 10,
            }),
        })))
        .expect("valid typed resource limit");
        assert_eq!(
            serde_json::from_slice::<Value>(&encoded).expect("decode response"),
            json!({
                "type": "terminal",
                "personality_agent_id": PAID,
                "generation": 7,
                "nonce": "boot-nonce",
                "request_id": "request-1",
                "result": {
                    "Err": {
                        "code": "resource_limit",
                        "resource_limit": {
                            "type": "output_bytes",
                            "observed": 11,
                            "limit": 10,
                        },
                    },
                },
            }),
        );
    }

    #[test]
    fn nested_resource_limit_rejects_unknown_fields() {
        let raw = br#"{"type":"terminal","generation":7,"nonce":"boot-nonce","request_id":"request-1","result":{"Err":{"code":"resource_limit","resource_limit":{"type":"output_bytes","observed":11,"limit":10,"extra":true}}}}"#;
        assert!(matches!(
            decode_rpc_frame::<Value>(raw, &identity()),
            Err(ToolError::Protocol(message)) if message.starts_with("invalid RPC JSON:")
        ));
    }

    #[test]
    fn error_codes_are_nonempty_bounded_and_forward_compatible() {
        for code in ["".to_owned(), "e".repeat(MAX_RPC_ERROR_CODE_BYTES + 1)] {
            let frame = terminal_frame(Err(RpcError {
                code,
                resource_limit: None,
            }));
            assert!(matches!(
                encode_rpc_frame(&frame),
                Err(ToolError::Protocol(message))
                    if message == "RPC error code must contain 1..=128 bytes"
            ));
            let raw_frame = serde_json::to_vec(&frame).expect("encode malformed error fixture");
            assert!(matches!(
                decode_rpc_frame::<Value>(&raw_frame, &identity()),
                Err(ToolError::Protocol(message))
                    if message == "RPC error code must contain 1..=128 bytes"
            ));
        }

        for code in ["known_failure", "provider_specific_failure"] {
            let frame = terminal_frame(Err(RpcError {
                code: code.to_owned(),
                resource_limit: None,
            }));
            let encoded = encode_rpc_frame(&frame).expect("bounded code remains valid");
            assert_eq!(
                decode_rpc_frame::<Value>(&encoded[..encoded.len() - 1], &identity()).unwrap(),
                frame,
            );
        }
    }

    #[test]
    fn read_operations_reject_limits_above_the_protocol_cap() {
        for operation in [
            ExecutorOperation::ReadFile {
                path: "notes.txt".to_owned(),
                offset: 0,
                limit: MAX_RPC_READ_BYTES,
                execution_id: "execution-1".to_owned(),
            },
            ExecutorOperation::ReadFile {
                path: "notes.txt".to_owned(),
                offset: 0,
                limit: MAX_RPC_READ_BYTES + 1,
                execution_id: "execution-1".to_owned(),
            },
        ] {
            let request = RpcRequest {
                personality_agent_id: PAID.parse().unwrap(),
                generation: 7,
                nonce: "boot-nonce".to_owned(),
                request_id: "request-1".to_owned(),
                operation,
            };
            let encoded = serde_json::to_vec(&request).unwrap();
            let decoded = decode_rpc_line::<ExecutorOperation>(&encoded, &identity());
            match request.operation {
                ExecutorOperation::ReadFile { limit, .. } if limit == MAX_RPC_READ_BYTES => {
                    assert!(decoded.is_ok())
                }
                ExecutorOperation::ReadFile { .. } => assert!(matches!(
                    decoded,
                    Err(ToolError::Protocol(message))
                        if message == "RPC read limit must be <= 51200 bytes"
                )),
                _ => unreachable!(),
            }
        }

        for (limit, valid) in [(MAX_RPC_READ_BYTES, true), (MAX_RPC_READ_BYTES + 1, false)] {
            let request = RpcRequest {
                personality_agent_id: PAID.parse().unwrap(),
                generation: 7,
                nonce: "boot-nonce".to_owned(),
                request_id: "request-1".to_owned(),
                operation: ArtifactOperation::ReadArtifact {
                    handle:
                        "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/execution-1"
                            .to_owned(),
                    offset: 0,
                    limit,
                },
            };
            let encoded = serde_json::to_vec(&request).unwrap();
            assert_eq!(
                decode_rpc_line::<ArtifactOperation>(&encoded, &identity()).is_ok(),
                valid,
            );
        }
    }

    #[test]
    fn artifact_operations_validate_handle_and_pattern_fields() {
        let handle = "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/execution-1";
        let valid = vec![
            ArtifactOperation::BeginToolOutput {
                execution_id: "execution-1".to_owned(),
                content: vec![],
            },
            ArtifactOperation::AppendToolOutput {
                handle: handle.to_owned(),
                offset: u64::MAX,
                content: vec![],
            },
            ArtifactOperation::FinishToolOutput {
                handle: handle.to_owned(),
            },
            ArtifactOperation::ReadArtifact {
                handle: handle.to_owned(),
                offset: u64::MAX,
                limit: MAX_RPC_READ_BYTES,
            },
            ArtifactOperation::GrepArtifact {
                handle: handle.to_owned(),
                pattern: "needle".to_owned(),
            },
        ];
        for operation in valid {
            let encoded = serde_json::to_vec(&RpcRequest {
                personality_agent_id: PAID.parse().unwrap(),
                generation: 7,
                nonce: "boot-nonce".to_owned(),
                request_id: "request-1".to_owned(),
                operation,
            })
            .unwrap();
            assert!(decode_rpc_line::<ArtifactOperation>(&encoded, &identity()).is_ok());
        }

        let invalid = vec![
            ArtifactOperation::BeginToolOutput {
                execution_id: "".to_owned(),
                content: vec![],
            },
            ArtifactOperation::AppendToolOutput {
                handle: "".to_owned(),
                offset: 0,
                content: vec![],
            },
            ArtifactOperation::FinishToolOutput {
                handle: "/workspace/not-an-artifact".to_owned(),
            },
            ArtifactOperation::ReadArtifact {
                handle: "workspace-file".to_owned(),
                offset: 0,
                limit: MAX_RPC_READ_BYTES,
            },
            ArtifactOperation::GrepArtifact {
                handle: handle.to_owned(),
                pattern: "".to_owned(),
            },
        ];
        for operation in invalid {
            let encoded = serde_json::to_vec(&RpcRequest {
                personality_agent_id: PAID.parse().unwrap(),
                generation: 7,
                nonce: "boot-nonce".to_owned(),
                request_id: "request-1".to_owned(),
                operation,
            })
            .unwrap();
            assert!(decode_rpc_line::<ArtifactOperation>(&encoded, &identity()).is_err());
        }
    }

    #[test]
    fn artifact_operation_wire_has_no_nested_owner_override() {
        let raw = json!({
            "personality_agent_id": PAID,
            "generation": 7,
            "nonce": "boot-nonce",
            "request_id": "request-1",
            "operation": {
                "type": "read_artifact",
                "personality_agent_id": "0198f0f4-9b72-7000-8000-000000000002",
                "handle": format!("artifact://{PAID}/tool-output/execution-1"),
                "offset": 0,
                "limit": MAX_RPC_READ_BYTES,
            },
        });
        let encoded = serde_json::to_vec(&raw).unwrap();

        assert!(matches!(
            decode_rpc_line::<ArtifactOperation>(&encoded, &identity()),
            Err(ToolError::Protocol(message)) if message.contains("unknown field")
        ));
    }

    #[test]
    fn artifact_operations_enforce_kind_specific_access() {
        let tool_output = "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/execution-1";
        let attachment = "artifact://0198f0f4-9b72-7000-8000-000000000001/attachments/input-1";
        let cases = [
            (
                ArtifactOperation::AppendToolOutput {
                    handle: tool_output.to_owned(),
                    offset: 0,
                    content: vec![],
                },
                true,
            ),
            (
                ArtifactOperation::FinishToolOutput {
                    handle: tool_output.to_owned(),
                },
                true,
            ),
            (
                ArtifactOperation::AppendToolOutput {
                    handle: attachment.to_owned(),
                    offset: 0,
                    content: vec![],
                },
                false,
            ),
            (
                ArtifactOperation::FinishToolOutput {
                    handle: attachment.to_owned(),
                },
                false,
            ),
            (
                ArtifactOperation::ReadArtifact {
                    handle: tool_output.to_owned(),
                    offset: 0,
                    limit: MAX_RPC_READ_BYTES,
                },
                true,
            ),
            (
                ArtifactOperation::ReadArtifact {
                    handle: attachment.to_owned(),
                    offset: 0,
                    limit: MAX_RPC_READ_BYTES,
                },
                true,
            ),
            (
                ArtifactOperation::GrepArtifact {
                    handle: tool_output.to_owned(),
                    pattern: "needle".to_owned(),
                },
                true,
            ),
            (
                ArtifactOperation::GrepArtifact {
                    handle: attachment.to_owned(),
                    pattern: "needle".to_owned(),
                },
                true,
            ),
        ];

        for (operation, expected_valid) in cases {
            let encoded = serde_json::to_vec(&RpcRequest {
                personality_agent_id: PAID.parse().unwrap(),
                generation: 7,
                nonce: "boot-nonce".to_owned(),
                request_id: "request-1".to_owned(),
                operation,
            })
            .unwrap();
            let decoded = decode_rpc_line::<ArtifactOperation>(&encoded, &identity());
            if expected_valid {
                assert!(decoded.is_ok());
            } else {
                assert!(matches!(
                    decoded,
                    Err(ToolError::InvalidPath(message))
                        if message == "tool-output mutation requires a tool-output artifact handle"
                ));
            }
        }
    }

    #[test]
    fn begin_tool_output_ids_are_canonical_handle_components() {
        for execution_id in ["execution_1.test".to_owned(), "e".repeat(MAX_RPC_ID_BYTES)] {
            let encoded = serde_json::to_vec(&RpcRequest {
                personality_agent_id: PAID.parse().unwrap(),
                generation: 7,
                nonce: "boot-nonce".to_owned(),
                request_id: "request-1".to_owned(),
                operation: ArtifactOperation::BeginToolOutput {
                    execution_id,
                    content: vec![],
                },
            })
            .unwrap();
            assert!(decode_rpc_line::<ArtifactOperation>(&encoded, &identity()).is_ok());
        }

        for invalid_component in [
            "with/slash".to_owned(),
            ".".to_owned(),
            "..".to_owned(),
            "会話".to_owned(),
            "x".repeat(201),
        ] {
            let encoded = serde_json::to_vec(&RpcRequest {
                personality_agent_id: PAID.parse().unwrap(),
                generation: 7,
                nonce: "boot-nonce".to_owned(),
                request_id: "request-1".to_owned(),
                operation: ArtifactOperation::BeginToolOutput {
                    execution_id: invalid_component,
                    content: vec![],
                },
            })
            .unwrap();
            assert!(matches!(
                decode_rpc_line::<ArtifactOperation>(&encoded, &identity()),
                Err(ToolError::InvalidPath(message))
                    if message == "invalid artifact handle component"
            ));
        }
    }

    #[test]
    fn executor_operations_only_decode_artifact_inputs_for_routable_tools() {
        let handle = "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/execution-1";
        let invalid = [
            ExecutorOperation::WriteFile {
                path: handle.to_owned(),
                content: String::new(),
                execution_id: "execution-1".to_owned(),
            },
            ExecutorOperation::EditFile {
                path: handle.to_owned(),
                old_string: "old".to_owned(),
                new_string: "new".to_owned(),
                execution_id: "execution-1".to_owned(),
            },
            ExecutorOperation::RemoveFile {
                path: handle.to_owned(),
                execution_id: "execution-1".to_owned(),
            },
            ExecutorOperation::ListDir {
                path: handle.to_owned(),
                execution_id: "execution-1".to_owned(),
            },
            ExecutorOperation::Glob {
                pattern: handle.to_owned(),
                execution_id: "execution-1".to_owned(),
            },
        ];
        for operation in invalid {
            let encoded = serde_json::to_vec(&RpcRequest {
                personality_agent_id: PAID.parse().unwrap(),
                generation: 7,
                nonce: "boot-nonce".to_owned(),
                request_id: "request-1".to_owned(),
                operation,
            })
            .unwrap();
            assert!(matches!(
                decode_rpc_line::<ExecutorOperation>(&encoded, &identity()),
                Err(ToolError::InvalidPath(message))
                    if message.starts_with("executor workspace ")
            ));
        }

        for operation in [
            ExecutorOperation::ReadFile {
                path: handle.to_owned(),
                offset: 0,
                limit: MAX_RPC_READ_BYTES,
                execution_id: "execution-read".to_owned(),
            },
            ExecutorOperation::Grep {
                path: handle.to_owned(),
                pattern: "needle".to_owned(),
                execution_id: "execution-grep".to_owned(),
            },
        ] {
            let encoded = serde_json::to_vec(&RpcRequest {
                personality_agent_id: PAID.parse().unwrap(),
                generation: 7,
                nonce: "boot-nonce".to_owned(),
                request_id: "request-1".to_owned(),
                operation,
            })
            .unwrap();
            assert!(decode_rpc_line::<ExecutorOperation>(&encoded, &identity()).is_ok());
        }

        let embedded = "workspace/artifact://literal";
        let valid = [
            ExecutorOperation::ReadFile {
                path: embedded.to_owned(),
                offset: 0,
                limit: MAX_RPC_READ_BYTES,
                execution_id: "execution-1".to_owned(),
            },
            ExecutorOperation::WriteFile {
                path: embedded.to_owned(),
                content: "artifact://literal content".to_owned(),
                execution_id: "execution-1".to_owned(),
            },
            ExecutorOperation::EditFile {
                path: embedded.to_owned(),
                old_string: "artifact://old".to_owned(),
                new_string: "artifact://new".to_owned(),
                execution_id: "execution-1".to_owned(),
            },
            ExecutorOperation::RemoveFile {
                path: embedded.to_owned(),
                execution_id: "execution-1".to_owned(),
            },
            ExecutorOperation::ListDir {
                path: embedded.to_owned(),
                execution_id: "execution-1".to_owned(),
            },
            ExecutorOperation::Glob {
                pattern: embedded.to_owned(),
                execution_id: "execution-1".to_owned(),
            },
            ExecutorOperation::Grep {
                path: embedded.to_owned(),
                pattern: "artifact://literal-pattern".to_owned(),
                execution_id: "execution-1".to_owned(),
            },
        ];
        for operation in valid {
            let encoded = serde_json::to_vec(&RpcRequest {
                personality_agent_id: PAID.parse().unwrap(),
                generation: 7,
                nonce: "boot-nonce".to_owned(),
                request_id: "request-1".to_owned(),
                operation,
            })
            .unwrap();
            assert!(decode_rpc_line::<ExecutorOperation>(&encoded, &identity()).is_ok());
        }
    }

    #[test]
    fn artifact_handle_parser_enforces_the_canonical_three_segment_shape() {
        assert_eq!(
            parse_artifact_handle(
                "artifact://0198f0f4-9b72-7000-8000-000000000001/attachments/attachment_1.json"
            )
            .unwrap(),
            ParsedArtifactHandle {
                personality_agent_id: PAID.parse().unwrap(),
                kind: ArtifactKind::Attachments,
                artifact_id: "attachment_1.json",
            },
        );
        assert_eq!(
            parse_artifact_handle(
                "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/execution-1"
            )
            .unwrap(),
            ParsedArtifactHandle {
                personality_agent_id: PAID.parse().unwrap(),
                kind: ArtifactKind::ToolOutput,
                artifact_id: "execution-1",
            },
        );

        let invalid = vec![
            "".to_owned(),
            "artifact://".to_owned(),
            "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/".to_owned(),
            "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output".to_owned(),
            "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/id/extra".to_owned(),
            "artifact://0198f0f4-9b72-7000-8000-000000000001/unknown/id".to_owned(),
            "artifact://./tool-output/id".to_owned(),
            "artifact://../tool-output/id".to_owned(),
            "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/.".to_owned(),
            "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/..".to_owned(),
            "artifact://conversation 1/tool-output/id".to_owned(),
            "artifact://会話/tool-output/id".to_owned(),
            "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/invalid:id".to_owned(),
            format!("artifact://{}/tool-output/id", "c".repeat(201)),
            format!(
                "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/{}",
                "a".repeat(201)
            ),
        ];
        for handle in invalid {
            assert!(
                parse_artifact_handle(&handle).is_err(),
                "accepted invalid handle {handle:?}"
            );
        }
        assert!(
            parse_artifact_handle_for_personality_agent(
                "artifact://0198f0f4-9b72-7000-8000-000000000002/tool-output/id",
                &PAID.parse().unwrap(),
            )
            .is_err()
        );
    }

    #[test]
    fn artifact_owner_component_is_a_strict_canonical_personality_agent_id() {
        let valid_handle = format!("artifact://{PAID}/attachments/artifact-_.");
        let parsed = parse_artifact_handle(&valid_handle).expect("canonical PAID handle");
        assert_eq!(parsed.personality_agent_id.as_str(), PAID);
        assert_eq!(
            resolve_input("read_file", &valid_handle).unwrap(),
            InputRoute::Artifact
        );
        let valid_request = serde_json::to_vec(&RpcRequest {
            personality_agent_id: PAID.parse().unwrap(),
            generation: 7,
            nonce: "boot-nonce".to_owned(),
            request_id: "request-1".to_owned(),
            operation: ArtifactOperation::ReadArtifact {
                handle: valid_handle,
                offset: 0,
                limit: MAX_RPC_READ_BYTES,
            },
        })
        .unwrap();
        assert!(decode_rpc_line::<ArtifactOperation>(&valid_request, &identity()).is_ok());

        let max_artifact_id_handle = format!("artifact://{PAID}/tool-output/{}", "a".repeat(200));
        assert_eq!(
            parse_artifact_handle(&max_artifact_id_handle)
                .expect("artifact IDs retain their 200-byte bound")
                .artifact_id
                .len(),
            200
        );

        for invalid_owner in [
            "not-a-uuid",
            "550e8400-e29b-41d4-a716-446655440000",
            "0198F0F4-9B72-7000-8000-000000000001",
        ] {
            let invalid_handle = format!("artifact://{invalid_owner}/attachments/artifact-_.");
            assert!(parse_artifact_handle(&invalid_handle).is_err());
            assert!(resolve_input("read_file", &invalid_handle).is_err());
        }
    }

    #[test]
    fn lifecycle_execution_ids_match_wire_identity_validation_without_partial_mutation() {
        for valid_execution_id in ["550e8400-e29b-41d4-a716-446655440000", "execution-_.1"] {
            let mut tracker = RpcLifecycleTracker::default();
            tracker
                .begin_execution("request-1", valid_execution_id)
                .expect("canonical execution ID begins");
            tracker
                .accept_cancel("cancel-1", valid_execution_id)
                .expect("canonical execution ID cancels");
            assert_eq!(tracker.active_requests.len(), 2);
            assert_eq!(tracker.executions.len(), 1);
            assert!(tracker.cancelled_executions.contains(valid_execution_id));
        }

        for invalid_execution_id in ["with/slash", ".", "..", "会話", "pipe|id", "plus+id"] {
            let mut begin_tracker = RpcLifecycleTracker::default();
            assert!(matches!(
                begin_tracker.begin_execution("request-1", invalid_execution_id),
                Err(ToolError::InvalidPath(message))
                    if message == "invalid artifact handle component"
            ));
            assert!(begin_tracker.active_requests.is_empty());
            assert!(begin_tracker.active_cancel_requests.is_empty());
            assert!(begin_tracker.executions.is_empty());
            assert!(begin_tracker.cancelled_executions.is_empty());
            begin_tracker
                .begin_execution("request-1", "execution-_.1")
                .expect("failed validation did not consume request or capacity");

            let mut cancel_tracker = RpcLifecycleTracker::default();
            cancel_tracker
                .begin_execution("request-1", "execution-_.1")
                .expect("begin cancellation target");
            assert!(matches!(
                cancel_tracker.accept_cancel("cancel-1", invalid_execution_id),
                Err(ToolError::InvalidPath(message))
                    if message == "invalid artifact handle component"
            ));
            assert_eq!(cancel_tracker.active_requests.len(), 1);
            assert!(cancel_tracker.active_cancel_requests.is_empty());
            assert_eq!(cancel_tracker.executions.len(), 1);
            assert!(cancel_tracker.cancelled_executions.is_empty());
            cancel_tracker
                .accept_cancel("cancel-1", "execution-_.1")
                .expect("failed validation did not consume cancel request or capacity");
            assert_eq!(cancel_tracker.active_requests.len(), 2);
            assert!(
                cancel_tracker
                    .cancelled_executions
                    .contains("execution-_.1")
            );
        }
    }

    #[test]
    fn lifecycle_cancel_requests_are_terminal_only() {
        let mut tracker = RpcLifecycleTracker::default();
        tracker
            .begin_execution("execution-request", "execution-1")
            .expect("begin execution");
        tracker
            .begin_request("ordinary-request")
            .expect("begin ordinary request");
        tracker
            .accept_update("execution-request")
            .expect("execution update");
        tracker
            .accept_update("ordinary-request")
            .expect("ordinary request update");

        tracker
            .accept_cancel("cancel-request", "execution-1")
            .expect("accept cancel");
        assert!(tracker.active_cancel_requests.contains("cancel-request"));
        assert!(matches!(
            tracker.accept_update("cancel-request"),
            Err(ToolError::Protocol(message))
                if message == "RPC cancel request cannot emit updates"
        ));

        tracker
            .accept_terminal("cancel-request")
            .expect("cancel terminal");
        assert!(!tracker.active_cancel_requests.contains("cancel-request"));
        assert!(matches!(
            tracker.accept_update("cancel-request"),
            Err(ToolError::Protocol(message))
                if message == "RPC update arrived after terminal response"
        ));
        assert!(matches!(
            tracker.accept_terminal("cancel-request"),
            Err(ToolError::Protocol(message))
                if message == "RPC request emitted more than one terminal response"
        ));
        assert!(matches!(
            tracker.accept_update("unknown-request"),
            Err(ToolError::Protocol(message))
                if message == "RPC update referenced an unknown request"
        ));
        tracker
            .accept_update("execution-request")
            .expect("execution remains update-capable");
        tracker
            .accept_update("ordinary-request")
            .expect("ordinary request remains update-capable");
    }

    #[test]
    fn lifecycle_enforces_one_terminal_and_exact_boot_scoped_uniqueness() {
        let mut tracker = RpcLifecycleTracker::default();
        tracker
            .begin_execution("request-1", "execution-1")
            .expect("begin");
        tracker.accept_update("request-1").expect("update");
        tracker
            .accept_cancel("cancel-1", "execution-1")
            .expect("cancel");
        assert!(tracker.accept_cancel("cancel-2", "execution-1").is_err());
        assert!(!tracker.active_cancel_requests.contains("cancel-2"));
        tracker
            .accept_terminal("cancel-1")
            .expect("cancel terminal");
        tracker.accept_terminal("request-1").expect("terminal");
        assert!(tracker.accept_update("request-1").is_err());
        assert!(tracker.accept_terminal("request-1").is_err());

        for index in 0..4_097 {
            let request_id = format!("request-next-{index}");
            tracker
                .begin_request(&request_id)
                .expect("begin replay entry");
            tracker
                .accept_terminal(&request_id)
                .expect("complete replay entry");
        }
        assert_eq!(tracker.completed_requests.len(), 4_099);
        assert!(matches!(
            tracker.begin_request("request-next-0"),
            Err(ToolError::Protocol(message)) if message == "RPC request_id must be unique"
        ));
        assert!(matches!(
            tracker.begin_execution("request-fresh", "execution-1"),
            Err(ToolError::Protocol(message)) if message == "RPC execution_id must be unique"
        ));
        assert!(!tracker.active_requests.contains("request-fresh"));
    }

    #[test]
    fn lifecycle_caps_active_requests_without_partial_mutation() {
        let mut tracker = RpcLifecycleTracker::default();
        for index in 0..RPC_ORDINARY_ACTIVE_REQUEST_CAPACITY {
            tracker
                .begin_request(&format!("request-{index}"))
                .expect("fill ordinary active capacity");
        }
        assert_eq!(
            tracker.active_requests.len(),
            RPC_ORDINARY_ACTIVE_REQUEST_CAPACITY
        );
        assert!(matches!(
            tracker.begin_request("request-over-cap"),
            Err(ToolError::Protocol(message))
                if message == "RPC ordinary active request limit of 4095 was reached"
        ));
        assert_eq!(
            tracker.active_requests.len(),
            RPC_ORDINARY_ACTIVE_REQUEST_CAPACITY
        );

        assert!(
            tracker
                .begin_execution("execution-request-over-cap", "execution-over-cap")
                .is_err()
        );
        assert!(
            !tracker
                .active_requests
                .contains("execution-request-over-cap")
        );
        assert!(!tracker.executions.contains_key("execution-over-cap"));

        let mut cancel_tracker = RpcLifecycleTracker::default();
        cancel_tracker
            .begin_execution("execution-request-1", "execution-1")
            .expect("begin first execution");
        cancel_tracker
            .begin_execution("execution-request-2", "execution-2")
            .expect("begin second execution");
        for index in 2..RPC_ORDINARY_ACTIVE_REQUEST_CAPACITY {
            cancel_tracker
                .begin_request(&format!("request-{index}"))
                .expect("saturate ordinary request capacity");
        }
        assert_eq!(
            cancel_tracker.active_requests.len(),
            RPC_ORDINARY_ACTIVE_REQUEST_CAPACITY
        );
        cancel_tracker
            .accept_cancel("cancel-1", "execution-1")
            .expect("reserved cancel slot remains available");
        assert_eq!(
            cancel_tracker.active_requests.len(),
            RPC_ACTIVE_REQUEST_CAPACITY
        );
        assert!(cancel_tracker.active_requests.contains("cancel-1"));
        assert!(cancel_tracker.active_cancel_requests.contains("cancel-1"));
        assert!(cancel_tracker.cancelled_executions.contains("execution-1"));

        assert!(matches!(
            cancel_tracker.accept_cancel("cancel-over-cap", "execution-2"),
            Err(ToolError::Protocol(message))
                if message == "RPC active request limit of 4096 was reached"
        ));
        assert_eq!(
            cancel_tracker.active_requests.len(),
            RPC_ACTIVE_REQUEST_CAPACITY
        );
        assert!(!cancel_tracker.active_requests.contains("cancel-over-cap"));
        assert!(
            !cancel_tracker
                .active_cancel_requests
                .contains("cancel-over-cap")
        );
        assert!(!cancel_tracker.cancelled_executions.contains("execution-2"));
        assert_eq!(cancel_tracker.executions.len(), 2);
    }

    #[test]
    fn only_read_file_and_grep_accept_artifact_inputs() {
        let handle = "artifact://0198f0f4-9b72-7000-8000-000000000001/tool-output/execution";
        assert_eq!(
            resolve_input("read_file", handle).unwrap(),
            InputRoute::Artifact
        );
        assert_eq!(resolve_input("grep", handle).unwrap(), InputRoute::Artifact);
        for tool in ["write_file", "edit_file", "list_dir", "glob", "bash"] {
            assert!(resolve_input(tool, handle).is_err(), "{tool}");
        }
        assert_eq!(
            resolve_input("write_file", "notes.txt").unwrap(),
            InputRoute::Workspace
        );
    }

    #[test]
    fn artifact_input_resolution_rejects_malformed_handles_before_routing() {
        for handle in [
            "artifact://",
            "artifact://conversation/tool-output",
            "artifact://conversation/tool-output/execution/extra",
            "artifact://conversation/unknown/execution",
            "artifact://../tool-output/execution",
            "artifact://conversation/tool-output/..",
            "artifact://conversation/tool-output/invalid:id",
        ] {
            for tool in ["read_file", "grep"] {
                assert!(
                    resolve_input(tool, handle).is_err(),
                    "{tool} accepted malformed handle {handle:?}"
                );
            }
        }

        assert_eq!(
            resolve_input(
                "read_file",
                "artifact://0198f0f4-9b72-7000-8000-000000000002/attachments/input_1.txt",
            )
            .unwrap(),
            InputRoute::Artifact,
        );
        assert_eq!(
            resolve_input("grep", "workspace/artifact://literal").unwrap(),
            InputRoute::Workspace,
        );
    }

    #[test]
    fn attachment_chunks_are_valid_and_fit_a_single_rpc_line() {
        let identity = RpcIdentity::from_wire(PAID, 1, "nonce").unwrap();
        let request = RpcRequest {
            personality_agent_id: PAID.parse().unwrap(),
            generation: 1,
            nonce: "nonce".to_owned(),
            request_id: "attachment-chunk".to_owned(),
            operation: ArtifactOperation::AppendAttachment {
                artifact_id: "input-1".to_owned(),
                total_bytes: (MAX_ATTACHMENT_CHUNK_BYTES * 2) as u64,
                content_digest: "a".repeat(64),
                offset: 0,
                content: vec![b'x'; MAX_ATTACHMENT_CHUNK_BYTES],
            },
        };
        let encoded = serde_json::to_vec(&request).unwrap();
        assert!(framed_rpc_len(encoded.len()).is_some());
        assert_eq!(
            decode_rpc_line::<ArtifactOperation>(&encoded, &identity).unwrap(),
            request
        );
    }
}
