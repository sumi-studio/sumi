//! Durable processes belonging to the authenticated PA's private workspace.
//! Actor identity comes from local-control authentication, never request JSON.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct StartProcessRequest {
    pub originating_tool_call_id: String,
    pub executable: String,
    pub args: Vec<String>,
    pub cwd: String,
    pub timeout_seconds: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ProcessOperationRequest {
    pub operation_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProcessStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ProcessOutputRequest {
    pub operation_id: String,
    pub stream: ProcessStream,
    pub offset: u64,
    pub limit: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProcessState {
    Accepted,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Indeterminate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProcessOperation {
    pub operation_id: String,
    pub personality_agent_id: String,
    pub originating_tool_call_id: String,
    pub executable: String,
    pub args: Vec<String>,
    pub cwd: String,
    pub timeout_seconds: u64,
    pub state: ProcessState,
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub exit_code: Option<i32>,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProcessOutput {
    pub operation_id: String,
    pub stream: ProcessStream,
    pub offset: u64,
    pub next_offset: u64,
    pub content: String,
    pub eof: bool,
    pub truncated: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ProcessApiError {
    #[error("invalid process request")]
    InvalidRequest,
    #[error("process access was not authorized")]
    Unauthorized,
    #[error("process operation was not found")]
    NotFound,
    #[error("process request conflicts with the existing operation")]
    Conflict,
    #[error("process capacity is full; existing operations are still active")]
    Busy,
    #[error("process service is unavailable")]
    Unavailable,
    #[error("process response violated its protocol")]
    Protocol,
}

pub(crate) type ProcessApiResult<T> = Result<T, ProcessApiError>;

pub(crate) fn valid_operation_id(id: &str) -> bool {
    id.len() == 64
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[async_trait]
pub(crate) trait ProcessApi: Send + Sync + 'static {
    async fn start(&self, request: &StartProcessRequest) -> ProcessApiResult<ProcessOperation>;
    async fn status(&self, request: &ProcessOperationRequest)
    -> ProcessApiResult<ProcessOperation>;
    async fn read_output(&self, request: &ProcessOutputRequest) -> ProcessApiResult<ProcessOutput>;
    async fn cancel(&self, request: &ProcessOperationRequest)
    -> ProcessApiResult<ProcessOperation>;
}
