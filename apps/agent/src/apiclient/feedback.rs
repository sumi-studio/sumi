//! The participant's own Feedback Inbox, through authenticated local control.
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum FeedbackRequest {
    Bootstrap {},
    List {
        #[serde(default)]
        status: Option<FeedbackFilter>,
        #[serde(default)]
        cursor: Option<String>,
    },
    Open {
        thread_id: String,
        #[serde(default)]
        cursor: Option<String>,
    },
    Create {
        title: String,
        body: String,
        #[serde(default)]
        request_id: Option<String>,
    },
    Reply {
        thread_id: String,
        body: String,
        #[serde(default)]
        request_id: Option<String>,
    },
    Status {
        thread_id: String,
        status: FeedbackStatus,
        revision: u64,
    },
    Read {
        thread_id: String,
        revision: u64,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FeedbackStatus {
    Open,
    Resolved,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FeedbackFilter {
    Open,
    Resolved,
    All,
}
impl FeedbackRequest {
    pub fn action(&self) -> &'static str {
        match self {
            Self::Bootstrap {} => "bootstrap",
            Self::List { .. } => "list",
            Self::Open { .. } => "open",
            Self::Create { .. } => "create",
            Self::Reply { .. } => "reply",
            Self::Status { .. } => "status",
            Self::Read { .. } => "read",
        }
    }
    pub fn is_read_only(&self) -> bool {
        matches!(
            self,
            Self::Bootstrap {} | Self::List { .. } | Self::Open { .. }
        )
    }
    pub fn thread_id(&self) -> Option<&str> {
        match self {
            Self::Open { thread_id, .. }
            | Self::Reply { thread_id, .. }
            | Self::Status { thread_id, .. }
            | Self::Read { thread_id, .. } => Some(thread_id),
            _ => None,
        }
    }
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::Create { request_id, .. } | Self::Reply { request_id, .. } => {
                request_id.as_deref()
            }
            _ => None,
        }
    }
    pub fn assign_request_id(&mut self) {
        if let Self::Create { request_id, .. } | Self::Reply { request_id, .. } = self {
            request_id.get_or_insert_with(|| {
                uuid::Builder::from_random_bytes(rand::random())
                    .into_uuid()
                    .to_string()
            });
        }
    }
    pub fn validate(&self) -> bool {
        fn valid_uuid(value: &str, version: usize) -> bool {
            Uuid::parse_str(value).is_ok_and(|id| {
                id.to_string() == value
                    && id.get_version_num() == version
                    && id.get_variant() == uuid::Variant::RFC4122
            })
        }
        fn text(value: &str, max: usize) -> bool {
            !value.trim().is_empty() && value.chars().count() <= max
        }
        if self.thread_id().is_some_and(|id| !valid_uuid(id, 7))
            || self.request_id().is_some_and(|id| !valid_uuid(id, 4))
        {
            return false;
        }
        match self {
            Self::Create { title, body, .. } => text(title, 160) && text(body, 20000),
            Self::Reply { body, .. } => text(body, 20000),
            Self::List { cursor, .. } | Self::Open { cursor, .. } => cursor
                .as_ref()
                .is_none_or(|c| !c.is_empty() && c.len() <= 4096),
            Self::Status { revision, .. } | Self::Read { revision, .. } => {
                *revision > 0 && *revision <= 9_007_199_254_740_991
            }
            Self::Bootstrap {} => true,
        }
    }
    pub fn wire(&self) -> Value {
        let mut value = serde_json::to_value(self).expect("serializable Feedback request");
        value
            .as_object_mut()
            .expect("request object")
            .retain(|key, value| key != "action" && !value.is_null());
        value
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FeedbackError {
    pub error: String,
}
impl FeedbackError {
    pub fn new(error: &str) -> Self {
        Self {
            error: error.into(),
        }
    }
}
#[async_trait]
pub(crate) trait FeedbackApi: Send + Sync + 'static {
    async fn request(&self, request: &FeedbackRequest) -> Result<Value, FeedbackError>;
}
