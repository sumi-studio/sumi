//! One stateful messaging view for a PersonalityAgent.
//!
//! This is deliberately not a bag of stateless REST verbs.  The agent sees an
//! overview, opens one place, and writes in the place it currently has open.
//! The view is a tool owned by the continuing person; it is not another agent
//! or another life-log Session.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    sync::Arc,
};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use uuid::{Uuid, Variant, Version};

use crate::{
    apiclient::apps::{AppInstallationResolutionError, ResolveEnabledWorkspaceAppRequest},
    apiclient::messaging::{
        CreateMessagingReplyLaterRequest, ExactMessagingScope, MessagingApi,
        OpenMessagingPlaceRequest, ReactMessagingReactionRequest, ReadMessagingThroughRequest,
        ResolveMessagingReplyLaterRequest, SetMessagingStatusRequest, WriteMessagingMessageRequest,
    },
    provider::types::{ToolDefinition, UserContent},
    tools::{
        AdapterIdentity, AppActionDescriptor, AppPrecondition, BoundExecutionArguments,
        BoundToolAdapter, BoundToolCtx, BoundToolExecutionOutcome, CapabilityClass, DescribeError,
        LiveAppPostCommit, LiveAppPostCommitOutcome, ResourceScope, ReviewProjection, Tool,
        ToolBindCtx, ToolBinding, ToolCtx, ToolError, ToolOutput, ToolRisk,
    },
};

const TOOL_NAME: &str = "messaging";
const BINDING_ADAPTER_ID: &str = "sumi.messaging";
const BINDING_ADAPTER_VERSION: u32 = 2;
const CLIENT_NONCE_DOMAIN: &[u8] = b"sumi-messaging-tool-v1";
const MAX_PLACE_ID_BYTES: usize = 256;
const MAX_CONTENT_BYTES: usize = 64 * 1024;
const MAX_REPLY_ID_BYTES: usize = 256;
const MAX_MESSAGE_ID_BYTES: usize = 256;
const MAX_CLIENT_NONCE_BYTES: usize = 128;
const MAX_MARKER_ID_BYTES: usize = 256;
// The server bounds emoji at 32 characters; 128 bytes covers any such UTF-8.
const MAX_EMOJI_BYTES: usize = 128;
// The server counts characters, not bytes, so this check must too: a 201
// character ASCII note is well under any byte budget and still a 400.
const MAX_STATUS_NOTE_CHARS: usize = 200;
const MAX_REPLY_LATER_NOTE_CHARS: usize = 500;
const DEFAULT_OPEN_LIMIT: usize = 20;
// A week, matching the server's bound on relative durations.
const MAX_RELATIVE_MINUTES: u32 = 7 * 24 * 60;
const MESSAGING_APP_ID: &str = "messaging";
// A single PersonalityAgent is single-threaded and normally inhabits only a
// handful of Workspace installations at once. Sixteen retains ample locality
// while bounding stale uninstall/reinstall and Workspace churn.
const MAX_CACHED_MESSAGING_VIEWS: usize = 16;

#[derive(Clone, Debug, Deserialize)]
struct MessagingProposal {
    workspace_id: String,
    #[serde(flatten)]
    action: MessagingAction,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum MessagingAction {
    /// See the places available to this person and their unread state.
    Overview {},
    /// Open one place as a cohesive screen: timeline, unread line and members.
    Open {
        place_id: String,
        #[serde(default)]
        before_seq: Option<u64>,
        #[serde(default)]
        limit: Option<u16>,
    },
    /// Write in the place currently open in this view.
    Write {
        content: String,
        #[serde(default)]
        urgency: MessagingUrgency,
        #[serde(default)]
        reply_to: Option<String>,
    },
    /// Toggle an emoji reaction on a message visible in the open place.
    React {
        #[serde(default)]
        message_id: Option<String>,
        #[serde(default)]
        seq: Option<u64>,
        emoji: String,
    },
    /// Declare one's own attention state.  Unlike every other action this one
    /// is not about a place: it is about the person, so no view need be open.
    Status {
        status: MessagingStatus,
        #[serde(default)]
        note: Option<String>,
        #[serde(default)]
        expires_in_minutes: Option<u32>,
    },
    /// Promise a later reply to a message visible in the open place.
    ReplyLater {
        #[serde(default)]
        message_id: Option<String>,
        #[serde(default)]
        seq: Option<u64>,
        #[serde(default)]
        note: Option<String>,
        #[serde(default)]
        remind_in_minutes: Option<u32>,
    },
    /// Mark one's own earlier promise as kept. The marker must already be
    /// known in this view, but its place need not remain open.
    ResolveReplyLater { marker_id: String },
}

/// Registry-sealed app arguments. Unlike the model-facing schema, every
/// stateful target has already been resolved to a durable identity.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum BoundMessagingAction {
    Overview {},
    Open {
        place_id: String,
        #[serde(default)]
        before_seq: Option<u64>,
        #[serde(default)]
        limit: Option<u16>,
    },
    Write {
        place_id: String,
        content: String,
        urgency: MessagingUrgency,
        #[serde(default)]
        reply_to: Option<String>,
    },
    React {
        place_id: String,
        message_id: String,
        emoji: String,
    },
    Status {
        status: MessagingStatus,
        #[serde(default)]
        note: Option<String>,
        #[serde(default)]
        expires_in_minutes: Option<u32>,
    },
    ReplyLater {
        place_id: String,
        message_id: String,
        #[serde(default)]
        note: Option<String>,
        #[serde(default)]
        remind_in_minutes: Option<u32>,
    },
    ResolveReplyLater {
        marker_id: String,
    },
}

#[derive(Clone, Debug, Deserialize)]
struct BoundMessagingInvocation {
    workspace_id: String,
    installation_id: String,
    authority_epoch: String,
    #[serde(flatten)]
    action: BoundMessagingAction,
}

#[derive(Clone, Copy)]
enum PostCommitMode {
    /// The legacy raw route cannot return a live post-commit hook. A read
    /// therefore remains pending until a later raw Messaging call, or remains
    /// safely unread if no such call occurs.
    DeferToLaterRawCall,
    ReturnLiveHook,
}

struct ExactMessagingExecutionContext<'a> {
    flow_id: &'a str,
    call_id: &'a str,
    cancel: &'a tokio_util::sync::CancellationToken,
    post_commit_mode: PostCommitMode,
}

struct ExactMessagingOutcome {
    response: Value,
    live_post_commit: Option<LiveAppPostCommit>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MessagingUrgency {
    Urgent,
    #[default]
    Normal,
    Fyi,
}

/// The three self-declared states.  There is no "offline" or "active": nothing
/// here is observed, all of it is said.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MessagingStatus {
    Available,
    Busy,
    Away,
}

/// One message currently on this view's screen. Reactions may only target
/// these (ADR 0011 §3: 見えていないものは操作できない — like a human, the
/// agent reacts to what the open place shows, never to an unseen permalink).
#[derive(Clone, Debug, PartialEq, Eq)]
struct VisibleMessage {
    message_id: String,
    seq: Option<u64>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct OpenPlaceWire {
    kind: String,
    #[serde(default)]
    channel_id: Option<String>,
    #[serde(default)]
    dm_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenParticipantWire {
    kind: String,
    #[serde(default)]
    human_id: Option<String>,
    #[serde(default)]
    personality_agent_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenReactionWire {
    emoji: String,
    participants: Vec<OpenParticipantWire>,
}

#[derive(Debug, Deserialize)]
struct OpenMessageWire {
    message_id: String,
    place: OpenPlaceWire,
    seq: u64,
    author: OpenParticipantWire,
    content: String,
    mentions: Vec<OpenParticipantWire>,
    urgency: String,
    reactions: Vec<OpenReactionWire>,
    // Value keeps null distinct from an omitted required field without
    // recreating the server's nullable-field machinery in this adapter.
    reply_to: Value,
    client_nonce: String,
    created_at: String,
    edited_at: Value,
    deleted: bool,
}

#[derive(Debug, Deserialize)]
struct OpenResponseWire {
    place: OpenPlaceWire,
    latest_seq: u64,
    last_read_seq: u64,
    #[serde(rename = "members")]
    _members: Vec<Value>,
    messages: Vec<OpenMessageWire>,
}

struct ValidatedOpenAdmission {
    visible_messages: Vec<VisibleMessage>,
    read_through_seq: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParticipantIdentity {
    kind: String,
    id: String,
}

/// One unresolved promise already projected into this local view by an exact
/// overview or reply-later result. Binding resolution never fetches it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct VisibleReplyLaterMarker {
    marker_id: String,
    owner: ParticipantIdentity,
    place_kind: String,
    place_id: String,
    message_id: String,
    note: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct MessagingViewState {
    focused_place_id: Option<String>,
    pending_read_through: BTreeMap<String, u64>,
    visible_messages: Vec<VisibleMessage>,
    self_participant: Option<ParticipantIdentity>,
    visible_reply_later_markers: Vec<VisibleReplyLaterMarker>,
}

struct CachedMessagingView {
    view: Arc<Mutex<MessagingViewState>>,
    last_used: u64,
}

#[derive(Default)]
struct MessagingViewCache {
    entries: BTreeMap<ExactMessagingScope, CachedMessagingView>,
    clock: u64,
}

impl MessagingViewCache {
    fn get_or_insert(&mut self, scope: &ExactMessagingScope) -> Arc<Mutex<MessagingViewState>> {
        self.clock = self.clock.wrapping_add(1);
        if self.clock == 0 {
            // Renormalize before an astronomically unlikely wrap so ordering
            // remains meaningful rather than silently reversing the LRU.
            let mut ordered = self
                .entries
                .iter_mut()
                .collect::<Vec<(&ExactMessagingScope, &mut CachedMessagingView)>>();
            ordered.sort_by_key(|(_, entry)| entry.last_used);
            for (index, (_, entry)) in ordered.into_iter().enumerate() {
                entry.last_used = index as u64;
            }
            self.clock = self.entries.len() as u64 + 1;
        }
        if let Some(entry) = self.entries.get_mut(scope) {
            entry.last_used = self.clock;
            return entry.view.clone();
        }
        if self.entries.len() == MAX_CACHED_MESSAGING_VIEWS {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(candidate_scope, entry)| (entry.last_used, *candidate_scope))
                .map(|(candidate_scope, _)| candidate_scope.clone())
                .expect("a full Messaging view cache has an oldest entry");
            // Any in-flight operation or post-result hook owns another Arc.
            // Removing the cache's reference cannot invalidate that work.
            self.entries.remove(&oldest);
        }
        let view = Arc::new(Mutex::new(MessagingViewState::default()));
        self.entries.insert(
            scope.clone(),
            CachedMessagingView {
                view: view.clone(),
                last_used: self.clock,
            },
        );
        view
    }
}

pub(crate) struct MessagingTool {
    api: Arc<dyn MessagingApi>,
    views: Arc<Mutex<MessagingViewCache>>,
}

impl MessagingTool {
    pub(crate) fn new(api: Arc<dyn MessagingApi>) -> Self {
        Self {
            api,
            views: Arc::new(Mutex::new(MessagingViewCache::default())),
        }
    }

    async fn resolve_scope_for_binding(
        &self,
        workspace_id: &str,
    ) -> Result<ExactMessagingScope, DescribeError> {
        validate_canonical_uuid_v7(workspace_id).map_err(|_| DescribeError::InvalidArguments)?;
        let installation = self
            .api
            .resolve_enabled_workspace_app(ResolveEnabledWorkspaceAppRequest {
                workspace_id,
                app_id: MESSAGING_APP_ID,
            })
            .await
            .map_err(map_app_resolution_error)?;
        if installation.workspace_id != workspace_id
            || validate_canonical_uuid_v7(&installation.workspace_id).is_err()
            || validate_canonical_uuid_v7(&installation.installation_id).is_err()
            || !is_canonical_authority_epoch(&installation.authority_epoch)
        {
            return Err(DescribeError::BindingInternal);
        }
        Ok(ExactMessagingScope {
            workspace_id: workspace_id.to_owned(),
            installation_id: installation.installation_id,
            authority_epoch: installation.authority_epoch,
        })
    }

    async fn view_for(&self, scope: &ExactMessagingScope) -> Arc<Mutex<MessagingViewState>> {
        let mut views = self.views.lock().await;
        views.get_or_insert(scope)
    }

    async fn retry_pending_reads_best_effort(
        &self,
        scope: &ExactMessagingScope,
        state: &mut MessagingViewState,
        cancel: &tokio_util::sync::CancellationToken,
    ) {
        let pending = state
            .pending_read_through
            .iter()
            .map(|(place_id, seq)| (place_id.clone(), *seq))
            .collect::<Vec<_>>();
        for (place_id, seq) in pending {
            let result = tokio::select! {
                _ = cancel.cancelled() => return,
                result = self.api.read_through(scope, ReadMessagingThroughRequest {
                    place_id: &place_id,
                    seq,
                }) => result,
            };
            if result.is_ok() {
                clear_pending_read_through(state, &place_id, seq);
            }
        }
    }
}

fn messaging_parameters_schema() -> Value {
    serde_json::json!({
        "type": "object",
        "description": concat!(
            "Choose an explicit workspace_id and one messaging action, then include only the ",
            "fields used by that action. No current or default Workspace is inferred. ",
            "overview needs no other fields; open requires place_id and may include before_seq ",
            "or limit; write requires content and may include urgency or reply_to; react ",
            "requires emoji plus exactly one of message_id or seq; reply_later requires exactly ",
            "one of message_id or seq and may include note or remind_in_minutes; status requires ",
            "status and may include note or expires_in_minutes; resolve_reply_later requires ",
            "marker_id. Write, react and reply_later act on the place most recently opened in ",
            "this tool view; status needs no open place; resolve_reply_later needs a marker ",
            "already shown or returned in this tool view, but not its place open."
        ),
        "properties": {
            "workspace_id": {
                "type": "string",
                "description": "Required for every action. The exact Sumi Workspace whose Messaging app is being used."
            },
            "action": {
                "type": "string",
                "enum": [
                    "overview", "open", "write", "react",
                    "status", "reply_later", "resolve_reply_later"
                ],
                "description": concat!(
                    "Action to perform: overview lists available places and unread state; open ",
                    "shows one place and focuses it for later writes; write sends a message to ",
                    "the currently open place; react toggles an emoji reaction on a message ",
                    "visible in the currently open place; reply_later promises a later reply to ",
                    "such a message so others see it and you are reminded; status declares your ",
                    "own availability; resolve_reply_later marks one of your promises as kept."
                )
            },
            "place_id": {
                "type": "string",
                "description": "Required for open and omitted for other actions. The place to open."
            },
            "before_seq": {
                "type": "integer",
                "minimum": 0,
                "description": "Optional for open and omitted for other actions. Return messages before this sequence number."
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 50,
                "description": "Optional for open and omitted for other actions. Maximum number of messages to return."
            },
            "content": {
                "type": "string",
                "description": "Required for write and omitted for other actions. Message text to send to the currently open place."
            },
            "urgency": {
                "type": "string",
                "enum": ["urgent", "normal", "fyi"],
                "description": "Optional for write and omitted for other actions. Message urgency; when omitted, normal is used."
            },
            "reply_to": {
                "type": "string",
                "description": "Optional for write and omitted for other actions. Message identifier to reply to."
            },
            "message_id": {
                "type": "string",
                "description": concat!(
                    "For react and reply_later, omitted for other actions. The target message by ",
                    "message_id. Provide exactly one of message_id or seq; the message must be ",
                    "visible in the currently open place."
                )
            },
            "seq": {
                "type": "integer",
                "minimum": 1,
                "description": concat!(
                    "For react and reply_later, omitted for other actions. The target message by ",
                    "its seq in the currently open place. Provide exactly one of message_id or seq."
                )
            },
            "emoji": {
                "type": "string",
                "description": concat!(
                    "Required for react and omitted for other actions. Emoji to toggle on the ",
                    "target message; reacting again with the same emoji removes your reaction."
                )
            },
            "status": {
                "type": "string",
                "enum": ["available", "busy", "away"],
                "description": concat!(
                    "Required for status and omitted for other actions. Your own availability, ",
                    "which you declare; nothing about you is published automatically."
                )
            },
            "note": {
                "type": "string",
                "maxLength": 500,
                "description": concat!(
                    "Optional for status and reply_later, omitted for other actions. A short ",
                    "line others see alongside the state or the promise. At most 200 characters ",
                    "for status and 500 for reply_later."
                )
            },
            "expires_in_minutes": {
                "type": "integer",
                "minimum": 1,
                "maximum": 10080,
                "description": concat!(
                    "Optional for status and omitted for other actions. Minutes until the status ",
                    "lapses on its own; when omitted it holds until you replace it."
                )
            },
            "remind_in_minutes": {
                "type": "integer",
                "minimum": 1,
                "maximum": 10080,
                "description": concat!(
                    "Optional for reply_later and omitted for other actions. Minutes until you ",
                    "are reminded to answer; when omitted the default delay is used. This time ",
                    "is yours alone and is not shown to the other participants."
                )
            },
            "marker_id": {
                "type": "string",
                "description": concat!(
                    "Required for resolve_reply_later and omitted for other actions. The ",
                    "marker_id of your unresolved promise already shown or returned in this ",
                    "tool view."
                )
            }
        },
        "required": ["workspace_id", "action"],
        "additionalProperties": false
    })
}

#[async_trait]
impl Tool for MessagingTool {
    fn def(&self) -> ToolDefinition {
        ToolDefinition {
            name: TOOL_NAME.to_owned(),
            description: concat!(
                "Use Sumi's shared messaging app as a person. Use overview to discover ",
                "available places, or open an explicitly known place to see its timeline, ",
                "members and unread state. Then write in that currently open place, or ",
                "react or promise a later reply to a ",
                "message visible in it. Declare your own availability with status. ",
                "Opening never publishes presence: what others see about your ",
                "attention is only what you declare."
            )
            .to_owned(),
            parameters: messaging_parameters_schema(),
        }
    }

    fn risk(&self) -> ToolRisk {
        ToolRisk::Mutating
    }

    fn bound_adapter(self: Arc<Self>) -> Option<Arc<dyn BoundToolAdapter>> {
        Some(self)
    }

    async fn execute(&self, ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
        self.execute_raw(ctx).await
    }
}

#[async_trait]
impl BoundToolAdapter for MessagingTool {
    fn identity(&self) -> AdapterIdentity {
        AdapterIdentity::new(BINDING_ADAPTER_ID, BINDING_ADAPTER_VERSION)
            .expect("static Messaging binding adapter identity must be valid")
    }

    async fn bind(&self, ctx: ToolBindCtx<'_>) -> Result<ToolBinding, DescribeError> {
        let proposal: MessagingProposal =
            serde_json::from_value(Value::Object(ctx.args.as_object().clone()))
                .map_err(|_| DescribeError::InvalidArguments)?;
        validate_action(&proposal.action).map_err(|_| DescribeError::InvalidArguments)?;
        let scope = self
            .resolve_scope_for_binding(&proposal.workspace_id)
            .await?;
        let view = self.view_for(&scope).await;

        match proposal.action {
            MessagingAction::Overview {} => messaging_binding(
                &scope,
                "overview",
                CapabilityClass::Read,
                vec![ResourceScope::collection("messaging", "place")],
                object([("action", Value::String("overview".to_owned()))]),
                object([("action", Value::String("overview".to_owned()))]),
            ),
            MessagingAction::Open {
                place_id,
                before_seq,
                limit,
            } => {
                let mut arguments = Map::new();
                arguments.insert("action".to_owned(), Value::String("open".to_owned()));
                arguments.insert("place_id".to_owned(), Value::String(place_id.clone()));
                insert_optional_u64(&mut arguments, "before_seq", before_seq);
                insert_optional_u64(&mut arguments, "limit", limit.map(u64::from));
                let review_projection = arguments.clone();
                messaging_binding(
                    &scope,
                    "open",
                    CapabilityClass::Read,
                    vec![ResourceScope::resource("messaging", "place", &place_id)],
                    review_projection,
                    arguments,
                )
            }
            MessagingAction::Write {
                content,
                urgency,
                reply_to,
            } => {
                let state = view.lock().await;
                let place_id = focused_place_for_binding(&state, "write")?;
                drop(state);
                let mut scopes = vec![ResourceScope::resource("messaging", "place", &place_id)];
                if let Some(reply_to) = &reply_to {
                    scopes.push(ResourceScope::resource("messaging", "message", reply_to));
                }
                let mut review_projection = object([
                    ("action", Value::String("write".to_owned())),
                    ("place_id", Value::String(place_id.clone())),
                    ("urgency", Value::String(urgency_text(urgency).to_owned())),
                    ("content", Value::String(content.clone())),
                    ("content_bytes", Value::from(content.len() as u64)),
                    (
                        "content_characters",
                        Value::from(content.chars().count() as u64),
                    ),
                ]);
                insert_optional_string(&mut review_projection, "reply_to", reply_to.clone());
                let mut arguments = Map::new();
                arguments.insert("action".to_owned(), Value::String("write".to_owned()));
                arguments.insert("place_id".to_owned(), Value::String(place_id));
                arguments.insert("content".to_owned(), Value::String(content));
                arguments.insert(
                    "urgency".to_owned(),
                    Value::String(urgency_text(urgency).to_owned()),
                );
                insert_optional_string(&mut arguments, "reply_to", reply_to);
                messaging_binding(
                    &scope,
                    "write",
                    CapabilityClass::Mutate,
                    scopes,
                    review_projection,
                    arguments,
                )
            }
            MessagingAction::React {
                message_id,
                seq,
                emoji,
            } => {
                let state = view.lock().await;
                let place_id = focused_place_for_binding(&state, "react")?;
                let target = visible_target_for_binding(&state, &message_id, seq, "react")?;
                let arguments = object([
                    ("action", Value::String("react".to_owned())),
                    ("place_id", Value::String(place_id.clone())),
                    ("message_id", Value::String(target.message_id.clone())),
                    ("emoji", Value::String(emoji)),
                ]);
                messaging_binding(
                    &scope,
                    "react",
                    CapabilityClass::Mutate,
                    vec![
                        ResourceScope::resource("messaging", "place", &place_id),
                        ResourceScope::resource("messaging", "message", &target.message_id),
                    ],
                    arguments.clone(),
                    arguments,
                )
            }
            MessagingAction::Status {
                status,
                note,
                expires_in_minutes,
            } => {
                let status = status_text(status).to_owned();
                let mut review_projection = object([
                    ("action", Value::String("status".to_owned())),
                    ("status", Value::String(status.clone())),
                    ("has_note", Value::Bool(note.is_some())),
                ]);
                if let Some(note) = &note {
                    review_projection.insert("note".to_owned(), Value::String(note.clone()));
                    review_projection.insert(
                        "note_characters".to_owned(),
                        Value::from(note.chars().count() as u64),
                    );
                }
                insert_optional_u64(
                    &mut review_projection,
                    "expires_in_minutes",
                    expires_in_minutes.map(u64::from),
                );
                let mut arguments = Map::new();
                arguments.insert("action".to_owned(), Value::String("status".to_owned()));
                arguments.insert("status".to_owned(), Value::String(status));
                insert_optional_string(&mut arguments, "note", note);
                insert_optional_u64(
                    &mut arguments,
                    "expires_in_minutes",
                    expires_in_minutes.map(u64::from),
                );
                messaging_binding(
                    &scope,
                    "status",
                    CapabilityClass::Mutate,
                    vec![ResourceScope::resource("messaging", "participant", "self")],
                    review_projection,
                    arguments,
                )
            }
            MessagingAction::ReplyLater {
                message_id,
                seq,
                note,
                remind_in_minutes,
            } => {
                let state = view.lock().await;
                let place_id = focused_place_for_binding(&state, "reply_later")?;
                let target = visible_target_for_binding(&state, &message_id, seq, "reply_later")?;
                let mut review_projection = object([
                    ("action", Value::String("reply_later".to_owned())),
                    ("place_id", Value::String(place_id.clone())),
                    ("message_id", Value::String(target.message_id.clone())),
                    ("has_note", Value::Bool(note.is_some())),
                ]);
                if let Some(note) = &note {
                    review_projection.insert("note".to_owned(), Value::String(note.clone()));
                    review_projection.insert(
                        "note_characters".to_owned(),
                        Value::from(note.chars().count() as u64),
                    );
                }
                insert_optional_u64(
                    &mut review_projection,
                    "remind_in_minutes",
                    remind_in_minutes.map(u64::from),
                );
                let mut arguments = object([
                    ("action", Value::String("reply_later".to_owned())),
                    ("place_id", Value::String(place_id.clone())),
                    ("message_id", Value::String(target.message_id.clone())),
                ]);
                insert_optional_string(&mut arguments, "note", note);
                insert_optional_u64(
                    &mut arguments,
                    "remind_in_minutes",
                    remind_in_minutes.map(u64::from),
                );
                messaging_binding(
                    &scope,
                    "reply_later",
                    CapabilityClass::Mutate,
                    vec![
                        ResourceScope::resource("messaging", "place", &place_id),
                        ResourceScope::resource("messaging", "message", &target.message_id),
                    ],
                    review_projection,
                    arguments,
                )
            }
            MessagingAction::ResolveReplyLater { marker_id } => {
                let state = view.lock().await;
                let marker = visible_reply_later_marker_for_binding(&state, &marker_id)?;
                let marker_scope =
                    ResourceScope::resource("messaging", "reply_later_marker", &marker_id);
                let arguments = object([
                    ("action", Value::String("resolve_reply_later".to_owned())),
                    ("marker_id", Value::String(marker_id.clone())),
                ]);
                let review_projection = object([
                    ("action", Value::String("resolve_reply_later".to_owned())),
                    ("marker_id", Value::String(marker_id)),
                    (
                        "marker_meaning",
                        Value::String("own_reply_later_promise".to_owned()),
                    ),
                    ("place_kind", Value::String(marker.place_kind.clone())),
                    ("place_id", Value::String(marker.place_id.clone())),
                    ("message_id", Value::String(marker.message_id.clone())),
                    ("note", Value::String(marker.note.clone())),
                    ("has_note", Value::Bool(!marker.note.is_empty())),
                    (
                        "note_characters",
                        Value::from(marker.note.chars().count() as u64),
                    ),
                ]);
                messaging_binding(
                    &scope,
                    "resolve_reply_later",
                    CapabilityClass::Mutate,
                    vec![
                        ResourceScope::resource("messaging", "participant", "self"),
                        ResourceScope::resource("messaging", "place", &marker.place_id),
                        ResourceScope::resource("messaging", "message", &marker.message_id),
                        marker_scope,
                    ],
                    review_projection,
                    arguments,
                )
            }
        }
    }

    async fn execute(&self, ctx: BoundToolCtx<'_>) -> Result<BoundToolExecutionOutcome, ToolError> {
        let invocation: BoundMessagingInvocation =
            serde_json::from_value(Value::Object(ctx.args.as_object().clone()))
                .map_err(|_| ToolError::InvalidArguments)?;
        let scope = ExactMessagingScope {
            workspace_id: invocation.workspace_id,
            installation_id: invocation.installation_id,
            authority_epoch: invocation.authority_epoch,
        };
        validate_exact_scope(&scope)?;
        validate_bound_action(&invocation.action)?;

        // Exact bound execution performs only the sealed app operation. It
        // does not flush delayed reads, initialize membership through an
        // overview, or reinterpret a target from current view state.
        let view = self.view_for(&scope).await;
        let mut state = view.lock().await;
        if ctx.cancel.is_cancelled() {
            return Err(ToolError::Cancelled);
        }
        let effect_receipt = ctx
            .committed_effect_permit
            .begin_local_effect()
            .complete(|| {
                self.execute_exact_action(
                    &scope,
                    view.clone(),
                    &mut state,
                    invocation.action,
                    ExactMessagingExecutionContext {
                        flow_id: ctx.flow_id,
                        call_id: ctx.call_id,
                        cancel: &ctx.cancel,
                        post_commit_mode: PostCommitMode::ReturnLiveHook,
                    },
                )
            })
            .await?;
        let effect_receipt = effect_receipt.try_map(|outcome| {
            Ok::<_, ToolError>((
                render_messaging_output(outcome.response)?,
                outcome.live_post_commit,
            ))
        })?;
        Ok(BoundToolExecutionOutcome::new(effect_receipt))
    }
}

impl MessagingTool {
    async fn execute_raw(&self, ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
        let proposal: MessagingProposal =
            serde_json::from_value(Value::Object(ctx.args.as_object().clone()))
                .map_err(|_| ToolError::InvalidArguments)?;
        validate_action(&proposal.action)?;
        let scope = self
            .resolve_scope_for_binding(&proposal.workspace_id)
            .await
            .map_err(|error| ToolError::Rpc(error.to_string()))?;
        let view = self.view_for(&scope).await;

        // Serialize only this exact Workspace installation view. It is client
        // state, not the PersonalityAgent or a separate life log.
        let mut state = view.lock().await;

        // Raw ToolOutput has no channel for a post-commit hook. The production
        // runner durably admits each prior ToolResult before starting its next
        // call, so only reads left pending by an earlier raw invocation are
        // eligible here. An Open below merely records its cursor: without a
        // later raw Messaging call it remains on the safe, unread side.
        self.retry_pending_reads_best_effort(&scope, &mut state, &ctx.cancel)
            .await;

        let action = resolve_raw_action(&state, proposal.action)?;
        let outcome = self
            .execute_exact_action(
                &scope,
                view.clone(),
                &mut state,
                action,
                ExactMessagingExecutionContext {
                    flow_id: ctx.flow_id,
                    call_id: ctx.call_id,
                    cancel: &ctx.cancel,
                    post_commit_mode: PostCommitMode::DeferToLaterRawCall,
                },
            )
            .await?;
        debug_assert!(outcome.live_post_commit.is_none());

        render_messaging_output(outcome.response)
    }

    /// Execute exactly one already-resolved Messaging action. Raw and bound
    /// paths share this single seven-arm implementation; only the admission
    /// delivery mode differs. No pre-action maintenance belongs here.
    async fn execute_exact_action(
        &self,
        scope: &ExactMessagingScope,
        view: Arc<Mutex<MessagingViewState>>,
        state: &mut MessagingViewState,
        action: BoundMessagingAction,
        execution: ExactMessagingExecutionContext<'_>,
    ) -> Result<ExactMessagingOutcome, ToolError> {
        let mut live_post_commit = None;
        let response = match action {
            BoundMessagingAction::Overview {} => {
                let response = tokio::select! {
                    _ = execution.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.overview(scope) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?;
                admit_overview_snapshot(state, &response);
                response
            }
            BoundMessagingAction::Open {
                place_id,
                before_seq,
                limit,
            } => {
                let response = tokio::select! {
                    _ = execution.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.open(scope, OpenMessagingPlaceRequest {
                        place_id: &place_id,
                        before_seq,
                        limit,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?;
                let admission = validate_open_response(&response, &place_id, before_seq, limit)?;
                state.focused_place_id = Some(place_id.clone());
                state.visible_messages = admission.visible_messages;
                if let Some(seq) = admission.read_through_seq {
                    record_pending_read_through(state, &place_id, seq);
                    if matches!(execution.post_commit_mode, PostCommitMode::ReturnLiveHook) {
                        live_post_commit = Some(read_through_post_commit(
                            self.api.clone(),
                            scope.clone(),
                            view.clone(),
                            place_id.clone(),
                            seq,
                        ));
                    }
                }
                response
            }
            BoundMessagingAction::Write {
                place_id,
                content,
                urgency,
                reply_to,
            } => {
                let nonce = client_nonce(execution.flow_id, execution.call_id);
                let response = tokio::select! {
                    _ = execution.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.write(scope, WriteMessagingMessageRequest {
                        place_id: &place_id,
                        content: &content,
                        urgency: urgency_text(urgency),
                        reply_to: reply_to.as_deref(),
                        client_nonce: &nonce,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?;
                if state.focused_place_id.as_deref() == Some(place_id.as_str())
                    && let Some(message_id) = response.get("message_id").and_then(Value::as_str)
                {
                    state.visible_messages.push(VisibleMessage {
                        message_id: message_id.to_owned(),
                        seq: response.get("seq").and_then(Value::as_u64),
                    });
                }
                response
            }
            BoundMessagingAction::React {
                place_id,
                message_id,
                emoji,
            } => {
                let nonce = client_nonce(execution.flow_id, execution.call_id);
                tokio::select! {
                    _ = execution.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.react(scope, ReactMessagingReactionRequest {
                        place_id: &place_id,
                        message_id: &message_id,
                        emoji: &emoji,
                        client_nonce: &nonce,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?
            }
            BoundMessagingAction::Status {
                status,
                note,
                expires_in_minutes,
            } => tokio::select! {
                _ = execution.cancel.cancelled() => return Err(ToolError::Cancelled),
                result = self.api.set_status(scope, SetMessagingStatusRequest {
                    status: status_text(status),
                    note: note.as_deref(),
                    expires_in_minutes,
                }) => result,
            }
            .map_err(|error| ToolError::Rpc(error.to_string()))?,
            BoundMessagingAction::ReplyLater {
                place_id,
                message_id,
                note,
                remind_in_minutes,
            } => {
                let response = tokio::select! {
                    _ = execution.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.reply_later(scope, CreateMessagingReplyLaterRequest {
                        place_id: &place_id,
                        message_id: &message_id,
                        note: note.as_deref(),
                        remind_in_minutes,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?;
                if let Some(marker) = reply_later_marker_from_response(&response) {
                    if state.self_participant.is_none() {
                        // The authenticated create endpoint can only return
                        // the caller's own marker, so this exact result can
                        // establish self without an implicit overview fetch.
                        state.self_participant = Some(marker.owner.clone());
                    }
                    upsert_visible_reply_later_marker(state, marker);
                }
                response
            }
            BoundMessagingAction::ResolveReplyLater { marker_id } => {
                let response = tokio::select! {
                    _ = execution.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.resolve_reply_later(scope, ResolveMessagingReplyLaterRequest {
                        marker_id: &marker_id,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?;
                state
                    .visible_reply_later_markers
                    .retain(|marker| marker.marker_id != marker_id);
                response
            }
        };
        Ok(ExactMessagingOutcome {
            response,
            live_post_commit,
        })
    }
}

fn render_messaging_output(response: Value) -> Result<ToolOutput, ToolError> {
    let rendered = serde_json::to_string_pretty(&response)
        .map_err(|error| ToolError::Protocol(error.to_string()))?;
    Ok(ToolOutput {
        content: vec![UserContent::Text { text: rendered }],
        details: response,
        is_error: false,
    })
}

fn messaging_binding(
    scope: &ExactMessagingScope,
    operation: &str,
    capability: CapabilityClass,
    mut resource_scopes: Vec<ResourceScope>,
    mut review_projection: Map<String, Value>,
    mut execution_arguments: Map<String, Value>,
) -> Result<ToolBinding, DescribeError> {
    resource_scopes.insert(
        0,
        ResourceScope::resource("workspace", "workspace", &scope.workspace_id),
    );
    review_projection.insert(
        "workspace_id".to_owned(),
        Value::String(scope.workspace_id.clone()),
    );
    execution_arguments.insert(
        "workspace_id".to_owned(),
        Value::String(scope.workspace_id.clone()),
    );
    execution_arguments.insert(
        "installation_id".to_owned(),
        Value::String(scope.installation_id.clone()),
    );
    execution_arguments.insert(
        "authority_epoch".to_owned(),
        Value::String(scope.authority_epoch.clone()),
    );
    Ok(ToolBinding::new(
        AppActionDescriptor::new(operation, capability, resource_scopes)?,
        ReviewProjection::from_value(Value::Object(review_projection))?,
        BoundExecutionArguments::from_value(Value::Object(execution_arguments))?,
    ))
}

fn object<const N: usize>(entries: [(&str, Value); N]) -> Map<String, Value> {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

fn insert_optional_string(arguments: &mut Map<String, Value>, key: &str, value: Option<String>) {
    if let Some(value) = value {
        arguments.insert(key.to_owned(), Value::String(value));
    }
}

fn insert_optional_u64(arguments: &mut Map<String, Value>, key: &str, value: Option<u64>) {
    if let Some(value) = value {
        arguments.insert(key.to_owned(), Value::from(value));
    }
}

fn read_through_post_commit(
    api: Arc<dyn MessagingApi>,
    scope: ExactMessagingScope,
    view: Arc<Mutex<MessagingViewState>>,
    place_id: String,
    seq: u64,
) -> LiveAppPostCommit {
    LiveAppPostCommit::new(move |cancel| async move {
        let result = tokio::select! {
            _ = cancel.cancelled() => {
                return LiveAppPostCommitOutcome::Deferred(ToolError::Cancelled);
            }
            result = api.read_through(&scope, ReadMessagingThroughRequest {
                place_id: &place_id,
                seq,
            }) => result,
        };
        match result {
            Ok(_) => {
                let mut state = view.lock().await;
                clear_pending_read_through(&mut state, &place_id, seq);
                LiveAppPostCommitOutcome::Applied
            }
            Err(error) => LiveAppPostCommitOutcome::Deferred(ToolError::Rpc(error.to_string())),
        }
    })
}

fn record_pending_read_through(state: &mut MessagingViewState, place_id: &str, seq: u64) {
    state
        .pending_read_through
        .entry(place_id.to_owned())
        .and_modify(|pending| *pending = (*pending).max(seq))
        .or_insert(seq);
}

fn clear_pending_read_through(state: &mut MessagingViewState, place_id: &str, admitted_seq: u64) {
    if state
        .pending_read_through
        .get(place_id)
        .is_some_and(|pending| *pending <= admitted_seq)
    {
        state.pending_read_through.remove(place_id);
    }
}

fn resolve_raw_action(
    state: &MessagingViewState,
    action: MessagingAction,
) -> Result<BoundMessagingAction, ToolError> {
    match action {
        MessagingAction::Overview {} => Ok(BoundMessagingAction::Overview {}),
        MessagingAction::Open {
            place_id,
            before_seq,
            limit,
        } => Ok(BoundMessagingAction::Open {
            place_id,
            before_seq,
            limit,
        }),
        MessagingAction::Write {
            content,
            urgency,
            reply_to,
        } => Ok(BoundMessagingAction::Write {
            place_id: state.focused_place_id.clone().ok_or_else(|| {
                ToolError::Protocol(
                    "open a messaging place before writing; writing is scoped to the place currently in view"
                        .to_owned(),
                )
            })?,
            content,
            urgency,
            reply_to,
        }),
        MessagingAction::React {
            message_id,
            seq,
            emoji,
        } => {
            let place_id = state.focused_place_id.clone().ok_or_else(|| {
                ToolError::Protocol(
                    "open a messaging place before reacting; reactions attach to messages visible in the place currently in view"
                        .to_owned(),
                )
            })?;
            let target = visible_target(state, &message_id, seq, "react")?;
            Ok(BoundMessagingAction::React {
                place_id,
                message_id: target.message_id,
                emoji,
            })
        }
        MessagingAction::Status {
            status,
            note,
            expires_in_minutes,
        } => Ok(BoundMessagingAction::Status {
            status,
            note,
            expires_in_minutes,
        }),
        MessagingAction::ReplyLater {
            message_id,
            seq,
            note,
            remind_in_minutes,
        } => {
            let place_id = state.focused_place_id.clone().ok_or_else(|| {
                ToolError::Protocol(
                    "open a messaging place before promising a reply; the promise attaches to a message visible in the place currently in view"
                        .to_owned(),
                )
            })?;
            let target = visible_target(state, &message_id, seq, "promise a reply")?;
            Ok(BoundMessagingAction::ReplyLater {
                place_id,
                message_id: target.message_id,
                note,
                remind_in_minutes,
            })
        }
        MessagingAction::ResolveReplyLater { marker_id } => {
            visible_reply_later_marker(state, &marker_id).ok_or_else(|| {
                ToolError::Protocol(
                    "that unresolved reply-later marker is not known in this messaging view"
                        .to_owned(),
                )
            })?;
            Ok(BoundMessagingAction::ResolveReplyLater { marker_id })
        }
    }
}

fn app_precondition(code: &str, message: String) -> DescribeError {
    DescribeError::AppPrecondition {
        precondition: AppPrecondition::new(code, message)
            .expect("static Messaging precondition code and bounded message must be valid"),
    }
}

fn map_app_resolution_error(error: AppInstallationResolutionError) -> DescribeError {
    match error {
        AppInstallationResolutionError::Forbidden
        | AppInstallationResolutionError::NotFound
        | AppInstallationResolutionError::InstallationNotFound
        | AppInstallationResolutionError::AppDisabled => app_precondition(
            "enabled_installation_required",
            "the selected Workspace must have an enabled Messaging installation and active membership"
                .to_owned(),
        ),
        AppInstallationResolutionError::AuthenticationUnavailable
        | AppInstallationResolutionError::ServiceUnavailable
        | AppInstallationResolutionError::TransportUnavailable => {
            DescribeError::BindingUnavailable
        }
        AppInstallationResolutionError::Protocol => DescribeError::BindingInternal,
    }
}

fn focused_place_for_binding(
    state: &MessagingViewState,
    operation: &str,
) -> Result<String, DescribeError> {
    state.focused_place_id.clone().ok_or_else(|| {
        app_precondition(
            "focused_resource_required",
            format!("open a messaging place before binding {operation}"),
        )
    })
}

fn visible_target_for_binding(
    state: &MessagingViewState,
    message_id: &Option<String>,
    seq: Option<u64>,
    operation: &str,
) -> Result<VisibleMessage, DescribeError> {
    find_visible_target(state, message_id, seq).ok_or_else(|| {
        app_precondition(
            "visible_target_required",
            format!(
                "target message must be visible in the currently open place before binding {operation}"
            ),
        )
    })
}

fn visible_reply_later_marker_for_binding(
    state: &MessagingViewState,
    marker_id: &str,
) -> Result<VisibleReplyLaterMarker, DescribeError> {
    visible_reply_later_marker(state, marker_id).ok_or_else(|| {
        app_precondition(
            "visible_owned_marker_required",
            "the unresolved reply-later marker must already be known in this messaging view"
                .to_owned(),
        )
    })
}

fn validate_action(action: &MessagingAction) -> Result<(), ToolError> {
    match action {
        MessagingAction::Overview {} => Ok(()),
        MessagingAction::Open {
            place_id, limit, ..
        } => {
            validate_bounded_nonempty(place_id, MAX_PLACE_ID_BYTES)?;
            if limit.is_some_and(|limit| !(1..=50).contains(&limit)) {
                return Err(ToolError::InvalidArguments);
            }
            Ok(())
        }
        MessagingAction::Write {
            content, reply_to, ..
        } => {
            if content.is_empty() || content.len() > MAX_CONTENT_BYTES {
                return Err(ToolError::InvalidArguments);
            }
            if reply_to
                .as_deref()
                .is_some_and(|reply| validate_bounded_nonempty(reply, MAX_REPLY_ID_BYTES).is_err())
            {
                return Err(ToolError::InvalidArguments);
            }
            Ok(())
        }
        MessagingAction::React {
            message_id,
            seq,
            emoji,
        } => {
            validate_visible_selector(message_id, seq)?;
            if emoji.is_empty()
                || emoji.len() > MAX_EMOJI_BYTES
                || emoji
                    .chars()
                    .any(|character| character.is_control() || character.is_whitespace())
            {
                return Err(ToolError::InvalidArguments);
            }
            Ok(())
        }
        MessagingAction::Status {
            note,
            expires_in_minutes,
            ..
        } => {
            validate_optional_note(note, MAX_STATUS_NOTE_CHARS)?;
            validate_relative_minutes(expires_in_minutes)
        }
        MessagingAction::ReplyLater {
            message_id,
            seq,
            note,
            remind_in_minutes,
        } => {
            validate_visible_selector(message_id, seq)?;
            validate_optional_note(note, MAX_REPLY_LATER_NOTE_CHARS)?;
            validate_relative_minutes(remind_in_minutes)
        }
        MessagingAction::ResolveReplyLater { marker_id } => {
            validate_bounded_nonempty(marker_id, MAX_MARKER_ID_BYTES)
        }
    }
}

fn validate_canonical_uuid_v7(value: &str) -> Result<(), ToolError> {
    let parsed = Uuid::parse_str(value).map_err(|_| ToolError::InvalidArguments)?;
    if parsed.get_version() != Some(Version::SortRand)
        || parsed.get_variant() != Variant::RFC4122
        || parsed.to_string() != value
    {
        return Err(ToolError::InvalidArguments);
    }
    Ok(())
}

fn validate_exact_scope(scope: &ExactMessagingScope) -> Result<(), ToolError> {
    validate_canonical_uuid_v7(&scope.workspace_id)?;
    validate_canonical_uuid_v7(&scope.installation_id)?;
    if !is_canonical_authority_epoch(&scope.authority_epoch) {
        return Err(ToolError::InvalidArguments);
    }
    Ok(())
}

fn is_canonical_authority_epoch(value: &str) -> bool {
    if value.is_empty()
        || value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return false;
    }
    value.parse::<i64>().is_ok_and(|epoch| epoch > 0)
}

fn validate_bound_action(action: &BoundMessagingAction) -> Result<(), ToolError> {
    match action {
        BoundMessagingAction::Overview {} => Ok(()),
        BoundMessagingAction::Open {
            place_id,
            before_seq,
            limit,
        } => validate_action(&MessagingAction::Open {
            place_id: place_id.clone(),
            before_seq: *before_seq,
            limit: *limit,
        }),
        BoundMessagingAction::Write {
            place_id,
            content,
            urgency,
            reply_to,
        } => {
            validate_bounded_nonempty(place_id, MAX_PLACE_ID_BYTES)?;
            validate_action(&MessagingAction::Write {
                content: content.clone(),
                urgency: *urgency,
                reply_to: reply_to.clone(),
            })
        }
        BoundMessagingAction::React {
            place_id,
            message_id,
            emoji,
        } => {
            validate_bounded_nonempty(place_id, MAX_PLACE_ID_BYTES)?;
            validate_action(&MessagingAction::React {
                message_id: Some(message_id.clone()),
                seq: None,
                emoji: emoji.clone(),
            })
        }
        BoundMessagingAction::Status {
            status,
            note,
            expires_in_minutes,
        } => validate_action(&MessagingAction::Status {
            status: *status,
            note: note.clone(),
            expires_in_minutes: *expires_in_minutes,
        }),
        BoundMessagingAction::ReplyLater {
            place_id,
            message_id,
            note,
            remind_in_minutes,
        } => {
            validate_bounded_nonempty(place_id, MAX_PLACE_ID_BYTES)?;
            validate_action(&MessagingAction::ReplyLater {
                message_id: Some(message_id.clone()),
                seq: None,
                note: note.clone(),
                remind_in_minutes: *remind_in_minutes,
            })
        }
        BoundMessagingAction::ResolveReplyLater { marker_id } => {
            validate_action(&MessagingAction::ResolveReplyLater {
                marker_id: marker_id.clone(),
            })
        }
    }
}

/// Exactly one selector: the gesture lands on one visible message. React and
/// reply_later share the rule because they are the same kind of act — a
/// response to something on screen.
fn validate_visible_selector(
    message_id: &Option<String>,
    seq: &Option<u64>,
) -> Result<(), ToolError> {
    if message_id.is_some() == seq.is_some() || seq == &Some(0) {
        return Err(ToolError::InvalidArguments);
    }
    if message_id
        .as_deref()
        .is_some_and(|id| validate_bounded_nonempty(id, MAX_MESSAGE_ID_BYTES).is_err())
    {
        return Err(ToolError::InvalidArguments);
    }
    Ok(())
}

fn validate_optional_note(note: &Option<String>, max_chars: usize) -> Result<(), ToolError> {
    if note
        .as_deref()
        .is_some_and(|note| note.chars().count() > max_chars || note.chars().any(char::is_control))
    {
        return Err(ToolError::InvalidArguments);
    }
    Ok(())
}

/// Durations the agent names are relative, so the server's clock fixes the
/// instant. Zero would mean "now", which is a reminder nobody asked for.
fn validate_relative_minutes(minutes: &Option<u32>) -> Result<(), ToolError> {
    if minutes.is_some_and(|minutes| minutes == 0 || minutes > MAX_RELATIVE_MINUTES) {
        return Err(ToolError::InvalidArguments);
    }
    Ok(())
}

/// Resolves one selector against the screen this view currently shows. A
/// message that is not on it cannot be acted on at all (ADR 0011 §3:
/// 見えていないものは操作できない).
fn visible_target(
    state: &MessagingViewState,
    message_id: &Option<String>,
    seq: Option<u64>,
    verb: &str,
) -> Result<VisibleMessage, ToolError> {
    find_visible_target(state, message_id, seq).ok_or_else(|| {
        ToolError::Protocol(format!(
            "that message is not visible in the currently open place; open the place (paging with before_seq if needed) so the message is on screen, then {verb}"
        ))
    })
}

fn find_visible_target(
    state: &MessagingViewState,
    message_id: &Option<String>,
    seq: Option<u64>,
) -> Option<VisibleMessage> {
    state
        .visible_messages
        .iter()
        .find(|message| match (message_id, seq) {
            (Some(id), _) => &message.message_id == id,
            (None, Some(seq)) => message.seq == Some(seq),
            (None, None) => false,
        })
        .cloned()
}

fn visible_reply_later_marker(
    state: &MessagingViewState,
    marker_id: &str,
) -> Option<VisibleReplyLaterMarker> {
    let self_participant = state.self_participant.as_ref()?;
    state
        .visible_reply_later_markers
        .iter()
        .find(|marker| marker.marker_id == marker_id && &marker.owner == self_participant)
        .cloned()
}

fn admit_overview_snapshot(state: &mut MessagingViewState, response: &Value) {
    state.self_participant = response.get("self").and_then(participant_identity_from);
    state.visible_reply_later_markers = response
        .get("reply_later_markers")
        .and_then(Value::as_array)
        .map(|markers| markers.iter().filter_map(reply_later_marker_from).collect())
        .unwrap_or_default();
}

fn reply_later_marker_from_response(response: &Value) -> Option<VisibleReplyLaterMarker> {
    response.get("marker").and_then(reply_later_marker_from)
}

fn reply_later_marker_from(marker: &Value) -> Option<VisibleReplyLaterMarker> {
    if marker.get("resolved")?.as_bool()? {
        return None;
    }
    let marker_id = marker.get("marker_id")?.as_str()?.to_owned();
    let owner = marker
        .get("participant")
        .and_then(participant_identity_from)?;
    let place = marker.get("place")?;
    let place_kind = place.get("kind")?.as_str()?.to_owned();
    let place_id = match place_kind.as_str() {
        "channel" => place.get("channel_id")?.as_str()?,
        "dm" => place.get("dm_id")?.as_str()?,
        _ => return None,
    }
    .to_owned();
    let message_id = marker.get("message_id")?.as_str()?.to_owned();
    let note = marker.get("note")?.as_str()?.to_owned();
    if validate_bounded_nonempty(&marker_id, MAX_MARKER_ID_BYTES).is_err()
        || validate_bounded_nonempty(&place_id, MAX_PLACE_ID_BYTES).is_err()
        || validate_bounded_nonempty(&message_id, MAX_MESSAGE_ID_BYTES).is_err()
        || validate_optional_note(&Some(note.clone()), MAX_REPLY_LATER_NOTE_CHARS).is_err()
    {
        return None;
    }
    Some(VisibleReplyLaterMarker {
        marker_id,
        owner,
        place_kind,
        place_id,
        message_id,
        note,
    })
}

fn participant_identity_from(participant: &Value) -> Option<ParticipantIdentity> {
    let kind = participant.get("kind")?.as_str()?.to_owned();
    let id = match kind.as_str() {
        "human" => participant.get("human_id")?.as_str()?,
        "personality_agent" => participant.get("personality_agent_id")?.as_str()?,
        _ => return None,
    }
    .to_owned();
    if validate_bounded_nonempty(&id, MAX_MESSAGE_ID_BYTES).is_err() {
        return None;
    }
    Some(ParticipantIdentity { kind, id })
}

fn upsert_visible_reply_later_marker(
    state: &mut MessagingViewState,
    marker: VisibleReplyLaterMarker,
) {
    state
        .visible_reply_later_markers
        .retain(|known| known.marker_id != marker.marker_id);
    state.visible_reply_later_markers.push(marker);
}

/// Authenticate one complete Messaging screen before any of it becomes local
/// view state or read evidence. The API returns dense per-place history in
/// ascending order even though it pages backwards; accepting a looser shape
/// would let an incomplete or cross-place response manufacture both visible
/// mutation targets and a read cursor.
fn validate_open_response(
    response: &Value,
    requested_place_id: &str,
    before_seq: Option<u64>,
    requested_limit: Option<u16>,
) -> Result<ValidatedOpenAdmission, ToolError> {
    let page: OpenResponseWire = serde_json::from_value(response.clone()).map_err(|error| {
        ToolError::Protocol(format!("invalid Messaging open response: {error}"))
    })?;
    validate_open_place(&page.place, requested_place_id)?;
    if page.last_read_seq > page.latest_seq {
        return Err(ToolError::Protocol(
            "Messaging open last_read_seq exceeds latest_seq".to_owned(),
        ));
    }
    let limit = requested_limit
        .map(usize::from)
        .unwrap_or(DEFAULT_OPEN_LIMIT);
    if page.messages.len() > limit {
        return Err(ToolError::Protocol(
            "Messaging open returned more messages than requested".to_owned(),
        ));
    }
    let bounded_before = before_seq.filter(|seq| *seq > 0);
    let expected_page_end = bounded_before
        .map(|seq| page.latest_seq.min(seq - 1))
        .unwrap_or(page.latest_seq);
    if page.messages.last().map(|message| message.seq).unwrap_or(0) != expected_page_end {
        return Err(ToolError::Protocol(
            "Messaging open page end is inconsistent with latest_seq or before_seq".to_owned(),
        ));
    }

    let mut message_ids = BTreeSet::new();
    let mut visible_messages = Vec::with_capacity(page.messages.len());
    let mut previous_seq: Option<u64> = None;
    for message in &page.messages {
        if message.seq == 0
            || message.seq > page.latest_seq
            || previous_seq.is_some_and(|previous| previous.checked_add(1) != Some(message.seq))
        {
            return Err(ToolError::Protocol(
                "Messaging open messages are not a positive contiguous ascending page".to_owned(),
            ));
        }
        if bounded_before.is_some_and(|before| message.seq >= before) {
            return Err(ToolError::Protocol(
                "Messaging open returned a sequence at or beyond before_seq".to_owned(),
            ));
        }
        if message.place != page.place {
            return Err(ToolError::Protocol(
                "Messaging open message belongs to a different place".to_owned(),
            ));
        }
        validate_open_message(message)?;
        if !message_ids.insert(message.message_id.as_str()) {
            return Err(ToolError::Protocol(
                "Messaging open contains a duplicate message_id".to_owned(),
            ));
        }
        visible_messages.push(VisibleMessage {
            message_id: message.message_id.clone(),
            seq: Some(message.seq),
        });
        previous_seq = Some(message.seq);
    }

    let mut read_through_seq = page.last_read_seq;
    for message in &page.messages {
        if message.seq <= read_through_seq {
            continue;
        }
        if read_through_seq.checked_add(1) != Some(message.seq) {
            break;
        }
        read_through_seq = message.seq;
    }

    Ok(ValidatedOpenAdmission {
        visible_messages,
        read_through_seq: (read_through_seq > page.last_read_seq).then_some(read_through_seq),
    })
}

fn validate_open_place(place: &OpenPlaceWire, requested_place_id: &str) -> Result<(), ToolError> {
    let place_id = match place.kind.as_str() {
        "channel" if place.dm_id.is_none() => place.channel_id.as_deref(),
        "dm" | "group_dm" if place.channel_id.is_none() => place.dm_id.as_deref(),
        _ => None,
    }
    .filter(|id| is_bounded_nonempty(id, MAX_PLACE_ID_BYTES))
    .ok_or_else(|| ToolError::Protocol("Messaging open returned an invalid place".to_owned()))?;
    if place_id != requested_place_id {
        return Err(ToolError::Protocol(
            "Messaging open response place does not match the requested place".to_owned(),
        ));
    }
    Ok(())
}

fn validate_open_message(message: &OpenMessageWire) -> Result<(), ToolError> {
    if !is_bounded_nonempty(&message.message_id, MAX_MESSAGE_ID_BYTES) {
        return Err(ToolError::Protocol(
            "Messaging open message has an invalid message_id".to_owned(),
        ));
    }
    validate_open_participant(&message.author, "author")?;
    for mention in &message.mentions {
        validate_open_participant(mention, "mention")?;
    }
    if !matches!(message.urgency.as_str(), "urgent" | "normal" | "fyi") {
        return Err(ToolError::Protocol(
            "Messaging open message has an invalid urgency".to_owned(),
        ));
    }
    if message.client_nonce.is_empty() || message.client_nonce.len() > MAX_CLIENT_NONCE_BYTES {
        return Err(ToolError::Protocol(
            "Messaging open message has an invalid client_nonce".to_owned(),
        ));
    }
    if !valid_nullable_string(&message.reply_to, MAX_REPLY_ID_BYTES) {
        return Err(ToolError::Protocol(
            "Messaging open message has an invalid reply_to".to_owned(),
        ));
    }
    if message.created_at.is_empty() || !valid_nullable_string(&message.edited_at, usize::MAX) {
        return Err(ToolError::Protocol(
            "Messaging open message has invalid time provenance".to_owned(),
        ));
    }

    for reaction in &message.reactions {
        if reaction.emoji.is_empty() {
            return Err(ToolError::Protocol(
                "Messaging open message has an invalid reaction".to_owned(),
            ));
        }
        for participant in &reaction.participants {
            validate_open_participant(participant, "reaction participant")?;
        }
    }

    if message.deleted {
        if !message.content.is_empty()
            || !message.mentions.is_empty()
            || !message.reactions.is_empty()
        {
            return Err(ToolError::Protocol(
                "Messaging open tombstone still carries removed experience data".to_owned(),
            ));
        }
    } else if message.content.is_empty() || message.content.len() > MAX_CONTENT_BYTES {
        return Err(ToolError::Protocol(
            "Messaging open live message has invalid content".to_owned(),
        ));
    }
    Ok(())
}

fn valid_nullable_string(value: &Value, max_bytes: usize) -> bool {
    value.is_null()
        || value
            .as_str()
            .is_some_and(|value| is_bounded_nonempty(value, max_bytes))
}

fn validate_open_participant(
    participant: &OpenParticipantWire,
    role: &str,
) -> Result<(), ToolError> {
    let valid = match participant.kind.as_str() {
        "human" if participant.personality_agent_id.is_none() => participant
            .human_id
            .as_deref()
            .is_some_and(|id| is_bounded_nonempty(id, MAX_MESSAGE_ID_BYTES)),
        "personality_agent" if participant.human_id.is_none() => participant
            .personality_agent_id
            .as_deref()
            .is_some_and(|id| is_bounded_nonempty(id, MAX_MESSAGE_ID_BYTES)),
        _ => false,
    };
    if !valid {
        return Err(ToolError::Protocol(format!(
            "Messaging open contains an invalid {role} participant"
        )));
    }
    Ok(())
}

fn is_bounded_nonempty(value: &str, max_bytes: usize) -> bool {
    !value.is_empty() && value.len() <= max_bytes && !value.chars().any(char::is_control)
}

fn validate_bounded_nonempty(value: &str, max_bytes: usize) -> Result<(), ToolError> {
    if !is_bounded_nonempty(value, max_bytes) {
        return Err(ToolError::InvalidArguments);
    }
    Ok(())
}

const fn urgency_text(urgency: MessagingUrgency) -> &'static str {
    match urgency {
        MessagingUrgency::Urgent => "urgent",
        MessagingUrgency::Normal => "normal",
        MessagingUrgency::Fyi => "fyi",
    }
}

const fn status_text(status: MessagingStatus) -> &'static str {
    match status {
        MessagingStatus::Available => "available",
        MessagingStatus::Busy => "busy",
        MessagingStatus::Away => "away",
    }
}

fn client_nonce(flow_id: &str, call_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(CLIENT_NONCE_DOMAIN);
    digest.update((flow_id.len() as u64).to_be_bytes());
    digest.update(flow_id.as_bytes());
    digest.update((call_id.len() as u64).to_be_bytes());
    digest.update(call_id.as_bytes());
    let digest = digest.finalize();
    let mut nonce = String::with_capacity(4 + digest.len() * 2);
    nonce.push_str("msg-");
    for byte in digest {
        write!(&mut nonce, "{byte:02x}").expect("writing to String cannot fail");
    }
    nonce
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, time::Duration};

    use anyhow::{Result, anyhow};
    use chrono::Utc;
    use serde_json::json;
    use tokio::sync::Mutex as AsyncMutex;
    use tokio_util::sync::CancellationToken;

    use super::*;

    use crate::{
        apiclient::apps::{
            AppInstallationResolutionResult, AppInstallationResolver, ResolvedAppInstallation,
        },
        approval::{
            authority::PolicyDecisionRecord,
            route_broker::{PendingApprovalRequest, provider_review_inputs_for_test},
            route_policy::{
                ElevatedPolicyEvaluation, NormalPolicyDecision, PolicyEvaluation, RoutePolicy,
            },
            route_reviewer::{
                EscalationReviewRequest, ExecutionReviewRequest,
                escalation_provider_wire_bodies_for_test, execution_provider_wire_bodies_for_test,
            },
        },
        provider::types::{ToolCall, ToolInvocationRoute, ValidatedToolArguments},
        store::Redactor,
        tools::{
            BoundExecutionError, BoundToolInvocation, ToolRegistry, ToolRegistryBuilder,
            WorkspacePaths,
        },
    };

    const TEST_WORKSPACE_ID: &str = "0198f0f4-9b72-7000-8000-000000000201";
    const TEST_INSTALLATION_ID: &str = "0198f0f4-9b72-7000-8000-000000000301";
    const TEST_WORKSPACE_B_ID: &str = "0198f0f4-9b72-7000-8000-000000000202";
    const TEST_INSTALLATION_B_ID: &str = "0198f0f4-9b72-7000-8000-000000000302";
    const TEST_WRONG_VARIANT_WORKSPACE_ID: &str = "0198f0f4-9b72-7000-0000-000000000201";
    const TEST_WRONG_VARIANT_INSTALLATION_ID: &str = "0198f0f4-9b72-7000-0000-000000000301";

    fn test_participant() -> Value {
        json!({"kind": "human", "human_id": "human-fixture"})
    }

    fn test_open_message(place_id: &str, seq: u64, deleted: bool) -> Value {
        json!({
            "message_id": format!("m{seq}"),
            "place": {"kind": "channel", "channel_id": place_id},
            "seq": seq,
            "author": test_participant(),
            "content": if deleted { "" } else { "visible" },
            "mentions": [],
            "urgency": "normal",
            "reactions": [],
            "reply_to": null,
            "client_nonce": format!("nonce-{seq}"),
            "created_at": "2026-08-12T00:00:00Z",
            "edited_at": null,
            "deleted": deleted
        })
    }

    fn test_open_response(
        place_id: &str,
        latest_seq: u64,
        last_read_seq: u64,
        first_seq: u64,
        last_seq: u64,
        tombstone_seq: Option<u64>,
    ) -> Value {
        let messages = if first_seq > last_seq {
            Vec::new()
        } else {
            (first_seq..=last_seq)
                .map(|seq| test_open_message(place_id, seq, tombstone_seq == Some(seq)))
                .collect::<Vec<_>>()
        };
        json!({
            "place": {"kind": "channel", "channel_id": place_id},
            "latest_seq": latest_seq,
            "last_read_seq": last_read_seq,
            "members": [],
            "messages": messages
        })
    }

    fn default_test_open_response(request: &OpenMessagingPlaceRequest<'_>) -> Value {
        let latest_seq = 7;
        let last_seq = request
            .before_seq
            .filter(|seq| *seq > 0)
            .map(|seq| latest_seq.min(seq - 1))
            .unwrap_or(latest_seq);
        let limit = u64::from(request.limit.unwrap_or(DEFAULT_OPEN_LIMIT as u16));
        let first_seq = last_seq.saturating_sub(limit.saturating_sub(1)).max(1);
        test_open_response(request.place_id, latest_seq, 5, first_seq, last_seq, None)
    }

    type RecordedStatus = (String, Option<String>, Option<u32>);
    type RecordedReplyLater = (String, String, Option<String>, Option<u32>);

    #[derive(Default)]
    struct FakeMessagingApi {
        calls: AsyncMutex<Vec<String>>,
        scopes: AsyncMutex<Vec<ExactMessagingScope>>,
        scope_resolutions: AsyncMutex<Vec<(String, String)>>,
        resolution_failures: AsyncMutex<VecDeque<AppInstallationResolutionError>>,
        resolved_installation_override: AsyncMutex<Option<String>>,
        reads: AsyncMutex<Vec<(String, u64)>>,
        writes: AsyncMutex<Vec<(String, String, String)>>,
        reacts: AsyncMutex<Vec<(String, String, String)>>,
        statuses: AsyncMutex<Vec<RecordedStatus>>,
        promises: AsyncMutex<Vec<RecordedReplyLater>>,
        resolutions: AsyncMutex<Vec<String>>,
        reply_later_markers: AsyncMutex<Vec<Value>>,
        open_responses: AsyncMutex<VecDeque<Value>>,
        failures: AsyncMutex<VecDeque<&'static str>>,
    }

    impl FakeMessagingApi {
        async fn record_scope(&self, scope: &ExactMessagingScope) {
            self.scopes.lock().await.push(scope.clone());
        }
    }

    #[async_trait]
    impl AppInstallationResolver for FakeMessagingApi {
        async fn resolve_enabled_workspace_app(
            &self,
            request: ResolveEnabledWorkspaceAppRequest<'_>,
        ) -> AppInstallationResolutionResult<ResolvedAppInstallation> {
            self.scope_resolutions
                .lock()
                .await
                .push((request.workspace_id.to_owned(), request.app_id.to_owned()));
            if let Some(error) = self.resolution_failures.lock().await.pop_front() {
                return Err(error);
            }
            if let Some(installation_id) = self.resolved_installation_override.lock().await.clone()
            {
                return Ok(ResolvedAppInstallation {
                    workspace_id: request.workspace_id.to_owned(),
                    installation_id,
                    authority_epoch: "1".to_owned(),
                });
            }
            if request.app_id != MESSAGING_APP_ID {
                return Err(AppInstallationResolutionError::Protocol);
            }
            let installation_id = match request.workspace_id {
                TEST_WORKSPACE_ID => TEST_INSTALLATION_ID,
                TEST_WORKSPACE_B_ID => TEST_INSTALLATION_B_ID,
                _ => return Err(AppInstallationResolutionError::NotFound),
            };
            Ok(ResolvedAppInstallation {
                workspace_id: request.workspace_id.to_owned(),
                installation_id: installation_id.to_owned(),
                authority_epoch: "1".to_owned(),
            })
        }
    }

    #[async_trait]
    impl MessagingApi for FakeMessagingApi {
        async fn overview(&self, scope: &ExactMessagingScope) -> Result<Value> {
            self.record_scope(scope).await;
            self.calls.lock().await.push("overview".to_owned());
            let reply_later_markers = self.reply_later_markers.lock().await.clone();
            Ok(json!({
                "self": {
                    "kind": "personality_agent",
                    "personality_agent_id": "agent-1"
                },
                "channels": [{"channel_id": "general"}],
                "reply_later_markers": reply_later_markers
            }))
        }

        async fn open(
            &self,
            scope: &ExactMessagingScope,
            request: OpenMessagingPlaceRequest<'_>,
        ) -> Result<Value> {
            self.record_scope(scope).await;
            self.calls
                .lock()
                .await
                .push(format!("open:{}", request.place_id));
            if let Some(response) = self.open_responses.lock().await.pop_front() {
                return Ok(response);
            }
            Ok(default_test_open_response(&request))
        }

        async fn write(
            &self,
            scope: &ExactMessagingScope,
            request: WriteMessagingMessageRequest<'_>,
        ) -> Result<Value> {
            self.record_scope(scope).await;
            self.calls
                .lock()
                .await
                .push(format!("write:{}", request.place_id));
            self.writes.lock().await.push((
                request.place_id.to_owned(),
                request.content.to_owned(),
                request.client_nonce.to_owned(),
            ));
            Ok(json!({
                "client_nonce": request.client_nonce,
                "message_id": "m8",
                "seq": 8,
                "created": true
            }))
        }

        async fn react(
            &self,
            scope: &ExactMessagingScope,
            request: ReactMessagingReactionRequest<'_>,
        ) -> Result<Value> {
            self.record_scope(scope).await;
            self.calls
                .lock()
                .await
                .push(format!("react:{}:{}", request.place_id, request.message_id));
            self.reacts.lock().await.push((
                request.place_id.to_owned(),
                request.message_id.to_owned(),
                format!("{}:{}", request.emoji, request.client_nonce),
            ));
            Ok(json!({
                "message": {"message_id": request.message_id,
                            "reactions": [{"emoji": request.emoji, "participants": []}]},
                "reacted": true
            }))
        }

        async fn set_status(
            &self,
            scope: &ExactMessagingScope,
            request: SetMessagingStatusRequest<'_>,
        ) -> Result<Value> {
            self.record_scope(scope).await;
            self.calls
                .lock()
                .await
                .push(format!("status:{}", request.status));
            self.statuses.lock().await.push((
                request.status.to_owned(),
                request.note.map(str::to_owned),
                request.expires_in_minutes,
            ));
            Ok(json!({"status": {"status": request.status, "note": request.note.unwrap_or("")}}))
        }

        async fn reply_later(
            &self,
            scope: &ExactMessagingScope,
            request: CreateMessagingReplyLaterRequest<'_>,
        ) -> Result<Value> {
            self.record_scope(scope).await;
            self.calls
                .lock()
                .await
                .push(format!("reply_later:{}", request.message_id));
            self.promises.lock().await.push((
                request.place_id.to_owned(),
                request.message_id.to_owned(),
                request.note.map(str::to_owned),
                request.remind_in_minutes,
            ));
            let marker = json!({
                "marker_id": "marker-1",
                "participant": {
                    "kind": "personality_agent",
                    "personality_agent_id": "agent-1"
                },
                "place": {"kind": "channel", "channel_id": request.place_id},
                "message_id": request.message_id,
                "note": request.note.unwrap_or(""),
                "remind_at": "2026-08-04T12:00:00Z",
                "resolved": false
            });
            let mut markers = self.reply_later_markers.lock().await;
            markers.retain(|known| known["marker_id"] != marker["marker_id"]);
            markers.push(marker.clone());
            Ok(json!({"marker": marker, "created": true}))
        }

        async fn resolve_reply_later(
            &self,
            scope: &ExactMessagingScope,
            request: ResolveMessagingReplyLaterRequest<'_>,
        ) -> Result<Value> {
            self.record_scope(scope).await;
            self.calls
                .lock()
                .await
                .push(format!("resolve:{}", request.marker_id));
            self.resolutions
                .lock()
                .await
                .push(request.marker_id.to_owned());
            self.reply_later_markers
                .lock()
                .await
                .retain(|marker| marker["marker_id"].as_str() != Some(request.marker_id));
            Ok(json!({"marker": {"marker_id": request.marker_id, "resolved": true}}))
        }

        async fn read_through(
            &self,
            scope: &ExactMessagingScope,
            request: ReadMessagingThroughRequest<'_>,
        ) -> Result<Value> {
            self.record_scope(scope).await;
            if self.failures.lock().await.pop_front() == Some("read") {
                return Err(anyhow!("read failed"));
            }
            self.calls
                .lock()
                .await
                .push(format!("read:{}", request.place_id));
            self.reads
                .lock()
                .await
                .push((request.place_id.to_owned(), request.seq));
            Ok(json!({"last_read_seq": request.seq}))
        }
    }

    async fn execute(
        tool: &MessagingTool,
        action: Value,
        call_id: &str,
    ) -> Result<ToolOutput, ToolError> {
        let args: ValidatedToolArguments =
            serde_json::from_value(with_default_workspace(action)).unwrap();
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        Tool::execute(
            tool,
            ToolCtx {
                flow_id: "flow",
                call_id,
                args: &args,
                cancel: CancellationToken::new(),
                on_update: Arc::new(|_| {}),
                workspace: &workspace,
            },
        )
        .await
    }

    fn tool_call(id: &str, action: Value) -> ToolCall {
        ToolCall {
            id: id.to_owned(),
            name: TOOL_NAME.to_owned(),
            route: ToolInvocationRoute::Normal,
            arguments: serde_json::from_value(with_default_workspace(action))
                .expect("object-shaped arguments"),
        }
    }

    fn with_default_workspace(mut action: Value) -> Value {
        action
            .as_object_mut()
            .expect("Messaging fixture action must be an object")
            .entry("workspace_id")
            .or_insert_with(|| Value::String(TEST_WORKSPACE_ID.to_owned()));
        action
    }

    async fn default_state(
        tool: &MessagingTool,
    ) -> tokio::sync::OwnedMutexGuard<MessagingViewState> {
        tool.view_for(&ExactMessagingScope {
            workspace_id: TEST_WORKSPACE_ID.to_owned(),
            installation_id: TEST_INSTALLATION_ID.to_owned(),
            authority_epoch: "1".to_owned(),
        })
        .await
        .lock_owned()
        .await
    }

    fn scoped_execution(mut arguments: Value) -> Value {
        let object = arguments
            .as_object_mut()
            .expect("bound execution arguments must be an object");
        object.insert(
            "workspace_id".to_owned(),
            Value::String(TEST_WORKSPACE_ID.to_owned()),
        );
        object.insert(
            "installation_id".to_owned(),
            Value::String(TEST_INSTALLATION_ID.to_owned()),
        );
        object.insert("authority_epoch".to_owned(), Value::String("1".to_owned()));
        arguments
    }

    fn scoped_review(mut projection: Value) -> Value {
        projection
            .as_object_mut()
            .expect("review projection must be an object")
            .insert(
                "workspace_id".to_owned(),
                Value::String(TEST_WORKSPACE_ID.to_owned()),
            );
        projection
    }

    fn scoped_resources(mut scopes: Vec<ResourceScope>) -> Vec<ResourceScope> {
        scopes.push(ResourceScope::resource(
            "workspace",
            "workspace",
            TEST_WORKSPACE_ID,
        ));
        scopes.sort();
        scopes
    }

    async fn bind_action(
        registry: &ToolRegistry,
        id: &str,
        action: Value,
    ) -> Result<BoundToolInvocation, DescribeError> {
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");
        let sealed = registry
            .bind(&tool_call(id, action), "flow", &workspace)
            .await?;
        Ok(registry.validate_bound(&sealed)?.clone())
    }

    async fn execute_bound_action(
        registry: &ToolRegistry,
        id: &str,
        action: Value,
    ) -> Result<BoundToolExecutionOutcome, BoundExecutionError> {
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");
        let sealed = registry
            .bind(&tool_call(id, action), "flow", &workspace)
            .await
            .map_err(BoundExecutionError::InvalidInvocation)?;
        let authorized = crate::approval::authority::AuthorizedBoundInvocation::for_test(sealed);
        registry
            .execute_bound(authorized, CancellationToken::new(), Arc::new(|_| {}))
            .await
    }

    async fn binding_fixture() -> (Arc<FakeMessagingApi>, Arc<MessagingTool>, ToolRegistry) {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = Arc::new(MessagingTool::new(api.clone()));
        {
            let scope = ExactMessagingScope {
                workspace_id: TEST_WORKSPACE_ID.to_owned(),
                installation_id: TEST_INSTALLATION_ID.to_owned(),
                authority_epoch: "1".to_owned(),
            };
            let view = tool.view_for(&scope).await;
            let mut state = view.lock().await;
            state.focused_place_id = Some("place-a".to_owned());
            state.visible_messages = vec![
                VisibleMessage {
                    message_id: "message-6".to_owned(),
                    seq: Some(6),
                },
                VisibleMessage {
                    message_id: "message-7".to_owned(),
                    seq: Some(7),
                },
            ];
            state.self_participant = Some(ParticipantIdentity {
                kind: "personality_agent".to_owned(),
                id: "agent-1".to_owned(),
            });
            state
                .visible_reply_later_markers
                .push(VisibleReplyLaterMarker {
                    marker_id: "marker-1".to_owned(),
                    owner: ParticipantIdentity {
                        kind: "personality_agent".to_owned(),
                        id: "agent-1".to_owned(),
                    },
                    place_kind: "channel".to_owned(),
                    place_id: "place-a".to_owned(),
                    message_id: "message-7".to_owned(),
                    note: "after review".to_owned(),
                });
        }
        let mut builder = ToolRegistryBuilder::default();
        builder
            .register(tool.clone())
            .expect("register Messaging binder");
        (api, tool, builder.build())
    }

    #[tokio::test]
    async fn real_bindings_send_exact_human_projection_to_both_reviewers() {
        const INVITE_CODE_SENTINEL: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopq";
        assert_eq!(INVITE_CODE_SENTINEL.chars().count(), 43);

        let (_, tool, registry) = binding_fixture().await;
        let scope = ExactMessagingScope {
            workspace_id: TEST_WORKSPACE_ID.to_owned(),
            installation_id: TEST_INSTALLATION_ID.to_owned(),
            authority_epoch: "1".to_owned(),
        };
        let view = tool.view_for(&scope).await;
        view.lock().await.visible_reply_later_markers[0]
            .note
            .clone_from(&INVITE_CODE_SENTINEL.to_owned());

        let actions = [
            (
                "write-secret",
                json!({"action":"write", "content":INVITE_CODE_SENTINEL}),
            ),
            (
                "status-secret",
                json!({
                    "action":"status",
                    "status":"busy",
                    "note":INVITE_CODE_SENTINEL
                }),
            ),
            (
                "reply-secret",
                json!({
                    "action":"reply_later",
                    "message_id":"message-6",
                    "note":INVITE_CODE_SENTINEL
                }),
            ),
            (
                "react-secret",
                json!({
                    "action":"react",
                    "message_id":"message-7",
                    "emoji":INVITE_CODE_SENTINEL
                }),
            ),
            (
                "resolve-secret",
                json!({
                    "action":"resolve_reply_later",
                    "marker_id":"marker-1"
                }),
            ),
        ];

        for (id, action) in actions {
            let bound = bind_action(&registry, id, action)
                .await
                .expect("real Messaging binding");
            let exact_projection =
                serde_json::to_string(&bound.review_projection).expect("exact Human projection");
            assert!(
                exact_projection.contains(INVITE_CODE_SENTINEL),
                "{id} must preserve exact local Human review content"
            );
            let provider_projection = serde_json::to_string(&bound.provider_review_projection)
                .expect("provider-safe projection");
            assert_eq!(
                provider_projection.matches(INVITE_CODE_SENTINEL).count(),
                0,
                "{id} leaked through the provider-safe projection"
            );

            let human_request = PendingApprovalRequest::from_bound(
                format!("approval-{id}"),
                ToolInvocationRoute::Elevated,
                &bound,
                &Redactor::v1(),
            )
            .expect("Human approval request")
            .public_request();
            let human_encoded =
                serde_json::to_string(&human_request).expect("public Human approval request");
            assert!(
                human_encoded.contains(INVITE_CODE_SENTINEL),
                "{id} Human request lost the exact payload"
            );

            let policy = RoutePolicy::baseline_only_v1();
            let normal_snapshot = match policy.evaluate_normal(&bound, Utc::now()) {
                PolicyEvaluation::Ready {
                    snapshot,
                    decision: NormalPolicyDecision::Unmatched,
                } => snapshot,
                other => panic!("{id} expected Normal/Unmatched, got {other:?}"),
            };
            let (execution_transcript, execution_action, execution_policy) =
                provider_review_inputs_for_test(
                    &bound,
                    &[],
                    ToolInvocationRoute::Normal,
                    PolicyDecisionRecord::Unmatched,
                    &normal_snapshot,
                    &Redactor::v1(),
                )
                .expect("Execution reviewer inputs");
            let execution_request = ExecutionReviewRequest {
                participants: None,
                transcript: execution_transcript,
                action: execution_action,
                policy: execution_policy,
            };

            let elevated_snapshot = match policy.evaluate_elevated(&bound, Utc::now()) {
                ElevatedPolicyEvaluation::Ready { snapshot } => snapshot,
                other => panic!("{id} expected Elevated/Ready, got {other:?}"),
            };
            let (escalation_transcript, escalation_action, escalation_policy) =
                provider_review_inputs_for_test(
                    &bound,
                    &[],
                    ToolInvocationRoute::Elevated,
                    PolicyDecisionRecord::ElevatedPreflight,
                    &elevated_snapshot,
                    &Redactor::v1(),
                )
                .expect("Escalation reviewer inputs");
            let escalation_request = EscalationReviewRequest {
                participants: None,
                transcript: escalation_transcript,
                action: escalation_action,
                policy: escalation_policy,
            };

            let local_digests = [
                bound.proposal_digest.to_hex(),
                bound.descriptor_digest.to_hex(),
                bound
                    .evidence_digest()
                    .expect("local evidence digest")
                    .to_hex(),
            ];
            for (provider, body) in execution_provider_wire_bodies_for_test(execution_request)
                .into_iter()
                .chain(escalation_provider_wire_bodies_for_test(escalation_request))
            {
                let encoded = body.to_string();
                assert!(encoded.contains("provider_evidence_digest"));
                assert!(
                    encoded.contains(INVITE_CODE_SENTINEL),
                    "{id} exact review projection missing from {provider}"
                );
                for digest in &local_digests {
                    assert_eq!(
                        encoded.matches(digest).count(),
                        0,
                        "{id} leaked an exact local digest through {provider}"
                    );
                }
            }
        }
    }

    fn assert_flat_provider_schema(value: &Value) {
        match value {
            Value::Object(object) => {
                for (key, child) in object {
                    assert!(
                        !matches!(
                            key.as_str(),
                            "oneOf"
                                | "anyOf"
                                | "allOf"
                                | "$defs"
                                | "$ref"
                                | "const"
                                | "default"
                                | "format"
                        ),
                        "provider-incompatible schema keyword {key}: {value}"
                    );
                    if key == "type" {
                        assert!(
                            child.is_string(),
                            "provider schema type must be a string, not an array: {child}"
                        );
                    }
                    assert_flat_provider_schema(child);
                }
            }
            Value::Array(values) => {
                for child in values {
                    assert_flat_provider_schema(child);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn provider_schema_is_flat_and_openai_compatible() {
        let tool = MessagingTool::new(Arc::new(FakeMessagingApi::default()));
        let schema = tool.def().parameters;

        assert_flat_provider_schema(&schema);
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["required"], json!(["workspace_id", "action"]));
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["properties"]["workspace_id"]["type"], "string");
        assert!(schema["properties"].get("app_id").is_none());
        assert!(schema["properties"].get("installation_id").is_none());
        assert!(schema["properties"].get("authority_epoch").is_none());
        assert_eq!(
            schema["properties"]["action"]["enum"],
            json!([
                "overview",
                "open",
                "write",
                "react",
                "status",
                "reply_later",
                "resolve_reply_later"
            ])
        );
        assert_eq!(schema["properties"]["before_seq"]["minimum"], 0);
        assert_eq!(schema["properties"]["limit"]["minimum"], 1);
        assert_eq!(schema["properties"]["limit"]["maximum"], 50);
        assert_eq!(
            schema["properties"]["urgency"]["enum"],
            json!(["urgent", "normal", "fyi"])
        );
        assert_eq!(schema["properties"]["seq"]["minimum"], 1);
        assert_eq!(schema["properties"]["emoji"]["type"], "string");
        assert_eq!(schema["properties"]["message_id"]["type"], "string");
        assert_eq!(
            schema["properties"]["status"]["enum"],
            json!(["available", "busy", "away"])
        );
        assert_eq!(schema["properties"]["note"]["type"], "string");
        assert_eq!(schema["properties"]["marker_id"]["type"], "string");
        for field in ["expires_in_minutes", "remind_in_minutes"] {
            assert_eq!(schema["properties"][field]["minimum"], 1);
            assert_eq!(schema["properties"][field]["maximum"], 10080);
        }
        assert_eq!(
            schema["properties"]
                .as_object()
                .expect("properties must be an object")
                .len(),
            16
        );
    }

    #[tokio::test]
    async fn model_must_select_a_workspace_but_cannot_supply_app_or_installation_identity() {
        let (api, _tool, registry) = binding_fixture().await;
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");
        let call = |id: &str, value: Value| ToolCall {
            id: id.to_owned(),
            name: TOOL_NAME.to_owned(),
            route: ToolInvocationRoute::Normal,
            arguments: serde_json::from_value(value).expect("object-shaped arguments"),
        };

        for (id, value) in [
            ("missing", json!({"action": "overview"})),
            (
                "app-authored",
                json!({
                    "workspace_id": TEST_WORKSPACE_ID,
                    "app_id": "messaging",
                    "action": "overview"
                }),
            ),
            (
                "installation-authored",
                json!({
                    "workspace_id": TEST_WORKSPACE_ID,
                    "installation_id": TEST_INSTALLATION_ID,
                    "action": "overview"
                }),
            ),
            (
                "wrong-variant-workspace",
                json!({
                    "workspace_id": TEST_WRONG_VARIANT_WORKSPACE_ID,
                    "action": "overview"
                }),
            ),
        ] {
            let error = match registry.bind(&call(id, value), "flow", &workspace).await {
                Ok(_) => panic!("scope inference and model-authored identities must fail"),
                Err(error) => error,
            };
            assert!(matches!(error, DescribeError::InvalidArguments));
        }
        assert!(
            api.scope_resolutions.lock().await.is_empty(),
            "invalid model proposals must fail before app resolution"
        );
    }

    #[tokio::test]
    async fn bind_resolves_fixed_messaging_identity_once_and_keeps_installation_private() {
        let (api, _tool, registry) = binding_fixture().await;
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");
        let sealed = registry
            .bind(
                &tool_call("status", json!({"action": "status", "status": "available"})),
                "flow",
                &workspace,
            )
            .await
            .expect("bind exact Messaging status");
        let invocation = registry
            .validate_bound(&sealed)
            .expect("live registry validates its seal");
        assert_eq!(
            api.scope_resolutions.lock().await.as_slice(),
            &[(TEST_WORKSPACE_ID.to_owned(), MESSAGING_APP_ID.to_owned())]
        );
        assert!(api.scopes.lock().await.is_empty());
        assert_eq!(
            invocation.review_projection.as_object()["workspace_id"],
            TEST_WORKSPACE_ID
        );
        assert!(
            !invocation
                .review_projection
                .as_object()
                .contains_key("installation_id")
        );
        assert!(
            !invocation
                .review_projection
                .as_object()
                .contains_key("authority_epoch")
        );
        assert!(
            invocation
                .descriptor
                .resource_scopes
                .iter()
                .any(|resource| resource
                    == &ResourceScope::resource("workspace", "workspace", TEST_WORKSPACE_ID,))
        );
        assert!(
            !serde_json::to_string(&invocation.descriptor)
                .unwrap()
                .contains(TEST_INSTALLATION_ID)
        );
        assert!(
            !serde_json::to_string(invocation.review_projection.as_object())
                .unwrap()
                .contains(TEST_INSTALLATION_ID)
        );
        assert_eq!(
            invocation.execution_arguments.as_object()["workspace_id"],
            TEST_WORKSPACE_ID
        );
        assert_eq!(
            invocation.execution_arguments.as_object()["installation_id"],
            TEST_INSTALLATION_ID
        );
        assert_eq!(
            invocation.execution_arguments.as_object()["authority_epoch"],
            "1"
        );

        let authorized = crate::approval::authority::AuthorizedBoundInvocation::for_test(sealed);
        registry
            .execute_bound(authorized, CancellationToken::new(), Arc::new(|_| {}))
            .await
            .expect("execute the privately sealed exact scope");
        assert_eq!(
            api.scope_resolutions.lock().await.len(),
            1,
            "execution must not resolve a newer installation"
        );
        assert_eq!(
            api.scopes.lock().await.as_slice(),
            &[ExactMessagingScope {
                workspace_id: TEST_WORKSPACE_ID.to_owned(),
                installation_id: TEST_INSTALLATION_ID.to_owned(),
                authority_epoch: "1".to_owned(),
            }]
        );
    }

    #[tokio::test]
    async fn bind_exposes_only_domain_preconditions_and_redacts_resolver_failures() {
        enum ExpectedBindingFailure {
            DomainPrecondition,
            Unavailable,
            Internal,
        }

        for (failure, expected) in [
            (
                AppInstallationResolutionError::Forbidden,
                ExpectedBindingFailure::DomainPrecondition,
            ),
            (
                AppInstallationResolutionError::NotFound,
                ExpectedBindingFailure::DomainPrecondition,
            ),
            (
                AppInstallationResolutionError::InstallationNotFound,
                ExpectedBindingFailure::DomainPrecondition,
            ),
            (
                AppInstallationResolutionError::AppDisabled,
                ExpectedBindingFailure::DomainPrecondition,
            ),
            (
                AppInstallationResolutionError::AuthenticationUnavailable,
                ExpectedBindingFailure::Unavailable,
            ),
            (
                AppInstallationResolutionError::ServiceUnavailable,
                ExpectedBindingFailure::Unavailable,
            ),
            (
                AppInstallationResolutionError::TransportUnavailable,
                ExpectedBindingFailure::Unavailable,
            ),
            (
                AppInstallationResolutionError::Protocol,
                ExpectedBindingFailure::Internal,
            ),
        ] {
            let api = Arc::new(FakeMessagingApi::default());
            api.resolution_failures.lock().await.push_back(failure);
            let tool = Arc::new(MessagingTool::new(api.clone()));
            let mut builder = ToolRegistryBuilder::default();
            builder.register(tool).expect("register Messaging");
            let registry = builder.build();
            let error = bind_action(&registry, "resolve-failure", json!({"action": "overview"}))
                .await
                .expect_err("resolver failure must stop binding");
            match expected {
                ExpectedBindingFailure::DomainPrecondition => match error {
                    DescribeError::AppPrecondition { precondition } => {
                        assert_eq!(precondition.code, "enabled_installation_required");
                        assert!(!precondition.message.contains(&failure.to_string()));
                    }
                    other => panic!("domain rejection mapped to {other:?}"),
                },
                ExpectedBindingFailure::Unavailable => {
                    assert_eq!(error, DescribeError::BindingUnavailable)
                }
                ExpectedBindingFailure::Internal => {
                    assert_eq!(error, DescribeError::BindingInternal)
                }
            }
            assert!(api.scopes.lock().await.is_empty());
        }
    }

    #[tokio::test]
    async fn bound_execution_rejects_a_wrong_variant_sealed_installation_id() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());
        let args = BoundExecutionArguments::from_value(json!({
            "workspace_id": TEST_WORKSPACE_ID,
            "installation_id": TEST_WRONG_VARIANT_INSTALLATION_ID,
            "action": "overview"
        }))
        .expect("object-shaped private arguments");
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");
        let result = BoundToolAdapter::execute(
            &tool,
            BoundToolCtx {
                flow_id: "flow",
                call_id: "wrong-variant-installation",
                args: &args,
                committed_effect_permit:
                    crate::approval::authority::CommittedExecutionPermit::executor_fixture(
                        "wrong-variant-installation",
                        ToolInvocationRoute::Normal,
                        crate::approval::authority::ExecutionAuthorityProvenance::AgentOwn,
                    ),
                cancel: CancellationToken::new(),
                on_update: Arc::new(|_| {}),
                workspace: &workspace,
            },
        )
        .await;
        assert!(matches!(result, Err(ToolError::InvalidArguments)));
        assert!(api.scopes.lock().await.is_empty());
        assert!(api.scope_resolutions.lock().await.is_empty());
    }

    #[tokio::test]
    async fn bind_refuses_to_seal_a_wrong_variant_resolved_installation_id() {
        let api = Arc::new(FakeMessagingApi::default());
        *api.resolved_installation_override.lock().await =
            Some(TEST_WRONG_VARIANT_INSTALLATION_ID.to_owned());
        let tool = Arc::new(MessagingTool::new(api.clone()));
        let mut builder = ToolRegistryBuilder::default();
        builder.register(tool).expect("register Messaging");
        let registry = builder.build();
        let error = bind_action(
            &registry,
            "wrong-resolved-installation",
            json!({"action": "overview"}),
        )
        .await
        .expect_err("non-RFC4122 installation identity must never be sealed");
        assert_eq!(error, DescribeError::BindingInternal);
        assert!(api.scopes.lock().await.is_empty());
        assert_eq!(api.scope_resolutions.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn messaging_view_state_is_isolated_by_workspace_and_installation() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = Arc::new(MessagingTool::new(api.clone()));
        let mut builder = ToolRegistryBuilder::default();
        builder.register(tool).expect("register Messaging");
        let registry = builder.build();

        execute_bound_action(
            &registry,
            "open-a",
            json!({
                "workspace_id": TEST_WORKSPACE_ID,
                "action": "open",
                "place_id": "place-a"
            }),
        )
        .await
        .expect("open Workspace A");
        let no_workspace_b_focus = bind_action(
            &registry,
            "write-b-before-open",
            json!({
                "workspace_id": TEST_WORKSPACE_B_ID,
                "action": "write",
                "content": "must not inherit A"
            }),
        )
        .await
        .expect_err("Workspace B cannot inherit Workspace A focus");
        assert!(matches!(
            no_workspace_b_focus,
            DescribeError::AppPrecondition { precondition }
                if precondition.code == "focused_resource_required"
        ));

        execute_bound_action(
            &registry,
            "open-b",
            json!({
                "workspace_id": TEST_WORKSPACE_B_ID,
                "action": "open",
                "place_id": "place-b"
            }),
        )
        .await
        .expect("open Workspace B");
        let bound_a = bind_action(
            &registry,
            "write-a",
            json!({
                "workspace_id": TEST_WORKSPACE_ID,
                "action": "write",
                "content": "A"
            }),
        )
        .await
        .expect("Workspace A retains only its own focus");
        let bound_b = bind_action(
            &registry,
            "write-b",
            json!({
                "workspace_id": TEST_WORKSPACE_B_ID,
                "action": "write",
                "content": "B"
            }),
        )
        .await
        .expect("Workspace B retains only its own focus");
        assert_eq!(
            bound_a.execution_arguments.as_object()["place_id"],
            "place-a"
        );
        assert_eq!(
            bound_b.execution_arguments.as_object()["place_id"],
            "place-b"
        );
        assert_eq!(
            bound_a.execution_arguments.as_object()["installation_id"],
            TEST_INSTALLATION_ID
        );
        assert_eq!(
            bound_b.execution_arguments.as_object()["installation_id"],
            TEST_INSTALLATION_B_ID
        );
    }

    fn churn_scope(index: usize) -> ExactMessagingScope {
        ExactMessagingScope {
            workspace_id: format!("0198f0f4-9b72-7000-8000-{index:012x}"),
            installation_id: format!(
                "0198f0f4-9b72-7000-8000-{:012x}",
                index + MAX_CACHED_MESSAGING_VIEWS + 1
            ),
            authority_epoch: "1".to_owned(),
        }
    }

    #[tokio::test]
    async fn messaging_view_cache_is_bounded_lru_under_installation_churn() {
        let tool = MessagingTool::new(Arc::new(FakeMessagingApi::default()));
        let scopes = (0..=MAX_CACHED_MESSAGING_VIEWS)
            .map(churn_scope)
            .collect::<Vec<_>>();

        for (index, scope) in scopes[..MAX_CACHED_MESSAGING_VIEWS].iter().enumerate() {
            tool.view_for(scope).await.lock().await.focused_place_id =
                Some(format!("place-{index}"));
        }
        // Scope zero becomes most-recently used, so the next insertion must
        // retire scope one without disturbing any other scope's state.
        assert_eq!(
            tool.view_for(&scopes[0])
                .await
                .lock()
                .await
                .focused_place_id
                .as_deref(),
            Some("place-0")
        );
        tool.view_for(&scopes[MAX_CACHED_MESSAGING_VIEWS])
            .await
            .lock()
            .await
            .focused_place_id = Some("newest".to_owned());

        {
            let cache = tool.views.lock().await;
            assert_eq!(cache.entries.len(), MAX_CACHED_MESSAGING_VIEWS);
            assert!(cache.entries.contains_key(&scopes[0]));
            assert!(!cache.entries.contains_key(&scopes[1]));
            assert!(
                cache
                    .entries
                    .contains_key(&scopes[MAX_CACHED_MESSAGING_VIEWS])
            );
        }
        assert!(
            tool.view_for(&scopes[1])
                .await
                .lock()
                .await
                .focused_place_id
                .is_none(),
            "an evicted installation must not inherit another scope's focus"
        );
    }

    #[tokio::test]
    async fn reinstalled_messaging_app_cannot_inherit_the_prior_installation_view() {
        let tool = MessagingTool::new(Arc::new(FakeMessagingApi::default()));
        let prior = ExactMessagingScope {
            workspace_id: TEST_WORKSPACE_ID.to_owned(),
            installation_id: TEST_INSTALLATION_ID.to_owned(),
            authority_epoch: "1".to_owned(),
        };
        let replacement = ExactMessagingScope {
            workspace_id: TEST_WORKSPACE_ID.to_owned(),
            installation_id: TEST_INSTALLATION_B_ID.to_owned(),
            authority_epoch: "1".to_owned(),
        };
        tool.view_for(&prior).await.lock().await.focused_place_id = Some("prior-place".to_owned());
        assert!(
            tool.view_for(&replacement)
                .await
                .lock()
                .await
                .focused_place_id
                .is_none()
        );
        tool.view_for(&replacement)
            .await
            .lock()
            .await
            .focused_place_id = Some("replacement-place".to_owned());
        assert_eq!(
            tool.view_for(&prior)
                .await
                .lock()
                .await
                .focused_place_id
                .as_deref(),
            Some("prior-place")
        );
    }

    #[tokio::test]
    async fn authority_epoch_rollover_cannot_inherit_the_prior_installation_view() {
        let tool = MessagingTool::new(Arc::new(FakeMessagingApi::default()));
        let epoch_one = ExactMessagingScope {
            workspace_id: TEST_WORKSPACE_ID.to_owned(),
            installation_id: TEST_INSTALLATION_ID.to_owned(),
            authority_epoch: "1".to_owned(),
        };
        let epoch_two = ExactMessagingScope {
            authority_epoch: "2".to_owned(),
            ..epoch_one.clone()
        };
        tool.view_for(&epoch_one)
            .await
            .lock()
            .await
            .focused_place_id = Some("epoch-one".to_owned());
        assert!(
            tool.view_for(&epoch_two)
                .await
                .lock()
                .await
                .focused_place_id
                .is_none()
        );
    }

    #[tokio::test]
    async fn bound_execution_rejects_noncanonical_sealed_authority_epochs() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");
        for authority_epoch in ["", "0", "01", "+1", "9223372036854775808"] {
            let args = BoundExecutionArguments::from_value(json!({
                "workspace_id": TEST_WORKSPACE_ID,
                "installation_id": TEST_INSTALLATION_ID,
                "authority_epoch": authority_epoch,
                "action": "overview"
            }))
            .expect("object-shaped private arguments");
            let result = BoundToolAdapter::execute(
                &tool,
                BoundToolCtx {
                    flow_id: "flow",
                    call_id: "malformed-epoch",
                    args: &args,
                    committed_effect_permit:
                        crate::approval::authority::CommittedExecutionPermit::executor_fixture(
                            "malformed-epoch",
                            ToolInvocationRoute::Normal,
                            crate::approval::authority::ExecutionAuthorityProvenance::AgentOwn,
                        ),
                    cancel: CancellationToken::new(),
                    on_update: Arc::new(|_| {}),
                    workspace: &workspace,
                },
            )
            .await;
            assert!(matches!(result, Err(ToolError::InvalidArguments)));
        }
        assert!(api.scopes.lock().await.is_empty());
    }

    #[tokio::test]
    async fn lru_eviction_does_not_invalidate_an_in_flight_read_through_hook() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());
        let scope = churn_scope(0);
        let view = tool.view_for(&scope).await;
        record_pending_read_through(&mut *view.lock().await, "place-a", 7);
        let hook = read_through_post_commit(
            api.clone(),
            scope.clone(),
            view.clone(),
            "place-a".to_owned(),
            7,
        );

        for index in 1..=MAX_CACHED_MESSAGING_VIEWS {
            tool.view_for(&churn_scope(index)).await;
        }
        assert!(
            !tool.views.lock().await.entries.contains_key(&scope),
            "oldest scope should leave the bounded cache even while its hook owns the view"
        );

        assert!(matches!(
            hook.invoke_after_result_commit(CancellationToken::new())
                .await,
            LiveAppPostCommitOutcome::Applied
        ));
        assert!(view.lock().await.pending_read_through.is_empty());
        assert_eq!(
            api.scopes.lock().await.as_slice(),
            std::slice::from_ref(&scope)
        );
        let replacement = tool.view_for(&scope).await;
        assert!(!Arc::ptr_eq(&view, &replacement));
        assert!(replacement.lock().await.pending_read_through.is_empty());
    }

    #[test]
    fn all_messaging_actions_still_deserialize() {
        let overview: MessagingAction =
            serde_json::from_value(json!({"action": "overview"})).unwrap();
        assert!(matches!(overview, MessagingAction::Overview {}));

        let open: MessagingAction = serde_json::from_value(json!({
            "action": "open",
            "place_id": "general",
            "before_seq": 0,
            "limit": 50
        }))
        .unwrap();
        assert!(matches!(
            open,
            MessagingAction::Open {
                place_id,
                before_seq: Some(0),
                limit: Some(50)
            } if place_id == "general"
        ));

        let write: MessagingAction = serde_json::from_value(json!({
            "action": "write",
            "content": "hello",
            "urgency": "fyi",
            "reply_to": "message-1"
        }))
        .unwrap();
        assert!(matches!(
            write,
            MessagingAction::Write {
                content,
                urgency: MessagingUrgency::Fyi,
                reply_to: Some(reply_to)
            } if content == "hello" && reply_to == "message-1"
        ));

        let react: MessagingAction = serde_json::from_value(json!({
            "action": "react",
            "seq": 7,
            "emoji": "👍"
        }))
        .unwrap();
        assert!(matches!(
            react,
            MessagingAction::React {
                message_id: None,
                seq: Some(7),
                emoji
            } if emoji == "👍"
        ));

        let status: MessagingAction = serde_json::from_value(json!({
            "action": "status",
            "status": "busy",
            "note": "取り込み中",
            "expires_in_minutes": 45
        }))
        .unwrap();
        assert!(matches!(
            status,
            MessagingAction::Status {
                status: MessagingStatus::Busy,
                note: Some(note),
                expires_in_minutes: Some(45)
            } if note == "取り込み中"
        ));

        let reply_later: MessagingAction = serde_json::from_value(json!({
            "action": "reply_later",
            "message_id": "message-1",
            "remind_in_minutes": 30
        }))
        .unwrap();
        assert!(matches!(
            reply_later,
            MessagingAction::ReplyLater {
                message_id: Some(message_id),
                seq: None,
                note: None,
                remind_in_minutes: Some(30)
            } if message_id == "message-1"
        ));

        let resolve: MessagingAction = serde_json::from_value(json!({
            "action": "resolve_reply_later",
            "marker_id": "marker-1"
        }))
        .unwrap();
        assert!(matches!(
            resolve,
            MessagingAction::ResolveReplyLater { marker_id } if marker_id == "marker-1"
        ));
    }

    #[tokio::test]
    async fn all_messaging_actions_bind_to_exact_app_owned_operations_without_side_effects() {
        let (api, _tool, registry) = binding_fixture().await;

        let overview = bind_action(&registry, "overview", json!({"action": "overview"}))
            .await
            .expect("bind overview");
        assert_eq!(overview.adapter.id, BINDING_ADAPTER_ID);
        assert_eq!(overview.adapter.version, BINDING_ADAPTER_VERSION);
        assert_eq!(overview.descriptor.operation, "overview");
        assert_eq!(overview.descriptor.capability, CapabilityClass::Read);
        assert_eq!(
            overview.descriptor.resource_scopes,
            scoped_resources(vec![ResourceScope::collection("messaging", "place")])
        );
        assert_eq!(
            Value::Object(overview.execution_arguments.as_object().clone()),
            scoped_execution(json!({"action": "overview"}))
        );

        let open = bind_action(
            &registry,
            "open",
            json!({
                "action": "open",
                "place_id": "place-b",
                "before_seq": 8,
                "limit": 25
            }),
        )
        .await
        .expect("bind open");
        assert_eq!(open.descriptor.operation, "open");
        assert_eq!(open.descriptor.capability, CapabilityClass::Read);
        assert_eq!(
            Value::Object(open.execution_arguments.as_object().clone()),
            scoped_execution(json!({
                "action": "open",
                "place_id": "place-b",
                "before_seq": 8,
                "limit": 25
            }))
        );
        assert_eq!(
            open.descriptor.resource_scopes,
            scoped_resources(vec![ResourceScope::resource(
                "messaging",
                "place",
                "place-b"
            )])
        );

        let write = bind_action(
            &registry,
            "write",
            json!({
                "action": "write",
                "content": "hello",
                "urgency": "urgent",
                "reply_to": "message-6"
            }),
        )
        .await
        .expect("bind write");
        assert_eq!(write.descriptor.operation, "write");
        assert_eq!(write.descriptor.capability, CapabilityClass::Mutate);
        assert_eq!(
            Value::Object(write.review_projection.as_object().clone()),
            scoped_review(json!({
                "action": "write",
                "place_id": "place-a",
                "urgency": "urgent",
                "content": "hello",
                "content_bytes": 5,
                "content_characters": 5,
                "reply_to": "message-6"
            }))
        );
        assert_eq!(write.review_projection.as_object()["content"], "hello");
        assert_eq!(
            Value::Object(write.execution_arguments.as_object().clone()),
            scoped_execution(json!({
                "action": "write",
                "place_id": "place-a",
                "content": "hello",
                "urgency": "urgent",
                "reply_to": "message-6"
            }))
        );
        assert!(
            write
                .descriptor
                .resource_scopes
                .contains(&ResourceScope::resource("messaging", "place", "place-a"))
        );
        assert!(
            write
                .descriptor
                .resource_scopes
                .contains(&ResourceScope::resource(
                    "messaging",
                    "message",
                    "message-6"
                ))
        );

        let react = bind_action(
            &registry,
            "react",
            json!({"action": "react", "seq": 7, "emoji": "👍"}),
        )
        .await
        .expect("bind react by visible seq");
        assert_eq!(react.descriptor.operation, "react");
        assert_eq!(react.descriptor.capability, CapabilityClass::Mutate);
        assert_eq!(
            Value::Object(react.execution_arguments.as_object().clone()),
            scoped_execution(json!({
                "action": "react",
                "place_id": "place-a",
                "message_id": "message-7",
                "emoji": "👍"
            }))
        );
        assert!(!react.execution_arguments.as_object().contains_key("seq"));

        let status = bind_action(
            &registry,
            "status",
            json!({
                "action": "status",
                "status": "busy",
                "note": "deep work",
                "expires_in_minutes": 30
            }),
        )
        .await
        .expect("bind status");
        assert_eq!(status.descriptor.operation, "status");
        assert_eq!(status.review_projection.as_object()["note"], "deep work");
        assert_eq!(status.review_projection.as_object()["has_note"], true);
        assert_eq!(status.review_projection.as_object()["note_characters"], 9);
        assert_eq!(
            status.descriptor.resource_scopes,
            scoped_resources(vec![ResourceScope::resource(
                "messaging",
                "participant",
                "self",
            )])
        );
        assert_eq!(
            Value::Object(status.execution_arguments.as_object().clone()),
            scoped_execution(json!({
                "action": "status",
                "status": "busy",
                "note": "deep work",
                "expires_in_minutes": 30
            }))
        );

        let reply_later = bind_action(
            &registry,
            "reply-later",
            json!({
                "action": "reply_later",
                "message_id": "message-6",
                "note": "after review",
                "remind_in_minutes": 45
            }),
        )
        .await
        .expect("bind reply later");
        assert_eq!(reply_later.descriptor.operation, "reply_later");
        assert_eq!(
            reply_later.review_projection.as_object()["note"],
            "after review"
        );
        assert_eq!(reply_later.review_projection.as_object()["has_note"], true);
        assert_eq!(
            Value::Object(reply_later.execution_arguments.as_object().clone()),
            scoped_execution(json!({
                "action": "reply_later",
                "place_id": "place-a",
                "message_id": "message-6",
                "note": "after review",
                "remind_in_minutes": 45
            }))
        );

        let resolve = bind_action(
            &registry,
            "resolve",
            json!({
                "action": "resolve_reply_later",
                "marker_id": "marker-1"
            }),
        )
        .await
        .expect("bind resolve reply later");
        assert_eq!(resolve.descriptor.operation, "resolve_reply_later");
        assert_eq!(
            Value::Object(resolve.review_projection.as_object().clone()),
            scoped_review(json!({
                "action": "resolve_reply_later",
                "marker_id": "marker-1",
                "marker_meaning": "own_reply_later_promise",
                "place_kind": "channel",
                "place_id": "place-a",
                "message_id": "message-7",
                "note": "after review",
                "has_note": true,
                "note_characters": 12
            }))
        );
        assert!(
            resolve
                .descriptor
                .resource_scopes
                .contains(&ResourceScope::resource("messaging", "participant", "self"))
        );
        assert!(
            resolve
                .descriptor
                .resource_scopes
                .contains(&ResourceScope::resource(
                    "messaging",
                    "reply_later_marker",
                    "marker-1"
                ))
        );
        assert_eq!(
            Value::Object(resolve.execution_arguments.as_object().clone()),
            scoped_execution(json!({
                "action": "resolve_reply_later",
                "marker_id": "marker-1"
            }))
        );

        assert!(api.calls.lock().await.is_empty());
        assert!(api.reads.lock().await.is_empty());
        assert!(api.writes.lock().await.is_empty());
        assert!(api.reacts.lock().await.is_empty());
        assert!(api.statuses.lock().await.is_empty());
        assert!(api.promises.lock().await.is_empty());
        assert!(api.resolutions.lock().await.is_empty());
    }

    #[tokio::test]
    async fn bound_write_keeps_place_a_after_the_view_focuses_place_b() {
        let (api, tool, registry) = binding_fixture().await;
        let proposal = json!({"action": "write", "content": "hello"});
        let bound_a = bind_action(&registry, "write-a", proposal.clone())
            .await
            .expect("bind write to place A");

        {
            let mut state = default_state(&tool).await;
            state.focused_place_id = Some("place-b".to_owned());
            state.visible_messages.clear();
        }

        let bound_b = bind_action(&registry, "write-b", proposal)
            .await
            .expect("bind write to place B");
        assert_eq!(
            bound_a.execution_arguments.as_object()["place_id"],
            "place-a"
        );
        assert_eq!(
            bound_b.execution_arguments.as_object()["place_id"],
            "place-b"
        );
        assert_eq!(bound_a.proposal_digest, bound_b.proposal_digest);
        assert_ne!(bound_a.descriptor_digest, bound_b.descriptor_digest);
        assert!(api.calls.lock().await.is_empty());
    }

    #[tokio::test]
    async fn unseen_or_not_owned_reply_later_markers_fail_before_review() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = Arc::new(MessagingTool::new(api.clone()));
        {
            let mut state = default_state(&tool).await;
            state.self_participant = Some(ParticipantIdentity {
                kind: "personality_agent".to_owned(),
                id: "agent-1".to_owned(),
            });
            state
                .visible_reply_later_markers
                .push(VisibleReplyLaterMarker {
                    marker_id: "marker-other".to_owned(),
                    owner: ParticipantIdentity {
                        kind: "personality_agent".to_owned(),
                        id: "agent-2".to_owned(),
                    },
                    place_kind: "channel".to_owned(),
                    place_id: "place-a".to_owned(),
                    message_id: "message-7".to_owned(),
                    note: String::new(),
                });
        }
        let mut builder = ToolRegistryBuilder::default();
        builder.register(tool).expect("register Messaging");
        let registry = builder.build();

        for marker_id in ["marker-unseen", "marker-other"] {
            let error = bind_action(
                &registry,
                marker_id,
                json!({
                    "action": "resolve_reply_later",
                    "marker_id": marker_id
                }),
            )
            .await
            .expect_err("only a known marker owned by self may reach review");
            assert!(matches!(
                error,
                DescribeError::AppPrecondition { precondition }
                    if precondition.code == "visible_owned_marker_required"
            ));
        }
        assert!(api.calls.lock().await.is_empty());
    }

    #[tokio::test]
    async fn raw_and_bound_paths_share_one_exact_executor_for_all_seven_actions() {
        let cases = [
            ("overview", json!({"action": "overview"})),
            (
                "open",
                json!({
                    "action": "open",
                    "place_id": "place-b",
                    "before_seq": 5,
                    "limit": 20
                }),
            ),
            (
                "write",
                json!({
                    "action": "write",
                    "content": "hello",
                    "urgency": "urgent",
                    "reply_to": "message-6"
                }),
            ),
            ("react", json!({"action": "react", "seq": 7, "emoji": "👍"})),
            (
                "status",
                json!({
                    "action": "status",
                    "status": "busy",
                    "note": "deep work",
                    "expires_in_minutes": 30
                }),
            ),
            (
                "reply-later",
                json!({
                    "action": "reply_later",
                    "message_id": "message-6",
                    "note": "after review",
                    "remind_in_minutes": 45
                }),
            ),
            (
                "resolve",
                json!({
                    "action": "resolve_reply_later",
                    "marker_id": "marker-1"
                }),
            ),
        ];

        for (id, action) in cases {
            let (raw_api, raw_tool, _raw_registry) = binding_fixture().await;
            let raw_output = execute(raw_tool.as_ref(), action.clone(), id)
                .await
                .expect("raw exact operation");

            let (bound_api, _bound_tool, bound_registry) = binding_fixture().await;
            let bound_outcome = execute_bound_action(&bound_registry, id, action)
                .await
                .expect("bound exact operation");

            assert_eq!(bound_outcome.output, raw_output, "action {id}");
            assert_eq!(
                *bound_api.calls.lock().await,
                *raw_api.calls.lock().await,
                "action {id} must issue the same exact app request"
            );
        }
    }

    #[tokio::test]
    async fn executing_a_bound_write_uses_place_a_without_rebinding_current_focus() {
        let (api, tool, registry) = binding_fixture().await;
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");
        let sealed = registry
            .bind(
                &tool_call("write-a", json!({"action": "write", "content": "hello"})),
                "flow",
                &workspace,
            )
            .await
            .expect("bind write to place A");

        {
            let mut state = default_state(&tool).await;
            state.focused_place_id = Some("place-b".to_owned());
            state.visible_messages.clear();
        }

        let authorized = crate::approval::authority::AuthorizedBoundInvocation::for_test(sealed);
        let outcome = registry
            .execute_bound(authorized, CancellationToken::new(), Arc::new(|_| {}))
            .await
            .expect("execute bound write");
        assert!(outcome.live_post_commit.is_none());
        assert_eq!(
            api.writes.lock().await.as_slice(),
            &[(
                ("place-a").to_owned(),
                "hello".to_owned(),
                client_nonce("flow", "write-a")
            )]
        );
        let state = default_state(&tool).await;
        assert_eq!(state.focused_place_id.as_deref(), Some("place-b"));
        assert!(state.visible_messages.is_empty());
    }

    #[tokio::test]
    async fn bound_permit_waits_through_view_lock_and_cancel_prevents_the_local_effect() {
        let (api, tool, registry) = binding_fixture().await;
        let workspace = WorkspacePaths::new("/workspace").expect("workspace path");
        let sealed = registry
            .bind(
                &tool_call(
                    "write-cancelled-while-locked",
                    json!({"action": "write", "content": "must not send"}),
                ),
                "flow",
                &workspace,
            )
            .await
            .expect("bind exact write before taking the execution lock");
        let authorized = crate::approval::authority::AuthorizedBoundInvocation::for_test(sealed);
        let cancel = CancellationToken::new();
        let state_guard = default_state(&tool).await;
        let execution = registry.execute_bound(authorized, cancel.clone(), Arc::new(|_| {}));
        tokio::pin!(execution);

        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut execution)
                .await
                .is_err(),
            "bound execution must retain its permit while waiting for the view lock"
        );
        assert!(api.calls.lock().await.is_empty());

        cancel.cancel();
        drop(state_guard);
        let error = match execution.await {
            Err(error) => error,
            Ok(_) => panic!("cancellation after the lock wait must prevent the local effect"),
        };
        assert!(matches!(
            error,
            BoundExecutionError::Tool(ToolError::Cancelled)
        ));
        assert!(api.calls.lock().await.is_empty());
        assert!(api.writes.lock().await.is_empty());
    }

    #[tokio::test]
    async fn bound_open_has_no_hidden_overview_or_pre_action_cursor_flush() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = Arc::new(MessagingTool::new(api.clone()));
        default_state(&tool)
            .await
            .pending_read_through
            .insert("old-place".to_owned(), 4);
        let mut builder = ToolRegistryBuilder::default();
        builder.register(tool.clone()).expect("register Messaging");
        let registry = builder.build();

        let outcome = execute_bound_action(
            &registry,
            "open",
            json!({"action": "open", "place_id": "general"}),
        )
        .await
        .expect("execute exact bound open");
        assert!(!outcome.output.is_error);
        assert_eq!(api.calls.lock().await.as_slice(), &["open:general"]);
        assert!(api.reads.lock().await.is_empty());
        {
            let state = default_state(&tool).await;
            assert_eq!(state.pending_read_through.get("old-place"), Some(&4));
            assert_eq!(state.pending_read_through.get("general"), Some(&7));
        }

        let hook = outcome
            .live_post_commit
            .expect("open returns maintenance eligible after result commit");
        assert!(matches!(
            hook.invoke_after_result_commit(CancellationToken::new())
                .await,
            LiveAppPostCommitOutcome::Applied
        ));
        assert_eq!(
            api.reads.lock().await.as_slice(),
            &[("general".to_owned(), 7)]
        );
        let state = default_state(&tool).await;
        assert_eq!(state.pending_read_through.get("old-place"), Some(&4));
        assert!(!state.pending_read_through.contains_key("general"));
    }

    #[tokio::test]
    async fn bound_open_admits_only_contiguous_pages_after_result_commit() {
        let api = Arc::new(FakeMessagingApi::default());
        api.open_responses.lock().await.extend([
            test_open_response("general", 25, 0, 16, 25, None),
            test_open_response("general", 25, 0, 6, 15, None),
            test_open_response("general", 25, 0, 1, 5, Some(3)),
            test_open_response("general", 25, 5, 6, 15, None),
            test_open_response("general", 25, 15, 16, 25, None),
        ]);
        let tool = Arc::new(MessagingTool::new(api.clone()));
        let mut builder = ToolRegistryBuilder::default();
        builder.register(tool.clone()).expect("register Messaging");
        let registry = builder.build();

        let latest_gap = execute_bound_action(
            &registry,
            "latest-gap",
            json!({"action": "open", "place_id": "general", "limit": 10}),
        )
        .await
        .expect("open latest page");
        assert_eq!(latest_gap.output.details["last_read_seq"], 0);
        assert_eq!(latest_gap.output.details["latest_seq"], 25);
        assert_eq!(latest_gap.output.details["messages"][0]["seq"], 16);
        assert!(latest_gap.live_post_commit.is_none());
        assert!(api.reads.lock().await.is_empty());
        assert!(
            !default_state(&tool)
                .await
                .pending_read_through
                .contains_key("general")
        );

        let middle_gap = execute_bound_action(
            &registry,
            "middle-gap",
            json!({
                "action": "open", "place_id": "general", "before_seq": 16, "limit": 10
            }),
        )
        .await
        .expect("page backward across another gap");
        assert!(middle_gap.live_post_commit.is_none());
        assert!(api.reads.lock().await.is_empty());

        let oldest = execute_bound_action(
            &registry,
            "oldest",
            json!({
                "action": "open", "place_id": "general", "before_seq": 6, "limit": 10
            }),
        )
        .await
        .expect("open oldest contiguous page");
        assert!(
            api.reads.lock().await.is_empty(),
            "open precedes durable admission"
        );
        assert!(matches!(
            oldest
                .live_post_commit
                .expect("1..5 is the first contiguous prefix")
                .invoke_after_result_commit(CancellationToken::new())
                .await,
            LiveAppPostCommitOutcome::Applied
        ));
        assert_eq!(
            api.reads.lock().await.as_slice(),
            &[("general".to_owned(), 5)]
        );

        for (call_id, action, want_seq) in [
            (
                "middle-contiguous",
                json!({
                    "action": "open", "place_id": "general", "before_seq": 16, "limit": 10
                }),
                15,
            ),
            (
                "latest-contiguous",
                json!({"action": "open", "place_id": "general", "limit": 10}),
                25,
            ),
        ] {
            let outcome = execute_bound_action(&registry, call_id, action)
                .await
                .expect("open next contiguous page");
            assert!(matches!(
                outcome
                    .live_post_commit
                    .expect("contiguous page returns post-result maintenance")
                    .invoke_after_result_commit(CancellationToken::new())
                    .await,
                LiveAppPostCommitOutcome::Applied
            ));
            assert_eq!(
                api.reads.lock().await.last().map(|(_, seq)| *seq),
                Some(want_seq)
            );
        }
        assert_eq!(
            api.reads.lock().await.as_slice(),
            &[
                ("general".to_owned(), 5),
                ("general".to_owned(), 15),
                ("general".to_owned(), 25)
            ]
        );
    }

    #[test]
    fn strict_open_wire_rejects_malformed_pages() {
        let valid = || test_open_response("general", 2, 0, 1, 2, None);
        let mut wrong_inner_place = valid();
        wrong_inner_place["messages"][0]["place"]["channel_id"] = json!("other");
        let mut incomplete_row = valid();
        incomplete_row["messages"][0] = json!({"message_id": "m1", "seq": 1});
        let mut missing_nullable_field = valid();
        missing_nullable_field["messages"][0]
            .as_object_mut()
            .expect("message object")
            .remove("reply_to");
        let mut missing_provenance = valid();
        missing_provenance["messages"][0]
            .as_object_mut()
            .expect("message object")
            .remove("client_nonce");
        let mut invalid_author = valid();
        invalid_author["messages"][0]["author"]["human_id"] = json!("");
        let mut duplicate_seq = valid();
        duplicate_seq["messages"][1]["seq"] = json!(1);
        let mut duplicate_message_id = valid();
        duplicate_message_id["messages"][1]["message_id"] = json!("m1");
        let mut reversed = valid();
        reversed["messages"]
            .as_array_mut()
            .expect("messages array")
            .swap(0, 1);
        let mut seq_zero = valid();
        seq_zero["messages"][0]["seq"] = json!(0);
        let mut invalid_tombstone = valid();
        invalid_tombstone["messages"][0]["deleted"] = json!(true);
        let mut gap = test_open_response("general", 3, 0, 1, 3, None);
        gap["messages"].as_array_mut().expect("messages").remove(1);
        let mut last_read_beyond_latest = valid();
        last_read_beyond_latest["last_read_seq"] = json!(3);
        let mut latest_behind_page = valid();
        latest_behind_page["latest_seq"] = json!(1);

        let cases = [
            (
                "wrong-outer-place",
                test_open_response("other", 2, 0, 1, 2, None),
                None,
                None,
            ),
            ("wrong-inner-place", wrong_inner_place, None, None),
            ("incomplete-row", incomplete_row, None, None),
            ("missing-nullable-field", missing_nullable_field, None, None),
            ("missing-provenance", missing_provenance, None, None),
            ("invalid-author", invalid_author, None, None),
            ("duplicate-seq", duplicate_seq, None, None),
            ("duplicate-message-id", duplicate_message_id, None, None),
            ("reverse-order", reversed, None, None),
            ("seq-zero", seq_zero, None, None),
            ("at-before-seq", valid(), Some(2), Some(10)),
            ("too-many-for-limit", valid(), None, Some(1)),
            ("gap", gap, None, None),
            ("invalid-tombstone", invalid_tombstone, None, None),
            (
                "last-read-beyond-latest",
                last_read_beyond_latest,
                None,
                None,
            ),
            ("latest-behind-page", latest_behind_page, None, None),
        ];

        for (id, response, before_seq, limit) in cases {
            assert!(
                matches!(
                    validate_open_response(&response, "general", before_seq, limit),
                    Err(ToolError::Protocol(_))
                ),
                "case {id}: malformed open wire must fail closed"
            );
        }
    }

    #[tokio::test]
    async fn invalid_open_wire_cannot_change_view_or_manufacture_read_evidence() {
        let api = Arc::new(FakeMessagingApi::default());
        api.open_responses
            .lock()
            .await
            .push_back(test_open_response("other", 2, 0, 1, 2, None));
        let tool = Arc::new(MessagingTool::new(api.clone()));
        {
            let mut state = default_state(&tool).await;
            state.focused_place_id = Some("old-place".to_owned());
            state.visible_messages = vec![VisibleMessage {
                message_id: "old-message".to_owned(),
                seq: Some(9),
            }];
            state.pending_read_through.insert("old-place".to_owned(), 9);
        }
        let expected_state = default_state(&tool).await.clone();
        let mut builder = ToolRegistryBuilder::default();
        builder.register(tool.clone()).expect("register Messaging");
        let registry = builder.build();

        let error = match execute_bound_action(
            &registry,
            "invalid-open",
            json!({"action": "open", "place_id": "general"}),
        )
        .await
        {
            Err(error) => error,
            Ok(_) => panic!("wrong-place open wire must fail before admission"),
        };
        assert!(matches!(
            error,
            BoundExecutionError::Tool(ToolError::Protocol(_))
        ));
        assert_eq!(*default_state(&tool).await, expected_state);
        assert!(api.reads.lock().await.is_empty());
    }

    #[tokio::test]
    async fn valid_older_page_admits_overlap_and_complete_tombstone() {
        let api = Arc::new(FakeMessagingApi::default());
        api.open_responses
            .lock()
            .await
            .push_back(test_open_response("general", 10, 2, 1, 5, Some(3)));
        let tool = Arc::new(MessagingTool::new(api.clone()));
        let mut builder = ToolRegistryBuilder::default();
        builder.register(tool.clone()).expect("register Messaging");
        let registry = builder.build();

        let outcome = execute_bound_action(
            &registry,
            "valid-overlap",
            json!({
                "action": "open", "place_id": "general", "before_seq": 6, "limit": 10
            }),
        )
        .await
        .expect("complete dense page is valid");
        assert_eq!(outcome.output.details["last_read_seq"], 2);
        assert_eq!(outcome.output.details["messages"][2]["seq"], 3);
        assert_eq!(outcome.output.details["messages"][2]["deleted"], true);
        assert_eq!(outcome.output.details["messages"][2]["content"], "");
        assert!(api.reads.lock().await.is_empty());
        {
            let state = default_state(&tool).await;
            assert_eq!(state.focused_place_id.as_deref(), Some("general"));
            assert_eq!(
                state
                    .visible_messages
                    .iter()
                    .map(|message| message.seq.expect("validated seq"))
                    .collect::<Vec<_>>(),
                vec![1, 2, 3, 4, 5]
            );
            assert_eq!(state.pending_read_through.get("general"), Some(&5));
        }
        assert!(matches!(
            outcome
                .live_post_commit
                .expect("2-overlap then 3..5 is contiguous")
                .invoke_after_result_commit(CancellationToken::new())
                .await,
            LiveAppPostCommitOutcome::Applied
        ));
        assert_eq!(
            api.reads.lock().await.as_slice(),
            &[("general".to_owned(), 5)]
        );
    }

    #[tokio::test]
    async fn failed_live_read_is_deferred_without_rewriting_the_tool_result() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = Arc::new(MessagingTool::new(api.clone()));
        let mut builder = ToolRegistryBuilder::default();
        builder.register(tool.clone()).expect("register Messaging");
        let registry = builder.build();

        let outcome = execute_bound_action(
            &registry,
            "open",
            json!({"action": "open", "place_id": "general"}),
        )
        .await
        .expect("the exact open result succeeds before maintenance");
        assert_eq!(outcome.output.details["latest_seq"], 7);
        api.failures.lock().await.push_back("read");
        let deferred = outcome
            .live_post_commit
            .expect("open returns a live maintenance hook")
            .invoke_after_result_commit(CancellationToken::new())
            .await;
        assert!(matches!(
            deferred,
            LiveAppPostCommitOutcome::Deferred(ToolError::Rpc(message))
                if message == "read failed"
        ));
        assert_eq!(
            default_state(&tool)
                .await
                .pending_read_through
                .get("general"),
            Some(&7)
        );

        api.failures.lock().await.push_back("read");
        execute(tool.as_ref(), json!({"action": "overview"}), "later-one")
            .await
            .expect("a failed retry must not fail an unrelated action");
        assert_eq!(
            default_state(&tool)
                .await
                .pending_read_through
                .get("general"),
            Some(&7)
        );

        execute(
            tool.as_ref(),
            json!({"action": "status", "status": "available"}),
            "later-two",
        )
        .await
        .expect("a later opportunistic retry may apply the pending cursor");
        assert!(default_state(&tool).await.pending_read_through.is_empty());
        assert_eq!(
            api.calls.lock().await.as_slice(),
            &[
                "open:general",
                "overview",
                "read:general",
                "status:available"
            ]
        );
    }

    #[test]
    fn pending_read_cursors_are_per_place_monotonic_maxima() {
        let mut state = MessagingViewState::default();
        record_pending_read_through(&mut state, "place-a", 7);
        record_pending_read_through(&mut state, "place-b", 4);
        record_pending_read_through(&mut state, "place-a", 3);
        assert_eq!(state.pending_read_through.get("place-a"), Some(&7));
        assert_eq!(state.pending_read_through.get("place-b"), Some(&4));

        clear_pending_read_through(&mut state, "place-a", 6);
        assert_eq!(state.pending_read_through.get("place-a"), Some(&7));
        clear_pending_read_through(&mut state, "place-a", 7);
        assert!(!state.pending_read_through.contains_key("place-a"));
        assert_eq!(state.pending_read_through.get("place-b"), Some(&4));
    }

    #[tokio::test]
    async fn recreated_adapter_safely_loses_only_process_local_pending_reads() {
        let api = Arc::new(FakeMessagingApi::default());
        let old_tool = Arc::new(MessagingTool::new(api.clone()));
        let mut old_builder = ToolRegistryBuilder::default();
        old_builder
            .register(old_tool.clone())
            .expect("register old Messaging adapter");
        let old_registry = old_builder.build();
        let old_outcome = execute_bound_action(
            &old_registry,
            "old-open",
            json!({"action": "open", "place_id": "general"}),
        )
        .await
        .expect("old open");
        assert_eq!(
            default_state(&old_tool)
                .await
                .pending_read_through
                .get("general"),
            Some(&7)
        );
        drop(old_outcome);
        drop(old_registry);
        drop(old_tool);

        let recreated = Arc::new(MessagingTool::new(api));
        assert!(
            default_state(&recreated)
                .await
                .pending_read_through
                .is_empty()
        );
        let mut recreated_builder = ToolRegistryBuilder::default();
        recreated_builder
            .register(recreated.clone())
            .expect("register recreated Messaging adapter");
        let recreated_registry = recreated_builder.build();
        let recomputed = execute_bound_action(
            &recreated_registry,
            "recreated-open",
            json!({"action": "open", "place_id": "general"}),
        )
        .await
        .expect("the next exact open safely recomputes the cursor");
        assert!(recomputed.live_post_commit.is_some());
        assert_eq!(
            default_state(&recreated)
                .await
                .pending_read_through
                .get("general"),
            Some(&7)
        );
    }

    #[tokio::test]
    async fn binding_reports_typed_view_preconditions_and_invalid_arguments() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = Arc::new(MessagingTool::new(api.clone()));
        let mut builder = ToolRegistryBuilder::default();
        builder.register(tool.clone()).expect("register Messaging");
        let registry = builder.build();

        let no_focus = bind_action(
            &registry,
            "write",
            json!({"action": "write", "content": "hello"}),
        )
        .await
        .expect_err("write must require a focused place");
        assert!(matches!(
            no_focus,
            DescribeError::AppPrecondition { precondition }
                if precondition.code == "focused_resource_required"
        ));

        default_state(&tool).await.focused_place_id = Some("place-a".to_owned());
        let invisible = bind_action(
            &registry,
            "react",
            json!({"action": "react", "seq": 7, "emoji": "👍"}),
        )
        .await
        .expect_err("reaction target must be visible");
        assert!(matches!(
            invisible,
            DescribeError::AppPrecondition { precondition }
                if precondition.code == "visible_target_required"
        ));

        let invalid = bind_action(
            &registry,
            "invalid",
            json!({
                "action": "react",
                "message_id": "message-7",
                "seq": 7,
                "emoji": "👍"
            }),
        )
        .await
        .expect_err("two visible selectors are invalid");
        assert_eq!(invalid, DescribeError::InvalidArguments);
        assert!(api.calls.lock().await.is_empty());
    }

    #[tokio::test]
    async fn raw_open_without_a_later_messaging_call_stays_safe_side_unread() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        execute(
            &tool,
            json!({"action": "open", "place_id": "general"}),
            "open",
        )
        .await
        .unwrap();
        assert!(api.reads.lock().await.is_empty());
        assert_eq!(
            default_state(&tool)
                .await
                .pending_read_through
                .get("general"),
            Some(&7)
        );
        assert_eq!(api.calls.lock().await.as_slice(), &["open:general"]);
    }

    #[tokio::test]
    async fn later_raw_messaging_call_retries_the_previous_open_cursor() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        execute(
            &tool,
            json!({"action": "open", "place_id": "general"}),
            "open",
        )
        .await
        .unwrap();

        // In production the runner's durable ToolResult receipt sits between
        // these two calls. This unit test freezes Messaging's side of that
        // migration seam without pretending raw ToolOutput carries the proof.
        execute(&tool, json!({"action": "overview"}), "overview")
            .await
            .unwrap();
        assert_eq!(
            api.reads.lock().await.as_slice(),
            &[("general".to_owned(), 7)]
        );
        assert_eq!(
            api.calls.lock().await.as_slice(),
            &["open:general", "read:general", "overview"]
        );
    }

    #[tokio::test]
    async fn write_targets_only_the_open_place_with_retry_stable_nonce() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());
        let error = execute(&tool, json!({"action": "write", "content": "hi"}), "write")
            .await
            .unwrap_err();
        assert!(matches!(error, ToolError::Protocol(_)));

        execute(
            &tool,
            json!({"action": "open", "place_id": "general"}),
            "open",
        )
        .await
        .unwrap();
        execute(&tool, json!({"action": "write", "content": "hi"}), "write")
            .await
            .unwrap();
        execute(&tool, json!({"action": "write", "content": "hi"}), "write")
            .await
            .unwrap();
        execute(
            &tool,
            json!({"action": "write", "content": "hi"}),
            "write-again",
        )
        .await
        .unwrap();
        let writes = api.writes.lock().await;
        assert_eq!(writes[0].0, "general");
        assert_eq!(writes[0].1, "hi");
        assert_eq!(writes[0].2, client_nonce("flow", "write"));
        assert_eq!(writes[1].2, writes[0].2);
        assert_eq!(writes[2].2, client_nonce("flow", "write-again"));
        assert_ne!(writes[2].2, writes[0].2);
    }

    #[tokio::test]
    async fn failed_delayed_read_is_preserved_without_blocking_later_actions() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());
        execute(
            &tool,
            json!({"action": "open", "place_id": "general"}),
            "open",
        )
        .await
        .unwrap();
        api.failures.lock().await.push_back("read");
        execute(&tool, json!({"action": "overview"}), "one")
            .await
            .expect("maintenance failure must not fail the unrelated action");
        assert_eq!(
            default_state(&tool)
                .await
                .pending_read_through
                .get("general"),
            Some(&7)
        );
        execute(&tool, json!({"action": "overview"}), "two")
            .await
            .unwrap();
        assert_eq!(api.reads.lock().await.len(), 1);
        assert!(default_state(&tool).await.pending_read_through.is_empty());
    }

    #[tokio::test]
    async fn react_requires_an_open_place() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());
        let error = execute(
            &tool,
            json!({"action": "react", "seq": 7, "emoji": "👍"}),
            "react",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::Protocol(_)));
        assert!(api.reacts.lock().await.is_empty());
    }

    #[tokio::test]
    async fn react_targets_only_messages_visible_on_the_open_screen() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());
        execute(
            &tool,
            json!({"action": "open", "place_id": "general"}),
            "open",
        )
        .await
        .unwrap();

        // By seq and by message_id, both against the visible screen.
        execute(
            &tool,
            json!({"action": "react", "seq": 7, "emoji": "👍"}),
            "r1",
        )
        .await
        .unwrap();
        execute(
            &tool,
            json!({"action": "react", "message_id": "m6", "emoji": "🎉"}),
            "r2",
        )
        .await
        .unwrap();
        assert_eq!(
            api.reacts.lock().await.as_slice(),
            &[
                (
                    "general".to_owned(),
                    "m7".to_owned(),
                    format!("👍:{}", client_nonce("flow", "r1"))
                ),
                (
                    "general".to_owned(),
                    "m6".to_owned(),
                    format!("🎉:{}", client_nonce("flow", "r2"))
                ),
            ]
        );

        // A message that is not on the screen cannot be reacted to, whether
        // addressed by id or by seq (ADR 0011 §3).
        let error = execute(
            &tool,
            json!({"action": "react", "message_id": "m404", "emoji": "👍"}),
            "r3",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::Protocol(_)));
        let error = execute(
            &tool,
            json!({"action": "react", "seq": 99, "emoji": "👍"}),
            "r4",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::Protocol(_)));
        assert_eq!(api.reacts.lock().await.len(), 2);
    }

    #[tokio::test]
    async fn react_rejects_ambiguous_or_malformed_selectors() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());
        for arguments in [
            json!({"action": "react", "emoji": "👍"}),
            json!({"action": "react", "message_id": "m7", "seq": 7, "emoji": "👍"}),
            json!({"action": "react", "seq": 0, "emoji": "👍"}),
            json!({"action": "react", "seq": 7, "emoji": ""}),
            json!({"action": "react", "seq": 7, "emoji": "a b"}),
        ] {
            let error = execute(&tool, arguments, "invalid").await.unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        assert!(api.reacts.lock().await.is_empty());
    }

    #[tokio::test]
    async fn status_is_declared_without_opening_a_place() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        // Attention state is about the person, not about a screen, so unlike
        // write/react/reply_later it needs no place in view.
        execute(
            &tool,
            json!({"action": "status", "status": "busy", "note": "別の対応中です",
                   "expires_in_minutes": 45}),
            "status",
        )
        .await
        .unwrap();
        assert_eq!(
            api.statuses.lock().await.as_slice(),
            &[(
                "busy".to_owned(),
                Some("別の対応中です".to_owned()),
                Some(45)
            )]
        );

        // Omitted fields stay off the wire so the server applies its own
        // defaults rather than receiving a null it must interpret.
        execute(
            &tool,
            json!({"action": "status", "status": "available"}),
            "status-2",
        )
        .await
        .unwrap();
        assert_eq!(
            api.statuses.lock().await[1],
            ("available".to_owned(), None, None)
        );

        for arguments in [
            json!({"action": "status"}),
            json!({"action": "status", "status": "invisible"}),
            json!({"action": "status", "status": "busy", "expires_in_minutes": 0}),
            json!({"action": "status", "status": "busy", "expires_in_minutes": 10081}),
        ] {
            let error = execute(&tool, arguments, "invalid").await.unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        assert_eq!(api.statuses.lock().await.len(), 2);
    }

    /// The server counts characters. Checking bytes here would both reject
    /// legal multibyte notes and let a 201 character ASCII note through to a
    /// server 400, so the two checks are stated in the same unit.
    #[tokio::test]
    async fn notes_are_bounded_by_characters_like_the_server() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        for note in ["a".repeat(200), "あ".repeat(200)] {
            execute(
                &tool,
                json!({"action": "status", "status": "busy", "note": note}),
                "status-note",
            )
            .await
            .unwrap();
        }
        let error = execute(
            &tool,
            json!({"action": "status", "status": "busy", "note": "a".repeat(201)}),
            "status-note-too-long",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::InvalidArguments));
        assert_eq!(api.statuses.lock().await.len(), 2);

        execute(
            &tool,
            json!({"action": "open", "place_id": "general"}),
            "open",
        )
        .await
        .unwrap();
        for note in ["a".repeat(500), "あ".repeat(500)] {
            execute(
                &tool,
                json!({"action": "reply_later", "seq": 7, "note": note}),
                "promise-note",
            )
            .await
            .unwrap();
        }
        let error = execute(
            &tool,
            json!({"action": "reply_later", "seq": 7, "note": "a".repeat(501)}),
            "promise-note-too-long",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::InvalidArguments));
        assert_eq!(api.promises.lock().await.len(), 2);
    }

    #[tokio::test]
    async fn reply_later_targets_only_messages_visible_on_the_open_screen() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        let error = execute(
            &tool,
            json!({"action": "reply_later", "seq": 7}),
            "no-place",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::Protocol(_)));

        execute(
            &tool,
            json!({"action": "open", "place_id": "general"}),
            "open",
        )
        .await
        .unwrap();
        execute(
            &tool,
            json!({"action": "reply_later", "seq": 7, "note": "後で必ず返します",
                   "remind_in_minutes": 60}),
            "promise",
        )
        .await
        .unwrap();
        assert_eq!(
            api.promises.lock().await.as_slice(),
            &[(
                "general".to_owned(),
                "m7".to_owned(),
                Some("後で必ず返します".to_owned()),
                Some(60)
            )]
        );

        // A message the open place does not show cannot be promised a reply,
        // exactly as it cannot be reacted to.
        let error = execute(
            &tool,
            json!({"action": "reply_later", "message_id": "m404"}),
            "missing",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::Protocol(_)));

        for arguments in [
            json!({"action": "reply_later"}),
            json!({"action": "reply_later", "message_id": "m7", "seq": 7}),
            json!({"action": "reply_later", "seq": 0}),
            json!({"action": "reply_later", "seq": 7, "remind_in_minutes": 0}),
            json!({"action": "reply_later", "seq": 7, "remind_in_minutes": 10081}),
        ] {
            let error = execute(&tool, arguments, "invalid").await.unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        assert_eq!(api.promises.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn resolving_a_promise_needs_only_its_marker() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        execute(
            &tool,
            json!({"action": "open", "place_id": "general"}),
            "open",
        )
        .await
        .unwrap();
        execute(
            &tool,
            json!({"action": "reply_later", "seq": 7, "note": "later"}),
            "promise",
        )
        .await
        .unwrap();
        default_state(&tool).await.focused_place_id = None;

        // Like the human's reply-later list, keeping a known own promise is
        // reachable from anywhere — the place it was made in need not be open.
        execute(
            &tool,
            json!({"action": "resolve_reply_later", "marker_id": "marker-1"}),
            "resolve",
        )
        .await
        .unwrap();
        assert_eq!(
            api.resolutions.lock().await.as_slice(),
            &["marker-1".to_owned()]
        );

        for arguments in [
            json!({"action": "resolve_reply_later"}),
            json!({"action": "resolve_reply_later", "marker_id": ""}),
        ] {
            let error = execute(&tool, arguments, "invalid").await.unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        assert_eq!(api.resolutions.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn overview_preserves_a_reply_later_marker_for_bound_resolve() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = Arc::new(MessagingTool::new(api.clone()));
        let mut builder = ToolRegistryBuilder::default();
        builder.register(tool).expect("register Messaging");
        let registry = builder.build();

        execute_bound_action(
            &registry,
            "open",
            json!({"action": "open", "place_id": "general"}),
        )
        .await
        .expect("open the target message");
        execute_bound_action(
            &registry,
            "promise",
            json!({"action": "reply_later", "seq": 7, "note": "later"}),
        )
        .await
        .expect("create reply-later marker");
        execute_bound_action(&registry, "overview", json!({"action": "overview"}))
            .await
            .expect("refresh overview without losing the marker");
        execute_bound_action(
            &registry,
            "resolve",
            json!({"action": "resolve_reply_later", "marker_id": "marker-1"}),
        )
        .await
        .expect("bind and resolve the marker admitted by overview");

        assert_eq!(
            api.calls.lock().await.as_slice(),
            &[
                "open:general",
                "reply_later:m7",
                "overview",
                "resolve:marker-1"
            ]
        );
        assert!(api.reply_later_markers.lock().await.is_empty());
    }

    #[tokio::test]
    async fn recreated_adapter_recovers_a_reply_later_marker_from_overview() {
        let api = Arc::new(FakeMessagingApi::default());
        let old_tool = Arc::new(MessagingTool::new(api.clone()));
        let mut old_builder = ToolRegistryBuilder::default();
        old_builder
            .register(old_tool.clone())
            .expect("register old Messaging adapter");
        let old_registry = old_builder.build();

        execute_bound_action(
            &old_registry,
            "old-open",
            json!({"action": "open", "place_id": "general"}),
        )
        .await
        .expect("old adapter opens the target message");
        execute_bound_action(
            &old_registry,
            "old-promise",
            json!({"action": "reply_later", "seq": 7, "note": "after restart"}),
        )
        .await
        .expect("old adapter creates the durable marker");
        drop(old_registry);
        drop(old_tool);

        let recreated = Arc::new(MessagingTool::new(api.clone()));
        let mut recreated_builder = ToolRegistryBuilder::default();
        recreated_builder
            .register(recreated)
            .expect("register recreated Messaging adapter");
        let recreated_registry = recreated_builder.build();
        execute_bound_action(
            &recreated_registry,
            "recreated-overview",
            json!({"action": "overview"}),
        )
        .await
        .expect("overview reconstructs durable reply-later state");
        execute_bound_action(
            &recreated_registry,
            "recreated-resolve",
            json!({"action": "resolve_reply_later", "marker_id": "marker-1"}),
        )
        .await
        .expect("recreated adapter binds and resolves the recovered marker");

        assert_eq!(
            api.resolutions.lock().await.as_slice(),
            &["marker-1".to_owned()]
        );
        assert!(api.reply_later_markers.lock().await.is_empty());
    }

    #[tokio::test]
    async fn own_written_message_is_immediately_reactable() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());
        execute(
            &tool,
            json!({"action": "open", "place_id": "general"}),
            "open",
        )
        .await
        .unwrap();
        execute(
            &tool,
            json!({"action": "write", "content": "追記"}),
            "write",
        )
        .await
        .unwrap();
        execute(
            &tool,
            json!({"action": "react", "seq": 8, "emoji": "✅"}),
            "react",
        )
        .await
        .unwrap();
        assert_eq!(
            api.reacts.lock().await.as_slice(),
            &[(
                "general".to_owned(),
                "m8".to_owned(),
                format!("✅:{}", client_nonce("flow", "react"))
            )]
        );
    }
}
