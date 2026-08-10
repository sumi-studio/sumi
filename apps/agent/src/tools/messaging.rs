//! One stateful messaging view for a PersonalityAgent.
//!
//! This is deliberately not a bag of stateless REST verbs.  The agent sees an
//! overview, opens one place, and writes in the place it currently has open.
//! The view is a tool owned by the continuing person; it is not another agent
//! or another life-log Session.

use std::{fmt::Write as _, sync::Arc};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::{
    apiclient::messaging::{
        CreateMessagingChannelRequest, CreateMessagingPollRequest,
        CreateMessagingReplyLaterRequest, CreateMessagingRoleRequest, CreateMessagingThreadRequest,
        DeleteMessagingRoleRequest, DuplicateMessagingChannelRequest, GetMessagingCallStateRequest,
        ListMessagingRolesRequest, ListMessagingThreadsRequest, MessagingApi,
        MessagingNotificationPlace, MessagingNotificationSettingsRequest, MessagingParticipant,
        OpenMessagingPlaceRequest, PollMessagingAttentionRequest, ReactMessagingReactionRequest,
        ReadMessagingThroughRequest, ResolveMessagingReplyLaterRequest, SearchMessagingRequest,
        SetMessagingChannelTopicRequest, SetMessagingMemberRolesRequest,
        SetMessagingProfileRequest, SetMessagingStatusRequest, StartMessagingDMRequest,
        UpdateMessagingChannelRequest, UpdateMessagingRoleRequest, VoteMessagingPollRequest,
        WriteMessagingMessageRequest,
    },
    provider::types::{ToolDefinition, UserContent},
    tools::{Tool, ToolCtx, ToolError, ToolOutput, ToolRisk},
};

const TOOL_NAME: &str = "messaging";
const CLIENT_NONCE_DOMAIN: &[u8] = b"sumi-messaging-tool-v1";
const MAX_PLACE_ID_BYTES: usize = 256;
const MAX_CONTENT_BYTES: usize = 64 * 1024;
const MAX_REPLY_ID_BYTES: usize = 256;
const MAX_MESSAGE_ID_BYTES: usize = 256;
const MAX_MARKER_ID_BYTES: usize = 256;
const MAX_PARTICIPANT_ID_BYTES: usize = 256;
const MAX_WORKSPACE_ID_BYTES: usize = 256;
// The server bounds a channel name at 200 characters and a topic at 1000
// bytes; four bytes per character covers any UTF-8 within the name bound.
const MAX_CHANNEL_NAME_BYTES: usize = 800;
const MAX_TOPIC_BYTES: usize = 1000;
// A group dm the agent opens in one gesture. Far beyond any real conversation,
// tight enough that a malformed argument cannot fan out.
const MAX_DM_PARTICIPANTS: usize = 32;
// The server bounds a thread name at 100 characters; four bytes per character
// covers any UTF-8 within that limit.
const MAX_THREAD_NAME_BYTES: usize = 400;
// The server bounds a poll question at 500 characters and an option at 200.
const MAX_POLL_QUESTION_BYTES: usize = 2000;
const MAX_POLL_OPTION_BYTES: usize = 800;
const MIN_POLL_OPTIONS: usize = 2;
const MAX_POLL_OPTIONS: usize = 10;
// The server bounds emoji at 32 characters; 128 bytes covers any such UTF-8.
const MAX_EMOJI_BYTES: usize = 128;
// The server bounds these notes at 200 and 500 characters; four bytes per
// character covers any UTF-8 within those limits.
const MAX_STATUS_NOTE_BYTES: usize = 800;
// The server bounds a display name at 80 characters and a tagline at 100;
// four bytes per character covers any UTF-8 within those limits.
const MAX_DISPLAY_NAME_BYTES: usize = 320;
const MAX_TAGLINE_BYTES: usize = 400;
const MAX_REPLY_LATER_NOTE_BYTES: usize = 2000;
// The server bounds a role name at 60 characters; four bytes per character
// covers any UTF-8 within that bound.
const MAX_ROLE_NAME_BYTES: usize = 240;
const MAX_ROLE_ID_BYTES: usize = 256;
// A workspace with more roles than this is not a thing one call should build.
const MAX_ROLE_IDS: usize = 32;
// A week, matching the server's bound on relative durations.
const MAX_RELATIVE_MINUTES: u32 = 7 * 24 * 60;
// The server bounds a search phrase at 200 bytes and one poll at 50 candidates.
const MAX_SEARCH_QUERY_BYTES: usize = 200;
const MAX_SEARCH_LIMIT: u16 = 50;
const MAX_ATTENTION_LIMIT: u16 = 50;
// A keyword list is a handful of words one wants to be called for, not a
// search index: the server bounds it at 32 words of 64 characters each.
const MAX_NOTIFICATION_KEYWORDS: usize = 32;
const MAX_NOTIFICATION_KEYWORD_BYTES: usize = 64 * 4;
const MAX_NOTIFICATION_PLACES: usize = 200;

/// The closed set of permissions a role may carry, matching the server's.  A
/// name outside it is refused here rather than silently dropped later: an
/// agent that asked for a permission and was quietly given a role without it
/// would believe something untrue about the workspace.
const ROLE_PERMISSIONS: [&str; 4] = [
    "manage_channels",
    "manage_roles",
    "manage_members",
    "mention_all",
];

#[derive(Debug, Deserialize)]
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
    /// With `expires_in_minutes` the state is temporary and lapses back to
    /// whatever was declared before it, so「1時間だけ取り込み中」does not have to
    /// be undone by hand.
    Status {
        status: MessagingStatus,
        #[serde(default)]
        note: Option<String>,
        #[serde(default)]
        expires_in_minutes: Option<u32>,
    },
    /// Read or change one's own名乗り — the display name and the one-line
    /// description others see next to it.  Like status this is about the
    /// person, not a place, so no view need be open.  Sending no field reads
    /// the current profile.
    Profile {
        #[serde(default)]
        display_name: Option<String>,
        #[serde(default)]
        tagline: Option<String>,
    },
    /// See who may do what here: the roles, who holds them, and one's own
    /// permissions.  Open to any member — knowing whom to ask is not itself a
    /// privilege.
    Roles {
        #[serde(default)]
        workspace_id: Option<String>,
    },
    /// Administer roles.  These four exist because the human settings screen
    /// has them: an operation that lived only in the UI would make this
    /// participant a lesser one.  The boundary is the permission, not the
    /// tool — without `manage_roles` (or `manage_members` for assign_roles)
    /// the server refuses exactly as it refuses a Human without it.
    CreateRole {
        #[serde(default)]
        workspace_id: Option<String>,
        role_name: String,
        #[serde(default)]
        role_color: Option<String>,
        #[serde(default)]
        permissions: Option<Vec<String>>,
    },
    /// Replace a role's name, colour and permissions.  Whole replacement, not
    /// a patch: the permissions named are the ones the role ends up holding.
    UpdateRole {
        #[serde(default)]
        workspace_id: Option<String>,
        role_id: String,
        role_name: String,
        #[serde(default)]
        role_color: Option<String>,
        #[serde(default)]
        permissions: Option<Vec<String>>,
    },
    /// Remove a role, withdrawing it from everyone holding it.
    DeleteRole {
        #[serde(default)]
        workspace_id: Option<String>,
        role_id: String,
    },
    /// Replace the roles one participant holds.  The member is named the way
    /// every participant is named — there is one member list, not a people
    /// list and a bot list.  An empty role_ids returns them to plain
    /// membership.
    AssignRoles {
        #[serde(default)]
        workspace_id: Option<String>,
        member_kind: MessagingMemberKind,
        member_id: String,
        role_ids: Vec<String>,
    },
    /// Rewrite the topic of the place currently open in this view — the one
    /// line at the top of the screen saying what the channel is for.  Like
    /// write, it acts on what is in view; unlike write, it needs
    /// `manage_channels`.
    SetTopic { topic: String },
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
    /// Mark one's own earlier promise as kept.  Like the human's reply-later
    /// list this is reachable from anywhere, not only from the place.
    ResolveReplyLater { marker_id: String },
    /// Open a direct conversation with one person (a dm) or several (a group
    /// dm), exactly like the human sidebar's「ダイレクトメッセージを開始」.
    /// The new place becomes the one in view, as it does for a human who is
    /// taken into the conversation they just opened.
    StartDm {
        participants: Vec<MessagingParticipant>,
    },
    /// Open a channel in the workspace, as the sidebar's「チャンネルを作成」
    /// does.  The new channel becomes the place in view.
    CreateChannel {
        name: String,
        #[serde(default)]
        topic: Option<String>,
        #[serde(default)]
        workspace_id: Option<String>,
        /// A voice channel is a place people are meant to talk in (ADR 0012).
        #[serde(default)]
        voice: Option<bool>,
    },
    /// Rename a channel, retopic it, or both.  An omitted field is left alone.
    UpdateChannel {
        place_id: String,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        topic: Option<String>,
    },
    /// Copy a channel's name and topic into a new, empty channel.  The copy
    /// carries no messages: it is a fresh place shaped like the original.
    DuplicateChannel {
        place_id: String,
        #[serde(default)]
        name: Option<String>,
    },
    /// Find messages one can already see.  Like status this is not about a
    /// place: remembering something said elsewhere should not require having
    /// guessed the place first.
    Search {
        query: String,
        #[serde(default)]
        place_id: Option<String>,
        #[serde(default)]
        limit: Option<u16>,
    },
    /// Read or change one's own notification setting — the identical resource
    /// a Human owns and changes from the UI.  With no field set this reads;
    /// any field present changes only that field, because naming one
    /// preference must not silently discard the rest.
    NotificationSettings {
        #[serde(default)]
        defaults_level: Option<MessagingNotifyLevel>,
        #[serde(default)]
        per_place: Option<Vec<MessagingNotifyPlace>>,
        #[serde(default)]
        keywords: Option<Vec<String>>,
    },
    /// Take in one's own AttentionCandidates: what arrived while one was not
    /// looking, and why.  `consume_through` acknowledges everything up to that
    /// candidate_seq before the rest is listed.
    ///
    /// This is a provisional wiring ahead of the wake-trigger design (ADR 0010
    /// / issue #173).  There, a candidate's arrival wakes the person; here the
    /// already-awake person comes to look.  What is durable either way is that
    /// nothing said while the runtime was stopped is lost.
    Attention {
        #[serde(default)]
        consume_through: Option<u64>,
        #[serde(default)]
        limit: Option<u16>,
    },
    /// See who is currently in a call (ADR 0012).  Like status this is about
    /// people rather than a screen, so no place need be open; naming one
    /// narrows the answer.  There is no action for joining a call: the ADR
    /// records the agent's own participation as an open question.
    GetCallState {
        #[serde(default)]
        place_id: Option<String>,
    },
    /// See the side conversations under the place currently in view.
    Threads {},
    /// Open a side conversation under the place currently in view, optionally
    /// growing it out of a message visible there.
    CreateThread {
        name: String,
        #[serde(default)]
        message_id: Option<String>,
        #[serde(default)]
        seq: Option<u64>,
    },
    /// Ask the open place a question everyone can answer.
    CreatePoll {
        question: String,
        options: Vec<String>,
        #[serde(default)]
        allow_multi: bool,
        #[serde(default)]
        content: Option<String>,
        #[serde(default)]
        closes_in_minutes: Option<u32>,
    },
    /// Answer a poll visible in the open place.  Restating the whole choice is
    /// the only way to change it; an empty list withdraws the vote.
    VotePoll {
        #[serde(default)]
        message_id: Option<String>,
        #[serde(default)]
        seq: Option<u64>,
        option_ids: Vec<String>,
    },
}

/// The three notification levels, identical to the ones a Human chooses in the
/// UI (契約ドラフト: HumanもAgentも同じ形).
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MessagingNotifyLevel {
    All,
    Mentions,
    Mute,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MessagingNotifyPlace {
    place_id: String,
    level: MessagingNotifyLevel,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MessagingUrgency {
    Urgent,
    #[default]
    Normal,
    Fyi,
}

/// The three self-declared states.  There is no "offline" or "active": nothing
/// here is observed, all of it is said.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MessagingStatus {
    Available,
    Busy,
    Away,
}

/// The two kinds of participant.  A PersonalityAgent is addressed exactly like
/// a Human, which is why role administration needs no second vocabulary.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MessagingMemberKind {
    Human,
    PersonalityAgent,
}

fn member_kind_text(kind: MessagingMemberKind) -> &'static str {
    match kind {
        MessagingMemberKind::Human => "human",
        MessagingMemberKind::PersonalityAgent => "personality_agent",
    }
}

/// One message currently on this view's screen. Reactions may only target
/// these (ADR 0011 §3: 見えていないものは操作できない — like a human, the
/// agent reacts to what the open place shows, never to an unseen permalink).
#[derive(Clone)]
struct VisibleMessage {
    message_id: String,
    seq: Option<u64>,
}

#[derive(Default)]
struct MessagingViewState {
    initialized: bool,
    focused_place_id: Option<String>,
    pending_read_through: Option<(String, u64)>,
    visible_messages: Vec<VisibleMessage>,
}

pub(crate) struct MessagingTool {
    api: Arc<dyn MessagingApi>,
    view: Mutex<MessagingViewState>,
}

impl MessagingTool {
    pub(crate) fn new(api: Arc<dyn MessagingApi>) -> Self {
        Self {
            api,
            view: Mutex::new(MessagingViewState::default()),
        }
    }

    async fn flush_admitted_read(
        &self,
        state: &mut MessagingViewState,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<(), ToolError> {
        let Some((place_id, seq)) = state.pending_read_through.clone() else {
            return Ok(());
        };
        let request = ReadMessagingThroughRequest {
            place_id: &place_id,
            seq,
        };
        let result = tokio::select! {
            _ = cancel.cancelled() => return Err(ToolError::Cancelled),
            result = self.api.read_through(request) => result,
        };
        result.map_err(|error| ToolError::Rpc(error.to_string()))?;
        state.pending_read_through = None;
        Ok(())
    }
}

fn messaging_parameters_schema() -> Value {
    serde_json::json!({
        "type": "object",
        "description": concat!(
            "Choose one messaging action and include only the fields used by that action. ",
            "overview needs no other fields; open requires place_id and may include before_seq ",
            "or limit; write requires content and may include urgency or reply_to; react ",
            "requires emoji plus exactly one of message_id or seq; reply_later requires exactly ",
            "one of message_id or seq and may include note or remind_in_minutes; status requires ",
            "status and may include note or expires_in_minutes; profile may include display_name ",
            "or tagline and reads the current profile when neither is given; ",
            "resolve_reply_later requires ",
            "marker_id; start_dm requires participants; create_channel requires name and may ",
            "include topic or workspace_id; update_channel requires place_id plus name, topic or ",
            "both; duplicate_channel requires place_id and may include name; search requires ",
            "query and may include place_id or limit; notification_settings takes any of ",
            "defaults_level, per_place or keywords and reads the current setting when given ",
            "none of them; attention may include consume_through or limit; get_call_state takes ",
            "an optional place_id; ",
            "roles takes no required field and may include workspace_id; ",
            "create_role requires role_name and may include role_color or permissions; ",
            "update_role requires role_id and role_name and may include role_color or ",
            "permissions; delete_role requires role_id; assign_roles requires member_kind, ",
            "member_id and role_ids. Every role action may include workspace_id. ",
            "set_topic requires topic; ",
            "write, react, reply_later and set_topic act on the place most recently opened in ",
            "this tool view; every other action needs no open place."
        ),
        "properties": {
            "action": {
                "type": "string",
                "enum": [
                    "overview", "open", "write", "react",
                    "status", "profile", "reply_later", "resolve_reply_later", "start_dm",
                    "create_channel", "update_channel", "duplicate_channel",
                    "search", "notification_settings", "attention", "get_call_state",
                    "threads", "create_thread", "create_poll", "vote_poll", "roles",
                    "create_role", "update_role", "delete_role", "assign_roles", "set_topic"
                ],
                "description": concat!(
                    "Action to perform: overview lists available places and unread state; open ",
                    "shows one place and focuses it for later writes; write sends a message to ",
                    "the currently open place; react toggles an emoji reaction on a message ",
                    "visible in the currently open place; reply_later promises a later reply to ",
                    "such a message so others see it and you are reminded; status declares your ",
                    "own availability; profile reads or changes your own display name and the ",
                    "one-line description shown next to it; resolve_reply_later marks one of ",
                    "your promises as kept; ",
                    "start_dm opens a direct conversation with one person, or a group ",
                    "conversation with several, and puts it in view; create_channel opens a new ",
                    "channel and puts it in view; update_channel renames or retopics a channel; ",
                    "duplicate_channel copies a channel's name and topic into a new empty one; ",
                    "search finds messages you can already see, anywhere; ",
                    "notification_settings reads or changes what is allowed to interrupt you; ",
                    "attention lists what arrived while you were not looking, and why; ",
                    "get_call_state reports who is currently in a voice or video call; ",
                    "threads lists the side conversations under the currently open place; ",
                    "create_thread opens one and moves this view into it; create_poll asks the ",
                    "open place a question; vote_poll answers one visible there; roles shows ",
                    "who may administer ",
                    "this workspace and what you yourself may do; create_role, update_role and ",
                    "delete_role change which bundles of permission exist; assign_roles changes ",
                    "which of them a participant holds; set_topic rewrites what the open channel ",
                    "says it is for."
                )
            },
            "place_id": {
                "type": "string",
                "description": concat!(
                    "Required for open, update_channel and duplicate_channel; optional for ",
                    "search and get_call_state; omitted for other actions. The place to open, ",
                    "edit or copy, the one place a search is restricted to, or the single place ",
                    "whose call to report."
                )
            },
            "name": {
                "type": "string",
                "description": concat!(
                    "Required for create_channel; optional for update_channel and ",
                    "duplicate_channel; omitted for other actions. The channel's name. For ",
                    "duplicate_channel, omitting it takes the derived default name for a copy."
                )
            },
            "topic": {
                "type": "string",
                "description": concat!(
                    "Optional for create_channel and update_channel, omitted for other actions. ",
                    "The one line describing what the channel is for."
                )
            },
            "workspace_id": {
                "type": "string",
                "description": concat!(
                    "Optional for create_channel and omitted for other actions. Which workspace ",
                    "to open the channel in; when omitted, the workspace you are in is used."
                )
            },
            "voice": {
                "type": "boolean",
                "description": concat!(
                    "Optional for create_channel and omitted for other actions. True opens a ",
                    "voice channel — a place people are meant to talk in (ADR 0012)."
                )
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
                "description": concat!(
                    "Optional for open, search and attention, omitted for other actions. ",
                    "Maximum number of messages, search hits or candidates to return."
                )
            },
            "content": {
                "type": "string",
                "description": concat!(
                    "Required for write, optional for create_poll, and omitted for other ",
                    "actions. For write, the message text sent to the currently open place; ",
                    "for create_poll, a line of your own text shown above the question."
                )
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
                    "For react, reply_later and create_thread, omitted for other actions. The ",
                    "target message by message_id. react and reply_later need exactly one of ",
                    "message_id or seq; create_thread takes at most one and starts a thread from ",
                    "nothing said when both are omitted. The message must be visible in the ",
                    "currently open place."
                )
            },
            "seq": {
                "type": "integer",
                "minimum": 1,
                "description": concat!(
                    "For react, reply_later and create_thread, omitted for other actions. The ",
                    "target message by its seq in the currently open place; provide at most one ",
                    "of message_id or seq."
                )
            },
            "name": {
                "type": "string",
                "description": concat!(
                    "Required for create_thread and omitted for other actions. The heading of ",
                    "the side conversation, as others will see it in the thread list."
                )
            },
            "question": {
                "type": "string",
                "description": "Required for create_poll and omitted for other actions. What the poll asks."
            },
            "options": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 2,
                "maxItems": 10,
                "description": concat!(
                    "Required for create_poll and omitted for other actions. Two to ten distinct ",
                    "choices, in the order they should be shown."
                )
            },
            "allow_multi": {
                "type": "boolean",
                "description": concat!(
                    "Optional for create_poll and omitted for other actions. When true a voter ",
                    "may pick several options; when omitted the poll takes one choice each."
                )
            },
            "closes_in_minutes": {
                "type": "integer",
                "minimum": 1,
                "maximum": 10080,
                "description": concat!(
                    "Optional for create_poll and omitted for other actions. Minutes until the ",
                    "poll stops accepting votes; when omitted it stays open."
                )
            },
            "option_ids": {
                "type": "array",
                "items": {"type": "string"},
                "maxItems": 10,
                "description": concat!(
                    "Required for vote_poll and omitted for other actions. The option_id values ",
                    "from the poll as the open place shows it. Restate your whole choice: an ",
                    "empty list withdraws your vote."
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
                    "which you declare; nothing about you is published automatically. These are ",
                    "the only three states: there is no offline or invisible, because nothing ",
                    "about your presence is observed in the first place."
                )
            },
            "display_name": {
                "type": "string",
                "description": concat!(
                    "Optional for profile and omitted for other actions. The name others see ",
                    "for you. Omit it to leave your current name unchanged."
                )
            },
            "tagline": {
                "type": "string",
                "description": concat!(
                    "Optional for profile and omitted for other actions. One line about what ",
                    "you do, shown next to your name. Omit it to leave it unchanged; send an ",
                    "empty string to remove it."
                )
            },
            "workspace_id": {
                "type": "string",
                "description": concat!(
                    "Optional for the role actions and omitted for other actions. Which ",
                    "workspace to describe or administer; when omitted your current one is used."
                )
            },
            "role_id": {
                "type": "string",
                "description": concat!(
                    "Required for update_role and delete_role, omitted for other actions. The ",
                    "role_id shown by the roles action."
                )
            },
            "role_name": {
                "type": "string",
                "description": concat!(
                    "Required for create_role and update_role, omitted for other actions. What ",
                    "this bundle of permissions is called, up to 60 characters, unique in the ",
                    "workspace."
                )
            },
            "role_color": {
                "type": "string",
                "description": concat!(
                    "Optional for create_role and update_role, omitted for other actions. The ",
                    "badge colour as #rrggbb in lowercase; the empty string leaves the role ",
                    "uncoloured."
                )
            },
            "permissions": {
                "type": "array",
                "items": {"type": "string", "enum": ROLE_PERMISSIONS},
                "description": concat!(
                    "Optional for create_role and update_role, omitted for other actions. The ",
                    "permissions this role ends up holding — naming them replaces the previous ",
                    "set rather than adding to it. Omitting the field leaves the role with none."
                )
            },
            "member_kind": {
                "type": "string",
                "enum": ["human", "personality_agent"],
                "description": concat!(
                    "Required for assign_roles and omitted for other actions. Which kind of ",
                    "participant member_id names; people and personality agents are addressed ",
                    "the same way."
                )
            },
            "member_id": {
                "type": "string",
                "description": concat!(
                    "Required for assign_roles and omitted for other actions. The id of the ",
                    "participant whose roles are being replaced."
                )
            },
            "role_ids": {
                "type": "array",
                "items": {"type": "string"},
                "description": concat!(
                    "Required for assign_roles and omitted for other actions. Every role that ",
                    "participant ends up holding; an empty list returns them to plain ",
                    "membership."
                )
            },
            "topic": {
                "type": "string",
                "description": concat!(
                    "Required for set_topic, optional for create_channel, omitted for other ",
                    "actions. The one line at the top of the channel saying what it is for; ",
                    "the empty string clears it."
                )
            },
            "note": {
                "type": "string",
                "description": concat!(
                    "Optional for status and reply_later, omitted for other actions. A short ",
                    "line others see alongside the state or the promise."
                )
            },
            "expires_in_minutes": {
                "type": "integer",
                "minimum": 1,
                "maximum": 10080,
                "description": concat!(
                    "Optional for status and omitted for other actions. Minutes until the status ",
                    "lapses on its own, returning you to whatever you had declared before it — ",
                    "use it for a state that is only true for a while (\"busy for the next ",
                    "hour\"). When omitted the status holds until you replace it."
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
                    "marker_id returned when you made the promise."
                )
            },
            "participants": {
                "type": "array",
                "maxItems": 32,
                "description": concat!(
                    "Required for start_dm and omitted for other actions. The people to open the ",
                    "conversation with, each copied from the participant object overview showed ",
                    "for that member. Do not list yourself. One entry opens the single direct ",
                    "conversation with that person; several open a group conversation."
                ),
                "items": {
                    "type": "object",
                    "properties": {
                        "kind": {
                            "type": "string",
                            "enum": ["human", "personality_agent"],
                            "description": "Which kind of participant this is."
                        },
                        "human_id": {
                            "type": "string",
                            "description": "Required when kind is human, omitted otherwise."
                        },
                        "personality_agent_id": {
                            "type": "string",
                            "description": "Required when kind is personality_agent, omitted otherwise."
                        }
                    },
                    "required": ["kind"],
                    "additionalProperties": false
                }
            },
            "query": {
                "type": "string",
                "description": concat!(
                    "Required for search and omitted for other actions. Words to look for. ",
                    "Matching is case-insensitive substring, so a partial word finds it; the ",
                    "results only ever come from places you can already see."
                )
            },
            "defaults_level": {
                "type": "string",
                "enum": ["all", "mentions", "mute"],
                "description": concat!(
                    "Optional for notification_settings and omitted for other actions. What may ",
                    "interrupt you in a place you have not singled out: all messages, only when ",
                    "you are called, or nothing."
                )
            },
            "per_place": {
                "type": "array",
                "description": concat!(
                    "Optional for notification_settings and omitted for other actions. Overrides ",
                    "for individual places. Sending this replaces the whole list of overrides."
                ),
                "items": {
                    "type": "object",
                    "properties": {
                        "place_id": {"type": "string"},
                        "level": {"type": "string", "enum": ["all", "mentions", "mute"]}
                    },
                    "required": ["place_id", "level"],
                    "additionalProperties": false
                }
            },
            "keywords": {
                "type": "array",
                "description": concat!(
                    "Optional for notification_settings and omitted for other actions. Words ",
                    "other than your name that should reach you. Sending this replaces the whole ",
                    "list; an empty array means you use no keywords."
                ),
                "items": {"type": "string"}
            },
            "consume_through": {
                "type": "integer",
                "minimum": 1,
                "description": concat!(
                    "Optional for attention and omitted for other actions. The candidate_seq you ",
                    "have taken in; everything up to and including it stops being offered. ",
                    "Acknowledging is idempotent and never moves the cursor backwards."
                )
            }
        },
        "required": ["action"],
        "additionalProperties": false
    })
}

#[async_trait]
impl Tool for MessagingTool {
    fn def(&self) -> ToolDefinition {
        ToolDefinition {
            name: TOOL_NAME.to_owned(),
            description: concat!(
                "Use Sumi's shared messaging app as a person. Start with overview, ",
                "open a place to see its timeline/members/unread state, then write in ",
                "that currently open place, or react or promise a later reply to a ",
                "message visible in it. When a tangent deserves its own room, list or ",
                "open a thread under the place. Declare your own availability with status, ",
                "say who you are — your name and what you do — with profile, or ",
                "open a new direct or group conversation with start_dm. ",
                "Use search to find something said elsewhere, attention to see what ",
                "arrived while you were not looking, notification_settings to ",
                "decide what is allowed to interrupt you, and get_call_state to see ",
                "who is currently in a call. ",
                "See who may administer this place with roles, and administer it yourself ",
                "with create_role, update_role, delete_role and assign_roles once you hold ",
                "the permission to. ",
                "Opening never publishes presence: what others see about your ",
                "attention is only what you declare. ",
                "A message may carry attachments; each one reports filename, mime, ",
                "size, an `alt` description written by the sender, and `spoiler`. ",
                "A spoilered attachment is one the sender chose to hide until the ",
                "reader opens it — treat it as hidden content: refer to it by its ",
                "alt description and do not reveal what it shows unless the reader ",
                "asks."
            )
            .to_owned(),
            parameters: messaging_parameters_schema(),
        }
    }

    fn risk(&self) -> ToolRisk {
        ToolRisk::Mutating
    }

    async fn execute(&self, ctx: ToolCtx<'_>) -> Result<ToolOutput, ToolError> {
        let action: MessagingAction =
            serde_json::from_value(Value::Object(ctx.args.as_object().clone()))
                .map_err(|_| ToolError::InvalidArguments)?;
        validate_action(&action)?;

        // Serialize this particular view. Multiple views may exist later; none
        // of them is the PersonalityAgent or owns a separate life log.
        let mut state = self.view.lock().await;
        self.flush_admitted_read(&mut state, &ctx.cancel).await?;

        // The MVP control plane creates/joins the shared default Workspace in
        // overview. Make that lifecycle precondition true even when the model
        // follows a permalink and opens a place first.
        let initial_overview = if state.initialized {
            None
        } else {
            let response = tokio::select! {
                _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                result = self.api.overview() => result,
            }
            .map_err(|error| ToolError::Rpc(error.to_string()))?;
            state.initialized = true;
            Some(response)
        };

        let response = match action {
            MessagingAction::Overview {} => match initial_overview {
                Some(response) => response,
                None => tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.overview() => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?,
            },
            MessagingAction::Open {
                place_id,
                before_seq,
                limit,
            } => {
                let response = tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.open(OpenMessagingPlaceRequest {
                        place_id: &place_id,
                        before_seq,
                        limit,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?;
                let last_visible_seq = response
                    .get("messages")
                    .and_then(Value::as_array)
                    .and_then(|messages| messages.last())
                    .and_then(|message| message.get("seq"))
                    .and_then(Value::as_u64);
                state.focused_place_id = Some(place_id.clone());
                state.pending_read_through = last_visible_seq.map(|seq| (place_id, seq));
                // The opened screen defines what can be reacted to; a new open
                // (including paging with before_seq) replaces the screen.
                state.visible_messages = visible_messages_from(&response);
                response
            }
            MessagingAction::Write {
                content,
                urgency,
                reply_to,
            } => {
                let place_id = state.focused_place_id.clone().ok_or_else(|| {
                    ToolError::Protocol(
                        "open a messaging place before writing; writing is scoped to the place currently in view"
                            .to_owned(),
                    )
                })?;
                let nonce = client_nonce(ctx.flow_id, ctx.call_id);
                let response = tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.write(WriteMessagingMessageRequest {
                        place_id: &place_id,
                        content: &content,
                        urgency: urgency_text(urgency),
                        reply_to: reply_to.as_deref(),
                        client_nonce: &nonce,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?;
                // The freshly sent message appears on the sender's own screen,
                // exactly like a human seeing their message land — so it is
                // immediately reactable.
                if let Some(message_id) = response.get("message_id").and_then(Value::as_str) {
                    state.visible_messages.push(VisibleMessage {
                        message_id: message_id.to_owned(),
                        seq: response.get("seq").and_then(Value::as_u64),
                    });
                }
                response
            }
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
                let target = visible_target(&state, &message_id, seq, "react")?;
                tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.react(ReactMessagingReactionRequest {
                        place_id: &place_id,
                        message_id: &target.message_id,
                        emoji: &emoji,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?
            }
            MessagingAction::Status {
                status,
                note,
                expires_in_minutes,
            } => tokio::select! {
                _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                result = self.api.set_status(SetMessagingStatusRequest {
                    status: status_text(status),
                    note: note.as_deref(),
                    expires_in_minutes,
                }) => result,
            }
            .map_err(|error| ToolError::Rpc(error.to_string()))?,
            MessagingAction::Profile {
                display_name,
                tagline,
            } => tokio::select! {
                _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                result = self.api.profile(SetMessagingProfileRequest {
                    display_name: display_name.as_deref(),
                    tagline: tagline.as_deref(),
                }) => result,
            }
            .map_err(|error| ToolError::Rpc(error.to_string()))?,
            MessagingAction::Roles { workspace_id } => tokio::select! {
                _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                result = self.api.roles(ListMessagingRolesRequest {
                    workspace_id: workspace_id.as_deref(),
                }) => result,
            }
            .map_err(|error| ToolError::Rpc(error.to_string()))?,
            MessagingAction::CreateRole {
                workspace_id,
                role_name,
                role_color,
                permissions,
            } => {
                let permissions = permissions.unwrap_or_default();
                tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.create_role(CreateMessagingRoleRequest {
                        workspace_id: workspace_id.as_deref(),
                        name: &role_name,
                        color: role_color.as_deref(),
                        permissions: &permissions,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?
            }
            MessagingAction::UpdateRole {
                workspace_id,
                role_id,
                role_name,
                role_color,
                permissions,
            } => {
                let permissions = permissions.unwrap_or_default();
                tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.update_role(UpdateMessagingRoleRequest {
                        workspace_id: workspace_id.as_deref(),
                        role_id: &role_id,
                        name: &role_name,
                        color: role_color.as_deref(),
                        permissions: &permissions,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?
            }
            MessagingAction::DeleteRole {
                workspace_id,
                role_id,
            } => tokio::select! {
                _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                result = self.api.delete_role(DeleteMessagingRoleRequest {
                    workspace_id: workspace_id.as_deref(),
                    role_id: &role_id,
                }) => result,
            }
            .map_err(|error| ToolError::Rpc(error.to_string()))?,
            MessagingAction::AssignRoles {
                workspace_id,
                member_kind,
                member_id,
                role_ids,
            } => tokio::select! {
                _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                result = self.api.set_member_roles(SetMessagingMemberRolesRequest {
                    workspace_id: workspace_id.as_deref(),
                    member_kind: member_kind_text(member_kind),
                    member_id: &member_id,
                    role_ids: &role_ids,
                }) => result,
            }
            .map_err(|error| ToolError::Rpc(error.to_string()))?,
            MessagingAction::SetTopic { topic } => {
                let place_id = state.focused_place_id.clone().ok_or_else(|| {
                    ToolError::Protocol(
                        "open a messaging place before setting its topic; the topic belongs to the place currently in view"
                            .to_owned(),
                    )
                })?;
                tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.set_channel_topic(SetMessagingChannelTopicRequest {
                        place_id: &place_id,
                        topic: &topic,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?
            }
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
                let target = visible_target(&state, &message_id, seq, "promise a reply")?;
                // The reminder itself arrives through the「予定された出来事」
                // wake trigger (#128); this call only makes the promise durable.
                tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.reply_later(CreateMessagingReplyLaterRequest {
                        place_id: &place_id,
                        message_id: &target.message_id,
                        note: note.as_deref(),
                        remind_in_minutes,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?
            }
            MessagingAction::ResolveReplyLater { marker_id } => tokio::select! {
                _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                result = self.api.resolve_reply_later(ResolveMessagingReplyLaterRequest {
                    marker_id: &marker_id,
                }) => result,
            }
            .map_err(|error| ToolError::Rpc(error.to_string()))?,
            MessagingAction::StartDm { participants } => {
                let response = tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.start_dm(StartMessagingDMRequest {
                        participants: &participants,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?;
                // A human who starts a conversation lands in it. Focus the new
                // place so writing needs no second gesture; nothing has been
                // seen there yet, so the screen starts empty.
                if let Some(dm_id) = response
                    .get("dm")
                    .and_then(|dm| dm.get("dm_id"))
                    .and_then(Value::as_str)
                {
                    state.focused_place_id = Some(dm_id.to_owned());
                    state.pending_read_through = None;
                    state.visible_messages.clear();
                }
                response
            }
            MessagingAction::Threads {} => {
                let place_id = state.focused_place_id.clone().ok_or_else(|| {
                    ToolError::Protocol(
                        "open a messaging place before listing threads; the list belongs to the place currently in view"
                            .to_owned(),
                    )
                })?;
                tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.threads(ListMessagingThreadsRequest {
                        place_id: &place_id,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?
            }
            MessagingAction::CreateThread {
                name,
                message_id,
                seq,
            } => {
                let place_id = state.focused_place_id.clone().ok_or_else(|| {
                    ToolError::Protocol(
                        "open a messaging place before creating a thread; the thread hangs under the place currently in view"
                            .to_owned(),
                    )
                })?;
                // An origin, when named, must be on this screen — the same rule
                // react and reply_later follow (ADR 0011 §3).
                let origin = if message_id.is_some() || seq.is_some() {
                    Some(visible_target(
                        &state,
                        &message_id,
                        seq,
                        "start a thread from it",
                    )?)
                } else {
                    None
                };
                let response = tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.create_thread(CreateMessagingThreadRequest {
                        place_id: &place_id,
                        name: &name,
                        parent_message_id: origin.as_ref().map(|target| target.message_id.as_str()),
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?;
                // Creating a thread takes you into it, exactly as the human UI
                // navigates to the new place. The screen starts empty.
                if let Some(thread_id) = response
                    .get("thread")
                    .and_then(|thread| thread.get("thread_id"))
                    .and_then(Value::as_str)
                {
                    state.focused_place_id = Some(thread_id.to_owned());
                    state.pending_read_through = None;
                    state.visible_messages.clear();
                }
                response
            }
            MessagingAction::CreateChannel {
                name,
                topic,
                workspace_id,
                voice,
            } => {
                let response = tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.create_channel(CreateMessagingChannelRequest {
                        workspace_id: workspace_id.as_deref(),
                        name: &name,
                        topic: topic.as_deref(),
                        voice,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?;
                focus_created_channel(&mut state, &response);
                response
            }
            MessagingAction::UpdateChannel {
                place_id,
                name,
                topic,
            } => tokio::select! {
                _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                result = self.api.update_channel(UpdateMessagingChannelRequest {
                    place_id: &place_id,
                    name: name.as_deref(),
                    topic: topic.as_deref(),
                }) => result,
            }
            .map_err(|error| ToolError::Rpc(error.to_string()))?,
            MessagingAction::DuplicateChannel { place_id, name } => {
                let response = tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.duplicate_channel(DuplicateMessagingChannelRequest {
                        place_id: &place_id,
                        name: name.as_deref(),
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?;
                focus_created_channel(&mut state, &response);
                response
            }
            // Search results are not a screen.  They stay out of
            // visible_messages, so a hit cannot be reacted to from the result
            // list — one opens the place first, exactly as a human does
            // (ADR 0011 §3: 見えていないものは操作できない).
            MessagingAction::Search {
                query,
                place_id,
                limit,
            } => tokio::select! {
                _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                result = self.api.search(SearchMessagingRequest {
                    query: &query,
                    place_id: place_id.as_deref(),
                    limit,
                }) => result,
            }
            .map_err(|error| ToolError::Rpc(error.to_string()))?,
            MessagingAction::NotificationSettings {
                defaults_level,
                per_place,
                keywords,
            } => {
                let places = per_place.as_ref().map(|entries| {
                    entries
                        .iter()
                        .map(|entry| MessagingNotificationPlace {
                            place_id: entry.place_id.as_str(),
                            level: notify_level_text(entry.level),
                        })
                        .collect()
                });
                let words = keywords
                    .as_ref()
                    .map(|words| words.iter().map(String::as_str).collect());
                tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.notification_settings(MessagingNotificationSettingsRequest {
                        defaults_level: defaults_level.map(notify_level_text),
                        per_place: places,
                        keywords: words,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?
            }
            MessagingAction::CreatePoll {
                question,
                options,
                allow_multi,
                content,
                closes_in_minutes,
            } => {
                let place_id = state.focused_place_id.clone().ok_or_else(|| {
                    ToolError::Protocol(
                        "open a messaging place before creating a poll; the question is asked of the place currently in view"
                            .to_owned(),
                    )
                })?;
                let nonce = client_nonce(ctx.flow_id, ctx.call_id);
                let response = tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.create_poll(CreateMessagingPollRequest {
                        place_id: &place_id,
                        question: &question,
                        options: &options,
                        allow_multi,
                        content: content.as_deref(),
                        client_nonce: &nonce,
                        closes_in_minutes,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?;
                // Like any message one sends, the poll lands on one's own
                // screen and can be answered or reacted to straight away.
                if let Some(message_id) = response.get("message_id").and_then(Value::as_str) {
                    state.visible_messages.push(VisibleMessage {
                        message_id: message_id.to_owned(),
                        seq: response.get("seq").and_then(Value::as_u64),
                    });
                }
                response
            }
            MessagingAction::VotePoll {
                message_id,
                seq,
                option_ids,
            } => {
                let place_id = state.focused_place_id.clone().ok_or_else(|| {
                    ToolError::Protocol(
                        "open a messaging place before voting; a poll is answered where it is shown"
                            .to_owned(),
                    )
                })?;
                let target = visible_target(&state, &message_id, seq, "vote on it")?;
                tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.vote_poll(VoteMessagingPollRequest {
                        place_id: &place_id,
                        message_id: &target.message_id,
                        option_ids: &option_ids,
                    }) => result,
                }
                .map_err(|error| ToolError::Rpc(error.to_string()))?
            }
            MessagingAction::Attention {
                consume_through,
                limit,
            } => tokio::select! {
                _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                result = self.api.attention(PollMessagingAttentionRequest {
                    consume_through,
                    limit,
                }) => result,
            }
            .map_err(|error| ToolError::Rpc(error.to_string()))?,
            MessagingAction::GetCallState { place_id } => tokio::select! {
                _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                result = self.api.call_state(GetMessagingCallStateRequest {
                    place_id: place_id.as_deref(),
                }) => result,
            }
            .map_err(|error| ToolError::Rpc(error.to_string()))?,
        };

        let rendered = serde_json::to_string_pretty(&response)
            .map_err(|error| ToolError::Protocol(error.to_string()))?;
        Ok(ToolOutput {
            content: vec![UserContent::Text { text: rendered }],
            details: response,
            is_error: false,
        })
    }
}

/// A channel that was just created becomes the place in view, the way a human
/// lands in the channel they made. Nothing has been seen there, so the screen
/// starts empty (ADR 0011 §3: 見えていないものは操作できない).
fn focus_created_channel(state: &mut MessagingViewState, response: &Value) {
    let Some(channel_id) = response
        .get("channel")
        .and_then(|channel| channel.get("channel_id"))
        .and_then(Value::as_str)
    else {
        return;
    };
    state.focused_place_id = Some(channel_id.to_owned());
    state.pending_read_through = None;
    state.visible_messages.clear();
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
            validate_optional_note(note, MAX_STATUS_NOTE_BYTES)?;
            validate_relative_minutes(expires_in_minutes)
        }
        MessagingAction::Profile {
            display_name,
            tagline,
        } => {
            // A present-but-blank name would ask the server to erase the one
            // thing every list needs; omitting the field is how one leaves it
            // alone. A blank tagline is legitimate — it removes the line.
            if display_name.as_deref().is_some_and(|name| {
                validate_bounded_nonempty(name, MAX_DISPLAY_NAME_BYTES).is_err()
            }) {
                return Err(ToolError::InvalidArguments);
            }
            if tagline.as_deref().is_some_and(|tagline| {
                tagline.len() > MAX_TAGLINE_BYTES || tagline.chars().any(char::is_control)
            }) {
                return Err(ToolError::InvalidArguments);
            }
            Ok(())
        }
        MessagingAction::Roles { workspace_id } => validate_optional_workspace(workspace_id),
        MessagingAction::CreateRole {
            workspace_id,
            role_name,
            role_color,
            permissions,
        } => {
            validate_optional_workspace(workspace_id)?;
            validate_role_shape(role_name, role_color, permissions)
        }
        MessagingAction::UpdateRole {
            workspace_id,
            role_id,
            role_name,
            role_color,
            permissions,
        } => {
            validate_optional_workspace(workspace_id)?;
            validate_bounded_nonempty(role_id, MAX_ROLE_ID_BYTES)?;
            validate_role_shape(role_name, role_color, permissions)
        }
        MessagingAction::DeleteRole {
            workspace_id,
            role_id,
        } => {
            validate_optional_workspace(workspace_id)?;
            validate_bounded_nonempty(role_id, MAX_ROLE_ID_BYTES)
        }
        MessagingAction::AssignRoles {
            workspace_id,
            member_id,
            role_ids,
            ..
        } => {
            validate_optional_workspace(workspace_id)?;
            validate_bounded_nonempty(member_id, MAX_PLACE_ID_BYTES)?;
            if role_ids.len() > MAX_ROLE_IDS {
                return Err(ToolError::InvalidArguments);
            }
            for role_id in role_ids {
                validate_bounded_nonempty(role_id, MAX_ROLE_ID_BYTES)?;
            }
            Ok(())
        }
        // A blank topic is legitimate: it is how the line is removed.
        MessagingAction::SetTopic { topic } => validate_topic(topic),
        MessagingAction::ReplyLater {
            message_id,
            seq,
            note,
            remind_in_minutes,
        } => {
            validate_visible_selector(message_id, seq)?;
            validate_optional_note(note, MAX_REPLY_LATER_NOTE_BYTES)?;
            validate_relative_minutes(remind_in_minutes)
        }
        MessagingAction::ResolveReplyLater { marker_id } => {
            validate_bounded_nonempty(marker_id, MAX_MARKER_ID_BYTES)
        }
        MessagingAction::StartDm { participants } => validate_dm_participants(participants),
        MessagingAction::CreateChannel {
            name,
            topic,
            workspace_id,
            ..
        } => {
            validate_bounded_nonempty(name, MAX_CHANNEL_NAME_BYTES)?;
            validate_optional_note(topic, MAX_TOPIC_BYTES)?;
            if workspace_id.as_deref().is_some_and(|workspace| {
                validate_bounded_nonempty(workspace, MAX_WORKSPACE_ID_BYTES).is_err()
            }) {
                return Err(ToolError::InvalidArguments);
            }
            Ok(())
        }
        MessagingAction::UpdateChannel {
            place_id,
            name,
            topic,
        } => {
            validate_bounded_nonempty(place_id, MAX_PLACE_ID_BYTES)?;
            // Naming nothing is not an edit; it would be a silent no-op that
            // reads to the model as a successful rename.
            if name.is_none() && topic.is_none() {
                return Err(ToolError::InvalidArguments);
            }
            if name.as_deref().is_some_and(|name| {
                validate_bounded_nonempty(name, MAX_CHANNEL_NAME_BYTES).is_err()
            }) {
                return Err(ToolError::InvalidArguments);
            }
            validate_optional_note(topic, MAX_TOPIC_BYTES)
        }
        MessagingAction::DuplicateChannel { place_id, name } => {
            validate_bounded_nonempty(place_id, MAX_PLACE_ID_BYTES)?;
            if name.as_deref().is_some_and(|name| {
                validate_bounded_nonempty(name, MAX_CHANNEL_NAME_BYTES).is_err()
            }) {
                return Err(ToolError::InvalidArguments);
            }
            Ok(())
        }
        MessagingAction::Search {
            query,
            place_id,
            limit,
        } => {
            // A blank query is not a search. Asking for everything is not a
            // question, and the server would refuse it anyway.
            if query.trim().is_empty() || query.len() > MAX_SEARCH_QUERY_BYTES {
                return Err(ToolError::InvalidArguments);
            }
            if place_id
                .as_deref()
                .is_some_and(|place| validate_bounded_nonempty(place, MAX_PLACE_ID_BYTES).is_err())
            {
                return Err(ToolError::InvalidArguments);
            }
            validate_limit(limit, MAX_SEARCH_LIMIT)
        }
        MessagingAction::NotificationSettings {
            per_place,
            keywords,
            ..
        } => {
            if per_place
                .as_ref()
                .is_some_and(|entries| entries.len() > MAX_NOTIFICATION_PLACES)
            {
                return Err(ToolError::InvalidArguments);
            }
            if let Some(entries) = per_place {
                for entry in entries {
                    validate_bounded_nonempty(&entry.place_id, MAX_PLACE_ID_BYTES)?;
                }
            }
            if let Some(words) = keywords {
                if words.len() > MAX_NOTIFICATION_KEYWORDS {
                    return Err(ToolError::InvalidArguments);
                }
                for word in words {
                    // 空文字は「呼ばれたい言葉」ではない。サーバー側でも落ちる。
                    if word.trim().is_empty()
                        || word.len() > MAX_NOTIFICATION_KEYWORD_BYTES
                        || word.chars().any(char::is_control)
                    {
                        return Err(ToolError::InvalidArguments);
                    }
                }
            }
            Ok(())
        }
        MessagingAction::Attention {
            consume_through,
            limit,
        } => {
            // Zero would mean "acknowledge nothing", which is what omitting the
            // field already says; a caller writing it means something else.
            if consume_through == &Some(0) {
                return Err(ToolError::InvalidArguments);
            }
            validate_limit(limit, MAX_ATTENTION_LIMIT)
        }
        MessagingAction::GetCallState { place_id } => {
            if place_id
                .as_deref()
                .is_some_and(|id| validate_bounded_nonempty(id, MAX_PLACE_ID_BYTES).is_err())
            {
                return Err(ToolError::InvalidArguments);
            }
            Ok(())
        }
        MessagingAction::Threads {} => Ok(()),
        MessagingAction::CreatePoll {
            question,
            options,
            content,
            closes_in_minutes,
            ..
        } => {
            validate_bounded_nonempty(question, MAX_POLL_QUESTION_BYTES)?;
            if !(MIN_POLL_OPTIONS..=MAX_POLL_OPTIONS).contains(&options.len()) {
                return Err(ToolError::InvalidArguments);
            }
            for option in options {
                validate_bounded_nonempty(option, MAX_POLL_OPTION_BYTES)?;
            }
            // Two identical choices cannot be told apart by a voter.
            for (index, option) in options.iter().enumerate() {
                if options[index + 1..].contains(option) {
                    return Err(ToolError::InvalidArguments);
                }
            }
            validate_optional_note(content, MAX_CONTENT_BYTES)?;
            validate_relative_minutes(closes_in_minutes)
        }
        MessagingAction::VotePoll {
            message_id,
            seq,
            option_ids,
        } => {
            validate_visible_selector(message_id, seq)?;
            // An empty list is a withdrawal, not a malformed vote.
            if option_ids.len() > MAX_POLL_OPTIONS {
                return Err(ToolError::InvalidArguments);
            }
            for option_id in option_ids {
                validate_bounded_nonempty(option_id, MAX_MESSAGE_ID_BYTES)?;
            }
            Ok(())
        }
        MessagingAction::CreateThread {
            name,
            message_id,
            seq,
        } => {
            validate_bounded_nonempty(name, MAX_THREAD_NAME_BYTES)?;
            // Unlike react, an origin is optional here: a thread may start
            // from a message or from nothing said yet. Naming both selectors
            // is still ambiguous.
            if message_id.is_some() && seq.is_some() {
                return Err(ToolError::InvalidArguments);
            }
            if seq == &Some(0) {
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
    }
}

/// The people a conversation is opened with. Each is named in the shape
/// overview already showed, and each names exactly one identity: a kind
/// without its matching id (or with the other kind's id) is not a person.
fn validate_dm_participants(participants: &[MessagingParticipant]) -> Result<(), ToolError> {
    if participants.is_empty() || participants.len() > MAX_DM_PARTICIPANTS {
        return Err(ToolError::InvalidArguments);
    }
    let mut seen = Vec::with_capacity(participants.len());
    for participant in participants {
        let id = match (
            participant.kind.as_str(),
            participant.human_id.as_deref(),
            participant.personality_agent_id.as_deref(),
        ) {
            ("human", Some(id), None) | ("personality_agent", None, Some(id)) => id,
            _ => return Err(ToolError::InvalidArguments),
        };
        validate_bounded_nonempty(id, MAX_PARTICIPANT_ID_BYTES)?;
        let key = format!("{}:{id}", participant.kind);
        if seen.contains(&key) {
            return Err(ToolError::InvalidArguments);
        }
        seen.push(key);
    }
    Ok(())
}

fn validate_limit(limit: &Option<u16>, max: u16) -> Result<(), ToolError> {
    if limit.is_some_and(|limit| limit == 0 || limit > max) {
        return Err(ToolError::InvalidArguments);
    }
    Ok(())
}

fn validate_optional_workspace(workspace_id: &Option<String>) -> Result<(), ToolError> {
    if workspace_id
        .as_deref()
        .is_some_and(|id| validate_bounded_nonempty(id, MAX_PLACE_ID_BYTES).is_err())
    {
        return Err(ToolError::InvalidArguments);
    }
    Ok(())
}

/// A topic is one line others read at the top of the screen, so a control
/// character in it would corrupt the header rather than say anything.
fn validate_topic(topic: &str) -> Result<(), ToolError> {
    if topic.len() > MAX_TOPIC_BYTES || topic.chars().any(char::is_control) {
        return Err(ToolError::InvalidArguments);
    }
    Ok(())
}

/// A colour the server will store: the empty string, which is how one asks for
/// no colour at all, or the lowercase `#rrggbb` its CHECK constraint accepts.
fn is_storable_role_color(color: &str) -> bool {
    color.is_empty()
        || (color.len() == 7
            && color.starts_with('#')
            && color[1..]
                .chars()
                .all(|character| character.is_ascii_hexdigit() && !character.is_ascii_uppercase()))
}

/// The shape create_role and update_role share. An unknown permission name is
/// refused here rather than dropped by the server, so an agent never walks away
/// believing a role carries something it does not.
fn validate_role_shape(
    role_name: &str,
    role_color: &Option<String>,
    permissions: &Option<Vec<String>>,
) -> Result<(), ToolError> {
    validate_bounded_nonempty(role_name, MAX_ROLE_NAME_BYTES)?;
    if role_name.chars().any(char::is_control) {
        return Err(ToolError::InvalidArguments);
    }
    if role_color
        .as_deref()
        .is_some_and(|color| !is_storable_role_color(color))
    {
        return Err(ToolError::InvalidArguments);
    }
    if let Some(permissions) = permissions
        && permissions
            .iter()
            .any(|permission| !ROLE_PERMISSIONS.contains(&permission.as_str()))
    {
        return Err(ToolError::InvalidArguments);
    }
    Ok(())
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

fn validate_optional_note(note: &Option<String>, max_bytes: usize) -> Result<(), ToolError> {
    if note
        .as_deref()
        .is_some_and(|note| note.len() > max_bytes || note.chars().any(char::is_control))
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
    state
        .visible_messages
        .iter()
        .find(|message| match (message_id, seq) {
            (Some(id), _) => &message.message_id == id,
            (None, Some(seq)) => message.seq == Some(seq),
            (None, None) => false,
        })
        .cloned()
        .ok_or_else(|| {
            ToolError::Protocol(format!(
                "that message is not visible in the currently open place; open the place (paging with before_seq if needed) so the message is on screen, then {verb}"
            ))
        })
}

/// Extracts the reactable screen contents from an open response. Entries
/// without a message_id (unexpected wire shapes) are skipped fail-closed.
fn visible_messages_from(response: &Value) -> Vec<VisibleMessage> {
    response
        .get("messages")
        .and_then(Value::as_array)
        .map(|messages| {
            messages
                .iter()
                .filter_map(|message| {
                    let message_id = message.get("message_id")?.as_str()?.to_owned();
                    let seq = message.get("seq").and_then(Value::as_u64);
                    Some(VisibleMessage { message_id, seq })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn validate_bounded_nonempty(value: &str, max_bytes: usize) -> Result<(), ToolError> {
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
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

const fn notify_level_text(level: MessagingNotifyLevel) -> &'static str {
    match level {
        MessagingNotifyLevel::All => "all",
        MessagingNotifyLevel::Mentions => "mentions",
        MessagingNotifyLevel::Mute => "mute",
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
    use std::collections::VecDeque;

    use anyhow::{Result, anyhow};
    use serde_json::json;
    use tokio::sync::Mutex as AsyncMutex;
    use tokio_util::sync::CancellationToken;

    use super::*;

    use crate::{provider::types::ValidatedToolArguments, tools::WorkspacePaths};

    #[derive(Default)]
    struct FakeMessagingApi {
        calls: AsyncMutex<Vec<String>>,
        reads: AsyncMutex<Vec<(String, u64)>>,
        writes: AsyncMutex<Vec<(String, String, String)>>,
        reacts: AsyncMutex<Vec<(String, String, String)>>,
        statuses: AsyncMutex<Vec<(String, Option<String>, Option<u32>)>>,
        profiles: AsyncMutex<Vec<(Option<String>, Option<String>)>>,
        role_reads: AsyncMutex<Vec<Option<String>>>,
        role_writes: AsyncMutex<Vec<(String, String, Vec<String>)>>,
        role_grants: AsyncMutex<Vec<(String, String, Vec<String>)>>,
        topics: AsyncMutex<Vec<(String, String)>>,
        promises: AsyncMutex<Vec<(String, String, Option<String>, Option<u32>)>>,
        resolutions: AsyncMutex<Vec<String>>,
        started_dms: AsyncMutex<Vec<Vec<MessagingParticipant>>>,
        channels: AsyncMutex<Vec<(String, String, Option<String>)>>,
        searches: AsyncMutex<Vec<(String, Option<String>, Option<u16>)>>,
        notifications: AsyncMutex<Vec<(Option<String>, usize, Vec<String>)>>,
        attentions: AsyncMutex<Vec<(Option<u64>, Option<u16>)>>,
        threads: AsyncMutex<Vec<(String, String, Option<String>)>>,
        polls: AsyncMutex<Vec<(String, String, Vec<String>, bool)>>,
        votes: AsyncMutex<Vec<(String, String, Vec<String>)>>,
        failures: AsyncMutex<VecDeque<&'static str>>,
    }

    #[async_trait]
    impl MessagingApi for FakeMessagingApi {
        async fn overview(&self) -> Result<Value> {
            self.calls.lock().await.push("overview".to_owned());
            Ok(json!({"channels": [{"channel_id": "general"}]}))
        }

        async fn open(&self, request: OpenMessagingPlaceRequest<'_>) -> Result<Value> {
            self.calls
                .lock()
                .await
                .push(format!("open:{}", request.place_id));
            Ok(json!({
                "place": {"kind": "channel", "channel_id": request.place_id},
                "latest_seq": 7,
                "last_read_seq": 3,
                "members": [],
                "messages": [
                    {"message_id": "m6", "seq": 6, "content": "earlier", "reactions": [],
                     "attachments": [
                        {"attachment_id": "a6", "filename": "ending.png",
                         "mime": "image/png", "size": 2048,
                         "spoiler": true, "alt": "結末の一枚"}
                     ]},
                    {"message_id": "m7", "seq": 7, "content": "hello",
                     "reactions": [{"emoji": "👍", "participants": []}]}
                ]
            }))
        }

        async fn write(&self, request: WriteMessagingMessageRequest<'_>) -> Result<Value> {
            self.calls
                .lock()
                .await
                .push(format!("write:{}", request.place_id));
            self.writes.lock().await.push((
                request.place_id.to_owned(),
                request.content.to_owned(),
                request.client_nonce.to_owned(),
            ));
            Ok(json!({"message_id": "m8", "seq": 8}))
        }

        async fn react(&self, request: ReactMessagingReactionRequest<'_>) -> Result<Value> {
            self.calls
                .lock()
                .await
                .push(format!("react:{}:{}", request.place_id, request.message_id));
            self.reacts.lock().await.push((
                request.place_id.to_owned(),
                request.message_id.to_owned(),
                request.emoji.to_owned(),
            ));
            Ok(json!({
                "message": {"message_id": request.message_id,
                            "reactions": [{"emoji": request.emoji, "participants": []}]},
                "reacted": true
            }))
        }

        async fn set_status(&self, request: SetMessagingStatusRequest<'_>) -> Result<Value> {
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

        async fn profile(&self, request: SetMessagingProfileRequest<'_>) -> Result<Value> {
            self.calls.lock().await.push("profile".to_owned());
            self.profiles.lock().await.push((
                request.display_name.map(str::to_owned),
                request.tagline.map(str::to_owned),
            ));
            Ok(json!({"profile": {
                "display_name": request.display_name.unwrap_or("Sumi"),
                "tagline": request.tagline.unwrap_or("")
            }}))
        }

        async fn roles(&self, request: ListMessagingRolesRequest<'_>) -> Result<Value> {
            self.calls.lock().await.push("roles".to_owned());
            self.role_reads
                .lock()
                .await
                .push(request.workspace_id.map(str::to_owned));
            Ok(json!({
                "workspace_id": request.workspace_id.unwrap_or("ws-1"),
                "roles": [{"role_id": "r1", "name": "Admin",
                           "permissions": {"manage_channels": true}}],
                "role_assignments": [],
                "members": [],
                "permissions": {}
            }))
        }

        async fn create_role(&self, request: CreateMessagingRoleRequest<'_>) -> Result<Value> {
            self.calls.lock().await.push("create_role".to_owned());
            self.role_writes.lock().await.push((
                request.name.to_owned(),
                request.color.unwrap_or_default().to_owned(),
                request.permissions.to_vec(),
            ));
            Ok(json!({"role": {"role_id": "r-new", "name": request.name}}))
        }

        async fn update_role(&self, request: UpdateMessagingRoleRequest<'_>) -> Result<Value> {
            self.calls
                .lock()
                .await
                .push(format!("update_role:{}", request.role_id));
            self.role_writes.lock().await.push((
                request.name.to_owned(),
                request.color.unwrap_or_default().to_owned(),
                request.permissions.to_vec(),
            ));
            Ok(json!({"role": {"role_id": request.role_id, "name": request.name}}))
        }

        async fn delete_role(&self, request: DeleteMessagingRoleRequest<'_>) -> Result<Value> {
            self.calls
                .lock()
                .await
                .push(format!("delete_role:{}", request.role_id));
            Ok(json!({"role_id": request.role_id, "deleted": true}))
        }

        async fn set_member_roles(
            &self,
            request: SetMessagingMemberRolesRequest<'_>,
        ) -> Result<Value> {
            self.calls.lock().await.push("assign_roles".to_owned());
            self.role_grants.lock().await.push((
                request.member_kind.to_owned(),
                request.member_id.to_owned(),
                request.role_ids.to_vec(),
            ));
            Ok(json!({"participant": {"kind": request.member_kind},
                      "role_ids": request.role_ids}))
        }

        async fn set_channel_topic(
            &self,
            request: SetMessagingChannelTopicRequest<'_>,
        ) -> Result<Value> {
            self.calls.lock().await.push("set_topic".to_owned());
            self.topics
                .lock()
                .await
                .push((request.place_id.to_owned(), request.topic.to_owned()));
            Ok(json!({"channel": {"channel_id": request.place_id, "topic": request.topic}}))
        }

        async fn reply_later(
            &self,
            request: CreateMessagingReplyLaterRequest<'_>,
        ) -> Result<Value> {
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
            Ok(json!({
                "marker": {"marker_id": "marker-1", "message_id": request.message_id,
                           "remind_at": "2026-08-04T12:00:00Z", "resolved": false},
                "created": true
            }))
        }

        async fn resolve_reply_later(
            &self,
            request: ResolveMessagingReplyLaterRequest<'_>,
        ) -> Result<Value> {
            self.calls
                .lock()
                .await
                .push(format!("resolve:{}", request.marker_id));
            self.resolutions
                .lock()
                .await
                .push(request.marker_id.to_owned());
            Ok(json!({"marker": {"marker_id": request.marker_id, "resolved": true}}))
        }

        async fn create_channel(
            &self,
            request: CreateMessagingChannelRequest<'_>,
        ) -> Result<Value> {
            self.calls
                .lock()
                .await
                .push(format!("create_channel:{}", request.name));
            self.channels.lock().await.push((
                "create".to_owned(),
                request.name.to_owned(),
                request.topic.map(str::to_owned),
            ));
            Ok(
                json!({"channel": {"channel_id": "ch-new", "name": request.name,
                                  "topic": request.topic.unwrap_or("")}}),
            )
        }

        async fn update_channel(
            &self,
            request: UpdateMessagingChannelRequest<'_>,
        ) -> Result<Value> {
            self.calls
                .lock()
                .await
                .push(format!("update_channel:{}", request.place_id));
            self.channels.lock().await.push((
                format!("update:{}", request.place_id),
                request.name.unwrap_or("").to_owned(),
                request.topic.map(str::to_owned),
            ));
            Ok(json!({"channel": {"channel_id": request.place_id,
                                  "name": request.name.unwrap_or("general")}}))
        }

        async fn duplicate_channel(
            &self,
            request: DuplicateMessagingChannelRequest<'_>,
        ) -> Result<Value> {
            self.calls
                .lock()
                .await
                .push(format!("duplicate_channel:{}", request.place_id));
            self.channels.lock().await.push((
                format!("duplicate:{}", request.place_id),
                request.name.unwrap_or("").to_owned(),
                None,
            ));
            Ok(json!({"channel": {"channel_id": "ch-copy", "name": "general のコピー"}}))
        }

        async fn start_dm(&self, request: StartMessagingDMRequest<'_>) -> Result<Value> {
            self.calls.lock().await.push("start_dm".to_owned());
            self.started_dms
                .lock()
                .await
                .push(request.participants.to_vec());
            let group = request.participants.len() > 1;
            Ok(json!({
                "dm": {
                    "dm_id": if group { "gdm-1" } else { "dm-1" },
                    "kind": if group { "group_dm" } else { "dm" },
                    "participants": []
                },
                "created": true
            }))
        }

        async fn threads(&self, request: ListMessagingThreadsRequest<'_>) -> Result<Value> {
            self.calls
                .lock()
                .await
                .push(format!("threads:{}", request.place_id));
            Ok(json!({"threads": []}))
        }

        async fn create_thread(&self, request: CreateMessagingThreadRequest<'_>) -> Result<Value> {
            self.calls
                .lock()
                .await
                .push(format!("create_thread:{}", request.place_id));
            self.threads.lock().await.push((
                request.place_id.to_owned(),
                request.name.to_owned(),
                request.parent_message_id.map(str::to_owned),
            ));
            Ok(json!({
                "thread": {
                    "thread_id": "th-1",
                    "name": request.name,
                    "parent_place": {"kind": "channel", "channel_id": request.place_id},
                    "parent_message_id": request.parent_message_id,
                    "message_count": 0,
                    "participants": []
                }
            }))
        }

        async fn create_poll(&self, request: CreateMessagingPollRequest<'_>) -> Result<Value> {
            self.calls
                .lock()
                .await
                .push(format!("create_poll:{}", request.place_id));
            self.polls.lock().await.push((
                request.place_id.to_owned(),
                request.question.to_owned(),
                request.options.to_vec(),
                request.allow_multi,
            ));
            Ok(json!({"message_id": "m9", "seq": 9}))
        }

        async fn vote_poll(&self, request: VoteMessagingPollRequest<'_>) -> Result<Value> {
            self.calls
                .lock()
                .await
                .push(format!("vote_poll:{}", request.message_id));
            self.votes.lock().await.push((
                request.place_id.to_owned(),
                request.message_id.to_owned(),
                request.option_ids.to_vec(),
            ));
            Ok(json!({"message": {"message_id": request.message_id}}))
        }

        async fn read_through(&self, request: ReadMessagingThroughRequest<'_>) -> Result<Value> {
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

        async fn search(&self, request: SearchMessagingRequest<'_>) -> Result<Value> {
            self.calls
                .lock()
                .await
                .push(format!("search:{}", request.query));
            self.searches.lock().await.push((
                request.query.to_owned(),
                request.place_id.map(str::to_owned),
                request.limit,
            ));
            Ok(json!({"results": [
                {"message_id": "m6", "place": {"kind": "channel", "channel_id": "general"},
                 "seq": 6, "snippet": request.query}
            ]}))
        }

        async fn notification_settings(
            &self,
            request: MessagingNotificationSettingsRequest<'_>,
        ) -> Result<Value> {
            self.calls.lock().await.push("notification".to_owned());
            self.notifications.lock().await.push((
                request.defaults_level.map(str::to_owned),
                request
                    .per_place
                    .as_ref()
                    .map(|entries| entries.len())
                    .unwrap_or_default(),
                request
                    .keywords
                    .as_ref()
                    .map(|words| words.iter().map(|word| (*word).to_owned()).collect())
                    .unwrap_or_default(),
            ));
            Ok(json!({"setting": {"defaults": {"level": request.defaults_level.unwrap_or("all")}}}))
        }

        async fn attention(&self, request: PollMessagingAttentionRequest) -> Result<Value> {
            self.calls.lock().await.push("attention".to_owned());
            self.attentions
                .lock()
                .await
                .push((request.consume_through, request.limit));
            Ok(json!({
                "candidates": [
                    {"candidate_id": "c1", "candidate_seq": 1,
                     "place": {"kind": "channel", "channel_id": "general"},
                     "message_seq": 7, "reason": "mention",
                     "arrival_time": "2026-08-04T12:00:00Z"}
                ],
                "consumed": request.consume_through.unwrap_or_default(),
                "latest_seq": 1
            }))
        }

        async fn call_state(&self, request: GetMessagingCallStateRequest<'_>) -> Result<Value> {
            self.calls
                .lock()
                .await
                .push(format!("call_state:{}", request.place_id.unwrap_or("*")));
            Ok(json!({"calls": [{
                "place": {"kind": "channel", "channel_id": request.place_id.unwrap_or("general")},
                "active": true,
                "participants": [
                    {"participant": {"kind": "human", "human_id": "h1"},
                     "screen_share": false}
                ]
            }]}))
        }
    }

    async fn execute(
        tool: &MessagingTool,
        action: Value,
        call_id: &str,
    ) -> Result<ToolOutput, ToolError> {
        let args: ValidatedToolArguments = serde_json::from_value(action).unwrap();
        let workspace = WorkspacePaths::new("/workspace").unwrap();
        tool.execute(ToolCtx {
            flow_id: "flow",
            call_id,
            args: &args,
            cancel: CancellationToken::new(),
            on_update: Arc::new(|_| {}),
            workspace: &workspace,
        })
        .await
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
        assert_eq!(schema["required"], json!(["action"]));
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(
            schema["properties"]["action"]["enum"],
            json!([
                "overview",
                "open",
                "write",
                "react",
                "status",
                "profile",
                "reply_later",
                "resolve_reply_later",
                "start_dm",
                "create_channel",
                "update_channel",
                "duplicate_channel",
                "search",
                "notification_settings",
                "attention",
                "get_call_state",
                "threads",
                "create_thread",
                "create_poll",
                "vote_poll",
                "roles",
                "create_role",
                "update_role",
                "delete_role",
                "assign_roles",
                "set_topic"
            ])
        );
        assert_eq!(
            schema["properties"]["defaults_level"]["enum"],
            json!(["all", "mentions", "mute"])
        );
        assert_eq!(schema["properties"]["query"]["type"], "string");
        assert_eq!(schema["properties"]["consume_through"]["minimum"], 1);
        assert_eq!(schema["properties"]["per_place"]["type"], "array");
        assert_eq!(schema["properties"]["keywords"]["items"]["type"], "string");
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
        assert_eq!(schema["properties"]["display_name"]["type"], "string");
        assert_eq!(schema["properties"]["tagline"]["type"], "string");
        assert_eq!(schema["properties"]["workspace_id"]["type"], "string");
        assert_eq!(schema["properties"]["role_id"]["type"], "string");
        assert_eq!(schema["properties"]["role_name"]["type"], "string");
        assert_eq!(schema["properties"]["role_color"]["type"], "string");
        assert_eq!(schema["properties"]["role_ids"]["type"], "array");
        assert_eq!(schema["properties"]["member_id"]["type"], "string");
        assert_eq!(
            schema["properties"]["member_kind"]["enum"],
            json!(["human", "personality_agent"])
        );
        // The permission vocabulary is closed and mirrors the server's.
        assert_eq!(
            schema["properties"]["permissions"]["items"]["enum"],
            json!([
                "manage_channels",
                "manage_roles",
                "manage_members",
                "mention_all"
            ])
        );
        assert_eq!(schema["properties"]["topic"]["type"], "string");
        assert_eq!(schema["properties"]["marker_id"]["type"], "string");
        assert_eq!(schema["properties"]["participants"]["type"], "array");
        for field in ["name", "topic", "workspace_id"] {
            assert_eq!(schema["properties"][field]["type"], "string");
        }
        assert_eq!(
            schema["properties"]["participants"]["items"]["properties"]["kind"]["enum"],
            json!(["human", "personality_agent"])
        );
        assert_eq!(schema["properties"]["question"]["type"], "string");
        assert_eq!(schema["properties"]["options"]["minItems"], 2);
        assert_eq!(schema["properties"]["options"]["maxItems"], 10);
        assert_eq!(schema["properties"]["allow_multi"]["type"], "boolean");
        assert_eq!(schema["properties"]["option_ids"]["type"], "array");
        assert_eq!(schema["properties"]["closes_in_minutes"]["minimum"], 1);
        for field in ["expires_in_minutes", "remind_in_minutes"] {
            assert_eq!(schema["properties"][field]["minimum"], 1);
            assert_eq!(schema["properties"][field]["maximum"], 10080);
        }
        assert_eq!(
            schema["properties"]
                .as_object()
                .expect("properties must be an object")
                .len(),
            39
        );
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

        let profile: MessagingAction = serde_json::from_value(json!({
            "action": "profile",
            "display_name": "墨",
            "tagline": "秘書"
        }))
        .unwrap();
        assert!(matches!(
            profile,
            MessagingAction::Profile {
                display_name: Some(display_name),
                tagline: Some(tagline)
            } if display_name == "墨" && tagline == "秘書"
        ));

        let roles: MessagingAction = serde_json::from_value(json!({
            "action": "roles",
            "workspace_id": "ws-1"
        }))
        .unwrap();
        assert!(matches!(
            roles,
            MessagingAction::Roles { workspace_id: Some(workspace_id) } if workspace_id == "ws-1"
        ));

        let create_role: MessagingAction = serde_json::from_value(json!({
            "action": "create_role",
            "role_name": "開発",
            "role_color": "#3366ff",
            "permissions": ["manage_channels"]
        }))
        .unwrap();
        assert!(matches!(
            create_role,
            MessagingAction::CreateRole {
                workspace_id: None,
                role_name,
                role_color: Some(role_color),
                permissions: Some(permissions)
            } if role_name == "開発" && role_color == "#3366ff"
                && permissions == vec!["manage_channels".to_owned()]
        ));

        let update_role: MessagingAction = serde_json::from_value(json!({
            "action": "update_role",
            "role_id": "role-1",
            "role_name": "設計"
        }))
        .unwrap();
        assert!(matches!(
            update_role,
            MessagingAction::UpdateRole {
                role_id,
                role_name,
                role_color: None,
                permissions: None,
                ..
            } if role_id == "role-1" && role_name == "設計"
        ));

        let delete_role: MessagingAction = serde_json::from_value(json!({
            "action": "delete_role",
            "role_id": "role-1"
        }))
        .unwrap();
        assert!(matches!(
            delete_role,
            MessagingAction::DeleteRole { role_id, .. } if role_id == "role-1"
        ));

        let assign_roles: MessagingAction = serde_json::from_value(json!({
            "action": "assign_roles",
            "member_kind": "human",
            "member_id": "human-1",
            "role_ids": ["role-1"]
        }))
        .unwrap();
        assert!(matches!(
            assign_roles,
            MessagingAction::AssignRoles {
                member_kind: MessagingMemberKind::Human,
                member_id,
                role_ids,
                ..
            } if member_id == "human-1" && role_ids == vec!["role-1".to_owned()]
        ));

        let create_channel: MessagingAction = serde_json::from_value(json!({
            "action": "create_channel",
            "name": "dev",
            "topic": "開発の相談"
        }))
        .unwrap();
        assert!(matches!(
            create_channel,
            MessagingAction::CreateChannel {
                workspace_id: None,
                name,
                topic: Some(topic),
                voice: None,
            } if name == "dev" && topic == "開発の相談"
        ));

        let set_topic: MessagingAction = serde_json::from_value(json!({
            "action": "set_topic",
            "topic": "レビュー予約はこちら"
        }))
        .unwrap();
        assert!(matches!(
            set_topic,
            MessagingAction::SetTopic { topic } if topic == "レビュー予約はこちら"
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

        let search: MessagingAction = serde_json::from_value(json!({
            "action": "search",
            "query": "デプロイ",
            "place_id": "general",
            "limit": 10
        }))
        .unwrap();
        assert!(matches!(
            search,
            MessagingAction::Search {
                query,
                place_id: Some(place_id),
                limit: Some(10)
            } if query == "デプロイ" && place_id == "general"
        ));

        // 何も名指さない呼びも通る。それが「読み」である。
        let read: MessagingAction =
            serde_json::from_value(json!({"action": "notification_settings"})).unwrap();
        assert!(matches!(
            read,
            MessagingAction::NotificationSettings {
                defaults_level: None,
                per_place: None,
                keywords: None
            }
        ));

        let settings: MessagingAction = serde_json::from_value(json!({
            "action": "notification_settings",
            "defaults_level": "mute",
            "per_place": [{"place_id": "general", "level": "all"}],
            "keywords": ["障害"]
        }))
        .unwrap();
        assert!(matches!(
            settings,
            MessagingAction::NotificationSettings {
                defaults_level: Some(MessagingNotifyLevel::Mute),
                per_place: Some(places),
                keywords: Some(keywords)
            } if places.len() == 1
                && places[0].place_id == "general"
                && matches!(places[0].level, MessagingNotifyLevel::All)
                && keywords == vec!["障害".to_owned()]
        ));

        let attention: MessagingAction = serde_json::from_value(json!({
            "action": "attention",
            "consume_through": 12
        }))
        .unwrap();
        assert!(matches!(
            attention,
            MessagingAction::Attention {
                consume_through: Some(12),
                limit: None
            }
        ));

        let all_calls: MessagingAction =
            serde_json::from_value(json!({"action": "get_call_state"})).unwrap();
        assert!(matches!(
            all_calls,
            MessagingAction::GetCallState { place_id: None }
        ));
        let one_call: MessagingAction =
            serde_json::from_value(json!({"action": "get_call_state", "place_id": "general"}))
                .unwrap();
        assert!(matches!(
            one_call,
            MessagingAction::GetCallState { place_id: Some(place_id) } if place_id == "general"
        ));
    }

    /// Reading who is in a call is about people, not about a screen — like
    /// status it needs no open place (ADR 0012).
    #[tokio::test]
    async fn call_state_is_readable_without_opening_a_place() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        let output = execute(&tool, json!({"action": "get_call_state"}), "calls")
            .await
            .unwrap();
        assert!(output.details["calls"][0]["active"].as_bool().unwrap());
        assert_eq!(
            api.calls.lock().await.as_slice(),
            ["overview", "call_state:*"]
        );

        execute(
            &tool,
            json!({"action": "get_call_state", "place_id": "general"}),
            "one-call",
        )
        .await
        .unwrap();
        assert_eq!(
            api.calls.lock().await.last().map(String::as_str),
            Some("call_state:general")
        );

        for invalid in [
            json!({"action": "get_call_state", "place_id": ""}),
            json!({"action": "get_call_state", "place_id": "a\nb"}),
        ] {
            execute(&tool, invalid, "invalid")
                .await
                .expect_err("malformed place_id must be rejected before the call");
        }
    }

    #[tokio::test]
    async fn open_does_not_mark_read_until_the_next_admitted_tool_result() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        // The topic is the line at the top of the screen, so there must be a
        // screen: without an open place the tool refuses rather than guessing.
        let error = execute(
            &tool,
            json!({"action": "set_topic", "topic": "x"}),
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
        assert!(api.reads.lock().await.is_empty());

        execute(&tool, json!({"action": "overview"}), "overview")
            .await
            .unwrap();
        assert_eq!(
            api.reads.lock().await.as_slice(),
            &[("general".to_owned(), 7)]
        );
        assert_eq!(
            api.calls.lock().await.as_slice(),
            &["overview", "open:general", "read:general", "overview"]
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
        let writes = api.writes.lock().await;
        assert_eq!(writes[0].0, "general");
        assert_eq!(writes[0].1, "hi");
        assert_eq!(writes[0].2, client_nonce("flow", "write"));
    }

    #[tokio::test]
    async fn failed_delayed_read_is_preserved_and_blocks_the_next_action() {
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
        assert!(
            execute(&tool, json!({"action": "overview"}), "one")
                .await
                .is_err()
        );
        execute(&tool, json!({"action": "overview"}), "two")
            .await
            .unwrap();
        assert_eq!(api.reads.lock().await.len(), 1);
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
                ("general".to_owned(), "m7".to_owned(), "👍".to_owned()),
                ("general".to_owned(), "m6".to_owned(), "🎉".to_owned()),
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

    #[tokio::test]
    async fn profile_is_read_and_changed_field_by_field_without_an_open_place() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        // 名乗り is about the person, so like status it needs no place in view.
        execute(&tool, json!({"action": "profile"}), "read")
            .await
            .unwrap();
        assert_eq!(api.profiles.lock().await.as_slice(), &[(None, None)]);

        // Naming one field leaves the other alone: omitted fields stay off the
        // wire rather than arriving as a null the server must interpret.
        execute(
            &tool,
            json!({"action": "profile", "tagline": "開発"}),
            "tagline",
        )
        .await
        .unwrap();
        assert_eq!(
            api.profiles.lock().await[1],
            (None, Some("開発".to_owned()))
        );

        // An empty tagline removes the line; an empty name would erase the one
        // thing every member list needs, so it is refused here.
        execute(&tool, json!({"action": "profile", "tagline": ""}), "clear")
            .await
            .unwrap();
        assert_eq!(api.profiles.lock().await[2], (None, Some(String::new())));

        for arguments in [
            json!({"action": "profile", "display_name": ""}),
            json!({"action": "profile", "display_name": "改\n行"}),
        ] {
            let error = execute(&tool, arguments, "invalid").await.unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        assert_eq!(api.profiles.lock().await.len(), 3);
    }

    #[tokio::test]
    async fn roles_are_readable_without_an_open_place() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        execute(&tool, json!({"action": "roles"}), "roles")
            .await
            .unwrap();
        assert_eq!(api.role_reads.lock().await.as_slice(), &[None]);

        execute(
            &tool,
            json!({"action": "roles", "workspace_id": "ws-2"}),
            "scoped",
        )
        .await
        .unwrap();
        assert_eq!(api.role_reads.lock().await[1], Some("ws-2".to_owned()));

        let error = execute(
            &tool,
            json!({"action": "roles", "workspace_id": ""}),
            "invalid",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::InvalidArguments));
        assert_eq!(api.role_reads.lock().await.len(), 2);
    }

    /// Every administrative gesture the human settings screen offers has an
    /// action here (AX 同型). The refusal for an agent that may not administer
    /// comes from the server's permission check, not from a missing tool.
    #[tokio::test]
    async fn every_role_administration_the_settings_screen_offers_has_an_action() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        execute(
            &tool,
            json!({"action": "create_role", "role_name": "開発",
                   "role_color": "#3366ff", "permissions": ["manage_channels"]}),
            "create",
        )
        .await
        .unwrap();
        execute(
            &tool,
            json!({"action": "update_role", "role_id": "r1", "role_name": "設計",
                   "permissions": []}),
            "update",
        )
        .await
        .unwrap();
        execute(
            &tool,
            json!({"action": "delete_role", "role_id": "r1"}),
            "delete",
        )
        .await
        .unwrap();
        execute(
            &tool,
            json!({"action": "assign_roles", "member_kind": "personality_agent",
                   "member_id": "pa-1", "role_ids": ["r1", "r2"]}),
            "assign",
        )
        .await
        .unwrap();

        assert_eq!(
            api.role_writes.lock().await.as_slice(),
            &[
                (
                    "開発".to_owned(),
                    "#3366ff".to_owned(),
                    vec!["manage_channels".to_owned()]
                ),
                // Naming no permission is how one is removed: the call replaces
                // the set rather than adding to it.
                ("設計".to_owned(), String::new(), vec![]),
            ]
        );
        assert_eq!(
            api.role_grants.lock().await.as_slice(),
            &[(
                "personality_agent".to_owned(),
                "pa-1".to_owned(),
                vec!["r1".to_owned(), "r2".to_owned()]
            )]
        );
        assert!(
            api.calls
                .lock()
                .await
                .contains(&"delete_role:r1".to_owned())
        );
    }

    #[tokio::test]
    async fn role_administration_refuses_shapes_the_server_would_reject() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        for arguments in [
            // A blank name is not a role anybody could point at.
            json!({"action": "create_role", "role_name": ""}),
            // Colours are the lowercase #rrggbb the schema stores.
            json!({"action": "create_role", "role_name": "色", "role_color": "red"}),
            json!({"action": "create_role", "role_name": "色", "role_color": "#AABBCC"}),
            // An unknown permission is refused here rather than dropped later,
            // so nobody walks away believing the role carries it.
            json!({"action": "create_role", "role_name": "夢", "permissions": ["become_owner"]}),
            json!({"action": "update_role", "role_id": "", "role_name": "改名"}),
            json!({"action": "delete_role", "role_id": ""}),
            json!({"action": "assign_roles", "member_kind": "bot",
                   "member_id": "b-1", "role_ids": []}),
            json!({"action": "assign_roles", "member_kind": "human",
                   "member_id": "", "role_ids": []}),
            json!({"action": "assign_roles", "member_kind": "human",
                   "member_id": "h-1", "role_ids": [""]}),
        ] {
            let error = execute(&tool, arguments, "invalid").await.unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        assert!(api.role_writes.lock().await.is_empty());
        assert!(api.role_grants.lock().await.is_empty());
    }

    /// manage_channels は agent にも与えられる権限なので、チャンネルを作る・
    /// トピックを書き換えるという human の画面の操作もここにある。
    #[tokio::test]
    async fn channels_are_created_anywhere_but_the_topic_belongs_to_the_open_place() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        execute(
            &tool,
            json!({"action": "create_channel", "name": "dev", "topic": "開発の相談"}),
            "create-channel",
        )
        .await
        .unwrap();
        assert_eq!(
            api.channels.lock().await.as_slice(),
            &[(
                "create".to_owned(),
                "dev".to_owned(),
                Some("開発の相談".to_owned())
            )]
        );

        execute(
            &tool,
            json!({"action": "open", "place_id": "general"}),
            "open",
        )
        .await
        .unwrap();
        execute(
            &tool,
            json!({"action": "set_topic", "topic": "レビュー予約はこちら"}),
            "set-topic",
        )
        .await
        .unwrap();
        assert_eq!(
            api.topics.lock().await.as_slice(),
            &[("general".to_owned(), "レビュー予約はこちら".to_owned())]
        );

        for arguments in [
            json!({"action": "create_channel", "name": ""}),
            json!({"action": "create_channel", "name": "dev\n"}),
            json!({"action": "set_topic", "topic": "改行\nは入らない"}),
        ] {
            let error = execute(&tool, arguments, "invalid").await.unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        assert_eq!(api.channels.lock().await.len(), 1);
        assert_eq!(api.topics.lock().await.len(), 1);
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

        // Like the human's reply-later list, keeping a promise is reachable
        // from anywhere — the place it was made in need not be open.
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
    async fn start_dm_opens_the_conversation_and_puts_it_in_view() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        // One person: the single direct conversation, and the agent lands in
        // it — writing needs no separate open, exactly as a human is taken
        // into the conversation they just started.
        execute(
            &tool,
            json!({"action": "start_dm",
                   "participants": [{"kind": "human", "human_id": "h-haru"}]}),
            "dm",
        )
        .await
        .unwrap();
        execute(
            &tool,
            json!({"action": "write", "content": "はじめまして"}),
            "w1",
        )
        .await
        .unwrap();
        assert_eq!(api.writes.lock().await[0].0, "dm-1");

        // Several people: a group conversation, which becomes the place in view.
        execute(
            &tool,
            json!({"action": "start_dm", "participants": [
                {"kind": "human", "human_id": "h-haru"},
                {"kind": "personality_agent", "personality_agent_id": "a-kuro"}
            ]}),
            "gdm",
        )
        .await
        .unwrap();
        execute(&tool, json!({"action": "write", "content": "3人で"}), "w2")
            .await
            .unwrap();
        assert_eq!(api.writes.lock().await[1].0, "gdm-1");
        assert_eq!(api.started_dms.lock().await.len(), 2);

        // Nothing has been seen in a place just opened, so there is nothing to
        // react to there yet.
        let error = execute(
            &tool,
            json!({"action": "react", "seq": 7, "emoji": "👍"}),
            "react",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::Protocol(_)));

        // A participant must name exactly one identity, and nobody twice.
        for arguments in [
            json!({"action": "start_dm", "participants": []}),
            json!({"action": "start_dm", "participants": [{"kind": "human"}]}),
            json!({"action": "start_dm",
                   "participants": [{"kind": "human", "personality_agent_id": "a-kuro"}]}),
            json!({"action": "start_dm", "participants": [
                {"kind": "human", "human_id": "h-haru"},
                {"kind": "human", "human_id": "h-haru"}
            ]}),
            json!({"action": "start_dm",
                   "participants": [{"kind": "app", "human_id": "h-haru"}]}),
        ] {
            let error = execute(&tool, arguments, "invalid").await.unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        assert_eq!(api.started_dms.lock().await.len(), 2);
    }

    #[tokio::test]
    async fn channel_lifecycle_matches_the_human_context_menu() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        // Creating a channel lands the agent in it, as a human is taken into
        // the channel they just made.
        execute(
            &tool,
            json!({"action": "create_channel", "name": "設計", "topic": "図面の相談"}),
            "create",
        )
        .await
        .unwrap();
        execute(
            &tool,
            json!({"action": "write", "content": "ここで話します"}),
            "w",
        )
        .await
        .unwrap();
        assert_eq!(api.writes.lock().await[0].0, "ch-new");

        execute(
            &tool,
            json!({"action": "update_channel", "place_id": "ch-new", "topic": "図面と素材"}),
            "update",
        )
        .await
        .unwrap();
        execute(
            &tool,
            json!({"action": "duplicate_channel", "place_id": "ch-new"}),
            "duplicate",
        )
        .await
        .unwrap();
        let channels = api.channels.lock().await.clone();
        assert_eq!(channels[0].0, "create");
        assert_eq!(channels[0].1, "設計");
        assert_eq!(channels[1].0, "update:ch-new");
        assert_eq!(channels[1].2, Some("図面と素材".to_owned()));
        assert_eq!(channels[2].0, "duplicate:ch-new");
        // The copy's name is the server's to derive; the tool does not invent
        // its own so the two sides cannot disagree about what a copy is called.
        assert_eq!(channels[2].1, "");

        for arguments in [
            json!({"action": "create_channel"}),
            json!({"action": "create_channel", "name": ""}),
            // An edit that names nothing is a silent no-op that reads as success.
            json!({"action": "update_channel", "place_id": "ch-new"}),
            json!({"action": "update_channel", "place_id": "ch-new", "name": ""}),
            json!({"action": "duplicate_channel"}),
        ] {
            let error = execute(&tool, arguments, "invalid").await.unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        assert_eq!(api.channels.lock().await.len(), 3);
    }

    #[tokio::test]
    async fn threads_need_an_open_place_and_creation_moves_the_view_into_it() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        // A thread hangs under a place, so both actions need one in view.
        for arguments in [
            json!({"action": "threads"}),
            json!({"action": "create_thread", "name": "認証リダイレクトの件"}),
        ] {
            let error = execute(&tool, arguments, "no-place").await.unwrap_err();
            assert!(matches!(error, ToolError::Protocol(_)));
        }
        assert!(api.threads.lock().await.is_empty());

        execute(
            &tool,
            json!({"action": "open", "place_id": "general"}),
            "open",
        )
        .await
        .unwrap();
        execute(&tool, json!({"action": "threads"}), "list")
            .await
            .unwrap();
        execute(
            &tool,
            json!({"action": "create_thread", "name": "認証リダイレクトの件", "seq": 7}),
            "create",
        )
        .await
        .unwrap();
        assert_eq!(
            api.threads.lock().await.as_slice(),
            &[(
                "general".to_owned(),
                "認証リダイレクトの件".to_owned(),
                Some("m7".to_owned())
            )]
        );

        // Creating moved the view into the new thread: the next write lands
        // there, and the parent's screen is no longer what can be acted on.
        execute(
            &tool,
            json!({"action": "write", "content": "続きはこちらで"}),
            "write",
        )
        .await
        .unwrap();
        assert_eq!(api.writes.lock().await[0].0, "th-1");
        let error = execute(
            &tool,
            json!({"action": "react", "seq": 7, "emoji": "👍"}),
            "react",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::Protocol(_)));
    }

    #[tokio::test]
    async fn create_thread_rejects_missing_names_and_unseen_or_ambiguous_origins() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());
        execute(
            &tool,
            json!({"action": "open", "place_id": "general"}),
            "open",
        )
        .await
        .unwrap();

        for arguments in [
            json!({"action": "create_thread"}),
            json!({"action": "create_thread", "name": ""}),
            json!({"action": "create_thread", "name": "x", "message_id": "m7", "seq": 7}),
            json!({"action": "create_thread", "name": "x", "seq": 0}),
        ] {
            let error = execute(&tool, arguments, "invalid").await.unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        // A message off the screen cannot become an origin (ADR 0011 §3).
        let error = execute(
            &tool,
            json!({"action": "create_thread", "name": "x", "message_id": "m404"}),
            "missing",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::Protocol(_)));
        assert!(api.threads.lock().await.is_empty());

        // Without an origin the thread simply starts from nothing said yet.
        execute(
            &tool,
            json!({"action": "create_thread", "name": "来週の段取り"}),
            "scratch",
        )
        .await
        .unwrap();
        assert_eq!(api.threads.lock().await[0].2, None);
    }

    #[tokio::test]
    async fn polls_are_asked_of_the_open_place_and_answered_where_they_are_shown() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        // Both acts belong to a place in view.
        for arguments in [
            json!({"action": "create_poll", "question": "いつ出す？", "options": ["今日", "明日"]}),
            json!({"action": "vote_poll", "seq": 7, "option_ids": ["o1"]}),
        ] {
            let error = execute(&tool, arguments, "no-place").await.unwrap_err();
            assert!(matches!(error, ToolError::Protocol(_)));
        }

        execute(
            &tool,
            json!({"action": "open", "place_id": "general"}),
            "open",
        )
        .await
        .unwrap();
        execute(
            &tool,
            json!({"action": "create_poll", "question": "いつ出す？",
                   "options": ["今日", "明日"], "allow_multi": true}),
            "poll",
        )
        .await
        .unwrap();
        assert_eq!(
            api.polls.lock().await.as_slice(),
            &[(
                "general".to_owned(),
                "いつ出す？".to_owned(),
                vec!["今日".to_owned(), "明日".to_owned()],
                true
            )]
        );

        // One's own poll lands on one's own screen and can be answered at once.
        execute(
            &tool,
            json!({"action": "vote_poll", "seq": 9, "option_ids": ["o1"]}),
            "vote",
        )
        .await
        .unwrap();
        // An empty list is a withdrawal, not a malformed vote.
        execute(
            &tool,
            json!({"action": "vote_poll", "message_id": "m9", "option_ids": []}),
            "withdraw",
        )
        .await
        .unwrap();
        assert_eq!(
            api.votes.lock().await.as_slice(),
            &[
                ("general".to_owned(), "m9".to_owned(), vec!["o1".to_owned()]),
                ("general".to_owned(), "m9".to_owned(), vec![]),
            ]
        );

        // A poll off the screen cannot be voted on (ADR 0011 §3).
        let error = execute(
            &tool,
            json!({"action": "vote_poll", "message_id": "m404", "option_ids": ["o1"]}),
            "unseen",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::Protocol(_)));
    }

    #[tokio::test]
    async fn create_poll_rejects_unaskable_questions() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());
        execute(
            &tool,
            json!({"action": "open", "place_id": "general"}),
            "open",
        )
        .await
        .unwrap();

        for arguments in [
            // A question with fewer than two choices is an announcement.
            json!({"action": "create_poll", "question": "?", "options": ["ひとつ"]}),
            json!({"action": "create_poll", "question": "", "options": ["a", "b"]}),
            // Two identical choices cannot be told apart by a voter.
            json!({"action": "create_poll", "question": "?", "options": ["a", "a"]}),
            json!({"action": "create_poll", "question": "?",
                   "options": ["a", "b"], "closes_in_minutes": 0}),
            json!({"action": "create_poll", "question": "?",
                   "options": ["a", "b"], "closes_in_minutes": 10081}),
            // Voting still needs exactly one selector.
            json!({"action": "vote_poll", "option_ids": ["o1"]}),
            json!({"action": "vote_poll", "message_id": "m7", "seq": 7, "option_ids": ["o1"]}),
        ] {
            let error = execute(&tool, arguments, "invalid").await.unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments));
        }
        assert!(api.polls.lock().await.is_empty());
        assert!(api.votes.lock().await.is_empty());
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
            &[("general".to_owned(), "m8".to_owned(), "✅".to_owned())]
        );
    }

    /// AX/UX 同型性: 送り手が添付に付けた「ネタバレ」と概要は、人間の画面と
    /// 同じくこの view からも見えなければならない。見えなければ agent は
    /// 隠されているはずの中身を平然と読み上げてしまう。
    #[tokio::test]
    async fn open_shows_the_sender_spoiler_and_alt_on_attachments() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());

        let output = execute(
            &tool,
            json!({"action": "open", "place_id": "general"}),
            "open",
        )
        .await
        .unwrap();

        let attachment = output
            .details
            .get("messages")
            .and_then(Value::as_array)
            .and_then(|messages| messages.first())
            .and_then(|message| message.get("attachments"))
            .and_then(Value::as_array)
            .and_then(|attachments| attachments.first())
            .expect("open response carries the message's attachments");
        assert_eq!(attachment.get("spoiler"), Some(&Value::Bool(true)));
        assert_eq!(
            attachment.get("alt").and_then(Value::as_str),
            Some("結末の一枚")
        );

        // モデルが読むテキストにも同じことが載る（details だけではない）。
        let UserContent::Text { text } = &output.content[0] else {
            panic!("messaging tool renders text");
        };
        assert!(text.contains("\"spoiler\": true"), "rendered: {text}");
        assert!(text.contains("結末の一枚"), "rendered: {text}");
    }

    #[tokio::test]
    async fn search_needs_no_open_place_and_never_becomes_a_screen() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());
        execute(
            &tool,
            json!({"action": "search", "query": "デプロイ", "limit": 5}),
            "search",
        )
        .await
        .unwrap();
        assert_eq!(
            api.searches.lock().await.as_slice(),
            &[("デプロイ".to_owned(), None, Some(5))]
        );
        // A hit is not something on screen: reacting to it must still require
        // opening the place first (ADR 0011 §3).
        let error = execute(
            &tool,
            json!({"action": "react", "seq": 6, "emoji": "👍"}),
            "react-after-search",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::Protocol(_)), "{error:?}");
    }

    #[tokio::test]
    async fn blank_search_query_is_refused_before_it_reaches_the_server() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());
        let error = execute(&tool, json!({"action": "search", "query": "   "}), "blank")
            .await
            .unwrap_err();
        assert!(matches!(error, ToolError::InvalidArguments), "{error:?}");
        assert!(api.searches.lock().await.is_empty());
    }

    #[tokio::test]
    async fn notification_settings_reads_when_nothing_is_named() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());
        execute(&tool, json!({"action": "notification_settings"}), "read")
            .await
            .unwrap();
        // 何も名指さない呼びは読み。既存の設定を空で上書きしない。
        assert_eq!(
            api.notifications.lock().await.as_slice(),
            &[(None, 0, Vec::<String>::new())]
        );

        execute(
            &tool,
            json!({
                "action": "notification_settings",
                "defaults_level": "mentions",
                "keywords": ["デプロイ", "障害"]
            }),
            "write",
        )
        .await
        .unwrap();
        assert_eq!(
            api.notifications.lock().await.last().unwrap(),
            &(
                Some("mentions".to_owned()),
                0,
                vec!["デプロイ".to_owned(), "障害".to_owned()]
            )
        );
    }

    #[tokio::test]
    async fn notification_settings_rejects_an_unknown_level() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());
        let error = execute(
            &tool,
            json!({"action": "notification_settings", "defaults_level": "silent"}),
            "unknown-level",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::InvalidArguments), "{error:?}");
        assert!(api.notifications.lock().await.is_empty());
    }

    #[tokio::test]
    async fn attention_acknowledges_then_lists_what_remains() {
        let api = Arc::new(FakeMessagingApi::default());
        let tool = MessagingTool::new(api.clone());
        let output = execute(
            &tool,
            json!({"action": "attention", "consume_through": 3, "limit": 10}),
            "attention",
        )
        .await
        .unwrap();
        assert_eq!(
            api.attentions.lock().await.as_slice(),
            &[(Some(3), Some(10))]
        );
        // 候補は message ref。本文は運ばれず、続きは place を開いて読む。
        let candidate = &output.details["candidates"][0];
        assert_eq!(candidate["reason"], "mention");
        assert!(candidate.get("content").is_none());

        // 0 は「何も ack しない」で、それは省略が既に言っていること。
        let error = execute(
            &tool,
            json!({"action": "attention", "consume_through": 0}),
            "zero-ack",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, ToolError::InvalidArguments), "{error:?}");
    }
}
