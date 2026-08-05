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
        CreateMessagingReplyLaterRequest, MessagingApi, OpenMessagingPlaceRequest,
        ReactMessagingReactionRequest, ReadMessagingThroughRequest,
        ResolveMessagingReplyLaterRequest, SetMessagingStatusRequest, WriteMessagingMessageRequest,
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
// The server bounds emoji at 32 characters; 128 bytes covers any such UTF-8.
const MAX_EMOJI_BYTES: usize = 128;
// The server counts characters, not bytes, so this check must too: a 201
// character ASCII note is well under any byte budget and still a 400.
const MAX_STATUS_NOTE_CHARS: usize = 200;
const MAX_REPLY_LATER_NOTE_CHARS: usize = 500;
// A week, matching the server's bound on relative durations.
const MAX_RELATIVE_MINUTES: u32 = 7 * 24 * 60;

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
    /// Mark one's own earlier promise as kept.  Like the human's reply-later
    /// list this is reachable from anywhere, not only from the place.
    ResolveReplyLater { marker_id: String },
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
            "status and may include note or expires_in_minutes; resolve_reply_later requires ",
            "marker_id. Write, react and reply_later act on the place most recently opened in ",
            "this tool view; status and resolve_reply_later need no open place."
        ),
        "properties": {
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
                    "marker_id returned when you made the promise."
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
                let nonce = client_nonce(ctx.flow_id, ctx.call_id);
                tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(ToolError::Cancelled),
                    result = self.api.react(ReactMessagingReactionRequest {
                        place_id: &place_id,
                        message_id: &target.message_id,
                        emoji: &emoji,
                        client_nonce: &nonce,
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
        promises: AsyncMutex<Vec<(String, String, Option<String>, Option<u32>)>>,
        resolutions: AsyncMutex<Vec<String>>,
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
                    {"message_id": "m6", "seq": 6, "content": "earlier", "reactions": []},
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
                format!("{}:{}", request.emoji, request.client_nonce),
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
            15
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
    async fn open_does_not_mark_read_until_the_next_admitted_tool_result() {
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
