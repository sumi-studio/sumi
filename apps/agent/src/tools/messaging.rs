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
        MessagingApi, OpenMessagingPlaceRequest, ReactMessagingReactionRequest,
        ReadMessagingThroughRequest, WriteMessagingMessageRequest,
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
// The server bounds emoji at 32 characters; 128 bytes covers any such UTF-8.
const MAX_EMOJI_BYTES: usize = 128;

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
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MessagingUrgency {
    Urgent,
    #[default]
    Normal,
    Fyi,
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
            "requires emoji plus exactly one of message_id or seq. Write and react act on ",
            "the place most recently opened in this tool view."
        ),
        "properties": {
            "action": {
                "type": "string",
                "enum": ["overview", "open", "write", "react"],
                "description": concat!(
                    "Action to perform: overview lists available places and unread state; open ",
                    "shows one place and focuses it for later writes; write sends a message to ",
                    "the currently open place; react toggles an emoji reaction on a message ",
                    "visible in the currently open place."
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
                    "For react and omitted for other actions. The message to react to, by ",
                    "message_id. Provide exactly one of message_id or seq; the message must be ",
                    "visible in the currently open place."
                )
            },
            "seq": {
                "type": "integer",
                "minimum": 1,
                "description": concat!(
                    "For react and omitted for other actions. The message to react to, by its ",
                    "seq in the currently open place. Provide exactly one of message_id or seq."
                )
            },
            "emoji": {
                "type": "string",
                "description": concat!(
                    "Required for react and omitted for other actions. Emoji to toggle on the ",
                    "target message; reacting again with the same emoji removes your reaction."
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
                "that currently open place or react to a message visible in it. ",
                "Opening never publishes presence."
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
                let target = state
                    .visible_messages
                    .iter()
                    .find(|message| match (&message_id, seq) {
                        (Some(id), _) => &message.message_id == id,
                        (None, Some(seq)) => message.seq == Some(seq),
                        (None, None) => false,
                    })
                    .cloned()
                    .ok_or_else(|| {
                        ToolError::Protocol(
                            "that message is not visible in the currently open place; open the place (paging with before_seq if needed) so the message is on screen, then react"
                                .to_owned(),
                        )
                    })?;
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
            // Exactly one selector: a reaction lands on one visible message.
            if message_id.is_some() == seq.is_some() {
                return Err(ToolError::InvalidArguments);
            }
            if message_id
                .as_deref()
                .is_some_and(|id| validate_bounded_nonempty(id, MAX_MESSAGE_ID_BYTES).is_err())
            {
                return Err(ToolError::InvalidArguments);
            }
            if seq == &Some(0) {
                return Err(ToolError::InvalidArguments);
            }
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
    }
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
                request.emoji.to_owned(),
            ));
            Ok(json!({
                "message": {"message_id": request.message_id,
                            "reactions": [{"emoji": request.emoji, "participants": []}]},
                "reacted": true
            }))
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
            json!(["overview", "open", "write", "react"])
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
            schema["properties"]
                .as_object()
                .expect("properties must be an object")
                .len(),
            10
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
}
